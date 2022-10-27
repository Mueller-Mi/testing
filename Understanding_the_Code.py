# Databricks notebook source
# MAGIC %md
# MAGIC #### This repo will be used to explain the code of daily dataingestion

# COMMAND ----------

# MAGIC %run ./dataingestion/init

# COMMAND ----------

# MAGIC %run ./ruleengine/init

# COMMAND ----------

from __future__ import print_function
import os
import sys

from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, SQLContext
import pandas as pd
from datetime import datetime,date,timedelta
import datetime as dt
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import atexit
import pyodbc
import time
import logging
import json

# COMMAND ----------

#prüft ob ein Store ist vorhanden 
try:
    store = getArgument('store')
    print(store)
except:
    store = None
    
# falls nicht 
if (store is None):
    #Manuelles setzen der Umgebungsvariablen 
    os.environ["PYSPARK_PYTHON"] = "/opt/cloudera/parcels/Anaconda/bin/python"
    os.environ["JAVA_HOME"] = "/usr/lib64/jvm/java"
    os.environ["SPARK_HOME"] = "/opt/cloudera/parcels/CDH/lib/spark"
    os.environ["PYLIB"] = os.environ["SPARK_HOME"] + "/python/lib"
    sys.path.insert(0, os.environ["PYLIB"] + "/py4j-0.10.7-src.zip")
    sys.path.insert(0, os.environ["PYLIB"] + "/pyspark.zip")

    # exec(open(os.path.join(os.environ["SPARK_HOME"], 'python/pyspark/shell.py')).read())
    
    # importing functions from dataingestion module
    from dataingestion import dataingestion_main as di
    from dataingestion import dataingestion_sql as di_sql
    from dataingestion import ruleengine_sql as re_sql
    from dataingestion import load_control as load

    # importing ruleengine script
    from ruleengine import rule_ingestion1_logger as re
    
    # from ruleengine import pep_export as pep
    from ruleengine import pep_export_final as pep_f
    from ruleengine import pep_export_staging as pep_s
    from ruleengine import daily_report as daily
    from ruleengine import db_metrics_report as metrics
    from ruleengine import databasesize_increase as dbsize

    # reading config file
    import yaml
    sys_var = sys.argv[1]
    config_file = open(sys_var, 'r')
    configuration = yaml.load(config_file)
    config_file.close()
else:
    # Falls ein Store gefunden wurde, wird das Notebook Config im Pfad Config aufgerufen, welches in 60sec abgearbeitet sein soll, ansonsten wird eine Fehlermeldung geworfen. 
    configuration = json.loads(dbutils.notebook.run('Config/config', 60, {'store': store}))
    configuration['runtime'] = 'databricks' # varibale Runtime wird databricks zugewiesen, dies dient später als Check ob es sich um das Cluster "on-premise" oder "Databricks" handelt
    global spark
    spark = spark
    

# COMMAND ----------

def main(config):
    if config.get('runtime', 'on-premise') == 'on-premise': # ich nehme mal an dass hier gecheckt wird ob es sich um den Server on Prem handelt und ansonsten die ressourcen von Azure verwendet werden 
        conf = SparkConf() #Verbindung mit dem Cluster aufbauen und konfiguieren
        conf.set("spark.executor.memory", config['input']['di_conf']['executor_memory'])
        conf.set("spark.driver.memory", config['input']['di_conf']['driver_memory'])
        conf.set("spark.submit.deployMode", config['input']['di_conf']['deploy_mode'])
        conf.set("spark.driver.maxResultSize", config['input']['di_conf']['max_result_size'])
        conf.set("spark.sql.broadcastTimeout", config['input']['di_conf']['broadcastTimeout'])
        conf.set("spark.sql.autoBroadcastJoinThreshold", config['input']['di_conf']['autoBroadcastJoinThreshold'])
    else:
        global spark
    
        # adding log filehandler
    currentdate = datetime.now().strftime("%Y%m%d")
    hrmi = datetime.now().strftime("%H%M")
    logpath = config["output"]["logpath"]["logpath"]
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    fomatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    if config.get('runtime', 'on-premise') == 'on-premise':
        file_handler = logging.FileHandler(filename=f'{logpath}/fraud_daily_ingestion_{currentdate}_{hrmi}.log')
    else:
        file_handler = logging.FileHandler(filename=f'/tmp/fraud_daily_ingestion_{currentdate}_{hrmi}.log')
    file_handler.setFormatter(fomatter)
    logger.addHandler(file_handler)

    # creating variables
    count = 0
    sparksession_count_re = 0
    failedlistappend = []
    active_inactive_list = []
    active_inactive_count = 0

    # get spark session
    if config.get('runtime', 'on-premise') == 'on-premise':
        if os.environ.get("SPARK_EXECUTOR_URI"):
            SparkContext.setSystemProperty("spark.executor.uri", os.environ["SPARK_EXECUTOR_URI"])
        SparkContext._ensure_initialized()

        try:
            spark = createsparksession(logger, conf)
        except Exception as e:
            import sys
            import traceback
            logger.error("Failed to initialize Spark session.")
            traceback.print_exc(file=sys.stderr)
            sys.exit(1)

    # spark is already availabe in databricks
    # processing SAP master data
    if config.get('runtime', 'on-premise') == 'on-premise':
        di.process_masterdata(spark, logger, config)
    else:
        di.process_masterdata(spark, logger, config)
        

    # get folder list
    try:
        if config.get('runtime','on-premise') == 'on-premise':
            # ich weiß nicht wo ich diese Funktion nachschauen kann :/  
            dir_path = get_listofpartfile(spark, logger, config) 
            logger.info('List of files %s', dir_path)
        else:
            dir_path = ''
            
        logger.info('calling loadcontrol_df')
        loadcontrol = loadcontrol_df(spark, logger, config)
        logger.info('loadcontrol_df is success')
        batch_df = read_files(spark, logger, dir_path, config, loadcontrol, active_inactive_list)
    except Exception as e:
        import sys
        logger.error(e)
        sys.exit(1)


    for batch in batch_df:
        try:

            date_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            pattern = '%Y-%m-%d %H:%M:%S'
            
            epoch_id = int(time.mktime(time.strptime(date_time, pattern)))

            storein_loadlog = di.dataingestion_process(spark, logger, dir_path, batch, epoch_id, count, config,
                                                       loadcontrol, active_inactive_list, active_inactive_count)
            
            re.rule_ingest1(spark, logger, epoch_id, sparksession_count_re, config, failedlistappend)
            if epoch_id not in failedlistappend:
                copy_to_sqldb(spark, logger, epoch_id, config)
                copy_staging_to_final(spark, logger, config)
                copy_lkp_tables(spark, logger, config)
#                 pep_s.create_pep_report(spark, config, logger)
                dbsize.create_db_size_report(spark, config, logger)
            else:
                logger.info('RuleEngine failed for epoch id {}'.format(epoch_id))
            
           
          
        except Exception as e:
            logger.error(e)


    try:
#         list_to_append_failure_call(spark, failedlistappend, config)
        logger.info('Appending Failed batch id to hive table')
    except Exception as e:
        logger.error('Failed batch id appended to hive table failed')
        # pep.create_pep_report(spark, config, logger)
    if config.get('runtime', 'on-premise') == 'on-premise':
        pep_f.pep_export_final(spark, config, logger)
    daily.create_daily_report(spark, config, logger)
#     metrics.create_db_metrics_report(spark, config, logger)
    daily_data_deletion(spark, logger, config)
    if config.get('runtime', 'on-premise') == 'on-premise':
        spark.stop()
    else:
        # move log from cluster to data lake
        dbutils.fs.mv(f'file:///databricks/driver/fraud_daily_ingestion_{currentdate}_{hrmi}.log', f'{logpath}/')
    
