# Databricks notebook source
#-------------------------------------------------------------------------------
#importing packages and functions
#-------------------------------------------------------------------------------
import os
import sys
import time
import atexit
import platform
import warnings
import logging
#import yaml
from pyspark import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
#-------------------------------------------------------------------------------
#importing functions from rule engine module
#-------------------------------------------------------------------------------

try:
    store = getArgument('store')
    #store = 'SB'
except:
    store = None
# importing ruleengine scripts
if store is None:
    import transactionclass as trcl
    import pricelookup as plk
    import bonstorno as bon
    import empties as empt
    import suspended as susp
    import line_item_derived as line
    #store = 'SB'

#-------------------------------------------------------------------------------
#import sparksession and start the same for rule ingestion
#-------------------------------------------------------------------------------


def createsparksession(logger,conf):
	import py4j
	from pyspark.conf import SparkConf
	from pyspark.context import SparkContext
	try:
		# Try to access HiveConf, it will raise exception if Hive is not added
		if conf.get('spark.sql.catalogImplementation', 'hive').lower() == 'hive':
			SparkContext._jvm.org.apache.hadoop.hive.conf.HiveConf()
			return SparkSession.builder.config(conf=conf)\
				.enableHiveSupport()\
				.getOrCreate()
		else:
			return SparkSession.builder.config(conf=conf).getOrCreate()
	except (py4j.protocol.Py4JError, TypeError):
		if conf.get('spark.sql.catalogImplementation', '').lower() == 'hive':
			logger.warning("Fall back to non-hive support because failing to access HiveConf, "
						  "please make sure you build spark with hive")
	except Exception:
		logger.warning("Error starting spark session")
        
#-------------------------------------------------------------------------------
#Rule engine function call
#-------------------------------------------------------------------------------
        
def rule_ingest1(spark, logger,epoch_id,sparksession_count_re,config,failedlistappend):    

#-------------------------------------------------------------------------------
#Creates spark session for first execution
#------------------------------------------------------------------------------- 
    if store is None:
      if sparksession_count_re == 0:

          conf = SparkConf()
          conf.set("spark.executor.memory", config['input']['re_conf']['executor_memory'])
          conf.set("spark.driver.memory", config['input']['re_conf']['driver_memory'])
          conf.set("spark.submit.deployMode", config['input']['re_conf']['deploy_mode'])
          conf.set("spark.driver.maxResultSize", config['input']['re_conf']['max_result_size'])
          conf.set("spark.sql.broadcastTimeout", config['input']['re_conf']['broadcastTimeout'])
          conf.set("spark.sql.autoBroadcastJoinThreshold", config['input']['re_conf']['autoBroadcastJoinThreshold'])

          if os.environ.get("SPARK_EXECUTOR_URI"):
              SparkContext.setSystemProperty("spark.executor.uri", os.environ["SPARK_EXECUTOR_URI"])

          SparkContext._ensure_initialized()

          try:
              spark = createsparksession(logger, conf)
          except Exception:
              import sys
              import traceback
              logger.error("Failed to initialize Spark session.")
              traceback.print_exc(file=sys.stderr)
              sys.exit(1)

          sc = spark.sparkContext
          sql = spark.sql
          atexit.register(lambda: sc.stop())

          # for compatibility
          sqlContext = spark._wrapped
          sqlCtx = sqlContext

          #print(spark)
          #print("SparkSession available as 'spark'.")

          # The ./bin/pyspark script stores the old PYTHONSTARTUP value in OLD_PYTHONSTARTUP,
          # which allows us to execute the user's PYTHONSTARTUP file:
          _pythonstartup = os.environ.get('OLD_PYTHONSTARTUP')
          if _pythonstartup and os.path.isfile(_pythonstartup):
              with open(_pythonstartup) as f:
                  code = compile(f.read(), _pythonstartup, 'exec')
                  #exec(code)        
    while 1:
        try:
            start=time.time()
            line.create_line_item_derived_dataframe(spark,epoch_id,logger,config)
            logger.info("line_item_derived function %s seconds "%(time.time()-start))
        except Exception as e:
            import sys        
            logger.error("Lineitem derived function failed due to : %s",e)
            failedlistappend.append(int(epoch_id))
            break

        try:
            start=time.time()
            trcl.create_flags_rules_classification_dataframe(spark,epoch_id,logger,config) 
            logger.info("Classification description function %s seconds "%(time.time()-start))
        except Exception as e:
            import sys
            logger.error("Transaction classification function failed due to : %s",e)
            failedlistappend.append(int(epoch_id))
            break


        try:
            start=time.time()
            plk.create_pricelookup_dataframe(spark,epoch_id,logger,config)
            logger.info("PriceLookup function %s seconds "%(time.time()-start))
        except Exception as e:        
            logger.error("price lookup fraud function failed due to : %s",e)
            failedlistappend.append(int(epoch_id))
            break


        try:
            start=time.time()
            bon.create_bonstrono_dataframe(spark,epoch_id,logger,config)
            logger.info("Bonstorno function %s seconds "%(time.time()-start))
        except Exception as e:
            logger.error("Bonstrono fraud function failed due to : %s",e)
            failedlistappend.append(int(epoch_id))
            break


        try:
            start=time.time()
            empt.create_repeated_empties_dataframe(spark,epoch_id,logger,config)
            logger.info("Repeated empties function %s seconds "%(time.time()-start))
        except Exception as e:
            import sys
            logger.error("Repeated empties function failed due to : %s",e)
            failedlistappend.append(int(epoch_id))
            break

        try:
            start=time.time()
            susp.create_suspended_fraud_dataframe(spark,epoch_id,logger,config)
            logger.info("Suspended fraud function %s seconds "%(time.time()-start))
            break
        except Exception as e:
            logger.error("Suspended fraud function failed due to : %s",e)
            failedlistappend.append(int(epoch_id))
            break
    
    sparksession_count_re=sparksession_count_re+1
    #logger.info("Counter Variable value for sparksession is : %s",sparksession_count_re)    
    """
    fbatch = sc.parallelize(failedlistappend)
    schema = StructType([StructField("batchid", IntegerType(), False)])
    row_fbatch = fbatch.map(lambda x: Row(x))
    if len(failedlistappend) == 0:
            failurebatch = spark.createDataFrame([], schema)
    else:
            failurebatch = spark.createDataFrame(row_fbatch,['batchid'])
    failurebatch.coalesce(1).write.mode('overwrite').parquet(config["output"]["ruleengine"]["restart_failure_batch"])
    
    """
    #path=config["output"]["ruleengine"]["restart_failure_batch"]
    #spark.sql("""alter table fraud.re_failure_batchid set location '{path}'""".format(path=path))
     


