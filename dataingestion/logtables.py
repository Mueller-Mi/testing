# Databricks notebook source
# -------------------------------------------------------------#
# importing functions
# -------------------------------------------------------------#
from __future__ import print_function
from pyspark.sql import functions as F
import pandas as pd
from pyspark.sql.types import *
import pyodbc


# -------------------------------------------------------------#
# defining log_tables
# -------------------------------------------------------------#
def log_tables(spark, config):
    """creating db_settings dictionary reading a config file

    Return : db_settings
    """

    db_settings = {
        'sqlserver': "jdbc:sqlserver:{serverip}:{port};database={database}".format(
            serverip=config['output']['db_setting']['server_ip'], port=config['output']['db_setting']['port'],
            database=config['output']['db_setting']['database_name']),
        'username': "{username}".format(username=config['output']['db_setting']['user_name']),
        'password': "{password}".format(password=config['output']['db_setting']['password']),
        'driver': "{driver}".format(driver=config['output']['db_setting']['driver']),
        'dailyloadlog_staging': "{tablename}".format(tablename=config['output']['log_tables']['dailyloadlog_staging']),
        'storein_staging': "{tablename}".format(tablename=config['output']['log_tables']['storein_staging']),
        'logging_staging': "{tablename}".format(tablename=config['output']['log_tables']['logging_staging']),
        'xmlfiles_staging': "{tablename}".format(tablename=config['output']['log_tables']['xmlfiles_staging']),
        'dailyloadlog_final': "{tablename}".format(tablename=config['output']['log_tables']['dailyloadlog_final']),
        'storein_final': "{tablename}".format(tablename=config['output']['log_tables']['storein_final']),
        'logging_final': "{tablename}".format(tablename=config['output']['log_tables']['logging_final']),
        'xmlfiles_final': "{tablename}".format(tablename=config['output']['log_tables']['xmlfiles_final'])
    }

    return db_settings


# -------------------------------------------------------------#
# defining daily_load_log
# -------------------------------------------------------------#
def daily_load_log(spark, dailyloadlog_df, config):
    """This function reads fina.DailyLoadLog to get the max LoadID
    and writes the data to staging.DailyLoadLog
     
    Return : None
    """
    
    db_settings = log_tables(spark, config)

    dailyloadlog = spark.read.format("jdbc") \
        .option("url", db_settings['sqlserver']) \
        .option("dbtable", db_settings['dailyloadlog_final']) \
        .option("user", db_settings['username']) \
        .option("password", db_settings['password']) \
        .option("driver", db_settings['driver']) \
        .load()

    _max = dailyloadlog.agg({"LoadID": "max"}).collect()[0]['max(LoadID)']

    if (_max == None):
        _max = 1
    else:
        _max = _max + 1

    dailyloadlog_df = dailyloadlog_df.toPandas()
    dailyloadlog_df.loc[:, ['LoadID']] = [_max]
    dailyloadlog_df = spark.createDataFrame(dailyloadlog_df)
    dailyloadlog_df = dailyloadlog_df.select('LoadID', 'SourceFolder', 'StoreCount', 'WarningsCount', 'ErrorsCount',
                                             'StartDatetime', 'EndDatetime')

    dailyloadlog_df.write.mode("overwrite").format("jdbc") \
        .option("url", db_settings['sqlserver']) \
        .option("dbtable", db_settings['dailyloadlog_staging']) \
        .option("user", db_settings['username']) \
        .option("password", db_settings['password']) \
        .option("driver", db_settings['driver']) \
        .save()
    

# -------------------------------------------------------------#
# definign store_in_daily_loadlog
# -------------------------------------------------------------#
def store_in_daily_loadlog(spark, stoteindailyloadlog_df, config):
    """This function reads final.StoreInDailyLoadLog to get max of LoadID and
    writes the updated data to staging.StoreInDailyLoadLog
    
    Return : None
    """

    db_settings = log_tables(spark, config)

    storeinlog = spark.read.format("jdbc") \
        .option("url", db_settings['sqlserver']) \
        .option("dbtable", db_settings['storein_final']) \
        .option("user", db_settings['username']) \
        .option("password", db_settings['password']) \
        .option("driver", db_settings['driver']) \
        .load()

    _max = storeinlog.agg({"LoadID": "max"}).collect()[0]['max(LoadID)']

    if _max is None:
        _max = 1
    else:
        _max = _max + 1

    stoteindailyloadlog_df = stoteindailyloadlog_df.toPandas()
    stoteindailyloadlog_df.loc[:, ['LoadID']] = [_max]
    stoteindailyloadlog_df = spark.createDataFrame(stoteindailyloadlog_df)
    stoteindailyloadlog_df = stoteindailyloadlog_df.select('RetailStoreID', 'LoadID', 'Status', 'BusinessDate',
                                                           'StartDatetime', 'EndDatetime', 'WarningsCount',
                                                           'ErrorsCount')

    stoteindailyloadlog_df.write.mode("overwrite").format("jdbc") \
        .option("url", db_settings['sqlserver']) \
        .option("dbtable", db_settings['storein_staging']) \
        .option("user", db_settings['username']) \
        .option("password", db_settings['password']) \
        .option("driver", db_settings['driver']) \
        .save()


# -------------------------------------------------------------#
# defining eventlog_di
# -------------------------------------------------------------#
def eventlog_di(spark, logging_df, config):
    """This function readsfinal.EventLog table to get max LoadID and
    writes the updated data to staging.EventLog
    
    Return : None
    """

    db_settings = log_tables(spark, config)

    loggingdf = spark.read.format("jdbc") \
        .option("url", db_settings['sqlserver']) \
        .option("dbtable", db_settings['logging_final']) \
        .option("user", db_settings['username']) \
        .option("password", db_settings['password']) \
        .option("driver", db_settings['driver']) \
        .load()

    _max = loggingdf.agg({"LoadID": "max"}).collect()[0]['max(LoadID)']

    if (_max == None):
        _max = 1
    else:
        _max = _max + 1

    logging_df = logging_df.toPandas()
    logging_df.loc[:, ['LoadID']] = [_max]
    logging_df = spark.createDataFrame(logging_df)

    logging_df = logging_df.select('EventDateTime', 'RetailStoreID', 'LoadID', 'Level', 'EventType', 'EventMessage')

    logging_df.write.mode("overwrite").format("jdbc") \
        .option("url", db_settings['sqlserver']) \
        .option("dbtable", db_settings['logging_staging']) \
        .option("user", db_settings['username']) \
        .option("password", db_settings['password']) \
        .option("driver", db_settings['driver']) \
        .save()


# -------------------------------------------------------------#
# defining xmlfiles
# -------------------------------------------------------------#
def xmlfiles(spark, xmlfiles_df, config):
    """This functions reads final.XMLFiles to get max of LoadID and
    writes the updated data to staging.XMLfiles
    
    Return : None
    """

    db_settings = log_tables(spark, config)

    xmlfilesdf = spark.read.format("jdbc") \
        .option("url", db_settings['sqlserver']) \
        .option("dbtable", db_settings['xmlfiles_final']) \
        .option("user", db_settings['username']) \
        .option("password", db_settings['password']) \
        .option("driver", db_settings['driver']) \
        .load()

    _max = xmlfilesdf.agg({"LoadID": "max"}).collect()[0]['max(LoadID)']

    if _max is None:
        _max = 1
    else:
        _max = _max + 1

    xmlfiles_df = xmlfiles_df.toPandas()
    xmlfiles_df.loc[:, ['LoadID']] = [_max]
    xmlfiles_df = spark.createDataFrame(xmlfiles_df)
    xmlfiles_df = xmlfiles_df.withColumn('IsDeleted', F.lit(0))
    xmlfiles_df = xmlfiles_df.select('xmlfilename', 'RetailStoreID', 'LoadID', 'BusinessDate', 'CreationDateTime',
                                     'ConsecutiveNumber', 'IsDeleted')

    xmlfiles_df.write.mode("overwrite").format("jdbc") \
        .option("url", db_settings['sqlserver']) \
        .option("dbtable", db_settings['xmlfiles_staging']) \
        .option("user", db_settings['username']) \
        .option("password", db_settings['password']) \
        .option("driver", db_settings['driver']) \
        .save()


def call_logtables_stored_procedure(spark, logger, config):
    logger.info('Copying data from staging logtables to final')

    driver = "{driver}".format(driver=config['output']['db_setting']['pyodbc_driver'])
    sqlserver = "{serverip}".format(serverip=config['output']['db_setting']['pyodbc_server'])
    database_name = "{database_name}".format(database_name=config['output']['db_setting']['database_name'])
    username = "{username}".format(username=config['output']['db_setting']['user_name'])
    password = "{password}".format(password=config['output']['db_setting']['password'])

    conn = pyodbc.connect(
        'DRIVER={driver};SERVER={server};DATABASE={db};UID={uid};PWD={pwd}'.format(driver=driver,
                                                                                   server=sqlserver, db=database_name,
                                                                                   uid=username,
                                                                                   pwd=password))

    sqlExecSP = "{call LogTables ()}"
    cursor = conn.cursor()
    cursor.execute(sqlExecSP)
    conn.commit()
