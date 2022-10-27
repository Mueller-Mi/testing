# Databricks notebook source
import pyodbc
from pyspark.sql import Window
from pyspark.sql import functions as F

#-------------------------------------------------------------#
#calling load_control_settings
#-------------------------------------------------------------#
def load_control_settings(spark, logger, config):
    
    """Creates a db_settings dictionary
    
    return: db_settings
    """
    
    logger.info('Creating db settings for LoadControl')   
    db_settings = {
        'sqlserver' : "jdbc:sqlserver:{serverip}:{port};database={database}".format(serverip=config['output']['db_setting']['server_ip'], port=config['output']['db_setting']['port'], database=config['output']['db_setting']['database_name'] ),
        'username' : "{username}".format(username=config['output']['db_setting']['user_name']),
        'password' : "{password}".format(password=config['output']['db_setting']['password']),
        'driver' : "{driver}".format(driver=config['output']['db_setting']['driver']),
        'storeshivelocation' : config['output']['processed_df_folder']['stores'],
        'stores' : config['output']['load_control']['stores'],
        'storein_final' : "{tablename}".format(tablename=config['output']['log_tables']['storein_final']),
        'loadcontrol_staging' : "{tablename}".format(tablename=config['output']['load_control']['loadcontrol_staging']),
        'loadcontrol_final' : "{tablename}".format(tablename=config['output']['load_control']['loadcontrol_final']),
        'database' : "{database}".format(database=config['input']['hive']['database'])
    }
    
    return db_settings
#-------------------------------------------------------------#
#calling read_stores
#-------------------------------------------------------------#
def read_stores(spark, logger, config):
    
    """Reads Stores table from the db
    
    return: stores_df
    """
    
    logger.info('Reading Stores table and updating hive table')
    
    db_settings = load_control_settings(spark, logger, config)

    stores_df = spark.read.format("jdbc")\
    .option("url", db_settings['sqlserver'])\
    .option("dbtable", db_settings['stores'])\
    .option("user", db_settings['username'])\
    .option("password", db_settings['password'])\
    .option("driver", db_settings['driver'])\
    .load()
    
    stores_df.coalesce(1).write.mode('overwrite').format('delta').option('overwriteSchema', 'true').save(db_settings['storeshivelocation'])

    spark.sql("""create table if not exists {fraud}.stores location '{path}'""".format(path=db_settings['storeshivelocation'], fraud=db_settings['database']))
    spark.sql("""alter table {fraud}.stores set location '{path}'""".format(path=db_settings['storeshivelocation'], fraud=db_settings['database']))
    
#     stores_df.coalesce(1).write.mode('overwrite').parquet(db_settings['storeshivelocation'])
    
#     spark.sql("""alter table {fraud}.stores set location '{path}'""".format(path=db_settings['storeshivelocation'], fraud=db_settings['database']))
    return stores_df

#-------------------------------------------------------------#
#calling stores_to_loadcontrol
#-------------------------------------------------------------#
def stores_to_loadcontrol(spark, logger, stores, config):
    
    """Reads Stores table and updates LoadControl
    
    return: loadcontrol_final
    """
    
    logger.info('Reading status from Stores and Updating LoadControl')
    
    db_settings = load_control_settings(spark, logger, config)
    
    loadcontrol_df = stores.select(F.col('StoreID').alias('RetailStoreID'), F.when(F.col('Status') == 'Aktiv', 'A')
                   .when(F.col('Status') == 'Inaktiv', 'I')
                   .when(F.col('Status') == 'Inaktiv (temp.)', 'T').alias('Status'),
                   F.col('Reason'))
    
    loadcontrol_df = loadcontrol_df.withColumn('Date', F.current_date())
    loadcontrol_df = loadcontrol_df.withColumn('LoadID', F.lit(None))
    loadcontrol_df = loadcontrol_df.registerTempTable('loadcontrol')
    
    loadcontrol_df = spark.sql("select RetailStoreID, date_sub(Date,1) as Date, Status, cast(NULL as smallint) as LoadID, Reason from loadcontrol")
    
    #updating loadcontrol table with new stores status
    loadcontrol_df.write.mode("overwrite").format("jdbc")\
    .option("url", db_settings['sqlserver'])\
    .option("dbtable", db_settings['loadcontrol_staging'])\
    .option("user", db_settings['username'])\
    .option("password", db_settings['password'])\
    .option("driver", db_settings['driver'])\
    .save()
    
    call_loadcontrol_stored_procedure(spark, logger, config)
    
    #reading history data from loadcontrol
    loadcontrol_final = spark.read.format("jdbc")\
    .option("url", db_settings['sqlserver'])\
    .option("dbtable", db_settings['loadcontrol_final'])\
    .option("user", db_settings['username'])\
    .option("password", db_settings['password'])\
    .option("driver", db_settings['driver'])\
    .load()
        
    loadcontrol_final = loadcontrol_final.dropDuplicates(['RetailStoreID', 'Date'])
    
    logger.info('stores to load control success')
    return loadcontrol_final

#-------------------------------------------------------------#
#calling update_loadcontrol
#-------------------------------------------------------------#
def update_loadcontrol(spark, logger, config, loadcontrol, storein_loadlog):
    
    """Updates LoadControl table with the status
    
    return: None
    """
    try:
      logger.info('Updating LoadControl Table')
    
      db_settings = load_control_settings(spark, logger, config)
      logger.info('db_settings are created')
      storein_loadlog = storein_loadlog.withColumnRenamed('BusinessDate', 'Date')
      storein_loadlog = storein_loadlog.withColumnRenamed('LoadID', 'StoreInLoadID')
      storein_loadlog = storein_loadlog.withColumnRenamed('Status', 'StoreInStatus')
      loadcontrol = loadcontrol.withColumnRenamed('Status', 'LoadControlStatus')
      loadcontrol = loadcontrol.withColumnRenamed('LoadID', 'LoadControlLoadID')

      storeinlog = spark.read.format("jdbc")\
      .option("url", db_settings['sqlserver'])\
      .option("dbtable", db_settings['storein_final'])\
      .option("user", db_settings['username'])\
      .option("password", db_settings['password'])\
      .option("driver", db_settings['driver'])\
      .load()

      _max = storeinlog.agg({"LoadID":"max"}).collect()[0]['max(LoadID)']

      if _max is None:
          _max = 1
      else:
          _max += 1

      storein_loadlog = storein_loadlog.withColumn('StoreInLoadID', F.lit(_max))

      #joining loadcontrol with storeindailyloadlog
      merged_df = loadcontrol.join(storein_loadlog, ['RetailStoreID', 'Date'], 'left')

      merged_df = merged_df.withColumn('LoadControlStatus', F.when(F.col('StoreInStatus') == 'L', 'L')
                                       .when(F.col('StoreInStatus') == 'E', 'E')
                                       .otherwise(F.col('LoadControlStatus')))

      merged_df = merged_df.withColumn('LoadControlLoadID', F.when(F.col('StoreInLoadID') == _max, _max)
                                       .otherwise(F.col('LoadControlLoadID')))

      merged_df = merged_df.select('RetailStoreID', 'LoadControlLoadID', 'LoadControlStatus', 'Date', 'Reason')
      merged_df = merged_df.withColumnRenamed('LoadControlLoadID', 'LoadID')
      merged_df = merged_df.withColumnRenamed('LoadControlStatus', 'Status')

      merged_df = merged_df.select('RetailStoreID', 'Date', 'Status', 'LoadID', 'Reason')
      merged_df = merged_df.dropDuplicates(['RetailStoreID', 'Date'])

      merged_df.write.mode("overwrite").format("jdbc")\
      .option("url", db_settings['sqlserver'])\
      .option("dbtable", db_settings['loadcontrol_staging'])\
      .option("user", db_settings['username'])\
      .option("password", db_settings['password'])\
      .option("driver", db_settings['driver'])\
      .save()

    except Exception as e:
      logger.error('got error in update_loadcontrol')
      logger.error(e)
      
    logger.info('calling loadcontrol sp')
    call_loadcontrol_stored_procedure(spark, logger, config)
    
    
    
def call_loadcontrol_stored_procedure(spark, logger, config):
    
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
    logger.info('call LoadControl stored proc')
    sqlExecSP ="{call LoadControl ()}"
    cursor = conn.cursor()
    cursor.execute(sqlExecSP)
    conn.commit()
    logger.info('loadcontrol sp is success')
    
    
   
