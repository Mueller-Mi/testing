# Databricks notebook source
import time

def create_db_size_report(spark,config,logger):

    

    """Creates db size increase in MB for ingested xml files

    Parameters
    ----------
    Spark session object
    Logger
    Configuration file


    """
    #------------------------------------------------------------------------------------------------------------#
    #Logger info updated
    #------------------------------------------------------------------------------------------------------------#    
    
    logger.info("Creating db size increase in MB for ingested xml files")

    #------------------------------------------------------------------------------------------------------------#
    #"Creates configuration dictionary"
    #------------------------------------------------------------------------------------------------------------#
    
    db_settings = {
        'sqlserver' : "jdbc:sqlserver:{serverip}:{port};database={database}".format(serverip=config['output']['db_setting']['server_ip'],              port=config['output']['db_setting']['port'], database=config['output']['db_setting']['database_name'] ),
        'username' : "{username}".format(username=config['output']['db_setting']['user_name']),
        'password' : "{password}".format(password=config['output']['db_setting']['password']),
        'driver' : "{driver}".format(driver=config['output']['db_setting']['driver'])
                  }

    #------------------------------------------------------------------------------------------------------------#
    #"Creates daily report dataframe"
    #------------------------------------------------------------------------------------------------------------#

    
    db_size = spark.read.format("jdbc")\
    .option("url", db_settings['sqlserver'])\
    .option("query", """SELECT  
    CAST(ROUND(((SUM(a.used_pages) * 8) / 1024.00), 2) AS NUMERIC(36, 2)) AS UsedSpaceMB
    
FROM 
    sys.tables t
INNER JOIN      
    sys.indexes i ON t.OBJECT_ID = i.object_id
INNER JOIN 
    sys.partitions p ON i.object_id = p.OBJECT_ID AND i.index_id = p.index_id
INNER JOIN 
    sys.allocation_units a ON p.partition_id = a.container_id
LEFT OUTER JOIN 
    sys.schemas s ON t.schema_id = s.schema_id
WHERE 
    t.NAME NOT LIKE 'dt%' 
	and upper(s.Name) in ('STAGING')
    AND t.is_ms_shipped = 0
    AND i.OBJECT_ID > 255 
GROUP BY 
    s.Name""")\
    .option("user", db_settings['username'])\
    .option("password", db_settings['password'])\
    .option("driver", db_settings['driver'])\
    .load()
    
    
    xmlcount = spark.read.format("jdbc")\
    .option("url", db_settings['sqlserver'])\
    .option("query", """SELECT
    count(xmlfilename) as XMLFilesCount
                            FROM
                              final.XMLFiles
                            where BusinessDate=CAST(CURRENT_TIMESTAMP-1 AS DATE)""")\
    .option("user", db_settings['username'])\
    .option("password", db_settings['password'])\
    .option("driver", db_settings['driver'])\
    .load()
    
    
    #UsedSpaceMB    = db_size.toPandas()['UsedSpaceMB'].unique()
    UsedSpaceMB = str(db_size.head()[0])
    #UsedSpaceMB = db_size.toPandas()['UsedSpaceMB']
    #UsedSpaceMB = UsedSpaceMB.to_string(index = False)
    XMLFilesCount    = xmlcount.toPandas()['XMLFilesCount'].unique()
    
    logger.warning('For each %s ingested XML files,the average database grows in size by %sMB',XMLFilesCount ,UsedSpaceMB)
            
