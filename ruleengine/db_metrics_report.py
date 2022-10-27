# Databricks notebook source
from datetime import datetime
import time
#import commands

def create_db_metrics_report(spark,config,logger):

    print("Creates db metrics report")

    """Creates db metrics for UI

    Parameters
    ----------
    Spark session object
    Logger
    Configuration file


    """
    #------------------------------------------------------------------------------------------------------------#
    #Logger info updated
    #------------------------------------------------------------------------------------------------------------#    
    
    logger.info("Creating dbmetrics report for database size  validation")

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

    
    db_size_report = spark.read.format("jdbc")\
    .option("url", db_settings['sqlserver'])\
    .option("query", """select * from 
       (SELECT 
           t.NAME AS TableName,
           s.Name AS SchemaName,
           p.rows,
           CAST(ROUND(((SUM(a.total_pages) * 8) / 1024.00), 2) AS NUMERIC(36, 2)) AS TotalSpaceMB,
           CAST(ROUND(((SUM(a.used_pages) * 8) / 1024.00), 2) AS NUMERIC(36, 2)) AS UsedSpaceMB, 
           CAST(ROUND(((SUM(a.total_pages) - SUM(a.used_pages)) * 8) / 1024.00, 2) AS NUMERIC(36, 2)) AS UnusedSpaceMB
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
           AND t.is_ms_shipped = 0
           AND i.OBJECT_ID > 255 
       	and s.Name='final'
       GROUP BY 
           t.Name, s.Name, p.Rows
       	)a
       
       	cross join
       
       	(select
                      HDR.*
                    , XMLFilesCount
                    from
                      (
                        SELECT
                          CAST(ReceiptdateTime AS DATE) AS BusinessDay,
                       count(case when TransactionType=2 then 1 end) as retail_transactions,
                       count(*) as Total_transactions
                       FROM
                            final.TransactionHeader HDR
                          group by
                            CAST(ReceiptdateTime AS DATE)
                        )
                        HDR
                        left join
                          (
                            SELECT
                            BusinessDate
                            , count(xmlfilename) as XMLFilesCount
                            FROM
                              final.XMLFiles
                            group by
                            BusinessDate
                          )
                          DLY
                          on
                             Businessday  = BusinessDate
       				where Businessday=CAST(CURRENT_TIMESTAMP-1 AS DATE)) m""")\
    .option("user", db_settings['username'])\
    .option("password", db_settings['password'])\
    .option("driver", db_settings['driver'])\
    .load()    

    #------------------------------------------------------------------------------------------------------------#
    #"gets date and time for filename"
    #------------------------------------------------------------------------------------------------------------#

    day  = datetime.now().strftime("%Y%m%d")
    hrmi = datetime.now().strftime("%H%M")

    #------------------------------------------------------------------------------------------------------------#
    #"Write siz report dataframe to parquet"
    #------------------------------------------------------------------------------------------------------------#

    start = time.time()
    
    db_size_report_1 = db_size_report.orderBy('UsedSpaceMB', ascending=False)
    db_size_report_1.coalesce(1).write.mode('overwrite').option("header","true").csv('{db_metrics_path}'.format(db_metrics_path=config["output"]["ruleengine"]["db_metrics_path"])) 

    #commands.getoutput('hadoop fs -cat {path_parquet}/*.csv > {local_folder}/DB_Metrics_{day}_{hrmi}.txt'.format(path_parquet=config["output"]["ruleengine"]["db_metrics_path"],day=day,hrmi=hrmi,local_folder=config["output"]["ruleengine"]["db_metrics_local_folder"]))    
    #------------------------------------------------------------------------------------------------------------#
    #Loggr info updated
    #------------------------------------------------------------------------------------------------------------#        
    
    logger.info("size report creation took %s seconds "%(time.time()-start))
