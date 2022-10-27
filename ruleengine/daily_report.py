# Databricks notebook source
import time

def create_daily_report(spark,config,logger):

    #print("Creates daily report for UI")

    """Creates daily report for UI

    Parameters
    ----------
    Spark session object
    Logger
    Configuration file


    """
    #------------------------------------------------------------------------------------------------------------#
    #Logger info updated
    #------------------------------------------------------------------------------------------------------------#    
    
    logger.info("Creating daily report for UI")

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

    
    daily_tlp = spark.read.format("jdbc")\
    .option("url", db_settings['sqlserver'])\
    .option("query", """select
               HDR.*
             , XMLFilesCount
             , avg(retail_transactions) over(partition by HDR.RetailStoreID,datename(weekday,Businessday) order by
                                             Businessday asc rows between 7 preceding and current row ) as Average_Retail_trn
             from
               (
                 SELECT
                   CAST(ReceiptdateTime AS DATE) AS BusinessDay
                 , HDR.RetailStoreID,
                count(case when TransactionType=0 then 1 end) as control_transactions,
                count(case when TransactionType=1 then 1 end) as functionlog_transactions,
                count(case when TransactionType=2 then 1 end) as retail_transactions,
                count(case when TransactionType=3 then 1 end) as tender_transactions
                FROM
                     final.TransactionHeader HDR
                   group by
                     CAST(ReceiptdateTime AS DATE)
                   , HDR.RetailStoreID
                 )
                 HDR
                 left join
                   (
                     SELECT
                       RetailStoreID
                     , BusinessDate
                     , count(xmlfilename) as XMLFilesCount
                     FROM
                       final.XMLFiles
                     group by
                       RetailStoreID
                     , BusinessDate
                   )
                   DLY
                   on
                     HDR.RetailStoreID=DLY.RetailStoreID
                     and Businessday  = BusinessDate""")\
    .option("user", db_settings['username'])\
    .option("password", db_settings['password'])\
    .option("driver", db_settings['driver'])\
    .load()    

    #------------------------------------------------------------------------------------------------------------#
    #"Write daily report dataframe to database"
    #------------------------------------------------------------------------------------------------------------#

    start = time.time()
    
    daily_tlp.write.mode("overwrite").format("jdbc")\
    .option("url", db_settings['sqlserver'])\
	.option("dbtable", "final.store_day_tuples")\
    .option("user", db_settings['username'])\
    .option("password", db_settings['password'])\
    .option("driver", db_settings['driver'])\
	.save()

    #------------------------------------------------------------------------------------------------------------#
    #Loggr info updated
    #------------------------------------------------------------------------------------------------------------#        
    
    logger.info("Daily report creation took %s seconds "%(time.time()-start))
