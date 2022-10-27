# Databricks notebook source
import time
def create_line_item_derived_dataframe(spark,epocid,logger,config):
    
    """Creates Flags_Rules_Classification Flags Dataframe 
    Parameters
    ----------
    
    Spark session object
    Logger
    Configuration file
    epocid
    
    Returns
    -------
    Spark DataFrame
    """

    #------------------------------------------------------------------------------------------------------------#
    #Variable for parqut path  created
    #------------------------------------------------------------------------------------------------------------

    path     = config["output"]["ruleengine"]["Lineitem_derived_folder"]
    database = config["input"]["hive"]["database"]

    #------------------------------------------------------------------------------------------------------------#    
    #Get the list of stores and display in the logger
    #------------------------------------------------------------------------------------------------------------#
    
    df_storeslog = spark.sql('select distinct retailstoreid from {fraud}.controltransaction'.format(fraud=database))
    storeslog    = df_storeslog.toPandas()['retailstoreid'].unique()
    
    logger.info("Rule Engine Script Execution Begin for Epocid %s",epocid)
    logger.info("Rule Engine Script Execution Begin for RetailStoreID")
    
    logger.info(storeslog)    
    logger.info("Creating LineItem Derived Dataframe")

    #------------------------------------------------------------------------------------------------------------#    
    #Lineitem derived data frame creation
    #------------------------------------------------------------------------------------------------------------#
    
    spark.sql("""create or replace temporary view LineItem_Derived_return as Select
       a.retailstoreid
     , a.WorkstationID
     , a.SequenceNumber
     , a.ReceiptDateTime
     , a.LineItemSequenceNumber
     , a.voidflag
     , b.ReturnItemLink as ItemLink
      From
       {fraud}.LineItem a
       JOIN
              {fraud}.Return b
              on
                     a.retailstoreid             =b.retailstoreid
                     and a.WorkstationID         =b.WorkstationID
                     and a.SequenceNumber        =b.SequenceNumber
                     and a.ReceiptDateTime       =b.ReceiptDateTime
                     and a.LineItemSequenceNumber=b.LineItemSequenceNumber""".format(fraud=database))
    
    spark.sql("""create or replace temporary view LineItem_Derived_sale as Select
       a.retailstoreid
     , a.WorkstationID
     , a.SequenceNumber
     , a.ReceiptDateTime
     , a.LineItemSequenceNumber
     , a.voidflag
     , b.SaleItemLink as ItemLink
      From
       {fraud}.LineItem a
       JOIN
              {fraud}.Sale b
              on
                     a.retailstoreid             =b.retailstoreid
                     and a.WorkstationID         =b.WorkstationID
                     and a.SequenceNumber        =b.SequenceNumber
                     and a.ReceiptDateTime       =b.ReceiptDateTime
                     and a.LineItemSequenceNumber=b.LineItemSequenceNumber""".format(fraud=database))
        
    spark.sql("""
    create or replace temporary view TB_LineItem_Derived_temp as
    select a.* from 
    (select * from LineItem_Derived_return
    union all select * from LineItem_Derived_sale) a""") 

    
    spark.sql("""create or replace temporary view TB_LineItem_Derived as select
          s.*
        , case
                    when q.VoidFlag         = 'true'
                              or s.VoidFlag = 'true'
                              then true
                              else false
          end as VoidFlagDerived
       from
          TB_LineItem_Derived_temp s
          left join
                    (select distinct retailstoreid, WorkstationID, SequenceNumber, ReceiptDateTime,
ItemLink,VoidFlag from TB_LineItem_Derived_temp) q
                    on
                              s.retailstoreid              = q.retailstoreid
                              and s.WorkstationID          = q.WorkstationID
                              and s.SequenceNumber         = q.SequenceNumber
                              and s.ReceiptDateTime        = q.ReceiptDateTime
                              and s.LineItemSequenceNumber = q.ItemLink
                              and q.VoidFlag               = 'true'""")
    
    lineitemderived=spark.sql("""
    select
    s.*
    ,q.VoidFlagDerived
    from
    {fraud}.LineItem s
    left join
    TB_LineItem_Derived q on
                                    s.retailstoreid              = q.retailstoreid
                                    and s.WorkstationID          = q.WorkstationID
                                    and s.SequenceNumber         = q.SequenceNumber
                                    and s.ReceiptDateTime        = q.ReceiptDateTime
                                    and s.LineItemSequenceNumber = q.LineItemSequenceNumber""".format(fraud=database))

    #---------------------------------------------------------------------------------------------------------#
    #Writes into parquet or delta and alter hive table
    #---------------------------------------------------------------------------------------------------------#
    start = time.time()
    if config['input']['file_type']['file_type']=='parquet':
        logger.info("writing parquet")
    
        lineitemderived.coalesce(10).write.partitionBy('retailstoreid').mode('overwrite').parquet('{path}/{epocid_r}'.format(epocid_r=epocid,path=path))

        drppart=','.join(['partition('+ part +')' for part in map(lambda x:str(x[0]),spark.sql("show partitions {fraud}.lineitemderived".format(fraud=database)).collect())])  

        if len(drppart) != 0:        
            spark.sql("alter table {fraud}.lineitemderived drop if exists ".format(fraud=database)+drppart)   
            
        spark.sql("""alter table {fraud}.lineitemderived set location '{path}/{epocid_r}'""".format(epocid_r=epocid,path=path,fraud=database))
        
        spark.sql("msck repair table {fraud}.lineitemderived".format(fraud=database))        
    
    else:
        logger.info("writing delta")
        
        lineitemderived.coalesce(10).write.partitionBy('retailstoreid').mode('overwrite').option("overwriteSchema", "true").format("delta").save('{path}/{epocid_r}'.format(epocid_r=epocid,path=path))
        
        spark.sql("""create table if not exists {fraud}.lineitemderived location '{path}/{epocid_r}'""".format(epocid_r=epocid,path=path,fraud=database))
        spark.sql("""alter table {fraud}.lineitemderived set location '{path}/{epocid_r}'""".format(epocid_r=epocid,path=path,fraud=database))
        
    #------------------------------------------------------------------------------------------------------------#
    #Logger info updated
    #------------------------------------------------------------------------------------------------------------
    
    logger.info("LineItem derived function took %s seconds "%(time.time()-start))

    #------------------------------------------------------------------------------------------------------------#
    #Drop temp tables
    #------------------------------------------------------------------------------------------------------------
    
    spark.catalog.dropTempView('TB_LineItem_Derived')
    spark.catalog.dropTempView('TB_LineItem_Derived_temp')

