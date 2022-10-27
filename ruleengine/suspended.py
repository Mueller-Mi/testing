# Databricks notebook source
import time
def create_suspended_fraud_dataframe(spark,epocid,logger,config):


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
    #Logger info updated
    #------------------------------------------------------------------------------------------------------------
    
    logger.info("Creating Suspended Fraud Dataframe")    

    #------------------------------------------------------------------------------------------------------------#
    #Variable for parqut path  created
    #------------------------------------------------------------------------------------------------------------
    
    path     = config["output"]["ruleengine"]["SuspendedFraud_folder"]
    database = config["input"]["hive"]["database"]

    #------------------------------------------------------------------------------------------------------------#    
    #Suspened data frame creation
    #------------------------------------------------------------------------------------------------------------#
    
    
    DF_SuspendedFraud = spark.sql("""
        select
     Susp.retailstoreid                                            as retailstoreid_suspended
    , Susp.WorkStationID                                           as WorkStationID_suspended
    , Susp.ReceiptDateTime                                         as ReceiptDateTime_suspended
    , Susp.ReceiptNumber                                           as ReceiptNumber_suspended
    , Susp.TransactionStatus                                       as TransactionStatus_suspended
    , Susp.class_id                                                as class_id_suspended
    , TH.EmployeeID                                                as EmployeeID_suspended
    , Susp.TransactionGrandAmount                                  as Total_suspended
    , Susp_R.retailstoreid                                         as retailstoreid_retrieved
    , Susp_R.WorkStationID                                         as WorkStationID_retrieved
    , Susp_R.ReceiptDateTime                                       as ReceiptDateTime_retrieved
    , Susp_R.ReceiptNumber                                         as ReceiptNumber_retrieved
    , Susp_R.TransactionStatus                                     as TransactionStatus_retrieved
    , coalesce(Susp_R.TransactionStatus1,Susp_R.TransactionStatus) as TransactionStatus_derived_retrived
    , Susp_R.class_id                                              as class_id_retrived
    , Case
      when ER.retailstoreid is not null
       and Susp_R.TransactionStatus1  ='Totaled'
       then 0
      when ER.retailstoreid is not null
       and (
        Susp_R.TransactionStatus1         != 'Totaled'
        or Susp_R.TransactionStatus1 is null
       )
       then 1
      when ER.retailstoreid   is null
       and Susp_R.TransactionStatus1='Totaled'
       then 2
      when ER.retailstoreid is null
       and (
        Susp_R.TransactionStatus1         !='Totaled'
        or Susp_R.TransactionStatus1 is null
       )
       then 3
     End as Suspended_Flag
    from
     (
      select
       der1.*
      , der2.TransactionStatus
      , der2.TransactionGrandAmount
      , CD.class_id
      from
       {fraud}.ReceiptHeaderAddonList der1
       left join
        {fraud}.RetailTransactionHeader der2
        on
         der1.retailstoreid      =der2.retailstoreid
         and der1.WorkstationID  =der2.WorkstationID
         and der1.ReceiptDateTime=der2.ReceiptDateTime
         and der1.ReceiptNumber  =der2.ReceiptNumber
       left join
        {fraud}.df_classification_description CD
        on
         der1.retailstoreid      =CD.retailstoreid
         and der1.WorkStationID  =CD.WorkStationID
         and der1.SequenceNumber =CD.SequenceNumber
         and der1.ReceiptDateTime=CD.ReceiptDateTime
         and CD.TransactionType  ='RetailTransaction'
      where
       der1.AddonKey='PARK_NUMBER'
     )
     Susp
     left join
      (
       select
        Ret_Comb.*
       , RHA_2.TransactionStatus1
       , CD.class_id
       from
        (
         select
          der1.retailstoreid
         , der1.WorkstationID
         , der1.SequenceNumber
         , der1.ReceiptDateTime
         , max(der1.AddonValue) as AddonValue
         , der1.ReceiptNumber
         , der1.AddonKey
         , der2.TransactionStatus
         from
          {fraud}.ReceiptHeaderAddonList der1
          left join
           {fraud}.RetailTransactionHeader der2
           on
            der1.retailstoreid      =der2.retailstoreid
            and der1.WorkstationID  =der2.WorkstationID
            and der1.ReceiptDateTime=der2.ReceiptDateTime
            and der1.ReceiptNumber  =der2.ReceiptNumber
         group by
          der1.retailstoreid
         , der1.WorkstationID
         , der1.SequenceNumber
         , der1.ReceiptNumber
         , der1.ReceiptDateTime
         , AddonKey
         , der2.TransactionStatus
         having
          der1.AddonKey              ='SERVER_PARKING_FETCHED_NUMBER'
          and der2.TransactionStatus!='PostVoided'
        )
        Ret_Comb
        left join
         {fraud}.df_classification_description CD
         on
          Ret_Comb.retailstoreid      =CD.retailstoreid
          and Ret_Comb.WorkStationID  =CD.WorkStationID
          and Ret_Comb.SequenceNumber =CD.SequenceNumber
          and Ret_Comb.ReceiptDateTime=CD.ReceiptDateTime
          and CD.TransactionType      ='RetailTransaction'
        left join
         (
          select
           RHA.*
          , RTH.TransactionStatus as TransactionStatus1
          from
           {fraud}.ReceiptHeaderAddonList RHA
           inner join
            {fraud}.RetailTransactionHeader RTH
            on
             RHA.retailstoreid        =RTH.retailstoreid
             and RHA.WorkStationID    =RTH.WorkStationID
             and RHA.SequenceNumber   =RTH.SequenceNumber
             and RHA.ReceiptDateTime  =RTH.ReceiptDateTime
             and RHA.AddonKey         ='SERVER_PARKING_FETCHED_NUMBER'
             and RTH.TransactionStatus='Totaled'
         )
         RHA_2
         on
          Ret_Comb.retailstoreid               =RHA_2.retailstoreid
          and to_date(Ret_Comb.ReceiptDateTime)= to_date(RHA_2.ReceiptDateTime)
          and Ret_Comb.AddonValue              =RHA_2.AddonValue
      )
      Susp_R
      on
       Susp.retailstoreid               =Susp_R.retailstoreid
       and Susp.ReceiptNumber          !=Susp_R.ReceiptNumber
       and Susp.AddonValue              =Susp_R.AddonValue
       and to_date(Susp.ReceiptDateTime)=to_date(Susp_R.ReceiptDateTime)
     left join
      {fraud}.TransactionHeader TH
      on
       Susp.retailstoreid      =TH.retailstoreid
       and Susp.WorkStationID  =TH.WorkStationID
       and Susp.SequenceNumber =TH.SequenceNumber
       and Susp.ReceiptDateTime=TH.ReceiptDateTime
       and TH.TransactionType  ='RetailTransaction'
     left join
      {fraud}.RetailTransactionHeader RT
      on
       Susp_R.retailstoreid      =RT.retailstoreid
       and Susp_R.WorkStationID  =RT.WorkStationID
       and Susp_R.SequenceNumber =RT.SequenceNumber
       and Susp_R.ReceiptDateTime=RT.ReceiptDateTime
     left join
      (
       select distinct
        retailstoreid
       , WorkStationID
       , ReceiptNumber
       , ReceiptDateTime
       from
        {fraud}.df_empties_redemption
      )
      ER
      on
       Susp.retailstoreid      =ER.retailstoreid
       and Susp.WorkStationID  =ER.WorkStationID
       and Susp.ReceiptNumber  =ER.ReceiptNumber
       and Susp.ReceiptDateTime=ER.ReceiptDateTime
    """.format(fraud=database))

    #---------------------------------------------------------------------------------------------------------#
    #Writes into parquet and alter hive table
    #---------------------------------------------------------------------------------------------------------#
    
    start = time.time()
    if config['input']['file_type']['file_type']=='parquet':
        DF_SuspendedFraud.coalesce(10).write.partitionBy('retailstoreid_suspended').mode('overwrite').parquet('{path}/{epocid_r}'.format(epocid_r=epocid,path=path))          
    
        drppart=','.join(['partition('+ part +')' for part in map(lambda x:str(x[0]),spark.sql("show partitions {fraud}.df_suspendedfraud".format(fraud=database)).collect())])   

        if len(drppart) != 0:        
            spark.sql("alter table {fraud}.df_suspendedfraud drop if exists ".format(fraud=database)+drppart)   
            
        spark.sql("""alter table {fraud}.df_suspendedfraud set location '{path}/{epocid_r}'""".format(epocid_r=epocid,path=path,fraud=database))
        
        spark.sql("msck repair table {fraud}.df_suspendedfraud".format(fraud=database))
        
    else:
        logger.info("writing delta")
        
        DF_SuspendedFraud.coalesce(10).write.partitionBy('retailstoreid_suspended').mode('overwrite').option("overwriteSchema", "true").format("delta").save('{path}/{epocid_r}'.format(epocid_r=epocid,path=path))
        
        spark.sql("""create table if not exists {fraud}.df_suspendedfraud location '{path}/{epocid_r}'""".format(epocid_r=epocid,path=path,fraud=database))
        spark.sql("""alter table {fraud}.df_suspendedfraud set location '{path}/{epocid_r}'""".format(epocid_r=epocid,path=path,fraud=database))
        
        

    #------------------------------------------------------------------------------------------------------------#
    #Logger info updated
    #------------------------------------------------------------------------------------------------------------
    logger.info("Suspended parquet written in path : %s",'{path}/{epocid_r}'.format(epocid_r=epocid,path=path))
    
    suspended_count = spark.sql("""select Suspended_Flag,count(*) as count from {fraud}.df_suspendedfraud
                              group by Suspended_Flag""".format(fraud=database))
    suspended_count1=suspended_count.toPandas()
    
    logger.debug(suspended_count1.to_string(index = False)) 
