# Databricks notebook source
from __future__ import print_function
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from itertools import chain
# import lkp_tables_update as lkp_update


# -------------------------------------------------------------#
# Creating ruleengine_to_sql
# -------------------------------------------------------------#
def ruleengine_to_sql(spark, logger, epoch_id, config):
    """create db settings for all the tables
    
    return: None
    """

    logger.info('Copying RuleEngine tables to db')

    db_settings = {
        'sqlserver': "jdbc:sqlserver:{serverip}:{port};database={database}".format(
            serverip=config['output']['db_setting']['server_ip'], port=config['output']['db_setting']['port'],
            database=config['output']['db_setting']['database_name']),
        'username': "{username}".format(username=config['output']['db_setting']['user_name']),
        'password': "{password}".format(password=config['output']['db_setting']['password']),
        'driver': "{driver}".format(driver=config['output']['db_setting']['driver'])
    }

    lineitem = "{tablename}".format(tablename=config['output']['db_tables']['lineitem'])
    emptiesredemption = "{tablename}".format(tablename=config['output']['db_tables']['emptiesredemption'])
    pricelookup = "{tablename}".format(tablename=config['output']['db_tables']['pricelookup'])
    suspendedfraud = "{tablename}".format(tablename=config['output']['db_tables']['suspendedfraud'])
    receiptvoidingtable = "{tablename}".format(tablename=config['output']['db_tables']['receiptvoidingtable'])
    transactionheader = "{tablename}".format(tablename=config['output']['db_tables']['transactionheader'])

    database = config["input"]["hive"]["database"]
    lineitem_sql(spark, logger, epoch_id, db_settings, lineitem, config, database)
    emptiesredemption_sql(spark, logger, epoch_id, db_settings, emptiesredemption, database)
    pricelookup_sql(spark, logger, epoch_id, db_settings, pricelookup, database)
    suspendedfraud_sql(spark, logger, epoch_id, db_settings, suspendedfraud, database)
    receiptvoiding_sql(spark, logger, epoch_id, db_settings, receiptvoidingtable, database)
    transactionheader_sql(spark, logger, epoch_id, db_settings, transactionheader, database)


# -------------------------------------------------------------#
# Creating lineitem_sql
# -------------------------------------------------------------#
def lineitem_sql(spark, logger, epoch_id, db_settings, lineitem, config, database):
    """Copy LineItem df to db
    
    return: None
    """

    logger.info('Copying LineItem df to db')

    LineItemdfToSQL = spark.sql(
        "select retailstoreid, WorkStationID, ReceiptDateTime, ReceiptNumber, LineItemSequenceNumber, EntryMethod, "
        "VoidFlag, LineItemBeginDateTime, DirectVoidFlag, Amount, NegativeAmountFlag from {fraud}.lineitem".format(
            fraud=database))

    LineItemDF_dict = {"true": 1, "false": -1}
    mapping_expr = F.create_map([F.lit(x) for x in chain(*LineItemDF_dict.items())])

    LineItemdfToSQL = LineItemdfToSQL.withColumn('NegativeAmountFlag',
                                                 mapping_expr[LineItemdfToSQL['NegativeAmountFlag']])
    LineItemdfToSQL = LineItemdfToSQL.withColumn("Amount", F.col("Amount") * F.col("NegativeAmountFlag"))
    LineItemdfToSQL = LineItemdfToSQL.drop(F.col('NegativeAmountFlag'))

    # ----------------------------------------------------------------#
    # Query to compute the flag feature : VoidedIsLastItem and VoidedFollowup#
    # and Added VoidedFlagDerived column to be ingested in DB for UI
    # add it before database ingestion in sql server#
    # ----------------------------------------------------------------#

    spark.sql("""
        create or replace temporary view LineItem_Derived_Return_lnvoid as
    Select
     a.retailstoreid
    , a.WorkstationID
    , a.ReceiptNumber
    , a.ReceiptDateTime
    , a.LineItemSequenceNumber
    , a.voidflag
    , b.RegularSalesUnitPrice
    , a.EntryMethod
    , b.ExtendedAmount
    , b.MerchandiseHierarchyLevel
    , b.ReturnItemLink as ItemLink
    , b.POSItemID
    , a.voidflag as VoidFlagDerived
    ,'Return'    as SRType
    From
     {fraud}.LineItem a
     JOIN
      {fraud}.Return b
      on
       a.retailstoreid             =b.retailstoreid
       and a.WorkstationID         =b.WorkstationID
       and a.ReceiptNumber         =b.ReceiptNumber
       and a.ReceiptDateTime       =b.ReceiptDateTime
       and a.LineItemSequenceNumber=b.LineItemSequenceNumber""".format(fraud=database))

    spark.sql("""
    create or replace temporary view LineItem_Derived_sale_lnvoid as
Select
 a.retailstoreid
, a.WorkstationID
, a.ReceiptNumber
, a.ReceiptDateTime
, a.LineItemSequenceNumber
, a.voidflag
, b.RegularSalesUnitPrice
, a.EntryMethod
, b.ExtendedAmount
, b.MerchandiseHierarchyLevel
, b.SaleItemLink as ItemLink
, b.POSItemID
, a.voidflag as VoidFlagDerived
,'Sale'      as SRType
From
 {fraud}.LineItem a
 JOIN
  {fraud}.Sale b
  on
   a.retailstoreid             =b.retailstoreid
   and a.WorkstationID         =b.WorkstationID
   and a.ReceiptNumber         =b.ReceiptNumber
   and a.ReceiptDateTime       =b.ReceiptDateTime
   and a.LineItemSequenceNumber=b.LineItemSequenceNumber""".format(fraud=database))

    spark.sql("""
    create
or
replace temporary view LineItem_Derived_lnvoid as
select *
from
 LineItem_Derived_Return_lnvoid
union all
select *
from
 LineItem_Derived_sale_lnvoid""")

    spark.sql("""
    create or replace temporary view DF_LineItem_Derived_lnvoid as
select
 s.*
, case
  when q.VoidFlag = 'True'
   or s.VoidFlag  = 'True'
   then True
   else False
 end as VoidFlagDerived_final
from
 LineItem_Derived_lnvoid s
 left join
  (select distinct retailstoreid, WorkstationID, Receiptnumber, ReceiptDateTime,
ItemLink,VoidFlag from LineItem_Derived_lnvoid) q
  on
   s.retailstoreid              = q.retailstoreid
   and s.WorkstationID          = q.WorkstationID
   and s.ReceiptNumber          = q.ReceiptNumber
   and s.ReceiptDateTime        = q.ReceiptDateTime
   and s.LineItemSequenceNumber = q.ItemLink
   and q.VoidFlag               = 'True'""")

    spark.sql("""
    create or replace temporary view DF_Alllines_lnvoid as
select
 line.*
, to_date(line.ReceiptDateTime) as ReceiptDate
, dense_rank() over(partition by line.retailstoreid,line.WorkstationID,to_date(line.ReceiptDateTime) order by
                    line.ReceiptDateTime) as row_id
from
 DF_LineItem_Derived_lnvoid line""")

    spark.sql("""create or replace temporary view Cancel_lines as
    select *
from
 DF_Alllines_lnvoid
where
 VoidFlag = 'True'""")
    spark.sql("""create or replace temporary view Non_Cancel_lines as 
    select *
from
 DF_Alllines_lnvoid
where
 VoidFlagDerived_final = 'False'""")

    spark.sql("""create or replace temporary view Followup_lines as 
    select distinct
 *
from
 (
  select
   prim.*
  , case
    when prim.EntryMethod=0
     then
     case
      when prim.POSItemID=follow.POSItemID
       and prim.SRType   =follow.SRType
       then 1
     end
    when prim.EntryMethod=1
     then
     case
      when prim.MerchandiseHierarchyLevel=follow.MerchandiseHierarchyLevel
       and prim.SRType                   =follow.SRType
       then 1
     end
   end as item_match
  from
   Cancel_lines prim
   join
    Non_Cancel_lines follow
    on
     follow.retailstoreid    =prim.retailstoreid
     and follow.WorkstationID=prim.WorkstationID
     and follow.ReceiptDate  =prim.ReceiptDate
     and follow.row_id       =(prim.row_id+1)
 )
 der
where
 item_match=1""")

    spark.sql("""create or replace temporary view SL_RT_Union as
    select
 retailstoreid
, WorkStationID
, ReceiptNumber
, ReceiptDateTime
, LineItemSequenceNumber
, ReturnItemLink as ItemLink
from
 {fraud}.Return
union
select
 retailstoreid
, WorkStationID
, ReceiptNumber
, ReceiptDateTime
, LineItemSequenceNumber
, SaleItemLink as ItemLink
from
 {fraud}.Sale""".format(fraud=database))

    LineItemDF = spark.sql("""select
 LI.retailstoreid
, LI.WorkStationID
, LI.ReceiptDateTime
, LI.ReceiptNumber
, LI.LineItemSequenceNumber
, LI.EntryMethod
, LI.VoidFlag
, LI.LineItemBeginDateTime
, LI.DirectVoidFlag
, LI.Amount
, CASE
  WHEN SL_RT.retailstoreid is NULL
   or LI.VoidFlag                ='False'
   then NULL
  WHEN LI.VoidFlag='True'
   and (
    SL_RT_MX.LastSeq=LI.LineItemSequenceNumber
   )
  THEN 1
  WHEN LI.VoidFlag='True'
   then 0
 END AS VoidedIsLastItem
, case
  when LI.VoidFlag              ='False'
   or LI.VoidFlag         is NULL
   or SL_RT.retailstoreid is NULL
   then NULL
  when FL.LineItemSequenceNumber is NULL
   THEN 0
  when FL.LineItemSequenceNumber is NOT NULL
   THEN 1
 END                        as VoidedIsInFollowup
, LIV.VoidFlagDerived_final as VoidFlagDerived
from
 {fraud}.LineItem LI
 left join
  (
   select
    retailstoreid
   , WorkStationID
   , ReceiptNumber
   , ReceiptDateTime
   , max(LineItemSequenceNumber) as LastSeq
   , max(ItemLink)               as LastItemLink
   from
    SL_RT_Union
   group by
    retailstoreid
   , WorkstationID
   , ReceiptNumber
   , ReceiptDateTime
  )
  SL_RT_MX
  on
   LI.retailstoreid       = SL_RT_MX.retailstoreid
   and LI.WorkstationID   = SL_RT_MX.WorkstationID
   and LI.ReceiptNumber   = SL_RT_MX.ReceiptNumber
   and LI.ReceiptDateTime = SL_RT_MX.ReceiptDateTime
 left join
  Followup_lines FL
  on
   LI.retailstoreid             = FL.retailstoreid
   and LI.WorkstationID         = FL.WorkstationID
   and LI.ReceiptNumber         = FL.ReceiptNumber
   and LI.ReceiptDateTime       = FL.ReceiptDateTime
   and LI.LineItemSequenceNumber=FL.LineItemSequenceNumber
 left join
  SL_RT_Union SL_RT
  on
   LI.retailstoreid             = SL_RT.retailstoreid
   and LI.WorkstationID         = SL_RT.WorkstationID
   and LI.ReceiptNumber         = SL_RT.ReceiptNumber
   and LI.ReceiptDateTime       = SL_RT.ReceiptDateTime
   and LI.LineItemSequenceNumber=SL_RT.LineItemSequenceNumber
 left join
  DF_LineItem_Derived_lnvoid LIV
  on
   LI.retailstoreid             = LIV.retailstoreid
   and LI.WorkstationID         = LIV.WorkstationID
   and LI.ReceiptNumber         = LIV.ReceiptNumber
   and LI.ReceiptDateTime       = LIV.ReceiptDateTime
   and LI.LineItemSequenceNumber=LIV.LineItemSequenceNumber""".format(fraud=database))

    spark.catalog.dropTempView('LineItem_Derived_Return_lnvoid')
    spark.catalog.dropTempView('LineItem_Derived_sale_lnvoid')
    spark.catalog.dropTempView('LineItem_Derived_lnvoid')
    spark.catalog.dropTempView('DF_LineItem_Derived_lnvoid')
    spark.catalog.dropTempView('DF_Alllines_lnvoid')
    spark.catalog.dropTempView('Cancel_lines')
    spark.catalog.dropTempView('Non_Cancel_lines')
    spark.catalog.dropTempView('Followup_lines')
    spark.catalog.dropTempView('SL_RT_Union')

    LineItemDF = LineItemDF.withColumnRenamed('retailstoreid', 'RetailStoreID')

    # EntryMethod_dict = {"Scanned":0, "Keyed":1}

    # updating entrymethod
    entrymethod = LineItemDF.select('EntryMethod').distinct()
    entrymethod = entrymethod.filter(F.col('EntryMethod').isNotNull())
    entrymethod = entrymethod.withColumnRenamed('EntryMethod', 'EntryMethodDesc')

    # reading lkp_table entrymethod from lkp_tables_updates
    entrymethod_df = lkp_update.read_lkp_entrymethod(spark, logger, config)
    entrymethod_df = entrymethod_df.join(entrymethod, ['EntryMethodDesc'], 'full')
    entrymethod_df = entrymethod_df.select('EntryMethod', 'EntryMethodDesc')

    _max = entrymethod_df.agg({"EntryMethod": "max"}).collect()[0]['max(EntryMethod)']

    if _max is None:
        _max = 1
    else:
        _max = _max + 1

    w = Window.orderBy('EntryMethod')

    entrymethod_df = entrymethod_df.withColumn('EntryMethod', F.when(F.col('EntryMethod') >= 0, F.col('EntryMethod'))
                                               .otherwise((F.row_number().over(w) - 1) + _max))

    # updating lkp_table entrymethod in lkp_tables_updates
    lkp_update.update_lkp_entrymethod(spark, logger, entrymethod_df, config)

    EntryMethod_dict = dict(entrymethod_df.rdd.map(lambda x: (x['EntryMethodDesc'], x['EntryMethod'])).collect())

    mapping_expr = F.create_map([F.lit(x) for x in chain(*EntryMethod_dict.items())])
    LineItemDF = LineItemDF.withColumn('EntryMethod', mapping_expr[LineItemDF['EntryMethod']])

    LineItemDF.write.mode("overwrite").format("jdbc") \
        .option("url", db_settings['sqlserver']) \
        .option("dbtable", lineitem) \
        .option("user", db_settings['username']) \
        .option("password", db_settings['password']) \
        .option("driver", db_settings['driver']) \
        .save()


# -------------------------------------------------------------#
# Creating emptiesredemption_sql
# -------------------------------------------------------------#
def emptiesredemption_sql(spark, logger, epoch_id, db_settings, emptiesredemption, database):
    """Copy Empties_Redemption df to db
    
    return: None
    """

    logger.info('Copying Empties_Redemption df to db')

    EmptiesRedemptiondfToSQL = spark.sql(
        "select retailstoreid as RetailStoreID , WorkstationID, ReceiptDateTime, ReceiptNumber, EmptiesStoreID, "
        "EmptiesWorkStationID, EmptiesReceiptNumber, EmptiesTotal, EmployeeID, FLAG, Concatenated_Empties_ID, "
        "class_id, LineItemSequenceNumber, IsVoided, EmptiesTimingFlag from {fraud}.df_empties_redemption".format(
            fraud=database))

    EmptiesRedemptiondfToSQL.write.mode("overwrite").format("jdbc") \
        .option("url", db_settings['sqlserver']) \
        .option("dbtable", emptiesredemption) \
        .option("user", db_settings['username']) \
        .option("password", db_settings['password']) \
        .option("driver", db_settings['driver']) \
        .save()


# -------------------------------------------------------------#
# Creating pricelookup_sql
# -------------------------------------------------------------#
def pricelookup_sql(spark, logger, epoch_id, db_settings, pricelookup, database):
    """Copy PriceLookup df to db
    
    return: None
    """

    logger.info('Copying PriceLookup df to db')

    PriceLookUpdfToSQL = spark.sql(
        "select retailstoreid as RetailStoreID, WorkstationID, ReceiptDateTimeFL, ReceiptDateTimeAT, ReceiptNumberAT, "
        "ReceiptNumberFL, EmployeeID, PriceLookupFlag, EAN, ActualPrice, ArticleDescription, "
        "MerchandiseHierarchyLevel, MerchandiseHierachyDescription, TransactionGrandAmount, StoreInfoID, IsEBUS from "
        "{fraud}.df_pricelookup".format(
            fraud=database))

    PriceLookUpdfToSQL.write.mode("overwrite").format("jdbc") \
        .option("url", db_settings['sqlserver']) \
        .option("dbtable", pricelookup) \
        .option("user", db_settings['username']) \
        .option("password", db_settings['password']) \
        .option("driver", db_settings['driver']) \
        .save()


# -------------------------------------------------------------#
# Creating suspendedfraud_sql
# -------------------------------------------------------------#
def suspendedfraud_sql(spark, logger, epoch_id, db_settings, suspendedfraud, database):
    """Copy SuspendedFraud df to db
    
    return: None
    """

    logger.info('Creating SuspendedFraud df to db')

    SuspendedFraudfToSQL = spark.sql(
        "select retailstoreid_suspended, WorkStationID_suspended, ReceiptDateTime_suspended, ReceiptNumber_suspended, "
        "TransactionStatus_suspended, class_id_suspended, EmployeeID_suspended, Total_suspended, "
        "RetailStoreID_retrieved, WorkStationID_retrieved, ReceiptDateTime_retrieved, ReceiptNumber_retrieved, "
        "TransactionStatus_retrieved, TransactionStatus_derived_retrived, class_id_retrived, Suspended_Flag from "
        "{fraud}.df_suspendedfraud".format(fraud=database))

    SuspendedFraudfToSQL = SuspendedFraudfToSQL.withColumnRenamed('retailstoreid_suspended', 'RetailStoreID_suspended')

    SuspendedFraudfToSQL = SuspendedFraudfToSQL.dropDuplicates(
        ['RetailStoreID_suspended', 'WorkStationID_suspended', 'ReceiptDateTime_suspended', 'ReceiptNumber_suspended'])

    SuspendedFraudfToSQL.write.mode("overwrite").format("jdbc") \
        .option("url", db_settings['sqlserver']) \
        .option("dbtable", suspendedfraud) \
        .option("user", db_settings['username']) \
        .option("password", db_settings['password']) \
        .option("driver", db_settings['driver']) \
        .save()


# -------------------------------------------------------------#
# Creating receiptvoiding_sql
# -------------------------------------------------------------#
def receiptvoiding_sql(spark, logger, epoch_id, db_settings, receiptvoidingtable, database):
    """Copy ReceiptVoidingTable df to db
    
    return: None
    """

    logger.info('Copying ReceiptVoiding df to db')

    ReceiptVodingdfToSQL = spark.sql(
        "select retailstoreid_1, WorkstationID_1, SequenceNumber_1, ReceiptDateTime_1, ReceiptNumber_1, "
        "RetailStoreID_2, WorkstationID_2, SequenceNumber_2, ReceiptDateTime_2, ReceiptNumber_2, BonStornoFlag, "
        "TransactionClassID_1, TransactionClassDescr_1, TransactionClassID_2, TransactionClassDescr_2, "
        "TransactionGrandAmount, NegativeTotalFlag, Approver from {fraud}.df_receiptvoidingtable".format(
            fraud=database))

    ReceiptVodingdfToSQL = ReceiptVodingdfToSQL.withColumnRenamed('retailstoreid_1', 'RetailStoreID_1')

    ReceiptVodingdfToSQL.write.mode("overwrite").format("jdbc") \
        .option("url", db_settings['sqlserver']) \
        .option("dbtable", receiptvoidingtable) \
        .option("user", db_settings['username']) \
        .option("password", db_settings['password']) \
        .option("driver", db_settings['driver']) \
        .save()


# -------------------------------------------------------------#
# Creating transactionheader_sql
# -------------------------------------------------------------#
def transactionheader_sql(spark, logger, epoch_id, db_settings, transactionheader, database):
    """Copy TransactionHeader df to db
    
    return: None
    """

    logger.info('Copying TransactionHeader df to db')

    TransactionHeader = spark.sql("select * from {fraud}.transactionheader".format(fraud=database))
    TransactionHeader = TransactionHeader.na.fill({'FunctionLogStoreInfoID': 0})

    TransactionType_dict = {"ControlTransaction": 0, "FunctionLog": 1, "RetailTransaction": 2,
                            "TenderControlTransaction": 3}
    mapping_expr = F.create_map([F.lit(x) for x in chain(*TransactionType_dict.items())])
    TransactionHeader = TransactionHeader.withColumn('TransactionType1',
                                                     mapping_expr[TransactionHeader['TransactionType']])

    Classification_Description = spark.sql("select * from {fraud}.df_classification_description".format(fraud=database))
    Classification_Description = Classification_Description.na.fill({'FunctionLogStoreInfoID': 0})

    TransactionHeaderdfToSQL = TransactionHeader.join(Classification_Description,
                                                      ['retailstoreid', 'WorkstationID', 'SequenceNumber',
                                                       'ReceiptDateTime', 'TransactionType',
                                                       'FunctionLogStoreInfoID']).select(
        TransactionHeader.RetailStoreID, TransactionHeader.WorkstationID, TransactionHeader.SequenceNumber,
        TransactionHeader.ReceiptNumber, TransactionHeader.ReceiptDateTime, TransactionHeader.BusinessDay,
        TransactionHeader.FunctionLogStoreInfoID, TransactionHeader.TransactionType1.alias('TransactionType'),
        TransactionHeader.BeginDateTime, TransactionHeader.EndDateTime,
        TransactionHeader.CancelFlag, TransactionHeader.TrainingModeFlag, TransactionHeader.EmployeeID,
        TransactionHeader.CurrencyCode,
        TransactionHeader.ApproverEmployeeID, Classification_Description.class_id, TransactionHeader.xmlfilename)

    TransactionHeaderdfToSQL.createOrReplaceTempView("TransactionHeaderdfToSQL")

    # Rule Engine logic to extract the next transaction or the followup transaction for
    # an employee for the same day. For the last the followup, mark it as null

    TransactionHeaderdfToSQL1 = spark.sql("""select
   orig.*
  , followup.*
from
 (
 (
select TH.*
  , row_number() over(partition by th.retailstoreid,th.employeeid,to_date(th.ReceiptDateTime) order by
                      th.ReceiptDateTime,th.FunctionLogStoreInfoID) as row_id
  from
   (select * from TransactionHeaderdfToSQL where class_id is not null) TH
 )
 orig
 left join
  (
   select
    th_f.RetailStoreID          as RetailStoreID_Followup
   , th_f.WorkStationID         as WorkStationID_Followup
   , th_f.ReceiptNumber         as ReceiptNumber_Followup
   , th_f.ReceiptDateTime       as ReceiptDatetIme_Followup
   , th_f.FunctionLogStoreInfoID as FunctionLogStoreInfoID_Followup
   , th_f.EmployeeID             as EmployeeID_Followup
   , RTH.TransactionGrandAmount as TransactionGrandAmount_Followup
   ,th_f.class_id               as class_id_Followup
   , row_number() over(partition by th_f.retailstoreid,th_f.employeeid,to_date(th_f.ReceiptDateTime) order by
                       th_f.ReceiptDateTime,th_f.FunctionLogStoreInfoID) as row_id_followup
   from
    (select * from TransactionHeaderdfToSQL where class_id is not null)TH_f
    left join
     {fraud}.RetailTransactionHeader RTH
     on
      TH_f.RetailStoreID      =RTH.RetailStoreID
      and TH_f.WorkStationID  =RTH.WorkStationID
      and TH_f.ReceiptDateTime=RTH.ReceiptDateTime
      and TH_f.ReceiptNumber  =RTH.ReceiptNumber
     and TH_f.TransactionType='2'
  )
  followup
  on
   orig.retailstoreid                              =followup.retailstoreid_Followup
   and orig.employeeid                             =followup.EmployeeID_Followup
   and to_date(orig.ReceiptDateTime)            = to_date(followup.ReceiptDateTime_Followup)
   and orig.row_id+1                               =followup.row_id_followup 
   )
   union


   select 
        th_null.*
        ,null as row_id
        ,null as RetailStoreID_Followup
        ,null  as WorkStationID_Followup
        ,null as ReceiptNumber_Followup
        ,null as ReceiptDatetIme_Followup
        ,null as FunctionLogStoreInfoID_Followup
        ,null as EmployeeID_Followup
        , null as TransactionGrandAmount_Followup
        ,null as class_id_Followup
        ,null as row_id_followup

   from 
   TransactionHeaderdfToSQL TH_null
   where TH_null.class_id is null
   """.format(fraud=database))

    TransactionHeaderdfToSQL2 = TransactionHeaderdfToSQL1.drop('row_id')
    TransactionHeaderdfToSQL3 = TransactionHeaderdfToSQL2.drop('row_id_followup', 'EmployeeID_Followup')
    #TransactionHeaderdfToSQL = TransactionHeaderdfToSQL3.withColumnRenamed('retailstoreid', 'RetailStoreID')
    # TransactionHeaderdfToSQL = TransactionHeaderdfToSQL.withColumnRenamed('businessday', 'BusinessDay')

    TransactionHeaderdfToSQL3.write.mode("overwrite").format("jdbc") \
        .option("url", db_settings['sqlserver']) \
        .option("dbtable", transactionheader) \
        .option("user", db_settings['username']) \
        .option("password", db_settings['password']) \
        .option("driver", db_settings['driver']) \
        .save()
