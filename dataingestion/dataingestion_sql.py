# Databricks notebook source
from __future__ import print_function
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from itertools import chain
try:
  store = dbutils.widgets.get('store')
except:
  store = None
store = 'SEH'
if store is None:
  import lkp_tables_update as lkp_update


# -------------------------------------------------------------#
# calling dataingestion_to_sql
# -------------------------------------------------------------#
def dataingestion_to_sql(spark, logger, epoch_id, config):
    """create db settings for all the tables
    
    return: None
    """

    logger.info('Copying DataIngestion df to db')

    db_settings = {
        'sqlserver': "jdbc:sqlserver:{serverip}:{port};database={database}".format(
            serverip=config['output']['db_setting']['server_ip'], port=config['output']['db_setting']['port'],
            database=config['output']['db_setting']['database_name']),
        'username': "{username}".format(username=config['output']['db_setting']['user_name']),
        'password': "{password}".format(password=config['output']['db_setting']['password']),
        'driver': "{driver}".format(driver=config['output']['db_setting']['driver']),
        'prefix': "{prefix}".format(prefix=config['input']['prefix']['prefix_store'])
    }

    loyaltyreward = "{tablename}".format(tablename=config['output']['db_tables']['loyaltyreward'])
    retailtransactionheader = "{tablename}".format(tablename=config['output']['db_tables']['retailtransactionheader'])
    retailpricemodifier = "{tablename}".format(tablename=config['output']['db_tables']['retailpricemodifier'])
    salereturn = "{tablename}".format(tablename=config['output']['db_tables']['salereturn'])
    tender = "{tablename}".format(tablename=config['output']['db_tables']['tender'])

    database = config["input"]["hive"]["database"]
    loyaltyreward_to_sql(spark, logger, epoch_id, db_settings, loyaltyreward, database)
    retailtransactionheader_to_sql(spark, logger, epoch_id, db_settings, retailtransactionheader, database)
    retailpricemodifier_sql(spark, logger, epoch_id, db_settings, retailpricemodifier, database)
    salereturn_sql(spark, logger, epoch_id, db_settings, salereturn, config, database)
    tender_sql(spark, logger, epoch_id, db_settings, tender, config, database)


# -------------------------------------------------------------#
# Creating loyaltyreward_to_sql
# -------------------------------------------------------------#
def loyaltyreward_to_sql(spark, logger, epoch_id, db_settings, loyaltyreward, database):
    """Copying LoyaltyReward table to db
    
    return: None
    """

    logger.info('Copying LoyaltyReward df to db')

    LoyaltyRewardToSQL = spark.sql(
        "select retailstoreid, WorkstationID, ReceiptNumber, ReceiptDateTime, COALESCE(ReferenceID, "
        "0) as ReferenceID, COALESCE(RuleDescription, 0) as RuleDescription , SUM(PointsAwarded) AS PointsAwarded "
        "from {fraud}.loyaltyreward GROUP BY retailstoreid, WorkstationID, ReceiptNumber, ReceiptDateTime, "
        "ReferenceID, RuleDescription".format(
            fraud=database))

    LoyaltyRewardToSQL = LoyaltyRewardToSQL.withColumnRenamed('retailstoreid', 'RetailStoreID')

    LoyaltyRewardToSQL.registerTempTable('LoyaltyRewardToSQL')

    LoyaltyRewardToSQL = spark.sql(
        "select RetailStoreID, WorkstationID, ReceiptNumber, ReceiptDateTime, COALESCE(ReferenceID, "
        "0) as ReferenceID, COALESCE(RuleDescription, 0) as RuleDescription , SUM(PointsAwarded) AS PointsAwarded "
        "from LoyaltyRewardToSQL GROUP BY RetailStoreID, WorkstationID, ReceiptNumber, ReceiptDateTime, ReferenceID, "
        "RuleDescription".format(
            fraud=database))

    LoyaltyRewardToSQL.write.mode("overwrite").format("jdbc") \
        .option("url", db_settings['sqlserver']) \
        .option("dbtable", loyaltyreward) \
        .option("user", db_settings['username']) \
        .option("password", db_settings['password']) \
        .option("driver", db_settings['driver']) \
        .save()


# -------------------------------------------------------------#
# Creating retailtransactionheader_to_sql
# -------------------------------------------------------------#
def retailtransactionheader_to_sql(spark, logger, epoch_id, db_settings, retailtransactionheader, database):
    """Copying RetailTransactionHeader table to db
    
    return: None
    """

    logger.info('Copying RetailTransactionHeader df to db')

    # Rule Engine logic to create line item voiding features for screen 29 (line item voiding report)
    # Rule Engine logic to include rebate flag

    spark.sql("""
create or replace temporary view SaleReturnline as
    select      
    a.retailstoreid
    , a.WorkstationID
    , a.ReceiptNumber
    , a.ReceiptDateTime
    , a.LineItemSequenceNumber
    , a.voidflag
    , a.Directvoidflag
    , a.VoidFlagDerived
    , b.ExtendedAmount
       from
    {fraud}.LineItemDerived a
     JOIN
     (select     
      a.retailstoreid
    , a.WorkstationID
    , a.ReceiptNumber
    , a.ReceiptDateTime
    , a.LineItemSequenceNumber
    , a.ExtendedAmount
    from {fraud}.sale a
    union
    select     
      a.retailstoreid
    , a.WorkstationID
    , a.ReceiptNumber
    , a.ReceiptDateTime
    , a.LineItemSequenceNumber
    , a.ExtendedAmount
    from {fraud}.Return a
    ) b
       
      on
       a.retailstoreid             =b.retailstoreid
       and a.WorkstationID         =b.WorkstationID
       and a.ReceiptNumber         =b.ReceiptNumber
       and a.ReceiptDateTime       =b.ReceiptDateTime
       and a.LineItemSequenceNumber=b.LineItemSequenceNumber""".format(fraud=database))

    spark.sql("""create or replace temporary view lineitem_voidcounts as select  
    a.retailstoreid
    , a.WorkstationID
    , a.ReceiptNumber
    , a.ReceiptDateTime
    ,count(case when a.VoidFlagDerived = false then 1 end) as NonVoidedItemCount
    ,count(case when a.VoidFlag = True then 1 end) as VoidedItemCount
    ,sum(case when a.VoidFlag =True  then ExtendedAmount else 0 end) as VoidedItemTotal
    ,count(case when a.Directvoidflag = True then 1 end) as DirectVoidedItemCount
    ,count(case when a.Directvoidflag = False  then 1 end) as LineVoidedItemCount    
    from SaleReturnline a
    group by 
    a.retailstoreid
    , a.WorkstationID
    , a.ReceiptNumber
    , a.ReceiptDateTime
    """)

    RetailTransactionHeader = spark.sql("""
    select a.*
    ,NonVoidedItemCount
    ,VoidedItemCount
    ,VoidedItemTotal
    ,DirectVoidedItemCount
    ,LineVoidedItemCount
    ,Rebate100Percent
    from {fraud}.retailtransactionheader a                                      
    left join  
    lineitem_voidcounts b
    on
       a.retailstoreid             =b.retailstoreid
       and a.WorkstationID         =b.WorkstationID
       and a.ReceiptNumber         =b.ReceiptNumber
       and a.ReceiptDateTime       =b.ReceiptDateTime
       
    left join
  (
   select
    case
     when (
       count
        (
         case
          when RuleDescription='Kleberabatt'
           and NewPrice       = 0.0
           and slrt.ExtendedAmount = 0
           then sr.lineitemsequencenumber
         else null
         end
        )
      )
      >= 1
      then 1
      else 0
    end as Rebate100Percent
   , sr.retailStoreID
   , sr.WorkstationID
   , sr.ReceiptDateTime
   , sr.ReceiptNumber
   from
    {fraud}.salereturn_rebate sr
    inner join 
        (
        select sl.retailstoreid,sl.workstationid,sl.receiptdatetime,sl.receiptnumber,
        sl.lineitemsequencenumber,sl.ExtendedAmount
            from 
        {fraud}.sale sl
    union
        select rt.retailstoreid,rt.workstationid,rt.receiptdatetime,rt.receiptnumber,
        rt.lineitemsequencenumber,rt.ExtendedAmount
            from 
        {fraud}.return rt
        ) slrt
    
    on 
    sr.retailstoreid       = slrt.retailstoreid
   and sr.workstationid   =  slrt.workstationid
   and sr.receiptdatetime = slrt.receiptdatetime
   and sr.receiptnumber   = slrt.receiptnumber
   and sr.lineitemsequencenumber = slrt.lineitemsequencenumber

   group by
    sr.retailStoreID
   , sr.WorkstationID
   , sr.ReceiptDateTime
   , sr.ReceiptNumber
  )
  rb
  on
   a.retailstoreid       = rb.retailstoreid
   and a.workstationid   = rb.workstationid
   and a.receiptdatetime = rb.receiptdatetime
   and a.receiptnumber   = rb.receiptnumber
    """.format(fraud=database))

    Customer = spark.sql(
        "select * from {fraud}.customer where CustomerType = 'DC' OR CustomerType = 'dc' OR CustomerType = 'Dc' OR "
        "CustomerType = 'dC'".format(
            fraud=database))

    RetailTransactionHeaderdfToSQL = RetailTransactionHeader.join(Customer,
                                                                  ['retailstoreid', 'WorkstationID', 'SequenceNumber',
                                                                   'ReceiptDateTime', ], 'left')

    TransactionStatus_dict = {"PostVoided": 0, "Suspended": 1, "SuspendedRetrieved": 2, "Totaled": 3}
    mapping_expr = F.create_map([F.lit(x) for x in chain(*TransactionStatus_dict.items())])

    RetailTransactionHeaderdfToSQL = RetailTransactionHeaderdfToSQL.withColumn('TransactionStatus1', mapping_expr[
        RetailTransactionHeaderdfToSQL['TransactionStatus']])
    RetailTransactionHeaderdfToSQL = RetailTransactionHeaderdfToSQL.withColumn("NegativeTotalFlag1", F.when(
        F.col('NegativeTotalFlag') == 'True', '-1').when(F.col('NegativeTotalFlag') == 'False', '1'))
    RetailTransactionHeaderdfToSQL = RetailTransactionHeaderdfToSQL.withColumn("TransactionGrandAmount1",
                                                                               F.col("TransactionGrandAmount") * F.col(
                                                                                   "NegativeTotalFlag1"))
    RetailTransactionHeaderdfToSQL = RetailTransactionHeaderdfToSQL.select(RetailTransactionHeader.RetailStoreID,
                                                                           RetailTransactionHeader.WorkstationID,
                                                                           RetailTransactionHeader.ReceiptDateTime,
                                                                           RetailTransactionHeader.ReceiptNumber,
                                                                           RetailTransactionHeaderdfToSQL.TransactionStatus1.alias(
                                                                               'TransactionStatus'),
                                                                           RetailTransactionHeaderdfToSQL.TransactionGrandAmount1.alias(
                                                                               'TransactionGrandAmount'),
                                                                           Customer.CustomerType,
                                                                           Customer.CustomerTypeDescription,
                                                                           Customer.AccountNumber,
                                                                           RetailTransactionHeader.NonVoidedItemCount,
                                                                           RetailTransactionHeader.VoidedItemCount,
                                                                           RetailTransactionHeader.VoidedItemTotal,
                                                                           RetailTransactionHeader.DirectVoidedItemCount,
                                                                           RetailTransactionHeader.LineVoidedItemCount,
                                                                           RetailTransactionHeader.Rebate100Percent)
#     RetailTransactionHeaderdfToSQL = RetailTransactionHeaderdfToSQL.withColumnRenamed('retailstoreid', 'RetailStoreID')
    RetailTransactionHeaderdfToSQL = RetailTransactionHeaderdfToSQL.dropDuplicates(
        ['RetailStoreID', 'WorkstationID', 'ReceiptDateTime', 'ReceiptNumber'])

    RetailTransactionHeaderdfToSQL.write.mode("overwrite").format("jdbc") \
        .option("url", db_settings['sqlserver']) \
        .option("dbtable", retailtransactionheader) \
        .option("user", db_settings['username']) \
        .option("password", db_settings['password']) \
        .option("driver", db_settings['driver']) \
        .save()


# -------------------------------------------------------------#
# Creating retailpricemodifier_sql
# -------------------------------------------------------------#
def retailpricemodifier_sql(spark, logger, epoch_id, db_settings, retailpricemodifier, database):
    """Copying RetailPriceModifier table to db
    
    return: None
    """

    logger.info('Copying RetailPriceModifier df to db')

    RetailPriceModifierToSQL = spark.sql(
        "select retailstoreid, WorkstationID, ReceiptDateTime, ReceiptNumber, LineItemSequenceNumber, "
        "PriceModifierSequenceNumber, AmountAction, AmountValue, PromotionID, PreviousPrice, NewPrice, ReasonCode, "
        "RebateID from {fraud}.retailpricemodifier".format(
            fraud=database))

    RetailPriceModifierToSQL = RetailPriceModifierToSQL.withColumnRenamed('retailstoreid', 'RetailStoreID')

    RetailPriceModifierToSQL = RetailPriceModifierToSQL.dropDuplicates(
        ['RetailStoreID', 'WorkstationID', 'ReceiptDateTime', 'ReceiptNumber', 'LineItemSequenceNumber',
         'PriceModifierSequenceNumber'])

    RetailPriceModifierToSQL.write.mode("overwrite").format("jdbc") \
        .option("url", db_settings['sqlserver']) \
        .option("dbtable", retailpricemodifier) \
        .option("user", db_settings['username']) \
        .option("password", db_settings['password']) \
        .option("driver", db_settings['driver']) \
        .save()


# -------------------------------------------------------------#
# Creating salereturn_sql
# -------------------------------------------------------------#
def salereturn_sql(spark, logger, epoch_id, db_settings, salereturn, config, database):
    """Copying SaleReturn table to db
    
    return: None
    """

    logger.info('Copying SaleReturn df to db')

    Return = spark.sql(
        "select retailstoreid, WorkstationID, ReceiptDateTime, ReceiptNumber, LineItemSequenceNumber, ItemType, "
        "POSItemID, ItemID, MerchandiseHierarchyLevel, Description, RegularSalesUnitPrice, ExtendedAmount, "
        "UnitOfMeasureCode, Quantity, Units, MerchandiseStructureItemFlag, SpecialPriceDescription from "
        "{fraud}.return".format(fraud=database))
    Return = Return.withColumn('ReturnFlag', F.lit(1))
    Return = Return.withColumn('ExtendedAmount', F.col('ExtendedAmount') * (-1))

    Sale = spark.sql(
        "select retailstoreid, WorkstationID, ReceiptDateTime, ReceiptNumber, LineItemSequenceNumber, ItemType, "
        "POSItemID, ItemID, MerchandiseHierarchyLevel, Description, RegularSalesUnitPrice, ExtendedAmount, "
        "UnitOfMeasureCode, Quantity, Units, MerchandiseStructureItemFlag, SpecialPriceDescription from "
        "{fraud}.sale".format(fraud=database))
    Sale = Sale.withColumn('ReturnFlag', F.lit(0))

    SaleReturndfToSQL = Sale.unionAll(Return)

    SaleReturndfToSQL = SaleReturndfToSQL.withColumnRenamed('retailstoreid', 'RetailStoreID')

    SaleReturndfToSQL.registerTempTable('SaleReturndfToSQL')

    spark.sql("""create or replace temporary view md_articlederived as
    select
     b.*
    from
     (
      select
       EAN
      , Unit
      , EAN_SAP_ID
      , ROW_NUMBER() OVER(PARTITION BY EAN,unit ORDER BY
                          EAN ) rn
      from
       (
        select distinct
         EAN as EAN
         ,' ' as EAN_SAP_ID
        , unit
        from
         {fraud}.md_articledescription
        union
        select distinct
         SAP_ID  as EAN 
         ,EAN as EAN_SAP_ID
        , unit
        from
         {fraud}.md_articledescription
       )
       a
     )
     b
    where
     rn = 1""".format(fraud=database))

    SaleReturndfToSQL_derived = spark.sql("""select s.*,coalesce(md.EAN,md1.EAN_SAP_ID) AS EAN_Lookup
    from  SaleReturndfToSQL s left join md_articlederived md
    on s.POSItemID=md.EAN and s.UnitOfMeasureCode=md.Unit
    left join (select distinct EAN,EAN_SAP_ID from md_articlederived where EAN != ' ') md1
    on s.ItemID=md1.EAN""")

    storesdf = spark.sql("""select StoreID as RetailStoreID, IsEBUSStore from {fraud}.stores""".format(fraud=database))

    merged_df = SaleReturndfToSQL_derived.join(storesdf, ["RetailStoreID"])
    merged_df = merged_df.select('RetailStoreID', 'IsEBUSStore', 'WorkstationID', 'ReceiptDateTime', 'ReceiptNumber',
                                 'LineItemSequenceNumber', 'ItemType', 'POSItemID', 'ItemID',
                                 'MerchandiseHierarchyLevel', 'Description', 'RegularSalesUnitPrice', 'ExtendedAmount',
                                 'UnitOfMeasureCode', 'Quantity', 'Units', 'MerchandiseStructureItemFlag',
                                 'SpecialPriceDescription', 'ReturnFlag', 'EAN_lookup')

    # updating itemtype
    itemtype = merged_df.select('ItemType').distinct()
    itemtype = itemtype.filter(F.col('ItemType').isNotNull())
    itemtype = itemtype.withColumnRenamed('ItemType', 'ItemTypeDesc')

    # reading lkp_table itemtype from lkp_tables_updates
    itemtype_df = lkp_update.read_lkp_itemtype(spark, logger, config)
    itemtype_df = itemtype_df.join(itemtype, ['ItemTypeDesc'], 'full')
    itemtype_df = itemtype_df.select('ItemType', 'ItemTypeDesc')

    _max = itemtype_df.agg({"ItemType": "max"}).collect()[0]['max(ItemType)']

    if _max is None:
        _max = 1
    else:
        _max = _max + 1

    w = Window.orderBy('ItemType')

    itemtype_df = itemtype_df.withColumn('ItemType', F.when(F.col('ItemType') >= 0, F.col('ItemType'))
                                         .otherwise((F.row_number().over(w) - 1) + _max))

    # updating lkp_table itemtype in lkp_tables_updates
    lkp_update.update_lkp_itemtype(spark, logger, itemtype_df, config)

    ItemType_dict = dict(itemtype_df.rdd.map(lambda x: (x['ItemTypeDesc'], x['ItemType'])).collect())

    mapping_expr = F.create_map([F.lit(x) for x in chain(*ItemType_dict.items())])
    merged_df = merged_df.withColumn('ItemType', mapping_expr[merged_df['ItemType']])

    merged_df.write.mode("overwrite").format("jdbc") \
        .option("url", db_settings['sqlserver']) \
        .option("dbtable", salereturn) \
        .option("user", db_settings['username']) \
        .option("password", db_settings['password']) \
        .option("driver", db_settings['driver']) \
        .save()


# -------------------------------------------------------------#
# Creating tender_sql
# -------------------------------------------------------------#
def tender_sql(spark, logger, epoch_id, db_settings, tender, config, database):
    """Copying Tender table to db
    
    return: None
    """

    logger.info('Copying Tender df to db')

    TenderdfToSQL = spark.sql(
        "select retailstoreid as RetailStoreID, WorkstationID, ReceiptDateTime, ReceiptNumber, "
        "LineItemSequenceNumber, TypeCode, TenderID, Amount, TenderChangeType, TenderChangeID, TenderChangeAmount, "
        "CardType, PrimaryAccountNumber, Cashback, IssuerIdentificationNumber, SerialNumber from "
        "{fraud}.tender".format(fraud=database))
    TenderdfToSQL = TenderdfToSQL.withColumn("Tip", F.lit(None).cast('Int'))

    # updating cardtype
    cardtype = TenderdfToSQL.select('CardType').distinct()
    cardtype = cardtype.filter(F.col('CardType').isNotNull())
    cardtype = cardtype.withColumnRenamed('CardType', 'CardTypeDesc')

    # reading lkp_table cardtype from lkp_tables_updates
    cardtype_df = lkp_update.read_lkp_cardtype(spark, logger, config)
    cardtype_df = cardtype_df.join(cardtype, ['CardTypeDesc'], 'full')
    cardtype_df = cardtype_df.select('CardType', 'CardTypeDesc')

    _max = cardtype_df.agg({"CardType": "max"}).collect()[0]['max(CardType)']

    if _max is None:
        _max = 1
    else:
        _max = _max + 1

    w = Window.orderBy('CardType')

    cardtype_df = cardtype_df.withColumn('CardType', F.when(F.col('CardType') >= 0, F.col('CardType'))
                                         .otherwise((F.row_number().over(w) - 1) + _max))

    # updating lkp_table cardtype in lkp_tables_updates
    lkp_update.update_lkp_cardtype(spark, logger, cardtype_df, config)

    # CardType_dict = {"Credit":0, "Debit":1}
    CardType_dict = dict(cardtype_df.rdd.map(lambda x: (x['CardTypeDesc'], x['CardType'])).collect())
    mapping_expr = F.create_map([F.lit(x) for x in chain(*CardType_dict.items())])
    TenderdfToSQL = TenderdfToSQL.withColumn('CardType', mapping_expr[TenderdfToSQL['CardType']])

    # updating typecode
    typecode = TenderdfToSQL.select('TypeCode').distinct()
    typecode = typecode.filter(F.col('TypeCode').isNotNull())
    typecode = typecode.withColumnRenamed('TypeCode', 'TypeCodeDesc')

    # reading lkp_table typecode from lkp_tables_updates
    typecode_df = lkp_update.read_lkp_typecode(spark, logger, config)
    typecode_df = typecode_df.join(typecode, ['TypeCodeDesc'], 'full')
    typecode_df = typecode_df.select('TypeCode', 'TypeCodeDesc')

    _max = typecode_df.agg({"TypeCode": "max"}).collect()[0]['max(TypeCode)']

    if _max is None:
        _max = 1
    else:
        _max = _max + 1

    w = Window.orderBy('TypeCode')

    typecode_df = typecode_df.withColumn('TypeCode', F.when(F.col('TypeCode') >= 0, F.col('TypeCode'))
                                         .otherwise((F.row_number().over(w) - 1) + _max))

    # updating lkp_table typecode in lkp_tables_updates
    lkp_update.update_lkp_typecode(spark, logger, typecode_df, config)

    # CardType_dict = {"Credit":0, "Debit":1}
    TypeCode_dict = dict(typecode_df.rdd.map(lambda x: (x['TypeCodeDesc'], x['TypeCode'])).collect())
    mapping_expr = F.create_map([F.lit(x) for x in chain(*TypeCode_dict.items())])
    TenderdfToSQL = TenderdfToSQL.withColumn('TypeCode', mapping_expr[TenderdfToSQL['TypeCode']])

    # TypeCode_dict = {"Refund":0, "Sale":1}
    # mapping_expr = F.create_map([F.lit(x) for x in chain(*TypeCode_dict.items())])
    # TenderdfToSQL = TenderdfToSQL.withColumn('TypeCode', mapping_expr[TenderdfToSQL['TypeCode']])

    # TenderID_dict = {"YTEC":0 , "YTVC":1, "YTSX":2, "YTER":3, "YTAM":4, "YTVO":5, "YTCE":6, "YTKR":7, "YTDT":8, "YTSZ":9, "YTVP":10, "YTVB":11, "YTVG":12, "YTMC":13, "YTDP":14, "YTEM":15, "YTSO":16, "YTVI":17, "YTPG":18, "YTMA":19, "YTNG":20, "YTCS":21}

    # updating tenderid
    tenderid = TenderdfToSQL.select('TenderID').distinct()
    tenderid = tenderid.filter(F.col('TenderID').isNotNull())
    tenderid = tenderid.withColumnRenamed('TenderID', 'TenderIDDesc')

    # reading lkp_table typecode from lkp_tables_updates
    tenderid_df = lkp_update.read_lkp_tenderid(spark, logger, config)
    tenderid_df = tenderid_df.join(tenderid, ['TenderIDDesc'], 'full')
    tenderid_df = tenderid_df.select('TenderID', 'TenderIDDesc')

    _max = tenderid_df.agg({"TenderID": "max"}).collect()[0]['max(TenderID)']

    if _max is None:
        _max = 1
    else:
        _max = _max + 1

    w = Window.orderBy('TenderID')

    tenderid_df = tenderid_df.withColumn('TenderID', F.when(F.col('TenderID') >= 0, F.col('TenderID'))
                                         .otherwise((F.row_number().over(w) - 1) + _max))

    # updating lkp_table typecode in lkp_tables_updates
    lkp_update.update_lkp_tenderid(spark, logger, tenderid_df, config)

    # CardType_dict = {"Credit":0, "Debit":1}
    TenderID_dict = dict(tenderid_df.rdd.map(lambda x: (x['TenderIDDesc'], x['TenderID'])).collect())

    mapping_expr = F.create_map([F.lit(x) for x in chain(*TenderID_dict.items())])
    TenderdfToSQL = TenderdfToSQL.withColumn('TenderID', mapping_expr[TenderdfToSQL['TenderID']])

    TenderdfToSQL.write.mode("overwrite").format("jdbc") \
        .option("url", db_settings['sqlserver']) \
        .option("dbtable", tender) \
        .option("user", db_settings['username']) \
        .option("password", db_settings['password']) \
        .option("driver", db_settings['driver']) \
        .save()
