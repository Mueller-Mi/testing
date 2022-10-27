# Databricks notebook source
import time
def create_pricelookup_dataframe(spark,epocid,logger,config):

    """Creates PriceLookup fraud dataframe

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
    
    logger.info("Creating PriceLookUp Fraud Dataframe")

    #------------------------------------------------------------------------------------------------------------#
    #Parquet path varaibale assigned
    #------------------------------------------------------------------------------------------------------------

    path     = config["output"]["ruleengine"]["PriceLookUp_folder"]
    database = config["input"]["hive"]["database"]

    
    #------------------------------------------------------------------------------------------------------------#    
    #Pricelookup data frame creation
    #------------------------------------------------------------------------------------------------------------#


    spark.sql("""create or replace temporary view FunctionLog_der_RT as 
        select distinct
        der1.*
        , der2.ACTUALPRICE
        , case
                when (
                            der1.SequenceNumber = 0
                        )
                        then 0
                        else 1
        end as seqFlag
    from
        (
                select
                        fl.retailstoreid
                    , fl.WorkstationID
                    , fl.SequenceNumber
                    , fl.ReceiptNumber
                    , fl.ReceiptDateTime
                    , fl.StoreInfoID
                    , ParameterValue as EAN
                from
                        {fraud}.Functionlog fl
                where
                        fl.FunctionID        = 12
                        and fl.FunctionName  = 'X_ARTICLE_INFORMATION'
                        and fl.ParameterName = 'ITEM_EAN'
        )
        der1
        join
                (
                        select
                            fl.retailstoreid
                            , fl.WorkstationID
                            , fl.SequenceNumber
                            , fl.ReceiptNumber
                            , fl.ReceiptDateTime
                            , fl.StoreInfoID
                            , ParameterValue as ACTUALPRICE
                        from
                            {fraud}.Functionlog fl
                        where
                            fl.FunctionID        = 12
                            and fl.FunctionName  = 'X_ARTICLE_INFORMATION'
                            and fl.ParameterName = 'ACTUAL_PRICE'
                )
                der2
                on
                        der1.retailstoreid       = der2.retailstoreid
                        and der1.WorkstationID   = der2.WorkstationID
                        and der1.SequenceNumber  = der2.SequenceNumber
                        and der1.ReceiptDateTime = der2.ReceiptDateTime
                        and der1.StoreInfoID     =der2.StoreInfoID""".format(fraud=database))

    #------------------------------------------------------------------------------------------------------------#
    # This is for non zero sequence number.
    #------------------------------------------------------------------------------------------------------------#

 
    spark.sql("""create or replace temporary view plkSeqNon0_RT as select
            pl.retailstoreid
        , pl.WorkstationID
        , pl.ReceiptDateTime
        , pl.EAN
        , pl.ACTUALPRICE
        , pl.seqFlag
        , pl.SequenceNumber
        , pl.StoreInfoID
        , rl.ReceiptDateTime as ReceiptDateTimeAT
     from
            FunctionLog_der_RT pl
            left join
                    {fraud}.RetailTransactionHeader rl
                    on
                                pl.retailstoreid                = rl.retailstoreid
                                and pl.WorkstationID            = rl.WorkstationID
                                and pl.SequenceNumber           =rl.SequenceNumber
                                and to_date(pl.ReceiptDateTime) = to_date(rl.ReceiptDateTime)
        where
            pl.SequenceNumber != 0""".format(fraud=database))

    #------------------------------------------------------------------------------------------------------------#
    # Extracting price lookup for zero sequence number and later
    #will be used to get the closest sequence number from Retail Transaction header table.
    #------------------------------------------------------------------------------------------------------------#


    spark.sql("""create or replace temporary view plkSeq0_RT as
      select *
      from
              FunctionLog_der_RT
      where
        SequenceNumber = 0""")

    #------------------------------------------------------------------------------------------------------------#
    # Compute date difference and extract the closest sequence number where
    # price look up sequence number is Zero

    #####datediff computed in seconds now with new logic
    #------------------------------------------------------------------------------------------------------------#

    spark.sql("""create or replace temporary view plkseqRetail_1_RT as 
       select
            pl.retailstoreid
        , pl.WorkstationID
        , pl.ReceiptDateTime
        , pl.StoreInfoID
        , pl.EAN
        , pl.ACTUALPRICE
        , pl.seqFlag
        , rl.SequenceNumber
        , rl.ReceiptDateTime as ReceiptDateTimeAT
        ,abs(unix_timestamp(date_format(rl.ReceiptDateTime,'yyyy-MM-dd HH:mm:ss'))-
        unix_timestamp(date_format(pl.ReceiptDateTime,'yyyy-MM-dd HH:mm:ss'))) as absdifReceiptDateTime
        
          from
            plkSeq0_RT pl
            left join
                    {fraud}.RetailTransactionHeader rl
                    on
                                pl.retailstoreid                = rl.retailstoreid
                                and pl.WorkstationID            = rl.WorkstationID
                                and to_date(pl.ReceiptDateTime) = to_date(rl.ReceiptDateTime)
                                and rl.ReceiptDateTime          > pl.ReceiptDateTime""".format(fraud=database))



    

    spark.sql("""create or replace temporary view plkseqRetail_RT as
        select
        b.*
           from
        (
                    select
                                    a.*
                                , row_number() over(partition by a.retailstoreid,a.WorkstationID,a.ReceiptDateTime order by
                                                    absdifReceiptDateTime) as row_num
                    from
                                    plkseqRetail_1_RT a
        )
        b
       where
        row_num=1""")

    #------------------------------------------------------------------------------------------------------------#
    #Append all sequnece number zero and non zero
    #------------------------------------------------------------------------------------------------------------#

    spark.sql("""create or replace temporary view fncSeqAll_RT as
    select
        a.*
     from
        (
                select
                        der.retailstoreid
                    , der.WorkstationID
                    , der.SequenceNumber
                    , der.ReceiptDateTime
                    , der.StoreInfoID
                    , der.EAN
                    , der.ACTUALPRICE
                    , der.seqFlag
                    , der.ReceiptDateTimeAT
                from
                        plkSeqNon0_RT der
                union
                select
                        ps.retailstoreid
                    , ps.WorkstationID
                    , ps.SequenceNumber
                    , ps.ReceiptDateTime
                    , ps.StoreInfoID
                    , ps.EAN
                    , ps.ACTUALPRICE
                    , ps.seqFlag
                    , ps.ReceiptDateTimeAT
                from
                        plkseqRetail_RT ps
        )
        a""")


    #------------------------------------------------------------------------------------------------------------#
    #Extracting other fields that are needed for price look up
    #------------------------------------------------------------------------------------------------------------#

    spark.sql("""create or replace temporary view plk_RT as
      select
            fl.*
        , th.ReceiptNumber
        , COALESCE(th.EmployeeID ,thfl.EmployeeID) as EmployeeID
        , rl.TransactionGrandAmount
        , ad.ArticleDescription
        , mh.MerchandiseHierarchyLevel
        , mh.DescriptionDE as MerchandiseHierachyDescription
        , st.IsEBUS
      from
            fncSeqAll_RT fl
            left join
                    {fraud}.RetailTransactionHeader rl
                    on
                                fl.retailstoreid                = rl.retailstoreid
                                and fl.WorkstationID            = rl.WorkstationID
                                and fl.SequenceNumber           = rl.SequenceNumber
                                and to_date(fl.ReceiptDateTime) = to_date(rl.ReceiptDateTime)
            left join
                    {fraud}.TransactionHeader th
                    on
                                rl.retailstoreid       = th.retailstoreid
                                and rl.WorkstationID   = th.WorkstationID
                                and rl.SequenceNumber  = th.SequenceNumber
                                and rl.ReceiptDateTime = th.ReceiptDateTime
                                and th.Transactiontype = 'RetailTransaction'
            left join
                    {fraud}.TransactionHeader thfl
                    on
                            fl.retailstoreid       = thfl.retailstoreid
                            and fl.WorkstationID   = thfl.WorkstationID
                            and fl.ReceiptDateTime = thfl.ReceiptDateTime
                            and COALESCE(fl.SequenceNumber,0)  = thfl.SequenceNumber
                            and fl.StoreInfoID     = thfl.FunctionLogStoreInfoID
                            and thfl.Transactiontype = 'FunctionLog'
            left join
                    (
                     select
                      b.*
                     from
                      (
                       select
                        EAN
                       , MerchandiseHierarchyLevel_EDEKA
                       , MerchandiseHierarchyLevel_EBUS
                       , ArticleDescription
                       , ROW_NUMBER() OVER(PARTITION BY EAN ORDER BY
                                           EAN ) rn
                       from
                        (
                         select distinct
                          EAN as EAN
                         , MerchandiseHierarchyLevel_EDEKA
                         , MerchandiseHierarchyLevel_EBUS
                         , Description as ArticleDescription
                         from
                          {fraud}.md_articledescription
                         union
                         select distinct
                          SAP_ID as EAN
                         , MerchandiseHierarchyLevel_EDEKA
                         , MerchandiseHierarchyLevel_EBUS
                         , Description as ArticleDescription
                         from
                          {fraud}.md_articledescription
                        )
                        a
                      )
                      b
                     where
                      rn = 1
                    )
                    ad on fl.EAN = cast(ad.EAN as varchar(40))            
            left join
                    (select distinct storeid,IsEBUSStore as IsEBUS,
                    case when IsEBUSStore = 1 then '1' else '0' end as 
                    IsEBUSStore from {fraud}.stores) st
                    on fl.retailstoreid = st.storeid
            left join
                  
                    (select distinct MerchandiseHierarchyLevel ,descriptionde, IsEBUS
                    from {fraud}.md_merchandisehierarchylevels) mh
                    on
                                mh.MerchandiseHierarchyLevel = case when st.IsEBUSStore=0 then ad.MerchandiseHierarchyLevel_Edeka else ad.MerchandiseHierarchyLevel_EBUS end and 
                                st.IsEBUSStore = mh.IsEBUS """.format(fraud=database))

    #------------------------------------------------------------------------------------------------------------#

    #Computing minimum line item sequence number for classification
    # of a price lookup flag

    #------------------------------------------------------------------------------------------------------------#
    
    spark.sql("""create or replace temporary view minLineItem_RT as
      select
            pl.retailstoreid
        , pl.WorkstationID
        , pl.SequenceNumber
        , pl.ReceiptDateTime
        , min(li.LineItemSequencenumber) as minLineItemSequenceNumber
       from
            plk_RT pl
            join
                    {fraud}.LineItem li
                    on
                            pl.retailstoreid                = li.retailstoreid
                            and pl.WorkstationID            = li.WorkstationID
                            and pl.SequenceNumber           = li.SequenceNumber
                            and to_date(pl.ReceiptDateTime) = to_date(li.ReceiptDateTime)
                            and li.LineItemBeginDateTime    > pl.ReceiptDateTime
      group by
            pl.retailstoreid
        , pl.WorkstationID
        , pl.SequenceNumber
        , pl.ReceiptDateTime""".format(fraud=database))

    #------------------------------------------------------------------------------------------------------------#

    #Combine minimum line item sequence number with price look up table

    #------------------------------------------------------------------------------------------------------------#
    
    spark.sql("""create or replace temporary view minpl_RT as
      select
            pl.*
        , ml.minLineItemSequenceNumber
      from
            plk_RT pl
            left join
                    minLineItem_RT ml
                    on
                                pl.retailstoreid      = ml.retailstoreid
                                and pl.WorkstationID  = ml.WorkstationID
                                and pl.SequenceNumber = ml.SequenceNumber
                                and pl.ReceiptDateTime=ml.ReceiptDateTime""")

    
    spark.sql("""create or replace temporary view prloup_RT as 
select
            mn.*
        , cd.class_id
        , case
                    when cd.class_id not in ('AN0'
                                            ,'AN1'
                                            ,'AN2'
                                            ,'AN3'
                                            ,'AN4'
                                            ,'AN5'
                                            ,'AN6'
                                            ,'AN7'
                                            ,'AN8'
                                            ,'AN9'
                                            ,'EN0'
                                            ,'EN1'
                                            ,'EN2'
                                            ,'EN3'
                                            ,'EN4'
                                            ,'EN5'
                                            ,'EN6'
                                            ,'EN7'
                                            ,'EN8'
                                            ,'EN9'
                                            ,'CN0'
                                            ,'CN1'
                                            ,'CN2'
                                            ,'CN3'
                                            ,'CN4'
                                            ,'CN5'
                                            ,'CN6'
                                            ,'CN7'
                                            ,'CN8'
                                            ,'CN9')
                                then 'Q'
                    when (
                                        mn.EAN = sl.POSItemID
                                )
                                then 'N'
                    when (
                                        li.EntryMethod = 'Keyed'
                                )
                                then 'M'
                                else 'P'
            end as PriceLookupFlag
       from
            minpl_RT mn
            left join
                    (
                            select
                                    retailstoreid
                                , WorkStationID
                                , SequenceNumber
                                , ReceiptDateTime
                                , LineItemSequenceNumber
                                , POSItemID
                                , MerchandiseHierarchyLevel
                            from
                                    {fraud}.Return
                            union
                            select
                                    retailstoreid
                                , WorkStationID
                                , SequenceNumber
                                , ReceiptDateTime
                                , LineItemSequenceNumber
                                , POSItemID
                                , MerchandiseHierarchyLevel
                            from
                                    {fraud}.Sale
                    )
                    sl
                    on
                                mn.retailstoreid                 = sl.retailstoreid
                                and mn.WorkstationID             = sl.WorkstationID
                                and mn.SequenceNumber            = sl.SequenceNumber
                                and to_date(mn.ReceiptDateTime)  = to_date(sl.ReceiptDateTime)
                                and mn.minLineItemSequenceNumber = sl.LineItemSequenceNumber
            left join
                    {fraud}.LineItem li
                    on
                                sl.retailstoreid              = li.retailstoreid
                                and sl.WorkstationID          = li.WorkstationID
                                and sl.SequenceNumber         = li.SequenceNumber
                                and sl.ReceiptDateTime        = li.ReceiptDateTime
                                and sl.LineItemSequenceNumber = li.LineItemSequenceNumber
            left join
                    {fraud}.DF_Classification_Description cd
                    on
                                mn.retailstoreid        = cd.retailstoreid
                                and mn.WorkstationID    = cd.WorkstationID
                                and mn.SequenceNumber   = cd.SequenceNumber
                                and mn.ReceiptDateTimeAT= cd.ReceiptDateTime
                                and cd.Transactiontype  = 'RetailTransaction'""".format(fraud=database)) 


    #------------------------------------------------------------------------------------------------------------#

    #Final price lookup table with all the variables needed.
    #------------------------------------------------------------------------------------------------------------#
    
    DF_PriceLookUp=spark.sql("""
select
            prl.retailstoreid
        , prl.WorkstationID
        , prl.ReceiptDateTime as ReceiptDateTimeFL
        , prl.ReceiptDateTimeAT
        , case
                when prl.seqFlag = 0
                            then prl.ReceiptNumber
                when der.ReceiptNumber is not null      /*for 1075*/
                            then der.ReceiptNumber
                            else 0
                end as ReceiptNumberAT
        , case
                    when der.ReceiptNumber is not null
                                then der.ReceiptNumber
                                else 0
            end as ReceiptNumberFL
        , prl.EmployeeID
        , prl.PriceLookupFlag
        , prl.EAN
        , prl.ActualPrice
        , prl.ArticleDescription
        , prl.MerchandiseHierarchyLevel
        , prl.MerchandiseHierachyDescription
        , prl.TransactionGrandAmount
        , prl.StoreInfoID
        , IsEBUS
      from
            prloup_RT prl
            left join
                    FunctionLog_der_RT der
                    on
                                prl.retailstoreid      =der.retailstoreid
                                and prl.WorkStationID  =der.WorkStationID
                                and prl.SequenceNumber =der.SequenceNumber
                                and prl.ReceiptDateTime=der.ReceiptDateTime
                                and prl.StoreInfoID    =der.StoreInfoID""")

    #---------------------------------------------------------------------------------------------------------#
    #Writes into parquet and alter hive table
    #---------------------------------------------------------------------------------------------------------#

    start = time.time()
    if config['input']['file_type']['file_type']=='parquet':
        DF_PriceLookUp.coalesce(10).write.partitionBy('retailstoreid').mode('overwrite').parquet('{path}/{epocid_r}'.format(epocid_r=epocid,path=path))  
    
        drppart=','.join(['partition('+ part +')' for part in map(lambda x:str(x[0]),spark.sql("show partitions {fraud}.df_pricelookup".format(fraud=database)).collect())])  

        if len(drppart) != 0:        
            spark.sql("alter table {fraud}.df_pricelookup drop if exists ".format(fraud=database)+drppart)   
            
        spark.sql("""alter table {fraud}.df_pricelookup set location '{path}/{epocid_r}'""".format(epocid_r=epocid,path=path,fraud=database))
        spark.sql("msck repair table {fraud}.df_pricelookup".format(fraud=database))
    
    else:
        logger.info("writing delta")
        
        DF_PriceLookUp.coalesce(10).write.partitionBy('retailstoreid').mode('overwrite').option("overwriteSchema", "true").format("delta").save('{path}/{epocid_r}'.format(epocid_r=epocid,path=path))
        
        spark.sql("""create table if not exists {fraud}.df_pricelookup location '{path}/{epocid_r}'""".format(epocid_r=epocid,path=path,fraud=database))
        spark.sql("""alter table {fraud}.df_pricelookup set location '{path}/{epocid_r}'""".format(epocid_r=epocid,path=path,fraud=database))
        
    #---------------------------------------------------------------------------------------------------------#
    #Logger info updated
    #---------------------------------------------------------------------------------------------------------#
    logger.info("PriceLookup parquet written in path : %s",'{path}/{epocid_r}'.format(epocid_r=epocid,path=path))

    pricelookup_count = spark.sql('select pricelookupflag,count(*) as count from {fraud}.df_pricelookup group by pricelookupflag'.format(fraud=database))
    pricelookup_count1=pricelookup_count.toPandas()
    logger.debug(pricelookup_count1.to_string(index = False)) 

    #------------------------------------------------------------------------------------------------------------#
    #Drop temp tables
    #------------------------------------------------------------------------------------------------------------
    
    spark.catalog.dropTempView('FunctionLog_der_RT')
    spark.catalog.dropTempView('plkSeqNon0_RT')
    spark.catalog.dropTempView('plkSeq0_RT')
    spark.catalog.dropTempView('plkseqRetail_1_RT')
    spark.catalog.dropTempView('fncSeqAll_RT')
    spark.catalog.dropTempView('plk_RT')
    spark.catalog.dropTempView('minLineItem_RT')
    spark.catalog.dropTempView('minpl_RT')
    spark.catalog.dropTempView('prloup_RT')
