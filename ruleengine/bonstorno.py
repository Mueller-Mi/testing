# Databricks notebook source
import time
def create_bonstrono_dataframe(spark,epocid,logger,config):

    """

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
    #Variables for path and stores prefix created
    #------------------------------------------------------------------------------------------------------------

    path     = config["output"]["ruleengine"]["ReceiptVoiding_folder"]
    database = config["input"]["hive"]["database"]

    #------------------------------------------------------------------------------------------------------------#
    #Loggr info updated
    #------------------------------------------------------------------------------------------------------------
    
    logger.info("Creating Bonstorno Fraud Dataframe")
  
 
    #------------------------------------------------------------------------------------------------------------#
    #Following code creates Derived line item table with Derived void flag
    #------------------------------------------------------------------------------------------------------------

    spark.sql(""" 
    create or replace temporary view TB_LineItem_Derived_bon as
  select *
  from
    (
      Select *
      from
        (  Select
    a.retailstoreid
  , a.WorkstationID
  , a.SequenceNumber
  , a.ReceiptDateTime
  , a.LineItemSequenceNumber
  , b.RegularSalesUnitPrice
  , b.ExtendedAmount
  , b.MerchandiseHierarchyLevel
  , b.ReturnItemLink as ItemLink
  , b.POSItemID
  , c.TransactionGrandAmount as TransactionGrandAmount
  From
    (select retailstoreid,WorkstationID,ReceiptDateTime,SequenceNumber,LineItemSequenceNumber,VoidFlagDerived from {fraud}.lineitemderived) a
    JOIN
      (
        select
          retailstoreid
        , WorkstationID
        , SequenceNumber
        , ReceiptDateTime
        , LineItemSequenceNumber
        , RegularSalesUnitPrice
        , ExtendedAmount
        , MerchandiseHierarchyLevel
        , ReturnItemLink
        , ItemType
        , POSItemID
        from
          {fraud}.Return
      )
      b
      on
        a.retailstoreid             =b.retailstoreid
        and a.WorkstationID         =b.WorkstationID
        and a.SequenceNumber        =b.SequenceNumber
        and a.ReceiptDateTime       =b.ReceiptDateTime
        and a.LineItemSequenceNumber=b.LineItemSequenceNumber
    JOIN
      (select retailstoreid,WorkstationID,ReceiptDateTime,SequenceNumber,TransactionGrandAmount from {fraud}.RetailTransactionHeader) c
      on
        a.retailstoreid      =c.retailstoreid
        and a.WorkstationID  =c.WorkstationID
        and a.SequenceNumber =c.SequenceNumber
        and a.ReceiptDateTime=c.ReceiptDateTime
  where
    b.ItemType!='Deposit' and VoidFlagDerived = 'false') LineItem_Derived_return_bon
      union all
      select *
      from
        (  Select
    a.retailstoreid
  , a.WorkstationID
  , a.SequenceNumber
  , a.ReceiptDateTime
  , a.LineItemSequenceNumber
  , b.RegularSalesUnitPrice
  , b.ExtendedAmount
  , b.MerchandiseHierarchyLevel
  , b.SaleItemLink as ItemLink
  , b.POSItemID
  , c.TransactionGrandAmount as TransactionGrandAmount
  From
    (select retailstoreid,WorkstationID,ReceiptDateTime,SequenceNumber,LineItemSequenceNumber,VoidFlagDerived from {fraud}.lineitemderived) a
    JOIN
      (
        select
          retailstoreid
        , WorkstationID
        , SequenceNumber
        , ReceiptDateTime
        , LineItemSequenceNumber
        , RegularSalesUnitPrice
        , ExtendedAmount
        , MerchandiseHierarchyLevel
        , SaleItemLink
        , ItemType
        , POSItemID
        from
          {fraud}.Sale
      )
      b
      on
        a.retailstoreid             =b.retailstoreid
        and a.WorkstationID         =b.WorkstationID
        and a.SequenceNumber        =b.SequenceNumber
        and a.ReceiptDateTime       =b.ReceiptDateTime
        and a.LineItemSequenceNumber=b.LineItemSequenceNumber
    JOIN
      (select retailstoreid,WorkstationID,ReceiptDateTime,SequenceNumber,TransactionGrandAmount from {fraud}.RetailTransactionHeader) c
      on
        a.retailstoreid      =c.retailstoreid
        and a.WorkstationID  =c.WorkstationID
        and a.SequenceNumber =c.SequenceNumber
        and a.ReceiptDateTime=c.ReceiptDateTime
  where
    b.ItemType!='Deposit' and VoidFlagDerived = 'false') LineItem_Derived_sale_bon
    )
    a""".format(fraud=database))


    spark.sql(""" create or replace temporary view TB_All_receipts as
select
            dsc.retailstoreid
        , dsc.WorkstationID
        , dsc.SequenceNumber
        , dsc.ReceiptDateTime
        , class_id
        , to_date(dsc.ReceiptDateTime) as ReceiptDate
        , row_number() over(partition by dsc.retailstoreid,dsc.WorkstationID order by
                            dsc.ReceiptDateTime) as row_id
        , line.MerchandiseHierarchyLevel
        , line.RegularSalesUnitPrice
        , line.ExtendedAmount
        , line.POSItemID
        , TransactionGrandAmount
        , case
                        when class_id like 'AA%'
                                    then '1'
                        when class_id like 'EA%'
                                    then '1'
                        when class_id like 'CA%'
                                    then '1'
                        when class_id like 'AN%'
                                    then '0'
                        when class_id like 'EN%'
                                    then '0'
                        when class_id like 'CN%'
                                    then '0'
                                    else 'I'
            end as void_class_flag
from
            {fraud}.df_classification_description dsc
            join
			TB_LineItem_Derived_bon line

                        
                        on
                                    dsc.retailstoreid      =line.retailstoreid
                                    and dsc.WorkstationID  =line.WorkstationID
                                    and dsc.SequenceNumber =line.SequenceNumber
                                    and dsc.ReceiptDateTime=line.ReceiptDateTime
                                    and dsc.Transactiontype='RetailTransaction'""".format(fraud=database))

   # spark.sql("""create or replace temporary view TB_Cancel_receipts as select * from TB_All_receipts where void_class_flag='1'""")


    spark.sql(""" create or replace temporary view TB_top_10_receipts
as
  select
    der.retailstoreid
  , der.WorkstationID
  , der.SequenceNumber
  , der.ReceiptDateTime_1 as ReceiptDateTime
  , row_num
  , item_match
  , TrnAmtMatch
  , row_number() over(partition by der.retailstoreid,der.WorkstationID,der.SequenceNumber,der.ReceiptDateTime_1 order by
                      der.row_num) as row_val
  from
    (
      select
        cncl.retailstoreid
      , cncl.WorkstationID
      , cncl.SequenceNumber
      , cncl.ReceiptDateTime as ReceiptDateTime_1
      , stg.ReceiptDateTime  as ReceiptDateTime_2
      , row_number() over(partition by cncl.retailstoreid,cncl.WorkstationID,cncl.SequenceNumber,cncl.ReceiptDateTime order by
                          stg.ReceiptDateTime) as row_num
      , count
          (
            case
              when cncl.TransactionGrandAmount=stg.TransactionGrandAmount
                then 1
            end
          )
        as TrnAmtMatch
      , count
          (
            case
              when
                  (
                    case
                      when cncl.POSItemID is not NULL
                        then cast(cncl.POSItemID as                 varchar(20))
                        else cast(cncl.MerchandiseHierarchyLevel as varchar(20))
                    end
                  )
                =
                  (
                    case
                      when stg.POSItemID is not NULL
                        then cast(stg.POSItemID as                 varchar(20))
                        else cast(stg.MerchandiseHierarchyLevel as varchar(20))
                    end
                  )
                and
                  (
                      (
                        case
                          when stg.RegularSalesUnitPrice!=0
                            then stg.RegularSalesUnitPrice
                            else stg.ExtendedAmount
                        end
                      )
                    =
                      (
                        case
                          when cncl.RegularSalesUnitPrice!=0
                            then cncl.RegularSalesUnitPrice
                            else cncl.ExtendedAmount
                        end
                      )
                  )
                then 1
            end
          )
        *1.0/count(distinct cncl.row_id) as item_match
      from
        (select * from TB_All_receipts where void_class_flag='1') cncl
        join
          TB_All_receipts stg
          on
            stg.retailstoreid      =cncl.retailstoreid
            and stg.WorkstationID  =cncl.WorkstationID
            and stg.ReceiptDate    =cncl.ReceiptDate
            and stg.row_id        >=cncl.row_id
            and stg.void_class_flag='0'
      group by
        cncl.retailstoreid
      , cncl.WorkstationID
      , cncl.SequenceNumber
      , cncl.ReceiptDateTime
      , stg.ReceiptDateTime
    )
    der
  where
    item_match  >=0.5
    and row_num <=10
  order by
    der.retailstoreid
  , der.WorkstationID
  , der.SequenceNumber
  , der.ReceiptDateTime_1
  , row_num""")


    spark.sql(""" create or replace temporary view TB_Bonstorno_Table_1
as
  select
    rec.retailstoreid
  , rec.WorkstationID
  , rec.SequenceNumber
  , rec.ReceiptDateTime
  , case
      when Cashcnt>0
        then concat('V',case
          when row_num>8
            then '9'
            else row_num
        end )
      when TrnAmtMatch >0
        and cardcnt    >0
        then concat('K',case
          when row_num>8
            then '9'
            else row_num
        end)
    end as Bonstorno_Flag
  from
    TB_top_10_receipts rec
    LEFT JOIN
      (
        select
          td.retailstoreid
        , td.WorkstationID
        , td.SequenceNumber
        , td.ReceiptDateTime
        , count
            (
              case
                when TenderID='YTCS'
                  then 1
              end
            )
          as Cashcnt
        , count
            (
              case
                when TenderID in ('YOAM'
                                , 'YOMA'
                                , 'YOMC'
                                , 'YOV0'
                                , 'YOVI'
                                , 'YOVO'
                                , 'YTAC'
                                , 'YTAM'
                                , 'YTDC'
                                , 'YTDD'
                                , 'YTDI'
                                , 'YTDK'
                                , 'YTDP'
                                , 'YTEA'
                                , 'YTEC'
                                , 'YTEK'
                                , 'YTGG'
                                , 'YTGK'
                                , 'YTKR'
                                , 'YTKT'
                                , 'YTLV'
                                , 'YTMA'
                                , 'YTMB'
                                , 'YTMC'
                                , 'YTNG'
                                , 'YTOF'
                                , 'YTSO'
                                , 'YTVB'
                                , 'YTVC'
                                , 'YTVE'
                                , 'YTVG'
                                , 'YTVI'
                                , 'YTVO'
                                , 'YTVP'
                                , 'YTVQ')
                  then 1
              end
            )
          as cardcnt
        from
          {fraud}.Tender td
        group by
          td.retailstoreid
        , td.WorkstationID
        , td.SequenceNumber
        , td.ReceiptDateTime
      )
      d
      on
        rec.retailstoreid      =d.retailstoreid
        and rec.WorkstationID  =d.WorkstationID
        and rec.SequenceNumber =d.SequenceNumber
        and rec.ReceiptDateTime=d.ReceiptDateTime
  where
    row_val=1""".format(fraud=database))


    spark.sql(""" create or replace temporary view TB_Bonstorno_Flags
as
  select
    a.retailstoreid
  , a.WorkstationID
  , a.SequenceNumber
  , a.ReceiptDatetime
  , case
      when max(d.TenderChangeAmount) is not null
        then 1
        else 0
    end as original_receipt_contains_change_flag
  , case
      when count
          (
            case
              when st.StoreID is not null and a.MerchandiseHierarchyLevel in 
             (01,
              02, 
              03, 
              15, 
              21, 
              22) then 1
              when st.StoreID is null and a.MerchandiseHierarchyLevel in (01
                                                 , 04
                                                 , 06
                                                 , 12
                                                 , 17)                then 1
            end
          )
        >0
        then 1
        else 0
    end as Suspicious_Product_Groups_flag
  , case
      when count
          (
            case
              when der.retailstoreid is not null
                then 1
            end
          )
        >0
        then 1
        else 0
    end as Without_FollowUp_Sales_flag
  from
    (select * from TB_All_receipts where void_class_flag='1') a
    left join
      {fraud}.Tender d
      on
        a.retailstoreid       = d.retailstoreid
        and a.WorkstationID   = d.WorkstationID
        and a.SequenceNumber  = d.SequenceNumber
        and a.ReceiptDatetime = d.ReceiptDateTime
    left join (select distinct StoreID	from {fraud}.stores where IsEBUSStore=1) st
        on a.retailstoreid       = st.StoreID
    left join
      (
        select
          a.*
        , row_number() over(partition by a.retailstoreid,a.WorkstationID,to_date(a.ReceiptDateTime),d.EmployeeID order by
                            a.ReceiptDateTime desc) as row_num
        from
          TB_All_receipts a
          join
            {fraud}.TransactionHeader d
            on
              a.retailstoreid       = d.retailstoreid
              and a.WorkstationID   = d.WorkstationID
              and a.SequenceNumber  = d.SequenceNumber
              and a.ReceiptDatetime = d.ReceiptDateTime
              and void_class_flag   ='0'
      )
      der
      on
        a.retailstoreid       = der.retailstoreid
        and a.WorkstationID   = der.WorkstationID
        and a.SequenceNumber  = der.SequenceNumber
        and a.ReceiptDatetime = der.ReceiptDateTime
        and der.row_num       =1
  group by
    a.retailstoreid
  , a.WorkstationID
  , a.SequenceNumber
  , a.ReceiptDatetime""".format(fraud=database))



    spark.sql(""" create or replace temporary view TB_Bonstorno_Table_2 as
select distinct
        cncl.retailstoreid
        , cncl.WorkstationID
        , cncl.SequenceNumber
        , cncl.ReceiptDateTime
        , case
                    when original_receipt_contains_change_flag  =0
                            and Suspicious_Product_Groups_flag=0
                            and Without_FollowUp_Sales_flag   =0
                            then 'AA'
                    when original_receipt_contains_change_flag  =0
                            and Suspicious_Product_Groups_flag=0
                            and Without_FollowUp_Sales_flag   =1
                            then 'AC'
                    when original_receipt_contains_change_flag  =0
                            and Suspicious_Product_Groups_flag=1
                            and Without_FollowUp_Sales_flag   =0
                            then 'WA'
                    when original_receipt_contains_change_flag  =0
                            and Suspicious_Product_Groups_flag=1
                            and Without_FollowUp_Sales_flag   =1
                            then 'WC'
                    when original_receipt_contains_change_flag  =1
                            and Suspicious_Product_Groups_flag=0
                            and Without_FollowUp_Sales_flag   =0
                            then 'AB'
                    when original_receipt_contains_change_flag  =1
                            and Suspicious_Product_Groups_flag=0
                            and Without_FollowUp_Sales_flag   =1
                            then 'AD'
                    when original_receipt_contains_change_flag  =1
                            and Suspicious_Product_Groups_flag=1
                            and Without_FollowUp_Sales_flag   =0
                            then 'WB'
                    when original_receipt_contains_change_flag  =1
                            and Suspicious_Product_Groups_flag=1
                            and Without_FollowUp_Sales_flag   =1
                            then 'WD'
        end as Bonstorno_Flag
from
        (select * from TB_All_receipts where void_class_flag='1') cncl
        join
                    TB_Bonstorno_Flags fl
                    on
                            cncl.retailstoreid      =fl.retailstoreid
                            and cncl.WorkstationID  =fl.WorkstationID
                            and cncl.SequenceNumber =fl.SequenceNumber
                            and cncl.ReceiptDateTime=fl.ReceiptDateTime
        left join
                    TB_Bonstorno_Table_1 bn
                    on
                            cncl.retailstoreid      =bn.retailstoreid
                            and cncl.WorkstationID  =bn.WorkstationID
                            and cncl.SequenceNumber =bn.SequenceNumber
                            and cncl.ReceiptDateTime=bn.ReceiptDateTime
where
        bn.retailstoreid is null""")


    DF_ReceiptVoidingTable=spark.sql("""
select
        bon.retailstoreid              as retailstoreid_1
        , bon.WorkstationID              as WorkstationID_1
        , bon.SequenceNumber             as SequenceNumber_1
        , bon.ReceiptDateTime            as ReceiptDateTime_1
        , rtl_org.ReceiptNumber          as ReceiptNumber_1
        , rtl_void.RetailStoreID         as RetailStoreID_2
        , rtl_void.WorkstationID         as WorkstationID_2
        , rtl_void.SequenceNumber        as SequenceNumber_2
        , rtl_void.ReceiptDateTime       as ReceiptDateTime_2
        , rtl_void.ReceiptNumber         as ReceiptNumber_2
        , bon.Bonstorno_Flag             as BonStornoFlag
        , class_org.class_id             as TransactionClassID_1
        , LKP_org.class_desc_en    as TransactionClassDescr_1
        , class_void.class_id            as TransactionClassID_2
        , LKP_void.class_desc_en   as TransactionClassDescr_2
        , rtl_org.TransactionGrandAmount as TransactionGrandAmount
        , rtl_org.NegativeTotalFlag      as NegativeTotalFlag
        , trn_hdr.ApproverEmployeeID     as Approver
from
        (select * from TB_Bonstorno_Table_1 union all 
		 select * from TB_Bonstorno_Table_2) bon
        join
                    (select retailstoreid,WorkstationID,ReceiptDateTime,SequenceNumber,TransactionGrandAmount,ReceiptNumber,
					NegativeTotalFlag from {fraud}.RetailTransactionHeader) rtl_org
                    ON
                            rtl_org.retailstoreid       = bon.retailstoreid
                            AND rtl_org.WorkstationID   = bon.WorkstationID
                            AND rtl_org.SequenceNumber  = bon.SequenceNumber
                            AND rtl_org.ReceiptDateTime = bon.ReceiptDateTime
        join
                    (select retailstoreid,WorkstationID,ReceiptDateTime,SequenceNumber,ReceiptNumber,NegativeTotalFlag,
					TransactionLinkRetailStoreID,TransactionLinkWorkstationID,TransactionLinkSequenceNumber,
					TransactionLinkReceiptDateTime from {fraud}.RetailTransactionHeader) rtl_void
                    ON
                            rtl_void.TransactionLinkRetailStoreID       = bon.retailstoreid
                            AND rtl_void.TransactionLinkWorkstationID   = bon.WorkstationID
                            AND rtl_void.TransactionLinkSequenceNumber  = bon.SequenceNumber
                            AND rtl_void.TransactionLinkReceiptDateTime = bon.ReceiptDateTime
        join
                    {fraud}.df_classification_description class_org
                    ON
                            class_org.retailstoreid       = rtl_org.retailstoreid
                            AND class_org.WorkstationID   = rtl_org.WorkstationID
                            AND class_org.SequenceNumber  = rtl_org.SequenceNumber
                            AND class_org.ReceiptDateTime = rtl_org.ReceiptDateTime
                            and class_org.Transactiontype ='RetailTransaction'
        join
                    {fraud}.df_classification_description class_void
                    ON
                            class_void.retailstoreid       = rtl_void.retailstoreid
                            AND class_void.WorkstationID   = rtl_void.WorkstationID
                            AND class_void.SequenceNumber  = rtl_void.SequenceNumber
                            AND class_void.ReceiptDateTime = rtl_void.ReceiptDateTime
                            and class_void.Transactiontype ='RetailTransaction'
        join
                    (select retailstoreid,WorkstationID,ReceiptDateTime,SequenceNumber,ApproverEmployeeID from {fraud}.TransactionHeader) trn_hdr
                    ON
                            rtl_void.retailstoreid       = trn_hdr.retailstoreid
                            AND rtl_void.WorkstationID   = trn_hdr.WorkstationID
                            AND rtl_void.SequenceNumber  = trn_hdr.SequenceNumber
                            AND rtl_void.ReceiptDateTime = trn_hdr.ReceiptDateTime
        LEFT JOIN
                    {fraud}.Lkp_Classification LKP_org
                    on
                            class_org.class_id=LKP_org.class_id
        LEFT JOIN
                    {fraud}.Lkp_Classification LKP_void
                    on
                            class_void.class_id=LKP_void.class_id""".format(fraud=database))

    #---------------------------------------------------------------------------------------------------------#
    #Writes into parquet and alter hive table
    #---------------------------------------------------------------------------------------------------------#
    
    start = time.time()
    if config['input']['file_type']['file_type']=='parquet':
        DF_ReceiptVoidingTable.coalesce(10).write.partitionBy('retailstoreid_1').mode('overwrite').parquet('{path}/{epocid_r}'.format(epocid_r=epocid,path=path))

        drppart=','.join(['partition('+ part +')' for part in map(lambda x:str(x[0]),spark.sql("show partitions {fraud}.df_receiptvoidingtable".format(fraud=database)).collect())]) 

        if len(drppart) != 0:        
            spark.sql("alter table {fraud}.df_receiptvoidingtable drop if exists ".format(fraud=database)+drppart)   
            
        spark.sql("""alter table {fraud}.df_receiptvoidingtable set location '{path}/{epocid_r}'""".format(epocid_r=epocid,path=path,fraud=database))
        
        spark.sql("msck repair table {fraud}.df_receiptvoidingtable".format(fraud=database))
    
    else:
        logger.info("writing delta")
        DF_ReceiptVoidingTable.coalesce(10).write.partitionBy('retailstoreid_1').mode('overwrite').option("overwriteSchema", "true").format("delta").save('{path}/{epocid_r}'.format(epocid_r=epocid,path=path))
        spark.sql("""create table if not exists {fraud}.df_receiptvoidingtable location '{path}/{epocid_r}'""".format(epocid_r=epocid,path=path,fraud=database))
        spark.sql("""alter table {fraud}.df_receiptvoidingtable set location '{path}/{epocid_r}'""".format(epocid_r=epocid,path=path,fraud=database))
        
    #------------------------------------------------------------------------------------------------------------#
    #Logger info updated
    #------------------------------------------------------------------------------------------------------------
    logger.info("ReceiptVoidingTable parquet written in path : %s",'{path}/{epocid_r}'.format(epocid_r=epocid,path=path))
    bon_count = spark.sql("""select count(*) as count,bonstornoflag from {fraud}.df_receiptvoidingtable
                              group by bonstornoflag""".format(fraud=database))
    bon_count1=bon_count.toPandas()
    logger.debug(bon_count1.to_string(index = False)) 

    #------------------------------------------------------------------------------------------------------------#
    #Drop temp tables
    #------------------------------------------------------------------------------------------------------------
    
    spark.catalog.dropTempView('TB_LineItem_Derived_bon')
    spark.catalog.dropTempView('TB_All_receipts')
    spark.catalog.dropTempView('TB_Cancel_receipts')
    spark.catalog.dropTempView('TB_top_10_receipts')
    spark.catalog.dropTempView('TB_Bonstorno_Table_1')
    spark.catalog.dropTempView('TB_Bonstorno_Flags')
    spark.catalog.dropTempView('TB_Bonstorno_Table_2')
    spark.catalog.dropTempView('TB_Bonstorno_Table')
