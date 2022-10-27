# Databricks notebook source
import time
def create_flags_rules_classification_dataframe(spark,epocid,logger,config):
    

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
    #Logger information about the program execution and stores
    #------------------------------------------------------------------------------------------------------------#

    logger.info("Creating Flags_Rules_Classification Flags Dataframe")

    #------------------------------------------------------------------------------------------------------------#
    #Variable for parqut path  created
    #------------------------------------------------------------------------------------------------------------
    
    path     = config["output"]["ruleengine"]["Classification_Description_folder"]
    database = config["input"]["hive"]["database"]
    
    #------------------------------------------------------------------------------------------------------------#    
    #Transaction classification data frame creation begins
    #------------------------------------------------------------------------------------------------------------#

    spark.sql("""create or replace temporary view ReceiptPositionAddonList_DER as
    select distinct
    der1.*
    , der2.EmptiesStoreID
    , der3.EmptiesWorkstationID
    from
    (
    select
    AD1.retailstoreid
    , AD1.WorkstationID
    , AD1.SequenceNumber
    , AD1.ReceiptDateTime
    , AD1.ReceiptNumber
    , AD1.LineItemSequenceNumber
    , AD1.AddonValue as EmptiesReceiptNumber
    from
    {fraud}.ReceiptPositionAddonList AD1
    where
    AD1.AddonKey='RECEIPT_NUMBER'
    )
    der1
    join
    (
    select
    AD2.retailstoreid
    , AD2.WorkstationID
    , AD2.SequenceNumber
    , AD2.ReceiptDateTime
    , AD2.ReceiptNumber
    , AD2.LineItemSequenceNumber
    , AD2.AddonValue as EmptiesStoreID
    from
    {fraud}.ReceiptPositionAddonList AD2
    where
    AD2.AddonKey='STORE_NUMBER'
    )
    der2
    on
    der1.retailstoreid             = der2.retailstoreid
    and der1.WorkstationID         = der2.WorkstationID
    and der1.ReceiptDateTime       = der2.ReceiptDateTime
    and der1.ReceiptNumber         =der2.ReceiptNumber
    and der1.LineItemSequenceNumber=der2.LineItemSequenceNumber
    join
    (
    select
    AD3.retailstoreid
    , AD3.WorkstationID
    , AD3.SequenceNumber
    , AD3.ReceiptDateTime
    , AD3.ReceiptNumber
    , AD3.LineItemSequenceNumber
    , AD3.AddonValue as EmptiesWorkstationID
    from
    {fraud}.ReceiptPositionAddonList AD3
    where
    AD3.AddonKey='WORKSTATION_ID'
    )
    der3
    on
    der1.retailstoreid             = der3.retailstoreid
    and der1.WorkstationID         = der3.WorkstationID
    and der1.ReceiptDateTime       = der3.ReceiptDateTime
    and der1.ReceiptNumber         =der3.ReceiptNumber
    and der1.LineItemSequenceNumber=der3.LineItemSequenceNumber""".format(fraud=database))

     #--------------------------------------------------------------------------------------------------------------#
    #Product booking ratio is calculated and stored at first. It will be used in determining mostlyproductgroupbookingsratio_Flag, onlyproductgroupbooking_Flag and onlyproductgroupbooking_empty_Flag
    #--------------------------------------------------------------------------------------------------------------#


    spark.sql("""
        create
    or
    replace temporary view TB_Product_Ratio as
    Select
     HDR.retailstoreid
    , HDR.WorkstationID
    , HDR.SequenceNumber
    , HDR.ReceiptDateTime
    , HDR.Transactiontype
    , (COUNT
      (
       CASE
        WHEN (
          EntryMethod='Keyed'
         )
         AND (
          SL.MerchandiseStructureItemFlag  ='true'
          or c.MerchandiseStructureItemFlag='true'
         )
         AND (
          VoidFlagDerived='false'
         )
         THEN 1
         ELSE NULL
       END
      )
     )*1.0/ NULLIF((COUNT
      (
       CASE
        WHEN VoidFlagDerived='false'
         THEN 1
         ELSE NULL
       END
      )
     ),0) AS productgroupbookingsratio_Val
     ,(COUNT
      (
       CASE
        WHEN (
          EntryMethod='Keyed'
         )
         AND (
          SL.MerchandiseStructureItemFlag  ='true'
          or c.MerchandiseStructureItemFlag='true'
         )
         AND (
          VoidFlagDerived='false' OR VoidFlagDerived='true'
         )
         THEN 1
         ELSE NULL
       END
      )
     )*1.0/ NULLIF((COUNT
      (
       CASE
        WHEN (VoidFlagDerived='false' OR VoidFlagDerived='true')
         THEN 1
         ELSE NULL
       END
      )
     ),0) AS productgroupbookingsratio_Val_ALL
    FROM
     (select retailstoreid,WorkstationID,ReceiptDateTime,SequenceNumber,Transactiontype from {fraud}.TransactionHeader) HDR
     LEFT JOIN
      {fraud}.RetailTransactionHeader a
      on
       a.retailstoreid      =HDR.retailstoreid
       and a.WorkstationID  =HDR.WorkstationID
       and a.SequenceNumber =HDR.SequenceNumber
       and a.ReceiptDateTime=HDR.ReceiptDateTime
     LEFT JOIN
      (select retailstoreid,WorkstationID,ReceiptDateTime,SequenceNumber,LineItemSequenceNumber,VoidFlagDerived,EntryMethod from {fraud}.lineitemderived) b
      on
       HDR.retailstoreid      =b.retailstoreid
       and HDR.WorkstationID  =b.WorkstationID
       and HDR.SequenceNumber =b.SequenceNumber
       and HDR.ReceiptDateTime=b.ReceiptDateTime
     LEFT JOIN
      (select retailstoreid,WorkstationID,ReceiptDateTime,SequenceNumber,LineItemSequenceNumber,MerchandiseStructureItemFlag from {fraud}.Return) c
      on
       HDR.retailstoreid           =c.retailstoreid
       and HDR.WorkstationID       =c.WorkstationID
       and HDR.SequenceNumber      =c.SequenceNumber
       and HDR.ReceiptDateTime     =c.ReceiptDateTime
       and b.LineItemSequenceNumber=c.LineItemSequenceNumber
     LEFT JOIN
      (select retailstoreid,WorkstationID,ReceiptDateTime,SequenceNumber,LineItemSequenceNumber,MerchandiseStructureItemFlag from {fraud}.Sale) SL
      on
       HDR.retailstoreid           =SL.retailstoreid
       and HDR.WorkstationID       =SL.WorkstationID
       and HDR.SequenceNumber      =SL.SequenceNumber
       and HDR.ReceiptDateTime     =SL.ReceiptDateTime
       and b.LineItemSequenceNumber=SL.LineItemSequenceNumber
    where
     HDR.Transactiontype='RetailTransaction'
    group by
     HDR.retailstoreid
    , HDR.WorkstationID
    , HDR.SequenceNumber
    , HDR.ReceiptDateTime
    , HDR.Transactiontype
    order by
     HDR.retailstoreid
    , HDR.WorkstationID
    , HDR.SequenceNumber
    , HDR.ReceiptDateTime
    , HDR.Transactiontype
    """.format(fraud=database))





    #--------------------------------------------------------------------------------------------------------------#
    #Following code creates Classification flags 
    #--------------------------------------------------------------------------------------------------------------#

    spark.sql("""
    create
or
replace temporary view TB_classification_desc as
Select
 HDR.RetailStoreID
, HDR.WorkstationID
, HDR.SequenceNumber
, HDR.ReceiptDateTime
, HDR.FunctionLogStoreInfoID
, HDR.Transactiontype
, productgroupbookingsratio_Val
, CASE
  WHEN Count
    (
     case
      when CancelFlag = 'true'
       then 1
       else null
     end
    )
   > 0
   then 1
   else 0
 end as Cancel_Flag
,CASE
  WHEN Count
    (
     case
      when TrainingModeFlag = 'true'
       then 1
       else null
     end
    )
   > 0
   then 1
   else 0
 end as TrainingMode_Flag
, CASE
  WHEN Round(ABS((SUM
    (
     case
      when VoidFlagDerived='false'
       THEN -1*c.ExtendedAmount
     END
    )
   +SUM
    (
     case
      when VoidFlagDerived='false'
       THEN SL.ExtendedAmount
     END
    )
   )),2)!=max(a.TransactionGrandAmount)
   THEN 1
  WHEN count
    (
     case
      when c.retailstoreid is not null
       then
       case
        when Rtl_P.action='With_Replace'
         then
         case
          when abs(cast(Round(Rtl_P.Replace_amount*c.Quantity*(coalesce(c.Units,1))+coalesce(final_modifier,0),2) as decimal(20,2)) - c.ExtendedAmount) >0.02
           then 1
         end
         else
         case
          when abs(cast(Round(c.RegularSalesUnitPrice*c.Quantity*(coalesce(c.Units,1))+coalesce(final_modifier,0),2) as decimal(20,2)) - c.ExtendedAmount) >0.02
           then
           case
            when abs(cast(Round((cast(left(right(c.POSItemID,6),5) as integer)*1.00/100)+coalesce(final_modifier,0),2) as decimal(20,2)) - c.ExtendedAmount) >0.02
             then 1
           end
         end
       end
      when SL.retailstoreid is not null
       then
       case
        when Rtl_P.action='With_Replace'
         then
         case
          when abs(cast(Round(Rtl_P.Replace_amount*SL.Quantity*(coalesce(SL.Units,1))+coalesce(final_modifier,0),2) as decimal(20,2))- SL.ExtendedAmount) > 0.02
           then 1
         end
         else
         case
          when abs(cast(Round(SL.RegularSalesUnitPrice*SL.Quantity*(coalesce(SL.Units,1))+coalesce(final_modifier,0),2) as decimal(20,2)) - SL.ExtendedAmount) >0.02
           then
           case
            when abs(cast(Round((cast(left(right(SL.POSItemID,6),5) as integer)*1.00/100)+coalesce(final_modifier,0),2) as decimal(20,2)) - SL.ExtendedAmount) >0.02
             then 1
           end
         end
       end
     end
    )
   >0
   then 1
   ELSE 0
 END AS Incorrect_receipt_Flag
, CASE
  WHEN COUNT
    (
     CASE
      WHEN a.TransactionLinkretailstoreid IS NOT NULL
       and a.TransactionLinkReasonCode              ='Voided'
       and a.TransactionStatus                      ='PostVoided'
       THEN 1
       ELSE NULL
     END
    )
   >0
   THEN 'Z'
  WHEN COUNT
    (
     CASE
      WHEN VOIDED_TRNS.TransactionLinkretailstoreid IS NOT NULL
       THEN 1
       ELSE NULL
     END
    )
   >0
   THEN '1'
   ELSE '0'
 END AS Void_flag
, CASE
  WHEN Count
    (
     case
      when TransactionStatus in ('Suspended'
                                ,'SuspendedRetrieved')
       then 1
       else null
     end
    )
   > 0
   then 1
   else 0
 end as put_on_hold_Flag
, case
  when COUNT
    (
     CASE
      WHEN (
        VoidFlagDerived='false'
       )
       THEN c.LineItemSequenceNumber
       ELSE NULL
     END
    )
   =0
   then 0
   else
   case
    when (
      COUNT
       (
        CASE
         When(
           VoidFlagDerived           = 'false'
           AND c.ItemType            in  ('DepositRefund','Deposit')
           AND d.retailstoreid is NULL
          )
          then c.ItemType
        END
       )
      =COUNT
       (
        CASE
         WHEN (
           VoidFlagDerived='false'
          )
          THEN b.LineItemSequenceNumber
          ELSE NULL
        END
       )
      AND COUNT
       (
        CASE
         when Adon.retailstoreid is NULL
          then 1
        END
       )
      = COUNT(b.LineItemSequenceNumber)
      AND COUNT
       (
        CASE
         when a.TransactionTaxAmount             =0
          and NegativeTransactionTaxAmount is null
          then 1
        END
       )
      >=1
     )
     then 1
     else 0
   END
 END AS empty_receipt_Flag
, case
  when COUNT
    (
     CASE
      WHEN (
        VoidFlagDerived='false'
       )
       THEN c.LineItemSequenceNumber
       ELSE NULL
     END
    )
   =0
   then 0
   else
   CASE
    WHEN (
      COUNT
       (
        CASE
         WHEN (
           VoidFlagDerived='false'
          )
          AND (
           c.ItemType in  ('DepositRefund','Deposit')
          )
          AND (
           EntryMethod='Scanned'
          )
          THEN c.LineItemSequenceNumber
          ELSE NULL
        END
       )
     )
     = (COUNT
      (
       CASE
        WHEN (
          VoidFlagDerived='false'
         )
         THEN b.LineItemSequenceNumber
         ELSE NULL
       END
      )
     )
     Then 1
     ELSE 0
   END
 end AS Receipt_withonlyScannedempties_flag
, CASE
  WHEN COUNT
    (
     CASE
      WHEN (
        TenderChangeAmount > 0
       )
       OR (
        NegativeTotalFlag='true'
       )
       THEN 1
       ELSE NULL
     END
    )
   > 0
   THEN 1
   ELSE 0
 END AS With_CashBack_Flag
, CASE
  WHEN COUNT
    (
     CASE
      WHEN (
        TransactionStatus='Totaled'
       )
       AND (
        PointsAwarded IS NOT NULL
       )
       THEN 1
       ELSE NULL
     END
    )
   > 0
   AND (
    Count(b.LineItemSequenceNumber)=1
   )
   THEN 1
   ELSE 0
 END AS LoyaltyRewards_Flag
, CASE
  WHEN COUNT
    (
     CASE
      WHEN (
        NegativeTotalFlag = 'false'
       )
       AND (
        TransactionGrandAmount>0
       )
       THEN 1
       ELSE NULL
     END
    )
   > 0
   THEN 1
   ELSE 0
 END AS Positive_total_Flag
, case
  when COUNT
    (
     CASE
      WHEN (
        VoidFlagDerived='false'
       )
       THEN c.LineItemSequenceNumber
       ELSE NULL
     END
    )
   =0
   then 0
   else
   CASE
    WHEN (
      COUNT
       (
        CASE
         WHEN (
           VoidFlagDerived='false'
          )
          AND (
           c.ItemType not in  ('DepositRefund','Deposit')
          )
          THEN c.LineItemSequenceNumber
          ELSE NULL
        END
       )
     )
     = (COUNT
      (
       CASE
        WHEN (
          VoidFlagDerived='false'
         )
         THEN b.LineItemSequenceNumber
         ELSE NULL
       END
      )
     )
     THEN 1
     ELSE 0
   END
 end AS only_returns_Flag
, case
  when COUNT
    (
     CASE
      WHEN (
        VoidFlagDerived='false'
       )
       THEN c.LineItemSequenceNumber
       ELSE NULL
     END
    )
   =0
   then 0
   else
   CASE
    WHEN (
      COUNT
       (
        CASE
         WHEN (
           VoidFlagDerived='false'
          )
          AND (
           c.ItemType in  ('DepositRefund','Deposit')
          )
          THEN c.LineItemSequenceNumber
          ELSE NULL
        END
       )
     )
     = (COUNT
      (
       CASE
        WHEN (
          VoidFlagDerived='false'
         )
         THEN b.LineItemSequenceNumber
         ELSE NULL
       END
      )
     )
     AND max(cast(productgroupbookingsratio_Val as int)) = 1
     THEN 1
     ELSE 0
   END
 end AS only_EmptiesPayOut_withAllproductgroup_flag
, CASE
  WHEN COUNT
    (
     CASE
      WHEN (
        c.ItemType         in  ('DepositRefund','Deposit')
        AND VoidFlagDerived='false'
        AND c.Quantity     >1
       )
       OR (
        c.ItemType          in  ('DepositRefund','Deposit')
        AND VoidFlagDerived ='false'
        AND EntryMethod     ='Keyed'
        and c.ExtendedAmount>5
       )
       THEN 1
       ELSE NULL
     END
    )
   > 0
   OR SUM
    (
     CASE
      WHEN c.ItemType     in  ('DepositRefund','Deposit')
       AND VoidFlagDerived='false'
       THEN c.ExtendedAmount
     END
    )
   >20.00
   OR count
    (
     case
      when c.ItemType     in  ('DepositRefund','Deposit')
       AND VoidFlagDerived='false'
       THEN 1
     END
    )
   > count(distinct
    (
     case
      when c.ItemType     in  ('DepositRefund','Deposit')
       AND VoidFlagDerived='false'
       THEN c.ExtendedAmount
     END
    )
   )
   THEN 1
   ELSE 0
 END AS Special_Empties_Flag
, CASE
  WHEN max(productgroupbookingsratio_Val) >0.5
   and max(productgroupbookingsratio_Val) < 1
   THEN 1
   ELSE 0
 END AS mostlyproductgroupbookingsratio_Flag
, case
  when COUNT
    (
     CASE
      WHEN (
        VoidFlagDerived='false'
       )
       THEN c.LineItemSequenceNumber
       ELSE NULL
     END
    )
   =0
   then 0
   else
   CASE
    WHEN (
      COUNT
       (
        CASE
         WHEN (
           VoidFlagDerived='false'
          )
          AND (
           c.ItemType in  ('DepositRefund','Deposit')
          )
          THEN c.LineItemSequenceNumber
          ELSE NULL
        END
       )
     )
     = (COUNT
      (
       CASE
        WHEN (
          VoidFlagDerived='false'
         )
         THEN b.LineItemSequenceNumber
         ELSE NULL
       END
      )
     )
     AND max(productgroupbookingsratio_Val) >0
     THEN 1
     ELSE 0
   END
 end AS only_EmptiesPayOut_withproductgroup_flag
, case
  when (
    COUNT
     (
      CASE
       WHEN (
         c.ItemType in  ('DepositRefund','Deposit')
        )
        then 1
        else NULL
      end
     )
   )
   >= 1
   then 1
   ELSE 0
 END AS atleastoneemptiesreturn_Flag
, case
  when COUNT
    (
     CASE
      WHEN (
        DirectVoidFlag='true'
       )
       THEN b.LineItemSequenceNumber
       ELSE NULL
     END
    )
   >0
   then 0
   else
   case
    when (
      COUNT
       (
        CASE
         WHEN (
           VoidFlagDerived   ='true'
           and DirectVoidFlag='false'
          )
          then b.LineItemSequenceNumber
          else NULL
        end
       )
     )
     > 0
     then 1
     ELSE 0
   END
 END AS lineitemcancellation_nodirectvoid_Flag
, case
  when (
    COUNT
     (
      CASE
       WHEN (
         VoidFlagDerived   ='true'
         and DirectVoidFlag='false'
        )
        then b.LineItemSequenceNumber
        else NULL
      end
     )
   )
   > 0
   then 1
   ELSE 0
 END AS has_lineitemcancellation_nodirectvoid_Flag
, case
  when (
    COUNT
     (
      CASE
       WHEN (
         VoidFlagDerived   ='true'
         and DirectVoidFlag='true'
        )
        then 1
        else NULL
      end
     )
   )
   >= 1
   THEN 1
   ELSE 0
 END AS lineitemcancellation_directvoid_Flag
, case
  when COUNT
    (
     CASE
      When(
        c.ItemType in  ('DepositRefund','Deposit')
       )
       then 1
       else NULL
     end
    )
                                                      > 0
   and max(cast(productgroupbookingsratio_Val as int))=1
   THEN 1
   ELSE 0
 END as onlyproductgroupbooking_empty_Flag
, case
  when max(cast(productgroupbookingsratio_Val_ALL as int))=1
   THEN 1
   ELSE 0
 END as onlyproductgroupbooking_Flag
, case
  when max(productgroupbookingsratio_Val) > 0
   and max(productgroupbookingsratio_Val) < 0.5
   THEN 1
   ELSE 0
 END as productgroupbooking_Flag
, case
  when (
    COUNT
     (
      CASE
       WHEN (
         c.ItemType      not  in  ('DepositRefund','Deposit')
         and VoidFlagDerived='false'
        )
        then c.ItemType
        else NULL
      end
     )
   )
   >= 1
   THEN 1
   ELSE 0
 END AS return_receipt_Flag
 , CASE
  WHEN COUNT
    (
     CASE
      WHEN (
        VoidFlagDerived = 'true'
       )
       AND (
          SL.MerchandiseStructureItemFlag  ='false'
          or c.MerchandiseStructureItemFlag='false'
         )
       THEN 1
       ELSE NULL
     END
    )
   > 0
   THEN 1
   ELSE 0
 END AS Voided_Scanned_Flag
 , case 
when count(CASE WHEN (VoidFlagDerived='false') then c.lineItemSequenceNumber else NULL end)=0 then 0
when ( count(CASE WHEN(c.MerchandiseHierarchyLevel=83 and st.IsEBUSStore = 'false' and VoidFlagDerived='false') then c.LineItemSequenceNumber else NULL end)= 
      count(CASE WHEN (VoidFlagDerived='false') then c.lineItemSequenceNumber else NULL end))
      and count(CASE WHEN (VoidFlagDerived='false') then c.lineItemSequenceNumber else NULL end)> 0 THEN 1 ELSE NULL 
END AS lotto_return_flag
,case 
when count(CASE WHEN (VoidFlagDerived='false') then SL.lineItemSequenceNumber else NULL end)=0 then 0
when ( count(CASE WHEN(SL.MerchandiseHierarchyLevel=83 and st.IsEBUSStore = 'false' and VoidFlagDerived='false') then SL.LineItemSequenceNumber else NULL end)= 
      count(CASE WHEN (VoidFlagDerived='false') then SL.lineItemSequenceNumber else NULL end))
      and count(CASE WHEN (VoidFlagDerived='false') then SL.lineItemSequenceNumber else NULL end)> 0 THEN 1 ELSE NULL  
END AS lotto_sale_flag
FROM
 {fraud}.TransactionHeader HDR
 LEFT JOIN
  {fraud}.RetailTransactionHeader a
  on
   a.retailstoreid      =HDR.retailstoreid
   and a.WorkstationID  =HDR.WorkstationID
   and a.SequenceNumber =HDR.SequenceNumber
   and a.ReceiptDateTime=HDR.ReceiptDateTime
 LEFT JOIN
  {fraud}.lineitemderived b
  on
   HDR.retailstoreid      =b.retailstoreid
   and HDR.WorkstationID  =b.WorkstationID
   and HDR.SequenceNumber =b.SequenceNumber
   and HDR.ReceiptDateTime=b.ReceiptDateTime
 LEFT JOIN
  {fraud}.Return c
  on
   HDR.retailstoreid           =c.retailstoreid
   and HDR.WorkstationID       =c.WorkstationID
   and HDR.SequenceNumber      =c.SequenceNumber
   and HDR.ReceiptDateTime     =c.ReceiptDateTime
   and b.LineItemSequenceNumber=c.LineItemSequenceNumber
 LEFT JOIN
  {fraud}.Sale SL
  on
   HDR.retailstoreid           =SL.retailstoreid
   and HDR.WorkstationID       =SL.WorkstationID
   and HDR.SequenceNumber      =SL.SequenceNumber
   and HDR.ReceiptDateTime     =SL.ReceiptDateTime
   and b.LineItemSequenceNumber=SL.LineItemSequenceNumber
 LEFT JOIN
  (
   select
    retailstoreid
   , WorkstationID
   , SequenceNumber
   , ReceiptDateTime
   , sum(TenderChangeAmount) as TenderChangeAmount
   from
    {fraud}.Tender
   group by
    retailstoreid
   , WorkstationID
   , SequenceNumber
   , ReceiptDateTime
  )
  d
  on
   HDR.retailstoreid      =d.retailstoreid
   and HDR.WorkstationID  =d.WorkstationID
   and HDR.SequenceNumber =d.SequenceNumber
   and HDR.ReceiptDateTime=d.ReceiptDateTime
 LEFT JOIN
  TB_Product_Ratio PR
  on
   HDR.retailstoreid      =PR.retailstoreid
   and HDR.WorkstationID  =PR.WorkstationID
   and HDR.SequenceNumber =PR.SequenceNumber
   and HDR.ReceiptDateTime=PR.ReceiptDateTime
 LEFT JOIN
  {fraud}.LoyaltyReward e
  on
   HDR.retailstoreid           =e.retailstoreid
   and HDR.WorkstationID       =e.WorkstationID
   and HDR.SequenceNumber      =e.SequenceNumber
   and HDR.ReceiptDateTime     =e.ReceiptDateTime
   and b.LineItemSequenceNumber=e.LineItemSequenceNumber
 left join
  ReceiptPositionAddonList_DER Adon
  on
   HDR.retailstoreid           =Adon.retailstoreid
   and HDR.WorkstationID       =Adon.WorkstationID
   and HDR.ReceiptNumber       =Adon.ReceiptNumber
   and HDR.ReceiptDateTime     =Adon.ReceiptDateTime
   and b.LineItemSequenceNumber=Adon.LineItemSequenceNumber
 left join
  (
   SELECT
    retailstoreid
   , WorkstationID
   , ReceiptDateTime
   , ReceiptNumber
   , LineItemSequenceNumber
   , cast(sum
     (
      case
       when AmountAction!='Replace'
        then
        case
         when AmountAction='Subtract'
          then -1*AmountValue
          else AmountValue
        end
        else 0
      end
     )
    as decimal(20,2)) as final_modifier
   , cast(sum
     (
      case
       when AmountAction='Replace'
        then AmountValue
        else 0
      end
     )
    as decimal(20,2)) as Replace_amount
   , case
     when count
       (
        case
         when AmountAction='Replace'
          then 1
        end
       )
      > 0
      then 'With_Replace'
      else 'Without_Replace'
    end as action
   FROM
    (
     select distinct
      *
     from
      {fraud}.RetailPriceModifier
    )
    b
   group by
    retailstoreid
   , WorkstationID
   , ReceiptDateTime
   , ReceiptNumber
   , LineItemSequenceNumber
  )
  Rtl_P
  on
   HDR.retailstoreid           =Rtl_P.retailstoreid
   and HDR.WorkstationID       =Rtl_P.WorkstationID
   and HDR.ReceiptNumber       =Rtl_P.ReceiptNumber
   and HDR.ReceiptDateTime     =Rtl_P.ReceiptDateTime
   and b.LineItemSequenceNumber=Rtl_P.LineItemSequenceNumber
 LEFT JOIN
  (
   SELECT
    TransactionLinkretailstoreid
   , TransactionLinkWorkstationID
   , TransactionLinkSequenceNumber
   , TransactionLinkBeginDateTime
   , TransactionLinkReceiptNumber
   , TransactionLinkReceiptDateTime
   FROM
    {fraud}.RetailTransactionHeader
   WHERE
    TransactionLinkReasonCode='Voided'
    and TransactionStatus    ='PostVoided'
  )
  VOIDED_TRNS
  ON
   VOIDED_TRNS.TransactionLinkretailstoreid       = HDR.retailstoreid
   AND VOIDED_TRNS.TransactionLinkWorkstationID   = HDR.WorkstationID
   AND VOIDED_TRNS.TransactionLinkSequenceNumber  = HDR.SequenceNumber
   AND VOIDED_TRNS.TransactionLinkReceiptDateTime = HDR.ReceiptDateTime
 LEFT JOIN
  {fraud}.Stores st
  ON 
   st.StoreID = HDR.retailstoreid 
group by
 HDR.retailstoreid
, HDR.WorkstationID
, HDR.SequenceNumber
, HDR.ReceiptDateTime
, HDR.FunctionLogStoreInfoID
, HDR.Transactiontype
, productgroupbookingsratio_Val
order by
 HDR.retailstoreid
, HDR.WorkstationID
, HDR.SequenceNumber
, HDR.ReceiptDateTime
, HDR.FunctionLogStoreInfoID
, HDR.Transactiontype
, productgroupbookingsratio_Val
    """.format(fraud=database))




    #--------------------------------------------------------------------------------------------------------------#
    #Following code creates Classification Description DF 
    #--------------------------------------------------------------------------------------------------------------#

    spark.sql("""
    create or replace temporary view TB_Classification_Description_temp as

    select *
    , case
    when Transactiontype='RetailTransaction'
                    then
                    case
                                    when Cancel_Flag = 1
                                            then 'JV1'
                                    when TrainingMode_Flag = 1
                                            then 'JVS'
                                    when Incorrect_receipt_Flag= 1
                                            then
                                            case
                                                            when Void_flag = 0
                                                                            then 'HNK'
                                                                            ELSE 'HAK'
                                            END
                                    when Void_flag               = 'Z'
                                            AND With_CashBack_Flag=0
                                            then 'ZN1'
                                    when Void_flag               = 'Z'
                                            AND With_CashBack_Flag=1
                                            then 'ZN2'
                                    when put_on_hold_Flag         = 1
                                            and empty_receipt_Flag = 1
                                            then
                                            case
                                                            when Void_flag = 0
                                                                            then 'HNL'
                                                                            ELSE 'HAL'
                                            END
                                    when put_on_hold_Flag                   = 1
                                            and atleastoneemptiesreturn_Flag = 0
                                            then 'HNT'
                                    when put_on_hold_Flag                   = 1
                                            and atleastoneemptiesreturn_Flag = 1
                                            then 'HLT'
                                    when LoyaltyRewards_Flag = 1
                                            then 'HND'
                                 when lotto_return_flag = 0 and lotto_sale_flag=1
                                   then case 
                                        when Void_flag = 0
                                            then 'LN1'
                                            ELSE 'LA1'
                                     END
                                when lotto_sale_flag = 0 and lotto_return_flag = 1
                                    then case 
                                        when Void_flag = 0
                                            then 'LN2'
                                            ELSE 'LA2'
                                     END
                                when lotto_sale_flag=1 and lotto_return_flag = 1 
                                    then case 
                                        when Void_flag = 0
                                            then 'LN0'
                                            ELSE 'LA0'
                                     END         
                                    when Positive_total_Flag        = 1
                                            and Special_Empties_Flag = 1
                                            then
                                            case
                                                            when Void_flag = 0
                                                                            then 'AN0'
                                                                            ELSE 'AA0'
                                            END
                                    when Positive_total_Flag                        = 1
                                            and mostlyproductgroupbookingsratio_Flag = 1
                                            then
                                            case
                                                            when Void_flag = 0
                                                                            then 'AN9'
                                                                            ELSE 'AA9'
                                            END                                        
                                    when (
                                                Voided_Scanned_Flag =1 
                                                and productgroupbooking_Flag =1 
                                                and Positive_total_Flag=1
                                            )
                                            then
                                            case
                                                            when Void_flag = 0
                                                                then 'AN8'
                                                                ELSE 'AA8'
                                            END
                                    when Positive_total_Flag                     = 1
                                            and onlyproductgroupbooking_empty_Flag= 1
                                            and onlyproductgroupbooking_Flag      = 1
                                            and atleastoneemptiesreturn_Flag      = 1
                                            then
                                            case
                                                            when Void_flag = 0
                                                                            then 'AN7'
                                                                            ELSE 'AA7'
                                            END
                                    when (
                                                            Positive_total_Flag             = 1
                                                            and onlyproductgroupbooking_Flag= 1
                                            )
                                            then
                                            case
                                                            when (
                                                                                            Void_flag = 0
                                                                            )
                                                                            then 'AN6'
                                                                            ELSE 'AA6'
                                            END
                                    when Positive_total_Flag                             = 1
                                            and min2of3are_empties_immediatevoid_void_Flag= 1
                                            then
                                            case
                                                            when Void_flag = 0
                                                                            THEN 'AN5'
                                                                            ELSE 'AA5'
                                            END
                                    when Positive_total_Flag               = 1
                                            and atleastoneemptiesreturn_Flag=1
                                            then
                                            case
                                                            when Void_flag = 0
                                                                            THEN 'AN4'
                                                                            ELSE 'AA4'
                                            END
                                    when Positive_total_Flag                         = 1
                                            and lineitemcancellation_nodirectvoid_Flag=1
                                            then
                                            case
                                                            when Void_flag = 0
                                                                            THEN 'AN3'
                                                                            ELSE 'AA3'
                                            END
                                    when Positive_total_Flag                        = 1
                                            and lineitemcancellation_directvoid_Flag = 1
                                            then
                                            case
                                                            when Void_flag = 0
                                                                            THEN 'AN2'
                                                                            ELSE 'AA2'
                                            END
                                    when Positive_total_Flag= 1
                                            then
                                            case
                                                            when Void_flag = 0
                                                                            THEN 'AN1'
                                                                            ELSE 'AA1'
                                            END
                                    when only_EmptiesPayOut_withAllproductgroup_flag=1
                                            then
                                            case
                                                            when Void_flag = 0
                                                                            THEN 'EN0'
                                                                            ELSE 'EA0'
                                            END
                                    when Receipt_withonlyScannedempties_flag = 1
                                            then
                                            case
                                                            when Void_flag = 0
                                                                            THEN 'EN9'
                                                                            ELSE 'EA9'
                                            END
                                    when only_returns_Flag         = 1
                                            and return_receipt_Flag = 1
                                            then
                                            case
                                                            when Void_flag = 0
                                                                            THEN 'EN8'
                                                                            ELSE 'EA8'
                                            END
                                    when only_EmptiesPayOut_withproductgroup_flag = 1
                                            then
                                            case
                                                            when Void_flag = 0
                                                                            THEN 'EN6'
                                                                            ELSE 'EA6'
                                            END
                                    when Special_Empties_Flag = 1
                                            then
                                            case
                                                            when Void_flag = 0
                                                                            THEN 'EN7'
                                                                            ELSE 'EA7'
                                            END
                                    when productgroupbooking_Flag = 1
                                            then
                                            case
                                                            when Void_flag = 0
                                                                            THEN 'EN5'
                                                                            ELSE 'EA5'
                                            END
                                    when (
                                                            atleastoneemptiesreturn_Flag             = 1
                                                            and lineitemcancellation_directvoid_Flag = 1
                                            )
                                            or (
                                                            atleastoneemptiesreturn_Flag               = 1
                                                            and lineitemcancellation_nodirectvoid_Flag = 1
                                            )
                                            then
                                            case
                                                            when Void_flag = 0
                                                                            THEN 'EN4'
                                                                            ELSE 'EA4'
                                            END
                                    when atleastoneemptiesreturn_Flag = 1
                                            then
                                            case
                                                            when Void_flag = 0
                                                                            THEN 'EN3'
                                                                            ELSE 'EA3'
                                            END
                                    when (
                                                            lineitemcancellation_nodirectvoid_Flag   = 1
                                                            and lineitemcancellation_directvoid_Flag = 1
                                            )
                                            or (
                                                            lineitemcancellation_nodirectvoid_Flag   = 0
                                                            and lineitemcancellation_directvoid_Flag = 1
                                            )
                                            or (
                                                            lineitemcancellation_nodirectvoid_Flag   = 1
                                                            and lineitemcancellation_directvoid_Flag = 0
                                            )
                                            then
                                            case
                                                            when Void_flag = 0
                                                                            THEN 'EN2'
                                                                            ELSE 'EA2'
                                            END
                                    when has_lineitemcancellation_nodirectvoid_Flag = 0
                                            and lineitemcancellation_directvoid_Flag =0
                                            and Positive_total_Flag                  = 0
                                            and atleastoneemptiesreturn_Flag         =0
                                            then
                                            case
                                                            when Void_flag = 0
                                                                            THEN 'EN1'
                                                                            ELSE 'EA1'
                                            END
                                    when (
                                                            Special_Empties_Flag    =1
                                                            and return_receipt_Flag = 1
                                            )
                                            then
                                            case
                                                            when Void_flag = 0
                                                                            THEN 'CN0'
                                                                            ELSE 'CA0'
                                            END
                                    when (
                                                            mostlyproductgroupbookingsratio_Flag=1
                                                            and return_receipt_Flag             = 1
                                            )
                                            then
                                            case
                                                            when Void_flag = 0
                                                                            THEN 'CN9'
                                                                            ELSE 'CA9'
                                            END                                         
                                    when ( 
                                                    Voided_Scanned_Flag =1 
                                                    and productgroupbooking_Flag =1                                                                                       and return_receipt_Flag     = 1
                                            )
                                            then
                                            case
                                                            when Void_flag = 0
                                                                            THEN 'CN8'
                                                                            ELSE 'CA8'
                                            END                                   
                                    when (
                                                            onlyproductgroupbooking_empty_Flag=1
                                                            and onlyproductgroupbooking_Flag  =1
                                                            and atleastoneemptiesreturn_Flag  =1
                                                            and return_receipt_Flag           = 1
                                            )
                                            then
                                            case
                                                            when Void_flag = 0
                                                                            THEN 'CN7'
                                                                            ELSE 'CA7'
                                            END
                                    when (
                                                            onlyproductgroupbooking_Flag=1
                                                            and return_receipt_Flag     = 1
                                            )
                                            then
                                            case
                                                            when Void_flag = 0
                                                                            THEN 'CN6'
                                                                            ELSE 'CA6'
                                            END

                                    when (
                                                            min2of3are_empties_immediatevoid_void_Flag=1
                                                            and return_receipt_Flag                   = 1
                                            )
                                            then
                                            case
                                                            when Void_flag = 0
                                                                            THEN 'CN5'
                                                                            ELSE 'CA5'
                                            END
                                    when (
                                                            atleastoneemptiesreturn_Flag=1
                                                            and return_receipt_Flag     = 1
                                            )
                                            then
                                            case
                                                            when Void_flag = 0
                                                                            THEN 'CN4'
                                                                            ELSE 'CA4'
                                            END
                                    when (
                                                            lineitemcancellation_nodirectvoid_Flag=1
                                                            and return_receipt_Flag               = 1
                                            )
                                            then
                                            case
                                                            when Void_flag = 0
                                                                            THEN 'CN3'
                                                                            ELSE 'CA3'
                                            END
                                    when (
                                                            lineitemcancellation_directvoid_Flag=1
                                                            and return_receipt_Flag             = 1
                                            )
                                            then
                                            case
                                                            when Void_flag = 0
                                                                            THEN 'CN2'
                                                                            ELSE 'CA2'
                                            END
                                    when (
                                                            return_receipt_Flag = 1
                                            )
                                            then
                                            case
                                                            when Void_flag = 0
                                                                            THEN 'CN1'
                                                                            ELSE 'CA1'
                                            END
                    END
    END AS class_id
    from
    (select
    retailstoreid
    , WorkstationID
    , SequenceNumber
    , ReceiptDateTime
    , FunctionLogStoreInfoID
    , Transactiontype
    , productgroupbookingsratio_Val
    , Cancel_Flag
    , TrainingMode_Flag
    , Incorrect_receipt_Flag
    , Void_flag
    , put_on_hold_Flag
    , empty_receipt_Flag
    , Receipt_withonlyScannedempties_flag
    , With_CashBack_Flag
    , LoyaltyRewards_Flag
    , Positive_total_Flag
    , only_returns_Flag
    , only_EmptiesPayOut_withAllproductgroup_flag
    , Special_Empties_Flag
    , mostlyproductgroupbookingsratio_Flag
    , only_EmptiesPayOut_withproductgroup_flag
    , atleastoneemptiesreturn_Flag
    , lineitemcancellation_nodirectvoid_Flag
    , has_lineitemcancellation_nodirectvoid_Flag
    , lineitemcancellation_directvoid_Flag
    , onlyproductgroupbooking_empty_Flag
    , onlyproductgroupbooking_Flag
    , productgroupbooking_Flag
    , return_receipt_Flag
    ,lotto_return_flag
    ,lotto_sale_flag
    , case
    when (
             atleastoneemptiesreturn_Flag+has_lineitemcancellation_nodirectvoid_Flag+lineitemcancellation_directvoid_Flag
                    )
                    >=2
                    THEN 1
                    ELSE 0
    END as min2of3are_empties_immediatevoid_void_Flag
    ,Voided_Scanned_Flag
    from
    TB_classification_desc) TB_Classification_Flags""")

    spark.sql("""
    create or replace temporary view TB_Classification_Description_Retail as
    Select
    FLG.retailstoreid
    , FLG.WorkstationID
    , FLG.SequenceNumber
    , FLG.ReceiptDateTime
    , FLG.FunctionLogStoreInfoID
    , FLG.Transactiontype
    , FLG.class_id
    from
    TB_Classification_Description_temp FLG
    where
    Transactiontype='RetailTransaction'""")




    spark.sql("""
    create or replace temporary view TB_Classification_Description_Control as
    Select
    CD.retailstoreid
    , CD.WorkstationID
    , CD.SequenceNumber
    , CD.ReceiptDateTime
    , CD.FunctionLogStoreInfoID
    , CD.Transactiontype
    , case
                    when TrainingMode_Flag = 1
                                    then 'JVN'
                    when ControlTransactionType= 'OperatorSignOn'
                                    then 'XN1'
                    when ControlTransactionType= 'OperatorSignOff'
                                    then 'XN5'
                    when NoSale          is not null
                                    and ReasonCode is null
                                    then 'HNW'
                    when ControlTransactionType = 'SessionSettle'
                                    then 'HNJ'
                                    Else 'HNG'
    END AS class_id
    from
    TB_Classification_Description_temp CD
    left join
                    {fraud}.ControlTransaction CT
                    on
                                    CD.retailstoreid      =CT.retailstoreid
                                    and CD.WorkstationID  =CT.WorkstationID
                                    and CD.SequenceNumber =CT.SequenceNumber
                                    and CD.ReceiptDateTime=CT.ReceiptDateTime
    where
    Transactiontype='ControlTransaction'""".format(fraud=database))




    spark.sql("""
    create or replace temporary view TB_Classification_Description_Function as
    Select
    CD.retailstoreid
    , CD.WorkstationID
    , CD.SequenceNumber
    , CD.ReceiptDateTime
    , CD.FunctionLogStoreInfoID
    , CD.Transactiontype
    , case
              when TrainingMode_Flag = 1
                         then 'JVN'       
              when FunctionID             = 89
                                    and FunctionName  = 'X_PAUSE'
                                    and ParameterName = 'FUNCTION_STATE'
                                    then
                                    case
                                                            when ParameterValue = 'BEGIN'
                                                                            then 'XN7'
                                                                            ELSE 'XN3'
                                    END
                    when FunctionName               ='X_ARTICLE_INFORMATION'
                                    and CD.SequenceNumber = 0
                                    and ParameterName     = 'ITEM_EAN'
                                    then 'XPL'
                    when CD.SequenceNumber =0
                                    and (
                                                            FunctionName         != 'X_ARTICLE_INFORMATION'
                                                            or FunctionName is null
                                    )
                                    then 'XNF'
    END as class_id
    from
    TB_Classification_Description_temp CD
    left join
                    {fraud}.FunctionLog FL
                    on
                                    CD.retailstoreid             =FL.retailstoreid
                                    and CD.WorkstationID         =FL.WorkstationID
                                    and CD.SequenceNumber        =FL.SequenceNumber
                                    and CD.ReceiptDateTime       =FL.ReceiptDateTime
                                    and CD.FunctionLogStoreInfoID=FL.StoreInfoID
                                    and
                                    (
                                                            (
                                                                            FunctionID       = 89
                                                                            and FunctionName = 'X_PAUSE'
                                                            )
                                                            or
                                                            (
                                                                            FunctionName='X_ARTICLE_INFORMATION'
                                                                            and ParameterName in ('ITEM_EAN'
                                                                                                                    ,'BEGIN'
                                                                                                                    ,'END')
                                                            )
                                    )
    where
    Transactiontype='FunctionLog'""".format(fraud=database))



    spark.sql("""
    create or replace temporary view TB_Classification_Description_Tender as
    Select
    CDF.retailstoreid
    , CDF.WorkstationID
    , CDF.SequenceNumber
    , CDF.ReceiptDateTime
    , CDF.FunctionLogStoreInfoID
    , CDF.Transactiontype
    , case
                    when TrainingMode_Flag = 1
                                    then 'JVN'
                    when a.retailstoreid           is not Null
                                    and a.TenderPickUpReason is NULL
                                    then 'HNM'
                    when d.retailstoreid is not Null
                                    then 'HNO'
                    when a.retailstoreid     is not Null
                                    and a.TenderPickUpReason ='AT'
                                    then 'HNP'
                    when s.retailstoreid is not Null
                                    then 'HNP'
                    when c.retailstoreid is not Null
                                    then 'HNR'
                    when b.retailstoreid         is not Null
                                    and b.PaidInAmount is not Null
                                    then 'GN1'
                    when b.retailstoreid          is not Null
                                    and b.PaidOutAmount is not Null
                                    then 'GN5'
                    when b.retailstoreid is not Null
                                    then 'GN6'
                                    else 'TDF'
    END as class_id
    FROM
    TB_Classification_Description_temp CDF
    LEFT JOIN
                    (
                            select distinct
                                            retailstoreid
                                            , WorkstationID
                                            , SequenceNumber
                                            , ReceiptDateTime
                                            , TenderPickUpReason
                            FROM
                                            {fraud}.TenderControlPickup
                    )
                    a
                    on
                                    a.retailstoreid      =CDF.retailstoreid
                                    and a.WorkstationID  =CDF.WorkstationID
                                    and a.SequenceNumber =CDF.SequenceNumber
                                    and a.ReceiptDateTime=CDF.ReceiptDateTime
    LEFT JOIN
                    (
                            select distinct
                                            retailstoreid
                                            , WorkstationID
                                            , SequenceNumber
                                            , ReceiptDateTime
                                            , PaidInAmount
                                            , PaidOutAmount
                            FROM
                                            {fraud}.TenderControlPaymentDeposit
                    )
                    b
                    on
                                    b.retailstoreid      =CDF.retailstoreid
                                    and b.WorkstationID  =CDF.WorkstationID
                                    and b.SequenceNumber =CDF.SequenceNumber
                                    and b.ReceiptDateTime=CDF.ReceiptDateTime
    LEFT JOIN
                    (
                            select distinct
                                            retailstoreid
                                            , WorkstationID
                                            , SequenceNumber
                                            , ReceiptDateTime
                            FROM
                                            {fraud}.TenderControlLoan
                    )
                    c
                    on
                                    c.retailstoreid      =CDF.retailstoreid
                                    and c.WorkstationID  =CDF.WorkstationID
                                    and c.SequenceNumber =CDF.SequenceNumber
                                    and c.ReceiptDateTime=CDF.ReceiptDateTime
    LEFT JOIN
                    (
                            select distinct
                                            retailstoreid
                                            , WorkstationID
                                            , SequenceNumber
                                            , ReceiptDateTime
                            FROM
                                            {fraud}.TenderControlTillSettle
                    )
                    d
                    on
                                    d.retailstoreid      =CDF.retailstoreid
                                    and d.WorkstationID  =CDF.WorkstationID
                                    and d.SequenceNumber =CDF.SequenceNumber
                                    and d.ReceiptDateTime=CDF.ReceiptDateTime
    LEFT JOIN
                    (
                            select distinct
                                            retailstoreid
                                            , WorkstationID
                                            , SequenceNumber
                                            , ReceiptDateTime
                            FROM
                                            {fraud}.TenderControlSafeSettle
                    )
                    s
                    on
                                    s.retailstoreid      =CDF.retailstoreid
                                    and s.WorkstationID  =CDF.WorkstationID
                                    and s.SequenceNumber =CDF.SequenceNumber
                                    and s.ReceiptDateTime=CDF.ReceiptDateTime
    where
    Transactiontype='TenderControlTransaction'""".format(fraud=database))



    DF_Classification_Description=spark.sql("""

    select *
    from
    TB_Classification_Description_Retail
    union
    select *
    from
    TB_Classification_Description_Control
    union
    select *
    from
    TB_Classification_Description_Function
    union
    select *
    from
    TB_Classification_Description_Tender""")


    #---------------------------------------------------------------------------------------------------------#
    #Writes into parquet and alter hive table
    #---------------------------------------------------------------------------------------------------------#
    
    start = time.time()
    if config['input']['file_type']['file_type']=='parquet':
        DF_Classification_Description.coalesce(10).write.partitionBy('retailstoreid').mode('overwrite').parquet('{path}/{epocid_r}'.format(epocid_r=epocid,path=path)) 
    

        drppart=','.join(['partition('+ part +')' for part in map(lambda x:str(x[0]),spark.sql("show partitions {fraud}.df_classification_description".format(fraud=database)).collect())])

        if len(drppart) != 0:    
            spark.sql("alter table {fraud}.df_classification_description drop if exists ".format(fraud=database)+drppart)   
            
        spark.sql("""alter table {fraud}.df_classification_description set location '{path}/{epocid_r}'""".format(epocid_r=epocid,path=path,fraud=database))
        
        spark.sql("msck repair table {fraud}.df_classification_description".format(fraud=database))
        
    else:
        logger.info("writing delta")
        
        DF_Classification_Description.coalesce(10).write.partitionBy('retailstoreid').mode('overwrite').option("overwriteSchema", "true").format("delta").save('{path}/{epocid_r}'.format(epocid_r=epocid,path=path))
        
        spark.sql("""create table if not exists {fraud}.df_classification_description location '{path}/{epocid_r}'""".format(epocid_r=epocid,path=path,fraud=database))
        spark.sql("""alter table {fraud}.df_classification_description set location '{path}/{epocid_r}'""".format(epocid_r=epocid,path=path,fraud=database))
        
    #------------------------------------------------------------------------------------------------------------#
    #Logger for incorrect receipt and entropy of classification
    #------------------------------------------------------------------------------------------------------------#
    logger.info("Transaction Classification parquet written in path : %s",'{path}/{epocid_r}'.format(epocid_r=epocid,path=path))

    df_trans_log        = spark.sql('select * from {fraud}.df_classification_description'.format(fraud=database))
    
    incorrectreceiptlog = df_trans_log.select('class_id').filter("class_id='HNK'").distinct().count()
    
    #To add Incorrect receipt unique key
    if incorrectreceiptlog > 10 :
            logger.warning('Incorrect receipt flagged in the current data',incorrectreceiptlog )
    
    if incorrectreceiptlog > 0 & incorrectreceiptlog < 10:
            incorrectreceipt_keycount = spark.sql("""select distinct 
            (retailstoreid||':'||workstationid||':'||sequencenumber||':'||receiptdatetime) as incorrectreceiptkey from {fraud}.df_classification_description where
                                    class_id='HNK'""".format(fraud=database))
            logger.warning('Incorrect receipt flagged in the current data')
            logger.info(incorrectreceipt_keycount.collect())
    
    entropyvalue        = df_trans_log.select("class_id").distinct().count()
    
    if incorrectreceiptlog >= 1 :
        logger.warning('Incorrect receipt flagged in the current data')
    
    if entropyvalue < 2 :
        logger.warning('Entropy of classification is lesser than 2')
        
    trans_count = spark.sql('select class_id,count(*) as count from {fraud}.df_classification_description group by class_id'.format(fraud=database))
    trans_count1=trans_count.toPandas()
    logger.debug(trans_count1.to_string(index = False)) 

    #------------------------------------------------------------------------------------------------------------#
    #Drop temp tables
    #------------------------------------------------------------------------------------------------------------
    
    spark.catalog.dropTempView('ReceiptPositionAddonList_DER')
    spark.catalog.dropTempView('LineItem_Derived_return')
    spark.catalog.dropTempView('LineItem_Derived_sale')
    spark.catalog.dropTempView('TB_LineItem_Derived_temp')
    spark.catalog.dropTempView('TB_LineItem_Derived')
    spark.catalog.dropTempView('TB_Product_Ratio')
    spark.catalog.dropTempView('TB_classification_desc')
    spark.catalog.dropTempView('TB_Classification_Flags')
    spark.catalog.dropTempView('TB_Classification_Description_temp')
    spark.catalog.dropTempView('TB_Classification_Description_Retail')
    spark.catalog.dropTempView('TB_Classification_Description_Control')
    spark.catalog.dropTempView('TB_Classification_Description_Function')
    spark.catalog.dropTempView('TB_Classification_Description_Tender')
    spark.catalog.dropTempView('TB_Classification_Description_Control')
    
