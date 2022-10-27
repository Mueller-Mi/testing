# Databricks notebook source
import time
def create_repeated_empties_dataframe(spark,epocid,logger,config):

    """Creates repeated empties redemption dataframe
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

    path     = config["output"]["ruleengine"]["Empties_folder"]
    database = config["input"]["hive"]["database"]

    #------------------------------------------------------------------------------------------------------------#
    #Logger info updated
    #------------------------------------------------------------------------------------------------------------

    logger.info("Creating Empties Redemption Dataframe")

    #---------------------------------------------------------------------------------------------------------#
    #Following code creates the DF for Empties with HNL classes
    #---------------------------------------------------------------------------------------------------------#

    spark.sql(""" create or replace temporary view HNL_Empties_RT as
    select
        CD.retailstoreid
        , CD.WorkstationID
        , CD.ReceiptDateTime
        , RTH.ReceiptNumber
        , CD.retailstoreid           as EmptiesStoreID
        , CD.WorkstationID           as EmptiesWorkstationID
        , LEFT(RTH.ReceiptNumber,4)          as EmptiesReceiptNumber
        , RTH.TransactionGrandAmount as EmptiesTotal
        , HDR.EmployeeID
        , NULL                                                                                               AS FLAG
        , CONCAT(lpad(CD.Retailstoreid,6,'0'),'-',CD.WorkstationID,'-',
        case  when RTH.ReceiptNumber<100 then lpad(RTH.ReceiptNumber,3,'0')
		else RTH.ReceiptNumber end,'-',
        case when RTH.TransactionGrandAmount<100 then lpad(cast(RTH.TransactionGrandAmount as decimal(5,2)),5,'0') 
            else RTH.TransactionGrandAmount end) as Concatenated_Empties_ID
        , CD.class_id as class_id
        , 0 as LineItemSequenceNumber
        , NULL as EmptiesTimingFlag
        , NULL as IsVoided
    from
        (select retailstoreid,WorkstationID,ReceiptDateTime,SequenceNumber,class_id from    {fraud}.df_classification_description)  CD
        left join
                    (select retailstoreid,WorkstationID,ReceiptDateTime,SequenceNumber,
					ReceiptNumber,EmployeeID,Transactiontype from {fraud}.TransactionHeader) HDR
                    on
                            CD.retailstoreid      =HDR.retailstoreid
                            and CD.WorkstationID  =HDR.WorkstationID
                            and CD.SequenceNumber =HDR.SequenceNumber
                            and CD.ReceiptDateTime=HDR.ReceiptDateTime
        left join
                    (select retailstoreid,WorkstationID,ReceiptDateTime,SequenceNumber,
					ReceiptNumber,TransactionGrandAmount from {fraud}.RetailTransactionHeader) RTH
                    on
                            CD.retailstoreid      =RTH.retailstoreid
                            and CD.WorkstationID  =RTH.WorkstationID
                            and CD.SequenceNumber =RTH.SequenceNumber
                            and CD.ReceiptDateTime=RTH.ReceiptDateTime
    where
        CD.class_id          ='HNL'
        and HDR.Transactiontype='RetailTransaction'""".format(fraud=database))

    #----------------------------------------------------------------------------------------------------------#
    #Following code creates the data from Addon list in a usable format and created redemption data to join it with other tables
    #----------------------------------------------------------------------------------------------------------#

    spark.sql("""create or replace temporary view ReceiptPositionAddonList_RT as
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
                , LEFT(AD1.AddonValue,4) as EmptiesReceiptNumber
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
                    and der1.LineItemSequenceNumber=der3.LineItemSequenceNumber
    """.format(fraud=database))

    #-------------------------------------------------------------------------------------------------------------#
    #Following code creates the combines the redemption data with other tables for further use
    #-------------------------------------------------------------------------------------------------------------#

    spark.sql(""" create or replace temporary view Redemption_RT as
select
        AD.retailstoreid
        , AD.WorkstationID
        , AD.ReceiptDateTime
        , AD.ReceiptNumber
        , AD.EmptiesStoreID
        , AD.EmptiesWorkstationID
        , AD.EmptiesReceiptNumber
        , RT.ExtendedAmount as EmptiesTotal
        , HDR.EmployeeID
        , CASE
                    WHEN RT.ItemType                 in  ('DepositRefund','Deposit')
                                AND LI.VoidFlag       = 'true'
                                AND LI.DirectVoidFlag = 'true'
                                THEN 'PF+SO'
                    WHEN RT.ItemType                 in  ('DepositRefund','Deposit')
                                AND LI.VoidFlag       = 'true'
                                AND LI.DirectVoidFlag = 'false'
                                THEN 'PF+ZS'
                    WHEN RT.ItemType in  ('DepositRefund','Deposit')
                                THEN 'PF'
        END                                                                                                     AS FLAG
        , CONCAT(lpad(AD.EmptiesStoreID,6,'0'),'-',AD.EmptiesWorkstationID,'-',
            case  when AD.EmptiesReceiptNumber<100 then lpad(AD.EmptiesReceiptNumber,3,'0')
		else AD.EmptiesReceiptNumber end ,'-',
        case when RT.ExtendedAmount<100 then lpad(cast(RT.ExtendedAmount as decimal(5,2)),5,'0') 
                else RT.ExtendedAmount end)  as Concatenated_Empties_ID
        , CD.class_id as class_id
        , RT.LineItemSequenceNumber
from
        {fraud}.df_classification_description CD
        left join
                    (select retailstoreid,WorkstationID,ReceiptDateTime,SequenceNumber,ExtendedAmount,
					ItemType,LineItemSequenceNumber from {fraud}.Return) RT
                    on
                                CD.retailstoreid      =RT.retailstoreid
                                and CD.WorkstationID  =RT.WorkstationID
                                and CD.SequenceNumber =RT.SequenceNumber
                                and CD.ReceiptDateTime=RT.ReceiptDateTime
        left join
                    (select retailstoreid,WorkstationID,ReceiptDateTime,SequenceNumber,EmployeeID,Transactiontype
					 from {fraud}.TransactionHeader) HDR
                    on
                                CD.retailstoreid      =HDR.retailstoreid
                                and CD.WorkstationID  =HDR.WorkstationID
                                and CD.SequenceNumber =HDR.SequenceNumber
                                and CD.ReceiptDateTime=HDR.ReceiptDateTime
        left join
                    (
                            select
                                    retailstoreid
                                , WorkstationID
                                , SequenceNumber
                                , ReceiptDateTime
                                , LineItemSequenceNumber
                                , VoidFlag
                                , DirectVoidFlag
                            from
                                    {fraud}.lineitemderived
                    )
                    LI
                    on
                                RT.retailstoreid             =LI.retailstoreid
                                and RT.WorkstationID         =LI.WorkstationID
                                and RT.SequenceNumber        =LI.SequenceNumber
                                and RT.ReceiptDateTime       =LI.ReceiptDateTime
                                and RT.LineItemSequenceNumber=LI.LineItemSequenceNumber
        inner join
                    ReceiptPositionAddonList_RT AD
                    on
                                RT.retailstoreid             =AD.retailstoreid
                                and RT.WorkstationID         =AD.WorkstationID
                                and RT.SequenceNumber        =AD.SequenceNumber
                                and RT.ReceiptDateTime       =AD.ReceiptDateTime
                                and RT.LineItemSequenceNumber=AD.LineItemSequenceNumber
where
        CD.class_id != 'HNL'
        and HDR.Transactiontype='RetailTransaction'
        and RT.ItemType        in  ('DepositRefund','Deposit')
order by
        CD.retailstoreid
        , CD.WorkstationID
        , CD.SequenceNumber
        , CD.ReceiptDateTime
        , RT.LineItemSequenceNumber""".format(fraud=database))


    spark.sql("""create or replace temporary view Redemption_RT_Final as 
select
        a.retailstoreid
        , a.WorkStationID
        , a.ReceiptDateTime
        , a.ReceiptNumber
        , a.EmptiesStoreID
        , a.EmptiesWorkStationID
        , a.EmptiesReceiptNumber
        , case
                    when void_cnt>0
                            then SUM_EmptiesTotalDerived
                            else SUM_EmptiesTotalDerived1
        end as EmptiesTotal
        , EmployeeID
        , FLAG
        , CONCAT(lpad(a.EmptiesStoreID,6,'0'),'-',a.EmptiesWorkstationID,'-',
                case  when a.EmptiesReceiptNumber<100 then lpad(a.EmptiesReceiptNumber,3,'0')
		else a.EmptiesReceiptNumber end ,'-',case
                    when void_cnt>0
                            then case when round(SUM_EmptiesTotalDerived,2)<100 
                                        then lpad(cast(round(SUM_EmptiesTotalDerived,2) as decimal(5,2)),5,'0') 
                                    else round(SUM_EmptiesTotalDerived,2) end 
                      else case when round(SUM_EmptiesTotalDerived1,2)<100 
                                        then lpad(cast(round(SUM_EmptiesTotalDerived1,2) as decimal(5,2)),5,'0') 
                                    else round(SUM_EmptiesTotalDerived1,2) end 
        end) as Concatenated_Empties_ID
        , a.class_id
        , a.LineItemSequenceNumber
        , case
                    when c.VoidFlagDerived='true'
                            then 1
                            else 0
        end as IsVoided
from
        Redemption_RT a
        left join
                    (
                            select
                                        ER.retailstoreid
                                    , ER.WorkStationID
                                    , ER.ReceiptNumber
                                    , ER.ReceiptDateTime
                                    , ER.EmptiesStoreID
                                    , ER.EmptiesWorkStationID
                                    , ER.EmptiesReceiptNumber
                                    , count
                                                (
                                                            case
                                                                    when VoidFlagDerived='false'
                                                                                then 1
                                                            end
                                                )
                                        void_cnt
                                    , sum
                                                (
                                                            case
                                                                    when VoidFlagDerived='false'
                                                                                and FLAG  ='PF'
                                                                                then EmptiesTotal
                                                                                else 0
                                                            end
                                                )
                                        as SUM_EmptiesTotalDerived
                                    , sum
                                                (
                                                            case
                                                                    when VoidFlagDerived='true'
                                                                                and FLAG  ='PF'
                                                                                then EmptiesTotal
                                                                                else 0
                                                            end
                                                )
                                        as SUM_EmptiesTotalDerived1
                            from
                                        Redemption_RT ER
                                        left join
                                                (Select retailstoreid,WorkStationID,ReceiptNumber,ReceiptDateTime,
												LineItemSequenceNumber,VoidFlagDerived from {fraud}.lineitemderived) LD
                                                on
                                                            ER.retailstoreid             =LD.retailstoreid
                                                            and ER.WorkStationID         =LD.WorkStationID
                                                            and ER.ReceiptNumber         =LD.ReceiptNumber
                                                            and ER.ReceiptDateTime       =LD.ReceiptDateTime
                                                            and ER.LineItemSequenceNumber=LD.LineItemSequenceNumber
                            group by
                                        ER.retailstoreid
                                    , ER.WorkStationID
                                    , ER.ReceiptNumber
                                    , ER.ReceiptDateTime
                                    , ER.EmptiesStoreID
                                    , ER.EmptiesWorkStationID
                                    , ER.EmptiesReceiptNumber
                    )
                    b
                    on
                            a.retailstoreid           = b.retailstoreid
                            and a.WorkStationID       = b.WorkStationID
                            and a.ReceiptNumber       = b.ReceiptNumber
                            and a.ReceiptDateTime     = b.ReceiptDateTime
                            and a.EmptiesStoreID      =b.EmptiesStoreID
                            and a.EmptiesWorkStationID=b.EmptiesWorkStationID
                            and a.EmptiesReceiptNumber=b.EmptiesReceiptNumber
        left join
                    (Select retailstoreid,WorkStationID,ReceiptNumber,ReceiptDateTime,
												LineItemSequenceNumber,VoidFlagDerived from {fraud}.lineitemderived) c
                    on
                            a.retailstoreid             =c.retailstoreid
                            and a.WorkStationID         =c.WorkStationID
                            and a.ReceiptNumber         =c.ReceiptNumber
                            and a.ReceiptDateTime       =c.ReceiptDateTime
                            and a.LineItemSequenceNumber=c.LineItemSequenceNumber""".format(fraud=database))



    spark.sql("""create or replace temporary view Redemption_Flag4_RT as
select
        R.*
        , E.ReceiptDateTimeEmpties
        ,abs(unix_timestamp(date_format(E.ReceiptDateTimeEmpties,'yyyy-MM-dd HH:mm:ss'))-unix_timestamp(date_format(R.ReceiptDateTime,'yyyy-MM-dd HH:mm:ss')))/60 as Minute_Diff  
from
        Redemption_RT_Final R
        left join
                    (
                            select
                                        H.retailstoreid
                                        , H.WorkStationID
                                        , H.ReceiptNumber
                                        , max(H.ReceiptDateTime) as ReceiptDateTimeEmpties
                            from
                                        HNL_Empties_RT H
                                        inner join
                                                    Redemption_RT_Final RE
                                                    on
                                                                RE.EmptiesStoreID          =H.retailstoreid
                                                                and RE.EmptiesWorkStationID=H.WorkStationID
                                                                and RE.EmptiesReceiptNumber=H.ReceiptNumber
                                                                and H.ReceiptDateTime      < RE.ReceiptDateTime
                            group by
                                        H.retailstoreid
                                        , H.WorkstationID
                                        , H.ReceiptNumber
                    )
                    E
                    on
                            R.EmptiesStoreID          =E.retailstoreid
                            and R.EmptiesWorkStationID=E.WorkStationID
                            and R.EmptiesReceiptNumber=E.ReceiptNumber""")


    
    spark.sql("""create or replace temporary view Redemption_Flag_Final as
select
    retailstoreid
    , WorkStationID
    , ReceiptDateTime
    , ReceiptNumber
    , EmptiesStoreID
    , EmptiesWorkStationID
    , EmptiesReceiptNumber
    , EmptiesTotal
    , EmployeeID
    , FLAG
    , Concatenated_Empties_ID
    , class_id
    , LineItemSequenceNumber
    , IsVoided
    , case
            when Minute_Diff <= 20
                    then 0
            when (
                            Minute_Diff                 >20
                            and to_date(ReceiptDateTime)=to_date(ReceiptDateTimeEmpties)
                            and class_id                ='EN9'
                    )
                    then 1
            when (
                            Minute_Diff                 >20
                            and to_date(ReceiptDateTime)=to_date(ReceiptDateTimeEmpties)
                            and class_id               !='EN9'
                    )
                    then 2
            when (
                            to_date(ReceiptDateTime)            !=to_date(ReceiptDateTimeEmpties)
                            and ReceiptDateTimeEmpties is not null
                    )
                    or (
                            ReceiptDateTimeEmpties is null
                            and EmptiesWorkStationID    >= 100
                    )
                    then 3
            when (
                            ReceiptDateTimeEmpties is null
                            and EmptiesWorkStationID     < 100
                    )
                    then NULL
    end as EmptiesTimingFlag
from
    Redemption_Flag4_RT""")

    Empties_Redemption = spark.sql("""select * from Redemption_Flag_Final union select * from HNL_Empties_RT""")

    #---------------------------------------------------------------------------------------------------------#
    #Writes into parquet and alter hive table
    #---------------------------------------------------------------------------------------------------------#

    start = time.time()
    if config['input']['file_type']['file_type']=='parquet':
        Empties_Redemption.coalesce(10).write.partitionBy('retailstoreid').mode('overwrite').parquet('{path}/{epocid_r}'.format(epocid_r=epocid,path=path))

        drppart=','.join(['partition('+ part +')' for part in map(lambda x:str(x[0]),spark.sql("show partitions {fraud}.df_empties_redemption".format(fraud=database)).collect())])  

        if len(drppart) != 0:        
            spark.sql("alter table {fraud}.df_empties_redemption drop if exists ".format(fraud=database)+drppart)   
            
        spark.sql("""alter table {fraud}.df_empties_redemption set location '{path}/{epocid_r}'""".format(epocid_r=epocid,path=path,fraud=database))
        
        spark.sql("msck repair table {fraud}.df_empties_redemption".format(fraud=database))
    
    else:
        logger.info("writing delta")
        
        Empties_Redemption.coalesce(10).write.partitionBy('retailstoreid').mode('overwrite').option("overwriteSchema", "true").format("delta").save('{path}/{epocid_r}'.format(epocid_r=epocid,path=path))
        
        spark.sql("""create table if not exists {fraud}.df_empties_redemption location '{path}/{epocid_r}'""".format(epocid_r=epocid,path=path,fraud=database))
        spark.sql("""alter table {fraud}.df_empties_redemption set location '{path}/{epocid_r}'""".format(epocid_r=epocid,path=path,fraud=database))
        
        
    #------------------------------------------------------------------------------------------------------------#
    #Logger info updated
    #------------------------------------------------------------------------------------------------------------
    logger.info("Empties parquet written in path : %s",'{path}/{epocid_r}'.format(epocid_r=epocid,path=path))
    
    empties_count = spark.sql("""select emptiestimingflag,count(*) as count from {fraud}.df_empties_redemption
                              group by emptiestimingflag""".format(fraud=database))
    empties_count1=empties_count.toPandas()
    logger.debug(empties_count1.to_string(index = False)) 

    #------------------------------------------------------------------------------------------------------------#
    #Drop temp tables
    #------------------------------------------------------------------------------------------------------------

    spark.catalog.dropTempView('HNL_Empties_RT')
    spark.catalog.dropTempView('ReceiptPositionAddonList_RT')
    spark.catalog.dropTempView('Redemption_RT')
    spark.catalog.dropTempView('LineItem_Derived_return')
    spark.catalog.dropTempView('LineItem_Derived_sale')
    spark.catalog.dropTempView('TB_LineItem_Derived')
    spark.catalog.dropTempView('Redemption_RT_Final')
    spark.catalog.dropTempView('Redemption_Flag4_RT')
    spark.catalog.dropTempView('Redemption_Flag_Final')
    
