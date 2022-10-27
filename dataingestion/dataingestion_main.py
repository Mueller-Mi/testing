# Databricks notebook source
from __future__ import print_function
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, SQLContext
# import commands
# import pandas as pd
import subprocess
from datetime import datetime
import pyodbc
import warnings
import time
import os
import sys
import json
import lit

# importing dataingestion script
try:
    store = dbutils.widgets.get('store')
except:
    store = None
if store is None:
    import dataingestion.xml_parsing as xml
    import dataingestion.logtables as log
    import dataingestion.masterdata_sql as md_sql
    import dataingestion.load_control as load

global file_type


# -------------------------------------------------------------#
# Defining dataingestion_process
# -------------------------------------------------------------#
def dataingestion_process(spark, logger, dir_path, batch, epoch_id, count, config, loadcontrol, active_inactive_list,
                          active_inactive_count):
    """Main script definition
    
    return: return value from StoreInDailyLoadLog
    """
    global file_type
    file_type = config['input']['file_type']['file_type']
    # log that main job is starting
    logger.warning('DataIngestion_main.py job is up and running')

    # creating a dictonary for Log Tables
    dict_list = {
        'sourcefolderlist': [],
        'stores': [],
        'storecountlist': [],
        'warninglist': [],
        'errorlist': [],
        'logginglist': [],
        'startdatetime': [],
        'enddatetime': [],
        'statuslist': [],
        'xmllist': [],
        'exceptioncount': 0
    }

    # creating a dictonary for EventLog
    log_dict = {
        'eventdatetime': [],
        'retailstoreid': [],
        'level': [],
        'eventtype': [],
        'eventmessage': []
    }

    folder = dir_path
    dict_list['sourcefolderlist'].append(folder)

    # start time of this process
    start_date_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    logger.info('Start date time%s', start_date_time)
    dict_list['startdatetime'].append(start_date_time)

    # invoking process para
    processdata = process_data(spark, logger, dir_path, batch, epoch_id, config, loadcontrol, active_inactive_list,
                               active_inactive_count, dict_list, log_dict)

    # end time of the process
    end_date_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    logger.info('End date time%s', end_date_time)
    dict_list['enddatetime'].append(end_date_time)

    # invoking log tables
    if dict_list['exceptioncount'] == 0:
        logger.info('exception count 0')
        dailyloadlogdf = dailyloadlog(spark, logger, dict_list, config)
        logger.info('dailyloadlog finished')
        storeindailyloadlog = storein_daily_loadlog(spark, logger, dict_list, config, processdata)
        logger.info('store_in_daily_loadlog finished')
        load.update_loadcontrol(spark, logger, config, loadcontrol, storeindailyloadlog)
        logger.info('calling logtables sp')
        log.call_logtables_stored_procedure(spark, logger, config)
        if config.get('runtime','on-premise') == 'on-premise':
          partfile_df = pd.DataFrame(dir_path, columns=['part_filename'])
          partfile_df = spark.createDataFrame(partfile_df)
          partfile_df = partfile_df.withColumn('status', F.lit(1))
          partfile_df.write.mode('append').parquet(config['input']['files']['filename'])
        logger.info('returning storeindailyloadlog')
        return storeindailyloadlog
    else:
        load.update_loadcontrol(spark, logger, config, loadcontrol, processdata)
        return processdata


# -------------------------------------------------------------#
# processing sap masterdata
# -------------------------------------------------------------#

def process_masterdata(spark, logger, config):
    """processes the master data for merchandisehierarchylevel and articledescription
    
    return: None
    """
    global file_type
    file_type = config['input']['file_type']['file_type']
    try:

        md_edeka_mhl = config['input']['files']['md_edeka_mhl']
        md_ebus_mhl = config['input']['files']['md_ebus_mhl']
        md_articles = config['input']['files']['md_articles']
        merchandiselevel = config['output']['processed_df_folder']['merchandiselevel']
        articledescription = config['output']['processed_df_folder']['articledescription']

        df_mhl_edeka = spark.read.format("csv").load(md_edeka_mhl, header=True, sep=';', mode='FAILFAST')
        df_mhl_ebus = spark.read.format("csv").load(md_ebus_mhl, header=True, sep=';', mode='FAILFAST')
        df_article_description = spark.read.format("csv").load(md_articles, header=True, sep=';', mode='FAILFAST')

        MDMerchandiseLevel = xml.merchandise_hierarchy_level(spark, logger, df_mhl_edeka, df_mhl_ebus)
        MDArticleDescription = xml.article_description(spark, logger, df_article_description)

        copy_masterdata(MDMerchandiseLevel,
                        '{path}'.format(path=config['output']['processed_df_folder']['merchandiselevel']))
        copy_masterdata(MDArticleDescription,
                        '{path}'.format(path=config['output']['processed_df_folder']['articledescription']))

        spark.sql("""create table if not exists {fraud}.MD_MerchandiseHierarchyLevels location '{path}'""".format(
            path=config['output']['processed_df_folder']['merchandiselevel'],
            fraud=config['input']['hive']['database']))
        spark.sql("""create table if not exists {fraud}.MD_ArticleDescription location '{path}'""".format(
            path=config['output']['processed_df_folder']['articledescription'],
            fraud=config['input']['hive']['database']))
        spark.sql(""" alter table {fraud}.MD_MerchandiseHierarchyLevels set location '{path}'""".format(
            path=config['output']['processed_df_folder']['merchandiselevel'],
            fraud=config['input']['hive']['database']))
        spark.sql("""alter table {fraud}.MD_ArticleDescription set location '{path}'""".format(
            path=config['output']['processed_df_folder']['articledescription'],
            fraud=config['input']['hive']['database']))


        md_sql.masterdata_to_sql(spark, logger, config)
        copy_stng_to_final(spark, logger, config)

    except Exception as e:
        logger.error('Failed to process master data')
        logger.error(e)

        
def process_master_data(spark, logger, config):
    """processes the master data for merchandisehierarchylevel and articledescription
    
    return: None
    """
    global file_type
    file_type = config['input']['file_type']['file_type']
    try:

        md_edeka_mhl = config['input']['files']['md_edeka_mhl']
        md_ebus_mhl = config['input']['files']['md_ebus_mhl']
        merchandiselevel = config['output']['processed_df_folder']['merchandiselevel']
        #articledescription = config['output']['processed_df_folder']['articledescription']

        df_mhl_edeka = spark.read.format("csv").load(md_edeka_mhl, header=True, sep=';', mode='FAILFAST')
        df_mhl_ebus = spark.read.format("csv").load(md_ebus_mhl, header=True, sep=';', mode='FAILFAST')
        #df_article_description = spark.sql("""select * from qualified.dfq_t_ezbi_gtin""")


        MDMerchandiseLevel = xml.merchandise_hierarchy_level(spark, logger, df_mhl_edeka, df_mhl_ebus)
        #MDArticleDescription = xml.mdarticle_description(spark, logger, df_article_description)

        copy_masterdata(MDMerchandiseLevel,
                        '{path}'.format(path=config['output']['processed_df_folder']['merchandiselevel']))
        #copy_masterdata(MDArticleDescription,
        #                '{path}'.format(path=config['output']['processed_df_folder']['articledescription']))

        spark.sql("""create table if not exists {fraud}.MD_MerchandiseHierarchyLevels location '{path}'""".format(
            path=config['output']['processed_df_folder']['merchandiselevel'],
            fraud=config['input']['hive']['database']))
        spark.sql("""create table if not exists {fraud}.MD_ArticleDescription location '{path}'""".format(
            path=config['output']['processed_df_folder']['articledescription'],
            fraud=config['input']['hive']['database']))
        spark.sql("""alter table {fraud}.MD_MerchandiseHierarchyLevels set location '{path}'""".format(
            path=config['output']['processed_df_folder']['merchandiselevel'],
            fraud=config['input']['hive']['database']))
        spark.sql("""alter table {fraud}.MD_ArticleDescription set location '{path}'""".format(
            path=config['output']['processed_df_folder']['articledescription'],
            fraud=config['input']['hive']['database']))

        md_sql.masterdata_to_sql(spark, logger, config)
        copy_stng_to_final(spark, logger, config)

    except Exception as e:
        logger.error('Failed to process master data in cloud')
        logger.error(e)        
        

# -------------------------------------------------------------#
# process_data function invokes all the functions
# -------------------------------------------------------------#

def process_data(spark, logger, stores_dir, batch, epoch_id, config, loadcontrol, active_inactive_list,
                 active_inactive_count, dict_list, log_dict):
    logger.info('Invoking functions in process para')
    global file_type
    file_type = config['input']['file_type']['file_type']
    try:

        config_dict = create_config_dict(config, log_dict)
        try:
            dict_df = create_data_frames(logger, spark, batch, epoch_id, config, config_dict, loadcontrol, dict_list,
                                         log_dict)
            update_table = update_hive_table(spark, logger, epoch_id, config)
            storeid = get_retailstoreid(spark, active_inactive_list, active_inactive_count, logger, dict_list, config,
                                        config_dict, log_dict)
            xmlfiles_df(spark, logger, dict_list, dict_df, config)
            return storeid
        except Exception as e:
            logger.error('In create_data_frame exception')
            dict_list['exceptioncount'] = dict_list['exceptioncount'] + 1
            logger.error(e)
        
    except Exception as e:
        logger.error('Exception occurred in dataingetion_main')
        eventdatetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        logger.error(e)
        logging_error = batch.select(F.col('RetailStoreID'))
        # log_dict['retailstoreid'].append(0)
        log_dict['eventdatetime'].append(eventdatetime)
        eventtype = 'Failed to process dataingestion'
        log_dict['eventtype'].append(eventtype)
        log_dict['level'].append('E')
        eventmessage = 'DataIngestion failed for epoch_id {}'.format(epoch_id)
        log_dict['eventmessage'].append(eventmessage)
        logging_error = logging_error.withColumn('EventDateTime', F.lit(eventdatetime))
        logging_error = logging_error.withColumn('LoadID', F.lit(0))
        logging_error = logging_error.withColumn('Level', F.lit('E'))
        logging_error = logging_error.withColumn('EventType', F.lit(eventtype))
        logging_error = logging_error.withColumn('EventMessage', F.lit(eventmessage))
        logger.error(logging_error.collect())
        logging_list(spark, logger, config, log_dict, logging_error)
        store_status = update_exception_status(spark, loadcontrol, logger, e, batch, config, dict_list)
        return store_status


# -------------------------------------------------------------#
# calling create_config_dict
# -------------------------------------------------------------#

def create_config_dict(config, log_dict):
    config_dict = {
        'controltransaction': config['output']['processed_df_folder']['controltransaction'],
        'customer': config['output']['processed_df_folder']['customer'],
        'functionlog': config['output']['processed_df_folder']['functionlog'],
        'lineitem': config['output']['processed_df_folder']['lineitem'],
        'loyaltyreward': config['output']['processed_df_folder']['loyaltyreward'],
        'returndf': config['output']['processed_df_folder']['return'],
        'sale': config['output']['processed_df_folder']['sale'],
        'receiptheaderaddonlist': config['output']['processed_df_folder']['receiptheaderaddonlist'],
        'receiptpositionaddonlist': config['output']['processed_df_folder']['receiptpositionaddonlist'],
        'retailpricemodifier': config['output']['processed_df_folder']['retailpricemodifier'],
        'retailtransactionheader': config['output']['processed_df_folder']['retailtransactionheader'],
        'tender': config['output']['processed_df_folder']['tender'],
        'tendercontrolloan': config['output']['processed_df_folder']['tendercontrolloan'],
        'tendercontrolpaymentdeposit': config['output']['processed_df_folder']['tendercontrolpaymentdeposit'],
        'tendercontrolsafesettle': config['output']['processed_df_folder']['tendercontrolsafesettle'],
        'tendercontroltillsettle': config['output']['processed_df_folder']['tendercontroltillsettle'],
        'tendercontrolpickup': config['output']['processed_df_folder']['tendercontrolpickup'],
        'transactionheader': config['output']['processed_df_folder']['transactionheader'],
        'salereturn_rebate': config['output']['processed_df_folder']['salereturn_rebate'],
        'employeeid_encryption': config['input']['encryption']['status'],
        'encryption_key': config['input']['encryption']['encryption_key'],
        'database': config['input']['hive']['database'],
        'type': config['input']['type']['type']
    }

    return config_dict


# -------------------------------------------------------------#
# calling get_retailstoreid
# -------------------------------------------------------------#

def get_retailstoreid(spark, active_inactive_list, active_inactive_count, logger, dict_list, config, config_dict,
                      log_dict):
    logger.info("In get_retailstoreid function in dataingestion_main to get the retailstoreid")
    if (active_inactive_count == 0):

        for active_inactive_df in active_inactive_list:

            storeid_date_df = active_inactive_df.select(F.col('RetailStoreID'), F.col('Date'))
            storeid_date_df = storeid_date_df.withColumnRenamed('Date', 'BusinessDate')
            storeid_date_df = storeid_date_df.dropDuplicates(['RetailStoreID', 'BusinessDate'])

            active_stores = active_inactive_df.select(F.col('RetailStoreID'), F.col('Status'))
            active_stores = active_stores.dropDuplicates(['RetailStoreID', 'Status'])

            active_stores = active_stores.registerTempTable('active_stores')
            active_stores = spark.sql(
                """select B.storeid, B.status from (select A.storeid, A.status from {fraud}.stores A where not exists (select distinct RetailStoreID, Status from active_stores where A.storeid = RetailStoreID)) B""".format(
                    fraud=config_dict['database']))
            active_stores = active_stores.withColumnRenamed('storeid', 'RetailStoreID')
            active_stores = active_stores.withColumnRenamed('status', 'Status')
            active_stores = active_stores.filter(F.col('Status') == 'Aktiv')
            active_list = list(set(active_stores.select(F.col('RetailStoreID')).toPandas()['RetailStoreID']))
            logger.info('Active stores which did not receive data%s', active_list)

            if len(active_list) == 0:
                logger.info('Received the data for all the active stores')
            else:
                active_stores = active_stores.withColumn('EventDateTime',
                                                         F.lit(datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
                active_stores = active_stores.withColumn('Level', F.lit('E'))
                eventtype = 'Active store but no data'
                eventmessage = 'Store is active but did not receive the data'
                active_stores = active_stores.withColumn('EventType', F.lit(eventtype))
                active_stores = active_stores.withColumn('EventMessage', F.lit(eventmessage))
                active_stores = active_stores.withColumn('LoadID', F.lit(0))
                active_stores = active_stores.select('EventDateTime', 'RetailStoreID', 'Level', 'EventType',
                                                     'EventMessage', 'LoadID')
                active_stores = active_stores.dropDuplicates(['RetailStoreID'])
                logger.info('calling log.eventlog_di')
                log.eventlog_di(spark, active_stores, config)

            inactive_stores_temp = active_inactive_df.select(F.col('RetailStoreID'), F.col('Status'), F.col('Date'))
            inactive_stores_temp = inactive_stores_temp.filter(inactive_stores_temp.Status == 'T')
            inactive_stores_temp = inactive_stores_temp.dropDuplicates(['RetailStoreID', 'Status'])
            inactive_business_date = inactive_stores_temp.select(F.col('Date'))
            inactive_business_date = list(set(inactive_business_date.select(F.col('Date')).toPandas()['Date']))
            inactive_list = list(set(inactive_stores_temp.select(F.col('RetailStoreID')).toPandas()['RetailStoreID']))
            logger.info('Store is temporarily inactive but still received the data%s', inactive_list)

            if len(inactive_list) == 0:
                logger.info('Did not receive the data for inactive stores')
            else:
                inactive_stores_temp = inactive_stores_temp.withColumn('EventDateTime', F.lit(
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
                inactive_stores_temp = inactive_stores_temp.withColumn('Level', F.lit('W'))
                eventtype = 'Inactive store but received the data'
                inactive_stores_temp = inactive_stores_temp.withColumn('eventtype', F.lit(eventtype))
                inactive_stores_temp = inactive_stores_temp.withColumn('EventMessage',
                                                                       F.concat(F.lit('Store '), F.col('RetailStoreID'),
                                                                                F.lit(
                                                                                    ' is temporarily inactive but still received the data for '),
                                                                                F.col('Date')))
                inactive_stores_temp = inactive_stores_temp.withColumn('LoadID', F.lit(0))
                inactive_stores_temp = inactive_stores_temp.select('EventDateTime', 'RetailStoreID', 'Level',
                                                                   'EventType', 'EventMessage', 'LoadID')
                inactive_stores_temp = inactive_stores_temp.dropDuplicates(['RetailStoreID'])
                logger.info('calling log.eventlog_di')
                log.eventlog_di(spark, inactive_stores_temp, config)

            return storeid_date_df

        active_inactive_count = active_inactive_count + 1


# -------------------------------------------------------------#
# calling create_data_frames
# -------------------------------------------------------------#

def create_data_frames(logger, spark, df, epoch_id, config, config_dict, loadcontrol, dict_list, log_dict):
    # calling create dataframe functions which creates a dataframe and writes it into the hdfs path
    logger.info("Creating DataIngestion DataFrames")

    dict_df = {'ControlTransaction': xml.control_transaction_df(logger, loadcontrol, df),
               'FunctionLog': xml.functionlog_df(logger, loadcontrol, df),
               'Customer': xml.customer_df(logger, loadcontrol, df, config),
               'LineItem': xml.lineitem_df(logger, loadcontrol, df),
               'TransactionHeader': xml.transactionheader_df(logger, loadcontrol, df, config_dict),
               'Loyaltyreward': xml.loyaltyreward_df(logger, loadcontrol, df),
               'RetailPriceModifier': xml.retail_pricemodifier_df(logger, loadcontrol, df),
               'ReceiptHeaderAddonList': xml.receiptheader_addonlist_df(logger, loadcontrol, df),
               'ReceiptPositionAddonList': xml.receiptposition_addonlist_df(logger, loadcontrol, df),
               'RetailTransactionHeader': xml.retail_transactionheader_df(logger, loadcontrol, df),
               'Return': xml.return_df(logger, loadcontrol, df),
               'Sale': xml.sale_df(logger, loadcontrol, df),
               'Tender': xml.tender_df(logger, loadcontrol, df),
               'TenderControlLoan': xml.tender_control_loan_df(logger, loadcontrol, df),
               'TenderControlPickup': xml.tender_control_pickup_df(logger, loadcontrol, df),
               'TenderControlPaymentDeposit': xml.tender_payment_deposit_df(logger, loadcontrol, df, config_dict),
               'TenderControlSafeSettle': xml.tender_control_safe_settle_df(logger, loadcontrol, df),
               'TenderControlTillSettle': xml.tender_control_till_settle_df(logger, loadcontrol, df),
               'SaleReturnRebate': xml.salereturn_rebate_df(logger, loadcontrol, df)}

    dict_list['statuslist'].append('L')
    retailstoreid = list(set(dict_df['TransactionHeader'].select(F.col('RetailStoreID')).toPandas()['RetailStoreID']))
    dict_list['stores'].append(len(retailstoreid))
    dict_list['storecountlist'].append(retailstoreid)
    logger.info('wrting df to adl')
    copy_to_parquet(dict_df['ControlTransaction'],
                    '{path}/{epoch_id}'.format(path=config_dict['controltransaction'], epoch_id=epoch_id))
    copy_to_parquet(dict_df['FunctionLog'],
                    '{path}/{epoch_id}'.format(path=config_dict['functionlog'], epoch_id=epoch_id))
    copy_to_parquet(dict_df['Customer'], '{path}/{epoch_id}'.format(path=config_dict['customer'], epoch_id=epoch_id))
    copy_to_parquet(dict_df['LineItem'], '{path}/{epoch_id}'.format(path=config_dict['lineitem'], epoch_id=epoch_id))
    copy_to_parquet(dict_df['Loyaltyreward'],
                    '{path}/{epoch_id}'.format(path=config_dict['loyaltyreward'], epoch_id=epoch_id))
    copy_to_parquet(dict_df['RetailPriceModifier'],
                    '{path}/{epoch_id}'.format(path=config_dict['retailpricemodifier'], epoch_id=epoch_id))
    copy_to_parquet(dict_df['ReceiptHeaderAddonList'],
                    '{path}/{epoch_id}'.format(path=config_dict['receiptheaderaddonlist'], epoch_id=epoch_id))
    copy_to_parquet(dict_df['ReceiptPositionAddonList'],
                    '{path}/{epoch_id}'.format(path=config_dict['receiptpositionaddonlist'], epoch_id=epoch_id))
    copy_to_parquet(dict_df['RetailTransactionHeader'],
                    '{path}/{epoch_id}'.format(path=config_dict['retailtransactionheader'], epoch_id=epoch_id))
    copy_to_parquet(dict_df['Return'], '{path}/{epoch_id}'.format(path=config_dict['returndf'], epoch_id=epoch_id))
    copy_to_parquet(dict_df['Sale'], '{path}/{epoch_id}'.format(path=config_dict['sale'], epoch_id=epoch_id))
    copy_to_parquet(dict_df['Tender'], '{path}/{epoch_id}'.format(path=config_dict['tender'], epoch_id=epoch_id))
    copy_to_parquet(dict_df['TenderControlLoan'],
                    '{path}/{epoch_id}'.format(path=config_dict['tendercontrolloan'], epoch_id=epoch_id))
    copy_to_parquet(dict_df['TenderControlPickup'],
                    '{path}/{epoch_id}'.format(path=config_dict['tendercontrolpickup'], epoch_id=epoch_id))
    copy_to_parquet(dict_df['TenderControlPaymentDeposit'],
                    '{path}/{epoch_id}'.format(path=config_dict['tendercontrolpaymentdeposit'], epoch_id=epoch_id))
    copy_to_parquet(dict_df['TenderControlSafeSettle'],
                    '{path}/{epoch_id}'.format(path=config_dict['tendercontrolsafesettle'], epoch_id=epoch_id))
    copy_to_parquet(dict_df['TenderControlTillSettle'],
                    '{path}/{epoch_id}'.format(path=config_dict['tendercontroltillsettle'], epoch_id=epoch_id))
    copy_to_parquet(dict_df['TransactionHeader'],
                    '{path}/{epoch_id}'.format(path=config_dict['transactionheader'], epoch_id=epoch_id))
    copy_to_parquet(dict_df['SaleReturnRebate'],
                    '{path}/{epoch_id}'.format(path=config_dict['salereturn_rebate'], epoch_id=epoch_id))
    logger.info('finished wrting df to adl')
    return dict_df


# -------------------------------------------------------------#
# calling update_hive_table
# -------------------------------------------------------------#
def update_hive_table(spark, logger, epoch_id, config_dict):
    logger.info(f'Updating hive tables for epoch_id {epoch_id}')
    db = config_dict['input']['hive']['database']
    _type = config_dict['input']['type']['type']
    employeeid_encryption = config_dict['input']['encryption']['status'],
    encryption_key = config_dict['input']['encryption']['encryption_key'],
    data = config_dict['output']['processed_df_folder'].copy()
    del(data['articledescription'])
    del(data['merchandiselevel'])
    del(data['stores'])
    for table in list(data.keys()):
        path = data[table]
#         logger.info(f'altering table {table}')
#         query = f'ALTER TABLE {db}.{table} SET LOCATION "{path}/{epoch_id}"'
        query = f'CREATE TABLE IF NOT EXISTS {db}.{table} LOCATION "{path}/{epoch_id}"'
        spark.sql(query)
        query = f'ALTER TABLE {db}.{table} SET LOCATION "{path}/{epoch_id}"'
#         logger.info(query)
        spark.sql(query)

def update_hive_table_old(spark, logger, epoch_id, config_dict, log_dict):
    logger.info('Updating hive tables for epoch_id {}'.format(epoch_id))

    # databricks check later
    drop_controltransaction = ','.join(['partition(' + part + ')' for part in map(lambda x: str(x[0]), spark.sql(
        "show partitions {fraud}.controltransaction".format(fraud=config_dict['database'])).collect())])
    if len(drop_controltransaction) != 0:
        spark.sql("alter table {fraud}.controltransaction drop if exists ".format(
            fraud=config_dict['database']) + drop_controltransaction)
    spark.sql("""alter table {fraud}.controltransaction set location '{path}/{epoch_id}'""".format(
        path=config_dict['controltransaction'], epoch_id=epoch_id, fraud=config_dict['database']))
    spark.sql("msck repair table {fraud}.controltransaction".format(fraud=config_dict['database']))

    # print("customer")
    drop_customer = ','.join(['partition(' + part + ')' for part in map(lambda x: str(x[0]), spark.sql(
        "show partitions {fraud}.customer".format(fraud=config_dict['database'])).collect())])
    if len(drop_customer) != 0:
        spark.sql("alter table {fraud}.customer drop if exists ".format(fraud=config_dict['database']) + drop_customer)
    spark.sql("""alter table {fraud}.customer set location '{path}/{epoch_id}'""".format(path=config_dict['customer'],
                                                                                         epoch_id=epoch_id,
                                                                                         fraud=config_dict['database']))
    spark.sql("msck repair table {fraud}.customer".format(fraud=config_dict['database']))

    # print("functionlog")
    drop_function = ','.join(['partition(' + part + ')' for part in map(lambda x: str(x[0]), spark.sql(
        "show partitions {fraud}.functionlog".format(fraud=config_dict['database'])).collect())])
    if len(drop_function) != 0:
        spark.sql(
            "alter table {fraud}.functionlog drop if exists ".format(fraud=config_dict['database']) + drop_function)
    spark.sql(
        """alter table {fraud}.functionlog set location '{path}/{epoch_id}'""".format(path=config_dict['functionlog'],
                                                                                      epoch_id=epoch_id,
                                                                                      fraud=config_dict['database']))
    spark.sql("msck repair table {fraud}.functionlog".format(fraud=config_dict['database']))

    # print("lineitem")
    drop_lineitem = ','.join(['partition(' + part + ')' for part in map(lambda x: str(x[0]), spark.sql(
        "show partitions {fraud}.lineitem".format(fraud=config_dict['database'])).collect())])
    if len(drop_lineitem) != 0:
        spark.sql("alter table {fraud}.lineitem drop if exists ".format(fraud=config_dict['database']) + drop_lineitem)
    spark.sql("""alter table {fraud}.lineitem set location '{path}/{epoch_id}'""".format(path=config_dict['lineitem'],
                                                                                         epoch_id=epoch_id,
                                                                                         fraud=config_dict['database']))
    spark.sql("msck repair table {fraud}.lineitem".format(fraud=config_dict['database']))

    # print("loyaltyreward")
    drop_loyaltyreward = ','.join(['partition(' + part + ')' for part in map(lambda x: str(x[0]), spark.sql(
        "show partitions {fraud}.loyaltyreward".format(fraud=config_dict['database'])).collect())])
    if len(drop_loyaltyreward) != 0:
        spark.sql("alter table {fraud}.loyaltyreward drop if exists ".format(
            fraud=config_dict['database']) + drop_loyaltyreward)
    spark.sql("""alter table {fraud}.loyaltyreward set location '{path}/{epoch_id}'""".format(
        path=config_dict['loyaltyreward'], epoch_id=epoch_id, fraud=config_dict['database']))
    spark.sql("msck repair table {fraud}.loyaltyreward".format(fraud=config_dict['database']))

    # print("return")
    drop_return = ','.join(['partition(' + part + ')' for part in map(lambda x: str(x[0]), spark.sql(
        "show partitions {fraud}.return".format(fraud=config_dict['database'])).collect())])
    if len(drop_return) != 0:
        spark.sql("alter table {fraud}.return drop if exists ".format(fraud=config_dict['database']) + drop_return)
    spark.sql("""alter table {fraud}.return set location '{path}/{epoch_id}'""".format(path=config_dict['returndf'],
                                                                                       epoch_id=epoch_id,
                                                                                       fraud=config_dict['database']))
    spark.sql("msck repair table {fraud}.return".format(fraud=config_dict['database']))

    # print("sale")
    drop_sale = ','.join(['partition(' + part + ')' for part in map(lambda x: str(x[0]), spark.sql(
        "show partitions {fraud}.sale".format(fraud=config_dict['database'])).collect())])
    if len(drop_sale) != 0:
        spark.sql("alter table {fraud}.sale drop if exists ".format(fraud=config_dict['database']) + drop_sale)
    spark.sql("""alter table {fraud}.sale set location '{path}/{epoch_id}'""".format(path=config_dict['sale'],
                                                                                     epoch_id=epoch_id,
                                                                                     fraud=config_dict['database']))
    spark.sql("msck repair table {fraud}.sale".format(fraud=config_dict['database']))

    # print("receiptheaderaddonlist")
    drop_receiptheader = ','.join(['partition(' + part + ')' for part in map(lambda x: str(x[0]), spark.sql(
        "show partitions {fraud}.receiptheaderaddonlist".format(fraud=config_dict['database'])).collect())])
    if len(drop_receiptheader) != 0:
        spark.sql("alter table {fraud}.receiptheaderaddonlist drop if exists ".format(
            fraud=config_dict['database']) + drop_receiptheader)
    spark.sql("""alter table {fraud}.receiptheaderaddonlist set location '{path}/{epoch_id}'""".format(
        path=config_dict['receiptheaderaddonlist'], epoch_id=epoch_id, fraud=config_dict['database']))
    spark.sql("msck repair table {fraud}.receiptheaderaddonlist".format(fraud=config_dict['database']))

    # print("receiptpositionaddonlist")
    drop_receiptposition = ','.join(['partition(' + part + ')' for part in map(lambda x: str(x[0]), spark.sql(
        "show partitions {fraud}.receiptpositionaddonlist".format(fraud=config_dict['database'])).collect())])
    if len(drop_receiptposition) != 0:
        spark.sql("alter table {fraud}.receiptpositionaddonlist drop if exists ".format(
            fraud=config_dict['database']) + drop_receiptposition)
    spark.sql("""alter table {fraud}.receiptpositionaddonlist set location '{path}/{epoch_id}'""".format(
        path=config_dict['receiptpositionaddonlist'], epoch_id=epoch_id, fraud=config_dict['database']))
    spark.sql("msck repair table {fraud}.receiptpositionaddonlist".format(fraud=config_dict['database']))

    # print("retailpricemodifier")
    drop_receiptprice = ','.join(['partition(' + part + ')' for part in map(lambda x: str(x[0]), spark.sql(
        "show partitions {fraud}.retailpricemodifier".format(fraud=config_dict['database'])).collect())])
    if len(drop_receiptprice) != 0:
        spark.sql("alter table {fraud}.retailpricemodifier drop if exists ".format(
            fraud=config_dict['database']) + drop_receiptprice)
    spark.sql("""alter table {fraud}.retailpricemodifier set location '{path}/{epoch_id}'""".format(
        path=config_dict['retailpricemodifier'], epoch_id=epoch_id, fraud=config_dict['database']))
    spark.sql("msck repair table {fraud}.retailpricemodifier".format(fraud=config_dict['database']))

    # print("retailtransactionheader")
    drop_retailtransaction = ','.join(['partition(' + part + ')' for part in map(lambda x: str(x[0]), spark.sql(
        "show partitions {fraud}.retailtransactionheader".format(fraud=config_dict['database'])).collect())])
    if len(drop_retailtransaction) != 0:
        spark.sql("alter table {fraud}.retailtransactionheader drop if exists ".format(
            fraud=config_dict['database']) + drop_retailtransaction)
    spark.sql("""alter table {fraud}.retailtransactionheader set location '{path}/{epoch_id}'""".format(
        path=config_dict['retailtransactionheader'], epoch_id=epoch_id, fraud=config_dict['database']))
    spark.sql("msck repair table {fraud}.retailtransactionheader".format(fraud=config_dict['database']))

    # print("tender")
    drop_tender = ','.join(['partition(' + part + ')' for part in map(lambda x: str(x[0]), spark.sql(
        "show partitions {fraud}.tender".format(fraud=config_dict['database'])).collect())])
    if len(drop_tender) != 0:
        spark.sql("alter table {fraud}.tender drop if exists ".format(fraud=config_dict['database']) + drop_tender)
    spark.sql("""alter table {fraud}.tender set location '{path}/{epoch_id}'""".format(path=config_dict['tender'],
                                                                                       epoch_id=epoch_id,
                                                                                       fraud=config_dict['database']))
    spark.sql("msck repair table {fraud}.tender".format(fraud=config_dict['database']))

    # print("tendercontrolloan")
    drop_tendercontrolloan = ','.join(['partition(' + part + ')' for part in map(lambda x: str(x[0]), spark.sql(
        "show partitions {fraud}.tendercontrolloan".format(fraud=config_dict['database'])).collect())])
    if len(drop_tendercontrolloan) != 0:
        spark.sql("alter table {fraud}.tendercontrolloan drop if exists ".format(
            fraud=config_dict['database']) + drop_tendercontrolloan)
    spark.sql("""alter table {fraud}.tendercontrolloan set location '{path}/{epoch_id}'""".format(
        path=config_dict['tendercontrolloan'], epoch_id=epoch_id, fraud=config_dict['database']))
    spark.sql("msck repair table {fraud}.tendercontrolloan".format(fraud=config_dict['database']))

    # print("tendercontrolpaymentdeposit")
    drop_tenderpayment = ','.join(['partition(' + part + ')' for part in map(lambda x: str(x[0]), spark.sql(
        "show partitions {fraud}.tendercontrolpaymentdeposit".format(fraud=config_dict['database'])).collect())])
    if len(drop_tenderpayment) != 0:
        spark.sql("alter table {fraud}.tendercontrolpaymentdeposit drop if exists ".format(
            fraud=config_dict['database']) + drop_tenderpayment)
    spark.sql("""alter table {fraud}.tendercontrolpaymentdeposit set location '{path}/{epoch_id}'""".format(
        path=config_dict['tendercontrolpaymentdeposit'], epoch_id=epoch_id, fraud=config_dict['database']))
    spark.sql("msck repair table {fraud}.tendercontrolpaymentdeposit".format(fraud=config_dict['database']))

    # print("tendercontrolsafesettle")
    drop_tendersafe = ','.join(['partition(' + part + ')' for part in map(lambda x: str(x[0]), spark.sql(
        "show partitions {fraud}.tendercontrolsafesettle".format(fraud=config_dict['database'])).collect())])
    if len(drop_tendersafe) != 0:
        spark.sql("alter table {fraud}.tendercontrolsafesettle drop if exists ".format(
            fraud=config_dict['database']) + drop_tendersafe)
    spark.sql("""alter table {fraud}.tendercontrolsafesettle set location '{path}/{epoch_id}'""".format(
        path=config_dict['tendercontrolsafesettle'], epoch_id=epoch_id, fraud=config_dict['database']))
    spark.sql("msck repair table {fraud}.tendercontrolsafesettle".format(fraud=config_dict['database']))

    # print("tendercontroltillsettle")
    drop_tendertill = ','.join(['partition(' + part + ')' for part in map(lambda x: str(x[0]), spark.sql(
        "show partitions {fraud}.tendercontroltillsettle".format(fraud=config_dict['database'])).collect())])
    if len(drop_tendertill) != 0:
        spark.sql("alter table {fraud}.tendercontroltillsettle drop if exists ".format(
            fraud=config_dict['database']) + drop_tendertill)
    spark.sql("""alter table {fraud}.tendercontroltillsettle set location '{path}/{epoch_id}'""".format(
        path=config_dict['tendercontroltillsettle'], epoch_id=epoch_id, fraud=config_dict['database']))
    spark.sql("msck repair table {fraud}.tendercontroltillsettle".format(fraud=config_dict['database']))

    # print("tendercontrolpickup")
    drop_controlpickup = ','.join(['partition(' + part + ')' for part in map(lambda x: str(x[0]), spark.sql(
        "show partitions {fraud}.tendercontrolpickup".format(fraud=config_dict['database'])).collect())])
    if len(drop_controlpickup) != 0:
        spark.sql("alter table {fraud}.tendercontrolpickup drop if exists ".format(
            fraud=config_dict['database']) + drop_controlpickup)
    spark.sql("""alter table {fraud}.tendercontrolpickup set location '{path}/{epoch_id}'""".format(
        path=config_dict['tendercontrolpickup'], epoch_id=epoch_id, fraud=config_dict['database']))
    spark.sql("msck repair table {fraud}.tendercontrolpickup".format(fraud=config_dict['database']))

    # print("transactionheader")
    drop_transactionheader = ','.join(['partition(' + part + ')' for part in map(lambda x: str(x[0]), spark.sql(
        "show partitions {fraud}.transactionheader".format(fraud=config_dict['database'])).collect())])
    if len(drop_transactionheader) != 0:
        spark.sql("alter table {fraud}.transactionheader drop if exists ".format(
            fraud=config_dict['database']) + drop_transactionheader)
    spark.sql("""alter table {fraud}.transactionheader set location '{path}/{epoch_id}'""".format(
        path=config_dict['transactionheader'], epoch_id=epoch_id, fraud=config_dict['database']))
    spark.sql("msck repair table {fraud}.transactionheader".format(fraud=config_dict['database']))

    drop_salereturnrebate = ','.join(['partition(' + part + ')' for part in map(lambda x: str(x[0]), spark.sql(
        "show partitions {fraud}.salereturn_rebate".format(fraud=config_dict['database'])).collect())])
    if len(drop_salereturnrebate) != 0:
        spark.sql("alter table {fraud}.salereturn_rebate drop if exists ".format(
            fraud=config_dict['database']) + drop_salereturnrebate)
    spark.sql("""alter table {fraud}.salereturn_rebate set location '{path}/{epoch_id}'""".format(
        path=config_dict['salereturn_rebate'], epoch_id=epoch_id, fraud=config_dict['database']))
    spark.sql("msck repair table {fraud}.salereturn_rebate".format(fraud=config_dict['database']))


# -------------------------------------------------------------#
# calling update_exception_status
# -------------------------------------------------------------#

def update_exception_status(spark, loadcontrol, logger, e, df, config, dict_list):
    logger.info('Updating exception status to Log and LoadControl tables')

    if dict_list['exceptioncount'] > 0:
        eventdatetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        dict_list['errorlist'].append(e)
        dict_list['statuslist'].append('E')
        retailstoreid = list(set(df.select(F.col('RetailStoreID')).toPandas()['RetailStoreID']))
        storeid_date_df = df.select(F.col('RetailStoreID'), F.col('GK:ReceiptDateTime').alias('BusinessDate'))
        storeid_date_df = storeid_date_df.withColumn('BusinessDate', F.to_date('BusinessDate'))
        storeid_date_df = storeid_date_df.dropDuplicates(['RetailStoreID', 'BusinessDate'])
        dict_list['stores'].append(len(retailstoreid))
        dict_list['storecountlist'].append(retailstoreid)
        dict_list['enddatetime'].append(eventdatetime)
        dailyloadlog(spark, logger, dict_list, config)
        storeindaily = storein_daily_loadlog(spark, logger, dict_list, config, storeid_date_df)
        load.update_loadcontrol(spark, logger, config, loadcontrol, storeindaily)
        log.call_logtables_stored_procedure(spark, logger, config)
        return storeindaily


# -------------------------------------------------------------#
# calling xmlfiles_df
# -------------------------------------------------------------#

def xmlfiles_df(spark, logger, dict_list, dict_df, config):
    logger.info("Creating XMLFiles table")

    prefix = "{prefix}".format(prefix=config['input']['prefix']['prefix_store'])

    xmlfilename_df = dict_df['TransactionHeader'].select(F.col('xmlfilename'))
    xmlfilename_df = xmlfilename_df.withColumn('RetailStoreID', F.regexp_extract("xmlfilename", "[0-9]{4}", 0))
    xmlfilename_df = xmlfilename_df.withColumn('RetailStoreID', F.concat(F.lit(prefix), F.col('RetailStoreID')))
    xmlfilename_df = xmlfilename_df.withColumn('BusinessDate', F.regexp_extract("xmlfilename", "[0-9]{8}", 0))
    xmlfilename_df = xmlfilename_df.withColumn('CreationDateTime', F.regexp_extract("xmlfilename", "[0-9]{14}", 0))
    xmlfilename_df = xmlfilename_df.withColumn('BusinessDate',
                                               F.date_format(F.to_date(('BusinessDate'), 'yyyyMMdd'), 'yyyy-MM-dd'))
    xmlfilename_df = xmlfilename_df.withColumn('ConsecutiveNumber',
                                               F.regexp_extract("xmlfilename", "[0-9]\.[a-zA-Z0-9]+\.", 0))
    xmlfilename_df = xmlfilename_df.withColumn('ConsecutiveNumber',
                                               F.regexp_extract("ConsecutiveNumber", "[0-9]{5}+", 0))
    xmlfilename_df = xmlfilename_df.withColumn('LoadID', F.lit(0))
    xmlfilename_df = xmlfilename_df.dropDuplicates(['xmlfilename'])
    xmlfilename_df = xmlfilename_df.toPandas()
    xmlfilename_df['CreationDateTime'] = pd.to_datetime(xmlfilename_df['CreationDateTime'])
    xmlfilename_df = spark.createDataFrame(xmlfilename_df)

    # invoking xmlfiles in logtables
    log.xmlfiles(spark, xmlfilename_df, config)


# -------------------------------------------------------------#
# calling dailyloadlog
# -------------------------------------------------------------#

def dailyloadlog(spark, logger, dict_list, config):
    logger.info('Creating DailyLoadLog table')

    warningscount = len(dict_list['warninglist'])
    errorscount = len(dict_list['errorlist'])

    SourceFolder = dict_list['sourcefolderlist']
    StoreCount = dict_list['stores']
    WarningsCount = warningscount
    ErrorsCount = errorscount
    StartDateTime = dict_list['startdatetime']
    EndDateTime = dict_list['enddatetime']
    LoadID = 0

    dailyloadlog_df = pd.DataFrame(
        data={'SourceFolder': [SourceFolder], 'StoreCount': StoreCount, 'WarningsCount': WarningsCount,
              'ErrorsCount': ErrorsCount, 'StartDateTime': StartDateTime, 'EndDateTime': EndDateTime, 'LoadID': LoadID})
    dailyloadlog_df = spark.createDataFrame(dailyloadlog_df)
    dailyloadlog_df = (dailyloadlog_df
                       .withColumn("tmp", F.arrays_zip('SourceFolder'))
                       .withColumn("tmp", F.explode("tmp"))
                       .select(F.col('tmp.SourceFolder'), 'StoreCount', 'WarningsCount', 'ErrorsCount', 'StartDateTime',
                               'EndDateTime', 'LoadID'))
    dailyloadlog_df = dailyloadlog_df.withColumn("SourceFolder", F.concat_ws(",", F.col('SourceFolder')))

    # invoking daily_log_log in logtables
    log.daily_load_log(spark, dailyloadlog_df, config)
    logger.info('dataingestion_main.dailyloadlog')


# -------------------------------------------------------------#
# calling storein_daily_loadlog
# -------------------------------------------------------------#

def storein_daily_loadlog(spark, logger, dict_list, config, processdata):
    logger.info('Creating StoreInDailyLoadLog table')

    warningscount = len(dict_list['warninglist'])
    errorscount = len(dict_list['errorlist'])

    storein_dict = {
        'RetailStoreID': dict_list['storecountlist'],
        'Status': dict_list['statuslist'],
        'StartDateTime': dict_list['startdatetime'],
        'EndDateTime': dict_list['enddatetime'],
        'WarningsCount': warningscount,
        'ErrorsCount': errorscount,
        'LoadID': 0
    }

    storein_df = pd.DataFrame(storein_dict, index=[0])
    storein_loadlog_df = spark.createDataFrame(storein_df)
    storein_loadlog_df = storein_loadlog_df.withColumn('RetailStoreID', F.explode(F.col('RetailStoreID')))
    storein_loadlog_df = storein_loadlog_df.withColumn('Status', F.split(F.col('Status'), ','))
    storein_loadlog_df = storein_loadlog_df.withColumn('Status', F.explode(F.col('Status')))
    stoteindailyloadlog_df = processdata.join(storein_loadlog_df, ['RetailStoreID'])

    # invoking store_in_daily_loadlog in logtables
    log.store_in_daily_loadlog(spark, stoteindailyloadlog_df, config)

    return stoteindailyloadlog_df


# -------------------------------------------------------------#
# calling logging_list
# -------------------------------------------------------------#

def logging_list(spark, logger, config, log_dict, logging_error):
    logger.info('Creating EventLog table')

    """
    
    if (len(log_dict['retailstoreid']) == 0):
        log_dict['retailstoreid'].append('null')
    
    logging_df = pd.DataFrame(log_dict, index=[0])
    
    logging_df = spark.createDataFrame(logging_df)
    logging_df = logging_df.withColumn('LoadID', F.lit(0))
    
    """

    logging_error = logging_error.select('EventDateTime', 'RetailStoreID', 'LoadID', 'Level', 'EventType',
                                         'EventMessage')
    logging_error = logging_error.dropDuplicates(['RetailStoreID'])

    log.eventlog_di(spark, logging_error, config)


# -------------------------------------------------------------#
# Defining copy_staging_to_final
# -------------------------------------------------------------#
def copy_stng_to_final(spark, logger, config):
    """Invokes the stored procedure initialized in the db to copy
    the data from staging to final
    
    Return : None
    """

    logger.info('Copying master data from staging to final')

    driver = "{driver}".format(driver=config['output']['db_setting']['pyodbc_driver'])
    sqlserver = "{serverip}".format(serverip=config['output']['db_setting']['pyodbc_server'])
    database_name = "{database_name}".format(database_name=config['output']['db_setting']['database_name'])
    username = "{username}".format(username=config['output']['db_setting']['user_name'])
    password = "{password}".format(password=config['output']['db_setting']['password'])

    conn = pyodbc.connect(
        'DRIVER={driver};SERVER={server};DATABASE={db};UID={uid};PWD={pwd}'.format(driver=driver,
                                                                                   server=sqlserver, db=database_name,
                                                                                   uid=username,
                                                                                   pwd=password))

    sqlExecSP = "{call MasterData ()}"
    cursor = conn.cursor()
    cursor.execute(sqlExecSP)
    conn.commit()


# -------------------------------------------------------------#
# calling copy_masterdata
# -------------------------------------------------------------#

def copy_masterdata(df, path):
    """writes the DataFrame intp the hdfs path
    
    Parameters
    ----------
    df : Returned dataframe from process_masterdata functions
    path : Path to which DataFrame needs to be copied

    Return
    ------
    None 
    
    """

    # databricks
    global file_type
    if file_type == 'parquet':
        df.coalesce(24).write.mode('overwrite').parquet(path)
    else:
        df.coalesce(24).write.mode('overwrite').format('delta').option('overwriteSchema', 'true').save(path)


# -------------------------------------------------------------#
# calling copy_to_parquet
# -------------------------------------------------------------#

def copy_to_parquet(df, path):
    """writes the DataFrame intp the hdfs path
    
    Parameters
    ----------
    df : Returned dataframe from create_data_frame functions
    path : Path to which DataFrame needs to be copied

    Return
    ------
    None 
    
    """
    # databricks
    global file_type
    if file_type == 'parquet':
        df.coalesce(24).write.partitionBy('RetailStoreID').mode('overwrite').parquet(path)
    else:
        df.coalesce(24).write.partitionBy('RetailStoreID').mode('overwrite').format('delta').option('overwriteSchema',
                                                                                                    'true').save(path)
