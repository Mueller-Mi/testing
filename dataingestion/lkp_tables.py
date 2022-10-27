# Databricks notebook source
#-*- coding: utf-8 -*-
#-------------------------------------------------------------#
#importing functions
#-------------------------------------------------------------#
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
from pyspark import SparkConf
from pyspark.context import SparkContext 
from pyspark.sql import SparkSession, SQLContext
import atexit
import logging
import json
from datetime import datetime
# import yaml

#-------------------------------------------------------------#
#defining main
#-------------------------------------------------------------#
def main(spark, sc):
    #store = 'SB'
    dfnb = spark.sql("show databases like 'fraud_nb'")
    dfsb = spark.sql("show databases like 'fraud_sb'")
    if dfsb.count() > 0:
        store='SB'
    if dfnb.count() > 0:
        store='NB'
    #configuration = json.loads(dbutils.notebook.run('../Config/config', 60, {'store': store}))
    if store == 'SB':
        sql_password = dbutils.secrets.getBytes(scope = "dbw-secretScope-prod-SB", key="kv-dbw-prod-user-sb-sql-db-password").decode('UTF-8')
    elif store == 'NB':
        sql_password = dbutils.secrets.getBytes(scope = "dbw-secretScope-prod-NB", key="kv-dbw-prod-user-nb-sql-db-password").decode('UTF-8')
    config_SB = {
    "input": {
        "account_number_cast": {"status": False},
        "batch_range": {"num_stores": 200},
        "check_list_switch": {"status": True},
        "date_range": {"date1": 0, "date2": 1},
        "di_conf": {
            "autoBroadcastJoinThreshold": "-1",
            "broadcastTimeout": "1200",
            "deploy_mode": "client",
            "driver_memory": "32g",
            "executor_memory": "32g",
            "max_result_size": "16g"
        },
        "encryption": {"encryption_key": 897598, "status": False},
        "files": {
            "init_lkp_classification": "/mnt/adls/SuedBayern/data/initfiles/lkp_classification",
            "md_ebus_mhl": "/mnt/adls/SuedBayern/data/masterdata/EBUS_WG_TXT.csv",
            "md_edeka_mhl": "/mnt/adls/SuedBayern/data/masterdata/FRAUD_EDEKA_WG_TXT.csv",
            "source_folder": "/mnt/prd/SuedBayern/EH/Kasse/EXPORT_XML/Transaktion.delta/"
        },
        "file_type": {"file_type": 'delta'},
        "hive": {"database": "fraud_sb"},
        "prefix": {"prefix_store": 40},
        "re_conf": {
            "autoBroadcastJoinThreshold": "-1",
            "broadcastTimeout": "1200",
            "deploy_mode": "client",
            "driver_memory": "39g",
            "executor_memory": "39g",
            "max_result_size": "25g"
        },
        "type": {"type": "seh"}
    },
    "output": {
        "db_setting": {
            "database_name": "orpfrd-prod-gwc-sqldb-SB",
            "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
            "password": sql_password,
            "user_name": "orpfrd_sb_proddbwuser",
            "port": 1433,
            "pyodbc_driver": "{ODBC Driver 17 for SQL Server}",
            "pyodbc_server": "orpfrd-prod-gwc-sql-sb.database.windows.net",
            "server_ip": "//orpfrd-prod-gwc-sql-sb.database.windows.net"
        },
        "db_tables": {
            "emptiesredemption": "staging.Empties_Redemption",
            "lineitem": "staging.LineItem",
            "loyaltyreward": "staging.LoyaltyReward",
            "mdarticledescription": "staging.MD_ArticleDescription",
            "mdmerchandiselevel": "staging.MD_MerchandiseHierarchyLevels",
            "pricelookup": "staging.PriceLookup",
            "receiptvoidingtable": "staging.ReceiptVoidingTable",
            "retailpricemodifier": "staging.RetailPriceModifier",
            "retailtransactionheader": "staging.RetailTransactionHeader",
            "salereturn": "staging.SaleReturn",
            "suspendedfraud": "staging.SuspendedFraud",
            "tender": "staging.Tender",
            "transactionheader": "staging.TransactionHeader"
        },
        "lkp_tables": {
            "cardtype": "staging.Lkp_CardType",
            "classification": "staging.Lkp_Classification",
            "entrymethod": "staging.Lkp_EntryMethod",
            "itemtype": "staging.Lkp_ItemType",
            "md_tendertypes": "staging.MD_TenderTypes",
            "tenderid": "staging.Lkp_TenderID",
            "transactionstatus": "staging.Lkp_TransactionStatus",
            "transactiontype": "staging.Lkp_TransactionType",
            "typecode": "staging.Lkp_TypeCode"
        },
        "lkp_tables_final": {
            "cardtype": "final.Lkp_CardType",
            "classification": "final.Lkp_Classification",
            "entrymethod": "final.Lkp_EntryMethod",
            "itemtype": "final.Lkp_ItemType",
            "md_tendertypes": "final.MD_TenderTypes",
            "tenderid": "final.Lkp_TenderID",
            "transactionstatus": "final.Lkp_TransactionStatus",
            "transactiontype": "final.Lkp_TransactionType",
            "typecode": "final.Lkp_TypeCode"
        },
        "load_control": {
            "loadcontrol_final": "final.LoadControl",
            "loadcontrol_staging": "staging.LoadControl",
            "stores": "final.Stores"
        },
        "log_tables": {
            "dailyloadlog_final": "final.DailyLoadLog",
            "dailyloadlog_staging": "staging.DailyLoadLog",
            "logging_final": "final.EventLog",
            "logging_staging": "staging.EventLog",
            "storein_final": "final.StoreInDailyLoadLog",
            "storein_staging": "staging.StoreInDailyLoadLog",
            "xmlfiles_final": "final.XMLFiles",
            "xmlfiles_staging": "staging.XMLFiles"
        },
        "logpath": {"logpath": "/mnt/adls/SuedBayern/logs/"},
        "lookupfolder": {
            "cardtype": "/mnt/adls/SuedBayern/data/lkp_tables/lkp_cardtype",
            "classification": "/mnt/adls/SuedBayern/data/lkp_tables/lkp_classification",
            "entrymethod": "/mnt/adls/SuedBayern/data/lkp_tables/lkp_entrymethod",
            "itemtype": "/mnt/adls/SuedBayern/data/lkp_tables/lkp_itemtype",
            "tenderid": "/mnt/adls/SuedBayern/data/lkp_tables/lkp_tenderid",
            "transactionstatus": "/mnt/adls/SuedBayern/data/lkp_tables/lkp_transactionstatus",
            "transactiontype": "/mnt/adls/SuedBayern/data/lkp_tables/lkp_transactiontype",
            "typecode": "/mnt/adls/SuedBayern/data/lkp_tables/lkp_typecode"
        },
        "processed_df_folder": {
            "articledescription": "/mnt/adls/SuedBayern/Hive/parquet/MD_ArticleDescription",
            "controltransaction": "/mnt/adls/SuedBayern/Hive/parquet/controltransaction",
            "customer": "/mnt/adls/SuedBayern/Hive/parquet/customer",
            "functionlog": "/mnt/adls/SuedBayern/Hive/parquet/functionlog",
            "lineitem": "/mnt/adls/SuedBayern/Hive/parquet/lineitem",
            "loyaltyreward": "/mnt/adls/SuedBayern/Hive/parquet/loyaltyreward",
            "merchandiselevel": "/mnt/adls/SuedBayern/Hive/parquet/MD_MerchandiseHierarchyLevels",
            "receiptheaderaddonlist": "/mnt/adls/SuedBayern/Hive/parquet/receiptheaderaddonlist",
            "receiptpositionaddonlist": "/mnt/adls/SuedBayern/Hive/parquet/receiptpositionaddonlist",
            "retailpricemodifier": "/mnt/adls/SuedBayern/Hive/parquet/retailpricemodifier",
            "retailtransactionheader": "/mnt/adls/SuedBayern/Hive/parquet/retailtransactionheader",
            "return": "/mnt/adls/SuedBayern/Hive/parquet/return",
            "sale": "/mnt/adls/SuedBayern/Hive/parquet/sale",
            "salereturn_rebate": "/mnt/adls/SuedBayern/Hive/parquet/salereturn_rebate",
            "stores": "/mnt/adls/SuedBayern/Hive/parquet/stores",
            "tender": "/mnt/adls/SuedBayern/Hive/parquet/tender",
            "tendercontrolloan": "/mnt/adls/SuedBayern/Hive/parquet/tendercontrolloan",
            "tendercontrolpaymentdeposit": "/mnt/adls/SuedBayern/Hive/parquet/tendercontrolpaymentdeposit",
            "tendercontrolpickup": "/mnt/adls/SuedBayern/Hive/parquet/tendercontrolpickup",
            "tendercontrolsafesettle": "/mnt/adls/SuedBayern/Hive/parquet/tendercontrolsafesettle",
            "tendercontroltillsettle": "/mnt/adls/SuedBayern/Hive/parquet/tendercontroltillsettle",
            "transactionheader": "/mnt/adls/SuedBayern/Hive/parquet/transactionheader"
        },
        "ruleengine": {
            "Classification_Description_folder": "/mnt/adls/SuedBayern/Hive/parquet/DF_Classification_Description",
            "Empties_folder": "/mnt/adls/SuedBayern/Hive/parquet/DF_Empties_Redemption",
            "Lineitem_derived_folder": "/mnt/adls/SuedBayern/Hive/parquet/derived/Lineitem_derived",
            "PriceLookUp_folder": "/mnt/adls/SuedBayern/Hive/parquet/DF_PriceLookUp",
            "ReceiptVoiding_folder": "/mnt/adls/SuedBayern/Hive/parquet/DF_ReceiptVoidingTable",
            "SuspendedFraud_folder": "/mnt/adls/SuedBayern/Hive/parquet/DF_SuspendedFraud",
            "db_metrics_local_folder": "/mnt/adls/SuedBayern/dbmes/SEH",
            "db_metrics_path": "/mnt/adls/SuedBayern/Hive/parquet/db_metrics",
            "pep_export_path": "/mnt/adls/SuedBayern/Hive/pep/PEP_Report",
            "pep_folder": "/mnt/adls/SuedBayern/datNayerna/SEH/PEP_Export",
            "pep_interim": "/mnt/adls/SuedBayern/Hive/pep/PEP_Intermediate",
            "pep_local_folder": "/mnt/adls/SuedBayern/pep/",
            "restart_failure_batch": "/mnt/adls/SuedBayern/Hive/parquet/re_failure_batch"
        },
        "stores": {
            "stores_path": "/mnt/adls/SuedBayern/initfiles/",
            "stores_staging": "staging.Stores"
        },
        "ui_tables_staging": {
            "reporttables": "staging.ReportTables",
            "tabledetails": "staging.TableDetails",
            "users": "staging.Users"
        }
    }
    }
    config_NB = {
    "input": {
        "account_number_cast": {"status": True},
        "batch_range": {"num_stores": 10000},
        "check_list_switch": {"status": True},
        "date_range": {"date1": 0, "date2": 1},
        "di_conf": {
            "autoBroadcastJoinThreshold": "-1",
            "broadcastTimeout": "1200",
            "deploy_mode": "client",
            "driver_memory": "14g",
            "executor_memory": "14g",
            "max_result_size": "7g"
        },
        "encryption": {"encryption_key": 897598, "status": True},
        "files": {
            "init_lkp_classification": "/mnt/adls/NordBayern/data/initfiles/lkp_classification",
            "md_ebus_mhl": "/mnt/adls/NordBayern/data/masterdata/EBUS_WG_TXT.csv",
            "md_edeka_mhl": "/mnt/adls/NordBayern/data/masterdata/FRAUD_EDEKA_WG_TXT.csv",
            "source_folder": "/mnt/prd/NordBayern/EH/Kasse/EXPORT_XML/Transaktion.delta/"
        
        },
        "file_type": {"file_type": 'delta'},
        "hive": {"database": "fraud_nb"},
        "prefix": {"prefix_store": 24},
        "re_conf": {
            "autoBroadcastJoinThreshold": "-1",
            "broadcastTimeout": "1200",
            "deploy_mode": "client",
            "driver_memory": "25g",
            "executor_memory": "25g",
            "max_result_size": "16g"
        },
        "type": {"type": "reh"}
    },
    "output": {
        "db_setting": {
            "database_name": "orpfrd-prod-gwc-sqldb-NB",
            "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
            "password": sql_password,
            "user_name": "orpfrd_nb_proddbwuser",
            "port": 1433,
            "pyodbc_driver": "{ODBC Driver 17 for SQL Server}",
            "pyodbc_server": "orpfrd-prod-gwc-sql-nb.database.windows.net",
            "server_ip": "//orpfrd-prod-gwc-sql-nb.database.windows.net",
           
        },
        "db_tables": {
            "emptiesredemption": "staging.Empties_Redemption",
            "lineitem": "staging.LineItem",
            "loyaltyreward": "staging.LoyaltyReward",
            "mdarticledescription": "staging.MD_ArticleDescription",
            "mdmerchandiselevel": "staging.MD_MerchandiseHierarchyLevels",
            "pricelookup": "staging.PriceLookup",
            "receiptvoidingtable": "staging.ReceiptVoidingTable",
            "retailpricemodifier": "staging.RetailPriceModifier",
            "retailtransactionheader": "staging.RetailTransactionHeader",
            "salereturn": "staging.SaleReturn",
            "suspendedfraud": "staging.SuspendedFraud",
            "tender": "staging.Tender",
            "transactionheader": "staging.TransactionHeader"
        },
        "lkp_tables": {
            "cardtype": "staging.Lkp_CardType",
            "classification": "staging.Lkp_Classification",
            "entrymethod": "staging.Lkp_EntryMethod",
            "itemtype": "staging.Lkp_ItemType",
            "md_tendertypes": "staging.MD_TenderTypes",
            "tenderid": "staging.Lkp_TenderID",
            "transactionstatus": "staging.Lkp_TransactionStatus",
            "transactiontype": "staging.Lkp_TransactionType",
            "typecode": "staging.Lkp_TypeCode"
        },
        "lkp_tables_final": {
            "cardtype": "final.Lkp_CardType",
            "classification": "final.Lkp_Classification",
            "entrymethod": "final.Lkp_EntryMethod",
            "itemtype": "final.Lkp_ItemType",
            "md_tendertypes": "final.MD_TenderTypes",
            "tenderid": "final.Lkp_TenderID",
            "transactionstatus": "final.Lkp_TransactionStatus",
            "transactiontype": "final.Lkp_TransactionType",
            "typecode": "final.Lkp_TypeCode"
        },
        "load_control": {
            "loadcontrol_final": "final.LoadControl",
            "loadcontrol_staging": "staging.LoadControl",
            "stores": "final.Stores"
        },
        "log_tables": {
            "dailyloadlog_final": "final.DailyLoadLog",
            "dailyloadlog_staging": "staging.DailyLoadLog",
            "logging_final": "final.EventLog",
            "logging_staging": "staging.EventLog",
            "storein_final": "final.StoreInDailyLoadLog",
            "storein_staging": "staging.StoreInDailyLoadLog",
            "xmlfiles_final": "final.XMLFiles",
            "xmlfiles_staging": "staging.XMLFiles"
        },
        "logpath": {"logpath": "/mnt/adls/NordBayern/logs/"},
        "lookupfolder": {
            "cardtype": "/mnt/adls/NordBayern/data/lkp_tables/lkp_cardtype",
            "classification": "/mnt/adls/NordBayern/data/lkp_tables/lkp_classification",
            "entrymethod": "/mnt/adls/NordBayern/data/lkp_tables/lkp_entrymethod",
            "itemtype": "/mnt/adls/NordBayern/data/lkp_tables/lkp_itemtype",
            "tenderid": "/mnt/adls/NordBayern/data/lkp_tables/lkp_tenderid",
            "transactionstatus": "/mnt/adls/NordBayern/data/lkp_tables/lkp_transactionstatus",
            "transactiontype": "/mnt/adls/NordBayern/data/lkp_tables/lkp_transactiontype",
            "typecode": "/mnt/adls/NordBayern/data/lkp_tables/lkp_typecode"
        },
        "processed_df_folder": {
            "articledescription": "/mnt/adls/NordBayern/Hive/parquet/MD_ArticleDescription",
            "controltransaction": "/mnt/adls/NordBayern/Hive/parquet/controltransaction",
            "customer": "/mnt/adls/NordBayern/Hive/parquet/customer",
            "functionlog": "/mnt/adls/NordBayern/Hive/parquet/functionlog",
            "lineitem": "/mnt/adls/NordBayern/Hive/parquet/lineitem",
            "loyaltyreward": "/mnt/adls/NordBayern/Hive/parquet/loyaltyreward",
            "merchandiselevel": "/mnt/adls/NordBayern/Hive/parquet/MD_MerchandiseHierarchyLevels",
            "receiptheaderaddonlist": "/mnt/adls/NordBayern/Hive/parquet/receiptheaderaddonlist",
            "receiptpositionaddonlist": "/mnt/adls/NordBayern/Hive/parquet/receiptpositionaddonlist",
            "retailpricemodifier": "/mnt/adls/NordBayern/Hive/parquet/retailpricemodifier",
            "retailtransactionheader": "/mnt/adls/NordBayern/Hive/parquet/retailtransactionheader",
            "return": "/mnt/adls/NordBayern/Hive/parquet/return",
            "sale": "/mnt/adls/NordBayern/Hive/parquet/sale",
            "salereturn_rebate": "/mnt/adls/NordBayern/Hive/parquet/salereturn_rebate",
            "stores": "/mnt/adls/NordBayern/Hive/parquet/stores",
            "tender": "/mnt/adls/NordBayern/Hive/parquet/tender",
            "tendercontrolloan": "/mnt/adls/NordBayern/Hive/parquet/tendercontrolloan",
            "tendercontrolpaymentdeposit": "/mnt/adls/NordBayern/Hive/parquet/tendercontrolpaymentdeposit",
            "tendercontrolpickup": "/mnt/adls/NordBayern/Hive/parquet/tendercontrolpickup",
            "tendercontrolsafesettle": "/mnt/adls/NordBayern/Hive/parquet/tendercontrolsafesettle",
            "tendercontroltillsettle": "/mnt/adls/NordBayern/Hive/parquet/tendercontroltillsettle",
            "transactionheader": "/mnt/adls/NordBayern/Hive/parquet/transactionheader"
        },
        "ruleengine": {
            "Classification_Description_folder": "/mnt/adls/NordBayern/Hive/parquet/DF_Classification_Description",
            "Empties_folder": "/mnt/adls/NordBayern/Hive/parquet/DF_Empties_Redemption",
            "Lineitem_derived_folder": "/mnt/adls/NordBayern/Hive/parquet/derived/Lineitem_derived",
            "PriceLookUp_folder": "/mnt/adls/NordBayern/Hive/parquet/DF_PriceLookUp",
            "ReceiptVoiding_folder": "/mnt/adls/NordBayern/Hive/parquet/DF_ReceiptVoidingTable",
            "SuspendedFraud_folder": "/mnt/adls/NordBayern/Hive/parquet/DF_SuspendedFraud",
            "db_metrics_local_folder": "/mnt/adls/NordBayern/dbmes/SEH",
            "db_metrics_path": "/mnt/adls/NordBayern/Hive/parquet/db_metrics",
            "pep_export_path": "/mnt/adls/NordBayern/Hive/pep/PEP_Report",
            "pep_folder": "/mnt/adls/NordBayern/datNayerna/SEH/PEP_Export",
            "pep_interim": "/mnt/adls/NordBayern/Hive/pep/PEP_Intermediate",
            "pep_local_folder": "/mnt/adls/NordBayern/pep/",
            "restart_failure_batch": "/mnt/adls/NordBayern/Hive/parquet/re_failure_batch"
        },
        "stores": {
            "stores_path": "/mnt/adls/NordBayern/initfiles/",
            "stores_staging": "staging.Stores"
        },
        "ui_tables_staging": {
            "reporttables": "staging.ReportTables",
            "tabledetails": "staging.TableDetails",
            "users": "staging.Users"
        }
    }
    }
    if dfsb.count() > 0:
        store='SB'
        configuration=config_SB
    if dfnb.count() > 0:
        store='NB'
        configuration=config_NB
    configuration['runtime'] = 'databricks'
    config = configuration
    currentdate = datetime.now().strftime("%Y%m%d")
    hrmi = datetime.now().strftime("%H%M")
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    fomatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler = logging.FileHandler(filename=f'/tmp/lkp_tables_{currentdate}_{hrmi}.log')
    file_handler.setFormatter(fomatter)
    logger.addHandler(file_handler)
    logger.info('Calling Lookup table definitions')
    
    db_settings = db_settings_lkptables(spark, logger, config)
    lkp_cardtype(spark, sc, logger, db_settings)
    lkp_classification(spark, sc, logger, db_settings)
    lkp_entrymethod(spark, sc, logger, db_settings)
    lkp_itemtype(spark, sc, logger, db_settings)
    lkp_tenderid(spark, sc, logger, db_settings)
    lkp_transactionstatus(spark, sc, logger, db_settings)
    lkp_transactiontype(spark, sc, logger, db_settings)
    lkp_typecode(spark, sc, logger, db_settings)
    


#-------------------------------------------------------------#
#calling db_settings
#-------------------------------------------------------------#
def db_settings_lkptables(spark, logger, config):

    """Creates a db_settings dictionary

    return: db_settings
    """

    logger.info('Creating db settings for LoadControl')

    db_settings = {
        'sqlserver' : "jdbc:sqlserver:{serverip}:{port};database={database}".format(serverip=config['output']['db_setting']['server_ip'], port=config['output']['db_setting']['port'], database=config['output']['db_setting']['database_name'] ),
        'username' : "{username}".format(username=config['output']['db_setting']['user_name']),
        'password' : "{password}".format(password=config['output']['db_setting']['password']),
        'driver' : "{driver}".format(driver=config['output']['db_setting']['driver']),
        
        'init_lkp_classification' : "{init_lkp_classification}".format(init_lkp_classification=config['input']['files']['init_lkp_classification']),

        'cardtypefolder' : "{cardtypefolder}".format(cardtypefolder=config['output']['lookupfolder']['cardtype']),
        'classificationfolder' : "{classificationfolder}".format(classificationfolder=config['output']['lookupfolder']['classification']),
        'entrymethodfolder' : "{entrymethodfolder}".format(entrymethodfolder=config['output']['lookupfolder']['entrymethod']),
        'itemtypefolder' : "{itemtypefolder}".format(itemtypefolder=config['output']['lookupfolder']['itemtype']),
        'tenderidfolder' : "{tenderidfolder}".format(tenderidfolder=config['output']['lookupfolder']['tenderid']),
        'transactionstatusfolder' : "{transactionstatusfolder}".format(transactionstatusfolder=config['output']['lookupfolder']['transactionstatus']),
        'transactiontypefolder' : "{transactiontypefolder}".format(transactiontypefolder=config['output']['lookupfolder']['transactiontype']),
        'typecodefolder' : "{typecodefolder}".format(typecodefolder=config['output']['lookupfolder']['typecode']),

        'cardtype' : "{cardtype}".format(cardtype=config['output']['lkp_tables']['cardtype']),
        'classification' : "{classification}".format(classification=config['output']['lkp_tables']['classification']),
        'entrymethod' : "{entrymethod}".format(entrymethod=config['output']['lkp_tables']['entrymethod']),
        'itemtype' : "{itemtype}".format(itemtype=config['output']['lkp_tables']['itemtype']),
        'tenderid' : "{tenderid}".format(tenderid=config['output']['lkp_tables']['tenderid']),
        'transactionstatus' : "{transactionstatus}".format(transactionstatus=config['output']['lkp_tables']['transactionstatus']),
        'transactiontype' : "{transactiontype}".format(transactiontype=config['output']['lkp_tables']['transactiontype']),
        'typecode' : "{typecode}".format(typecode=config['output']['lkp_tables']['typecode']),
        
        'database' : "{database}".format(database=config['input']['hive']['database'])
    }

    return db_settings

#-------------------------------------------------------------#
#calling lkp_cardtype
#-------------------------------------------------------------#
def lkp_cardtype(spark, sc, logger, db_settings):
    
    """Creates a lkp_cardtype_df

    return: None
    """

    logger.info('Creating Lkp_CardType DataFrame')

    rdd = sc.parallelize([(0, 'Credit') , (1, 'Debit')])

    lkp_cardtype_df = rdd.toDF(['CardType', 'CardTypeDesc'])

    lkp_cardtype_df.coalesce(1).write.mode('overwrite').format('delta').save(db_settings['cardtypefolder'])

    spark.sql("""create table if not exists {fraud}.Lkp_CardType using delta location '{path}'""".format(path=db_settings['cardtypefolder'], fraud=db_settings['database']))

    lkp_cardtype_df.write.mode("overwrite").format("jdbc")\
    .option("url", db_settings['sqlserver'])\
    .option("dbtable", db_settings['cardtype'])\
    .option("user", db_settings['username'])\
    .option("password", db_settings['password'])\
    .option("driver", db_settings['driver'])\
    .save()
    
#-------------------------------------------------------------#
#calling lkp_classification
#-------------------------------------------------------------#
def lkp_classification(spark, sc, logger, db_settings):
    
    """Creates a lkp_classification_df

    return: None
    """

    logger.info('Creating Lkp_Classification DataFrame')

    lkp_classification = spark.read.format("text").option("header", "true").option("delimiter", "/t").option("encoding", "utf-8").load(db_settings['init_lkp_classification'])

    lkp_classification_df = lkp_classification.withColumn('class_id', F.split('value', '\t')[0])
    lkp_classification_df = lkp_classification_df.withColumn('class_desc_en', F.split('value', '\t')[1])
    lkp_classification_df = lkp_classification_df.withColumn('class_desc_gr', F.split('value', '\t')[2])
    lkp_classification_df = lkp_classification_df.select('class_id', 'class_desc_en', 'class_desc_gr')

    lkp_classification_df.coalesce(1).write.mode('overwrite').format('delta').save(db_settings['classificationfolder'])

    spark.sql("""create table if not exists {fraud}.Lkp_Classification using delta location '{path}'""".format(path=db_settings['classificationfolder'], fraud=db_settings['database']))

    lkp_classification_df.write.mode("overwrite").format("jdbc")\
    .option("url", db_settings['sqlserver'])\
    .option("dbtable", db_settings['classification'])\
    .option("user", db_settings['username'])\
    .option("password", db_settings['password'])\
    .option("driver", db_settings['driver'])\
    .save()
    
#-------------------------------------------------------------#
#calling lkp_entrymethod
#-------------------------------------------------------------#
def lkp_entrymethod(spark, sc, logger, db_settings):
    
    """Creates a lkp_entrymethod_df

    return: None
    """

    logger.info('Creating Lkp_EntryMethod DataFrame')

    rdd = sc.parallelize([(0, 'Scanned') , (1, 'Keyed')])

    lkp_entrymethod_df = rdd.toDF(['EntryMethod', 'EntryMethodDesc'])

    lkp_entrymethod_df.coalesce(1).write.mode('overwrite').format('delta').save(db_settings['entrymethodfolder'])

    spark.sql("""create table  if not exists {fraud}.Lkp_EntryMethod using delta location '{path}'""".format(path=db_settings['entrymethodfolder'], fraud=db_settings['database']))

    lkp_entrymethod_df.write.mode("overwrite").format("jdbc")\
    .option("url", db_settings['sqlserver'])\
    .option("dbtable", db_settings['entrymethod'])\
    .option("user", db_settings['username'])\
    .option("password", db_settings['password'])\
    .option("driver", db_settings['driver'])\
    .save()

#-------------------------------------------------------------#
#calling lkp_itemtype
#-------------------------------------------------------------#
def lkp_itemtype(spark, sc, logger, db_settings):

    """Creates a lkp_itemtype_df

    return: None
    """
    
    logger.info('Creating Lkp_ItemType DataFrame')

    rdd = sc.parallelize([(0, 'Deposit') , (1, 'Stock'), (2, 'DepositRefund')])

    lkp_itemtype_df = rdd.toDF(['ItemType', 'ItemTypeDesc'])

    lkp_itemtype_df.coalesce(1).write.mode('overwrite').format('delta').save(db_settings['itemtypefolder'])

    spark.sql("""create table  if not exists {fraud}.Lkp_ItemType using delta location '{path}'""".format(path=db_settings['itemtypefolder'], fraud=db_settings['database']))

    lkp_itemtype_df.write.mode("overwrite").format("jdbc")\
    .option("url", db_settings['sqlserver'])\
    .option("dbtable", db_settings['itemtype'])\
    .option("user", db_settings['username'])\
    .option("password", db_settings['password'])\
    .option("driver", db_settings['driver'])\
    .save()

#-------------------------------------------------------------#
#calling lkp_tenderid
#-------------------------------------------------------------#
def lkp_tenderid(spark, sc, logger, db_settings):
    
    """Creates a lkp_tenderid_df

    return: None
    """

    logger.info('Creating Lkp_TenderID DataFrame')

    rdd = sc.parallelize([(0, 'YTEC') , (1, 'YTVC'), (2, 'YTSX'), (3, 'YTER'), (4, 'YTAM'), (5, 'YTVO'), (6, 'YTCE'), (7, 'YTKR'), (8, 'YTDT'), (9, 'YTSZ'), (10, 'YTVP'), (11, 'YTVB'), (12, 'YTVG'), (13, 'YTMC'), (14, 'YTDP'), (15, 'YTEM'), (16, 'YTSO'), (17, 'YTVI'), (18, 'YTPG'), (19, 'YTMA'), (20, 'YTNG'), (21, 'YTCS')])

    lkp_tender_df = rdd.toDF(['TenderID', 'TenderIDDesc'])

    lkp_tender_df.coalesce(1).write.mode('overwrite').format('delta').save(db_settings['tenderidfolder'])

    spark.sql("""create table if not exists {fraud}.Lkp_TenderID using delta location '{path}'""".format(path=db_settings['tenderidfolder'], fraud=db_settings['database']))

    lkp_tender_df.write.mode("overwrite").format("jdbc")\
    .option("url", db_settings['sqlserver'])\
    .option("dbtable", db_settings['tenderid'])\
    .option("user", db_settings['username'])\
    .option("password", db_settings['password'])\
    .option("driver", db_settings['driver'])\
    .save()

#-------------------------------------------------------------#
#calling lkp_transactionstatus
#-------------------------------------------------------------#
def lkp_transactionstatus(spark, sc, logger, db_settings):
    
    """Creates a lkp_transactionstatus_df

    return: None
    """

    logger.info('Creating Lkp_TransactionStatus DataFrame')
                                   
    l = [(0,'PostVoided'),
    (1,'Suspended'),
    (2,'SuspendedRetrieved'),
    (3,'Totaled')]
                                   
    lkp_tranactionstatus_df = spark.createDataFrame(l,['TransactionStatus', 'TransactionStatusDesc'])
                                   
    lkp_tranactionstatus_df.coalesce(1).write.mode('overwrite').format('delta').save(db_settings['transactionstatusfolder'])
                                   
    spark.sql("""create table if not exists {fraud}.Lkp_TransactionStatus using delta location '{path}'""".format(path=db_settings['transactionstatusfolder'], fraud=db_settings['database']))

    lkp_tranactionstatus_df.write.mode("overwrite").format("jdbc")\
    .option("url", db_settings['sqlserver'])\
    .option("dbtable", db_settings['transactionstatus'])\
    .option("user", db_settings['username'])\
    .option("password", db_settings['password'])\
    .option("driver", db_settings['driver'])\
    .save()
    
#-------------------------------------------------------------#
#calling lkp_transactiontype
#-------------------------------------------------------------#
def lkp_transactiontype(spark, sc, logger, db_settings):
    
    """Creates a lkp_transactiontype_df

    return: None
    """
                                   
    logger.info('Creating Lkp_TransactionType DataFrame')
                                   
    l = [(0,'ControlTransaction'),
    (1,'FunctionLog'),
    (2,'RetailTransaction'),
    (3,'TenderControlTransaction')]
                                   
    lkp_transactiontype_df = spark.createDataFrame(l,['TransactionType', 'TransactionTypeDesc'])
                                   
    lkp_transactiontype_df.coalesce(1).write.mode('overwrite').format('delta').save(db_settings['transactiontypefolder'])
                                   
    spark.sql("""create table if not exists {fraud}.Lkp_TransactionType using delta location '{path}'""".format(path=db_settings['transactiontypefolder'], fraud=db_settings['database']))

    lkp_transactiontype_df.write.mode("overwrite").format("jdbc")\
    .option("url", db_settings['sqlserver'])\
    .option("dbtable", db_settings['transactiontype'])\
    .option("user", db_settings['username'])\
    .option("password", db_settings['password'])\
    .option("driver", db_settings['driver'])\
    .save()
    
#-------------------------------------------------------------#
#calling lkp_typecode
#-------------------------------------------------------------#
def lkp_typecode(spark, sc, logger, db_settings):
    
    """Creates a lkp_typecode_df

    return: None
    """
                                   
    logger.info('Creating Lkp_TypeCode DataFrame')
                                   
    rdd = sc.parallelize([(0, 'Refund') , (1, 'Sale')])
                                   
    lkp_typecode_df = rdd.toDF(['TypeCode', 'TypeCodeDesc'])
                                   
    lkp_typecode_df.coalesce(1).write.mode('overwrite').format('delta').save(db_settings['typecodefolder'])
                                   
    spark.sql("""create table if not exists {fraud}.Lkp_TypeCode using delta location '{path}'""".format(path=db_settings['typecodefolder'], fraud=db_settings['database']))

    lkp_typecode_df.write.mode("overwrite").format("jdbc")\
    .option("url", db_settings['sqlserver'])\
    .option("dbtable", db_settings['typecode'])\
    .option("user", db_settings['username'])\
    .option("password", db_settings['password'])\
    .option("driver", db_settings['driver'])\
    .save()
    
#-------------------------------------------------------------#
#creating copy_staging_to_final
#-------------------------------------------------------------#
def copy_staging_to_final(spark, logger, config):
    
    """Copies data from staging to final tables
    
    Return : None
    """
    
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
    
    sqlExecSP ="{call Lkp_Tables ()}"
    cursor = conn.cursor()
    cursor.execute(sqlExecSP)
    conn.commit()
    
if __name__ == "__main__": 
    
      main(spark, sc)
                                   
                                   

# COMMAND ----------


