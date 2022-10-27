# Databricks notebook source
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
            "md_articles": "/mnt/adls/SuedBayern/data/masterdata/FRAUD_MFEAN_WG.csv",
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
            "database_name": "orpfrd-dev-gwc-sqldb-SB",
            "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
            "password": sql_password,
            "user_name": "orpfrd_sb_devdbwuser",
            "port": 1433,
            "pyodbc_driver": "{ODBC Driver 17 for SQL Server}",
            "pyodbc_server": "orpfrd-dev-gwc-sql-sb.database.windows.net",
            "server_ip": "//orpfrd-dev-gwc-sql-sb.database.windows.net"
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
