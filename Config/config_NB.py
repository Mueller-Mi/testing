# Databricks notebook source
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
        "encryption": {"encryption_key": 897598, "status": False},
        "files": {
            "init_lkp_classification": "/mnt/adls/NordBayern/data/initfiles/lkp_classification",
            "md_ebus_mhl": "/mnt/adls/NordBayern/data/masterdata/EBUS_WG_TXT.csv",
            "md_edeka_mhl": "/mnt/adls/NordBayern/data/masterdata/FRAUD_EDEKA_WG_TXT.csv",
            "md_articles": "/mnt/adls/NordBayern/data/masterdata/FRAUD_MFEAN_WG.csv",
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
