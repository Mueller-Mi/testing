# Databricks notebook source
# MAGIC %run ./dataingestion_main

# COMMAND ----------

# MAGIC %run ./dataingestion_sql

# COMMAND ----------

# MAGIC %run ./lkp_tables_update

# COMMAND ----------

# MAGIC %run ./load_control

# COMMAND ----------

# MAGIC %run ./logtables

# COMMAND ----------

# MAGIC %run ./masterdata_sql

# COMMAND ----------

# MAGIC %run ./ruleengine_sql

# COMMAND ----------

# MAGIC %run ./xml_parsing

# COMMAND ----------

#dataingestion_main
class di:
  @staticmethod
  def dataingestion_process(spark, logger, dir_path, batch, epoch_id, count, config, loadcontrol, active_inactive_list,
                          active_inactive_count):
    return dataingestion_process(spark, logger, dir_path, batch, epoch_id, count, config, loadcontrol, active_inactive_list,
                          active_inactive_count)
  @staticmethod
  def process_masterdata(spark, logger, config):
    return process_masterdata(spark, logger, config)
  @staticmethod
  def process_master_data(spark, logger, config):
    return process_master_data(spark, logger, config)
  @staticmethod
  def process_data(spark, logger, stores_dir, batch, epoch_id, config, loadcontrol, active_inactive_list,
                 active_inactive_count, dict_list, log_dict):
    return process_data(spark, logger, stores_dir, batch, epoch_id, config, loadcontrol, active_inactive_list,
                 active_inactive_count, dict_list, log_dict)
  @staticmethod
  def create_config_dict(config, log_dict):
    return create_config_dict(config, log_dict)
  @staticmethod
  def get_retailstoreid(spark, active_inactive_list, active_inactive_count, logger, dict_list, config, config_dict,
                      log_dict):
    return get_retailstoreid(spark, active_inactive_list, active_inactive_count, logger, dict_list, config, config_dict,
                      log_dict)
  @staticmethod
  def create_data_frames(logger, spark, df, epoch_id, config, config_dict, loadcontrol, dict_list, log_dict):
    return create_data_frames(logger, spark, df, epoch_id, config, config_dict, loadcontrol, dict_list, log_dict)
  @staticmethod
  def update_hive_table(spark, logger, epoch_id, config_dict, log_dict):
    return update_hive_table(spark, logger, epoch_id, config_dict, log_dict)
  @staticmethod
  def update_exception_status(spark, loadcontrol, logger, e, df, config, dict_list):
    return update_exception_status(spark, loadcontrol, logger, e, df, config, dict_list)
  @staticmethod
  def xmlfiles_df(spark, logger, dict_list, dict_df, config):
    return xmlfiles_df(spark, logger, dict_list, dict_df, config)
  @staticmethod
  def dailyloadlog(spark, logger, dict_list, config):
    return dailyloadlog(spark, logger, dict_list, config)
  @staticmethod
  def storein_daily_loadlog(spark, logger, dict_list, config, processdata):
    return storein_daily_loadlog(spark, logger, dict_list, config, processdata)
  @staticmethod
  def logging_list(spark, logger, config, log_dict, logging_error):
    return logging_list(spark, logger, config, log_dict, logging_error)
  @staticmethod
  def copy_stng_to_final(spark, logger, config):
    return copy_stng_to_final(spark, logger, config)
  @staticmethod
  def copy_masterdata(df, path):
    return copy_masterdata(df, path)
  @staticmethod
  def copy_to_parquet(df, path):
    return copy_to_parquet(df, path)

# COMMAND ----------

#dataingestion_sql
class di_sql:
	@staticmethod
	def dataingestion_to_sql(spark, logger, epoch_id, config):
		return dataingestion_to_sql(spark, logger, epoch_id, config)
	@staticmethod
	def loyaltyreward_to_sql(spark, logger, epoch_id, db_settings, loyaltyreward, database):
		return loyaltyreward_to_sql(spark, logger, epoch_id, db_settings, loyaltyreward, database)
	@staticmethod
	def retailpricemodifier_sql(spark, logger, epoch_id, db_settings, retailpricemodifier, database):
		return retailpricemodifier_sql(spark, logger, epoch_id, db_settings, retailpricemodifier, database)
	@staticmethod
	def retailtransactionheader_to_sql(spark, logger, epoch_id, db_settings, retailtransactionheader, database):
		return retailtransactionheader_to_sql(spark, logger, epoch_id, db_settings, retailtransactionheader, database)
	@staticmethod
	def salereturn_sql(spark, logger, epoch_id, db_settings, salereturn, config, database):
		return salereturn_sql(spark, logger, epoch_id, db_settings, salereturn, config, database)
	@staticmethod
	def tender_sql(spark, logger, epoch_id, db_settings, tender, config, database):
		return tender_sql(spark, logger, epoch_id, db_settings, tender, config, database)

# COMMAND ----------

#lkp_tables_update
class lkp_update:
	@staticmethod
	def db_settings_lkptables(spark, logger, config):
		return db_settings_lkptables(spark, logger, config)
	@staticmethod
	def read_lkp_cardtype(spark, logger, config):
		return read_lkp_cardtype(spark, logger, config)
	@staticmethod
	def read_lkp_entrymethod(spark, logger, config):
		return read_lkp_entrymethod(spark, logger, config)
	@staticmethod
	def read_lkp_itemtype(spark, logger, config):
		return read_lkp_itemtype(spark, logger, config)
	@staticmethod
	def read_lkp_tenderid(spark, logger, config):
		return read_lkp_tenderid(spark, logger, config)
	@staticmethod
	def read_lkp_typecode(spark, logger, config):
		return read_lkp_typecode(spark, logger, config)
	@staticmethod
	def update_lkp_cardtype(spark, logger, cardtype_df, config):
		return update_lkp_cardtype(spark, logger, cardtype_df, config)
	@staticmethod
	def update_lkp_entrymethod(spark, logger, entrymethod_df, config):
		return update_lkp_entrymethod(spark, logger, entrymethod_df, config)
	@staticmethod
	def update_lkp_itemtype(spark, logger, itemtype_df, config):
		return update_lkp_itemtype(spark, logger, itemtype_df, config)
	@staticmethod
	def update_lkp_tenderid(spark, logger, tenderid_df, config):
		return update_lkp_tenderid(spark, logger, tenderid_df, config)
	@staticmethod
	def update_lkp_typecode(spark, logger, typecode_df, config):
		return update_lkp_typecode(spark, logger, typecode_df, config)


# COMMAND ----------

#load_control
class load:
	@staticmethod
	def call_loadcontrol_stored_procedure(spark, logger, config):
		return call_loadcontrol_stored_procedure(spark, logger, config)
	@staticmethod
	def load_control_settings(spark, logger, config):
		return load_control_settings(spark, logger, config)
	@staticmethod
	def read_stores(spark, logger, config):
		return read_stores(spark, logger, config)
	@staticmethod
	def stores_to_loadcontrol(spark, logger, stores, config):
		return stores_to_loadcontrol(spark, logger, stores, config)
	@staticmethod
	def update_loadcontrol(spark, logger, config, loadcontrol, storein_loadlog):
		return update_loadcontrol(spark, logger, config, loadcontrol, storein_loadlog)


# COMMAND ----------

#ruleengine_sql
class re_sql:
	@staticmethod
	def emptiesredemption_sql(spark, logger, epoch_id, db_settings, emptiesredemption, database):
		return emptiesredemption_sql(spark, logger, epoch_id, db_settings, emptiesredemption, database)
	@staticmethod
	def lineitem_sql(spark, logger, epoch_id, db_settings, lineitem, config, database):
		return lineitem_sql(spark, logger, epoch_id, db_settings, lineitem, config, database)
	@staticmethod
	def pricelookup_sql(spark, logger, epoch_id, db_settings, pricelookup, database):
		return pricelookup_sql(spark, logger, epoch_id, db_settings, pricelookup, database)
	@staticmethod
	def receiptvoiding_sql(spark, logger, epoch_id, db_settings, receiptvoidingtable, database):
		return receiptvoiding_sql(spark, logger, epoch_id, db_settings, receiptvoidingtable, database)
	@staticmethod
	def ruleengine_to_sql(spark, logger, epoch_id, config):
		return ruleengine_to_sql(spark, logger, epoch_id, config)
	@staticmethod
	def suspendedfraud_sql(spark, logger, epoch_id, db_settings, suspendedfraud, database):
		return suspendedfraud_sql(spark, logger, epoch_id, db_settings, suspendedfraud, database)
	@staticmethod
	def transactionheader_sql(spark, logger, epoch_id, db_settings, transactionheader, database):
		return transactionheader_sql(spark, logger, epoch_id, db_settings, transactionheader, database)


# COMMAND ----------

#xml_parsing
class xml:
	@staticmethod
	def article_description(spark, logger, df_article_description):
		return article_description(spark, logger, df_article_description)
	@staticmethod
	def mdarticle_description(spark, logger, df_article_description):
		return mdarticle_description(spark, logger, df_article_description)
	@staticmethod
	def control_transaction_df(logger, loadcontrol, df):
		return control_transaction_df(logger, loadcontrol, df)
	@staticmethod
	def customer_df(logger, loadcontrol, df, config):
		return customer_df(logger, loadcontrol, df, config)
	@staticmethod
	def functionlog_df(logger, loadcontrol, df):
		return functionlog_df(logger, loadcontrol, df)
	@staticmethod
	def lineitem_df(logger, loadcontrol, df):
		return lineitem_df(logger, loadcontrol, df)
	@staticmethod
	def loyaltyreward_df(logger, loadcontrol, df):
		return loyaltyreward_df(logger, loadcontrol, df)
	@staticmethod
	def merchandise_hierarchy_level(spark, logger, df_mhl_edeka, df_mhl_ebus):
		return merchandise_hierarchy_level(spark, logger, df_mhl_edeka, df_mhl_ebus)
	@staticmethod
	def receiptheader_addonlist_df(logger, loadcontrol, df):
		return receiptheader_addonlist_df(logger, loadcontrol, df)
	@staticmethod
	def receiptposition_addonlist_df(logger, loadcontrol, df):
		return receiptposition_addonlist_df(logger, loadcontrol, df)
	@staticmethod
	def retail_pricemodifier_df(logger, loadcontrol, df):
		return retail_pricemodifier_df(logger, loadcontrol, df)
	@staticmethod
	def retail_transactionheader_df(logger, loadcontrol, df):
		return retail_transactionheader_df(logger, loadcontrol, df)
	@staticmethod
	def return_df(logger, loadcontrol, df):
		return return_df(logger, loadcontrol, df)
	@staticmethod
	def sale_df(logger, loadcontrol, df):
		return sale_df(logger, loadcontrol, df)
	@staticmethod
	def salereturn_rebate_df(logger, loadcontrol, df):
		return salereturn_rebate_df(logger, loadcontrol, df)
	@staticmethod
	def tender_control_loan_df(logger, loadcontrol, df):
		return tender_control_loan_df(logger, loadcontrol, df)
	@staticmethod
	def tender_control_pickup_df(logger, loadcontrol, df):
		return tender_control_pickup_df(logger, loadcontrol, df)
	@staticmethod
	def tender_control_safe_settle_df(logger, loadcontrol, df):
		return tender_control_safe_settle_df(logger, loadcontrol, df)
	@staticmethod
	def tender_control_till_settle_df(logger, loadcontrol, df):
		return tender_control_till_settle_df(logger, loadcontrol, df)
	@staticmethod
	def tender_df(logger, loadcontrol, df):
		return tender_df(logger, loadcontrol, df)
	@staticmethod
	def tender_payment_deposit_df(logger, loadcontrol, df, config_dict):
		return tender_payment_deposit_df(logger, loadcontrol, df, config_dict)
	@staticmethod
	def transactionheader_df(logger, loadcontrol, df, config_dict):
		return transactionheader_df(logger, loadcontrol, df, config_dict)

# COMMAND ----------

#masterdata_sql
class md_sql:
	@staticmethod
	def article_description_sql(spark, logger, config, db_settings):
		return article_description_sql(spark, logger, config, db_settings)
	@staticmethod
	def masterdata_to_sql(spark, logger, config):
		return masterdata_to_sql(spark, logger, config)
	@staticmethod
	def merchandise_hierarchy_level_sql(spark, logger, config, db_settings):
		return merchandise_hierarchy_level_sql(spark, logger, config, db_settings)

# COMMAND ----------

#logtables
class log:
	@staticmethod
	def call_logtables_stored_procedure(spark, logger, config):
		return call_logtables_stored_procedure(spark, logger, config)
	@staticmethod
	def daily_load_log(spark, dailyloadlog_df, config):
		return daily_load_log(spark, dailyloadlog_df, config)
	@staticmethod
	def eventlog_di(spark, logging_df, config):
		return eventlog_di(spark, logging_df, config)
	@staticmethod
	def log_tables(spark, config):
		return log_tables(spark, config)
	@staticmethod
	def store_in_daily_loadlog(spark, stoteindailyloadlog_df, config):
		return store_in_daily_loadlog(spark, stoteindailyloadlog_df, config)
	@staticmethod
	def xmlfiles(spark, xmlfiles_df, config):
		return xmlfiles(spark, xmlfiles_df, config)
