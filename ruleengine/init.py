# Databricks notebook source
# MAGIC %run ./rule_ingestion1_logger

# COMMAND ----------

# MAGIC %run ./line_item_derived

# COMMAND ----------

# MAGIC %run ./transactionclass

# COMMAND ----------

# MAGIC %run ./pricelookup

# COMMAND ----------

# MAGIC %run ./empties

# COMMAND ----------

# MAGIC %run ./suspended

# COMMAND ----------

# MAGIC %run ./bonstorno

# COMMAND ----------

# MAGIC %run ./daily_report

# COMMAND ----------

# MAGIC %run ./databasesize_increase

# COMMAND ----------

# MAGIC %run ./db_metrics_report

# COMMAND ----------

#rule_ingestion1_logger
class re:
  @staticmethod
  def createsparksession(logger,conf):
    return createsparksession(logger,conf)
  @staticmethod
  def rule_ingest1(spark, logger,epoch_id,sparksession_count_re,config,failedlistappend):
    return rule_ingest1(spark, logger,epoch_id,sparksession_count_re,config,failedlistappend)

# COMMAND ----------

#transactionclass
class trcl:
  @staticmethod
  def create_flags_rules_classification_dataframe(spark,epocid,logger,config):
    return create_flags_rules_classification_dataframe(spark,epocid,logger,config)  

# COMMAND ----------

#pricelookup
class plk:
  @staticmethod
  def create_pricelookup_dataframe(spark,epocid,logger,config):
    return create_pricelookup_dataframe(spark,epocid,logger,config)

# COMMAND ----------

#bonstorno
class bon:
  @staticmethod
  def create_bonstrono_dataframe(spark,epocid,logger,config):
    return create_bonstrono_dataframe(spark,epocid,logger,config)

# COMMAND ----------

#empties
class empt:
  @staticmethod
  def create_repeated_empties_dataframe(spark,epocid,logger,config):
    return create_repeated_empties_dataframe(spark,epocid,logger,config)

# COMMAND ----------

#suspended
class susp:
  @staticmethod
  def create_suspended_fraud_dataframe(spark,epocid,logger,config):
    return create_suspended_fraud_dataframe(spark,epocid,logger,config)

# COMMAND ----------

#line_item_derived
class line:
  @staticmethod
  def create_line_item_derived_dataframe(spark,epocid,logger,config):
    return create_line_item_derived_dataframe(spark,epocid,logger,config)

# COMMAND ----------

#daily_report
class daily:
  @staticmethod
  def create_daily_report(spark,config,logger):
    return create_daily_report(spark,config,logger)

# COMMAND ----------

#db_metrics_report
class metrics:
  @staticmethod
  def create_db_metrics_report(spark,config,logger):
    return create_db_metrics_report(spark,config,logger)

# COMMAND ----------

#databasesize_increase
class dbsize:
  @staticmethod
  def create_db_size_report(spark,config,logger):
    return create_db_size_report(spark,config,logger)
