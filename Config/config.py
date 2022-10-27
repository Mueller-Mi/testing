# Databricks notebook source
#SQL
store = dbutils.widgets.get('store')
if store == 'SB':
    sql_password = dbutils.secrets.getBytes(scope = "dbw-secretScope-dev-SB", key="kv-dbw-dev-user-sb-sql-db-password").decode('UTF-8')
elif store == 'NB':
    sql_password = dbutils.secrets.getBytes(scope = "dbw-secretScope-dev-NB", key="kv-dbw-dev-user-nb-sql-db-password").decode('UTF-8')

# COMMAND ----------

# MAGIC %run ./config_SB

# COMMAND ----------

# MAGIC 
# MAGIC %run ./config_NB

# COMMAND ----------

import json
#store = dbutils.widgets.get('store')

#store = 'SB'
config = {
  'SB': config_SB,
  'NB': config_NB
}
dbutils.notebook.exit(json.dumps(config[store]))

# COMMAND ----------

#dbutils.widgets.get('store')
