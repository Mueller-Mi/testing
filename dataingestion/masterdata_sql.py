# Databricks notebook source
from __future__ import print_function
from pyspark.sql import functions as F
from itertools import chain


def masterdata_to_sql(spark, logger, config):
    db_settings = {
        'sqlserver': "jdbc:sqlserver:{serverip}:{port};database={database}".format(
            serverip=config['output']['db_setting']['server_ip'], port=config['output']['db_setting']['port'],
            database=config['output']['db_setting']['database_name']),
        'username': "{username}".format(username=config['output']['db_setting']['user_name']),
        'password': "{password}".format(password=config['output']['db_setting']['password']),
        'driver': "{driver}".format(driver=config['output']['db_setting']['driver']),
        'prefix': "{prefix}".format(prefix=config['input']['prefix']['prefix_store']),
        'mdmerchandiselevel': "{tablename}".format(tablename=config['output']['db_tables']['mdmerchandiselevel']),
        'mdarticledescription': "{tablename}".format(tablename=config['output']['db_tables']['mdarticledescription'])
    }

    merchandise_hierarchy_level_sql(spark, logger, config, db_settings)
    article_description_sql(spark, logger, config, db_settings)


def merchandise_hierarchy_level_sql(spark, logger, config, db_settings):
    logger.info('Copying Merchandise Hierarchy Level data to DB')

    merchandise_hierarchy_level = spark.sql(
        "select MerchandiseHierarchyLevel, DescriptionDE, IsEBUS from {fraud}.MD_MerchandiseHierarchyLevels".format(
            fraud=config['input']['hive']['database']))

    merchandise_hierarchy_level.write.mode("overwrite").format("jdbc") \
        .option("url", db_settings['sqlserver']) \
        .option("dbtable", db_settings['mdmerchandiselevel']) \
        .option("user", db_settings['username']) \
        .option("password", db_settings['password']) \
        .option("driver", db_settings['driver']) \
        .save()


def article_description_sql(spark, logger, config, db_settings):
    logger.info('Copying Article Description data to DB')

    article_description = spark.sql(
        "select SAP_ID, EAN, Unit, MerchandiseHierarchyLevel_EDEKA, MerchandiseHierarchyLevel_EBUS, Description as "
        "ArticleDescription from {fraud}.MD_ArticleDescription".format(fraud=config['input']['hive']['database']))

    article_description.write.mode("overwrite").format("jdbc") \
        .option("url", db_settings['sqlserver']) \
        .option("dbtable", db_settings['mdarticledescription']) \
        .option("user", db_settings['username']) \
        .option("password", db_settings['password']) \
        .option("driver", db_settings['driver']) \
        .save()
