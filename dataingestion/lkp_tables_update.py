# Databricks notebook source
# -------------------------------------------------------------#
# importing functions
# -------------------------------------------------------------#
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
import logging
# import yaml


# -------------------------------------------------------------#
# calling db_settings
# -------------------------------------------------------------#
def db_settings_lkptables(spark, logger, config):
    """Creates a db_settings dictionary

    return: db_settings
    """

    logger.info('Creating db settings for Lkp_Tables')

    db_settings = {
        'sqlserver': "jdbc:sqlserver:{serverip}:{port};database={database}".format(
            serverip=config['output']['db_setting']['server_ip'], port=config['output']['db_setting']['port'],
            database=config['output']['db_setting']['database_name']),
        'username': "{username}".format(username=config['output']['db_setting']['user_name']),
        'password': "{password}".format(password=config['output']['db_setting']['password']),
        'driver': "{driver}".format(driver=config['output']['db_setting']['driver']),
        'database': "{database}".format(database=config['input']['hive']['database']),

        'cardtypefolder': "{cardtypefolder}".format(cardtypefolder=config['output']['lookupfolder']['cardtype']),
        'classificationfolder': "{classificationfolder}".format(
            classificationfolder=config['output']['lookupfolder']['classification']),
        'entrymethodfolder': "{entrymethodfolder}".format(
            entrymethodfolder=config['output']['lookupfolder']['entrymethod']),
        'itemtypefolder': "{itemtypefolder}".format(itemtypefolder=config['output']['lookupfolder']['itemtype']),
        'tenderidfolder': "{tenderidfolder}".format(tenderidfolder=config['output']['lookupfolder']['tenderid']),
        'transactionstatusfolder': "{transactionstatusfolder}".format(
            transactionstatusfolder=config['output']['lookupfolder']['transactionstatus']),
        'transactiontypefolder': "{transactiontypefolder}".format(
            transactiontypefolder=config['output']['lookupfolder']['transactiontype']),
        'typecodefolder': "{typecodefolder}".format(typecodefolder=config['output']['lookupfolder']['typecode']),

        'cardtype': "{cardtype}".format(cardtype=config['output']['lkp_tables']['cardtype']),
        'classification': "{classification}".format(classification=config['output']['lkp_tables']['classification']),
        'entrymethod': "{entrymethod}".format(entrymethod=config['output']['lkp_tables']['entrymethod']),
        'itemtype': "{itemtype}".format(itemtype=config['output']['lkp_tables']['itemtype']),
        'tenderid': "{tenderid}".format(tenderid=config['output']['lkp_tables']['tenderid']),
        'transactionstatus': "{transactionstatus}".format(
            transactionstatus=config['output']['lkp_tables']['transactionstatus']),
        'transactiontype': "{transactiontype}".format(
            transactiontype=config['output']['lkp_tables']['transactiontype']),
        'typecode': "{typecode}".format(typecode=config['output']['lkp_tables']['typecode']),

        'cardtype_final': "{cardtype}".format(cardtype=config['output']['lkp_tables_final']['cardtype']),
        'classification_final': "{classification}".format(
            classification=config['output']['lkp_tables_final']['classification']),
        'entrymethod_final': "{entrymethod}".format(entrymethod=config['output']['lkp_tables_final']['entrymethod']),
        'itemtype_final': "{itemtype}".format(itemtype=config['output']['lkp_tables_final']['itemtype']),
        'tenderid_final': "{tenderid}".format(tenderid=config['output']['lkp_tables_final']['tenderid']),
        'transactionstatus_final': "{transactionstatus}".format(
            transactionstatus=config['output']['lkp_tables_final']['transactionstatus']),
        'transactiontype_final': "{transactiontype}".format(
            transactiontype=config['output']['lkp_tables_final']['transactiontype']),
        'typecode_final': "{typecode}".format(typecode=config['output']['lkp_tables_final']['typecode'])
    }

    return db_settings


# -------------------------------------------------------------#
# calling lkp_cardtype
# -------------------------------------------------------------#
def read_lkp_cardtype(spark, logger, config):
    """Creates a lkp_cardtype_df

    return: None
    """

    logger.info('Reading Lkp_CardType DataFrame')

    db_settings = db_settings_lkptables(spark, logger, config)

    cardtype = spark.read.format("jdbc") \
        .option("url", db_settings['sqlserver']) \
        .option("dbtable", db_settings['cardtype_final']) \
        .option("user", db_settings['username']) \
        .option("password", db_settings['password']) \
        .option("driver", db_settings['driver']) \
        .load()

    return cardtype


def update_lkp_cardtype(spark, logger, cardtype_df, config):
    logger.info('Updating Lkp_CardType DataFrame')

    db_settings = db_settings_lkptables(spark, logger, config)
    # databricks
    if config['input']['file_type']['file_type'] == 'parquet':
        cardtype_df.coalesce(1).write.mode('overwrite').parquet(db_settings['cardtypefolder'])
    else:
        cardtype_df.coalesce(1).write.mode('overwrite').format('delta').option('overwriteSchema', 'true').save(
            db_settings['cardtypefolder'])
    spark.sql("""alter table {fraud}.Lkp_CardType set location '{path}'""".format(path=db_settings['cardtypefolder'],
                                                                                  fraud=db_settings['database']))

    cardtype_df.write.mode("overwrite").format("jdbc") \
        .option("url", db_settings['sqlserver']) \
        .option("dbtable", db_settings['cardtype']) \
        .option("user", db_settings['username']) \
        .option("password", db_settings['password']) \
        .option("driver", db_settings['driver']) \
        .save()


# -------------------------------------------------------------#
# calling lkp_typecode
# -------------------------------------------------------------#
def read_lkp_typecode(spark, logger, config):
    logger.info('Reading Lkp_TypeCode DataFrame')

    db_settings = db_settings_lkptables(spark, logger, config)

    typecode = spark.read.format("jdbc") \
        .option("url", db_settings['sqlserver']) \
        .option("dbtable", db_settings['typecode_final']) \
        .option("user", db_settings['username']) \
        .option("password", db_settings['password']) \
        .option("driver", db_settings['driver']) \
        .load()

    return typecode


def update_lkp_typecode(spark, logger, typecode_df, config):
    logger.info('Updating Lkp_TypeCode DataFrame')

    db_settings = db_settings_lkptables(spark, logger, config)
    # databricks
    if config['input']['file_type']['file_type'] == 'parquet':
        typecode_df.coalesce(1).write.mode('overwrite').parquet(db_settings['typecodefolder'])
    else:
        typecode_df.coalesce(1).write.mode('overwrite').format('delta').option('overwriteSchema', 'true').save(
            db_settings['typecodefolder'])
    spark.sql("""alter table {fraud}.Lkp_TypeCode set location '{path}'""".format(path=db_settings['typecodefolder'],
                                                                                  fraud=db_settings['database']))

    typecode_df.write.mode("overwrite").format("jdbc") \
        .option("url", db_settings['sqlserver']) \
        .option("dbtable", db_settings['typecode']) \
        .option("user", db_settings['username']) \
        .option("password", db_settings['password']) \
        .option("driver", db_settings['driver']) \
        .save()


# -------------------------------------------------------------#
# calling lkp_tenderid
# -------------------------------------------------------------#
def read_lkp_tenderid(spark, logger, config):
    logger.info('Reading Lkp_TenderID DataFrame')

    db_settings = db_settings_lkptables(spark, logger, config)

    tenderid = spark.read.format("jdbc") \
        .option("url", db_settings['sqlserver']) \
        .option("dbtable", db_settings['tenderid_final']) \
        .option("user", db_settings['username']) \
        .option("password", db_settings['password']) \
        .option("driver", db_settings['driver']) \
        .load()

    return tenderid


def update_lkp_tenderid(spark, logger, tenderid_df, config):
    logger.info('Updating Lkp_TenderID DataFrame')

    db_settings = db_settings_lkptables(spark, logger, config)

    # databricks
    if config['input']['file_type']['file_type'] == 'parquet':
        tenderid_df.coalesce(1).write.mode('overwrite').parquet(db_settings['tenderidfolder'])
    else:
        tenderid_df.coalesce(1).write.mode('overwrite').format('delta').option('overwriteSchema', 'true').save(
            db_settings['tenderidfolder'])
    spark.sql("""alter table {fraud}.Lkp_TenderID set location '{path}'""".format(path=db_settings['tenderidfolder'],
                                                                                  fraud=db_settings['database']))

    tenderid_df.write.mode("overwrite").format("jdbc") \
        .option("url", db_settings['sqlserver']) \
        .option("dbtable", db_settings['tenderid']) \
        .option("user", db_settings['username']) \
        .option("password", db_settings['password']) \
        .option("driver", db_settings['driver']) \
        .save()


# -------------------------------------------------------------#
# calling lkp_itemtype
# -------------------------------------------------------------#
def read_lkp_itemtype(spark, logger, config):
    logger.info('Reading Lkp_ItemType DataFrame')

    db_settings = db_settings_lkptables(spark, logger, config)

    itemtype = spark.read.format("jdbc") \
        .option("url", db_settings['sqlserver']) \
        .option("dbtable", db_settings['itemtype_final']) \
        .option("user", db_settings['username']) \
        .option("password", db_settings['password']) \
        .option("driver", db_settings['driver']) \
        .load()

    return itemtype


def update_lkp_itemtype(spark, logger, itemtype_df, config):
    logger.info('UpdatingLkp_ItemType DataFrame')

    db_settings = db_settings_lkptables(spark, logger, config)

    # databricks
    if config['input']['file_type']['file_type'] == 'parquet':
        itemtype_df.coalesce(1).write.mode('overwrite').parquet(db_settings['itemtypefolder'])
    else:
        itemtype_df.coalesce(1).write.mode('overwrite').format('delta').option('overwriteSchema', 'true').save(
            db_settings['itemtypefolder'])

    spark.sql("""alter table {fraud}.Lkp_ItemType set location '{path}'""".format(path=db_settings['itemtypefolder'],
                                                                                  fraud=db_settings['database']))

    itemtype_df.write.mode("overwrite").format("jdbc") \
        .option("url", db_settings['sqlserver']) \
        .option("dbtable", db_settings['itemtype']) \
        .option("user", db_settings['username']) \
        .option("password", db_settings['password']) \
        .option("driver", db_settings['driver']) \
        .save()


# -------------------------------------------------------------#
# calling lkp_entrymethod
# -------------------------------------------------------------#
def read_lkp_entrymethod(spark, logger, config):
    logger.info('Reading Lkp_EntryMethod DataFrame')

    db_settings = db_settings_lkptables(spark, logger, config)

    entrymethod = spark.read.format("jdbc") \
        .option("url", db_settings['sqlserver']) \
        .option("dbtable", db_settings['entrymethod_final']) \
        .option("user", db_settings['username']) \
        .option("password", db_settings['password']) \
        .option("driver", db_settings['driver']) \
        .load()

    return entrymethod


def update_lkp_entrymethod(spark, logger, entrymethod_df, config):
    logger.info('Updating Lkp_EntryMethod DataFrame')

    db_settings = db_settings_lkptables(spark, logger, config)
    # databricks
    if config['input']['file_type']['file_type'] == 'parquet':
        entrymethod_df.coalesce(1).write.mode('overwrite').parquet(db_settings['entrymethodfolder'])
    else:
        entrymethod_df.coalesce(1).write.mode('overwrite').format('delta').option('overwriteSchema', 'true').save(
            db_settings['entrymethodfolder'])

    spark.sql(
        """alter table {fraud}.Lkp_EntryMethod set location '{path}'""".format(path=db_settings['entrymethodfolder'],
                                                                               fraud=db_settings['database']))

    entrymethod_df.write.mode("overwrite").format("jdbc") \
        .option("url", db_settings['sqlserver']) \
        .option("dbtable", db_settings['entrymethod']) \
        .option("user", db_settings['username']) \
        .option("password", db_settings['password']) \
        .option("driver", db_settings['driver']) \
        .save()
