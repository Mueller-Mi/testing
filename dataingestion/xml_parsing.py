# Databricks notebook source
# -*- coding: utf-8 -*-
# -------------------------------------------------------------#
# Importing functions
# -------------------------------------------------------------#
from __future__ import print_function
from pyspark.sql import functions as F
from pyspark.sql.types import *


# -------------------------------------------------------------#
# Creating TransactionHeaderDF DataFrame
# -------------------------------------------------------------#

def transactionheader_df(logger, loadcontrol, df, config_dict):
    logger.info("Creating TransactionHeaderDF")

    TransactionHeaderDF = df.select('RetailStoreID', 'WorkstationID', 'SequenceNumber',
                                    'GK_ReceiptDateTime',
                                    'GK_ReceiptNumber', 'BeginDateTime', 'EndDateTime',
                                    'Date',
                                    '_CancelFlag', '_FixVersion',
                                    '_MajorVersion', '_MinorVersion',
                                    '_OfflineFlag', '_TrainingModeFlag',
                                    'TillID', 'OperatorID__EmployeeID',
                                    'CurrencyCode',
                                    'GK_LocationID',
                                    'GK_ReceiptReturnedFlag',
                                    'GK_OperatorBypassApproval_SequenceNumber',
                                    'GK_OperatorBypassApproval_ApproverID__EmployeeID',
                                    'GK_FunctionLog_GK_StoreInfoID',
                                    'Status',
                                    'InputFileName',
                                    F.when(F.col('ControlTransaction__Version').isNotNull(), 'ControlTransaction')
                                    .when(F.col('GK_FunctionLog_GK_StoreInfoID').isNotNull(), 'FunctionLog')
                                    .when(F.col('RetailTransaction__Version').isNotNull(), 'RetailTransaction')
                                    .when(F.col('TenderControlTransaction__Version').isNotNull(),
                                          'TenderControlTransaction')
                                    .alias('TransactionType'))

    TransactionHeaderDF = TransactionHeaderDF.withColumnRenamed('GK_ReceiptDateTime', 'ReceiptDateTime')
    TransactionHeaderDF = TransactionHeaderDF.withColumnRenamed('GK_ReceiptNumber', 'ReceiptNumber')
    TransactionHeaderDF = TransactionHeaderDF.withColumnRenamed('_CancelFlag', 'CancelFlag')
    TransactionHeaderDF = TransactionHeaderDF.withColumnRenamed('_FixVersion', 'FixVersion')
    TransactionHeaderDF = TransactionHeaderDF.withColumnRenamed('_MajorVersion', 'MajorVersion')
    TransactionHeaderDF = TransactionHeaderDF.withColumnRenamed('_MinorVersion', 'MinorVersion')
    TransactionHeaderDF = TransactionHeaderDF.withColumnRenamed('_OfflineFlag', 'OfflineFlag')
    TransactionHeaderDF = TransactionHeaderDF.withColumnRenamed('_TrainingModeFlag', 'TrainingModeFlag')
    TransactionHeaderDF = TransactionHeaderDF.withColumnRenamed('OperatorID__EmployeeID', 'EmployeeID')
    TransactionHeaderDF = TransactionHeaderDF.withColumnRenamed('GK_LocationID', 'LocationID')
    TransactionHeaderDF = TransactionHeaderDF.withColumnRenamed('GK_ReceiptReturnedFlag', 'ReceiptReturnedFlag')
    TransactionHeaderDF = TransactionHeaderDF.withColumnRenamed('GK_OperatorBypassApproval_SequenceNumber',
                                                                'OperatorBypassApprovalSequenceNumber')
    TransactionHeaderDF = TransactionHeaderDF.withColumnRenamed('GK_OperatorBypassApproval_ApproverID__EmployeeID',
                                                                'ApproverEmployeeID')
    TransactionHeaderDF = TransactionHeaderDF.withColumnRenamed('GK_FunctionLog_GK_StoreInfoID',
                                                                'FunctionLogStoreInfoID')
    TransactionHeaderDF = TransactionHeaderDF.withColumnRenamed('InputFileName', 'xmlfilename')

    TransactionHeaderDF = TransactionHeaderDF.withColumn('xmlfilename',
                                                         F.regexp_extract("xmlfilename",
                                                                          "[0-9]{4}\.(\w+)\.[a-zA-Z0-9]+\.+[0-9]{5}.[a-zA-Z].+",
                                                                          0))
    TransactionHeaderDF = TransactionHeaderDF.withColumn('BusinessDay', F.split('ReceiptDateTime', 'T')[0])

    TransactionHeaderDF = TransactionHeaderDF.filter(F.col("retailstoreid").isNotNull())
    TransactionHeaderDF = TransactionHeaderDF.filter(F.col("WorkstationID").isNotNull())
    TransactionHeaderDF = TransactionHeaderDF.filter(F.col("SequenceNumber").isNotNull())
    TransactionHeaderDF = TransactionHeaderDF.filter(F.col("ReceiptDateTime").isNotNull())
    TransactionHeaderDF = TransactionHeaderDF.filter(F.col("TransactionType").isNotNull())

    TransactionHeaderDF = TransactionHeaderDF.withColumn('RetailStoreID', F.col('RetailStoreID').cast('long'))
    TransactionHeaderDF = TransactionHeaderDF.withColumn('WorkstationID', F.col('WorkstationID').cast('long'))
    TransactionHeaderDF = TransactionHeaderDF.withColumn('SequenceNumber', F.col('SequenceNumber').cast('long'))
    TransactionHeaderDF = TransactionHeaderDF.withColumn('ReceiptDateTime', F.col('ReceiptDateTime').cast('string'))
    TransactionHeaderDF = TransactionHeaderDF.withColumn('ReceiptNumber', F.col('ReceiptNumber').cast('long'))
    TransactionHeaderDF = TransactionHeaderDF.withColumn('BeginDateTime', F.col('BeginDateTime').cast('string'))
    TransactionHeaderDF = TransactionHeaderDF.withColumn('EndDateTime', F.col('EndDateTime').cast('string'))
    TransactionHeaderDF = TransactionHeaderDF.withColumn('CancelFlag', F.col('CancelFlag').cast('boolean'))
    TransactionHeaderDF = TransactionHeaderDF.withColumn('FixVersion', F.col('FixVersion').cast('long'))
    TransactionHeaderDF = TransactionHeaderDF.withColumn('MajorVersion', F.col('MajorVersion').cast('long'))
    TransactionHeaderDF = TransactionHeaderDF.withColumn('MinorVersion', F.col('MinorVersion').cast('long'))
    TransactionHeaderDF = TransactionHeaderDF.withColumn('OfflineFlag', F.col('OfflineFlag').cast('boolean'))
    TransactionHeaderDF = TransactionHeaderDF.withColumn('TrainingModeFlag', F.col('TrainingModeFlag').cast('boolean'))
    TransactionHeaderDF = TransactionHeaderDF.withColumn('TillID', F.col('TillID').cast('long'))
    TransactionHeaderDF = TransactionHeaderDF.withColumn('EmployeeID', F.col('EmployeeID').cast('long'))
    TransactionHeaderDF = TransactionHeaderDF.withColumn('LocationID', F.col('LocationID').cast('string'))
    TransactionHeaderDF = TransactionHeaderDF.withColumn('ReceiptReturnedFlag',
                                                         F.col('ReceiptReturnedFlag').cast('boolean'))
    TransactionHeaderDF = TransactionHeaderDF.withColumn('OperatorBypassApprovalSequenceNumber',
                                                         F.col('OperatorBypassApprovalSequenceNumber').cast('long'))
    TransactionHeaderDF = TransactionHeaderDF.withColumn('ApproverEmployeeID', F.col('ApproverEmployeeID').cast('long'))
    TransactionHeaderDF = TransactionHeaderDF.withColumn('LocationID', F.col('LocationID').cast('long'))
    TransactionHeaderDF = TransactionHeaderDF.withColumn('FunctionLogStoreInfoID',
                                                         F.col('FunctionLogStoreInfoID').cast('long'))

    TransactionHeaderDF = TransactionHeaderDF.dropDuplicates(
        ['RetailStoreID', 'WorkstationID', 'SequenceNumber', 'ReceiptDateTime', 'ReceiptNumber', 'TransactionType',
         'FunctionLogStoreInfoID'])

    # TransactionHeaderDF = TransactionHeaderDF.withColumn('Date', F.split('ReceiptDateTime', 'T')[0])

    if config_dict['employeeid_encryption'] == True:
        TransactionHeaderDF = TransactionHeaderDF.withColumn('EmployeeID', TransactionHeaderDF['EmployeeID'].bitwiseXOR(
            config_dict['encryption_key']))
    TransactionHeaderDF.select('RetailStoreID').distinct().show()
    return TransactionHeaderDF


# -------------------------------------------------------------#
# Creating ControlTransactionDF
# -------------------------------------------------------------#

def control_transaction_df(logger, loadcontrol, df):
    """Transform original xml data
    
    Parameters
    ----------
    arg1 : Input DataFrame read in readDatatodf()
    
    Returns
    -------
    Transformed DataFrame
    
    """

    logger.info("Creating ContronTransactionDF")

    ControlTransactionDF = df.filter(F.col("ControlTransaction__Version").isNotNull())

    ControlTransactionDF = ControlTransactionDF.select('RetailStoreID', 'WorkStationID', 'SequenceNumber',
                                                       'GK_ReceiptDateTime', 'GK_ReceiptNumber',
                                                       'ControlTransaction_OperatorSignOn_StartDateTimestamp',
                                                       'ControlTransaction_OperatorSignOff_StartDateTimestamp',
                                                       'ControlTransaction_OperatorSignOff_GK_ForcedSignOffFlag',
                                                       'ControlTransaction_NoSale', 'ControlTransaction_ReasonCode',
                                                       'ControlTransaction_POSEOD_StartDateTimestamp')

    ControlTransactionDF = ControlTransactionDF.withColumnRenamed(
        'Controltransaction_OperatorSignOn_StartDateTimestamp', 'SignOnStartDateTimestamp')
    ControlTransactionDF = ControlTransactionDF.withColumnRenamed(
        'Controltransaction_OperatorSignOff_StartDateTimestamp', 'SignOffStartDateTimestamp')
    ControlTransactionDF = ControlTransactionDF.withColumnRenamed(
        'Controltransaction_OperatorSignOff_GK_ForcedSignOffFlag', 'ForcedSignOffFlag')
    ControlTransactionDF = ControlTransactionDF.withColumnRenamed('Controltransaction_NoSale', 'NoSale')
    ControlTransactionDF = ControlTransactionDF.withColumnRenamed('Controltransaction_ReasonCode', 'ReasonCode')
    ControlTransactionDF = ControlTransactionDF.withColumnRenamed('ControlTransaction_POSEOD_StartDateTimestamp',
                                                                  'StartDateTimestamp')
    ControlTransactionDF = ControlTransactionDF.withColumnRenamed('GK_ReceiptDateTime', 'ReceiptDateTime')
    ControlTransactionDF = ControlTransactionDF.withColumnRenamed('GK_ReceiptNumber', 'ReceiptNumber')

    ControlTransactionDF = ControlTransactionDF.withColumn('ControlTransactionType',
                                                           F.when(F.col("SignOnStartDateTimestamp").isNotNull(),
                                                                  'OperatorSignOn')
                                                           .when(F.col("SignOffStartDateTimestamp").isNotNull(),
                                                                 'OperatorSignOff')
                                                           .when(F.col("NoSale").isNotNull(), 'NoSale')
                                                           .when(F.col("StartDateTimestamp").isNotNull(),
                                                                 'SessionSettle')
                                                           .otherwise('SystemProgram'))

    ControlTransactionDF = ControlTransactionDF.dropDuplicates(
        ["RetailStoreID", "WorkstationID", "SequenceNumber", "ReceiptDateTime",
         "ReceiptNumber"])

    ControlTransactionDF = ControlTransactionDF.withColumn('RetailStoreID', F.col('RetailStoreID').cast('long'))
    ControlTransactionDF = ControlTransactionDF.withColumn('WorkstationID', F.col('WorkstationID').cast('long'))
    ControlTransactionDF = ControlTransactionDF.withColumn('SequenceNumber', F.col('SequenceNumber').cast('long'))
    ControlTransactionDF = ControlTransactionDF.withColumn('ReceiptDateTime', F.col('ReceiptDateTime').cast('string'))
    ControlTransactionDF = ControlTransactionDF.withColumn('ReceiptNumber', F.col('ReceiptNumber').cast('long'))
    ControlTransactionDF = ControlTransactionDF.withColumn('SignOnStartDateTimestamp',
                                                           F.col('SignOnStartDateTimestamp').cast('string'))
    ControlTransactionDF = ControlTransactionDF.withColumn('SignOffStartDateTimestamp',
                                                           F.col('SignOffStartDateTimestamp').cast('string'))
    ControlTransactionDF = ControlTransactionDF.withColumn('ForcedSignOffFlag',
                                                           F.col('ForcedSignOffFlag').cast('boolean'))
    ControlTransactionDF = ControlTransactionDF.withColumn('StartDateTimestamp',
                                                           F.col('StartDateTimestamp').cast('string'))

    ControlTransactionDF = ControlTransactionDF.withColumn('Date', F.split('ReceiptDateTime', 'T')[0])

    # ControlTransactionDF = loadcontrol.join(ControlTransactionDF, ["RetailStoreID", "Date"])

    # ControlTransactionDF = ControlTransactionDF.filter((ControlTransactionDF.Status == 'A') | (ControlTransactionDF.Status == 'L') | (ControlTransactionDF.Status =='E'))

    return ControlTransactionDF


# -------------------------------------------------------------#
# Creating FunctionLogDF
# -------------------------------------------------------------#

def functionlog_df(logger, loadcontrol, df):
    logger.info("Creating FunctionLogDF")

    FunctionlogDF = df.select('RetailStoreID', 'WorkStationID', 'SequenceNumber', 'GK_ReceiptDateTime',
                              'GK_ReceiptNumber', 'GK_FunctionLog_GK_StoreInfoID', 'GK_FunctionLog_GK_FunctionID',
                              'GK_FunctionLog_GK_FunctionName',
                              'GK_FunctionLog_GK_ParameterList_GK_Parameter_GK_ParameterName',
                              'GK_FunctionLog_GK_ParameterList_GK_Parameter_GK_ParameterValue')

    FunctionlogDF = FunctionlogDF.withColumnRenamed('GK_ReceiptDateTime', 'ReceiptDateTime')
    FunctionlogDF = FunctionlogDF.withColumnRenamed('GK_ReceiptNumber', 'ReceiptNumber')
    FunctionlogDF = FunctionlogDF.withColumnRenamed('GK_FunctionLog_GK_StoreInfoID', 'StoreInfoID')
    FunctionlogDF = FunctionlogDF.withColumnRenamed('GK_FunctionLog_GK_FunctionID', 'FunctionID')
    FunctionlogDF = FunctionlogDF.withColumnRenamed('GK_FunctionLog_GK_FunctionName', 'FunctionName')
    FunctionlogDF = FunctionlogDF.withColumnRenamed('GK_FunctionLog_GK_ParameterList_GK_Parameter_GK_ParameterName',
                                                    'ParameterName')
    FunctionlogDF = FunctionlogDF.withColumnRenamed('GK_FunctionLog_GK_ParameterList_GK_Parameter_GK_ParameterValue',
                                                    'ParameterValue')
    
    FunctionlogDF = (FunctionlogDF.withColumn("tmp", F.arrays_zip('ParameterName', 'ParameterValue'))
                     .withColumn("tmp", F.explode("tmp"))
                     .select('RetailStoreID', 'WorkstationID', 'SequenceNumber', 'ReceiptDateTime', 'ReceiptNumber',
                             'StoreInfoID', 'FunctionID', 'FunctionName', 'tmp.ParameterName', 'tmp.ParameterValue'))
    FunctionlogDF = FunctionlogDF.filter(F.col('StoreInfoID').isNotNull())
    
    FunctionlogDF = FunctionlogDF.dropDuplicates()
    FunctionlogDF = FunctionlogDF.withColumn('RetailStoreID', F.col('RetailStoreID').cast('long'))
    FunctionlogDF = FunctionlogDF.withColumn('WorkstationID', F.col('WorkstationID').cast('long'))
    FunctionlogDF = FunctionlogDF.withColumn('SequenceNumber', F.col('SequenceNumber').cast('long'))
    FunctionlogDF = FunctionlogDF.withColumn('ReceiptDateTime', F.col('ReceiptDateTime').cast('string'))
    FunctionlogDF = FunctionlogDF.withColumn('ReceiptNumber', F.col('ReceiptNumber').cast('long'))
    FunctionlogDF = FunctionlogDF.withColumn('StoreInfoID', F.col('StoreInfoID').cast('long'))
    FunctionlogDF = FunctionlogDF.withColumn('FunctionID', F.col('FunctionID').cast('long'))

    FunctionlogDF = FunctionlogDF.withColumn('Date', F.split('ReceiptDateTime', 'T')[0])

    # FunctionlogDF = loadcontrol.join(FunctionlogDF, ["RetailStoreID", "Date"])

    # FunctionlogDF = FunctionlogDF.filter((FunctionlogDF.Status == 'A') | (FunctionlogDF.Status == 'L') | (FunctionlogDF.Status =='E'))

    return FunctionlogDF


# -------------------------------------------------------------#
# Creating LineitemDF DataFrame
# -------------------------------------------------------------#

def lineitem_df(logger, loadcontrol, df):
    logger.info("Creating LineItemDF")

    LineItemDF = df.select('RetailStoreID', 'WorkStationID', 'SequenceNumber', 'GK_ReceiptDateTime', 'GK_ReceiptNumber',
                           'RetailTransaction_LineItem_SequenceNumber',
                           'RetailTransaction_LineItem__EntryMethod', 'RetailTransaction_LineItem__VoidFlag',
                           'RetailTransaction_LineItem_BeginDateTime', 'RetailTransaction_LineItem_Tax__TaxSubType',
                           'RetailTransaction_LineItem_Tax__TaxType',
                           'RetailTransaction_LineItem_Tax_TaxableAmount__TaxIncludedInTaxableAmountFlag',
                           'RetailTransaction_LineItem_Tax_TaxableAmount__VALUE',
                           'RetailTransaction_LineItem_Tax_Amount', 'RetailTransaction_LineItem_Tax_Percent',
                           'RetailTransaction_LineItem_Tax_GK_TaxGroupID',
                           'RetailTransaction_LineItem_GK_DirectVoidFlag',
                           'RetailTransaction_LineItem_Tax_GK_NegativeAmountFlag')

    LineItemDF = LineItemDF.withColumnRenamed('RetailTransaction_LineItem_SequenceNumber', 'LineItemSequenceNumber')
    LineItemDF = LineItemDF.withColumnRenamed('RetailTransaction_LineItem__EntryMethod', 'EntryMethod')
    LineItemDF = LineItemDF.withColumnRenamed('RetailTransaction_LineItem__VoidFlag', 'VoidFlag')
    LineItemDF = LineItemDF.withColumnRenamed('RetailTransaction_LineItem_BeginDateTime', 'LineItemBeginDateTime')
    LineItemDF = LineItemDF.withColumnRenamed('RetailTransaction_LineItem_Tax__TaxSubType', 'TaxSubType')
    LineItemDF = LineItemDF.withColumnRenamed(
        'RetailTransaction_LineItem_Tax_TaxableAmount__TaxIncludedInTaxableAmountFlag',
        'TaxIncludedInTaxableAmountFlag')
    LineItemDF = LineItemDF.withColumnRenamed('RetailTransaction_LineItem_Tax_TaxableAmount__Value', 'TaxableAmount')
    LineItemDF = LineItemDF.withColumnRenamed('RetailTransaction_LineItem_Tax_Amount', 'Amount')
    LineItemDF = LineItemDF.withColumnRenamed('RetailTransaction_LineItem_Tax_Percent', 'TaxPercent')
    LineItemDF = LineItemDF.withColumnRenamed('RetailTransaction_LineItem_Tax_GK_TaxGroupID', 'TaxGroupID')
    LineItemDF = LineItemDF.withColumnRenamed('RetailTransaction_LineItem_GK_DirectVoidFlag', 'DirectVoidFlag')
    LineItemDF = LineItemDF.withColumnRenamed('RetailTransaction_LineItem_Tax_GK_NegativeAmountFlag',
                                              'NegativeAmountFlag')
    LineItemDF = LineItemDF.withColumnRenamed('RetailTransaction_LineItem_Tax__TaxType', 'TaxType')
    LineItemDF = LineItemDF.withColumnRenamed('GK_ReceiptDateTime', 'ReceiptDateTime')
    LineItemDF = LineItemDF.withColumnRenamed('GK_ReceiptNumber', 'ReceiptNumber')

    LineItemDF = (LineItemDF.withColumn("tmp",
                                        F.arrays_zip('LineItemSequenceNumber', 'EntryMethod', 'VoidFlag', 'TaxSubType',
                                                     'TaxIncludedInTaxableAmountFlag', 'TaxableAmount', 'Amount',
                                                     'TaxPercent', 'TaxGroupID', 'NegativeAmountFlag', 'DirectVoidFlag',
                                                     'TaxType', 'LineItemBeginDateTime')).withColumn("tmp",
                                                                                                     F.explode("tmp"))
                  .select('RetailStoreID', 'WorkstationID', 'SequenceNumber', 'ReceiptDateTime', 'ReceiptNumber',
                          F.col('tmp.LineItemSequenceNumber'), F.col('tmp.EntryMethod'), F.col('tmp.VoidFlag'),
                          F.col('tmp.TaxSubType'), F.col('tmp.TaxIncludedInTaxableAmountFlag'),
                          F.col('tmp.TaxableAmount'), F.col('tmp.Amount'), F.col('tmp.TaxPercent'),
                          F.col('tmp.TaxGroupID'), F.col('tmp.NegativeAmountFlag'), F.col('tmp.DirectVoidFlag'),
                          F.col('tmp.TaxType'), F.col('tmp.LineItemBeginDateTime')))

    LineItemDF = LineItemDF.dropDuplicates(
        ["RetailStoreID", "WorkstationID", "SequenceNumber", "ReceiptDateTime", "ReceiptNumber",
         "LineItemSequenceNumber"])

    LineItemDF = LineItemDF.withColumn('RetailStoreID', F.col('RetailStoreID').cast('long'))
    LineItemDF = LineItemDF.withColumn('WorkstationID', F.col('WorkstationID').cast('long'))
    LineItemDF = LineItemDF.withColumn('SequenceNumber', F.col('SequenceNumber').cast('long'))
    LineItemDF = LineItemDF.withColumn('ReceiptDateTime', F.col('ReceiptDateTime').cast('string'))
    LineItemDF = LineItemDF.withColumn('ReceiptNumber', F.col('ReceiptNumber').cast('long'))
    LineItemDF = LineItemDF.withColumn('LineItemSequenceNumber', F.col('LineItemSequenceNumber').cast('long'))
    LineItemDF = LineItemDF.withColumn('EntryMethod', F.col('EntryMethod').cast('string'))
    LineItemDF = LineItemDF.withColumn('VoidFlag', F.col('VoidFlag').cast('boolean'))
    LineItemDF = LineItemDF.withColumn('TaxIncludedInTaxableAmountFlag',
                                       F.col('TaxIncludedInTaxableAmountFlag').cast('boolean'))
    LineItemDF = LineItemDF.withColumn('TaxableAmount', F.col('TaxableAmount').cast('double'))
    LineItemDF = LineItemDF.withColumn('Amount', F.col('Amount').cast('double'))
    LineItemDF = LineItemDF.withColumn('TaxPercent', F.col('TaxPercent').cast('double'))
    LineItemDF = LineItemDF.withColumn('TaxGroupID', F.col('TaxGroupID').cast('long'))
    LineItemDF = LineItemDF.withColumn('NegativeAmountFlag', F.col('NegativeAmountFlag').cast('boolean'))
    LineItemDF = LineItemDF.withColumn('DirectVoidFlag', F.col('DirectVoidFlag').cast('boolean'))
    LineItemDF = LineItemDF.withColumn('LineItemBeginDateTime', F.col('LineItemBeginDateTime').cast('string'))

    LineItemDF = LineItemDF.withColumn('Date', F.split('ReceiptDateTime', 'T')[0])

    # LineItemDF = loadcontrol.join(LineItemDF, ["RetailStoreID", "Date"])

    # LineItemDF = LineItemDF.filter((LineItemDF.Status == 'A') | (LineItemDF.Status == 'L') | (LineItemDF.Status =='E'))

    return LineItemDF


# -------------------------------------------------------------#
# Creating LoyaltyrewardDF DataFrame
# -------------------------------------------------------------#

def loyaltyreward_df(logger, loadcontrol, df):
    logger.info("Creating LoyaltyRewardDF")

    LoyaltyRewardDF = df.select('RetailStoreID', 'WorkStationID', 'SequenceNumber', 'GK_ReceiptDateTime',
                                'GK_ReceiptNumber', 'RetailTransaction_LineItem_SequenceNumber',
                                'RetailTransaction_LineItem_LoyaltyReward_PromotionID',
                                'RetailTransaction_LineItem_LoyaltyReward_ReasonCode',
                                'RetailTransaction_LineItem_LoyaltyReward_GK_PointsAwarded',
                                'RetailTransaction_LineItem_LoyaltyReward_GK_PriceDerivationRule_PriceDerivationRuleID',
                                'RetailTransaction_LineItem_LoyaltyReward_GK_PriceDerivationRule_Eligibility__Type',
                                'RetailTransaction_LineItem_LoyaltyReward_GK_PriceDerivationRule_Eligibility_ReferenceID',
                                'RetailTransaction_LineItem_LoyaltyReward_GK_PriceDerivationRule_Amount',
                                'RetailTransaction_LineItem_LoyaltyReward_GK_PriceDerivationRule_GK_Origin',
                                'RetailTransaction_LineItem_LoyaltyReward_GK_PriceDerivationRule_GK_AppliedQuantity',
                                'RetailTransaction_LineItem_LoyaltyReward_GK_PriceDerivationRule_GK_RuleDescription',
                                'RetailTransaction_LineItem_LoyaltyReward_GK_ItemLink', )

    LoyaltyRewardDF = LoyaltyRewardDF.withColumnRenamed('GK_ReceiptDateTime', 'ReceiptDateTime')
    LoyaltyRewardDF = LoyaltyRewardDF.withColumnRenamed('GK_ReceiptNumber', 'ReceiptNumber')
    LoyaltyRewardDF = LoyaltyRewardDF.withColumnRenamed('RetailTransaction_LineItem_SequenceNumber',
                                                        'LineItemSequenceNumber')
    LoyaltyRewardDF = LoyaltyRewardDF.withColumnRenamed('RetailTransaction_LineItem_LoyaltyReward_PromotionID',
                                                        'PromotionID')
    LoyaltyRewardDF = LoyaltyRewardDF.withColumnRenamed('RetailTransaction_LineItem_LoyaltyReward_ReasonCode',
                                                        'ReasonCode')
    LoyaltyRewardDF = LoyaltyRewardDF.withColumnRenamed('RetailTransaction_LineItem_LoyaltyReward_GK_PointsAwarded',
                                                        'PointsAwarded')
    LoyaltyRewardDF = LoyaltyRewardDF.withColumnRenamed(
        'RetailTransaction_LineItem_LoyaltyReward_GK_PriceDerivationRule_PriceDerivationRuleID',
        'PriceDerivationRuleID')
    LoyaltyRewardDF = LoyaltyRewardDF.withColumnRenamed(
        'RetailTransaction_LineItem_LoyaltyReward_GK_PriceDerivationRule_Eligibility_Type', 'EligibilityType')
    LoyaltyRewardDF = LoyaltyRewardDF.withColumnRenamed(
        'RetailTransaction_LineItem_LoyaltyReward_GK_PriceDerivationRule_Eligibility_ReferenceID', 'ReferenceID')
    LoyaltyRewardDF = LoyaltyRewardDF.withColumnRenamed(
        'RetailTransaction_LineItem_LoyaltyReward_GK_PriceDerivationRule_Amount', 'Amount')
    LoyaltyRewardDF = LoyaltyRewardDF.withColumnRenamed(
        'RetailTransaction_LineItem_LoyaltyReward_GK_PriceDerivationRule_GK_Origin', 'Origin')
    LoyaltyRewardDF = LoyaltyRewardDF.withColumnRenamed(
        'RetailTransaction_LineItem_LoyaltyReward_GK_PriceDerivationRule_GK_AppliedQuantity', 'AppliedQuantity')
    LoyaltyRewardDF = LoyaltyRewardDF.withColumnRenamed(
        'RetailTransaction_LineItem_LoyaltyReward_GK_PriceDerivationRule_GK_RuleDescription', 'RuleDescription')
    LoyaltyRewardDF = LoyaltyRewardDF.withColumnRenamed('RetailTransaction_LineItem_LoyaltyReward_GK_ItemLink',
                                                        'ItemLink')
    LoyaltyRewardDF = LoyaltyRewardDF.withColumnRenamed(
        'RetailTransaction_LineItem_LoyaltyReward_GK_PriceDerivationRule_Eligibility__Type', 'EligibilityType')

    LoyaltyRewardDF = (LoyaltyRewardDF
                       .withColumn("tmp",
                                   F.arrays_zip('LineItemSequenceNumber', 'PromotionID', 'ReasonCode', 'PointsAwarded',
                                                'PriceDerivationRuleID', 'EligibilityType', 'ReferenceID', 'Amount',
                                                'Origin', 'AppliedQuantity', 'RuleDescription', 'ItemLink'))
                       .withColumn("tmp", F.explode("tmp"))
                       .select('RetailStoreID', 'WorkstationID', 'SequenceNumber', 'ReceiptDateTime', 'ReceiptNumber',
                               F.col('tmp.LineItemSequenceNumber'), F.col('tmp.PromotionID'), F.col('tmp.ReasonCode'),
                               F.col('tmp.Origin'), F.col('tmp.AppliedQuantity'), F.col('tmp.RuleDescription'),
                               F.col('tmp.ReferenceID'), F.col('tmp.ItemLink'), F.col('tmp.PointsAwarded'),
                               F.col('tmp.PriceDerivationRuleID'), F.col('tmp.EligibilityType'), F.col('tmp.Amount')))

    LoyaltyRewardDF = LoyaltyRewardDF.dropDuplicates(
        ["RetailStoreID", "WorkstationID", "SequenceNumber", "ReceiptDateTime", "ReceiptNumber",
         "LineItemSequenceNumber"])

    LoyaltyRewardDF = LoyaltyRewardDF.filter(F.col('PointsAwarded').isNotNull())

    LoyaltyRewardDF = LoyaltyRewardDF.withColumn('RetailStoreID', F.col('RetailStoreID').cast('long'))
    LoyaltyRewardDF = LoyaltyRewardDF.withColumn('WorkstationID', F.col('WorkstationID').cast('long'))
    LoyaltyRewardDF = LoyaltyRewardDF.withColumn('SequenceNumber', F.col('SequenceNumber').cast('long'))
    LoyaltyRewardDF = LoyaltyRewardDF.withColumn('ReceiptDateTime', F.col('ReceiptDateTime').cast('string'))
    LoyaltyRewardDF = LoyaltyRewardDF.withColumn('ReceiptNumber', F.col('ReceiptNumber').cast('long'))
    LoyaltyRewardDF = LoyaltyRewardDF.withColumn('LineItemSequenceNumber', F.col('LineItemSequenceNumber').cast('long'))
    LoyaltyRewardDF = LoyaltyRewardDF.withColumn('ReasonCode', F.col('ReasonCode').cast('long'))
    LoyaltyRewardDF = LoyaltyRewardDF.withColumn('Origin', F.col('Origin').cast('long'))
    LoyaltyRewardDF = LoyaltyRewardDF.withColumn('AppliedQuantity', F.col('AppliedQuantity').cast('double'))
    LoyaltyRewardDF = LoyaltyRewardDF.withColumn('ReferenceID', F.col('ReferenceID').cast('long'))
    LoyaltyRewardDF = LoyaltyRewardDF.withColumn('ItemLink', F.col('ItemLink').cast('long'))
    LoyaltyRewardDF = LoyaltyRewardDF.withColumn('PointsAwarded', F.col('PointsAwarded').cast('double'))
    LoyaltyRewardDF = LoyaltyRewardDF.withColumn('Amount', F.col('Amount').cast('double'))

    LoyaltyRewardDF = LoyaltyRewardDF.withColumn('Date', F.split('ReceiptDateTime', 'T')[0])

    # LoyaltyRewardDF = loadcontrol.join(LoyaltyRewardDF, ["RetailStoreID", "Date"])

    # LoyaltyRewardDF = LoyaltyRewardDF.filter((LoyaltyRewardDF.Status == 'A') | (LoyaltyRewardDF.Status == 'L') | (LoyaltyRewardDF.Status =='E'))

    return LoyaltyRewardDF


# -------------------------------------------------------------#
# Creating ReceiptheaderaddonlistDF DataFrame
# -------------------------------------------------------------#

def receiptheader_addonlist_df(logger, loadcontrol, df):
    logger.info("Creating ReceiptHeaderAddonListDF")

    ReceiptHeaderAddonListDF = df.select('RetailStoreID', 'WorkStationID', 'GK_ReceiptDateTime', 'GK_ReceiptNumber',
                                         'SequenceNumber',
                                         'GK_ReceiptHeaderAddonList_GK_Addon_GK_AddonPos',
                                         'GK_ReceiptHeaderAddonList_GK_Addon_GK_Key',
                                         'GK_ReceiptHeaderAddonList_GK_Addon_GK_Value', )

    ReceiptHeaderAddonListDF = ReceiptHeaderAddonListDF.withColumnRenamed('GK_ReceiptDateTime', 'ReceiptDateTime')
    ReceiptHeaderAddonListDF = ReceiptHeaderAddonListDF.withColumnRenamed('GK_ReceiptNumber', 'ReceiptNumber')
    ReceiptHeaderAddonListDF = ReceiptHeaderAddonListDF.withColumnRenamed(
        'GK_ReceiptHeaderAddonList_GK_Addon_GK_AddonPos', 'AddonPos')
    ReceiptHeaderAddonListDF = ReceiptHeaderAddonListDF.withColumnRenamed('GK_ReceiptHeaderAddonList_GK_Addon_GK_Key',
                                                                          'AddonKey')
    ReceiptHeaderAddonListDF = ReceiptHeaderAddonListDF.withColumnRenamed('GK_ReceiptHeaderAddonList_GK_Addon_GK_Value',
                                                                          'AddonValue')

    ReceiptHeaderAddonListDF = (
        ReceiptHeaderAddonListDF.withColumn("tmp", F.arrays_zip('AddonPos', 'AddonKey', 'AddonValue'))
        .withColumn("tmp", F.explode("tmp"))
        .select('RetailStoreID', 'WorkstationID', 'SequenceNumber', 'ReceiptDateTime', 'ReceiptNumber', 'tmp.AddonPos',
                'tmp.AddonKey', 'tmp.AddonValue'))

    ReceiptHeaderAddonListDF = ReceiptHeaderAddonListDF.withColumn('RetailStoreID', F.col('RetailStoreID').cast('long'))
    ReceiptHeaderAddonListDF = ReceiptHeaderAddonListDF.withColumn('WorkstationID', F.col('WorkstationID').cast('long'))
    ReceiptHeaderAddonListDF = ReceiptHeaderAddonListDF.withColumn('SequenceNumber',
                                                                   F.col('SequenceNumber').cast('long'))
    ReceiptHeaderAddonListDF = ReceiptHeaderAddonListDF.withColumn('ReceiptDateTime',
                                                                   F.col('ReceiptDateTime').cast('string'))
    ReceiptHeaderAddonListDF = ReceiptHeaderAddonListDF.withColumn('ReceiptNumber', F.col('ReceiptNumber').cast('long'))
    ReceiptHeaderAddonListDF = ReceiptHeaderAddonListDF.withColumn('AddonPos', F.col('AddonPos').cast('long'))

    ReceiptHeaderAddonListDF = ReceiptHeaderAddonListDF.withColumn('Date', F.split('ReceiptDateTime', 'T')[0])

    ReceiptHeaderAddonListDF = ReceiptHeaderAddonListDF.dropDuplicates(
        ["RetailStoreID", "WorkstationID", "SequenceNumber", "ReceiptDateTime", "ReceiptNumber", "AddonPos", "AddonKey",
         "AddonValue"])

    # ReceiptHeaderAddonListDF = loadcontrol.join(ReceiptHeaderAddonListDF, ["RetailStoreID", "Date"])

    # ReceiptHeaderAddonListDF = ReceiptHeaderAddonListDF.filter((ReceiptHeaderAddonListDF.Status == 'A') | (ReceiptHeaderAddonListDF.Status == 'L') | (ReceiptHeaderAddonListDF.Status =='E'))

    return ReceiptHeaderAddonListDF


# -------------------------------------------------------------#
# Creating ReceiptpositionaddonlistDF DataFrame
# -------------------------------------------------------------#

def receiptposition_addonlist_df(logger, loadcontrol, df):
    logger.info("Creating ReceiptPositionAddonListDF")

    staging_SaleReceiptPositionAddonListDF = df.select('RetailStoreID', 'WorkstationID', 'SequenceNumber',
                                                       'GK_ReceiptDateTime',
                                                       'GK_ReceiptNumber', 'RetailTransaction_LineItem_SequenceNumber',
                                                       'RetailTransaction_LineItem_Sale_GK_ReceiptPositionAddonList_GK_Addon_GK_AddonPos',
                                                       'RetailTransaction_LineItem_Sale_GK_ReceiptPositionAddonList_GK_Addon_GK_Key',
                                                       'RetailTransaction_LineItem_Sale_GK_ReceiptPositionAddonList_GK_Addon_GK_Value')
    staging_SaleReceiptPositionAddonListDF = staging_SaleReceiptPositionAddonListDF.withColumnRenamed(
        'GK_ReceiptDateTime', 'ReceiptDateTime')
    staging_SaleReceiptPositionAddonListDF = staging_SaleReceiptPositionAddonListDF.withColumnRenamed(
        'GK_ReceiptNumber', 'ReceiptNumber')
    staging_SaleReceiptPositionAddonListDF = staging_SaleReceiptPositionAddonListDF.withColumnRenamed(
        'RetailTransaction_LineItem_SequenceNumber', 'LineItemSequenceNumber')
    staging_SaleReceiptPositionAddonListDF = staging_SaleReceiptPositionAddonListDF.withColumnRenamed(
        'RetailTransaction_LineItem_Sale_GK_ReceiptPositionAddonList_GK_Addon_GK_AddonPos', 'AddonPos')
    staging_SaleReceiptPositionAddonListDF = staging_SaleReceiptPositionAddonListDF.withColumnRenamed(
        'RetailTransaction_LineItem_Sale_GK_ReceiptPositionAddonList_GK_Addon_GK_Key', 'AddonKey')
    staging_SaleReceiptPositionAddonListDF = staging_SaleReceiptPositionAddonListDF.withColumnRenamed(
        'RetailTransaction_LineItem_Sale_GK_ReceiptPositionAddonList_GK_Addon_GK_Value', 'AddonValue')

    staging_SaleReceiptPositionAddonListDF = (staging_SaleReceiptPositionAddonListDF.withColumn("tmp", F.arrays_zip(
        'LineItemSequenceNumber', 'AddonPos', 'AddonKey', 'AddonValue')).withColumn("tmp", F.explode("tmp"))
                                              .select('RetailStoreID', 'WorkstationID', 'SequenceNumber',
                                                      'ReceiptDateTime', 'ReceiptNumber',
                                                      F.col('tmp.LineItemSequenceNumber'), F.col('tmp.AddonPos'),
                                                      F.col('tmp.AddonKey'), F.col('tmp.AddonValue')))

    staging_SaleReceiptPositionAddonListDF = (staging_SaleReceiptPositionAddonListDF.withColumn("tmp",
                                                                                                F.arrays_zip('AddonPos',
                                                                                                             'AddonKey',
                                                                                                             'AddonValue')).withColumn(
        "tmp", F.explode("tmp"))
                                              .select('RetailStoreID', 'WorkstationID', 'SequenceNumber',
                                                      'ReceiptDateTime', 'ReceiptNumber', 'LineItemSequenceNumber',
                                                      F.col('tmp.AddonPos'), F.col('tmp.AddonKey'),
                                                      F.col('tmp.AddonValue')))

    staging_ReturnReceiptPositionAddonListDF = df.select('RetailStoreID', 'WorkstationID', 'SequenceNumber',
                                                         'GK_ReceiptDateTime',
                                                         'GK_ReceiptNumber',
                                                         'RetailTransaction_LineItem_SequenceNumber',
                                                         'RetailTransaction_LineItem_Return_GK_ReceiptPositionAddonList_GK_Addon_GK_AddonPos',
                                                         'RetailTransaction_LineItem_Return_GK_ReceiptPositionAddonList_GK_Addon_GK_Key',
                                                         'RetailTransaction_LineItem_Return_GK_ReceiptPositionAddonList_GK_Addon_GK_Value')

    staging_ReturnReceiptPositionAddonListDF = staging_ReturnReceiptPositionAddonListDF.withColumnRenamed(
        'GK_ReceiptDateTime', 'ReceiptDateTime')
    staging_ReturnReceiptPositionAddonListDF = staging_ReturnReceiptPositionAddonListDF.withColumnRenamed(
        'GK_ReceiptNumber', 'ReceiptNumber')
    staging_ReturnReceiptPositionAddonListDF = staging_ReturnReceiptPositionAddonListDF.withColumnRenamed(
        'RetailTransaction_LineItem_SequenceNumber', 'LineItemSequenceNumber')
    staging_ReturnReceiptPositionAddonListDF = staging_ReturnReceiptPositionAddonListDF.withColumnRenamed(
        'RetailTransaction_LineItem_Return_GK_ReceiptPositionAddonList_GK_Addon_GK_AddonPos', 'AddonPos')
    staging_ReturnReceiptPositionAddonListDF = staging_ReturnReceiptPositionAddonListDF.withColumnRenamed(
        'RetailTransaction_LineItem_Return_GK_ReceiptPositionAddonList_GK_Addon_GK_Key', 'AddonKey')
    staging_ReturnReceiptPositionAddonListDF = staging_ReturnReceiptPositionAddonListDF.withColumnRenamed(
        'RetailTransaction_LineItem_Return_GK_ReceiptPositionAddonList_GK_Addon_GK_Value', 'AddonValue')

    staging_ReturnReceiptPositionAddonListDF = (staging_ReturnReceiptPositionAddonListDF.withColumn("tmp", F.arrays_zip(
        'LineItemSequenceNumber', 'AddonPos', 'AddonKey', 'AddonValue')).withColumn("tmp", F.explode("tmp"))
                                                .select('RetailStoreID', 'WorkstationID', 'SequenceNumber',
                                                        'ReceiptDateTime', 'ReceiptNumber',
                                                        F.col('tmp.LineItemSequenceNumber'), F.col('tmp.AddonPos'),
                                                        F.col('tmp.AddonKey'), F.col('tmp.AddonValue')))

    staging_ReturnReceiptPositionAddonListDF = (staging_ReturnReceiptPositionAddonListDF.withColumn("tmp", F.arrays_zip(
        'AddonPos', 'AddonKey', 'AddonValue')).withColumn("tmp", F.explode("tmp"))
                                                .select('RetailStoreID', 'WorkstationID', 'SequenceNumber',
                                                        'ReceiptDateTime', 'ReceiptNumber', 'LineItemSequenceNumber',
                                                        F.col('tmp.AddonPos'), F.col('tmp.AddonKey'),
                                                        F.col('tmp.AddonValue')))

    ReceiptPositionAddonListDF = staging_ReturnReceiptPositionAddonListDF.union(staging_SaleReceiptPositionAddonListDF)

    ReceiptPositionAddonListDF = ReceiptPositionAddonListDF.withColumn('RetailStoreID',
                                                                       F.col('RetailStoreID').cast('long'))
    ReceiptPositionAddonListDF = ReceiptPositionAddonListDF.withColumn('WorkstationID',
                                                                       F.col('WorkstationID').cast('long'))
    ReceiptPositionAddonListDF = ReceiptPositionAddonListDF.withColumn('SequenceNumber',
                                                                       F.col('SequenceNumber').cast('long'))
    ReceiptPositionAddonListDF = ReceiptPositionAddonListDF.withColumn('ReceiptDateTime',
                                                                       F.col('ReceiptDateTime').cast('string'))
    ReceiptPositionAddonListDF = ReceiptPositionAddonListDF.withColumn('ReceiptNumber',
                                                                       F.col('ReceiptNumber').cast('long'))
    ReceiptPositionAddonListDF = ReceiptPositionAddonListDF.withColumn('LineItemSequenceNumber',
                                                                       F.col('LineItemSequenceNumber').cast('long'))
    ReceiptPositionAddonListDF = ReceiptPositionAddonListDF.withColumn('AddonPos', F.col('AddonPos').cast('long'))

    ReceiptPositionAddonListDF = ReceiptPositionAddonListDF.withColumn('Date', F.split('ReceiptDateTime', 'T')[0])

    # ReceiptPositionAddonListDF = loadcontrol.join(ReceiptPositionAddonListDF, ["RetailStoreID", "Date"])

    # ReceiptPositionAddonListDF = ReceiptPositionAddonListDF.filter((ReceiptPositionAddonListDF.Status == 'A') | (ReceiptPositionAddonListDF.Status == 'L') | (ReceiptPositionAddonListDF.Status =='E'))

    return ReceiptPositionAddonListDF


# -------------------------------------------------------------#
# Creating CustomerDF DataFrame
# -------------------------------------------------------------#

def customer_df(logger, loadcontrol, df, config):
    logger.info("Creating CustomerDF")

    CustomerDF = df.select('RetailStoreID', 'WorkStationID', 'SequenceNumber', 'GK_ReceiptDateTime', 'GK_ReceiptNumber',
                           'RetailTransaction_GK_Customer_Address_AddressLine',
                           'RetailTransaction_GK_Customer_AccountNumber',
                           'RetailTransaction_GK_Customer_GK_CustomerType',
                           'RetailTransaction_GK_Customer_GK_CustomerTypeDescription',
                           'RetailTransaction_GK_Customer_GK_CustomerGenericFlag',
                           'RetailTransaction_GK_Customer_GK_CustomerRequisationRequiredFlag',
                           'RetailTransaction_GK_Customer_GK_CustomerBuyerRequiredFlag',
                           'RetailTransaction_GK_Customer_GK_CustomerContactRequiredFlag',
                           'RetailTransaction_GK_Customer_GK_CustomerGroup')

    CustomerDF = CustomerDF.withColumnRenamed('GK_ReceiptDateTime', 'ReceiptDateTime')
    CustomerDF = CustomerDF.withColumnRenamed('GK_ReceiptNumber', 'ReceiptNumber')
    CustomerDF = CustomerDF.withColumnRenamed('RetailTransaction_GK_Customer_Address_AddressLine', 'AddressLine')
    CustomerDF = CustomerDF.withColumnRenamed('RetailTransaction_GK_Customer_AccountNumber', 'AccountNumber')
    CustomerDF = CustomerDF.withColumnRenamed('RetailTransaction_GK_Customer_GK_CustomerType', 'CustomerType')
    CustomerDF = CustomerDF.withColumnRenamed('RetailTransaction_GK_Customer_GK_CustomerTypeDescription',
                                              'CustomerTypeDescription')
    CustomerDF = CustomerDF.withColumnRenamed('RetailTransaction_GK_Customer_GK_CustomerGenericFlag',
                                              'CustomerGenericFlag')
    CustomerDF = CustomerDF.withColumnRenamed('RetailTransaction_GK_Customer_GK_CustomerRequisationRequiredFlag',
                                              'CustomerRequisationRequiredFlag')
    CustomerDF = CustomerDF.withColumnRenamed('RetailTransaction_GK_Customer_GK_CustomerBuyerRequiredFlag',
                                              'CustomerBuyerRequiredFlag')
    CustomerDF = CustomerDF.withColumnRenamed('RetailTransaction_GK_Customer_GK_CustomerContactRequiredFlag',
                                              'CustomerContactRequiredFlag')
    CustomerDF = CustomerDF.withColumnRenamed('RetailTransaction_GK_Customer_GK_CustomerGroup', 'CustomerGroup')

    CustomerDF = (CustomerDF.withColumn("Address", F.explode_outer("AddressLine"))
                  .select('RetailStoreID', 'WorkstationID', 'SequenceNumber', 'ReceiptNumber', 'ReceiptDateTime',
                          'Address', 'AccountNumber',
                          'CustomerType', 'CustomerTypeDescription', 'CustomerGenericFlag',
                          'CustomerRequisationRequiredFlag',
                          'CustomerBuyerRequiredFlag', 'CustomerContactRequiredFlag', 'CustomerGroup'))

    CustomerDF = (CustomerDF.withColumn("tmp", F.arrays_zip('AccountNumber', 'CustomerType', 'CustomerTypeDescription',
                                                            'CustomerGenericFlag', 'CustomerRequisationRequiredFlag',
                                                            'CustomerBuyerRequiredFlag', 'CustomerContactRequiredFlag',
                                                            'CustomerGroup'))
                  .withColumn("tmp", F.explode_outer("tmp"))
                  .select('RetailStoreID', 'WorkstationID', 'SequenceNumber', 'ReceiptNumber', 'ReceiptDateTime',
                          'Address', F.col('tmp.AccountNumber'), F.col('tmp.CustomerType'),
                          F.col('tmp.CustomerTypeDescription'), F.col('tmp.CustomerGenericFlag'),
                          F.col('tmp.CustomerRequisationRequiredFlag'), F.col('tmp.CustomerBuyerRequiredFlag'),
                          F.col('tmp.CustomerContactRequiredFlag'), F.col('tmp.CustomerGroup')))

    CustomerDF = CustomerDF.withColumn("Address", F.concat_ws(",", "Address"))

    CustomerDF = CustomerDF.withColumn('RetailStoreID', F.col('RetailStoreID').cast('long'))
    CustomerDF = CustomerDF.withColumn('WorkstationID', F.col('WorkstationID').cast('long'))
    CustomerDF = CustomerDF.withColumn('SequenceNumber', F.col('SequenceNumber').cast('long'))
    CustomerDF = CustomerDF.withColumn('ReceiptDateTime', F.col('ReceiptDateTime').cast('string'))
    CustomerDF = CustomerDF.withColumn('ReceiptNumber', F.col('ReceiptNumber').cast('long'))
    # if(config['input']['type']['type'] == 'reh' or config['input']['type']['type'] == 'KuU'):
    if config['input']['account_number_cast']['status']:
        CustomerDF = CustomerDF.withColumn('AccountNumber', F.col('AccountNumber').cast('long'))
    CustomerDF = CustomerDF.withColumn('CustomerGenericFlag', F.col('CustomerGenericFlag').cast('boolean'))
    CustomerDF = CustomerDF.withColumn('CustomerRequisationRequiredFlag',
                                       F.col('CustomerRequisationRequiredFlag').cast('boolean'))
    CustomerDF = CustomerDF.withColumn('CustomerBuyerRequiredFlag', F.col('CustomerBuyerRequiredFlag').cast('boolean'))
    CustomerDF = CustomerDF.withColumn('CustomerContactRequiredFlag',
                                       F.col('CustomerContactRequiredFlag').cast('boolean'))

    CustomerDF = CustomerDF.dropDuplicates(
        ["RetailStoreID", "WorkstationID", "SequenceNumber", "ReceiptDateTime", "ReceiptNumber"])

    CustomerDF = CustomerDF.withColumn('Date', F.split('ReceiptDateTime', 'T')[0])

    # CustomerDF = loadcontrol.join(CustomerDF, ["RetailStoreID", "Date"])

    # CustomerDF = CustomerDF.filter((CustomerDF.Status == 'A') | (CustomerDF.Status == 'L') | (CustomerDF.Status =='E'))

    return CustomerDF


# -------------------------------------------------------------#
# Creating RetailpricemodifierDF DataFrame
# -------------------------------------------------------------#

def retail_pricemodifier_df(logger, loadcontrol, df):
    logger.info("Creating RetailpricemodifierDF")

    ReturnDF = df.select('RetailStoreID', 'WorkStationID', 'SequenceNumber', 'GK_ReceiptDateTime', 'GK_ReceiptNumber',
                         'RetailTransaction_LineItem_SequenceNumber',
                         'RetailTransaction_LineItem_Return_RetailPriceModifier_SequenceNumber',
                         'RetailTransaction_LineItem_Return_RetailPriceModifier_Amount__Action',
                         'RetailTransaction_LineItem_Return_RetailPriceModifier_Amount__VALUE',
                         'RetailTransaction_LineItem_Return_RetailPriceModifier_PreviousPrice',
                         'RetailTransaction_LineItem_Return_RetailPriceModifier_NewPrice',
                         'RetailTransaction_LineItem_Return_RetailPriceModifier_PromotionID',
                         'RetailTransaction_LineItem_Return_RetailPriceModifier_ReasonCode',
                         'RetailTransaction_LineItem_Return_RetailPriceModifier_GK_RebateID')

    ReturnDF = ReturnDF.withColumnRenamed('GK_ReceiptDateTime', 'ReceiptDateTime')
    ReturnDF = ReturnDF.withColumnRenamed('GK_ReceiptNumber', 'ReceiptNumber')
    ReturnDF = ReturnDF.withColumnRenamed('RetailTransaction_LineItem_Return_RetailPriceModifier_SequenceNumber',
                                          'PriceModifierSequenceNumber')
    ReturnDF = ReturnDF.withColumnRenamed('RetailTransaction_LineItem_Return_RetailPriceModifier_Amount__Action',
                                          'AmountAction')
    ReturnDF = ReturnDF.withColumnRenamed('RetailTransaction_LineItem_Return_RetailPriceModifier_Amount__VALUE',
                                          'AmountValue')
    ReturnDF = ReturnDF.withColumnRenamed('RetailTransaction_LineItem_Return_RetailPriceModifier_PreviousPrice',
                                          'PreviousPrice')
    ReturnDF = ReturnDF.withColumnRenamed('RetailTransaction_LineItem_Return_RetailPriceModifier_NewPrice', 'NewPrice')
    ReturnDF = ReturnDF.withColumnRenamed('RetailTransaction_LineItem_Return_RetailPriceModifier_ReasonCode',
                                          'ReasonCode')
    ReturnDF = ReturnDF.withColumnRenamed('RetailTransaction_LineItem_Return_RetailPriceModifier_GK_RebateID',
                                          'RebateID')
    ReturnDF = ReturnDF.withColumnRenamed('RetailTransaction_LineItem_Return_RetailPriceModifier_PromotionID',
                                          'PromotionID')
    ReturnDF = ReturnDF.withColumnRenamed('RetailTransaction_LineItem_SequenceNumber', 'LineItemSequenceNumber')

    ReturnDF = (ReturnDF.withColumn("tmp", F.arrays_zip('LineItemSequenceNumber', 'PriceModifierSequenceNumber',
                                                        'AmountAction', 'AmountValue', 'PreviousPrice', 'NewPrice',
                                                        'PromotionID', 'ReasonCode', 'RebateID')).withColumn("tmp",
                                                                                                             F.explode(
                                                                                                                 "tmp"))
                .select('RetailStoreID', 'WorkstationID', 'ReceiptDateTime', 'ReceiptNumber',
                        'tmp.LineItemSequenceNumber', 'tmp.PriceModifierSequenceNumber', 'tmp.AmountAction',
                        'tmp.AmountValue', 'tmp.PromotionID', 'tmp.PreviousPrice', 'tmp.NewPrice', 'tmp.ReasonCode',
                        'tmp.RebateID'))

    ReturnDF = (ReturnDF.withColumn("tmp", F.arrays_zip('PriceModifierSequenceNumber', 'AmountAction',
                                                        'PromotionID', 'PreviousPrice', 'NewPrice', 'ReasonCode',
                                                        'RebateID'))
                .withColumn("tmp", F.explode("tmp"))
                .select('RetailStoreID', 'WorkstationID', 'ReceiptDateTime', 'ReceiptNumber', 'LineItemSequenceNumber',
                        'tmp.PriceModifierSequenceNumber', 'tmp.AmountAction', 'AmountValue', 'tmp.PromotionID',
                        'tmp.PreviousPrice', 'tmp.NewPrice', 'tmp.ReasonCode', 'tmp.RebateID'))

    SalesDF = df.select('RetailStoreID', 'WorkStationID', 'SequenceNumber', 'GK_ReceiptDateTime', 'GK_ReceiptNumber',
                        'RetailTransaction_LineItem_SequenceNumber',
                        'RetailTransaction_LineItem_Sale_RetailPriceModifier_SequenceNumber',
                        'RetailTransaction_LineItem_Sale_RetailPriceModifier_Amount__Action',
                        'RetailTransaction_LineItem_Sale_RetailPriceModifier_Amount__VALUE',
                        'RetailTransaction_LineItem_Sale_RetailPriceModifier_PromotionID',
                        'RetailTransaction_LineItem_Sale_RetailPriceModifier_PreviousPrice',
                        'RetailTransaction_LineItem_Sale_RetailPriceModifier_NewPrice',
                        'RetailTransaction_LineItem_Sale_RetailPriceModifier_GK_ReasonCode__VALUE',
                        'RetailTransaction_LineItem_Sale_RetailPriceModifier_GK_RebateID')

    SalesDF = SalesDF.withColumnRenamed('GK_ReceiptDateTime', 'ReceiptDateTime')
    SalesDF = SalesDF.withColumnRenamed('GK_ReceiptNumber', 'ReceiptNumber')
    SalesDF = SalesDF.withColumnRenamed('RetailTransaction_LineItem_SequenceNumber', 'LineItemSequenceNumber')
    SalesDF = SalesDF.withColumnRenamed('RetailTransaction_LineItem_Sale_RetailPriceModifier_SequenceNumber',
                                        'PriceModifierSequenceNumber')
    SalesDF = SalesDF.withColumnRenamed('RetailTransaction_LineItem_Sale_RetailPriceModifier_Amount__Action',
                                        'AmountAction')
    SalesDF = SalesDF.withColumnRenamed('RetailTransaction_LineItem_Sale_RetailPriceModifier_Amount__VALUE',
                                        'AmountValue')
    SalesDF = SalesDF.withColumnRenamed('RetailTransaction_LineItem_Sale_RetailPriceModifier_PromotionID',
                                        'PromotionID')
    SalesDF = SalesDF.withColumnRenamed('RetailTransaction_LineItem_Sale_RetailPriceModifier_PreviousPrice',
                                        'PreviousPrice')
    SalesDF = SalesDF.withColumnRenamed('RetailTransaction_LineItem_Sale_RetailPriceModifier_NewPrice', 'NewPrice')
    SalesDF = SalesDF.withColumnRenamed('RetailTransaction_LineItem_Sale_RetailPriceModifier_GK_ReasonCode__VALUE',
                                        'ReasonCode')
    SalesDF = SalesDF.withColumnRenamed('RetailTransaction_LineItem_Sale_RetailPriceModifier_GK_RebateID', 'RebateID')

    SalesDF = (SalesDF.withColumn("tmp",
                                  F.arrays_zip("LineItemSequenceNumber", "PriceModifierSequenceNumber", "AmountAction",
                                               "AmountValue", "PromotionID", "PreviousPrice", "NewPrice", "ReasonCode",
                                               "RebateID"))
               .withColumn("tmp", F.explode("tmp"))
               .select('RetailStoreID', 'WorkstationID', 'ReceiptDateTime', 'ReceiptNumber',
                       'tmp.LineItemSequenceNumber', 'tmp.PriceModifierSequenceNumber', 'tmp.AmountAction',
                       'tmp.AmountValue', 'tmp.PromotionID', 'tmp.PreviousPrice', 'tmp.NewPrice', 'tmp.ReasonCode',
                       'tmp.RebateID'))

    SalesDF = (SalesDF.withColumn("tmp", F.arrays_zip('PriceModifierSequenceNumber', 'AmountAction',
                                                      'PromotionID', 'PreviousPrice', 'NewPrice', 'ReasonCode',
                                                      'RebateID'))
               .withColumn("tmp", F.explode("tmp"))
               .select('RetailStoreID', 'WorkstationID', 'ReceiptDateTime', 'ReceiptNumber', 'LineItemSequenceNumber',
                       'tmp.PriceModifierSequenceNumber', 'tmp.AmountAction', 'AmountValue', 'tmp.PromotionID',
                       'tmp.PreviousPrice', 'tmp.NewPrice', 'tmp.ReasonCode', 'tmp.RebateID'))

    MergedDF = ReturnDF.unionAll(SalesDF)

    MergedDF = MergedDF.filter(F.col('PriceModifierSequenceNumber').isNotNull())

    MergedDF = MergedDF.dropDuplicates(
        ['RetailStoreID', 'WorkstationID', 'ReceiptDateTime', 'ReceiptNumber', 'LineItemSequenceNumber',
         'PriceModifierSequenceNumber'])

    MergedDF = MergedDF.withColumn('RetailStoreID', F.col('RetailStoreID').cast('long'))
    MergedDF = MergedDF.withColumn('WorkstationID', F.col('WorkstationID').cast('long'))
    MergedDF = MergedDF.withColumn('ReceiptDateTime', F.col('ReceiptDateTime').cast('string'))
    MergedDF = MergedDF.withColumn('ReceiptNumber', F.col('ReceiptNumber').cast('long'))
    MergedDF = MergedDF.withColumn('LineItemSequenceNumber', F.col('LineItemSequenceNumber').cast('long'))
    MergedDF = MergedDF.withColumn('PriceModifierSequenceNumber', F.col('PriceModifierSequenceNumber').cast('long'))
    MergedDF = MergedDF.withColumn('AmountValue', F.col('AmountValue').cast('double'))
    MergedDF = MergedDF.withColumn('PreviousPrice', F.col('PreviousPrice').cast('double'))
    MergedDF = MergedDF.withColumn('NewPrice', F.col('NewPrice').cast('double'))

    # MergedDF = loadcontrol.join(MergedDF, ["RetailStoreID", "Date"])

    # MergedDF = MergedDF.filter((MergedDF.Status == 'A') | (MergedDF.Status == 'L') | (MergedDF.Status =='E'))

    return MergedDF


# -------------------------------------------------------------#
# Creating ReturnDF DataFrame
# -------------------------------------------------------------#

def return_df(logger, loadcontrol, df):
    logger.info("Creating ReturnDF")

    ReturnDF = df.select('RetailStoreID', 'WorkStationID', 'GK_ReceiptNumber', 'SequenceNumber', 'GK_ReceiptDateTime',
                         'RetailTransaction_LineItem_SequenceNumber', 'RetailTransaction_LineItem_Return__ItemType',
                         'RetailTransaction_LineItem_Return_POSIdentity__POSIDType',
                         'RetailTransaction_LineItem_Return_POSIdentity_POSItemID',
                         'RetailTransaction_LineItem_Return_MerchandiseHierarchy__VALUE',
                         'RetailTransaction_LineItem_Return_ItemID',
                         'RetailTransaction_LineItem_Return_Description',
                         'RetailTransaction_LineItem_Return_TaxIncludedInPriceFlag',
                         'RetailTransaction_LineItem_Return_RegularSalesUnitPrice',
                         'RetailTransaction_LineItem_Return_Quantity__UnitOfMeasureCode',
                         'RetailTransaction_LineItem_Return_Quantity__Units',
                         'RetailTransaction_LineItem_Return_Quantity__VALUE',
                         'RetailTransaction_LineItem_Return_ExtendedAmount',
                         'RetailTransaction_LineItem_Return_GK_ExtendedGrossAmount',
                         'RetailTransaction_LineItem_Return_GK_MerchandiseStructureItemFlag',
                         'RetailTransaction_LineItem_Return_ItemLink',
                         'RetailTransaction_LineItem_Return_GK_ItemLink__LinkType',
                         'RetailTransaction_LineItem_Return_GK_ItemLink__VALUE',
                         'RetailTransaction_LineItem_Return_GK_SpecialPriceDescription',
                         'RetailTransaction_LineItem_Return_Tax__TaxSubType',
                         'RetailTransaction_LineItem_Return_Tax__TaxType',
                         'RetailTransaction_LineItem_Return_Tax_TaxableAmount__TaxIncludedInTaxableAmountFlag',
                         'RetailTransaction_LineItem_Return_TransactionLink__ReasonCode',
                         'RetailTransaction_LineItem_Return_TransactionLink_SequenceNumber',
                         'RetailTransaction_LineItem_Return_Disposal__Method',
                         'RetailTransaction_LineItem_Return_Tax_TaxableAmount__VALUE',
                         'RetailTransaction_LineItem_Return_Tax_Amount',
                         'RetailTransaction_LineItem_Return_Tax_GK_TaxGroupID',
                         'RetailTransaction_LineItem_Return_Tax_Percent')

    ReturnDF = ReturnDF.withColumnRenamed('GK_ReceiptDateTime', 'ReceiptDateTime')
    ReturnDF = ReturnDF.withColumnRenamed('GK_ReceiptNumber', 'ReceiptNumber')
    ReturnDF = ReturnDF.withColumnRenamed('RetailTransaction_LineItem_Return__ItemType', 'ItemType')
    ReturnDF = ReturnDF.withColumnRenamed('RetailTransaction_LineItem_SequenceNumber', 'LineItemSequenceNumber')
    ReturnDF = ReturnDF.withColumnRenamed('RetailTransaction_LineItem_Return_POSIdentity__POSIDType', 'POSIDType')
    ReturnDF = ReturnDF.withColumnRenamed('RetailTransaction_LineItem_Return_POSIdentity_POSItemID', 'POSItemID')
    ReturnDF = ReturnDF.withColumnRenamed('RetailTransaction_LineItem_Return_ItemID', 'ItemID')
    ReturnDF = ReturnDF.withColumnRenamed('RetailTransaction_LineItem_Return_MerchandiseHierarchy__VALUE',
                                          'MerchandiseHierarchyLevel')
    ReturnDF = ReturnDF.withColumnRenamed('RetailTransaction_LineItem_Return_Description', 'Description')
    ReturnDF = ReturnDF.withColumnRenamed('RetailTransaction_LineItem_Return_TaxIncludedInPriceFlag',
                                          'TaxIncludedInPriceFlag')
    ReturnDF = ReturnDF.withColumnRenamed('RetailTransaction_LineItem_Return_RegularSalesUnitPrice',
                                          'RegularSalesUnitPrice')

    ReturnDF = ReturnDF.withColumnRenamed('RetailTransaction_LineItem_Return_ExtendedAmount', 'ExtendedAmount')
    ReturnDF = ReturnDF.withColumnRenamed('RetailTransaction_LineItem_Return_Quantity__UnitOfMeasureCode',
                                          'UnitOfMeasureCode')
    ReturnDF = ReturnDF.withColumnRenamed('RetailTransaction_LineItem_Return_Quantity__VALUE', 'Quantity')
    ReturnDF = ReturnDF.withColumnRenamed('RetailTransaction_LineItem_Return_Quantity__Units', 'Units')
    ReturnDF = ReturnDF.withColumnRenamed('RetailTransaction_LineItem_Return_GK_ExtendedGrossAmount',
                                          'ExtendedGrossAmount')
    ReturnDF = ReturnDF.withColumnRenamed('RetailTransaction_LineItem_Return_GK_MerchandiseStructureItemFlag',
                                          'MerchandiseStructureItemFlag')
    ReturnDF = ReturnDF.withColumnRenamed('RetailTransaction_LineItem_Return_ItemLink', 'ReturnItemLink')
    ReturnDF = ReturnDF.withColumnRenamed('RetailTransaction_LineItem_Return_GK_ItemLink__LinkType', 'ItemLinkType')

    ReturnDF = ReturnDF.withColumnRenamed('RetailTransaction_LineItem_Return_GK_ItemLink__VALUE', 'ItemLink')
    ReturnDF = ReturnDF.withColumnRenamed('RetailTransaction_LineItem_Return_GK_SpecialPriceDescription',
                                          'SpecialPriceDescription')
    ReturnDF = ReturnDF.withColumnRenamed('RetailTransaction_LineItem_Return_Tax__TaxSubType', 'TaxSubType')
    ReturnDF = ReturnDF.withColumnRenamed('RetailTransaction_LineItem_Return_Tax__TaxType', 'TaxType')
    ReturnDF = ReturnDF.withColumnRenamed(
        'RetailTransaction_LineItem_Return_Tax_TaxableAmount__TaxIncludedInTaxableAmountFlag',
        'TaxIncludedInTaxableAmountFlag')
    ReturnDF = ReturnDF.withColumnRenamed('RetailTransaction_LineItem_Return_Tax_TaxableAmount__VALUE', 'TaxableAmount')
    ReturnDF = ReturnDF.withColumnRenamed('RetailTransaction_LineItem_Return_Tax_Amount', 'TaxAmount')
    ReturnDF = ReturnDF.withColumnRenamed('RetailTransaction_LineItem_Return_Tax_Percent', 'TaxPercent')
    ReturnDF = ReturnDF.withColumnRenamed('RetailTransaction_LineItem_Return_Tax_GK_TaxGroupID', 'TaxGroupID')
    ReturnDF = ReturnDF.withColumnRenamed('RetailTransaction_LineItem_Return_TransactionLink__ReasonCode',
                                          'TransactionLinkReasonCode')
    ReturnDF = ReturnDF.withColumnRenamed('RetailTransaction_LineItem_Return_TransactionLink_SequenceNumber',
                                          'TransactionLinkSequenceNumber')
    ReturnDF = ReturnDF.withColumnRenamed('RetailTransaction_LineItem_Return_Disposal__Method', 'DisposalMethod')

    ReturnDF = (ReturnDF
                .withColumn("tmp",
                            F.arrays_zip('LineItemSequenceNumber', 'ItemType', 'POSIDType', 'POSItemID', 'TaxSubType',
                                         'TaxIncludedInTaxableAmountFlag', 'TaxableAmount', 'TaxAmount', 'TaxPercent',
                                         'TaxGroupID', 'ItemID', 'MerchandiseHierarchyLevel', 'Description',
                                         'TaxIncludedInPriceFlag', 'RegularSalesUnitPrice', 'ExtendedAmount',
                                         'UnitOfMeasureCode', 'Quantity', 'Units', 'ExtendedGrossAmount',
                                         'MerchandiseStructureItemFlag', 'ReturnItemLink', 'ItemLinkType', 'ItemLink',
                                         'SpecialPriceDescription', 'TaxType', 'DisposalMethod',
                                         'TransactionLinkSequenceNumber', 'TransactionLinkReasonCode'))
                .withColumn("tmp", F.explode("tmp"))
                .select('RetailStoreID', 'WorkstationID', 'SequenceNumber', 'ReceiptDateTime', 'ReceiptNumber',
                        F.col('tmp.LineItemSequenceNumber'), F.col('tmp.ItemType'), F.col('tmp.POSIDType'),
                        F.col('tmp.POSItemID'), F.col('tmp.TaxSubType'), F.col('tmp.TaxIncludedInTaxableAmountFlag'),
                        F.col('tmp.TaxableAmount'), F.col('tmp.TaxPercent'), F.col('tmp.TaxGroupID'),
                        F.col('tmp.TaxAmount'), F.col('tmp.ItemID'), F.col('tmp.TaxType'),
                        F.col('tmp.MerchandiseHierarchyLevel'), F.col('tmp.Description'),
                        F.col('tmp.TaxIncludedInPriceFlag'), F.col('tmp.RegularSalesUnitPrice'),
                        F.col('tmp.ExtendedAmount'), F.col('tmp.UnitOfMeasureCode'), F.col('tmp.Quantity'),
                        F.col('tmp.Units'), F.col('tmp.ExtendedGrossAmount'), F.col('tmp.MerchandiseStructureItemFlag'),
                        F.col('tmp.ReturnItemLink'), F.col('tmp.ItemLinkType'), F.col('tmp.ItemLink'),
                        F.col('tmp.SpecialPriceDescription'), F.col('tmp.DisposalMethod'),
                        F.col('tmp.TransactionLinkSequenceNumber'), F.col('tmp.TransactionLinkReasonCode')))

    ReturnDF = (ReturnDF
                .withColumn("tmp",
                            F.arrays_zip('TaxSubType', 'TaxIncludedInTaxableAmountFlag',
                                         'TaxPercent', 'TaxGroupID', 'TaxType'))
                .withColumn("tmp", F.explode_outer("tmp"))
                .select('RetailStoreID', 'WorkstationID', 'SequenceNumber', 'ReceiptDateTime', 'ReceiptNumber',
                        'LineItemSequenceNumber', 'ItemType', 'POSIDType', 'POSItemID', F.col('tmp.TaxSubType'),
                        F.col('tmp.TaxIncludedInTaxableAmountFlag'), F.col('TaxableAmount'),
                        F.col('tmp.TaxPercent'), F.col('tmp.TaxGroupID'), F.col('TaxAmount'), 'ItemID',
                        F.col('tmp.TaxType'), 'MerchandiseHierarchyLevel', 'Description', 'TaxIncludedInPriceFlag',
                        'RegularSalesUnitPrice', 'ExtendedAmount', 'UnitOfMeasureCode', 'Quantity', 'Units',
                        'ExtendedGrossAmount', 'MerchandiseStructureItemFlag', 'ReturnItemLink', 'ItemLinkType',
                        'ItemLink', 'SpecialPriceDescription', 'DisposalMethod', 'TransactionLinkSequenceNumber',
                        'TransactionLinkReasonCode'))

    ReturnDF = ReturnDF.filter(F.col('ItemType').isNotNull())

    ReturnDF = ReturnDF.dropDuplicates(
        ["RetailStoreID", "WorkstationID", "SequenceNumber", "ReceiptDateTime", "ReceiptNumber",
         "LineItemSequenceNumber"])

    ReturnDF = ReturnDF.withColumn('RetailStoreID', F.col('RetailStoreID').cast('long'))
    ReturnDF = ReturnDF.withColumn('WorkstationID', F.col('WorkstationID').cast('long'))
    ReturnDF = ReturnDF.withColumn('SequenceNumber', F.col('SequenceNumber').cast('long'))
    ReturnDF = ReturnDF.withColumn('ReceiptDateTime', F.col('ReceiptDateTime').cast('string'))
    ReturnDF = ReturnDF.withColumn('ReceiptNumber', F.col('ReceiptNumber').cast('long'))
    ReturnDF = ReturnDF.withColumn('LineItemSequenceNumber', F.col('LineItemSequenceNumber').cast('long'))
    ReturnDF = ReturnDF.withColumn('POSItemID', F.col('POSItemID').cast('decimal(22, 0)'))
    ReturnDF = ReturnDF.withColumn('TaxIncludedInTaxableAmountFlag',
                                   F.col('TaxIncludedInTaxableAmountFlag').cast('boolean'))
    ReturnDF = ReturnDF.withColumn('TaxableAmount', F.col('TaxableAmount').cast('double'))
    ReturnDF = ReturnDF.withColumn('TaxPercent', F.col('TaxPercent').cast('double'))
    ReturnDF = ReturnDF.withColumn('TaxGroupID', F.col('TaxGroupID').cast('long'))
    ReturnDF = ReturnDF.withColumn('TaxAmount', F.col('TaxAmount').cast('double'))
    ReturnDF = ReturnDF.withColumn('ItemID', F.col('ItemID').cast('long'))
    ReturnDF = ReturnDF.withColumn('MerchandiseHierarchyLevel', F.col('MerchandiseHierarchyLevel').cast('long'))
    ReturnDF = ReturnDF.withColumn('TaxIncludedInPriceFlag', F.col('TaxIncludedInPriceFlag').cast('boolean'))
    ReturnDF = ReturnDF.withColumn('RegularSalesUnitPrice', F.col('RegularSalesUnitPrice').cast('double'))
    ReturnDF = ReturnDF.withColumn('ExtendedAmount', F.col('ExtendedAmount').cast('double'))
    ReturnDF = ReturnDF.withColumn('Quantity', F.col('Quantity').cast('double'))
    ReturnDF = ReturnDF.withColumn('Units', F.col('Units').cast('double'))
    ReturnDF = ReturnDF.withColumn('ExtendedGrossAmount', F.col('ExtendedGrossAmount').cast('double'))
    ReturnDF = ReturnDF.withColumn('MerchandiseStructureItemFlag',
                                   F.col('MerchandiseStructureItemFlag').cast('boolean'))
    ReturnDF = ReturnDF.withColumn('ReturnItemLink', F.col('ReturnItemLink').cast('long'))
    ReturnDF = ReturnDF.withColumn('ItemLink', F.col('ItemLink').cast('long'))
    ReturnDF = ReturnDF.withColumn('SpecialPriceDescription', F.col('SpecialPriceDescription').cast('long'))
    ReturnDF = ReturnDF.withColumn('TransactionLinkSequenceNumber', F.col('TransactionLinkSequenceNumber').cast('long'))

    ReturnDF = ReturnDF.withColumn('Date', F.split('ReceiptDateTime', 'T')[0])
    # changes
    fun = F.udf(lambda x: x if x != 0 else None, returnType=DoubleType())
    ReturnDF = ReturnDF.withColumn('Units', fun(F.col('Units')))
    # ReturnDF = loadcontrol.join(ReturnDF, ["RetailStoreID", "Date"])

    # ReturnDF = ReturnDF.filter((ReturnDF.Status == 'A') | (ReturnDF.Status == 'L') | (ReturnDF.Status =='E'))

    return ReturnDF




# -------------------------------------------------------------#
# Creating SaleDF DataFrame
# -------------------------------------------------------------#

def sale_df(logger, loadcontrol, df):
    logger.info("Creating SaleDF")

    SaleDF = df.select('RetailStoreID', 'WorkStationID', 'GK_ReceiptNumber', 'SequenceNumber', 'GK_ReceiptDateTime',
                       'RetailTransaction_LineItem_SequenceNumber', 'RetailTransaction_LineItem_Return__ItemType',
                       'RetailTransaction_LineItem_Sale_POSIdentity__POSIDType',
                       'RetailTransaction_LineItem_Sale_POSIdentity_POSItemID',
                       'RetailTransaction_LineItem_Sale_MerchandiseHierarchy__VALUE',
                       'RetailTransaction_LineItem_Sale_ItemID',
                       'RetailTransaction_LineItem_Sale_Description',
                       'RetailTransaction_LineItem_Sale_TaxIncludedInPriceFlag',
                       'RetailTransaction_LineItem_Sale_RegularSalesUnitPrice',
                       'RetailTransaction_LineItem_Sale_Quantity__UnitOfMeasureCode',
                       'RetailTransaction_LineItem_Sale_Quantity__Units',
                       'RetailTransaction_LineItem_Sale_Quantity__VALUE',
                       'RetailTransaction_LineItem_Sale__ItemType',
                       'RetailTransaction_LineItem_Sale_ExtendedAmount',
                       'RetailTransaction_LineItem_Sale_GK_ExtendedGrossAmount',
                       'RetailTransaction_LineItem_Sale_GK_MerchandiseStructureItemFlag',
                       'RetailTransaction_LineItem_Sale_ItemLink',
                       'RetailTransaction_LineItem_Sale_GK_ItemLink__LinkType',
                       'RetailTransaction_LineItem_Sale_GK_ItemLink__VALUE',
                       'RetailTransaction_LineItem_Sale_GK_SpecialPriceDescription',
                       'RetailTransaction_LineItem_Sale_Tax__TaxSubType',
                       'RetailTransaction_LineItem_Sale_Tax__TaxType',
                       'RetailTransaction_LineItem_Sale_Tax_TaxableAmount__TaxIncludedInTaxableAmountFlag',
                       'RetailTransaction_LineItem_Sale_Tax_TaxableAmount__VALUE',
                       'RetailTransaction_LineItem_Sale_Tax_Amount',
                       'RetailTransaction_LineItem_Sale_Tax_GK_TaxGroupID',
                       'RetailTransaction_LineItem_Sale_Tax_Percent')

    SaleDF = SaleDF.withColumnRenamed('GK_ReceiptDateTime', 'ReceiptDateTime')
    SaleDF = SaleDF.withColumnRenamed('GK_ReceiptNumber', 'ReceiptNumber')
    SaleDF = SaleDF.withColumnRenamed('RetailTransaction_LineItem_Sale__ItemType', 'ItemType')
    SaleDF = SaleDF.withColumnRenamed('RetailTransaction_LineItem_SequenceNumber', 'LineItemSequenceNumber')
    SaleDF = SaleDF.withColumnRenamed('RetailTransaction_LineItem_Sale_POSIdentity__POSIDType', 'POSIDType')
    SaleDF = SaleDF.withColumnRenamed('RetailTransaction_LineItem_Sale_POSIdentity_POSItemID', 'POSItemID')
    SaleDF = SaleDF.withColumnRenamed('RetailTransaction_LineItem_Sale_ItemID', 'ItemID')
    SaleDF = SaleDF.withColumnRenamed('RetailTransaction_LineItem_Sale_MerchandiseHierarchy__VALUE',
                                      'MerchandiseHierarchyLevel')
    SaleDF = SaleDF.withColumnRenamed('RetailTransaction_LineItem_Sale_Description', 'Description')
    SaleDF = SaleDF.withColumnRenamed('RetailTransaction_LineItem_Sale_TaxIncludedInPriceFlag',
                                      'TaxIncludedInPriceFlag')
    SaleDF = SaleDF.withColumnRenamed('RetailTransaction_LineItem_Sale_RegularSalesUnitPrice', 'RegularSalesUnitPrice')

    SaleDF = SaleDF.withColumnRenamed('RetailTransaction_LineItem_Sale_ExtendedAmount', 'ExtendedAmount')
    SaleDF = SaleDF.withColumnRenamed('RetailTransaction_LineItem_Sale_Quantity__UnitOfMeasureCode',
                                      'UnitOfMeasureCode')
    SaleDF = SaleDF.withColumnRenamed('RetailTransaction_LineItem_Sale_Quantity__VALUE', 'Quantity')
    SaleDF = SaleDF.withColumnRenamed('RetailTransaction_LineItem_Sale_Quantity__Units', 'Units')
    SaleDF = SaleDF.withColumnRenamed('RetailTransaction_LineItem_Sale_GK_ExtendedGrossAmount', 'ExtendedGrossAmount')
    SaleDF = SaleDF.withColumnRenamed('RetailTransaction_LineItem_Sale_GK_MerchandiseStructureItemFlag',
                                      'MerchandiseStructureItemFlag')
    SaleDF = SaleDF.withColumnRenamed('RetailTransaction_LineItem_Sale_ItemLink', 'SaleItemLink')
    SaleDF = SaleDF.withColumnRenamed('RetailTransaction_LineItem_Sale_GK_ItemLink__LinkType', 'ItemLinkType')

    SaleDF = SaleDF.withColumnRenamed('RetailTransaction_LineItem_Sale_GK_ItemLink__VALUE', 'ItemLink')
    SaleDF = SaleDF.withColumnRenamed('RetailTransaction_LineItem_Sale_GK_SpecialPriceDescription',
                                      'SpecialPriceDescription')
    SaleDF = SaleDF.withColumnRenamed('RetailTransaction_LineItem_Sale_Tax__TaxSubType', 'TaxSubType')
    SaleDF = SaleDF.withColumnRenamed('RetailTransaction_LineItem_Sale_Tax__TaxType', 'TaxType')
    SaleDF = SaleDF.withColumnRenamed(
        'RetailTransaction_LineItem_Sale_Tax_TaxableAmount__TaxIncludedInTaxableAmountFlag',
        'TaxIncludedInTaxableAmountFlag')
    SaleDF = SaleDF.withColumnRenamed('RetailTransaction_LineItem_Sale_Tax_TaxableAmount__VALUE', 'TaxableAmount')
    SaleDF = SaleDF.withColumnRenamed('RetailTransaction_LineItem_Sale_Tax_Amount', 'TaxAmount')
    SaleDF = SaleDF.withColumnRenamed('RetailTransaction_LineItem_Sale_Tax_Percent', 'TaxPercent')
    SaleDF = SaleDF.withColumnRenamed('RetailTransaction_LineItem_Sale_Tax_GK_TaxGroupID', 'TaxGroupID')

    SaleDF = (SaleDF
              .withColumn("tmp",
                          F.arrays_zip('LineItemSequenceNumber', 'ItemType', 'POSIDType', 'POSItemID', 'TaxSubType',
                                       'TaxIncludedInTaxableAmountFlag', 'TaxableAmount', 'TaxAmount', 'TaxPercent',
                                       'TaxGroupID', 'ItemID', 'MerchandiseHierarchyLevel', 'Description',
                                       'TaxIncludedInPriceFlag', 'RegularSalesUnitPrice', 'ExtendedAmount',
                                       'UnitOfMeasureCode', 'Quantity', 'Units', 'ExtendedGrossAmount',
                                       'MerchandiseStructureItemFlag', 'SaleItemLink', 'ItemLinkType', 'ItemLink',
                                       'SpecialPriceDescription', 'TaxType'))
              .withColumn("tmp", F.explode("tmp"))
              .select('RetailStoreID', 'WorkstationID', 'SequenceNumber', 'ReceiptDateTime', 'ReceiptNumber',
                      F.col('tmp.LineItemSequenceNumber'), F.col('tmp.ItemType'), F.col('tmp.POSIDType'),
                      F.col('tmp.POSItemID'), F.col('tmp.TaxSubType'), F.col('tmp.TaxIncludedInTaxableAmountFlag'),
                      F.col('tmp.TaxableAmount'), F.col('tmp.TaxPercent'), F.col('tmp.TaxGroupID'),
                      F.col('tmp.TaxAmount'), F.col('tmp.ItemID'), F.col('tmp.TaxType'),
                      F.col('tmp.MerchandiseHierarchyLevel'), F.col('tmp.Description'),
                      F.col('tmp.TaxIncludedInPriceFlag'), F.col('tmp.RegularSalesUnitPrice'),
                      F.col('tmp.ExtendedAmount'), F.col('tmp.UnitOfMeasureCode'), F.col('tmp.Quantity'),
                      F.col('tmp.Units'), F.col('tmp.ExtendedGrossAmount'), F.col('tmp.MerchandiseStructureItemFlag'),
                      F.col('tmp.SaleItemLink'), F.col('tmp.ItemLinkType'), F.col('tmp.ItemLink'),
                      F.col('tmp.SpecialPriceDescription')))

    SaleDF = (SaleDF
              .withColumn("tmp",
                          F.arrays_zip('TaxSubType', 'TaxIncludedInTaxableAmountFlag',
                                       'TaxPercent', 'TaxGroupID', 'TaxType'))
              .withColumn("tmp", F.explode_outer("tmp"))
              .select('RetailStoreID', 'WorkstationID', 'SequenceNumber', 'ReceiptDateTime', 'ReceiptNumber',
                      'LineItemSequenceNumber', 'ItemType', 'POSIDType', 'POSItemID', F.col('tmp.TaxSubType'),
                      F.col('tmp.TaxIncludedInTaxableAmountFlag'), F.col('TaxableAmount'), F.col('tmp.TaxPercent'),
                      F.col('tmp.TaxGroupID'), F.col('TaxAmount'), 'ItemID', F.col('tmp.TaxType'),
                      'MerchandiseHierarchyLevel', 'Description', 'TaxIncludedInPriceFlag', 'RegularSalesUnitPrice',
                      'ExtendedAmount', 'UnitOfMeasureCode', 'Quantity', 'Units', 'ExtendedGrossAmount',
                      'MerchandiseStructureItemFlag', 'SaleItemLink', 'ItemLinkType', 'ItemLink',
                      'SpecialPriceDescription'))

    SaleDF = SaleDF.filter(F.col('ItemType').isNotNull())

    SaleDF = SaleDF.dropDuplicates(
        ["RetailStoreID", "WorkstationID", "SequenceNumber", "ReceiptDateTime", "ReceiptNumber",
         "LineItemSequenceNumber"])

    SaleDF = SaleDF.withColumn('RetailStoreID', F.col('RetailStoreID').cast('long'))
    SaleDF = SaleDF.withColumn('WorkstationID', F.col('WorkstationID').cast('long'))
    SaleDF = SaleDF.withColumn('SequenceNumber', F.col('SequenceNumber').cast('long'))
    SaleDF = SaleDF.withColumn('ReceiptDateTime', F.col('ReceiptDateTime').cast('string'))
    SaleDF = SaleDF.withColumn('ReceiptNumber', F.col('ReceiptNumber').cast('long'))
    SaleDF = SaleDF.withColumn('LineItemSequenceNumber', F.col('LineItemSequenceNumber').cast('long'))
    SaleDF = SaleDF.withColumn('POSItemID', F.col('POSItemID').cast('decimal(22, 0)'))
    SaleDF = SaleDF.withColumn('TaxIncludedInTaxableAmountFlag',
                               F.col('TaxIncludedInTaxableAmountFlag').cast('boolean'))
    SaleDF = SaleDF.withColumn('TaxableAmount', F.col('TaxableAmount').cast('double'))
    SaleDF = SaleDF.withColumn('TaxPercent', F.col('TaxPercent').cast('double'))
    SaleDF = SaleDF.withColumn('TaxGroupID', F.col('TaxGroupID').cast('long'))
    SaleDF = SaleDF.withColumn('TaxAmount', F.col('TaxAmount').cast('double'))
    SaleDF = SaleDF.withColumn('ItemID', F.col('ItemID').cast('long'))
    SaleDF = SaleDF.withColumn('MerchandiseHierarchyLevel', F.col('MerchandiseHierarchyLevel').cast('long'))
    SaleDF = SaleDF.withColumn('TaxIncludedInPriceFlag', F.col('TaxIncludedInPriceFlag').cast('boolean'))
    SaleDF = SaleDF.withColumn('RegularSalesUnitPrice', F.col('RegularSalesUnitPrice').cast('double'))
    SaleDF = SaleDF.withColumn('ExtendedAmount', F.col('ExtendedAmount').cast('double'))
    SaleDF = SaleDF.withColumn('Quantity', F.col('Quantity').cast('double'))
    SaleDF = SaleDF.withColumn('Units', F.col('Units').cast('double'))
    SaleDF = SaleDF.withColumn('ExtendedGrossAmount', F.col('ExtendedGrossAmount').cast('double'))
    SaleDF = SaleDF.withColumn('MerchandiseStructureItemFlag', F.col('MerchandiseStructureItemFlag').cast('boolean'))
    SaleDF = SaleDF.withColumn('SaleItemLink', F.col('SaleItemLink').cast('long'))
    SaleDF = SaleDF.withColumn('ItemLink', F.col('ItemLink').cast('long'))
    SaleDF = SaleDF.withColumn('SpecialPriceDescription', F.col('SpecialPriceDescription').cast('long'))

    SaleDF = SaleDF.withColumn('Date', F.split('ReceiptDateTime', 'T')[0])
    # changes
    fun = F.udf(lambda x: x if x != 0 else None, returnType=DoubleType())
    SaleDF = SaleDF.withColumn('Units', fun(F.col('Units')))
    # SaleDF = loadcontrol.join(SaleDF, ["RetailStoreID", "Date"])

    # SaleDF = SaleDF.filter((SaleDF.Status == 'A') | (SaleDF.Status == 'L') | (SaleDF.Status =='E'))

    return SaleDF


# -------------------------------------------------------------#
# Creating RetailtransactionheaderDF
# -------------------------------------------------------------#

def retail_transactionheader_df(logger, loadcontrol, df):
    logger.info("Creating RetailtransactionheaderDF")

    RetailTransactionHeaderDF = df.filter(F.col('RetailTransaction__Version').isNotNull())

    RetailTransactionHeaderDF = RetailTransactionHeaderDF.select('RetailStoreID', 'WorkStationID', 'SequenceNumber',
                                                                 'GK_ReceiptNumber', 'GK_ReceiptDateTime',
                                                                 'RetailTransaction__OutsideSalesFlag',
                                                                 'RetailTransaction__SplitCheckFlag',
                                                                 'RetailTransaction__TransactionStatus',
                                                                 'RetailTransaction_Total__TotalType',
                                                                 'RetailTransaction_Total__VALUE',
                                                                 'RetailTransaction__TypeCode',
                                                                 'RetailTransaction_ReceiptDateTime',
                                                                 'RetailTransaction_GK_AmendmentFlag',
                                                                 'RetailTransaction_GK_NegativeTotalFlag',
                                                                 'RetailTransaction_TransactionLink_RetailStoreID',
                                                                 'RetailTransaction_TransactionLink_WorkstationID',
                                                                 'RetailTransaction_TransactionLink_SequenceNumber',
                                                                 'RetailTransaction_TransactionLink_BeginDateTime',
                                                                 'RetailTransaction_TransactionLink_GK_ReceiptDateTime',
                                                                 'RetailTransaction_TransactionLink_GK_ReceiptNumber',
                                                                 'RetailTransaction_TransactionLink__ReasonCode', )

    RetailTransactionHeaderDF = RetailTransactionHeaderDF.withColumnRenamed('RetailTransaction_Total__TotalType',
                                                                            'RetailTransactionTotalType')
    RetailTransactionHeaderDF = RetailTransactionHeaderDF.withColumnRenamed('RetailTransaction_Total__VALUE',
                                                                            'RetailTransactionTotalValue')
    RetailTransactionHeaderDF = RetailTransactionHeaderDF.withColumnRenamed('GK_ReceiptDateTime', 'ReceiptDateTime')
    RetailTransactionHeaderDF = RetailTransactionHeaderDF.withColumnRenamed('GK_ReceiptNumber', 'ReceiptNumber')
    RetailTransactionHeaderDF = RetailTransactionHeaderDF.withColumnRenamed('RetailTransaction__OutsideSalesFlag',
                                                                            'OutsideSalesFlag')
    RetailTransactionHeaderDF = RetailTransactionHeaderDF.withColumnRenamed('RetailTransaction__SplitCheckFlag',
                                                                            'SplitCheckFlag')
    RetailTransactionHeaderDF = RetailTransactionHeaderDF.withColumnRenamed('RetailTransaction__TransactionStatus',
                                                                            'TransactionStatus')
    RetailTransactionHeaderDF = RetailTransactionHeaderDF.withColumnRenamed('RetailTransaction__TypeCode', 'TypeCode')
    RetailTransactionHeaderDF = RetailTransactionHeaderDF.withColumnRenamed('RetailTransaction_ReceiptDateTime',
                                                                            'RetailTransactionReceiptDateTime')
    RetailTransactionHeaderDF = RetailTransactionHeaderDF.withColumnRenamed('RetailTransaction_GK_AmendmentFlag',
                                                                            'AmendmentFlag')
    RetailTransactionHeaderDF = RetailTransactionHeaderDF.withColumnRenamed('RetailTransaction_GK_NegativeTotalFlag',
                                                                            'NegativeTotalFlag')
    RetailTransactionHeaderDF = RetailTransactionHeaderDF.withColumnRenamed(
        'RetailTransaction_TransactionLink_RetailStoreID', 'TransactionLinkRetailStoreID')
    RetailTransactionHeaderDF = RetailTransactionHeaderDF.withColumnRenamed(
        'RetailTransaction_TransactionLink_WorkstationID', 'TransactionLinkWorkstationID')
    RetailTransactionHeaderDF = RetailTransactionHeaderDF.withColumnRenamed(
        'RetailTransaction_TransactionLink_SequenceNumber', 'TransactionLinkSequenceNumber')
    RetailTransactionHeaderDF = RetailTransactionHeaderDF.withColumnRenamed(
        'RetailTransaction_TransactionLink_BeginDateTime', 'TransactionLinkBeginDateTime')
    RetailTransactionHeaderDF = RetailTransactionHeaderDF.withColumnRenamed(
        'RetailTransaction_TransactionLink_GK_ReceiptNumber', 'TransactionLinkReceiptNumber')
    RetailTransactionHeaderDF = RetailTransactionHeaderDF.withColumnRenamed(
        'RetailTransaction_TransactionLink_GK_ReceiptDateTime', 'TransactionLinkReceiptDateTime')
    RetailTransactionHeaderDF = RetailTransactionHeaderDF.withColumnRenamed(
        'RetailTransaction_TransactionLink__ReasonCode', 'TransactionLinkReasonCode')

    RetailTransactionHeaderDF = RetailTransactionHeaderDF.withColumn("TotalTypeMapping", F.map_from_arrays(
        F.col('RetailTransactionTotalType'), F.col('RetailTransactionTotalValue')))

    RetailTransactionHeaderDF = RetailTransactionHeaderDF.withColumn("TransactionGrandAmount",
                                                                     F.element_at(F.col("TotalTypeMapping"),
                                                                                  "TransactionGrandAmount"))
    RetailTransactionHeaderDF = RetailTransactionHeaderDF.withColumn("TransactionTaxAmount",
                                                                     F.element_at(F.col("TotalTypeMapping"),
                                                                                  "TransactionTaxAmount"))
    RetailTransactionHeaderDF = RetailTransactionHeaderDF.withColumn("NegativeTransactionTaxAmount",
                                                                     F.element_at(F.col("TotalTypeMapping"),
                                                                                  "GK:NegativeTransactionTaxAmount"))

    columns_to_drop = ['RetailTransactionTotalType', 'TotalTypeMapping', 'RetailTransactionTotalValue']
    RetailTransactionHeaderDF = RetailTransactionHeaderDF.drop(*columns_to_drop)

    RetailTransactionHeaderDF = RetailTransactionHeaderDF.filter(F.col("RetailTransactionReceiptDateTime").isNotNull())

    RetailTransactionHeaderDF = RetailTransactionHeaderDF.dropDuplicates(
        ["RetailStoreID", "WorkstationID", "SequenceNumber", "ReceiptDateTime", "ReceiptNumber"])

    RetailTransactionHeaderDF = RetailTransactionHeaderDF.withColumn('Date', F.split('ReceiptDateTime', 'T')[0])

    RetailTransactionHeaderDF = RetailTransactionHeaderDF.withColumn('RetailStoreID',
                                                                     F.col('RetailStoreID').cast('long'))
    RetailTransactionHeaderDF = RetailTransactionHeaderDF.withColumn('WorkstationID',
                                                                     F.col('WorkstationID').cast('long'))
    RetailTransactionHeaderDF = RetailTransactionHeaderDF.withColumn('SequenceNumber',
                                                                     F.col('SequenceNumber').cast('long'))
    RetailTransactionHeaderDF = RetailTransactionHeaderDF.withColumn('ReceiptDateTime',
                                                                     F.col('ReceiptDateTime').cast('string'))
    RetailTransactionHeaderDF = RetailTransactionHeaderDF.withColumn('ReceiptNumber',
                                                                     F.col('ReceiptNumber').cast('long'))
    RetailTransactionHeaderDF = RetailTransactionHeaderDF.withColumn('OutsideSalesFlag',
                                                                     F.col('OutsideSalesFlag').cast('boolean'))
    RetailTransactionHeaderDF = RetailTransactionHeaderDF.withColumn('SplitCheckFlag',
                                                                     F.col('SplitCheckFlag').cast('boolean'))
    RetailTransactionHeaderDF = RetailTransactionHeaderDF.withColumn('RetailTransactionReceiptDateTime',
                                                                     F.col('RetailTransactionReceiptDateTime').cast(
                                                                         'string'))
    RetailTransactionHeaderDF = RetailTransactionHeaderDF.withColumn('AmendmentFlag',
                                                                     F.col('AmendmentFlag').cast('boolean'))
    RetailTransactionHeaderDF = RetailTransactionHeaderDF.withColumn('NegativeTotalFlag',
                                                                     F.col('NegativeTotalFlag').cast('boolean'))
    RetailTransactionHeaderDF = RetailTransactionHeaderDF.withColumn('TransactionLinkRetailStoreID',
                                                                     F.col('TransactionLinkRetailStoreID').cast('long'))
    RetailTransactionHeaderDF = RetailTransactionHeaderDF.withColumn('TransactionLinkWorkstationID',
                                                                     F.col('TransactionLinkWorkstationID').cast('long'))
    RetailTransactionHeaderDF = RetailTransactionHeaderDF.withColumn('TransactionLinkSequenceNumber',
                                                                     F.col('TransactionLinkSequenceNumber').cast(
                                                                         'long'))
    RetailTransactionHeaderDF = RetailTransactionHeaderDF.withColumn('TransactionLinkBeginDateTime',
                                                                     F.col('TransactionLinkBeginDateTime').cast(
                                                                         'string'))
    RetailTransactionHeaderDF = RetailTransactionHeaderDF.withColumn('TransactionLinkReceiptDateTime',
                                                                     F.col('TransactionLinkReceiptDateTime').cast(
                                                                         'string'))
    RetailTransactionHeaderDF = RetailTransactionHeaderDF.withColumn('TransactionLinkReceiptNumber',
                                                                     F.col('TransactionLinkReceiptNumber').cast('long'))
    RetailTransactionHeaderDF = RetailTransactionHeaderDF.withColumn('TransactionGrandAmount',
                                                                     F.col('TransactionGrandAmount').cast('double'))
    RetailTransactionHeaderDF = RetailTransactionHeaderDF.withColumn('TransactionTaxAmount',
                                                                     F.col('TransactionTaxAmount').cast('double'))
    RetailTransactionHeaderDF = RetailTransactionHeaderDF.withColumn('NegativeTransactionTaxAmount',
                                                                     F.col('NegativeTransactionTaxAmount').cast(
                                                                         'double'))

    # RetailTransactionHeaderDF = loadcontrol.join(RetailTransactionHeaderDF, ["RetailStoreID", "Date"])

    # RetailTransactionHeaderDF = RetailTransactionHeaderDF.filter((RetailTransactionHeaderDF.Status == 'A') | (RetailTransactionHeaderDF.Status == 'L') | (RetailTransactionHeaderDF.Status =='E'))

    return RetailTransactionHeaderDF


# -------------------------------------------------------------#
# Creating TenderDF DataFrame
# -------------------------------------------------------------#

def tender_df(logger, loadcontrol, df):
    logger.info("Creating TenderDF")

    TenderDF = df.select('RetailStoreID', 'WorkStationID', 'SequenceNumber', 'GK_ReceiptDateTime', 'GK_ReceiptNumber',
                         'RetailTransaction_LineItem_SequenceNumber', 'RetailTransaction_LineItem_Tender__TenderType',
                         'RetailTransaction_LineItem_Tender__TypeCode', 'RetailTransaction_LineItem_Tender_TenderID',
                         'RetailTransaction_LineItem_Tender_Amount__VALUE',
                         'RetailTransaction_LineItem_Tender_CashBack',
                         'RetailTransaction_LineItem_Tender_TenderChange__TenderType',
                         'RetailTransaction_LineItem_Tender_TenderChange_TenderID',
                         'RetailTransaction_LineItem_Tender_TenderChange_Amount',
                         'RetailTransaction_LineItem_Tender_Authorization__ElectronicSignature',
                         'RetailTransaction_LineItem_Tender_Authorization__ForceOnline',
                         'RetailTransaction_LineItem_Tender_Authorization__HostAuthorized',
                         'RetailTransaction_LineItem_Tender_Authorization_RequestedAmount',
                         'RetailTransaction_LineItem_Tender_Authorization_AuthorizationCode',
                         'RetailTransaction_LineItem_Tender_Authorization_ReferenceNumber',
                         'RetailTransaction_LineItem_Tender_Authorization_MerchantNumber',
                         'RetailTransaction_LineItem_Tender_Authorization_ProviderID',
                         'RetailTransaction_LineItem_Tender_Authorization_AuthorizationDateTime',
                         'RetailTransaction_LineItem_Tender_Authorization_AuthorizingTermID',
                         'RetailTransaction_LineItem_Tender_CreditDebit__CardType',
                         'RetailTransaction_LineItem_Tender_CreditDebit_IssuerIdentificationNumber',
                         'RetailTransaction_LineItem_Tender_CreditDebit_PrimaryAccountNumber',
                         'RetailTransaction_LineItem_Tender_CreditDebit_IssueSequence',
                         'RetailTransaction_LineItem_Tender_CreditDebit_ExpirationDate',
                         'RetailTransaction_LineItem_Tender_CreditDebit_ReconciliationCode',
                         'RetailTransaction_LineItem_Tender_CreditDebit_StartDate',
                         'RetailTransaction_LineItem_Tender_CreditDebit_GK_TerminalReceiptNumber',
                         'RetailTransaction_LineItem_Tender_CreditDebit_GK_TraceNumber',
                         'RetailTransaction_LineItem_Tender_Voucher_SerialNumber')

    TenderDF = TenderDF.withColumnRenamed('GK_ReceiptDateTime', 'ReceiptDateTime')
    TenderDF = TenderDF.withColumnRenamed('GK_ReceiptNumber', 'ReceiptNumber')
    TenderDF = TenderDF.withColumnRenamed('RetailTransaction_LineItem_Tender__TenderType', 'TenderType')
    TenderDF = TenderDF.withColumnRenamed('RetailTransaction_LineItem_SequenceNumber', 'LineItemSequenceNumber')
    TenderDF = TenderDF.withColumnRenamed('RetailTransaction_LineItem_Tender__TypeCode', 'TypeCode')
    TenderDF = TenderDF.withColumnRenamed('RetailTransaction_LineItem_Tender_TenderID', 'TenderID')
    TenderDF = TenderDF.withColumnRenamed('RetailTransaction_LineItem_Tender_Amount__VALUE', 'Amount')
    TenderDF = TenderDF.withColumnRenamed('RetailTransaction_LineItem_Tender_Cashback', 'Cashback')
    TenderDF = TenderDF.withColumnRenamed('RetailTransaction_LineItem_Tender_TenderChange__TenderType',
                                          'TenderChangeType')
    TenderDF = TenderDF.withColumnRenamed('RetailTransaction_LineItem_Tender_TenderChange_TenderID', 'TenderChangeID')
    TenderDF = TenderDF.withColumnRenamed('RetailTransaction_LineItem_Tender_TenderChange_Amount', 'TenderChangeAmount')
    TenderDF = TenderDF.withColumnRenamed('RetailTransaction_LineItem_Tender_Authorization__ElectronicSignature',
                                          'ElectronicSignature')
    TenderDF = TenderDF.withColumnRenamed('RetailTransaction_LineItem_Tender_Authorization__ForceOnline', 'ForceOnline')
    TenderDF = TenderDF.withColumnRenamed('RetailTransaction_LineItem_Tender_Authorization__HostAuthorized',
                                          'HostAuthorized')
    TenderDF = TenderDF.withColumnRenamed('RetailTransaction_LineItem_Tender_Authorization_RequestedAmount',
                                          'RequestedAmount')
    TenderDF = TenderDF.withColumnRenamed('RetailTransaction_LineItem_Tender_Authorization_AuthorizationCode',
                                          'AuthorizationCode')
    TenderDF = TenderDF.withColumnRenamed('RetailTransaction_LineItem_Tender_Authorization_ReferenceNumber',
                                          'ReferenceNumber')
    TenderDF = TenderDF.withColumnRenamed('RetailTransaction_LineItem_Tender_Authorization_MerchantNumber',
                                          'MerchantNumber')
    TenderDF = TenderDF.withColumnRenamed('RetailTransaction_LineItem_Tender_Authorization_ProviderID', 'ProviderID')
    TenderDF = TenderDF.withColumnRenamed('RetailTransaction_LineItem_Tender_Authorization_AuthorizationDateTime',
                                          'AuthorizationDateTime')
    TenderDF = TenderDF.withColumnRenamed('RetailTransaction_LineItem_Tender_Authorization_AuthorizingTermID',
                                          'AuthorizingTermID')
    TenderDF = TenderDF.withColumnRenamed('RetailTransaction_LineItem_Tender_CreditDebit__CardType', 'CardType')
    TenderDF = TenderDF.withColumnRenamed('RetailTransaction_LineItem_Tender_CreditDebit_IssuerIdentificationNumber',
                                          'IssuerIdentificationNumber')
    TenderDF = TenderDF.withColumnRenamed('RetailTransaction_LineItem_Tender_CreditDebit_PrimaryAccountNumber',
                                          'PrimaryAccountNumber')
    TenderDF = TenderDF.withColumnRenamed('RetailTransaction_LineItem_Tender_CreditDebit_IssueSequence',
                                          'IssueSequence')
    TenderDF = TenderDF.withColumnRenamed('RetailTransaction_LineItem_Tender_CreditDebit_ExpirationDate',
                                          'ExpirationDate')
    TenderDF = TenderDF.withColumnRenamed('RetailTransaction_LineItem_Tender_CreditDebit_ReconciliationCode',
                                          'ReconciliationCode')
    TenderDF = TenderDF.withColumnRenamed('RetailTransaction_LineItem_Tender_CreditDebit_StartDate', 'StartDate')
    TenderDF = TenderDF.withColumnRenamed('RetailTransaction_LineItem_Tender_CreditDebit_GK_TerminalReceiptNumber',
                                          'TerminalReceiptNumber')
    TenderDF = TenderDF.withColumnRenamed('RetailTransaction_LineItem_Tender_CreditDebit_GK_TraceNumber', 'TraceNumber')
    TenderDF = TenderDF.withColumnRenamed('RetailTransaction_LineItem_Tender_Voucher_SerialNumber', 'SerialNumber')

    TenderDF = (TenderDF
                .withColumn("tmp",
                            F.arrays_zip('LineItemSequenceNumber', 'TenderType', 'TypeCode', 'TenderID', 'Amount',
                                         'Cashback', 'TenderChangeType', 'TenderChangeID', 'TenderChangeAmount',
                                         'ElectronicSignature', 'ForceOnline', 'HostAuthorized', 'RequestedAmount',
                                         'AuthorizationCode', 'ReferenceNumber', 'MerchantNumber', 'ProviderID',
                                         'AuthorizationDateTime', 'AuthorizingTermID', 'CardType',
                                         'IssuerIdentificationNumber', 'PrimaryAccountNumber', 'IssueSequence',
                                         'ExpirationDate', 'ReconciliationCode', 'StartDate', 'TerminalReceiptNumber',
                                         'TraceNumber', 'SerialNumber'))
                .withColumn("tmp", F.explode("tmp"))
                .select('RetailStoreID', 'WorkstationID', 'SequenceNumber', 'ReceiptDateTime', 'ReceiptNumber',
                        F.col('tmp.LineItemSequenceNumber'), F.col('tmp.TenderType'), F.col('tmp.TenderID'),
                        F.col('tmp.TypeCode'), F.col('tmp.Amount'), F.col('tmp.Cashback'),
                        F.col('tmp.TenderChangeType'), F.col('tmp.TenderChangeID'), F.col('tmp.TenderChangeAmount'),
                        F.col('tmp.ElectronicSignature'), F.col('tmp.ForceOnline'), F.col('tmp.HostAuthorized'),
                        F.col('tmp.RequestedAmount'), F.col('tmp.AuthorizationCode'), F.col('tmp.ReferenceNumber'),
                        F.col('tmp.MerchantNumber'), F.col('tmp.ProviderID'), F.col('tmp.AuthorizationDateTime'),
                        F.col('tmp.AuthorizingTermID'), F.col('tmp.CardType'), F.col('tmp.IssuerIdentificationNumber'),
                        F.col('tmp.PrimaryAccountNumber'), F.col('tmp.IssueSequence'), F.col('tmp.ExpirationDate'),
                        F.col('tmp.ReconciliationCode'), F.col('tmp.StartDate'), F.col('tmp.TerminalReceiptNumber'),
                        F.col('tmp.TraceNumber'), F.col('tmp.SerialNumber')))

    TenderDF = (TenderDF
                .withColumn("tmp", F.arrays_zip('TenderChangeType', 'TenderChangeID'))
                .withColumn("tmp", F.explode_outer("tmp"))
                .select('RetailStoreID', 'WorkstationID', 'SequenceNumber', 'ReceiptDateTime', 'ReceiptNumber',
                        'LineItemSequenceNumber', 'TenderType', 'TenderID', 'TypeCode', 'Amount', 'Cashback',
                        F.col('tmp.TenderChangeType'), F.col('tmp.TenderChangeID'), F.col('TenderChangeAmount'),
                        'ElectronicSignature', 'ForceOnline', 'HostAuthorized', 'RequestedAmount', 'AuthorizationCode',
                        'ReferenceNumber', 'MerchantNumber', 'ProviderID', 'AuthorizationDateTime', 'AuthorizingTermID',
                        'CardType', 'IssuerIdentificationNumber', 'PrimaryAccountNumber', 'IssueSequence',
                        'ExpirationDate', 'ReconciliationCode', 'StartDate', 'TerminalReceiptNumber', 'TraceNumber',
                        'SerialNumber'))

    TenderDF = TenderDF.filter(F.col('TenderID').isNotNull())

    TenderDF = TenderDF.withColumn('RetailStoreID', F.col('RetailStoreID').cast('long'))
    TenderDF = TenderDF.withColumn('WorkstationID', F.col('WorkstationID').cast('long'))
    TenderDF = TenderDF.withColumn('SequenceNumber', F.col('SequenceNumber').cast('long'))
    TenderDF = TenderDF.withColumn('ReceiptDateTime', F.col('ReceiptDateTime').cast('string'))
    TenderDF = TenderDF.withColumn('ReceiptNumber', F.col('ReceiptNumber').cast('long'))
    TenderDF = TenderDF.withColumn('LineItemSequenceNumber', F.col('LineItemSequenceNumber').cast('long'))
    TenderDF = TenderDF.withColumn('Amount', F.col('Amount').cast('double'))
    TenderDF = TenderDF.withColumn('Cashback', F.col('Cashback').cast('double'))
    TenderDF = TenderDF.withColumn('TenderChangeAmount', F.col('TenderChangeAmount').cast('double'))
    TenderDF = TenderDF.withColumn('ElectronicSignature', F.col('ElectronicSignature').cast('boolean'))
    TenderDF = TenderDF.withColumn('ForceOnline', F.col('ForceOnline').cast('boolean'))
    TenderDF = TenderDF.withColumn('HostAuthorized', F.col('HostAuthorized').cast('boolean'))
    TenderDF = TenderDF.withColumn('RequestedAmount', F.col('RequestedAmount').cast('double'))
    TenderDF = TenderDF.withColumn('AuthorizationCode', F.col('AuthorizationCode').cast('long'))
    TenderDF = TenderDF.withColumn('ReferenceNumber', F.col('ReferenceNumber').cast('long'))
    TenderDF = TenderDF.withColumn('MerchantNumber', F.col('MerchantNumber').cast('long'))
    TenderDF = TenderDF.withColumn('ProviderID', F.col('ProviderID').cast('long'))
    TenderDF = TenderDF.withColumn('AuthorizationDateTime', F.col('AuthorizationDateTime').cast('string'))
    TenderDF = TenderDF.withColumn('AuthorizingTermID', F.col('AuthorizingTermID').cast('long'))
    TenderDF = TenderDF.withColumn('IssuerIdentificationNumber', F.col('IssuerIdentificationNumber').cast('long'))
    TenderDF = TenderDF.withColumn('PrimaryAccountNumber', F.col('PrimaryAccountNumber').cast('long'))
    TenderDF = TenderDF.withColumn('IssueSequence', F.col('IssueSequence').cast('long'))
    TenderDF = TenderDF.withColumn('ReconciliationCode', F.col('ReconciliationCode').cast('long'))
    TenderDF = TenderDF.withColumn('TerminalReceiptNumber', F.col('TerminalReceiptNumber').cast('long'))
    TenderDF = TenderDF.withColumn('TraceNumber', F.col('TraceNumber').cast('long'))
    TenderDF = TenderDF.withColumn('SerialNumber', F.col('SerialNumber').cast('long'))

    TenderDF = TenderDF.dropDuplicates(
        ["RetailStoreID", "WorkstationID", "SequenceNumber", "ReceiptDateTime", "ReceiptNumber",
         "LineItemSequenceNumber"])

    TenderDF = TenderDF.withColumn('Date', F.split('ReceiptDateTime', 'T')[0])

    # TenderDF = loadcontrol.join(TenderDF, ["RetailStoreID", "Date"])

    # TenderDF = TenderDF.filter((TenderDF.Status == 'A') | (TenderDF.Status == 'L') | (TenderDF.Status =='E'))

    return TenderDF



# -------------------------------------------------------------#
# Creating TendercontrolloanDF DataFrame
# -------------------------------------------------------------#

def tender_control_loan_df(logger, loadcontrol, df):
    logger.info("Creating TenderControlLoanDF")

    TenderControlLoanDF = df.select('RetailStoreID', 'WorkStationID', 'SequenceNumber', 'GK_ReceiptDateTime',
                                    'GK_ReceiptNumber', 'TenderControlTransaction_TenderLoan__TenderType',
                                    'TenderControlTransaction_TenderLoan_Totals_Amount__VALUE',
                                    'TenderControlTransaction_TenderLoan_Totals_Amount__Currency')

    TenderControlLoanDF = TenderControlLoanDF.withColumnRenamed('GK_ReceiptDateTime', 'ReceiptDateTime')
    TenderControlLoanDF = TenderControlLoanDF.withColumnRenamed('GK_ReceiptNumber', 'ReceiptNumber')
    TenderControlLoanDF = TenderControlLoanDF.withColumnRenamed('TenderControlTransaction_TenderLoan__TenderType',
                                                                'TenderLoanType')
    TenderControlLoanDF = TenderControlLoanDF.withColumnRenamed(
        'TenderControlTransaction_TenderLoan_Totals_Amount__VALUE', 'TenderLoanTotalAmount')
    TenderControlLoanDF = TenderControlLoanDF.withColumnRenamed(
        'TenderControlTransaction_TenderLoan_Totals_Amount__Currency', 'TenderLoanTotalCurrency')

    TenderControlLoanDF = TenderControlLoanDF.filter(F.col('TenderLoanType').isNotNull())
    TenderControlLoanDF = TenderControlLoanDF.filter(F.col('TenderLoanTotalAmount').isNotNull())
    TenderControlLoanDF = TenderControlLoanDF.filter(F.col('TenderLoanTotalCurrency').isNotNull())

    TenderControlLoanDF = TenderControlLoanDF.dropDuplicates(
        ["RetailStoreID", "WorkstationID", "SequenceNumber", "ReceiptDateTime", "ReceiptNumber"])

    TenderControlLoanDF = TenderControlLoanDF.withColumn('RetailStoreID', F.col('RetailStoreID').cast('long'))
    TenderControlLoanDF = TenderControlLoanDF.withColumn('WorkstationID', F.col('WorkstationID').cast('long'))
    TenderControlLoanDF = TenderControlLoanDF.withColumn('SequenceNumber', F.col('SequenceNumber').cast('long'))
    TenderControlLoanDF = TenderControlLoanDF.withColumn('ReceiptDateTime', F.col('ReceiptDateTime').cast('string'))
    TenderControlLoanDF = TenderControlLoanDF.withColumn('ReceiptNumber', F.col('ReceiptNumber').cast('long'))
    TenderControlLoanDF = TenderControlLoanDF.withColumn('TenderLoanTotalAmount',
                                                         F.col('TenderLoanTotalAmount').cast('double'))

    TenderControlLoanDF = TenderControlLoanDF.withColumn('Date', F.split('ReceiptDateTime', 'T')[0])

    # TenderControlLoanDF = loadcontrol.join(TenderControlLoanDF, ["RetailStoreID", "Date"])

    # TenderControlLoanDF = TenderControlLoanDF.filter((TenderControlLoanDF.Status == 'A') | (TenderControlLoanDF.Status == 'L') | (TenderControlLoanDF.Status =='E'))

    return TenderControlLoanDF


# -------------------------------------------------------------#
# Creating TendercontrolpickupDF DataFrame
# -------------------------------------------------------------#

def tender_control_pickup_df(logger, loadcontrol, df):
    logger.info("Creating TenderControlPickupDF")

    TenderControlPickupDF = df.select('RetailStoreID', 'WorkStationID', 'GK_ReceiptNumber', 'SequenceNumber',
                                      'GK_ReceiptDateTime',
                                      'TenderControlTransaction_TenderPickup_Totals__TenderType',
                                      'TenderControlTransaction_TenderPickup_Totals_TenderTotal_Amount__VALUE',
                                      'TenderControlTransaction_TenderPickup_Totals_TenderTotal_Amount__Currency',
                                      'TenderControlTransaction_TenderPickup_Reason',
                                      'TenderControlTransaction_TenderPickup_Totals_TenderTotal_Count', )

    TenderControlPickupDF = TenderControlPickupDF.withColumnRenamed('GK_ReceiptDateTime', 'ReceiptDateTime')
    TenderControlPickupDF = TenderControlPickupDF.withColumnRenamed('GK_ReceiptNumber', 'ReceiptNumber')
    TenderControlPickupDF = TenderControlPickupDF.withColumnRenamed(
        'TenderControlTransaction_TenderPickup_Totals__TenderType', 'TenderType')
    TenderControlPickupDF = TenderControlPickupDF.withColumnRenamed(
        'TenderControlTransaction_TenderPickup_Totals_TenderTotal_Amount__VALUE', 'TenderTotalAmount')
    TenderControlPickupDF = TenderControlPickupDF.withColumnRenamed(
        'TenderControlTransaction_TenderPickup_Totals_TenderTotal_Amount__Currency', 'TenderTotalCurrency')
    TenderControlPickupDF = TenderControlPickupDF.withColumnRenamed('TenderControlTransaction_TenderPickup_Reason',
                                                                    'TenderPickUpReason')
    TenderControlPickupDF = TenderControlPickupDF.withColumnRenamed(
        'TenderControlTransaction_TenderPickup_Totals_TenderTotal_Count', 'TenderTotalCount')

    TenderControlPickupDF = (TenderControlPickupDF
                             .withColumn("tmp", F.arrays_zip('TenderType', 'TenderTotalAmount', 'TenderTotalCurrency',
                                                             'TenderTotalCount'))
                             .withColumn("tmp", F.explode("tmp"))
                             .select('RetailStoreID', 'WorkstationID', 'SequenceNumber', 'ReceiptDateTime',
                                     'ReceiptNumber', 'TenderPickUpReason', F.col('tmp.TenderType'),
                                     F.col('tmp.TenderTotalAmount'), F.col('tmp.TenderTotalCurrency'),
                                     F.col('tmp.TenderTotalCount')))

    TenderControlPickupDF = TenderControlPickupDF.dropDuplicates()

    TenderControlPickupDF = TenderControlPickupDF.withColumn('RetailStoreID', F.col('RetailStoreID').cast('long'))
    TenderControlPickupDF = TenderControlPickupDF.withColumn('WorkstationID', F.col('WorkstationID').cast('long'))
    TenderControlPickupDF = TenderControlPickupDF.withColumn('SequenceNumber', F.col('SequenceNumber').cast('long'))
    TenderControlPickupDF = TenderControlPickupDF.withColumn('ReceiptDateTime', F.col('ReceiptDateTime').cast('string'))
    TenderControlPickupDF = TenderControlPickupDF.withColumn('ReceiptNumber', F.col('ReceiptNumber').cast('long'))
    TenderControlPickupDF = TenderControlPickupDF.withColumn('TenderTotalAmount',
                                                             F.col('TenderTotalAmount').cast('double'))
    TenderControlPickupDF = TenderControlPickupDF.withColumn('TenderTotalCount', F.col('TenderTotalCount').cast('long'))

    TenderControlPickupDF = TenderControlPickupDF.withColumn('Date', F.split('ReceiptDateTime', 'T')[0])

    # TenderControlPickupDF = loadcontrol.join(TenderControlPickupDF, ["RetailStoreID", "Date"])

    # TenderControlPickupDF = TenderControlPickupDF.filter((TenderControlPickupDF.Status == 'A') | (TenderControlPickupDF.Status == 'L') | (TenderControlPickupDF.Status =='E'))

    return TenderControlPickupDF


# -------------------------------------------------------------#
# Creating TendercontrolsafesettleDF DataFrame
# -------------------------------------------------------------#

def tender_control_safe_settle_df(logger, loadcontrol, df):
    logger.info("Creating TenderControlSafeSettleDF")

    TenderControlSafeSettleDF = df.select('RetailStoreID', 'WorkStationID', 'GK_ReceiptNumber', 'SequenceNumber',
                                          'GK_ReceiptDateTime',
                                          'TenderControlTransaction_SafeSettle_TenderCloseBalanceTotalAmount',
                                          'TenderControlTransaction_SafeSettle_TenderOpenBalanceTotalAmount',
                                          'TenderControlTransaction_SafeSettle_TenderLoanTotalAmount',
                                          'TenderControlTransaction_SafeSettle_TotalTenderLoanCount',
                                          'TenderControlTransaction_SafeSettle_TenderPickupTotalAmount',
                                          'TenderControlTransaction_SafeSettle_TotalTenderPickupCount',
                                          'TenderControlTransaction_SafeSettle_TenderDepositTotalAmount',
                                          'TenderControlTransaction_SafeSettle_TenderReceiptTotalAmount', )

    TenderControlSafeSettleDF = TenderControlSafeSettleDF.withColumnRenamed('GK_ReceiptDateTime', 'ReceiptDateTime')
    TenderControlSafeSettleDF = TenderControlSafeSettleDF.withColumnRenamed('GK_ReceiptNumber', 'ReceiptNumber')
    TenderControlSafeSettleDF = TenderControlSafeSettleDF.withColumnRenamed(
        'TenderControlTransaction_SafeSettle_TenderOpenBalanceTotalAmount', 'TenderOpenBalanceTotalAmount')
    TenderControlSafeSettleDF = TenderControlSafeSettleDF.withColumnRenamed(
        'TenderControlTransaction_SafeSettle_TenderCloseBalanceTotalAmount', 'TenderCloseBalanceTotalAmount')
    TenderControlSafeSettleDF = TenderControlSafeSettleDF.withColumnRenamed(
        'TenderControlTransaction_SafeSettle_TenderLoanTotalAmount', 'TenderLoanTotalAmount')
    TenderControlSafeSettleDF = TenderControlSafeSettleDF.withColumnRenamed(
        'TenderControlTransaction_SafeSettle_TotalTenderLoanCount', 'TotalTenderLoanCount')
    TenderControlSafeSettleDF = TenderControlSafeSettleDF.withColumnRenamed(
        'TenderControlTransaction_SafeSettle_TenderPickupTotalAmount', 'TenderPickupTotalAmount')
    TenderControlSafeSettleDF = TenderControlSafeSettleDF.withColumnRenamed(
        'TenderControlTransaction_SafeSettle_TotalTenderPickupCount', 'TotalTenderPickupCount')
    TenderControlSafeSettleDF = TenderControlSafeSettleDF.withColumnRenamed(
        'TenderControlTransaction_SafeSettle_TenderDepositTotalAmount', 'TenderDepositTotalAmount')
    TenderControlSafeSettleDF = TenderControlSafeSettleDF.withColumnRenamed(
        'TenderControlTransaction_SafeSettle_TenderReceiptTotalAmount', 'TenderReceiptTotalAmount')

    TenderControlSafeSettleDF = TenderControlSafeSettleDF.dropDuplicates(
        ["RetailStoreID", "WorkstationID", "SequenceNumber", "ReceiptDateTime", "ReceiptNumber"])

    TenderControlSafeSettleDF = TenderControlSafeSettleDF.filter(F.col("TenderOpenBalanceTotalAmount").isNotNull())
    TenderControlSafeSettleDF = TenderControlSafeSettleDF.filter(F.col("TenderCloseBalanceTotalAmount").isNotNull())
    TenderControlSafeSettleDF = TenderControlSafeSettleDF.filter(F.col("TenderLoanTotalAmount").isNotNull())
    TenderControlSafeSettleDF = TenderControlSafeSettleDF.filter(F.col("TotalTenderLoanCount").isNotNull())
    TenderControlSafeSettleDF = TenderControlSafeSettleDF.filter(F.col("TenderPickupTotalAmount").isNotNull())
    TenderControlSafeSettleDF = TenderControlSafeSettleDF.filter(F.col("TotalTenderPickupCount").isNotNull())
    TenderControlSafeSettleDF = TenderControlSafeSettleDF.filter(F.col("TenderDepositTotalAmount").isNotNull())
    TenderControlSafeSettleDF = TenderControlSafeSettleDF.filter(F.col("TenderReceiptTotalAmount").isNotNull())

    TenderControlSafeSettleDF = TenderControlSafeSettleDF.withColumn('RetailStoreID',
                                                                     F.col('RetailStoreID').cast('long'))
    TenderControlSafeSettleDF = TenderControlSafeSettleDF.withColumn('WorkstationID',
                                                                     F.col('WorkstationID').cast('long'))
    TenderControlSafeSettleDF = TenderControlSafeSettleDF.withColumn('SequenceNumber',
                                                                     F.col('SequenceNumber').cast('long'))
    TenderControlSafeSettleDF = TenderControlSafeSettleDF.withColumn('ReceiptDateTime',
                                                                     F.col('ReceiptDateTime').cast('string'))
    TenderControlSafeSettleDF = TenderControlSafeSettleDF.withColumn('ReceiptNumber',
                                                                     F.col('ReceiptNumber').cast('long'))
    TenderControlSafeSettleDF = TenderControlSafeSettleDF.withColumn('TenderOpenBalanceTotalAmount',
                                                                     F.col('TenderOpenBalanceTotalAmount').cast(
                                                                         'double'))
    TenderControlSafeSettleDF = TenderControlSafeSettleDF.withColumn('TenderCloseBalanceTotalAmount',
                                                                     F.col('TenderCloseBalanceTotalAmount').cast(
                                                                         'double'))
    TenderControlSafeSettleDF = TenderControlSafeSettleDF.withColumn('TenderLoanTotalAmount',
                                                                     F.col('TenderLoanTotalAmount').cast('double'))
    TenderControlSafeSettleDF = TenderControlSafeSettleDF.withColumn('TotalTenderLoanCount',
                                                                     F.col('TotalTenderLoanCount').cast('long'))
    TenderControlSafeSettleDF = TenderControlSafeSettleDF.withColumn('TenderPickupTotalAmount',
                                                                     F.col('TenderPickupTotalAmount').cast('double'))
    TenderControlSafeSettleDF = TenderControlSafeSettleDF.withColumn('TotalTenderPickupCount',
                                                                     F.col('TotalTenderPickupCount').cast('long'))
    TenderControlSafeSettleDF = TenderControlSafeSettleDF.withColumn('TenderDepositTotalAmount',
                                                                     F.col('TenderDepositTotalAmount').cast('double'))
    TenderControlSafeSettleDF = TenderControlSafeSettleDF.withColumn('TenderReceiptTotalAmount',
                                                                     F.col('TenderReceiptTotalAmount').cast('double'))

    TenderControlSafeSettleDF = TenderControlSafeSettleDF.withColumn('Date', F.split('ReceiptDateTime', 'T')[0])

    # TenderControlSafeSettleDF = loadcontrol.join(TenderControlSafeSettleDF, ["RetailStoreID", "Date"])

    # TenderControlSafeSettleDF = TenderControlSafeSettleDF.filter((TenderControlSafeSettleDF.Status == 'A') | (TenderControlSafeSettleDF.Status == 'L') | (TenderControlSafeSettleDF.Status =='E'))

    return TenderControlSafeSettleDF


# -------------------------------------------------------------#
# Creating TendercontroltillsettleDF DataFrame
# -------------------------------------------------------------#

def tender_control_till_settle_df(logger, loadcontrol, df):
    logger.info("Creating TenderControlTillSettleDF")

    TenderControlTillSettleDF = df.select('RetailStoreID', 'WorkStationID', 'GK_ReceiptNumber', 'SequenceNumber',
                                          'GK_ReceiptDateTime',
                                          'TenderControlTransaction_TillSettle_TransactionCount',
                                          'TenderControlTransaction_TillSettle_TotalNetSalesAmount',
                                          'TenderControlTransaction_TillSettle_TotalTaxAmount',
                                          'TenderControlTransaction_TillSettle_TotalGrossSalesExemptTaxAmount',
                                          'TenderControlTransaction_TillSettle_GrossPositiveAmount',
                                          'TenderControlTransaction_TillSettle_GrossNegativeAmount',
                                          'TenderControlTransaction_TillSettle_TenderSummary__TenderType',
                                          'TenderControlTransaction_TillSettle_TenderSummary_Beginning_Amount__VALUE',
                                          'TenderControlTransaction_TillSettle_TenderSummary_Ending_Amount__Currency',
                                          'TenderControlTransaction_TillSettle_TenderSummary_Beginning_Amount__Currency',
                                          'TenderControlTransaction_TillSettle_TenderSummary_Ending_Amount__VALUE',
                                          'TenderControlTransaction_TillSettle_TenderSummary_Over_Amount__VALUE',
                                          'TenderControlTransaction_TillSettle_TenderSummary_Over_Amount__Currency',
                                          'TenderControlTransaction_TillSettle_TotalMeasures_SaleLineItemCount',
                                          'TenderControlTransaction_TillSettle_TotalMeasures_NoSaleTransactionCount',
                                          'TenderControlTransaction_TillSettle_TotalMeasures_LineItemOverrideCount',
                                          'TenderControlTransaction_TillSettle_TotalMeasures_WeightedLineItemCount',
                                          'TenderControlTransaction_TillSettle_TotalMeasures_LineItemKeyedCount',
                                          'TenderControlTransaction_TillSettle_TotalMeasures_LineItemKeyedPercentage',
                                          'TenderControlTransaction_TillSettle_TotalMeasures_LineItemScannedCount',
                                          'TenderControlTransaction_TillSettle_TotalMeasures_LineItemScannedPercentage',
                                          'TenderControlTransaction_TillSettle_TotalMeasures_LineItemOpenDepartmentCount',
                                          'TenderControlTransaction_TillSettle_TotalMeasures_LineItemOpenDepartmentPercentage',
                                          'TenderControlTransaction_TillSettle_TotalMeasures_RingTime',
                                          'TenderControlTransaction_TillSettle_TotalMeasures_IdleTime',
                                          'TenderControlTransaction_TillSettle_TotalMeasures_LockTime',
                                          'TenderControlTransaction_TillSettle_TotalMeasures_TenderTime')

    TenderControlTillSettleDF = TenderControlTillSettleDF.withColumnRenamed('GK_ReceiptDateTime', 'ReceiptDateTime')
    TenderControlTillSettleDF = TenderControlTillSettleDF.withColumnRenamed('GK_ReceiptNumber', 'ReceiptNumber')
    TenderControlTillSettleDF = TenderControlTillSettleDF.withColumnRenamed(
        'TenderControlTransaction_TillSettle_TransactionCount', 'TransactionCount')
    TenderControlTillSettleDF = TenderControlTillSettleDF.withColumnRenamed(
        'TenderControlTransaction_TillSettle_TotalNetSalesAmount', 'TotalNetSalesAmount')
    TenderControlTillSettleDF = TenderControlTillSettleDF.withColumnRenamed(
        'TenderControlTransaction_TillSettle_TotalTaxAmount', 'TotalTaxAmount')
    TenderControlTillSettleDF = TenderControlTillSettleDF.withColumnRenamed(
        'TenderControlTransaction_TillSettle_TotalGrossSalesExemptTaxAmount', 'TotalGrossSalesExemptTaxAmount')
    TenderControlTillSettleDF = TenderControlTillSettleDF.withColumnRenamed(
        'TenderControlTransaction_TillSettle_GrossPositiveAmount', 'GrossPositiveAmount')
    TenderControlTillSettleDF = TenderControlTillSettleDF.withColumnRenamed(
        'TenderControlTransaction_TillSettle_GrossNegativeAmount', 'GrossNegativeAmount')
    TenderControlTillSettleDF = TenderControlTillSettleDF.withColumnRenamed(
        'TenderControlTransaction_TillSettle_TenderSummary__TenderType', 'TenderType')
    TenderControlTillSettleDF = TenderControlTillSettleDF.withColumnRenamed(
        'TenderControlTransaction_TillSettle_TenderSummary_Beginning_Amount__VALUE', 'BeginningAmount')
    TenderControlTillSettleDF = TenderControlTillSettleDF.withColumnRenamed(
        'TenderControlTransaction_TillSettle_TenderSummary_Beginning_Amount__Currency', 'BeginningCurrency')
    TenderControlTillSettleDF = TenderControlTillSettleDF.withColumnRenamed(
        'TenderControlTransaction_TillSettle_TenderSummary_Ending_Amount__VALUE', 'EndingAmount')
    TenderControlTillSettleDF = TenderControlTillSettleDF.withColumnRenamed(
        'TenderControlTransaction_TillSettle_TenderSummary_Ending_Amount__Currency', 'EndingCurrency')
    TenderControlTillSettleDF = TenderControlTillSettleDF.withColumnRenamed(
        'TenderControlTransaction_TillSettle_TenderSummary_Over_Amount__VALUE', 'OverAmount')
    TenderControlTillSettleDF = TenderControlTillSettleDF.withColumnRenamed(
        'TenderControlTransaction_TillSettle_TenderSummary_Over_Amount__Currency', 'OverCurrency')
    TenderControlTillSettleDF = TenderControlTillSettleDF.withColumnRenamed(
        'TenderControlTransaction_TillSettle_TotalMeasures_SaleLineItemCount', 'SaleLineItemCount')
    TenderControlTillSettleDF = TenderControlTillSettleDF.withColumnRenamed(
        'TenderControlTransaction_TillSettle_TotalMeasures_NoSaleTransactionCount', 'NoSaleTransactionCount')
    TenderControlTillSettleDF = TenderControlTillSettleDF.withColumnRenamed(
        'TenderControlTransaction_TillSettle_TotalMeasures_LineItemOverrideCount', 'LineItemOverrideCount')
    TenderControlTillSettleDF = TenderControlTillSettleDF.withColumnRenamed(
        'TenderControlTransaction_TillSettle_TotalMeasures_WeightedLineItemCount', 'WeightedLineItemCount')
    TenderControlTillSettleDF = TenderControlTillSettleDF.withColumnRenamed(
        'TenderControlTransaction_TillSettle_TotalMeasures_LineItemKeyedCount', 'LineItemKeyedCount')
    TenderControlTillSettleDF = TenderControlTillSettleDF.withColumnRenamed(
        'TenderControlTransaction_TillSettle_TotalMeasures_LineItemKeyedPercentage', 'LineItemKeyedPercentage')
    TenderControlTillSettleDF = TenderControlTillSettleDF.withColumnRenamed(
        'TenderControlTransaction_TillSettle_TotalMeasures_LineItemScannedCount', 'LineItemScannedCount')
    TenderControlTillSettleDF = TenderControlTillSettleDF.withColumnRenamed(
        'TenderControlTransaction_TillSettle_TotalMeasures_LineItemScannedPercentage', 'LineItemScannedPercentage')
    TenderControlTillSettleDF = TenderControlTillSettleDF.withColumnRenamed(
        'TenderControlTransaction_TillSettle_TotalMeasures_LineItemOpenDepartmentCount', 'LineItemOpenDepartmentCount')
    TenderControlTillSettleDF = TenderControlTillSettleDF.withColumnRenamed(
        'TenderControlTransaction_TillSettle_TotalMeasures_LineItemOpenDepartmentPercentage',
        'LineItemOpenDepartmentPercentage')
    TenderControlTillSettleDF = TenderControlTillSettleDF.withColumnRenamed(
        'TenderControlTransaction_TillSettle_TotalMeasures_RingTime', 'RingTime')
    TenderControlTillSettleDF = TenderControlTillSettleDF.withColumnRenamed(
        'TenderControlTransaction_TillSettle_TotalMeasures_IdleTime', 'IdleTime')
    TenderControlTillSettleDF = TenderControlTillSettleDF.withColumnRenamed(
        'TenderControlTransaction_TillSettle_TotalMeasures_LockTime', 'LockTime')
    TenderControlTillSettleDF = TenderControlTillSettleDF.withColumnRenamed(
        'TenderControlTransaction_TillSettle_TotalMeasures_TenderTime', 'TenderTime')

    TenderControlTillSettleDF = (TenderControlTillSettleDF.withColumn("tmp",
                                                                      F.arrays_zip('TenderType', 'BeginningAmount',
                                                                                   'BeginningCurrency', 'EndingAmount',
                                                                                   'EndingCurrency', 'OverAmount',
                                                                                   'OverCurrency')).withColumn("tmp",
                                                                                                               F.explode_outer(
                                                                                                                   "tmp")))

    TenderControlTillSettleDF = (TenderControlTillSettleDF
                                 .select('RetailStoreID', 'WorkstationID', 'SequenceNumber', 'ReceiptDateTime',
                                         'ReceiptNumber', 'TransactionCount', 'TotalNetSalesAmount', 'TotalTaxAmount',
                                         'TotalGrossSalesExemptTaxAmount', 'GrossPositiveAmount', 'GrossNegativeAmount',
                                         F.col('tmp.TenderType'), F.col('tmp.BeginningAmount'),
                                         F.col('tmp.BeginningCurrency'), F.col('tmp.EndingAmount'),
                                         F.col('tmp.EndingCurrency'), F.col('tmp.OverAmount'),
                                         F.col('tmp.OverCurrency'), 'SaleLineItemCount', 'NoSaleTransactionCount',
                                         'LineItemOverrideCount', 'WeightedLineItemCount', 'LineItemKeyedCount',
                                         'LineItemKeyedPercentage', 'LineItemScannedCount', 'LineItemScannedPercentage',
                                         'LineItemOpenDepartmentCount', 'LineItemOpenDepartmentPercentage', 'RingTime',
                                         'IdleTime', 'LockTime', 'TenderTime'))

    TenderControlTillSettleDF = TenderControlTillSettleDF.filter(F.col("TransactionCount").isNotNull())

    TenderControlTillSettleDF = TenderControlTillSettleDF.dropDuplicates()

    TenderControlTillSettleDF = TenderControlTillSettleDF.withColumn('RetailStoreID',
                                                                     F.col('RetailStoreID').cast('long'))
    TenderControlTillSettleDF = TenderControlTillSettleDF.withColumn('WorkstationID',
                                                                     F.col('WorkstationID').cast('long'))
    TenderControlTillSettleDF = TenderControlTillSettleDF.withColumn('SequenceNumber',
                                                                     F.col('SequenceNumber').cast('long'))
    TenderControlTillSettleDF = TenderControlTillSettleDF.withColumn('ReceiptDateTime',
                                                                     F.col('ReceiptDateTime').cast('string'))
    TenderControlTillSettleDF = TenderControlTillSettleDF.withColumn('ReceiptNumber',
                                                                     F.col('ReceiptNumber').cast('long'))
    TenderControlTillSettleDF = TenderControlTillSettleDF.withColumn('TransactionCount',
                                                                     F.col('TransactionCount').cast('long'))
    TenderControlTillSettleDF = TenderControlTillSettleDF.withColumn('TotalNetSalesAmount',
                                                                     F.col('TotalNetSalesAmount').cast('double'))
    TenderControlTillSettleDF = TenderControlTillSettleDF.withColumn('TotalTaxAmount',
                                                                     F.col('TotalTaxAmount').cast('double'))
    TenderControlTillSettleDF = TenderControlTillSettleDF.withColumn('TotalGrossSalesExemptTaxAmount',
                                                                     F.col('TotalGrossSalesExemptTaxAmount').cast(
                                                                         'double'))
    TenderControlTillSettleDF = TenderControlTillSettleDF.withColumn('GrossPositiveAmount',
                                                                     F.col('GrossPositiveAmount').cast('double'))
    TenderControlTillSettleDF = TenderControlTillSettleDF.withColumn('GrossNegativeAmount',
                                                                     F.col('GrossNegativeAmount').cast('double'))
    TenderControlTillSettleDF = TenderControlTillSettleDF.withColumn('BeginningAmount',
                                                                     F.col('BeginningAmount').cast('double'))
    TenderControlTillSettleDF = TenderControlTillSettleDF.withColumn('EndingAmount',
                                                                     F.col('EndingAmount').cast('double'))
    TenderControlTillSettleDF = TenderControlTillSettleDF.withColumn('OverAmount', F.col('OverAmount').cast('double'))
    TenderControlTillSettleDF = TenderControlTillSettleDF.withColumn('SaleLineItemCount',
                                                                     F.col('SaleLineItemCount').cast('long'))
    TenderControlTillSettleDF = TenderControlTillSettleDF.withColumn('NoSaleTransactionCount',
                                                                     F.col('NoSaleTransactionCount').cast('long'))
    TenderControlTillSettleDF = TenderControlTillSettleDF.withColumn('LineItemOverrideCount',
                                                                     F.col('LineItemOverrideCount').cast('long'))
    TenderControlTillSettleDF = TenderControlTillSettleDF.withColumn('WeightedLineItemCount',
                                                                     F.col('WeightedLineItemCount').cast('long'))
    TenderControlTillSettleDF = TenderControlTillSettleDF.withColumn('LineItemKeyedCount',
                                                                     F.col('LineItemKeyedCount').cast('long'))
    TenderControlTillSettleDF = TenderControlTillSettleDF.withColumn('LineItemKeyedPercentage',
                                                                     F.col('LineItemKeyedPercentage').cast('double'))
    TenderControlTillSettleDF = TenderControlTillSettleDF.withColumn('LineItemScannedCount',
                                                                     F.col('LineItemScannedCount').cast('long'))
    TenderControlTillSettleDF = TenderControlTillSettleDF.withColumn('LineItemScannedPercentage',
                                                                     F.col('LineItemScannedPercentage').cast('double'))
    TenderControlTillSettleDF = TenderControlTillSettleDF.withColumn('LineItemOpenDepartmentCount',
                                                                     F.col('LineItemOpenDepartmentCount').cast('long'))
    TenderControlTillSettleDF = TenderControlTillSettleDF.withColumn('LineItemOpenDepartmentPercentage',
                                                                     F.col('LineItemOpenDepartmentPercentage').cast(
                                                                         'double'))
    TenderControlTillSettleDF = TenderControlTillSettleDF.withColumn('RingTime', F.col('RingTime').cast('double'))
    TenderControlTillSettleDF = TenderControlTillSettleDF.withColumn('IdleTime', F.col('IdleTime').cast('double'))
    TenderControlTillSettleDF = TenderControlTillSettleDF.withColumn('LockTime', F.col('LockTime').cast('double'))
    TenderControlTillSettleDF = TenderControlTillSettleDF.withColumn('TenderTime', F.col('TenderTime').cast('double'))

    TenderControlTillSettleDF = TenderControlTillSettleDF.withColumn('Date', F.split('ReceiptDateTime', 'T')[0])

    # TenderControlTillSettleDF = loadcontrol.join(TenderControlTillSettleDF, ["RetailStoreID", "Date"])

    # TenderControlTillSettleDF = TenderControlTillSettleDF.filter((TenderControlTillSettleDF.Status == 'A') | (TenderControlTillSettleDF.Status == 'L') | (TenderControlTillSettleDF.Status =='E'))

    return TenderControlTillSettleDF


# -------------------------------------------------------------#
# Creating Tendercontrolpaymentdeposit DataFrame
# -------------------------------------------------------------#

def tender_payment_deposit_df(logger, loadcontrol, df, config_dict):
    logger.info("creating TenderControlPaymentDepositDF")

    TenderControlPaymentDepositDF = df.filter(F.col("TenderControlTransaction__Version").isNotNull())

    TenderControlPaymentDepositDF = TenderControlPaymentDepositDF.select('RetailStoreID', 'WorkStationID',
                                                                         'GK_ReceiptNumber', 'SequenceNumber',
                                                                         'GK_ReceiptDateTime',
                                                                         'TenderControlTransaction_GK_PaidIn__TenderType',
                                                                         'TenderControlTransaction_GK_PaidIn__TypeCode',
                                                                         'TenderControlTransaction_GK_PaidIn_Amount__VALUE',
                                                                         'TenderControlTransaction_GK_PaidIn_Amount__Currency',
                                                                         'TenderControlTransaction_GK_PaidIn_Count',
                                                                         'TenderControlTransaction_GK_PaidIn_Reason',
                                                                         'TenderControlTransaction_PaidOut_Amount__Currency',
                                                                         'TenderControlTransaction_PaidOut_Amount__VALUE',
                                                                         'TenderControlTransaction_Deposit_Account',
                                                                         'TenderControlTransaction_Deposit_Amount',
                                                                         'TenderControlTransaction_Deposit_BagID',
                                                                         'TenderControlTransaction_Deposit_Description',
                                                                         'TenderControlTransaction_Deposit_Depositor__EmployeeID',
                                                                         'TenderControlTransaction_Deposit_DepositDetail__TenderType',
                                                                         'TenderControlTransaction_Deposit_DepositDetail_TenderTotal_Amount', )

    TenderControlPaymentDepositDF = TenderControlPaymentDepositDF.withColumnRenamed('GK_ReceiptDateTime',
                                                                                    'ReceiptDateTime')
    TenderControlPaymentDepositDF = TenderControlPaymentDepositDF.withColumnRenamed('GK_ReceiptNumber', 'ReceiptNumber')
    TenderControlPaymentDepositDF = TenderControlPaymentDepositDF.withColumnRenamed(
        'TenderControlTransaction_GK_PaidIn__TenderType', 'PaidInTenderType')
    TenderControlPaymentDepositDF = TenderControlPaymentDepositDF.withColumnRenamed(
        'TenderControlTransaction_GK_PaidIn__TypeCode', 'PaidInTypeCode')
    TenderControlPaymentDepositDF = TenderControlPaymentDepositDF.withColumnRenamed(
        'TenderControlTransaction_GK_PaidIn_Amount__VALUE', 'PaidInAmount')
    TenderControlPaymentDepositDF = TenderControlPaymentDepositDF.withColumnRenamed(
        'TenderControlTransaction_GK_PaidIn_Amount__Currency', 'PaidInCurrency')
    TenderControlPaymentDepositDF = TenderControlPaymentDepositDF.withColumnRenamed(
        'TenderControlTransaction_GK_PaidIn_Count', 'PaidInCount')
    TenderControlPaymentDepositDF = TenderControlPaymentDepositDF.withColumnRenamed(
        'TenderControlTransaction_GK_PaidIn_Reason', 'PaidInReason')
    TenderControlPaymentDepositDF = TenderControlPaymentDepositDF.withColumnRenamed(
        'TenderControlTransaction_PaidOut_Amount__VALUE', 'PaidOutAmount')
    TenderControlPaymentDepositDF = TenderControlPaymentDepositDF.withColumnRenamed(
        'TenderControlTransaction_PaidOut_Amount__Currency', 'PaidOutCurrency')
    TenderControlPaymentDepositDF = TenderControlPaymentDepositDF.withColumnRenamed(
        'TenderControlTransaction_PaidOut_Count', 'PaidOutCount')
    TenderControlPaymentDepositDF = TenderControlPaymentDepositDF.withColumnRenamed(
        'TenderControlTransaction_PaidOut_Reason', 'PaidOutReason')
    TenderControlPaymentDepositDF = TenderControlPaymentDepositDF.withColumnRenamed(
        'TenderControlTransaction_Deposit_Bank', 'Bank')
    TenderControlPaymentDepositDF = TenderControlPaymentDepositDF.withColumnRenamed(
        'TenderControlTransaction_Deposit_Account', 'Account')
    TenderControlPaymentDepositDF = TenderControlPaymentDepositDF.withColumnRenamed(
        'TenderControlTransaction_Deposit_BagID', 'BagID')
    TenderControlPaymentDepositDF = TenderControlPaymentDepositDF.withColumnRenamed(
        'TenderControlTransaction_Deposit_Amount', 'Amount')
    TenderControlPaymentDepositDF = TenderControlPaymentDepositDF.withColumnRenamed(
        'TenderControlTransaction_Deposit_Description', 'Description')
    TenderControlPaymentDepositDF = TenderControlPaymentDepositDF.withColumnRenamed(
        'TenderControlTransaction_Deposit_Depositor__EmployeeID', 'EmployeeID')
    TenderControlPaymentDepositDF = TenderControlPaymentDepositDF.withColumnRenamed(
        'TenderControlTransaction_Deposit_DepositDetail__TenderType', 'DepositTenderType')
    TenderControlPaymentDepositDF = TenderControlPaymentDepositDF.withColumnRenamed(
        'TenderControlTransaction_Deposit_DepositDetail_TenderTotal_Amount', 'TenderTotalAmount')

    TenderControlPaymentDepositDF = TenderControlPaymentDepositDF.withColumn("tmp", F.arrays_zip('PaidInTenderType',
                                                                                                 'PaidInTypeCode',
                                                                                                 'PaidInAmount',
                                                                                                 'PaidInCurrency',
                                                                                                 'PaidInCount',
                                                                                                 'PaidInReason')).withColumn(
        "tmp", F.explode_outer("tmp")).select('RetailStoreID', 'WorkStationID', 'SequenceNumber', 'ReceiptNumber',
                                              'ReceiptDateTime', 'tmp.PaidInTenderType', 'tmp.PaidInTypeCode',
                                              'tmp.PaidInAmount', 'tmp.PaidInCurrency', 'tmp.PaidInCount',
                                              'tmp.PaidInReason', 'PaidOutCurrency', 'PaidOutAmount', 'Account',
                                              'Amount', 'BagID', 'Description', 'EmployeeID', 'DepositTenderType',
                                              'TenderTotalAmount')

    TenderControlPaymentDepositDF = TenderControlPaymentDepositDF.dropDuplicates(
        ["RetailStoreID", "WorkstationID", "SequenceNumber", "ReceiptDateTime", "ReceiptNumber"])

    TenderControlPaymentDepositDF = TenderControlPaymentDepositDF.withColumn('RetailStoreID',
                                                                             F.col('RetailStoreID').cast('long'))
    TenderControlPaymentDepositDF = TenderControlPaymentDepositDF.withColumn('WorkstationID',
                                                                             F.col('WorkstationID').cast('long'))
    TenderControlPaymentDepositDF = TenderControlPaymentDepositDF.withColumn('SequenceNumber',
                                                                             F.col('SequenceNumber').cast('long'))
    TenderControlPaymentDepositDF = TenderControlPaymentDepositDF.withColumn('ReceiptDateTime',
                                                                             F.col('ReceiptDateTime').cast('string'))
    TenderControlPaymentDepositDF = TenderControlPaymentDepositDF.withColumn('ReceiptNumber',
                                                                             F.col('ReceiptNumber').cast('long'))
    TenderControlPaymentDepositDF = TenderControlPaymentDepositDF.withColumn('PaidInAmount',
                                                                             F.col('PaidInAmount').cast('double'))
    TenderControlPaymentDepositDF = TenderControlPaymentDepositDF.withColumn('PaidInCount',
                                                                             F.col('PaidInCount').cast('long'))
    TenderControlPaymentDepositDF = TenderControlPaymentDepositDF.withColumn('PaidOutAmount',
                                                                             F.col('PaidOutAmount').cast('double'))
    TenderControlPaymentDepositDF = TenderControlPaymentDepositDF.withColumn('EmployeeID',
                                                                             F.col('EmployeeID').cast('long'))
    TenderControlPaymentDepositDF = TenderControlPaymentDepositDF.withColumn('Amount', F.col('Amount').cast('double'))
    TenderControlPaymentDepositDF = TenderControlPaymentDepositDF.withColumn('EmployeeID',
                                                                             F.col('EmployeeID').cast('long'))
    TenderControlPaymentDepositDF = TenderControlPaymentDepositDF.withColumn('TenderTotalAmount',
                                                                             F.col('TenderTotalAmount').cast('double'))

    if (config_dict['employeeid_encryption'] == True):
        TenderControlPaymentDepositDF = TenderControlPaymentDepositDF.withColumn('EmployeeID',
                                                                                 TenderControlPaymentDepositDF[
                                                                                     'EmployeeID'].bitwiseXOR(
                                                                                     config_dict['encryption_key']))

    TenderControlPaymentDepositDF = TenderControlPaymentDepositDF.withColumn('Date', F.split('ReceiptDateTime', 'T')[0])

    # TenderControlPaymentDepositDF = loadcontrol.join(TenderControlPaymentDepositDF, ["RetailStoreID", "Date"])

    # TenderControlPaymentDepositDF = TenderControlPaymentDepositDF.filter((TenderControlPaymentDepositDF.Status == 'A') | (TenderControlPaymentDepositDF.Status == 'L') | (TenderControlPaymentDepositDF.Status =='E'))

    return TenderControlPaymentDepositDF


# -------------------------------------------------------------#
# Creating salereturn_rebate DataFrame
# -------------------------------------------------------------#

def salereturn_rebate_df(logger, loadcontrol, df):
    logger.info('Creating SaleReturnRebate')

    Return_rebate = df.select('RetailStoreID', 'WorkStationID', 'GK_ReceiptNumber', 'SequenceNumber',
                              'GK_ReceiptDateTime',
                              'RetailTransaction_LineItem_SequenceNumber',
                              'RetailTransaction_LineItem_Return_RetailPriceModifier_SequenceNumber',
                              'RetailTransaction_LineItem_Return_RetailPriceModifier_NewPrice',
                              'RetailTransaction_LineItem_Return_RetailPriceModifier_PriceDerivationRule_GK_RuleDescription')

    Return_rebate = Return_rebate.withColumnRenamed('GK_ReceiptNumber', 'ReceiptNumber')
    Return_rebate = Return_rebate.withColumnRenamed('GK_ReceiptDateTime', 'ReceiptDateTime')
    Return_rebate = Return_rebate.withColumnRenamed('RetailTransaction_LineItem_SequenceNumber',
                                                    'LineItemSequenceNumber')
    Return_rebate = Return_rebate.withColumnRenamed(
        'RetailTransaction_LineItem_Return_RetailPriceModifier_SequenceNumber', 'RetailPriceModifierSequenceNumber')
    Return_rebate = Return_rebate.withColumnRenamed('RetailTransaction_LineItem_Return_RetailPriceModifier_NewPrice',
                                                    'NewPrice')
    Return_rebate = Return_rebate.withColumnRenamed(
        'RetailTransaction_LineItem_Return_RetailPriceModifier_PriceDerivationRule_GK_RuleDescription',
        'RuleDescription')

    Return_rebate = (Return_rebate.withColumn("tmp", F.arrays_zip('LineItemSequenceNumber',
                                                                  'RetailPriceModifierSequenceNumber', 'NewPrice',
                                                                  'RuleDescription'))
                     .withColumn("tmp", F.explode("tmp"))
                     .select('RetailStoreID', 'WorkStationID', 'ReceiptNumber', 'ReceiptDateTime', 'SequenceNumber',
                             'tmp.LineItemSequenceNumber',
                             'tmp.RetailPriceModifierSequenceNumber', 'tmp.NewPrice', 'tmp.RuleDescription'))

    Return_rebate = (Return_rebate.withColumn("tmp", F.arrays_zip('RetailPriceModifierSequenceNumber', 'NewPrice',
                                                                  'RuleDescription'))
                     .withColumn("tmp", F.explode("tmp"))
                     .select('RetailStoreID', 'WorkStationID', 'ReceiptNumber', 'ReceiptDateTime', 'SequenceNumber',
                             'LineItemSequenceNumber',
                             'tmp.RetailPriceModifierSequenceNumber', 'tmp.NewPrice', 'tmp.RuleDescription'))

    Return_rebate = Return_rebate.withColumn('SaleReturnFlag', F.lit(0))

    Return_rebate = Return_rebate.select('RetailStoreID', 'WorkStationID', 'ReceiptNumber', 'ReceiptDateTime',
                                         'SequenceNumber', 'LineItemSequenceNumber',
                                         'RetailPriceModifierSequenceNumber', 'NewPrice', 'RuleDescription',
                                         'SaleReturnFlag')

    Sale_rebate = df.select('RetailStoreID', 'WorkStationID', 'GK_ReceiptNumber', 'SequenceNumber',
                            'GK_ReceiptDateTime',
                            'RetailTransaction_LineItem_SequenceNumber',
                            'RetailTransaction_LineItem_Sale_RetailPriceModifier_SequenceNumber',
                            'RetailTransaction_LineItem_Sale_RetailPriceModifier_NewPrice',
                            'RetailTransaction_LineItem_Sale_RetailPriceModifier_PriceDerivationRule_GK_RuleDescription')

    Sale_rebate = Sale_rebate.withColumnRenamed('GK_ReceiptNumber', 'ReceiptNumber')
    Sale_rebate = Sale_rebate.withColumnRenamed('GK_ReceiptDateTime', 'ReceiptDateTime')
    Sale_rebate = Sale_rebate.withColumnRenamed('RetailTransaction_LineItem_SequenceNumber', 'LineItemSequenceNumber')
    Sale_rebate = Sale_rebate.withColumnRenamed('RetailTransaction_LineItem_Sale_RetailPriceModifier_SequenceNumber',
                                                'RetailPriceModifierSequenceNumber')
    Sale_rebate = Sale_rebate.withColumnRenamed('RetailTransaction_LineItem_Sale_RetailPriceModifier_NewPrice',
                                                'NewPrice')
    Sale_rebate = Sale_rebate.withColumnRenamed(
        'RetailTransaction_LineItem_Sale_RetailPriceModifier_PriceDerivationRule_GK_RuleDescription', 'RuleDescription')

    Sale_rebate = (Sale_rebate.withColumn("tmp",
                                          F.arrays_zip('LineItemSequenceNumber', 'RetailPriceModifierSequenceNumber',
                                                       'NewPrice', 'RuleDescription'))
                   .withColumn("tmp", F.explode("tmp"))
                   .select('RetailStoreID', 'WorkStationID', 'ReceiptNumber', 'ReceiptDateTime', 'SequenceNumber',
                           'tmp.LineItemSequenceNumber',
                           'tmp.RetailPriceModifierSequenceNumber', 'tmp.NewPrice', 'tmp.RuleDescription'))

    Sale_rebate = (
        Sale_rebate.withColumn("tmp", F.arrays_zip('RetailPriceModifierSequenceNumber', 'NewPrice', 'RuleDescription'))
        .withColumn("tmp", F.explode("tmp"))
        .select('RetailStoreID', 'WorkStationID', 'ReceiptNumber', 'ReceiptDateTime', 'SequenceNumber',
                'LineItemSequenceNumber',
                'tmp.RetailPriceModifierSequenceNumber', 'tmp.NewPrice', 'tmp.RuleDescription'))

    Sale_rebate = Sale_rebate.withColumn('SaleReturnFlag', F.lit(1))

    Sale_rebate = Sale_rebate.select('RetailStoreID', 'WorkStationID', 'ReceiptNumber', 'ReceiptDateTime',
                                     'SequenceNumber', 'LineItemSequenceNumber', 'RetailPriceModifierSequenceNumber',
                                     'NewPrice', 'RuleDescription', 'SaleReturnFlag')

    merged_rebate = Return_rebate.union(Sale_rebate)

    merged_rebate = merged_rebate.dropDuplicates(
        ['RetailStoreID', 'WorkstationID', 'ReceiptDateTime', 'ReceiptNumber', 'LineItemSequenceNumber',
         'RetailPriceModifierSequenceNumber'])

    merged_rebate = merged_rebate.withColumn('RetailStoreID', F.col('RetailStoreID').cast('long'))
    merged_rebate = merged_rebate.withColumn('WorkstationID', F.col('WorkstationID').cast('long'))
    merged_rebate = merged_rebate.withColumn('SequenceNumber', F.col('SequenceNumber').cast('long'))
    merged_rebate = merged_rebate.withColumn('ReceiptDateTime', F.col('ReceiptDateTime').cast('string'))
    merged_rebate = merged_rebate.withColumn('ReceiptNumber', F.col('ReceiptNumber').cast('long'))
    merged_rebate = merged_rebate.withColumn('NewPrice', F.col('NewPrice').cast('double'))
    merged_rebate = merged_rebate.withColumn('LineItemSequenceNumber', F.col('LineItemSequenceNumber').cast('long'))
    merged_rebate = merged_rebate.withColumn('RetailPriceModifierSequenceNumber',
                                             F.col('RetailPriceModifierSequenceNumber').cast('long'))

    return merged_rebate


# -------------------------------------------------------------#
# Creating merchandise_hierarchy_level DataFrame
# -------------------------------------------------------------#

def merchandise_hierarchy_level(spark, logger, df_mhl_edeka, df_mhl_ebus):
    logger.info('Creating merchandise hierarchy level')

    df_mhl_edeka = df_mhl_edeka.selectExpr('CAST(Warengruppe AS int) as MerchandiseHierarchyLevel',
                                           'char255 as DescriptionDE')
    df_mhl_edeka = df_mhl_edeka.withColumn('IsEBUS', F.lit(0))

    df_mhl_ebus = df_mhl_ebus.selectExpr('CAST(Warengruppe AS int) as MerchandiseHierarchyLevel',
                                         'char255 as DescriptionDE')
    df_mhl_ebus = df_mhl_ebus.withColumn('IsEBUS', F.lit(1))

    edeka_ebus_mhl = df_mhl_edeka.unionAll(df_mhl_ebus)

    return edeka_ebus_mhl


# -------------------------------------------------------------#
# Creating merchandise_hierarchy_level DataFrame
# -------------------------------------------------------------#

def article_description(spark, logger, df_article_description):
    logger.info('Creating article description')

    df_article_description = df_article_description.selectExpr(
        'CAST(Artikelnummer AS bigint) AS SAP_ID',
        '`Mengeneinheit für Anzeige` AS Unit',
        'CAST(`Global Trade Item Number (GTIN)` AS bigint) AS EAN',
        '`Kennzeichen Haupt-GTIN` AS IsMainEAN',
        'Artikelkurztext AS Description',
        'CAST(LEFT(`CCG-Klassifikation`, 2) AS SMALLINT) AS MerchandiseHierarchyLevel_EDEKA',
        'CAST(`Externe Warengruppe` AS SMALLINT) AS MerchandiseHierarchyLevel_EBUS'
    )

    df_article_description.registerTempTable('df_articles')
    df_article_description = spark.sql(
        "select SAP_ID, EAN, Unit, IsMainEAN, Description, MerchandiseHierarchyLevel_EDEKA, "
        "MerchandiseHierarchyLevel_EBUS from df_articles")

    return df_article_description
  
    
# -------------------------------------------------------------#
# Creating merchandise_hierarchy_level DataFrame for Cloud
# -------------------------------------------------------------#

def mdarticle_description(spark, logger, df_article_description):
    logger.info('Creating article description for cloud')
    
    df_article_description_material = spark.sql("""select MaterialID,MaterialGroup1 from qualified.dfq_t_ezbi_material""")
    df_article_description = df_article_description.join(df_article_description_material,['MaterialID'])

    df_article_description = df_article_description.withColumn("IsMainEAN", F.lit("")) 
    df_article_description = df_article_description.withColumn("MerchandiseHierarchyLevel_EBUS", F.lit("0")) 
    df_article_description = df_article_description.withColumn("MerchandiseHierarchyLevel_EBUS",df_article_description["MerchandiseHierarchyLevel_EBUS"].cast(IntegerType()))
    df_article_description = df_article_description.selectExpr(
        'CAST(MaterialID AS bigint) AS SAP_ID',
        'BaseUOM AS Unit',
        'CAST(GTIN AS bigint) AS EAN',
        'IsMainEAN AS IsMainEAN',
        'GTINDesc AS Description',
        'CAST(LEFT(MaterialGroup1, 2) AS SMALLINT) AS MerchandiseHierarchyLevel_EDEKA',
        'CAST(MerchandiseHierarchyLevel_EBUS AS SMALLINT) AS MerchandiseHierarchyLevel_EBUS'
    )
    
    df_article_description.registerTempTable('df_articles')
    df_article_description = spark.sql(
        "select SAP_ID, EAN, Unit, IsMainEAN, Description, MerchandiseHierarchyLevel_EDEKA, "
        "MerchandiseHierarchyLevel_EBUS from df_articles")

    return df_article_description
