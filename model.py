from db import SQLServer
import configparser,logging
from itertools import islice
config = configparser.ConfigParser()
from datetime import datetime
config.read(r'settings/config.txt')

# Create and configure logger
logging.basicConfig(filename="Logs/unoLog/logs.log",
                    format='%(asctime)s %(message)s',
                    filemode='w')
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

config = configparser.ConfigParser()
config.read(r'settings/config.txt') 

bulklimit =  config.get('mall_config', 'bulk_limit')

if bulklimit == "":
    bulklimit=100
else:
   bulklimit = int(bulklimit)

db = SQLServer()       
class TaskModel:

    def getSyncTable(self):
        try:
            sql=f"SELECT id, table_name, FORMAT((start_recordDT), 'yyyy-MM-dd hh:mm:ss') as startdate, FORMAT((end_recordDT), 'yyyy-MM-dd hh:mm:ss') as enddate from sync_table where status=0;"
            return db.fetchAll(sql)
        except Exception as e:
            logger.exception("Exception occurred: %s", str(e))
    def perSummaryTable(self, tablename, start_date, end_date):
        try:
            query=''
            if tablename=='accounting_report_summary':
                query = f"SELECT TOP {bulklimit} CCCODE, TRN_DATE, TER_NO, MERCHANT_NAME, BRN_CODE, BRN_DESC, MALL_AREA_CODE, MALL_AREA_DESCRIPTION, BUILDING_CODE, BUILDING_DESCRIPTION, MF_CODE, MF_DESCRIPTION, STORE_NAME, OLD_GRNTOT, NEW_GRNTOT, GROSS_SLS, DISCOUNTS, REFUND_AMT, VOID_AMNT, SCHRGE_AMT, VAT_AMNT, LOCAL_TAX, VATEXEMPT_SLS, NETSALES, CASH_SLS, OTHER_SLS, CHECK_SLS, GC_SLS, MASTERCARD_SLS, VISA_SLS, AMEX_SLS, DINERS_SLS, JCB_SLS, GCASH_SLS, PAYMAYA_SLS, ALIPAY_SLS, WECHAT_SLS, GRAB_SLS, FOODPANDA_SLS, MASTERDEBIT_SLS, VISADEBIT_SLS, PAYPAL_SLS, NO_CASH_PAYMENT, NO_OTHER_SLS, NO_CHECK_SLS, NO_GC_SLS, NO_MASTERCARD_SLS, NO_VISA_SLS, NO_AMEX_SLS, NO_DINERS_SLS, NO_JCB_SLS, NO_GCASH_SLS, NO_PAYMAYA_SLS, NO_ALIPAY_SLS, NO_WECHAT_SLS, NO_GRAB_SLS, NO_FOODPANDA_SLS, NO_MASTERDEBIT_SLS, NO_VISADEBIT_SLS, NO_PAYPAL_SLS, PWD_DISC, SNRCIT_DISC, EMPLO_DISC, AYALA_DISC, STORE_DISC, OTHER_DISC, NO_PWD_DISC, NO_SNRCIT_DISC, NO_EMPLO_DISC, NO_AYALA_DISC, NO_STORE_DISC, NO_OTHER_DISC, NO_TRN from {tablename} where UPDATE_TIME BETWEEN '{start_date}' AND '{end_date}' AND tag_sync=0 ORDER BY TRN_DATE DESC;"
            if tablename=='it_report_summary':
                query = f"SELECT TOP {bulklimit} CCCODE, TRN_DATE, TER_NO, BRN_CODE, BRN_DESC, MALL_AREA_CODE, MALL_AREA_DESCRIPTION, BUILDING_CODE, BUILDING_DESCRIPTION, MF_CODE, MF_DESCRIPTION, STORE_NAME, COMPANY_CODE, MERCHANT_CLASS_DESCRIPTION, TEMPORARY_CONTRACT_NUMBER, NETSALES from {tablename} where UPDATE_TIME BETWEEN '{start_date}' AND '{end_date}' AND tag_sync=0 ORDER BY TRN_DATE DESC;"
            # query = f"SELECT top 10 * from {tablename} where UPDATE_TIME BETWEEN '{start_date}' AND '{end_date}';"
            return db.fetchAll(query)
        except Exception as e:
            logger.exception("Exception occurred: %s", str(e))
    def updateSyncTable(self, id):
        try:
            sql=f"UPDATE sync_table set status=1 WHERE status = 0 AND id = {id};"
            return db.update(sql)
        except Exception as e:
            logger.exception("Exception occurred: %s", str(e))
    def updateSummaryTable(self, sql):
        try:
            return db.update(sql)
        except Exception as e:
            logger.exception("Exception occurred: %s", str(e))
    def countRows(self, tablename, start_date, end_date):
        try:
            sql=f"SELECT COUNT(*) from {tablename} where UPDATE_TIME BETWEEN '{start_date}' AND '{end_date}' AND tag_sync=0 ;"
            return db.fetchOne(sql)
        except Exception as e:
            logger.exception("Exception occurred: %s", str(e))
    def deleteSyncTable(self, id):
        try:
            sql=f"DELETE from sync_table WHERE status = 1 AND id = {id};"
            return db.remove(sql)
        except Exception as e:
            logger.exception("Exception occurred: %s", str(e))

    def getTableColumn(self, tablename):
        try:
            column=f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{tablename}'"
            return db.selectColumn(column)
        except Exception as e:
            logger.exception("Exception occurred when query: %s", str(e))
    def checkExist(self, column, tablename, condition):
        try:
            sql=f"SELECT {column} FROM dbo.{tablename} WHERE {condition};"
            return db.fetchOne(sql)
        except Exception as e:
            logger.exception("Exception occurred when query: %s", str(e))
    def checkUserMallExist(self, tablename, condition):
        try:
            sql=f"SELECT * FROM dbo.{tablename} WHERE {condition};"
            return db.fetchOne(sql)
        except Exception as e:
            logger.exception("Exception occurred when query: %s", str(e))
    def selectRow(self, column, tablename, condition):
        try:
            sql=f"SELECT {column} FROM dbo.{tablename} WHERE {condition};"
            return db.fetchOne(sql)
        except Exception as e:
            logger.exception("Exception occurred when query: %s", str(e))
    def updateRow(self, column, tablename, values, condition):
        try:
            sql=f"UPDATE dbo.{tablename} SET {column}={values} WHERE {condition};"
            return db.update(sql)
        except Exception as e:
            logger.exception("Exception occurred when query: %s", str(e))
    def insertRow(self, tablename, values):
        try:
            sql = ''
            keylist = "("
            valuelist = "("
            firstPair = True
            for batch in batched(values.items(), bulklimit):
                for key, value in batch:
                    if value==None:
                        continue
                    if not firstPair:
                        keylist += ", "
                        valuelist += ", "
                    firstPair = False
                    keylist += key
                    if isinstance(value, str):
                        value=str2(value)
                        valuelist += "'" + value + "'"
                    else:
                        valuelist += str(value)
                keylist += ")"
                valuelist += ")"
                sql += "INSERT INTO dbo." + tablename + " " + keylist + " VALUES " + valuelist + "\n"
            db.insert(sql)
        except Exception as e:
            logger.exception("Exception occurred when insert row: %s", str(e))
    def removeRow(self, tablename, condition):
        try:
            sql=f" DELETE FROM dbo.{tablename}  WHERE {condition};"
            return db.remove(sql)
        except Exception as e:
            logger.exception("Exception occurred when query: %s", str(e))
def str2(words):
    return str(words).replace("'", '"')
def batched(iterable, n):
    # batched('ABCDEFG', 3) → ABC DEF G
    if n < 1:
        raise ValueError('n must be at least one')
    iterator = iter(iterable)
    while batch := tuple(islice(iterator, n)):
        yield batch
def parsing_date(text):
    for fmt in ('%Y-%m-%d', '%Y-%m-%d %H:%M:%S'):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            pass
    raise ValueError('no valid date format found')
    

            

        