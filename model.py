from db import SQLServer
import configparser,logging
from itertools import islice
config = configparser.ConfigParser()
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
    
    def getMappingHeader(self):
        try:
            sql=f"SELECT TOP ({bulklimit}) HEADER_ID,TEMPLATE,TRN_DATE,MERCHANT_CODE,POS_VENDOR_CODE,CREATED_AT,LAST_MODIFIED,SUBMISSION_FLAG,MALL_CODE,TAG FROM dbo.header_sales mh WITH (NOLOCK) WHERE mh.TAG=0 AND YEAR(TRN_DATE) >= (YEAR(GETDATE()))-1 ORDER BY mh.TRN_DATE ASC;"
            return db.fetchAll(sql)
        except Exception as e:
            logger.exception("Exception occurred when select header_sales: %s", str(e))
    def getTransaction(self):
        try:
            sql=f"SELECT * FROM dbo.hourly_sales WITH (NOLOCK) WHERE POS_AGENT_MAPPING_HEADER IN (SELECT TOP ({bulklimit}) HEADER_ID FROM dbo.header_sales mh WITH (NOLOCK) WHERE mh.TAG=0 AND YEAR(mh.TRN_DATE) >= (YEAR(GETDATE()))-1 ORDER BY mh.TRN_DATE ASC);"
            return db.fetchAll(sql)
        except Exception as e:
            logger.exception("Exception occurred when select hourly_sales: %s", str(e))
    # def getDaily(self):
    #     sql=f"SELECT * FROM dbo.eod_sales WHERE POS_AGENT_MAPPING_HEADER IN (SELECT TOP ({bulklimit}) HEADER_ID FROM dbo.header_sales mh WHERE mh.TAG=0 AND YEAR(TRN_DATE)=YEAR(GETDATE()) ORDER BY mh.TRN_DATE ASC);"
    #     return db.fetchAll(sql)
    
    def getDaily(self):
        try:
            sql=f"SELECT TOP ({bulklimit}) POS_AGENT_MAPPING_HEADER,CCCODE,MERCHANT_NAME,TER_NO,TRN_DATE,STRANS,ETRANS,GROSS_SLS,VAT_AMNT,VATABLE_SLS,NONVAT_SLS,VATEXEMPT_SLS,VATEXEMPT_AMNT,OLD_GRNTOT,NEW_GRNTOT,LOCAL_TAX,VOID_AMNT,NO_VOID,DISCOUNTS,NO_DISC,REFUND_AMT,NO_REFUND,SNRCIT_DISC,NO_SNRCIT,PWD_DISC,NO_PWD,EMPLO_DISC,NO_EMPLO,AYALA_DISC,NO_AYALA,STORE_DISC,NO_STORE,OTHER_DISC,NO_OTHER_DISC,SCHRGE_AMT,OTHER_SCHR,CASH_SLS,CARD_SLS,EPAY_SLS,DCARD_SLS,OTHER_SLS,CHECK_SLS,GC_SLS,MASTERCARD_SLS,VISA_SLS,AMEX_SLS,DINERS_SLS,JCB_SLS,GCASH_SLS,PAYMAYA_SLS,ALIPAY_SLS,WECHAT_SLS,GRAB_SLS,FOODPANDA_SLS,OPEN_SALES,OPEN_SALES_2,OPEN_SALES_3,OPEN_SALES_4,OPEN_SALES_5,OPEN_SALES_6,OPEN_SALES_7,OPEN_SALES_8,OPEN_SALES_9,OPEN_SALES_10,OPEN_SALES_11,MASTERDEBIT_SLS,VISADEBIT_SLS,PAYPAL_SLS,ONLINE_SLS,GC_EXCESS,NO_VATEXEMT,NO_SCHRGE,NO_OTHER_SUR,NO_CASH,NO_CARD,NO_EPAY,NO_DCARD_SLS,NO_OTHER_SLS,NO_CHECK,NO_GC,NO_MASTERCARD_SLS,NO_VISA_SLS,NO_AMEX_SLS,NO_DINERS_SLS,NO_JCB_SLS,NO_GCASH_SLS,NO_PAYMAYA_SLS,NO_ALIPAY_SLS,NO_WECHAT_SLS,NO_GRAB_SLS,NO_FOODPANDA_SLS,NO_OPEN_SALES,NO_OPEN_SALES_2,NO_OPEN_SALES_3,NO_OPEN_SALES_4,NO_OPEN_SALES_5,NO_OPEN_SALES_6,NO_OPEN_SALES_7,NO_OPEN_SALES_8,NO_OPEN_SALES_9,NO_OPEN_SALES_10,NO_OPEN_SALES_11,NO_MASTERDEBIT_SLS,NO_VISADEBIT_SLS,NO_PAYPAL_SLS,NO_ONLINE_SLS,NO_NOSALE,NO_CUST,NO_TRN,PREV_EODCTR,EODCTR,NETSALES,TAG FROM dbo.eod_sales WITH (NOLOCK) WHERE SYNC_TAG=0 AND YEAR(TRN_DATE) >= (YEAR(GETDATE()))-1 ORDER BY TRN_DATE ASC;"
            return db.fetchAll(sql)
        except Exception as e:
            logger.exception("Exception occurred when select eod_sales: %s", str(e))
    
    def getMappingLogs(self):
        try:
            sql=f"SELECT TOP ({bulklimit}) STATUS, MESSAGE, TEXT, TEMPLATE, TRN_DATE, FILENAME, MERCHANT_CODE, STORE_NAME, MALL_CODE, TERMINAL_NO, IS_NULL_ERROR, VALIDATED_AT, TAG FROM dbo.mapping_log WITH (NOLOCK) WHERE STATUS=0 AND TAG=0 AND YEAR(TRN_DATE) >= (YEAR(GETDATE()))-1 ORDER BY TRN_DATE ASC;"
            return db.fetchAll(sql)
        except Exception as e:
            logger.exception("Exception occurred when select mapping_logs: %s", str(e))
    def postMappingHeader(self, param):
        try:
            sql=f"UPDATE dbo.header_sales SET TAG=1 WHERE HEADER_ID = {param}"
            return db.update(sql)
        except Exception as e:
            logger.exception("Exception occurred when update header_sales: %s", str(e))
    def postDailyMapping(self, param):
        try:
            sql=f"UPDATE dbo.eod_sales SET SYNC_TAG=1 WHERE CCCODE = '{param[0]}' AND CAST(TRN_DATE AS DATE) = '{param[1]}' AND TER_NO = '{param[2]}'"
            return db.update(sql)
        except Exception as e:
            logger.exception("Exception occurred when update eod_sales: %s", str(e))
    def postMappingLogs(self, param):
        try:
            sql=f"UPDATE dbo.mapping_log SET TAG=1 WHERE MERCHANT_CODE = '{param[0]}' AND CAST(TRN_DATE AS DATE) = '{param[1]}' AND TERMINAL_NO = '{param[2]}' AND TEMPLATE = {param[3]}"
            return db.update(sql)
        except Exception as e:
            logger.exception("Exception occurred when update mapping_log: %s", str(e))
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
    

            

        