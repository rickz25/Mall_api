from datetime import date, datetime
from decimal import Decimal
import logging, configparser, dataclasses, json
from itertools import islice

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

# convertion of datetime and decimal
def default(obj):
    if isinstance(obj, (datetime, date)):
        return obj.strftime("%Y-%m-%d %H:%M:%S") 
    if isinstance(obj, Decimal):
        return str(obj)
    if dataclasses.is_dataclass(obj):
        return dataclasses.asdict(obj)
    raise TypeError ("Type %s not serializable" % type(obj))

class DataCompiler:
    def __init__(self):
        self.builder = QueryBuilder()
    def insert_data(self, data):
        datas={}
        try:
            # Mapping header
            MappingHeader = data['MappingHeader']
            if MappingHeader:
                header_data = self.builder.build_query_MappingHeader(MappingHeader)
                
                # Transaction mapping
                TransactionMapping = data['TransactionMapping']
                if TransactionMapping:
                    transaction_data = self.builder.build_query_Transaction(TransactionMapping)
                else:
                    transaction_data=[]
            else: 
                header_data=[]
                transaction_data=[]

            # Daily mapping
            DailyMapping = data['DailyMapping']
            if DailyMapping:
                daily_data = self.builder.build_query_Daily(DailyMapping)
            else: 
                daily_data=[]
                
            # Mapping log
            MappingLogs = data['MappingLogs']
            if MappingLogs:
                log_data = self.builder.build_query_MappingLogs(MappingLogs)
            else: 
                log_data=[]
            
            # Tag
            datas['header_sales'] = header_data
            datas['hourly_sales'] = transaction_data
            datas['eod_sales'] = daily_data
            datas['logs'] = log_data
            return datas

        except Exception as e:
            str_error = str(e)
            logger.exception("02 - Exception occurred: %s", str_error)

class QueryBuilder:

    # Insert Mapping Header
    def build_query(self, jsondata, tablename):
        try:
            insertSql = ''
            sqlstatement={}
            for i in jsondata: 
                keylist = "("
                valuelist = "("
                firstPair = True
                for key, value in i.items():
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
                insertSql += "INSERT INTO " + tablename + " " + keylist + " VALUES " + valuelist + ";"
            sqlstatement=insertSql
            return sqlstatement
        except Exception as e:
            logger.exception("Exception occurred when Insert Daily: %s", str(e))
    
    # Insert Transaction
    def build_query_Transaction(self, jsondata):
        try:
            TABLE_NAME = "dbo.hourly_sales"
            deleteSql = ''
            insertSql = ''
            DataArray={}
            batcharray=[]
            for batch in batched(jsondata, (bulklimit + 100)):
                sqlstatement={}
                for i in batch:
                    d = parsing_date(i['TRN_DATE'])
                    trn_date = d.strftime('%Y-%m-%d')
                    CCCODE = (i['CCCODE']).strip()
                    TER_NO = (i['TER_NO']).strip()
                    TRANSACTION_NO = (i['TRANSACTION_NO']).strip()
                    # For deletion statement
                    deleteSql += f"DELETE from {TABLE_NAME} WHERE CAST(TRN_DATE AS DATE)='{trn_date}' AND CCCODE='{CCCODE}' AND TER_NO= '{TER_NO}' AND TRANSACTION_NO='{TRANSACTION_NO}'; \n"
                        
                    keylist = "("
                    valuelist = "("
                    firstPair = True
                    for key, value in i.items():
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
                    insertSql += "INSERT INTO " + TABLE_NAME + " " + keylist + " VALUES " + valuelist + ";\n"
                sqlstatement['delete']=deleteSql
                sqlstatement['insert']=insertSql
                batcharray.append(sqlstatement)
            DataArray['data']=batcharray
            return DataArray
        except Exception as e:
            logger.exception("Exception occurred when Insert Transaction: %s", str(e))
    
    # Insert Daily
    def build_query_Daily(self, jsondata):
        try:
            TABLE_NAME = "dbo.eod_sales"
            daily_tag=[]
            deleteSql = ''
            insertSql = ''
            sqlstatement={}
            DataArray={}
            for i in jsondata:
                trn_date = (i['TRN_DATE']).strip()
                ter_no = (i['TER_NO']).strip()
                cccode = (i['CCCODE']).strip()
                tag_id =f"{cccode}_{trn_date}_{ter_no}"
                # append tag
                daily_tag.append(tag_id)
                # For deletion statement
                deleteSql += f"DELETE from {TABLE_NAME} WHERE CAST(TRN_DATE AS DATE)='{trn_date}' AND CCCODE='{cccode}' AND TER_NO= '{ter_no}'; \n"
                    
                keylist = "("
                valuelist = "("
                firstPair = True
                for key, value in i.items():
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
                insertSql += "INSERT INTO " + TABLE_NAME + " " + keylist + " VALUES " + valuelist + "; \n"
            sqlstatement['delete']=deleteSql
            sqlstatement['insert']=insertSql
            DataArray['tag']=daily_tag
            DataArray['data']=sqlstatement
            return DataArray
        except Exception as e:
            logger.exception("Exception occurred when Insert Daily: %s", str(e))
    
    # Inser MappingLogs
    def build_query_MappingLogs(self, jsondata):
        try:
            log_tag=[]
            deleteSql = ''
            insertSql = ''
            sqlstatement={}
            DataArray={}
            TABLE_NAME = "dbo.mapping_log"
            for i in jsondata:
                d = parsing_date(i['TRN_DATE'])
                trn_date = d.strftime('%Y-%m-%d')
                mall_code = i['MALL_CODE']
                merchant_code = (i['MERCHANT_CODE']).strip()
                ter_no = (i['TERMINAL_NO']).strip()
                template = i['TEMPLATE']
                unique_id =f"{merchant_code}_{trn_date}_{ter_no}_{template}"
                # append tag
                log_tag.append(unique_id)
                # For deletion statement
                deleteSql += f"DELETE from {TABLE_NAME} WHERE CAST(TRN_DATE AS DATE)='{trn_date}' AND MERCHANT_CODE='{merchant_code}' AND MALL_CODE= {mall_code} AND TEMPLATE= {template} AND TERMINAL_NO= '{ter_no}'; \n"
                    
                keylist = "("
                valuelist = "("
                firstPair = True
                for key, value in i.items():
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
                insertSql += "INSERT INTO " + TABLE_NAME + " " + keylist + " VALUES " + valuelist + "; \n"
            sqlstatement['delete']=deleteSql
            sqlstatement['insert']=insertSql
            DataArray['tag']=log_tag
            DataArray['data']=sqlstatement
            return DataArray

        except Exception as e:
            logger.exception("Exception occurred when Insert Mapping_log: %s", str(e))

def parsing_date(text):
    for fmt in ('%Y-%m-%d', '%Y-%m-%d %H:%M:%S'):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            pass
    raise ValueError('no valid date format found')
def rmv_space(string):
    return str(string).replace(" ", "")      

def str2(words):
    return str(words).replace("'", '"')  

def batched(iterable, n):
    # batched('ABCDEFG', 3) → ABC DEF G
    if n < 1:
        raise ValueError('n must be at least one')
    iterator = iter(iterable)
    while batch := tuple(islice(iterator, n)):
        yield batch   