import dataclasses,configparser, logging
from datetime import date, datetime
from decimal import Decimal
from itertools import islice
from model import TaskModel
model = TaskModel()

config = configparser.ConfigParser()
config.read(r'settings/config.txt') 

# Create and configure logger
logging.basicConfig(filename="Logs/unoLog/logs.log",
                    format='%(asctime)s %(message)s',
                    filemode='w')
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

hq_ip =  config.get('mall_config', 'HQ_IP')
port =  config.get('mall_config', 'Port')
token = "Bearer eyJhbGciOiJIUzI1NiJ9.eyJSb2xlIjoiQWRtaW4iLCJJc3N1ZXIiOiJJc3N1ZXIiLCJVc2VybmFtZSI6IkphdmFJblVzZSIsImV4cCI6MTY1NDc1NDg1NCwiaWF0IjoxNjU0NzU0ODU0fQ.p6WAfLuC39cMk3XEF4LcU5iZy1rzbL0VTKVpTY7mRGQ"

config = configparser.ConfigParser()
config.read(r'settings/config.txt') 

# convertion of datetime and decimal
def default(obj):
    if isinstance(obj, (datetime, date)):
        return obj.strftime("%Y-%m-%d %H:%M:%S") 
    if isinstance(obj, Decimal):
        return str(obj)
    if dataclasses.is_dataclass(obj):
            return dataclasses.asdict(obj)
    raise TypeError ("Type %s not serializable" % type(obj))

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
class QueryBuilder:
    # Insert Mapping Header
    def build_query(self, jsondata, tablename):
        try:
            insertSql = ""
            where_condition = []
            sqlScript=[]
            column=''
            if jsondata:
                for i in jsondata: 
                    cccode = i['CCCODE'].strip()
                    trn_date = i['TRN_DATE'].strip()
                    ter_no = i['TER_NO'].strip()
                    where_condition.append(f"'{trn_date}_{cccode}_{ter_no}'")
                    keylist = "("
                    valuelist = "("
                    firstPair = True
                    for key, value in i.items():
                        if value==None:
                            # continue
                            valuelist += 0
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
                    column = keylist
                    # insertSql.append("INSERT INTO " + tablename + " " + keylist + " VALUES " + valuelist + "")
                    if i == jsondata[-1]:
                        insertSql +=valuelist + ";"
                    else:
                        insertSql +=valuelist + ", \n"
                convert_toString =','.join(where_condition)
                updateSql =f"UPDATE {tablename} set tag_sync=1 WHERE TRIM(TRN_DATE)+ '_' + TRIM(CCCODE) + '_' + TRIM(TER_NO) IN ({convert_toString});" 
                deleteSql =f"DELETE FROM {tablename} WHERE TRIM(TRN_DATE)+ '_' + TRIM(CCCODE) + '_' + TRIM(TER_NO) IN ({convert_toString});" 
                insertScript = f"INSERT INTO {tablename} " + column + "\n VALUES" + insertSql
                model.updateSummaryTable(updateSql)
                sqlScript.append(deleteSql)
                sqlScript.append(insertScript)
                return sqlScript
        except Exception as e:
            logger.exception("Exception occurred when Insert Daily: %s", str(e))

class TaskController:
    def __init__(self, model):
        self.model = model
    
    def post_maintenance(self, response):
        str_error=""
        try:
            for tablename in response['data']:
                for values  in response['data'][tablename]:
                    columns = self.model.getTableColumn(tablename)
                    if tablename =='merchant_reason':
                        columns.pop(4)

                    for i in range(len(columns)):
                        try:
                            column = columns[i][0]
                            val = values[column]
                            if val is None or val =='':
                                continue
                            # merchant and merchant_contacts
                            if tablename =='merchant' or tablename =='merchant_contacts' or  tablename =='merchant_office_contacts' or tablename =='merchant_reason':
                                id = values['MERCHANT_CODE']
                                condition = f"MERCHANT_CODE = '{id}' AND (LEN(ISNULL({column},'')) = 0)"
                                check_condition = f"MERCHANT_CODE = '{id}'"
                                exist = self.model.checkExist(column, tablename, check_condition)
                                if exist !=None:
                                    checkNull = self.model.selectRow(column, tablename, condition)
                                    if checkNull !=None:
                                        self.model.updateRow(column, tablename, val, check_condition)
                                else:
                                    self.model.insertRow(tablename, values)

                            # reason
                            if tablename =='reason':
                                id = values['REASON_CODE']
                                condition = f"REASON_CODE = '{id}' AND (LEN(ISNULL({column},'')) = 0)"
                                check_condition = f"REASON_CODE = '{id}'"
                                exist = self.model.checkExist(column, tablename, check_condition)
                                if exist !=None:
                                    checkNull = self.model.selectRow(column, tablename, condition)
                                    if checkNull:
                                        self.model.updateRow(column, tablename, val, check_condition)
                                else:
                                    self.model.insertRow(tablename, values)
                            
                            # company
                            if tablename =='company':
                                id = values['COMPANY_CODE']
                                condition = f"COMPANY_CODE = '{id}' AND (LEN(ISNULL({column},'')) = 0)"
                                check_condition = f"COMPANY_CODE = '{id}'"
                                exist = self.model.checkExist(column, tablename, check_condition)
                                if exist !=None:
                                    checkNull = self.model.selectRow(column, tablename, condition)
                                    if checkNull:
                                        self.model.updateRow(column, tablename, val, check_condition)
                                else:
                                    self.model.insertRow(tablename, values)

                            # contract
                            if tablename =='contract':
                                id = values['CONTRACT_ID']
                                condition = f"CONTRACT_ID = '{id}' AND (LEN(ISNULL({column},'')) = 0)"
                                check_condition = f"CONTRACT_ID = '{id}'"
                                exist = self.model.checkExist(column, tablename, check_condition)
                                if exist !=None:
                                    checkNull = self.model.selectRow(column, tablename, condition)
                                    if checkNull:
                                        self.model.updateRow(column, tablename, val, check_condition)
                                else:
                                    self.model.insertRow(tablename, values)

                            # merchant_class
                            if tablename =='merchant_class':
                                id = values['MERCHANT_CLASS_CODE']
                                condition = f"MERCHANT_CLASS_CODE = '{id}' AND (LEN(ISNULL({column},'')) = 0)"
                                check_condition = f"MERCHANT_CLASS_CODE = '{id}'"
                                exist = self.model.checkExist(column, tablename, check_condition)
                                if exist !=None:
                                    checkNull = self.model.selectRow(column, tablename, condition)
                                    if checkNull:
                                        self.model.updateRow(column, tablename, val, check_condition)
                                else:
                                    self.model.insertRow(tablename, values)
                            
                            # mall and mall_contacts
                            if tablename =='mall' or tablename =='mall_contacts':
                                id = values['BRN_CODE']
                                condition = f"BRN_CODE = '{id}' AND (LEN(ISNULL({column},'')) = 0)"
                                check_condition = f"BRN_CODE = '{id}'"
                                exist = self.model.checkExist(column, tablename, check_condition)
                                if exist !=None:
                                    checkNull = self.model.selectRow(column, tablename, condition)
                                    if checkNull:
                                        self.model.updateRow(column, tablename, val, check_condition)
                                else:
                                    self.model.insertRow(tablename, values)
                            
                            # mother_brand
                            if tablename =='mother_brand':
                                id = values['MOTHER_BRAND_CODE']
                                condition = f"MOTHER_BRAND_CODE = '{id}' AND (LEN(ISNULL({column},'')) = 0)"
                                check_condition = f"MOTHER_BRAND_CODE = '{id}'"
                                exist = self.model.checkExist(column, tablename, check_condition)
                                if exist !=None:
                                    checkNull = self.model.selectRow(column, tablename, condition)
                                    if checkNull:
                                        self.model.updateRow(column, tablename, val, check_condition)
                                else:
                                    self.model.insertRow(tablename, values)

                            # merchant_brand
                            if tablename =='merchant_brand':
                                id = values['MERCHANT_BRAND_CODE']
                                condition = f"MERCHANT_BRAND_CODE = '{id}' AND (LEN(ISNULL({column},'')) = 0)"
                                check_condition = f"MERCHANT_BRAND_CODE = '{id}'"
                                exist = self.model.checkExist(column, tablename, check_condition)
                                if exist !=None:
                                    checkNull = self.model.selectRow(column, tablename, condition)
                                    if checkNull:
                                        self.model.updateRow(column, tablename, val, check_condition)
                                else:
                                    self.model.insertRow(tablename, values)

                            # category and pos_vendor_category
                            if tablename =='category' or tablename =='pos_vendor_category':
                                id = values['CATEGORY_CODE']
                                condition = f"CATEGORY_CODE = '{id}' AND (LEN(ISNULL({column},'')) = 0)"
                                check_condition = f"CATEGORY_CODE = '{id}'"
                                exist = self.model.checkExist(column, tablename, check_condition)
                                if exist !=None:
                                    checkNull = self.model.selectRow(column, tablename, condition)
                                    if checkNull:
                                        self.model.updateRow(column, tablename, val, check_condition)
                                else:
                                    self.model.insertRow(tablename, values)

                            # category_sub
                            if tablename =='category_sub':
                                id = values['SCATEGORY_CODE']
                                condition = f"SCATEGORY_CODE = '{id}' AND (LEN(ISNULL({column},'')) = 0)"
                                check_condition = f"SCATEGORY_CODE = '{id}'"
                                exist = self.model.checkExist(column, tablename, check_condition)
                                if exist !=None:
                                    checkNull = self.model.selectRow(column, tablename, condition)
                                    if checkNull:
                                        self.model.updateRow(column, tablename, val, check_condition)
                                else:
                                    self.model.insertRow(tablename, values)

                            # contract_type
                            if tablename =='contract_type':
                                id = values['CONTRACT_CODE']
                                condition = f"CONTRACT_CODE = '{id}' AND (LEN(ISNULL({column},'')) = 0)"
                                check_condition = f"CONTRACT_CODE = '{id}'"
                                exist = self.model.checkExist(column, tablename, check_condition)
                                if exist !=None:
                                    checkNull = self.model.selectRow(column, tablename, condition)
                                    if checkNull:
                                        self.model.updateRow(column, tablename, val, check_condition)
                                else:
                                    self.model.insertRow(tablename, values)

                            # building
                            if tablename =='building':
                                id = values['BUILDING_CODE']
                                condition = f"BUILDING_CODE = '{id}' AND (LEN(ISNULL({column},'')) = 0)"
                                check_condition = f"BUILDING_CODE = '{id}'"
                                exist = self.model.checkExist(column, tablename, check_condition)
                                if exist !=None:
                                    checkNull = self.model.selectRow(column, tablename, condition)
                                    if checkNull:
                                        self.model.updateRow(column, tablename, val, check_condition)
                                else:
                                    self.model.insertRow(tablename, values)

                            # mall_floor
                            if tablename =='mall_floor':
                                id = values['MF_CODE']
                                condition = f"MF_CODE = '{id}' AND (LEN(ISNULL({column},'')) = 0)"
                                check_condition = f"MF_CODE = '{id}'"
                                exist = self.model.checkExist(column, tablename, check_condition)
                                if exist !=None:
                                    checkNull = self.model.selectRow(column, tablename, condition)
                                    if checkNull:
                                        self.model.updateRow(column, tablename, val, check_condition)
                                else:
                                    self.model.insertRow(tablename, values)

                            # mall_area
                            if tablename =='mall_area':
                                id = values['MALL_AREA_CODE']
                                condition = f"MALL_AREA_CODE = '{id}' AND (LEN(ISNULL({column},'')) = 0)"
                                check_condition = f"MALL_AREA_CODE = '{id}'"
                                exist = self.model.checkExist(column, tablename, check_condition)
                                if exist !=None:
                                    checkNull = self.model.selectRow(column, tablename, condition)
                                    if checkNull:
                                        self.model.updateRow(column, tablename, val, check_condition)
                                else:
                                    self.model.insertRow(tablename, values)

                            # pos_vendor and pos_vendor_contacts
                            if tablename =='pos_vendor' or tablename =='pos_vendor_contacts':
                                id = values['POS_VENDOR_CODE']
                                condition = f"POS_VENDOR_CODE = '{id}' AND (LEN(ISNULL({column},'')) = 0)"
                                check_condition = f"POS_VENDOR_CODE = '{id}'"
                                exist = self.model.checkExist(column, tablename, check_condition)
                                if exist !=None:
                                    checkNull = self.model.selectRow(column, tablename, condition)
                                    if checkNull:
                                        self.model.updateRow(column, tablename, val, check_condition)
                                else:
                                    self.model.insertRow(tablename, values)

                            # user_mall
                            if tablename =='user_mall':
                                username = values['username']
                                mallcode = values['MALL_CODE']
                                condition = f"username = '{username}' AND MALL_CODE ='{mallcode}'"
                                exist = self.model.checkUserMallExist(tablename, condition)
                                if exist == None:
                                     self.model.insertRow(tablename, values)

                            # users
                            if tablename =='users':
                                id = values['username']
                                condition = f"username = '{id}' AND (LEN(ISNULL({column},'')) = 0)"
                                check_condition = f"username = '{id}'"
                                exist = self.model.checkExist(column, tablename, check_condition)
                                if exist !=None:
                                    checkNull = self.model.selectRow(column, tablename, condition)
                                    if checkNull:
                                        self.model.updateRow(column, tablename, val, check_condition)
                                else:
                                    self.model.insertRow(tablename, values)
                        except Exception as e:
                            str_error +=str(e)
                            continue

        except Exception as e:
            str_error += str(e)
        return str_error
    

            
        