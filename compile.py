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