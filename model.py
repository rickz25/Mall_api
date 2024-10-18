from db import SQLServer
db = SQLServer()        
class TaskModel:

    def getMappingHeader(self):
        sql="SELECT TOP (100) HEADER_ID,TEMPLATE,TRN_DATE,MERCHANT_CODE,POS_VENDOR_CODE,CREATED_AT,LAST_MODIFIED,SUBMISSION_FLAG,MALL_CODE,TAG FROM dbo.header_sales mh WHERE CAST(mh.LAST_MODIFIED AS DATE)=CAST(GETDATE() AS DATE) AND mh.TAG=0;"
        return db.fetchAll(sql)
    def getTransaction(self):
        sql="SELECT * FROM dbo.hourly_sales WHERE POS_AGENT_MAPPING_HEADER IN (SELECT TOP (100) HEADER_ID FROM dbo.header_sales mh WHERE CAST(mh.LAST_MODIFIED AS DATE)=CAST(GETDATE() AS DATE) AND mh.TAG=0);"
        return db.fetchAll(sql)
    def getDaily(self):
        sql="SELECT * FROM dbo.eod_sales WHERE POS_AGENT_MAPPING_HEADER IN (SELECT TOP (100) HEADER_ID FROM dbo.header_sales mh WHERE CAST(mh.LAST_MODIFIED AS DATE)=CAST(GETDATE() AS DATE) AND mh.TAG=0);"
        return db.fetchAll(sql)
    def getMappingLogs(self):
        sql="SELECT TOP (100) STATUS, MESSAGE, TEXT, TEMPLATE, TRN_DATE, FILENAME, MERCHANT_CODE, STORE_NAME, MALL_CODE, TERMINAL_NO, IS_NULL_ERROR, VALIDATED_AT, TAG FROM dbo.mapping_log WHERE STATUS=0 AND CAST(VALIDATED_AT AS DATE)=CAST(GETDATE() AS DATE) AND TAG=0;"
        return db.fetchAll(sql)
    def postMappingHeader(self, param):
        sql=f"UPDATE dbo.header_sales SET TAG=1 WHERE HEADER_ID = {param}"
        return db.update(sql)
    def postMappingLogs(self, param):
        sql=f"UPDATE dbo.mapping_log SET TAG=1 WHERE MERCHANT_CODE = '{param[0]}' AND CAST(TRN_DATE AS DATE) = '{param[1]}' AND TERMINAL_NO = '{param[2]}' AND TEMPLATE = {param[3]}"
        return db.update(sql)
    def getTableColumn(self, tablename):
        column=f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{tablename}'"
        return db.selectColumn(column)
    def checkExist(self, column, tablename, condition):
        sql=f"SELECT {column} FROM dbo.{tablename} WHERE {condition};"
        return db.fetchOne(sql)
    def selectRow(self, column, tablename, condition):
        sql=f"SELECT {column} FROM dbo.{tablename} WHERE {condition};"
        return db.fetchOne(sql)
    def updateRow(self, column, tablename, values, condition):
        sql=f"UPDATE dbo.{tablename} SET {column}={values} WHERE {condition};"
        return db.update(sql)
    def insertRow(self, tablename, values):
        sql = ''
        keylist = "("
        valuelist = "("
        firstPair = True
        for key, value in values.items():
            if value==None:
                continue
            if not firstPair:
                keylist += ", "
                valuelist += ", "
            firstPair = False
            keylist += key
            if isinstance(value, str):
                valuelist += "'" + value + "'"
            else:
                valuelist += str(value)
        keylist += ")"
        valuelist += ")"
        sql += "INSERT INTO dbo." + tablename + " " + keylist + " VALUES " + valuelist + "\n"
        return db.insert(sql)
    def removeRow(self, tablename, condition):
        sql=f" DELETE FROM dbo.{tablename}  WHERE {condition};"
        return db.remove(sql)
        

            

        