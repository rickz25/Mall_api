import pyodbc, configparser, time, logging

# Create and configure logger
logging.basicConfig(filename="Logs/unoLog/logs.log",
                    format='%(asctime)s %(message)s',
                    filemode='w')
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

config = configparser.RawConfigParser()
config.read(r'settings/config.txt') 
class SQLServer:
    def __init__(self):
        driver=config.get('mall_config', 'db_driver')
        server =config.get('mall_config', 'db_host')
        database = config.get('mall_config', 'db_name')
        user = config.get('mall_config', 'db_username')
        pd = config.get('mall_config', 'db_password')
        add_details = config.get('mall_config', 'db_add_details')
        self.conn = pyodbc.connect(f"Driver={driver};"
                    rf"Server={server};"
                    f"Database={database};"
                    f"{add_details}"
                    f"uid={user};"
                    f"pwd={pd};")

    def fetchOne(self,sql):
        self.cursor = self.conn.cursor()
        self.cursor.execute(sql)
        return self.cursor.fetchone()
    def fetchAll(self,sql):
        retry_flag = 3
        retry_count = 0
        while retry_count < retry_flag:
            try:
                self.cursor = self.conn.cursor()
                self.cursor.execute(sql)
                columns = [column[0] for column in  self.cursor.description]
                results = []
                for row in  self.cursor.fetchall():
                    results.append(dict(zip(columns, row)))
                return results
            except Exception as e:
                retry_count = retry_count + 1
                time.sleep(1)


        # connection = None
        # while True:
        #     # time.sleep(1)
        #     if not connection:  # No connection yet? Connect.
        #        self.cursor = self.conn.cursor()
        #     try:
        #         self.cursor.execute(sql)
        #         columns = [column[0] for column in  self.cursor.description]
        #         results = []
        #         for row in self.cursor.fetchall():
        #             results.append(dict(zip(columns, row)))
        #         return results
        #     except pyodbc.Error as pe:
        #         if pe.args[0] == "08S01":  # Communication error.
        #             # Nuke the connection and retry.
        #             try:
        #                 self.conn.close()
        #             except:
        #                 pass
        #             connection = None
        #             continue
        #         raise logger.exception("Exception occurred: %s", pe)

    def insert(self, statement):
        retry_flag = 3
        retry_count = 0
        while retry_count < retry_flag:
            try:
                self.cursor = self.conn.cursor()
                self.cursor.execute(statement)
                self.conn.commit()
                break
            except Exception as e:
                retry_count = retry_count + 1
                time.sleep(1)
    def update(self, statement):
        retry_flag = 3
        retry_count = 0
        while retry_count < retry_flag:
            try:
                self.cursor = self.conn.cursor()
                self.cursor.execute(statement)
                self.conn.commit()
                break
            except Exception as e:
                retry_count = retry_count + 1
                time.sleep(1)
    def remove(self, statement):
        retry_flag = 3
        retry_count = 0
        while retry_count < retry_flag:
            try:
                self.cursor = self.conn.cursor()
                self.cursor.execute(statement)
                self.conn.commit()
                break
            except Exception as e:
                retry_count = retry_count + 1
                time.sleep(1)
    def selectColumn(self,sql):
        retry_flag = 3
        retry_count = 0
        while retry_count < retry_flag:
            try:
                self.cursor = self.conn.cursor()
                self.cursor.execute(sql)
                return self.cursor.fetchall()
            except Exception as e:
                retry_count = retry_count + 1
                time.sleep(1)
    def rows(self):
        self.cursor = self.conn.cursor()
        return self.cursor.rowcount
