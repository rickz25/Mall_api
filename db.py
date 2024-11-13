import sqlite3
import pyodbc
import configparser
import time

config = configparser.ConfigParser()
config.read(r'settings/config.txt') 

class LiteDB:
    def __init__(self, db):
        self.conn = sqlite3.connect(db,check_same_thread=False)
        self.cur = self.conn.cursor()
        self.conn.commit()

    def fetchSetting(self):
        self.cur.execute("SELECT * FROM settings")
        rows = self.cur.fetchone()
        return rows
    def getStatus(self):
        self.cur.execute("SELECT * FROM status")
        rows = self.cur.fetchone()
        return rows

    def insert(self, part, customer, retailer, price):
        self.cur.execute("INSERT INTO settings VALUES (NULL, ?, ?, ?, ?)",
                         (part, customer, retailer, price))
        self.conn.commit()

    def remove(self, id):
        self.cur.execute("DELETE FROM settings WHERE id=?", (id,))
        self.conn.commit()
    def __del__(self):
        self.conn.close()


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
        self.cursor = self.conn.cursor()

    def fetchOne(self,sql):
        self.cursor.execute(sql)
        return self.cursor.fetchone()
    def fetchAll(self,sql):
        retry_flag = 3
        retry_count = 0
        while retry_count < retry_flag:
            try:
                self.cursor.execute(sql)
                columns = [column[0] for column in  self.cursor.description]
                results = []
                for row in  self.cursor.fetchall():
                    results.append(dict(zip(columns, row)))
                return results
            except Exception as e:
                retry_count = retry_count + 1
                time.sleep(1)
    def insert(self, statement):
        retry_flag = 3
        retry_count = 0
        while retry_count < retry_flag:
            try:
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
                self.cursor.execute(sql)
                return self.cursor.fetchall()
            except Exception as e:
                retry_count = retry_count + 1
                time.sleep(1)
    def rows(self):
        return self.cursor.rowcount

    # def __del__(self):
    #     self.conn.close()