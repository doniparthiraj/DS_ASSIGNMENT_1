import mysql.connector


class SQLHandler:
    def __init__(self,host='localhost',user='root',password='abc',db='dQdb'):
        self.host=host
        self.user=user
        self.password=password
        self.db=db

    def connect(self):
        connected=False
        while not connected:
            try:
                self.mydb = mysql.connector.connect(host=self.host,user=self.user,password=self.password)
                self.UseDB(self.db)
                connected=True
            except Exception:
                pass
    
    def query(self, sql):
        try:
            cursor = self.mydb.cursor()
            cursor.execute(sql)
        except Exception:
            self.connect()
            cursor = self.mydb.cursor()
            cursor.execute(sql)
        res=cursor.fetchall()
        cursor.close()
        self.mydb.commit()
        return res

    def UseDB(self,dbname=None):
        res=self.query("SHOW DATABASES")
        if dbname not in [r[0] for r in res]:
            self.query(f"CREATE DATABASE {dbname}")
        self.query(f"USE {dbname}")

    def DropDB(self,dbname=None):
        res=self.query("SHOW DATABASES")
        if dbname in [r[0] for r in res]:
            self.query(f"DROP DATABASE {dbname}")

