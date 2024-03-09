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

    def insert_query(self,sql,val):
        try:
            cursor = self.mydb.cursor()
            cursor.execute(sql,val)
        except Exception:
            self.connect()
            cursor = self.mydb.cursor()
            cursor.execute(sql,val)
        res=cursor.fetchall()
        cursor.close()
        self.mydb.commit()
        return res

    def get_status(self,servers):
        try:
            self.connect()
            res = self.query("Select * from ShardT_Schema")
            shards = []
            for shard_data in res:
                shard_dict = {
                    "Stud_id_low": shard_data[0],
                    "Shard_id": shard_data[1],
                    "Shard_size": shard_data[2]
                }
                shards.append(shard_dict)
            mapdata = self.query("Select * from MapT_Schema")
            for shard, server in mapdata:
                servers[server].append(shard)
            return shards,servers

        except Exception as e:
            return {"error": f"An error occurred: {str(e)}"},500

    def add_map_table(self,shard,ser):
        try:
            self.connect()
            insert_map_query = "INSERT INTO MapT_Schema (Shard_id,Server_id) VALUES (%s , %s)"
            values = (shard,ser)
            self.insert_query(insert_map_query,values)
            return  {"message":"Comfigured database"},200
        except Exception as e:
            return {"error": f"An error occurred: {str(e)}"},500

    def add_shard_table(self,Stud_id_low,Shard_id,Shard_size,valid_idx):
        try:
            self.connect()
            insert_shard_query = "INSERT INTO ShardT_Schema (Stud_id_low, Shard_id, Shard_size,valid_idx) VALUES (%s, %s, %s, %s)"
            values = (Stud_id_low,Shard_id,Shard_size,valid_idx)

            self.insert_query(insert_map_query,values)
            return  {"message":"Comfigured database"},200
        except Exception as e:
            return {"error": f"An error occurred: {str(e)}"},500
            
    def initialize_shard_map_table(self,data):
        try:
            self.connect()
            N = data.get('N')
            schema = data.get('schema',{})
            columns = schema.get('columns',[])
            dtypes = schema.get('dtypes',[])
            shards = data.get('shards',[])
            servers = data.get('servers',{})

            create_shard_query = '''CREATE TABLE IF NOT EXISTS ShardT_Schema (
                                    Stud_id_low INT NOT NULL,
                                    Shard_id VARCHAR(255) NOT NULL,
                                    Shard_size INT NOT NULL,
                                    valid_idx INT NOT NULL,
                                    PRIMARY KEY(Stud_id_low)
                                )'''
            
            self.query(create_shard_query)

            insert_shard_query = "INSERT INTO ShardT_Schema (Stud_id_low, Shard_id, Shard_size,valid_idx) VALUES (%s, %s, %s, %s)"
            
            for shard in shards:
                values = (shard["Stud_id_low"], shard["Shard_id"], shard["Shard_size"],shard["Stud_id_low"])
                self.insert_query(insert_shard_query,values)

            res = self.query("Select * from ShardT_Schema")
            print(res,flush = True)

            create_map_query = '''CREATE TABLE IF NOT EXISTS MapT_Schema (
                                Shard_id varchar(255) NOT NULL,
                                Server_id varchar(255) NOT NULL
                                )'''
            self.query(create_map_query)

            insert_map_query = "INSERT INTO MapT_Schema (Shard_id,Server_id) VALUES (%s , %s)"

            for key,val in servers.items():
                for v in val:
                    values = (v,key)
                    # print(values,flush = True)
                    self.insert_query(insert_map_query,values)

            res = self.query("Select * from MapT_Schema")
            print(res,flush = True)

            return {"message":"Comfigured database"},200
        except Exception as e:
            return {"error": f"An error occurred: {str(e)}"},500

