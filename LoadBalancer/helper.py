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

    def update_query(self, sql, val):
        try:
            cursor = self.mydb.cursor()
            cursor.execute(sql, val)
            res = cursor.fetchall()
            self.mydb.commit()  # Commit changes before fetching results
            cursor.close()
            return res
        except mysql.connector.Error as err:
            print(f"upError: {err}")
            self.mydb.rollback()
            return None

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

    def get_all(self):
        try:
            self.connect()
            res = self.query("Select * from ShardT_Schema")
            res1 = self.query("Select * from MapT_Schema")
            return res,res1
        except Exception as e:
            return {"error": f"An error occurred: {str(e)}"},500
    def add_map_table(self,shard,ser):
        try:
            self.connect()
            insert_map_query = "INSERT INTO MapT_Schema (Shard_id,Server_id) VALUES (%s , %s)"
            values = (shard,ser)    
            self.update_query(insert_map_query,values)
            print("Inserted",values)
            return  {"message":"Comfigured database"},200
        except Exception as e:
            return {"error": f"An error occurred: {str(e)}"},500

    def add_shard_table(self,Stud_id_low,Shard_id,Shard_size,valid_idx):
        try:
            self.connect()
            insert_shard_query = "INSERT INTO ShardT_Schema (Stud_id_low, Shard_id, Shard_size,valid_idx) VALUES (%s, %s, %s, %s)"
            values = (Stud_id_low,Shard_id,Shard_size,valid_idx)

            self.update_query(insert_shard_query,values)
            print("Inserted",values)
            return  {"message":"Comfigured database"},200
        except Exception as e:
            return {"error": f"An error occurred: {str(e)}"},500

    def remove_server(self, server):
        try:
            self.connect()
            shards = "SELECT Shard_id from MapT_Schema where Server_id = %s"
            resp = self.update_query(shards,(server,))
            
            delete_map_query = "DELETE FROM MapT_Schema where Server_id = %s"
            self.update_query(delete_map_query,(server,))

            for sid in resp:
                x = 'SELECT Server_id from MapT_Schema where Shard_id = %s'
                q = self.update_query(x,sid)

                if not q:
                    delete_shard = "DELETE FROM ShardT_Schema where Shard_id = %s"
                    self.update_query(delete_shard,sid)

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
                self.update_query(insert_shard_query,values)

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
                    self.update_query(insert_map_query,values)

            res = self.query("Select * from MapT_Schema")
            print(res,flush = True)

            return {"message":"Comfigured database"},200
        except Exception as e:
            return {"error": f"An error occurred: {str(e)}"},500

    def shards_required(self, low, high):
        try:
            self.connect()
            print(low,type(low),flush=True)
            range_query ='''Select Shard_id from ShardT_Schema 
                            where ( %s BETWEEN Stud_id_low AND Stud_id_low+Shard_size ) 
                                OR( %s BETWEEN Stud_id_low AND Stud_id_low+Shard_size )'''
            db_response = self.update_query(range_query,(low,high))
            response = []
            for x in db_response:
                response.append(x[0])
            return response
        except Exception as e:
            return {"error": f"An error occurred: {str(e)}"},500

    def studid_to_shard(self, sid):
        try:
            self.connect()
            query = '''select Shard_id from ShardT_Schema where %s BETWEEN Stud_id_low AND Stud_id_low+Shard_size'''
            response = self.update_query(query, (sid,))
            print('Stud_id_to_shard',response,flush=True)
            return response[0][0]
        except Exception as e:
            return {"error": f"An error occurred: {str(e)}"},500

    def get_shard_servers(self,sid):
        try:
            self.connect()
            query = '''select Server_id from MapT_Schema where Shard_id = %s'''
            response = self.update_query(query, (sid,))
            return response[0]
        except Exception as e:
            return {"error": f"An error occurred: {str(e)}"},500

    def get_shard_idx(self,sid):
        try:
            self.connect()
            query = '''select valid_idx from ShardT_Schema where Shard_id = %s'''
            response = self.update_query(query, (sid,))
            return response[0][0]
        except Exception as e:
            return {"error": f"An error occurred: {str(e)}"},500

    def update_shard_idx(self, new_idx, sid):
        try:
            self.connect()
            query = '''update ShardT_Schema SET valid_idx = %s where Shard_id = %s'''
            response = self.update_query(query, (new_idx, sid))
            return response
        except Exception as e:
            return {"error": f"An error occurred: {str(e)}"},500