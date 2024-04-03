from flask import Flask,jsonify,redirect,request
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

    def initialize_shard_tables(self, payload, server_name):
        
        try:        
            # Extract schema and shards from the payload
            self.connect()
            schema = payload.get('schema', {})
            columns = schema.get('columns', [])
            dtypes = schema.get('dtypes', [])
            shards = payload.get('shards', [])

            response_string = ""
            # Create shard tables in the database
            for shard in shards:
                table_name = f'StudT_{shard}'
                create_table_query = f'''
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        {', '.join([f'{column} INT' if dtype == 'Number' else f'{column} VARCHAR(100)' if dtype == 'String' else f'{column} {dtype}' for column, dtype in zip(columns, dtypes)])},
                        PRIMARY KEY (Stud_id)
                    );
                '''
    
                print(create_table_query,flush=True)
                self.query(create_table_query) 
            return {"message": "Shard tables initialized successfully"},200
    
        except Exception as e:
            return {"error": f"An error occurred: {str(e)}"},500
        
    def write_to_database(self, payload):
        
        try:
            self.connect()
            shard = payload.get('shard', "\0")
            curr_idx = payload.get('curr_idx', 0)
            data = payload.get('data', [])
            table_name = f'StudT_{shard}'
            
            for i in range(len(data)):
                Stud_id = data[i].get('Stud_id',0)
                Stud_name = str(data[i].get('Stud_name',""))
                Stud_marks = data[i].get('Stud_marks',"")
                insert_into_table_query = f'''
                        INSERT INTO {table_name} (Stud_id, Stud_name, Stud_marks) VALUES ({Stud_id},'{Stud_name}',{Stud_marks});
                    '''
                self.query(insert_into_table_query) 
            
                print(insert_into_table_query,flush=True)
            return jsonify({
                'message': {
                    "message": "Data entries added",
                    "current_idx": curr_idx+len(data),
                    "status" : "success"
                    },
                    "status": "successful"
                }), 200
    
        except Exception as e:
            return {"error": f"An error occurred: {str(e)}"},500

    def update_to_database(self, payload):
        
        try:
            self.connect()
            shard = payload.get('shard', "\0")
            original_Stud_id = payload.get('Stud_id', 0)
            data = payload.get('data', {})
            table_name = f'StudT_{shard}'

            Stud_id = data.get('Stud_id',0)
            Stud_name = str(data.get('Stud_name',""))
            Stud_marks = data.get('Stud_marks',0)
           
            update_into_table_query = f'''
                    UPDATE {table_name} SET Stud_id={Stud_id},Stud_name='{Stud_name}',Stud_marks={Stud_marks} WHERE Stud_id={original_Stud_id};
                '''
            self.query(update_into_table_query) 
        
            print(update_into_table_query,flush=True)
            return jsonify({
                'message': {
                    "message": f"Data entry for Stud_id:{Stud_id} updated",
                    "status" : "success"
                    },
                    "status": "successful"
                }), 200
    
        except Exception as e:
            return {"error": f"An error occurred: {str(e)}"},500
        
    def delete_to_database(self, payload):
        
        try:
            self.connect()
            shard = payload.get('shard', "\0")
            Stud_id = payload.get('Stud_id', 0)
            table_name = f'StudT_{shard}'
           
            delete_into_table_query = f'''
                    DELETE FROM {table_name} WHERE Stud_id={Stud_id};
                '''
            self.query(delete_into_table_query) 
        
            print(delete_into_table_query,flush=True)
            return jsonify({
                'message': {
                    "message": f"Data entry for Stud_id:{Stud_id} removed",
                    "status" : "success"
                    },
                    "status": "successful"
                }), 200
    
        except Exception as e:
            return {"error": f"An error occurred: {str(e)}"},500
        
    def read_from_database(self, payload):
        
        try:
            self.connect()
            shard = payload.get('shard', "\0")
            Stud_id = payload.get('Stud_id', 0)
            stud_id_low = Stud_id.get('low', 0)
            stud_id_high = Stud_id.get('high', 0)
            table_name = f'StudT_{shard}'
           
            read_from_table_query = f'''
                    SELECT * FROM {table_name} WHERE Stud_id BETWEEN {stud_id_low} AND {stud_id_high};
                '''
            data = []
            result = self.query(read_from_table_query) 
            for row in result:
                row_dict = {}
                row_dict["Stud_id"] = row[0]
                row_dict["Stud_name"] = row[1]
                row_dict["Stud_marks"] = row[2]
                data.append(row_dict)
        
            print(read_from_table_query,flush=True)
            return jsonify({
                'message': {
                    "data": data,
                    "status" : "success"
                    },
                    "status": "successful"
                }), 200
    
        except Exception as e:
            return {"error": f"An error occurred: {str(e)}"},500
        
    def copy_from_database(self, payload):
        
        try:
            self.connect()
            details = {}
            shards = payload.get('shards', [])
            for shard in shards:
                data = []
                table_name = f'StudT_{shard}'
            
                copy_from_table_query = f'''
                        SELECT * FROM {table_name};
                    '''
                result = self.query(copy_from_table_query) 
                for row in result:
                    row_dict = {}
                    row_dict["Stud_id"] = row[0]
                    row_dict["Stud_name"] = row[1]
                    row_dict["Stud_marks"] = row[2]
                    data.append(row_dict)
                details[shard] = data
        
                print(copy_from_table_query,flush=True)

            return jsonify({
                    **details,
                    "status" : "success"
                }), 200
    
        except Exception as e:
            return {"error": f"An error occurred: {str(e)}"},500

    