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

    def initialize_shard_tables(self, payload):
        self.connect()
        try:        
            # Extract schema and shards from the payload
            schema = payload.get('schema', {})
            columns = schema.get('columns', [])
            dtypes = schema.get('dtypes', [])
            shards = payload.get('shards', [])
    
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
    