import mysql.connector as con

db_config = {
   
    'host': 'lb_server',
    'user': 'root',
    'password': 'abcd',
    'database': 'STUDENT',
    'port' : 3306
}

class SQLHandler:
    def __init__(self):
        self.connection = None
        
    def connect_to_database(self):
        try:
            self.connection = con.connect(**db_config)
        except con.Error as e:
            print("Error connecting to MySQL database:", e)
    
    # def has_table(self,data):
    #     if self.connection is None:
    #         self.connect_to_database()
    #         if self.connection is None:
    #             return "Error : COnnection to database failed"
    #     try:
    #         cursor = self.connection.cursor()
    #         all_tables = cursor.execute("SHOW TABLES")
    #         if all_tables:
    #             if data in [r[0] for r in all_tables]:
    #                 return True
        
    #         return False
    #     except con.Error as e:
    #         print("error connecting to the table")

    def initialize_shard_tables(self, payload):
        if self.connection is None:
            self.connect_to_database()  # Ensure connection is established
            if self.connection is None:
                return "Error: Connection to database failed."
        
        try:
            cursor = self.connection.cursor()
        
            # Extract schema and shards from the payload
            N = payload.get('N')
            schema = payload.get('schema', {})
            columns = schema.get('columns', [])
            dtypes = schema.get('dtypes', [])
            shards = payload.get('shards', [])
            servers = payload.get('servers',{})

            print(N,schema,columns,dtypes,shards,servers,flush = True)
            
            if len(servers) <= N:
                create_table_query = """
                    CREATE TABLE IF NOT EXISTS ShardT_Schema (
                        Stud_id_low INT NOT NULL,
                        Shard_id VARCHAR(255) NOT NULL,
                        Shard_size INT NOT NULL,
                        valid_idx INT NOT NULL,
                        PRIMARY KEY (Stud_id_low),
                        CHECK (LENGTH(Stud_id_low) = 6),
                        CHECK (valid_idx >= Stud_id_low AND valid_idx <= Stud_id_low + Shard_size)
                    )
                    """

                cursor.execute(create_table_query)
                self.connection.commit()
                #cursor.execute("USE STUDENT")
                #cursor.execute("create table if not exists MapT_Schema (Shard_id varchar(255) not null,Server_id varchar(255) not null)")
                
                table = cursor.execute("SHOW TABLES")
                print(table,flush = True)
                # for shard in shards:
                #     insert_query = "INSERT INTO ShardT_Schema (Stud_id_low, Shard_id, Shard_size, valid_idx) VALUES (" + str(shard['Stud_id_low']) + ", '" + str(shard['Shard_id']) + "', " + str(shard['Shard_size']) + ", " + str(shard['Stud_id_low']) + ")"
                #     cursor.execute(insert_query)
                #     print(insert_query,flush = True)
                
                # for key,val in servers.items():
                #     for v in val:
                #         insert_query_ser = f"INSERT INTO MapT_Schema (Shard_id,Server_id) VALUES ({v},{key})"
                #         cursor.execute(insert_query_ser)
                #         print(insert_query_ser,flush = True)
                
                # #write code to call config for every server and their shard .

                self.connection.commit()
                return {"message": "Configured Database","status":"Success"},200
            
            return{"error" :"Length of Servers List is greater Than N"},400
    
        except Exception as e:
            return {"error": f"An error occurred: {str(e)}"},500


    # Other database operations here...
    def insert_data(self, data):
        try:
            cursor = self.connection.cursor()
            cursor.execute("INSERT INTO your_table_name (column_name) VALUES (%s)", (data,))
            self.connection.commit()
            print("Data inserted successfully")
            cursor.close()
        except con.Error as e:
            print("Error inserting data:", e)

    def fetch_data(self):
        try:
            cursor = self.connection.cursor()
            cursor.execute("SELECT * FROM your_table_name")
            rows = cursor.fetchall()
            print("Fetched data:")
            for row in rows:
                print(row)
            cursor.close()
        except con.Error as e:
            print("Error fetching data:", e)

    def close_connection(self):
        try:
            self.connection.close()
            print("Connection closed")
        except con.Error as e:
            print("Error closing connection:", e)
