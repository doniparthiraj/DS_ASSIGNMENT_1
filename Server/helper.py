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
    
    def initialize_shard_tables(self, payload):
        if self.connection is None:
            self.connect_to_database()  # Ensure connection is established
            if self.connection is None:
                return "Error: Connection to database failed."
        
        try:
            cursor = self.connection.cursor()
        
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
                cursor.execute(create_table_query)
 
            self.connection.commit()
 
            return {"message": "Shard tables initialized successfully"}
    
        except Exception as e:
            return {"error": f"An error occurred: {str(e)}"}

    # Other database operations here...
    def insert_data(self, data):
        try:
            cursor = self.connection.cursor()
            cursor.execute("INSERT INTO your_table_name (column_name) VALUES (%s)", (data,))
            self.connection.commit()
            print("Data inserted successfully")
            cursor.close()
        except mysql.connector.Error as e:
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
        except mysql.connector.Error as e:
            print("Error fetching data:", e)

    def close_connection(self):
        try:
            self.connection.close()
            print("Connection closed")
        except mysql.connector.Error as e:
            print("Error closing connection:", e)
