import sqlite3
from sqlite3 import Error



class WeatherSQLlite:
    def __init__(self, db_file):
        self.db_file = db_file
        self.table_created = False
        self.conn = None
        
    def __del__(self):
        self.terminate_connection()
        
    def create_schema(self, columns_name_type):
        final_column_name = columns_name_type
        my_string = '''
                    DROP TABLE IF EXISTS WEATHER;
                    CREATE TABLE IF NOT EXISTS WEATHER (
                    '''
        for key, value in final_column_name.items():
            restriction = ''
            if key == 'id':
                restriction = 'PRIMARY KEY'
            else:
                restriction = 'NOT NULL'
            my_string += key + ' ' + value + restriction + ','
        my_string = my_string[:-1] #drop last comma
        my_string += ');'
        return my_string
        
        
    def create_connection(self, columns_name_type):
        """ create a database connection to a SQLite database """
        conn = None
        db_file = self.db_file
        try:
            self.conn = sqlite3.connect(db_file)
            print('Connected...')
            cur = self.conn.cursor()
            if not self.table_created:
                sql_create_table = self.create_schema(columns_name_type)
                cur.executescript(sql_create_table)
                self.table_created = True
        except Error as e:
            print(e)
        return conn

    def insert_entries_to_DB(self, sql_data):
        # conn = self.create_connection()
        cur = self.conn.cursor()
        for data in sql_data:
            sql_insert = data[0]
            cur.execute(sql_insert, data[1])
            self.conn.commit ()

    def get_data(self, sql_query):
        cur = self.conn.cursor()
        cur.execute(sql_query)
        data = cur.fetchall()
        return data
    
    def terminate_connection(self):
        if self.conn:
            self.conn.close()
        
