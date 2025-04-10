from typing import List, Tuple
from datetime import date
import psycopg2
from psycopg2.extras import execute_values 
class Loader:
    def __init__(self,db_host, db_port, db_database, db_username, db_password):
        self.db_host = db_host
        self.db_port = db_port
        self.db_database = db_database
        self.db_username = db_username
        self.db_password = db_password
    
    def load(self, data : List[Tuple[date,float]]):
        conn = None
        try:
            conn = psycopg2.connect(host = self.db_host,
                port = self.db_port,
                database=self.db_database,
                user = self.db_username,
                password = self.db_password) 

            cur = conn.cursor() 
            cur.execute('''CREATE TABLE IF NOT EXISTS us_sales_data(
                                sales_date DATE NOT NULL,
                                sales_amount float NOT NULL);''')       

            
            insert_query = '''
            INSERT INTO Us_Sales_Data (sales_date, sales_amount)
            VALUES %s
            '''
            execute_values(cur,insert_query,data)

            conn.commit()
        except Exception as e:
            if conn is not None:
                conn.rollback()
            raise
        finally:
            if conn is not None:
                conn.close()      
