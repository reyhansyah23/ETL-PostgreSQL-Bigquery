# this script is for make a connection to database (postgresql)

import json
import psycopg2

class Connection:

    def __init__(self):
        f = open('config.json')
        self.config = json.load(f)
  
    # Connect to Database
    def connect_db(self):
        config = self.config['db_1']
        conn = None

        try:
            conn = psycopg2.connect(
                database=config['database'], 
                user=config['user'], 
                password=config['password'], 
                host=config['host'], 
                port=config['port']
                )
            return conn    
        except (Exception, psycopg2.DatabaseError) as error:
            return error
       

if __name__ == '__main__':
    # test connection
    job = Connection()
    job.connect_db()

