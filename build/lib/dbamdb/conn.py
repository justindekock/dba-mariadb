from os import environ
from dotenv import load_dotenv
import mariadb

def main():
    conn = Conn()
    conn.show_db()
    conn.show_users()
    conn.show_tables('nba')
    
class Conn:
    def __init__(self, env='prod'):
        load_dotenv()
        self.host = environ['TS_DOMAIN'] 
        self.port = int(environ['PROD_PORT' if env=='prod' else 'DEV_PORT'])
        self.database = environ['PROD_DB' if env=='prod' else 'DEV_DB']
        self.user = environ['DB_USER' if env=='prod' else 'DEV_USER']
        self.passw = environ['PASS' if env=='prod' else 'DEV_PASS']
        self.conn = self.connect()
        self.cur = self.conn.cursor()
    
    def connect(self):
        try:
            return mariadb.connect(
                user = self.user, password = self.passw, host = self.host, 
                port = self.port, database = self.database
            )
        except mariadb.Error as e:
            print(e)
            
    def show_db(self):
        self.cur.execute('show databases')
        for i in self.cur.fetchall():
            print(i[0])
            
    def show_users(self):
        self.cur.execute('select user, host from mysql.user')
        for i in self.cur.fetchall():
            print(f'{i[0]} : {i[1]}')
            
    def show_tables(self):
        self.cur.execute(f'show tables from {self.database}')
        for i in self.cur.fetchall():
            print(i[0])
        
        
if __name__ == '__main__':
    main()
