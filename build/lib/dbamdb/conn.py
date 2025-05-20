from os import environ
from dotenv import load_dotenv
import pandas as pd
import mariadb

load_dotenv()
def main():
    conn = Conn()
    conn.show_db()
    conn.show_users()
    conn.show_tables('nba')
    
class Conn:
    def __init__(self, env='prod'):
        #load_dotenv()
        self.shit = 'FUCK!!!'
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
            
    # get the a list of the fields in the cursor
    def curcols(self, cur):
        return cur.metadata['field']
    
    def fields_str(self, db_fields):
        # first field starts the string, then add a comma and the next field
        valid_fields = db_fields[0] 
        for i in range(1, len(db_fields)):
            valid_fields = valid_fields + ', ' + db_fields[i]
        return valid_fields
             
    
    def table_validation(self, input_table):
        db_tables = self.show_tables()
        for db_table in db_tables:
            if db_table == input_table:
                return db_table
        return f"Invalid input: '{input_table}' does not exist in database"
    
    def field_validation(self, table, fields):
        db_fields = self.show_cols(table)['Field'].unique().tolist()
        bad_fields = []
        for i, field in enumerate(fields): 
            if field not in db_fields:
                bad_fields.append(field)
                del fields[i]
                
        if not bad_fields:
            return self.fields_str(db_fields)
        else:
            return bad_fields
        
    def show_cols(self, table='team'):
        valid_table = self.table_validation(table)
        if valid_table == table:
            q = f'show columns from {valid_table}'
            conn = self.connect()
            cur = conn.cursor()
            cur.execute(q)
            res = cur.fetchall()
            return pd.DataFrame(res, columns=self.curcols(cur))
        else:
            raise Exception(valid_table)
        
    # select season, players, teams, players/teams together    
    def select(self, query, fetch_type='fetchone', table='team') -> pd.DataFrame:    
        queries = {
            'season': 'select * from season',
            'player': 'select * from player',
            'team': 'select * from team',
            'player_team': '''
                select 
                    a.player, b.team_name, b.lg, a.active 
                from player a 
                    inner join team b 
                    on b.team_id = a.team_id
                '''
        }
        
        q = queries[query]
        
        conn = self.connect()
        cur = conn.cursor()
        cur.execute(q)
        
        if fetch_type == 'fetchone':
            res = cur.fetchone()
        elif fetch_type == 'fetchall':
            res = cur.fetchall()
        else: 
            raise Exception('fetch_type should be fetchone or fetchall')
        
        # return query output as df
        return pd.DataFrame(res, columns=self.curcols(cur))
        
if __name__ == '__main__':
    main()
