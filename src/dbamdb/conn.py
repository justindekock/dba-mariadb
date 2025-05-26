from os import environ
from dotenv import load_dotenv
import pandas as pd
import mariadb

load_dotenv()
def main():
    database = DBConn('dev')
    # database.connect()
    # database.connection.select_db('test')
    
    database.insert('test1', ('c1', 'c2', 'c3'), [
        (1, 'abc', 'xyz'),
        (2, 'abc', 'xyz'),
        (3, 'abc', 'xyz')
        ])
    
    
class DBConn:
    def __init__(self, env='prod'):
        #load_dotenv()
        self.host = environ['TS_DOMAIN'] 
        self.port = int(environ['PROD_PORT' if env=='prod' else 'DEV_PORT'])
        self.database = environ['PROD_DB' if env=='prod' else 'DEV_DB']
        self.user = environ['DB_USER' if env=='prod' else 'DEV_USER']
        self.passw = environ['PASS' if env=='prod' else 'DEV_PASS']
        self.logpath = None
        self.connection = None
        self.conn_error = None
        
    
    def connect(self):
        try:
            self.connection = mariadb.connect(
                user = self.user, password = self.passw, host = self.host, 
                port = self.port, database = self.database
            )
        except mariadb.Error as e:
            self.conn_error = e
            print(e)
        
        return self.connection
    
    def delete_temp_player(self):
        q = 'delete from player_temp'
        
        before = self.select_count('player_temp')
        # print(f'Rows before deleting: {before}')
        
        conn = self.connect()
        conn.begin()
        cur = conn.cursor()
        
        try:
            cur.execute(q)
            conn.commit()
            
        except mariadb.Error as e:
            print(e)
            return e
        
        conn.close()
        
        after = self.select_count('player_temp')
        # print(f'Rows after deleting: {after}')
        
        return [f'Rows before deleting: {before}', f'Rows after deleting: {after}']
            
    def insert(self, table, fields, values):
        valid_table = table # TODO - add the validation back iN
        valid_fields = self.fields_str(fields)
        if len(values[0]) == len(fields):
                val_ph = '?'
                for i in range(1, len(values[0])):
                    val_ph = val_ph + ', ' + '?'
        
        q = f'insert ignore into {valid_table} ({valid_fields}) values ({val_ph})' 
        
        # print(q)
        # print(values)
        conn = self.connect()
        if self.connection:
            try:
                
                conn.begin()
                cur = conn.cursor()
                
                before = self.select_count(valid_table)
                
                cur.executemany(q, values)
                conn.commit()   
                
                after = self.select_count(valid_table)
                conn.close()
                
                
                return([f'Rows before insert: {before}', f'Rows after insert: {after}'])
                    
            except mariadb.Error as e:
                print(e)
                return e
                
        else: 
            return self.conn_error

            
    
    def show_db(self):
        conn = self.connect()
        cur = conn.cursor()
        cur.execute('show databases')
        for i in cur.fetchall():
            print(i[0])
            
    def show_users(self):
        conn = self.connect()
        cur = conn.cursor()
        cur.execute('select user, host from mysql.user')
        for i in cur.fetchall():
            print(f'{i[0]} : {i[1]}')
            
    def show_tables(self):
        conn = self.connect()
        cur = conn.cursor()
        cur.execute(f'show tables from {self.database}')
        for i in cur.fetchall():
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
        if db_tables:
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
        valid_table = table#self.table_validation(table)
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
    def select(self, query, fetch_type='fetchone') -> pd.DataFrame:    
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
    
    # return the count of a table
    def select_count(self, table) -> int:
        conn = self.connect()
        cur = conn.cursor()
        valid_table = table#self.table_validation(table)
        if valid_table == table:
            cur.execute(f'select count(*) from {valid_table}')
            return cur.fetchone()[0]
        conn.close()
        
if __name__ == '__main__':
    main()
