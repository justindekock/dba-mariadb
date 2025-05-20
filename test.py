from dbamdb import conn

connect = conn.Conn('dev')

print(connect.select('player', 'fetchall'))
# print(connect.show_tables())