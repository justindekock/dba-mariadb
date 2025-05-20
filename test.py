from dbamdb import conn

connect = conn.Conn()
print(connect.show_tables())