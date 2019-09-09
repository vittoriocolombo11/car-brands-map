import pandas as pd
import sqlite3

class Database:
    def __init__(self, dbname):
        self.dbname = dbname
        self.conn = sqlite3.connect(dbname)
        
    def query(self, q):
        return pd.read_sql_query(q, self.conn)
    
    def to_table(self, df, name):
        df.to_sql(name, self.conn, index=False)
    
    def insert_record(self, table, val):
        cur = self.conn.cursor()
        cur.execute(f"""
        INSERT INTO {table} VALUES {tuple(val)}
        """)
        self.conn.commit()
        
    def insert_records(self, table, vals):
        s =  f"INSERT INTO {table} VALUES\n"
        for val in vals:
            s += f"{tuple(val)},\n"
        s = s[:-2]
        cur = self.conn.cursor()
        cur.execute(s)
        self.conn.commit()
        
        
    def drop_table(self, table):
        s = "DROP TABLE IF EXISTS " + table
        cur = self.conn.cursor()
        cur.execute(s)
        self.conn.commit()
        
    def drop_view(self, view):
        s = "DROP VIEW IF EXISTS " + view
        cur = self.conn.cursor()
        cur.execute(s)
        self.conn.commit()
        
    def create_table(self, table, schema):
        s = f"CREATE TABLE {table} (\n"
        for (k, v) in schema.items():
            s += f" {k} {v},\n"
        s = s[:-2] + ")"
        cur = self.conn.cursor()
        cur.execute(s)
        self.conn.commit()
        
    def create_view(self, view, query, temp=True):
        s = "CREATE "
        if temp:
            s += "TEMP "
        s += f"VIEW {view} AS\n" + query
        cur = self.conn.cursor()
        cur.execute(s)
        self.conn.commit()
        
    def tables(self):
        s = """
        SELECT type, name, sql 
        FROM sqlite_master 
        WHERE type='table' OR type='view' ;
        """
        return self.query(s)
    
    def table_info(self, table):
        return self.query(f"PRAGMA table_info({table})")
