from bench_base import BenchBase
import json
import os
import sqlite3

create_table_command = """
    CREATE TABLE docs (id text primary key, name text, number int, small_number int, embedded text);
    """
    
create_index_command = """
    CREATE INDEX small_number_idx ON docs (small_number);
    """

insert_template = """
    INSERT INTO docs VALUES ('%(id)s','%(name)s',%(number)d,%(small_number)d,'%(embedded)s');
"""

get_template = """
    SELECT * FROM docs WHERE id='%s';
    """

query_template = """
    SELECT * FROM docs WHERE %s;
    """

def json_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        value = row[idx]
        if isinstance(value, basestring) and value[0] == "{":
            value = json.loads(value)
        d[col[0]] = value
    return d

class BenchSQLite(BenchBase):
    
    def __init__(self, *args, **kwargs):
        super(BenchSQLite, self).__init__(*args, **kwargs)
    
    def create_database(self):
        self.conn = sqlite3.connect(self.db_name + ".db")
        self.conn.row_factory = json_factory
        self.cursor = self.conn.cursor()
        self.cursor.execute(create_table_command)

    def delete_database(self):
        self.conn.close()
        os.remove(self.db_name + ".db")
            
    def create(self, record):
        for key, value in record.items():
            if isinstance(value, dict):
                record[key] = json.dumps(value)
        return self.cursor.execute(insert_template % record)
    
    def get(self, key):
        return self.cursor.execute(get_template % key).next()
        
    def query(self, **kwargs):
        conditions = ["%s=%r" % item for item in kwargs.items()]
        query_statement = query_template % " and ".join(conditions)
        results = self.cursor.execute(query_statement).fetchall()
        count = len(results) # force a lookup, in case it's lazy
        return results


class BenchSQLiteIndexed(BenchSQLite):
    
    def create_database(self, *args, **kwargs):
        super(BenchSQLiteIndexed, self).create_database(*args, **kwargs)
        self.cursor.execute(create_index_command)
