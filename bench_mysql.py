from bench_base import BenchBase
import json
import MySQLdb
import MySQLdb.cursors

create_table_command = """
    CREATE TABLE %s (id char(64) primary key, name char(128), number int, small_number int, embedded text);
    """
    
create_index_command = """
    CREATE INDEX small_number_idx ON %s (small_number);
    """

insert_template_1 = """INSERT INTO %s VALUES """
insert_template_2 = """('%(id)s','%(name)s',%(number)d,%(small_number)d,'%(embedded)s');"""

get_template = """
    SELECT * FROM %s WHERE id='%s';
    """

query_template = """
    SELECT * FROM %s WHERE %s;
    """    

def jsonify_text_fields(data):
    for key, value in data.items():
        if isinstance(value, basestring) and value[0] == "{":
            data[key] = json.loads(value)
    return data

class BenchMySQL(BenchBase):
    
    def __init__(self, *args, **kwargs):
        super(BenchMySQL, self).__init__(*args, **kwargs)
    
    def create_database(self):
        try:
             self.db = MySQLdb.connect(db="test", user="test", passwd="test", cursorclass=MySQLdb.cursors.DictCursor)
        except:
             raise Exception("Need to run in mysql: grant all on test.* to 'test'@'localhost' identified by 'test';")
        self.cursor = self.db.cursor()
        self.cursor.execute(create_table_command % self.db_name)
        
    def delete_database(self):
        self.cursor.execute("DROP TABLE %s;" % self.db_name)
            
    def create(self, record):
        for key, value in record.items():
            if isinstance(value, dict):
                record[key] = json.dumps(value)
        return self.cursor.execute((insert_template_1 % self.db_name) + (insert_template_2 % record))
    
    def get(self, key):
        self.cursor.execute(get_template % (self.db_name, key))
        return jsonify_text_fields(self.cursor.fetchone())
        
    def query(self, **kwargs):
        conditions = ["%s=%r" % item for item in kwargs.items()]
        query_statement = query_template % (self.db_name, " and ".join(conditions))
        self.cursor.execute(query_statement)
        results = self.cursor.fetchall()
        return map(jsonify_text_fields, results)
    
class BenchMySQLIndexed(BenchMySQL):
    
    def create_database(self, *args, **kwargs):
        super(BenchMySQLIndexed, self).create_database(*args, **kwargs)
        self.cursor.execute(create_index_command % self.db_name)

