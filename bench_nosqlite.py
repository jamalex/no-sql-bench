from bench_base import BenchBase
from nosqlite import client, server

import shutil

class BenchNoSQLite(BenchBase):
    
    ID_FIELD = "_id"
    
    def __init__(self, *args, **kwargs):
        super(BenchNoSQLite, self).__init__(*args, **kwargs)
    
    def create_database(self):
        self.server = server(directory=self.db_name)
        self.client = client(self.server.port)
        self.db = self.client.db
        self.collection = self.db.test_collection

    def delete_database(self):
        self.server.quit()
        shutil.rmtree(self.db_name)
            
    def create(self, record):
        self.collection.insert(record)
    
    def get(self, key):
        return self.collection.find_one(_id=key)
        
    def query(self, **kwargs):
        return list(self.collection.find(**kwargs))
    
    