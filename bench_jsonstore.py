import os

from bench_base import BenchBase
from jsonstore.store import EntryManager

class BenchJsonStore(BenchBase):
    
    ID_FIELD = "__id__"
    
    def __init__(self, *args, **kwargs):
        super(BenchJsonStore, self).__init__(*args, **kwargs)
    
    def create_database(self):
        self.db = EntryManager(self.db_name + ".db")

    def delete_database(self):
        os.remove(self.db_name + ".db")
            
    def create(self, record):
        self.db.create(record)
    
    def get(self, key):
        return self.db.search(__id__=key)
        
    def query(self, **kwargs):
        results = self.db.search(kwargs)
        count = len(results) # force a lookup, in case it's lazy
        return results
    
    