from bench_base import BenchBase
from CodernityDB.database import Database
from CodernityDB.hash_index import HashIndex

import shutil

class WithSmallNumberIndex(HashIndex):

    def __init__(self, *args, **kwargs):
        kwargs['key_format'] = 'I'
        super(WithSmallNumberIndex, self).__init__(*args, **kwargs)

    def make_key_value(self, data):
        a_val = data.get("small_number")
        if a_val is not None:
            return a_val, None
        return None

    def make_key(self, key):
        return key


class BenchCodernityDB(BenchBase):
    
    ID_FIELD = "_id"
    
    def __init__(self, *args, **kwargs):
        super(BenchCodernityDB, self).__init__(*args, **kwargs)
    
    def create_database(self):
        self.db = Database(self.db_name)
        self.db.create()
        self.db.add_index(WithSmallNumberIndex(self.db.path, "small_number"))

    def delete_database(self):
        self.db.close()
        shutil.rmtree(self.db_name)
            
    def create(self, record):
        self.db.insert(record)
    
    def get(self, key):
        return self.db.get("id", key, with_doc=True)
        
    def query(self, **kwargs):
        key, val = kwargs.items()[0]
        return list(self.db.get_many(key, val, limit=-1, with_doc=True))
    
    