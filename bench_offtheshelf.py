from bench_base import BenchBase
from offtheshelf import Database, Collection

import os

class BenchOffTheShelf(BenchBase):
    
    ID_FIELD = "_id"
    
    def __init__(self, *args, **kwargs):
        super(BenchOffTheShelf, self).__init__(*args, **kwargs)
    
    def create_database(self):
        self.db = Database(self.db_name + ".db")
        self.collection = self.db.get_collection("test_collection")

    def delete_database(self):
        self.db.close()
        os.remove(self.db_name + ".db")
            
    def create(self, record):
        self.collection.insert(record)
        self.db.save()
    
    def get(self, key):
        return self.collection.find_one({"_id": key})
        
    def query(self, **kwargs):
        return list(self.collection.find(kwargs))
    
    