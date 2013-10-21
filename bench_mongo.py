from bench_base import BenchBase
from pymongo import MongoClient

class BenchMongoDB(BenchBase):
    
    ID_FIELD = "_id"
    
    def __init__(self, *args, **kwargs):
        self.client = MongoClient()
        super(BenchMongoDB, self).__init__(*args, **kwargs)
    
    def create_database(self):
        self.db = self.client[self.db_name]
        self.collection = self.db.test_collection

    def delete_database(self):
        self.client.drop_database(self.db_name)
            
    def create(self, record):
        self.collection.insert(record)
    
    def get(self, key):
        return self.collection.find_one({"_id": key})
        
    def query(self, **kwargs):
        return list(self.collection.find(kwargs))
    
    