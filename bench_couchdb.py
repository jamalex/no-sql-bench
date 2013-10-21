from bench_base import BenchBase
from couchdb import *

query_template = """
    function(doc) {
        if (%s) {
            emit(doc);
        }
    }
    """

class BenchCouchDB(BenchBase):
    
    ID_FIELD = "_id"
    
    def __init__(self, *args, **kwargs):
        self.server = Server()
        super(BenchCouchDB, self).__init__(*args, **kwargs)
    
    def create_database(self):
        self.db = self.server.create(self.db_name)

    def delete_database(self):
        self.server.delete(self.db_name)
            
    def create(self, record):
        self.db[record[self.ID_FIELD]] = record
    
    def get(self, key):
        return self.db.get(key)
        
    def query(self, **kwargs):
        conditions = ["doc.%s == %r" % item for item in kwargs.items()]
        query_fn = query_template % " && ".join(conditions)
        results = self.db.query(query_fn)
        count = len(results) # force a lookup, in case it's lazy
        return results
    
    