import random
import uuid
from exceptions import NotImplementedError

def default_generator():
    return {
        "name": uuid.uuid4().hex.upper(),
        "number": random.randint(1, 1000000000),
        "small_number": random.randint(1, 20),
        "embedded": {
            "inner_name": uuid.uuid4().hex.upper(),
            "inner_number": random.randint(1, 1000000000),
            "inner_small_number": random.randint(1, 20),
            "inner_fixed_name": "Bob",
        },
    }

class BenchBase(object):
    
    db_name = None
    records = None
    count = None
    ID_FIELD = "id"
    
    def __init__(self, count=1000):
        self.db_name = "test_" + uuid.uuid4().hex
        self.count = count
        self.create_database()
        self.initialize_records()

    def create_database(self):
        raise NotImplementedError("Must define a 'create_database' method.")

    def delete_database(self):
        raise NotImplementedError("Must define a 'delete_database' method.")
    
    def initialize_records(self, generator=default_generator):
        self.records = []
        for i in range(self.count):
            record = generator()
            record[self.ID_FIELD] = uuid.uuid4().hex
            self.records.append(record)
    
    def create_records(self):
        assert self.records, "Records have not been initialized."
        for record in self.records:
            self.create(record)
        
    def create(self, record):
        raise NotImplementedError("Must define a 'create' method.")
    
    def get(self, key):
        raise NotImplementedError("Must define a 'get' method.")
    
    def get_random_specific_record(self):
        assert self.records, "Records have not been initialized."
        return self.get(random.choice(self.records)[self.ID_FIELD])
    
    def query(self, **kwargs):
        raise NotImplementedError("Must define a 'query' method.")
        
    def __del__(self):
        self.delete_database()