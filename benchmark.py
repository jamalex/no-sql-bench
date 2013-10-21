import bench_sqlite
import bench_couchdb
import bench_jsonstore
import bench_mysql
import bench_codernity
import bench_mongo
import bench_offtheshelf
import bench_nosqlite

from math import sqrt
import time
import csv

dbs = {
    "sqlite3": bench_sqlite.BenchSQLite,
    "sqlite3_ind": bench_sqlite.BenchSQLiteIndexed,
    "couchdb": bench_couchdb.BenchCouchDB,
    "jsonstore": bench_jsonstore.BenchJsonStore,
    "mysql": bench_mysql.BenchMySQL,
    "mysql_ind": bench_mysql.BenchMySQLIndexed,
    "codernity_ind": bench_codernity.BenchCodernityDB,
    "mongodb": bench_mongo.BenchMongoDB,
    "offtheshelf": bench_offtheshelf.BenchOffTheShelf,
    "nosqlite": bench_nosqlite.BenchNoSQLite,
}

def benchmark_function(cls, function, count=1000, iterations=5, **kwargs):
    times = []
    
    bench = cls(count=count)
    bench.create_records()
    
    time.sleep(5)
    
    for i in range(iterations):
        if function == "create_records":
            bench = cls(count=count)
        start = time.time()
        getattr(bench, function)(**kwargs)
        stop = time.time()
        times.append(stop - start)
        
    return times
    

def compare_function(function, **kwargs):
    results = {}
    for db_name, db in dbs.items():
        times = benchmark_function(db, function, **kwargs)
        n = float(len(times))
        mean = sum(times) / n
        std = sqrt(sum((x-mean)**2 for x in times) / n)
        result = results[db_name] = {
            "mean": mean,
            "std": std,
        }
        
        first = times[0]
        mean_after_first = sum(times[1:]) / (n-1)
        
        # if the first run was particularly different, log it separately
        if abs(first - mean_after_first) > std * 2:
            result["mean_after_first"] = mean_after_first
            result["first"] = first
            
    min_mean = min([result["mean"] for result in results.values()])
    for result in results.values():
        result["factor"] = result["mean"] / float(min_mean)
        if "mean_after_first" in result:
            result["factor_after_first"] = result["mean_after_first"] / float(min_mean)
            result["factor_first"] = result["first"] / float(min_mean)
    return results
    

def full_benchmark(**kwargs):
    results = {}
    results["create_records"] = compare_function("create_records", iterations=3, **kwargs)
    results["lookup_by_id"] = compare_function("get_random_specific_record", iterations=500, **kwargs)
    results["query"] = compare_function("query", iterations=100, small_number=15, **kwargs)
    return results
    
def test_output_format():
    for db_name, db in dbs.items():
        d = db(count=1000)
        d.create_records()
        value = d.get_random_specific_record()
        try:
            assert isinstance(value["embedded"]["inner_small_number"], int)
        except:
            raise Exception("Wrong format for database '%s':\n\n%r" % (db_name, value))
        print "%s: Success!" % db_name

def save_as_csv(results, filename="results.csv"):
    with open(filename, "wb") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["DB Name", "Create 10000 records (seconds)", "Create 10000 records (factor)", "Lookup by ID (seconds)", "Lookup by ID (factor)", "Query by field (seconds)", "Query by field (factor)"])
        for db_name in results["create_records"]:
            row = [db_name]
            row += [results["create_records"][db_name]["mean"], results["create_records"][db_name]["factor"]]
            row += [results["lookup_by_id"][db_name]["mean"], results["lookup_by_id"][db_name]["factor"]]
            row += [results["query"][db_name]["mean"], results["query"][db_name]["factor"]]
            writer.writerow(row)