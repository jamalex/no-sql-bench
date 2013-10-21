no-sql-bench
============

Some benchmarks for comparing read/write/query speeds of candidate DBs for KA Lite.

Install dependencies (from inside a virtualenv if desired):

`pip install -r requirements.txt`

Then, from the Python (or IPython) interpreter, you can `import benchmark` and run one of the following:

`benchmark.benchmark_function(<benchclass>, <functionname>)`
(where `<benchclass>` is one of the db backends specified inside the `benchmark.dbs` dictionary, and <functionname> is the method name on that class you want to benchmark)

`benchmark.compare_function(<functionname>)`
(benchmark the function across all databases in the `benchmark.dbs` dictionary, and compare results)

`benchmark.full_benchmark()`
(benchmark all the main functions across all databases in the `benchmark.dbs` dictionary)

Note that you'll want to comment out all lines in the `benchmark.dbs` dictionary that you don't want to test.

* If you want to test MongoDB, it must be installed and mongod must be running.
* If you want to test MySQL, it must be installed/running and have permissions on the "test" database for user/pass "test"/"test" (`grant all on test.* to 'test'@'localhost' identified by 'test';`).
* If you want to test CouchDB, it must be installed and running.
* All other DBs are pure Python, and don't need to have anything else installed.

The wrappers for all databases will return results as a dictionary, fully deserialized. The data structure being stored in each DB is currently:

    {
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


