# Benchmark
This directory contains benchmark scripts. We implemented some demos here, they are:

|Benchmark|Filename|Scenario|Website|
| ------- | ------ | -------| ----- |
|TPC-C|bm_tpcc.py|heavy concurrent transactions|http://www.tpc.org/tpcc/|
|TPC-H|bm_tpch.py|data analytic for complex queries|http://www.tpc.org/tpch/|
|TPC-DS|bm_tpds.py|simulate a decision support system|http://www.tpc.org/tpcds/|
|sysbench|bm_sysbench.py|some simple queries for light concurrent.|https://github.com/akopytov/sysbench|

# Interface
You can see code in ```__init__.py```:

```python
import importlib
from ssh import ExecutorFactory

local_ssh = ExecutorFactory() \
    .set_host('127.0.0.1') \
    .get_executor()


def get_benchmark_instance(name):
    bm = importlib.import_module('bm_{}'.format(name))  # load benchmark script by filename.
    
    # a wrapper for benchmark run() function:
    def wrapper(server_ssh):
        res = bm.run(server_ssh, local_ssh)  # implement your own run() function.
        return res
    return wrapper

```

So if you want to implement your own benchmark which simulate your 
production scenario, you should do as the following.
**First of all, we assume that xxx is your benchmark name.**

* Set filename as ```bm_xxx.py```;
* Implement a function named run() in bm_xxx.py;
* The signature of run() must be ```def run(remote_server_ssh, local_host_ssh) -> float```, that 
means you could use two SSH sessions to implement your code logic if they are useful, and you should 
return a numeric value as feedback;
* Pass argument '--benchmark xxx' when execute ```main.py```

**You could see a detailed demonstration -- bm_tpcc.py.**