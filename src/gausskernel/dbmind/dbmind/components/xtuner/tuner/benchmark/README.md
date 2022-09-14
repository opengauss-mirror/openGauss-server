# Benchmark
This directory contains benchmark scripts. We implemented some demos here, they are:

|Benchmark|Filename|Scenario|Website|
| ------- | ------ | -------| ----- |
|TPC-C|tpcc.py|heavy concurrent transactions|http://www.tpc.org/tpcc/|
|TPC-H|tpch.py|data analytic for complex queries|http://www.tpc.org/tpch/|
|TPC-DS|tpds.py|simulate a decision support system|http://www.tpc.org/tpcds/|
|sysbench|sysbench.py|some simple queries for light concurrent.|https://github.com/akopytov/sysbench|

# Implementation
If you want to implement your benchmark which simulate your 
production scenario, you should do as the following.
**Firstly, we assume that xxx is your benchmark name.**

* Set file name as ```xxx.py``` in the benchmark directory;
* Implement a function named 'run' in xxx.py and declare two global variables of the string type, one called 'path' and the other called 'cmd'.;
* The signature of `run()` must be ```def run(remote_server_ssh, local_host_ssh) -> float```, that 
means you could use two SSH sessions to implement your code logic if they are useful, and you should 
return a numeric value as feedback;
* You can set xxx, the name of benchmark you want to run, in the xtuner.conf. The parameter name is `benchmark_script`.

# References
1. https://opengauss.org/zh/docs/1.0.1/docs/Developerguide/%E6%B5%8B%E8%AF%95TPCC%E6%80%A7%E8%83%BD.html
2. https://www.modb.pro/db/29135
3. https://www.modb.pro/db/23243
4. https://ankane.org/tpc-h
5. https://ankane.org/tpc-ds
6. https://severalnines.com/database-blog/how-benchmark-postgresql-performance-using-sysbench

