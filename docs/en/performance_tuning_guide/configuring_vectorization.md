# Configuring Vectorized Execution Engine

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-07-17T06:49:46.824Z -->

openGauss supports the row execution engine and the vectorized execution engine, which correspond to row-store tables and column-store tables respectively.

- One batch at a time, reading more data and saving I/O.
- More records in a batch, improving CPU cache hit rate.
- Pipeline mode execution, reducing the number of function calls.
- Processes a batch of data at a time, achieving high efficiency.

Therefore, the openGauss database can achieve better query performance for complex analytical queries. However, column-store tables perform poorly in data insertion and data update, making them unsuitable for services with frequent data insertion and update operations.

To improve the query performance of row-store tables in complex analytical queries, the openGauss database provides the capability for row-store tables to use the vectorized execution engine. By setting the GUC parameter [try_vector_engine_strategy](../database_reference/optimizer_method_configuration.md), you can convert queries involving row-store tables into vectorized execution plans for execution.

Converting row-store tables to the vectorized execution engine is not applicable to all query scenarios. Referring to the advantages of the vectorized engine, performance gains can be achieved through vectorized execution when a query involves operations such as expression evaluation, multi-table joins, and aggregation. In principle, converting row-store tables to vectorized execution incurs conversion overhead, which may lead to performance degradation. However, the aforementioned operations — expression evaluation, join operations, and aggregation operations — can achieve performance gains after being converted to vectorized execution. Therefore, whether performance improves after converting a query to vectorized execution depends on whether the performance gains from vectorized execution can outweigh the conversion overhead.

Taking TPCH Q1 as an example, when using the row execution engine, the scan operator takes 405210 ms and the aggregation operation takes 2618964 ms. After converting to the vectorized execution engine, the scan operator (SeqScan + VectorAdapter) takes 470840 ms and the aggregation operation takes 212384 ms, resulting in an overall performance improvement for the query.

TPCH Q1 row execution engine execution plan:

```sql
                                                                QUERY PLAN                                                                 
-------------------------------------------------------------------------------------------------------------------------------------------
 Sort  (cost=43539570.49..43539570.50 rows=6 width=260) (actual time=3024174.439..3024174.439 rows=4 loops=1)
   Sort Key: l_returnflag, l_linestatus
   Sort Method: quicksort  Memory: 25kB
   ->  HashAggregate  (cost=43539570.30..43539570.41 rows=6 width=260) (actual time=3024174.396..3024174.403 rows=4 loops=1)
         Group By Key: l_returnflag, l_linestatus
         ->  Seq Scan on lineitem  (cost=0.00..19904554.46 rows=590875396 width=28) (actual time=0.016..405210.038 rows=596140342 loops=1)
               Filter: (l_shipdate <= '1998-10-01 00:00:00'::timestamp without time zone)
               Rows Removed by Filter: 3897560
 Total runtime: 3024174.578 ms
(9 rows)
```

TPCH Q1 vectorized execution engine execution plan:

```sql
                                                                             QUERY PLAN                                                                             
--------------------------------------------------------------------------------------------------------------------------------------------------------------------
 Row Adapter  (cost=43825808.18..43825808.18 rows=6 width=298) (actual time=683224.925..683224.927 rows=4 loops=1)
   ->  Vector Sort  (cost=43825808.16..43825808.18 rows=6 width=298) (actual time=683224.919..683224.919 rows=4 loops=1)
         Sort Key: l_returnflag, l_linestatus
         Sort Method: quicksort  Memory: 3kB
         ->  Vector Sonic Hash Aggregate  (cost=43825807.98..43825808.08 rows=6 width=298) (actual time=683224.837..683224.837 rows=4 loops=1)
               Group By Key: l_returnflag, l_linestatus
               ->  Vector Adapter(type: BATCH MODE)  (cost=19966853.54..19966853.54 rows=596473861 width=66) (actual time=0.982..470840.274 rows=596140342 loops=1)
                     Filter: (l_shipdate <= '1998-10-01 00:00:00'::timestamp without time zone)
                     Rows Removed by Filter: 3897560
                     ->  Seq Scan on lineitem  (cost=0.00..19966853.54 rows=596473861 width=66) (actual time=0.364..199301.737 rows=600037902 loops=1)
 Total runtime: 683225.564 ms
(11 rows)
```