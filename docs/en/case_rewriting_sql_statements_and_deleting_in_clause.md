# Case: Rewriting SQL Statements and Deleting in-clause<a name="EN-US_TOPIC_0000001382071337"></a>

## Symptom <a name="en-us_topic_0075873761_section47054049113429"></a>

in-clause/any-clause is a common SQL statement constraint. Sometimes, the clause following **in** or **any** is a constant. For example:

```
select 
count(1) 
from calc_empfyc_c1_result_tmp_t1 
where ls_pid_cusr1 in ('20120405', '20130405')
```

Or

```
select 
count(1) 
from calc_empfyc_c1_result_tmp_t1 
where ls_pid_cusr1 in any('20120405', '20130405');
```

Sometimes, the **IN**/**ANY** clause is used as follows:

```
SELECT 
*
FROM test1 t1, test2 t2
WHERE t1.a = any(values(t2.a),(t2.b));
```

**a** and **b** are two columns in **t2**, and **"t1.a = any\(values\(t2.ba,\(t2.b\)\)"** is equivalent to **"t1.a = t2.a or t1.a = t2.b"**._

Therefore, join-condition is essentially an inequality, and nestloop must be used for this join operation. The execution plan is as follows:

```
                                                           QUERY PLAN
---------------------------------------------------------------------------------------------------------------------------------
 Nested Loop  (cost=0.00..138614.38 rows=2309100 width=16) (actual time=0.152..19225.483 rows=1000 loops=1)
   Join Filter: (SubPlan 1)
   Rows Removed by Join Filter: 999000
   ->  Seq Scan on test1 t1  (cost=0.00..31.49 rows=2149 width=8) (actual time=0.021..3.309 rows=1000 loops=1)
   ->  Materialize  (cost=0.00..42.23 rows=2149 width=8) (actual time=0.331..1265.810 rows=1000000 loops=1000)
         ->  Seq Scan on test2 t2  (cost=0.00..31.49 rows=2149 width=8) (actual time=0.013..0.268 rows=1000 loops=1)
   SubPlan 1
     ->  Values Scan on "*VALUES*"  (cost=0.00..0.03 rows=2 width=4) (actual time=2890.741..7372.739 rows=1999000 loops=1000000)
 Total runtime: 19227.328 ms
(9 rows)
```

## Optimization Description <a name="en-us_topic_0075873761_section53307463113534"></a>

The test result shows that both result sets are too large. As a result, nestloop is time-consuming with more than one hour to return results. Therefore, the key to performance optimization is to eliminate nestloop, using more efficient hashjoin. From the perspective of semantic equivalence, the SQL statements can be written as follows:

```
SELECT
*
FROM (
    SELECT * FROM test1 t1, test2 t2 WHERE t1.a = t2.a
    UNION
    SELECT * FROM test1 t1, test2 t2 WHERE t1.a = t2.b
);
```

The optimized SQL queries consist of two equivalent join subqueries, and each subquery can be used for hashjoin in this scenario. The optimized execution plan is as follows.

```
                                                           QUERY PLAN
---------------------------------------------------------------------------------------------------------------------------------
 HashAggregate  (cost=1634.99..2096.81 rows=46182 width=16) (actual time=6.369..6.772 rows=1000 loops=1)
   Group By Key: t1.a, t1.b, t2.a, t2.b
   ->  Append  (cost=58.35..1173.17 rows=46182 width=16) (actual time=0.833..3.414 rows=2000 loops=1)
         ->  Hash Join  (cost=58.35..355.67 rows=23091 width=16) (actual time=0.832..1.590 rows=1000 loops=1)
               Hash Cond: (t1.a = t2.a)
               ->  Seq Scan on test1 t1  (cost=0.00..31.49 rows=2149 width=8) (actual time=0.015..0.156 rows=1000 loops=1)
               ->  Hash  (cost=31.49..31.49 rows=2149 width=8) (actual time=0.531..0.531 rows=1000 loops=1)
                      Buckets: 32768  Batches: 1  Memory Usage: 40kB
                     ->  Seq Scan on test2 t2  (cost=0.00..31.49 rows=2149 width=8) (actual time=0.010..0.199 rows=1000 loops=1)
         ->  Hash Join  (cost=58.35..355.67 rows=23091 width=16) (actual time=0.694..1.421 rows=1000 loops=1)
               Hash Cond: (t1.a = t2.b)
               ->  Seq Scan on test1 t1  (cost=0.00..31.49 rows=2149 width=8) (actual time=0.010..0.160 rows=1000 loops=1)
               ->  Hash  (cost=31.49..31.49 rows=2149 width=8) (actual time=0.524..0.524 rows=1000 loops=1)
                      Buckets: 32768  Batches: 1  Memory Usage: 40kB
                     ->  Seq Scan on test2 t2  (cost=0.00..31.49 rows=2149 width=8) (actual time=0.008..0.177 rows=1000 loops=1)
 Total runtime: 7.759 ms
(16 rows)
```
