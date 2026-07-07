# **案例：改写SQL消除子查询（案例2）**<a name="ZH-CN_TOPIC_0000001331390682"></a>

## 现象描述<a name="section162711147142017"></a>

如下SQL语句：

```
UPDATE normal_date n SET time = (
    SELECT time FROM normal_date_part p WHERE p.id = n.id
)
WHERE EXISTS
    (SELECT 1
    FROM normal_date_part n2
    WHERE n2.id = n.id);
```

计划为：

```
                                                                           QUERY PLAN


----------------------------------------------------------------------------------------------------------------------------------------------------------------
 Update on normal_date n  (cost=224.40..2334150.22 rows=5129 width=16) (actual time=17.336..42944.734 rows=10000 loops=1)
   ->  Hash Semi Join  (cost=224.40..2334150.22 rows=5129 width=16) (actual time=16.997..42852.967 rows=10000 loops=1)
         Hash Cond: (n.id = n2.id)
         ->  Seq Scan on normal_date n  (cost=0.00..160.29 rows=5129 width=10) (actual time=0.113..7.271 rows=10000 loops=1)
         ->  Hash  (cost=160.29..160.29 rows=5129 width=10) (actual time=7.381..7.381 rows=10000 loops=1)
                Buckets: 32768  Batches: 1  Memory Usage: 430kB
               ->  Seq Scan on normal_date n2  (cost=0.00..160.29 rows=5129 width=10) (actual time=0.052..3.501 rows=10000 loops=1)
         SubPlan 1
           ->  Partition Iterator  (cost=0.00..455.00 rows=1 width=8) (actual time=21006.481..42756.884 rows=10000 loops=10000)
                 Iterations: 331
                 ->  Partitioned Seq Scan on normal_date_part p  (cost=0.00..455.00 rows=1 width=8) (actual time=27228.532..27261.944 rows=10000 loops=3310000)
                       Filter: (id = n.id)
                       Rows Removed by Filter: 99990000
                       Selected Partitions:  1..331
 Total runtime: 42947.153 ms
(15 rows)
```

## 优化说明<a name="section1156532282917"></a>

很明显，执行计划中存在SubPlan，并且SubPlan中的运算相当重，即此SubPlan是一个明确的性能瓶颈点。

根据SQL语意等价改写SQL消除SubPlan如下：

```
update normal_date n set time = p.time from normal_date_part p where p.id = n.id;
```
