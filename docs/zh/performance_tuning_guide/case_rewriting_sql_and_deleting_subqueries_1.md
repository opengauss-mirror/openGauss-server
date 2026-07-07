# 案例：**改写SQL消除子查询（案例1）**

## 现象描述<a name="zh-cn_topic_0075873755_section58732893101941"></a>

```
select 
    1,
    (select count(*) from normal_date n where n.id = a.id) as GZCS 
from normal_date a;
```

此SQL性能较差，查看发现执行计划中存在SubPlan，具体如下：

```
                                                              QUERY PLAN
---------------------------------------------------------------------------------------------------------------------------------------
 Seq Scan on normal_date a  (cost=0.00..888118.42 rows=5129 width=4) (actual time=2.394..22194.907 rows=10000 loops=1)
   SubPlan 1
     ->  Aggregate  (cost=173.12..173.12 rows=1 width=8) (actual time=22179.496..22179.942 rows=10000 loops=10000)
           ->  Seq Scan on normal_date n  (cost=0.00..173.11 rows=1 width=0) (actual time=11279.349..22159.608 rows=10000 loops=10000)
                 Filter: (id = a.id)
                 Rows Removed by Filter: 99990000
 Total runtime: 22196.415 ms
(7 rows)
```

## 优化说明<a name="zh-cn_topic_0075873755_section63577672101958"></a>

此优化的核心就是消除子查询。分析业务场景发现_a**.**id_不为null，那么从SQL语义出发，可以等价改写SQL为：

```
select 
count(*) 
from normal_date n, normal_date a
where n.id = a.id
group by  a.id;
计划如下：
                                                            QUERY PLAN
----------------------------------------------------------------------------------------------------------------------------------
 HashAggregate  (cost=480.86..532.15 rows=5129 width=12) (actual time=21.539..24.356 rows=10000 loops=1)
   Group By Key: a.id
   ->  Hash Join  (cost=224.40..455.22 rows=5129 width=4) (actual time=6.402..13.484 rows=10000 loops=1)
         Hash Cond: (n.id = a.id)
         ->  Seq Scan on normal_date n  (cost=0.00..160.29 rows=5129 width=4) (actual time=0.087..1.459 rows=10000 loops=1)
         ->  Hash  (cost=160.29..160.29 rows=5129 width=4) (actual time=6.065..6.065 rows=10000 loops=1)
                Buckets: 32768  Batches: 1  Memory Usage: 352kB
               ->  Seq Scan on normal_date a  (cost=0.00..160.29 rows=5129 width=4) (actual time=0.046..2.738 rows=10000 loops=1)
 Total runtime: 26.844 ms
(9 rows)
```

>[!NOTE]说明
>为了保证改写的等效性，在_normal\_date.id_加了_not null_约束。
