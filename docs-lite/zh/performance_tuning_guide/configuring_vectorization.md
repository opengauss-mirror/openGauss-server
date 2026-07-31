# 配置向量化<a name="ZH-CN_TOPIC_0000001241171615"></a>

openGauss数据库支持行执行引擎和向量化执行引擎，分别对应行存表和列存表。

- 一次一个batch，读取更多数据，节省IO。
- batch中记录较多，CPU cache命中率提升。
- Pipeline模式执行，函数调用次数少。
- 一次处理一批数据，效率高。

openGauss数据库所以对于分析类的复杂查询能够获得更好的查询性能。但列存表在数据插入和数据更新上表现不佳，对于存在数据频繁插入和更新的业务无法使用列存表。

为了提升行存表在分析类的复杂查询上的查询性能，openGauss数据库提供行存表使用向量化执行引擎的能力。通过设置GUC参数[try\_vector\_engine\_strategy](https://docs.opengauss.org/zh/docs/latest-lite/database_reference/optimizer_method_configuration.html#section145867222412)，可以将包含行存表的查询语句转换为向量化执行计划执行。

行存表转换为向量化执行引擎执行不是对所有的查询场景都适用。参考向量化引擎的优势，如果查询语句中包含表达式计算、多表join、聚集等操作时，通过转换为向量化执行能够获得性能提升。从原理上分析，行存表转换为向量化执行，会产生转换的开销，导致性能下降。而上述操作的表达式计算、join操作、聚集操作转换为向量化执行之后，能够获得获得性能提升。所以查询转换为向量化执行后，性能是否提升，取决于查询转换为向量化之后获得的性能提升能否高于转换产生的性能开销。

以TPCH Q1为例，使用行执行引擎时，扫描算子的执行时间为405210ms，聚集操作的执行时间为2618964ms；而转换为向量化执行引擎后，扫描算子（SeqScan + VectorAdapter）的执行时间为470840ms，聚集操作的执行时间为212384ms，所以查询能够获得性能提升。

TPCH Q1 行执行引擎执行计划：

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

TPCH Q1 向量化执行引擎执行计划：

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
