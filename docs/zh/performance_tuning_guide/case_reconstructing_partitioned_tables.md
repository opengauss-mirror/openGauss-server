# **案例：改建分区表**

## 现象描述<a name="zh-cn_topic_0075873749_section3352584712931"></a>

如下简单SQL语句查询， 性能瓶颈点在normal\_date的Scan上。

```
                                                                  QUERY PLAN
-----------------------------------------------------------------------------------------------------------------------------------------------
 Seq Scan on normal_date  (cost=0.00..259.00 rows=30 width=12) (actual time=0.100..3.466 rows=30 loops=1)
   Filter: (("time" >= '2022-09-01 00:00:00'::timestamp without time zone) AND ("time" <= '2022-10-01 00:00:00'::timestamp without time zone))
   Rows Removed by Filter: 9970
 Total runtime: 3.587 ms
(4 rows)
```

## 优化分析<a name="zh-cn_topic_0075873749_section45836675121326"></a>

从业务层确认表数据\(在time字段上\)有明显的日期特征，符合分区表的特征。重新规划normal\_date表的表定义：字段time为分区键、月为间隔单位定义分区表normal\_date\_part。修改后结果如下，性能提升近10倍。

```
                                                                     QUERY PLAN
-----------------------------------------------------------------------------------------------------------------------------------------------------
 Partition Iterator  (cost=0.00..480.00 rows=30 width=12) (actual time=0.038..0.085 rows=30 loops=1)
   Iterations: 2
   ->  Partitioned Seq Scan on normal_date_part  (cost=0.00..480.00 rows=30 width=12) (actual time=0.049..0.063 rows=30 loops=2)
         Filter: (("time" >= '2022-09-01 00:00:00'::timestamp without time zone) AND ("time" <= '2022-10-01 00:00:00'::timestamp without time zone))
         Rows Removed by Filter: 31
         Selected Partitions:  3..4
 Total runtime: 0.360 ms
(7 rows)
```
