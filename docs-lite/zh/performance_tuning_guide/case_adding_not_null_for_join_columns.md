# **案例：增加JOIN列非空条件**<a name="ZH-CN_TOPIC_0000001382351029"></a>

```
SELECT * FROM join_a a JOIN join_b b ON a.b = b.b;
```

执行计划下：

```
                                                      QUERY PLAN
----------------------------------------------------------------------------------------------------------------------
 Hash Join  (cost=58.35..14677.69 rows=1074607 width=16) (actual time=23.374..23.384 rows=10 loops=1)
   Hash Cond: (a.b = b.b)
   ->  Seq Scan on join_a a  (cost=0.00..2248.10 rows=100010 width=8) (actual time=0.495..12.551 rows=100010 loops=1)
   ->  Hash  (cost=31.49..31.49 rows=2149 width=8) (actual time=0.614..0.614 rows=1000 loops=1)
          Buckets: 32768  Batches: 1  Memory Usage: 40kB
         ->  Seq Scan on join_b b  (cost=0.00..31.49 rows=2149 width=8) (actual time=0.009..0.183 rows=1000 loops=1)
 Total runtime: 23.716 ms
(7 rows)
```

## 优化分析<a name="zh-cn_topic_0073253827_zh-cn_topic_0040046525_section27179124161918"></a>

1. 分析执行计划可知，在顺序扫描阶段耗时较多。
2. 建议在语句中手动添加JOIN列的非空判断，修改后的语句如下所示。

    ```
    SELECT * FROM join_a a JOIN join_b b ON a.b = b.b where a.b IS NOT NULL;
    ```
    
    执行计划如下：

    ```
                                                     QUERY PLAN
    ---------------------------------------------------------------------------------------------------------------------
     Hash Join  (cost=58.22..14560.97 rows=1063762 width=16) (actual time=13.237..13.247 rows=10 loops=1)
       Hash Cond: (a.b = b.b)
       ->  Seq Scan on join_a a  (cost=0.00..2248.10 rows=99510 width=8) (actual time=12.417..12.422 rows=10 loops=1)
             Filter: (b IS NOT NULL)
             Rows Removed by Filter: 100000
       ->  Hash  (cost=31.49..31.49 rows=2138 width=8) (actual time=0.566..0.566 rows=1000 loops=1)
              Buckets: 32768  Batches: 1  Memory Usage: 40kB
             ->  Seq Scan on join_b b  (cost=0.00..31.49 rows=2138 width=8) (actual time=0.011..0.229 rows=1000 loops=1)
                   Filter: (b IS NOT NULL)
     Total runtime: 13.556 ms
    (10 rows)
    ```
