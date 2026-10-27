# 谓词下推的Hint

## 功能描述<a name="section290819468377"></a>

通过predpush Hint指定谓词下推的源表和目标子查询或视图，将关联谓词下推到目标内部，以优化查询执行计划；也可通过 no predpush 禁止当前查询块的 predpush 优化。

## 语法格式<a name="section530131664410"></a>

```sql
predpush(src)
predpush(src, dest)
predpush(src1 src2 ...)
predpush(src1 src2 ..., dest)
```

>[!NOTE]说明
>本参数仅在rewrite\_rule中的predpushnormal/predpushforce/predpush选项打开时生效。

- predpushnormal：使用Predicate Push查询重写规则（下推谓词条件到子查询中）。
- predpushforce：使用Predicate Push查询重写规则（下推谓词条件到子查询中，尽可能的利用索引加速）。
- predpush：在predpushnormal和predpushforce中根据代价选择最优计划。

>[!NOTE]说明
>针对rewrite\_rule中的predpushforce选项，新增支持自动选择下推源表的功能。当无显式指定 predpush(src)时，在所有满足基础表类型、等值连接和安全、可下推检查，至少一个分支满足前缀索引等条件，针对可下推的一个或多个表作为自动下推源表，生成SUBQUERY_PARAM，等价于原先`/*+set(rewrite_rule predpushforce) predpush(src1 src2)*/`场景。针对要下推的谓词，判断至少一个分支目标表对应的列是否有索引，并且符合前缀索引（前缀索引满足仅考虑要下推谓词的满足条件，不结合原分支已有谓词或其他将下推谓词联合考虑前缀索引满足情况），才选择下推源表。针对predpushforce选项新增支持自动选择下推源表，当源表确定时，复用已有的逻辑，下推源表时会将该源表所有满足的谓词均下推，包含不含索引列的谓词，同时支持下推多个源表。

```sql
no predpush
```
>[!NOTE]说明
>禁止当前查询块的 predpush 优化，no predpush 不要求 rewrite_rule 设置为某个特定值，也不需要额外设置 GUC。

## 示例<a name="section5736356154"></a>

清理环境：

```sql
drop view if exists v_addr_segm_uniaue_new;
drop table if exists tx_record_registry;
drop table if exists addr_segm_a;
drop table if exists addr_segm_b;
```

准备表、数据和索引：

```sql
CREATE TABLE tx_record_registry (
int_id BIGINT,
tx_id VARCHAR(50),
tx_flag INT
);

CREATE TABLE addr_segm_a (
int_id BIGINT,
stateflag INT,
name VARCHAR(100),
create_time TIMESTAMP
);

CREATE TABLE addr_segm_b (
int_id BIGINT,
stateflag INT,
name VARCHAR(100),
create_time TIMESTAMP
);

CREATE INDEX idx_tx_registry_tx_id0 ON tx_record_registry(tx_id, tx_flag);
CREATE INDEX idx_addr_segm_id_state1 ON addr_segm_a(int_id, stateflag);
CREATE INDEX idx_addr_segm_id_state2 ON addr_segm_b(int_id, stateflag);

CREATE OR REPLACE VIEW v_addr_segm_uniaue_new AS
SELECT int_id, stateflag, name, create_time, 'A' as source FROM addr_segm_a
WHERE stateflag = 0
UNION ALL
SELECT int_id, stateflag, name, create_time, 'B' as source FROM addr_segm_b
WHERE stateflag = 0;

INSERT INTO tx_record_registry VALUES
(1089160340628635648, '1089155245382361088', 1),
(1089160470784761856, '1089155245382361088', 2),
(1089160542693425152, '1089155245382361088', 5),
(1999999999999999999, 'other_tx_id', 1);

INSERT INTO addr_segm_a VALUES
(1089160340628635648, 0, '测试数据A1', NOW()),
(1089160470784761856, 0, '测试数据A2', NOW()),
(1089160542693425152, 0, '测试数据A3', NOW()),
(1111111111111111111, 0, '其他数据A1', NOW()),
(2222222222222222222, 1, '无效数据A', NOW());

INSERT INTO addr_segm_b VALUES
(1089160340628635648, 0, '测试数据B1', NOW()),
(1089160470784761856, 0, '测试数据B2', NOW()),
(1089160542693425152, 0, '测试数据B3', NOW()),
(3333333333333333333, 0, '其他数据B1', NOW()),
(4444444444444444444, 1, '无效数据B', NOW());

INSERT INTO addr_segm_a
SELECT generate_series,
0,
'Test_' || generate_series,
NOW() - (random() * interval '365 days')
FROM generate_series(1, 10000);

INSERT INTO addr_segm_b
SELECT generate_series + 10000,
0,
'Test_' || (generate_series + 10000),
NOW() - (random() * interval '365 days')
FROM generate_series(1, 10000);

select count(*) from tx_record_registry;
select count(*) from addr_segm_a;
select count(*) from addr_segm_b;

analyse tx_record_registry;
analyse addr_segm_a;
analyse addr_segm_b;
```

查看表的数据量：

```sql
openGauss=# select count(*) from tx_record_registry;
 count
-------
     4
(1 row)

openGauss=# select count(*) from addr_segm_a;
 count
-------
 10005
(1 row)

openGauss=# select count(*) from addr_segm_b;
 count
-------
 10005
(1 row)
```

针对不同参数设置，查看如下语句的执行计划：

```sql
EXPLAIN ANALYZE
SELECT *
FROM v_addr_segm_uniaue_new T
WHERE 1 = 1
AND (
stateflag = 0
AND int_id IN (
SELECT int_id
FROM tx_record_registry r
WHERE r.tx_id = '1089155245382361088'
AND tx_flag IN (1, 2, 5)
)
);
```

查看执行计划：

```sql
openGauss=# EXPLAIN ANALYZE
openGauss-# SELECT /*+set(rewrite_rule none)*/ *
openGauss-# FROM v_addr_segm_uniaue_new T
openGauss-# WHERE 1 = 1
openGauss-# AND (
openGauss(# stateflag = 0
openGauss(# AND int_id IN (
openGauss(# SELECT int_id
openGauss(# FROM tx_record_registry r
openGauss(# WHERE r.tx_id = '1089155245382361088'
openGauss(# AND tx_flag IN (1, 2, 5)
openGauss(# )
openGauss(# );
                                                           QUERY PLAN
--------------------------------------------------------------------------------------------------------------------------------
 Hash Join  (cost=1.14..654.37 rows=10004 width=62) (actual time=0.282..33.757 rows=6 loops=1)
   Hash Cond: (addr_segm_a.int_id = r.int_id)
   ->  Append  (cost=0.00..600.21 rows=20008 width=62) (actual time=0.063..25.738 rows=20008 loops=1)
         ->  Seq Scan on addr_segm_a  (cost=0.00..200.06 rows=10004 width=29) (actual time=0.061..11.010 rows=10004 loops=1)
               Filter: (stateflag = 0)
               Rows Removed by Filter: 1
         ->  Seq Scan on addr_segm_b  (cost=0.00..200.06 rows=10004 width=31) (actual time=0.030..10.686 rows=10004 loops=1)
               Filter: (stateflag = 0)
               Rows Removed by Filter: 1
   ->  Hash  (cost=1.10..1.10 rows=3 width=8) (actual time=0.103..0.103 rows=3 loops=1)
          Buckets: 32768  Batches: 1  Memory Usage: 257kB
         ->  HashAggregate  (cost=1.07..1.10 rows=3 width=8) (actual time=0.077..0.080 rows=3 loops=1)
               Group By Key: r.int_id
               ->  Seq Scan on tx_record_registry r  (cost=0.00..1.06 rows=3 width=8) (actual time=0.024..0.031 rows=3 loops=1)
                     Filter: (((tx_id)::text = '1089155245382361088'::text) AND (tx_flag = ANY ('{1,2,5}'::integer[])))
                     Rows Removed by Filter: 1
 Total runtime: 34.319 ms
(17 rows)

openGauss=# EXPLAIN ANALYZE
openGauss-# SELECT /*+no predpush*/ *
openGauss-# FROM v_addr_segm_uniaue_new T
openGauss-# WHERE 1 = 1
openGauss-# AND (
openGauss(# stateflag = 0
openGauss(# AND int_id IN (
openGauss(# SELECT int_id
openGauss(# FROM tx_record_registry r
openGauss(# WHERE r.tx_id = '1089155245382361088'
openGauss(# AND tx_flag IN (1, 2, 5)
openGauss(# )
openGauss(# );
                                                           QUERY PLAN
--------------------------------------------------------------------------------------------------------------------------------
 Hash Join  (cost=1.14..654.37 rows=10004 width=62) (actual time=0.236..33.180 rows=6 loops=1)
   Hash Cond: (addr_segm_a.int_id = r.int_id)
   ->  Append  (cost=0.00..600.21 rows=20008 width=62) (actual time=0.049..25.075 rows=20008 loops=1)
         ->  Seq Scan on addr_segm_a  (cost=0.00..200.06 rows=10004 width=29) (actual time=0.048..10.568 rows=10004 loops=1)
               Filter: (stateflag = 0)
               Rows Removed by Filter: 1
         ->  Seq Scan on addr_segm_b  (cost=0.00..200.06 rows=10004 width=31) (actual time=0.026..10.628 rows=10004 loops=1)
               Filter: (stateflag = 0)
               Rows Removed by Filter: 1
   ->  Hash  (cost=1.10..1.10 rows=3 width=8) (actual time=0.085..0.085 rows=3 loops=1)
          Buckets: 32768  Batches: 1  Memory Usage: 257kB
         ->  HashAggregate  (cost=1.07..1.10 rows=3 width=8) (actual time=0.063..0.066 rows=3 loops=1)
               Group By Key: r.int_id
               ->  Seq Scan on tx_record_registry r  (cost=0.00..1.06 rows=3 width=8) (actual time=0.022..0.027 rows=3 loops=1)
                     Filter: (((tx_id)::text = '1089155245382361088'::text) AND (tx_flag = ANY ('{1,2,5}'::integer[])))
                     Rows Removed by Filter: 1
 Total runtime: 33.668 ms
(17 rows)

openGauss=# EXPLAIN ANALYZE
openGauss-# SELECT /*+set(rewrite_rule predpushnormal) predpush(r)*/ *
openGauss-# FROM v_addr_segm_uniaue_new T
openGauss-# WHERE 1 = 1
openGauss-# AND (
openGauss(# stateflag = 0
openGauss(# AND int_id IN (
openGauss(# SELECT int_id
openGauss(# FROM tx_record_registry r
openGauss(# WHERE r.tx_id = '1089155245382361088'
openGauss(# AND tx_flag IN (1, 2, 5)
openGauss(# )
openGauss(# );
                                                           QUERY PLAN
--------------------------------------------------------------------------------------------------------------------------------
 Hash Join  (cost=1.14..654.37 rows=10004 width=62) (actual time=0.237..33.118 rows=6 loops=1)
   Hash Cond: (addr_segm_a.int_id = r.int_id)
   ->  Append  (cost=0.00..600.21 rows=20008 width=62) (actual time=0.042..25.063 rows=20008 loops=1)
         ->  Seq Scan on addr_segm_a  (cost=0.00..200.06 rows=10004 width=29) (actual time=0.041..10.395 rows=10004 loops=1)
               Filter: (stateflag = 0)
               Rows Removed by Filter: 1
         ->  Seq Scan on addr_segm_b  (cost=0.00..200.06 rows=10004 width=31) (actual time=0.033..10.694 rows=10004 loops=1)
               Filter: (stateflag = 0)
               Rows Removed by Filter: 1
   ->  Hash  (cost=1.10..1.10 rows=3 width=8) (actual time=0.085..0.085 rows=3 loops=1)
          Buckets: 32768  Batches: 1  Memory Usage: 257kB
         ->  HashAggregate  (cost=1.07..1.10 rows=3 width=8) (actual time=0.063..0.066 rows=3 loops=1)
               Group By Key: r.int_id
               ->  Seq Scan on tx_record_registry r  (cost=0.00..1.06 rows=3 width=8) (actual time=0.022..0.028 rows=3 loops=1)
                     Filter: (((tx_id)::text = '1089155245382361088'::text) AND (tx_flag = ANY ('{1,2,5}'::integer[])))
                     Rows Removed by Filter: 1
 Total runtime: 33.691 ms
(17 rows)

openGauss=# EXPLAIN ANALYZE
openGauss-# SELECT /*+set(rewrite_rule predpushnormal)*/ *
openGauss-# FROM v_addr_segm_uniaue_new T
openGauss-# WHERE 1 = 1
openGauss-# AND (
openGauss(# stateflag = 0
openGauss(# AND int_id IN (
openGauss(# SELECT int_id
openGauss(# FROM tx_record_registry r
openGauss(# WHERE r.tx_id = '1089155245382361088'
openGauss(# AND tx_flag IN (1, 2, 5)
openGauss(# )
openGauss(# );
                                                           QUERY PLAN
--------------------------------------------------------------------------------------------------------------------------------
 Hash Join  (cost=1.14..654.37 rows=10004 width=62) (actual time=0.272..32.992 rows=6 loops=1)
   Hash Cond: (addr_segm_a.int_id = r.int_id)
   ->  Append  (cost=0.00..600.21 rows=20008 width=62) (actual time=0.051..25.024 rows=20008 loops=1)
         ->  Seq Scan on addr_segm_a  (cost=0.00..200.06 rows=10004 width=29) (actual time=0.048..10.689 rows=10004 loops=1)
               Filter: (stateflag = 0)
               Rows Removed by Filter: 1
         ->  Seq Scan on addr_segm_b  (cost=0.00..200.06 rows=10004 width=31) (actual time=0.023..10.326 rows=10004 loops=1)
               Filter: (stateflag = 0)
               Rows Removed by Filter: 1
   ->  Hash  (cost=1.10..1.10 rows=3 width=8) (actual time=0.106..0.106 rows=3 loops=1)
          Buckets: 32768  Batches: 1  Memory Usage: 257kB
         ->  HashAggregate  (cost=1.07..1.10 rows=3 width=8) (actual time=0.083..0.084 rows=3 loops=1)
               Group By Key: r.int_id
               ->  Seq Scan on tx_record_registry r  (cost=0.00..1.06 rows=3 width=8) (actual time=0.025..0.031 rows=3 loops=1)
                     Filter: (((tx_id)::text = '1089155245382361088'::text) AND (tx_flag = ANY ('{1,2,5}'::integer[])))
                     Rows Removed by Filter: 1
 Total runtime: 33.474 ms
(17 rows)

-- 设置predpushforce且指定predpush，下推谓词，走Nested Loop，提升查询效率
openGauss=# EXPLAIN ANALYZE
openGauss-# SELECT /*+set(rewrite_rule predpushforce) predpush(r)*/ *
openGauss-# FROM v_addr_segm_uniaue_new T
openGauss-# WHERE 1 = 1
openGauss-# AND (
openGauss(# stateflag = 0
openGauss(# AND int_id IN (
openGauss(# SELECT int_id
openGauss(# FROM tx_record_registry r
openGauss(# WHERE r.tx_id = '1089155245382361088'
openGauss(# AND tx_flag IN (1, 2, 5)
openGauss(# )
openGauss(# );
                                                                    QUERY PLAN

----------------------------------------------------------------------------------------------------------------------------------
----------------
 Nested Loop  (cost=1.07..50.96 rows=2 width=62) (actual time=0.119..0.213 rows=6 loops=1)
   ->  HashAggregate  (cost=1.07..1.10 rows=3 width=8) (actual time=0.071..0.073 rows=3 loops=1)
         Group By Key: r.int_id
         ->  Seq Scan on tx_record_registry r  (cost=0.00..1.06 rows=3 width=8) (actual time=0.032..0.038 rows=3 loops=1)
               Filter: (((tx_id)::text = '1089155245382361088'::text) AND (tx_flag = ANY ('{1,2,5}'::integer[])))
               Rows Removed by Filter: 1
   ->  Append  (cost=0.00..16.58 rows=4 width=62) (actual time=0.060..0.103 rows=6 loops=3)
         ->  Index Scan using idx_addr_segm_id_state1 on addr_segm_a  (cost=0.00..8.27 rows=2 width=29) (actual time=0.053..0.057
rows=3 loops=3)
               Index Cond: ((r.int_id = int_id) AND (stateflag = 0))
         ->  Index Scan using idx_addr_segm_id_state2 on addr_segm_b  (cost=0.00..8.27 rows=2 width=31) (actual time=0.032..0.035
rows=3 loops=3)
               Index Cond: ((r.int_id = int_id) AND (stateflag = 0))
 Total runtime: 0.581 ms
(12 rows)

-- 设置predpushforce且未指定predpush，至少一个分支满足前缀索引，自动选择predpush(r)，下推谓词，走Nested Loop，提升查询效率
openGauss=# EXPLAIN ANALYZE
openGauss-# SELECT /*+set(rewrite_rule predpushforce)*/ *
openGauss-# FROM v_addr_segm_uniaue_new T
openGauss-# WHERE 1 = 1
openGauss-# AND (
openGauss(# stateflag = 0
openGauss(# AND int_id IN (
openGauss(# SELECT int_id
openGauss(# FROM tx_record_registry r
openGauss(# WHERE r.tx_id = '1089155245382361088'
openGauss(# AND tx_flag IN (1, 2, 5)
openGauss(# )
openGauss(# );
                                                                    QUERY PLAN

----------------------------------------------------------------------------------------------------------------------------------
----------------
 Nested Loop  (cost=1.07..50.96 rows=2 width=62) (actual time=0.096..0.185 rows=6 loops=1)
   ->  HashAggregate  (cost=1.07..1.10 rows=3 width=8) (actual time=0.063..0.065 rows=3 loops=1)
         Group By Key: r.int_id
         ->  Seq Scan on tx_record_registry r  (cost=0.00..1.06 rows=3 width=8) (actual time=0.030..0.037 rows=3 loops=1)
               Filter: (((tx_id)::text = '1089155245382361088'::text) AND (tx_flag = ANY ('{1,2,5}'::integer[])))
               Rows Removed by Filter: 1
   ->  Append  (cost=0.00..16.58 rows=4 width=62) (actual time=0.049..0.088 rows=6 loops=3)
         ->  Index Scan using idx_addr_segm_id_state1 on addr_segm_a  (cost=0.00..8.27 rows=2 width=29) (actual time=0.044..0.048
rows=3 loops=3)
               Index Cond: ((r.int_id = int_id) AND (stateflag = 0))
         ->  Index Scan using idx_addr_segm_id_state2 on addr_segm_b  (cost=0.00..8.27 rows=2 width=31) (actual time=0.027..0.030
rows=3 loops=3)
               Index Cond: ((r.int_id = int_id) AND (stateflag = 0))
 Total runtime: 0.530 ms
(12 rows)

openGauss=# EXPLAIN ANALYZE
openGauss-# SELECT /*+set(rewrite_rule 'predpush') predpush(r)*/ *
openGauss-# FROM v_addr_segm_uniaue_new T
openGauss-# WHERE 1 = 1
openGauss-# AND (
openGauss(# stateflag = 0
openGauss(# AND int_id IN (
openGauss(# SELECT int_id
openGauss(# FROM tx_record_registry r
openGauss(# WHERE r.tx_id = '1089155245382361088'
openGauss(# AND tx_flag IN (1, 2, 5)
openGauss(# )
openGauss(# );
                                                           QUERY PLAN
--------------------------------------------------------------------------------------------------------------------------------
 Hash Join  (cost=1.14..654.37 rows=10004 width=62) (actual time=0.220..33.307 rows=6 loops=1)
   Hash Cond: (addr_segm_a.int_id = r.int_id)
   ->  Append  (cost=0.00..600.21 rows=20008 width=62) (actual time=0.036..25.220 rows=20008 loops=1)
         ->  Seq Scan on addr_segm_a  (cost=0.00..200.06 rows=10004 width=29) (actual time=0.035..10.606 rows=10004 loops=1)
               Filter: (stateflag = 0)
               Rows Removed by Filter: 1
         ->  Seq Scan on addr_segm_b  (cost=0.00..200.06 rows=10004 width=31) (actual time=0.035..10.602 rows=10004 loops=1)
               Filter: (stateflag = 0)
               Rows Removed by Filter: 1
   ->  Hash  (cost=1.10..1.10 rows=3 width=8) (actual time=0.075..0.075 rows=3 loops=1)
          Buckets: 32768  Batches: 1  Memory Usage: 257kB
         ->  HashAggregate  (cost=1.07..1.10 rows=3 width=8) (actual time=0.053..0.056 rows=3 loops=1)
               Group By Key: r.int_id
               ->  Seq Scan on tx_record_registry r  (cost=0.00..1.06 rows=3 width=8) (actual time=0.020..0.025 rows=3 loops=1)
                     Filter: (((tx_id)::text = '1089155245382361088'::text) AND (tx_flag = ANY ('{1,2,5}'::integer[])))
                     Rows Removed by Filter: 1
 Total runtime: 33.781 ms
(17 rows)

openGauss=# EXPLAIN ANALYZE
openGauss-# SELECT /*+set(rewrite_rule 'predpush')*/ *
openGauss-# FROM v_addr_segm_uniaue_new T
openGauss-# WHERE 1 = 1
openGauss-# AND (
openGauss(# stateflag = 0
openGauss(# AND int_id IN (
openGauss(# SELECT int_id
openGauss(# FROM tx_record_registry r
openGauss(# WHERE r.tx_id = '1089155245382361088'
openGauss(# AND tx_flag IN (1, 2, 5)
openGauss(# )
openGauss(# );
                                                           QUERY PLAN
--------------------------------------------------------------------------------------------------------------------------------
 Hash Join  (cost=1.14..654.37 rows=10004 width=62) (actual time=0.304..33.362 rows=6 loops=1)
   Hash Cond: (addr_segm_a.int_id = r.int_id)
   ->  Append  (cost=0.00..600.21 rows=20008 width=62) (actual time=0.064..25.291 rows=20008 loops=1)
         ->  Seq Scan on addr_segm_a  (cost=0.00..200.06 rows=10004 width=29) (actual time=0.061..10.839 rows=10004 loops=1)
               Filter: (stateflag = 0)
               Rows Removed by Filter: 1
         ->  Seq Scan on addr_segm_b  (cost=0.00..200.06 rows=10004 width=31) (actual time=0.027..10.626 rows=10004 loops=1)
               Filter: (stateflag = 0)
               Rows Removed by Filter: 1
   ->  Hash  (cost=1.10..1.10 rows=3 width=8) (actual time=0.107..0.107 rows=3 loops=1)
          Buckets: 32768  Batches: 1  Memory Usage: 257kB
         ->  HashAggregate  (cost=1.07..1.10 rows=3 width=8) (actual time=0.080..0.083 rows=3 loops=1)
               Group By Key: r.int_id
               ->  Seq Scan on tx_record_registry r  (cost=0.00..1.06 rows=3 width=8) (actual time=0.023..0.030 rows=3 loops=1)
                     Filter: (((tx_id)::text = '1089155245382361088'::text) AND (tx_flag = ANY ('{1,2,5}'::integer[])))
                     Rows Removed by Filter: 1
 Total runtime: 33.936 ms
(17 rows)

```

删除索引，对比predpushforce设置时有无指定predpush的差异：

```sql
openGauss=# drop index idx_addr_segm_id_state1;
DROP INDEX
openGauss=# drop index idx_addr_segm_id_state2;
DROP INDEX
-- 设置predpushforce且指定predpush，下推谓词，走Nested Loop
openGauss=# EXPLAIN ANALYZE
openGauss-# SELECT /*+set(rewrite_rule predpushforce) predpush(r)*/ *
openGauss-# FROM v_addr_segm_uniaue_new T
openGauss-# WHERE 1 = 1
openGauss-# AND (
openGauss(# stateflag = 0
openGauss(# AND int_id IN (
openGauss(# SELECT int_id
openGauss(# FROM tx_record_registry r
openGauss(# WHERE r.tx_id = '1089155245382361088'
openGauss(# AND tx_flag IN (1, 2, 5)
openGauss(# )
openGauss(# );
                                                        QUERY PLAN
--------------------------------------------------------------------------------------------------------------------------
 Nested Loop  (cost=1.07..1351.79 rows=2 width=62) (actual time=0.173..47.793 rows=6 loops=1)
   ->  HashAggregate  (cost=1.07..1.10 rows=3 width=8) (actual time=0.116..0.122 rows=3 loops=1)
         Group By Key: r.int_id
         ->  Seq Scan on tx_record_registry r  (cost=0.00..1.06 rows=3 width=8) (actual time=0.054..0.062 rows=3 loops=1)
               Filter: (((tx_id)::text = '1089155245382361088'::text) AND (tx_flag = ANY ('{1,2,5}'::integer[])))
               Rows Removed by Filter: 1
   ->  Append  (cost=0.00..450.19 rows=4 width=62) (actual time=0.082..47.618 rows=6 loops=3)
         ->  Seq Scan on addr_segm_a  (cost=0.00..225.07 rows=2 width=29) (actual time=0.061..23.813 rows=3 loops=3)
               Filter: ((stateflag = 0) AND (r.int_id = int_id))
               Rows Removed by Filter: 30012
         ->  Seq Scan on addr_segm_b  (cost=0.00..225.07 rows=2 width=31) (actual time=0.061..23.766 rows=3 loops=3)
               Filter: ((stateflag = 0) AND (r.int_id = int_id))
               Rows Removed by Filter: 30012
 Total runtime: 48.257 ms
(14 rows)

-- 设置predpushforce且未指定predpush，任意分支均不满足前缀索引，不自动选择predpush，不下推谓词，走Hash Join
openGauss=# EXPLAIN ANALYZE
openGauss-# SELECT /*+set(rewrite_rule predpushforce)*/ *
openGauss-# FROM v_addr_segm_uniaue_new T
openGauss-# WHERE 1 = 1
openGauss-# AND (
openGauss(# stateflag = 0
openGauss(# AND int_id IN (
openGauss(# SELECT int_id
openGauss(# FROM tx_record_registry r
openGauss(# WHERE r.tx_id = '1089155245382361088'
openGauss(# AND tx_flag IN (1, 2, 5)
openGauss(# )
openGauss(# );
                                                           QUERY PLAN
--------------------------------------------------------------------------------------------------------------------------------
 Hash Join  (cost=1.14..654.37 rows=10004 width=62) (actual time=0.342..33.750 rows=6 loops=1)
   Hash Cond: (addr_segm_a.int_id = r.int_id)
   ->  Append  (cost=0.00..600.21 rows=20008 width=62) (actual time=0.070..25.639 rows=20008 loops=1)
         ->  Seq Scan on addr_segm_a  (cost=0.00..200.06 rows=10004 width=29) (actual time=0.068..10.949 rows=10004 loops=1)
               Filter: (stateflag = 0)
               Rows Removed by Filter: 1
         ->  Seq Scan on addr_segm_b  (cost=0.00..200.06 rows=10004 width=31) (actual time=0.056..10.675 rows=10004 loops=1)
               Filter: (stateflag = 0)
               Rows Removed by Filter: 1
   ->  Hash  (cost=1.10..1.10 rows=3 width=8) (actual time=0.118..0.118 rows=3 loops=1)
          Buckets: 32768  Batches: 1  Memory Usage: 257kB
         ->  HashAggregate  (cost=1.07..1.10 rows=3 width=8) (actual time=0.086..0.089 rows=3 loops=1)
               Group By Key: r.int_id
               ->  Seq Scan on tx_record_registry r  (cost=0.00..1.06 rows=3 width=8) (actual time=0.027..0.033 rows=3 loops=1)
                     Filter: (((tx_id)::text = '1089155245382361088'::text) AND (tx_flag = ANY ('{1,2,5}'::integer[])))
                     Rows Removed by Filter: 1
 Total runtime: 34.411 ms
(17 rows)

```
