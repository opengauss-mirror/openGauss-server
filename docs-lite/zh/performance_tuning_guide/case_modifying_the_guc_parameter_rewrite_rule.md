# 案例：调整查询重写GUC参数rewrite\_rule<a name="ZH-CN_TOPIC_0000001086786554"></a>

rewrite\_rule包含了多个查询重写规则：magicset、partialpush、uniquecheck、disablerep、intargetlist、predpush、union_all_faststart。下面简要说明一下其中重要的几个规则的使用场景：

## 目标列子查询提升参数intargetlist<a name="section66521181379"></a>

通过将目标列中子查询提升，转为JOIN，往往可以极大提升查询性能。举例如下查询：

```
openGauss=#  set rewrite_rule='none';
SET
openGauss=# create table t1(c1 int,c2 int);
CREATE TABLE
openGauss=# create table t2(c1 int,c2 int);
CREATE TABLE
openGauss=#  explain (verbose on, costs off) select c1,(select avg(c2) from t2 where t2.c2=t1.c2) from t1 where t1.c1<100 order by t1.c2;
                  QUERY PLAN
-----------------------------------------------
 Sort
   Output: t1.c1, ((SubPlan 1)), t1.c2
   Sort Key: t1.c2
   ->  Seq Scan on public.t1
         Output: t1.c1, (SubPlan 1), t1.c2
         Filter: (t1.c1 < 100)
         SubPlan 1
           ->  Aggregate
                 Output: avg(t2.c2)
                 ->  Seq Scan on public.t2
                       Output: t2.c1, t2.c2
                       Filter: (t2.c2 = t1.c2)
(12 rows)
```

由于目标列中的相关子查询\(select avg\(c2\) from t2 where t2.c2=t1.c2\)无法提升的缘故，导致每扫描t1的一行数据，就会触发子查询的一次执行，效率低下。如果打开intargetlist参数会把子查询提升转为JOIN，来提升查询的性能：

```
openGauss=#  set rewrite_rule='intargetlist';
SET
openGauss=# explain (verbose on, costs off) select c1,(select avg(c2) from t2 where t2.c2=t1.c2) from t1 where t1.c1<100 order by t1.c2;
                  QUERY PLAN
-----------------------------------------------
 Sort
   Output: t1.c1, (avg(t2.c2)), t1.c2
   Sort Key: t1.c2
   ->  Hash Left Join
         Output: t1.c1, (avg(t2.c2)), t1.c2
         Hash Cond: (t1.c2 = t2.c2)
         ->  Seq Scan on public.t1
               Output: t1.c1, t1.c2
               Filter: (t1.c1 < 100)
         ->  Hash
               Output: (avg(t2.c2)), t2.c2
               ->  HashAggregate
                     Output: avg(t2.c2), t2.c2
                     Group By Key: t2.c2
                     ->  Seq Scan on public.t2
                           Output: t2.c2
(16 rows)
```

## 提升无agg的子查询uniquecheck<a name="section20180151614815"></a>

子链接提升需要保证对于每个条件只有一行输出，对于有agg的子查询可以自动提升，对于无agg的子查询如：

select t1.c1 from t1 where t1.c1 = \(select t2.c1 from t2 where t1.c1=t2.c2\) ;

重写为：

select t1.c1 from t1 join \(select t2.c1 from t2 where t2.c1 is not null group by t2.c1\(unique check\)\) tt\(c1\) on tt.c1=t1.c1;

为了保证语义等价，子查询tt必须保证对于每个group by t2.c1只能有一行输出。打开uniquecheck查询重写参数保证可以提升并且等价，如果在运行时输出了多于一行的数据，就会报错。

```
openGauss=# set rewrite_rule='uniquecheck';
SET
openGauss=#  explain verbose select t1.c1 from t1 where t1.c1 = (select t2.c1 from t2 where t1.c1=t2.c1);
                                     QUERY PLAN
-------------------------------------------------------------------------------------
 Hash Join  (cost=43.36..104.40 rows=2149 distinct=[200, 200] width=4)
   Output: t1.c1
   Hash Cond: (t1.c1 = subquery."?column?")
   ->  Seq Scan on public.t1  (cost=0.00..31.49 rows=2149 width=4)
         Output: t1.c1, t1.c2
   ->  Hash  (cost=40.86..40.86 rows=200 width=8)
         Output: subquery."?column?", subquery.c1
         ->  Subquery Scan on subquery  (cost=36.86..40.86 rows=200 width=8)
               Output: subquery."?column?", subquery.c1
               ->  HashAggregate  (cost=36.86..38.86 rows=200 width=4)
                     Output: t2.c1, t2.c1
                     Group By Key: t2.c1
                     Filter: (t2.c1 IS NOT NULL)
                     Unique Check Required
                     ->  Seq Scan on public.t2  (cost=0.00..31.49 rows=2149 width=4)
                           Output: t2.c1
(16 rows)
```

注意：因为分组group by t2.c1 unique check发生在过滤条件tt.c1=t1.c1之前，可能导致原来不报错的查询重写之后报错。举例：

有t1,t2表，其中的数据为：

```
openGauss=#  select * from t1 order by c2;
 c1 | c2
----+----
  1 |  1
  2 |  2
  3 |  3
(3 rows)
openGauss=#  select * from t2 order by c2;
 c1 | c2
----+----
  1 |  1
  2 |  2
  3 |  3
  4 |  4
  4 |  4
  5 |  5
(6 rows)
```

分别关闭和打开uniquecheck参数对比，打开之后报错。

```
openGauss=#  select t1.c1 from t1 where t1.c1 = (select t2.c1 from t2 where t1.c1=t2.c2) ;
 c1
----
  1
  2
  3
(3 rows)
openGauss=#  set rewrite_rule='uniquecheck';
SET
openGauss=#  select t1.c1 from t1 where t1.c1 = (select t2.c1 from t2 where t1.c1=t2.c2) ;
ERROR:  more than one row returned by a subquery used as an expression
```

## 去除多余distinct和group by子句remove_redundant_distinct_group_by<a name="section20180151614545"></a>

子查询提升需要保证没有distinct、group by子句，而对于如下ANY_sublink的语句：

explain select t1.c1 from t1 where t1.c2 = 5 and t1.c1 in (select t2.c1 from t2 group by t2.c1);

或者

explain select t1.c1 from t1 where t1.c2 = 5 and t1.c1 in (select distinct t2.c1 from t2);

可以发现distinct或group by对子查询的结果并无影响，仅做了去重，在in场景下并无意义，可以直接去除，重写为：

explain select t1.c1 from t1 where t1.c2 = 5 and t1.c1 in (select t2.c1 from t2);

具体的执行计划变更如下：

```
openGauss=# explain select t1.c1 from t1 where t1.c2 = 5 and t1.c1 in (select t2.c1 from t2 group by t2.c1);
                                    QUERY PLAN
----------------------------------------------------------------------------------
 Hash Right Semi Join  (cost=52.01..56.79 rows=6 width=4)
   Hash Cond: (t2.c1 = t1.c1)
   ->  HashAggregate  (cost=36.86..38.86 rows=200 width=4)
         Group By Key: t2.c1
         ->  Seq Scan on t2  (cost=0.00..31.49 rows=2149 width=4)
   ->  Hash  (cost=15.01..15.01 rows=11 width=4)
         ->  Bitmap Heap Scan on t1  (cost=4.34..15.01 rows=11 width=4)
               Recheck Cond: (c2 = 5)
               ->  Bitmap Index Scan on t1_idx  (cost=0.00..4.33 rows=11 width=0)
                     Index Cond: (c2 = 5)
(10 rows)

openGauss=# set rewrite_rule = 'remove_redundant_distinct_group_by';
SET
openGauss=# explain select t1.c1 from t1 where t1.c2 = 5 and t1.c1 in (select t2.c1 from t2 group by t2.c1);
                                 QUERY PLAN
-----------------------------------------------------------------------------
 Nested Loop Semi Join  (cost=4.34..23.55 rows=6 width=4)
   ->  Bitmap Heap Scan on t1  (cost=4.34..15.01 rows=11 width=4)
         Recheck Cond: (c2 = 5)
         ->  Bitmap Index Scan on t1_idx  (cost=0.00..4.33 rows=11 width=0)
               Index Cond: (c2 = 5)
   ->  Index Only Scan using t2_idx on t2  (cost=0.00..4.47 rows=11 width=4)
         Index Cond: (c1 = t1.c1)
(7 rows)
```

原先扫描t2表时，由于存在group by子句，导致t2.c1 = t1.c1过滤条件无法下推到扫描中，需要做全表扫描。如果打开remove_redundant_distinct_group_by参数，去掉了group by子句，选择了参数化路径，提升了查询性能。

需要注意的是，即使打开了该参数，部分场景也不会做该优化，原因是可能会影响结果：

* 对于distinct场景，使用了distinct on子句或limit子句。
* 对于group by场景，使用了聚集函数、窗口函数、having子句、groupingSets或limit子句。

## UNION ALL 分支快速启动重排 union_all_faststart

对于无顶层 ORDER BY 的 UNION ALL 查询，如果只需要少量结果，前置分支较高的取数成本可能延长查询等待时间。开启 `rewrite_rule` 中的 `union_all_faststart` 后，优化器会对满足条件的查询，根据 LIMIT/OFFSET 需求和分支成本估算值调整分支顺序。

本例将 `k1` 与 `k2` 的连接放在前面，将 `k1` 与仅含 5 行的 `k3` 的连接放在后面，通过 `LIMIT 1` 获取一行结果。目标是让预计取数成本较低的 `k1 JOIN k3` 分支优先执行。

### 准备数据

```sql
DROP TABLE IF EXISTS k1;
DROP TABLE IF EXISTS k2;
DROP TABLE IF EXISTS k3;

CREATE TABLE k1(id int, id1 int);
CREATE TABLE k2(id int, id1 int);
CREATE TABLE k3(id int, id1 int);
INSERT INTO k1 SELECT id, id FROM generate_series(1, 1000000) id;
INSERT INTO k2 SELECT id, id FROM generate_series(1000000, 2000000) id;
INSERT INTO k3 VALUES(1, 1);
INSERT INTO k3 VALUES(2, 2);
INSERT INTO k3 VALUES(3, 3);
INSERT INTO k3 VALUES(4, 4);
INSERT INTO k3 VALUES(5, 5);

CREATE INDEX i_id_k1 ON k1(id);
ANALYZE k1;
ANALYZE k2;
ANALYZE k3;
```

### 关闭特性，查看基线计划和执行耗时

```sql
SET rewrite_rule = 'none';

EXPLAIN
SELECT * FROM (
    (SELECT * FROM k1 JOIN k2 ON k1.id1 = k2.id1)
    UNION ALL
    (SELECT * FROM k1 JOIN k3 ON k1.id = k3.id)
) aa LIMIT 1;

EXPLAIN ANALYZE
SELECT * FROM (
    (SELECT * FROM k1 JOIN k2 ON k1.id1 = k2.id1)
    UNION ALL
    (SELECT * FROM k1 JOIN k3 ON k1.id = k3.id)
) aa LIMIT 1;
```

检查 Append 的子计划顺序，记录 `k1 JOIN k2` 分支是否在前，以及顶层 Limit 的实际首行时间和查询总耗时。本例前一分支能够返回匹配结果，因此按原顺序取得一行后，后一分支通常不再执行。

### 开启特性，对比计划和执行耗时

```sql
SET rewrite_rule = 'union_all_faststart';

EXPLAIN
SELECT * FROM (
    (SELECT * FROM k1 JOIN k2 ON k1.id1 = k2.id1)
    UNION ALL
    (SELECT * FROM k1 JOIN k3 ON k1.id = k3.id)
) aa LIMIT 1;

EXPLAIN ANALYZE
SELECT * FROM (
    (SELECT * FROM k1 JOIN k2 ON k1.id1 = k2.id1)
    UNION ALL
    (SELECT * FROM k1 JOIN k3 ON k1.id = k3.id)
) aa LIMIT 1;

RESET rewrite_rule;
```

当优化器估算 `k1 JOIN k3` 分支满足本次取数需求的成本更低时，该分支会被排到 Append 的前面。它返回一行后即可满足 LIMIT，后面的 `k1 JOIN k2` 分支通常显示为未执行（`never executed`）。通过 Append 分支顺序确认是否发生重排，再比较两种配置下顶层 Limit 的实际首行时间和查询总耗时。

具体连接方式、成本、重排顺序及耗时以当前环境的实际计划为准。评估收益时，应对两种配置分别预热并重复测试，避免仅凭一次执行的缓存差异判断效果。该优化依赖成本估算，不保证所有查询都加速。

### 计划对比示例

以下为上述 SQL 在测试环境中实际执行 `EXPLAIN` 得到的计划。

关闭特性（`SET rewrite_rule = 'none';`）：

```text
Limit  (cost=26925.00..26925.04 rows=1 width=16)
  ->  Result  (cost=26925.00..65101.38 rows=1000005 width=16)
        ->  Append  (cost=26925.00..65101.38 rows=1000005 width=16)
              ->  Hash Join  (cost=26925.00..55100.01 rows=1000000 width=16)
                    Hash Cond: (k2.id1 = public.k1.id1)
                    ->  Seq Scan on k2  (cost=0.00..14425.01 rows=1000001 width=8)
                    ->  Hash  (cost=14425.00..14425.00 rows=1000000 width=8)
                          ->  Seq Scan on k1  (cost=0.00..14425.00 rows=1000000 width=8)
              ->  Merge Join  (cost=1.13..1.31 rows=5 width=16)
                    Merge Cond: (public.k1.id = k3.id)
                    ->  Index Scan using i_id_k1 on k1  (cost=0.00..30436.25 rows=1000000 width=8)
                    ->  Sort  (cost=1.11..1.12 rows=5 width=8)
                          Sort Key: k3.id
                          ->  Seq Scan on k3  (cost=0.00..1.05 rows=5 width=8)
```

开启特性（`SET rewrite_rule = 'union_all_faststart';`）：

```text
Limit  (cost=1.13..1.20 rows=1 width=16)
  ->  Result  (cost=1.13..65101.38 rows=1000005 width=16)
        ->  Append  (cost=1.13..65101.38 rows=1000005 width=16)
              ->  Merge Join  (cost=1.13..1.31 rows=5 width=16)
                    Merge Cond: (public.k1.id = k3.id)
                    ->  Index Scan using i_id_k1 on k1  (cost=0.00..30436.25 rows=1000000 width=8)
                    ->  Sort  (cost=1.11..1.12 rows=5 width=8)
                          Sort Key: k3.id
                          ->  Seq Scan on k3  (cost=0.00..1.05 rows=5 width=8)
              ->  Hash Join  (cost=26925.00..55100.01 rows=1000000 width=16)
                    Hash Cond: (k2.id1 = public.k1.id1)
                    ->  Seq Scan on k2  (cost=0.00..14425.01 rows=1000001 width=8)
                    ->  Hash  (cost=14425.00..14425.00 rows=1000000 width=8)
                          ->  Seq Scan on k1  (cost=0.00..14425.00 rows=1000000 width=8)
```

| 对比项 | 关闭特性 | 开启特性 |
| --- | --- | --- |
| Append 首个分支 | `k1 JOIN k2`，Hash Join | `k1 JOIN k3`，Merge Join |
| Append 启动成本 | 26925.00 | 1.13 |
| Append 总成本 | 65101.38 | 65101.38 |
| 顶层 Limit 成本 | 26925.00..26925.04 | 1.13..1.20 |

该计划表明，开启特性后，`k1 JOIN k3` 分支被移到 Append 首位；两个分支内部的连接方式和成本保持不变。Append 的总成本仍为 65101.38，因为重排没有减少执行全部分支的估算工作量，而是调整了获取少量结果时优先访问的分支。

上述输出确认了分支重排和估算成本的变化。`cost` 是优化器代价估算值，不是毫秒。实际首行时间、查询总耗时及各分支是否执行，需通过 `EXPLAIN ANALYZE` 进一步确认。

### 使用说明

- LIMIT 须直接约束 UNION ALL 输出；顶层 PERCENT 或上层集合操作要求全量取数时，跳过重排。
- 特殊 LIMIT 仅检查分支根节点（最多透过一层 `SubqueryScan`）；命中 PERCENT/WITH TIES 标记时，当前 Append 不重排。
- 本例没有顶层 ORDER BY，开启特性后返回的具体记录可能来自不同分支。对比时应验证返回行数和数据来源，不要求截断后的具体记录相同。
- 本例仅有两个分支，不受 `union_all_faststart_limit_threshold` 限制。该参数仅控制 5 个及以上分支场景允许重排的最大 `LIMIT + OFFSET` 需求量，默认值为 100，取 0 时禁用该档重排；它不控制查询实际返回的行数。
- 本例使用 `none` 和 `union_all_faststart` 单独设置规则，以便对照。业务会话中若需保留其他规则，应在 `rewrite_rule` 中一并列出；`none` 会关闭该参数控制的所有可选重写规则。
- 参数的完整说明参见《数据库参考》中的[其他优化器选项](https://docs.opengauss.org/zh/docs/latest/database_reference/other_optimizer_options.html)。

### 多分支重排与阈值控制

对于 **5 个及以上 UNION ALL 分支**，本特性仅按分支的 `startup_cost`（启动成本）升序排列，不枚举比较分支排列的取数成本。

该行为由 `union_all_faststart_limit_threshold` 控制。只有启用 `rewrite_rule` 中的 `union_all_faststart`、通过其他适用性检查，且 `LIMIT + OFFSET` **小于或等于阈值**时，才进行上述排序；需求量超过阈值时，跳过本特性的分支重排。

该参数为 USERSET 类型，可通过会话 `SET` 调整，取值范围为 0～2147483647，默认值为 **100**。设置为 **0** 时禁用 5 个及以上分支的重排，**不影响 2～4 分支**。该阈值不是分支数量上限，也不改变 SQL 中 LIMIT/OFFSET 的取数语义。以上两分支案例不受此阈值限制。
