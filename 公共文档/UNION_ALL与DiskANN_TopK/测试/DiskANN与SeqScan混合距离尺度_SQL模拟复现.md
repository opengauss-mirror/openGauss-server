# delta/snapshot/delete 典型结构：DiskANN 与 SeqScan 混合距离尺度 Top-10 SQL 模拟复现

## 1. 结论摘要

本用例不修改内核、不重新编译，只用 SQL 和 hint 模拟“DiskANN 分支将原始平方 L2 `d²` 直接交给上层，SeqScan 分支仍交付 SQL L2 `d`”的错误改写。业务结构与目标场景一致：

```text
delta_t
UNION ALL
snapshot_t Anti Join delete_t
```

查询包含必需的 `WHERE title IS NOT NULL`、欧式距离排序和 `LIMIT 10`，最外层只返回 `row_id`。

2026-09-14 在 openGauss Debug 构建 `098d77c8`、端口 `55440` 上实测：

| 指标 | 实测值 |
|---|---:|
| 正确 Top-10 中 delta 行 | 5 |
| 正确 Top-10 中 snapshot 行 | 5 |
| 错误混合键 Top-10 中 delta 行 | 0 |
| 错误混合键 Top-10 中 snapshot 行 | 10 |
| 两份 Top-10 交集 | 5 |
| 正确结果遗漏 | 5 |
| 错误多出行 | 5 |
| Recall@10 | 0.50 |

正确 Top-10：

```text
101, 201, 102, 202, 103, 203, 104, 204, 105, 205
```

错误混合键 Top-10：

```text
201, 202, 203, 204, 205, 206, 207, 208, 209, 210
```

delta 分支中真实距离为 `5、7、9、11、13` 的 5 条行都已被 DiskANN 找回，却在上层混合比较中被排除。因此这是召回之后的跨分支排名错误，不是 DiskANN 原有的 ANN 召回误差。

真实混合访问路径的设计证据见 [DiskANN 距离结果复用：详细开发设计](../设计/ANN距离结果复用详细开发设计.md) 第 `10.4.1` 节。

## 2. 数据设计

查询向量为：

```text
q = [2,2,2,2]
```

向量只在第一维与 q 不同，所以距离均为容易核对的整数。

### 2.1 WHERE 和 delete 真正生效

| 来源 | row_id | L2 | 处理 |
|---|---:|---:|---|
| delta_t | 100 | 1 | `title IS NULL`，被 WHERE 过滤 |
| snapshot_t | 3 | 2 | `title IS NULL`，被 WHERE 过滤 |
| snapshot_t | 1 | 4 | 命中 delete_t，被 Anti Join 删除 |
| snapshot_t | 212 | 28 | 命中 delete_t，被 Anti Join 删除 |

因此这些行不会偶然混入 Top-10，WHERE 与 delete 语义不是只为了让计划看起来更像业务。

### 2.2 有效候选故意交错

```text
delta_t：   d = 5, 7, 9, 11, 13, 15, 17, 19, 21, 23, ...
snapshot_t：d = 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, ...
```

所以正确的全局 Top-10 必然是 delta/snapshot 交错的 5+5 结果。

错误改写下：

```text
delta_t 上传：   d² = 25, 49, 81, 121, 169, ...
snapshot_t 上传：d  =  6,  8, 10,  12,  14, ...
```

snapshot 前 10 条的键为 `6..24`，全部小于 delta 第一条的键 `25`，因此错误 Top-10 会被 snapshot 完全占满。

## 3. SQL 模拟原理与边界

当前 SQL 不能直接读取某个 DiskANN 候选的 `cand->distance` 或 `xs_orderbyvals`。但对本用例的非 PQ L2 路径，DiskANN 最终精排分数的数值口径是：

```text
vector_l2_squared_distance(embedding, query) = d²
```

所以可以用 SQL 显式计算同一个 `d²`，模拟错误改写已把 AM 原始值交给上层：

| 角色 | SQL 实现 | 口径 |
|---|---|---|
| delta 分支搜索 | `<->` + `indexscan(dt delta_embedding_diskann)` | DiskANN AM 按 `d²` 排序 |
| delta 分支模拟上传键 | `vector_l2_squared_distance(...)` | `d²` |
| snapshot 分支 | `<->` + `tablescan(st)` | SeqScan 计算 SQL L2 `d` |
| delete 语义 | `NOT EXISTS` | Merge Anti Join |
| 上层消费 | `ORDER BY mixed_sort_key LIMIT 10` | 混合比较 `d²` 和 `d` |

这个 SQL 验证的是数值与结果正确性，不是性能。SQL 中的平方距离仍会计算一次；未来内核实现的目标才是直接读取 AM 缓存。两者的值在本测试前提下都是同一口径的 `d²`，因此可以等价验证上层混合比较的正确性。

## 4. 完整可执行 SQL

以下 SQL 可整段粘贴到 `gsql`。所有对象都在事务中创建，末尾 `ROLLBACK` 后不保留 schema、表、索引或数据。

```sql
\set ON_ERROR_STOP on
BEGIN;

DROP SCHEMA IF EXISTS union_all_diskann_scale_top10_repro CASCADE;
CREATE SCHEMA union_all_diskann_scale_top10_repro;
SET LOCAL search_path = union_all_diskann_scale_top10_repro, public;

SET LOCAL enable_indexscan = on;
SET LOCAL enable_seqscan = on;
SET LOCAL enable_bitmapscan = off;
SET LOCAL enable_hashjoin = off;
SET LOCAL enable_mergejoin = on;
SET LOCAL enable_nestloop = off;

CREATE TABLE delta_t (
    row_id integer PRIMARY KEY,
    title text,
    embedding vector(4)
);

CREATE TABLE snapshot_t (
    row_id integer PRIMARY KEY,
    title text,
    embedding vector(4)
);

CREATE TABLE delete_t (
    row_id integer PRIMARY KEY,
    reason text
);

INSERT INTO delta_t VALUES
    (100, NULL,          '[3,2,2,2]'),
    (101, 'delta-d5',    '[7,2,2,2]'),
    (102, 'delta-d7',    '[9,2,2,2]'),
    (103, 'delta-d9',    '[11,2,2,2]'),
    (104, 'delta-d11',   '[13,2,2,2]'),
    (105, 'delta-d13',   '[15,2,2,2]'),
    (106, 'delta-d15',   '[17,2,2,2]'),
    (107, 'delta-d17',   '[19,2,2,2]'),
    (108, 'delta-d19',   '[21,2,2,2]'),
    (109, 'delta-d21',   '[23,2,2,2]'),
    (110, 'delta-d23',   '[25,2,2,2]'),
    (111, 'delta-d25',   '[27,2,2,2]'),
    (112, 'delta-d27',   '[29,2,2,2]');

INSERT INTO snapshot_t VALUES
    (1,   'snapshot-deleted-d4',  '[6,2,2,2]'),
    (3,   NULL,                   '[4,2,2,2]'),
    (201, 'snapshot-d6',          '[8,2,2,2]'),
    (202, 'snapshot-d8',          '[10,2,2,2]'),
    (203, 'snapshot-d10',         '[12,2,2,2]'),
    (204, 'snapshot-d12',         '[14,2,2,2]'),
    (205, 'snapshot-d14',         '[16,2,2,2]'),
    (206, 'snapshot-d16',         '[18,2,2,2]'),
    (207, 'snapshot-d18',         '[20,2,2,2]'),
    (208, 'snapshot-d20',         '[22,2,2,2]'),
    (209, 'snapshot-d22',         '[24,2,2,2]'),
    (210, 'snapshot-d24',         '[26,2,2,2]'),
    (211, 'snapshot-d26',         '[28,2,2,2]'),
    (212, 'snapshot-deleted-d28', '[30,2,2,2]');

INSERT INTO delete_t VALUES
    (1,   'deleted'),
    (212, 'deleted'),
    (999, 'not-present');

CREATE INDEX delta_embedding_diskann ON delta_t
USING diskann (embedding vector_l2_ops);

CREATE VIEW gs_1w_1bucket AS
SELECT row_id, title, embedding
FROM delta_t
UNION ALL
SELECT s.row_id, s.title, s.embedding
FROM snapshot_t s
WHERE NOT EXISTS (
    SELECT 1 FROM delete_t d WHERE d.row_id = s.row_id
);

-- 证据一：同一套表能真实生成目标 Merge Append 计划。
SET LOCAL enable_union_all_diskann_topk_pushdown = on;
SET LOCAL enable_union_all_required_columns_pushdown = on;

EXPLAIN (VERBOSE ON, COSTS OFF)
SELECT row_id
FROM (
    SELECT /*+ indexscan(dt delta_embedding_diskann) */
           dt.row_id, dt.title, dt.embedding
    FROM delta_t dt
    UNION ALL
    SELECT /*+ tablescan(st) */
           st.row_id, st.title, st.embedding
    FROM snapshot_t st
    WHERE NOT EXISTS (
        SELECT 1 FROM delete_t dd WHERE dd.row_id = st.row_id
    )
) u
WHERE title IS NOT NULL
ORDER BY embedding <-> '[2,2,2,2]'::vector
LIMIT 10;

-- 证据二：关闭自动改写，隔离测试 SQL 显式构造的 d²/d 口径。
SET LOCAL enable_union_all_diskann_topk_pushdown = off;
SET LOCAL enable_union_all_required_columns_pushdown = off;

EXPLAIN (VERBOSE ON, COSTS OFF)
SELECT row_id
FROM (
    SELECT d.row_id,
           vector_l2_squared_distance(
               d.embedding, '[2,2,2,2]'::vector
           ) AS mixed_sort_key
    FROM (
        SELECT /*+ indexscan(dt delta_embedding_diskann) */
               dt.row_id, dt.embedding
        FROM delta_t dt
        WHERE dt.title IS NOT NULL
        ORDER BY dt.embedding <-> '[2,2,2,2]'::vector
        LIMIT 10
    ) d
    UNION ALL
    SELECT s.row_id,
           s.embedding <-> '[2,2,2,2]'::vector AS mixed_sort_key
    FROM (
        SELECT /*+ tablescan(st) */
               st.row_id, st.embedding
        FROM snapshot_t st
        WHERE st.title IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM delete_t dd WHERE dd.row_id = st.row_id
          )
        ORDER BY st.embedding <-> '[2,2,2,2]'::vector
        LIMIT 10
    ) s
) candidates
ORDER BY mixed_sort_key
LIMIT 10;

-- SQL 1（正确）：两个分支向上层提供的 sort_key 都是 SQL L2 d。
CREATE TEMP TABLE exact_top10 AS
SELECT row_number() OVER (ORDER BY sort_key)::integer AS pos,
       src,
       row_id,
       real_l2,
       sort_key
FROM (
    SELECT 'delta'::text AS src,
           d.row_id,
           d.embedding <-> '[2,2,2,2]'::vector AS real_l2,
           d.embedding <-> '[2,2,2,2]'::vector AS sort_key
    FROM (
        SELECT /*+ indexscan(dt delta_embedding_diskann) */
               dt.row_id, dt.embedding
        FROM delta_t dt
        WHERE dt.title IS NOT NULL
        ORDER BY dt.embedding <-> '[2,2,2,2]'::vector
        LIMIT 10
    ) d
    UNION ALL
    SELECT 'snapshot'::text AS src,
           s.row_id,
           s.embedding <-> '[2,2,2,2]'::vector AS real_l2,
           s.embedding <-> '[2,2,2,2]'::vector AS sort_key
    FROM (
        SELECT /*+ tablescan(st) */
               st.row_id, st.embedding
        FROM snapshot_t st
        WHERE st.title IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM delete_t dd WHERE dd.row_id = st.row_id
          )
        ORDER BY st.embedding <-> '[2,2,2,2]'::vector
        LIMIT 10
    ) s
) x
ORDER BY sort_key
LIMIT 10;

-- 正确 SQL 的 Top-10 结果表。
SELECT pos, src, row_id, real_l2, sort_key
FROM exact_top10
ORDER BY pos;

-- SQL 2（错误模拟）：仅 delta 分支改为向上层提供 d²。
CREATE TEMP TABLE mixed_top10 AS
SELECT row_number() OVER (ORDER BY sort_key)::integer AS pos,
       src,
       row_id,
       real_l2,
       sort_key
FROM (
    SELECT 'delta'::text AS src,
           d.row_id,
           d.embedding <-> '[2,2,2,2]'::vector AS real_l2,
           vector_l2_squared_distance(
               d.embedding, '[2,2,2,2]'::vector
           ) AS sort_key
    FROM (
        SELECT /*+ indexscan(dt delta_embedding_diskann) */
               dt.row_id, dt.embedding
        FROM delta_t dt
        WHERE dt.title IS NOT NULL
        ORDER BY dt.embedding <-> '[2,2,2,2]'::vector
        LIMIT 10
    ) d
    UNION ALL
    SELECT 'snapshot'::text AS src,
           s.row_id,
           s.embedding <-> '[2,2,2,2]'::vector AS real_l2,
           s.embedding <-> '[2,2,2,2]'::vector AS sort_key
    FROM (
        SELECT /*+ tablescan(st) */
               st.row_id, st.embedding
        FROM snapshot_t st
        WHERE st.title IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM delete_t dd WHERE dd.row_id = st.row_id
          )
        ORDER BY st.embedding <-> '[2,2,2,2]'::vector
        LIMIT 10
    ) s
) x
ORDER BY sort_key
LIMIT 10;

-- 错误模拟 SQL 的 Top-10 结果表；字段与正确 SQL 完全相同。
SELECT pos, src, row_id, real_l2, sort_key
FROM mixed_top10
ORDER BY pos;

-- 集合指标：交集、遗漏、多出与 Recall@10。
SELECT count(*) AS overlap_count,
       10 - count(*) AS exact_missing_count,
       10 - count(*) AS mixed_extra_count,
       round(count(*)::numeric / 10, 2) AS recall_at_10
FROM exact_top10 e
JOIN mixed_top10 m USING (row_id);

-- 列出遗漏和多出的具体行。
SELECT 'missing_from_mixed'::text AS difference,
       CASE WHEN e.row_id BETWEEN 100 AND 199
            THEN 'delta' ELSE 'snapshot' END AS src,
       e.row_id,
       e.real_l2
FROM exact_top10 e
LEFT JOIN mixed_top10 m USING (row_id)
WHERE m.row_id IS NULL
UNION ALL
SELECT 'extra_in_mixed'::text,
       m.src,
       m.row_id,
       m.real_l2
FROM mixed_top10 m
LEFT JOIN exact_top10 e USING (row_id)
WHERE e.row_id IS NULL
ORDER BY difference, row_id;

ROLLBACK;
```

## 5. 计划证据

### 5.1 真实目标形状：Merge Append + Ann Index Scan + Anti Join

```text
                                                       QUERY PLAN
------------------------------------------------------------------------------------------------------------------------
 Limit
   Output: dt.row_id, ((dt.embedding <-> '[2,2,2,2]'::vector))
   ->  Result
         Output: dt.row_id, ((dt.embedding <-> '[2,2,2,2]'::vector))
         ->  Merge Append
               Sort Key: ((dt.embedding <-> '[2,2,2,2]'::vector))
               ->  Limit
                     Output: dt.row_id, ((dt.embedding <-> '[2,2,2,2]'::vector))
                     ->  Ann Index Scan using delta_embedding_diskann on union_all_diskann_scale_top10_repro.delta_t dt
                           Output: dt.row_id, (dt.embedding <-> '[2,2,2,2]'::vector)
                           Order By: (dt.embedding <-> '[2,2,2,2]'::vector)
                           Filter Mode: Post-Filter
                           Filter: (dt.title IS NOT NULL)
               ->  Limit
                     Output: st.row_id, ((st.embedding <-> '[2,2,2,2]'::vector))
                     ->  Sort
                           Output: st.row_id, ((st.embedding <-> '[2,2,2,2]'::vector))
                           Sort Key: ((st.embedding <-> '[2,2,2,2]'::vector))
                           ->  Merge Anti Join
                                 Output: st.row_id, (st.embedding <-> '[2,2,2,2]'::vector)
                                 Merge Cond: (st.row_id = dd.row_id)
                                 ->  Sort
                                       Output: st.row_id, st.embedding
                                       Sort Key: st.row_id
                                       ->  Seq Scan on union_all_diskann_scale_top10_repro.snapshot_t st
                                             Output: st.row_id, st.embedding
                                             Filter: (st.title IS NOT NULL)
                                 ->  Sort
                                       Output: dd.row_id
                                       Sort Key: dd.row_id
                                       ->  Seq Scan on union_all_diskann_scale_top10_repro.delete_t dd
                                             Output: dd.row_id
(32 rows)
```

这证明 DiskANN delta 分支与 SeqScan snapshot Anti Join delete 分支确实能同时成为 Merge Append 输入。当前版本两个分支向上传的都是 SQL L2 `d`，所以该计划当前是正确的。

### 5.2 SQL 显式混合 d²/d 的模拟计划

```text
                                                             QUERY PLAN
------------------------------------------------------------------------------------------------------------------------------------
 Limit
   Output: d.row_id, (vector_l2_squared_distance(d.embedding, '[2,2,2,2]'::vector))
   ->  Sort
         Output: d.row_id, (vector_l2_squared_distance(d.embedding, '[2,2,2,2]'::vector))
         Sort Key: (vector_l2_squared_distance(d.embedding, '[2,2,2,2]'::vector))
         ->  Result
               Output: d.row_id, (vector_l2_squared_distance(d.embedding, '[2,2,2,2]'::vector))
               ->  Append
                     ->  Subquery Scan on d
                           Output: d.row_id, vector_l2_squared_distance(d.embedding, '[2,2,2,2]'::vector)
                           ->  Limit
                                 Output: dt.row_id, dt.embedding, ((dt.embedding <-> '[2,2,2,2]'::vector))
                                 ->  Ann Index Scan using delta_embedding_diskann on union_all_diskann_scale_top10_repro.delta_t dt
                                       Output: dt.row_id, dt.embedding, (dt.embedding <-> '[2,2,2,2]'::vector)
                                       Order By: (dt.embedding <-> '[2,2,2,2]'::vector)
                                       Filter Mode: Post-Filter
                                       Filter: (dt.title IS NOT NULL)
                     ->  Subquery Scan on s
                           Output: s.row_id, (s.embedding <-> '[2,2,2,2]'::vector)
                           ->  Limit
                                 Output: st.row_id, st.embedding, ((st.embedding <-> '[2,2,2,2]'::vector))
                                 ->  Sort
                                       Output: st.row_id, st.embedding, ((st.embedding <-> '[2,2,2,2]'::vector))
                                       Sort Key: ((st.embedding <-> '[2,2,2,2]'::vector))
                                       ->  Merge Anti Join
                                             Output: st.row_id, st.embedding, (st.embedding <-> '[2,2,2,2]'::vector)
                                             Merge Cond: (st.row_id = dd.row_id)
                                             ->  Sort
                                                   Output: st.row_id, st.embedding
                                                   Sort Key: st.row_id
                                                   ->  Seq Scan on union_all_diskann_scale_top10_repro.snapshot_t st
                                                         Output: st.row_id, st.embedding
                                                         Filter: (st.title IS NOT NULL)
                                             ->  Sort
                                                   Output: dd.row_id
                                                   Sort Key: dd.row_id
                                                   ->  Seq Scan on union_all_diskann_scale_top10_repro.delete_t dd
                                                         Output: dd.row_id
(38 rows)
```

这里顶层是 Sort，因为当前优化器正确地不会将 `vector_l2_squared_distance(...)` 与 `<->` 宣称为同一 pathkey。错误内核改写若将 `d²` 放入了原 `<->` 的 targetlist 位置，Merge Append 则会直接比较该槽位中的 `d²` 与 `d`。Sort 与 Merge Append 节点形状不同，但对本用例的跨分支标量比较结果相同。

## 6. Top-10 结果差异

### 6.1 正确 SQL 的 Top-10 查询结果

```text
 pos |   src    | row_id | real_l2 | sort_key
-----+----------+--------+---------+----------
   1 | delta    |    101 |       5 |        5
   2 | snapshot |    201 |       6 |        6
   3 | delta    |    102 |       7 |        7
   4 | snapshot |    202 |       8 |        8
   5 | delta    |    103 |       9 |        9
   6 | snapshot |    203 |      10 |       10
   7 | delta    |    104 |      11 |       11
   8 | snapshot |    204 |      12 |       12
   9 | delta    |    105 |      13 |       13
  10 | snapshot |    205 |      14 |       14
(10 rows)
```

### 6.2 错误模拟 SQL 的 Top-10 查询结果

```text
 pos |   src    | row_id | real_l2 | sort_key
-----+----------+--------+---------+----------
   1 | snapshot |    201 |       6 |        6
   2 | snapshot |    202 |       8 |        8
   3 | snapshot |    203 |      10 |       10
   4 | snapshot |    204 |      12 |       12
   5 | snapshot |    205 |      14 |       14
   6 | snapshot |    206 |      16 |       16
   7 | snapshot |    207 |      18 |       18
   8 | snapshot |    208 |      20 |       20
   9 | snapshot |    209 |      22 |       22
  10 | snapshot |    210 |      24 |       24
(10 rows)
```

两张表的字段顺序和含义完全一致；区别只来自生成它们的 SQL：正确 SQL 的两个分支都按 `d` 排序，错误模拟 SQL 的 delta 分支按 `d²` 与 snapshot 分支的 `d` 混合排序。

### 6.3 集合指标

```text
 overlap_count | exact_missing_count | mixed_extra_count | recall_at_10
---------------+---------------------+-------------------+--------------
             5 |                   5 |                 5 |          .50
(1 row)
```

### 6.4 遗漏与多出行

```text
     difference     |   src    | row_id | real_l2
--------------------+----------+--------+---------
 extra_in_mixed     | snapshot |    206 |      16
 extra_in_mixed     | snapshot |    207 |      18
 extra_in_mixed     | snapshot |    208 |      20
 extra_in_mixed     | snapshot |    209 |      22
 extra_in_mixed     | snapshot |    210 |      24
 missing_from_mixed | delta    |    101 |       5
 missing_from_mixed | delta    |    102 |       7
 missing_from_mixed | delta    |    103 |       9
 missing_from_mixed | delta    |    104 |      11
 missing_from_mixed | delta    |    105 |      13
(10 rows)
```

## 7. 证据边界与验收口径

本用例能证明：

1. 典型 `delta_t UNION ALL (snapshot_t Anti Join delete_t)` 结构能实际生成 DiskANN + SeqScan + Merge Append 计划。
2. WHERE、delete 语义、分支 Limit 和最终 Limit 10 都真正参与查询。
3. delta 分支所有应进入正确 Top-10 的行已被 DiskANN 找回。
4. 上层混合比较 `d²` 与 `d` 可以使 Recall@10 从 1.00 下降到 0.50。
5. 这是候选已找回之后的全局排名错误，不是 ANN 原有召回误差。

本用例不能证明 SQL 中的 `d²` 是从当次 AM 候选对象直接读出的，也不能验证零重算性能收益；这两项必须由真实内核通道与计数器验证。本用例仅覆盖非 PQ、最终完整向量精排的 L2 口径。

### 7.1 对 AM 内部 `d²` 开平方的误差分析

对本用例限定的**非 PQ、完整原始 vector、最终精排 L2** 路径，修复不需要在执行器里重新计算一遍向量距离。DiskANN L2 support function 已经产生 float 平方距离 `d²`，而 SQL `<->` 的核心计算为：

```cpp
sqrt((double)VectorL2SquaredDistance(a->dim, a->x, b->x))
```

因此 AM 对同一个已精排候选执行：

```cpp
double output_distance = sqrt((double)candidate.search_distance);
```

就能把 `d²` 转换为与 SeqScan `<->` 相同的 `d` 口径。它只增加一次 O(1) 标量开平方，不增加 O(dim) 向量遍历。

| 问题 | 结论 |
|---|---|
| 是否产生新的量化/算法误差 | 不产生 |
| 与 SQL `<->` 的数值差异 | 同一向量、同一 float `d²`、同一 double `sqrt` 时预期完全一致；宽泛表述为至多原有浮点舍入级差异 |
| 是否改变 DiskANN 分支内顺序 | 不改变，`sqrt` 在有限非负值上严格单调 |
| 是否修复本文 d²/d 混合排序 | 是，因为 DiskANN 与 SeqScan 都向全局比较域提供 `d` |
| 是否适用 PQ/量化距离 | 不适用；开平方不会消除已有的近似误差 |

实施时必须确认该值的来源是当前 query 与有效索引条目的完整 L2 平方距离，不能对 PQ 估算分数、Cosine/内积分数或其他变换分数盲目开平方。应使用 double `sqrt`，不应使用 `sqrtf` 后再转 double。完整实现契约见 [DiskANN 距离结果复用：详细开发设计](../设计/ANN距离结果复用详细开发设计.md) 第 `4.1.1` 节。

### 7.2 修复后的统一口径与验收

修复后必须选择一种统一口径：

```text
口径 A：所有分支向上层提供 d
DiskANN d² -> 一次标量 sqrt -> d
Snapshot SeqScan -> d
```

或者，在已证明距离只用于排序时：

```text
口径 B：同一全局比较域的所有分支都提供精确 d²
DiskANN -> 直接复用 d²
Snapshot SeqScan -> 产生 d² 排序键
```

禁止形成：

```text
DiskANN -> d²
Snapshot SeqScan -> d
              ↓
       Sort / Merge Append
```

修复后本用例必须满足：

```text
Top-10 = 101,201,102,202,103,203,104,204,105,205
overlap_count = 10
exact_missing_count = 0
mixed_extra_count = 0
Recall@10 = 1.00
```
