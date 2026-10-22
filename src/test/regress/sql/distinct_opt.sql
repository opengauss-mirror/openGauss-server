-- ==================================================================
-- DISTINCT 冗余优化 测试用例
-- 验证 check_distinct_redundant_by_unique() 修复缺陷后的正确性
--
-- 验证目标:
--   正向场景: SELECT 列覆盖唯一索引全部键列 → DISTINCT 应被消除
--   负向场景: 无法严格证明全表唯一 → DISTINCT 必须保留, 结果不能错
--
-- 每个测试都输出 EXPLAIN 计划 + 去重结果行数, 双重验证。
-- 去重结果统一用 count(*) FROM (SELECT DISTINCT ...) 统计, 包含 NULL。
-- ==================================================================

DROP DATABASE IF EXISTS distinct_opt_b;
DROP TABLE IF EXISTS t_pk CASCADE;
DROP TABLE IF EXISTS t_composite CASCADE;
DROP TABLE IF EXISTS t_unique_notnull CASCADE;
DROP TABLE IF EXISTS t_unique_nullable CASCADE;
DROP TABLE IF EXISTS t_partial CASCADE;
DROP TABLE IF EXISTS t_deferrable CASCADE;
DROP TABLE IF EXISTS t_join CASCADE;
DROP TABLE IF EXISTS t_expr CASCADE;
DROP TABLE IF EXISTS t_nokey CASCADE;
DROP TABLE IF EXISTS t_partition_local CASCADE;
DROP TABLE IF EXISTS t_partition_gpi CASCADE;
DROP TABLE IF EXISTS t_coll CASCADE;
DROP TABLE IF EXISTS t_multiidx CASCADE;
DROP TABLE IF EXISTS t_include CASCADE;
DROP TABLE IF EXISTS t_ps_idx CASCADE;
DROP TABLE IF EXISTS t_ps_nn CASCADE;
DROP TABLE IF EXISTS t_ps_con CASCADE;
DROP TABLE IF EXISTS t_empty CASCADE;
DROP TABLE IF EXISTS t_single CASCADE;
DROP TABLE IF EXISTS t_multi CASCADE;

-- [正向 T1] 单列主键覆盖 → 应消除 DISTINCT
CREATE TABLE t_pk(k int PRIMARY KEY, v text);
INSERT INTO t_pk SELECT g, concat('v', g) FROM generate_series(1, 5) g;
EXPLAIN (COSTS OFF) SELECT DISTINCT k, v FROM t_pk;
SELECT count(*) AS correct_count FROM (SELECT DISTINCT k, v FROM t_pk) s;

-- [正向 T2] 复合主键全部键列覆盖 → 应消除 DISTINCT
CREATE TABLE t_composite(a int, b int, c int, PRIMARY KEY(a, b));
-- a 列有重复值, 但 (a,b) 组合唯一
INSERT INTO t_composite SELECT g % 3, g, g * 10 FROM generate_series(1, 6) g;
EXPLAIN (COSTS OFF) SELECT DISTINCT a, b FROM t_composite;
SELECT count(*) AS correct_count FROM (SELECT DISTINCT a, b FROM t_composite) s;

-- [正向 T3] NOT NULL 唯一索引覆盖 → 应消除 DISTINCT
CREATE TABLE t_unique_notnull(k int NOT NULL UNIQUE, v text);
INSERT INTO t_unique_notnull SELECT g, concat('x', g) FROM generate_series(1, 5) g;
EXPLAIN (COSTS OFF) SELECT DISTINCT k FROM t_unique_notnull;
SELECT count(*) AS correct_count FROM (SELECT DISTINCT k FROM t_unique_notnull) s;

-- [负向 N1] 缺陷1: UNIQUE 可空列, 多个 NULL → 应保留 DISTINCT
CREATE TABLE t_unique_nullable(k int UNIQUE);
INSERT INTO t_unique_nullable VALUES (NULL), (NULL), (1), (2);
EXPLAIN (COSTS OFF) SELECT DISTINCT k FROM t_unique_nullable;
SELECT count(*) AS correct_count FROM (SELECT DISTINCT k FROM t_unique_nullable) s;

-- [负向 N2] 缺陷2: 部分唯一索引 → 应保留 DISTINCT
CREATE TABLE t_partial(k int NOT NULL);
CREATE UNIQUE INDEX t_partial_u ON t_partial(k) WHERE k > 0;
INSERT INTO t_partial VALUES (0), (0), (1), (2);
EXPLAIN (COSTS OFF) SELECT DISTINCT k FROM t_partial;
SELECT count(*) AS correct_count FROM (SELECT DISTINCT k FROM t_partial) s;

-- [负向 N3] 缺陷3: 延迟唯一约束 → 应保留 DISTINCT
CREATE TABLE t_deferrable(k int UNIQUE DEFERRABLE INITIALLY DEFERRED);
BEGIN;
INSERT INTO t_deferrable VALUES (1), (1);
EXPLAIN (COSTS OFF) SELECT DISTINCT k FROM t_deferrable;
SELECT count(*) AS correct_count FROM (SELECT DISTINCT k FROM t_deferrable) s;
ROLLBACK;

-- [负向 N4] 缺陷4: JOIN 倍增行源 → 应保留 DISTINCT
CREATE TABLE t_join(k int PRIMARY KEY);
INSERT INTO t_join VALUES (1);
EXPLAIN (COSTS OFF)
SELECT DISTINCT t.k FROM t_join t CROSS JOIN (VALUES (1), (2)) v(x);
SELECT count(*) AS correct_count FROM (
    SELECT DISTINCT t.k FROM t_join t CROSS JOIN (VALUES (1), (2)) v(x)
) s;

-- [负向 N5] 表达式索引 (键列无法匹配普通列) → 应保留 DISTINCT
CREATE TABLE t_expr(k int);
CREATE UNIQUE INDEX t_expr_u ON t_expr(lower(k::text));
INSERT INTO t_expr VALUES (1), (2);
EXPLAIN (COSTS OFF) SELECT DISTINCT k FROM t_expr;
SELECT count(*) AS correct_count FROM (SELECT DISTINCT k FROM t_expr) s;

-- [负向 N6] 复合主键只覆盖部分键列 → 应保留 DISTINCT
EXPLAIN (COSTS OFF) SELECT DISTINCT a FROM t_composite;
SELECT count(*) AS correct_count FROM (SELECT DISTINCT a FROM t_composite) s;

-- [负向 N7] 无任何唯一索引覆盖 → 应保留 DISTINCT
CREATE TABLE t_nokey(k int, v text);
INSERT INTO t_nokey SELECT g % 3, 'v' FROM generate_series(1, 9) g;
EXPLAIN (COSTS OFF) SELECT DISTINCT k FROM t_nokey;
SELECT count(*) AS correct_count FROM (SELECT DISTINCT k FROM t_nokey) s;

-- [正向 T4] 分区表 local 主键覆盖 → 应消除 DISTINCT
CREATE TABLE t_partition_local(k int, v text, PRIMARY KEY(k))
PARTITION BY RANGE(k)(
    PARTITION p1 VALUES LESS THAN (10),
    PARTITION p2 VALUES LESS THAN (MAXVALUE)
);
INSERT INTO t_partition_local SELECT g, concat('p', g) FROM generate_series(1, 20) g;
EXPLAIN (COSTS OFF) SELECT DISTINCT k, v FROM t_partition_local;
SELECT count(*) AS correct_count FROM (SELECT DISTINCT k, v FROM t_partition_local) s;

-- [负向 N8] 分区表 GPI 主键不能作为 DISTINCT 优化依据
CREATE TABLE t_partition_gpi(k int NOT NULL, p int, v text)
PARTITION BY RANGE(p)(
    PARTITION p1 VALUES LESS THAN (10),
    PARTITION p2 VALUES LESS THAN (MAXVALUE)
);
CREATE UNIQUE INDEX t_partition_gpi_u ON t_partition_gpi(k) GLOBAL;
ALTER TABLE t_partition_gpi ADD CONSTRAINT t_partition_gpi_pkey PRIMARY KEY USING INDEX t_partition_gpi_u;
INSERT INTO t_partition_gpi SELECT g, g % 20, concat('g', g) FROM generate_series(1, 20) g;
EXPLAIN (COSTS OFF) SELECT DISTINCT k, v FROM t_partition_gpi;
SELECT count(*) AS correct_count FROM (SELECT DISTINCT k, v FROM t_partition_gpi) s;

-- ==================================================================
-- 补充回归一: 等值语义 (索引 opfamily/collation 必须与 DISTINCT 等值语义匹配)
-- ==================================================================

-- [负向 N9] 唯一索引 collation 与 DISTINCT 表达式 collation 不匹配 → 应保留 DISTINCT
CREATE TABLE t_coll(k text NOT NULL);
CREATE UNIQUE INDEX t_coll_en ON t_coll(k COLLATE "en_US.utf8");
INSERT INTO t_coll SELECT 'k' || g FROM generate_series(1, 4) g;
EXPLAIN (COSTS OFF) SELECT DISTINCT k FROM t_coll;
SELECT count(*) AS correct_count FROM (SELECT DISTINCT k FROM t_coll) s;

-- ==================================================================
-- 补充回归二: 多索引遍历与 INCLUDE 列
-- ==================================================================

-- [正向 T5] 首个唯一索引可空不合格, 继续遍历命中后续合格唯一索引 → 应消除 DISTINCT
CREATE TABLE t_multiidx(a int UNIQUE, k text NOT NULL UNIQUE);
INSERT INTO t_multiidx VALUES (NULL, 'x'), (1, 'y'), (2, 'z');
EXPLAIN (COSTS OFF) SELECT DISTINCT k FROM t_multiidx;
SELECT count(*) AS correct_count FROM (SELECT DISTINCT k FROM t_multiidx) s;

-- [正向 T6] ubtree 唯一索引键列覆盖 → 应消除 DISTINCT (INCLUDE 列不参与唯一性证明)
CREATE TABLE t_include(k int NOT NULL, v text) WITH (STORAGE_TYPE = USTORE);
CREATE UNIQUE INDEX t_include_u ON t_include USING ubtree(k) INCLUDE(v);
INSERT INTO t_include SELECT g, 'v' || g FROM generate_series(1, 5) g;
EXPLAIN (COSTS OFF) SELECT DISTINCT k, v FROM t_include;
SELECT count(*) AS correct_count FROM (SELECT DISTINCT k, v FROM t_include) s;

-- [负向 N10] DISTINCT 列只是 INCLUDE 列而非键列 → 应保留 DISTINCT
EXPLAIN (COSTS OFF) SELECT DISTINCT v FROM t_include;
SELECT count(*) AS correct_count FROM (SELECT DISTINCT v FROM t_include) s;

-- ==================================================================
-- 补充回归三: 排序与访问路径
-- ==================================================================

-- [正向 T7] 消除 DISTINCT 后仍须满足 ORDER BY 排序要求
EXPLAIN (COSTS OFF) SELECT DISTINCT k, v FROM t_pk ORDER BY k DESC;
SELECT DISTINCT k, v FROM t_pk ORDER BY k DESC;

-- [负向路径 N11] 关闭 HashAgg 后负向场景退化为 Sort + Unique, 结果仍正确
SET enable_hashagg = off;
EXPLAIN (COSTS OFF) SELECT DISTINCT k FROM t_nokey;
SELECT count(*) AS correct_count FROM (SELECT DISTINCT k FROM t_nokey) s;
RESET enable_hashagg;

-- ==================================================================
-- 补充回归四: DDL 与预编译语句缓存 (DDL 后必须按新元数据重新规划)
-- ==================================================================

-- [正向 T8] DROP INDEX 后重新规划 → DISTINCT 恢复, 重复行正确去重
CREATE TABLE t_ps_idx(k int NOT NULL, v text);
CREATE UNIQUE INDEX t_ps_idx_u ON t_ps_idx(k);
INSERT INTO t_ps_idx VALUES (1, 'a'), (2, 'b');
PREPARE ps_idx AS SELECT DISTINCT k FROM t_ps_idx;
EXPLAIN (COSTS OFF) EXECUTE ps_idx;
DROP INDEX t_ps_idx_u;
INSERT INTO t_ps_idx VALUES (1, 'dup');
EXPLAIN (COSTS OFF) EXECUTE ps_idx;
EXECUTE ps_idx;

-- [负向 N12] 删除 NOT NULL 后唯一性证明不再成立 → 重新规划保留 DISTINCT
CREATE TABLE t_ps_nn(k int NOT NULL, v text);
CREATE UNIQUE INDEX t_ps_nn_u ON t_ps_nn(k);
INSERT INTO t_ps_nn VALUES (1, 'a'), (2, 'b');
PREPARE ps_nn AS SELECT DISTINCT k FROM t_ps_nn;
EXPLAIN (COSTS OFF) EXECUTE ps_nn;
ALTER TABLE t_ps_nn ALTER COLUMN k DROP NOT NULL;
EXPLAIN (COSTS OFF) EXECUTE ps_nn;
EXECUTE ps_nn;

-- [负向 N13] 删除唯一约束后重新规划, 后续写入的重复行必须被 DISTINCT 正确去重
CREATE TABLE t_ps_con(k int NOT NULL, v text, CONSTRAINT t_ps_con_u UNIQUE(k));
INSERT INTO t_ps_con VALUES (1, 'a'), (2, 'b');
PREPARE ps_con AS SELECT DISTINCT k FROM t_ps_con;
EXPLAIN (COSTS OFF) EXECUTE ps_con;
ALTER TABLE t_ps_con DROP CONSTRAINT t_ps_con_u;
INSERT INTO t_ps_con VALUES (1, 'dup'), (1, 'dup2');
EXPLAIN (COSTS OFF) EXECUTE ps_con;
EXECUTE ps_con;

-- ==================================================================
-- 补充回归五: 空表与单行表 (结果正确性只能来自元数据证明, 与行数无关)
-- ==================================================================

-- [正向 T9] 空表 + 主键: 按元数据消除 DISTINCT, 不依赖行数
CREATE TABLE t_empty(k int PRIMARY KEY, v text);
EXPLAIN (COSTS OFF) SELECT DISTINCT k, v FROM t_empty;
SELECT count(*) AS correct_count FROM (SELECT DISTINCT k, v FROM t_empty) s;

-- [负向 N14] 单行表无唯一键: 行数为 1 也不能消除 DISTINCT
CREATE TABLE t_single(k int, v text);
INSERT INTO t_single VALUES (1, 'only');
EXPLAIN (COSTS OFF) SELECT DISTINCT k, v FROM t_single;
SELECT count(*) AS correct_count FROM (SELECT DISTINCT k, v FROM t_single) s;

-- ==================================================================
-- 补充回归六: 多重集语义 (EXCEPT ALL 双向均无差异)
-- ==================================================================

CREATE TABLE t_multi(k int PRIMARY KEY, v text);
INSERT INTO t_multi VALUES (1, 'a'), (2, 'a'), (3, 'b'), (4, 'c'), (5, 'c');
SELECT count(*) AS diff_forward FROM (
    SELECT k, v FROM t_multi EXCEPT ALL SELECT DISTINCT k, v FROM t_multi
) s;
SELECT count(*) AS diff_backward FROM (
    SELECT DISTINCT k, v FROM t_multi EXCEPT ALL SELECT k, v FROM t_multi
) s;

-- [B库正向] B-format 主键覆盖 -> 应消除 DISTINCT
CREATE DATABASE distinct_opt_b DBCOMPATIBILITY 'B';
\c distinct_opt_b

CREATE TABLE t_b_pk(k int PRIMARY KEY, v text);
INSERT INTO t_b_pk SELECT g, concat('v', g) FROM generate_series(1, 5) g;
EXPLAIN (COSTS OFF) SELECT DISTINCT k, v FROM t_b_pk;
SELECT count(*) AS correct_count FROM (SELECT DISTINCT k, v FROM t_b_pk) s;

-- [B库负向] B-format 非主键唯一约束不能作为 DISTINCT 优化依据
CREATE TABLE t_b_unique(k int NOT NULL UNIQUE, v text);
INSERT INTO t_b_unique SELECT g, concat('v', g) FROM generate_series(1, 5) g;
EXPLAIN (COSTS OFF) SELECT DISTINCT k FROM t_b_unique;
SELECT count(*) AS correct_count FROM (SELECT DISTINCT k FROM t_b_unique) s;

-- 即使重新开启 unique_checks, 非主键唯一约束也可能已经包含重复键
SET unique_checks = off;
INSERT INTO t_b_unique VALUES (1, 'duplicate');
RESET unique_checks;
EXPLAIN (COSTS OFF) SELECT DISTINCT k FROM t_b_unique;
SELECT count(*) AS correct_count FROM (SELECT DISTINCT k FROM t_b_unique) s;

DROP TABLE t_b_pk;
DROP TABLE t_b_unique;

\c regression
DROP DATABASE distinct_opt_b;

DROP TABLE IF EXISTS t_pk CASCADE;
DROP TABLE IF EXISTS t_composite CASCADE;
DROP TABLE IF EXISTS t_unique_notnull CASCADE;
DROP TABLE IF EXISTS t_unique_nullable CASCADE;
DROP TABLE IF EXISTS t_partial CASCADE;
DROP TABLE IF EXISTS t_deferrable CASCADE;
DROP TABLE IF EXISTS t_join CASCADE;
DROP TABLE IF EXISTS t_expr CASCADE;
DROP TABLE IF EXISTS t_nokey CASCADE;
DROP TABLE IF EXISTS t_partition_local CASCADE;
DROP TABLE IF EXISTS t_partition_gpi CASCADE;
DROP TABLE IF EXISTS t_coll CASCADE;
DROP TABLE IF EXISTS t_multiidx CASCADE;
DROP TABLE IF EXISTS t_include CASCADE;
DROP TABLE IF EXISTS t_ps_idx CASCADE;
DROP TABLE IF EXISTS t_ps_nn CASCADE;
DROP TABLE IF EXISTS t_ps_con CASCADE;
DROP TABLE IF EXISTS t_empty CASCADE;
DROP TABLE IF EXISTS t_single CASCADE;
DROP TABLE IF EXISTS t_multi CASCADE;
DEALLOCATE ALL;
