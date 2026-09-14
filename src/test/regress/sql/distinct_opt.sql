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
