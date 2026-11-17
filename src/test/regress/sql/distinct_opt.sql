-- ==================================================================
-- DISTINCT 冗余优化 测试用例
-- 验证 check_distinct_redundant_by_unique() 修复缺陷后的正确性
--
-- 验证目标:
--   正向场景: DISTINCT / DISTINCT ON 键覆盖唯一索引全部键列 → 去重应被消除
--   负向场景: 无法严格证明查询结果唯一 → DISTINCT 必须保留, 结果不能错
--
-- 测试同时检查 EXPLAIN 计划和查询结果。
-- 基础用例统计包含 NULL 的去重行数；部分索引用例输出排序后的完整结果。
-- ==================================================================

DROP DATABASE IF EXISTS distinct_opt_b;
DROP DATABASE IF EXISTS distinct_opt_c;
DROP DATABASE IF EXISTS distinct_opt_pg;
DROP DATABASE IF EXISTS distinct_opt_d;
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
DROP TABLE IF EXISTS t_gpi_unique CASCADE;
DROP TABLE IF EXISTS t_gpi_composite CASCADE;
DROP TABLE IF EXISTS t_gpi_null CASCADE;
DROP TABLE IF EXISTS t_gpi_unusable CASCADE;
DROP TABLE IF EXISTS t_gpi_update CASCADE;
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

-- Partial unique indexes prove uniqueness only inside their predicate.
CREATE SCHEMA distinct_partial;
SET search_path = distinct_partial;
CREATE TABLE t_status(k int NOT NULL, status int, payload text);
CREATE UNIQUE INDEX t_status_u ON t_status(k) WHERE status = 1;
INSERT INTO t_status VALUES (1,1,'a'), (2,1,'b'), (3,1,'c'),
    (1,0,'x'), (1,0,'x'), (2,0,'y'), (2,0,'y'), (4,0,'z');
CREATE TABLE t_soft(k int NOT NULL, deleted boolean, payload text);
CREATE UNIQUE INDEX t_soft_u ON t_soft(k) WHERE NOT deleted;
INSERT INTO t_soft VALUES (1,false,'a'), (2,false,'b'), (1,true,'x'), (1,true,'x');
CREATE TABLE t_range(k int NOT NULL);
CREATE UNIQUE INDEX t_range_u ON t_range(k) WHERE k > 0;
INSERT INTO t_range VALUES (-1), (-1), (0), (0), (1), (2), (3);
CREATE TABLE t_comp(a int NOT NULL, b int NOT NULL, status int, deleted boolean);
CREATE UNIQUE INDEX t_comp_u ON t_comp(a,b) WHERE status = 1 AND NOT deleted;
INSERT INTO t_comp VALUES (1,1,1,false), (1,2,1,false), (2,1,1,false),
    (1,1,0,false), (1,1,0,false), (1,1,1,true), (1,1,1,true);
CREATE TABLE t_null(k int, status int);
CREATE UNIQUE INDEX t_null_u ON t_null(k) WHERE status = 1;
INSERT INTO t_null VALUES (NULL,1), (NULL,1), (1,1), (2,1);
CREATE TABLE t_nonunique(k int NOT NULL, status int);
CREATE INDEX t_nonunique_i ON t_nonunique(k) WHERE status = 1;
INSERT INTO t_nonunique VALUES (1,1), (1,1), (2,1);
ANALYZE t_status;
ANALYZE t_soft;
ANALYZE t_range;
ANALYZE t_comp;
ANALYZE t_null;
ANALYZE t_nonunique;

-- Check proofs independently of selecting an index access path.
SET enable_indexscan = off;
SET enable_indexonlyscan = off;
SET enable_bitmapscan = off;

-- Matching status and additional output column
EXPLAIN (COSTS OFF) SELECT DISTINCT k, payload FROM t_status WHERE status = 1 ORDER BY k;
SELECT DISTINCT k, payload FROM t_status WHERE status = 1 ORDER BY k;

-- DISTINCT ON uses its own key, including a key not projected in the result.
EXPLAIN (COSTS OFF) SELECT DISTINCT ON (k) payload FROM t_status WHERE status = 1 ORDER BY k;
SELECT DISTINCT ON (k) payload FROM t_status WHERE status = 1 ORDER BY k;

-- Soft deletion predicate
EXPLAIN (COSTS OFF) SELECT DISTINCT k, payload FROM t_soft WHERE NOT deleted ORDER BY k;
SELECT DISTINCT k, payload FROM t_soft WHERE NOT deleted ORDER BY k;

-- Stronger range implies the index predicate
EXPLAIN (COSTS OFF) SELECT DISTINCT k FROM t_range WHERE k > 1 ORDER BY k;
SELECT DISTINCT k FROM t_range WHERE k > 1 ORDER BY k;

-- Both predicate terms and all composite key columns
EXPLAIN (COSTS OFF) SELECT DISTINCT a,b FROM t_comp WHERE status = 1 AND NOT deleted ORDER BY a,b;
SELECT DISTINCT a,b FROM t_comp WHERE status = 1 AND NOT deleted ORDER BY a,b;

-- No predicate: duplicates outside the index must collapse
EXPLAIN (COSTS OFF) SELECT DISTINCT k FROM t_status ORDER BY k;
SELECT DISTINCT k FROM t_status ORDER BY k;

-- Nonmatching status
EXPLAIN (COSTS OFF) SELECT DISTINCT k FROM t_status WHERE status = 0 ORDER BY k;
SELECT DISTINCT k FROM t_status WHERE status = 0 ORDER BY k;

-- Wider range
EXPLAIN (COSTS OFF) SELECT DISTINCT k FROM t_range WHERE k >= 0 ORDER BY k;
SELECT DISTINCT k FROM t_range WHERE k >= 0 ORDER BY k;

-- OR admits rows outside the index
EXPLAIN (COSTS OFF) SELECT DISTINCT k FROM t_status WHERE status = 1 OR k = 1 ORDER BY k;
SELECT DISTINCT k FROM t_status WHERE status = 1 OR k = 1 ORDER BY k;

-- Missing predicate conjunct
EXPLAIN (COSTS OFF) SELECT DISTINCT a,b FROM t_comp WHERE status = 1 ORDER BY a,b;
SELECT DISTINCT a,b FROM t_comp WHERE status = 1 ORDER BY a,b;

-- Missing composite key column
EXPLAIN (COSTS OFF) SELECT DISTINCT a FROM t_comp WHERE status = 1 AND NOT deleted ORDER BY a;
SELECT DISTINCT a FROM t_comp WHERE status = 1 AND NOT deleted ORDER BY a;

-- Nonunique partial index
EXPLAIN (COSTS OFF) SELECT DISTINCT k FROM t_nonunique WHERE status = 1 ORDER BY k;
SELECT DISTINCT k FROM t_nonunique WHERE status = 1 ORDER BY k;

-- Nullable key includes repeated NULLs
EXPLAIN (COSTS OFF) SELECT DISTINCT k FROM t_null WHERE status = 1 ORDER BY k NULLS LAST;
SELECT DISTINCT k FROM t_null WHERE status = 1 ORDER BY k NULLS LAST;

-- WHERE does not replace a catalog NOT NULL constraint
EXPLAIN (COSTS OFF) SELECT DISTINCT k FROM t_null WHERE status = 1 AND k IS NOT NULL ORDER BY k;
SELECT DISTINCT k FROM t_null WHERE status = 1 AND k IS NOT NULL ORDER BY k;

-- ORDER BY payload still needs sorting; LIMIT applies afterwards
EXPLAIN (COSTS OFF) SELECT DISTINCT k,payload FROM t_status WHERE status = 1 ORDER BY payload DESC,k LIMIT 2;
SELECT DISTINCT k,payload FROM t_status WHERE status = 1 ORDER BY payload DESC,k LIMIT 2;

RESET enable_indexscan;
RESET enable_indexonlyscan;
RESET enable_bitmapscan;
SET enable_seqscan = off;

-- Index access path also permits the proof
EXPLAIN (COSTS OFF) SELECT DISTINCT k FROM t_status WHERE status = 1 ORDER BY k;
SELECT DISTINCT k FROM t_status WHERE status = 1 ORDER BY k;

RESET enable_seqscan;
SET enable_indexscan = off;
SET enable_indexonlyscan = off;
SET enable_bitmapscan = off;

SET plan_cache_mode = force_generic_plan;
PREPARE partial_param(int) AS SELECT DISTINCT k FROM t_status WHERE status = $1 ORDER BY k;

EXPLAIN (COSTS OFF) EXECUTE partial_param(1);
EXECUTE partial_param(1);

EXPLAIN (COSTS OFF) EXECUTE partial_param(0);
EXECUTE partial_param(0);

EXECUTE partial_param(1);

EXECUTE partial_param(0);

DEALLOCATE partial_param;

SET plan_cache_mode = force_custom_plan;
PREPARE partial_param(int) AS SELECT DISTINCT k FROM t_status WHERE status = $1 ORDER BY k;

EXPLAIN (COSTS OFF) EXECUTE partial_param(1);
EXECUTE partial_param(1);

EXPLAIN (COSTS OFF) EXECUTE partial_param(0);
EXECUTE partial_param(0);

EXECUTE partial_param(1);

EXECUTE partial_param(0);

DEALLOCATE partial_param;

RESET plan_cache_mode;
-- Adaptive generic plans must not reuse parameter-specific uniqueness.
-- The chosen plan shape here depends on the adaptive plan manager
-- (enable_cachedplan_mgr, a POSTMASTER GUC), so it is asserted only in the
-- deterministic force_generic_plan / force_custom_plan cases above.  This case
-- keeps verifying that repeated executions still return correct results.
PREPARE partial_adaptive(int) AS SELECT /*+ choose_adaptive_gplan */ DISTINCT k
    FROM t_status WHERE status = $1 ORDER BY k;

EXECUTE partial_adaptive(1);

EXECUTE partial_adaptive(0);

EXECUTE partial_adaptive(1);

EXECUTE partial_adaptive(0);

EXECUTE partial_adaptive(1);

EXECUTE partial_adaptive(0);

EXECUTE partial_adaptive(1);

EXECUTE partial_adaptive(0);

EXECUTE partial_adaptive(1);

EXECUTE partial_adaptive(0);

EXECUTE partial_adaptive(1);

EXECUTE partial_adaptive(0);

DEALLOCATE partial_adaptive;
SET plan_cache_mode = force_generic_plan;
-- A fixed predicate remains a proof even with another unresolved parameter.
PREPARE partial_fixed(int) AS SELECT DISTINCT k FROM t_status
    WHERE status = 1 AND k >= $1 ORDER BY k;
EXPLAIN (COSTS OFF) EXECUTE partial_fixed(1);
EXECUTE partial_fixed(1);
EXPLAIN (COSTS OFF) EXECUTE partial_fixed(2);
EXECUTE partial_fixed(2);
-- DDL invalidates a cached proof even when its plan uses a sequential scan.
DROP INDEX t_status_u;
INSERT INTO t_status VALUES (1,1,'a');
EXPLAIN (COSTS OFF) EXECUTE partial_fixed(1);
EXECUTE partial_fixed(1);
DEALLOCATE partial_fixed;
RESET plan_cache_mode;
RESET enable_indexscan;
RESET enable_indexonlyscan;
RESET enable_bitmapscan;
DROP TABLE t_status, t_soft, t_range, t_comp, t_null, t_nonunique;
RESET search_path;
DROP SCHEMA distinct_partial;

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

-- [正向 T10] 分区表 GPI 主键覆盖 → 应消除 DISTINCT
CREATE TABLE t_partition_gpi(k int NOT NULL, p int, v text)
PARTITION BY RANGE(p)(
    PARTITION p1 VALUES LESS THAN (10),
    PARTITION p2 VALUES LESS THAN (MAXVALUE)
);
CREATE UNIQUE INDEX t_partition_gpi_u ON t_partition_gpi(k) GLOBAL;
ALTER TABLE t_partition_gpi ADD CONSTRAINT t_partition_gpi_pkey PRIMARY KEY USING INDEX t_partition_gpi_u;
-- 跨分区重复键必须被 GPI 拒绝, 说明全局唯一性由索引实时强制
INSERT INTO t_partition_gpi VALUES (1, 1);
INSERT INTO t_partition_gpi VALUES (1, 15);
INSERT INTO t_partition_gpi SELECT g, g % 20, concat('g', g) FROM generate_series(2, 20) g;
EXPLAIN (COSTS OFF) SELECT DISTINCT k, v FROM t_partition_gpi;
SELECT count(*) AS correct_count FROM (SELECT DISTINCT k, v FROM t_partition_gpi) s;

-- A globally unique ON key may be hidden from the output.
EXPLAIN (COSTS OFF) SELECT DISTINCT ON (k) v FROM t_partition_gpi WHERE k >= 18 ORDER BY k;
SELECT DISTINCT ON (k) v FROM t_partition_gpi WHERE k >= 18 ORDER BY k;

-- [正向 T11] A-format 非主键唯一 GPI 覆盖 → 应消除 DISTINCT
CREATE TABLE t_gpi_unique(k int NOT NULL, p int, v text)
PARTITION BY RANGE(p)(
    PARTITION p1 VALUES LESS THAN (10),
    PARTITION p2 VALUES LESS THAN (MAXVALUE)
);
CREATE UNIQUE INDEX t_gpi_unique_u ON t_gpi_unique(k) GLOBAL;
INSERT INTO t_gpi_unique SELECT g, g % 20, concat('u', g) FROM generate_series(1, 20) g;
EXPLAIN (COSTS OFF) SELECT DISTINCT k FROM t_gpi_unique;
SELECT count(*) AS correct_count FROM (SELECT DISTINCT k FROM t_gpi_unique) s;

-- [正向 T12] 复合 GPI 全部键列覆盖 → 应消除 DISTINCT
CREATE TABLE t_gpi_composite(a int NOT NULL, b int NOT NULL, p int)
PARTITION BY RANGE(p)(
    PARTITION p1 VALUES LESS THAN (10),
    PARTITION p2 VALUES LESS THAN (MAXVALUE)
);
CREATE UNIQUE INDEX t_gpi_composite_u ON t_gpi_composite(a, b) GLOBAL;
INSERT INTO t_gpi_composite SELECT g % 3, g, g % 20 FROM generate_series(1, 20) g;
EXPLAIN (COSTS OFF) SELECT DISTINCT a, b FROM t_gpi_composite;
SELECT count(*) AS correct_count FROM (SELECT DISTINCT a, b FROM t_gpi_composite) s;

-- [正向 T13] 分区裁剪后 GPI 仍提供证明 → 应消除 DISTINCT
-- Check both single-partition plan shapes, independent of the test configuration.
SET partition_iterator_elimination = off;
EXPLAIN (COSTS OFF) SELECT DISTINCT k, v FROM t_partition_gpi WHERE p = 15;
SELECT count(*) AS correct_count FROM (
    SELECT DISTINCT k, v FROM t_partition_gpi WHERE p = 15) s;
SET partition_iterator_elimination = on;
EXPLAIN (COSTS OFF) SELECT DISTINCT k, v FROM t_partition_gpi WHERE p = 15;
SELECT count(*) AS correct_count FROM (
    SELECT DISTINCT k, v FROM t_partition_gpi WHERE p = 15) s;
RESET partition_iterator_elimination;

-- [负向 N15] 复合 GPI 只覆盖部分键列 → 应保留 DISTINCT
EXPLAIN (COSTS OFF) SELECT DISTINCT a FROM t_gpi_composite;
SELECT count(*) AS correct_count FROM (SELECT DISTINCT a FROM t_gpi_composite) s;

-- [负向 N16] 可空唯一 GPI 键 (包含重复 NULL) → 应保留 DISTINCT
CREATE TABLE t_gpi_null(k int, p int)
PARTITION BY RANGE(p)(
    PARTITION p1 VALUES LESS THAN (10),
    PARTITION p2 VALUES LESS THAN (MAXVALUE)
);
CREATE UNIQUE INDEX t_gpi_null_u ON t_gpi_null(k) GLOBAL;
INSERT INTO t_gpi_null VALUES (NULL, 1), (NULL, 15), (1, 1), (2, 15);
EXPLAIN (COSTS OFF) SELECT DISTINCT k FROM t_gpi_null;
SELECT count(*) AS correct_count FROM (SELECT DISTINCT k FROM t_gpi_null) s;

-- [负向 N17] 分区 DDL 后未 UPDATE GLOBAL INDEX 的不可用 GPI → 应保留 DISTINCT
CREATE TABLE t_gpi_unusable(k int NOT NULL, p int, v text)
PARTITION BY RANGE(p)(
    PARTITION p1 VALUES LESS THAN (10),
    PARTITION p2 VALUES LESS THAN (20),
    PARTITION p3 VALUES LESS THAN (MAXVALUE)
);
CREATE UNIQUE INDEX t_gpi_unusable_u ON t_gpi_unusable(k) GLOBAL;
ALTER TABLE t_gpi_unusable ADD CONSTRAINT t_gpi_unusable_pkey PRIMARY KEY USING INDEX t_gpi_unusable_u;
INSERT INTO t_gpi_unusable SELECT g, g % 30, concat('x', g) FROM generate_series(1, 30) g;
EXPLAIN (COSTS OFF) SELECT DISTINCT k, v FROM t_gpi_unusable;
-- Cache a proof without an index access path, then invalidate the GPI.
SET enable_indexscan = off;
SET enable_indexonlyscan = off;
SET enable_bitmapscan = off;
SET plan_cache_mode = force_generic_plan;
PREPARE ps_gpi(int) AS
    SELECT DISTINCT k FROM t_gpi_unusable WHERE k >= $1 ORDER BY k;
EXPLAIN (COSTS OFF) EXECUTE ps_gpi(28);
EXECUTE ps_gpi(28);
ALTER TABLE t_gpi_unusable DROP PARTITION p1;
-- k = 28 survives in p3; the unusable GPI no longer enforces its uniqueness.
INSERT INTO t_gpi_unusable VALUES (28, 25, 'x28');
SELECT indisusable FROM pg_index WHERE indrelid = 't_gpi_unusable'::regclass AND indisunique;
SELECT count(*) AS physical_count FROM t_gpi_unusable WHERE k >= 28;
EXPLAIN (COSTS OFF) EXECUTE ps_gpi(28);
EXECUTE ps_gpi(28);
DEALLOCATE ps_gpi;
RESET plan_cache_mode;
RESET enable_indexscan;
RESET enable_indexonlyscan;
RESET enable_bitmapscan;
EXPLAIN (COSTS OFF) SELECT DISTINCT k, v FROM t_gpi_unusable;
SELECT count(*) AS correct_count FROM (SELECT DISTINCT k, v FROM t_gpi_unusable) s;

-- 带 UPDATE GLOBAL INDEX 的分区 DDL 会重建索引, GPI 保持可用 → 应消除 DISTINCT
CREATE TABLE t_gpi_update(k int NOT NULL, p int, v text)
PARTITION BY RANGE(p)(
    PARTITION p1 VALUES LESS THAN (10),
    PARTITION p2 VALUES LESS THAN (20),
    PARTITION p3 VALUES LESS THAN (MAXVALUE)
);
CREATE UNIQUE INDEX t_gpi_update_u ON t_gpi_update(k) GLOBAL;
ALTER TABLE t_gpi_update ADD CONSTRAINT t_gpi_update_pkey PRIMARY KEY USING INDEX t_gpi_update_u;
INSERT INTO t_gpi_update SELECT g, g % 30, concat('y', g) FROM generate_series(1, 30) g;
ALTER TABLE t_gpi_update DROP PARTITION p1 UPDATE GLOBAL INDEX;
EXPLAIN (COSTS OFF) SELECT DISTINCT k, v FROM t_gpi_update;
SELECT count(*) AS correct_count FROM (SELECT DISTINCT k, v FROM t_gpi_update) s;

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

-- ==================================================================
-- DISTINCT ON: prove uniqueness of the ON key, not of the SELECT list.
-- ==================================================================

-- Hidden primary key: duplicate projected payloads must all survive.
EXPLAIN (COSTS OFF) SELECT DISTINCT ON (k) v FROM t_multi ORDER BY k;
SELECT DISTINCT ON (k) v FROM t_multi ORDER BY k;

-- Eliminating Unique must preserve the final order and LIMIT/OFFSET.
EXPLAIN (COSTS OFF) SELECT DISTINCT ON (k) v FROM t_multi ORDER BY k DESC LIMIT 2 OFFSET 1;
SELECT DISTINCT ON (k) v FROM t_multi ORDER BY k DESC LIMIT 2 OFFSET 1;

-- A primary key in SELECT cannot prove uniqueness of a different ON key.
EXPLAIN (COSTS OFF) SELECT DISTINCT ON (v) k, v FROM t_multi ORDER BY v, k DESC;
SELECT DISTINCT ON (v) k, v FROM t_multi ORDER BY v, k DESC;

-- All composite ON keys are required, even when the output includes both.
EXPLAIN (COSTS OFF) SELECT DISTINCT ON (a, b) c FROM t_composite ORDER BY a, b;
SELECT DISTINCT ON (a, b) c FROM t_composite ORDER BY a, b;
EXPLAIN (COSTS OFF) SELECT DISTINCT ON (a) a, b, c FROM t_composite ORDER BY a, b DESC;
SELECT DISTINCT ON (a) a, b, c FROM t_composite ORDER BY a, b DESC;

-- A target SRF can multiply a unique base row before DISTINCT ON.
EXPLAIN (COSTS OFF) SELECT DISTINCT ON (k) k, generate_series(1,2) AS g
    FROM t_multi WHERE k <= 2 ORDER BY k, g DESC;
SELECT DISTINCT ON (k) k, generate_series(1,2) AS g
    FROM t_multi WHERE k <= 2 ORDER BY k, g DESC;

-- Nullable keys and non-injective expressions still need deduplication.
EXPLAIN (COSTS OFF) SELECT DISTINCT ON (k) k FROM t_unique_nullable ORDER BY k NULLS LAST;
SELECT DISTINCT ON (k) k FROM t_unique_nullable ORDER BY k NULLS LAST;
EXPLAIN (COSTS OFF) SELECT DISTINCT ON (k % 2) k, v FROM t_multi ORDER BY k % 2, k DESC;
SELECT DISTINCT ON (k % 2) k, v FROM t_multi ORDER BY k % 2, k DESC;

-- [B库正向] B-format 主键覆盖 -> 应消除 DISTINCT
CREATE DATABASE distinct_opt_b DBCOMPATIBILITY 'B';
\c distinct_opt_b

CREATE TABLE t_b_pk(k int PRIMARY KEY, v text);
INSERT INTO t_b_pk SELECT g, concat('v', g) FROM generate_series(1, 5) g;
EXPLAIN (COSTS OFF) SELECT DISTINCT k, v FROM t_b_pk;
SELECT count(*) AS correct_count FROM (SELECT DISTINCT k, v FROM t_b_pk) s;

-- B-format primary keys also prove uniqueness of hidden ON keys.
EXPLAIN (COSTS OFF) SELECT DISTINCT ON (k) v FROM t_b_pk ORDER BY k;
SELECT DISTINCT ON (k) v FROM t_b_pk ORDER BY k;

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

-- Re-enabling unique_checks does not repair old duplicates for DISTINCT ON.
EXPLAIN (COSTS OFF) SELECT DISTINCT ON (k) v FROM t_b_unique ORDER BY k, v;
SELECT DISTINCT ON (k) v FROM t_b_unique ORDER BY k, v;

-- B-format partial unique indexes are not a uniqueness proof.
CREATE TABLE t_b_partial(k int NOT NULL, status int);
CREATE UNIQUE INDEX t_b_partial_u ON t_b_partial(k) WHERE status = 1;
INSERT INTO t_b_partial VALUES (1,1), (2,1), (1,0), (1,0);
EXPLAIN (COSTS OFF) SELECT DISTINCT k FROM t_b_partial WHERE status = 1 ORDER BY k;
SELECT DISTINCT k FROM t_b_partial WHERE status = 1 ORDER BY k;
SET unique_checks = off;
INSERT INTO t_b_partial VALUES (1,1);
RESET unique_checks;
EXPLAIN (COSTS OFF) SELECT DISTINCT k FROM t_b_partial WHERE status = 1 ORDER BY k;
SELECT DISTINCT k FROM t_b_partial WHERE status = 1 ORDER BY k;
DROP TABLE t_b_partial;

-- [B库负向] B-format 非主键唯一 GPI 不能作为 DISTINCT 优化依据
CREATE TABLE t_b_gpi(k int NOT NULL, p int, v text)
PARTITION BY RANGE(p)(
    PARTITION p1 VALUES LESS THAN (10),
    PARTITION p2 VALUES LESS THAN (MAXVALUE)
);
CREATE UNIQUE INDEX t_b_gpi_u ON t_b_gpi(k) GLOBAL;
INSERT INTO t_b_gpi SELECT g, g % 20, concat('v', g) FROM generate_series(1, 5) g;
SET unique_checks = off;
INSERT INTO t_b_gpi VALUES (1, 15);
RESET unique_checks;
EXPLAIN (COSTS OFF) SELECT DISTINCT k FROM t_b_gpi;
SELECT count(*) AS correct_count FROM (SELECT DISTINCT k FROM t_b_gpi) s;
DROP TABLE t_b_gpi;

-- [B库正向] B-format GPI 主键覆盖 -> 应消除 DISTINCT
CREATE TABLE t_b_gpi_pk(k int NOT NULL, p int, v text)
PARTITION BY RANGE(p)(
    PARTITION p1 VALUES LESS THAN (10),
    PARTITION p2 VALUES LESS THAN (MAXVALUE)
);
CREATE UNIQUE INDEX t_b_gpi_pk_u ON t_b_gpi_pk(k) GLOBAL;
ALTER TABLE t_b_gpi_pk ADD CONSTRAINT t_b_gpi_pk_pkey PRIMARY KEY USING INDEX t_b_gpi_pk_u;
INSERT INTO t_b_gpi_pk SELECT g, g % 20, concat('v', g) FROM generate_series(1, 20) g;
EXPLAIN (COSTS OFF) SELECT DISTINCT k, v FROM t_b_gpi_pk;
SELECT count(*) AS correct_count FROM (SELECT DISTINCT k, v FROM t_b_gpi_pk) s;
DROP TABLE t_b_gpi_pk;

DROP TABLE t_b_pk;
DROP TABLE t_b_unique;

\c regression
DROP DATABASE distinct_opt_b;

-- Other compatibility modes share the same uniqueness proof.
CREATE DATABASE distinct_opt_c DBCOMPATIBILITY 'C';
\c distinct_opt_c
CREATE TABLE t_mode(k int PRIMARY KEY, u int NOT NULL UNIQUE, g int, v text);
INSERT INTO t_mode VALUES (1,11,1,'same'), (2,12,1,'same'), (3,13,2,'other'), (4,14,2,'other');
EXPLAIN (COSTS OFF) SELECT DISTINCT u FROM t_mode ORDER BY u;
SELECT DISTINCT u FROM t_mode ORDER BY u;
EXPLAIN (COSTS OFF) SELECT DISTINCT ON (k) v FROM t_mode ORDER BY k DESC LIMIT 2 OFFSET 1;
SELECT DISTINCT ON (k) v FROM t_mode ORDER BY k DESC LIMIT 2 OFFSET 1;
EXPLAIN (COSTS OFF) SELECT DISTINCT ON (g) k, v FROM t_mode ORDER BY g, k DESC;
SELECT DISTINCT ON (g) k, v FROM t_mode ORDER BY g, k DESC;
DROP TABLE t_mode;

CREATE DATABASE distinct_opt_pg DBCOMPATIBILITY 'PG';
\c distinct_opt_pg
CREATE TABLE t_mode(k int PRIMARY KEY, u int NOT NULL UNIQUE, g int, v text);
INSERT INTO t_mode VALUES (1,11,1,'same'), (2,12,1,'same'), (3,13,2,'other'), (4,14,2,'other');
EXPLAIN (COSTS OFF) SELECT DISTINCT u FROM t_mode ORDER BY u;
SELECT DISTINCT u FROM t_mode ORDER BY u;
EXPLAIN (COSTS OFF) SELECT DISTINCT ON (k) v FROM t_mode ORDER BY k DESC LIMIT 2 OFFSET 1;
SELECT DISTINCT ON (k) v FROM t_mode ORDER BY k DESC LIMIT 2 OFFSET 1;
EXPLAIN (COSTS OFF) SELECT DISTINCT ON (g) k, v FROM t_mode ORDER BY g, k DESC;
SELECT DISTINCT ON (g) k, v FROM t_mode ORDER BY g, k DESC;
DROP TABLE t_mode;

CREATE DATABASE distinct_opt_d DBCOMPATIBILITY 'D';
\c distinct_opt_d
CREATE TABLE t_mode(k int PRIMARY KEY, u int NOT NULL UNIQUE, g int, v text);
INSERT INTO t_mode VALUES (1,11,1,'same'), (2,12,1,'same'), (3,13,2,'other'), (4,14,2,'other');
EXPLAIN (COSTS OFF) SELECT DISTINCT u FROM t_mode ORDER BY u;
SELECT DISTINCT u FROM t_mode ORDER BY u;
EXPLAIN (COSTS OFF) SELECT DISTINCT ON (k) v FROM t_mode ORDER BY k DESC LIMIT 2 OFFSET 1;
SELECT DISTINCT ON (k) v FROM t_mode ORDER BY k DESC LIMIT 2 OFFSET 1;
EXPLAIN (COSTS OFF) SELECT DISTINCT ON (g) k, v FROM t_mode ORDER BY g, k DESC;
SELECT DISTINCT ON (g) k, v FROM t_mode ORDER BY g, k DESC;
DROP TABLE t_mode;

\c regression
DROP DATABASE distinct_opt_c;
DROP DATABASE distinct_opt_pg;
DROP DATABASE distinct_opt_d;

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
DROP TABLE IF EXISTS t_gpi_unique CASCADE;
DROP TABLE IF EXISTS t_gpi_composite CASCADE;
DROP TABLE IF EXISTS t_gpi_null CASCADE;
DROP TABLE IF EXISTS t_gpi_unusable CASCADE;
DROP TABLE IF EXISTS t_gpi_update CASCADE;
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
