-- ============================================================================
-- openGauss UNION ALL DiskANN Top-K WHERE 下推完整测试用例
-- ============================================================================

-- 清理环境
DROP VIEW IF EXISTS v CASCADE;
DROP TABLE IF EXISTS t1 CASCADE;
DROP TABLE IF EXISTS t2 CASCADE;

-- 创建测试表
CREATE TABLE t1 (
    id BIGINT PRIMARY KEY,
    val VECTOR(4) NOT NULL,
    title TEXT
);

CREATE TABLE t2 (
    id BIGINT PRIMARY KEY,
    val VECTOR(4) NOT NULL,
    title TEXT
);

-- 插入测试数据
INSERT INTO t1 (id, val, title) VALUES
    (1, '[0.1,0.2,0.3,0.4]', 'doc_1'),
    (2, '[0.5,0.6,0.7,0.8]', 'doc_2');

INSERT INTO t2 (id, val, title) VALUES
    (11, '[0.15,0.25,0.35,0.45]', 'doc_11'),
    (12, '[0.55,0.65,0.75,0.85]', 'doc_12');

-- 创建 DiskANN 索引
CREATE INDEX t1_val_diskann ON t1 USING diskann (val vector_l2_ops);
CREATE INDEX t2_val_diskann ON t2 USING diskann (val vector_l2_ops);

-- 分析表
ANALYZE t1;
ANALYZE t2;

-- 创建 UNION ALL 视图
CREATE VIEW v AS
    SELECT * FROM t1
    UNION ALL
    SELECT * FROM t2;

-- ============================================================================
-- 测试 1: 无 WHERE 子句的 Top-K 查询
-- ============================================================================
\echo ''
\echo '=== 测试 1: 无 WHERE 子句 ==='
\echo '预期结果: LIMIT 下推到每个叶子，使用 Ann Index Scan'

SET enable_union_all_diskann_topk_pushdown = on;
SET enable_seqscan = off;

-- 查看执行计划
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM v
ORDER BY val <-> '[0.1,0.2,0.3,0.4]'::vector
LIMIT 2;

-- 执行查询
\echo ''
\echo '实际查询结果:'
SELECT id, title, val 
FROM v
ORDER BY val <-> '[0.1,0.2,0.3,0.4]'::vector
LIMIT 2;

-- ============================================================================
-- 测试 2: 带 WHERE 子句的 Top-K 查询
-- ============================================================================
\echo ''
\echo '=== 测试 2: 带 WHERE 子句 ==='
\echo '预期结果: WHERE 和 LIMIT 都下推到每个叶子'

-- 查看执行计划
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM v
WHERE title LIKE 'doc_%'
ORDER BY val <-> '[0.1,0.2,0.3,0.4]'::vector
LIMIT 2;

-- 执行查询
\echo ''
\echo '实际查询结果:'
SELECT id, title, val
FROM v
WHERE title LIKE 'doc_%'
ORDER BY val <-> '[0.1,0.2,0.3,0.4]'::vector
LIMIT 2;

-- ============================================================================
-- 测试 3: 带过滤条件的 WHERE 子句
-- ============================================================================
\echo ''
\echo '=== 测试 3: 带过滤条件的 WHERE 子句 ==='
\echo '预期结果: WHERE 过滤后再执行 Top-K'

-- 查看执行计划
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM v
WHERE id > 1
ORDER BY val <-> '[0.1,0.2,0.3,0.4]'::vector
LIMIT 2;

-- 执行查询
\echo ''
\echo '实际查询结果:'
SELECT id, title, val
FROM v
WHERE id > 1
ORDER BY val <-> '[0.1,0.2,0.3,0.4]'::vector
LIMIT 2;

-- ============================================================================
-- 测试 4: 复杂 WHERE 条件
-- ============================================================================
\echo ''
\echo '=== 测试 4: 复杂 WHERE 条件 ==='
\echo '预期结果: 多个条件都下推'

-- 查看执行计划
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM v
WHERE title LIKE 'doc_%' AND id < 20
ORDER BY val <-> '[0.1,0.2,0.3,0.4]'::vector
LIMIT 2;

-- 执行查询
\echo ''
\echo '实际查询结果:'
SELECT id, title, val
FROM v
WHERE title LIKE 'doc_%' AND id < 20
ORDER BY val <-> '[0.1,0.2,0.3,0.4]'::vector
LIMIT 2;

-- ============================================================================
-- 测试 5: 关闭优化对比
-- ============================================================================
\echo ''
\echo '=== 测试 5: 关闭优化（对比）==='
\echo '预期结果: 不应该有叶子级别的 LIMIT'

SET enable_union_all_diskann_topk_pushdown = off;

-- 查看执行计划
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM v
WHERE title LIKE 'doc_%'
ORDER BY val <-> '[0.1,0.2,0.3,0.4]'::vector
LIMIT 2;

-- 执行查询（验证结果一致性）
\echo ''
\echo '实际查询结果（应该与测试2相同）:'
SELECT id, title, val
FROM v
WHERE title LIKE 'doc_%'
ORDER BY val <-> '[0.1,0.2,0.3,0.4]'::vector
LIMIT 2;

-- ============================================================================
-- 测试 6: 不同的 LIMIT 表达式形式
-- ============================================================================
\echo ''
\echo '=== 测试 6: 不同的 LIMIT 表达式形式 ==='

SET enable_union_all_diskann_topk_pushdown = on;

-- 6.1 普通整数 LIMIT
\echo ''
\echo '6.1 普通整数 LIMIT 2:'
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM v ORDER BY val <-> '[0.1,0.2,0.3,0.4]'::vector LIMIT 2;

-- 6.2 显式 bigint LIMIT
\echo ''
\echo '6.2 显式 bigint LIMIT 2::bigint:'
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM v ORDER BY val <-> '[0.1,0.2,0.3,0.4]'::vector LIMIT 2::bigint;

-- 6.3 较大的 LIMIT
\echo ''
\echo '6.3 较大的 LIMIT 10:'
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM v ORDER BY val <-> '[0.1,0.2,0.3,0.4]'::vector LIMIT 10;

-- ============================================================================
-- 测试 7: 验证 LIMIT 下推的效果
-- ============================================================================
\echo ''
\echo '=== 测试 7: 使用 EXPLAIN ANALYZE 验证实际执行 ==='

-- 带 LIMIT 下推
\echo ''
\echo '7.1 优化开启（有 LIMIT 下推）:'
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF)
SELECT * FROM v
WHERE title LIKE 'doc_%'
ORDER BY val <-> '[0.1,0.2,0.3,0.4]'::vector
LIMIT 2;

-- 不带 LIMIT 下推
\echo ''
\echo '7.2 优化关闭（无 LIMIT 下推）:'
SET enable_union_all_diskann_topk_pushdown = off;
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF)
SELECT * FROM v
WHERE title LIKE 'doc_%'
ORDER BY val <-> '[0.1,0.2,0.3,0.4]'::vector
LIMIT 2;

-- ============================================================================
-- 测试总结
-- ============================================================================
\echo ''
\echo '============================================================================'
\echo '测试完成！'
\echo ''
\echo '验证要点:'
\echo '1. 测试1-4 应该在每个叶子看到 Limit 节点'
\echo '2. 测试2-4 应该在每个叶子看到 Filter 节点'
\echo '3. 测试5 不应该有叶子级别的 Limit'
\echo '4. 所有测试的查询结果应该一致'
\echo '5. 测试7 应该显示优化后扫描的行数更少'
\echo '============================================================================'
