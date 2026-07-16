set enable_expr_fusion=on;
DROP SCHEMA IF EXISTS index_ustore_smp CASCADE;
CREATE SCHEMA index_ustore_smp;
SET CURRENT_SCHEMA TO index_ustore_smp;
 
DROP TABLE IF EXISTS t1;
CREATE TABLE t1 (a INT, b INT, c INT) WITH (STORAGE_TYPE = USTORE);
CREATE INDEX i_t1 on t1(b);

--forward scan
SET QUERY_DOP = 1004;
TRUNCATE t1;
INSERT INTO t1 VALUES(GENERATE_SERIES(1,600), GENERATE_SERIES(1,600), GENERATE_SERIES(1,600));
ANALYZE t1;
EXPLAIN (COSTS OFF) SELECT /*+indexonlyscan(t1 i_t1)*/ COUNT(*) FROM t1;
SELECT /*+indexonlyscan(t1 i_t1)*/ COUNT(*) FROM t1;
EXPLAIN (COSTS OFF) SELECT /*+indexonlyscan(t1 i_t1)*/ COUNT(*) FROM t1 WHERE b < 200;
SELECT /*+indexonlyscan(t1 i_t1)*/ COUNT(*) FROM t1 WHERE b < 200;
EXPLAIN (COSTS OFF) SELECT /*+indexscan(t1 i_t1)*/ COUNT(a) FROM t1 WHERE b < 200;
SELECT /*+indexscan(t1 i_t1)*/ COUNT(a) FROM t1 WHERE b < 200;

-- backward scan
SET QUERY_DOP = 1004;
TRUNCATE t1;
INSERT INTO t1 VALUES(GENERATE_SERIES(1,600), GENERATE_SERIES(1,600), GENERATE_SERIES(1,600));
ANALYZE t1;
EXPLAIN (COSTS OFF) SELECT COUNT(*) FROM (SELECT /*+indexonlyscan(t1 i_t1)*/ b FROM t1 ORDER BY b DESC);
SELECT COUNT(*) FROM (SELECT /*+indexonlyscan(t1 i_t1)*/ b FROM t1 ORDER BY b DESC);
EXPLAIN (COSTS OFF) SELECT COUNT(*) FROM (SELECT /*+indexonlyscan(t1 i_t1)*/ b FROM t1 WHERE b < 200 ORDER BY b DESC);
SELECT COUNT(*) FROM (SELECT /*+indexonlyscan(t1 i_t1)*/ b FROM t1 WHERE b < 200 ORDER BY b DESC);
EXPLAIN (COSTS OFF) SELECT COUNT(*) FROM (SELECT /*+indexscan(t1 i_t1)*/ a FROM t1 WHERE b < 200 ORDER BY b DESC);
SELECT COUNT(*) FROM (SELECT /*+indexscan(t1 i_t1)*/ a FROM t1 WHERE b < 200 ORDER BY b DESC);

--partiton table
SET QUERY_DOP = 1002;
DROP TABLE IF EXISTS t1;
CREATE TABLE t1 (a INT, b INT, c INT) 
WITH (STORAGE_TYPE = USTORE)
partition by range (b)
(
partition p1 values less than(150),
partition p2 values less than(300),
partition p3 values less than(450),
partition p4 values less than (maxvalue)
);
CREATE INDEX i_t1 on t1(b) local;
INSERT INTO t1 VALUES(GENERATE_SERIES(1,600), GENERATE_SERIES(1,600), GENERATE_SERIES(1,600));
ANALYZE t1;
EXPLAIN (COSTS OFF) SELECT COUNT(*) FROM (SELECT /*+indexonlyscan(t1 i_t1)*/ b FROM t1 ORDER BY b DESC);
SELECT COUNT(*) FROM (SELECT /*+indexonlyscan(t1 i_t1)*/ b FROM t1 ORDER BY b DESC);
EXPLAIN (COSTS OFF) SELECT COUNT(*) FROM (SELECT /*+indexonlyscan(t1 i_t1)*/ b FROM t1 ORDER BY b ASC);
SELECT COUNT(*) FROM (SELECT /*+indexonlyscan(t1 i_t1)*/ b FROM t1 ORDER BY b ASC);

-- multi plan node 
TRUNCATE t1;
INSERT INTO t1 VALUES(GENERATE_SERIES(1,200), GENERATE_SERIES(1,200), GENERATE_SERIES(1,200));
EXPLAIN (COSTS OFF) SELECT count(*) FROM t1 AS t00, t1 AS t01 where t00.b < 200 AND t00.b > 100 AND t01.b < 190;
SELECT count(*) FROM t1 AS t00, t1 AS t01 where t00.b < 200 AND t00.b > 100 AND t01.b < 190;

RESET ENABLE_SEQSCAN;
RESET QUERY_DOP;
DROP TABLE index_ustore_smp.t1;
SET CURRENT_SCHEMA=PUBLIC;
DROP SCHEMA index_ustore_smp CASCADE;

set enable_expr_fusion=off;