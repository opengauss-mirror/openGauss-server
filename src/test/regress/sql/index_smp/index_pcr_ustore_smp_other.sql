
DROP SCHEMA IF EXISTS index_pcr_ustore_smp_other CASCADE;
CREATE SCHEMA index_pcr_ustore_smp_other;
SET CURRENT_SCHEMA TO index_pcr_ustore_smp_other;
set enable_mergejoin = false;
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
CREATE INDEX i_t1 on t1(b) local with (index_type=pcr);
-- (1) some partition tables are empty.
INSERT INTO t1 VALUES(GENERATE_SERIES(1,600), GENERATE_SERIES(1,600), GENERATE_SERIES(1,600));
ANALYZE t1;
EXPLAIN (COSTS OFF) SELECT COUNT(*) FROM (SELECT /*+indexonlyscan(t1 i_t1)*/ b FROM t1 ORDER BY b DESC);
SELECT COUNT(*) FROM (SELECT /*+indexonlyscan(t1 i_t1)*/ b FROM t1 ORDER BY b DESC);
EXPLAIN (COSTS OFF) SELECT COUNT(*) FROM (SELECT /*+indexonlyscan(t1 i_t1)*/ b FROM t1 ORDER BY b ASC);
SELECT COUNT(*) FROM (SELECT /*+indexonlyscan(t1 i_t1)*/ b FROM t1 ORDER BY b ASC);

EXPLAIN (COSTS OFF) SELECT COUNT(*) FROM (SELECT /*+indexonlyscan(t1 i_t1)*/ b FROM t1 WHERE b < 200 ORDER BY b DESC);
SELECT COUNT(*) FROM (SELECT /*+indexonlyscan(t1 i_t1)*/ b FROM t1 WHERE b < 200 ORDER BY b DESC);
EXPLAIN (COSTS OFF) SELECT COUNT(*) FROM (SELECT /*+indexonlyscan(t1 i_t1)*/ b FROM t1 WHERE b < 200 ORDER BY b ASC);
SELECT COUNT(*) FROM (SELECT /*+indexonlyscan(t1 i_t1)*/ b FROM t1 WHERE b < 200 ORDER BY b ASC);

EXPLAIN (COSTS OFF) SELECT COUNT(*) FROM (SELECT /*+indexscan(t1 i_t1)*/ a FROM t1 WHERE b < 200 ORDER BY b DESC);
SELECT COUNT(*) FROM (SELECT /*+indexscan(t1 i_t1)*/ a FROM t1 WHERE b < 200 ORDER BY b DESC);
EXPLAIN (COSTS OFF) SELECT COUNT(*) FROM (SELECT /*+indexscan(t1 i_t1)*/ a FROM t1 WHERE b < 200 ORDER BY b ASC);
SELECT COUNT(*) FROM (SELECT /*+indexscan(t1 i_t1)*/ a FROM t1 WHERE b < 200 ORDER BY b ASC);
-- (2) all partition tables are not empty.
TRUNCATE t1;
INSERT INTO t1 VALUES(GENERATE_SERIES(1,1200), GENERATE_SERIES(1,1200), GENERATE_SERIES(1,1200));
ANALYZE t1;
EXPLAIN (COSTS OFF) SELECT COUNT(*) FROM (SELECT /*+indexonlyscan(t1 i_t1)*/ b FROM t1 ORDER BY b DESC);
SELECT COUNT(*) FROM (SELECT /*+indexonlyscan(t1 i_t1)*/ b FROM t1 ORDER BY b DESC);
EXPLAIN (COSTS OFF) SELECT COUNT(*) FROM (SELECT /*+indexonlyscan(t1 i_t1)*/ b FROM t1 ORDER BY b ASC);
SELECT COUNT(*) FROM (SELECT /*+indexonlyscan(t1 i_t1)*/ b FROM t1 ORDER BY b ASC);

EXPLAIN (COSTS OFF) SELECT COUNT(*) FROM (SELECT /*+indexonlyscan(t1 i_t1)*/ b FROM t1 WHERE b < 400 ORDER BY b DESC);
SELECT COUNT(*) FROM (SELECT /*+indexonlyscan(t1 i_t1)*/ b FROM t1 WHERE b < 400 ORDER BY b DESC);
EXPLAIN (COSTS OFF) SELECT COUNT(*) FROM (SELECT /*+indexonlyscan(t1 i_t1)*/ b FROM t1 WHERE b < 400 ORDER BY b ASC);
SELECT COUNT(*) FROM (SELECT /*+indexonlyscan(t1 i_t1)*/ b FROM t1 WHERE b < 400 ORDER BY b ASC);

EXPLAIN (COSTS OFF) SELECT COUNT(*) FROM (SELECT /*+indexscan(t1 i_t1)*/ a FROM t1 WHERE b < 400 ORDER BY b DESC);
SELECT COUNT(*) FROM (SELECT /*+indexscan(t1 i_t1)*/ a FROM t1 WHERE b < 400 ORDER BY b DESC);
EXPLAIN (COSTS OFF) SELECT COUNT(*) FROM (SELECT /*+indexscan(t1 i_t1)*/ a FROM t1 WHERE b < 400 ORDER BY b ASC);
SELECT COUNT(*) FROM (SELECT /*+indexscan(t1 i_t1)*/ a FROM t1 WHERE b < 400 ORDER BY b ASC);
DROP INDEX i_t1;
CREATE INDEX i_t1 on t1(b) global with (index_type=pcr);
ANALYZE t1;
EXPLAIN (COSTS OFF) SELECT COUNT(*) FROM (SELECT /*+indexonlyscan(t1 i_t1)*/ b FROM t1 ORDER BY b DESC);
SELECT COUNT(*) FROM (SELECT /*+indexonlyscan(t1 i_t1)*/ b FROM t1 ORDER BY b DESC);
EXPLAIN (COSTS OFF) SELECT COUNT(*) FROM (SELECT /*+indexonlyscan(t1 i_t1)*/ b FROM t1 ORDER BY b ASC);
SELECT COUNT(*) FROM (SELECT /*+indexonlyscan(t1 i_t1)*/ b FROM t1 ORDER BY b ASC);

EXPLAIN (COSTS OFF) SELECT COUNT(*) FROM (SELECT /*+indexonlyscan(t1 i_t1)*/ b FROM t1 WHERE b < 400 ORDER BY b DESC);
SELECT COUNT(*) FROM (SELECT /*+indexonlyscan(t1 i_t1)*/ b FROM t1 WHERE b < 400 ORDER BY b DESC);
EXPLAIN (COSTS OFF) SELECT COUNT(*) FROM (SELECT /*+indexonlyscan(t1 i_t1)*/ b FROM t1 WHERE b < 400 ORDER BY b ASC);
SELECT COUNT(*) FROM (SELECT /*+indexonlyscan(t1 i_t1)*/ b FROM t1 WHERE b < 400 ORDER BY b ASC);

EXPLAIN (COSTS OFF) SELECT COUNT(*) FROM (SELECT /*+indexscan(t1 i_t1)*/ a FROM t1 WHERE b < 400 ORDER BY b DESC);
SELECT COUNT(*) FROM (SELECT /*+indexscan(t1 i_t1)*/ a FROM t1 WHERE b < 400 ORDER BY b DESC);
EXPLAIN (COSTS OFF) SELECT COUNT(*) FROM (SELECT /*+indexscan(t1 i_t1)*/ a FROM t1 WHERE b < 400 ORDER BY b ASC);
SELECT COUNT(*) FROM (SELECT /*+indexscan(t1 i_t1)*/ a FROM t1 WHERE b < 400 ORDER BY b ASC);

-- another query
SET QUERY_DOP = 1002;
DROP TABLE IF EXISTS t1;
CREATE TABLE t1 (a INT, b INT, c INT) WITH (STORAGE_TYPE = USTORE);
CREATE INDEX i_t1 on t1(b) with (index_type=pcr);
INSERT INTO t1 SELECT x,x,x FROM GENERATE_SERIES(1, 700) x;
INSERT INTO t1 SELECT 1001, 1002, x FROM GENERATE_SERIES(1, 300) x;
ANALYZE t1;
EXPLAIN (COSTS OFF) SELECT /*+indexonlyscan(t1 i_t1)*/ COUNT(*) FROM t1 WHERE b = 1002;
SELECT /*+indexonlyscan(t1 i_t1)*/ COUNT(*) FROM t1 WHERE b = 1002;
EXPLAIN (COSTS OFF) SELECT /*+indexscan(t1 i_t1)*/ COUNT(a) FROM t1 WHERE b = 1002;
SELECT /*+indexscan(t1 i_t1)*/ COUNT(a) FROM t1 WHERE b = 1002;
EXPLAIN (COSTS OFF) SELECT COUNT(*) FROM (SELECT /*+indexonlyscan(t1 i_t1)*/ b FROM t1 WHERE b = 1002 ORDER BY b DESC);
SELECT COUNT(*) FROM (SELECT /*+indexonlyscan(t1 i_t1)*/ b FROM t1 WHERE b = 1002 ORDER BY b DESC);
EXPLAIN (COSTS OFF) SELECT COUNT(*) FROM (SELECT /*+indexscan(t1 i_t1)*/ a FROM t1 WHERE b = 1002 ORDER BY b DESC);
SELECT COUNT(*) FROM (SELECT /*+indexscan(t1 i_t1)*/ a FROM t1 WHERE b = 1002 ORDER BY b DESC);

EXPLAIN (COSTS OFF) SELECT /*+indexonlyscan(t1 i_t1)*/ COUNT(*) FROM t1 WHERE b BETWEEN 200 AND 500 AND b != 400;
SELECT /*+indexonlyscan(t1 i_t1)*/ COUNT(*) FROM t1 WHERE b BETWEEN 200 AND 500 AND b != 400;
EXPLAIN (COSTS OFF) SELECT /*+indexscan(t1 i_t1)*/ COUNT(a) FROM t1 WHERE b BETWEEN 200 AND 500 AND b != 400;
SELECT /*+indexscan(t1 i_t1)*/ COUNT(a) FROM t1 WHERE b BETWEEN 200 AND 500 AND b != 400;

-- IS NOT NULL
EXPLAIN (COSTS OFF) SELECT /*+indexonlyscan(t1 i_t1)*/ COUNT(*) FROM t1 WHERE b IS NOT NULL;
SELECT /*+indexonlyscan(t1 i_t1)*/ COUNT(*) FROM t1 WHERE b IS NOT NULL;
EXPLAIN (COSTS OFF) SELECT /*+indexscan(t1 i_t1)*/ COUNT(a) FROM t1 WHERE b IS NOT NULL;
SELECT /*+indexscan(t1 i_t1)*/ COUNT(a) FROM t1 WHERE b IS NOT NULL;


-- JOIN
TRUNCATE t1;
INSERT INTO t1 VALUES(GENERATE_SERIES(1,600), GENERATE_SERIES(1,600), GENERATE_SERIES(1,600));
DROP TABLE IF EXISTS t2;
CREATE TABLE t2 (a INT, b INT, c INT) WITH (STORAGE_TYPE = USTORE);
CREATE INDEX i_t2 ON t2(b) with (index_type=pcr);
INSERT INTO t2 VALUES(GENERATE_SERIES(300,600), GENERATE_SERIES(300,600), GENERATE_SERIES(300,600));
ANALYZE t1;
ANALYZE t2;
SET ENABLE_SEQSCAN=false;
EXPLAIN (COSTS OFF) SELECT /*+indexonlyscan(t1 i_t1) indexonlyscan(t2 i_t2) leading((t1 t2))*/ COUNT(*)
FROM t1,t2 WHERE t1.b=t2.b;
SELECT /*+indexonlyscan(t1 i_t1) indexonlyscan(t2 i_t2) leading((t1 t2))*/ COUNT(*) FROM t1,t2 WHERE t1.b=t2.b;
EXPLAIN (COSTS OFF) SELECT /*+indexscan(t1 i_t1) indexscan(t2 i_t2) leading((t1 t2))*/ COUNT(t1.a)+COUNT(t2.a) 
FROM t1,t2 WHERE t1.b=t2.b;
SELECT /*+indexonlyscan(t1 i_t1) indexonlyscan(t2 i_t2) leading((t1 t2))*/ COUNT(*) FROM t1,t2 WHERE t1.b=t2.b;

-- >, >=, <, <=, =, !=, is null
TRUNCATE t1;
EXPLAIN (COSTS OFF) SELECT /*+indexonlyscan(t1 i_t1)*/ COUNT(*) FROM t1 WHERE b > 50;
SELECT /*+indexonlyscan(t1 i_t1)*/ COUNT(*) FROM t1 WHERE b > 50;
INSERT INTO t1 VALUES(GENERATE_SERIES(1,2000), GENERATE_SERIES(1,2000), GENERATE_SERIES(1,2000));
SET QUERY_DOP=1064;
EXPLAIN (COSTS OFF) SELECT /*+indexonlyscan(t1 i_t1)*/ COUNT(*) FROM t1 WHERE b > 50 AND b < 1200;
SELECT /*+indexonlyscan(t1 i_t1)*/ COUNT(*) FROM t1 WHERE b >= 50 AND b < 1200;
SELECT /*+indexonlyscan(t1 i_t1)*/ COUNT(*) FROM t1 WHERE b > 50 AND b <= 1200;
SET QUERY_DOP=1002;
EXPLAIN (COSTS OFF) SELECT /*+indexonlyscan(t1 i_t1)*/ COUNT(*) FROM t1 WHERE b != 100;
SELECT /*+indexonlyscan(t1 i_t1)*/ COUNT(*) FROM t1 WHERE b != 100;
EXPLAIN (COSTS OFF) SELECT /*+indexonlyscan(t1 i_t1)*/ COUNT(*) FROM t1 WHERE b > 100;
SELECT /*+indexonlyscan(t1 i_t1)*/ COUNT(*) FROM t1 WHERE b > 100;
EXPLAIN (COSTS OFF) SELECT /*+indexonlyscan(t1 i_t1)*/ COUNT(*) FROM t1 WHERE b < 1999;
SELECT /*+indexonlyscan(t1 i_t1)*/ COUNT(*) FROM t1 WHERE b < 1999;
EXPLAIN (COSTS OFF) SELECT /*+indexonlyscan(t1 i_t1)*/ COUNT(*) FROM t1 WHERE b IS NULL;
SELECT /*+indexonlyscan(t1 i_t1)*/ COUNT(*) FROM t1 WHERE b IS NULL;
EXPLAIN (COSTS OFF) SELECT /*+indexonlyscan(t1 i_t1)*/ COUNT(*) FROM t1;
SELECT /*+indexonlyscan(t1 i_t1)*/ COUNT(*) FROM t1;
EXPLAIN (COSTS OFF) SELECT COUNT(*) FROM (SELECT /*+indexonlyscan(t1 i_t1)*/ b FROM t1 WHERE b < 2000 ORDER BY b DESC);
SELECT COUNT(*) FROM (SELECT /*+indexonlyscan(t1 i_t1)*/ b FROM t1 WHERE b < 2000 ORDER BY b DESC);

-- multi plan node 
TRUNCATE t1;
INSERT INTO t1 VALUES(GENERATE_SERIES(1,200), GENERATE_SERIES(1,200), GENERATE_SERIES(1,200));
EXPLAIN (COSTS OFF) SELECT count(*) FROM t1 AS t00, t1 AS t01 where t00.b < 200 AND t00.b > 100 AND t01.b < 190;
SELECT count(*) FROM t1 AS t00, t1 AS t01 where t00.b < 200 AND t00.b > 100 AND t01.b < 190;

RESET ENABLE_SEQSCAN;
RESET QUERY_DOP;
RESET enable_mergejoin;
drop table t1;
drop table t2;
SET CURRENT_SCHEMA=PUBLIC;
DROP SCHEMA index_pcr_ustore_smp_other CASCADE;