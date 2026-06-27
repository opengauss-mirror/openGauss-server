set enable_expr_fusion=on;
DROP SCHEMA IF EXISTS index_pcr_ustore_smp CASCADE;
CREATE SCHEMA index_pcr_ustore_smp;
SET CURRENT_SCHEMA TO index_pcr_ustore_smp;
set enable_mergejoin = false;

DROP TABLE IF EXISTS t1;
CREATE TABLE t1 (a INT, b INT, c INT) WITH (STORAGE_TYPE = USTORE);
CREATE INDEX i_t1 on t1(b) with (index_type=pcr);
--forward scan
--without analyze
SET QUERY_DOP = 1002;
-- 1 page
TRUNCATE t1;
INSERT INTO t1 VALUES(GENERATE_SERIES(1,100), GENERATE_SERIES(1,100), GENERATE_SERIES(1,100));
EXPLAIN (COSTS OFF) SELECT /*+indexonlyscan(t1 i_t1)*/ COUNT(*) FROM t1;
SELECT /*+indexonlyscan(t1 i_t1)*/ COUNT(*) FROM t1;
EXPLAIN (COSTS OFF) SELECT /*+indexonlyscan(t1 i_t1)*/ COUNT(*) FROM t1 WHERE b < 30;
SELECT /*+indexonlyscan(t1 i_t1)*/ COUNT(*) FROM t1 WHERE b < 30;
EXPLAIN (COSTS OFF) SELECT /*+indexscan(t1 i_t1)*/ COUNT(a) FROM t1 WHERE b < 30;
SELECT /*+indexscan(t1 i_t1)*/ COUNT(a) FROM t1 WHERE b < 30;
 
-- with analyze
SET QUERY_DOP = 1002;
-- 1 page
TRUNCATE t1;
INSERT INTO t1 VALUES(GENERATE_SERIES(1,600), GENERATE_SERIES(1,600), GENERATE_SERIES(1,600));
EXPLAIN (COSTS OFF) SELECT /*+indexonlyscan(t1 i_t1)*/ COUNT(*) FROM t1;
SELECT /*+indexonlyscan(t1 i_t1)*/ COUNT(*) FROM t1;
EXPLAIN (COSTS OFF) SELECT /*+indexonlyscan(t1 i_t1)*/ COUNT(*) FROM t1 WHERE b < 200;
SELECT /*+indexonlyscan(t1 i_t1)*/ COUNT(*) FROM t1 WHERE b < 200;
EXPLAIN (COSTS OFF) SELECT /*+indexscan(t1 i_t1)*/ COUNT(a) FROM t1 WHERE b < 200;
SELECT /*+indexscan(t1 i_t1)*/ COUNT(a) FROM t1 WHERE b < 200;
 
SET QUERY_DOP = 1002;
-- 2 page
TRUNCATE t1;
INSERT INTO t1 VALUES(GENERATE_SERIES(1,200), GENERATE_SERIES(1,200), GENERATE_SERIES(1,200));
ANALYZE t1;
EXPLAIN (COSTS OFF) SELECT /*+indexonlyscan(t1 i_t1)*/ COUNT(*) FROM t1;
SELECT /*+indexonlyscan(t1 i_t1)*/ COUNT(*) FROM t1;
EXPLAIN (COSTS OFF) SELECT /*+indexonlyscan(t1 i_t1)*/ COUNT(*) FROM t1 WHERE b < 60;
SELECT /*+indexonlyscan(t1 i_t1)*/ COUNT(*) FROM t1 WHERE b < 60;
EXPLAIN (COSTS OFF) SELECT /*+indexscan(t1 i_t1)*/ COUNT(a) FROM t1 WHERE b < 60;
SELECT /*+indexscan(t1 i_t1)*/ COUNT(a) FROM t1 WHERE b < 60;
 
SET QUERY_DOP = 1002;
-- 4 page
TRUNCATE t1;
INSERT INTO t1 VALUES(GENERATE_SERIES(1,600), GENERATE_SERIES(1,600), GENERATE_SERIES(1,600));
ANALYZE t1;
EXPLAIN (COSTS OFF) SELECT /*+indexonlyscan(t1 i_t1)*/ COUNT(*) FROM t1;
SELECT /*+indexonlyscan(t1 i_t1)*/ COUNT(*) FROM t1;
EXPLAIN (COSTS OFF) SELECT /*+indexonlyscan(t1 i_t1)*/ COUNT(*) FROM t1 WHERE b < 200;
SELECT /*+indexonlyscan(t1 i_t1)*/ COUNT(*) FROM t1 WHERE b < 200;
EXPLAIN (COSTS OFF) SELECT /*+indexscan(t1 i_t1)*/ COUNT(a) FROM t1 WHERE b < 200;
SELECT /*+indexscan(t1 i_t1)*/ COUNT(a) FROM t1 WHERE b < 200;
 
SET QUERY_DOP = 1003;
-- 4 page
TRUNCATE t1;
INSERT INTO t1 VALUES(GENERATE_SERIES(1,600), GENERATE_SERIES(1,600), GENERATE_SERIES(1,600));
ANALYZE t1;
EXPLAIN (COSTS OFF) SELECT /*+indexonlyscan(t1 i_t1)*/ COUNT(*) FROM t1;
SELECT /*+indexonlyscan(t1 i_t1)*/ COUNT(*) FROM t1;
EXPLAIN (COSTS OFF) SELECT /*+indexonlyscan(t1 i_t1)*/ COUNT(*) FROM t1 WHERE b < 200;
SELECT /*+indexonlyscan(t1 i_t1)*/ COUNT(*) FROM t1 WHERE b < 200;
EXPLAIN (COSTS OFF) SELECT /*+indexscan(t1 i_t1)*/ COUNT(a) FROM t1 WHERE b < 200;
SELECT /*+indexscan(t1 i_t1)*/ COUNT(a) FROM t1 WHERE b < 200;
 
SET QUERY_DOP = 1004;
-- 4 page
TRUNCATE t1;
INSERT INTO t1 VALUES(GENERATE_SERIES(1,600), GENERATE_SERIES(1,600), GENERATE_SERIES(1,600));
ANALYZE t1;
EXPLAIN (COSTS OFF) SELECT /*+indexonlyscan(t1 i_t1)*/ COUNT(*) FROM t1;
SELECT /*+indexonlyscan(t1 i_t1)*/ COUNT(*) FROM t1;
EXPLAIN (COSTS OFF) SELECT /*+indexonlyscan(t1 i_t1)*/ COUNT(*) FROM t1 WHERE b < 200;
SELECT /*+indexonlyscan(t1 i_t1)*/ COUNT(*) FROM t1 WHERE b < 200;
EXPLAIN (COSTS OFF) SELECT /*+indexscan(t1 i_t1)*/ COUNT(a) FROM t1 WHERE b < 200;
SELECT /*+indexscan(t1 i_t1)*/ COUNT(a) FROM t1 WHERE b < 200;
 
SET QUERY_DOP = 1064;
-- 4 page
TRUNCATE t1;
INSERT INTO t1 VALUES(GENERATE_SERIES(1,600), GENERATE_SERIES(1,600), GENERATE_SERIES(1,600));
ANALYZE t1;
EXPLAIN (COSTS OFF) SELECT /*+indexonlyscan(t1 i_t1)*/ COUNT(*) FROM t1;
SELECT /*+indexonlyscan(t1 i_t1)*/ COUNT(*) FROM t1;
EXPLAIN (COSTS OFF) SELECT /*+indexonlyscan(t1 i_t1)*/ COUNT(*) FROM t1 WHERE b < 200;
SELECT /*+indexonlyscan(t1 i_t1)*/ COUNT(*) FROM t1 WHERE b < 200;
EXPLAIN (COSTS OFF) SELECT /*+indexscan(t1 i_t1)*/ COUNT(a) FROM t1 WHERE b < 200;
SELECT /*+indexscan(t1 i_t1)*/ COUNT(a) FROM t1 WHERE b < 200;
 
-- insert after analyze
SET QUERY_DOP = 1002;
TRUNCATE t1;
INSERT INTO t1 VALUES(GENERATE_SERIES(1,600), GENERATE_SERIES(1,600), GENERATE_SERIES(1,600));
ANALYZE t1;
INSERT INTO t1 VALUES(GENERATE_SERIES(1,600), GENERATE_SERIES(1,600), GENERATE_SERIES(1,600));
EXPLAIN (COSTS OFF) SELECT /*+indexonlyscan(t1 i_t1)*/ COUNT(*) FROM t1;
SELECT /*+indexonlyscan(t1 i_t1)*/ COUNT(*) FROM t1;
EXPLAIN (COSTS OFF) SELECT /*+indexonlyscan(t1 i_t1)*/ COUNT(*) FROM t1 WHERE b < 200;
SELECT /*+indexonlyscan(t1 i_t1)*/ COUNT(*) FROM t1 WHERE b < 200;
EXPLAIN (COSTS OFF) SELECT /*+indexscan(t1 i_t1)*/ COUNT(a) FROM t1 WHERE b < 200;
SELECT /*+indexscan(t1 i_t1)*/ COUNT(a) FROM t1 WHERE b < 200;
 
-- delete after analyze
SET QUERY_DOP = 1002;
TRUNCATE t1;
INSERT INTO t1 VALUES(GENERATE_SERIES(1,600), GENERATE_SERIES(1,600), GENERATE_SERIES(1,600));
ANALYZE t1;
DELETE FROM t1 WHERE b <= 500;
EXPLAIN (COSTS OFF) SELECT /*+indexonlyscan(t1 i_t1)*/ COUNT(*) FROM t1;
SELECT /*+indexonlyscan(t1 i_t1)*/ COUNT(*) FROM t1;
EXPLAIN (COSTS OFF) SELECT /*+indexonlyscan(t1 i_t1)*/ COUNT(*) FROM t1 WHERE b < 550;
SELECT /*+indexonlyscan(t1 i_t1)*/ COUNT(*) FROM t1 WHERE b < 550;
EXPLAIN (COSTS OFF) SELECT /*+indexscan(t1 i_t1)*/ COUNT(a) FROM t1 WHERE b < 550;
SELECT /*+indexscan(t1 i_t1)*/ COUNT(a) FROM t1 WHERE b < 550;
 
-- vacuum
SET QUERY_DOP = 1002;
TRUNCATE t1;
INSERT INTO t1 VALUES(GENERATE_SERIES(1,600), GENERATE_SERIES(1,600), GENERATE_SERIES(1,600));
ANALYZE t1;
DELETE FROM t1 WHERE b <= 500;
VACUUM FULL t1;
EXPLAIN (COSTS OFF) SELECT /*+indexonlyscan(t1 i_t1)*/ COUNT(*) FROM t1;
SELECT /*+indexonlyscan(t1 i_t1)*/ COUNT(*) FROM t1;
EXPLAIN (COSTS OFF) SELECT /*+indexonlyscan(t1 i_t1)*/ COUNT(*) FROM t1 WHERE b < 550;
SELECT /*+indexonlyscan(t1 i_t1)*/ COUNT(*) FROM t1 WHERE b < 550;
EXPLAIN (COSTS OFF) SELECT /*+indexscan(t1 i_t1)*/ COUNT(a) FROM t1 WHERE b < 550;
SELECT /*+indexscan(t1 i_t1)*/ COUNT(a) FROM t1 WHERE b < 550;
 
-- function
SET QUERY_DOP = 1002;
TRUNCATE t1;
INSERT INTO t1 VALUES(GENERATE_SERIES(1,600), GENERATE_SERIES(1,600), GENERATE_SERIES(1,600));
ANALYZE t1;
CREATE OR REPLACE FUNCTION select_one_table(out1 OUT bigint)
AS 'SELECT /*+indexonlyscan(t1 i_t1)*/ COUNT(*) FROM t1'
LANGUAGE SQL;
SELECT select_one_table();

CREATE OR REPLACE FUNCTION select_one_table1(out1 OUT bigint)
AS 'SELECT /*+indexonlyscan(t1 i_t1)*/ COUNT(*) FROM t1 WHERE b < 200'
LANGUAGE SQL;
SELECT select_one_table1();

CREATE OR REPLACE FUNCTION select_one_table2(out1 OUT bigint)
AS 'SELECT /*+indexscan(t1 i_t1)*/ COUNT(a) FROM t1 WHERE b < 200'
LANGUAGE SQL;
SELECT select_one_table2();
--pbe
PREPARE p1 as SELECT /*+indexonlyscan(t1 i_t1)*/ COUNT(*) FROM t1;
EXECUTE p1;

PREPARE p2 as SELECT /*+indexscan(t1 i_t1)*/ COUNT(a) FROM t1;
EXECUTE p2;

PREPARE p3 as SELECT /*+indexonlyscan(t1 i_t1)*/ COUNT(*) FROM t1 WHERE b < 200;
EXECUTE p3;

PREPARE p4 as SELECT /*+indexscan(t1 i_t1)*/ COUNT(a) FROM t1 WHERE b < 200;
EXECUTE p4;
 
 
-- backward scan
--without analyze
SET QUERY_DOP = 1002;
-- 1 page
TRUNCATE t1;
INSERT INTO t1 VALUES(GENERATE_SERIES(1,100), GENERATE_SERIES(1,100), GENERATE_SERIES(1,100));
EXPLAIN (COSTS OFF) SELECT COUNT(*) FROM (SELECT /*+indexonlyscan(t1 i_t1)*/ b FROM t1 ORDER BY b DESC);
SELECT COUNT(*) FROM (SELECT /*+indexonlyscan(t1 i_t1)*/ b FROM t1 ORDER BY b DESC);
EXPLAIN (COSTS OFF) SELECT COUNT(*) FROM (SELECT /*+indexonlyscan(t1 i_t1)*/ b FROM t1 WHERE b < 30 ORDER BY b DESC);
SELECT COUNT(*) FROM (SELECT /*+indexonlyscan(t1 i_t1)*/ b FROM t1 WHERE b < 30 ORDER BY b DESC);
EXPLAIN (COSTS OFF) SELECT COUNT(*) FROM (SELECT /*+indexscan(t1 i_t1)*/ a FROM t1 WHERE b < 30 ORDER BY b DESC);
SELECT COUNT(*) FROM (SELECT /*+indexscan(t1 i_t1)*/ a FROM t1 WHERE b < 30 ORDER BY b DESC);
 
-- with analyze
SET QUERY_DOP = 1002;
-- 1 page
TRUNCATE t1;
INSERT INTO t1 VALUES(GENERATE_SERIES(1,600), GENERATE_SERIES(1,600), GENERATE_SERIES(1,600));
EXPLAIN (COSTS OFF) SELECT COUNT(*) FROM (SELECT /*+indexonlyscan(t1 i_t1)*/ b FROM t1 ORDER BY b DESC);
SELECT COUNT(*) FROM (SELECT /*+indexonlyscan(t1 i_t1)*/ b FROM t1 ORDER BY b DESC);
EXPLAIN (COSTS OFF) SELECT COUNT(*) FROM (SELECT /*+indexonlyscan(t1 i_t1)*/ b FROM t1 WHERE b < 200 ORDER BY b DESC);
SELECT COUNT(*) FROM (SELECT /*+indexonlyscan(t1 i_t1)*/ b FROM t1 WHERE b < 200 ORDER BY b DESC);
EXPLAIN (COSTS OFF) SELECT COUNT(*) FROM (SELECT /*+indexscan(t1 i_t1)*/ a FROM t1 WHERE b < 200 ORDER BY b DESC);
SELECT COUNT(*) FROM (SELECT /*+indexscan(t1 i_t1)*/ a FROM t1 WHERE b < 200 ORDER BY b DESC);
 
SET QUERY_DOP = 1002;
-- 2 page
TRUNCATE t1;
INSERT INTO t1 VALUES(GENERATE_SERIES(1,200), GENERATE_SERIES(1,200), GENERATE_SERIES(1,200));
ANALYZE t1;
EXPLAIN (COSTS OFF) SELECT COUNT(*) FROM (SELECT /*+indexonlyscan(t1 i_t1)*/ b FROM t1 ORDER BY b DESC);
SELECT COUNT(*) FROM (SELECT /*+indexonlyscan(t1 i_t1)*/ b FROM t1 ORDER BY b DESC);
EXPLAIN (COSTS OFF) SELECT COUNT(*) FROM (SELECT /*+indexonlyscan(t1 i_t1)*/ b FROM t1 WHERE b < 60 ORDER BY b DESC);
SELECT COUNT(*) FROM (SELECT /*+indexonlyscan(t1 i_t1)*/ b FROM t1 WHERE b < 60 ORDER BY b DESC);
EXPLAIN (COSTS OFF) SELECT COUNT(*) FROM (SELECT /*+indexscan(t1 i_t1)*/ a FROM t1 WHERE b < 60 ORDER BY b DESC);
SELECT COUNT(*) FROM (SELECT /*+indexscan(t1 i_t1)*/ a FROM t1 WHERE b < 60 ORDER BY b DESC);
 
SET QUERY_DOP = 1002;
-- 4 page
TRUNCATE t1;
INSERT INTO t1 VALUES(GENERATE_SERIES(1,600), GENERATE_SERIES(1,600), GENERATE_SERIES(1,600));
ANALYZE t1;
EXPLAIN (COSTS OFF) SELECT COUNT(*) FROM (SELECT /*+indexonlyscan(t1 i_t1)*/ b FROM t1 ORDER BY b DESC);
SELECT COUNT(*) FROM (SELECT /*+indexonlyscan(t1 i_t1)*/ b FROM t1 ORDER BY b DESC);
EXPLAIN (COSTS OFF) SELECT COUNT(*) FROM (SELECT /*+indexonlyscan(t1 i_t1)*/ b FROM t1 WHERE b < 200 ORDER BY b DESC);
SELECT COUNT(*) FROM (SELECT /*+indexonlyscan(t1 i_t1)*/ b FROM t1 WHERE b < 200 ORDER BY b DESC);
EXPLAIN (COSTS OFF) SELECT COUNT(*) FROM (SELECT /*+indexscan(t1 i_t1)*/ a FROM t1 WHERE b < 200 ORDER BY b DESC);
SELECT COUNT(*) FROM (SELECT /*+indexscan(t1 i_t1)*/ a FROM t1 WHERE b < 200 ORDER BY b DESC);
 
SET QUERY_DOP = 1003;
-- 4 page
TRUNCATE t1;
INSERT INTO t1 VALUES(GENERATE_SERIES(1,600), GENERATE_SERIES(1,600), GENERATE_SERIES(1,600));
ANALYZE t1;
EXPLAIN (COSTS OFF) SELECT COUNT(*) FROM (SELECT /*+indexonlyscan(t1 i_t1)*/ b FROM t1 ORDER BY b DESC);
SELECT COUNT(*) FROM (SELECT /*+indexonlyscan(t1 i_t1)*/ b FROM t1 ORDER BY b DESC);
EXPLAIN (COSTS OFF) SELECT COUNT(*) FROM (SELECT /*+indexonlyscan(t1 i_t1)*/ b FROM t1 WHERE b < 200 ORDER BY b DESC);
SELECT COUNT(*) FROM (SELECT /*+indexonlyscan(t1 i_t1)*/ b FROM t1 WHERE b < 200 ORDER BY b DESC);
EXPLAIN (COSTS OFF) SELECT COUNT(*) FROM (SELECT /*+indexscan(t1 i_t1)*/ a FROM t1 WHERE b < 200 ORDER BY b DESC);
SELECT COUNT(*) FROM (SELECT /*+indexscan(t1 i_t1)*/ a FROM t1 WHERE b < 200 ORDER BY b DESC);
 
SET QUERY_DOP = 1004;
-- 4 page
TRUNCATE t1;
INSERT INTO t1 VALUES(GENERATE_SERIES(1,600), GENERATE_SERIES(1,600), GENERATE_SERIES(1,600));
ANALYZE t1;
EXPLAIN (COSTS OFF) SELECT COUNT(*) FROM (SELECT /*+indexonlyscan(t1 i_t1)*/ b FROM t1 ORDER BY b DESC);
SELECT COUNT(*) FROM (SELECT /*+indexonlyscan(t1 i_t1)*/ b FROM t1 ORDER BY b DESC);
EXPLAIN (COSTS OFF) SELECT COUNT(*) FROM (SELECT /*+indexonlyscan(t1 i_t1)*/ b FROM t1 WHERE b < 200 ORDER BY b DESC);
SELECT COUNT(*) FROM (SELECT /*+indexonlyscan(t1 i_t1)*/ b FROM t1 WHERE b < 200 ORDER BY b DESC);
EXPLAIN (COSTS OFF) SELECT COUNT(*) FROM (SELECT /*+indexscan(t1 i_t1)*/ a FROM t1 WHERE b < 200 ORDER BY b DESC);
SELECT COUNT(*) FROM (SELECT /*+indexscan(t1 i_t1)*/ a FROM t1 WHERE b < 200 ORDER BY b DESC);
 
SET QUERY_DOP = 1064;
-- 4 page
TRUNCATE t1;
INSERT INTO t1 VALUES(GENERATE_SERIES(1,600), GENERATE_SERIES(1,600), GENERATE_SERIES(1,600));
ANALYZE t1;
EXPLAIN (COSTS OFF) SELECT COUNT(*) FROM (SELECT /*+indexonlyscan(t1 i_t1)*/ b FROM t1 ORDER BY b DESC);
SELECT COUNT(*) FROM (SELECT /*+indexonlyscan(t1 i_t1)*/ b FROM t1 ORDER BY b DESC);
EXPLAIN (COSTS OFF) SELECT COUNT(*) FROM (SELECT /*+indexonlyscan(t1 i_t1)*/ b FROM t1 WHERE b < 200 ORDER BY b DESC);
SELECT COUNT(*) FROM (SELECT /*+indexonlyscan(t1 i_t1)*/ b FROM t1 WHERE b < 200 ORDER BY b DESC);
EXPLAIN (COSTS OFF) SELECT COUNT(*) FROM (SELECT /*+indexscan(t1 i_t1)*/ a FROM t1 WHERE b < 200 ORDER BY b DESC);
SELECT COUNT(*) FROM (SELECT /*+indexscan(t1 i_t1)*/ a FROM t1 WHERE b < 200 ORDER BY b DESC);
 
-- insert after analyze
SET QUERY_DOP = 1002;
TRUNCATE t1;
INSERT INTO t1 VALUES(GENERATE_SERIES(1,600), GENERATE_SERIES(1,600), GENERATE_SERIES(1,600));
ANALYZE t1;
INSERT INTO t1 VALUES(GENERATE_SERIES(1,600), GENERATE_SERIES(1,600), GENERATE_SERIES(1,600));
EXPLAIN (COSTS OFF) SELECT COUNT(*) FROM (SELECT /*+indexonlyscan(t1 i_t1)*/ b FROM t1 ORDER BY b DESC);
SELECT COUNT(*) FROM (SELECT /*+indexonlyscan(t1 i_t1)*/ b FROM t1 ORDER BY b DESC);
EXPLAIN (COSTS OFF) SELECT COUNT(*) FROM (SELECT /*+indexonlyscan(t1 i_t1)*/ b FROM t1 WHERE b < 200 ORDER BY b DESC);
SELECT COUNT(*) FROM (SELECT /*+indexonlyscan(t1 i_t1)*/ b FROM t1 WHERE b < 200 ORDER BY b DESC);
EXPLAIN (COSTS OFF) SELECT COUNT(*) FROM (SELECT /*+indexscan(t1 i_t1)*/ a FROM t1 WHERE b < 200 ORDER BY b DESC);
SELECT COUNT(*) FROM (SELECT /*+indexscan(t1 i_t1)*/ a FROM t1 WHERE b < 200 ORDER BY b DESC);
 
-- delete after analyze
SET QUERY_DOP = 1002;
TRUNCATE t1;
INSERT INTO t1 VALUES(GENERATE_SERIES(1,600), GENERATE_SERIES(1,600), GENERATE_SERIES(1,600));
ANALYZE t1;
DELETE FROM t1 WHERE b <= 500;
EXPLAIN (COSTS OFF) SELECT COUNT(*) FROM (SELECT /*+indexonlyscan(t1 i_t1)*/ b FROM t1 ORDER BY b DESC);
SELECT COUNT(*) FROM (SELECT /*+indexonlyscan(t1 i_t1)*/ b FROM t1 ORDER BY b DESC);
EXPLAIN (COSTS OFF) SELECT COUNT(*) FROM (SELECT /*+indexonlyscan(t1 i_t1)*/ b FROM t1 WHERE b < 550 ORDER BY b DESC);
SELECT COUNT(*) FROM (SELECT /*+indexonlyscan(t1 i_t1)*/ b FROM t1 WHERE b < 550 ORDER BY b DESC);
EXPLAIN (COSTS OFF) SELECT COUNT(*) FROM (SELECT /*+indexscan(t1 i_t1)*/ a FROM t1 WHERE b < 550 ORDER BY b DESC);
SELECT COUNT(*) FROM (SELECT /*+indexscan(t1 i_t1)*/ a FROM t1 WHERE b < 550 ORDER BY b DESC);
 
-- vacuum
SET QUERY_DOP = 1002;
TRUNCATE t1;
INSERT INTO t1 VALUES(GENERATE_SERIES(1,600), GENERATE_SERIES(1,600), GENERATE_SERIES(1,600));
ANALYZE t1;
DELETE FROM t1 WHERE b <= 500;
VACUUM FULL t1;
EXPLAIN (COSTS OFF) SELECT COUNT(*) FROM (SELECT /*+indexonlyscan(t1 i_t1)*/ b FROM t1 ORDER BY b DESC);
SELECT COUNT(*) FROM (SELECT /*+indexonlyscan(t1 i_t1)*/ b FROM t1 ORDER BY b DESC);
EXPLAIN (COSTS OFF) SELECT COUNT(*) FROM (SELECT /*+indexonlyscan(t1 i_t1)*/ b FROM t1 WHERE b < 550 ORDER BY b DESC);
SELECT COUNT(*) FROM (SELECT /*+indexonlyscan(t1 i_t1)*/ b FROM t1 WHERE b < 550 ORDER BY b DESC);
EXPLAIN (COSTS OFF) SELECT COUNT(*) FROM (SELECT /*+indexscan(t1 i_t1)*/ a FROM t1 WHERE b < 550 ORDER BY b DESC);
SELECT COUNT(*) FROM (SELECT /*+indexscan(t1 i_t1)*/ a FROM t1 WHERE b < 550 ORDER BY b DESC);
 
-- function
SET QUERY_DOP = 1002;
TRUNCATE t1;
INSERT INTO t1 VALUES(GENERATE_SERIES(1,600), GENERATE_SERIES(1,600), GENERATE_SERIES(1,600));
ANALYZE t1;
CREATE OR REPLACE FUNCTION select_one_table(out1 OUT bigint)
AS 'SELECT COUNT(*) FROM (SELECT /*+indexonlyscan(t1 i_t1)*/ b FROM t1 ORDER BY b DESC)'
LANGUAGE SQL;
SELECT select_one_table();
CREATE OR REPLACE FUNCTION select_one_table1(out1 OUT bigint)
AS 'SELECT COUNT(*) FROM (SELECT /*+indexonlyscan(t1 i_t1)*/ b FROM t1 WHERE b < 200 ORDER BY b DESC)'
LANGUAGE SQL;
SELECT select_one_table1();
CREATE OR REPLACE FUNCTION select_one_table2(out1 OUT bigint)
AS 'SELECT COUNT(*) FROM (SELECT /*+indexscan(t1 i_t1)*/ a FROM t1 WHERE b < 200 ORDER BY b DESC)'
LANGUAGE SQL;
SELECT select_one_table2();
--pbe
PREPARE p5 as SELECT COUNT(*) FROM (SELECT /*+indexonlyscan(t1 i_t1)*/ b FROM t1 ORDER BY b DESC);
EXECUTE p5;
PREPARE p6 as SELECT COUNT(*) FROM (SELECT /*+indexonlyscan(t1 i_t1)*/ b FROM t1 WHERE b < 200 ORDER BY b DESC);
EXECUTE p6;
PREPARE p7 as SELECT COUNT(*) FROM (SELECT /*+indexscan(t1 i_t1)*/ a FROM t1 WHERE b < 200 ORDER BY b DESC);
EXECUTE p7;
 
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
DROP FUNCTION index_pcr_ustore_smp.select_one_table();
DROP FUNCTION index_pcr_ustore_smp.select_one_table1();
DROP FUNCTION index_pcr_ustore_smp.select_one_table2();
DROP TABLE index_pcr_ustore_smp.t1;
DROP TABLE index_pcr_ustore_smp.t2;
SET CURRENT_SCHEMA=PUBLIC;
DROP SCHEMA index_pcr_ustore_smp CASCADE;

set enable_expr_fusion=off;