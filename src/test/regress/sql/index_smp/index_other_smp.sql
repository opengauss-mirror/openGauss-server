--
-- indexsmp_othercases
-- test cases for parallel index(only)scan,
-- most for optimizer's special cases
--

-- partial index scan for partition table, index scan should be paralleled
\c regression
set enable_expr_fusion=on;
DROP SCHEMA IF EXISTS index_smp_othercase CASCADE;
CREATE SCHEMA index_smp_othercase;
SET CURRENT_SCHEMA = index_smp_othercase;
DROP DATABASE IF EXISTS indexsmp_b;
SET QUERY_DOP = 1004;
DROP TABLE IF EXISTS partition_scan_stu_info1;
CREATE TABLE partition_scan_stu_info1(SN INT, NAME NAME)
PARTITION BY RANGE (SN)
(
PARTITION P1_partition_scan_stu_info1 VALUES LESS THAN(10),
PARTITION P2_partition_scan_stu_info1 VALUES LESS THAN(20),
PARTITION P3_partition_scan_stu_info1 VALUES LESS THAN(30),
PARTITION P4_partition_scan_stu_info1 VALUES LESS THAN(40)
);
DROP TABLE IF EXISTS partition_scan_stu_info2;
CREATE TABLE partition_scan_stu_info2(SN INT, PHONE NAME)
PARTITION BY RANGE (SN)
(
PARTITION P1_partition_scan_stu_info2 VALUES LESS THAN(10),
PARTITION P2_partition_scan_stu_info2 VALUES LESS THAN(20),
PARTITION P3_partition_scan_stu_info2 VALUES LESS THAN(30),
PARTITION P4_partition_scan_stu_info2 VALUES LESS THAN(40)
);
CREATE UNIQUE INDEX index_on_partition_scan_stu_info1 ON partition_scan_stu_info1 (SN) LOCAL;
CREATE UNIQUE INDEX index_on_partition_scan_stu_info2 ON partition_scan_stu_info2 (SN) LOCAL;
INSERT INTO partition_scan_stu_info1 VALUES (1,'DFM');
INSERT INTO partition_scan_stu_info1 VALUES (11,'CHAO');
INSERT INTO partition_scan_stu_info1 VALUES (21,'ZJR');
INSERT INTO partition_scan_stu_info1 VALUES (31,'JYH');
INSERT INTO partition_scan_stu_info2 VALUES (1,'15478523126');
INSERT INTO partition_scan_stu_info2 VALUES (11,'15236997586');
INSERT INTO partition_scan_stu_info2 VALUES (21,'15936985364');
INSERT INTO partition_scan_stu_info2 VALUES (31,'15873285556');
--Results
SELECT * FROM partition_scan_stu_info1 WHERE SN <40 order by SN;

SET enable_seqscan = OFF;
SET enable_indexscan = ON;
SET enable_indexonlyscan = ON;
SET enable_bitmapscan = ON;
SET enable_tidscan = ON;
SET enable_sort = ON;
SET enable_material = OFF;
SET enable_nestloop = ON;
SET enable_hashjoin = ON;

--FULL INDEX
EXPLAIN (COSTS OFF) SELECT * FROM partition_scan_stu_info1 WHERE SN <40;
SELECT * FROM partition_scan_stu_info1 WHERE SN <40 order by SN;

--PARTIAL INDEX
ALTER INDEX index_on_partition_scan_stu_info1 MODIFY PARTITION p4_partition_scan_stu_info1_sn_idx UNUSABLE;
EXPLAIN SELECT * FROM partition_scan_stu_info1 WHERE SN <40 ;
SELECT * FROM partition_scan_stu_info1 WHERE SN <40 order by SN;

SET QUERY_DOP = 2002;
DROP TABLE IF EXISTS partition_scan_stu_info1;
DROP TABLE IF EXISTS partition_scan_stu_info2;
RESET CURRENT_SCHEMA;
DROP SCHEMA IF EXISTS index_smp_othercase CASCADE;
set enable_expr_fusion=off;