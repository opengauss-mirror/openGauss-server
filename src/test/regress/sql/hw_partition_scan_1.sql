
---- test for scan for partitioned table
---- add by jiayanhua
---- case 1: partitioned table + partitioned table

--prepare
CREATE TABLE partition_scan_stu_info1(SN INT, NAME NAME)
PARTITION BY RANGE (SN)
(
	PARTITION P1_partition_scan_stu_info1 VALUES LESS THAN(10),
	PARTITION P2_partition_scan_stu_info1 VALUES LESS THAN(20),
	PARTITION P3_partition_scan_stu_info1 VALUES LESS THAN(30),
	PARTITION P4_partition_scan_stu_info1 VALUES LESS THAN(40)
);

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

SELECT * FROM partition_scan_stu_info1 ORDER BY 1;
SELECT * FROM partition_scan_stu_info2 ORDER BY 1;

-- SeqScan test
SET enable_seqscan = ON;
SET enable_indexscan = OFF;
SET enable_indexonlyscan = OFF;
SET enable_bitmapscan = OFF;
SET enable_tidscan = OFF;
SET enable_sort = ON;
SET enable_material = OFF;
SET enable_nestloop = OFF;
SET enable_hashjoin = OFF;

EXPLAIN (COSTS OFF) SELECT SN FROM partition_scan_stu_info1 WHERE SN < 10 ORDER BY SN; --one psrtition: the first
EXPLAIN (COSTS OFF) SELECT SN FROM partition_scan_stu_info1 WHERE SN < 20 ORDER BY SN; --more than one partitions
EXPLAIN (COSTS OFF) SELECT SN FROM partition_scan_stu_info1 WHERE SN > 30 ORDER BY SN; --one psrtition: the last
EXPLAIN (COSTS OFF) SELECT SN FROM partition_scan_stu_info1 WHERE SN > 40 ORDER BY SN; -- no partition
EXPLAIN (COSTS OFF) SELECT SN FROM partition_scan_stu_info1 ORDER BY SN; -- all partitions
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info1.SN > 40; 
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40;
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40 AND partition_scan_stu_info1.SN > 40;
SELECT SN FROM partition_scan_stu_info1 WHERE SN < 10 ORDER BY SN;
SELECT SN FROM partition_scan_stu_info1 WHERE SN < 20 ORDER BY SN;
SELECT SN FROM partition_scan_stu_info1 WHERE SN < 30 ORDER BY SN;
SELECT SN FROM partition_scan_stu_info1 WHERE SN > 40 ORDER BY SN;
SELECT SN FROM partition_scan_stu_info1 ORDER BY SN;
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info1.SN > 40;
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40;
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40 AND partition_scan_stu_info1.SN > 40;

--IndexScan test
SET enable_seqscan = OFF;
SET enable_indexscan = ON;
SET enable_indexonlyscan = OFF;
SET enable_bitmapscan = OFF;
SET enable_tidscan = OFF;
SET enable_sort = ON;
SET enable_material = OFF;
SET enable_nestloop = OFF;
SET enable_hashjoin = OFF;

EXPLAIN (COSTS OFF) SELECT SN FROM partition_scan_stu_info1 WHERE SN < 10 ORDER BY SN; --one psrtition: the first
EXPLAIN (COSTS OFF) SELECT SN FROM partition_scan_stu_info1 WHERE SN < 20 ORDER BY SN; --more than one partitions
EXPLAIN (COSTS OFF) SELECT SN FROM partition_scan_stu_info1 WHERE SN > 30 ORDER BY SN; --one psrtition: the last
EXPLAIN (COSTS OFF) SELECT SN FROM partition_scan_stu_info1 WHERE SN > 40 ORDER BY SN; -- no partition
EXPLAIN (COSTS OFF) SELECT SN FROM partition_scan_stu_info1 ORDER BY SN; -- all partitions
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info1.SN > 40; 
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40;
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40 AND partition_scan_stu_info1.SN > 40;
SELECT SN FROM partition_scan_stu_info1 WHERE SN < 10 ORDER BY SN;
SELECT SN FROM partition_scan_stu_info1 WHERE SN < 20 ORDER BY SN;
SELECT SN FROM partition_scan_stu_info1 WHERE SN < 30 ORDER BY SN;
SELECT SN FROM partition_scan_stu_info1 WHERE SN > 40 ORDER BY SN;
SELECT SN FROM partition_scan_stu_info1 ORDER BY SN;
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info1.SN > 40;
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40;
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40 AND partition_scan_stu_info1.SN > 40;

--IndexOnlyScan test
SET enable_seqscan = OFF;
SET enable_indexscan = ON;
SET enable_indexonlyscan = ON;
SET enable_bitmapscan = OFF;
SET enable_tidscan = OFF;
SET enable_sort = ON;
SET enable_material = OFF;
SET enable_nestloop = OFF;
SET enable_hashjoin = OFF;

EXPLAIN (COSTS OFF) SELECT SN FROM partition_scan_stu_info1 WHERE SN < 10 ORDER BY SN; --one psrtition: the first
EXPLAIN (COSTS OFF) SELECT SN FROM partition_scan_stu_info1 WHERE SN < 20 ORDER BY SN; --more than one partitions
EXPLAIN (COSTS OFF) SELECT SN FROM partition_scan_stu_info1 WHERE SN > 30 ORDER BY SN; --one psrtition: the last
EXPLAIN (COSTS OFF) SELECT SN FROM partition_scan_stu_info1 WHERE SN > 40 ORDER BY SN; -- no partition
EXPLAIN (COSTS OFF) SELECT SN FROM partition_scan_stu_info1 ORDER BY SN; -- all partitions
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info1.SN > 40; 
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40;
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40 AND partition_scan_stu_info1.SN > 40;
SELECT SN FROM partition_scan_stu_info1 WHERE SN < 10 ORDER BY SN;
SELECT SN FROM partition_scan_stu_info1 WHERE SN < 20 ORDER BY SN;
SELECT SN FROM partition_scan_stu_info1 WHERE SN < 30 ORDER BY SN;
SELECT SN FROM partition_scan_stu_info1 WHERE SN > 40 ORDER BY SN;
SELECT SN FROM partition_scan_stu_info1 ORDER BY SN;
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info1.SN > 40;
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40;
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40 AND partition_scan_stu_info1.SN > 40;

--BitmapHeapScan test
SET enable_seqscan = OFF;
SET enable_indexscan = OFF;
SET enable_indexonlyscan = OFF;
SET enable_bitmapscan = ON;
SET enable_tidscan = OFF;
SET enable_sort = ON;
SET enable_material = OFF;
SET enable_nestloop = OFF;
SET enable_hashjoin = OFF;

EXPLAIN (COSTS OFF) SELECT SN FROM partition_scan_stu_info1 WHERE SN < 10 ORDER BY SN; --one psrtition: the first
EXPLAIN (COSTS OFF) SELECT SN FROM partition_scan_stu_info1 WHERE SN < 20 ORDER BY SN; --more than one partitions
EXPLAIN (COSTS OFF) SELECT SN FROM partition_scan_stu_info1 WHERE SN > 30 ORDER BY SN; --one psrtition: the last
EXPLAIN (COSTS OFF) SELECT SN FROM partition_scan_stu_info1 WHERE SN > 40 ORDER BY SN; -- no partition
EXPLAIN (COSTS OFF) SELECT SN FROM partition_scan_stu_info1 ORDER BY SN; -- all partitions
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info1.SN > 40; 
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40;
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40 AND partition_scan_stu_info1.SN > 40;
SELECT SN FROM partition_scan_stu_info1 WHERE SN < 10 ORDER BY SN;
SELECT SN FROM partition_scan_stu_info1 WHERE SN < 20 ORDER BY SN;
SELECT SN FROM partition_scan_stu_info1 WHERE SN < 30 ORDER BY SN;
SELECT SN FROM partition_scan_stu_info1 WHERE SN > 40 ORDER BY SN;
SELECT SN FROM partition_scan_stu_info1 ORDER BY SN;
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info1.SN > 40;
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40;
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40 AND partition_scan_stu_info1.SN > 40;

--TidScan test
SET enable_seqscan = OFF;
SET enable_indexscan = OFF;
SET enable_indexonlyscan = OFF;
SET enable_bitmapscan = OFF;
SET enable_tidscan = ON;
SET enable_sort = ON;
SET enable_material = OFF;
SET enable_nestloop = OFF;
SET enable_hashjoin = OFF;

EXPLAIN (COSTS OFF) SELECT SN FROM partition_scan_stu_info1 WHERE CTID = '(2,1)' ORDER BY SN;
EXPLAIN (COSTS OFF) SELECT SN FROM partition_scan_stu_info1 WHERE CTID = '(2,2)' ORDER BY SN;
SELECT CTID , * FROM partition_scan_stu_info1 ORDER BY SN;
SELECT SN FROM partition_scan_stu_info1 WHERE CTID = '(2,1)' ORDER BY SN;
SELECT SN FROM partition_scan_stu_info1 WHERE CTID = '(2,2)' ORDER BY SN;

-- Disables the planner's use of partitionwisejoin plans
SET ENABLE_PARTITIONWISE = FALSE;

--
----hashjoin + seqscan
--
SET enable_seqscan = ON;
SET enable_indexscan = OFF;
SET enable_indexonlyscan = OFF;
SET enable_bitmapscan = OFF;
SET enable_tidscan = OFF;
SET enable_sort = ON;
SET enable_material = OFF;
SET enable_nestloop = OFF;
SET enable_mergejoin = OFF;
SET enable_hashjoin = ON;

-- explain cross join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 CROSS JOIN partition_scan_stu_info2;
-- explain inner join 
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 INNER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain left outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain right outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 RIGHT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain full outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 FULL OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain for no partition to join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info1.SN > 40; 
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40;
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40 AND partition_scan_stu_info1.SN > 40;
-- rsult of cross join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 CROSS JOIN partition_scan_stu_info2 order by 1, 2;
-- rsult of inner joi
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 INNER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of left join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of right join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 RIGHT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of FULL join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 FULL OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- result of no partition to join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info1.SN > 40;
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40;
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40 AND partition_scan_stu_info1.SN > 40;

--
----hashjoin + indexscan
--
SET enable_seqscan = OFF;
SET enable_indexscan = ON;
SET enable_indexonlyscan = OFF;
SET enable_bitmapscan = OFF;
SET enable_tidscan = OFF;
SET enable_sort = ON;
SET enable_material = OFF;
SET enable_nestloop = OFF;
SET enable_mergejoin = OFF;
SET enable_hashjoin = ON;

--
---- explain cross join
--
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 CROSS JOIN partition_scan_stu_info2;
-- explain inner join 
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 INNER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain left outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain right outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 RIGHT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain full outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 FULL OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain for no partition to join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info1.SN > 40; 
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40;
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40 AND partition_scan_stu_info1.SN > 40;
-- rsult of cross join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 CROSS JOIN partition_scan_stu_info2 order by 1, 2;
-- rsult of inner joi
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 INNER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of left join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of right join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 RIGHT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of FULL join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 FULL OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- result of no partition to join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info1.SN > 40;
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40;
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40 AND partition_scan_stu_info1.SN > 40;

--
--hashjoin + indexonlyscan
--
SET enable_seqscan = OFF;
SET enable_indexscan = OFF;
SET enable_indexonlyscan = ON;
SET enable_bitmapscan = OFF;
SET enable_tidscan = OFF;
SET enable_sort = ON;
SET enable_material = OFF;
SET enable_nestloop = OFF;
SET enable_mergejoin = OFF;
SET enable_hashjoin = ON;

-- explain cross join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 CROSS JOIN partition_scan_stu_info2;
-- explain inner join 
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 INNER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain left outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain right outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 RIGHT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain full outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 FULL OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain for no partition to join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info1.SN > 40; 
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40;
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40 AND partition_scan_stu_info1.SN > 40;
-- rsult of cross join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 CROSS JOIN partition_scan_stu_info2 order by 1, 2;
-- rsult of inner joi
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 INNER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of left join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of right join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 RIGHT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of FULL join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 FULL OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- result of no partition to join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info1.SN > 40;
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40;
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40 AND partition_scan_stu_info1.SN > 40;

--
---- hashjoin + bitmapscan
--
SET enable_seqscan = OFF;
SET enable_indexscan = OFF;
SET enable_indexonlyscan = OFF;
SET enable_bitmapscan = ON;
SET enable_tidscan = OFF;
SET enable_mergejoin = OFF;
SET enable_sort = ON;
SET enable_material = OFF;
SET enable_nestloop = OFF;
SET enable_hashjoin = ON;

-- explain cross join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 CROSS JOIN partition_scan_stu_info2;
-- explain inner join 
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 INNER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain left outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain right outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 RIGHT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain full outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 FULL OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain for no partition to join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info1.SN > 40; 
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40;
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40 AND partition_scan_stu_info1.SN > 40;
-- rsult of cross join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 CROSS JOIN partition_scan_stu_info2 order by 1, 2;
-- rsult of inner joi
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 INNER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of left join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of right join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 RIGHT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of FULL join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 FULL OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- result of no partition to join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info1.SN > 40;
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40;
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40 AND partition_scan_stu_info1.SN > 40;

--
---- hashjoin + tidscan
--
SET enable_seqscan = OFF;
SET enable_indexscan = OFF;
SET enable_indexonlyscan = OFF;
SET enable_bitmapscan = OFF;
SET enable_tidscan = ON;
SET enable_sort = ON;
SET enable_material = OFF;
SET enable_nestloop = OFF;
SET enable_mergejoin = OFF;
SET enable_hashjoin = ON;

-- explain cross join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 CROSS JOIN partition_scan_stu_info2;
-- explain inner join 
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 INNER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain left outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain right outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 RIGHT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain full outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 FULL OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain for no partition to join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info1.SN > 40; 
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40;
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40 AND partition_scan_stu_info1.SN > 40;
-- rsult of cross join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 CROSS JOIN partition_scan_stu_info2 order by 1, 2;
-- rsult of inner joi
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 INNER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of left join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of right join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 RIGHT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of FULL join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 FULL OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- result of no partition to join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info1.SN > 40;
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40;
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40 AND partition_scan_stu_info1.SN > 40;

--
----nestloop + seqscan
--
SET enable_seqscan = ON;
SET enable_indexscan = OFF;
SET enable_indexonlyscan = OFF;
SET enable_bitmapscan = OFF;
SET enable_tidscan = OFF;
SET enable_sort = ON;
SET enable_material = OFF;
SET enable_nestloop = ON;
SET enable_mergejoin = OFF;
SET enable_hashjoin = OFF;

-- explain cross join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 CROSS JOIN partition_scan_stu_info2;
-- explain inner join 
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 INNER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain left outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain right outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 RIGHT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain full outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 FULL OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain for no partition to join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info1.SN > 40; 
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40;
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40 AND partition_scan_stu_info1.SN > 40;
-- rsult of cross join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 CROSS JOIN partition_scan_stu_info2 order by 1, 2;
-- rsult of inner joi
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 INNER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of left join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of right join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 RIGHT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of FULL join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 FULL OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- result of no partition to join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info1.SN > 40;
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40;
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40 AND partition_scan_stu_info1.SN > 40;

--
----nestloop + indexscan
--
SET enable_seqscan = OFF;
SET enable_indexscan = ON;
SET enable_indexonlyscan = OFF;
SET enable_bitmapscan = OFF;
SET enable_tidscan = OFF;
SET enable_sort = ON;
SET enable_material = OFF;
SET enable_nestloop = ON;
SET enable_mergejoin = OFF;
SET enable_hashjoin = OFF;

--
---- explain cross join
--
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 CROSS JOIN partition_scan_stu_info2;
-- explain inner join 
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 INNER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain left outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain right outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 RIGHT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain full outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 FULL OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain for no partition to join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info1.SN > 40; 
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40;
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40 AND partition_scan_stu_info1.SN > 40;
-- rsult of cross join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 CROSS JOIN partition_scan_stu_info2 order by 1, 2;
-- rsult of inner joi
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 INNER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of left join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of right join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 RIGHT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of FULL join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 FULL OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- result of no partition to join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info1.SN > 40;
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40;
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40 AND partition_scan_stu_info1.SN > 40;

--
--nestloop + indexonlyscan
--
SET enable_seqscan = OFF;
SET enable_indexscan = OFF;
SET enable_indexonlyscan = ON;
SET enable_bitmapscan = OFF;
SET enable_tidscan = OFF;
SET enable_sort = ON;
SET enable_material = OFF;
SET enable_nestloop = ON;
SET enable_mergejoin = OFF;
SET enable_hashjoin = OFF;

-- explain cross join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 CROSS JOIN partition_scan_stu_info2;
-- explain inner join 
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 INNER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain left outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain right outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 RIGHT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain full outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 FULL OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain for no partition to join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info1.SN > 40; 
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40;
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40 AND partition_scan_stu_info1.SN > 40;
-- rsult of cross join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 CROSS JOIN partition_scan_stu_info2 order by 1, 2;
-- rsult of inner joi
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 INNER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of left join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of right join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 RIGHT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of FULL join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 FULL OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- result of no partition to join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info1.SN > 40;
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40;
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40 AND partition_scan_stu_info1.SN > 40;

--
---- nestloop + bitmapscan
--
SET enable_seqscan = OFF;
SET enable_indexscan = OFF;
SET enable_indexonlyscan = OFF;
SET enable_bitmapscan = ON;
SET enable_tidscan = OFF;
SET enable_sort = ON;
SET enable_material = OFF;
SET enable_nestloop = ON;
SET enable_mergejoin = OFF;
SET enable_hashjoin = OFF;

-- explain cross join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 CROSS JOIN partition_scan_stu_info2;
-- explain inner join 
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 INNER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain left outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain right outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 RIGHT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain full outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 FULL OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain for no partition to join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info1.SN > 40; 
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40;
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40 AND partition_scan_stu_info1.SN > 40;
-- rsult of cross join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 CROSS JOIN partition_scan_stu_info2 order by 1, 2;
-- rsult of inner joi
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 INNER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of left join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of right join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 RIGHT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of FULL join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 FULL OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- result of no partition to join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info1.SN > 40;
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40;
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40 AND partition_scan_stu_info1.SN > 40;

--
---- nestloop + tidscan
--
SET enable_seqscan = OFF;
SET enable_indexscan = OFF;
SET enable_indexonlyscan = OFF;
SET enable_bitmapscan = OFF;
SET enable_tidscan = ON;
SET enable_sort = ON;
SET enable_material = OFF;
SET enable_nestloop = ON;
SET enable_mergejoin = OFF;
SET enable_hashjoin = OFF;

-- explain cross join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 CROSS JOIN partition_scan_stu_info2;
-- explain inner join 
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 INNER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain left outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain right outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 RIGHT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain full outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 FULL OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain for no partition to join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info1.SN > 40; 
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40;
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40 AND partition_scan_stu_info1.SN > 40;
-- rsult of cross join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 CROSS JOIN partition_scan_stu_info2 order by 1, 2;
-- rsult of inner joi
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 INNER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of left join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of right join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 RIGHT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of FULL join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 FULL OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- result of no partition to join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info1.SN > 40;
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40;
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40 AND partition_scan_stu_info1.SN > 40;

--
----mergejoin + seqscan
--
SET enable_seqscan = ON;
SET enable_indexscan = OFF;
SET enable_indexonlyscan = OFF;
SET enable_bitmapscan = OFF;
SET enable_tidscan = OFF;
SET enable_sort = ON;
SET enable_material = OFF;
SET enable_nestloop = OFF;
SET enable_mergejoin = ON;
SET enable_hashjoin = OFF;

-- explain cross join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 CROSS JOIN partition_scan_stu_info2;
-- explain inner join 
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 INNER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain left outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain right outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 RIGHT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain full outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 FULL OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain for no partition to join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info1.SN > 40; 
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40;
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40 AND partition_scan_stu_info1.SN > 40;
-- rsult of cross join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 CROSS JOIN partition_scan_stu_info2 order by 1, 2;
-- rsult of inner joi
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 INNER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of left join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of right join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 RIGHT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of FULL join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 FULL OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- result of no partition to join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info1.SN > 40;
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40;
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40 AND partition_scan_stu_info1.SN > 40;

--
----mergejoin + indexscan
--
SET enable_seqscan = OFF;
SET enable_indexscan = ON;
SET enable_indexonlyscan = OFF;
SET enable_bitmapscan = OFF;
SET enable_tidscan = OFF;
SET enable_sort = ON;
SET enable_material = OFF;
SET enable_nestloop = OFF;
SET enable_mergejoin = ON;
SET enable_hashjoin = OFF;

--
---- explain cross join
--
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 CROSS JOIN partition_scan_stu_info2;
-- explain inner join 
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 INNER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain left outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain right outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 RIGHT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain full outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 FULL OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain for no partition to join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info1.SN > 40; 
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40;
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40 AND partition_scan_stu_info1.SN > 40;
-- rsult of cross join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 CROSS JOIN partition_scan_stu_info2 order by 1, 2;
-- rsult of inner joi
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 INNER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of left join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of right join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 RIGHT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of FULL join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 FULL OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- result of no partition to join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info1.SN > 40;
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40;
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40 AND partition_scan_stu_info1.SN > 40;

--
--mergejoin + indexonlyscan
--
SET enable_seqscan = OFF;
SET enable_indexscan = OFF;
SET enable_indexonlyscan = ON;
SET enable_bitmapscan = OFF;
SET enable_tidscan = OFF;
SET enable_sort = ON;
SET enable_material = OFF;
SET enable_nestloop = OFF;
SET enable_mergejoin = ON;
SET enable_hashjoin = OFF;

-- explain cross join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 CROSS JOIN partition_scan_stu_info2;
-- explain inner join 
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 INNER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain left outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain right outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 RIGHT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain full outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 FULL OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain for no partition to join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info1.SN > 40; 
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40;
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40 AND partition_scan_stu_info1.SN > 40;
-- rsult of cross join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 CROSS JOIN partition_scan_stu_info2 order by 1, 2;
-- rsult of inner joi
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 INNER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of left join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of right join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 RIGHT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of FULL join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 FULL OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- result of no partition to join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info1.SN > 40;
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40;
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40 AND partition_scan_stu_info1.SN > 40;

--
---- mergejoin + bitmapscan
--
SET enable_seqscan = OFF;
SET enable_indexscan = OFF;
SET enable_indexonlyscan = OFF;
SET enable_bitmapscan = ON;
SET enable_tidscan = OFF;
SET enable_sort = ON;
SET enable_material = OFF;
SET enable_nestloop = OFF;
SET enable_mergejoin = ON;
SET enable_hashjoin = OFF;

-- explain cross join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 CROSS JOIN partition_scan_stu_info2;
-- explain inner join 
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 INNER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain left outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain right outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 RIGHT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain full outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 FULL OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain for no partition to join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info1.SN > 40; 
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40;
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40 AND partition_scan_stu_info1.SN > 40;
-- rsult of cross join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 CROSS JOIN partition_scan_stu_info2 order by 1, 2;
-- rsult of inner joi
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 INNER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of left join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of right join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 RIGHT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of FULL join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 FULL OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- result of no partition to join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info1.SN > 40;
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40;
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40 AND partition_scan_stu_info1.SN > 40;

--
---- mergejoin + tidscan
--
SET enable_seqscan = OFF;
SET enable_indexscan = OFF;
SET enable_indexonlyscan = OFF;
SET enable_bitmapscan = OFF;
SET enable_tidscan = ON;
SET enable_sort = ON;
SET enable_material = OFF;
SET enable_nestloop = OFF;
SET enable_mergejoin = ON;
SET enable_hashjoin = OFF;

-- explain cross join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 CROSS JOIN partition_scan_stu_info2;
-- explain inner join 
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 INNER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain left outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain right outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 RIGHT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain full outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 FULL OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain for no partition to join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info1.SN > 40; 
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40;
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40 AND partition_scan_stu_info1.SN > 40;
-- rsult of cross join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 CROSS JOIN partition_scan_stu_info2 order by 1, 2;
-- rsult of inner joi
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 INNER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of left join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of right join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 RIGHT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of FULL join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 FULL OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- result of no partition to join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info1.SN > 40;
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40;
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40 AND partition_scan_stu_info1.SN > 40;

--
---- all + seqscan
--
SET enable_seqscan = ON;
SET enable_indexscan = OFF;
SET enable_indexonlyscan = OFF;
SET enable_bitmapscan = OFF;
SET enable_tidscan = OFF;
SET enable_sort = ON;
SET enable_material = OFF;
SET enable_nestloop = ON;
SET enable_mergejoin = ON;
SET enable_hashjoin = ON;

-- explain cross join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 CROSS JOIN partition_scan_stu_info2;
-- explain inner join 
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 INNER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain left outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain right outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 RIGHT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain full outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 FULL OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain for no partition to join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info1.SN > 40; 
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40;
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40 AND partition_scan_stu_info1.SN > 40;
-- rsult of cross join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 CROSS JOIN partition_scan_stu_info2 order by 1, 2;
-- rsult of inner joi
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 INNER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of left join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of right join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 RIGHT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of FULL join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 FULL OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- result of no partition to join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info1.SN > 40;
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40;
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40 AND partition_scan_stu_info1.SN > 40;

--
---- all + indexscan
--
SET enable_seqscan = OFF;
SET enable_indexscan = ON;
SET enable_indexonlyscan = OFF;
SET enable_bitmapscan = OFF;
SET enable_tidscan = OFF;
SET enable_sort = ON;
SET enable_material = OFF;
SET enable_nestloop = ON;
SET enable_mergejoin = ON;
SET enable_hashjoin = ON;

--
---- explain cross join
--
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 CROSS JOIN partition_scan_stu_info2;
-- explain inner join 
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 INNER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain left outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain right outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 RIGHT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain full outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 FULL OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain for no partition to join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info1.SN > 40; 
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40;
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40 AND partition_scan_stu_info1.SN > 40;
-- rsult of cross join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 CROSS JOIN partition_scan_stu_info2 order by 1, 2;
-- rsult of inner joi
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 INNER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of left join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of right join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 RIGHT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of FULL join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 FULL OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain for no partition to join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info1.SN > 40; 
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40;
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40 AND partition_scan_stu_info1.SN > 40;
-- result of no partition to join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info1.SN > 40;
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40;
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40 AND partition_scan_stu_info1.SN > 40;

--
-- all + indexonlyscan
--
SET enable_seqscan = OFF;
SET enable_indexscan = OFF;
SET enable_indexonlyscan = ON;
SET enable_bitmapscan = OFF;
SET enable_tidscan = OFF;
SET enable_sort = ON;
SET enable_material = OFF;
SET enable_nestloop = ON;
SET enable_mergejoin = ON;
SET enable_hashjoin = ON;

-- explain cross join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 CROSS JOIN partition_scan_stu_info2;
-- explain inner join 
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 INNER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain left outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain right outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 RIGHT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain full outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 FULL OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain for no partition to join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info1.SN > 40; 
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40;
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40 AND partition_scan_stu_info1.SN > 40;
-- rsult of cross join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 CROSS JOIN partition_scan_stu_info2 order by 1, 2;
-- rsult of inner joi
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 INNER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of left join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of right join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 RIGHT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of FULL join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 FULL OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain for no partition to join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info1.SN > 40; 
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40;
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40 AND partition_scan_stu_info1.SN > 40;
-- result of no partition to join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info1.SN > 40;
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40;
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40 AND partition_scan_stu_info1.SN > 40;

--
---- all + bitmapscan
--
SET enable_seqscan = OFF;
SET enable_indexscan = OFF;
SET enable_indexonlyscan = OFF;
SET enable_bitmapscan = ON;
SET enable_tidscan = OFF;
SET enable_sort = ON;
SET enable_material = OFF;
SET enable_nestloop = ON;
SET enable_mergejoin = ON;
SET enable_hashjoin = ON;

-- explain cross join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 CROSS JOIN partition_scan_stu_info2;
-- explain inner join 
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 INNER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain left outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain right outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 RIGHT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain full outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 FULL OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain for no partition to join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info1.SN > 40; 
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40;
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40 AND partition_scan_stu_info1.SN > 40;
-- rsult of cross join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 CROSS JOIN partition_scan_stu_info2 order by 1, 2;
-- rsult of inner joi
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 INNER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of left join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of right join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 RIGHT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of FULL join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 FULL OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain for no partition to join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info1.SN > 40; 
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40;
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40 AND partition_scan_stu_info1.SN > 40;
-- result of no partition to join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info1.SN > 40;
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40;
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40 AND partition_scan_stu_info1.SN > 40;

--
---- all + tidscan
--
SET enable_seqscan = OFF;
SET enable_indexscan = OFF;
SET enable_indexonlyscan = OFF;
SET enable_bitmapscan = OFF;
SET enable_tidscan = ON;
SET enable_sort = ON;
SET enable_material = OFF;
SET enable_nestloop = ON;
SET enable_mergejoin = ON;
SET enable_hashjoin = ON;

-- explain cross join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 CROSS JOIN partition_scan_stu_info2;
-- explain inner join 
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 INNER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain left outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain right outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 RIGHT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain full outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 FULL OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain for no partition to join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info1.SN > 40; 
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40;
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40 AND partition_scan_stu_info1.SN > 40;
-- rsult of cross join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 CROSS JOIN partition_scan_stu_info2 order by 1, 2;
-- rsult of inner joi
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 INNER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of left join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of right join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 RIGHT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of FULL join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 FULL OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- result of no partition to join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info1.SN > 40;
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40;
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40 AND partition_scan_stu_info1.SN > 40;

--
---- all is open
--
SET enable_seqscan = ON;
SET enable_indexscan = ON;
SET enable_indexonlyscan = ON;
SET enable_bitmapscan = ON;
SET enable_tidscan = ON;
SET enable_sort = ON;
SET enable_material = OFF;
SET enable_nestloop = ON;
SET enable_mergejoin = ON;
SET enable_hashjoin = ON;

-- explain cross join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 CROSS JOIN partition_scan_stu_info2;
-- explain inner join 
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 INNER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain left outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain right outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 RIGHT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain full outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 FULL OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain for no partition to join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info1.SN > 40; 
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40;
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40 AND partition_scan_stu_info1.SN > 40;
-- rsult of cross join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 CROSS JOIN partition_scan_stu_info2 order by 1, 2;
-- rsult of inner joi
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 INNER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of left join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of right join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 RIGHT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of FULL join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 FULL OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- result of no partition to join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info1.SN > 40;
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40;
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40 AND partition_scan_stu_info1.SN > 40;

-- clean up
DROP TABLE partition_scan_stu_info1;
DROP TABLE partition_scan_stu_info2;

--- case 2: ordinary table + partitioned table
--prepare
CREATE TABLE partition_scan_stu_info1(SN INT, NAME NAME)
PARTITION BY RANGE (SN)
(
	PARTITION P1_partition_scan_stu_info1 VALUES LESS THAN(10),
	PARTITION P2_partition_scan_stu_info1 VALUES LESS THAN(20),
	PARTITION P3_partition_scan_stu_info1 VALUES LESS THAN(30),
	PARTITION P4_partition_scan_stu_info1 VALUES LESS THAN(40)
);

CREATE TABLE partition_scan_stu_info2(SN INT, PHONE NAME);

CREATE UNIQUE INDEX index_on_partition_scan_stu_info1 ON partition_scan_stu_info1 (SN) LOCAL;

CREATE UNIQUE INDEX index_on_partition_scan_stu_info2 ON partition_scan_stu_info2 (SN);

INSERT INTO partition_scan_stu_info1 VALUES (1,'DFM');
INSERT INTO partition_scan_stu_info1 VALUES (11,'CHAO');
INSERT INTO partition_scan_stu_info1 VALUES (21,'ZJR');
INSERT INTO partition_scan_stu_info1 VALUES (31,'JYH');

INSERT INTO partition_scan_stu_info2 VALUES (1,'15478523126');
INSERT INTO partition_scan_stu_info2 VALUES (11,'15236997586');
INSERT INTO partition_scan_stu_info2 VALUES (21,'15936985364');
INSERT INTO partition_scan_stu_info2 VALUES (31,'15873285556');

SELECT * FROM partition_scan_stu_info1 order by 1, 2;
SELECT * FROM partition_scan_stu_info2 order by 1, 2;

--
----hashjoin + seqscan
--
SET enable_seqscan = ON;
SET enable_indexscan = OFF;
SET enable_indexonlyscan = OFF;
SET enable_bitmapscan = OFF;
SET enable_tidscan = OFF;
SET enable_sort = ON;
SET enable_material = OFF;
SET enable_nestloop = OFF;
SET enable_mergejoin = OFF;
SET enable_hashjoin = ON;

-- explain cross join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 CROSS JOIN partition_scan_stu_info2;
-- explain inner join 
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 INNER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain left outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain right outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 RIGHT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain full outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 FULL OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain for no partition to join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info1.SN > 40; 
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40;
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40 AND partition_scan_stu_info1.SN > 40;
-- rsult of cross join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 CROSS JOIN partition_scan_stu_info2 order by 1, 2;
-- rsult of inner joi
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 INNER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of left join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of right join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 RIGHT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of FULL join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 FULL OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- result of no partition to join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info1.SN > 40;
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40;
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40 AND partition_scan_stu_info1.SN > 40;

--
----hashjoin + indexscan
--
SET enable_seqscan = OFF;
SET enable_indexscan = ON;
SET enable_indexonlyscan = OFF;
SET enable_bitmapscan = OFF;
SET enable_tidscan = OFF;
SET enable_sort = ON;
SET enable_material = OFF;
SET enable_nestloop = OFF;
SET enable_mergejoin = OFF;
SET enable_hashjoin = ON;

--
---- explain cross join
--
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 CROSS JOIN partition_scan_stu_info2;
-- explain inner join 
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 INNER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain left outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain right outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 RIGHT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain full outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 FULL OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain for no partition to join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info1.SN > 40; 
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40;
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40 AND partition_scan_stu_info1.SN > 40;
-- rsult of cross join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 CROSS JOIN partition_scan_stu_info2 order by 1, 2;
-- rsult of inner joi
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 INNER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of left join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of right join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 RIGHT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of FULL join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 FULL OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain for no partition to join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info1.SN > 40; 
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40;
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40 AND partition_scan_stu_info1.SN > 40;
-- result of no partition to join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info1.SN > 40;
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40;
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40 AND partition_scan_stu_info1.SN > 40;

--
--hashjoin + indexonlyscan
--
SET enable_seqscan = OFF;
SET enable_indexscan = OFF;
SET enable_indexonlyscan = ON;
SET enable_bitmapscan = OFF;
SET enable_tidscan = OFF;
SET enable_sort = ON;
SET enable_material = OFF;
SET enable_nestloop = OFF;
SET enable_mergejoin = OFF;
SET enable_hashjoin = ON;

-- explain cross join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 CROSS JOIN partition_scan_stu_info2;
-- explain inner join 
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 INNER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain left outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain right outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 RIGHT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain full outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 FULL OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain for no partition to join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info1.SN > 40; 
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40;
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40 AND partition_scan_stu_info1.SN > 40;
-- rsult of cross join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 CROSS JOIN partition_scan_stu_info2 order by 1, 2;
-- rsult of inner joi
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 INNER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of left join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of right join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 RIGHT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of FULL join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 FULL OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- result of no partition to join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info1.SN > 40;
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40;
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40 AND partition_scan_stu_info1.SN > 40;

--
---- hashjoin + bitmapscan
--
SET enable_seqscan = OFF;
SET enable_indexscan = OFF;
SET enable_indexonlyscan = OFF;
SET enable_bitmapscan = ON;
SET enable_tidscan = OFF;
SET enable_mergejoin = OFF;
SET enable_sort = ON;
SET enable_material = OFF;
SET enable_nestloop = OFF;
SET enable_hashjoin = ON;

-- explain cross join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 CROSS JOIN partition_scan_stu_info2;
-- explain inner join 
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 INNER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain left outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain right outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 RIGHT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain full outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 FULL OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain for no partition to join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info1.SN > 40; 
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40;
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40 AND partition_scan_stu_info1.SN > 40;
-- rsult of cross join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 CROSS JOIN partition_scan_stu_info2 order by 1, 2;
-- rsult of inner joi
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 INNER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of left join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of right join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 RIGHT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of FULL join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 FULL OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain for no partition to join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info1.SN > 40; 
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40;
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40 AND partition_scan_stu_info1.SN > 40;
-- result of no partition to join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info1.SN > 40;
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40;
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40 AND partition_scan_stu_info1.SN > 40;

--
---- hashjoin + tidscan
--
SET enable_seqscan = OFF;
SET enable_indexscan = OFF;
SET enable_indexonlyscan = OFF;
SET enable_bitmapscan = OFF;
SET enable_tidscan = ON;
SET enable_sort = ON;
SET enable_material = OFF;
SET enable_nestloop = OFF;
SET enable_mergejoin = OFF;
SET enable_hashjoin = ON;

-- explain cross join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 CROSS JOIN partition_scan_stu_info2;
-- explain inner join 
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 INNER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain left outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain right outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 RIGHT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain full outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 FULL OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain for no partition to join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info1.SN > 40; 
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40;
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40 AND partition_scan_stu_info1.SN > 40;
-- rsult of cross join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 CROSS JOIN partition_scan_stu_info2 order by 1, 2;
-- rsult of inner joi
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 INNER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of left join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of right join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 RIGHT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of FULL join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 FULL OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- result of no partition to join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info1.SN > 40;
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40;
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40 AND partition_scan_stu_info1.SN > 40;

--
----nestloop + seqscan
--
SET enable_seqscan = ON;
SET enable_indexscan = OFF;
SET enable_indexonlyscan = OFF;
SET enable_bitmapscan = OFF;
SET enable_tidscan = OFF;
SET enable_sort = ON;
SET enable_material = OFF;
SET enable_nestloop = ON;
SET enable_mergejoin = OFF;
SET enable_hashjoin = OFF;

-- explain cross join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 CROSS JOIN partition_scan_stu_info2;
-- explain inner join 
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 INNER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain left outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain right outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 RIGHT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain full outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 FULL OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain for no partition to join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info1.SN > 40; 
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40;
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40 AND partition_scan_stu_info1.SN > 40;
-- rsult of cross join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 CROSS JOIN partition_scan_stu_info2 order by 1, 2;
-- rsult of inner joi
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 INNER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of left join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of right join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 RIGHT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of FULL join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 FULL OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- result of no partition to join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info1.SN > 40;
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40;
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40 AND partition_scan_stu_info1.SN > 40;

--
----nestloop + indexscan
--
SET enable_seqscan = OFF;
SET enable_indexscan = ON;
SET enable_indexonlyscan = OFF;
SET enable_bitmapscan = OFF;
SET enable_tidscan = OFF;
SET enable_sort = ON;
SET enable_material = OFF;
SET enable_nestloop = ON;
SET enable_mergejoin = OFF;
SET enable_hashjoin = OFF;

--
---- explain cross join
--
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 CROSS JOIN partition_scan_stu_info2;
-- explain inner join 
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 INNER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain left outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain right outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 RIGHT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain full outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 FULL OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain for no partition to join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info1.SN > 40; 
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40;
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40 AND partition_scan_stu_info1.SN > 40;
-- rsult of cross join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 CROSS JOIN partition_scan_stu_info2 order by 1, 2;
-- rsult of inner joi
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 INNER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of left join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of right join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 RIGHT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of FULL join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 FULL OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- result of no partition to join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info1.SN > 40;
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40;
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40 AND partition_scan_stu_info1.SN > 40;

--
--nestloop + indexonlyscan
--
SET enable_seqscan = OFF;
SET enable_indexscan = OFF;
SET enable_indexonlyscan = ON;
SET enable_bitmapscan = OFF;
SET enable_tidscan = OFF;
SET enable_sort = ON;
SET enable_material = OFF;
SET enable_nestloop = ON;
SET enable_mergejoin = OFF;
SET enable_hashjoin = OFF;

-- explain cross join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 CROSS JOIN partition_scan_stu_info2;
-- explain inner join 
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 INNER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain left outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain right outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 RIGHT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain full outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 FULL OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain for no partition to join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info1.SN > 40; 
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40;
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40 AND partition_scan_stu_info1.SN > 40;
-- rsult of cross join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 CROSS JOIN partition_scan_stu_info2 order by 1, 2;
-- rsult of inner joi
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 INNER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of left join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of right join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 RIGHT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of FULL join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 FULL OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- result of no partition to join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info1.SN > 40;
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40;
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40 AND partition_scan_stu_info1.SN > 40;

--
---- nestloop + bitmapscan
--
SET enable_seqscan = OFF;
SET enable_indexscan = OFF;
SET enable_indexonlyscan = OFF;
SET enable_bitmapscan = ON;
SET enable_tidscan = OFF;
SET enable_sort = ON;
SET enable_material = OFF;
SET enable_nestloop = ON;
SET enable_mergejoin = OFF;
SET enable_hashjoin = OFF;

-- explain cross join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 CROSS JOIN partition_scan_stu_info2;
-- explain inner join 
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 INNER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain left outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain right outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 RIGHT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain full outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 FULL OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain for no partition to join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info1.SN > 40; 
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40;
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40 AND partition_scan_stu_info1.SN > 40;
-- rsult of cross join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 CROSS JOIN partition_scan_stu_info2 order by 1, 2;
-- rsult of inner joi
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 INNER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of left join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of right join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 RIGHT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of FULL join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 FULL OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- result of no partition to join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info1.SN > 40;
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40;
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40 AND partition_scan_stu_info1.SN > 40;

--
---- nestloop + tidscan
--
SET enable_seqscan = OFF;
SET enable_indexscan = OFF;
SET enable_indexonlyscan = OFF;
SET enable_bitmapscan = OFF;
SET enable_tidscan = ON;
SET enable_sort = ON;
SET enable_material = OFF;
SET enable_nestloop = ON;
SET enable_mergejoin = OFF;
SET enable_hashjoin = OFF;

-- explain cross join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 CROSS JOIN partition_scan_stu_info2;
-- explain inner join 
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 INNER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain left outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain right outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 RIGHT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain full outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 FULL OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain for no partition to join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info1.SN > 40; 
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40;
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40 AND partition_scan_stu_info1.SN > 40;
-- rsult of cross join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 CROSS JOIN partition_scan_stu_info2 order by 1, 2;
-- rsult of inner joi
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 INNER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of left join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of right join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 RIGHT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of FULL join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 FULL OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- result of no partition to join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info1.SN > 40;
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40;
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40 AND partition_scan_stu_info1.SN > 40;

--
----mergejoin + seqscan
--
SET enable_seqscan = ON;
SET enable_indexscan = OFF;
SET enable_indexonlyscan = OFF;
SET enable_bitmapscan = OFF;
SET enable_tidscan = OFF;
SET enable_sort = ON;
SET enable_material = OFF;
SET enable_nestloop = OFF;
SET enable_mergejoin = ON;
SET enable_hashjoin = OFF;

-- explain cross join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 CROSS JOIN partition_scan_stu_info2;
-- explain inner join 
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 INNER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain left outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain right outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 RIGHT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain full outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 FULL OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain for no partition to join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info1.SN > 40; 
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40;
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40 AND partition_scan_stu_info1.SN > 40;
-- rsult of cross join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 CROSS JOIN partition_scan_stu_info2 order by 1, 2;
-- rsult of inner joi
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 INNER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of left join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of right join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 RIGHT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of FULL join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 FULL OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- result of no partition to join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info1.SN > 40;
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40;
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40 AND partition_scan_stu_info1.SN > 40;

--
----mergejoin + indexscan
--
SET enable_seqscan = OFF;
SET enable_indexscan = ON;
SET enable_indexonlyscan = OFF;
SET enable_bitmapscan = OFF;
SET enable_tidscan = OFF;
SET enable_sort = ON;
SET enable_material = OFF;
SET enable_nestloop = OFF;
SET enable_mergejoin = ON;
SET enable_hashjoin = OFF;

--
---- explain cross join
--
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 CROSS JOIN partition_scan_stu_info2;
-- explain inner join 
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 INNER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain left outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain right outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 RIGHT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain full outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 FULL OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain for no partition to join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info1.SN > 40; 
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40;
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40 AND partition_scan_stu_info1.SN > 40;
-- rsult of cross join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 CROSS JOIN partition_scan_stu_info2 order by 1, 2;
-- rsult of inner joi
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 INNER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of left join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of right join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 RIGHT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of FULL join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 FULL OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- result of no partition to join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info1.SN > 40;
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40;
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40 AND partition_scan_stu_info1.SN > 40;

--
--mergejoin + indexonlyscan
--
SET enable_seqscan = OFF;
SET enable_indexscan = OFF;
SET enable_indexonlyscan = ON;
SET enable_bitmapscan = OFF;
SET enable_tidscan = OFF;
SET enable_sort = ON;
SET enable_material = OFF;
SET enable_nestloop = OFF;
SET enable_mergejoin = ON;
SET enable_hashjoin = OFF;

-- explain cross join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 CROSS JOIN partition_scan_stu_info2;
-- explain inner join 
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 INNER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain left outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain right outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 RIGHT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain full outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 FULL OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain for no partition to join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info1.SN > 40; 
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40;
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40 AND partition_scan_stu_info1.SN > 40;
-- rsult of cross join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 CROSS JOIN partition_scan_stu_info2 order by 1, 2;
-- rsult of inner joi
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 INNER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of left join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of right join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 RIGHT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of FULL join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 FULL OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- result of no partition to join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info1.SN > 40;
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40;
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40 AND partition_scan_stu_info1.SN > 40;

--
---- mergejoin + bitmapscan
--
SET enable_seqscan = OFF;
SET enable_indexscan = OFF;
SET enable_indexonlyscan = OFF;
SET enable_bitmapscan = ON;
SET enable_tidscan = OFF;
SET enable_sort = ON;
SET enable_material = OFF;
SET enable_nestloop = OFF;
SET enable_mergejoin = ON;
SET enable_hashjoin = OFF;

-- explain cross join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 CROSS JOIN partition_scan_stu_info2;
-- explain inner join 
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 INNER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain left outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain right outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 RIGHT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain full outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 FULL OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain for no partition to join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info1.SN > 40; 
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40;
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40 AND partition_scan_stu_info1.SN > 40;
-- rsult of cross join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 CROSS JOIN partition_scan_stu_info2 order by 1, 2;
-- rsult of inner joi
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 INNER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of left join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of right join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 RIGHT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of FULL join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 FULL OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- result of no partition to join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info1.SN > 40;
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40;
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40 AND partition_scan_stu_info1.SN > 40;

--
---- mergejoin + tidscan
--
SET enable_seqscan = OFF;
SET enable_indexscan = OFF;
SET enable_indexonlyscan = OFF;
SET enable_bitmapscan = OFF;
SET enable_tidscan = ON;
SET enable_sort = ON;
SET enable_material = OFF;
SET enable_nestloop = OFF;
SET enable_mergejoin = ON;
SET enable_hashjoin = OFF;

-- explain cross join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 CROSS JOIN partition_scan_stu_info2;
-- explain inner join 
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 INNER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain left outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain right outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 RIGHT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain full outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 FULL OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain for no partition to join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info1.SN > 40; 
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40;
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40 AND partition_scan_stu_info1.SN > 40;
-- rsult of cross join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 CROSS JOIN partition_scan_stu_info2 order by 1, 2;
-- rsult of inner joi
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 INNER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of left join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of right join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 RIGHT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of FULL join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 FULL OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- result of no partition to join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info1.SN > 40;
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40;
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40 AND partition_scan_stu_info1.SN > 40;

--
---- all + seqscan
--
SET enable_seqscan = ON;
SET enable_indexscan = OFF;
SET enable_indexonlyscan = OFF;
SET enable_bitmapscan = OFF;
SET enable_tidscan = OFF;
SET enable_sort = ON;
SET enable_material = OFF;
SET enable_nestloop = ON;
SET enable_mergejoin = ON;
SET enable_hashjoin = ON;

-- explain cross join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 CROSS JOIN partition_scan_stu_info2;
-- explain inner join 
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 INNER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain left outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain right outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 RIGHT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain full outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 FULL OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain for no partition to join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info1.SN > 40; 
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40;
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40 AND partition_scan_stu_info1.SN > 40;
-- rsult of cross join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 CROSS JOIN partition_scan_stu_info2 order by 1, 2;
-- rsult of inner joi
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 INNER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of left join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of right join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 RIGHT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of FULL join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 FULL OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- result of no partition to join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info1.SN > 40;
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40;
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40 AND partition_scan_stu_info1.SN > 40;

--
---- all + indexscan
--
SET enable_seqscan = OFF;
SET enable_indexscan = ON;
SET enable_indexonlyscan = OFF;
SET enable_bitmapscan = OFF;
SET enable_tidscan = OFF;
SET enable_sort = ON;
SET enable_material = OFF;
SET enable_nestloop = ON;
SET enable_mergejoin = ON;
SET enable_hashjoin = ON;

--
---- explain cross join
--
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 CROSS JOIN partition_scan_stu_info2;
-- explain inner join 
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 INNER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain left outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain right outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 RIGHT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain full outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 FULL OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain for no partition to join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info1.SN > 40; 
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40;
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40 AND partition_scan_stu_info1.SN > 40;
-- rsult of cross join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 CROSS JOIN partition_scan_stu_info2 order by 1, 2;
-- rsult of inner joi
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 INNER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of left join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of right join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 RIGHT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of FULL join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 FULL OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- result of no partition to join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info1.SN > 40;
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40;
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40 AND partition_scan_stu_info1.SN > 40;

--
-- all + indexonlyscan
--
SET enable_seqscan = OFF;
SET enable_indexscan = OFF;
SET enable_indexonlyscan = ON;
SET enable_bitmapscan = OFF;
SET enable_tidscan = OFF;
SET enable_sort = ON;
SET enable_material = OFF;
SET enable_nestloop = ON;
SET enable_mergejoin = ON;
SET enable_hashjoin = ON;

-- explain cross join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 CROSS JOIN partition_scan_stu_info2;
-- explain inner join 
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 INNER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain left outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain right outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 RIGHT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain full outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 FULL OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain for no partition to join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info1.SN > 40; 
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40;
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40 AND partition_scan_stu_info1.SN > 40;
-- rsult of cross join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 CROSS JOIN partition_scan_stu_info2 order by 1, 2;
-- rsult of inner joi
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 INNER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of left join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of right join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 RIGHT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of FULL join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 FULL OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- result of no partition to join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info1.SN > 40;
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40;
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40 AND partition_scan_stu_info1.SN > 40;

--
---- all + bitmapscan
--
SET enable_seqscan = OFF;
SET enable_indexscan = OFF;
SET enable_indexonlyscan = OFF;
SET enable_bitmapscan = ON;
SET enable_tidscan = OFF;
SET enable_sort = ON;
SET enable_material = OFF;
SET enable_nestloop = ON;
SET enable_mergejoin = ON;
SET enable_hashjoin = ON;

-- explain cross join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 CROSS JOIN partition_scan_stu_info2;
-- explain inner join 
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 INNER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain left outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain right outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 RIGHT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain full outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 FULL OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain for no partition to join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info1.SN > 40; 
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40;
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40 AND partition_scan_stu_info1.SN > 40;
-- rsult of cross join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 CROSS JOIN partition_scan_stu_info2 order by 1, 2;
-- rsult of inner joi
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 INNER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of left join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of right join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 RIGHT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of FULL join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 FULL OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- result of no partition to join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info1.SN > 40;
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40;
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40 AND partition_scan_stu_info1.SN > 40;

--
---- all + tidscan
--
SET enable_seqscan = OFF;
SET enable_indexscan = OFF;
SET enable_indexonlyscan = OFF;
SET enable_bitmapscan = OFF;
SET enable_tidscan = ON;
SET enable_sort = ON;
SET enable_material = OFF;
SET enable_nestloop = ON;
SET enable_mergejoin = ON;
SET enable_hashjoin = ON;

-- explain cross join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 CROSS JOIN partition_scan_stu_info2;
-- explain inner join 
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 INNER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain left outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain right outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 RIGHT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain full outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 FULL OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain for no partition to join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info1.SN > 40; 
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40;
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40 AND partition_scan_stu_info1.SN > 40;
-- rsult of cross join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 CROSS JOIN partition_scan_stu_info2 order by 1, 2;
-- rsult of inner joi
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 INNER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of left join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of right join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 RIGHT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of FULL join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 FULL OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- result of no partition to join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info1.SN > 40;
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40;
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40 AND partition_scan_stu_info1.SN > 40;

--
---- all is open
--
SET enable_seqscan = ON;
SET enable_indexscan = ON;
SET enable_indexonlyscan = ON;
SET enable_bitmapscan = ON;
SET enable_tidscan = ON;
SET enable_sort = ON;
SET enable_material = OFF;
SET enable_nestloop = ON;
SET enable_mergejoin = ON;
SET enable_hashjoin = ON;

-- explain cross join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 CROSS JOIN partition_scan_stu_info2;
-- explain inner join 
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 INNER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain left outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain right outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 RIGHT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain full outer join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 FULL OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- explain for no partition to join
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info1.SN > 40; 
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40;
EXPLAIN (COSTS OFF) SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40 AND partition_scan_stu_info1.SN > 40;
-- rsult of cross join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 CROSS JOIN partition_scan_stu_info2 order by 1, 2;
-- rsult of inner joi
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 INNER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of left join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of right join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 RIGHT OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- rsult of FULL join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 FULL OUTER JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) ORDER BY partition_scan_stu_info1.SN, partition_scan_stu_info1.NAME, partition_scan_stu_info1.NAME;
-- result of no partition to join
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info1.SN > 40;
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40;
SELECT partition_scan_stu_info1.NAME, partition_scan_stu_info2.PHONE FROM partition_scan_stu_info1 LEFT JOIN partition_scan_stu_info2 ON (partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) WHERE partition_scan_stu_info2.SN > 40 AND partition_scan_stu_info1.SN > 40;

--
---- test WITH_QUERY & EXIST & IN
--
-- EXPLAIN WITH_QUERY CASE: JOIN +  EXISTS PARTITION 
EXPLAIN (COSTS OFF) WITH STU_INFO2_VIEW AS (SELECT * FROM partition_scan_stu_info2 WHERE SN < 40 ORDER BY SN )
SELECT STU_INFO2_VIEW.SN, partition_scan_stu_info1.NAME, STU_INFO2_VIEW.PHONE
FROM STU_INFO2_VIEW LEFT JOIN partition_scan_stu_info1
ON (partition_scan_stu_info1.SN < STU_INFO2_VIEW.SN);

-- RESULT OF WITH_QUERY CASE: JOIN +  EXISTS PARTITION 
WITH STU_INFO2_VIEW AS (SELECT * FROM partition_scan_stu_info2 WHERE SN < 40 ORDER BY SN )
SELECT STU_INFO2_VIEW.SN, partition_scan_stu_info1.NAME, STU_INFO2_VIEW.PHONE
FROM STU_INFO2_VIEW LEFT JOIN partition_scan_stu_info1
ON (partition_scan_stu_info1.SN < STU_INFO2_VIEW.SN)
ORDER BY 1, 2, 3;

-- EXPLAIN WITH_QUERY CASE: JOIN +  NON PARTITION 
EXPLAIN (COSTS OFF) WITH STU_INFO2_VIEW AS (SELECT * FROM partition_scan_stu_info2 WHERE SN > 40 ORDER BY SN )
SELECT STU_INFO2_VIEW.SN, partition_scan_stu_info1.NAME, STU_INFO2_VIEW.PHONE
FROM STU_INFO2_VIEW LEFT JOIN partition_scan_stu_info1
ON (partition_scan_stu_info1.SN < STU_INFO2_VIEW.SN);

-- RESULT OF WITH_QUERY CASE:  JOIN +  NON PARTITION
WITH STU_INFO2_VIEW AS (SELECT * FROM partition_scan_stu_info2 WHERE SN > 40 ORDER BY SN )
SELECT STU_INFO2_VIEW.SN, partition_scan_stu_info1.NAME, STU_INFO2_VIEW.PHONE
FROM STU_INFO2_VIEW LEFT JOIN partition_scan_stu_info1
ON (partition_scan_stu_info1.SN < STU_INFO2_VIEW.SN);

-- EXPLAIN EXIST
EXPLAIN (COSTS OFF) SELECT * FROM partition_scan_stu_info1 WHERE EXISTS (SELECT partition_scan_stu_info2.SN FROM partition_scan_stu_info2 WHERE partition_scan_stu_info1.SN = partition_scan_stu_info2.SN);

-- RESULT OF EXIST
SELECT * FROM partition_scan_stu_info1 WHERE EXISTS (SELECT partition_scan_stu_info2.SN FROM partition_scan_stu_info2 WHERE partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) order by 1;

-- EXPLAIN NOT EXIST
EXPLAIN (COSTS OFF) SELECT * FROM partition_scan_stu_info1 WHERE partition_scan_stu_info1.SN IN (SELECT partition_scan_stu_info2.SN FROM partition_scan_stu_info2);

-- RESULT OF NOT EXIST
SELECT * FROM partition_scan_stu_info1 WHERE NOT EXISTS (SELECT partition_scan_stu_info2.SN FROM partition_scan_stu_info2 WHERE partition_scan_stu_info1.SN = partition_scan_stu_info2.SN);

-- EXPLAIN IN
EXPLAIN (COSTS OFF) SELECT * FROM partition_scan_stu_info1 WHERE partition_scan_stu_info1.SN IN (SELECT partition_scan_stu_info2.SN FROM partition_scan_stu_info2 WHERE partition_scan_stu_info1.SN = partition_scan_stu_info2.SN);

-- RESULT OF IN
SELECT * FROM partition_scan_stu_info1 WHERE partition_scan_stu_info1.SN IN (SELECT partition_scan_stu_info2.SN FROM partition_scan_stu_info2 WHERE partition_scan_stu_info1.SN = partition_scan_stu_info2.SN) order by 1;

-- EXPLAIN NOT IN
EXPLAIN (COSTS OFF) SELECT * FROM partition_scan_stu_info1 WHERE partition_scan_stu_info1.SN NOT IN (SELECT partition_scan_stu_info2.SN FROM partition_scan_stu_info2 WHERE partition_scan_stu_info1.SN = partition_scan_stu_info2.SN);

-- RESULT OF NOT IN
SELECT * FROM partition_scan_stu_info1 WHERE partition_scan_stu_info1.SN NOT IN (SELECT partition_scan_stu_info2.SN FROM partition_scan_stu_info2 WHERE partition_scan_stu_info1.SN = partition_scan_stu_info2.SN);

-- EXPLAIN WITH_QUERY CASE: EXISTS PARTITION 
EXPLAIN (COSTS OFF) WITH STU_INFO2_VIEW AS (SELECT SN FROM partition_scan_stu_info2 WHERE SN < 40 ORDER BY SN )
DELETE FROM partition_scan_stu_info1 USING partition_scan_stu_info2 
WHERE partition_scan_stu_info1.SN < partition_scan_stu_info2.SN AND partition_scan_stu_info2.SN IN (SELECT SN FROM STU_INFO2_VIEW) 
RETURNING partition_scan_stu_info1.NAME;

-- RESULT OF WITH_QUERY CASE: EXISTS PARTITION
with delete_partition_scan_stu_info1_NAME as (
WITH STU_INFO2_VIEW AS (SELECT SN FROM partition_scan_stu_info2 WHERE SN < 40 ORDER BY SN )
DELETE FROM partition_scan_stu_info1 USING partition_scan_stu_info2 
WHERE partition_scan_stu_info1.SN < partition_scan_stu_info2.SN AND partition_scan_stu_info2.SN IN (SELECT SN FROM STU_INFO2_VIEW) 
RETURNING partition_scan_stu_info1.NAME )
select * from delete_partition_scan_stu_info1_NAME order by 1;

-- EXPLAIN WITH_QUERY CASE: NONE PARTITION 
EXPLAIN (COSTS OFF) WITH STU_INFO2_VIEW AS (SELECT SN FROM partition_scan_stu_info2 WHERE SN > 40 ORDER BY SN )
DELETE FROM partition_scan_stu_info1 USING partition_scan_stu_info2 
WHERE partition_scan_stu_info1.SN < partition_scan_stu_info2.SN AND partition_scan_stu_info2.SN IN (SELECT SN FROM STU_INFO2_VIEW) 
RETURNING partition_scan_stu_info1.NAME;

-- RESULT OF EXPLAIN WITH_QUERY CASE: NONE PARTITION 
WITH STU_INFO2_VIEW AS (SELECT SN FROM partition_scan_stu_info2 WHERE SN > 40 ORDER BY SN )
DELETE FROM partition_scan_stu_info1 USING partition_scan_stu_info2 
WHERE partition_scan_stu_info1.SN < partition_scan_stu_info2.SN AND partition_scan_stu_info2.SN IN (SELECT SN FROM STU_INFO2_VIEW) 
RETURNING partition_scan_stu_info1.NAME;

-- clean up
DROP TABLE partition_scan_stu_info1;
DROP TABLE partition_scan_stu_info2;
