--
---- TEST FOR PWJ
--
set enable_partitionwise = on;
-- PREPARE
-- CREATE TABLE partition_pwj_test_t1
CREATE TABLE partition_pwj_test_t1 (A INT4, B TEXT)
PARTITION BY RANGE (A)
(
	PARTITION p1_partition_pwj_test_t1 VALUES LESS THAN (10),
	PARTITION p2_partition_pwj_test_t1 VALUES LESS THAN (20),
	PARTITION p3_partition_pwj_test_t1 VALUES LESS THAN (100),
	PARTITION p4_partition_pwj_test_t1 VALUES LESS THAN (110)
);
-- CREATE TABLE partition_pwj_test_t2
CREATE TABLE partition_pwj_test_t2 (A INT4, B TEXT)
PARTITION BY RANGE (A)
(
	PARTITION p1_partition_pwj_test_t2 VALUES LESS THAN (10),
	PARTITION p2_partition_pwj_test_t2 VALUES LESS THAN (20),
	PARTITION p3_partition_pwj_test_t2 VALUES LESS THAN (100),
	PARTITION p4_partition_pwj_test_t2 VALUES LESS THAN (110)
);
-- CREATE TABLE partition_pwj_test_t3
CREATE TABLE partition_pwj_test_t3 (A INT4, B TEXT)
PARTITION BY RANGE (A)
(
	PARTITION p1_partition_pwj_test_t3 VALUES LESS THAN (10),
	PARTITION p2_partition_pwj_test_t3 VALUES LESS THAN (20),
	PARTITION p3_partition_pwj_test_t3 VALUES LESS THAN (100),
	PARTITION p4_partition_pwj_test_t3 VALUES LESS THAN (110)
);
-- CREATE TABLE partition_pwj_test_t4
CREATE TABLE partition_pwj_test_t4 (A INT4, B TEXT);

-- CREATE INDEX ON partition_pwj_test_t1
CREATE INDEX INDEX_ON_TEST_T1 ON partition_pwj_test_t1 (A) LOCAL;
-- CREATE INDEX ON partition_pwj_test_t2
CREATE INDEX INDEX_ON_TEST_T2 ON partition_pwj_test_t2 (A) LOCAL;
-- CREATE INDEX ON partition_pwj_test_t3
CREATE INDEX INDEX_ON_TEST_T3 ON partition_pwj_test_t3 (A) LOCAL;
-- CREATE INDEX ON partition_pwj_test_t4
CREATE INDEX INDEX_ON_TEST_T4 ON partition_pwj_test_t4 (A);
-- CREATE INDEX ON partition_pwj_test_t1
CREATE INDEX INDEX_ON_TEST_T1_1 ON partition_pwj_test_t1 (B) LOCAL;
-- CREATE INDEX ON partition_pwj_test_t2
CREATE INDEX INDEX_ON_TEST_T2_1 ON partition_pwj_test_t2 (B) LOCAL;
-- CREATE INDEX ON partition_pwj_test_t3
CREATE INDEX INDEX_ON_TEST_T3_1 ON partition_pwj_test_t3 (B) LOCAL;
-- CREATE INDEX ON partition_pwj_test_t4
CREATE INDEX INDEX_ON_TEST_T4_1 ON partition_pwj_test_t4 (B);

-- INSERT RECORD
INSERT INTO partition_pwj_test_t1 VALUES (1, 'TEST_T1_1'), (2, 'TEST_T1_2'), (11, 'TEST_T1_11'), (12, 'TEST_T1_12'), (91, 'TEST_T1_91'), (92, 'TEST_T1_92'), (101, 'TEST_T1_101'), (102, 'TEST_T1_102');
INSERT INTO partition_pwj_test_t2 VALUES (1, 'TEST_T2_1'), (2, 'TEST_T2_2'), (11, 'TEST_T2_11'), (12, 'TEST_T2_12'), (91, 'TEST_T2_91'), (92, 'TEST_T2_92'), (101, 'TEST_T2_101'), (102, 'TEST_T3_102');
INSERT INTO partition_pwj_test_t3 VALUES (1, 'TEST_T3_1'), (2, 'TEST_T3_2'), (11, 'TEST_T3_11'), (12, 'TEST_T3_12'), (91, 'TEST_T3_91'), (92, 'TEST_T3_92'), (101, 'TEST_T3_101'), (102, 'TEST_T3_102');
INSERT INTO partition_pwj_test_t4 VALUES (1, 'TEST_T4_1'), (2, 'TEST_T4_2'), (11, 'TEST_T4_11'), (12, 'TEST_T4_12'), (91, 'TEST_T4_91'), (92, 'TEST_T4_92'), (101, 'TEST_T4_101'), (102, 'TEST_T4_102');

SELECT REL.RELNAME, PART.RELNAME, PART.RANGENUM, PART.INTERVALNUM, PART.PARTKEY, PART.PARTKEY, PART.BOUNDARIES
FROM PG_PARTITION PART INNER JOIN PG_CLASS REL ON (REL.OID = PART.PARENTID)
WHERE REL.RELNAME IN('partition_pwj_test_t1', 'partition_pwj_test_t2', 'partition_pwj_test_t3', 'partition_pwj_test_t4') ORDER BY REL.RELNAME, PART.RELNAME;

SELECT * FROM partition_pwj_test_t1 order by 1, 2;
SELECT * FROM partition_pwj_test_t2 order by 1, 2;
SELECT * FROM partition_pwj_test_t3 order by 1, 2;
SELECT * FROM partition_pwj_test_t4 order by 1, 2;

--
---- MERGEJOIN + SEQSCAN
--
SET ENABLE_PARTITIONWISE = ON;
SET ENABLE_SEQSCAN = ON;
SET ENABLE_INDEXSCAN = OFF;
SET ENABLE_INDEXONLYSCAN = OFF;
SET ENABLE_BITMAPSCAN = OFF;
SET ENABLE_TIDSCAN = OFF;
SET ENABLE_SORT = ON;
SET ENABLE_MATERIAL = ON;
SET ENABLE_MERGEJOIN = ON;
SET ENABLE_NESTLOOP = OFF;
SET ENABLE_HASHJOIN = OFF;

-- JOINTYPE INNER JOIN -- TRY TO USE PARTITIONWISE JOIN
EXPLAIN (COSTS OFF, NODES OFF) SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A);
SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A) order by 1, 2, 3;

-- CLOSE SORT + OPEN MATERIAL
SET ENABLE_SORT = OFF;
SET ENABLE_MATERIAL = ON;
EXPLAIN (COSTS OFF, NODES OFF) SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A);
SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A) order by 1, 2, 3;

-- OPEN SORT + CLOSE MATERIAL
SET ENABLE_SORT = ON;
SET ENABLE_MATERIAL = OFF;
EXPLAIN (COSTS OFF, NODES OFF) SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A);
SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A) order by 1, 2, 3;

-- CLOSE SORT + CLOSE MATERIAL
SET ENABLE_SORT = OFF;
SET ENABLE_MATERIAL = OFF;
EXPLAIN (COSTS OFF, NODES OFF) SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A);
SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A) order by 1, 2, 3;

-- CLOSE PARTITIONWISEJOIN
SET ENABLE_PARTITIONWISE = OFF;
EXPLAIN (COSTS OFF, NODES OFF) SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A);
SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A) order by 1, 2, 3;

--
---- MERGEJOIN + INDEXSCAN
--
SET ENABLE_PARTITIONWISE = ON;
SET ENABLE_SEQSCAN = OFF;
SET ENABLE_INDEXSCAN = ON;
SET ENABLE_INDEXONLYSCAN = OFF;
SET ENABLE_BITMAPSCAN = OFF;
SET ENABLE_TIDSCAN = OFF;
SET ENABLE_SORT = ON;
SET ENABLE_MATERIAL = ON;
SET ENABLE_MERGEJOIN = ON;
SET ENABLE_NESTLOOP = OFF;
SET ENABLE_HASHJOIN = OFF;

-- JOINTYPE INNER JOIN -- TRY TO USE PARTITIONWISE JOIN
-- OPEN SORT + OPEN MATERIAL
EXPLAIN (COSTS OFF, NODES OFF) SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A);
SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A) order by 1, 2, 3;

-- CLOSE SORT + OPEN MATERIAL
SET ENABLE_SORT = OFF;
SET ENABLE_MATERIAL = ON;
EXPLAIN (COSTS OFF, NODES OFF) SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A);
SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A) order by 1, 2, 3;

-- OPEN SORT + CLOSE MATERIAL
SET ENABLE_SORT = ON;
SET ENABLE_MATERIAL = OFF;
EXPLAIN (COSTS OFF, NODES OFF) SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A);
SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A) order by 1, 2, 3;

-- CLOSE SORT + CLOSE MATERIAL
SET ENABLE_SORT = OFF;
SET ENABLE_MATERIAL = OFF;
EXPLAIN (COSTS OFF, NODES OFF) SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A);
SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A) order by 1, 2, 3;

-- CLOSE PARTITIONWISEJOIN
SET ENABLE_PARTITIONWISE = OFF;
EXPLAIN (COSTS OFF, NODES OFF) SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A);
SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A) order by 1, 2, 3;

--
---- MERGEJOIN + INDEXONLYSCAN
--
SET ENABLE_PARTITIONWISE = ON;
SET ENABLE_SEQSCAN = OFF;
SET ENABLE_INDEXSCAN = OFF;
SET ENABLE_INDEXONLYSCAN = ON;
SET ENABLE_BITMAPSCAN = OFF;
SET ENABLE_TIDSCAN = OFF;
SET ENABLE_SORT = ON;
SET ENABLE_MATERIAL = ON;
SET ENABLE_MERGEJOIN = ON;
SET ENABLE_NESTLOOP = OFF;
SET ENABLE_HASHJOIN = OFF;

-- JOINTYPE INNER JOIN -- TRY TO USE PARTITIONWISE JOIN
-- OPEN SORT + OPEN MATERIAL
EXPLAIN (COSTS OFF, NODES OFF) SELECT partition_pwj_test_t1.A, partition_pwj_test_t2.A FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A);
SELECT partition_pwj_test_t1.A, partition_pwj_test_t2.A FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A) order by 1, 2;

-- CLOSE SORT + OPEN MATERIAL
SET ENABLE_SORT = OFF;
SET ENABLE_MATERIAL = ON;
EXPLAIN (COSTS OFF, NODES OFF) SELECT partition_pwj_test_t1.A, partition_pwj_test_t2.A FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A);
SELECT partition_pwj_test_t1.A, partition_pwj_test_t2.A FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A) order by 1, 2;

-- OPEN SORT + CLOSE MATERIAL
SET ENABLE_SORT = ON;
SET ENABLE_MATERIAL = OFF;
EXPLAIN (COSTS OFF, NODES OFF) SELECT partition_pwj_test_t1.A, partition_pwj_test_t2.A FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A);
SELECT partition_pwj_test_t1.A, partition_pwj_test_t2.A FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A) order by 1, 2;

-- CLOSE SORT + CLOSE MATERIAL
SET ENABLE_SORT = OFF;
SET ENABLE_MATERIAL = OFF;
EXPLAIN (COSTS OFF, NODES OFF) SELECT partition_pwj_test_t1.A, partition_pwj_test_t2.A FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A);
SELECT partition_pwj_test_t1.A, partition_pwj_test_t2.A FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A) order by 1, 2;

-- CLOSE PARTITIONWISEJOIN
SET ENABLE_PARTITIONWISE = OFF;
EXPLAIN (COSTS OFF, NODES OFF) SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A);
SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A) order by 1, 2, 3;

--
---- MERGEJOIN + BITMAPSCAN
--
SET ENABLE_PARTITIONWISE = ON;
SET ENABLE_SEQSCAN = OFF;
SET ENABLE_INDEXSCAN = OFF;
SET ENABLE_INDEXONLYSCAN = OFF;
SET ENABLE_BITMAPSCAN = ON;
SET ENABLE_TIDSCAN = OFF;
SET ENABLE_SORT = ON;
SET ENABLE_MATERIAL = ON;
SET ENABLE_MERGEJOIN = ON;
SET ENABLE_NESTLOOP = OFF;
SET ENABLE_HASHJOIN = OFF;

-- JOINTYPE INNER JOIN -- TRY TO USE PARTITIONWISE JOIN
-- OPEN SORT + OPEN MATERIAL
EXPLAIN (COSTS OFF, NODES OFF) SELECT partition_pwj_test_t1.A, partition_pwj_test_t2.A FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A AND partition_pwj_test_t1.A in (1,2,11,12,91,92,101,102));
SELECT partition_pwj_test_t1.A, partition_pwj_test_t2.A FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A AND partition_pwj_test_t1.A in (1,2,11,12,91,92,101,102)) order by 1, 2;

-- CLOSE SORT + OPEN MATERIAL
SET ENABLE_SORT = OFF;
SET ENABLE_MATERIAL = ON;
EXPLAIN (COSTS OFF, NODES OFF) SELECT partition_pwj_test_t1.A, partition_pwj_test_t2.A FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A AND partition_pwj_test_t1.A in (1,2,11,12,91,92,101,102));
SELECT partition_pwj_test_t1.A, partition_pwj_test_t2.A FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A AND partition_pwj_test_t1.A in (1,2,11,12,91,92,101,102)) order by 1, 2;

-- OPEN SORT + CLOSE MATERIAL
SET ENABLE_SORT = ON;
SET ENABLE_MATERIAL = OFF;
EXPLAIN (COSTS OFF, NODES OFF) SELECT partition_pwj_test_t1.A, partition_pwj_test_t2.A FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A AND partition_pwj_test_t1.A in (1,2,11,12,91,92,101,102));
SELECT partition_pwj_test_t1.A, partition_pwj_test_t2.A FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A AND partition_pwj_test_t1.A in (1,2,11,12,91,92,101,102)) order by 1, 2;

-- CLOSE SORT + CLOSE MATERIAL
SET ENABLE_SORT = OFF;
SET ENABLE_MATERIAL = OFF;
EXPLAIN (COSTS OFF, NODES OFF) SELECT partition_pwj_test_t1.A, partition_pwj_test_t2.A FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A AND partition_pwj_test_t1.A in (1,2,11,12,91,92,101,102));
SELECT partition_pwj_test_t1.A, partition_pwj_test_t2.A FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A AND partition_pwj_test_t1.A in (1,2,11,12,91,92,101,102)) order by 1, 2;

-- CLOSE PARTITIONWISEJOIN
SET ENABLE_PARTITIONWISE = OFF;
EXPLAIN (COSTS OFF, NODES OFF) SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A);
SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A) order by 1, 2, 3;

--
---- NESTLOOP + SEQSCAN
--
SET ENABLE_PARTITIONWISE = TRUE;
SET ENABLE_SEQSCAN = ON;
SET ENABLE_INDEXSCAN = OFF;
SET ENABLE_INDEXONLYSCAN = OFF;
SET ENABLE_BITMAPSCAN = OFF;
SET ENABLE_TIDSCAN = OFF;
SET ENABLE_SORT = ON;
SET ENABLE_MATERIAL = ON;
SET ENABLE_MERGEJOIN = OFF;
SET ENABLE_NESTLOOP = ON;
SET ENABLE_HASHJOIN = OFF;

-- JOINTYPE INNER JOIN -- TRY TO USE PARTITIONWISE JOIN
EXPLAIN (COSTS OFF, NODES OFF) SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A);
SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A) order by 1, 2, 3;

-- CLOSE SORT + OPEN MATERIAL
SET ENABLE_SORT = OFF;
SET ENABLE_MATERIAL = ON;
EXPLAIN (COSTS OFF, NODES OFF) SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A);
SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A) order by 1, 2, 3;

-- OPEN SORT + CLOSE MATERIAL
SET ENABLE_SORT = ON;
SET ENABLE_MATERIAL = OFF;
EXPLAIN (COSTS OFF, NODES OFF) SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A);
SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A) order by 1, 2, 3;

-- CLOSE SORT + CLOSE MATERIAL
SET ENABLE_SORT = OFF;
SET ENABLE_MATERIAL = OFF;
EXPLAIN (COSTS OFF, NODES OFF) SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A);
SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A) order by 1, 2, 3;

-- CLOSE PARTITIONWISEJOIN
SET ENABLE_PARTITIONWISE = OFF;
EXPLAIN (COSTS OFF, NODES OFF) SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A);
SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A) order by 1, 2, 3;

--
---- NESTLOOP + INDEX
--
SET ENABLE_PARTITIONWISE = TRUE;
SET ENABLE_SEQSCAN = OFF;
SET ENABLE_INDEXSCAN = ON;
SET ENABLE_INDEXONLYSCAN = OFF;
SET ENABLE_BITMAPSCAN = OFF;
SET ENABLE_TIDSCAN = OFF;
SET ENABLE_SORT = ON;
SET ENABLE_MATERIAL = ON;
SET ENABLE_MERGEJOIN = OFF;
SET ENABLE_NESTLOOP = ON;
SET ENABLE_HASHJOIN = OFF;

-- JOINTYPE INNER JOIN -- TRY TO USE PARTITIONWISE JOIN
EXPLAIN (COSTS OFF, NODES OFF) SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A);
SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A) order by 1, 2, 3;

-- CLOSE SORT + OPEN MATERIAL
SET ENABLE_SORT = OFF;
SET ENABLE_MATERIAL = ON;
EXPLAIN (COSTS OFF, NODES OFF) SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A);
SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A) order by 1, 2, 3;

-- OPEN SORT + CLOSE MATERIAL
SET ENABLE_SORT = ON;
SET ENABLE_MATERIAL = OFF;
EXPLAIN (COSTS OFF, NODES OFF) SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A);
SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A) order by 1, 2, 3;

-- CLOSE SORT + CLOSE MATERIAL
SET ENABLE_SORT = OFF;
SET ENABLE_MATERIAL = OFF;
EXPLAIN (COSTS OFF, NODES OFF) SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A);
SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A) order by 1, 2, 3;

-- CLOSE PARTITIONWISEJOIN
SET ENABLE_PARTITIONWISE = OFF;
EXPLAIN (COSTS OFF, NODES OFF) SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A);
SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A) order by 1, 2, 3;

--
---- NESTLOOP + INDEXONLYSCAN
--
SET ENABLE_PARTITIONWISE = TRUE;
SET ENABLE_SEQSCAN = OFF;
SET ENABLE_INDEXSCAN = OFF;
SET ENABLE_INDEXONLYSCAN = ON;
SET ENABLE_BITMAPSCAN = OFF;
SET ENABLE_TIDSCAN = OFF;
SET ENABLE_SORT = ON;
SET ENABLE_MATERIAL = ON;
SET ENABLE_MERGEJOIN = OFF;
SET ENABLE_NESTLOOP = ON;
SET ENABLE_HASHJOIN = OFF;

-- JOINTYPE INNER JOIN -- TRY TO USE PARTITIONWISE JOIN
EXPLAIN (COSTS OFF, NODES OFF) SELECT partition_pwj_test_t1.A, partition_pwj_test_t2.A FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A);
SELECT partition_pwj_test_t1.A, partition_pwj_test_t2.A FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A) order by 1, 2;

-- CLOSE SORT + OPEN MATERIAL
SET ENABLE_SORT = OFF;
SET ENABLE_MATERIAL = ON;
EXPLAIN (COSTS OFF, NODES OFF) SELECT partition_pwj_test_t1.A, partition_pwj_test_t2.A FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A);
SELECT partition_pwj_test_t1.A, partition_pwj_test_t2.A FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A) order by 1, 2;

-- OPEN SORT + CLOSE MATERIAL
SET ENABLE_SORT = ON;
SET ENABLE_MATERIAL = OFF;
EXPLAIN (COSTS OFF, NODES OFF) SELECT partition_pwj_test_t1.A, partition_pwj_test_t2.A FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A);
SELECT partition_pwj_test_t1.A, partition_pwj_test_t2.A FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A) order by 1, 2;

-- CLOSE SORT + CLOSE MATERIAL
SET ENABLE_SORT = OFF;
SET ENABLE_MATERIAL = OFF;
EXPLAIN (COSTS OFF, NODES OFF) SELECT partition_pwj_test_t1.A, partition_pwj_test_t2.A FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A);
SELECT partition_pwj_test_t1.A, partition_pwj_test_t2.A FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A) order by 1, 2;

-- CLOSE PARTITIONWISEJOIN
SET ENABLE_PARTITIONWISE = OFF;
EXPLAIN (COSTS OFF, NODES OFF) SELECT partition_pwj_test_t1.A, partition_pwj_test_t2.A FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A);
SELECT partition_pwj_test_t1.A, partition_pwj_test_t2.A FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A) order by 1, 2;
--
---- NESTLOOP + BITMAPSCAN
--
SET ENABLE_PARTITIONWISE = TRUE;
SET ENABLE_SEQSCAN = OFF;
SET ENABLE_INDEXSCAN = OFF;
SET ENABLE_INDEXONLYSCAN = OFF;
SET ENABLE_BITMAPSCAN = ON;
SET ENABLE_TIDSCAN = OFF;
SET ENABLE_SORT = ON;
SET ENABLE_MATERIAL = ON;
SET ENABLE_MERGEJOIN = OFF;
SET ENABLE_NESTLOOP = ON;
SET ENABLE_HASHJOIN = OFF;

-- JOINTYPE INNER JOIN -- TRY TO USE PARTITIONWISE JOIN
EXPLAIN (COSTS OFF, NODES OFF) SELECT partition_pwj_test_t1.A, partition_pwj_test_t2.A FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A AND partition_pwj_test_t1.A in (1,2,11,12,91,92,101,102));
SELECT partition_pwj_test_t1.A, partition_pwj_test_t2.A FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A AND partition_pwj_test_t1.A in (1,2,11,12,91,92,101,102)) order by 1, 2;

-- CLOSE SORT + OPEN MATERIAL
SET ENABLE_SORT = OFF;
SET ENABLE_MATERIAL = ON;
EXPLAIN (COSTS OFF, NODES OFF) SELECT partition_pwj_test_t1.A, partition_pwj_test_t2.A FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A AND partition_pwj_test_t1.A in (1,2,11,12,91,92,101,102));
SELECT partition_pwj_test_t1.A, partition_pwj_test_t2.A FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A AND partition_pwj_test_t1.A in (1,2,11,12,91,92,101,102)) order by 1, 2;

-- OPEN SORT + CLOSE MATERIAL
SET ENABLE_SORT = ON;
SET ENABLE_MATERIAL = OFF;
EXPLAIN (COSTS OFF, NODES OFF) SELECT partition_pwj_test_t1.A, partition_pwj_test_t2.A FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A AND partition_pwj_test_t1.A in (1,2,11,12,91,92,101,102));
SELECT partition_pwj_test_t1.A, partition_pwj_test_t2.A FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A AND partition_pwj_test_t1.A in (1,2,11,12,91,92,101,102)) order by 1, 2;

-- CLOSE SORT + CLOSE MATERIAL
SET ENABLE_SORT = OFF;
SET ENABLE_MATERIAL = OFF;
EXPLAIN (COSTS OFF, NODES OFF) SELECT partition_pwj_test_t1.A, partition_pwj_test_t2.A FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A AND partition_pwj_test_t1.A in (1,2,11,12,91,92,101,102));
SELECT partition_pwj_test_t1.A, partition_pwj_test_t2.A FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A AND partition_pwj_test_t1.A in (1,2,11,12,91,92,101,102)) order by 1, 2;

-- CLOSE PARTITIONWISEJOIN
SET ENABLE_PARTITIONWISE = OFF;
EXPLAIN (COSTS OFF, NODES OFF) SELECT partition_pwj_test_t1.A, partition_pwj_test_t2.A FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A AND partition_pwj_test_t1.A in (1,2,11,12,91,92,101,102));
SELECT partition_pwj_test_t1.A, partition_pwj_test_t2.A FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A AND partition_pwj_test_t1.A in (1,2,11,12,91,92,101,102)) order by 1, 2;

--
---- HASHJOIN + SEQSCAN
--
SET ENABLE_PARTITIONWISE = TRUE;
SET ENABLE_SEQSCAN = ON;
SET ENABLE_INDEXSCAN = OFF;
SET ENABLE_INDEXONLYSCAN = OFF;
SET ENABLE_BITMAPSCAN = OFF;
SET ENABLE_TIDSCAN = OFF;
SET ENABLE_SORT = ON;
SET ENABLE_MATERIAL = ON;
SET ENABLE_MERGEJOIN = OFF;
SET ENABLE_NESTLOOP = OFF;
SET ENABLE_HASHJOIN = ON;

-- JOINTYPE INNER JOIN -- TRY TO USE PARTITIONWISE JOIN
EXPLAIN (COSTS OFF, NODES OFF) SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A);
SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A) order by 1, 2, 3;

-- CLOSE SORT + OPEN MATERIAL
SET ENABLE_SORT = OFF;
SET ENABLE_MATERIAL = ON;
EXPLAIN (COSTS OFF, NODES OFF) SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A);
SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A) order by 1, 2, 3;

-- OPEN SORT + CLOSE MATERIAL
SET ENABLE_SORT = ON;
SET ENABLE_MATERIAL = OFF;
EXPLAIN (COSTS OFF, NODES OFF) SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A);
SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A) order by 1, 2, 3;

-- CLOSE SORT + CLOSE MATERIAL
SET ENABLE_SORT = OFF;
SET ENABLE_MATERIAL = OFF;
EXPLAIN (COSTS OFF, NODES OFF) SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A);
SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A) order by 1, 2, 3;

-- CLOSE PARTITIONWISEJOIN
SET ENABLE_PARTITIONWISE = OFF;
EXPLAIN (COSTS OFF, NODES OFF) SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A);
SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A) order by 1, 2, 3;

--
---- HASHJOIN + INDEXSACN
--
SET ENABLE_PARTITIONWISE = TRUE;
SET ENABLE_SEQSCAN = OFF;
SET ENABLE_INDEXSCAN = ON;
SET ENABLE_INDEXONLYSCAN = OFF;
SET ENABLE_BITMAPSCAN = OFF;
SET ENABLE_TIDSCAN = OFF;
SET ENABLE_SORT = ON;
SET ENABLE_MATERIAL = ON;
SET ENABLE_MERGEJOIN = OFF;
SET ENABLE_NESTLOOP = OFF;
SET ENABLE_HASHJOIN = ON;

-- JOINTYPE INNER JOIN -- TRY TO USE PARTITIONWISE JOIN
EXPLAIN (COSTS OFF, NODES OFF) SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A);
SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A) order by 1, 2, 3;

-- CLOSE SORT + OPEN MATERIAL
SET ENABLE_SORT = OFF;
SET ENABLE_MATERIAL = ON;
EXPLAIN (COSTS OFF, NODES OFF) SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A);
SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A) order by 1, 2, 3;

-- OPEN SORT + CLOSE MATERIAL
SET ENABLE_SORT = ON;
SET ENABLE_MATERIAL = OFF;
EXPLAIN (COSTS OFF, NODES OFF) SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A);
SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A) order by 1, 2, 3;

-- CLOSE SORT + CLOSE MATERIAL
SET ENABLE_SORT = OFF;
SET ENABLE_MATERIAL = OFF;
EXPLAIN (COSTS OFF, NODES OFF) SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A);
SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A) order by 1, 2, 3;

-- CLOSE PARTITIONWISEJOIN
SET ENABLE_PARTITIONWISE = OFF;
EXPLAIN (COSTS OFF, NODES OFF) SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A);
SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A) order by 1, 2, 3;

--
---- HASHJOIN + INDEXONLYSACN
--
SET ENABLE_PARTITIONWISE = TRUE;
SET ENABLE_SEQSCAN = OFF;
SET ENABLE_INDEXSCAN = OFF;
SET ENABLE_INDEXONLYSCAN = ON;
SET ENABLE_BITMAPSCAN = OFF;
SET ENABLE_TIDSCAN = OFF;
SET ENABLE_SORT = ON;
SET ENABLE_MATERIAL = ON;
SET ENABLE_MERGEJOIN = OFF;
SET ENABLE_NESTLOOP = OFF;
SET ENABLE_HASHJOIN = ON;

-- JOINTYPE INNER JOIN -- TRY TO USE PARTITIONWISE JOIN
EXPLAIN (COSTS OFF, NODES OFF) SELECT partition_pwj_test_t1.A, partition_pwj_test_t2.A FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A);
SELECT partition_pwj_test_t1.A, partition_pwj_test_t2.A FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A) order by 1, 2;

-- CLOSE SORT + OPEN MATERIAL
SET ENABLE_SORT = OFF;
SET ENABLE_MATERIAL = ON;
EXPLAIN (COSTS OFF, NODES OFF) SELECT partition_pwj_test_t1.A, partition_pwj_test_t2.A FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A);
SELECT partition_pwj_test_t1.A, partition_pwj_test_t2.A FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A) order by 1, 2;

-- OPEN SORT + CLOSE MATERIAL
SET ENABLE_SORT = ON;
SET ENABLE_MATERIAL = OFF;
EXPLAIN (COSTS OFF, NODES OFF) SELECT partition_pwj_test_t1.A, partition_pwj_test_t2.A FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A);
SELECT partition_pwj_test_t1.A, partition_pwj_test_t2.A FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A) order by 1, 2;

-- CLOSE SORT + CLOSE MATERIAL
SET ENABLE_SORT = OFF;
SET ENABLE_MATERIAL = OFF;
EXPLAIN (COSTS OFF, NODES OFF) SELECT partition_pwj_test_t1.A, partition_pwj_test_t2.A FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A);
SELECT partition_pwj_test_t1.A, partition_pwj_test_t2.A FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A) order by 1, 2;

-- CLOSE PARTITIONWISEJOIN
SET ENABLE_PARTITIONWISE = OFF;
EXPLAIN (COSTS OFF, NODES OFF) SELECT partition_pwj_test_t1.A, partition_pwj_test_t2.A FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A);
SELECT partition_pwj_test_t1.A, partition_pwj_test_t2.A FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A) order by 1, 2;

--
---- HASHJOIN + BITMAPSCAN
--
SET ENABLE_PARTITIONWISE = TRUE;
SET ENABLE_SEQSCAN = OFF;
SET ENABLE_INDEXSCAN = OFF;
SET ENABLE_INDEXONLYSCAN = OFF;
SET ENABLE_BITMAPSCAN = ON;
SET ENABLE_TIDSCAN = OFF;
SET ENABLE_SORT = ON;
SET ENABLE_MATERIAL = ON;
SET ENABLE_MERGEJOIN = OFF;
SET ENABLE_NESTLOOP = OFF;
SET ENABLE_HASHJOIN = ON;

-- JOINTYPE INNER JOIN -- TRY TO USE PARTITIONWISE JOIN
EXPLAIN (COSTS OFF, NODES OFF) SELECT partition_pwj_test_t1.A, partition_pwj_test_t2.A FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A AND partition_pwj_test_t1.A in (1,2,11,12,91,92,101,102));
SELECT partition_pwj_test_t1.A, partition_pwj_test_t2.A FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A AND partition_pwj_test_t1.A in (1,2,11,12,91,92,101,102)) order by 1, 2;

-- CLOSE SORT + OPEN MATERIAL
SET ENABLE_SORT = OFF;
SET ENABLE_MATERIAL = ON;
EXPLAIN (COSTS OFF, NODES OFF) SELECT partition_pwj_test_t1.A, partition_pwj_test_t2.A FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A AND partition_pwj_test_t1.A in (1,2,11,12,91,92,101,102));
SELECT partition_pwj_test_t1.A, partition_pwj_test_t2.A FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A AND partition_pwj_test_t1.A in (1,2,11,12,91,92,101,102)) order by 1, 2;

-- OPEN SORT + CLOSE MATERIAL
SET ENABLE_SORT = ON;
SET ENABLE_MATERIAL = OFF;
EXPLAIN (COSTS OFF, NODES OFF) SELECT partition_pwj_test_t1.A, partition_pwj_test_t2.A FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A AND partition_pwj_test_t1.A in (1,2,11,12,91,92,101,102));
SELECT partition_pwj_test_t1.A, partition_pwj_test_t2.A FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A AND partition_pwj_test_t1.A in (1,2,11,12,91,92,101,102)) order by 1, 2;

-- CLOSE SORT + CLOSE MATERIAL
SET ENABLE_SORT = OFF;
SET ENABLE_MATERIAL = OFF;
EXPLAIN (COSTS OFF, NODES OFF) SELECT partition_pwj_test_t1.A, partition_pwj_test_t2.A FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A AND partition_pwj_test_t1.A in (1,2,11,12,91,92,101,102));
SELECT partition_pwj_test_t1.A, partition_pwj_test_t2.A FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A AND partition_pwj_test_t1.A in (1,2,11,12,91,92,101,102)) order by 1, 2;

-- CLOSE PARTITIONWISEJOIN
SET ENABLE_PARTITIONWISE = OFF;
EXPLAIN (COSTS OFF, NODES OFF) SELECT partition_pwj_test_t1.A, partition_pwj_test_t2.A FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A AND partition_pwj_test_t1.A in (1,2,11,12,91,92,101,102));
SELECT partition_pwj_test_t1.A, partition_pwj_test_t2.A FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A AND partition_pwj_test_t1.A in (1,2,11,12,91,92,101,102)) order by 1, 2;

--
---- other cases
--
SET ENABLE_PARTITIONWISE = TRUE;
SET ENABLE_SEQSCAN = ON;
SET ENABLE_INDEXSCAN = ON;
SET ENABLE_INDEXONLYSCAN = ON;
SET ENABLE_BITMAPSCAN = ON;
SET ENABLE_TIDSCAN = ON;
SET ENABLE_SORT = ON;
SET ENABLE_MATERIAL = ON;
SET ENABLE_MERGEJOIN = ON;
SET ENABLE_NESTLOOP = ON;
SET ENABLE_HASHJOIN = ON;

--
---- jointype
--
-- JOINTYPE LEFT JOIN
EXPLAIN (COSTS OFF, NODES OFF) SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B FROM partition_pwj_test_t1 LEFT JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A);
SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B FROM partition_pwj_test_t1 LEFT JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A) order by 1, 2, 3;

-- JOINTYPE FULL JOIN
EXPLAIN (COSTS OFF, NODES OFF) SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B FROM partition_pwj_test_t1 FULL JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A);
SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B FROM partition_pwj_test_t1 FULL JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A) order by 1, 2, 3;

-- JOINTYPE RIGHT JOIN
EXPLAIN (COSTS OFF, NODES OFF) SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B FROM partition_pwj_test_t1 RIGHT JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A);
SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B FROM partition_pwj_test_t1 RIGHT JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A) order by 1, 2, 3;

-- JOINTYPE CROSS JOIN
EXPLAIN (COSTS OFF, NODES OFF) SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B FROM partition_pwj_test_t1 CROSS JOIN partition_pwj_test_t2;
SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B FROM partition_pwj_test_t1 CROSS JOIN partition_pwj_test_t2 order by 1, 2, 3;

-- JOINTYPE INNER JOIN -- TRY TO USE PARTITIONWISE JOIN
EXPLAIN (COSTS OFF, NODES OFF) SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A);
SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A) order by 1, 2, 3;

-- PARTITIONED TABLE + ORDINARY TABLE
EXPLAIN (COSTS OFF, NODES OFF) SELECT partition_pwj_test_t1.*, partition_pwj_test_t4.B FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t4 ON (partition_pwj_test_t1.A = partition_pwj_test_t4.A);
SELECT partition_pwj_test_t1.*, partition_pwj_test_t4.B FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t4 ON (partition_pwj_test_t1.A = partition_pwj_test_t4.A) order by 1, 2, 3;


--
---- JOIN-CONDITION
--

-- JOIN-COLUMN IS NOT IN PARTITIONKEY
EXPLAIN (COSTS OFF, NODES OFF) SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.B = partition_pwj_test_t2.B);
SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.B = partition_pwj_test_t2.B);

-- JOIN-CONFITION IS NOT AN EQUAL EXPRESSION
EXPLAIN (COSTS OFF, NODES OFF) SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A > partition_pwj_test_t2.A);
SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A > partition_pwj_test_t2.A) order by 1, 2, 3;

-- JOIN-CONFITION IS NOT OPERATOR
EXPLAIN (COSTS OFF, NODES OFF) SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (TRUE);
SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (TRUE) order by 1, 2, 3;

-- ONE JOIN-CONFITION MATCH PWJ
EXPLAIN (COSTS OFF, NODES OFF) SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A > 1 AND partition_pwj_test_t2.A > 2 AND partition_pwj_test_t1.A = partition_pwj_test_t2.A);
SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A > 1 AND partition_pwj_test_t2.A > 2 AND partition_pwj_test_t1.A = partition_pwj_test_t2.A) order by 1, 2, 3;

--
---- PARITION NUM
--

-- THE NUM IS NOT EUQAL
EXPLAIN (COSTS OFF, NODES OFF) SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A AND partition_pwj_test_t1.A > 1 AND partition_pwj_test_t2.A > 12);
SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A AND partition_pwj_test_t1.A > 1 AND partition_pwj_test_t2.A > 12) order by 1, 2, 3;

-- THE NUM IS EUQAL, BUT THE BOUNDARY IS NOT EQUAL
EXPLAIN (COSTS OFF, NODES OFF) SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A AND partition_pwj_test_t2.A > 12 AND partition_pwj_test_t1.A < 100);
SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A AND partition_pwj_test_t2.A > 12 AND partition_pwj_test_t1.A < 100) order by 1, 2, 3;

-- THE NUM IS EUQAL, AND THE BOUNDARY IS EQUAL
EXPLAIN (COSTS OFF, NODES OFF) SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A);
SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A) order by 1, 2, 3;

EXPLAIN (COSTS OFF, NODES OFF) SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A AND partition_pwj_test_t1.A < 10 AND partition_pwj_test_t2.A < 10);
SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B FROM partition_pwj_test_t1 INNER JOIN partition_pwj_test_t2 ON (partition_pwj_test_t1.A = partition_pwj_test_t2.A AND partition_pwj_test_t1.A < 10 AND partition_pwj_test_t2.A < 10) order by 1, 2, 3;

--
---- MUTLI-KEYPOINT TEST
--

EXPLAIN (COSTS OFF, NODES OFF) SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B, partition_pwj_test_t3.B FROM partition_pwj_test_t1, partition_pwj_test_t2, partition_pwj_test_t3 WHERE partition_pwj_test_t1.A = partition_pwj_test_t2.A AND partition_pwj_test_t2.A = partition_pwj_test_t3.A;
SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B, partition_pwj_test_t3.B FROM partition_pwj_test_t1, partition_pwj_test_t2, partition_pwj_test_t3 WHERE partition_pwj_test_t1.A = partition_pwj_test_t2.A AND partition_pwj_test_t2.A = partition_pwj_test_t3.A order by 1, 2, 3;

EXPLAIN (COSTS OFF, NODES OFF) SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B, partition_pwj_test_t3.B FROM partition_pwj_test_t1, partition_pwj_test_t2, partition_pwj_test_t3 WHERE partition_pwj_test_t1.A = partition_pwj_test_t2.A AND partition_pwj_test_t2.A = partition_pwj_test_t3.A AND partition_pwj_test_t1.A > 10;
SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B, partition_pwj_test_t3.B FROM partition_pwj_test_t1, partition_pwj_test_t2, partition_pwj_test_t3 WHERE partition_pwj_test_t1.A = partition_pwj_test_t2.A AND partition_pwj_test_t2.A = partition_pwj_test_t3.A AND partition_pwj_test_t1.A > 10 order by 1, 2, 3, 4;

EXPLAIN (COSTS OFF, NODES OFF) SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B, partition_pwj_test_t3.B FROM partition_pwj_test_t1, partition_pwj_test_t2, partition_pwj_test_t3 WHERE partition_pwj_test_t1.A = partition_pwj_test_t2.A AND partition_pwj_test_t2.A = partition_pwj_test_t3.A AND partition_pwj_test_t1.A > 10 AND partition_pwj_test_t2.A > 10;
SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B, partition_pwj_test_t3.B FROM partition_pwj_test_t1, partition_pwj_test_t2, partition_pwj_test_t3 WHERE partition_pwj_test_t1.A = partition_pwj_test_t2.A AND partition_pwj_test_t2.A = partition_pwj_test_t3.A AND partition_pwj_test_t1.A > 10 AND partition_pwj_test_t2.A > 10 order by 1, 2, 3, 4;

EXPLAIN (COSTS OFF, NODES OFF) SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B, partition_pwj_test_t3.B FROM partition_pwj_test_t1, partition_pwj_test_t2, partition_pwj_test_t3 WHERE partition_pwj_test_t1.A = partition_pwj_test_t2.A AND partition_pwj_test_t2.A = partition_pwj_test_t3.A AND partition_pwj_test_t1.A > 10 AND partition_pwj_test_t2.A > 20;
SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B, partition_pwj_test_t3.B FROM partition_pwj_test_t1, partition_pwj_test_t2, partition_pwj_test_t3 WHERE partition_pwj_test_t1.A = partition_pwj_test_t2.A AND partition_pwj_test_t2.A = partition_pwj_test_t3.A AND partition_pwj_test_t1.A > 10 AND partition_pwj_test_t2.A > 20 order by 1, 2, 3, 4;

EXPLAIN (COSTS OFF, NODES OFF) SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B, partition_pwj_test_t3.B, partition_pwj_test_t4.B FROM partition_pwj_test_t1, partition_pwj_test_t2, partition_pwj_test_t3, partition_pwj_test_t4 WHERE partition_pwj_test_t1.A = partition_pwj_test_t2.A AND partition_pwj_test_t2.A = partition_pwj_test_t3.A;
SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B, partition_pwj_test_t3.B, partition_pwj_test_t4.B FROM partition_pwj_test_t1, partition_pwj_test_t2, partition_pwj_test_t3, partition_pwj_test_t4 WHERE partition_pwj_test_t1.A = partition_pwj_test_t2.A AND partition_pwj_test_t2.A = partition_pwj_test_t3.A order by 1, 2, 3, 4, 5;

EXPLAIN (COSTS OFF, NODES OFF) SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B, partition_pwj_test_t3.B, partition_pwj_test_t4.B FROM partition_pwj_test_t1, partition_pwj_test_t2, partition_pwj_test_t3, partition_pwj_test_t4 WHERE partition_pwj_test_t1.A = partition_pwj_test_t2.A AND partition_pwj_test_t2.A = partition_pwj_test_t3.A AND partition_pwj_test_t4.A = partition_pwj_test_t1.A;
SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B, partition_pwj_test_t3.B, partition_pwj_test_t4.B FROM partition_pwj_test_t1, partition_pwj_test_t2, partition_pwj_test_t3, partition_pwj_test_t4 WHERE partition_pwj_test_t1.A = partition_pwj_test_t2.A AND partition_pwj_test_t2.A = partition_pwj_test_t3.A AND partition_pwj_test_t4.A = partition_pwj_test_t1.A order by 1, 2, 3, 4, 5;

SET ENABLE_SORT = OFF;
EXPLAIN (COSTS OFF, NODES OFF) SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B, partition_pwj_test_t3.B, partition_pwj_test_t4.B FROM partition_pwj_test_t1, partition_pwj_test_t2, partition_pwj_test_t3, partition_pwj_test_t4 WHERE partition_pwj_test_t1.A = partition_pwj_test_t2.A AND partition_pwj_test_t2.A = partition_pwj_test_t3.A AND partition_pwj_test_t4.A = partition_pwj_test_t1.A;
SELECT partition_pwj_test_t1.*, partition_pwj_test_t2.B, partition_pwj_test_t3.B, partition_pwj_test_t4.B FROM partition_pwj_test_t1, partition_pwj_test_t2, partition_pwj_test_t3, partition_pwj_test_t4 WHERE partition_pwj_test_t1.A = partition_pwj_test_t2.A AND partition_pwj_test_t2.A = partition_pwj_test_t3.A AND partition_pwj_test_t4.A = partition_pwj_test_t1.A order by 1, 2, 3, 4, 5;

--
---- CLEAN UP
--
DROP TABLE partition_pwj_test_t1;
DROP TABLE partition_pwj_test_t2;
DROP TABLE partition_pwj_test_t3;
DROP TABLE partition_pwj_test_t4;

RESET ENABLE_PARTITIONWISE;
RESET ENABLE_SEQSCAN;
RESET ENABLE_INDEXSCAN;
RESET ENABLE_INDEXONLYSCAN;
RESET ENABLE_BITMAPSCAN;
RESET ENABLE_TIDSCAN;
RESET ENABLE_SORT;
RESET ENABLE_MATERIAL;
RESET ENABLE_MERGEJOIN;
RESET ENABLE_NESTLOOP;
RESET ENABLE_HASHJOIN;

