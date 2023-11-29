---- test for scan for partitioned table
---- add by jiayanhua
---- case 1: partitioned table + partitioned table

--prepare
CREATE TABLE heap_tbl_scan_test_03(SN INT, NAME NAME)
PARTITION BY RANGE (SN)
(
	PARTITION P1_heap_tbl_scan_test_03 VALUES LESS THAN(10),
	PARTITION P2_heap_tbl_scan_test_03 VALUES LESS THAN(20),
	PARTITION P3_heap_tbl_scan_test_03 VALUES LESS THAN(30),
	PARTITION P4_heap_tbl_scan_test_03 VALUES LESS THAN(40)
);

CREATE TABLE heap_tbl_scan_test_04(SN INT, PHONE NAME)
PARTITION BY RANGE (SN)
(
	PARTITION P1_heap_tbl_scan_test_04 VALUES LESS THAN(10),
	PARTITION P2_heap_tbl_scan_test_04 VALUES LESS THAN(20),
	PARTITION P3_heap_tbl_scan_test_04 VALUES LESS THAN(30),
	PARTITION P4_heap_tbl_scan_test_04 VALUES LESS THAN(40)
);

INSERT INTO heap_tbl_scan_test_03 VALUES (1,'DFM');
INSERT INTO heap_tbl_scan_test_03 VALUES (11,'CHAO');
INSERT INTO heap_tbl_scan_test_03 VALUES (21,'ZJR');
INSERT INTO heap_tbl_scan_test_03 VALUES (31,'JYH');

INSERT INTO heap_tbl_scan_test_04 VALUES (1,'15478523126');
INSERT INTO heap_tbl_scan_test_04 VALUES (11,'15236997586');
INSERT INTO heap_tbl_scan_test_04 VALUES (21,'15936985364');
INSERT INTO heap_tbl_scan_test_04 VALUES (31,'15873285556');

CREATE UNIQUE INDEX index_on_heap_tbl_scan_test_03 ON heap_tbl_scan_test_03 (SN) LOCAL;

CREATE UNIQUE INDEX index_on_heap_tbl_scan_test_04 ON heap_tbl_scan_test_04 (SN) LOCAL;

SELECT * FROM heap_tbl_scan_test_03 ORDER BY 1;
SELECT * FROM heap_tbl_scan_test_04 ORDER BY 1;
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
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 CROSS JOIN heap_tbl_scan_test_04;
-- explain inner join 
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 INNER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- explain left outer join
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT OUTER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- explain right outer join
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 RIGHT OUTER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- explain full outer join
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 FULL OUTER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- explain for no partition to join
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) WHERE heap_tbl_scan_test_03.SN > 40; 
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) WHERE heap_tbl_scan_test_04.SN > 40;
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) WHERE heap_tbl_scan_test_04.SN > 40 AND heap_tbl_scan_test_03.SN > 40;
-- rsult of cross join
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 CROSS JOIN heap_tbl_scan_test_04 order by 1, 2;
-- rsult of inner joi
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 INNER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- rsult of left join
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT OUTER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- rsult of right join
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 RIGHT OUTER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- rsult of FULL join
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 FULL OUTER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- result of no partition to join
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) WHERE heap_tbl_scan_test_03.SN > 40;
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) WHERE heap_tbl_scan_test_04.SN > 40;
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) WHERE heap_tbl_scan_test_04.SN > 40 AND heap_tbl_scan_test_03.SN > 40;

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
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 CROSS JOIN heap_tbl_scan_test_04;
-- explain inner join 
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 INNER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- explain left outer join
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT OUTER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- explain right outer join
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 RIGHT OUTER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- explain full outer join
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 FULL OUTER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- explain for no partition to join
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) WHERE heap_tbl_scan_test_03.SN > 40; 
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) WHERE heap_tbl_scan_test_04.SN > 40;
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) WHERE heap_tbl_scan_test_04.SN > 40 AND heap_tbl_scan_test_03.SN > 40;
-- rsult of cross join
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 CROSS JOIN heap_tbl_scan_test_04 order by 1, 2;
-- rsult of inner joi
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 INNER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- rsult of left join
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT OUTER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- rsult of right join
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 RIGHT OUTER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- rsult of FULL join
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 FULL OUTER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- result of no partition to join
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) WHERE heap_tbl_scan_test_03.SN > 40;
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) WHERE heap_tbl_scan_test_04.SN > 40;
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) WHERE heap_tbl_scan_test_04.SN > 40 AND heap_tbl_scan_test_03.SN > 40;

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
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 CROSS JOIN heap_tbl_scan_test_04;
-- explain inner join 
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 INNER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- explain left outer join
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT OUTER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- explain right outer join
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 RIGHT OUTER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- explain full outer join
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 FULL OUTER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- explain for no partition to join
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) WHERE heap_tbl_scan_test_03.SN > 40; 
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) WHERE heap_tbl_scan_test_04.SN > 40;
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) WHERE heap_tbl_scan_test_04.SN > 40 AND heap_tbl_scan_test_03.SN > 40;
-- rsult of cross join
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 CROSS JOIN heap_tbl_scan_test_04 order by 1, 2;
-- rsult of inner joi
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 INNER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- rsult of left join
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT OUTER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- rsult of right join
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 RIGHT OUTER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- rsult of FULL join
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 FULL OUTER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- result of no partition to join
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) WHERE heap_tbl_scan_test_03.SN > 40;
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) WHERE heap_tbl_scan_test_04.SN > 40;
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) WHERE heap_tbl_scan_test_04.SN > 40 AND heap_tbl_scan_test_03.SN > 40;

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
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 CROSS JOIN heap_tbl_scan_test_04;
-- explain inner join 
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 INNER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- explain left outer join
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT OUTER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- explain right outer join
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 RIGHT OUTER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- explain full outer join
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 FULL OUTER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- explain for no partition to join
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) WHERE heap_tbl_scan_test_03.SN > 40; 
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) WHERE heap_tbl_scan_test_04.SN > 40;
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) WHERE heap_tbl_scan_test_04.SN > 40 AND heap_tbl_scan_test_03.SN > 40;
-- rsult of cross join
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 CROSS JOIN heap_tbl_scan_test_04 order by 1, 2;
-- rsult of inner joi
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 INNER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- rsult of left join
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT OUTER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- rsult of right join
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 RIGHT OUTER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- rsult of FULL join
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 FULL OUTER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- result of no partition to join
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) WHERE heap_tbl_scan_test_03.SN > 40;
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) WHERE heap_tbl_scan_test_04.SN > 40;
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) WHERE heap_tbl_scan_test_04.SN > 40 AND heap_tbl_scan_test_03.SN > 40;

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
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 CROSS JOIN heap_tbl_scan_test_04;
-- explain inner join 
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 INNER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- explain left outer join
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT OUTER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- explain right outer join
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 RIGHT OUTER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- explain full outer join
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 FULL OUTER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- explain for no partition to join
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) WHERE heap_tbl_scan_test_03.SN > 40; 
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) WHERE heap_tbl_scan_test_04.SN > 40;
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) WHERE heap_tbl_scan_test_04.SN > 40 AND heap_tbl_scan_test_03.SN > 40;
-- rsult of cross join
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 CROSS JOIN heap_tbl_scan_test_04 order by 1, 2;
-- rsult of inner joi
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 INNER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- rsult of left join
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT OUTER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- rsult of right join
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 RIGHT OUTER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- rsult of FULL join
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 FULL OUTER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- result of no partition to join
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) WHERE heap_tbl_scan_test_03.SN > 40;
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) WHERE heap_tbl_scan_test_04.SN > 40;
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) WHERE heap_tbl_scan_test_04.SN > 40 AND heap_tbl_scan_test_03.SN > 40;

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
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 CROSS JOIN heap_tbl_scan_test_04;
-- explain inner join 
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 INNER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- explain left outer join
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT OUTER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- explain right outer join
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 RIGHT OUTER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- explain full outer join
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 FULL OUTER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- explain for no partition to join
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) WHERE heap_tbl_scan_test_03.SN > 40; 
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) WHERE heap_tbl_scan_test_04.SN > 40;
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) WHERE heap_tbl_scan_test_04.SN > 40 AND heap_tbl_scan_test_03.SN > 40;
-- rsult of cross join
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 CROSS JOIN heap_tbl_scan_test_04 order by 1, 2;
-- rsult of inner joi
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 INNER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- rsult of left join
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT OUTER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- rsult of right join
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 RIGHT OUTER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- rsult of FULL join
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 FULL OUTER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- result of no partition to join
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) WHERE heap_tbl_scan_test_03.SN > 40;
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) WHERE heap_tbl_scan_test_04.SN > 40;
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) WHERE heap_tbl_scan_test_04.SN > 40 AND heap_tbl_scan_test_03.SN > 40;

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
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 CROSS JOIN heap_tbl_scan_test_04;
-- explain inner join 
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 INNER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- explain left outer join
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT OUTER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- explain right outer join
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 RIGHT OUTER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- explain full outer join
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 FULL OUTER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- explain for no partition to join
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) WHERE heap_tbl_scan_test_03.SN > 40; 
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) WHERE heap_tbl_scan_test_04.SN > 40;
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) WHERE heap_tbl_scan_test_04.SN > 40 AND heap_tbl_scan_test_03.SN > 40;
-- rsult of cross join
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 CROSS JOIN heap_tbl_scan_test_04 order by 1, 2;
-- rsult of inner joi
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 INNER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- rsult of left join
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT OUTER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- rsult of right join
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 RIGHT OUTER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- rsult of FULL join
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 FULL OUTER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- result of no partition to join
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) WHERE heap_tbl_scan_test_03.SN > 40;
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) WHERE heap_tbl_scan_test_04.SN > 40;
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) WHERE heap_tbl_scan_test_04.SN > 40 AND heap_tbl_scan_test_03.SN > 40;

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
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 CROSS JOIN heap_tbl_scan_test_04;
-- explain inner join 
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 INNER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- explain left outer join
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT OUTER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- explain right outer join
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 RIGHT OUTER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- explain full outer join
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 FULL OUTER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- explain for no partition to join
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) WHERE heap_tbl_scan_test_03.SN > 40; 
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) WHERE heap_tbl_scan_test_04.SN > 40;
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) WHERE heap_tbl_scan_test_04.SN > 40 AND heap_tbl_scan_test_03.SN > 40;
-- rsult of cross join
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 CROSS JOIN heap_tbl_scan_test_04 order by 1, 2;
-- rsult of inner joi
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 INNER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- rsult of left join
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT OUTER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- rsult of right join
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 RIGHT OUTER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- rsult of FULL join
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 FULL OUTER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- result of no partition to join
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) WHERE heap_tbl_scan_test_03.SN > 40;
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) WHERE heap_tbl_scan_test_04.SN > 40;
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) WHERE heap_tbl_scan_test_04.SN > 40 AND heap_tbl_scan_test_03.SN > 40;

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
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 CROSS JOIN heap_tbl_scan_test_04;
-- explain inner join 
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 INNER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- explain left outer join
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT OUTER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- explain right outer join
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 RIGHT OUTER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- explain full outer join
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 FULL OUTER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- explain for no partition to join
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) WHERE heap_tbl_scan_test_03.SN > 40; 
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) WHERE heap_tbl_scan_test_04.SN > 40;
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) WHERE heap_tbl_scan_test_04.SN > 40 AND heap_tbl_scan_test_03.SN > 40;
-- rsult of cross join
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 CROSS JOIN heap_tbl_scan_test_04 order by 1, 2;
-- rsult of inner joi
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 INNER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- rsult of left join
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT OUTER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- rsult of right join
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 RIGHT OUTER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- rsult of FULL join
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 FULL OUTER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- explain for no partition to join
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) WHERE heap_tbl_scan_test_03.SN > 40; 
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) WHERE heap_tbl_scan_test_04.SN > 40;
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) WHERE heap_tbl_scan_test_04.SN > 40 AND heap_tbl_scan_test_03.SN > 40;
-- result of no partition to join
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) WHERE heap_tbl_scan_test_03.SN > 40;
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) WHERE heap_tbl_scan_test_04.SN > 40;
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) WHERE heap_tbl_scan_test_04.SN > 40 AND heap_tbl_scan_test_03.SN > 40;

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
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 CROSS JOIN heap_tbl_scan_test_04;
-- explain inner join 
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 INNER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- explain left outer join
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT OUTER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- explain right outer join
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 RIGHT OUTER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- explain full outer join
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 FULL OUTER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- explain for no partition to join
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) WHERE heap_tbl_scan_test_03.SN > 40; 
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) WHERE heap_tbl_scan_test_04.SN > 40;
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) WHERE heap_tbl_scan_test_04.SN > 40 AND heap_tbl_scan_test_03.SN > 40;
-- rsult of cross join
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 CROSS JOIN heap_tbl_scan_test_04 order by 1, 2;
-- rsult of inner joi
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 INNER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- rsult of left join
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT OUTER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- rsult of right join
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 RIGHT OUTER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- rsult of FULL join
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 FULL OUTER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- explain for no partition to join
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) WHERE heap_tbl_scan_test_03.SN > 40; 
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) WHERE heap_tbl_scan_test_04.SN > 40;
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) WHERE heap_tbl_scan_test_04.SN > 40 AND heap_tbl_scan_test_03.SN > 40;
-- result of no partition to join
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) WHERE heap_tbl_scan_test_03.SN > 40;
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) WHERE heap_tbl_scan_test_04.SN > 40;
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) WHERE heap_tbl_scan_test_04.SN > 40 AND heap_tbl_scan_test_03.SN > 40;

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
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 CROSS JOIN heap_tbl_scan_test_04;
-- explain inner join 
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 INNER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- explain left outer join
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT OUTER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- explain right outer join
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 RIGHT OUTER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- explain full outer join
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 FULL OUTER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- explain for no partition to join
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) WHERE heap_tbl_scan_test_03.SN > 40; 
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) WHERE heap_tbl_scan_test_04.SN > 40;
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) WHERE heap_tbl_scan_test_04.SN > 40 AND heap_tbl_scan_test_03.SN > 40;
-- rsult of cross join
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 CROSS JOIN heap_tbl_scan_test_04 order by 1, 2;
-- rsult of inner joi
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 INNER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- rsult of left join
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT OUTER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- rsult of right join
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 RIGHT OUTER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- rsult of FULL join
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 FULL OUTER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- explain for no partition to join
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) WHERE heap_tbl_scan_test_03.SN > 40; 
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) WHERE heap_tbl_scan_test_04.SN > 40;
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) WHERE heap_tbl_scan_test_04.SN > 40 AND heap_tbl_scan_test_03.SN > 40;
-- result of no partition to join
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) WHERE heap_tbl_scan_test_03.SN > 40;
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) WHERE heap_tbl_scan_test_04.SN > 40;
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) WHERE heap_tbl_scan_test_04.SN > 40 AND heap_tbl_scan_test_03.SN > 40;

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
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 CROSS JOIN heap_tbl_scan_test_04;
-- explain inner join 
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 INNER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- explain left outer join
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT OUTER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- explain right outer join
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 RIGHT OUTER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- explain full outer join
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 FULL OUTER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- explain for no partition to join
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) WHERE heap_tbl_scan_test_03.SN > 40; 
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) WHERE heap_tbl_scan_test_04.SN > 40;
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) WHERE heap_tbl_scan_test_04.SN > 40 AND heap_tbl_scan_test_03.SN > 40;
-- rsult of cross join
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 CROSS JOIN heap_tbl_scan_test_04 order by 1, 2;
-- rsult of inner joi
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 INNER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- rsult of left join
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT OUTER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- rsult of right join
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 RIGHT OUTER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- rsult of FULL join
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 FULL OUTER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- result of no partition to join
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) WHERE heap_tbl_scan_test_03.SN > 40;
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) WHERE heap_tbl_scan_test_04.SN > 40;
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) WHERE heap_tbl_scan_test_04.SN > 40 AND heap_tbl_scan_test_03.SN > 40;

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
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 CROSS JOIN heap_tbl_scan_test_04;
-- explain inner join 
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 INNER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- explain left outer join
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT OUTER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- explain right outer join
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 RIGHT OUTER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- explain full outer join
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 FULL OUTER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- explain for no partition to join
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) WHERE heap_tbl_scan_test_03.SN > 40; 
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) WHERE heap_tbl_scan_test_04.SN > 40;
EXPLAIN (COSTS OFF) SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) WHERE heap_tbl_scan_test_04.SN > 40 AND heap_tbl_scan_test_03.SN > 40;
-- rsult of cross join
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 CROSS JOIN heap_tbl_scan_test_04 order by 1, 2;
-- rsult of inner joi
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 INNER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- rsult of left join
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT OUTER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- rsult of right join
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 RIGHT OUTER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- rsult of FULL join
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 FULL OUTER JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) ORDER BY heap_tbl_scan_test_03.SN, heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_03.NAME;
-- result of no partition to join
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) WHERE heap_tbl_scan_test_03.SN > 40;
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) WHERE heap_tbl_scan_test_04.SN > 40;
SELECT heap_tbl_scan_test_03.NAME, heap_tbl_scan_test_04.PHONE FROM heap_tbl_scan_test_03 LEFT JOIN heap_tbl_scan_test_04 ON (heap_tbl_scan_test_03.SN = heap_tbl_scan_test_04.SN) WHERE heap_tbl_scan_test_04.SN > 40 AND heap_tbl_scan_test_03.SN > 40;

-- clean up
DROP TABLE heap_tbl_scan_test_03;
DROP TABLE heap_tbl_scan_test_04;

