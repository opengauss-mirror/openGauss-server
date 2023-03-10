CREATE SCHEMA partition_a_db_schema;
SET CURRENT_SCHEMA TO partition_a_db_schema;
-- -----------------------------------test partitions clause with A compatibility
-- range with partitions clause
CREATE TABLE t_range_partitions_clause (a int, b int, c int)
PARTITION BY RANGE (a,b) PARTITIONS 3;
CREATE TABLE t_range_partitions_clause (a int, b int, c int)
PARTITION BY RANGE (a,b) PARTITIONS 3
(
    PARTITION p1 VALUES LESS THAN (100,100),
    PARTITION p2 VALUES LESS THAN (200,200),
    PARTITION p3 VALUES LESS THAN (MAXVALUE,MAXVALUE)
);
-- list with partitions clause
CREATE TABLE t_list_partitions_clause (a int, b int, c int)
PARTITION BY LIST (a,b) PARTITIONS 3;
CREATE TABLE t_list_partitions_clause (a int, b int, c int)
PARTITION BY LIST (a,b) PARTITIONS 3
(
    PARTITION p1 VALUES ((0,0)),
    PARTITION p2 VALUES ((1,1), (1,2)),
    PARTITION p3 VALUES ((2,1), (2,2), (2,3))
);
-- hash with partitions clause
CREATE TABLE t_hash_partitions_clause (a int, b int, c int)
PARTITION BY HASH(a) PARTITIONS 3;
DROP TABLE t_hash_partitions_clause;
CREATE TABLE t_hash_partitions_clause (a int, b int, c int)
PARTITION BY HASH(a) PARTITIONS 0;
CREATE TABLE t_hash_partitions_clause (a int, b int, c int)
PARTITION BY HASH(a) PARTITIONS -1;
CREATE TABLE t_hash_partitions_clause (a int, b int, c int)
PARTITION BY HASH(a) PARTITIONS 2.5;
CREATE TABLE t_hash_partitions_clause (a int, b int, c int)
PARTITION BY HASH(a) PARTITIONS '5';
CREATE TABLE t_hash_partitions_clause (a int, b int, c int)
PARTITION BY HASH(a) PARTITIONS 1048576;
CREATE TABLE t_hash_partitions_clause (a int, b int, c int)
PARTITION BY HASH(a) PARTITIONS 3
(
    PARTITION p1,
    PARTITION p2,
    PARTITION p3
);
DROP TABLE t_hash_partitions_clause;
CREATE TABLE t_hash_partitions_clause (a int, b int, c int)
PARTITION BY HASH(a) PARTITIONS 4
(
    PARTITION p1,
    PARTITION p2,
    PARTITION p3
);

-- range-range with subpartitions clause
CREATE TABLE t_range_range_subpartitions_clause (a int, b int, c int)
PARTITION BY RANGE (a) PARTITIONS 2 SUBPARTITION BY RANGE(c)
(
    PARTITION p1 VALUES LESS THAN (100) (
        SUBPARTITION p1sub1 VALUES LESS THAN (100),
        SUBPARTITION p1sub2 VALUES LESS THAN (MAXVALUE)
    ),
    PARTITION p2 VALUES LESS THAN (200) (
        SUBPARTITION p2sub1 VALUES LESS THAN (100),
        SUBPARTITION p2sub2 VALUES LESS THAN (MAXVALUE)
    )
);
CREATE TABLE t_range_range_subpartitions_clause (a int, b int, c int)
PARTITION BY RANGE (a) SUBPARTITION BY RANGE(c) SUBPARTITIONS 2
(
    PARTITION p1 VALUES LESS THAN (100) (
        SUBPARTITION p1sub1 VALUES LESS THAN (100),
        SUBPARTITION p1sub2 VALUES LESS THAN (MAXVALUE)
    ),
    PARTITION p2 VALUES LESS THAN (200) (
        SUBPARTITION p2sub1 VALUES LESS THAN (100),
        SUBPARTITION p2sub2 VALUES LESS THAN (MAXVALUE)
    )
);
-- range-list with subpartitions clause
CREATE TABLE t_range_list_subpartitions_clause (a int, b int, c int)
PARTITION BY RANGE (a) PARTITIONS 2 SUBPARTITION BY LIST(c)
(
    PARTITION p1 VALUES LESS THAN (100) (
        SUBPARTITION p1sub1 VALUES (0),
        SUBPARTITION p1sub2 VALUES (1)
    ),
    PARTITION p2 VALUES LESS THAN (200) (
        SUBPARTITION p2sub1 VALUES (0),
        SUBPARTITION p2sub2 VALUES (1)
    )
);
CREATE TABLE t_range_list_subpartitions_clause (a int, b int, c int)
PARTITION BY RANGE (a) SUBPARTITION BY LIST(c) SUBPARTITIONS 2
(
    PARTITION p1 VALUES LESS THAN (100) (
        SUBPARTITION p1sub1 VALUES (0),
        SUBPARTITION p1sub2 VALUES (1)
    ),
    PARTITION p2 VALUES LESS THAN (200) (
        SUBPARTITION p2sub1 VALUES (0),
        SUBPARTITION p2sub2 VALUES (1)
    )
);
-- range-hash with subpartitions clause
CREATE TABLE t_range_hash_subpartitions_clause (a int, b int, c int)
PARTITION BY RANGE (a) SUBPARTITION BY HASH(c) SUBPARTITIONS 2
(
    PARTITION p1 VALUES LESS THAN (100) (
        SUBPARTITION p1sub1,
        SUBPARTITION p1sub2
    ),
    PARTITION p2 VALUES LESS THAN (200) (
        SUBPARTITION p2sub1,
        SUBPARTITION p2sub2
    )
);
DROP TABLE t_range_hash_subpartitions_clause;
CREATE TABLE t_range_hash_subpartitions_clause (a int, b int, c int)
PARTITION BY RANGE (a) SUBPARTITION BY HASH(c) SUBPARTITIONS 2
(
    PARTITION p1 VALUES LESS THAN (100),
    PARTITION p2 VALUES LESS THAN (200)
);
DROP TABLE t_range_hash_subpartitions_clause;
CREATE TABLE t_range_hash_subpartitions_clause (a int, b int, c int)
PARTITION BY RANGE (a) SUBPARTITION BY HASH(c) SUBPARTITIONS 0
(
    PARTITION p1 VALUES LESS THAN (100),
    PARTITION p2 VALUES LESS THAN (200)
);
CREATE TABLE t_range_hash_subpartitions_clause (a int, b int, c int)
PARTITION BY RANGE (a) SUBPARTITION BY HASH(c) SUBPARTITIONS 0.2E+1
(
    PARTITION p1 VALUES LESS THAN (100),
    PARTITION p2 VALUES LESS THAN (200)
);
CREATE TABLE t_range_hash_subpartitions_clause (a int, b int, c int)
PARTITION BY RANGE (a) SUBPARTITION BY HASH(c) SUBPARTITIONS 2.5
(
    PARTITION p1 VALUES LESS THAN (100),
    PARTITION p2 VALUES LESS THAN (200)
);
CREATE TABLE t_range_hash_subpartitions_clause (a int, b int, c int)
PARTITION BY RANGE (a) SUBPARTITION BY HASH(c) SUBPARTITIONS '5'
(
    PARTITION p1 VALUES LESS THAN (100),
    PARTITION p2 VALUES LESS THAN (200)
);
CREATE TABLE t_range_hash_subpartitions_clause (a int, b int, c int)
PARTITION BY RANGE (a) SUBPARTITION BY HASH(c) SUBPARTITIONS 1048576
(
    PARTITION p1 VALUES LESS THAN (100),
    PARTITION p2 VALUES LESS THAN (200)
);

-- list-range with subpartitions clause
CREATE TABLE t_hash_range_subpartitions_clause (a int, b int, c int)
PARTITION BY LIST (a) SUBPARTITION BY RANGE(c) SUBPARTITIONS 2
(
    PARTITION p1 VALUES (100) (
        SUBPARTITION p1sub1 VALUES LESS THAN (100),
        SUBPARTITION p1sub2 VALUES LESS THAN (MAXVALUE,MAXVALUE)
    ),
    PARTITION p2 VALUES (200) (
        SUBPARTITION p2sub1 VALUES LESS THAN (100),
        SUBPARTITION p2sub2 VALUES LESS THAN (MAXVALUE,MAXVALUE)
    )
);
-- list-list with subpartitions clause
CREATE TABLE t_hash_list_subpartitions_clause (a int, b int, c int)
PARTITION BY LIST (a) SUBPARTITION BY LIST(c) SUBPARTITIONS 2
(
    PARTITION p1 VALUES (100) (
        SUBPARTITION p1sub1 VALUES (0),
        SUBPARTITION p1sub2 VALUES (1)
    ),
    PARTITION p2 VALUES (200) (
        SUBPARTITION p2sub1 VALUES (0),
        SUBPARTITION p2sub2 VALUES (1)
    )
);
-- list-hash with subpartitions clause
CREATE TABLE t_hash_hash_subpartitions_clause (a int, b int, c int)
PARTITION BY LIST (a) SUBPARTITION BY HASH(c) SUBPARTITIONS 2
(
    PARTITION p1 VALUES (100) (
        SUBPARTITION p1sub1,
        SUBPARTITION p1sub2
    ),
    PARTITION p2 VALUES (200) (
        SUBPARTITION p2sub1,
        SUBPARTITION p2sub2
    )
);
DROP TABLE t_hash_hash_subpartitions_clause;
CREATE TABLE t_hash_hash_subpartitions_clause (a int, b int, c int)
PARTITION BY LIST (a) SUBPARTITION BY HASH(c) SUBPARTITIONS 2
(
    PARTITION p1 VALUES (100),
    PARTITION p2 VALUES (200)
);
DROP TABLE t_hash_hash_subpartitions_clause;
CREATE TABLE t_hash_hash_subpartitions_clause (a int, b int, c int)
PARTITION BY LIST (a) PARTITIONS 2 SUBPARTITION BY HASH(c) SUBPARTITIONS 2;

-- hash-range with subpartitions clause
CREATE TABLE t_hash_range_subpartitions_clause (a int, b int, c int)
PARTITION BY HASH (a) PARTITIONS 2 SUBPARTITION BY RANGE(c)
(
    PARTITION p1 (
        SUBPARTITION p1sub1 VALUES LESS THAN (100),
        SUBPARTITION p1sub2 VALUES LESS THAN (MAXVALUE)
    ),
    PARTITION p2 (
        SUBPARTITION p2sub1 VALUES LESS THAN (100),
        SUBPARTITION p2sub2 VALUES LESS THAN (MAXVALUE)
    )
);
DROP TABLE t_hash_range_subpartitions_clause;
-- hash-list with subpartitions clause
CREATE TABLE t_hash_list_subpartitions_clause (a int, b int, c int)
PARTITION BY HASH (a) PARTITIONS 2 SUBPARTITION BY LIST(c)
(
    PARTITION p1 (
        SUBPARTITION p1sub1 VALUES (0),
        SUBPARTITION p1sub2 VALUES (1)
    ),
    PARTITION p2 (
        SUBPARTITION p2sub1 VALUES (0),
        SUBPARTITION p2sub2 VALUES (1)
    )
);
DROP TABLE t_hash_list_subpartitions_clause;
-- hash-hash with subpartitions clause
CREATE TABLE t_hash_hash_subpartitions_clause (a int, b int, c int)
PARTITION BY HASH (a) PARTITIONS 2 SUBPARTITION BY HASH(c)
(
    PARTITION p1 (
        SUBPARTITION p1sub1,
        SUBPARTITION p1sub2
    ),
    PARTITION p2 (
        SUBPARTITION p2sub1,
        SUBPARTITION p2sub2
    )
);
DROP TABLE t_hash_hash_subpartitions_clause;
CREATE TABLE t_hash_hash_subpartitions_clause (a int, b int, c int)
PARTITION BY HASH (a) PARTITIONS 2 SUBPARTITION BY HASH(c) SUBPARTITIONS 2
(
    PARTITION p1 (
        SUBPARTITION p1sub1,
        SUBPARTITION p1sub2
    ),
    PARTITION p2 (
        SUBPARTITION p2sub1,
        SUBPARTITION p2sub2
    )
);
DROP TABLE t_hash_hash_subpartitions_clause;
CREATE TABLE t_hash_hash_subpartitions_clause (a int, b int, c int)
PARTITION BY HASH (a) SUBPARTITION BY HASH(c) SUBPARTITIONS 2
(
    PARTITION p1 (
        SUBPARTITION p1sub1,
        SUBPARTITION p1sub2
    ),
    PARTITION p2 (
        SUBPARTITION p2sub1,
        SUBPARTITION p2sub2
    )
);
DROP TABLE t_hash_hash_subpartitions_clause;
CREATE TABLE t_hash_hash_subpartitions_clause (a int, b int, c int)
PARTITION BY HASH (a) SUBPARTITION BY HASH(c) SUBPARTITIONS 2
(
    PARTITION p1,
    PARTITION p2
);
DROP TABLE t_hash_hash_subpartitions_clause;
CREATE TABLE T_HASH_HASH_SUBPARTITIONS_CLAUSE (a int, b int, c int)
PARTITION BY HASH(a) PARTITIONS 2 SUBPARTITION BY HASH(c) SUBPARTITIONS 2;
SELECT pg_get_tabledef('T_HASH_HASH_SUBPARTITIONS_CLAUSE');
DROP TABLE T_HASH_HASH_SUBPARTITIONS_CLAUSE;
CREATE TABLE T_HASH_HASH_SUBPARTITIONS_CLAUSE (a int, b int, c int)
PARTITION BY HASH (a) SUBPARTITION BY HASH(c) SUBPARTITIONS 2;
DROP TABLE T_HASH_HASH_SUBPARTITIONS_CLAUSE;
CREATE TABLE T_HASH_HASH_SUBPARTITIONS_CLAUSE (a int, b int, c int)
PARTITION BY HASH (a) PARTITIONS 2 SUBPARTITION BY HASH(c);
DROP TABLE T_HASH_HASH_SUBPARTITIONS_CLAUSE;
CREATE TABLE T_HASH_HASH_SUBPARTITIONS_CLAUSE (a int, b int, c int)
PARTITION BY HASH (a) SUBPARTITION BY HASH(c);
SELECT pg_get_tabledef('T_HASH_HASH_SUBPARTITIONS_CLAUSE');
DROP TABLE T_HASH_HASH_SUBPARTITIONS_CLAUSE;
CREATE TABLE T_HASH_PARTITIONS_CLAUSE (a int, b int, c int)
PARTITION BY HASH (a) PARTITIONS 2;
SELECT pg_get_tabledef('T_HASH_PARTITIONS_CLAUSE');
DROP TABLE T_HASH_PARTITIONS_CLAUSE;
CREATE TABLE T_HASH_PARTITIONS_CLAUSE (a int, b int, c int)
PARTITION BY HASH (a);
SELECT pg_get_tabledef('T_HASH_PARTITIONS_CLAUSE');
DROP TABLE T_HASH_PARTITIONS_CLAUSE;

-- -----------------------------------test A compatibility syntax error
CREATE TABLE t_multi_keys_range (a int, b int, c int)
PARTITION BY RANGE(a)
(
    PARTITION p1 VALUES LESS THAN (100),
    PARTITION p2 VALUES LESS THAN (200),
    PARTITION p3 VALUES LESS THAN (300),
    PARTITION p4 VALUES LESS THAN MAXVALUE
);

CREATE TABLE t_multi_keys_range (a int, b int, c int)
PARTITION BY RANGE COLUMNS(a,b)
(
    PARTITION p1 VALUES LESS THAN (100,100),
    PARTITION p2 VALUES LESS THAN (200,200),
    PARTITION p3 VALUES LESS THAN (300,300),
    PARTITION p4 VALUES LESS THAN (400,MAXVALUE),
    PARTITION p5 VALUES LESS THAN (MAXVALUE,MAXVALUE)
);

CREATE TABLE t_multi_keys_range (a int, b int, c int)
PARTITION BY RANGE (a,b) PARTITIONS 5
(
    PARTITION p1 VALUES LESS THAN (100,100),
    PARTITION p2 VALUES LESS THAN (200,200),
    PARTITION p3 VALUES LESS THAN (300,300),
    PARTITION p4 VALUES LESS THAN (400,MAXVALUE),
    PARTITION p5 VALUES LESS THAN (MAXVALUE,MAXVALUE)
);

CREATE TABLE t_multi_keys_list (a int, b int, c int)
PARTITION BY LIST COLUMNS(a,b)
(
    PARTITION p1 VALUES IN ( (0,0), (NULL,NULL) ),
    PARTITION p2 VALUES IN ( (0,1), (0,2), (0,3), (1,1), (1,2) ),
    PARTITION p3 VALUES IN ( (1,0), (2,0), (2,1), (3,0), (3,1) ),
    PARTITION p4 VALUES IN ( (1,3), (2,2), (2,3), (3,2), (3,3) )
);

CREATE TABLE t_multi_keys_list (a int, b int, c int)
PARTITION BY LIST (a,b)
(
    PARTITION p1 VALUES IN ( (0,0), (NULL,NULL) ),
    PARTITION p2 VALUES IN ( (0,1), (0,2), (0,3), (1,1), (1,2) ),
    PARTITION p3 VALUES IN ( (1,0), (2,0), (2,1), (3,0), (3,1) ),
    PARTITION p4 VALUES IN ( (1,3), (2,2), (2,3), (3,2), (3,3) )
);

CREATE TABLE t_multi_keys_list (a int, b int, c int)
PARTITION BY LIST (a,b) PARTITIONS 5
(
    PARTITION p1 VALUES ( (0,0), (NULL,NULL) ),
    PARTITION p2 VALUES ( (0,1), (0,2), (0,3), (1,1), (1,2) ),
    PARTITION p3 VALUES ( (1,0), (2,0), (2,1), (3,0), (3,1) ),
    PARTITION p4 VALUES ( (1,3), (2,2), (2,3), (3,2), (3,3) )
);

CREATE TABLE t_part_by_key (a int, b int, c int)
PARTITION BY KEY(a)
(
    PARTITION p1,
    PARTITION p2,
    PARTITION p3,
    PARTITION p4,
    PARTITION p5
);

CREATE TABLE t_multi_keys_list_tbspc (a int, b varchar(4), c int)
PARTITION BY LIST (a,b)
(
    PARTITION p1 VALUES ( (0,NULL) ) TABLESPACE = pg_default,
    PARTITION p2 VALUES ( (0,'1'), (0,'2'), (0,'3'), (1,'1'), (1,'2') )
);
DROP TABLE t_multi_keys_list_tbspc;
-- -----------------------------------test multi list keys with A compatibility
CREATE TABLE t_multi_keys_list_default (a int, b int, c int)
PARTITION BY LIST (a,b)
(
    PARTITION p1 VALUES ( DEFAULT ),
    PARTITION p2 VALUES ( (0,1), (0,2), (0,3), (1,1), (1,2) ),
    PARTITION p3 VALUES ( (NULL,0), (2,1) ),
    PARTITION p4 VALUES ( (3,2), (NULL,NULL) ),
    PARTITION pd VALUES ( DEFAULT )
);
CREATE TABLE t_multi_keys_list_default (a int, b varchar(4), c int)
PARTITION BY LIST (a,b)
(
    PARTITION p1 VALUES ( (0,NULL) ),
    PARTITION p2 VALUES ( (0,'1'), (0,'2'), (0,'3'), (1,'1'), (1,'2') ),
    PARTITION p3 VALUES ( (NULL,'0'), (2,'1') ),
    PARTITION p4 VALUES ( (3,'2'), (NULL,NULL) ),
    PARTITION pd VALUES ( DEFAULT )
);
CREATE INDEX t_multi_keys_list_default_idx_l ON t_multi_keys_list_default(a,b,c) LOCAL;
SELECT pg_get_tabledef('t_multi_keys_list_default'::regclass);

INSERT INTO t_multi_keys_list_default VALUES(0,NULL,0);
SELECT * FROM t_multi_keys_list_default PARTITION(p1) ORDER BY a,b;
INSERT INTO t_multi_keys_list_default VALUES(0,'1',0);
INSERT INTO t_multi_keys_list_default VALUES(0,'2',0);
INSERT INTO t_multi_keys_list_default VALUES(0,'3',0);
INSERT INTO t_multi_keys_list_default VALUES(1,'1',0);
INSERT INTO t_multi_keys_list_default VALUES(1,'2',0);
SELECT * FROM t_multi_keys_list_default PARTITION(p2) ORDER BY a,b;
INSERT INTO t_multi_keys_list_default VALUES(NULL,0,0);
INSERT INTO t_multi_keys_list_default VALUES(2,'1',0);
SELECT * FROM t_multi_keys_list_default PARTITION(p3) ORDER BY a,b;
INSERT INTO t_multi_keys_list_default VALUES(3,'2',0);
INSERT INTO t_multi_keys_list_default VALUES(NULL,NULL,0);
SELECT * FROM t_multi_keys_list_default PARTITION(p4) ORDER BY a,b;
INSERT INTO t_multi_keys_list_default VALUES(4,'4',4);
SELECT * FROM t_multi_keys_list_default PARTITION(pd) ORDER BY a,b;

EXPLAIN (costs false)
SELECT a FROM t_multi_keys_list_default WHERE a IS NULL;
SELECT a FROM t_multi_keys_list_default WHERE a IS NULL;
EXPLAIN (costs false)
SELECT a FROM t_multi_keys_list_default WHERE a = 0;
SELECT a FROM t_multi_keys_list_default WHERE a = 0;
EXPLAIN (costs false)
SELECT b FROM t_multi_keys_list_default WHERE b IS NULL;
SELECT b FROM t_multi_keys_list_default WHERE b IS NULL;
EXPLAIN (costs false)
SELECT b FROM t_multi_keys_list_default WHERE b = '1';
SELECT b FROM t_multi_keys_list_default WHERE b = '1';
EXPLAIN (costs false)
SELECT a FROM t_multi_keys_list_default WHERE a = 4;
SELECT a FROM t_multi_keys_list_default WHERE a = 4;
EXPLAIN (costs false)
SELECT a,b FROM t_multi_keys_list_default WHERE a < 1 ORDER BY 1,2;
SELECT a,b FROM t_multi_keys_list_default WHERE a < 1 ORDER BY 1,2;
EXPLAIN (costs false)
SELECT * FROM t_multi_keys_list_default WHERE (a,b) = (0,'1');
SELECT * FROM t_multi_keys_list_default WHERE (a,b) = (0,'1');
EXPLAIN (costs false)
SELECT * FROM t_multi_keys_list_default WHERE (a,b) = (NULL,'0');
SELECT * FROM t_multi_keys_list_default WHERE (a,b) = (NULL,'0');
EXPLAIN (costs false)
SELECT * FROM t_multi_keys_list_default WHERE a IS NULL AND b = '0';
SELECT * FROM t_multi_keys_list_default WHERE a IS NULL AND b = '0';
EXPLAIN (costs false)
SELECT * FROM t_multi_keys_list_default WHERE (a IS NULL AND a = 0) AND b = '0' ORDER BY 1,2;
SELECT * FROM t_multi_keys_list_default WHERE (a IS NULL AND a = 0) AND b = '0' ORDER BY 1,2;
EXPLAIN (costs false)
SELECT * FROM t_multi_keys_list_default WHERE (a IS NULL OR a = 3) AND b = '2' ORDER BY 1,2;
SELECT * FROM t_multi_keys_list_default WHERE (a IS NULL OR a = 3) AND b = '2' ORDER BY 1,2;
EXPLAIN (costs false)
SELECT * FROM t_multi_keys_list_default WHERE (a IS NULL OR a IS NOT NULL) AND b = '0' ORDER BY 1,2;
SELECT * FROM t_multi_keys_list_default WHERE (a IS NULL OR a IS NOT NULL) AND b = '0' ORDER BY 1,2;
EXPLAIN (costs false)
SELECT * FROM t_multi_keys_list_default WHERE b IS NOT NULL AND a = 0 ORDER BY 1,2;
SELECT * FROM t_multi_keys_list_default WHERE b IS NOT NULL AND a = 0 ORDER BY 1,2;
EXPLAIN (costs false)
SELECT * FROM t_multi_keys_list_default WHERE (b IS NOT NULL AND b = '1') AND a = 2 ORDER BY 1,2;
SELECT * FROM t_multi_keys_list_default WHERE (b IS NOT NULL AND b = '1') AND a = 2 ORDER BY 1,2;
EXPLAIN (costs false)
SELECT * FROM t_multi_keys_list_default WHERE (b IS NOT NULL OR b = '0') AND a = 0 ORDER BY 1,2;
SELECT * FROM t_multi_keys_list_default WHERE (b IS NOT NULL OR b = '0') AND a = 0 ORDER BY 1,2;
EXPLAIN (costs false)
SELECT * FROM t_multi_keys_list_default WHERE a IS NULL AND b IS NULL ORDER BY 1,2;
SELECT * FROM t_multi_keys_list_default WHERE a IS NULL AND b IS NULL ORDER BY 1,2;
EXPLAIN (costs false)
SELECT * FROM t_multi_keys_list_default WHERE a IS NULL AND b IS NOT NULL ORDER BY 1,2;
SELECT * FROM t_multi_keys_list_default WHERE a IS NULL AND b IS NOT NULL ORDER BY 1,2;
EXPLAIN (costs false)
SELECT * FROM t_multi_keys_list_default WHERE a IS NOT NULL AND b IS NULL ORDER BY 1,2;
SELECT * FROM t_multi_keys_list_default WHERE a IS NOT NULL AND b IS NULL ORDER BY 1,2;
EXPLAIN (costs false)
SELECT * FROM t_multi_keys_list_default WHERE a IS NOT NULL AND b IS NOT NULL ORDER BY 1,2;
SELECT * FROM t_multi_keys_list_default WHERE a IS NOT NULL AND b IS NOT NULL ORDER BY 1,2;
EXPLAIN (costs false)
SELECT * FROM t_multi_keys_list_default WHERE a IS NULL OR b IS NULL ORDER BY 1,2;
SELECT * FROM t_multi_keys_list_default WHERE a IS NULL OR b IS NULL ORDER BY 1,2;
EXPLAIN (costs false)
SELECT * FROM t_multi_keys_list_default WHERE a IS NULL OR b IS NOT NULL ORDER BY 1,2;
SELECT * FROM t_multi_keys_list_default WHERE a IS NULL OR b IS NOT NULL ORDER BY 1,2;
EXPLAIN (costs false)
SELECT * FROM t_multi_keys_list_default WHERE a IS NOT NULL OR b IS NULL ORDER BY 1,2;
SELECT * FROM t_multi_keys_list_default WHERE a IS NOT NULL OR b IS NULL ORDER BY 1,2;
EXPLAIN (costs false)
SELECT * FROM t_multi_keys_list_default WHERE a IS NOT NULL OR b IS NOT NULL ORDER BY 1,2;
SELECT * FROM t_multi_keys_list_default WHERE a IS NOT NULL OR b IS NOT NULL ORDER BY 1,2;
EXPLAIN (costs false)
SELECT * FROM t_multi_keys_list_default WHERE a IS NULL OR a IS NULL ORDER BY 1,2;
SELECT * FROM t_multi_keys_list_default WHERE a IS NULL OR a IS NULL ORDER BY 1,2;
EXPLAIN (costs false)
SELECT * FROM t_multi_keys_list_default WHERE a IS NULL AND a IS NULL ORDER BY 1,2;
SELECT * FROM t_multi_keys_list_default WHERE a IS NULL AND a IS NULL ORDER BY 1,2;
EXPLAIN (costs false)
SELECT * FROM t_multi_keys_list_default WHERE a IS NOT NULL OR a IS NOT NULL ORDER BY 1,2;
SELECT * FROM t_multi_keys_list_default WHERE a IS NOT NULL OR a IS NOT NULL ORDER BY 1,2;
EXPLAIN (costs false)
SELECT * FROM t_multi_keys_list_default WHERE a IS NOT NULL AND a IS NOT NULL ORDER BY 1,2;
SELECT * FROM t_multi_keys_list_default WHERE a IS NOT NULL AND a IS NOT NULL ORDER BY 1,2;
EXPLAIN (costs false)
SELECT * FROM t_multi_keys_list_default WHERE a IS NULL OR a IS NOT NULL ORDER BY 1,2;
SELECT * FROM t_multi_keys_list_default WHERE a IS NULL OR a IS NOT NULL ORDER BY 1,2;
EXPLAIN (costs false)
SELECT * FROM t_multi_keys_list_default WHERE a IS NULL AND a IS NOT NULL ORDER BY 1,2;
SELECT * FROM t_multi_keys_list_default WHERE a IS NULL AND a IS NOT NULL ORDER BY 1,2;
EXPLAIN (costs false)
SELECT * FROM t_multi_keys_list_default WHERE a = 0 OR b = '0' ORDER BY 1,2;
SELECT * FROM t_multi_keys_list_default WHERE a = 0 OR b = '0' ORDER BY 1,2;
EXPLAIN (costs false)
SELECT * FROM t_multi_keys_list_default WHERE (a,b) IN ((NULL,'0'), (3,'2')) ORDER BY 1,2;
SELECT * FROM t_multi_keys_list_default WHERE (a,b) IN ((NULL,'0'), (3,'2')) ORDER BY 1,2;
EXPLAIN (costs false)
SELECT * FROM t_multi_keys_list_default WHERE (a,b) =ANY(ARRAY[(2,'1'::varchar), (3,'2'::varchar)]) ORDER BY 1,2;
SELECT * FROM t_multi_keys_list_default WHERE (a,b) =ANY(ARRAY[(2,'1'::varchar), (3,'2'::varchar)]) ORDER BY 1,2;
EXPLAIN (costs false)
SELECT * FROM t_multi_keys_list_default WHERE a =ANY(ARRAY[2, 3]) ORDER BY 1,2;
SELECT * FROM t_multi_keys_list_default WHERE a =ANY(ARRAY[2, 3]) ORDER BY 1,2;
PREPARE part_bdb_stmt(varchar) as SELECT a,b FROM t_multi_keys_list_default WHERE b = $1 ORDER BY 1,2;
EXPLAIN (costs false)
EXECUTE part_bdb_stmt('3');
EXECUTE part_bdb_stmt('1');
EXECUTE part_bdb_stmt('2');
PREPARE part_bdb_stmt1(int) as SELECT a,b FROM t_multi_keys_list_default WHERE a != $1 ORDER BY 1,2;
EXPLAIN (costs false)
EXECUTE part_bdb_stmt1(3);
EXECUTE part_bdb_stmt1(1);
EXECUTE part_bdb_stmt1(2);
PREPARE part_bdb_stmt2(int) as SELECT a,b FROM t_multi_keys_list_default WHERE a >= $1 ORDER BY 1,2;
EXPLAIN (costs false)
EXECUTE part_bdb_stmt2(3);
EXECUTE part_bdb_stmt2(1);
EXECUTE part_bdb_stmt2(2);
PREPARE part_bdb_stmt3(int, varchar) as SELECT a,b FROM t_multi_keys_list_default WHERE (a,b) = ($1,$2);
EXPLAIN (costs false)
EXECUTE part_bdb_stmt3(0,'1');
EXECUTE part_bdb_stmt3(0,'1');
EXECUTE part_bdb_stmt3(3,'2');

UPDATE t_multi_keys_list_default SET a=2, b='1' where a=1 and b='1';
SELECT * FROM t_multi_keys_list_default PARTITION(p3) ORDER BY a,b;
UPDATE t_multi_keys_list_default SET a=NULL, b='0' where a=1 and b='2';
SELECT * FROM t_multi_keys_list_default PARTITION(p3) ORDER BY a,b;

EXPLAIN (costs false)
DELETE t_multi_keys_list_default PARTITION(p3) where b = '0';
DELETE t_multi_keys_list_default PARTITION(p3) where b = '0';
SELECT * FROM t_multi_keys_list_default PARTITION(p3) ORDER BY a,b;
EXPLAIN (costs false)
DELETE t_multi_keys_list_default PARTITION FOR(0,NULL);
DELETE t_multi_keys_list_default PARTITION FOR(0,NULL);
SELECT * FROM t_multi_keys_list_default PARTITION(p1) ORDER BY a,b;
EXPLAIN (costs false)
DELETE t_multi_keys_list_default PARTITION FOR(0,'3');
DELETE t_multi_keys_list_default PARTITION FOR(0,'3');
SELECT * FROM t_multi_keys_list_default PARTITION(p2) ORDER BY a,b;
-- alter table partition
CREATE INDEX test_multi_list_key_gi on t_multi_keys_list_default(c);
CREATE TABLESPACE part_adb_temp_tbspc RELATIVE LOCATION 'tablespace/part_adb_temp_tbspc';
ALTER TABLE t_multi_keys_list_default MOVE PARTITION FOR(0,NULL) TABLESPACE part_adb_temp_tbspc;
SELECT pg_get_tabledef('t_multi_keys_list_default'::regclass);

CREATE TABLE t_alter_partition_temp (a int, b varchar(4), c int);
INSERT INTO t_alter_partition_temp VALUES(NULL,'0',1);
INSERT INTO t_alter_partition_temp VALUES(2,'1',2);
CREATE INDEX t_alter_partition_temp_idx_l ON t_alter_partition_temp(a,b,c);
SELECT * FROM t_alter_partition_temp ORDER BY a,b,c;
SELECT * FROM t_multi_keys_list_default PARTITION(p3) ORDER BY a,b,c;
ALTER TABLE t_multi_keys_list_default EXCHANGE PARTITION (p3) WITH TABLE t_alter_partition_temp UPDATE GLOBAL INDEX;
SELECT * FROM t_alter_partition_temp ORDER BY a,b,c;
SELECT * FROM t_multi_keys_list_default PARTITION(p3) ORDER BY a,b,c;
DROP TABLE IF EXISTS t_alter_partition_temp;

ALTER TABLE t_multi_keys_list_default ADD PARTITION p5 VALUES ((2,1));

ALTER TABLE t_multi_keys_list_default DROP PARTITION FOR (1,'5');
ALTER TABLE t_multi_keys_list_default DROP PARTITION FOR (2,'1') UPDATE GLOBAL INDEX;
SELECT * FROM t_multi_keys_list_default PARTITION FOR (1,'5') ORDER BY a,b;
SELECT * FROM t_multi_keys_list_default PARTITION FOR (2,'1') ORDER BY a,b;
ALTER TABLE t_multi_keys_list_default TRUNCATE PARTITION FOR (NULL,NULL) UPDATE GLOBAL INDEX;
SELECT * FROM t_multi_keys_list_default PARTITION FOR (NULL,NULL) ORDER BY a,b;
ALTER TABLE t_multi_keys_list_default RENAME PARTITION FOR (0,NULL) TO p0;
ALTER TABLE t_multi_keys_list_default ADD PARTITION pd VALUES (DEFAULT);
SELECT pg_get_tabledef('t_multi_keys_list_default'::regclass);

-- test views
SELECT table_name,partitioning_type,partition_count,partitioning_key_count,subpartitioning_type FROM MY_PART_TABLES WHERE table_name = 't_multi_keys_list_default' ORDER BY 1;
SELECT table_name,partition_name,high_value,subpartition_count FROM MY_TAB_PARTITIONS WHERE table_name = 't_multi_keys_list_default' ORDER BY 1,2;
SELECT table_name,partition_name,subpartition_name,high_value,high_value_length FROM MY_TAB_SUBPARTITIONS WHERE table_name = 't_multi_keys_list_default' ORDER BY 1,2,3;
SELECT table_name,index_name,partition_count,partitioning_key_count,partitioning_type,subpartitioning_type FROM MY_PART_INDEXES WHERE table_name = 't_multi_keys_list_default' ORDER BY 1,2;
SELECT index_name,partition_name,high_value,high_value_length FROM MY_IND_PARTITIONS WHERE index_name = 't_multi_keys_list_default_idx_l' ORDER BY 1,2;
SELECT index_name,partition_name,subpartition_name,high_value,high_value_length FROM MY_IND_SUBPARTITIONS WHERE index_name = 't_multi_keys_list_default_idx_l' ORDER BY 1,2,3;

-- test partition key value and datatype not matched
CREATE TABLE t_single_key_list_value (a int, b int, c int)
PARTITION BY LIST (a)
(
    PARTITION p1 VALUES ( 0 ),
    PARTITION p2 VALUES ( 1 ),
    PARTITION p3 VALUES ( 2, date '12-10-2010' )
); -- ERROR
CREATE TABLE t_multi_keys_list_value (a int, b int, c int)
PARTITION BY LIST (a,b)
(
    PARTITION p1 VALUES ( (0,0) ),
    PARTITION p2 VALUES ( (0,1) ),
    PARTITION p3 VALUES ( (2,1), (NULL,date '12-10-2010') )
); -- ERROR

DROP TABLE IF EXISTS t_multi_keys_list_default;
DROP TABLESPACE part_adb_temp_tbspc;
DROP SCHEMA partition_a_db_schema CASCADE;

-- -----------------------------------test with B compatibility
create database part_bdb WITH ENCODING 'UTF-8' dbcompatibility 'B';
\c part_bdb
CREATE SCHEMA partition_b_db_schema;
SET CURRENT_SCHEMA TO partition_b_db_schema;

-- -----------------------------------test partitions clause with B compatibility
-- range with partitions clause
CREATE TABLE t_range_partitions_clause (a int, b int, c int)
PARTITION BY RANGE COLUMNS(a,b) PARTITIONS 3;
CREATE TABLE t_range_partitions_clause (a int, b int, c int)
PARTITION BY RANGE COLUMNS(a,b) PARTITIONS 3
(
    PARTITION p1 VALUES LESS THAN (100,100),
    PARTITION p2 VALUES LESS THAN (200,200),
    PARTITION p3 VALUES LESS THAN (MAXVALUE,MAXVALUE)
);
DROP TABLE t_range_partitions_clause;
CREATE TABLE t_range_partitions_clause (a int, b int, c int)
PARTITION BY RANGE COLUMNS(a,b) PARTITIONS 2
(
    PARTITION p1 VALUES LESS THAN (100,100),
    PARTITION p2 VALUES LESS THAN (200,200),
    PARTITION p3 VALUES LESS THAN (MAXVALUE,MAXVALUE)
);
-- list with partitions clause
CREATE TABLE t_list_partitions_clause (a int, b int, c int)
PARTITION BY LIST COLUMNS(a,b) PARTITIONS 3;
CREATE TABLE t_list_partitions_clause (a int, b int, c int)
PARTITION BY LIST COLUMNS(a,b) PARTITIONS 3
(
    PARTITION p1 VALUES IN ((0,0)),
    PARTITION p2 VALUES IN ((1,1), (1,2)),
    PARTITION p3 VALUES IN ((2,1), (2,2), (2,3))
);
DROP TABLE t_list_partitions_clause;
CREATE TABLE t_list_partitions_clause (a int, b int, c int)
PARTITION BY LIST COLUMNS(a,b) PARTITIONS 1
(
    PARTITION p1 VALUES IN ((0,0)),
    PARTITION p2 VALUES IN ((1,1), (1,2)),
    PARTITION p3 VALUES IN ((2,1), (2,2), (2,3))
);
-- key with partitions clause
CREATE TABLE t_hash_partitions_clause (a int, b int, c int)
PARTITION BY KEY(a) PARTITIONS 3;
DROP TABLE t_hash_partitions_clause;
CREATE TABLE t_hash_partitions_clause (a int, b int, c int)
PARTITION BY KEY(a) PARTITIONS 0;
CREATE TABLE t_hash_partitions_clause (a int, b int, c int)
PARTITION BY KEY(a) PARTITIONS -1;
CREATE TABLE t_hash_partitions_clause (a int, b int, c int)
PARTITION BY KEY(a) PARTITIONS 2.5;
CREATE TABLE t_hash_partitions_clause (a int, b int, c int)
PARTITION BY KEY(a) PARTITIONS '5';
CREATE TABLE t_hash_partitions_clause (a int, b int, c int)
PARTITION BY KEY(a) PARTITIONS 1048576;
CREATE TABLE t_hash_partitions_clause (a int, b int, c int)
PARTITION BY KEY(a) PARTITIONS 3
(
    PARTITION p1,
    PARTITION p2,
    PARTITION p3
);
DROP TABLE t_hash_partitions_clause;
CREATE TABLE t_hash_partitions_clause (a int, b int, c int)
PARTITION BY KEY(a) PARTITIONS 4
(
    PARTITION p1,
    PARTITION p2,
    PARTITION p3
);

-- range-range with subpartitions clause
CREATE TABLE t_range_range_subpartitions_clause (a int, b int, c int)
PARTITION BY RANGE COLUMNS(a) PARTITIONS 2 SUBPARTITION BY RANGE(c)
(
    PARTITION p1 VALUES LESS THAN (100) (
        SUBPARTITION p1sub1 VALUES LESS THAN (100),
        SUBPARTITION p1sub2 VALUES LESS THAN (MAXVALUE)
    ),
    PARTITION p2 VALUES LESS THAN (200) (
        SUBPARTITION p2sub1 VALUES LESS THAN (100),
        SUBPARTITION p2sub2 VALUES LESS THAN (MAXVALUE)
    )
);
DROP TABLE t_range_range_subpartitions_clause;
CREATE TABLE t_range_range_subpartitions_clause (a int, b int, c int)
PARTITION BY RANGE COLUMNS(a) PARTITIONS 3 SUBPARTITION BY RANGE(c)
(
    PARTITION p1 VALUES LESS THAN (100) (
        SUBPARTITION p1sub1 VALUES LESS THAN (100),
        SUBPARTITION p1sub2 VALUES LESS THAN (MAXVALUE)
    ),
    PARTITION p2 VALUES LESS THAN (200) (
        SUBPARTITION p2sub1 VALUES LESS THAN (100),
        SUBPARTITION p2sub2 VALUES LESS THAN (MAXVALUE)
    )
);
-- range-list with subpartitions clause
CREATE TABLE t_range_list_subpartitions_clause (a int, b int, c int)
PARTITION BY RANGE COLUMNS(a) PARTITIONS 2 SUBPARTITION BY LIST(c)
(
    PARTITION p1 VALUES LESS THAN (100) (
        SUBPARTITION p1sub1 VALUES (0),
        SUBPARTITION p1sub2 VALUES (1)
    ),
    PARTITION p2 VALUES LESS THAN (200) (
        SUBPARTITION p2sub1 VALUES (0),
        SUBPARTITION p2sub2 VALUES (1)
    )
);
DROP TABLE t_range_list_subpartitions_clause;
CREATE TABLE t_range_list_subpartitions_clause (a int, b int, c int)
PARTITION BY RANGE COLUMNS(a) PARTITIONS 1 SUBPARTITION BY LIST(c)
(
    PARTITION p1 VALUES LESS THAN (100) (
        SUBPARTITION p1sub1 VALUES (0),
        SUBPARTITION p1sub2 VALUES (1)
    ),
    PARTITION p2 VALUES LESS THAN (200) (
        SUBPARTITION p2sub1 VALUES (0),
        SUBPARTITION p2sub2 VALUES (1)
    )
);
-- range-key with subpartitions clause
CREATE TABLE t_range_hash_subpartitions_clause (a int, b int, c int)
PARTITION BY RANGE COLUMNS(a) PARTITIONS 2 SUBPARTITION BY key(c)
(
    PARTITION p1 VALUES LESS THAN (100) (
        SUBPARTITION p1sub1,
        SUBPARTITION p1sub2
    ),
    PARTITION p2 VALUES LESS THAN (200) (
        SUBPARTITION p2sub1,
        SUBPARTITION p2sub2
    )
);
DROP TABLE t_range_hash_subpartitions_clause;
CREATE TABLE t_range_hash_subpartitions_clause (a int, b int, c int)
PARTITION BY RANGE COLUMNS(a) PARTITIONS 2 SUBPARTITION BY key(c) SUBPARTITIONS 2
(
    PARTITION p1 VALUES LESS THAN (100) (
        SUBPARTITION p1sub1,
        SUBPARTITION p1sub2
    ),
    PARTITION p2 VALUES LESS THAN (200) (
        SUBPARTITION p2sub1,
        SUBPARTITION p2sub2
    )
);
DROP TABLE t_range_hash_subpartitions_clause;
CREATE TABLE t_range_hash_subpartitions_clause (a int, b int, c int)
PARTITION BY RANGE COLUMNS(a) SUBPARTITION BY key(c) SUBPARTITIONS 2
(
    PARTITION p1 VALUES LESS THAN (100) (
        SUBPARTITION p1sub1,
        SUBPARTITION p1sub2
    ),
    PARTITION p2 VALUES LESS THAN (200) (
        SUBPARTITION p2sub1,
        SUBPARTITION p2sub2
    )
);
DROP TABLE t_range_hash_subpartitions_clause;
CREATE TABLE t_range_hash_subpartitions_clause (a int, b int, c int)
PARTITION BY RANGE COLUMNS(a) SUBPARTITION BY key(c) SUBPARTITIONS 2
(
    PARTITION p1 VALUES LESS THAN (100),
    PARTITION p2 VALUES LESS THAN (200)
);
DROP TABLE t_range_hash_subpartitions_clause;
CREATE TABLE t_range_hash_subpartitions_clause (a int, b int, c int)
PARTITION BY RANGE COLUMNS(a) SUBPARTITION BY key(c) SUBPARTITIONS 0
(
    PARTITION p1 VALUES LESS THAN (100),
    PARTITION p2 VALUES LESS THAN (200)
);
CREATE TABLE t_range_hash_subpartitions_clause (a int, b int, c int)
PARTITION BY RANGE COLUMNS(a) SUBPARTITION BY key(c) SUBPARTITIONS 0.2E+1
(
    PARTITION p1 VALUES LESS THAN (100),
    PARTITION p2 VALUES LESS THAN (200)
);
CREATE TABLE t_range_hash_subpartitions_clause (a int, b int, c int)
PARTITION BY RANGE COLUMNS(a) SUBPARTITION BY key(c) SUBPARTITIONS 2.5
(
    PARTITION p1 VALUES LESS THAN (100),
    PARTITION p2 VALUES LESS THAN (200)
);
CREATE TABLE t_range_hash_subpartitions_clause (a int, b int, c int)
PARTITION BY RANGE COLUMNS(a) SUBPARTITION BY key(c) SUBPARTITIONS '5'
(
    PARTITION p1 VALUES LESS THAN (100),
    PARTITION p2 VALUES LESS THAN (200)
);
CREATE TABLE t_range_hash_subpartitions_clause (a int, b int, c int)
PARTITION BY RANGE COLUMNS(a) SUBPARTITION BY key(c) SUBPARTITIONS 1048576
(
    PARTITION p1 VALUES LESS THAN (100),
    PARTITION p2 VALUES LESS THAN (200)
);
CREATE TABLE t_range_hash_subpartitions_clause (a int, b int, c int)
PARTITION BY RANGE COLUMNS(a) PARTITIONS 2 SUBPARTITION BY KEY(c) SUBPARTITIONS 3
(
    PARTITION p1 VALUES LESS THAN (100) (
        SUBPARTITION p1sub1,
        SUBPARTITION p1sub2,
        SUBPARTITION p1sub3
    )
);
CREATE TABLE t_range_hash_subpartitions_clause (a int, b int, c int)
PARTITION BY RANGE COLUMNS(a) PARTITIONS 2 SUBPARTITION BY KEY(c) SUBPARTITIONS 3
(
    PARTITION p1 VALUES LESS THAN (100) (
        SUBPARTITION p1sub1,
        SUBPARTITION p1sub2,
        SUBPARTITION p1sub3
    ),
    PARTITION p2 VALUES LESS THAN (200) (
        SUBPARTITION p2sub1,
        SUBPARTITION p2sub2,
        SUBPARTITION p2sub3,
        SUBPARTITION p2sub4
    )
);

CREATE TABLE t_range_hash_subpartitions_clause (a int, b int, c int)
PARTITION BY RANGE COLUMNS(a) PARTITIONS 2 SUBPARTITION BY KEY(c) SUBPARTITIONS 3
(
    PARTITION p1 VALUES LESS THAN (100),
    PARTITION p2 VALUES LESS THAN (200) (
        SUBPARTITION p2sub1,
        SUBPARTITION p2sub2,
        SUBPARTITION p2sub3
    )
);
DROP TABLE t_range_hash_subpartitions_clause;
CREATE TABLE t_range_hash_subpartitions_clause (a int, b int, c int)
PARTITION BY RANGE COLUMNS(a) PARTITIONS 2 SUBPARTITION BY KEY(c) SUBPARTITIONS 3
(
    PARTITION p1 VALUES LESS THAN (100),
    PARTITION p2 VALUES LESS THAN (200) (
        SUBPARTITION p2sub1,
        SUBPARTITION p2sub2,
        SUBPARTITION p1sp0
    )
);
CREATE TABLE t_range_hash_subpartitions_clause (a int, b int, c int)
PARTITION BY RANGE COLUMNS(a) PARTITIONS 2 SUBPARTITION BY HASH(c) SUBPARTITIONS 11
(
    PARTITION p11111111111111111111111111111111111111111111111111111111111 VALUES LESS THAN (100),
    PARTITION p2 VALUES LESS THAN (200)
);
CREATE TABLE t_range_hash_subpartitions_clause (a int, b int, c int)
PARTITION BY RANGE COLUMNS(a) PARTITIONS 2 SUBPARTITION BY HASH(c) SUBPARTITIONS 11
(
    PARTITION p1111111111111111111111111111111111111111111111111111111111 VALUES LESS THAN (100),
    PARTITION p2 VALUES LESS THAN (200)
);
SELECT pg_get_tabledef('t_range_hash_subpartitions_clause'::regclass);
DROP TABLE t_range_hash_subpartitions_clause;

-- list-range with subpartitions clause
CREATE TABLE t_hash_range_subpartitions_clause (a int, b int, c int)
PARTITION BY LIST COLUMNS (a) PARTITIONS 2 SUBPARTITION BY RANGE(c)
(
    PARTITION p1 VALUES IN (100) (
        SUBPARTITION p1sub1 VALUES LESS THAN (100),
        SUBPARTITION p1sub2 VALUES LESS THAN (MAXVALUE)
    ),
    PARTITION p2 VALUES IN (200) (
        SUBPARTITION p2sub1 VALUES LESS THAN (100),
        SUBPARTITION p2sub2 VALUES LESS THAN (MAXVALUE)
    )
);
DROP TABLE t_hash_range_subpartitions_clause;
-- list-list with subpartitions clause
CREATE TABLE t_hash_list_subpartitions_clause (a int, b int, c int)
PARTITION BY LIST COLUMNS (a) PARTITIONS 2 SUBPARTITION BY LIST(c)
(
    PARTITION p1 VALUES IN (100) (
        SUBPARTITION p1sub1 VALUES (0),
        SUBPARTITION p1sub2 VALUES (1)
    ),
    PARTITION p2 VALUES IN (200) (
        SUBPARTITION p2sub1 VALUES (0),
        SUBPARTITION p2sub2 VALUES (1)
    )
);
DROP TABLE t_hash_list_subpartitions_clause;
-- list-key with subpartitions clause
CREATE TABLE t_list_hash_subpartitions_clause (a int, b int, c int)
PARTITION BY LIST COLUMNS (a) PARTITIONS 2 SUBPARTITION BY KEY(c)
(
    PARTITION p1 VALUES IN (100) (
        SUBPARTITION p1sub1,
        SUBPARTITION p1sub2
    ),
    PARTITION p2 VALUES IN (200) (
        SUBPARTITION p2sub1,
        SUBPARTITION p2sub2
    )
);
DROP TABLE t_list_hash_subpartitions_clause;
CREATE TABLE t_list_hash_subpartitions_clause (a int, b int, c int)
PARTITION BY LIST COLUMNS (a) PARTITIONS 2 SUBPARTITION BY KEY(c) SUBPARTITIONS 2
(
    PARTITION p1 VALUES IN (100) (
        SUBPARTITION p1sub1,
        SUBPARTITION p1sub2
    ),
    PARTITION p2 VALUES IN (200) (
        SUBPARTITION p2sub1,
        SUBPARTITION p2sub2
    )
);
DROP TABLE t_list_hash_subpartitions_clause;
CREATE TABLE t_list_hash_subpartitions_clause (a int, b int, c int)
PARTITION BY LIST COLUMNS (a) SUBPARTITION BY KEY(c) SUBPARTITIONS 2
(
    PARTITION p1 VALUES IN (100) (
        SUBPARTITION p1sub1,
        SUBPARTITION p1sub2
    ),
    PARTITION p2 VALUES IN (200) (
        SUBPARTITION p2sub1,
        SUBPARTITION p2sub2
    )
);
DROP TABLE t_list_hash_subpartitions_clause;
CREATE TABLE t_list_hash_subpartitions_clause (a int, b int, c int)
PARTITION BY LIST COLUMNS (a) SUBPARTITION BY KEY(c) SUBPARTITIONS 2
(
    PARTITION p1 VALUES IN (100),
    PARTITION p2 VALUES IN (200)
);
DROP TABLE t_list_hash_subpartitions_clause;
CREATE TABLE t_list_hash_subpartitions_clause (a int, b int, c int)
PARTITION BY LIST COLUMNS (a) SUBPARTITION BY KEY(c) SUBPARTITIONS 2;

-- key-range with subpartitions clause
CREATE TABLE t_hash_range_subpartitions_clause (a int, b int, c int)
PARTITION BY KEY (a) PARTITIONS 2 SUBPARTITION BY RANGE(c)
(
    PARTITION p1 (
        SUBPARTITION p1sub1 VALUES LESS THAN (100),
        SUBPARTITION p1sub2 VALUES LESS THAN (MAXVALUE)
    ),
    PARTITION p2 (
        SUBPARTITION p2sub1 VALUES LESS THAN (100),
        SUBPARTITION p2sub2 VALUES LESS THAN (MAXVALUE)
    )
);
DROP TABLE t_hash_range_subpartitions_clause;
-- key-list with subpartitions clause
CREATE TABLE t_hash_list_subpartitions_clause (a int, b int, c int)
PARTITION BY KEY (a) PARTITIONS 2 SUBPARTITION BY LIST(c)
(
    PARTITION p1 (
        SUBPARTITION p1sub1 VALUES (0),
        SUBPARTITION p1sub2 VALUES (1)
    ),
    PARTITION p2 (
        SUBPARTITION p2sub1 VALUES (0),
        SUBPARTITION p2sub2 VALUES (1)
    )
);
DROP TABLE t_hash_list_subpartitions_clause;
-- key-hash with subpartitions clause
CREATE TABLE t_hash_hash_subpartitions_clause (a int, b int, c int)
PARTITION BY KEY (a) PARTITIONS 2 SUBPARTITION BY HASH(c)
(
    PARTITION p1 (
        SUBPARTITION p1sub1,
        SUBPARTITION p1sub2
    ),
    PARTITION p2 (
        SUBPARTITION p2sub1,
        SUBPARTITION p2sub2
    )
);
DROP TABLE t_hash_hash_subpartitions_clause;
CREATE TABLE t_hash_hash_subpartitions_clause (a int, b int, c int)
PARTITION BY KEY (a) PARTITIONS 2 SUBPARTITION BY HASH(c) SUBPARTITIONS 2
(
    PARTITION p1 (
        SUBPARTITION p1sub1,
        SUBPARTITION p1sub2
    ),
    PARTITION p2 (
        SUBPARTITION p2sub1,
        SUBPARTITION p2sub2
    )
);
DROP TABLE t_hash_hash_subpartitions_clause;
CREATE TABLE t_hash_hash_subpartitions_clause (a int, b int, c int)
PARTITION BY KEY (a) PARTITIONS 1 SUBPARTITION BY HASH(c) SUBPARTITIONS 2
(
    PARTITION p1 (
        SUBPARTITION p1sub1,
        SUBPARTITION p1sub2
    ),
    PARTITION p2 (
        SUBPARTITION p2sub1,
        SUBPARTITION p2sub2
    )
);
CREATE TABLE t_hash_hash_subpartitions_clause (a int, b int, c int)
PARTITION BY KEY (a) PARTITIONS 2 SUBPARTITION BY HASH(c) SUBPARTITIONS 3
(
    PARTITION p1 (
        SUBPARTITION p1sub1,
        SUBPARTITION p1sub2
    ),
    PARTITION p2 (
        SUBPARTITION p2sub1,
        SUBPARTITION p2sub2
    )
);
CREATE TABLE t_hash_hash_subpartitions_clause (a int, b int, c int)
PARTITION BY KEY (a) SUBPARTITION BY HASH(c) SUBPARTITIONS 2
(
    PARTITION p1 (
        SUBPARTITION p1sub1,
        SUBPARTITION p1sub2
    ),
    PARTITION p2 (
        SUBPARTITION p2sub1,
        SUBPARTITION p2sub2
    )
);
DROP TABLE t_hash_hash_subpartitions_clause;
CREATE TABLE t_hash_hash_subpartitions_clause (a int, b int, c int)
PARTITION BY KEY (a) SUBPARTITION BY HASH(c) SUBPARTITIONS 2
(
    PARTITION p1,
    PARTITION p2
);
DROP TABLE t_hash_hash_subpartitions_clause;

-- test the key of partition and subpartition is same column
CREATE TABLE t_single_key_range_subpart(id int, birthdate int)
    PARTITION BY RANGE (birthdate)
    SUBPARTITION BY HASH (birthdate)
    SUBPARTITIONS 2 (
    PARTITION p0 VALUES LESS THAN (1990),
    PARTITION p1 VALUES LESS THAN (2000),
    PARTITION p2 VALUES LESS THAN MAXVALUE
);
DROP TABLE IF EXISTS t_single_key_range_subpart;

-- --------------------------------------------------------test range columns syntax with B compatibility
CREATE TABLE t_single_key_range (a int, b int, c int)
PARTITION BY RANGE COLUMNS(a)
(
    PARTITION p1 VALUES LESS THAN (100) TABLESPACE = pg_default,
    PARTITION p2 VALUES LESS THAN (200),
    PARTITION p3 VALUES LESS THAN (300),
    PARTITION p4 VALUES LESS THAN MAXVALUE
);
SELECT pg_get_tabledef('t_single_key_range'::regclass);
DROP TABLE IF EXISTS t_single_key_range;

-- error
CREATE TABLE t_multi_keys_range (a int, b int, c int)
PARTITION BY RANGE COLUMNS(a,b)
(
    PARTITION p1 VALUES LESS THAN (100,100),
    PARTITION p2 VALUES LESS THAN (200,200),
    PARTITION p3 VALUES LESS THAN (300,300),
    PARTITION p4 VALUES LESS THAN MAXVALUE
);

CREATE TABLE t_multi_keys_range (a int, b int, c int)
PARTITION BY RANGE COLUMNS(a,b)
(
    PARTITION p1 VALUES LESS THAN (100,100),
    PARTITION p2 VALUES LESS THAN (200,200),
    PARTITION p3 VALUES LESS THAN (300,300),
    PARTITION p4 VALUES LESS THAN (400,MAXVALUE),
    PARTITION p5 VALUES LESS THAN (MAXVALUE,MAXVALUE)
);
\d+ t_multi_keys_range
SELECT pg_get_tabledef('t_multi_keys_range'::regclass);
DROP TABLE IF EXISTS t_multi_keys_range;

-- --------------------------------------------------------test number of columns
CREATE TABLE t_multi_keys_range_num (a int, b int, c int, d int, e int, f int, g int, h int, i int, j int, k int, l int, m int, n int, o int, p int, q int)
PARTITION BY RANGE COLUMNS(a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q)
(
    PARTITION p1 VALUES LESS THAN (100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100),
    PARTITION p2 VALUES LESS THAN (200,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100),
    PARTITION p3 VALUES LESS THAN (300,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100),
    PARTITION p4 VALUES LESS THAN (400,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100),
    PARTITION p5 VALUES LESS THAN (MAXVALUE,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100)
);
CREATE TABLE t_multi_keys_range_num (a int, b int, c int, d int, e int, f int, g int, h int, i int, j int, k int, l int, m int, n int, o int, p int, q int)
PARTITION BY RANGE COLUMNS(a,b,c,d)
(
    PARTITION p1 VALUES LESS THAN (100,100,100,100),
    PARTITION p2 VALUES LESS THAN (200,100,100,100),
    PARTITION p3 VALUES LESS THAN (300,100,100),
    PARTITION p4 VALUES LESS THAN (400,100,100,100),
    PARTITION p5 VALUES LESS THAN (MAXVALUE,100,100,100)
);
CREATE TABLE t_multi_keys_range_num (a int, b int, c int, d int, e int, f int, g int, h int, i int, j int, k int, l int, m int, n int, o int, p int, q int)
PARTITION BY RANGE COLUMNS(a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p)
(
    PARTITION p1 VALUES LESS THAN (100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100),
    PARTITION p2 VALUES LESS THAN (200,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100),
    PARTITION p3 VALUES LESS THAN (300,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100),
    PARTITION p4 VALUES LESS THAN (400,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100),
    PARTITION p5 VALUES LESS THAN (MAXVALUE,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100)
);
DROP TABLE IF EXISTS t_multi_keys_range_num;

-- --------------------------------------------------------test key partition with B compatibility
CREATE TABLE t_part_by_key (a int, b int, c int)
PARTITION BY KEY(a)
(
    PARTITION p1 TABLESPACE = pg_default,
    PARTITION p2,
    PARTITION p3,
    PARTITION p4,
    PARTITION p5
);
\d+ t_part_by_key
SELECT pg_get_tabledef('t_part_by_key'::regclass);
DROP TABLE IF EXISTS t_part_by_key;

CREATE TABLE t_part_by_key_num (a int, b int, c int, d int, e int, f int, g int, h int, i int, j int, k int, l int, m int, n int, o int, p int, q int)
PARTITION BY KEY(a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p)
(
    PARTITION p1,
    PARTITION p2,
    PARTITION p3,
    PARTITION p4,
    PARTITION p5
);

-- --------------------------------------------------------test list partition with B compatibility
-- errors
CREATE TABLE t_multi_keys_list_err (a int, b int, c int, d int, e int, f int, g int, h int, i int, j int, k int, l int, m int, n int, o int, p int, q int)
PARTITION BY LIST COLUMNS(a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p)
(
    PARTITION p1 VALUES IN (100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100),
    PARTITION p2 VALUES IN (200,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100),
    PARTITION p3 VALUES IN (300,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100),
    PARTITION p4 VALUES IN (400,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100)
);
CREATE TABLE t_multi_keys_list_err (a int, b int, c int, d int, e int, f int, g int, h int, i int, j int, k int, l int, m int, n int, o int, p int, q int)
PARTITION BY LIST COLUMNS(a,b,c,d)
(
    PARTITION p1 VALUES IN (100,100,100,100),
    PARTITION p2 VALUES IN (200,100,100,100),
    PARTITION p3 VALUES IN (300,100,100,100),
    PARTITION p4 VALUES IN (400,100,100,100)
);
CREATE TABLE t_multi_keys_list_err (a int, b int, c int, d int, e int, f int, g int, h int, i int, j int, k int, l int, m int, n int, o int, p int, q int)
PARTITION BY LIST COLUMNS(a,b,c,d)
(
    PARTITION p1 VALUES IN ((100,100,100,100)),
    PARTITION p2 VALUES IN ((200,100,100,100)),
    PARTITION p3 VALUES IN ((300,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100)),
    PARTITION p4 VALUES IN ((400,100,100,100))
);
CREATE TABLE t_multi_keys_list_err (a int, b int, c int, d int, e int, f int, g int, h int, i int, j int, k int, l int, m int, n int, o int, p int, q int)
PARTITION BY LIST COLUMNS(a,b,c,d)
(
    PARTITION p1 VALUES IN ((100,100,NULL,100),(100,100,NULL,100)),
    PARTITION p2 VALUES IN ((200,200,100,100),(100,100,100,200)),
    PARTITION p3 VALUES IN ((300,300,100,100),(100,100,100,300)),
    PARTITION p4 VALUES IN ((400,400,100,100),(100,100,100,400))
);
CREATE TABLE t_multi_keys_list_err (a int, b int, c int, d int, e int, f int, g int, h int, i int, j int, k int, l int, m int, n int, o int, p int, q int)
PARTITION BY LIST COLUMNS(a,b,c,d)
(
    PARTITION p1 VALUES IN ((100,100,NULL,100),(100,100,100,400)),
    PARTITION p2 VALUES IN ((200,200,100,100),(100,100,100,300)),
    PARTITION p3 VALUES IN ((300,300,100,100),(100,100,100,200)),
    PARTITION p4 VALUES IN ((400,400,100,100),(100,100,NULL,100))
);
CREATE TABLE t_multi_keys_list_err (a int, b int, c int)
PARTITION BY LIST COLUMNS(a,b)
(
    PARTITION p1 VALUES IN ( (0,MAXVALUE) ),
    PARTITION p2 VALUES IN ( (0,1), (0,2), (0,3), (1,1), (1,2) )
);
CREATE TABLE t_multi_keys_list_err (a int, b int, c int)
PARTITION BY LIST COLUMNS(a,b)
(
    PARTITION p1 VALUES IN ( (0,DEFAULT) ),
    PARTITION p2 VALUES IN ( (0,1), (0,2), (0,3), (1,1), (1,2) )
);
CREATE TABLE t_multi_keys_list_err (a int, b int, c int)
PARTITION BY LIST COLUMNS(a,b)
(
    PARTITION p1 VALUES IN (MAXVALUE),
    PARTITION p2 VALUES IN ( (0,1), (0,2), (0,3), (1,1), (1,2) )
);
CREATE TABLE t_multi_keys_list_err (a int, b int, c int)
PARTITION BY LIST COLUMNS(a,b)
(
    PARTITION p1 VALUES IN ( (NULL, NULL)),
    PARTITION p2 VALUES IN ( (0,1), (0,2), (0,A), (1,1), (1,2) )
);
CREATE TABLE t_multi_keys_list_err (a int, b int, c int, d int, e int, f int, g int, h int, i int, j int, k int, l int, m int, n int, o int, p int, q int)
PARTITION BY LIST COLUMNS(a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q)
(
    PARTITION p1 VALUES IN ((100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100)),
    PARTITION p2 VALUES IN ((200,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100)),
    PARTITION p3 VALUES IN ((300,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100)),
    PARTITION p4 VALUES IN ((400,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100))
);

-- normal
CREATE TABLE t_multi_keys_list (a int, b int, c int, d int, e int, f int, g int, h int, i int, j int, k int, l int, m int, n int, o int, p int, q int)
PARTITION BY LIST COLUMNS(a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p)
(
    PARTITION p1 VALUES IN ((100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100)),
    PARTITION p2 VALUES IN ((200,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100)),
    PARTITION p3 VALUES IN ((300,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100)),
    PARTITION p4 VALUES IN ((400,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100))
);
DROP TABLE IF EXISTS t_multi_keys_list;
CREATE TABLE t_multi_keys_list (a int, b int, c int)
PARTITION BY LIST COLUMNS(a,b)
(
    PARTITION p1 VALUES IN ( (0,0) ) TABLESPACE = pg_default,
    PARTITION p2 VALUES IN ( (0,1), (0,2), (0,3), (1,1), (1,2) ),
    PARTITION p3 VALUES IN ( (2,0), (2,1) ),
    PARTITION p4 VALUES IN ( (3,2), (3,3) )
);
SELECT pg_get_tabledef('t_multi_keys_list'::regclass);

INSERT INTO t_multi_keys_list VALUES(0,0,0);
SELECT * FROM t_multi_keys_list PARTITION(p1) ORDER BY 1,2;
INSERT INTO t_multi_keys_list VALUES(0,1,0);
INSERT INTO t_multi_keys_list VALUES(0,2,0);
INSERT INTO t_multi_keys_list VALUES(0,3,0);
INSERT INTO t_multi_keys_list VALUES(1,1,0);
INSERT INTO t_multi_keys_list VALUES(1,2,0);
SELECT * FROM t_multi_keys_list PARTITION(p2) ORDER BY 1,2;
INSERT INTO t_multi_keys_list VALUES(2,0,0);
INSERT INTO t_multi_keys_list VALUES(2,1,0);
SELECT * FROM t_multi_keys_list PARTITION(p3) ORDER BY 1,2;
INSERT INTO t_multi_keys_list VALUES(3,2,0);
INSERT INTO t_multi_keys_list VALUES(3,3,0);
SELECT * FROM t_multi_keys_list PARTITION(p4) ORDER BY 1,2;
INSERT INTO t_multi_keys_list VALUES(4,4,4);
DROP TABLE IF EXISTS t_multi_keys_list;

-- test with null keys
CREATE TABLE t_multi_keys_list_null (a int, b int, c int)
PARTITION BY LIST COLUMNS(a,b)
(
    PARTITION p1 VALUES IN ( (0,NULL) ),
    PARTITION p2 VALUES IN ( (0,1), (0,2), (0,3), (1,1), (1,2) ),
    PARTITION p3 VALUES IN ( (NULL,0), (2,1) ),
    PARTITION p4 VALUES IN ( (3,2), (NULL,NULL) )
);
CREATE INDEX t_multi_keys_list_null_idx_l ON t_multi_keys_list_null(a,b,c) LOCAL;
SELECT pg_get_tabledef('t_multi_keys_list_null'::regclass);

INSERT INTO t_multi_keys_list_null VALUES(0,NULL,0);
SELECT * FROM t_multi_keys_list_null PARTITION(p1) ORDER BY a,b;
INSERT INTO t_multi_keys_list_null VALUES(0,1,0);
INSERT INTO t_multi_keys_list_null VALUES(0,2,0);
INSERT INTO t_multi_keys_list_null VALUES(0,3,0);
INSERT INTO t_multi_keys_list_null VALUES(1,1,0);
INSERT INTO t_multi_keys_list_null VALUES(1,2,0);
SELECT * FROM t_multi_keys_list_null PARTITION(p2) ORDER BY a,b;
INSERT INTO t_multi_keys_list_null VALUES(NULL,0,0);
INSERT INTO t_multi_keys_list_null VALUES(2,1,0);
SELECT * FROM t_multi_keys_list_null PARTITION(p3) ORDER BY a,b;
INSERT INTO t_multi_keys_list_null VALUES(3,2,0);
INSERT INTO t_multi_keys_list_null VALUES(NULL,NULL,0);
SELECT * FROM t_multi_keys_list_null PARTITION(p4) ORDER BY a,b;
INSERT INTO t_multi_keys_list_null VALUES(4,4,4);

EXPLAIN (costs false)
SELECT a FROM t_multi_keys_list_null WHERE a IS NULL;
SELECT a FROM t_multi_keys_list_null WHERE a IS NULL;
EXPLAIN (costs false)
SELECT a FROM t_multi_keys_list_null WHERE a = 0;
SELECT a FROM t_multi_keys_list_null WHERE a = 0;
EXPLAIN (costs false)
SELECT b FROM t_multi_keys_list_null WHERE b IS NULL;
SELECT b FROM t_multi_keys_list_null WHERE b IS NULL;
EXPLAIN (costs false)
SELECT b FROM t_multi_keys_list_null WHERE b = 1;
SELECT b FROM t_multi_keys_list_null WHERE b = 1;
EXPLAIN (costs false)
SELECT a FROM t_multi_keys_list_null WHERE a = 4;
SELECT a FROM t_multi_keys_list_null WHERE a = 4;
EXPLAIN (costs false)
SELECT a,b FROM t_multi_keys_list_null WHERE a < 1 ORDER BY 1,2;
SELECT a,b FROM t_multi_keys_list_null WHERE a < 1 ORDER BY 1,2;
EXPLAIN (costs false)
SELECT * FROM t_multi_keys_list_null WHERE (a,b) = (0,1);
SELECT * FROM t_multi_keys_list_null WHERE (a,b) = (0,1);
EXPLAIN (costs false)
SELECT * FROM t_multi_keys_list_null WHERE (a,b) = (NULL,0);
SELECT * FROM t_multi_keys_list_null WHERE (a,b) = (NULL,0);
EXPLAIN (costs false)
SELECT * FROM t_multi_keys_list_null WHERE a IS NULL AND b = 0;
SELECT * FROM t_multi_keys_list_null WHERE a IS NULL AND b = 0;
EXPLAIN (costs false)
SELECT * FROM t_multi_keys_list_null WHERE a = 0 OR b = 0 ORDER BY 1,2;
SELECT * FROM t_multi_keys_list_null WHERE a = 0 OR b = 0 ORDER BY 1,2;
EXPLAIN (costs false)
SELECT * FROM t_multi_keys_list_null WHERE a IN (2,0) ORDER BY 1,2;
SELECT * FROM t_multi_keys_list_null WHERE a IN (2,0) ORDER BY 1,2;
EXPLAIN (costs false)
SELECT * FROM t_multi_keys_list_null WHERE (a,b) IN ((1,1), (3,2)) ORDER BY 1,2;
SELECT * FROM t_multi_keys_list_null WHERE (a,b) IN ((1,1), (3,2)) ORDER BY 1,2;
EXPLAIN (costs false)
SELECT * FROM t_multi_keys_list_null WHERE (a,b) =ANY(ARRAY[(2,1), (3,2)]) ORDER BY 1,2;
SELECT * FROM t_multi_keys_list_null WHERE (a,b) =ANY(ARRAY[(2,1), (3,2)]) ORDER BY 1,2;
EXPLAIN (costs false)
SELECT * FROM t_multi_keys_list_null WHERE a =ANY(ARRAY[2, 3]) ORDER BY 1,2;
SELECT * FROM t_multi_keys_list_null WHERE a =ANY(ARRAY[2, 3]) ORDER BY 1,2;
PREPARE part_bdb_stmt(int) as SELECT a,b FROM t_multi_keys_list_null WHERE b = $1 ORDER BY 1,2;
EXPLAIN (costs false)
EXECUTE part_bdb_stmt(3);
EXECUTE part_bdb_stmt(1);
EXECUTE part_bdb_stmt(2);
PREPARE part_bdb_stmt1(int) as SELECT a,b FROM t_multi_keys_list_null WHERE a != $1 ORDER BY 1,2;
EXPLAIN (costs false)
EXECUTE part_bdb_stmt1(3);
EXECUTE part_bdb_stmt1(1);
EXECUTE part_bdb_stmt1(2);
PREPARE part_bdb_stmt2(int) as SELECT a,b FROM t_multi_keys_list_null WHERE a >= $1 ORDER BY 1,2;
EXPLAIN (costs false)
EXECUTE part_bdb_stmt2(3);
EXECUTE part_bdb_stmt2(1);
EXECUTE part_bdb_stmt2(2);
PREPARE part_bdb_stmt3(int, int) as SELECT a,b FROM t_multi_keys_list_null WHERE (a,b) = ($1,$2);
EXPLAIN (costs false)
EXECUTE part_bdb_stmt3(0,1);
EXECUTE part_bdb_stmt3(0,1);
EXECUTE part_bdb_stmt3(3,2);

UPDATE t_multi_keys_list_null SET a=2, b=1 where a=1 and b=1;
SELECT * FROM t_multi_keys_list_null PARTITION(p3) ORDER BY a,b;
UPDATE t_multi_keys_list_null SET a=NULL, b=0 where a=1 and b=2;
SELECT * FROM t_multi_keys_list_null PARTITION(p3) ORDER BY a,b;

EXPLAIN (costs false)
DELETE t_multi_keys_list_null PARTITION(p3, p4) where b = 0;
DELETE t_multi_keys_list_null PARTITION(p3, p4) where b = 0;
SELECT * FROM t_multi_keys_list_null PARTITION(p3) ORDER BY a,b;
EXPLAIN (costs false)
DELETE t_multi_keys_list_null PARTITION FOR(0,NULL);
DELETE t_multi_keys_list_null PARTITION FOR(0,NULL);
SELECT * FROM t_multi_keys_list_null PARTITION(p1) ORDER BY a,b;
-- alter table partition
CREATE INDEX test_multi_list_key_gi on t_multi_keys_list_null(c);
CREATE TABLESPACE part_bdb_temp_tbspc RELATIVE LOCATION 'tablespace/part_bdb_temp_tbspc';
ALTER TABLE t_multi_keys_list_null MOVE PARTITION FOR(0,NULL) TABLESPACE part_bdb_temp_tbspc;
SELECT pg_get_tabledef('t_multi_keys_list_null'::regclass);

CREATE TABLE t_alter_partition_temp (a int, b int, c int);
INSERT INTO t_alter_partition_temp VALUES(NULL,0,1);
INSERT INTO t_alter_partition_temp VALUES(2,1,2);
CREATE INDEX t_alter_partition_temp_idx_l ON t_alter_partition_temp(a,b,c);
SELECT * FROM t_alter_partition_temp ORDER BY a,b,c;
SELECT * FROM t_multi_keys_list_null PARTITION(p3) ORDER BY a,b,c;
ALTER TABLE t_multi_keys_list_null EXCHANGE PARTITION (p3) WITH TABLE t_alter_partition_temp UPDATE GLOBAL INDEX;
SELECT * FROM t_alter_partition_temp ORDER BY a,b,c;
SELECT * FROM t_multi_keys_list_null PARTITION(p3) ORDER BY a,b,c;
DROP TABLE IF EXISTS t_alter_partition_temp;

ALTER TABLE t_multi_keys_list_null ADD PARTITION p5 VALUES (1);
ALTER TABLE t_multi_keys_list_null ADD PARTITION p5 VALUES ((2,1));
ALTER TABLE t_multi_keys_list_null ADD PARTITION p5 VALUES ((2,1,1));
ALTER TABLE t_multi_keys_list_null ADD PARTITION p5 VALUES ((4,NULL),(2,2),(4,NULL));
ALTER TABLE t_multi_keys_list_null ADD PARTITION p5 VALUES ((4,4));
INSERT INTO t_multi_keys_list_null VALUES(4,4,4);
SELECT * FROM t_multi_keys_list_null PARTITION(p5) ORDER BY a,b;

ALTER TABLE t_multi_keys_list_null DROP PARTITION FOR (1);
ALTER TABLE t_multi_keys_list_null DROP PARTITION FOR (1,5,8);
ALTER TABLE t_multi_keys_list_null DROP PARTITION FOR (1,5);
ALTER TABLE t_multi_keys_list_null DROP PARTITION FOR (2,1) UPDATE GLOBAL INDEX;
SELECT * FROM t_multi_keys_list_null PARTITION(p3) ORDER BY a,b;
ALTER TABLE t_multi_keys_list_null TRUNCATE PARTITION FOR (4,4) UPDATE GLOBAL INDEX;
SELECT * FROM t_multi_keys_list_null PARTITION(p5) ORDER BY a,b;
ALTER TABLE t_multi_keys_list_null RENAME PARTITION FOR (0,NULL) TO p0;
SELECT pg_get_tabledef('t_multi_keys_list_null'::regclass);

-- test views
SELECT table_name,partitioning_type,partition_count,partitioning_key_count,subpartitioning_type FROM MY_PART_TABLES ORDER BY 1;
SELECT table_name,partition_name,high_value,subpartition_count FROM MY_TAB_PARTITIONS ORDER BY 1,2;
SELECT table_name,partition_name,subpartition_name,high_value,high_value_length FROM MY_TAB_SUBPARTITIONS ORDER BY 1,2,3;
SELECT table_name,index_name,partition_count,partitioning_key_count,partitioning_type,subpartitioning_type FROM MY_PART_INDEXES ORDER BY 1,2;
SELECT index_name,partition_name,high_value,high_value_length FROM MY_IND_PARTITIONS ORDER BY 1,2;
SELECT index_name,partition_name,subpartition_name,high_value,high_value_length FROM MY_IND_SUBPARTITIONS ORDER BY 1,2,3;

DROP TABLE IF EXISTS t_multi_keys_list_null;
DROP TABLESPACE part_bdb_temp_tbspc;

-- test subpart
CREATE TABLE t_keys_range_list (a int, b int, c int)
PARTITION BY RANGE COLUMNS(a) SUBPARTITION BY LIST (b,c)
(
    PARTITION p1 VALUES LESS THAN (100) (
        SUBPARTITION sbp11 VALUES ((1,1)),
        SUBPARTITION sbp12 VALUES ((1,2)),
    ),
    PARTITION p2 VALUES LESS THAN (200) (
        SUBPARTITION sbp21 VALUES ((2,1)),
        SUBPARTITION sbp22 VALUES ((2,2)),
    ),
    PARTITION p5 VALUES LESS THAN (MAXVALUE) (
        SUBPARTITION sbp31 VALUES ((3,1)),
        SUBPARTITION sbp32 VALUES ((3,2)),
    )
);
\d+ t_keys_range_list
SELECT pg_get_tabledef('t_keys_range_list'::regclass);
DROP TABLE IF EXISTS t_keys_range_list;

-- MAXVALUE in subpartiton
CREATE TABLE range_011
(
co1 SMALLINT
,co2 INTEGER
,co3 BIGINT
)
PARTITION BY range COLUMNS(co1) PARTITIONS 3 SUBPARTITION BY range (co2)
(
PARTITION p_range_1 values less than (10)
(
SUBPARTITION p_range_1_1 values less than ( 20 ),
SUBPARTITION p_range_1_2 values less than ( 100 ),
SUBPARTITION p_range_1_5 values less than (MAXVALUE)
),
PARTITION p_range_2 values less than (20)
(
SUBPARTITION p_range_2_1 values less than ( 20 ),
SUBPARTITION p_range_2_2 values less than ( 100 ),
SUBPARTITION p_range_2_5 values less than (MAXVALUE)
),
PARTITION p_range_3 values less than MAXVALUE
(
SUBPARTITION p_range_3_1 values less than ( 20 ),
SUBPARTITION p_range_3_2 values less than ( 100 ),
SUBPARTITION p_range_3_5 values less than (MAXVALUE)
)) ENABLE ROW MOVEMENT;
DROP TABLE range_011;
CREATE TABLE range_011
(
co1 SMALLINT
,co2 INTEGER
,co3 BIGINT
)
PARTITION BY range COLUMNS(co1) PARTITIONS 3 SUBPARTITION BY range (co2)
(
PARTITION p_range_1 values less than (10)
(
SUBPARTITION p_range_1_1 values less than ( 20 ),
SUBPARTITION p_range_1_2 values less than ( 100 ),
SUBPARTITION p_range_1_5 values less than (MAXVALUE)
),
PARTITION p_range_2 values less than (20)
(
SUBPARTITION p_range_2_1 values less than ( 20 ),
SUBPARTITION p_range_2_2 values less than ( 100 ),
SUBPARTITION p_range_2_5 values less than (MAXVALUE)
),
PARTITION p_range_3 values less than MAXVALUE
(
SUBPARTITION p_range_3_1 values less than ( 20 ),
SUBPARTITION p_range_3_2 values less than ( 100 ),
SUBPARTITION p_range_3_5 values less than MAXVALUE
)) ENABLE ROW MOVEMENT; -- ERROR

-- END
DROP SCHEMA partition_b_db_schema CASCADE;
\c regression
drop database part_bdb;