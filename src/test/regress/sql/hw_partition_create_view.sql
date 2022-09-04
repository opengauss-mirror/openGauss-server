DROP SCHEMA hw_partition_create_view CASCADE;
CREATE SCHEMA hw_partition_create_view;
SET CURRENT_SCHEMA TO hw_partition_create_view;

--
-- 1.create view as select partition on partitioned table
--
CREATE TABLE range_test1(c1 int,c2 int)
PARTITION BY RANGE (c1)
(
    PARTITION p1 VALUES LESS THAN (251),
    PARTITION p2 VALUES LESS THAN (501),
    PARTITION p3 VALUES LESS THAN (751),
    PARTITION p4 VALUES LESS THAN (MAXVALUE)
);
CREATE INDEX range_test1_idx ON range_test1(c1) GLOBAL;
INSERT INTO range_test1 SELECT generate_series(1,1000), generate_series(1,1000);

CREATE VIEW range_test1_v1 AS SELECT * FROM range_test1 PARTITION (p1);
CREATE VIEW range_test1_v2 AS SELECT * FROM range_test1 PARTITION (p2);
CREATE VIEW range_test1_v3 AS SELECT * FROM range_test1 PARTITION FOR (600);

--test for view, read OK
SELECT COUNT(*) FROM range_test1_v1;
SELECT COUNT(*) FROM range_test1_v2;
SELECT COUNT(*) FROM range_test1_v3;

-- do DDL on destination partition
ALTER TABLE range_test1 DROP PARTITION p1;
-- the partition is missing, error
SELECT COUNT(*) FROM range_test1_v1;

ALTER TABLE range_test1 TRUNCATE PARTITION p2;
-- the partition is reserved, success
SELECT COUNT(*) FROM range_test1_v2;

-- do DDL on destination partition
REINDEX INDEX range_test1_idx;
ALTER TABLE range_test1 TRUNCATE PARTITION p3 UPDATE GLOBAL INDEX;
-- the partition is rebuilt, error
SELECT COUNT(*) FROM range_test1_v3;

--
-- 2.create view as select partition/subpartition on subpartitioned table
--
CREATE TABLE range_range_test1(c1 int,c2 int)
PARTITION BY RANGE (c1) SUBPARTITION BY RANGE (c2) 
(
    PARTITION p1 VALUES LESS THAN (251)
    (
        SUBPARTITION p1_1 VALUES LESS THAN (501),
        SUBPARTITION p1_2 VALUES LESS THAN (MAXVALUE)
    ),
    PARTITION p2 VALUES LESS THAN (501)
    (
        SUBPARTITION p2_1 VALUES LESS THAN (501),
        SUBPARTITION p2_2 VALUES LESS THAN (MAXVALUE)
    ),
    PARTITION p3 VALUES LESS THAN (751)
    (
        SUBPARTITION p3_1 VALUES LESS THAN (501),
        SUBPARTITION p3_2 VALUES LESS THAN (MAXVALUE)
    ),
    PARTITION p4 VALUES LESS THAN (MAXVALUE)
    (
        SUBPARTITION p4_1 VALUES LESS THAN (501),
        SUBPARTITION p4_2 VALUES LESS THAN (MAXVALUE)
    )
);
CREATE INDEX range_range_test1_idx ON range_range_test1(c1) GLOBAL;
INSERT INTO range_range_test1 SELECT generate_series(1,1000), generate_series(1,1000);

CREATE VIEW range_range_test1_v1 AS SELECT * FROM range_range_test1 PARTITION (p1);
CREATE VIEW range_range_test1_v2 AS SELECT * FROM range_range_test1 SUBPARTITION (p1_1);
CREATE VIEW range_range_test1_v3 AS SELECT * FROM range_range_test1 PARTITION FOR (400);
CREATE VIEW range_range_test1_v4 AS SELECT * FROM range_range_test1 SUBPARTITION FOR (400, 400);

--test for view, read OK
SELECT COUNT(*) FROM range_range_test1_v1;
SELECT COUNT(*) FROM range_range_test1_v2;
SELECT COUNT(*) FROM range_range_test1_v3;
SELECT COUNT(*) FROM range_range_test1_v4;

-- do DDL on destination partition
ALTER TABLE range_range_test1 DROP SUBPARTITION p1_1;
-- the subpartition p1_1 is missing
SELECT COUNT(*) FROM range_range_test1_v2;

ALTER TABLE range_range_test1 DROP PARTITION p1;
-- the partition p1 is missing
SELECT COUNT(*) FROM range_range_test1_v1;
SELECT COUNT(*) FROM range_range_test1_v2;

REINDEX INDEX range_range_test1_idx;
ALTER TABLE range_range_test1 TRUNCATE SUBPARTITION p2_1 UPDATE GLOBAL INDEX;
-- the subpartition p2_1 is rebuilt
SELECT COUNT(*) FROM range_range_test1_v4;

-- rebuild range_range_test1_v4
DROP VIEW range_range_test1_v4;
CREATE VIEW range_range_test1_v4 AS SELECT * FROM range_range_test1 SUBPARTITION FOR (400, 400);
ALTER TABLE range_range_test1 TRUNCATE PARTITION p2 UPDATE GLOBAL INDEX;
-- the partition p2 is reserved, but the subpartition p2_1 is rebuilt
SELECT COUNT(*) FROM range_range_test1_v3;
SELECT COUNT(*) FROM range_range_test1_v4;

--finish
DROP TABLE range_test1 CASCADE;
DROP TABLE range_range_test1 CASCADE;

DROP SCHEMA hw_partition_create_view CASCADE;
RESET CURRENT_SCHEMA;