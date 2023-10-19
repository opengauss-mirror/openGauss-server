DROP SCHEMA test_insert_update_007 CASCADE;
CREATE SCHEMA test_insert_update_007;
SET CURRENT_SCHEMA TO test_insert_update_007;

-- test t4 with one primary key with three columns
CREATE TABLE t4 (
    col1 INT,
    col2 INT DEFAULT 0,
    col3 VARCHAR DEFAULT 1,
    col4 TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    col5 BIGSERIAL,
    PRIMARY KEY (col2, col3, col5)
)DISTRIBUTE BY hash(col2)
PARTITION BY RANGE (col2)
(
    PARTITION P1 VALUES LESS THAN (2),
    PARTITION P2 VALUES LESS THAN (100),
    PARTITION P3 VALUES LESS THAN (MAXVALUE)
)ENABLE ROW MOVEMENT;

--- should insert
INSERT INTO t4 VALUES (1) ON DUPLICATE KEY UPDATE col1 = 10;
INSERT INTO t4 VALUES (1) ON DUPLICATE KEY UPDATE col1 = 10;
INSERT INTO t4 VALUES (2, 2, 2) ON DUPLICATE KEY UPDATE col1 = 200;
INSERT INTO t4 VALUES (100, 100, 100, CURRENT_TIMESTAMP, 100) ON DUPLICATE KEY UPDATE col1 = 1000;
SELECT col1, col2, col3, col5 FROM t4 ORDER BY col5;

--- should update
INSERT INTO t4 VALUES (2, 2, 2, CURRENT_TIMESTAMP, 3) ON DUPLICATE KEY UPDATE col1 = 200;
INSERT INTO t4 VALUES (100, 100, 100, CURRENT_TIMESTAMP, 100) ON DUPLICATE KEY UPDATE col1 = 1000;
SELECT col1, col2, col3, col5 FROM t4 ORDER BY col5;

--- error: duplicate key update on (x, x, 20)
--- this is because current version is not inplace update but merge,
--- so when the subquery contains multiple same values, it will cause duplicate insert failure.
SELECT col3, sum(col3) * 10 FROM t4 GROUP BY col3 ORDER BY 1, 2;
INSERT INTO t4 (col1, col5)
    (SELECT col3, sum(col3) * 10 FROM t4 GROUP BY col3)
    ON DUPLICATE KEY UPDATE col1 = 3;

-- test t5 with sequence column in constaint index
CREATE TABLE t5 (
    col1 INT,
    col2 TEXT DEFAULT '1',
    col3 BIGSERIAL,
--    col4 TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    col5 INTEGER(10, 5) DEFAULT RANDOM(),
    CONSTRAINT u_t5_index1 UNIQUE (col1, col3)
)DISTRIBUTE BY hash(col1)
PARTITION BY RANGE (col1)
(
    PARTITION P1 VALUES LESS THAN (2),
    PARTITION P2 VALUES LESS THAN (4),
    PARTITION P3 VALUES LESS THAN (6),
    PARTITION P4 VALUES LESS THAN (MAXVALUE)
)DISABLE ROW MOVEMENT;

--- should insert
INSERT INTO t5 VALUES (1), (1), (1) ON DUPLICATE KEY UPDATE col2 = col3;
INSERT INTO t5 DEFAULT VALUES ON DUPLICATE KEY UPDATE col2 = col3;
SELECT * FROM t5 ORDER BY col3;

--- should update
INSERT INTO t5 (col1, col3) VALUES (1, 1), (1, 2), (1, 3) ON DUPLICATE KEY UPDATE col5 = col2, col2 = col3 * 10;
SELECT * FROM t5 WHERE col1 = 1 ORDER BY col3;

--- should some insert some update
INSERT INTO t5 (col1, col3) VALUES (2, 5), (2, 6);
SELECT col1, col2, col3 FROM t5 ORDER BY col3;
INSERT INTO t5 (col1) VALUES (2), (2), (2) ON DUPLICATE KEY UPDATE col2 = col3;
SELECT col1, col2, col3 FROM t5 ORDER BY col3;

--- should INSERT and sequence starting from 7
INSERT INTO t5 VALUES (2), (2);
SELECT col1, col2, col3 FROM t5 ORDER BY col3;

--- test subquery, some insert and some update
INSERT INTO t5 (col1, col3)
    SELECT col1, col3 + 2 FROM t5 PARTITION (P1)
ON DUPLICATE KEY UPDATE col2 = col1 * 100;
SELECT col1, col2, col3 FROM t5 ORDER BY col3;

-- test with volatile function as default column in constraint index
TRUNCATE t5;
ALTER TABLE t5 DROP CONSTRAINT u_t5_index1;
ALTER TABLE t5 ADD CONSTRAINT u_t5_index2 UNIQUE (col1, col5);

--- should insert
INSERT INTO t5 VALUES (3), (3), (3) ON DUPLICATE KEY UPDATE col2 = col3;
INSERT INTO t5 (col1) VALUES (4), (4), (4) ON DUPLICATE KEY UPDATE col2 = col3;
SELECT * FROM t5 ORDER BY col3;

--- should update
INSERT INTO t5 (col1, col5) SELECT col1, col5 FROM t5 where col1 = 3 ON DUPLICATE KEY UPDATE col2 = col5 * 100;
SELECT * FROM t5 ORDER BY col3;

--- test subquery
---- should insert
INSERT INTO t5 (col1, col2) SELECT col1, col2 FROM t5 PARTITION (P3) ON DUPLICATE KEY UPDATE col2 = col5 * 100;
SELECT * FROM t5 ORDER BY col3;

---- should update
INSERT INTO t5 SELECT * FROM t5 PARTITION (P3) ON DUPLICATE KEY UPDATE col2 = col5 * 1000;
SELECT * FROM t5 ORDER BY col3;

-- test t6 with one more index
CREATE TABLE t6 (
    col1 INT,
    col2 INT DEFAULT 1,
    col3 BIGSERIAL,
--    col4 TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    col5 INTEGER(10, 5) DEFAULT RANDOM(),
    col6 TEXT,
    col7 TEXT
)DISTRIBUTE BY hash(col1)
PARTITION BY RANGE (col1)
(
    PARTITION P1 VALUES LESS THAN (2),
    PARTITION P2 VALUES LESS THAN (4),
    PARTITION P3 VALUES LESS THAN (6),
    PARTITION P4 VALUES LESS THAN (MAXVALUE)
)ENABLE ROW MOVEMENT;

ALTER TABLE t6 ADD PRIMARY KEY (col1, col3);
ALTER TABLE t6 ADD CONSTRAINT u_t6_index1 UNIQUE (col1, col5, col6);

INSERT INTO t6 (col1) VALUES (1), (2), (3), (4), (5);
SELECT col1, col2, col3, col6, col7 FROM t6 ORDER BY col3;

--- should insert
INSERT INTO t6 (col1) VALUES (1), (2), (3), (4), (5) ON DUPLICATE KEY UPDATE col6 = power(col1, col2);
SELECT col1, col2, col3, col6, col7 FROM t6 ORDER BY col3;

--- should update because primary key matches
INSERT INTO t6 (col1, col3) VALUES (6, 11), (6, 12);
SELECT col1, col2, col3, col6, col7 FROM t6 WHERE col1 = 6 ORDER BY col3;
INSERT INTO t6 (col1) VALUES (6), (6), (6) ON DUPLICATE KEY UPDATE col2 = col1 + col3, col6 =  col2 * 10;
SELECT col1, col2, col3, col6, col7 FROM t6 WHERE col1 = 6 ORDER BY col3;

--- should update for those col6 is not null bacause they will match the unique index,
--- and insert for those col6 is null because the unique index containing null never matches,
--- also primary key will not match
--- be ware the sequence column of the inserted row will jump n step, where n is the count of the not null rows,
--- because those sequence have to be generated during the unique index join stage.
INSERT INTO t6 (col1, col5, col6) 
    (SELECT col1, col5, col6 FROM t6 PARTITION (P4) WHERE col1 = 6)
    ON DUPLICATE KEY UPDATE
        col7 = col2 + 1;
SELECT col1, col2, col3, col6, col7 FROM t6 WHERE col1 = 6 ORDER BY col1, col3, col6, col7;

--- should update because unique index and primary key both match
INSERT INTO t6 (col1, col3, col5, col6)
    (SELECT col1, col3, col5, col6 FROM t6 PARTITION (P4) WHERE col1 = 6 AND col6 IS NOT NULL)
    ON DUPLICATE KEY UPDATE
        col7 = col7 * 10;
SELECT col1, col2, col3, col6, col7 FROM t6 WHERE col1 = 6 ORDER BY col1, col3, col6, col7;

--- insert when col3 = 17 because constaints does not match,
--- but update when col3 = 18 because 18 has been inserted and will cause a match
INSERT INTO t6 (col1, col3, col5, col6) VALUES (7, 18, 100, 100);
SELECT * FROM t6 WHERE col3 > 16 ORDER BY col1, col3, col6, col7;
INSERT INTO t6 (col1, col5, col6) VALUES (7, 10, 10), (7, 100, 100) ON DUPLICATE KEY UPDATE
    col7 = col3 * 100;
SELECT * FROM t6 WHERE col1 = 7 ORDER BY col1, col3, col6, col7;

DROP SCHEMA test_insert_update_007 CASCADE;