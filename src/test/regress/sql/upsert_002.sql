DROP SCHEMA test_upsert_002 CASCADE;
CREATE SCHEMA test_upsert_002;
SET CURRENT_SCHEMA TO test_upsert_002;

-- test t1 with no index
CREATE TABLE t1 (
    col1 INT,
    col2 INT,
    col3 INT DEFAULT 1,
--    col4 TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    col5 BIGSERIAL
)  ;

INSERT INTO t1 VALUES (1, 2) ON DUPLICATE KEY UPDATE col1 = 3;

--- should always insert
INSERT INTO t1 VALUES (1, 2) ON DUPLICATE KEY UPDATE col2 = 3;
INSERT INTO t1 VALUES (1, 2) ON DUPLICATE KEY UPDATE t1.col2 = 4;

--- appoint column list in insert clause, should always insert
INSERT INTO t1(col1, col3) VALUES (1, 3) ON DUPLICATE KEY UPDATE col2 = 5;
INSERT INTO t1(col1, col3) VALUES (1, 3) ON DUPLICATE KEY UPDATE t1.col2 = 6;

--- multiple rows, should always insert
INSERT INTO t1 VALUES (2, 1), (2, 1) ON DUPLICATE KEY UPDATE col2 = 7, col3 = 7;
SELECT * FROM t1 ORDER BY col5;

--- test union, should insert
INSERT INTO t1 (col1, col2)
    SELECT * FROM
        (SELECT col1, col2 FROM t1
        UNION
        SELECT col1, col3 FROM t1) AS union_table
    ON DUPLICATE KEY UPDATE col3 = (col1 + col2 + col3) * 10;
SELECT col1, col2, col3 FROM t1 WHERE col5 > 6 ORDER BY col1, col2;

--- test subquery, should insert
INSERT INTO t1
    (SELECT col1 || col2 || '00' FROM t1 ORDER BY col5)
    ON DUPLICATE KEY UPDATE col3 = col1 * 100;
SELECT col1, col2, col3 FROM t1 WHERE col1 >= 100 ORDER BY col1;

-- test t2 with one primary key
CREATE TABLE t2 (
    col1 INT,
    col2 INT PRIMARY KEY,
    col3 INT DEFAULT 1,
    col4 TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    col5 BIGSERIAL
)  ;

--- primary key or unique key are not allowed to update
INSERT INTO t2 VALUES (1, 1) ON DUPLICATE KEY UPDATE col2 = 3;
INSERT INTO t2 VALUES (1, 1) ON DUPLICATE KEY UPDATE t2.col2 = 3;
INSERT INTO t2 (col2, col3, col4, col5)
    VALUES  (10, 10, CURRENT_TIMESTAMP(0), 10),
            (20, 20, CURRENT_TIMESTAMP(1), 20),
            (30, 30, CURRENT_TIMESTAMP(2), 30)
    ON DUPLICATE KEY UPDATE
        col1 = 100,
        col2 = 100,
        col3 = 100,
        col4 = '2019-08-09'::TIMESTAMP,
        col5 = 100;

--- should insert
INSERT INTO t2 VALUES (1, 1) ON DUPLICATE KEY UPDATE col1 = 30;
INSERT INTO t2 VALUES (2, 2) ON DUPLICATE KEY UPDATE t2.col1 = 40;
INSERT INTO t2 VALUES (3, 3) ON DUPLICATE KEY UPDATE col1 = col1 * 2;
INSERT INTO t2 VALUES (4, 4) ON DUPLICATE KEY UPDATE t2.col1 = t2.col1 * 2 ;
INSERT INTO t2 VALUES (5, 5) ON DUPLICATE KEY UPDATE col1 = col2 + 1;
INSERT INTO t2 VALUES (6, 6) ON DUPLICATE KEY UPDATE t2.col1 = t2.col2 + 1;
INSERT INTO t2 VALUES (7, 7) ON DUPLICATE KEY UPDATE col1 = extract(dow from col4) + 10;
INSERT INTO t2 VALUES (8, 8) ON DUPLICATE KEY UPDATE t2.col1 = extract(century from col4) * 100 + extract(isodow from col4);
SELECT col1, col2, col3, col5 FROM t2 ORDER BY col5;

--- should update
INSERT INTO t2 VALUES (3, 1) ON DUPLICATE KEY UPDATE col1 = 30, col3 = col5 + 1;
INSERT INTO t2 VALUES (4, 2) ON DUPLICATE KEY UPDATE t2.col1 = 40, t2.col3 = t2.col5;
INSERT INTO t2 VALUES (3, 3), (4, 4) ON DUPLICATE KEY UPDATE col3 = t2.col5 + 1;
INSERT INTO t2 VALUES (5, 5) ON DUPLICATE KEY UPDATE col1 = extract(dow from col4) + 10;
INSERT INTO t2 VALUES (6, 6) ON DUPLICATE KEY UPDATE t2.col1 = extract(century from col4) * 100 + extract(isodow from col4);
SELECT col1, col2, col3, col5 FROM t2 ORDER BY col5;

-- primary key are not allowed to be null
INSERT INTO t2 (col1) VALUES (10) ON DUPLICATE KEY UPDATE col1 = 10;

--- appoint column list in insert clause
---- should insert
INSERT INTO t2 (col2, col3) VALUES (9, 9) ON DUPLICATE KEY UPDATE col1 = 90;
INSERT INTO t2 (col2, col3, col4, col5)
    VALUES  (10, 10, CURRENT_TIMESTAMP(0), 10),
            (20, 20, CURRENT_TIMESTAMP(1), 20),
            (30, 30, CURRENT_TIMESTAMP(2), 30)
    ON DUPLICATE KEY UPDATE
        col1 = 100,
        col3 = 100,
        col4 = '2019-08-20'::TIMESTAMP,
        col5 = 100;
SELECT * FROM t2 ORDER BY col5;

---- should update
INSERT INTO t2 (col2, col3) VALUES (9, 9) ON DUPLICATE KEY UPDATE col1 = 90;
INSERT INTO t2 (col2, col3, col4, col5)
    VALUES  (10, 10, CURRENT_TIMESTAMP(0), 10),
            (20, 20, CURRENT_TIMESTAMP(1), 20),
            (30, 30, CURRENT_TIMESTAMP(2), 30)
    ON DUPLICATE KEY UPDATE
        col1 = 100,
        col3 = 100,
        col4 = '2019-08-20'::TIMESTAMP,
        col5 = 100;
SELECT * FROM t2 ORDER BY col5, col2;

--- test subquery
---- should insert
INSERT INTO t2
    (SELECT col1 * 1000, col2 * 1000 + 1 FROM t2 ORDER BY col5 LIMIT 2)
    ON DUPLICATE KEY UPDATE col3 = col2 + 1;
INSERT INTO t2 (col2, col3)
    (SELECT col1 * 1000 + 2, col2 * 1000 FROM t2 ORDER BY col5 LIMIT 2 OFFSET 1)
    ON DUPLICATE KEY UPDATE col3 = col2 + 1;
SELECT col1, col2, col3, col5 FROM t2 ORDER BY col5, col2;

---- should update
INSERT INTO t2
    (SELECT col1 * 1000, col2 * 1000 + 1 FROM t2 ORDER BY col5 LIMIT 2)
    ON DUPLICATE KEY UPDATE col3 = col2 + 1;
INSERT INTO t2 (col2, col3)
    (SELECT col1 * 1000 + 2, col2 * 1000 FROM t2 ORDER BY col5 LIMIT 2 OFFSET 1)
    ON DUPLICATE KEY UPDATE col3 = col2 + 1;
SELECT col1, col2, col3, col5 FROM t2 ORDER BY col5, col2;

--- test union, some insert some update
SELECT col1, col2 FROM t1 WHERE col2 IS NOT NULL
    UNION
    SELECT col1, col3 FROM t1 ORDER BY 1, 2;
INSERT INTO t2 (col1, col2)
    SELECT * FROM
        (SELECT col1, col2 FROM t1 WHERE col2 IS NOT NULL
        UNION
        SELECT col1, col3 FROM t1) AS union_table
    ON DUPLICATE KEY UPDATE col3 = (col1 + col2 + col3);

SELECT col1, col2, col3, col5 FROM t2 ORDER BY col5, col2;

INSERT INTO t2 (col1, col2)
    SELECT col1, col2 FROM t1 WHERE col2 IS NOT NULL
    UNION
    SELECT col1, col3 FROM t1
    ON DUPLICATE KEY UPDATE col3 = col3 + 1;
SELECT col1, col2, col3, col5 FROM t2 ORDER BY col5, col2;
reset behavior_compat_options;

-- test INTERSECT, should update
(SELECT col1, col1 + col2 FROM t1 WHERE col2 IS NOT NULL
    UNION
    SELECT 1, 2)
    INTERSECT
    SELECT col1, col3 FROM t1;

INSERT INTO t2 (col1, col2)
    (SELECT col1, col1 + col2 FROM t1 WHERE col2 IS NOT NULL
        UNION
        SELECT 1, 2)
    INTERSECT
    SELECT col1, col3 FROM t1
    ON DUPLICATE KEY UPDATE col3 = col3 + 1;
SELECT col1, col2, col3, col5 FROM t2 WHERE col2 = 3 ORDER BY col5, col2;

-- test EXCEPT, should update
SELECT col1, col2 FROM t1 WHERE col2 IS NOT NULL
    EXCEPT
    (SELECT col1, col3 FROM t1
        UNION
        SELECT NULL, NULL);

INSERT INTO t2 (col1, col2)
    SELECT col1, col2 FROM t1 WHERE col2 IS NOT NULL
    EXCEPT
    (SELECT col1, col3 FROM t1
        UNION
        SELECT NULL, NULL)
    ON DUPLICATE KEY UPDATE col3 = col3 + 1;
SELECT col1, col2, col3, col5 FROM t2 WHERE col2 = 2 ORDER BY col5, col2;

-- test unique index with not default value
ALTER TABLE t2 DROP CONSTRAINT t2_pkey;
CREATE UNIQUE INDEX t2_u_index ON t2(col2, col5);
SELECT col1, col2 FROM t1 WHERE col2 IS NOT NULL
    EXCEPT
    (SELECT col1, col3 FROM t1
        UNION
        SELECT NULL, NULL);
INSERT INTO t2
    SELECT col1, col2 FROM t1 WHERE col2 IS NOT NULL
    EXCEPT
    (SELECT col1, col3 FROM t1
        UNION
        SELECT NULL, NULL)
    ON DUPLICATE KEY UPDATE col3 = col3 + 1;
SELECT col1, col2, col3, col5 FROM t2 WHERE col2 = 2 ORDER BY col5, col2;

-- test t3 with one primary index with two columns
CREATE TABLE t3 (
    col1 INT,
    col2 INT,
    col3 INT DEFAULT 1,
--    col4 TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    col5 BIGSERIAL,
    PRIMARY KEY (col2, col3)
) ;

--- column referenced by primary key are not allowed to update
INSERT INTO t3 VALUES (1, 1) ON DUPLICATE KEY UPDATE col3 = 3;

--- should insert when not contains primary key and not all primary key referred columns have default value.
--- but will fail cause primary key should not be null
INSERT INTO t3 (col1) VALUES (1) ON DUPLICATE KEY UPDATE col1 = 2;

--- should insert
--- (SEQUENCE BUG: the serial column will starts from 2 since the above statement has applied for a sequence)
INSERT INTO t3 VALUES (1, 1) ON DUPLICATE KEY UPDATE col1 = 10;
INSERT INTO t3 VALUES (2, 2, 2) ON DUPLICATE KEY UPDATE col5 = 20;
SELECT * FROM t3 order by col5;

--- should update
INSERT INTO t3 VALUES (1, 1) ON DUPLICATE KEY UPDATE col1 = 10;
INSERT INTO t3 VALUES (2, 2, 2) ON DUPLICATE KEY UPDATE col5 = 20;
SELECT * FROM t3 order by col5;

--- test subquery
---- should insert
INSERT INTO t3 (col2, col3) (SELECT max(col2) + 1, max(col3) + 1 FROM t3) ON DUPLICATE KEY UPDATE col1 = col2 + col3;
SELECT * FROM t3 order by col5;

---- should update
INSERT INTO t3 (col2, col3) (SELECT max(col2), max(col3) FROM t3) ON DUPLICATE KEY UPDATE col1 = col2 + col3;
SELECT * FROM t3 order by col5;

-- test t3 with one unique index with two columns
TRUNCATE t3;
ALTER TABLE t3 DROP CONSTRAINT t3_pkey;
ALTER TABLE t3 ALTER COLUMN col2 DROP NOT NULL;
CREATE UNIQUE INDEX t3_ukey ON t3 (col2, col3);

--- column referenced by unique key are not allowed to update
INSERT INTO t3 VALUES (1, 1) ON DUPLICATE KEY UPDATE col3 = 3;

--- should insert cause not contains unique key and not all unique key referred columns have default value.
INSERT INTO t3 (col1) VALUES (1) ON DUPLICATE KEY UPDATE col1 = 2;
INSERT INTO t3 (col1) VALUES (1) ON DUPLICATE KEY UPDATE col1 = 2;
SELECT * FROM t3 order by col5;

--- should insert
INSERT INTO t3 VALUES (1, 1) ON DUPLICATE KEY UPDATE col1 = 10;
INSERT INTO t3 VALUES (2, 2, 2) ON DUPLICATE KEY UPDATE t3.col5 = 20;
SELECT * FROM t3 ORDER BY col5;

--- should update
INSERT INTO t3 VALUES (1, 1) ON DUPLICATE KEY UPDATE t3.col1 = 10;
INSERT INTO t3 VALUES (2, 2, 2) ON DUPLICATE KEY UPDATE col5 = 20;
SELECT * FROM t3 ORDER BY col5;

--- test subquery
---- should insert
INSERT INTO t3 (SELECT 100, NULL, max(col3) FROM t3) ON DUPLICATE KEY UPDATE col1 = col2 + col3;
INSERT INTO t3 (SELECT 100, NULL, max(col3) FROM t3) ON DUPLICATE KEY UPDATE col1 = col2 + col3;
INSERT INTO t3 (col2, col3) (SELECT max(col2) + 1, max(col3) + 1 FROM t3) ON DUPLICATE KEY UPDATE col1 = col2 + col3;
SELECT * FROM t3 ORDER BY col5;

---- should update
INSERT INTO t3 (col2, col3) (SELECT max(col2), max(col3) FROM t3) ON DUPLICATE KEY UPDATE col1 = col2 + col3;
SELECT * FROM t3 ORDER BY col5;

DROP SCHEMA test_upsert_002 CASCADE;
