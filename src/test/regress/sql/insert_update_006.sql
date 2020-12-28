DROP SCHEMA test_insert_update_006 CASCADE;
CREATE SCHEMA test_insert_update_006;
SET CURRENT_SCHEMA TO test_insert_update_006;

-- test t1 with no index
CREATE TEMPORARY TABLE t1 (
    col1 TEXT,
    col2 VARCHAR,
    col3 VARCHAR(10) DEFAULT '1'
--    col4 TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
--    col5 BIGSERIAL
) DISTRIBUTE BY hash(col1);

--- distribute key are not allowed to update
INSERT INTO t1 VALUES (1, 2) ON DUPLICATE KEY UPDATE col1 = 3;

--- should always insert
INSERT INTO t1 VALUES (1, 2) ON DUPLICATE KEY UPDATE col2 = 3;
INSERT INTO t1 VALUES (1, 2) ON DUPLICATE KEY UPDATE t1.col2 = 4;

--- appoint column list in insert clause, should always insert
INSERT INTO t1(col1, col3) VALUES (1, 3) ON DUPLICATE KEY UPDATE col2 = 5;
INSERT INTO t1(col1, col3) VALUES (1, 3) ON DUPLICATE KEY UPDATE t1.col2 = 6;

--- multiple rows, should always insert
INSERT INTO t1 VALUES (2, 1), (2, 1) ON DUPLICATE KEY UPDATE col2 = 7, col3 = 7;
SELECT * FROM t1 ORDER BY col1, col2, col3;

--- test union, should insert
INSERT INTO t1 (col1, col2)
    SELECT * FROM
        (SELECT col1, col2 FROM t1
        UNION
        SELECT col1, col3 FROM t1) AS union_table
    ON DUPLICATE KEY UPDATE col3 = (col1::INT + col2::INT + col3) * 10;
SELECT col1, col2, col3 FROM t1 ORDER BY col1, col2, col3;

--- test subquery, should insert
INSERT INTO t1
    (SELECT col1 || col2 || '00' FROM t1 ORDER BY col1, col2)
    ON DUPLICATE KEY UPDATE col3 = col1 * 100;
SELECT col1, col2, col3 FROM t1 WHERE col1 >= 100 ORDER BY col1, col2, col3;

-- test t2 with one primary key
CREATE TEMPORARY TABLE t2 (
    col1 INT,
    col2 TEXT PRIMARY KEY,
    col3 VARCHAR DEFAULT '1',
    col4 TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    col5 INTEGER(10, 5) DEFAULT RANDOM()
) DISTRIBUTE BY hash(col2);

--- distribute key are not allowed to update
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
SELECT col1, col2, col3, col5 FROM t2 ORDER BY col1, col2, col3;

--- should update
INSERT INTO t2 VALUES (3, 1) ON DUPLICATE KEY UPDATE col1 = 30, col3 = col5 + 1;
INSERT INTO t2 VALUES (4, 2) ON DUPLICATE KEY UPDATE t2.col1 = 40, t2.col3 = t2.col5 + 1;
INSERT INTO t2 VALUES (3, 3), (4, 4) ON DUPLICATE KEY UPDATE col3 = t2.col5 + 1;
INSERT INTO t2 VALUES (5, 5) ON DUPLICATE KEY UPDATE col1 = extract(dow from col4) + 10;
INSERT INTO t2 VALUES (6, 6) ON DUPLICATE KEY UPDATE t2.col1 = extract(century from col4) * 100 + extract(isodow from col4);
SELECT col1, col2, col3, col5 FROM t2 ORDER BY col1, col2, col3;

-- primary key are not allowed to be null
INSERT INTO t2 (col1) VALUES (10) ON DUPLICATE KEY UPDATE col1 = 10;

--- appoint column list in insert clause
---- should insert
INSERT INTO t2 (col2, col3) VALUES (9, 9) ON DUPLICATE KEY UPDATE col1 = 90, col5 = 90;
INSERT INTO t2 (col2, col3, col4, col5)
    VALUES  (10, 10, CURRENT_TIMESTAMP(0), 10),
            (20, 20, CURRENT_TIMESTAMP(1), 20),
            (30, 30, CURRENT_TIMESTAMP(2), 30)
    ON DUPLICATE KEY UPDATE
        col1 = 100,
        col3 = 100,
        col4 = '2019-08-20'::TIMESTAMP,
        col5 = 100;
SELECT col1, col2, col3, col5 FROM t2 WHERE col2 = 9 OR col5 > 1 ORDER BY col1, col2, col3, col5;

---- should update
INSERT INTO t2 (col2, col3) VALUES (9, 9) ON DUPLICATE KEY UPDATE col1 = 90, col5 = 90;
INSERT INTO t2 (col2, col3, col4, col5)
    VALUES  (10, 10, CURRENT_TIMESTAMP(0), 10),
            (20, 20, CURRENT_TIMESTAMP(1), 20),
            (30, 30, CURRENT_TIMESTAMP(2), 30)
    ON DUPLICATE KEY UPDATE
        col1 = 100,
        col3 = 100,
        col4 = '2019-08-20'::TIMESTAMP,
        col5 = 100;
SELECT col1, col2, col3, col5 FROM t2 WHERE col2 = 9 OR col5 > 1 ORDER BY col1, col2, col3, col5;

--- test subquery
---- should insert
SELECT col1 * 1000, col2 * 1000 + 1 FROM t2 ORDER BY col1 LIMIT 2;

SELECT col1, col2, col3 FROM t2 
    WHERE col1 >= (SELECT col1 * 1000 FROM t2 ORDER BY col1 LIMIT 1)
    ORDER BY col1, col2;

INSERT INTO t2
    (SELECT col1 * 1000, col2 * 1000 + 1 FROM t2 ORDER BY col1 LIMIT 2)
    ON DUPLICATE KEY UPDATE col3 = col1 + 1;

SELECT col1, col2, col3 FROM t2 
    WHERE col1 >= (SELECT col1 * 1000 FROM t2 ORDER BY col1 LIMIT 1)
    ORDER BY col1, col2;

---- should update
INSERT INTO t2
    (SELECT col1 * 1000, col2 * 1000 + 1 FROM t2 ORDER BY col1 LIMIT 2)
    ON DUPLICATE KEY UPDATE col3 = col2 + 1;

SELECT col1, col2, col3 FROM t2
    WHERE col1 >= (SELECT col1 * 1000 FROM t2 ORDER BY col1 LIMIT 1)
    ORDER BY col1, col2;

--- test subquery
--- should insert
SELECT col1 * 1000 + 2, col2 * 1000 FROM t2 ORDER BY col1 LIMIT 2 OFFSET 1;
SELECT col1, col2, col3 FROM t2
    WHERE col2 >= (SELECT col1 * 1000 + 2 FROM t2 ORDER BY col1 LIMIT 1 OFFSET 1)
    ORDER BY col1, col2;

INSERT INTO t2 (col2, col3)
    (SELECT col1 * 1000 + 2, col2 * 1000 FROM t2 ORDER BY col1 LIMIT 2 OFFSET 1)
    ON DUPLICATE KEY UPDATE col3 = col2 + 1;
SELECT col1, col2, col3 FROM t2 
    WHERE col2 >= (SELECT col1 * 1000 + 2 FROM t2 ORDER BY col1 LIMIT 1 OFFSET 1)
    ORDER BY col1, col2;

-- should update
INSERT INTO t2 (col2, col3)
    (SELECT col1 * 1000 + 2, col2 * 1000 FROM t2 ORDER BY col1 LIMIT 2 OFFSET 1)
    ON DUPLICATE KEY UPDATE col3 = col2 + 1;

SELECT col1, col2, col3 FROM t2 
    WHERE col2 >= (SELECT col1 * 1000 + 2 FROM t2 ORDER BY col1 LIMIT 1 OFFSET 1)
    ORDER BY col1, col2;

--- test union, some insert some update
SELECT col1, col2 FROM t1 WHERE col2 IS NOT NULL
    UNION
    SELECT col1, col3 FROM t1 ORDER BY 1, 2;
INSERT INTO t2 (col1, col2)
    SELECT * FROM
        (SELECT col1, col2 FROM t1 WHERE col2 IS NOT NULL
        UNION
        SELECT col1, col3 FROM t1) AS union_table
    ON DUPLICATE KEY UPDATE col3 = (col1 + col2 + col3::FLOAT) * 10;

set behavior_compat_options='merge_update_multi';
INSERT INTO t2 (col1, col2)
    SELECT * FROM
        (SELECT col1, col2 FROM t1 WHERE col2 IS NOT NULL
        UNION
        SELECT col1, col3 FROM t1) AS union_table
    ON DUPLICATE KEY UPDATE col3 = (col1 + col2 + col3::FLOAT) * 10;
SELECT col1, col2, col3 FROM t2 ORDER BY col1, col2, col3;
reset behavior_compat_options;

-- test t3 with one primary index with two columns
CREATE TEMPORARY TABLE t3 (
    col1 INT,
    col2 VARCHAR,
    col3 TEXT DEFAULT 1,
--    col4 TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    col5 TEXT,
    PRIMARY KEY (col2, col3)
)DISTRIBUTE BY hash(col2);

--- column referenced by primary key are not allowed to update
INSERT INTO t3 VALUES (1, 1) ON DUPLICATE KEY UPDATE col3 = 3;

--- should insert when not contains primary key and not all primary key referred columns have default value.
--- but will fail cause primary key should not be null
INSERT INTO t3 (col1) VALUES (1) ON DUPLICATE KEY UPDATE col1 = 2;

--- should insert
--- (SEQUENCE BUG: the serial column will starts from 2 since the above statement has applied for a sequence)
INSERT INTO t3 VALUES (1, 1) ON DUPLICATE KEY UPDATE col1 = 10;
INSERT INTO t3 VALUES (2, 2, 2) ON DUPLICATE KEY UPDATE col5 = 20;
SELECT * FROM t3 order by col1, col2, col3;

--- should update
INSERT INTO t3 VALUES (1, 1) ON DUPLICATE KEY UPDATE col1 = 10;
INSERT INTO t3 VALUES (2, 2, 2) ON DUPLICATE KEY UPDATE col5 = 20;
SELECT * FROM t3 order by col1, col2, col3;

--- test subquery
---- should insert
INSERT INTO t3 (col2, col3) (SELECT max(col2) + 1, max(col3) + 1 FROM t3) ON DUPLICATE KEY UPDATE col1 = col2 + col3;
SELECT * FROM t3 order by col1, col2, col3;

---- should update
INSERT INTO t3 (col2, col3) (SELECT max(col2), max(col3) FROM t3) ON DUPLICATE KEY UPDATE col1 = col2 + col3;
SELECT * FROM t3 order by col1, col2, col3;

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
SELECT * FROM t3 order by col1, col2, col3;

--- should insert
INSERT INTO t3 VALUES (1, 1) ON DUPLICATE KEY UPDATE col1 = 10;
INSERT INTO t3 VALUES (2, 2, 2) ON DUPLICATE KEY UPDATE t3.col5 = 20;
SELECT * FROM t3 ORDER BY col1, col2, col3;

--- should update
INSERT INTO t3 VALUES (1, 1) ON DUPLICATE KEY UPDATE t3.col1 = 10;
INSERT INTO t3 VALUES (2, 2, 2) ON DUPLICATE KEY UPDATE col5 = 20;
SELECT * FROM t3 ORDER BY col1, col2, col3;

--- test subquery
---- should insert
INSERT INTO t3 (SELECT 100, NULL, max(col3) FROM t3) ON DUPLICATE KEY UPDATE col1 = col2 + col3;
INSERT INTO t3 (SELECT 100, NULL, max(col3) FROM t3) ON DUPLICATE KEY UPDATE col1 = col2 + col3;
INSERT INTO t3 (col2, col3) (SELECT max(col2) + 1, max(col3) + 1 FROM t3) ON DUPLICATE KEY UPDATE col1 = col2 + col3;
SELECT * FROM t3 ORDER BY col1, col2, col3;

---- should update
INSERT INTO t3 (col2, col3) (SELECT max(col2), max(col3) FROM t3) ON DUPLICATE KEY UPDATE col1 = col2 + col3;
SELECT * FROM t3 ORDER BY col1, col2, col3;

DROP SCHEMA test_insert_update_006 CASCADE;
