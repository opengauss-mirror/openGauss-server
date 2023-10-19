
DROP SCHEMA test_insert_update_001 CASCADE;
CREATE SCHEMA test_insert_update_001;
SET CURRENT_SCHEMA TO test_insert_update_001;

-- test description
\h INSERT

-- test permission
--- test with no sequence column
CREATE TABLE t00 (col1 INT DEFAULT 1 PRIMARY KEY, col2 INT);
CREATE USER insert_update_tester PASSWORD '123456@cc';
GRANT ALL PRIVILEGES ON SCHEMA test_insert_update_001 TO insert_update_tester;
SET SESSION SESSION AUTHORIZATION insert_update_tester PASSWORD '123456@cc';
INSERT INTO test_insert_update_001.t00 VALUES(1) ON DUPLICATE KEY UPDATE col2 = 5;
RESET SESSION AUTHORIZATION;

---- error: only have INSERT permission
GRANT INSERT ON test_insert_update_001.t00 TO insert_update_tester;
SET SESSION SESSION AUTHORIZATION insert_update_tester PASSWORD '123456@cc';
INSERT INTO test_insert_update_001.t00 VALUES(1) ON DUPLICATE KEY UPDATE col2 = 5;
RESET SESSION AUTHORIZATION;

---- success: have INSERT UPDATE permission
GRANT INSERT, UPDATE ON test_insert_update_001.t00 TO insert_update_tester;
SET SESSION SESSION AUTHORIZATION insert_update_tester PASSWORD '123456@cc';
INSERT INTO test_insert_update_001.t00 VALUES(1) ON DUPLICATE KEY UPDATE col2 = 5;
RESET SESSION AUTHORIZATION;

--- have SELECT INSERT UPDATE permission
GRANT SELECT, INSERT, UPDATE ON test_insert_update_001.t00 TO insert_update_tester;
SET SESSION SESSION AUTHORIZATION insert_update_tester PASSWORD '123456@cc';
INSERT INTO test_insert_update_001.t00 VALUES(1) ON DUPLICATE KEY UPDATE col2 = 5;
INSERT INTO test_insert_update_001.t00 VALUES(1) ON DUPLICATE KEY UPDATE col3 = 5;
RESET SESSION AUTHORIZATION;

--- test with sequnce column
CREATE TABLE t01 (col1 INT , col2 BIGSERIAL PRIMARY KEY, col3 INT) with(storage_type=ustore);

---- error: don't have UPDATE permission on sequence table.
GRANT SELECT, INSERT, UPDATE ON test_insert_update_001.t01 TO insert_update_tester;
SET SESSION SESSION AUTHORIZATION insert_update_tester PASSWORD '123456@cc';
INSERT INTO test_insert_update_001.t01 VALUES(1) ON DUPLICATE KEY UPDATE col3 = 5;
RESET SESSION AUTHORIZATION;

---- have SELECT INSERT UPDATE permission on target relation, and UPDATE permission on sequence table.
GRANT UPDATE ON test_insert_update_001.t01_col2_seq TO insert_update_tester;
SET SESSION SESSION AUTHORIZATION insert_update_tester PASSWORD '123456@cc';
INSERT INTO test_insert_update_001.t01 VALUES(1) ON DUPLICATE KEY UPDATE col3 = 5;
INSERT INTO test_insert_update_001.t01 VALUES(1) ON DUPLICATE KEY UPDATE col3 = 5;
RESET SESSION AUTHORIZATION;

-- test ommit INSERT target column
CREATE TABLE t02 (col1 INT DEFAULT 1 PRIMARY KEY, col2 INT, col3 INT) with(storage_type=ustore);
INSERT INTO t02 VALUES(1, 2, 3) ON DUPLICATE KEY UPDATE col2 = 20;
SELECT * FROM t02 ORDER BY 1, 2;
INSERT INTO t02 VALUES(1, 2, 3) ON DUPLICATE KEY UPDATE col2 = 20;
SELECT * FROM t02 ORDER BY 1, 2;

ALTER TABLE t02 DROP COLUMN col2;
ALTER TABLE t02 ADD COLUMN col4 INT;
INSERT INTO t02 VALUES(1, 2, 3) ON DUPLICATE KEY UPDATE col4 = 40;
SELECT * FROM t02 ORDER BY 1, 2;
INSERT INTO t02 VALUES(2, 3, 4) ON DUPLICATE KEY UPDATE col4 = 40;
SELECT * FROM t02 ORDER BY 1, 2;
INSERT INTO t02 VALUES(2, 3, 4) ON DUPLICATE KEY UPDATE col4 = 40;
SELECT * FROM t02 ORDER BY 1, 2;

-- test restriction
--- test replication table
CREATE TABLE t03 (col1 int PRIMARY KEY, col2 INT) with(storage_type=ustore);
--- error: not allowed volatile function as default value
INSERT INTO t03(col2) VALUES(1) ON DUPLICATE KEY UPDATE col2 = 100;

--- error: primary key are not allowed to update
INSERT INTO t03 VALUES(1) ON DUPLICATE KEY UPDATE col1 = 1;
--- error: clause other than VALUSES are not allowed to use
INSERT INTO t03 SELECT * FROM t03 ON DUPLICATE KEY UPDATE col2 = 1;
--- error: expression index are not supported
CREATE UNIQUE INDEX u_expr_index ON t03 USING ubtree (abs(col1));
INSERT INTO t03 VALUES(-10, 10) ON DUPLICATE KEY UPDATE col2 = 20;
DROP INDEX u_expr_index;

-- test with stream operator on
INSERT INTO t03 VALUES(1) ON DUPLICATE KEY UPDATE col2 = 100;
SELECT * FROM t03;

INSERT INTO t03 VALUES(1) ON DUPLICATE KEY UPDATE col2 = 100;
SELECT * FROM t03;
SELECT * FROM t03;

--- test PBE
PREPARE p1 AS INSERT INTO t03 VALUES($1, $2) ON DUPLICATE KEY UPDATE col2 = $1*100;
EXECUTE p1(5, 50);
SELECT * FROM t03 WHERE col1 = 5;
EXECUTE p1(5, 50);
SELECT * FROM t03 WHERE col1 = 5;
DELETE t03 WHERE col1 = 5;

---- test with primary key
INSERT INTO t03 VALUES(2) ON DUPLICATE KEY UPDATE col2 = 200;
SELECT * FROM t03;

INSERT INTO t03 VALUES(2) ON DUPLICATE KEY UPDATE col2 = 200;
SELECT * FROM t03;
SELECT * FROM t03;

---- test with unique key without NOT NULL constraint 
---- the result is change compare with heap_table
drop table t03;
create table t03(col1 int PRIMARY KEY, col2 INT, col3 int) with(storage_type=ustore);
CREATE UNIQUE INDEX ON t03 (col1, col3);
---- unique constraints might contain NULL, depends on the plan
----- for cn light and fqs, it can be done
INSERT INTO t03 VALUES(3) ON DUPLICATE KEY UPDATE col2 = 300;
----- for stream or pgxc it can not be done
INSERT INTO t03 VALUES(3) ON DUPLICATE KEY UPDATE col2 = 300;

---- test with unique key with NOT NULL constraint, should success
CREATE UNIQUE INDEX ON t03 (col1);
INSERT INTO t03 VALUES(3) ON DUPLICATE KEY UPDATE col2 = 300;
----- only col1 has result:3
SELECT * FROM t03; 

INSERT INTO t03 VALUES(3) ON DUPLICATE KEY UPDATE col2 = 300;
SELECT * FROM t03;
SELECT * FROM t03;

---- test PBE
PREPARE p2 AS INSERT INTO t03 VALUES($1, $2) ON DUPLICATE KEY UPDATE col2 = $1*100;
EXECUTE p2(5, 50);
SELECT * FROM t03 WHERE col1 = 5;
EXECUTE p2(5, 50);
SELECT * FROM t03 WHERE col1 = 5;

--- error: test with clause
WITH tmp(col1, col2) AS (SELECT * FROM t01)
INSERT INTO t01 SELECT * FROM tmp ON DUPLICATE KEY UPDATE col1 = 1;

WITH RECURSIVE rq AS
    (
    SELECT col1, col2 FROM t00 WHERE col1 = 1
    UNION ALL
    SELECT origin.col1, rq.col2
    FROM rq JOIN t00 AS origin ON origin.col1 = rq.col1
    )
    INSERT INTO t03 SELECT * FROM rq ON DUPLICATE KEY UPDATE col1 = rq.col1;

--- error: test returning clause
INSERT INTO t01 VALUES (1) ON DUPLICATE KEY UPDATE col1 = 1 RETURNING NOT(1::bool);

--- error: distribute key are not allowed to UPDATE
CREATE TABLE t04 (col1 INT, col2 INT) with(storage_type=ustore);
INSERT INTO t04 VALUES (1) ON DUPLICATE KEY UPDATE col1 = 5;

--- error: unique index referenced column are not allowed to UPDATE
CREATE UNIQUE INDEX t04_u_index ON t04(col1, col2);
INSERT INTO t04 VALUES (1) ON DUPLICATE KEY UPDATE col2 = 5;
DROP INDEX t04_u_index;

--- error: primary key referenced column are not allowed to UPDATE
ALTER TABLE t04 ADD PRIMARY KEY (col1, col2);
INSERT INTO t04 VALUES (1) ON DUPLICATE KEY UPDATE col2 = 5;

--- error: invalid column
INSERT INTO t04 (col2, col3) VALUES (2, 3) ON DUPLICATE KEY UPDATE col2 = 5;

--- error: duplicate column
INSERT INTO t04 (col2, col2) VALUES (2, 3) ON DUPLICATE KEY UPDATE col2 = 5;

-- error: target column more than insert target
INSERT INTO t04 (col1, col2) VALUES (2, 3, 4) ON DUPLICATE KEY UPDATE col2 = 5;
INSERT INTO t04 (col1, col2) VALUES (1) ON DUPLICATE KEY UPDATE col2 = 5;
INSERT INTO t04 (col1, col2) SELECT col1 FROM t04 ON DUPLICATE KEY UPDATE col2 = 5;
INSERT INTO t04 (col1, col2) SELECT *, col1 FROM t04 ON DUPLICATE KEY UPDATE col2 = 5;
INSERT INTO t04 VALUES (2, 3, 4) ON DUPLICATE KEY UPDATE col2 = 5;
INSERT INTO t04 VALUES (1) ON DUPLICATE KEY UPDATE col2 = 5;
INSERT INTO t04 SELECT col1 FROM t04 ON DUPLICATE KEY UPDATE col2 = 5;
INSERT INTO t04 SELECT *, col1 FROM t04 ON DUPLICATE KEY UPDATE col2 = 5;

-- test DEFAULT VALUES
truncate t00;
truncate t01;

--- without sequence
----should insert
INSERT INTO t00 DEFAULT VALUES ON DUPLICATE KEY UPDATE col2 = col1;
SELECT * FROM t00 ORDER BY 1, 2;
---- should update
INSERT INTO t00 DEFAULT VALUES ON DUPLICATE KEY UPDATE col2 = col1;
SELECT * FROM t00 ORDER BY 1, 2;

--- test drop column
drop table t00;
create table t00(col1 int default 1 primary key, col3 int default 100) with(storage_type=ustore);
----should insert
INSERT INTO t00 DEFAULT VALUES ON DUPLICATE KEY UPDATE col3 = col1;
SELECT * FROM t00 ORDER BY 1, 2;
---- should update
INSERT INTO t00 DEFAULT VALUES ON DUPLICATE KEY UPDATE col3 = col1;
SELECT * FROM t00 ORDER BY 1, 2;

--- with sequence
----should insert
INSERT INTO t01 DEFAULT VALUES ON DUPLICATE KEY UPDATE col1 = col2;
INSERT INTO t01 (col2) SELECT col2 + 1 FROM t01 LIMIT 1;
SELECT * FROM t01 ORDER BY 1, 2, 3;

---- should update
INSERT INTO t01 DEFAULT VALUES ON DUPLICATE KEY UPDATE col1 = col2;
SELECT * FROM t01 ORDER BY 1, 2, 3;

-- test VALUES(DEFAULT)
CREATE TABLE t05 (col1 INT , col2 INT DEFAULT 1 PRIMARY KEY, col3 INT DEFAULT 100) with(storage_type=ustore);
--- should insert
INSERT INTO t05 VALUES(DEFAULT) ON DUPLICATE KEY UPDATE col3 = 1000;
SELECT * FROM t05 ORDER BY 1, 2, 3;

--- should update
INSERT INTO t05 VALUES(DEFAULT) ON DUPLICATE KEY UPDATE col3 = 1000;
SELECT * FROM t05 ORDER BY 1, 2, 3;

-- test UPDATE DEFAULT
INSERT INTO t05 (col1, col2, col3) VALUES (2, 2, 2) ON DUPLICATE KEY UPDATE col3 = DEFAULT;
SELECT * FROM t05 ORDER BY 1, 2, 3;
INSERT INTO t05 (col1, col2, col3) VALUES (2, 2, 2) ON DUPLICATE KEY UPDATE col3 = DEFAULT;
SELECT * FROM t05 ORDER BY 1, 2, 3;

-- test VALUES (DEFAULT, ...)
TRUNCATE t05;
--- should insert
INSERT INTO t05 VALUES(DEFAULT, DEFAULT, 200), (DEFAULT, 200, DEFAULT)
ON DUPLICATE KEY UPDATE col3 = DEFAULT, col1 = col3;
SELECT * FROM t05 ORDER BY 1, 2, 3;

--- should update
INSERT INTO t05 VALUES(DEFAULT, DEFAULT, 200), (DEFAULT, 200, DEFAULT)
ON DUPLICATE KEY UPDATE col3 = DEFAULT, col1 = col3;
SELECT * FROM t05 ORDER BY 1, 2, 3;

--- test drop coulmn
BEGIN;
ALTER TABLE t05 ADD COLUMN col4 INT;
ALTER TABLE t05 ADD COLUMN col5 INT DEFAULT 500;
ALTER TABLE t05 DROP COLUMN col3;
TRUNCATE t05;
INSERT INTO t05 VALUES(DEFAULT, DEFAULT, DEFAULT, 600) ON DUPLICATE KEY UPDATE col5 = DEFAULT;
SELECT * FROM t05 ORDER BY 1, 2, 3;
INSERT INTO t05 VALUES(DEFAULT, DEFAULT, DEFAULT, 600) ON DUPLICATE KEY UPDATE col5 = DEFAULT;
SELECT * FROM t05 ORDER BY 1, 2, 3;
ROLLBACK;

-- test schema
SET current_schema = public;
TRUNCATE test_insert_update_001.t05;
--- should insert
INSERT INTO test_insert_update_001.t05 VALUES(DEFAULT, DEFAULT, 200), (DEFAULT, 200, DEFAULT)
ON DUPLICATE KEY UPDATE col3 = DEFAULT, col1 = col3;
SELECT * FROM test_insert_update_001.t05 ORDER BY 1, 2, 3;

--- should update
INSERT INTO test_insert_update_001.t05 VALUES(DEFAULT, DEFAULT, 200), (DEFAULT, 200, DEFAULT)
ON DUPLICATE KEY UPDATE col3 = DEFAULT, col1 = col3;
SELECT * FROM test_insert_update_001.t05 ORDER BY 1, 2, 3;

--- test using schema on update
INSERT INTO test_insert_update_001.t05 VALUES(DEFAULT, DEFAULT, 200), (DEFAULT, 200, DEFAULT)
ON DUPLICATE KEY UPDATE t05.col3 = DEFAULT, t05.col1 = t05.col3 + 1;
SELECT * FROM test_insert_update_001.t05 ORDER BY 1, 2, 3;

--- error: should not append schema
INSERT INTO test_insert_update_001.t05 VALUES(DEFAULT, DEFAULT, 200), (DEFAULT, 200, DEFAULT)
ON DUPLICATE KEY UPDATE test_insert_update_001.t05.col3 = DEFAULT, t05.col1 = t05.col3 + 1;

INSERT INTO test_insert_update_001.t05 VALUES(DEFAULT, DEFAULT, 200), (DEFAULT, 200, DEFAULT)
ON DUPLICATE KEY UPDATE t05.col3 = DEFAULT, t05.col1 = test_insert_update_001.t05.col3 + 1;

SET CURRENT_SCHEMA TO test_insert_update_001;

DROP USER insert_update_tester CASCADE;
DROP SCHEMA test_insert_update_001 CASCADE;
