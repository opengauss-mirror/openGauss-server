CREATE DATABASE test_db DBCOMPATIBILITY 'B';
\c test_db
SET autocommit = 1;
CREATE TABLE test_table (a text);
CREATE DATABASE test_drop;
INSERT INTO test_table values('aaaaa');
SELECT * FROM test_table;
ROLLBACK;
SELECT * FROM test_table;

SET autocommit = 0;
-- DML
-- rollback the insert statement
INSERT INTO test_table values('bbbbb');
SELECT * FROM test_table;
ROLLBACK;
SELECT * FROM test_table;

-- commit the insert statement
INSERT INTO test_table values('ccccc');
SELECT * FROM test_table;
COMMIT;
SELECT * FROM test_table;

-- commit the insert statement auto
INSERT INTO test_table values('ddddd');
SELECT * FROM test_table;
SET autocommit = 1;
SELECT * FROM test_table;

SET autocommit = 0;
-- DDL
-- rollback the create table statement
CREATE TABLE test_a (a text);
INSERT INTO test_a values('aaaaa');
SELECT * FROM test_a;
ROLLBACK;
SELECT * FROM test_a;
COMMIT;

-- commit the create table statement
CREATE TABLE test_b (a text);
INSERT INTO test_b values('aaaaa');
SELECT * FROM test_b;
COMMIT;
SELECT * FROM test_b;

-- commit the create table statement auto
CREATE TABLE test_c (a text);
INSERT INTO test_c values('aaaaa');
SELECT * FROM test_c;
SET autocommit = 1;
SELECT * FROM test_c;

-- prepare test
SET autocommit = 0;
INSERT INTO test_table values('eeeee');
PREPARE TRANSACTION 'test_id';
SET autocommit = 1;
SELECT * FROM test_table;
COMMIT PREPARED 'test_id';
SELECT * FROM test_table;

-- truncate the table test_table
TRUNCATE test_table;
SELECT * FROM test_table;
ROLLBACK;
TRUNCATE test_table;
SELECT * FROM test_table;
COMMIT;
SELECT * FROM test_table;

-- something statement could not execute in the transaction block
SET autocommit = 0;
START TRANSACTION;
BEGIN;
CREATE DATABASE test_error;
ROLLBACK;
VACUUM;
ROLLBACK;
DROP DATABASE test_drop;
ROLLBACK;
CLUSTER test_table;
ROLLBACK;
CREATE TABLESPACE gs_basebackup_tablespace relative LOCATION 'gs_basebackup_tablespace';
ROLLBACK;
CREATE INDEX CONCURRENTLY ON test_table(a);
ROLLBACK;
REINDEX DATABASE test_db;
ROLLBACK;
DROP DATABASE test_error;
ROLLBACK;
DROP TABLESPACE test_space;
ROLLBACK;
DROP INDEX test_index;
ROLLBACK;
REINDEX TABLE CONCURRENTLY test_table;
-- test about set autocommit = 1 when the transaction is aborted
SET autocommit = 1;
ROLLBACK;

-- set autocommit = 0 in a transaction block
SET autocommit = 1;
TRUNCATE test_table;
BEGIN;
INSERT INTO test_table values('aaaaa');
SET autocommit = 0;
SET autocommit = 1;
SHOW autocommit;
SELECT * FROM test_table;

-- only set autocommit = 1 cannot commit transaction
BEGIN;
INSERT INTO test_table values('bbbbb');
SET autocommit = 1;
ROLLBACK;
SELECT * FROM test_table;

-- set autocommit = 0 and rollback
BEGIN;
INSERT INTO test_table values('ccccc');
SET autocommit = 0;
ROLLBACK;
SELECT * FROM test_table;
SHOW autocommit;

-- error in transaction block
SET autocommit = 0;
CREATE;
SET autocommit = 1;
ROLLBACK;
SET autocommit = 1;

\c regression
DROP DATABASE test_db;
DROP DATABASE test_drop;
