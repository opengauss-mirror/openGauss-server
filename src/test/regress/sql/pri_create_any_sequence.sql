CREATE USER test_create_any_sequence_role PASSWORD 'Gauss@1234';
GRANT create any sequence to test_create_any_sequence_role;

CREATE SCHEMA pri_sequence_schema;
set search_path=pri_sequence_schema;

SET ROLE test_create_any_sequence_role PASSWORD 'Gauss@1234';
set search_path=pri_sequence_schema;
---
--- test creation of SERIAL column
---
CREATE TABLE serialTest (f1 text, f2 serial);
reset role;
GRANT create any table to test_create_any_sequence_role;
SET ROLE test_create_any_sequence_role PASSWORD 'Gauss@1234';
set search_path=pri_sequence_schema;
CREATE TABLE serialTest (f1 text, f2 serial);

INSERT INTO serialTest VALUES ('foo');
INSERT INTO serialTest VALUES ('bar');
INSERT INTO serialTest VALUES ('force', 100);
SELECT * FROM serialTest ORDER BY f1, f2;

reset role;
revoke create any table from test_create_any_sequence_role;
SET ROLE test_create_any_sequence_role PASSWORD 'Gauss@1234';
-- basic sequence operations using both text and oid references
CREATE SEQUENCE sequence_test;

SELECT setval('sequence_test'::text, 32);
SELECT nextval('sequence_test'::regclass);
SELECT setval('sequence_test'::text, 99, false);
SELECT nextval('sequence_test'::regclass);
SELECT setval('sequence_test'::regclass, 32);
SELECT nextval('sequence_test'::text);
SELECT setval('sequence_test'::regclass, 99, false);
SELECT nextval('sequence_test'::text);

CREATE SEQUENCE sequence_test1 START WITH 32;
CREATE SEQUENCE sequence_test2 START WITH 24 INCREMENT BY 4 MAXVALUE 36 MINVALUE 5 CYCLE;
SELECT nextval('sequence_test2');
SELECT nextval('sequence_test2');

create sequence seqCycle maxvalue 5 cycle;

--normal case with cache
create sequence seq maxvalue 100 cache 5 increment 2 start 2;
select seq.nextval;
--failed
CREATE TYPE pri_person_type1 AS (id int, name text); --permission denied
CREATE TYPE pri_person_type2 AS (id int, name text); --permission denied
CREATE FUNCTION pri_func_add_sql(integer, integer) RETURNS integer
AS 'select $1 + $2;'
LANGUAGE SQL
IMMUTABLE
RETURNS NULL ON NULL INPUT;
reset role;
drop table pri_sequence_schema.serialtest;
DROP SEQUENCE pri_sequence_schema.sequence_test;
DROP SEQUENCE pri_sequence_schema.sequence_test1;
DROP SEQUENCE pri_sequence_schema.sequence_test2;
DROP SEQUENCE pri_sequence_schema.seqCycle;
DROP SEQUENCE pri_sequence_schema.seq;
DROP SCHEMA pri_sequence_schema cascade;
DROP USER test_create_any_sequence_role cascade;