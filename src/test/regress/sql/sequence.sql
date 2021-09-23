
---
--- test creation of SERIAL column
---

CREATE TABLE serialTest (f1 text, f2 serial);

INSERT INTO serialTest VALUES ('foo');
INSERT INTO serialTest VALUES ('bar');
INSERT INTO serialTest VALUES ('force', 100);
INSERT INTO serialTest VALUES ('wrong', NULL);

SELECT * FROM serialTest ORDER BY f1, f2;

-- test smallserial / bigserial
CREATE TABLE serialTest2 (f1 text, f2 serial, f3 smallserial, f4 serial2,
  f5 bigserial, f6 serial8);

INSERT INTO serialTest2 (f1)
  VALUES ('test_defaults');

INSERT INTO serialTest2 (f1, f2, f3, f4, f5, f6)
  VALUES ('test_max_vals', 2147483647, 32767, 32767, 9223372036854775807,
          9223372036854775807),
         ('test_min_vals', -2147483648, -32768, -32768, -9223372036854775808,
          -9223372036854775808);

-- All these INSERTs should fail:
INSERT INTO serialTest2 (f1, f3)
  VALUES ('bogus', -32769);

INSERT INTO serialTest2 (f1, f4)
  VALUES ('bogus', -32769);

INSERT INTO serialTest2 (f1, f3)
  VALUES ('bogus', 32768);

INSERT INTO serialTest2 (f1, f4)
  VALUES ('bogus', 32768);

INSERT INTO serialTest2 (f1, f5)
  VALUES ('bogus', -9223372036854775809);

INSERT INTO serialTest2 (f1, f6)
  VALUES ('bogus', -9223372036854775809);

INSERT INTO serialTest2 (f1, f5)
  VALUES ('bogus', 9223372036854775808);

INSERT INTO serialTest2 (f1, f6)
  VALUES ('bogus', 9223372036854775808);

SELECT * FROM serialTest2 ORDER BY f2 ASC;

SELECT nextval('serialTest2_f2_seq');
SELECT nextval('serialTest2_f3_seq');
SELECT nextval('serialTest2_f4_seq');
SELECT nextval('serialTest2_f5_seq');
SELECT nextval('serialTest2_f6_seq');

-- basic sequence operations using both text and oid references
CREATE SEQUENCE sequence_test;

SELECT nextval('sequence_test'::text);
SELECT nextval('sequence_test'::regclass);
SELECT currval('sequence_test'::text);
SELECT currval('sequence_test'::regclass);
SELECT setval('sequence_test'::text, 32);
SELECT nextval('sequence_test'::regclass);
SELECT setval('sequence_test'::text, 99, false);
SELECT nextval('sequence_test'::regclass);
SELECT setval('sequence_test'::regclass, 32);
SELECT nextval('sequence_test'::text);
SELECT setval('sequence_test'::regclass, 99, false);
SELECT nextval('sequence_test'::text);

DROP SEQUENCE sequence_test;

-- renaming sequences
CREATE SEQUENCE foo_seq;
ALTER TABLE foo_seq RENAME TO foo_seq_new;
SELECT * FROM foo_seq_new;
SELECT nextval('foo_seq_new');
SELECT nextval('foo_seq_new');
SELECT * FROM foo_seq_new;
DROP SEQUENCE IF EXISTS foo_seq_new;
DROP SEQUENCE IF EXISTS foo_seq;

-- renaming serial sequences
ALTER TABLE serialtest_f2_seq RENAME TO serialtest_f2_foo;
INSERT INTO serialTest VALUES ('more');
SELECT * FROM serialTest ORDER BY f1, f2;

--
-- Check dependencies of serial and ordinary sequences
--

-- Enforce use of COMMIT instead of 2PC for temporary objects

--CREATE TEMP SEQUENCE myseq2;
--CREATE TEMP SEQUENCE myseq3;
--CREATE TEMP TABLE t1 (
--  f1 serial,
--  f2 int DEFAULT nextval('myseq2'),
--  f3 int DEFAULT nextval('myseq3'::text)
--);
-- Both drops should fail, but with different error messages:
--DROP SEQUENCE t1_f1_seq;
--DROP SEQUENCE myseq2;
-- This however will work:
--DROP SEQUENCE myseq3;
--DROP TABLE t1;
-- Fails because no longer existent:
--DROP SEQUENCE t1_f1_seq;
-- Now OK:
--DROP SEQUENCE myseq2;

--
-- Alter sequence
--

ALTER SEQUENCE IF EXISTS sequence_test2 RESTART WITH 24
	 INCREMENT BY 4 MAXVALUE 36 MINVALUE 5 CYCLE;

CREATE SEQUENCE sequence_test2 START WITH 32;

SELECT nextval('sequence_test2');

ALTER SEQUENCE sequence_test2 RESTART WITH 24
	 INCREMENT BY 4 MAXVALUE 36 MINVALUE 5 CYCLE;
SELECT nextval('sequence_test2');
SELECT nextval('sequence_test2');
SELECT nextval('sequence_test2');
SELECT nextval('sequence_test2');
SELECT nextval('sequence_test2');

ALTER SEQUENCE sequence_test2 RESTART;

SELECT nextval('sequence_test2');
SELECT nextval('sequence_test2');
SELECT nextval('sequence_test2');

-- Information schema
SELECT * FROM information_schema.sequences WHERE sequence_name IN
  ('sequence_test2', 'serialtest2_f2_seq', 'serialtest2_f3_seq',
   'serialtest2_f4_seq', 'serialtest2_f5_seq', 'serialtest2_f6_seq')
  ORDER BY sequence_name ASC;

-- Test comments
COMMENT ON SEQUENCE asdf IS 'won''t work';
COMMENT ON SEQUENCE sequence_test2 IS 'will work';
COMMENT ON SEQUENCE sequence_test2 IS NULL;

-- Test lastval()
CREATE SEQUENCE seq;
SELECT nextval('seq');
SELECT lastval();
SELECT setval('seq', 99);
SELECT lastval();

CREATE SEQUENCE seq2;
SELECT nextval('seq2');
SELECT lastval();

DROP SEQUENCE seq2;
-- should fail
SELECT lastval();

CREATE USER seq_user PASSWORD 'gauss@123';

START TRANSACTION;
SET LOCAL SESSION AUTHORIZATION seq_user PASSWORD 'gauss@123';
CREATE SEQUENCE seq3;
SELECT nextval('seq3');
REVOKE ALL ON seq3 FROM seq_user;
SELECT lastval();
ROLLBACK;

-- Sequences should get wiped out as well:
DROP TABLE serialTest, serialTest2;

-- Make sure sequences are gone:
SELECT * FROM information_schema.sequences WHERE sequence_name IN
  ('sequence_test2', 'serialtest2_f2_seq', 'serialtest2_f3_seq',
   'serialtest2_f4_seq', 'serialtest2_f5_seq', 'serialtest2_f6_seq')
  ORDER BY sequence_name ASC;

DROP USER seq_user;
DROP SEQUENCE seq;

create sequence seqllt;
create temp table seqllt(c1 int);
select nextval('seqllt');
drop table seqllt;
drop sequence seqllt;

create schema origSchema;
create schema newSchema;
create sequence  origSchema.seq1;
alter sequence origSchema.seq1 set schema newSchema;
drop sequence origSchema.seq1;
drop schema origSchema;
drop schema newSchema;

create table seqCycleTab(c1 int, c2 int); 
create sequence seqCycle maxvalue 5 cycle;
insert into seqCycleTab values(nextval('seqCycle'),generate_series(1,8));
select * from seqCycleTab order by c1,c2;
drop table seqCycleTab;
drop sequence seqCycle;

create sequence seqMin increment by -1 start -1 minvalue -2;
select nextval('seqMin');
select nextval('seqMin');
select nextval('seqMin');
drop sequence seqMin;

create table t1(c1 int);
create sequence seq1;
alter table t1 add column c2 int default nextval('seq1')+1;
drop table t1;
drop sequence seq1;

--temp sequence
create temp sequence seq;
create sequence pg_temp.seq;
--test alter sequence maxvalue
--normal case with nocache
create sequence seq maxvalue 100;
alter sequence  seq maxvalue -1;
alter sequence seq maxvalue 90;
select * from seq;
alter sequence seq maxvalue 110;
select * from seq;
alter sequence seq nomaxvalue;
select * from seq;
alter sequence seq maxvalue 2;
select nextval('seq');
select nextval('seq');
select nextval('seq');
alter sequence seq maxvalue 10;
select nextval('seq');
drop sequence seq;

--normal case with cache
create sequence seq maxvalue 100 cache 5 increment 2 start 2;
select seq.nextval;
execute direct on(coordinator2) 'select seq.nextval';
alter sequence seq maxvalue 15;
alter sequence seq maxvalue 50;
execute direct on (coordinator1) 'select seq.nextval';
execute direct on (coordinator2) 'select seq.nextval';
alter sequence seq maxvalue 100;
execute direct on (coordinator1) 'select seq.nextval';
execute direct on (coordinator2) 'select seq.nextval';
alter sequence seq minvalue 0;
drop sequence seq;

--serial in table
create table test_seq (a int, b serial);
insert into test_seq(a) values(generate_series(1,10));
select * from test_seq order by a;
alter sequence test_seq_b_seq maxvalue 9;
alter sequence test_seq_b_seq maxvalue 20;
insert into test_seq(a) values(generate_series(11,20));
select * from test_seq order by a;
alter sequence test_seq_b_seq maxvalue 100;
insert into test_seq(a) values(generate_series(11,20));
select * from test_seq order by a;
drop table test_seq;

--lock nextval on cn
create sequence seq;
create table test_base(a int, b int);
insert into test_base values(1,1);
create table t_nextval (a int, b int);
create table t_serial(a int, b serial);
begin;
insert into t_nextval select a,nextval('seq') from test_base;
select a.* from pg_locks a,pg_class b where a.relation= b.oid and b.relname='seq';
end;
begin;
insert into t_serial(a) select a from test_base;
select a.* from pg_locks a,pg_class b where a.relation= b.oid and b.relname='t_serial_b_seq';
end;
drop sequence seq;
drop table t_nextval;
drop table test_base;
drop table t_serial;

