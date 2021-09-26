--
-- TEMP
-- Test temp relations and indexes
--

-- Enforce use of COMMIT instead of 2PC for temporary objects

-- test temp table/index masking

CREATE TABLE temptest(col int);

CREATE INDEX i_temptest ON temptest(col);

CREATE TEMP TABLE temptest(tcol int);

CREATE INDEX i_temptest ON temptest(tcol);

SELECT * FROM temptest;

DROP INDEX i_temptest;

DROP TABLE temptest;

SELECT * FROM temptest;

DROP INDEX i_temptest;

DROP TABLE temptest;

-- test temp table selects

CREATE TABLE temptest(col int);

INSERT INTO temptest VALUES (1);

CREATE TEMP TABLE temptest(tcol float);

INSERT INTO temptest VALUES (2.1);

SELECT * FROM temptest;

DROP TABLE temptest;

SELECT * FROM temptest;

DROP TABLE temptest;

-- test temp table deletion

CREATE TEMP TABLE temptest(col int);

\c

-- Enforce use of COMMIT instead of 2PC for temporary objects

SELECT * FROM temptest;

-- Test ON COMMIT DELETE ROWS

CREATE TEMP TABLE temptest(col int) ON COMMIT DELETE ROWS;
START TRANSACTION;
INSERT INTO temptest VALUES (1);
INSERT INTO temptest VALUES (2);

SELECT * FROM temptest  ORDER BY 1;
COMMIT;

SELECT * FROM temptest;

DROP TABLE temptest;

START TRANSACTION;
CREATE TEMP TABLE temptest(col) ON COMMIT PRESERVE ROWS AS SELECT 1;

SELECT * FROM temptest;
COMMIT;

SELECT * FROM temptest;

DROP TABLE temptest;

-- Test ON COMMIT DROP

START TRANSACTION;

CREATE TEMP TABLE temptest(col int) ON COMMIT DROP;

INSERT INTO temptest VALUES (1);
INSERT INTO temptest VALUES (2);

SELECT * FROM temptest ORDER BY 1;
COMMIT;

SELECT * FROM temptest;

START TRANSACTION;
CREATE TEMP TABLE temptest(col) ON COMMIT DROP AS SELECT 1;

SELECT * FROM temptest;
COMMIT;

SELECT * FROM temptest;

-- Test foreign keys
START TRANSACTION;
CREATE TEMP TABLE temptest1(col int PRIMARY KEY);
CREATE TEMP TABLE temptest2(col int REFERENCES temptest1)
  ON COMMIT DELETE ROWS;
INSERT INTO temptest1 VALUES (1);
INSERT INTO temptest2 VALUES (1);
COMMIT;
SELECT * FROM temptest1;
SELECT * FROM temptest2;

START TRANSACTION;
CREATE TEMP TABLE temptest3(col int PRIMARY KEY) ON COMMIT DELETE ROWS;
CREATE TEMP TABLE temptest4(col int REFERENCES temptest3);
COMMIT;

-- Test manipulation of temp schema's placement in search path

create table public.whereami (f1 text);
insert into public.whereami values ('public');

create temp table whereami (f1 text);
insert into whereami values ('temp');

create function public.whoami() returns text
  as $$select 'public'::text$$ language sql;

create function pg_temp.whoami() returns text
  as $$select 'temp'::text$$ language sql;

-- default should have pg_temp implicitly first, but only for tables
select * from whereami;
select whoami();

-- can list temp first explicitly, but it still doesn't affect functions
set search_path = pg_temp, public;
select * from whereami;
select whoami();

-- or put it last for security
set search_path = public, pg_temp;
select * from whereami;
select whoami();

-- you can invoke a temp function explicitly, though
select pg_temp.whoami();

drop table public.whereami;


--TEST CASE FOR TEMP TABLE
--1. temp table's feature:
--	1). Coordinator isolation
--  2). Session isolation.
--  3). Auto drop when session quit.
--  4). Unlogged
--  5). Other features are the same as ordinary table.

--2. temp table's SQL interface:
--  1) Create
--  2) Use
--  3) Analyze
--  4) Truncate
--  5) Discard
--  6) Alter


--Prepare
create table test_ordinary(a int, b int);
insert into test_ordinary select generate_series(1, 1000), generate_series(1, 1000);
create index ordinary_idx on test_ordinary(a);

create table col_ordinary(a int) with (orientation = column);
insert into col_ordinary select a from test_ordinary;

create sequence test_seq;

create schema test_temp;

create temp table test_base(a int);
insert into test_base select generate_series(1, 1000);

create temp table test_temp1(a int, b varchar2(3000));
insert into test_temp1 select a, lpad(a, 3000, '-') from test_base;
create index temp1_idx on test_temp1(a, b);

create temp table test_temp2(a int, b varchar2(3000));
insert into test_temp2 select a, lpad(a, 3000, '+') from test_base;
create index temp2_idx on test_temp2(a);

create temp table temp_col1(a int, b varchar2(3000)) with (orientation = column);
insert into temp_col1 select * from test_temp1;
create index col1_idx on temp_col1(a);

create temp table temp_col2(a int, b varchar2(3000)) with (orientation = column);
insert into temp_col2 select * from temp_col1;

--Session isolation

--Auto drop when session quit.

--Unlogged

--Other features

--temp function

--Create temp table
create local temp table tl(a int, b varchar2(3000));
create local temporary table lt(a int, b varchar2(3000));
create temp table pg_temp.temp_t(a int);
create table pg_temp.temp_t1(a int);
create temp table temp_t2 as select * from temp_t1;
create temp table pg_temp.temp_t3 as select * from temp_t1;
create table pg_temp.temp_t4 as select * from temp_t1;
create temp table pg_temp.temp_t5(like temp_t1);
create temp table temp_t6(like temp_t1);
create table pg_temp.temp_t7(like temp_t1);
select * from pg_temp.temp_t1;
--Analyze
Analyze test_base;
Analyze test_temp1;
select relname, relpages, reltuples from pg_class where relname = 'test_temp1';
Analyze pg_temp.test_temp2;
select relname, relpages, reltuples from pg_class where relname = 'test_temp2';
Analyze temp_col1;
select relname, relpages, reltuples from pg_class where relname = 'temp_col1';
select relname, relpages, reltuples from pg_class where relname = 'temp_col2';
Analyze temp_col2;

--Use
select a/0 from test_temp1;
select * from test_temp1 order by 1 limit 30;
select t1.a, t2.a from test_temp1 t1, test_temp2 t2 where t1.a = t2.a order by t1.a limit 50;
select t1.a, t2.a from test_temp1 t1, test_ordinary t2 where t1.a = t2.a order by t1.a limit 50;

select t1.a, t2.b from test_temp1 t1, test_ordinary t2 where t1.a = t2.b order by t1.a limit 50;

explain select t1.a, t2.a from test_temp1 t1, test_col t2 where t1.a = t2.a order by t1.a limit 50;
select t1.a, t2.a from test_temp1 t1, test_col t2 where t1.a = t2.a order by t1.a limit 50;

--test Alter
Alter table test_temp1 rename to test_temp3;
Alter table test_temp3 set schema pg_temp;
Alter table test_temp3 set schema public;
Alter table test_temp3 rename to test_temp1;


--Col Table
select t1.a from temp_col1 t1, col_ordinary t2 where t1.a = t2.a order by 1 limit 30;
select t1.a from temp_col1 t1, temp_col2 t2 where t1.a = t2.a order by 1 limit 30;
select t1.a from test_temp1 t1, temp_col1 t2 where t1.a = t2.a order by 1 limit 30;

Alter temp_col1 rename to temp_col2;
Create view col_v1 as select * from temp_col2;
Alter view col_v1 alter a set default 1;
Drop view col_v1;

--test View
create view v1 as select * from test_temp1;
select a from v1 order by 1 limit 50;
create view v2 as select t1.a from test_temp1 t1, test_ordinary t2 where t1.a = t2.a;
select a from v2 order by a limit 50;

create view v3 as select t1.a from test_temp1 t1, test_temp2 t2 where t1.a = t2.a;
select a from v3 order by a limit 50;

alter view v2 rename to v4;
alter view v4 alter a set default 1;

alter view v4 set schema test_temp;

create view v5 as select * from test_ordinary;

drop view v1, v5;
drop view v1;
drop view v3,v4;


--test SEQ
create temp sequence temp_seq;
alter sequence temp_seq rename to temp_seq1;
alter sequence temp_seq1 minvalue 10;
drop sequence test_seq;
create temp table test_serial(a serial, b varchar2(100));
create temp table test_serial2(a int);
alter table test_serial2 add b serial;
drop table test_serial;
drop table test_serial2;

--test rule

--test trigger

--test Reindex
reindex index temp1_idx, ordinary_idx;
reindex index temp1_idx;
reindex index temp2_idx;
reindex table test_temp1;
--test Drop index
drop index temp1_idx, ordinary_idx;
drop index temp1_idx, temp2_idx;

--Vacuum
vacuum test_temp1;
vacuum test_temp2;
vacuum full test_temp1;
vacuum full test_temp2;
--Truncate
Truncate test_base;
Truncate test_temp1;
Truncate test_temp2;
Truncate test_col;

--Test Drop
Drop table test_base;
Drop table test_temp1, test_ordinary;
Drop table test_temp1, test_temp2;
Drop table temp_col1, temp_col2;