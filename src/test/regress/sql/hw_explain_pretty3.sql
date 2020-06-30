----
--- CREATE TABLE
----
-- Auto-analyze
\c
set codegen_cost_threshold=0;
set explain_perf_mode = summary;
show autoanalyze;

-- Testset 1 normal cases
set autoanalyze = on;
create table aa_t1 (id int, num int);

-- one table
-- aa_t1 no
insert into aa_t1 values (1,1),(2,2);
select relname, reltuples, relpages from pg_class where relname='aa_t1';

-- insert into select
create table aa_t2 (id int, num int);
-- aa_t1 yes, aa_t2 no
explain analyze insert into aa_t2 select * from aa_t1;
select relname, reltuples, relpages from pg_class where relname='aa_t1' or relname='aa_t2' order by relname;
-- aa_t1 no, aa_t2 no
explain analyze insert into aa_t2 select * from aa_t1;
select relname, reltuples, relpages from pg_class where relname='aa_t1' or relname='aa_t2' order by relname;

-- create table as
-- aa_t2 yes, aa_t3 no
explain analyze create table aa_t3 as select * from aa_t2;
select relname, reltuples, relpages from pg_class where relname='aa_t1' or relname='aa_t2' or relname='aa_t3' order by relname;

-- one table with groupby
set autoanalyze = off;
-- aa_t3 no
explain analyze select num, count(*) from aa_t3 group by num;
select relname, reltuples, relpages from pg_class where relname='aa_t3';
set autoanalyze = on;
-- aa_t3 yes
explain analyze select num, count(*) from aa_t3 group by num;
select relname, reltuples, relpages from pg_class where relname='aa_t3';
-- aa_t3 no
explain analyze select num, count(*) from aa_t3 group by num;

-- multiple tables
drop table aa_t2;
create table aa_t2 (id int, num int);
-- aa_t1 no, aa_t2 no
insert into aa_t2 select * from aa_t1;
select relname, reltuples, relpages from pg_class where relname='aa_t1' or relname='aa_t2' order by relname;
set autoanalyze = off;
-- aa_t1 no, aa_t2 no
explain analyze select * from aa_t1, aa_t2 where aa_t1.id = aa_t2.id;
select relname, reltuples, relpages from pg_class where relname='aa_t1' or relname='aa_t2' order by relname;
set autoanalyze = on;
-- aa_t1 no, aa_t2 yes
explain analyze select * from aa_t1, aa_t2 where aa_t1.id = aa_t2.id;
select relname, reltuples, relpages from pg_class where relname='aa_t1' or relname='aa_t2' order by relname;
-- aa_t1 no, aa_t2 no
explain analyze select * from aa_t1, aa_t2 where aa_t1.id = aa_t2.id;


-- Testset 2 special cases (not work)
set autoanalyze = on;
drop table aa_t1;
drop table aa_t2;
create table aa_t1 (id int, num int);
insert into aa_t1 values (1,1),(2,2);
create table aa_t2 (id int, num int);
insert into aa_t2 values (1,1),(2,2);

-- not stream plan
-- aa_t1 no, aa_t2 no
explain analyze select aa_t1.num from aa_t1, aa_t2 where aa_t1.id = aa_t2.id;
explain analyze select num, count(*) from aa_t1 group by num;
explain analyze select * from aa_t1, aa_t2 where aa_t1.id = aa_t2.id;
select relname, reltuples, relpages from pg_class where relname='aa_t1' or relname='aa_t2' order by relname;

-- user has no privilege
create user test_aa_user identified by "Test@Mpp";
grant select on aa_t1 to test_aa_user;
grant select on aa_t2 to test_aa_user;
SET SESSION AUTHORIZATION test_aa_user PASSWORD 'Test@Mpp';
SELECT session_user, current_user;
-- aa_t1 no, aa_t2 no
explain analyze select * from aa_t1, aa_t2 where aa_t1.id = aa_t2.id;
select relname, reltuples, relpages from pg_class where relname='aa_t1' or relname='aa_t2' order by relname;
create table aa_t2 (id int, num int);
insert into aa_t2 values (1,1),(2,2);
-- aa_t1 no, aa_t2 yes
explain analyze select * from aa_t1, aa_t2 where aa_t1.id = aa_t2.id;
select relname, reltuples, relpages from pg_class where relname='aa_t1' or relname='aa_t2' order by relname, reltuples;
drop table aa_t2;
\c
drop user test_aa_user cascade;
set explain_perf_mode = summary;
set autoanalyze = on;

-- table created in transaction
drop table aa_t3;
start transaction;
-- aa_t3 no, aa_t2 yes
explain analyze create table aa_t3 as select * from aa_t2;
-- aa_t3 no
explain analyze select num, count(*) from aa_t3 group by num;
-- aa_t1 yes, aa_t2 no
explain analyze select * from aa_t1, aa_t2 where aa_t1.id = aa_t2.id;
select relname, reltuples, relpages from pg_class where relname='aa_t1' or relname='aa_t2' or relname='aa_t3' order by relname;
abort;
select relname, reltuples, relpages from pg_class where relname='aa_t1' or relname='aa_t2' or relname='aa_t3' order by relname;

-- table created in function
drop table aa_t2;
create table aa_t2 (id int, num int);
insert into aa_t2 values (1,1),(2,2);
CREATE OR REPLACE FUNCTION test_aa()
RETURNS bool
AS $$
DECLARE
	BEGIN
		execute('create table aa_t3 as select * from aa_t2;');
		execute('select num, count(*) from aa_t3 group by num;');
		return true;
	END; $$
LANGUAGE 'plpgsql' NOT FENCED;
-- aa_t3 no, aa_t2 yes
select test_aa();
select relname, reltuples, relpages from pg_class where relname='aa_t2' or relname='aa_t3' order by relname;
-- no
explain analyze select num, count(*) from aa_t2 group by num;
-- aa_t3 yes
explain analyze select num, count(*) from aa_t3 group by num;
select relname, reltuples, relpages from pg_class where relname='aa_t2' or relname='aa_t3' order by relname;

-- cannot get lock on table
drop table aa_t1;
drop table aa_t2;
create table aa_t1 (id int, num int);
insert into aa_t1 values (1,1),(2,2);
create table aa_t2 (id int, num int);
insert into aa_t2 values (1,1),(2,2);
start transaction;
lock table aa_t2 in access exclusive mode;
-- aa_t2 no
explain analyze select num, count(*) from aa_t2 group by num;
select relname, reltuples, relpages from pg_class where relname='aa_t2';
-- aa_t1 yes, aa_t2 no
explain analyze select * from aa_t1, aa_t2 where aa_t1.id = aa_t2.id;
select relname, reltuples, relpages from pg_class where relname='aa_t1' or relname='aa_t2' order by relname;
abort;
-- aa_t1 no, aa_t2 yes
explain analyze select * from aa_t1, aa_t2 where aa_t1.id = aa_t2.id;
select relname, reltuples, relpages from pg_class where relname='aa_t1' or relname='aa_t2' order by relname;

-- can not analyze (multiple)
drop table aa_t1;
drop table aa_t2;
start transaction;
create table aa_t1 (id int, num int);
insert into aa_t1 values (1,1),(2,2);
create table aa_t2 (id int, num int);
insert into aa_t2 values (1,1),(2,2);
-- aa_t1 no, aa_t2 no
explain analyze select * from aa_t1, aa_t2 where aa_t1.id = aa_t2.id;
select relname, reltuples, relpages from pg_class where relname='aa_t1' or relname='aa_t2' order by relname;
end;

-- temp table
create temp table tmp_aa (id int, num int);
insert into tmp_aa values (1,1),(2,2);
-- tmp_aa no
explain analyze select num, count(*) from tmp_aa group by num;
select relname, reltuples, relpages from pg_class where relname='tmp_aa';

-- subquery not pullup
-- aa_t1 no, aa_t2 no
explain verbose select * from aa_t2 where id in (select aa_t2.id - 1 from aa_t1);
select relname, reltuples, relpages from pg_class where relname='aa_t1' or relname='aa_t2' order by relname;


-- Testset 3 explain test
drop table aa_t1;
drop table aa_t2;
create table aa_t1 (id int, num int);
insert into aa_t1 values (1,1),(2,2);
create table aa_t2 (id int, num int);
insert into aa_t2 values (1,1),(2,2);

-- aa_t3 yes but not show
drop table aa_t3;
create table aa_t3 (id int, num int);
insert into aa_t3 values (1,1),(2,2);
explain select num, count(*) from aa_t3 group by num;
select relname, reltuples, relpages from pg_class where relname='aa_t3';

drop table aa_t3;
create table aa_t3 (id int, num int);
insert into aa_t3 values (1,1),(2,2);
-- aa_t3 yes but not show
set explain_perf_mode = normal;
explain analyze select num, count(*) from aa_t3 group by num;
select relname, reltuples, relpages from pg_class where relname='aa_t3';
set explain_perf_mode = summary;

-- error
explain analyze cursor cursor_1 for select num, count(*) from aa_t1 group by num;
-- aa_t1 yes
explain analyze select num, count(*) from aa_t1 group by num;
select relname, reltuples, relpages from pg_class where relname='aa_t1';

-- error
explain analyze cursor cursor_1 for select num, count(*) from aa_t1 group by num;
-- aa_t1 no
explain analyze select num, count(*) from aa_t1 group by num;

-- error
explain analyze cursor cursor_1 for select num, count(*) from aa_t1 group by num;
set explain_perf_mode=normal;
-- aa_t2 yes
explain analyze select * from aa_t1, aa_t2 where aa_t1.id = aa_t2.id;
select relname, reltuples, relpages from pg_class where relname='aa_t1' or relname='aa_t2' order by relname;
set explain_perf_mode=summary;

-- error
explain analyze cursor cursor_1 for select num, count(*) from aa_t1 group by num;
-- no
explain analyze select * from aa_t1, aa_t2 where aa_t1.id = aa_t2.id;

-- clean
drop table if exists aa_t1;
drop table if exists aa_t2;
drop table if exists aa_t3;
