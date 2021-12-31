create database pl_test_llt DBCOMPATIBILITY 'pg';
\c pl_test_llt
--int1
create table source(a int);
insert into source values(1);
drop table t1;
drop table t2;
create table t1(a int1, b int1);
create table t2(a int1, b int1);
insert into t1 select generate_series(1, 127), generate_series(1, 127) from source;
insert into t2 select generate_series(1, 127), generate_series(1, 127) from source;
analyze t1;
analyze t2;
explain (costs off) select * from t1, t2 where t1.a = t2.b;
select * from t1, t2 where t1.a = t2.b order by 1, 2, 3, 4;

drop table t1;
drop table t2;

--oid
create table t1(a oid, b oid);
create table t2(a oid, b oid);
insert into t1 select generate_series(1, 127), generate_series(1, 127) from source;
insert into t2 select generate_series(1, 127), generate_series(1, 127) from source;
analyze t1;
analyze t2;
explain (costs off) select * from t1, t2 where t1.a = t2.b;
select * from t1, t2 where t1.a = t2.b order by 1, 2, 3, 4;

drop table t1;
drop table t2;

--bool
create table t1(a bool, b bool);
create table t2(a bool, b bool);
explain (costs off) select * from t1, t2 where t1.a = t2.b;
select * from t1, t2 where t1.a = t2.b order by 1, 2, 3, 4;

drop table t1;
drop table t2;

--char
create table t1(a "char", b "char");
create table t2(a "char", b "char");
insert into t1 select generate_series(1, 127)::char,generate_series(1, 127)::char from source;
insert into t2 select generate_series(1, 127)::char,generate_series(1, 127)::char from source;
analyze t1;
analyze t2;
explain (costs off) select * from t1, t2 where t1.a = t2.b;
select * from t1, t2 where t1.a = t2.b order by 1, 2, 3, 4;

drop table t1;
drop table t2;

--name
create table t1(a name, b name);
create table t2(a name, b name);
explain (costs off) select * from t1, t2 where t1.a = t2.b order by 1, 2, 3, 4;
select * from t1, t2 where t1.a = t2.b order by 1, 2, 3, 4;

drop table t1;
drop table t2;

--int2vector
create table t1(a int2vector, b int2vector);
create table t2(a int2vector, b int2vector);
insert into t1 select * from t2;

drop table t1;
drop table t2;

--NVARCHAR2
create table t1(a NVARCHAR2, b NVARCHAR2);
create table t2(a NVARCHAR2, b NVARCHAR2);
insert into t1 select * from t2;

drop table t1;
drop table t2;

--OIDVECTOR
create table t1(a OIDVECTOR, b OIDVECTOR);
create table t2(a OIDVECTOR, b OIDVECTOR);
insert into t1 select * from t2;
drop table t1;
drop table t2;

--FLOAT4
create table t1(a FLOAT4, b FLOAT4);
create table t2(a FLOAT4, b FLOAT4);
insert into t1 select * from t2;
drop table t1;
drop table t2;
--ABSTIME
create table t1(a ABSTIME, b ABSTIME);
create table t2(a ABSTIME, b ABSTIME);
insert into t1 select * from t2;
drop table t1;
drop table t2;

--RELTIME
create table t1(a RELTIME, b RELTIME);
create table t2(a RELTIME, b RELTIME);
insert into t1 select * from t2;
drop table t1;
drop table t2;

--CASH
create table t1(a MONEY, b MONEY);
create table t2(a MONEY, b MONEY);
insert into t1 select * from t2;
drop table t1;
drop table t2;

--BYTEA
create table t1(a BYTEA, b BYTEA);
create table t2(a BYTEA, b BYTEA);
insert into t1 select * from t2;
drop table t1;
drop table t2;

--DATE
create table t1(a DATE, b DATE);
create table t2(a DATE, b DATE);
insert into t1 select * from t2;
drop table t1;
drop table t2;

--INTERVAL
create table t1(a INTERVAL, b INTERVAL);
create table t2(a INTERVAL, b INTERVAL);
insert into t1 select * from t2;
drop table t1;
drop table t2;

--TIMETZ
create table t1(a TIMETZ, b TIMETZ);
create table t2(a TIMETZ, b TIMETZ);
insert into t1 select * from t2;
drop table t1;
drop table t2;

-- SMALLDATETIME
create table t1(a SMALLDATETIME, b SMALLDATETIME);
create table t2(a SMALLDATETIME, b SMALLDATETIME);
insert into t1 select * from t2;
drop table t1;
drop table t2;


--check passwd
create user testtest password 'tsettset';
create user test_llt password 'Ttest@123';
alter user test_llt with password '321@tsetT';

create table t1(a int);

create function test(t1) returns void
as $$
begin
return;
end $$ language plpgsql;

declare
a t1;
begin
perform test(a);
end;
/
explain (costs off) select * from t1 where not exists (select a from t2);
select * from t1 where not exists (select a from t2);

explain (costs off) select * from t1 where not a = any (select a from t2);
select * from t1 where not a = any (select a from t2);


create table test_range_datatype_int2(a int2)
partition by range(a)
(
partition test_range_datatype_int2_p1 values less than (1),
partition test_range_datatype_int2_p2 values less than (2)
);

vacuum full test_range_datatype_int2;

set enable_kill_query=on;
create user test_llt_cancel password 'Ttest@123';
set current_schema = test_llt;
create table test(a int);
set current_schema = public;
drop user test_llt_cancel cascade;


--bytealt
create table test_bytea(a bytea, b bytea);
insert into test_bytea values('abc', 'abcd');
select * from test_bytea where a < b;

--rawne
create table test_raw(a raw, b raw);
insert into test_raw values('abc', 'abc'), ('abc', 'abcde');

select * from test_raw where a != b;

--rawtotext
select rawtohex(a) from test_raw;

--DCH_from_char
select to_timestamp('20120930 09:30 pm', 'yyyymmdd hh:MI pm');
select to_timestamp('20120930 09:30 p.m.', 'yyyymmdd hh:MI p.m.');
select to_timestamp('20120930 09:30 111', 'yyyymmdd hh:MI MS');
select to_timestamp('20120930 09:30 111', 'yyyymmdd hh:MI US');
select to_timestamp('20120930 09:30 11111', 'yyyymmdd hh:MI SSSSS');
select to_timestamp('20120930 09:30 11111 TZ', 'yyyymmdd hh:MI SSSSS TZ');

select to_timestamp('20120930 09:30 111 B.C.', 'yyyymmdd hh:MI US B.C.');
select to_timestamp('20120930 09:30 111 BC', 'yyyymmdd hh:MI US BC');
select to_timestamp('20120930 09:30 111 fri', 'yyyymmdd hh:MI US dy');
select to_timestamp('20120930 09:30 111 fri 3', 'yyyymmdd hh:MI US dy Q');

select to_timestamp('2012I30 09:30 111 fri 3', 'yyyyRMdd hh:MI US dy Q');
select to_timestamp('20120930 09:30 5 1234', 'yyyymmdd hh:MI W J');
select to_timestamp('20120930 09:30 pm', 'yyyymmdd hh:MI pm FF');

select to_timestamp('20120930 09:30 5555', 'yyyymmdd hh:MI RRRR');
select to_timestamp('20120930 09:30 55', 'yyyymmdd hh:MI RR');
select to_timestamp('20120930 09:30 55', 'yyyymmdd hh:MI FF');

--create role
create role llt_1 password 'Ttest@123' auditadmin;
create role llt_2 password 'Ttest@123' auditadmin auditadmin;

create role llt_3 password 'Ttest@123' sysadmin sysadmin;

create role llt_3 password 'Ttest@123' default tablespace abc;
create role llt_4 password 'Ttest@123' default tablespace abc default tablespace abc;

create role llt_5 password 'Ttest@123' profile default;

--alter role
alter role llt_5 auditadmin auditadmin;
alter role llt_5 sysadmin sysadmin;

--pg_test_err_contain_err
select pg_test_err_contain_err(1);
select pg_test_err_contain_err(2);
select pg_test_err_contain_err(3);
select pg_test_err_contain_err(4);
select pg_test_err_contain_err(5);


--DCH_check
select to_timestamp('20130230','yyyyMMDD');
select to_timestamp('0230','MMDD');
select to_timestamp('2014366','yyyyddd');
select to_timestamp('13','HH12');
select to_timestamp('13000','sssss');
select to_timestamp('monday','DAY');
select to_timestamp('monday','Day');
select to_timestamp('monday','day');
select to_timestamp('mon','DY');

select to_timestamp('july','MONTH');
select to_timestamp('july','month');
select to_timestamp('feb','mon');
select to_timestamp('I','rm');

select to_date(19980101100000);


create schema alter_llt1;
create user alter_llt2 password 'Ttest@123';

create table alter_llt1.t1(f1 serial primary key, f2 int check (f2 > 0));
create view alter_llt1.v1 as select * from alter_llt1.t1;

create function alter_llt1.plus1(int) returns int as 'select $1+1' language sql; 

--create domain alter_llt1.posint integer check (value > 0);

create type alter_llt1.ctype as (f1 int, f2 text);

create function alter_llt1.same(alter_llt1.ctype, alter_llt1.ctype) returns boolean language sql
as 'select $1.f1 is not distinct from $2.f1 and $1.f2 is not distinct from $2.f2';

create operator alter_llt2.> (procedure = alter_llt1.same, leftarg  = alter_llt1.ctype, rightarg = alter_llt1.ctype);

--do_to_timestamp
select to_timestamp('20130101a','YYYYMMdd');

SELECT TO_TIMESTAMP('2014', 'YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY');
SELECT TO_TIMESTAMP('2014-15', 'YYYY-HH-YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY');
SELECT TO_TIMESTAMP('12-15', 'SSSSS-SS');
SELECT TO_TIMESTAMP('12-01', 'SSSSS-HH');
SELECT TO_TIMESTAMP('2014-01-02', 'YYYY-DDD-MM');
SELECT TO_TIMESTAMP('2014-01-02', 'YYYY-DDD-DD');
SELECT TO_TIMESTAMP('2014-01-02', 'YYYY12-DDD-DD');
SELECT TO_TIMESTAMP('2014', 'YYYY12YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY');

CREATE TABLE LLT_PART_TEST1(A INT , B INT, C INT)
PARTITION BY RANGE(A)
(
    PARTITION P1 VALUES LESS THAN (1000)
);

SELECT * FROM LLT_PART_TEST1 WHERE A > 10 AND A IS NULL;

INSERT INTO LLT_PART_TEST1 VALUES (GENERATE_SERIES(1, 130), GENERATE_SERIES(1, 130), GENERATE_SERIES(1, 130));
CREATE INDEX IDX_ON_T1 ON LLT_PART_TEST1(A) LOCAL;
VACUUM LLT_PART_TEST1 PARTITION (P1);
CLUSTER LLT_PART_TEST1 USING IDX_ON_T1;

CREATE VIEW VIEW_ON_LLT_PART_TEST1 AS SELECT * FROM LLT_PART_TEST1;
INSERT INTO VIEW_ON_LLT_PART_TEST1 SELECT * FROM LLT_PART_TEST1;

CREATE OR REPLACE FUNCTION RESULT_COUNT()
RETURNS INTEGER
AS $$
DECLARE
  MYINTEGER INTEGER ;
BEGIN
  SELECT COUNT(*) INTO MYINTEGER FROM LLT_PART_TEST1 WHERE A < 1;
  RETURN MYINTEGER;
END;
$$LANGUAGE PLPGSQL;

CALL RESULT_COUNT();

DROP TABLE LLT_PART_TEST1 CASCADE;
DROP FUNCTION RESULT_COUNT();

CREATE TABLE LLT_INT1_AVG_ACCUM(A INT1);
INSERT INTO LLT_INT1_AVG_ACCUM VALUES (GENERATE_SERIES(1, 130));
SELECT AVG(A) FROM LLT_INT1_AVG_ACCUM;
DROP TABLE LLT_INT1_AVG_ACCUM;

select count(*)::bool from pg_stat_get_activity(NULL);

select smalldatetime_smaller('19860111','19860405');
select smalldatetime_smaller('19860405','19860111');

select smalldatetime_larger('19570906','19600907');
select smalldatetime_larger('19600907','19570906');

--blacklist in llt
SECURITY LABEL FOR selinux ON TABLE mytable IS 'system_u:object_r:sepgsql_table_t:s0';
CREATE TYPE bigobj (INPUT = lo_filein, OUTPUT = lo_fileout, INTERNALLENGTH = VARIABLE);
CREATE TYPE float8_range AS RANGE (subtype = float8, subtype_diff = float8mi);
ALTER TYPE colors ADD VALUE 'orange' AFTER 'red';
CREATE COLLATION french (LOCALE = 'fr_FR.utf8');
UNLISTEN virtual;
NOTIFY virtual;
CREATE LANGUAGE plperl;
CREATE OPERATOR FAMILY name USING
ALTER OPERATOR FAMILY integer_ops USING btree ADD
OPERATOR 1 < (int4, int2) ,
OPERATOR 2 <= (int4, int2) ,
FUNCTION 1 btint24cmp(int2, int4) ;
ALTER TEXT SEARCH DICTIONARY my_dict ( StopWords = newrussian ); 

drop table t1 CASCADE;
drop table test_raw cascade;
\c regression;
drop database IF EXISTS pl_test_llt;
