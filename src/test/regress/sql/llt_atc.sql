-- add test case into 'case DCH_RRRR' and 'case DCH_RR' in DCH_from_char function, then the function adjust_partial_year_to_current_year will covered:
select to_date('06-JAN-40 00:00:00','DD-MON-RRRR HH24:MI:SS');
select to_date('01/05/81','dd/mm/rr');

-- add test case for float4_to_char function
select to_char(0.15::float4);

-- add test case for str_toupper function(just part)
select upper('12345' COLLATE "de_DE");

-- add test case for function chr
select chr(65536::int4);
select chr(2048::int4);
select chr(128::int4);

-- add test case for function rtrim1
select rtrim(' ');

-- add test case for function make_ruledef
drop table if exists llt_make_ruledef_t1;
drop table if exists llt_make_ruledef_t2;
create table llt_make_ruledef_t1(q1 int);
create table llt_make_ruledef_t2(q1 int);
CREATE or replace RULE "_RETURN" AS
    ON SELECT TO llt_make_ruledef_t1
    DO INSTEAD
        SELECT * FROM llt_make_ruledef_t2;
select pg_get_ruledef(oid, true) from pg_rewrite where rulename='_RETURN' and ev_class in (select oid from pg_class where relname='llt_make_ruledef_t1');
drop view llt_make_ruledef_t1;
drop table llt_make_ruledef_t2;

-- add test case for function byteane
select 'a'::bytea <> 'b'::bytea;

-- add test case for function byteage
select byteage('a'::bytea,'b'::bytea);

-- add test case for function pg_size_pretty_numeric
select pg_size_pretty(5::numeric);
select pg_size_pretty(10241::numeric);
select pg_size_pretty(3024100::numeric);
select pg_size_pretty(302410000::numeric);
select pg_size_pretty(302410000000::numeric);
select pg_size_pretty(302410000000000::numeric);

-- add test case for function nvarchar2
drop table if exists llt_nvarchar2;
create table llt_nvarchar2 (q nvarchar2(8));
insert into llt_nvarchar2 values('123456789');
drop table llt_nvarchar2;

-- add test case for function bpcharlenb
select lengthb('a'::bpchar);

-- add test case for function pg_get_running_xacts
drop table if exists running_xacts_tbl;
create table running_xacts_tbl(handle int4);
insert into running_xacts_tbl(select handle from pg_get_running_xacts());
drop table running_xacts_tbl;

-- add test case for funtion pg_convert_to_nocase and pg_convert_nocase
select convert_to_nocase('12345', 'GBK');

-- add test case for function pg_stat_get_redo_stat
drop table if exists pg_stat_get_redo_stat_tbl;
create table pg_stat_get_redo_stat_tbl(a int8);
insert into pg_stat_get_redo_stat_tbl select phywrts from pg_stat_get_redo_stat();
drop table pg_stat_get_redo_stat_tbl;

-- add test case for function SplitDatestrWithoutSeparator
select to_date('20140502 124330');

-- add test case for function pgaudit_audit_object and pgaudit_ProcessUtility
create schema pgaudit_audit_object;
alter schema pgaudit_audit_object rename to pgaudit_audit_object_1;
drop schema pgaudit_audit_object_1;
create role davide WITH PASSWORD 'jw8s0F411_1';
ALTER ROLE davide SET maintenance_work_mem = 100000;
alter role davide rename to davide1;
drop role davide1;
create table pgaudit_audit_object (a int, b int);
CREATE VIEW vista AS SELECT * from pgaudit_audit_object;
alter view vista rename to vista1;
drop view vista1;
drop table pgaudit_audit_object;
CREATE DATABASE lusiadas;
alter database lusiadas rename to lusiadas1;
drop database lusiadas1;
