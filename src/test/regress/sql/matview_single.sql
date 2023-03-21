-- prepare
create table test(c1 int);
insert into test values(1);
-- base op
drop Materialized view mvtest_tm;

CREATE MATERIALIZED VIEW mvtest_tm AS select *from test;
select *From mvtest_tm;

insert into test values(1);
REFRESH MATERIALIZED VIEW mvtest_tm;
select *from mvtest_tm;

drop Materialized view mvtest_tm;

-- error
create incremental MATERIALIZED VIEW mvtest_inc AS select *from test;
REFRESH incremental MATERIALIZED VIEW mvtest_tm;

-- test matview with anounymous types
create database test_imv_db with dbcompatibility 'b';
\c test_imv_db

-- case 1: drop type first
create table imv1_t(a set('ad','asf'), c int);
create incremental materialized view imv1_v as select * from imv1_t;
create incremental materialized view imv12_v as select * from imv1_t;
drop type imv1_t_a_set cascade;
select oid, relname from pg_class where relname like 'mlog%';
drop table imv1_t cascade;
select oid, relname from pg_class where relname like 'mlog%';

-- case 2: drop view first
create table imv1_t(a set('ad','asf'), c int);
create incremental materialized view imv1_v as select * from imv1_t;
create incremental materialized view imv12_v as select * from imv1_t;
drop materialized view imv1_v;
select oid, relname from pg_class where relname like 'mlog%';
drop materialized view imv12_v;
select oid, relname from pg_class where relname like 'mlog%';
drop table imv1_t cascade;

-- case 3: drop table directly
create table imv1_t(a set('ad','asf'), c int);
create incremental materialized view imv1_v as select * from imv1_t;
create incremental materialized view imv12_v as select * from imv1_t;
drop table imv1_t cascade;
select oid, relname from pg_class where relname like 'mlog%';

-- case 4: multi columns
create table imv1_t(a set('ad','asf'), b set('b', 'bb'), c int, d set('b', 'bb'));
create incremental materialized view imv1_v as select * from imv1_t;
create incremental materialized view imv12_v as select * from imv1_t;
create incremental materialized view imv13_v as select * from imv1_t;
\d
drop table imv1_t cascade;
select oid, relname from pg_class where relname like 'mlog%';
\d

\c regression
drop database test_imv_db;
