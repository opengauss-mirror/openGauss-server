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

create table test_syn(id int unique,a1 varchar(20));
create materialized view mv_test_syn as select * from test_syn;
create incremental materialized view imv_test_syn as select * from test_syn;
create synonym s_mv_test_syn for mv_test_syn;
create synonym s_imv_test_syn for imv_test_syn;

REFRESH MATERIALIZED VIEW s_mv_test_syn;
REFRESH MATERIALIZED VIEW s_imv_test_syn;
REFRESH INCREMENTAL MATERIALIZED VIEW s_mv_test_syn;
REFRESH INCREMENTAL MATERIALIZED VIEW s_imv_test_syn;

drop synonym s_mv_test_syn;
drop synonym s_imv_test_syn;
drop table test_syn cascade;

-- case 5: drop mlog table.
create table imv1_t(a int);
insert into imv1_t values(1);
create incremental materialized view imv1_v as select * from imv1_t;

declare
    oid int := (select oid from pg_class where relname = 'imv1_t');
    table_name varchar(20) := 'mlog_' || oid;
    sql_stmt text := 'Drop table ' || table_name;
begin
    execute sql_stmt;
END;
/

-- case 6: drop table that looks like a mlog with valid oid.
drop materialized view imv1_v;
declare
    oid int := (select oid from pg_class where relname = 'imv1_t');
    table_name varchar(20) := 'mlog_' || oid;
    create_stmt text := 'Create table ' || table_name || '(a int)' ;
    drop_stmt text := 'Drop table ' || table_name;
begin
    execute create_stmt;
    execute drop_stmt;
END;
/

-- case 7: drop table that looks like a mlog without valid oid.
create table mlog_99999(a int);
drop table mlog_99999;

drop table imv1_t cascade;

-- test about the privileges of refresh
create table t (id int);
insert into t select generate_series(1,10);
create materialized view mv_t as select * from t;
create user testuser with password 'Gauss@123';
grant usage on schema public to testuser;
grant select on t to testuser;
grant select on mv_t to testuser;

set role testuser password 'Gauss@123';
-- failed, permission denied
refresh materialized view mv_t;
reset role;
grant delete,insert on mv_t to testuser;
set role testuser password 'Gauss@123';
-- failed, permission denied
refresh materialized view mv_t;
reset role;
grant index on mv_t to testuser;
set role testuser password 'Gauss@123';
-- failed, permission denied
refresh materialized view mv_t;

reset role;
alter table mv_t owner to testuser;
set role testuser password 'Gauss@123';
-- success
refresh materialized view mv_t;

reset role;
drop user testuser cascade;

\c regression
drop database test_imv_db;
