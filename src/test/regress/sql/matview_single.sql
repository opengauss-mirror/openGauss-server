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

-- mlog基础语法
create table mview_log_t1(c1 int, c2 varchar(10));
create materialized view log on mview_log_t1;
drop materialized view log on mview_log_t1;
-- 复用手动创建的物化视图日志
create materialized view log on mview_log_t1;
create incremental materialized view v1 as select c1, c2 from mview_log_t1 where c1 > 1;
create incremental materialized view v2 as select c1 from mview_log_t1;
insert into mview_log_t1 values(1,'1');
insert into mview_log_t1 values(2,'2');
update mview_log_t1 set c1 = 3, c2 = '3' where c1 = 1;
delete from mview_log_t1 where c1 = 2;
refresh incremental materialized view v1;
refresh incremental materialized view v2;
select * from v1;
select * from v2;
drop materialized view v1;
drop materialized view v2; -- 物化视图日志一起被删除
-- 重复创建删除物化视图日志
create incremental materialized view v1 as select c1, c2 from mview_log_t1 where c1 > 1;
create incremental materialized view v2 as select c1 from mview_log_t1;
insert into mview_log_t1 values(1,'1');
drop materialized view log on mview_log_t1;  -- 自动创建的物化视图日志也可手动删除
refresh incremental materialized view v1; -- 缺少物化视图日志，增量刷新报错
refresh materialized view v1;             -- 缺少物化视图日志，全量刷新成功，不会自动创建物化视图日志。
create materialized view log on mview_log_t1;
create materialized view log on mview_log_t1; -- 物化视图日志重复，创建报错。
drop materialized view log on mview_log_t1;
drop materialized view log on mview_log_t1; -- 物化视图日志不存在，删除报错。
create materialized view log on mview_log_t1;
refresh incremental materialized view v1; -- 物化视图日志创建时间晚于最近一次刷新时间，增量刷新报错
refresh materialized view v1;
refresh materialized view v2;
insert into mview_log_t1 values(2,'2');
refresh incremental materialized view v1; -- 全量刷新后，增量刷新成功
refresh incremental materialized view v2;
select * from v1;
select * from v2;
-- 删除所有对象
drop materialized view log on mview_log_t1;
drop materialized view v1; -- 先删除物化视图日志，再删除物化视图
drop materialized view v2;
drop table mview_log_t1;

-- 延迟刷新基础功能
create table defer_t1(c1 int);
insert into defer_t1 values(1);
create materialized view v1 build deferred as select * from defer_t1;
create materialized view v2 build immediate as select * from defer_t1;
select * from v1; -- 无数据
select * from v2; -- 有数据
refresh materialized view v1;
select * from v1; -- 有数据
-- 同时放开with no data，但不支持一起指定
create incremental materialized view v3 as select * from defer_t1 with no data;
create incremental materialized view v4 as select * from defer_t1 with data;
create materialized view v5 as select * from defer_t1 with no data;
create materialized view v6 as select * from defer_t1 with data;
create materialized view v7 build deferred as select * from defer_t1 with no data; -- 语法不支持
select * from v3; -- 无数据
select * from v4; -- 有数据
select * from v5; -- 无数据
select * from v6; -- 有数据
refresh incremental materialized view v3; -- 报错，第一次需全量刷新
refresh materialized view v3;
refresh materialized view v5;
select * from v3; -- 有数据
select * from v5; -- 有数据
-- 结合手动创建删除mlog
drop materialized view log on defer_t1;
create materialized view log on defer_t1;
insert into defer_t1 values(2);
create incremental materialized view v8 as select * from defer_t1 with no data;
select * from v8; -- 无数据
refresh materialized view v8;
select * from v8; -- 有数据
drop table defer_t1 cascade;

-- 多张表的情况
create table cov_t1(c1 int);
create table cov_t2(c1 int);
create materialized view log on cov_t1;
create materialized view log on cov_t2;
drop materialized view log on cov_t2;
create materialized view log on cov_t2;
create materialized view log on cov_t2;
drop table cov_t1,cov_t2 cascade;

-- 只有mlog没有incre matview，不维护mlog
create table only_mlog_t1(c1 int);
create materialized view log on only_mlog_t1;
insert into only_mlog_t1 values (1);
create table only_mlog_record(c1 int);
declare
    oid int := (select oid from pg_class where relname = 'only_mlog_t1');
    table_name varchar(20) := 'mlog_' || oid;
    stmt text := 'insert into only_mlog_record select count(*) from ' || table_name;
begin
    execute stmt;
    commit;
END;
/
select * from only_mlog_record;
drop table only_mlog_t1, only_mlog_record;

-- \help
\h create materialized view log
\h drop materialized view log
\h create materialized view

create table imv_ind_t(a int);
create incremental materialized view imv_ind_v as select * from imv_ind_t;
-- failed, drop mlog index
declare
    oid int := (select oid from pg_class where relname = 'imv_ind_t');
    index_name varchar(20) := 'mlog_' || oid || '_index';
    sql_stmt text := 'Drop index ' || index_name;
begin
    execute sql_stmt;
END;
/
-- copy
copy imv_ind_t from stdin;
1
2
3
\.
refresh incremental materialized view imv_ind_v;
select * from imv_ind_v;
drop table imv_ind_t cascade;

-- clear mlog
create table clear_t(c1 int);
create incremental materialized view clear_v as select * from clear_t;
insert into clear_t values(1);
create table clear_mlog_record(c1 int);
declare
    oid int := (select oid from pg_class where relname = 'clear_t');
    table_name varchar(20) := 'mlog_' || oid;
    stmt text := 'insert into clear_mlog_record select count(*) from ' || table_name;
begin
    execute stmt;
    commit;
END;
/
select * from clear_mlog_record;
truncate table clear_mlog_record;

refresh incremental materialized view clear_v;

declare
    oid int := (select oid from pg_class where relname = 'clear_t');
    table_name varchar(20) := 'mlog_' || oid;
    stmt text := 'insert into clear_mlog_record select count(*) from ' || table_name;
begin
    execute stmt;
    commit;
END;
/
select * from clear_mlog_record;
drop materialized view clear_v;
drop table clear_t, clear_mlog_record;
