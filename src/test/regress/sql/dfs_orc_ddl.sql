-- 1. data object except from hdfs table

-- 1.1 tablespace
start transaction;
create tablespace hdfs_ts1 location '/home/zhangyue/mpphome/hadoop_data' with(filesystem='hdfs', address='10.185.178.241:25000,10.185.178.239:25000', cfgpath='/opt/config', storepath='/user/zhangyue/28_zy_mppdb.db');
rollback;

start transaction;
drop tablespace hdfs_ts1;
rollback;

-- 1.2 database
start transaction;
create database db1;
rollback;

create database xact_db;
start transaction;
drop database xact_db;
rollback;
drop database if exists xact_db;

-- 1.3 schmea
start transaction;
create schema schm1;
commit;

start transaction;
drop schema schm1;
rollback;
drop schema schm1;


-- 2. hdfs table ddl in function or procedure

-- 2.1 CREATE TABLE in function
create or replace function test_func_create(i integer) return integer
as
begin
    create table t3(id int) tablespace hdfs_ts;
    return i+1;
end;
/
select test_func_create(1);
drop function if exists test_func_create;

-- 2.2 DROP TABLE in function
drop table if exists t5;
create table t5(id int) tablespace hdfs_ts;
create or replace function test_func_drop(i integer) return integer
as
begin
    drop table t5;
    return i+1;
end;
/
select test_func_drop(1);
drop table if exists t5;
drop function if exists test_func_drop;

-- 2.3 CREATE TABLE in procedure
create or replace procedure test_proc_create(i in integer)
as
begin
    create table t11(id int) tablespace hdfs_ts;
end;
/
select test_proc_create(1);
drop procedure if exists test_proc_create;

-- 2.4 DROP TABLE in procedure
drop table if exists t13;
create table t13(id int) tablespace hdfs_ts;
create or replace procedure test_proc_drop(i in integer)
as
begin
    drop table t13;
end;
/
select test_proc_drop(1);
drop table if exists t13;
drop procedure if exists test_proc_drop;

-- 3. hdfs table ddl in transaction block

-- 3.1 CREATE TABLE in transaction block
start transaction;
create table t21(id int) tablespace hdfs_ts;
abort;

-- 3.2 CREATE TABLE AS in transaction block
create table tas(id int);
start transaction;
create table t21 tablespace hdfs_ts as select * from tas;
abort;
drop table tas;


-- 3.3 DROP TABLE in transaction block
drop table if exists t23;
create table t23(id int) tablespace hdfs_ts;
start transaction;
drop table t23;
abort;
drop table if exists t23;

-- 4. normal operation

set cstore_insert_mode=main;

-- 4.1 create table, drop table
drop table if exists t1;
create table t1(id int) tablespace hdfs_ts;
\d t1
drop table t1;
\d t1

-- 4.2 create table, insert data, drop table
drop table if exists t1;
create table t1(id int) tablespace hdfs_ts;
\d t1
insert into t1 values (1);
drop table t1;
\d t1

-- 4.3 drop multiple tables
drop table if exists rowt;
drop table if exists colt;
drop table if exists hdfst;
create table rowt(id int);
create table colt(id int) with (orientation=column);
create table hdfst(id int) tablespace hdfs_ts;
\d rowt
\d colt
\d hdfst
drop table rowt, colt, hdfst;
\d rowt
\d colt
\d hdfst

-- 4.4 drop multiple tables with data
drop table if exists rowt;
create table rowt(id int);
insert into rowt values (1);

drop table if exists colt;
create table colt(id int) with (orientation=column);
insert into colt values (2);

drop table if exists hdfst;
create table hdfst(id int) tablespace hdfs_ts;
insert into hdfst values (3);
\d rowt
\d colt
\d hdfst
drop table rowt, colt, hdfst;
\d rowt
\d colt
\d hdfst

-- 4.5 drop multiple hdfs tables with data
drop table if exists hdfst1;
create table hdfst1(id int) tablespace hdfs_ts;
insert into hdfst1 values (1);

drop table if exists hdfst2;
create table hdfst2(id int) tablespace hdfs_ts;
insert into hdfst2 values (1);

\d hdfst1
\d hdfst2
drop table hdfst1, hdfst2;
\d hdfst1
\d hdfst2

-- 4.6 drop schema which contains multiple hdfs tables
create schema hdfs_table_schema;
set current_schema=hdfs_table_schema;
create table st1(id int) tablespace hdfs_ts;
create table st2(id int) tablespace hdfs_ts;
\d
reset current_schema;
drop schema hdfs_table_schema cascade;

-- 4.7 drop hdfs table which contians view
create table t1(di int, val int) tablespace hdfs_ts;
create view tview as select val from t1;
drop table t1 cascade;

-- 4.8 in transaction block, drop schema which contains multiple hdfs tables
create schema hdfs_table_schema;
set current_schema=hdfs_table_schema;
create table st1(id int) tablespace hdfs_ts;
create table st2(id int) tablespace hdfs_ts;
reset current_schema;

start transaction;
drop schema hdfs_table_schema cascade;
abort;

drop schema hdfs_table_schema cascade;

