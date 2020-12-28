set enable_global_stats = true;

create schema dfs_redis_schema;
set current_schema = 'dfs_redis_schema';
create table dfs_redis(a int, b int) tablespace hdfs_ts;
create table temp (a int, b int);
\d+ temp
insert into temp values(generate_series(1,100),generate_series(1,100));

drop table if exists temp_dfs;
create table temp_dfs (like dfs_redis  INCLUDING STORAGE INCLUDING RELOPTIONS INCLUDING DISTRIBUTION INCLUDING CONSTRAINTS)  tablespace hdfs_ts;
\d+ temp_dfs
----test commit in cstore_insert_mode=delta state
set cstore_insert_mode=delta;
insert into dfs_redis select * from temp;
set cstore_insert_mode=main;
insert into dfs_redis select * from temp;
select count (*) from dfs_redis;

start transaction;
set cstore_insert_mode=delta;
insert into temp_dfs select * from dfs_redis;
select count (*) from dfs_redis;
select count(*) from temp_dfs;

SELECT pg_catalog.gs_switch_relfilenode('dfs_redis','temp_dfs');
select count(*) from dfs_redis;
commit;


select count(*) from dfs_redis;
----test commit in cstore_insert_mode=main state
drop table if exists dfs_redis;
create table dfs_redis(a int, b int) tablespace hdfs_ts;
set cstore_insert_mode=delta;
insert into dfs_redis select * from temp;
set cstore_insert_mode=main;
insert into dfs_redis select * from temp;
drop table temp_dfs;
create table temp_dfs (like dfs_redis  INCLUDING STORAGE INCLUDING RELOPTIONS INCLUDING DISTRIBUTION INCLUDING CONSTRAINTS)  tablespace hdfs_ts;
select count (*) from dfs_redis;

start transaction;
set cstore_insert_mode=main;
insert into temp_dfs select * from dfs_redis;
select count (*) from dfs_redis;

SELECT pg_catalog.gs_switch_relfilenode('dfs_redis','temp_dfs');
select count(*) from dfs_redis;
commit;


select count(*) from dfs_redis;

----test abort in cstore_insert_mode=delta state
drop table if exists dfs_redis;
create table dfs_redis(a int, b int) tablespace hdfs_ts;
set cstore_insert_mode=delta;
insert into dfs_redis select * from temp;
set cstore_insert_mode=main;
insert into dfs_redis select * from temp;
drop table temp_dfs;
create table temp_dfs (like dfs_redis  INCLUDING STORAGE INCLUDING RELOPTIONS INCLUDING DISTRIBUTION INCLUDING CONSTRAINTS)  tablespace hdfs_ts;
select count (*) from dfs_redis;

start transaction;
set cstore_insert_mode=delta;
insert into temp_dfs select * from dfs_redis;
select count (*) from dfs_redis;
select count(*) from temp_dfs;

SELECT pg_catalog.gs_switch_relfilenode('dfs_redis','temp_dfs');
select count(*) from dfs_redis;
rollback;


select count(*) from dfs_redis;
----test abort in cstore_insert_mode=main state
drop table if exists dfs_redis;
create table dfs_redis(a int, b int) tablespace hdfs_ts;
set cstore_insert_mode=delta;
insert into dfs_redis select * from temp;
set cstore_insert_mode=main;
insert into dfs_redis select * from temp;
drop table temp_dfs;
create table temp_dfs (like dfs_redis  INCLUDING STORAGE INCLUDING RELOPTIONS INCLUDING DISTRIBUTION INCLUDING CONSTRAINTS)  tablespace hdfs_ts;
select count (*) from dfs_redis;

start transaction;
set cstore_insert_mode=main;
insert into temp_dfs select * from dfs_redis;
select count (*) from dfs_redis;

SELECT pg_catalog.gs_switch_relfilenode('dfs_redis','temp_dfs');
select count(*) from dfs_redis;
rollback;

select count(*) from dfs_redis;

-------------------------------------------------------------------------test redistribute HDFS partition table
drop table if exists dfs_redis;
create table dfs_redis(a int, b int) tablespace hdfs_ts partition by values (b);

drop table if exists temp_dfs;
create table temp_dfs(a int, b int) tablespace hdfs_ts partition by values (b);
\d+ temp_dfs
----test commit in cstore_insert_mode=delta state
set cstore_insert_mode=delta;
insert into dfs_redis select * from temp;
set cstore_insert_mode=main;
insert into dfs_redis select * from temp;
select count (*) from dfs_redis;

start transaction;
set cstore_insert_mode=delta;
insert into temp_dfs select * from dfs_redis;
select count (*) from dfs_redis;
select count(*) from temp_dfs;

SELECT pg_catalog.gs_switch_relfilenode('dfs_redis','temp_dfs');
select count(*) from dfs_redis;
commit;


select count(*) from dfs_redis;
----test commit in cstore_insert_mode=main state
drop table if exists dfs_redis;
create table dfs_redis(a int, b int) tablespace hdfs_ts partition by values (b);
set cstore_insert_mode=delta;
insert into dfs_redis select * from temp;
set cstore_insert_mode=main;
insert into dfs_redis select * from temp;
drop table temp_dfs;
create table temp_dfs(a int, b int) tablespace hdfs_ts partition by values (b);
select count (*) from dfs_redis;

start transaction;
set cstore_insert_mode=main;
insert into temp_dfs select * from dfs_redis;
select count (*) from dfs_redis;

SELECT pg_catalog.gs_switch_relfilenode('dfs_redis','temp_dfs');
select count(*) from dfs_redis;
commit;


select count(*) from dfs_redis;

----test abort in cstore_insert_mode=delta state
drop table if exists dfs_redis;
create table dfs_redis(a int, b int) tablespace hdfs_ts partition by values (b);
set cstore_insert_mode=delta;
insert into dfs_redis select * from temp;
set cstore_insert_mode=main;
insert into dfs_redis select * from temp;
drop table temp_dfs;
create table temp_dfs(a int, b int) tablespace hdfs_ts partition by values (b);
select count (*) from dfs_redis;

start transaction;
set cstore_insert_mode=delta;
insert into temp_dfs select * from dfs_redis;
select count (*) from dfs_redis;
select count(*) from temp_dfs;

SELECT pg_catalog.gs_switch_relfilenode('dfs_redis','temp_dfs');
select count(*) from dfs_redis;
rollback;


select count(*) from dfs_redis;
----test abort in cstore_insert_mode=main state
drop table if exists dfs_redis;
create table dfs_redis(a int, b int) tablespace hdfs_ts partition by values (b);
set cstore_insert_mode=delta;
insert into dfs_redis select * from temp;
set cstore_insert_mode=main;
insert into dfs_redis select * from temp;
drop table temp_dfs;
create table temp_dfs(a int, b int) tablespace hdfs_ts partition by values (b);
select count (*) from dfs_redis;

start transaction;
set cstore_insert_mode=main;
insert into temp_dfs select * from dfs_redis;
select count (*) from dfs_redis;

SELECT pg_catalog.gs_switch_relfilenode('dfs_redis','temp_dfs');
select count(*) from dfs_redis;
rollback;

select count(*) from dfs_redis;
drop schema dfs_redis_schema cascade;