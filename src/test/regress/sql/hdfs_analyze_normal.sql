set enable_global_stats = true;
create schema analyze_schema;
set current_schema=analyze_schema;

--set enable_global_stats=off;

---test data store on delta
drop table if exists dfs_tbl;
create table dfs_tbl(a int, b int) tablespace hdfs_ts;
set cstore_insert_mode=delta;
drop table if exists temp;
create table temp (a int, b int);
insert into temp values(generate_series(1,100),generate_series(1,100));
insert into dfs_tbl select * from temp;

select relname , relpages, reltuples from pg_class where relname='dfs_tbl';
select relname , relpages, reltuples from pg_class where oid in ( select reldeltarelid from pg_class where relname='dfs_tbl');
select staattnum, stainherit from pg_statistic where starelid in (select oid from pg_class where relname='dfs_tbl') order by 1, 2;
select staattnum, stainherit from pg_statistic where starelid in (select reldeltarelid from pg_class where relname='dfs_tbl') order by 1, 2;
--analyze
analyze dfs_tbl;

execute direct  on (datanode1) 'select count(*) from dfs_tbl';
select relname , relpages, reltuples from pg_class where relname='dfs_tbl';
select relname , relpages, reltuples from pg_class where oid in ( select reldeltarelid from pg_class where relname='dfs_tbl');
select staattnum, stainherit from pg_statistic where starelid in (select oid from pg_class where relname='dfs_tbl') order by 1, 2;
select staattnum, stainherit from pg_statistic where starelid in (select reldeltarelid from pg_class where relname='dfs_tbl') order by 1, 2;

--test for analyze sample table on Delta
set default_statistics_target=-2;
analyze dfs_tbl;
select relname , relpages, reltuples from pg_class where relname='dfs_tbl';
select relname , relpages, reltuples from pg_class where oid in ( select reldeltarelid from pg_class where relname='dfs_tbl');
select staattnum, stainherit from pg_statistic where starelid in (select oid from pg_class where relname='dfs_tbl') order by 1, 2;
select staattnum, stainherit from pg_statistic where starelid in (select reldeltarelid from pg_class where relname='dfs_tbl') order by 1, 2;
reset default_statistics_target;

---test data store on HDFS
drop table if exists dfs_tbl;
create table dfs_tbl(a int, b int) tablespace hdfs_ts;
set cstore_insert_mode=main;
drop table if exists temp;
create table temp (a int, b int);
insert into temp values(generate_series(1,100),generate_series(1,100));
insert into dfs_tbl select * from temp;

select relname , relpages, reltuples from pg_class where relname='dfs_tbl';
select relname , relpages, reltuples from pg_class where oid in ( select reldeltarelid from pg_class where relname='dfs_tbl');
select staattnum, stainherit from pg_statistic where starelid in (select oid from pg_class where relname='dfs_tbl') order by 1, 2;
select staattnum, stainherit from pg_statistic where starelid in (select reldeltarelid from pg_class where relname='dfs_tbl') order by 1, 2;
--analyze
analyze dfs_tbl;
execute direct  on (datanode1) 'select count(*) from dfs_tbl';
select relname , relpages, reltuples from pg_class where relname='dfs_tbl';
select relname , relpages, reltuples from pg_class where oid in ( select reldeltarelid from pg_class where relname='dfs_tbl');
select staattnum, stainherit from pg_statistic where starelid in (select oid from pg_class where relname='dfs_tbl') order by 1, 2;
select staattnum, stainherit from pg_statistic where starelid in (select reldeltarelid from pg_class where relname='dfs_tbl') order by 1, 2;
--test for analyze sample table on HDFS
set default_statistics_target=-2;
analyze dfs_tbl;
select relname , relpages, reltuples from pg_class where relname='dfs_tbl';
select relname , relpages, reltuples from pg_class where oid in ( select reldeltarelid from pg_class where relname='dfs_tbl');
select staattnum, stainherit from pg_statistic where starelid in (select oid from pg_class where relname='dfs_tbl') order by 1, 2;
select staattnum, stainherit from pg_statistic where starelid in (select reldeltarelid from pg_class where relname='dfs_tbl') order by 1, 2;
reset default_statistics_target;

--test data store on HDFS and Delta
drop table if exists dfs_tbl;
create table dfs_tbl(a int, b int) tablespace hdfs_ts;
create table dfs_tbl_rep(a int, b int) tablespace hdfs_ts distribute by replication;
set cstore_insert_mode=delta;
drop table if exists temp;
create table temp (a int, b int);
insert into temp values(generate_series(1,100),generate_series(1,100));
insert into dfs_tbl select * from temp;
insert into dfs_tbl_rep select * from temp;

set cstore_insert_mode=main;
drop table if exists temp;
create table temp (a int, b int);
insert into temp values(generate_series(1,100),generate_series(1,100));
insert into dfs_tbl select * from temp;
insert into dfs_tbl_rep select * from temp;

analyze dfs_tbl_rep;
select count(*) from pg_stats where tablename='dfs_tbl_rep';
select count(*) from pg_stats where tablename in (select b.relname from pg_class a, pg_class b where a.relname='dfs_tbl_rep'  and b.oid=a.reldeltarelid);
vacuum deltamerge dfs_tbl_rep;
analyze dfs_tbl_rep;
select count(*) from pg_stats where tablename='dfs_tbl_rep';
select count(*) from pg_stats where tablename in (select b.relname from pg_class a, pg_class b where a.relname='dfs_tbl_rep'  and b.oid=a.reldeltarelid);

truncate table dfs_tbl_rep;
analyze dfs_tbl_rep;
set cstore_insert_mode=delta;
insert into dfs_tbl_rep select * from temp;
vacuum deltamerge dfs_tbl_rep;
analyze dfs_tbl_rep;
select count(*) from pg_stats where tablename='dfs_tbl_rep';
select count(*) from pg_stats where tablename in (select b.relname from pg_class a, pg_class b where a.relname='dfs_tbl_rep'  and b.oid=a.reldeltarelid);
drop table dfs_tbl_rep;

execute direct  on (datanode1) 'select count(*) from dfs_tbl';
execute direct  on (datanode1) 'select count(*) from cstore.pg_delta_dfs_tbl';
select relname , relpages, reltuples from pg_class where relname='dfs_tbl';
select relname , relpages, reltuples from pg_class where oid in ( select reldeltarelid from pg_class where relname='dfs_tbl');
select staattnum, stainherit from pg_statistic where starelid in (select oid from pg_class where relname='dfs_tbl') order by 1, 2;
select staattnum, stainherit from pg_statistic where starelid in (select reldeltarelid from pg_class where relname='dfs_tbl') order by 1, 2;
--analyze
analyze dfs_tbl;
execute direct  on (datanode1) 'select count(*) from dfs_tbl';
select relname , relpages, reltuples from pg_class where relname='dfs_tbl';
select relname , relpages, reltuples from pg_class where oid in ( select reldeltarelid from pg_class where relname='dfs_tbl');
select staattnum, stainherit from pg_statistic where starelid in (select oid from pg_class where relname='dfs_tbl') order by 1, 2;
select staattnum, stainherit from pg_statistic where starelid in (select reldeltarelid from pg_class where relname='dfs_tbl') order by 1, 2;
--test for analyze sample table on HDFS and Delta
set default_statistics_target=-2;
analyze dfs_tbl;
select relname , relpages, reltuples from pg_class where relname='dfs_tbl';
select relname , relpages, reltuples from pg_class where oid in ( select reldeltarelid from pg_class where relname='dfs_tbl');
select staattnum, stainherit from pg_statistic where starelid in (select oid from pg_class where relname='dfs_tbl') order by 1, 2;
select staattnum, stainherit from pg_statistic where starelid in (select reldeltarelid from pg_class where relname='dfs_tbl') order by 1, 2;
reset default_statistics_target;

--test data store on HDFS and Delta, the ratio of HDFS's row count with complex table less than 0.01, using complex's stats as Delta's stats
drop table if exists dfs_tbl;
create table dfs_tbl(a int, b int) tablespace hdfs_ts;
set cstore_insert_mode=delta;
drop table if exists temp;
create table temp (a int, b int);
insert into temp values(generate_series(1,300),generate_series(1,300));
insert into dfs_tbl select * from temp;

set cstore_insert_mode=main;
drop table if exists temp;
create table temp (a int, b int);
insert into temp values(1,2),(3,4);
insert into dfs_tbl select * from temp;

set default_statistics_target=-2;
analyze dfs_tbl;
select count(*) from dfs_tbl;
select count(*) from cstore.pg_delta_dfs_tbl;
select relname , relpages, reltuples from pg_class where relname='dfs_tbl';
select relname , relpages, reltuples from pg_class where oid in ( select reldeltarelid from pg_class where relname='dfs_tbl');
select staattnum, stainherit from pg_statistic where starelid in (select oid from pg_class where relname='dfs_tbl') order by 1, 2;
select staattnum, stainherit from pg_statistic where starelid in (select reldeltarelid from pg_class where relname='dfs_tbl') order by 1, 2;
reset default_statistics_target;

--test data store on HDFS and Delta, the ratio of Delta's row count with complex table less than 0.01, using complex's stats as HDFS's stats
drop table if exists dfs_tbl;
create table dfs_tbl(a int, b int) tablespace hdfs_ts;
set cstore_insert_mode=main;
drop table if exists temp;
create table temp (a int, b int);
insert into temp values(generate_series(1,300),generate_series(1,300));
insert into dfs_tbl select * from temp;

set cstore_insert_mode=delta;
drop table if exists temp;
create table temp (a int, b int);
insert into temp values(1,2),(3,4);
insert into dfs_tbl select * from temp;

set default_statistics_target=-2;
analyze dfs_tbl;
select count(*) from dfs_tbl;
select count(*) from cstore.pg_delta_dfs_tbl;
select relname , relpages, reltuples from pg_class where relname='dfs_tbl';
select relname , relpages, reltuples from pg_class where oid in ( select reldeltarelid from pg_class where relname='dfs_tbl');
select staattnum, stainherit from pg_statistic where starelid in (select oid from pg_class where relname='dfs_tbl') order by 1, 2;
select staattnum, stainherit from pg_statistic where starelid in (select reldeltarelid from pg_class where relname='dfs_tbl') order by 1, 2;
reset default_statistics_target;

--test no data
drop table if exists dfs_tbl;
create table dfs_tbl(a int, b int) tablespace hdfs_ts;
select relname , relpages, reltuples from pg_class where relname='dfs_tbl';
select relname , relpages, reltuples from pg_class where oid in ( select reldeltarelid from pg_class where relname='dfs_tbl');
select staattnum, stainherit from pg_statistic where starelid in (select oid from pg_class where relname='dfs_tbl') order by 1, 2;
select staattnum, stainherit from pg_statistic where starelid in (select reldeltarelid from pg_class where relname='dfs_tbl') order by 1, 2;
--analyze
analyze dfs_tbl;

execute direct  on (datanode1) 'select count(*) from dfs_tbl';
select relname , relpages, reltuples from pg_class where relname='dfs_tbl';
select relname , relpages, reltuples from pg_class where oid in ( select reldeltarelid from pg_class where relname='dfs_tbl');
select staattnum, stainherit from pg_statistic where starelid in (select oid from pg_class where relname='dfs_tbl') order by 1, 2;
select staattnum, stainherit from pg_statistic where starelid in (select reldeltarelid from pg_class where relname='dfs_tbl') order by 1, 2;

--test insert data after analyze
drop table if exists dfs_tbl;
create table dfs_tbl(a int, b int) tablespace hdfs_ts;
set cstore_insert_mode=delta;
drop table if exists temp;
create table temp (a int, b int);
insert into temp values(generate_series(1,100),generate_series(1,100));
insert into dfs_tbl select * from temp;

set cstore_insert_mode=main;
drop table if exists temp;
create table temp (a int, b int);
insert into temp values(generate_series(1,100),generate_series(1,100));
insert into dfs_tbl select * from temp;
--analyze
analyze dfs_tbl;
execute direct  on (datanode1) 'select count(*) from dfs_tbl';
set cstore_insert_mode=delta;
drop table if exists temp;
create table temp (a int, b int);
insert into temp values(generate_series(1,50),generate_series(1,50));
insert into dfs_tbl select * from temp;

set cstore_insert_mode=main;
drop table if exists temp;
create table temp (a int, b int);
insert into temp values(generate_series(1,50),generate_series(1,50));
insert into dfs_tbl select * from temp;

--analyze
analyze dfs_tbl;
execute direct  on (datanode1) 'select count(*) from dfs_tbl';
execute direct  on (datanode1) 'select count(*) from cstore.pg_delta_dfs_tbl';
select relname , relpages, reltuples from pg_class where relname='dfs_tbl';
select relname , relpages, reltuples from pg_class where oid in ( select reldeltarelid from pg_class where relname='dfs_tbl');
select staattnum, stainherit from pg_statistic where starelid in (select oid from pg_class where relname='dfs_tbl') order by 1, 2;
select staattnum, stainherit from pg_statistic where starelid in (select reldeltarelid from pg_class where relname='dfs_tbl') order by 1, 2;

--test update data alter analyze
drop table if exists dfs_tbl;
create table dfs_tbl(a int, b int) tablespace hdfs_ts;
set cstore_insert_mode=delta;
drop table if exists temp;
create table temp (a int, b int);
insert into temp values(generate_series(1,100),generate_series(1,100));
insert into dfs_tbl select * from temp;

set cstore_insert_mode=main;
drop table if exists temp;
create table temp (a int, b int);
insert into temp values(generate_series(1,100),generate_series(1,100));
insert into dfs_tbl select * from temp;
--analyze
analyze dfs_tbl;
execute direct  on (datanode1) 'select count(*) from dfs_tbl';

update dfs_tbl set b = 30 where b <= 60;

--analyze
analyze dfs_tbl;
execute direct  on (datanode1) 'select count(*) from dfs_tbl';
execute direct  on (datanode1) 'select count(*) from cstore.pg_delta_dfs_tbl';
select relname , relpages, reltuples from pg_class where relname='dfs_tbl';
select relname , relpages, reltuples from pg_class where oid in ( select reldeltarelid from pg_class where relname='dfs_tbl');
select staattnum, stainherit from pg_statistic where starelid in (select oid from pg_class where relname='dfs_tbl') order by 1, 2;
select staattnum, stainherit from pg_statistic where starelid in (select reldeltarelid from pg_class where relname='dfs_tbl') order by 1, 2;

--test delete after analyze
drop table if exists dfs_tbl;
create table dfs_tbl(a int, b int) tablespace hdfs_ts;
set cstore_insert_mode=delta;
drop table if exists temp;
create table temp (a int, b int);
insert into temp values(generate_series(1,100),generate_series(1,100));
insert into dfs_tbl select * from temp;

set cstore_insert_mode=main;
drop table if exists temp;
create table temp (a int, b int);
insert into temp values(generate_series(1,100),generate_series(1,100));
insert into dfs_tbl select * from temp;
--analyze
analyze dfs_tbl;
execute direct  on (datanode1) 'select count(*) from dfs_tbl';

delete from dfs_tbl where b<= 60;

--analyze
analyze dfs_tbl;
execute direct  on (datanode1) 'select count(*) from dfs_tbl';
select relname , relpages, reltuples from pg_class where relname='dfs_tbl';
execute direct  on (datanode1) 'select count(*) from cstore.pg_delta_dfs_tbl';
select relname , relpages, reltuples from pg_class where oid in ( select reldeltarelid from pg_class where relname='dfs_tbl');
select staattnum, stainherit from pg_statistic where starelid in (select oid from pg_class where relname='dfs_tbl') order by 1, 2;
select staattnum, stainherit from pg_statistic where starelid in (select reldeltarelid from pg_class where relname='dfs_tbl') order by 1, 2;

--test delete all data after analyze
drop table if exists dfs_tbl;
create table dfs_tbl(a int, b int) tablespace hdfs_ts;
set cstore_insert_mode=delta;
drop table if exists temp;
create table temp (a int, b int);
insert into temp values(generate_series(1,100),generate_series(1,100));
insert into dfs_tbl select * from temp;

set cstore_insert_mode=main;
drop table if exists temp;
create table temp (a int, b int);
insert into temp values(generate_series(1,100),generate_series(1,100));
insert into dfs_tbl select * from temp;
--analyze
analyze dfs_tbl;
execute direct  on (datanode1) 'select count(*) from dfs_tbl';

delete from dfs_tbl;

--analyze
analyze dfs_tbl;
execute direct  on (datanode1) 'select count(*) from dfs_tbl';
execute direct  on (datanode1) 'select count(*) from cstore.pg_delta_dfs_tbl';
select relname , relpages, reltuples from pg_class where relname='dfs_tbl';
select relname , relpages, reltuples from pg_class where oid in ( select reldeltarelid from pg_class where relname='dfs_tbl');
select staattnum, stainherit from pg_statistic where starelid in (select oid from pg_class where relname='dfs_tbl') order by 1, 2;
select staattnum, stainherit from pg_statistic where starelid in (select reldeltarelid from pg_class where relname='dfs_tbl') order by 1, 2;


----test analyze column
---test data store on Delta
drop table if exists dfs_tbl;
create table dfs_tbl(a int, b int) tablespace hdfs_ts;
set cstore_insert_mode=delta;
drop table if exists temp;
create table temp (a int, b int);
insert into temp values(generate_series(1,100),generate_series(1,100));
insert into dfs_tbl select * from temp;

--analyze
analyze dfs_tbl (b);
execute direct  on (datanode1) 'select count(*) from dfs_tbl';
select relname , relpages, reltuples from pg_class where relname='dfs_tbl';
select relname , relpages, reltuples from pg_class where oid in ( select reldeltarelid from pg_class where relname='dfs_tbl');
select staattnum, stainherit from pg_statistic where starelid in (select oid from pg_class where relname='dfs_tbl') order by 1, 2;
select staattnum, stainherit from pg_statistic where starelid in (select reldeltarelid from pg_class where relname='dfs_tbl') order by 1, 2;

---test data store on HDFS
drop table if exists dfs_tbl;
create table dfs_tbl(a int, b int) tablespace hdfs_ts;
set cstore_insert_mode=main;
drop table if exists temp;
create table temp (a int, b int);
insert into temp values(generate_series(1,100),generate_series(1,100));
insert into dfs_tbl select * from temp;

--analyze
analyze dfs_tbl (b);
execute direct  on (datanode1) 'select count(*) from dfs_tbl';
select relname , relpages, reltuples from pg_class where relname='dfs_tbl';
select relname , relpages, reltuples from pg_class where oid in ( select reldeltarelid from pg_class where relname='dfs_tbl');
select staattnum, stainherit from pg_statistic where starelid in (select oid from pg_class where relname='dfs_tbl') order by 1, 2;
select staattnum, stainherit from pg_statistic where starelid in (select reldeltarelid from pg_class where relname='dfs_tbl') order by 1, 2;

----test data on HDFS and Delta
drop table if exists dfs_tbl;
create table dfs_tbl(a int, b int) tablespace hdfs_ts;
set cstore_insert_mode=delta;
drop table if exists temp;
create table temp (a int, b int);
insert into temp values(generate_series(1,100),generate_series(1,100));
insert into dfs_tbl select * from temp;

set cstore_insert_mode=main;
drop table if exists temp;
create table temp (a int, b int);
insert into temp values(generate_series(1,100),generate_series(1,100));
insert into dfs_tbl select * from temp;

analyze dfs_tbl (b);
execute direct  on (datanode1) 'select count(*) from dfs_tbl';
execute direct  on (datanode1) 'select count(*) from cstore.pg_delta_dfs_tbl';
select relname , relpages, reltuples from pg_class where relname='dfs_tbl';
select relname , relpages, reltuples from pg_class where oid in ( select reldeltarelid from pg_class where relname='dfs_tbl');
select staattnum, stainherit from pg_statistic where starelid in (select oid from pg_class where relname='dfs_tbl') order by 1, 2;
select staattnum, stainherit from pg_statistic where starelid in (select reldeltarelid from pg_class where relname='dfs_tbl') order by 1, 2;

--test no data 
drop table if exists dfs_tbl;
create table dfs_tbl(a int, b int) tablespace hdfs_ts;
analyze dfs_tbl (b);
execute direct  on (datanode1) 'select count(*) from dfs_tbl';
select relname , relpages, reltuples from pg_class where relname='dfs_tbl';
select relname , relpages, reltuples from pg_class where oid in ( select reldeltarelid from pg_class where relname='dfs_tbl');
select staattnum, stainherit from pg_statistic where starelid in (select oid from pg_class where relname='dfs_tbl') order by 1, 2;
select staattnum, stainherit from pg_statistic where starelid in (select reldeltarelid from pg_class where relname='dfs_tbl') order by 1, 2;

----------------------------------------------------test distinct
drop table if exists dfs_tbl;
create table dfs_tbl(a int, b int) tablespace hdfs_ts;
set cstore_insert_mode=delta;
drop table if exists temp;
create table temp (a int, b int);
insert into temp values(generate_series(1,100),generate_series(1,100));
insert into dfs_tbl select * from temp;

set cstore_insert_mode=main;
drop table if exists temp;
create table temp (a int, b int);
insert into temp values(generate_series(1,100),generate_series(1,100));
insert into dfs_tbl select * from temp;

analyze dfs_tbl (b);
execute direct  on (datanode1) 'select count(distinct(b)) from dfs_tbl';
execute direct  on (datanode1) 'select count(distinct(b)) from cstore.pg_delta_dfs_tbl';

select staattnum, stadistinct, stainherit from pg_statistic where starelid in (select oid from pg_class where relname='dfs_tbl') order by 1, 2, 3;
select staattnum, stadistinct, stainherit from pg_statistic where starelid in (select reldeltarelid from pg_class where relname='dfs_tbl') order by 1, 2, 3;

--test data in Delta
drop table if exists dfs_tbl;
create table dfs_tbl(a int, b int) tablespace hdfs_ts;
set cstore_insert_mode=delta;
drop table if exists temp;
create table temp (a int, b int);
insert into temp values(generate_series(1,100),generate_series(1,100));
insert into dfs_tbl select * from temp;

analyze dfs_tbl;
execute direct  on (datanode1) 'select count(distinct(b)) from dfs_tbl';
execute direct  on (datanode1) 'select count(distinct(b)) from cstore.pg_delta_dfs_tbl';

select staattnum, stadistinct, stainherit from pg_statistic where starelid in (select oid from pg_class where relname='dfs_tbl') order by 1, 2, 3;
select staattnum, stadistinct, stainherit from pg_statistic where starelid in (select reldeltarelid from pg_class where relname='dfs_tbl') order by 1, 2, 3;

--test data in HDFS
drop table if exists dfs_tbl;
create table dfs_tbl(a int, b int) tablespace hdfs_ts;
set cstore_insert_mode=main;
drop table if exists temp;
create table temp (a int, b int);
insert into temp values(generate_series(1,100),generate_series(1,100));
insert into dfs_tbl select * from temp;

analyze dfs_tbl;
execute direct  on (datanode1) 'select count(distinct(b)) from dfs_tbl';
execute direct  on (datanode1) 'select count(distinct(b)) from cstore.pg_delta_dfs_tbl';

select staattnum, stadistinct, stainherit from pg_statistic where starelid in (select oid from pg_class where relname='dfs_tbl') order by 1, 2, 3;
select staattnum, stadistinct, stainherit from pg_statistic where starelid in (select reldeltarelid from pg_class where relname='dfs_tbl') order by 1, 2, 3;


-----------------------------------------test analyze all tables
drop table if exists dfs_tbl;
create table dfs_tbl(a int, b int) tablespace hdfs_ts;
set cstore_insert_mode=main;
drop table if exists temp;
create table temp (a int, b int);
insert into temp values(generate_series(1,100),generate_series(1,100));
insert into dfs_tbl select * from temp;
set cstore_insert_mode=delta;
drop table if exists temp;
create table temp (a int, b int);
insert into temp values(generate_series(1,100),generate_series(1,100));
insert into dfs_tbl select * from temp;

drop table if exists row_tbl;
create table row_tbl(a int, b int);
insert into row_tbl values(generate_series(1,100),generate_series(1,100));

drop table if exists col_tbl;
create table col_tbl(a int, b int) with (orientation=column);

insert into col_tbl select * from row_tbl;

-------------------------------------------------------------------HDFS partition table analyze
--set enable_global_stats=off;

---test data store on delta
drop table if exists dfs_tbl_p;
create table dfs_tbl_p(a int, b int) tablespace hdfs_ts partition by values (b);
set cstore_insert_mode=delta;
drop table if exists temp;
create table temp (a int, b int);
insert into temp values(generate_series(1,100),generate_series(1,100));
insert into dfs_tbl_p select * from temp;

select relname , relpages, reltuples from pg_class where relname='dfs_tbl_p';
select relname , relpages, reltuples from pg_class where oid in ( select reldeltarelid from pg_class where relname='dfs_tbl_p');
select staattnum, stainherit from pg_statistic where starelid in (select oid from pg_class where relname='dfs_tbl_p') order by 1, 2;
select staattnum, stainherit from pg_statistic where starelid in (select reldeltarelid from pg_class where relname='dfs_tbl_p') order by 1, 2;
--analyze
analyze dfs_tbl_p;

execute direct  on (datanode1) 'select count(*) from dfs_tbl_p';
select relname , relpages, reltuples from pg_class where relname='dfs_tbl_p';
select relname , relpages, reltuples from pg_class where oid in ( select reldeltarelid from pg_class where relname='dfs_tbl_p');
select staattnum, stainherit from pg_statistic where starelid in (select oid from pg_class where relname='dfs_tbl_p') order by 1, 2;
select staattnum, stainherit from pg_statistic where starelid in (select reldeltarelid from pg_class where relname='dfs_tbl_p') order by 1, 2;

---test data store on HDFS
drop table if exists dfs_tbl_p;
create table dfs_tbl_p(a int, b int) tablespace hdfs_ts partition by values (b);
set cstore_insert_mode=main;
drop table if exists temp;
create table temp (a int, b int);
insert into temp values(generate_series(1,100),generate_series(1,100));
insert into dfs_tbl_p select * from temp;

select relname , relpages, reltuples from pg_class where relname='dfs_tbl_p';
select relname , relpages, reltuples from pg_class where oid in ( select reldeltarelid from pg_class where relname='dfs_tbl_p');
select staattnum, stainherit from pg_statistic where starelid in (select oid from pg_class where relname='dfs_tbl_p') order by 1, 2;
select staattnum, stainherit from pg_statistic where starelid in (select reldeltarelid from pg_class where relname='dfs_tbl_p') order by 1, 2;
--analyze
analyze dfs_tbl_p;
execute direct  on (datanode1) 'select count(*) from dfs_tbl_p';
select relname , relpages, reltuples from pg_class where relname='dfs_tbl_p';
select relname , relpages, reltuples from pg_class where oid in ( select reldeltarelid from pg_class where relname='dfs_tbl_p');
select staattnum, stainherit from pg_statistic where starelid in (select oid from pg_class where relname='dfs_tbl_p') order by 1, 2;
select staattnum, stainherit from pg_statistic where starelid in (select reldeltarelid from pg_class where relname='dfs_tbl_p') order by 1, 2;

--test data store on HDFS and Delta
drop table if exists dfs_tbl_p;
create table dfs_tbl_p(a int, b int) tablespace hdfs_ts partition by values (b);
set cstore_insert_mode=delta;
drop table if exists temp;
create table temp (a int, b int);
insert into temp values(generate_series(1,100),generate_series(1,100));
insert into dfs_tbl_p select * from temp;

set cstore_insert_mode=main;
drop table if exists temp;
create table temp (a int, b int);
insert into temp values(generate_series(1,100),generate_series(1,100));
insert into dfs_tbl_p select * from temp;

execute direct  on (datanode1) 'select count(*) from dfs_tbl_p';
execute direct  on (datanode1) 'select count(*) from cstore.pg_delta_dfs_tbl_p';
select relname , relpages, reltuples from pg_class where relname='dfs_tbl_p';
select relname , relpages, reltuples from pg_class where oid in ( select reldeltarelid from pg_class where relname='dfs_tbl_p');
select staattnum, stainherit from pg_statistic where starelid in (select oid from pg_class where relname='dfs_tbl_p') order by 1, 2;
select staattnum, stainherit from pg_statistic where starelid in (select reldeltarelid from pg_class where relname='dfs_tbl_p') order by 1, 2;
--analyze
analyze dfs_tbl_p;
execute direct  on (datanode1) 'select count(*) from dfs_tbl_p';
select relname , relpages, reltuples from pg_class where relname='dfs_tbl_p';
select relname , relpages, reltuples from pg_class where oid in ( select reldeltarelid from pg_class where relname='dfs_tbl_p');
select staattnum, stainherit from pg_statistic where starelid in (select oid from pg_class where relname='dfs_tbl_p') order by 1, 2;
select staattnum, stainherit from pg_statistic where starelid in (select reldeltarelid from pg_class where relname='dfs_tbl_p') order by 1, 2;

--test no data
drop table if exists dfs_tbl_p;
create table dfs_tbl_p(a int, b int) tablespace hdfs_ts partition by values (b);
select relname , relpages, reltuples from pg_class where relname='dfs_tbl_p';
select relname , relpages, reltuples from pg_class where oid in ( select reldeltarelid from pg_class where relname='dfs_tbl_p');
select staattnum, stainherit from pg_statistic where starelid in (select oid from pg_class where relname='dfs_tbl_p') order by 1, 2;
select staattnum, stainherit from pg_statistic where starelid in (select reldeltarelid from pg_class where relname='dfs_tbl_p') order by 1, 2;
--analyze
analyze dfs_tbl_p;

execute direct  on (datanode1) 'select count(*) from dfs_tbl_p';
select relname , relpages, reltuples from pg_class where relname='dfs_tbl_p';
select relname , relpages, reltuples from pg_class where oid in ( select reldeltarelid from pg_class where relname='dfs_tbl_p');
select staattnum, stainherit from pg_statistic where starelid in (select oid from pg_class where relname='dfs_tbl_p') order by 1, 2;
select staattnum, stainherit from pg_statistic where starelid in (select reldeltarelid from pg_class where relname='dfs_tbl_p') order by 1, 2;

--test insert data after analyze
drop table if exists dfs_tbl_p;
create table dfs_tbl_p(a int, b int) tablespace hdfs_ts partition by values (b);
set cstore_insert_mode=delta;
drop table if exists temp;
create table temp (a int, b int);
insert into temp values(generate_series(1,100),generate_series(1,100));
insert into dfs_tbl_p select * from temp;

set cstore_insert_mode=main;
drop table if exists temp;
create table temp (a int, b int);
insert into temp values(generate_series(1,100),generate_series(1,100));
insert into dfs_tbl_p select * from temp;
--analyze
analyze dfs_tbl_p;
execute direct  on (datanode1) 'select count(*) from dfs_tbl_p';
set cstore_insert_mode=delta;
drop table if exists temp;
create table temp (a int, b int);
insert into temp values(generate_series(1,50),generate_series(1,50));
insert into dfs_tbl_p select * from temp;

set cstore_insert_mode=main;
drop table if exists temp;
create table temp (a int, b int);
insert into temp values(generate_series(1,50),generate_series(1,50));
insert into dfs_tbl_p select * from temp;

--analyze
analyze dfs_tbl_p;
execute direct  on (datanode1) 'select count(*) from dfs_tbl_p';
execute direct  on (datanode1) 'select count(*) from cstore.pg_delta_dfs_tbl_p';
select relname , relpages, reltuples from pg_class where relname='dfs_tbl_p';
select relname , relpages, reltuples from pg_class where oid in ( select reldeltarelid from pg_class where relname='dfs_tbl_p');
select staattnum, stainherit from pg_statistic where starelid in (select oid from pg_class where relname='dfs_tbl_p') order by 1, 2;
select staattnum, stainherit from pg_statistic where starelid in (select reldeltarelid from pg_class where relname='dfs_tbl_p') order by 1, 2;

--test update data alter analyze
drop table if exists dfs_tbl_p;
create table dfs_tbl_p(a int, b int) tablespace hdfs_ts partition by values (b);
set cstore_insert_mode=delta;
drop table if exists temp;
create table temp (a int, b int);
insert into temp values(generate_series(1,100),generate_series(1,100));
insert into dfs_tbl_p select * from temp;

set cstore_insert_mode=main;
drop table if exists temp;
create table temp (a int, b int);
insert into temp values(generate_series(1,100),generate_series(1,100));
insert into dfs_tbl_p select * from temp;
--analyze
analyze dfs_tbl_p;
execute direct  on (datanode1) 'select count(*) from dfs_tbl_p';

update dfs_tbl_p set b = 30 where b <= 60;

--analyze
analyze dfs_tbl_p;
execute direct  on (datanode1) 'select count(*) from dfs_tbl_p';
execute direct  on (datanode1) 'select count(*) from cstore.pg_delta_dfs_tbl_p';
select relname , relpages, reltuples from pg_class where relname='dfs_tbl_p';
select relname , relpages, reltuples from pg_class where oid in ( select reldeltarelid from pg_class where relname='dfs_tbl_p');
select staattnum, stainherit from pg_statistic where starelid in (select oid from pg_class where relname='dfs_tbl_p') order by 1, 2;
select staattnum, stainherit from pg_statistic where starelid in (select reldeltarelid from pg_class where relname='dfs_tbl_p') order by 1, 2;

--test delete after analyze
drop table if exists dfs_tbl_p;
create table dfs_tbl_p(a int, b int) tablespace hdfs_ts partition by values (b);
set cstore_insert_mode=delta;
drop table if exists temp;
create table temp (a int, b int);
insert into temp values(generate_series(1,100),generate_series(1,100));
insert into dfs_tbl_p select * from temp;

set cstore_insert_mode=main;
drop table if exists temp;
create table temp (a int, b int);
insert into temp values(generate_series(1,100),generate_series(1,100));
insert into dfs_tbl_p select * from temp;
--analyze
analyze dfs_tbl_p;
execute direct  on (datanode1) 'select count(*) from dfs_tbl_p';

delete from dfs_tbl_p where b<= 60;

--analyze
analyze dfs_tbl_p;
execute direct  on (datanode1) 'select count(*) from dfs_tbl_p';
select relname , relpages, reltuples from pg_class where relname='dfs_tbl_p';
execute direct  on (datanode1) 'select count(*) from cstore.pg_delta_dfs_tbl_p';
select relname , relpages, reltuples from pg_class where oid in ( select reldeltarelid from pg_class where relname='dfs_tbl_p');
select staattnum, stainherit from pg_statistic where starelid in (select oid from pg_class where relname='dfs_tbl_p') order by 1, 2;
select staattnum, stainherit from pg_statistic where starelid in (select reldeltarelid from pg_class where relname='dfs_tbl_p') order by 1, 2;

--test delete all data after analyze
drop table if exists dfs_tbl_p;
create table dfs_tbl_p(a int, b int) tablespace hdfs_ts partition by values (b);
set cstore_insert_mode=delta;
drop table if exists temp;
create table temp (a int, b int);
insert into temp values(generate_series(1,100),generate_series(1,100));
insert into dfs_tbl_p select * from temp;

set cstore_insert_mode=main;
drop table if exists temp;
create table temp (a int, b int);
insert into temp values(generate_series(1,100),generate_series(1,100));
insert into dfs_tbl_p select * from temp;
--analyze
analyze dfs_tbl_p;
execute direct  on (datanode1) 'select count(*) from dfs_tbl_p';

delete from dfs_tbl_p;

--analyze
analyze dfs_tbl_p;
execute direct  on (datanode1) 'select count(*) from dfs_tbl_p';
execute direct  on (datanode1) 'select count(*) from cstore.pg_delta_dfs_tbl_p';
select relname , relpages, reltuples from pg_class where relname='dfs_tbl_p';
select relname , relpages, reltuples from pg_class where oid in ( select reldeltarelid from pg_class where relname='dfs_tbl_p');
select staattnum, stainherit from pg_statistic where starelid in (select oid from pg_class where relname='dfs_tbl_p') order by 1, 2;
select staattnum, stainherit from pg_statistic where starelid in (select reldeltarelid from pg_class where relname='dfs_tbl_p') order by 1, 2;


----test analyze column
---test data store on Delta
drop table if exists dfs_tbl_p;
create table dfs_tbl_p(a int, b int) tablespace hdfs_ts partition by values (b);
set cstore_insert_mode=delta;
drop table if exists temp;
create table temp (a int, b int);
insert into temp values(generate_series(1,100),generate_series(1,100));
insert into dfs_tbl_p select * from temp;

--analyze
analyze dfs_tbl_p (b);
execute direct  on (datanode1) 'select count(*) from dfs_tbl_p';
select relname , relpages, reltuples from pg_class where relname='dfs_tbl_p';
select relname , relpages, reltuples from pg_class where oid in ( select reldeltarelid from pg_class where relname='dfs_tbl_p');
select staattnum, stainherit from pg_statistic where starelid in (select oid from pg_class where relname='dfs_tbl_p') order by 1, 2;
select staattnum, stainherit from pg_statistic where starelid in (select reldeltarelid from pg_class where relname='dfs_tbl_p') order by 1, 2;

---test data store on HDFS
drop table if exists dfs_tbl_p;
create table dfs_tbl_p(a int, b int) tablespace hdfs_ts partition by values (b);
set cstore_insert_mode=main;
drop table if exists temp;
create table temp (a int, b int);
insert into temp values(generate_series(1,100),generate_series(1,100));
insert into dfs_tbl_p select * from temp;

--analyze
analyze dfs_tbl_p (b);
execute direct  on (datanode1) 'select count(*) from dfs_tbl_p';
select relname , relpages, reltuples from pg_class where relname='dfs_tbl_p';
select relname , relpages, reltuples from pg_class where oid in ( select reldeltarelid from pg_class where relname='dfs_tbl_p');
select staattnum, stainherit from pg_statistic where starelid in (select oid from pg_class where relname='dfs_tbl_p') order by 1, 2;
select staattnum, stainherit from pg_statistic where starelid in (select reldeltarelid from pg_class where relname='dfs_tbl_p') order by 1, 2;

----test data on HDFS and Delta
drop table if exists dfs_tbl_p;
create table dfs_tbl_p(a int, b int) tablespace hdfs_ts partition by values (b);
set cstore_insert_mode=delta;
drop table if exists temp;
create table temp (a int, b int);
insert into temp values(generate_series(1,100),generate_series(1,100));
insert into dfs_tbl_p select * from temp;

set cstore_insert_mode=main;
drop table if exists temp;
create table temp (a int, b int);
insert into temp values(generate_series(1,100),generate_series(1,100));
insert into dfs_tbl_p select * from temp;

analyze dfs_tbl_p (b);
execute direct  on (datanode1) 'select count(*) from dfs_tbl_p';
execute direct  on (datanode1) 'select count(*) from cstore.pg_delta_dfs_tbl_p';
select relname , relpages, reltuples from pg_class where relname='dfs_tbl_p';
select relname , relpages, reltuples from pg_class where oid in ( select reldeltarelid from pg_class where relname='dfs_tbl_p');
select staattnum, stainherit from pg_statistic where starelid in (select oid from pg_class where relname='dfs_tbl_p') order by 1, 2;
select staattnum, stainherit from pg_statistic where starelid in (select reldeltarelid from pg_class where relname='dfs_tbl_p') order by 1, 2;

--test no data 
drop table if exists dfs_tbl_p;
create table dfs_tbl_p(a int, b int) tablespace hdfs_ts partition by values (b);
analyze dfs_tbl_p (b);
execute direct  on (datanode1) 'select count(*) from dfs_tbl_p';
select relname , relpages, reltuples from pg_class where relname='dfs_tbl_p';
select relname , relpages, reltuples from pg_class where oid in ( select reldeltarelid from pg_class where relname='dfs_tbl_p');
select staattnum, stainherit from pg_statistic where starelid in (select oid from pg_class where relname='dfs_tbl_p') order by 1, 2;
select staattnum, stainherit from pg_statistic where starelid in (select reldeltarelid from pg_class where relname='dfs_tbl_p') order by 1, 2;

----------------------------------------------------test distinct
drop table if exists dfs_tbl_p;
create table dfs_tbl_p(a int, b int) tablespace hdfs_ts partition by values (b);
set cstore_insert_mode=delta;
drop table if exists temp;
create table temp (a int, b int);
insert into temp values(generate_series(1,100),generate_series(1,100));
insert into dfs_tbl_p select * from temp;

set cstore_insert_mode=main;
drop table if exists temp;
create table temp (a int, b int);
insert into temp values(generate_series(1,100),generate_series(1,100));
insert into dfs_tbl_p select * from temp;

analyze dfs_tbl_p (b);
execute direct  on (datanode1) 'select count(distinct(b)) from dfs_tbl_p';
execute direct  on (datanode1) 'select count(distinct(b)) from cstore.pg_delta_dfs_tbl_p';

select staattnum, stadistinct, stainherit from pg_statistic where starelid in (select oid from pg_class where relname='dfs_tbl_p') order by 1, 2, 3;
select staattnum, stadistinct, stainherit from pg_statistic where starelid in (select reldeltarelid from pg_class where relname='dfs_tbl_p') order by 1, 2, 3;

--test data in Delta
drop table if exists dfs_tbl_p;
create table dfs_tbl_p(a int, b int) tablespace hdfs_ts partition by values (b);
set cstore_insert_mode=delta;
drop table if exists temp;
create table temp (a int, b int);
insert into temp values(generate_series(1,100),generate_series(1,100));
insert into dfs_tbl_p select * from temp;

analyze dfs_tbl_p;
execute direct  on (datanode1) 'select count(distinct(b)) from dfs_tbl_p';
execute direct  on (datanode1) 'select count(distinct(b)) from cstore.pg_delta_dfs_tbl_p';

select staattnum, stadistinct, stainherit from pg_statistic where starelid in (select oid from pg_class where relname='dfs_tbl_p') order by 1, 2, 3;
select staattnum, stadistinct, stainherit from pg_statistic where starelid in (select reldeltarelid from pg_class where relname='dfs_tbl_p') order by 1, 2, 3;

--test data in HDFS
drop table if exists dfs_tbl_p;
create table dfs_tbl_p(a int, b int) tablespace hdfs_ts partition by values (b);
set cstore_insert_mode=main;
drop table if exists temp;
create table temp (a int, b int);
insert into temp values(generate_series(1,100),generate_series(1,100));
insert into dfs_tbl_p select * from temp;

analyze dfs_tbl_p;
execute direct  on (datanode1) 'select count(distinct(b)) from dfs_tbl_p';
execute direct  on (datanode1) 'select count(distinct(b)) from cstore.pg_delta_dfs_tbl_p';

select staattnum, stadistinct, stainherit from pg_statistic where starelid in (select oid from pg_class where relname='dfs_tbl_p') order by 1, 2, 3;
select staattnum, stadistinct, stainherit from pg_statistic where starelid in (select reldeltarelid from pg_class where relname='dfs_tbl_p') order by 1, 2, 3;


-----------------------------------------test analyze all tables
drop table if exists dfs_tbl_p;
create table dfs_tbl_p(a int, b int) tablespace hdfs_ts partition by values (b);
set cstore_insert_mode=main;
drop table if exists temp;
create table temp (a int, b int);
insert into temp values(generate_series(1,100),generate_series(1,100));
insert into dfs_tbl_p select * from temp;
set cstore_insert_mode=delta;
drop table if exists temp;
create table temp (a int, b int);
insert into temp values(generate_series(1,100),generate_series(1,100));
insert into dfs_tbl_p select * from temp;

drop table if exists row_tbl;
create table row_tbl(a int, b int);
insert into row_tbl values(generate_series(1,100),generate_series(1,100));

drop table if exists col_tbl;
create table col_tbl(a int, b int) with (orientation=column);

insert into col_tbl select * from row_tbl;

--analyze after delete column with index
DROP TABLE IF exists HDFS_ADD_DROP_COLUMN_TABLE_005;
CREATE TABLE HDFS_ADD_DROP_COLUMN_TABLE_005 (sn int)
WITH(orientation = 'orc',version = 0.12) TABLESPACE hdfs_ts DISTRIBUTE BY HASH(sn);
SET cstore_insert_mode=delta;
INSERT INTO HDFS_ADD_DROP_COLUMN_TABLE_005 VALUES(generate_series(1,100));
ALTER TABLE HDFS_ADD_DROP_COLUMN_TABLE_005 ADD COLUMN c_tinyint tinyint null;
CREATE INDEX index_drop_005 ON HDFS_ADD_DROP_COLUMN_TABLE_005(c_tinyint);
INSERT INTO HDFS_ADD_DROP_COLUMN_TABLE_005(sn) VALUES(generate_series(101,200));
analyze hdfs_add_drop_column_table_005;
DELETE FROM HDFS_ADD_DROP_COLUMN_TABLE_005 ;
ALTER TABLE HDFS_ADD_DROP_COLUMN_TABLE_005 DROP COLUMN c_tinyint;
set cstore_insert_mode=main;
INSERT INTO HDFS_ADD_DROP_COLUMN_TABLE_005 VALUES(generate_series(1,100));
VACUUM FULL HDFS_ADD_DROP_COLUMN_TABLE_005;
ANALYZE HDFS_ADD_DROP_COLUMN_TABLE_005;

drop table if exists tt;
create table tt(a int, b int);
insert into tt values(1, generate_series(1, 6000));
insert into tt select *from tt;
insert into tt select *from tt;
insert into tt select *from tt;
select count(*) from tt;
drop table if exists dfstbl002;
create table dfstbl002(a int, b int) tablespace hdfs_ts;
insert into dfstbl002 select *from tt;

set default_statistics_target=-100;
set enable_global_stats=off;
analyze dfstbl002;
drop table if exists tt;
drop table if exists dfstbl002;
drop schema analyze_schema cascade;