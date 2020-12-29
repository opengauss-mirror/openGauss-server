--==========================================================
--==========================================================
\set ECHO all
set default_statistics_target=-2;

--========================================================== hdfs table
create table t1 (a int, b int, c int, d int) tablespace hdfs_ts distribute by hash(a);
create table t1r (a int, b int, c int, d int) tablespace hdfs_ts distribute by replication;

set cstore_insert_mode=main;

insert into t1 values (1, 1, 1, 1);
insert into t1 values (2, 1, 1, 1);
insert into t1 values (3, 2, 1, 1);
insert into t1 values (4, 2, 1, 1);
insert into t1 values (5, 3, 2, 1);
insert into t1 values (6, 3, 2, 1);
insert into t1 values (7, 4, 2, 1);
insert into t1 values (8, 4, 2, 1);
insert into t1r select * from t1;

analyze t1 ((a, c));
analyze t1 ((b, c));

analyze t1r ((a, c));
analyze t1r ((b, c));

select * from pg_ext_stats where (schemaname='public' and tablename='t1') or (schemaname='cstore' and tablename='pg_delta_public_t1') order by ext_info, tablename, inherited;
select * from pg_ext_stats where (schemaname='public' and tablename='t1r') or (schemaname='cstore' and tablename='pg_delta_public_t1r') order by ext_info, tablename, inherited;

alter table t1 add statistics ((a, b));
alter table t1 add statistics ((b, c));
alter table t1 add statistics ((a, d));
alter table t1 add statistics ((b, d));
alter table t1 add statistics ((b, c, d));
alter table t1 add statistics ((a, b, c, d));

alter table t1r add statistics ((a, b));
alter table t1r add statistics ((b, c));
alter table t1r add statistics ((a, d));
alter table t1r add statistics ((b, d));
alter table t1r add statistics ((b, c, d));
alter table t1r add statistics ((a, b, c, d));

select * from pg_ext_stats where (schemaname='public' and tablename='t1') or (schemaname='cstore' and tablename='pg_delta_public_t1') order by ext_info, tablename, inherited;
select * from pg_ext_stats where (schemaname='public' and tablename='t1r') or (schemaname='cstore' and tablename='pg_delta_public_t1r') order by ext_info, tablename, inherited;

analyze t1 ((b, d));
analyze t1 ((c, d));

analyze t1r ((b, d));
analyze t1r ((c, d));

select * from pg_ext_stats where (schemaname='public' and tablename='t1') or (schemaname='cstore' and tablename='pg_delta_public_t1') order by ext_info, tablename, inherited;
select * from pg_ext_stats where (schemaname='public' and tablename='t1r') or (schemaname='cstore' and tablename='pg_delta_public_t1r') order by ext_info, tablename, inherited;

analyze t1;
analyze t1r;

select * from pg_ext_stats where (schemaname='public' and tablename='t1') or (schemaname='cstore' and tablename='pg_delta_public_t1') order by ext_info, tablename, inherited;
select * from pg_ext_stats where (schemaname='public' and tablename='t1r') or (schemaname='cstore' and tablename='pg_delta_public_t1r') order by ext_info, tablename, inherited;

alter table t1 delete statistics ((a, c));
alter table t1 delete statistics ((a, b));
alter table t1 delete statistics ((b, c));
alter table t1 delete statistics ((a, d));
alter table t1 delete statistics ((b, d));
alter table t1 delete statistics ((b, c, d));
alter table t1 delete statistics ((a, b, c, d));
alter table t1 delete statistics ((c, d));

alter table t1r delete statistics ((a, c));
alter table t1r delete statistics ((a, b));
alter table t1r delete statistics ((b, c));
alter table t1r delete statistics ((a, d));
alter table t1r delete statistics ((b, d));
alter table t1r delete statistics ((b, c, d));
alter table t1r delete statistics ((a, b, c, d));
alter table t1r delete statistics ((c, d));

select * from pg_ext_stats where (schemaname='public' and tablename='t1') or (schemaname='cstore' and tablename='pg_delta_public_t1') order by ext_info, tablename, inherited;
select * from pg_ext_stats where (schemaname='public' and tablename='t1r') or (schemaname='cstore' and tablename='pg_delta_public_t1r') order by ext_info, tablename, inherited;

--========================================================== hdfs foreign table
create foreign table ft1(a int, b int, c int, d int) 
server hdfs_server
options(format 'orc', foldername '/user/cyn/hadoop_data_2018061401/tablespace_secondary/postgres/public.t1') 
distribute by roundrobin;

create foreign table ft1r(a int, b int, c int, d int) 
server hdfs_server
options(format 'orc', foldername '/user/cyn/hadoop_data_2018061401/tablespace_secondary/postgres/public.t1') 
distribute by replication;

analyze ft1 ((a, c));
analyze ft1 ((b, c));

analyze ft1r ((a, c));
analyze ft1r ((b, c));

select * from pg_ext_stats where (schemaname='public' and tablename='ft1') or (schemaname='cstore' and tablename='pg_delta_public_ft1') order by ext_info, tablename, inherited;
select * from pg_ext_stats where (schemaname='public' and tablename='ft1r') or (schemaname='cstore' and tablename='pg_delta_public_ft1r') order by ext_info, tablename, inherited;

alter table ft1 add statistics ((a, b));
alter table ft1 add statistics ((b, c));
alter table ft1 add statistics ((a, d));
alter table ft1 add statistics ((b, d));
alter table ft1 add statistics ((b, c, d));
alter table ft1 add statistics ((a, b, c, d));

alter table ft1r add statistics ((a, b));
alter table ft1r add statistics ((b, c));
alter table ft1r add statistics ((a, d));
alter table ft1r add statistics ((b, d));
alter table ft1r add statistics ((b, c, d));
alter table ft1r add statistics ((a, b, c, d));

select * from pg_ext_stats where (schemaname='public' and tablename='ft1') or (schemaname='cstore' and tablename='pg_delta_public_ft1') order by ext_info, tablename, inherited;
select * from pg_ext_stats where (schemaname='public' and tablename='ft1r') or (schemaname='cstore' and tablename='pg_delta_public_ft1r') order by ext_info, tablename, inherited;

analyze ft1 ((b, d));
analyze ft1 ((c, d));

analyze ft1r ((b, d));
analyze ft1r ((c, d));

select * from pg_ext_stats where (schemaname='public' and tablename='ft1') or (schemaname='cstore' and tablename='pg_delta_public_ft1') order by ext_info, tablename, inherited;
select * from pg_ext_stats where (schemaname='public' and tablename='ft1r') or (schemaname='cstore' and tablename='pg_delta_public_ft1r') order by ext_info, tablename, inherited;

analyze ft1;
analyze ft1r;

select * from pg_ext_stats where (schemaname='public' and tablename='ft1') or (schemaname='cstore' and tablename='pg_delta_public_ft1') order by ext_info, tablename, inherited;
select * from pg_ext_stats where (schemaname='public' and tablename='ft1r') or (schemaname='cstore' and tablename='pg_delta_public_ft1r') order by ext_info, tablename, inherited;

drop foreign table ft1;
drop foreign table ft1r;

select * from pg_ext_stats where (schemaname='public' and tablename='ft1') or (schemaname='cstore' and tablename='pg_delta_public_ft1') order by ext_info, tablename, inherited;
select * from pg_ext_stats where (schemaname='public' and tablename='ft1r') or (schemaname='cstore' and tablename='pg_delta_public_ft1r') order by ext_info, tablename, inherited;

--========================================================== clear environment
drop table t1;
drop table t1r;

select * from pg_ext_stats where (schemaname='public' and tablename='t1') or (schemaname='cstore' and tablename='pg_delta_public_t1') order by ext_info, tablename, inherited;
select * from pg_ext_stats where (schemaname='public' and tablename='t1r') or (schemaname='cstore' and tablename='pg_delta_public_t1r') order by ext_info, tablename, inherited;




