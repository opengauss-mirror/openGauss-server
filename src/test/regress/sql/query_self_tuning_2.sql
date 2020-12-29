set resource_track_duration=0;
set resource_track_cost=30;
set resource_track_level=operator;
set resource_track_log=detail;

/* FIFO test */
create table t01(c1 int, c2 int, c3 int);
create table t02(c1 int, c2 int, c3 int);
create table t03(c1 int, c2 int, c3 int);
create table t04(c1 int, c2 int, c3 int);
create table t05(c1 int, c2 int, c3 int);
create table t06(c1 int, c2 int, c3 int);
create table t07(c1 int, c2 int, c3 int);
create table t08(c1 int, c2 int, c3 int);
create table t09(c1 int, c2 int, c3 int);
create table t10(c1 int, c2 int, c3 int);

insert into t01 select v,v,v from generate_series(1,20) as v;
insert into t02 select * from t01;
insert into t03 select * from t01;
insert into t04 select 1,c2,c3 from t01;
insert into t05 select * from t01;
insert into t06 select 1,c2,c3 from t01;
insert into t07 select * from t01;
insert into t08 select * from t01;
insert into t09 select * from t01;
insert into t10 select * from t01;
analyze t01;
analyze t02;
create index self_tuning_index_03 on t01(c3);
create index self_tuning_index_02 on t01(c2);


update pg_class set reltuples = 1200000 where relname in ('t01', 't02', 't03', 't04', 't05', 't06', 't07', 't08', 't09', 't10');

/* FIFO test */ select /*+ nestloop(t01 t03) nestloop(t02 t05) broadcast(t07) broadcast(t02) */
t01.c1, sum(t02.c2) from t01, t02, t03, t04, t05, t06, t07, t08
where t01.c2 = t02.c2 and
	  t01.c2 = t03.c2 and
	  t01.c2 = t04.c2 and
	  t01.c2 = t05.c2 and
	  t02.c3 = t05.c3 and
	  t01.c2 = t06.c2 and
	  t01.c2 = t07.c2 and
	  t01.c2 = t08.c2
group by 1
order by 1,2;

select warning, query_plan from pgxc_wlm_session_history where query like '%FIFO%';

set enable_seqscan=off;
set enable_indexscan=on;
select count(*)self_tuning_bitmapOr from t01 where c3>1 or c2>2;

drop table t01;
drop table t02;
drop table t03;
drop table t04;
drop table t05;
drop table t06;
drop table t07;
drop table t08;
drop table t09;
drop table t10;