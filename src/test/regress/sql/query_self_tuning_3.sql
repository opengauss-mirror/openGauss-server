\c postgres
create schema sql_self_tuning_3;
set current_schema='sql_self_tuning_3';

set resource_track_duration=0;
set resource_track_cost=30;
set resource_track_level=operator;

/*
 * SQL-Self Tuning scenario[6] statistic not collect
 * operator mode should report both operator/query level issues
 */
create table t61(c1 int, c2 int, c3 int);
create table t62(c1 int, c2 int, c3 int);
create table t63(c1 int, c2 int, c3 int);
update pg_class set reltuples = 90000000000 where relname = 't63';

set resource_track_level=query;
/* Test statistics not collect query level */ select count(*), t62.c1 from t61 join t62 on t61.c2 = t62.c2 group by 2;
/* Test statistics not collect query level */ select count(*), t63.c1 from t61 join t63 on t61.c2 = t63.c2 group by 2;
select query, query_plan, warning from pgxc_wlm_session_history where query like '%Test statistics not collect%' order by 1;
set resource_track_level=operator;

/* Test statistics not collect operator level */ select count(*), t62.c1 from t61 join t62 on t61.c2 = t62.c2 group by 2;
/* Test statistics not collect operator level */ select count(*), t63.c1 from t61 join t63 on t61.c2 = t63.c2 group by 2;
select query, query_plan, warning from pgxc_wlm_session_history where query like '%Test statistics not collect%' order by 1;

/*
 * SQL-Self Tuning scenario[7] Test case for Estimated row not accurate
 */
create table t8(c1 int, c2 int, c3 int);
insert into t8 select v,v,v from generate_series(1,100) as v;

update pg_class set reltuples = 90000000000 where relname = 't8';

-- Do test
/* Test inaccurate estimation rows */ select count(*), c2 from t8 group by 2 order by 2;

-- Verify diagnosis results
select query, query_plan, warning from pgxc_wlm_session_history where query like '%Test inaccurate estimation rows%' order by 3;

drop table t8;

/* Additional test */
/* SQL with LIMIT clause only reports issue in query level scope */

set resource_track_level=query;
/* Query with LIMIT in query level */ select count(*), t63.c1 from t61 join t63 on t61.c2 = t63.c2 group by 2 LIMIT 1;
select query, query_plan, warning from pgxc_wlm_session_history where query like '%Query with LIMIT in query level%' order by 1;

set resource_track_level=operator;
/* Query with LIMIT in operator level */ select count(*), t63.c1 from t61 join t63 on t61.c2 = t63.c2 group by 2 LIMIT 1;
select query, query_plan, warning from pgxc_wlm_session_history where query like '%Query with LIMIT in operator level%' order by 1;

/* Query without LIMIT in operator level */ select count(*), t63.c1 from t61 join t63 on t61.c2 = t63.c2 group by 2;
select query, query_plan, warning from pgxc_wlm_session_history where query like '%Query without LIMIT in operator level%' order by 1;

drop table t61;
drop table t62;
drop table t63;

-- test create table as
create table t12(c1 int, c2 int, c3 int) distribute by hash(c1);
create table t13(c1 int, c2 int, c3 int) distribute by hash(c1);
insert into t12 select v,v,v from generate_series(1,20) as v;
insert into t13 select * from t12;
update pg_class set reltuples = 1200000 where relname in ('t12', 't13');
create table t11 as
/* CREATE table AS test */select a.* from t12 as a, t13 as b where a.c2 = b.c2;
select query, query_plan, warning from pgxc_wlm_session_history where query like '%CREATE table AS test%' order by 1;
drop table t11;
drop table t12;
drop table t13;

/* test delete-using can be found operator level issue */
create table t12(c1 int, c2 int, c3 int) distribute by hash(c1);
create table t13(c1 int, c2 int, c3 int) distribute by hash(c1);
insert into t12 select v,v,v from generate_series(1,20) as v;
insert into t13 select * from t12;
update pg_class set reltuples = 1200000 where relname in ('t12', 't13');
/* delete using's select */select * from t12 as a, t13 as b where a.c2 = b.c2 order by 1;
/* delete using */delete from t12 as a using t13 as b where a.c2 = b.c2;
select query, query_plan, warning from pgxc_wlm_session_history where query like '%delete using%' order by 1;
drop table t12;
drop table t13;

/* test update-using can be found operator level issue */
create table t12(c1 int, c2 int, c3 int) distribute by hash(c1);
create table t13(c1 int, c2 int, c3 int) distribute by hash(c1);
insert into t12 select v,v,v from generate_series(1,20) as v;
insert into t13 select * from t12;
update pg_class set reltuples = 1200000 where relname in ('t12', 't13');
/* update using's select */select * from t12 as a, t13 as b where a.c2 = b.c2 order by 1;
/* update using */update t12 as a set c3 =0 from t13 as b where a.c2 = b.c2;
select query, query_plan, warning from pgxc_wlm_session_history where query like '%update using%' order by 1;
drop table t12;
drop table t13;

/* test insert can be found operator level issue */
create table t12(c1 int, c2 int, c3 int) distribute by hash(c1);
create table t13(c1 int, c2 int, c3 int) distribute by hash(c1);
update pg_class set reltuples = 12000000 where relname in ('t12', 't13');
insert into t12 select v,v,v from generate_series(1,20) as v;
/* insert test's select */select * from t12 where c2 < 15 order by 1;
/* insert test */insert into t13 select * from t12 where c2 < 15 order by 1;
select query, query_plan, warning from pgxc_wlm_session_history where query like '%insert test%' order by 1;
drop table t12;
drop table t13;

-- test initplan && subplan
create table t01(c1 int, c2 int, c3 int);
create table t02(c1 int, c2 int, c3 int);
insert into t01 select v,v,v from generate_series(1,20) as v;
insert into t02 select * from t01;
analyze t01;
analyze t02;
update pg_class set reltuples = 10000000 where relname in ('t01', 't02');

/* test initplan (none-correlated subquery ) */select *
from t01 where c2 < (select c3 from t02 where t02.c2 = 1);
/* test subplan (none-correlated subquery ) */select *
from t01 where c2 < (select c3 from t02 where t02.c3 = t01.c3);
select query, query_plan, warning from pgxc_wlm_session_history where query like '%test initplan%' or query like '%test subplan%' order by 1;

drop table t01;
drop table t02;

-- test rescan case when we found materialize plan node 
create table t01(c1 int, c2 int, c3 int);
create table t02(c1 int, c2 int, c3 int);
insert into t01 select v,v,v from generate_series(1,20) as v;
insert into t02 select * from t01;
update pg_class set reltuples = 9000000 where relname in ('t01', 't02');

/* Skip rescan erows reporting */
select /*+ nestloop(t01 t02) */ * from t01 join t02 on t01.c2 = t02.c2 order by 1;
select query, query_plan, warning from pgxc_wlm_session_history where query like '%Skip rescan erows reporting%' order by 1;

drop table t01;
drop table t02;

/* test deduplicate with append plan */
create table t01(c1 int, c2 int, c3 int);
create table t02(c1 int, c2 int, c3 int);
insert into t01 select v,v,v from generate_series(1,20) as v;
insert into t02 select * from t01;
analyze t01;
analyze t02;
update pg_class set reltuples = 10000000 where relname in ('t01', 't02');

/*test setop*/select count(*)
from (select c1,c2 from t01 union all select c1,c2 from t02) as dt;

/*test setop*/select count(*)
from (select c1,c2 from t01 union select c1,c2 from t02) as dt;

select query, query_plan, warning from pgxc_wlm_session_history where query like '%test setop%' order by 1,2,3;

drop table t01;
drop table t02;
drop schema sql_self_tuning_3 cascade;
reset resource_track_level;
reset resource_track_cost;
reset resource_track_duration;

\c regression

