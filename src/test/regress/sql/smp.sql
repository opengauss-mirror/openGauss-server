create schema test_smp;
set search_path=test_smp;

create table t1(a int, b int, c int, d bigint);
insert into t1 values(generate_series(1, 100), generate_series(1, 10), generate_series(1, 2), generate_series(1, 50));
create table t2(a int, b int);
insert into t2 values(generate_series(1, 10), generate_series(1, 30));
create table t3(a int, b int, c int);
insert into t3 values(generate_series(1, 50), generate_series(1, 100), generate_series(1, 10));

analyze t1;
analyze t2;
analyze t3;

set query_dop=1002;
explain (costs off) select * from t2 order by 1,2;
select * from t2 order by 1,2;

set enable_nestloop=on;
set enable_mergejoin=off;
set enable_hashjoin=off;
explain (costs off) select t1.a,t2.b from t1, t2 where t1.a = t2.a order by 1,2;
select t1.a,t2.b from t1, t2 where t1.a = t2.a order by 1,2;

set enable_nestloop=off;
set enable_hashjoin=on;
explain (costs off) select t1.a,t2.b,t3.c from t1, t2, t3 where t1.a = t2.a and t1.b = t3.c order by 1,2,3;
select t1.a,t2.b,t3.c from t1, t2, t3 where t1.a = t2.a and t1.b = t3.c order by 1,2,3;

set enable_nestloop=on;
explain (costs off) select a, avg(b), sum(c) from t1 group by a order by 1,2,3;
select a, avg(b), sum(c) from t1 group by a order by 1,2,3;

explain (costs off) select median(a) from t1;
select median(a) from t1;

explain (costs off) select first(a) from t1;
select first(a) from t1;

explain (costs off) select sum(b)+median(a) as result from t1;
select sum(b)+median(a) as result from t1;

explain (costs off) select a, count(distinct b) from t1 group by a order by 1 limit 10;
select a, count(distinct b) from t1 group by a order by 1 limit 10;

explain (costs off) select count(distinct b), count(distinct c) from t1 limit 10;
select count(distinct b), count(distinct c) from t1 limit 10;

explain (costs off) select distinct b from t1 union all select distinct a from t2 order by 1;
select distinct b from t1 union all select distinct a from t2 order by 1;

explain (costs off) select * from t1 where t1.a in (select t2.a from t2, t3 where t2.b = t3.c) order by 1,2,3;
select * from t1 where t1.a in (select t2.a from t2, t3 where t2.b = t3.c) order by 1,2,3;

explain (costs off) with s1 as (select t1.a as a, t3.b as b from t1,t3 where t1.b=t3.c) select * from t2, s1 where t2.b=s1.a order by 1,2,3,4;
with s1 as (select t1.a as a, t3.b as b from t1,t3 where t1.b=t3.c) select * from t2, s1 where t2.b=s1.a order by 1,2,3,4;

explain (costs off) select * from t1 order by a limit 10;
select * from t1 order by a limit 10;

explain (costs off) select * from t1 order by a limit 10 offset 20;
select * from t1 order by a limit 10 offset 20;

--clean
set search_path=public;
drop schema test_smp cascade;
