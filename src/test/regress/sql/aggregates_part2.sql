/*
 * This file is used to test three possible paths of hash agg stream paths
 */
-- Part-2
drop schema if exists distribute_aggregates_part2 cascade;
create schema distribute_aggregates_part2;
set current_schema = distribute_aggregates_part2;

-- prepare a temp table for import data
create table tmp_t1(c1 int);
insert into tmp_t1 values (1);

-- Create Table and Insert Data
create table t_agg1(a int, b int, c int, d int, e int, f int, g regproc);
create table t_agg2(a int, b int, c int);
insert into t_agg1 select generate_series(1, 10000), generate_series(1, 10000)%5000, generate_series(1, 10000)%500, generate_series(1, 10000)%5, 500, 3, 'sin' from tmp_t1;
insert into t_agg2 select generate_series(1, 10), generate_series(11, 2, -1), generate_series(3, 12);
/*select * from table_skewness('t_agg1', 'b,c') order by 1, 2, 3;*/
analyze t_agg1;
analyze t_agg2;

-- (2) distinct clause
explain (costs off) select avg(x) from (select distinct(b) x from t_agg1);
select avg(x) from (select distinct(b) x from t_agg1);
explain (costs off) select avg(x) from (select distinct(b+c) x from t_agg1);
select avg(x) from (select distinct(b+c) x from t_agg1);
explain (costs off) select sum(x) from (select distinct(1) x from t_agg1);
select sum(x) from (select distinct(1) x from t_agg1);

-- (3) windowagg
explain (costs off) select b, sum(x) from (select b, row_number() over (partition by b, d, e order by d, e) x from t_agg1) group by b;
select b, sum(x) from (select b, row_number() over (partition by b, d, e order by d, e) x from t_agg1) group by b order by b limit 10;
explain (costs off) select x, sum(a+b+c) from (select a, b, c, row_number() over (partition by c order by a, b) x from t_agg1) group by x;
select x, sum(a+b+c) from (select a, b, c, row_number() over (partition by c order by a, b) x from t_agg1) group by x order by x limit 10;

-- join
explain (costs off) select t_agg1.b, sum(t_agg1.a) from t_agg1 join t_agg2 on t_agg1.c=t_agg2.b group by t_agg1.b;
select t_agg1.b, sum(t_agg1.a) from t_agg1 join t_agg2 on t_agg1.c=t_agg2.b group by t_agg1.b order by t_agg1.b limit 10;
explain (costs off) select a, b, count(c) from (select t_agg1.c a, t_agg1.b b, t_agg2.a-t_agg2.b c from t_agg1 join t_agg2 on t_agg1.b=t_agg2.c) group by a, b; 
select a, b, count(c) from (select t_agg1.c a, t_agg1.b b, t_agg2.a-t_agg2.b c from t_agg1 join t_agg2 on t_agg1.b=t_agg2.c) group by a, b order by a, b limit 10; 
explain (costs off) select a, b, count(c) from (select t_agg1.c a, t_agg1.b b, sum(t_agg2.a-t_agg2.b) c from t_agg1 join t_agg2 on t_agg1.b=t_agg2.c group by t_agg1.b, t_agg1.c) group by a, b; 
select a, b, count(c) from (select t_agg1.c a, t_agg1.b b, sum(t_agg2.a-t_agg2.b) c from t_agg1 join t_agg2 on t_agg1.b=t_agg2.c group by t_agg1.b, t_agg1.c) group by a, b order by a, b limit 10; 
explain (costs off) select a, b, count(c) from (select t_agg1.c a, t_agg1.b b, sum(t_agg2.a-t_agg2.b) c from t_agg1 join t_agg2 on t_agg1.b=t_agg2.c group by t_agg1.b, t_agg1.c having avg(t_agg2.a-t_agg2.b)>=0) group by a, b having count(c)=1; 
select a, b, count(c) from (select t_agg1.c a, t_agg1.b b, sum(t_agg2.a-t_agg2.b) c from t_agg1 join t_agg2 on t_agg1.b=t_agg2.c group by t_agg1.b, t_agg1.c having avg(t_agg2.a-t_agg2.b)>=0) group by a, b having count(c)=1 order by a, b limit 10; 

-- Case 3: hashagg + redistribute + hashagg + gather, applicable to relatively small aggregate set, with much rows
-- eliminated by hash agg 
-- group by clause
explain (costs off) select c, sum(e) from t_agg1 group by c;
select c, sum(e) from t_agg1 group by c order by c limit 10;
explain (costs off) select c, sum(a+b), avg(d) from t_agg1 group by c;
select c, sum(a+b), avg(d) from t_agg1 group by c order by c limit 10;
explain (costs off) select c, d, max(a), min(b) from t_agg1 group by c, d;
select c, d, max(a), min(b) from t_agg1 group by c, d order by c, d limit 10;
explain (costs off) select c, min(b), rank() over (partition by d order by d) from t_agg1 group by c, d;
select c, min(b), rank() over (partition by d order by d) from t_agg1 group by c, d order by c, d limit 10;

-- distinct clause
explain (costs off) select distinct(c) from t_agg1;
select distinct(c) from t_agg1 order by 1 limit 10;
explain (costs off) select distinct(c*d) from t_agg1;
select distinct(c*d) from t_agg1 order by 1 limit 10;

-- subquery
-- (1) group by clause
explain (costs off) select sum(x) from (select sum(b) x from t_agg1 group by c);
select sum(x) from (select sum(b) x from t_agg1 group by c);
explain (costs off) select sum(x) from (select sum(a) x from t_agg1 group by b, c);
select sum(x) from (select sum(a) x from t_agg1 group by b, c);
explain (costs off) select count(*) from (select 2*d x, c+d y, a/10 z from t_agg1) group by x, y, z;
select count(*) from (select 2*d x, c+d y, a/10 z from t_agg1) group by x, y, z order by x, y, z limit 10;

-- (2) distinct clause
explain (costs off) select count(*) from (select distinct(c) from t_agg1);
select count(*) from (select distinct(c) from t_agg1);
explain (costs off) select count(*) from (select distinct(c*d) from t_agg1);
select count(*) from (select distinct(c*d) from t_agg1);

reset current_schema;
drop schema if exists distribute_aggregates_part2 cascade;
