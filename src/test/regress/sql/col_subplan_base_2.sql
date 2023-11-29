/*
 * This file is used to test pull down of subplan expressions
 */
create schema col_distribute_subplan_base_2;
set current_schema = col_distribute_subplan_base_2;
-- Create Table and Insert Data
create table t_subplan1(a1 int, b1 int, c1 int, d1 int) with (orientation = column)  ;
create table t_subplan2(a2 int, b2 int, c2 int, d2 int) with (orientation = column)  ;
insert into t_subplan1 select generate_series(1, 100)%98, generate_series(1, 100)%20, generate_series(1, 100)%13, generate_series(1, 100)%6;
insert into t_subplan2 select generate_series(1, 50)%48, generate_series(1, 50)%28, generate_series(1, 50)%12, generate_series(1, 50)%9;

create table t_subplan5 with (orientation = column) as select * from t_subplan1;
create table t_subplan6 with (orientation = column) as select * from t_subplan2;

--create row table
create table t_subplan7 as select * from t_subplan1;

-- other sdv failed case
create table t_subplan3(a3 int, b3 int) with (orientation=column)  
partition by range(a3) (partition p1 values less than (25), partition p2 values less than (maxvalue));
insert into t_subplan3 values(1, 20);
insert into t_subplan3 values(27, 27);
explain (costs off)
select * from t_subplan3 where b3=(select max(b2) from t_subplan2);

select * from t_subplan3 where b3=(select max(b2) from t_subplan2);

explain (costs off)
select count(*) from t_subplan3 t1 group by a3,b3
having not exists
(select sum(t2.b3), t2.a3, t2.b3, rank() over (partition by t1.b3 order by t2.a3) from t_subplan3 t2
group by 2,3 order by 1,2,3,4);

select count(*) from t_subplan3 t1 group by a3,b3
having not exists
(select sum(t2.b3), t2.a3, t2.b3, rank() over (partition by t1.b3 order by t2.a3) from t_subplan3 t2
group by 2,3 order by 1,2,3,4);

explain (costs off, verbose on)
select c1, d1 from t_subplan1
where exists(select b3 from t_subplan3 where a3>=2)
group by c1, d1 order by c1+1 desc, 2 desc limit 5;

select c1, d1 from t_subplan1
where exists(select b3 from t_subplan3 where a3>=2)
group by c1, d1 order by c1+1 desc, 2 desc limit 5;

explain (costs off, verbose on)
select * from t_subplan2 where
 exists (select d1 from t_subplan1 where d1<8 and
  exists (select b1 from t_subplan1 where c1<20 and
   exists (select * from t_subplan1 where d1<9 and d1 >1 order by d1 limit 7) order by c1,b1 limit 10))
and exists( select max(a1),count(b1),c2 from t_subplan1 group by c2 having c2>2 or c2 is null)
order by a2, b2, c2, d2 limit 10;

select * from t_subplan2 where
 exists (select d1 from t_subplan1 where d1<8 and
  exists (select b1 from t_subplan1 where c1<20 and
   exists (select * from t_subplan1 where d1<9 and d1 >1 order by d1 limit 7) order by c1,b1 limit 10))
and exists( select max(a1),count(b1),c2 from t_subplan1 group by c2 having c2>2 or c2 is null)
order by a2, b2, c2, d2 limit 10;

explain (costs off, verbose on)
select * from t_subplan2 where
 exists( select max(a1),count(b1),c2 from t_subplan1 group by rollup(c2,c2) having c2>2 and c2 is not null)
order by a2,b2,c2,d2 limit 10;

select * from t_subplan2 where
 exists( select max(a1),count(b1),c2 from t_subplan1 group by rollup(c2,c2) having c2>2 and c2 is not null)
order by a2,b2,c2,d2 limit 10;

-- test any, all, rowcompare, array sublink
explain (costs off, verbose on)
select * from t_subplan1 where a1 in (select count(c2) from t_subplan2) or d1=0 order by 1,2,3,4;

select * from t_subplan1 where a1 in (select count(c2) from t_subplan2) or d1=0 order by 1,2,3,4;

explain (costs off, verbose on)
select * from t_subplan1 where a1 in (select count(c2) from t_subplan2 where c1>d2 minus all select d1 from t_subplan2) or d1=0 order by 1,2,3,4;

select * from t_subplan1 where a1 in (select count(c2) from t_subplan2 where c1>d2 minus all select d1 from t_subplan2) or d1=0 order by 1,2,3,4;

explain (costs off, verbose on)
select * from t_subplan1 where a1 in (select count(c2)::int from t_subplan2 where c1>d2 union all select d1 from t_subplan2) or d1=0 order by 1,2,3,4 limit 5;

select * from t_subplan1 where a1 in (select count(c2)::int from t_subplan2 where c1>d2 union all select d1 from t_subplan2) or d1=0 order by 1,2,3,4 limit 5;

explain (costs off, verbose on)
select b1, count(*) from t_subplan1
where c1 = all (select b2 from t_subplan2 where b2>4)
or d1 != all (select c2 from t_subplan2 where c2>10)
group by b1 order by 1, 2 limit 5;

select b1, count(*) from t_subplan1
where c1 = all (select b2 from t_subplan2 where b2>4)
or d1 != all (select c2 from t_subplan2 where c2>10)
group by b1 order by 1, 2 limit 5;
 
select b1, count(*) from t_subplan5
where c1 = all (select b2 from t_subplan6 where b2>4)
or d1 != all (select c2 from t_subplan6 where c2>10)
group by b1 order by 1, 2 limit 5;

explain (costs off, verbose on)
select b1, count(*) from t_subplan1
where c1 = any (select b2 from t_subplan2 where b2>4)
or d1 != any (select c2 from t_subplan2 where c2>10)
group by b1 order by 1, 2 limit 5;

select b1, count(*) from t_subplan1
where c1 = any (select b2 from t_subplan2 where b2>4)
or d1 != any (select c2 from t_subplan2 where c2>10)
group by b1 order by 1, 2 limit 5;

select b1, count(*) from t_subplan5
where c1 = any (select b2 from t_subplan6 where b2>4)
or d1 != any (select c2 from t_subplan6 where c2>10)
group by b1 order by 1, 2 limit 5;

select b1, count(*) from t_subplan5
where (c1 > 10 and c1 = any (select b2 from t_subplan6 where b2>4))
or d1 != any (select c2 from t_subplan6 where c2>10)
group by b1 order by 1, 2 limit 5;

explain (costs off, verbose on)
select * from t_subplan1 where (10,15)<=(select b1, min(b2) from t_subplan2 group by b1) order by a1, b1, c1, d1;

select * from t_subplan1 where (10,15)<=(select b1, min(b2) from t_subplan2 group by b1) order by a1, b1, c1, d1;
select * from t_subplan5 where (10,15)<=(select b1, min(b2) from t_subplan6 group by b1) order by a1, b1, c1, d1;
explain (costs off, verbose on)
select * from t_subplan1 where (b1,c1) < (select a2, b2 from t_subplan2 where b2=4 and a2=4) order by a1, b1, c1, d1;

select * from t_subplan1 where (b1,c1) < (select a2, b2 from t_subplan2 where b2=4 and a2=4) order by a1, b1, c1, d1;
select * from t_subplan5 where (b1,c1) < (select a2, b2 from t_subplan6 where b2=4 and a2=4) order by a1, b1, c1, d1;
explain (costs off, verbose on)
select * from t_subplan1 where array(select max(b2) from t_subplan2 group by b1)=array(select min(b2) from t_subplan2 group by b1);

select * from t_subplan1 where array(select max(b2) from t_subplan2 group by b1)=array(select min(b2) from t_subplan2 group by b1);

explain (costs off, verbose on)
select array(select max(b2) from t_subplan2 group by b1) from t_subplan1 order by a1, b1, c1, d1;

select array(select max(b2) from t_subplan2 group by b1) from t_subplan1 order by a1, b1, c1, d1;

explain (costs off, verbose on)
select array(select a1 from t_subplan1 where t_subplan1.b1=t_subplan2.b2 order by 1) from t_subplan2 order by a2, b2, c2, d2;

select array(select a1 from t_subplan1 where t_subplan1.b1=t_subplan2.b2 order by 1) from t_subplan2 order by a2, b2, c2, d2;

-- test cte sublink
explain (costs off, verbose on)
select (with cte(foo) as (select a1) select foo from cte) from t_subplan1 order by 1 limit 3;
select (with cte(foo) as (select a1) select foo from cte) from t_subplan1 order by 1 limit 3;
explain (costs off, verbose on)
select (with cte(foo) as (values(b1)) values((select foo from cte))) from t_subplan1 order by 1 limit 3;
select (with cte(foo) as (values(b1)) values((select foo from cte))) from t_subplan1 order by 1 limit 3;
explain (costs off, verbose on)
select (with cte(foo) as (select avg(a1) from t_subplan1) select foo from cte) from t_subplan1 order by 1 limit 3;
select (with cte(foo) as (select avg(a1) from t_subplan1) select foo from cte) from t_subplan1 order by 1 limit 3;
explain (costs off, verbose on)
select (with cte(foo) as (select t_subplan1.b1 from t_subplan2 limit 1) select foo from cte) from t_subplan1 order by 1 limit 3;
select (with cte(foo) as (select t_subplan1.b1 from t_subplan2 limit 1) select foo from cte) from t_subplan1 order by 1 limit 3;
explain (costs off, verbose on)
select (with cte(foo) as (select t_subplan1.b1 from t_subplan2 limit 1) select foo+t_subplan1.c1 from cte) from t_subplan1 order by 1 limit 3; 
select (with cte(foo) as (select t_subplan1.b1 from t_subplan2 limit 1) select foo+t_subplan1.c1 from cte) from t_subplan1 order by 1 limit 3; 
explain (costs off, verbose on)
select (with cte(foo) as (select t_subplan1.b1 from t_subplan2 limit 1) values((select foo from cte))) from t_subplan1 order by 1 limit 3;
select (with cte(foo) as (select t_subplan1.b1 from t_subplan2 limit 1) values((select foo from cte))) from t_subplan1 order by 1 limit 3;
explain (costs off, verbose on)
select (with cte(foo) as (values(b1)) select foo from cte) from t_subplan1 order by 1 limit 3;
select (with cte(foo) as (values(b1)) select foo from cte) from t_subplan1 order by 1 limit 3;
explain (costs off, verbose on)
select (select b1 from (values((select b1 from t_subplan2 limit 1), (select a1 from t_subplan2 limit 1))) as t(c,d)) from t_subplan1 order by 1 limit 3; 
select (select b1 from (values((select b1 from t_subplan2 limit 1), (select a1 from t_subplan2 limit 1))) as t(c,d)) from t_subplan1 order by 1 limit 3; 

-- test cte sublink applied in different subquery level
explain (costs off, verbose on)
select * from t_subplan1 where c1 = (with tmp as (select d2 from t_subplan2 where b2=a1)
 select count(*) from (select 'abc', count(d2) from tmp))
 order by 1,2,3,4;

select * from t_subplan1 where c1 = (with tmp as (select d2 from t_subplan2 where b2=a1)
 select count(*) from (select 'abc', count(d2) from tmp))
 order by 1,2,3,4;

explain (costs off, verbose on)
select * from t_subplan1 where a1 = (with tmp as (select d2 from t_subplan2 where b2=a1)
 select count(*) from (select 'abc', d2 from tmp))
 order by 1,2,3,4;

select * from t_subplan1 where a1 = (with tmp as (select d2 from t_subplan2 where b2=a1)
 select count(*) from (select 'abc', d2 from tmp))
 order by 1,2,3,4;

explain (costs off, verbose on)
select * from t_subplan1 where c1 = (with tmp as (select d2 from t_subplan2 where b2=a1)
 select count(*) from (select 'abc', count(d2) from tmp tmp1
 where d2>(select count(*) from tmp tmp2 where tmp2.d2=tmp1.d2)))
 order by 1,2,3,4;

select * from t_subplan1 where c1 = (with tmp as (select d2 from t_subplan2 where b2=a1)
 select count(*) from (select 'abc', count(d2) from tmp tmp1
 where d2>(select count(*) from tmp tmp2 where tmp2.d2=tmp1.d2)))
 order by 1,2,3,4;

explain (costs off, verbose on)
select * from t_subplan1 where c1 = (with tmp as (select d2 from t_subplan2 where b2=a1)
 select count(*) from (select 'abc', d2 from tmp tmp1
 where d2>(select count(*) from tmp tmp2 where tmp2.d2=tmp1.d2)))
 order by 1,2,3,4;

select * from t_subplan1 where c1 = (with tmp as (select d2 from t_subplan2 where b2=a1)
 select count(*) from (select 'abc', d2 from tmp tmp1
 where d2>(select count(*) from tmp tmp2 where tmp2.d2=tmp1.d2)))
 order by 1,2,3,4;

explain (costs off, verbose on)
select * from t_subplan1 left join
(select a2, b2, (select b1 from t_subplan1 limit 1) c2 from t_subplan2)
on b2=b1 and a1 not in (null)
inner join t_subplan1 t3 on t3.a1=t_subplan1.c1 where t3.a1=0 order by 1,2,3,4 limit 5;

select * from t_subplan1 left join
(select a2, b2, (select b1 from t_subplan1 limit 1) c2 from t_subplan2)
on b2=b1 and a1 not in (null)
inner join t_subplan1 t3 on t3.a1=t_subplan1.c1 where t3.a1=0 order by 1,2,3,4 limit 5;

explain (costs off, verbose on)
select count(*) from t_subplan2 group by a2,b2 order by (a2,b2) > some(select min(a1), length(trim(b2)) from t_subplan1), 1;

select count(*) from t_subplan2 group by a2,b2 order by (a2,b2) > some(select min(a1), length(trim(b2)) from t_subplan1), 1 limit 10;

-- update
explain (costs off, verbose on)
update t_subplan2 set d2 = t1.b from (select max(a1) b from t_subplan1 group by c1 not in (select a1*0-1 from t_subplan1)) t1;

update t_subplan2 set d2 = t1.b from (select max(a1) b from t_subplan1 group by c1 not in (select a1*0-1 from t_subplan1)) t1;

explain (costs off, verbose on) 
select count(*) from t_subplan1 group by a1 having(avg(b1) = some (select b2 from t_subplan2)) order by 1 limit 5;
select count(*) from t_subplan1 group by a1 having(avg(b1) = some (select b2 from t_subplan2)) order by 1 limit 5;

explain (costs off, verbose on) 
select max(a1), b1 = some (select b2 from t_subplan2) from t_subplan1 group by b1 order by 1,2 limit 5;
select max(a1), b1 = some (select b2 from t_subplan2) from t_subplan1 group by b1 order by 1,2 limit 5;

explain (costs off, verbose on) 
select max(a1), min(b1) = some (select b2 from t_subplan2) from t_subplan1 group by b1 order by 1,2 limit 5;
select max(a1), min(b1) = some (select b2 from t_subplan2) from t_subplan1 group by b1 order by 1,2 limit 5;

explain (costs off, verbose on) 
select a1, b1 from t_subplan1 group by a1, b1 having(grouping(b1)) = some (select b2 from t_subplan2) order by 1,2 limit 5;
select a1, b1 from t_subplan1 group by a1, b1 having(grouping(b1)) = some (select b2 from t_subplan2) order by 1,2 limit 5;

explain (costs off, verbose on) 
select a1, rank() over(partition by a1)  = some (select a2  from t_subplan2) from t_subplan1 order by 1,2 limit 5;
select a1, rank() over(partition by a1)  = some (select a2  from t_subplan2) from t_subplan1 order by 1,2 limit 5;



explain (costs off, verbose on) 
select * from t_subplan7 t1 where a1 in (select t1.a1 - 1 from t_subplan1);
select * from t_subplan7 t1 where a1 in (select t1.a1 - 1 from t_subplan1);

set work_mem = '1MB';
set enable_nestloop = off;
set enable_hashjoin = off;
explain (costs off, verbose on) 
select * from t_subplan1 where c1 > any(select c2 from t_subplan2 join t_subplan3 on a3 = a2 where b2 < b1) order by 1,2,3,4 limit 10;
select * from t_subplan1 where c1 > any(select c2 from t_subplan2 join t_subplan3 on a3 = a2 where b2 < b1) order by 1,2,3,4 limit 10;

set enable_mergejoin=off;
set enable_hashjoin = on;
explain (costs off, verbose on) 
select * from t_subplan1 where c1 > any(select c2 from t_subplan2 join t_subplan3 on a3 = a2 where b2 < b1) order by 1,2,3,4 limit 10;
select * from t_subplan1 where c1 > any(select c2 from t_subplan2 join t_subplan3 on a3 = a2 where b2 < b1) order by 1,2,3,4 limit 10;

explain (costs off, verbose on)
select b1, count(*) from t_subplan1 where c1 = all (select b2 from t_subplan2 where b2 != c1) group by b1 order by 1, 2 limit 5;
select b1, count(*) from t_subplan1 where c1 = all (select b2 from t_subplan2 where b2 != c1) group by b1 order by 1, 2 limit 5;

set enable_seqscan=off;
create index t_subplan2_idx on t_subplan2(a2, b2);
explain (verbose on, costs off)
select * from t_subplan1 t1 where exists (select 1 from t_subplan1 t2 join t_subplan2 t3 on t2.a1=t3.b2 and t1.b1=t3.a2);

select * from t_subplan1 t1 where exists (select 1 from t_subplan1 t2 join t_subplan2 t3 on t2.a1=t3.b2 and t1.b1=t3.a2) order by 1,2,3,4 limit 5 offset 5;

drop schema col_distribute_subplan_base_2 cascade;
