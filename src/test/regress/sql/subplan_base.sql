/*
 * This file is used to test pull down of subplan expressions
 */
create schema distribute_subplan_base;
set current_schema = distribute_subplan_base;
-- Create Table and Insert Data
create table t_subplan1(a1 int, b1 int, c1 int, d1 int) with (autovacuum_enabled= false)  ;
create table t_subplan2(a2 int, b2 int, c2 int, d2 int) with (autovacuum_enabled= false)  ;
insert into t_subplan1 select generate_series(1, 100)%98, generate_series(1, 100)%20, generate_series(1, 100)%13, generate_series(1, 100)%6;
insert into t_subplan2 select generate_series(1, 50)%48, generate_series(1, 50)%28, generate_series(1, 50)%12, generate_series(1, 50)%9;

--subplan correlation limit clause, correlation having clause
explain (costs off) select a1 from t_subplan1 as t1 where t1.a1 = (select t2.a2 from t_subplan2 as t2 limit t1.b1);
explain (costs off) select a1 from t_subplan1 as t1 where t1.a1 = (select t2.a2 from t_subplan2 as t2 group by t2.a2 having t1.b1 > 10);

-- 1. initplan
explain (costs off)
select case when (select count(*)
                  from t_subplan2
                  where a2 between 1 and 20) > 15
            then (select avg(b2)
                  from t_subplan2
                  where a2 between 1 and 20)
            else (select avg(c2)
                  from t_subplan2
                  where a2 between 1 and 20) end bucket1 ,
       case when (select count(*)
                  from t_subplan2
                  where a2 between 1 and 20) > 25
             then (select avg(b2)
                  from t_subplan2
                  where a2 between 1 and 20)
            else (select avg(c2)
                  from t_subplan2
                  where a2 between 1 and 20) end bucket2
from t_subplan1
where a1 = 5 or a1 = 6
;

select case when (select count(*)
                  from t_subplan2
                  where a2 between 1 and 20) > 15
            then (select avg(b2)
                  from t_subplan2
                  where a2 between 1 and 20)
            else (select avg(c2)
                  from t_subplan2
                  where a2 between 1 and 20) end bucket1 ,
       case when (select count(*)
                  from t_subplan2
                  where a2 between 1 and 20) > 25
             then (select avg(b2)
                  from t_subplan2
                  where a2 between 1 and 20)
            else (select avg(c2)
                  from t_subplan2
                  where a2 between 1 and 20) end bucket2
from t_subplan1
where a1 = 5 or a1 = 6
;

explain (costs off)
select  a1, count(*) cnt
 from t_subplan1
 ,t_subplan2
 where a1 = a2
		and c2 >
             (select (avg (d1))
              from t_subplan1 t1
               where a1 > 
					(select avg(a2)
					from t_subplan2 t2))
 group by a1
 order by a1, cnt
 limit 10;

select  a1, count(*) cnt
 from t_subplan1
 ,t_subplan2
 where a1 = a2
		and c2 >
             (select (avg (d1))
              from t_subplan1 t1
               where a1 > 
					(select avg(a2)
					from t_subplan2 t2))
 group by a1
 order by a1, cnt
 limit 10;

explain (costs off)
select  a1, count(*) cnt
 from t_subplan1
 ,t_subplan2
 where a1 = a2
		and c2 >
             (select (avg (d1))
              from t_subplan1 t1
               where a1 > 
					(select avg(a2)
					from t_subplan2 t2))
 group by a1
 order by a1, cnt
 offset (select avg(d2) from t_subplan2);

select  a1, count(*) cnt
 from t_subplan1
 ,t_subplan2
 where a1 = a2
		and c2 >
             (select (avg (d1))
              from t_subplan1 t1
               where a1 > 
					(select avg(a2)
					from t_subplan2 t2))
 group by a1
 order by a1, cnt
 offset (select avg(d2) from t_subplan2);

-- 2. subplan
explain (costs off)
with t as
(select d1
,d2
,sum(c1+c2) as total
from t_subplan1
,t_subplan2
where a1 = a2
group by d1
,d2)
 select  total
from t ctr1
where ctr1.total > (select avg(total)*1.2
from t ctr2
where ctr1.d2 = ctr2.d2)
order by 1
limit 10;

with t as
(select d1
,d2
,sum(c1+c2) as total
from t_subplan1
,t_subplan2
where a1 = a2
group by d1
,d2)
 select  total
from t ctr1
where ctr1.total > (select avg(total)*1.2
from t ctr2
where ctr1.d2 = ctr2.d2)
order by 1
limit 10;

explain (costs off)
with t as
(select d1
,d2
,sum(c1+c2) as total
from t_subplan1
,t_subplan2
where a1 = a2
group by d1
,d2)
 select  total
from t ctr1
where ctr1.total > (select avg(total)*1.2
from t ctr2
where ctr1.d2 = ctr2.d2
and ctr1.d2+ctr2.d2 < (select avg(total)*3
from t ctr3
where ctr2.d2=ctr3.d2))
order by 1
limit 10;

with t as
(select d1
,d2
,sum(c1+c2) as total
from t_subplan1
,t_subplan2
where a1 = a2
group by d1
,d2)
 select  total
from t ctr1
where ctr1.total > (select avg(total)*1.2
from t ctr2
where ctr1.d2 = ctr2.d2
and ctr1.d2+ctr2.d2 < (select avg(total)*3
from t ctr3
where ctr2.d2=ctr3.d2))
order by 1
limit 10;

explain (costs off)
with t as
(select d1
,d2
,sum(c1+c2) as total
from t_subplan1
,t_subplan2
where a1 = a2
group by d1
,d2)
 select  total
from t ctr1
where ctr1.total > (select avg(total)*1.2
from t ctr2
where ctr1.d2 = ctr2.d2
and ctr1.d2+ctr2.d2 < (select avg(total)*3
from t ctr3
where ctr2.d2=ctr3.d2
and ctr1.d1 = ctr3.d1))
order by 1
limit 10;

with t as
(select d1
,d2
,sum(c1+c2) as total
from t_subplan1
,t_subplan2
where a1 = a2
group by d1
,d2)
 select  total
from t ctr1
where ctr1.total > (select avg(total)*1.2
from t ctr2
where ctr1.d2 = ctr2.d2
and ctr1.d2+ctr2.d2 < (select avg(total)*3
from t ctr3
where ctr2.d2=ctr3.d2
and ctr1.d1 = ctr3.d1))
order by 1
limit 10;

explain (costs off)
select * from t_subplan1 t1
where
 exists (select * from t_subplan2 t2
	where t1.a1=t2.a2) and
 (exists (select * from t_subplan2 t2
	where t1.b1+20=t2.b2) or
  exists (select * from t_subplan2 t2
	where t1.c1 = t2.c2))
order by 1,2,3,4;
;

select * from t_subplan1 t1
where
 exists (select * from t_subplan2 t2
	where t1.a1=t2.a2) and
 (exists (select * from t_subplan2 t2
	where t1.b1+20=t2.b2) or
  exists (select * from t_subplan2 t2
	where t1.c1 = t2.c2))
order by 1,2,3,4;
;

-- 3. initplan & subplan
explain (costs off)
select  a1, count(*) cnt
 from t_subplan1
 ,t_subplan2
 where a1 = a2
		and c2 >
             (select avg (d1)
              from t_subplan1
               where a1+b1<200 )
        and b1 > 1.2 *
             (select avg(b2)
             from t_subplan2 t2
             where t2.c2=t_subplan1.c1)
 group by a1
 order by a1, cnt
 limit 10;
 
select  a1, count(*) cnt
 from t_subplan1
 ,t_subplan2
 where a1 = a2
		and c2 >
             (select avg (d1)
              from t_subplan1
               where a1+b1<200 )
        and b1 > 1.2 *
             (select avg(b2)
             from t_subplan2 t2
             where t2.c2=t_subplan1.c1)
 group by a1
 order by a1, cnt
 limit 10;
 
explain (costs off)
select  a1, count(*) cnt
 from t_subplan1
 ,t_subplan2
 where a1 = a2
		and c2 >
             (select avg (d1)
              from t_subplan1
               where a1+b1<200 )
        and b1 > 1.2 *
             (select avg(b2)
             from t_subplan2 t2
             where t2.c2=t_subplan1.c1 and
			 t_subplan1.d1 < (select max(d2) from t_subplan2))
 group by a1
 order by a1, cnt
 limit 10;

select  a1, count(*) cnt
 from t_subplan1
 ,t_subplan2
 where a1 = a2
		and c2 >
             (select avg (d1)
              from t_subplan1
               where a1+b1<200 )
        and b1 > 1.2 *
             (select avg(b2)
             from t_subplan2 t2
             where t2.c2=t_subplan1.c1 and
			 t_subplan1.d1 < (select max(d2) from t_subplan2))
 group by a1
 order by a1, cnt
 limit 10;

explain (costs off)
select  a1, count(*) cnt
 from t_subplan1
 ,t_subplan2
 where a1 = a2
		and c2 >
             (select avg (d1)
              from t_subplan1
               where a1+b1<200 )
        and b1 > 1.2 *
             (select avg(b2)
             from t_subplan2 t2
             where t2.c2=t_subplan1.c1 and
			 t2.d2 > (select min(d1) from t_subplan1))
 group by a1
 order by a1, cnt
 limit 10;

select  a1, count(*) cnt
 from t_subplan1
 ,t_subplan2
 where a1 = a2
		and c2 >
             (select avg (d1)
              from t_subplan1
               where a1+b1<200 )
        and b1 > 1.2 *
             (select avg(b2)
             from t_subplan2 t2
             where t2.c2=t_subplan1.c1 and
			 t2.d2 > (select min(d1) from t_subplan1))
 group by a1
 order by a1, cnt
 limit 10;

explain (costs off)
select  a1, count(*) cnt
 from t_subplan1
 ,t_subplan2
 where a1 = a2
		and c2 >
             (select (avg (d1))
              from t_subplan1 t1
               where a1 > 
					(select avg(a2)
					from t_subplan2 t2
					where t1.d1=t2.d2))
 group by a1
 order by a1, cnt
 limit 10;

select  a1, count(*) cnt
 from t_subplan1
 ,t_subplan2
 where a1 = a2
		and c2 >
             (select (avg (d1))
              from t_subplan1 t1
               where a1 > 
					(select avg(a2)
					from t_subplan2 t2
					where t1.d1=t2.d2))
 group by a1
 order by a1, cnt
 limit 10;

-- setop
explain (costs off)
select * from t_subplan1 where a1 in (select a2 from t_subplan2 where b2=b1 intersect select c2 from t_subplan2) order by 1, 2 limit 5;

select * from t_subplan1 where a1 in (select a2 from t_subplan2 where b2=b1 intersect select c2 from t_subplan2) order by 1, 2 limit 5;

explain (costs off)
select * from t_subplan1 where a1 in (select a2 from t_subplan2 where b2=b1 union all select c2 from t_subplan2) order by 1, 2 limit 5;

select * from t_subplan1 where a1 in (select a2 from t_subplan2 where b2=b1 union all select c2 from t_subplan2) order by 1, 2 limit 5;

explain (costs off)
select * from t_subplan1 where a1 in (select a2 from t_subplan2 where b2=b1 union all select c2 from t_subplan2 where d2=c1) order by 1, 2 limit 5;

select * from t_subplan1 where a1 in (select a2 from t_subplan2 where b2=b1 union all select c2 from t_subplan2 where d2=c1) order by 1, 2 limit 5;

-- other sdv failed case
create table t_subplan3(a3 int, b3 int, primary key(a3))   partition by range(a3) (partition p1 values less than (25), partition p2 values less than (maxvalue));
create table t_subplan4(a4 int, b4 int)  ;
insert into t_subplan3 values(1, 20);
insert into t_subplan3 values(27, 27);
insert into t_subplan4 values(1, 20);
insert into t_subplan4 values(27, 27);
explain (costs off)
delete t_subplan3 where b3=(select max(b2) from t_subplan2);

delete t_subplan3 where b3=(select max(b2) from t_subplan2);
select * from t_subplan3;

explain (costs off)
update t_subplan4 set b4=(select a2 from t_subplan2 where a2=40);

update t_subplan4 set b4=(select a2 from t_subplan2 where a2=40);
select * from t_subplan4;

explain (costs off)
select max(a3) from t_subplan3 where (a3, b3)=(select a3, b3 from t_subplan3) and a3=1;

select max(a3) from t_subplan3 where (a3, b3)=(select a3, b3 from t_subplan3) and a3=1;

delete from t_subplan4 where a4=27;
explain (costs off)
select max(a4) from t_subplan4 where (a4, b4)=(select a4, b4 from t_subplan4) and a4=1;

select max(a4) from t_subplan4 where (a4, b4)=(select a4, b4 from t_subplan4) and a4=1;

explain (costs off)
select * from t_subplan2 where (b2, c2) = (select b3, a3+7 from t_subplan3) or (b2, d2) = (select b4/2, a4+2 from t_subplan4) order by 1, 2, 3, 4;

select * from t_subplan2 where (b2, c2) = (select b3, a3+7 from t_subplan3) or (b2, d2) = (select b4/2, a4+2 from t_subplan4) order by 1, 2, 3, 4;

explain (costs off)
with recursive abc as (select * from t_subplan2)
update t_subplan3 set b3=(select max(b2) from t_subplan2) from abc where abc.b2=b3;

with recursive abc as (select * from t_subplan2)
update t_subplan3 set b3=(select max(b2) from t_subplan2) from abc where abc.b2=b3;

explain (costs off)
SELECT ((SELECT PG_DATABASE_SIZE((SELECT OID FROM PG_DATABASE WHERE upper(DATNAME)='POSTGRES'))) = (SELECT PG_DATABASE_SIZE((SELECT OID FROM PG_DATABASE WHERE upper(DATNAME)='REGRESSION'))));

SELECT ((SELECT PG_DATABASE_SIZE((SELECT OID FROM PG_DATABASE WHERE upper(DATNAME)='POSTGRES'))) = (SELECT PG_DATABASE_SIZE((SELECT OID FROM PG_DATABASE WHERE upper(DATNAME)='REGRESSION'))));

create table t1(a int, b int, primary key(a, b))  ;
create table t2(a int, b int);
insert into t1 select generate_series(1, 10), generate_series(1, 10);
insert into t2 values(1, 5), (2, 15);

explain (costs off)
with abc as (select a, b from t2)
update t1 set b=(select b from t2 where a=2) from abc where abc.a=1 and abc.a=t1.a;

with abc as (select a, b from t2)
update t1 set b=(select b from t2 where a=2) from abc where abc.a=1 and abc.a=t1.a;

select * from t1 order by 1,2;

explain (costs off)
select * from t2 where a <= all (select a from t2);

select * from t2 where a <= all (select a from t2);

explain (costs off, verbose on)
select c1, d1 from t_subplan1
where exists(select b from t2 where a>=2)
group by c1, d1 order by c1+1 desc, 2 desc limit 5;

select c1, d1 from t_subplan1
where exists(select b from t2 where a>=2)
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
 
explain (costs off, verbose on)
select b1, count(*) from t_subplan1
where c1 = any (select b2 from t_subplan2 where b2>4)
or d1 != any (select c2 from t_subplan2 where c2>10)
group by b1 order by 1, 2 limit 5;

select b1, count(*) from t_subplan1
where c1 = any (select b2 from t_subplan2 where b2>4)
or d1 != any (select c2 from t_subplan2 where c2>10)
group by b1 order by 1, 2 limit 5;
 
explain (costs off, verbose on)
select * from t_subplan1 where (10,15)<=(select b1, min(b2) from t_subplan2 group by b1) order by a1, b1, c1, d1;

select * from t_subplan1 where (10,15)<=(select b1, min(b2) from t_subplan2 group by b1) order by a1, b1, c1, d1;

explain (costs off, verbose on)
select * from t_subplan1 where (b1,c1) < (select a2, b2 from t_subplan2 where b2=4 and a2=4) order by a1, b1, c1, d1;

select * from t_subplan1 where (b1,c1) < (select a2, b2 from t_subplan2 where b2=4 and a2=4) order by a1, b1, c1, d1;

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
select (with cte(foo) as (select a1 from dual) select foo from cte) from t_subplan1 order by 1 limit 3;
select (with cte(foo) as (select a1 from dual) select foo from cte) from t_subplan1 order by 1 limit 3;
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

explain (costs off, verbose on)
select * from t_subplan2 where (select 200 from t_subplan1 group by 1 limit 1) not in (select a1 from t_subplan1 order by 1)
order by 1,2,3,4 limit 5;

select * from t_subplan2 where (select 200 from t_subplan1 group by 1 limit 1) not in (select a1 from t_subplan1 order by 1)
order by 1,2,3,4 limit 5;

-- update
explain (costs off, verbose on)
update t_subplan2 set d2 = t1.b from (select max(a1) b from t_subplan1 group by c1 not in (select a1*0-1 from t_subplan1)) t1;

update t_subplan2 set d2 = t1.b from (select max(a1) b from t_subplan1 group by c1 not in (select a1*0-1 from t_subplan1)) t1;

drop schema distribute_subplan_base cascade;
