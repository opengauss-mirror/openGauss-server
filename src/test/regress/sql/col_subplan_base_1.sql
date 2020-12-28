/*
 * This file is used to test pull down of subplan expressions
 */
create schema col_distribute_subplan_base;
set current_schema = col_distribute_subplan_base;
-- Create Table and Insert Data
create table t_subplan1(a1 int, b1 int, c1 int, d1 int) with (orientation = column)  ;
create table t_subplan2(a2 int, b2 int, c2 int, d2 int) with (orientation = column)  ;
insert into t_subplan1 select generate_series(1, 100)%98, generate_series(1, 100)%20, generate_series(1, 100)%13, generate_series(1, 100)%6 from public.src;
insert into t_subplan2 select generate_series(1, 50)%48, generate_series(1, 50)%28, generate_series(1, 50)%12, generate_series(1, 50)%9 from public.src;

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

--test sonicagg rescan
set enable_sonic_hashagg=on;

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
where ctr1.total > (select avg(total)
from t ctr2
where ctr1.d2 = ctr2.d2
and ctr1.d2+ctr2.d2 < (select avg(total)
from t ctr3
where ctr2.d2=ctr3.d2  group by d1 order by 1 limit 1)
group by d1 order by 1 limit 1
)
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
where ctr1.total > (select avg(total)
from t ctr2
where ctr1.d2 = ctr2.d2
and ctr1.d2+ctr2.d2 < (select avg(total)
from t ctr3
where ctr2.d2=ctr3.d2  group by d1 order by 1 limit 1)
group by d1 order by 1 limit 1
)
order by 1
limit 10;

set enable_sonic_hashagg=off;

drop schema col_distribute_subplan_base cascade;
