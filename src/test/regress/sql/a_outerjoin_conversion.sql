create database pl_test_outerjoin DBCOMPATIBILITY 'pg';
\c pl_test_outerjoin;

create schema plus_outerjoin;
set current_schema='plus_outerjoin';
create table t11(c1 int, c2 int, c3 int);
create table t12(c1 int, c2 int, c3 int);
create table t13(c1 int, c2 int, c3 int);
create table t14(A4 int, B4 int, c4 int);
create table t_a (id int, name varchar(10), code int);
create table t_b (id int, name varchar(10), code int);

-- insert base
insert into t11 select v,v,v from generate_series(1,10) as v;
insert into t12 select * from t11;
insert into t13 select * from t11;
insert into t14 select v,v,v from generate_series(1,30) as v;
insert into t_a values (1, 'tom', 3);
insert into t_b values (1, 'bat', 6);

-- insert t11 t12, t13's not match values
insert into t11 select v,v,v from generate_series(11,15) as v;
insert into t12 select v,v,v from generate_series(16,20) as v;
insert into t13 select v,v,v from generate_series(21,25) as v;

/* "(+)" used in simple case: single join condition, two relation */
select * from t11, t12 where t11.c1    = t12.c2(+) order by 1,2,3,4,5,6;
explain(costs off)
select * from t11, t12 where t11.c1    = t12.c2(+) order by 1,2,3,4,5,6;

select * from t11, t12 where t11.c1(+) = t12.c2 order by 1,2,3,4,5,6;
explain(costs off)
select * from t11, t12 where t11.c1(+) = t12.c2 order by 1,2,3,4,5,6;

select * from t11, t12 where t11.c1 = t12.c1(+) and t11.c1 > 10 order by 1,2,3,4,5,6;
explain(costs off)
select * from t11, t12 where t11.c1 = t12.c1(+) and t11.c1 > 10 order by 1,2,3,4,5,6;

-- with "in" operator together--
select * from t11, t12 where t11.c1 = t12.c1(+) and t11.c1 in (select c3 from t13 where c1 > 8) order by 1,2,3,4,5,6;
explain(costs off)
select * from t11, t12 where t11.c1 = t12.c1(+) and t11.c1 in (select c3 from t13 where c1 > 8) order by 1,2,3,4,5,6;

select * from t11, t12 where t11.c1 = t12.c1(+) and t12.c1 in (select c3 from t13 where c1 > 8) order by 1,2,3,4,5,6;
explain(costs off)
select * from t11, t12 where t11.c1 = t12.c1(+) and t12.c1 in (select c3 from t13 where c1 > 8) order by 1,2,3,4,5,6;

/* multi-join-condition */
select * from t11, t12 where t11.c1(+) = t12.c1 and t11.c2(+) = t12.c2    order by 1,2,3,4,5,6;
explain(costs off)
select * from t11, t12 where t11.c1(+) = t12.c1 and t11.c2(+) = t12.c2    order by 1,2,3,4,5,6;

select * from t11, t12 where t11.c1 = t12.c1(+) and t11.c2 = t12.c2(+) order by 1,2,3,4,5,6;
explain(costs off)
select * from t11, t12 where t11.c1 = t12.c1(+) and t11.c2 = t12.c2(+) order by 1,2,3,4,5,6;

/* (+) used in multi-relation join with multi-join-condition */
select t14.c4, t11.c1, t12.c2, t13.c3
from t11,t12,t13,t14
where t11.c1(+) = t14.a4 and
      t12.c1(+) = t14.a4 and
      t13.c1(+) = t14.a4 and
      t14.b4 >= 8
order by t14.c4,t11.c1, t12.c2, t13.c3;

explain(costs off)
select t14.c4, t11.c1, t12.c2, t13.c3
from t11,t12,t13,t14
where t11.c1(+) = t14.a4 and
      t12.c1(+) = t14.a4 and
      t13.c1(+) = t14.a4 and
      t14.b4 >= 8
order by t14.c4,t11.c1, t12.c2, t13.c3;

select t14.c4, t11.c1, t12.c2, t13.c3
from t11,t12,t13,t14
where t11.c1(+) = t14.a4 and
      t14.a4  =  t12.c1(+)and
      t13.c1(+) = t14.a4 and
      t14.b4 >= 8
order by t14.c4;

explain(costs off)
select t14.c4, t11.c1, t12.c2, t13.c3
from t11,t12,t13,t14
where t11.c1(+) = t14.a4 and
      t14.a4  =  t12.c1(+)and
      t13.c1(+) = t14.a4 and
      t14.b4 >= 8
order by t14.c4,t11.c1, t12.c2, t13.c3;

/* (+) used in subquery */
select * from t14, (select t12.c1 as dt_col1, t13.c2 as dt_col2 from t12,t13 where t12.c1 = t13.c1(+)) dt where t14.a4 = dt.dt_col1 order by 1,2,3,4,5;

explain(costs off)
select * from t14, (select t12.c1 as dt_col1, t13.c2 as dt_col2 from t12,t13 where t12.c1 = t13.c1(+)) dt where t14.a4 = dt.dt_col1 order by 1,2,3,4,5;

/* (+) used in subLink */
select t11.c1,t11.c2, t12.c1, t12.c2
from t11, t12
where t11.c1 = t12.c1(+) and t11.c2 in (
    select t14.a4
    from t13, t14
    where t14.b4(+) = t13.c2
) order by 1,2,3,4;

explain (costs off)
select t11.c1,t11.c2, t12.c1, t12.c2
from t11, t12
where t11.c1 = t12.c1(+) and t11.c2 in (
    select t14.a4
    from t13, t14
    where t14.b4(+) = t13.c2
) order by 1,2,3,4;

select t11.c1,t11.c2, t12.c1, t12.c2
from t11, t12
where t11.c1 = t12.c1(+) and t11.c2 in (
    select t14.a4
    from t13, t14
    where t14.b4 = t13.c2(+)
) order by 1,2,3,4;

explain (costs off)
select t11.c1,t11.c2, t12.c1, t12.c2
from t11, t12
where t11.c1 = t12.c1(+) and t11.c2 in (
    select t14.a4
    from t13, t14
    where t14.b4 = t13.c2(+)
) order by 1,2,3,4;

select t11.c1,t11.c2, t12.c1, t12.c2
from t11, t12
where t11.c1 = t12.c1(+) and t11.c2 in (
    select t14.a4
    from t13, t14
    where t14.b4 = t13.c2
) order by 1,2,3,4;

explain (costs off)
select t11.c1,t11.c2, t12.c1, t12.c2
from t11, t12
where t11.c1 = t12.c1(+) and t11.c2 in (
    select t14.a4
    from t13, t14
    where t14.b4 = t13.c2
) order by 1,2,3,4;

/* (+) used in subquery-RTE join with relation-RTE */
select *
from t14, (
	select t12.c1 as dt_col1, t13.c2 as dt_col2
	from t12,t13
	where t12.c1 = t13.c1(+)
) dt where t14.a4 = dt.dt_col1(+) order by 1,2,3,4;

explain (costs off)
select *
from t14, (
	select t12.c1 as dt_col1, t13.c2 as dt_col2
	from t12,t13
	where t12.c1 = t13.c1(+)
) dt where t14.a4 = dt.dt_col1(+) order by 1,2,3,4;

select *
from t14, (
	select t12.c1 as dt_col1, t13.c2 as dt_col2
	from t12,t13
	where t12.c1 = t13.c1(+)
) dt where t14.a4(+) = dt.dt_col1 order by 1,2,3,4;

explain (costs off)
select *
from t14, (
	select t12.c1 as dt_col1, t13.c2 as dt_col2
	from t12,t13
	where t12.c1 = t13.c1(+)
) dt where t14.a4(+) = dt.dt_col1 order by 1,2,3,4;

/* (+) used in setop */
select *
from
(
	select t11.c1,t11.c2 from t11, t12 where t11.c1 = t12.c1(+)
	union all
	select t11.c1,t11.c2 from t11, t12 where t11.c1(+) = t12.c1
) order by 1,2;

explain (costs off)
select *
from
(
    select t11.c1,t11.c2 from t11, t12 where t11.c1 = t12.c1(+)
    union all
    select t11.c1,t11.c2 from t11, t12 where t11.c1(+) = t12.c1
) order by 1,2;


select *
from
(
    select t11.c1,t11.c2 from t11, t12 where t11.c1 = t12.c1(+)
    union
    select t11.c1,t11.c2 from t11, t12 where t11.c1(+) = t12.c1
) order by 1,2;

explain  (costs off)
select *
from
(
    select t11.c1,t11.c2 from t11, t12 where t11.c1 = t12.c1(+)
    union
    select t11.c1,t11.c2 from t11, t12 where t11.c1(+) = t12.c1
) order by 1,2;

--used in correlative query--
--treated as inner join when outer relation is in outer query--
select t11.c1, t11.c1 from t11 where t11.c1 > 5 and t11.c2 in (select t12.c2 from t12 where t11.c1 = t12.c1(+)) order by 1,2;
select t11.c1, t11.c1, (select t12.c2 from t12 where t11.c1 = t12.c1(+)) from t11 where t11.c1 > 5 order by 1,2,3;
explain (costs off)
select t11.c1, t11.c1 from t11 where t11.c1 > 5 and t11.c2 in (select t12.c2 from t12 where t11.c1 = t12.c1(+)) order by 1,2;

--report error, (+)can only be used in the relation in current query level --
select t11.c1, t11.c1 from t11 where t11.c1 > 5 and t11.c2 in (select t12.c2 from t12 where t12.c1 = t11.c1(+)) order by 1,2;


-- (+) used in common relation join with tablefunc
select t11.c1, t11.c2, tf.c1 from t11, unnest(array[-2, -1, 0, 1,2,3]) as tf(c1) where t11.c1(+) = tf.c1 order by 1,3;
select t11.c1, t11.c2, tf.c1 from t11, unnest(array[-2, -1, 0, 1,2,3]) as tf(c1) where t11.c1 = tf.c1(+) order by 1,3;

--used with or clause
select * from t11, t12 where t11.c1 = t12.c1(+) and (t12.c1 > 2 or t12.c1 < 13) order by 1,2,3,4;
select * from t11, t12 where t11.c1 = t12.c1(+) and (t11.c1 > 2 or t12.c1 < 13) order by 1,2,3,4;
select * from t11, t12 where t11.c1 = t12.c1(+) and (t11.c1 > 10 or t11.c1 < 13) order by 1,2,3,4;

-- Report Error with "or"
select * from t11, t12 where t11.c1 = t12.c1(+) or 1=1;
select * from t11, t12 where t11.c2 > 1 or  (t11.c2 > 10 and t11.c1 = t12.c1(+) and 1=1);

--other join condition kind
select * from t11, t12 where t11.c1 < t12.c2(+) order by 1,2,3,4,5,6 limit 3;

/* ignore (+) */
select * from t11, t12 where t11.c1(+) = 1      order by 1,2,3,4,5,6;

/* report error when some join condition with more than one "(+)" */
select * from t11, t12 where t11.c1    = t12.c1(+) and t11.c2(+) = t12.c2(+) order by 1,2,3,4,5,6;

--report error when there is "(+)" in whereclause and join in fromclause.
select * from t11 left join t12 on t11.c1 = t12.c2 where t11.c1 = t12.c2(+) order by 1,2,3,4,5,6;

--report error when there is more than one left-RTE with same right-RTE--
select * from t11, t13, t12 where t11.c1 = t12.c2(+) and t13.c3 = t12.c2(+) order by 1,2,3,4,5,6;

--should specified "(+)" on all join condition, else it will treated as innner join--
select * from t11, t12  where t11.c1 = t12.c2(+) and t11.c3 = t12.c2 order by 1,2,3,4,5,6;
select * from t11, t12, t13  where t11.c1 = t12.c2(+) and t13.c3(+) = t12.c2 and t13.c2 = t12.c1 order by 1,2,3,4,5,6;

--t13,t14 will treated as innner join , and t11,t12 will treated as outer join.--
select * from t11, t12, t13 , t14 where t11.c1 = t12.c2(+) and t13.c3(+) = t14.C4 and t13.c2 = t14.a4 order by 1,2,3,4,5,6,7,8,9,10,11,12 limit 3;

--report error when there is "(+)" in whereclause and join in fromclause.--
select * from t11 left join t12 on t11.c1 = t12.c2 where t11.c1 = t12.c2(+) order by 1,2,3,4,5,6;
select * from t11 right join t12 on t11.c1 = t12.c2 where t11.c1 = t12.c2(+) order by 1,2,3,4,5,6;
select * from t11 full join t12 on t11.c1 = t12.c2 where t11.c1 = t12.c2(+) order by 1,2,3,4,5,6;
select * from t11 NATURAL join t12 where t11.c1 = t12.c2(+) order by t11.c1;

--where condition treated as join filter (t11.c1(+)  = 1)--
select * from t11 , t12 where t11.c2 (+)= t12.c2 and t11.c1(+)  = 1 order by 1,2,3,4,5,6;

--(+) can used in fromClause--
select * from t11 inner join t12 on t11.c1 = t12.c2(+) order by 1,2,3,4,5,6;
select * from t11 left join t12 on t11.c1 = t12.c2(+) order by 1,2,3,4,5,6;
select * from t11 right join t12 on t11.c1 = t12.c2(+) order by 1,2,3,4,5,6;



----with out Identifier------
create table t1(a1 int, b1 int, c1 int)  ;
create table t2(a2 int, b2 int, c2 int)  ;

insert into t1 select v,v,v from generate_series(1,10) as v;
insert into t2 select * from t1;

insert into t1 select v,v,v from generate_series(11,15) as v;
insert into t2 select v,v,v from generate_series(16,20) as v;

--without specified relation name before column name.
select * from t1,t2 where t1.a1(+) = a2 order by 4;
select * from t1,t2 where t1.a1 = a2(+) order by 2;
select * from t1,t2 where a1 = a2(+) order by 3;
select * from t1,t2 where a1(+) = a2 order by 4;

select * from (select a1 from t1) as dt1, t2 where dt1.a1 = a2(+) order by 1;

--without specified relation name before column name.
select * from (select a1 from t1) as dt1, t2 where a1(+) = a2 order by 1, 2;
select t1.a1, t1.b1, tf.x1 from t1, unnest(array[-2, -1, 0, 1,2,3]) as tf(x1) where a1(+) = x1 order by 1,3;
select t1.a1, t1.b1, tf.x1 from t1, unnest(array[-2, -1, 0, 1,2,3]) as tf(x1) where a1 = x1(+) order by 1,3;

--report error when x2 is invalid.
select t1.a1, t1.b1, tf.x1 from t1, unnest(array[-2, -1, 0, 1,2,3]) as tf(x1) where a1 = x2(+) order by 1,3;


create table t15 (like t11);
--unsupport (+) in update/delete/merge-into whereclause--
update t15 set c2 = 1 from t11 where t11.c1 = t15.c2(+);
delete from t15 using t11 where t11.c1 = t15.c2(+);
merge into t15 
using t11
on (t11.c1 = t15.c2)
when matched then
  update set t15.c1 = t11.c2 where t11.c3 = t15.c3(+)
when not matched then
  insert values(2,3,5);

--support subquery in update/delete/merge-into whereclause--
explain(costs off)
update t15 set c2 = 1 where t15.c1 = (select t11.c2 from t11, t15 where t15.c2(+) = t11.c3);
explain(costs off)
delete from t15 using t11 where exists (select t11.c2 from t11, t15 where t15.c2(+) = t11.c3);

-----used in procedure------
CREATE OR REPLACE function plus_join_test_1 (var1 in int) return int
AS
var2 int;
BEGIN
    var2 :=var1+1 ;
    select t11.c1 into var2 from t11, t12 where  t11.c1 = t12.c2(+) and t11.c2(+) = var2 order by 1;
    return var2;
END;
/
select plus_join_test_1(1);

----used with "IN" condition------
select * from t11, t12 where t12.c1(+) in (1,2,3) and t11.c2 = t12.c2(+) order by 1,2,3,4,5,6;

----used with join clause, only const in whereclause-------
select * from t11 left join t12 on t11.c1 = t12.c2 where t11.c2(+) >2  order by 1,2,3,4,5,6;

--------unsupport used in outer join with SubQuery----------
select * from t11, t12 where  t12.c2(+) + t11.c1(+) = (select min(1) from t11) + t11.c1;
select * from t11, t12 where  t11.c2(+) = (select min(1) from t11);
select * from t11, t12 where  t11.c2(+) + 1 = (select min(1) from t11)+1;
select * from t11, t12 where  t11.c2(+) + 1 = (select min(1) from t11);
select * from t11, t12 where  t12.c2   = (select min(1) from t11) + t11.c1(+) order by 1,2,3,4,5,6;

------JoinExpr and (+) not in same query level is  valid---
--select * from (select t11.c1 a from t11,t12 where t11.c1 = t12.c2(+)) inner join t13 on (a = c3) order by 1,2,3,4;
select * from (select t11.c1 a from t11 left join t12 on t11.c1 = t12.c2) inner join t13 on (a = c3) order by 1,2,3,4;
explain (verbose on, costs off, analyze on, timing off, cpu off)
select * from (select t11.c1 a from t11 left join t12 on t11.c1 = t12.c2) inner join t13 on (a = c3) order by 1,2,3,4;
explain(verbose on, costs off)
select * from (select t11.c1 a from t11,t12 where t11.c1 = t12.c2(+)) inner join t13 on (a = c3) order by 1,2,3,4;

----used in create view ---------
create view plus_v as select t11.c1, t12.c2 from t11, t12 where t11.c1 = t12.c2(+) and t12.c2(+) in (1,2,3);
\d+ plus_v

-------used with is (not) null --------
select t11.c1, t12.c2 from t11, t12 where t11.c2 = t12.c3(+) and t12.c2 is not null order by 1,2;
select t11.c1, t12.c2 from t11, t12 where t11.c2 = t12.c3(+) and t11.c3 is not null order by 1,2;
select t11.c1, t12.c2, t13.c2 from t11, t12, t13 where t11.c2 = t12.c3(+) and t11.c3 = t13.c1(+) and (t13.c2 > t12.c1)::bool;

-------used (+) with un common expression, like is null, is not null--------
select t11.c1, t12.c2, t13.c2 from t11, t12, t13 where t11.c2 = t12.c3(+) and t11.c3 = t13.c1(+) and t13.c2(+) is not null;
select t11.c1, t12.c2, t13.c2 from t11, t12, t13 where t11.c2 = t12.c3(+) and t11.c3 = t13.c1(+) and (t13.c2(+) > t12.c1)::bool;
select * from t_a a,t_b b where b.id=a.id(+) and a.code(+) + 1 * 2 + a.code(+) IS NOT NULL ;

drop view plus_v;
drop function plus_join_test_1();
drop table t1;
drop table t2;
drop table t11;
drop table t12;
drop table t13;
drop table t14;
drop table t15;
drop table t_a;
drop table t_b;
drop schema plus_outerjoin;
\c regression;
