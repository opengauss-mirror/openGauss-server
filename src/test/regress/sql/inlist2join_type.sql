create schema inlist2join_c;
set current_schema=inlist2join_c;
set qrw_inlist2join_optmode=1;
create table t1(c1 int, c2 int, c3 int) with (orientation=column);
insert into t1 select v,v,v from generate_series(1,12) as v;
create table t2(c1 int, c2 int, c3 int) with (orientation=column);
insert into t2 select v,v,v from generate_series(1,10) as v;

explain (costs off) select c1,c2 from t1 where t1.c2 in (3,4,7);
select c1,c2 from t1 where t1.c2 in (3,4,7) order by 1;

explain (costs off) select c1,c3 from t1 where t1.c2 in (3,4,7);
select c1,c3 from t1 where t1.c3 in (3,4,7) order by 1;

explain (costs off) select c2,c3 from t1 where t1.c2 in (3,4,7);
select c2,c3 from t1 where t1.c3 in (3,4,7) order by 1;

explain (costs off) select t1.c1 as c1, t2.c2 as c2
from t1,t2
where t1.c1 = t2.c1 and t1.c1 in (3,4,5,6)
order by 1,2;

select t1.c1 as c1, t2.c2 as c2
from t1,t2
where t1.c1 = t2.c1 and t1.c1 in (3,4,5,6)
order by 1,2;

explain (costs off) select * from
(
    select t1.c1 as c1, t2.c2 as c2, count(*) as sum
    from t1,t2
    where t1.c1 = t2.c1
    group by 1,2
) as dt where dt.c1 in (3,4,5,6)
order by 1,2,3;

select * from
(
    select t1.c1 as c1, t2.c2 as c2, count(*) as sum
    from t1,t2
    where t1.c1 = t2.c1
    group by 1,2
) as dt where dt.c1 in (3,4,5,6)
order by 1,2,3;

explain (costs off) select t1.c2, count(*) from t1 where t1.c1 in (1,2,3) group by t1.c2 having t1.c2 in (1,3) order by 1;
select t1.c2, count(*) from t1 where t1.c1 in (1,2,3) group by t1.c2 having t1.c2 in (1,3) order by 1;

explain (costs off) select t1.c3, t1.c2 from t1 where t1.c1 > 1 AND t1.c2 in (select t2.c2 from t2 where t2.c1 IN (3,4,5,6,7)) order by 1,2;
select t1.c3, t1.c2 from t1 where t1.c1 > 1 AND t1.c2 in (select t2.c2 from t2 where t2.c1 IN (3,4,5,6,7)) order by 1,2;

select t1.c2, sum(t1.c3) from t1 where t1.c1 in (1,2,3) group by t1.c2 order by t1.c2 limit 2;
select max(t1.c2) from t1 where t1.c1 < 4 and t1.c1 in (2,3,4);
select t1.c2, t2.c3, count(*) from t1,t2 where t1.c1 = t2.c2 and t1.c1 in (1,2,3) group by t1.c2,t2.c3 order by t1.c2;
select t1.c2, t2.c3, sum(t1.c3) from t1,t2 where t1.c1 = t2.c2 and t1.c1 in (1,2,3) group by t1.c2,t2.c3 order by t1.c2 limit 2;

select * from (select * from t1 where t1.c1 in (1,4,11) union all select * from t2) as dt order by 1;
select * from (select * from t1 union all select * from t2 where t2.c1 in (1,4,11)) as dt order by 1;
select * from (select * from t1 where t1.c1 in (1,4,11) union all select * from t2 where t2.c1 in (2,3)) as dt order by 1;

--测试expression inlist场景
-- allow for inlist2join
explain (costs off) select t1.c1, t2.c1 from t1, t2 where t1.c1 = t2.c1 and (t1.c1 + 2) in (1,2,3,4,5) order by 1;
select t1.c1, t2.c1 from t1, t2 where t1.c1 = t2.c1 and (t1.c1 + 2) in (1,2,3,4,5) order by 1;

explain (costs off) select t1.c1, t2.c1 from t1, t2 where t1.c1 = t2.c1 and (t1.c2 * 2) in (1,2,3,4,5) order by 1;
select t1.c1, t2.c1 from t1, t2 where t1.c1 = t2.c1 and (t1.c2 *2) in (1,2,3,4,5) order by 1;

-- disallow for inlistjoin
explain (costs off) select t1.c1, t2.c1 from t1, t2 where t1.c1 = t2.c1 and (t1.c1 + t1.c2) in (1,2,3,4,5) order by 1;
select t1.c1, t2.c1 from t1, t2 where t1.c1 = t2.c1 and (t1.c1 + t2.c2) in (1,2,3,4,5) order by 1;

--
-- forbid inlist2join query rewrite if there's subplan
set qrw_inlist2join_optmode=1;
create table t3(c1 int, c2 int, c3 int);
insert into t3 select v,v,v from generate_series(1,10) as v;

-- the sublink can be pull up, so there is no subpaln, do inlist2join
explain (costs off) select * from t1 where t1.c1 in (select c1 from t2 where t2.c2 > 2) AND t1.c2 in (1,2,3);
explain (costs off) select * from t1 where exists (select c1 from t2 where t2.c2 = t1.c1) and t1.c1 in (1,2,3);
explain (costs off) select * from t1 join t2 on t1.c1=t2.c1 and t1.c1 not in (select c2 from t3 where t3.c2>2) and t1.c3 in (1,2,3);

-- the sublink can not be pull up, so there is subpaln, do not inlist2join
explain (costs off) select * from t1 where t1.c1 in (select c1 from t2 where t2.c2 = t1.c2) AND t1.c2 in (1,2,3);
explain (costs off) select * from t1 where exists (select c1 from t2 where t2.c2 > 2) and t1.c1 in (1,2,3);
explain (costs off) select * from t1 join t2 on t1.c1=t2.c1 and t1.c1 not in (select c2 from t3 where t3.c2=t1.c2) and t1.c3 in (1,2,3);
--

drop table t1;
drop table t2;
drop table t3;
drop schema inlist2join_c cascade;

