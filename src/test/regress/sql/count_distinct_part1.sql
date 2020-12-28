/*
 * This file is used to test pull down of count(distinct) expression
 */
drop schema if exists distribute_count_distinct_part1 cascade;
create schema distribute_count_distinct_part1;
set current_schema = distribute_count_distinct_part1;

-- prepare a temp table for import data
create table tmp_t1(c1 int);
insert into tmp_t1 values (1);

-- Create Table and Insert Data
create table t_distinct(a int, b int, c int, d int, e regproc);
insert into t_distinct select generate_series(1, 10000)%5001, generate_series(1, 10000)%750, generate_series(1, 10000)%246, generate_series(1, 10000)%7, 'sin' from tmp_t1;
analyze t_distinct;

-- Case 1 (with group by clause)
-- Case 1.1 top level
explain (costs off) select d, count(distinct(a)), avg(a::numeric(7, 2)) from t_distinct group by d;
select d, count(distinct(a)), avg(a::numeric(7, 2)) from t_distinct group by d order by d;
explain (costs off) select c, count(distinct(b)), avg(a::numeric(7, 2)) from t_distinct group by c;
select c, count(distinct(b)), avg(a::numeric(7, 2)) from t_distinct group by c order by c limit 10;
explain (costs off) select 1, 1, b, count(distinct(a)), avg(a::numeric(7, 2)) from t_distinct group by 1,2,3 order by 1,2,3;
select 1, 1, b, count(distinct(a)), avg(a::numeric(7, 2)) from t_distinct group by 1,2,3 order by 1,2,3 limit 10;
explain (costs off) select d, count(distinct(b)), avg(a::numeric(7, 2)) from t_distinct group by d;
select d, count(distinct(b)), avg(a::numeric(7, 2)) from t_distinct group by d order by d;
explain (costs off) select c, count(distinct(a)), count(distinct(b)), count(distinct(d)) from t_distinct group by c;
select c, count(distinct(a)), count(distinct(b)), count(distinct(d)) from t_distinct group by c order by count(distinct(a)), c limit 10;
explain (costs off) select c, count(distinct(a)) ca, count(distinct(b)) cb, count(distinct(d)) cd from t_distinct group by c having count(distinct(b))+count(distinct(d))=47;
select c, count(distinct(a)) ca, count(distinct(b)) cb, count(distinct(d)) cd from t_distinct group by c having count(distinct(b))+count(distinct(d))=47 order by c limit 10;
explain (costs off) select a, count(distinct(b+d)), max(c+b) from t_distinct group by a;
select a, count(distinct(b+d)), max(c+b) from t_distinct group by a order by a limit 10;
explain (costs off) select 1, count(distinct(b)), max(c), min(d), avg(a) from t_distinct group by 1;
select 1, count(distinct(b)), max(c), min(d), avg(a) from t_distinct group by 1;
explain (costs off) SELECT sum(c),avg(c),max(c),min(c),avg(c),avg(c),sum(c),sum(c),count(distinct(c)) FROM t_distinct GROUP BY d order by d;
SELECT sum(c),avg(c),max(c),min(c),avg(c),avg(c),sum(c),sum(c),count(distinct(c)) FROM t_distinct GROUP BY d order by d;
explain (costs off) select sum(a) like '%1%', count(distinct c), d from t_distinct group by d order by 1,2,3;
select sum(a) like '%1%', count(distinct c), d from t_distinct group by d order by 1,2,3;
explain (costs off) select sum(a) like '%1%', count(distinct c), c from t_distinct group by c order by 1,2,3;
select sum(a) like '%1%', count(distinct c), c from t_distinct group by c order by 1,2,3 limit 10;
explain (costs off) select a from (select a, b from t_distinct union all select b, a from t_distinct) group by a having count(distinct(b+1)) = 1 order by 1 limit 5; 
select a from (select a, b from t_distinct union all select b, a from t_distinct) group by a having count(distinct(b+1)) = 1 order by 1 limit 5; 

-- Case 1.2 sub level
explain (costs off) select c, ca, cb from (select c, count(distinct(a)) ca, max(b) cb, min(c+d) cd from t_distinct group by c) x where cd=7;
select c, ca, cb from (select c, count(distinct(a)) ca, max(b) cb, min(c+d) cd from t_distinct group by c) x where cd=7;
explain (costs off) select c, ca, cb from (select c, count(distinct(a)) ca, count(distinct(b)) cb, count(distinct(d)) cd from t_distinct group by c) x where cd=7;
select c, ca, cb from (select c, count(distinct(a)) ca, count(distinct(b)) cb, count(distinct(d)) cd from t_distinct group by c) x where cd=7 order by 1, 2, 3 limit 10;
explain (costs off) select avg(cab) from (select d, count(distinct(a+b)) cab, max(c+a) mca from t_distinct group by d);
select avg(cab::numeric(15,10)) from (select d, count(distinct(a+b)) cab, max(c+a) mca from t_distinct group by d);
explain (costs off) select tt.a, x.cbd, x.ccb from t_distinct tt, (select a, count(distinct(b+d)) cbd, max(c+b) ccb from t_distinct group by a) x where x.cbd=tt.a;
select tt.a, x.cbd, x.ccb from t_distinct tt, (select a, count(distinct(b+d)) cbd, max(c+b) ccb from t_distinct group by a) x where x.cbd=tt.a order by 1, 2, 3 limit 10;

-- Case 1.3 with distinct
explain (costs off) select distinct count(distinct(a)) from t_distinct group by d;
select distinct count(distinct(a)) from t_distinct group by d order by 1;
explain (costs off) select distinct count(distinct(b)) from t_distinct group by a;
select distinct count(distinct(b)) from t_distinct group by a order by 1;

-- Case 1.4 Can't push down
explain (costs off) select count(distinct(a)), max(d) from t_distinct group by e; 
select count(distinct(a)), max(d) from t_distinct group by e; 
explain (costs off) select count(distinct a order by a nulls last) from t_distinct;
select count(distinct a order by a nulls last) from t_distinct;

-- Case 1.5 not hashable
explain (costs off) select count(distinct(e)) from t_distinct group by e limit 5;
select count(distinct(e)) from t_distinct group by e limit 5;

-- Case 2 (without group by clause)
-- Case 2.1 not hashable
explain (costs off) select count(distinct(e)) from t_distinct;
select count(distinct(e)) from t_distinct;

-- Case 2.2 top level
-- Case 2.2.1 count_distinct within target list
explain (costs off) select count(distinct(a)), max(b+c), avg(d::numeric(15,10)) from t_distinct;
select count(distinct(a)), max(b+c), avg(d::numeric(15,10)) from t_distinct;
explain (costs off) select count(distinct(b)), max(a-c), avg(d::numeric(15,10)) from t_distinct;
select avg(distinct(b)), max(a-c), avg(d::numeric(15,10)) from t_distinct;
explain (costs off) select count(distinct(b+c)), max(a-c), avg(d::numeric(15,10)) from t_distinct;
select sum(distinct(b+c)), max(a-c), avg(d::numeric(15,10)) from t_distinct;

-- Case 2.2.2 count_distinct within other place
explain (costs off) select max(b) from t_distinct order by count(distinct(d));
select max(b) from t_distinct order by count(distinct(d));
explain (costs off) select max(b) from t_distinct having count(distinct(d))=7;
select max(b) from t_distinct having count(distinct(d))=7;
explain (costs off) select avg(a+b+c+d) from t_distinct having count(distinct(a))=max(a)+1;
select avg((a+b+c+d)::numeric(15,10)) from t_distinct having count(distinct(a))=max(a)+1;
explain (costs off) select avg(c) from t_distinct order by count(distinct(a));
select avg(c::numeric(15,10)) from t_distinct order by count(distinct(a));
explain (costs off) select avg(c) from t_distinct order by count(distinct(d));
select avg(c::numeric(15,10)) from t_distinct order by count(distinct(d));

-- test sublink in multi count(distinct)
explain (costs off) select 1 from t_distinct t1 where (a,b) not in
 (select coalesce(t1.a+t2.b, t1.c+t2.d), coalesce(max(distinct(t2.c)), min(distinct(t2.d))) from t_distinct t2 group by 1);
explain (costs off) select 1 from t_distinct t1 where (a,b,c) not in
 (select coalesce(t1.a+t2.b, t1.c+t2.d), t1.c, coalesce(max(distinct(t2.c)), min(distinct(t2.d))) from t_distinct t2 group by 1);
explain (costs off) select 1 from t_distinct t1 where (a,b) not in
 (select coalesce(t1.a+t2.b, t1.c+t2.d), coalesce(max(distinct(t2.c)), min(distinct(t2.d))) from t_distinct t2 where t2.a in
 (select avg(t3.c) from t_distinct t3 where t3.b=t1.a) group by 1);
explain (costs off) select 1 from t_distinct t1 where (a,b) not in
 (select coalesce(t1.a+t2.b, t1.c+t2.d), coalesce(max(distinct(t2.c)), min(distinct(t2.d))) from t_distinct t2 where t2.a in
 (select avg(t3.c) from t_distinct t3 where t2.b=t1.a) group by 1);
explain (costs off) select 1 from t_distinct t1 where (a,b) not in
 (with tmp as (select * from t_distinct)
 select coalesce(t1.a+t2.b, t1.c+t2.d), coalesce(max(distinct(t2.c)), min(distinct(t2.d))) from tmp t2 where t2.a in
 (select avg(t3.c) from tmp t3 where t2.b=t1.a) group by 1);

CREATE TABLE promotion (
    p_promo_sk integer NOT NULL,
    p_promo_id character(16) NOT NULL,
    p_start_date_sk integer,
    p_end_date_sk integer,
    p_item_sk integer,
    p_cost numeric(15,2),
    p_response_target integer,
    p_promo_name character(50),
    p_channel_dmail character(1),
    p_channel_email character(1),
    p_channel_catalog character(1),
    p_channel_tv character(1),
    p_channel_radio character(1),
    p_channel_press character(1),
    p_channel_event character(1),
    p_channel_demo character(1),
    p_channel_details character varying(100),
    p_purpose character(15),
    p_discount_active character(1)
);
/*DISTRIBUTE BY HASH (p_cost);*/

set enable_sort = on;
set enable_hashagg = off;
--/* stream's targetlist must be the same as lower operator's targetlist */
explain (costs off, verbose on)
select 
    count(min(p_cost)) over(),
    max(distinct p_cost) AS C1,
    p_promo_sk,
    p_promo_name
from promotion
group by p_promo_name, p_promo_sk;

reset current_schema;
drop schema if exists distribute_count_distinct_part1 cascade;
