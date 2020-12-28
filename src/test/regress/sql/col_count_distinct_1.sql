/*
 * This file is used to test pull down of count(distinct) expression
 */

create schema col_distribute_count_distinct_1;
set current_schema = col_distribute_count_distinct_1;

-- Create Table and Insert Data
create table src(c1 int);
insert into src values(0);
create table t_distinct(a int, b int, c int, d int) with (orientation=column);
INSERT INTO t_distinct select generate_series(1, 1000)%501, generate_series(1, 1000)%75, generate_series(1, 1000)%25, generate_series(1, 1000)%7 from src;
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

-- Clean Table
drop table t_distinct;
reset current_schema;
drop schema col_distribute_count_distinct_1 cascade;
