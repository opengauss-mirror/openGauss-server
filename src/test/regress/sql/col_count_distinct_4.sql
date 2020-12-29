/*
 * This file is used to test pull down of count(distinct) expression
 */

create schema col_distribute_count_distinct_4;
set current_schema = col_distribute_count_distinct_4;

-- Create Table and Insert Data
create table src(c1 int);
insert into src values(0);

create table t_distinct(a int, b int, c int, d int) with (orientation=column);
INSERT INTO t_distinct select generate_series(1, 1000)%501, generate_series(1, 1000)%75, generate_series(1, 1000)%25, generate_series(1, 1000)%7 from src;
analyze t_distinct;

-- Case 5 multi-level count(distinct)
-- Case 5.1 normal case
explain (costs off) select count(distinct(a)), count(distinct(b)) from t_distinct;
select count(distinct(a)), count(distinct(b)) from t_distinct;
explain (costs off) select count(distinct(a)) from t_distinct having count(distinct(a))>5000;
select count(distinct(a)) from t_distinct having count(distinct(a))>5000;
explain (costs off) select count(distinct(b)) from t_distinct order by count(distinct(d)); 
select count(distinct(b)) from t_distinct order by count(distinct(d)); 
explain (costs off) select count(distinct(a)) col1, max(b) col2, count(distinct(b)) col3, c, count(distinct(c)) col4, d from t_distinct group by d,c;
select count(distinct(a)) col1, max(b) col2, count(distinct(b)) col3, c, count(distinct(c)) col4, d from t_distinct group by d,c order by d,c limit 10;
explain (costs off) select count(distinct(a)) col1, max(b) col2, count(distinct(b)) col3, min(c) col4, count(distinct(c)) guo, avg(a) qiang from t_distinct;
select count(distinct(a)) col1, max(b) col2, count(distinct(b)) col3, min(c) col4, count(distinct(c)) guo, avg(a) qiang from t_distinct;
explain (costs off) select count(distinct(a))+avg(b) col2, count(c) col3, d from t_distinct group by d having count(distinct(c))>5;
select count(distinct(a))+avg(b) col2, count(c) col3, d from t_distinct group by d having count(distinct(c))>5 order by d;
explain (costs off) select count(distinct(a)) col1, avg(b) col2, count(c) col3, d from t_distinct group by d order by d, avg(distinct(c));
select count(distinct(a)) col1, avg(b) col2, count(c) col3, d from t_distinct group by d order by d, avg(distinct(c));
explain (costs off) select count(distinct(a)) col1, d, avg(b) col2, sum(distinct(a)) col3, avg(distinct(c)) col4 from t_distinct group by d order by d, avg(distinct(c));
select count(distinct(a)) col1, d, avg(b) col2, sum(distinct(a)) col3, avg(distinct(c)) col4 from t_distinct group by d order by d, avg(distinct(c));
explain (costs off) select distinct case when min(distinct c)>60 then min(distinct c) else null end as min, count(distinct(b)) from t_distinct group by b;
select distinct case when min(distinct c)>60 then min(distinct c) else null end as min, count(distinct(b)) from t_distinct group by b order by 1 nulls first limit 5;
explain (costs off) select count(distinct(a)) col1, d, avg(b) col2, sum(distinct(a)) col3, avg(distinct(c)) col4 from t_distinct group by d having col1=1428 or d+col4>125 order by d, avg(distinct(c));
select count(distinct(a)) col1, d, avg(b) col2, sum(distinct(a)) col3, avg(distinct(c)) col4 from t_distinct group by d having col1=1428 or d+col4>125 order by d, avg(distinct(c));

-- Case 5.2 set operation
explain (costs off) select count(distinct a),max(distinct b),c from t_distinct group by c union (select count(distinct a),max(distinct b),d from t_distinct group by d) order by 3,2,1 limit 10;
select count(distinct a),max(distinct b),c from t_distinct group by c union (select count(distinct a),max(distinct b),d from t_distinct group by d) order by 3,2,1 limit 10;

-- Case 5.3 null case
insert into t_distinct values(1, null, null, null), (2, null, 3, null), (2, 3, null, null), (2, null, null, 3), (1, null, 5, 6), (1, 3, null, 5), (1, 3, 5, null), (1, null, null, null), (2, null, 3, null), (2, 3, null, null), (2, null, null, 3), (1, null, 5, 6), (1, 3, null, 5), (1, 3, 5, null);
set enable_mergejoin=off;
set enable_hashjoin=off;
explain (costs off) select count(distinct(a)), count(distinct(a*0)), b, c, d from t_distinct group by b, c, d order by 3 desc, 4 desc, 5 desc limit 10;
select count(distinct(a)), count(distinct(a*0)), b, c, d from t_distinct group by b, c, d order by 3 desc, 4 desc, 5 desc limit 10;
explain (costs off) select count(distinct(a)), count(distinct(b)), c, d from t_distinct group by c, d order by 3 desc, 4 desc limit 10;
select count(distinct(a)), count(distinct(b)), c, d from t_distinct group by c, d order by 3 desc, 4 desc limit 10;
explain (costs off) select count(distinct(a)), count(distinct(b)), count(distinct(c)), d from t_distinct group by d order by 4 desc limit 10;
select count(distinct(a)), count(distinct(b)), count(distinct(c)), d from t_distinct group by d order by 4 desc limit 10;
set enable_nestloop=off;
set enable_hashjoin=on;
explain (costs off) select count(distinct(a)), count(distinct(a*0)), b, c, d from t_distinct group by b, c, d order by 3 desc, 4 desc, 5 desc limit 10;
select count(distinct(a)), count(distinct(a*0)), b, c, d from t_distinct group by b, c, d order by 3 desc, 4 desc, 5 desc limit 10;
explain (costs off) select count(distinct(a)), count(distinct(b)), c, d from t_distinct group by c, d order by 3 desc, 4 desc limit 10;
select count(distinct(a)), count(distinct(b)), c, d from t_distinct group by c, d order by 3 desc, 4 desc limit 10;
explain (costs off) select count(distinct(a)), count(distinct(b)), count(distinct(c)), d from t_distinct group by d order by 4 desc limit 10;
select count(distinct(a)), count(distinct(b)), count(distinct(c)), d from t_distinct group by d order by 4 desc limit 10;
set enable_hashjoin=off;
set enable_mergejoin=on;
explain (costs off) select count(distinct(a)), count(distinct(a*0)), b, c, d from t_distinct group by b, c, d order by 3 desc, 4 desc, 5 desc limit 10;
select count(distinct(a)), count(distinct(a*0)), b, c, d from t_distinct group by b, c, d order by 3 desc, 4 desc, 5 desc limit 10;
explain (costs off) select count(distinct(a)), count(distinct(b)), c, d from t_distinct group by c, d order by 3 desc, 4 desc limit 10;
select count(distinct(a)), count(distinct(b)), c, d from t_distinct group by c, d order by 3 desc, 4 desc limit 10;
explain (costs off) select count(distinct(a)), count(distinct(b)), count(distinct(c)), d from t_distinct group by d order by 4 desc limit 10;
select count(distinct(a)), count(distinct(b)), count(distinct(c)), d from t_distinct group by d order by 4 desc limit 10;
reset enable_hashjoin;
reset enable_nestloop;

-- dummy column redistribute
explain (costs off) select count(distinct(c)) from (select a, ''::text as c from (select t1.a from t_distinct t1 join t_distinct t2 on t1.a=t2.a join t_distinct t3 on t2.a=t3.a) where a>5);
select count(distinct(c)) from (select a, ''::text as c from (select t1.a from t_distinct t1 join t_distinct t2 on t1.a=t2.a join t_distinct t3 on t2.a=t3.a) where a>5);

-- Clean Table
drop table t_distinct;
reset current_schema;
drop schema col_distribute_count_distinct_4 cascade;
