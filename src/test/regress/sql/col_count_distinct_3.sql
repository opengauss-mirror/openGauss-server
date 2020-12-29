/*
 * This file is used to test pull down of count(distinct) expression
 */

create schema col_distribute_count_distinct_3;
set current_schema = col_distribute_count_distinct_3;

-- Create Table and Insert Data
create table src(c1 int);
insert into src values(0);

create table t_distinct(a int, b int, c int, d int) with (orientation=column);
INSERT INTO t_distinct select generate_series(1, 1000)%501, generate_series(1, 1000)%75, generate_series(1, 1000)%25, generate_series(1, 1000)%7 from src;
analyze t_distinct;

-- Case 3 groupagg optimization
set enable_hashagg=off;
explain (costs off) select avg(distinct(a)) from t_distinct;
select avg(distinct(a)) from t_distinct;
explain (costs off) select avg(distinct(b::numeric(5,1)))+5, d from t_distinct group by d order by 2;
select avg(distinct(b::numeric(5,1)))+5, d from t_distinct group by d order by 2;
explain (costs off) select avg(distinct(c))+count(distinct(c)), b from t_distinct group by b order by 2;
select avg(distinct(c))+count(distinct(c)), b from t_distinct group by b order by 2 limit 10;
explain (costs off) select c from t_distinct group by c having avg(distinct(c))>50 order by 1;
select c from t_distinct group by c having avg(distinct(c))>50 order by 1 limit 10;
explain (costs off) select b, c from t_distinct group by b, c order by b, count(distinct(c))-c;
select b, c from t_distinct group by b, c order by b, count(distinct(c))-c limit 10;
explain (costs off) select count(distinct(c)), d from t_distinct group by d having avg(distinct(c)) <> 0 order by 2;
select count(distinct(c)), d from t_distinct group by d having avg(distinct(c)) <> 0 order by 2;
reset enable_hashagg;

-- Case 4 two_level_hashagg
explain (costs off) select count(distinct(b)), count(c), d from t_distinct group by d order by d;
select count(distinct(b)), count(c), d from t_distinct group by d order by d;
explain (costs off) select avg(distinct(b)), d, count(c) from t_distinct group by d order by d;
select avg(distinct(b)), d, count(c) from t_distinct group by d order by d;
explain (costs off) select count(c), count(distinct(a)), d from t_distinct group by d order by d;
select count(c), count(distinct(a)), d from t_distinct group by d order by d;
explain (costs off) select count(c), d, count(distinct(d)) from t_distinct group by d order by 1;
select count(c), d, count(distinct(d)) from t_distinct group by d order by 1, 2;
explain (costs off) select count(c), d, count(distinct(b)) from t_distinct group by d order by 3;
select count(c), d, count(distinct(b)) from t_distinct group by d order by 3, 2;
explain (costs off) select count(d), count(distinct(d)), a%2, b%2, c%2 from t_distinct group by 3,4,5 order by 3,4,5;
select count(d), count(distinct(d)), a%2, b%2, c%2 from t_distinct group by 3,4,5 order by 3,4,5;
explain (costs off) select count(c), d from t_distinct group by d having count(distinct(d))=1 order by d;
select count(c), d from t_distinct group by d having count(distinct(d))=1 order by d;
explain (costs off) select count(c), count(distinct(b)) from t_distinct group by d having count(d)=1428 order by d;
select count(c), count(distinct(b)) from t_distinct group by d having count(d)=1428 order by d;
explain (costs off) select count(distinct(c)), d from t_distinct group by d order by count(c), d;
select count(distinct(c)), d from t_distinct group by d order by count(c), d;
explain (costs off) select count(distinct(c)), d from t_distinct where c <= any (select count(distinct(b)) from t_distinct group by c limit 5) group by d order by d;
select count(distinct(c)), d from t_distinct where c <= any (select count(distinct(b)) from t_distinct group by c limit 5) group by d order by d;

-- Clean Table
drop table t_distinct;
reset current_schema;
drop schema col_distribute_count_distinct_3 cascade;
