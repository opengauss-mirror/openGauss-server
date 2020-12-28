/*
 * This file is used to test pull down of count(distinct) expression
 */
drop schema if exists distribute_count_distinct_part2 cascade;
create schema distribute_count_distinct_part2;
set current_schema = distribute_count_distinct_part2;

-- prepare a temp table for import data
create table tmp_t1(c1 int);
insert into tmp_t1 values (1);

-- Create Table and Insert Data
create table t_distinct(a int, b int, c int, d int, e regproc);
insert into t_distinct select generate_series(1, 10000)%5001, generate_series(1, 10000)%750, generate_series(1, 10000)%246, generate_series(1, 10000)%7, 'sin' from tmp_t1;
analyze t_distinct;

-- Case 2.3 sub level
-- Case 2.3.1 count_distinct within target list
explain (costs off) select distinct da from (select count(distinct(a)) da, max(b+c), avg(d) from t_distinct);
select distinct da from (select min(distinct(a)) da, max(b+c), avg(d) from t_distinct);
explain (costs off) select distinct db from (select count(distinct(b)) db, max(a-c), avg(d) from t_distinct);
select distinct db from (select max(distinct(b)) db, max(a-c), avg(d) from t_distinct);
explain (costs off) select distinct db from (select count(distinct(b+c)) db, max(a-c), avg(d) from t_distinct);
select distinct db from (select count(distinct(b+c)) db, max(a-c), avg(d) from t_distinct);

-- Case 2.3.3 count_distinct within other place
explain (costs off) select distinct mb from (select max(b) mb from t_distinct order by count(distinct(d)));
select distinct mb from (select max(b) mb from t_distinct order by count(distinct(d)));
explain (costs off) select distinct mb from (select max(b) mb from t_distinct having count(distinct(d))=7);
select distinct mb from (select max(b) mb from t_distinct having count(distinct(d))=7);
explain (costs off) select distinct aabcd from (select avg(a+b+c+d) aabcd from t_distinct having count(distinct(a))=max(a)+1);
select distinct aabcd from (select avg(a+b+c+d) aabcd from t_distinct having count(distinct(a))=max(a)+1);
explain (costs off) select distinct ac from (select avg(c) ac from t_distinct order by count(distinct(a)));
select distinct ac from (select avg(c::numeric(15,10)) ac from t_distinct order by count(distinct(a)));
explain (costs off) select distinct ac from (select avg(c) ac from t_distinct order by count(distinct(d)));
select distinct ac from (select avg(c::numeric(15,10)) ac from t_distinct order by count(distinct(d)));

-- Case 2.4 with distinct
explain (costs off) select distinct count(distinct(b)) from t_distinct;
select distinct count(distinct(b)) from t_distinct;
explain (costs off) select distinct count(distinct(a)) from t_distinct;
select distinct count(distinct(a)) from t_distinct;

-- Case 2.5 non-projection-capable-subplan
explain (costs off) select count(distinct(b+c)) from (select b, c, d from t_distinct union all select b, c, d from t_distinct);
select count(distinct(b+c)) from (select b, c, d from t_distinct union all select b, c, d from t_distinct);

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

reset current_schema;
drop schema if exists distribute_count_distinct_part2 cascade;
