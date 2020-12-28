/*
 * This file is used to test pull down of count(distinct) expression
 */

create schema col_distribute_count_distinct_2;
set current_schema = col_distribute_count_distinct_2;

-- Create Table and Insert Data
create table src(c1 int);
insert into src values(0);

create table t_distinct(a int, b int, c int, d int) with (orientation=column);
INSERT INTO t_distinct select generate_series(1, 1000)%501, generate_series(1, 1000)%75, generate_series(1, 1000)%25, generate_series(1, 1000)%7 from src;
analyze t_distinct;

-- Case 2 (without group by clause)
-- Case 2.1 top level
-- Case 2.1.1 count_distinct within target list
explain (costs off) select count(distinct(a)), max(b+c), avg(d::numeric(15,10)) from t_distinct;
select count(distinct(a)), max(b+c), avg(d::numeric(15,10)) from t_distinct;
explain (costs off) select count(distinct(b)), max(a-c), avg(d::numeric(15,10)) from t_distinct;
select avg(distinct(b)), max(a-c), avg(d::numeric(15,10)) from t_distinct;
explain (costs off) select count(distinct(b+c)), max(a-c), avg(d::numeric(15,10)) from t_distinct;
select sum(distinct(b+c)), max(a-c), avg(d::numeric(15,10)) from t_distinct;

-- Case 2.1.2 count_distinct within other place
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

-- Case 2.2 sub level
-- Case 2.2.1 count_distinct within target list
explain (costs off) select distinct da from (select count(distinct(a)) da, max(b+c), avg(d) from t_distinct);
select distinct da from (select min(distinct(a)) da, max(b+c), avg(d) from t_distinct);
explain (costs off) select distinct db from (select count(distinct(b)) db, max(a-c), avg(d) from t_distinct);
select distinct db from (select max(distinct(b)) db, max(a-c), avg(d) from t_distinct);
explain (costs off) select distinct db from (select count(distinct(b+c)) db, max(a-c), avg(d) from t_distinct);
select distinct db from (select count(distinct(b+c)) db, max(a-c), avg(d) from t_distinct);

-- Case 2.2.2 count_distinct within other place
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

-- Case 2.3 with distinct
explain (costs off) select distinct count(distinct(b)) from t_distinct;
select distinct count(distinct(b)) from t_distinct;
explain (costs off) select distinct count(distinct(a)) from t_distinct;
select distinct count(distinct(a)) from t_distinct;

-- Case 2.4 non-projection-capable-subplan
explain (costs off) select count(distinct(b+c)) from (select b, c, d from t_distinct union all select b, c, d from t_distinct);
select count(distinct(b+c)) from (select b, c, d from t_distinct union all select b, c, d from t_distinct);

-- Clean Table
drop table t_distinct;
reset current_schema;
drop schema col_distribute_count_distinct_2 cascade;
