/*
 * This file is used to test pull down of count(distinct) expression
 */
drop schema if exists distribute_count_distinct_part3 cascade;
create schema distribute_count_distinct_part3;
set current_schema = distribute_count_distinct_part3;

-- prepare a temp table for import data
create table tmp_t1(c1 int);
insert into tmp_t1 values (1);

-- Create Table and Insert Data
create table t_distinct(a int, b int, c int, d int, e regproc);
insert into t_distinct select generate_series(1, 10000)%5001, generate_series(1, 10000)%750, generate_series(1, 10000)%246, generate_series(1, 10000)%7, 'sin' from tmp_t1;
analyze t_distinct;

-- Case 5.2 set operation
explain (costs off) select count(distinct a),max(distinct b),c from t_distinct group by c union (select count(distinct a),max(distinct b),d from t_distinct group by d) order by 3,2,1 limit 10;
select count(distinct a),max(distinct b),c from t_distinct group by c union (select count(distinct a),max(distinct b),d from t_distinct group by d) order by 3,2,1 limit 10;

-- Case 5.3 not null case
set enable_mergejoin=off;
set enable_hashjoin=off;
alter table t_distinct alter d set not null;
explain (costs off) select count(distinct(a)), count(distinct(b)), d from t_distinct group by d order by d;
select count(distinct(a)), count(distinct(b)), d from t_distinct group by d order by d;
explain (costs off) select count(distinct(a)), count(distinct(b)), d from t_distinct group by c, d order by c, d limit 10;
select count(distinct(a)), count(distinct(b)), d from t_distinct group by c, d order by c, d limit 10;
alter table t_distinct alter c set not null;
explain (costs off) select count(distinct(a)), count(distinct(b)), d from t_distinct group by c, d order by c, d limit 10;
select count(distinct(a)), count(distinct(b)), d from t_distinct group by c, d order by c, d limit 10;
alter table t_distinct alter c drop not null;
alter table t_distinct alter d drop not null;

-- Case 5.4 null case
insert into t_distinct values(1, null, null, null), (2, null, 3, null), (2, 3, null, null), (2, null, null, 3), (1, null, 5, 6), (1, 3, null, 5), (1, 3, 5, null), (1, null, null, null), (2, null, 3, null), (2, 3, null, null), (2, null, null, 3), (1, null, 5, 6), (1, 3, null, 5), (1, 3, 5, null);
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
explain (costs off) select count(distinct(a)) col1, avg(b) col2, sum(distinct(b)) col3, avg(distinct(c)) col4, count(distinct(a%b)) col5, max(distinct(c not in (5, 6))) col6, d, d is not null, coalesce(d, 5), d<>5 from t_distinct where b<>0 group by d, d is not null, coalesce(d, 5), d<>5 order by d, avg(distinct(c));
select count(distinct(a)) col1, avg(b) col2, sum(distinct(b)) col3, avg(distinct(c)) col4, count(distinct(a%b)) col5, max(distinct(c not in (5, 6))) col6, d, d is not null, coalesce(d, 5), d<>5 from t_distinct where b<>0 group by d, d is not null, coalesce(d, 5), d<>5 order by d, avg(distinct(c));
explain (costs off) select count(distinct(a)) col1, avg(b) col2, sum(distinct(b)) col3, avg(distinct(c)) col4, count(distinct(a%b)) col5, max(distinct(c not in (5, 6))) col6, d, d is not null, coalesce(d, 5), d<>5 from t_distinct group by d, d is not null, coalesce(d, 5), d<>5 order by d, avg(distinct(c));
select count(distinct(a)) col1, avg(b) col2, sum(distinct(b)) col3, avg(distinct(c)) col4, count(distinct(a%b)) col5, max(distinct(c not in (5, 6))) col6, d, d is not null, coalesce(d, 5), d<>5 from t_distinct group by d, d is not null, coalesce(d, 5), d<>5 order by d, avg(distinct(c));
reset enable_hashjoin;
reset enable_nestloop;

-- distinct and bias test
create table t_distinct1(a int, b int, c int, d int, e regproc);
insert into t_distinct1 select generate_series(1, 10000), generate_series(1, 10000)%20, generate_series(1, 10000)%20, generate_series(1, 10000)%1000, 'sin' from tmp_t1;
analyze t_distinct1;
-- distribute by d
explain (verbose on, costs off) select sum(a),count(distinct(b)),avg(c),d from t_distinct1 group by b,c,d;
select sum(a),count(distinct(b)),avg(c),d from t_distinct1 group by b,c,d order by 1 desc limit 10;
explain (verbose on, costs off) select avg(a::numeric(7, 2)) over(partition by b, d) from t_distinct1;
select avg(a::numeric(7, 2)) over(partition by b, d) from t_distinct1 order by 1 desc limit 10;
-- all expr is skew
explain (verbose on, costs off) select avg(a::numeric(7, 2)),count(distinct(c)) from t_distinct1 group by b,c;
select avg(a::numeric(7, 2)),count(distinct(c)) from t_distinct1 group by b,c order by 1 desc limit 10;
explain (verbose on, costs off) select abs(b), count(distinct(a)) from t_distinct1 group by abs(b) having abs(b)+sum(c)>1000;
select abs(b), count(distinct(a)) from t_distinct1 group by abs(b) having abs(b)+sum(c)>1000 order by 1,2 desc limit 10;

create table select_others_table_000(c_boolean bool, c_serial int, c_money int, c_regproc regproc, c_inet inet,
c_cidr cidr, c_int int, c_timestamp_without timestamp);

explain (costs off)
select count(distinct c_boolean), sum(c_int), count(c_timestamp_without), c_regproc, count(distinct c_money), c_inet, count(distinct c_cidr) 
from select_others_table_000 group by c_regproc,c_inet having count(c_cidr)>1 order by 1,2;
-- Clean Table
drop table t_distinct;
drop table t_distinct1;

reset current_schema;
drop schema if exists distribute_count_distinct_part3 cascade;
