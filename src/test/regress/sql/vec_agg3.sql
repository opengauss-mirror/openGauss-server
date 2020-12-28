/*
 * This file is used to test the function of ExecVecAggregation()---(3)
 */
set enable_hashagg = off;
create schema vec;
set current_schema to vec; 

--text sort agg and plain agg
create table aggt_col(a numeric, b int, c varchar(10))with(orientation=column);
insert into aggt_col values(1, 1, 'abc'), (1, 1, 'abc'), (1, 1, 'abc'),(1, 2, 'efg'), (1, 3, 'hij');
select * from aggt_col order by 1, 2, 3;
select count(distinct a) from aggt_col;
select count(distinct b) from aggt_col;
select count(distinct c) from aggt_col;

select avg(distinct a) from aggt_col;
select avg(distinct b) from aggt_col;

select a, sum(a), avg(distinct a),count(distinct b), sum(b)  from aggt_col group by a;
select a, count(distinct b), max(c) from aggt_col group by a;
select a, count(distinct b), sum(b)  from aggt_col group by a;

delete from aggt_col;

--insert into aggt_col values(1, generate_series(1, 2200), 'agg'||generate_series(1, 2200));
create table src(a int);
insert into src values(1);
insert into aggt_col select 1, generate_series(1,2200), 'agg'||generate_series(1, 2200) from src;

select a, count(distinct b),  count(distinct c) from aggt_col group by a;
insert into aggt_col values(1, 2200, 'agg2200');
select a, count(distinct b),  count(distinct c) from aggt_col group by a;
insert into aggt_col values(2, 2200, 'agg2200');
select a, count(distinct b),  count(distinct c) from aggt_col group by a  order by 1;

delete from aggt_col;

--insert into aggt_col values(generate_series(1, 2200), generate_series(1, 2200), 'agg'||generate_series(1, 2200));
insert into aggt_col select generate_series(1, 2200), generate_series(1, 2200), 'agg'||generate_series(1, 2200) from src;
select a, count(distinct b),  count(distinct c) from aggt_col group by a order by 1;
insert into aggt_col values(1, 2201, 'agg2201'), (1, 2202, 'agg2202'), (1, 2203, 'agg2203');
select a, count(distinct b),  count(distinct c) from aggt_col group by a order by 1;
insert into aggt_col values(2, 2, 'agg2'), (2, 2, 'agg2'), (2, 2, 'agg2');
select a, count(distinct b),  count(distinct c) from aggt_col group by a  order by 1;


drop table  aggt_col;
create table aggt_col(a numeric, b int, c varchar(10), d int)with(orientation=column);

--insert into aggt_col values(1, 1, 'agg1', generate_series(1, 1000));
insert into aggt_col select 1, 1, 'agg1', generate_series(1, 1000) from src;
--insert into aggt_col values(1, 2, 'agg2', generate_series(1, 500));
insert into aggt_col select 1, 2, 'agg2', generate_series(1, 500) from src;
select count(distinct a) from  aggt_col;
select count(distinct b) from  aggt_col;
select count(distinct c) from  aggt_col;
select a, count(distinct b), count(distinct c), sum(a), sum(b), max(c) from  aggt_col group by a order by 1;
select b, count(distinct a), count(distinct c), sum(a), sum(b), max(c) from  aggt_col group by b order by 1;
select c, count(distinct a), count(distinct b), sum(a), sum(b)  from  aggt_col group by c order by 1;

drop table  aggt_col;
create table aggt_col(a numeric, b int, c varchar(10), d int)with(orientation=column);

--insert into aggt_col values(1, 1, 'agg1', generate_series(1, 1500));
insert into aggt_col select 1, 1, 'agg1', generate_series(1, 1500) from src;
select count(distinct a) from  aggt_col;
select count(distinct b) from  aggt_col;
select count(distinct c) from  aggt_col;
select a, count(distinct b), count(distinct c) from  aggt_col group by a order by 1;
select b, count(distinct a), count(distinct c) from  aggt_col group by b order by 1;
select c, count(distinct a), count(distinct b) from  aggt_col group by c order by 1;
select sum(a) as sum_value, d from aggt_col group by d  having sum_value < 0;

drop table  aggt_col;
create table aggt_col(a numeric, b int, c varchar(10), d int)with(orientation=column);
--insert into aggt_col values(1, 1, 'agg1', generate_series(1, 2000));
insert into aggt_col select 1, 1, 'agg1', generate_series(1, 2000) from src;

select count(distinct a) from  aggt_col;
select count(distinct b) from  aggt_col;
select count(distinct c) from  aggt_col;
select a, count(distinct b), count(distinct c) from  aggt_col group by a order by 1;
select b, count(distinct a), count(distinct c) from  aggt_col group by b order by 1;
select c, count(distinct a), count(distinct b) from  aggt_col group by c order by 1;
insert into aggt_col values(1, 2, 'agg2');
insert into aggt_col values(1, 3, 'agg3');
select count(distinct a) from  aggt_col;
select count(distinct b) from  aggt_col;
select count(distinct c) from  aggt_col;
select a, count(distinct b), count(distinct c) from  aggt_col group by a order by 1;
select b, count(distinct a), count(distinct c) from  aggt_col group by b order by 1;
select c, count(distinct a), count(distinct b) from  aggt_col group by c order by 1;
insert into aggt_col values(10, 20, 'agg30');
select count(distinct a) from  aggt_col;
select count(distinct b) from  aggt_col;
select count(distinct c) from  aggt_col;
select a, count(distinct b), count(distinct c) from  aggt_col group by a order by 1;
select b, count(distinct a), count(distinct c) from  aggt_col group by b order by 1;
select c, count(distinct a), count(distinct b) from  aggt_col group by c order by 1;

delete from aggt_col;
--insert into aggt_col values(1, generate_series(1, 500), 'agg'||generate_series(1, 500));
insert into aggt_col select 1, 1, 'agg1', generate_series(1, 2000) from src;
--insert into aggt_col values(2, generate_series(1, 1500), 'agg'||generate_series(1, 1500));
insert into aggt_col select 2, generate_series(1, 1500), 'agg'||generate_series(1, 1500) from src;

analyze aggt_col;
select count(distinct a) from  aggt_col;
select count(distinct b) from  aggt_col;
select count(distinct c) from  aggt_col;
select a, count(distinct b), count(distinct c), max(b), max(c) from  aggt_col group by a order by 1;
select b, count(distinct a), count(distinct c), max(b), max(c) from  aggt_col group by b order by 1;
select c, count(distinct a), count(distinct b), max(b), max(c) from  aggt_col group by c order by 1;

delete from aggt_col;
--insert into aggt_col values(2, generate_series(1, 1010), 'agg'||generate_series(1, 1010));
insert into aggt_col select 2, generate_series(1, 1010), 'agg'||generate_series(1, 1010) from src;
--insert into aggt_col values(3, generate_series(1, 800), 'agg'||generate_series(1, 800));
insert into aggt_col select 3, generate_series(1, 800), 'agg'||generate_series(1, 800) from src;
--insert into aggt_col values(4, generate_series(1, 800), 'agg'||generate_series(1, 800));
--insert into aggt_col values(5, generate_series(1, 800), 'agg'||generate_series(1, 800));
--insert into aggt_col values(6, generate_series(1, 800), 'agg'||generate_series(1, 800));


select count(distinct a) from  aggt_col;
select count(distinct b) from  aggt_col;
select count(distinct c) from  aggt_col;
select a, count(distinct b), count(distinct c) from  aggt_col group by a order by 1;
select b, count(distinct a), count(distinct c) from  aggt_col group by b order by 1;
select c, count(distinct a), count(distinct b) from  aggt_col group by c order by 1;


create table aggt_col_numeric(a int, b numeric(10,0), c varchar(10), d int)with(orientation=column);

--insert into aggt_col_numeric values(1, 100, 'aggtest', generate_series(1, 2200));
insert into aggt_col_numeric select 1, 100, 'aggtest', generate_series(1, 2200) from src;
select count(distinct b) from aggt_col_numeric;
select count(distinct c) from aggt_col_numeric;
select count(distinct b),  count(distinct c) from aggt_col_numeric group by a;
select sum(b), avg(b), count(distinct b),  count(distinct c) from aggt_col_numeric group by a;
explain (costs off) select sum(b), avg(b), count(distinct b),  count(distinct c) from aggt_col_numeric group by a;
drop table aggt_col;
drop table aggt_col_numeric;

create table t1_agg_col(a int, b timetz, c tinterval, d interval, e int)with(orientation = column);
--insert into t1_agg_col values(1, '10:11:12', '["Feb 10, 1947 23:59:12" "Jan 14, 1973 03:14:21"]', 1, generate_series(1, 1000));
insert into t1_agg_col select 1, '10:11:12', '["Feb 10, 1947 23:59:12" "Jan 14, 1973 03:14:21"]', 1, generate_series(1, 1000) from src;
select a, count(distinct b), count(distinct c), count(distinct d) from t1_agg_col group by a;
insert into t1_agg_col values(1, '10:11:12', '["Feb 10, 1947 23:59:12" "Jan 14, 1973 03:14:21"]', 1);
select a, count(distinct b), count(distinct c), count(distinct d) from t1_agg_col group by a;
insert into t1_agg_col values(1, '10:11:13', '["Feb 10, 1957 23:59:12" "Jan 14, 1973 03:14:21"]', 2);
select a, count(distinct b), count(distinct c), count(distinct d) from t1_agg_col group by a;
insert into t1_agg_col values(2, '10:11:13', '["Feb 10, 1957 23:59:12" "Jan 14, 1973 03:14:21"]', 2);
select a, count(distinct b), count(distinct c), count(distinct d) from t1_agg_col group by a order by 1;
select count(distinct b), 1 as col1, 'plain agg' as col2 from t1_agg_col order by 1;
--print error and show ql statement.
explain select a from t1_agg_col group by 1, 2;
drop table t1_agg_col;
drop table src;
drop schema vec;
reset enable_hashagg;
