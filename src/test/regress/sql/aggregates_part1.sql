/*
 * This file is used to test three possible paths of hash agg stream paths
 */

-- Part-1
drop schema if exists distribute_aggregates_part1 cascade;
create schema distribute_aggregates_part1;
set current_schema = distribute_aggregates_part1;

-- prepare a temp table for import data
create table tmp_t1(c1 int);
insert into tmp_t1 values (1);

-- Create Table and Insert Data
create table t_agg1(a int, b int, c int, d int, e int, f int, g regproc);
create table t_agg2(a int, b int, c int);
insert into t_agg1 select generate_series(1, 10000), generate_series(1, 10000)%5000, generate_series(1, 10000)%500, generate_series(1, 10000)%5, 500, 3, 'sin' from tmp_t1;
insert into t_agg2 select generate_series(1, 10), generate_series(11, 2, -1), generate_series(3, 12);
/*select * from table_skewness('t_agg1', 'b,c') order by 1, 2, 3;*/
analyze t_agg1;
analyze t_agg2;

-- Case 1: hashagg + gather + hashagg, applicable to very small aggregate set, usually much less than DN number
-- group by clause
explain (costs off) select e, sum(c) from t_agg1 group by e;
select e, sum(c) from t_agg1 group by e;
explain (costs off) select e, sum(b+c), avg(d) from t_agg1 group by e;
select e, sum(b+c), avg(d) from t_agg1 group by e;
explain (costs off) select f, e, max(a), min(b) from t_agg1 group by f, e;
select f, e, max(a), min(b) from t_agg1 group by f, e;
explain (costs off) select e, min(b), rank() over (partition by f order by f) from t_agg1 group by f, e;
select e, min(b), rank() over (partition by f order by f) from t_agg1 group by f, e;

-- distinct clause
explain (costs off) select distinct(e) from t_agg1;
select distinct(e) from t_agg1;
explain (costs off) select distinct(e+f) from t_agg1;
select distinct(e+f) from t_agg1;

-- join
explain (costs off) select sum(t_agg1.a) from t_agg1 join t_agg2 on t_agg1.c=t_agg2.b group by t_agg1.e;
select sum(t_agg1.a) from t_agg1 join t_agg2 on t_agg1.c=t_agg2.b group by t_agg1.e;
explain (costs off) select a, sum(c) from (select t_agg1.e a, t_agg1.b b, t_agg2.a-t_agg2.b c from t_agg1 join t_agg2 on t_agg1.b=t_agg2.c) group by a;
select a, sum(c) from (select t_agg1.e a, t_agg1.b b, t_agg2.a-t_agg2.b c from t_agg1 join t_agg2 on t_agg1.b=t_agg2.c) group by a;
explain (costs off) select a, sum(c) from (select t_agg1.b b, t_agg1.e a, sum(t_agg2.a-t_agg2.b) c from t_agg1 join t_agg2 on t_agg1.b=t_agg2.c group by 1, 2) group by a;
select a, sum(c) from (select t_agg1.b b, t_agg1.e a, sum(t_agg2.a-t_agg2.b) c from t_agg1 join t_agg2 on t_agg1.b=t_agg2.c group by 1, 2) group by a;
explain (costs off) select a, sum(c) from (select t_agg1.b b, t_agg1.e a, sum(t_agg2.a-t_agg2.b) c from t_agg1 join t_agg2 on t_agg1.b=t_agg2.c group by 1, 2) group by a having sum(c)=-20;
select a, sum(c) from (select t_agg1.b b, t_agg1.e a, sum(t_agg2.a-t_agg2.b) c from t_agg1 join t_agg2 on t_agg1.b=t_agg2.c group by 1, 2) group by a having sum(c)=-20;

-- Case 2: redistribute + hashagg + gather, applicable to large aggregate set, with less rows eliminated by hash agg
-- group by clause
explain (costs off) select b, sum(a+c), avg(d) from t_agg1 group by b;
select b, sum(a+c), avg(d) from t_agg1 group by b order by b limit 10;
explain (costs off) select b, c, max(a), min(d) from t_agg1 group by b, c;
select b, c, max(a), min(d) from t_agg1 group by b, c order by b, c limit 10;
explain (costs off) select b, d, min(c), rank() over (partition by d order by d) from t_agg1 group by b, d;
select b, d, min(c), rank() over (partition by d order by d) from t_agg1 group by b, d order by b, d limit 10;

-- distinct clause
explain (costs off) select distinct(b) from t_agg1;
select distinct(b) from t_agg1 order by 1 limit 10;
explain (costs off) select distinct(b+c) from t_agg1;
select distinct(b+c) from t_agg1 order by 1 limit 10;

-- subquery
-- (1) group by clause
explain (costs off) select sum(x) from (select sum(a) x from t_agg1 group by b);
select sum(x) from (select sum(a) x from t_agg1 group by b);
explain (costs off) select sum(x) from (select sum(a) x from t_agg1 group by b, c);
select sum(x) from (select sum(a) x from t_agg1 group by b, c);
explain (costs off) select sum(c+d) from (select 2*b x, c, d from t_agg1) group by x;
select sum(c+d) from (select 2*b x, c, d from t_agg1) group by x order by x limit 10;
explain (costs off) select sum(x) from (select 1, sum(a) x from t_agg1 group by 1);
select sum(x) from (select 1, sum(a) x from t_agg1 group by 1);
explain (costs off) select avg(z) from (select 2*a x, a+1 y, a-1 z from t_agg1) group by x, y;
select avg(z) from (select 2*a x, a+1 y, a-1 z from t_agg1) group by x, y order by x, y limit 10;

CREATE TABLE sales_transaction_line
(
    sales_tran_id number(38,10) null,
    tran_line_status_cd clob null,
    tran_line_sales_type_cd char(100) null
)with (orientation=row);

explain (costs off)
SELECT
    CAST(TRAN_LINE_STATUS_CD AS char) c1,
    CAST(TRAN_LINE_STATUS_CD AS char) c2,
    TRAN_LINE_SALES_TYPE_CD c3,
    DENSE_RANK() OVER (order by TRAN_LINE_SALES_TYPE_CD)  
FROM sales_transaction_line 
GROUP BY c1,c3 ;

explain (costs off)
SELECT
   CAST(TRAN_LINE_STATUS_CD AS char) c1,
   CAST(TRAN_LINE_STATUS_CD AS char) c2,
   TRAN_LINE_SALES_TYPE_CD c3,
   DENSE_RANK() OVER (order by TRAN_LINE_SALES_TYPE_CD)  
FROM sales_transaction_line 
GROUP BY c1,c2,c3 ;
SELECT 
    CAST(TRAN_LINE_STATUS_CD AS char) c1,
    CAST(TRAN_LINE_STATUS_CD AS char) c2,
    TRAN_LINE_SALES_TYPE_CD c3,
    DENSE_RANK() OVER (order by TRAN_LINE_SALES_TYPE_CD)  
FROM sales_transaction_line 
GROUP BY c2,c3;

reset current_schema;
drop schema if exists distribute_aggregates_part1 cascade;
