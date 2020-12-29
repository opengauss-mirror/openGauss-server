/*
 * This file is used to test three possible paths of hash agg stream paths
 */

-- Part-3
drop schema if exists distribute_aggregates_part3 cascade;
create schema distribute_aggregates_part3;
set current_schema = distribute_aggregates_part3;

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

-- (3) windowagg
explain (costs off) select c, sum(x) from (select c, row_number() over (partition by c, d, e order by d, e) x from t_agg1) group by c;
select c, sum(x) from (select c, row_number() over (partition by c, d, e order by d, e) x from t_agg1) group by c order by c limit 10;
explain (costs off) select x, sum(c+d+e) from (select c, d, e, row_number() over (partition by d order by c, e) x from t_agg1) group by x;
select x, sum(c+d+e) from (select c, d, e, row_number() over (partition by d order by c, e) x from t_agg1) group by x order by x limit 10;

-- join
explain (costs off) select sum(t_agg1.a) from t_agg1 join t_agg2 on t_agg1.c=t_agg2.b group by t_agg1.c;
select sum(t_agg1.a) from t_agg1 join t_agg2 on t_agg1.c=t_agg2.b group by t_agg1.c order by t_agg1.c limit 10;
explain (costs off) select a, b, count(c) from (select t_agg1.c a, t_agg1.d b, t_agg2.b-t_agg2.a c from t_agg1 join t_agg2 on t_agg1.b=t_agg2.c) group by a, b; 
select a, b, count(c) from (select t_agg1.c a, t_agg1.d b, t_agg2.b-t_agg2.a c from t_agg1 join t_agg2 on t_agg1.b=t_agg2.c) group by a, b order by a, b limit 10;
explain (costs off) select a, b, count(c) from (select t_agg1.c a, t_agg1.d b, sum(t_agg2.b-t_agg2.a) c from t_agg1 join t_agg2 on t_agg1.b=t_agg2.c group by t_agg1.d, t_agg1.c) group by a, b; 
select a, b, count(c) from (select t_agg1.c a, t_agg1.d b, sum(t_agg2.b-t_agg2.a) c from t_agg1 join t_agg2 on t_agg1.b=t_agg2.c group by t_agg1.d, t_agg1.c) group by a, b order by a, b limit 10;
explain (costs off) select a, b, count(c) from (select t_agg1.c a, t_agg1.d b, sum(t_agg2.b-t_agg2.a) c from t_agg1 join t_agg2 on t_agg1.b=t_agg2.c group by t_agg1.d, t_agg1.c having avg(t_agg2.a-t_agg2.b)>=0) group by a, b having count(c)=1; 
select a, b, count(c) from (select t_agg1.c a, t_agg1.d b, sum(t_agg2.b+t_agg2.a) c from t_agg1 join t_agg2 on t_agg1.b=t_agg2.c group by t_agg1.d, t_agg1.c having avg(t_agg2.a-t_agg2.b)>=0) group by a, b having count(c)=1 order by a, b limit 10;

-- distinct pull down
explain (costs off) select distinct count(a) as result from t_agg1 group by a order by count(a) using < fetch next 15 rows only;
select distinct count(a) as result from t_agg1 group by a order by count(a) using < fetch next 15 rows only;
explain (costs off) select distinct count(d) as result from t_agg1 group by c order by count(d) using < fetch next 15 rows only;
select distinct count(d) as result from t_agg1 group by c order by count(d) using < fetch next 15 rows only;
select * from (select distinct count(d) as result from t_agg1 group by c order by count(d) using < limit all) order by result using < nulls first fetch next 15 rows only;
explain (costs off) select * from (select distinct count(a) as result from t_agg1 group by a order by count(a) using < limit all) order by result using < nulls first fetch next 15 rows only;
select * from (select distinct count(a) as result from t_agg1 group by a order by count(a) using < limit all) order by result using < nulls first fetch next 15 rows only;
explain (costs off) select * from (select distinct count(d) as result from t_agg1 group by c order by count(d) using < limit all) order by result using < nulls first fetch next 15 rows only;
select * from (select distinct count(d) as result from t_agg1 group by c order by count(d) using < limit all) order by result using < nulls first fetch next 15 rows only;

set enable_hashagg=off;
explain (costs off) select all * from (select distinct case when b%2=1 then 'id=1' when b%2=0 then 'id=0' else 'id=2' end as result from t_agg1 order by 1); 
select all * from (select distinct case when b%2=1 then 'id=1' when b%2=0 then 'id=0' else 'id=2' end as result from t_agg1 order by 1) order by 1; 
explain (costs off) select all * from (select case when b%2=1 then 'id=1' when b%2=0 then 'id=0' else 'id=2' end as result from t_agg1 group by result order by 1); 
select all * from (select case when b%2=1 then 'id=1' when b%2=0 then 'id=0' else 'id=2' end as result from t_agg1 group by 1 order by 1) order by 1; 
explain (costs off) select sum(x) from (select sum(a) x from t_agg1 group by b);
select sum(x) from (select sum(a) x from t_agg1 group by b);
reset enable_hashagg;

explain (costs off) select x from (select d, sum(a) x from t_agg1 group by 1);
select x from (select d, sum(a) x from t_agg1 group by 1) order by 1;
explain (costs off) select x from (select g, sum(a) x from t_agg1 group by 1);
select x from (select g, sum(a) x from t_agg1 group by 1) order by 1;

set plan_mode_seed=2;
explain (costs off) SELECT DISTINCT * FROM ( SELECT DISTINCT SUM(b) as result1 FROM t_agg1 where b <= 8 group by b having b < 10 order by result1 ASC OFFSET 1 ROW FETCH NEXT 30 ROW ONLY ) AS RESULT where result1 !=(3+3-1) group by result1 order by 1;
SELECT DISTINCT * FROM ( SELECT DISTINCT SUM(b) as result1 FROM t_agg1 where b <= 8 group by b having b < 10 order by result1 ASC OFFSET 1 ROW FETCH NEXT 30 ROW ONLY ) AS RESULT where result1 !=(3+3-1) group by result1 order by 1;
explain (costs off) SELECT ALL * FROM (SELECT DISTINCT CASE WHEN d=1 THEN 'ID1' WHEN d=2 THEN 'ID2' ELSE 'ID0' END as result FROM t_agg1 where d is not null group by d order by 1 OFFSET 1 ROW FETCH NEXT 25 ROW ONLY ) ;
SELECT ALL * FROM (SELECT DISTINCT CASE WHEN d=1 THEN 'ID1' WHEN d=2 THEN 'ID2' ELSE 'ID0' END as result FROM t_agg1 where d is not null group by d order by 1 OFFSET 1 ROW FETCH NEXT 25 ROW ONLY ) ;
reset plan_mode_seed;

-- Clean Table
drop table t_agg1;
drop table t_agg2;

create table distkey_choose(a int, b int, c int);
insert into distkey_choose select generate_series(1, 100), generate_series(2, 101), generate_series(1, 20000) % 400 from tmp_t1;
analyze distkey_choose;
explain (verbose on, costs off) select b, case when c = 1 then 1 else 2 end from distkey_choose group by 1, 2;
explain (verbose on, costs off) select substr(c, 1, 2), a+b, b from distkey_choose group by 1, 2, 3;
explain (verbose on, costs off) select substr(c, 1, 1)||substr(c, 2, 1)||substr(c, 3, 1)||substr(c, 4, 1), a+b, b from distkey_choose group by 1, 2, 3;
explain (verbose on, costs off) select c>b, b from distkey_choose group by 1, 2;
explain (verbose on, costs off) select distinct count(a), count(b) from distkey_choose group by a;
explain (verbose on, costs off) select distinct count(1), count(2) from distkey_choose group by a;
explain (verbose on, costs off) select coalesce(t2.a, 0), t1.b, t1.c+5 from distkey_choose t1 left join distkey_choose t2 on t1.a=t2.a and t2.a=1 group by 1, 2, 3;
explain (verbose on, costs off) select t1.b, t2.a+5, t1.c+5 from distkey_choose t1 inner join distkey_choose t2 on t1.a=t2.a group by 1, 2, 3;
drop table distkey_choose;

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
drop schema if exists distribute_aggregates_part3 cascade;
