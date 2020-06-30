
set current_schema=vector_window_engine;
----
--- test 3: boudary case
----
select a, b, rank() over(partition by a order by b) from vector_window_table_05 order by 1, 2;
select a, b, dense_rank() over(partition by a order by b) from vector_window_table_05 order by 1, 2;
select a, b, rank() over(partition by a order by b), count(a) over(partition by a), sum(b) over(partition by a) from vector_window_table_05 order by 1, 2;
----
--- test 4: row_number
----
select c_nationkey, c_phone, row_number() over(partition by c_nationkey order by c_phone) from vector_engine.customer, vector_engine.nation where nation.n_nationkey = customer.c_nationkey order by 1, 2, 3;
select l_returnflag,l_linestatus , l_shipdate, row_number() over(partition by l_returnflag, l_linestatus order by l_shipdate) from vector_engine.lineitem order by 1,2,3,4;
select o_orderpriority, o_orderdate, row_number() over(partition by o_orderpriority, o_shippriority order by o_orderdate) from vector_engine.orders order by 1,2,3;
select s_nationkey, s_phone, row_number() over(partition by s_nationkey order by s_phone) from vector_engine.supplier order by 1, 2, 3;

select l_orderkey, l_partkey,  count(l_comment) over (partition by l_orderkey), rank() over (partition by l_orderkey) from vector_engine.lineitem order by 1, 2;

select l_orderkey, l_partkey,  count(l_comment) over (partition by l_orderkey order by l_partkey), rank() over (partition by l_orderkey order by l_partkey), row_number() over (partition  by l_orderkey order by l_partkey) from vector_engine.lineitem order by 1, 2, 3, 4, 5;

select l_orderkey, l_partkey,  avg(l_orderkey) over (partition by l_orderkey order by l_partkey), rank() over (partition by l_orderkey order by l_partkey), row_number() over (partition  by l_orderkey order by l_partkey) from vector_engine.lineitem order by 1, 2, 3, 4, 5 limit 100;
----
--- test 5: Other Test: un-supported Window Function of Vector Engine
--- fall back to original non-vectorized plan by turn off GUC param ENABLE_VECTOR_ENGINE.
----
CREATE TABLE VECTOR_WINDOW_ENGINE.EMPSALARY (DEPNAME VARCHAR, EMPNO BIGINT, SALARY INT, ENROLL_DATE DATE) WITH(ORIENTATION =COLUMN)  ;
INSERT INTO VECTOR_WINDOW_ENGINE.EMPSALARY VALUES
('develop', 10, 5200, '2007-08-01'),
('sales', 1, 5000, '2006-10-01'),
('personnel', 5, 3500, '2007-12-10'),
('sales', 4, 4800, '2007-08-08'),
('personnel', 2, 3900, '2006-12-23'),
('develop', 7, 4200, '2008-01-01'),
('develop', 9, 4500, '2008-01-01'),
('sales', 3, 4800, '2007-08-01'),
('develop', 8, 6000, '2006-10-01'),
('develop', 11, 5200, '2007-08-15');

CREATE UNLOGGED TABLE VECTOR_WINDOW_ENGINE.TMP_CSUM_1
(
      ACCNO                  VARCHAR(17)
      ,TIMESTMP              VARCHAR(26)
      ,BANK_FLAG             CHAR(3)
      ,AMOUNT                NUMERIC(17)
      ,AMOUNTSUM             NUMERIC(17)
      ,LOANBAL               NUMERIC(17)
      ,Etl_Tx_Dt             DATE
)
WITH (ORIENTATION=COLUMN)
 ;

CREATE UNLOGGED TABLE VECTOR_WINDOW_ENGINE.LOAN_DPSIT_ACCT_EVENT_INFO
(     ACCNO                  VARCHAR(17)
      ,CINO                  VARCHAR(15)
      ,ORGANNO               VARCHAR(10)
      ,AMOUNT                NUMERIC(17)
      ,BANK_FLAG             CHAR(3)
      ,TIMESTMP              VARCHAR(26)
      ,LOANBAL               NUMERIC(17)
      ,Etl_Tx_Dt             DATE
)
WITH (ORIENTATION=COLUMN)
 ;

CREATE TABLE VECTOR_WINDOW_ENGINE.COL_TENK1
(
		 FOUR	INT
		,TEN	INT
		,SUM	INT
		,LAST_VALUE	INT 
)
WITH (ORIENTATION=COLUMN);

CREATE TABLE VECTOR_WINDOW_ENGINE.TENK1
(
UNIQUE1 INT4
,UNIQUE2 INT4
,TWO INT4
,FOUR INT4
,TEN INT4
,TWENTY INT4
,HUNDRED INT4
,THOUSAND INT4
,TWOTHOUSAND INT4
,FIVETHOUS INT4
,TENTHOUS INT4
,ODD INT4
,EVEN INT4
,STRINGU1 VARCHAR
,STRINGU2 VARCHAR
,STRING4 VARCHAR
)
WITH (ORIENTATION=COLUMN);
INSERT INTO VECTOR_WINDOW_ENGINE.TENK1 SELECT * FROM PUBLIC.TENK1;

SET ENABLE_VECTOR_ENGINE = OFF;

EXPLAIN (COSTS OFF)
SELECT EMPNO, DEPNAME, SALARY, BONUS, DEPADJ, MIN(BONUS) OVER (ORDER BY EMPNO), MAX(DEPADJ) OVER () FROM(
	SELECT *,
		CASE WHEN ENROLL_DATE < '2008-01-01' THEN 2008 - EXTRACT(YEAR FROM ENROLL_DATE) END * 500 AS BONUS,
		CASE WHEN
			AVG(SALARY) OVER (PARTITION BY DEPNAME) < SALARY
		THEN 200 END AS DEPADJ FROM EMPSALARY
)S ORDER BY EMPNO;

SELECT EMPNO, DEPNAME, SALARY, BONUS, DEPADJ, MIN(BONUS) OVER (ORDER BY EMPNO), MAX(DEPADJ) OVER () FROM(
	SELECT *,
		CASE WHEN ENROLL_DATE < '2008-01-01' THEN 2008 - EXTRACT(YEAR FROM ENROLL_DATE) END * 500 AS BONUS,
		CASE WHEN
			AVG(SALARY) OVER (PARTITION BY DEPNAME) < SALARY
		THEN 200 END AS DEPADJ FROM EMPSALARY
)S ORDER BY EMPNO;

EXPLAIN (COSTS OFF)
INSERT INTO TMP_CSUM_1
  (ACCNO,
   TIMESTMP,
   BANK_FLAG,
   AMOUNT,
   AMOUNTSUM,
   LOANBAL,
   Etl_Tx_Dt)
SELECT ACCNO, TIMESTMP, BANK_FLAG, AMOUNT, SUM(AMOUNT) OVER(PARTITION BY ACCNO ORDER BY ACCNO, TIMESTMP ROWS UNBOUNDED PRECEDING), LOANBAL, to_date('20141231', 'YYYYMMDD') FROM LOAN_DPSIT_ACCT_EVENT_INFO;

EXPLAIN (COSTS OFF)
INSERT INTO COL_TENK1
SELECT FOUR, TEN,
	SUM(TEN) OVER (PARTITION BY FOUR ORDER BY TEN RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
	LAST_VALUE(TEN) OVER (PARTITION BY FOUR ORDER BY TEN RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
FROM (SELECT DISTINCT TEN, FOUR FROM TENK1) SS ORDER BY 1, 2, 3, 4;

INSERT INTO COL_TENK1
SELECT FOUR, TEN,
	SUM(TEN) OVER (PARTITION BY FOUR ORDER BY TEN RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
	LAST_VALUE(TEN) OVER (PARTITION BY FOUR ORDER BY TEN RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
FROM (SELECT DISTINCT TEN, FOUR FROM TENK1) SS ORDER BY 1, 2, 3, 4;

SELECT * FROM COL_TENK1 ORDER BY 1,2,3,4;
RESET ENABLE_VECTOR_ENGINE;

set enable_vector_engine=on;
--test window agg function
create table temp_t1 (a int, b int, c int);
insert into temp_t1 values(1,1,1);
create table tmp_t1(a int, b int, c int);
insert into tmp_t1 select generate_series(1, 10000), generate_series(1, 10000), generate_series(1, 10000) from temp_t1;
insert into tmp_t1 values(NULL,10000002, NULL);
create table vec_count_over(a int, b int, c int)with(orientation = column);
insert into vec_count_over select * from tmp_t1;
select a, b, count(1) over()  from vec_count_over order by 1, 2 limit 10;

explain (verbose on, costs off)
select  a,  count(a) over (partition by a) from vec_count_over order by 1,2;

select  a,  count(a) over (partition by a) from vec_count_over order by 1,2;
select  a,  count(*) over (partition by a) from vec_count_over order by 1,2;

select  a, sum(a) over (partition by a) from vec_count_over order by 1,2;

select  a, sum(a) over (partition by a),  count(a) over (partition by a) from vec_count_over order by 1,2;

select  a, sum(a) over (partition by a), sum(a) over (partition by a) from vec_count_over order by 1,2;

select  a, sum(a) over (partition by a),  count(a) over (partition by a),  rank() over (partition by a),  row_number() over (partition by a) from vec_count_over order by 1,2;

explain (costs off)  select avg(l_discount) over(partition by l_partkey), min(l_tax) over (partition by l_partkey), max(l_tax) over (partition by l_partkey) from vector_engine.lineitem order by 1,2,3 limit 100;
 
select avg(l_discount) over(partition by l_partkey), min(l_tax) over (partition by l_partkey), max(l_tax) over (partition by l_partkey) from vector_engine.lineitem order by 1,2,3 limit 100;

---test case with spill
set work_mem='64kB';
set hashagg_table_size=100000;
select col_timetz, rank() over (order by 1), count(*) from vector_window_table_06 group by 1 order by 1, 2, 3;
select col_interval, rank() over (order by 1), count(*) from vector_window_table_06 group by 1 order by 1, 2, 3;
select col_tinterval, rank() over (order by 1), count(*) from vector_window_table_06 group by 1 order by 1, 2, 3;
reset work_mem;
reset hashagg_table_size;

create table hl_test001(a int,b varchar2(10)) with (ORIENTATION = COLUMN); 
insert into hl_test001 values(1,'gauss,ap'); 
insert into hl_test001 values(2,'xi,an'); 

select sum(case a when 1 then 1 else 0 end) over (partition by b) from hl_test001 order by 1;

analyze vector_engine.lineitem;

explain (costs off)
select * from
(select l_returnflag, l_shipdate, row_number() over (partition by l_returnflag order by l_shipdate) as rn
from vector_engine.lineitem
) as AA where AA.rn < 10 order by 1, 2, 3;

select * from
(select l_returnflag, l_shipdate, row_number() over (partition by l_returnflag order by l_shipdate) as rn
from vector_engine.lineitem
) as AA where AA.rn < 10 order by 1, 2, 3;

explain (costs off)
select * from
(select l_returnflag, l_shipdate, rank() over (partition by l_returnflag order by l_shipdate) as rk
from vector_engine.lineitem
) as AA where AA.rk < 10 order by 1, 2, 3;

select * from
(select l_returnflag, l_shipdate, rank() over (partition by l_returnflag order by l_shipdate) as rk
from vector_engine.lineitem
) as AA where AA.rk < 10 order by 1, 2, 3;

explain (verbose on, costs off)
select * from
(select l_returnflag, l_shipdate, rank() over (partition by l_returnflag order by l_shipdate) as rk,
   row_number() over (partition by l_returnflag order by l_shipdate) as rn
from vector_engine.lineitem
) as AA where AA.rk < 10 order by 1, 2, 3;

select * from
(select l_returnflag, l_shipdate, rank() over (partition by l_returnflag order by l_shipdate) as rk,
   row_number() over (partition by l_returnflag order by l_shipdate) as rn
from vector_engine.lineitem
) as AA where AA.rk < 10 order by 1, 2, 3;

explain (verbose on, costs off)
select * from
(select l_returnflag, l_shipdate,
   sum(1) over (partition by l_returnflag order by l_returnflag) as rn,
   rank() over (partition by l_shipdate order by l_returnflag) as rk
from vector_engine.lineitem
) as AA where AA.rk < 10 order by 1, 2, 3;

select * from
(select l_returnflag, l_shipdate,
   sum(1) over (partition by l_returnflag order by l_returnflag) as rn,
   rank() over (partition by l_returnflag order by l_shipdate) as rk
from vector_engine.lineitem
) as AA where AA.rk < 10 order by 1, 2, 3;

explain (verbose on, costs off)
select * from
(select l_returnflag, l_shipdate, rank() over (partition by l_returnflag order by l_shipdate) as rk,
   row_number() over (partition by l_returnflag order by l_shipdate) as rn
from vector_engine.lineitem
) as AA where AA.rk <= 0 order by 1, 2, 3;

select * from
(select l_returnflag, l_shipdate, rank() over (partition by l_returnflag order by l_shipdate) as rk,
   row_number() over (partition by l_returnflag order by l_shipdate) as rn
from vector_engine.lineitem
) as AA where AA.rk <= 0 order by 1, 2, 3;

-- >=can not use two level
explain (verbose on, costs off)
select * from
(select l_returnflag, l_shipdate, rank() over (partition by l_returnflag order by l_shipdate) as rk,
   row_number() over (partition by l_returnflag order by l_shipdate) as rn
from vector_engine.lineitem
) as AA where AA.rk >= 10 order by 1, 2, 3;

-- rank is not upper levels, can not use two level
explain (verbose on, costs off)
select * from
(select l_returnflag, l_shipdate, rank() over (partition by l_returnflag order by l_shipdate),
   sum(1) over (partition by l_shipdate order by l_returnflag) as rk
from vector_engine.lineitem
) as AA where AA.rk < 10 order by 1, 2, 3;

create table window_t1(a int, b int) with (orientation=column);
create table window_t_rep(a int, b int) with (orientation=column)  ;
insert into window_t1 select generate_series(1,5), generate_series(1,5); 
insert into window_t_rep select generate_series(1,5), generate_series(1,5); 
analyze window_t1;
analyze window_t_rep;
-- test on multi-windowagg case
explain (costs off) select * from (select a, sum(b) over (order by a rows 2 preceding), sum(a+b) over (partition by a order by a),
sum(b) over (order by a desc rows 2 preceding), sum(a+b) over (partition by a order by a desc),
sum(b) over (order by b, a rows 2 preceding) from window_t1) t1, window_t1 t2 where t1.a=t2.a order by 1;
select * from (select a, sum(b) over (order by a rows 2 preceding), sum(a+b) over (partition by a order by a),
sum(b) over (order by a desc rows 2 preceding), sum(a+b) over (partition by a order by a desc),
sum(b) over (order by b, a rows 2 preceding) from window_t1) t1, window_t1 t2 where t1.a=t2.a order by 1;
explain (costs off) select * from (select a, sum(b) over (order by a), sum(a+b) over (partition by a order by a),
sum(b) over (order by a desc), sum(a+b) over (partition by a order by a desc),
sum(b) over (order by b, a) from window_t1) t1, window_t1 t2 where t1.a=t2.a order by 1;
select * from (select a, sum(b) over (order by a), sum(a+b) over (partition by a order by a),
sum(b) over (order by a desc), sum(a+b) over (partition by a order by a desc),
sum(b) over (order by b, a) from window_t1) t1, window_t1 t2 where t1.a=t2.a order by 1;
-- test on append case
explain (costs off) (select a, sum(b) over (order by a rows 2 preceding) from window_t1 union all
select a, sum(b) over (order by a rows 2 preceding) from window_t1) order by 1,2;
(select a, sum(b) over (order by a rows 2 preceding) from window_t1 union all
select a, sum(b) over (order by a rows 2 preceding) from window_t1) order by 1,2;
explain (costs off) (select a, sum(b) over (order by a rows 2 preceding) from window_t1 union all select a, b from window_t_rep) order by 1,2;
(select a, sum(b) over (order by a rows 2 preceding) from window_t1 union all select a, b from window_t_rep) order by 1,2;
explain (costs off) (select a, sum(b) over (order by a rows 2 preceding) from window_t1 union all select a, b from window_t_rep
union select a, sum(b) over (order by a rows 2 preceding) from window_t1) order by 1,2;
(select a, sum(b) over (order by a rows 2 preceding) from window_t1 union all select a, b from window_t_rep
union select a, sum(b) over (order by a rows 2 preceding) from window_t1) order by 1,2;
drop table window_t1;
drop table window_t_rep;

create table vector_window_engine.vector_window_table_08
(
    c1 timestamp, 
    c2 int
) with(orientation=column)  ;
insert into vector_window_engine.vector_window_table_08 values('2018-06-20',generate_series(1,1010));
analyze vector_window_engine.vector_window_table_08;
select * from (select *, rank() over (order by c1) as RN from vector_window_engine.vector_window_table_08) where c2 = 1000;
select * from (select *, rank() over (order by c1) as RN from vector_window_engine.vector_window_table_08) where c2 = 1001;

drop table vector_window_engine.vector_window_table_08;
create table vector_window_engine.vector_window_table_08
(
    c1 timestamp, 
    c2 int
) with(orientation=column)  ;
insert into vector_window_engine.vector_window_table_08 values('2018-06-20',generate_series(1,999));
insert into vector_window_engine.vector_window_table_08 values('2018-06-21',1000);
insert into vector_window_engine.vector_window_table_08 values('2018-06-22',1001);
analyze vector_window_engine.vector_window_table_08;
select * from (select *, rank() over (order by c1) as RN from vector_window_engine.vector_window_table_08) where c2 = 1000;
select * from (select *, rank() over (order by c1) as RN from vector_window_engine.vector_window_table_08) where c2 = 1001;

drop table vector_window_engine.vector_window_table_08;
create table vector_window_engine.vector_window_table_08
(
    c1 timestamp, 
    c2 int
) with(orientation=column)  ;
insert into vector_window_engine.vector_window_table_08 values('2018-06-20',generate_series(1,1000));
insert into vector_window_engine.vector_window_table_08 values('2018-06-21',1001);
analyze vector_window_engine.vector_window_table_08;
select * from (select *, rank() over (order by c1) as RN from vector_window_engine.vector_window_table_08) where c2 = 1000;
select * from (select *, rank() over (order by c1) as RN from vector_window_engine.vector_window_table_08) where c2 = 1001;
drop table vector_window_engine.vector_window_table_08;
