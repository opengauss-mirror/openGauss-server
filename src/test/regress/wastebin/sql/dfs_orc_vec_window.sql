set enable_global_stats = true;
----
--- Create Talbe
----
create schema vector_window_engine;
set current_schema=vector_window_engine;

create table vector_window_engine.VECTOR_WINDOW_TABLE_01(
   depname	varchar  
  ,empno	bigint  
  ,salary	int  
  ,enroll	date  
  ,timeset	timetz
)with(orientation = orc) tablespace hdfs_ts;

COPY vector_window_engine.VECTOR_WINDOW_TABLE_01(depname, empno, salary, enroll, timeset) FROM stdin;
develop	10	5200	2007-08-01	16:00:00+08
develop	2	5200	2007-08-01	16:00:00+08
sales	1	5000	2006-10-01	16:30:00+08
personnel	5	3500	2007-12-10	16:30:00+08
sales	4	4800	2007-08-08	08:00:00+08
develop	7	4200	2009-01-01	08:00:00+06
personnel	2	3900	2006-12-23	08:30:00+06
develop	7	4200	2008-01-01	16:00:00+08
develop	9	4500	2008-01-01	16:30:00+08
sales	3	4800	2007-08-01	16:40:00+08
develop	8	6000	2006-10-01	08:30:00+06
develop	11	5200	2007-08-15	08:30:00+06
develop	5	\N	2007-08-15	\N
develop	6	\N	\N	08:30:00+06
\N	\N	\N	\N	\N
\.

create table vector_window_engine.ROW_WINDOW_TABLE_02
(
   depname	varchar
  ,salary	int
  ,enroll	date
);

create table vector_window_engine.VECTOR_WINDOW_TABLE_02
(
   depname	varchar
  ,salary	int
  ,enroll	date
)with(orientation = orc) tablespace hdfs_ts;

CREATE OR REPLACE PROCEDURE func_insert_tbl_window_02()
AS
BEGIN
        FOR I IN 0..2500 LOOP
                if i < 1200 then
                        INSERT INTO vector_window_engine.row_window_table_02 VALUES('develop',4200,'2007-08-08');
				elsif i > 1199 AND i < 2400 then
                        INSERT INTO vector_window_engine.row_window_table_02 VALUES('personnel', 3900, '2008-08-01');
                else
                        INSERT INTO vector_window_engine.row_window_table_02 VALUES('sales',6000,'2009-09-02');
                end if;
        END LOOP;
END;
/
CALL func_insert_tbl_window_02();
insert into vector_window_engine.row_window_table_02 values('develop',3800,'2007-08-08');
insert into vector_window_engine.row_window_table_02 values('develop',3800,'2007-08-08');
insert into vector_window_engine.row_window_table_02 values('develop',4800,'2007-08-08');
insert into vector_window_engine.row_window_table_02 values('develop',5800,'2007-08-08');
insert into vector_window_engine.row_window_table_02 values('develop',5800,'2007-08-08');
insert into vector_window_engine.row_window_table_02 values('personnel',4800,'2008-08-01');
insert into vector_window_engine.row_window_table_02 values('sales',4800,'2009-09-02');
insert into vector_window_engine.row_window_table_02 values('sales',6800,'2009-09-02');
insert into vector_window_table_02 select * from row_window_table_02;

create table vector_window_engine.ROW_WINDOW_TABLE_03
(
   depname	varchar
  ,salary	int
  ,enroll	date
);

create table vector_window_engine.VECTOR_WINDOW_TABLE_03
(
   depname	varchar
  ,salary	int
  ,enroll	date
)with(orientation = orc) tablespace hdfs_ts;

CREATE OR REPLACE PROCEDURE func_insert_tbl_window_03()
AS
BEGIN
        FOR I IN 0..5005 LOOP
                if i < 5000 then
                        INSERT INTO vector_window_engine.row_window_table_03 VALUES('develop',4200+i,'2007-08-08');
                else
                        INSERT INTO vector_window_engine.row_window_table_03 VALUES('sales',6000,'2009-09-02');
                end if;
        END LOOP;
END;
/
CALL func_insert_tbl_window_03();
insert into vector_window_table_03 select * from row_window_table_03;

create table vector_window_engine.VECTOR_WINDOW_TABLE_04
(
   depid	int
  ,salary	int 
, partial cluster key(depid))with (orientation = column)
partition by range (depid)
(
  partition win_tab_hash_1 values less than (5),
  partition win_tab_hash_2 values less than (10),
  partition win_tab_hash_3 values less than (15)
);

insert into vector_window_table_04 select generate_series(0,10), generate_series(10,20);

create table vector_window_engine.ROW_WINDOW_TABLE_05
(
    a	int
   ,b	int
);

create table vector_window_engine.VECTOR_WINDOW_TABLE_05
(
    a	int
   ,b	int
)with(orientation = orc) tablespace hdfs_ts distribute by hash(a);

CREATE OR REPLACE PROCEDURE func_insert_tbl_window_05()
AS
BEGIN
	FOR I IN 0..1999 LOOP
		INSERT INTO vector_window_engine.row_window_table_05 values(1,1);
	END LOOP;
END;
/

CALL func_insert_tbl_window_05();
insert into vector_window_table_05 select * from row_window_table_05;
insert into vector_window_table_05 values (1, 2);

analyze vector_window_table_01;
analyze vector_window_table_02;
analyze vector_window_table_03;
analyze vector_window_table_04;
analyze vector_window_table_05;

----
--- test 1: Basic Test: Rank()
----
select depname, salary, rank() over(partition by depname order by salary) from vector_window_table_01 order by 1, 2;

select depname, timeset, rank() over(partition by depname order by timeset) from vector_window_table_01 order by 1, 2;

SELECT depname, empno, salary, rank() over w FROM vector_window_table_01 window w AS (partition by depname order by salary) order by rank() over w, empno;

select rank() over (partition by depname order by enroll) as rank_1, depname, enroll from vector_window_table_01 order by 2, 3;

select rank() over (order by length('abcd'));

select rank() over (order by 1), count(*) from vector_window_table_01 group by 1;

select rank() over () from vector_window_table_04 order by 1 ;

----
--- test 2: Basic Test: Rank() with big amount;
----
select depname, salary, rank() over(partition by depname order by salary) from vector_window_table_02 order by 1, 2;

select depname, salary, enroll, rank() over(partition by depname order by salary, enroll desc) from vector_window_table_02 order by 1, 2, 3;

select salary, rank() over(order by salary) from vector_window_table_02 order by 1, 2;

select salary, rank() over() from vector_window_table_02 order by 1, 2;

delete from vector_window_table_02 where vector_window_table_02.salary = 4200;
select depname, salary, rank() over(partition by depname order by salary) from vector_window_table_02 order by 1, 2;

delete from vector_window_table_02 where vector_window_table_02.depname = 'develop' or vector_window_table_02.depname = 'sales';
select depname, salary, rank() over(partition by depname order by salary) from vector_window_table_02 order by 1, 2;

select depname, salary, rank() over(partition by depname order by salary, enroll) from vector_window_table_03 order by 1, 2;

select depname, salary, row_number() over(partition by depname order by salary, enroll) from vector_window_table_03 order by 1, 2;

----
--- test 3: boudary case
----
select a, b, rank() over(partition by a order by b) from vector_window_table_05 order by 1, 2;

----
--- test 4: row_number
----
select c_nationkey, c_phone, row_number() over(partition by c_nationkey order by c_phone) from vector_engine.customer, vector_engine.nation where nation.n_nationkey = customer.c_nationkey order by 1,2,3;
select l_returnflag,l_linestatus , l_shipdate, row_number() over(partition by l_returnflag, l_linestatus order by l_shipdate) from vector_engine.lineitem order by 1,2,3,4;
select o_orderpriority, o_orderdate, row_number() over(partition by o_orderpriority, o_shippriority order by o_orderdate) from vector_engine.orders order by 1,2,3;
select s_nationkey, s_phone, row_number() over(partition by s_nationkey order by s_phone) from vector_engine.supplier order by 1,2,3;

----
--- test 5: Other Test: un-supported Window Function of Vector Engine
--- fall back to original non-vectorized plan by turn off GUC param ENABLE_VECTOR_ENGINE.
----
CREATE TABLE VECTOR_WINDOW_ENGINE.EMPSALARY (DEPNAME VARCHAR, EMPNO BIGINT, SALARY INT, ENROLL_DATE DATE) WITH(orientation = orc) tablespace hdfs_ts  DISTRIBUTE BY HASH(EMPNO);
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
DISTRIBUTE BY HASH (ACCNO);

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
DISTRIBUTE BY HASH (ACCNO);

CREATE TABLE VECTOR_WINDOW_ENGINE.COL_TENK1
(
		 FOUR	INT
		,TEN	INT
		,SUM	INT
		,LAST_VALUE	INT 
)
WITH (orientation = orc)  tablespace hdfs_ts;

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
WITH (orientation = orc)  tablespace hdfs_ts;
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
----
--- Drop Tables
----
drop schema vector_window_engine cascade;
