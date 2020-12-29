/*
 * This file is used to test the function of ExecVecHashJoin()---(3)
 */
----
--- Create Table and Insert Data
----
create schema vector_hashjoin_engine_third;
set  current_schema =  vector_hashjoin_engine_third;

\parallel on  8
CREATE TABLE vector_hashjoin_engine_third.MF1_PTHNFSUB(
	STATUS DECIMAL(1,0),
	VALUEDAY DATE)with(orientation=column) distribute by hash(status);
CREATE TABLE vector_hashjoin_engine_third.MF1_NTHPARAT(  
	CURRTYPE DECIMAL(3,0),
	RATECODE DECIMAL(1,0),
	Etl_Tx_Dt DATE) with(orientation=column) distribute by hash(currtype);
 
CREATE TABLE t1_hashConst_col(col_1 int, col_2 int) with(orientation=column) distribute by hash(col_1);
CREATE TABLE t2_hashConst_col(col_1 int, col_2 int) with(orientation=column) distribute by hash(col_1);

create table ROW_HASHJOIN_TABLE_11
(
   c1 int
  ,c2 int
  ,c3 char(64)
)distribute by hash(c1);

create table ROW_HASHJOIN_TABLE_12
(
   c1 int
  ,c2 int
  ,c3 char(64)
)distribute by hash(c2);


create table VECTOR_HASHJOIN_TABLE_11
(
   c1 int
  ,c2 int
  ,c3 char(64)
)with (orientation = column) distribute by hash(c1);

create table VECTOR_HASHJOIN_TABLE_12
(
   c1 int
  ,c2 int
  ,c3 char(64)
)with (orientation = column) distribute by hash(c2);
\parallel off

\parallel on  11
insert into vector_hashjoin_engine_third.MF1_PTHNFSUB values(4, '2014-12-19');
insert into vector_hashjoin_engine_third.MF1_PTHNFSUB values(1, '2014-10-19');
insert into vector_hashjoin_engine_third.MF1_PTHNFSUB values(2, '2014-08-19');
insert into vector_hashjoin_engine_third.MF1_PTHNFSUB values(3, '2014-02-09');
insert into vector_hashjoin_engine_third.MF1_NTHPARAT values (5, 4, '2014-12-19');

insert into t1_hashConst_col values(1, 2);
insert into t2_hashConst_col values(1, 3);

insert into ROW_HASHJOIN_TABLE_11 values (1, 101, 'aec101');
insert into ROW_HASHJOIN_TABLE_11 values (generate_series(2,200), 102, 'aec102');
insert into ROW_HASHJOIN_TABLE_12 values (102, 1, 'bec102');
insert into ROW_HASHJOIN_TABLE_12 values (101, generate_series(2,200), 'bec101');
\parallel off

begin
	for i in 1..6 loop
		insert into VECTOR_HASHJOIN_TABLE_11 select * from ROW_HASHJOIN_TABLE_11;
		insert into VECTOR_HASHJOIN_TABLE_12 select * from ROW_HASHJOIN_TABLE_12;
	end loop;
end;
/
----
--- ICBC Case: HashJoin with case--when expression 
----

explain (verbose on, costs off) select 
	count(*) 
FROM 	
	vector_hashjoin_engine_third.MF1_PTHNFSUB T1  
LEFT JOIN 	
	vector_hashjoin_engine_third.MF1_NTHPARAT T2  
ON 	
	T2.CURRTYPE=1 
AND 	
	CASE WHEN CAST('20141231' AS DATE)-T1.VALUEDAY<=90 THEN substring('abcd', 1)                        	    
	     WHEN CAST('20141231' AS DATE)-T1.VALUEDAY>90 AND CAST('20141231' AS DATE)-T1.VALUEDAY<=180 THEN '0200300'                          	
	     WHEN CAST('20141231' AS DATE)-T1.VALUEDAY>180 AND CAST('20141231' AS DATE)-T1.VALUEDAY<=360 THEN trim('jjhh',4)                          	
	     ELSE '0201200'	
	END=lpad(T2.RATECODE, 7, '0');

select 
	count(*) 
FROM 	
	vector_hashjoin_engine_third.MF1_PTHNFSUB T1  
LEFT JOIN 	
	vector_hashjoin_engine_third.MF1_NTHPARAT T2  
ON 	
	T2.CURRTYPE=1 
AND 	
	CASE WHEN CAST('20141231' AS DATE)-T1.VALUEDAY<=90 THEN substring('abcd', 1)                        	    
	     WHEN CAST('20141231' AS DATE)-T1.VALUEDAY>90 AND CAST('20141231' AS DATE)-T1.VALUEDAY<=180 THEN '0200300'                          	
	     WHEN CAST('20141231' AS DATE)-T1.VALUEDAY>180 AND CAST('20141231' AS DATE)-T1.VALUEDAY<=360 THEN trim('jjhh',4)                          	
	     ELSE '0201200'	
	END=lpad(T2.RATECODE, 7, '0');

	
--test for semi join 

explain (verbose on, costs off)  
SELECT    'COL' AS ETL_SERVER,
    D.ETL_SYSTEM,
    D.ETL_JOB
FROM ETL_JOB_DEPENDENCY D,ETL_JOB_PRIORITY P
WHERE D.ETL_JOB=P.ETL_JOB
AND D.ENABLE='1'
AND D.ETL_JOB IN (
SELECT NEXT_JOB FROM (SELECT NEXT_JOB
FROM (SELECT N2.*, D.DEPENDENCY_JOB AS NEXT_JOB_DEPENDENCY_JOB
FROM (SELECT N.DEPENDENCY_JOB  AS FINISHED_JOB,
    N.ETL_JOB         AS NEXT_JOB
FROM ETL_JOB_DEPENDENCY N
WHERE N.DEPENDENCY_JOB = 'MF1_PTHGKSUB_A'
AND N.ENABLE='1') N2
 left JOIN ETL_JOB_DEPENDENCY D ON (N2.NEXT_JOB = D.ETL_JOB AND D.ENABLE='1')) N3
 left JOIN ETL_JOB J2 ON (J2.ETL_JOB = N3.NEXT_JOB_DEPENDENCY_JOB AND J2.LAST_TXDATE = TO_DATE('2015-05-28 13:49:47','yyyy-mm-dd hh24:mi:ss'))
GROUP BY NEXT_JOB)
) order by 1,2,3;

SELECT    'COL' AS ETL_SERVER,
    D.ETL_SYSTEM,
    D.ETL_JOB
FROM ETL_JOB_DEPENDENCY D,ETL_JOB_PRIORITY P
WHERE D.ETL_JOB=P.ETL_JOB
AND D.ENABLE='1'
AND D.ETL_JOB IN (
SELECT NEXT_JOB FROM (SELECT NEXT_JOB
FROM (SELECT N2.*, D.DEPENDENCY_JOB AS NEXT_JOB_DEPENDENCY_JOB
FROM (SELECT N.DEPENDENCY_JOB  AS FINISHED_JOB,
    N.ETL_JOB         AS NEXT_JOB
FROM ETL_JOB_DEPENDENCY N
WHERE N.DEPENDENCY_JOB = 'MF1_PTHGKSUB_A'
AND N.ENABLE='1') N2
 left JOIN ETL_JOB_DEPENDENCY D ON (N2.NEXT_JOB = D.ETL_JOB AND D.ENABLE='1')) N3
 left JOIN ETL_JOB J2 ON (J2.ETL_JOB = N3.NEXT_JOB_DEPENDENCY_JOB AND J2.LAST_TXDATE = TO_DATE('2015-05-28 13:49:47','yyyy-mm-dd hh24:mi:ss'))
GROUP BY NEXT_JOB)
) order by 1,2,3;
   
explain (verbose on, costs off)select count(*) from t1_hashConst_col left join t2_hashConst_col on(t1_hashConst_col.col_1 = t2_hashConst_col.col_1 and t1_hashConst_col.col_2 > t2_hashConst_col.col_2);

select count(*) from t1_hashConst_col left join t2_hashConst_col on(t1_hashConst_col.col_1 = t2_hashConst_col.col_1 and t1_hashConst_col.col_2 > t2_hashConst_col.col_2);

---
-- test repartition process in hash join
---

analyze VECTOR_HASHJOIN_TABLE_11;
analyze VECTOR_HASHJOIN_TABLE_12;

set work_mem=64;
set enable_nestloop=off;
set enable_mergejoin=off;
set enable_compress_spill=on;
explain (verbose on, costs off) select A.c2, count(*) from VECTOR_HASHJOIN_TABLE_11 A inner join VECTOR_HASHJOIN_TABLE_12 B on A.c2=B.c1 group by A.c2 order by A.c2
select A.c2, count(*) from VECTOR_HASHJOIN_TABLE_11 A inner join VECTOR_HASHJOIN_TABLE_12 B on A.c2=B.c1 group by A.c2 order by A.c2;
select B.c1, count(*) from VECTOR_HASHJOIN_TABLE_11 A right join VECTOR_HASHJOIN_TABLE_12 B on A.c2=B.c1 group by B.c1 order by B.c1;
select A.c3, count(*) from VECTOR_HASHJOIN_TABLE_11 A inner join VECTOR_HASHJOIN_TABLE_12 B on substring(A.c3, 2) = substring(B.c3, 2) group by A.c3 order by A.c3;
select B.c3, count(*) from VECTOR_HASHJOIN_TABLE_11 A right join VECTOR_HASHJOIN_TABLE_12 B on substring(A.c3, 2) = substring(B.c3, 2) group by B.c3 order by B.c3;
select B.c3, count(*) from VECTOR_HASHJOIN_TABLE_11 A right join VECTOR_HASHJOIN_TABLE_12 B on substring(A.c3, 2) = substring(B.c3, 2) and A.c1+1=B.c2+2 group by B.c3 order by B.c3;

set enable_compress_spill=off;
explain (verbose on, costs off) select B.c3, count(*) from VECTOR_HASHJOIN_TABLE_11 A right join VECTOR_HASHJOIN_TABLE_12 B on substring(A.c3, 2) = substring(B.c3, 2) group by B.c3 order by B.c3;
select A.c2, count(*) from VECTOR_HASHJOIN_TABLE_11 A inner join VECTOR_HASHJOIN_TABLE_12 B on A.c2=B.c1 group by A.c2 order by A.c2;
select B.c1, count(*) from VECTOR_HASHJOIN_TABLE_11 A right join VECTOR_HASHJOIN_TABLE_12 B on A.c2=B.c1 group by B.c1 order by B.c1;
select A.c3, count(*) from VECTOR_HASHJOIN_TABLE_11 A inner join VECTOR_HASHJOIN_TABLE_12 B on substring(A.c3, 2) = substring(B.c3, 2) group by A.c3 order by A.c3;
select B.c3, count(*) from VECTOR_HASHJOIN_TABLE_11 A right join VECTOR_HASHJOIN_TABLE_12 B on substring(A.c3, 2) = substring(B.c3, 2) group by B.c3 order by B.c3;
select B.c3, count(*) from VECTOR_HASHJOIN_TABLE_11 A right join VECTOR_HASHJOIN_TABLE_12 B on substring(A.c3, 2) = substring(B.c3, 2) and A.c1+1=B.c2+2 group by B.c3 order by B.c3;


reset work_mem;
set query_dop = 2002;

----
--- Clean table and resource
----

drop schema vector_hashjoin_engine_third cascade;
