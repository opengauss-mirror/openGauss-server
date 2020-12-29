set enable_global_stats = true;

/*
 * This file is used to test the function of ExecVecHashJoin()
 */
----
--- Create Table and Insert Data
----
create schema vector_hashjoin_engine;
set current_schema=vector_hashjoin_engine;
set codegen_cost_threshold = 0;
CREATE TABLE vector_hashjoin_engine.ROW_HASHJOIN_TABLE_01(
 C_CHAR_1 CHAR(1),
 C_CHAR_2 CHAR(10),
 C_CHAR_3 CHAR(100),
 C_VARCHAR_1 VARCHAR(1),
 C_VARCHAR_2 VARCHAR(10),
 C_VARCHAR_3 VARCHAR(1024),
 C_INT BIGINT,
 C_BIGINT BIGINT,
 C_SMALLINT BIGINT,
 C_FLOAT FLOAT,
 C_NUMERIC numeric(20,5),
 C_DP double precision,
 C_DATE DATE,
 C_TS_WITHOUT TIMESTAMP WITHOUT TIME ZONE,
 C_TS_WITH TIMESTAMP WITH TIME ZONE );

CREATE OR REPLACE PROCEDURE func_insert_tbl_hashjoin_01()
AS  
BEGIN  
       FOR I IN 1..50 LOOP  
	   FOR J IN 1..30 LOOP  
         INSERT INTO ROW_HASHJOIN_TABLE_01 VALUES('A','b20_000aa','b20_000aaaaAHWGS','a','aaadf','b20_000aaaaaaaFAFEFAGEAFEAFEAGEAGEAGEE_'||i,i,j,i+j,i+0.0001,i+0.00001,i+0.000001,'2000-01-01','2000-01-01 01:01:01','2000-01-01 01:01:01+01');
       END LOOP; 
	    END LOOP; 
END;
/
CALL func_insert_tbl_hashjoin_01();
select count(*) from ROW_HASHJOIN_TABLE_01;

CREATE TABLE vector_hashjoin_engine.ROW_HASHJOIN_TABLE_02(
 C_CHAR_1 CHAR(1),
 C_CHAR_2 CHAR(10),
 C_CHAR_3 CHAR(100),
 C_VARCHAR_1 VARCHAR(1),
 C_VARCHAR_2 VARCHAR(10),
 C_VARCHAR_3 VARCHAR(1024),
 C_INT BIGINT,
 C_BIGINT BIGINT,
 C_SMALLINT BIGINT,
 C_FLOAT FLOAT,
 C_NUMERIC numeric(20,5),
 C_DP double precision,
 C_DATE DATE,
 C_TS_WITHOUT TIMESTAMP WITHOUT TIME ZONE,
 C_TS_WITH TIMESTAMP WITH TIME ZONE );

CREATE OR REPLACE PROCEDURE func_insert_tbl_hashjoin_02()
AS  
BEGIN  
       FOR I IN 5..54 LOOP  
	   FOR J IN 5..34 LOOP  
         INSERT INTO ROW_HASHJOIN_TABLE_02 VALUES('B','b20_001eq','b20_001XXXXEFFE','b','bbbbb','b20_001bbbbbxxdfregwhrghwerhwrwhrwehrewhr_'||i,i,j,i+j,i+0.0001,I+0.00001,I+0.000001,'2000-01-01','2000-01-01 01:01:01','2000-01-01 01:01:01+01');
       END LOOP; 
	    END LOOP; 
END;
/
CALL func_insert_tbl_hashjoin_02();
select count(*) from ROW_HASHJOIN_TABLE_02;

CREATE TABLE vector_hashjoin_engine.VECTOR_HASHJOIN_TABLE_01(
 C_CHAR_1 CHAR(1),
 C_CHAR_2 CHAR(10),
 C_CHAR_3 CHAR(100),
 C_VARCHAR_1 VARCHAR(1),
 C_VARCHAR_2 VARCHAR(10),
 C_VARCHAR_3 VARCHAR(1024),
 C_INT BIGINT,
 C_BIGINT BIGINT,
 C_SMALLINT BIGINT,
 C_FLOAT FLOAT,
 C_NUMERIC numeric(20,5),
 C_DP double precision,
 C_DATE DATE,
 C_TS_WITHOUT TIMESTAMP WITHOUT TIME ZONE,
 C_TS_WITH TIMESTAMP WITH TIME ZONE, PARTIAL CLUSTER KEY(C_DP))WITH (orientation = orc) tablespace hdfs_ts;
INSERT INTO vector_hashjoin_engine.VECTOR_HASHJOIN_TABLE_01 SELECT * FROM vector_hashjoin_engine.ROW_HASHJOIN_TABLE_01;
select count(*) from VECTOR_HASHJOIN_TABLE_01;

CREATE TABLE vector_hashjoin_engine.VECTOR_HASHJOIN_TABLE_02(
 C_CHAR_1 CHAR(1),
 C_CHAR_2 CHAR(10),
 C_CHAR_3 CHAR(100),
 C_VARCHAR_1 VARCHAR(1),
 C_VARCHAR_2 VARCHAR(10),
 C_VARCHAR_3 VARCHAR(1024),
 C_INT BIGINT,
 C_BIGINT BIGINT,
 C_SMALLINT BIGINT,
 C_FLOAT FLOAT,
 C_NUMERIC numeric(20,5),
 C_DP double precision,
 C_DATE DATE,
 C_TS_WITHOUT TIMESTAMP WITHOUT TIME ZONE,
 C_TS_WITH TIMESTAMP WITH TIME ZONE, PARTIAL CLUSTER KEY(C_INT))WITH (orientation = orc) tablespace hdfs_ts  DISTRIBUTE BY HASH (C_INT);
 
INSERT INTO vector_hashjoin_engine.VECTOR_HASHJOIN_TABLE_02 SELECT * FROM vector_hashjoin_engine.ROW_HASHJOIN_TABLE_02;
select count(*) from VECTOR_HASHJOIN_TABLE_02;

CREATE TABLE vector_hashjoin_engine.ROW_HASHJOIN_TABLE_03(
 C_CHAR_1 CHAR(1),
 C_CHAR_2 CHAR(10),
 C_CHAR_3 CHAR(100),
 C_VARCHAR_1 VARCHAR(1),
 C_VARCHAR_2 VARCHAR(10),
 C_VARCHAR_3 VARCHAR(1024),
 C_INT BIGINT,
 C_BIGINT BIGINT,
 C_SMALLINT BIGINT,
 C_FLOAT FLOAT,
 C_NUMERIC numeric(20,5),
 C_DP double precision,
 C_DATE DATE,
 C_TS_WITHOUT TIMESTAMP WITHOUT TIME ZONE,
 C_TS_WITH TIMESTAMP WITH TIME ZONE );

CREATE OR REPLACE PROCEDURE func_insert_tbl_hashjoin_03()
AS  
BEGIN  
       FOR I IN 1..30 LOOP  
	   FOR J IN 1..20 LOOP  
         INSERT INTO ROW_HASHJOIN_TABLE_03 VALUES('A','path_inner','path_inner_aaaaAHWGS','a','aaadf','path_inner_aaaaaaaFAFEFAGEAFEAFEAGEAGEAGEE_'||i,i,j,i+j,i+0.0001,i+0.00001,i+0.000001,'2000-01-01','2000-01-01 01:01:01','2000-01-01 01:01:01+01');
       END LOOP; 
	    END LOOP; 
END;
/
CALL func_insert_tbl_hashjoin_03();

CREATE TABLE vector_hashjoin_engine.ROW_HASHJOIN_TABLE_04(
 C_CHAR_1 CHAR(1),
 C_CHAR_2 CHAR(10),
 C_CHAR_3 CHAR(100),
 C_VARCHAR_1 VARCHAR(1),
 C_VARCHAR_2 VARCHAR(10),
 C_VARCHAR_3 VARCHAR(1024),
 C_INT BIGINT,
 C_BIGINT BIGINT,
 C_SMALLINT BIGINT,
 C_FLOAT FLOAT,
 C_NUMERIC numeric(20,5),
 C_DP double precision,
 C_DATE DATE,
 C_TS_WITHOUT TIMESTAMP WITHOUT TIME ZONE,
 C_TS_WITH TIMESTAMP WITH TIME ZONE );

CREATE OR REPLACE PROCEDURE func_insert_tbl_hashjoin_04()
AS  
BEGIN  
       FOR I IN 1..15 LOOP  
	   FOR J IN 1..10 LOOP  
         INSERT INTO ROW_HASHJOIN_TABLE_04 VALUES('B','b20_001eq','b20_001XXXXEFFE','b','bbbbb','b20_001bbbbbxxdfregwhrghwerhwrwhrwehrewhr_'||i,i,j,i+j,i+0.0001,I+0.00001,I+0.000001,'2000-01-01','2000-01-01 01:01:01','2000-01-01 01:01:01+01');
       END LOOP; 
	    END LOOP; 
END;
/
CALL func_insert_tbl_hashjoin_04();

CREATE TABLE vector_hashjoin_engine.VECTOR_HASHJOIN_TABLE_03(
 C_CHAR_1 CHAR(1),
 C_CHAR_2 CHAR(10),
 C_CHAR_3 CHAR(100),
 C_VARCHAR_1 VARCHAR(1),
 C_VARCHAR_2 VARCHAR(10),
 C_VARCHAR_3 VARCHAR(1024),
 C_INT BIGINT,
 C_BIGINT BIGINT,
 C_SMALLINT BIGINT,
 C_FLOAT FLOAT,
 C_NUMERIC numeric(20,5),
 C_DP double precision,
 C_DATE DATE,
 C_TS_WITHOUT TIMESTAMP WITHOUT TIME ZONE,
 C_TS_WITH TIMESTAMP WITH TIME ZONE ) WITH (orientation = orc) tablespace hdfs_ts;
 insert into vector_hashjoin_engine.VECTOR_HASHJOIN_TABLE_03 select * from vector_hashjoin_engine.ROW_HASHJOIN_TABLE_03;
 
 CREATE TABLE vector_hashjoin_engine.VECTOR_HASHJOIN_TABLE_04(
 C_CHAR_1 CHAR(1),
 C_CHAR_2 CHAR(10),
 C_CHAR_3 CHAR(100),
 C_VARCHAR_1 VARCHAR(1),
 C_VARCHAR_2 VARCHAR(10),
 C_VARCHAR_3 VARCHAR(1024),
 C_INT BIGINT,
 C_BIGINT BIGINT,
 C_SMALLINT BIGINT,
 C_FLOAT FLOAT,
 C_NUMERIC numeric(20,5),
 C_DP double precision,
 C_DATE DATE,
 C_TS_WITHOUT TIMESTAMP WITHOUT TIME ZONE,
 C_TS_WITH TIMESTAMP WITH TIME ZONE )  WITH (orientation = orc) tablespace hdfs_ts;
 insert into vector_hashjoin_engine.VECTOR_HASHJOIN_TABLE_04 select * from vector_hashjoin_engine.ROW_HASHJOIN_TABLE_04;

create table vector_hashjoin_engine.ROW_HASHJOIN_TABLE_05
(
   c1 int
  ,c2 int
  ,c3 char(100)
)distribute by hash(c1);

create table vector_hashjoin_engine.ROW_HASHJOIN_TABLE_06
(
   c1 int
  ,c2 int
  ,c3 char(100)
)distribute by hash(c2);

insert into ROW_HASHJOIN_TABLE_05 select generate_series(1,10000), generate_series(1,10000), 'row'|| generate_series(1,10000);
insert into ROW_HASHJOIN_TABLE_06 select generate_series(5000,10000,1), generate_series(5000,10000,1), 'row'|| generate_series(1,5001);

create table vector_hashjoin_engine.VECTOR_HASHJOIN_TABLE_05
(
   c1 int
  ,c2 int
  ,c3 char(100)
)with (orientation = orc) tablespace hdfs_ts  distribute by hash(c1);

create table vector_hashjoin_engine.VECTOR_HASHJOIN_TABLE_06
(
   c1 int
  ,c2 int
  ,c3 char(100)
)with (orientation = orc) tablespace hdfs_ts  distribute by hash(c2);

insert into VECTOR_HASHJOIN_TABLE_05 select * from ROW_HASHJOIN_TABLE_05;
insert into VECTOR_HASHJOIN_TABLE_06 select * from ROW_HASHJOIN_TABLE_06;

create table vector_hashjoin_engine.VECTOR_HASHJOIN_TABLE_09
(
   c1 int1
  ,c2 int
)with (orientation = orc) tablespace hdfs_ts  distribute by hash(c2);

create table vector_hashjoin_engine.VECTOR_HASHJOIN_TABLE_10
(
   c1 int1
  ,c2 int
)with (orientation = orc) tablespace hdfs_ts  distribute by hash(c2);

insert into VECTOR_HASHJOIN_TABLE_09 select generate_series(1,50)%2;
insert into VECTOR_HASHJOIN_TABLE_10 select generate_series(1,50)%4;

analyze vector_hashjoin_table_01;
analyze vector_hashjoin_table_02;
analyze vector_hashjoin_table_03;
analyze vector_hashjoin_table_04;
analyze vector_hashjoin_table_05;
analyze vector_hashjoin_table_06;
analyze vector_hashjoin_table_09;
analyze vector_hashjoin_table_10;


set enable_nestloop=off;
set enable_mergejoin=off;

----
--- case 1: HashJoin Inner Join
----
explain (verbose on, costs off) SELECT A.C_INT, A.C_BIGINT, A.C_VARCHAR_3, A.C_DATE FROM VECTOR_HASHJOIN_TABLE_01 A INNER JOIN VECTOR_HASHJOIN_TABLE_02 B ON A.C_INT = B.C_INT WHERE A.C_INT <8  AND  A.C_BIGINT<9  ORDER BY 1, 2;

SELECT A.C_INT, A.C_BIGINT, A.C_VARCHAR_3, A.C_DATE FROM VECTOR_HASHJOIN_TABLE_01 A INNER JOIN VECTOR_HASHJOIN_TABLE_02 B ON A.C_INT = B.C_INT WHERE A.C_INT <8  AND  A.C_BIGINT<9  ORDER BY 1, 2;
SELECT A.C_INT, A.C_BIGINT, A.C_VARCHAR_3, A.C_DATE FROM VECTOR_HASHJOIN_TABLE_01 A INNER JOIN VECTOR_HASHJOIN_TABLE_02 B ON A.C_INT = B.C_INT WHERE A.C_INT >48 AND  A.C_BIGINT<10 ORDER BY 1, 2;
SELECT B.C_INT , A.C_BIGINT FROM VECTOR_HASHJOIN_TABLE_04 B left JOIN VECTOR_HASHJOIN_TABLE_03 A ON A.C_INT = B.C_INT where coalesce(B.C_BIGINT,2) > A.C_BIGINT order by 1,2;
SELECT A.c1, B.c1 from VECTOR_HASHJOIN_TABLE_09 A INNER JOIN VECTOR_HASHJOIN_TABLE_10 B ON A.c1 = B.c1 order by 1, 2 limit 10;

----
--- case 2: HashJoin Left/Right Join
----
explain (verbose on, costs off) SELECT A.C_INT,A.C_BIGINT,A.C_VARCHAR_3,A.C_DATE FROM VECTOR_HASHJOIN_TABLE_01 A LEFT JOIN VECTOR_HASHJOIN_TABLE_02 B ON A.C_INT = B.C_INT WHERE A.C_INT >45 AND  A.C_BIGINT>28 ORDER BY 1, 2;

SELECT A.C_INT,A.C_BIGINT,A.C_VARCHAR_3,A.C_DATE FROM VECTOR_HASHJOIN_TABLE_01 A LEFT JOIN VECTOR_HASHJOIN_TABLE_02 B ON A.C_INT = B.C_INT WHERE A.C_INT >45 AND  A.C_BIGINT>28 ORDER BY 1, 2;
SELECT A.C_INT,A.C_BIGINT,A.C_VARCHAR_3,A.C_DATE FROM VECTOR_HASHJOIN_TABLE_01 A LEFT JOIN VECTOR_HASHJOIN_TABLE_02 B ON A.C_INT = B.C_INT WHERE A.C_INT <4 AND  A.C_BIGINT>25 ORDER BY 1, 2;

explain (verbose on, costs off) SELECT A.C_INT,A.C_BIGINT FROM VECTOR_HASHJOIN_TABLE_01 A LEFT JOIN VECTOR_HASHJOIN_TABLE_02 B ON A.C_INT = B.C_INT and A.C_BIGINT > 1000 WHERE A.C_INT <4 ORDER BY 1, 2;
SELECT A.C_INT,A.C_BIGINT FROM VECTOR_HASHJOIN_TABLE_01 A LEFT JOIN VECTOR_HASHJOIN_TABLE_02 B ON A.C_INT = B.C_INT and A.C_BIGINT > 1000 WHERE A.C_INT <4 ORDER BY 1, 2;

SELECT B.C_INT, A.C_BIGINT FROM  VECTOR_HASHJOIN_TABLE_04 B RIGHT JOIN VECTOR_HASHJOIN_TABLE_03 A ON A.C_INT = B.C_INT AND A.C_BIGINT<2 order by 1,2;
SELECT B.C_INT , A.C_BIGINT FROM VECTOR_HASHJOIN_TABLE_04 B left JOIN VECTOR_HASHJOIN_TABLE_03 A ON A.C_INT = B.C_INT where coalesce(A.C_BIGINT,2) <2 order by 1,2;
SELECT B.C_INT, A.C_BIGINT FROM VECTOR_HASHJOIN_TABLE_04 B left JOIN VECTOR_HASHJOIN_TABLE_03 A ON A.C_INT = B.C_INT and coalesce(B.C_BIGINT,2) > A.C_BIGINT where coalesce(A.C_BIGINT,2) < 2 order by 1,2;

----
--- case 3: HashJoin Anti Join
----
explain (verbose on, costs off)  SELECT B.C_INT , A.C_BIGINT FROM  VECTOR_HASHJOIN_TABLE_04 B  RIGHT JOIN VECTOR_HASHJOIN_TABLE_03 A ON A.C_INT = B.C_INT AND A.C_BIGINT<2 where B.C_INT is NULL order by 1,2;
SELECT B.C_INT , A.C_BIGINT FROM  VECTOR_HASHJOIN_TABLE_04 B  RIGHT JOIN VECTOR_HASHJOIN_TABLE_03 A ON A.C_INT = B.C_INT AND A.C_BIGINT<2 where B.C_INT is NULL order by 1,2;

explain (verbose on, costs off) SELECT B.C_INT, A.C_BIGINT FROM VECTOR_HASHJOIN_TABLE_04 B RIGHT JOIN VECTOR_HASHJOIN_TABLE_03 A ON A.C_INT = B.C_INT where B.C_INT is NULL order by 1,2;
SELECT B.C_INT, A.C_BIGINT FROM VECTOR_HASHJOIN_TABLE_04 B RIGHT JOIN VECTOR_HASHJOIN_TABLE_03 A ON A.C_INT = B.C_INT  where B.C_INT is NULL order by 1,2;

----
--- case 4: HashJoin Complicate Hash Value
----
select A.*, B.c3 from VECTOR_HASHJOIN_TABLE_05 A join VECTOR_HASHJOIN_TABLE_05 B on substring(A.c3, 2) = substring(B.c3, 2) order by A.c1 limit 1000;
select A.*, B.c3 from VECTOR_HASHJOIN_TABLE_05 A left join VECTOR_HASHJOIN_TABLE_05 B on substring(A.c3, 2) = substring(B.c3, 2) order by A.c1 limit 1000;
select A.*, B.c3 from VECTOR_HASHJOIN_TABLE_05 A right join VECTOR_HASHJOIN_TABLE_05 B on substring(A.c3, 2) = substring(B.c3, 2) order by A.c1 limit 1000;
select * from VECTOR_HASHJOIN_TABLE_05 A right  join VECTOR_HASHJOIN_TABLE_05 B on  A.c1 = B.c1 and substring(A.c3, 2) = substring(B.c3, 2) order by 1,2,3,4 limit 1000;
select * from VECTOR_HASHJOIN_TABLE_05 A right  join VECTOR_HASHJOIN_TABLE_05 B on  A.c1 = B.c1 and substring(A.c3, 2) < substring(B.c3, 2) order by 1,2,3,4;


----
--- case 5: HashJoin with Flush to Disk 
----
set work_mem=64;
select A.*, B.c3 from VECTOR_HASHJOIN_TABLE_05 A join VECTOR_HASHJOIN_TABLE_05 B on substring(A.c3, 2) = substring(B.c3, 2) order by A.c1;
select A.*, B.c3 from VECTOR_HASHJOIN_TABLE_05 A left join VECTOR_HASHJOIN_TABLE_05 B on substring(A.c3, 2) = substring(B.c3, 2) order by A.c1;
select A.*, B.c3 from VECTOR_HASHJOIN_TABLE_05 A right join VECTOR_HASHJOIN_TABLE_05 B on substring(A.c3, 2) = substring(B.c3, 2) order by A.c1;
select * from VECTOR_HASHJOIN_TABLE_05 A right  join VECTOR_HASHJOIN_TABLE_05 B on  A.c1 = B.c1 and substring(A.c3, 2) = substring(B.c3, 2) order by 1,2,3,4;
select * from VECTOR_HASHJOIN_TABLE_05 A right  join VECTOR_HASHJOIN_TABLE_05 B on  A.c1 = B.c1 and substring(A.c3, 2) < substring(B.c3, 2) order by 1,2,3,4;
reset work_mem;


-----
-----
create table vector_hashjoin_engine.ROW_HASHJOIN_TABLE_07
(
   c_int int
  ,c_smallint smallint
  ,c3 char(100)
)distribute by hash(c_int);

create table vector_hashjoin_engine.ROW_HASHJOIN_TABLE_08
(
   c_int int
  ,c_bigint bigint
  ,c3 char(100)
)distribute by hash(c_int);

insert into ROW_HASHJOIN_TABLE_07 select generate_series(-10000,-1), generate_series(-10000,-1), 'vector_hashjoin_row'|| generate_series(1,10000);
insert into ROW_HASHJOIN_TABLE_07 values(1,2,NULL);
insert into ROW_HASHJOIN_TABLE_07 values(NULL,-2,NULL);
insert into ROW_HASHJOIN_TABLE_07 values(-3,NULL,NULL);
insert into ROW_HASHJOIN_TABLE_08 select generate_series(-10000,-5000), generate_series(-10000,-5000), 'vector_hashjoin_row'|| generate_series(1,5001);
insert into ROW_HASHJOIN_TABLE_08 values(1,2,NULL);
insert into ROW_HASHJOIN_TABLE_08 values(NULL,-2,NULL);
insert into ROW_HASHJOIN_TABLE_08 values(-3,NULL,NULL);

create table vector_hashjoin_engine.vector_hashjoin_table_07
(
   c_int int
  ,c_smallint smallint
  ,c3 char(100)
)with (orientation = orc) tablespace hdfs_ts  distribute by hash(c_int);
    
create table vector_hashjoin_engine.vector_hashjoin_table_08
(
   c_int int
  ,c_bigint bigint
  ,c3 char(100)
)with (orientation = orc) tablespace hdfs_ts  distribute by hash(c_int);

insert into vector_hashjoin_engine.vector_hashjoin_table_07 select * from vector_hashjoin_engine.ROW_HASHJOIN_TABLE_07;
insert into vector_hashjoin_engine.vector_hashjoin_table_08 select * from vector_hashjoin_engine.ROW_HASHJOIN_TABLE_08;
insert into vector_hashjoin_engine.vector_hashjoin_table_07 select * from vector_hashjoin_engine.ROW_HASHJOIN_TABLE_07;
insert into vector_hashjoin_engine.vector_hashjoin_table_08 select * from vector_hashjoin_engine.ROW_HASHJOIN_TABLE_08;
insert into vector_hashjoin_engine.vector_hashjoin_table_08 select * from vector_hashjoin_engine.ROW_HASHJOIN_TABLE_08;

analyze vector_hashjoin_engine.vector_hashjoin_table_07;
analyze vector_hashjoin_engine.vector_hashjoin_table_08;

select * from VECTOR_HASHJOIN_TABLE_07 A join  VECTOR_HASHJOIN_TABLE_08 B on a.c_int=b.c_bigint order by 1,2,3,4,5,6 limit 100;
select * from VECTOR_HASHJOIN_TABLE_07 A join VECTOR_HASHJOIN_TABLE_08 B on a.c_int=b.c_int and a.c_smallint=b.c_bigint order by 1,2,3,4,5,6 limit 100;
select * from VECTOR_HASHJOIN_TABLE_07 A join VECTOR_HASHJOIN_TABLE_08 B on a.c_int=b.c_int +1 order by 1,2,3,4,5,6 limit 50;
select * from VECTOR_HASHJOIN_TABLE_07 A right join VECTOR_HASHJOIN_TABLE_08 B on a.c_smallint=b.c_int order by 1,2,3,4,5,6 limit 100;
select * from VECTOR_HASHJOIN_TABLE_07 A right join VECTOR_HASHJOIN_TABLE_08 B on substring(A.c3, 2) = substring(B.c3, 2) order by 1,2,3,4,5,6 limit 50;
select * from VECTOR_HASHJOIN_TABLE_07 A right join VECTOR_HASHJOIN_TABLE_08 B on substring(A.c3, 2) = substring(B.c3, 2) and a.c_smallint=b.c_bigint order by 1,2,3,4,5,6 limit 50;

explain (verbose on, costs off) select * from VECTOR_HASHJOIN_TABLE_07 A right join VECTOR_HASHJOIN_TABLE_08 B on substring(A.c3, 2) = substring(B.c3, 2) and a.c_smallint=b.c_bigint order by 1,2,3,4,5,6 limit 50;
set work_mem=64;
select * from VECTOR_HASHJOIN_TABLE_07 A join       VECTOR_HASHJOIN_TABLE_08 B on a.c_int=b.c_bigint order by 1,2,3,4,5,6 limit 100;
select * from VECTOR_HASHJOIN_TABLE_07 A join VECTOR_HASHJOIN_TABLE_08 B on a.c_int=b.c_int and a.c_smallint=b.c_bigint order by 1,2,3,4,5,6 limit 100;
select * from VECTOR_HASHJOIN_TABLE_07 A join VECTOR_HASHJOIN_TABLE_08 B on a.c_int=b.c_int +1 order by 1,2,3,4,5,6 limit 50;
select * from VECTOR_HASHJOIN_TABLE_07 A right join VECTOR_HASHJOIN_TABLE_08 B on a.c_smallint=b.c_int order by 1,2,3,4,5,6 limit 100;
select * from VECTOR_HASHJOIN_TABLE_07 A right join VECTOR_HASHJOIN_TABLE_08 B on substring(A.c3, 2) = substring(B.c3, 2) order by 1,2,3,4,5,6 limit 50;
select * from VECTOR_HASHJOIN_TABLE_07 A right join VECTOR_HASHJOIN_TABLE_08 B on substring(A.c3, 2) = substring(B.c3, 2) and a.c_smallint=b.c_bigint order by 1,2,3,4,5,6 limit 50;
reset work_mem;
----
--- ICBC Case: HashJoin with case--when expression 
----
CREATE TABLE vector_hashjoin_engine.MF1_PTHNFSUB(
	STATUS DECIMAL(1,0),
	VALUEDAY DATE)with(orientation = orc) tablespace hdfs_ts ;
CREATE TABLE vector_hashjoin_engine.MF1_NTHPARAT(  
	CURRTYPE DECIMAL(3,0),
	RATECODE DECIMAL(1,0),
	Etl_Tx_Dt DATE) with(orientation = orc) tablespace hdfs_ts ;
insert into vector_hashjoin_engine.MF1_PTHNFSUB values(4, '2014-12-19');
insert into vector_hashjoin_engine.MF1_PTHNFSUB values(1, '2014-10-19');
insert into vector_hashjoin_engine.MF1_PTHNFSUB values(2, '2014-08-19');
insert into vector_hashjoin_engine.MF1_PTHNFSUB values(3, '2014-02-09');
insert into vector_hashjoin_engine.MF1_NTHPARAT values (5, 4, '2014-12-19');

explain (verbose on, costs off) select 
	count(*) 
FROM 	
	vector_hashjoin_engine.MF1_PTHNFSUB T1  
LEFT JOIN 	
	vector_hashjoin_engine.MF1_NTHPARAT T2  
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
	vector_hashjoin_engine.MF1_PTHNFSUB T1  
LEFT JOIN 	
	vector_hashjoin_engine.MF1_NTHPARAT T2  
ON 	
	T2.CURRTYPE=1 
AND 	
	CASE WHEN CAST('20141231' AS DATE)-T1.VALUEDAY<=90 THEN substring('abcd', 1)                        	    
	     WHEN CAST('20141231' AS DATE)-T1.VALUEDAY>90 AND CAST('20141231' AS DATE)-T1.VALUEDAY<=180 THEN '0200300'                          	
	     WHEN CAST('20141231' AS DATE)-T1.VALUEDAY>180 AND CAST('20141231' AS DATE)-T1.VALUEDAY<=360 THEN trim('jjhh',4)                          	
	     ELSE '0201200'	
	END=lpad(T2.RATECODE, 7, '0');
	
--test for semi join
set current_schema=vector_engine; 

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
   
set  current_schema =  vector_hashjoin_engine; 

CREATE TABLE t1_hashConst_col(col_1 int, col_2 int) with(orientation = orc) tablespace hdfs_ts ;
CREATE TABLE t2_hashConst_col(col_1 int, col_2 int) with(orientation = orc) tablespace hdfs_ts ;
insert into t1_hashConst_col values(1, 2);
insert into t2_hashConst_col values(1, 3);

explain (verbose on, costs off)select count(*) from t1_hashConst_col left join t2_hashConst_col on(t1_hashConst_col.col_1 = t2_hashConst_col.col_1 and t1_hashConst_col.col_2 > t2_hashConst_col.col_2);

select count(*) from t1_hashConst_col left join t2_hashConst_col on(t1_hashConst_col.col_1 = t2_hashConst_col.col_1 and t1_hashConst_col.col_2 > t2_hashConst_col.col_2);
drop table t1_hashConst_col;
drop table t2_hashConst_col;

----
--- Clean table and resource
----
drop schema vector_hashjoin_engine cascade;
