set enable_global_stats = true;
/*
 * This file is used to test the function of ExecVecMergeJoin()
 */
----
--- Create Table and Insert Data
----
create schema vector_mergejoin_engine;
set search_path to vector_mergejoin_engine;

create table vector_mergejoin_engine.ROW_MERGEJOIN_TABLE_01
(
   col_int      int 
  ,col_char     char(25)
  ,col_vchar    varchar(35)
  ,col_date     date
  ,col_num      numeric(10,4)
  ,col_float1   float4
  ,col_float2   float8
  ,col_timetz	timetz
  ,col_interval	interval
  ,col_tinterval	tinterval
)distribute by hash(col_int);

create table vector_mergejoin_engine.ROW_MERGEJOIN_TABLE_02
(
   col_int      int 
  ,col_char     char(25)
  ,col_vchar    varchar(35)
  ,col_date     date
  ,col_num      numeric(10,4)
  ,col_float1   float4
  ,col_float2   float8
  ,col_timetz	timetz
  ,col_interval	interval
  ,col_tinterval	tinterval
)distribute by hash(col_int);

create table vector_mergejoin_engine.VECTOR_MERGEJOIN_TABLE_01
(
   col_int      int 
  ,col_char     char(25)
  ,col_vchar    varchar(35)
  ,col_date     date
  ,col_num      numeric(10,4)
  ,col_float1   float4
  ,col_float2   float8
  ,col_timetz	timetz
  ,col_interval	interval
  ,col_tinterval	tinterval
)with(orientation = orc) tablespace hdfs_ts  distribute by hash(col_int);

create table vector_mergejoin_engine.VECTOR_MERGEJOIN_TABLE_02
(
   col_int      int 
  ,col_char     char(25)
  ,col_vchar    varchar(35)
  ,col_date     date
  ,col_num      numeric(10,4)
  ,col_float1   float4
  ,col_float2   float8
  ,col_timetz	timetz
  ,col_interval	interval
  ,col_tinterval	tinterval
)with(orientation = orc) tablespace hdfs_ts  distribute by hash(col_int);

CREATE OR REPLACE PROCEDURE func_insert_tbl_mergejoin_01()
AS
BEGIN
        FOR I IN 0..2000 LOOP
                if i = 19 OR i = 59 OR i = 159 OR i = 1899 then
                        INSERT INTO vector_mergejoin_engine.row_mergejoin_table_01 VALUES(i,NULL,NULL, '2015-01-01',i+0.1,i+0.2,i+0.3,'11:40:22+06','1 day 13:24:56','["Sep 5, 1983 23:59:12" "Oct6, 1983 23:59:12"]');
				elsif i = 150 OR i = 300 OR i = 1500 then
                        INSERT INTO vector_mergejoin_engine.row_mergejoin_table_01 VALUES(i,'mergejoin_char'||i,'mergejoin_varchar'||i,'2015-01-01',i+0.1,i+0.2,i+0.3,'11:20:22+06','2 day 13:24:56','["Sep 4, 1983 23:59:12" "Oct4, 1983 23:59:12"]');
                        INSERT INTO vector_mergejoin_engine.row_mergejoin_table_01 VALUES(i,'mergejoin_char'||i,'mergejoin_varchar'||i,'2015-01-01',i+0.1,i+0.2,i+0.3,'11:24:22+06','2 day 13:24:56','["Sep 4, 1983 23:59:12" "Oct4, 1983 23:59:12"]');
				elsif i = 450 OR i = 800 OR i = 1200 then
                        INSERT INTO vector_mergejoin_engine.row_mergejoin_table_01 VALUES(i,'mergejoin_char'||i,'mergejoin_varchar'||i,'2015-01-01',i+0.1,i+0.2,i+0.3,'11:20:22+06','2 day 13:24:56','["Sep 4, 1983 23:59:12" "Oct4, 1983 23:59:12"]');
                        INSERT INTO vector_mergejoin_engine.row_mergejoin_table_01 VALUES(i,'mergejoin_char'||i,'mergejoin_varchar'||i,'2015-01-01',i+0.1,i+0.2,i+0.3,'11:20:22+06','6 day 13:24:56','["Sep 4, 1983 23:59:12" "Oct4, 1983 23:59:12"]');
                        INSERT INTO vector_mergejoin_engine.row_mergejoin_table_01 VALUES(i,'mergejoin_char'||i,'mergejoin_varchar'||i,'2015-01-01',i+0.1,i+0.2,i+0.3,'11:20:22+06','2 day 13:24:56','["Sep 4, 1983 23:59:12" "Oct4, 1983 23:59:12"]');
                else
                        INSERT INTO vector_mergejoin_engine.row_mergejoin_table_01 VALUES(i,'mergejoin_char'||i,'mergejoin_varchar'||i,'2015-01-01',i+0.1,i+0.2,i+0.3,'11:30:22+06','2 day 13:24:56','["Sep 7, 1983 23:59:12" "Oct4, 1983 23:59:12"]');
                end if;
        END LOOP;
END;
/
CALL func_insert_tbl_mergejoin_01();

CREATE OR REPLACE PROCEDURE func_insert_tbl_mergejoin_02()
AS
BEGIN
        FOR I IN 0..2000 LOOP
                if i = 2 OR i = 32 OR i = 252 OR i = 1722 then
                        INSERT INTO vector_mergejoin_engine.row_mergejoin_table_02 VALUES(i,NULL,NULL, '2015-01-01',i+0.1,i+0.2,i+0.3,'11:40:22+06','2 day 13:24:56','["Sep 4, 1983 23:59:12" "Oct4, 1983 23:59:12"]');
				elsif i = 250 OR i = 480 OR i = 1480 then
                        INSERT INTO vector_mergejoin_engine.row_mergejoin_table_02 VALUES(i,'mergejoin_char'||i,'mergejoin_varchar'||i,'2015-01-01',i+0.1,i+0.2,i+0.3,'11:40:22+06','5 day 13:24:56','["Sep 4, 1983 23:59:12" "Oct4, 1983 23:59:12"]');
                        INSERT INTO vector_mergejoin_engine.row_mergejoin_table_02 VALUES(i,'mergejoin_char'||i,'mergejoin_varchar'||i,'2015-01-01',i+0.1,i+0.2,i+0.3,'11:20:22+06','3 day 13:24:56','["Sep 4, 1983 23:59:12" "Oct4, 1983 23:59:12"]');
				elsif i = 465 OR i = 735 OR i = 1805 then
                        INSERT INTO vector_mergejoin_engine.row_mergejoin_table_02 VALUES(i,'mergejoin_char'||i,'mergejoin_varchar'||i,'2015-01-01',i+0.1,i+0.2,i+0.3,'11:20:22+06','4 day 13:24:56','["Sep 4, 1983 23:59:12" "Oct4, 1983 23:59:12"]');
                        INSERT INTO vector_mergejoin_engine.row_mergejoin_table_02 VALUES(i,'mergejoin_char'||i,'mergejoin_varchar'||i,'2015-01-01',i+0.1,i+0.2,i+0.3,'11:20:22+06','2 day 13:24:56','["Sep 4, 1983 23:59:12" "Oct4, 1983 23:59:12"]');
                        INSERT INTO vector_mergejoin_engine.row_mergejoin_table_02 VALUES(i,'mergejoin_char'||i,'mergejoin_varchar'||i,'2015-01-01',i+0.1,i+0.2,i+0.3,'11:24:22+06','8 day 13:24:56','["Sep 4, 1983 23:59:12" "Oct4, 1983 23:59:12"]');
                else
                        INSERT INTO vector_mergejoin_engine.row_mergejoin_table_02 VALUES(i,'mergejoin_char'||i,'mergejoin_varchar'||i,'2015-01-01',i+0.1,i+0.2,i+0.3,'11:20:22+06','7 day 13:24:56','["Sep 4, 1983 23:59:12" "Oct1, 1983 23:59:12"]');
                end if;
        END LOOP;
END;
/
CALL func_insert_tbl_mergejoin_02();

insert into vector_mergejoin_table_01 select * from row_mergejoin_table_01;
insert into vector_mergejoin_table_02 select * from row_mergejoin_table_02;

create table vector_mergejoin_engine.VECTOR_MERGEJOIN_TABLE_03
(
   ID int NOT NULL
  ,NAME varchar(10) NOT NULL
  ,ZIP char(9) NOT NULL
)with (orientation = orc) tablespace hdfs_ts distribute by hash(ID);

create table vector_mergejoin_engine.VECTOR_MERGEJOIN_TABLE_04
(
   ID int NOT NULL
  ,STREET varchar(20) NOT NULL
  ,ZIP char(9) NOT NULL
  ,C_D_ID int NOT NULL
  ,C_ID int NOT NULL
)with (orientation = orc) tablespace hdfs_ts distribute by hash(ID);

create index zip_idx on vector_mergejoin_engine.VECTOR_MERGEJOIN_TABLE_03(ZIP);
create index id_c_d_id on vector_mergejoin_engine.VECTOR_MERGEJOIN_TABLE_04(ID,C_D_ID,C_ID);

COPY VECTOR_MERGEJOIN_TABLE_03 (ID, NAME, ZIP) FROM stdin; 
2	jaqspofube	496611111
4	dmkczswa	522411111
7	jcanwmh	950211111
10	wfnlmpcw	760511111
3	qcscbhkkql	545511111
5	vsfcguexuf	329711111
6	escpbk	784411111
8	wzdnxwhm	979511111
9	ydcuynmyud	684011111
\.

COPY VECTOR_MERGEJOIN_TABLE_04 (ID, STREET, ZIP, C_D_ID, C_ID) FROM stdin;
1	vsoynwlksfrgx	218111111	8	1041
2	ivpyeyvvvmxd	496611111	6	651
2	vwpptvzucxnwwdp	563811111	6	1425
6	wnnqkwrvow	614611111	1	930
6	adftvwodcymypwkm	731811111	1	1119
6	czsafpfyatalnt	056511111	4	1070
9	nobmaggbplxvojhootj	732911111	5	218
9	rytskymwwimdisl	174311111	5	1260
9	hteuhfwglhkvkgezrwgn	684011111	10	1381
\.

create table vector_mergejoin_engine.VECTOR_MERGEJOIN_TABLE_05
(
   ID int 
  ,NAME varchar(10) 
  ,ZIP char(9) 
)with (orientation = orc) tablespace hdfs_ts distribute by hash(ID);

create table vector_mergejoin_engine.VECTOR_MERGEJOIN_TABLE_06
(
   ID int
  ,STREET varchar(20) 
  ,ZIP char(9)
  ,C_D_ID int 
  ,C_ID int
)with (orientation = orc) tablespace hdfs_ts distribute by hash(ID);

create table vector_mergejoin_engine.VECTOR_MERGEJOIN_TABLE_07
(
   ID int
  ,name varchar(10)
  ,street varchar(20)
  ,d_id int
)with (orientation = orc) tablespace hdfs_ts distribute by hash(ID) ;

create index zip_idx_05 on vector_mergejoin_engine.VECTOR_MERGEJOIN_TABLE_05(ZIP);
create index id_cd_idx on vector_mergejoin_engine.VECTOR_MERGEJOIN_TABLE_06(ID,C_D_ID,C_ID);
create index id_d_idx on vector_mergejoin_engine.VECTOR_MERGEJOIN_TABLE_07(ID,D_ID);
create index name_idx on vector_mergejoin_engine.VECTOR_MERGEJOIN_TABLE_07(NAME);

COPY VECTOR_MERGEJOIN_TABLE_05(ID, NAME, ZIP) FROM stdin; 
\N	\N	\N
2	jaqspofube	496611111
4	dmkczswa	522411111
7	jcanwmh	950211111
10	wfnlmpcw	760511111
3	qcscbhkkql	545511111
5	vsfcguexuf	329711111
6	escpbk	784411111
8	wzdnxwhm	979511111
9	ydcuynmyud	684011111
\.

COPY VECTOR_MERGEJOIN_TABLE_06(ID, STREET, ZIP, C_D_ID, C_ID) FROM stdin; 
\N	\N	\N	\N	\N
1	zzaakzzqvvoludqvj	754611111	1	201
1	vsoynwlksfrgx	218111111	8	1041
2	ivpyeyvvvmxd	074211111	6	651
2	vwpptvzucxnwwdp	563811111	6	1425
6	wnnqkwrvow	614611111	1	930
6	adftvwodcymypwkm	731811111	1	1119
6	czsafpfyatalnt	056511111	4	1070
9	nobmaggbplxvojhootj	732911111	5	218
9	rytskymwwimdisl	174311111	5	1260
9	hteuhfwglhkvkgezrwgn	927111111	10	1381
\.

COPY VECTOR_MERGEJOIN_TABLE_07(ID, NAME, STREET, D_ID) FROM stdin;
\N	\N	\N	\N
3	whmwhy	reebdyaozaxksnup	1
3	thljrhb	uenvarvnig	7
4	dhcbedta	zqovoksfdzctaz	5
4	xzwxdnxkq	tyxheriveufuixgn	8
1	swotbb	ogsxoekiohenrovqcr	1
1	hlysyik	uslotvsjfagtix	8
9	iavkghx	okbixjuzrmoafuksgwk	2
9	ygptsjv	nshiuzehbxoticobyid	4
8	mklxitc	rzcvomvfkwedbzultbul	3
7	lqblvxm	jeyfmmprhwzn	9
7	hvfgdobl	vrpixtpapgpstsfs	4
7	lqblvxm	jeyfmmprhwzn	9
2	vzglpg	qnsvqaarnaayxotrqcm	5
2	vzglpg	qnsvqaarnaayxotrqdf	15
5	azqjfcsiw	wwndhzxkhovdtgqf	2
5	ueigupb	builqzzqfdgyui	6
6	tqsqzbjri	afowiivutvfbpyzc	10
\.

analyze vector_mergejoin_table_01;
analyze vector_mergejoin_table_02;
analyze vector_mergejoin_table_03;
analyze vector_mergejoin_table_04;
analyze vector_mergejoin_table_05;
analyze vector_mergejoin_table_06;
analyze vector_mergejoin_table_07;

set enable_hashjoin=off;
set enable_nestloop=off;
---
-- case 1: MergeJoin Inner Join
---
explain (verbose on, costs off) select A.col_int, B.col_int from vector_mergejoin_table_01 A, vector_mergejoin_table_02 B where A.col_int = B.col_int order by 1, 2 limit 100;
select A.col_int, B.col_int from vector_mergejoin_table_01 A, vector_mergejoin_table_02 B where A.col_int = B.col_int order by 1, 2 limit 100;
select A.col_int, B.col_int from vector_mergejoin_table_01 A join vector_mergejoin_table_02 B on(A.col_int = B.col_int) and A.col_int is null order by 1, 2;
select A.col_int, B.col_int from vector_mergejoin_table_01 A join vector_mergejoin_table_02 B on(A.col_int = B.col_int) and B.col_int is not null order by 1, 2 limit 100;
select A.col_int, B.col_int, A.col_char,B.col_char from vector_mergejoin_table_01 A, vector_mergejoin_table_02 B where A.col_char = B.col_char and A.col_vchar=B.col_vchar order by 1, 2 limit 100;
select A.col_int, sum(A.col_int) from vector_mergejoin_table_01 A, vector_mergejoin_table_02 B where A.col_int = B.col_int group by A.col_int order by 1 limit 100;
select count(*) from vector_mergejoin_table_01 A, vector_mergejoin_table_02 B where A.col_num = B.col_num;
select A.col_timetz, B.col_timetz from vector_mergejoin_table_01 A, vector_mergejoin_table_02 B where A.col_timetz = B.col_timetz order by 1, 2 limit 10;
select A.col_interval, B.col_interval from vector_mergejoin_table_01 A, vector_mergejoin_table_02 B where A.col_interval = B.col_interval order by 1, 2 limit 10;
select A.col_tinterval, B.col_tinterval from vector_mergejoin_table_01 A, vector_mergejoin_table_02 B where A.col_tinterval = B.col_tinterval order by 1, 2 limit 10;

---
-- case 2: MergeJoin Left Join
---
explain (verbose on, costs off) select * from vector_mergejoin_table_01 A left join vector_mergejoin_table_02 B on A.col_int = B.col_int order by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20 limit 1000;

select * from vector_mergejoin_table_01 A left join vector_mergejoin_table_02 B on A.col_int = B.col_int order by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20 limit 1000;
select count(*) from vector_mergejoin_table_01 A left join vector_mergejoin_table_02 B on A.col_int = B.col_int;
select * from vector_mergejoin_table_02 A left join vector_mergejoin_table_01 B on A.col_int = B.col_int order by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20 limit 1000;
select count(*) from vector_mergejoin_table_02 A left join vector_mergejoin_table_01 B using(col_int);

----
--- case 3: MergeJoin Semi Join
----
explain (verbose on, costs off) select * from vector_mergejoin_table_01 A where A.col_int in(select B.col_int from vector_mergejoin_table_02 B left join vector_mergejoin_table_01 using(col_int)) order by 1,2,3,4,5,6,7,8,9,10 limit 50;

select * from vector_mergejoin_table_01 A where A.col_int in(select B.col_int from vector_mergejoin_table_02 B left join vector_mergejoin_table_01 using(col_int)) order by 1,2,3,4,5,6,7,8,9,10 limit 50;
select * from vector_mergejoin_table_01 A where A.col_int in(select B.col_int from vector_mergejoin_table_02 B left join vector_mergejoin_table_01 C on B.col_int = C.col_int) order by 1,2,3,4,5,6,7,8,9,10 limit 50;
select count(*) from vector_mergejoin_table_01 A where A.col_int in(select B.col_int from vector_mergejoin_table_02 B left join vector_mergejoin_table_01 C on B.col_int = C.col_int);
select * from vector_mergejoin_table_02 A where A.col_int in(select B.col_int from vector_mergejoin_table_01 B left join vector_mergejoin_table_02 C on B.col_int = C.col_int) order by 1,2,3,4,5,6,7,8,9,10 limit 50;

----
--- case 4: MergeJoin Full Join
----
--explain (verbose on, costs off) select * from vector_mergejoin_table_01 A full join vector_mergejoin_table_02 B on A.col_int = B.col_int order by 1,2,3,4,5,6,7,8,9, 10 limit 100;

--select * from vector_mergejoin_table_01 A full join vector_mergejoin_table_02 B on A.col_int = B.col_int order by 1,2,3,4,5,6,7,8,9, 10 limit 100;
--select * from vector_mergejoin_table_01 A full join vector_mergejoin_table_02 B using(col_int) order by 1,2,3,4,5,6,7,8,9, 10 limit 100;
--select * from vector_mergejoin_table_02 A full join vector_mergejoin_table_01 B on A.col_int = B.col_int order by 1,2,3,4,5,6,7,8,9, 10 limit 100;
--select count(*) from vector_mergejoin_table_01 A full join vector_mergejoin_table_02 B on A.col_int = B.col_int;

----
--- case 5: MergeJoin Anti Join
----
SET ENABLE_SEQSCAN=ON;

explain (verbose on, costs off) select A.ID, A.zip from vector_mergejoin_engine.vector_mergejoin_table_03 A, vector_mergejoin_engine.vector_mergejoin_table_04 B where (A.ID, A.zip) NOT IN (SELECT ID, zip FROM vector_mergejoin_engine.vector_mergejoin_table_04) order by 1, 2;

select A.ID, A.zip from vector_mergejoin_engine.vector_mergejoin_table_03 A, vector_mergejoin_engine.vector_mergejoin_table_04 B where (A.ID, A.zip) NOT IN (SELECT ID, zip FROM vector_mergejoin_engine.vector_mergejoin_table_04) order by 1, 2;
select A.ID, A.zip from vector_mergejoin_engine.vector_mergejoin_table_04 A, vector_mergejoin_engine.vector_mergejoin_table_03 B where (A.ID, A.zip) NOT IN (SELECT ID, zip FROM vector_mergejoin_engine.vector_mergejoin_table_03) order by 1, 2;

----
--- case 6: aggregation with null
----
select vector_mergejoin_engine.VECTOR_MERGEJOIN_TABLE_05.ID,max(vector_mergejoin_engine.VECTOR_MERGEJOIN_TABLE_05.ID),vector_mergejoin_engine.VECTOR_MERGEJOIN_TABLE_05.ID>2,1+2 as RESULT from vector_mergejoin_engine.VECTOR_MERGEJOIN_TABLE_05 INNER join vector_mergejoin_engine.VECTOR_MERGEJOIN_TABLE_06 USING(ID) INNER join vector_mergejoin_engine.VECTOR_MERGEJOIN_TABLE_07 ON vector_mergejoin_engine.VECTOR_MERGEJOIN_TABLE_06.ID=vector_mergejoin_engine.VECTOR_MERGEJOIN_TABLE_07.ID group by vector_mergejoin_engine.VECTOR_MERGEJOIN_TABLE_05.ID order by 1 DESC NULLS LAST fetch FIRST ROW ONLY;

----
--- case 7: Complicate Case
----
CREATE TABLE vector_mergejoin_engine.TBL_COMBINE_NESTLOOP_SEQSCAN_00(
	C_CHAR_1 CHAR(1),
	C_CHAR_2 CHAR(10),
	C_CHAR_3 CHAR(100),
	C_VARCHAR_1 VARCHAR(1),
	C_VARCHAR_2 VARCHAR(10),
	C_VARCHAR_3 VARCHAR(1024),
	C_INT INTEGER,
	C_BIGINT BIGINT,
	C_INT_01 INTEGER,
	C_BIGINT_01 BIGINT,
	C_SMALLINT SMALLINT,
	C_FLOAT FLOAT,
	C_NUMERIC numeric(10,5),
	C_DP double precision,
	C_DATE DATE,
	C_TS_WITHOUT TIMESTAMP WITHOUT TIME ZONE,
	C_TS_WITH TIMESTAMP WITH TIME ZONE, PARTIAL CLUSTER KEY(C_CHAR_3))WITH (orientation = orc) tablespace hdfs_ts;
CREATE INDEX idx_TBL_COMBINE_NESTLOOP_SEQSCAN_00_01 on TBL_COMBINE_NESTLOOP_SEQSCAN_00(C_INT_01);
CREATE INDEX idx_TBL_COMBINE_NESTLOOP_SEQSCAN_00_02 on TBL_COMBINE_NESTLOOP_SEQSCAN_00(C_BIGINT_01);
CREATE TABLE vector_mergejoin_engine.TBL_COMBINE_NESTLOOP_SEQSCAN_00_xx1(
	C_CHAR_1 CHAR(1),
	C_CHAR_2 CHAR(10),
	C_CHAR_3 CHAR(100),
	C_VARCHAR_1 VARCHAR(1),
	C_VARCHAR_2 VARCHAR(10),
	C_VARCHAR_3 VARCHAR(1024),
	C_INT INTEGER,
	C_BIGINT BIGINT,
	C_INT_01 INTEGER,
	C_BIGINT_01 BIGINT,
	C_SMALLINT SMALLINT,
	C_FLOAT FLOAT,
	C_NUMERIC numeric(10,5),
	C_DP double precision,
	C_DATE DATE,
	C_TS_WITHOUT TIMESTAMP WITHOUT TIME ZONE,
	C_TS_WITH TIMESTAMP WITH TIME ZONE);	
CREATE OR REPLACE PROCEDURE PROC_TBL_COMBINE_NESTLOOP_SEQSCAN_00()
AS  
BEGIN  
       FOR I IN 1..50 LOOP  
	   FOR J IN 1..40 LOOP  
         INSERT INTO TBL_COMBINE_NESTLOOP_SEQSCAN_00_xx1 VALUES('A','SEQSCAN_01','SEQSCAN_01AAAA','a','aaadf','SEQSCAN_01AAAAAAAAAAAAAAAAAAAAAA'||i,i,j,i,j,i+j,i+0.0001,i+0.00001,i+0.000001,'2000-01-01','2000-01-01 01:01:01','2000-01-01 01:01:01+01');
       END LOOP; 
	    END LOOP; 
END;  
/
CALL PROC_TBL_COMBINE_NESTLOOP_SEQSCAN_00();
insert into TBL_COMBINE_NESTLOOP_SEQSCAN_00 select * from TBL_COMBINE_NESTLOOP_SEQSCAN_00_xx1;
CREATE TABLE vector_mergejoin_engine.TBL_COMBINE_NESTLOOP_SEQSCAN_01(
	C_CHAR_1 CHAR(1),
	C_CHAR_2 CHAR(10),
	C_CHAR_3 CHAR(100),
	C_VARCHAR_1 VARCHAR(1),
	C_VARCHAR_2 VARCHAR(10),
	C_VARCHAR_3 VARCHAR(1024),
	C_INT INTEGER,
	C_BIGINT BIGINT,
	C_INT_01 INTEGER,
	C_BIGINT_01 BIGINT,
	C_SMALLINT SMALLINT,
	C_FLOAT FLOAT,
	C_NUMERIC numeric(10,5),
	C_DP double precision,
	C_DATE DATE,
	C_TS_WITHOUT TIMESTAMP WITHOUT TIME ZONE,
	C_TS_WITH TIMESTAMP WITH TIME ZONE, PARTIAL CLUSTER KEY(C_INT))WITH (orientation = orc) tablespace hdfs_ts DISTRIBUTE BY HASH (C_INT) ;
CREATE TABLE vector_mergejoin_engine.TBL_COMBINE_NESTLOOP_SEQSCAN_01_xx2(
	C_CHAR_1 CHAR(1),
	C_CHAR_2 CHAR(10),
	C_CHAR_3 CHAR(100),
	C_VARCHAR_1 VARCHAR(1),
	C_VARCHAR_2 VARCHAR(10),
	C_VARCHAR_3 VARCHAR(1024),
	C_INT INTEGER,
	C_BIGINT BIGINT,
	C_INT_01 INTEGER,
	C_BIGINT_01 BIGINT,
	C_SMALLINT SMALLINT,
	C_FLOAT FLOAT,
	C_NUMERIC numeric(10,5),
	C_DP double precision,
	C_DATE DATE,
	C_TS_WITHOUT TIMESTAMP WITHOUT TIME ZONE,
	C_TS_WITH TIMESTAMP WITH TIME ZONE);
CREATE INDEX idx_TBL_COMBINE_NESTLOOP_SEQSCAN_01_01 on TBL_COMBINE_NESTLOOP_SEQSCAN_01(C_INT_01);
CREATE INDEX idx_TBL_COMBINE_NESTLOOP_SEQSCAN_01_02 on TBL_COMBINE_NESTLOOP_SEQSCAN_01(C_BIGINT_01);	
CREATE OR REPLACE PROCEDURE PROC_TBL_COMBINE_NESTLOOP_SEQSCAN_01()
AS  
BEGIN  
       FOR I IN 1..40 LOOP  
	   FOR J IN 1..50 LOOP  
         INSERT INTO TBL_COMBINE_NESTLOOP_SEQSCAN_01_xx2 VALUES('B','SEQSCAN_02','SEQSCAN_02BBBB','B','BBBBBB','SEQSCAN_02BBBBBBBBBBBBBBBBBBBBBBB_'||i,i,j,i,j,i+j,i+0.0001,i+0.00001,i+0.000001,'2000-01-01','2000-01-01 01:01:01','2000-01-01 01:01:01+01');
       END LOOP; 
	    END LOOP; 
END;  
/
CALL PROC_TBL_COMBINE_NESTLOOP_SEQSCAN_01();
insert into TBL_COMBINE_NESTLOOP_SEQSCAN_01 select * from TBL_COMBINE_NESTLOOP_SEQSCAN_01_xx2;
CREATE TABLE vector_mergejoin_engine.TBL_COMBINE_NESTLOOP_SEQSCAN_02(
	C_CHAR_1 CHAR(1),
	C_CHAR_2 CHAR(10),
	C_CHAR_3 CHAR(100),
	C_VARCHAR_1 VARCHAR(1),
	C_VARCHAR_2 VARCHAR(10),
	C_VARCHAR_3 VARCHAR(1024),
	C_INT INTEGER,
	C_BIGINT BIGINT,
	C_INT_01 INTEGER,
	C_BIGINT_01 BIGINT,
	C_SMALLINT SMALLINT,
	C_FLOAT FLOAT,
	C_NUMERIC numeric(10,5),
	C_DP double precision,
	C_DATE DATE,
	C_TS_WITHOUT TIMESTAMP WITHOUT TIME ZONE,
	C_TS_WITH TIMESTAMP WITH TIME ZONE, PARTIAL CLUSTER KEY(C_VARCHAR_3))WITH (orientation = orc) tablespace hdfs_ts;
CREATE TABLE vector_mergejoin_engine.TBL_COMBINE_NESTLOOP_SEQSCAN_02_xx3(
	C_CHAR_1 CHAR(1),
	C_CHAR_2 CHAR(10),
	C_CHAR_3 CHAR(100),
	C_VARCHAR_1 VARCHAR(1),
	C_VARCHAR_2 VARCHAR(10),
	C_VARCHAR_3 VARCHAR(1024),
	C_INT INTEGER,
	C_BIGINT BIGINT,
	C_INT_01 INTEGER,
	C_BIGINT_01 BIGINT,
	C_SMALLINT SMALLINT,
	C_FLOAT FLOAT,
	C_NUMERIC numeric(10,5),
	C_DP double precision,
	C_DATE DATE,
	C_TS_WITHOUT TIMESTAMP WITHOUT TIME ZONE,
	C_TS_WITH TIMESTAMP WITH TIME ZONE);
CREATE INDEX idx_TBL_COMBINE_NESTLOOP_SEQSCAN_02_01 on TBL_COMBINE_NESTLOOP_SEQSCAN_02(C_INT_01);
CREATE INDEX idx_TBL_COMBINE_NESTLOOP_SEQSCAN_02_02 on TBL_COMBINE_NESTLOOP_SEQSCAN_02(C_BIGINT_01);	
CREATE OR REPLACE PROCEDURE PROC_TBL_COMBINE_NESTLOOP_SEQSCAN_02()
AS  
BEGIN  
       FOR I IN 1..50 LOOP  
	   FOR J IN 1..50 LOOP  
         INSERT INTO TBL_COMBINE_NESTLOOP_SEQSCAN_02_xx3 VALUES('C','SEQSCAN_03','SEQSCAN_03CCCC','C','CCCCCC','SEQSCAN_03CCCCCCCCCCCCCCC_'||i,i,j,i,j,i+j,i+0.0001,i+0.00001,i+0.000001,'2000-01-01','2000-01-01 01:01:01','2000-01-01 01:01:01+01');
       END LOOP; 
	    END LOOP; 
END;  
/
CALL PROC_TBL_COMBINE_NESTLOOP_SEQSCAN_02();
insert into TBL_COMBINE_NESTLOOP_SEQSCAN_02 select * from TBL_COMBINE_NESTLOOP_SEQSCAN_02_xx3;

CREATE TABLE vector_mergejoin_engine.TBL_COMBINE_NESTLOOP_SEQSCAN_03(
	C_CHAR_1 CHAR(1),
	C_CHAR_2 CHAR(10),
	C_CHAR_3 CHAR(100),
	C_VARCHAR_1 VARCHAR(1),
	C_VARCHAR_2 VARCHAR(10),
	C_VARCHAR_3 VARCHAR(1024),
	C_INT INTEGER,
	C_BIGINT BIGINT,
	C_INT_01 INTEGER,
	C_BIGINT_01 BIGINT,
	C_SMALLINT SMALLINT,
	C_FLOAT FLOAT,
	C_NUMERIC numeric(10,5),
	C_DP double precision,
	C_DATE DATE,
	C_TS_WITHOUT TIMESTAMP WITHOUT TIME ZONE,
	C_TS_WITH TIMESTAMP WITH TIME ZONE )WITH (orientation = column)
	partition by range (C_INT)--INTERVAL (20)
	( 
		partition TBL_COMBINE_NESTLOOP_SEQSCAN_03_1 values less than (20),
		partition TBL_COMBINE_NESTLOOP_SEQSCAN_03_2 values less than (40),
		partition TBL_COMBINE_NESTLOOP_SEQSCAN_03_3 values less than (60),
		partition TBL_COMBINE_NESTLOOP_SEQSCAN_03_4 values less than (90),
		partition TBL_COMBINE_NESTLOOP_SEQSCAN_03_5 values less than (201),
		partition TBL_COMBINE_NESTLOOP_SEQSCAN_03_6 values less than (maxvalue)
    );
CREATE TABLE vector_mergejoin_engine.TBL_COMBINE_NESTLOOP_SEQSCAN_03_xx4(
	C_CHAR_1 CHAR(1),
	C_CHAR_2 CHAR(10),
	C_CHAR_3 CHAR(100),
	C_VARCHAR_1 VARCHAR(1),
	C_VARCHAR_2 VARCHAR(10),
	C_VARCHAR_3 VARCHAR(1024),
	C_INT INTEGER,
	C_BIGINT BIGINT,
	C_INT_01 INTEGER,
	C_BIGINT_01 BIGINT,
	C_SMALLINT SMALLINT,
	C_FLOAT FLOAT,
	C_NUMERIC numeric(10,5),
	C_DP double precision,
	C_DATE DATE,
	C_TS_WITHOUT TIMESTAMP WITHOUT TIME ZONE,
	C_TS_WITH TIMESTAMP WITH TIME ZONE )
	partition by range (C_INT)--INTERVAL (20)
	( 
		partition TBL_COMBINE_NESTLOOP_SEQSCAN_03_1 values less than (20),
		partition TBL_COMBINE_NESTLOOP_SEQSCAN_03_2 values less than (40),
		partition TBL_COMBINE_NESTLOOP_SEQSCAN_03_3 values less than (60),
		partition TBL_COMBINE_NESTLOOP_SEQSCAN_03_4 values less than (90),
		partition TBL_COMBINE_NESTLOOP_SEQSCAN_03_5 values less than (201),
		partition TBL_COMBINE_NESTLOOP_SEQSCAN_03_6 values less than (maxvalue)
    );
CREATE INDEX idx_TBL_COMBINE_NESTLOOP_SEQSCAN_03_01 ON TBL_COMBINE_NESTLOOP_SEQSCAN_03 (C_INT_01) local;
CREATE INDEX idx_TBL_COMBINE_NESTLOOP_SEQSCAN_03_02 ON TBL_COMBINE_NESTLOOP_SEQSCAN_03 (C_BIGINT_01) local;
CREATE OR REPLACE PROCEDURE PROC_TBL_COMBINE_NESTLOOP_SEQSCAN_03()
AS  
BEGIN  
       FOR I IN 1..200 LOOP  
         INSERT INTO TBL_COMBINE_NESTLOOP_SEQSCAN_03_xx4 VALUES('X','SEQSCAN_03','SEQSCAN_03_INTERVAL','b','SEQSCAN_03','SEQSCAN_03_INTERVAL_TABLE'||i,i,i+1,i,i+1,i+2,i+1.1,1.11,1.111,'2000-01-01','2000-01-01 01:01:01','2000-01-01 01:01:01+01');
       END LOOP; 
END;  
/
CALL PROC_TBL_COMBINE_NESTLOOP_SEQSCAN_03();
insert into TBL_COMBINE_NESTLOOP_SEQSCAN_03 select * from TBL_COMBINE_NESTLOOP_SEQSCAN_03_xx4;

CREATE TABLE vector_mergejoin_engine.TBL_COMBINE_NESTLOOP_SEQSCAN_04(
	C_CHAR_1 CHAR(1),
	C_CHAR_2 CHAR(10),
	C_CHAR_3 CHAR(100),
	C_VARCHAR_1 VARCHAR(1),
	C_VARCHAR_2 VARCHAR(10),
	C_VARCHAR_3 VARCHAR(1024),
	C_INT INTEGER,
	C_BIGINT BIGINT,
	C_INT_01 INTEGER,
	C_BIGINT_01 BIGINT,
	C_SMALLINT SMALLINT,
	C_FLOAT FLOAT,
	C_NUMERIC numeric(10,5),
	C_DP double precision,
	C_DATE DATE,
	C_TS_WITHOUT TIMESTAMP WITHOUT TIME ZONE,
	C_TS_WITH TIMESTAMP WITH TIME ZONE , PARTIAL CLUSTER KEY(C_INT))WITH (orientation = column) DISTRIBUTE BY HASH (C_INT) 
	partition by range (C_INT)
( 
     partition TBL_COMBINE_NESTLOOP_SEQSCAN_04_1 values less than (30),
     partition TBL_COMBINE_NESTLOOP_SEQSCAN_04_2 values less than (50),
     partition TBL_COMBINE_NESTLOOP_SEQSCAN_04_3 values less than (80),
	 partition TBL_COMBINE_NESTLOOP_SEQSCAN_04_4 values less than (150),
	 partition TBL_COMBINE_NESTLOOP_SEQSCAN_04_5 values less than (301)
);
CREATE TABLE vector_mergejoin_engine.TBL_COMBINE_NESTLOOP_SEQSCAN_04_xx5(
	C_CHAR_1 CHAR(1),
	C_CHAR_2 CHAR(10),
	C_CHAR_3 CHAR(100),
	C_VARCHAR_1 VARCHAR(1),
	C_VARCHAR_2 VARCHAR(10),
	C_VARCHAR_3 VARCHAR(1024),
	C_INT INTEGER,
	C_BIGINT BIGINT,
	C_INT_01 INTEGER,
	C_BIGINT_01 BIGINT,
	C_SMALLINT SMALLINT,
	C_FLOAT FLOAT,
	C_NUMERIC numeric(10,5),
	C_DP double precision,
	C_DATE DATE,
	C_TS_WITHOUT TIMESTAMP WITHOUT TIME ZONE,
	C_TS_WITH TIMESTAMP WITH TIME ZONE) DISTRIBUTE BY HASH (C_INT) 
	partition by range (C_INT)
( 
     partition TBL_COMBINE_NESTLOOP_SEQSCAN_04_1 values less than (30),
     partition TBL_COMBINE_NESTLOOP_SEQSCAN_04_2 values less than (50),
     partition TBL_COMBINE_NESTLOOP_SEQSCAN_04_3 values less than (80),
	 partition TBL_COMBINE_NESTLOOP_SEQSCAN_04_4 values less than (150),
	 partition TBL_COMBINE_NESTLOOP_SEQSCAN_04_5 values less than (301)
);
CREATE INDEX idx_TBL_COMBINE_NESTLOOP_SEQSCAN_04_01 ON TBL_COMBINE_NESTLOOP_SEQSCAN_04 (C_INT_01) LOCAL;
CREATE INDEX idx_TBL_COMBINE_NESTLOOP_SEQSCAN_04_02 ON TBL_COMBINE_NESTLOOP_SEQSCAN_04 (C_BIGINT_01) LOCAL;
CREATE OR REPLACE PROCEDURE PROC_TBL_COMBINE_NESTLOOP_SEQSCAN_04()
AS  
BEGIN  
       FOR I IN 1..300 LOOP  
         INSERT INTO TBL_COMBINE_NESTLOOP_SEQSCAN_04_xx5 VALUES('Y','SEQSCAN_04','SEQSCAN_04_RANGE','b','SEQSCAN_04','SEQSCAN_04_RANGE_TABLE_300'||i,i,i+1,i,i+1,i+2,i+1.1,1.11,1.111,'2000-01-01','2000-01-01 01:01:01','2000-01-01 01:01:01+01');
       END LOOP; 
END;  
/
CALL PROC_TBL_COMBINE_NESTLOOP_SEQSCAN_04();
insert into TBL_COMBINE_NESTLOOP_SEQSCAN_04 select * from TBL_COMBINE_NESTLOOP_SEQSCAN_04_xx5;

SET TIME ZONE 'PRC';set datestyle to iso;
SET CURRENT_SCHEMA='vector_mergejoin_engine';
SET ENABLE_INDEXONLYSCAN=off;
SET ENABLE_HASHJOIN=OFF;
SET ENABLE_MERGEJOIN=ON;
SET ENABLE_INDEXSCAN=ON;
SET ENABLE_NESTLOOP=OFF;
SET ENABLE_SEQSCAN=OFF;
SET ENABLE_BITMAPSCAN=OFF;

--explain analyze 
Select 
T0.C_INT_01 AS T0_C_INT_01,
T0.C_BIGINT_01 AS T0_C_BIGINT_01
 From
  TBL_COMBINE_NESTLOOP_SEQSCAN_00 T0,
  TBL_COMBINE_NESTLOOP_SEQSCAN_01 T1,
  TBL_COMBINE_NESTLOOP_SEQSCAN_02 T2,
  TBL_COMBINE_NESTLOOP_SEQSCAN_03 T3,
  TBL_COMBINE_NESTLOOP_SEQSCAN_04 T4
 Where
 T0.C_INT_01>=5
 and T0.C_INT_01<50
 and T1.C_INT_01>=7
 and T1.C_INT_01<50
 and T0.C_INT_01 =  T1.C_INT_01
 and T2.C_INT_01>=8
 and T2.C_INT_01<40
 and T0.C_INT_01 =  T2.C_INT_01
 and T0.C_BIGINT_01 = T2.C_BIGINT_01
 and 
 T3.C_INT_01>=10
 and T3.C_INT_01<100
 and T0.C_INT_01 =  T3.C_INT_01
 and T4.C_INT_01>=15
 and T4.C_INT_01<200
 and T0.C_INT_01 =  T4.C_INT_01
 ORDER BY 1,2 DESC LIMIT 500 OFFSET 40000; 
  
 
--explain analyze 
Select 
T1.C_INT_01 AS T1_C_INT_01,
T1.C_BIGINT_01 AS T1_C_BIGINT_01
 From
  TBL_COMBINE_NESTLOOP_SEQSCAN_00 T0,
  TBL_COMBINE_NESTLOOP_SEQSCAN_01 T1,
  TBL_COMBINE_NESTLOOP_SEQSCAN_02 T2,
  TBL_COMBINE_NESTLOOP_SEQSCAN_03 T3,
  TBL_COMBINE_NESTLOOP_SEQSCAN_04 T4
 Where
T0.C_INT_01>=5
 and T0.C_INT_01<50
 and T1.C_INT_01>=7
 and T1.C_INT_01<50
 and T0.C_INT_01 =  T1.C_INT_01
 and T2.C_INT_01>=8
 and T2.C_INT_01<40
 and T0.C_INT_01 =  T2.C_INT_01
 and T0.C_BIGINT_01 = T2.C_BIGINT_01
 and 
 T3.C_INT_01>=5
 and T3.C_INT_01<100
 and T0.C_INT_01 =  T3.C_INT_01
 and T4.C_INT_01>=5
 and T4.C_INT_01<200
 and T0.C_INT_01 =  T4.C_INT_01
 ORDER BY 1,2 DESC LIMIT 3000 OFFSET 30000; 
 
 
--explain analyze 
Select 
T2.C_INT_01 AS T2_C_INT_01,
T2.C_BIGINT_01 AS T2_C_BIGINT_01
 From
  TBL_COMBINE_NESTLOOP_SEQSCAN_00 T0,
  TBL_COMBINE_NESTLOOP_SEQSCAN_01 T1,
  TBL_COMBINE_NESTLOOP_SEQSCAN_02 T2,
  TBL_COMBINE_NESTLOOP_SEQSCAN_03 T3,
  TBL_COMBINE_NESTLOOP_SEQSCAN_04 T4
 Where
T0.C_INT_01>=5
 and T0.C_INT_01<50
 and T1.C_INT_01>=7
 and T1.C_INT_01<50
 and T0.C_INT_01 =  T1.C_INT_01
 and T2.C_INT_01>=8
 and T2.C_INT_01<40
 and T0.C_INT_01 =  T2.C_INT_01
 and T0.C_BIGINT_01 = T2.C_BIGINT_01
 and 
 T3.C_INT_01>=2
 and T3.C_INT_01<100
 and T0.C_INT_01 =  T3.C_INT_01
 and T4.C_INT_01>=5
 and T4.C_INT_01<200
 and T0.C_INT_01 =  T4.C_INT_01
 ORDER BY 1,2 DESC LIMIT 1000 OFFSET 5000; 
 
 
--explain analyze 
Select 
T3.C_INT_01 AS T3_C_INT_01,
T3.C_BIGINT_01 AS T3_C_BIGINT_01
 From
  TBL_COMBINE_NESTLOOP_SEQSCAN_00 T0,
  TBL_COMBINE_NESTLOOP_SEQSCAN_01 T1,
  TBL_COMBINE_NESTLOOP_SEQSCAN_02 T2,
  TBL_COMBINE_NESTLOOP_SEQSCAN_03 T3,
  TBL_COMBINE_NESTLOOP_SEQSCAN_04 T4
 Where
T0.C_INT_01>=5
 and T0.C_INT_01<50
 and T1.C_INT_01>=7
 and T1.C_INT_01<50
 and T0.C_INT_01 =  T1.C_INT_01
 and T2.C_INT_01>=8
 and T2.C_INT_01<40
 and T0.C_INT_01 =  T2.C_INT_01
 and T0.C_BIGINT_01 = T2.C_BIGINT_01
 and 
 T3.C_INT_01>=2
 and T3.C_INT_01<40
 and T0.C_INT_01 =  T3.C_INT_01
 and T4.C_INT_01>=5
 and T4.C_INT_01<30
 and T0.C_INT_01 =  T4.C_INT_01
 ORDER BY 1,2 DESC; 

-- test for Vector Merge Full Join
SET ENABLE_NESTLOOP TO FALSE;
SET ENABLE_HASHJOIN TO FALSE; 

CREATE TABLE TBL_MERGEJOIN_NATURAL_OUT_1200501(ID INTEGER,NAME VARCHAR(3000), PARTIAL CLUSTER KEY(ID)) WITH (orientation = orc) tablespace hdfs_ts DISTRIBUTE BY HASH (ID) ;
CREATE INDEX TBL_MERGEJOIN_NATURAL_OUT_1200501_IDX ON TBL_MERGEJOIN_NATURAL_OUT_1200501(ID);
CREATE TABLE TBL_MERGEJOIN_NATURAL_IN_1200501(ID INTEGER,ADDRESS VARCHAR(3000), PARTIAL CLUSTER KEY(ADDRESS)) WITH (orientation = orc) tablespace hdfs_ts DISTRIBUTE BY HASH (ADDRESS) ;
CREATE INDEX TBL_MERGEJOIN_NATURAL_IN_1200501_IDX ON TBL_MERGEJOIN_NATURAL_IN_1200501(ID);

CREATE OR REPLACE PROCEDURE PRO_MERGEJOIN_NATURAL_IN_1200501()
AS
BEGIN
	FOR I IN 1..200 LOOP
		INSERT INTO TBL_MERGEJOIN_NATURAL_IN_1200501  VALUES (3,'IN_CHINA_3_'||I);
	END LOOP; 
END;
/
CALL PRO_MERGEJOIN_NATURAL_IN_1200501();
SELECT COUNT(*) FROM TBL_MERGEJOIN_NATURAL_IN_1200501;

CREATE OR REPLACE PROCEDURE PRO_MERGEJOIN_NATURAL_OUT_1200501()
AS
BEGIN
	FOR I IN 1..5 LOOP
		INSERT INTO TBL_MERGEJOIN_NATURAL_OUT_1200501  VALUES (I,'OUT_JEFF_'||I);
	END LOOP; 
END;
/
CALL PRO_MERGEJOIN_NATURAL_OUT_1200501();

EXPLAIN (costs off)
SELECT *
FROM TBL_MERGEJOIN_NATURAL_IN_1200501 NATURAL
FULL JOIN TBL_MERGEJOIN_NATURAL_OUT_1200501
ORDER BY 1, 2, 3 ASC;

SELECT *
FROM TBL_MERGEJOIN_NATURAL_IN_1200501 NATURAL
FULL JOIN TBL_MERGEJOIN_NATURAL_OUT_1200501
ORDER BY 1, 2, 3 ASC;

----
--- Clean table and resource
----
drop schema vector_mergejoin_engine cascade;
