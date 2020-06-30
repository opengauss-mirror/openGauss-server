/*
 * This file is used to test the function of ExecVecMergeJoin(): part 0: prepare table and insert data
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
);

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
);

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
)with(orientation = column);

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
)with(orientation = column);

CREATE OR REPLACE PROCEDURE func_insert_tbl_mergejoin_01()
AS
BEGIN
        FOR I IN 0..200 LOOP
                if i = 19 OR i = 59 OR i = 159 then
                        INSERT INTO vector_mergejoin_engine.row_mergejoin_table_01 VALUES(i,NULL,NULL, '2015-01-01',i+0.1,i+0.2,i+0.3,'11:40:22+06','1 day 13:24:56','["Sep 5, 1983 23:59:12" "Oct6, 1983 23:59:12"]');
				elsif i = 15 OR i = 30 OR i = 150 then
                        INSERT INTO vector_mergejoin_engine.row_mergejoin_table_01 VALUES(i,'mergejoin_char'||i,'mergejoin_varchar'||i,'2015-01-01',i+0.1,i+0.2,i+0.3,'11:20:22+06','2 day 13:24:56','["Sep 4, 1983 23:59:12" "Oct4, 1983 23:59:12"]');
                        INSERT INTO vector_mergejoin_engine.row_mergejoin_table_01 VALUES(i,'mergejoin_char'||i,'mergejoin_varchar'||i,'2015-01-01',i+0.1,i+0.2,i+0.3,'11:24:22+06','2 day 13:24:56','["Sep 4, 1983 23:59:12" "Oct4, 1983 23:59:12"]');
				elsif i = 45 OR i = 80 OR i = 120 then
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
        FOR I IN 0..200 LOOP
                if i = 2 OR i = 32 OR i = 52 OR i = 172 then
                        INSERT INTO vector_mergejoin_engine.row_mergejoin_table_02 VALUES(i,NULL,NULL, '2015-01-01',i+0.1,i+0.2,i+0.3,'11:40:22+06','2 day 13:24:56','["Sep 4, 1983 23:59:12" "Oct4, 1983 23:59:12"]');
				elsif i = 25 OR i = 48 OR i = 148 then
                        INSERT INTO vector_mergejoin_engine.row_mergejoin_table_02 VALUES(i,'mergejoin_char'||i,'mergejoin_varchar'||i,'2015-01-01',i+0.1,i+0.2,i+0.3,'11:40:22+06','5 day 13:24:56','["Sep 4, 1983 23:59:12" "Oct4, 1983 23:59:12"]');
                        INSERT INTO vector_mergejoin_engine.row_mergejoin_table_02 VALUES(i,'mergejoin_char'||i,'mergejoin_varchar'||i,'2015-01-01',i+0.1,i+0.2,i+0.3,'11:20:22+06','3 day 13:24:56','["Sep 4, 1983 23:59:12" "Oct4, 1983 23:59:12"]');
				elsif i = 46 OR i = 73 OR i = 180 then
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
)with (orientation=column);

create table vector_mergejoin_engine.VECTOR_MERGEJOIN_TABLE_04
(
   ID int NOT NULL
  ,STREET varchar(20) NOT NULL
  ,ZIP char(9) NOT NULL
  ,C_D_ID int NOT NULL
  ,C_ID int NOT NULL
)with (orientation=column);

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
)with (orientation=column);

create table vector_mergejoin_engine.VECTOR_MERGEJOIN_TABLE_06
(
   ID int
  ,STREET varchar(20) 
  ,ZIP char(9)
  ,C_D_ID int 
  ,C_ID int
)with (orientation=column);

create table vector_mergejoin_engine.VECTOR_MERGEJOIN_TABLE_07
(
   ID int
  ,name varchar(10)
  ,street varchar(20)
  ,d_id int
)with (orientation=column);

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

create table vector_mergejoin_engine.VECTOR_MERGEJOIN_TABLE_08
(
   col_int      int 
  ,col_char     char(25)
  ,col_vchar    varchar(35)
  ,col_date     date
  ,col_num      numeric(10,4)
  ,col_float1   float4
  ,col_float2   float8
  ,col_timetz	 timetz
  ,col_interval interval
  ,col_tinterval	 tinterval
)with(orientation = column);

