set enable_global_stats = true;
/*
 * This file is used to test the function of ExecVecNestLoop()
 */

----
--- Create Table and Insert Data
----
create schema vec_nestloop_engine;
set current_schema = vec_nestloop_engine;
SET statement_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET default_tablespace = '';
SET default_with_oids = false;

create table vec_nestloop_engine.VECTOR_NESTLOOP_TABLE_01
(
   col_int	int
  ,col_char	char(25)
  ,col_vchar	varchar(35)
  ,col_date	date
  ,col_num	numeric(10,4)
  ,col_float1	float4
  ,col_float2	float8
  ,col_timetz	timetz
) with(orientation = orc) tablespace hdfs_ts distribute by hash(col_int);

create table vec_nestloop_engine.VECTOR_NESTLOOP_TABLE_02
(
   col_int	int
  ,col_char	char(25)
  ,col_vchar	varchar(35)
  ,col_date	date
  ,col_num	numeric(10,4)
  ,col_float1	float4
  ,col_float2	float8
  ,col_timetz	timetz
) with(orientation = orc) tablespace hdfs_ts distribute by hash(col_int);

create table vec_nestloop_engine.ROW_NESTLOOP_TABLE_03
(
   col_int	int
  ,col_char	char(25)
  ,col_vchar	varchar(35)
  ,col_date	date
  ,col_num	numeric(10,4)
  ,col_float1	float4
  ,col_float2	float8
  ,col_timetz	timetz
);

create table vec_nestloop_engine.ROW_NESTLOOP_TABLE_04
(
   col_int	int
  ,col_char	char(25)
  ,col_vchar	varchar(35)
  ,col_date	date
  ,col_num	numeric(10,4)
  ,col_float1	float4
  ,col_float2	float8
  ,col_timetz	timetz
);

create table vec_nestloop_engine.VECTOR_NESTLOOP_TABLE_03
(
   col_int	int
  ,col_char	char(25)
  ,col_vchar	varchar(35)
  ,col_date	date
  ,col_num	numeric(10,4)
  ,col_float1	float4
  ,col_float2	float8
  ,col_timetz	timetz
) with(orientation = orc) tablespace hdfs_ts distribute by hash(col_int);

create table vec_nestloop_engine.VECTOR_NESTLOOP_TABLE_04
(
   col_int	int
  ,col_char	char(25)
  ,col_vchar	varchar(35)
  ,col_date	date
  ,col_num	numeric(10,4)
  ,col_float1	float4
  ,col_float2	float8
  ,col_timetz	timetz
) with(orientation = orc) tablespace hdfs_ts distribute by hash(col_int);

COPY VECTOR_NESTLOOP_TABLE_01(col_int, col_char, col_vchar, col_date, col_num, col_float1, col_float2, col_timetz) FROM stdin;
1	test_char_1	test_varchar_1	2012-12-01	10.01	20.01	30.01	16:00:01+08
4	\N	\N	2012-12-04	10.04	20.04	30.04	16:00:04+08
5	test_char_5	test_varchar_5	2012-12-05	10.05	20.05	30.05	16:00:05+08
7	test_char_7	\N	2012-12-07	10.07	20.07	30.07	16:00:07+08
9	test_char_9	test_varchar_9	2012-12-09	10.09	20.09	30.09	16:00:09+08
10	test_char_10	\N	2012-12-10	10.10	20.10	30.10	16:00:10+08
11	test_char_11	test_varchar_11	2012-12-11	10.11	20.11	30.11	16:00:11+08
12	\N	test_varchar_12	2012-12-12	10.12	20.12	30.12	16:00:12+08
14	test_char_14	test_varchar_14	2012-12-14	10.14	20.14	30.14	16:00:14+08
\.

COPY VECTOR_NESTLOOP_TABLE_02(col_int, col_char, col_vchar, col_date, col_num, col_float1, col_float2, col_timetz) FROM stdin;
3	test_char_3	test_varchar_3	2012-12-03	10.13	20.13	30.13	16:00:03+08
8	test_char_8	\N	2012-12-08	10.18	20.18	30.18	16:00:08+08
6	test_char_6	test_varchar_6	2012-12-06	10.16	20.16	30.16	16:00:06+08
2	test_char_2	\N	2012-12-02	10.11	20.11	30.11	16:00:02+08
13	test_char_13	test_varchar_13	2012-12-13	10.13	20.13	30.13	16:00:13+08
15	\N	test_varchar_15	2012-12-15	10.15	20.15	30.15	16:00:15+08
1	test_char_1	test_varchar_1	2012-12-01	10.01	20.01	30.01	16:00:01+08
4	\N	\N	2012-12-04	10.04	20.04	30.04	16:00:04+08
\.

CREATE OR REPLACE PROCEDURE func_insert_tbl_nestloop_03()
AS  
BEGIN  
	FOR I IN 1..1500 LOOP 
	 	if i = 20 OR i = 30 OR i = 60 OR i = 70 then
         		INSERT INTO vec_nestloop_engine.row_nestloop_table_03 VALUES(i,NULL,NULL, '2015-01-01',9.12+i,99.123+i,999.1234+i,'16:00:00+08');
	  	else
         		INSERT INTO vec_nestloop_engine.row_nestloop_table_03 VALUES(i,'Vectorize_char'||i,'Vectorize_Varchar'||i,'2015-01-01',9.12+i,99.123+i,999.1234+i,'16:00:00+08');
		end if;
     	END LOOP; 
END;
/
CALL func_insert_tbl_nestloop_03();

CREATE OR REPLACE PROCEDURE func_insert_tbl_nestloop_04()
AS  
BEGIN  
	FOR I IN 501..2500 LOOP 
	 	if i = 1026 then
         		INSERT INTO vec_nestloop_engine.row_nestloop_table_04 VALUES(i,NULL,NULL, '2016-01-01',9.12+i-500,99.123+i-500,999.1234+i-500,'16:00:00+08');
	  	else
         		INSERT INTO vec_nestloop_engine.row_nestloop_table_04 VALUES(i,'Vectorize_char'||i,'Vectorize_Varchar'||i,'2016-01-01',9.12+i-500,99.123+i-500,499.1234+i,'18:00:00+08');
		end if;
     	END LOOP; 
END;
/
CALL func_insert_tbl_nestloop_04();

insert into vector_nestloop_table_03 select * from row_nestloop_table_03;
insert into vector_nestloop_table_04 select * from row_nestloop_table_04;

CREATE TABLE VECTOR_NESTLOOP_TABLE_05 (
    id integer
   ,name character varying(100)
   ,zip character(9)
)
WITH (orientation = column)
DISTRIBUTE BY HASH (id)
PARTITION BY RANGE (id)
(
    PARTITION b1_p1 VALUES LESS THAN (3),
    PARTITION b1_p2 VALUES LESS THAN (4),
    PARTITION b1_p3 VALUES LESS THAN (MAXVALUE)
);

CREATE TABLE VECTOR_NESTLOOP_TABLE_06 (
    c_id integer,
    street character varying(300),
    zip character(9),
    c_d_id bigint,
    id integer
)
WITH (orientation = column)
DISTRIBUTE BY HASH (c_id)
PARTITION BY RANGE (c_id)
(
    PARTITION b5_p1 VALUES LESS THAN (3),
    PARTITION b5_p2 VALUES LESS THAN (4),
    PARTITION b5_p3 VALUES LESS THAN (30),
    PARTITION b5_p4 VALUES LESS THAN (31),
    PARTITION b5_p5 VALUES LESS THAN (1000),
    PARTITION b5_p6 VALUES LESS THAN (MAXVALUE)
);

CREATE TABLE VECTOR_NESTLOOP_TABLE_07 (
    id integer,
    street character varying(20),
    zip character(9),
    c_d_id integer,
    c_w_id integer
)
WITH (orientation = column)
DISTRIBUTE BY HASH (id)
PARTITION BY RANGE (c_d_id)
(
    PARTITION b7_p1 VALUES LESS THAN (1),
    PARTITION b7_p2 VALUES LESS THAN (2),
    PARTITION b7_p3 VALUES LESS THAN (3),
    PARTITION b7_p4 VALUES LESS THAN (4),
    PARTITION b7_p5 VALUES LESS THAN (5),
    PARTITION b7_p6 VALUES LESS THAN (6),
    PARTITION b7_p7 VALUES LESS THAN (7),
    PARTITION b7_p8 VALUES LESS THAN (8),
    PARTITION b7_p9 VALUES LESS THAN (9),
    PARTITION b7_p10 VALUES LESS THAN (10),
    PARTITION b7_p11 VALUES LESS THAN (MAXVALUE)
);

CREATE TABLE VECTOR_NESTLOOP_TABLE_08 (
    n_nationkey integer NOT NULL,
    n_name character(25) NOT NULL,
    n_regionkey integer NOT NULL,
    n_comment character varying(152)
)
with (orientation = orc) tablespace hdfs_ts
DISTRIBUTE BY HASH (n_nationkey);

COPY VECTOR_NESTLOOP_TABLE_05(id, name, zip) FROM stdin;
2	jaqspofube	496611111
6	escpbk	784411111
5	vsfcguexuf	329711111
8	wzdnxwhm	979511111
9	ydcuynmyud	684011111
\N	\N	\N
3	qcscbhkkql	545511111
4	dmkczswa	522411111
7	jcanwmh	950211111
10	wfnlmpcw	760511111
\.

COPY VECTOR_NESTLOOP_TABLE_06(c_id, street, zip, c_d_id, id) FROM stdin;
2	zujptddcedguji	233511111	5	9
12	eewmfgftyc	140811111	8	10
28	naeassocyxnpvs	553811111	2	1
17	ptmbcyxfikvk	600111111	10	1
68	nwzxupcyxjn	554811111	4	2
50	ocedlkninihebuomng	619811111	8	5
77	qlpcyxvkyecvjmkv	632411111	2	1
120	rfpxtulhavcyxkolal	354011111	10	2
76	tiwuioyuoycyxxsbg	096211111	5	1
108	xhqacedixtkfik	506111111	3	10
56	zcedsocxnsnef	058911111	8	7
\N	\N	\N	\N	\N
\N	\N	\N	90909	\N
29	vvhimihhcyxf	311811111	8	2
11	gvfocmcvjngqpocyxsg	837311111	2	9
98	udaecyxekvpoasenoj	161011111	2	2
103	lrhcedkfecnkbmlf	787311111	5	2
33	ncyxyxqdszhhezyfm	428111111	1	2
114	kftymjawqx	449711111	5	2
48	cedghmwtxzm	506311111	3	2
140	sftymujjkicrzku	567211111	4	3
100	vvkvhmgmftyuxuqmt	063011111	6	4
97	zklftymlrzwx	966611111	1	4
205	idyecedfrbjpowbcrjy	997411111	3	4
34	eutefcedyuciynb	200511111	5	5
117	rjycyxqraqbmsfvlau	439811111	4	5
61	tsllexdwkqqucyx	713911111	1	6
\.

COPY VECTOR_NESTLOOP_TABLE_07(id, street, zip, c_d_id, c_w_id) FROM stdin;
5	kqknnhddbcjdatsxo	264111111	2	7
6	nsbfftgmawdlhtzul	107911111	2	7
8	uonensnprshnqjchfwrg	050311111	2	7
5	eubtksospds	751111111	3	7
6	jiwttgwzublcartaz	046211111	3	7
9	lpppbjkmtgittjsygci	929111111	3	7
7	ubppxzkxodtvimcpqvzz	819011111	2	7
4	zkficldpmrfy	473211111	2	7
3	xtfgtclbjpqtmglgtiih	538111111	3	7
\N	\N	\N	1890	2
7	jhrhlimigpvdowex	004911111	3	7
10	fnetmkshnfbzommw	982311111	3	7
4	gyownovalcnxhetrksly	731611111	3	7
\.

COPY VECTOR_NESTLOOP_TABLE_08 (n_nationkey, n_name, n_regionkey, n_comment) FROM stdin;
13	JORDAN                   	4	ic deposits are blithely about the carefully regular pa
17	PERU                     	1	platelets. blithely pending dependencies use fluffily across the even pinto beans. carefully silent accoun
19	ROMANIA                  	3	ular asymptotes are about the furious multipliers. express dependencies nag above the ironically ironic account
4	EGYPT                    	4	y above the carefully unusual theodolites. final dugouts are quickly across the furiously regular d
10	IRAN                     	4	efully alongside of the slyly final dependencies. 
16	MOZAMBIQUE               	0	s. ironic, unusual asymptotes wake blithely r
20	SAUDI ARABIA             	4	ts. silent requests haggle. closely express packages sleep across the blithely
21	VIETNAM                  	2	hely enticingly express accounts. even, final 
5	ETHIOPIA                 	0	ven packages wake quickly. regu
8	INDIA                    	2	ss excuses cajole slyly across the packages. deposits print aroun
12	JAPAN                    	2	ously. final, express gifts cajole a
23	UNITED KINGDOM           	3	eans boost carefully special requests. accounts are. carefull
7	GERMANY                  	3	l platelets. regular accounts x-ray: unusual, regular acco
18	CHINA                    	2	c dependencies. furiously express notornis sleep slyly regular accounts. ideas sleep. depos
6	FRANCE                   	3	refully final requests. regular, ironi
0	ALGERIA                  	0	 haggle. carefully final deposits detect slyly agai
3	CANADA                   	1	eas hang ironic, silent packages. slyly regular packages are furiously over the tithes. fluffily bold
11	IRAQ                     	4	nic deposits boost atop the quickly final requests? quickly regula
22	RUSSIA                   	3	 requests against the platelets use never according to the quickly regular pint
24	UNITED STATES            	1	y final packages. slow foxes cajole quickly. quickly silent platelets breach ironic accounts. unusual pinto be
1	ARGENTINA                	1	al foxes promise slyly according to the regular accounts. bold requests alon
2	BRAZIL                   	1	y alongside of the pending deposits. carefully special packages are about the ironic forges. slyly special 
9	INDONESIA                	2	 slyly express asymptotes. regular deposits haggle slyly. carefully ironic hockey players sleep blithely. carefull
15	MOROCCO                  	0	rns. blithely bold courts among the closely regular packages use furiously bold platelets?
14	KENYA                    	0	 pending excuses haggle furiously deposits. pending, express pinto beans wake fluffily past t
\.

analyze vector_nestloop_table_01;
analyze vector_nestloop_table_02;
analyze vector_nestloop_table_03;
analyze vector_nestloop_table_04;
analyze vector_nestloop_table_05;
analyze vector_nestloop_table_06;
analyze vector_nestloop_table_07;
analyze vector_nestloop_table_08;
----
--- case 1: NestLoop Inner Join
----
set enable_force_vector_engine=on;

create or replace function test_vec_Nestloop() returns integer
as
$$
	declare int_result_1 integer;
	int_result_2 integer;
        var_result_1 varchar;
        var_result_2 varchar;
begin
	select A.col_vchar, B.col_vchar into var_result_1, var_result_2 from vector_nestloop_table_01 A, vector_nestloop_table_02 B where A.col_int < B.col_int order by 1, 2 limit 1;
        raise info '%, %', var_result_1, var_result_2;
        return 0;
end;
$$language plpgsql;
select test_vec_Nestloop();

explain (verbose on, costs off) select A.col_int, B.col_int, A.col_vchar, B.col_vchar from vector_nestloop_table_01 A, vector_nestloop_table_02 B where A.col_int < B.col_int order by 1,2;
select A.col_int, B.col_int, A.col_vchar, B.col_vchar from vector_nestloop_table_01 A, vector_nestloop_table_02 B where A.col_int < B.col_int order by 1,2;
select A.col_vchar, B.col_vchar from vector_nestloop_table_01 A, vector_nestloop_table_02 B where A.col_vchar < B.col_vchar order by 1,2;
select sum(A.col_int), max(A.col_int), avg(B.col_int), min(B.col_int) from vector_nestloop_table_01 A, vector_nestloop_table_02 B where A.col_int < B.col_int order by 1, 2;
select max(A.col_vchar), min(B.col_vchar) from vector_nestloop_table_01 A, vector_nestloop_table_02 B where A.col_vchar < B.col_vchar group by A.col_vchar order by 1, 2;
select * from vector_nestloop_table_03 A, vector_nestloop_table_04 B where A.col_int > B.col_int and A.col_int < 500;
select A.col_int, B.col_int, A.col_float1, B.col_float1 from vector_nestloop_table_03 A, vector_nestloop_table_04 B where A.col_int >= B.col_int and A.col_int >= 1500 order by 1 ,2;
select count(*) from vector_nestloop_table_03 A, vector_nestloop_table_04 B where B.col_int > A.col_int and A.col_int < 3;

----
--- case 2: NestLoop Left Join
----
select A.col_vchar, B.col_vchar from vector_nestloop_table_03 A left join vector_nestloop_table_04 B on (A.col_int > B.col_int) where A.col_int < 500 order by 1, 2 limit 220;
select A.col_int, B.col_int, A.col_date, b.col_date from vector_nestloop_table_03 A left join vector_nestloop_table_04 B on(A.col_int < B.col_int) where A.col_int = 550 order by 1, 2 limit 1000;

----
--- case 3: NestLoop Anti Join and Semi Join
----
set enable_mergejoin=false;
set enable_hashjoin=false;
explain (verbose on, costs off) select * from vector_nestloop_table_01 A where A.col_int in (select B.col_int from vector_nestloop_table_02 B);
explain (verbose on, costs off) select * from vector_nestloop_table_01 A where A.col_int not in (select B.col_int from vector_nestloop_table_02 B);
select * from vector_nestloop_table_01 A where A.col_int in (select B.col_int from vector_nestloop_table_02 B) order by 1, 2;
select count(*) from vector_nestloop_table_01 A where A.col_int in (select B.col_int from vector_nestloop_table_02 B);
select * from vector_nestloop_table_01 A where A.col_int not in (select B.col_int from vector_nestloop_table_02 B) order by 1, 2;
select count(*) from vector_nestloop_table_01 A where A.col_int not in (select B.col_int from vector_nestloop_table_02 B);

----
--- Special Case: both joinqual and otherqual be NULL
----
select A.col_int as c1, A.col_char as c2, A.col_vchar from vector_nestloop_table_01 A inner join vector_nestloop_table_01 B on A.col_int = B.col_int where A.col_int=1 and A.col_char = 'test_char_1' and B.col_char = 'test_char_1' order by 1, 2, 3;
select count(*) from vector_nestloop_table_01 A inner join vector_nestloop_table_01 B on A.col_int = B.col_int where A.col_int=1 and A.col_char = 'test_char_1' and B.col_char = 'test_char_1';

CREATE INDEX vecvtor_nestloop_base_index_01 ON VECTOR_NESTLOOP_TABLE_05 USING psort (id) LOCAL(PARTITION b1_p1_id_idx, PARTITION b1_p2_id_idx, PARTITION b1_p3_id_idx) ;
CREATE INDEX vecvtor_nestloop_base_index_02 ON VECTOR_NESTLOOP_TABLE_06 USING psort (id, c_d_id, c_id) LOCAL(PARTITION b5_p1_id_c_d_id_c_id_idx, PARTITION b5_p2_id_c_d_id_c_id_idx, PARTITION b5_p3_id_c_d_id_c_id_idx, PARTITION b5_p4_id_c_d_id_c_id_idx, PARTITION b5_p5_id_c_d_id_c_id_idx, PARTITION b5_p6_id_c_d_id_c_id_idx) ;
CREATE INDEX vecvtor_nestloop_base_index_03 ON VECTOR_NESTLOOP_TABLE_07 USING psort (id, c_d_id, c_w_id) LOCAL(PARTITION b7_p1_id_c_d_id_c_w_id_idx, PARTITION b7_p2_id_c_d_id_c_w_id_idx, PARTITION b7_p3_id_c_d_id_c_w_id_idx, PARTITION b7_p4_id_c_d_id_c_w_id_idx, PARTITION b7_p5_id_c_d_id_c_w_id_idx, PARTITION b7_p6_id_c_d_id_c_w_id_idx, PARTITION b7_p7_id_c_d_id_c_w_id_idx, PARTITION b7_p8_id_c_d_id_c_w_id_idx, PARTITION b7_p9_id_c_d_id_c_w_id_idx, PARTITION b7_p10_id_c_d_id_c_w_id_idx, PARTITION b7_p11_id_c_d_id_c_w_id_idx) ;
CREATE INDEX zip_idx ON VECTOR_NESTLOOP_TABLE_05 USING psort (zip) LOCAL(PARTITION b1_p1_zip_idx, PARTITION b1_p2_zip_idx, PARTITION b1_p3_zip_idx) ;

SELECT 	A.id, A.name, A.zip FROM VECTOR_NESTLOOP_TABLE_05 A WHERE (A.id IN (SELECT distinct C.id FROM VECTOR_NESTLOOP_TABLE_07 C WHERE NOT (EXISTS (SELECT * FROM VECTOR_NESTLOOP_TABLE_06 B WHERE B.c_id = C.id)))) ORDER BY 1,2,3;

----
--- Special case: material all
----
explain (verbose on, costs off) select * from vector_nestloop_table_01 A, vector_nestloop_table_02 B, vector_nestloop_table_03 C, vector_nestloop_table_04 D where A.col_int = B.col_int and C.col_int = D.col_int order by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20 limit 5;
select * from vector_nestloop_table_01 A, vector_nestloop_table_02 B, vector_nestloop_table_03 C, vector_nestloop_table_04 D where A.col_int = B.col_int and C.col_int = D.col_int order by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20 limit 5;

---
---
explain (verbose on, costs off) select count(*) from vector_nestloop_table_08 t1 left join vector_nestloop_table_08 t2 on t1.n_nationkey=t2.n_regionkey
 where CASE WHEN t2.n_regionkey=1 then '1' else '2' end = '2';
select count(*) from vector_nestloop_table_08 t1 left join vector_nestloop_table_08 t2 on t1.n_nationkey=t2.n_regionkey
 where CASE WHEN t2.n_regionkey=1 then '1' else '2' end = '2';

----
--- Clean table and resource
----
drop schema vec_nestloop_engine cascade;
