/*
 * This file is used to test the function of sort & sortagg with LLVM Optimization
 */
----
--- Create Table and Insert Data
----
drop schema if exists llvm_vecsort_engine cascade;
create schema llvm_vecsort_engine;
set current_schema = llvm_vecsort_engine;
set codegen_cost_threshold=0;

CREATE TABLE llvm_vecsort_engine.LLVM_VECSORT_TABLE_01(
    col_int1	int,
    col_int2	int,
    col_bint	bigint,
    coL_char1	char(1),
    col_bpchar1	bpchar(3),
    col_bpchar2	bpchar(7),
    col_bpchar3	bpchar(15),
    col_bpchar4	bpchar(24),
    col_varchar1	varchar(8),
    col_varchar2	varchar(11),
    col_varchar3	varchar(23),
    col_varchar4	varchar(40)
)with(orientation=column);

copy llvm_vecsort_table_01 from stdin;
1	1	126	A	TUI	ZUK	tianjing	z25687459	test	bottle	whatisgoodfortodaysfood	begreat
2	2	256	B	ZUI	JUK	beijing	bookisfriend      	far	bottle	zooisforchildren	letusmakeupajokeforteacher
1	1	458	A	TUI	ZUKK	tianjing 	j00142589678	great	uuuuuuuuuuu	begreater	formulatedwithsqualanceandantioxidant
3	89	2158744521187556	B	TI	ZUKK6	shijiazhu	j00142589678	greatest	greate	uoooooooooou	formulatedwithsqualanceandantioxidant
7	123	9856	C	KUX	XJPUIO	shijiazhu	kehlssince1985	P	outputlink	\N	hereisshe
7	123	11116	B	ZUX	XJPUIO	shijiazhu 	kehlssince1985	LK  	outputlink	\N	hereisshe
9	\N	11116	B	\N	XJPUIOT	tianjing	kehlssince	LK  	outputlink	mergesort	hereissheandbeau
90	34	-89008987776	\N	AMX	AMX	\N	dayisdayisday	\N	uuuuuuuuuuu	begreater	formulatedwithsqualanceandantioxidant
90	34	0	\N	AMX	aMX	\N	dayisdayisday	\N	uuuuuuuuuuu	begreater	formulatedwithsqualanceandantioxidant
9	\N	-1	A	AMX	KUL	\N	dayisdayisday	ZK	TINK	begreater	\N
\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N
-21	-1	-58795	C	TUI	\N	shanghai	c	zara	\N	mergesort	letusmakeupajokefortea
0	0	12587487	B	tea	beauty	while	ZooK	be	P	mergesortinternal	likehere
21	1	145872599	N	CJ	LIKEA	\N	u	u	u	u	faraway
7	-72	45896	A	TMT	likehim	beagoodguy	justforfunandjustforfuna	how	\N	awayfromhome	whataboutwhat
7	-72	75896	A	TMT	likehim	beagoodguy	justforfunandjustforfuna	how	\N	awa	ABT
15	890	\N	D	UIL	be	akindleissogood	Zo	ugg	ugg	ki	u
17	58	26	D	NO	THH	akindleissogood	AJ	uuc	testa	far	farha
\.

CREATE TABLE llvm_vecsort_engine.LLVM_VECSORT_TABLE_02(
    col_int	int,
    col_bint	bigint,
    col_num1	numeric(9,2),
    col_num2	numeric(15,4),
    col_num3	numeric(18,0),
    col_num4	numeric(18,4),
    col_num5	numeric(22,18),
    col_num6	numeric(40,19),
    col_num7	numeric(40,24),
    col_text	text,
    col_vchar	varchar(15)
)with(orientation=column);

copy llvm_vecsort_table_02 from stdin;
1	125	2.56	145.2456	58325587	1587795.25	15.12589564856898458	145.200000004	478587878.2222222222222222222548	likehere	a
-1	25879	25.36	-12.3657	-24	698.25	201.021111111	1452.25878	254.365489879933333253	justforyou	are
0	-12547882214583	0.01	65.254	9865878588888	125.0000	1250.125800000000000001	0.01	256.36545	likeherebutthere	uggui
1	124	-3.56	-124.236	-9865472	-458.258	1254.25463	12547825478288752.25	45687.1258749	goanywhere	likeare
12587	1	-0.6	5686	875	12.258	-0.0000001	-458697.1258	-0.25896587	begreater	likeherea
9	1258795442145878	125.8	45.54	89657852	14587.2582	1.00	-78955877.2568752	-1458772.125458	likeherebutthere	likeareb
\N	76	986580.21	123456789.36	12584	1457.258	98.68932475689	125.2525252525252525	546.25647895426842563489	begoodandissolikeat	justforfun
\N	76	986580.21	123456789.36	12584	1457.258	98.68932475687	125.2525252525252524	546.25647895426842563488	begreateh	likehereb
2	\N	986580.21	963.3689	7854956	-3457.258	-98.68932475687	-125.2525252525252524	-546.25647895426842563488	begreateZUKI	likeherec
90	7	3657.25	0.0001	125	95.86	0.25	0.356	1.25689	TUI	KIND
27	-1	-256	-20.5	0	0	0	0	0	KUI	PORT
2	90	\N	98	789568	568.36	-0.25	-3.698	\N	YUI	portportportway
1	90	65	\N	867	589	58.3	69.333333333333333	2.3658789654789512587	input	jump
1	92	65	6.3	7	45.68	58.325856587496852478	69.333333333333333	2.365878965478951258725478	input	\N
1	986	45	3.3	14587952546710	\N	\N	69.333333333333332	2.365878965478951258725478	input	\N
-1	286	45	3.3	14587952546710	6.5	9.86	\N	\N	\N	gofaraway
2	-186	\N	3.3	-14587952546710	-6.89	9.86	693	9.368	begoodandissolikeah	gofaraway
2	86	\N	9.3	\N	6.89	\N	293	2.368	begoodandissolikeag	gofarawaye
27	4587	\N	9.3	6.36	6.21	56.3	193	2.368	\N	gofarawaye
688	-45879	-65.35	698.258	96	\N	\N	\N	9.6587	begreatet	dg
\.

create table llvm_vecsort_engine.LLVM_VECSORT_TABLE_03
(
    L_ORDERKEY    BIGINT NOT NULL
  , L_PARTKEY     BIGINT NOT NULL
  , L_SUPPKEY     BIGINT NOT NULL
  , L_LINENUMBER  BIGINT NOT NULL
  , L_QUANTITY    DECIMAL(15,2) NOT NULL
  , L_EXTENDEDPRICE  DECIMAL(15,2) NOT NULL
  , L_DISCOUNT    DECIMAL(15,2) NOT NULL
  , L_TAX         DECIMAL(15,2) NOT NULL
  , L_RETURNFLAG  CHAR(1) NOT NULL
  , L_LINESTATUS  CHAR(1) NOT NULL
  , L_SHIPDATE    DATE NOT NULL
  , L_COMMITDATE  DATE NOT NULL
  , L_RECEIPTDATE DATE NOT NULL
  , L_SHIPINSTRUCT CHAR(25) NOT NULL
  , L_SHIPMODE     CHAR(10) NOT NULL
  , L_COMMENT      VARCHAR(44) NOT NULL
) with (orientation = column);

insert into llvm_vecsort_table_03 select * from vector_engine.lineitem_vec where L_ORDERKEY < 4962;

analyze llvm_vecsort_table_01;
analyze llvm_vecsort_table_02;
analyze llvm_vecsort_table_03;

----
--- test1 : test comparemulticolumns with different data type in sort node
----
-- test with char, varchar type
select col_int1, col_int2 from llvm_vecsort_table_01 order by col_int1, col_int2;
select col_bint from llvm_vecsort_table_01 order by col_bint;
select col_char1 from llvm_vecsort_table_01 order by col_char1, col_bint;
select col_bpchar1, col_bpchar2 from llvm_vecsort_table_01 order by col_bpchar1, col_bpchar2;
select col_bpchar1, col_bpchar3 from llvm_vecsort_table_01 order by col_bpchar1, col_bpchar3; 
select col_bpchar3, col_bpchar4 from llvm_vecsort_table_01 order by col_bpchar3, col_bpchar4;
select col_varchar1, col_varchar2 from llvm_vecsort_table_01 order by col_varchar1, col_varchar2;
select col_varchar3, col_varchar4 from llvm_vecsort_table_01 order by col_varchar3, col_varchar4;
select col_varchar1, col_varchar3, col_varchar4 from llvm_vecsort_table_01 order by col_varchar1, col_varchar3, col_varchar4;
select col_int1, col_bint, col_varchar3, col_varchar4 from llvm_vecsort_table_01 order by col_int1, col_bint, col_varchar1, col_varchar3, col_varchar4;
select col_varchar1, col_varchar4 from (select * from llvm_vecsort_table_01 order by col_bpchar2, col_bpchar3) order by col_varchar1, col_varchar4;

-- test with numric type
select col_int, col_num1 from llvm_vecsort_table_02 order by col_int, col_num1;
select col_num3, col_int from llvm_vecsort_table_02 order by col_num3, col_int;
select col_num2, col_num4 from llvm_vecsort_table_02 order by col_num2, col_num4;
select col_num3, col_num5 from llvm_vecsort_table_02 order by col_num3, col_num5;
select col_num6 from llvm_vecsort_table_02 order by col_num6;
select col_num7, col_text from llvm_vecsort_table_02 order by col_num7, col_text;
select col_text, col_vchar from llvm_vecsort_table_02 order by col_text, col_vchar;
select col_num1, col_num7, col_text from llvm_vecsort_table_02 order by col_num1, col_num7, col_text;
select col_num2 from (select col_num2, col_num7 from llvm_vecsort_table_02 order by col_num7) order by col_num2;
select col_text, col_num3 from (select * from llvm_vecsort_table_02 order by col_num5, col_num6) order by col_text, col_num3;

-- test with desc order
select col_int1, col_bpchar2, col_bpchar4 from llvm_vecsort_table_01 order by col_int1, col_bpchar2, col_bpchar4 desc;
select col_bint, col_bpchar3, col_varchar3 from llvm_vecsort_table_01 order by col_bint, col_bpchar3 nulls first, col_varchar3 desc;
select col_int, col_num1, col_num3, col_num7 from llvm_vecsort_table_02 order by col_int, col_num3 desc, col_num7 desc;
select col_int, col_num2, col_num4, col_num6 from llvm_vecsort_table_02 order by col_num2 nulls first, col_num4 desc, col_num6;

----
--- test2 : test comparemulticolumns with limit node
----
select col_int1, col_bpchar1, col_varchar2 from llvm_vecsort_table_01 order by col_int1, col_bpchar1, col_varchar2 limit 5;
select col_int2, col_bpchar3, col_varchar3 from llvm_vecsort_table_01 order by col_int2, col_bpchar3, col_bpchar4, col_varchar4 limit 5;
select col_bint, col_bpchar4, col_varchar4 from llvm_vecsort_table_01 order by col_bint, col_bpchar4, col_varchar4 limit 5;
select col_bint, col_num2, col_num7 from llvm_vecsort_table_02 order by col_num2, col_num7 limit 5;
select col_bint, col_num2, col_num6 from llvm_vecsort_table_02 order by col_num2, col_num6 limit 5 offset 3;
select col_varchar1, col_varchar4 from (select * from llvm_vecsort_table_01 order by col_bpchar2, col_bpchar3) order by col_varchar1, col_varchar4 limit 5;
select col_text from (select col_text, col_num7 from llvm_vecsort_table_02 order by col_num7) order by col_text limit 5;

----
--- test3 : test sortagg with only group by rollup(...) case
----
explain (verbose on, costs off) select 'store' channel, col_int1, col_bint, sum(col_int2) from llvm_vecsort_table_01 group by rollup(channel, col_int1, col_bint) order by 1, 2, 3, 4;
select 'store' channel, col_int1, col_bint, sum(col_int2) from llvm_vecsort_table_01 group by rollup(channel, col_int1, col_bint) order by 1, 2, 3, 4;
select avg(col_int1), col_bpchar1, col_bpchar2 from llvm_vecsort_table_01 group by rollup(col_bpchar1, col_bpchar2) order by 1, 2, 3;
select avg(col_int1), col_bpchar3, col_bpchar4 from llvm_vecsort_table_01 group by rollup(col_bpchar3, col_bpchar4) order by 1, 2, 3;
select sum(col_num1), col_text, col_vchar from llvm_vecsort_table_02 group by rollup(col_text, col_vchar) order by 1, 2, 3;
select sum(col_num1), col_num2, col_vchar from llvm_vecsort_table_02 group by rollup(col_num2, col_vchar) order by 1, 2, 3;

set enable_hashagg = off;
explain (verbose on, costs off) select sum(col_int1), avg(col_bint), col_char1 from llvm_vecsort_table_01 group by col_char1 order by 1, 2, 3;
select sum(col_int1), avg(col_bint), col_char1 from llvm_vecsort_table_01 group by col_char1 order by 1, 2, 3;
select avg(col_num1), col_text from llvm_vecsort_table_02 group by col_text order by 1, 2;
reset enable_hashagg;

----
--- test4 : test mergejoin with sort node
----
set enable_nestloop = off;
set enable_hashjoin = off;
explain (verbose on, costs off) select count(*) from llvm_vecsort_table_01 A inner join llvm_vecsort_table_02 B on A.col_int1 = B.col_int;
select A.col_int1, B.col_int from llvm_vecsort_table_01 A inner join llvm_vecsort_table_02 B on A.col_int1 = B.col_int order by 1, 2;
select A.col_char1, B.L_LINESTATUS from llvm_vecsort_table_01 A inner join  llvm_vecsort_table_03 B on A.col_char1 = B.L_LINESTATUS order by 1, 2;
select count(*) from llvm_vecsort_table_01 A inner join llvm_vecsort_table_02 B on A.col_bpchar3 = B.col_vchar;
reset enable_nestloop;
reset enable_hashjoin;

----
--- test5 : test window func with sort operation
----
select col_int2, col_bint, col_varchar2, rank() over(order by col_bint, col_varchar2) from llvm_vecsort_table_01 order by 1, 2, 3;
select col_bpchar1, col_bpchar4, rank() over(order by col_bpchar1, col_bpchar2) from llvm_vecsort_table_01 order by 1, 2, 3;
select col_bint, col_num2, col_num6, row_number() over(partition by col_bint order by col_num6) from llvm_vecsort_table_02 order by 1, 2, 3;
select col_num1, col_num4, col_num5, col_num7, count(*) over (order by col_num4), sum(col_int) over (order by col_num5, col_num7) from llvm_vecsort_table_02 order by 1, 2, 3;

----
--- test6 : test direct on datanode
----

select col_int2, col_bint from llvm_vecsort_table_01 order by 1, 2 limit 2;
select col_bint, col_bpchar2 from llvm_vecsort_table_01 order by col_bint desc, col_bpchar2 nulls first limit 3;
select col_bint, col_num2 from llvm_vecsort_table_02 order by col_bint, col_num2 desc limit 4;
----
--- clean table and resource
----
drop schema llvm_vecsort_engine cascade;