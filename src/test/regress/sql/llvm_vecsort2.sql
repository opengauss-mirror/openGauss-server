/*
 * This file is used to test the function of sort & sortagg with LLVM Optimization
 */
----
--- Create Table and Insert Data
----
drop schema if exists llvm_vecsort_engine2 cascade;
create schema llvm_vecsort_engine2;
set current_schema = llvm_vecsort_engine2;
set codegen_cost_threshold=0;

create table llvm_vecsort_engine2.LLVM_VECSORT_TABLE_03
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

create table llvm_vecsort_engine2.LLVM_VECSORT_TABLE_04
(
    col_int	int
   ,col_bint	bigint
   ,col_num1	numeric
   ,col_num2	numeric
   ,col_num3	numeric(8,2)
   ,col_num4	numeric(20,2)
   ,col_vchar	varchar
   ,col_text	text
) with (orientation = column); 

copy llvm_vecsort_table_04 from stdin;
1	123	-0.12587459	987654384193888888888888	2.56	2563.254	bebestme	sheisbeautiful
1	256	0	569842.2564789556211452145236	3.65	584.257	bebestmeto	sheissobeautiful
1	2	2.458	587.254	658.25	3.6	bebestme	sheisbeautiful
2	254	1542	6589.36	12.25	254.6	bebestyou	bebestyou
2	254	65.24	-0.00000000000000000000000000012	12.24	58.65	bebestme	seeyoutomorrow
2	142	-2541.6587	254632541	2.25	6.36	bebestme	likeyou
2	65	-415.5477893125447583	698547452455662245587	2.36	12548254.36	bebestyou	seeyoutomorrow
2	6325487	15246.2548879666336	25.125000001	25.25	65.36	bebestyou	seeyoutomorrow
4	542	45269858796	41.25488896	6.36	12.25	justforyou	begreat
78	698	452.2547	0	1.25	5.25	justforyou	likeyou
-587	569874	652.00	0.01	5623.36	69.36	likemeforeverandgoneverseethetime	howaboutthiscse
0	-2541	0.012	23654.3698775244562	2.3	3.65	youreallyme	payformagic
45	8597	256987.00000000	25478963542.25644878632	1.25	6.36	youreallyme	playmagic
58	41	-10.012548	542.2558	6.36	25.21	justforyou	A
65	5879	0.000000000000000000000000002	102478.25874	78.21	69.36	justforyou	kattypary
96	14	-25.36	69365256348996666254.63	2.35	63.48	findanotherday	BB
75	598756324489	36.215	7.256369	2.0	14.21	Findanotherday	cC
78	2587365	1489636987	365215.25214	78.20	6.0	\N	Disble
95	0	2.01	5263.369874152	2.1	6.5	findanotherday	justforyou
-0	-0	0.0	0.0	5.2	4.6	likemeforeverandgoneverseethetime	likemeforeverandgoneverseethetime
-23	5	25.36	3.36	2.25	6.3	kindle	kindle
5	56	2.25	25.36587954893	2.1	3.65	justforyou	justforyou
12	963	1458752.365	63.36	2.12	3.6	behappy	heimifan
\.

analyze llvm_vecsort_table_03;
analyze llvm_vecsort_table_04;

----
--- test1 : test comparemulticolumns with different data type in sort node
----
-- test with numric type
select col_num1, col_num2 from llvm_vecsort_table_04 order by col_num1, col_num2, col_int;
select col_num1, col_vchar from llvm_vecsort_table_04 order by col_num1, col_vchar, col_num2;
select col_num2, col_text from llvm_vecsort_table_04 order by col_num2, col_text, col_bint;

----
--- test2 : test comparemulticolumns with limit node
----
select col_num1, col_num2, col_num3 from llvm_vecsort_table_04 order by col_num1, col_num3 limit 4 offset 5;
select col_num2, col_num4, col_text from llvm_vecsort_table_04 order by col_num2, col_num4 limit 10;

----
--- test3 : test sortagg with only group by rollup(...) case
----
select sum(col_num1), col_vchar, col_text from llvm_vecsort_table_04 group by rollup(col_vchar, col_text) order by 1, 2, 3;

----
--- test4 : test mergejoin with sort node
----
set enable_nestloop = off;
set enable_hashjoin = off;
select A.col_text, B.col_text, sum(A.col_num1) from llvm_vecsort_table_04 A inner join llvm_vecsort_table_04 B on A.col_text = B.col_text group by rollup(1, 2), 1, 2 order by 1, 2, 3;
reset enable_nestloop;
reset enable_hashjoin;

----
--- test5 : test window func with sort operation
----
select col_num1, col_num4, rank() over(order by col_num2) from llvm_vecsort_table_04 order by 1, 2, 3;

----
--- test6 : test comparemulticolumns with external sort
----
set work_mem=64;
set query_mem=0;
explain (analyze on, costs off, timing off) select * from llvm_vecsort_table_03 order by 1,2,3,4,5,6,7,8,9,10,14,15,16;
select * from llvm_vecsort_table_03 order by 1,2,3,4,5,6,7,8,9,10,14,15,16;
reset work_mem;
reset query_mem;

----
--- clean table and resource
----
drop schema llvm_vecsort_engine2 cascade;

----
---  case 7 : test llvm sort with lc_collate 'C'
----
create database llvmsortdb with lc_collate='C' template template0;
\c llvmsortdb

drop schema if exists llvm_vecsort_c_engine cascade;
create schema llvm_vecsort_c_engine;
set current_schema = llvm_vecsort_c_engine;
set codegen_cost_threshold=0;

create table llvm_vecsort_c_engine.LLVM_VECSORT_TABLE(
    col_int int,
    col_bint	bigint,
    col_bchar1	bpchar(3),
    col_bchar2	bpchar(9),
    col_bchar3	bpchar(18),
    col_bchar4	bpchar(24),
    col_vchar1	varchar(8),
    col_vchar2	varchar(11),
    col_vchar3	varchar(23),
    col_vchar4	varchar(40)
)with(orientation=column);

copy llvm_vecsort_table from stdin;
1	1	TUI	XJPUIO	tianjing	j00142589678	test	bottle	whatisgoodfortodaysfood	begreat
2	256	ZUI	ZUKK	beijing	bookisfriend	far	bottle	zooisforchildren	letusmakeupajokeforteacher
1	3	AMX	XJPUIO	shijiazhu	kehlssince1985	LK	outputlink	mergesort	hereissheandbeau
3	5	KIL	begood	kehlssince	be	funk	outputlink	mergesort	formulatedwithsqualanceandantioxidant
9	52	aMX	yesterday	TINK	likeherea	begreat	justforyou	mergesortinternal	formulatedwithsqualanceandantioxidant
7	25	nil	KUL	likehim	beagoodguy	how	\N	awayfromhome	whataboutwhat
-2	582	UIL	be	akindleissogood	Zo	ugg	ugg	ki	u
158	6	tea	beauty	while	ZooK	be	P	mergesortinternal	likehere
7	-21	AMX	KUL	\N	dayisdayisday	ZK	TINK	begreater	\N
90	34	AMX	AMX	Pig	\N	bottle	outputlink	mergesort	hereissheandbeau
90	-2134	P	teaandtea	howaboutthis	Z	C	LK	outputlink	hereisshe
0	24	lp	about	dataisgooddata	five	Zo	uuc	testa	farha
-2	25	oK	just	justforyou	begreat	bebestme	seeyou	seeyoutomorrow	seeyoutomorrow
1	2	Kij	\N	bebestmeto	bebestme	likeyou	a	sheisbeautiful	outputlink
15	-72	TMT	likehim	beagoodguy	justforfunand	how	\N	awayfromhome	whataboutwhat
7	-31	TH	beauty	beagoodguy	akindleissogood	Zo	zara	TINK	letusmakeupajokeforteacher
3	1	far	book	algorithm	analysis	\N	Rq	Request	input
7	14	Y	value	outpit	see	you	andI	zara	keep
8	32	\N	UIL	beagoodguy	AJ	uuc	testa	far	ABT
\.

analyze llvm_vecsort_table;

select col_bchar1, col_bchar3 from llvm_vecsort_table order by col_bchar1, col_bchar3;
select col_bchar2, col_vchar2 from llvm_vecsort_table order by col_bchar2, col_vchar2;
select col_vchar1, col_vchar4 from llvm_vecsort_table order by col_vchar1, col_vchar4;
select col_bchar2, col_bchar4 from llvm_vecsort_table order by col_bchar2, col_bchar4 limit 5;
select col_vchar3, col_bchar1 from llvm_vecsort_table order by col_vchar3, col_bchar1 desc limit 5;

----
--- clean table and resource
----
drop schema llvm_vecsort_c_engine cascade;
\c postgres
drop database llvmsortdb;
