/* 
 * This file is used to test the function of vecexpression.cpp with LLVM Optimization
 * It is only an auxiliary file to vexecpression.sql to cover the basic functionality 
 * about expression.
 */
/********************************
    T_Var,
    T_Const,
    T_Case,
    T_OpExpr,
    T_ArrayExpr,
    T_BoolenTest,
    T_NullTest,
    T_NULLIF,
    T_BOOL (AND/OR/NOT)
********************************/
----
--- Create Table and Insert Data
----
drop schema if exists llvm_vecexpr_engine2 cascade ;
create schema llvm_vecexpr_engine2;
set current_schema = llvm_vecexpr_engine2;
set codegen_cost_threshold=0;

CREATE TABLE llvm_vecexpr_engine2.LLVM_VECEXPR_TABLE_02(
    col_int	int,
    col_bigint	bigint,
    col_float	float4,
    col_float8	float8,
    col_char	char(10),
    col_bpchar	bpchar,
    col_varchar	varchar,
    col_text1	text,
    col_text2   text,
    col_num1	numeric(10,2),
    col_num2	numeric,
    col_date	date,
    col_time    time
)with(orientation=column);

COPY LLVM_VECEXPR_TABLE_02(col_int, col_bigint, col_float, col_float8, col_char, col_bpchar, col_varchar, col_text1, col_text2, col_num1, col_num2, col_date, col_time) FROM stdin;
1	256	3.1	3.25	beijing	AaaA	newcode	myword	myword1	3.25	3.6547	2011-11-01 00:00:00	2017-09-09 19:45:37
3	12400	2.6	3.64755	hebei	BaaB	knife	sea	car	1.62	3.64	2017-10-09 19:45:37	2017-10-09 21:45:37
5	25685	1.0	25	anhui	CccC	computer	game	game2	7	3.65	2012-11-02 00:00:00	2018-04-09 19:45:37
-16	1345971420	3.2	2.15	hubei	AaaA	phone	pen	computer	4.24	6.36	2012-11-04 00:00:00	2012-11-02 00:03:10
64	-2566	1.25	2.7	jilin	DddD	girl	flower	window	65	69.36	2012-11-03 00:00:00	2011-12-09 19:45:37
\N	256	3.1	4.25	anhui	BbbB	knife	phone	light	78.12	2.35684156	2017-10-09 19:45:37	1984-2-6 01:00:30
81	\N	4.8	3.65	luxi	EeeE	girl	sea	crow	145	56	2018-01-09 19:45:37	2018-01-09 19:45:37
25	365487	\N	3.12	lufei	EeeE	call	you	them	7.12	6.36848	2018-05-09 19:45:37	2018-05-09 19:45:37
36	5879	10.15	\N	lefei	GggG	call	you	them	2.5	2.5648	2015-02-26 02:15:01	1984-2-6 02:15:01
27	256	4.25	63.27	\N	FffF	code	throw	away	2.1	25.65	2018-03-09 19:45:37	2017-09-09 14:20:00
9	-128	-2.4	56.123	jiangsu	\N	greate	book	boy	7	-1.23	2017-12-09 19:45:37	 2012-11-02 14:20:25
1001	78956	1.25	2.568	hangzhou	CccC	\N	away	they	6.36	58.254	2017-10-09 19:45:37	1984-2-6 01:00:30
2005	12400	12.24	2.7	hangzhou	AaaA	flower	\N	car	12546	3.2546	2017-09-09 19:45:37	2012-11-02 00:03:10
8	5879	\N	1.36	luxi	DeeD	walet	wall	\N	2.58	3.54789	2000-01-01	2000-01-01 01:01:01
652	25489	8.88	1.365	hebei	god	piece	sugar	pow	\N	2.1	2012-11-02 00:00:00	2012-11-02 00:00:00
417	2	9.19	0.256	jiangxi	xizang	walet	bottle	water	11.50	-1.01256	\N	1984-2-6 01:00:30
18	65	-0.125	78.96	henan	PooP	line	black	redline	24	3.1415926	2000-01-01	\N
\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N
700	58964785	3.25	1.458	\N	qingdao	\N	2897	dog	9.36	\N	\N	2017-10-09 20:45:37
505	1	3.24	\N	\N	BbbB	\N	myword	pen	147	875	2000-01-01 01:01:01	2000-01-01 01:01:01
3	12400	2.6	3.64755	 	  	   	 	  	1.62	3.64	2017-10-09 19:45:37	2017-10-09 21:45:37
\.

CREATE TABLE llvm_vecexpr_engine2.LLVM_VECEXPR_TABLE_03(
	col_sint	int2,
    col_int	int,
    col_bigint	bigint,
    col_char	char(10),
    col_bpchar	bpchar,
    col_varchar	varchar,
    col_text	text,
    col_date	date,
    col_time    timestamp
)with(orientation=column);
create index llvm_index_01 on llvm_vecexpr_table_03(col_int);

COPY LLVM_VECEXPR_TABLE_03(col_sint, col_int, col_bigint, col_char, col_bpchar, col_varchar, col_text, col_date, col_time) FROM stdin;
1	1	256	11	111	1111	123456	2000-01-01 01:01:01	2000-01-01 01:01:01
2	2	128	24	75698	56789	12345	2017-09-09 19:45:37	2012-11-02 00:03:10
3	30	2899	11111	1111	12345	123456	2015-02-26	2012-12-02 02:15:01
4	417	258	245	111	1111	123456	2018-05-09 19:45:37	1984-2-6 01:00:30
5	25	365487	111	1111	12345	123456	2015-02-26	1984-2-6 01:00:30
6	27	6987	11	111	24568	123456	2018-05-09	2018-03-07 19:45:37
7	18	1348971452	24	2563	2222	56789	2000-01-01	2000-01-01 01:01:01
8	\N	258	\N	1258	25879	25689	2014-05-12	2004-2-6 07:30:30
\N	569	254879963	11	\N	547	36589	2016-01-20	2012-11-02 00:00:00
8	4	\N	11	111	\N	56897	2013-05-08	2012-11-02 00:03:10
\N	56	58964	25	365487	5879	\N	2018-03-07	1999-2-6 01:00:30
\N	694	2	364	56897	\N	\N	2018-11-05	2011-2-6 01:00:30
-1	-30	-3658	5879	11	25879	\N	2018-03-07	2011-2-6 01:00:30
-2	-15	-24	3698	58967	698745	5879	2012-11-02	2012-11-02 00:00:00
-3	2147483645	258	3698	36587	125478	111	2015-02-2	2000-01-01 01:01:01
12	-48	-9223372036854775802	258	36987	12587	2547	2014-03-12	2012-11-02 01:00:00
3	-2	9223372036854775801	3689	256987	36547	14587	2016-01-20	2012-11-02 07:00:00
-6	-2147483640	-1587452	1112	1115	12548	36589	\N	1999-2-6 01:00:30
-6	-2147483640	-1587452	1112	1115	12548	36589	2014-03-12	\N
\.

analyze llvm_vecexpr_table_02;
analyze llvm_vecexpr_table_03;
----
--- case 1 : arithmetic operation
----
select * from llvm_vecexpr_table_03 where (col_bigint + 4) * 2 -3 > 5 order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_03 where col_bigint/2 > 2 order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_03 where col_int + 4::bigint > 5 order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_03 where col_int * 2::bigint > 7 order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_03 where col_int - 2::bigint > 3 order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_03 where col_int / 2::bigint > 5 order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_03 where ((col_bigint + 1::bigint) * 2::bigint) -1::bigint > 3 order by 1, 2, 3;
select * from llvm_vecexpr_table_03 where col_bigint / 2::bigint > 2 order by 1, 2, 3, 4, 5;

----
--- case 2 : basic comparison operation
----
select * from llvm_vecexpr_table_03 where col_time > '2013-05-08' order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_03 where col_time >= '2013-05-08' order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_03 where col_time < '2013-05-08' order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_03 where col_time <= '2000-01-01 01:01:01' order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_03 where col_time = '2000-01-01 01:01:01' order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_03 where col_time != '2000-01-01 01:01:01' order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_03 where col_varchar > '111' order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_03 where col_text > '12345' order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_03 where col_text < '23456' order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_03 where '12' < substr(col_text,1,2) order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_03 where substr(btrim(substr(btrim(col_text), 1, 3)),1,1) > '1' order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_03 where substr(col_varchar, 1, 2) = '25' order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_03 where substr(substr(col_text, 1, 3), 1, 2) = '12' order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_03 where substr(substr(col_varchar, 1, 3), 1, 2) = '12' order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_03 where substr(btrim(substr(col_bpchar, 1, 3)), 1, 2) = '12' order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_03 where col_sint = 3::smallint order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_03 where col_sint != 3::smallint order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_03 where col_sint < 8::smallint order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_03 where col_sint <= 8::smallint order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_03 where col_sint > 6::smallint order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_03 where col_sint >= 6::smallint order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_03 where col_int + 2147483645 > 5 order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_03 where col_int - 2147483645 > 0 order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_03 where col_int/0 > 1 order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_03 where col_int = 0::bigint order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_03 where col_int != 0::bigint order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_03 where col_int < 0::bigint order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_03 where col_int <= 0::bigint order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_03 where col_int > 0::bigint order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_03 where col_int >= 0::bigint order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_03 where col_bigint = -1::int order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_03 where col_bigint/0 > 2 order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_03 where col_bigint != -1::int order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_03 where col_bigint < -1::int order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_03 where col_bigint <= -1::int order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_03 where col_bigint > -1::int order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_03 where col_bigint >= -1::int order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_03 where col_int >= -1::smallint order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_03 where col_sint = 3::int order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_03 where col_sint != 3::int order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_03 where col_sint < 8::int order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_03 where col_sint <= 8::int order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_03 where col_sint > 6::int order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_03 where col_sint >= 6::int order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_03 where col_int = 3::smallint order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_03 where col_int != 3::smallint order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_03 where col_int < 8::smallint order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_03 where col_int <= 8::smallint order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_03 where col_int > 6::smallint order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_03 where col_int >= 6::smallint order by 1, 2, 3, 4, 5;
---boolean test with some other types
select * from llvm_vecexpr_table_03 where col_text is true order by 1, 2, 3;
select * from llvm_vecexpr_table_03 where col_sint is true order by 1, 2, 3;
select * from llvm_vecexpr_table_03 where col_bigint is false order by 1, 2, 3;

--comparison with index scan
set enable_seqscan=off;
set enable_bitmapscan=off;
explain (verbose on, costs off, analyze on) select * from llvm_vecexpr_table_03 where col_int > 4;

explain (analyze on, costs off, timing off) select * from llvm_vecexpr_table_03 where col_int > 4;
explain (analyze on, costs off, timing off, format json) select * from llvm_vecexpr_table_03 where col_int > 4;
explain (analyze on, costs off, timing off, format xml) select * from llvm_vecexpr_table_03 where col_int > 4;
explain (analyze on, costs off, timing off, format yaml) select * from llvm_vecexpr_table_03 where col_int > 4;
reset enable_seqscan;

----
--- case 3 : Case Expr - int4eq, int8eq, float4eq, float8eq, texteq, bpchareq, 
---          dateeq, time, timetzeq, timestampeq, timestamptzeq
----													
select * from llvm_vecexpr_table_03 where case col_bigint when -2 then 12 
					when -3658 then 24 else 36 end = 36 order by 1, 2, 3, 4, 5;	
														
select * from llvm_vecexpr_table_03 where case col_bigint when 12548632547785 then 12 
					when -9223372036854775802 then 24 else 36 end = 24 order by 1, 2, 3, 4, 5;

select * from llvm_vecexpr_table_03 where case col_sint when -2 then 24 when 8 then 36 else 48 end = 36 order by 1, 2, 3, 4, 5;

select * from llvm_vecexpr_table_03 where case col_int when -2::int2 then 24 when 30::int2 then 36 else 48 end = 36 order by 1, 2, 3, 4, 5;
														
select * from llvm_vecexpr_table_03 where case when col_char = '11' then '2000-01-01 01:01:01' 
									when col_varchar = '25879' then '2011-12-09 19:45:37' 
									else '2018-03-07 02:02:02' end = '2011-12-09 19:45:37' order by 1, 2, 3, 4, 5;

select * from llvm_vecexpr_table_03 where case col_int when 12::bigint then 4 when 30::bigint then 12 else 1254468447542 end = 12 order by 1, 2, 3, 4, 5; 

select * from llvm_vecexpr_table_03 where col_sint in (1::smallint, 2::smallint, 3::smallint) order by 1, 2, 3, 4, 5;

select * from llvm_vecexpr_table_03 where col_varchar = any(array['1111', '2222', '56789', NULL]) and col_text > '1112' order by 1, 2, 3, 4, 5;

select * from llvm_vecexpr_table_03 where col_sint in (1::smallint, 2::smallint, 3::smallint) order by 1, 2, 3, 4, 5;

----
--- case 6 : Nullif
----
select * from llvm_vecexpr_table_03 where nullif(col_sint, 1::int2) is NULL order by 1, 2, 3;
select * from llvm_vecexpr_table_03 where nullif(col_sint, 4) is NULL order by 1, 2, 3;
select * from llvm_vecexpr_table_03 where col_varchar not like '%2%5%' order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_03 where col_text not like '12%5%' order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_03 where col_text not like '1%6' order by 1, 2, 3, 4, 5;

----
--- case 9 : multi IR case
----
set enable_nestloop = off;
set enable_mergejoin = off;
select * from llvm_vecexpr_table_02 A join llvm_vecexpr_table_03 B on A.col_char = B.col_char and A.col_int = B.col_int where A.col_int < 10 order by 1, 2, 3, 4, 5;

----
--- clean table and resource
----
drop schema if exists llvm_vecexpr_engine2 cascade ;