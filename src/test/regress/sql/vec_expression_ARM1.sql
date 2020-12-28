/*
 * This file is used to test the function of vecexpression.cpp --- test（1）
 */
/*******************************
Expression Type:
	T_Var,
	T_Const,
	T_Param,
	T_Aggref,
	T_WindowFunc,
	T_ArrayRef,
	T_FuncExpr,
	T_NamedArgExpr,
	T_OpExpr,
	T_DistinctExpr,
	T_NullIfExpr,
	T_ScalarArrayOpExpr,
	T_BoolExpr,
	T_SubLink,
	T_SubPlan,
	T_AlternativeSubPlan,
	T_FieldSelect,
	T_FieldStore,
	T_RelabelType,
	T_CoerceViaIO,
	T_ArrayCoerceExpr,
	T_ConvertRowtypeExpr,
	T_CollateExpr,
	T_CaseExpr,
	T_CaseWhen,
	T_CaseTestExpr,
	T_ArrayExpr,
	T_RowExpr,
	T_RowCompareExpr,
	T_CoalesceExpr,
	T_MinMaxExpr,
	T_XmlExpr,
	T_NullTest,
	T_BooleanTest
	
Using Type:
	qual
	targetlist
*********************************/

----
--- Create Table and Insert Data
----
create schema vector_expression_engine_first;
set current_schema=vector_expression_engine_first;

create table vector_expression_engine_first.VECTOR_EXPR_TABLE_01
(
   a	bool
  ,b	bool
  ,c	int
)with (orientation=column);

COPY VECTOR_EXPR_TABLE_01(a, b, c) FROM stdin;
true	false	1
true	true	1
true	\N	1
false	true	1
false	false	1
false	\N	1
\N	true	1
\N	false	1
\N	\N	1
\.

create table vector_expression_engine_first.VECTOR_EXPR_TABLE_02
(
   col_int	int
  ,col_int2	int
  ,col_char	char(20)
  ,col_varchar	varchar(30)
  ,col_date	date
  ,col_num	numeric(10,2)
  ,col_num2	numeric(10,4)
  ,col_float	float4
  ,col_float2	float8
)with (orientation=column);

COPY VECTOR_EXPR_TABLE_02(col_int, col_int2, col_char, col_varchar, col_date, col_num, col_num2, col_float, col_float2) FROM stdin;
0	1935401906	aabccd	aabccd	2011-11-01 00:00:00	1.20	10.0000	\N	1.1
1	1345971420	abccd	abccd	2012-11-02 00:00:00	11.18	1.1181	55.555	55.555
2	656473370	aabccd	aabccd	2011-11-01 00:00:00	1.20	10.0000	1.1	1.1
2	1269710788	abccd	abccd	2012-11-02 00:00:00	11.18	1.1181	55.555	55.555
2	1156776517	aabccd	aabccd	2011-11-01 00:00:00	1.20	10.0000	1.1	1.1
2	1289013296	abccd	abccd	2012-11-02 00:00:00	11.18	1.1181	55.555	55.555
2	1415564928	aabccd	aabccd	\N	1.20	10.0000	1.1	1.1
8	1345971420	abccd	abccd	2012-11-02 00:00:00	11.18	1.1181	55.555	55.555
8	435456494	aabccd	aabccd	2011-11-01 00:00:00	1.20	10.0000	1.1	1.1
8	66302641	\N	abccd	2012-11-02 00:00:00	11.18	1.1181	55.555	55.555
8	915852158	aabccd	aabccd	2011-11-01 00:00:00	1.20	10.0000	1.1	1.1
8	1345971420	abccd	abccd	2012-11-02 00:00:00	11.18	1.1181	55.555	55.555
8	961711400	aabccd	aabccd	2011-11-01 00:00:00	1.20	10.0000	1.1	1.1
8	74070078	abccd	\N	2012-11-02 00:00:00	11.18	1.1181	55.555	55.555
87	656473370	aabccd	aabccd	2011-11-01 00:00:00	1.20	10.0000	1.1	1.1
87	843938989	abccd	abccd	2012-11-02 00:00:00	11.18	1.1181	55.555	55.555
87	189351248	abbccd	abbccd	2012-11-02 00:00:00	1.62	6.2110	2.2	2.2
87	1388679963	abbccd	abbccd	2012-11-02 00:00:00	1.62	6.2110	2.2	2.2
87	556726251	abbccd	abbccd	2012-11-02 00:00:00	1.62	6.2110	2.2	2.2
87	634715959	abbccd	abbccd	2012-11-02 00:00:00	1.62	6.2110	2.2	2.2
87	1489080225	abbccd	abbccd	2012-11-02 00:00:00	1.62	6.2110	2.2	2.2
87	649132105	abbccd	abbccd	2012-11-02 00:00:00	1.62	6.2110	2.2	2.2
87	886008616	abbccd	abbccd	2012-11-02 00:00:00	1.62	6.2110	2.2	2.2
123	1935401906	abbccd	\N	2012-11-02 00:00:00	1.62	6.2110	2.2	2.2
123	1935401906	aabbcd	aabbcd	2012-11-03 00:00:00	29.00	24.1100	3.33	3.33
123	846480997	aabbcd	aabbcd	2012-11-03 00:00:00	29.00	24.1100	3.33	3.33
123	1102020422	aabbcd	aabbcd	2012-11-03 00:00:00	29.00	24.1100	3.33	3.33
123	1533442662	aabbcd	aabbcd 	2012-11-03 00:00:00	\N	24.1100	3.33	3.33
123	1935401906	aabbcd	aabbcd	2012-11-03 00:00:00	29.00	24.1100	3.33	3.33
123	656473370	\N	aabbcd	2012-11-03 00:00:00	29.00	24.1100	3.33	3.33
123	539384293	aabbcd	aabbcd	2012-11-03 00:00:00	29.00	24.1100	3.33	3.33
\N	\N	aabbcd	aabbcd	2012-11-03 00:00:00	29.00	24.1100	\N	3.33
\N	\N	abbccd	abbccd	2012-12-01 00:00:00	221.70	11.1700	13822.2	13822.237
\N	\N	abbccd	abbccd	2012-12-01 00:00:00	221.70	11.1700	13822.2	13822.237
\N	\N	abbccd	abbccd	2012-12-01 00:00:00	221.70	11.1700	\N	13822.237
\N	\N	abbccd	abbccd	2012-12-01 00:00:00	221.70	11.1700	13822.2	13822.237
\N	\N	abbccd	abbccd	2012-12-01 00:00:00	221.70	11.1700	13822.2	13822.237
\N	\N	abbccd	abbccd	2012-12-01 00:00:00	221.70	11.1700	13822.2	13822.237
\N	\N	abbccd	abbccd	2012-12-01 00:00:00	221.70	11.1700	13822.2	13822.237
\N	\N	abbccd	abbccd	2012-12-01 00:00:00	221.70	11.1700	13822.2	13822.237
\N	\N	aaccccd	aaccccd	2012-12-02 00:00:00	3.78	37.3737	2.58	\N
\N	\N	aaccccd	aaccccd	2012-12-02 00:00:00	\N	37.3737	2.58	2.58
\N	\N	aaccccd	aaccccd	2012-12-02 00:00:00	3.78	37.3737	2.58	2.58
\N	\N	aaccccd	aaccccd	2012-12-02 00:00:00	3.78	37.3737	2.58	2.58
\N	\N	aaccccd	aaccccd	2012-12-02 00:00:00	3.78	37.3737	2.58	\N
\N	\N	aaccccd	aaccccd	\N	3.78	37.3737	2.58	2.58
\N	\N	aaccccd	aaccccd	2012-12-02 00:00:00	3.78	37.3737	2.58	2.58
\N	\N	aaccccd	aaccccd	2012-12-02 00:00:00	3.78	37.3737	2.58	2.58
\N	\N	abbccd	abbccd	2013-11-12 00:00:00	87.10	1.8700	3.67233	3.672335
\N	\N	abbccd	abbccd	2013-11-12 00:00:00	87.10	1.8700	3.67233	3.672335
\N	\N	abbccd	abbccd	2013-11-12 00:00:00	87.10	1.8700	3.67233	3.672335
\N	\N	abbccd	abbccd	2013-11-12 00:00:00	87.10	1.8700	3.67233	3.672335
\N	\N	abbccd	abbccd	2013-11-12 00:00:00	87.10	1.8700	3.67233	3.672335
\N	\N	abbccd	abbccd	2013-11-12 00:00:00	87.10	1.8700	3.67233	3.672335
\N	\N	abbccd	abbccd	2013-11-12 00:00:00	87.10	1.8700	3.67233	3.672335
\N	\N	abbccd	abbccd	2013-11-12 00:00:00	87.10	1.8700	3.67233	3.672335
\N	\N	acbccd	acbccd	2012-11-01 00:00:00	5.10	131.1100	44.4	44.4
\N	\N	acbccd	acbccd	2012-11-01 00:00:00	5.10	131.1100	44.4	44.4
\N	\N	acbccd	acbccd	2012-11-01 00:00:00	5.10	131.1100	44.4	44.4
\N	\N	acbccd	acbccd	2012-11-01 00:00:00	5.10	131.1100	44.4	44.4
\N	\N	acbccd	acbccd	2012-11-01 00:00:00	5.10	131.1100	44.4	44.4
\N	\N	acbccd	acbccd	2012-11-01 00:00:00	5.10	131.1100	44.4	44.4
\N	\N	acbccd	acbccd	2012-11-01 00:00:00	5.10	131.1100	44.4	44.4
\N	\N	acbccd	acbccd	2012-11-01 00:00:00	5.10	131.1100	44.4	44.4
\N	\N	aaccccd	aaccccd	2012-12-02 00:00:00	3.78	37.3737	2.58	2.58
\N	\N	abbccd	abbccd	2013-11-12 00:00:00	87.10	\N	3.67233	3.672335
\N	\N	aaccccd	aaccccd	2012-12-02 00:00:00	3.78	37.3737	2.58	2.58
\N	\N	abbccd	abbccd	2013-11-12 00:00:00	87.10	1.8700	3.67233	3.672335
\N	\N	aaccccd	aaccccd	2012-12-02 00:00:00	3.78	37.3737	2.58	2.58
\N	\N	abbccd	abbccd	2013-11-12 00:00:00	87.10	1.8700	3.67233	3.672335
\N	\N	aaccccd	aaccccd	2012-12-02 00:00:00	3.78	37.3737	2.58	2.58
\N	\N	abbccd	abbccd	2013-11-12 00:00:00	87.10	1.8700	3.67233	3.672335
\N	\N	aaccccd	aaccccd	2012-12-02 00:00:00	3.78	37.3737	2.58	2.58
\N	\N	abbccd	abbccd	2013-11-12 00:00:00	87.10	1.8700	3.67233	3.672335
\N	\N	aaccccd	aaccccd	2012-12-02 00:00:00	3.78	37.3737	2.58	2.58
\N	\N	abbccd	abbccd	2013-11-12 00:00:00	87.10	1.8700	3.67233	3.672335
\N	\N	aaccccd	aaccccd	2012-12-02 00:00:00	3.78	37.3737	\N	2.58
\N	\N	abbccd	abbccd	2013-11-12 00:00:00	87.10	1.8700	3.67233	3.672335
\N	\N	aaccccd	aaccccd	\N	3.78	37.3737	2.58	2.58
\N	\N	abbccd	abbccd	2013-11-12 00:00:00	87.10	1.8700	3.67233	3.672335
\.

CREATE TABLE vector_expression_engine_first.VECTOR_EXPR_TABLE_03(
    a      int,
    b      int,
    c      int 
) WITH (orientation=column) distribute by hash (a);

COPY VECTOR_EXPR_TABLE_03(a, b, c) FROM stdin;
1	1	1
1	1	2
1	1	3
1	1	4
1	2	1
1	2	2
1	2	3
2	1	1
2	1	2
2	1	3
2	1	4
2	2	1
2	2	2
2	2	3
2	2	4
2	1	3
2	3	2
2	3	3
2	3	4
3	1	1
3	1	2
3	1	3
3	1	4
3	2	3
3	2	1
4	0	0
\.

CREATE TABLE vector_expression_engine_first.VECTOR_EXPR_TABLE_04
(
   a varchar
  ,b char(10)
  ,c varchar(10)
  ,d text
) with(orientation=column);

COPY VECTOR_EXPR_TABLE_04(a, b, c, d) FROM stdin;
abc	\N	\N	\N
\N	1	2.0	\N
\N	\N	2.0	\N
\N	\N	\N	\N
\N	\N	\N	def
\N	2	\N	\N
\.

CREATE TABLE vector_expression_engine_first.VECTOR_EXPR_TABLE_05
(
   a bool
  ,b int
  ,c bool
) with (orientation=column);

COPY VECTOR_EXPR_TABLE_05(a, b, c) FROM stdin;
1	1	1
1	1	0
1	0	1
1	0	0
0	1	1
0	1	0
0	0	1
0	0	0
\N	0	0
1	\N	0
\.

CREATE TABLE vector_expression_engine_first.VECTOR_EXPR_TABLE_06
(
   a varchar
  ,b char(10)
  ,c text
) with(orientation=column);

COPY VECTOR_EXPR_TABLE_06(a, b, c) FROM stdin;
abc	abb	\N
\N	abb	abc
abcdefgh	\N	abcdefgg
abc	\N	aabcdefg
\N	\N	\N
\.

CREATE TABLE vector_expression_engine_first.VECTOR_EXPR_TABLE_07
(
   col_num	numeric(5, 0)
  ,col_int	int
  ,col_timestamptz	timestamptz
  ,col_varchar	varchar
  ,col_char	char(2)
  ,col_interval	interval
  ,col_timetz	timetz
  ,col_tinterval	tinterval
) with(orientation=column);

COPY VECTOR_EXPR_TABLE_07(col_num, col_int, col_timestamptz, col_varchar, col_char, col_interval, col_timetz, col_tinterval) FROM stdin;
123	5	2017-09-09 19:45:37	2017-09-09 19:45:37	a	2 day 13:34:56	1984-2-6 01:00:30+8	["Sep 4, 1983 23:59:12" "Oct 4, 1983 23:59:12"]
234	6	2017-10-09 19:45:37	2017-10-09 19:45:37	c	1 day 18:34:56	1986-2-6 03:00:30+8	["May 10, 1947 23:59:12" "Jan 14, 1973 03:14:21"]
345	7	2017-11-09 19:45:37	2017-11-09 19:45:37	d	1 day 13:34:56	1987-2-6 08:00:30+8	["epoch" "Mon May 1 00:30:30 1995"]
456	8	2017-12-09 19:45:37	2017-12-09 19:45:37	h	18 day 14:34:56	1989-2-6 06:00:30+8	["Feb 15 1990 12:15:03" "2001-09-23 11:12:13"]
567	9	2018-01-09 19:45:37	2018-01-09 19:45:37	m	18 day 15:34:56	1990-2-6 12:00:30+8	\N
678	10	2018-02-09 19:45:37	2018-02-09 19:45:37	\N	7 day 16:34:56	2002-2-6 00:00:30+8	["-infinity" "infinity"]
789	11	2018-03-09 19:45:37	2018-03-09 19:45:37	g	22 day 13:34:56	1984-2-6 00:00:30+8	["Feb 10, 1947 23:59:12" "Jan 14, 1973 03:14:21"]
147	12	2018-04-09 19:45:37	2018-04-09 19:45:37	l	\N	1984-2-7 00:00:30+8	["Feb 10, 1947 23:59:12" "Jan 14, 1973 03:14:21"]
369	13	2018-05-09 19:45:37	2018-05-09 19:45:37	a	21 day 13:34:56	\N	["Feb 10, 1947 23:59:12" "Jan 14, 1973 03:14:21"]
\.

CREATE TABLE vector_expression_engine_first.VECTOR_EXPR_TABLE_08
(
   col_num	numeric(3,0)
  ,col_int	int
)with(orientation=column);

COPY VECTOR_EXPR_TABLE_08(col_num, col_int) FROM stdin;
1	1
\.

analyze vector_expr_table_01;
analyze vector_expr_table_02;
analyze vector_expr_table_03;
analyze vector_expr_table_04;
analyze vector_expr_table_05;
analyze vector_expr_table_06;
analyze vector_expr_table_07;
analyze vector_expr_table_08;

select * from VECTOR_EXPR_TABLE_03 where (CASE
                         WHEN (SELECT distinct -5
                                 FROM VECTOR_EXPR_TABLE_04
                                ) NOT LIKE '%C%'   THEN
                          VECTOR_EXPR_TABLE_03.a else NULL end) = 0;
----
--- case 1: AND OR NOT
----
explain (verbose on, costs off) select a, b, a and b, a or b, not a from vector_expr_table_01;
select a, b, a and b, a or b, not a from vector_expr_table_01 order by 1, 2, 3, 4, 5;

--NULL Test
explain (verbose on, costs off) select * from vector_expr_table_01 where a is NULL order by 1, 2;
select * from vector_expr_table_01 where a is NULL order by 1, 2;

select * from vector_expr_table_01 where a is not NULL order by 1, 2;

select a from vector_expr_table_01 where a is NULL order by 1;

select a from vector_expr_table_01 where a is not NULL order by 1;

select * from vector_expr_table_01 where b is not NULL order by 1, 2;

select * from vector_expr_table_01 where a is NULL and b is not NULL order by 1, 2;

select * from vector_expr_table_01 where a is not NULL and b is NULL order by 1, 2;

select * from vector_expr_table_01 where a is not NULL and b is not NULL order by 1, 2;

select a is not NULL, a from vector_expr_table_01 order by 1, 2;

select a is NULL, a from vector_expr_table_01 order by 1, 2;

select a is not NULL, a from vector_expr_table_01 where a is not NULL order by 1, 2;

select a is NULL, a from vector_expr_table_01 where a is NULL order by 1, 2;

--Operation
explain (verbose on, costs off) select * from vector_expr_table_02 where col_int = 1;

select * from vector_expr_table_02 where col_int = 1;

select col_int, col_int2, col_int + col_int2 from vector_expr_table_02 where col_int = 1;

--Date
select col_date, sum(1) from vector_expr_table_02 where col_date between date '2012-11-02'  and date '2012-12-20' group by col_date order by 1, 2; 
select A.col_date, sum(1) s from vector_expr_table_02 A where extract(year from A.col_date) >= 2012 group by A.col_date order by A.col_date, s;

explain (verbose on, costs off) select A.col_date, extract(year from B.col_date) y from vector_expr_table_02 A join vector_expr_table_02 B on A.col_char=B.col_char group by A.col_date,extract(year from B.col_date) order by 1, y;
select A.col_date, extract(year from B.col_date) y from vector_expr_table_02 A join vector_expr_table_02 B on A.col_char=B.col_char group by A.col_date,extract(year from B.col_date) order by 1, y;

explain (verbose on, costs off) select A.col_date, substring(B.col_varchar, 1, 2) y from vector_expr_table_02 A join vector_expr_table_02 B on A.col_char = B.col_char group by A.col_date,substring(B.col_varchar, 1, 2) order by A.col_date, y;
select A.col_date, substring(B.col_varchar, 1, 2) y from vector_expr_table_02 A join vector_expr_table_02 B on A.col_char = B.col_char group by A.col_date,substring(B.col_varchar, 1, 2) order by A.col_date, y;

select A.col_date, sum(1) s from vector_expr_table_02 A where abs(-extract(year from A.col_date)) >= 2012 group by A.col_date order by A.col_date, s;

-- String ops does not require special plan, FIXME: IN-list is still not right) 
select col_int, sum(1) from vector_expr_table_02 where col_char like '%cc%' group by col_int order by col_int; 
select col_int, sum(1) from vector_expr_table_02 where col_varchar like '%cc%' group by col_int order by col_int;
select col_int, sum(1) from vector_expr_table_02 where col_char like 'ab%' group by col_int order by col_int;
select col_int, sum(1) from vector_expr_table_02 where col_varchar like 'ab%' group by col_int order by col_int;
select col_char, sum(1) from vector_expr_table_02 where col_char not like '%d' group by col_char order by col_char;
select col_varchar, sum(1) from vector_expr_table_02 where col_varchar not like '%d' group by col_varchar order by col_varchar;
select col_int, sum(1) from vector_expr_table_02 where col_char = 'aabccd' group by col_int order by col_int; 
select col_int, sum(1) from vector_expr_table_02 where col_varchar = 'aabccd' group by col_int order by col_int; 
select col_int, sum(1) from vector_expr_table_02 where col_char <> 'aabccd' group by col_int order by col_int; 
select col_int, sum(1) from vector_expr_table_02 where col_varchar <> 'aabccd' group by col_int order by col_int; 
select col_int, sum(1) from vector_expr_table_02 where substring(col_char from 1 for 2) <> 'aa' group by col_int order by col_int;
select col_int, sum(1) from vector_expr_table_02 where substring(col_char from 1 for 2) <> 'aa' group by col_int order by col_int;
select col_int, sum(1) from vector_expr_table_02 where substring(col_char, 1, 2) <> 'aa' group by col_int order by col_int;
select col_int, 'OkThisSoundsGood' from vector_expr_table_02 where substring(col_char from 1 for 2) <> 'aa' order by col_int;

--Float, Integer
select min(col_int),min(col_int2),min(col_char),min(col_varchar),min(col_date),min(col_num),min(col_num2),min(col_float),min(col_float2) from vector_expr_table_02;
select max(col_int),max(col_int2),max(col_char),max(col_varchar),max(col_date),max(col_num),max(col_num2),max(col_float),max(col_float2) from vector_expr_table_02;
select count(col_int),count(col_int2),count(col_char),count(col_varchar),count(col_date),count(col_num),count(col_num2),count(col_float),count(col_float2) from vector_expr_table_02;
select sum(col_int),sum(col_int2),sum(col_num),sum(col_num2),sum(col_float),sum(col_float2) from vector_expr_table_02;
select col_int,sum(col_int) from vector_expr_table_02 group by col_int order by col_int;
select col_int2,sum(col_int2) from vector_expr_table_02 group by col_int2 order by col_int2;
select col_num,sum(col_num) from vector_expr_table_02 group by col_num order by col_num;
select col_num2,sum(col_num2) from vector_expr_table_02 group by col_num2 order by col_num2;
select col_float,sum(col_float) from vector_expr_table_02 group by col_float order by col_float;
select col_float2,sum(col_float2) from vector_expr_table_02 group by col_float2 order by col_float2;

select col_int, col_int2, sum(abs(-col_int!)+abs(col_int-col_int2!)) as a from vector_expr_table_02 where col_int < 25 AND col_int > 2 AND col_int2 <= 1935401906 group by col_int, col_int2 order by col_int, col_int2;
select col_int, col_int2, sum(abs(-col_int)+width_bucket(5.35, 0.024, 10.06, col_int)) as a from vector_expr_table_02 where col_int < 25 AND col_int > 2 AND col_int2 <= 2036166893 group by col_int, col_int2 order by col_int, col_int2;
select col_int, col_int2, sum(abs(-col_int)+abs(-col_int2)) as a from vector_expr_table_02 where col_int < 25 AND col_int > 2 AND col_int2 <= 2036166893 group by col_int, col_int2 order by col_int, col_int2;
select col_int, col_int2, sum(width_bucket(5.35::float, 0.024::float, 10.06::float, col_int)) as a from vector_expr_table_02 where col_int < 25 AND col_int > 2 AND col_int2 <= 1935401906 group by col_int, col_int2 order by col_int, col_int2;
select col_float,avg(col_float) from vector_expr_table_02 group by col_float order by col_float;
select col_float2,avg(col_float2) from vector_expr_table_02 group by col_float2 order by col_float2;
select count(col_int) + 2, avg(col_num) - 3 from vector_expr_table_02;

select count(col_num2),min(col_char),max(col_varchar),sum(col_float),avg(col_num2) from vector_expr_table_02;
select count(col_num2),min(col_char),max(col_varchar),sum(col_float),avg(col_num2) from vector_expr_table_02 group by col_float2 order by col_float2;

select col_int, col_int2, substring(col_varchar, 1, 2), count(*) from vector_expr_table_02 where col_date > '2012-10-1' group by col_int, col_int2, substring(col_varchar, 1, 2) order by col_int, col_int2, 4;

----
--- test 2: Test Case Expression
----
explain (verbose on, costs off) select col_int2, sum(case when col_int in (1, 7, 229, 993, 81, 6) then 1 else 0 end) as a, 'myConstString' from vector_expr_table_02 group by col_int2 order by col_int2;
select col_int2, sum(case when col_int in (1, 7, 229, 993, 81, 6) then 1 else 0 end) as a from vector_expr_table_02 group by col_int2 order by col_int2;
select col_int2, sum(case when col_int < 17 or col_int > 37 then 1 else 0 end) as a from vector_expr_table_02 group by col_int2 order by col_int2;
explain (verbose on, costs off) select col_int, col_int2, sum(case when col_int < 17 AND col_int > 7 AND col_int2 <= 1935401906 then 1 else 0 end) as a from vector_expr_table_02   where col_int < 25 AND col_int > 2 AND col_int2 <= 2036973298 group by col_int, col_int2 order by col_int, col_int2;
explain (verbose on, costs off) select col_int, col_int2, sum(case when col_int < 17 OR (col_int > 37 AND col_int2 > 10) then 1 else 0 end) as a from vector_expr_table_02   where col_int < 19 OR (col_int > 37 AND col_int2 > 2) group by col_int, col_int2 order by col_int, col_int2;
explain (verbose on, costs off) select col_int, col_int2, sum(case when col_int < 17 OR NOT (col_int > 7 AND col_int2 > 2) then 1 else 0 end) as a from vector_expr_table_02 where col_int < 19 OR NOT (col_int > 2 AND col_int2 > 1) AND NOT (col_int > 10 AND col_int2 < 10) group by col_int2, col_int order by col_int, col_int2;

select col_int, col_int2, 'myConstString', sum(case when col_int < 17 AND col_int > 7 AND col_int2 <= 1935401906 then 1 else 0 end) as a, 'my2ndConstString' from vector_expr_table_02 where col_int < 25 AND col_int > 2 AND col_int2 <= 2036973298 group by col_int, col_int2 order by col_int, col_int2;
select col_int, col_int2, sum(case when col_int < 17 AND col_int > 7 AND col_int2 <= 1935401906 then 1 else 0 end) as a from vector_expr_table_02   where col_int < 25 AND col_int > 2 AND col_int2 <= 2036973298 group by col_int, col_int2 order by col_int, col_int2;
select col_int, col_int2, sum(case when col_int < 17 OR (col_int > 37 AND col_int2 > 10) then 1 else 0 end) as a from vector_expr_table_02 where col_int < 19 OR (col_int > 37 AND col_int2 > 2) group by col_int, col_int2 order by col_int, col_int2;
select col_int, col_int2, sum(case when col_int < 17 OR NOT (col_int > 7 AND col_int2 > 2) then 1 else 0 end) as a from vector_expr_table_02 where col_int < 19 OR NOT (col_int > 2 AND col_int2 > 1) AND NOT (col_int > 10 AND col_int2 < 10) group by col_int2, col_int order by col_int, col_int2;
select col_int, col_int2, sum(case when col_int < 17 OR NOT (col_int > 7 AND col_int2 > 2) then 1 else 0 end) as a from vector_expr_table_02 where col_int < 19 OR NOT (col_int > 2 AND col_int2 > 1) AND NOT (col_int > 10 AND col_int2 < 10) group by col_int2, col_int order by col_int, col_int2;
select col_int2, sum(case when col_int < 17 or col_int > 37 then 100*col_num + 10.0*col_num +col_num*col_num2-col_num*col_num2 when col_int > 17 and col_int < 37 then col_num2-col_num else 6 end), sum (col_num-col_num2+col_num/col_num2+10) as a from vector_expr_table_02 group by col_int2 order by col_int2;
     
select col_char, sum(1) from vector_expr_table_02 where col_char in ('aabccd', 'abccd', 'abbccd') group by col_char order by col_char; 
select col_varchar, sum(1) from vector_expr_table_02 where col_varchar in ('aabccd', 'abccd', 'abbccd') group by col_varchar order by col_varchar; 
select overlay(col_varchar placing  'test' from 2) from vector_expr_table_02 order by col_varchar limit 1;

----
--- test 3: Test Multi-Level Case Expression
----
--simple one level
explain (verbose on, costs off) select a, b, case 
													when a = 1 then '1X'
													when a = 2 then '2X'
													when a = 3 then '3X'
													end
										from vector_expr_table_03;

select a, b, case 
				when a = 1 then '1X'
				when a = 2 then '2X'
				when a = 3 then '3X'
				end
		from vector_expr_table_03 order by 1, 2;

--one level with default value
select a, b, case 
				when a = 1 then '1X'
				when a = 2 then '2X'
				when a = 3 then '3X'
				else 'other'
				end
		from vector_expr_table_03 order by 1, 2;

--simple 2 level
explain (verbose on, costs off)
select a, b, case 
				when a = 1 then 
							case 
								when b = 1 then '11' 
								when b = 2 then '12'
								when b = 3 then '13'
								end
				when a = 2 then 
							case
								when b = 1 then '21' 
								when b = 2 then '22'
								when b = 3 then '23'
							end
		  end from vector_expr_table_03;

select a, b, case 
				when a = 1 then 
							case 
								when b = 1 then '11' 
								when b = 2 then '12'
								when b = 3 then '13'
								end
				when a = 2 then 
							case
								when b = 1 then '21' 
								when b = 2 then '22'
								when b = 3 then '23'
							end
				end from vector_expr_table_03 order by 1, 2;

--2 level with default value
select a, b, case 
				when a = 1 then 
							case 
								when b = 1 then '11' 
								when b = 2 then '12'
								when b = 3 then '13'
								else '1X other'
								end
				when a = 2 then 
							case
								when b = 1 then '21' 
								when b = 2 then '22'
								when b = 3 then '23'
								else '2X other'
							end
				else 'other'
				end from vector_expr_table_03 order by 1, 2;


--3 level
select a, b, c, case 
				when a = 1 then 
							case 
								when b = 1 then case
													when c = 1 then '111'
													when c = 2 then '112'
													end
								when b = 2 then '12'
								when b = 3 then case
													when c = 1 then '131'
													when c = 3 then '133'
													else '13X other'
													end
								else '1X other'
								end
				when a = 2 then 
							case
								when b = 1 then '21' 
								when b = 2 then case
													when c = 2 then '222'
													when c = 3 then '223'
													else '22X other'
													end
								when b = 3 then '23'
								else '2X other'
							end
				else 'other'
				end from vector_expr_table_03 order by 1, 2, 3; 

select a, b, case a when true then case b when true then '11' else '10' end else case b when true then '01' else '00' end end from vector_expr_table_05 order by 1, 2;

select a, b, c, case a > b 
				when true then 
							case b > ( case c when true then 1 when false then 0 end) 
									when true then  'a>b b>c' 
									when false then 'a>b b<=c'
									end
				when false then
							case b > ( case c when true then 1 when false then 0 end) 
									when true then  'a<=b b>c' 
									when false then 'a<=b b<=c'
									end
				end
				from vector_expr_table_05 order by 1, 2, 3;

select a, b, c, case a > b 
				when b > c then 
							case b > ( case c when true then 1 when false then 0 end) 
									when true then  'a>b == b>c == 1' 
									when false then 'a>b == b>c == 0'
									end
				when b <= c then
							case a > ( case c when true then 1 when false then 0 end) 
									when true then  'a>b == b<=c  a>c==1'
									when false then 'a>b == b<=c a>c==0'
									end
				end
				from vector_expr_table_05 order by 1, 2, 3;

select a, b, c, case a > b 
				when true then 
							case b > ( case c>'b' when true then '1' when false then '0' end) 
									when true then  'a>b b>c' 
									when false then 'a>b b<=c'
									end
				when false then
							case b > ( case c <='b' when true then '1' when false then '0' end) 
									when true then  'a<=b b>c' 
									when false then 'a<=b b<=c'
									end
				end
				from vector_expr_table_06 order by 1, 2, 3;


select a, b, c, case a > b 
				when b > c then 
							case b > ( case c when true then 1 when false then 0 end) 
									when true then  'a>b == b>c == 1' 
									when false then 'a>b == b>c == 0'
									else 'a>b == b>c'
									end
				when b <= c then
							case a > ( case c when true then 1 when false then 0 end) 
									when true then  'a>b == b<=c  a>c==1'
									when false then 'a>b == b<=c a>c==0'
									else 'a>b ==b<=c'
									end
				else 'other'
				end
				from vector_expr_table_05 order by 1, 2, 3;
				
				
----
--- test 4: Scarlar Array OP( ANY & ALL)
----
--OP ANY
explain (verbose on, costs off) select col_int from vector_expr_table_02 where col_int = ANY(array[NULL]) order by 1;
select col_int from vector_expr_table_02 where col_int = ANY(array[NULL]) order by 1;

select col_int from vector_expr_table_02 where col_int = ANY(array[NULL, 0]) order by 1;
select col_int from vector_expr_table_02 where col_int = ANY(array[0, NULL]) order by 1;
select col_int from vector_expr_table_02 where col_int = ANY(array[NULL, 1]) order by 1;
select col_int from vector_expr_table_02 where col_int = ANY(array[1, NULL]) order by 1;

select col_int from vector_expr_table_02 where col_int > ANY(array[NULL]) order by 1;
select col_int from vector_expr_table_02 where col_int > ANY(array[NULL, 0]) order by 1;
select col_int from vector_expr_table_02 where col_int > ANY(array[0, NULL]) order by 1;
select col_int from vector_expr_table_02 where col_int > ANY(array[NULL, 1]) order by 1;
select col_int from vector_expr_table_02 where col_int > ANY(array[1, NULL]) order by 1;

select col_int from vector_expr_table_02 where col_int < ANY(array[NULL]) order by 1;
select col_int from vector_expr_table_02 where col_int < ANY(array[NULL, 0]) order by 1;
select col_int from vector_expr_table_02 where col_int < ANY(array[0, NULL]) order by 1;
select col_int from vector_expr_table_02 where col_int < ANY(array[NULL, 1]) order by 1;
select col_int from vector_expr_table_02 where col_int < ANY(array[1, NULL]) order by 1;

select col_int from vector_expr_table_02 where col_int = ANY(array[1, 2, 3]) order by 1;
select col_int from vector_expr_table_02 where col_int > ANY(array[-1, 2, 0]) order by 1;
select col_int from vector_expr_table_02 where col_int < ANY(array[8, -1, 0]) order by 1;

--OP ALL
select col_int from vector_expr_table_02 where col_int = ALL(array[NULL]) order by 1;
select col_int from vector_expr_table_02 where col_int = ALL(array[NULL, 0]) order by 1;
select col_int from vector_expr_table_02 where col_int = ALL(array[0, NULL]) order by 1;
select col_int from vector_expr_table_02 where col_int = ALL(array[NULL, 1]) order by 1;
select col_int from vector_expr_table_02 where col_int = ALL(array[1, NULL]) order by 1;

select col_int from vector_expr_table_02 where col_int > ALL(array[NULL]) order by 1;
select col_int from vector_expr_table_02 where col_int > ALL(array[NULL, 0]) order by 1;
select col_int from vector_expr_table_02 where col_int > ALL(array[0, NULL]) order by 1;
select col_int from vector_expr_table_02 where col_int > ALL(array[NULL, 1]) order by 1;
select col_int from vector_expr_table_02 where col_int > ALL(array[1, NULL]) order by 1;

select col_int from vector_expr_table_02 where col_int < ALL(array[NULL]) order by 1;
select col_int from vector_expr_table_02 where col_int < ALL(array[NULL, 0]) order by 1;
select col_int from vector_expr_table_02 where col_int < ALL(array[0, NULL]) order by 1;
select col_int from vector_expr_table_02 where col_int < ALL(array[NULL, 1]) order by 1;
select col_int from vector_expr_table_02 where col_int < ALL(array[1, NULL]) order by 1;

select col_int from vector_expr_table_02 where col_int = ALL(array[1, 2, 3]) order by 1;
select col_int from vector_expr_table_02 where col_int > ALL(array[-1, 2, 0]) order by 1;
select col_int from vector_expr_table_02 where col_int < ALL(array[8, -1, 0]) order by 1;

--in expr
set enable_codegen=off;
select col_tinterval from vector_expr_table_07 where col_tinterval in ('["Sep 4, 1983 23:59:12" "Oct 4, 1983 23:59:12"]', '["May 10, 1947 23:59:12" "Jan 14, 1973 03:14:21"]') order by 1;
select col_interval from vector_expr_table_07 where col_interval in ('2 day 13:34:56', '1 day 18:34:56') order by 1; 
select col_timetz from vector_expr_table_07 where col_timetz in ('08:00:30+08', '00:00:30+08', '12:00:30+08') order by 1;
reset enable_codegen;

----
--- test 5: Coalesce Expression
----
select a, b, c, d, coalesce(a, b, c, d) abcd, coalesce(a, b, c) abc, coalesce(a, b) ab, coalesce(a, c) ac, coalesce(b, c) bc, coalesce(a) a, coalesce(b) b, coalesce(c) c, coalesce(d) d from vector_expr_table_04 order by 1, 2, 3, 4;

select a, b, c, coalesce(a, coalesce(a)),  coalesce(a, coalesce(a, coalesce(a))), coalesce(a, coalesce(b, coalesce(c))), coalesce(a, coalesce(a, b, c)) from vector_expr_table_04 order by 1, 2, 3;

----
--- test 6: Boolean Expression
----
select a, a is true from vector_expr_table_05 order by a;
select a, a is true from vector_expr_table_05 where a is true order by a;
select a, a is true from vector_expr_table_05 where b is true order by a;

select a, a is not true from vector_expr_table_05 order by a;
select a, a is not true from vector_expr_table_05 where a is not true order by a;
select a, a is not true from vector_expr_table_05 where b is true order by a;

select a, a is false from vector_expr_table_05 order by a;
select a, a is false from vector_expr_table_05 where a is false order by a;
select a, a is false from vector_expr_table_05 where b is false order by a;

select a, a is not false from vector_expr_table_05 order by a;
select a, a is not false from vector_expr_table_05 where a is not false order by a;
select a, a is not false from vector_expr_table_05 where b is false order by a;

select a, a is unknown from vector_expr_table_05 order by a;
select a, a is unknown from vector_expr_table_05 where a is unknown order by a;
select a, a is unknown from vector_expr_table_05 where b is unknown order by a;

select a, a is not unknown from vector_expr_table_05 order by a;
select a, a is not unknown from vector_expr_table_05 where a is not unknown order by a;
select a, a is not unknown from vector_expr_table_05 where b is unknown order by a;

--nvl 
select nvl(b, 0) from VECTOR_EXPR_TABLE_05 order by 1;

----
--- test 7: Min-Max Expression
----
select a, b, c, greatest(a::int, b), least(a::int, b), greatest(a, c), least(a, c), greatest(b::bool, c), least(b::bool, c), greatest(a::int, b, c::int), least(a::int, b, c::int) from  vector_expr_table_05 order by 1, 2, 3;

select a, b, c, greatest(a, b), least(a, b), greatest(a, c), least(a, c), greatest(b, c), least(b, c), greatest(a, b, c), least(a, b, c) from  vector_expr_table_06 order by 1, 2, 3;

SELECT a, GREATEST(CASE WHEN (a > '0') THEN a ELSE ('-1') END, '1') AS greatest FROM vector_expr_table_06 order by 1, 2;

select greatest(a::int, b), least(a::int, b) from  vector_expr_table_05 where greatest(a::int, b)*2 > 0 and least(a::int, b)*2<10 order by 1,2;

----
--- test 8: CoerceViaIO Expression
----
select col_int::numeric from vector_expr_table_07 where col_int = 5;
select col_num::int from vector_expr_table_07 where col_int = 5;
select col_int::varchar from vector_expr_table_07 where col_int = 5;
select col_num::varchar from vector_expr_table_07 where col_int = 5;
select col_timestamptz::varchar from vector_expr_table_07 where col_int = 5;
select col_timestamptz::text from vector_expr_table_07 where col_int = 5;
select col_varchar::timestamp with time zone from vector_expr_table_07 where col_int = 5;

select col_char::text from vector_expr_table_07 order by 1;
select col_interval::text from vector_expr_table_07 order by 1;
select col_timetz::text from vector_expr_table_07 order by 1;
select col_tinterval::text from vector_expr_table_07 order by 1; 
select (col_tinterval::text)::tinterval from vector_expr_table_07 order by 1;
select (col_timetz::text)::timetz from vector_expr_table_07 order by 1;
select (col_interval::varchar)::interval from vector_expr_table_07 order by 1;

delete from vector_expr_table_07 where col_varchar='2017-09-09 19:45:37';
insert into vector_expr_table_07 values (123, 5, '2017-09-09 19:45:37', '1');
set enable_hashjoin=off;
explain (verbose on, costs off) select * from (select substr(col_varchar, 0,1) col1 from vector_expr_table_07 ) table1 left join vector_expr_table_08 on table1.col1=vector_expr_table_08.col_num;
select * from (select substr(col_varchar, 0,1) col1 from vector_expr_table_07 ) table1 left join vector_expr_table_08 on table1.col1=vector_expr_table_08.col_num where col_num is not null;
reset enable_hashjoin;

----
--- Clean Resource and Tables
----
drop schema vector_expression_engine_first cascade;

