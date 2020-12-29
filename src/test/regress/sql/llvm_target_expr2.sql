/* 
 * This file is used to test the LLVM Optimization in target list expression.
 * It's purpose is to  cover the basic functionality about expressions.
 */
/********************************
Expression Type:
    T_Var,
    T_Const,
    T_Case,
    T_OpExpr,
    T_ScalarArrayOp
    T_FuncExpr,
    T_BoolExpr,
    T_BoolenTest,
    T_NullTest
Using Tye:
    targetlist
********************************/
----
--- Create Table and Insert Data
----
drop schema if exists llvm_target_engine2 cascade ;
create schema llvm_target_engine2;
set current_schema = llvm_target_engine2;
set time zone 'PRC';
set codegen_cost_threshold=0;

CREATE TABLE llvm_target_engine2.llvm_vecexpr_table_03
( col_num	numeric(5, 0)
  ,col_int	int
  ,col_date	date
  ,col_timestamptz	timestamptz
  ,col_varchar	varchar
  ,col_char	char(2)
  ,col_interval	interval
  ,col_timetz	timetz
  ,col_tinterval	tinterval
) with(orientation=column);


COPY llvm_vecexpr_table_03(col_num, col_int, col_date, col_timestamptz, col_varchar, col_char, col_interval, col_timetz, col_tinterval) FROM stdin;
123	5	2017-10-09	2017-09-09 19:45:37-02	2017-09-09 19:45:37	a	2 day 13:34:56	1984-2-6 01:00:30+8	["Sep 4, 1983 23:59:12" "Oct 4, 2000 23:59:12"]
\N	5	2017-10-09	2017-09-09 19:45:37-02	2015-08-08 19:45:37	a	2 months 1 day 13:34:56	01:00:30+5	["Sep 4, 1983 23:59:12" "Oct 4, 2000 23:59:12"]
234	0	2017-10-09	2017-10-09 19:45:37+08	2017-10-09	c	1 day 18:34:56	1986-2-6 03:00:30+0	["May 10, 1947 23:59:12" "Jan 14, 1973 03:14:21"]
2.34	60	\N	2017-10-09 19:45:37+08	19:45:37	c	2 years 1 day 18:34:56	1986-2-6 03:00:30	["May 10, 1947 23:59:12" "Jan 14, 1973 03:14:21"]
-234	6	2017-10-09	2017-10-09 19:45:37+08	2017-10-09 19:45:37	c	-2 years 1 day 18:34:56	03:00:30	["May 10, 1947 23:59:12" "Jan 14, 1973 03:14:21"]
345	7	\N	2017-11-09 19:45:37+8	2017-11-09 19:45:37	d	1 day 13:34:56	1987-2-6 08:00:30+4	["epoch" "Mon May 1 00:30:30 1995"]
456	8	2015-12-09	2017-12-09 19:45:37-02	2017-12-09 19:45:37	h	18 day 14:34:56	06:00:30+8	["Feb 15 1990 12:15:03" "2005-09-23 11:12:13"]
567	9	\N	2018-01-09 19:45:37-2	2018-01-09 19:45:37	m	18 day 15:34:56	1990-2-6 12:00:30+8	\N
6.78	1	2018-03-09	2018-02-09 19:45:37+08	2018-02-09 19:45:37	\N	7 day 16:34:56	2002-2-6 00:00:30+8	["-infinity" "infinity"]
\N	10	2018-03-09	\N	2018-02-09	\N	1 year 10 months 12 hours	2002-2-6 00:00:30+8	["-infinity" "infinity"]
789	11	2019-12-11	2018-03-09 19:45:37+8	2018-03-09 19:45:37	g	22 day 13:34:56	1984-2-6 00:00:20+8	["Feb 10, 2010 23:59:12" "May 14, 2016 13:14:21"]
78.9	0	2019-12-11	2018-03-09 19:45:37+8	19:45:37	g	-22 day 13:34:56	1984-2-6 00:00:20	["Feb 10, 2010 20:59:12" "May 14, 2016 03:14:21"]
147	12	2015-05-09	2018-04-09 19:45:37+05	2018-04-09 19:45:37	l	\N	1984-2-7 00:00:20-4	["Feb 10, 2010 2:59:12" "Dec 14, 2016 03:14:21"]
369	13	1984-2-4	2018-05-09 19:45:37+5	2018-05-09 19:45:37	a	21 day 13:34:56	\N	["Feb 10, 2010 13:59:12" "Nov 14, 2016 03:14:21"]
\.

CREATE TABLE llvm_target_engine2.LLVM_VECEXPR_TABLE_04(
    col_bool boolean,
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
    col_time    timestamp
)with(orientation=column);
create index llvm_index_41 on llvm_vecexpr_table_04(col_int, col_char);

COPY LLVM_VECEXPR_TABLE_04(col_bool, col_int, col_bigint, col_float, col_float8, col_char, col_bpchar, col_varchar, col_text1, col_text2, col_num1, col_num2, col_date, col_time) FROM stdin;
1	1	256	3.1	3.25	beijing	AaaA	newcode	myword	myword1	3.25	3.6547	2011-11-01 00:00:00	2017-09-09 19:45:37
1	0	26	3.0	10.25	beijing	AaaA	newcode	myword	myword2	-3.2	-0.6547	\N	2017-09-09 19:45:37
0	3	12400	2.6	3.64755	hebei	BaaB	knife	sea	car	1.62	3.64	2017-10-09 19:45:37	2017-10-09 21:45:37
f	5	0	1.0	25	anhui	CccC	computer	game	game2	7	3.65	2012-11-02 00:00:00	2018-04-09 19:45:37
t	-16	1345971420	3.2	2.15	hubei	AaaA	phone	pen	computer	-4.24	-6.36	2012-11-04 00:00:00	2012-11-02 00:03:10
0	-10	1345971420	3.2	2.15	hubei	AaaA	phone	pen	computer	4.24	6.36	2012-11-04 00:00:00	2012-11-02 00:03:10
t	64	-2566	1.25	2.7	jilin	DddD	girl	flower	window	65	-69.36	2012-11-03 00:00:00	2011-12-09 19:45:37
\N	64	-2566	1.25	2.7	jilin	DddD	boy	flower	window	65	69.36	2012-11-03 00:00:00	2011-12-09 19:45:37
f	\N	256	3.1	4.25	anhui	BbbB	knife	phone	light	78.12	2.35684156	2017-10-09 19:45:37	1984-2-6 01:00:30
t	81	\N	4.8	3.65	luxi	EeeE	girl	sea	crow	145	56	2018-01-09 19:45:37	2018-01-09 19:45:37
0	8	\N	5.8	30.65	luxing	EffE	girls	sea	crown	14	506	\N	\N
0	25	365487	\N	3.12	lufei	EeeE	call	you	them	7.12	6.36848	2018-05-09 19:45:37	2018-05-09 19:45:37
1	36	5879	10.15	\N	lefei	GggG	say	you	them	2.5	-2.5648	2015-02-26 02:15:01	1984-2-6 02:15:01
0	36	-59	10.15	\N	lefei	GggG	call	you	them	2.5	2.5648	2015-02-26 02:15:01	1984-2-6 02:15:01
t	0	0	10.15	\N	hefei	GggG	call	your	them	-2.5	2.5648	\N	1984-2-6 02:15:01
f	27	256	4.25	63.27	\N	FffF	code	throw	away	2.1	25.65	2018-03-09 19:45:37	2017-09-09 14:20:00
\N	9	-128	-2.4	56.123	jiangsu	\N	greate	book	boy	7	-1.23	2017-12-09 19:45:37	 2012-11-02 14:20:25
0	1001	78956	1.25	2.568	hangzhou	CccC	\N	away	they	6.36	58.254	2017-10-09 19:45:37	1984-2-6 01:00:30
1	2005	12400	12.24	2.7	hangzhou	AaaA	flower	\N	car	12546	3.2546	2017-09-09 19:45:37	2012-11-02 00:03:10
\N	8	5879	\N	1.36	luxi	DeeD	walet	wall	\N	2.58	3.54789	2000-01-01	2000-01-01 01:01:01
f	652	25489	8.88	1.365	hebei	god	piece	sugar	pow	\N	2.1	2012-11-02 00:00:00	2012-11-02 00:00:00
1	417	2	9.19	0.256	jiangxi	xizang	walet	bottle	water	11.50	-1.01256	\N	1984-2-6 01:00:30
0	18	65	-0.125	78.96	henan	PooP	line	black	redline	24	3.1415926	2000-01-01	\N
1	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N
\N	-700	58964785	3.25	1.458	\N	qingdao	\N	2897	dog	9.36	\N	\N	2017-10-09 20:45:37
1	-505	1	3.24	\N	\N	BbbB	\N	myword	pen	147	875	2000-01-01 01:01:01	2000-01-01 01:01:01
\.

CREATE TABLE llvm_target_engine2.LLVM_VECEXPR_TABLE_05(
    col_int	int,
	col_char1 char(1)	default ' ',
	col_char2 char(16)	not null,
	col_varchar1 varchar(20),
	col_varchar2 varchar,
	col_text	text
)with(orientation=column);

copy LLVM_VECEXPR_TABLE_05 from stdin;
1	 	AAA	BBtextBB	tianjing	timeisok
-1	 	 	 	nanjing 	timeon 
	a	 bb 	  	 	 
10	 	aa 	 cc 	 	 
\.

analyze llvm_vecexpr_table_03;
analyze llvm_vecexpr_table_04;
analyze llvm_vecexpr_table_05;

-----
---  booleantest expr
-----
explain (verbose on, costs off) 
select col_bool, col_int, col_bool is false and col_int is not true 
    from llvm_vecexpr_table_04;

select col_bool, col_bool is false from llvm_vecexpr_table_04 order by 1, 2;
select col_bool, col_bool is not true from llvm_vecexpr_table_04 order by 1, 2;
select col_bool, col_bool is unknown from llvm_vecexpr_table_04 order by 1, 2;

-----
---  nulltest
-----
explain (verbose on, costs off) 
select col_bool is null, col_bigint is not null, col_char is not null, col_bpchar is null,
        col_text1 is not null, col_date is null, col_time is not null 
    from llvm_vecexpr_table_04;

select col_num is null, col_int is not null, col_timestamptz is not null,col_interval is null, 
        col_timetz is null, col_tinterval is not null 
    from llvm_vecexpr_table_03
    order by 1, 2, 3, 4, 5, 6;

select col_bool is not null, col_float is null, col_bpchar is null, col_text1 is null, 
        col_num1 is not null, col_time is not null 
    from llvm_vecexpr_table_04 
    order by 1, 2, 3, 4, 5, 6;
-----
---  bool expr
-----

select col_bool, col_int, col_bool and col_int, col_bool or col_int, not col_bool
    from llvm_vecexpr_table_04 
    order by 1, 2, 3, 4, 5;

-----
---  nulltest + bool expr + booleantest + opCodgen + funCodegen
-----
select col_int, col_bool,
        col_int is not null 
        and (col_bool is not null and col_bool is true) 
    from llvm_vecexpr_table_04 order by 1, 2, 3;

-----
---  case-when 
-----
select col_num, col_varchar, col_date,
        case when col_num > 123.5 then 'BIGGER'
             when col_num = 123.5 then 'EQUAL'
             when col_num < 123.5 then 'SMALLER'
             else 'UKNOWN' end,
        case when col_varchar > '2017-09-09 19:45:37' then 'BIGGER'
            when col_varchar = '2017-09-09 19:45:37' then 'EQUAL'
            when col_varchar < '2017-09-09 19:45:37' then 'SMALLER'
            else 'unknown' end,
        case when col_date > '2017-10-09' then 'BIGGER'
            when col_date = '2017-10-09' then 'EQUAL'
            when col_date < '2017-10-09' then 'SAMLLER'
            else Null end
    from llvm_vecexpr_table_03
    order by 1, 2, 3, 4, 5, 6;

----
---   ArrayOp + bool Expr + nulltest +  in targetlist
----
select col_num, col_date, col_int, col_interval, col_char, col_timetz,
        col_num > any(array[Null]),
        col_date < any(array['2017-10-09 00:00:30', '2016-1-5 10:12:00']),
        col_int in (0, 2, 4, 8),
        col_char = any(array[Null]),
        col_interval in ('1 day 13:34:56', '1 year 10 months 12:00:00', '-22 days 13:34:56'),
        col_timetz in ('01:00:30+8', '03:00:30', '00:00:20-4', '00:00:30', '8:00:30+8')
    from llvm_vecexpr_table_03
    order by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12;

select col_timetz, col_timetz in ('01:00:30+8', '03:00:30', '12:00:30+08') from llvm_vecexpr_table_03 order by 1;

select col_num1, col_num1 in (Null) from llvm_vecexpr_table_04 order by 1, 2;

select col_float, col_num1, col_num2, col_text2,
        col_float in (1.62, 7, 11.50, 2.1, Null, 1.0, 2.0, 3.2, 8.8),
        col_num2 = any(array[3.64, -6.36, 6.36, 25.64, 10.0, Null]),
        col_num2 <= any(array[3.64, -6.36, 6.36, 25,64, 10.0]),
        col_text2 >= any(array['car', 'you', 'me', 'him']),
        col_text1 in ('you', 'me', 'him', 'your', 'them')
    from llvm_vecexpr_table_04
    order by 1, 2, 3, 4, 5, 6, 7, 8, 9;

-----
---  lpad + substring
-----
select nullif(col_interval, 33) from llvm_vecexpr_table_03 order by 1; 

----
----
set codegen_strategy = pure;
explain (verbose on, costs off, analyze on)
select col_text1 from llvm_vecexpr_table_04 where rtrim(col_text1) like '%r' order by 1;
set codegen_strategy = partial;

----
----
select col_text1, substrb(col_text1, -4) from llvm_vecexpr_table_04 order by 1, 2;
----
----
set enable_seqscan = off;
set enable_bitmapscan = off;

explain (verbose on, costs off) 
select * from llvm_vecexpr_table_04 where col_int > 10 and nullif(col_num1, col_num2) is not null;

select * from llvm_vecexpr_table_04 where col_int > 10 and nullif(col_num1, col_num2) is not null order by 1, 2, 3, 4, 5;

set enable_seqscan = on;
set enable_bitmapscan = on;

----
----
set enable_nestloop = off;
select * from llvm_vecexpr_table_04 as t4, llvm_vecexpr_table_05 as t5 where t5.col_int = nullif(t4.col_num1, '') order by 1, 2, 3;
set enable_nestloop = on;

----
--- clean table and resource
----
drop schema llvm_target_engine2 cascade;
