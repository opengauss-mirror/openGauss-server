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
drop schema if exists llvm_target_engine cascade ;
create schema llvm_target_engine;
set current_schema = llvm_target_engine;
set time zone 'PRC';
set codegen_cost_threshold=0;

CREATE TABLE llvm_target_engine.LLVM_VECEXPR_TABLE_01(
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
)with(orientation=column)

partition by range (col_int)
(
    partition llvm_vecexpr_table_01_01 values less than (0),
    partition llvm_vecexpr_table_01_02 values less than (100),
    partition llvm_vecexpr_table_01_03 values less than (500),
    partition llvm_vecexpr_table_01_04 values less than (maxvalue)
);

COPY LLVM_VECEXPR_TABLE_01(col_int, col_bigint, col_float, col_float8, col_char, col_bpchar, col_varchar, col_text1, col_text2, col_num1, col_num2, col_date, col_time) FROM stdin;
1	256	3.1	3.25	beijing	AaaA	newcode	myword	myword1	3.25	3.6547	\N	2017-09-09 19:45:37
0	26	3.0	10.25	beijing	AaaA	newcode	myword	myword2	-3.2	-0.6547	\N	2017-09-09 19:45:37
3	12400	2.6	3.64755	hebei	BaaB	knife	sea	car	1.62	3.64	2017-10-09 19:45:37	2017-10-09 21:45:37
5	25685	1.0	25	anhui	CccC	computer	game	game2	7	3.65	2012-11-02 00:00:00	2018-04-09 19:45:37
-16	1345971420	3.2	2.15	hubei	AaaA	phone	pen	computer	-4.24	-6.36	2012-11-04 00:00:00	2012-11-02 00:03:10
-10	1345971420	3.2	2.15	hubei	AaaA	phone	pen	computer	4.24	0.00	2012-11-04 00:00:00	2012-11-02 00:03:10
64	-2566	1.25	2.7	jilin	DddD	girl	flower	window	65	-69.36	2012-11-03 00:00:00	2011-12-09 19:45:37
64	0	1.25	2.7	jilin	DddD	boy	flower	window	65	69.36	2012-11-03 00:00:00	2011-12-09 19:45:37
\N	256	3.1	4.25	anhui	BbbB	knife	phone	light	78.12	2.35684156	2017-10-09 19:45:37	1984-2-6 01:00:30
81	\N	4.8	3.65	luxi	EeeE	girl	sea	crow	145	56	2018-01-09 19:45:37	2018-01-09 19:45:37
8	\N	5.8	30.65	luxing	EffE	girls	sea	crown	\N	506	\N	\N
25	0	\N	3.12	lufei	EeeE	call	you	them	7.12	6.36848	2018-05-09 19:45:37	2018-05-09 19:45:37
36	5879	10.15	\N	lefei	GggG	say	you	them	2.5	-2.5648	2015-02-26 02:15:01	1984-2-6 02:15:01
36	59	10.15	\N	lefei	GggG	call	you	them	2.5	\N	2015-02-26 02:15:01	1984-2-6 02:15:01
0	0	10.15	\N	hefei	GggG	call	your	them	-2.5	2.5648	\N	1984-2-6 02:15:01
27	256	4.25	63.27	\N	FffF	code	throw	away	2.1	25.65	2018-03-09 19:45:37	\N
9	-128	-2.4	56.123	jiangsu	\N	greate	book	boy	7	-1.23	2017-12-09 19:45:37	 2012-11-02 14:20:25
1001	78956	1.25	2.568	hangzhou	CccC	\N	away	they	6.36	58.254	2017-10-09 19:45:37	1984-2-6 01:00:30
2005	12400	12.24	2.7	hangzhou	AaaA	flower	\N	car	12546	3.2546	2017-09-09 19:45:37	2012-11-02 00:03:10
8	5879	\N	1.36	luxi	DeeD	walet	wall	\N	2.58	3.54789	2000-01-01	2000-01-01 01:01:01
652	25489	8.88	1.365	hebei	god	piece	sugar	pow	\N	2.1	2012-11-02 00:00:00	2012-11-02 00:00:00
417	2	9.19	0.256	jiangxi	xizang	walet	bottle	water	11.50	-1.01256	\N	1984-2-6 01:00:30
18	65	-0.125	78.96	henan	PooP	line	black	redline	24	3.1415926	2000-01-01	\N
\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N
-700	58964785	3.25	1.458	\N	qingdao	\N	2897	dog	9.36	\N	\N	2017-10-09 20:45:37
-505	1	3.24	\N	\N	BbbB	\N	myword	pen	147	875	2000-01-01 01:01:01	2000-01-01 01:01:01
\.


CREATE TABLE llvm_target_engine.LLVM_VECEXPR_TABLE_02(
    col_bool	bool,
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
create index llvm_index_01 on llvm_vecexpr_table_02(col_int);
create index llvm_index_02 on llvm_vecexpr_table_02(col_char);
create index llvm_index_03 on llvm_vecexpr_table_02(col_varchar);
create index llvm_index_04 on llvm_vecexpr_table_02(col_text);
create index llvm_index_05 on llvm_vecexpr_table_02(col_date);

COPY LLVM_VECEXPR_TABLE_02(col_bool, col_sint, col_int, col_bigint, col_char, col_bpchar, col_varchar, col_text, col_date, col_time) FROM stdin;
f	1	0	256	11	111	1111	123456	2000-01-01 01:01:01	2000-01-01 01:01:01
1	1	1	0	101	11	11011	3456	\N	2000-01-01 01:01:01
0	2	2	128	24	75698	56789	12345	2017-09-09 19:45:37	\N
1	3	30	2899	11111	1111	12345	123456	2015-02-26	2012-12-02 02:15:01
0	4	417	0	245	111	1111	123456	2018-05-09 19:45:37	1984-2-6 01:00:30
f	5	\N	365487	111	1111	12345	123456	\N	1984-2-6 01:00:30
0	6	0	6987	11	111	24568	123456	\N	2018-03-07 19:45:37
t	7	18	1348971452	24	2563	2222	56789	2000-01-01	2000-01-01 01:01:01
0	8	\N	258	\N	1258	25879	25689	2014-05-12	2004-2-6 07:30:30
1	\N	569	254879963	11	\N	547	36589	2016-01-20	2012-11-02 00:00:00
\N	8	4	\N	11	111	\N	56897	2013-05-08	2012-11-02 00:03:10
\N	8	\N	\N	11	111	\N	56897	2013-05-08	2012-11-02 00:03:10
1	\N	56	58964	25	365487	5879	\N	2018-03-07	1999-2-6 01:00:30
t	\N	694	2	364	56897	\N	\N	2018-11-05	2011-2-6 01:00:30
f	-1	-30	-3658	5879	11	25879	\N	2018-03-07	\N
1	-2	-15	-24	3698	58967	698745	5879	2012-11-02	2012-11-02 00:00:00
\N	-3	2147483645	258	3698	36587	125478	111	2015-02-2	2000-01-01 01:01:01
0	12	-48	-9223372036854775802	258	36987	12587	2547	2014-03-12	2012-11-02 01:00:00
1	-3	-2	9223372036854775801	3689	256987	36547	14587	2016-01-20	2012-11-02 07:00:00
\N	-6	-2147483640	-1587452	1112	1115	12548	36589	\N	1999-2-6 01:00:30
t	-6	\N	-1587452	1112	1115	12548	36589	2014-03-12	\N
\.

analyze llvm_vecexpr_table_01;
analyze llvm_vecexpr_table_02;

-----
---  booleantest expr
-----
select col_bool, col_bool is not false from llvm_vecexpr_table_02 order by 1, 2;
select col_bool, col_bool is true from llvm_vecexpr_table_02 order by 1, 2;
select col_bool, col_bool is not unknown from llvm_vecexpr_table_02 order by 1, 2;
-----
---  nulltest
-----
select col_int is null, col_float8 is not null, col_bpchar is null, col_varchar is not null, 
        col_num1 is not null, col_date is null 
    from llvm_vecexpr_table_01
    order by 1, 2, 3, 4, 5, 6;

select col_bool is null, col_sint is not null, col_char is null, col_varchar is not null, 
        col_date is null, col_time is not null 
    from llvm_vecexpr_table_02
    order by 1, 2, 3, 4, 5, 6;

-----
---  bool expr
-----
select col_bool, col_int, col_bool and col_int, col_bool or col_int, not col_bool
    from llvm_vecexpr_table_02 
    order by 1, 2, 3, 4, 5;

select col_int, col_bigint, col_int and col_bigint, col_int or col_bigint, not col_int
    from llvm_vecexpr_table_01
    order by 1, 2, 3, 4, 5;

-----
---  opCodegen with result type TIMESTAMP
-----
---  explain performance
select col_date, col_date - interval '2 years 10 months' < timestamp '2015-1-1'  
    from llvm_vecexpr_table_01 
    where col_date + interval '2 years 1 month 10 days' >= timestamp '2016-11-1 12:20'
    order by 1, 2;


-----
---  nulltest + bool expr + booleantest + opCodgen + funCodegen
-----
select col_int, col_bool,
        col_int is not null and col_int is true 
        or col_bool is not null and col_bool is true 
    from llvm_vecexpr_table_02 order by 1, 2, 3;

select (col_int - col_float)*(col_num2 - col_float8) < (col_num1 - col_bigint)/0.1,
        (length(rtrim(col_char, ' ')) - length(col_bpchar))*(length(col_varchar) - length(col_text1)) = 0
    from llvm_vecexpr_table_01
    order by 1, 2;

select (col_int - col_float)*(col_num2 - col_float8) < (col_num1 - col_bigint)/0.1 
        or (length(rtrim(col_char, ' ')) - length(col_bpchar))*(length(col_varchar) - length(col_text1)) = 0 
        and (col_date - col_time < INTERVAL '1 year 5 months')
    from llvm_vecexpr_table_01
    order by 1;

-----
---  case-when 
-----
select col_bool, col_int, col_char, 
        case col_bool when false then 'FALSE'
                when true then 'TRUE'
                else 'UNKNOWN' end,
        case when col_int > 0 then 'POSITIVE'
             when col_int < 0 then 'NEGATIVE'
             when col_int = 0 then 'ZERO'
            else Null end,
        case when col_char > '1112' then 'BIGGER'
             when col_char = '1112' then 'EQUAL'
             when col_char < '1112' then 'SMALLER'
            else 'UNKNOWN' end
    from llvm_vecexpr_table_02
    order by 1, 2, 3, 4, 5, 6;

select col_bigint, col_text1, col_num2,
        case col_bigint when 0 then col_date is Null
                    when 256 then col_date is not Null
                    else Null end,
        case when col_text1 = col_text2 then col_time != col_date
            when col_text1 > col_text2 then col_time < col_date
            when col_text1 < col_text2 then col_time > col_date
            else col_text1 is Null end, 
        case when col_num2 > 0 then col_num2 > col_num1
            when col_num2 < 0 then col_num2 < col_num1
            when col_num2 = 0 then col_num2 != col_num1
            else col_num2 is not null end
    from llvm_vecexpr_table_01
    order by 1, 2, 3, 4;

-----
---  multi-level case-when + textlike + nulltest + funcCodegen + bool expr 
-----
select col_date, col_time, 
        case when col_date is null then
                case when col_time is null then 'null'
                    when col_time like '2016%' then '2016'
                    when col_time < '2016-01-01 00:00:00' then '2015'
                    else '2017' end
             when col_date like '2016%' then '2016'
             when col_date < '2016-01-01 00:00:00' then '2015'
             else '2017' end
    from llvm_vecexpr_table_02 order by 1, 2, 3;

select col_char, col_text1, col_num1, col_num2,  
            case when substr(col_char, 1, 2) = 'lu' then
                    case when substr(col_text1, 1, 3) = 'you' then 'AA'
				         when substr(col_text1, 1, 3) = 'sea' then 'AB'
				         else 'AC' end
			   when substr(col_char, 1, 2) = 'an' then
			        case when substr(col_text1, 1, 3) = 'you' then 'BB'
						 when substr(col_text1, 1, 3) = 'sea' then 'BC'
						 else 'BD' end
			   else 'AA' end = 'AA' 
            and 
            case when col_num2 > 0 then col_num1 > col_num2
                 when col_num2 < 0 then col_num1 < col_num2
                else col_num1 != 0 end
            and 
            (case col_num1 when 145.0 then null
                    when 12546.0 then null
                    else col_num1 end 
                        is not null)
    from llvm_vecexpr_table_01 order by 1, 2, 3, 4, 5;
 
----
---   ArrayOp + bool Expr + nulltest +  in targetlist
----
select col_int, col_char, col_float, col_num1, 
        col_int in (-16, 1, 79, 1000000000000, Null, -2000000000000, Null)
            or col_char is NULL
            or col_char in ('hangzhou', 'jiangsu')
            and col_num1 in (1.62, 7, 11.50, 2.1)
            and col_float in (1.62::float4, 7::float4, 11.5000::float4, 2.1::float4)
            or col_num1 is NULL
            or col_date = any(array[timestamp '2012-11-02', '2012-11-04', '2018-04-04', '2015-02-26', Null])
    from llvm_vecexpr_table_01
    order by 1, 2, 3, 4, 5;

select col_bigint, col_bpchar, col_varchar, col_char, 
        col_bigint = any(array[Null, 12400, 1, 2, -256, 256, 2]),
        col_bpchar in ('AaaA', 'BbbB', 'DddD') 
            or col_bpchar is NULL
            or col_varchar in ('girl', 'boy', Null, 'flower') 
            or col_char = any(array['hangzhou', 'jiangsu']),
        col_text1 in ('window', 'pen') 
            or col_text1 is NULL
            or col_float8 = any(array[3.5, 2.7, 10.15, 25]) 
            and col_bigint in(58964785, -2566, 1345971420) 
            or col_time in ('2012-11-02 00:03:10', '2018-05-09 19:45:37', '2017-10-09 21:45:37')
    from llvm_vecexpr_table_01 
    order by 1, 2, 3, 4, 5, 6, 7;

select A.col_int, A.col_bigint, A.col_num1, a.col_float8, A.col_num1, a.col_date, 
        (A.col_num1 - A.col_int)/A.col_float8 <= A.col_bigint
        and ( substr(A.col_date, 1, 4) in (select substr(B.col_date, 1, 4) 
                                                from llvm_vecexpr_table_02 as B 
                                                ))
    from llvm_vecexpr_table_01 as A 
    order by 1, 2, 3, 4, 5, 6, 7;

-----
---  lpad + substring
-----
select col_text1, col_text2, col_varchar,
        lpad(substring(col_text1, 1, 3), 10, 'wow') = 'wowwowwyou',
        lpad(substring(col_varchar, length(col_varchar)-1, 2), 9, 'kne') = 'kneknekne',
        lpad(col_bpchar, 5, 'abcdef'),
        lpad(col_bpchar, 5, Null)
    from llvm_vecexpr_table_01
    order by 1, 2, 3, 4, 5 ,6;

----
--- to_number
----
select col_float, to_number(col_float) from llvm_vecexpr_table_01 order by 1, 2;
select col_float8, to_numeric(col_float8) from llvm_vecexpr_table_01 order by 1, 2;

----
--- non-strict function
----
select col_num1, col_text1, (col_num1 || col_text1) from llvm_vecexpr_table_01 order by 1, 2, 3;

----
--- do not codegen for system catalog
----
select xc_node_id, col_float from llvm_vecexpr_table_01 order by 1, 2;
select ctid, col_float from llvm_vecexpr_table_01 order by 1, 2;
select xc_node_id, col_float from llvm_vecexpr_table_01 where xc_node_id > -300000000 order by 1, 2;

----
--- enable_codegen_print
----
set enable_codegen_print = on;
select col_float, to_number(col_float) from llvm_vecexpr_table_01 order by 1, 2;
set enable_codegen_print = off;

----
--- text not like
----
select col_int, col_text not like '%2%4%' from llvm_vecexpr_table_02 order by 1, 2;

----
----
select col_varchar, col_text1 from llvm_vecexpr_table_01 where translate(col_varchar, col_text1, col_varchar) = translate(col_varchar, col_text1, col_varchar) order by 1, 2;

----
----
select cast(col_char as interval) from llvm_vecexpr_table_02 order by 1;

----
--- clean table and resource
----
drop schema llvm_target_engine cascade;
