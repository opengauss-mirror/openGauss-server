/* 
 * This file is used to test the function of vecexpression.cpp with LLVM Optimization
 * It is only an auxiliary file to vexecpression.sql to cover the basic functionality 
 * about expression in C compatibility.
 */
/********************************
    T_Var,
    T_Const,
    T_OpExpr,
********************************/
----
--- case : test llvm  with compatibility = 'C'
----
create database tdtest dbcompatibility = 'C';
\c tdtest

create schema llvm_vecexpr_td_engine;
set current_schema = llvm_vecexpr_td_engine;

CREATE TABLE llvm_vecexpr_td_engine.LLVM_VECEXPR_TABLE_01(
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

copy llvm_vecexpr_table_01 from stdin;
1	1	256	11	111	1111	123456	2000-01-01 01:01:01	2000-01-01 01:01:01
2	2	128	24	75698	56789	12345	2017-09-09 19:45:37	2012-11-02 00:03:10
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
-6	-2147483640	-1587452	1112	1115	12548	36589	2014-03-12	1999-2-6 01:00:30
\.

select * from llvm_vecexpr_table_01 where col_date = '2018-03-07' order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_01 where col_date != '2018-03-07' order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_01 where col_date < '2018-03-07' order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_01 where col_date <= '2018-03-07' order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_01 where col_date > '2018-03-07' order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_01 where col_date >= '2018-03-07' order by 1, 2, 3, 4, 5;

select * from llvm_vecexpr_table_01 where case col_date when '2000-01-01' then 2 
									when '2018-03-07' then 4 else 6 end = 4 order by 1, 2, 3, 4, 5;
	
select * from llvm_vecexpr_table_01 where col_date in ('2000-01-01', NULL, '2014-05-12') order by 1, 2, 3, 4, 5;

select * from llvm_vecexpr_table_01 where case when col_char = '11' then '2000-01-01' 
									when col_varchar = '25879' then '2012-11-02' 
									else '2018-03-07' end = '2012-11-02' order by 1, 2, 3, 4, 5; 

----
--- clean table and resource
----
drop schema llvm_vecexpr_td_engine cascade;
\c postgres

