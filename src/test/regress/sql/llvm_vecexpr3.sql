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
drop schema if exists llvm_vecexpr_engine3 cascade ;
create schema llvm_vecexpr_engine3;
set current_schema = llvm_vecexpr_engine3;
set codegen_cost_threshold=0;

CREATE TABLE llvm_vecexpr_engine3.LLVM_VECEXPR_TABLE_04(
	col_bool1 bool,
	col_bool2 bool,
	col_int int,
	col_money	money,
	col_real	real,
	col_decimal	decimal,
	col_timetz	time with time zone,
	col_tstz	timestamp with time zone,
	col_interval	interval,
	col_intervaltz	tinterval
)WITH(orientation=column);

COPY LLVM_VECEXPR_TABLE_04(col_bool1, col_bool2, col_int, col_money, col_real, col_decimal, col_timetz, col_tstz, col_interval, col_intervaltz) FROM stdin;
1	0	1	1.2	3.62	3.62	12:34:48.124588+08	2013-01-04 11:27:07+08	1 day 12:34:56	["1947-05-10 23:59:12+08" "1973-01-14 03:14:21+08"]
1	0	2	3.26	7.81	2.548	00:34:48.124568+08	1986-2-6 00:00:30+8	4 days 12:34:56	["1937-05-10 23:59:12+08" "1977-01-14 03:14:21+08"]
1	0	\N	4.75	3.24	3.69	12:30:08.124568+08	1916-2-6 00:10:30+8	3 days 12:34:56	["1937-05-10 23:59:12+08" "2001-01-14 15:14:21+08"]
0	1	2	1.1	3.78	95.69	12:31:13.210195+08	2015-2-6 10:00:30+8	6 days 12:34:56	["1937-06-11 23:59:12+08" "2001-11-14 15:14:21+08"]
0	1	\N	\N	\N	47.12	12:27:55.563043+08	1966-2-6 00:00:30+8	3 days 12:34:56	["1947-06-11 23:59:12+08" "2011-11-14 15:14:21+08"]
0	1	3	15.00	2.67	7.56	12:20:28.683875+08	1997-12-6 10:00:30+8	4 days 11:34:56	["1947-06-11 23:59:12+08" "2007-12-14 15:14:21+08"]
0	\N	1	25	1.25	\N	\N	2011-02-6 10:00:30+8	6 days 12:34:56	["1917-06-11 00:59:12+08" "2017-12-14 15:14:21+08"]
0	0	7	6.36	2.25	6.69	12:07:15.563043+08	2011-02-6 10:00:30+8	6 days 12:34:56	["1917-06-11 00:59:12+08" "2017-12-14 15:14:21+08"]
\N	1	9	69.76	6.36	12.58	07:31:13.210195+08	2014-06-03 10:00:30+8	2 days 07:34:56	["1927-06-11 00:59:12+08" "1956-12-14 13:14:21+08"]
1	\N	10	4.51	-0.124	-6.34	12:34:48.124588+08	2013-01-04 11:27:07+08	1 day 12:34:56	["1947-05-10 23:59:12+08" "1973-01-14 03:14:21+08"]
true	false	15	-0.145	9.687452114	69.30	10:34:48.124588+08	2014-01-04 11:27:07+08	2 day 12:34:56	["1947-05-10 23:59:12+08" "1969-02-28 03:14:21+08"]
true	true	27	2.458	-1.24568	-69	11:34:48.124588+08	2013-08-06 11:27:07+08	7 days 07:34:56	["1927-05-15 23:50:12+08" "1973-01-14 03:14:21+08"]
true	\N	36	\N	3.69	2.589	12:30:08.124568+08	1916-2-6 00:10:30+8	3 days 12:34:56	["1937-05-10 23:59:12+08" "2001-01-14 15:14:21+08"]
false	false	69	6.98	6.98	2.56	12:31:08.124568+08	1916-2-6 00:10:30+8	3 days 12:34:56	["1936-05-10 23:59:12+08" "2011-01-15 15:14:21+08"]
false	true	\N	69.8	5.58	\N	12:31:08.134568+08	\N	6 days 12:34:56	["1917-06-11 00:59:12+08" "2017-12-14 15:14:21+08"]
false	\N	57	3.25	6.45	-0.21	12:32:48.124589+06	2011-03-6 10:00:30+8	\N	["1917-06-11 00:59:12+08" "2007-12-14 15:14:21+08"]
\N	\N	\N	\N	\N	\N	\N	\N	\N	\N
\N	false	36	3.68	1.25	5.25	\N	2011-04-06 12:00:30+8	8 days 17:34:56	["1937-06-11 00:59:12+08" "1999-12-14 15:14:21+08"]
\N	true	45	\N	3.69	-25	11:30:08.124568+08	1906-3-6 00:10:30+8	1 days 17:34:56	\N
\.

CREATE TABLE llvm_vecexpr_engine3.LLVM_VECEXPR_TABLE_05(
	col_int		int,
	col_num0	numeric,
	col_num1	numeric(10,2),
	col_num2	numeric(18,8),
	col_num3	numeric(39,0),
	col_num4	numeric(39,29)
)with(orientation=column);

copy LLVM_VECEXPR_TABLE_05(col_int, col_num0, col_num1, col_num2, col_num3, col_num4) FROM stdin;
1	1.253656	256.45	-12039.05282747	-170141183460469231731687303715884105728	-93.0222221292000000000000000000001
2	25488966.5789	1.25	-396.35555516	170141183460469231731687303715884105727	 1000000107.7058
\N	3.68	7859635.38	1000002653.66111	9223372036854775807	1000000102.62778000000000000000002
3	6.58000000000000000000006	6589.36	-1473.97530717	-9223372036854775808	-74.3728394318
5	\N	2.24	0.0	9876543249	-96.30271595308
95	5.21	\N	-396.35555516	170141183460469231731687303715884105727	-1.78629629451
24	-1.258	27.36	\N	-170141183460469231731687303715884105728	-33.66999996633
8	10.0000000000000025	12.25	1000000705.18259	\N	1000000042.88235
20	-1589	-2.52	-1726.43851679	8297	\N
\.

analyze llvm_vecexpr_table_04;
analyze llvm_vecexpr_table_05;
----
--- case 1 : arithmetic operation
----
select * from llvm_vecexpr_table_05 where col_num2 + col_num3 > col_num3 - col_num4 order by 1;
select * from llvm_vecexpr_table_05 where col_num2 * col_num4 > col_num3 order by 1;
select * from llvm_vecexpr_table_05 where col_num0 - col_num4 < col_num2 / col_num0 order by 1;
select * from llvm_vecexpr_table_05 where (col_num3 + col_num1) / col_num2 <= 152487546852.6589 order by 1;
select * from llvm_vecexpr_table_05 where col_num1 = 1.25 or col_num2 < 25.0 order by 1;
select * from llvm_vecexpr_table_05 where col_num2 <= 12.25 and 3654.78 <= col_num1 order by 1;
select * from llvm_vecexpr_table_05 where col_num1 > 25.0 or col_num1 > 10.0  order by 1;
select * from llvm_vecexpr_table_05 where col_num1 != 9223372036854775807 and col_num2 >= 10.0  order by 1;
select * from llvm_vecexpr_table_05 where col_num1 = 1.25 or -1726.43851679 = col_num2 order by 1;
select * from llvm_vecexpr_table_05 where 12 > col_num2 and -400.00 > col_num2 order by 1;
select * from llvm_vecexpr_table_05 where 25.25 >= col_num1 and col_num2 >= -111110.0000001 order by 1;

----
--- case 2 : basic comparison operation
----
select * from llvm_vecexpr_table_04 where col_tstz + col_interval > '2010-12-30 00:00:00+08' order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_05 where col_num3 > col_num0 order by 1;
select * from llvm_vecexpr_table_05 where col_num3 = 170141183460469231731687303715884105727 order by 1;
select * from llvm_vecexpr_table_05 where col_num2 <= col_num1 order by 1;
select * from llvm_vecexpr_table_05 where col_num4 != col_num0 order by 1;
select * from llvm_vecexpr_table_05 where col_num1 > col_num2 * col_num4 order by 1;

---boolean test with input type boolean
select * from llvm_vecexpr_table_04 where col_bool1 is true order by 1, 2, 3;
select * from llvm_vecexpr_table_04 where col_bool1 is not true order by 1, 2, 3;
select * from llvm_vecexpr_table_04 where col_bool1 is false order by 1, 2, 3;
select * from llvm_vecexpr_table_04 where col_bool1 is not false order by 1, 2, 3;
select * from llvm_vecexpr_table_04 where col_bool1 is unknown order by 1, 2, 3;
select * from llvm_vecexpr_table_04 where col_bool1 is not unknown order by 1, 2, 3;
---boolean test with input type int
select * from llvm_vecexpr_table_04 where col_int is true order by 1, 2, 3;
select * from llvm_vecexpr_table_04 where col_int is not true order by 1, 2, 3;
select * from llvm_vecexpr_table_04 where col_int is false order by 1, 2, 3;
select * from llvm_vecexpr_table_04 where col_int is not false order by 1, 2, 3;
select * from llvm_vecexpr_table_04 where col_int is unknown order by 1, 2, 3;
select * from llvm_vecexpr_table_04 where col_int is not unknown order by 1, 2, 3;
select * from llvm_vecexpr_table_04 where col_interval is null order by 1, 2, 3;

select * from llvm_vecexpr_table_04 where case col_real when 1.25 then case when col_timetz = '00:34:48.124568+08' then 12 
																			when col_timetz = '07:31:13.210195+08' then 25
																			else 27 end
														when 3.69 then case when col_interval = '1 day 17:34:56' then 37
																			when col_interval = '6 days 12:34:56' then 49
																			else 50 end
														else 19 end > 35;

select * from llvm_vecexpr_table_04 where case when col_tstz < '1950-01-01 00:00:01+8' then false
                                                when col_tstz < '1990-01-01 00:00:01+8' then true
                                                when col_tstz < '2000-01-01 00:00:01+8' then false
                                                when col_tstz < '2012-01-01 00:00:01+8' then true
                                                else false end order by 1, 2, 3, 4, 5;
----
--- case 4 : ArrayOp Expr - int4eq, int8eq, float4eq, float8eq, bpchareq, texteq, dateeq, substr with texteq, timetz
----
select col_bool1, col_real, col_interval from llvm_vecexpr_table_04 where col_bool1 in (1, 0, true, false, NULL) and col_interval in ('3 days 12:34:56', '6 days 12:34:56', '1 day 12:34:56') order by 1, 2, 3;

select col_real, col_timetz from llvm_vecexpr_table_04 where col_real > any(array[1.2, 0.75, 3.69, NULL]) or col_timetz in ('12:34:48.124588+08', '07:31:13.210195+08', '12:30:08.124568+08') order by 1, 2;

----
---- case 5 :bool and /or /not
----
select col_bool1 and col_bool2 and not col_int from llvm_vecexpr_table_04 where col_bool1 and col_bool2 and (not col_int) order by 1, 2, 3;

select * from llvm_vecexpr_table_04 where col_bool1 and col_bool2 order by 1, 2, 3;

select col_bool1, col_bool2 from llvm_vecexpr_table_04 where col_bool1 or (not col_bool2) order by 1, 2;

select col_bool1, col_bool2, col_int from llvm_vecexpr_table_04 where not col_bool1 and not col_bool2 and col_int order by 1, 2, 3;

----
--- case 6 : Nullif
----
select * from llvm_vecexpr_table_04 where nullif(col_timetz, '12:30:08.124568+08') is NULL order by 1, 2, 3, 4, 5;

select * from llvm_vecexpr_table_04 where nullif(col_tstz, '2015-02-06 10:00:30+08') is NULL order by 1, 2, 3, 4, 5;

select col_int, col_real, col_decimal from llvm_vecexpr_table_04 where nullif(col_real, col_decimal) is not NULL order by 1, 2, 3;

select col_int, col_intervaltz from llvm_vecexpr_table_04 where nullif(col_intervaltz, '["1937-06-11 23:59:12+08" "2001-11-14 15:14:21+08"]') is not NULL order by 1, 2;

----
--- clean table and resource
----
drop schema llvm_vecexpr_engine3 cascade ;