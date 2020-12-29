/*
 * This file is used to test the function of vecexpression.cpp --- test(2)
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
create schema vector_expression_engine_second;
set current_schema=vector_expression_engine_second;


CREATE TABLE vector_expression_engine_second.VECTOR_EXPR_TABLE_09
(
   col_num	numeric(3,0)
  ,col_num2	numeric(10,0)
  ,col_varchar	varchar
  ,col_text	text
)with(orientation=column);

COPY VECTOR_EXPR_TABLE_09(col_num, col_num2, col_varchar, col_text) FROM stdin;
1.23	56789	1234	1.23
1.23	102.45	abcd	1.23
\.

CREATE TABLE vector_expression_engine_second.VECTOR_EXPR_TABLE_10
(
   col_int int
  ,col_dp 	double precision
  ,col_time	time
  ,col_interval	interval
) with (orientation=column);

COPY VECTOR_EXPR_TABLE_10(col_int, col_dp, col_time, col_interval) FROM stdin;
1	10.1	02:15:01	4 day 13:24:56
2	20.2	16:00:52	1 day 17:52:09
3	-30.3	14:20:00	1 day 17:52:09
4	\N	11:00:10	\N
\.


CREATE TABLE vector_expression_engine_second.VECTOR_EXPR_TABLE_11
(
   col_int	int
  ,col_int2	int
  ,col_char	char(10)
  ,col_varchar	varchar  
) with (orientation=column);

COPY VECTOR_EXPR_TABLE_11(col_int, col_int2, col_char, col_varchar) FROM stdin;
1	1	test	test
10	10	atest	atest
2	\N	atest	atest
\N	3	atest	atest
\N	3	\N	atest
\N	3	atest	\N
\N	\N	\N	\N
12	13	12	13
12	13	123	13
\.

CREATE TABLE vector_expression_engine_second.ROW_EXPR_TABLE_12
(
    i1      int,
    i2      int,
    i3      int8,
    c1      char(1),
    c2      char(6),
    n1      numeric(15, 2),
    n2      numeric(16, 2),
    d1      date    
)distribute by hash (i2);

INSERT INTO ROW_EXPR_TABLE_12 VALUES
(1, 2, 1, 'a', 'aabbcc', 1.0, 3.27, '1995-11-01 3:25 pm'),(2, 3, 3, 'b', 'xxbbcc', 1.0, 6.32, '1996-02-01 1:12 pm'),
(10, 11, 4, 'c', 'aacc', 1.0, 2.27, '1995-03-11 4:15 am'),(21, 6, 6, 'd', 'xxbbcc', 1.0, 1.11, '2005-01-21 3:25 pm'),
(21, 6, 6, 'd', 'xxbbcc', 1.0, 1.11, '2005-01-21 3:25 pm'),(21, 6, NULL, 'd', NULL, 1.0, 1.11, '2005-01-21 3:25 pm'),
(21, 5, NULL, 'd', NULL, 1.0, 1.11, '2005-01-21 3:25 pm');

insert into ROW_EXPR_TABLE_12 select * from ROW_EXPR_TABLE_12;    
insert into ROW_EXPR_TABLE_12 select * from ROW_EXPR_TABLE_12;    
insert into ROW_EXPR_TABLE_12 select * from ROW_EXPR_TABLE_12;    
insert into ROW_EXPR_TABLE_12 select * from ROW_EXPR_TABLE_12;  
insert into ROW_EXPR_TABLE_12 select * from ROW_EXPR_TABLE_12;  
insert into ROW_EXPR_TABLE_12 select * from ROW_EXPR_TABLE_12;  
insert into ROW_EXPR_TABLE_12 select * from ROW_EXPR_TABLE_12;  
insert into ROW_EXPR_TABLE_12 select * from ROW_EXPR_TABLE_12;  
insert into ROW_EXPR_TABLE_12 select * from ROW_EXPR_TABLE_12;  
insert into ROW_EXPR_TABLE_12 select * from ROW_EXPR_TABLE_12;  
insert into ROW_EXPR_TABLE_12 select * from ROW_EXPR_TABLE_12;  
insert into ROW_EXPR_TABLE_12 select * from ROW_EXPR_TABLE_12;  

INSERT INTO ROW_EXPR_TABLE_12 VALUES
(2, 3, 5, 'b', 'xxbbcc', 1.0, 6.32, '1995-01-01 3:25 pm'),(10, 11, 3, 'a', 'aacc', 1.0, 2.27, '1996-01-01 3:25 pm'),
(2, 3, 2, 'e', 'xxbbcc', 6.0, 2.32, '1996-01-01 3:25 pm'),(12, 6, 1, 'e', 'xxbbcc', 2.0, 1.21, '1996-01-01 4:25 pm');

CREATE TABLE vector_expression_engine_second.VECTOR_EXPR_TABLE_12
(
    i1      int,
    i2      int,
    i3      int8,
    c1      char(1),
    c2      char(6),
    n1      numeric(15, 2),
    n2      numeric(16, 2),
    d1      date    
)with(orientation=column) distribute by hash (i2);
insert into VECTOR_EXPR_TABLE_12 select * from ROW_EXPR_TABLE_12;
insert into VECTOR_EXPR_TABLE_12 select * from ROW_EXPR_TABLE_12;
insert into ROW_EXPR_TABLE_12 select * from ROW_EXPR_TABLE_12;
insert into VECTOR_EXPR_TABLE_12 select * from ROW_EXPR_TABLE_12;

CREATE TABLE vector_expression_engine_second.VECTOR_EXPR_TABLE_13
(
	col_int		int
   ,col_date	date
   ,col_num		numeric(5,1)
)with(orientation = column) distribute by hash(col_int);

COPY VECTOR_EXPR_TABLE_13(col_int, col_date, col_num) FROM stdin;
1	2015-02-26	1.1
2	2015-02-26	2.3
3	2015-01-26	2.3
4	2015-02-26	10
4	2015-03-26	10
7	2015-04-26	3.6
8	\N	1.2
\N	\N	\N
\.

CREATE TABLE vector_expression_engine_second.VECTOR_EXPR_TABLE_14
(
   a	int
  ,b 	bpchar
  ,c	text
)with(orientation=column) distribute by hash(a);

COPY VECTOR_EXPR_TABLE_14(a, b, c) FROM stdin;
1	  1234   	56358
3	12487	569
4	25  	   36 
6	  098   	587  
0	1234	45678
12	\N	  782  
15	 123	\N
3	890 	23  
12	1245	6589
56	25	8912
6	  89 	569
\N	\N	\N
\N	452	\N
\.

CREATE TABLE vector_expression_engine_second.VECTOR_EXPR_TABLE_15
(
	a	INT,
	b	TIMESTAMP
)WITH(ORIENTATION=COLUMN) DISTRIBUTE BY HASH(a);

INSERT INTO VECTOR_EXPR_TABLE_15 VALUES(1, NULL);
INSERT INTO VECTOR_EXPR_TABLE_15 VALUES(2, '2015-03-10 20:37:10.473294'),(3, '2015-03-10 20:37:10.473294');

analyze vector_expr_table_09;
analyze vector_expr_table_10;
analyze vector_expr_table_11;
analyze vector_expr_table_12;
analyze vector_expr_table_13;
analyze vector_expr_table_14;
analyze vector_expr_table_15;

----
--- test 9: SUBSTRING SUBSTR TRIM Expression 
----
select substring(col_varchar, 0) from vector_expr_table_09 order by 1;
select substring(col_varchar, 0, 2) from vector_expr_table_09 order by 1;
select substr(col_varchar, 0) from vector_expr_table_09 order by 1;
select substr(col_varchar, 0, 2) from vector_expr_table_09 order by 1;
select substring(col_num2, 2) from vector_expr_table_09 order by 1;
select substring(col_num2, 2, 2) from vector_expr_table_09 order by 1;
select substr(col_num2, 2) from vector_expr_table_09 order by 1;
select substr(col_num2, 2, 2) from vector_expr_table_09 order by 1;
select distinct coalesce(trim(trailing '.' from col_text), '') from vector_expr_table_09 order by 1;
select distinct coalesce(trim(trailing '.' from col_num), '') from vector_expr_table_09 order by 1;

----
--- test 10: NULLIF Expression
----
SELECT '' AS Five, NULLIF(A.col_int, B.col_int) AS "NULLIF(A.I, B.I)", NULLIF(B.col_int, 4) AS "NULLIF(B.I,4)" FROM vector_expr_table_10 A, vector_expr_table_11 B ORDER BY 2, 3;
SELECT NULLIF(A.col_time, B.col_int) AS "NULLIF(A.T, B.I)", NULLIF(B.col_int, 4) AS "NULLIF(B.I,4)" FROM vector_expr_table_10 A, vector_expr_table_11 B ORDER BY 1, 2;
SELECT NULLIF(A.col_interval, B.col_int) AS "NULLIF(A.IN, B.I)", NULLIF(B.col_int, 4) AS "NULLIF(B.I,4)" FROM vector_expr_table_10 A, vector_expr_table_11 B ORDER BY 1, 2;
select nullif(col_int, col_int2) from vector_expr_table_11 order by 1;
select nullif(col_char, col_varchar) from vector_expr_table_11 order by 1;
select nullif(col_int,col_varchar) from vector_expr_table_11  where col_int > 10 order by 1;
select nullif(col_int,col_char) from vector_expr_table_11  where col_int > 10 order by 1;
select nullif(col_int2,col_varchar) from vector_expr_table_11  where col_int > 10 order by 1;
select * from vector_expr_table_11 where nullif(col_int,col_int2) > 1 and col_int > 10 order by 1,2,3,4;
select * from vector_expr_table_11 where nullif(col_int,col_char) > 1 and col_int > 10 order by 1,2,3,4;
select * from vector_expr_table_11 where nullif(col_int,col_char) is NULL and col_int > 10 order by 1,2,3,4;
select * from vector_expr_table_11 where nullif(col_int,col_char) is not NULL and col_int > 10 order by 1,2,3,4;

----
--- test 11: DISTINCT Expression
----
select * from vector_expr_table_11 where col_int is distinct from col_int2 order by 1, 2;
select * from vector_expr_table_11 where not(col_int is distinct from col_int2) order by 1, 2;
select * from vector_expr_table_11 where col_char is distinct from col_varchar order by 1, 2;
select * from vector_expr_table_11 t1 inner join vector_expr_table_11 t2 on t1.col_int = t2.col_int where t1.col_int is distinct from t1.col_int2 and t1.col_int > 5 order by 1, 2, 3, 4, 5, 6, 7, 8;
select * from vector_expr_table_11 t1 inner join vector_expr_table_11 t2 on t1.col_int = t2.col_int where t1.col_int is distinct from t2.col_int2 and t1.col_int > 5 order by 1, 2, 3, 4, 5, 6, 7, 8;

----
--- test 12: Agg + Case When
----
select c1, c2, count(i1) ci1, avg((n2 - 1)*n1), sum(n1+n2), sum(i2+2*i1), sum((2)*(n1+2)*(n2-1)), min(n1), max(n1+n2), sum(i3), avg (case 
                    	when i2 > 5 then 3.21+n1
                    	when i1 > 9 then 2.1+n2+n1
                    	when i1 > 2 then 1.2
                    	else 2.22
                    	end) 
  from vector_expr_table_12 where d1 > '1995-12-01'  
  group by c1, c2 order by ci1, c2; 

select c1, c2, count(i1), avg((n2 - 1)*n1), sum(n1+n2), sum(i2+2*i1), sum(i3)
  from vector_expr_table_12 where d1 > '1995-12-01'  
  group by c1, c2 
  having sum((2)*(n1+2)*(n2-1)) > 22.8
     and sum((2)*(n1+2)*(n2-1)) < 2280 
     and avg (case 
                    	when i2 > 5 then 3.21+n1
                    	when i1 > 9 then 2.1+n2+n1
                    	when i1 > 2 then 1.2
                    	else 2.22
                    	end)  > 2.3 order by c1;
                
select i1+i2, min(i1), max(i2+1), min(n1), max(n1+n2), sum(n2) from vector_expr_table_12 group by i1+i2 order by sum(n1), sum(n2);
select c1, count(*) from vector_expr_table_12 group by c1 having sum(n2) + sum(n1)> 16192 order by sum(i1), sum(i2); 
select i3+sum(i1), sum(n2)/count(n2), sum(n1)/count(n1) from vector_expr_table_12 group by i3 order by sum(i2) + i3;
select case when i1+i2> 3 then 2 else 1 end, sum(n1)+sum(n2) from vector_expr_table_12 group by i1, i2 order by case when i1+i2> 3 then 2 else 1 end, sum(i1)+sum(i2);
select c1, c2, count(i1), sum(n1+n2), sum((n2-1)*n1) from vector_expr_table_12 group by c1, c2 having sum(n2) > 3 order by c1, c2;

----
--- test 13: tcol_inteq/ne/ge/gt/le/lt
----
explain (verbose on, costs off) SELECT col_int, col_date, col_num FROM vector_expr_table_13 WHERE (ctid != '(0,1)') and col_int < 4 and col_num > 5 order by 1,2,3;
SELECT col_int, col_date, col_num FROM vector_expr_table_13 WHERE (ctid != '(0,1)') and col_int < 4 and col_num > 5 order  by 1,2,3;
SELECT col_int, col_date, col_num FROM vector_expr_table_13 WHERE (ctid != '(0,1)') order  by 1,2,3;
SELECT col_int, col_date, col_num FROM vector_expr_table_13 WHERE (ctid = '(0,1)') and col_int < 4 and col_num > 5 order  by 1,2,3;
SELECT col_int, col_date, col_num FROM vector_expr_table_13 WHERE (ctid = '(0,1)') order  by 1,2,3;
SELECT col_int, col_date, col_num FROM vector_expr_table_13 WHERE ('(0,1)' = ctid) and col_int < 4 and col_num > 5 order  by 1,2,3;
SELECT col_int, col_date, col_num FROM vector_expr_table_13 WHERE ('(0,1)' = ctid) order  by 1,2,3;

explain (verbose on, costs off) SELECT col_int, col_date, col_num FROM vector_expr_table_13 WHERE (ctid >= '(0,1)') and col_int < 4 and col_num > 5 order by 1,2,3;
SELECT * FROM vector_expr_table_13 WHERE (ctid >= '(0,1)') and col_int < 4 and col_num > 5 order  by 1,2,3;
SELECT * FROM vector_expr_table_13 WHERE (ctid >= '(0,1)') order  by 1,2,3;
SELECT * FROM vector_expr_table_13 WHERE (ctid > '(0,1)') and col_int < 4 and col_num > 5 order  by 1,2,3;
SELECT * FROM vector_expr_table_13 WHERE (ctid > '(0,1)') order  by 1,2,3;

explain (verbose on, costs off) SELECT col_int, col_date, col_num FROM vector_expr_table_13 WHERE (ctid <= '(0,1)') and col_int < 4 and col_num > 5 order by 1,2,3;
SELECT * FROM vector_expr_table_13 WHERE (ctid <= '(0,1)') and col_int < 4 and col_num > 5 order  by 1,2,3;
SELECT * FROM vector_expr_table_13 WHERE (ctid <= '(0,1)') order by 1,2,3;
SELECT * FROM vector_expr_table_13 WHERE (ctid < '(0,1)') and col_int < 4 and col_num > 5 order by 1,2,3;
SELECT * FROM vector_expr_table_13 WHERE (ctid < '(0,1)') order by 1,2,3;

----
--- test 14: lpad, bpcharlen, ltrim/rtrim/btrim
----
select * from vector_expr_table_14 A where lpad(A.b, 2) = '12' order by 1, 2, 3;
select * from vector_expr_table_14 A where A.a > 2 and lpad(A.b, 2) = '12' order by 1, 2, 3;
select * from vector_expr_table_14 A where A.a > 2 and length(A.b) = 1 order by 1, 2, 3;
select lpad(A.b, 15, 'ab') from vector_expr_table_14 A order by 1;
select lpad(A.b, 15, A.c) from vector_expr_table_14 A order by 1;
select lpad(A.b, NULL, A.c) is null from vector_expr_table_14 A order by 1;
select lpad(A.b, 0, A.c) is null from vector_expr_table_14 A order by 1;
select lpad(A.b, -100, A.c) is null from vector_expr_table_14 A order by 1;
select A.a, lpad(A.b, length(A.c), A.c) from vector_expr_table_14 A order by A.a;
select length(b) from vector_expr_table_14 order by 1;
select length(A.b) from vector_expr_table_14 A where A.a > 2 order by 1;
select ltrim(b) from vector_expr_table_14 order by 1;
select rtrim(b) from vector_expr_table_14 order by 1;
select btrim(c) from vector_expr_table_14 order by 1;
select trim(a1), trim(a2) from (select a || '  ' as a1, c || '    ' as a2 from vector_expr_table_14 order by 1, 2) order by 1, 2;
--estimate selectivity for special expression
select A.b,A.c from vector_expr_table_14 A  join vector_expr_table_14 B on  A.c = rpad(B.c, 3) ORDER BY 1,2;
select A.b,A.c from vector_expr_table_14 A  join vector_expr_table_14 B on  lpad(A.c,2) = rpad(B.c,2) ORDER BY 1,2;
--unsupport estimate selectivity for expression
select A.b,A.c from vector_expr_table_14 A where COALESCE(A.c, A.b) = '569' ORDER BY 1,2;
--semi join
select A.b,A.c from vector_expr_table_14 A where ltrim(A.c, '123') in (select B.c from vector_expr_table_14 B) ORDER BY 1,2;
set cost_param=1;
--not inner join
select A.b,A.c from vector_expr_table_14 A join vector_expr_table_14 B on A.c != trim(B.c) ORDER BY 1,2 limit 5;
--not semi join
select A.b,A.c from vector_expr_table_14 A where not exists (select * from vector_expr_table_14 B where A.c != trim(B.c)) ORDER BY 1,2;
reset cost_param;

----
--- test 15: vtextne
----
SELECT a, b, DATE_TRUNC('day', b) <> a FROM vector_expr_table_15 ORDER BY 1;
SELECT a, b, DATE_TRUNC('day'::TEXT, b)::TEXT <> a::TEXT FROM vector_expr_table_15 ORDER BY 1;

----
--- test 16: A db like lpad & rpad
----
select length(lpad('',-10,'*'));
select length(lpad('1',-10,'*'));
select length(rpad('',-10,'*'));
select length(rpad('1',-10,'*'));


----
--- Clean Resource and Tables
----
drop schema vector_expression_engine_second cascade;

