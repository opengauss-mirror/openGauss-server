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
drop schema if exists llvm_target_engine3 cascade ;
create schema llvm_target_engine3;
set current_schema = llvm_target_engine3;
set time zone 'PRC';
set codegen_cost_threshold=0;

CREATE TABLE llvm_target_engine3.LLVM_VECEXPR_TABLE_05(
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

CREATE TABLE llvm_target_engine3.LLVM_VECEXPR_TABLE_06(
    col_int	int not null,
    col_num1	numeric(9,0),
    col_num2	numeric(18,18),
    col_num3	numeric(20,2),
    col_num4	numeric(20,18),
    col_num5	numeric(21,20)
)with(orientation=column);

copy LLVM_VECEXPR_TABLE_06 from stdin;
1	123	0.98	12.21	5.6845	2.112
2	245	0.75	6.5	7.1245	6.336
3	45	\N	3.65	4.25	9.612
4	758	0.64	2.36	69.32	1.11
5	\N	0.67	78	\N	2.22
9	6897	0.35	65.2	1.24	\N
\.

CREATE TABLE llvm_target_engine3.LLVM_VECEXPR_TABLE_07(
    col_int	int not null,
    col_text	text
)with(orientation=column);

copy LLVM_VECEXPR_TABLE_07 from stdin;
1	abcdquiABCD
2	56789ddJKLI
3	UYHJdgdgsg89uijhhIJK
4	gdgbfasdjgklquitl$dg
5	ABCDSDJK987Udag
\.

analyze llvm_vecexpr_table_05;
analyze llvm_vecexpr_table_06;
analyze llvm_vecexpr_table_07;

-----
---  orc compatability with substr , rtrim , btrim
-----
select col_char1, col_char2,
        rtrim(col_char1),
        btrim(col_char2),
        rtrim(col_char1) is null and substr(col_char2, 1, 2) is not null
    from llvm_vecexpr_table_05
    order by 1, 2, 3, 4, 5;
select col_varchar1, col_varchar2, col_text,
        btrim(col_varchar2),
        rtrim(col_text)
    from llvm_vecexpr_table_05
    order by 1, 2, 3, 4, 5;

-----
---  nulltest + bool expr + booleantest + opCodgen + funCodegen
-----
select col_num1, col_num1 < 500, col_num2 >= 0.75 from llvm_vecexpr_table_06 order by 1, 2, 3;

-----
---  lpad + substring
-----
select col_int, substr(col_text, 3, 5) from llvm_vecexpr_table_07 order by 1, 2;
select col_int, substr(col_text, 7, 2) from llvm_vecexpr_table_07 order by 1, 2;
----
--- concat
----
select col_char1||col_char2 from llvm_vecexpr_table_05 order by 1;
select length(col_varchar1||col_varchar2) from llvm_vecexpr_table_05 order by 1;

----
----
select col_int, col_num2 + 9 from llvm_vecexpr_table_06 order by 1, 2;
select col_int, col_num2 - (-9) from llvm_vecexpr_table_06 order by 1, 2;

----
--- clean table and resource
----
drop schema llvm_target_engine3 cascade;

----
--- create database with encoding SQL-ASCII
----
create database asciitest template template0 encoding 'SQL_ASCII' lc_ctype 'C' lc_collate 'C';
\c asciitest
create schema llvm_vectar_ascii_engine;
set current_schema = llvm_vectar_ascii_engine;

CREATE TABLE LLVM_VECTAR_TABLE_01(
    col_int	int not null,
    col_text	text
)with(orientation=column);

copy LLVM_VECTAR_TABLE_01 from stdin;
1	abcddgaABCD
2	56789$J*KLI
3	UYHJ*&^89uijhhIJK
4	1234djgklquitl$dg
5	ABCDSDJK987Ugdss
\.

select col_int, substr(col_text, 3, 6) from LLVM_VECTAR_TABLE_01 order by 1;
select col_int, substr(col_text, 5, 2) from LLVM_VECTAR_TABLE_01 order by 1;
select col_int, substr(col_text, 6, 3) from LLVM_VECTAR_TABLE_01 order by 1;
select col_int, substr(col_text, 11, 3) from LLVM_VECTAR_TABLE_01 order by 1;
select col_int, substr(col_text, 12, 5) from LLVM_VECTAR_TABLE_01 order by 1;

drop schema llvm_vectar_ascii_engine cascade;
\c postgres
