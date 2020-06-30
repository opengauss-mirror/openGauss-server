/*
 * This file is used to test the function of hashagg with LLVM Optimization
 */
----
--- Create Table and Insert Data
----
drop schema if exists llvm_vecagg_engine2 cascade;
create schema llvm_vecagg_engine2;
set current_schema = llvm_vecagg_engine2;
set codegen_cost_threshold=0;

CREATE TABLE llvm_vecagg_engine2.LLVM_VECAGG_TABLE_02(
    col_int1	int,
    col_int2	int,
    col_bint1	bigint,
    col_char1	char(10),
    col_varchar1	varchar(17),
    col_varchar2	varchar(40),
    col_text	text,
    col_num1	numeric(9,2),
    col_num2	numeric(15,4),
    col_num3	numeric(18,0),
    col_num4	numeric(18,4),
    col_num5	numeric(22,2),
    col_num6	numeric(40,5)	
)with(orientation=column);

copy llvm_vecagg_table_02 from stdin;
9	90	223372036854775807	shenmiwu	ali	todayisagoodaygofar	guy	3.6	125.256	25869	5687.125	147.21	65778951.25
1	10	-223372036854775808	shenmiwu	ali 	whydayisnotsogood	guy	21.2	125.2562	1258698	5669.225	145872.21	65778951.75
1	10	2233720368545808	shenmi	alo 	whydayisnotsogood	guy	22	225.2562	857946	2769.755	8958725.9	65778951.75587
1	20	\N	shenmi	alo 	Iknowisgood	guy	75.1	6987.262	857946	2769.80	89758725.9	6985951.75587
1	20	\N	shenmi	alo 	Iknowisgood	guy	936.2	6987.111	817926	1769.20	89756725.9	\N
1	20	\N	shenmi	alo 	Iknowisgood	guy	136.2	6987.111	817926	1769.20	89756725.9	\N
8	20	36	lihai	ali 	Iknowevery	hai	\N	7854784887.111	817926587964	17690000002001.20	5487552145289756725.9	\N
9	10	1235720368774	lihai	ala 	Iknoweoery	ha	\N	4887.111	817926587964	17690000002001.20	5487552145289756725.9	123
9	10	1235720368774	lihai	ala 	Iknoweoery	ha	\N	4887.111	817926587964	\N	5487552145289756725.9	5487
9	10	1235720368774	lihai	ala 	Iknoweoery	ha	\N	4887.111	817926587964	\N	5487552145289756725.9	548736987.205
9	10	123	lihaile	ala 	Iknoweoery	ha	2.7	312887.178	\N	-125.3654	89756725.92	5487
1	-126	123	lihaile	ala  	Iknowyou	ha	-2.7	312887.178	\N	-22225.3654	89756725.92	-258749685478.36254
1	-226	123	lihaile	ala  	Iknowyou	ya	-12.7	\N	\N	-22225.3654	\N	-2587949685478.36254
8	-226	2583	lihaile  	alaalaala  	Iknowyou	ya67y	62.9	\N	58.2	-22225.3654	\N	-2587949685478.36254
8	-226	2583	lihaile  	alaalaala  	Iknowyou	ya67y	62.9	\N	58.2	-22225.3654	\N	-2587949685478.36254
8	-126	2583	lihaile  	alaalaala  	Iknowyou	ya67y	62.9	\N	58.2	-22225.3654	\N	-2587949685478.36254
19	426	1234583	lihaile  	alaalaala  	Iknowyou	ya67y	62.9	\N	223372036854775807	-22225.3654	\N	485587949685478.36254
19	426	1234583	lihaile  	alaalaala  	Iknowyou	ya67y	62.9	\N	223372036854775807	-22225.3654	\N	485587949685478.36254
\.

analyze llvm_vecagg_table_02;

----
--- test1 : test build hash table with date, timestamp, char, varchar
----
select sum(col_num1), substr(col_text, 1, 2), col_varchar2 from llvm_vecagg_table_02 group by substr(col_text, 1, 2), col_varchar2 order by 1, 2;


----
--- test2 : test sum(numeric) & avg(numeric) with different agg level
----
explain (verbose on, costs off) select sum(col_bint1), avg(col_num1), col_text from llvm_vecagg_table_02  group by col_text order by 1, 2;
select sum(col_bint1), avg(col_num1), col_text from llvm_vecagg_table_02  group by col_text order by 1, 2;
select sum(col_bint1), sum(col_num1), col_text from llvm_vecagg_table_02  group by col_text order by 1, 2;
select avg(col_bint1), avg(col_num3), col_varchar1 from llvm_vecagg_table_02  group by col_varchar1 order by 1, 2;
select avg(col_num1), sum(col_num3), sum(col_num6), col_varchar1, col_varchar2 from llvm_vecagg_table_02  group by col_varchar1, col_varchar2 order by 1, 2;
select avg(col_bint1), avg(col_num6), col_text, col_varchar2 from llvm_vecagg_table_02  group by col_text, col_varchar2 order by 1, 2;
select avg(col_bint1), avg(col_num1 + col_num2 - 2), col_varchar2 from llvm_vecagg_table_02  group by col_varchar2 order by 1, 2;
select avg(col_bint1), avg(col_num1 * 1.5 + col_num3 - 2), col_varchar2 from llvm_vecagg_table_02  group by col_varchar2 order by 1, 2;
select sum(col_num1 * 1.5 + col_num3 - 2), col_varchar2 from llvm_vecagg_table_02  group by col_varchar2 order by 1, 2;
select sum(col_num1 * col_num2), col_varchar1 from llvm_vecagg_table_02  group by col_varchar1 order by 1, 2;

explain (verbose on, costs off) select sum(col_bint1), avg(col_num1), col_text from llvm_vecagg_table_02  group by col_text order by 1, 2;
select sum(col_bint1), avg(col_num1), col_text from llvm_vecagg_table_02  group by col_text order by 1, 2;
select avg(col_bint1), avg(col_num3), col_varchar1 from llvm_vecagg_table_02  group by col_varchar1 order by 1, 2;
select avg(col_num1), sum(col_num3), col_varchar2 from llvm_vecagg_table_02  group by col_varchar2 order by 1, 2;
select avg(col_bint1), avg(col_num6), col_text, col_varchar2 from llvm_vecagg_table_02  group by col_text, col_varchar2 order by 1, 2;
select avg(col_bint1), avg(col_num1 + col_num2 + 2), col_varchar2 from llvm_vecagg_table_02  group by col_varchar2 order by 1, 2;
select avg(col_bint1), sum(col_num1 - col_num2 - 1125.00), col_char1 from llvm_vecagg_table_02  group by col_char1 order by 1, 2;

----
--- clean table and resource
----
drop schema llvm_vecagg_engine2 cascade;
