/*
 * This file is used to test the function of hashagg with LLVM Optimization
 */
----
--- Create Table and Insert Data
----
drop schema if exists llvm_vecagg_engine3 cascade;
create schema llvm_vecagg_engine3;
set current_schema = llvm_vecagg_engine3;
set codegen_cost_threshold=0;

CREATE TABLE llvm_vecagg_engine3.LLVM_VECAGG_TABLE_03(
    col_int	int,
    col_bint	bigint,
    col_vchar1	varchar(7),
    col_vchar2	varchar(16),
    col_num1	numeric(17,5),
    col_num2	numeric(18,0),
    col_num3	numeric(18,2),
    col_num4	numeric(19,4)
)with(orientation=column);

copy llvm_vecagg_table_03 from stdin;
1	123	I know	Youhavemyheartfo	125.87589	223372036854775806	2233720368547758.05	922337203685477.5805
1	2587963	be far	Youhavemyheartwo	360287970189.63967	823372036854775805	9223372036854775.04	199223372036853.7801
1	258	I know	Youhavemyheartfi	23602879709.63967	12233720368547758	2223372036854775.04	399223372036853.9801
2	-458	be far	youhaveyourheart	98125.87589	-20368547758	9758.05	258.1425
2	-345	Know	youhaveyourheart	-360287970189.63968	20368547758	-9758.05	-898771145265875.2145
2	9875	Know	youhaveyourheart	-360287970189.63968	-923720368547758098	-9758.05	-9987711452875.2145
2	6987	beword	youhaveyourheart	-260287970189.63968	-923372036854775801	-9758.04	-1987711452875.2145
2	698754	beword	youhaveyourpan	-26028797018	-22233720368547758	-923720368547758.06	-258987711452875.2145
2	87549	know	youhaveyourpan	232879701892.63967	12233720368547758	-922330368527758.06	28987711452875.2145
36	8	know	justforfun	98125.87589	12233720368	1000000000000000.01	100000000000000.0001
-56	4	sweet	keepin mind 	56987.256	222337203685475805	-1000000000000000.00	-15420000000000.0000
67	9223372	sweet	keepin body	-15478.256	223372036854775806	256874.3648896	-154200000000000.36
6985	4875	people	keepin body	4587962145.36	922337203685473807	-7895	-1540.0214
6	8759	people	letsgo	0.0	0	-0.1	-0.0000
\N	548	\N	letsgo	-0.0	-0	0.01	-0.0001
8	\N	kindle	\N	0.1	1	0.02	6.3564
-74	69857	kindle	keepin body	3.6	-1	0.25	\N
52	87	people	\N	\N	\N	41	20
25	\N	\N	Youhavemyheartwo	0.00000	-2	\N	-20.1
458	7	kindle	keep	7.25412	\N	18.02	180.26
\.

analyze llvm_vecagg_table_03;

----
--- test1 : test build hash table with date, timestamp, char, varchar
----
--- group by having
select col_bint, col_int from llvm_vecagg_table_03 group by col_bint, col_int having sum((-1.20000)) <= -1 order by 1, 2;
select col_bint from llvm_vecagg_table_03 group by col_bint having sum(col_num1) > 25836.98 order by 1;

----
--- test2 : test sum(numeric) & avg(numeric) with change
----
select sum(col_bint), sum(col_num1), sum(col_num2) from llvm_vecagg_table_03 group by col_int order by 1, 2, 3;
select sum(col_num3), avg(col_num4) from llvm_vecagg_table_03 group by col_vchar2 order by 1, 2;
select avg(col_num2), sum(col_num4), col_int from llvm_vecagg_table_03 group by col_int, col_vchar1 order by 3, 1;
select avg(col_num4), avg(col_num3), col_int from llvm_vecagg_table_03 group by col_int order by 1, 2;
select sum(col_num2), sum(col_num3), sum(col_num4) from llvm_vecagg_table_03 group by col_int, col_vchar2 order by 1, 2, 3;
select avg(col_num1), sum(col_num2) from llvm_vecagg_table_03 group by col_vchar1, col_vchar2 order by 1, 2;
select avg(col_num4 * 2.005), sum(col_num2  + col_num3), col_vchar1, col_vchar2 from llvm_vecagg_table_03 group by col_vchar1, col_vchar2 order by 1, 2, 3, 4;
select sum(col_num2 -(-200000.005)), sum(col_num4  + col_num3), col_vchar1, col_vchar2 from llvm_vecagg_table_03 group by col_vchar1, col_vchar2 order by 1, 2;
select sum(col_num4 -(-200000.005) + col_num1), avg(col_num1 * 2 + col_num4  - col_num3), col_int from llvm_vecagg_table_03 group by col_int order by 1, 2, 3;

----
--- test4 : test print hash conflict case
----
set analysis_options="on(HASH_CONFLICT)";
select sum(col_num3), avg(col_num3-(-0.25)) from llvm_vecagg_table_03 group by col_int order by 1, 2;
reset analysis_options;

----
--- test5 : test distinct in aggregate functions
----
select col_int, max(distinct col_num1 order by col_num1), min(col_num2 order by col_num2) from llvm_vecagg_table_03 group by col_int order by 1, 2, 3;

----
--- clean table and resource
----
drop schema llvm_vecagg_engine3 cascade;
