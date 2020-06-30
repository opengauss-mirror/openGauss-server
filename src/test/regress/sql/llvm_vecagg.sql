/*
 * This file is used to test the function of hashagg with LLVM Optimization
 */
----
--- Create Table and Insert Data
----
drop schema if exists llvm_vecagg_engine cascade;
create schema llvm_vecagg_engine;
set current_schema = llvm_vecagg_engine;
set codegen_cost_threshold=0;

CREATE TABLE llvm_vecagg_engine.LLVM_VECAGG_TABLE_01(
    col_int1	int,
    col_int2	int,
    col_bint	bigint,
    col_date1	timestamp,
    col_date2	date,
    coL_char1	char(1),
    col_bpchar1	bpchar(3),
    col_bpchar2	bpchar(7),
    col_bpchar3	bpchar(15),
    col_bpchar4	bpchar(24),
    col_varchar1	varchar(5),
    col_varchar2	varchar(8),
    col_varchar3	varchar(11),
    col_varchar4	varchar(23)
)with(orientation=column);

copy llvm_vecagg_table_01 from stdin;
1	1	25678	2017-11-04 00:00:00	1985-11-05 12:15:00	A	BAT	BAT U	TIANJINGOP	Whatsupforyou	about	justforu	egunmanyin	zenmecainengkey
1	1	25678	2017-11-04 00:00:00	1985-11-05 12:15:00	A	BAT	BAT U	TIANJINGOP	Whatsupforyou	about	justforu	egunmanyin	zenmecainengkey  
2	2	1258	2014-12-12	1965-12-24 15:00:03	B	Ali	AliGo	Alpha	TIANJINGOP	about	justforu	egunmanyin	zenmecainengkey
2	2	1258	2014-12-12	1965-12-24 15:00:03	B	Ali	AliGo	Alpha	TIANJINGOP	about	justforu	egunman  	zenmecainengkey
2	2	1258	2014-12-12	1965-12-24 15:00:03	B	Ali	AliGo	Alpha   	TIANJINGOP	about	justforu	egunman	zenmecainengkey       
1	1	25678	2017-11-04 00:00:00	1985-11-05 12:15:00	B	BAT	BAT U 	TIANJINGOP 	becauseofyou	see	justforu	egunmanyin	zenmecainengkey  
1	1	25678	2017-11-04 00:00:00	1985-11-05 12:15:00	\N	BAT	BAT U 	GUIDAOZHEZHAY 	becauseofyou	see	justforu	egunmanyin	zenmecainengkey  
90	5	256785869	\N	1985-11-05 12:15:00	\N	BAT	BAT U 	GUIDAOZHEZHAY 	becauseofyouforyou	see	justforu	egunmanyin	zenmecainengkey  
90	5	\N	2014-12-12	1985-11-05 12:15:00	\N	BAT	BAT U 	GUIDAOZHEZHAY  	\N	see	\N	egunmanyin	zenmecainengkey  
90	5	\N	2014-12-12	1985-11-05 12:15:00	C	Gao	BAT U 	GUIDAOZHEZHAY  	GUIDAOZHEZHAYEKESHIHAO	see	justfor	egunmanyin	\N
2	1	1234525678	2017-11-04 00:00:00	1965-12-24 15:00:03	A	BAT	AliGo	\N	Whatsupforyou	about	justfor	egunmanyin	todayisgood
90	1	1234525678	2017-11-04 00:00:00	1965-12-24 15:00:03	C	DAY	AliGo	\N	Whatsupforyou	about	justforu	egunman  	todayisgood
90	1	1234525678	2017-11-04 00:00:00	1965-12-24 15:00:03	C	DAY	AliGo	\N	Whatsupforyou	about	\N	egunman  	todayisgood
24	5	1234525678	2017-11-04 10:10:00	\N	C	DAY	Ali	\N	Whatsupforyou	about	\N	egunman  	\N
24	5	1234525678	2017-11-04 10:10:00	1965-12-24 15:00:03	C	\N	Ali	\N	Whatsupforyou	about	see	egu  	\N
24	5	1234525678	2017-11-04 10:10:00	1965-12-24 15:00:03	C	\N	Ali	\N	Whatsupforyou	about	iam	egu  	todayisgood
12	5	1234525678	2017-11-04 10:10:00	1965-12-24 15:00:03	C	\N	Ali	\N	Whatsupforyou	xinyu	iam	klu  	todayisgood
12	5	1234525678	2017-11-04 10:10:00	1965-12-24 15:00:03	C	\N	Ali	\N	Whatsupforyou	about	iam	klu  	todayisgood
\.

analyze llvm_vecagg_table_01;

----
--- test1 : test build hash table with date, timestamp, char, varchar
----
explain (verbose on, costs off) select sum(col_bint), col_bpchar1 from llvm_vecagg_table_01 group by col_bpchar1;
select sum(col_bint), col_bpchar1 from llvm_vecagg_table_01 group by col_bpchar1 order by 1, 2;
select sum(col_bint), col_bpchar2 from llvm_vecagg_table_01 group by col_bpchar2 order by 1, 2;
select sum(col_bint), length(col_bpchar3), col_bpchar3 from llvm_vecagg_table_01 group by col_bpchar3 order by 1, 2, 3;
select avg(col_bint), length(col_bpchar4), col_bpchar4 from llvm_vecagg_table_01 group by col_bpchar4 order by 1, 2;
select count(*), length(col_varchar1), col_varchar1 from llvm_vecagg_table_01 group by col_varchar1 order by 1, 2;
select count(*), length(col_varchar2), col_varchar2 from llvm_vecagg_table_01 group by col_varchar2 order by 1, 2;
select count(*), length(col_varchar3), col_varchar3 from llvm_vecagg_table_01 group by col_varchar3 order by 1, 2;
select sum(col_int2), length(col_varchar4), col_varchar4 from llvm_vecagg_table_01 group by col_varchar4 order by 1, 2;
select sum(col_bint), col_date1 from llvm_vecagg_table_01 group by col_date1 order by 1, 2;
select sum(col_bint), col_date2 from llvm_vecagg_table_01 group by col_date2 order by 1, 2;

--- group by substr
select sum(col_bint), substr(col_bpchar4, 1, 5) from llvm_vecagg_table_01 group by substr(col_bpchar4, 1, 5) order by 1, 2;
select sum(col_int2), substr(col_varchar2, 1, 7) from llvm_vecagg_table_01 group by substr(col_varchar2, 1, 7) order by 1, 2;

----
--- clean table and resource
----
drop schema llvm_vecagg_engine cascade;
