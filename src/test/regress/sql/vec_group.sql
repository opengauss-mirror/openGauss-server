/*
 * This file is used to test the function of ExecVecGroup()
 */
----
--- Create Table and Insert Data
----
create schema vector_group_engine;
set current_schema=vector_group_engine;
set enable_hashagg=off;

create table vector_group_engine.ROW_GROUP_TABLE_01
(
	col_int0	int4
   ,col_int		int
   ,col_bint	bigint
   ,col_serial	int	
   ,col_char	char(25)
   ,col_vchar	varchar(35)
   ,col_text	text
   ,col_num		numeric(10,4)
   ,col_decimal	decimal
   ,col_float	float
   ,col_date	date
   ,col_time	time
   ,col_timetz	timetz
   ,col_interval	interval
   ,col_tinterval	tinterval
)distribute by hash(col_int);

create table vector_group_engine.VECTOR_GROUP_TABLE_01
(
	col_int0	int4
   ,col_int		int
   ,col_bint	bigint
   ,col_serial	int	
   ,col_char	char(25)
   ,col_vchar	varchar(35)
   ,col_text	text
   ,col_num		numeric(10,4)
   ,col_decimal	decimal
   ,col_float	float
   ,col_date	date
   ,col_time	time
   ,col_timetz	timetz
   ,col_interval	interval
   ,col_tinterval	tinterval
)with(orientation=column) distribute by hash(col_int);

insert into ROW_GROUP_TABLE_01 values(1, 10, 100, 2147483647, 'aa', 'aaaaaa', 'tiananmen', 9.01, 10.01, 1100.01, '2015-02-14', '16:00:38', '1996-2-6 01:00:30+8', '2 day 13:24:56','["Sep 4, 1983 23:59:12" "Oct4, 1983 23:59:12"]');
insert into ROW_GROUP_TABLE_01 values(11, 20, 100, -2147483647, 'aa', 'gggggg', 'xierqi', 7.01, 10.01, 1100.01, '2015-03-14', '16:02:38', '1996-2-8 01:00:30+8', '2 day 13:56:56','["May 10, 1947 23:59:12" "Jan 14, 1973 03:14:21"]');
insert into ROW_GROUP_TABLE_01 values(1, 30, 100, -2146483647, 'bb', 'aaaaaa', 'xierqi', 0.01, 10.01, 100.01, '2015-04-15', '16:02:38', '1996-2-6 01:00:30+8', '2 day 13:24:56','["May 10, 1947 23:59:12" "Jan 14, 1973 03:14:21"]');
insert into ROW_GROUP_TABLE_01 values(11, 40, 100, 2147483647, 'bb', 'aaaaaa', 'tiananmen', 0.04, 10.01, 100.01, '2015-05-16', '16:00:38', '1996-2-8 01:00:30+8', '2 day 13:24:56','["May 10, 1947 23:59:12" "Jan 14, 1973 03:14:21"]');
insert into ROW_GROUP_TABLE_01 values(1, 50, 200, 2119483647, 'cc', 'aaaaaa', 'wangfujing', 7.01, 10.11, 100.01, '2015-02-18', '16:00:38', '1996-2-6 01:00:30+8', '2 day 13:56:56','["Sep 4, 1983 23:59:12" "Oct4, 1983 23:59:12"]');
insert into ROW_GROUP_TABLE_01 values(11, 10, 100, 2105788455, 'dd', 'hhhhhh', 'wangfujing', 9.01, 10.01, 100.01, '2015-08-09', '16:00:38', '1996-2-8 01:00:30+8', '2 day 13:24:56','["Sep 4, 1983 23:59:12" "Oct4, 1983 23:59:12"]');
insert into ROW_GROUP_TABLE_01 values(1, 20, 100, 1158745898, 'dd', 'aaaaaa', 'tiananmen', 0.01, 10.01, 100.01, '2015-10-06', '16:05:38', '1996-2-6 01:00:30+8', '8 day 13:24:56','["Sep 4, 1983 23:59:12" "Oct4, 1983 23:59:12"]');
insert into ROW_GROUP_TABLE_01 values(11, 30, 100, 1198754521, 'ee', 'ffffff', 'xierqi', 0.08, 10.01, 1100.01, '2015-12-02', '16:05:38', '1996-2-6 01:00:30+8', '2 day 13:24:56','["Sep 4, 1983 23:59:12" "Oct4, 1983 23:59:12"]');
insert into ROW_GROUP_TABLE_01 values(1, 40, 200, -1246521526, 'ee', 'aaaaaa', 'wangfujing', 0.08, 10.01, 1100.01, '2015-06-16', '16:00:38', '1996-2-8 01:00:30+8', '10 day 13:24:56','["Feb 10, 1947 23:59:12" "Jan 14, 1973 03:14:21"]');
insert into ROW_GROUP_TABLE_01 values(11, 50, 100, 2024856154, 'gg', 'ffffff', 'tiananmen', 0.01, 10.11, 100.01, '2015-05-20', '16:00:38', '1996-2-6 01:00:30+8', '2 day 13:24:56','["Feb 10, 1947 23:59:12" "Jan 14, 1973 03:14:21"]');

insert into vector_group_table_01 select * from row_group_table_01;

CREATE TABLE vector_group_engine.row_group_table_02(
 C_CHAR_1 CHAR(1),
 C_CHAR_2 CHAR(10),
 C_CHAR_3 CHAR(100),
 C_VARCHAR_1 VARCHAR(1),
 C_VARCHAR_2 VARCHAR(10),
 C_VARCHAR_3 VARCHAR(1024),
 C_INT BIGINT,
 C_BIGINT BIGINT,
 C_SMALLINT BIGINT,
 C_FLOAT FLOAT,
 C_NUMERIC numeric(20,5),
 C_DP double precision,
 C_DATE DATE,
 C_TS_WITHOUT TIMESTAMP WITHOUT TIME ZONE,
 C_TS_WITH TIMESTAMP WITH TIME ZONE );
 
CREATE TABLE vector_group_engine.vector_group_table_02(
 C_CHAR_1 CHAR(1),
 C_CHAR_2 CHAR(10),
 C_CHAR_3 CHAR(100),
 C_VARCHAR_1 VARCHAR(1),
 C_VARCHAR_2 VARCHAR(10),
 C_VARCHAR_3 VARCHAR(1024),
 C_INT BIGINT,
 C_BIGINT BIGINT,
 C_SMALLINT BIGINT,
 C_FLOAT FLOAT,
 C_NUMERIC numeric(20,5),
 C_DP double precision,
 C_DATE DATE,
 C_TS_WITHOUT TIMESTAMP WITHOUT TIME ZONE,
 C_TS_WITH TIMESTAMP WITH TIME ZONE, PARTIAL CLUSTER KEY(C_NUMERIC))WITH (ORIENTATION=COLUMN);

create table vector_group_engine.t1_shuju(x int);
create table vector_group_engine.t2_shuju(x int);
insert into vector_group_engine.t1_shuju select generate_series(1, 110);
insert into vector_group_engine.t2_shuju select generate_series(1, 20);

insert into vector_group_engine.row_group_table_02 select 'A','b20_000eq','b20_000EFGGAHWGS','a','abcdx','b20_0001111ABHTFADFADFDAFAFEFAGEAFEAFEAGEAGEAGEE_'||i.x,i.x,j.x,i.x+j.x,i.x+0.0001,i.x+0.00001,i.x+0.000001,'2000-01-01','2000-01-01 01:01:01','2000-01-01 01:01:01+01' from vector_group_engine.t1_shuju i, vector_group_engine.t2_shuju j;

insert into vector_group_table_02 select * from row_group_table_02;

analyze vector_group_table_01;
analyze vector_group_table_02; 
 
----
--- case 1: Basic Test
----
explain (verbose on, costs off) select col_bint from vector_group_table_01 group by col_bint having col_bint > 100 and col_bint < 300;
select col_bint from vector_group_table_01 group by col_bint having col_bint > 100 and col_bint < 300;
select col_vchar from vector_group_table_01 group by col_vchar having col_vchar > 'aaaaaa' order by 1;
select col_text from vector_group_table_01 group by col_text having col_text = 'wangfujing' order by 1;
select col_text, col_text from vector_group_table_01 group by 1, 2 order by 1, 2;
select col_num from vector_group_table_01 group by col_num order by 1;
select col_time from vector_group_table_01 group by col_time having col_time > '16:00:38';
select col_timetz from vector_group_table_01 group by col_timetz order by 1;
select col_interval from vector_group_table_01 group by col_interval order by 1;
select A.col_float + A.col_decimal from vector_group_table_01 A group by A.col_float + A.col_decimal order by 1;
select A.col_float + A.col_decimal, A.col_float, A.col_decimal from vector_group_table_01 A group by A.col_float, A.col_decimal order by 1, 2;
select A.col_int + B.col_bint from vector_group_table_01 A, row_group_table_01 B where A.col_int = B.col_int group by A.col_int + B.col_bint order by 1;

----
--- case 2: With NULL
----
insert into ROW_GROUP_TABLE_01 values(1, 40, 200, -1246521526, 'ee', NULL, 'wangfujing', 0.08, 10.01, 1100.01, NULL, '16:00:38', '1996-2-8 01:00:30+8', NULL);
insert into ROW_GROUP_TABLE_01 values(11, 50, 100, 2024856154, NULL, 'ffffff', 'tiananmen', 0.01, 10.11, 100.01, '2015-05-20', '16:00:38', '1996-2-6 01:00:30+8', '2 day 13:24:56','["Feb 10, 1947 23:59:12" "Jan 14, 1973 03:14:21"]');
insert into ROW_GROUP_TABLE_01 values(1, NULL, 200, -1246521526, 'ee', 'aaaaaa', 'wangfujing', 0.08, 10.01, 1100.01, NULL, '16:00:38', '1996-2-8 01:00:30+8', NULL);
insert into ROW_GROUP_TABLE_01 values(11, 50, 100, 2024856154, NULL, 'ffffff', 'tiananmen', 0.01, 10.11, 100.01, '2015-05-20', '16:00:38', '1996-2-6 01:00:30+8', '2 day 13:24:56','["Feb 10, 1947 23:59:12" "Jan 14, 1973 03:14:21"]');
insert into ROW_GROUP_TABLE_01 values(1, 40, 200, -1246521526, 'ee', NULL, 'wangfujing', 0.08, 10.01, 1100.01, '2015-06-15', NULL, '1996-2-8 01:00:30+8', NULL);
delete from vector_group_table_01;
insert into vector_group_table_01 select * from row_group_table_01;

select col_int from vector_group_table_01 group by col_int order by 1;
select col_char from vector_group_table_01 group by col_char order by 1;
select col_date from vector_group_table_01 group by col_date having col_date > '2015-01-01' order by 1;

----
--- case 3: With Big amount
----
SELECT C_INT,C_FLOAT,(C_INT+C_FLOAT) FROM vector_group_engine.vector_group_table_02 WHERE C_INT>10 GROUP BY C_INT,C_FLOAT HAVING C_FLOAT <20.0001 ORDER BY C_INT DESC LIMIT 5 OFFSET 2 ;

----
--- Clean Table and Resource
----
reset enable_hashagg;
drop schema vector_group_engine cascade;
