create schema inlist2join_type_c;
set current_schema=inlist2join_type_c;
set qrw_inlist2join_optmode = cost_base;

create table type
(
   col_int      TINYINT
  ,col_int2     SMALLINT
  ,col_int4	INTEGER
  ,col_int8	BIGINT
  ,col_char     CHAR(20)
  ,col_varchar  VARCHAR(30)
  ,col_num      DECIMAL(10,2)
  ,col_num2     NUMERIC(10,4)
  ,col_float    FLOAT4
  ,col_float2   FLOAT8
  ,col_float3	FLOAT(3)
  ,col_float4	BINARY_DOUBLE
  ,col_float5	DECIMAL(10,4)
  ,col_float6	INTEGER(6,3)
  ,col_bool	BOOLEAN
  ,col_text	TEXT
)with (orientation=column)
distribute by hash(col_int);

insert into type values
(0, 5 ,	193540, 1935401906, 'aabccd', 'aabccd', 1.20 , 10.0000, null    , 1.1   , 10.1234, 321.321, 123.123654, 123.123654, true,'aabccd'), 
(1, 6 ,	134597, 1345971420, 'abccd',  'abccd' , 11.18, 1.1181 , 55.555, 55.555, 10.1234, 321.321, 123.123654, 123.123654, false, 'abccd' ),  
(2, 7 ,	656473, 656473370 , 'aabccd', 'aabccd', 1.20 , 10.0000, 1.1   , 1.1   , 10.1234, 321.321, 124.123654, 123.123654, true , 'aabccd'),  
(3, 8 ,	126971, 1269710788, 'abccd',  'abccd' , 11.18, 1.1181 , 55.555, 55.555, 10.1234, 321.321, 123.123654, 123.123654, false, 'abccd' ),  
(4, 9 ,	115677, 1156776517, 'aabccd', 'aabccd', 1.20 , 10.0000, 1.1   , 1.1   , 10.1234, 321.321, 123.123654, 124.123654, true , 'aabccd'),  
(5, 10,	128901, 1289013296, 'abccd',  'abccd' , 11.18, 1.1181 , 55.555, 55.555, 10.1234, 321.321, 123.123654, 123.123654, true , 'abccd' );

select col_int from type where col_int in (1,2) order by 1;
explain (costs off) select col_int from type where col_int in (1,2) order by 1;
select col_int2 from type where col_int2 in (5,10) order by 1;
explain (costs off) select col_int2 from type where col_int2 in (5,10) order by 1;
select col_int4 from type where col_int4 in (134597, 134597) order by 1;
explain (costs off) select col_int4 from type where col_int4 in (134597, 134597) order by 1;
select col_int8 from type where col_int8 in (1345971420, 1156776517) order by 1;
explain (costs off) select col_int8 from type where col_int8 in (1345971420, 1156776517) order by 1;
select col_char from type where col_char in ('aabccd','aabccd', 'aab') order by 1;
explain (costs off) select col_char from type where col_char in ('aabccd','aabccd', 'aab') order by 1;
select col_varchar from type where col_varchar in ('abccd', 'abc','aac') order by 1;
explain (costs off) select col_varchar from type where col_varchar in ('abccd', 'abc','aac') order by 1;
select col_num from type where col_num in (1.20, 11.18, 3,45) order by 1;
explain (costs off) select col_num from type where col_num  in (1.20, 11.18, 3,45) order by 1;
select col_num2 from type where col_num2 in (1.1181, 10.0000) order by 1;
explain (costs off) select col_num2 from type where col_num2 in (1.1181, 10.0000) order by 1;
select col_float from type where col_float in (1.1, 55.555) order by 1;
explain (costs off) select col_float from type where col_float in (1.1, 55.555) order by 1;
select col_float2 from type where col_float2 in (10.1234, 1.1, 11.2222) order by 1;
explain (costs off) select col_float2 from type where col_float2 in (10.1234, 1.1, 11.2222) order by 1;
select col_float3 from type where col_float3	in (341.321, 10.1234, 10.1114, 11.2222) order by 1;
explain (costs off) select col_float3 from type where col_float3 in (341.321, 10.1234, 10.1114, 11.2222) order by 1;
select col_float4 from type where col_float4	in (321.321, 500.123) order by 1;
explain (costs off) select col_float4 from type where col_float4 in (321.321, 500.123) order by 1;
select col_float5 from type where col_float5	in (123.123654, 123.1237) order by 1;
explain (costs off) select col_float5 from type where col_float5 in (123.123654,123.1237) order by 1;
select col_float6 from type where col_float6	in (123.124, 113.123654) order by 1;
explain (costs off) select col_float6 from type where col_float6 in (123.124, 113.123654) order by 1;
select col_bool from type where col_bool in (true, false) order by 1;
explain (costs off) select col_bool from type where col_bool in (true, false) order by 1;
select col_text from type where col_text in ('abccd', 'aab') order by 1;
explain (costs off) select col_text from type where col_text in ('abccd', 'aab') order by 1;

CREATE TABLE time
(                                                                                                                              
  col_int		int
  ,col_date		date
  ,col_timestamp	timestamp
  ,col_timestamptz	timestamptz
  ,col_smalldatetime	smalldatetime
  ,col_char		char
  ,col_interval		interval
  ,col_time		time
  ,col_timetz		timetz
  ,col_tinterval	tinterval
)with (orientation=column)
distribute by hash(col_int);

COPY time(col_int, col_date, col_timestamp, col_timestamptz, col_smalldatetime, col_char, col_interval, col_time, col_timetz, col_tinterval) FROM stdin;
3	2011-11-01 00:00:00	2017-09-09 19:45:37	2017-09-09 19:45:37	2003-04-12 04:05:06	a	2 day 13:34:56	1984-2-6 01:00:30	1984-2-6 01:00:30+8	["Sep 4, 1983 23:59:12" "Oct 4, 1983 23:59:12"]
6	2012-11-02 00:00:00	2017-10-09 19:45:37	2017-10-09 19:45:37	2003-04-12 04:05:07	c	1 day 18:34:56	1986-2-6 03:00:30	1986-2-6 03:00:30+8	["May 10, 1947 23:59:12" "Jan 14, 1973 03:14:21"]
7	2011-11-01 00:00:00	2017-11-09 19:45:37	2017-11-09 19:45:37	2003-04-12 04:05:08	d	1 day 13:34:56	1987-2-6 08:00:30	1987-2-6 08:00:30+8	["epoch" "Mon May 1 00:30:30 1995"]
8	2012-11-02 00:00:00	2017-12-09 19:45:37	2017-12-09 19:45:37	2003-04-12 04:05:09	h	18 day 14:34:56	1989-2-6 06:00:30	1989-2-6 06:00:30+8	["Feb 15 1990 12:15:03" "2001-09-23 11:12:13"]
\.

select col_char from time where col_char in ('a', 'd') order by 1;
explain (costs off) select col_char from time where col_char in ('a', 'd') order by 1;
select col_date from time where col_date in ('2011-11-01 00:00:00', '2012-11-02 00:00:00') order by 1;
explain (costs off) select col_date from time where col_date in ('2011-11-01 00:00:00', '2012-11-02 00:00:00') order by 1;
select col_timestamp from time where col_timestamp in ('2017-09-09 19:45:37', '2017-09-09 19:45:37') order by 1;
explain (costs off) select col_timestamp from time where col_timestamp in ('2017-09-09 19:45:37', '2017-09-09 19:45:37') order by 1;
select col_timestamptz from time where col_timestamptz in ('2017-09-09 19:45:37', '2017-09-09 19:45:37') order by 1;
explain (costs off) select col_timestamptz from time where col_timestamptz in ('2017-09-09 19:45:37', '2017-09-09 19:45:37') order by 1;
select col_smalldatetime from time where col_smalldatetime in ('2017-09-09 19:45:37', '2003-04-12 04:05:06') order by 1;
explain (costs off) select col_smalldatetime from time where col_smalldatetime in ('2017-09-09 19:45:37', '2003-04-12 04:05:06') order by 1;
select col_time from time where col_time  in ('08:00:30', '00:00:30', '12:00:30') order by 1;
explain (costs off) select col_time from time where col_time  in ('08:00:30', '00:00:30', '12:00:30') order by 1;
select col_timetz from time where col_timetz  in ('08:00:30+08', '00:00:30+08', '12:00:30+08') order by 1;
explain (costs off) select col_timetz from time where col_timetz  in ('08:00:30+08', '00:00:30+08', '12:00:30+08') order by 1;
select col_interval from time where col_interval in ('2 day 13:34:56', '1 day 18:34:56') order by 1;
explain (costs off) select col_interval from time where col_interval in ('2 day 13:34:56', '1 day 18:34:56') order by 1;
select col_tinterval from time where col_tinterval in ('["Sep 4, 1983 23:59:12" "Oct 4, 1983 23:59:12"]', '["May 10, 1947 23:59:12" "Jan 14, 1973 03:14:21"]') order by 1;
explain (costs off) select col_tinterval from time where col_tinterval in ('["Sep 4, 1983 23:59:12" "Oct 4, 1983 23:59:12"]', '["May 10, 1947 23:59:12" "Jan 14, 1973 03:14:21"]') order by 1;
drop schema inlist2join_type_c cascade;
