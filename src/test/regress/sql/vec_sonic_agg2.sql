-- test for partition table
set current_schema=vsonic_agg_engine;
set enable_sonic_hashjoin=on;
set enable_sonic_hashagg=on;
set enable_sort = off;
set time zone prc;
set time zone prc;
set datestyle to iso;

--groupby columns with different types
explain (costs off)
select
	min(col_a2)
from VECTOR_AGG_TABLE_03 
where col_a1>1 and col_a2>5 and col_a3%100<5 and col_a4%100<5 
group by 
	col_a1,col_a3,col_a4,
	col_smallint,col_integer,col_bigint,col_oid,
	col_real,col_numeric,col_numeric2,col_numeric3,
	col_numeric4,col_float,col_float2,col_t1,
	col_t2,col_t3,col_double_precision,
	col_decimal,col_char,col_char2,col_char3,
	col_varchar,col_varchar2,col_varchar3,col_text,
	col_time_without_time_zone,col_time_with_time_zone,col_timestamp_without_timezone,col_timestamp_with_timezone,
	col_smalldatetime,col_date,col_inet,col_inet
	order by 1
limit 10;

select
	min(col_a2)
from VECTOR_AGG_TABLE_03 
where col_a1>1 and col_a2>5 and col_a3%100<5 and col_a4%100<5 
group by 
	col_a1,col_a3,col_a4,
	col_smallint,col_integer,col_bigint,col_oid,
	col_real,col_numeric,col_numeric2,col_numeric3,
	col_numeric4,col_float,col_float2,col_t1,
	col_t2,col_t3,col_double_precision,
	col_decimal,col_char,col_char2,col_char3,
	col_varchar,col_varchar2,col_varchar3,col_text,
	col_time_without_time_zone,col_time_with_time_zone,col_timestamp_without_timezone,col_timestamp_with_timezone,
	col_smalldatetime,col_date
	order by 1
limit 10;

--test for int4, int8 , numeric operator
explain (costs off) 
select 
	col_a3+3,col_a3-3,col_a3*2,col_a3/2,
	col_a4+col_bigint,col_a4-col_bigint,col_a4*col_bigint,col_a4/col_bigint,
	col_numeric+col_numeric4,col_numeric-col_numeric4,col_numeric*col_numeric4,col_numeric/col_numeric4
from VECTOR_AGG_TABLE_03  where col_a1>1 and col_a2>5 and col_a3%100<5 and col_a4%100<5
group by 
	col_a3, col_a4,col_numeric,col_numeric4,col_bigint order by 1 limit 10;

	
select 
	col_a3+3,col_a3-3,col_a3*2,col_a3/2,
	col_a4+col_bigint,col_a4-col_bigint,col_a4*col_bigint,col_a4/col_bigint,
	col_numeric+col_numeric4,col_numeric-col_numeric4,col_numeric*col_numeric4,col_numeric/col_numeric4
from VECTOR_AGG_TABLE_03  where col_a1>1 and col_a2>5 and col_a3%100<5 and col_a4%100<5
group by 
	col_a3, col_a4,col_numeric,col_numeric4,col_bigint order by 1 limit 10;

--int2,int4,int8 and numeric
select
	col_float2,min(col_a2),avg(col_a2),sum(col_a2),
	sum(col_a3),max(col_a3),avg(col_a3),count(col_a3),
	max(col_numeric),avg(col_numeric),count(col_numeric),
	sum(col_numeric3),avg(col_numeric3),
	max(col_a4),avg(col_a4),sum(col_a4)
from VECTOR_AGG_TABLE_03 
where col_a1>1 and col_a2>5 and col_a3%100<5 and col_a4%100<5
group by 
	col_float2
	order by 5 limit 10;
	
--agg column is all null 
update VECTOR_AGG_TABLE_03 set col_a3 = null where col_a3 is not null;
--groupby column is all null 
update VECTOR_AGG_TABLE_03 set col_smallint = null where col_smallint is not null;
update VECTOR_AGG_TABLE_03 set col_char2 = null where col_char2 is not null;
--agg colum is random null 
update VECTOR_AGG_TABLE_03 set col_numeric = null where col_a4%7=4;
--groupby colum is random null 
update VECTOR_AGG_TABLE_03 set col_integer = null where col_bigint%11>4;


select
	min(col_a2),avg(col_a2),sum(col_a2),
	sum(col_a3),max(col_a3),avg(col_a3),count(col_a3),
	max(col_numeric),avg(col_numeric),count(col_numeric),
	sum(col_numeric3),avg(col_numeric3),
	max(col_a4),avg(col_a4),sum(col_a4)
from VECTOR_AGG_TABLE_03 
group by 
	col_a4,
	col_smallint
	order by 12 limit 10;
	
select
	min(col_a2),avg(col_a2),sum(col_a2),
	sum(col_a3),max(col_a3),avg(col_a3),count(col_a3),
	max(col_numeric),avg(col_numeric),count(col_numeric),
	sum(col_numeric3),avg(col_numeric3),
	max(col_a4),avg(col_a4),sum(col_a4)
from VECTOR_AGG_TABLE_03 
group by 
	col_a4,
	col_smallint
	order by 12 limit 10;
	
	
select
	min(col_a2),avg(col_a2),sum(col_a2),
	sum(col_a3),max(col_a3),avg(col_a3),count(col_a3),
	max(col_numeric),avg(col_numeric),count(col_numeric),
	sum(col_numeric3),avg(col_numeric3),
	max(col_a4),avg(col_a4),sum(col_a4)
from VECTOR_AGG_TABLE_03 
group by 
	col_a4,
	col_smallint
	order by 12 limit 10;

select 
	max(col_a3),avg(col_a3),avg(col_numeric),count(col_numeric)
from VECTOR_AGG_TABLE_03 where col_bigint%37=2  group by col_char having(min(col_char) = any(select col_char from vector_agg_table_02))
 order by 3 limit 10;

select 	min(col_a2),avg(col_a2),count(col_a2),
	max(col_a3),avg(col_a3),count(col_a3),
	min(col_numeric),avg(col_numeric),count(col_numeric),
	sum(col_numeric3),avg(col_numeric3),count(col_numeric3),
	max(col_integer),avg(col_integer),count(*)
from VECTOR_AGG_TABLE_03 where col_varchar in (select min(col_varchar) from VECTOR_AGG_TABLE_03 group by col_char2 order by 1);

-- mix of join and agg
select 	
	max(col_a3),avg(col_a3),count(col_a3),
	min(col_numeric),avg(col_numeric),count(col_numeric),
	sum(col_numeric3),avg(col_numeric3),count(col_numeric3),
	max(col_integer),avg(col_integer),count(*)
from VECTOR_AGG_TABLE_03 A, vector_agg_table_02 B where A.col_bigint = B.col_bint group by A.col_integer, B.col_bint order by 1, 2 limit 30;

select 	
	max(col_a3),avg(col_a3),count(col_a3),
	min(col_numeric),avg(col_numeric),count(col_numeric),
	sum(col_numeric3),avg(col_numeric3),count(col_numeric3),
	max(col_integer),avg(col_integer),count(*)
from VECTOR_AGG_TABLE_03 A, vector_agg_table_02 B where A.col_bigint = B.col_bint group by A.col_integer + B.col_bint order by 1 limit 30;

--test for in disk
set work_mem='64kB';

select
	min(col_a2),avg(col_a2),sum(col_a2),
	sum(col_a3),max(col_a3),avg(col_a3),count(col_a3),
	max(col_numeric),avg(col_numeric),count(col_numeric),
	sum(col_numeric3),avg(col_numeric3),
	max(col_a4),avg(col_a4),sum(col_a4)
from VECTOR_AGG_TABLE_03 
group by 
	col_a4,
	col_smallint
	order by 12 limit 10;

select 
	max(col_a3),avg(col_a3),avg(col_numeric),count(col_numeric)
from VECTOR_AGG_TABLE_03 where col_bigint%37=2  group by col_char having(min(col_char) = any(select col_char from vector_agg_table_02))
 order by 3 limit 10;