create schema basefunc;
set current_schema=basefunc;

create table type
(
   col_int      TINYINT
  ,col_int2     SMALLINT
  ,col_int4		INTEGER
  ,col_int8	 	BIGINT
  ,col_smallserial	SMALLSERIAL
  ,col_serial 		SERIAL
  ,col_bigserial	BIGSERIAL
  ,col_real		REAL
  ,col_float    FLOAT4
  ,col_binaryp	DOUBLE PRECISION
  ,col_float8   FLOAT8
  ,col_float3	FLOAT(3)
  ,col_float50	FLOAT(50)
  ,col_double	BINARY_DOUBLE
  ,col_bool	BOOLEAN
);

insert into type values(0, 5 ,	193540, 1935401906, default, default, default,1.20, 10.0000, 1.1, 10.1234, 321.321, 123.123654, 123.123654);

CREATE TABLE varlentype
(
	col_int			TINYINT
   ,col_decimal	  DECIMAL(10,2)
   ,col_numeric	  NUMERIC(10,4)
   ,col_number	  NUMBER(10,4)
   ,col_dec   DEC(10,4)
   ,col_integer   INTEGER(10,4)
   ,col_char1		CHAR
   ,col_char20		CHAR(20)
   ,col_character	CHARACTER(20)
   ,col_nchar		NCHAR(20)
   ,col_varchar  	VARCHAR(20)
   ,col_charatervaring	CHARACTER VARYING(20)
   ,col_varchar2		VARCHAR2(20)
   ,col_nvarchar2		NVARCHAR2(20)
   ,col_text		TEXT
   ,col_clob		CLOB
);

insert into varlentype values(1,12,123,1234,12345,123456,'a','abc','abc','abc','abc','abc','abc','abc','abc','abc');
insert into varlentype values(2,12.12,123.123,1234.1234,12345.12345,123456.123456,'a','你好','你好','你好','你好','你好','你好','你好','你好','你好');

CREATE TABLE time
(                                                                                                                              
  col_int		int
  ,col_bigint		BIGINT
  ,col_date		date
  ,col_timestamp	timestamp
  ,col_timestamptz	timestamptz
  ,col_smalldatetime	smalldatetime
  ,col_char		char
  ,col_interval		interval
  ,col_time		time
  ,col_timetz		timetz
  ,col_tinterval	tinterval
  ,col_daytosecond 	INTERVAL DAY TO SECOND
  ,col_reltime		RELTIME
  ,col_abstime		ABSTIME
 );

COPY time(col_int, col_bigint, col_date, col_timestamp, col_timestamptz, col_smalldatetime, col_char, col_interval,
	col_time,col_timetz,col_tinterval,col_daytosecond,col_reltime,col_abstime) FROM stdin;
3	2	2011-11-01 10:10:10	2017-09-09 19:45:37	2017-09-09 19:45:37	2003-04-12 04:05:06	a	2 day	2017-09-09 19:45:37	2017-09-09 19:45:37	["Sep 4, 1983 23:59:12" "Oct 4, 1983 23:59:12"]	2 day	2007	2007-12-20 18:31:34
6	2	2012-11-02 10:10:10	2017-10-09 19:45:37	2017-10-09 19:45:37	2003-04-12 04:05:07	c	1 day	2017-09-09 19:45:37	2017-09-09 19:45:37	["Sep 4, 1983 23:59:12" "Oct 4, 1983 23:59:12"]	2 day	2007	2007-12-20 18:31:34
7	2	2011-11-01 10:10:10	2017-11-09 19:45:37	2017-11-09 19:45:37	2003-04-12 04:05:08	d	1 day	2017-09-09 19:45:37	2017-09-09 19:45:37	["Sep 4, 1983 23:59:12" "Oct 4, 1983 23:59:12"]	2 day	2007	2007-12-20 18:31:34
8	2	2012-11-02 10:10:10	2017-12-09 19:45:37	2017-12-09 19:45:37	2003-04-12 04:05:09	h	18 day	2017-09-09 19:45:37	2017-09-09 19:45:37	["Sep 4, 1983 23:59:12" "Oct 4, 1983 23:59:12"]	2 day	2007	2007-12-20 18:31:34
\.

-- test for sysdate + 1/24
select col_timestamp + 1/24 from time order by col_int;
select col_timestamp + 1/3 from time order by col_int;
select col_timestamptz + 1/24 from time order by col_int;
select col_timestamptz + 1/3 from time order by col_int;
select col_date + 1/24 from time order by col_int;
select col_date + 1/3 from time order by col_int;

-- tests for trunc
select trunc(col_timestamp) from time order by col_int;
select trunc(col_timestamptz) from time order by col_int;
select trunc(col_date) from time order by col_int;

-- test for substr
select substr(15::interval, 1, 4);
select substr(col_interval, 1, 5) from time order by col_int;

select substr('abcdef', col_bigint, 3) from time order by col_int;
select substr('abcdef', col_bigint) from time order by col_int;

select substr(15::interval, col_bigint, 3) from time order by col_int;
select substr(col_interval, col_bigint) from time order by col_int;

select substr('abc', 111111111111111::bigint, col_bigint)  from time;
select substr('abc', col_bigint, 1111111111111111111::bigint),sysdate  from time;
select substr('abc', 2, 2147483647);
select substr('jkeifkekls', -5, 2147483645);

-- tests for numtodsinterval
set intervalstyle=a;
select numtodsinterval(1500,'HOUR');
select numtodsinterval(-0.1,'HOUR');
select numtodsinterval(150032,'second');
select numtodsinterval(-.1500321234,'second');
--boundary test
SELECT numtodsinterval(-2147483648, 'DAY');
SELECT numtodsinterval(-2147483648, 'HOUR');
SELECT numtodsinterval(-2147483648, 'MINUTE');
SELECT numtodsinterval(-2147483648, 'SECOND');
SELECT numtodsinterval(2147483647, 'DAY');
SELECT numtodsinterval(999999999.99999999999, 'DAY');
SELECT numtodsinterval(2147483647, 'HOUR');
SELECT numtodsinterval(2147483647, 'MINUTE');
SELECT numtodsinterval(2147483647, 'SECOND');
SELECT numtodsinterval(123456789.123456789, 'DAY');
SELECT '2147483647 days 24 hours'::interval;

set intervalstyle=postgres;

-- tests for datalength
SELECT * from type;

SELECT datalength(col_int) as len_int1,
	datalength(col_int2) as len_int2,
	datalength(col_int4) as len_int4,
	datalength(col_int8) as len_int8,
	datalength(col_smallserial) as len_smallserial,
	datalength(col_serial) as len_serial,
	datalength(col_bigserial) as len_bigserial,
	datalength(col_real) as len_real,
	datalength(col_float) as len_float,
	datalength(col_binaryp) as len_binaryp,
	datalength(col_float8) as len_float8,
	datalength(col_float3) as len_float3,
	datalength(col_float50) as len_float50,
	datalength(col_double) as len_double,
	datalength(col_bool) as len_bool
from type;

SELECT * from time order by col_int;

SELECT datalength(col_date) as len_date,
	datalength(col_timestamp) as len_timestamp,
	datalength(col_timestamptz) as len_timestamptz,
	datalength(col_time) as len_time,
	datalength(col_timetz) as len_timetz,
	datalength(col_smalldatetime) as len_smalldatetime,
	datalength(col_interval) as len_interval,
	datalength(col_tinterval) as len_tinterval,
	datalength(col_daytosecond) as len_daytosecond,
	datalength(col_reltime) as len_reltime,
	datalength(col_abstime) as len_abstime
from time;

SELECT * from varlentype order by col_int;

SELECT datalength(col_decimal) as len_decimal,
	datalength(col_numeric) as len_numeric,
	datalength(col_number) as len_number,
	datalength(col_dec) as len_dec,
	datalength(col_integer) as len_integer,
	datalength(col_char1) as len_char1,
	datalength(col_char20) as len_char20,
	datalength(col_character) as len_character,
	datalength(col_nchar) as len_nchar,
	datalength(col_varchar) as len_varchar,
	datalength(col_charatervaring) as len_charvaring,
	datalength(col_varchar2) as len_varchar2,
	datalength(col_nvarchar2) as len_nvarchar2,
	datalength(col_text) as len_text,
	datalength(col_clob) as len_clob
from varlentype order by col_int;

drop table type;
drop table varlentype;
drop table time;

-- tests for repeat
-- create table at first
create table test_null_repeat(id int, col2 text); 
create table test_numeric(id int, col2 number); 
insert into test_numeric values(1,1.1),(2,1.2),(3,1.3);
insert into test_null_repeat values(1,'');
insert into test_null_repeat values(2,null);
insert into test_null_repeat values(3,repeat('Pg', 0));
-- check the length
select lengthb(repeat('Pg', 0));
-- check the value 
select repeat('Pg', 0) is null;
-- update by the result
update test_numeric set col2=test_null_repeat.col2 from test_null_repeat where test_numeric.id=test_null_repeat.id;
-- check the table
select * from test_numeric order by id;
-- drop table at last
drop table test_null_repeat;
drop table test_numeric;
--test function pg_partition_filepath
CREATE TABLE test_func_partition_filepath_table
(
    c1 int,
    c2 int,
    c3 int
)
partition by range (c1)
(
    partition p0_partition_filepath_table1 VALUES less than (10000),
    partition p1_partition_filepath_table1 VALUES less than (20000),
    partition p2_partition_filepath_table1 VALUES less than (30000),
    partition p3_partition_filepath_table1 VALUES less than (maxvalue)
);

create or replace function func_get_partition_filepath(partname text) returns text as $$ 
declare
    partoid integer;
    filepath text;
begin
    select oid from pg_partition where relname = partname into partoid;
    select * from pg_partition_filepath(partoid) into filepath;
    return filepath;
end;
$$ language plpgsql;
select func_get_partition_filepath('p0_partition_filepath_table1');
select func_get_partition_filepath('p1_partition_filepath_table1');
select func_get_partition_filepath('p2_partition_filepath_table1');
select func_get_partition_filepath('p3_partition_filepath_table1');
drop function func_get_partition_filepath;
drop table test_func_partition_filepath_table;
drop schema basefunc cascade;
