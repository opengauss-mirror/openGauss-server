/*
 * 1) testing compatibility of data type with A db
 * 2) testing implicit conversion between some data types
 */

/* 1) compatibility of data type */

/* a.Date type */
CREATE TABLE testdate(
        d date
        );
--insert values

--insert with ISO style
insert into testdate values('2012-7-15 12:59:32');

--insert with Postgres style
insert into testdate values('14-AUG-2012 12:55:18');

--insert with SQL style
insert into testdate values('1/8/1999 12:38:24');

--insert without time
insert into testdate values(to_date('20110505','yyyymmdd'));

--insert with invalid value
insert into testdate values('201200000-7-15 12:59:31');
insert into testdate values('2012-15-16 12:59:32');
insert into testdate values('2012-7-35 12:35:32');
insert into testdate values('2012-7-15 25:30:31');
insert into testdate values('2012-7-15 25:30:31');
insert into testdate values('2012-7-15 23:67:30');

--show all dates
select * from testdate d order by d;

--change datestyle to Postgres
SET Datestyle = 'Postgres,DMY';

insert into testdate values('1/8/1999 12:38:24');
--See the change
select * from testdate order by d;

--change datestyle to ISO mode which is default output style of GaussDB
set datestyle = iso,ymd;

--See the change
select * from testdate ORDER BY d;

--Formatted Output
select to_char(d,'dd/mm/yy hh24:mi:ss') from testdate order by to_char;
select to_char(d,'mm/dd/yyyy hh24:mi:ss') from testdate order by to_char;
select to_date('2009-8-1 19:01:01','YYYY-MM-DD HH24:MI:SS') order by to_date;
select to_date('July 31,09,21 09:01:01','Month DD,YY,CC HH:MI:SS') order by to_date;

--To see if the Date type can execute comparison operation
select * from testdate where d>to_date('20120501','yyyymmdd') order by d;

drop table testdate;

/* b.Interval type */
CREATE TABLE testinterval(
        i interval
        );

insert into testinterval values(interval '2 12:59:34.5678' day to second(3));

insert into testinterval values(interval '2 12:59:34.5678' day(3) to second(3));

insert into testinterval values(interval '15-9' year to month);

insert into testinterval values(interval '15-9' year(3) to month);

insert into testinterval values(interval '15-9' year(3) to month(3));

insert into testinterval values(interval '1 year 2 months 3 days 4 hours 5 minutes 6 seconds');

insert into testinterval values(interval '1-9 3 4:4:6');

select * from testinterval order by i;

drop table testinterval;

/* c.LOB and Raw type*/
CREATE TABLE test_lob(b1 blob,c1 clob,r1 raw);

INSERT INTO test_lob values('Insert a blob-type data','Huawei Gauss DB version 1.0','Hello World!');
INSERT INTO test_lob values('20120711','test for delete','Hello World!');
SELECT * FROM test_lob;
SELECT * FROM test_lob;
DROP TABLE test_lob;

/* d.Varchar,char and Varchar2 type*/
CREATE TABLE char_t(c1 varchar2(12), c2 char(10), c3 varchar(12));
insert into char_t values('varchar2test','chartest','varchartest');

insert into char_t values('the regress test is boring','chartest','varchartest');
insert into char_t values('varchar2test','the regress test is boring','varchartest');
insert into char_t values('the regress test is boring','chartest','the regress test is boring');

DROP TABLE char_t;

/* e.Number type*/
create table number_test(
  c1 number,
  c2 number(8),
  c3 number(13,5),
  c4 number(38,10)
);
insert into number_test values(10,9,12.3,666666666666666666666.22222);
insert into number_test values(10.22,9,12.3,666666666666666666666.22222);
insert into number_test values(10,9.99,12.3,666666666666666666666.22222);
insert into number_test values(10,9,12.3777777777,666666666666666666666.22222);
insert into number_test values(10,9,12.3,666666666666666666666666666666666666666666666666.22222);
select * from number_test order by 1, 2, 3, 4;
drop table number_test;

/* f.BINARY_DOUBLE type */
CREATE TABLE test_type(
	my_double BINARY_DOUBLE
	);
INSERT INTO test_type VALUES(15.23448);
INSERT INTO test_type VALUES(1E-323);
INSERT INTO test_type VALUES(1E-324);
INSERT INTO test_type VALUES(1E+308);
INSERT INTO test_type VALUES(1E+309);
SELECT * FROM test_type order by 1;
DROP TABLE test_type;

/* g.Type BINARY_INTEGER */
CREATE TABLE test_type(
	my_double BINARY_INTEGER
	);
INSERT INTO test_type values(0);
INSERT INTO test_type values(-2147483648);
INSERT INTO test_type values(2147483647);
INSERT INTO test_type values(2147483648);
INSERT INTO test_type values(-2147483649);
SELECT * FROM test_type order by 1;
DROP TABLE test_type;

/* h.Integer(p,s) format */
CREATE TABLE test_type(
	my_integer INTEGER(10,4)
	);
INSERT INTO test_type values(99999.9);
INSERT INTO test_type values(999999.9);
INSERT INTO test_type values(9999999.9);
SELECT * FROM test_type order by 1;
DROP TABLE test_type;

/* 2)tests for implicit conversion between some data types */

/* a.test implicit conversion between int2 and int4 */
create table i4ti2(
	a int2,
	b int4
);
insert into i4ti2 values(9,10);
select a&b from i4ti2;
drop table i4ti2;

/* b.test implicit conversion between number(including 
     int2,int4,int8,float4,float8,number) and text */
select substr('123',1,2)*12;
select 123::int2||1;
select 123::int4||1;
select 123::int8||1;
select 123.1::float4||1;
select 123.1::float8||1;


/* 3)test the other implicit conversion*/
--CHAR
CREATE OR REPLACE FUNCTION TEST_FUNC(tempdata char) RETURNS VOID AS $$
BEGIN
	raise info'TEST CHAR VALUE IS %',tempdata;  
END;
$$ LANGUAGE plpgsql;

select TEST_FUNC('abc'::clob);
select TEST_FUNC('abc'::varchar2);
drop function test_func(char);

--VARCHAR2
CREATE OR REPLACE FUNCTION TEST_FUNC(tempdata varchar2(50)) RETURNS VOID AS $$
BEGIN
	raise info'TEST VARCHAR VALUE IS %',tempdata;  
END;
$$ LANGUAGE plpgsql;

select TEST_FUNC('abc'::clob);
select TEST_FUNC(123);
select TEST_FUNC('123'::integer);
select TEST_FUNC('abc'::raw);
select TEST_FUNC('abc'::char);
select TEST_FUNC('2012-08-01 11:23:45'::timestamp); --with timezone?
select TEST_FUNC(interval '1 18:00:00' day to second);
select TEST_FUNC('1 day 18:00:00');

drop function test_func(varchar2);

--CLOB
CREATE OR REPLACE FUNCTION TEST_FUNC(tempdata CLOB) RETURNS VOID AS $$
BEGIN
	raise info'TEST CLOB VALUE IS %',tempdata;  
END;
$$ LANGUAGE plpgsql;

select TEST_FUNC('abc'::char);
select TEST_FUNC('abc'::varchar2);

drop function test_func(clob);

--INTEGER
CREATE OR REPLACE FUNCTION TEST_FUNC(tempdata INTEGER) RETURNS VOID AS $$
BEGIN
	raise info'TEST INTEGER VALUE IS %',tempdata;  
END;
$$ LANGUAGE plpgsql;

select TEST_FUNC('123'::char(3));
select TEST_FUNC('123'::varchar2);
drop function test_func(integer);

--TIMESTAMP
CREATE OR REPLACE FUNCTION TEST_FUNC(tempdata TIMESTAMP) RETURNS VOID AS $$
BEGIN
	raise info'TEST TIMESTAMP VALUE IS %',tempdata;  
END;
$$ LANGUAGE plpgsql;

select TEST_FUNC('2012-08-01 11:23:45'::char(20));
select TEST_FUNC('2012-08-01 11:23:45'::varchar2);
drop function test_func(timestamp);

--INTERVAL
CREATE OR REPLACE FUNCTION TEST_FUNC(tempdata INTERVAL) RETURNS VOID AS $$
BEGIN
	raise info'TEST INTERVAL VALUE IS %',tempdata;  
END;
$$ LANGUAGE plpgsql;
select TEST_FUNC('1 day 18:00:00'::char(20));
select TEST_FUNC('1 day 18:00:00'::varchar2);
drop function test_func(INTERVAL);

create function ftime(a abstime)returns int
as $$
begin
return 1;
end;
$$ language plpgsql;

create function ftime(a timetz)returns int
as $$
begin
return 1;
end;
$$ language plpgsql;

select ftime('2012-11-14');

create function funtime(a reltime) returns int
as $$
begin
return 0;   
end;                    
$$ language plpgsql;

create function funTime(a timestamptz)returns int
as $$
begin
return 1;
end;
$$ language plpgsql;

select funtime('2012-11-14');

values(1,1),(2,2.2);

SELECT 1 AS one UNION SELECT 1.1::float8;

select greatest(1, 1.1);
select greatest(1.1, 1);
select least(1, 0.9);
select least(0.9, 1);

create or replace function test_cast(a numeric)
returns int as
$$
declare

begin
	case a
		when 1.1 then
			raise info '%', '1.1';
		when 1 then
			raise info '%', '1';
	end case;
	return 1;
end;
$$ language plpgsql;

select test_cast(1);
select test_cast(1.1);

create table test_cast( a numeric[]);
insert into test_cast values(array[1,2,1.1,4]);
select * from test_cast;
drop table test_cast;

create table test_cast (a int);
create table test_cast1 (a numeric);
insert into test_cast values(1);
insert into test_cast1 values(1.1);
select * from test_cast join test_cast1 using (a);

select * from test_cast where a in (1.1,2,3);

CREATE TABLE test_cast2 (a INTEGER, b NUMERIC);
INSERT INTO  test_cast2  VALUES(1.8,3.01);
INSERT INTO  test_cast2  VALUES(1,2.0);
INSERT INTO  test_cast2  VALUES(6,5.99);
INSERT INTO  test_cast2  VALUES(0,-0.00);
INSERT INTO  test_cast2  VALUES(3,1);
INSERT INTO  test_cast2  VALUES(7,8);

SELECT a FROM test_cast2 except SELECT  b FROM test_cast2 ORDER BY 1;
SELECT b FROM test_cast2 except SELECT  a FROM test_cast2 ORDER BY b;

SELECT b FROM test_cast2 except SELECT  a FROM test_cast2 ORDER BY 1;
SELECT a FROM test_cast2 except SELECT  b FROM test_cast2 ORDER BY a;

create table test_cast3(a char(10),b varchar(10));
insert into test_cast3 values('  ','9');
select nvl(a ,b) as RESULT from test_cast3;
select coalesce(a, b) as RESULT from test_cast3;

drop table test_cast;
drop table test_cast1;
drop table test_cast2;
drop table test_cast3;
drop function test_cast;

create table test_raw(r raw);
insert into test_raw values ('a');
select r from test_raw order by r;
drop table test_raw;

create table test_raw (r raw);
insert into test_raw values ('a');
insert into test_raw values ('b');
insert into test_raw values ('s');
insert into test_raw values ('as');
insert into test_raw values ('c');
insert into test_raw values ('f');
insert into test_raw values ('dd');
insert into test_raw values ('d');
insert into test_raw values ('e');
insert into test_raw values ('12');
select r from test_raw order by r desc;
select r from test_raw order by r asc;
drop table test_raw;

create table test_raw (a raw(1), b raw(1));
insert into test_raw values ('a', 'a');
insert into test_raw values ('b', 'c');
insert into test_raw values ('d', '9');
insert into test_raw values ('6', '6');
insert into test_raw values ('5', 'f');
select * from test_raw where a < b order by a desc;
select * from test_raw where a > b order by b asc;
select * from test_raw where a < b or a > b order by a desc;
select * from test_raw where a < b or a > b order by a asc;
select * from test_raw where a = b order by a desc;
select * from test_raw where a = b order by a asc;
select * from test_raw where a >= b order by a desc;
select * from test_raw where a >= b order by a asc;
select * from test_raw where a <= b order by a desc;
select * from test_raw where a <= b order by a asc;
drop table test_raw;

create table test_raw1 (a raw(1), b raw(1));
create table test_raw2 (a raw(1), b raw(1));
insert into test_raw1 values ('a', 'a');
insert into test_raw1 values ('b', '4');
insert into test_raw1 values ('2', '9');
insert into test_raw1 values ('6', '6');
insert into test_raw1 values ('5', 'e');
insert into test_raw2 values ('a', 'a');
insert into test_raw2 values ('d', 'c');
insert into test_raw2 values ('d', '9');
insert into test_raw2 values ('2', '6');
insert into test_raw2 values ('1', 'f');
select * from test_raw1 where a like 'd';
select * from test_raw1 test1 cross join test_raw2 test2 where test1.a = test2.b order by 1,2,3;
select * from test_raw1 test1 join test_raw2 test2 using(a) order by 1,2,3;
select * from test_raw1 test1 full join test_raw2 test2 using(a) order by 1,2,3;
select * from test_raw1 test1 left join test_raw2 test2 using(a) order by 1,2,3;
select * from test_raw1 test1 right join test_raw2 test2 using(a) order by 1,2,3;
select * from test_raw1 test1 inner join test_raw2 test2 using(a) order by 1,2,3;
select * from test_raw1 test1 inner join test_raw2 test2 using(a) order by 1,2,3;
select * from test_raw1 test1 natural join test_raw2 test2 order by 1,2;
drop table test_raw1;
drop table test_raw2;
CREATE SCHEMA DATA_TYPE;
CREATE TABLE DATA_TYPE.INPUT_OF_ARRAY_003 (COL_INTERVAL_1 INTERVAL, COL_INTERVAL_2  INTERVAL[]);
INSERT INTO DATA_TYPE.INPUT_OF_ARRAY_003 VALUES (1,'{1}');
SELECT  *  FROM DATA_TYPE.INPUT_OF_ARRAY_003;
INSERT INTO DATA_TYPE.INPUT_OF_ARRAY_003 VALUES (2, '{3}');
INSERT INTO DATA_TYPE.INPUT_OF_ARRAY_003 VALUES (10, '{10}');
select * from DATA_TYPE.INPUT_OF_ARRAY_003 VALUES order by 1,2;
INSERT INTO DATA_TYPE.INPUT_OF_ARRAY_003 VALUES (60, '{60}');
INSERT INTO DATA_TYPE.INPUT_OF_ARRAY_003 VALUES (120, '{120}');
select * from DATA_TYPE.INPUT_OF_ARRAY_003 VALUES  order by 1,2;
INSERT INTO DATA_TYPE.INPUT_OF_ARRAY_003 VALUES (120, '{120, 100, 15, 20}');
select * from DATA_TYPE.INPUT_OF_ARRAY_003 VALUES  order by 1,2;
drop TABLE DATA_TYPE.INPUT_OF_ARRAY_003;
drop SCHEMA DATA_TYPE;

--int to char
CREATE OR REPLACE PROCEDURE SP_TEST_1(V_CHAR CHAR)
AS
BEGIN
END;
/

CREATE OR REPLACE PROCEDURE SP_TEST_2
AS
    V_INT INTEGER;
BEGIN
    V_INT := 123456789;
    SP_TEST_1(V_INT);
END;
/
CALL SP_TEST_2();

DROP PROCEDURE SP_TEST_2;
DROP PROCEDURE SP_TEST_1;


CREATE OR REPLACE PROCEDURE SP_TEST_1(V_CHAR BPCHAR)
AS
BEGIN
END;
/

CREATE OR REPLACE PROCEDURE SP_TEST_2
AS
    V_INT INTEGER;
BEGIN
    V_INT := 123456789;
    SP_TEST_1(V_INT);
END;
/
CALL SP_TEST_2();

DROP PROCEDURE SP_TEST_2;
DROP PROCEDURE SP_TEST_1;


CREATE OR REPLACE PROCEDURE SP_TEST_1(V_CHAR text)
AS
BEGIN
END;
/

CREATE OR REPLACE PROCEDURE SP_TEST_2
AS
    V_INT INTEGER;
BEGIN
    V_INT := 123456789;
    SP_TEST_1(V_INT);
END;
/
CALL SP_TEST_2();

DROP PROCEDURE SP_TEST_2;
DROP PROCEDURE SP_TEST_1;

--create schema
CREATE SCHEMA FVT_GAUSSDB_ADAPT_1;

--create function with varchar parameter
CREATE OR REPLACE FUNCTION FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_055(tempdata VARCHAR) RETURNS VOID AS $$
BEGIN
 raise info'VARCHAR type value is %',tempdata;  
END;
$$ LANGUAGE plpgsql;
--create function with bpchar parameter
CREATE OR REPLACE FUNCTION FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_055(tempdata BPCHAR) RETURNS VOID AS $$
BEGIN
 raise info'BPCHAR type value is %',tempdata;  
END;
$$ LANGUAGE plpgsql;
--select varchar function
select FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_055(cast('abc' as CLOB));
--select bpchar function
drop function FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_055(VARCHAR);
select FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_055(cast('abc' as CLOB));

--create function with text parameter
CREATE OR REPLACE FUNCTION FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_056(tempdata TEXT) RETURNS VOID AS $$
BEGIN
 raise info'TEXT type value is %',tempdata;  
END;
$$ LANGUAGE plpgsql;
--create function with varchar parameter
CREATE OR REPLACE FUNCTION FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_056(tempdata VARCHAR) RETURNS VOID AS $$
BEGIN
 raise info'VARCHAR type value is %',tempdata;  
END;
$$ LANGUAGE plpgsql;
--create function with numeric parameter
CREATE OR REPLACE FUNCTION FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_056(tempdata NUMERIC) RETURNS VOID AS $$
BEGIN
 raise info'NUMERIC type value is %',tempdata;  
END;
$$ LANGUAGE plpgsql;
--create function with int8 parameter
CREATE OR REPLACE FUNCTION FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_056(tempdata INT8) RETURNS VOID AS $$
BEGIN
 raise info'INT8 type value is %',tempdata;  
END;
$$ LANGUAGE plpgsql;
--create function with int4 parameter
CREATE OR REPLACE FUNCTION FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_056(tempdata INT4) RETURNS VOID AS $$
BEGIN
 raise info'INT4 type value is %',tempdata;  
END;
$$ LANGUAGE plpgsql;
--create function with int2 parameter
CREATE OR REPLACE FUNCTION FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_056(tempdata INT2) RETURNS VOID AS $$
BEGIN
 raise info'INT2 type value is %',tempdata;  
END;
$$ LANGUAGE plpgsql;
--select numeric function
select FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_056(cast('123.456' as float4));
--select text function
drop function FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_056(NUMERIC);
select FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_056(cast('123.456' as float4));
--select varchar function
drop function FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_056(TEXT);
select FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_056(cast('123.456' as float4));
--select int8 function
drop function FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_056(VARCHAR);
select FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_056(cast('123.456' as float4));
--select in4 function
drop function FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_056(INT8);
select FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_056(cast('123.456' as float4));
--select int2 function
drop function FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_056(INT4);
select FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_056(cast('123.456' as float4));

--create function with text parameter
CREATE OR REPLACE FUNCTION FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_057(tempdata TEXT) RETURNS VOID AS $$
BEGIN
 raise info'TEXT type value is %',tempdata;  
END;
$$ LANGUAGE plpgsql;
--create function with varchar parameter
CREATE OR REPLACE FUNCTION FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_057(tempdata VARCHAR) RETURNS VOID AS $$
BEGIN
 raise info'VARCHAR type value is %',tempdata;  
END;
$$ LANGUAGE plpgsql;
--create function with numeric parameter
CREATE OR REPLACE FUNCTION FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_057(tempdata NUMERIC) RETURNS VOID AS $$
BEGIN
 raise info'NUMERIC type value is %',tempdata;  
END;
$$ LANGUAGE plpgsql;
--create function with float4 parameter
CREATE OR REPLACE FUNCTION FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_057(tempdata FLOAT4) RETURNS VOID AS $$
BEGIN
 raise info'FLOAT4 type value is %',tempdata;  
END;
$$ LANGUAGE plpgsql;
--create function with in8 parameter
CREATE OR REPLACE FUNCTION FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_057(tempdata INT8) RETURNS VOID AS $$
BEGIN
 raise info'INT8 type value is %',tempdata;  
END;
$$ LANGUAGE plpgsql;
--create function with int4 parameter
CREATE OR REPLACE FUNCTION FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_057(tempdata INT4) RETURNS VOID AS $$
BEGIN
 raise info'INT4 type value is %',tempdata;  
END;
$$ LANGUAGE plpgsql;
--create function with int2 parameter
CREATE OR REPLACE FUNCTION FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_057(tempdata INT2) RETURNS VOID AS $$
BEGIN
 raise info'INT2 type value is %',tempdata;  
END;
$$ LANGUAGE plpgsql;
--select numeric function
select FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_057(cast('123.456' as float8));
--select text function
drop function FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_057(NUMERIC);
select FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_057(cast('123.456' as float8));
--select varchar function
drop function FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_057(TEXT);
select FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_057(cast('123.456' as float8));
--select float4 function
drop function FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_057(VARCHAR);
select FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_057(cast('123.456' as float8));
--select int8 function
drop function FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_057(FLOAT4);
select FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_057(cast('123.456' as float8));
--select int4 function
drop function FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_057(INT8);
select FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_057(cast('123.456' as float8));
--select int2 function
drop function FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_057(INT4);
select FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_057(cast('123.456' as float8));

--create function with text parameter
CREATE OR REPLACE FUNCTION FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_058(tempdata TEXT) RETURNS VOID AS $$
BEGIN
 raise info'TEXT type value is %',tempdata;  
END;
$$ LANGUAGE plpgsql;
--create function with varchar parameter
CREATE OR REPLACE FUNCTION FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_058(tempdata VARCHAR) RETURNS VOID AS $$
BEGIN
 raise info'VARCHAR type value is %',tempdata;  
END;
$$ LANGUAGE plpgsql;
--create function with interval parameter
CREATE OR REPLACE FUNCTION FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_058(tempdata INTERVAL) RETURNS VOID AS $$
BEGIN
 raise info'INTERVAL type value is %',tempdata;  
END;
$$ LANGUAGE plpgsql;
--select text function
select FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_058(cast('123' as int2));
--select varchar function
drop function FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_058(TEXT);
select FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_058(cast('123' as int2));
--select interval function
drop function FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_058(VARCHAR);
select FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_058(cast('123' as int2));

--create function with text parameter
CREATE OR REPLACE FUNCTION FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_059(tempdata TEXT) RETURNS VOID AS $$
BEGIN
 raise info'TEXT type value is %',tempdata;  
END;
$$ LANGUAGE plpgsql;
--create function with varchar parameter
CREATE OR REPLACE FUNCTION FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_059(tempdata VARCHAR) RETURNS VOID AS $$
BEGIN
 raise info'VARCHAR type value is %',tempdata;  
END;
$$ LANGUAGE plpgsql;
--create function with int2 parameter
CREATE OR REPLACE FUNCTION FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_059(tempdata INT2) RETURNS VOID AS $$
BEGIN
 raise info'INT2 type value is %',tempdata;  
END;
$$ LANGUAGE plpgsql;
--create function with interval parameter
CREATE OR REPLACE FUNCTION FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_059(tempdata INTERVAL) RETURNS VOID AS $$
BEGIN
 raise info'INTERVAL type value is %',tempdata;  
END;
$$ LANGUAGE plpgsql;
--select text function
select FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_059(cast('123' as int4));
--select varchar function
drop function FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_059(TEXT);
select FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_059(cast('123' as int4));
--select int2 function
drop function FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_059(VARCHAR);
select FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_059(cast('123' as int4));
--select interval function
drop function FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_059(INT2);
select FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_059(cast('123' as int4));

--create function with text parameter
CREATE OR REPLACE FUNCTION FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_060(tempdata TEXT) RETURNS VOID AS $$
BEGIN
 raise info'TEXT type value is %',tempdata;  
END;
$$ LANGUAGE plpgsql;
--create function with varchar parameter
CREATE OR REPLACE FUNCTION FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_060(tempdata VARCHAR) RETURNS VOID AS $$
BEGIN
 raise info'VARCHAR type value is %',tempdata;  
END;
$$ LANGUAGE plpgsql;
--select text function
select FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_060(cast('123' as int8));
--select varchar function
drop function FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_060(TEXT);
select FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_060(cast('123' as int8));

--create function with text parameter
CREATE OR REPLACE FUNCTION FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_061(tempdata TEXT) RETURNS VOID AS $$
BEGIN
 raise info'TEXT type value is %',tempdata;  
END;
$$ LANGUAGE plpgsql;
--create function with varchar parameter
CREATE OR REPLACE FUNCTION FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_061(tempdata VARCHAR) RETURNS VOID AS $$
BEGIN
 raise info'VARCHAR type value is %',tempdata;  
END;
$$ LANGUAGE plpgsql;
--create function with int8 parameter
CREATE OR REPLACE FUNCTION FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_061(tempdata INT8) RETURNS VOID AS $$
BEGIN
 raise info'INT8 type value is %',tempdata;  
END;
$$ LANGUAGE plpgsql;
--create function with int4 parameter
CREATE OR REPLACE FUNCTION FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_061(tempdata INT4) RETURNS VOID AS $$
BEGIN
 raise info'INT4 type value is %',tempdata;  
END;
$$ LANGUAGE plpgsql;
--create function with int2 parameter
CREATE OR REPLACE FUNCTION FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_061(tempdata INT2) RETURNS VOID AS $$
BEGIN
 raise info'INT2 type value is %',tempdata;  
END;
$$ LANGUAGE plpgsql;
--create function with interval parameter
CREATE OR REPLACE FUNCTION FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_061(tempdata INTERVAL) RETURNS VOID AS $$
BEGIN
 raise info'INTERVAL type value is %',tempdata;  
END;
$$ LANGUAGE plpgsql;
--select text function
select FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_061(cast('123.456' as numeric));
--select varchar function
drop function FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_061(TEXT);
select FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_061(cast('123.456' as numeric));
--select int8 function
drop function FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_061(VARCHAR);
select FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_061(cast('123.456' as numeric));
--select in4 function
drop function FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_061(INT8);
select FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_061(cast('123.456' as numeric));
--select int2 function
drop function FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_061(INT4);
select FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_061(cast('123.456' as numeric));
--select interval function
drop function FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_061(INT2);
select FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_061(cast('12.345' as numeric));

--create function with text parameter
CREATE OR REPLACE FUNCTION FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_062(tempdata TEXT) RETURNS VOID AS $$
BEGIN
 raise info'TEXT type value is %',tempdata;  
END;
$$ LANGUAGE plpgsql;
--create function with varchar parameter
CREATE OR REPLACE FUNCTION FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_062(tempdata VARCHAR) RETURNS VOID AS $$
BEGIN
 raise info'VARCHAR type value is %',tempdata;  
END;
$$ LANGUAGE plpgsql;
--select text function
select FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_062(cast('0100ABC' as raw));
--select varchar function
drop function FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_062(TEXT);
select FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_062(cast('0100ABC' as raw));

--create function with numeric parameter
CREATE OR REPLACE FUNCTION FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_063(tempdata NUMERIC) RETURNS VOID AS $$
BEGIN
 raise info'NUMERIC type value is %',tempdata;  
END;
$$ LANGUAGE plpgsql;
--create function with float8 parameter
CREATE OR REPLACE FUNCTION FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_063(tempdata FLOAT8) RETURNS VOID AS $$
BEGIN
 raise info'FLOAT8 type value is %',tempdata;  
END;
$$ LANGUAGE plpgsql;
--create function with float4 parameter
CREATE OR REPLACE FUNCTION FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_063(tempdata FLOAT4) RETURNS VOID AS $$
BEGIN
 raise info'FLOAT4 type value is %',tempdata;  
END;
$$ LANGUAGE plpgsql;
--create function with int8 parameter
CREATE OR REPLACE FUNCTION FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_063(tempdata INT8) RETURNS VOID AS $$
BEGIN
 raise info'INT8 type value is %',tempdata;  
END;
$$ LANGUAGE plpgsql;
--create function with int4 parameter
CREATE OR REPLACE FUNCTION FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_063(tempdata INT4) RETURNS VOID AS $$
BEGIN
 raise info'INT4 type value is %',tempdata;  
END;
$$ LANGUAGE plpgsql;
--create function with int2 parameter
CREATE OR REPLACE FUNCTION FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_063(tempdata INT2) RETURNS VOID AS $$
BEGIN
 raise info'INT2 type value is %',tempdata;  
END;
$$ LANGUAGE plpgsql;
--create function with raw parameter
CREATE OR REPLACE FUNCTION FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_063(tempdata RAW) RETURNS VOID AS $$
BEGIN
 raise info'RAW type value is %',tempdata;  
END;
$$ LANGUAGE plpgsql;
--select numeric function
select FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_063(cast('123.456' as text));
--select float8 function
drop function FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_063(NUMERIC);
select FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_063(cast('123.456' as text));
--select float4 function
drop function FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_063(FLOAT8);
select FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_063(cast('123.456' as text));
--select int8 function
drop function FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_063(FLOAT4);
select FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_063(cast('123.456' as text));
--select int4 function
drop function FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_063(INT8);
select FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_063(cast('123.56' as text));
--select int2 function
drop function FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_063(INT4);
select FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_063(cast('123.456' as text));
--select raw function, failed! donot have text to raw implicit cast
drop function FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_063(INT2);
select FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_063(cast('123' as text));

--create function with text parameter
CREATE OR REPLACE FUNCTION FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_064(tempdata TEXT) RETURNS VOID AS $$
BEGIN
 raise info'TEXT type value is %',tempdata;  
END;
$$ LANGUAGE plpgsql;
--create function with varchar parameter
CREATE OR REPLACE FUNCTION FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_064(tempdata VARCHAR) RETURNS VOID AS $$
BEGIN
 raise info'VARCHAR type value is %',tempdata;  
END;
$$ LANGUAGE plpgsql;
--select text function
select FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_064(cast('2004-10-19 10:23:54' as timestamp));
--select varchar function
drop function FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_064(TEXT);
select FVT_GAUSSDB_ADAPT_1.IMPLICIT_CONVERSION_064(cast('2004-10-19 10:23:54' as timestamp));

--drop schema
DROP SCHEMA FVT_GAUSSDB_ADAPT_1;
--
-- INT1
--

CREATE TABLE TINYINT_TBL(f1 tinyint);


INSERT INTO TINYINT_TBL(f1) VALUES (123.5     );
INSERT INTO TINYINT_TBL(f1) VALUES ('20     ');
INSERT INTO TINYINT_TBL(f1) VALUES ('');

INSERT INTO TINYINT_TBL(f1) VALUES (null);

-- largest and smallest values
INSERT INTO TINYINT_TBL(f1) VALUES ('255');

INSERT INTO TINYINT_TBL(f1) VALUES ('0'); 

-- bad input values -- should give errors
INSERT INTO TINYINT_TBL(f1) VALUES ('    -1');
INSERT INTO TINYINT_TBL(f1) VALUES ('256');
INSERT INTO TINYINT_TBL(f1) VALUES ('asdf');
INSERT INTO TINYINT_TBL(f1) VALUES ('     ');
INSERT INTO TINYINT_TBL(f1) VALUES ('   asdf   ');
INSERT INTO TINYINT_TBL(f1) VALUES ('- 1234');
INSERT INTO TINYINT_TBL(f1) VALUES ('123       5');
INSERT INTO TINYINT_TBL(f1) VALUES ('34.5');


SELECT '' AS six, * FROM TINYINT_TBL ORDER BY 1,2;

SELECT '' AS three, i.* FROM TINYINT_TBL i WHERE i.f1 <> int2 '0' ORDER BY 1,2;

SELECT '' AS three, i.* FROM TINYINT_TBL i WHERE i.f1 <> tinyint '0' ORDER BY 1,2;

SELECT '' AS one, i.* FROM TINYINT_TBL i WHERE i.f1 = int2 '0' ORDER BY 1,2;

SELECT '' AS one, i.* FROM TINYINT_TBL i WHERE i.f1 = tinyint '0' ORDER BY 1,2;

SELECT '' AS is_zone, i.* FROM TINYINT_TBL i WHERE i.f1 < int2 '0' ORDER BY 1,2;

SELECT '' AS is_zone, i.* FROM TINYINT_TBL i WHERE i.f1 < tinyint '0' ORDER BY 1,2;

SELECT '' AS one, i.* FROM TINYINT_TBL i WHERE i.f1 <= int2 '0' ORDER BY 1,2;

SELECT '' AS one, i.* FROM TINYINT_TBL i WHERE i.f1 <= tinyint '0' ORDER BY 1,2;

SELECT '' AS three, i.* FROM TINYINT_TBL i WHERE i.f1 > int2 '0' ORDER BY 1,2;

SELECT '' AS three, i.* FROM TINYINT_TBL i WHERE i.f1 > tinyint '0' ORDER BY 1,2;

SELECT '' AS four, i.* FROM TINYINT_TBL i WHERE i.f1 >= int2 '0' ORDER BY 1,2;

SELECT '' AS four, i.* FROM TINYINT_TBL i WHERE i.f1 >= tinyint '0' ORDER BY 1,2;

-- positive odds
SELECT '' AS one, i.* FROM TINYINT_TBL i WHERE (i.f1 % int2 '2') = int2 '1' ORDER BY 1,2;

-- any evens
SELECT '' AS three, i.* FROM TINYINT_TBL i WHERE (i.f1 % int4 '2') = int2 '0' ORDER BY 1,2;

SELECT '' AS out_limit, i.f1, i.f1 * tinyint '2' AS x FROM TINYINT_TBL i ORDER BY 1,2;

SELECT '' AS three, i.f1, i.f1 * tinyint '2' AS x FROM TINYINT_TBL i
WHERE abs(f1) < 128 ORDER BY 1,2;



SELECT '' AS out_limit, i.f1, i.f1 + tinyint '1' AS x FROM TINYINT_TBL i ORDER BY 1,2;

SELECT '' AS three, i.f1, i.f1 + tinyint '1' AS x FROM TINYINT_TBL i
WHERE f1 < 255 ORDER BY 1,2;



SELECT '' AS out_limit, i.f1, i.f1 - tinyint '1' AS x FROM TINYINT_TBL i ORDER BY 1,2;

SELECT '' AS three, i.f1, i.f1 - tinyint '1' AS x FROM TINYINT_TBL ORDER BY 1,2i
WHERE f1 > 0;

SELECT '' AS two, i.f1, i.f1 - int4 '1' AS x FROM TINYINT_TBL i ORDER BY 1,2;

SELECT '' AS six, i.f1, i.f1 / int2 '2' AS x FROM TINYINT_TBL i ORDER BY 1,2;

SELECT '' AS six, i.f1, i.f1 / int4 '2' AS x FROM TINYINT_TBL i ORDER BY 1,2;

SELECT ''AS two, * FROM TINYINT_TBL WHERE f1 is null ORDER BY 1,2;
SELECT ''AS zero, * FROM TINYINT_TBL WHERE f1='' ORDER BY 1,2;
--
-- more complex expressions
--
update TINYINT_TBL set f1=f1+1 ; 

drop table TINYINT_TBL;

-- variations on unary minus parsing
SELECT tinyint '2' * int2 '2' = int2 '16' / int2 '4' AS true;
SELECT tinyint '2' * int2 '2' = int2 '16' / int4 '4' AS true;
SELECT tinyint '2' * int4 '2' = int4 '16' / int2 '4' AS true;
SELECT tinyint '100' < tinyint '99' AS false;

-- corner case
SELECT (1::tinyint<<8)::text;       
SELECT ((1::tinyint<<8)-1)::text;

show convert_string_to_digit;
select 2 > '15'::text;
set convert_string_to_digit = off;
select 2 > '15'::text;

SELECT pg_catalog.array_dims(' '::anyarray) AS a
,LENGTH(pg_catalog.array_dims(' '::anyarray)) AS b
;
SELECT pg_catalog.array_to_string(' '::anyarray,' '::text) AS a
,LENGTH(pg_catalog.array_to_string(' '::anyarray,' '::text)) AS b
;
SELECT pg_catalog.array_to_string(' '::anyarray,' '::text,''::text) AS a
,LENGTH(pg_catalog.array_to_string(' '::anyarray,' '::text,' '::text)) AS b
;
SELECT pg_catalog.array_dims('{1,2,3}'::anyarray) AS a
,LENGTH(pg_catalog.array_dims('{1,2,3}'::anyarray)) AS b
;
