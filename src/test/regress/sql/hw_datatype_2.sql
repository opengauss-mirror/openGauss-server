--change datestyle to Postgres
SET Datestyle = 'Postgres,DMY';
--change datestyle to ISO mode which is default output style of GaussDB
set datestyle = iso,ymd;
create schema hw_datatype_2;
set search_path = hw_datatype_2;
--TEST FOR A NEW TYPE TINYINT
--TEST FOR INT1SEND IN INT2
DROP TABLE IF EXISTS TEST_TINYINT;
CREATE TABLE TEST_TINYINT (ID INT2);
INSERT INTO TEST_TINYINT VALUES (2);
SELECT * FROM TEST_TINYINT;
INSERT INTO TEST_TINYINT VALUES (0);
INSERT INTO TEST_TINYINT VALUES (255);
INSERT INTO TEST_TINYINT VALUES (-1);
INSERT INTO TEST_TINYINT VALUES (256);
INSERT INTO TEST_TINYINT VALUES (1000);
SELECT INT1SEND(ID) FROM TEST_TINYINT WHERE ID >= 0 AND ID <= 255 ORDER BY 1;
SELECT INT1SEND(ID) FROM TEST_TINYINT WHERE ID > 255 ORDER BY 1;
SELECT * FROM TEST_TINYINT ORDER BY 1;
DROP TABLE TEST_TINYINT;

/*
 * adapt Sybase, add a new data type tinyint 
 *		tinyinteq			- returns 1 if arg1 == arg2
 *		tinyintne			- returns 1 if arg1 != arg2
 *		tinyintlt			- returns 1 if arg1 < arg2
 *		tinyintle			- returns 1 if arg1 <= arg2
 *		tinyintgt			- returns 1 if arg1 > arg2
 *		tinyintge			- returns 1 if arg1 >= arg2
 */
DROP TABLE IF EXISTS TEST_TINYINT;

CREATE TABLE TEST_TINYINT (ID TINYINT,IT TINYINT);

INSERT INTO TEST_TINYINT VALUES (2,2);
INSERT INTO TEST_TINYINT VALUES (0,5);
INSERT INTO TEST_TINYINT VALUES (254,255);
INSERT INTO TEST_TINYINT VALUES (0,0);
INSERT INTO TEST_TINYINT VALUES (255,255);
INSERT INTO TEST_TINYINT VALUES (-1,-1);
INSERT INTO TEST_TINYINT VALUES (1, -0.4);
INSERT INTO TEST_TINYINT VALUES (256,256);
INSERT INTO TEST_TINYINT VALUES (25,20);
INSERT INTO TEST_TINYINT VALUES (1000,1000);

SELECT * FROM TEST_TINYINT ORDER BY 1, 2;
SELECT INT1EQ(ID,IT) AS THREE FROM TEST_TINYINT ORDER BY 1;
SELECT INT1NE(ID,IT) AS THREE FROM TEST_TINYINT ORDER BY 1;
SELECT INT1LT(ID,IT) AS ONE FROM TEST_TINYINT ORDER BY 1;
SELECT INT1LE(ID,IT) AS FOUR FROM TEST_TINYINT ORDER BY 1;
SELECT INT1GT(ID,IT) AS ONE FROM TEST_TINYINT ORDER BY 1;
SELECT INT1GE(ID,IT) AS FOUR FROM TEST_TINYINT ORDER BY 1;

DROP TABLE IF EXISTS TEST_TINYINT;
CREATE TABLE TEST_TINYINT (ID TINYINT,IT TINYINT);
INSERT INTO TEST_TINYINT VALUES (2,2);
INSERT INTO TEST_TINYINT VALUES (0,5);
INSERT INTO TEST_TINYINT VALUES (15,5);
INSERT INTO TEST_TINYINT VALUES (255,5);
INSERT INTO TEST_TINYINT VALUES (150,3);
INSERT INTO TEST_TINYINT VALUES (50,0);

SELECT ID,IT,int1pl(ID,IT) FROM TEST_TINYINT WHERE ID < 100 ORDER BY 1,2;
SELECT ID,IT,int1pl(ID,IT) FROM TEST_TINYINT WHERE ID > 100 ORDER BY 1,2;
SELECT ID,IT,int1mi(ID,IT) FROM TEST_TINYINT WHERE ID < 100 ORDER BY 1,2;
SELECT ID,IT,int1mi(ID,IT) FROM TEST_TINYINT WHERE ID > 100 ORDER BY 1,2;
SELECT ID,IT,int1mul(ID,IT) FROM TEST_TINYINT ORDER BY 1,2;
SELECT ID,IT,int1div(ID,IT) FROM TEST_TINYINT ORDER BY 1,2;

SELECT ID,IT,int1and(ID,IT) FROM TEST_TINYINT ORDER BY 1,2;
SELECT ID,IT,int1or(ID,IT) FROM TEST_TINYINT ORDER BY 1,2;	
SELECT ID,IT,int1xor(ID,IT) FROM TEST_TINYINT ORDER BY 1,2;
SELECT ID,IT,int1not(IT) FROM TEST_TINYINT ORDER BY 1,2;	

SELECT ID,IT,int1shl(ID,IT) FROM TEST_TINYINT ORDER BY 1,2;
SELECT ID,IT,int1shr(ID,IT) FROM TEST_TINYINT ORDER BY 1,2;
	
SELECT ID,IT,int1larger(ID,IT) FROM TEST_TINYINT ORDER BY 1,2;
SELECT ID,IT,int1smaller(ID,IT) FROM TEST_TINYINT ORDER BY 1,2;
	
SELECT int1abs(ID) FROM TEST_TINYINT ORDER BY 1;
SELECT int1mod(ID,IT) FROM TEST_TINYINT ORDER BY 1;
	  
SELECT int1um(ID) FROM TEST_TINYINT ORDER BY 1;
SELECT int1up(ID) FROM TEST_TINYINT ORDER BY 1;
SELECT int1inc(ID) FROM TEST_TINYINT ORDER BY 1;
 
/*
 * adapt Sybase, add a new data type tinyint
 * test for order by 
 */
DROP TABLE IF EXISTS TEST_TINYINT;

CREATE TABLE TEST_TINYINT (ID INT1);

INSERT INTO TEST_TINYINT VALUES (1);
INSERT INTO TEST_TINYINT VALUES (2);
INSERT INTO TEST_TINYINT VALUES (3);
INSERT INTO TEST_TINYINT VALUES (4);

SELECT * FROM TEST_TINYINT ORDER BY 1;

SELECT ID FROM TEST_TINYINT ORDER BY ID;

/*
 * adapt Sybase, add a new data type tinyint
 *		i1toi2			- convert int1 to int2
 *		i1toi4			- convert int1 to int4
 *		i1toi8			- convert int1 to int8
 *		i1tof4			- convert int1 to float4
 *		i1tof8			- convert int1 to float8
 *		int1_bool		- convert int1 to bool
 *		int1_numeric	- convert int1 to numeric
 *		i2toi1			- convert int2 to int1
 *		i4toi1			- convert int4 to int1
 *		i8toi1			- convert int8 to int1
 *		f4toi1			- convert float4 to int1
 *		f8toi1			- convert float8 to int1
 *		bool_int1		- convert bool to int1
 *		numeric_int1	- convert numeric to int1
 */
DROP TABLE IF EXISTS TEST_TINYINT;

CREATE TABLE TEST_TINYINT (ID INT1);

INSERT INTO TEST_TINYINT VALUES (1);

SELECT * FROM TEST_TINYINT;

SELECT I1TOI2(ID) FROM TEST_TINYINT ORDER BY 1;
SELECT I1TOI4(ID) FROM TEST_TINYINT ORDER BY 1;
SELECT I1TOI8(ID) FROM TEST_TINYINT ORDER BY 1;
SELECT I1TOF4(ID) FROM TEST_TINYINT ORDER BY 1;
SELECT I1TOF8(ID) FROM TEST_TINYINT ORDER BY 1;
SELECT BOOL(ID) FROM TEST_TINYINT ORDER BY 1;

DROP TABLE TEST_TINYINT;

DROP TABLE IF EXISTS TEST_TINYINT;
CREATE TABLE TEST_TINYINT (ID int2);
INSERT INTO TEST_TINYINT VALUES (1);
SELECT i2toi1(id) FROM TEST_TINYINT;
DROP TABLE TEST_TINYINT;

DROP TABLE IF EXISTS TEST_TINYINT;
CREATE TABLE TEST_TINYINT (ID int4);
INSERT INTO TEST_TINYINT VALUES (1);
INSERT INTO TEST_TINYINT VALUES (0);
INSERT INTO TEST_TINYINT VALUES (255);
INSERT INTO TEST_TINYINT VALUES (-1);
INSERT INTO TEST_TINYINT VALUES (256);
SELECT i4toi1(id) FROM TEST_TINYINT;
DROP TABLE TEST_TINYINT;

DROP TABLE IF EXISTS TEST_TINYINT;
CREATE TABLE TEST_TINYINT (ID int8);
INSERT INTO TEST_TINYINT VALUES (1);
INSERT INTO TEST_TINYINT VALUES (0);
INSERT INTO TEST_TINYINT VALUES (255);
INSERT INTO TEST_TINYINT VALUES (-1);
INSERT INTO TEST_TINYINT VALUES (256);
SELECT i8toi1(id) FROM TEST_TINYINT;

DROP TABLE IF EXISTS TEST_TINYINT;
CREATE TABLE TEST_TINYINT (ID float4);
INSERT INTO TEST_TINYINT VALUES (1);
INSERT INTO TEST_TINYINT VALUES (0);
INSERT INTO TEST_TINYINT VALUES (255);
INSERT INTO TEST_TINYINT VALUES (-1);
INSERT INTO TEST_TINYINT VALUES (256);
SELECT f4toi1(id) FROM TEST_TINYINT;


DROP TABLE IF EXISTS TEST_TINYINT;
CREATE TABLE TEST_TINYINT (ID float8);
INSERT INTO TEST_TINYINT VALUES (1);
INSERT INTO TEST_TINYINT VALUES (0);
INSERT INTO TEST_TINYINT VALUES (255);
INSERT INTO TEST_TINYINT VALUES (-1);
INSERT INTO TEST_TINYINT VALUES (256);
SELECT f8toi1(id) FROM TEST_TINYINT;

DROP TABLE TEST_TINYINT;

SELECT BOOL_INT1('t');
SELECT BOOL_INT1('f');
SELECT BOOL_INT1(null);

DROP TABLE IF EXISTS TEST_TINYINT;
CREATE TABLE TEST_TINYINT (ID INT1);
INSERT INTO TEST_TINYINT VALUES (1);
SELECT int1_NUMERIC(id) FROM TEST_TINYINT;
SELECT ID::OID FROM TEST_TINYINT;
SELECT ID::TEXT FROM TEST_TINYINT;
SELECT ID::VARCHAR FROM TEST_TINYINT;
SELECT ID::REGPROC FROM TEST_TINYINT;
SELECT ID::REGOPER FROM TEST_TINYINT;
SELECT ID::REGOPERATOR FROM TEST_TINYINT;
SELECT ID::REGCLASS FROM TEST_TINYINT;
SELECT ID::REGTYPE FROM TEST_TINYINT;
SELECT ID::REGCONFIG FROM TEST_TINYINT;
SELECT ID::INTERVAL FROM TEST_TINYINT;
SELECT ID::REGDICTIONARY  FROM TEST_TINYINT;
DROP TABLE TEST_TINYINT;

DROP TABLE IF EXISTS TEST_TINYINT;
CREATE TABLE TEST_TINYINT (ID NUMERIC);
INSERT INTO TEST_TINYINT VALUES (1);
INSERT INTO TEST_TINYINT VALUES (0);
INSERT INTO TEST_TINYINT VALUES (255);
INSERT INTO TEST_TINYINT VALUES (-1);
INSERT INTO TEST_TINYINT VALUES (256);
SELECT numeric_int1(id) FROM TEST_TINYINT;
DROP TABLE TEST_TINYINT;

DROP TABLE IF EXISTS TEST_TINYINT;
CREATE TABLE TEST_TINYINT (ID int1);
INSERT INTO TEST_TINYINT VALUES (1);
SELECT int1_numeric(id) FROM TEST_TINYINT;
DROP TABLE TEST_TINYINT;

DROP TABLE IF EXISTS TEST_TINYINT;
CREATE TABLE TEST_TINYINT (ID text);
INSERT INTO TEST_TINYINT VALUES (1);
SELECT id :: INT1 FROM TEST_TINYINT;
DROP TABLE TEST_TINYINT;

---add for test of new data type tinyint
DROP TABLE IF EXISTS TEST;
CREATE TABLE TEST (ID TEXT);
INSERT INTO TEST VALUES ('100');
SELECT INT1(ID) FROM TEST;

INSERT INTO TEST VALUES ('1001');
SELECT INT1(ID) FROM TEST;
DROP TABLE TEST;

DROP TABLE IF EXISTS TEST;
CREATE TABLE TEST (ID INT1);
INSERT INTO TEST VALUES (12);
INSERT INTO TEST VALUES (123);
SELECT TO_TEXT(ID) FROM TEST ORDER BY ID;
SELECT TO_TEXT(ID) FROM TEST ORDER BY ID;
SELECT NUMTODAY(ID) FROM TEST  ORDER BY 1;
DROP TABLE TEST;

DROP TABLE IF EXISTS TEST_TINYINT;
/*CREATE TABLE TEST_TINYINT (ID INT1 primary key);
INSERT INTO TEST_TINYINT VALUES (15);
INSERT INTO TEST_TINYINT VALUES (22);
INSERT INTO TEST_TINYINT VALUES (13);
INSERT INTO TEST_TINYINT VALUES (1);
SELECT * FROM TEST_TINYINT;
SELECT * FROM TEST_TINYINT ORDER BY ID;
SELECT * FROM TEST_TINYINT WHERE ID LIKE '1';
SELECT * FROM TEST_TINYINT WHERE ID LIKE '1%';
DROP TABLE IF EXISTS TEST_TINYINT;*/
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
drop table test_cast;
drop table test_cast1;
drop function test_cast(numeric);

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
select * from test_raw1 test1 cross join test_raw2 test2 where test1.a = test2.b order by 1,2;
select * from test_raw1 test1 join test_raw2 test2 using(a) order by 1;
select * from test_raw1 test1 full join test_raw2 test2 using(a) order by 1,2,3;
select * from test_raw1 test1 left join test_raw2 test2 using(a) order by 1,2,3;
select * from test_raw1 test1 right join test_raw2 test2 using(a) order by 1,2,3;
select * from test_raw1 test1 inner join test_raw2 test2 using(a) order by 1,2,3;
select * from test_raw1 test1 inner join test_raw2 test2 using(a) order by 1,2,3;
select * from test_raw1 test1 natural join test_raw2 test2;
drop table test_raw1;
drop table test_raw2;
reset search_path;
