create schema test_float;
set current_schema to test_float;

set behavior_compat_options = 'float_as_numeric, truncate_numeric_tail_zero';
-- test normal functions
CREATE TABLE t1 (a float(1), b float(80), c float(126));
CREATE TABLE t2 (a float);
\d t1;
\d t2;
DROP TABLE t2;
INSERT INTO t1 VALUES (0,0,0);
INSERT INTO t1 VALUES (123.4567890123456789012345678901234567890123,123.4567890123456789012345678901234567890123,123.4567890123456789012345678901234567890123);
INSERT INTO t1 VALUES (1234567890123456789012345678901234567890123,1234567890123456789012345678901234567890123,1234567890123456789012345678901234567890123);
SELECT * FROM t1;
DELETE FROM t1 where a = 123.4567890123456789012345678901234567890123;
DELETE FROM t1;
INSERT INTO t1 VALUES (NULL, NULL, NULL);
UPDATE t1 SET a = 999999999, b = 9999.99999999999999, c = 0.000000000999999999999999;
SELECT * FROM t1;
DROP TABLE t1;

CREATE TABLE float_test (
  float2 FLOAT(2),
  float10 FLOAT(10),
  float20 FLOAT(20)
);
\d float_test;
INSERT INTO float_test (float2, float10, float20) VALUES (93.5, 93.5, 93.5);    
INSERT INTO float_test (float2, float10, float20) VALUES (13884.2, 13884.2, 13884.2);
INSERT INTO float_test (float2, float10, float20) VALUES (123.456, 123.456, 123.456);
INSERT INTO float_test (float2, float10, float20) VALUES (0.00123, 0.00123, 0.00123);
INSERT INTO float_test (float2, float10, float20) VALUES (-93.5, -93.5, -93.5);    
INSERT INTO float_test (float2, float10, float20) VALUES (-13884.2, -13884.2, -13884.2);
INSERT INTO float_test (float2, float10, float20) VALUES (-123.456, -123.456, -123.456);
INSERT INTO float_test (float2, float10, float20) VALUES (-0.00123, -0.00123, -0.00123);
SELECT * FROM float_test;
DROP TABLE float_test;

-- boundary test
set behavior_compat_options = 'float_as_numeric, truncate_numeric_tail_zero';
CREATE TABLE t1(a float(0));
CREATE TABLE t1(a float(-432));
CREATE TABLE t1(a float(127));
CREATE TABLE t1(a float(1277));

set behavior_compat_options = 'float_as_numeric, truncate_numeric_tail_zero';
CREATE TABLE t1(a float(1), b float(126), c numeric);
INSERT INTO t1(a) VALUES (222^222222222222222);
INSERT INTO t1(b) VALUES (222^222222222222222);
INSERT INTO t1(c) VALUES (222^222222222222222);
INSERT INTO t1(a) VALUES (-222^222222222222222);
INSERT INTO t1(b) VALUES (-222^222222222222222);
INSERT INTO t1(c) VALUES (-222^222222222222222);
INSERT INTO t1 SELECT null, null, null;
INSERT INTO t1 SELECT 'NaN', 'NaN', 'NaN';
SELECT * FROM t1;
DROP TABLE t1;

-- alter table test
set behavior_compat_options = 'float_as_numeric, truncate_numeric_tail_zero';
CREATE TABLE t1(col1 int, col2 float(5), col3 float(44), col4 float(20));
INSERT INTO t1 VALUES (1, 12345678901234.567890123456789, 123456789123456789123456789123456789123456789.123456789123456789123456789123456789, 123.123);
INSERT INTO t1 VALUES (12, 0.12345678901234567890123456789, 0.123456789123456789123456789123456789123456789123456789123456789123456789123456789,123.123);
INSERT INTO t1 VALUES (21, 123456789.12345,123456789123456789123456789.12345678912, 123.123);
SELECT pg_get_tabledef('t1');
SELECT * FROM t1 ORDER BY col1;
-- alter table not empty
ALTER TABLE t1 MODIFY (col1 float(1)); -- error
-- alter table empty
DELETE FROM t1;
SELECT * FROM t1 ORDER BY col1;
ALTER TABLE t1 MODIFY (col1 float(1)); -- success
SELECT pg_get_tabledef('t1');
SELECT * FROM t1 ORDER BY col1;
DROP TABLE t1;

-- PL/SQL test

CREATE OR REPLACE PACKAGE pak1 as 
var1 float(16);
var2 float(120);
type tp_tb1 is table of var1%type;
tb1 tp_tb1;
type tp_tb2 is table of var2%type;
tb2 tp_tb2;
procedure p1;
end pak1;
/
CREATE OR REPLACE package body pak1 as 
procedure p1 as
begin
tb1 = tp_tb1(1234244, 12.32432456, 0.00000002342, -32994, -23.000345, -0.32424234);
raise info '%', tb1;
tb2 = tp_tb2(1234244, 12.32432456, 0.00000002342, -32994, -23.000345, -0.32424234);
raise info '%', tb2;
end;
end pak1;
/
call pak1.p1();
DROP PACKAGE pak1;

reset behavior_compat_options;
reset current_schema;
drop schema test_float cascade;
