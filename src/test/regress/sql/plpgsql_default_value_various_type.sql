create schema test_pkg_default_value;
set current_schema = test_pkg_default_value;

-- test default expr
CREATE FUNCTION func1(num2 inout int,num3 inout int) RETURNS int
AS $$
DECLARE
BEGIN
num2 := num2 + 9 + num3;
return num2;
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION func3(num2 inout int) RETURNS int
AS $$
DECLARE
BEGIN
num2 := num2 + 2;
return num2;
END
$$
LANGUAGE plpgsql;

CREATE FUNCTION func2(num1 inout int, st1 inout varchar(10), num2 inout int) RETURNS int
AS $$
DECLARE
BEGIN
  num2 := num2 + 9;
  raise info 'str1 is %', NVL(str1,'NULL');
  raise info 'str2 is %', NVL(str2,'NULL');
  raise info 'num1 is %', num1;
  raise info 'num2 is %', num2;
return num2;
END
$$
LANGUAGE plpgsql;

CREATE FUNCTION func4(num2 inout int,num3 inout bool) RETURNS int
AS $$
DECLARE
BEGIN
num2 := num2 + 1;
return num2;
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE PACKAGE r_types IS
  TYPE r_type_1 IS RECORD (f int := (+1));
END r_types;
/
select adsrc from PG_ATTRDEF where adrelid in (select typrelid from PG_TYPE where typname ilike '%r_type_1%' limit 1);

DECLARE
  r1 r_types.r_type_1;
BEGIN
  raise info 'r1.f is %', r1.f;
END;
/

CREATE OR REPLACE PACKAGE r_types IS
  TYPE r_type_1 IS RECORD (f int := 1+-9+8*(-7)+1*-2);
END r_types;
/
select adsrc from PG_ATTRDEF where adrelid in (select typrelid from PG_TYPE where typname ilike '%r_type_1%' limit 1);

DECLARE
  r1 r_types.r_type_1;
BEGIN
  raise info 'r1.f is %', r1.f;
END;
/

CREATE OR REPLACE PACKAGE r_types IS
  TYPE r_type_1 IS RECORD (f int := 1+-9+8/-11*(-7)+1*-2*2/5);
END r_types;
/
select adsrc from PG_ATTRDEF where adrelid in (select typrelid from PG_TYPE where typname ilike '%r_type_1%' limit 1);

DECLARE
  r1 r_types.r_type_1;
BEGIN
  raise info 'r1.f is %', r1.f;
END;
/

CREATE OR REPLACE PACKAGE r_types IS
  TYPE r_type_1 IS RECORD (f int := 8*-11*(-7)+1*-2*2/5+9);
END r_types;
/
select adsrc from PG_ATTRDEF where adrelid in (select typrelid from PG_TYPE where typname ilike '%r_type_1%' limit 1);

DECLARE
  r1 r_types.r_type_1;
BEGIN
  raise info 'r1.f is %', r1.f;
END;
/

CREATE OR REPLACE PACKAGE r_types IS
  TYPE r_type_1 IS RECORD (f int := -1);
END r_types;
/
select adsrc from PG_ATTRDEF where adrelid in (select typrelid from PG_TYPE where typname ilike '%r_type_1%' limit 1);

DECLARE
  r1 r_types.r_type_1;
BEGIN
  raise info 'r1.f is %', r1.f;
END;
/

CREATE OR REPLACE PACKAGE r_types IS
  TYPE r_type_1 IS RECORD (f VARCHAR2(5) := 'abus');
END r_types;
/
select adsrc from PG_ATTRDEF where adrelid in (select typrelid from PG_TYPE where typname ilike '%r_type_1%' limit 1);

DECLARE
  r1 r_types.r_type_1;
BEGIN
  raise info 'r1.f is %', NVL(r1.f,'NULL');
END;
/

-- test expr error
CREATE OR REPLACE PACKAGE r_types IS
  TYPE r_type_1 IS RECORD (f int := (1+));
END r_types;
/

CREATE OR REPLACE PACKAGE r_types IS
  TYPE r_type_1 IS RECORD (f int := +);
END r_types;
/

CREATE OR REPLACE PACKAGE r_types IS
  TYPE r_type_1 IS RECORD (f int := (+));
END r_types;
/

CREATE OR REPLACE PACKAGE r_types IS
  TYPE r_type_1 IS RECORD (f int := (a+b));
END r_types;
/

-- normal default
CREATE OR REPLACE PACKAGE r_types IS
  TYPE r_type_1 IS RECORD (f VARCHAR2(5) := 'abcde', f2 VARCHAR2(5) := 'waxtr');
END r_types;
/
DECLARE
 TYPE r_type_2 IS RECORD (f VARCHAR2(5) := 'a1c2e', f2 VARCHAR2(5) := 'w5x3r');
  r1 r_types.r_type_1;
  r2 r_type_2;
BEGIN
  raise info 'r1.f is %', NVL(r1.f,'NULL');
  raise info 'r1.f2 is %', NVL(r1.f2,'NULL');
  raise info 'r2.f is %', NVL(r2.f,'NULL');
  raise info 'r1.f2 is %', NVL(r2.f2,'NULL');
END;
/	

CREATE OR REPLACE PACKAGE r_types IS
  TYPE r_type_1 IS RECORD (f date := '2001-9-28', f2 timestamp  := '1957-06-13');
END r_types;
/	
DECLARE
  TYPE r_type_2 IS RECORD (f date := '2000-7-28', f2 timestamp  := '1987-06-03');
  r1 r_types.r_type_1;
  r2 r_type_2;
BEGIN
  raise info 'r1.f is %', r1.f;
  raise info 'r1.f2 is %', r1.f2;
  raise info 'r1.f is %', r2.f;
  raise info 'r1.f2 is %', r2.f2;
END;
/

CREATE OR REPLACE PACKAGE r_types IS
  TYPE r_type_1 IS RECORD (f boolean := true, f2 numeric := 19.578);
END r_types;
/	
DECLARE
  TYPE r_type_2 IS RECORD (f boolean := false, f2 numeric := 13.278);
  r1 r_types.r_type_1;
  r2 r_type_2;
BEGIN
  raise info 'r1.f is %', r1.f;
  raise info 'r1.f2 is %', r1.f2;
  raise info 'r1.f is %', r2.f;
  raise info 'r1.f2 is %', r2.f2;
END;
/

CREATE OR REPLACE PACKAGE r_types IS
  TYPE r_type_1 IS RECORD (f money := 10.5, f2 int := 15);
END r_types;
/	
DECLARE
  TYPE r_type_2 IS RECORD (f money := 9.6, f2 int := 13);
  r1 r_types.r_type_1;
  r2 r_type_2;
BEGIN
  raise info 'r1.f is %', r1.f;
  raise info 'r1.f2 is %', r1.f2;
  raise info 'r1.f is %', r2.f;
  raise info 'r1.f2 is %', r2.f2;
END;
/

CREATE OR REPLACE PACKAGE r_types IS
  TYPE r_type_1 IS RECORD (f integer := 10, f2 oid := 15);
END r_types;
/	
DECLARE
  TYPE r_type_2 IS RECORD (f integer := 9, f2 oid := 13);
  r1 r_types.r_type_1;
  r2 r_type_2;
BEGIN
  raise info 'r1.f is %', r1.f;
  raise info 'r1.f2 is %', r1.f2;
  raise info 'r1.f is %', r2.f;
  raise info 'r1.f2 is %', r2.f2;
END;
/

CREATE OR REPLACE PACKAGE r_types IS
  TYPE r_type_1 IS RECORD (f name := 'abcde', f2 text := 'waxtr');
END r_types;
/
DECLARE
 TYPE r_type_2 IS RECORD (f name := 'a1c2e', f2 text := 'w5x3r');
  r1 r_types.r_type_1;
  r2 r_type_2;
BEGIN
  raise info 'r1.f is %', NVL(r1.f,'NULL');
  raise info 'r1.f2 is %', NVL(r1.f2,'NULL');
  raise info 'r2.f is %', NVL(r2.f,'NULL');
  raise info 'r1.f2 is %', NVL(r2.f2,'NULL');
END;
/

-- pkg param default
CREATE OR REPLACE PACKAGE r_types IS
  TYPE r_type_1 IS RECORD (f VARCHAR2(5) := 'abcde', f2 VARCHAR2(5) := 'syutw');
  TYPE r_type_2 IS RECORD (f VARCHAR2(5));
END r_types;
/

CREATE OR REPLACE PROCEDURE p1 (
  x OUT r_types.r_type_1,
  y OUT r_types.r_type_2,
  z OUT VARCHAR2) 
AUTHID CURRENT_USER IS
BEGIN
  raise info 'x.f is %', NVL(x.f,'NULL');
  raise info 'x.f2 is %', NVL(x.f2,'NULL');
  raise info 'y.f is %', NVL(y.f,'NULL');
  raise info 'z is %', NVL(z,'NULL');
END;
/

CREATE OR REPLACE PROCEDURE p2 (
  x IN r_types.r_type_1,
  y IN r_types.r_type_2,
  z IN VARCHAR2) 
AUTHID CURRENT_USER IS
BEGIN
  raise info 'x.f is %', NVL(x.f,'NULL');
  raise info 'x.f2 is %', NVL(x.f2,'NULL');
  raise info 'y.f is %', NVL(y.f,'NULL');
  raise info 'z is %', NVL(z,'NULL');
END;
/

CREATE OR REPLACE PROCEDURE p3 (
  x INOUT r_types.r_type_1,
  y INOUT r_types.r_type_2,
  z INOUT VARCHAR2) 
AUTHID CURRENT_USER IS
BEGIN
  raise info 'x.f is %', NVL(x.f,'NULL');
  raise info 'x.f2 is %', NVL(x.f2,'NULL');
  raise info 'y.f is %', NVL(y.f,'NULL');
  raise info 'z is %', NVL(z,'NULL');
END;
/
DECLARE
  r1 r_types.r_type_1;
  r2 r_types.r_type_2;
  s  VARCHAR2(5) := 'fghij';
BEGIN
  raise info '===============p1===================';
  p1 (r1, r2, s);
  raise info '===============p2===================';
  p2 (r1, r2, s);
  raise info '===============p3===================';
  p3 (r1, r2, s);
  r1.f:='waqrt';
  r1.f2:='bfgrf';
  r2.f:='nuytg';
  raise info '===============p1===================';
  p1 (r1, r2, s);
  raise info '===============p2===================';
  p2 (r1, r2, s);
  raise info '===============p3===================';
  p3 (r1, r2, s);
END;
/

DROP FUNCTION func1;
DROP FUNCTION func2;
DROP FUNCTION func3;
DROP FUNCTION func4;
DROP PACKAGE r_types;
DROP SCHEMA test_pkg_default_value;