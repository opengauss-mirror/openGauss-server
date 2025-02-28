-- FOR PL/pgSQL ARRAY of RECORD TYPE scenarios --

-- check compatibility --
show sql_compatibility; -- expect ORA --

-- create new schema --
drop schema if exists plpgsql_assignlist;
create schema plpgsql_assignlist;
set current_schema = plpgsql_assignlist;

-- initialize table and type--
create type o1 as (o1a int, o1b int);
create type o2 as (o2a o1, o2b int);
create type o3 as (o3a o2, o3b int);
create type o4 as (o4a o3, o4b int);
create type o5 as (o5a o2[], o5b int);
create type o6 as (o6a o5, o6b int);

----------------------------------------------------
------------------ START OF TESTS ------------------
----------------------------------------------------

-- test assign list without array: nested record
create or replace function get_age RETURNS integer as $$ 
declare
    type r1 is record  (r1a int, r1b int);
    type r2 is record  (r2a r1, r2b int);
    type r3 is record  (r3a r2, r3b int);
	va r3;
begin
	va.r3a.r2a.r1a := 123;
    raise info '%', va;
    va := (((4,3),2),1);
    raise info '%', va;
    va.r3a.r2a.r1a := 456;
    raise info '%', va;
    return va.r3a.r2a.r1a;
end;
$$ language plpgsql;
select get_age();

-- test assign list without array: nested composite type
create or replace function get_age RETURNS integer as $$ 
declare
	va o4;
begin
    va.o4a.o3a.o2a.o1a := 123;
    raise info '%', va;
    va.o4a.o3a.o2a := (456, 789);
    raise info '%', va;
    return va.o4a.o3a.o2a.o1a;
end;
$$ language plpgsql;
select get_age();

-- test assign list with array: array in first three word
create or replace function get_age RETURNS integer as $$ 
declare
    TYPE o3_arr is VARRAY(10) of o3;
	va o3_arr;
begin
    va(1).o3a.o2a.o1a := 123;
    raise info '%', va;
    va(2).o3a.o2a := (456, 789);
    raise info '%', va;
    va(3).o3a := ((123, 456),789);
    raise info '%', va;
    return va(2).o3a.o2a.o1b;
end;
$$ language plpgsql;
select get_age();


-- test assign list with array: array in first three word
create or replace function get_age RETURNS integer as $$ 
declare
	va o5;
begin
    va.o5a(1).o2a.o1a := 123;
    raise info '%', va;
    va.o5a(2).o2a := (456, 789);
    raise info '%', va;
    va.o5a(3) := ((123, 456),789);
    raise info '%', va;
    return va.o5a(2).o2a.o1a;
end;
$$ language plpgsql;
select get_age();

-- test assign list with array: array in first three word
create or replace function get_age RETURNS integer as $$ 
declare
	va o6;
begin
    va.o6a.o5a(1).o2a.o1a := 123;
    raise info '%', va;
    va.o6a.o5a(2).o2a := (456, 789);
    raise info '%', va;
    va.o6a.o5a(3) := ((123, 456),789);
    raise info '%', va;
    return va.o6a.o5a(2).o2a.o1a;
end;
$$ language plpgsql;
select get_age();

-- test assign list with array: with record nested
create or replace function get_age RETURNS integer as $$ 
declare
	TYPE r1 is RECORD  (r1a int, r1b int);
    TYPE r1_arr is VARRAY(10) of r1;
    TYPE r2 is RECORD (r2a r1_arr);
    va r2;
begin
    va.r2a(1).r1a := 123;
    raise info '%', va.r2a(1).r1a;
    va.r2a(2) := (456, 789);
    raise info '%', va;
    va.r2a(2).r1b := 999;
    raise info '%', va;
    return va.r2a(2).r1b;
end;
$$ language plpgsql;
select get_age();

-- test assign list with table: with record nested
create or replace function get_age RETURNS integer as $$ 
declare
	TYPE r1 is RECORD  (r1a int, r1b int);
    TYPE r1_arr is table of r1 index by varchar2(10);
    TYPE r2 is RECORD (r2a r1_arr);
    va r2;
begin
    va.r2a('a').r1a := 123;
    raise info '%', va.r2a('a').r1a;
    va.r2a('aa') := (456, 789);
    raise info '%', va;
    va.r2a('aa').r1b := 999;
    raise info '%', va;
    return va.r2a('aa').r1b;
end;
$$ language plpgsql;
select get_age();

--test assign list with array: array not in first three word
create or replace function get_age RETURNS integer as $$ 
declare
    TYPE r1 is RECORD (r1a o6, r1b int);
	va r1;
begin
    va.r1a.o6a.o5a(1).o2a.o1a := 123;
    raise info '%', va;
    va.r1a.o6a.o5a(2).o2a := (456, 789);
    raise info '%', va;
    va.r1a.o6a.o5a(3) := ((123, 456),789);
    raise info '%', va;
    return va.r1a.o6a.o5a[2].o2a.o1a;
end;
$$ language plpgsql;
select get_age();

--test assign a value to a variable with a custom type
DECLARE
  TYPE t_rec IS RECORD (
    id   NUMBER,
    val1 VARCHAR2(10),
    val2 VARCHAR2(10),
    val3 VARCHAR2(10)  );
  TYPE t_rec2 IS RECORD (
    id   NUMBER,
    val1 VARCHAR2(10),
    val2 VARCHAR2(10),
    val3 VARCHAR2(10)  );

  l_rec t_rec:= t_rec2(1, ',', 'TWO', 'THREE');
BEGIN
  raise info 'team.LIMIT = %', l_rec.val1;
END;
/

set enable_pltype_name_check = on;
DECLARE
  TYPE t_rec IS RECORD (
    id   NUMBER,
    val1 VARCHAR2(10),
    val2 VARCHAR2(10),
    val3 VARCHAR2(10)  );
  TYPE t_rec2 IS RECORD (
    id   NUMBER,
    val1 VARCHAR2(10),
    val2 VARCHAR2(10),
    val3 VARCHAR2(10)  );

  l_rec t_rec:= t_rec(1, 'ONE', 'TWO', 'THREE');
BEGIN
  raise info 'team.LIMIT = %', l_rec.val1;
END;
/

DECLARE
  TYPE type1 IS RECORD (val1 int, val2 VARCHAR2(10));
  l_recxx type1:=type1(1,upper('one'));
BEGIN
  raise info 'l_recxx is %',l_recxx;
  raise info 'l_recxx is %,%',l_recxx.val1,l_recxx.val2;
END;
/

DECLARE
  TYPE type1 IS RECORD (val1 int, val2 VARCHAR2(10));
  l_recxx type1:=type1(1,lower('ONE'));
BEGIN
  raise info 'l_recxx is %',l_recxx;
  raise info 'l_recxx is %,%',l_recxx.val1,l_recxx.val2;
END;
/

CREATE OR REPLACE FUNCTION funcname(str inout int) RETURNS int
AS $$
DECLARE
BEGIN
return str;
END
$$
LANGUAGE plpgsql;

DECLARE
  TYPE type1 IS RECORD (val1 int, val2 VARCHAR2(10));
  l_recxx type1:=type1(1,funcname(1));
BEGIN
  raise info 'l_recxx is %',l_recxx;
  raise info 'l_recxx is %,%',l_recxx.val1,l_recxx.val2;
END;
/

DROP FUNCTION funcname;

CREATE OR REPLACE FUNCTION funcname(str inout VARCHAR2(10)) RETURNS VARCHAR2(10)
AS $$
DECLARE
BEGIN
return str;
END
$$
LANGUAGE plpgsql;

DECLARE
  TYPE type1 IS RECORD (val1 int, val2 VARCHAR2(10));
  l_recxx type1:=type1(1,funcname('ONE'));
BEGIN
  raise info 'l_recxx is %',l_recxx;
  raise info 'l_recxx is %,%',l_recxx.val1,l_recxx.val2;
END;
/

DECLARE
  TYPE type1 IS RECORD (val1 int, val2 VARCHAR2(10));
  l_recxx type1:=type1(1,funcname('ONE', 'TWO'));
BEGIN
  raise info 'l_recxx is %',l_recxx;
  raise info 'l_recxx is %,%',l_recxx.val1,l_recxx.val2;
END;
/

DECLARE
  TYPE type1 IS RECORD (val1 int, val2 VARCHAR2(10));
  l_recxx type1:=type1(1,uuuu1('ONE', 'TWO'));
BEGIN
  raise info 'l_recxx is %',l_recxx;
  raise info 'l_recxx is %,%',l_recxx.val1,l_recxx.val2;
END;
/

DROP FUNCTION funcname;

DECLARE
  TYPE t_rec IS RECORD (
    id   NUMBER,
    val1 VARCHAR2(10),
    val2 VARCHAR2(10),
    val3 VARCHAR2(10)  );
  TYPE t_rec2 IS RECORD (
    id   NUMBER,
    val1 VARCHAR2(10),
    val2 VARCHAR2(10),
    val3 VARCHAR2(10)  );

  l_rec t_rec:= t_rec2(1, ',', 'TWO', 'THREE');
BEGIN
  raise info 'team.LIMIT = %', l_rec.val1;
END;
/

DECLARE
  TYPE t_rec IS RECORD (
    id   NUMBER,
    val1 VARCHAR2(10),
    val2 VARCHAR2(10),
    val3 VARCHAR2(10)  );
  TYPE t_rec2 IS RECORD (
    id   NUMBER,
    val1 VARCHAR2(10),
    val2 VARCHAR2(10),
    val3 VARCHAR2(10)  );

  l_rec t_rec:= t_rec2(1, 'ONE', 'TWO', 'THREE');
BEGIN
  raise info 'team.LIMIT = %', l_rec.val1;
END;
/

DECLARE
  TYPE t_rec IS RECORD (
    id   NUMBER,
    val1 VARCHAR2(10),
    val2 VARCHAR2(10),
    val3 VARCHAR2(10)  );
  TYPE t_rec2 IS RECORD (
    id   NUMBER,
    val1 VARCHAR2(10),
    val2 VARCHAR2(10),
    val3 VARCHAR2(10)  );

  l_rec t_rec;
BEGIN
  l_rec := t_rec2(1, 'ONE', 'TWO', 'THREE');
END;
/

DECLARE
  TYPE t_rec IS RECORD (
    id   NUMBER,
    val1 VARCHAR2(10),
    val2 VARCHAR2(10),
    val3 VARCHAR2(10)  );

  l_rec t_rec;
BEGIN
  l_rec := t_rec(1, 'ONE', 'TWO', 'THREE');
END;
/

DECLARE
  TYPE t_rec IS RECORD (
    id   NUMBER,
    val1 VARCHAR2(10),
    val2 VARCHAR2(10),
    val3 VARCHAR2(10)  );

  l_rec t_rec;
BEGIN
  l_rec := ROW(1, 'ONE', 'TWO', 'THREE');
END;
/

DECLARE
  TYPE t_rec IS RECORD (
    id   NUMBER,
    val1 VARCHAR2(10),
    val2 VARCHAR2(10),
    val3 VARCHAR2(10)  );

  l_rec t_rec;
BEGIN
  l_rec := row(1, 'ONE', 'TWO', 'THREE');
END;
/

DECLARE
  TYPE t_rec IS RECORD (
    id   NUMBER,
    val1 VARCHAR2(10),
    val2 VARCHAR2(10),
    val3 VARCHAR2(10)  );

  l_rec t_rec;
BEGIN
  l_rec := t_rec2(1, 'ONE', 'TWO', 'THREE');
END;
/

DECLARE
  TYPE nt_type IS TABLE OF INTEGER;
  TYPE va_type IS VARRAY(4) OF INTEGER;
  nt  nt_type;
BEGIN
  nt := va_type(1,3,5);
  raise info 'nt(1) = %', nt(1);
  raise info 'nt(3) = %', nt(3);
END;
/

DECLARE
  TYPE nt_type IS TABLE OF INTEGER;
  TYPE va_type IS VARRAY(4) OF INTEGER;
  nt  nt_type;
BEGIN
  nt := nt_type(1,3,5);
  raise info 'team.LIMIT = %', nt(2);
END;
/

DECLARE
  TYPE nt_type IS TABLE OF INTEGER;
  TYPE va_type IS VARRAY(4) OF INTEGER;
  nt  nt_type:= va_type(1,3,5);
BEGIN
  raise info 'nt(3) = %', nt(1);
END;
/

DECLARE
  TYPE nt_type IS TABLE OF INTEGER;
  TYPE va_type IS VARRAY(4) OF INTEGER;
  nt  nt_type:= nt_type(1,3,5);
BEGIN
  raise info 'nt(3) = %', nt(1);
END;
/

DECLARE
  TYPE va_type IS VARRAY(4) OF INTEGER;
  nt  va_type := va_type(1,3,5);
BEGIN
  raise info 'nt(1) = %', nt(1);
  raise info 'nt(3) = %', nt(3);
END;
/

DECLARE
  TYPE va_type IS VARRAY(4) OF INTEGER;
  nt  va_type;
BEGIN
  nt := va_type(1,3,5);
  raise info 'nt(1) = %', nt(1);
  raise info 'nt(3) = %', nt(3);
END;
/

DECLARE
  TYPE nt_type IS TABLE OF INTEGER;
  nt  nt_type := nt_type(1,3,5);
BEGIN
  raise info 'nt(1) = %', nt(1);
  raise info 'nt(3) = %', nt(3);
END;
/

DECLARE
  TYPE nt_type IS TABLE OF INTEGER;
  nt  nt_type;
BEGIN
  nt := nt_type(1,3,5);
  raise info 'nt(1) = %', nt(1);
  raise info 'nt(3) = %', nt(3);
END;
/

--test nest record
create type to1 as (val1 VARCHAR2(10), val2 VARCHAR2(10));
create type to2 as (val3 VARCHAR2(10), val4 to1);
create type to3 as (val5 VARCHAR2(10), val6 to2, val7 to1);

DECLARE
    l_rec to3:= ('ONE', to2('TWO',to2(',', 'FOUR')),to1('FIVE','SIX'));
BEGIN
    RAISE NOTICE 'ID: %, NAME: %', l_rec.val5, l_rec.val6.val3;
    RAISE NOTICE 'ID: %, NAME: %', l_rec.val6.val4.val1, l_rec.val7.val2;
END;
/

DECLARE
    l_rec to3;
BEGIN
    l_rec := ('ONE', to2('TWO',to3(',', 'FOUR')),to1('FIVE','SIX'));
    RAISE NOTICE 'ID: %, NAME: %', l_rec.val5, l_rec.val6.val3;
    RAISE NOTICE 'ID: %, NAME: %', l_rec.val6.val4.val1, l_rec.val7.val2;
END;
/

CREATE OR REPLACE PACKAGE r_types IS
  TYPE t_rec0 IS RECORD (val1 VARCHAR2(10), val2 VARCHAR2(10));
  TYPE t_rec1 IS RECORD (val4 t_rec0, val3 VARCHAR2(10));
END r_types;
/

CREATE OR REPLACE PROCEDURE test_nested AS
DECLARE
    TYPE t_rec2 IS RECORD (val6 r_types.t_rec1, val7 r_types.t_rec0, val5 VARCHAR2(10));
    l_rec t_rec2;
BEGIN
    l_rec := t_rec2(r_types.t_rec1(r_types.t_rec0(',', 'FOUR'), 'TWO'), r_types.t_rec0('FIVE','SIX'), 'ONE');
    RAISE NOTICE 'ID: %, NAME: %', l_rec.val5, l_rec.val6.val3;
    RAISE NOTICE 'ID: %, NAME: %', l_rec.val6.val4.val1, l_rec.val7.val2;
END;
/

CREATE OR REPLACE PROCEDURE test_nested AS
DECLARE
    TYPE t_rec2 IS RECORD (val6 r_types.t_rec1, val7 r_types.t_rec0, val5 VARCHAR2(10));
    l_rec t_rec2;
BEGIN
    l_rec := t_rec2(r_types.t_rec1(r_types.t_rec1(',', 'FOUR'), 'TWO'), r_types.t_rec0('FIVE','SIX'), 'ONE');
    RAISE NOTICE 'ID: %, NAME: %', l_rec.val5, l_rec.val6.val3;
    RAISE NOTICE 'ID: %, NAME: %', l_rec.val6.val4.val1, l_rec.val7.val2;
END;
/

CREATE OR REPLACE PACKAGE r_types IS
  TYPE t_rec0 IS RECORD (val1 VARCHAR2(10), val2 VARCHAR2(10));
  TYPE t_rec1 IS RECORD (val3 VARCHAR2(10), val4 t_rec0);
END r_types;
/

CREATE OR REPLACE PROCEDURE test_nested AS
DECLARE
    TYPE t_rec2 IS RECORD (val5 VARCHAR2(10), val6 r_types.t_rec1, val7 r_types.t_rec0);
    l_rec t_rec2;
BEGIN
    l_rec := t_rec2('ONE', r_types.t_rec1('TWO',r_types.t_rec0(',', 'FOUR')),r_types.t_rec0('FIVE','SIX'));
    RAISE NOTICE 'ID: %, NAME: %', l_rec.val5, l_rec.val6.val3;
    RAISE NOTICE 'ID: %, NAME: %', l_rec.val6.val4.val1, l_rec.val7.val2;
END;
/

CREATE OR REPLACE PROCEDURE test_nested AS
DECLARE
    TYPE t_rec2 IS RECORD (val5 VARCHAR2(10), val6 r_types.t_rec1, val7 r_types.t_rec0);
    l_rec t_rec2;
BEGIN
    l_rec := t_rec2('ONE', r_types.t_rec1('TWO',r_types.t_rec1(',', 'FOUR')),r_types.t_rec0('FIVE','SIX'));
    RAISE NOTICE 'ID: %, NAME: %', l_rec.val5, l_rec.val6.val3;
    RAISE NOTICE 'ID: %, NAME: %', l_rec.val6.val4.val1, l_rec.val7.val2;
END;
/

CREATE OR REPLACE PROCEDURE test_nested AS
DECLARE
    TYPE t_rec2 IS RECORD (val5 VARCHAR2(10), val6 r_types.t_rec1, val7 r_types.t_rec0);
    l_rec t_rec2;
BEGIN
    l_rec := t_rec2('ONE', r_types.t_rec1('TWO',r_types.t_rec0(',', 'FOUR')),t_rec2('FIVE','SIX'));
    RAISE NOTICE 'ID: %, NAME: %', l_rec.val5, l_rec.val6.val3;
    RAISE NOTICE 'ID: %, NAME: %', l_rec.val6.val4.val1, l_rec.val7.val2;
END;
/

CREATE OR REPLACE PROCEDURE test_nested AS
DECLARE
    TYPE t_rec1 IS RECORD (val3 VARCHAR2(10), val4 r_types.t_rec0);
    TYPE t_rec2 IS RECORD (val5 VARCHAR2(10), val6 t_rec1, val7 r_types.t_rec0);
    l_rec t_rec2;
BEGIN
    l_rec := t_rec2('ONE', r_types.t_rec1('TWO',r_types.t_rec0(',', 'FOUR')),r_types.t_rec0('FIVE','SIX'));
    RAISE NOTICE 'ID: %, NAME: %', l_rec.val5, l_rec.val6.val3;
    RAISE NOTICE 'ID: %, NAME: %', l_rec.val6.val4.val1, l_rec.val7.val2;
END;
/

CREATE OR REPLACE PROCEDURE test_nested AS
DECLARE
    TYPE t_rec1 IS RECORD (val3 VARCHAR2(10), val4 r_types.t_rec0);
    TYPE t_rec2 IS RECORD (val5 VARCHAR2(10), val6 t_rec1, val7 r_types.t_rec0);
    l_rec t_rec2;
BEGIN
    l_rec := t_rec2('ONE', t_rec1('TWO',r_types.t_rec0(',', 'FOUR')),r_types.t_rec0('FIVE','SIX'));
    RAISE NOTICE 'ID: %, NAME: %', l_rec.val5, l_rec.val6.val3;
    RAISE NOTICE 'ID: %, NAME: %', l_rec.val6.val4.val1, l_rec.val7.val2;
END;
/

CREATE OR REPLACE PROCEDURE test_nested AS
DECLARE
    TYPE t_rec1 IS RECORD (val3 VARCHAR2(10), val4 r_types.t_rec0);
    TYPE t_rec2 IS RECORD (val5 VARCHAR2(10), val6 t_rec1, val7 r_types.t_rec0);
    l_rec t_rec2;
BEGIN
    l_rec := t_rec2('ONE', r_types.t_rec1('TWO',r_types.t_rec0(',', 'FOUR')),t_rec2('FIVE','SIX'));
    RAISE NOTICE 'ID: %, NAME: %', l_rec.val5, l_rec.val6.val3;
    RAISE NOTICE 'ID: %, NAME: %', l_rec.val6.val4.val1, l_rec.val7.val2;
END;
/

DROP PACKAGE r_types;

CREATE OR REPLACE PACKAGE plpgsql_assignlist.r_types IS
  TYPE t_rec0 IS RECORD (val1 VARCHAR2(10), val2 VARCHAR2(10));
  TYPE t_rec1 IS RECORD (val4 t_rec0, val3 VARCHAR2(10));
END r_types;
/

CREATE OR REPLACE PROCEDURE test_nested AS
DECLARE
    TYPE t_rec2 IS RECORD (val6 plpgsql_assignlist.r_types.t_rec1, val7 plpgsql_assignlist.r_types.t_rec0, val5 VARCHAR2(10));
    l_rec t_rec2;
BEGIN
    l_rec := t_rec2(r_types.t_rec1(r_types.t_rec0(',', 'FOUR'), 'TWO'),plpgsql_assignlist.r_types.t_rec0('FIVE','SIX'), 'ONE');
    RAISE NOTICE 'ID: %, NAME: %', l_rec.val5, l_rec.val6.val3;
    RAISE NOTICE 'ID: %, NAME: %', l_rec.val6.val4.val1, l_rec.val7.val2;
END;
/

CREATE OR REPLACE PROCEDURE test_nested AS
DECLARE
    TYPE t_rec2 IS RECORD (val6 plpgsql_assignlist.r_types.t_rec1, val7 r_types.t_rec0, val5 VARCHAR2(10));
    l_rec t_rec2;
BEGIN
    l_rec := t_rec2(r_types.t_rec1(plpgsql_assignlist.r_types.t_rec1(',', 'FOUR'), 'TWO'),r_types.t_rec0('FIVE','SIX'), 'ONE');
    RAISE NOTICE 'ID: %, NAME: %', l_rec.val5, l_rec.val6.val3;
    RAISE NOTICE 'ID: %, NAME: %', l_rec.val6.val4.val1, l_rec.val7.val2;
END;
/

CREATE OR REPLACE PACKAGE plpgsql_assignlist.r_types IS
  TYPE t_rec0 IS RECORD (val1 VARCHAR2(10), val2 VARCHAR2(10));
  TYPE t_rec1 IS RECORD (val3 VARCHAR2(10), val4 t_rec0);
END r_types;
/

CREATE OR REPLACE PROCEDURE test_nested AS
DECLARE
    TYPE t_rec2 IS RECORD (val5 VARCHAR2(10), val6 plpgsql_assignlist.r_types.t_rec1, val7 plpgsql_assignlist.r_types.t_rec0);
    l_rec t_rec2;
BEGIN
    l_rec := t_rec2('ONE', r_types.t_rec1('TWO',r_types.t_rec0(',', 'FOUR')),plpgsql_assignlist.r_types.t_rec0('FIVE','SIX'));
    RAISE NOTICE 'ID: %, NAME: %', l_rec.val5, l_rec.val6.val3;
    RAISE NOTICE 'ID: %, NAME: %', l_rec.val6.val4.val1, l_rec.val7.val2;
END;
/

CREATE OR REPLACE PROCEDURE test_nested AS
DECLARE
    TYPE t_rec2 IS RECORD (val5 VARCHAR2(10), val6 plpgsql_assignlist.r_types.t_rec1, val7 r_types.t_rec0);
    l_rec t_rec2;
BEGIN
    l_rec := t_rec2('ONE', r_types.t_rec1('TWO',plpgsql_assignlist.r_types.t_rec1(',', 'FOUR')),r_types.t_rec0('FIVE','SIX'));
    RAISE NOTICE 'ID: %, NAME: %', l_rec.val5, l_rec.val6.val3;
    RAISE NOTICE 'ID: %, NAME: %', l_rec.val6.val4.val1, l_rec.val7.val2;
END;
/

CREATE OR REPLACE PROCEDURE test_nested AS
DECLARE
    TYPE t_rec2 IS RECORD (val5 VARCHAR2(10), val6 r_types.t_rec1, val7 plpgsql_assignlist.r_types.t_rec0);
    l_rec t_rec2;
BEGIN
    l_rec := t_rec2('ONE', plpgsql_assignlist.r_types.t_rec1('TWO',plpgsql_assignlist.r_types.t_rec0(',', 'FOUR')),t_rec2('FIVE','SIX'));
    RAISE NOTICE 'ID: %, NAME: %', l_rec.val5, l_rec.val6.val3;
    RAISE NOTICE 'ID: %, NAME: %', l_rec.val6.val4.val1, l_rec.val7.val2;
END;
/

CREATE OR REPLACE PROCEDURE test_nested AS
DECLARE
    TYPE t_rec1 IS RECORD (val3 VARCHAR2(10), val4 plpgsql_assignlist.r_types.t_rec0);
    TYPE t_rec2 IS RECORD (val5 VARCHAR2(10), val6 t_rec1, val7 plpgsql_assignlist.r_types.t_rec0);
    l_rec t_rec2;
BEGIN
    l_rec := t_rec2('ONE', plpgsql_assignlist.r_types.t_rec1('TWO',plpgsql_assignlist.r_types.t_rec0(',', 'FOUR')),plpgsql_assignlist.r_types.t_rec0('FIVE','SIX'));
    RAISE NOTICE 'ID: %, NAME: %', l_rec.val5, l_rec.val6.val3;
    RAISE NOTICE 'ID: %, NAME: %', l_rec.val6.val4.val1, l_rec.val7.val2;
END;
/

CREATE OR REPLACE PROCEDURE test_nested AS
DECLARE
    TYPE t_rec1 IS RECORD (val3 VARCHAR2(10), val4 r_types.t_rec0);
    TYPE t_rec2 IS RECORD (val5 VARCHAR2(10), val6 t_rec1, val7 r_types.t_rec0);
    l_rec t_rec2;
BEGIN
    l_rec := t_rec2('ONE', t_rec1('TWO',plpgsql_assignlist.r_types.t_rec0(',', 'FOUR')),r_types.t_rec0('FIVE','SIX'));
    RAISE NOTICE 'ID: %, NAME: %', l_rec.val5, l_rec.val6.val3;
    RAISE NOTICE 'ID: %, NAME: %', l_rec.val6.val4.val1, l_rec.val7.val2;
END;
/

CREATE OR REPLACE PROCEDURE test_nested AS
DECLARE
    TYPE t_rec1 IS RECORD (val3 VARCHAR2(10), val4 plpgsql_assignlist.r_types.t_rec0);
    TYPE t_rec2 IS RECORD (val5 VARCHAR2(10), val6 t_rec1, val7 r_types.t_rec0);
    l_rec t_rec2;
BEGIN
    l_rec := t_rec2('ONE', plpgsql_assignlist.r_types.t_rec1('TWO',r_types.t_rec0(',', 'FOUR')),t_rec2('FIVE','SIX'));
    RAISE NOTICE 'ID: %, NAME: %', l_rec.val5, l_rec.val6.val3;
    RAISE NOTICE 'ID: %, NAME: %', l_rec.val6.val4.val1, l_rec.val7.val2;
END;
/

DROP PACKAGE r_types;

CREATE OR REPLACE PROCEDURE test_nested AS
DECLARE
    TYPE arr2 IS VARRAY(5) OF INTEGER;
    TYPE arr1 IS VARRAY(5) OF INTEGER;
    TYPE nt1 IS VARRAY(10) OF arr1;
    TYPE rec1 IS RECORD(id int, arrarg nt1);
    arr_rec rec1:=rec1(7, nt1(arr2(1,2,4,5),arr1(1,3)));
BEGIN
    RAISE NOTICE 'ID: %', arr_rec.id;
END;
/

CREATE OR REPLACE PROCEDURE test_nested AS
DECLARE
    TYPE arr2 IS VARRAY(5) OF INTEGER;
    TYPE arr1 IS VARRAY(5) OF INTEGER;
    TYPE nt1 IS VARRAY(10) OF arr1;
    TYPE rec1 IS RECORD(id int, arrarg nt1);
    arr_rec rec1:=rec1(7, nt1(arr1(1,2,4,5),arr1(1,3)));
BEGIN
    RAISE NOTICE 'ID: %', arr_rec.id;
END;
/

CREATE OR REPLACE PROCEDURE test_nested AS
DECLARE
    TYPE arr2 IS VARRAY(5) OF INTEGER;
    TYPE arr1 IS VARRAY(5) OF INTEGER;
    TYPE nt1 IS VARRAY(10) OF arr1;
    TYPE rec1 IS RECORD(id int, arrarg nt1);
    arr_rec rec1;
BEGIN
    arr_rec:=rec1(7, nt1(arr2(1,2,4,5),arr1(1,3)))
    RAISE NOTICE 'ID: %', arr_rec.id;
END;
/

CREATE OR REPLACE PROCEDURE test_nested AS
DECLARE
    TYPE arr2 IS TABLE OF INTEGER;
    TYPE arr1 IS TABLE OF INTEGER;
    TYPE nt1 IS TABLE OF arr1;
    TYPE rec1 IS RECORD(id int, arrarg nt1);
    arr_rec rec1:=rec1(7, nt1(arr2(1,2,4,5),arr1(1,3)));
BEGIN
    RAISE NOTICE 'ID: %', arr_rec.id;
END;
/

CREATE OR REPLACE PROCEDURE test_nested AS
DECLARE
    TYPE arr2 IS TABLE OF INTEGER;
    TYPE arr1 IS TABLE OF INTEGER;
    TYPE nt1 IS TABLE OF arr1;
    TYPE rec1 IS RECORD(id int, arrarg nt1);
    arr_rec rec1:=rec1(7, nt1(arr1(1,2,4,5),arr1(1,3)));
BEGIN
    RAISE NOTICE 'ID: %', arr_rec.id;
END;
/

CREATE OR REPLACE PROCEDURE test_nested AS
DECLARE
    TYPE arr2 IS TABLE OF INTEGER;
    TYPE arr1 IS TABLE OF INTEGER;
    TYPE nt1 IS TABLE OF arr1;
    TYPE rec1 IS RECORD(id int, arrarg nt1);
    arr_rec rec1;
BEGIN
    arr_rec:=rec1(7, nt1(arr2(1,2,4,5),arr1(1,3)))
    RAISE NOTICE 'ID: %', arr_rec.id;
END;
/

CREATE OR REPLACE PROCEDURE test_nested AS
DECLARE
    TYPE t_rec2 IS RECORD (val5 VARCHAR2(10), val6 to2, val7 to1);
    l_rec t_rec2;
BEGIN
    l_rec := ('ONE', to2('TWO',to2(',', 'FOUR')),to1('FIVE','SIX'));
    RAISE NOTICE 'ID: %, NAME: %', l_rec.val5, l_rec.val6.val3;
    RAISE NOTICE 'ID: %, NAME: %', l_rec.val6.val4.val1, l_rec.val7.val2;
END;
/

CREATE OR REPLACE PROCEDURE test_nested AS
DECLARE
    TYPE t_rec2 IS RECORD (val5 VARCHAR2(10), val6 to2, val7 to1);
    l_rec t_rec2;
BEGIN
    l_rec := ('ONE', to2('TWO',to1(',', 'FOUR')),to1('FIVE','SIX'));
    RAISE NOTICE 'ID: %, NAME: %', l_rec.val5, l_rec.val6.val3;
    RAISE NOTICE 'ID: %, NAME: %', l_rec.val6.val4.val1, l_rec.val7.val2;
END;
/

CREATE OR REPLACE PROCEDURE test_nested AS
DECLARE
    TYPE t_rec0 IS RECORD (val1 VARCHAR2(10), val2 VARCHAR2(10));
    TYPE t_rec1 IS RECORD (val3 VARCHAR2(10), val4 t_rec0);
    TYPE t_rec2 IS RECORD (val5 VARCHAR2(10), val6 to2, val7 t_rec0);
    l_rec t_rec2;
BEGIN
    l_rec := ('ONE', to2('TWO',to1(',', 'FOUR')),to1('FIVE','SIX'));
    RAISE NOTICE 'ID: %, NAME: %', l_rec.val5, l_rec.val6.val3;
    RAISE NOTICE 'ID: %, NAME: %', l_rec.val6.val4.val1, l_rec.val7.val2;
END;
/

CREATE OR REPLACE PROCEDURE test_nested AS
DECLARE
    TYPE t_rec0 IS RECORD (val1 VARCHAR2(10), val2 VARCHAR2(10));
    TYPE t_rec1 IS RECORD (val3 VARCHAR2(10), val4 to1);
    TYPE t_rec2 IS RECORD (val5 VARCHAR2(10), val6 to2, val7 t_rec1);
    l_rec t_rec2;
BEGIN
    l_rec := ('ONE', to2('TWO',to1(',', 'FOUR')),t_rec2('A', to1('FIVE','SIX')));
    RAISE NOTICE 'ID: %, NAME: %', l_rec.val5, l_rec.val6.val3;
    RAISE NOTICE 'ID: %, NAME: %', l_rec.val6.val4.val1, l_rec.val7.val4.val1;
END;
/

DECLARE
    l_rec to3;
BEGIN
    l_rec := to2('ONE', to2('TWO',to1(',', 'FOUR')),to1('FIVE','SIX'));
    RAISE NOTICE 'ID: %, NAME: %', l_rec.val5, l_rec.val6.val3;
    RAISE NOTICE 'ID: %, NAME: %', l_rec.val6.val4.val1, l_rec.val7.val2;
END;
/

CREATE OR REPLACE PROCEDURE test_nested AS
DECLARE
    TYPE t_rec0 IS RECORD (val1 VARCHAR2(10), val2 VARCHAR2(10));
    TYPE t_rec1 IS RECORD (val3 VARCHAR2(10), val4 t_rec0);
    TYPE t_rec2 IS RECORD (val5 VARCHAR2(10), val6 t_rec1, val7 t_rec0);
    l_rec t_rec2;
BEGIN
    l_rec := ('ONE', t_rec1('TWO',t_rec2(',', 'FOUR')),t_rec0('FIVE','SIX'));
    RAISE NOTICE 'ID: %, NAME: %', l_rec.val5, l_rec.val6.val3;
    RAISE NOTICE 'ID: %, NAME: %', l_rec.val6.val4.val1, l_rec.val7.val2;
END;
/

CREATE OR REPLACE PROCEDURE test_nested AS
DECLARE
    TYPE t_rec0 IS RECORD (val1 VARCHAR2(10), val2 VARCHAR2(10));
    TYPE t_rec1 IS RECORD (val3 VARCHAR2(10), val4 t_rec0);
    TYPE t_rec2 IS RECORD (val5 VARCHAR2(10), val6 t_rec1, val7 t_rec0);
    l_rec t_rec2 := ('ONE', t_rec1(',',t_rec2('THREE', 'FOUR')),t_rec0('FIVE','SIX'));
BEGIN
    RAISE NOTICE 'ID: %, NAME: %', l_rec.val5, l_rec.val6.val3;
    RAISE NOTICE 'ID: %, NAME: %', l_rec.val6.val4.val1, l_rec.val7.val2;
END;
/

--test nest table
DECLARE
TYPE t1 IS VARRAY(10) OF INTEGER;  -- varray of integer
TYPE t2 IS VARRAY(10) OF INTEGER;  -- varray of integer
TYPE nt2 IS VARRAY(10) OF t2;      -- varray of varray of integer
TYPE nt1 IS VARRAY(10) OF t1;      -- varray of varray of integer
TYPE nt22 IS VARRAY(10) OF nt2;      -- varray of varray of integer
TYPE nt11 IS VARRAY(10) OF nt1;      -- varray of varray of integer
nva nt11;
BEGIN
  nva := nt11(nt1(t1(4,5,6), t2(55,6,73), t1(2,4), t1(2,5,6)),nt1(t1(2,4),t1(2,8)));
END;
/

DECLARE
TYPE t1 IS VARRAY(10) OF INTEGER;  -- varray of integer
TYPE t2 IS VARRAY(10) OF INTEGER;  -- varray of integer
TYPE nt2 IS VARRAY(10) OF t2;      -- varray of varray of integer
TYPE nt1 IS VARRAY(10) OF t1;      -- varray of varray of integer
TYPE nt22 IS VARRAY(10) OF nt2;      -- varray of varray of integer
TYPE nt11 IS VARRAY(10) OF nt1;      -- varray of varray of integer
nva nt11;
BEGIN
  nva := nt22(nt1(t1(4,5,6), t1(55,6,73), t1(2,4), t1(2,5,6)),nt1(t1(2,4),t1(2,8)));
END;
/

DECLARE
TYPE t1 IS VARRAY(10) OF INTEGER;  -- varray of integer
TYPE t2 IS VARRAY(10) OF INTEGER;  -- varray of integer
TYPE nt2 IS VARRAY(10) OF t2;      -- varray of varray of integer
TYPE nt1 IS VARRAY(10) OF t1;      -- varray of varray of integer
TYPE nt22 IS VARRAY(10) OF nt2;      -- varray of varray of integer
TYPE nt11 IS VARRAY(10) OF nt1;      -- varray of varray of integer
nva nt11;
BEGIN
  nva := nt11(nt1(t1(4,5,6), t1(55,6,73), t1(2,4), t1(2,5,6)),nt2(t1(2,4),t1(2,8)));
END;
/

DECLARE
TYPE t1 IS VARRAY(10) OF INTEGER;  -- varray of integer
TYPE t2 IS VARRAY(10) OF INTEGER;  -- varray of integer
TYPE nt2 IS VARRAY(10) OF t2;      -- varray of varray of integer
TYPE nt1 IS VARRAY(10) OF t1;      -- varray of varray of integer
TYPE nt22 IS VARRAY(10) OF nt2;      -- varray of varray of integer
TYPE nt11 IS VARRAY(10) OF nt1;      -- varray of varray of integer
nva nt11 := nt11(nt1(t1(4,5,6), t2(55,6,73), t1(2,4), t1(2,5,6)),nt1(t1(2,4),t1(2,8)));
va2 t2;
BEGIN
  va2(1):=9;
END;
/

DECLARE
TYPE t1 IS VARRAY(10) OF INTEGER;  -- varray of integer
TYPE t2 IS VARRAY(10) OF INTEGER;  -- varray of integer
TYPE nt2 IS VARRAY(10) OF t2;      -- varray of varray of integer
TYPE nt1 IS VARRAY(10) OF t1;      -- varray of varray of integer
TYPE nt22 IS VARRAY(10) OF nt2;      -- varray of varray of integer
TYPE nt11 IS VARRAY(10) OF nt1;      -- varray of varray of integer
nva nt11 := nt22(nt1(t1(4,5,6), t1(55,6,73), t1(2,4), t1(2,5,6)),nt1(t1(2,4),t1(2,8)));
va2 t2;
BEGIN
  va2(1):=9;
END;
/

DECLARE
TYPE t1 IS VARRAY(10) OF INTEGER;  -- varray of integer
TYPE t2 IS VARRAY(10) OF INTEGER;  -- varray of integer
TYPE nt2 IS VARRAY(10) OF t2;      -- varray of varray of integer
TYPE nt1 IS VARRAY(10) OF t1;      -- varray of varray of integer
TYPE nt22 IS VARRAY(10) OF nt2;      -- varray of varray of integer
TYPE nt11 IS VARRAY(10) OF nt1;      -- varray of varray of integer
nva nt11 := nt11(nt1(t1(4,5,6), t1(55,6,73), t1(2,4), t1(2,5,6)),nt2(t1(2,4),t1(2,8)));
va2 t2;
BEGIN
  va2(1):=9;
END;
/

set enable_pltype_name_check = off;

--test o1.col1.col2 ref
create type ct as (num int,info text);
create type ct1 as (num int,info ct);
create or replace package autonomous_pkg_a IS
count_public ct1 := (1,(1,'a')::ct)::ct1;
function autonomous_f_public(num1 int) return int;
end autonomous_pkg_a;
/
create or replace package body autonomous_pkg_a as
count_private ct1 :=(2,(2,'b')::ct)::ct1;
function autonomous_f_public(num1 int) return int
is
declare
re_int int;
begin
count_public.num = num1 + count_public.num;
count_private.num = num1 + count_private.num;
raise info 'count_public.info.num: %', count_public.info.num;
count_public.info.num = count_public.info.num + num1;
raise info 'count_public.info.num: %', count_public.info.num;
count_private.info.num = count_private.info.num + num1;
re_int = count_public.num +count_private.num;
return re_int;
end;
end autonomous_pkg_a;
/

select autonomous_pkg_a.autonomous_f_public(10);
drop package autonomous_pkg_a;
drop type ct1;
drop type ct;

-- test select into syntax error
declare
va int;
vb varchar2;
vc varchar2;
begin
select 1,'a','b' into va,vb vc;
end;
/

--------------------------------------------------
------------------ END OF TESTS ------------------
--------------------------------------------------
drop function if exists get_age();
drop type if exists o6;
drop type if exists o5;
drop type if exists o4;
drop type if exists o3;
drop type if exists o2;
drop type if exists o1;

-- clean up --
drop schema if exists plpgsql_assignlist cascade;
