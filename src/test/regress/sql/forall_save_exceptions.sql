create schema forall_save_exceptions;

set search_path = forall_save_exceptions;

CREATE TABLE if not exists test_forall(a char(10));

----
-- test pragma exception_init
----
DECLARE
    l_error_count  integer;
    ex_dml_errors EXCEPTION;
    PRAGMA EXCEPTION_INIT(ex_dml_errors, -1);
    PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN

END;
/

DECLARE
    l_error_count  integer;
    ex_dml_errors EXCEPTION;
    PRAGMA EXCEPTION_INIT(ex_dml_errors, -15);
    PRAGMA EXCEPTION_INIT(ex_dml_errors, -24381);
    PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
    RAISE ex_dml_errors;
EXCEPTION
    WHEN ex_dml_errors THEN
        RAISE NOTICE 'test:%',SQLcode;
END;
/

DECLARE
    l_error_count  integer;
    ex_dml_errors EXCEPTION;
    PRAGMA EXCEPTION_INIT(ex_dml_errors, -1);
    PRAGMA EXCEPTION_INIT(ex_dml_errors, -2);
    PRAGMA EXCEPTION_INIT(ex_dml_errors, -3);
    PRAGMA EXCEPTION_INIT(ex_dml_errors, -4);
    PRAGMA EXCEPTION_INIT(ex_dml_errors, -5);
    PRAGMA EXCEPTION_INIT(ex_dml_errors, -6);
    PRAGMA EXCEPTION_INIT(ex_dml_errors, -7);
    PRAGMA EXCEPTION_INIT(ex_dml_errors, -8);
    PRAGMA EXCEPTION_INIT(ex_dml_errors, -9);
    PRAGMA EXCEPTION_INIT(ex_dml_errors, -10);
    PRAGMA EXCEPTION_INIT(ex_dml_errors, -9);
    PRAGMA EXCEPTION_INIT(ex_dml_errors, -8);
    PRAGMA EXCEPTION_INIT(ex_dml_errors, -7);
    PRAGMA EXCEPTION_INIT(ex_dml_errors, -6);
    PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
    RAISE ex_dml_errors;
EXCEPTION
    WHEN ex_dml_errors THEN
        RAISE NOTICE 'test:%',SQLcode;
END;
/

-- only numeric initialization
DECLARE
    l_error_count  integer;
    ex_dml_errors EXCEPTION;
    PRAGMA EXCEPTION_INIT(ex_dml_errors, raise_exception);
    PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN

END;
/

DECLARE
    l_error_count  integer;
    ex_dml_errors EXCEPTION;
    PRAGMA EXCEPTION_INIT(ex_dml_errors, 'aaa');
    PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN

END;
/

DECLARE
    l_error_count  integer;
    ex_dml_errors EXCEPTION;
    PRAGMA EXCEPTION_INIT(ex_dml_errors, '1.1');
    PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN

END;
/

DECLARE
    l_error_count  integer;
    ex_dml_errors EXCEPTION;
    PRAGMA EXCEPTION_INIT(ex_dml_errors, 1.1);
    PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN

END;
/

DECLARE
    l_error_count  integer;
    ex_dml_errors EXCEPTION;
    PRAGMA EXCEPTION_INIT(ex_dml_errors, '-1');
    PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN

END;
/

-- expresion not supported, sqlcode must <= 0, and must be int32 (the range is -2147483647~-1)
DECLARE
    l_error_count  integer;
    ex_dml_errors EXCEPTION;
    PRAGMA EXCEPTION_INIT(ex_dml_errors, 1-2);
    PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN

END;
/

DECLARE
    l_error_count  integer;
    ex_dml_errors EXCEPTION;
    PRAGMA EXCEPTION_INIT(ex_dml_errors, 0);
    PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN

END;
/

DECLARE
    l_error_count  integer;
    ex_dml_errors EXCEPTION;
    PRAGMA EXCEPTION_INIT(ex_dml_errors, 1);
    PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN

END;
/

DECLARE
    l_error_count  integer;
    ex_dml_errors EXCEPTION;
    PRAGMA EXCEPTION_INIT(ex_dml_errors, 15);
    PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN

END;
/

DECLARE
    l_error_count  integer;
    ex_dml_errors EXCEPTION;
    PRAGMA EXCEPTION_INIT(ex_dml_errors, -2147483648);
    PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN

END;
/

DECLARE
    l_error_count  integer;
    ex_dml_errors EXCEPTION;
    PRAGMA EXCEPTION_INIT(ex_dml_errors, -2147483647);
    PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN

END;
/

--exception init with not declared
DECLARE
    l_error_count  integer;
    PRAGMA EXCEPTION_INIT(ex_dml_errors, -1);
    PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN

END;
/

--exception init with system error
DECLARE
    x integer := 0;
    y integer;
    PRAGMA EXCEPTION_INIT(division_by_zero, -1);
    PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
    y := x / 0;
EXCEPTION
    WHEN division_by_zero THEN
    RAISE NOTICE 'test:%',SQLstate;
END;
/

DECLARE
    x integer := 0;
    y integer;
    PRAGMA EXCEPTION_INIT(division_by_zero, -1);
    PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
    y := x / 0;
EXCEPTION
    WHEN division_by_zero THEN
    RAISE NOTICE 'test:%',SQLstate;
END;
/

----
-- support exception when custum exception
----
DECLARE
    ex_dml_errors EXCEPTION;
BEGIN
    INSERT INTO test VALUES (1);
EXCEPTION
    WHEN ex_dml_errors THEN
        RAISE NOTICE 'DML error occurs';
    WHEN others THEN
        RAISE NOTICE 'Other error occurs';
END;
/

DECLARE
    DEADLOCK_DETECTED EXCEPTION;
    PRAGMA EXCEPTION_INIT(DEADLOCK_DETECTED, -60);
BEGIN
    IF 1 > 0 THEN
        RAISE DEADLOCK_DETECTED;
    END IF;
EXCEPTION
    WHEN DEADLOCK_DETECTED THEN
        RAISE NOTICE 'test:%',SQLcode;
END;
/

----
-- when exception init, SQLstate is the same as SQLcode
----
DECLARE
    DEADLOCK_DETECTED EXCEPTION;
    PRAGMA EXCEPTION_INIT(DEADLOCK_DETECTED, -60);
BEGIN
    IF 1 > 0 THEN
        RAISE DEADLOCK_DETECTED;
    END IF;
EXCEPTION
    WHEN DEADLOCK_DETECTED THEN
        RAISE NOTICE 'test:%',SQLstate;
END;
/

--the sqlerrm is " xxx: non-GaussDB Exception", xxx is |sqlcode|
DECLARE
    DEADLOCK_DETECTED EXCEPTION;
    PRAGMA EXCEPTION_INIT(DEADLOCK_DETECTED, -60);
BEGIN
    IF 1 > 0 THEN
        RAISE DEADLOCK_DETECTED;
    END IF;
EXCEPTION
    WHEN DEADLOCK_DETECTED THEN
        RAISE NOTICE 'test:%',SQLerrm;
END;
/

-- when not init, SQLcode is generated default with type int, and SQLstate is a text
DECLARE
    DEADLOCK_DETECTED EXCEPTION;
BEGIN
    IF 1 > 0 THEN
        RAISE DEADLOCK_DETECTED;
    END IF;
EXCEPTION
    WHEN DEADLOCK_DETECTED THEN
        RAISE NOTICE 'test:%',SQLcode;
END;
/

DECLARE
    DEADLOCK_DETECTED EXCEPTION;
BEGIN
    IF 1 > 0 THEN
        RAISE DEADLOCK_DETECTED;
    END IF;
EXCEPTION
    WHEN DEADLOCK_DETECTED THEN
        RAISE NOTICE 'test:%',SQLstate;
END;
/

----
-- forall support save exceptions grammar
----

-- function
CREATE OR REPLACE FUNCTION test_func(iter IN integer) RETURN integer as
DECLARE
    l_error_count  integer;
    ex_dml_errors EXCEPTION;
    PRAGMA EXCEPTION_INIT(ex_dml_errors, -24381);
BEGIN
    FORALL i IN 1 .. 2 SAVE EXCEPTIONS
      INSERT INTO test_forall
      VALUES (1);
EXCEPTION
    WHEN ex_dml_errors THEN
        l_error_count := SQL%BULK_EXCEPTIONS.count;
        DBe_OUTPUT.print_line('Number of failures: ' || l_error_count);
        FOR i IN 1 .. l_error_count LOOP
            DBE_OUTPUT.print_line('Error: ' || i || 
                ' Array Index: ' || SQL%BULK_EXCEPTIONS(i).error_index ||
                ' Message: ' || SQL%BULK_EXCEPTIONS(i).error_message);
        END LOOP;
    RETURN 0;
END;
/

-- procedure
CREATE OR REPLACE PROCEDURE test_proc(iter IN integer) as
DECLARE
    l_error_count  integer;
    ex_dml_errors EXCEPTION;
    PRAGMA EXCEPTION_INIT(ex_dml_errors, 24381);
BEGIN
    FORALL i IN 1 .. 2 SAVE EXCEPTIONS
      INSERT INTO test_forall
      VALUES (1);
EXCEPTION
    WHEN ex_dml_errors THEN
            l_error_count := SQL%BULK_EXCEPTIONS.count;
            DBe_OUTPUT.print_line('Number of failures: ' || l_error_count);
            FOR i IN 1 .. l_error_count LOOP
                DBE_OUTPUT.print_line('Error: ' || i || 
                    ' Array Index: ' || SQL%BULK_EXCEPTIONS(i).error_index ||
                    ' Message: ' || SQL%BULK_EXCEPTIONS(i).error_message);
            END LOOP;
END;
/

-- anonimous block
DECLARE
    l_error_count  integer;
    ex_dml_errors EXCEPTION;
    PRAGMA EXCEPTION_INIT(ex_dml_errors, -24381);
BEGIN
    FORALL i IN 1 .. 10
    SAVE EXCEPTIONS
        INSERT INTO test_forall
        VALUES (1);
EXCEPTION
    WHEN ex_dml_errors THEN
        l_error_count := SQL%BULK_EXCEPTIONS.count;
        DBe_OUTPUT.print_line('Number of failures: ' || l_error_count);
        FOR i IN 1 .. l_error_count LOOP
            DBe_OUTPUT.print_line('Error: ' || i || 
            ' Array Index: ' || SQL%BULK_EXCEPTIONS(i).error_index ||
            ' Message: ' || SQL%BULK_EXCEPTIONS(i).error_message);
        END LOOP;
END;
/

-- test functionality
CREATE TABLE exception_test (
id  NUMBER(10) NOT NULL UNIQUE
);

DECLARE
  TYPE t_tab IS TABLE OF exception_test%ROWTYPE;

  l_tab          t_tab := t_tab();
  l_error_count  NUMBER;
BEGIN
  -- Fill the collection.
  FOR i IN 1 .. 100 LOOP
    l_tab.extend;
    l_tab(l_tab.last).id := i;
  END LOOP;

  -- Cause a failure.
  l_tab(50).id := NULL;
  l_tab(51).id := NULL;
  l_tab(60).id := 59;
  
  EXECUTE IMMEDIATE 'TRUNCATE TABLE exception_test';

  BEGIN
    FORALL i IN l_tab.first .. l_tab.last SAVE EXCEPTIONS
      INSERT INTO exception_test
      VALUES l_tab(i);
  EXCEPTION
    WHEN others THEN
      l_error_count := SQL%BULK_EXCEPTIONS.count;
      DBE_OUTPUT.print_line('Number of failures: ' || l_error_count);
      FOR i IN 1 .. l_error_count LOOP
        DBE_OUTPUT.print_line('Error: ' || i || 
          ' Array Index: ' || SQL%BULK_EXCEPTIONS(i).error_index ||
          ' Error Code: ' || -SQL%BULK_EXCEPTIONS(i).ERROR_CODE ||
          ' Error Message: ' || SQL%BULK_EXCEPTIONS(i).ERROR_MESSAGE);
      END LOOP;
  END;

  -- Cause a failure. To test leftovers in bulk exceptions
  l_tab(50).id := 50;
  l_tab(51).id := 51;
  l_tab(60).id := 60;
  l_tab(70).id := 69;
  
  EXECUTE IMMEDIATE 'TRUNCATE TABLE exception_test';

  BEGIN
    FORALL i IN l_tab.first .. l_tab.last SAVE EXCEPTIONS
      INSERT INTO exception_test
      VALUES l_tab(i);
  EXCEPTION
    WHEN others THEN
      l_error_count := SQL%BULK_EXCEPTIONS.count;
      DBE_OUTPUT.print_line('Number of failures: ' || l_error_count);
      FOR i IN 1 .. l_error_count LOOP
        DBE_OUTPUT.print_line('Error: ' || i || 
          ' Array Index: ' || SQL%BULK_EXCEPTIONS(i).error_index ||
          ' Error Code: ' || -SQL%BULK_EXCEPTIONS(i).ERROR_CODE ||
          ' Error Message: ' || SQL%BULK_EXCEPTIONS(i).ERROR_MESSAGE);
      END LOOP;
  END;
END;
/

-- try in function
CREATE OR REPLACE FUNCTION func_test_forall() RETURNS int AS
$BODY$
DECLARE
  TYPE t_tab IS TABLE OF exception_test%ROWTYPE;

  l_tab          t_tab := t_tab();
  l_error_count  NUMBER;
BEGIN
  -- Fill the collection.
  FOR i IN 1 .. 20 LOOP
    l_tab.extend;
    l_tab(l_tab.last).id := i;
  END LOOP;

  -- Cause a failure.
  l_tab(10).id := 9;
  
  EXECUTE IMMEDIATE 'TRUNCATE TABLE exception_test';

  BEGIN
    FORALL i IN l_tab.first .. l_tab.last SAVE EXCEPTIONS
      INSERT INTO exception_test
      VALUES l_tab(i);
  EXCEPTION
    WHEN others THEN
      l_error_count := SQL%BULK_EXCEPTIONS.count;
      DBE_OUTPUT.print_line('Number of failures: ' || l_error_count);
      FOR i IN 1 .. l_error_count LOOP
        DBE_OUTPUT.print_line('Error: ' || i || 
          ' Array Index: ' || SQL%BULK_EXCEPTIONS(i).error_index ||
          ' Error Code: ' || -SQL%BULK_EXCEPTIONS(i).ERROR_CODE ||
          ' Error Message: ' || SQL%BULK_EXCEPTIONS(i).ERROR_MESSAGE);
      END LOOP;
  END;

  return 0;
END;
$BODY$
LANGUAGE plpgsql;

select * from func_test_forall();

-- for all should only be followed by DMLs
DECLARE
    l_error_count  integer;
    l_sql varchar(1024);
BEGIN
    l_sql :='INSERT INTO test_forall VALUES (1);';
    FORALL i IN 1 .. 2 SAVE EXCEPTIONS
        execute immediate l_sql using l_tab(i);
END;
/

DECLARE
    l_error_count  integer;
    l_sql varchar(1024);
BEGIN
    l_sql :='INSERT INTO test_forall VALUES (1);';
    FORALL i IN 1 .. 2
        execute immediate l_sql using l_tab(i);
END;
/

truncate test_forall;

DECLARE
    l_sql varchar(1024);
    target int;
BEGIN
    FORALL i IN 1 .. 2 --merge OK
        merge into test_forall using (SELECT 1 a) src on test_forall.a = src.a WHEN NOT MATCHED THEN INSERT VALUES (src.a);
    FORALL i IN 1 .. 2 --select OK
        SELECT a into target from test_forall;
    FORALL i IN 1 .. 2 --insert OK
        INSERT INTO test_forall VALUES (1);
    FORALL i IN 1 .. 2 --update OK
        UPDATE test_forall SET a = 2;
    FORALL i IN 1 .. 2 --delete OK
        DELETE FROM test_forall;
    FORALL i IN 1 .. 2 SAVE EXCEPTIONS --merge OK
        merge into test_forall using (SELECT 1 a) src on test_forall.a = src.a WHEN NOT MATCHED THEN INSERT VALUES (src.a);
    FORALL i IN 1 .. 2 SAVE EXCEPTIONS --select OK
        SELECT a into target from test_forall;
    FORALL i IN 1 .. 2 SAVE EXCEPTIONS --insert OK
        INSERT INTO test_forall VALUES (1);
    FORALL i IN 1 .. 2 SAVE EXCEPTIONS --update OK
        UPDATE test_forall SET a = 2;
    FORALL i IN 1 .. 2 SAVE EXCEPTIONS --delete OK
        DELETE FROM test_forall;
END;
/

create table test_conflict(last_modified timestamp, comment text);
insert into test_conflict values(now(), 'donda');

-- default (equivalent to use_column under a compatibility)
CREATE or replace FUNCTION conf_func(id int, comment text) RETURNS text AS $$
    DECLARE
        curtime timestamp := '2021-09-15 20:59:14';
        var text;
    BEGIN
        select comment into var from test_conflict;
        return var;
    END;
$$ LANGUAGE plpgsql;

select * from conf_func(1,'off-season');

-- error
CREATE or replace FUNCTION conf_func(id int, comment text) RETURNS text AS $$
    #variable_conflict error
    DECLARE
        curtime timestamp := '2021-09-15 20:59:14';
        var text;
    BEGIN
        select comment into var from test_conflict;
        return var;
    END;
$$ LANGUAGE plpgsql;

select * from conf_func(1,'off-season');

-- use_column
CREATE or replace FUNCTION conf_func(id int, comment text) RETURNS text AS $$
    #variable_conflict use_column
    DECLARE
        curtime timestamp := '2021-09-15 20:59:14';
        var text;
    BEGIN
        select comment into var from test_conflict;
        return var;
    END;
$$ LANGUAGE plpgsql;

select * from conf_func(1,'off-season');

-- use_variable
CREATE or replace FUNCTION conf_func(id int, comment text) RETURNS text AS $$
    #variable_conflict use_variable
    DECLARE
        curtime timestamp := '2021-09-15 20:59:14';
        var text;
    BEGIN
        select comment into var from test_conflict;
        return var;
    END;
$$ LANGUAGE plpgsql;

select * from conf_func(1,'off-season');

-- test original case
create table test_orig(c1 int,c2 int);
insert into test_orig values(1,2);
create or replace procedure pro_tblof_pro_004(c1 in number,c2 out number)
as
 type ARRAY_INTEGER is table of int;
 tblof001 ARRAY_INTEGER := ARRAY_INTEGER();
begin
 tblof001.extend(10);
 tblof001(1) :=1;
 c2 :=tblof001(1);
 select c2 into tblof001(2) from test_orig;
 DBE_OUTPUT.PRINT_LINE(tblof001(tblof001.FIRST));
 DBE_OUTPUT.PRINT_LINE('tblof001.last is '||tblof001.last);
 DBE_OUTPUT.PRINT_LINE('tblof001.2 is '||tblof001(2));
 DBE_OUTPUT.PRINT_LINE('tblof001.3 is '||c2);
end;
/

declare
a number;
begin
 pro_tblof_pro_004(1,a);
 DBE_OUTPUT.PRINT_LINE(a);
end;
/

-- test nested forall save exceptions
drop type if exists type01;
drop table if exists t_06;
create table t_06(c1 numeric(6,0),c2 date,c3 char(4));
drop table if exists t_07;
create table t_07(c1 numeric(8,0), c2 date, c3 char(10) not null);
create type type01 is table of t_07%rowtype;

create or replace procedure p1(l_error_count out number)
as
l_error_count number:=0;
type02 type01;
begin
truncate table t_07;
for i in 1..10 loop
type02(i).c1=5000+i;
type02(i).c2='20210929';
type02(i).c3=i||'id';
end loop;
type02(2).c3=null;
forall i in 1..type02.count save exceptions
insert into t_07
values type02(i);
exception
   when forall_dml_error then
     l_error_count := sql%bulk_exceptions.count;
     dbe_output.print_line('number of failures: ' || l_error_count);
     for i in 1 .. l_error_count loop
       dbe_output.print_line('error: ' || i || 
         ' array index: ' || sql%bulk_exceptions[i].error_index ||
         ' messagecode: ' || sql%bulk_exceptions[i].error_code ||
		 ' errormessage: ' || sql%bulk_exceptions[i].error_message);
     end loop; 
	 l_error_count := -(sql%bulk_exceptions.count)+type02.count;
	 dbe_output.print_line('successfully inserted: ' || l_error_count ||'rows');

end;
/

call p1(1);

create or replace procedure p2(l_error_count out number)
as
l_error_count number:=0;
type02 type01;

begin
for i in 1..10 loop
type02(i).c1=5001+i;
type02(i).c2='20210930';
type02(i).c3=i||'id';
end loop;
type02(1).c3='a12345';
truncate table t_06;
insert into t_06(c1,c2,c3) select * from t_07;
forall i in 1..type02.count
update t_06 set c1=(select p1()),
                c2=type02(i).c2
		 where  c1=type02(i).c1;

exception
   when forall_dml_error then
     l_error_count := sql%bulk_exceptions.count;
     dbe_output.print_line('number of failures: ' || l_error_count);
     for i in 1 .. l_error_count loop
       dbe_output.print_line('error: ' || i || 
         ' array index: ' || sql%bulk_exceptions[i].error_index ||
         ' messagecode: ' || sql%bulk_exceptions[i].error_code ||
		 ' errormessage: ' || sql%bulk_exceptions[i].error_message);
     end loop; 
	 l_error_count := -(sql%bulk_exceptions.count)+type02.count;
	 dbe_output.print_line('successfully inserted: ' || l_error_count ||'rows');

end;
/

select p2();

create or replace procedure p2(l_error_count out number)
as
l_error_count number:=0;
type02 type01;

begin
for i in 1..10 loop
type02(i).c1=5001+i;
type02(i).c2='20210930';
type02(i).c3=i||'id';
end loop;
type02(1).c3='a12345';
truncate table t_06;
insert into t_06(c1,c2,c3) select * from t_07;
forall i in 1..type02.count save exceptions
update t_06 set c1=(select p1()),
                c2=type02(i).c2
		 where  c1=type02(i).c1;

exception
   when forall_dml_error then
     l_error_count := sql%bulk_exceptions.count;
     dbe_output.print_line('number of failures: ' || l_error_count);
     for i in 1 .. l_error_count loop
       dbe_output.print_line('error: ' || i || 
         ' array index: ' || sql%bulk_exceptions[i].error_index ||
         ' messagecode: ' || sql%bulk_exceptions[i].error_code ||
		 ' errormessage: ' || sql%bulk_exceptions[i].error_message);
     end loop; 
	 l_error_count := -(sql%bulk_exceptions.count)+type02.count;
	 dbe_output.print_line('successfully inserted: ' || l_error_count ||'rows');

end;
/

select p2();

--test with implicit_savepoint
drop table if exists tab_01;
create table tab_01(
c1 varchar2(6),
c2 varchar2(8) not null,
c3 number(9,1) default 10+238/5*3,
c4 varchar2(2),
c5 timestamp(6) check(c4 between 1 and 30),
constraint pk_tab01_c1 primary key(c1)
);

drop table if exists tab_02;
create table tab_02(
c1 varchar2(10),
c2 varchar2(10) not null,
c3 number(15,0),
c4 varchar2(5),
c5 timestamp(6),
constraint pk_tab02_c1 primary key(c1)
);

create or replace procedure save_exps_insert02(v1 in numeric)
as
l_error_count  number;
type tab_02_type is table of tab_02%rowtype;
tab_02_01 tab_02_type;

begin

  execute immediate 'truncate table tab_01';

  for i in 1..50 loop
    tab_02_01(i).c1:=i;
	tab_02_01(i).c2:='001'||i;
	tab_02_01(i).c4:=i;
	tab_02_01(i).c5:='2010-12-12';
  end loop;
  
  tab_02_01(10).c1 := 9;
  tab_02_01(8).c1 := 7;
  tab_02_01(6).c1 := null;
  tab_02_01(11).c2 := null;
  tab_02_01(1).c3:=2;
  tab_02_01(2).c3:=1234567890;
  
  forall i in 1..tab_02_01.count  save exceptions
    insert into tab_01(c1,c2,c3,c4,c5)
	   values (decode(tab_02_01(i).c1,'49','490',tab_02_01(i).c1),
	          'p'||tab_02_01(i).c2,
			  tab_02_01(i).c3,
			  case when tab_02_01(i).c1=50 then substr(tab_02_01(i).c4,1,1)
			       when tab_02_01(i).c1=49 then substr(tab_02_01(i).c4,2,1)
                   else tab_02_01(i).c4 end ,
			  tab_02_01(i).c5 );
      
 exception
   when forall_dml_error then
     l_error_count := sql%bulk_exceptions.count;
     dbe_output.print_line('number of failures: ' || l_error_count);
     for i in 1 .. l_error_count loop
       dbe_output.print_line('error: ' || i || 
         ' array index: ' || sql%bulk_exceptions[i].error_index ||
         ' messagecode: ' || sql%bulk_exceptions[i].error_code ||
		 ' errormessage: ' || sql%bulk_exceptions[i].error_message);	
     end loop;
	 	 for i in 1..sql%bulk_exceptions.count loop
	     if  -sql%bulk_exceptions[i].error_code=-33575106 then
		    insert into tab_01 values(100+tab_02_01(i).c1,
			                  tab_02_01(i).c2,
							  v1,
							  tab_02_01(i).c4,
							  tab_02_01(i).c5);
		end if;
	    end loop;
   when others then
      rollback ;
      raise;
end;
/

set behavior_compat_options='plstmt_implicit_savepoint';
call save_exps_insert02(9);

----
-- test decode coercion
----
show sql_compatibility;

create table test_decode_coercion(
    col_bool bool,
    col_sint int2,
    col_int int,
    col_bigint bigint,
    col_char char(10),
    col_bpchar bpchar,
    col_varchar varchar,
    col_text text,
    col_date date,
    col_time timestamp
);

COPY test_decode_coercion(col_bool, col_sint, col_int, col_bigint, col_char, col_bpchar, col_varchar, col_text, col_date, col_time) FROM stdin;
f	1	0	256	11	111	1111	123456	2000-01-01 01:01:01	2000-01-01 01:01:01
\.

-- case 1. coerce first argument to second argument's unknown type
set sql_beta_feature = 'none';

select decode(2,'ff3',5,2); -- to be supported

select case when 2 = 'ff3' then 5 else 2 end; -- to be supported

-- valid coercions
select
    decode(col_char, 'arbitrary', 1, 2),
    decode(col_bpchar, 'arbitrary', 1, 2),
    decode(col_varchar, 'arbitrary', 1, 2),
    decode(col_text, 'arbitrary', 1, 2),
    decode(col_date, '2021-09-17', 1, 2),
    decode(col_time, '2000-01-01 01:01:01', 1, 2)
from test_decode_coercion;

-- to be supported ones
select 
    decode(col_sint, 'arbitrary', 1, 2),
    decode(col_int, 'arbitrary', 1, 2),
    decode(col_bigint, 'arbitrary', 1, 2)
from test_decode_coercion;

-- invalid
select 
    decode(col_bool, 'arbitrary', 1, 2)
from test_decode_coercion;

-- invalid
select 
    decode(col_date, 'arbitrary', 1, 2)
from test_decode_coercion;

-- invalid
select 
    decode(col_time, 'arbitrary', 1, 2)
from test_decode_coercion;

set sql_beta_feature = 'a_style_coerce';

select decode(2,'ff3',5,2); -- now ok

select case when 2 = 'ff3' then 5 else 2 end; -- now ok

-- still valid
select
    decode(col_char, 'arbitrary', 1, 2),
    decode(col_bpchar, 'arbitrary', 1, 2),
    decode(col_varchar, 'arbitrary', 1, 2),
    decode(col_text, 'arbitrary', 1, 2),
    decode(col_date, '2021-09-17', 1, 2),
    decode(col_time, '2000-01-01 01:01:01', 1, 2)
from test_decode_coercion;

-- now supported
select 
    decode(col_sint, 'arbitrary', 1, 2),
    decode(col_int, 'arbitrary', 1, 2),
    decode(col_bigint, 'arbitrary', 1, 2)
from test_decode_coercion;

-- still fail
select 
    decode(col_bool, 'arbitrary', 1, 2)
from test_decode_coercion;

-- still fail
select 
    decode(col_date, 'arbitrary', 1, 2)
from test_decode_coercion;

-- still fail
select 
    decode(col_time, 'arbitrary', 1, 2)
from test_decode_coercion;

-- case 2. decode case results need coercion
set sql_beta_feature = 'none';

select decode(2,3,'r',2); -- to be supported

select case when 2 = 3 then 'r' else 2 end; -- to be supported

-- valid coercions
select
    decode(1, 2, 'never', col_char),
    decode(1, 2, 'never', col_bpchar),
    decode(1, 2, 'never', col_varchar),
    decode(1, 2, 'never', col_text),
    decode(1, 2, '2021-09-17', col_date),
    decode(1, 2, '2000-01-01 01:01:01', col_time)
from test_decode_coercion;

-- to be supported
select
    decode(1, 2, 'never', col_sint),
    decode(1, 2, 'never', col_int),
    decode(1, 2, 'never', col_bigint)
from test_decode_coercion;

-- invalid
select 
    decode(1, 2, 'never', col_bool)
from test_decode_coercion;

-- invalid
select 
    decode(1, 2, 'never', col_date)
from test_decode_coercion;

-- invalid
select 
    decode(1, 2, 'never', col_time)
from test_decode_coercion;

set sql_beta_feature = 'a_style_coerce';

select decode(2,3,'r',2); -- now ok

select case when 2 = 3 then 'r' else 2 end; -- now ok

-- still valid
select
    decode(1, 2, 'never', col_char),
    decode(1, 2, 'never', col_bpchar),
    decode(1, 2, 'never', col_varchar),
    decode(1, 2, 'never', col_text)
from test_decode_coercion;

-- now supported
select
    decode(1, 2, 'never', col_sint),
    decode(1, 2, 'never', col_int),
    decode(1, 2, 'never', col_bigint)
from test_decode_coercion;

-- still invalid
select 
    decode(1, 2, 'never', col_bool)
from test_decode_coercion;

-- now supported
select 
    decode(1, 2, 'never', col_date)
from test_decode_coercion;

-- now supported
select 
    decode(1, 2, 'never', col_time)
from test_decode_coercion;

drop schema forall_save_exceptions cascade;


