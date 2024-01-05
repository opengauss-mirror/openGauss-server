create database pl_test_pkg_func DBCOMPATIBILITY 'pg';
\c pl_test_pkg_func;
create schema package_schema;
set current_schema= package_schema;

--test package function defination
create or replace function get_sal(NAME VARCHAR2) RETURN NUMBER package
IS
  BEGIN
    RETURN 1;
  END;
  /

create or replace function get_sal(NAME int) RETURN NUMBER package
IS
  BEGIN
    RETURN 1;
  END;
  /

select proname, propackage from pg_proc where proname='get_sal';

create or replace function test_package_function(col int, col2  out int)
returns integer package as $$
declare
    col_type text;
begin
     col := 122;
	 return 0;
end;
$$ language plpgsql;

select proname, propackage from pg_proc where proname='test_package_function';

--test function overload
create or replace function package_func_overload(col int, col2  int)
return integer package
as
declare
    col_type text;
begin
     col := 122;
	 return 0;
end;
/

create or replace function package_func_overload(col int, col2 smallint)
return integer package
as
declare
    col_type text;
begin
     col := 122;
	 return 0;
end;
/

create or replace function package_func_overload(col int, col2  bigint)
return integer package
as
declare
    col_type text;
begin
     col := 122;
	 return 0;
end;
/

--exception case
create or replace function package_func_overload(col int, col2 out int)
return integer  package
as
declare
    col_type text;
begin
     col := 122;
	 return 0;
end;
/

create or replace procedure package_func_overload(col int, col2 out varchar)
package
as
declare
    col_type text;
begin
     col2 := '122';
end;
/


select proname, propackage from pg_proc where proname='package_func_overload';

DECLARE
	resut  int;
	para1  int = 1;
	para2  smallint = 0;
	para3  bigint = 2;
	para4  varchar;
BEGIN
   package_func_overload(1, para1);
   package_func_overload(1, para2);
   package_func_overload(1, para3);
   package_func_overload(1, para4);
END;
/

DECLARE
	resut  int;
	para1  int = 1;
	para2  smallint = 0;
BEGIN
   resut = package_func_overload(1, 1);
   package_func_overload(1, para1);
   package_func_overload(1, para2);
END;
/

--test named args
DECLARE
	resut  int;
	para1  int = 1;
	para2  smallint = 0;
BEGIN
   package_func_overload(col => 1, col2 => para1);
   package_func_overload(col2 => 1, col => para2);
   package_func_overload(col2 => para2, col => para1);
END;
/

call package_func_overload(1, 1);
call package_func_overload(1, '1');
select package_func_overload(1, 1);

--package function and none package function can not overload
create or replace procedure package_func_overload_1(col int, col2  varchar)
package
as
declare
    col_type text;
begin
     col2 := '122';
end;
/

 create or replace function package_func_overload_1(col int, col2  int)
 returns integer  as $$
 declare
     col_type text;
 begin
      col := 122;
  return 0;
 end;
 $$ language plpgsql;

 --test function replace
 create or replace function package_func_overload_2(col int, col2  bigint, col3 out int)
returns integer as $$
declare
    col_type text;
begin
     col := 122;
	 return 0;
end;
$$ language plpgsql;

create or replace function package_func_overload_2(col int, col2  bigint)
returns integer package as $$
declare
    col_type text;
begin
     col := 122;
	 return 0;
end;
$$ language plpgsql;

--test case for none overload function
create or replace function package_func_overload_3(col int, col2 out int)
return integer package
as
declare
    col_type text;
begin
     col := 122;
	 return 0;
end;
/
create or replace function package_func_overload_3(col int, col2 int)
return integer package
as
declare
    col_type text;
begin
     col := 122;
	 return 0;
end;
/

--none package function overload
create or replace function package_func_overload_4(col int, col2 out int)
returns integer as $$
declare
    col_type text;
begin
     col := 122;
	 return 0;
end;
$$ language plpgsql;

create or replace function package_func_overload_4(col int, col2  int)
returns integer as $$
declare
    col_type text;
begin
     col := 122;
	 return 0;
end;
$$ language plpgsql;


--test not enough parameter
create or replace function test_para1(col int)
returns integer package as $$
declare
begin
     col := 122;
	 return 0;
end;
$$ language plpgsql;
create or replace function test_para1(col smallint)
returns integer package as $$
declare
begin
     col := 122;
	 return 0;
end;
$$ language plpgsql;

DECLARE
	resut  int;
	para1  int = 1;
	para2  smallint = 0;
BEGIN
   test_para1();
END;
/

--do not support VARIADIC parameter for package function
CREATE OR REPLACE FUNCTION test_select(i IN INTEGER, VARIADIC arr INTEGER[])
RETURN INTEGER package
AS
	temp INTEGER :=0;
BEGIN
	temp:= arr[i];
	RETURN temp;
END;
/

CREATE OR REPLACE FUNCTION read_file(arg1 integer, arg3 integer  default 1 , out arg2 text)
RETURNS text package
 AS $$
DECLARE
  t1 text;
BEGIN
  t1 := 'abc1';
  return t1;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION read_file(arg1 integer, arg4 integer, arg3 integer  default 1 , out arg2 text)
RETURNS text package
AS $$
DECLARE
  t1 text;
BEGIN
  t1 := 'abc2';
  return t1;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION read_file1(arg1 integer ) RETURNS void AS $$
declare
	t2 text;
	t3 text;
begin
	t2 := read_file(10);
	raise notice 'this value: %',t2;
	return;
end;
$$ LANGUAGE plpgsql;
call read_file1(1);

--test inout parameter
create or replace function test_proc_define
(
	in_1  	IN VARCHAR2,
	in_2    VARCHAR2,
	out_1  	OUT VARCHAR2,
	inout_1  IN OUT VARCHAR
)
returns integer package as $$
declare
    col_type text;
BEGIN
	out_1 	:= in_1;
	inout_1 := inout_1 || in_2;
	return 0;
END;
$$ language plpgsql;

create or replace function test_proc_define
(
   in_1  IN VARCHAR2,
   in_2    VARCHAR2,
   out_1  OUT VARCHAR2
)
returns integer package as $$
declare
    col_type text;
BEGIN
	out_1 := in_1;
	return 1;
END;
$$ language plpgsql;

select  test_proc_define('hello', 'world', 'NO BIND');

--test alter function
alter function package_func_overload(int, smallint) package;

--test diffent namspace
create schema package_nps;
set current_schema = package_nps;
CREATE OR REPLACE FUNCTION read_file(arg1 integer, arg3 integer  default 1 , out arg2 text)
RETURNS text package
 AS $$
DECLARE
  t1 text;
BEGIN
  t1 := 'abc1';
  return t1;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION read_file(arg1 integer, arg4 integer, arg3 integer  default 1 , out arg2 text)
RETURNS text package
AS $$
DECLARE
  t1 text;
BEGIN
  t1 := 'abc2';
  return t1;
END;
$$ LANGUAGE plpgsql;

--get function defination
select pg_get_functiondef(f.oid) from (select oid , pronargs from pg_proc where proname='read_file' and pronargs = 2) f;

--test call
 call read_file(1,2,3,'ee');
 select read_file(1,2,3,'ee');
 select read_file(1,2,3);
 call read_file(1,2,3);

 select read_file(1,2,'eer');
 call  read_file(1,2,'eer');

 call  read_file(1,2);
 call read_file(arg1=>1, arg3=>3, arg4=>2, arg2=>'dd');

DECLARE
	resut  text;
	ds     text;
BEGIN
   resut = read_file(arg1=>1, arg3=>3, arg4=>2, arg2=>ds);
END;
/

drop function read_file(arg1 integer, arg3 integer  , out arg2 text) ;
DECLARE
	resut  text;
	ds     text;
BEGIN
   resut = read_file(arg1=>1, arg3=>3, arg4=>2, arg2=>ds);
END;
/

DECLARE
	resut  text;
	ds     text;
BEGIN
   read_file(arg1=>1, arg3=>3, arg4=>2, arg2=>ds);
END;
/


create or replace function test_inout_para1(col inout int, col2 int)
returns integer package as $$
declare
begin
     col := 122;
end;
$$ language plpgsql;

create or replace function test_inout_para1(col inout text, col2 int)
returns text package as $$
declare
begin
     col := 123;
end;
$$ language plpgsql;


DECLARE
	resut  int;
	resut1  text;
	para1  int = 1;
	para2  smallint = 0;
BEGIN
    test_inout_para1(resut,2);
    test_inout_para1(resut1,2);
END;
/

CREATE OR REPLACE PROCEDURE func_inner()
package
as
begin
end;
/

CREATE OR REPLACE PROCEDURE func_inner(id OUT NUMERIC, sex IN CHAR default 'f', name OUT varchar2, age IN INTEGER default 20)
package
AS
	temp_age INTEGER := 0;
	temp_sex INTEGER := 2;
	temp_num INTEGER := 0;
	name_dafault1 varchar2(100) := 'chao';
	name_dafault2 varchar2(100) := 'dfm';
	id_dafault1 NUMERIC := 1;
	id_dafault2 NUMERIC := 2;
BEGIN
	temp_age :=  age;
	id := id_dafault1;
	IF temp_age > 40 THEN
		id := id_dafault2;
	END IF;
	IF sex <> 'f' THEN
		temp_sex := 1;
	END IF;
	temp_num := temp_sex;
	IF temp_num > 0 THEN
		name := name_dafault1;
	END IF;
	IF temp_num > 1 THEN
		name := name_dafault2;
	END IF;
END;
/

CALL func_inner(id, age=>80,name=>'dfm');
CALL func_inner(id, sex=>'f',age=>40);

CREATE OR REPLACE PROCEDURE func_outter1(sex IN CHAR default 'f',age IN INTEGER default 20)
AS
	temp_id NUMERIC;
	temp_name varchar2(100);
	temp_sex char;
	temp_age NUMERIC;
BEGIN
	temp_sex := sex;
	temp_age := age;
	func_inner(temp_id,temp_sex,name=>temp_name,age=>temp_age);
END;
/
CALL func_outter1();

CREATE OR REPLACE PROCEDURE func_outter3(sex IN CHAR default 'f',age IN INTEGER default 20)
AS
	temp_id NUMERIC;
	temp_name varchar2(100);
	temp_sex char;
	temp_age NUMERIC;
BEGIN
	temp_sex := sex;
	temp_age := age;
	func_inner(temp_id,name=>temp_name);
END;
/
CALL func_outter3();

CREATE OR REPLACE PROCEDURE func_outter4(sex IN CHAR default 'f',age IN INTEGER default 20)
AS
	temp_id NUMERIC;
	temp_name varchar2(100);
	temp_sex char;
	temp_age NUMERIC;
BEGIN
	temp_sex := sex;
	temp_age := age;
	func_inner(temp_id,temp_sex,name=>temp_name);
END;
/
CALL func_outter4();
CALL func_outter4(age=>20);

CREATE OR REPLACE PROCEDURE func_outter5(sex IN CHAR default 'f',age IN INTEGER default 20)
AS
	temp_id NUMERIC;
	temp_name varchar2(100);
	temp_sex char;
	temp_age NUMERIC;
BEGIN
	temp_sex := sex;
	temp_age := age;
	func_inner(temp_id,age=>temp_age,name=>temp_name);
END;
/
CALL func_outter5();

CREATE OR REPLACE PROCEDURE func_outter6(sex IN CHAR default 'f',age IN INTEGER default 20)
AS
	temp_id NUMERIC;
	temp_name varchar2(100);
	temp_sex char;
	temp_age NUMERIC;
BEGIN
	temp_sex := sex;
	temp_age := age;
	func_inner(sex=>temp_sex,id=>temp_id,age=>temp_age,name=>temp_name);
END;
/
CALL func_outter6();

CREATE OR REPLACE PROCEDURE func_outter7(sex IN CHAR default 'f',age IN INTEGER default 20)
AS
	temp_id NUMERIC;
	temp_name varchar2(100);
	temp_sex char;
	temp_age NUMERIC;
BEGIN
	temp_sex := sex;
	temp_age := age;
	func_inner(temp_id,temp_sex,temp_name,temp_age);
END;
/
CALL func_outter7();


CREATE OR REPLACE PROCEDURE test_default_out()
package
as
begin
end;
/
CREATE OR REPLACE PROCEDURE test_default_out( i in integer, j out integer, k out integer, m in integer default 1, n in integer default 1, o out integer)
package
AS
	BEGIN
		j:=i;
		k := j + i;
		o := i + j + k + m + n;
		RETURN;
    END;
/
declare
a int := 1;
b int := 1;
c int := 1;
begin
	test_default_out(1, k=>a, n=>1, j=>b, o=>c);
end;
/

--error test
declare
a int := 1;
b int := 1;
c int := 1;
begin
	test_default_out(1, k=>a, n=>1, j=>b, j=>c);
end;
/

declare
a int := 1;
b int := 1;
c int := 1;
begin
	test_default_out(1, k=>a, n=>1, j=>1, o=>c);
end;
/

create or replace function test_para2(col in text, col2 out int)
returns text package as $$
declare
begin
     col2 := 123;
end;
$$ language plpgsql;

create or replace function test_para2(col in text, col2  varchar)
returns text package as $$
declare
begin
     col2 := 123;
end;
$$ language plpgsql;

declare
	varcl clob;
	buffer int;
	tr text;
begin
	test_para2(1, buffer);
	raise info 'buffer: %', buffer;
end;
/

drop schema package_schema cascade;
drop schema package_nps cascade;

\c regression;

drop schema if exists s1;
drop schema if exists s2;
create schema s1;
create schema s2;
set current_schema to s1;
create function package_func_overload_1(col int)
returns integer as $$
declare
begin
    return 0;
end;
$$ language plpgsql;
set current_schema to s2;
create function package_func_overload_1(col int)
returns integer as $$
declare
begin
    return 0;
end;
$$ language plpgsql;

reset current_schema;
drop schema s1 cascade;
drop schema s2 cascade;

create schema s;
set current_schema to s;
CREATE OR REPLACE PACKAGE p1 IS
PROCEDURE testpro1(var3 int);
PROCEDURE testpro1(var2 char);
END p1;
/

create function testpro1(col int)
returns integer as $$
declare
begin
     return 0;
end;
$$ language plpgsql;

reset current_schema;
drop schema s cascade;

drop package if exists pkg112;
create or replace package pkg112 
as
type ty1 is table of integer index by integer;
procedure p1(v1 in ty1,v2 out ty1,v3 inout ty1,v4 int);
procedure p1(v2 out ty1,v3 inout ty1,v4 int);
procedure p4();
pv1 ty1;
end pkg112;
/
set behavior_compat_options='proc_outparam_override';
drop package if exists pkg112;
create or replace package pkg112 
as
type ty1 is table of integer index by integer;
procedure p1(v1 in ty1,v2 out ty1,v3 inout ty1,v4 int);
procedure p1(v2 out ty1,v3 inout ty1,v4 int);
procedure p4();
pv1 ty1;
end pkg112;
/

drop package if exists pkg112;
set behavior_compat_options='';

--fix package synonym 
DROP DATABASE IF EXISTS db;
CREATE DATABASE db DBCOMPATIBILITY 'A';
\c db
CREATE USER pkg_user1 PASSWORD 'Abc@123456';
grant all on database db to pkg_user1;
CREATE USER pkg_user2 PASSWORD 'Abc@123456';
grant all on database db to pkg_user2;

create or replace synonym pkg_user2.syn1 for pkg_user1.pkg1;

SET ROLE pkg_user1 PASSWORD 'Abc@123456';
create or replace package pkg1 IS
cons1 constant text := 'lili';
PROCEDURE p1(p int);
PROCEDURE p1(p text);
end pkg1;
/
create or replace package body pkg1 IS
PROCEDURE p1(p int) IS
BEGIN
raise info 'the number is %.',p;
end;
PROCEDURE p1(p text) IS
BEGIN
raise info 'the text is %.',p;
end;
end pkg1;
/
grant all privileges on package pkg1 to pkg_user2;

SET ROLE pkg_user2 PASSWORD 'Abc@123456';
create or replace package pkg2 IS
PROCEDURE f1(p int);
end pkg2;
/
create or replace package body pkg2 is
PROCEDURE f1(p int) IS
BEGIN
syn1.p1(p);
end;
end pkg2;
/
call pkg2.f1(5);

-- create package which has func with end name
create or replace package trigger_test as
    function tri_insert_func() return trigger;
end trigger_test;
/
create or replace package body trigger_test as
    function tri_insert_func() return trigger as 
    begin
        insert into test_trigger_des_tbl values(new.id1, new.id2, new.id3);
        return new;
    end tri_insert_func;
end trigger_test;
/

DROP PACKAGE trigger_test;
\c regression
drop database db;
drop user pkg_user1;
drop user pkg_user2;

