create schema out_param_func_overload;
set current_schema= out_param_func_overload;
set behavior_compat_options='proc_outparam_override';


-- 0
create or replace package pkg_type 
as 
function func(i1 in int, o1 out number, o2 out varchar2) return number;
function func(i1 in int, i2 in int, o1 out number, o2 out varchar2) return number;
end pkg_type;
/

create or replace package body pkg_type
is
   function func(i1 in int, o1 out number, o2 out varchar2)
	return number
	is
	BEGIN
	    raise notice 'func(i1 in int, o1 out number, o2 out varchar2)';
		o1 := 12.34;
		o2 := 'test1';
		return 12.34;
	end;
	function func(i1 in int, i2 in int, o1 out number, o2 out varchar2)
	return number
	is
	begin 
	    raise notice 'func(i1 in int, i2 in int, o1 out number, o2 out varchar2)';
		o1 := 43.21;
		o2 := 'test2';
		return 43.21;
	end;
end pkg_type;
/

DECLARE
ii1 int := -1;
ii2 int := -1;
oo1 number := -1;
oo2 varchar2 := '';
rr1 number := -1;
begin
    rr1 := pkg_type.func(ii1, ii2, oo1, oo2);
	raise notice 'pkg_type ii1:%', ii1;
	raise notice 'pkg_type ii2:%', ii2;
	raise notice 'pkg_type oo1:%', oo1;
	raise notice 'pkg_type oo2:%', oo2;
	raise notice 'pkg_type rr1:%', rr1;
END;
/

DECLARE
ii1 int := -1;
ii2 int := -1;
oo1 number := -1;
oo2 varchar2 := '';
rr1 number := -1;
begin
    rr1 := pkg_type.func(ii1, oo1, oo2);
	raise notice 'pkg_type ii1:%', ii1;
	raise notice 'pkg_type ii2:%', ii2;
	raise notice 'pkg_type oo1:%', oo1;
	raise notice 'pkg_type oo2:%', oo2;
	raise notice 'pkg_type rr1:%', rr1;
END;
/
drop package pkg_type;

-- 1 private
create or replace package pkg_type as 
function func(a varchar2) return varchar2;
end pkg_type;
/
create or replace package body pkg_type as 
function func(a varchar2) return varchar2
as
b varchar2(5) :='var';
BEGIN
return b;
end;
function func(a integer) return integer
as
b integer := 2;
BEGIN
return b;
end;
end pkg_type;
/
select pkg_type.func(1);
drop package pkg_type;

-- test overload
create or replace package pkg_type is 
   function func(a int) return int;
   function func(a int, b out int) return int;
end pkg_type;
/
create or replace package body pkg_type
is
    function func(a int)
	return int
	is
	BEGIN
	    raise notice 'func(a int)';
		return 1;
	end;
	function func(a int, b out int)
	return int
	is
	begin 
	    b := 1;
		raise notice 'func(a int, b out int)';
		return 2;
	end;
end pkg_type;
/

DECLARE
a int := -1;
b int := -1;
c int := -1;
begin
    c := pkg_type.func(a);
	raise notice 'pkg_type a:%', a;
	raise notice 'pkg_type b:%', b;
	raise notice 'pkg_type c:%', c;
END;
/

DECLARE
a int := -1;
b int := -1;
c int := -1;
begin
    c := pkg_type.func(a, b);
	raise notice 'pkg_type a:%', a;
	raise notice 'pkg_type b:%', b;
	raise notice 'pkg_type c:%', c;
END;
/
drop package pkg_type;


-- test overload with out
create or replace package pkg_type is 
   function func(a out int) return int;
   function func(a int, b out int) return int;
end pkg_type;
/

create or replace package body pkg_type
is
    function func(a out int)
	return int
	is
	BEGIN
	    raise notice 'func(a out int)';
		a := 1;
		return 2;
	end;
	function func(a int, b out int)
	return int
	is
	begin 
		raise notice 'func(a int, b out int)';
	    b := 1;
		return 3;
	end;
end pkg_type;
/

DECLARE
a int := -1;
b int := -1;
c int := -1;
begin
    c := pkg_type.func(a, b);
	raise notice 'pkg_type a:%', a;
	raise notice 'pkg_type b:%', b;
	raise notice 'pkg_type c:%', c;
END;
/

DECLARE
a int := -1;
b int := -1;
c int := -1;
begin
    c := pkg_type.func(a);
	raise notice 'pkg_type a:%', a;
	raise notice 'pkg_type b:%', b;
	raise notice 'pkg_type c:%', c;
END;
/
drop package pkg_type;


create or replace package pkg_type is 
   function func(a int, b out int) return int;
   function func2(a int, b out int) return int;
end pkg_type;
/
create or replace package body pkg_type
is
    function func(a int, b out int)
	return int
	is
	BEGIN
	    b := 1;
	    raise notice 'func(a int, b out int)';
		return 1;
	end;
	function func2(a int, b out int)
	return int
	is
	begin 
	    b := 2;
		raise notice 'func2(a int, b out int): b:%', b;
		func(a, b);
		raise notice 'func2(a int, b out int): b:%', b;
		return 2;
	end;
end pkg_type;
/

DECLARE
a int := -1;
b int := -1;
c int := -1;
begin
    c := pkg_type.func2(a, b);
	raise notice 'pkg_type a:%', a;
	raise notice 'pkg_type b:%', b;
	raise notice 'pkg_type c:%', c;
END;
/

drop package pkg_type;

CREATE OR REPLACE PACKAGE pac_test_1 AS
FUNCTION f_test_1(para1 in out int, para2 in out int, para3 in out int) RETURN int;
END pac_test_1;
/

CREATE OR REPLACE PACKAGE BODY pac_test_1 AS
FUNCTION f_test_1(para1 in out int, para2 in out int, para3 in out int)
RETURN int IS
BEGIN
RETURN 1;
END;
END pac_test_1;
/

create table t1(c1 int,c2 text) with (ORIENTATION=COLUMN);;
insert into t1 select a,a || 'test' from generate_series(1,10) as a;

create view v1 as select c1,c2,pac_test_1.f_test_1(c1,c1,c1) from t1;
select * from v1;

drop view v1;
drop package pac_test_1;
drop table t1;

create or replace procedure proc_test is
begin
perform count(1);
end;
/

--clean
reset behavior_compat_options;

drop schema out_param_func_overload cascade;
