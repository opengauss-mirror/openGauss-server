create schema out_param_schema;
set current_schema= out_param_schema;
set behavior_compat_options='proc_outparam_override';

--1--------return 变量
CREATE or replace FUNCTION func1(in a integer, out b integer)
RETURNS int
AS $$
DECLARE
    c int;
    BEGIN
        c := 1;
        b := a + c;
        return c;
    END; $$
LANGUAGE 'plpgsql' NOT FENCED;

select func1(2, null);
call func1(2, NULL);
select * from func1(2,null);
select func1(a => 2, b => null);
select * from func1(a => 2, b => null);
declare
    result integer;
    a integer := 2;
    b integer := NULL;
begin 
    result := func1(a => a, b => b);  
    raise info 'b is: %', b;
    raise info 'result is: %', result;
end;
/

declare
    result integer;
    a integer := 2;
    b integer := NULL;
begin
    select * into result from func1(a => a, b => b);
    raise info 'b is: %', b;
    raise info 'result is: %', result;
end;
/

declare
    result integer;
    a integer := 2;
    b integer := NULL;
begin 
    result := func1(a, b);  
    raise info 'b is: %', b;
    raise info 'result is: %', result;
end;
/

declare
    result integer;
    a integer := 2;
    b integer := NULL;
begin  
    func1(a => a, b => b);   
    raise info 'b is: %', b;
    raise info 'result is: %', result;
end;
/

declare
    result text;
    a integer := 2;
    b integer := NULL;
begin
    func1(a => a, b => b);   
    raise info 'b is: %', b;
    raise info 'result is: %', result;
end;
/
---inout参数
CREATE or replace FUNCTION func1_1(in a integer, inout b integer)
RETURNS int
AS $$
DECLARE
    c int;
    BEGIN
        c := 1;
        b := a + c;
        return c;
    END; $$
LANGUAGE 'plpgsql' NOT FENCED;

select func1_1(2, null);
call func1_1(2, NULL);
select * from func1_1(2,null);
declare
    result integer;
    a integer := 2;
    b integer := NULL;
begin 
    result := func1_1(a => a, b => b);  
    raise info 'b is: %', b;
    raise info 'result is: %', result;
end;
/

declare
    result integer;
    a integer := 2;
    b integer := NULL;
begin 
    result := func1_1(a, b);  
    raise info 'b is: %', b;
    raise info 'result is: %', result;
end;
/

declare
    result integer;
    a integer := 2;
    b integer := NULL;
begin  
    func1_1(a => a, b => b);   
    raise info 'b is: %', b;
    raise info 'result is: %', result;
end;
/

declare
    result text;
    a integer := 2;
    b integer := NULL;
begin
    func1_1(a => a, b => b);   
    raise info 'b is: %', b;
    raise info 'result is: %', result;
end;
/

--2--------return 变量运算
CREATE or replace FUNCTION func2(in a integer, out b integer)
RETURNS int
AS $$
DECLARE
    c int;
    BEGIN
        c := 1;
        b := a + c;
        return b + c;
    END; $$
LANGUAGE 'plpgsql' NOT FENCED;

select func2(2, null);
call func2(2, NULL);
select * from func2(2,null);
declare
    result integer;
    a integer := 2;
    b integer := NULL;
begin 
    result := func2(a => a, b => b);  
    raise info 'b is: %', b;
    raise info 'result is: %', result;
end;
/

declare
    result integer;
    a integer := 2;
    b integer := NULL;
begin 
    result := func2(a, b);  
    raise info 'b is: %', b;
    raise info 'result is: %', result;
end;
/

declare
    result integer;
    a integer := 2;
    b integer := NULL;
begin  
    func2(a => a, b => b);   
    raise info 'b is: %', b;
    raise info 'result is: %', result;
end;
/

declare
    result text;
    a integer := 2;
    b integer := NULL;
begin
    func2(a => a, b => b);   
    raise info 'b is: %', b;
    raise info 'result is: %', result;
end;
/

--3------return 常量
CREATE or replace FUNCTION func3(in a integer, out b integer)
RETURNS int
AS $$
DECLARE
    c int;
    BEGIN
        c := 1;
        b := a + c;
        return 123;
    END; $$
LANGUAGE 'plpgsql' NOT FENCED;

select func3(2, null);
call func3(2, NULL);
select * from func3(2,null);
declare
    result integer;
    a integer := 2;
    b integer := NULL;
begin 
    result := func3(a => a, b => b);  
    raise info 'b is: %', b;
    raise info 'result is: %', result;
end;
/

declare
    result integer;
    a integer := 2;
    b integer := NULL;
begin 
    result := func3(a, b);  
    raise info 'b is: %', b;
    raise info 'result is: %', result;
end;
/

declare
    result integer;
    a integer := 2;
    b integer := NULL;
begin  
    func3(a => a, b => b);   
    raise info 'b is: %', b;
    raise info 'result is: %', result;
end;
/

declare
    result text;
    a integer := 2;
    b integer := NULL;
begin
    func3(a => a, b => b);   
    raise info 'b is: %', b;
    raise info 'result is: %', result;
end;
/

--4------多out
CREATE or replace FUNCTION func4(in a integer, out b integer, out d integer)
RETURNS int
AS $$
DECLARE
    c int;
    BEGIN
        c := 1;
        b := a + c;
        d := a + c;
        return c;
    END; $$
LANGUAGE 'plpgsql' NOT FENCED;

select func4(2,NULL,NULL);
call func4(2, NULL,NULL);
select * from func4(2, NULL,NULL);
declare
    result integer;
    a integer := 2;
    b integer := NULL;
    d integer := NULL;
begin  
    result := func4(a => a, b => b,d => d);
    raise info 'b is: %', b;
	 raise info 'd is: %', d;
    raise info 'result is: %', result;
end;
/

declare
    result integer;
    a integer := 2;
    b integer := NULL;
    d integer := NULL;
begin  
    result := func4(a, b, d);
    raise info 'b is: %', b;
	 raise info 'd is: %', d;
    raise info 'result is: %', result;
end;
/

declare
    result integer;
    a integer := 2;
    b integer := NULL;
    d integer := NULL;
begin  
    func4(a => a, b => b,d => d);
    raise info 'b is: %', b;
	 raise info 'd is: %', d;
    raise info 'result is: %', result;
end;
/

declare
    result integer;
    a integer := 2;
    b integer := NULL;
    d integer := NULL;
begin  
    func4(a => a, b => b,d => d);
    raise info 'b is: %', b;
	 raise info 'd is: %', d;
    raise info 'result is: %', result;
end;
/
---inout参数
CREATE or replace FUNCTION func4_1(in a integer, inout b integer, inout d integer)
RETURNS int
AS $$
DECLARE
    c int;
    BEGIN
        c := 1;
        b := a + c;
        d := a + c;
        return c;
    END; $$
LANGUAGE 'plpgsql' NOT FENCED;

select func4_1(2,NULL,NULL);
call func4_1(2, NULL,NULL);
select * from func4_1(2, NULL,NULL);
declare
    result integer;
    a integer := 2;
    b integer := NULL;
    d integer := NULL;
begin  
    result := func4_1(a => a, b => b,d => d);
    raise info 'b is: %', b;
	 raise info 'd is: %', d;
    raise info 'result is: %', result;
end;
/

declare
    result integer;
    a integer := 2;
    b integer := NULL;
    d integer := NULL;
begin  
    result := func4_1(a, b, d);
    raise info 'b is: %', b;
	 raise info 'd is: %', d;
    raise info 'result is: %', result;
end;
/

declare
    result integer;
    a integer := 2;
    b integer := NULL;
    d integer := NULL;
begin  
    func4_1(a => a, b => b,d => d);
    raise info 'b is: %', b;
	 raise info 'd is: %', d;
    raise info 'result is: %', result;
end;
/

declare
    result integer;
    a integer := 2;
    b integer := NULL;
    d integer := NULL;
begin  
    func4_1(a => a, b => b,d => d);
    raise info 'b is: %', b;
	 raise info 'd is: %', d;
    raise info 'result is: %', result;
end;
/

--5-- 有out+ 无return 不支持，在执行时报错--
--5.1
CREATE or replace FUNCTION func5_1(in a integer, out b integer)
RETURNS int
AS $$
DECLARE
    c int;
    BEGIN
        c := 1;
        b := a + c;
        --return;
    END; $$
LANGUAGE 'plpgsql' NOT FENCED;

select func5_1(2, NULL);
call func5_1(2, NULL);
select * from func5_1(2, NULL);
declare
    result integer;
    a integer := 2;
    b integer := NULL;
begin  
    result := func5_1(a => a, b => b);
    raise info 'b is: %', b;
    raise info 'result is: %', result;
end;
/

declare
    result integer;
    a integer := 2;
    b integer := NULL;
begin  
    result := func5_1(a, b);
    raise info 'b is: %', b;
    raise info 'result is: %', result;
end;
/

declare
    result integer;
    a integer := 2;
    b integer := NULL;
begin  
    func5_1(a => a, b => b);
    raise info 'b is: %', b;
    raise info 'result is: %', result;
end;
/

--5.2
CREATE or replace FUNCTION func5_2(in a integer, out b integer)
RETURNS int
AS $$
DECLARE
    c int;
    BEGIN
        c := 1;
        b := a + c;
        return;
    END; $$
LANGUAGE 'plpgsql' NOT FENCED;

select func5_2(2, NULL);
call func5_2(2, NULL);
select * from func5_2(2, NULL);
declare
    result integer;
    a integer := 2;
    b integer := NULL;
begin  
    result := func5_2(a => a, b => b);
    raise info 'b is: %', b;
    raise info 'result is: %', result;
end;
/

declare
    result integer;
    a integer := 2;
    b integer := NULL;
begin  
    result := func5_2(a, b);
    raise info 'b is: %', b;
    raise info 'result is: %', result;
end;
/

declare
    result integer;
    a integer := 2;
    b integer := NULL;
begin  
    func5_2(a => a, b => b);
    raise info 'b is: %', b;
    raise info 'result is: %', result;
end;
/

--6自治事务
--6.1 单out
CREATE or replace FUNCTION func6_1(in a integer, out b integer)
RETURNS int
AS $$
DECLARE
	PRAGMA AUTONOMOUS_TRANSACTION; 
    c int;
    BEGIN
        c := 1;
        b := a + c;
        return c;
    END; $$
LANGUAGE 'plpgsql' NOT FENCED;

select func6_1(2, null);
call func6_1(2, NULL);
select * from func6_1(2,null);
declare
    result integer;
    a integer := 2;
    b integer := NULL;
begin 
    result := func6_1(a => a, b => b);  
    raise info 'b is: %', b;
    raise info 'result is: %', result;
end;
/

declare
    result integer;
    a integer := 2;
    b integer := NULL;
begin 
    result := func6_1(a, b);  
    raise info 'b is: %', b;
    raise info 'result is: %', result;
end;
/

declare
    result integer;
    a integer := 2;
    b integer := NULL;
begin  
    func6_1(a => a, b => b);   
    raise info 'b is: %', b;
    raise info 'result is: %', result;
end;
/

declare
    result text;
    a integer := 2;
    b integer := NULL;
begin
    func6_1(a => a, b => b);   
    raise info 'b is: %', b;
    raise info 'result is: %', result;
end;
/

--6.2 多out
CREATE or replace FUNCTION func6_2(in a integer, out b integer, out d integer)
RETURNS int
AS $$
DECLARE
	PRAGMA AUTONOMOUS_TRANSACTION;
    c int;
    BEGIN
        c := 1;
        b := a + c;
        d := a + c;
        return c;
    END; $$
LANGUAGE 'plpgsql' NOT FENCED;

select func6_2(2,NULL,NULL);
call func6_2(2, NULL,NULL);
select * from func6_2(2, NULL,NULL);
declare
    result integer;
    a integer := 2;
    b integer := NULL;
    d integer := NULL;
begin  
    result := func6_2(a => a, b => b,d => d);
    raise info 'b is: %', b;
	 raise info 'd is: %', d;
    raise info 'result is: %', result;
end;
/

declare
    result integer;
    a integer := 2;
    b integer := NULL;
    d integer := NULL;
begin  
    result := func6_2(a, b,d);
    raise info 'b is: %', b;
	 raise info 'd is: %', d;
    raise info 'result is: %', result;
end;
/

declare
    result integer;
    a integer := 2;
    b integer := NULL;
    d integer := NULL;
begin  
    func6_2(a => a, b => b,d => d);
    raise info 'b is: %', b;
	 raise info 'd is: %', d;
    raise info 'result is: %', result;
end;
/

declare
    result integer;
    a integer := 2;
    b integer := NULL;
    d integer := NULL;
begin  
    func6_2(a => a, b => b,d => d);
    raise info 'b is: %', b;
	 raise info 'd is: %', d;
    raise info 'result is: %', result;
end;
/

--7 packge
--7.1普通out出参
create or replace package pck7_1
is
function func7_1(in a int, out b int)
return int;
end pck7_1;
/ 

CREATE or replace package body pck7_1 as FUNCTION func7_1(in a int, out b integer)
RETURN int
AS 
DECLARE
	--PRAGMA AUTONOMOUS_TRANSACTION; 
	c int;
    BEGIN
        c := 1;
        b := a + c;
        return c;
    END;
end pck7_1;
/

select pck7_1.func7_1(2, null);
call pck7_1.func7_1(2, NULL);
select * from pck7_1.func7_1(2,null);
declare
    result integer;
    a integer := 2;
    b integer := NULL;
begin 
    result := pck7_1.func7_1(a => a, b => b);  
    raise info 'b is: %', b;
    raise info 'result is: %', result;
end;
/

declare
    result integer;
    a integer := 2;
    b integer := NULL;
begin 
    result := pck7_1.func7_1(a, b);  
    raise info 'b is: %', b;
    raise info 'result is: %', result;
end;
/

declare
    result integer;
    a integer := 2;
    b integer := NULL;
begin  
    pck7_1.func7_1(a => a, b => b);   
    raise info 'b is: %', b;
    raise info 'result is: %', result;
end;
/

declare
    result text;
    a integer := 2;
    b integer := NULL;
begin
    pck7_1.func7_1(a => a, b => b);   
    raise info 'b is: %', b;
    raise info 'result is: %', result;
end;
/
--7.2带自治事务out出参

create or replace package pck7_2
is
function func7_2(in a int, out b int)
return int;
end pck7_2;
/ 

CREATE or replace package body pck7_2 as FUNCTION func7_2(in a int, out b integer)
RETURN int
AS 
DECLARE
	PRAGMA AUTONOMOUS_TRANSACTION; 
	c int;
    BEGIN
        c := 1;
        b := a + c;
        return c;
    END;
end pck7_2;
/

select pck7_2.func7_2(2, null);
call pck7_2.func7_2(2, NULL);
select * from pck7_2.func7_2(2,null);
declare
    result integer;
    a integer := 2;
    b integer := NULL;
begin 
    result := pck7_2.func7_2(a => a, b => b);  
    raise info 'b is: %', b;
    raise info 'result is: %', result;
end;
/

declare
    result integer;
    a integer := 2;
    b integer := NULL;
begin 
    result := pck7_2.func7_2(a, b);  
    raise info 'b is: %', b;
    raise info 'result is: %', result;
end;
/

declare
    result integer;
    a integer := 2;
    b integer := NULL;
begin  
    pck7_2.func7_2(a => a, b => b);   
    raise info 'b is: %', b;
    raise info 'result is: %', result;
end;
/

declare
    result text;
    a integer := 2;
    b integer := NULL;
begin
    pck7_2.func7_2(a => a, b => b);   
    raise info 'b is: %', b;
    raise info 'result is: %', result;
end;
/

--8 out出参不允许重载限制
--8.1 plpgsql语言的带out参数同名函数只能存在一个
CREATE or replace FUNCTION func8_1(in a integer)
RETURNS int
AS $$
DECLARE
    b int;
    c int;
    BEGIN
        c := 1;
        b := a + c;
        return c;
    END; $$
LANGUAGE 'plpgsql' NOT FENCED;
CREATE or replace FUNCTION func8_1(in a integer, out b integer)
RETURNS int
AS $$
DECLARE
    c int;
    BEGIN
        c := 1;
        b := a + c;
        return c;
    END; $$
LANGUAGE 'plpgsql' NOT FENCED;
CREATE or replace FUNCTION func8_1(in a integer, out b integer, out d integer)
RETURNS int
AS $$
DECLARE
    c int;
    BEGIN
        c := 1;
        b := a + c;
		d := b;
        return c;
    END; $$
LANGUAGE 'plpgsql' NOT FENCED;
--8.2 同一schema、package下，不允许存在同名的plpgsql语言的out出参函数，但可以replace
CREATE or replace FUNCTION func8_2(in a integer, out b integer)
RETURNS int
AS $$
DECLARE
    c int;
    BEGIN
        c := 1;
        b := a + c;
        return c;
    END; $$
LANGUAGE 'plpgsql' NOT FENCED;
CREATE or replace FUNCTION func8_2(in a integer, out b integer, out d integer)
RETURNS int
AS $$
DECLARE
    c int;
    BEGIN
        c := 1;
        b := a + c;
		d := b;
        return c;
    END; $$
LANGUAGE 'plpgsql' NOT FENCED;
CREATE or replace FUNCTION func8_2(in a integer, out b integer)
RETURNS int
AS $$
DECLARE
    c int;
    BEGIN
        c := 1;
        b := a + c;
        return c;
    END; $$
LANGUAGE 'plpgsql' NOT FENCED;

create or replace package pck8_2
is
function func8_2(in a int, out b int)
return int;
function func8_2(in a int, out b int, out d integer)
return int;
end pck8_2;
/ 

--8.3 同一schema、package下，允许存在同名的psql语言的不带out出参函数
CREATE or replace FUNCTION func8_3(in a integer)
RETURNS int
AS $$
DECLARE
    c int;
	b int;
    BEGIN
        c := 1;
        b := a + c;
        return c;
    END; $$
LANGUAGE 'plpgsql' NOT FENCED;
CREATE or replace FUNCTION func8_3(in a integer, in b integer)
RETURNS int
AS $$
DECLARE
    c int;
    BEGIN
        c := 1;
        b := a + c;
        return c;
    END; $$
LANGUAGE 'plpgsql' NOT FENCED;

create or replace package pck8_3
is
function func8_3(in a int)
return int;
function func8_3(in a int, in b int)
return int;
end pck8_3;
/ 
select proname from pg_proc where proname = 'func8_3' order by 1;

create or replace function f1(in a int, out b int) return int
as
declare
c int;
begin
c := a - 1;
b := a + 1;
return c;
end;
/

select * from generate_series(1,100) where generate_series > f1(90, null);

declare
res int;
begin
res := f1(10, 888); -- out出参传入常量，报错
raise info 'res is:%',res;
end;
/

drop function f1;

create or replace package pck1 is
type tp1 is record(v01 number, v03 varchar2, v02 number);
function f1(in a int, out c tp1) return int;
end pck1;
/

create or replace package body pck1 is
function f1(in a int, out c tp1) return int
as
declare
begin
c.v01:=a;
return a;
end;
end pck1;
/

select pck1.f1(10,(1,'a',2));
select *from pck1.f1(10,(1,'a',2));
call pck1.f1(10,(1,'a',2));


--clean
reset behavior_compat_options;

drop schema out_param_schema cascade;
