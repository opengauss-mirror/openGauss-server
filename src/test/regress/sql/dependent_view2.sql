create schema dependent_view2;
set current_schema to 'dependent_view2';

--from function table
create or replace function fun1(n1 in number, n2 in number)
return number
is
sum number;
begin
select n1 + n2 into sum;
return sum;
end;
/
create view v1 as select * from fun1(1,2);
select * from v1;
drop function fun1;
select * from v1; -- error
\d+ v1
create or replace function fun1(n1 in number, n2 in number)
return number
is
sum number;
begin
select n1 + n2 into sum;
return sum;
end;
/
select * from v1; -- success

-- targetlist
drop view v1;
create view v1 as select fun1(1,2);
drop function fun1;
select * from v1; -- error
\d+ v1
create or replace function fun1(n1 in number, n2 in number)
return number
is
sum number;
begin
select n1 + n2 into sum;
return sum;
end;
/
select * from v1; -- success

-- jointree
create table tab (a number, b number);
insert into tab values (1,3);
drop view v1;
create view v1 as select * from tab where b = fun1(1,2);
drop function fun1;
select * from v1; -- error
\d+ v1
create or replace function fun1(n1 in number, n2 in number)
return number
is
sum number;
begin
select n1 + n2 into sum;
return sum;
end;
/
select * from v1; -- success

-- cte
drop view v1;
create view v1 as with tmp as (select * from tab where b = fun1(1,2)) select * from tmp;
drop function fun1;
select * from v1; -- error
\d+ v1
create or replace function fun1(n1 in number, n2 in number)
return number
is
sum number;
begin
select n1 + n2 into sum;
return sum;
end;
/
select * from v1; -- success

-- subquery
drop view v1;
create view v1 as select *, (select fun1(1,2)) from tab;
drop function fun1;
select * from v1; -- error
\d+ v1
create or replace function fun1(n1 in number, n2 in number)
return number
is
sum number;
begin
select n1 + n2 into sum;
return sum;
end;
/
select * from v1; -- success

--union
drop view v1;
create view v1 as select a from tab union select fun1(1,2);
drop function fun1;
select * from v1; -- error
\d+ v1
create or replace function fun1(n1 in number, n2 in number)
return number
is
sum number;
begin
select n1 + n2 into sum;
return sum;
end;
/
select * from v1; -- success

-- rebuild function with different arg type
drop function fun1;
select * from v1; -- error
\d+ v1
create or replace function fun1(n1 in varchar, n2 in varchar)
return number
is
sum number;
begin
select n1 + n2 into sum;
return sum;
end;
/
select * from v1; -- success

-- rebuild function with different return type
drop view v1;
create view v1 as select fun1(1,2);
drop function fun1;
select * from v1; -- error
\d+ v1
create or replace function fun1(n1 in varchar, n2 in varchar)
return varchar
is
sum varchar;
begin
select n1 + n2 into sum;
return sum;
end;
/
select * from v1; -- success
\d+ v1

-- rebuild function with less number of args
drop function fun1;
select * from v1; -- error
\d+ v1
create or replace function fun1(n1 in varchar)
return varchar
is
begin
return 'abc';
end;
/
select * from v1; -- error
create or replace function fun1(n1 in varchar, n2 in varchar)
return varchar
is
sum varchar;
begin
select n1 + n2 into sum;
return sum;
end;
/
select * from v1; -- success
\d+ v1

--rebuild function with different schema
drop function fun1;
select * from v1; -- error
\d+ v1
create schema test_dv2;
create or replace function test_dv2.fun1(n1 in varchar, n2 in varchar)
return varchar
is
sum varchar;
begin
select n1 + n2 into sum;
return sum;
end;
/
select * from v1; -- error
drop schema test_dv2 cascade;

--test package
create or replace package my_pkg as
    function fun1(n1 in number, n2 in number) return number;
end my_pkg;
/

create or replace package body my_pkg as
    function fun1(n1 in number, n2 in number)
return number
is
sum number;
begin
select n1 + n2 into sum;
return sum;
end;
end my_pkg;
/
drop view v1;
create view v1 as select my_pkg.fun1(1,2);
drop package my_pkg;
select * from v1; -- error
create or replace package my_pkg as
    function fun1(n1 in number, n2 in number) return number;
end my_pkg;
/

create or replace package body my_pkg as
    function fun1(n1 in number, n2 in number)
return number
is
sum number;
begin
select n1 + n2 into sum;
return sum;
end;
end my_pkg;
/
select * from v1; -- success

-- test material view
create or replace function fun1(n1 in varchar, n2 in varchar)
return varchar
is
sum varchar;
begin
select n1 + n2 into sum;
return sum;
end;
/
create materialized view mv1 as select fun1(1,2);
drop function fun1;
select * from mv1; --success
refresh materialized view mv1; --error
create or replace function fun1(n1 in varchar, n2 in varchar)
return varchar
is
sum varchar;
begin
select n1 + n2 into sum;
return sum;
end;
/
refresh materialized view mv1; --success

--test create or replace function
drop view v1;
create view v1 as select fun1(1,1);
create or replace function fun1(n1 in number, n2 in number) --success
return number
is
sum number;
begin
select n1 + n2 into sum;
return sum;
end;
/
select valid from pg_object where object_oid = 'v1'::regclass; --invalid
select * from v1; --success
create or replace function fun1(n1 in varchar, n2 in varchar) --success
return number
is
sum number;
begin
select n1 + n2 into sum;
return sum;
end;
/
select valid from pg_object where object_oid = 'v1'::regclass; --valid
select * from v1; --success

-- test function overloading
create or replace function fun1(n1 in number, n2 in number)
returns number
as $$
declare
sum number;
begin
select n1 + n2 into sum;
return sum;
end; $$
language PLPGSQL;
create or replace function fun1(n1 in varchar, n2 in varchar)
returns number
as $$
declare
sum number;
begin
select n1 + n2 into sum;
return sum;
end; $$
language PLPGSQL;
drop view v1;
create view v1 as select fun1(1,1);
drop function fun1(number,number);
select valid from pg_object where object_oid = 'v1'::regclass; --invalid
select * from v1; --success

--test procedure
CREATE OR REPLACE procedure depend_p1(var1 varchar,var2 out varchar)
as
p_num varchar:='aaa';
begin
var2:=var1||p_num;
END;
/
CREATE OR REPLACE VIEW depend_v1 AS select depend_p1('aa');
select * from depend_v1;
select definition from pg_views where viewname='depend_v1';
drop procedure depend_p1;
select * from depend_v1; --failed
select valid from pg_object where object_oid = ' depend_v1'::regclass; --invalid
CREATE OR REPLACE procedure depend_p1(var1 varchar,var2 out varchar)
as
p_num varchar:='aaa';
begin
var2:=var1||p_num;
END;
/
select * from depend_v1; --success

drop schema dependent_view2 cascade;
reset current_schema;
