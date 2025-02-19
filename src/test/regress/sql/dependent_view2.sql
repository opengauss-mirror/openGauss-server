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
\d+ v1
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

-- test multi-function
CREATE TABLE employees (
    employee_id serial PRIMARY KEY,
    employee_name varchar(50) NOT NULL,
    department varchar(50),
    salary numeric(10, 2),
    hire_date date
);
INSERT INTO employees (employee_name, department, salary, hire_date)
VALUES
    ('张三', '研发部', 8000.00, '2020-01-01'),
    ('李四', '市场部', 7500.00, '2021-03-15'),
    ('王五', '财务部', 7000.00, '2019-11-20'),
    ('赵六', '研发部', 8500.00, '2022-05-10');

CREATE  FUNCTION calculate_working_years(p_hire_date date) RETURNS integer
AS $$
DECLARE
    v_working_years integer;
    v_current_date date := current_date;
BEGIN
    v_working_years := extract(year from age(v_current_date, p_hire_date));
    RETURN v_working_years;
END;
$$ LANGUAGE plpgsql;
CREATE FUNCTION calculate_bonus_percentage(p_working_years integer) RETURNS numeric(5, 2)
AS $$
DECLARE
    v_bonus_percentage numeric(5, 2);
BEGIN
    IF p_working_years < 3 THEN
        v_bonus_percentage := 0.1;
    ELSIF p_working_years >= 3 AND p_working_years < 5 THEN
        v_bonus_percentage := 0.15;
    ELSE
        v_bonus_percentage := 0.2;
    END IF;
    RETURN v_bonus_percentage;
END;
$$ LANGUAGE plpgsql;

CREATE VIEW employee_bonus_view AS
SELECT employee_name,
       calculate_working_years(hire_date) AS working_years,
       calculate_bonus_percentage(calculate_working_years(hire_date)) AS bonus_percentage
FROM employees;

drop FUNCTION calculate_working_years;

CREATE OR REPLACE FUNCTION calculate_working_years(p_hire_date date) RETURNS integer
AS $$
DECLARE
    v_working_years integer;
    v_current_date date := current_date;
BEGIN
    v_working_years := extract(year from age(v_current_date, p_hire_date));
    RETURN v_working_years;
END;
$$ LANGUAGE plpgsql;

select pg_get_viewdef('employee_bonus_view');
select * from employee_bonus_view;

-- test function overloading with different nums of arg
drop FUNCTION if exists func_ddl0022(int,int);
CREATE FUNCTION func_ddl0022(a int, b int)
RETURNS int
AS $$
declare
sum int;
BEGIN
    select a + b into sum;
return sum;
END;
$$
LANGUAGE plpgsql;
drop FUNCTION if exists func_ddl0022(int,int,int);
CREATE  FUNCTION func_ddl0022(a int, b int,c int)
RETURNS int
AS $$
declare
sum int;
BEGIN
    select a + b + c into sum;
return sum;
END;
$$
LANGUAGE plpgsql;
create view v_ddl0022 as select func_ddl0022(1,2,3);
drop FUNCTION func_ddl0022(int,int,int);
CREATE  FUNCTION func_ddl0022(a int, b int,c int)
RETURNS int
AS $$
declare
sum int;
BEGIN
    select a + b + c into sum;
return sum;
END;
$$
LANGUAGE plpgsql;
select * from v_ddl0022;
-- delete non-depend function with same name
drop FUNCTION func_ddl0022(int,int);
select valid from pg_object where object_oid='v_ddl0022'::regclass;

drop schema dependent_view2 cascade;
reset current_schema;
