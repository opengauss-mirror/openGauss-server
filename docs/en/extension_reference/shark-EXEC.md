# EXEC<a name="ZH-CN_TOPIC_0289899950"></a>

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-07-30T06:32:10.626Z pushedAt=2026-09-22T02:09:02.886Z -->

## Function Description<a name="zh-cn_topic_0283136841_zh-cn_topic_0237122186_zh-cn_topic_0059779029_s8a5c6264f78f49e3aa93f388d68cd3e6"></a>

Used to execute functions, stored procedures, and SQL statements.

## Precautions<a name="zh-cn_topic_0283136841_zh-cn_topic_0237122186_zh-cn_topic_0059779029_s8cb7444b58764d99913a4cc61f397f9f"></a>

- This section only contains the syntax newly added by shark. The original openGauss syntax has not been deleted or modified.
- The exec syntax is newly supported.

## Syntax<a name="en_topic_0283136841_en_topic_0237122186_en_topic_0059779029_s29888afda1844d6f9fc677f1b59b5b7d"></a>

```
Executes a function or stored procedure.
EXEC { [ @return_status = ] { module_name | @module_name_var } [ [ @parameter = ] { value | @variable [ OUTPUT ] } ] [ ,...n ] };
Executes a SQL statement.
EXEC( { @string_variable | 'tsql_string' } [ + ...n ] );
```

## Examples<a name="zh-cn_topic_0283136841_zh-cn_topic_0237122186_zh-cn_topic_0059779029_s51d29fa208274032a4e5308b57638421"></a>

```
----Execute a function.
test_d=# create or replace function employee_salary_func (
test_d(#     employee_salary NUMERIC
test_d(# ) returns NUMERIC
test_d-# as $$
test_d$# begin
test_d$#     return employee_salary;
test_d$# end;
test_d$# $$ language plpgsql;
CREATE FUNCTION
test_d=# declare
test_d-# result NUMERIC;
test_d-# begin
test_d$# exec result = employee_salary_func 100.01;
test_d$# raise notice 'result: %',result;
test_d$# end;
test_d$# /
NOTICE:  result: 100.01
ANONYMOUS BLOCK EXECUTE

----Execute a stored procedure.
test_d=# create table employees(
test_d(#     employee_id integer,
test_d(#     employee_name VARCHAR(100),
test_d(#     department VARCHAR(50),
test_d(#     salary NUMERIC
test_d(# );
CREATE TABLE
test_d=# create or replace procedure add_employee_details_proc(
test_d(#     IN id1 INTEGER,
test_d(#     IN department1 text,
test_d(#     IN name1 VARCHAR(100),
test_d(#     IN salary1 NUMERIC
test_d(# )
test_d-# package as
test_d$# begin
test_d$#     insert into employees values(id1, name1, department1, salary1);
test_d$# end;
test_d$# /
CREATE PROCEDURE
test_d=# exec add_employee_details_proc 1001, 'cc', 'aaa', 7000;
CALL
test_d=# select * from employees ;
 employee_id | employee_name | department | salary
-------------+---------------+------------+--------
        1001 | aaa           | cc         |   7000
(1 row)

----Execute an SQL statement.
test_d=# exec ('delete from employees where employee_id = 1001;');
ANONYMOUS BLOCK EXECUTE
test_d=# select * from employees ;
 employee_id | employee_name | department | salary
-------------+---------------+------------+--------
(0 rows)

```