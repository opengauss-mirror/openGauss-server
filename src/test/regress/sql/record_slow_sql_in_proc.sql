\c postgres
alter system set instr_unique_sql_count to 1000;
delete from statement_history;
--generate slow sql
CREATE TABLE big_table (
                           id SERIAL PRIMARY KEY,
                           column1 INT,
                           column2 VARCHAR(100)
);
--set the slow sql threshold
set log_min_duration_statement = 50; --50ms

--test slow sql in proc
create or replace procedure test_slow_sql()
is
begin
perform 1;
PERFORM pg_sleep(0.1);
end;
/
-- record all sql
set track_stmt_stat_level = 'L1,L1';
set instr_unique_sql_track_type = 'all';
call test_slow_sql();
-- record slow sql
set track_stmt_stat_level = 'OFF,L1';
call test_slow_sql();
call pg_sleep(0.2);
select query, query_plan, is_slow_sql from statement_history where parent_query_id != 0 order by start_time;
delete from statement_history;

set track_stmt_stat_level = 'L1,L1';
set instr_unique_sql_track_type = 'all';

--test exec_plsql(possibly with an insert query)
create or replace procedure test_exec_plsql()
is
begin
INSERT INTO big_table (column1, column2) SELECT generate_series(1, 10000), 'data' || generate_series(1, 10000);
end;
/
call test_exec_plsql();
call pg_sleep(0.2);
select query, query_plan from statement_history where parent_query_id != 0 order by start_time;
delete from statement_history;


--test perform
CREATE OR REPLACE PROCEDURE test_exec_perform()
AS
BEGIN
PERFORM pg_sleep(0.1);
END;
/
call test_exec_perform();
call pg_sleep(0.2);
select query, query_plan from statement_history where parent_query_id != 0 order by start_time;
delete from statement_history;

--test return query
CREATE OR REPLACE FUNCTION test_return_query()
RETURNS TABLE (column1 int, column2 VARCHAR(100))
LANGUAGE plpgsql
AS $$
BEGIN
RETURN QUERY SELECT column1, column2 FROM big_table WHERE column1 = 9909 ORDER BY column1 DESC;
END;
$$;
call test_return_query();
call pg_sleep(0.2);
select query, query_plan from statement_history where parent_query_id != 0 order by start_time;
delete from statement_history;

--test open cursor
CREATE OR REPLACE PROCEDURE test_exec_open()
AS
DECLARE
cur refcursor;
    row record;
BEGIN
OPEN cur FOR SELECT column1, column2 FROM big_table where column1 = 9909;
LOOP
FETCH NEXT FROM cur INTO row;
        EXIT WHEN NOT FOUND;
        RAISE NOTICE 'id: %, name: %', row.column1, row.column2;
END LOOP;
CLOSE cur;
END;
/
call test_exec_open();
call pg_sleep(0.2);
select query, query_plan from statement_history where parent_query_id != 0 order by start_time;
delete from statement_history;

--test for with select
CREATE OR REPLACE PROCEDURE test_exec_fors()
AS
DECLARE
data varchar(100);
BEGIN
FOR data IN SELECT column2 FROM big_table where column1 = 9909 LOOP
    RAISE NOTICE 'column_value: %', data;
END LOOP;
END;
/
call test_exec_fors();
call pg_sleep(0.2);
select query, query_plan from statement_history where parent_query_id != 0 order by start_time;
delete from statement_history;

--test for with a cursor
CREATE OR REPLACE PROCEDURE test_exec_forc()
AS
DECLARE
CURSOR cur_cursor is SELECT column1 FROM big_table where column1 = 9909;
num int;
BEGIN
for num in cur_cursor
    LOOP
        RAISE NOTICE '%',num;
END LOOP;
END;
/
call test_exec_forc();
call pg_sleep(0.2);
select query, query_plan from statement_history where parent_query_id != 0 order by start_time;
delete from statement_history;

--test dynamic plsql
CREATE OR REPLACE PROCEDURE test_exec_dynexecsql(second int)
AS
DECLARE
sql_stmt VARCHAR;
BEGIN
    sql_stmt := 'call pg_sleep('|| second ||')';
EXECUTE sql_stmt;
END;
/
call test_exec_dynexecsql(1);
call pg_sleep(0.2);
select query, query_plan from statement_history where parent_query_id != 0 order by start_time;
delete from statement_history;

--test dynamic fors
CREATE OR REPLACE PROCEDURE test_exec_dynfors()
AS
DECLARE
data varchar(100);
    sql_stmt VARCHAR;
BEGIN
    sql_stmt := 'SELECT column2 FROM big_table where column1 = 9909';
FOR data IN EXECUTE sql_stmt LOOP
    RAISE NOTICE 'column_value: %', data;
END LOOP;
END;
/
call test_exec_dynfors();
call pg_sleep(0.2);
select query, query_plan from statement_history where parent_query_id != 0 order by start_time;
delete from statement_history;


CREATE OR REPLACE PACKAGE test_proc_in_pkg IS
PROCEDURE proc_pkg();
END test_proc_in_pkg;
/


create or replace package body test_proc_in_pkg is
procedure proc_pkg()
is
begin
create table if not exists test1(col1 int);
insert into test1 values(1);
end;
begin
proc_pkg();
end test_proc_in_pkg;
/
select test_proc_in_pkg.proc_pkg();
call pg_sleep(0.2);
select query, query_plan from statement_history where parent_query_id != 0 order by start_time;
delete from statement_history;