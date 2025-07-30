\c postgres
-- check statement_trace_decode func defination
\sf statement_trace_decode

-- check full sql conatin trace field
\sf DBE_PERF.get_global_full_sql_by_timestamp

-- check slow sql conatin trace field
\sf DBE_PERF.get_global_slow_sql_by_timestamp

-- The structure of function standby_statement_history must be consistent with table statement_history,for trace field.
select
    (select relnatts from pg_class where relname = 'statement_history' limit 1)
    =
    (select array_length(proargnames, 1) - 1 from pg_proc where proname = 'standby_statement_history' order by 1 limit 1)
    as issame;

--test full/slow sql in proc
--generate slow sql
drop table if exists big_table;
CREATE TABLE big_table (
     id SERIAL PRIMARY KEY,
     column1 INT,
     column2 VARCHAR(100)
);

create or replace procedure test_slow_sql()
is
begin
perform 1;
PERFORM pg_sleep(0.1);
end;
/

-- record full sql
set track_stmt_stat_level = 'L1,OFF';
show track_stmt_stat_level;
call test_slow_sql();
select statement_trace_decode(trace) from statement_history limit 0;
delete from statement_history;

-- record slow sql
set log_min_duration_statement = 50;
set track_stmt_stat_level = 'OFF,L1';
show log_min_duration_statement;
show track_stmt_stat_level;
call test_slow_sql();
select statement_trace_decode(trace) from statement_history limit 0;
delete from statement_history;
drop table big_table;
