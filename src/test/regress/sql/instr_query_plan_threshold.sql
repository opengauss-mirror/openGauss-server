\c postgres;
create schema instr_query_plan_threshold;
set current_schema to instr_query_plan_threshold;
create table query_plan_table(id int, num int);
create unique index idIndex on query_plan_table(id);
insert into query_plan_table select generate_series(1,10000),generate_series(1,10000);
select reset_unique_sql('GLOBAL', 'ALL', 0);

-- full sql
set track_stmt_stat_level = 'L1,OFF';
set log_min_duration_statement = '0ms';
set statement_timeout = '0ms';
delete statement_history;
select t1.id from query_plan_table t1 where t1.id = 1;
select t1.num from query_plan_table t1 where t1.num = 1;
select pg_sleep(1);
-- expect 2 row
select count(*) from statement_history where query like '%from query_plan_table%' and query_plan is not null;

delete statement_history;
select reset_unique_sql('GLOBAL', 'ALL', 0);
set log_min_duration_statement = '10ms';
set statement_timeout = '1100ms';
select t1.id from query_plan_table t1 where t1.id = 1;
select count(t1.num) from query_plan_table t1, query_plan_table t2;
select pg_sleep(1);
-- expect 2 row
select count(*) from statement_history where query like '%from query_plan_table%' and query_plan is not null;

-- slow sql
set track_stmt_stat_level = 'OFF,L1';
set log_min_duration_statement = '0ms';
delete statement_history;
select reset_unique_sql('GLOBAL', 'ALL', 0);
select t1.id from query_plan_table t1 where t1.id = 1;
select t1.num from query_plan_table t1 where t1.num = 1;
select pg_sleep(1);
-- expect 2 row
select count(*) from statement_history where query like '%from query_plan_table%' and query_plan is not null;

set log_min_duration_statement = -1;
delete statement_history;
select reset_unique_sql('GLOBAL', 'ALL', 0);
select t1.id from query_plan_table t1 where t1.id = 1;
select t1.num from query_plan_table t1 where t1.num = 1;
select pg_sleep(1);
-- expect 0 row
select count(*) from statement_history where query like '%from query_plan_table%' and query_plan is not null;

set log_min_duration_statement = '1s';
set statement_timeout = '2s';
delete statement_history;
select reset_unique_sql('GLOBAL', 'ALL', 0);
select t1.id from query_plan_table t1 where t1.id = 1;
select t1.num from query_plan_table t1 where t1.num = 1;
select t1.num, pg_sleep(1) from query_plan_table t1 where t1.num = 1;
select count(t1.num) from query_plan_table t1, query_plan_table t2, query_plan_table t3, query_plan_table t4;
select pg_sleep(1);
-- expect 2 row
select count(*) from statement_history where query like '%from query_plan_table%' and query_plan is not null;

set track_stmt_stat_level = 'OFF,L0';
set log_min_duration_statement = '30min';
set statement_timeout = '0ms';
drop table query_plan_table;
drop schema instr_query_plan_threshold cascade;
\c regression;

