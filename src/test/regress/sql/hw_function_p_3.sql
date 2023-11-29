create schema hw_function_p_3;
set search_path to hw_function_p_3;

--pg_get_functiondef
select * from pg_get_functiondef((select oid from pg_proc where proname='pg_table_size'));

--pg_stat_get_status, just for llt coverage
select thread_name from pg_stat_get_status(pg_backend_pid());
select thread_name from pg_stat_get_status(99001);  --invalid thread id
create user jack with password 'ttest@123';
set role jack password 'ttest@123';
select count(*) from pg_stat_get_thread();
reset role;
drop user jack;

--pg_stat_get_backend_client_addr pg_stat_get_backend_client_port, just for llt coverage
select * from pg_stat_get_backend_client_addr(1);
select * from pg_stat_get_backend_client_port(1);

create table t1(a int, b int, c int);
select pg_table_size('t1');

--test bug for track function
CREATE OR REPLACE FUNCTION test_func(a INT) RETURNS INT AS $$
BEGIN
    RETURN 10;
END;
$$ LANGUAGE plpgsql;

\c
set search_path to hw_function_p_3;
SET track_functions = 'all';

BEGIN;
SELECT pg_stat_get_mem_mbytes_reserved(10);
SELECT test_func(10);
COMMIT;
SELECT test_func(10);
SELECT test_func(10);
reset track_functions;
drop function test_func;

--drop data
drop schema hw_function_p_3 cascade;