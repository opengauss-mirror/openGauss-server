----------------------------------------------------------
-- The common user should not have the permission.
---------------------------------------------------------

DROP ROLE IF EXISTS alter_system_testuser;
CREATE USER alter_system_testuser PASSWORD 'test@1233';

SET SESSION AUTHORIZATION alter_system_testuser PASSWORD 'test@1233';
show use_workload_manager;
ALTER SYSTEM SET use_workload_manager to off;
show io_limits;
ALTER SYSTEM SET io_limits to 100;
select pg_sleep(1); -- wait to reload postgres.conf file
show use_workload_manager;
show io_limits;

\c
DROP user alter_system_testuser;

-------------------------------------------------------
-- clear pg_query_audit for the last test
-------------------------------------------------------
show audit_enabled;
show audit_set_parameter;
show audit_user_violation;
SELECT pg_delete_audit('2000-01-01 ','9999-01-01');


--------------------------------------------------------
-- for POSTMASTER GUC
--------------------------------------------------------
SHOW enable_thread_pool;
ALTER SYSTEM SET enable_thread_pool to on;
ALTER SYSTEM SET enable_thread_pool to off;

------------------------------------------------------
-- for SIGHUP GUC
------------------------------------------------------
-- change
SHOW password_lock_time;
ALTER SYSTEM SET  password_lock_time to 1.1;
SHOW autovacuum;
ALTER SYSTEM SET autovacuum to off;
SHOW log_destination;
ALTER SYSTEM SET log_destination to 'stderr,csvlog';
SHOW autovacuum_mode;
ALTER SYSTEM SET autovacuum_mode to 'analyze';
SHOW parctl_min_cost;
ALTER SYSTEM SET parctl_min_cost TO 1000;


select pg_sleep(2);  -- wait to reload postgres.conf file


-- check result and change back.
SHOW password_lock_time;
ALTER SYSTEM SET  password_lock_time to 1;
SHOW autovacuum;
ALTER SYSTEM SET autovacuum to on;
SHOW log_destination;
ALTER SYSTEM SET log_destination to 'stderr';
SHOW autovacuum_mode;
ALTER SYSTEM SET autovacuum_mode to mix;
SHOW parctl_min_cost;
ALTER SYSTEM SET parctl_min_cost TO 100000;

select pg_sleep(2);  -- wait to reload postgres.conf file

SHOW password_lock_time;
SHOW autovacuum;
SHOW log_destination;
SHOW autovacuum_mode;
SHOW parctl_min_cost;



-- some err case
ALTER SYSTEM SET password_lock_time to true;
ALTER SYSTEM SET autovacuum to 'lalala';
ALTER SYSTEM SET log_destination to 'abcdefg';
ALTER SYSTEM SET autovacuum_mode to 123;
ALTER SYSTEM SET autovacuum_mode to lalala;
ALTER SYSTEM SET parctl_min_cost TO -100;
ALTER SYSTEM SET parctl_min_cost TO 1.1;

select pg_sleep(2);  -- wait to reload postgres.conf file

SHOW password_lock_time;
SHOW autovacuum;
SHOW log_destination;
SHOW autovacuum_mode;
SHOW parctl_min_cost;

------------------------------------------------------
-- FOR BACKEND GUC
------------------------------------------------------
show ignore_system_indexes;
ALTER SYSTEM SET ignore_system_indexes TO on;
show ignore_system_indexes;
ALTER SYSTEM SET ignore_system_indexes TO off;
show ignore_system_indexes;


----------------------------------------------------
-- for USERSET GUC
----------------------------------------------------
show io_limits;
ALTER SYSTEM SET io_limits to 100;
show io_limits;
ALTER SYSTEM SET io_limits to 0;
show io_limits;


-----------------------------------------------------
-- for SUSET GUC
----------------------------------------------------
show autoanalyze;
ALTER SYSTEM SET autoanalyze to on;
show autoanalyze;
ALTER SYSTEM SET autoanalyze to off;
show autoanalyze;


-----------------------------------------------------
-- UNSUPPORT SET TO DEFAULT
-----------------------------------------------------
SHOW parctl_min_cost;
ALTER SYSTEM SET parctl_min_cost TO 1000;
ALTER SYSTEM SET parctl_min_cost TO default;
select pg_sleep(1);
SHOW parctl_min_cost;
ALTER SYSTEM SET parctl_min_cost TO 100000;

-------------------------------------------------------
-- can not in a transaction
-------------------------------------------------------
BEGIN;
SHOW autovacuum;
ALTER SYSTEM SET autovacuum to off;
SHOW autovacuum;
ALTER SYSTEM SET autovacuum to on;
SHOW autovacuum;
END;

-------------------------------------------------------
-- shoule be audited.
------------------------------------------------------
SELECT type,result,userid,database,client_conninfo,object_name,detail_info FROM pg_query_audit('2000-01-01 08:00:00','9999-01-01 08:00:00');

