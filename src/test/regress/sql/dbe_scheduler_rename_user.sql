-- create users
create user scheduler_user password 'scheduler_user@123.';

-- grant
select DBE_SCHEDULER.grant_user_authorization('scheduler_user', 'create job');

-- switch role/user and execute job
set role scheduler_user password "scheduler_user@123.";
create table my_tbl_01(tms date, phone text);
select DBE_SCHEDULER.create_job(job_name=>'job_01', job_type=>'PLSQL_BLOCK', job_action=>'insert into my_tbl_01 values (sysdate::date, 13001230123);', start_date=>sysdate, repeat_interval=>'FREQ=MINUTELY;INTERVAL=1', end_date=>sysdate+1,enabled=>true, auto_drop=>false);
select DBE_SCHEDULER.run_job('job_01', false);
select count(*) from pg_job where log_user = 'scheduler_user' and nspname = 'scheduler_user';
select count(*) from pg_job where log_user = priv_user;
select count(*) from pg_job where job_name = 'job_01';

-- alter and rename pg_job user
reset role;
alter user scheduler_user rename to scheduler_new_user;

-- switch new role/user to execute job
set role scheduler_new_user password "scheduler_user@123.";
select count(*) from pg_job where log_user = 'scheduler_new_user' and nspname = 'scheduler_new_user';
select count(*) from pg_job where log_user != priv_user;

-- return and stop job
reset role;
select DBE_SCHEDULER.drop_job('job_01', true);
select count(*) from pg_job where job_name = 'job_01';