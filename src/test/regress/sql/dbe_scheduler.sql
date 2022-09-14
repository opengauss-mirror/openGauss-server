-- check define_program_argument
select DBE_SCHEDULER.create_program('program1', 'STORED_PROCEDURE', 'select pg_sleep(1);', 3, false, 'test');
select * from gs_job_attribute where attribute_name <> 'owner';
select what, job_name from pg_job_proc;
select DBE_SCHEDULER.define_program_argument('program1', 0, 'arg0', 'int', 16);
select DBE_SCHEDULER.define_program_argument('program1', 4, 'arg4', 'int', 16); 
select DBE_SCHEDULER.define_program_argument('program2', 1, 'arg1', 'boolean', false); 
select DBE_SCHEDULER.define_program_argument('program1', 1, 'arg1', 'boolean', false); 
select * from gs_job_argument;
select DBE_SCHEDULER.define_program_argument('program1', 1, 'arg1', 'int', 16); 
select * from gs_job_argument;
select DBE_SCHEDULER.define_program_argument('program1', 2, 'arg2', 'boolean', 'false', false); 
select * from gs_job_argument;
select DBE_SCHEDULER.drop_program('program1', true);
select * from gs_job_attribute where attribute_name <> 'owner';
select * from gs_job_argument;
select dbname, node_name, interval, nspname, job_name, end_date, enable from pg_job where start_date is not null;
select what, job_name from pg_job_proc;

-- check create_program / drop_program
select DBE_SCHEDULER.create_program('program1', 'STORED_PROCEDURE', 'select pg_sleep(1);', 3, false, 'test');
select DBE_SCHEDULER.create_program('program2', 'sql', 'select pg_sleep(1);', 3, false, 'test');
select DBE_SCHEDULER.create_program('program2', 'STORED_PROCEDURE', 'select pg_sleep(1);', 3, false, 'test');
select DBE_SCHEDULER.create_job('job1', 'program1', '2021-07-20',  'interval ''3 minute''', '2121-07-20', 'DEFAULT_JOB_CLASS', false, false,'test', 'style', NULL, NULL);
select what, job_name from pg_job_proc;
select * from gs_job_attribute where attribute_name <> 'owner';
select DBE_SCHEDULER.drop_program('program2,program1', false);
select * from gs_job_attribute where attribute_name <> 'owner';
select DBE_SCHEDULER.drop_program('program1,program2', true);
select * from gs_job_attribute where attribute_name <> 'owner';
select DBE_SCHEDULER.drop_job('program1,job1', true, false, 'STOP_ON_FIRST_ERROR');
select DBE_SCHEDULER.drop_job('job1,program1', true, false, 'TRANSACTIONAL');
select * from gs_job_attribute where attribute_name <> 'owner';
select DBE_SCHEDULER.drop_job('job1,program1', true, false, 'ABSORB_ERRORS');
select * from gs_job_attribute where attribute_name <> 'owner';
select * from gs_job_argument;
select dbname, node_name, interval, nspname, job_name, end_date, enable from pg_job where start_date is not null;
select what, job_name from pg_job_proc;

-- set_attribute
--program
select DBE_SCHEDULER.create_program('program1', 'STORED_PROCEDURE', 'select pg_sleep(1);', 3, false, 'test');
select DBE_SCHEDULER.set_attribute('program1', 'number_of_argument', '2', NULL);
select DBE_SCHEDULER.set_attribute('program1', 'number_of_arguments', '2', NULL);
select * from gs_job_attribute where attribute_name <> 'owner';
select DBE_SCHEDULER.set_attribute('program1', 'number_of_arguments', 3, NULL);
select * from gs_job_attribute where attribute_name <> 'owner';
select DBE_SCHEDULER.set_attribute('program1', 'enabled', true);
select * from gs_job_attribute where attribute_name <> 'owner';
select DBE_SCHEDULER.set_attribute('program1', 'number_of_arguments', 0, NULL);
select DBE_SCHEDULER.set_attribute('program1', 'enabled', true);
select * from gs_job_attribute where attribute_name <> 'owner';
select DBE_SCHEDULER.create_schedule('schedule1', NULL, 'sysdate', NULL, 'test');
select DBE_SCHEDULER.create_job(job_name=>'job1', program_name=>'program1', schedule_name=>'schedule1');
select what, job_name from pg_job_proc;
select DBE_SCHEDULER.set_attribute('program1', 'program_action', 'create role r1 password ''12345'';', NULL); -- failed
select DBE_SCHEDULER.set_attribute('program1', 'program_action', 'select pg_sleep(2);', NULL);
select what, job_name from pg_job_proc;
--schedule
select * from gs_job_attribute where attribute_name <> 'owner';
select dbname, node_name, interval, nspname, job_name, end_date, enable from pg_job where start_date is not null;
select DBE_SCHEDULER.set_attribute('schedule1', 'start_date', '2021-7-20');
select DBE_SCHEDULER.set_attribute('schedule1', 'end_date', '2021-7-20');
select DBE_SCHEDULER.set_attribute('schedule1', 'repeat_interval', 'interval ''2000 s''');
select * from gs_job_attribute where attribute_name <> 'owner';
select dbname, node_name, interval, nspname, job_name, end_date, enable from pg_job where start_date is not null;
--job
select DBE_SCHEDULER.create_program('program2', 'STORED_PROCEDURE', 'select pg_sleep(1);', 3, false, 'test');
select DBE_SCHEDULER.create_schedule('schedule2', NULL, 'sysdate', NULL, 'test');
select DBE_SCHEDULER.set_attribute('job1', 'program_name', 'program2');
select DBE_SCHEDULER.set_attribute('job1', 'schedule_name', 'schedule2');
select DBE_SCHEDULER.set_attribute('job1', 'job_class', 'unknown');
select DBE_SCHEDULER.create_job_class('test');
select DBE_SCHEDULER.set_attribute('job1', 'job_class', 'test');
select DBE_SCHEDULER.set_attribute('job1', 'enabled', true);
select * from gs_job_attribute where attribute_name <> 'owner';
select dbname, node_name, interval, nspname, job_name, end_date, enable from pg_job where start_date is not null;
select what, job_name from pg_job_proc;
select DBE_SCHEDULER.drop_program('program1,program2', true);
select DBE_SCHEDULER.drop_job('job1', true, false, 'STOP_ON_FIRST_ERROR');
select DBE_SCHEDULER.drop_schedule('schedule1,schedule2', false);
select DBE_SCHEDULER.drop_job_class('test');
select DBE_SCHEDULER.create_job(job_name=>'job1', job_type=>'STORED_PROCEDURE', job_action=>'select pg_sleep(1);');
select * from gs_job_attribute where attribute_name <> 'owner';
select dbname, node_name, interval, nspname, job_name, end_date, enable from pg_job where start_date is not null;
select what, job_name from pg_job_proc;
select * from gs_job_attribute where attribute_name <> 'owner';
select DBE_SCHEDULER.set_attribute('job1', 'start_date', '2021-7-20');
select DBE_SCHEDULER.set_attribute('job1', 'end_date', '2021-7-20');
select DBE_SCHEDULER.set_attribute('job1', 'repeat_interval', 'interval ''2000 s''');
select DBE_SCHEDULER.set_attribute('job1', 'number_of_arguments', 2);
select DBE_SCHEDULER.set_attribute('job1', 'job_action', 'create role r1 password ''12345'';'); -- failed
select DBE_SCHEDULER.set_attribute('job1', 'job_action', 'select pg_sleep(2);');
select DBE_SCHEDULER.set_attribute('job1', 'job_type', 'STORED_PROCEDURE');
select * from gs_job_attribute where attribute_name <> 'owner';
select dbname, node_name, interval, nspname, job_name, end_date, enable from pg_job where start_date is not null;
select what, job_name from pg_job_proc;
select DBE_SCHEDULER.drop_job('job1', true, false, 'STOP_ON_FIRST_ERROR');

--create_schedule dropxxx
select DBE_SCHEDULER.create_schedule('schedule1', NULL, 'sysdate', NULL, 'test');
select * from gs_job_attribute where attribute_name <> 'owner';
select DBE_SCHEDULER.create_schedule('schedule2', NULL, 'sysdate', NULL, 'test');
select DBE_SCHEDULER.create_job('job1', 'schedule1', 'STORED_PROCEDURE', 'select pg_sleep(1);', 0, 'DEFAULT_JOB_CLASS', true, true, NULL, NULL, NULL);
select * from gs_job_attribute where attribute_name <> 'owner';
select * from gs_job_argument;
select dbname, node_name, interval, nspname, job_name, end_date, enable from pg_job where start_date is not null;
select what, job_name from pg_job_proc;
select DBE_SCHEDULER.drop_job('schedule1', true, false, 'STOP_ON_FIRST_ERROR');
select DBE_SCHEDULER.drop_program('schedule1', false);
select DBE_SCHEDULER.drop_schedule('schedule1', false);
select DBE_SCHEDULER.drop_schedule('schedule1,schedule2', true);
select * from gs_job_attribute where attribute_name <> 'owner';
select DBE_SCHEDULER.drop_job('job1', true, false, 'STOP_ON_FIRST_ERROR');
select * from gs_job_attribute where attribute_name <> 'owner';
select * from gs_job_argument;
select dbname, node_name, interval, nspname, job_name, end_date, enable from pg_job where start_date is not null;
select what, job_name from pg_job_proc;

--set_job_arguemnt_value
select DBE_SCHEDULER.create_program('program1', 'STORED_PROCEDURE', 'select pg_sleep(1);', 3, false, 'test');
select DBE_SCHEDULER.create_job('job1', 'program1', '2021-07-20', 'sysdate', '2121-07-20', 'DEFAULT_JOB_CLASS', false, false,'test', 'style', NULL, NULL);
select * from gs_job_attribute where attribute_name <> 'owner';
select * from gs_job_argument;
select dbname, node_name, interval, nspname, job_name, end_date, enable from pg_job where start_date is not null;
select what, job_name from pg_job_proc;
select DBE_SCHEDULER.set_job_argument_value('job1', 1, 1);
select * from gs_job_argument;
select DBE_SCHEDULER.set_job_argument_value('job1', 1, 11);
select * from gs_job_argument;
select DBE_SCHEDULER.set_job_argument_value('job1', 'default_name_of_arg1', 111);
select * from gs_job_argument;
select DBE_SCHEDULER.define_program_argument('program1', 2, 'arg2', 'boolean', 'false', false); 
select * from gs_job_argument;
select DBE_SCHEDULER.set_job_argument_value('job1', 'arg2', 2);
select * from gs_job_argument;
select DBE_SCHEDULER.set_job_argument_value('job1', 'arg2', 22);
select * from gs_job_argument;
select DBE_SCHEDULER.drop_job('job1', true, false, 'STOP_ON_FIRST_ERROR');
select DBE_SCHEDULER.drop_program('program1', false);
select * from gs_job_attribute where attribute_name <> 'owner';
select * from gs_job_argument;
select dbname, node_name, interval, nspname, job_name, end_date, enable from pg_job where start_date is not null;
select what, job_name from pg_job_proc;

-- check create_job
select DBE_SCHEDULER.create_schedule('schedule1', NULL, 'sysdate', '2021-7-28', 'test');
select DBE_SCHEDULER.create_program('program1', 'STORED_PROCEDURE', 'select pg_sleep(1);', 3, false, 'test');
select DBE_SCHEDULER.create_job(job_name=>'job1', job_type=>'STORED_PROCEDURE', job_action=>'select pg_sleep(1);');
select * from gs_job_attribute where attribute_name <> 'owner';
select dbname, node_name, interval, nspname, job_name, end_date, enable from pg_job where start_date is not null;
select what, job_name from pg_job_proc;
select DBE_SCHEDULER.drop_job('job1', true, false, 'STOP_ON_FIRST_ERROR');
select DBE_SCHEDULER.create_job(job_name=>'job2', program_name=>'program1', schedule_name=>'schedule1');
select * from gs_job_attribute where attribute_name <> 'owner';
select dbname, node_name, interval, nspname, job_name, end_date, enable from pg_job where start_date is not null;
select what, job_name from pg_job_proc;
select DBE_SCHEDULER.drop_job('job2', true, false, 'STOP_ON_FIRST_ERROR');
select DBE_SCHEDULER.create_job(job_name=>'job3', program_name=>'program1');
select job_name, enable from pg_job where job_name = 'job3';
select * from gs_job_attribute where attribute_name <> 'owner';
select dbname, node_name, interval, nspname, job_name, end_date, enable from pg_job where start_date is not null;
select what, job_name from pg_job_proc;
select DBE_SCHEDULER.drop_job('job3', true, false, 'STOP_ON_FIRST_ERROR');
select DBE_SCHEDULER.create_job(job_name=>'job4', schedule_name=>'schedule1', job_type=>'STORED_PROCEDURE', job_action=>'select pg_sleep(4);');
select * from gs_job_attribute where attribute_name <> 'owner';
select dbname, node_name, interval, nspname, job_name, end_date, enable from pg_job where start_date is not null;
select what, job_name from pg_job_proc;
select DBE_SCHEDULER.drop_job('job4', true, false, 'STOP_ON_FIRST_ERROR');
select DBE_SCHEDULER.drop_schedule('schedule1', true);
select DBE_SCHEDULER.drop_program('program1', false);
select * from gs_job_attribute where attribute_name <> 'owner';
select * from gs_job_argument;
select dbname, node_name, interval, nspname, job_name, end_date, enable from pg_job where start_date is not null;
select what, job_name from pg_job_proc;

-- enable/disable
select DBE_SCHEDULER.create_program('program1', 'STORED_PROCEDURE', 'select pg_sleep(1);', 0, false, 'test');
select * from gs_job_attribute where attribute_name <> 'owner';
select DBE_SCHEDULER.enable('program1', 'STOP_ON_FIRST_ERROR');
select * from gs_job_attribute where attribute_name <> 'owner';
select DBE_SCHEDULER.disable('program1', false, 'STOP_ON_FIRST_ERROR');
select * from gs_job_attribute where attribute_name <> 'owner';
select DBE_SCHEDULER.create_job(job_name=>'job1', job_type=>'STORED_PROCEDURE', job_action=>'select pg_sleep(1);');
select * from gs_job_attribute where attribute_name <> 'owner';
select DBE_SCHEDULER.enable('job1', 'STOP_ON_FIRST_ERROR');
select * from gs_job_attribute where attribute_name <> 'owner';
select DBE_SCHEDULER.disable('job1', false, 'STOP_ON_FIRST_ERROR');
select * from gs_job_attribute where attribute_name <> 'owner';
select DBE_SCHEDULER.drop_job('job1', true, false, 'STOP_ON_FIRST_ERROR');
select DBE_SCHEDULER.drop_program('program1', false);
select * from gs_job_attribute where attribute_name <> 'owner';
select * from gs_job_argument;
select dbname, node_name, interval, nspname, job_name, end_date, enable from pg_job where start_date is not null;
select what, job_name from pg_job_proc;

--create / drop job_class
select DBE_SCHEDULER.create_job_class('test');
select DBE_SCHEDULER.create_job(job_name=>'job1', job_type=>'STORED_PROCEDURE', job_action=>'select pg_sleep(1);', job_class=>'test');
select DBE_SCHEDULER.create_job(job_name=>'job3', job_type=>'STORED_PROCEDURE', job_action=>'select pg_sleep(1);', job_class=>'testxxx');
select DBE_SCHEDULER.create_job(job_name=>'job2', job_type=>'STORED_PROCEDURE', job_action=>'select pg_sleep(1);');
select * from gs_job_attribute where attribute_name <> 'owner';
select DBE_SCHEDULER.set_attribute('job2', 'job_class', 'test');
select * from gs_job_attribute where attribute_name <> 'owner';
select DBE_SCHEDULER.drop_job_class('test', false);
select DBE_SCHEDULER.drop_job_class('test', true);
select * from gs_job_attribute where attribute_name <> 'owner';
select DBE_SCHEDULER.drop_job('job1,job2', true, false, 'STOP_ON_FIRST_ERROR');
select * from gs_job_attribute where attribute_name <> 'owner';
select * from gs_job_argument;
select dbname, node_name, interval, nspname, job_name, end_date, enable from pg_job where start_date is not null;
select what, job_name from pg_job_proc;

-- generate_job_name
select DBE_SCHEDULER.generate_job_name();
select DBE_SCHEDULER.generate_job_name('');
select DBE_SCHEDULER.generate_job_name('job_prefix_');

create table t1(c1 int);
create or replace procedure p1(id int) as
begin
  insert into t1 values(id);
end;
/
select DBE_SCHEDULER.create_program('program2', 'STORED_PROCEDURE', 'public.p1', 1, true, 'test');
select DBE_SCHEDULER.create_program('program2', 'STORED_PROCEDURE', 'public.p1', 1, false, 'test');
select DBE_SCHEDULER.define_program_argument('program2', 1, 'arg1', 'int', 2);
select DBE_SCHEDULER.enable('program2', 'TRANSACTIONAL');
select DBE_SCHEDULER.create_job(job_name=>'job2', program_name=>'program2', enabled=>true, auto_drop=>false);
select DBE_SCHEDULER.run_job('job2', false);
select pg_sleep(2);
select * from t1;
select DBE_SCHEDULER.create_job(job_name=>'job3', job_type=>'PLSQL_BLOCK', job_action=>'insert into public.t1 values(3);', enabled=>true, auto_drop=>false);
select DBE_SCHEDULER.run_job('job3', false);
select pg_sleep(2);
select * from t1;
drop table t1;

select DBE_SCHEDULER.drop_job('job2', true);
select DBE_SCHEDULER.drop_job('job3', true);
select DBE_SCHEDULER.drop_program('program2', true);

-- others
select DBE_SCHEDULER.create_program('program1', 'STORED_PROCEDURE', 'select pg_sleep(1);', 1, false, 'test');
select DBE_SCHEDULER.enable('program1', 'TRANSACTIONAL');
select DBE_SCHEDULER.define_program_argument('program1', 1, 'arg1', 'int', 16);
select DBE_SCHEDULER.enable('program1', 'TRANSACTIONAL');
select * from gs_job_attribute where attribute_name <> 'owner';
select DBE_SCHEDULER.set_attribute('program1', 'program_type', 'PLSQL_BLOCK');
select DBE_SCHEDULER.set_attribute('program1', 'number_of_arguments', 0);
select DBE_SCHEDULER.set_attribute('program1', 'number_of_arguments', 1);
select DBE_SCHEDULER.set_attribute('program1', 'program_type', 'STORED_PROCEDURE');
select DBE_SCHEDULER.set_attribute('program1', 'number_of_arguments', -1);
select DBE_SCHEDULER.set_attribute('program1', 'number_of_arguments', 0);
select DBE_SCHEDULER.set_attribute('program1', 'number_of_arguments', 1);
select DBE_SCHEDULER.set_attribute('program1', 'number_of_arguments', 255);
select DBE_SCHEDULER.set_attribute('program1', 'number_of_arguments', 256);

select DBE_SCHEDULER.drop_program('program1', true);
select * from gs_job_attribute where attribute_name <> 'owner'; -- empty


select DBE_SCHEDULER.create_program('programdb1', 'PLSQL_BLOCK', 'select pg_sleep(1);', 0, false, 'test');
select DBE_SCHEDULER.create_job('jobdb1', 'programdb1', '2021-07-20', 'sysdate', '2121-07-20', 'DEFAULT_JOB_CLASS', false, false,'test', 'style', NULL, NULL);
select dbname, node_name, interval, nspname, job_name, end_date, enable from pg_job;
create database test11;
\c test11
select DBE_SCHEDULER.create_program('programdb1', 'PLSQL_BLOCK', 'select pg_sleep(1);', 0, false, 'test');
select DBE_SCHEDULER.create_job('jobdb1', 'programdb1', '2021-07-20', 'sysdate', '2121-07-20', 'DEFAULT_JOB_CLASS', false, false,'test', 'style', NULL, NULL);
select dbname, node_name, interval, nspname, job_name, end_date, enable from pg_job;
select dbe_scheduler.run_job('jobdb1', false);
select dbe_scheduler.drop_job('jobdb1');
select dbname, node_name, interval, nspname, job_name, end_date, enable from pg_job;
\c regression
select dbe_scheduler.drop_job('jobdb1');
select DBE_SCHEDULER.drop_program('programdb1', true);
select dbname, node_name, interval, nspname, job_name, end_date, enable from pg_job;
select * from gs_job_attribute where attribute_name <> 'owner';

select DBE_SCHEDULER.create_credential('cre_1', 'scheduler_user1', '''''passwd''');