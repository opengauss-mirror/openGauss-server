-- create users
create user scheduler_user1 password 'scheduler_user1.';
create user scheduler_user2 password 'scheduler_user2.';

--grant
select DBE_SCHEDULER.grant_user_authorization('scheduler_user1', 'create job');
select DBE_SCHEDULER.grant_user_authorization('scheduler_user1', 'create external job');
select DBE_SCHEDULER.grant_user_authorization('scheduler_user1', 'run external job');
select DBE_SCHEDULER.grant_user_authorization('scheduler_user1', 'execute any program');
select attribute_name, attribute_value from gs_job_attribute;
select DBE_SCHEDULER.revoke_user_authorization('scheduler_user1', 'create job');
select DBE_SCHEDULER.revoke_user_authorization('scheduler_user1', 'create external job');
select DBE_SCHEDULER.revoke_user_authorization('scheduler_user1', 'run external job');
select DBE_SCHEDULER.revoke_user_authorization('scheduler_user1', 'execute any program');
select attribute_name, attribute_value from gs_job_attribute;
select DBE_SCHEDULER.grant_user_authorization('scheduler_user1', 'xxx');

-- no privilege
SET ROLE scheduler_user1 PASSWORD "scheduler_user1.";
select DBE_SCHEDULER.create_credential('cre_1', 'scheduler_user1', ''); -- failed
select DBE_SCHEDULER.create_program('program1', 'STORED_PROCEDURE', 'select pg_sleep(1);', 0, false, 'test'); -- failed
select DBE_SCHEDULER.create_schedule('schedule1', NULL, 'sysdate', NULL, 'test'); -- failed
select DBE_SCHEDULER.create_job(job_name=>'job1', job_type=>'STORED_PROCEDURE', job_action=>'select pg_sleep(1);', enabled=>true, auto_drop=>false); -- failed

RESET ROLE;
select DBE_SCHEDULER.grant_user_authorization('scheduler_user1', 'create job');

-- create job privilege
SET ROLE scheduler_user1 PASSWORD "scheduler_user1.";
select DBE_SCHEDULER.create_program('program1', 'STORED_PROCEDURE', 'select pg_sleep(1);', 0, false, 'test');
select DBE_SCHEDULER.create_schedule('schedule1', NULL, 'sysdate', NULL, 'test');
select DBE_SCHEDULER.create_job(job_name=>'job1', job_type=>'STORED_PROCEDURE', job_action=>'select pg_sleep(1);', enabled=>true, auto_drop=>false);
select DBE_SCHEDULER.create_job(job_name=>'job2', program_name=>'program1');

RESET ROLE;
select count(*) from adm_scheduler_jobs;
SET ROLE scheduler_user1 PASSWORD "scheduler_user1.";

-- create external job privilege
select DBE_SCHEDULER.create_program('program1', 'EXTERNAL_SCRIPT', '/usr/bin/pwd'); -- failed
select DBE_SCHEDULER.create_job(job_name=>'job1', job_type=>'EXTERNAL_SCRIPT', job_action=>'/usr/bin/pwd', enabled=>true, auto_drop=>false); -- failed

RESET ROLE;
select DBE_SCHEDULER.grant_user_authorization('scheduler_user1', 'create external job');

SET ROLE scheduler_user1 PASSWORD "scheduler_user1.";
select DBE_SCHEDULER.create_program('program2', 'EXTERNAL_SCRIPT', '/usr/bin/pwd');
select DBE_SCHEDULER.create_job(job_name=>'job3', job_type=>'EXTERNAL_SCRIPT', job_action=>'/usr/bin/pwd', enabled=>true, auto_drop=>false);

-- cross user
RESET ROLE;
select DBE_SCHEDULER.grant_user_authorization('scheduler_user2', 'create job');

SET ROLE scheduler_user2 PASSWORD "scheduler_user2.";
select DBE_SCHEDULER.create_job(job_name=>'job4', program_name=>'program1');  -- failed

RESET ROLE;
select DBE_SCHEDULER.grant_user_authorization('scheduler_user2', 'execute any program');

SET ROLE scheduler_user2 PASSWORD "scheduler_user2.";
select DBE_SCHEDULER.create_job(job_name=>'job4', program_name=>'program1');
select DBE_SCHEDULER.create_job(job_name=>'job5', program_name=>'program2');  -- failed

RESET ROLE;
select DBE_SCHEDULER.grant_user_authorization('scheduler_user2', 'create external job');

SET ROLE scheduler_user2 PASSWORD "scheduler_user2.";
select DBE_SCHEDULER.create_job(job_name=>'job5', program_name=>'program2');

RESET ROLE;
select count(*) from adm_scheduler_jobs;
SET ROLE scheduler_user2 PASSWORD "scheduler_user2.";

select DBE_SCHEDULER.run_job(job_name=>'job4', use_current_session=>false);
select DBE_SCHEDULER.run_job(job_name=>'job5', use_current_session=>true);  -- failed

RESET ROLE;
select DBE_SCHEDULER.enable('job4');
select enable from pg_job where job_name = 'job4';
select DBE_SCHEDULER.revoke_user_authorization('scheduler_user2', 'execute any program');
select enable from pg_job where job_name = 'job4';

RESET ROLE;
select DBE_SCHEDULER.drop_job('job1', true);
select DBE_SCHEDULER.drop_job('job2', true);
select DBE_SCHEDULER.drop_job('job3', true);
select DBE_SCHEDULER.drop_job('job4', true);
select DBE_SCHEDULER.drop_job('job5', true);
select DBE_SCHEDULER.drop_program('program1', true);
select DBE_SCHEDULER.drop_program('program2', true);
select DBE_SCHEDULER.drop_schedule('schedule1', true);
select DBE_SCHEDULER.revoke_user_authorization('scheduler_user1', 'create job');
select DBE_SCHEDULER.revoke_user_authorization('scheduler_user1', 'create external job');
select DBE_SCHEDULER.revoke_user_authorization('scheduler_user1', 'run external job');
select DBE_SCHEDULER.revoke_user_authorization('scheduler_user1', 'execute any program');
select DBE_SCHEDULER.revoke_user_authorization('scheduler_user2', 'create job');
select DBE_SCHEDULER.revoke_user_authorization('scheduler_user2', 'create external job');
select DBE_SCHEDULER.revoke_user_authorization('scheduler_user2', 'run external job');
select DBE_SCHEDULER.revoke_user_authorization('scheduler_user1', 'execute any program');

-- check object cleanups --
select DBE_SCHEDULER.grant_user_authorization('scheduler_user1', 'create job');
select DBE_SCHEDULER.grant_user_authorization('scheduler_user2', 'execute any program');
drop user scheduler_user1;
drop user scheduler_user2;
select attribute_name, attribute_value from gs_job_attribute; -- empty