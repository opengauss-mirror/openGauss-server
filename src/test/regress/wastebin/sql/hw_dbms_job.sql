--
-- SIMULATE A DB'S TIME EXPRESSION
--
set datestyle = 'iso, mdy';
--timestamp plus numeric
SELECT TIMESTAMP '4012-12-24 12:00:00' + 1.5 AS cal_result;
SELECT TIMESTAMP '4012-12-24 12:00:00' + 1.0/24 AS cal_result;
SELECT TIMESTAMP '4012-12-24 12:00:00' + 1.0/1440 AS cal_result;
SELECT TIMESTAMP '4012-12-24 12:00:00' + 1.0/24 + 30.0/1440 AS cal_result;
select TIMESTAMP '4012-12-24 12:00:00' + 1.0/24 AS cal_reault;
SELECT TIMESTAMP '4012-12-24 12:00:00' + 233.0/24 AS cal_result;
SELECT TIMESTAMP '4012-12-24 12:00:00' + 222222233333.0/24 AS cal_result;

--date_trunc(fmt, timestamp)
select date_trunc('Y', timestamp '4012-12-24 12:12:12') AS cal_result;
select date_trunc('D', timestamp '4012-12-24 12:12:12') AS cal_result;
select date_trunc('H', timestamp '4012-12-24 12:12:12') AS cal_result;
select date_trunc('M', timestamp '4012-12-24 12:12:12') AS cal_result;
select date_trunc('S', timestamp '4012-12-24 12:12:12') AS cal_result; 
select date_trunc('MMMMYYY', timestamp '4012-12-24 12:12:12') AS cal_result;

--TIMESTAMP_MI_NUMERIC
SELECT TIMESTAMP '4012-12-24 12:00:00' - 1.5 AS cal_result;
SELECT TIMESTAMP '4012-12-24 12:00:00' - 1.0/24 AS cal_result;
SELECT TIMESTAMP '4012-12-24 12:00:00' - 1.0/1440 AS cal_result;
SELECT TIMESTAMP '4012-12-24 12:00:00' - 1.0/24 + 30.0/1440 AS cal_result;
select timestamp '4012-12-24 12:00:00' - 1.0/24 - 1.0/24 AS cal_result;

--add_months(timestamp, int)	
select add_months(to_date('2017-2-29', 'yyyy-mm-dd'), 13) from dual;
select add_months(to_date('2017-5-29', 'yyyy-mm-dd'), 11) from dual;
select add_months(to_date('2017-5-13', 'yyyy-mm-dd'), 11) from dual;
select add_months(to_date('2017-5-29', 'yyyy-mm-dd'), -11) from dual; 
select add_months(to_date('2017-5-29', 'yyyy-mm-dd'), -13) from dual;
select add_months(to_date('2017-5-13', 'yyyy-mm-dd'), -11) from dual;
select add_months(to_date('2017-5-13', 'yyyy-mm-dd'), 0) from dual; 
select add_months(to_date('2017-5-29', 'yyyy-mm-dd'),9999999) from dual;
select add_months(to_date('2017-5-29', 'yyyy-mm-dd'),-9999999) from dual;

--last_day
select last_day(to_date('2017-01-01', 'YYYY-MM-DD')) AS cal_result;
select last_day(to_date('2017-04-01', 'YYYY-MM-DD')) AS cal_result;
select last_day(to_date('2017-02-01', 'YYYY-MM-DD')) AS cal_result;
select last_day(to_date('2017-01-01', 'YYYY-MM-DD')) AS cal_result;
select last_day(to_date('2017-04-01', 'YYYY-MM-DD')) AS cal_result;
select last_day(to_date('2017-02-01', 'YYYY-MM-DD')) AS cal_result;
select last_day() AS cal_result;
select last_day(to_date('200021')) AS cal_result;

--next_day
select next_day(timestamp '2017-05-25 00:00:00','Sunday')AS cal_result;
select next_day(timestamp '2017-05-25 00:00:00','Sun')AS cal_result;
select next_day(timestamp '2017-05-25 00:00:00',1)AS cal_result;
select next_day(timestamp '2017-05-25 00:00:00','Monday')AS cal_result;
select next_day(timestamp '2017-05-25 00:00:00','mon')AS cal_result;
select next_day(timestamp '2017-05-25 00:00:00',2)AS cal_result;
select next_day(timestamp '2017-05-25 00:00:00','tuesday')AS cal_result;
select next_day(timestamp '2017-05-25 00:00:00','tue')AS cal_result;
select next_day(timestamp '2017-05-25 00:00:00',3)AS cal_result;
select next_day(timestamp '2017-05-25 00:00:00','wednesday')AS cal_result;
select next_day(timestamp '2017-05-25 00:00:00','wed')AS cal_result;
select next_day(timestamp '2017-05-25 00:00:00',4)AS cal_result;
select next_day(timestamp '2017-05-25 00:00:00','thursday')AS cal_result;
select next_day(timestamp '2017-05-25 00:00:00','thu')AS cal_result;
select next_day(timestamp '2017-05-25 00:00:00',5)AS cal_result;
select next_day(timestamp '2017-05-25 00:00:00','friday')AS cal_result;
select next_day(timestamp '2017-05-25 00:00:00','fri')AS cal_result;
select next_day(timestamp '2017-05-25 00:00:00',6)AS cal_result;
select next_day(timestamp '2017-05-25 00:00:00','saturday')AS cal_result;
select next_day(timestamp '2017-05-25 00:00:00','sat')AS cal_result;
select next_day(timestamp '2017-05-25 00:00:00',7)AS cal_result;
select next_day(timestamp '2017-05-25 00:00:00',8)AS cal_result;
select next_day(timestamp '2017-05-25 00:00:00','svn')AS cal_result;
select next_day(timestamp '2017-05-25 00:00:00',1.1)AS cal_result;
select next_day(timestamp '2017-05-25 00:00:00',10)AS cal_result;
select next_day(timestamp '2017-05-25 00:00:00',-1)AS cal_result;
select next_day(timestamp '2017-05-25 00:00:00','sundau')AS cal_result;
select next_day(timestamp '2017-05-25 00:00:00','saturday')AS cal_result;


--
-- simulate A db's dbms_job
--
create table test(id int, time date);

create or replace function test() returns void 
as $$
DECLARE
	var integer := 0;
	id_set integer := 1;
BEGIN
	SELECT COUNT(*) INTO var FROM test;
	IF var = 0 THEN
		INSERT INTO test VALUES(id_set, TIMESTAMP '4012-12-24 12:00:00');
	ELSE
		SELECT MAX(test.id) INTO id_set FROM test;
		INSERT INTO test VALUES(id_set + 1, TIMESTAMP '4012-12-24 12:00:00');
	END IF;
END;
$$ LANGUAGE plpgsql;

--dbms_job.submit
call dbms_job.submit('call public.test(); ', TIMESTAMP '4012-12-24 12:00:00', 'TIMESTAMP ''4012-12-24 12:00:00'' + 1.0/24', :a);
select * from dbms_job.submit('call public.test(); ', TIMESTAMP '4012-12-24 12:00:00', 'TIMESTAMP ''4012-12-24 12:00:00'' + 2.0/24');
select * from dbms_job.submit('call public.test(); ', TIMESTAMP '4012-12-24 12:00:00');
select * from dbms_job.submit('call public.test(); ', TIMESTAMP '4012-12-24 12:00:00', 'TIMESTAMP ''4012-12-24 12:00:00'' + 1.0/24');
select * from dbms_job.submit('begin public.test(); end;', TIMESTAMP '4012-12-24 12:00:00', 'TIMESTAMP ''4012-12-24 12:00:00'' + 3.0/24');
select * from dbms_job.submit('insert into public.test values(100, TIMESTAMP ''4012-12-24 12:00:00''+1.0/24) ; ', TIMESTAMP '4012-12-24 12:00:00', 'TIMESTAMP ''4012-12-24 12:00:00'' + 4.0/24');
select * from dbms_job.submit('begin public.test();insert into public.test values(102, TIMESTAMP ''4012-12-24 12:00:00''+1.0/24) ; end;', TIMESTAMP '4012-12-24 12:00:00', 'TIMESTAMP ''4012-12-24 12:00:00'' + 5.0/24');
select * from dbms_job.submit('insert into public.test values(103, TIMESTAMP ''4012-12-24 12:00:00''+1.0/24) ; insert into public.test values(104, TIMESTAMP ''4012-12-24 12:00:00''+1.0/24) ;', TIMESTAMP '4012-12-24 12:00:00', 'TIMESTAMP ''4012-12-24 12:00:00'' + 6.0/24');
select * from dbms_job.submit('inset ino public.test values(103, TIMESTAMP ''4012-12-24 12:00:00''+1.0/24) ; ', TIMESTAMP '4012-12-24 12:00:00', 'TIMESTAMP ''4012-12-24 12:00:00'' + 6.0/24'); --what missspelling
select * from dbms_job.submit('insert into publicpubilic.test values(103, TIMESTAMP ''4012-12-24 12:00:00''+1.0/24) ; ', TIMESTAMP '4012-12-24 12:00:00', 'TIMESTAMP ''4012-12-24 12:00:00'' + 6.0/24'); --operating a inexistent table
select * from dbms_job.submit('insert into publicpubilic.test values(103, TIMESTAMP ''4012-12-24 12:00:00''+1.0/24) ; ', TIMESTAMP '4012-12-24 12:00:00', 'TIMESTAMP ''4012-12-24 12:00:00'' + 5.0/24'); --interval misspeling

select count(*) from pg_job order by 1;

select * from test;
select * from dbms_job.submit('call public.test(); ');
--test job_scheduler.c
select pg_sleep(5);
\o /dev/null
select * from test;
select job_id,current_postgres_pid,node_name,job_status from pg_job where job_id=12;

delete from test;
select * from dbms_job.submit('call public.test(); ', sysdate, 'interval ''5 seconds''');
select pg_sleep(14);
select * from test order by id;
drop table test;

select * from dbms_job.submit('', TIMESTAMP '4012-12-24 12:00:00', 'TIMESTAMP ''4012-12-24 12:00:00'' + 2.0/24');
select * from dbms_job.submit('call public.test(); ', '', 'TIMESTAMP ''4012-12-24 12:00:00'' + 2.0/24');
select * from dbms_job.submit('call public.test(); ', TIMESTAMP '4012-12-24 12:00:00', '');
select * from dbms_job.submit('call public.test(); ', TIMESTAMP '4012-12-24 12:00:00', 'interrval ''1''');
select * from dbms_job.submit('call public.test(); ', TIMESTAMP '4012-12-24 12:00:00', 'TIMESTAMP ''2000-12-24 12:00:00''');
select * from dbms_job.submit('call public.test(); ', sysdate, 'sysdate + interval ''5 seconds''');
select pg_sleep(8);
\o

--dbms_job.isubmit
call dbms_job.isubmit(18, 'public.test; ', TIMESTAMP '4012-12-24 12:00:00', 'TIMESTAMP ''4012-12-24 12:00:00'' + 1.0/24');--correct
call dbms_job.isubmit(18, ' call public.test; ', TIMESTAMP '2020-12-24 12:00:00', 'TIMESTAMP ''2020-12-24 12:00:00'' + 1.0/24');--repeat the same job_id
call dbms_job.isubmit(18, '', TIMESTAMP '4012-12-24 12:00:00', 'TIMESTAMP ''4012-12-24 12:00:00'' + 1.0/24');
call dbms_job.isubmit(18, 'public.test; ', '', 'TIMESTAMP ''4012-12-24 12:00:00'' + 1.0/24');
call dbms_job.isubmit(18, 'public.test; ', TIMESTAMP '4012-12-24 12:00:00', '');
call dbms_job.isubmit(19, 'public.test; ', TIMESTAMP '4012-12-24 12:00:00', 'null1');
call dbms_job.isubmit(18, 'public.test; ', sysdate, 'TIMESTAMP ''2000-12-24 12:00:00''');
call dbms_job.isubmit(20, 'public.test; ', TIMESTAMP '4012-12-24 12:00:00', 'sysdate');
call dbms_job.isubmit(-1, 'public.test; ', sysdate, 'Interval ''1 hours'''); 

--dbms_job.remove
call dbms_job.remove(7);
call dbms_job.remove(23);
call dbms_job.remove(-1);

--create table and job
create table test_if(a date);
call dbms_job.isubmit(118,'insert into public.test_if values(sysdate);', TIMESTAMP '2222-11-21 11:11:11', 'TIMESTAMP ''2222-11-21 11:11:11'' + 1.0/24');
select job_id,node_name,start_date,last_start_date,last_end_date,last_suc_date,this_run_date,next_run_date from pg_job where job_id=118;

--dbms_job.broken
call dbms_job.broken(-1,true);
call dbms_job.broken(100,true);
\o /dev/null
call dbms_job.broken(118,true,TIMESTAMP '2222-11-21 11:11:11');
select job_status, next_run_date from pg_job where job_id = 118;
call dbms_job.broken(20,false,TIMESTAMP '2222-11-21 11:11:11');
select job_status, next_run_date from pg_job where job_id = 118;
call dbms_job.broken(118,null,TIMESTAMP '2222-11-21 11:11:11');
select job_status, next_run_date from pg_job where job_id = 118;
call dbms_job.broken(118,true,'');
select job_status, next_run_date from pg_job where job_id = 118;
\o
call dbms_job.broken(118,true);
select job_status, next_run_date from pg_job where job_id = 118;


--dbms_job.change
call dbms_job.change(-1,'insert into public.test_if values(sysdate);',sysdate,'sysdate+1');
call dbms_job.change(100,'insert into public.test_if values(sysdate);',sysdate,'sysdate+1');
call dbms_job.change(118,null,TIMESTAMP '2222-11-21 11:11:11', 'TIMESTAMP ''2222-11-21 11:11:11'' + 1.0/24'); 
select job, what, next_date, interval from user_jobs where job = 118;
call dbms_job.change(118,'call public.test;', null, 'TIMESTAMP ''2211-12-21 12:12:12'' + 1.0/24'); 
select job, what, next_date, interval from user_jobs where job = 118;
call dbms_job.change(118,'insert into public.test_if values(sysdate);',TIMESTAMP '2211-12-21 12:12:12', null); 
select job, what, next_date, interval from user_jobs where job = 118;
call dbms_job.change(118,'insert into public.test_if values(sysdate);',TIMESTAMP '2222-11-21 11:11:11', 'TIMESTAMP ''2222-11-21 11:11:11'' + 1.0/24'); 
select next_run_date from pg_job where job_id = 118;
select what from pg_job_proc where job_id = 118;
select interval from pg_job where job_id = 118;
call dbms_job.change(118,'insert into public.test_if values(sysdate);',TIMESTAMP '2222-11-21 11:11:11', '');
select next_run_date from pg_job where job_id = 118;
select what from pg_job_proc where job_id = 118;
select interval from pg_job where job_id = 118;
call dbms_job.change(118,null,null, 'TIMESTAMP ''2000-11-21 11:11:11''');
call dbms_job.change(118,null,null, 'interrval ''1''');
call dbms_job.change(118,null,null, 'null1');
call dbms_job.change(118,null,null, 'null');
call dbms_job.change(118,null,null,null);
select interval from pg_job where job_id = 118;

--dbms_job.interval
call dbms_job.interval(-1,'sysdate+1');
call dbms_job.interval(100,'sysdate+1');
call dbms_job.interval(118,'TIMESTAMP ''2222-11-21 11:11:11'' + 1.0/24'); 
select interval from pg_job where job_id = 118;
--invalid parameter
call dbms_job.interval(118,'');
select interval from pg_job where job_id = 118;

call dbms_job.interval(118,'TIMESTAMP ''2000-11-21 11:11:11''');
call dbms_job.interval(118,'interrval ''1''');
call dbms_job.interval(118,'null1');
call dbms_job.interval(118,null);
select interval from pg_job where job_id = 118;
call dbms_job.interval(118,'null');
select interval from pg_job where job_id = 118;

--dbms_job.next_date
call dbms_job.next_date(-1,sysdate);
call dbms_job.next_date(100,sysdate);
call dbms_job.next_date(118, '');
call dbms_job.next_date(118,TIMESTAMP '2222-11-21 22:33:33');
select next_run_date from pg_job where job_id = 118;

--dbms_job.what
call dbms_job.what(-1,'insert into public.test_if values(sysdate);');
call dbms_job.what(100,'insert into public.test_if values(sysdate);');
call dbms_job.what(118,'');
call dbms_job.what(118,'insert into public.test_if values(sysdate+5);');
select what from pg_job_proc where job_id = 118;
call dbms_job.what(118,'');
select what from pg_job_proc where job_id = 118;

--clear env
drop table public.test_if;

--S1.DROP TABLE
DROP TABLE IF EXISTS T_JOB_parallel_003;
--S2.CREATE TABLE
CREATE TABLE T_JOB_parallel_003 (n_num integer,v_jname text,d_date timestamp,v_note text) with(orientation=column) distribute by replication;
--S3.CREATE FUNCTION for insert value.
CREATE OR REPLACE PROCEDURE PRC_JOB_parallel_003_1() 
AS
BEGIN
   INSERT INTO T_JOB_parallel_003 VALUES(1,'T_JOB_parallel_003',SYSDATE,'');
   INSERT INTO T_JOB_parallel_003 VALUES(2,'T_JOB_parallel_003',SYSDATE,'');
END;
/
--S6.create proceduer for add job.
CREATE OR REPLACE PROCEDURE PRC_JOB_SUBMIT_11()
AS
N_NUM integer :=1;
BEGIN
 FOR I IN 1..11 LOOP
  perform DBMS_JOB.SUBMIT('call PRC_JOB_parallel_003_1();', sysdate+1, 'null',:a);
 END LOOP;
END;
/
--S7.call function for submit job
CALL PRC_JOB_SUBMIT_11();
select pg_sleep(5);
\o /dev/null
select count(*) from pg_job;
\o
DROP PROCEDURE IF EXISTS PRC_JOB_parallel_003_1;
DROP PROCEDURE IF EXISTS PRC_JOB_SUBMIT_11;
DROP TABLE IF EXISTS T_JOB_parallel_003;

--Test for auth of job
create user test_job_user1 identified by 'AAAaaa123';
create user test_job_user2 identified by 'AAAaaa123';
create database test_job_db;
update pg_job set job_status='d' where job_id=118;
\c test_job_db
select count(*) from pg_job where job_id=118;
call dbms_job.remove(118);
call dbms_job.broken(118,true,TIMESTAMP '2222-11-21 11:11:11');
call dbms_job.change(118,'call public.test();',sysdate,'sysdate+1');
call dbms_job.interval(118,'TIMESTAMP ''2222-11-21 11:11:11'' + 1.0/24'); 
call dbms_job.next_date(118,TIMESTAMP '2222-11-21 11:11:11');
call dbms_job.what(118,'insert into public.test_if values(sysdate);');

set ROLE test_job_user1 PASSWORD 'AAAaaa123';
call dbms_job.isubmit(200, 'call public.test(); ', sysdate);
set ROLE test_job_user2 PASSWORD 'AAAaaa123';
call dbms_job.remove(200);
call dbms_job.broken(200,true,TIMESTAMP '2222-11-21 11:11:11');
call dbms_job.change(200,'call public.test();',sysdate,'sysdate+1');
call dbms_job.interval(200,'TIMESTAMP ''2222-11-21 11:11:11'' + 1.0/24'); 
call dbms_job.next_date(200,TIMESTAMP '2222-11-21 11:11:11');
call dbms_job.what(200,'insert into public.test_if values(sysdate);');

\c regression
select count(*) from pg_job where job_id=200;
drop user test_job_user1 cascade;
select count(*) from pg_job where job_id=200;
drop user test_job_user2 cascade;
drop database test_job_db;
reset datestyle;
