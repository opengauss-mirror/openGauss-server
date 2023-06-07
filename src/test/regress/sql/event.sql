drop database if exists event_b;
create database event_b with dbcompatibility  'b';
\c event_b
create user event_a sysadmin password 'event_123';
create user event_b sysadmin password 'event_123';
--CREATE EVENT 
--Schedule Parameter Test 
--CHECK Schedule AT .. situation 
create event IF NOT EXISTS ee11 on schedule at '2022-12-09 17:24:11' disable do insert into t values(0);
select pg_sleep(0.2);
select  job_name, nspname from pg_job where dbname='event_b';
drop event if exists ee11;
create event IF NOT EXISTS ee11 on schedule at sysdate disable do insert into t values(0);
select pg_sleep(0.2);
select  job_name, nspname from pg_job where dbname='event_b';
drop event if exists ee11;
create event IF NOT EXISTS ee11 on schedule at CURRENT_DATE disable do insert into t values(0);
select pg_sleep(0.2);
select  job_name, nspname from pg_job where dbname='event_b';
drop event if exists ee11;
create event IF NOT EXISTS ee11 on schedule at CURRENT_TIME disable do insert into t values(0);
select pg_sleep(0.2);
select  job_name, nspname from pg_job where dbname='event_b';
drop event if exists ee11;
create event IF NOT EXISTS ee11 on schedule at CURRENT_TIME (1) disable do insert into t values(0);
select pg_sleep(0.2);
select  job_name, nspname from pg_job where dbname='event_b';
drop event if exists ee11;
create event IF NOT EXISTS ee11 on schedule at CURRENT_TIMESTAMP disable do insert into t values(0);
select pg_sleep(0.2);
select  job_name, nspname from pg_job where dbname='event_b';
drop event if exists ee11;
create event IF NOT EXISTS ee11 on schedule at CURRENT_TIMESTAMP (1) disable do insert into t values(0);
select pg_sleep(0.2);
select  job_name, nspname from pg_job where dbname='event_b';
drop event if exists ee11;
create event IF NOT EXISTS ee11 on schedule at LOCALTIME disable do insert into t values(0);
select pg_sleep(0.2);
select  job_name, nspname from pg_job where dbname='event_b';
drop event if exists ee11;
create event IF NOT EXISTS ee11 on schedule at LOCALTIME (1) disable do insert into t values(0);
select pg_sleep(0.2);
select  job_name, nspname from pg_job where dbname='event_b';
drop event if exists ee11;
create event IF NOT EXISTS ee11 on schedule at LOCALTIMESTAMP disable do insert into t values(0);
select pg_sleep(0.2);
select  job_name, nspname from pg_job where dbname='event_b';
drop event if exists ee11;
create event IF NOT EXISTS ee11 on schedule at LOCALTIMESTAMP (1) disable do insert into t values(0);
select pg_sleep(0.2);
select  job_name, nspname from pg_job where dbname='event_b';
drop event if exists ee11;
create event IF NOT EXISTS ee11 on schedule at LOCALTIMESTAMP (1) disable do insert into t values(0);
select pg_sleep(0.2);
select  job_name, nspname from pg_job where dbname='event_b';
drop event if exists ee11;
create event IF NOT EXISTS ee11 on schedule at now() disable do insert into t values(0);
select pg_sleep(0.2);
select  job_name, nspname from pg_job where dbname='event_b';
drop event if exists ee11;
create event IF NOT EXISTS ee11 on schedule at CURRENT_TIMESTAMP + interval 1 minute disable do insert into t values(0);
select pg_sleep(0.2);
select  job_name, nspname from pg_job where dbname='event_b';
drop event if exists ee11;
create event IF NOT EXISTS ee11 on schedule at SYSDATE + interval 10 second disable do insert into t values(0);
select pg_sleep(0.2);
select  job_name, nspname from pg_job where dbname='event_b';
drop event if exists ee11;
create event IF NOT EXISTS ee11 on schedule at SYSDATE + interval 0.5 second disable do insert into t values(0);
select pg_sleep(0.2);
select  job_name, nspname from pg_job where dbname='event_b';
drop event if exists ee11;
create event IF NOT EXISTS ee11 on schedule at now() + interval 1 hour disable do insert into t values(0);
select pg_sleep(0.2);
select  job_name, nspname from pg_job where dbname='event_b';
drop event if exists ee11;
create event IF NOT EXISTS ee11 on schedule at LOCALTIMESTAMP + interval '00:00' minute to second disable do insert into t values(0);
select pg_sleep(0.2);
select  job_name, nspname from pg_job where dbname='event_b';
drop event if exists ee11;
create event IF NOT EXISTS ee11 on schedule at now() + interval 1 year + interval '00:00' minute to second disable do insert into t values(0);
select pg_sleep(0.2);
select  job_name, nspname from pg_job where dbname='event_b';
drop event if exists ee11;
create event IF NOT EXISTS ee11 on schedule at now() + interval 666666666666666666666666666667 year + interval '00:00' minute to second disable do insert into t values(0);
select pg_sleep(0.2);
select  job_name, nspname from pg_job where dbname='event_b';
drop event if exists ee11;
create event IF NOT EXISTS ee11 on schedule at sysdate + interval 1234567890 second + interval 1234567890 minute disable do insert into t values(0);
select pg_sleep(0.2);
select  job_name, nspname from pg_job where dbname='event_b';
drop event if exists ee11;
create event IF NOT EXISTS ee11 on schedule at sysdate + interval 1.5 second + interval 1.33 minute disable do insert into t values(0);
select pg_sleep(0.2);
select  job_name, nspname from pg_job where dbname='event_b';
drop event if exists ee11;

--CHECK Schedule EVERY ..situation 
create event IF NOT EXISTS evtest on schedule every 1 minute disable do insert into t values(0);
select pg_sleep(0.2);
select  job_name, nspname from pg_job where dbname='event_b';
drop event if exists evtest;
create event IF NOT EXISTS evtest on schedule every '00:30' minute to second disable do insert into t values(0);
select pg_sleep(0.2);
select  job_name, nspname from pg_job where dbname='event_b';
drop event if exists evtest;
create event IF NOT EXISTS evtest on schedule every 1 minute starts sysdate disable do insert into t values(0);
select pg_sleep(0.2);
select  job_name, nspname from pg_job where dbname='event_b';
drop event if exists evtest;
create event IF NOT EXISTS evtest on schedule every 1 minute ends sysdate + interval 1 hour disable do insert into t values(0);
select  job_name, nspname from pg_job where dbname='event_b';
drop event if exists evtest;
create event IF NOT EXISTS evtest on schedule every 1 minute starts sysdate + interval 1 day ends now() + interval 1 year disable do insert into t values(0);
select pg_sleep(0.2);
select  job_name, nspname from pg_job where dbname='event_b';
drop event if exists evtest;
create event IF NOT EXISTS evtest on schedule every 1 minute starts sysdate + interval 1 day + interval '00:99' minute to second disable do insert into t values(0);
select pg_sleep(0.2);
select  job_name, nspname from pg_job where dbname='event_b';
drop event if exists evtest;
--test time unit
drop event if exists ev_unit;
create event ev_unit on schedule every '1-1' YEAR_MONTH do select 1;
select interval from pg_job where job_name='ev_unit';
drop event if exists ev_unit;
create event ev_unit on schedule every '-1 10' DAY_HOUR do select 1;
select interval from pg_job where job_name='ev_unit';
drop event if exists ev_unit;
create event ev_unit on schedule every '1 1:00' DAY_MINUTE do select 1;
select interval from pg_job where job_name='ev_unit';
drop event if exists ev_unit;
create event ev_unit on schedule every '1 1:1:1' DAY_SECOND do select 1;
select interval from pg_job where job_name='ev_unit';
drop event if exists ev_unit;
create event ev_unit on schedule every '1:1:1' HOUR_MINUTE do select 1;
select interval from pg_job where job_name='ev_unit';
drop event if exists ev_unit;
create event ev_unit on schedule every '1:1' MINUTE_SECOND do select 1;
select interval from pg_job where job_name='ev_unit';
drop event if exists ev_unit;
create event ev_unit on schedule every '01:12:30' HOUR_SECOND do select 1;
select interval from pg_job where job_name='ev_unit';
drop event if exists ev_unit;


--if not exists 
create event e on schedule every 1 month disable do select 1;
select pg_sleep(0.2);
create event e on schedule at sysdate disable do select 1;
select pg_sleep(0.2);
create event if not exists e on schedule at sysdate disable do select 1;
select pg_sleep(0.2);
drop event e;

--auto_drop 
drop event e;
create event e on schedule at sysdate disable do select 1;
select pg_sleep(0.2);
drop event e;

create event e on schedule at sysdate on completion not preserve disable do select 1;
select pg_sleep(0.2);
drop event e;

create event e on schedule at sysdate on completion preserve disable do select 1;
drop event e;

--job_status 
create event e on schedule every 1 hour do select 1;
drop event e;
create event e on schedule every 1 hour enable do select 1;
drop event e;
create event e on schedule every 1 hour disable do select 1;
drop event e;
create event e on schedule every 1 hour disable on slave do select 1;
drop event e;

--comment 
create event e on schedule at sysdate disable do select 1;
select pg_sleep(0.2);
drop event e;
create event e on schedule at sysdate disable comment '======' do select 1;
select pg_sleep(0.2);
drop event e;
create event e on schedule at sysdate disable comment 'fsdfjksadfhkjsfafkjsdfhjkahfdsknvxhguiyeurfbsdbccguyaHUFAWEFKSJBFCNJNDAgudagsHJBHDSBHJFBSAHBkjbhjbhjBHJBUbhbhBYGUIOInkb' do select 1;
select pg_sleep(0.2);
drop event e;
create event e on schedule at now() disable
comment '=================================================================================================
==========================================================================================================
==========================================================================================================
==========================================================================================================
==========================================================================================================
'
do  select 1;
select pg_sleep(0.2);
drop event e;

--sql body 
--abort
CREATE TABLE customer_demographics_t1
(
    CD_DEMO_SK                INTEGER               NOT NULL,
    CD_GENDER                 CHAR(1)                       ,
    CD_MARITAL_STATUS         CHAR(1)                       ,
    CD_EDUCATION_STATUS       CHAR(20)                      ,
    CD_PURCHASE_ESTIMATE      INTEGER                       ,
    CD_CREDIT_RATING          CHAR(10)                      ,
    CD_DEP_COUNT              INTEGER                       ,
    CD_DEP_EMPLOYED_COUNT     INTEGER                       ,
    CD_DEP_COLLEGE_COUNT      INTEGER
)
WITH (ORIENTATION = COLUMN,COMPRESSION=MIDDLE)
;
INSERT INTO customer_demographics_t1 VALUES(1920801,'M', 'U', 'DOCTOR DEGREE', 200, 'GOOD', 1, 0,0);
SELECT * FROM customer_demographics_t1 WHERE cd_demo_sk = 1920801;
START TRANSACTION;
UPDATE customer_demographics_t1 SET cd_education_status= 'Unknown';
SELECT * FROM customer_demographics_t1 WHERE cd_demo_sk = 1920801;
create event e on schedule at sysdate do ABORT; 
SELECT * FROM customer_demographics_t1 WHERE cd_demo_sk = 1920801;
DROP TABLE customer_demographics_t1;
ABORT;

--CALL
CREATE FUNCTION func_add_sql(num1 integer, num2 integer) RETURN integer
AS
BEGIN
RETURN num1 + num2;
END;
/
create event e on schedule at sysdate disable do CALL func_add_sql(1, 3);
DROP FUNCTION func_add_sql;
drop event e;

--ALTER EVENT 
--alter schedule 
\c event_b
drop event e;
create definer=event_a event e on schedule at '2023-01-16 21:05:40' disable do select 1;
show events where job_name='e';
alter definer=event_a event e on schedule at '2023-01-16 21:05:40' + interval 1 year;
show events where job_name='e';
alter definer=event_a event e on schedule every 1 year;
show events where job_name='e';
alter definer=event_a event e on schedule every 0.5 minute starts '2023-01-16 21:05:40' + interval '00:50' minute to second;
show events where job_name='e';
alter definer=event_a event e on schedule at '2023-01-16 21:05:40' + interval 500 second;
show events where job_name='e';
drop event e;

--alter auto_drop 
drop event e;
create definer=event_a event e on schedule at '2023-01-16 21:05:40' disable do select 1;
show events where job_name='e';
select * from gs_job_attribute where job_name='e' and attribute_name='auto_drop';
drop event e;
create definer=event_a event e on schedule at '2023-01-16 21:05:40' on completion preserve disable do select 1;
show events where job_name='e';
select * from gs_job_attribute where job_name='e' and attribute_name='auto_drop';
alter definer=event_a event e on completion not preserve;
show events where job_name='e';
select * from gs_job_attribute where job_name='e' and attribute_name='auto_drop';
alter definer=event_a event e on completion preserve;
show events where job_name='e';
select * from gs_job_attribute where job_name='e' and attribute_name='auto_drop';
drop event e;

--alter event_name 
drop event e;
create event e on schedule at '2023-01-16 21:05:40' disable do select 1;
select  job_name, nspname from pg_job where dbname='event_b';
alter event e rename to e_new;
select  job_name, nspname from pg_job where dbname='event_b';
select what,job_name from pg_job_proc where job_name='e_new';
alter event e_new rename to e;
select  job_name, nspname from pg_job where dbname='event_b';
select what,job_name from pg_job_proc where job_name='e';
drop event e;

--alter status 
drop table if exists a;
create table a(a int);
create event e on schedule at '2023-01-16 21:05:40' disable do insert into a values(0);
select * from a;
alter event e on schedule every 1 year enable do insert into a values(0);
select * from a;
truncate table a;
alter event e disable;
select * from a;
drop event e;
create event e on schedule every 1 minute starts '3000-01-16 21:05:40' do select 1;
select enable from pg_job where job_name='e';
alter event e disable;
select enable from pg_job where job_name='e';
alter event e enable;
select enable from pg_job where job_name='e';
drop event e;

--Alter event combination test. 
drop event e;
create event e on schedule at '2023-01-16 21:05:40' disable do select 1;
alter definer=event_b event e on schedule every 1 year ends '2023-01-16 21:05:40' + interval 1 year;
alter event e disable;
alter event e do select 2;
alter event e rename to ee comment 'test ee' do select sysdate;
alter event ee comment '========test=========';
alter event ee on schedule at '2023-01-16 21:05:40' + interval 1 year on completion preserve rename to test_e;
drop event if exists test_e;

--Test owner
create user evtest_owner password 'event_123';
create event e on schedule at sysdate disable do select 1;
alter definer=evtest_owner event e;
select log_user, priv_user from pg_job where job_name='e';
alter event e rename to ee;
alter definer=evtest_owner event ee rename to e;
select log_user, priv_user from pg_job where job_name='e';
create definer=evtest_owner event e_a on schedule at sysdate disable do select 1;
select log_user, priv_user from pg_job where job_name='e_a';
alter event e_a rename to ea;
alter definer=evtest_owner event ea rename to e_a;
select log_user, priv_user from pg_job where job_name='e_a';
select log_user, priv_user from pg_job where dbname='event_b';
drop user evtest_owner;
select log_user, priv_user from pg_job where dbname='event_b';
select * from gs_job_attribute where job_name='e' or job_name='e_a';

--SHOW EVENTS
drop event if exists e1;
create definer=event_a event e1 on schedule at '2023-01-16 21:05:40' disable do select 1;

select  job_name, nspname from pg_job where dbname='event_b';
show events in a;
show events from a;
show events like 'e';
show events like 'e%';
show events like 'e_';
show events where job_name='e1';
drop event if exists e1;

--security check
drop user if exists event_se_a cascade;
drop user if exists event_se_b cascade;
drop user if exists event_se_c cascade;
drop user if exists event_se_d cascade;

create user event_se_a with MONADMIN password 'event_123';
create user event_se_b with OPRADMIN password 'event_123';
create user event_se_c with INDEPENDENT password 'event_123';
create user event_se_d with SYSADMIN  password 'event_123';
drop event if exists e;
create definer=event_se_a event e on schedule at sysdate do select 1;
create definer=event_se_b event e on schedule at sysdate do select 1;
create definer=event_se_c event e on schedule at sysdate do select 1;
create definer=event_se_d event e on schedule at sysdate do select 1;
drop event if exists e;

create event e on schedule at sysdate disable do select 1;
alter definer=event_se_a event e;
alter definer=event_se_b event e;
alter definer=event_se_c event e;
alter definer=event_se_d event e;
drop event if exists e;

\c event_b
drop user if exists event_se_a cascade;
drop user if exists event_se_b cascade;
drop user if exists event_se_c cascade;
drop user if exists event_se_d cascade;

--test privilege
drop user if exists priv_a cascade;
drop user if exists priv_b cascade;
drop user if exists priv_c cascade;

create user priv_a password 'event_123';
create user priv_b with sysadmin password 'event_123';
create user priv_c password 'event_123';

--test CREATE
set role priv_a password 'event_123';
drop event if exists priv_e_a;
--fail Non-administrator users do not have the permission
create event priv_b.priv_e_a on schedule at sysdate disable do select 1;

\c event_b
grant create on schema priv_b to priv_a;
set role priv_a password 'event_123';
--success
create event priv_b.priv_e_a on schedule at sysdate disable do select 1;
drop event if exists priv_e_a;

set role priv_b password 'event_123';
--success
drop event if exists priv_e_b;
create event priv_a.priv_e_b on schedule at sysdate disable do select 1;
drop event if exists priv_e_b;

\c event_b
revoke all on schema priv_b from priv_a;

--test ALTER
set role priv_a password 'event_123';
drop event if exists priv_e_a;
create event priv_a.priv_e_a on schedule at sysdate disable do select 1;

--fail Non-administrator users do not have the permission
set role priv_c password 'event_123';
alter event priv_a.priv_e_a do select 2;

\c event_b
grant usage on schema priv_a to priv_c;
set role priv_c password 'event_123';
--fail only owner and sysadmin user have the permission
alter event priv_a.priv_e_a do select 2;
\c event_b
alter definer = priv_c event priv_a.priv_e_a;
set role priv_c password 'event_123';
--success
alter event priv_a.priv_e_a do select 2;
drop event if exists priv_e_a;

set role priv_a password 'event_123';
drop event if exists priv_e_a;
create event priv_a.priv_e_a on schedule at sysdate disable do select 1;
set role priv_b password 'event_123';
--success
alter event priv_a.priv_e_a do select 2;
drop event if exists priv_e_a;
revoke all on schema priv_a from priv_c;

--test DROP
set role priv_a password 'event_123';
drop event if exists priv_e_a;
create event priv_a.priv_e_a on schedule at sysdate disable do select 1;

set role priv_c password 'event_123';
--fail Non-administrator users do not have the permission
drop event if exists priv_e_a;

\c event_b
grant usage on schema priv_a to priv_c;
set role priv_c password 'event_123';
--fail only owner and sysadmin user have the permission
drop event if exists priv_e_a;

--success
set role priv_a password 'event_123';
drop event if exists priv_e_a;
create event priv_a.priv_e_a on schedule at sysdate disable do select 1;

set role priv_b password 'event_123';
--success
drop event if exists priv_e_a;

\c event_b
drop user if exists priv_a cascade;
drop user if exists priv_b cascade;
drop user if exists priv_c cascade;

--test sql help
\h CREATE EVENT
\h ALTER EVENT
\h DROP EVENT
\h SHOW EVENTS

drop table if exists event_a.a;
drop table if exists event_b.t;
drop schema if exists event_a;
drop schema if exists event_b;
drop user if exists event_a;
drop user if exists event_b;
\c regression
drop database if exists event_b;
