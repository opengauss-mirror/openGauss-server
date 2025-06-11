create database event_db dbcompatibility 'B';
\c event_db
create schema event_s;
set current_schema = event_s;
set dolphin.b_compatibility to on;

show events;

create user event_a sysadmin password 'event_123';
create definer=event_a event e1 on schedule at '2023-01-16 21:05:40' disable do select 1;

select  job_name, nspname from pg_job where dbname='event_b';
show events in a;
show events from a;
show events like 'e';
show events like 'e%';
show events like 'e_';
show events where job_name='e1';
drop event if exists e1;
drop user if exists event_a;

reset current_schema;
drop schema event_s;
\c postgres
drop database if exists event_db;