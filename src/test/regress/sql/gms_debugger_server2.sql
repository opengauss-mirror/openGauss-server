-- setups
create extension if not exists gms_debug;
drop schema if exists gms_debugger_test2 cascade;
create schema gms_debugger_test2;
set search_path = gms_debugger_test2;

-- commit/rollback in procedure
create table tb1(a int);
create or replace procedure test_debug2 as
begin
    insert into tb1 values (1000);
    commit;
    insert into tb1 values (2000);
    rollback;
end;
/


-- start debug
select * from gms_debug.initialize('datanode1-1');

select pg_sleep(1);

-- start debug - 1st run
select * from test_debug2();

-- start debug - 2nd run - to be aborted
select * from test_debug2();

select * from gms_debug.debug_off();

drop schema gms_debugger_test2 cascade;
