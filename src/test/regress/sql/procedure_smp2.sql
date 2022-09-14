drop schema smp_test2 cascade;
create schema smp_test2;
set current_schema=smp_test2;

create table t1(id int, val text);
insert into t1 values(1,'abc'),(2,'bcd'),(3,'dafs');

create or replace package pkg_smp IS
procedure pp1();
END pkg_smp;
/

create or replace package body pkg_smp is
procedure pp1()
is
begin
    execute 'select count(*) from t1';
    execute 'select count(*) from t1';
end;
end pkg_smp;
/

create or replace procedure p1()
is
begin
    execute 'select count(*) from t1';
	call pkg_smp.pp1();
end;
/

create or replace procedure p2()
is
begin
    execute 'select count(*) from t1';
	insert into t1 values(4, 'dsb');
	rollback;
	execute 'select count(*) from t1';
	insert into t1 values(4, 'abc');
	commit;
end;
/

create or replace procedure p3()
is
begin
    savepoint p7_1;
    execute 'select count(*) from t1';
	insert into t1 values(5, 'abc');
    rollback to p7_1;
	execute 'select count(*) from t1';
	insert into t1 values(5, 'efg');
	commit;
end;
/

create or replace procedure p4()
is
begin
    execute 'select count(*) from t1';
	raise exception 'raise exception';
exception
    when others then
        execute 'select count(*) from t1';
end;
/


set enable_auto_explain=true;
set auto_explain_level=log; 
set client_min_messages=log;

set query_dop=1004;
set sql_beta_feature='enable_plsql_smp';

set current_schema=smp_test2;

call p1();
call p2();
call p3();
call p4();

set current_schema=public;
set enable_indexscan=off;
set enable_bitmapscan=off;
drop schema smp_test2 cascade;
