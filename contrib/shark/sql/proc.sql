create schema test_proc;
set current_schema to test_proc;

create procedure p1()
is
begin        
RAISE INFO 'call procedure: p1';
end;
/

create proc p2()
is
begin
RAISE INFO 'call procedure: p2';
end;
/

\df p1();
\df p2();

alter procedure p1() stable;
alter proc p2() stable;
select provolatile from pg_proc where proname = 'p1';
select provolatile from pg_proc where proname = 'p2';

alter procedure p1() rename to new_p1;
alter proc p2() rename to new_p2;
\df new_p1();
\df new_p2();

create user test_proc_user with password 'Test@123';
grant all privileges to test_proc_user;
alter procedure new_p1() owner to test_proc_user;
alter proc new_p2() owner to test_proc_user;
select usename from pg_user a, pg_proc b where a.usesysid = b.proowner and b.proname = 'new_p1';
select usename from pg_user a, pg_proc b where a.usesysid = b.proowner and b.proname = 'new_p2';

create schema new_schema;
alter procedure new_p1() set schema new_schema;
alter proc new_p2() set schema new_schema;
select nspname from pg_namespace a, pg_proc b where a.oid = b.pronamespace and b.proname = 'new_p1';
select nspname from pg_namespace a, pg_proc b where a.oid = b.pronamespace and b.proname = 'new_p2';

alter procedure new_p1 compile;
alter procedure new_p1() compile;
alter proc new_p2 compile;
alter proc new_p2() compile;

call new_schema.new_p1();
call new_schema.new_p2();

set current_schema to new_schema;
drop proc new_p1();
drop procedure new_p1();

drop proc if exists new_p2();
drop procedure if exists new_p2();

set current_schema to test_proc;
drop schema new_schema;
drop user test_proc_user;

create procedure p1()
is
begin        
RAISE INFO 'call procedure: p1';
end;
/

create procedure p2()
is
begin        
RAISE INFO 'call procedure: p2';
end;
/

drop proc p1;
drop procedure p2;

drop proc if exists new_p1;
drop procedure if exists new_p2;

drop procedure if exists proc1;
drop type if exists s_type;
create type s_type as (
    id integer,
    name varchar,
    addr text
);
set behavior_compat_options = '';
create or replace procedure proc1(a s_type)
is
begin
RAISE INFO 'call a: %', a;
end;
/

call proc1(((1,'zhang','shanghai')));

alter type s_type ADD attribute a int;
call proc1(((1,'zhang','shanghai', 10)));
alter procedure proc1(s_type) compile;
set behavior_compat_options = 'plpgsql_dependency';
alter proc proc1(s_type) compile; --error, plpgsql_dependency only valid in A database

create table proc(id int, proc varchar(10));
insert into proc values(1, 'test1');
select * from proc;
drop table proc;

drop procedure proc1;
drop type s_type;

create or replace procedure sp_proc1
(para1  in   integer,
 para2  inout  integer)
as
begin
   raise info 'para1 is %', para1;
   raise info 'para2 is %', para2 * 3;
end;
/

call sp_proc1(1, 2);

create or replace proc sp_proc1
(para1  in   integer,
 para2  inout  integer)
as
begin
   raise info 'para1 is %', para1;
   raise info 'para2 is %', para2 * 2;
end;
/

call sp_proc1(1, 2);

drop procedure sp_proc1;

drop schema test_proc cascade;
