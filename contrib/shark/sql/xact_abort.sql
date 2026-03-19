create schema d_dbproc;
set current_schema = d_dbproc;
set xact_abort to on;
set xact_abort on;
set xact_abort off;
show xact_abort;--off
create table t1(id int primary key, name VARCHAR(100));
insert into t1 values(1, 'zhangsan');
--xact_abort off
begin;
insert into t1 values(1, 'zhangsan');
insert into t1 values(2, 'lisi');
end;
select * from t1; --2row

--savepoint
begin;
insert into t1 values(3, 'maliu');
savepoint p1;
insert into t1 values(4, 'wangwu');
rollback to savepoint p1;
insert into t1 values(5, 'liming');
rollback;
select * from t1;--2row

begin;
insert into t1 values(3, 'maliu');
savepoint p1;
insert into t1 values(4, 'wangwu');
rollback to savepoint p1;
insert into t1 values(5, 'liming');
end;
select * from t1;--4row

begin;
insert into t1 values(1);
insert into t1 values(4, 'lisi');
end;
select * from t1; --5row

--xact_abort on
truncate t1;
select * from t1;
set xact_abort on;
begin;
insert into t1 values(1, 'zhangsan');
insert into t1 values(1, 'zhangsan');
end;
select * from t1;

--savepoint
begin;
insert into t1 values(1, 'zhangsan');
savepoint p1;
insert into t1 values(2, 'lisi');
rollback to savepoint p1;
insert into t1 values(3, 'wanger');
end;
select * from t1;

truncate t1;
begin;
insert into t1 values(1, 'zhangsan');
savepoint p1;
insert into t1 values(2, 'lisi');
rollback to savepoint p1;
insert into t1 values(1, 'wanger');
end;
select * from t1;

begin;
insert into t1 values(1, 'zhangsan');
savepoint p1;
insert into t1 values(2, 'lisi');
insert into t1 values(3, 'wanger');
end;
select * from t1;

--create procedure
truncate t1;
insert into t1 values(2, 'lisi');
create or replace procedure d_dbproc.proc1()
as
begin
    insert into t1 values(2, 'lisi');
    insert into t1 values(1, 'wanger');
end;
/
set xact_abort on;
show xact_abort;
call d_dbproc.proc1();
select * from t1;--1row

truncate t1;
insert into t1 values(2, 'lisi2');
create or replace procedure d_dbproc.proc2()
as
begin
    insert into t1 values(2, 'lisi');
    insert into t1 values(3, 'wanger');
end;
/
set xact_abort off;
show xact_abort;
call d_dbproc.proc2();
select * from t1;--2row

truncate t1;
insert into t1 values(2, 'lisi');
create or replace procedure d_dbproc.proc3() as
    
begin
    insert into t1 values(2, 'zhangsan');
    insert into t1 values(3, 'zhangsan');
end;
/
set xact_abort on;
show xact_abort;
call d_dbproc.proc3();
select * from t1;--3row
truncate t1;
insert into t1 values(1, 'zhangsan');
CREATE OR replace PROCEDURE d_dbproc.proc4() AS
BEGIN
        RAISE EXCEPTION 'raise exception';
        EXCEPTION
        WHEN OTHERS THEN
           insert into t1 values(1, 'lisi');
           insert into t1 values(2, 'wanger');
END;
/
CALL d_dbproc.proc4();
select * from t1;

create table t99(id int primary key);
create procedure usp(@id int) as begin insert into t99 values(@id);end;
/

call usp('');
select * from t99;
call usp('');
select * from t99;
drop procedure usp();
drop table t99;

set xact_abort off;
truncate t1;
insert into t1 values(2, 'lisi');
CREATE OR replace PROCEDURE d_dbproc.proc5() AS
BEGIN
        RAISE EXCEPTION 'raise exception';
        EXCEPTION
        WHEN OTHERS THEN
           insert into t1 values(1, 'lisi');
           insert into t1 values(2, 'wanger');
END;
/
CALL d_dbproc.proc5();
select * from t1;
--dump
drop schema d_dbproc cascade;
alter user tester2 set xact_abort to on;
