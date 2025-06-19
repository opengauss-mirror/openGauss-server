create schema transaction_test;
set search_path = 'transaction_test';

create table t1(c1 int);

begin tran;
insert into t1 values(1);
select * from t1 order by c1;
commit tran;
select * from t1 order by c1;

begin tran transaction1;
insert into t1 values(2);
select * from t1 order by c1;
commit tran transaction1;
select * from t1 order by c1;

begin tran transaction1;
insert into t1 values(3);
select * from t1 order by c1;
commit tran transaction2;

begin tran transaction1;
insert into t1 values(4);
select * from t1 order by c1;
commit tran;

begin tran;
insert into t1 values(5);
select * from t1 order by c1;
commit tran transaction2;

begin tran;
insert into t1 values(6);
select * from t1 order by c1;
rollback tran;
select * from t1 order by c1;

begin tran transaction1;
insert into t1 values(7);
select * from t1 order by c1;
rollback tran;
select * from t1 order by c1;

begin tran transaction1;
insert into t1 values(8);
save tran savepoint1;
insert into t1 values(9);
rollback tran savePoint1;
select * from t1 order by c1;
commit tran;

begin tran transaction1;
insert into t1 values(10);
save tran savepoint1;
insert into t1 values(11);
rollback tran;
select * from t1 order by c1;

begin tran;
save tran savePoint1;
insert into t1 values(12);
select * from t1 order by c1;
rollback tran savePoint1;
select * from t1 order by c1;
save tran savePoint2;
insert into t1 values(13);
save tran savePoint3;
insert into t1 values(14);
save tran savePoint4;
insert into t1 values(15);
select * from t1 order by c1;
rollback tran savePoint4;
select * from t1 order by c1;
rollback tran savePoint3;
select * from t1 order by c1;
rollback tran savePoint2;
select * from t1 order by c1;
commit tran;
select * from t1 order by c1;

begin tran;
save tran savePoint1;
drop table t1;
\d t1;
rollback tran savePoint1;
select * from t1 order by c1;
save tran savePoint5;
insert into t1 values(16);
save tran savePoint6;
insert into t1 values(17);
save tran savePoint7;
insert into t1 values(18);
select * from t1 order by c1;
rollback tran savePoint6;
select * from t1 order by c1;
commit tran;
select * from t1 order by c1;

begin tran;
insert into t1 values(19);
save tran savePoint8;
insert into t1 values(20);
save tran savePoint9;
insert into t1 values(21);
save tran savePoint10;
insert into t1 values(22);
select * from t1 order by c1;
rollback tran savePoint8;
select * from t1 order by c1;
commit tran;
select * from t1 order by c1;

begin tran transaction1;
save tran savePoint1;
insert into t1 values(23);
select * from t1 order by c1;
-- expect error
rollback tran savePoint2;
select * from t1 order by c1;
rollback tran;
select * from t1 order by c1;

begin tran transaction1;
insert into t1 values(24);
begin tran transaction2;
insert into t1 values(25);
select * from t1 order by c1;
commit tran transaction2;
select * from t1 order by c1;
commit tran transaction1;
select * from t1 order by c1;

begin tran transaction1;
insert into t1 values(26);
begin tran transaction2;
insert into t1 values(27);
select * from t1 order by c1;
rollback tran;
select * from t1 order by c1;
rollback tran;
select * from t1 order by c1;

begin tran transaction1;
insert into t1 values(28);
begin tran transaction2;
insert into t1 values(29);
select * from t1 order by c1;
commit tran transaction1;
select * from t1 order by c1;
commit tran transaction2;
select * from t1 order by c1;

truncate table t1;
begin tran;
insert into t1 values(1);
commit work;
select * from t1;

begin tran;
insert into t1 values(2);
rollback work;
select * from t1;

begin tran;
insert into t1 values(3);
commit;
select * from t1;

begin tran;
insert into t1 values(4);
rollback;
select * from t1;

truncate table t1;
begin transaction;
insert into t1 values(1);
select * from t1 order by c1;
commit transaction;
select * from t1 order by c1;

begin transaction transaction1;
insert into t1 values(2);
select * from t1 order by c1;
commit transaction transaction1;
select * from t1 order by c1;

begin transaction transaction1;
insert into t1 values(3);
select * from t1 order by c1;
commit transaction transaction2;

begin transaction transaction1;
insert into t1 values(4);
select * from t1 order by c1;
commit transaction;

begin transaction;
insert into t1 values(5);
select * from t1 order by c1;
commit transaction transaction2;

begin transaction;
insert into t1 values(6);
select * from t1 order by c1;
rollback transaction;
select * from t1 order by c1;

begin transaction transaction1;
insert into t1 values(7);
select * from t1 order by c1;
rollback transaction;
select * from t1 order by c1;

begin transaction transaction1;
insert into t1 values(8);
save transaction savepoint1;
insert into t1 values(9);
rollback transaction savePoint1;
select * from t1 order by c1;
commit transaction;

begin transaction transaction1;
insert into t1 values(10);
save transaction savepoint1;
insert into t1 values(11);
rollback transaction;
select * from t1 order by c1;

begin transaction;
save transaction savePoint1;
insert into t1 values(12);
select * from t1 order by c1;
rollback transaction savePoint1;
select * from t1 order by c1;
save transaction savePoint2;
insert into t1 values(13);
save transaction savePoint3;
insert into t1 values(14);
save transaction savePoint4;
insert into t1 values(15);
select * from t1 order by c1;
rollback transaction savePoint4;
select * from t1 order by c1;
rollback transaction savePoint3;
select * from t1 order by c1;
rollback transaction savePoint2;
select * from t1 order by c1;
commit transaction;
select * from t1 order by c1;

begin transaction;
save transaction savePoint1;
drop table t1;
\d t1;
rollback transaction savePoint1;
select * from t1 order by c1;
save transaction savePoint5;
insert into t1 values(16);
save transaction savePoint6;
insert into t1 values(17);
save transaction savePoint7;
insert into t1 values(18);
select * from t1 order by c1;
rollback transaction savePoint6;
select * from t1 order by c1;
commit transaction;
select * from t1 order by c1;

begin transaction;
insert into t1 values(19);
save transaction savePoint8;
insert into t1 values(20);
save transaction savePoint9;
insert into t1 values(21);
save transaction savePoint10;
insert into t1 values(22);
select * from t1 order by c1;
rollback transaction savePoint8;
select * from t1 order by c1;
commit transaction;
select * from t1 order by c1;

begin transaction transaction1;
save transaction savePoint1;
insert into t1 values(23);
select * from t1 order by c1;
-- expect error
rollback transaction savePoint2;
select * from t1 order by c1;
rollback transaction;
select * from t1 order by c1;

begin transaction transaction1;
insert into t1 values(24);
begin transaction transaction2;
insert into t1 values(25);
select * from t1 order by c1;
commit transaction transaction2;
select * from t1 order by c1;
commit transaction transaction1;
select * from t1 order by c1;

begin transaction transaction1;
insert into t1 values(26);
begin transaction transaction2;
insert into t1 values(27);
select * from t1 order by c1;
rollback transaction;
select * from t1 order by c1;
rollback transaction;
select * from t1 order by c1;

begin transaction transaction1;
insert into t1 values(28);
begin transaction transaction2;
insert into t1 values(29);
select * from t1 order by c1;
commit transaction transaction1;
select * from t1 order by c1;
commit transaction transaction2;
select * from t1 order by c1;

truncate table t1;
begin transaction;
insert into t1 values(1);
commit work;
select * from t1;

begin transaction;
insert into t1 values(2);
rollback work;
select * from t1;

begin transaction;
insert into t1 values(3);
commit;
select * from t1;

begin transaction;
insert into t1 values(4);
rollback;
select * from t1;

truncate table t1;
create or replace procedure p1 () as 
BEGIN
    insert into t1 values(1);
    commit transaction;
    insert into t1 values(2);
    rollback transaction;
    save tran savepoint1;
    insert into t1 values(3);
    rollback tran savepoint1;
    insert into t1 values(4);
    commit transaction transactionName1;
END;
/

select p1();
select * from t1;

truncate table t1;
create or replace function f1 () returns int language pltsql as 
$$
BEGIN
    insert into t1 values(5);
    commit tran transactionName2;
    insert into t1 values(6);
    rollback tran;
    save tran savepoint1;
    insert into t1 values(7);
    rollback tran savepoint1;
    insert into t1 values(8);
    commit tran;
    return 1;
end;
$$;
select f1();
select * from t1;

truncate table t1;
do $$
BEGIN
    insert into t1 values(1);
    commit transaction;
    insert into t1 values(2);
    rollback transaction;
    save tran savepoint1;
    insert into t1 values(3);
    rollback tran savepoint1;
    insert into t1 values(4);
    commit transaction;
END
$$;


drop table if exists ValueTable;
CREATE TABLE ValueTable (id INT);

create or replace procedure procedure_15()
is
begin
INSERT INTO ValueTable VALUES(1);
rollback;
COMMIT;
end;
/

create or replace procedure procedure_15()
is
begin
INSERT INTO ValueTable VALUES(2);
SAVE TRAN save_point1;
COMMIT;
end;
/

create or replace procedure procedure_15()
is
begin
INSERT INTO ValueTable VALUES(3);
ROLLBACK TRAN;
COMMIT TRAN;
end;
/

create or replace procedure procedure_15()
is
begin
INSERT INTO ValueTable VALUES(4);
ROLLBACK TRANSACTION;
COMMIT TRANSACTION;
end;
/

drop procedure procedure_15();

BEGIN
    INSERT INTO ValueTable VALUES(1);
    SAVE TRAN save_point1;
    INSERT INTO ValueTable VALUES(2);
    ROLLBACK TRAN save_point1;
    INSERT INTO ValueTable VALUES(3);
    COMMIT TRANSACTION;
END;
/

select * from ValueTable;

drop table ValueTable;
drop function p1();
drop function f1();
drop table t1;
drop schema transaction_test cascade;
