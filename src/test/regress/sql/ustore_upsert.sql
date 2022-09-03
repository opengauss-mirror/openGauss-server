create table t1(a int primary key, b int) with (storage_type=ustore);
insert into t1 values (1,1);
insert into t1 values (1,2) on duplicate key update b=2;
start transaction isolation level repeatable read;
insert into t1 values (1,2) on duplicate key update nothing;
commit transaction;
drop table t1;