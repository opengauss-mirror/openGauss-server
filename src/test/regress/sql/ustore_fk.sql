create table t1(a int primary key, b int)with(storage_type=ustore);
create table t2(a int references t1, b int)with(storage_type=ustore);

insert into t2 values(1,1);
insert into t1 values(1,1);
insert into t2 values(1,1);
insert into t2 values(2,2);

update t2 set a=2,b=2;

insert into t1 values(2,2);
update t2 set a=2,b=2;


insert into t2 values(3,3);
update t1 set a=3,b=3 where a=1;

insert into t2 values(3,3);

delete from t1;

insert into t1 values(4,4);
delete from t1 where a=4;

delete from t2;
delete from t1;
drop table t1 cascade;
drop table t2 cascade;
