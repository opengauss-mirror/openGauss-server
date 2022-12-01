--T1
drop foreign table test_update_column;
create foreign table test_update_column (x int primary key, y int not null, z int not null, data int);
create index idx1 on test_update_column (y);
create unique index idx2 on test_update_column (z);
insert into test_update_column values (1,2,3,100);
update test_update_column set y=3 where x=1;
select * from test_update_column;
update test_update_column set z=4 where x=1;
select * from test_update_column;

--T2
drop foreign table test_update_column;
create foreign table test_update_column (x int primary key, y int not null, z int not null, data int);
create index idx1 on test_update_column (y);
create unique index idx2 on test_update_column (z);
insert into test_update_column values (1,2,3,100);
begin;
update test_update_column set y=3 where x=1;
select * from test_update_column;
update test_update_column set z=4 where x=1;
select * from test_update_column;
commit;

--T3
drop foreign table test_update_column;
create foreign table test_update_column (x int primary key, y int not null, z int not null, data int);
create index idx1 on test_update_column (y);
create unique index idx2 on test_update_column (z);
begin;
insert into test_update_column values (1,2,3,100);
update test_update_column set y=3 where x=1;
select * from test_update_column;
update test_update_column set z=4 where x=1;
select * from test_update_column;
commit;

--T4
drop foreign table test_update_column;
create foreign table test_update_column (x int primary key, y int not null, z int not null, data int);
create index idx1 on test_update_column (y);
create unique index idx2 on test_update_column (z);
insert into test_update_column values (1,2,3,100);
begin;
delete from test_update_column;
insert into test_update_column values (1,2,3,100);
update test_update_column set y=3 where x=1;
select * from test_update_column;
update test_update_column set z=4 where x=1;
select * from test_update_column;
commit;
select * from test_update_column;

--T5
drop foreign table test_update_column;
create foreign table test_update_column (x int primary key, y int not null, z int not null, data int);
create index idx1 on test_update_column (y);
create unique index idx2 on test_update_column (z);
insert into test_update_column values (1,2,3,100);
insert into test_update_column values (3,3,4,100);
begin;
update test_update_column set y=3 where x=1;
select * from test_update_column;
update test_update_column set z=4 where x=1;
select * from test_update_column;
commit;
select * from test_update_column;

--T6
drop foreign table test_update_column;
create foreign table test_update_column (x int primary key, y int not null, z int not null, data int);
create index idx1 on test_update_column (y);
create unique index idx2 on test_update_column (z);
insert into test_update_column values (1,2,3,100);
begin;
update test_update_column set y=3 where x=1;
select * from test_update_column;
update test_update_column set z=4 where x=1;
select * from test_update_column;
insert into test_update_column values (3,3,4,100);
commit;
select * from test_update_column;

--T7
drop foreign table test_update_column;
create foreign table test_update_column (x int primary key, y int not null, z int not null, data int);
create index idx1 on test_update_column (y);
create unique index idx2 on test_update_column (z);
insert into test_update_column values (1,2,3,100);
begin;
select * from test_update_column;
update test_update_column set y=3 where x=1;
select * from test_update_column;
update test_update_column set z=5 where x=1;
select * from test_update_column;
insert into test_update_column values (3,3,4,100);
commit;
select * from test_update_column;

--T8
drop foreign table test_update_column;
create foreign table test_update_column (x int primary key, y int not null, z int not null, data int);
create index idx1 on test_update_column (y);
create unique index idx2 on test_update_column (z);
insert into test_update_column values (1,2,3,100);
begin;
select * from test_update_column;
update test_update_column set y=3 where x=1;
select * from test_update_column;
update test_update_column set z=5 where x=1;
select * from test_update_column;
insert into test_update_column values (3,3,4,100);
select * from test_update_column;
update test_update_column set y=100 where y=3;
commit;
select * from test_update_column;

--T9
drop foreign table test_update_column;
create foreign table test_update_column (x int primary key, y int not null, z int not null, data int);
create index idx1 on test_update_column (y);
create unique index idx2 on test_update_column (z);
insert into test_update_column values (1,2,3,100);
insert into test_update_column values (3,3,4,100);
begin;
select * from test_update_column;
update test_update_column set z=5;
commit;
select * from test_update_column;

--T10
drop foreign table test_update_column;
create foreign table test_update_column (x int primary key, y int not null, z int not null, data int);
create index idx1 on test_update_column (y);
create unique index idx2 on test_update_column (z);
insert into test_update_column values (1,2,3,100);
update test_update_column set y=3 where x=1;
alter foreign table test_update_column drop column y;
select * from test_update_column;
update test_update_column set z=4 where x=1;
select * from test_update_column;

--T11
drop foreign table test_update_column;
create foreign table test_update_column (x int primary key, y int not null, z int not null, data int);
create index idx1 on test_update_column (y);
create unique index idx2 on test_update_column (z);
insert into test_update_column values (1,2,3,100);
update test_update_column set y=3 where x=1;
select * from test_update_column;
update test_update_column set z=4 where x=1;
select * from test_update_column;
alter foreign table test_update_column drop column z;
select * from test_update_column;

--T12
drop foreign table test_update_column;
create foreign table test_update_column (x int primary key, y int not null, z int not null, data int);
create index idx1 on test_update_column (y);
create unique index idx2 on test_update_column (z);
insert into test_update_column values (1,2,3,100);
begin;
update test_update_column set y=3 where x=1;
select * from test_update_column;
update test_update_column set z=4 where x=1;
select * from test_update_column;
alter foreign table test_update_column drop column z;
commit;
select * from test_update_column;

--T13
drop foreign table test_update_column;
create foreign table test_update_column (x int primary key, y int not null, z int not null, data int);
create index idx1 on test_update_column (y);
create unique index idx2 on test_update_column (z);
insert into test_update_column values (1,2,3,100);
begin;
update test_update_column set y=3 where x=1;
select * from test_update_column;
alter foreign table test_update_column drop column z;
update test_update_column set z=4 where x=1;
select * from test_update_column;
commit;
select * from test_update_column;

--T14
drop foreign table test_update_column;
create foreign table test_update_column (x int primary key, y int not null, z int not null, data int);
create index idx1 on test_update_column (y);
create unique index idx2 on test_update_column (z);
insert into test_update_column values (1,2,3,100);
begin;
delete from test_update_column;
insert into test_update_column values (1,2,3,100);
update test_update_column set y=3 where x=1;
select * from test_update_column;
update test_update_column set z=4 where x=1;
select * from test_update_column;
alter foreign table test_update_column drop column z;
commit;
select * from test_update_column;

--T15
drop foreign table test_update_column;
create foreign table test_update_column (x int primary key, y int not null, z int not null, data int);
create index idx1 on test_update_column (y);
create unique index idx2 on test_update_column (z);
insert into test_update_column values (1,2,3,100);
begin;
select * from test_update_column;
update test_update_column set y=3 where x=1;
select * from test_update_column;
update test_update_column set z=5 where x=1;
select * from test_update_column;
insert into test_update_column values (3,3,4,100);
select * from test_update_column;
update test_update_column set y=100 where y=3;
select * from test_update_column;
alter foreign table test_update_column drop column y;
commit;
select * from test_update_column;

--T16
drop foreign table test_update_column;
create foreign table test_update_column (x int primary key, y int not null, z int not null, data int);
create index idx1 on test_update_column (y);
create unique index idx2 on test_update_column (z);
insert into test_update_column values (1,2,3,100);
insert into test_update_column values (3,3,4,100);
begin;
delete from test_update_column;
insert into test_update_column values (1,2,3,100);
insert into test_update_column values (3,3,5,100);
update test_update_column set z=z+1;
commit;
select * from test_update_column;

--T17
drop foreign table test_update_column;
create foreign table test_update_column (x int primary key, y int not null, z int not null, data int);
create index idx1 on test_update_column (y);
create unique index idx2 on test_update_column (z);
insert into test_update_column values (1,2,3,100);
insert into test_update_column values (3,3,4,100);
begin;
delete from test_update_column;
insert into test_update_column values (1,2,3,100);
insert into test_update_column values (3,3,5,100);
update test_update_column set z=z+1;
alter foreign table test_update_column drop column y;
commit;
select * from test_update_column;


--T18
drop foreign table test_update_column;
create foreign table test_update_column (x int primary key, y int not null, z int not null, data int);
create index idx1 on test_update_column (y);
create unique index idx2 on test_update_column (z);
insert into test_update_column values (1,2,3,100);
insert into test_update_column values (3,3,4,100);
begin;
delete from test_update_column;
insert into test_update_column values (1,2,3,100);
insert into test_update_column values (3,3,5,100);
update test_update_column set z=z+1;
delete from test_update_column;
insert into test_update_column values (1,2,3,100);
insert into test_update_column values (3,3,5,100);
update test_update_column set z=z+1;
commit;
select * from test_update_column;

--T19
drop foreign table test_update_column;
create foreign table test_update_column (x int primary key, y int not null, z int not null, data int);
create index idx1 on test_update_column (y);
create unique index idx2 on test_update_column (z);
insert into test_update_column values (1,2,3,100);
insert into test_update_column values (3,3,4,100);
begin;
delete from test_update_column;
insert into test_update_column values (1,2,3,100);
insert into test_update_column values (3,3,5,100);
update test_update_column set z=z+1;
delete from test_update_column;
insert into test_update_column values (1,2,3,100);
insert into test_update_column values (3,3,5,100);
update test_update_column set z=z+1;
alter foreign table test_update_column drop column y;
commit;
select * from test_update_column;

drop foreign table test_update_column;
