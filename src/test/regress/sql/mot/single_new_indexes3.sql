--T0
drop foreign table test_new_gc;
create foreign table test_new_gc (x int, y int not null, z int not null,data int,
primary key(x)
);
create index idx1 on test_new_gc (y);
create index idx2 on test_new_gc (z);

begin;
insert into test_new_gc (x,y,z,data) values (1,2,3,0);
select * from test_new_gc where x=1;
select * from test_new_gc where y=2;
select * from test_new_gc where z=3;
update test_new_gc set data=100 where z=3;
select * from test_new_gc where x=1;
select * from test_new_gc where y=2;
select * from test_new_gc where z=3;
commit;
begin;
update test_new_gc set data=200 where x=1;
select * from test_new_gc where x=1;
select * from test_new_gc where y=2;
select * from test_new_gc where z=3;
commit;


--T1

drop foreign table test_new_gc;
create foreign table test_new_gc (x int, y int not null, z int not null, data int,
primary key(x)
);

create index idx1 on test_new_gc (y);
create index idx2 on test_new_gc (z);

begin;
insert into test_new_gc (x,y,z,data) values (1,2,3,0);
select * from test_new_gc where x=1;
select * from test_new_gc where y=2;
select * from test_new_gc where z=3;
update test_new_gc set data=100 where x=1;
select * from test_new_gc where x=1;
select * from test_new_gc where y=2;
select * from test_new_gc where z=3;
commit;
begin;
update test_new_gc set data=200 where y=2;
select * from test_new_gc where x=1;
select * from test_new_gc where y=2;
select * from test_new_gc where z=3;
commit;


--T2
drop foreign table test_new_gc;
create foreign table test_new_gc (x int, y int not null, z int not null,
primary key(x)
);

create index idx1 on test_new_gc (y);
create index idx2 on test_new_gc (z);

begin;
insert into test_new_gc (x,y,z) values (1,2,3);
select * from test_new_gc where x=1;
select * from test_new_gc where y=2;
select * from test_new_gc where z=3;
delete from test_new_gc where z=3;
select * from test_new_gc where x=1;
select * from test_new_gc where y=2;
select * from test_new_gc where z=3;
commit;
select * from test_new_gc where x=1;
select * from test_new_gc where y=2;
select * from test_new_gc where z=3;

--T3
drop foreign table test_new_gc;
create foreign table test_new_gc (x int, y int not null, z int not null,
primary key(x)
);

create index idx1 on test_new_gc (y);
create index idx2 on test_new_gc (z);

insert into test_new_gc (x,y,z) values (1,2,3);
begin;
select * from test_new_gc where x=1;
select * from test_new_gc where y=2;
select * from test_new_gc where z=3;
delete from test_new_gc where z=3;
select * from test_new_gc where x=1;
select * from test_new_gc where y=2;
select * from test_new_gc where z=3;
commit;

--T4 Duplicates
delete from test_new_gc;
insert into test_new_gc (x,y,z) values (1,2,3);
insert into test_new_gc (x,y,z) values (1,2,3);

--T5 Blocked Duplicates
delete from test_new_gc;
begin;
insert into test_new_gc (x,y,z) values (1,2,3);
insert into test_new_gc (x,y,z) values (1,2,3);
commit;

--T5.1 unique index duplicates!
drop foreign table test_new_gc;
create foreign table test_new_gc (x int, y int not null, z int not null,
primary key(x)
);
create index idx1 on test_new_gc (y);
create unique index idx2 on test_new_gc (z);
insert into test_new_gc (x,y,z) values (1,2,3);
insert into test_new_gc (x,y,z) values (2,3,3);

--T5.2 unique index duplicates!
drop foreign table test_new_gc;
create foreign table test_new_gc (x int, y int not null, z int not null,
primary key(x)
);
create index idx1 on test_new_gc (y);
create unique index idx2 on test_new_gc (z);
begin;
insert into test_new_gc (x,y,z) values (1,2,3);
insert into test_new_gc (x,y,z) values (2,3,3);
commit;

--T6 Insert-Delete-Insert All in A block
drop foreign table test_new_gc;
create foreign table test_new_gc (x int, y int not null, z int not null, data int,
primary key(x)
);
create index idx1 on test_new_gc (y);
create index idx2 on test_new_gc (z);
begin;
insert into test_new_gc (x,y,z) values (1,2,3);
delete from test_new_gc where x=1;
insert into test_new_gc (x,y,z) values (1,2,3);
delete from test_new_gc where x=1;
insert into test_new_gc (x,y,z) values (1,2,3);
delete from test_new_gc where x=1;
insert into test_new_gc (x,y,z) values (1,2,3);
delete from test_new_gc where x=1;
commit;
select * from test_new_gc where x=1;
select * from test_new_gc where y=2;
select * from test_new_gc where z=3;

--T6.1 Insert-Delete-Insert All in A block - unique index
drop foreign table test_new_gc;
create foreign table test_new_gc (x int, y int not null, z int not null,data int,
primary key(x)
);
create index idx1 on test_new_gc (y);
create unique index idx2 on test_new_gc (z);
begin;
insert into test_new_gc (x,y,z) values (1,2,3);
delete from test_new_gc where x=1;
insert into test_new_gc (x,y,z) values (1,2,3);
delete from test_new_gc where x=1;
insert into test_new_gc (x,y,z) values (1,2,3);
delete from test_new_gc where x=1;
insert into test_new_gc (x,y,z) values (1,2,3);
delete from test_new_gc where x=1;
commit;
select * from test_new_gc where x=1;
select * from test_new_gc where y=2;
select * from test_new_gc where z=3;


--T7 Insert-Delete-Insert All in A block II
drop foreign table test_new_gc;
create foreign table test_new_gc (x int, y int not null, z int not null,
primary key(x)
);

create index idx1 on test_new_gc (y);
create index idx2 on test_new_gc (z);
begin;
insert into test_new_gc (x,y,z) values (1,2,3);
delete from test_new_gc where x=1;
insert into test_new_gc (x,y,z) values (1,2,3);
delete from test_new_gc where x=1;
insert into test_new_gc (x,y,z) values (1,2,3);
delete from test_new_gc where x=1;
insert into test_new_gc (x,y,z) values (1,2,88);
commit;
select * from test_new_gc where x=1;
select * from test_new_gc where y=2;
select * from test_new_gc where z=88;

--T7.1 Insert-Delete-Insert All in A block II - unique
drop foreign table test_new_gc;
create foreign table test_new_gc (x int, y int not null, z int not null,
primary key(x)
);

create index idx1 on test_new_gc (y);
create unique index idx2 on test_new_gc (z);
begin;
insert into test_new_gc (x,y,z) values (1,2,3);
delete from test_new_gc where x=1;
insert into test_new_gc (x,y,z) values (1,2,3);
delete from test_new_gc where x=1;
insert into test_new_gc (x,y,z) values (1,2,3);
delete from test_new_gc where x=1;
insert into test_new_gc (x,y,z) values (1,2,88);
commit;
select * from test_new_gc where x=1;
select * from test_new_gc where y=2;
select * from test_new_gc where z=3;
select * from test_new_gc where z=88;

--New Delete Design

--T8 Insert-Begin-Delete-Insert-Rollback
drop foreign table test_new_gc;
create foreign table test_new_gc (x int, y int not null, z int not null, data int,
primary key(x)
);
create index idx1 on test_new_gc (y);
create index idx2 on test_new_gc (z);

insert into test_new_gc (x,y,z) values (1,2,3);
begin;
delete from test_new_gc where x=1;
select * from test_new_gc where x=1;
select * from test_new_gc where y=2;
select * from test_new_gc where z=3;
insert into test_new_gc (x,y,z) values (1,2,4);
select * from test_new_gc where x=1;
select * from test_new_gc where y=2;
select * from test_new_gc where z=3;
select * from test_new_gc where z=4;
rollback;
select * from test_new_gc where x=1;
select * from test_new_gc where y=2;
select * from test_new_gc where z=3;

--T8.1 Insert-Begin-Delete-Insert-Rollback - unique
drop foreign table test_new_gc;
create foreign table test_new_gc (x int, y int not null, z int not null, data int,
primary key(x)
);
create index idx1 on test_new_gc (y);
create unique index idx2 on test_new_gc (z);

insert into test_new_gc (x,y,z) values (1,2,3);
begin;
delete from test_new_gc where x=1;
select * from test_new_gc where x=1;
select * from test_new_gc where y=2;
select * from test_new_gc where z=3;
insert into test_new_gc (x,y,z) values (1,2,4);
select * from test_new_gc where x=1;
select * from test_new_gc where y=2;
select * from test_new_gc where z=4;
rollback;
select * from test_new_gc where x=1;
select * from test_new_gc where y=2;
select * from test_new_gc where z=3;

--T9 Insert-Begin-Delete-Insert-Rollback
drop foreign table test_new_gc;
create foreign table test_new_gc (x int, y int not null, z int not null, data int,
primary key(x)
);

create index idx1 on test_new_gc (y);
create index idx2 on test_new_gc (z);

insert into test_new_gc (x,y,z) values (1,2,3);
begin;
delete from test_new_gc where x=1;
select * from test_new_gc where x=1;
select * from test_new_gc where y=2;
select * from test_new_gc where z=3;
insert into test_new_gc (x,y,z) values (1,2,4);
select * from test_new_gc where x=1;
select * from test_new_gc where y=2;
select * from test_new_gc where z=3;
select * from test_new_gc where z=4;
delete from test_new_gc where y=2;
select * from test_new_gc where x=1;
select * from test_new_gc where y=2;
select * from test_new_gc where z=4;
insert into test_new_gc (x,y,z) values (1,2,5);
select * from test_new_gc where x=1;
select * from test_new_gc where y=2;
select * from test_new_gc where z=3;
select * from test_new_gc where z=4;
select * from test_new_gc where z=5;
delete from test_new_gc where z=5;
select * from test_new_gc where x=1;
select * from test_new_gc where y=2;
select * from test_new_gc where z=5;
commit;
--T10
drop foreign table test_new;
create foreign table test_new (i int primary key, x int);
insert into test_new values (generate_series(1,100));
begin;
update test_new set x = i+1;
delete from test_new where i = 3;
select * from test_new where i = 3;
commit;
--T11
drop foreign table test_new;
create foreign table test_new (i int , x int);
insert into test_new values (generate_series(1,100));
begin;
update test_new set x = i+1;
delete from test_new where x % 3 = 0;
insert into test_new values (3,333);
end;
drop foreign table test_new;
--T12 Inserts Tests
drop foreign table test_new_gc;
create foreign table test_new_gc (x int, y int not null, z int not null, data int,
primary key(x)
);
create index idx1 on test_new_gc (y);
create unique index idx2 on test_new_gc (z);

insert into test_new_gc (x,y,z) values (1,2,3);
begin;
delete from test_new_gc where x=1;
select * from test_new_gc where x=1;
select * from test_new_gc where y=2;
select * from test_new_gc where z=3;
insert into test_new_gc (x,y,z) values (1,2,4);
select * from test_new_gc where x=1;
select * from test_new_gc where y=2;
select * from test_new_gc where z=3;
select * from test_new_gc where z=4;
commit;
select * from test_new_gc where x=1;
select * from test_new_gc where y=2;
select * from test_new_gc where z=4;

--T13 Inserts Tests II 
drop foreign table test_new_gc;
create foreign table test_new_gc (x int, y int not null, z int  not null,data int,
primary key(x)
);

create index idx1 on test_new_gc (y);
create unique index idx2 on test_new_gc (z);

insert into test_new_gc (x,y,z) values (1,2,3);
begin;
delete from test_new_gc where x=1;
select * from test_new_gc where x=1;
select * from test_new_gc where y=2;
select * from test_new_gc where z=3;
insert into test_new_gc (x,y,z) values (1,2,4);
select * from test_new_gc where x=1;
select * from test_new_gc where y=2;
select * from test_new_gc where z=3;
select * from test_new_gc where z=4;
delete from test_new_gc where y=2;
select * from test_new_gc where x=1;
select * from test_new_gc where y=2;
select * from test_new_gc where z=4;
insert into test_new_gc (x,y,z) values (1,2,5);
select * from test_new_gc where x=1;
select * from test_new_gc where y=2;
select * from test_new_gc where z=3;
select * from test_new_gc where z=4;
select * from test_new_gc where z=5;
delete from test_new_gc where z=5;
select * from test_new_gc where x=1;
select * from test_new_gc where y=2;
select * from test_new_gc where z=5;
insert into test_new_gc (x,y,z) values (1,2,100);
commit;
select * from test_new_gc where x=1;
select * from test_new_gc where y=2;
select * from test_new_gc where z=3;
select * from test_new_gc where z=4;
select * from test_new_gc where z=100;
drop foreign table test_new_gc;

-- bug 150
create foreign table test_new (x integer primary key, y integer not null, c1 varchar(1020), c2 varchar(1020)) ;
create index idx on test_new (x,y);
begin;
insert into test_new values (generate_series(1, 500), generate_series(1, 500));

delete from test_new where x = 15;
insert into test_new values (15,15);
insert into test_new values (15,15);
end;
drop foreign table test_new;

-- bug 170
create foreign table test_new (i int primary key, x int not null);
insert into test_new values (generate_series(1,100));
begin;
update test_new set x = i+1;
delete from test_new where i = 3;
insert into test_new values (3,333);
commit;

begin;
delete from test_new where i = 3;
insert into test_new values (3,333);
commit;
drop foreign table test_new;

-- bug 169
create foreign table test_new (i int primary key, x int not null);
insert into test_new values (generate_series(1,100));
begin;
update test_new set x = i+1;
delete from test_new where i = 3;
insert into test_new values (3,333);
commit;
delete from test_new where i = 3;
drop foreign table test_new;
