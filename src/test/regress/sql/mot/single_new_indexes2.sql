
-- BUG 94
create foreign table test_new2 (x int primary key, y int not null, z int not null);
-- create duplicates
insert into test_new2 (y,x,z) values (generate_series(10,20), generate_series(10,20), generate_series(10,20));
insert into test_new2 (y,x,z) values (generate_series(10,20), generate_series(10,20), generate_series(10,20));

create index idx1 on test_new2 (x);
create unique index idx2 on test_new2 (x);

select * from test_new2 where x > 0 order by x;

drop foreign table test_new2;

-- BUG 95
create foreign table test_new2 (x int not null, y int not null, z int not null);

create index idx1 on test_new2 (x);
create index idx2 on test_new2 (y);
create index idx3 on test_new2 (z);
create unique index uidx on test_new2 (z,x);
insert into test_new2 (y,x,z) values (generate_series(10,20), generate_series(10,20), generate_series(10,20));
truncate test_new2;
insert into test_new2 (y,x,z) values (generate_series(10,20), generate_series(10,20), generate_series(10,20));

drop foreign table test_new2;

-- BUG 97
create foreign table test_new2 (x int not null, y int not null, z int not null);

create index idx1 on test_new2 (x);
create index idx2 on test_new2 (y);
create index idx3 on test_new2 (z);

insert into test_new2 values (1,1,1);
insert into test_new2 select * from test_new2; 
insert into test_new2 select * from test_new2;
insert into test_new2 select * from test_new2;
insert into test_new2 select * from test_new2;
insert into test_new2 select * from test_new2;
insert into test_new2 select * from test_new2;

drop foreign table test_new2;

-- BUG 98
create foreign table test_new2 (x int, y int, z int, primary key(x,y,z));
insert into test_new2 values (1,2,3);
insert into test_new2 values (2,3,4);
insert into test_new2 values (3,4,5);
insert into test_new2 values (4,5,6);
insert into test_new2 values (5,6,7);
insert into test_new2 values (6,7,8);
select * from test_new2 where x = y = z;

drop foreign table test_new2;

-- TEST 1

create foreign table test_new2 (x varchar(16), y varchar(16) not null, z varchar(16), primary key(x,z));
create index idx1 on test_new2 (y);

insert into test_new2 values ('aa', 'aa', 'zz');
insert into test_new2 values ('zz', 'aa', 'aa');
insert into test_new2 values ('xxx', 'aa', 'tttt');
insert into test_new2 values ('ii', 'aa', 'eee');
insert into test_new2 values ('eeee', 'aa', 'iii');

drop foreign table test_new2;

-- TEST 2
create foreign table test_new2 (x int not null, y int, z int);
insert into test_new2 (y,x,z) values (generate_series(10,20), generate_series(10,20), generate_series(10,20));

create index idx1 on test_new2 (x);
create unique index idx2 on test_new2 (x);

select * from test_new2 where x > 0 order by x;

drop foreign table test_new2;
--


-- TEST 3
create foreign table test_new2 (x int not null, y int not null, z int not null, primary key(x,y,z));
create unique index idx1 on test_new2 (x);
create unique index idx2 on test_new2 (y);
create unique index idx3 on test_new2 (z);

insert into test_new2 values (1,2,3);
insert into test_new2 values (2,3,4);
insert into test_new2 values (3,4,5);
insert into test_new2 values (4,5,6);
insert into test_new2 values (5,6,7);
insert into test_new2 values (6,7,8);

select * from test_new2 where x < 0 order by z desc;
select * from test_new2 where x <> 0 order by z desc;
insert into test_new2 values (-4,-5,-6);
select * from test_new2 where x < 0 order by z desc;
select * from test_new2 where x <> 0 order by z desc;

insert into test_new2 values (0,-5,-6);
insert into test_new2 values (0,100,100);
select * from test_new2 where x <> 0 order by z ;
select * from test_new2 where x >= 0 and y <> 2 order by z ;

drop foreign table test_new2;

-- TEST 4
create foreign table test_new21 (x int, y int, z int, primary key(x,y,z));
create foreign table test_new22 (x int, y int, z int, primary key(x,y,z));

insert into test_new21 (y,x,z) values (generate_series(10,100), generate_series(10,100), generate_series(10,100));
insert into test_new22 select * from test_new21;
truncate test_new21;
insert into test_new21 select * from test_new22;
truncate test_new22;

drop foreign table test_new21;
drop foreign table test_new22;

-- TEST 5

create foreign table test_new2 (x int not null, y int not null, z int not null) ;

create unique index idx1 on test_new2 (x);
create unique index idx2 on test_new2 (y);
create unique index idx3 on test_new2 (z);

insert into test_new2 values (1,1,1);
insert into test_new2 (x,y,z) values (2,2,2);

insert into test_new2 (x) values (generate_series(10,20));
select * from test_new2;

drop foreign table test_new2;

-- BUG 153

CREATE FOREIGN TABLE MATERIAL_BATCH_1_002_1(
C_CHAR_1 CHAR(1) not null,
C_CHAR_2 CHAR(10) not null,
C_CHAR_3 CHAR(100) not null,
C_VARCHAR_1 VARCHAR(1) not null,
C_VARCHAR_2 VARCHAR(10) not null,
C_VARCHAR_3 VARCHAR(64) not null,
C_INT BIGINT not null,
C_BIGINT BIGINT not null,
C_SMALLINT BIGINT not null,
C_FLOAT FLOAT not null,
C_NUMERIC numeric(20,5) not null,
C_DP double precision not null,
C_DATE DATE not null,
C_TS_WITHOUT TIMESTAMP  not null,
C_TS_WITH TIMESTAMP not null);

CREATE INDEX MATERIAL_INDEX_002_1 ON MATERIAL_BATCH_1_002_1(C_CHAR_1,C_CHAR_2,C_CHAR_3,C_VARCHAR_1,C_VARCHAR_2,C_VARCHAR_3,C_INT,C_BIGINT,C_TS_WITH);
CREATE INDEX MATERIAL_INDEX_002_2 ON MATERIAL_BATCH_1_002_1(C_CHAR_1,C_CHAR_2,C_CHAR_3,C_VARCHAR_1,C_VARCHAR_2,C_VARCHAR_3,C_INT,C_BIGINT,C_TS_WITH,C_SMALLINT);

DROP FOREIGN TABLE MATERIAL_BATCH_1_002_1;

--












