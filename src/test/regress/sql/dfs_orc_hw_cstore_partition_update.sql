set enable_global_stats = true;
--01--------------------------------------------------------------------
--syntax test ,  base update command
create table cstore_update_table
(
	c1 int ,
	c2 int
)with(orientation = column)
partition by range (c1)
(
	partition cstore_update_table_p0 values less than (5),
	partition cstore_update_table_p1 values less than (10),
	partition cstore_update_table_p2 values less than (15)
);
create index on cstore_update_table(c1,c2)local;
insert into cstore_update_table select generate_series(0,10), generate_series(0,10);
select * from cstore_update_table order by c1;
--10 rows   1---10

update cstore_update_table set c2 = c2 + 10;
select * from cstore_update_table order by c1;
--10 rows   10---20
drop table cstore_update_table;

--02--------------------------------------------------------------------
--where clause
create table cstore_update_table
(
	c1 int ,
	c2 int
)with(orientation = column)
partition by range (c1)
(
	partition cstore_update_table_p0 values less than (5),
	partition cstore_update_table_p1 values less than (10),
	partition cstore_update_table_p2 values less than (15)
);
create index on cstore_update_table(c1,c2)local;
insert into cstore_update_table select generate_series(0,10), generate_series(0,10);
select * from cstore_update_table order by c1;
--10 rows   1---10

update cstore_update_table set c2 = c2 + 10  where c2 > 5;
select * from cstore_update_table order by 1, 2;
--10 rows   1-5   16-20

drop table cstore_update_table;

--05--------------------------------------------------------------------
--update the non partition key column 
create table cstore_update_table
(
	c1 int ,
	c2 int ,
	c3 int
)with(orientation = column)
partition by range (c1, c2)
(
	partition cstore_update_table_p0 values less than (5,5),
	partition cstore_update_table_p1 values less than (10,10),
	partition cstore_update_table_p2 values less than (15,15)
);
create index on cstore_update_table(c1,c2)local;
insert into cstore_update_table select generate_series(0,10), generate_series(0,10), generate_series(0,10);

update cstore_update_table set c3 = 0 ;
select * from cstore_update_table order by 1, 2, 3;
-- 11 rows ,  all 0
drop table cstore_update_table;

--06--------------------------------------------------------------------
--update the partition key ,but no row movenment 
create table cstore_update_table
(
	c1 int ,
	c2 int ,
	c3 int
)with(orientation = column)
partition by range (c1, c2 )
(
	partition cstore_update_table_p0 values less than (10,0),
	partition cstore_update_table_p1 values less than (20,0),
	partition cstore_update_table_p2 values less than (30,0)
);
create index on cstore_update_table(c1,c2)local;
insert into cstore_update_table select generate_series(0,29), generate_series(0,29), generate_series(0,29);
update cstore_update_table set c3 = 0 where c1 < 10 ;
select * from cstore_update_table order by 1, 2, 3;
-- 30 rows ,   c1 = 0 in first 10 rows  
drop table cstore_update_table;
--07--------------------------------------------------------------------
--row movement ,but no interval partition been created
create table cstore_update_table
(
	c1 int ,
	c2 int ,
	c3 int
)with(orientation = column)
partition by range (c1, c2 )
(
	partition cstore_update_table_p0 values less than (10,0),
	partition cstore_update_table_p1 values less than (20,0),
	partition cstore_update_table_p2 values less than (30,0)
)enable row movement;
create index on cstore_update_table(c1, c2)local;
insert into cstore_update_table select generate_series(0,29), generate_series(0,29), generate_series(0,29);
update cstore_update_table set c3 = c3 + 5 where c1 < 10 ;
select * from cstore_update_table order by c1;
-- 30 rows ,   c1 = 0 in first 10 rows  
drop table cstore_update_table;
--08--------------------------------------------------------------------
--update the partition key , has row movement ,and a new interval partition been created because of row movement
create table cstore_update_table
(
	c1 int ,
	c2 int ,
	c3 int
)with(orientation = column)
partition by range (c1)
(
	partition cstore_update_table_p0 values less than (10),
	partition cstore_update_table_p1 values less than (20),
	partition cstore_update_table_p2 values less than (30)
)enable row movement;
create index on cstore_update_table(c1,c2)local;
insert into cstore_update_table select generate_series(0,29), generate_series(0,29), generate_series(0,29);
update cstore_update_table set c3 = c3 + 5 where c1 < 30 ;
select * from cstore_update_table order by c1;
-- 30 rows ,   c1 = 0 in first 10 rows  
drop table cstore_update_table;
--09--------------------------------------------------------------------
--update the partition key column , and set null to the column 
create table cstore_update_table
(
	c1 int ,
	c2 int ,
	c3 int
)with(orientation = column)
partition by range (c1, c2 )
(
	partition cstore_update_table_p0 values less than (10,0),
	partition cstore_update_table_p1 values less than (20,0),
	partition cstore_update_table_p2 values less than (30,0)
);
create index on cstore_update_table(c1,c2)local;
insert into cstore_update_table select generate_series(0,29), generate_series(0,29), generate_series(0,29);
update cstore_update_table set c3 = NULL where c3 < 30 ;
select * from cstore_update_table order by 1, 2, 3;
-- 30 rows ,   c1 = 0 in first 10 rows  
drop table cstore_update_table;

--12--------------------------------------------------------------------
--syntax test ,with clause
create table cstore_update_table1
(
	c1 int ,
	c2 int 
)with(orientation = column);

insert into cstore_update_table1 select generate_series(0,9), generate_series(0,9);

create table cstore_update_table
(
	c1 int ,
	c2 int ,
	c3 int
)with(orientation = column)
partition by range (c1 )
(
	partition cstore_update_table_p0 values less than (10),
	partition cstore_update_table_p1 values less than (20),
	partition cstore_update_table_p2 values less than (30)
)
;
create index on cstore_update_table(c1,c2)local;
insert into cstore_update_table select generate_series(0,29), generate_series(0,29), generate_series(0,29);
select * from cstore_update_table order by 1, 2, 3;


with tmp as (select c1 from cstore_update_table1)
update cstore_update_table set c2 = 0 where cstore_update_table.c1 in  (select u.c1 from tmp t , cstore_update_table u where t.c1 = u.c1);

select * from cstore_update_table order by 1, 2, 3;

drop table cstore_update_table;
drop table cstore_update_table1;

--16--------------------------------------------------------------------
--give a large value to create interval partition
create table cstore_update_table
(
	c1 int ,
	c2 int
)with(orientation = column)
partition by range (c1)
(
	partition cstore_update_table_p0 values less than (50),
	partition cstore_update_table_p1 values less than (100),
	partition cstore_update_table_p2 values less than (150)
)enable row movement
;

create index on cstore_update_table(c1,c2)local;

insert into cstore_update_table values (1,1);

update cstore_update_table set c2 = 999999999 where c1 = 1;
--error   the sequnece number is too large for range table
select c2  from cstore_update_table;

drop table cstore_update_table;

--17--------------------------------------------------------------------
--update with from clause
create table cstore_update_table
(
	c1 int ,
	c2 int
)with(orientation = column)
partition by range (c1)
(
	partition cstore_update_table_p0 values less than (50),
	partition cstore_update_table_p1 values less than (100),
	partition cstore_update_table_p2 values less than (150)
)enable row movement
;
create table cstore_update_table1
(
	c1 int ,
	c2 int
)with(orientation = column)
partition by range (c1)
(
	partition cstore_update_table1_p0 values less than (50),
	partition cstore_update_table1_p1 values less than (100),
	partition cstore_update_table1_p2 values less than (150)
)enable row movement
;

create index on cstore_update_table(c1,c2)local;

insert into cstore_update_table select generate_series(144,146),generate_series(4,6);
insert into cstore_update_table1 select generate_series(144,146),generate_series(7,9);

select * from cstore_update_table order by 1, 2;
select * from cstore_update_table1 order by 1, 2;
select * from cstore_update_table, cstore_update_table1 order by 1, 2, 3, 4;
update cstore_update_table set c2 = cstore_update_table1.c2 from cstore_update_table1 where cstore_update_table.c1 = cstore_update_table1.c1;
select *  from cstore_update_table order by 1, 2;

drop table cstore_update_table;
drop table cstore_update_table1;

--18--------------------------------------------------------------------
--not null constraint
create table cstore_update_table
(
	c1 int ,
	c2 int not null,
	c3 int
)with(orientation = column)
partition by range (c1,c2)
(
	partition cstore_update_table_p0 values less than (50,0),
	partition cstore_update_table_p1 values less than (100,0),
	partition cstore_update_table_p2 values less than (150,0)
)enable row movement
;

create index on cstore_update_table(c1,c2)local;

insert into cstore_update_table values(99,null);
--error
insert into cstore_update_table values(99,0);

update cstore_update_table set c2 = null where c2  = 0;
--error 
drop table cstore_update_table;

--19--------------------------------------------------------------------
create table cstore_update_table
(
	c1 int,
	c2 int
)with(orientation = column)
partition by range (c2)
(
	partition cstore_update_table_p0 values less than (10),
	partition cstore_update_table_p1 values less than (20),
	partition cstore_update_table_p2 values less than (30)
)enable row movement;

create index on cstore_update_table(c1,c2)local;
insert into cstore_update_table values (0, 5);
select * from cstore_update_table partition (cstore_update_table_p0);
update cstore_update_table set c2=15 where c2=5;
select * from cstore_update_table partition (cstore_update_table_p0);
select * from cstore_update_table partition (cstore_update_table_p1);

drop table cstore_update_table;


CREATE TABLE hw_partition_update_tt(c_id int NOT NULL,c_first varchar(16) NOT NULL,c_data varchar(500))
with(orientation = column)
partition by range(c_id)
(
	partition hw_partition_update_tt_p1 values less than (11),
	partition hw_partition_update_tt_p2 values less than (31)
) ENABLE ROW MOVEMENT;
insert into hw_partition_update_tt values(1,'aaaaaaaaaaaaaaaa','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa');
insert into hw_partition_update_tt values(3,'aaaaaaaaaaaaaaaa','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa');
insert into hw_partition_update_tt values(5,'aaaaaaaaaaaaaaaa','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa');
insert into hw_partition_update_tt values(7,'aaaaaaaaaaaaaaaa','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa');
insert into hw_partition_update_tt values(10,'aaaaaaaaaaaaaaaa','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa');
insert into hw_partition_update_tt values(15,'aaaaaaaaaaaaaaaa','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa');
insert into hw_partition_update_tt values(18,'aaaaaaaaaaaaaaaa','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa');
insert into hw_partition_update_tt values(24,'aaaaaaaaaaaaaaaa','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa');
insert into hw_partition_update_tt values(28,'aaaaaaaaaaaaaaaa','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa');

START TRANSACTION ISOLATION LEVEL READ COMMITTED;
UPDATE hw_partition_update_tt SET (c_first,c_data) = ('aaaaa','aaaaa') where c_id > 10 and c_id <= 20;
SELECT distinct c_first,c_data,count(c_first) FROM hw_partition_update_tt where c_id > 10 and c_id <= 20 GROUP BY c_first,c_data order by c_first;
-- /* skip the case for pgxc */
--SELECT RELNAME FROM PG_PARTITION WHERE PARENTID IN (SELECT OID FROM PG_CLASS WHERE RELNAME IN ('hw_partition_update_tt')) order by RELNAME;

--PRAGMA AUTONOMOUS_TRANSACTION;
--UPDATE hw_partition_update_tt SET (c_id,c_first,c_data) = (c_id + 10000,'aaaaa','aaaaa') where c_id > 20;
--SELECT RELNAME FROM PG_PARTITION WHERE PARENTID IN (SELECT OID FROM PG_CLASS WHERE RELNAME IN ('hw_partition_update_tt')) order by RELNAME;
--SELECT distinct c_first,c_data,count(c_first) FROM hw_partition_update_tt where c_id > 10000 GROUP BY c_first,c_data order by c_first;
--ROLLBACK;

SELECT distinct c_first,c_data,count(c_first) FROM hw_partition_update_tt where c_id > 10 and c_id <= 20 GROUP BY c_first,c_data order by c_first;
SELECT count(c_first) FROM hw_partition_update_tt;
COMMIT;

DROP TABLE hw_partition_update_tt;


create table test_update_rowmovement (a int, b int)
with(orientation = column)
partition by range (b)
(
	partition test_update_rowmovement_p1 values less than (10)
) enable row movement;

insert into test_update_rowmovement values (-1, 5);

update test_update_rowmovement set b=16;

drop table test_update_rowmovement;

