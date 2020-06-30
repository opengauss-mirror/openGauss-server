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
