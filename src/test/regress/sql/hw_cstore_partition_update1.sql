--update the partition key column , and set null to the column 
create table cstore_update1_table
(
	c1 int ,
	c2 int ,
	c3 int
)with(orientation = column)
partition by range (c1, c2 )
(
	partition cstore_update1_table_p0 values less than (10,0),
	partition cstore_update1_table_p1 values less than (20,0),
	partition cstore_update1_table_p2 values less than (30,0)
);
create index on cstore_update1_table(c1,c2)local;
insert into cstore_update1_table select generate_series(0,29), generate_series(0,29), generate_series(0,29);
update cstore_update1_table set c3 = NULL where c3 < 30 ;
select * from cstore_update1_table order by 1, 2, 3;
-- 30 rows ,   c1 = 0 in first 10 rows  
drop table cstore_update1_table;

--12--------------------------------------------------------------------
--syntax test ,with clause
create table cstore_update1_table1
(
	c1 int ,
	c2 int 
)with(orientation = column);

insert into cstore_update1_table1 select generate_series(0,9), generate_series(0,9);

create table cstore_update1_table
(
	c1 int ,
	c2 int ,
	c3 int
)with(orientation = column)
partition by range (c1 )
(
	partition cstore_update1_table_p0 values less than (10),
	partition cstore_update1_table_p1 values less than (20),
	partition cstore_update1_table_p2 values less than (30)
)
;
create index on cstore_update1_table(c1,c2)local;
insert into cstore_update1_table select generate_series(0,29), generate_series(0,29), generate_series(0,29);
select * from cstore_update1_table order by 1, 2, 3;


with tmp as (select c1 from cstore_update1_table1)
update cstore_update1_table set c2 = 0 where cstore_update1_table.c1 in  (select u.c1 from tmp t , cstore_update1_table u where t.c1 = u.c1);

select * from cstore_update1_table order by 1, 2, 3;

drop table cstore_update1_table;
drop table cstore_update1_table1;

--16--------------------------------------------------------------------
--give a large value to create interval partition
create table cstore_update1_table
(
	c1 int ,
	c2 int
)with(orientation = column)
partition by range (c1)
(
	partition cstore_update1_table_p0 values less than (50),
	partition cstore_update1_table_p1 values less than (100),
	partition cstore_update1_table_p2 values less than (150)
)enable row movement
;

create index on cstore_update1_table(c1,c2)local;

insert into cstore_update1_table values (1,1);

update cstore_update1_table set c2 = 999999999 where c1 = 1;
--error   the sequnece number is too large for range table
select c2  from cstore_update1_table;

drop table cstore_update1_table;

--17--------------------------------------------------------------------
--update with from clause
create table cstore_update1_table
(
	c1 int ,
	c2 int
)with(orientation = column)
partition by range (c1)
(
	partition cstore_update1_table_p0 values less than (50),
	partition cstore_update1_table_p1 values less than (100),
	partition cstore_update1_table_p2 values less than (150)
)enable row movement
;
create table cstore_update1_table1
(
	c1 int ,
	c2 int
)with(orientation = column)
partition by range (c1)
(
	partition cstore_update1_table1_p0 values less than (50),
	partition cstore_update1_table1_p1 values less than (100),
	partition cstore_update1_table1_p2 values less than (150)
)enable row movement
;

create index on cstore_update1_table(c1,c2)local;

insert into cstore_update1_table select generate_series(144,146),generate_series(4,6);
insert into cstore_update1_table1 select generate_series(144,146),generate_series(7,9);

select * from cstore_update1_table order by 1, 2;
select * from cstore_update1_table1 order by 1, 2;
select * from cstore_update1_table, cstore_update1_table1 order by 1, 2, 3, 4;
update cstore_update1_table set c2 = cstore_update1_table1.c2 from cstore_update1_table1 where cstore_update1_table.c1 = cstore_update1_table1.c1;
select *  from cstore_update1_table order by 1, 2;

drop table cstore_update1_table;
drop table cstore_update1_table1;
