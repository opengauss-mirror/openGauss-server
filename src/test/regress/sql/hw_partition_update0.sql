--01--------------------------------------------------------------------
--syntax test ,  base update command
create table update_table
(
	c1 int ,
	c2 int
)
partition by range (c1)
(
	partition update_table_p0 values less than (50),
	partition update_table_p1 values less than (100),
	partition update_table_p2 values less than (150)
);
create index on update_table(c1,c2)local;
insert into update_table select generate_series(0,10), generate_series(0,10);
select * from update_table order by c1;
--10 rows   1---10

update update_table set c2 = c2 + 10;
select * from update_table order by c1;
--10 rows   10---20
drop table update_table;

--02--------------------------------------------------------------------
--where clause
create table update_table
(
	c1 int ,
	c2 int
)
partition by range (c1)
(
	partition update_table_p0 values less than (50),
	partition update_table_p1 values less than (100),
	partition update_table_p2 values less than (150)
);
create index on update_table(c1,c2)local;
insert into update_table select generate_series(0,10), generate_series(0,10);
select * from update_table order by c1;
--10 rows   1---10

update update_table set c2 = c2 + 10  where c2 > 5;
select * from update_table order by 1, 2;
--10 rows   1-5   16-20

drop table update_table;
--03--------------------------------------------------------------------
--WHERE CURRENT OF  clause
--gaussdb not support DECLARE cursor
--create table update_table
--(
--	c1 int ,
--	c2 int
--)
----partition by range (c1)
----INTERVAL (10)
----(
----	partition update_table_p0 values less than (50),
----	partition update_table_p1 values less than (100),
----	partition update_table_p2 values less than (150)
----);
--insert into update_table select generate_series(0,10),1;
--create index on update_table(c1,c2)local;
--
--start transaction ;
--DECLARE update_cursor SCROLL CURSOR with HOLD  FOR (select * from update_table) ;
--FETCH FORWARD 5 FROM update_cursor;
----  5 rows 
--
--
--update update_table set c1 = 0 where  CURRENT OF update_cursor;
--
--select * from update_table;
--
--
--close update_cursor;
--drop table update_table;
--commit;

--04--------------------------------------------------------------------
--returning clause

create table update_table
(
	c1 int ,
	c2 int
)
partition by range (c1)
(
	partition update_table_p0 values less than (50),
	partition update_table_p1 values less than (100),
	partition update_table_p2 values less than (150)
);
create index on update_table(c1,c2)local;
insert into update_table select generate_series(0,10), generate_series(0,10);

update update_table set c2 = 0 returning c2;
drop table update_table;

--10 rows     all 0
--05--------------------------------------------------------------------
--update the non partition key column 
create table update_table
(
	c1 int ,
	c2 int ,
	c3 int
)
partition by range (c1, c2)
(
	partition update_table_p0 values less than (50,50),
	partition update_table_p1 values less than (100,100),
	partition update_table_p2 values less than (150,150)
);
create index on update_table(c1,c2)local;
insert into update_table select generate_series(0,10), generate_series(0,10), generate_series(0,10);

update update_table set c3 = 0 ;
select * from update_table order by 1, 2, 3;
-- 11 rows ,  all 0
drop table update_table;

--06--------------------------------------------------------------------
--update the partition key ,but no row movenment 
create table update_table
(
	c1 int ,
	c2 int ,
	c3 int
)
partition by range (c1, c2 )
(
	partition update_table_p0 values less than (10,0),
	partition update_table_p1 values less than (20,0),
	partition update_table_p2 values less than (30,0)
);
create index on update_table(c1,c2)local;
insert into update_table select generate_series(0,29), generate_series(0,29), generate_series(0,29);
update update_table set c3 = 0 where c1 < 10 ;
select * from update_table order by 1, 2, 3;
-- 30 rows ,   c1 = 0 in first 10 rows  
drop table update_table;
--07--------------------------------------------------------------------
--row movement ,but no interval partition been created
create table update_table
(
	c1 int ,
	c2 int ,
	c3 int
)
partition by range (c1, c2 )
(
	partition update_table_p0 values less than (10,0),
	partition update_table_p1 values less than (20,0),
	partition update_table_p2 values less than (30,0)
)enable row movement;
create index on update_table(c1, c2)local;
insert into update_table select generate_series(0,29), generate_series(0,29), generate_series(0,29);
update update_table set c3 = c3 + 5 where c1 < 10 ;
select * from update_table order by c1;
-- 30 rows ,   c1 = 0 in first 10 rows  
drop table update_table;
--08--------------------------------------------------------------------
--update the partition key , has row movement ,and a new interval partition been created because of row movement
create table update_table
(
	c1 int ,
	c2 int ,
	c3 int
)
partition by range (c1)
(
	partition update_table_p0 values less than (10),
	partition update_table_p1 values less than (20),
	partition update_table_p2 values less than (30)
)enable row movement;
;
create index on update_table(c1,c2)local;
insert into update_table select generate_series(0,29), generate_series(0,29), generate_series(0,29);
update update_table set c3 = c3 + 5 where c1 < 30 ;
select * from update_table order by c1;
-- 30 rows ,   c1 = 0 in first 10 rows  
drop table update_table;
--09--------------------------------------------------------------------
--update the partition key column , and set null to the column 
create table update_table
(
	c1 int ,
	c2 int ,
	c3 int
)
partition by range (c1, c2 )
(
	partition update_table_p0 values less than (10,0),
	partition update_table_p1 values less than (20,0),
	partition update_table_p2 values less than (30,0)
);
create index on update_table(c1,c2)local;
insert into update_table select generate_series(0,29), generate_series(0,29), generate_series(0,29);
update update_table set c3 = NULL where c3 < 30 ;
select * from update_table order by 1, 2, 3;
-- 30 rows ,   c1 = 0 in first 10 rows  
drop table update_table;
--10--------------------------------------------------------------------
--create a before update trigger on partitioned table , trigger insert a values to target table ,and cause a interval 
--partition been created , 
--the real update action will cause a row movement to created partition in the trigger .
create table update_table
(
	c1 int ,
	c2 int ,
	c3 int
)
partition by range (c1)
(
	partition update_table_p0 values less than (10),
	partition update_table_p1 values less than (20),
	partition update_table_p2 values less than (40)
)enable row movement;
create index on update_table(c1,c2)local;
insert into update_table select generate_series(0,29), generate_series(0,29), generate_series(0,29);


CREATE FUNCTION update_trigger() RETURNS trigger AS $update_trigger$
    BEGIN
       insert into update_table values (31,0);
       raise NOTICE 'insert values to update_table';
       RETURN NULL ;
    END;
$update_trigger$ LANGUAGE plpgsql;

create trigger update_trigger before update on update_table
 EXECUTE PROCEDURE update_trigger();



update update_table set c3 = 32 where c1 = 28 ;
select * from update_table order by 1, 2, 3;


drop trigger update_trigger on update_table;

drop function update_trigger();
drop table update_table;

