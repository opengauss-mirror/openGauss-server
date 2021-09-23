--1.function
--a
create table list_partition_truncate_table
(
	c1 int ,
	c2 int
)
partition by list (c1)
(
	partition p1 values (1000),
	partition p2 values (2000),
	partition p3 values (3000),
	partition p4 values (4000),
	partition p5 values (5000)
);
insert into list_partition_truncate_table values(1000),(2000),(3000),(4000),(5000);
select count(*) from list_partition_truncate_table;
--5 rows
alter table list_partition_truncate_table truncate partition p1;
select count(*) from list_partition_truncate_table;
--4 rows
alter table list_partition_truncate_table truncate partition for (2000);
select count(*) from list_partition_truncate_table;
--3 rows
truncate table list_partition_truncate_table;
select count(*) from list_partition_truncate_table;
-- 0 rows
drop table list_partition_truncate_table;

--b
create table list_partition_truncate_table
(
	c1 int ,
	c2 int
)
partition by list (c1)
(
	partition p1 values (1000),
	partition p2 values (2000),
	partition p3 values (3000),
	partition p4 values (4000),
	partition p5 values (5000)
);
insert into list_partition_truncate_table values(1000),(2000),(3000),(4000),(5000);
alter table list_partition_truncate_table truncate partition for (6000);
--error
alter table list_partition_truncate_table truncate partition for (5000), truncate partition for (6000);
select count(*) from list_partition_truncate_table;
-- 5 rows
alter table list_partition_truncate_table truncate partition for (5000), truncate partition p6;
select count(*) from list_partition_truncate_table;
-- 5 rows
alter table list_partition_truncate_table truncate partition for (5000), truncate partition p2;
select count(*) from list_partition_truncate_table;
-- 3 rows
drop table list_partition_truncate_table;



--2.sytax test
create table list_partition_truncate_table
(
	c1 int ,
	c2 int
)
partition by list (c1)
(
	partition p1 values (1000),
	partition p2 values (2000),
	partition p3 values (3000),
	partition p4 values (4000),
	partition p5 values (5000)
);
alter table list_partition_truncate_table truncate p1;
--error
alter table list_partition_truncate_table truncate partition;
--error
drop table list_partition_truncate_table;



--3.index
create table list_partition_truncate_table
(
	c1 int ,
	c2 int
)
partition by list (c1)
(
	partition p1 values (1000),
	partition p2 values (2000),
	partition p3 values (3000),
	partition p4 values (4000),
	partition p5 values (5000)
);
create index on list_partition_truncate_table(c1,c2) local;
insert into list_partition_truncate_table values(1000),(2000),(3000),(4000),(5000);
select count(*) from list_partition_truncate_table;
--5 rows
alter table list_partition_truncate_table truncate partition p1;
select count(*) from list_partition_truncate_table;
--4 rows
alter table list_partition_truncate_table truncate partition for (2000);
select count(*) from list_partition_truncate_table;
--3 rows
drop table list_partition_truncate_table;

--4.toast table partition
create table list_partition_truncate_table
(
	c1 int ,
	c2 text
)
partition by list (c1)
(
	partition p1 values (1000),
	partition p2 values (2000),
	partition p3 values (3000),
	partition p4 values (4000),
	partition p5 values (5000)
);
create index on list_partition_truncate_table(c1,c2) local;
insert into list_partition_truncate_table values(1000,'0'),(2000,'0'),(3000,'0'),(4000,'0'),(5000,'0');
select count(*) from list_partition_truncate_table;
--5 rows
alter table list_partition_truncate_table truncate partition p1;
select count(*) from list_partition_truncate_table;
--4 rows
alter table list_partition_truncate_table truncate partition for (2000);
select count(*) from list_partition_truncate_table;
--3 rows
drop table list_partition_truncate_table;

--5.transaction
--truncate command and create table in same transaction
start transaction ;
create table list_partition_truncate_table
(
	c1 int ,
	c2 text
)
partition by list (c1)
(
	partition p1 values (1000),
	partition p2 values (2000),
	partition p3 values (3000),
	partition p4 values (4000),
	partition p5 values (5000)
);
create index on list_partition_truncate_table(c1,c2) local;
insert into list_partition_truncate_table values(1000,'0'),(2000,'0'),(3000,'0'),(4000,'0'),(5000,'0');
select count(*) from list_partition_truncate_table;
--5 rows
alter table list_partition_truncate_table truncate partition p1;
select count(*) from list_partition_truncate_table;
--4 rows
rollback;
select count(*) from list_partition_truncate_table;
--error

start transaction ;
create table list_partition_truncate_table
(
	c1 int ,
	c2 text
)
partition by list (c1)
(
	partition p1 values (1000),
	partition p2 values (2000),
	partition p3 values (3000),
	partition p4 values (4000),
	partition p5 values (5000)
);
create index on list_partition_truncate_table(c1,c2) local;
insert into list_partition_truncate_table values(1000,'0'),(2000,'0'),(3000,'0'),(4000,'0'),(5000,'0');
select count(*) from list_partition_truncate_table;
--5 rows
alter table list_partition_truncate_table truncate partition p1;
select count(*) from list_partition_truncate_table;
--4 rows
commit;
select count(*) from list_partition_truncate_table;
--4 rows
drop table list_partition_truncate_table;


--truncate partiton and drop parttion in same transaction
create table list_partition_truncate_table
(
	c1 int ,
	c2 text
)
partition by list (c1)
(
	partition p1 values (1000),
	partition p2 values (2000),
	partition p3 values (3000),
	partition p4 values (4000),
	partition p5 values (5000)
);
create index on list_partition_truncate_table(c1,c2) local;
insert into list_partition_truncate_table values(1000,'0'),(2000,'0'),(3000,'0'),(4000,'0'),(5000,'0');
select count(*) from list_partition_truncate_table;
--5 rows
start transaction ;
alter table list_partition_truncate_table truncate partition p1;
select count(*) from list_partition_truncate_table;
--4 rows
alter table list_partition_truncate_table drop partition p1;
rollback;
select count(*) from list_partition_truncate_table;
--5 rows

start transaction ;
alter table list_partition_truncate_table truncate partition p1;
select count(*) from list_partition_truncate_table;
--4 rows
commit;
select count(*) from list_partition_truncate_table;
--4 rows
drop table list_partition_truncate_table;


--truncate partiton and add parttion in same transaction
create table list_partition_truncate_table
(
	c1 int ,
	c2 text
)
partition by list (c1)
(
	partition p1 values (1000),
	partition p2 values (2000),
	partition p3 values (3000),
	partition p4 values (4000),
	partition p5 values (5000)
);
create index on list_partition_truncate_table(c1,c2) local;
insert into list_partition_truncate_table values(1000,'0'),(2000,'0'),(3000,'0'),(4000,'0'),(5000,'0');
select count(*) from list_partition_truncate_table;
--5 rows
start transaction ;
alter table list_partition_truncate_table add partition p6 values (6000);
insert into list_partition_truncate_table values(6000,'0');
select count(*) from list_partition_truncate_table;
--6 rows
alter table list_partition_truncate_table truncate partition p6;
select count(*) from list_partition_truncate_table;
--5 rows
rollback;
select count(*) from list_partition_truncate_table;
--5 rows
drop table list_partition_truncate_table;

create table list_partition_truncate_table
(
	c1 int ,
	c2 text
)
partition by list (c1)
(
	partition p1 values (1000),
	partition p2 values (2000),
	partition p3 values (3000),
	partition p4 values (4000),
	partition p5 values (5000)
);
create index on list_partition_truncate_table(c1,c2) local;
insert into list_partition_truncate_table values(1000,'0'),(2000,'0'),(3000,'0'),(4000,'0'),(5000,'0');
select count(*) from list_partition_truncate_table;
--5 rows
start transaction ;
alter table list_partition_truncate_table add partition p6 values (6000);
insert into list_partition_truncate_table values(6000,'0');
select count(*) from list_partition_truncate_table;
--6 rows
alter table list_partition_truncate_table truncate partition p6;
select count(*) from list_partition_truncate_table;
--5 rows
commit;
select count(*) from list_partition_truncate_table;
--5 rows
drop table list_partition_truncate_table;


--truncate same partition in a command
create table list_partition_truncate_table
(
	c1 int ,
	c2 text
)
partition by list (c1)
(
	partition p1 values (1000),
	partition p2 values (2000),
	partition p3 values (3000),
	partition p4 values (4000),
	partition p5 values (5000)
);
insert into list_partition_truncate_table values(1000,'0'),(2000,'0'),(3000,'0'),(4000,'0'),(5000,'0');
select count(*) from list_partition_truncate_table;
--5 rows
alter table list_partition_truncate_table truncate partition p1, truncate partition for(1000);
select count(*) from list_partition_truncate_table;
--4 rows
truncate list_partition_truncate_table;

start transaction;
alter table list_partition_truncate_table add partition p6 values (6000);
insert into list_partition_truncate_table values(6000,'0');
alter table list_partition_truncate_table truncate partition p6, truncate partition for(6000);
select count(*) from list_partition_truncate_table partition(p6);
--0 rows
rollback;
select count(*) from list_partition_truncate_table partition(p6);
--error

start transaction;
alter table list_partition_truncate_table add partition p6 values (6000);
insert into list_partition_truncate_table values(6000,'0');
alter table list_partition_truncate_table truncate partition p6, truncate partition for(6000);
select count(*) from list_partition_truncate_table partition(p6);
--0 rows
commit;
select count(*) from list_partition_truncate_table partition(p6);
--0 rows
drop table list_partition_truncate_table;



--4. global index
--drop table and index
drop table if exists alter_table;

create table alter_table
(
    INV_DATE_SK               integer               not null,
    INV_ITEM_SK               integer               not null,
    INV_WAREHOUSE_SK          integer               not null,
    INV_QUANTITY_ON_HAND      integer
)
partition by list(inv_date_sk)
(
    partition p0 values (1000,2000,3000,4000,5000),
    partition p1 values (10000,12000,14000,16000,18000),
    partition p2 values (20000,22000,24000,26000,28000)
);
--succeed

insert into alter_table values (generate_series(1000,5000,1000),generate_series(1000,5000,1000),generate_series(1000,5000,1000));
insert into alter_table values (generate_series(10000,18000,2000),generate_series(10000,18000,2000),generate_series(10000,18000,2000));
insert into alter_table values (generate_series(20000,28000,2000),generate_series(20000,28000,2000),generate_series(20000,28000,2000));
--succeed 

create index local_alter_table_index1 on alter_table(INV_DATE_SK) local;

create index global_alter_table_index1 on alter_table(INV_ITEM_SK) global;

create index global_alter_table_index2 on alter_table(INV_WAREHOUSE_SK) global;

explain (costs off) select count(*) from alter_table where INV_DATE_SK < 10000;

select count(*) from alter_table where INV_DATE_SK < 10000;

explain (costs off) select count(*) from alter_table where INV_DATE_SK < 20000;

select count(*) from alter_table where INV_DATE_SK < 20000;

explain (costs off) select count(*) from alter_table where INV_ITEM_SK < 10000;

select count(*) from alter_table where INV_ITEM_SK < 10000;

explain (costs off) select count(*) from alter_table where INV_ITEM_SK < 10000;

select count(*) from alter_table where INV_ITEM_SK < 20000;

explain (costs off) select count(*) from alter_table where INV_WAREHOUSE_SK < 10000;

select count(*) from alter_table where INV_WAREHOUSE_SK < 10000;

explain (costs off) select count(*) from alter_table where INV_WAREHOUSE_SK < 20000;

select count(*) from alter_table where INV_WAREHOUSE_SK < 20000;

explain (costs off) select count(*) from alter_table where INV_WAREHOUSE_SK < 30000;

select count(*) from alter_table where INV_WAREHOUSE_SK < 30000;

select part.relname, part.indextblid, part.parttype, part.rangenum, part.intervalnum, part.partstrategy, part.relallvisible,
    part.reltoastrelid, part.partkey, part.interval, part.boundaries, part.reltuples
from pg_class class, pg_partition part, pg_index ind where class.relname = 'alter_table' and ind.indrelid = class.oid and part.parentid = ind.indrelid
order by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11;

ALTER TABLE alter_table TRUNCATE partition p2 update global index;

select part.relname, part.indextblid, part.parttype, part.rangenum, part.intervalnum, part.partstrategy, part.relallvisible,
    part.reltoastrelid, part.partkey, part.interval, part.boundaries, part.reltuples
from pg_class class, pg_partition part, pg_index ind where class.relname = 'alter_table' and ind.indrelid = class.oid and part.parentid = ind.indrelid
order by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11;

explain (costs off) select count(*) from alter_table where INV_DATE_SK < 10000;

select count(*) from alter_table where INV_DATE_SK < 10000;

explain (costs off) select count(*) from alter_table where INV_DATE_SK < 20000;

select count(*) from alter_table where INV_DATE_SK < 20000;

explain (costs off) select count(*) from alter_table where INV_ITEM_SK < 10000;

select count(*) from alter_table where INV_ITEM_SK < 10000;

explain (costs off) select count(*) from alter_table where INV_ITEM_SK < 20000;

select count(*) from alter_table where INV_ITEM_SK < 20000;

explain (costs off) select count(*) from alter_table where INV_WAREHOUSE_SK < 10000;

select count(*) from alter_table where INV_WAREHOUSE_SK < 10000;

explain (costs off) select count(*) from alter_table where INV_WAREHOUSE_SK < 20000;

select count(*) from alter_table where INV_WAREHOUSE_SK < 20000;

explain (costs off) select count(*) from alter_table where INV_WAREHOUSE_SK < 30000;

select count(*) from alter_table where INV_WAREHOUSE_SK < 30000;

--clean
drop index if exists local_alter_table_index1;
drop index if exists global_alter_table_index1;
drop index if exists global_alter_table_index2;
drop table if exists alter_table;



--5. Ustore
--a
create table list_partition_truncate_table
(
	c1 int ,
	c2 int
) WITH (STORAGE_TYPE = USTORE, init_td=32)
partition by list (c1)
(
	partition p1 values (1000),
	partition p2 values (2000),
	partition p3 values (3000),
	partition p4 values (4000),
	partition p5 values (5000)
);
insert into list_partition_truncate_table values(1000),(2000),(3000),(4000),(5000);
select count(*) from list_partition_truncate_table;
--5 rows
alter table list_partition_truncate_table truncate partition p1;
select count(*) from list_partition_truncate_table;
--4 rows
alter table list_partition_truncate_table truncate partition for (2000);
select count(*) from list_partition_truncate_table;
--3 rows
truncate table list_partition_truncate_table;
select count(*) from list_partition_truncate_table;
-- 0 rows
drop table list_partition_truncate_table;

--b
create table list_partition_truncate_table
(
	c1 int ,
	c2 int
)
partition by list (c1)
(
	partition p1 values (1000),
	partition p2 values (2000),
	partition p3 values (3000),
	partition p4 values (4000),
	partition p5 values (5000)
);
insert into list_partition_truncate_table values(1000),(2000),(3000),(4000),(5000);
alter table list_partition_truncate_table truncate partition for (6000);
--error
alter table list_partition_truncate_table truncate partition for (5000), truncate partition for (6000);
select count(*) from list_partition_truncate_table;
-- 5 rows
alter table list_partition_truncate_table truncate partition for (5000), truncate partition p6;
select count(*) from list_partition_truncate_table;
-- 5 rows
alter table list_partition_truncate_table truncate partition for (5000), truncate partition p2;
select count(*) from list_partition_truncate_table;
-- 3 rows
drop table list_partition_truncate_table;


