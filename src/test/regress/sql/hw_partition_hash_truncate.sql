--1.function
--a
create table hash_partition_truncate_table
(
	c1 int ,
	c2 int
)
partition by hash (c1)
(
	partition p1,
	partition p2
);
insert into hash_partition_truncate_table values(1),(2),(3),(4);
select count(*) from hash_partition_truncate_table;
--4 rows
alter table hash_partition_truncate_table truncate partition p1;
select count(*) from hash_partition_truncate_table;
--2 rows
alter table hash_partition_truncate_table truncate partition for (1);
alter table hash_partition_truncate_table truncate partition for (3);
select count(*) from hash_partition_truncate_table;
--0 rows
truncate table hash_partition_truncate_table;
select count(*) from hash_partition_truncate_table;
--0 rows
drop table hash_partition_truncate_table;

--b
create table hash_partition_truncate_table
(
	c1 int ,
	c2 int
)
partition by hash (c1)
(
	partition p1,
	partition p2
);
insert into hash_partition_truncate_table values(1),(2),(3),(4);
alter table hash_partition_truncate_table truncate partition for (1), truncate partition p6;
select count(*) from hash_partition_truncate_table;
-- 4 rows
alter table hash_partition_truncate_table truncate partition for (1), truncate partition for (3);
select count(*) from hash_partition_truncate_table;
-- 0 rows
drop table hash_partition_truncate_table;



--2.sytax test
create table hash_partition_truncate_table
(
	c1 int ,
	c2 int
)
partition by hash (c1)
(
	partition p1,
	partition p2
);
alter table hash_partition_truncate_table truncate p1;
--error
alter table hash_partition_truncate_table truncate partition;
--error
drop table hash_partition_truncate_table;



--3.index
create table hash_partition_truncate_table
(
	c1 int ,
	c2 int
)
partition by hash (c1)
(
	partition p1,
	partition p2
);
create index on hash_partition_truncate_table(c1,c2) local;
insert into hash_partition_truncate_table values(1),(2),(3),(4);
select count(*) from hash_partition_truncate_table;
--4 rows
alter table hash_partition_truncate_table truncate partition for (3);
select count(*) from hash_partition_truncate_table;
--2 rows
alter table hash_partition_truncate_table truncate partition for (2);
select count(*) from hash_partition_truncate_table;
--0 rows
drop table hash_partition_truncate_table;

--4.toast table partition
create table hash_partition_truncate_table
(
	c1 int ,
	c2 text
)
partition by hash (c1)
(
	partition p1,
	partition p2
);
create index on hash_partition_truncate_table(c1,c2) local;
insert into hash_partition_truncate_table values(1,'0'),(2,'0'),(3,'0'),(4,'0'),(5,'0');
select count(*) from hash_partition_truncate_table;
--5 rows
alter table hash_partition_truncate_table truncate partition for (3);
select count(*) from hash_partition_truncate_table;
--3 rows
alter table hash_partition_truncate_table truncate partition for (2);
select count(*) from hash_partition_truncate_table;
--0 rows
drop table hash_partition_truncate_table;

--5.transaction
--truncate command and create table in same transaction
start transaction ;
create table hash_partition_truncate_table
(
	c1 int ,
	c2 text
)
partition by hash (c1)
(
	partition p1,
	partition p2
);
create index on hash_partition_truncate_table(c1,c2) local;
insert into hash_partition_truncate_table values(1,'0'),(2,'0'),(3,'0'),(4,'0'),(5,'0');
select count(*) from hash_partition_truncate_table;
--5 rows
alter table hash_partition_truncate_table truncate partition for (3);
select count(*) from hash_partition_truncate_table;
--3 rows
rollback;
select count(*) from hash_partition_truncate_table;
--error

start transaction ;
create table hash_partition_truncate_table
(
	c1 int ,
	c2 text
)
partition by hash (c1)
(
	partition p1,
	partition p2
);
create index on hash_partition_truncate_table(c1,c2) local;
insert into hash_partition_truncate_table values(1,'0'),(2,'0'),(3,'0'),(4,'0'),(5,'0');
select count(*) from hash_partition_truncate_table;
--5 rows
alter table hash_partition_truncate_table truncate partition for (3);
select count(*) from hash_partition_truncate_table;
--3 rows
commit;
select count(*) from hash_partition_truncate_table;
--3 rows
drop table hash_partition_truncate_table;

--truncate same partition in a command
create table hash_partition_truncate_table
(
	c1 int ,
	c2 text
)
partition by hash (c1)
(
	partition p1,
	partition p2
);
insert into hash_partition_truncate_table values(1,'0'),(2,'0'),(3,'0'),(4,'0'),(5,'0');
select count(*) from hash_partition_truncate_table;
--5 rows
alter table hash_partition_truncate_table truncate partition for (4), truncate partition for(3);
select count(*) from hash_partition_truncate_table;
--3 rows
drop table hash_partition_truncate_table;



--4. global index
--drop table and index
drop table if exists exchange_table;
drop table if exists alter_table_hash;

create table alter_table_hash
(
    INV_DATE_SK               integer               not null,
    INV_ITEM_SK               integer               not null,
    INV_WAREHOUSE_SK          integer               not null,
    INV_QUANTITY_ON_HAND      integer
)
partition by hash(inv_date_sk)
(
    partition p0,
    partition p1,
    partition p2
);
--succeed

insert into alter_table_hash values (generate_series(1000,5000,1000),generate_series(1000,5000,1000),generate_series(1000,5000,1000));
insert into alter_table_hash values (generate_series(10000,18000,2000),generate_series(10000,18000,2000),generate_series(10000,18000,2000));
insert into alter_table_hash values (generate_series(20000,28000,2000),generate_series(20000,28000,2000),generate_series(20000,28000,2000));
--succeed

create index hash_local_alter_table_hash_index1 on alter_table_hash(INV_DATE_SK) local;

create index hash_hash_global_alter_table_hash_index2 on alter_table_hash(INV_ITEM_SK) global;

create index global_alter_table_hash_index2 on alter_table_hash(INV_WAREHOUSE_SK) global;

explain (costs off) select count(*) from alter_table_hash where INV_DATE_SK < 10000;

select count(*) from alter_table_hash where INV_DATE_SK < 10000;

explain (costs off) select count(*) from alter_table_hash where INV_DATE_SK < 20000;

select count(*) from alter_table_hash where INV_DATE_SK < 20000;

explain (costs off) select count(*) from alter_table_hash where INV_ITEM_SK < 10000;

select count(*) from alter_table_hash where INV_ITEM_SK < 10000;

explain (costs off) select count(*) from alter_table_hash where INV_ITEM_SK < 10000;

select count(*) from alter_table_hash where INV_ITEM_SK < 20000;

explain (costs off) select count(*) from alter_table_hash where INV_WAREHOUSE_SK < 10000;

select count(*) from alter_table_hash where INV_WAREHOUSE_SK < 10000;

explain (costs off) select count(*) from alter_table_hash where INV_WAREHOUSE_SK < 20000;

select count(*) from alter_table_hash where INV_WAREHOUSE_SK < 20000;

explain (costs off) select count(*) from alter_table_hash where INV_WAREHOUSE_SK < 30000;

select count(*) from alter_table_hash where INV_WAREHOUSE_SK < 30000;

select part.relname, part.indextblid, part.parttype, part.rangenum, part.intervalnum, part.partstrategy, part.relallvisible,
    part.reltoastrelid, part.partkey, part.interval, part.boundaries, part.reltuples
from pg_class class, pg_partition part, pg_index ind where class.relname = 'alter_table_hash' and ind.indrelid = class.oid and part.parentid = ind.indrelid
order by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11;

ALTER TABLE alter_table_hash TRUNCATE partition for (3) update global index;

select part.relname, part.indextblid, part.parttype, part.rangenum, part.intervalnum, part.partstrategy, part.relallvisible,
    part.reltoastrelid, part.partkey, part.interval, part.boundaries, part.reltuples
from pg_class class, pg_partition part, pg_index ind where class.relname = 'alter_table_hash' and ind.indrelid = class.oid and part.parentid = ind.indrelid
order by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11;

explain (costs off) select count(*) from alter_table_hash where INV_DATE_SK < 10000;

select count(*) from alter_table_hash where INV_DATE_SK < 10000;

explain (costs off) select count(*) from alter_table_hash where INV_DATE_SK < 20000;

select count(*) from alter_table_hash where INV_DATE_SK < 20000;

explain (costs off) select count(*) from alter_table_hash where INV_ITEM_SK < 10000;

select count(*) from alter_table_hash where INV_ITEM_SK < 10000;

explain (costs off) select count(*) from alter_table_hash where INV_ITEM_SK < 10000;

select count(*) from alter_table_hash where INV_ITEM_SK < 20000;

explain (costs off) select count(*) from alter_table_hash where INV_WAREHOUSE_SK < 10000;

select count(*) from alter_table_hash where INV_WAREHOUSE_SK < 10000;

explain (costs off) select count(*) from alter_table_hash where INV_WAREHOUSE_SK < 20000;

select count(*) from alter_table_hash where INV_WAREHOUSE_SK < 20000;

explain (costs off) select count(*) from alter_table_hash where INV_WAREHOUSE_SK < 30000;

select count(*) from alter_table_hash where INV_WAREHOUSE_SK < 30000;

--clean
drop index if exists hash_local_alter_table_hash_index1;
drop index if exists hash_hash_global_alter_table_hash_index2;
drop index if exists global_alter_table_hash_index2;
drop table if exists alter_table_hash;



--5. Ustore
--a
create table hash_partition_truncate_table
(
	c1 int ,
	c2 int
) WITH (STORAGE_TYPE = USTORE, init_td=32)
partition by hash (c1)
(
	partition p1,
	partition p2
);
insert into hash_partition_truncate_table values(1),(2),(3),(4);
select count(*) from hash_partition_truncate_table;
--4 rows
alter table hash_partition_truncate_table truncate partition p1;
select count(*) from hash_partition_truncate_table;
--2 rows
alter table hash_partition_truncate_table truncate partition for (1);
alter table hash_partition_truncate_table truncate partition for (3);
select count(*) from hash_partition_truncate_table;
--0 rows
truncate table hash_partition_truncate_table;
select count(*) from hash_partition_truncate_table;
--0 rows
drop table hash_partition_truncate_table;

--b
create table hash_partition_truncate_table
(
	c1 int ,
	c2 int
)
partition by hash (c1)
(
	partition p1,
	partition p2
);
insert into hash_partition_truncate_table values(1),(2),(3),(4);
alter table hash_partition_truncate_table truncate partition for (1), truncate partition p6;
select count(*) from hash_partition_truncate_table;
-- 4 rows
alter table hash_partition_truncate_table truncate partition for (1), truncate partition for (3);
select count(*) from hash_partition_truncate_table;
-- 0 rows
drop table hash_partition_truncate_table;

