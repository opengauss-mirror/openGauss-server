--drop table and index
drop index if exists local_exchange_table_index1;
drop table if exists exchange_table;
drop index if exists local_alter_table_index1;
drop index if exists global_alter_table_index1;
drop index if exists global_alter_table_index2;
drop table if exists alter_table;

create table alter_table
(
    INV_DATE_SK               integer               not null,
    INV_ITEM_SK               integer               not null,
    INV_WAREHOUSE_SK          integer               not null,
    INV_QUANTITY_ON_HAND      integer
)
partition by range(inv_date_sk)
(
    partition p0 values less than(5000),
    partition p1 values less than(10000),
    partition p2 values less than(20000),
    partition p3 values less than(30000),
    partition p4 values less than(40000),
    partition p5 values less than(50000),
    partition p6 values less than(60000),
    partition p7 values less than(100001)
);
--succeed

insert into alter_table values (generate_series(1,100000), generate_series(1,100000), generate_series(1,100000));
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

explain (costs off) select count(*) from alter_table where INV_ITEM_SK < 20000;

select count(*) from alter_table where INV_ITEM_SK < 20000;

explain (costs off) select count(*) from alter_table where INV_WAREHOUSE_SK < 10000;

select count(*) from alter_table where INV_WAREHOUSE_SK < 10000;

explain (costs off) select count(*) from alter_table where INV_WAREHOUSE_SK < 20000;

select count(*) from alter_table where INV_WAREHOUSE_SK < 20000;

select part.relname, part.indextblid, part.parttype, part.rangenum, part.intervalnum, part.partstrategy, part.relallvisible,
    part.reltoastrelid, part.partkey, part.interval, part.boundaries, part.reltuples
from pg_class class, pg_partition part, pg_index ind where class.relname = 'alter_table' and ind.indrelid = class.oid and part.parentid = ind.indrelid
order by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11;

ALTER TABLE alter_table add partition p8 values less than(500000);

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

select part.relname, part.indextblid, part.parttype, part.rangenum, part.intervalnum, part.partstrategy, part.relallvisible,
    part.reltoastrelid, part.partkey, part.interval, part.boundaries, part.reltuples
from pg_class class, pg_partition part, pg_index ind where class.relname = 'alter_table' and ind.indrelid = class.oid and part.parentid = ind.indrelid
order by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11;

ALTER TABLE alter_table drop partition p1;

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

select part.relname, part.indextblid, part.parttype, part.rangenum, part.intervalnum, part.partstrategy, part.relallvisible,
    part.reltoastrelid, part.partkey, part.interval, part.boundaries, part.reltuples
from pg_class class, pg_partition part, pg_index ind where class.relname = 'alter_table' and ind.indrelid = class.oid and part.parentid = ind.indrelid
order by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11;

ALTER TABLE alter_table TRUNCATE partition p2;

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

select part.relname, part.indextblid, part.parttype, part.rangenum, part.intervalnum, part.partstrategy, part.relallvisible,
    part.reltoastrelid, part.partkey, part.interval, part.boundaries, part.reltuples
from pg_class class, pg_partition part, pg_index ind where class.relname = 'alter_table' and ind.indrelid = class.oid and part.parentid = ind.indrelid
order by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11;

ALTER TABLE alter_table SPLIT PARTITION p0 AT (2500) INTO (PARTITION p10, PARTITION p11);

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

select part.relname, part.indextblid, part.parttype, part.rangenum, part.intervalnum, part.partstrategy, part.relallvisible,
    part.reltoastrelid, part.partkey, part.interval, part.boundaries, part.reltuples
from pg_class class, pg_partition part, pg_index ind where class.relname = 'alter_table' and ind.indrelid = class.oid and part.parentid = ind.indrelid
order by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11;

ALTER TABLE alter_table MERGE PARTITIONS p10, p11 INTO PARTITION p1;

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

create table exchange_table
(
    INV_DATE_SK               integer               not null,
    INV_ITEM_SK               integer               not null,
    INV_WAREHOUSE_SK          integer               not null,
    INV_QUANTITY_ON_HAND      integer
);
--succeed

insert into exchange_table values (generate_series(1,4000), generate_series(1,4000), generate_series(1,4000));
--succeed 

create index local_exchange_table_index1 on exchange_table(INV_DATE_SK);

select part.relname, part.indextblid, part.parttype, part.rangenum, part.intervalnum, part.partstrategy, part.relallvisible,
    part.reltoastrelid, part.partkey, part.interval, part.boundaries, part.reltuples
from pg_class class, pg_partition part, pg_index ind where class.relname = 'alter_table' and ind.indrelid = class.oid and part.parentid = ind.indrelid
order by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11;

ALTER TABLE alter_table EXCHANGE PARTITION FOR(2500) WITH TABLE exchange_table;

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

vacuum freeze pg_partition;

--clean
drop index if exists local_exchange_table_index1;
drop table if exists exchange_table;
drop index if exists local_alter_table_index1;
drop index if exists global_alter_table_index1;
drop index if exists global_alter_table_index2;
drop table if exists alter_table;
