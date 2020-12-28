--
---- test merge partitions update global index 
--

--drop table and index
drop index if exists local_merge_table_index1;
drop index if exists global_merge_table_index1;
drop index if exists global_merge_table_index2;
drop table if exists merge_table;

create table merge_table
(
    INV_DATE_SK               integer               not null,
    INV_ITEM_SK               integer               not null,
    INV_WAREHOUSE_SK          integer               not null,
    INV_QUANTITY_ON_HAND      integer
)
partition by range(inv_date_sk)
(
    partition p1 values less than(10000),
    partition p2 values less than(20000),
    partition p3 values less than(30000),
    partition p4 values less than(40000),
    partition p5 values less than(50000),
    partition p6 values less than(60000),
    partition p7 values less than(maxvalue)
);
--succeed

insert into merge_table values (generate_series(1,100000), generate_series(1,100000), generate_series(1,100000));
--succeed 

create index local_merge_table_index1 on merge_table(INV_DATE_SK) local;

create index global_merge_table_index1 on merge_table(INV_ITEM_SK) global;

create index global_merge_table_index2 on merge_table(INV_WAREHOUSE_SK) global;

explain (costs off) select count(*) from merge_table where INV_DATE_SK < 10000;

select count(*) from merge_table where INV_DATE_SK < 10000;

explain (costs off) select count(*) from merge_table where INV_DATE_SK < 20000; 

select count(*) from merge_table where INV_DATE_SK < 20000;

explain (costs off) select count(*) from merge_table where INV_ITEM_SK < 10000;

select count(*) from merge_table where INV_ITEM_SK < 10000;

explain (costs off) select count(*) from merge_table where INV_ITEM_SK < 20000; 

select count(*) from merge_table where INV_ITEM_SK < 20000;

explain (costs off) select count(*) from merge_table where INV_WAREHOUSE_SK < 10000;

select count(*) from merge_table where INV_WAREHOUSE_SK < 10000;

explain (costs off) select count(*) from merge_table where INV_WAREHOUSE_SK < 20000; 

select count(*) from merge_table where INV_WAREHOUSE_SK < 20000;

select part.relname, part.indextblid, part.parttype, part.rangenum, part.intervalnum, part.partstrategy, part.relallvisible,
    part.reltoastrelid, part.partkey, part.interval, part.boundaries, part.reltuples
from pg_class class, pg_partition part, pg_index ind where class.relname = 'merge_table' and ind.indrelid = class.oid and part.parentid = ind.indrelid
order by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11;

alter table merge_table merge partitions p1, p2 into partition p0 update global index;

explain (costs off) select count(*) from merge_table where INV_DATE_SK < 10000;

select count(*) from merge_table where INV_DATE_SK < 10000;

explain (costs off) select count(*) from merge_table where INV_DATE_SK < 20000; 

select count(*) from merge_table where INV_DATE_SK < 20000;

explain (costs off) select count(*) from merge_table where INV_ITEM_SK < 10000;

select count(*) from merge_table where INV_ITEM_SK < 10000;

explain (costs off) select count(*) from merge_table where INV_ITEM_SK < 20000; 

select count(*) from merge_table where INV_ITEM_SK < 20000;

explain (costs off) select count(*) from merge_table where INV_WAREHOUSE_SK < 10000;

select count(*) from merge_table where INV_WAREHOUSE_SK < 10000;

explain (costs off) select count(*) from merge_table where INV_WAREHOUSE_SK < 20000; 

select count(*) from merge_table where INV_WAREHOUSE_SK < 20000;

select part.relname, part.indextblid, part.parttype, part.rangenum, part.intervalnum, part.partstrategy, part.relallvisible,
    part.reltoastrelid, part.partkey, part.interval, part.boundaries, part.reltuples
from pg_class class, pg_partition part, pg_index ind where class.relname = 'merge_table' and ind.indrelid = class.oid and part.parentid = ind.indrelid
order by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11;

alter table merge_table merge partitions p3, p4, p5 into partition p5 update global index;

explain (costs off) select count(*) from merge_table where INV_DATE_SK < 10000;

select count(*) from merge_table where INV_DATE_SK < 10000;

explain (costs off) select count(*) from merge_table where INV_DATE_SK < 20000; 

select count(*) from merge_table where INV_DATE_SK < 20000;

explain (costs off) select count(*) from merge_table where INV_ITEM_SK < 10000;

select count(*) from merge_table where INV_ITEM_SK < 10000;

explain (costs off) select count(*) from merge_table where INV_ITEM_SK < 20000; 

select count(*) from merge_table where INV_ITEM_SK < 20000;

explain (costs off) select count(*) from merge_table where INV_WAREHOUSE_SK < 10000;

select count(*) from merge_table where INV_WAREHOUSE_SK < 10000;

explain (costs off) select count(*) from merge_table where INV_WAREHOUSE_SK < 20000; 

select count(*) from merge_table where INV_WAREHOUSE_SK < 20000;

select part.relname, part.indextblid, part.parttype, part.rangenum, part.intervalnum, part.partstrategy, part.relallvisible,
    part.reltoastrelid, part.partkey, part.interval, part.boundaries, part.reltuples
from pg_class class, pg_partition part, pg_index ind where class.relname = 'merge_table' and ind.indrelid = class.oid and part.parentid = ind.indrelid
order by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11;

vacuum analyze;

explain (costs off) select count(*) from merge_table where INV_DATE_SK < 10000;

select count(*) from merge_table where INV_DATE_SK < 10000;

explain (costs off) select count(*) from merge_table where INV_DATE_SK < 20000; 

select count(*) from merge_table where INV_DATE_SK < 20000;

explain (costs off) select count(*) from merge_table where INV_ITEM_SK < 10000;

select count(*) from merge_table where INV_ITEM_SK < 10000;

explain (costs off) select count(*) from merge_table where INV_ITEM_SK < 20000; 

select count(*) from merge_table where INV_ITEM_SK < 20000;

explain (costs off) select count(*) from merge_table where INV_WAREHOUSE_SK < 10000;

select count(*) from merge_table where INV_WAREHOUSE_SK < 10000;

explain (costs off) select count(*) from merge_table where INV_WAREHOUSE_SK < 20000; 

select count(*) from merge_table where INV_WAREHOUSE_SK < 20000;

select part.relname, part.indextblid, part.parttype, part.rangenum, part.intervalnum, part.partstrategy, part.relallvisible,
    part.reltoastrelid, part.partkey, part.interval, part.boundaries, part.reltuples
from pg_class class, pg_partition part, pg_index ind where class.relname = 'merge_table' and ind.indrelid = class.oid and part.parentid = ind.indrelid
order by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11;

alter table merge_table merge partitions p6, p7 into partition p7;

explain (costs off) select count(*) from merge_table where INV_DATE_SK < 10000;

select count(*) from merge_table where INV_DATE_SK < 10000;

explain (costs off) select count(*) from merge_table where INV_DATE_SK < 20000; 

select count(*) from merge_table where INV_DATE_SK < 20000;

explain (costs off) select count(*) from merge_table where INV_ITEM_SK < 10000;

select count(*) from merge_table where INV_ITEM_SK < 10000;

explain (costs off) select count(*) from merge_table where INV_ITEM_SK < 20000; 

select count(*) from merge_table where INV_ITEM_SK < 20000;

explain (costs off) select count(*) from merge_table where INV_WAREHOUSE_SK < 10000;

select count(*) from merge_table where INV_WAREHOUSE_SK < 10000;

explain (costs off) select count(*) from merge_table where INV_WAREHOUSE_SK < 20000; 

select count(*) from merge_table where INV_WAREHOUSE_SK < 20000;

select part.relname, part.indextblid, part.parttype, part.rangenum, part.intervalnum, part.partstrategy, part.relallvisible,
    part.reltoastrelid, part.partkey, part.interval, part.boundaries, part.reltuples
from pg_class class, pg_partition part, pg_index ind where class.relname = 'merge_table' and ind.indrelid = class.oid and part.parentid = ind.indrelid
order by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11;

--clean
drop index if exists local_merge_table_index1;
drop index if exists global_merge_table_index1;
drop index if exists global_merge_table_index2;
drop table if exists merge_table;

