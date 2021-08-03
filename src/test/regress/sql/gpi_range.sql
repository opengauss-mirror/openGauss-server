--
---- test reange partitioned global index
--

--drop table and index
drop index if exists local_gpi_range_table_index1;
drop index if exists global_gpi_range_table_index1;
drop index if exists global_gpi_range_table_index2;
drop table if exists gpi_range_table;
drop table if exists gpi_range_like_table;

create table gpi_range_table
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
    partition p2 values less than(20000)
);
--succeed

insert into gpi_range_table values (generate_series(1,10000), generate_series(1,10000), generate_series(1,10000));
--succeed

create index local_gpi_range_table_index1 on gpi_range_table(INV_DATE_SK) local;

create index global_gpi_range_table_index1 on gpi_range_table(INV_ITEM_SK) global;

create index global_gpi_range_table_index2 on gpi_range_table(INV_WAREHOUSE_SK) global;

create table gpi_range_like_table(like tb1 INCLUDING INDEXES);
--error

create table gpi_range_like_table(like tb1 INCLUDING ALL);
--succeed

\dS+ gpi_range_like_table;

--to select all index object
select part.relname, part.parttype, part.rangenum, part.intervalnum, part.partstrategy, part.relallvisible, part.reltoastrelid, part.partkey, part.interval, part.boundaries
from pg_class class, pg_partition part, pg_index ind where class.relname = 'gpi_range_table' and ind.indrelid = class.oid and part.parentid = ind.indrelid
order by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10;

explain (costs off) select count(*) from gpi_range_table where INV_DATE_SK < 10000;
select count(*) from gpi_range_table where INV_DATE_SK < 10000;
explain (costs off) select count(*) from gpi_range_table where INV_ITEM_SK < 10000;
select count(*) from gpi_range_table where INV_ITEM_SK < 10000;
explain (costs off) select count(*) from gpi_range_table where INV_WAREHOUSE_SK < 10000;
select count(*) from gpi_range_table where INV_WAREHOUSE_SK < 10000;

analyze verify fast local_gpi_range_table_index1;
analyze verify fast global_gpi_range_table_index1;
analyze verify fast global_gpi_range_table_index2;

--clean
drop index if exists local_gpi_range_table_index1;
drop index if exists global_gpi_range_table_index1;
drop index if exists global_gpi_range_table_index2;
drop table if exists gpi_range_table;
drop table if exists gpi_range_like_table;

