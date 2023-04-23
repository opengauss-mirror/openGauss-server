--
---- test cluster on global index
--

--drop table and index
drop index if exists global_inventory_table_02_index1;
drop index if exists global_inventory_table_02_index2;
drop table if exists inventory_table_02;

create table inventory_table_02
(
    inv_date_sk               integer               not null,
    inv_item_sk               numeric               not null,
    inv_warehouse_sk          integer               not null,
    inv_quantity_on_hand      integer
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

insert into inventory_table_02 values (generate_series(1,100000), random(), generate_series(1,100000));
--succeed 

vacuum analyze inventory_table_02;

select true from (select correlation from pg_stats where tablename='inventory_table_02' and attname='inv_date_sk') where correlation = 1;
select true from (select correlation from pg_stats where tablename='inventory_table_02' and attname='inv_item_sk') where correlation = 1;

select true from (select ctid,inv_date_sk,inv_item_sk from inventory_table_02 where inv_item_sk=(select min(inv_item_sk) from inventory_table_02)) where ctid = '(0,1)';
select true from (select ctid,inv_date_sk,inv_item_sk from inventory_table_02 where inv_date_sk=(select min(inv_date_sk) from inventory_table_02)) where ctid = '(0,1)';

create index global_inventory_table_02_index1 on inventory_table_02(inv_date_sk) global;

create index global_inventory_table_02_index2 on inventory_table_02(inv_item_sk) global;

cluster inventory_table_02 using global_inventory_table_02_index2;

select true from (select ctid,inv_date_sk,inv_item_sk from inventory_table_02 where inv_item_sk=(select min(inv_item_sk) from inventory_table_02)) where ctid = '(0,1)';
select true from (select ctid,inv_date_sk,inv_item_sk from inventory_table_02 where inv_date_sk=(select min(inv_date_sk) from inventory_table_02)) where ctid = '(0,1)';

vacuum analyze inventory_table_02;

select true from (select correlation from pg_stats where tablename='inventory_table_02' and attname='inv_date_sk') where correlation = 1;
select true from (select correlation from pg_stats where tablename='inventory_table_02' and attname='inv_item_sk') where correlation = 1;

cluster inventory_table_02 using global_inventory_table_02_index1;

select true from (select ctid,inv_date_sk,inv_item_sk from inventory_table_02 where inv_item_sk=(select min(inv_item_sk) from inventory_table_02)) where ctid = '(0,1)';
select true from (select ctid,inv_date_sk,inv_item_sk from inventory_table_02 where inv_date_sk=(select min(inv_date_sk) from inventory_table_02)) where ctid = '(0,1)';

vacuum analyze inventory_table_02;

select true from (select correlation from pg_stats where tablename='inventory_table_02' and attname='inv_date_sk') where correlation = 1;
select true from (select correlation from pg_stats where tablename='inventory_table_02' and attname='inv_item_sk') where correlation = 1;

select part.relname, part.parttype, part.rangenum, part.intervalnum, part.partstrategy, part.partkey, part.interval, part.boundaries, part.reltuples
from pg_class class, pg_partition part, pg_index ind where class.relname = 'inventory_table_02' and ind.indrelid = class.oid and part.parentid = ind.indexrelid
order by 1, 2, 3, 4, 5, 6, 7, 8, 9;

select part.relname, part.parttype, part.rangenum, part.intervalnum, part.partstrategy, part.partkey, part.interval, part.boundaries, part.reltuples
from pg_class class, pg_partition part, pg_index ind where class.relname = 'inventory_table_02' and ind.indrelid = class.oid and part.parentid = ind.indrelid
order by 1, 2, 3, 4, 5, 6, 7, 8, 9;

cluster inventory_table_02 using global_inventory_table_02_index2;

\dS+ inventory_table_02;

--clean
drop index if exists global_inventory_table_02_index1;
drop index if exists global_inventory_table_02_index2;
drop table if exists inventory_table_02;
