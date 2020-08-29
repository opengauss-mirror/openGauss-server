--
---- test cluster on global index
--

--drop table and index
drop index if exists global_inventory_table_01_index1;
drop index if exists global_inventory_table_01_index2;
drop table if exists inventory_table_01;

create table inventory_table_01
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

insert into inventory_table_01 values ( 1, 0.205546309705824,  1 );
insert into inventory_table_01 values ( 2, 0.635608241427690,  2 );
insert into inventory_table_01 values ( 3, 0.868086945265532,  3 );
insert into inventory_table_01 values ( 4, 0.513698554132134,  4 );
insert into inventory_table_01 values ( 5, 0.172576570883393,  5 );
insert into inventory_table_01 values ( 6, 0.537533693015575,  6 );
insert into inventory_table_01 values ( 7, 0.967260550707579,  7 );
insert into inventory_table_01 values ( 8, 0.515049927402288,  8 );
insert into inventory_table_01 values ( 9, 0.583286581560969,  9 );
insert into inventory_table_01 values (10, 0.313009374774992, 10 );

vacuum analyze inventory_table_01;

select ctid,* from inventory_table_01;

create index global_inventory_table_01_index1 on inventory_table_01(inv_date_sk) global;

create index global_inventory_table_01_index2 on inventory_table_01(inv_item_sk) global;

cluster inventory_table_01 using global_inventory_table_01_index2;

vacuum analyze inventory_table_01;

select ctid,* from inventory_table_01;

cluster inventory_table_01 using global_inventory_table_01_index1;

vacuum analyze inventory_table_01;

select ctid,* from inventory_table_01;

cluster inventory_table_01 using global_inventory_table_01_index2;

\dS+ inventory_table_01;

vacuum analyze inventory_table_01;

select ctid,* from inventory_table_01;

--clean
drop index if exists global_inventory_table_01_index1;
drop index if exists global_inventory_table_01_index2;
drop table if exists inventory_table_01;
