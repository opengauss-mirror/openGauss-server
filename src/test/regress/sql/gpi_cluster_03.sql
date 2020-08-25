--
---- test cluster on global index
--

--drop table and index
drop index if exists global_inventory_table_index1;
drop index if exists global_inventory_table_index2;
drop index if exists inventory_table_index1;
drop index if exists inventory_table_index2;
drop table if exists inventory_table;

CREATE TABLE inventory_table
(
    INV_DATE_SK               INTEGER               NOT NULL,
    INV_ITEM_SK               INTEGER               NOT NULL,
    INV_WAREHOUSE_SK          INTEGER               NOT NULL,
    INV_QUANTITY_ON_HAND      INTEGER
)
PARTITION BY RANGE(INV_DATE_SK)
(
    PARTITION P1 VALUES LESS THAN(10000),
    PARTITION P2 VALUES LESS THAN(20000),
    PARTITION P3 VALUES LESS THAN(30000),
    PARTITION P4 VALUES LESS THAN(40000),
    PARTITION P5 VALUES LESS THAN(50000),
    PARTITION P6 VALUES LESS THAN(60000),
    PARTITION P7 VALUES LESS THAN(MAXVALUE)
);
--succeed

insert into inventory_table values (generate_series(1,40000), generate_series(1,40000), generate_series(1,40000));
--succeed

create index inventory_table_index1 on inventory_table(INV_DATE_SK) local;
--succeed

create index inventory_table_index2 on inventory_table(INV_DATE_SK) local;
--succeed

create index global_inventory_table_index1 on inventory_table(INV_ITEM_SK) global;
--succeed

create index global_inventory_table_index2 on inventory_table(INV_ITEM_SK) global;
--succeed

\dS+ inventory_table;

cluster inventory_table using global_inventory_table_index1;

\dS+ inventory_table;

cluster inventory_table PARTITION(P3) USING inventory_table_index1;

\dS+ inventory_table;

cluster inventory_table using global_inventory_table_index1;

\dS+ inventory_table;

alter table inventory_table cluster on inventory_table_index1;

\dS+ inventory_table;

alter table inventory_table cluster on global_inventory_table_index1;

\dS+ inventory_table;

alter table inventory_table cluster on global_inventory_table_index2;

\dS+ inventory_table;

cluster inventory_table;

\dS+ inventory_table;

cluster inventory_table using global_inventory_table_index1;

\dS+ inventory_table;

cluster inventory_table;

\dS+ inventory_table;

SELECT conname FROM pg_constraint WHERE conrelid = 'inventory_table'::regclass
ORDER BY 1;

SELECT relname, relkind,
    EXISTS(SELECT 1 FROM pg_class WHERE oid = c.reltoastrelid) AS hastoast
FROM pg_class c WHERE relname = 'inventory_table' ORDER BY relname;

-- Verify that indisclustered is correctly set
SELECT pg_class.relname FROM pg_index, pg_class, pg_class AS pg_class_2
WHERE pg_class.oid=indexrelid
    AND indrelid=pg_class_2.oid
    AND pg_class_2.relname = 'inventory_table'
    AND indisclustered;

-- Try changing indisclustered
ALTER TABLE inventory_table CLUSTER ON global_inventory_table_index2;
SELECT pg_class.relname FROM pg_index, pg_class, pg_class AS pg_class_2
WHERE pg_class.oid=indexrelid
    AND indrelid=pg_class_2.oid
    AND pg_class_2.relname = 'inventory_table'
    AND indisclustered;

-- Try turning off all clustering
ALTER TABLE inventory_table SET WITHOUT cluster;
SELECT pg_class.relname FROM pg_index, pg_class, pg_class AS pg_class_2
WHERE pg_class.oid=indexrelid
    AND indrelid=pg_class_2.oid
    AND pg_class_2.relname = 'inventory_table'
    AND indisclustered;

\parallel on
cluster inventory_table using global_inventory_table_index1;
cluster inventory_table using global_inventory_table_index1;
cluster inventory_table using global_inventory_table_index1;
cluster inventory_table using global_inventory_table_index1;
cluster inventory_table using global_inventory_table_index1;
cluster inventory_table using global_inventory_table_index1;
\parallel off

\parallel on
cluster inventory_table using global_inventory_table_index1;
cluster inventory_table using global_inventory_table_index2;
cluster inventory_table using global_inventory_table_index1;
cluster inventory_table using global_inventory_table_index2;
cluster inventory_table using global_inventory_table_index1;
cluster inventory_table using global_inventory_table_index2;
\parallel off

\parallel on
cluster inventory_table using global_inventory_table_index1;
cluster inventory_table;
cluster inventory_table using global_inventory_table_index1;
cluster inventory_table;
cluster inventory_table using global_inventory_table_index1;
cluster inventory_table;
\parallel off

\parallel on
cluster inventory_table using global_inventory_table_index1;
cluster inventory_table;
cluster inventory_table using global_inventory_table_index1;
cluster inventory_table;
cluster inventory_table using global_inventory_table_index1;
cluster inventory_table;
\parallel off

--clean
drop index if exists global_inventory_table_index1;
drop index if exists global_inventory_table_index2;
drop index if exists inventory_table_index1;
drop index if exists inventory_table_index2;
drop table if exists inventory_table;
