
DROP SCHEMA hw_subpartition_size CASCADE;
CREATE SCHEMA hw_subpartition_size;
SET CURRENT_SCHEMA TO hw_subpartition_size;

---- test function:
-- 1. pg_table_size
-- 2. pg_indexes_size
-- 3. pg_total_relation_size
-- 4. pg_relation_size
-- 5. pg_partition_size
-- 6. pg_partition_indexes_size

CREATE TABLE range_list_sales
(
    product_id     INT4,
    customer_id    INT4,
    time_id        DATE,
    channel_id     CHAR(1),
    type_id        INT4,
    quantity_sold  NUMERIC(3),
    amount_sold    NUMERIC(10,2)
)
PARTITION BY RANGE (customer_id) SUBPARTITION BY LIST (channel_id)
(
    PARTITION customer1 VALUES LESS THAN (2000)
    (
        SUBPARTITION customer1_channel1 VALUES ('0', '1', '2'),
        SUBPARTITION customer1_channel2 VALUES ('3', '4', '5'),
        SUBPARTITION customer1_channel3 VALUES ('6', '7', '8'),
        SUBPARTITION customer1_channel4 VALUES ('9')
    ),
    PARTITION customer2 VALUES LESS THAN (5000)
    (
        SUBPARTITION customer2_channel1 VALUES ('0', '1', '2', '3', '4'),
        SUBPARTITION customer2_channel2 VALUES (DEFAULT)
    ),
    PARTITION customer3 VALUES LESS THAN (8000),
    PARTITION customer4 VALUES LESS THAN (12000)
    (
        SUBPARTITION customer4_channel1 VALUES ('0', '1', '2', '3', '4', '5', '6', '7', '8', '9')
    )
);
INSERT INTO range_list_sales SELECT generate_series(1,10000),
                                    generate_series(1,10000),
                                    date_pli('2008-01-01', generate_series(1,10000)),
                                    generate_series(1,10000)%10,
                                    generate_series(1,10000)%10,
                                    generate_series(1,10000)%1000,
                                    generate_series(1,10000);
CREATE INDEX range_list_sales_idx1 ON range_list_sales(product_id) LOCAL;
CREATE INDEX range_list_sales_idx2 ON range_list_sales(channel_id, type_id) LOCAL;
CREATE INDEX range_list_sales_idx3 ON range_list_sales(time_id) GLOBAL;
ALTER TABLE range_list_sales ADD CONSTRAINT range_list_sales_pkey PRIMARY KEY (customer_id);

-- 1. pg_table_size
SELECT pg_table_size('range_list_sales');

SELECT pg_table_size('range_list_sales_idx1');
SELECT pg_table_size('range_list_sales_idx2');
SELECT pg_table_size('range_list_sales_idx3');
SELECT pg_table_size('range_list_sales_pkey');

--2. pg_indexes_size
SELECT pg_indexes_size('range_list_sales');
-- should be equal
SELECT pg_indexes_size('range_list_sales') =
    pg_table_size('range_list_sales_idx1') +
    pg_table_size('range_list_sales_idx2') +
    pg_table_size('range_list_sales_idx3') +
    pg_table_size('range_list_sales_pkey');

-- 3. pg_total_relation_size
SELECT pg_total_relation_size('range_list_sales');
-- should be equal
SELECT pg_total_relation_size('range_list_sales') =
    pg_table_size('range_list_sales') + pg_indexes_size('range_list_sales');

-- 4. pg_relation_size
SELECT pg_relation_size('range_list_sales');
SELECT pg_relation_size('range_list_sales', 'main');

SELECT pg_relation_size('range_list_sales_idx1');
SELECT pg_relation_size('range_list_sales_idx2');
SELECT pg_relation_size('range_list_sales_idx3');
SELECT pg_relation_size('range_list_sales_pkey');

-- 5. pg_partition_size
SELECT pg_partition_size('range_list_sales', 'customer1');
SELECT pg_partition_size('range_list_sales', 'customer2_channel1');
-- should be equal
SELECT pg_table_size('range_list_sales') =
    pg_partition_size('range_list_sales', 'customer1') +
    pg_partition_size('range_list_sales', 'customer2') +
    pg_partition_size('range_list_sales', 'customer3') +
    pg_partition_size('range_list_sales', 'customer4');
-- should be equal
SELECT pg_partition_size('range_list_sales', 'customer1') =
    pg_partition_size('range_list_sales', 'customer1_channel1') +
    pg_partition_size('range_list_sales', 'customer1_channel2') +
    pg_partition_size('range_list_sales', 'customer1_channel3') +
    pg_partition_size('range_list_sales', 'customer1_channel4');
-- (oid, oid)
SELECT (SELECT pg_partition_size('range_list_sales'::regclass::oid, oid) from pg_partition where parentid = 'range_list_sales'::regclass and relname='customer1')
    =
    (SELECT sum(pg_partition_size('range_list_sales'::regclass::oid, oid)) from pg_partition where parentid in (SELECT oid from pg_partition where parentid = 'range_list_sales'::regclass and relname='customer1'));
-- should be equal
SELECT pg_partition_size('range_list_sales', 'customer4') = pg_partition_size('range_list_sales', 'customer4_channel1');

-- invalid parameter, error
SELECT pg_partition_size('range_list_sales', 'parttemp');

create table test_pg_partition_size2 (a int);
create table test_pg_partition_size3 (a int, b int)
partition by range (a)
subpartition by hash (b)
(
    partition p1 values less than (4)
        (subpartition sp1)
);
--ERROR regular table
select pg_partition_size('test_pg_partition_size2', 'customer1')>0;
select pg_partition_size('test_pg_partition_size2', 'customer1_channel1')>0;
select pg_partition_size('test_pg_partition_size2'::regclass::oid, oid)>0 from pg_partition where parentid = 'range_list_sales'::regclass and relname='customer1';
select pg_partition_size('test_pg_partition_size2'::regclass::oid, oid)>0 from pg_partition where parentid = 
    (select oid from pg_partition where parentid = 'range_list_sales'::regclass and relname='customer1') and relname = 'customer1_channel1';
--ERROR partitioned table does not match the partition
select pg_partition_size('test_pg_partition_size3', 'range_list_sales')>0;
select pg_partition_size('test_pg_partition_size3', 'customer1')>0;
select pg_partition_size('test_pg_partition_size3', 'customer1_channel1')>0;
select pg_partition_size('test_pg_partition_size3'::regclass::oid, 'range_list_sales'::regclass::oid)>0;
select pg_partition_size('test_pg_partition_size3'::regclass::oid, oid)>0 from pg_partition where parentid = 'range_list_sales'::regclass and relname='customer1';
select pg_partition_size('test_pg_partition_size3'::regclass::oid, oid)>0 from pg_partition where parentid = 
    (select oid from pg_partition where parentid = 'range_list_sales'::regclass and relname='customer1') and relname = 'customer1_channel1';

-- 6. pg_partition_indexes_size
SELECT pg_partition_indexes_size('range_list_sales', 'customer1');
SELECT pg_partition_indexes_size('range_list_sales', 'customer2_channel1');
-- should be equal, all_index = glable_index + local_index
SELECT pg_indexes_size('range_list_sales') =
    (pg_table_size('range_list_sales_idx3') + pg_table_size('range_list_sales_pkey')) +
    (pg_partition_indexes_size('range_list_sales', 'customer1') +
     pg_partition_indexes_size('range_list_sales', 'customer2') +
     pg_partition_indexes_size('range_list_sales', 'customer3') +
     pg_partition_indexes_size('range_list_sales', 'customer4'));
-- should be equal
SELECT pg_partition_indexes_size('range_list_sales', 'customer1') =
    pg_partition_indexes_size('range_list_sales', 'customer1_channel1') +
    pg_partition_indexes_size('range_list_sales', 'customer1_channel2') +
    pg_partition_indexes_size('range_list_sales', 'customer1_channel3') +
    pg_partition_indexes_size('range_list_sales', 'customer1_channel4');
-- (oid, oid)
SELECT (SELECT pg_partition_indexes_size('range_list_sales'::regclass::oid, oid) from pg_partition where parentid = 'range_list_sales'::regclass and relname='customer1')
    =
    (SELECT sum(pg_partition_indexes_size('range_list_sales'::regclass::oid, oid)) from pg_partition where parentid in (SELECT oid from pg_partition where parentid = 'range_list_sales'::regclass and relname='customer1'));
-- should be equal
SELECT pg_partition_indexes_size('range_list_sales', 'customer4') = pg_partition_indexes_size('range_list_sales', 'customer4_channel1');

CREATE INDEX test_pg_partition_size2_idx ON test_pg_partition_size2(a);
CREATE INDEX test_pg_partition_size3_idx ON test_pg_partition_size3(a) LOCAL;
--ERROR regular table
select pg_partition_indexes_size('test_pg_partition_size2', 'customer1')>0;
select pg_partition_indexes_size('test_pg_partition_size2', 'customer1_channel1')>0;
select pg_partition_indexes_size('test_pg_partition_size2'::regclass::oid, oid)>0 from pg_partition where parentid = 'range_list_sales'::regclass and relname='customer1';
select pg_partition_indexes_size('test_pg_partition_size2'::regclass::oid, oid)>0 from pg_partition where parentid = 
    (select oid from pg_partition where parentid = 'range_list_sales'::regclass and relname='customer1') and relname = 'customer1_channel1';
--ERROR partitioned table does not match the partition
select pg_partition_indexes_size('test_pg_partition_size3', 'range_list_sales')>0;
select pg_partition_indexes_size('test_pg_partition_size3', 'customer1')>0;
select pg_partition_indexes_size('test_pg_partition_size3', 'customer1_channel1')>0;
select pg_partition_indexes_size('test_pg_partition_size3'::regclass::oid, 'range_list_sales'::regclass::oid)>0;
select pg_partition_indexes_size('test_pg_partition_size3'::regclass::oid, oid)>0 from pg_partition where parentid = 'range_list_sales'::regclass and relname='customer1';
select pg_partition_indexes_size('test_pg_partition_size3'::regclass::oid, oid)>0 from pg_partition where parentid = 
    (select oid from pg_partition where parentid = 'range_list_sales'::regclass and relname='customer1') and relname = 'customer1_channel1';

-- finish, clean
DROP TABLE test_pg_partition_size2;
DROP TABLE test_pg_partition_size3;
DROP TABLE range_list_sales;
DROP SCHEMA hw_subpartition_size CASCADE;
RESET CURRENT_SCHEMA;

