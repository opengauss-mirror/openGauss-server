DROP SCHEMA hw_partition_add_drop_partition CASCADE;
CREATE SCHEMA hw_partition_add_drop_partition;
SET CURRENT_SCHEMA TO hw_partition_add_drop_partition;

--
----range table----
--
--prepare
CREATE TABLE range_sales
(
    product_id     INT4 NOT NULL,
    customer_id    INT4 PRIMARY KEY,
    time_id        DATE,
    channel_id     CHAR(1),
    type_id        INT4,
    quantity_sold  NUMERIC(3),
    amount_sold    NUMERIC(10,2)
)
PARTITION BY RANGE (time_id)
(
    PARTITION time_2008 VALUES LESS THAN ('2009-01-01'),
    PARTITION time_2009 VALUES LESS THAN ('2010-01-01'),
    PARTITION time_2010 VALUES LESS THAN ('2011-01-01'),
    PARTITION time_2011 VALUES LESS THAN ('2012-01-01')
);
INSERT INTO range_sales SELECT generate_series(1,1000),
                               generate_series(1,1000),
                               date_pli('2008-01-01', generate_series(1,1000)),
                               generate_series(1,1000)%10,
                               generate_series(1,1000)%10,
                               generate_series(1,1000)%1000,
                               generate_series(1,1000);
CREATE INDEX range_sales_idx ON range_sales(product_id) LOCAL;

--check for add partition
--fail, can not add subpartition on no-subpartitioned table
ALTER TABLE range_sales ADD PARTITION time_temp1 VALUES LESS THAN ('2013-01-01')
    (
        SUBPARTITION time_temp1_part1 VALUES LESS THAN (200),
        SUBPARTITION time_temp1_part2 VALUES LESS THAN (500),
        SUBPARTITION time_temp1_part3 VALUES LESS THAN (800),
        SUBPARTITION time_temp1_part4 VALUES LESS THAN (1200)
    );
--fail, out of range
ALTER TABLE range_sales ADD PARTITION time_temp2 VALUES LESS THAN ('2011-06-01');
--fail, invalid format
ALTER TABLE range_sales ADD PARTITION time_temp3 VALUES ('2013-01-01');
--success, add 1 partition
ALTER TABLE range_sales ADD PARTITION time_2012 VALUES LESS THAN ('2013-01-01');
--success, add 1 partition
ALTER TABLE range_sales ADD PARTITION time_end VALUES LESS THAN (MAXVALUE);
--fail, out of range
ALTER TABLE range_sales ADD PARTITION time_temp4 VALUES LESS THAN ('2014-01-01');

--check for ok after add
SELECT p1.relname, p1.parttype, p1.partstrategy, p1.relfilenode!=0 hasfilenode, p1.reltablespace, p1.partkey, p1.boundaries
    FROM pg_class c1, pg_partition p1, pg_namespace n1
    WHERE c1.relname='range_sales'
        AND c1.relnamespace=n1.oid
        AND n1.nspname=CURRENT_SCHEMA
        AND (p1.parentid=c1.oid)
    ORDER BY p1.parttype, p1.relname;
SELECT p1.relname, p1.parttype, p1.partstrategy, p1.relfilenode!=0 hasfilenode, p1.indisusable
    FROM pg_class c1, pg_partition p1, pg_namespace n1
    WHERE c1.relname='range_sales_idx'
        AND c1.relnamespace=n1.oid
        AND n1.nspname=CURRENT_SCHEMA
        AND (p1.parentid=c1.oid)
    ORDER BY p1.relname;
\d+ range_sales

--check for drop partition (for)
--success, drop partition time_2009
ALTER TABLE range_sales DROP PARTITION time_2009;
--success, drop partition time_2011
ALTER TABLE range_sales DROP PARTITION FOR ('2011-06-01');
--fail, invalid type
ALTER TABLE range_sales DROP PARTITION FOR (1);
--fail, number not equal to the number of partkey
ALTER TABLE range_sales DROP PARTITION FOR ('2011-06-01', 1);
--fail, can not drop subpartition on no-subpartition table
ALTER TABLE range_sales DROP SUBPARTITION FOR ('2011-06-01', 1);
--success, drop partition time_2012
ALTER TABLE range_sales DROP PARTITION FOR ('2011-06-01');

--check for ok after drop
SELECT count(*) FROM range_sales;
SELECT p1.relname, p1.parttype, p1.partstrategy, p1.relfilenode!=0 hasfilenode, p1.reltablespace, p1.partkey, p1.boundaries
    FROM pg_class c1, pg_partition p1, pg_namespace n1
    WHERE c1.relname='range_sales'
        AND c1.relnamespace=n1.oid
        AND n1.nspname=CURRENT_SCHEMA
        AND (p1.parentid=c1.oid)
    ORDER BY p1.parttype, p1.relname;
SELECT p1.relname, p1.parttype, p1.partstrategy, p1.relfilenode!=0 hasfilenode, p1.indisusable
    FROM pg_class c1, pg_partition p1, pg_namespace n1
    WHERE c1.relname='range_sales_idx'
        AND c1.relnamespace=n1.oid
        AND n1.nspname=CURRENT_SCHEMA
        AND (p1.parentid=c1.oid)
    ORDER BY p1.relname;
\d+ range_sales

--
----range table, multiple partkeys----
--
--prepare
CREATE TABLE range2_sales
(
    product_id     INT4 NOT NULL,
    customer_id    INT4 PRIMARY KEY,
    time_id        DATE,
    channel_id     CHAR(1),
    type_id        INT4,
    quantity_sold  NUMERIC(3),
    amount_sold    NUMERIC(10,2)
)
PARTITION BY RANGE (time_id, product_id)
(
    PARTITION time_2008 VALUES LESS THAN ('2009-01-01', 200),
    PARTITION time_2009 VALUES LESS THAN ('2010-01-01', 500),
    PARTITION time_2010 VALUES LESS THAN ('2011-01-01', 800),
    PARTITION time_2011 VALUES LESS THAN ('2012-01-01', 1200)
);
INSERT INTO range2_sales SELECT generate_series(1,1000),
                                generate_series(1,1000),
                                date_pli('2008-01-01', generate_series(1,1000)),
                                generate_series(1,1000)%10,
                                generate_series(1,1000)%10,
                                generate_series(1,1000)%1000,
                                generate_series(1,1000);
CREATE INDEX range2_sales_idx ON range2_sales(product_id) LOCAL;

--check for add partition
--fail, can not add subpartition on no-subpartitioned table
ALTER TABLE range2_sales ADD PARTITION time_temp1 VALUES LESS THAN ('2013-01-01', 1500)
    (
        SUBPARTITION time_temp1_part1 VALUES LESS THAN (200),
        SUBPARTITION time_temp1_part2 VALUES LESS THAN (500),
        SUBPARTITION time_temp1_part3 VALUES LESS THAN (800),
        SUBPARTITION time_temp1_part4 VALUES LESS THAN (1200)
    );
--fail, out of range
ALTER TABLE range2_sales ADD PARTITION time_temp2 VALUES LESS THAN ('2011-06-01', 100);
--fail, invalid format
ALTER TABLE range2_sales ADD PARTITION time_temp3 VALUES ('2013-01-01', 1500);
--success, add 1 partition
ALTER TABLE range2_sales ADD PARTITION time_2012 VALUES LESS THAN ('2013-01-01', 1500);
--success, add 1 partition
ALTER TABLE range2_sales ADD PARTITION time_end VALUES LESS THAN (MAXVALUE, MAXVALUE);
--fail, out of range
ALTER TABLE range2_sales ADD PARTITION time_temp4 VALUES LESS THAN ('2014-01-01', 2000);

--check for ok after add
SELECT p1.relname, p1.parttype, p1.partstrategy, p1.relfilenode!=0 hasfilenode, p1.reltablespace, p1.partkey, p1.boundaries
    FROM pg_class c1, pg_partition p1, pg_namespace n1
    WHERE c1.relname='range2_sales'
        AND c1.relnamespace=n1.oid
        AND n1.nspname=CURRENT_SCHEMA
        AND (p1.parentid=c1.oid)
    ORDER BY p1.parttype, p1.relname;
SELECT p1.relname, p1.parttype, p1.partstrategy, p1.relfilenode!=0 hasfilenode, p1.indisusable
    FROM pg_class c1, pg_partition p1, pg_namespace n1
    WHERE c1.relname='range2_sales_idx'
        AND c1.relnamespace=n1.oid
        AND n1.nspname=CURRENT_SCHEMA
        AND (p1.parentid=c1.oid)
    ORDER BY p1.relname;
\d+ range2_sales

--check for drop partition (for)
--success, drop partition time_2009
ALTER TABLE range2_sales DROP PARTITION time_2009;
--success, drop partition time_2011
ALTER TABLE range2_sales DROP PARTITION FOR ('2011-06-01', 600);
--fail, invalid type
ALTER TABLE range2_sales DROP PARTITION FOR (1, 100);
--fail, number not equal to the number of partkey
ALTER TABLE range2_sales DROP PARTITION FOR ('2011-06-01');
--fail, can not drop subpartition on no-subpartition table
ALTER TABLE range2_sales DROP SUBPARTITION FOR ('2011-06-01', 1);
--success, drop partition time_2012
ALTER TABLE range2_sales DROP PARTITION FOR ('2011-06-01', 100);

--check for ok after drop
SELECT count(*) FROM range2_sales;
SELECT p1.relname, p1.parttype, p1.partstrategy, p1.relfilenode!=0 hasfilenode, p1.reltablespace, p1.partkey, p1.boundaries
    FROM pg_class c1, pg_partition p1, pg_namespace n1
    WHERE c1.relname='range2_sales'
        AND c1.relnamespace=n1.oid
        AND n1.nspname=CURRENT_SCHEMA
        AND (p1.parentid=c1.oid)
    ORDER BY p1.parttype, p1.relname;
SELECT p1.relname, p1.parttype, p1.partstrategy, p1.relfilenode!=0 hasfilenode, p1.indisusable
    FROM pg_class c1, pg_partition p1, pg_namespace n1
    WHERE c1.relname='range2_sales_idx'
        AND c1.relnamespace=n1.oid
        AND n1.nspname=CURRENT_SCHEMA
        AND (p1.parentid=c1.oid)
    ORDER BY p1.relname;
\d+ range2_sales

--
----interval table----
--
--prepare
CREATE TABLE interval_sales
(
    product_id     INT4 NOT NULL,
    customer_id    INT4 PRIMARY KEY,
    time_id        DATE,
    channel_id     CHAR(1),
    type_id        INT4,
    quantity_sold  NUMERIC(3),
    amount_sold    NUMERIC(10,2)
)
PARTITION BY RANGE (time_id) INTERVAL ('1 year')
(
    PARTITION time_2008 VALUES LESS THAN ('2009-01-01'),
    PARTITION time_2009 VALUES LESS THAN ('2010-01-01'),
    PARTITION time_2010 VALUES LESS THAN ('2011-01-01')
);
INSERT INTO interval_sales SELECT generate_series(1,1000),
                                  generate_series(1,1000),
                                  date_pli('2009-01-01', generate_series(1,1000)),
                                  generate_series(1,1000)%10,
                                  generate_series(1,1000)%10,
                                  generate_series(1,1000)%1000,
                                  generate_series(1,1000);
CREATE INDEX interval_sales_idx ON interval_sales(product_id) LOCAL;

--check for add partition
--fail, can not add subpartition on no-subpartitioned table
ALTER TABLE interval_sales ADD PARTITION time_temp1 VALUES LESS THAN ('2013-01-01')
    (
        SUBPARTITION time_temp1_part1 VALUES LESS THAN (200),
        SUBPARTITION time_temp1_part2 VALUES LESS THAN (500),
        SUBPARTITION time_temp1_part3 VALUES LESS THAN (800),
        SUBPARTITION time_temp1_part4 VALUES LESS THAN (1200)
    );
--fail, not support add interval
ALTER TABLE interval_sales ADD PARTITION time_2012 VALUES LESS THAN ('2013-01-01');

--check for ok after add
SELECT p1.relname, p1.parttype, p1.partstrategy, p1.relfilenode!=0 hasfilenode, p1.reltablespace, p1.partkey, p1.boundaries
    FROM pg_class c1, pg_partition p1, pg_namespace n1
    WHERE c1.relname='interval_sales'
        AND c1.relnamespace=n1.oid
        AND n1.nspname=CURRENT_SCHEMA
        AND (p1.parentid=c1.oid)
    ORDER BY p1.parttype, p1.relname;
SELECT p1.relname, p1.parttype, p1.partstrategy, p1.relfilenode!=0 hasfilenode, p1.indisusable
    FROM pg_class c1, pg_partition p1, pg_namespace n1
    WHERE c1.relname='interval_sales_idx'
        AND c1.relnamespace=n1.oid
        AND n1.nspname=CURRENT_SCHEMA
        AND (p1.parentid=c1.oid)
    ORDER BY p1.relname;
\d+ interval_sales

--check for drop partition (for)
--success, drop partition time_2009
ALTER TABLE interval_sales DROP PARTITION time_2009;
--success, drop partition sys_p1
ALTER TABLE interval_sales DROP PARTITION FOR ('2011-06-01');
--fail, invalid type
ALTER TABLE interval_sales DROP PARTITION FOR (1);
--fail, number not equal to the number of partkey
ALTER TABLE interval_sales DROP PARTITION FOR ('2010-06-01', 1);
--fail, can not drop subpartition on no-subpartition table
ALTER TABLE interval_sales DROP SUBPARTITION FOR ('2010-06-01', 1);

--check for ok after drop
SELECT p1.relname, p1.parttype, p1.partstrategy, p1.relfilenode!=0 hasfilenode, p1.reltablespace, p1.partkey, p1.boundaries
    FROM pg_class c1, pg_partition p1, pg_namespace n1
    WHERE c1.relname='interval_sales'
        AND c1.relnamespace=n1.oid
        AND n1.nspname=CURRENT_SCHEMA
        AND (p1.parentid=c1.oid)
    ORDER BY p1.parttype, p1.relname;
SELECT p1.relname, p1.parttype, p1.partstrategy, p1.relfilenode!=0 hasfilenode, p1.indisusable
    FROM pg_class c1, pg_partition p1, pg_namespace n1
    WHERE c1.relname='interval_sales_idx'
        AND c1.relnamespace=n1.oid
        AND n1.nspname=CURRENT_SCHEMA
        AND (p1.parentid=c1.oid)
    ORDER BY p1.relname;
\d+ interval_sales

--
----list table----
--
--prepare
CREATE TABLE list_sales
(
    product_id     INT4 NOT NULL,
    customer_id    INT4 PRIMARY KEY,
    time_id        DATE,
    channel_id     CHAR(1),
    type_id        INT4,
    quantity_sold  NUMERIC(3),
    amount_sold    NUMERIC(10,2)
)
PARTITION BY LIST (channel_id)
(
    PARTITION channel1 VALUES ('0', '1', '2'),
    PARTITION channel2 VALUES ('3', '4', '5'),
    PARTITION channel3 VALUES ('6', '7'),
    PARTITION channel4 VALUES ('8', '9')
);
INSERT INTO list_sales SELECT generate_series(1,1000),
                              generate_series(1,1000),
                              date_pli('2008-01-01', generate_series(1,1000)),
                              generate_series(1,1000)%10,
                              generate_series(1,1000)%10,
                              generate_series(1,1000)%1000,
                              generate_series(1,1000);
CREATE INDEX list_sales_idx ON list_sales(product_id) LOCAL;

--check for add partition
--fail, can not add subpartition on no-subpartitioned table
ALTER TABLE list_sales ADD PARTITION channel_temp1 VALUES ('X')
    (
        SUBPARTITION channel_temp1_part1 VALUES LESS THAN (200),
        SUBPARTITION channel_temp1_part2 VALUES LESS THAN (500),
        SUBPARTITION channel_temp1_part3 VALUES LESS THAN (800),
        SUBPARTITION channel_temp1_part4 VALUES LESS THAN (1200)
    );
--fail, out of range
ALTER TABLE list_sales ADD PARTITION channel_temp2 VALUES ('8', 'X');
--fail, value conflict
ALTER TABLE list_sales ADD PARTITION channel_temp3 VALUES ('X', 'X', 'Z');
--fail, invalid format
ALTER TABLE list_sales ADD PARTITION channel_temp4 VALUES LESS THAN('X');
--success, add 1 partition
ALTER TABLE list_sales ADD PARTITION channel5 VALUES ('X', 'Z');
--success, add 1 partition
ALTER TABLE list_sales ADD PARTITION channel_default VALUES (DEFAULT);
--fail, out of range
ALTER TABLE list_sales ADD PARTITION channel_temp5 VALUES ('P');

--check for ok after add
SELECT p1.relname, p1.parttype, p1.partstrategy, p1.relfilenode!=0 hasfilenode, p1.reltablespace, p1.partkey, p1.boundaries
    FROM pg_class c1, pg_partition p1, pg_namespace n1
    WHERE c1.relname='list_sales'
        AND c1.relnamespace=n1.oid
        AND n1.nspname=CURRENT_SCHEMA
        AND (p1.parentid=c1.oid)
    ORDER BY p1.parttype, p1.relname;
SELECT p1.relname, p1.parttype, p1.partstrategy, p1.relfilenode!=0 hasfilenode, p1.indisusable
    FROM pg_class c1, pg_partition p1, pg_namespace n1
    WHERE c1.relname='list_sales_idx'
        AND c1.relnamespace=n1.oid
        AND n1.nspname=CURRENT_SCHEMA
        AND (p1.parentid=c1.oid)
    ORDER BY p1.relname;
\d+ list_sales

--check for drop partition (for)
--success, drop partition channel2
ALTER TABLE list_sales DROP PARTITION channel2;
--success, drop partition channel3
ALTER TABLE list_sales DROP PARTITION FOR ('6');
--fail, invalid type
ALTER TABLE list_sales DROP PARTITION FOR (10);
--fail, number not equal to the number of partkey
ALTER TABLE list_sales DROP PARTITION FOR ('6', 1);
--fail, can not drop subpartition on no-subpartition table
ALTER TABLE list_sales DROP SUBPARTITION FOR ('6', 1);
--success, drop partition channel_default
ALTER TABLE list_sales DROP PARTITION FOR ('6');

--check for ok after drop
SELECT count(*) FROM list_sales;
SELECT p1.relname, p1.parttype, p1.partstrategy, p1.relfilenode!=0 hasfilenode, p1.reltablespace, p1.partkey, p1.boundaries
    FROM pg_class c1, pg_partition p1, pg_namespace n1
    WHERE c1.relname='list_sales'
        AND c1.relnamespace=n1.oid
        AND n1.nspname=CURRENT_SCHEMA
        AND (p1.parentid=c1.oid)
    ORDER BY p1.parttype, p1.relname;
SELECT p1.relname, p1.parttype, p1.partstrategy, p1.relfilenode!=0 hasfilenode, p1.indisusable
    FROM pg_class c1, pg_partition p1, pg_namespace n1
    WHERE c1.relname='list_sales_idx'
        AND c1.relnamespace=n1.oid
        AND n1.nspname=CURRENT_SCHEMA
        AND (p1.parentid=c1.oid)
    ORDER BY p1.relname;
\d+ list_sales

--
----hash table----
--
--prepare
CREATE TABLE hash_sales
(
    product_id     INT4 NOT NULL,
    customer_id    INT4 PRIMARY KEY,
    time_id        DATE,
    channel_id     CHAR(1),
    type_id        INT4,
    quantity_sold  NUMERIC(3),
    amount_sold    NUMERIC(10,2)
)
PARTITION BY HASH (product_id)
(
    PARTITION product1,
    PARTITION product2,
    PARTITION product3,
    PARTITION product4
);
INSERT INTO hash_sales SELECT generate_series(1,1000),
                              generate_series(1,1000),
                              date_pli('2008-01-01', generate_series(1,1000)),
                              generate_series(1,1000)%10,
                              generate_series(1,1000)%10,
                              generate_series(1,1000)%1000,
                              generate_series(1,1000);
CREATE INDEX hash_sales_idx ON hash_sales(product_id) LOCAL;

--check for add partition
--fail, not support add hash
ALTER TABLE hash_sales ADD PARTITION product_temp1
    (
        SUBPARTITION product_temp1_part1 VALUES LESS THAN (200),
        SUBPARTITION product_temp1_part2 VALUES LESS THAN (500),
        SUBPARTITION product_temp1_part3 VALUES LESS THAN (800),
        SUBPARTITION product_temp1_part4 VALUES LESS THAN (1200)
    );
--fail, not support add hash
ALTER TABLE hash_sales ADD PARTITION product_temp2;
--fail, invalid format
ALTER TABLE hash_sales ADD PARTITION product_temp3 VALUES LESS THAN('X');

--check for ok after add
SELECT p1.relname, p1.parttype, p1.partstrategy, p1.relfilenode!=0 hasfilenode, p1.reltablespace, p1.partkey, p1.boundaries
    FROM pg_class c1, pg_partition p1, pg_namespace n1
    WHERE c1.relname='hash_sales'
        AND c1.relnamespace=n1.oid
        AND n1.nspname=CURRENT_SCHEMA
        AND (p1.parentid=c1.oid)
    ORDER BY p1.parttype, p1.relname;
SELECT p1.relname, p1.parttype, p1.partstrategy, p1.relfilenode!=0 hasfilenode, p1.indisusable
    FROM pg_class c1, pg_partition p1, pg_namespace n1
    WHERE c1.relname='hash_sales_idx'
        AND c1.relnamespace=n1.oid
        AND n1.nspname=CURRENT_SCHEMA
        AND (p1.parentid=c1.oid)
    ORDER BY p1.relname;
\d+ hash_sales

--check for drop partition (for)
--fail, not support drop hash
ALTER TABLE hash_sales DROP PARTITION product2;
--fail, not support drop hash
ALTER TABLE hash_sales DROP PARTITION FOR (0);
--fail, not support drop hash
ALTER TABLE hash_sales DROP PARTITION FOR (0, 0);
--fail, can not drop subpartition on no-subpartition table
ALTER TABLE hash_sales DROP SUBPARTITION FOR(0, 0);
--fail, can not drop subpartition on no-subpartition table
ALTER TABLE hash_sales DROP SUBPARTITION FOR(0);

--check for ok after drop
SELECT count(*) FROM hash_sales;
SELECT p1.relname, p1.parttype, p1.partstrategy, p1.relfilenode!=0 hasfilenode, p1.reltablespace, p1.partkey, p1.boundaries
    FROM pg_class c1, pg_partition p1, pg_namespace n1
    WHERE c1.relname='hash_sales'
        AND c1.relnamespace=n1.oid
        AND n1.nspname=CURRENT_SCHEMA
        AND (p1.parentid=c1.oid)
    ORDER BY p1.parttype, p1.relname;
SELECT p1.relname, p1.parttype, p1.partstrategy, p1.relfilenode!=0 hasfilenode, p1.indisusable
    FROM pg_class c1, pg_partition p1, pg_namespace n1
    WHERE c1.relname='hash_sales_idx'
        AND c1.relnamespace=n1.oid
        AND n1.nspname=CURRENT_SCHEMA
        AND (p1.parentid=c1.oid)
    ORDER BY p1.relname;
\d+ hash_sales

--finish
DROP TABLE range_sales;
DROP TABLE range2_sales;
DROP TABLE interval_sales;
DROP TABLE list_sales;
DROP TABLE hash_sales;

DROP SCHEMA hw_partition_add_drop_partition CASCADE;
RESET CURRENT_SCHEMA;
