DROP SCHEMA hw_subpartition_add_drop_partition CASCADE;
CREATE SCHEMA hw_subpartition_add_drop_partition;
SET CURRENT_SCHEMA TO hw_subpartition_add_drop_partition;

--
----range-range table----
--
--prepare
CREATE TABLE range_range_sales
(
    product_id     INT4 NOT NULL,
    customer_id    INT4 PRIMARY KEY,
    time_id        DATE,
    channel_id     CHAR(1),
    type_id        INT4,
    quantity_sold  NUMERIC(3),
    amount_sold    NUMERIC(10,2)
)
PARTITION BY RANGE (customer_id) SUBPARTITION BY RANGE (time_id)
(
    PARTITION customer1 VALUES LESS THAN (200)
    (
        SUBPARTITION customer1_2008 VALUES LESS THAN ('2009-01-01'),
        SUBPARTITION customer1_2009 VALUES LESS THAN ('2010-01-01'),
        SUBPARTITION customer1_2010 VALUES LESS THAN ('2011-01-01'),
        SUBPARTITION customer1_2011 VALUES LESS THAN ('2012-01-01')
    ),
    PARTITION customer2 VALUES LESS THAN (500)
    (
        SUBPARTITION customer2_2008 VALUES LESS THAN ('2009-01-01'),
        SUBPARTITION customer2_2009 VALUES LESS THAN ('2010-01-01'),
        SUBPARTITION customer2_2010 VALUES LESS THAN ('2011-01-01'),
        SUBPARTITION customer2_2011 VALUES LESS THAN ('2012-01-01')
    ),
    PARTITION customer3 VALUES LESS THAN (800),
    PARTITION customer4 VALUES LESS THAN (1200)
    (
        SUBPARTITION customer4_all VALUES LESS THAN ('2012-01-01')
    )
);
INSERT INTO range_range_sales SELECT generate_series(1,1000),
                                     generate_series(1,1000),
                                     date_pli('2008-01-01', generate_series(1,1000)),
                                     generate_series(1,1000)%10,
                                     generate_series(1,1000)%10,
                                     generate_series(1,1000)%1000,
                                     generate_series(1,1000);
CREATE INDEX range_range_sales_idx ON range_range_sales(product_id) LOCAL;

--check for add partition/subpartition
--fail, value conflict
ALTER TABLE range_range_sales ADD PARTITION customer_temp1 VALUES LESS THAN (1500)
    (
        SUBPARTITION customer_temp1_2008 VALUES LESS THAN ('2009-01-01'),
        SUBPARTITION customer_temp1_2009 VALUES LESS THAN ('2010-01-01'),
        SUBPARTITION customer_temp1_2010 VALUES LESS THAN ('2012-01-01'),
        SUBPARTITION customer_temp1_2011 VALUES LESS THAN ('2011-01-01')
    );
--success, add 4 subpartition
ALTER TABLE range_range_sales ADD PARTITION customer5 VALUES LESS THAN (1500)
    (
        SUBPARTITION customer5_2008 VALUES LESS THAN ('2009-01-01'),
        SUBPARTITION customer5_2009 VALUES LESS THAN ('2010-01-01'),
        SUBPARTITION customer5_2010 VALUES LESS THAN ('2011-01-01'),
        SUBPARTITION customer5_2011 VALUES LESS THAN ('2012-01-01')
    );
--fail, out of range
ALTER TABLE range_range_sales ADD PARTITION customer_temp2 VALUES LESS THAN (1100);
--fail, invalid format
ALTER TABLE range_range_sales ADD PARTITION customer_temp3 VALUES (1300);
--success, add 1 default subpartition
ALTER TABLE range_range_sales ADD PARTITION customer6 VALUES LESS THAN (MAXVALUE);
--fail, out of range
ALTER TABLE range_range_sales ADD PARTITION customer_temp4 VALUES LESS THAN (1800);
--success, add 1 subpartition
ALTER TABLE range_range_sales MODIFY PARTITION customer1 ADD SUBPARTITION customer1_2012 VALUES LESS THAN ('2013-01-01');
--fail, out of range
ALTER TABLE range_range_sales MODIFY PARTITION customer3 ADD SUBPARTITION customer3_temp1 VALUES LESS THAN ('2015-01-01');
--fail, out of range
ALTER TABLE range_range_sales MODIFY PARTITION customer4 ADD SUBPARTITION customer4_temp1 VALUES LESS THAN ('2011-01-01');
--fail, invalid format
ALTER TABLE range_range_sales MODIFY PARTITION customer2 ADD SUBPARTITION customer2_temp1 VALUES ('2015-01-01');

--check for ok after add
SELECT p1.relname, p1.parttype, p1.partstrategy, p1.relfilenode!=0 hasfilenode, p1.reltablespace, p1.partkey, p1.boundaries
    FROM pg_class c1, pg_partition p1, pg_namespace n1
    WHERE c1.relname='range_range_sales'
        AND c1.relnamespace=n1.oid
        AND n1.nspname=CURRENT_SCHEMA
        AND (p1.parentid=c1.oid
            OR p1.parentid IN (
                SELECT p2.oid FROM pg_class c2, pg_partition p2, pg_namespace n2
                    WHERE c2.relname='range_range_sales'
                        AND c2.relnamespace=n2.oid
                        AND n2.nspname=CURRENT_SCHEMA
                        AND (p2.parentid=c2.oid)
            ))
    ORDER BY p1.parttype, p1.relname;
SELECT p1.relname, p1.parttype, p1.partstrategy, p1.relfilenode!=0 hasfilenode, p1.indisusable
    FROM pg_class c1, pg_partition p1, pg_namespace n1
    WHERE c1.relname='range_range_sales_idx'
        AND c1.relnamespace=n1.oid
        AND n1.nspname=CURRENT_SCHEMA
        AND (p1.parentid=c1.oid)
    ORDER BY p1.relname;
\d+ range_range_sales

--check for drop partition/subpartition (for)
--success, drop partition customer2
ALTER TABLE range_range_sales DROP PARTITION customer2;
--success
ALTER TABLE range_range_sales DROP SUBPARTITION customer1_2008;
--fail, the only subpartition
ALTER TABLE range_range_sales DROP SUBPARTITION customer4_all;
--success, drop partition customer3
ALTER TABLE range_range_sales DROP PARTITION FOR (400);
--fail, number not equal to the number of partkey
ALTER TABLE range_range_sales DROP PARTITION FOR (400, '2010-01-01');
--fail, number not equal to the number of partkey
ALTER TABLE range_range_sales DROP SUBPARTITION FOR (1400);
--fail, invalid type
ALTER TABLE range_range_sales DROP PARTITION FOR ('2010-01-01');
--fail, invalid type
ALTER TABLE range_range_sales DROP SUBPARTITION FOR ('2010-01-01', 1400);
--success, drop subpartition customer5_2010
ALTER TABLE range_range_sales DROP SUBPARTITION FOR(1400, '2010-01-01');
--fail, the only subpartition in customer6
ALTER TABLE range_range_sales DROP SUBPARTITION FOR(2000, '2009-01-01');
--fail, no subpartition find
ALTER TABLE range_range_sales DROP SUBPARTITION FOR(1400, '2012-01-01');

--check for ok after drop
SELECT count(*) FROM range_range_sales;
SELECT p1.relname, p1.parttype, p1.partstrategy, p1.relfilenode!=0 hasfilenode, p1.reltablespace, p1.partkey, p1.boundaries
    FROM pg_class c1, pg_partition p1, pg_namespace n1
    WHERE c1.relname='range_range_sales'
        AND c1.relnamespace=n1.oid
        AND n1.nspname=CURRENT_SCHEMA
        AND (p1.parentid=c1.oid
            OR p1.parentid IN (
                SELECT p2.oid FROM pg_class c2, pg_partition p2, pg_namespace n2
                    WHERE c2.relname='range_range_sales'
                        AND c2.relnamespace=n2.oid
                        AND n2.nspname=CURRENT_SCHEMA
                        AND (p2.parentid=c2.oid)
            ))
    ORDER BY p1.parttype, p1.relname;
SELECT p1.relname, p1.parttype, p1.partstrategy, p1.relfilenode!=0 hasfilenode, p1.indisusable
    FROM pg_class c1, pg_partition p1, pg_namespace n1
    WHERE c1.relname='range_range_sales_idx'
        AND c1.relnamespace=n1.oid
        AND n1.nspname=CURRENT_SCHEMA
        AND (p1.parentid=c1.oid)
    ORDER BY p1.relname;
\d+ range_range_sales

--
----range-list table----
--
--prepare
CREATE TABLE range_list_sales
(
    product_id     INT4 NOT NULL,
    customer_id    INT4 PRIMARY KEY,
    time_id        DATE,
    channel_id     CHAR(1),
    type_id        INT4,
    quantity_sold  NUMERIC(3),
    amount_sold    NUMERIC(10,2)
)
PARTITION BY RANGE (customer_id) SUBPARTITION BY LIST (channel_id)
(
    PARTITION customer1 VALUES LESS THAN (200)
    (
        SUBPARTITION customer1_channel1 VALUES ('0', '1', '2'),
        SUBPARTITION customer1_channel2 VALUES ('3', '4', '5'),
        SUBPARTITION customer1_channel3 VALUES ('6', '7', '8'),
        SUBPARTITION customer1_channel4 VALUES ('9')
    ),
    PARTITION customer2 VALUES LESS THAN (500)
    (
        SUBPARTITION customer2_channel1 VALUES ('0', '1', '2', '3', '4'),
        SUBPARTITION customer2_channel2 VALUES (DEFAULT)
    ),
    PARTITION customer3 VALUES LESS THAN (800),
    PARTITION customer4 VALUES LESS THAN (1200)
    (
        SUBPARTITION customer4_channel1 VALUES ('0', '1', '2', '3', '4', '5', '6', '7', '8', '9')
    )
);
INSERT INTO range_list_sales SELECT generate_series(1,1000),
                                    generate_series(1,1000),
                                    date_pli('2008-01-01', generate_series(1,1000)),
                                    generate_series(1,1000)%10,
                                    generate_series(1,1000)%10,
                                    generate_series(1,1000)%1000,
                                    generate_series(1,1000);
CREATE INDEX range_list_sales_idx ON range_list_sales(product_id) LOCAL;

--check for add partition/subpartition
--fail, value conflict
ALTER TABLE range_list_sales ADD PARTITION customer_temp1 VALUES LESS THAN (1500)
    (
        SUBPARTITION customer_temp1_channel1 VALUES ('0', '1', '2'),
        SUBPARTITION customer_temp1_channel2 VALUES ('3', '4', '5'),
        SUBPARTITION customer_temp1_channel3 VALUES ('6', '7', '5')
    );
--fail, value conflict
ALTER TABLE range_list_sales ADD PARTITION customer_temp2 VALUES LESS THAN (1500)
    (
        SUBPARTITION customer_temp2_channel1 VALUES ('0', '1', '2'),
        SUBPARTITION customer_temp2_channel2 VALUES ('3', '4', '5'),
        SUBPARTITION customer_temp2_channel3 VALUES ('6', '7', '8', '7', '8')
    );
--success, add 4 subpartition
ALTER TABLE range_list_sales ADD PARTITION customer5 VALUES LESS THAN (1500)
    (
        SUBPARTITION customer5_channel1 VALUES ('0', '1', '2'),
        SUBPARTITION customer5_channel2 VALUES ('3', '4', '5'),
        SUBPARTITION customer5_channel3 VALUES ('6', '7', '8'),
        SUBPARTITION customer5_channel4 VALUES ('9')
    );
--fail, out of range
ALTER TABLE range_list_sales ADD PARTITION customer_temp3 VALUES LESS THAN (1100);
--fail, invalid format
ALTER TABLE range_list_sales ADD PARTITION customer_temp4 VALUES (1300);
--success, add 1 default subpartition
ALTER TABLE range_list_sales ADD PARTITION customer6 VALUES LESS THAN (MAXVALUE);
--fail, out of range
ALTER TABLE range_list_sales ADD PARTITION customer_temp5 VALUES LESS THAN (1800);
--success, add 1 subpartition
ALTER TABLE range_list_sales MODIFY PARTITION customer1 ADD SUBPARTITION customer1_channel5 VALUES ('X');
--fail, out of range
ALTER TABLE range_list_sales MODIFY PARTITION customer2 ADD SUBPARTITION customer2_temp1 VALUES ('X');
--fail, out of range
ALTER TABLE range_list_sales MODIFY PARTITION customer3 ADD SUBPARTITION customer3_temp1 VALUES ('X');
--fail, invalid format
ALTER TABLE range_list_sales MODIFY PARTITION customer4 ADD SUBPARTITION customer4_temp1 VALUES LESS THAN ('X');

--check for ok after add
SELECT p1.relname, p1.parttype, p1.partstrategy, p1.relfilenode!=0 hasfilenode, p1.reltablespace, p1.partkey, p1.boundaries
    FROM pg_class c1, pg_partition p1, pg_namespace n1
    WHERE c1.relname='range_list_sales'
        AND c1.relnamespace=n1.oid
        AND n1.nspname=CURRENT_SCHEMA
        AND (p1.parentid=c1.oid
            OR p1.parentid IN (
                SELECT p2.oid FROM pg_class c2, pg_partition p2, pg_namespace n2
                    WHERE c2.relname='range_list_sales'
                        AND c2.relnamespace=n2.oid
                        AND n2.nspname=CURRENT_SCHEMA
                        AND (p2.parentid=c2.oid)
            ))
    ORDER BY p1.parttype, p1.relname;
SELECT p1.relname, p1.parttype, p1.partstrategy, p1.relfilenode!=0 hasfilenode, p1.indisusable
    FROM pg_class c1, pg_partition p1, pg_namespace n1
    WHERE c1.relname='range_list_sales_idx'
        AND c1.relnamespace=n1.oid
        AND n1.nspname=CURRENT_SCHEMA
        AND (p1.parentid=c1.oid)
    ORDER BY p1.relname;
\d+ range_list_sales

--check for drop partition/subpartition (for)
--success, drop partition customer2
ALTER TABLE range_list_sales DROP PARTITION customer2;
--success
ALTER TABLE range_list_sales DROP SUBPARTITION customer1_channel1;
--fail, the only subpartition
ALTER TABLE range_list_sales DROP SUBPARTITION customer4_channel1;
--success, drop partition customer3
ALTER TABLE range_list_sales DROP PARTITION FOR (400);
--fail, number not equal to the number of partkey
ALTER TABLE range_list_sales DROP PARTITION FOR (400, '4');
--fail, number not equal to the number of partkey
ALTER TABLE range_list_sales DROP SUBPARTITION FOR (1400);
--fail, invalid type
ALTER TABLE range_list_sales DROP PARTITION FOR ('abc');
--fail, invalid type
ALTER TABLE range_list_sales DROP SUBPARTITION FOR ('abc', 1400);
--success, drop subpartition customer5_channel3
ALTER TABLE range_list_sales DROP SUBPARTITION FOR(1400, '7');
--fail, the only subpartition in customer6
ALTER TABLE range_list_sales DROP SUBPARTITION FOR(2000, 'X');
--fail, no subpartition find
ALTER TABLE range_list_sales DROP SUBPARTITION FOR(1100, 'X');

--check for ok after drop
SELECT count(*) FROM range_list_sales;
SELECT p1.relname, p1.parttype, p1.partstrategy, p1.relfilenode!=0 hasfilenode, p1.reltablespace, p1.partkey, p1.boundaries
    FROM pg_class c1, pg_partition p1, pg_namespace n1
    WHERE c1.relname='range_list_sales'
        AND c1.relnamespace=n1.oid
        AND n1.nspname=CURRENT_SCHEMA
        AND (p1.parentid=c1.oid
            OR p1.parentid IN (
                SELECT p2.oid FROM pg_class c2, pg_partition p2, pg_namespace n2
                    WHERE c2.relname='range_list_sales'
                        AND c2.relnamespace=n2.oid
                        AND n2.nspname=CURRENT_SCHEMA
                        AND (p2.parentid=c2.oid)
            ))
    ORDER BY p1.parttype, p1.relname;
SELECT p1.relname, p1.parttype, p1.partstrategy, p1.relfilenode!=0 hasfilenode, p1.indisusable
    FROM pg_class c1, pg_partition p1, pg_namespace n1
    WHERE c1.relname='range_list_sales_idx'
        AND c1.relnamespace=n1.oid
        AND n1.nspname=CURRENT_SCHEMA
        AND (p1.parentid=c1.oid)
    ORDER BY p1.relname;
\d+ range_list_sales

--
----range-hash table----
--
--prepare
CREATE TABLE range_hash_sales
(
    product_id     INT4 NOT NULL,
    customer_id    INT4 PRIMARY KEY,
    time_id        DATE,
    channel_id     CHAR(1),
    type_id        INT4,
    quantity_sold  NUMERIC(3),
    amount_sold    NUMERIC(10,2)
)
PARTITION BY RANGE (customer_id) SUBPARTITION BY HASH (product_id)
(
    PARTITION customer1 VALUES LESS THAN (200)
    (
        SUBPARTITION customer1_product1,
        SUBPARTITION customer1_product2,
        SUBPARTITION customer1_product3,
        SUBPARTITION customer1_product4
    ),
    PARTITION customer2 VALUES LESS THAN (500)
    (
        SUBPARTITION customer2_product1,
        SUBPARTITION customer2_product2
    ),
    PARTITION customer3 VALUES LESS THAN (800),
    PARTITION customer4 VALUES LESS THAN (1200)
    (
        SUBPARTITION customer4_product1
    )
);
INSERT INTO range_hash_sales SELECT generate_series(1,1000),
                                    generate_series(1,1000),
                                    date_pli('2008-01-01', generate_series(1,1000)),
                                    generate_series(1,1000)%10,
                                    generate_series(1,1000)%10,
                                    generate_series(1,1000)%1000,
                                    generate_series(1,1000);
CREATE INDEX range_hash_sales_idx ON range_hash_sales(product_id) LOCAL;

--check for add partition/subpartition
--success, add 4 subpartition
ALTER TABLE range_hash_sales ADD PARTITION customer5 VALUES LESS THAN (1500)
    (
        SUBPARTITION customer5_product1,
        SUBPARTITION customer5_product2,
        SUBPARTITION customer5_product3,
        SUBPARTITION customer5_product4
    );
--fail, out of range
ALTER TABLE range_hash_sales ADD PARTITION customer_temp1 VALUES LESS THAN (1100);
--fail, invalid format
ALTER TABLE range_hash_sales ADD PARTITION customer_temp2 VALUES (1300);
--success, add 1 default subpartition
ALTER TABLE range_hash_sales ADD PARTITION customer6 VALUES LESS THAN (MAXVALUE);
--fail, out of range
ALTER TABLE range_hash_sales ADD PARTITION customer_temp3 VALUES LESS THAN (1800);
--fail, not support add hash
ALTER TABLE range_hash_sales MODIFY PARTITION customer1 ADD SUBPARTITION customer1_temp1;
--fail, invalid format
ALTER TABLE range_hash_sales MODIFY PARTITION customer4 ADD SUBPARTITION customer4_temp1 VALUES LESS THAN ('X');

--check for ok after add
SELECT p1.relname, p1.parttype, p1.partstrategy, p1.relfilenode!=0 hasfilenode, p1.reltablespace, p1.partkey, p1.boundaries
    FROM pg_class c1, pg_partition p1, pg_namespace n1
    WHERE c1.relname='range_hash_sales'
        AND c1.relnamespace=n1.oid
        AND n1.nspname=CURRENT_SCHEMA
        AND (p1.parentid=c1.oid
            OR p1.parentid IN (
                SELECT p2.oid FROM pg_class c2, pg_partition p2, pg_namespace n2
                    WHERE c2.relname='range_hash_sales'
                        AND c2.relnamespace=n2.oid
                        AND n2.nspname=CURRENT_SCHEMA
                        AND (p2.parentid=c2.oid)
            ))
    ORDER BY p1.parttype, p1.relname;
SELECT p1.relname, p1.parttype, p1.partstrategy, p1.relfilenode!=0 hasfilenode, p1.indisusable
    FROM pg_class c1, pg_partition p1, pg_namespace n1
    WHERE c1.relname='range_hash_sales_idx'
        AND c1.relnamespace=n1.oid
        AND n1.nspname=CURRENT_SCHEMA
        AND (p1.parentid=c1.oid)
    ORDER BY p1.relname;
\d+ range_hash_sales

--check for drop partition/subpartition (for)
--success, drop partition customer2
ALTER TABLE range_hash_sales DROP PARTITION customer2;
--fail, not support drop hash
ALTER TABLE range_hash_sales DROP SUBPARTITION customer1_product1;
--fail, not support drop hash
ALTER TABLE range_hash_sales DROP SUBPARTITION customer4_product1;
--success, drop partition customer3
ALTER TABLE range_hash_sales DROP PARTITION FOR (400);
--fail, number not equal to the number of partkey
ALTER TABLE range_hash_sales DROP PARTITION FOR (400, '2010-01-01');
--fail, invalid type
ALTER TABLE range_hash_sales DROP PARTITION FOR ('2010-01-01');
--fail, not support drop hash
ALTER TABLE range_hash_sales DROP SUBPARTITION FOR(1400, 1);

--check for ok after drop
SELECT count(*) FROM range_hash_sales;
SELECT p1.relname, p1.parttype, p1.partstrategy, p1.relfilenode!=0 hasfilenode, p1.reltablespace, p1.partkey, p1.boundaries
    FROM pg_class c1, pg_partition p1, pg_namespace n1
    WHERE c1.relname='range_hash_sales'
        AND c1.relnamespace=n1.oid
        AND n1.nspname=CURRENT_SCHEMA
        AND (p1.parentid=c1.oid
            OR p1.parentid IN (
                SELECT p2.oid FROM pg_class c2, pg_partition p2, pg_namespace n2
                    WHERE c2.relname='range_hash_sales'
                        AND c2.relnamespace=n2.oid
                        AND n2.nspname=CURRENT_SCHEMA
                        AND (p2.parentid=c2.oid)
            ))
    ORDER BY p1.parttype, p1.relname;
SELECT p1.relname, p1.parttype, p1.partstrategy, p1.relfilenode!=0 hasfilenode, p1.indisusable
    FROM pg_class c1, pg_partition p1, pg_namespace n1
    WHERE c1.relname='range_hash_sales_idx'
        AND c1.relnamespace=n1.oid
        AND n1.nspname=CURRENT_SCHEMA
        AND (p1.parentid=c1.oid)
    ORDER BY p1.relname;
\d+ range_hash_sales

--
----list-range table----
--
--prepare
CREATE TABLE list_range_sales
(
    product_id     INT4 NOT NULL,
    customer_id    INT4 PRIMARY KEY,
    time_id        DATE,
    channel_id     CHAR(1),
    type_id        INT4,
    quantity_sold  NUMERIC(3),
    amount_sold    NUMERIC(10,2)
)
PARTITION BY LIST (channel_id) SUBPARTITION BY RANGE (customer_id) 
(
    PARTITION channel1 VALUES ('0', '1', '2')
    (
        SUBPARTITION channel1_customer1 VALUES LESS THAN (200),
        SUBPARTITION channel1_customer2 VALUES LESS THAN (500),
        SUBPARTITION channel1_customer3 VALUES LESS THAN (800),
        SUBPARTITION channel1_customer4 VALUES LESS THAN (1200)
    ),
    PARTITION channel2 VALUES ('3', '4', '5')
    (
        SUBPARTITION channel2_customer1 VALUES LESS THAN (500),
        SUBPARTITION channel2_customer2 VALUES LESS THAN (MAXVALUE)
    ),
    PARTITION channel3 VALUES ('6', '7'),
    PARTITION channel4 VALUES ('8', '9')
    (
        SUBPARTITION channel4_customer1 VALUES LESS THAN (1200)
    )
);
INSERT INTO list_range_sales SELECT generate_series(1,1000),
                                    generate_series(1,1000),
                                    date_pli('2008-01-01', generate_series(1,1000)),
                                    generate_series(1,1000)%10,
                                    generate_series(1,1000)%10,
                                    generate_series(1,1000)%1000,
                                    generate_series(1,1000);
CREATE INDEX list_range_sales_idx ON list_range_sales(product_id) LOCAL;

--check for add partition/subpartition
--fail, value conflict
ALTER TABLE list_range_sales ADD PARTITION channel_temp1 VALUES ('X')
    (
        SUBPARTITION channel_temp1_customer1 VALUES LESS THAN (200),
        SUBPARTITION channel_temp1_customer2 VALUES LESS THAN (500),
        SUBPARTITION channel_temp1_customer3 VALUES LESS THAN (800),
        SUBPARTITION channel_temp1_customer4 VALUES LESS THAN (700)
    );
--success, add 4 subpartition
ALTER TABLE list_range_sales ADD PARTITION channel5 VALUES ('X')
    (
        SUBPARTITION channel5_customer1 VALUES LESS THAN (200),
        SUBPARTITION channel5_customer2 VALUES LESS THAN (500),
        SUBPARTITION channel5_customer3 VALUES LESS THAN (800),
        SUBPARTITION channel5_customer4 VALUES LESS THAN (1200)
    );
--fail, value conflict
ALTER TABLE list_range_sales ADD PARTITION channel_temp2 VALUES ('0', 'Z', 'C');
--fail, invalid format
ALTER TABLE list_range_sales ADD PARTITION channel_temp3 VALUES LESS THAN ('Z');
--success, add 1 default subpartition
ALTER TABLE list_range_sales ADD PARTITION channel6 VALUES (DEFAULT);
--fail, value conflict
ALTER TABLE list_range_sales ADD PARTITION channel_temp4 VALUES ('M', 'X');
--success, add 1 subpartition
ALTER TABLE list_range_sales MODIFY PARTITION channel1 ADD SUBPARTITION channel1_customer5 VALUES LESS THAN (MAXVALUE);
--fail, out of range
ALTER TABLE list_range_sales MODIFY PARTITION channel2 ADD SUBPARTITION channel2_temp1 VALUES LESS THAN (2000);
--fail, out of range
ALTER TABLE list_range_sales MODIFY PARTITION channel3 ADD SUBPARTITION channel3_temp1 VALUES LESS THAN (2000);
--fail, invalid format
ALTER TABLE list_range_sales MODIFY PARTITION channel4 ADD SUBPARTITION channel4_temp1 VALUES (1500);

--check for ok after add
SELECT p1.relname, p1.parttype, p1.partstrategy, p1.relfilenode!=0 hasfilenode, p1.reltablespace, p1.partkey, p1.boundaries
    FROM pg_class c1, pg_partition p1, pg_namespace n1
    WHERE c1.relname='list_range_sales'
        AND c1.relnamespace=n1.oid
        AND n1.nspname=CURRENT_SCHEMA
        AND (p1.parentid=c1.oid
            OR p1.parentid IN (
                SELECT p2.oid FROM pg_class c2, pg_partition p2, pg_namespace n2
                    WHERE c2.relname='list_range_sales'
                        AND c2.relnamespace=n2.oid
                        AND n2.nspname=CURRENT_SCHEMA
                        AND (p2.parentid=c2.oid)
            ))
    ORDER BY p1.parttype, p1.relname;
SELECT p1.relname, p1.parttype, p1.partstrategy, p1.relfilenode!=0 hasfilenode, p1.indisusable
    FROM pg_class c1, pg_partition p1, pg_namespace n1
    WHERE c1.relname='list_range_sales_idx'
        AND c1.relnamespace=n1.oid
        AND n1.nspname=CURRENT_SCHEMA
        AND (p1.parentid=c1.oid)
    ORDER BY p1.relname;
\d+ list_range_sales

--check for drop partition/subpartition (for)
--success, drop partition channel2
ALTER TABLE list_range_sales DROP PARTITION channel2;
--success
ALTER TABLE list_range_sales DROP SUBPARTITION channel1_customer1;
--fail, the only subpartition
ALTER TABLE list_range_sales DROP SUBPARTITION channel4_customer1;
--success, drop partition channel3
ALTER TABLE list_range_sales DROP PARTITION FOR ('6');
--fail, number not equal to the number of partkey
ALTER TABLE list_range_sales DROP PARTITION FOR('X', 700);
--fail, number not equal to the number of partkey
ALTER TABLE list_range_sales DROP SUBPARTITION FOR('X');
--fail, invalid type
ALTER TABLE list_range_sales DROP PARTITION FOR (10);
--fail, invalid type
ALTER TABLE list_range_sales DROP SUBPARTITION FOR(700, 'X');
--success, drop subpartition channel5_customer3
ALTER TABLE list_range_sales DROP SUBPARTITION FOR('X', 700);
--fail, the only subpartition in channel6
ALTER TABLE list_range_sales DROP SUBPARTITION FOR('K', 100);
--fail, no subpartition find
ALTER TABLE list_range_sales DROP SUBPARTITION FOR('X', 2500);

--check for ok after drop
SELECT count(*) FROM list_range_sales;
SELECT p1.relname, p1.parttype, p1.partstrategy, p1.relfilenode!=0 hasfilenode, p1.reltablespace, p1.partkey, p1.boundaries
    FROM pg_class c1, pg_partition p1, pg_namespace n1
    WHERE c1.relname='list_range_sales'
        AND c1.relnamespace=n1.oid
        AND n1.nspname=CURRENT_SCHEMA
        AND (p1.parentid=c1.oid
            OR p1.parentid IN (
                SELECT p2.oid FROM pg_class c2, pg_partition p2, pg_namespace n2
                    WHERE c2.relname='list_range_sales'
                        AND c2.relnamespace=n2.oid
                        AND n2.nspname=CURRENT_SCHEMA
                        AND (p2.parentid=c2.oid)
            ))
    ORDER BY p1.parttype, p1.relname;
SELECT p1.relname, p1.parttype, p1.partstrategy, p1.relfilenode!=0 hasfilenode, p1.indisusable
    FROM pg_class c1, pg_partition p1, pg_namespace n1
    WHERE c1.relname='list_range_sales_idx'
        AND c1.relnamespace=n1.oid
        AND n1.nspname=CURRENT_SCHEMA
        AND (p1.parentid=c1.oid)
    ORDER BY p1.relname;
\d+ list_range_sales

--
----list-list table----
--
--prepare
CREATE TABLE list_list_sales
(
    product_id     INT4 NOT NULL,
    customer_id    INT4 PRIMARY KEY,
    time_id        DATE,
    channel_id     CHAR(1),
    type_id        INT4,
    quantity_sold  NUMERIC(3),
    amount_sold    NUMERIC(10,2)
)
PARTITION BY LIST (channel_id) SUBPARTITION BY LIST (type_id) 
(
    PARTITION channel1 VALUES ('0', '1', '2')
    (
        SUBPARTITION channel1_type1 VALUES (0, 1, 2),
        SUBPARTITION channel1_type2 VALUES (3, 4),
        SUBPARTITION channel1_type3 VALUES (5, 6, 7),
        SUBPARTITION channel1_type4 VALUES (8, 9)
    ),
    PARTITION channel2 VALUES ('3', '4', '5')
    (
        SUBPARTITION channel2_type1 VALUES (0, 1, 2, 3),
        SUBPARTITION channel2_type2 VALUES (DEFAULT)
    ),
    PARTITION channel3 VALUES ('6', '7'),
    PARTITION channel4 VALUES ('8', '9')
    (
        SUBPARTITION channel4_type1 VALUES (0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
    )
);
INSERT INTO list_list_sales SELECT generate_series(1,1000),
                                   generate_series(1,1000),
                                   date_pli('2008-01-01', generate_series(1,1000)),
                                   generate_series(1,1000)%10,
                                   generate_series(1,1000)%10,
                                   generate_series(1,1000)%1000,
                                   generate_series(1,1000);
CREATE INDEX list_list_sales_idx ON list_list_sales(product_id) LOCAL;

--check for add partition/subpartition
--fail, value conflict
ALTER TABLE list_list_sales ADD PARTITION channel_temp1 VALUES ('X')
    (
        SUBPARTITION channel_temp1_type1 VALUES (0, 1, 2),
        SUBPARTITION channel_temp1_type2 VALUES (3, 4, 5),
        SUBPARTITION channel_temp1_type3 VALUES (6, 7, 5)
    );
--fail, value conflict
ALTER TABLE list_list_sales ADD PARTITION channel_temp2 VALUES ('X')
    (
        SUBPARTITION channel_temp2_type1 VALUES (0, 1, 2),
        SUBPARTITION channel_temp2_type2 VALUES (3, 4, 5),
        SUBPARTITION channel_temp2_type3 VALUES (6, 7, 8, 7, 8)
    );
--success, add 4 subpartition
ALTER TABLE list_list_sales ADD PARTITION channel5 VALUES ('X')
    (
        SUBPARTITION channel5_type1 VALUES (0, 1, 2),
        SUBPARTITION channel5_type2 VALUES (3, 4),
        SUBPARTITION channel5_type3 VALUES (5, 6, 7),
        SUBPARTITION channel5_type4 VALUES (8, 9)
    );
--fail, value conflict
ALTER TABLE list_list_sales ADD PARTITION channel_temp3 VALUES ('0', 'Z', 'C');
--fail, value conflict
ALTER TABLE list_list_sales ADD PARTITION channel_temp4 VALUES ('Z', 'Z', 'C');
--fail, invalid format
ALTER TABLE list_list_sales ADD PARTITION channel_temp5 VALUES LESS THAN ('Z');
--success, add 1 default subpartition
ALTER TABLE list_list_sales ADD PARTITION channel6 VALUES (DEFAULT);
--fail, value conflict
ALTER TABLE list_list_sales ADD PARTITION channel_temp6 VALUES ('M', 'X');
--success, add 1 subpartition
ALTER TABLE list_list_sales MODIFY PARTITION channel1 ADD SUBPARTITION channel1_type5 VALUES (DEFAULT);
--fail, out of range
ALTER TABLE list_list_sales MODIFY PARTITION channel2 ADD SUBPARTITION channel2_temp1 VALUES (10, 11, 12);
--fail, out of range
ALTER TABLE list_list_sales MODIFY PARTITION channel3 ADD SUBPARTITION channel3_temp1 VALUES (10, 11, 12);
--fail, invalid format
ALTER TABLE list_list_sales MODIFY PARTITION channel4 ADD SUBPARTITION channel4_temp1 VALUES LESS THAN (1500);

--check for ok after add
SELECT p1.relname, p1.parttype, p1.partstrategy, p1.relfilenode!=0 hasfilenode, p1.reltablespace, p1.partkey, p1.boundaries
    FROM pg_class c1, pg_partition p1, pg_namespace n1
    WHERE c1.relname='list_list_sales'
        AND c1.relnamespace=n1.oid
        AND n1.nspname=CURRENT_SCHEMA
        AND (p1.parentid=c1.oid
            OR p1.parentid IN (
                SELECT p2.oid FROM pg_class c2, pg_partition p2, pg_namespace n2
                    WHERE c2.relname='list_list_sales'
                        AND c2.relnamespace=n2.oid
                        AND n2.nspname=CURRENT_SCHEMA
                        AND (p2.parentid=c2.oid)
            ))
    ORDER BY p1.parttype, p1.relname;
SELECT p1.relname, p1.parttype, p1.partstrategy, p1.relfilenode!=0 hasfilenode, p1.indisusable
    FROM pg_class c1, pg_partition p1, pg_namespace n1
    WHERE c1.relname='list_list_sales_idx'
        AND c1.relnamespace=n1.oid
        AND n1.nspname=CURRENT_SCHEMA
        AND (p1.parentid=c1.oid)
    ORDER BY p1.relname;
\d+ list_list_sales

--check for drop partition/subpartition (for)
--success, drop partition channel2
ALTER TABLE list_list_sales DROP PARTITION channel2;
--success
ALTER TABLE list_list_sales DROP SUBPARTITION channel1_type1;
--fail, the only subpartition
ALTER TABLE list_list_sales DROP SUBPARTITION channel4_type1;
--success, drop partition channel3
ALTER TABLE list_list_sales DROP PARTITION FOR ('6');
--fail, number not equal to the number of partkey
ALTER TABLE list_list_sales DROP PARTITION FOR('X', 6);
--fail, number not equal to the number of partkey
ALTER TABLE list_list_sales DROP SUBPARTITION FOR('X');
--fail, invalid type
ALTER TABLE list_list_sales DROP PARTITION FOR (10);
--fail, invalid type
ALTER TABLE list_list_sales DROP SUBPARTITION FOR(10, 'X');
--success, drop subpartition channel5_type3
ALTER TABLE list_list_sales DROP SUBPARTITION FOR('X', 6);
--fail, the only subpartition in channel6
ALTER TABLE list_list_sales DROP SUBPARTITION FOR('K', 10);
--fail, no subpartition find
ALTER TABLE list_list_sales DROP SUBPARTITION FOR('X', 5);

--check for ok after drop
SELECT count(*) FROM list_list_sales;
SELECT p1.relname, p1.parttype, p1.partstrategy, p1.relfilenode!=0 hasfilenode, p1.reltablespace, p1.partkey, p1.boundaries
    FROM pg_class c1, pg_partition p1, pg_namespace n1
    WHERE c1.relname='list_list_sales'
        AND c1.relnamespace=n1.oid
        AND n1.nspname=CURRENT_SCHEMA
        AND (p1.parentid=c1.oid
            OR p1.parentid IN (
                SELECT p2.oid FROM pg_class c2, pg_partition p2, pg_namespace n2
                    WHERE c2.relname='list_list_sales'
                        AND c2.relnamespace=n2.oid
                        AND n2.nspname=CURRENT_SCHEMA
                        AND (p2.parentid=c2.oid)
            ))
    ORDER BY p1.parttype, p1.relname;
SELECT p1.relname, p1.parttype, p1.partstrategy, p1.relfilenode!=0 hasfilenode, p1.indisusable
    FROM pg_class c1, pg_partition p1, pg_namespace n1
    WHERE c1.relname='list_list_sales_idx'
        AND c1.relnamespace=n1.oid
        AND n1.nspname=CURRENT_SCHEMA
        AND (p1.parentid=c1.oid)
    ORDER BY p1.relname;
\d+ list_list_sales

--
----list-hash table----
--
--prepare
CREATE TABLE list_hash_sales
(
    product_id     INT4 NOT NULL,
    customer_id    INT4 PRIMARY KEY,
    time_id        DATE,
    channel_id     CHAR(1),
    type_id        INT4,
    quantity_sold  NUMERIC(3),
    amount_sold    NUMERIC(10,2)
)
PARTITION BY LIST (channel_id) SUBPARTITION BY HASH (product_id)
(
    PARTITION channel1 VALUES ('0', '1', '2')
    (
        SUBPARTITION channel1_product1,
        SUBPARTITION channel1_product2,
        SUBPARTITION channel1_product3,
        SUBPARTITION channel1_product4
    ),
    PARTITION channel2 VALUES ('3', '4', '5')
    (
        SUBPARTITION channel2_product1,
        SUBPARTITION channel2_product2
    ),
    PARTITION channel3 VALUES ('6', '7'),
    PARTITION channel4 VALUES ('8', '9')
    (
        SUBPARTITION channel4_product1
    )
);
INSERT INTO list_hash_sales SELECT generate_series(1,1000),
                                   generate_series(1,1000),
                                   date_pli('2008-01-01', generate_series(1,1000)),
                                   generate_series(1,1000)%10,
                                   generate_series(1,1000)%10,
                                   generate_series(1,1000)%1000,
                                   generate_series(1,1000);
CREATE INDEX list_hash_sales_idx ON list_hash_sales(product_id) LOCAL;

--check for add partition/subpartition
--success, add 4 subpartition
ALTER TABLE list_hash_sales ADD PARTITION channel5 VALUES ('X')
    (
        SUBPARTITION channel5_product1,
        SUBPARTITION channel5_product2,
        SUBPARTITION channel5_product3,
        SUBPARTITION channel5_product4
    );
--fail, value conflict
ALTER TABLE list_hash_sales ADD PARTITION channel_temp1 VALUES ('0', 'Z', 'C');
--fail, value conflict
ALTER TABLE list_hash_sales ADD PARTITION channel_temp2 VALUES ('Z', 'Z', 'C');
--fail, invalid format
ALTER TABLE list_hash_sales ADD PARTITION channel_temp3 VALUES LESS THAN ('Z');
--success, add 1 default subpartition
ALTER TABLE list_hash_sales ADD PARTITION channel6 VALUES (DEFAULT);
--fail, value conflict
ALTER TABLE list_hash_sales ADD PARTITION channel_temp4 VALUES ('M', 'X');
--fail, not support add hash
ALTER TABLE list_hash_sales MODIFY PARTITION channel1 ADD SUBPARTITION channel1_temp1;
--fail, invalid format
ALTER TABLE list_hash_sales MODIFY PARTITION channel4 ADD SUBPARTITION channel4_temp1 VALUES LESS THAN (1500);

--check for ok after add
SELECT p1.relname, p1.parttype, p1.partstrategy, p1.relfilenode!=0 hasfilenode, p1.reltablespace, p1.partkey, p1.boundaries
    FROM pg_class c1, pg_partition p1, pg_namespace n1
    WHERE c1.relname='list_hash_sales'
        AND c1.relnamespace=n1.oid
        AND n1.nspname=CURRENT_SCHEMA
        AND (p1.parentid=c1.oid
            OR p1.parentid IN (
                SELECT p2.oid FROM pg_class c2, pg_partition p2, pg_namespace n2
                    WHERE c2.relname='list_hash_sales'
                        AND c2.relnamespace=n2.oid
                        AND n2.nspname=CURRENT_SCHEMA
                        AND (p2.parentid=c2.oid)
            ))
    ORDER BY p1.parttype, p1.relname;
SELECT p1.relname, p1.parttype, p1.partstrategy, p1.relfilenode!=0 hasfilenode, p1.indisusable
    FROM pg_class c1, pg_partition p1, pg_namespace n1
    WHERE c1.relname='list_hash_sales_idx'
        AND c1.relnamespace=n1.oid
        AND n1.nspname=CURRENT_SCHEMA
        AND (p1.parentid=c1.oid)
    ORDER BY p1.relname;
\d+ list_hash_sales

--check for drop partition/subpartition (for)
--success, drop partition channel2
ALTER TABLE list_hash_sales DROP PARTITION channel2;
--fail, not support drop hash
ALTER TABLE list_hash_sales DROP SUBPARTITION channel1_product1;
--fail, not support drop hash
ALTER TABLE list_hash_sales DROP SUBPARTITION channel4_product1;
--success, drop partition channel3
ALTER TABLE list_hash_sales DROP PARTITION FOR ('6');
--fail, number not equal to the number of partkey
ALTER TABLE list_hash_sales DROP PARTITION FOR ('6', '2010-01-01');
--fail, invalid type
ALTER TABLE list_hash_sales DROP PARTITION FOR (10);
--fail, not support drop hash
ALTER TABLE list_hash_sales DROP SUBPARTITION FOR('X', 6);

--check for ok after drop
SELECT count(*) FROM list_hash_sales;
SELECT p1.relname, p1.parttype, p1.partstrategy, p1.relfilenode!=0 hasfilenode, p1.reltablespace, p1.partkey, p1.boundaries
    FROM pg_class c1, pg_partition p1, pg_namespace n1
    WHERE c1.relname='list_hash_sales'
        AND c1.relnamespace=n1.oid
        AND n1.nspname=CURRENT_SCHEMA
        AND (p1.parentid=c1.oid
            OR p1.parentid IN (
                SELECT p2.oid FROM pg_class c2, pg_partition p2, pg_namespace n2
                    WHERE c2.relname='list_hash_sales'
                        AND c2.relnamespace=n2.oid
                        AND n2.nspname=CURRENT_SCHEMA
                        AND (p2.parentid=c2.oid)
            ))
    ORDER BY p1.parttype, p1.relname;
SELECT p1.relname, p1.parttype, p1.partstrategy, p1.relfilenode!=0 hasfilenode, p1.indisusable
    FROM pg_class c1, pg_partition p1, pg_namespace n1
    WHERE c1.relname='list_hash_sales_idx'
        AND c1.relnamespace=n1.oid
        AND n1.nspname=CURRENT_SCHEMA
        AND (p1.parentid=c1.oid)
    ORDER BY p1.relname;
\d+ list_hash_sales

--
----hash-range table----
--
--prepare
CREATE TABLE hash_range_sales
(
    product_id     INT4 NOT NULL,
    customer_id    INT4 PRIMARY KEY,
    time_id        DATE,
    channel_id     CHAR(1),
    type_id        INT4,
    quantity_sold  NUMERIC(3),
    amount_sold    NUMERIC(10,2)
)
PARTITION BY HASH (product_id) SUBPARTITION BY RANGE (customer_id)
(
    PARTITION product1
    (
        SUBPARTITION product1_customer1 VALUES LESS THAN (200),
        SUBPARTITION product1_customer2 VALUES LESS THAN (500),
        SUBPARTITION product1_customer3 VALUES LESS THAN (800),
        SUBPARTITION product1_customer4 VALUES LESS THAN (1200)
    ),
    PARTITION product2
    (
        SUBPARTITION product2_customer1 VALUES LESS THAN (500),
        SUBPARTITION product2_customer2 VALUES LESS THAN (MAXVALUE)
    ),
    PARTITION product3,
    PARTITION product4
    (
        SUBPARTITION product4_customer1 VALUES LESS THAN (1200)
    )
);
INSERT INTO hash_range_sales SELECT generate_series(1,1000),
                                    generate_series(1,1000),
                                    date_pli('2008-01-01', generate_series(1,1000)),
                                    generate_series(1,1000)%10,
                                    generate_series(1,1000)%10,
                                    generate_series(1,1000)%1000,
                                    generate_series(1,1000);
CREATE INDEX hash_range_sales_idx ON hash_range_sales(product_id) LOCAL;

--check for add partition/subpartition
--fail, not support add hash
ALTER TABLE hash_range_sales ADD PARTITION product_temp1
    (
        SUBPARTITION product_temp1_customer1 VALUES LESS THAN (200),
        SUBPARTITION product_temp1_customer2 VALUES LESS THAN (500),
        SUBPARTITION product_temp1_customer3 VALUES LESS THAN (800),
        SUBPARTITION product_temp1_customer4 VALUES LESS THAN (1200)
    );
--fail, not support add hash
ALTER TABLE hash_range_sales ADD PARTITION product_temp2;
--success, add 1 subpartition
ALTER TABLE hash_range_sales MODIFY PARTITION product1 ADD SUBPARTITION product1_customer5 VALUES LESS THAN (1800);
--fail, out of range
ALTER TABLE hash_range_sales MODIFY PARTITION product2 ADD SUBPARTITION product2_temp1 VALUES LESS THAN (1800);
--fail, invalid format
ALTER TABLE hash_range_sales MODIFY PARTITION product4 ADD SUBPARTITION product4_temp1 VALUES (DEFAULT);
--success, add 1 subpartition
ALTER TABLE hash_range_sales MODIFY PARTITION product4 ADD SUBPARTITION product4_customer2 VALUES LESS THAN (MAXVALUE);

--check for ok after add
SELECT p1.relname, p1.parttype, p1.partstrategy, p1.relfilenode!=0 hasfilenode, p1.reltablespace, p1.partkey, p1.boundaries
    FROM pg_class c1, pg_partition p1, pg_namespace n1
    WHERE c1.relname='hash_range_sales'
        AND c1.relnamespace=n1.oid
        AND n1.nspname=CURRENT_SCHEMA
        AND (p1.parentid=c1.oid
            OR p1.parentid IN (
                SELECT p2.oid FROM pg_class c2, pg_partition p2, pg_namespace n2
                    WHERE c2.relname='hash_range_sales'
                        AND c2.relnamespace=n2.oid
                        AND n2.nspname=CURRENT_SCHEMA
                        AND (p2.parentid=c2.oid)
            ))
    ORDER BY p1.parttype, p1.relname;
SELECT p1.relname, p1.parttype, p1.partstrategy, p1.relfilenode!=0 hasfilenode, p1.indisusable
    FROM pg_class c1, pg_partition p1, pg_namespace n1
    WHERE c1.relname='hash_range_sales_idx'
        AND c1.relnamespace=n1.oid
        AND n1.nspname=CURRENT_SCHEMA
        AND (p1.parentid=c1.oid)
    ORDER BY p1.relname;
\d+ hash_range_sales

--check for drop partition/subpartition (for)
--fail, not support drop hash
ALTER TABLE hash_range_sales DROP PARTITION product2;
--success, drop subpartition product1_customer1
ALTER TABLE hash_range_sales DROP SUBPARTITION product1_customer1;
--success, drop subpartition product4_customer1
ALTER TABLE hash_range_sales DROP SUBPARTITION product4_customer1;
--fail, the only subpartition in product4
ALTER TABLE hash_range_sales DROP SUBPARTITION product4_customer2;
--fail, not support drop hash
ALTER TABLE hash_range_sales DROP PARTITION FOR(0);
--fail, not support drop hash
ALTER TABLE hash_range_sales DROP PARTITION FOR(0, 100);
--fail, number not equal to the number of partkey
ALTER TABLE hash_range_sales DROP SUBPARTITION FOR(0);
--fail, invalid type
ALTER TABLE hash_range_sales DROP SUBPARTITION FOR('2010-01-01', 100);
--success, drop subpartition product1_customer2, but not suggest to do this operation
ALTER TABLE hash_range_sales DROP SUBPARTITION FOR(0, 100);
--fail, no subpartition find
ALTER TABLE hash_range_sales DROP SUBPARTITION FOR(0, 2300);

--check for ok after drop
SELECT count(*) FROM hash_range_sales;
SELECT p1.relname, p1.parttype, p1.partstrategy, p1.relfilenode!=0 hasfilenode, p1.reltablespace, p1.partkey, p1.boundaries
    FROM pg_class c1, pg_partition p1, pg_namespace n1
    WHERE c1.relname='hash_range_sales'
        AND c1.relnamespace=n1.oid
        AND n1.nspname=CURRENT_SCHEMA
        AND (p1.parentid=c1.oid
            OR p1.parentid IN (
                SELECT p2.oid FROM pg_class c2, pg_partition p2, pg_namespace n2
                    WHERE c2.relname='hash_range_sales'
                        AND c2.relnamespace=n2.oid
                        AND n2.nspname=CURRENT_SCHEMA
                        AND (p2.parentid=c2.oid)
            ))
    ORDER BY p1.parttype, p1.relname;
SELECT p1.relname, p1.parttype, p1.partstrategy, p1.relfilenode!=0 hasfilenode, p1.indisusable
    FROM pg_class c1, pg_partition p1, pg_namespace n1
    WHERE c1.relname='hash_range_sales_idx'
        AND c1.relnamespace=n1.oid
        AND n1.nspname=CURRENT_SCHEMA
        AND (p1.parentid=c1.oid)
    ORDER BY p1.relname;
\d+ hash_range_sales

--
----hash-list table----
--
--prepare
CREATE TABLE hash_list_sales
(
    product_id     INT4 NOT NULL,
    customer_id    INT4 PRIMARY KEY,
    time_id        DATE,
    channel_id     CHAR(1),
    type_id        INT4,
    quantity_sold  NUMERIC(3),
    amount_sold    NUMERIC(10,2)
)
PARTITION BY HASH (product_id) SUBPARTITION BY LIST (channel_id)
(
    PARTITION product1
    (
        SUBPARTITION product1_channel1 VALUES ('0', '1', '2'),
        SUBPARTITION product1_channel2 VALUES ('3', '4', '5'),
        SUBPARTITION product1_channel3 VALUES ('6', '7', '8'),
        SUBPARTITION product1_channel4 VALUES ('9')
    ),
    PARTITION product2
    (
        SUBPARTITION product2_channel1 VALUES ('0', '1', '2', '3', '4'),
        SUBPARTITION product2_channel2 VALUES (DEFAULT)
    ),
    PARTITION product3,
    PARTITION product4
    (
        SUBPARTITION product4_channel1 VALUES ('0', '1', '2', '3', '4', '5', '6', '7', '8', '9')
    )
);
INSERT INTO hash_list_sales SELECT generate_series(1,1000),
                                   generate_series(1,1000),
                                   date_pli('2008-01-01', generate_series(1,1000)),
                                   generate_series(1,1000)%10,
                                   generate_series(1,1000)%10,
                                   generate_series(1,1000)%1000,
                                   generate_series(1,1000);
CREATE INDEX hash_list_sales_idx ON hash_list_sales(product_id) LOCAL;

--check for add partition/subpartition
--fail, not support add hash
ALTER TABLE hash_list_sales ADD PARTITION product_temp1
    (
        SUBPARTITION product_temp1_channel1 VALUES ('0', '1', '2'),
        SUBPARTITION product_temp1_channel2 VALUES ('3', '4', '5'),
        SUBPARTITION product_temp1_channel3 VALUES ('6', '7', '8'),
        SUBPARTITION product_temp1_channel4 VALUES ('9')
    );
--fail, not support add hash
ALTER TABLE hash_list_sales ADD PARTITION product_temp2;
--success, add 1 subpartition
ALTER TABLE hash_list_sales MODIFY PARTITION product1 ADD SUBPARTITION product1_channel5 VALUES ('X');
--fail, out of range
ALTER TABLE hash_list_sales MODIFY PARTITION product2 ADD SUBPARTITION product2_temp1 VALUES ('X');
--fail, out of range
ALTER TABLE hash_list_sales MODIFY PARTITION product3 ADD SUBPARTITION product3_temp1 VALUES ('X');
--fail, invalid format
ALTER TABLE hash_list_sales MODIFY PARTITION product4 ADD SUBPARTITION product4_temp1 VALUES LESS THAN (MAXVALUE);
--success, add 1 subpartition
ALTER TABLE hash_list_sales MODIFY PARTITION product4 ADD SUBPARTITION product4_channel2 VALUES (DEFAULT);

--check for ok after add
SELECT p1.relname, p1.parttype, p1.partstrategy, p1.relfilenode!=0 hasfilenode, p1.reltablespace, p1.partkey, p1.boundaries
    FROM pg_class c1, pg_partition p1, pg_namespace n1
    WHERE c1.relname='hash_list_sales'
        AND c1.relnamespace=n1.oid
        AND n1.nspname=CURRENT_SCHEMA
        AND (p1.parentid=c1.oid
            OR p1.parentid IN (
                SELECT p2.oid FROM pg_class c2, pg_partition p2, pg_namespace n2
                    WHERE c2.relname='hash_list_sales'
                        AND c2.relnamespace=n2.oid
                        AND n2.nspname=CURRENT_SCHEMA
                        AND (p2.parentid=c2.oid)
            ))
    ORDER BY p1.parttype, p1.relname;
SELECT p1.relname, p1.parttype, p1.partstrategy, p1.relfilenode!=0 hasfilenode, p1.indisusable
    FROM pg_class c1, pg_partition p1, pg_namespace n1
    WHERE c1.relname='hash_list_sales_idx'
        AND c1.relnamespace=n1.oid
        AND n1.nspname=CURRENT_SCHEMA
        AND (p1.parentid=c1.oid)
    ORDER BY p1.relname;
\d+ hash_list_sales

--check for drop partition/subpartition (for)
--fail, not support drop hash
ALTER TABLE hash_list_sales DROP PARTITION product2;
--success, drop subpartition product1_channel1
ALTER TABLE hash_list_sales DROP SUBPARTITION product1_channel1;
--success, drop subpartition product4_channel1
ALTER TABLE hash_list_sales DROP SUBPARTITION product4_channel1;
--fail, the only subpartition in product4
ALTER TABLE hash_list_sales DROP SUBPARTITION product4_channel2;
--fail, not support drop hash
ALTER TABLE hash_list_sales DROP PARTITION FOR(0);
--fail, not support drop hash
ALTER TABLE hash_list_sales DROP PARTITION FOR(0, '4');
--fail, number not equal to the number of partkey
ALTER TABLE hash_list_sales DROP SUBPARTITION FOR(0);
--fail, invalid type
ALTER TABLE hash_list_sales DROP SUBPARTITION FOR('2010-01-01', '4');
--success, drop subpartition product1_channel2, but not suggest to do this operation
ALTER TABLE hash_list_sales DROP SUBPARTITION FOR(0, '4');
--fail, no subpartition find
ALTER TABLE hash_list_sales DROP SUBPARTITION FOR(0, 'Z');

--check for ok after drop
SELECT count(*) FROM hash_list_sales;
SELECT p1.relname, p1.parttype, p1.partstrategy, p1.relfilenode!=0 hasfilenode, p1.reltablespace, p1.partkey, p1.boundaries
    FROM pg_class c1, pg_partition p1, pg_namespace n1
    WHERE c1.relname='hash_list_sales'
        AND c1.relnamespace=n1.oid
        AND n1.nspname=CURRENT_SCHEMA
        AND (p1.parentid=c1.oid
            OR p1.parentid IN (
                SELECT p2.oid FROM pg_class c2, pg_partition p2, pg_namespace n2
                    WHERE c2.relname='hash_list_sales'
                        AND c2.relnamespace=n2.oid
                        AND n2.nspname=CURRENT_SCHEMA
                        AND (p2.parentid=c2.oid)
            ))
    ORDER BY p1.parttype, p1.relname;
SELECT p1.relname, p1.parttype, p1.partstrategy, p1.relfilenode!=0 hasfilenode, p1.indisusable
    FROM pg_class c1, pg_partition p1, pg_namespace n1
    WHERE c1.relname='hash_list_sales_idx'
        AND c1.relnamespace=n1.oid
        AND n1.nspname=CURRENT_SCHEMA
        AND (p1.parentid=c1.oid)
    ORDER BY p1.relname;
\d+ hash_list_sales

--
----hash-hash table----
--
--prepare
CREATE TABLE hash_hash_sales
(
    product_id     INT4 NOT NULL,
    customer_id    INT4 PRIMARY KEY,
    time_id        DATE,
    channel_id     CHAR(1),
    type_id        INT4,
    quantity_sold  NUMERIC(3),
    amount_sold    NUMERIC(10,2)
)
PARTITION BY HASH (product_id) SUBPARTITION BY HASH (customer_id)
(
    PARTITION product1
    (
        SUBPARTITION product1_customer1,
        SUBPARTITION product1_customer2,
        SUBPARTITION product1_customer3,
        SUBPARTITION product1_customer4
    ),
    PARTITION product2
    (
        SUBPARTITION product2_customer1,
        SUBPARTITION product2_customer2
    ),
    PARTITION product3,
    PARTITION product4
    (
        SUBPARTITION product4_customer1
    )
);
INSERT INTO hash_hash_sales SELECT generate_series(1,1000),
                                   generate_series(1,1000),
                                   date_pli('2008-01-01', generate_series(1,1000)),
                                   generate_series(1,1000)%10,
                                   generate_series(1,1000)%10,
                                   generate_series(1,1000)%1000,
                                   generate_series(1,1000);
CREATE INDEX hash_hash_sales_idx ON hash_hash_sales(product_id) LOCAL;

--check for add partition/subpartition
--fail, not support add hash
ALTER TABLE hash_hash_sales ADD PARTITION product_temp1
    (
        SUBPARTITION product_temp1_customer1,
        SUBPARTITION product_temp1_customer2,
        SUBPARTITION product_temp1_customer3,
        SUBPARTITION product_temp1_customer4
    );
--fail, not support add hash
ALTER TABLE hash_hash_sales ADD PARTITION product_temp2;
--fail, not support add hash
ALTER TABLE hash_hash_sales MODIFY PARTITION product1 ADD SUBPARTITION product1_temp1;

--check for ok after add
SELECT p1.relname, p1.parttype, p1.partstrategy, p1.relfilenode!=0 hasfilenode, p1.reltablespace, p1.partkey, p1.boundaries
    FROM pg_class c1, pg_partition p1, pg_namespace n1
    WHERE c1.relname='hash_hash_sales'
        AND c1.relnamespace=n1.oid
        AND n1.nspname=CURRENT_SCHEMA
        AND (p1.parentid=c1.oid
            OR p1.parentid IN (
                SELECT p2.oid FROM pg_class c2, pg_partition p2, pg_namespace n2
                    WHERE c2.relname='hash_hash_sales'
                        AND c2.relnamespace=n2.oid
                        AND n2.nspname=CURRENT_SCHEMA
                        AND (p2.parentid=c2.oid)
            ))
    ORDER BY p1.parttype, p1.relname;
SELECT p1.relname, p1.parttype, p1.partstrategy, p1.relfilenode!=0 hasfilenode, p1.indisusable
    FROM pg_class c1, pg_partition p1, pg_namespace n1
    WHERE c1.relname='hash_hash_sales_idx'
        AND c1.relnamespace=n1.oid
        AND n1.nspname=CURRENT_SCHEMA
        AND (p1.parentid=c1.oid)
    ORDER BY p1.relname;
\d+ hash_hash_sales

--check for drop partition/subpartition (for)
--fail, not support drop hash
ALTER TABLE hash_hash_sales DROP PARTITION product2;
--fail, not support drop hash
ALTER TABLE hash_hash_sales DROP SUBPARTITION product1_customer1;
--fail, not support drop hash
ALTER TABLE hash_hash_sales DROP SUBPARTITION product4_customer1;
--fail, not support drop hash
ALTER TABLE hash_hash_sales DROP PARTITION FOR(0);
--fail, not support drop hash
ALTER TABLE hash_hash_sales DROP PARTITION FOR(0, 0);
--fail, not support drop hash
ALTER TABLE hash_hash_sales DROP SUBPARTITION FOR(0, 0);
--fail, not support drop hash
ALTER TABLE hash_hash_sales DROP SUBPARTITION FOR(0);

--check for ok after drop
SELECT count(*) FROM hash_hash_sales;
SELECT p1.relname, p1.parttype, p1.partstrategy, p1.relfilenode!=0 hasfilenode, p1.reltablespace, p1.partkey, p1.boundaries
    FROM pg_class c1, pg_partition p1, pg_namespace n1
    WHERE c1.relname='hash_hash_sales'
        AND c1.relnamespace=n1.oid
        AND n1.nspname=CURRENT_SCHEMA
        AND (p1.parentid=c1.oid
            OR p1.parentid IN (
                SELECT p2.oid FROM pg_class c2, pg_partition p2, pg_namespace n2
                    WHERE c2.relname='hash_hash_sales'
                        AND c2.relnamespace=n2.oid
                        AND n2.nspname=CURRENT_SCHEMA
                        AND (p2.parentid=c2.oid)
            ))
    ORDER BY p1.parttype, p1.relname;
SELECT p1.relname, p1.parttype, p1.partstrategy, p1.relfilenode!=0 hasfilenode, p1.indisusable
    FROM pg_class c1, pg_partition p1, pg_namespace n1
    WHERE c1.relname='hash_hash_sales_idx'
        AND c1.relnamespace=n1.oid
        AND n1.nspname=CURRENT_SCHEMA
        AND (p1.parentid=c1.oid)
    ORDER BY p1.relname;
\d+ hash_hash_sales

--finish
DROP TABLE range_range_sales;
DROP TABLE range_list_sales;
DROP TABLE range_hash_sales;
DROP TABLE list_range_sales;
DROP TABLE list_list_sales;
DROP TABLE list_hash_sales;
DROP TABLE hash_range_sales;
DROP TABLE hash_list_sales;
DROP TABLE hash_hash_sales;

DROP SCHEMA hw_subpartition_add_drop_partition CASCADE;
RESET CURRENT_SCHEMA;
