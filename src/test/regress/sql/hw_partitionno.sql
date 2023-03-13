-- prepare
DROP SCHEMA partitionno CASCADE;
CREATE SCHEMA partitionno;
SET CURRENT_SCHEMA TO partitionno;

PREPARE partition_get_partitionno AS
SELECT relname, partitionno, subpartitionno, boundaries
FROM pg_partition
WHERE parentid = (
    SELECT c.oid
    FROM pg_class c
       JOIN pg_namespace n ON c.relnamespace = n.oid
    WHERE c.relname = $1
       AND n.nspname = CURRENT_SCHEMA
)
ORDER BY relname;

PREPARE subpartition_get_partitionno AS
WITH partition_oid AS (
       SELECT oid
       FROM pg_partition
       WHERE parentid = (
           SELECT c.oid
           FROM pg_class c
               JOIN pg_namespace n ON c.relnamespace = n.oid
           WHERE c.relname = $1
               AND n.nspname = CURRENT_SCHEMA
       )
    )
SELECT relname, partitionno, subpartitionno, boundaries
FROM pg_partition p
    JOIN partition_oid part
    ON p.oid = part.oid OR p.parentid = part.oid
ORDER BY relname;

--
-- 1. test for range partition
--
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
CREATE INDEX range_sales_idx1 ON range_sales(product_id) LOCAL;
CREATE INDEX range_sales_idx2 ON range_sales(time_id) GLOBAL;
EXECUTE partition_get_partitionno('range_sales');

-- add/drop partition
ALTER TABLE range_sales ADD PARTITION time_default VALUES LESS THAN (MAXVALUE);
ALTER TABLE range_sales DROP PARTITION time_2008;
EXECUTE partition_get_partitionno('range_sales');

-- merge/split partition
ALTER TABLE range_sales SPLIT PARTITION time_default AT ('2013-01-01') INTO (PARTITION time_2012, PARTITION time_default_temp);
ALTER TABLE range_sales RENAME PARTITION time_default_temp TO time_default;
ALTER TABLE range_sales MERGE PARTITIONS time_2009, time_2010 INTO PARTITION time_2010_old UPDATE GLOBAL INDEX;
EXECUTE partition_get_partitionno('range_sales');

-- truncate partition with gpi
ALTER TABLE range_sales TRUNCATE PARTITION time_2011 UPDATE GLOBAL INDEX;
EXECUTE partition_get_partitionno('range_sales');

-- vacuum full
VACUUM FULL range_sales;
EXECUTE partition_get_partitionno('range_sales');

--reset
ALTER TABLE range_sales RESET PARTITION;
EXECUTE partition_get_partitionno('range_sales');

DROP TABLE range_sales;

--
-- 2. test for interval partition
--
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
CREATE INDEX interval_sales_idx1 ON interval_sales(product_id) LOCAL;
CREATE INDEX interval_sales_idx2 ON interval_sales(time_id) GLOBAL;

EXECUTE partition_get_partitionno('interval_sales');

-- add/drop partition
INSERT INTO interval_sales VALUES (1,1,'2013-01-01','A',1,1,1);
INSERT INTO interval_sales VALUES (2,2,'2012-01-01','B',2,2,2);
ALTER TABLE interval_sales DROP PARTITION time_2008;
EXECUTE partition_get_partitionno('interval_sales');

-- merge/split partition
ALTER TABLE interval_sales SPLIT PARTITION time_2009 AT ('2009-01-01') INTO (PARTITION time_2008, PARTITION time_2009_temp);
ALTER TABLE interval_sales RENAME PARTITION time_2009_temp TO time_2009;
ALTER TABLE interval_sales MERGE PARTITIONS time_2009, time_2010 INTO PARTITION time_2010_old UPDATE GLOBAL INDEX;
EXECUTE partition_get_partitionno('interval_sales');

-- truncate partition with gpi
ALTER TABLE interval_sales TRUNCATE PARTITION time_2008 UPDATE GLOBAL INDEX;
EXECUTE partition_get_partitionno('interval_sales');

-- vacuum full
VACUUM FULL interval_sales;
EXECUTE partition_get_partitionno('interval_sales');

--reset
ALTER TABLE interval_sales RESET PARTITION;
EXECUTE partition_get_partitionno('interval_sales');

DROP TABLE interval_sales;

--
-- 3. test for list partition
--
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
CREATE INDEX list_sales_idx1 ON list_sales(product_id) LOCAL;
CREATE INDEX list_sales_idx2 ON list_sales(time_id) GLOBAL;

EXECUTE partition_get_partitionno('list_sales');

-- add/drop partition
ALTER TABLE list_sales ADD PARTITION channel_default VALUES (DEFAULT);
ALTER TABLE list_sales DROP PARTITION channel4;
EXECUTE partition_get_partitionno('list_sales');

-- truncate partition with gpi
ALTER TABLE list_sales TRUNCATE PARTITION channel2 UPDATE GLOBAL INDEX;
EXECUTE partition_get_partitionno('list_sales');

-- vacuum full
VACUUM FULL list_sales;
EXECUTE partition_get_partitionno('list_sales');

--reset
ALTER TABLE list_sales RESET PARTITION;
EXECUTE partition_get_partitionno('list_sales');

DROP TABLE list_sales;

--
-- 4. test for list-range partition
--
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
CREATE INDEX list_range_sales_idx ON list_range_sales(product_id) GLOBAL;

EXECUTE subpartition_get_partitionno('list_range_sales');

-- add/drop partition
ALTER TABLE list_range_sales DROP PARTITION channel3;
ALTER TABLE list_range_sales ADD PARTITION channel3 VALUES ('6', '7')
(
    SUBPARTITION channel3_customer1 VALUES LESS THAN (200),
    SUBPARTITION channel3_customer2 VALUES LESS THAN (500),
    SUBPARTITION channel3_customer3 VALUES LESS THAN (800)
);
ALTER TABLE list_range_sales ADD PARTITION channel5 VALUES (DEFAULT);
ALTER TABLE list_range_sales MODIFY PARTITION channel4 ADD SUBPARTITION channel4_customer2 VALUES LESS THAN (2000);
EXECUTE subpartition_get_partitionno('list_range_sales');

-- merge/split partition
ALTER TABLE list_range_sales SPLIT SUBPARTITION channel2_customer2 AT (800) INTO (SUBPARTITION channel2_customer3, SUBPARTITION channel2_customer4);
EXECUTE subpartition_get_partitionno('list_range_sales');

-- truncate partition with gpi
ALTER TABLE list_range_sales TRUNCATE PARTITION channel1 UPDATE GLOBAL INDEX;
ALTER TABLE list_range_sales TRUNCATE SUBPARTITION channel4_customer1 UPDATE GLOBAL INDEX;
EXECUTE subpartition_get_partitionno('list_range_sales');

-- vacuum full
VACUUM FULL list_range_sales;
EXECUTE subpartition_get_partitionno('list_range_sales');

--reset
ALTER TABLE list_range_sales RESET PARTITION;
EXECUTE subpartition_get_partitionno('list_range_sales');

DROP TABLE list_range_sales;

-- clean
DEALLOCATE partition_get_partitionno;
DEALLOCATE subpartition_get_partitionno;

DROP SCHEMA partitionno CASCADE;
