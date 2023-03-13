-- prepare
DROP SCHEMA partition_parallel CASCADE;
CREATE SCHEMA partition_parallel;
SET CURRENT_SCHEMA TO partition_parallel;

--
----range table----
--
--prepare
CREATE TABLE range_sales
(
    product_id     INT4 NOT NULL,
    customer_id    INT4 NOT NULL,
    time_id        DATE,
    channel_id     CHAR(1),
    type_id        INT4,
    quantity_sold  NUMERIC(3),
    amount_sold    NUMERIC(10,2)
) WITH (STORAGE_TYPE=USTORE)
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
CREATE INDEX range_sales_idx1 ON range_sales(channel_id) LOCAL;
CREATE INDEX range_sales_idx2 ON range_sales(customer_id) GLOBAL;

--create a temp table to exchange
CREATE TABLE range_temp
(
    product_id     INT4 NOT NULL,
    customer_id    INT4 NOT NULL,
    time_id        DATE,
    channel_id     CHAR(1),
    type_id        INT4,
    quantity_sold  NUMERIC(3),
    amount_sold    NUMERIC(10,2)
) WITH (STORAGE_TYPE=USTORE);
CREATE INDEX ON range_temp(channel_id);
INSERT INTO range_temp SELECT * FROM range_sales WHERE time_id < '2009-01-01';

--drop
\parallel on
ALTER TABLE range_sales DROP PARTITION time_2008 UPDATE GLOBAL INDEX;
UPDATE range_sales SET channel_id = 'X' WHERE time_id = '2010-06-01';
INSERT INTO range_sales VALUES(1,1,'2011-06-01', 'X',1,1,1);
SELECT /*+ tablescan(range_sales)*/ COUNT(*) FROM range_sales PARTITION (time_2010);
\parallel off
SELECT COUNT(*) FROM range_sales WHERE channel_id = 'X';

--split
ALTER TABLE range_sales RENAME PARTITION time_2009 TO time_2009_temp;
\parallel on
ALTER TABLE range_sales SPLIT PARTITION time_2009_temp AT ('2009-01-01')
    INTO (PARTITION time_2008, PARTITION time_2009) UPDATE GLOBAL INDEX;
UPDATE range_sales PARTITION (time_2010) SET channel_id = '0' WHERE channel_id = 'X';
DELETE FROM range_sales WHERE channel_id = 'X' AND time_id >= '2011-01-01';
\parallel off
SELECT COUNT(*) FROM range_sales WHERE channel_id = 'X';

--truncate
\parallel on
ALTER TABLE range_sales TRUNCATE PARTITION time_2008 UPDATE GLOBAL INDEX;
SELECT /*+ tablescan(range_sales)*/ COUNT(*) FROM range_sales PARTITION (time_2008);
SELECT /*+ indexonlyscan(range_sales range_sales_idx1)*/ COUNT(channel_id) FROM range_sales PARTITION (time_2008);
SELECT /*+ indexonlyscan(range_sales range_sales_idx2)*/ COUNT(customer_id) FROM range_sales;
UPDATE range_sales SET channel_id = 'X' WHERE time_id = '2010-06-01';
INSERT INTO range_sales VALUES(1,1,'2011-06-01', 'X',1,1,1);
\parallel off
SELECT COUNT(*) FROM range_sales WHERE channel_id = 'X';

--exchange
\parallel on
ALTER TABLE range_sales EXCHANGE PARTITION (time_2008) WITH TABLE range_temp UPDATE GLOBAL INDEX;
SELECT /*+ tablescan(range_sales)*/ COUNT(*) FROM range_sales PARTITION (time_2008);
SELECT /*+ indexonlyscan(range_sales range_sales_idx1)*/ COUNT(channel_id) FROM range_sales PARTITION (time_2008);
SELECT /*+ indexonlyscan(range_sales range_sales_idx2)*/ COUNT(customer_id) FROM range_sales;
UPDATE range_sales PARTITION (time_2010) SET channel_id = '0' WHERE channel_id = 'X';
DELETE FROM range_sales WHERE channel_id = 'X' AND time_id >= '2011-01-01';
\parallel off
SELECT COUNT(*) FROM range_sales WHERE channel_id = 'X';

--merge
\parallel on
ALTER TABLE range_sales MERGE PARTITIONS time_2008, time_2009 INTO PARTITION time_2009 UPDATE GLOBAL INDEX;
UPDATE range_sales SET channel_id = 'X' WHERE time_id = '2010-06-01';
INSERT INTO range_sales VALUES(1,1,'2011-06-01', 'X',1,1,1);
\parallel off
SELECT COUNT(*) FROM range_sales WHERE channel_id = 'X';

--finish
DROP TABLE range_sales;
DROP TABLE range_temp;

--
----list table----
--
--prepare
CREATE TABLE list_sales
(
    product_id     INT4 NOT NULL,
    customer_id    INT4 NOT NULL,
    time_id        DATE,
    channel_id     CHAR(1),
    type_id        INT4,
    quantity_sold  NUMERIC(3),
    amount_sold    NUMERIC(10,2)
) WITH (STORAGE_TYPE=USTORE)
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
CREATE INDEX list_sales_idx1 ON list_sales(channel_id) LOCAL;
CREATE INDEX list_sales_idx2 ON list_sales(type_id) GLOBAL;

--create a temp table to exchange
CREATE TABLE list_temp
(
    product_id     INT4 NOT NULL,
    customer_id    INT4 NOT NULL,
    time_id        DATE,
    channel_id     CHAR(1),
    type_id        INT4,
    quantity_sold  NUMERIC(3),
    amount_sold    NUMERIC(10,2)
) WITH (STORAGE_TYPE=USTORE);
CREATE INDEX ON list_temp(channel_id);
INSERT INTO list_temp SELECT * FROM list_sales WHERE channel_id in ('0', '1', '2');

--drop
\parallel on
ALTER TABLE list_sales DROP PARTITION channel1 UPDATE GLOBAL INDEX;
UPDATE list_sales SET type_id = -1 WHERE channel_id = '6';
INSERT INTO list_sales VALUES(1,1,'2011-06-01', '8',-1,1,1);
SELECT /*+ tablescan(list_sales)*/ COUNT(*) FROM list_sales PARTITION (channel3);
\parallel off
SELECT COUNT(*) FROM list_sales WHERE type_id = -1;

--add
\parallel on
ALTER TABLE list_sales ADD PARTITION channel1 VALUES ('0', '1', '2');
UPDATE list_sales PARTITION (channel3) SET type_id = 1 WHERE type_id = -1;
DELETE FROM list_sales WHERE type_id = -1 AND channel_id in  ('8', '9');
\parallel off
SELECT COUNT(*) FROM list_sales WHERE type_id = -1;

--truncate
\parallel on
ALTER TABLE list_sales TRUNCATE PARTITION channel1 UPDATE GLOBAL INDEX;
SELECT /*+ tablescan(list_sales)*/ COUNT(*) FROM list_sales PARTITION (channel1);
SELECT /*+ indexonlyscan(list_sales list_sales_idx1)*/ COUNT(channel_id) FROM list_sales PARTITION (channel1);
SELECT /*+ indexonlyscan(list_sales list_sales_idx2)*/ COUNT(type_id) FROM list_sales;
UPDATE list_sales SET type_id = -1 WHERE channel_id = '6';
INSERT INTO list_sales VALUES(1,1,'2011-06-01', '8',-1,1,1);
\parallel off
SELECT COUNT(*) FROM list_sales WHERE type_id = -1;

--exchange
\parallel on
ALTER TABLE list_sales EXCHANGE PARTITION (channel1) WITH TABLE list_temp UPDATE GLOBAL INDEX;
SELECT /*+ tablescan(list_sales)*/ COUNT(*) FROM list_sales PARTITION (channel1);
SELECT /*+ indexonlyscan(list_sales list_sales_idx1)*/ COUNT(channel_id) FROM list_sales PARTITION (channel1);
SELECT /*+ indexonlyscan(list_sales list_sales_idx2)*/ COUNT(type_id) FROM list_sales;
UPDATE list_sales PARTITION (channel3) SET type_id = 1 WHERE type_id = -1;
DELETE FROM list_sales WHERE type_id = -1 AND channel_id in  ('8', '9');
\parallel off
SELECT COUNT(*) FROM list_sales WHERE type_id = -1;

--finish
DROP TABLE list_sales;
DROP TABLE list_temp;

--
----interval table----
--
--prepare
CREATE TABLE interval_sales
(
    product_id     INT4 NOT NULL,
    customer_id    INT4 NOT NULL,
    time_id        DATE,
    channel_id     CHAR(1),
    type_id        INT4,
    quantity_sold  NUMERIC(3),
    amount_sold    NUMERIC(10,2)
) WITH (STORAGE_TYPE=USTORE)
PARTITION BY RANGE (time_id) INTERVAL ('1 year')
(
    PARTITION time_2008 VALUES LESS THAN ('2009-01-01'),
    PARTITION time_2009 VALUES LESS THAN ('2010-01-01'),
    PARTITION time_2010 VALUES LESS THAN ('2011-01-01'),
    PARTITION time_2011 VALUES LESS THAN ('2012-01-01')
);
INSERT INTO interval_sales SELECT generate_series(1,1000),
                                  generate_series(1,1000),
                                  date_pli('2008-01-01', generate_series(1,1000)),
                                  generate_series(1,1000)%10,
                                  generate_series(1,1000)%10,
                                  generate_series(1,1000)%1000,
                                  generate_series(1,1000);
CREATE INDEX interval_sales_idx1 ON interval_sales(channel_id) LOCAL;
CREATE INDEX interval_sales_idx2 ON interval_sales(customer_id) GLOBAL;

--create a temp table to exchange
CREATE TABLE interval_temp
(
    product_id     INT4 NOT NULL,
    customer_id    INT4 NOT NULL,
    time_id        DATE,
    channel_id     CHAR(1),
    type_id        INT4,
    quantity_sold  NUMERIC(3),
    amount_sold    NUMERIC(10,2)
) WITH (STORAGE_TYPE=USTORE);
CREATE INDEX ON interval_temp(channel_id);
INSERT INTO interval_temp SELECT * FROM interval_sales WHERE time_id < '2009-01-01';

--drop
\parallel on
ALTER TABLE interval_sales DROP PARTITION time_2008 UPDATE GLOBAL INDEX;
UPDATE interval_sales SET channel_id = 'X' WHERE time_id = '2010-06-01';
INSERT INTO interval_sales VALUES(1,1,'2011-06-01', 'X',1,1,1);
SELECT /*+ tablescan(interval_sales)*/ COUNT(*) FROM interval_sales PARTITION (time_2010);
\parallel off
SELECT COUNT(*) FROM interval_sales WHERE channel_id = 'X';

--split
ALTER TABLE interval_sales RENAME PARTITION time_2009 TO time_2009_temp;
\parallel on
ALTER TABLE interval_sales SPLIT PARTITION time_2009_temp AT ('2009-01-01')
    INTO (PARTITION time_2008, PARTITION time_2009) UPDATE GLOBAL INDEX;
UPDATE interval_sales PARTITION (time_2010) SET channel_id = '0' WHERE channel_id = 'X';
DELETE FROM interval_sales WHERE channel_id = 'X' AND time_id >= '2011-01-01';
\parallel off
SELECT COUNT(*) FROM interval_sales WHERE channel_id = 'X';

--truncate
\parallel on
ALTER TABLE interval_sales TRUNCATE PARTITION time_2008 UPDATE GLOBAL INDEX;
SELECT /*+ tablescan(interval_sales)*/ COUNT(*) FROM interval_sales PARTITION (time_2008);
SELECT /*+ indexonlyscan(interval_sales interval_sales_idx1)*/ COUNT(channel_id) FROM interval_sales PARTITION (time_2008);
SELECT /*+ indexonlyscan(interval_sales interval_sales_idx2)*/ COUNT(customer_id) FROM interval_sales;
UPDATE interval_sales SET channel_id = 'X' WHERE time_id = '2010-06-01';
INSERT INTO interval_sales VALUES(1,1,'2011-06-01', 'X',1,1,1);
\parallel off
SELECT COUNT(*) FROM interval_sales WHERE channel_id = 'X';

--exchange
\parallel on
ALTER TABLE interval_sales EXCHANGE PARTITION (time_2008) WITH TABLE interval_temp UPDATE GLOBAL INDEX;
SELECT /*+ tablescan(interval_sales)*/ COUNT(*) FROM interval_sales PARTITION (time_2008);
SELECT /*+ indexonlyscan(interval_sales interval_sales_idx1)*/ COUNT(channel_id) FROM interval_sales PARTITION (time_2008);
SELECT /*+ indexonlyscan(interval_sales interval_sales_idx2)*/ COUNT(customer_id) FROM interval_sales;
UPDATE interval_sales PARTITION (time_2010) SET channel_id = '0' WHERE channel_id = 'X';
DELETE FROM interval_sales WHERE channel_id = 'X' AND time_id >= '2011-01-01';
\parallel off
SELECT COUNT(*) FROM interval_sales WHERE channel_id = 'X';

--merge
\parallel on
ALTER TABLE interval_sales MERGE PARTITIONS time_2008, time_2009 INTO PARTITION time_2009 UPDATE GLOBAL INDEX;
UPDATE interval_sales SET channel_id = 'X' WHERE time_id = '2010-06-01';
INSERT INTO interval_sales VALUES(1,1,'2011-06-01', 'X',1,1,1);
\parallel off
SELECT COUNT(*) FROM interval_sales WHERE channel_id = 'X';

--insert
\parallel on
INSERT INTO interval_sales VALUES (1,1,'2017-06-01','1',1,1);
INSERT INTO interval_sales VALUES (1,1,'2017-07-01','1',1,1);
INSERT INTO interval_sales VALUES (1,1,'2018-06-01','1',1,1);
UPDATE interval_sales PARTITION (time_2010) SET channel_id = '0' WHERE channel_id = 'X';
DELETE FROM interval_sales WHERE channel_id = 'X' AND time_id >= '2011-01-01';
\parallel off
SELECT COUNT(*) FROM interval_sales WHERE channel_id = 'X';

--finish
DROP TABLE interval_sales;
DROP TABLE interval_temp;

--
----range-list table----
--
--prepare
CREATE TABLE range_list_sales
(
    product_id     INT4 NOT NULL,
    customer_id    INT4 NOT NULL,
    time_id        DATE,
    channel_id     CHAR(1),
    type_id        INT4,
    quantity_sold  NUMERIC(3),
    amount_sold    NUMERIC(10,2)
) WITH (STORAGE_TYPE=USTORE)
PARTITION BY RANGE (customer_id) SUBPARTITION BY LIST (channel_id)
(
    PARTITION customer1 VALUES LESS THAN (200)
    (
        SUBPARTITION customer1_channel1 VALUES ('0', '1', '2'),
        SUBPARTITION customer1_channel2 VALUES ('3', '4', '5'),
        SUBPARTITION customer1_channel3 VALUES ('6', '7', '8'),
        SUBPARTITION customer1_channel4 VALUES (DEFAULT)
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
CREATE INDEX range_list_sales_idx1 ON range_list_sales(customer_id) LOCAL;
CREATE INDEX range_list_sales_idx2 ON range_list_sales(channel_id) GLOBAL;

--create a temp table to exchange
CREATE TABLE range_list_temp
(
    product_id     INT4 NOT NULL,
    customer_id    INT4 NOT NULL,
    time_id        DATE,
    channel_id     CHAR(1),
    type_id        INT4,
    quantity_sold  NUMERIC(3),
    amount_sold    NUMERIC(10,2)
) WITH (STORAGE_TYPE=USTORE);
CREATE INDEX ON range_list_temp(customer_id);
INSERT INTO range_list_temp SELECT * FROM range_list_sales WHERE customer_id < 200 AND channel_id in ('0', '1', '2');

--drop
\parallel on
ALTER TABLE range_list_sales DROP SUBPARTITION customer1_channel1 UPDATE GLOBAL INDEX;
UPDATE range_list_sales SET type_id = -1 WHERE customer_id = 700;
INSERT INTO range_list_sales VALUES(1,1000,'2011-06-01', '1',-1,1,1);
SELECT /*+ tablescan(range_list_sales)*/ COUNT(*) FROM range_list_sales SUBPARTITION (customer1_channel4);
\parallel off
SELECT COUNT(*) FROM range_list_sales WHERE type_id = -1;

--split
\parallel on
ALTER TABLE range_list_sales SPLIT SUBPARTITION customer1_channel4 VALUES ('0', '1', '2')
    INTO (SUBPARTITION customer1_channel1, SUBPARTITION customer1_channel4_temp) UPDATE GLOBAL INDEX;
UPDATE range_list_sales PARTITION (customer3) SET type_id = 1 WHERE customer_id = 700;
DELETE FROM range_list_sales WHERE type_id = -1 AND customer_id >= 800;
\parallel off
SELECT COUNT(*) FROM range_list_sales WHERE type_id = -1;

--truncate
\parallel on
ALTER TABLE range_list_sales TRUNCATE PARTITION customer1 UPDATE GLOBAL INDEX;
SELECT /*+ tablescan(range_list_sales)*/ COUNT(*) FROM range_list_sales SUBPARTITION (customer1_channel2);
SELECT /*+ indexonlyscan(range_list_sales range_list_sales_idx1)*/ COUNT(customer_id) FROM range_list_sales SUBPARTITION (customer1_channel3);
SELECT /*+ indexonlyscan(range_list_sales range_list_sales_idx2)*/ COUNT(channel_id) FROM range_list_sales;
UPDATE range_list_sales SET type_id = -1 WHERE customer_id = 700;
INSERT INTO range_list_sales VALUES(1,1000,'2011-06-01', '1',-1,1,1);
\parallel off
SELECT COUNT(*) FROM range_list_sales WHERE type_id = -1;

--finish
DROP TABLE range_list_sales;
DROP TABLE range_list_temp;

-- clean
DROP SCHEMA partition_parallel CASCADE;
