--
-- INSERT UPDATE, test explain command, comes from merge_explain and merge_explain_pretty
--

-- initial
CREATE SCHEMA test_insert_update_008;
SET current_schema = test_insert_update_008;

-- SET enable_upsert_to_merge=ON to test the upsert implemented by merge,
-- real upsert will be tested in specialized case.
SET enable_upsert_to_merge TO ON;

CREATE TABLE products_base
(
product_id INTEGER DEFAULT 0,
product_name VARCHAR(60) DEFAULT 'null',
category VARCHAR(60) DEFAULT 'unknown',
total INTEGER DEFAULT '0'
);

INSERT INTO products_base VALUES (1501, 'vivitar 35mm', 'electrncs', 100);
INSERT INTO products_base VALUES (1502, 'olympus is50', 'electrncs', 100);
INSERT INTO products_base VALUES (1600, 'play gym', 'toys', 100);
INSERT INTO products_base VALUES (1601, 'lamaze', 'toys', 100);
INSERT INTO products_base VALUES (1666, 'harry potter', 'dvd', 100);

CREATE TABLE newproducts_base
(
product_id INTEGER DEFAULT 0,
product_name VARCHAR(60) DEFAULT 'null',
category VARCHAR(60) DEFAULT 'unknown',
total INTEGER DEFAULT '0'
);

INSERT INTO newproducts_base VALUES (1502, 'olympus camera', 'electrncs', 200);
INSERT INTO newproducts_base VALUES (1601, 'lamaze', 'toys', 200);
INSERT INTO newproducts_base VALUES (1666, 'harry potter', 'toys', 200);
INSERT INTO newproducts_base VALUES (1700, 'wait interface', 'books', 200);

ANALYZE products_base;
ANALYZE newproducts_base;

--
-- row table
--
CREATE TABLE products_row
(
product_id INTEGER DEFAULT 0 PRIMARY KEY,
product_name VARCHAR(60) DEFAULT 'null',
category VARCHAR(60) DEFAULT 'unknown',
total INTEGER DEFAULT '0'
);

CREATE TABLE newproducts_row
(
product_id INTEGER DEFAULT 0 PRIMARY KEY,
product_name VARCHAR(60) DEFAULT 'null',
category VARCHAR(60) DEFAULT 'unknown',
total INTEGER DEFAULT '0'
);

INSERT INTO products_row SELECT * FROM products_base;
INSERT INTO newproducts_row SELECT * FROM newproducts_base;
ANALYZE products_row;
ANALYZE newproducts_row;

SET explain_perf_mode = normal;
-- explain verbose
EXPLAIN (VERBOSE on, COSTS off)
INSERT INTO products_row
    SELECT product_id, product_name, category, total FROM newproducts_row
    ON DUPLICATE KEY UPDATE
    product_name = excluded.product_name,
    category = excluded.category,
    total = excluded.total;

EXPLAIN (VERBOSE on, COSTS off)
INSERT INTO products_row
    SELECT newproducts_row.product_id,
        newproducts_row.product_name,
        newproducts_row.category,
        newproducts_row.total
        FROM newproducts_row, products_row
        WHERE products_row.total + newproducts_row.total < 1000
    ON DUPLICATE KEY UPDATE
        product_name = excluded.product_name,
        category = excluded.category,
        total = excluded.total;

EXPLAIN (VERBOSE on, COSTS off)
INSERT INTO products_row
    SELECT product_id, product_name, category, total
        FROM newproducts_row WHERE product_id IS NOT NULL AND product_name IS NOT NULL
    ON DUPLICATE KEY UPDATE
        product_name = excluded.product_name,
        category = excluded.category,
        total = excluded.total;

-- explain analyze
BEGIN;
EXPLAIN (ANALYZE on, COSTS off, TIMING off)
INSERT INTO products_row
    SELECT product_id, product_name, category, total FROM newproducts_row
    ON DUPLICATE KEY UPDATE
    product_name = excluded.product_name,
    category = excluded.category,
    total = excluded.total;
ROLLBACK;

-- explain performance
\o insert_update_explain.txt
BEGIN;
EXPLAIN PERFORMANCE
INSERT INTO products_row
    SELECT product_id, product_name, category, total FROM newproducts_row
    ON DUPLICATE KEY UPDATE
    product_name = excluded.product_name,
    category = excluded.category,
    total = excluded.total;
ROLLBACK;
\o

-- explain analyze
BEGIN;
EXPLAIN (ANALYZE on, COSTS off, TIMING off)
INSERT INTO products_row
    SELECT product_id, product_name, category, total FROM newproducts_row
    ON DUPLICATE KEY UPDATE
    product_name = excluded.product_name,
    category = excluded.category,
    total = excluded.total;
ROLLBACK;

-- pretty mode performance
SET explain_perf_mode = pretty;

-- explain verbose
EXPLAIN (VERBOSE on, COSTS off)
INSERT INTO products_row
    SELECT product_id, product_name, category, total FROM newproducts_row
    ON DUPLICATE KEY UPDATE
    product_name = excluded.product_name,
    category = excluded.category,
    total = excluded.total;

-- explain analyze
BEGIN;
EXPLAIN (ANALYZE on, COSTS off, TIMING off)
INSERT INTO products_row
    SELECT product_id, product_name, category, total FROM newproducts_row
    ON DUPLICATE KEY UPDATE
    product_name = excluded.product_name,
    category = excluded.category,
    total = excluded.total;
ROLLBACK;

-- explain analyze
BEGIN;
EXPLAIN (ANALYZE on, COSTS off, TIMING off)
INSERT INTO products_row
    SELECT product_id, product_name, category, total FROM newproducts_row
    ON DUPLICATE KEY UPDATE
    product_name = excluded.product_name,
    category = excluded.category,
    total = excluded.total;
ROLLBACK;

-- explain performance
\o insert_update_explain_pretty.txt
BEGIN;
EXPLAIN PERFORMANCE
INSERT INTO products_row
    SELECT product_id, product_name, category, total FROM newproducts_row
    ON DUPLICATE KEY UPDATE
    product_name = excluded.product_name,
    category = excluded.category,
    total = excluded.total;
ROLLBACK;

SET explain_perf_mode = run;

BEGIN;
EXPLAIN PERFORMANCE
INSERT INTO products_row
    SELECT product_id, product_name, category, total FROM newproducts_row
    ON DUPLICATE KEY UPDATE
    product_name = excluded.product_name,
    category = excluded.category,
    total = excluded.total;
ROLLBACK;

SET explain_perf_mode = summary;

BEGIN;
EXPLAIN PERFORMANCE
INSERT INTO products_row
    SELECT product_id, product_name, category, total FROM newproducts_row
    ON DUPLICATE KEY UPDATE
    product_name = excluded.product_name,
    category = excluded.category,
    total = excluded.total;
ROLLBACK;
\o

CREATE TABLE item
(
    a INT DEFAULT 3,
    item_id NUMERIC(18,10),
    item_name VARCHAR(100),
    item_level NUMERIC(39,0),
    item_desc VARCHAR(250),
    item_subclass_cd VARCHAR(50),
    item_type_cd VARCHAR(50),
    inventory_ind CHAR(300),
    vendor_party_id SMALLINT,
    commodity_cd VARCHAR(50),
    brand_cd VARCHAR(50),
    item_available CHAR(100),
    CONSTRAINT u_item_index UNIQUE (item_subclass_cd, vendor_party_id)
)  
PARTITION BY RANGE (vendor_party_id)
(
PARTITION item_1 VALUES LESS THAN (0),
PARTITION item_2 VALUES LESS THAN (1),
PARTITION item_3 VALUES LESS THAN (2),
PARTITION item_4 VALUES LESS THAN (3),
PARTITION item_5 VALUES LESS THAN (6),
PARTITION item_6 VALUES LESS THAN (8),
PARTITION item_7 VALUES LESS THAN (10),
PARTITION item_8 VALUES LESS THAN (15),
PARTITION item_9 VALUES LESS THAN (MAXVALUE)
) ENABLE ROW MOVEMENT;

CREATE TABLE region
(
    a INT DEFAULT 8,
    region_cd VARCHAR(50),
    region_name VARCHAR(100),
    division_cd VARCHAR(50),
    region_mgr_associate_id number(18,9)
);

CREATE TABLE associate_benefit_expense
(
    a INT DEFAULT 44,
    period_end_dt DATE,
    associate_expns_type_cd VARCHAR(50),
    associate_party_id INTEGER,
    benefit_hours_qty decimal(38,11),
    benefit_cost_amt number(38,4)
)
PARTITION BY RANGE (associate_expns_type_cd)
(
PARTITION associate_benefit_expense_1 VALUES LESS THAN ('B'),
PARTITION associate_benefit_expense_2 VALUES LESS THAN ('E'),
PARTITION associate_benefit_expense_3 VALUES LESS THAN ('G'),
PARTITION associate_benefit_expense_4 VALUES LESS THAN ('I'),
PARTITION associate_benefit_expense_5 VALUES LESS THAN ('L'),
PARTITION associate_benefit_expense_6 VALUES LESS THAN ('N'),
PARTITION associate_benefit_expense_7 VALUES LESS THAN ('P'),
PARTITION associate_benefit_expense_8 VALUES LESS THAN ('Q'),
PARTITION associate_benefit_expense_9 VALUES LESS THAN ('R'),
PARTITION associate_benefit_expense_10 VALUES LESS THAN ('T'),
PARTITION associate_benefit_expense_11 VALUES LESS THAN ('U'),
PARTITION associate_benefit_expense_12 VALUES LESS THAN ('V'),
PARTITION associate_benefit_expense_13 VALUES LESS THAN (MAXVALUE)
) ENABLE ROW MOVEMENT;

INSERT INTO item (ITEM_ID, ITEM_NAME, ITEM_DESC, ITEM_SUBCLASS_CD, ITEM_TYPE_CD, INVENTORY_IND, VENDOR_PARTY_ID, COMMODITY_CD, BRAND_CD,ITEM_AVAILABLE) VALUES (0.12, ' ' , 'A' , NULL, 'TGK' , 'A' , 2, 'A' , 'A' , 'Y');
INSERT INTO item (ITEM_ID, ITEM_NAME, ITEM_DESC, ITEM_SUBCLASS_CD, ITEM_TYPE_CD, INVENTORY_IND, VENDOR_PARTY_ID, COMMODITY_CD, BRAND_CD,ITEM_AVAILABLE) VALUES (1.3, 'B' , NULL, 'B' , 'B' , NULL, 1, 'B' , NULL , 'N');
INSERT INTO item (ITEM_ID, ITEM_NAME, ITEM_DESC, ITEM_SUBCLASS_CD, ITEM_TYPE_CD, INVENTORY_IND, VENDOR_PARTY_ID, COMMODITY_CD, BRAND_CD,ITEM_AVAILABLE) VALUES (2.23, 'C' , 'C' , NULL, 'C' , 'C' , 2, 'C' , 'C' , 'N');
INSERT INTO item (ITEM_ID, ITEM_NAME, ITEM_DESC, ITEM_SUBCLASS_CD, ITEM_TYPE_CD, INVENTORY_IND, VENDOR_PARTY_ID, COMMODITY_CD, BRAND_CD,ITEM_AVAILABLE) VALUES (3.33, 'D' , 'D' , 'PT' , NULL, 'D' , 3, 'D' , 'D' , 'N');
INSERT INTO item (ITEM_ID, ITEM_NAME, ITEM_DESC, ITEM_SUBCLASS_CD, ITEM_TYPE_CD, INVENTORY_IND, VENDOR_PARTY_ID, COMMODITY_CD, BRAND_CD,ITEM_AVAILABLE) VALUES (4.98, ' ' , NULL, 'E' , 'E' , 'E' , 4, 'E' , 'E' , 'Y');
INSERT INTO item (ITEM_ID, ITEM_NAME, ITEM_DESC, ITEM_SUBCLASS_CD, ITEM_TYPE_CD, INVENTORY_IND, VENDOR_PARTY_ID, COMMODITY_CD, BRAND_CD,ITEM_AVAILABLE) VALUES (5.01, NULL, 'F' , ' ' , 'F' , 'F' , 5, 'F' , 'F' , 'N');
INSERT INTO item (ITEM_ID, ITEM_NAME, ITEM_DESC, ITEM_SUBCLASS_CD, ITEM_TYPE_CD, INVENTORY_IND, VENDOR_PARTY_ID, COMMODITY_CD, BRAND_CD,ITEM_AVAILABLE) VALUES (6, 'G' , 'G' , 'G' , '_D' , 'G' , 6, 'G' , NULL ,'N');
INSERT INTO item (ITEM_ID, ITEM_NAME, ITEM_DESC, ITEM_SUBCLASS_CD, ITEM_TYPE_CD, INVENTORY_IND, VENDOR_PARTY_ID, COMMODITY_CD, BRAND_CD,ITEM_AVAILABLE) VALUES (0.7, NULL, NULL, NULL, 'H' , 'H' , 7, NULL, 'G' , 'Y');
INSERT INTO item (ITEM_ID, ITEM_NAME, ITEM_DESC, ITEM_SUBCLASS_CD, ITEM_TYPE_CD, INVENTORY_IND, VENDOR_PARTY_ID, COMMODITY_CD, BRAND_CD,ITEM_AVAILABLE) VALUES (0.08, 'I' , ' ' , ' T ' , NULL, 'I' , 8, 'I' , '' , 'N');
INSERT INTO item (ITEM_ID, ITEM_NAME, ITEM_DESC, ITEM_SUBCLASS_CD, ITEM_TYPE_CD, INVENTORY_IND, VENDOR_PARTY_ID, COMMODITY_CD, BRAND_CD,ITEM_AVAILABLE) VALUES (9.12, ' ' , 'J' , ' PP' , 'J' , 'J' , 9, 'J' , NULL , 'Y');
INSERT INTO item (ITEM_ID, ITEM_NAME, ITEM_DESC, ITEM_SUBCLASS_CD, ITEM_TYPE_CD, INVENTORY_IND, VENDOR_PARTY_ID, COMMODITY_CD, BRAND_CD,ITEM_AVAILABLE) VALUES (10.10, NULL, ' ' , 'A' , 'A' , 'A' , 2, NULL, 'A','Y');
--INSERT INTO item (ITEM_ID, ITEM_NAME, ITEM_DESC, ITEM_SUBCLASS_CD, ITEM_TYPE_CD, INVENTORY_IND, VENDOR_PARTY_ID, COMMODITY_CD, BRAND_CD,ITEM_AVAILABLE) VALUES (11.11, 'B' , 'B' , 'B' , 'BCDAA' , NULL, 1, 'B' , 'B','N');
INSERT INTO item (ITEM_ID, ITEM_NAME, ITEM_DESC, ITEM_SUBCLASS_CD, ITEM_TYPE_CD, INVENTORY_IND, VENDOR_PARTY_ID, COMMODITY_CD, BRAND_CD,ITEM_AVAILABLE) VALUES (12.02, 'D' , NULL, NULL, 'C' , 'C' , 2, 'C' , 'C','N');
INSERT INTO item (ITEM_ID, ITEM_NAME, ITEM_DESC, ITEM_SUBCLASS_CD, ITEM_TYPE_CD, INVENTORY_IND, VENDOR_PARTY_ID, COMMODITY_CD, BRAND_CD,ITEM_AVAILABLE) VALUES (13.99, NULL, ' ' , 'D' , 'D' , 'D' , 3, 'D' , 'D','Y');
--INSERT INTO item (ITEM_ID, ITEM_NAME, ITEM_DESC, ITEM_SUBCLASS_CD, ITEM_TYPE_CD, INVENTORY_IND, VENDOR_PARTY_ID, COMMODITY_CD, BRAND_CD,ITEM_AVAILABLE) VALUES (14, 'G' , 'E' , 'E' , NULL, 'E' , 4, 'E' , 'E','N');
INSERT INTO item (ITEM_ID, ITEM_NAME, ITEM_DESC, ITEM_SUBCLASS_CD, ITEM_TYPE_CD, INVENTORY_IND, VENDOR_PARTY_ID, COMMODITY_CD, BRAND_CD,ITEM_AVAILABLE) VALUES (15, 'F' , ' ' , 'C' , 'CLEANING' , 'F' , 5, 'F' , 'F','Y');
INSERT INTO item (ITEM_ID, ITEM_NAME, ITEM_DESC, ITEM_SUBCLASS_CD, ITEM_TYPE_CD, INVENTORY_IND, VENDOR_PARTY_ID, COMMODITY_CD, BRAND_CD,ITEM_AVAILABLE) VALUES (16, ''  , 'Z' , NULL, 'G' , 'G' , 6, 'G' , NULL,'N');
INSERT INTO item (ITEM_ID, ITEM_NAME, ITEM_DESC, ITEM_SUBCLASS_CD, ITEM_TYPE_CD, INVENTORY_IND, VENDOR_PARTY_ID, COMMODITY_CD, BRAND_CD,ITEM_AVAILABLE) VALUES (17, NULL, ''  , '     PAPER' , 'H' , ''  , 7, NULL, NULL,'Y');
INSERT INTO item (ITEM_ID, ITEM_NAME, ITEM_DESC, ITEM_SUBCLASS_CD, ITEM_TYPE_CD, INVENTORY_IND, VENDOR_PARTY_ID, COMMODITY_CD, BRAND_CD,ITEM_AVAILABLE) VALUES (19, ' ' , 'B' , ''  , ''  , 'I' , 8, 'I' , NULL,'N');
INSERT INTO item (ITEM_ID, ITEM_NAME, ITEM_DESC, ITEM_SUBCLASS_CD, ITEM_TYPE_CD, INVENTORY_IND, VENDOR_PARTY_ID, COMMODITY_CD, BRAND_CD,ITEM_AVAILABLE) VALUES (20 , 'A' , 'J' , 'J' , 'J' , NULL, 9, 'J' , 'G','Y');

/*--REGION--*/
INSERT INTO REGION (REGION_CD, REGION_NAME, DIVISION_CD, REGION_MGR_ASSOCIATE_ID) VALUES ('A', 'A ', 'A', 0.123433);
INSERT INTO REGION (REGION_CD, REGION_NAME, DIVISION_CD, REGION_MGR_ASSOCIATE_ID) VALUES ('B', 'B', 'B', NULL);
INSERT INTO REGION (REGION_CD, REGION_NAME, DIVISION_CD, REGION_MGR_ASSOCIATE_ID) VALUES ('C', 'C', 'C', 2.232008908);
INSERT INTO REGION (REGION_CD, REGION_NAME, DIVISION_CD, REGION_MGR_ASSOCIATE_ID) VALUES ('D', '   DD', 'D', 3.878789);
INSERT INTO REGION (REGION_CD, REGION_NAME, DIVISION_CD, REGION_MGR_ASSOCIATE_ID) VALUES ('E', 'A', 'E', 4.89060603);
INSERT INTO REGION (REGION_CD, REGION_NAME, DIVISION_CD, REGION_MGR_ASSOCIATE_ID) VALUES ('F', 'F', 'F', 5.82703827);
INSERT INTO REGION (REGION_CD, REGION_NAME, DIVISION_CD, REGION_MGR_ASSOCIATE_ID) VALUES ('G', 'G', 'TTT', NULL);
INSERT INTO REGION (REGION_CD, REGION_NAME, DIVISION_CD, REGION_MGR_ASSOCIATE_ID) VALUES ('H', 'H', 'G', 7.3829083);
INSERT INTO REGION (REGION_CD, REGION_NAME, DIVISION_CD, REGION_MGR_ASSOCIATE_ID) VALUES ('I', 'C', 'M', 8.983989);
INSERT INTO REGION (REGION_CD, REGION_NAME, DIVISION_CD, REGION_MGR_ASSOCIATE_ID) VALUES ('J', 'J', 'G', NULL);
INSERT INTO REGION (REGION_CD, REGION_NAME, DIVISION_CD, REGION_MGR_ASSOCIATE_ID) VALUES ('K', ' ', 'C', 2.232008908);
INSERT INTO REGION (REGION_CD, REGION_NAME, DIVISION_CD, REGION_MGR_ASSOCIATE_ID) VALUES ('L', 'D', 'X', 3.878789);
INSERT INTO REGION (REGION_CD, REGION_NAME, DIVISION_CD, REGION_MGR_ASSOCIATE_ID) VALUES ('M', 'TTTTTT ', 'D' , 4.89060603);
INSERT INTO REGION (REGION_CD, REGION_NAME, DIVISION_CD, REGION_MGR_ASSOCIATE_ID) VALUES ('N', 'G' , 'B' , NULL);
INSERT INTO REGION (REGION_CD, REGION_NAME, DIVISION_CD, REGION_MGR_ASSOCIATE_ID) VALUES ('O' , 'G', 'F', 6.6703972);

INSERT INTO ASSOCIATE_BENEFIT_EXPENSE (PERIOD_END_DT, ASSOCIATE_EXPNS_TYPE_CD, ASSOCIATE_PARTY_ID, BENEFIT_HOURS_QTY, BENEFIT_COST_AMT) VALUES (DATE '1970-01-01', 'A',  5, 0.5 , 0.5);
INSERT INTO ASSOCIATE_BENEFIT_EXPENSE (PERIOD_END_DT, ASSOCIATE_EXPNS_TYPE_CD, ASSOCIATE_PARTY_ID, BENEFIT_HOURS_QTY, BENEFIT_COST_AMT) VALUES (DATE '1973-01-01', 'B',  1, NULL, 1.0);
INSERT INTO ASSOCIATE_BENEFIT_EXPENSE (PERIOD_END_DT, ASSOCIATE_EXPNS_TYPE_CD, ASSOCIATE_PARTY_ID, BENEFIT_HOURS_QTY, BENEFIT_COST_AMT) VALUES (DATE '1976-01-01', 'C',  2, 2.0 , NULL);
INSERT INTO ASSOCIATE_BENEFIT_EXPENSE (PERIOD_END_DT, ASSOCIATE_EXPNS_TYPE_CD, ASSOCIATE_PARTY_ID, BENEFIT_HOURS_QTY, BENEFIT_COST_AMT) VALUES (DATE '1979-01-01', 'D',  3, 3.0 , 3.0);
INSERT INTO ASSOCIATE_BENEFIT_EXPENSE (PERIOD_END_DT, ASSOCIATE_EXPNS_TYPE_CD, ASSOCIATE_PARTY_ID, BENEFIT_HOURS_QTY, BENEFIT_COST_AMT) VALUES (DATE '1982-01-01', 'E',  4, 4.0 , 4.0);
INSERT INTO ASSOCIATE_BENEFIT_EXPENSE (PERIOD_END_DT, ASSOCIATE_EXPNS_TYPE_CD, ASSOCIATE_PARTY_ID, BENEFIT_HOURS_QTY, BENEFIT_COST_AMT) VALUES (DATE '1985-01-01', 'F',  5, 5.0 , 5.0);
INSERT INTO ASSOCIATE_BENEFIT_EXPENSE (PERIOD_END_DT, ASSOCIATE_EXPNS_TYPE_CD, ASSOCIATE_PARTY_ID, BENEFIT_HOURS_QTY, BENEFIT_COST_AMT) VALUES (DATE '1988-01-01', 'F',  6, NULL, 6.0);
INSERT INTO ASSOCIATE_BENEFIT_EXPENSE (PERIOD_END_DT, ASSOCIATE_EXPNS_TYPE_CD, ASSOCIATE_PARTY_ID, BENEFIT_HOURS_QTY, BENEFIT_COST_AMT) VALUES (DATE '1991-01-01', 'G',  6, NULL, NULL);
INSERT INTO ASSOCIATE_BENEFIT_EXPENSE (PERIOD_END_DT, ASSOCIATE_EXPNS_TYPE_CD, ASSOCIATE_PARTY_ID, BENEFIT_HOURS_QTY, BENEFIT_COST_AMT) VALUES (DATE '1994-01-01', 'G', 15, 8.0 , 8.0);
INSERT INTO ASSOCIATE_BENEFIT_EXPENSE (PERIOD_END_DT, ASSOCIATE_EXPNS_TYPE_CD, ASSOCIATE_PARTY_ID, BENEFIT_HOURS_QTY, BENEFIT_COST_AMT) VALUES (DATE '1997-01-01', 'G', 16, 9.0 , 9.0);
INSERT INTO ASSOCIATE_BENEFIT_EXPENSE (PERIOD_END_DT, ASSOCIATE_EXPNS_TYPE_CD, ASSOCIATE_PARTY_ID, BENEFIT_HOURS_QTY, BENEFIT_COST_AMT) VALUES (DATE '1983-01-03', 'I', 14, 4.0 , 4.0);
INSERT INTO ASSOCIATE_BENEFIT_EXPENSE (PERIOD_END_DT, ASSOCIATE_EXPNS_TYPE_CD, ASSOCIATE_PARTY_ID, BENEFIT_HOURS_QTY, BENEFIT_COST_AMT) VALUES (DATE '1984-01-01', 'GO', 15, 5.0 , NULL);
INSERT INTO ASSOCIATE_BENEFIT_EXPENSE (PERIOD_END_DT, ASSOCIATE_EXPNS_TYPE_CD, ASSOCIATE_PARTY_ID, BENEFIT_HOURS_QTY, BENEFIT_COST_AMT) VALUES (DATE '1985-05-01', 'I', 16, 6.0 , 6.0);
INSERT INTO ASSOCIATE_BENEFIT_EXPENSE (PERIOD_END_DT, ASSOCIATE_EXPNS_TYPE_CD, ASSOCIATE_PARTY_ID, BENEFIT_HOURS_QTY, BENEFIT_COST_AMT) VALUES (DATE '1990-01-01', 'TTT', 16, NULL, 7.0);
INSERT INTO ASSOCIATE_BENEFIT_EXPENSE (PERIOD_END_DT, ASSOCIATE_EXPNS_TYPE_CD, ASSOCIATE_PARTY_ID, BENEFIT_HOURS_QTY, BENEFIT_COST_AMT) VALUES (DATE '1992-02-01', 'A', 15, 8.0 , 8.0);
INSERT INTO ASSOCIATE_BENEFIT_EXPENSE (PERIOD_END_DT, ASSOCIATE_EXPNS_TYPE_CD, ASSOCIATE_PARTY_ID, BENEFIT_HOURS_QTY, BENEFIT_COST_AMT) VALUES (DATE '1997-02-01', 'G', 17, 9.0 , NULL);
INSERT INTO ASSOCIATE_BENEFIT_EXPENSE (PERIOD_END_DT, ASSOCIATE_EXPNS_TYPE_CD, ASSOCIATE_PARTY_ID, BENEFIT_HOURS_QTY, BENEFIT_COST_AMT) VALUES (DATE '1997-05-01', 'G' , 17, 9.0 , NULL);

ANALYZE item;
ANALYZE region;
ANALYZE associate_benefit_expense;

EXPLAIN (VERBOSE ON, COSTS OFF)
INSERT INTO item (item_level, item_subclass_cd, item_desc, vendor_party_id)
    SELECT Table_001.REGION_MGR_ASSOCIATE_ID Column_003,
        Table_002.associate_expns_type_cd Column_004,
        CAST(Table_001.region_name AS VARCHAR) Column_005,
        10 Column_006
--        'o' Column_007,
--        'F' Column_008,
--        pg_client_encoding() Column_009
    FROM region Table_001, associate_benefit_expense Table_002
ON DUPLICATE KEY UPDATE item_level = -1000;

EXPLAIN (VERBOSE ON, COSTS OFF)
INSERT INTO products_row VALUES(100)
    ON DUPLICATE KEY UPDATE total=100;

SET enable_light_proxy=off;
EXPLAIN (VERBOSE ON, COSTS OFF)
INSERT INTO products_row VALUES(100)
    ON DUPLICATE KEY UPDATE total=100;

EXPLAIN (VERBOSE ON, COSTS OFF)
INSERT INTO products_row VALUES(100)
    ON DUPLICATE KEY UPDATE total=100;

EXPLAIN (VERBOSE ON, COSTS OFF)
INSERT INTO products_row VALUES(100)
    ON DUPLICATE KEY UPDATE total=100;

RESET enable_light_proxy;

DROP SCHEMA test_insert_update_008 CASCADE;
