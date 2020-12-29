--
-- MERGE INTO, test explain command
--

-- initial
CREATE SCHEMA mergeinto_explain;
SET current_schema = mergeinto_explain;

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
product_id INTEGER DEFAULT 0,
product_name VARCHAR(60) DEFAULT 'null',
category VARCHAR(60) DEFAULT 'unknown',
total INTEGER DEFAULT '0'
);

CREATE TABLE newproducts_row
(
product_id INTEGER DEFAULT 0,
product_name VARCHAR(60) DEFAULT 'null',
category VARCHAR(60) DEFAULT 'unknown',
total INTEGER DEFAULT '0'
);

INSERT INTO products_row SELECT * FROM products_base;
INSERT INTO newproducts_row SELECT * FROM newproducts_base;
ANALYZE products_row;
ANALYZE newproducts_row;

-- explain verbose
EXPLAIN (VERBOSE on, COSTS off)
MERGE INTO products_row p
USING newproducts_row np
ON p.product_id = np.product_id
WHEN MATCHED THEN
  UPDATE SET product_name = np.product_name, category = np.category, total = np.total
WHEN NOT MATCHED THEN  
  INSERT VALUES (np.product_id, np.product_name, np.category, np.total);

EXPLAIN (VERBOSE on, COSTS off)
MERGE INTO products_row p
USING newproducts_row np
ON p.product_id = np.product_id
WHEN MATCHED THEN
  UPDATE SET product_name = np.product_name, category = np.category, total = np.total
  WHERE p.total+np.total < 1000
WHEN NOT MATCHED THEN  
  INSERT VALUES (np.product_id, np.product_name, np.category, np.total)
  WHERE np.total < 1000;

EXPLAIN (VERBOSE on, COSTS off)
MERGE INTO products_row p
USING newproducts_row np
ON p.product_id = np.product_id
WHEN MATCHED THEN
  UPDATE SET product_name = np.product_name, category = np.category, total = np.total
  WHERE p.total < 1000
WHEN NOT MATCHED THEN  
  INSERT VALUES (np.product_id, np.product_name, np.category, np.total)
  WHERE np.product_id IS NOT NULL AND np.product_name IS NOT NULL;

-- explain analyze
BEGIN;
EXPLAIN (ANALYZE on, COSTS off, TIMING off)
MERGE INTO products_row p
USING newproducts_row np
ON p.product_id = np.product_id
WHEN MATCHED THEN
  UPDATE SET product_name = np.product_name, category = np.category, total = np.total
WHEN NOT MATCHED THEN  
  INSERT VALUES (np.product_id, np.product_name, np.category, np.total);
ROLLBACK;

-- explain performance
\o merge_explain.txt
BEGIN;
EXPLAIN PERFORMANCE
MERGE INTO products_row p
USING newproducts_row np
ON p.product_id = np.product_id
WHEN MATCHED THEN
  UPDATE SET product_name = np.product_name, category = np.category, total = np.total
WHEN NOT MATCHED THEN  
  INSERT VALUES (np.product_id, np.product_name, np.category, np.total);
ROLLBACK;
\o

-- explain analyze
BEGIN;
EXPLAIN (ANALYZE on, COSTS off, TIMING off)
MERGE INTO products_row p
USING newproducts_row np
ON p.product_id = np.product_id
WHEN MATCHED THEN
  UPDATE SET product_name = np.product_name, category = np.category, total = np.total
WHEN NOT MATCHED THEN  
  INSERT VALUES (np.product_id, np.product_name, np.category, np.total);
ROLLBACK;


drop table if exists item cascade;
create table item
(
    a int default 3,
	item_id number(18,10)  ,
    item_name varchar(100) ,
    item_level number(39,0),
    item_desc varchar(250) ,
    item_subclass_cd varchar(50) ,
    item_type_cd varchar(50) ,
    inventory_ind char(300) ,
    vendor_party_id smallint ,
    commodity_cd varchar(50),
    brand_cd varchar(50) ,
    item_available char(100)
) with(orientation=column)
partition by range (vendor_party_id)
(
partition item_1 values less than (0),
partition item_2 values less than (1),
partition item_3 values less than (2),
partition item_4 values less than (3),
partition item_5 values less than (6),
partition item_6 values less than (8),
partition item_7 values less than (10),
partition item_8 values less than (15),
partition item_9 values less than (maxvalue)
) ENABLE ROW MOVEMENT;

drop table if exists region cascade;
create table region
(
    a int default 8,
	region_cd varchar(50)  ,
    region_name varchar(100)  ,
    division_cd varchar(50)  ,
    REGION_MGR_ASSOCIATE_ID number(18,9)
);

drop table if exists associate_benefit_expense cascade;
create table associate_benefit_expense
(
    a int default 44,
	period_end_dt date  ,
    associate_expns_type_cd varchar(50)  ,
    associate_party_id integer  ,
    benefit_hours_qty decimal(38,11) ,
    benefit_cost_amt number(38,4)
)   
partition by range (associate_expns_type_cd )
(
partition associate_benefit_expense_1 values less than ('B'),
partition associate_benefit_expense_2 values less than ('E'),
partition associate_benefit_expense_3 values less than ('G'),
partition associate_benefit_expense_4 values less than ('I'),
partition associate_benefit_expense_5 values less than ('L'),
partition associate_benefit_expense_6 values less than ('N'),
partition associate_benefit_expense_7 values less than ('P'),
partition associate_benefit_expense_8 values less than ('Q'),
partition associate_benefit_expense_9 values less than ('R'),
partition associate_benefit_expense_10 values less than ('T'),
partition associate_benefit_expense_11 values less than ('U'),
partition associate_benefit_expense_12 values less than ('V'),
partition associate_benefit_expense_13 values less than (maxvalue)
)ENABLE ROW MOVEMENT;

INSERT INTO ITEM (ITEM_ID, ITEM_NAME, ITEM_DESC, ITEM_SUBCLASS_CD, ITEM_TYPE_CD, INVENTORY_IND, VENDOR_PARTY_ID, COMMODITY_CD, BRAND_CD,ITEM_AVAILABLE) VALUES (0.12, ' ' , 'A' , NULL, 'TGK' , 'A' , 2, 'A' , 'A' , 'Y');
INSERT INTO ITEM (ITEM_ID, ITEM_NAME, ITEM_DESC, ITEM_SUBCLASS_CD, ITEM_TYPE_CD, INVENTORY_IND, VENDOR_PARTY_ID, COMMODITY_CD, BRAND_CD,ITEM_AVAILABLE) VALUES (1.3, 'B' , NULL, 'B' , 'B' , NULL, 1, 'B' , NULL , 'N');
INSERT INTO ITEM (ITEM_ID, ITEM_NAME, ITEM_DESC, ITEM_SUBCLASS_CD, ITEM_TYPE_CD, INVENTORY_IND, VENDOR_PARTY_ID, COMMODITY_CD, BRAND_CD,ITEM_AVAILABLE) VALUES (2.23, 'C' , 'C' , NULL, 'C' , 'C' , 2, 'C' , 'C' , 'N');
INSERT INTO ITEM (ITEM_ID, ITEM_NAME, ITEM_DESC, ITEM_SUBCLASS_CD, ITEM_TYPE_CD, INVENTORY_IND, VENDOR_PARTY_ID, COMMODITY_CD, BRAND_CD,ITEM_AVAILABLE) VALUES (3.33, 'D' , 'D' , 'PT' , NULL, 'D' , 3, 'D' , 'D' , 'N');
INSERT INTO ITEM (ITEM_ID, ITEM_NAME, ITEM_DESC, ITEM_SUBCLASS_CD, ITEM_TYPE_CD, INVENTORY_IND, VENDOR_PARTY_ID, COMMODITY_CD, BRAND_CD,ITEM_AVAILABLE) VALUES (4.98, ' ' , NULL, 'E' , 'E' , 'E' , 4, 'E' , 'E' , 'Y');
INSERT INTO ITEM (ITEM_ID, ITEM_NAME, ITEM_DESC, ITEM_SUBCLASS_CD, ITEM_TYPE_CD, INVENTORY_IND, VENDOR_PARTY_ID, COMMODITY_CD, BRAND_CD,ITEM_AVAILABLE) VALUES (5.01, NULL, 'F' , ' ' , 'F' , 'F' , 5, 'F' , 'F' , 'N');
INSERT INTO ITEM (ITEM_ID, ITEM_NAME, ITEM_DESC, ITEM_SUBCLASS_CD, ITEM_TYPE_CD, INVENTORY_IND, VENDOR_PARTY_ID, COMMODITY_CD, BRAND_CD,ITEM_AVAILABLE) VALUES (6, 'G' , 'G' , 'G' , '_D' , 'G' , 6, 'G' , NULL ,'N');
INSERT INTO ITEM (ITEM_ID, ITEM_NAME, ITEM_DESC, ITEM_SUBCLASS_CD, ITEM_TYPE_CD, INVENTORY_IND, VENDOR_PARTY_ID, COMMODITY_CD, BRAND_CD,ITEM_AVAILABLE) VALUES (0.7, NULL, NULL, NULL, 'H' , 'H' , 7, NULL, 'G' , 'Y');
INSERT INTO ITEM (ITEM_ID, ITEM_NAME, ITEM_DESC, ITEM_SUBCLASS_CD, ITEM_TYPE_CD, INVENTORY_IND, VENDOR_PARTY_ID, COMMODITY_CD, BRAND_CD,ITEM_AVAILABLE) VALUES (0.08, 'I' , ' ' , ' T ' , NULL, 'I' , 8, 'I' , '' , 'N');
INSERT INTO ITEM (ITEM_ID, ITEM_NAME, ITEM_DESC, ITEM_SUBCLASS_CD, ITEM_TYPE_CD, INVENTORY_IND, VENDOR_PARTY_ID, COMMODITY_CD, BRAND_CD,ITEM_AVAILABLE) VALUES (9.12, ' ' , 'J' , ' PP' , 'J' , 'J' , 9, 'J' , NULL , 'Y');
INSERT INTO ITEM (ITEM_ID, ITEM_NAME, ITEM_DESC, ITEM_SUBCLASS_CD, ITEM_TYPE_CD, INVENTORY_IND, VENDOR_PARTY_ID, COMMODITY_CD, BRAND_CD,ITEM_AVAILABLE) VALUES (10.10, NULL, ' ' , 'A' , 'A' , 'A' , 2, NULL, 'A','Y');
INSERT INTO ITEM (ITEM_ID, ITEM_NAME, ITEM_DESC, ITEM_SUBCLASS_CD, ITEM_TYPE_CD, INVENTORY_IND, VENDOR_PARTY_ID, COMMODITY_CD, BRAND_CD,ITEM_AVAILABLE) VALUES (11.11, 'B' , 'B' , 'B' , 'BCDAA' , NULL, 1, 'B' , 'B','N');
INSERT INTO ITEM (ITEM_ID, ITEM_NAME, ITEM_DESC, ITEM_SUBCLASS_CD, ITEM_TYPE_CD, INVENTORY_IND, VENDOR_PARTY_ID, COMMODITY_CD, BRAND_CD,ITEM_AVAILABLE) VALUES (12.02, 'D' , NULL, NULL, 'C' , 'C' , 2, 'C' , 'C','N');
INSERT INTO ITEM (ITEM_ID, ITEM_NAME, ITEM_DESC, ITEM_SUBCLASS_CD, ITEM_TYPE_CD, INVENTORY_IND, VENDOR_PARTY_ID, COMMODITY_CD, BRAND_CD,ITEM_AVAILABLE) VALUES (13.99, NULL, ' ' , 'D' , 'D' , 'D' , 3, 'D' , 'D','Y');
INSERT INTO ITEM (ITEM_ID, ITEM_NAME, ITEM_DESC, ITEM_SUBCLASS_CD, ITEM_TYPE_CD, INVENTORY_IND, VENDOR_PARTY_ID, COMMODITY_CD, BRAND_CD,ITEM_AVAILABLE) VALUES (14, 'G' , 'E' , 'E' , NULL, 'E' , 4, 'E' , 'E','N');
INSERT INTO ITEM (ITEM_ID, ITEM_NAME, ITEM_DESC, ITEM_SUBCLASS_CD, ITEM_TYPE_CD, INVENTORY_IND, VENDOR_PARTY_ID, COMMODITY_CD, BRAND_CD,ITEM_AVAILABLE) VALUES (15, 'F' , ' ' , 'C' , 'CLEANING' , 'F' , 5, 'F' , 'F','Y');
INSERT INTO ITEM (ITEM_ID, ITEM_NAME, ITEM_DESC, ITEM_SUBCLASS_CD, ITEM_TYPE_CD, INVENTORY_IND, VENDOR_PARTY_ID, COMMODITY_CD, BRAND_CD,ITEM_AVAILABLE) VALUES (16, ''  , 'Z' , NULL, 'G' , 'G' , 6, 'G' , NULL,'N');
INSERT INTO ITEM (ITEM_ID, ITEM_NAME, ITEM_DESC, ITEM_SUBCLASS_CD, ITEM_TYPE_CD, INVENTORY_IND, VENDOR_PARTY_ID, COMMODITY_CD, BRAND_CD,ITEM_AVAILABLE) VALUES (17, NULL, ''  , '     PAPER' , 'H' , ''  , 7, NULL, NULL,'Y');
INSERT INTO ITEM (ITEM_ID, ITEM_NAME, ITEM_DESC, ITEM_SUBCLASS_CD, ITEM_TYPE_CD, INVENTORY_IND, VENDOR_PARTY_ID, COMMODITY_CD, BRAND_CD,ITEM_AVAILABLE) VALUES (19, ' ' , 'B' , ''  , ''  , 'I' , 8, 'I' , NULL,'N');
INSERT INTO ITEM (ITEM_ID, ITEM_NAME, ITEM_DESC, ITEM_SUBCLASS_CD, ITEM_TYPE_CD, INVENTORY_IND, VENDOR_PARTY_ID, COMMODITY_CD, BRAND_CD,ITEM_AVAILABLE) VALUES (20 , 'A' , 'J' , 'J' , 'J' , NULL, 9, 'J' , 'G','Y');
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

explain (verbose on, costs off)
MERGE INTO item Table_006 USING (
    SELECT Table_001.REGION_MGR_ASSOCIATE_ID Column_003,CAST(Table_001.region_name AS varchar) Column_004,
        Table_002.associate_expns_type_cd Column_005,'o' Column_006,'F' Column_007,10 Column_008,pg_client_encoding() Column_010
    FROM region Table_001,associate_benefit_expense Table_002) Table_005 ON ( Table_005.Column_004 = Table_006.item_subclass_cd ) WHEN NOT MATCHED 
	THEN INSERT ( Table_006.item_level, Table_006.item_desc) VALUES ( Table_005.Column_003, Table_005.Column_005 );

	MERGE INTO item Table_006 USING (
    SELECT Table_001.REGION_MGR_ASSOCIATE_ID Column_003,CAST(Table_001.region_name AS varchar) Column_004,
        Table_002.associate_expns_type_cd Column_005,'o' Column_006,'F' Column_007,10 Column_008,pg_client_encoding() Column_010
    FROM region Table_001,associate_benefit_expense Table_002) Table_005 ON ( Table_005.Column_004 = Table_006.item_subclass_cd ) WHEN NOT MATCHED 
	THEN INSERT ( Table_006.item_level, Table_006.item_desc) VALUES ( Table_005.Column_003, Table_005.Column_005 );
-- clean up
DROP SCHEMA mergeinto_explain CASCADE;
