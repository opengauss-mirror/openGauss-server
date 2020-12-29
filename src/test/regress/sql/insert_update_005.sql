-- initial, rewrite from merge_2.sql
DROP SCHEMA test_insert_update_005 CASCADE;
CREATE SCHEMA test_insert_update_005;
SET current_schema = test_insert_update_005;

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
INSERT INTO newproducts_base VALUES (1601, 'lamaze2', 'toys2', 200);
INSERT INTO newproducts_base VALUES (1666, 'harry potter2', 'toys', 200);
INSERT INTO newproducts_base VALUES (1700, 'wait interface', 'books', 200);


CREATE TABLE products_row
(
    product_id INTEGER DEFAULT 0,
    product_name VARCHAR(60) DEFAULT 'null',
    category VARCHAR(60) DEFAULT 'unknown',
    total INTEGER DEFAULT '0',
    PRIMARY KEY (product_id)
);

CREATE TABLE newproducts_row
(
    product_id INTEGER DEFAULT 0,
    product_name VARCHAR(60) DEFAULT 'null',
    category VARCHAR(60) DEFAULT 'unknown',
    total INTEGER DEFAULT '0',
    PRIMARY KEY (product_id)
);

INSERT INTO products_row SELECT * FROM products_base;
INSERT INTO newproducts_row SELECT * FROM newproducts_base;

--example 1
CREATE VIEW v AS (SELECT * FROM newproducts_row WHERE total > 100);

INSERT INTO products_row (product_id,product_name, category, total)
SELECT product_id, product_name, category || 'DEF', total + 200 FROM V
ON DUPLICATE KEY UPDATE products_row.product_name = products_row.product_name,
products_row.category = products_row.category || 'ABC', products_row.total = products_row.total + 100;

SELECT * FROM products_row ORDER BY 1;
DROP VIEW v;

--example 2
TRUNCATE products_row;
INSERT INTO products_row SELECT * FROM products_base;

INSERT INTO products_row (product_id, product_name, category, total)
SELECT product_id, product_name, category || 'DEF', total + 200 FROM products_row
ON DUPLICATE KEY UPDATE products_row.product_name = products_row.product_name,
products_row.category = products_row.category || 'ABC' ,products_row.total = products_row.total + 100;

SELECT * FROM products_row ORDER BY 1;

--example 3
TRUNCATE products_row;
INSERT INTO products_row SELECT * FROM products_base;

INSERT INTO products_row (product_id,product_name, category, total)
SELECT product_id, product_name, category, total FROM newproducts_row
ON DUPLICATE KEY UPDATE products_row.product_name = DEFAULT,
products_row.category = DEFAULT, products_row.total = DEFAULT;

SELECT * FROM products_row ORDER BY 1;

--example 4
TRUNCATE products_row;
INSERT INTO products_row SELECT * FROM products_base;

BEGIN;
DO LANGUAGE plpgsql $$
BEGIN
INSERT INTO products_row (product_id, product_name, category, total)
SELECT product_id, product_name, category, total FROM newproducts_row
ON DUPLICATE KEY UPDATE product_name = __unnamed_subquery_source__.product_name,
category = __unnamed_subquery_source__.category, total = __unnamed_subquery_source__.total;
END;
$$;

SELECT * FROM products_row order by 1;
ROLLBACK;

--example 5
TRUNCATE products_row;
INSERT INTO products_row SELECT * FROM products_base;

BEGIN;
BEGIN
INSERT INTO products_row (product_id,product_name, category, total)
SELECT product_id, product_name, category, total FROM newproducts_row
ON DUPLICATE KEY UPDATE product_name = __unnamed_subquery_source__.product_name,
category = __unnamed_subquery_source__.category, total = __unnamed_subquery_source__.total;
END;
/
SELECT * FROM products_row order by 1;
ROLLBACK;

--example 6
TRUNCATE products_row;
INSERT INTO products_row SELECT * FROM products_base;

CREATE OR REPLACE PROCEDURE p1()
AS
BEGIN
INSERT INTO products_row (product_id,product_name, category, total)
SELECT product_id, product_name, category, total FROM newproducts_row
ON DUPLICATE KEY UPDATE product_name = __unnamed_subquery_source__.product_name,
category = __unnamed_subquery_source__.category, total =__unnamed_subquery_source__.total;
END;
/

BEGIN;
SELECT p1();
SELECT * FROM products_row order by 1;
ROLLBACK;

CREATE OR REPLACE PROCEDURE p2 (param1 IN text)
AS
BEGIN
INSERT INTO products_row (product_name, total) VALUES (param1, 20) 
ON DUPLICATE KEY UPDATE category = param1;
END;
/

BEGIN;
SELECT p2('param1');
SELECT * FROM products_row order by 1;
SELECT p2('param1');
SELECT * FROM products_row order by 1;
ROLLBACK;

--example 7
CREATE TABLE des(a BIGINT, b BIGINT, c VARCHAR(1000), PRIMARY KEY(a));
CREATE TABLE src(a BIGINT, b BIGINT, c VARCHAR(1000), PRIMARY KEY(a));
CREATE INDEX src_i ON src(b);
CREATE INDEX des_i ON des(b);

INSERT INTO des VALUES(generate_series(1, 10), generate_series(1, 10), 'des');
INSERT INTO src VALUES(generate_series(1, 10), generate_series(1, 10), 'src');

INSERT INTO src (SELECT a + 100, b + 100, c FROM src);
INSERT INTO src (SELECT a + 1000, b + 1000, c FROM src);
INSERT INTO src (SELECT a + 10000, b + 10000, c FROM src);
INSERT INTO src (SELECT a + 100000, b + 100000, c FROM src);
INSERT INTO src (SELECT a + 1000000, b + 1000000, c FROM src);
INSERT INTO src (SELECT a + 10000000, b + 10000000, c FROM src);
INSERT INTO src (SELECT a + 100000000, b + 100000000, c FROM src);
INSERT INTO src (SELECT a + 1000000000, b + 100000000, c FROM src);
INSERT INTO src (SELECT a + 10000000000, b + 100000000, c FROM src);

INSERT INTO des (SELECT a + 100, b + 100, c FROM des);
INSERT INTO des (SELECT a + 1000, b + 1000, c FROM des);
INSERT INTO des (SELECT a + 10000, b + 10000, c FROM des);
INSERT INTO des (SELECT a + 100000, b + 100000, c FROM des);
INSERT INTO des (SELECT a + 1000000, b + 1000000, c FROM des);
INSERT INTO des (SELECT a + 10000000, b + 10000000, c FROM des);
INSERT INTO des (SELECT a + 100000000, b + 100000000, c FROM des);
INSERT INTO des (SELECT a + 1000000000, b + 1000000000, c FROM des);
INSERT INTO des (SELECT a + 10000000000, b + 1000000000, c FROM des);

INSERT INTO des (a, b, c)
SELECT a, b, c FROM src
ON DUPLICATE KEY UPDATE b = __unnamed_subquery_source__.b + 10, c = __unnamed_subquery_source__.c;

--rows shall be the same
SELECT COUNT(*) FROM src;
SELECT COUNT(*) FROM des;

--column b of des is 10 bigger than src
SELECT * FROM src WHERE a = 105;
SELECT * FROM des WHERE b = 115;

DROP TABLE des;
DROP TABLE src;

--example 8
BEGIN;
ALTER TABLE products_row DROP COLUMN category;

INSERT INTO products_row (product_id, product_name, total)
SELECT product_id, product_name, total  FROM newproducts_row
ON DUPLICATE KEY UPDATE product_name = products_row.product_name ||' + '|| __unnamed_subquery_source__.product_name,
total = products_row.total + __unnamed_subquery_source__.total;

SELECT * FROM products_row ORDER BY 1;

TRUNCATE products_row;
ALTER TABLE products_row ADD COLUMN category VARCHAR;
INSERT INTO products_row SELECT product_id, product_name, total, category FROM products_base;

INSERT into products_row (product_id, product_name, total)
SELECT product_id, product_name, total FROM newproducts_row
ON DUPLICATE KEY UPDATE product_name = products_row.product_name ||' + '|| __unnamed_subquery_source__.product_name,
total = products_row.total + __unnamed_subquery_source__.total;

SELECT * FROM products_row ORDER BY 1;
ROLLBACK;

--example 9
CREATE TABLE test_partition_source
(c_smallint SMALLINT, c_numeric NUMERIC, PRIMARY KEY(c_smallint))
    PARTITION by range (c_smallint)
(
    PARTITION TBL_1 VALUES LESS THAN (-1),
    PARTITION TBL_2 VALUES LESS THAN (30),
    PARTITION TBL_3 VALUES LESS THAN (60),
    PARTITION TBL_4 VALUES LESS THAN (100)
)disable ROW movement;

INSERT INTO test_partition_source VALUES(-1, 5);
INSERT INTO test_partition_source VALUES(40, 5);
INSERT INTO test_partition_source VALUES(66, 5);

CREATE TABLE test_partition(a INT, b NUMERIC,PRIMARY KEY(a));
INSERT INTO test_partition VALUES(-1, 10);
INSERT INTO test_partition VALUES(2, 10);

SET behavior_compat_options='merge_update_multi';

INSERT INTO test_partition_source (c_smallint, c_numeric)
SELECT * FROM test_partition
ON DUPLICATE KEY UPDATE c_numeric = __unnamed_subquery_source__.c_numeric;

SELECT * FROM test_partition_source ORDER BY 1, 2;

--- error: distribute key are not allowed to update
INSERT INTO test_partition_source (c_smallint, c_numeric)
SELECT * FROM test_partition
ON DUPLICATE KEY UPDATE c_smallint = 1;

reset behavior_compat_options;

ALTER TABLE test_partition_source enable ROW movement;

INSERT INTO test_partition_source (c_smallint, c_numeric)
SELECT * FROM test_partition
ON DUPLICATE KEY UPDATE c_numeric = __unnamed_subquery_source__.c_numeric;

SELECT * FROM test_partition_source ORDER BY 1, 2;

--- error: distribute key are not allowed to update
INSERT INTO test_partition_source (c_smallint, c_numeric)
SELECT * FROM test_partition
ON DUPLICATE KEY UPDATE c_smallint = 1;

reset behavior_compat_options;

-- clean up
DROP SCHEMA test_insert_update_005 CASCADE;
