-- initial, transform from merge_1.sql
DROP SCHEMA test_insert_update_004 CASCADE;
CREATE SCHEMA test_insert_update_004;
SET current_schema = test_insert_update_004;

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

--example 1 ok
INSERT INTO products_row (product_id, product_name, category, total)
SELECT product_id, product_name, category, total FROM newproducts_row
ON DUPLICATE KEY UPDATE product_name = __unnamed_subquery_source__.product_name,
category= __unnamed_subquery_source__.category, total= __unnamed_subquery_source__.total;

SELECT * FROM products_row ORDER BY 1;

--example 2 ok
TRUNCATE products_row;
INSERT INTO products_row SELECT * FROM products_base;

INSERT INTO products_row (product_id, product_name, category, total)
SELECT product_id, product_name, category || 'ABC', total + 100 FROM newproducts_row
ON DUPLICATE KEY UPDATE products_row.product_name = __unnamed_subquery_source__.product_name,
products_row.category= __unnamed_subquery_source__.category, products_row.total= __unnamed_subquery_source__.total;

SELECT * FROM products_row ORDER BY 1;

--example 3 ok
TRUNCATE products_row;
INSERT INTO products_row SELECT * FROM products_base;

INSERT INTO products_row (product_id, product_name, category, total)
SELECT product_id, product_name, category || 'ABC', total + 100 FROM newproducts_row
ON DUPLICATE KEY UPDATE products_row.product_name = products_row.product_name,
products_row.category= products_row.category || 'ABC',products_row.total= products_row.total + 100 ;

SELECT * FROM products_row ORDER BY 1;

--example 4 ok
TRUNCATE products_row;
INSERT INTO products_row SELECT * FROM products_base;

INSERT INTO products_row (product_id, product_name, category, total)
SELECT product_id, product_name, category, total FROM newproducts_row
ON DUPLICATE KEY UPDATE product_name = products_row.product_name ||' + '|| __unnamed_subquery_source__.product_name,
category = products_row.category ||' + ' || __unnamed_subquery_source__.category,
total = products_row.total + __unnamed_subquery_source__.total;

SELECT * FROM products_row ORDER BY 1;

--example 5 ok
TRUNCATE products_row;
INSERT INTO products_row SELECT * FROM products_base;

INSERT INTO products_row (product_id, product_name, category, total)
SELECT product_id, product_name, category || 'DEF', total + 200 FROM newproducts_row
ON DUPLICATE KEY UPDATE product_name = products_row.product_name, category = products_row.category || 'ABC',
total = products_row.total + 100;

SELECT * FROM products_row ORDER BY 1;

--example 6 ok
TRUNCATE products_row;
INSERT INTO products_row SELECT * FROM products_base;

INSERT INTO products_row (product_id, product_name, category, total)
SELECT product_id, product_name, category || 'DEF', total + 200 FROM 
(
SELECT 1501 AS product_id, 'vivitar 35mm' AS product_name, 'electrncs' AS category, 100 AS total UNION ALL
SELECT 1502 AS product_id, 'olympus is50' AS product_name, 'electrncs' AS category, 100 AS total UNION ALL
SELECT 1600 AS product_id, 'play gym' AS product_name, 'toys' AS category, 100 AS total UNION ALL
SELECT 1601 AS product_id, 'lamaze' AS product_name, 'toys' AS category, 100 AS total UNION ALL
SELECT 1666 AS product_id, 'harry potter' AS product_name, 'dvd' AS category, 100 AS total
)
ON DUPLICATE KEY UPDATE product_name = products_row.product_name,
category = products_row.category || 'ABC',
total = products_row.total + 100;

SELECT * FROM products_row ORDER BY 1;

--example 7 ok
TRUNCATE products_row;
INSERT INTO products_row SELECT * FROM products_base;

INSERT INTO products_row (product_id, product_name, category, total)
SELECT product_id, product_name, category || 'DEF', total + 200 FROM
(
SELECT product_id, product_name, category, SUM(total) AS total FROM newproducts_row GROUP BY product_id, product_name, category
)
ON DUPLICATE KEY UPDATE product_name = products_row.product_name,
category = products_row.category || 'ABC',
total = products_row.total + 100;

SELECT * FROM products_row ORDER BY 1;

--example 8 ok
TRUNCATE products_row;
INSERT INTO products_row SELECT * FROM products_base;

INSERT INTO products_row (product_id, product_name, category, total)
SELECT product_id, product_name, category || 'DEF', total + 200 FROM
(
SELECT product_id, product_name, category, SUM(total) AS total
FROM newproducts_row
GROUP BY product_id, product_name, category
HAVING SUM(total) > 100
ORDER BY total
)
ON DUPLICATE KEY UPDATE product_name = products_row.product_name,
category = products_row.category || 'ABC',
total = products_row.total + 100;

SELECT * FROM products_row ORDER BY 1;

--example 9 ok
TRUNCATE products_row;
INSERT INTO products_row SELECT * FROM products_base;

INSERT INTO products_row (product_id, product_name, category, total)
SELECT product_id, product_name, category || 'DEF', total + 200  FROM
(
    SELECT sp.product_id, sp.product_name, snp.category, snp.total FROM newproducts_row snp, products_row sp WHERE sp.product_id = snp.product_id
)
ON DUPLICATE KEY UPDATE product_name = products_row.product_name ,category = products_row.category || 'ABC',
total = products_row.total + 100;

SELECT * FROM products_row ORDER BY 1;

--example 10 ok
CREATE TABLE products_part
(
    product_id INTEGER,
    product_name VARCHAR2(60),
    category VARCHAR2(60),
    total INTEGER,
    primary key(product_id)
)
PARTITION BY RANGE (product_id)
(
    PARTITION P1 VALUES LESS THAN (1600),
    PARTITION P2 VALUES LESS THAN (1700),
    PARTITION P3 VALUES LESS THAN (1800)
)ENABLE ROW MOVEMENT;

INSERT INTO products_part SELECT * FROM products_base;

INSERT INTO products_part (product_id, product_name, category, total)
SELECT product_id, product_name, category, total FROM newproducts_row
ON DUPLICATE KEY UPDATE product_name = __unnamed_subquery_source__.product_name,
category= __unnamed_subquery_source__.category, total= __unnamed_subquery_source__.total;

SELECT * FROM products_part ORDER BY 1;

-- test unlogged table
CREATE TABLE customer
(
c_customer_sk INTEGER NOT NULL,
c_customer_id char(16) NOT NULL,
c_current_cdemo_sk INTEGER,
c_current_hdemo_sk INTEGER,
PRIMARY KEY(c_customer_sk)
)DISTRIBUTE BY hash (c_customer_sk);

CREATE unlogged TABLE col_using
(
c1 INT,
c2 VARCHAR(100),
c3 DATE,
c4 TIMESTAMP WITH TIME ZONE,
c6 text,
PRIMARY KEY(c1)
)DISTRIBUTE BY hash(c1);

INSERT INTO customer VALUES (1,2,3,4);

-- should insert
INSERT INTO col_using (c1, c2, c6)
SELECT c_customer_sk, CONCAT(TRIM(BOTH ' ' FROM c_customer_id),' ') cc,
SUM(c_current_cdemo_sk) c_sum
FROM customer
GROUP BY 1,2
ORDER BY 1,2,3 LIMIT 1
ON DUPLICATE KEY UPDATE c2 = col_using.c2 || 'ABC', c6 = col_using.c6;
SELECT * FROM col_using;

-- should update
INSERT INTO col_using (c1, c2, c6)
SELECT c_customer_sk, CONCAT(TRIM(BOTH ' ' FROM c_customer_id),' ') cc,
SUM(c_current_cdemo_sk) c_sum
FROM customer
GROUP BY 1,2
ORDER BY 1,2,3 LIMIT 1
ON DUPLICATE KEY UPDATE c2 = col_using.c2 || 'ABC', c6 = col_using.c6;
SELECT * FROM col_using;

-- clean up
DROP SCHEMA test_insert_update_004 CASCADE;
