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
) with(storage_type=ustore);

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
) with(storage_type=ustore);

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
) with(storage_type=ustore);

CREATE TABLE newproducts_row
(
    product_id INTEGER DEFAULT 0,
    product_name VARCHAR(60) DEFAULT 'null',
    category VARCHAR(60) DEFAULT 'unknown',
    total INTEGER DEFAULT '0',
    PRIMARY KEY (product_id)
) with(storage_type=ustore);

INSERT INTO products_row SELECT * FROM products_base;
INSERT INTO newproducts_row SELECT * FROM newproducts_base;

--example 1 ok
TRUNCATE products_row;
INSERT INTO products_row SELECT * FROM products_base;

INSERT INTO products_row (product_id, product_name, category, total)
SELECT product_id, product_name, category || 'ABC', total + 100 FROM newproducts_row
ON DUPLICATE KEY UPDATE products_row.product_name = products_row.product_name,
products_row.category= products_row.category || 'ABC',products_row.total= products_row.total + 100 ;

SELECT * FROM products_row ORDER BY 1;


--example 2 ok
TRUNCATE products_row;
INSERT INTO products_row SELECT * FROM products_base;

INSERT INTO products_row (product_id, product_name, category, total)
SELECT product_id, product_name, category || 'DEF', total + 200 FROM newproducts_row
ON DUPLICATE KEY UPDATE product_name = products_row.product_name, category = products_row.category || 'ABC',
total = products_row.total + 100;

SELECT * FROM products_row ORDER BY 1;

--example 3 ok
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

--example 4 ok
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

--example 5 ok
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

--example 6 ok
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
-- clean up
DROP SCHEMA test_insert_update_004 CASCADE;
