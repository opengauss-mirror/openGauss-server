--
-- MERGE INTO 
--

-- initial
CREATE SCHEMA mergeinto_1;
SET current_schema = mergeinto_1;

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
-- stream mode(MERGE can be pushed down), row table
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
  UPDATE SET product_name = np.product_name, category = np.category, total = np.total where np.product_id = 5;

EXPLAIN (VERBOSE on, COSTS off)
MERGE INTO products_row p
USING newproducts_row np
ON p.product_id = np.product_id
WHEN MATCHED THEN
  UPDATE SET product_name = np.product_name, category = np.category, total = np.total
WHEN NOT MATCHED THEN  
  INSERT VALUES (np.product_id, np.product_name, np.category, np.total);

-- only MATCHED clause
MERGE INTO products_row p
USING newproducts_row np
ON p.product_id = np.product_id
WHEN MATCHED THEN
  UPDATE SET product_name = np.product_name, category = np.category, total = np.total;
SELECT * FROM products_row ORDER BY 1;

-- only MATCHED clause, has expressions
TRUNCATE products_row;
INSERT INTO products_row SELECT * FROM products_base;

MERGE INTO products_row p
USING newproducts_row np
ON p.product_id = np.product_id
WHEN MATCHED THEN
  UPDATE SET product_name = np.product_name, category = np.category || 'ABC', total = np.total + 100;
SELECT * FROM products_row ORDER BY 1;

-- only NOT MATCHED clause
TRUNCATE products_row;
INSERT INTO products_row SELECT * FROM products_base;

MERGE INTO products_row p
USING newproducts_row np
ON p.product_id = np.product_id
WHEN NOT MATCHED THEN  
  INSERT VALUES (np.product_id, np.product_name, np.category, np.total);
SELECT * FROM products_row ORDER BY 1;

TRUNCATE products_row;
INSERT INTO products_row SELECT * FROM products_base;

MERGE INTO products_row p
USING newproducts_row np
ON p.product_id = np.product_id
WHEN NOT MATCHED THEN  
  INSERT VALUES (np.product_id, np.product_name, np.category);
SELECT * FROM products_row ORDER BY 1;

-- only NOT MATCHED clause has insert targetlist
TRUNCATE products_row;
INSERT INTO products_row SELECT * FROM products_base;

MERGE INTO products_row p
USING newproducts_row np
ON p.product_id = np.product_id
WHEN NOT MATCHED THEN 
  INSERT (product_id, total) VALUES (np.product_id, np.total); --notice: we have 2 fields missing
SELECT * FROM products_row ORDER BY 1;

TRUNCATE products_row;
INSERT INTO products_row SELECT * FROM products_base;

MERGE INTO products_row p
USING newproducts_row np
ON p.product_id = np.product_id
WHEN NOT MATCHED THEN  
  INSERT (product_id, total) VALUES (np.product_id, np.total); --notice: we have 2 fields missing
SELECT * FROM products_row ORDER BY 1;

-- only NOT MATCHED clause has insert targetlist
TRUNCATE products_row;
INSERT INTO products_row SELECT * FROM products_base;

MERGE INTO products_row p
USING newproducts_row np
ON p.product_id = np.product_id
WHEN NOT MATCHED THEN 
  INSERT (total, product_id) VALUES (np.total, np.product_id); --notice: 2 fields missing and reversed
SELECT * FROM products_row ORDER BY 1;

TRUNCATE products_row;
INSERT INTO products_row SELECT * FROM products_base;

MERGE INTO products_row p
USING newproducts_row np
ON p.product_id = np.product_id
WHEN NOT MATCHED THEN  
  INSERT (total, product_id) VALUES (np.total, np.product_id); --notice: 2 fields missing and reversed
SELECT * FROM products_row ORDER BY 1;

-- only NOT MATCHED clause, has expressions
TRUNCATE products_row;
INSERT INTO products_row SELECT * FROM products_base;

MERGE INTO products_row p
USING newproducts_row np
ON p.product_id = np.product_id
WHEN NOT MATCHED THEN  
  INSERT VALUES (np.product_id, np.product_name, np.category || 'ABC', np.total + 100);
SELECT * FROM products_row ORDER BY 1;

-- both MATCHED and NOT MATCHED clause
TRUNCATE products_row;
INSERT INTO products_row SELECT * FROM products_base;

MERGE INTO products_row p
USING newproducts_row np
ON p.product_id = np.product_id
WHEN MATCHED THEN
  UPDATE SET product_name = np.product_name, category = np.category, total = np.total
WHEN NOT MATCHED THEN  
  INSERT VALUES (np.product_id, np.product_name, np.category, np.total);
SELECT * FROM products_row ORDER BY 1;

-- both MATCHED and NOT MATCHED clause has constant qual
TRUNCATE products_row;
INSERT INTO products_row SELECT * FROM products_base;

MERGE INTO products_row p
USING newproducts_row np
ON p.product_id = np.product_id AND 1=1
WHEN MATCHED THEN
  UPDATE SET product_name = np.product_name, category = np.category, total = np.total
WHEN NOT MATCHED THEN  
  INSERT VALUES (np.product_id, np.product_name, np.category, np.total);
SELECT * FROM products_row ORDER BY 1,2,3,4;

-- both MATCHED and NOT MATCHED clause has constant qual with subquery
TRUNCATE products_row;
INSERT INTO products_row SELECT * FROM products_base;

MERGE INTO products_row p
USING newproducts_row np
ON p.product_id = np.product_id AND 1=(select total from products_row order by 1 limit 1)
WHEN MATCHED THEN
  UPDATE SET product_name = np.product_name, category = np.category, total = np.total
WHEN NOT MATCHED THEN  
  INSERT VALUES (np.product_id, np.product_name, np.category, np.total);
SELECT * FROM products_row ORDER BY 1,2,3,4;

-- both MATCHED and NOT MATCHED clause, has expressions
TRUNCATE products_row;
INSERT INTO products_row SELECT * FROM products_base;

MERGE INTO products_row p
USING newproducts_row np
ON p.product_id = np.product_id
WHEN MATCHED THEN
  UPDATE SET product_name = np.product_name, category = np.category || 'ABC', total = np.total + 100
WHEN NOT MATCHED THEN  
  INSERT VALUES (np.product_id, np.product_name, np.category || 'ABC', np.total + 100);
SELECT * FROM products_row ORDER BY 1;

MERGE INTO products_row p
USING newproducts_row np
ON p.product_id = np.product_id
WHEN MATCHED THEN
  UPDATE SET product_name = p.product_name, category = p.category || 'ABC', total = p.total + 100
WHEN NOT MATCHED THEN  
  INSERT VALUES (np.product_id, np.product_name, np.category || 'ABC', np.total + 100);
SELECT * FROM products_row ORDER BY 1;

-- both MATCHED and NOT MATCHED clause, has expressions, has WHERE conditions
TRUNCATE products_row;
INSERT INTO products_row SELECT * FROM products_base;

MERGE INTO products_row p
USING newproducts_row np
ON p.product_id = np.product_id
WHEN MATCHED THEN
  UPDATE SET product_name = np.product_name, category = np.category || 'ABC', total = np.total + 100 WHERE p.category = 'dvd'
WHEN NOT MATCHED THEN  
  INSERT VALUES (np.product_id, np.product_name, np.category || 'ABC', np.total + 100) WHERE np.category != 'books';
SELECT * FROM products_row ORDER BY 1;

-- both MATCHED and NOT MATCHED clause, has expressions, which WHERE conditions is constant
TRUNCATE products_row;
INSERT INTO products_row SELECT * FROM products_base;

MERGE INTO products_row p
USING newproducts_row np
ON p.product_id = np.product_id
WHEN MATCHED THEN
  UPDATE SET product_name = np.product_name, category = np.category || 'ABC', total = np.total + 100 WHERE true
WHEN NOT MATCHED THEN  
  INSERT VALUES (np.product_id, np.product_name, np.category || 'ABC', np.total + 100) WHERE false;
SELECT * FROM products_row ORDER BY 1;

-- both MATCHED and NOT MATCHED clause, has expressions, has WHERE conditions
TRUNCATE products_row;
INSERT INTO products_row SELECT * FROM products_base;

MERGE INTO products_row p
USING newproducts_row np
ON p.product_id = np.product_id
WHEN MATCHED THEN
  UPDATE SET product_name = np.product_name, category = np.category || 'ABC', total = np.total + 100
  WHERE p.category = 'dvd' AND np.category = 'toys'
WHEN NOT MATCHED THEN  
  INSERT VALUES (np.product_id, np.product_name, np.category || 'DEF', np.total + 100)
  WHERE np.category != 'books' AND np.total > 100;

SELECT * FROM products_row ORDER BY 1;

-- partitioned table
CREATE TABLE products_part
(
product_id INTEGER,
product_name VARCHAR2(60),
category VARCHAR2(60),
total INTEGER
)
PARTITION BY RANGE (product_id)
(
  PARTITION P1 VALUES LESS THAN (1600),
  PARTITION P2 VALUES LESS THAN (1700),
  PARTITION P3 VALUES LESS THAN (1800)
) ENABLE ROW MOVEMENT;

INSERT INTO products_part SELECT * FROM products_base;
ANALYZE products_part;

MERGE INTO products_part p
USING newproducts_row np
ON p.product_id = np.product_id
WHEN MATCHED THEN
  UPDATE SET product_name = np.product_name, category = np.category || 'ABC', total = np.total + 100 WHERE p.category = 'dvd'
WHEN NOT MATCHED THEN  
  INSERT VALUES (np.product_id, np.product_name, np.category || 'ABC', np.total + 100) WHERE np.category != 'books';
SELECT * FROM products_part ORDER BY 1;


-- do a simple equivalent of an INSERT SELECT
TRUNCATE products_row;
INSERT INTO products_row SELECT * FROM products_base;

SELECT * FROM products_row ORDER BY 1;
SELECT * FROM newproducts_row ORDER BY 1;

MERGE INTO products_row p
USING newproducts_row np
ON p.product_id = np.product_id
WHEN NOT MATCHED THEN
    INSERT VALUES (np.product_id, np.product_name, np.category, np.total);

SELECT * FROM products_row ORDER BY 1;


-- the classic merge
TRUNCATE products_row;
INSERT INTO products_row SELECT * FROM products_base;

SELECT * FROM products_row ORDER BY 1;
SELECT * FROM newproducts_row ORDER BY 1;

MERGE INTO products_row p
USING newproducts_row np
ON p.product_id = np.product_id
WHEN NOT MATCHED THEN
    INSERT VALUES (np.product_id, np.product_name, np.category, np.total)
WHEN MATCHED THEN
    UPDATE SET product_name = p.product_name ||' + '|| np.product_name,
	           category     = p.category ||' + ' || np.category,
			   total        = p.total + np.total;

SELECT * FROM products_row ORDER BY 1;

-- do a simple equivalent of an INSERT SELECT with targetlist
TRUNCATE products_row;
INSERT INTO products_row SELECT * FROM products_base;

SELECT * FROM products_row ORDER BY 1;
SELECT * FROM newproducts_row ORDER BY 1;

MERGE INTO products_row p
USING newproducts_row np
ON p.product_id = np.product_id
WHEN NOT MATCHED THEN
    INSERT (product_id, product_name, total) 
	VALUES (np.product_id, np.product_name, np.total);

SELECT * FROM products_row ORDER BY 1;


-- the on clause is true
TRUNCATE products_row;
INSERT INTO products_row SELECT * FROM products_base;

SELECT * FROM products_row ORDER BY 1;
SELECT * FROM newproducts_row ORDER BY 1;

MERGE INTO products_row p
USING newproducts_row np
ON (select true)
WHEN NOT MATCHED THEN
    INSERT (product_id, product_name, total) 
	VALUES (np.product_id, np.product_name, np.total);

SELECT * FROM products_row ORDER BY 1;

--subquery
TRUNCATE products_row;
INSERT INTO products_row SELECT * FROM products_base;

MERGE INTO products_row p
USING (select * from newproducts_row ) np
ON p.product_id = np.product_id
WHEN MATCHED THEN 
	UPDATE SET product_name = p.product_name, category = p.category || 'ABC', total = p.total + 100
WHEN NOT MATCHED THEN
	INSERT VALUES (np.product_id, np.product_name, np.category || 'DEF', np.total + 200);

SELECT * FROM products_row ORDER BY 1;

--subquery with expression
TRUNCATE products_row;
INSERT INTO products_row SELECT * FROM products_base;

MERGE INTO products_row p
USING (select * from newproducts_row ) np
ON p.product_id = np.product_id
WHEN MATCHED THEN 
	UPDATE SET product_name = p.product_name, category = p.category || 'ABC', total = p.total + 100 WHERE np.product_name ='lamaze'
WHEN NOT MATCHED THEN
	INSERT VALUES (np.product_id, np.product_name, np.category || 'DEF', np.total + 200) WHERE np.product_name = 'wait interface';

SELECT * FROM products_row order by 1;

--subquery with expression
TRUNCATE products_row;
INSERT INTO products_row SELECT * FROM products_base;

MERGE INTO products_row p
USING (select sp.product_id, sp.product_name, snp.category, snp.total from newproducts_row snp, products_row sp where sp.product_id = snp.product_id) np
ON p.product_id = np.product_id
WHEN MATCHED THEN 
	UPDATE SET product_name = p.product_name, category = p.category || 'ABC', total = p.total + 100 WHERE product_name ='lamaze'
WHEN NOT MATCHED THEN
	INSERT VALUES (np.product_id, np.product_name, np.category || 'DEF', np.total + 200) WHERE np.product_name = 'wait interface';

SELECT * FROM products_row order by 1;


--subquery has constant
TRUNCATE products_row;
INSERT INTO products_row SELECT * FROM products_base;

MERGE INTO products_row p
USING (
select 1501 as product_id, 'vivitar 35mm' as product_name, 'electrncs' as category, 100 as total union all
select 1502 as product_id, 'olympus is50' as product_name, 'electrncs' as category, 100 as total union all
select 1600 as product_id, 'play gym' as product_name, 'toys' as category, 100 as total union all
select 1601 as product_id, 'lamaze' as product_name, 'toys' as category, 100 as total union all
select 1666 as product_id, 'harry potter' as product_name, 'dvd' as category, 100 as total
 ) np
ON p.product_id = np.product_id
WHEN MATCHED THEN 
	UPDATE SET product_name = p.product_name, category = p.category || 'ABC', total = p.total + 100
WHEN NOT MATCHED THEN
	INSERT VALUES (np.product_id, np.product_name, np.category || 'DEF', np.total + 200);

SELECT * FROM products_row ORDER BY 1;


--subquery has aggeration
TRUNCATE products_row;
INSERT INTO products_row SELECT * FROM products_base;

MERGE INTO products_row p
USING (
select product_id, product_name, category, sum(total) as total from newproducts_row group by product_id, product_name, category
 ) np
ON p.product_id = np.product_id
WHEN MATCHED THEN 
	UPDATE SET product_name = p.product_name, category = p.category || 'ABC', total = p.total + 100
WHEN NOT MATCHED THEN
	INSERT VALUES (np.product_id, np.product_name, np.category || 'DEF', np.total + 200);

SELECT * FROM products_row ORDER BY 1;

--subquery has aggeration
TRUNCATE products_row;
INSERT INTO products_row SELECT * FROM products_base;

MERGE INTO products_row p
USING (
select product_id, product_name, category, sum(total) as total
from newproducts_row
group by product_id, product_name, category
having sum(total)>100
order by total
 ) np
ON p.product_id = np.product_id
WHEN MATCHED THEN 
	UPDATE SET product_name = p.product_name, category = p.category || 'ABC', total = p.total + 100
WHEN NOT MATCHED THEN
	INSERT VALUES (np.product_id, np.product_name, np.category || 'DEF', np.total + 200);

SELECT * FROM products_row ORDER BY 1;

-- clean up
DROP SCHEMA mergeinto_1 CASCADE;
