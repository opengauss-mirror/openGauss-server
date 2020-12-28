--
-- MERGE INTO 
--

-- initial
CREATE SCHEMA xc_mergeinto;
SET current_schema = xc_mergeinto;

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
-- PGXC mode(MERGE cannot be pushed down), row table
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


--source table have a view
TRUNCATE products_row;
INSERT INTO products_row SELECT * FROM products_base;
CREATE VIEW v AS (SELECT * FROM newproducts_row WHERE total > 100);

MERGE INTO products_row p
USING v np
ON p.product_id = np.product_id
WHEN MATCHED THEN 
	UPDATE SET product_name = p.product_name, category = p.category || 'ABC', total = p.total + 100
WHEN NOT MATCHED THEN
	INSERT VALUES (np.product_id, np.product_name, np.category || 'DEF', np.total + 200);

SELECT * FROM products_row ORDER BY 1;

--self merge
TRUNCATE products_row;
INSERT INTO products_row SELECT * FROM products_base;

BEGIN;

MERGE INTO products_row p
USING products_row np
ON p.product_id = np.product_id
WHEN MATCHED THEN 
	UPDATE SET product_name = p.product_name, category = p.category || 'ABC', total = p.total + 100
WHEN NOT MATCHED THEN
	INSERT VALUES (np.product_id, np.product_name, np.category || 'DEF', np.total + 200);

SELECT * FROM products_row ORDER BY 1;

ROLLBACK;
-- Function scans
CREATE TABLE fs_target (a int, b int, c text);
MERGE INTO fs_target t
USING generate_series(1,100,1) AS id
ON t.a = id
WHEN MATCHED THEN
    UPDATE SET b = b + id
WHEN NOT MATCHED THEN
    INSERT VALUES (id, -1);

MERGE INTO fs_target t
USING generate_series(1,100,2) AS id
ON t.a = id
WHEN MATCHED THEN
    UPDATE SET b = b + id, c = 'updated '|| id.*::text
WHEN NOT MATCHED THEN
    INSERT VALUES (id, -1, 'inserted ' || id.*::text);

SELECT count(*) FROM fs_target;
DROP TABLE fs_target;

--default values for insert values
BEGIN;

MERGE INTO products_row p
USING newproducts_row np
ON p.product_id = np.product_id
WHEN NOT MATCHED THEN
  INSERT VALUES (DEFAULT);

MERGE INTO products_row p
USING newproducts_row np
ON p.product_id = np.product_id
WHEN NOT MATCHED THEN
  INSERT VALUES (DEFAULT, np.product_name, np.category, DEFAULT);

MERGE INTO products_row p
USING newproducts_row np
ON p.product_id = np.product_id
WHEN NOT MATCHED THEN
  INSERT DEFAULT VALUES;

select * from  products_row order by 1,2,3,4;
ROLLBACK;

--default values for update values
BEGIN;

MERGE INTO products_row p
USING newproducts_row np
ON p.product_id = np.product_id
WHEN MATCHED THEN
  UPDATE SET product_name = DEFAULT, category = DEFAULT, total = DEFAULT 
WHEN NOT MATCHED THEN
  INSERT VALUES (DEFAULT);

select * from  products_row order by 1;  
ROLLBACK;


----------------------plpgsql-----------------------------------

BEGIN;
DO LANGUAGE plpgsql $$
BEGIN
MERGE INTO products_row p
USING newproducts_row np
ON p.product_id = np.product_id
WHEN MATCHED THEN
  UPDATE SET product_name = np.product_name, category = np.category , total = np.total
WHEN NOT MATCHED THEN
  INSERT VALUES (np.product_id, np.product_name, np.category, np.total);
END;
$$;
SELECT * FROM products_row order by 1;
ROLLBACK;

--NON ANOYBLOCK 
BEGIN;

BEGIN
MERGE INTO products_row p
USING newproducts_row np
ON p.product_id = np.product_id
WHEN MATCHED THEN
  UPDATE SET product_name = np.product_name, category = np.category , total = np.total
WHEN NOT MATCHED THEN
  INSERT VALUES (np.product_id, np.product_name, np.category, np.total);
END;
/

SELECT * FROM products_row order by 1;

ROLLBACK;


--stored procedure
create or replace procedure p1()
AS
BEGIN
	MERGE INTO products_row p
	USING newproducts_row np
	ON p.product_id = np.product_id
	WHEN MATCHED THEN
	  UPDATE SET product_name = np.product_name, category = np.category , total = np.total
	WHEN NOT MATCHED THEN
	  INSERT VALUES (np.product_id, np.product_name, np.category, np.total);
END;
/

BEGIN;
select p1();
SELECT * FROM products_row order by 1;
ROLLBACK;

--stored procedure with params in insert targetlist
create or replace procedure p2( param1 IN text)
AS
BEGIN
	MERGE INTO products_row p
	USING newproducts_row np
	ON p.product_id = np.product_id
	WHEN MATCHED THEN
	  UPDATE SET product_name = np.product_name, category = np.category , total = np.total
	WHEN NOT MATCHED THEN
	  INSERT VALUES (np.product_id, 'inserted by proc', param1, np.total);
END;
/

BEGIN;
select p2('param1');
SELECT * FROM products_row order by 1;
ROLLBACK;

BEGIN;
select p2('param2');
SELECT * FROM products_row order by 1;
ROLLBACK;


--stored procedure with params in update targetlist
create or replace procedure p3( param1 IN text)
AS
BEGIN
	MERGE INTO products_row p
	USING newproducts_row np
	ON p.product_id = np.product_id
	WHEN MATCHED THEN
	  UPDATE SET product_name = np.product_name, category = param1 , total = np.total
	WHEN NOT MATCHED THEN
	  INSERT VALUES (np.product_id, 'inserted by proc', np.category, np.total);
END;
/

BEGIN;
select p3('param1');
SELECT * FROM products_row order by 1;
ROLLBACK;

BEGIN;
select p3('param2');
SELECT * FROM products_row order by 1;
ROLLBACK;


--stored procedure with params in where conditions
create or replace procedure p4( param1 IN text)
AS
BEGIN
	MERGE INTO products_row p
	USING newproducts_row np
	ON p.product_id = np.product_id
	WHEN MATCHED THEN
	  UPDATE SET product_name = np.product_name, category = np.category || 'DEF' , total = np.total
	  WHERE category = param1
	WHEN NOT MATCHED THEN
	  INSERT VALUES (np.product_id, 'inserted by proc', np.category, np.total);
END;
/

BEGIN;
select p4('toys'); --only update toys
SELECT * FROM products_row order by 1;
ROLLBACK;

BEGIN;
select p4('dvd'); --only update dvd
SELECT * FROM products_row order by 1;
ROLLBACK;

BEGIN;
select p4('electrncs'); --only update electrncs
SELECT * FROM products_row order by 1;
ROLLBACK;

--dropped column and add column
BEGIN;
ALTER TABLE products_row DROP COLUMN category;

MERGE INTO products_row p
USING newproducts_row np
ON p.product_id = np.product_id
WHEN NOT MATCHED THEN
    INSERT VALUES (np.product_id, np.product_name, np.total)
WHEN MATCHED THEN
    UPDATE SET product_name = p.product_name ||' + '|| np.product_name,
			   total        = p.total + np.total;

SELECT * FROM products_row ORDER BY 1;

TRUNCATE products_row;
ALTER TABLE products_row ADD COLUMN category VARCHAR;
INSERT INTO products_row SELECT product_id,product_name,total,category FROM products_base;

MERGE INTO products_row p
USING newproducts_row np
ON p.product_id = np.product_id
WHEN NOT MATCHED THEN
    INSERT VALUES (np.product_id, np.product_name, np.total)
WHEN MATCHED THEN
    UPDATE SET product_name = p.product_name ||' + '|| np.product_name,
			   total        = p.total + np.total;

SELECT * FROM products_row ORDER BY 1;
ROLLBACK;

--join key diffs from distribute key
BEGIN;
MERGE INTO products_row p
USING newproducts_row np
ON p.total = np.product_id
WHEN NOT MATCHED THEN
    INSERT VALUES (np.product_id, np.product_name, np.category, np.total);

SELECT * FROM products_row ORDER BY 1,2,3,4;
ROLLBACK;

BEGIN;
MERGE INTO products_row p
USING newproducts_row np
ON p.total = np.product_id
WHEN MATCHED THEN
    UPDATE SET product_name = p.product_name ||' + '|| np.product_name;

SELECT * FROM products_row ORDER BY 1;
ROLLBACK;

BEGIN;
MERGE INTO products_row p
USING newproducts_row np
ON p.total = np.product_id
WHEN NOT MATCHED THEN
    INSERT VALUES (np.product_id, np.product_name, np.category, np.total)
WHEN MATCHED THEN
    UPDATE SET product_name = p.product_name ||' + '|| np.product_name;

SELECT * FROM products_row ORDER BY 1,2,3,4;
ROLLBACK;

BEGIN;
MERGE INTO products_row p
USING newproducts_row np
ON p.total = np.total
WHEN NOT MATCHED THEN
    INSERT VALUES (np.product_id, np.product_name, np.category, np.total);

SELECT * FROM products_row ORDER BY 1,2,3,4;
ROLLBACK;

BEGIN;
MERGE INTO products_row p
USING newproducts_row np
ON p.total = np.total
WHEN MATCHED THEN
    UPDATE SET product_name = p.product_name ||' + '|| np.product_name;

SELECT * FROM products_row ORDER BY 1;
ROLLBACK;

BEGIN;
MERGE INTO products_row p
USING newproducts_row np
ON p.total = np.total
WHEN NOT MATCHED THEN
    INSERT VALUES (np.product_id, np.product_name, np.category, np.total)
WHEN MATCHED THEN
    UPDATE SET product_name = p.product_name ||' + '|| np.product_name;

SELECT * FROM products_row ORDER BY 1,2,3,4;
ROLLBACK;

--target table distributed by multiple columns, and then test dropped column and added column
CREATE TABLE products_row_multi
(
product_id INTEGER,
product_name VARCHAR(60),
category VARCHAR(60),
total INTEGER
)
DISTRIBUTE BY HASH(product_id, category);

INSERT INTO products_row_multi SELECT * FROM products_base;
ANALYZE products_row_multi;

BEGIN;
MERGE INTO products_row_multi p
USING newproducts_row np
ON p.product_id = np.product_id
WHEN MATCHED THEN
  UPDATE SET product_name = p.product_name || 'ABC', total = p.total + 100
WHEN NOT MATCHED THEN  
  INSERT VALUES (np.product_id, np.product_name, np.category || 'ABC', np.total + 100);

SELECT * FROM products_row_multi ORDER BY 1;
ROLLBACK;

BEGIN;
UPDATE products_row_multi SET total = 100; --generate some dead rows
MERGE INTO products_row_multi p
USING newproducts_row np
ON p.product_id = np.product_id
WHEN MATCHED THEN
  UPDATE SET product_name = p.product_name || 'ABC', total = p.total + 100
WHEN NOT MATCHED THEN  
  INSERT VALUES (np.product_id, np.product_name, np.category || 'ABC', np.total + 100);

SELECT * FROM products_row_multi ORDER BY 1;
ROLLBACK;

ALTER TABLE products_row_multi DROP COLUMN product_name; --dropped column
BEGIN;
MERGE INTO products_row_multi p
USING newproducts_row np
ON p.product_id = np.product_id
WHEN MATCHED THEN
  UPDATE SET total = p.total + 100
WHEN NOT MATCHED THEN  
  INSERT VALUES (np.product_id, np.category || 'ABC', np.total + 100);

SELECT * FROM products_row_multi ORDER BY 1;
ROLLBACK;

ALTER TABLE products_row_multi ADD COLUMN product_name VARCHAR;  --added column
TRUNCATE products_row_multi;
INSERT INTO products_row_multi SELECT product_id,category,total,product_name FROM products_base;

BEGIN;
MERGE INTO products_row_multi p
USING newproducts_row np
ON p.product_id = np.product_id
WHEN MATCHED THEN
  UPDATE SET product_name = p.product_name || 'ABC', total = p.total + 100;

SELECT * FROM products_row_multi ORDER BY 1;
ROLLBACK;

BEGIN;
MERGE INTO products_row_multi p
USING newproducts_row np
ON p.product_id = np.product_id
WHEN NOT MATCHED THEN  
  INSERT VALUES (np.product_id, np.category || 'ABC', np.total + 100, np.product_name);

SELECT * FROM products_row_multi ORDER BY 1;
ROLLBACK;

BEGIN;
MERGE INTO products_row_multi p
USING newproducts_row np
ON p.product_id = np.product_id
WHEN MATCHED THEN
  UPDATE SET product_name = p.product_name || 'ABC', total = p.total + 100
WHEN NOT MATCHED THEN  
  INSERT VALUES (np.product_id, np.category || 'ABC', np.total + 100, np.product_name);

SELECT * FROM products_row_multi ORDER BY 1;
ROLLBACK;

----------------------------
--
--  ERROR:  attribute 3 has wrong type
--  DETAIL:  Table has type bigint, but query expects timestamp without time zone.
----------------------------
create table customer
(
    c_customer_sk             integer               not null,
    c_customer_id             char(16)              not null,
    c_current_cdemo_sk        integer                       ,
    c_current_hdemo_sk        integer                       ,
    c_current_addr_sk         integer                       ,
    c_first_shipto_date_sk    integer                       ,
    c_first_sales_date_sk     integer                       ,
    c_salutation              char(10)                      ,
    c_first_name              char(20)                      ,
    c_last_name               char(30)                      ,
    c_preferred_cust_flag     char(1)                       ,
    c_birth_day               integer                       ,
    c_birth_month             integer                       ,
    c_birth_year              integer                       ,
    c_birth_country           varchar(20)                   ,
    c_login                   char(13)                      ,
    c_email_address           char(50)                      ,
    c_last_review_date        char(10)                      
) 
 with (orientation = column,compression=middle)
distribute by hash (c_customer_sk);

insert into customer values (1,2,3,4,5,6,7,8,9,10,1,12,13,14,15,16,17,18);

--distribute on c3
create unlogged table col_using
(c1 int, 
c2 varchar(100), 
c3 date, 
c4 timestamp with time zone, 
c5 numeric, 
c6 text )distribute by hash(c3);

merge into col_using t1
using(
select c_customer_sk, concat(trim(both ' ' from c_first_name),' ') cc,
sum(c_current_cdemo_sk) c_sum
from customer 
group by 1,2
order by 1,2,3 limit 1
)t2 on (t1.c1=t2.c_sum)
when not matched then insert values(t2.c_customer_sk);

select * from col_using;

--distribute on c3, c6
create unlogged table col_using2
(c1 int, 
c2 varchar(100), 
c3 date, 
c4 timestamp with time zone, 
c5 numeric, 
c6 text )distribute by hash(c3,c6);

merge into col_using2 t1
using(
select c_customer_sk, concat(trim(both ' ' from c_first_name),' ') cc,
sum(c_current_cdemo_sk) c_sum
from customer 
group by 1,2
order by 1,2,3 limit 1
)t2 on (t1.c1=t2.c_sum)
when not matched then insert values(t2.c_customer_sk);

select * from col_using2;


--distribute on c3, c6 with c1 dropped
create unlogged table col_using3
(c1 int, 
c2 varchar(100), 
c3 date, 
c4 timestamp with time zone, 
c5 numeric, 
c6 text )distribute by hash(c3,c6);

alter table col_using3 drop column c1;

merge into col_using3 t1
using(
select c_customer_sk, concat(trim(both ' ' from c_first_name),' ') cc,
sum(c_current_cdemo_sk) c_sum
from customer 
group by 1,2
order by 1,2,3 limit 1
)t2 on (t1.c2=t2.c_sum)
when not matched then insert values(t2.c_customer_sk);

select * from col_using3;


--add testcase for dts
 CREATE  TABLE item (                                    
         i_item_sk integer NOT NULL,                     
         i_item_id character(16) NOT NULL,               
         i_rec_start_date timestamp(0) without time zone,
         i_rec_end_date timestamp(0) without time zone,  
         i_item_desc character varying(200),             
         i_current_price numeric(7,2),                   
         i_wholesale_cost numeric(7,2),                  
         i_brand_id integer,                             
         i_brand character(50),                          
         i_class_id integer,                             
         i_class character(50),                          
         i_category_id integer,                          
         i_category character(50),                       
         i_manufact_id integer,                          
         i_manufact character(50),                       
         i_size character(20),                           
         i_formulation character(20),                    
         i_color character(20),                          
         i_units character(10),                          
         i_container character(10),                      
         i_manager_id integer,                           
         i_product_name character(50)                    
 )                                                       
 WITH (orientation=column, compression=low)              
 DISTRIBUTE BY HASH(i_item_sk)  ;

 CREATE  TABLE date_dim (                                               
         d_date_sk integer NOT NULL,                                    
         d_date_id character(16) NOT NULL,                              
         d_date timestamp(0) without time zone,                         
         d_month_seq integer,                                           
         d_week_seq integer,                                            
         d_quarter_seq integer,                                         
         d_year integer,                                                
         d_dow integer,                                                 
         d_moy integer,                                                 
         d_dom integer,                                                 
         d_qoy integer,                                                 
         d_fy_year integer,                                             
         d_fy_quarter_seq integer,                                      
         d_fy_week_seq integer,                                         
         d_day_name character(9),                                       
         d_quarter_name character(6),                                   
         d_holiday character(1),                                        
         d_weekend character(1),                                        
         d_following_holiday character(1),                              
         d_first_dom integer,                                           
         d_last_dom integer,                                            
         d_same_day_ly integer,                                         
         d_same_day_lq integer,                                         
         d_current_day character(1),                                    
         d_current_week character(1),                                   
         d_current_month character(2),                                  
         d_current_quarter character(1),                                
         d_current_year character(1)                                    
 )                                                                      
 WITH (orientation=column, compression=low)                             
 DISTRIBUTE BY HASH(d_date_sk)                                                                                         
 PARTITION BY RANGE (d_year)                                            
 (                                                                      
          PARTITION p1 VALUES LESS THAN (1950) TABLESPACE pg_default,   
          PARTITION p2 VALUES LESS THAN (2000) TABLESPACE pg_default,   
          PARTITION p3 VALUES LESS THAN (2050) TABLESPACE pg_default,   
          PARTITION p4 VALUES LESS THAN (2100) TABLESPACE pg_default,   
          PARTITION p5 VALUES LESS THAN (3000) TABLESPACE pg_default,   
          PARTITION p6 VALUES LESS THAN (MAXVALUE) TABLESPACE pg_default
 )                                                                      
 ENABLE ROW MOVEMENT;
 
  CREATE  TABLE store_sales (                                           
         ss_sold_date_sk integer,                                       
         ss_sold_time_sk integer,                                       
         ss_item_sk integer NOT NULL,                                   
         ss_customer_sk integer,                                        
         ss_cdemo_sk integer,                                           
         ss_hdemo_sk integer,                                           
         ss_addr_sk integer,                                            
         ss_store_sk integer,                                           
         ss_promo_sk integer,                                           
         ss_ticket_number bigint NOT NULL,                              
         ss_quantity integer,                                           
         ss_wholesale_cost numeric(7,2),                                
         ss_list_price numeric(7,2),                                    
         ss_sales_price numeric(7,2),                                   
         ss_ext_discount_amt numeric(7,2),                              
         ss_ext_sales_price numeric(7,2),                               
         ss_ext_wholesale_cost numeric(7,2),                            
         ss_ext_list_price numeric(7,2),                                
         ss_ext_tax numeric(7,2),                                       
         ss_coupon_amt numeric(7,2),                                    
         ss_net_paid numeric(7,2),                                      
         ss_net_paid_inc_tax numeric(7,2),                              
         ss_net_profit numeric(7,2)                                     
 )                                                                      
 WITH (orientation=column, compression=low)                             
 DISTRIBUTE BY HASH(ss_item_sk)                                                                                         
 PARTITION BY RANGE (ss_sold_date_sk)                                   
 (                                                                      
          PARTITION p1 VALUES LESS THAN (2451179) TABLESPACE pg_default,
          PARTITION p2 VALUES LESS THAN (2451544) TABLESPACE pg_default,
          PARTITION p3 VALUES LESS THAN (2451910) TABLESPACE pg_default,
          PARTITION p4 VALUES LESS THAN (2452275) TABLESPACE pg_default,
          PARTITION p5 VALUES LESS THAN (2452640) TABLESPACE pg_default,
          PARTITION p6 VALUES LESS THAN (2453005) TABLESPACE pg_default,
          PARTITION p7 VALUES LESS THAN (MAXVALUE) TABLESPACE pg_default
 )                                                                      
 ENABLE ROW MOVEMENT;

 create table col_update
(c1 serial not null, 
c2 varchar(100) not null, 
c3 date default '2018-06-14',  
c4 timestamp with time zone default '2018-06-14 12:30:30+8',  
c5 numeric(18,9) default -999999999.999999999,  
c6 text default 'comments',
partial cluster key(c1,c3))with(orientation=column, compression=high)distribute by hash(c2,c4)
partition by range(c1)
(partition col_on_01 start(-32768) end(0) every(200),
partition col_on_02 start(0) end(32767) every(500) );

merge into col_update t1
using (
select distinct date_dim.d_year,item.i_brand,
      sum(store_sales.ss_coupon_amt        )
from date_dim INNER JOIN (store_sales RIGHT JOIN item on store_sales.ss_item_sk           =item.i_item_sk        ) on date_dim.d_date_sk          =store_sales.ss_sold_date_sk
where date_dim.d_year             <2000
      and item.i_item_id         in (select i_item_id from item where i_category like '%a%')
group by date_dim.d_year             ,
      item.i_brand)t2
on extract(year from t1.c3)=t2.d_year
when/*matched part*/ matched then update set t1.c1=t2.i_brand*t2.sum
when not matched then insert values(t2.d_year,t2.i_brand);

-- clean up
DROP SCHEMA xc_mergeinto CASCADE;
