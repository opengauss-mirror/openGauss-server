--
-- MERGE INTO: test various restrictions of merge into 
--

-- initial
CREATE SCHEMA mergeinto_restriction;
SET current_schema = mergeinto_restriction;

CREATE TABLE products_base
(
product_id INTEGER,
product_name VARCHAR2(60),
category VARCHAR2(60),
total INTEGER
);

INSERT INTO products_base VALUES (1501, 'vivitar 35mm', 'electrncs', 100);
INSERT INTO products_base VALUES (1502, 'olympus is50', 'electrncs', 100);
INSERT INTO products_base VALUES (1600, 'play gym', 'toys', 100);
INSERT INTO products_base VALUES (1601, 'lamaze', 'toys', 100);
INSERT INTO products_base VALUES (1666, 'harry potter', 'dvd', 100);


CREATE TABLE newproducts_base
(
product_id INTEGER,
product_name VARCHAR2(60),
category VARCHAR2(60),
total INTEGER
);

INSERT INTO newproducts_base VALUES (1502, 'olympus camera', 'electrncs', 200);
INSERT INTO newproducts_base VALUES (1601, 'lamaze', 'toys', 200);
INSERT INTO newproducts_base VALUES (1666, 'harry potter', 'toys', 200);
INSERT INTO newproducts_base VALUES (1700, 'wait interface', 'books', 200);

ANALYZE products_base;
ANALYZE newproducts_base;


CREATE TABLE products_row
(
product_id INTEGER,
product_name VARCHAR2(60),
category VARCHAR2(60),
total INTEGER
);

CREATE TABLE newproducts_row
(
product_id INTEGER,
product_name VARCHAR2(60),
category VARCHAR2(60),
total INTEGER
);

INSERT INTO products_row SELECT * FROM products_base;
INSERT INTO newproducts_row SELECT * FROM newproducts_base;
ANALYZE products_row;
ANALYZE newproducts_row;

--error: system catalog cannot support for merge into
MERGE INTO pg_class p
USING pg_class  np
ON p.oid = np.oid
WHEN MATCHED THEN
  UPDATE SET p.relname = 'dummy';

--error: on clause refence system column
MERGE INTO products_row p
USING newproducts_row np
ON p.xc_node_id = np.xc_node_id
WHEN MATCHED THEN
  UPDATE SET p.total = 1;

--error: where clause refence system column
MERGE INTO products_row p
USING newproducts_row np
ON p.product_id = np.product_id
WHEN MATCHED THEN
  UPDATE SET p.total = 1 WHERE p.xc_node_id=1;

--error: try to update a system column
MERGE INTO products_row p
USING newproducts_row np
ON p.product_id = np.product_id
WHEN MATCHED THEN
  UPDATE SET p.xc_node_id = 1;

--error: try to use system column as lvalue 
MERGE INTO products_row p
USING newproducts_row np
ON p.product_id = np.product_id
WHEN NOT MATCHED THEN
	INSERT (p.xc_node_id) VALUES(1);

--error: try to use system column as rvalue 
MERGE INTO products_row p
USING newproducts_row np
ON p.product_id = np.product_id
WHEN NOT MATCHED THEN
	INSERT VALUES (np.xc_node_id);

--error: missing actions
MERGE INTO products_row p
USING newproducts_row np
ON p.product_id = np.product_id;

--error: update distribute key
MERGE INTO products_row p
USING newproducts_row np
ON p.product_id = np.product_id
WHEN MATCHED THEN
  UPDATE SET p.product_id = 1;

--error: update ON caluse
MERGE INTO products_row p
USING newproducts_row np
ON p.product_name = np.product_name
WHEN MATCHED THEN
  UPDATE SET p.product_name = 1;

--error: insert cannot reference to target rel
MERGE INTO products_row p
USING newproducts_row np
ON p.product_name = np.product_name
WHEN NOT MATCHED THEN
  INSERT VALUES (p.product_id, p.product_name, p.category, p.total);

MERGE INTO products_row p
USING newproducts_row np
ON p.product_name = np.product_name
WHEN NOT MATCHED THEN
  INSERT VALUES (np.product_id, np.product_name, np.category, np.total) WHERE p.total > 100;

--error: cannot insert twice
MERGE INTO products_row p
USING newproducts_row np
ON p.product_name = np.product_name
WHEN NOT MATCHED THEN
  INSERT VALUES  (np.product_id, np.product_name, np.category, np.total) 
WHEN NOT MATCHED THEN
  INSERT VALUES  (np.product_id, np.product_name, np.category, np.total);

--error: cannot update twice
MERGE INTO products_row p
USING (select product_id,product_name,  category, sum(total) as total from newproducts_row  group by product_id,product_name,  category ) np
ON p.product_id = np.product_id
WHEN MATCHED THEN
  UPDATE SET product_name = np.product_name, category = np.category, total = np.total
WHEN MATCHED THEN
  UPDATE SET product_name = np.product_name, category = np.category, total = np.total;
 
--error: cannot insert/update twice
MERGE INTO products_row p
USING (select product_id,product_name,  category, sum(total) as total from newproducts_row  group by product_id,product_name,  category ) np
ON p.product_id = np.product_id
WHEN MATCHED THEN
  UPDATE SET product_name = np.product_name, category = np.category, total = np.total
WHEN MATCHED THEN
  UPDATE SET product_name = np.product_name, category = np.category, total = np.total
WHEN NOT MATCHED THEN
  INSERT VALUES  (np.product_id, np.product_name, np.category, np.total) 
WHEN NOT MATCHED THEN
  INSERT VALUES  (np.product_id, np.product_name, np.category, np.total);

--error: subquery in qual
MERGE INTO products_row p
USING newproducts_row np
ON p.product_id = np.product_id
WHEN MATCHED THEN
  UPDATE SET product_name = np.product_name, category = np.category , total = np.total
  WHERE product_id = (select 1); 

--error: subquery in insert
MERGE INTO products_row p
USING newproducts_row np
ON p.product_id = np.product_id
WHEN NOT MATCHED THEN
  INSERT (total) VALUES ((select 1));

--error: insert cannot refer to target columns
MERGE INTO products_row p
USING newproducts_row np
ON p.product_id = np.product_id
WHEN NOT MATCHED THEN
  INSERT VALUES (product_id, product_name, category, total);

--error: subquery in update  
MERGE INTO products_row p
USING newproducts_row np
ON p.product_id = np.product_id
WHEN MATCHED THEN
  UPDATE SET  total = (select 1); 

--error: replicate table
create table rep( a int) distribute by replication;
merge into rep a using rep b on a.a=b.a when not  matched then insert values(default);
drop table rep;

--error: PBE
PREPARE prepared_merge_into
AS
MERGE INTO products_row p
	USING newproducts_row np
	ON p.product_id = np.product_id
WHEN MATCHED THEN
	  UPDATE SET product_name = np.product_name, category = np.category , total = np.total;

-- test stream_walker, fall back to xc plan
EXPLAIN (VERBOSE ON, COSTS OFF)
MERGE INTO products_row p
USING newproducts_row np
ON p.product_id = np.product_id
WHEN MATCHED THEN
UPDATE SET product_name = np.product_name, category = np.category || pg_backend_pid()::text, total = np.total + 100 WHERE p.category = 'dvd';

EXPLAIN (VERBOSE ON, COSTS OFF)
MERGE INTO products_row p
USING newproducts_row np
ON p.product_id = np.product_id
WHEN MATCHED THEN
UPDATE SET product_name = np.product_name, category = np.category || 'ABC', total = np.total + 100 WHERE p.category = pg_backend_pid()::text;


-- clean up
DROP SCHEMA mergeinto_restriction CASCADE;
