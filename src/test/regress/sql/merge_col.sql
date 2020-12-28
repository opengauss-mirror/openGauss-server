--
-- MERGE INTO 
--

-- initial
CREATE SCHEMA mergeinto_col;
SET current_schema = mergeinto_col;

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
-- stream mode(MERGE can be pushed down), column table
--

CREATE TABLE products_col
(
product_id INTEGER DEFAULT 0,
product_name VARCHAR(60) DEFAULT 'null',
category VARCHAR(60) DEFAULT 'unknown',
total INTEGER DEFAULT '0'
)
WITH (ORIENTATION=column);

CREATE TABLE newproducts_col
(
product_id INTEGER DEFAULT 0,
product_name VARCHAR(60) DEFAULT 'null',
category VARCHAR(60) DEFAULT 'unknown',
total INTEGER DEFAULT '0'
)
WITH (ORIENTATION=column);

INSERT INTO products_col SELECT * FROM products_base;
INSERT INTO newproducts_col SELECT * FROM newproducts_base;
ANALYZE products_col;
ANALYZE newproducts_col;

-- explain verbose
EXPLAIN (VERBOSE on, COSTS off)
MERGE INTO products_col p
USING newproducts_col np
ON p.product_id = np.product_id
WHEN MATCHED THEN
  UPDATE SET product_name = np.product_name, category = np.category, total = np.total
WHEN NOT MATCHED THEN  
  INSERT VALUES (np.product_id, np.product_name, np.category, np.total);

-- only MATCHED clause
MERGE INTO products_col p
USING newproducts_col np
ON p.product_id = np.product_id
WHEN MATCHED THEN
  UPDATE SET product_name = np.product_name, category = np.category, total = np.total;
SELECT * FROM products_col ORDER BY 1;

-- only MATCHED clause, has expressions
TRUNCATE products_col;
INSERT INTO products_col SELECT * FROM products_base;

MERGE INTO products_col p
USING newproducts_col np
ON p.product_id = np.product_id
WHEN MATCHED THEN
  UPDATE SET product_name = np.product_name, category = np.category || 'ABC', total = np.total + 100;
SELECT * FROM products_col ORDER BY 1;

-- only NOT MATCHED clause
TRUNCATE products_col;
INSERT INTO products_col SELECT * FROM products_base;

MERGE INTO products_col p
USING newproducts_col np
ON p.product_id = np.product_id
WHEN NOT MATCHED THEN  
  INSERT VALUES (np.product_id, np.product_name, np.category, np.total);
SELECT * FROM products_col ORDER BY 1;

TRUNCATE products_col;
INSERT INTO products_col SELECT * FROM products_base;

MERGE INTO products_col p
USING newproducts_col np
ON p.product_id = np.product_id
WHEN NOT MATCHED THEN  
  INSERT VALUES (np.product_id, np.product_name, np.category);
SELECT * FROM products_col ORDER BY 1;

-- only NOT MATCHED clause has insert targetlist
TRUNCATE products_col;
INSERT INTO products_col SELECT * FROM products_base;

MERGE INTO products_col p
USING newproducts_col np
ON p.product_id = np.product_id
WHEN NOT MATCHED THEN 
  INSERT (product_id, total) VALUES (np.product_id, np.total); --notice: we have 2 fields missing
SELECT * FROM products_col ORDER BY 1;

TRUNCATE products_col;
INSERT INTO products_col SELECT * FROM products_base;

MERGE INTO products_col p
USING newproducts_col np
ON p.product_id = np.product_id
WHEN NOT MATCHED THEN  
  INSERT (product_id, total) VALUES (np.product_id, np.total); --notice: we have 2 fields missing
SELECT * FROM products_col ORDER BY 1;

-- only NOT MATCHED clause has insert targetlist
TRUNCATE products_col;
INSERT INTO products_col SELECT * FROM products_base;

MERGE INTO products_col p
USING newproducts_col np
ON p.product_id = np.product_id
WHEN NOT MATCHED THEN 
  INSERT (total, product_id) VALUES (np.total, np.product_id); --notice: 2 fields missing and reversed
SELECT * FROM products_col ORDER BY 1;

TRUNCATE products_col;
INSERT INTO products_col SELECT * FROM products_base;

MERGE INTO products_col p
USING newproducts_col np
ON p.product_id = np.product_id
WHEN NOT MATCHED THEN  
  INSERT (total, product_id) VALUES (np.total, np.product_id); --notice: 2 fields missing and reversed
SELECT * FROM products_col ORDER BY 1;

-- only NOT MATCHED clause, has expressions
TRUNCATE products_col;
INSERT INTO products_col SELECT * FROM products_base;

MERGE INTO products_col p
USING newproducts_col np
ON p.product_id = np.product_id
WHEN NOT MATCHED THEN  
  INSERT VALUES (np.product_id, np.product_name, np.category || 'ABC', np.total + 100);
SELECT * FROM products_col ORDER BY 1;

-- both MATCHED and NOT MATCHED clause
TRUNCATE products_col;
INSERT INTO products_col SELECT * FROM products_base;

MERGE INTO products_col p
USING newproducts_col np
ON p.product_id = np.product_id
WHEN MATCHED THEN
  UPDATE SET product_name = np.product_name, category = np.category, total = np.total
WHEN NOT MATCHED THEN  
  INSERT VALUES (np.product_id, np.product_name, np.category, np.total);
SELECT * FROM products_col ORDER BY 1;

-- both MATCHED and NOT MATCHED clause has constant qual
TRUNCATE products_col;
INSERT INTO products_col SELECT * FROM products_base;

MERGE INTO products_col p
USING newproducts_col np
ON p.product_id = np.product_id AND 1=1
WHEN MATCHED THEN
  UPDATE SET product_name = np.product_name, category = np.category, total = np.total
WHEN NOT MATCHED THEN  
  INSERT VALUES (np.product_id, np.product_name, np.category, np.total);
SELECT * FROM products_col ORDER BY 1,2,3,4;

-- both MATCHED and NOT MATCHED clause has constant qual with subquery
TRUNCATE products_col;
INSERT INTO products_col SELECT * FROM products_base;

MERGE INTO products_col p
USING newproducts_col np
ON p.product_id = np.product_id AND 1=(select total from products_col order by 1 limit 1)
WHEN MATCHED THEN
  UPDATE SET product_name = np.product_name, category = np.category, total = np.total
WHEN NOT MATCHED THEN  
  INSERT VALUES (np.product_id, np.product_name, np.category, np.total);
SELECT * FROM products_col ORDER BY 1,2,3,4;

-- both MATCHED and NOT MATCHED clause, has expressions
TRUNCATE products_col;
INSERT INTO products_col SELECT * FROM products_base;

MERGE INTO products_col p
USING newproducts_col np
ON p.product_id = np.product_id
WHEN MATCHED THEN
  UPDATE SET product_name = np.product_name, category = np.category || 'ABC', total = np.total + 100
WHEN NOT MATCHED THEN  
  INSERT VALUES (np.product_id, np.product_name, np.category || 'ABC', np.total + 100);
SELECT * FROM products_col ORDER BY 1;

MERGE INTO products_col p
USING newproducts_col np
ON p.product_id = np.product_id
WHEN MATCHED THEN
  UPDATE SET product_name = p.product_name, category = p.category || 'ABC', total = p.total + 100
WHEN NOT MATCHED THEN  
  INSERT VALUES (np.product_id, np.product_name, np.category || 'ABC', np.total + 100);
SELECT * FROM products_col ORDER BY 1;

-- both MATCHED and NOT MATCHED clause, has expressions, has WHERE conditions
TRUNCATE products_col;
INSERT INTO products_col SELECT * FROM products_base;

MERGE INTO products_col p
USING newproducts_col np
ON p.product_id = np.product_id
WHEN MATCHED THEN
  UPDATE SET product_name = np.product_name, category = np.category || 'ABC', total = np.total + 100 WHERE p.category = 'dvd'
WHEN NOT MATCHED THEN  
  INSERT VALUES (np.product_id, np.product_name, np.category || 'ABC', np.total + 100) WHERE np.category != 'books';
SELECT * FROM products_col ORDER BY 1;

-- both MATCHED and NOT MATCHED clause, has expressions, which WHERE conditions is constant
TRUNCATE products_col;
INSERT INTO products_col SELECT * FROM products_base;

MERGE INTO products_col p
USING newproducts_col np
ON p.product_id = np.product_id
WHEN MATCHED THEN
  UPDATE SET product_name = np.product_name, category = np.category || 'ABC', total = np.total + 100 WHERE true
WHEN NOT MATCHED THEN  
  INSERT VALUES (np.product_id, np.product_name, np.category || 'ABC', np.total + 100) WHERE false;
SELECT * FROM products_col ORDER BY 1;

-- both MATCHED and NOT MATCHED clause, has expressions, has WHERE conditions
TRUNCATE products_col;
INSERT INTO products_col SELECT * FROM products_base;

MERGE INTO products_col p
USING newproducts_col np
ON p.product_id = np.product_id
WHEN MATCHED THEN
  UPDATE SET product_name = np.product_name, category = np.category || 'ABC', total = np.total + 100
  WHERE p.category = 'dvd' AND np.category = 'toys'
WHEN NOT MATCHED THEN  
  INSERT VALUES (np.product_id, np.product_name, np.category || 'DEF', np.total + 100)
  WHERE np.category != 'books' AND np.total > 100;

SELECT * FROM products_col ORDER BY 1;

-- partitioned table
CREATE TABLE products_part_col
(
product_id INTEGER,
product_name VARCHAR2(60),
category VARCHAR2(60),
total INTEGER
)
WITH (ORIENTATION=column)
PARTITION BY RANGE (product_id)
(
  PARTITION P1 VALUES LESS THAN (1600),
  PARTITION P2 VALUES LESS THAN (1700),
  PARTITION P3 VALUES LESS THAN (1800)
) ENABLE ROW MOVEMENT;

INSERT INTO products_part_col SELECT * FROM products_base;
ANALYZE products_part_col;

MERGE INTO products_part_col p
USING newproducts_col np
ON p.product_id = np.product_id
WHEN MATCHED THEN
  UPDATE SET product_name = np.product_name, category = np.category || 'ABC', total = np.total + 100 WHERE p.category = 'dvd'
WHEN NOT MATCHED THEN  
  INSERT VALUES (np.product_id, np.product_name, np.category || 'ABC', np.total + 100) WHERE np.category != 'books';
SELECT * FROM products_part_col ORDER BY 1;


-- do a simple equivalent of an INSERT SELECT
TRUNCATE products_col;
INSERT INTO products_col SELECT * FROM products_base;

SELECT * FROM products_col ORDER BY 1;
SELECT * FROM newproducts_col ORDER BY 1;

MERGE INTO products_col p
USING newproducts_col np
ON p.product_id = np.product_id
WHEN NOT MATCHED THEN
    INSERT VALUES (np.product_id, np.product_name, np.category, np.total);

SELECT * FROM products_col ORDER BY 1;


-- the classic merge
TRUNCATE products_col;
INSERT INTO products_col SELECT * FROM products_base;

SELECT * FROM products_col ORDER BY 1;
SELECT * FROM newproducts_col ORDER BY 1;

MERGE INTO products_col p
USING newproducts_col np
ON p.product_id = np.product_id
WHEN NOT MATCHED THEN
    INSERT VALUES (np.product_id, np.product_name, np.category, np.total)
WHEN MATCHED THEN
    UPDATE SET product_name = p.product_name ||' + '|| np.product_name,
	           category     = p.category ||' + ' || np.category,
			   total        = p.total + np.total;

SELECT * FROM products_col ORDER BY 1;

-- do a simple equivalent of an INSERT SELECT with targetlist
TRUNCATE products_col;
INSERT INTO products_col SELECT * FROM products_base;

SELECT * FROM products_col ORDER BY 1;
SELECT * FROM newproducts_col ORDER BY 1;

MERGE INTO products_col p
USING newproducts_col np
ON p.product_id = np.product_id
WHEN NOT MATCHED THEN
    INSERT (product_id, product_name, total) 
	VALUES (np.product_id, np.product_name, np.total);

SELECT * FROM products_col ORDER BY 1;


-- the on clause is true
TRUNCATE products_col;
INSERT INTO products_col SELECT * FROM products_base;

SELECT * FROM products_col ORDER BY 1;
SELECT * FROM newproducts_col ORDER BY 1;

MERGE INTO products_col p
USING newproducts_col np
ON (select true)
WHEN NOT MATCHED THEN
    INSERT (product_id, product_name, total) 
	VALUES (np.product_id, np.product_name, np.total);

SELECT * FROM products_col ORDER BY 1;

--subquery
TRUNCATE products_col;
INSERT INTO products_col SELECT * FROM products_base;

MERGE INTO products_col p
USING (select * from newproducts_col ) np
ON p.product_id = np.product_id
WHEN MATCHED THEN 
	UPDATE SET product_name = p.product_name, category = p.category || 'ABC', total = p.total + 100
WHEN NOT MATCHED THEN
	INSERT VALUES (np.product_id, np.product_name, np.category || 'DEF', np.total + 200);

SELECT * FROM products_col ORDER BY 1;

--subquery with expression
TRUNCATE products_col;
INSERT INTO products_col SELECT * FROM products_base;

MERGE INTO products_col p
USING (select * from newproducts_col ) np
ON p.product_id = np.product_id
WHEN MATCHED THEN 
	UPDATE SET product_name = p.product_name, category = p.category || 'ABC', total = p.total + 100 WHERE np.product_name ='lamaze'
WHEN NOT MATCHED THEN
	INSERT VALUES (np.product_id, np.product_name, np.category || 'DEF', np.total + 200) WHERE np.product_name = 'wait interface';

SELECT * FROM products_col order by 1;

--subquery with expression
TRUNCATE products_col;
INSERT INTO products_col SELECT * FROM products_base;

MERGE INTO products_col p
USING (select sp.product_id, sp.product_name, snp.category, snp.total from newproducts_col snp, products_col sp where sp.product_id = snp.product_id) np
ON p.product_id = np.product_id
WHEN MATCHED THEN 
	UPDATE SET product_name = p.product_name, category = p.category || 'ABC', total = p.total + 100 WHERE product_name ='lamaze'
WHEN NOT MATCHED THEN
	INSERT VALUES (np.product_id, np.product_name, np.category || 'DEF', np.total + 200) WHERE np.product_name = 'wait interface';

SELECT * FROM products_col order by 1;


--subquery has constant
TRUNCATE products_col;
INSERT INTO products_col SELECT * FROM products_base;

MERGE INTO products_col p
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

SELECT * FROM products_col ORDER BY 1;


--subquery has aggeration
TRUNCATE products_col;
INSERT INTO products_col SELECT * FROM products_base;

MERGE INTO products_col p
USING (
select product_id, product_name, category, sum(total) as total from newproducts_col group by product_id, product_name, category
 ) np
ON p.product_id = np.product_id
WHEN MATCHED THEN 
	UPDATE SET product_name = p.product_name, category = p.category || 'ABC', total = p.total + 100
WHEN NOT MATCHED THEN
	INSERT VALUES (np.product_id, np.product_name, np.category || 'DEF', np.total + 200);

SELECT * FROM products_col ORDER BY 1;

--subquery has aggeration
TRUNCATE products_col;
INSERT INTO products_col SELECT * FROM products_base;

MERGE INTO products_col p
USING (
select product_id, product_name, category, sum(total) as total
from newproducts_col
group by product_id, product_name, category
having sum(total)>100
order by total
 ) np
ON p.product_id = np.product_id
WHEN MATCHED THEN 
	UPDATE SET product_name = p.product_name, category = p.category || 'ABC', total = p.total + 100
WHEN NOT MATCHED THEN
	INSERT VALUES (np.product_id, np.product_name, np.category || 'DEF', np.total + 200);

SELECT * FROM products_col ORDER BY 1;


--source table have a view
TRUNCATE products_col;
INSERT INTO products_col SELECT * FROM products_base;
CREATE VIEW v AS (SELECT * FROM newproducts_col WHERE total > 100);

MERGE INTO products_col p
USING v np
ON p.product_id = np.product_id
WHEN MATCHED THEN 
	UPDATE SET product_name = p.product_name, category = p.category || 'ABC', total = p.total + 100
WHEN NOT MATCHED THEN
	INSERT VALUES (np.product_id, np.product_name, np.category || 'DEF', np.total + 200);

SELECT * FROM products_col ORDER BY 1;

--self merge
TRUNCATE products_col;
INSERT INTO products_col SELECT * FROM products_base;

BEGIN;

MERGE INTO products_col p
USING products_col np
ON p.product_id = np.product_id
WHEN MATCHED THEN 
	UPDATE SET product_name = p.product_name, category = p.category || 'ABC', total = p.total + 100
WHEN NOT MATCHED THEN
	INSERT VALUES (np.product_id, np.product_name, np.category || 'DEF', np.total + 200);

SELECT * FROM products_col ORDER BY 1;

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

MERGE INTO products_col p
USING newproducts_col np
ON p.product_id = np.product_id
WHEN NOT MATCHED THEN
  INSERT VALUES (DEFAULT);

MERGE INTO products_col p
USING newproducts_col np
ON p.product_id = np.product_id
WHEN NOT MATCHED THEN
  INSERT VALUES (DEFAULT, np.product_name, np.category, DEFAULT);

MERGE INTO products_col p
USING newproducts_col np
ON p.product_id = np.product_id
WHEN NOT MATCHED THEN
  INSERT DEFAULT VALUES;

select * from  products_col order by 1,2,3,4;
ROLLBACK;

--default values for update values
BEGIN;

MERGE INTO products_col p
USING newproducts_col np
ON p.product_id = np.product_id
WHEN MATCHED THEN
  UPDATE SET product_name = DEFAULT, category = DEFAULT, total = DEFAULT 
WHEN NOT MATCHED THEN
  INSERT VALUES (DEFAULT);

select * from  products_col order by 1;  
ROLLBACK;


--plpgsql

BEGIN;
DO LANGUAGE plpgsql $$
BEGIN
MERGE INTO products_col p
USING newproducts_col np
ON p.product_id = np.product_id
WHEN MATCHED THEN
  UPDATE SET product_name = np.product_name, category = np.category , total = np.total
WHEN NOT MATCHED THEN
  INSERT VALUES (np.product_id, np.product_name, np.category, np.total);
END;
$$;
SELECT * FROM products_col order by 1;
ROLLBACK;

--NON ANOYBLOCK 
BEGIN;

BEGIN
MERGE INTO products_col p
USING newproducts_col np
ON p.product_id = np.product_id
WHEN MATCHED THEN
  UPDATE SET product_name = np.product_name, category = np.category , total = np.total
WHEN NOT MATCHED THEN
  INSERT VALUES (np.product_id, np.product_name, np.category, np.total);
END;
/

SELECT * FROM products_col order by 1;

ROLLBACK;


--stored procedure
create or replace procedure p1()
AS
BEGIN
	MERGE INTO products_col p
	USING newproducts_col np
	ON p.product_id = np.product_id
	WHEN MATCHED THEN
	  UPDATE SET product_name = np.product_name, category = np.category , total = np.total
	WHEN NOT MATCHED THEN
	  INSERT VALUES (np.product_id, np.product_name, np.category, np.total);
END;
/

BEGIN;
select p1();
SELECT * FROM products_col order by 1;
ROLLBACK;

--stored procedure with params
create or replace procedure p2( param1 IN text)
AS
BEGIN
	MERGE INTO products_col p
	USING newproducts_col np
	ON p.product_id = np.product_id
	WHEN MATCHED THEN
	  UPDATE SET product_name = np.product_name, category = np.category , total = np.total
	WHEN NOT MATCHED THEN
	  INSERT VALUES (np.product_id, 'inserted by proc', param1, np.total);
END;
/

BEGIN;
select p2('param1');
SELECT * FROM products_col order by 1;
ROLLBACK;

BEGIN;
select p2('param2');
SELECT * FROM products_col order by 1;
ROLLBACK;


--stored procedure with params in update targetlist
create or replace procedure p3( param1 IN text)
AS
BEGIN
	MERGE INTO products_col p
	USING newproducts_col np
	ON p.product_id = np.product_id
	WHEN MATCHED THEN
	  UPDATE SET product_name = np.product_name, category = param1 , total = np.total
	WHEN NOT MATCHED THEN
	  INSERT VALUES (np.product_id, 'inserted by proc', np.category, np.total);
END;
/

BEGIN;
select p3('param1');
SELECT * FROM products_col order by 1;
ROLLBACK;

BEGIN;
select p3('param2');
SELECT * FROM products_col order by 1;
ROLLBACK;


--stored procedure with params in where conditions
create or replace procedure p4( param1 IN text)
AS
BEGIN
	MERGE INTO products_col p
	USING newproducts_col np
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
SELECT * FROM products_col order by 1;
ROLLBACK;

BEGIN;
select p4('dvd'); --only update dvd
SELECT * FROM products_col order by 1;
ROLLBACK;

BEGIN;
select p4('electrncs'); --only update electrncs
SELECT * FROM products_col order by 1;
ROLLBACK;


--update index
create table src(a bigint, b bigint, c varchar(1000)) WITH (ORIENTATION=column);
create table des(a bigint, b bigint, c varchar(1000)) WITH (ORIENTATION=column);
create index src_i on src(b);
create index des_i on des(b);

insert into des values(generate_series(1,10), generate_series(1,10), 'des');
insert into src values(generate_series(1,10), generate_series(1,10), 'src');

insert into src (select a+100, b+100, c from src);
insert into src (select a+1000, b+1000, c from src);
insert into src (select a+10000, b+10000, c from src);
insert into src (select a+100000, b+100000, c from src);
insert into src (select a+1000000, b+1000000, c from src);
insert into src (select a+10000000, b+10000000, c from src);
insert into src (select a+100000000, b+100000000, c from src);
insert into src (select a+1000000000, b+100000000, c from src);
insert into src (select a+10000000000, b+100000000, c from src);

insert into des (select a+100, b+100, c from des);
insert into des (select a+1000, b+1000, c from des);
insert into des (select a+10000, b+10000, c from des);
insert into des (select a+100000, b+100000, c from des);
insert into des (select a+1000000, b+1000000, c from des);
insert into des (select a+10000000, b+10000000, c from des);
insert into des (select a+100000000, b+100000000, c from des);
insert into des (select a+1000000000, b+1000000000, c from des);
insert into des (select a+10000000000, b+1000000000, c from des);

MERGE INTO des d
USING src s
ON d.a = s.a
WHEN MATCHED THEN
  UPDATE SET b = s.b + 10, c = s.c
WHEN NOT MATCHED THEN
  INSERT VALUES (s.a, s.b);

--rows shall be the same
select count(*) from src;
select count(*) from des;

--column b of des is 10 bigger than src
select * from src where a = 105;
select * from des where b = 115;

drop table des;
drop table src;

--dropped column and add column
BEGIN;
ALTER TABLE products_col DROP COLUMN category;

MERGE INTO products_col p
USING newproducts_col np
ON p.product_id = np.product_id
WHEN NOT MATCHED THEN
    INSERT VALUES (np.product_id, np.product_name, np.total)
WHEN MATCHED THEN
    UPDATE SET product_name = p.product_name ||' + '|| np.product_name,
			   total        = p.total + np.total;

SELECT * FROM products_col ORDER BY 1;

TRUNCATE products_col;
ALTER TABLE products_col ADD COLUMN category VARCHAR;
INSERT INTO products_col SELECT product_id,product_name,total,category FROM products_base;

MERGE INTO products_col p
USING newproducts_col np
ON p.product_id = np.product_id
WHEN NOT MATCHED THEN
    INSERT VALUES (np.product_id, np.product_name, np.total)
WHEN MATCHED THEN
    UPDATE SET product_name = p.product_name ||' + '|| np.product_name,
			   total        = p.total + np.total;

SELECT * FROM products_col ORDER BY 1;
ROLLBACK;

--join key diffs from distribute key
BEGIN;
MERGE INTO products_col p
USING newproducts_col np
ON p.total = np.product_id
WHEN NOT MATCHED THEN
    INSERT VALUES (np.product_id, np.product_name, np.category, np.total);

SELECT * FROM products_col ORDER BY 1,2,3,4;
ROLLBACK;

BEGIN;
MERGE INTO products_col p
USING newproducts_col np
ON p.total = np.product_id
WHEN MATCHED THEN
    UPDATE SET product_name = p.product_name ||' + '|| np.product_name;

SELECT * FROM products_col ORDER BY 1;
ROLLBACK;

BEGIN;
MERGE INTO products_col p
USING newproducts_col np
ON p.total = np.product_id
WHEN NOT MATCHED THEN
    INSERT VALUES (np.product_id, np.product_name, np.category, np.total)
WHEN MATCHED THEN
    UPDATE SET product_name = p.product_name ||' + '|| np.product_name;

SELECT * FROM products_col ORDER BY 1,2,3,4;
ROLLBACK;

BEGIN;
MERGE INTO products_col p
USING newproducts_col np
ON p.total = np.total
WHEN NOT MATCHED THEN
    INSERT VALUES (np.product_id, np.product_name, np.category, np.total);

SELECT * FROM products_col ORDER BY 1,2,3,4;
ROLLBACK;

BEGIN;
MERGE INTO products_col p
USING newproducts_col np
ON p.total = np.total
WHEN MATCHED THEN
    UPDATE SET product_name = p.product_name ||' + '|| np.product_name;

SELECT * FROM products_col ORDER BY 1;
ROLLBACK;

BEGIN;
MERGE INTO products_col p
USING newproducts_col np
ON p.total = np.total
WHEN NOT MATCHED THEN
    INSERT VALUES (np.product_id, np.product_name, np.category, np.total)
WHEN MATCHED THEN
    UPDATE SET product_name = p.product_name ||' + '|| np.product_name;

SELECT * FROM products_col ORDER BY 1,2,3,4;
ROLLBACK;

--target table distributed by multiple columns, and then test dropped column and added column
CREATE TABLE products_col_multi
(
product_id INTEGER,
product_name VARCHAR(60),
category VARCHAR(60),
total INTEGER
)
WITH (ORIENTATION=column)
DISTRIBUTE BY HASH(product_id, category);

INSERT INTO products_col_multi SELECT * FROM products_base;
ANALYZE products_col_multi;

BEGIN;
MERGE INTO products_col_multi p
USING newproducts_col np
ON p.product_id = np.product_id
WHEN MATCHED THEN
  UPDATE SET product_name = p.product_name || 'ABC', total = p.total + 100
WHEN NOT MATCHED THEN  
  INSERT VALUES (np.product_id, np.product_name, np.category || 'ABC', np.total + 100);

SELECT * FROM products_col_multi ORDER BY 1;
ROLLBACK;

BEGIN;
UPDATE products_col_multi SET total = 100; --generate some dead rows
MERGE INTO products_col_multi p
USING newproducts_col np
ON p.product_id = np.product_id
WHEN MATCHED THEN
  UPDATE SET product_name = p.product_name || 'ABC', total = p.total + 100
WHEN NOT MATCHED THEN  
  INSERT VALUES (np.product_id, np.product_name, np.category || 'ABC', np.total + 100);

SELECT * FROM products_col_multi ORDER BY 1;
ROLLBACK;

ALTER TABLE products_col_multi DROP COLUMN product_name; --dropped column
BEGIN;
MERGE INTO products_col_multi p
USING newproducts_col np
ON p.product_id = np.product_id
WHEN MATCHED THEN
  UPDATE SET total = p.total + 100
WHEN NOT MATCHED THEN  
  INSERT VALUES (np.product_id, np.category || 'ABC', np.total + 100);

SELECT * FROM products_col_multi ORDER BY 1;
ROLLBACK;

ALTER TABLE products_col_multi ADD COLUMN product_name VARCHAR;  --added column
TRUNCATE products_col_multi;
INSERT INTO products_col_multi SELECT product_id,category,total,product_name FROM products_base;

BEGIN;
MERGE INTO products_col_multi p
USING newproducts_col np
ON p.product_id = np.product_id
WHEN MATCHED THEN
  UPDATE SET product_name = p.product_name || 'ABC', total = p.total + 100;

SELECT * FROM products_col_multi ORDER BY 1;
ROLLBACK;

BEGIN;
MERGE INTO products_col_multi p
USING newproducts_col np
ON p.product_id = np.product_id
WHEN NOT MATCHED THEN  
  INSERT VALUES (np.product_id, np.category || 'ABC', np.total + 100, np.product_name);

SELECT * FROM products_col_multi ORDER BY 1;
ROLLBACK;

BEGIN;
MERGE INTO products_col_multi p
USING newproducts_col np
ON p.product_id = np.product_id
WHEN MATCHED THEN
  UPDATE SET product_name = p.product_name || 'ABC', total = p.total + 100
WHEN NOT MATCHED THEN  
  INSERT VALUES (np.product_id, np.category || 'ABC', np.total + 100, np.product_name);

SELECT * FROM products_col_multi ORDER BY 1;
ROLLBACK;

-- clean up
DROP SCHEMA mergeinto_col CASCADE;
