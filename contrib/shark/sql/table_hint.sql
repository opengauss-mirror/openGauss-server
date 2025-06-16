create schema test_table_hint;
set current_schema to test_table_hint;

-- select
create table test_hint(id int);
select * from test_hint;
select * from test_hint t;
select * from test_hint t (nolock);
select * from test_hint t (readuncommitted);
select * from test_hint t (updlock);
select * from test_hint t (repeatableread);
select * from test_hint t (serializable);
select * from test_hint t (readcommitted);
select * from test_hint t (tablock);
select * from test_hint t (tablockx);
select * from test_hint t (paglock);
select * from test_hint t (rowlock);
select * from test_hint t (nowait);
select * from test_hint t (readpast);
select * from test_hint t (xlock);
select * from test_hint t (snapshot);
select * from test_hint t (noexpand);
select * from test_hint t (error_hint); -- error hint, result same as A database
select * from test_hint (nolock); --ok, no alias
select * from test_hint (readuncommitted);
select * from test_hint (updlock);
select * from test_hint (repeatableread);
select * from test_hint (serializable);
select * from test_hint (readcommitted);
select * from test_hint (tablock);
select * from test_hint (tablockx);
select * from test_hint (paglock);
select * from test_hint (rowlock);
select * from test_hint (nowait);
select * from test_hint (readpast);
select * from test_hint (xlock);
select * from test_hint (snapshot);
select * from test_hint (noexpand);
select * from test_hint(xlock); --ok
select * from test_hint t(noexpand); --ok
select * from test_hint (error_hint); -- error hint, result same as A database
select * from test_hint (error_hint) t; -- error hint, result same as A database
select * from test_hint (nolock) t; --error, hint should after alias 
select * from test_hint t with(nolock); --ok
select * from test_hint t with (nolock); --ok
select * from test_hint t with ( nolock); --ok
select * from test_hint t with (nolock ); --ok
select * from test_hint t with ( nolock ); --ok
select * from test_hint t with (nolock, nowait);
select * from test_hint t with (nolock, nowait, noexpand);
select * from test_hint t with (nolock, nowait, noexpand);
select * from test_hint t with (nolock, nowait, noexpand) where id = 10;
select * from test_hint t with (nolock, nowait, noexpand) order by id;
select * from test_hint t with (updlock) where id is not null;
select * from test_hint t with (nolock,nowait); --ok
select * from test_hint t with (nolock. nowait); --error, delimiter
select * from test_hint t with (nolock; nowait); --error, delimiter
select * from test_hint t with (nolock, nowait, error_hint); --error hint
select * from test_hint t (nolock, nowait); --error, no with
select * from test_hint t nolock, nowait; --error, no with and parenthesis
select * from test_hint t with nolock, nowait; --error, no parenthesis
select * from test_hint t with nolock; --error, no parenthesis
select * from test_hint t nolock; --error, no parenthesis
select * from test_hint with(nolock nowait); --d database ok, og ok, table hint can be splited by space
drop table test_hint;

-- join
create table t1(col1 int, col2 int, col3 int, col4 int, col5 int);
create table t2(col1 int, col2 int, col3 int, col4 int, col5 int);
create table t3(col1 int, col2 int, col3 int, col4 int, col5 int);
create table t4(col1 int, col2 int, col3 int, col4 int, col5 int);
create table t5(col1 int, col2 int, col3 int, col4 int, col5 int);

select * from t1 a (nolock) left join t2 b (nolock) on a.col1 = b.col1; --ok
select * from t1 (nolock) left join t2 (nolock) on t1.col1 = t2.col1; --ok

select * from t1 a inner join t2 b (nolock) on a.col1 = b.col1 inner join t3 c (nolock) on b.col2 = c.col2 
inner join t4 d (nolock) on c.col3 = d.col3 left join t5 e (nolock) on a.col4 = e.col4; --ok

select * from t1 a  inner join t2 b (nolock) on a.col1 = b.col1 left outer join t3 c (nolock) on b.col2 = c.col2 
right join t4 d (nolock) on c.col3 = d.col3 left join t5 e (nolock) on a.col4 = e.col4; --ok

select * from t1 a  inner join t2 b (nolock) on a.col1 = b.col1 join t3 c (nolock) on b.col2 = c.col2 
cross join t4 d (nolock) left join t5 e (nolock) on a.col4 = e.col4; --ok

select * from t1 a  asof join t2 b (nolock) on a.col1 = b.col1; --d database error because asof

select * from t1 a  natural join t3 c (nolock) cross apply t4 d (nolock) outer apply t5 e (nolock); --d database error because natural

select * from t1 a full join t5 e (nolock) on a.col4 = e.col4; --ok

select * from t1 a full join t5 e (nolock) on a.col4 = e.col4 where a.col1 > 10 and e.col4 < 100; --ok

drop table t1;
drop table t2;
drop table t3;
drop table t4;
drop table t5;

-- select into
create table t6(col1 int, col2 int, col3 int, col4 int, col5 int);
create table t7(c1 int, c2 int, c3 int, c4 int, c5 int);
select * into test6_copy1 from t6 with (nolock, nowait);
select * into test6_copy2 from t6 with (nolock, nowait) where col1 >10;
select * into table test6_copy3 from t6 with (nolock, nowait) where col1 >10; --d database error
select * into test6_copy31 from t6 with (nolock, nowait) where col1 >10; --d database ok
select col1, col2 into table test6_copy4 from t6 with (nolock, nowait) where col1 >10; --d database error
select col1, col2 into test6_copy41 from t6 with (nolock, nowait) where col1 >10; --d database ok
select * into test6_copy5 from t6 with (nolock, nowait) cross join t7 with (nolock,nowait);
select * into test6_copy6 from t6 with (nolock, nowait) cross join t7 with (nolock, nowait); 
select * into test6_copy7 with (nolock, nowait) from t6 with (nolock, nowait) cross join t7 with (nolock, nowait); --error

drop table t6;
drop table t7;

-- with cte
CREATE TABLE employees (
id SERIAL PRIMARY KEY,
name TEXT,
department TEXT,
salary INT
);

INSERT INTO employees (name, department, salary) VALUES
('Alice', 'HR', 6000),
('Bob', 'IT', 8000),
('Charlie', 'IT', 7500),
('David', 'Sales', 5500);

WITH department_avg AS (
SELECT department, AVG(salary) AS avg_salary
FROM employees
GROUP BY department
) select * from department_avg;

WITH department_avg AS (
SELECT department, AVG(salary) AS avg_salary
FROM employees with (nowait)
GROUP BY department
) select * from department_avg with (nowait);

-- partition table
CREATE TABLE partition_table1
(
    WR_RETURNED_DATE_SK       INTEGER,
    WR_RETURNED_TIME_SK       INTEGER
)
PARTITION BY RANGE(WR_RETURNED_DATE_SK)
(
        PARTITION P1 VALUES LESS THAN(2450815),
        PARTITION P2 VALUES LESS THAN(2451179),
        PARTITION P3 VALUES LESS THAN(2451544),
        PARTITION P4 VALUES LESS THAN(2451910),
        PARTITION P5 VALUES LESS THAN(2452275),
        PARTITION P6 VALUES LESS THAN(2452640),
        PARTITION P7 VALUES LESS THAN(2453005),
        PARTITION P8 VALUES LESS THAN(MAXVALUE)
);
insert into partition_table1 values(2451176);
SELECT count(*) FROM partition_table1 with (nolock, nowait) PARTITION (p2);
SELECT COUNT(*) FROM partition_table1 with (nolock, nowait) PARTITION FOR (2451176);

SELECT count(*) FROM partition_table1 PARTITION (p2) with (nolock, nowait);
SELECT COUNT(*) FROM partition_table1 PARTITION FOR (2451176) with (nolock, nowait);

drop table partition_table1;

-- merge into
DROP TABLE if exists products;
DROP TABLE if exists newproducts;

CREATE TABLE products
(
product_id INTEGER,
product_name VARCHAR(60),
category VARCHAR(60)
);

INSERT INTO products VALUES (1501, 'vivitar 35mm', 'electrncs');
INSERT INTO products VALUES (1502, 'olympus is50', 'electrncs');
INSERT INTO products VALUES (1600, 'play gym', 'toys');
INSERT INTO products VALUES (1601, 'lamaze', 'toys');
INSERT INTO products VALUES (1666, 'harry potter', 'dvd');

CREATE TABLE newproducts
(
product_id INTEGER,
product_name VARCHAR(60),
category VARCHAR(60)
);

INSERT INTO newproducts VALUES (1502, 'olympus camera', 'electrncs');
INSERT INTO newproducts VALUES (1601, 'lamaze', 'toys');
INSERT INTO newproducts VALUES (1666, 'harry potter', 'toys');
INSERT INTO newproducts VALUES (1700, 'wait interface', 'books');

-- error for result table with hint
MERGE INTO products p with (nowait) 
USING newproducts np 
ON (p.product_id = np.product_id)   
WHEN MATCHED THEN  
  UPDATE SET p.product_name = np.product_name, p.category = np.category WHERE p.product_name != 'play gym'  
WHEN NOT MATCHED THEN  
  INSERT VALUES (np.product_id, np.product_name, np.category) WHERE np.category = 'books'; -- d database error

-- error for result table with hint
MERGE INTO products p with (nowait) 
USING newproducts np with (nowait) 
ON (p.product_id = np.product_id)   
WHEN MATCHED THEN  
  UPDATE SET p.product_name = np.product_name, p.category = np.category WHERE p.product_name != 'play gym'  
WHEN NOT MATCHED THEN  
  INSERT VALUES (np.product_id, np.product_name, np.category) WHERE np.category = 'books'; -- d database error

MERGE INTO products p 
USING newproducts np with (nowait)
ON (p.product_id = np.product_id)
WHEN MATCHED AND p.product_name != 'play gym' THEN  
  UPDATE SET 
    p.product_name = np.product_name, 
    p.category = np.category
WHEN NOT MATCHED AND np.category = 'books' THEN  
  INSERT (product_id, product_name, category) 
  VALUES (np.product_id, np.product_name, np.category); --d database gram ok

MERGE INTO products p 
USING newproducts np
ON (p.product_id = np.product_id)   
WHEN MATCHED THEN  
  UPDATE SET p.product_name = np.product_name, p.category = np.category WHERE p.product_name != 'play gym'
WHEN NOT MATCHED THEN  
  INSERT VALUES (np.product_id, np.product_name, np.category) WHERE np.category = 'books'; --og ok

SELECT * FROM products with (nowait) ORDER BY product_id;

-- og using table with hint
MERGE INTO products p 
USING newproducts np with (nowait) 
ON (p.product_id = np.product_id)   
WHEN MATCHED THEN  
  UPDATE SET p.product_name = np.product_name, p.category = np.category WHERE p.product_name != 'play gym'
WHEN NOT MATCHED THEN  
  INSERT VALUES (np.product_id, np.product_name, np.category) WHERE np.category = 'books';

SELECT * FROM products with (nowait) ORDER BY product_id;

DROP TABLE products;
DROP TABLE newproducts;

-- insert
drop table if exists t1;
create table t1(c1 int, c2 int);
insert into t1 values(1, 2);
insert into t1 with (nowait) values(3, 4); -- d database ok
insert into t1 with(nowait) values(3, 4); -- d database ok
insert into t1 with(nowait) (c1, c2) values(3, 4); -- d database ok
insert into t1 with(nowait) (c1,c2) values(3, 4); -- d database ok
insert into t1 with(nowait) (c1,c2)values(3, 4); -- d database ok
insert into t1 with(nowait)(c1,c2)values(3, 4); -- d database ok
insert into t1 with(nowait)(c1,c2,c3)values(3, 4); --error, c3 is not exist
insert into t1 (nowait) values(3, 4); -- d database error, og error, with is necessary
insert into t1 with (nowait) values(5, 6); -- d database ok
insert into t1 with (xlock, nowait) values(7, 8); -- d database ok
insert into t1 as table_t1 (c1, c2) values(9,10); --d database error
insert into t1 as table_t1 (nowait) (c1, c2) values(9,10); --d database error, og error no with
insert into t1 as table_t1 with (nowait) (c1, c2) values(9,10); --d database error
insert into t1 as table_t1 with (nowait, nolock) (c1, c2) values(9,10); --d database error

-- no into in insert statement
insert t1 values(1, 2);
insert t1 with (nowait) values(3, 4); -- d database ok
insert t1 with(nowait) values(3, 4); -- d database ok
insert t1 with(nowait) (c1, c2) values(3, 4); -- d database ok
insert t1 with(nowait) (c1,c2) values(3, 4); -- d database ok
insert t1 with(nowait) (c1,c2)values(3, 4); -- d database ok
insert t1 with(nowait)(c1,c2)values(3, 4); -- d database ok
insert t1 with(nowait)(c1,c2,c3)values(3, 4); --error, c3 is not exist
insert t1 (nowait) values(3, 4); -- d database error, og error, with is necessary
insert t1 with (nowait) values(5, 6); -- d database ok
insert t1 with (xlock, nowait) values(7, 8); -- d database ok
insert t1 as table_t1 (c1, c2) values(9,10); --d database error
insert t1 as table_t1 (nowait) (c1, c2) values(9,10); --d database error, og error no with
insert t1 as table_t1 with (nowait) (c1, c2) values(9,10); --d database error
insert t1 as table_t1 with (nowait, nolock) (c1, c2) values(9,10); --d database error

-- table hint as identifier
show d_format_behavior_compat_options;
create table testabc(nowait int);
insert into testabc (nowait) values(1);
insert into testabc (nowait) (nowait) values(1);
insert into testabc with (nowait) (nowait) values(1);
insert into testabc values(1);
select max(nowait) from testabc;
drop table testabc;

create table test_hint_keyword(nolock int, readuncommitted int, updlock int, repeatableread int, serializable int, readcommitted int,
tablock int, tablockx int, paglock int, rowlock int, nowait int, readpast int, xlock int, snapshot int, noexpand int);

drop table test_hint_keyword;

CREATE OR REPLACE FUNCTION add_numbers(nowait INT, nolock INT)
RETURNS INT AS $$
DECLARE
    result INT;
BEGIN
    result := a + b;
    RETURN result;
END;
$$ LANGUAGE plpgsql;

select add_numbers(3, 7);

drop function add_numbers();

-- table hint as identifier, need to set guc param
set d_format_behavior_compat_options = 'enable_table_hint_identifier';
create table testabc(nowait int);
insert into testabc (nowait) values(1);
insert into testabc (nowait) (nowait) values(1);
insert into testabc with (nowait) (nowait) values(1);
insert into testabc values(1);
select max(nowait) from testabc;
drop table testabc;

create table test_hint_keyword(nolock int, readuncommitted int, updlock int, repeatableread int, serializable int, readcommitted int,
tablock int, tablockx int, paglock int, rowlock int, nowait int, readpast int, xlock int, snapshot int, noexpand int);

drop table test_hint_keyword;

CREATE OR REPLACE FUNCTION add_numbers(a INT, b INT)
RETURNS INT AS $$
DECLARE
    result INT;
BEGIN
    result := a + b;
    RETURN result;
END;
$$ LANGUAGE plpgsql;

select add_numbers(3, 7);

drop function add_numbers();

set d_format_behavior_compat_options = '';
show d_format_behavior_compat_options;

-- partition table
drop table if exists partition_table1;
CREATE TABLE partition_table1
(
    WR_RETURNED_DATE_SK       INTEGER,
    WR_RETURNED_TIME_SK       INTEGER
)
PARTITION BY RANGE(WR_RETURNED_DATE_SK)
(
        PARTITION P1 VALUES LESS THAN(2450815),
        PARTITION P2 VALUES LESS THAN(2451179),
        PARTITION P8 VALUES LESS THAN(MAXVALUE)
);
insert into partition_table1 with (nolock, nowait) values(2451176, 1);
insert into partition_table1 with (nolock, nowait) partition (p1) values(2450000, 1);
insert into partition_table1 with (nolock, nowait) partition for (2451176) values(2451176, 1);
insert into partition_table1 with (nolock, nowait) partition for (2451176) as table1_alias values(2451176, 1);

insert into partition_table1 partition (p1) with (nolock, nowait) values(2450000, 1);
insert into partition_table1 partition for (2451176) with (nolock, nowait) values(2451176, 1);
insert into partition_table1 partition for (2451176) as table1_alias with (nolock, nowait) values(2451176, 1);

drop table partition_table1;

-- attention alias and hint position
drop table if exists list_list;
CREATE TABLE list_list
(
    month_code VARCHAR2 ( 30 ) NOT NULL ,
    dept_code  VARCHAR2 ( 30 ) NOT NULL ,
    user_no    VARCHAR2 ( 30 ) NOT NULL ,
    sales_amt  int
)
PARTITION BY LIST (month_code) SUBPARTITION BY LIST (dept_code)
(
  PARTITION p_201901 VALUES ( '201902' )
  (
    SUBPARTITION p_201901_a VALUES ( '1' ),
    SUBPARTITION p_201901_b VALUES ( '2' )
  ),
  PARTITION p_201902 VALUES ( '201903' )
  (
    SUBPARTITION p_201902_a VALUES ( '1' ),
    SUBPARTITION p_201902_b VALUES ( '2' )
  )
);
insert into list_list (nolock) partition (p_201901) values('201902', '1', '1', 1);
insert into list_list with (nolock) partition for ('201902') values('201902', '1', '1', 1);
insert into list_list with (nolock, nowait) SUBPARTITION (p_201901_b) values('201902', '2', '1', 1);
insert into list_list (nolock) partition (p_201902) as t1 values('201903', '1', '1', 1);
insert into list_list with (nolock) partition for ('201903') as t1 values('201903', '1', '1', 1);
insert into list_list with (nolock, nowait) SUBPARTITION (p_201902_b) as t1 values('201903', '2', '1', 1);

insert into list_list partition (p_201901) (nolock) values('201902', '1', '1', 1);
insert into list_list partition for ('201902') with (nolock) values('201902', '1', '1', 1);
insert into list_list SUBPARTITION (p_201901_b) with (nolock, nowait) values('201902', '2', '1', 1);
insert into list_list partition (p_201902) as t1 (nolock) values('201903', '1', '1', 1);
insert into list_list partition for ('201903') as t1 with (nolock) values('201903', '1', '1', 1);
insert into list_list SUBPARTITION (p_201902_b) as t1 with (nolock, nowait) values('201903', '2', '1', 1);

select * from list_list;
drop table list_list;

-- update
update t1 (xlock) set c1 = 5 where c1 = 3; -- d database error
update t1 with (xlock) set c1 = 10 where c1 = 5; -- d database ok
update t1 with (xlock, nowait) set c1 = 3 where c1 = 10; -- d database ok
update t1 (xlock) as alias_t1 set c1 = 20 where c1 = 2; -- d database not support as alias
update t1 with (xlock) as alias_t1 set c1 = 20 where c1 = 2; -- d database not support as alias
update t1 with (xlock, nowait) as alias_t1 set c1 = 20 where c1 = 2; -- d database not support as alias
update t1 as alias_t1 (lock) set c1 = 20 where c1 = 2; -- d database not support as alias
update t1 as alias_t1 with (xlock) set c1 = 20 where c1 = 2; -- d database not support as alias
update t1 as alias_t1 with (xlock, nowait) set c1 = 20 where c1 = 2; -- d database not support as alias

-- delete
delete from t1 (nolock) where c1 = 3;
delete from t1 with (nolock) where c1 = 4;
delete from t1 with (nolock, nowait) where c1 = 5;
drop table t1;

drop schema test_table_hint cascade;