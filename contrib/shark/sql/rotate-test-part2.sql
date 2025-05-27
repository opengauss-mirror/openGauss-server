create schema shark_rotate_test_part2;
set search_path = 'shark_rotate_test_part2';

set d_format_behavior_compat_options = 'enable_sbr_identifier';

-- part1: pivot
CREATE TABLE sales (year INT,  product VARCHAR(50),  amount DECIMAL(10, 2));  

INSERT INTO sales (year, product, amount) VALUES  
(2020, 'A', 100),  
(2020, 'B', 200),  
(2021, 'A', 150),  
(2021, 'B', 250);

SELECT *
FROM ( SELECT year, product, amount  FROM sales ) AS source_table
rotate (count(amount) FOR product IN (A, B, a)
) as pivot;

SELECT *
FROM ( SELECT year, product, amount  FROM sales ) AS source_table
rotate (count(amount) FOR product IN ([A], [B], "A", "B", C)
) as pivot;

SELECT *
FROM ( SELECT year, product, amount  FROM sales) AS source_table 
rotate ( count(amount) FOR product IN (A, B) );


SELECT *
FROM ( SELECT year, product, amount  FROM sales) AS source_table 
rotate ( count(amount) FOR product IN (A, B) ) pivot_table;

SELECT *
FROM ( SELECT year, product, amount  FROM sales ) AS source_table
rotate (count(amount) FOR product IN (A, B, 'A', 'SSS', 1, 2, 2.3, 1011)
) as pivot_table;

SELECT *
FROM ( SELECT year, product, amount  FROM sales ) AS source_table
rotate (count(*) FOR product IN (A, B)
) as pivot; 


SELECT *
FROM ( SELECT year, product, amount  FROM sales ) AS source_table
rotate (count(amount) FOR product IN (A, B)
) as pivot order by year;


SELECT *
FROM ( SELECT year, product, amount  FROM sales ) AS source_table
rotate (count(*) FOR product IN (A, B)
) as pivot_table where pivot_table.year > 1; 


-- expect error
SELECT *
FROM ( SELECT year, product, amount  FROM sales ) AS source_table
rotate (count(*) FOR product IN (A, B)
) as pivot_table where source_table.year > 1;  


SELECT *
FROM ( SELECT year, product, amount  FROM sales ) AS source_table
rotate (count(amount) FOR product IN (A.b)
) as pivot;


CREATE PROCEDURE proc1(param1 CHAR(200))
AS
DECLARE
    cur refcursor;
    row record;
BEGIN 
    open cur for SELECT * FROM (SELECT year, product, amount  FROM sales) AS source_table rotate (SUM(amount) FOR product IN ('A', 'B')) ;
    LOOP
        FETCH NEXT FROM cur INTO row;
        EXIT WHEN NOT FOUND;
        RAISE NOTICE '%', row;
    END LOOP;
END;
/

CALL proc1(param1:='123');


declare
con1 varchar;
con2 varchar;
sql1 varchar;
begin
    con1 = 'A';
	con2 = 'B';
	sql1 = 'SELECT * FROM (SELECT year, product, amount  FROM sales) AS source_table rotate (SUM(amount) FOR product IN (''' || con1 || ''',''' || con2 || '''))';
	EXECUTE IMMEDIATE sql1;
end;
/

 create  table CategoryTable2 (
      MergeDocID bigint,
      CategoryFieldName nvarchar(100),
      CategoryFieldValue nvarchar(255)
  ); 
  
  select * 
  from CategoryTable2 rotate  (max(CategoryFieldValue )
  for CategoryFieldName
  in (ItemAssetCategory_Code, ItemCostCategory_Code, ItemCreditCategory_Code, ItemMRPCategory_Code, ItemPriceCategory_Code, ItemProductionCategory_Code, ItemPurchaseCategory_Code, ItemSaleCategory_Code, ItemStockCategory_Code, ItemAssetCategory_Name, ItemCostCategory_Name, ItemCreditCategory_Name, ItemMRPCategory_Name, ItemPriceCategory_Name, ItemProductionCategory_Name, ItemPurchaseCategory_Name, ItemSaleCategory_Name, ItemStockCategory_Name));  

create view v1 as SELECT *
FROM ( SELECT year, product, amount  FROM sales ) AS source_table
rotate (count(*) FOR product IN (A, B)
) as pivot_table where pivot_table.year > 1; 

select * from v1;

-- part2: unpivot

CREATE TABLE sales2 (  
    year INT,  
    product VARCHAR(50),  
    amount DECIMAL(10, 2),
	sale1 int,
	sale2 int,
	sale3 int,
	sale4 int,
	sale5 int
);  

INSERT INTO sales2 (year, product, amount, sale1, sale2, sale3, sale4, sale5) VALUES  
(2020, 'A', 100, 1, 1, 1, 1, 1),  
(2020, 'B', 200, 2, 2, 2, 2, 2),  
(2021, 'A', 150, 3, 3, 3, 3, 3),  
(2021, 'B', 250, 4, 4, 4, 4, 4),
(2022, 'C', 250, 5, 5, 5, 5, 5);

SELECT *
FROM sales2
not rotate(
   sale_all For sale IN (sale1, sale2, sale3, sale4)
) as unpivot;


SELECT *
FROM sales2
not rotate(
   sale_all For sale IN (sale1, sale2, [sale3], [sale4])
) unpivot;

SELECT *
FROM sales2
not rotate(
   sale_all For sale IN (sale1, sale2, sale3, sale4)
);

SELECT *
FROM sales2
not rotate(
   sale_all For sale IN (sale1, sale2, sale3, sale4)
) unpivot;

INSERT INTO sales2 (year, product, amount, sale1, sale2, sale3, sale4, sale5) VALUES (2021, 'A', 150, NULL, NULL, NULL, NULL, NULL), (2021, 'B', 250, NULL, NULL, NULL, NULL, NULL), (2022, 'C', 250, NULL, NULL, NULL, NULL, NULL);

SELECT * FROM sales2  not rotate( sale_all For sale IN (sale1, sale2, sale3, sale4)) as unpvt;

create table aaa as SELECT *
FROM sales2 not rotate(sale_all For sale IN (sale1, sale2, sale3, sale4)) unpvt;

CREATE TABLE t_not_rotate0001_01 (
    Product     VARCHAR(50),
    Q1_Sales    DECIMAL(10,2),
    Q2_Sales    DECIMAL(10,2),
    Q3_Sales    DECIMAL(10,2),
    Q4_Sales    DECIMAL(10,2)
);

INSERT INTO t_not_rotate0001_01 VALUES
    ('Laptop', 1000.00, 1500.00, 1200.00, 1800.00),
    ('Phone',  800.00,  900.00,  950.00,  1100.00),
    ('Tablet', NULL,    500.00,  NULL,     700.00);


SELECT Product, 123Quarter, sales
FROM (
SELECT Product, Q1_Sales, Q2_Sales, Q3_Sales, Q4_Sales
FROM t_not_rotate0001_01
) AS SourceTable
not rotate (
sales FOR Quarter IN (Q1_Sales, Q2_Sales, Q3_Sales, Q4_Sales)
) AS UnpivotTable;


SELECT Product, 123Quarter, sales
FROM (
SELECT Product, Q1_Sales, Q2_Sales, Q3_Sales, Q4_Sales
FROM t_not_rotate0001_01
) AS SourceTable
not rotate (
sales FOR Quarter IN (Q1_Sales, Q2_Sales, Q3_Sales, Q4_Sales)
) AS UnpivotTable;


SELECT Product, Quarter, 123sales
FROM (
SELECT Product, Q1_Sales, Q2_Sales, Q3_Sales, Q4_Sales
FROM t_not_rotate0001_01
) AS SourceTable
not rotate (
sales FOR Quarter IN (Q1_Sales, Q2_Sales, Q3_Sales, Q4_Sales)
) AS UnpivotTable;


SELECT 'Product', Quarter, Sales
FROM (
SELECT Product, Q1_Sales, Q2_Sales, Q3_Sales, Q4_Sales
FROM t_not_rotate0001_01
) AS SourceTable
not rotate (
Sales FOR Quarter IN (Q1_Sales, Q2_Sales, Q3_Sales, Q4_Sales)
) AS UnpivotTable;


SELECT Product, Quarter, 'Sales'
FROM (
SELECT Product, Q1_Sales, Q2_Sales, Q3_Sales, Q4_Sales
FROM t_not_rotate0001_01
) AS SourceTable
not rotate (
Sales FOR Quarter IN (Q1_Sales, Q2_Sales, Q3_Sales, Q4_Sales)
) AS UnpivotTable;


drop table t_not_rotate0001_01;

CREATE PROCEDURE proc2(param1 CHAR(200))
AS
DECLARE
    cur refcursor;
    row record;
BEGIN 
    open cur for SELECT * FROM sales2 not rotate(sale_all For sale IN (sale1, sale2, sale3, sale4)) unpvt;
    LOOP
        FETCH NEXT FROM cur INTO row;
        EXIT WHEN NOT FOUND;
        RAISE NOTICE '%', row;
    END LOOP;
END;
/

CALL proc2(param1:='123');


declare
con1 varchar;
con2 varchar;
sql1 varchar;
begin
    con1 = 'sale1';
	con2 = 'sale2';
	sql1 = 'SELECT * FROM sales2 not rotate(sale_all For sale IN (' || con1 || ',' || con2 || ')) unpvt';
	EXECUTE IMMEDIATE sql1;
end;
/


SELECT *
FROM sales2
not rotate(
   sale_all For sale IN (sale1, sale2, sale3, sale4)
) order by 1,2,3;


create view v2 as SELECT * FROM sales2 not rotate( sale_all For sale IN (sale1, sale2, sale3, sale4)) order by 1,2,3;

select * from v2;

set enable_ignore_case_in_dquotes on;
select * from sales2 as source1
rotate(sum(amount) for product in ('A', 'B')) as source2
not rotate(sale_all for sale in (sale1, sale2, sale3, sale4)) as unpivot_table order by 1,2,3;

set enable_ignore_case_in_dquotes off;

CREATE TABLE t_not_rotate0018 (
ProductID INT PRIMARY KEY,
ProductName VARCHAR(50),
Q1_Sales DECIMAL(10,2),
Q2_Sales DECIMAL(10,2),
Q3_Sales DECIMAL(10,2),
Q4_Sales DECIMAL(10,2)
);

INSERT INTO t_not_rotate0018 VALUES
(1, 'Laptop', 1500.00, 2000.00, 1800.00, 2200.00),
(2, 'Phone', 800.00, 1200.00, 900.00, 1500.00);

SELECT ProductID, ProductName, Quarter, Sales FROM t_not_rotate0018 not rotate ( Sales FOR Quarter IN (Q1_Sales, Q2_Sales, Q3_Sales, Q4_Sales) ) AS UnpivotSales WHERE UnpivotSales.ProductID = 1 ORDER BY Sales DESC;

drop table t_not_rotate0018;

drop table if exists t_rotate0005;
CREATE TABLE t_rotate0005 (
    ID          INT PRIMARY KEY,
    ProductName VARCHAR(50)
) collate utf8_general_ci;
INSERT INTO t_rotate0005 (ID, ProductName) VALUES
    (1, 'Laptop'),
    (2, 'Phone'),
    (3, 'Tablet');
drop table if exists t_rotate0005_01;
CREATE TABLE t_rotate0005_01 (
    ProductID   INT,
    Quarter     CHAR(2),
    Amount      DECIMAL(10,2),
    FOREIGN KEY (ProductID) REFERENCES t_rotate0005(ID)
) collate utf8_general_ci;
INSERT INTO t_rotate0005_01 (ProductID, Quarter, Amount) VALUES
    (1, 'Q1', 1000.00),
    (1, 'Q2', 1500.00),
    (2, 'Q1', 800.00),
    (2, 'Q3', 2000.00),
    (3, 'Q4', 500.00);

SELECT *
FROM (
SELECT p.ProductName, s.Quarter, s.Amount
    FROM t_rotate0005_01 s
    JOIN t_rotate0005 p ON s.ProductID = p.ID
) AS SourceTable
rotate (SUM(Amount) FOR Quarter IN (Q1, Q2, q3, q4)) AS rotateTable;


drop table t_rotate0005_01;
drop table t_rotate0005;

CREATE TABLE t_not_rotate0001_01 ( Product VARCHAR(50), Q1_Sales DECIMAL(10,2), Q2_Sales DECIMAL(10,2), Q3_Sales DECIMAL(10,2), Q4_Sales DECIMAL(10,2) );
INSERT INTO t_not_rotate0001_01 VALUES ('Laptop', 1000.00, 1500.00, 1200.00, 1800.00), ('Phone', 800.00, 900.00, 950.00, 1100.00), ('Tablet', NULL, 500.00, NULL, 700.00);

SELECT Product, Quarter, Sales FROM ( SELECT Product, Q1_Sales, Q2_Sales, Q3_Sales, Q4_Sales FROM t_not_rotate0001_01 ) as "table" not rotate ( Sales FOR Quarter IN (Q1_Sales, Q2_Sales, Q3_Sales, Q4_Sales) ) AS UnpivotTable;

drop table t_not_rotate0001_01;

-- some bug fix
CREATE TABLE t_not_rotate0019 ( ProductID INT PRIMARY KEY, ProductName VARCHAR(50), Q1_Sales DECIMAL(10,2), Q2_Sales DECIMAL(10,2), Q1_Cost DECIMAL(10,2), Q2_Cost DECIMAL(10,2) );
INSERT INTO t_not_rotate0019 VALUES (1, 'Laptop', 1500.00, 2000.00, 1000.00, 1200.00), (2, 'Phone', 800.00, 1200.00, 500.00, 700.00);
SELECT ProductID, ProductName, Sales_Quarter, Sales_Amount, Cost_Quarter, Cost_Amount FROM t_not_rotate0019 not rotate ( Sales_Amount FOR Sales_Quarter IN (Q1_Sales, Q2_Sales) ) AS SalesUnpivot not rotate ( Cost_Amount FOR Cost_Quarter IN (Q1_Cost, Q2_Cost) ) AS CostUnpivot;
drop table t_not_rotate0019;


drop table if exists t_not_rotate0021_01;
CREATE TABLE t_not_rotate0021_01 ( dept_id INT PRIMARY KEY, dept_name VARCHAR(50) NOT NULL UNIQUE, location VARCHAR(100) );
INSERT INTO t_not_rotate0021_01 (dept_id, dept_name, location) VALUES (1, 'HR', 'Beijing'), (2, 'IT', 'Shanghai');
drop table if exists t_not_rotate0021_02;
CREATE TABLE t_not_rotate0021_02 ( emp_id INT PRIMARY KEY, emp_name VARCHAR(50) NOT NULL, email VARCHAR(100) UNIQUE, salary DECIMAL(10,2) CHECK (salary > 0), age INT CHECK (age > 18), hire_date DATE DEFAULT CURRENT_DATE, dept_id INT, CONSTRAINT fk_dept FOREIGN KEY (dept_id) REFERENCES t_not_rotate0021_01(dept_id) ON DELETE SET NULL );
INSERT INTO t_not_rotate0021_02 (emp_id, emp_name, email, salary, age, dept_id) VALUES (101, 'Alice', 'alice@example.com', 50000.00, 25, 1), (102, 'Bob', 'bob@example.com', 60000.00, 30, 2);

SELECT u.emp_id, u.emp_name, u.attribute, u.value, d.dept_name, d.location FROM ( SELECT emp_id, emp_name, salary, CAST(age AS DECIMAL(10,2)) AS age, dept_id FROM t_not_rotate0021_02 ) AS e not rotate ( value FOR attribute IN (salary, age) ) AS u JOIN t_not_rotate0021_01 d ON u.dept_id = d.dept_id ORDER BY u.emp_id, u.attribute;

drop table t_not_rotate0021_02;
drop table t_not_rotate0021_01;

-- part3: ANSI_NULLS
set ANSI_NULLS on;
select NULL = NULL;
select 1 = NULL;
select NULL <> NULL;
select 1 <> NULL;
select NULL > NULL;
select 1 > NULL;
select NULL IS NULL;
select 1 IS NULL;
select NULL IS NOT NULL;
select 1 IS NOT NULL;
select 1 != NULL;
select NULL != NULL;

set ANSI_NULLS off;
select NULL = NULL;
select 1 = NULL;
select NULL <> NULL;
select 1 <> NULL;
select NULL > NULL;
select 1 > NULL;
select NULL IS NULL;
select 1 IS NULL;
select NULL IS NOT NULL;
select 1 IS NOT NULL;
select 1 != NULL;
select NULL != NULL;


CREATE TABLE test1 (a INT NULL);  
INSERT INTO test1 values (NULL),(0),(1);
set ANSI_NULLS on;
select * from test1 where NULL = NULL;
select * from test1 where 1 = NULL;
select * from test1 where NULL <> NULL;
select * from test1 where 1 <> NULL;
select * from test1 where NULL > NULL;
select * from test1 where 1 > NULL;
select * from test1 where NULL IS NULL;
select * from test1 where 1 IS NULL;
select * from test1 where NULL IS NOT NULL;
select * from test1 where 1 IS NOT NULL;
select * from test1 where 1 != NULL;
select * from test1 where NULL != NULL;


set ANSI_NULLS off;
select * from test1 where NULL = NULL;
select * from test1 where 1 = NULL;
select * from test1 where NULL <> NULL;
select * from test1 where 1 <> NULL;
select * from test1 where NULL > NULL;
select * from test1 where 1 > NULL;
select * from test1 where NULL IS NULL;
select * from test1 where 1 IS NULL;
select * from test1 where NULL IS NOT NULL;
select * from test1 where 1 IS NOT NULL;
select * from test1 where 1 != NULL;
select * from test1 where NULL != NULL;

set d_format_behavior_compat_options 'enable_sbr_identifier';
set vacuum_cost_page_dirty 20;
set cpu_tuple_cost 0.02;
set effective_cache_size '128MB';

reset vacuum_cost_page_dirty;
reset cpu_tuple_cost;
reset effective_cache_size;

set ansi_nulls on;
show ansi_nulls;
show transform_null_equals;
set transform_null_equals on;
show transform_null_equals;
show ansi_nulls;

set ansi_nulls = off;
show ansi_nulls;
show transform_null_equals;
set transform_null_equals = off;
show transform_null_equals;
show ansi_nulls;

-- part3 : body and rownum
create table body(body varchar);
insert into body values ('body');
select body from body;

create table rownum(rownum varchar);
insert into rownum values ('rownum');
select rownum from rownum;

create table pivot(pivot varchar);
insert into pivot values ('pivot');
select pivot from pivot;

create table unpivot(unpivot varchar);
insert into unpivot values ('unpivot');
select unpivot from unpivot;

drop table sales;
drop table categorytable2;
drop view v1;
drop table sales2;
drop table aaa;
drop view v2;
drop table test1;
drop table body;
drop table rownum;
drop table pivot;
drop table unpivot;

drop schema shark_rotate_test_part2 cascade;
