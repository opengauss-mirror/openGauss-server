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
