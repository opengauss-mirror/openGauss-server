--
-- Test rotate and not rotate grammer
--

-- Create
create table original_orders (id int, year int, order_mode text, order_total int);
insert into original_orders values (1,2020,'direct',5000), (2,2020,'online',1000), (3,2021,'online',1000), (4,2021,'direct',1000), (5,2022,'direct',5000), (6,2020,'direct',500);

select * from ( select year, order_mode, order_total from original_orders) rotate (sum(order_total) for order_mode in ('direct' as store, 'online' as internet)) order by year;
with tt as (
    select year, order_mode, order_total from original_orders
)
select * from  tt rotate (sum(order_total) for order_mode in ('direct' as store, 'online' as internet)) order by year;

select * from (select year, order_mode, order_total from original_orders) rotate (sum(order_total) for order_mode in ('online' as internet )) order by year;

create table rotate_orders as (select * from (select year, order_mode, order_total from original_orders) as t rotate (sum(order_total) for order_mode in ('direct' as store, 'online' as internet)) order by year);

-- test not rotate (column transform to row)
select * from rotate_orders not rotate ( yearly_total for order_mode in ( store as 'direct', internet as 'online'));
with tt as (
    select * from rotate_orders
)
select * from tt not rotate ( yearly_total for order_mode in ( store as 'direct', internet as 'online')) order by year;

select * from rotate_orders not rotate exclude nulls (yearly_total for order_mode in ( store as 'direct', internet as 'online'));

select * from rotate_orders not rotate include nulls ( yearly_total for ordre_mode in ( store as 'direct', internet as 'online'));

-- rotate and not rotate are in the same sql
select * from (select year, direct, online from (select year, order_mode, order_total from original_orders) rotate (sum(order_total) for order_mode in ('direct', 'online')) order by year) as rotate_t not rotate ( yearly_total for order_mode in (direct, online));

select * from (select year, order_mode, yearly_total from (select * from rotate_orders not rotate ( yearly_total for order_mode in ( store as 'direct', internet as 'online')))) rotate (sum(yearly_total) for order_mode in ('direct' as store, 'online' as internet) ) order by year;
with tt as(
    select year, order_mode, order_total from original_orders
)
select * from (select year, direct, online from tt rotate (sum(order_total) for order_mode in ('direct', 'online')) order by year) as rotate_t not rotate ( yearly_total for order_mode in (direct, online));

-- create view
create view rotate_view as (select * from (select year, order_mode, order_total from original_orders) as t rotate (sum(order_total) for order_mode in ('direct' as store, 'online' as internet)) order by year);
select * from rotate_view;

create view notrotate_view as (select * from rotate_orders not rotate ( yearly_total for order_mode in ( store as 'direct', internet as 'online')));
select * from notrotate_view;

drop view rotate_view;
drop view notrotate_view;

-- 子查询
select * from (select year, direct as store, online as internet from (select year, order_mode, order_total from original_orders ) as orders rotate (sum(order_total) for order_mode in ('direct', 'online')) )order by year;
with orders as (
    select year, order_mode, order_total from original_orders
)
select * from (select year, direct as store, online as internet from orders rotate (sum(order_total) for order_mode in ('direct', 'online')) )order by year;

select year, order_mode, yearly_total from(select * from rotate_orders not rotate ( yearly_total for order_mode in ( store as 'direct', internet as 'online'))) where year> 2020;

with a_tab as (
    select * from (select year, order_mode, order_total from original_orders ) as orders rotate (sum(order_total) for order_mode in ('direct', 'online')) order by year
),
b_tab as (
    select * from a_tab
)
select * from b_tab;

with a_tab as (
    select * from rotate_orders
),
b_tab as (select * from (select * from a_tab not rotate exclude nulls (yearly_total for order_mode in ( store as 'direct', internet as 'online'))) order by year )
select * from b_tab;
-- SMP
set query_dop = 4;
select * from ( select year, order_mode, order_total from original_orders) rotate (sum(order_total) for order_mode in ('direct' as store, 'online' as internet)) order by year;
with tt as (
    select year, order_mode, order_total from original_orders
)
select * from  tt rotate (sum(order_total) for order_mode in ('direct' as store, 'online' as internet)) order by year;

select * from rotate_orders not rotate ( yearly_total for order_mode in ( store as 'direct', internet as 'online'));
set query_dop = 1;

-- table name contain quotes
create table "'rotate'orders" as (select * from (select year, order_mode, order_total from original_orders) as t rotate (sum(order_total) for order_mode in ('direct' as store, 'online' as internet)) order by year);
select * from "'rotate'orders" not rotate ( yearly_total for order_mode in ( store as 'direct', internet as 'online'));

create table "ROTATEorders" as (select * from (select year, order_mode, order_total from original_orders) as t rotate (sum(order_total) for order_mode in ('direct' as store, 'online' as internet)) order by year);
select * from "ROTATEorders" not rotate ( yearly_total for order_mode in ( store as 'direct', internet as 'online'));

create table "rotate@orders" as (select * from (select year, order_mode, order_total from original_orders) as t rotate (sum(order_total) for order_mode in ('direct' as store, 'online' as internet)) order by year);
select * from "rotate@orders" not rotate ( yearly_total for order_mode in ( store as 'direct', internet as 'online'));

--procedure
CREATE OR REPLACE PROCEDURE proc_rotate is
DECLARE
    total_max int;
BEGIN
    SELECT max(order_total)
    INTO total_max
    FROM original_orders;
RAISE NOTICE 'total_max: %', total_max;
CREATE TABLE proc_rotate_tt AS
    SELECT * FROM (
        SELECT * FROM rotate_orders 
        WHERE  store < total_max )
    not rotate ( yearly_total for order_mode in ( store as 'direct', internet as 'online'));
    COMMIT;
END;
/

call proc_rotate();
select * from proc_rotate_tt;

drop procedure proc_rotate;
drop table proc_rotate_tt;
drop table "'rotate'orders";
drop table "ROTATEorders";
drop table "rotate@orders";
drop table original_orders;
drop table rotate_orders;

-- more than one col
create table original_orders2 (id int, year int, order_mode text, order_total int, order_value int);
insert into original_orders2 values (1,2020,'direct',5000,10),(2,2020,'online',1000,20), (3,2021,'online',1000,30), (4,2021,'direct',1000,50), (5,2022,'direct',5000,100), (6,2020,'direct',500,2);

select * from ( select year, order_mode, order_total from original_orders2) rotate (sum(order_total) ss,AVG(order_value) av for order_mode in ('direct' as store, 'online' as internet)) order by year;

create table rotate_orders2 as ( select * from ( select year, order_mode, order_total from original_orders2 ) as t rotate (sum(order_total) ss, avg(order_value) av for order_mode in ( 'direct' as store, 'online' as internet)) order by year);
select * from rotate_orders2 not rotate ( (yearly_total, yearly_arv) for order_mode in ( ( store_ss,store_av) as 'direct', (internet_ss,internet_av) as 'online'));

drop table original_orders2;
drop table rotate_orders2;

--in clause without alias
create table sale ( product varchar(50), month varchar(30), sales int);
insert into sale (product, month, sales) values ('a','january',100),('a','february',100),('a','march',100);
select * from sale rotate (sum(sales) for month in ('january','february','march'));

create table stu(name varchar(50), math int, english int, chinese int);
insert into stu values ('zhang',7,8,9);
select * from stu;
select * from stu not rotate (num for list in (math, english, chinese));

drop table sale;
drop table stu;

--in clause not string
create table sale ( product varchar(50), month int, sales int);
insert into sale (product, month, sales) values ('a',1,100),('a',2,100),('a',3,100);
select * from sale rotate (sum(sales) for month in (1,2,3));
drop table sale;

create table sale ( product varchar(50), month float, sales int);
insert into sale (product, month, sales) values ('a',1.1,100),('a',1.2,100),('a',1.3,100);
select * from sale rotate (sum(sales) for month in (1.1,1.2,1.3));
drop table sale;

--not rotate clause with different arguments
create table saleslist (consumer varchar2(20), commodity varchar2(20), salesnum number(8));
insert into saleslist values ('aaa','上衣',5),('bbb','裤子',3),('ccc','袜子',2),('ddd','上衣',4),('eee','裤子',6);
create table rotate_sales as select * from saleslist rotate(max(salesnum) for commodity in ('上衣' as 上衣, '裤子' as 裤子, '袜子' as 袜子, '帽子' as 帽子)) where 1=1;
select * from rotate_sales not rotate (salesnum for aaa in (上衣, 裤子, 袜子, 帽子));
select * from rotate_sales not rotate (salesnum for aaa in (上衣));
drop table rotate_sales;
drop table saleslist;

-- column table
create table product_column (id int, name varchar(20), value int) with (orientation = column);
insert into product_column values 
(1,'a',10),(2,'b',20),(3,'c',30),(4,'a',40),(5,'b',50),(6,'c',60);
select * from (select name, value from product_column) rotate(sum(value) for name in ('a','b','c'));

with tt as (
    select name, value from product_column
)
select * from tt rotate(sum(value) for name in ('a','b','c'));
create table product_column_un (a int, b int, c int) with (orientation = column);
insert into product_column_un values(50,70,90);
select * from product_column_un not rotate (value for name in (a,b,c));

drop table product_column;
drop table product_column_un;

--partition table
create table orders_par (id int, year int, order_mode text, order_total int) partition by range(order_total) (partition par1 values less than (1000),partition par2 values less than (2000),partition par3 values less than (maxvalue));
insert into orders_par values (1,2020,'direct',500), (2,2020,'online',1000), (3,2021,'online',200), (4,2021,'direct',100), (5,2022,'direct',2000), (6,2020,'direct',5000);
select * from ( select year, order_mode, order_total from orders_par) rotate (sum(order_total)
for order_mode in ('direct' as store, 'online' as internet)) order by year;
with tt as (
    select year, order_mode, order_total from orders_par
)
select * from tt rotate (sum(order_total) for order_mode in ('direct' as store, 'online' as internet)) order by year;
select * from ( select year, order_mode, order_total from orders_par partition(par1)) rotate (sum(order_total)
for order_mode in ('direct' as store, 'online' as internet)) order by year;
with tt as (
    select year, order_mode, order_total from orders_par partition(par1)
)
select * from tt rotate (sum(order_total) for order_mode in ('direct' as store, 'online' as internet)) order by year;

create table rotate_orders_par(year int, store int, internet int) partition by range(store)(partition par1 values less than (1000),partition par2 values less than (maxvalue));
insert into rotate_orders_par values (2020, 5500,1000),(2021,100,200),(2022,2000,null);
select * from rotate_orders_par not rotate ( yearly_total for order_mode in ( store as 'direct', internet as 'online'));

drop table orders_par;
drop table rotate_orders_par;

--ustore
create table product_ustore(id int, name varchar(10), value int) with (storage_type=ustore);
insert into product_ustore values (10,'a',10),(20,'b',20),(30,'c',30),(101,'a',40),(201,'b',50),(301,'c',60);
select * from (select name, value from product_ustore) rotate(sum(value) for name in ('a','b','c'));
with tt as (
    select name, value from product_ustore
)
select * from tt rotate(sum(value) for name in ('a','b','c'));

create table stu_ustore (name varchar(20), math int, english int, chinese int) with (storage_type=ustore);
insert into stu_ustore values('Tom',10,20,30);
select * from stu_ustore not rotate (num for list in (math, english,chinese));

drop table product_ustore;
drop table stu_ustore;

--segment
create table product_segment(id int, name varchar(10), value int) with (segment=on);
insert into product_segment values (1,'a',10),(2,'b',20),(3,'c',30),(4,'a',40),(5,'b',50),(6,'c',60);
select * from (select name, value from product_segment) rotate(sum(value) for name in ('a','b','c'));
with tt as (
    select name, value from product_segment
)
select * from tt rotate(sum(value) for name in ('a','b','c'));

create table stu_segment (name varchar(20), math int, english int, chinese int) with (segment=on);
insert into stu_segment values('Tom',10,20,30);
select * from stu_segment not rotate (num for list in (math, english,chinese));

drop table product_segment;
drop table stu_segment;

-- multi-value
create table rotate_case (id int, score int, class varchar(20), semester int);
insert into rotate_case values (2, 35, null, null),(2, 35, 'phy', null),(2, 35, null, 1),(3, 40, 'math',1),(3, 50, 'math',1),(4, 55, 'phy', 1);
select * from rotate_case rotate (max(score) for (class, semester) in (('phy',1),('math',1))) order by id;
select * from rotate_case rotate (max(score) for (class, semester) in (('phy',1) as physics, ('math',1) as mathematics)) order by id;

create table stu_segment1 (name varchar(20), math1 int, math2 int, english1 int, english2 int, chinese1 int, chinese2 int);
insert into stu_segment1 values('Tom',10,20,30,40,50,60);
insert into stu_segment1 values('Jery',10,null,20,null,null,null);

select * from stu_segment1 not rotate include nulls ((num1,num2) for list1 in ((math1, math2), (english1, english2), (chinese1, chinese2)));
select * from stu_segment1 not rotate exclude nulls ((num1,num2) for list1 in ((math1, math2), (english1, english2), (chinese1, chinese2)));
select * from stu_segment1 not rotate ((num1,num2) for list1 in ((math1, math2), (english1, english2), (chinese1, chinese2)));
select * from stu_segment1 not rotate include nulls ((num1,num2) for list1 in ((math1, math2) as 'math', (english1, english2) as 'english', (chinese1, chinese2) as 'chinese'));
select * from stu_segment1 not rotate include nulls ((num1,num2) for (list1,list2) in ((math1, math2) as ('m1','m2'), (english1, english2) as ('e1','e2'), (chinese1, chinese2) as ('c1','c2')));
select * from stu_segment1 not rotate include nulls ((num1,num2) for (list1,list2) in ((math1, math2) as 'math', (english1, english2) as 'english', (chinese1, chinese2) as 'chinese'));

drop table rotate_case;
drop table stu_segment1;

-- test other condition for not rotate
create table rotate_orders (year int, store int, internet int);
insert into rotate_orders values (2020, 5500, 1000),(2021, 1000, 1000),(2022, 5000, null),(2023, null, 2000);
select * from rotate_orders not rotate include nulls (yearly_total for order_mode in (internet as 'online', store as 'direct')) where yearly_total is not null;
select * from rotate_orders not rotate include nulls (yearly_total for order_mode in (internet as 'online', store as 'direct')) limit 1;
select * from rotate_orders not rotate include nulls (yearly_total for order_mode in (internet as 'online', store as 'direct')) order by yearly_total desc;

--test alias for not rotate
select year as yy, order_mode as mode, yearly_total as total from rotate_orders not rotate (yearly_total for order_mode in (internet as 'online', store as 'direct'));
select year as yy, yearly_total as total from rotate_orders not rotate (yearly_total for order_mode in (internet as 'online', store as 'direct'));
select year as yy, order_mode as mode, yearly_total as total from rotate_orders not rotate include nulls (yearly_total for order_mode in (internet as 'online', store as 'direct')) where yearly_total is not null order by yy desc;
drop table rotate_orders;