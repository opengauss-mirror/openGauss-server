create schema reduce_orderby;
set current_schema = reduce_orderby;
--create table
create table reduce_orderby_t1(a int, b varchar(10));
create table reduce_orderby_t2(a int, b varchar(10));
--insert into hash order by in subquery
explain (costs off) insert into reduce_orderby_t1 select a, b from reduce_orderby_t1 order by b;

explain (costs off) insert into reduce_orderby_t1 select * from (select a, b from reduce_orderby_t1 order by b) union all (select a, b from reduce_orderby_t2 order by a);

explain (costs off) insert into reduce_orderby_t1 select v.b from (select * from (select a, b from reduce_orderby_t1 order by b) union all (select a, b from reduce_orderby_t2 order by a)) v group by 1;

explain (costs off) insert into reduce_orderby_t1 select * from (select * from (select * from reduce_orderby_t1 order by 1));

--table setop and there is order by in subquery
explain (costs off) select * from (select a, b from reduce_orderby_t1 order by b) union all (select a, b from reduce_orderby_t2 order by a);

explain (costs off) select v.b from (select * from (select a, b from reduce_orderby_t1 order by b) union all (select a, b from reduce_orderby_t2 order by a)) v group by 1;

explain (costs off) (select a,b from reduce_orderby_t1 where a<10 group by 1,2 order by 1,2) intersect (select a,b from reduce_orderby_t1 where a>5 group by 1,2 order by 1,2) except all select a,b from reduce_orderby_t2 where b>10 group by 1,2 order by 1,2;

explain (costs off) (select a,b from reduce_orderby_t1 t1 where t1.a in (select b from reduce_orderby_t2 where b<100 order by 1) group by 1,2 order by 1,2) intersect (select tt1.a,tt1.b from reduce_orderby_t1 tt1 left join reduce_orderby_t2 tt2 on tt1.a=tt2.b group by 1,2 order by 1,2) except all select t32.a,t32.b from reduce_orderby_t2 t32 right join reduce_orderby_t1 t31 on t31.b=t32.a group by 1,2 order by 1,2; 

explain (costs off) 
(select a,b from reduce_orderby_t1 t1 where t1.a in (select b from reduce_orderby_t2 union all (select b from reduce_orderby_t1 order by 1) order by 1) group by 1,2 order by 1,2) 
intersect (select tt1.a,tt1.b from reduce_orderby_t1 tt1 left join (select a from reduce_orderby_t2 union (select a from reduce_orderby_t1 order by 1) order by 1) tt2 on tt1.a=tt2.a group by 1,2 order by 1,2) except all select t32.a,t32.b from reduce_orderby_t2 t32 right join reduce_orderby_t1 t31 on t31.b=t32.a group by 1,2 order by 1,2;

--table join and there is order by in subquery
explain (costs off) select a, b from reduce_orderby_t1 order by a;

explain (costs off) select t1.a from reduce_orderby_t1 t1 left join reduce_orderby_t2 t2 on t1.a=t2.b order by t1.a;

explain (costs off) select t1.a from reduce_orderby_t1 t1 where t1.b in (select t3.b from reduce_orderby_t1 t3 left join reduce_orderby_t2 t2 on t3.a=t2.b order by 1) order by t1.a;

--drop table
drop table reduce_orderby_t1;
drop table reduce_orderby_t2;

CREATE TABLE orders (user_id int, order_id int, order_date date, quantity int, revenue float, product text);
INSERT INTO orders VALUES
(1, 1, '2021-03-05', 1, 15, 'books'),
(1, 2, '2022-03-07', 1, 3, 'music'),
(1, 3, '2022-06-15', 1, 900, 'travel'),
(1, 4, '2021-11-17', 2, 25, 'books'),
(2, 5, '2022-08-03', 2, 32, 'books'),
(2, 6, '2021-04-12', 2, 4, 'music'),
(2, 7, '2021-06-29', 3, 9, 'books'),
(2, 8, '2022-11-03', 1, 8, 'music'),
(3, 9, '2022-11-07', 1, 575, 'food'),
(3, 10, '2022-11-20', 2, 95, 'food'),
(3, 11, '2022-11-20', 1, 95, 'food'),
(4, 12, '2022-11-20', 2, 95, 'books'),
(4, 13, '2022-11-21', 1, 95, 'food'),
(4, 14, '2022-11-23', 4, 17, 'books'),
(5, 15, '2022-11-20', 1, 95, 'food'),
(5, 16, '2022-11-25', 2, 95, 'books'),
(5, 17, '2022-11-29', 1, 95, 'food');

explain (costs off) SELECT avg(o.quantity) AS avg_quantity,
sum(o.revenue) AS total_revenue
FROM (
SELECT DISTINCT ON (user_id)
user_id, product
FROM orders
ORDER BY user_id, order_date
) init
JOIN orders o USING (user_id, product)
WHERE init.product = 'books';

SELECT avg(o.quantity) AS avg_quantity,
sum(o.revenue) AS total_revenue
FROM (
SELECT DISTINCT ON (user_id)
user_id, product
FROM orders
ORDER BY user_id, order_date
) init
JOIN orders o USING (user_id, product)
WHERE init.product = 'books';


explain (costs off) SELECT avg(o.quantity) AS avg_quantity,
sum(o.revenue) AS total_revenue
FROM (
SELECT DISTINCT ON (user_id)
user_id, product
FROM orders
ORDER BY user_id, order_date
) init
JOIN orders o USING (user_id, product)
WHERE init.product = 'books' offset 10;

create table outer_(id int);
create table inner_(id1 int, id2 int);
set enable_hashjoin TO off;
set enable_mergejoin to off;
explain (costs off) select * from (select * from outer_ order by id desc) o left join inner_ i on o.id = i.id1 limit 1;
explain (costs off) select * from (select * from outer_ order by id desc) o left join inner_ i on o.id = i.id1 where rownum < 10;

drop table orders;
--drop schema
reset current_schema;
drop schema reduce_orderby cascade;
