set explain_perf_mode = pretty;
set current_schema='shipping_schema';
---EC函数 多count distinct场景不下推---
explain (num_costs off)
SELECT count(distinct(a)),count(distinct(b))  FROM 
exec_on_extension('ecshipping', 'select a, b from shipping_schema.shipping_test_row ;') 
as (a int, b int);

---EC CTE场景不下推---
explain (num_costs off)
with tmp1 as
(select a from (select a from exec_on_extension('ecshipping', 'select a, b from shipping_schema.shipping_test_row ;') as (a int)))
select b from 
shipping_test_col, tmp1
where 
shipping_test_col.b = tmp1.a;

---下推场景1---
explain (num_costs off)
with tmp1 as
(select b from shipping_test_col)
select a from (select a from exec_on_extension('ecshipping', 'select a, b from shipping_schema.shipping_test_row ;') as (a int)) tmp2
, tmp1
where 
tmp2.a = tmp1.b;


---EC结果和复制表join（含CTE） 没有stream,存在倾斜，需要设置multiple---
set enable_hashjoin = on;
set enable_mergejoin = off;
set enable_nestloop = off;
explain (num_costs off)
with tmp1 as
(select b from shipping_test_replicalte)
select a from (select a from exec_on_extension('ecshipping', 'select a, b from shipping_schema.shipping_test_row ;') as (a int)) tmp2
, tmp1
where 
tmp2.a = tmp1.b;

--EC结果和复制表join2 hashjoin 没有stream,存在倾斜，需要设置multiple--
set enable_hashjoin = on;
set enable_mergejoin = off;
set enable_nestloop = off;
explain (num_costs off)
select tmp2.a from 
exec_on_extension('ecshipping', 'select a, b from shipping_schema.shipping_test_row ;') as tmp2(a int), 
shipping_test_replicalte tmp1
where tmp2.a=tmp1.b;

--EC结果和复制表join2 hashjoin 有stream，不再倾斜，不需要设置multiple--
set enable_hashjoin = on;
set enable_mergejoin = off;
set enable_nestloop = off;
explain (num_costs off)
select tmp2.a from 
exec_on_extension('ecshipping', 'select a, b from shipping_schema.shipping_test_row ;') as tmp2(a int) full join 
shipping_test_replicalte tmp1
on tmp2.a=tmp1.b;

--EC结果和复制表join2 nestloop，没有stream，数据倾斜，需要设置multiple --
set enable_hashjoin = off ;
set enable_mergejoin = off;
set enable_nestloop = on;
explain (num_costs off)
select /*leading((tmp1 tmp2))*/tmp2.a from 
exec_on_extension('ecshipping', 'select a, b from shipping_schema.shipping_test_row ;') as tmp2(a int) join 
shipping_test_replicalte tmp1
on tmp2.a=tmp1.b;

--EC结果和复制表join2 mergejoin，两边sort 没有stream，需要设置multiple --
set enable_hashjoin = off ;
set enable_nestloop = off;
set enable_mergejoin = on;
explain (num_costs off)
select tmp2.a from 
exec_on_extension('ecshipping', 'select a, b from shipping_schema.shipping_test_row ;') as tmp2(a int) join 
shipping_test_replicalte tmp1
on tmp2.a=tmp1.b;

--两个EC函数join,两边redistribute + sort 走merger join，有steam，不需要设置multiple---
set enable_hashjoin = off ;
set enable_nestloop = off;
set enable_mergejoin = on;
explain (num_costs off)
select tmp2.a from 
exec_on_extension('ecshipping', 'select a from shipping_schema.shipping_test_row ;') as tmp2(a int) join 
exec_on_extension('ecshipping', 'select b from shipping_schema.shipping_test_row ;') as tmp1(b int)
on tmp2.a=tmp1.b;


----EC函数有DN裁剪的表做join----
explain (num_costs off)
SELECT  sum(c1), a1  FROM 
exec_on_extension('ds_libra_115', 'select a1, c1 from ship_t1;') 
as ship_t1(a1 int, c1 int), ship_t2 where ship_t1.a1 = ship_t2.b2 and c2 = 1 group by a1 order by 1,2;

----EC与没有from子句的表做join---
explain (num_costs off)
SELECT  sum(c1)  FROM 
exec_on_extension('ds_libra_115', 'select a1, c1 from ship_t1;') 
as ship_t1(a1 int, c1 int) , (select (select c2) from ship_t2) tmp  
where ship_t1.a1 = tmp.c2;

explain (num_costs off)
SELECT  sum(c1) sum,a1  FROM 
exec_on_extension('ds_libra_115', 'select a1, c1 from ship_t1;') 
as ship_t1(a1 int, c1 int) right join (select (select c2) from ship_t2) tmp  
on ship_t1.a1 = tmp.c2 group by a1 order by a1, sum;

---两个EC函数的join-----
explain (num_costs off)
SELECT  count(*)  FROM 
exec_on_extension('ds_libra_115', 'select a1, c1 from ship_t1;') 
as ship_t1(c1 int), 
exec_on_extension('ds_libra_115', 'select a2, c2 from ship_t2;') as ship_t2(c2 int) where c2<1  
and c2 = c1;

explain (num_costs off)
SELECT  count(*)  FROM 
exec_on_extension('ds_libra_115', 'select a1, c1 from ship_t1;') 
as ship_t1(c1 int) left join
exec_on_extension('ds_libra_115', 'select a2, c2 from ship_t2;') as ship_t2(c2 int) on c2 = c1 where c2<1;

 --------CTE和EC函数结果做Join(CTE被多次复用)-------------
explain (num_costs off)
 with tmp as(
     select a1,sum(d1) num from ship_t1 group by a1 order by 1,2)
 select c2 from exec_on_extension('ds_libra_115', 'select a2, c2 from ship_t2;') as ship_t2(c2 int),
     tmp t1 where t1.num < (select avg(num) from tmp t2 where t1.a1 = t2.a1)
     order by 1;

-----EC函数Agg + Gather + Agg----------
explain (num_costs off)
SELECT  count(*)  FROM 
exec_on_extension('ds_libra_115', 'select a1, c1 from ship_t1;') 
as ship_t1(c1 int);

-----EC函数Agg + redistribute + Agg----------
explain (num_costs off) 
select d2, c2 from ship_t2 inner join
(SELECT /*+ rows(ship_t1*100)*/ sum(a1) total, c1  FROM 
exec_on_extension('ds_libra_115', 'select a1, c1 from ship_t1;') 
as ship_t1(a1 int, c1 int) group by c1) on c1 = d2 order by 1,2 limit 3;

-----EC函数redistribute + Agg----------
explain (num_costs off)
select d2, c2 from ship_t2 inner join
(SELECT /*+ rows(ship_t1*100)*/ sum(a1) total, c1  FROM 
exec_on_extension('ds_libra_115', 'select a1, c1 from ship_t1;') 
as ship_t1(a1 int, c1 int) group by c1) on c1 = d2 order by 1,2 limit 3;

----EC函数sort/GroupAgg---
set enable_hashagg = off;
explain (num_costs off)
select d2, c2 from ship_t2 inner join
(SELECT  sum(a1) total, c1  FROM 
exec_on_extension('ds_libra_115', 'select a1, c1 from ship_t1;') 
as ship_t1(a1 int, c1 int) group by c1) on c1 = d2 order by 1,2 limit 3;

reset enable_hashagg;

-----EC函数AP函数---

explain (num_costs off)
select d2, c2 from ship_t2 inner join
(SELECT  sum(a1) total, c1,a1  FROM 
exec_on_extension('ds_libra_115', 'select a1, c1 from ship_t1;') 
as ship_t1(a1 int, c1 int) group by c1,a1, grouping sets(c1,a1,())) on c1 = d2 order by 1,2 limit 3;

----WindowAgg 函数---
explain (num_costs off)
select c2, sum(d2) over (partition by c2 order by d2)
from exec_on_extension('ds_libra_115', 'select a1, c1 from ship_t1;') as ship_t1(a1 int, c1 int),
ship_t2 where c2<2  and c1 = d2
group by c2,d2
order by 1,2;

----count distinct/windowAgg---
explain (num_costs off)
select count (distinct(c2)), sum(d2) over (partition by c2 order by d2)
from exec_on_extension('ds_libra_115', 'select a1, c1 from ship_t1;') as ship_t1(a1 int, c1 int),
ship_t2 where c2  and c1 = d2
group by c2,d2
order by 1,2
limit 3;

----多个EC函数union,union all----
explain (num_costs off)
select col1, col2 from (
select count (distinct(c1)) col1, sum(a1) over (partition by c1 order by a1) col2
from exec_on_extension('ds_libra_115', 'select a1, c1 from ship_t1;') as ship_t1(a1 int, c1 int)
group by a1,c1
union all
select c2 col1 , a2 col2
from 
exec_on_extension('ds_libra_115', 'select a2, c2 from ship_t2;') as ship_t2(a2 int, c2 int)
union
select c1 col1 , a2 col2
from 
exec_on_extension('ds_libra_115', 'select a2, c1 from ship_t2 inner join ship_t1 on d1=d2;') 
as ship_t2(a2 int, c1 int)
intersect
select c2 col1, d2 col2
from 
ship_t2 where c2 <1) order by 1,2 limit 3;

----EC函数 相关子查询---
explain (num_costs off)
select sum(c1), d1 from ship_t1 where exists
(select c2 
from 
exec_on_extension('ds_libra_115', 'select a2, c2 from ship_t2;') as ship_t2(a2 int, c2 int) where a2 > c1)
group by
d1
order by 1,2 limit 3;

explain (num_costs off)
select sum(c1), a1 from exec_on_extension('ds_libra_115', 'select a1, c1 from ship_t1;') as ship_t1(a1 int, c1 int) 
where exists
(select c2 
from 
exec_on_extension('ds_libra_115', 'select a2, c2 from ship_t2;') as ship_t2(a2 int, c2 int) where a2 > c1)
group by
a1
order by 1,2 limit 3;

----EC函数 非相关子查询---
explain (num_costs off)
select sum(c1), d1 from ship_t1 where b1 in
(select c2 
from 
exec_on_extension('ds_libra_115', 'select a2, c2 from ship_t2;') as ship_t2(a2 int, c2 int) where a2 < 100)
group by
d1
order by 1,2 limit 3;

-----多nodegroup场景组合场景1:------
set expected_computing_nodegroup='group1';
explain (num_costs off)
select col1, col2 from (
select count (distinct(c1)) col1, sum(a1) over (partition by c1 order by a1) col2
from exec_on_extension('ds_libra_115', 'select a1, c1 from ship_t1;') as ship_t1(a1 int, c1 int), ship_t5
where d5 = c1
group by a1,c1
union all
select c4 col1 , a4 col2
from ship_t4
union
select c5 col1 , a5 col2
from 
ship_t5 where d5 = 1
intersect
select c2 col1, a2 col2
from exec_on_extension('ds_libra_115', 'select a2, c2 from ship_t2;') as ship_t2(a2 int, c2 int)
full join ship_t4 on c4 = c2 where c2 <1) order by 1,2 limit 3;

reset expected_computing_nodegroup;

-----多nodegroup场景组合场景2:------
set expected_computing_nodegroup='group1';
explain (num_costs off)
select c4 col1, a4 col2
from ship_t4 where ( 
select a2 from 
exec_on_extension('ds_libra_115', 'select a2, c2 from ship_t2;') as ship_t2(a2 int, c2 int) 
where c4 = c2 and a2 < 1) > 2 order by 1,2 limit 3;
reset expected_computing_nodegroup;

-------------------------------------------exec_hadoop_sql--------------------------------------------
----EC函数有DN裁剪的表做join----
explain (num_costs off)
SELECT  sum(c1), a1  FROM 
exec_hadoop_sql('ecshipping', 'select a1, c1 from ship_t1;','') 
as ship_t1(a1 int, c1 int), ship_t2 where ship_t1.a1 = ship_t2.b2 and c2 = 1 group by a1 order by 1,2;


explain (num_costs off)
SELECT  sum(c1), a1  FROM 
exec_hadoop_sql('ecshipping', 'select a1, c1 from ship_t1;','') 
as ship_t1(a1 int, c1 int) full join ship_t2 on ship_t1.a1 = ship_t2.b2 where c2 = 1 group by a1 order by 1,2;


----EC与没有from子句的表做join---
explain (num_costs off)
SELECT  sum(c1)  FROM 
exec_hadoop_sql('ecshipping', 'select a1, c1 from ship_t1;','') 
as ship_t1(a1 int, c1 int) , (select (select c2) from ship_t2) tmp  
where ship_t1.a1 = tmp.c2;

explain (num_costs off)
SELECT  sum(c1) sum,a1  FROM 
exec_hadoop_sql('ecshipping', 'select a1, c1 from ship_t1;','') 
as ship_t1(a1 int, c1 int) right join (select (select c2) from ship_t2) tmp  
on ship_t1.a1 = tmp.c2 group by a1 order by a1, sum;

---两个EC函数的join-----
explain (num_costs off)
SELECT  count(*)  FROM 
exec_hadoop_sql('ecshipping', 'select a1, c1 from ship_t1;','') 
as ship_t1(c1 int), 
exec_hadoop_sql('ecshipping', 'select a2, c2 from ship_t2;','') as ship_t2(c2 int) where c2<1  
and c2 = c1;

explain (num_costs off) 
SELECT  count(*)  FROM 
exec_hadoop_sql('ecshipping', 'select a1, c1 from ship_t1;','') 
as ship_t1(c1 int) left join
exec_hadoop_sql('ecshipping', 'select a2, c2 from ship_t2;','') as ship_t2(c2 int) on c2 = c1 where c2<1;


 --------CTE和EC函数结果做Join(CTE被多次复用)-------------
 explain (num_costs off)
 with tmp as(
     select a1,sum(d1) num from ship_t1 group by a1 order by 1,2)
 select c2 from exec_hadoop_sql('ecshipping', 'select a2, c2 from ship_t2;','') as ship_t2(c2 int),
     tmp t1 where t1.num < (select avg(num) from tmp t2 where t1.a1 = t2.a1)
     order by 1;


-----EC函数Agg + Gather + Agg----------
explain (num_costs off) 
SELECT  count(*)  FROM 
exec_hadoop_sql('ecshipping', 'select a1, c1 from ship_t1;','') 
as ship_t1(c1 int);

-----EC函数Agg + redistribute + Agg----------
explain (num_costs off)  
select d2, c2 from ship_t2 inner join
(SELECT /*+ rows(ship_t1*100)*/ sum(a1) total, c1  FROM 
exec_hadoop_sql('ecshipping', 'select a1, c1 from ship_t1;','') 
as ship_t1(a1 int, c1 int) group by c1) on c1 = d2 order by 1,2 limit 3;

-----EC函数redistribute + Agg----------
explain (num_costs off)  
select d2, c2 from ship_t2 inner join
(SELECT /*+ rows(ship_t1*100)*/ sum(a1) total, c1  FROM 
exec_hadoop_sql('ecshipping', 'select a1, c1 from ship_t1;','') 
as ship_t1(a1 int, c1 int) group by c1) on c1 = d2 order by 1,2 limit 3;


----EC函数sort/GroupAgg---
set enable_hashagg = off;
explain (num_costs off)  
select d2, c2 from ship_t2 inner join
(SELECT  sum(a1) total, c1  FROM 
exec_hadoop_sql('ecshipping', 'select a1, c1 from ship_t1;','') 
as ship_t1(a1 int, c1 int) group by c1) on c1 = d2 order by 1,2 limit 3;
reset enable_hashagg;

-----EC函数AP函数---

explain (num_costs off)  
select d2, c2 from ship_t2 inner join
(SELECT  sum(a1) total, c1,a1  FROM 
exec_hadoop_sql('ecshipping', 'select a1, c1 from ship_t1;','') 
as ship_t1(a1 int, c1 int) group by c1,a1, grouping sets(c1,a1,())) on c1 = d2 order by 1,2 limit 3;


----WindowAgg 函数---
explain (num_costs off)
select c2, sum(d2) over (partition by c2 order by d2)
from exec_hadoop_sql('ecshipping', 'select a1, c1 from ship_t1;','') as ship_t1(a1 int, c1 int),
ship_t2 where c2<2  and c1 = d2
group by c2,d2
order by 1,2;

----count distinct/windowAgg---
explain (num_costs off)
select count (distinct(c2)), sum(d2) over (partition by c2 order by d2)
from exec_hadoop_sql('ecshipping', 'select a1, c1 from ship_t1;','') as ship_t1(a1 int, c1 int),
ship_t2 where c2  and c1 = d2
group by c2,d2
order by 1,2
limit 3;

----多个EC函数union,union all----
explain (num_costs off)
select col1, col2 from (
select count (distinct(c1)) col1, sum(a1) over (partition by c1 order by a1) col2
from exec_hadoop_sql('ecshipping', 'select a1, c1 from ship_t1;','') as ship_t1(a1 int, c1 int)
group by a1,c1
union all
select c2 col1 , a2 col2
from 
exec_hadoop_sql('ecshipping', 'select a2, c2 from ship_t2;','') as ship_t2(a2 int, c2 int)
union
select c1 col1 , a2 col2
from 
exec_hadoop_sql('ecshipping', 'select a2, c1 from ship_t2 inner join ship_t1 on d1=d2;','') 
as ship_t2(a2 int, c1 int)
intersect
select c2 col1, d2 col2
from 
ship_t2 where c2 <1) order by 1,2 limit 3;


----EC函数 相关子查询---
explain (num_costs off)
select sum(c1), d1 from ship_t1 where exists
(select c2 
from 
exec_hadoop_sql('ecshipping', 'select a2, c2 from ship_t2;','') as ship_t2(a2 int, c2 int) where a2 > c1)
group by
d1
order by 1,2 limit 3;

explain (num_costs off)
select sum(c1), a1 from exec_hadoop_sql('ecshipping', 'select a1, c1 from ship_t1;','') as ship_t1(a1 int, c1 int) 
where exists
(select c2 
from 
exec_hadoop_sql('ecshipping', 'select a2, c2 from ship_t2;','') as ship_t2(a2 int, c2 int) where a2 > c1)
group by
a1
order by 1,2 limit 3;



----EC函数 非相关子查询---
explain (num_costs off)
select sum(c1), d1 from ship_t1 where b1 in
(select c2 
from 
exec_hadoop_sql('ecshipping', 'select a2, c2 from ship_t2;','') as ship_t2(a2 int, c2 int) where a2 < 100)
group by
d1
order by 1,2 limit 3;

-----多nodegroup场景组合场景1:------
set expected_computing_nodegroup='group1';
explain (num_costs off)
select col1, col2 from (
select count (distinct(c1)) col1, sum(a1) over (partition by c1 order by a1) col2
from exec_hadoop_sql('ecshipping', 'select a1, c1 from ship_t1;','') as ship_t1(a1 int, c1 int), ship_t5
where d5 = c1
group by a1,c1
union all
select c4 col1 , a4 col2
from ship_t4
union
select c5 col1 , a5 col2
from 
ship_t5 where d5 = 1
intersect
select c2 col1, a2 col2
from exec_hadoop_sql('ecshipping', 'select a2, c2 from ship_t2;','') as ship_t2(a2 int, c2 int)
full join ship_t4 on c4 = c2 where c2 <1) order by 1,2 limit 3;

reset expected_computing_nodegroup;

-----多nodegroup场景组合场景2:------
set expected_computing_nodegroup='group1';
explain (num_costs off)
select c4 col1, a4 col2
from ship_t4 where ( 
select a2 from 
exec_hadoop_sql('ecshipping', 'select a2, c2 from ship_t2;','') as ship_t2(a2 int, c2 int) 
where c4 = c2 and a2 < 1) > 2 order by 1,2 limit 3;

reset expected_computing_nodegroup;