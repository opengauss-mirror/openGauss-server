set current_schema='shipping_schema';
set explain_perf_mode = pretty;
-----对View_a.* 的强转场景不能下推---
-----case1 不下推---
explain (num_costs off)
select (select (a.*)::text) from view_t10 a order by 1;
select (select (a.*)::text) from view_t10 a order by 1;
-----case2 不下推---
explain (num_costs off)
select (a.*)::text  from view_t10 a order by 1;
select (a.*)::text  from view_t10 a order by 1;
-----case3 不下推---
explain (num_costs off)
select (a.*) from view_t10 a, view_t10 b where (a.*)::text = (b.*)::text order by 1; 
select (a.*) from view_t10 a, view_t10 b where (a.*)::text = (b.*)::text order by 1; 
-----case4 下推---
explain (num_costs off)
select (a.*) from view_t10 a, view_t10 b where (select a.*)::text = (select b.*)::text order by 1;
select (a.*) from view_t10 a, view_t10 b where (select a.*)::text = (select b.*)::text order by 1;
----case5 不下推
explain (num_costs off)
select (select (select (select (a.*)::text)) from view_t10 a order by 1 limit 1 );
select (select (select (select (a.*)::text)) from view_t10 a order by 1 limit 1 );


---Array_SubLink测试,下推结果正确------
---case1----
explain (num_costs off)
SELECT ARRAY(select f2 from t11 order by f2) AS "ARRAY";  
SELECT ARRAY(select f2 from t11 order by f2) AS "ARRAY"; 
---case2----
explain (num_costs off)
SELECT ARRAY(select f2 from t11 order by f2) AS "ARRAY", f1 from t11 order by 1,2; 
SELECT ARRAY(select f2 from t11 order by f2) AS "ARRAY", f1 from t11 order by 1,2; 
---case3----
explain (num_costs off)
SELECT ARRAY(select (select f2 from t11 order by f2 limit 1)) AS "ARRAY", f1 from t11 order by 1,2; 
SELECT ARRAY(select (select f2 from t11 order by f2 limit 1)) AS "ARRAY", f1 from t11 order by 1,2; 
---case4----
explain (num_costs off)
select ARRAY(select f2  from t11 where f2 >(select f2 from t11 order by f2 limit 1) ) order by 1;
---case5----
explain (num_costs off)
SELECT ARRAY(select f2 from t11 ) AS "ARRAY", f1 from t11 order by 2; 
---case6----
explain (num_costs off)
SELECT ARRAY(select avg(f2) over(partition by f2 order by f2) from t11) as "ARRAY" ;
---case7----
explain (verbose on, num_costs off)
SELECT ARRAY(select sum(f2) over(order by f2) from t11) as "ARRAY" ;

---case8---
explain (verbose on, num_costs off)
SELECT ARRAY(select sum(f2) over(order by f2) from t11 order by f2) as "ARRAY" ;

--case9---
set  enable_seqscan=off;
explain (verbose on, num_costs off)
select array(select f0 from t13 order by 1) as "array";
select array(select f0 from t13 order by 1) as "array";
reset enable_seqscan;

---case10--
set  enable_seqscan=off;
explain (verbose on, num_costs off)
select array(select f0 from (select f0 from t13 order by 1) order by 1) as "array";
select array(select f0 from (select f0 from t13 order by 1) order by 1) as "array";
reset enable_seqscan;



---insert values ----
create table shipping_insert (a int);
explain (verbose on, num_costs off)
insert into shipping_insert values ((select 1));
insert into shipping_insert values ((select 1));
explain (verbose on, num_costs off)
insert into shipping_insert values ((select 1 from dual));

explain (verbose on, num_costs off)
insert into shipping_insert values ((select 1));
explain (verbose on, num_costs off)
insert into shipping_insert values ((select 1 from dual));

---group by ----
explain (verbose on, num_costs off)
select a from shipping_test_col group by (select array(select 1)),a;
select a from shipping_test_col group by (select array(select 1)),a;

---having---
explain (verbose on, num_costs off)
select 1 from shipping_test_col having count(a) <=any (select count(b)  group by 1);
select 1 from shipping_test_col having count(a) <=any (select count(b)  group by 1);

-----order by ---
explain (verbose on, num_costs off)
select f0 from t12 group by f0 having count(f0) <=any (select count(f1)  group by f0) order by (select f0);
select f0 from t12 group by f0 having count(f0) <=any (select count(f1)  group by f0) order by (select f0);

---setop/limit---
explain (verbose on, num_costs off)
select f0 from t12 where f2 = (select 1.21) 
union 
select (2) 
union all 
select t1.f0 from t12 t1 full join t12 t2 on (t1.f1 = t2.f1 and t1.f2 < (select 2)) order by 1 
limit (select f0 from t12 group by f0 having count(f0) <=any (select count(f1)  group by f0) order by (select f0) limit 1);

select f0 from t12 where f2 = (select 1.21) 
union 
select (2) 
union all 
select t1.f0 from t12 t1 full join t12 t2 on (t1.f1 = t2.f1 and t1.f2 < (select 2)) order by 1 
limit (select f0 from t12 group by f0 having count(f0) <=any (select count(f1)  group by f0) order by (select f0) limit 1);

---SubPlan---
explain (verbose on, num_costs off)
select f0 from t12 where exists (select 1) and (select 1.25) > any(select f2) order by 1;
select f0 from t12 where exists (select 1) and (select 1.25) > any(select f2) order by 1;

---SubLink---
explain (verbose on, num_costs off)
select (select f0 > all (select f2 from t12)) from t12 where exists (select 1) and (select 1.25) > any(select f2) order by 1;
select (select f0 > all (select f2 from t12)) from t12 where exists (select 1) and (select 1.25) > any(select f2) order by 1;

----CTE---
explain (verbose on, num_costs off)
with tmp(c1) as (
(select 3) union select (select f0) from t12 where (select f1)='cat1' )
select f1 from t12,tmp where tmp.c1 = t12.f0 limit (select 2);
with tmp(c1) as (
(select 3) union select (select f0) from t12 where (select f1)='cat1' )
select f1 from t12,tmp where tmp.c1 = t12.f0 limit (select 2);

---distinct ---
explain (verbose on, num_costs off)
select count(distinct(select f1)) from t12 where exists (select 1) and (select 1.25) > any(select f2) order by 1;
select count(distinct(select f1)) from t12 where exists (select 1) and (select 1.25) > any(select f2) order by 1;

---window Agg---
explain (verbose on, num_costs off)
select sum(f0), sum(f2) over (partition by (select f1) order by (select f1)) tmp
from  t12 where (select f1)='cat1' group by f2,f0,f1
order by 1,2;

select sum(f0), sum(f2) over (partition by (select f1) order by (select f1)) tmp
from t12 where (select f1)='cat1' group by f2,f0,f1
order by 1,2;

----Agg---
explain (verbose on, num_costs off)
select sum((select (f0))), sum(f2) over (partition by (select f1) order by (select f1)) tmp
from  t12 where (select f1)='cat1' group by f2,f0,f1
order by 1,2 limit 2;

select sum((select (f0))), sum(f2) over (partition by (select f1) order by (select f1)) tmp
from  t12 where (select f1)='cat1' group by f2,f0,f1
order by 1,2 limit 2;


---多nodegroup场景---
set expected_computing_nodegroup =group1;
---case1---
explain (verbose on, num_costs off)
select f0 from t14 where f2 = (select 1.21) 
union 
select (2) 
union all 
select t1.f0 from t14 t1 full join t14 t2 on (t1.f1 = t2.f1 and t1.f2 < (select 2)) order by 1 
limit (select f0 from t14 group by f0 having count(f0) <=any (select count(f1)  group by f0) order by (select f0) limit 1);

select f0 from t14 where f2 = (select 1.21) 
union 
select (2) 
union all 
select t1.f0 from t14 t1 full join t14 t2 on (t1.f1 = t2.f1 and t1.f2 < (select 2)) order by 1 
limit (select f0 from t14 group by f0 having count(f0) <=any (select count(f1)  group by f0) order by (select f0) limit 1);

----case2---
explain (verbose on, num_costs off)
select sum((select (f0))), sum(f2) over (partition by (select f1) order by (select f1)) tmp
from  t14 where (select f1)='cat1' group by cube(f2,f0,(select f1))
order by 1,2 limit 2;

select sum((select (f0))), sum(f2) over (partition by (select f1) order by (select f1)) tmp
from  t14 where (select f1)='cat1' group by cube(f2,f0,(select f1))
order by 1,2 limit 2;

---Array_SubLink nodegroup场景------
---case1----
explain (num_costs off)
SELECT ARRAY(select f2 from t14 order by f2) AS "ARRAY";  
SELECT ARRAY(select f2 from t14 order by f2) AS "ARRAY"; 
---case2----
explain (num_costs off)
SELECT ARRAY(select f2 from t14 order by f2) AS "ARRAY", f1 from t14 order by 1,2; 
SELECT ARRAY(select f2 from t14 order by f2) AS "ARRAY", f1 from t14 order by 1,2; 
---case3----
explain (num_costs off)
SELECT ARRAY(select (select f2 from t14 order by f2 limit 1)) AS "ARRAY", f1 from t14 order by 1,2; 
SELECT ARRAY(select (select f2 from t14 order by f2 limit 1)) AS "ARRAY", f1 from t14 order by 1,2; 
---case4----
explain (num_costs off)
select ARRAY(select f2  from t14 where f2 >(select f2 from t14 order by f2 limit 1) order by 1) order by 1;
---case5----
explain (num_costs off)
SELECT ARRAY(select f2 from t14 ) AS "ARRAY", f1 from t14 order by 2; 
---case6----
explain (num_costs off)
SELECT ARRAY(select avg(f2) over(partition by f2 order by f2) from t14) as "ARRAY" ;
---case7----
explain (verbose on, num_costs off)
SELECT ARRAY(select sum(f2) over(order by f2) from t14) as "ARRAY" ;

---case8---
explain (verbose on, num_costs off)
SELECT ARRAY(select sum(f2) over(order by f2) from t14 order by f2) as "ARRAY" ;

--case9---
set  enable_seqscan=off;
explain (verbose on, num_costs off)
select array(select f0 from t15 order by 1) as "array";
select array(select f0 from t15 order by 1) as "array";
reset enable_seqscan;

---case10--
set  enable_seqscan=off;
explain (verbose on, num_costs off)
select array(select f0 from (select f0 from t15 order by 1) order by 1) as "array";
select array(select f0 from (select f0 from t15 order by 1) order by 1) as "array";
reset enable_seqscan;
