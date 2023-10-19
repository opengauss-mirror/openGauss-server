--pg_typeof(any)
--textanycat(text,any)
--anytextcat(any,text)
--format(text,any)
set current_schema='shipping_schema';

---------------------pg_typeof(any)--------------------------
----case1 shipping---
explain (num_costs off)
select pg_typeof(t4.a),pg_typeof(t5.b), pg_typeof(t4.c) from t4, t5 where t4.b=t5.a limit 1;

select pg_typeof(t4.a),pg_typeof(t5.b), pg_typeof(t4.c) from t4, t5 where t4.b=t5.a limit 1;

----case2 shipping---
explain (num_costs off)
select sum(t4.a),t4.c from t4, t5 where t4.b=t5.a and pg_typeof(shipping_schema.t5.c)::text ='text' group by t4.c order by 1,2;

select sum(t4.a),t4.c from t4, t5 where t4.b=t5.a and pg_typeof(shipping_schema.t5.c)::text ='text' group by t4.c order by 1,2;

---case3 shipping---
explain (num_costs off) 
select pg_typeof(t4.a) , pg_typeof(t4.c) from t4;

select pg_typeof(t4.a) , pg_typeof(t4.c) from t4 order by 1,2;

---------------------anytextcat(any,text)--------------------------
----case1  record cause not-shipping---
explain (num_costs off)
select anytextcat(t4,t5.c) from t4, t5 where t4.b=t5.a order by 1 limit 3;

select anytextcat(t4,t5.c) from t4, t5 where t4.b=t5.a order by 1 limit 3;

----case2: shipping---
explain (num_costs off)
select anytextcat(t5.c,min((t4.b::numeric))),t4.a from t4, t5 where t4.b=t5.c group by t5.c ,t4.a order by 1,2;

---case3: array_expr cases not-shipping ---
explain (num_costs off)
select anytextcat(concat(array[t4.a,t4.b]),t5.c) from t4, t5 where t4.a=t5.a order by 1 limit 3;

select anytextcat(concat(array[t4.a,t4.b]),t5.c) from t4, t5 where t4.a=t5.a order by 1 limit 3;

---case4: shipping---
explain (num_costs off)
select anytextcat(t4.a,t4.c) from t4 where t4.a  limit 3;

select anytextcat(t4.a,t4.c) from t4 where t4.a order by 1 limit 3;


---case5: array_expr cases not-shipping ---
explain (num_costs off)
select anytextcat(concat(array[t4.a,t4.b]),t4.c) from t4;

select anytextcat(concat(array[t4.a,t4.b]),t4.c) from t4 order by 1 limit 3;


---------------------textanycat(text,any)--------------------------

----case1  record cause not-shipping---
explain (num_costs off)
select textanycat(t5.c,t4) from t4, t5 where t4.b=t5.a order by 1 limit 3;

select textanycat(t5.c,t4) from t4, t5 where t4.b=t5.a order by 1 limit 3;

----case2: shipping---
explain (num_costs off)
select t4.a from t4, t5 where t4.b=t5.c and textanycat(t5.c,(t4.b::numeric))='11' group by t5.c ,t4.a, t4.b order by 1;

select t4.a from t4, t5 where t4.b=t5.c and textanycat(t5.c,(t4.b::numeric))='11' group by t5.c ,t4.a, t4.b order by 1;
---case3: shipping ---
explain (num_costs off)
select sum(t5.a) from t4, t5 where t4.a=t5.a group by t5.c,t4.a,t4.b having length(textanycat(concat(t4.a,t4.b),t5.c))>1 order by 1 limit 3;

select sum(t5.a) from t4, t5 where t4.a=t5.a group by t5.c,t4.a,t4.b having length(textanycat(concat(t4.a,t4.b),t5.c))>1 order by 1 limit 3;

---case4: array_expr cases not-shipping ---
explain (num_costs off)
select sum(t5.a) from t4, t5 where t4.a=t5.a group by t5.c,t4.a,t4.b having length(textanycat(concat(array[t4.a,t4.b]),t5.c))>1 order by 1 limit 3;

select sum(t5.a) from t4, t5 where t4.a=t5.a group by t5.c,t4.a,t4.b having length(textanycat(concat(array[t4.a,t4.b]),t5.c))>1 order by 1 limit 3;


---case5: array_expr cases not-shipping ---
explain (num_costs off)
select textanycat(concat(array[t4.a,t4.b]),t4.c) from t4;

select textanycat(concat(array[t4.a,t4.b]),t4.c) from t4 order by 1 limit 3;

---------------------format(text,any)--------------------------
----case1  record cause not-shipping---
explain (num_costs off)
select format('Hello %s, %1$s', t4) from t4, t5 where t4.b=t5.a order by 1 limit 3;

select format('Hello %s, %1$s', t4) from t4, t5 where t4.b=t5.a order by 1 limit 3;


----case2: shipping---
explain (num_costs off)
select format('Hello %s, %1$s', t4.c, t5.a) from t4, t5 where t4.b=t5.c and textanycat(t5.c,(t4.b::numeric))='11' group by t5.a ,t4.a, t4.b,t4.c order by 1 limit 3;

select format('Hello %s, %1$s', t4.c, t5.a) from t4, t5 where t4.b=t5.c and textanycat(t5.c,(t4.b::numeric))='11' group by t5.a ,t4.a, t4.b, t4.c order by 1 limit 3;

----case3: shipping---
explain (num_costs off)
select format('Hello %s, %1$s', t4.c) from t4;

select format('Hello %s, %1$s', t4.c) from t4 order by 1 limit 3;


