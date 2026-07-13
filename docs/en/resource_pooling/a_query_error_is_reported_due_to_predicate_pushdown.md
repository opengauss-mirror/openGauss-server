# A Query Error Is Reported Due to Predicate Pushdown<a name="EN-US_TOPIC_0289900936"></a>

## Symptom<a name="en-us_topic_0283137100_en-us_topic_0059778167_s7a2ed06fefd0448fae90f40fe4291f8d"></a>

When a predicate is pushed down in a plan, an error should not be reported according to the query execution sequence in the SQL standard. However, an error occurs during the execution.

```
openGauss=# select * from tba;
 a 
---
 -1
  2
(2 rows)

openGauss=# select * from tbb;
 b 
---
 -1
  1
(2 rows)

openGauss=# select * from tba join tbb on a > b where b > 0 and sqrt(a) > 1;
ERROR: cannot table square root of a negative number
```

Execute the SQL standard process.

1. Execute the FROM clause to ensure that all data meets the `a > b` condition.
2. Execute the WHERE clause with the `b > 0` condition. If the result is `true`, `a > 0` can be deduced and the execution continues. If the result is `false`, the subsequent conditions are short-circuited and will not be executed.
3. Execute the WHERE clause with the `sqrt(a) > 1` condition.

However, an error is reported, indicating that the input parameter is a negative value.

## Cause Analysis<a name="en-us_topic_0283137100_en-us_topic_0059778167_s74d2dfcb815b4d8ca504c549a923e5ed"></a>

```
openGauss=# explain (costs off) select * from tba join tbb on a > b where b > 0 and sqrt(a) > 1;
            QUERY PLAN            
----------------------------------
Nest loop
  Join Filter: (a > b)
  -> Seq Scan on public.tba
       Filter: (sqrt(a) > 1)
  -> Materialize
       -> Seq Scan on public.tbb
            Filter: (b > 0)
(7 rows)
```

According to the analysis plan, the original `a > b`, `b > 0`, and `sqrt(a) > 1` conditions are split and pushed down to different operators. As a result, the conditions are not executed in sequence.
In addition, the current equivalence class inference supports only equal sign (=) inference and cannot automatically supplement `a > 0`.
As a result, an error is reported during the query.

## Procedure<a name="en-us_topic_0283137100_section485620163250"></a>

Predicate pushdown can greatly improve query performance, and this special short-circuit and derivation scenario is not considered in most database optimizers. Therefore, you are advised to modify the query statement and manually add `a > 0` under related conditions.

```
openGauss=# select * from tba join tbb on a > b where b > 0 and a > 0 and sqrt(a) > 1;
 a | b 
---+---
 2 | 1 
(1 row)

openGauss=# explain (costs off) select * from tba join tbb on a > b where b > 0 and a > 0 and sqrt(a) > 1;
              QUERY PLAN              
--------------------------------------
Nest loop
  Join Filter: (a > b)
  -> Seq Scan on public.tba
       Filter: (a > 0 and sqrt(a) > 1)
  -> Materialize
       -> Seq Scan on public.tbb
            Filter: (b > 0)
(7 rows)
```
