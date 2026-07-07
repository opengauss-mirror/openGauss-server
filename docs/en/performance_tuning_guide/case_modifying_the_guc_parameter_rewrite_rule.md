# Case: Modifying the GUC Parameter rewrite\_rule<a name="EN-US_TOPIC_0000001086786554"></a>

**rewrite\_rule**  contains multiple query rewriting rules:  **magicset**,  **partialpush**,  **uniquecheck**,  **disablerep**,  **intargetlist**, and  **predpush**. The following describes the application scenarios of some important rules:

## Promoting the Subquery in the Target Column Using intargetlist<a name="section66521181379"></a>

The query performance can be greatly improved by converting the subquery in the target column to JOIN. The following is an example:

```
openGauss=#  set rewrite_rule='none';
SET
postgres=# create table t1(c1 int,c2 int);
CREATE TABLE
postgres=# create table t2(c1 int,c2 int);
CREATE TABLE
postgres=#  explain (verbose on, costs off) select c1,(select avg(c2) from t2 where t2.c2=t1.c2) from t1 where t1.c1<100 order by t1.c2;
                  QUERY PLAN
-----------------------------------------------
 Sort
   Output: t1.c1, ((SubPlan 1)), t1.c2
   Sort Key: t1.c2
   ->  Seq Scan on public.t1
         Output: t1.c1, (SubPlan 1), t1.c2
         Filter: (t1.c1 < 100)
         SubPlan 1
           ->  Aggregate
                 Output: avg(t2.c2)
                 ->  Seq Scan on public.t2
                       Output: t2.c1, t2.c2
                       Filter: (t2.c2 = t1.c2)
(12 rows)
```

Because the subquery  **\(select avg\(c2\) from t2 where t2.c2=t1.c2\)**  in the target column cannot be pulled up, execution of the subquery is triggered each time a row of data of  **t1**  is scanned, and the query efficiency is low. If the  **intargetlist**  parameter is enabled, the subquery is converted to JOIN to improve the query performance.

```
openGauss=#  set rewrite_rule='intargetlist';
SET
openGauss=# explain (verbose on, costs off) select c1,(select avg(c2) from t2 where t2.c2=t1.c2) from t1 where t1.c1<100 order by t1.c2;
                  QUERY PLAN
-----------------------------------------------
 Sort
   Output: t1.c1, (avg(t2.c2)), t1.c2
   Sort Key: t1.c2
   ->  Hash Left Join
         Output: t1.c1, (avg(t2.c2)), t1.c2
         Hash Cond: (t1.c2 = t2.c2)
         ->  Seq Scan on public.t1
               Output: t1.c1, t1.c2
               Filter: (t1.c1 < 100)
         ->  Hash
               Output: (avg(t2.c2)), t2.c2
               ->  HashAggregate
                     Output: avg(t2.c2), t2.c2
                     Group By Key: t2.c2
                     ->  Seq Scan on public.t2
                           Output: t2.c2
(16 rows)
```

## Promoting the Subquery Without Aggregate Using uniquecheck<a name="section20180151614815"></a>

Ensure that each condition has only one line of output. The subqueries with aggregate functions can be automatically pulled up. For subqueries without aggregate functions, the following is an example:

select t1.c1 from t1 where t1.c1 = \(select t2.c1 from t2 where t1.c1=t2.c2\) ;

Rewrite as follows:

select t1.c1 from t1 join \(select t2.c1 from t2 where t2.c1 is not null group by t2.c1\(unique check\)\) tt\(c1\) on tt.c1=t1.c1;

To ensure semantic equivalence, the subquery  **tt**  must ensure that each  **group by t2.c1**  has only one line of output. Enable the  **uniquecheck**  query rewriting parameter to ensure that the query can be pulled up and equivalent. If more than one row of data is output at run time, an error is reported.

```
postgres=# set rewrite_rule='uniquecheck';
SET
openGauss=#  explain verbose select t1.c1 from t1 where t1.c1 = (select t2.c1 from t2 where t1.c1=t2.c1);
                                     QUERY PLAN
-------------------------------------------------------------------------------------
 Hash Join  (cost=43.36..104.40 rows=2149 distinct=[200, 200] width=4)
   Output: t1.c1
   Hash Cond: (t1.c1 = subquery."?column?")
   ->  Seq Scan on public.t1  (cost=0.00..31.49 rows=2149 width=4)
         Output: t1.c1, t1.c2
   ->  Hash  (cost=40.86..40.86 rows=200 width=8)
         Output: subquery."?column?", subquery.c1
         ->  Subquery Scan on subquery  (cost=36.86..40.86 rows=200 width=8)
               Output: subquery."?column?", subquery.c1
               ->  HashAggregate  (cost=36.86..38.86 rows=200 width=4)
                     Output: t2.c1, t2.c1
                     Group By Key: t2.c1
                     Filter: (t2.c1 IS NOT NULL)
                     Unique Check Required
                     ->  Seq Scan on public.t2  (cost=0.00..31.49 rows=2149 width=4)
                           Output: t2.c1
(16 rows)
```

Note: Because  **group by t2.c1 unique check**  occurs before the filter condition  **tt.c1=t1.c1**, an error may be reported after the query that does not report an error is rewritten. An example is as follows:

There are tables  **t1**  and  **t2**. The data in the tables is as follows:

```
openGauss=#  select * from t1 order by c2;
 c1 | c2
----+----
  1 |  1
  2 |  2
  3 |  3
(3 rows)
openGauss=#  select * from t2 order by c2;
 c1 | c2
----+----
  1 |  1
  2 |  2
  3 |  3
  4 |  4
  4 |  4
  5 |  5
(6 rows)
```

Disable and enable the  **uniquecheck**  parameter for comparison. After the parameter is enabled, an error is reported.

```
openGauss=#  select t1.c1 from t1 where t1.c1 = (select t2.c1 from t2 where t1.c1=t2.c2) ;
 c1
----
  1
  2
  3
(3 rows)
openGauss=#  set rewrite_rule='uniquecheck';
SET
openGauss=#  select t1.c1 from t1 where t1.c1 = (select t2.c1 from t2 where t1.c1=t2.c2) ;
ERROR:  more than one row returned by a subquery used as an expression
```
