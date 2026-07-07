# Hint of Parameterized Paths at the Same Level<a name="EN-US_TOPIC_0000001266694989"></a>

## Function<a name="section290819468377"></a>

The  **predpush\_same\_level**  hint is used to specify the generation of parameterized paths between tables or MVs at the same level.

## Syntax<a name="section530131664410"></a>

```
predpush_same_level(src, dest)
predpush_same_level(src1 src2 ..., dest)
```

>[!NOTE]NOTE 
>This parameter takes effect only when the  **predpushforce**  option in  **rewrite\_rule**  is enabled.

## Examples<a name="section5736356154"></a>

Prepare parameters, tables, and indexes.

```
openGauss=# set rewrite_rule = 'predpushforce';
SET
openGauss=# create table t1(a int, b int);
CREATE TABLE
openGauss=# create table t2(a int, b int);
CREATE TABLE
openGauss=# create index idx1 on t1(a);
CREATE INDEX
openGauss=# create index idx2 on t2(a);
CREATE INDEX
```

Run the following statement to view the plan:

```
openGauss=# explain select * from t1, t2 where t1.a = t2.a;
                            QUERY PLAN
------------------------------------------------------------------
 Hash Join  (cost=27.50..56.25 rows=1000 width=16)
   Hash Cond: (t1.a = t2.a)
   ->  Seq Scan on t1  (cost=0.00..15.00 rows=1000 width=8)
   ->  Hash  (cost=15.00..15.00 rows=1000 width=8)
         ->  Seq Scan on t2  (cost=0.00..15.00 rows=1000 width=8)
(5 rows)
```

The filter condition  **t1.a = t2.a**  is displayed on  **Join**. In this case,  **predpush\_same\_level\(t1, t2\)**  can be used to push the condition down to the scan operator of t2.

```
openGauss=# explain select /*+predpush_same_level(t1, t2)*/ * from t1, t2 where t1.a = t2.a;
                             QUERY PLAN
---------------------------------------------------------------------
 Nested Loop  (cost=0.00..335.00 rows=1000 width=16)
   ->  Seq Scan on t1  (cost=0.00..15.00 rows=1000 width=8)
   ->  Index Scan using idx2 on t2  (cost=0.00..0.31 rows=1 width=8)
         Index Cond: (a = t1.a)
(4 rows)
```

>[!TIP]NOTICE 
>
>- **predpush\_same\_level**  can specify multiple  **src**  parameters in the same condition.
>- If the specified  **src**  and  **dest**  conditions do not exist or do not meet the parameterized path requirements, this hint does not take effect.
