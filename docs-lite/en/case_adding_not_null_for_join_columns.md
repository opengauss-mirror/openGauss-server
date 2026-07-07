# **Case: Adding NOT NULL for JOIN Columns**<a name="EN-US_TOPIC_0000001382351029"></a>

```
SELECT * FROM join_a a JOIN join_b b ON a.b = b.b;
```

The execution plan is as follows:

```
                                                      QUERY PLAN
----------------------------------------------------------------------------------------------------------------------
 Hash Join  (cost=58.35..14677.69 rows=1074607 width=16) (actual time=23.374..23.384 rows=10 loops=1)
   Hash Cond: (a.b = b.b)
   ->  Seq Scan on join_a a  (cost=0.00..2248.10 rows=100010 width=8) (actual time=0.495..12.551 rows=100010 loops=1)
   ->  Hash  (cost=31.49..31.49 rows=2149 width=8) (actual time=0.614..0.614 rows=1000 loops=1)
          Buckets: 32768  Batches: 1  Memory Usage: 40kB
         ->  Seq Scan on join_b b  (cost=0.00..31.49 rows=2149 width=8) (actual time=0.009..0.183 rows=1000 loops=1)
 Total runtime: 23.716 ms
(7 rows)
```

## Optimization Analysis<a name="en-us_topic_0073253827_en-us_topic_0040046525_section27179124161918"></a>

1. As shown in the execution plan, the sequential scan phase is time consuming.
2. Therefore, you are advised to manually add **NOT NULL** for **JOIN** columns in the statement, as shown below:

    ```
    SELECT
     * 
    SELECT * FROM join_a a JOIN join_b b ON a.b = b.b where a.b IS NOT NULL;
    ```

    The execution plan is as follows:

    ```
                                                         QUERY PLAN
    ---------------------------------------------------------------------------------------------------------------------
     Hash Join  (cost=58.22..14560.97 rows=1063762 width=16) (actual time=13.237..13.247 rows=10 loops=1)
       Hash Cond: (a.b = b.b)
       ->  Seq Scan on join_a a  (cost=0.00..2248.10 rows=99510 width=8) (actual time=12.417..12.422 rows=10 loops=1)
             Filter: (b IS NOT NULL)
             Rows Removed by Filter: 100000
       ->  Hash  (cost=31.49..31.49 rows=2138 width=8) (actual time=0.566..0.566 rows=1000 loops=1)
              Buckets: 32768  Batches: 1  Memory Usage: 40kB
             ->  Seq Scan on join_b b  (cost=0.00..31.49 rows=2138 width=8) (actual time=0.011..0.229 rows=1000 loops=1)
                   Filter: (b IS NOT NULL)
     Total runtime: 13.556 ms
    (10 rows)
    ```
