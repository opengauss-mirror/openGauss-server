# Case: Reconstructing Partitioned Tables<a name="EN-US_TOPIC_0000001330911106"></a>

## Symptom<a name="en-us_topic_0075873749_section3352584712931"></a>

In the following simple SQL statements, the performance bottlenecks exist in the scan operation on the **normal\_date** table.

```
                                                                  QUERY PLAN
-----------------------------------------------------------------------------------------------------------------------------------------------
 Seq Scan on normal_date  (cost=0.00..259.00 rows=30 width=12) (actual time=0.100..3.466 rows=30 loops=1)
   Filter: (("time" >= '2022-09-01 00:00:00'::timestamp without time zone) AND ("time" <= '2022-10-01 00:00:00'::timestamp without time zone))
   Rows Removed by Filter: 9970
 Total runtime: 3.587 ms
(4 rows)
```

## Optimization Analysis<a name="en-us_topic_0075873749_section45836675121326"></a>

Obviously, the table data \(in the **time** column\) has date features in the service layer, and this meet the features of a partitioned table. Replan the table definition of the **normal\_date** table by defining a partitioned table **normal\_date\_part**. Set the **time** column as a partitioning key and month as an interval unit for the partitioned table. The modified result is as follows, and the performance is nearly 10 times.

```
                                                                     QUERY PLAN
-----------------------------------------------------------------------------------------------------------------------------------------------------
 Partition Iterator  (cost=0.00..480.00 rows=30 width=12) (actual time=0.038..0.085 rows=30 loops=1)
   Iterations: 2
   ->  Partitioned Seq Scan on normal_date_part  (cost=0.00..480.00 rows=30 width=12) (actual time=0.049..0.063 rows=30 loops=2)
         Filter: (("time" >= '2022-09-01 00:00:00'::timestamp without time zone) AND ("time" <= '2022-10-01 00:00:00'::timestamp without time zone))
         Rows Removed by Filter: 31
         Selected Partitions:  3..4
 Total runtime: 0.360 ms
(7 rows)
```
