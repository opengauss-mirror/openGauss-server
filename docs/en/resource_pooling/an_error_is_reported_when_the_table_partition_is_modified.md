# An Error Is Reported When the Table Partition Is Modified<a name="EN-US_TOPIC_0291615103"></a>

## Symptom<a name="section2456113514495"></a>

When  **ALTER TABLE PARTITION**  is performed, the following error message is displayed:

```
ERROR:start value of partition "XX" NOT EQUAL up-boundary of last partition.
```

## Cause Analysis<a name="section14776440134911"></a>

If the  **ALTER TABLE PARTITION**  statement involves both the DROP PARTITION operation and the ADD PARTITION operation, openGauss always performs the DROP PARTITION operation before the ADD PARTITION operation regardless of their orders. However, performing DROP PARTITION before ADD PARTITION causes a partition gap. As a result, an error is reported.

## Procedure<a name="section1838384914918"></a>

To prevent partition gaps, set  **END**  in DROP PARTITION to the value of  **START**  in ADD PARTITION. The following is an example:

```
-- Create a partitioned table partitiontest.
postgres=#  CREATE TABLE partitiontest
(  
c_int integer, 
c_time TIMESTAMP WITHOUT TIME ZONE 
) 
PARTITION BY range (c_int) 
( 
partition p1 start(100)end(108),  
partition p2 start(108)end(120) 
);
-- An error is reported when the following statements are used:
postgres=#  ALTER TABLE partitiontest ADD PARTITION p3 start(120)end(130), DROP PARTITION p2; 
ERROR:  start value of partition "p3" NOT EQUAL up-boundary of last partition. 
postgres=#  ALTER TABLE partitiontest DROP PARTITION p2,ADD PARTITION p3 start(120)end(130); 
ERROR:  start value of partition "p3" NOT EQUAL up-boundary of last partition.
-- Change them as follows:
postgres=#  ALTER TABLE partitiontest ADD PARTITION p3 start(108)end(130), DROP PARTITION p2;
postgres=#  ALTER TABLE partitiontest DROP PARTITION p2,ADD PARTITION p3 start(108)end(130);
```
