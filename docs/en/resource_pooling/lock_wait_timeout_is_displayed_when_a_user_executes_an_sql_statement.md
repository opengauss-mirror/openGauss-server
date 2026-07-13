# "Lock wait timeout" Is Displayed When a User Executes an SQL Statement<a name="EN-US_TOPIC_0291615101"></a>

## Symptom<a name="section158125414577"></a>

"Lock wait timeout" is displayed when a user executes an SQL statement.

```
ERROR:  Lock wait timeout: thread 140533638080272 waiting for ShareLock on relation 16409 of database 13218 after 1200000.122 ms ERROR:  Lock wait timeout: thread 140533638080272 waiting for AccessExclusiveLock on relation 16409 of database 13218 after 1200000.193 ms
```

## Cause Analysis<a name="section7762348125715"></a>

Lock waiting times out in the database.

## Procedure<a name="section72471253195718"></a>

- After detecting such errors, the database automatically retries the SQL statements. The number of retries is controlled by  **max\_query\_retry\_times**.

- To analyze the cause of the lock wait timeout, find the SQL statements that time out in the  **pg\_locks**and  **pg\_stat\_activity**system catalogs.
