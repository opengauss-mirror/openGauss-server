# Primary Node Is Hung in Demoting During a Switchover<a name="EN-US_TOPIC_0291615094"></a>

## Symptom<a name="section6629536475"></a>

In a cluster deployed with one primary and multiple standby DNs, if system resources are insufficient and a switchover occurs, a node is hung in demoting.

## Cause Analysis<a name="section84391240378"></a>

If system resources are insufficient, the third-party management thread cannot be created. As a result, the managed sub-threads cannot exit and the primary node is hung in demoting.

## Procedure<a name="section64961744772"></a>

Run the following command to stop the process of the primary node so that the standby node can be promoted to primary: Perform the following operations only in the preceding scenario.

```
 kill -9 PID
```
