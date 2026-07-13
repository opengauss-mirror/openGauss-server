# Reindexing Fails<a name="EN-US_TOPIC_0291615106"></a>

## Symptom<a name="section094511327226"></a>

When an index of the desc table is damaged, a series of operations cannot be performed. The error information may be as follows:

```
index \"%s\" contains corrupted page at block
 %u" ,RelationGetRelationName(rel),BufferGetBlockNumber(buf), please reindex it.
```

## Cause Analysis<a name="section461110379220"></a>

In actual operations, indexes may break down due to software or hardware faults. For example, if disk space is insufficient or pages are damaged after indexes are split, the indexes may be damaged.

## Procedure<a name="section8841134114226"></a>

If the table is a column-store table named  **pg\_cudesc\_**_xxxxx_**\_index**, the desc index table is damaged. Find the OID and table corresponding to the primary table based on the desc index table name, and run the following statement to recreate the cudesc index.

```
REINDEX INTERNAL TABLE name;
```
