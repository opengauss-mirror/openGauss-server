# LLVM Application Scenarios and Restrictions<a name="EN-US_TOPIC_0245374539"></a>

## Application Scenarios<a name="en-us_topic_0237121504_en-us_topic_0066033419_section279430195545"></a>

- Expressions supporting LLVM

    The query statements that contain the following expressions support LLVM optimization:

    1. Case...when...
    2. IN
    3. Bool (AND/OR/NOT)
    4. BooleanTest (IS\_NOT\_KNOWN/IS\_UNKNOWN/IS\_TRUE/IS\_NOT\_TRUE/IS\_FALSE/IS\_NOT\_FALSE)
    5. NullTest (IS\_NOT\_NULL/IS\_NULL)
    6. Operator
    7. Function
    8. Nullif

    Supported data types for expression computing are bool, tinyint, smallint, int, bigint, float4, float8, numeric, date, time, timetz, timestamp, timestamptz, interval, bpchar, varchar, text, and oid.

    Consider using LLVM only if expressions are used in the following content in a vectorized executor: **filter** in the **Scan** node; **complicate hash condition**, **hash join filter**, and **hash join target** in the **Hash Join** node; **filter** and **join filter** in the **Nested Loop** node; **merge join filter** and **merge join target** in the **Merge Join** node; and **filter** in the **Group** node.

- Operators supporting LLVM

    1. Join: HashJoin
    2. Agg: HashAgg
    3. Sort

    Where HashJoin supports only Hash Inner Join, and the corresponding hash cond supports comparisons between int4, bigint, and bpchar. HashAgg supports sum and avg operations of bigint and numeric data types. Group By statements supports int4, bigint, bpchar, text, varchar, timestamp, and count\(\*\) aggregation operation. Sort supports only comparisons between int4, bigint, numeric, bpchar, text, and varchar data types. Except the preceding operations, LLVM cannot be used. You can use the explain performance tool to check whether LLVM can be used.

## Non-applicable Scenarios<a name="en-us_topic_0237121504_en-us_topic_0066033419_section316931181001"></a>

- Tables that have small amount of data cannot be dynamically compiled.
- Query jobs with a non-vectorized execution path cannot be generated.
