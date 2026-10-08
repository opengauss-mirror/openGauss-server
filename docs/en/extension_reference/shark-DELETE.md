# DELETE

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-07-30T06:31:45.724Z pushedAt=2026-09-21T10:03:17.916Z -->

## Description<a name="zh-cn_topic_0283136795_zh-cn_topic_0237122131_zh-cn_topic_0059778379_se9507fb26df547a795ac7940e3a19ebf"></a>

DELETE deletes rows that satisfy the WHERE clause from the specified table. If the WHERE clause is absent, all rows in the table will be deleted, leaving only the table structure.

## NOTE<a name="zh-cn_topic_0283136795_zh-cn_topic_0237122131_zh-cn_topic_0059778379_sfc96c070e8574f4ea9a2726e898fda17"></a>

- This section contains only the syntax newly added by shark. The original openGauss DELETE syntax has not been deleted or modified. For the original openGauss DELETE syntax, see [DELETE](../sql_reference/delete.md).
- The table_hint clause is newly supported.

## Syntax<a name="zh-cn_topic_0283136795_zh-cn_topic_0237122131_zh-cn_topic_0059778379_s84baecef89484d5f87f57b0545b46202"></a>

```
Single-table deletion:
[ WITH [ RECURSIVE ] with_query [, ...] ]
DELETE [/*+ plan_hint */] [FROM] [ ONLY ] table_name [ * ]
[ [ [partition_clause] [ [ AS ] alias ] [ table_hint_clause ] ] | [ [ [ AS ] alias ] [partitions_clause] ] ]
    [ USING using_list ]
    [ FROM from_list 
    [JOIN join_table ON join_condition]... ]
    [ WHERE condition | WHERE CURRENT OF cursor_name ]
    [ ORDER BY {expression [ [ ASC | DESC | USING operator ]
    [ LIMIT { count } ]
    [ RETURNING { * | { output_expr [ [ AS ] output_name ] } [, ...] } ];

Multi-table deletion:
[ WITH [ RECURSIVE ] with_query [, ...] ]
DELETE [/*+ plan_hint */] [FROM] 
    {[ ONLY ] table_name [ * ] [ [ [partition_clause] [ [ AS ] alias ] [ table_hint_clause ] ] | [ [ [ AS ] alias ] [partitions_clause] ] ]} [, ...]
    [ USING using_list ]
    [ FROM from_list 
    [JOIN join_table ON join_condition]... ]
    [ WHERE condition  ];
Or
[ WITH [ RECURSIVE ] with_query [, ...] ]
DELETE [/*+ plan_hint */]
    {[ ONLY ] table_name [ * ] [ [ [partition_clause] [ [ AS ] alias ] [ table_hint_clause ] ] | [ [ [ AS ] alias ] [partitions_clause] ] ]} [, ...]
    [ FROM using_list ]
    [ WHERE condition ];
```

- The table_hint clause table_hint_clause is:

    ```
    WITH ( <table_hint> [, ...] ) 
    ```

## Parameter Description<a name="zh-cn_topic_0283136795_zh-cn_topic_0237122131_zh-cn_topic_0059778379_s6df87c0dd87c49e29a034e0ff3385ca7"></a>

- **JOIN**

    JOIN includes INNER JOIN, LEFT JOIN, RIGHT JOIN, FULL JOIN, and CROSS JOIN.

- **WITH ( <table_hint> [, ...] )**

    - Unlike the SELECT clause, where WITH is optional for a single hint, WITH is mandatory for the DELETE clause. table_hint supports a list of options, separated by commas or spaces. That is, WITH (hint1), WITH (hint1, hint2, ...), and WITH (hint1 hint2 ...) are all supported, while (hint1) is not supported.

    - Supported hints include NOLOCK, READUNCOMMITTED, UPDLOCK, REPEATABLEREAD, SERIALIZABLE, READCOMMITTED, TABLOCK, TABLOCKX, PAGLOCK, ROWLOCK, NOWAIT, READPAST, XLOCK, SNAPSHOT, and NOEXPAND.

    - When the above hints need to be used as identifiers, such as column names or variable names, set d_format_behavior_compat_options = 'enable_table_hint_identifier'. The default value of this variable is d_format_behavior_compat_options = ''.

    - All hints are only syntactically supported and have no actual semantics.

    - Relevant NOTICE messages will be printed for hints.

## table_hint Clause Example<a name="zh-cn_topic_0283136578_zh-cn_topic_0237122106_zh-cn_topic_0059777455_s985289833081489e9d77c485755bd362"></a>

```
create table t1 (c1 int);

delete from t1 with (nolock) where c1 = 5;
NOTICE:  The nolock option is currently ignored

delete from t1 with (nolock, nowait) where c1 = 5;
NOTICE:  The nolock option is currently ignored
NOTICE:  The nowait option is currently ignored

delete from t1 with (nolock nowait) where c1 = 5;
NOTICE:  The nolock option is currently ignored
NOTICE:  The nowait option is currently ignored
```

## Related Links<a name="section156744489391"></a>

[DELETE](../sql_reference/delete.md)