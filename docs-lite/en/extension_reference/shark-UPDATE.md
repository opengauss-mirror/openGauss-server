# UPDATE

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-09-13T02:31:06.041Z pushedAt=2026-09-29T02:34:56.697Z -->

## Description<a name="zh-cn_topic_0283137651_zh-cn_topic_0237122194_zh-cn_topic_0059778969_s85747c5f88e64562a8ff9ddacda19939"></a>

Updates data in a table. UPDATE changes the values of the specified columns in all rows that satisfy the condition. The WHERE clause specifies the condition. The columns specified in the SET clause will be modified. Columns that are not listed retain their original values.

## Precautions<a name="zh-cn_topic_0283137651_zh-cn_topic_0237122194_zh-cn_topic_0059778969_s7e9e912f472543cbb190edb83e5f22d3"></a>

- This section describes only the syntax newly added in shark. The original openGauss UPDATE syntax is not deleted or modified. For details about the original openGauss UPDATE syntax, see [UPDATE](https://docs.opengauss.org/en/docs/latest/sql_reference/update.html).
- Support for the table_hint clause is added.

## Syntax Format<a name="zh-cn_topic_0283137651_zh-cn_topic_0237122194_zh-cn_topic_0059778969_sd8d9ff15ff6c45c9aebd16c861936c07"></a>

```
Single-table update:
[ WITH [ RECURSIVE ] with_query [, ...] ]
UPDATE [/*+ plan_hint */] [ ONLY ] table_name [ partition_clause ] [ * ] [ [ AS ] alias ] [table_hint_clause]
SET {column_name = { expression | DEFAULT } 
    |( column_name [, ...] ) = {( { expression | DEFAULT } [, ...] ) |sub_query }}[, ...]
    [ FROM from_list] [JOIN join_table ON join_condition]... ]
    [ WHERE condition | WHERE CURRENT OF cursor_name ]
    [ ORDER BY {expression [ [ ASC | DESC | USING operator ]
    [ LIMIT { count } ]
    [ RETURNING {* 
                | {output_expression [ [ AS ] output_name ]} [, ...] }];

Multi-table update:
[ WITH [ RECURSIVE ] with_query [, ...] ]
UPDATE [/*+ plan_hint */] table_list
SET {column_name = { expression | DEFAULT } 
    |( column_name [, ...] ) = {( { expression | DEFAULT } [, ...] ) |sub_query }}[, ...]
    [ FROM from_list] [ WHERE condition ];

where sub_query can be:
SELECT [ ALL | DISTINCT [ ON ( expression [, ...] ) ] ]
{ * | {expression [ [ AS ] output_name ]} [, ...] }
[ FROM from_item [, ...] ] [JOIN join_table ON join_condition]... ]
[ WHERE condition ]
[ GROUP BY grouping_element [, ...] ]
[ HAVING condition [, ...] ]
[ ORDER BY {expression [ [ ASC | DESC | USING operator ] | nlssort_expression_clause ] [ NULLS { FIRST | LAST } ]} [, ...] ]
[ LIMIT { [offset,] count | ALL } ]
```

- The table\_hint clause `table_hint_clause` is:

    ```
    WITH ( <table_hint> [, ...] ) 
    ```

## Parameters<a name="zh-cn_topic_0283137651_zh-cn_topic_0237122194_zh-cn_topic_0059778969_sf3e3262b89854b3d829a94054116838d"></a>

- **JOIN**

    JOIN includes INNER JOIN, LEFT JOIN, RIGHT JOIN, FULL JOIN, and CROSS JOIN.

- **WITH ( <table_hint> [, ...] )**

    - Unlike the SELECT clause, WITH is optional for a single hint, but is required for the UPDATE clause. table_hint supports a list of options, where options are separated by commas or spaces. That is, WITH (hint1), WITH (hint1, hint2, ...), and WITH (hint1 hint2 ...) are supported, while (hint1) is not.

    - Supported hints include NOLOCK, READUNCOMMITTED, UPDLOCK, REPEATABLEREAD, SERIALIZABLE, READCOMMITTED, TABLOCK, TABLOCKX, PAGLOCK, ROWLOCK, NOWAIT, READPAST, XLOCK, SNAPSHOT, and NOEXPAND.

    - If the preceding hints need to be used as identifiers, such as column names or variable names, set d_format_behavior_compat_options = 'enable_table_hint_identifier' (which defaults to '').

    - All hints are supported only for syntax compatibility and have no actual effect.

    - A corresponding NOTICE message is printed for the hints.

## table_hint Clause Examples<a name="zh-cn_topic_0283136578_zh-cn_topic_0237122106_zh-cn_topic_0059777455_s985289833081489e9d77c485755bd362"></a>

```
create table t1(c1 int);

update t1 with (xlock) set c1 = 10 where c1 = 5;
NOTICE:  The xlock option is currently ignored

update t1 with (xlock, nowait) set c1 = 3 where c1 = 10;
NOTICE:  The xlock option is currently ignored
NOTICE:  The nowait option is currently ignored

update t1 as alias_t1 with (xlock) set c1 = 20 where c1 = 2;
NOTICE:  The xlock option is currently ignored

update t1 as alias_t1 with (xlock, nowait) set c1 = 20 where c1 = 2;
NOTICE:  The xlock option is currently ignored
NOTICE:  The nowait option is currently ignored

update t1 as alias_t1 with (xlock nowait) set c1 = 20 where c1 = 2;
NOTICE:  The xlock option is currently ignored
NOTICE:  The nowait option is currently ignored
```

## Reference<a name="section156744489391"></a>

[UPDATE](https://docs.opengauss.org/en/docs/latest/sql_reference/update.html)
