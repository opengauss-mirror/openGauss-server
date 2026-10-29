# INSERT

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-07-30T06:33:32.433Z pushedAt=2026-09-22T03:56:30.234Z -->

## Function Description<a name="zh-cn_topic_0283137542_zh-cn_topic_0237122167_zh-cn_topic_0059778902_s86b6c9741c7741d3976c5e358e8d5486"></a>

Adds one or more rows of data to a table.

## Notes<a name="zh-cn_topic_0283137542_zh-cn_topic_0237122167_zh-cn_topic_0059778902_sdd2da7fe44624eb99ee77013ff96c6bd"></a>

- This section contains only the syntax newly added by shark. The original openGauss syntax has not been deleted or modified. For the original openGauss INSERT syntax, see [INSERT](https://docs.opengauss.org/en/docs/latest/sql_reference/insert.html).
- The table_hint clause is newly supported.

## Syntax<a name="zh-cn_topic_0283137542_zh-cn_topic_0237122167_zh-cn_topic_0059778902_se242be9719f44731b261539dbd42d7b9"></a>

```
[ WITH [ RECURSIVE ] with_query [, ...] ]
INSERT [/*+ plan_hint */] [INTO] table_name [partition_clause] [ AS alias ] [table_hint_clause] [ ( column_name [, ...] ) ]
    { DEFAULT VALUES
    | VALUES {( { expression | DEFAULT } [, ...] ) }[, ...] 
    | query }
    [ ON DUPLICATE KEY UPDATE { NOTHING | { column_name = { expression | DEFAULT } } [, ...] [ WHERE condition ] }]
    [ RETURNING {* | {output_expression [ [ AS ] output_name ] }[, ...]} ];
```

- The table_hint clause (table_hint_clause) is:

    ```
    WITH ( <table_hint> [, ...] ) 
    ```

## Parameter Description<a name="zh-cn_topic_0283137651_zh-cn_topic_0237122194_zh-cn_topic_0059778969_sf3e3262b89854b3d829a94016854138d"></a>

- **JOIN**

    JOIN includes INNER JOIN, LEFT JOIN, RIGHT JOIN, FULL JOIN, and CROSS JOIN.

- **WITH ( <table_hint> [, ...] )**

    - Unlike the SELECT clause, where WITH is optional for a single hint, WITH is mandatory for the INSERT clause. table_hint supports a list of options, separated by commas or spaces. That is, WITH (hint1), WITH (hint1, hint2, ...), and WITH (hint1 hint2 ...) are all supported, while (hint1) is not supported.

    - Supported hints include NOLOCK, READUNCOMMITTED, UPDLOCK, REPEATABLEREAD, SERIALIZABLE, READCOMMITTED, TABLOCK, TABLOCKX, PAGLOCK, ROWLOCK, NOWAIT, READPAST, XLOCK, SNAPSHOT, and NOEXPAND.

    - When the above hints need to be used as identifiers, such as column names or variable names, set d_format_behavior_compat_options = 'enable_table_hint_identifier'. The default value of this variable is d_format_behavior_compat_options = ''.

    - All hints are supported only at the syntax level and have no actual semantic effect.

    - Relevant NOTICE messages are printed for hints.

## table_hint Clause Example<a name="zh-cn_topic_0283136578_zh-cn_topic_0237122106_zh-cn_topic_0059777455_s985289833081489e9d77c485755bd362"></a>

```
create table t1(c1 int, c2 int);

insert into t1 values(1, 2);

insert into t1 with (nowait) values(3, 4);
NOTICE:  The nowait option is currently ignored

insert into t1 with (nowait) (c1, c2) values(5, 6);
NOTICE:  The nowait option is currently ignored

insert into t1 with (xlock, nowait) values(7, 8);
NOTICE:  The xlock option is currently ignored
NOTICE:  The nowait option is currently ignored

insert into t1 as table_t1 with (nowait) (c1, c2) values(9, 10);
NOTICE:  The nowait option is currently ignored

-- no into in insert statement
insert t1 values(1, 2);

insert t1 with (nowait) values(3, 4);
NOTICE:  The nowait option is currently ignored

insert t1 with (nowait) (c1, c2) values(5, 6);
NOTICE:  The nowait option is currently ignored

insert t1 with (xlock, nowait) values(7, 8);
NOTICE:  The xlock option is currently ignored
NOTICE:  The nowait option is currently ignored

insert t1 as table_t1 with (nowait, nolock) (c1, c2) values(9, 10);
NOTICE:  The nowait option is currently ignored
NOTICE:  The nolock option is currently ignored

CREATE TABLE partition_table1
(
    WR_RETURNED_DATE_SK       INTEGER,
    WR_RETURNED_TIME_SK       INTEGER
)
PARTITION BY RANGE(WR_RETURNED_DATE_SK)
(
        PARTITION P1 VALUES LESS THAN(2450815),
        PARTITION P2 VALUES LESS THAN(2451179),
        PARTITION P8 VALUES LESS THAN(MAXVALUE)
);

insert into partition_table1 with (nolock, nowait) values(2451176, 1);
NOTICE:  The nolock option is currently ignored
NOTICE:  The nowait option is currently ignored

insert into partition_table1 partition (p1) with (nolock, nowait) values(2450000, 1);
NOTICE:  The nolock option is currently ignored
NOTICE:  The nowait option is currently ignored

insert into partition_table1 partition for (2451176) with (nolock, nowait) values(2451176, 1);
NOTICE:  The nolock option is currently ignored
NOTICE:  The nowait option is currently ignored

insert into partition_table1 partition for (2451176) as table1_alias with (nolock, nowait) values(2451176, 1);
NOTICE:  The nolock option is currently ignored
NOTICE:  The nowait option is currently ignored
```

## Related Links<a name="section156744489391"></a>

[INSERT](https://docs.opengauss.org/en/docs/latest/sql_reference/insert.html)