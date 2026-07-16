# INSERT

## 功能描述<a name="zh-cn_topic_0283137542_zh-cn_topic_0237122167_zh-cn_topic_0059778902_s86b6c9741c7741d3976c5e358e8d5486"></a>

向表中添加一行或多行数据。

## 注意事项<a name="zh-cn_topic_0283137542_zh-cn_topic_0237122167_zh-cn_topic_0059778902_sdd2da7fe44624eb99ee77013ff96c6bd"></a>

- 本章节只包含shark新增的语法，原openGauss的语法未做删除和修改。原openGauss的INSERT语法请参考章节[INSERT](../sql_reference/INSERT.md)。
- 新增支持table_hint子句。

## 语法格式<a name="zh-cn_topic_0283137542_zh-cn_topic_0237122167_zh-cn_topic_0059778902_se242be9719f44731b261539dbd42d7b9"></a>

```
[ WITH [ RECURSIVE ] with_query [, ...] ]
INSERT [/*+ plan_hint */] [INTO] table_name [partition_clause] [ AS alias ] [table_hint_clause] [ ( column_name [, ...] ) ]
    { DEFAULT VALUES
    | VALUES {( { expression | DEFAULT } [, ...] ) }[, ...] 
    | query }
    [ ON DUPLICATE KEY UPDATE { NOTHING | { column_name = { expression | DEFAULT } } [, ...] [ WHERE condition ] }]
    [ RETURNING {* | {output_expression [ [ AS ] output_name ] }[, ...]} ];
```

- 其中table\_hint子句table\_hint\_clause为：

    ```
    WITH ( <table_hint> [, ...] ) 
    ```

## 参数说明<a name="zh-cn_topic_0283137651_zh-cn_topic_0237122194_zh-cn_topic_0059778969_sf3e3262b89854b3d829a94054116838d"></a>

- **JOIN**

    JOIN包含 INNER JOIN，LEFT JOIN，RIGHT JOIN，FULL JOIN，CROSS JOIN。
  
- **WITH ( <table_hint> [, ...] )**

    - 不同于SELECT子句，针对单个hint，WITH可选，针对INSERT子句，WITH必选，table_hint支持给出一个列表选项，列表通过逗号或者空格分隔，即WITH (hint1)、WITH (hint1, hint2, ...)、WITH (hint1 hint2 ...)均支持，(hint1)不支持。

    - 支持的hint包括NOLOCK、READUNCOMMITTED、UPDLOCK、REPEATABLEREAD、SERIALIZABLE、READCOMMITTED、TABLOCK、TABLOCKX、PAGLOCK、ROWLOCK、NOWAIT、READPAST、XLOCK、SNAPSHOT、NOEXPAND。

    - 当上述hint需要当做标识符，用于列名、变量名等，需要设置d_format_behavior_compat_options = 'enable_table_hint_identifier'，该变量默认值d_format_behavior_compat_options = ''。

    - 所有的hint仅语法支持，无实际含义。

    - 针对hint，会打印相关NOTICE信息。

## table_hint子句示例<a name="zh-cn_topic_0283136578_zh-cn_topic_0237122106_zh-cn_topic_0059777455_s985289833081489e9d77c485755bd362"></a>

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

## 相关链接<a name="section156744489391"></a>

[INSERT](../sql_reference/INSERT.md)
