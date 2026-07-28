# DELETE

## 功能描述<a name="zh-cn_topic_0283136795_zh-cn_topic_0237122131_zh-cn_topic_0059778379_se9507fb26df547a795ac7940e3a19ebf"></a>

DELETE从指定的表里删除满足WHERE子句的行。如果WHERE子句不存在，将删除表中所有行，结果只保留表结构。

## 注意事项<a name="zh-cn_topic_0283136795_zh-cn_topic_0237122131_zh-cn_topic_0059778379_sfc96c070e8574f4ea9a2726e898fda17"></a>

- 本章节仅包含shark新增语法，原openGauss的DELETE语法未作删除和修改。原openGauss的DELETE语法请参考章节[DELETE](https://docs.opengauss.org/zh/docs/latest/sql_reference/delete.html)。
- 新增支持table_hint子句。

## 语法格式<a name="zh-cn_topic_0283136795_zh-cn_topic_0237122131_zh-cn_topic_0059778379_s84baecef89484d5f87f57b0545b46202"></a>

```
单表删除：
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

多表删除：
[ WITH [ RECURSIVE ] with_query [, ...] ]
DELETE [/*+ plan_hint */] [FROM] 
    {[ ONLY ] table_name [ * ] [ [ [partition_clause] [ [ AS ] alias ] [ table_hint_clause ] ] | [ [ [ AS ] alias ] [partitions_clause] ] ]} [, ...]
    [ USING using_list ]
    [ FROM from_list 
    [JOIN join_table ON join_condition]... ]
    [ WHERE condition  ];
或
[ WITH [ RECURSIVE ] with_query [, ...] ]
DELETE [/*+ plan_hint */]
    {[ ONLY ] table_name [ * ] [ [ [partition_clause] [ [ AS ] alias ] [ table_hint_clause ] ] | [ [ [ AS ] alias ] [partitions_clause] ] ]} [, ...]
    [ FROM using_list ]
    [ WHERE condition ];
```

- 其中table\_hint子句table\_hint\_clause为：

    ```
    WITH ( <table_hint> [, ...] ) 
    ```

## 参数说明<a name="zh-cn_topic_0283136795_zh-cn_topic_0237122131_zh-cn_topic_0059778379_s6df87c0dd87c49e29a034e0ff3385ca7"></a>

- **JOIN**

    JOIN包含 INNER JOIN，LEFT JOIN，RIGHT JOIN，FULL JOIN，CROSS JOIN。

- **WITH ( <table_hint> [, ...] )**

    - 不同于SELECT子句，针对单个hint，WITH可选，针对DELETE子句，WITH必选，table_hint支持给出一个列表选项，列表通过逗号或者空格分隔，即WITH (hint1)、WITH (hint1, hint2, ...)、WITH (hint1 hint2 ...)均支持，(hint1)不支持。

    - 支持的hint包括NOLOCK、READUNCOMMITTED、UPDLOCK、REPEATABLEREAD、SERIALIZABLE、READCOMMITTED、TABLOCK、TABLOCKX、PAGLOCK、ROWLOCK、NOWAIT、READPAST、XLOCK、SNAPSHOT、NOEXPAND。

    - 当上述hint需要当做标识符，用于列名、变量名等，需要设置d_format_behavior_compat_options = 'enable_table_hint_identifier'，该变量默认值d_format_behavior_compat_options = ''。

    - 所有的hint仅语法支持，无实际含义。

    - 针对hint，会打印相关NOTICE信息。

## table_hint子句示例<a name="zh-cn_topic_0283136578_zh-cn_topic_0237122106_zh-cn_topic_0059777455_s985289833081489e9d77c485755bd362"></a>

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

## 相关链接<a name="section156744489391"></a>

[DELETE](https://docs.opengauss.org/zh/docs/latest/sql_reference/delete.html)
