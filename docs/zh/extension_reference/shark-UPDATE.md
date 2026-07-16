# UPDATE

## 功能描述<a name="zh-cn_topic_0283137651_zh-cn_topic_0237122194_zh-cn_topic_0059778969_s85747c5f88e64562a8ff9ddacda19939"></a>

更新表中的数据。UPDATE修改满足条件的所有行中指定的字段值，WHERE子句声明条件，SET子句指定的字段会被修改，没有出现的字段则保持它们的原值。

## 注意事项<a name="zh-cn_topic_0283137651_zh-cn_topic_0237122194_zh-cn_topic_0059778969_s7e9e912f472543cbb190edb83e5f22d3"></a>

- 本章节仅包含shark新增语法，原openGauss的UPDATE语法未作删除和修改。原openGauss的UPDATE语法请参考章节[UPDATE](../sql_reference/update.md)。
- 新增支持table_hint子句。

## 语法格式<a name="zh-cn_topic_0283137651_zh-cn_topic_0237122194_zh-cn_topic_0059778969_sd8d9ff15ff6c45c9aebd16c861936c07"></a>

```
单表更新：
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

多表更新：
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

- 其中table\_hint子句table\_hint\_clause为：

    ```
    WITH ( <table_hint> [, ...] ) 
    ```

## 参数说明<a name="zh-cn_topic_0283137651_zh-cn_topic_0237122194_zh-cn_topic_0059778969_sf3e3262b89854b3d829a94054116838d"></a>

- **JOIN**

    JOIN包含 INNER JOIN，LEFT JOIN，RIGHT JOIN，FULL JOIN，CROSS JOIN。
  
- **WITH ( <table_hint> [, ...] )**

    - 不同于SELECT子句，针对单个hint，WITH可选，针对UPDATE子句，WITH必选，table_hint支持给出一个列表选项，列表通过逗号或者空格分隔，即WITH (hint1)、WITH (hint1, hint2, ...)、WITH (hint1 hint2 ...)均支持，(hint1)不支持。

    - 支持的hint包括NOLOCK、READUNCOMMITTED、UPDLOCK、REPEATABLEREAD、SERIALIZABLE、READCOMMITTED、TABLOCK、TABLOCKX、PAGLOCK、ROWLOCK、NOWAIT、READPAST、XLOCK、SNAPSHOT、NOEXPAND。

    - 当上述hint需要当做标识符，用于列名、变量名等，需要设置d_format_behavior_compat_options = 'enable_table_hint_identifier'，该变量默认值d_format_behavior_compat_options = ''。

    - 所有的hint仅语法支持，无实际含义。

    - 针对hint，会打印相关NOTICE信息。

## table_hint子句示例<a name="zh-cn_topic_0283136578_zh-cn_topic_0237122106_zh-cn_topic_0059777455_s985289833081489e9d77c485755bd362"></a>

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

## 相关链接<a name="section156744489391"></a>

[UPDATE](../sql_reference/update.md)
