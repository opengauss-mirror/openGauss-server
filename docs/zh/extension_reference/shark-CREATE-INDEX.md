# CREATE INDEX

## 功能描述<a name="zh-cn_topic_0283137126_zh-cn_topic_0237122076_zh-cn_topic_0059779051_s2baab5c876044795a12b5949f22d2144"></a>

在指定的表上创建索引。

索引可以用来提高数据库查询性能，但是不恰当的使用将导致数据库性能下降。建议仅在匹配如下某条原则时创建索引：

- 经常执行查询的字段。
- 在连接条件上创建索引，对于存在多字段连接的查询，建议在这些字段上建立组合索引。例如，select \* from t1 join t2 on t1.a=t2.a and t1.b=t2.b，可以在t1表上的a、b字段上建立组合索引。
- where子句的过滤条件字段上（尤其是范围条件）。
- 在经常出现在order by、group by和distinct后的字段。

在分区表上创建索引与在普通表上创建索引的语法不太一样，使用时请注意，如分区表上不支持并行创建索引，不支持创建部分索引。

新增可以指定 ALGORITHM 选项语法。

## 注意事项<a name="zh-cn_topic_0283136578_zh-cn_topic_0237122106_zh-cn_topic_0059777455_s31780559299b4f62bec935a2c4679b84"></a>

- 本章节只包含shark新增的语法，原openGauss的语法未做删除和修改。
- 新增支持columnstore选项

## 语法格式<a name="zh-cn_topic_0283136578_zh-cn_topic_0237122106_zh-cn_topic_0059777455_sa24c1a88574742bcb5427f58f5abb732"></a>

- 在表上创建索引。

  ```
  CREATE [ UNIQUE ] [ opt_clustered ] [COLUMNSTORE] INDEX [ CONCURRENTLY ] [ [schema_name.]index_name ] ON table_name [ USING method ]
      ({ { column_name [ ( length ) ] | ( expression ) } [ COLLATE collation ] [ opclass ] [ ASC | DESC ] [ NULLS { FIRST | LAST } ] }[, ...] )
      [ INCLUDE ( column_name [, ...] )]    
      [ WITH ( {storage_parameter = value} [, ... ] ) ]
      [ TABLESPACE tablespace_name ]
      [ COMMENT text ]
      [ VISIBLE | INVISIBLE ]
      [ WHERE predicate ];
  ```

## 参数说明<a name="zh-cn_topic_0283136578_zh-cn_topic_0237122106_zh-cn_topic_0059777455_s82e47e35c54c477094dcafdc90e5d85a"></a>

- **COLUMNSTORE**

    该关键字为创建兼容D库的语法，指定列存选项。仅语法作用，没有实际功能。

- **opt_clustered**

    参数内容为CLUSTERED/NONCLUSTERED，兼容D库的语法，指定创建聚合/非聚合索引。仅语法作用，没有实际功能。

## 示例<a name="zh-cn_topic_0283136578_zh-cn_topic_0237122106_zh-cn_topic_0059777455_s985289833081489e9d77c485755bd362"></a>

```sql
openGauss=# create table t1 (a int);
CREATE TABLE
openGauss=# create columnstore index on t1 (a);
NOTICE:  The COLUMNSTORE option is currently ignored
CREATE INDEX

openGauss=# create table t1 (a int);
CREATE TABLE
openGauss=# create clustered index on t1 (a);
NOTICE:  The COLUMNSTORE option is currently ignored
CREATE INDEX
```

## 相关链接<a name="section156744489391"></a>

[CREATE INDEX](https://docs.opengauss.org/zh/docs/latest/sql_reference/create_index.html)
