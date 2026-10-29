# CREATE INDEX

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-07-30T06:30:55.384Z pushedAt=2026-09-21T09:49:26.375Z -->

## Description<a name="zh-cn_topic_0283137126_zh-cn_topic_0237122076_zh-cn_topic_0059779051_s2baab5c876044795a12b5949f22d2144"></a>

Creates an index on the specified table.

Indexes can be used to improve database query performance, but improper use will lead to database performance degradation. It is recommended that an index be created only when one of the following principles is met:

- Fields that are frequently queried.
- Create indexes on join conditions. For queries involving multi-field joins, it is recommended that a composite index be created on these fields. For example, select * from t1 join t2 on t1.a=t2.a and t1.b=t2.b, a composite index can be created on the a and b fields of the t1 table.
- On the filter condition fields in the WHERE clause (especially range conditions).
- On fields that frequently appear after ORDER BY, GROUP BY, and DISTINCT.

The syntax for creating an index on a partitioned table differs from that on a regular table. Pay attention to the differences when using it. For example, parallel index creation and partial index creation are not supported on partitioned tables.

The ALGORITHM option syntax can now be specified.

## NOTE<a name="zh-cn_topic_0283136578_zh-cn_topic_0237122106_zh-cn_topic_0059777455_s31780559299b4f62bec935a2c4679b84"></a>

- This section only contains syntax newly added by shark. The original openGauss syntax has not been deleted or modified.
- The columnstore option is newly supported.

## Syntax<a name="zh-cn_topic_0283136578_zh-cn_topic_0237122106_zh-cn_topic_0059777455_sa24c1a88574742bcb5427f58f5abb732"></a>

- Creates an index on a table.

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

## Parameter Description<a name="zh-cn_topic_0283136578_zh-cn_topic_0237122106_zh-cn_topic_0059777455_s82e47e35c54c477094dcafdc90e5d85a"></a>

- **COLUMNSTORE**

    This keyword is syntax compatible with D databases, specifying the columnstore option. It serves only a syntactic purpose and has no actual functionality.

- **opt_clustered**

    The parameter value is CLUSTERED or NONCLUSTERED, which is syntax compatible with D databases and specifies the creation of a clustered or nonclustered index. It serves only a syntactic purpose and has no actual functionality.

## Examples<a name="zh-cn_topic_0283136578_zh-cn_topic_0237122106_zh-cn_topic_0059777455_s985289833081489e9d77c485755bd362"></a>

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

## Related Links<a name="section156744489391"></a>

[CREATE INDEX](https://docs.opengauss.org/en/docs/latest/sql_reference/create_index.html)