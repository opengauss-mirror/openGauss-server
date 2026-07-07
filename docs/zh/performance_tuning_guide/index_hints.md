# INDEX HINTS

## 注意事项

兼容限制：

- 功能仅在B兼容模式下生效。
- 目前仅支持MySql数据库中完整语法的部分语法及功能。
- 语法仅在查询语句中使用此功能时生效。
- 若按照index hint name没有精确匹配到已有的index, 将按照前缀匹配查找，若有且只有一个index以hint name开头, 则认为与该index匹配。

## 功能描述

为指定的表在扫描时显示的指定期望使用的索引名称。

- 使用USE INDEX 指定的索引，会在扫描时综合考虑使用此索引扫描的代价和顺序扫描的代价，会选择代价更低的使用。

- 使用FORCE INDEX 指定的索引，如果可以使用索引扫描，会在扫描时强制使用此索引进行扫描。

- FORCE INDEX 和USE INDEX 不能同时作用在同一张表中。

- 多个index_hint 连用等价index_list 中写多个索引名字。

- IGNORE INDEX 所指定要忽略的索引的级别是更高的，意味着当IGNORE INDEX指定了某个索引要被忽略时，不会考虑是否有其他HINT指定要使用这个索引。

## 语法格式

```
tbl_name [ partition_clause ] [ [ AS ] alias ] [ index_hint_list ]

index_hint_list:
    index_hint [ index_hint ]
index_hint:
    USE { INDEX | KEY } ( [ index_list ] )
  | { FORCE | IGNORE } { INDEX | KEY } ( index_list )
index_list:
    index_name [ , index_name ] ...
```

## 参数说明

- **index_list**

  索引的名称，使用逗号分隔。当加载dolpin插件时，此处可以使用PRIMARY关键字，表示使用这个表的主键索引。

- **tbl_name**

  泛指一个表名。

## 示例

```sql
openGauss=# explain (costs off,verbose true  )select * from db_1097149_tb force key (index_1097149_4) where col2= 3 and col4 = 'a';
                        QUERY PLAN                        
----------------------------------------------------------
 Index Scan using index_1097149_4 on public.db_1097149_tb
   Output: col1, col2, col3, col4
   Index Cond: ((db_1097149_tb.col4)::text = 'a'::text)
   Filter: (db_1097149_tb.col2 = 3)
(4 rows)

openGauss=# explain (costs off) select * from db_1130449_tb IGNORE INDEX (index_1130449) where col2= 3;
        QUERY PLAN         
---------------------------
 Seq Scan on db_1130449_tb
   Filter: (col2 = 3)
(2 rows)

-- PRIMARY示例
openGauss=# explain (costs off) select a from t1 force index(primary) where a > 2 order by a;
             QUERY PLAN              
-------------------------------------
 [Bypass]
 Index Only Scan using t1_pkey on t1
   Index Cond: (a > 2)
(3 rows)

```
