# gms_stats

## gms_stats概述

gms_stats是一个基于openGauss的插件，用于良好地估计统计数据（尤其是针对较大的分区表），并能获得更好的统计结果，最终制定出速度更快的SQL执行计划。目前支持的接口有：

- `GATHER_SCHEMA_STATS`（用于收集某个schema下对象的统计信息）。
- `CREATE_STAT_TABLE`
- `DROP_STAT_TABLE`
- `GATHER_DATABASE_STATS`
- `GATHER_TABLE_STATS`
- `GATHER_INDEX_STATS`
- `DELETE_SCHEMA_STATS`
- `DELETE_TABLE_STATS`
- `DELETE_COLUMN_STATS`
- `DELETE_INDEX_STATS`
- `SET_TABLE_STATS`
- `SET_INDEX_STATS`
- `SET_COLUMN_STATS`
- `IMPORT_SCHEMA_STATS`
- `IMPORT_TABLE_STATS`
- `IMPORT_INDEX_STATS`
- `IMPORT_COLUMN_STATS`
- `EXPORT_SCHEMA_STATS`
- `EXPORT_TABLE_STATS`
- `EXPORT_INDEX_STATS`
- `EXPORT_COLUMN_STATS`
- `GET_STATS_HISTORY_AVAILABILITY`
- `GET_STATS_HISTORY_RETENTION`
- `PURGE_STATS`
- `RESTORE_SCHEMA_STATS`
- `RESTORE_TABLE_STATS`
- `LOCK_SCHEMA_STATS`
- `LOCK_PARTITION_STATS`
- `LOCK_TABLE_STATS`
- `UNLOCK_SCHEMA_STATS`
- `UNLOCK_PARTITION_STATS`
- `UNLOCK_TABLE_STATS`

## gms_stats限制

- 仅支持Create extension命令方式加载插件。

## gms_stats安装

openGauss打包编译时默认已经包含了gms_stats, 可以在安装完openGauss后，直接通过create extension gms_stats;加载插件。

## gms_stats使用

### 创建Extension<a name="section21088306113"></a>

创建gms_stats Extension可直接使用CREATE Extension命令进行创建：

```
openGauss=# CREATE Extension gms_stats;
```

### 使用Extension<a name="section107391050141118"></a>

#### 声明

- CREATE_STAT_TABLE(ownname VARCHAR22, stattab VARCHAR22, tblspace VARCHAR22 DEFAULT NULL, global_temporary BOOLEAN DEFAULT FALSE);

  **描述**：此过程在指定schema下创建用于收集统计信息的用户表。

  **参数说明**：

  - `ownname`：指定要创建用户表的schema名；
  - `stattab`：指定要创建的用户表的名称；
  - `tblspace`：指定要创建的用户表使用的tablespace名称；
  - `global_temporary`：指定要创建的用户表是否为全局临时表。

  **使用说明**：需要用户拥有在指定schema下CREATE表的权限。

- DROP_STAT_TABLE(ownname VARCHAR22, stattab VARCHAR22);

  **描述**：此过程用于删除指定shema下的用户表。

  **参数说明**：

  - `ownname`：指定要删除用户表所在的schema名；
  - `stattab`：指定要删除的用户表的名称；

  **权限**：需要用户拥有在指定schema下DROP表的权限。

- GATHER_DATABASE_STATS(estimate_percent NUMBER DEFAULT NULL, block_sample BOOLEAN DEFAULT FALSE, method_opt VARCHAR2 DEFAULT NULL, degree NUMBER DEFAULT NULL, granularity VARCHAR2 DEFAULT NULL, cascade BOOLEAN DEFAULT NULL, stattab VARCHAR2 DEFAULT NULL, statid VARCHAR2 DEFAULT NULL, options VARCHAR2 DEFAULT 'GATHER', objlist OUT ObjectTab, statown VARCHAR2 DEFAULT NULL, gather_sys BOOLEAN DEFAULT TRUE, no_invalidate BOOLEAN DEFAULT NULL, obj_filter_list ObjectTab DEFAULT NULL);

  **描述**：此过程用于收集当前database中所有对象的统计信息数据。

  **参数说明**：

  - **estimate_percent**：确定采样的行的百分比，在(0.000001 ~ 100)之间。**暂不支持该参数设置**；
  - **block_sample**：是否使用随机块抽样而不是随机行抽样。**暂不支持该参数设置**；
  - **method_opt**：收集统计信息的方法选项。**暂不支持该参数设置**；
  - **degree**：确定统计信息收集的并行度。**暂不支持该参数配置**；
  - **granularity**：要收集的统计信息的粒度(仅分区表相关)。**暂不支持该参数配置**；
  - **cascade**：确定是否收集索引统计信息作为收集统计信息的一部分。**暂不支持该参数配置**，使用固定TRUE；
  - **stattab**：指示存储统计信息的用户表表名。如果该值不为NULL，则将本次统计信息存储到该用户表中；
  - **statid**：指示存储统计信息的用户表OID。如果该值不为NULL，则将本次统计信息存储到该用户表中；
  - **statown**：指示统计信息存储的用户表所在schmea，如果为NULL，则使用当前所属schema；
  - **options**：指定哪些对象需要收集统计信息。**暂不支持该参数设置**；
  - **objlist**：指定过时的对象列表或者为空。**暂不支持该参数设置**；
  - **no_invalidate**：控制在收集统计信息时从属游标的无效。**暂不支持该参数设置**；
  - **gather_sys**：是否统计系统表数据；
  - **obj_filter_list**：对象列表。如果指定该参数（其中至少需要有一项数据），则只会收集该列表中对象的统计信息。

  **权限**：需要拥有管理员权限或当前database的owner执行此过程。

- GATHER_SCHEMA_STATS(ownname VARCHAR22, estimate_percent NUMBER DEFAULT 100, block_sample boolean DEFAULT FALSE, method_opt VARCHAR22 DEFAULT 'FOR ALL COLUMNS SIZE AUTO', degree NUMBER DEFAULT NULL, granularity VARCHAR22 DEFAULT 'GLOBAL', cascade boolean DEFAULT FALSE, stattab VARCHAR22 DEFAULT NULL, statid VARCHAR22 DEFAULT NULL, options VARCHAR22 DEFAULT 'GATHER', objlist ObjectTab DEFAULT NULL, statown VARCHAR22 DEFAULT NULL, no_invalidate boolean DEFAULT FALSE, force boolean DEFAULT FALSE, obj_filter_list objecttab DEFAULT NULL);

  **描述**：此过程用于收集指定schema下所有对象的统计信息数据。

  **参数说明**：

  - **ownname**：指定要收集统计信息的schema名称；

  - **estimate_percent**：确定采样的行的百分比，在(0.000001 ~ 100)之间。**暂不支持该参数设置**；
  - **block_sample**：是否使用随机块抽样而不是随机行抽样。**暂不支持该参数设置**；
  - **method_opt**：收集统计信息的方法选项。**暂不支持该参数设置**；
  - **degree**：确定统计信息收集的并行度。**暂不支持该参数配置**；
  - **granularity**：要收集的统计信息的粒度(仅分区表相关)。**暂不支持该参数配置**；
  - **cascade**：确定是否收集索引统计信息作为收集统计信息的一部分。**暂不支持该参数配置**，使用固定TRUE；
  - **stattab**：指示存储统计信息的用户表表名。如果该值不为NULL，则将本次统计信息存储到该用户表中；
  - **statid**：指示存储统计信息的用户表OID。如果该值不为NULL，则将本次统计信息存储到该用户表中；
  - **statown**：指示统计信息存储的用户表所在schmea，如果为NULL，则使用当前所属schema；
  - **options**：指定哪些对象需要收集统计信息。**暂不支持该参数设置**；
  - **objlist**：指定过时的对象列表或者为空。**暂不支持该参数设置**；
  - **no_invalidate**：控制在收集统计信息时从属游标的无效。**暂不支持该参数设置**；
  - **force**：指定统计对象被锁时，如何处理。若为TRUE，则统计所有对象统计信息，若为FALSE，则跳过被锁的对象；
  - **obj_filter_list**：对象列表。如果指定该参数（其中至少需要有一项数据），则只会收集该列表中对象的统计信息。

  **权限**：需要拥有管理员权限或当前database的owner执行此过程。

- GATHER_TABLE_STATS(ownname VARCHAR22, tabname VARCHAR22, partname VARCHAR22 DEFAULT NULL, estimate_percent NUMBER DEFAULT NULL, block_sample BOOLEAN DEFAULT NULL, method_opt VARCHAR22 DEFAULT NULL, degree NUMBER DEFAULT NULL, granularity VARCHAR22 DEFAULT NULL, cascade BOOLEAN DEFAULT NULL, stattab VARCHAR22 DEFAULT NULL, statid VARCHAR22 DEFAULT NULL, statown VARCHAR22 DEFAULT NULL, no_invalidate BOOLEAN DEFAULT NULL, stattype VARCHAR22 DEFAULT NULL, force BOOLEAN DEFAULT FALSE, context text DEFAULT NULL, options VARCHAR22 DEFAULT NULL);

  **描述**：此过程用于收集指定表或分区的统计信息数据。

  **参数说明**：

  - **ownname**：指定要收集统计信息的schema名称；
  - **tabname**：指定要收集统计信息的表名称；
  - **partname**：指定要收集统计信息的分区的名称；

  - **estimate_percent**：确定采样的行的百分比，在(0.000001 ~ 100)之间。**暂不支持该参数设置**；
  - **block_sample**：是否使用随机块抽样而不是随机行抽样。**暂不支持该参数设置**；
  - **method_opt**：收集统计信息的方法选项。**暂不支持该参数设置**；
  - **degree**：确定统计信息收集的并行度。**暂不支持该参数配置**；
  - **granularity**：要收集的统计信息的粒度(仅分区表相关)。**暂不支持该参数配置**；
  - **cascade**：确定是否收集索引统计信息作为收集统计信息的一部分。**暂不支持该参数配置**，使用固定TRUE；
  - **stattab**：指示存储统计信息的用户表表名。如果该值不为NULL，则将本次统计信息存储到该用户表中；
  - **statid**：指示存储统计信息的用户表OID。如果该值不为NULL，则将本次统计信息存储到该用户表中；
  - **statown**：指示统计信息存储的用户表所在schmea，如果为NULL，则使用当前所属schema；
  - **no_invalidate**：控制在收集统计信息时从属游标的无效。**暂不支持该参数设置**；
  - **stattype**：当前存储过程只支持固定值`DATA`。**暂不支持配置该参数**。
  - **force**：指定统计对象被锁时，如何处理。若为TRUE，则无论是否被锁都进行统计；若为FALSE，则在对象被锁时报错；
  - **context**：暂不支持设置。
  - **options**：指定哪些对象需要收集统计信息。**暂不支持该参数设置**。

  **权限**：需要拥有管理员权限或当前database的owner或为当前表的owner执行此过程。

- GATHER_INDEX_STATS(ownname VARCHAR2, indname VARCHAR2, partname VARCHAR2 DEFAULT NULL, estimate_percent NUMBER DEFAULT NULL, stattab VARCHAR2 DEFAULT NULL, statid VARCHAR2 DEFAULT NULL, statown VARCHAR2 DEFAULT NULL, degree NUMBER DEFAULT NULL, granularity VARCHAR2 DEFAULT NULL, no_invalidate BOOLEAN DEFAULT NULL, force BOOLEAN DEFAULT FALSE);

  **描述**：此过程用于收集指定索引所在表的统计信息。实际效果与`GATHER_TABLE_STATS`一致。

  **参数说明**：

  - **ownname**：指定要收集统计信息的schema名称；
  - **indname**：指定要收集统计信息的索引名称；
  - **partname**：指定要收集统计信息的分区的名称；**暂不支持该参数设置**；

  - **estimate_percent**：确定采样的行的百分比，在(0.000001 ~ 100)之间。**暂不支持该参数设置**；
  - **stattab**：指示存储统计信息的用户表表名。如果该值不为NULL，则将本次统计信息存储到该用户表中；
  - **statid**：指示存储统计信息的用户表OID。如果该值不为NULL，则将本次统计信息存储到该用户表中；
  - **statown**：指示统计信息存储的用户表所在schmea，如果为NULL，则使用当前所属schema；
  - **degree**：确定统计信息收集的并行度。**暂不支持该参数配置**；
  - **granularity**：要收集的统计信息的粒度(仅分区表相关)。**暂不支持该参数配置**；
  - **no_invalidate**：控制在收集统计信息时从属游标的无效。**暂不支持该参数设置**；
  - **force**：指定统计对象被锁时，如何处理。若为TRUE，则无论是否被锁都进行统计；若为FALSE，则在对象被锁时报错；

  **权限**：需要拥有管理员权限或当前database的owner或为当前表的owner执行此过程。

  **使用说明**：当前`GATHER_INDEX_STATS`实际效果与`GATHER_TABLE_STATS`效果一致。通过查找指定索引所在的表，再执行统计该表的信息。

- DELETE_SCHEMA_STATS(ownname VARCHAR2, stattab VARCHAR2 DEFAULT NULL, statid VARCHAR2 DEFAULT NULL, statown VARCHAR2 DEFAULT NULL, no_invalidate BOOLEAN DEFAULT NULL, force BOOLEAN DEFAULT FALSE, stat_category VARCHAR2 DEFAULT NULL);

  **描述**：此过程用于删除指定schema下所有对象的统计信息。

  **参数说明**：

  - **ownname**：指定要删除统计信息的schema名称；
  - **stattab**：指示存储统计信息的用户表表名。如果该值不为NULL，则将本次删除该用户表中的统计信息；
  - **statid**：指示存储统计信息的用户表OID。如果该值不为NULL，则将本次删除该用户表中的统计信息；
  - **statown**：指示统计信息存储的用户表所在schmea，如果为NULL，则使用当前所属schema；
  - **no_invalidate**：控制在收集统计信息时从属游标的无效。**暂不支持该参数设置**；
  - **force**：指定统计对象被锁时，如何处理。若为TRUE，则删除所有对象统计信息，若为FALSE，则跳过被锁的对象；
  - **stat_category**：要删除的统计数据。**暂不支持该参数设置**。

  **权限**：需要拥有管理员权限或当前database的owner执行此过程。

- DELETE_TABLE_STATS(ownname VARCHAR2, tabname VARCHAR2, partname VARCHAR2 DEFAULT NULL, stattab VARCHAR2 DEFAULT NULL, statid VARCHAR2 DEFAULT NULL, cascade_parts BOOLEAN DEFAULT TRUE, cascade_columns BOOLEAN DEFAULT TRUE, cascade_indexes BOOLEAN DEFAULT TRUE, statown VARCHAR2 DEFAULT NULL, no_invalidate BOOLEAN DEFAULT NULL, force BOOLEAN DEFAULT FALSE, stat_category VARCHAR2 DEFAULT NULL);

  **描述**：此过程用于删除指定schema下指定表的统计信息。

  **参数说明**：

  - **ownname**：指定要删除统计信息的schema名称；
  - **tabname**：指定要删除统计信息的表名称；
  - **partname**：指定要删除统计信息的分区名称；
  - **stattab**：指示存储统计信息的用户表表名。如果该值不为NULL，则将本次删除该用户表中的统计信息；
  - **statid**：指示存储统计信息的用户表OID。如果该值不为NULL，则将本次删除该用户表中的统计信息；
  - **cascade_parts**：表明是否要再分区上操作；
  - **cascade_columns**：表明是否删除表中的列相关统计信息。
  - **cascade_indexs**：表明是否删除表中的列相关统计信息。
  - **statown**：指示统计信息存储的用户表所在schmea，如果为NULL，则使用当前所属schema；
  - **no_invalidate**：控制在收集统计信息时从属游标的无效。**暂不支持该参数设置**；
  - **force**：指定统计对象被锁时，如何处理。若为TRUE，则删除指定对象统计信息，若为FALSE，则在对象被锁时报错；
  - **stat_category**：要删除的统计数据。**暂不支持该参数设置**。

  **权限**：需要拥有管理员权限或当前database的owner或为当前表的owner执行此过程。

  **使用说明**：

  - 当前指定**partname**时，只会删除分区表pg_partition相关的统计信息，不会删除pg_statistic中存储的分区统计信息。

- DELETE_COLUMN_STATS(ownname VARCHAR2, tabname VARCHAR2, colname VARCHAR2, partname VARCHAR2 DEFAULT NULL, stattab VARCHAR2 DEFAULT NULL, statid VARCHAR2 DEFAULT NULL, cascade_parts BOOLEAN DEFAULT TRUE, statown VARCHAR2 DEFAULT NULL, no_invalidate BOOLEAN DEFAULT NULL, force BOOLEAN DEFAULT FALSE, col_stat_type VARCHAR22 DEFAULT 'ALL');

  **描述**：此过程用于删除指定schema下指定表指定列的统计信息。

  **参数说明**：

  - **ownname**：指定要删除统计信息的schema名称；
  - **tabname**：指定要删除统计信息的表名称；
  - **partname**：指定要删除统计信息的分区名称；**暂不支持该参数设置**；
  - **stattab**：指示存储统计信息的用户表表名。如果该值不为NULL，则将本次删除该用户表中的统计信息；
  - **statid**：指示存储统计信息的用户表OID。如果该值不为NULL，则将本次删除该用户表中的统计信息；
  - **cascade_parts**：表明是否要再分区上操作；
  - **statown**：指示统计信息存储的用户表所在schmea，如果为NULL，则使用当前所属schema；
  - **no_invalidate**：控制在收集统计信息时从属游标的无效。**暂不支持该参数设置**；
  - **force**：指定统计对象被锁时，如何处理。若为TRUE，则删除指定对象统计信息，若为FALSE，则在对象被锁时报错；
  - **col_stat_type**：要删除的列统计数据。**暂不支持该参数设置**。

  **权限**：需要拥有管理员权限或当前database的owner或为当前表的owner执行此过程。

- DELETE_INDEX_STATS(ownname VARCHAR2, indname VARCHAR2, partname VARCHAR2 DEFAULT NULL, stattab VARCHAR2 DEFAULT NULL, statid VARCHAR2 DEFAULT NULL, cascade_parts BOOLEAN DEFAULT TRUE, statown VARCHAR2 DEFAULT NULL, no_invalidate BOOLEAN DEFAULT NULL, stattype VARCHAR2 DEFAULT 'ALL', force BOOLEAN DEFAULT FALSE, stat_category VARCHAR2 DEFAULT NULL);

  **描述**：此过程用于删除指定schema下指定索引的统计信息。

  **参数说明**：

  - **ownname**：指定要删除统计信息的schema名称；
  - **indname**：指定要删除统计信息的索引名称；
  - **partname**：指定要删除统计信息的分区名称；**暂不支持该参数设置**；
  - **stattab**：指示存储统计信息的用户表表名。如果该值不为NULL，则将本次删除该用户表中的统计信息；
  - **statid**：指示存储统计信息的用户表OID。如果该值不为NULL，则将本次删除该用户表中的统计信息；
  - **cascade_parts**：表明是否要再分区上操作；
  - **statown**：指示统计信息存储的用户表所在schmea，如果为NULL，则使用当前所属schema；
  - **no_invalidate**：控制在收集统计信息时从属游标的无效。**暂不支持该参数设置**；
  - **stattype**：当前存储过程只支持固定值`DATA`。**暂不支持配置该参数**。
  - **force**：指定统计对象被锁时，如何处理。若为TRUE，则删除指定对象统计信息，若为FALSE，则在对象被锁时报错；
  - **stat_category**：要删除的统计数据。**暂不支持该参数设置**。

  **权限**：需要拥有管理员权限或当前database的owner或为当前表的owner执行此过程。

  **使用说明**：

  - 删除索引统计信息，当前是删除索引所对应列的统计信息。

- SET_TABLE_STATS(ownname VARCHAR2, tabname VARCHAR2, partname VARCHAR2 DEFAULT NULL, stattab VARCHAR2 DEFAULT NULL, statid VARCHAR2 DEFAULT NULL, numrows NUMBER DEFAULT NULL, numblks NUMBER DEFAULT NULL, avgrlen NUMBER DEFAULT NULL, flags NUMBER DEFAULT NULL, statown VARCHAR2 DEFAULT NULL, no_invalidate BOOLEAN DEFAULT NULL, cachedblk NUMBER DEFAULT NULL, cachehit NUMBER DEFAULT NULL, force BOOLEAN DEFAULT FALSE, im_imcu_count NUMBER DEFAULT NULL, im_block_count NUMBER DEFAULT NULL, scanrate NUMBER DEFAULT NULL);

  **描述**：此过程用于修改统计信息表或分区相关统计信息。

  **参数说明**：

  - **ownname**：指定要修改统计信息的schema名称；
  - **tabname**：指定要修改统计信息的表名称；
  - **partname**：指定要修改统计信息的分区名称；
  - **stattab**：指示存储统计信息的用户表表名。如果该值不为NULL，则将本次修改该用户表中的统计信息；
  - **statid**：指示存储统计信息的用户表OID。如果该值不为NULL，则将本次修改该用户表中的统计信息；
  - **numrows**：指定要修改的表的行数；
  - **numblks**：指定要修改表的block的行数；
  - **avgrlen**：修改表平均每行长度。**暂不支持该参数**。
  - **flags**：内部参数。**暂不支持该参数**。
  - **cachedblk**：内部参数。**暂不支持该参数**。
  - **cachehit**：内部参数。**暂不支持该参数**。
  - **force**：指定统计对象被锁时，如何处理。若为TRUE，则修改指定对象统计信息，若为FALSE，则在对象被锁时报错；
  - **im_imcu_count**：修改im_imcu_count列数据。**暂不支持该参数**。
  - **im_block_count**：修改im_block_count列数据。**暂不支持该参数**。
  - **scanrate**：数据库扫描外部表的速率，单位为MB/s。此参数仅与外部表相关。**暂不支持该参数**。

  **权限**：需要拥有管理员权限或当前database的owner或为当前表的owner执行此过程。

- SET_INDEX_STATS(ownname VARCHAR2, indname VARCHAR2, partname VARCHAR2 DEFAULT NULL, stattab VARCHAR2 DEFAULT NULL, statid VARCHAR2 DEFAULT NULL, numrows NUMBER DEFAULT NULL, numlblks NUMBER DEFAULT NULL, numdist NUMBER DEFAULT NULL, avglblk NUMBER DEFAULT NULL, avgdblk NUMBER DEFAULT NULL, clstfct NUMBER DEFAULT NULL, indlevel NUMBER DEFAULT NULL, flags NUMBER DEFAULT NULL, statown VARCHAR2 DEFAULT NULL, no_invalidate BOOLEAN DEFAULT NULL, guessq NUMBER DEFAULT NULL, cachedblk NUMBER DEFAULT NULL, cachehit NUMBER DEFUALT NULL, force BOOLEAN DEFAULT FALSE);

  **描述**：此过程用于修改索引相关统计信息。

  **参数说明**：

  - **ownname**：指定要修改统计信息的schema名称；
  - **indname**：指定要修改统计信息的索引名称；
  - **partname**：指定要修改统计信息的分区名称；**暂不支持该参数**。
  - **stattab**：指示存储统计信息的用户表表名。如果该值不为NULL，则将本次修改该用户表中的统计信息；
  - **statid**：指示存储统计信息的用户表OID。如果该值不为NULL，则将本次修改该用户表中的统计信息；
  - **numrows**：指定要修改的表的行数；**暂不支持该参数**；
  - **numblks**：指定要修改表的block的行数；**暂不支持该参数**；
  - **numdist**：指定去重之后索引key的数量；
  - **avglblk**：对于这个索引(分区)，每个不同的键出现在叶子块中的平均整数个数，如果没有提供，则该值从 `numlblks` 和 `numdist` 推导而来。**暂不支持该参数**；
  - **avgdblk**：对于这个索引(分区)，一个不同的键指向表中数据块的平均整数值，如果没有提供，则该值从 `clstfct` 和 `numdist` 推导而来。**暂不支持该参数**；
  - **clstfct**：指示表中基于索引值的行的顺序量。**暂不支持该参数**；
  - **indlevel**：索引的高度。**暂不支持该参数**；
  - **flags**：内部参数。**暂不支持该参数**；
  - **statown**：指示统计信息存储的用户表所在schmea，如果为NULL，则使用当前所属schema；
  - **no_invalidate**：控制在收集统计信息时从属游标的无效。**暂不支持该参数设置**；
  - **guessq**：对于索引组织的表上的辅助索引，有效猜测的行所占的百分比。**暂不支持该参数**；
  - **cachedblk**：内部参数。**暂不支持该参数**。
  - **cachehit**：内部参数。**暂不支持该参数**。
  - **force**：指定统计对象被锁时，如何处理。若为TRUE，则修改指定对象统计信息，若为FALSE，则在对象被锁时报错；

  **权限**：需要拥有管理员权限或当前database的owner或为当前表的owner执行此过程。

  **使用说明**：

  - 当前该过程实际是修改索引所对应的列的统计信息。

- SET_COLUMN_STATS(ownname VARCHAR2, tabname VARCHAR2, colname VARCHAR2, partname VARCHAR2 DEFAULT NULL, stattab VARCHAR2 DEFAULT NULL, statid VARCHAR2 DEFAULT NULL, distcnt NUMBER DEFAULT NULL, density NUMBER DEFAULT NULL, nullcnt NUMBER DEFAULT NULL, srec text DEFAULT NULL, avgclen NUMBER DEFAULT NULL, flags NUMBER DEFAULT NULL, statown VARCHAR2 DEFAULT NULL, no_invalidate BOOLEAN DEFAULT NULL, force BOOLEAN DEFAULT FALSE);

  **描述**：此过程用于修改指定列相关统计信息。

  **参数说明**：

  - **ownname**：指定要修改统计信息的schema名称；
  - **tabname**：指定要修改统计信息的表名称；
  - **colname**：指定要修改统计信息的列的名称；
  - **partname**：指定要修改统计信息的分区名称；**暂不支持该参数**。
  - **stattab**：指示存储统计信息的用户表表名。如果该值不为NULL，则将本次修改该用户表中的统计信息；
  - **statid**：指示存储统计信息的用户表OID。如果该值不为NULL，则将本次修改该用户表中的统计信息；
  - **distcnt**：指定要修改的行去重后的行数；
  - **density**：列数据. 如果此值为NULL且`distcnt`不为NULL，则密度由distcnt派生。**暂不支持该参数**；
  - **nullcnt**：指定要修改的行NULL值的数量；
  - **srec**：`StatRec`类型的记录，包含列的统计信息，如最小值、最大值等。**暂不支持该参数**。
  - **avgclen**：修改表平均每行长度。**暂不支持该参数**；
  - **flags**：内部参数。**暂不支持该参数**。
  - **statown**：指示统计信息存储的用户表所在schmea，如果为NULL，则使用当前所属schema；
  - **no_invalidate**：控制在收集统计信息时从属游标的无效。**暂不支持该参数设置**；
  - **force**：指定统计对象被锁时，如何处理。若为TRUE，则修改指定对象统计信息，若为FALSE，则在对象被锁时报错；

  **权限**：需要拥有管理员权限或当前database的owner或为当前表的owner执行此过程。

- IMPORT_SCHEMA_STATS(ownname VARCHAR2, stattab VARCHAR2, statid VARCHAR2 DEFAULT NULL, statown VARCHAR2 DEFAULT NULL, no_invalidate BOOLEAN DEFAULT NULL, force BOOLEAN DEFAULT FALSE, stat_category VARCHAR2 DEFAULT NULL);

  **描述**：此过程用于从指定用户表中将指定schema下的统计信息导入到系统表中。

  - **ownname**：指定要导入统计信息的schema名称；
  - **stattab**：指示存储统计信息的用户表表名，从该用户表中导入统计信息；
  - **statid**：指示存储统计信息的用户表OID。如果该值不为NULL，从该用户表中导入统计信息；
  - **statown**：指示统计信息存储的用户表所在schmea，如果为NULL，则使用当前所属schema；
  - **no_invalidate**：控制在收集统计信息时从属游标的无效。**暂不支持该参数设置**；
  - **force**：指定统计对象被锁时，如何处理。若为TRUE，则导入指定对象统计信息，若为FALSE，则在对象被锁时跳过；
  - **stat_category**：要导入的统计数据。**暂不支持该参数设置**。

  **权限**：需要拥有管理员权限或当前database的owner执行此过程。

- IMPORT_TABLE_STATS(ownname VARCHAR2, tabname VARCHAR2, partname VARCHAR2 DEFAULT NULL, stattab VARCHAR2, statid VARCHA2 DEFAULT NULL, cascade BOOLEAN DEFAULT TRUE, statown VARCHAR2 DEFAULT NULL, no_invalidate BOOLEAN DEFAULT NULL, force BOOLEAN DEFAULT FALSE, stat_category VARCHAR2 DEFAULT NULL);

  **描述**：此过程用于从指定用户表中将指定表的统计信息导入到系统表中。

  **参数说明**：

  - **ownname**：指定要导入统计信息的schema名称；
  - **tabname**：指定要导入统计信息的表的名称；
  - **partname**：指定要导入的统计信息的分区的名称；
  - **stattab**：指示存储统计信息的用户表表名，从该用户表中导入统计信息；
  - **statid**：指示存储统计信息的用户表OID，如果该值不为NULL，从该用户表中导入统计信息；
  - **cascade**：是否导出索引相关统计信息；
  - **statown**：指示统计信息存储的用户表所在schmea，如果为NULL，则使用当前所属schema；
  - **no_invalidate**：控制在收集统计信息时从属游标的无效。**暂不支持该参数设置**；
  - **force**：指定统计对象被锁时，如何处理。若为TRUE，则导入指定对象统计信息，若为FALSE，则在对象被锁时跳过；
  - **stat_category**：要导入的统计数据。**暂不支持该参数设置**。

  **权限**：需要拥有管理员权限或当前database的owner或为当前表的owner执行此过程。

  **使用说明**：

  - 如果partname不为NULL时，当前不对pg_statistic系统表中的分区统计信息做处理。

- IMPORT_INDEX_STATS(ownname VARCHAR2, indname VARCHAR2, partname VARCHAR2 DEFAULT NULL, stattab VARCHAR2, statid VARCHAR2 DEFAULT NULL, statown VARCHAR2 DEFAULT NULL, no_invalidate BOOLEAN DEFAULT NULL, force BOOLEAN DEFAULT FALSE);

  **描述**：此过程用于从指定用户表中将指定索引的统计信息导入到系统表中。

  **参数说明**：

  - **ownname**：指定要导入统计信息的schema名称；
  - **indname**：指定要导入统计信息的索引的名称；
  - **partname**：指定要导入的统计信息的分区的名称；**暂不支持该参数设置**；
  - **stattab**：指示存储统计信息的用户表表名，从该用户表中导入统计信息；
  - **statid**：指示存储统计信息的用户表OID，如果该值不为NULL，从该用户表中导入统计信息；
  - **statown**：指示统计信息存储的用户表所在schmea，如果为NULL，则使用当前所属schema；
  - **no_invalidate**：控制在收集统计信息时从属游标的无效。**暂不支持该参数设置**；
  - **force**：指定统计对象被锁时，如何处理。若为TRUE，则导入指定对象统计信息，若为FALSE，则在对象被锁时跳过；

  **权限**：需要拥有管理员权限或当前database的owner或为当前表的owner执行此过程。

- IMPORT_COLUMN_STATS(ownname VARCHAR2, tabname VARCHAR2, colname VARCHAR2, partname VARCHAR2 DEFAULT NULL, stattab VARCHAR2, statid VARCHA2 DEFAULT NULL, statown VARCHAR2 DEFAULT NULL, no_invalidate BOOLEAN DEFAULT NULL, force BOOLEAN DEFAULT FALSE);

  **描述**：此过程用于从指定用户表中将指定列的统计信息导入到系统表中。

  **参数说明**：

  - **ownname**：指定要导入统计信息的schema名称；
  - **tabname**：指定要导入统计信息的表的名称；
  - **colname**：指定要导入统计信息的列的名称；
  - **partname**：指定要导入的统计信息的分区的名称；**暂不支持该参数设置**；
  - **stattab**：指示存储统计信息的用户表表名，从该用户表中导入统计信息；
  - **statid**：指示存储统计信息的用户表OID，如果该值不为NULL，从该用户表中导入统计信息；
  - **statown**：指示统计信息存储的用户表所在schmea，如果为NULL，则使用当前所属schema；
  - **no_invalidate**：控制在收集统计信息时从属游标的无效。**暂不支持该参数设置**；
  - **force**：指定统计对象被锁时，如何处理。若为TRUE，则导入指定对象统计信息，若为FALSE，则在对象被锁时跳过；

  **权限**：需要拥有管理员权限或当前database的owner或为当前表的owner执行此过程。

- EXPORT_SCHEMA_STATS(ownname VARCHAR2, stattab VARCHAR2, statid VARCHAR2 DEFAULT NULL, statown VARCHAR2 DEFAULT NULL, stat_category VARCHAR22 DEFAULT NULL);

  **描述**：此过程用于将指定schema下的统计信息导出到指定用户表中。

  **参数说明**：

  - **ownname**：指定要导入统计信息的schema名称；
  - **stattab**：指示存储统计信息的用户表表名，本次导出该用户表中的统计信息；
  - **statid**：指示存储统计信息的用户表OID，如果该值不为NULL，本次导出该用户表中的统计信息；
  - **statown**：指示统计信息存储的用户表所在schmea，如果为NULL，则使用当前所属schema；
  - **stat_category**：要导出的统计数据。**暂不支持该参数设置**。

  **权限**：需要拥有管理员权限或当前database的owner执行此过程。

- EXPORT_TABLE_STATS(ownname VARCHAR2, tabname VARCHAR2, partname VARCHAR2 DEFAULT NULL, stattab VARCHAR2, statid VARCHA2 DEFAULT NULL, cascade BOOLEAN DEFAULT TRUE, statown VARCHAR2 DEFAULT NULL, stat_category VARCHAR2 DEFAULT NULL);

  **描述**：该过程导出指定schema下的指定表的统计信息到指定用户表中。

  **参数说明**：

  - **ownname**：指定要导出统计信息的schema名称；
  - **tabname**：指定要导出统计信息的表的名称；
  - **partname**：指定要导出的统计信息的分区的名称；
  - **stattab**：指示存储统计信息的用户表表名，本次导出该用户表中的统计信息；
  - **statid**：指示存储统计信息的用户表OID，如果该值不为NULL，本次导出该用户表中的统计信息；
  - **cascade**：是否导出索引相关统计信息；
  - **statown**：指示统计信息存储的用户表所在schmea，如果为NULL，则使用当前所属schema；
  - **stat_category**：要导入的统计数据。**暂不支持该参数设置**。

  **权限**：需要拥有管理员权限或当前database的owner或为当前表的owner执行此过程。

  **使用说明**：

  - 如果partname不为NULL时，当前不对pg_statistic系统表中的分区统计信息做处理。****

- EXPORT_INDEX_STATS(ownname VARCHAR2, indname VARCHAR2, partname VARCHAR2 DEFAULT NULL, stattab VARCHAR2, statid VARCHAR2 DEFAULT NULL, statown VARCHAR2 DEFAULT NULL);

  **描述**：该过程导出指定schema下的指定索引的统计信息到指定用户表中。

  **参数说明**：

  - **ownname**：指定要导出统计信息的schema名称；
  - **indname**：指定要导出统计信息的索引的名称；
  - **partname**：指定要导出的统计信息的分区的名称；**暂不支持该参数设置**;
  - **stattab**：指示存储统计信息的用户表表名，本次导出该用户表中的统计信息；
  - **statid**：指示存储统计信息的用户表OID，如果该值不为NULL，本次导出该用户表中的统计信息；
  - **statown**：指示统计信息存储的用户表所在schmea，如果为NULL，则使用当前所属schema；

  **权限**：需要拥有管理员权限或当前database的owner或为当前表的owner执行此过程。

  **使用说明**：

  - 当前导出索引统计信息，实际是导出索引对应列的统计信息。

- EXPORT_COLUMN_STATS(ownname VARCHAR2, tabname VARCHAR2, colname VARCHAR2, partname VARCHAR2 DEFAULT NULL, stattab VARCHAR2, statid VARCHAR2 DEFAULT NULL, statown VARCHAR2 DEFAULT NULL);

  **描述**：该过程导出指定schema下的指定表下指定列的统计信息到指定用户表中。

  **参数说明**：

  - **ownname**：hui指定要导出统计信息的schema名称；
  - **tabname**：指定要导出统计信息的表的名称；
  - **colname**：指定要导出统计信息的列的名称；
  - **partname**：指定要导出的统计信息的分区的名称；**暂不支持该参数设置**;
  - **stattab**：指示存储统计信息的用户表表名，本次导出该用户表中的统计信息；
  - **statid**：指示存储统计信息的用户表OID，如果该值不为NULL，本次导出该用户表中的统计信息；
  - **statown**：指示统计信息存储的用户表所在schmea，如果为NULL，则使用当前所属schema；

  **权限**：需要拥有管理员权限或当前database的owner或为当前表的owner执行此过程。

- GET_STATS_HISTORY_AVAILABILITY() RETURN TIMESTAMP WITH TIMEZONE;

  **描述**：查询并返回最早可用的统计信息时间戳。

- GET_STATS_HISTORY_RETENTION() RETURN NUMBER;

  **描述**：返回当前数据库中统计信息历史数据的保留天数。

- PURGE_STATS(before_timestamp TIMESTAMP WITH TIME ZONE);

  **描述**：从系统表pg_statistic_history中删除指定时间戳之前的统计信息历史数据。

  **参数说明**：

  - **as_of_timestamp**：在此时间戳之前保存的统计版本将被清除。如果为NULL，则使用自动清除策略（删除系统表pg_statistic_history中统计时间位于`当前时间 - GET_STATS_HISTORY_RETENTION()`之前的数据）。

  **权限**：需要拥有管理员权限或当前database的owner执行此过程。

- RESTORE_SCHEMA_STATS(ownname VARCHAR2, as_of_timestamp TIMESTAMP WITH TIME ZONE, force BOOLEAN DEFAULT FALSE, no_invalidate BOOLEAN DEFAULT NULL);

  **描述**：恢复指定schema中的所有对象的统计信息到指定的时间点。

  **参数说明**：

  - **ownname**：指定要恢复统计信息的schema名称；
  - **as_of_timestamp**：指定要恢复统计信息的时间点；
  - **force**：指定统计对象被锁时，如何处理。若为TRUE，则修改指定对象统计信息，若为FALSE，则跳过被锁对象；
  - **no_invalidate**：控制在收集统计信息时从属游标的无效。**暂不支持该参数设置**；

  **权限**：需要拥有管理员权限或当前database的owner执行此过程。

- RESTORE_TABLE_STATS(ownname VARCHAR2, tabname VARCHAR2, as_of_timestamp TIMESTAMP WITH TIME ZONE, restore_cluster_index BOOLEAN DEFAULT FALSE, force BOOLEAN DEFAULT FALSE, no_invalidate BOOLEAN DEFAULT NULL);

  **描述**：恢复指定schema中指定表的统计信息到指定的时间点。

  **参数说明**：

  - **ownname**：指定要恢复统计信息的schema名称；
  - **tabname**：指定要恢复统计信息的表的名称；
  - **as_of_timestamp**：指定要恢复统计信息的时间点；
  - **restore_cluster_index**：如果表是集群的一部分，如果设置为TRUE，则恢复集群索引的统计信息。**暂不支持该参数**。
  - **force**：指定统计对象被锁时，如何处理。若为TRUE，则修改指定对象统计信息，若为FALSE，则跳过被锁对象；
  - **no_invalidate**：控制在收集统计信息时从属游标的无效。**暂不支持该参数设置**；

  **权限**：需要拥有管理员权限或当前database的owner或为当前表的owner执行此过程。

  **使用说明**：

  - 如果恢复点该表已锁，则恢复后该表仍然是已锁状态。

- LOCK_SCHEMA_STATS(ownname VARCHAR2);

  **描述**：此过程对指定schema下的所有对象进行统计信息收集操作加锁。

  **参数说明**：

  - **ownname**：指定要加锁统计信息的schema名称；

- LOCK_TABLE_STATS(ownname VARCHAR2, tabname VARCHAR2);

  **描述**：此过程对指定schema下指定表的统计信息操作加锁。

  **参数说明**：

  - **ownname**：指定要加锁统计信息的schema名称；
  - **tabname**：指定要加锁统计信息的表的名称；

- LOCK_PARTITION_STATS(ownname VARCHAR2, tabname VARCHAR2, partname VARCHAR2);

  **描述**：此过程对指定schema下指定表下的指定分区的统计信息操作加锁。

  **参数说明**：

  - **ownname**：指定要加锁统计信息的schema名称；
  - **tabname**：指定要加锁统计信息的表的名称；
  - **partname**：指定要加锁统计信息的分区的名称；

- UNLOCK_SCHEMA_STATS(ownname VARCHAR2);

  **描述**：此过程对指定schema下的所有对象进行统计信息收集操作解锁。

  **参数说明**：

  - **ownname**：指定要解锁统计信息的schema名称；

- UNLOCK_TABLE_STATS(ownname VARCHAR2, tabname VARCHAR2);

  **描述**：此过程对指定schema下指定表的统计信息操作解锁。

  **参数说明**：

  - **ownname**：指定要解锁统计信息的schema名称；
  - **tabname**：指定要解锁统计信息的表的名称；

- UNLOCK_PARTITION_STATS(ownname VARCHAR2, tabname VARCHAR2, partname VARCHAR2);

  **描述**：此过程对指定schema下指定表下的指定分区的统计信息操作解锁。

  **参数说明**：

  - **ownname**：指定要解锁统计信息的schema名称；
  - **tabname**：指定要解锁统计信息的表的名称；
  - **partname**：指定要解锁统计信息的分区的名称；

#### 使用

- 准备数据

```sql
create schema sc_stats;
set current_schema = sc_stats;

create table t_stats (id int, c2 text, c3 char(1), constraint t_stats_pk primary key (id));
insert into t_stats values (generate_series(1, 100), 'aabbcc', 'Y');
insert into t_stats values (generate_series(101, 200), '123dfg', 'N');
insert into t_stats values (generate_series(201, 300), '人面桃花相映红', 'N');
insert into t_stats values (generate_series(301, 400), 'fortunate', 'Y');
insert into t_stats values (generate_series(401, 500), 'open@gauss', 'Y');
insert into t_stats values (generate_series(501, 600), '127.0.0.1', 'N');
insert into t_stats values (generate_series(601, 700), '!@#$!%#!', 'N');
insert into t_stats values (generate_series(701, 800), '[1,2,3,4]', 'Y');
insert into t_stats values (generate_series(801, 900), '{"name":"张三","age":18}', 'Y');
insert into t_stats values (generate_series(901, 1000), '', 'N');

create table t_part(c1 int, c2 char(1), c3 text)
partition by list(c2) (
    partition t_part_list_r values ('r'),
    partition t_part_list_v values ('v'),
    partition t_part_list_i values ('i')
);
insert into t_part values (generate_series(1, 100), 'r', 'aabbcc');
insert into t_part values (generate_series(101, 200), 'v', '123dfg');
insert into t_part values (generate_series(201, 300), 'i', '人面桃花相映红');
insert into t_part values (generate_series(301, 400), 'r', 'fortunate');
insert into t_part values (generate_series(401, 500), 'v', 'open@gauss');
insert into t_part values (generate_series(501, 600), 'i', '127.0.0.1');
insert into t_part values (generate_series(601, 700), 'r', '!@#$!%#!');
insert into t_part values (generate_series(701, 800), 'v', '{"name":"张三","age":18}');
insert into t_part values (generate_series(801, 900), 'i', '');
insert into t_part values (generate_series(901, 920), 'r', 'Hello');
insert into t_part values (generate_series(921, 960), 'v', 'Kitty');
insert into t_part values (generate_series(961, 1000), 'v', 'Cats');
insert into t_part values (1001, 'i', 'Dog');

create table t_sub_part(c1 int, c2 char(1), c3 varchar2(100))
partition by range(c1) subpartition by list(c2) (
    partition p_less_300 values less than(300) (
        subpartition subp_less_300_r values ('r'),
        subpartition subp_less_300_v values ('v'),
        subpartition subp_less_300_i values ('i')
    ),
    partition p_less_600 values less than(600) (
        subpartition subp_less_600_r values ('r'),
        subpartition subp_less_600_v values ('v'),
        subpartition subp_less_600_i values ('i')
    ),
    partition p_max values less than(maxvalue) (
        subpartition subp_max_r values ('r'),
        subpartition subp_max_v values ('v'),
        subpartition subp_max_i values ('i')
    )
);
insert into t_sub_part values (generate_series(1, 100), 'r', 'aabbcc');
insert into t_sub_part values (generate_series(101, 200), 'v', '123dfg');
insert into t_sub_part values (generate_series(201, 300), 'i', '人面桃花相映红');
insert into t_sub_part values (generate_series(301, 400), 'r', 'fortunate');
insert into t_sub_part values (generate_series(401, 500), 'v', 'open@gauss');
insert into t_sub_part values (generate_series(501, 600), 'i', '127.0.0.1');
insert into t_sub_part values (generate_series(601, 700), 'r', '!@#$!%#!');
insert into t_sub_part values (generate_series(701, 800), 'v', '{"name":"张三","age":18}');
insert into t_sub_part values (generate_series(801, 900), 'i', '');
insert into t_sub_part values (generate_series(901, 920), 'r', 'Hello');
insert into t_sub_part values (generate_series(921, 960), 'v', 'Kitty');
insert into t_sub_part values (generate_series(961, 1000), 'v', 'Cats');
insert into t_sub_part values (1001, 'i', 'Dog');

create table t_stats_us (c1 int, c2 text, c3 char(1), constraint t_stats_us_pk primary key (c1))
with (storage_type=ustore);
insert into t_stats_us values (generate_series(1, 100), 'aabbcc', 'Y');
insert into t_stats_us values (generate_series(101, 200), '123dfg', 'N');
insert into t_stats_us values (generate_series(201, 300), '人面桃花相映红', 'N');
insert into t_stats_us values (generate_series(301, 400), 'fortunate', 'Y');
insert into t_stats_us values (generate_series(401, 500), 'open@gauss', 'Y');
insert into t_stats_us values (generate_series(501, 600), '127.0.0.1', 'N');
insert into t_stats_us values (generate_series(601, 700), '!@#$!%#!', 'N');
insert into t_stats_us values (generate_series(701, 800), '[1,2,3,4]', 'Y');
insert into t_stats_us values (generate_series(801, 900), '{"name":"张三","age":18}', 'Y');
insert into t_stats_us values (generate_series(901, 1000), '', 'N');

create table t_stats_col (c1 int, c2 text, c3 char(1), constraint t_stats_col_pk primary key (c1))
with (orientation = column);

insert into t_stats_col values (generate_series(1, 100), 'aabbcc', 'Y');
insert into t_stats_col values (generate_series(101, 200), '123dfg', 'N');
insert into t_stats_col values (generate_series(201, 300), '人面桃花相映红', 'N');
insert into t_stats_col values (generate_series(301, 400), 'fortunate', 'Y');
insert into t_stats_col values (generate_series(401, 500), 'open@gauss', 'Y');
insert into t_stats_col values (generate_series(501, 600), '127.0.0.1', 'N');
insert into t_stats_col values (generate_series(601, 700), '!@#$!%#!', 'N');
insert into t_stats_col values (generate_series(701, 800), '[1,2,3,4]', 'Y');
insert into t_stats_col values (generate_series(801, 900), '{"name":"张三","age":18}', 'Y');
insert into t_stats_col values (generate_series(901, 1000), '', 'N');
```

- CREATE_STAT_TABLE

```sql
openGauss=# call gms_stats.create_stat_table('sc_stats', 't_tmp_stats');
 create_stat_table
-------------------

(1 row)
openGauss=# \d t_tmp_stats
         Table "sc_stats.t_tmp_stats"
    Column     |       Type       | Modifiers
---------------+------------------+-----------
 namespaceid   | oid              |
 starelid      | oid              |
 partid        | oid              |
 statype       | "char"           |
 starelkind    | "char"           |
 staattnum     | smallint         |
 stainherit    | boolean          |
 stanullfrac   | real             |
 stawidth      | integer          |
 stadistinct   | real             |
 reltuples     | double precision |
 relpages      | double precision |
 stakind1      | smallint         |
 stakind2      | smallint         |
 stakind3      | smallint         |
 stakind4      | smallint         |
 stakind5      | smallint         |
 staop1        | oid              |
 staop2        | oid              |
 staop3        | oid              |
 staop4        | oid              |
 staop5        | oid              |
 stanumbers1   | real[]           |
 stanumbers2   | real[]           |
 stanumbers3   | real[]           |
 stanumbers4   | real[]           |
 stanumbers5   | real[]           |
 stavalues1    | anyarray         |
 stavalues2    | anyarray         |
 stavalues3    | anyarray         |
 stavalues4    | anyarray         |
 stavalues5    | anyarray         |
 stadndistinct | real             |
 staextinfo    | text             |
Indexes:
    "t_tmp_stats_namespac_type_rel_idx" btree (namespaceid, statype, starelid) TABLESPACE pg_default
```

- DROP_STAT_TABLE

```sql
openGauss=# call gms_stats.drop_stat_table('sc_stats', 't_tmp_stats');
 drop_stat_table
-----------------

(1 row)
openGauss=# \d t_tmp_stats
Did not find any relation named "t_tmp_stats".
```

- GATHER_DATABASE_STATS

```sql
openGauss=# call gms_stats.gather_database_stats();
 gather_database_stats
-----------------------

(1 row)
openGauss=# call gms_stats.gather_database_stats(stattab=>'t_tmp_stats', statown=>'sc_stats');
 gather_database_stats
-----------------------

(1 row)
```

- GATHER_SCHEMA_STATS

```sql
openGauss=# call gms_stats.gather_schema_stats('sc_stats');
 gather_schema_stats
---------------------

(1 row)
openGauss=# call gms_stats.gather_schema_stats('sc_stats', stattab=>'t_tmp_stats');
 gather_schema_stats
---------------------

(1 row)
```

- GATHER_TABLE_STATS

```sql
openGauss=# call gms_stats.gather_table_stats('sc_stats', 't_stats');
 gather_table_stats
--------------------

(1 row)
openGauss=# call gms_stats.gather_table_stats('sc_stats', 't_stats', stattab=>'t_tmp_stats');
 gather_table_stats
--------------------

(1 row)
openGauss=# call gms_stats.gather_table_stats('sc_stats', 't_part', 't_part_list_r', stattab=>'t_tmp_stats');
 gather_table_stats
--------------------

(1 row)
```

- GATHER_INDEX_STATS

```sql
openGauss=# call gms_stats.gather_index_stats('sc_stats', 't_stats_pk');
 gather_index_stats
--------------------

(1 row)
openGauss=# call gms_stats.delete_index_stats('sc_stats', 't_stats_pk', stattab=>'t_tmp_stats');
 delete_index_stats
--------------------

(1 row)
```

- DELETE_SCHEMA_STATS

```sql
openGauss=# call gms_stats.delete_schema_stats('sc_stats');
 delete_schema_stats
---------------------

(1 row)
openGauss=# call gms_stats.delete_schema_stats('sc_stats', stattab=>'t_tmp_stats');
 delete_schema_stats
---------------------

(1 row)
```

- DELETE_TABLE_STATS

```sql
openGauss=# call gms_stats.delete_table_stats('sc_stats', 't_stats');
 delete_table_stats
--------------------

(1 row)
openGauss=# call gms_stats.delete_table_stats('sc_stats', 't_stats', stattab=>'t_tmp_stats');
 delete_table_stats
--------------------

(1 row)
openGauss=# call gms_stats.delete_table_stats('sc_stats', 't_part', 't_part_list_r', stattab=>'t_tmp_stats');
 delete_table_stats
--------------------

(1 row)
```

- DELETE_COLUMN_STATS

```sql
openGauss=# call gms_stats.delete_column_stats('sc_stats', 't_stats', 'c3');
 delete_column_stats
---------------------

(1 row)
openGauss=# call gms_stats.delete_column_stats('sc_stats', 't_stats', 'c3', stattab=>'t_tmp_stats');
 delete_column_stats
---------------------

(1 row)
```

- DELETE_INDEX_STATS

```sql
openGauss=# call gms_stats.delete_index_stats('sc_stats', 't_stats_pk');
 delete_index_stats
--------------------

(1 row)
openGauss=# call gms_stats.delete_index_stats('sc_stats', 't_stats_pk', stattab=>'t_tmp_stats');
 delete_index_stats
--------------------

(1 row)
```

- SET_TABLE_STATS

```sql
openGauss=# call gms_stats.set_table_stats('sc_stats', 't_stats', numrows=>2345);
 set_table_stats
-----------------

(1 row)
openGauss=# call gms_stats.set_table_stats('sc_stats', 't_stats', numblks=>16);
 set_table_stats
-----------------

(1 row)
openGauss=# call gms_stats.set_table_stats('sc_stats', 't_stats', stattab=>'t_tmp_stats', numrows=>1100);
 set_table_stats
-----------------

(1 row)
openGauss=# call gms_stats.set_table_stats('sc_stats', 't_stats', stattab=>'t_tmp_stats', numblks=>10);
 set_table_stats
-----------------

(1 row)
```

- SET_INDEX_STATS

```sql
openGauss=# call gms_stats.set_index_stats('sc_stats', 't_stats_pk', numdist=>100);
 set_index_stats
-----------------

(1 row)
openGauss=# call gms_stats.set_index_stats('sc_stats', 't_stats_pk', stattab=>'t_tmp_stats', numdist=>100);
 set_index_stats
-----------------

(1 row)
```

- SET_COLUMN_STATS

```sql
openGauss=# call gms_stats.set_column_stats('sc_stats', 't_stats', 'c2', nullcnt=>0.2);
 set_column_stats
------------------

(1 row)
openGauss=# call gms_stats.set_column_stats('sc_stats', 't_stats', 'c2', distcnt=>1000);
 set_column_stats
------------------

(1 row)
openGauss=# call gms_stats.set_column_stats('sc_stats', 't_stats', 'c2', stattab=>'t_tmp_stats', nullcnt=>0.2);
 set_column_stats
------------------

(1 row)
openGauss=# call gms_stats.set_column_stats('sc_stats', 't_stats', 'c2', stattab=>'t_tmp_stats', distcnt=>1000);
 set_column_stats
------------------

(1 row)
```

- IMPORT_SCHEMA_STATS

```sql
openGauss=# call gms_stats.import_schema_stats('sc_stats', stattab=>'t_tmp_stats');
 import_schema_stats
---------------------

(1 row)
```

- IMPORT_TABLE_STATS

```sql
openGauss=# call gms_stats.import_table_stats('sc_stats', 't_stats', stattab=>'t_tmp_stats');
 import_table_stats
--------------------

(1 row)
```

- IMPORT_INDEX_STATS

```sql
openGauss=# call gms_stats.import_index_stats('sc_stats', 't_stats_pk', stattab=>'t_tmp_stats');
 import_index_stats
--------------------

(1 row)
```

- IMPORT_COLUMN_STATS

```sql
openGauss=# call gms_stats.import_column_stats('sc_stats', 't_stats', 'c2', stattab=>'t_tmp_stats');
 import_column_stats
---------------------

(1 row)
```

- EXPORT_SCHEMA_STATS

```sql
openGauss=# call gms_stats.export_schema_stats('sc_stats', stattab=>'t_tmp_stats');
 export_schema_stats
---------------------

(1 row)

```

- EXPORT_TABLE_STATS

```sql
openGauss=# call gms_stats.export_index_stats('sc_stats', 't_stats_pk', stattab=>'t_tmp_stats');
 export_index_stats
--------------------

(1 row)
```

- EXPORT_INDEX_STATS

```sql
openGauss=# call gms_stats.export_index_stats('sc_stats', 't_stats_pk', stattab=>'t_tmp_stats');
 export_index_stats
--------------------

(1 row)
```

- EXPORT_COLUMN_STATS

```sql
openGauss=# call gms_stats.export_column_stats('sc_stats', 't_stats', 'c2', stattab=>'t_tmp_stats');
 export_column_stats
---------------------

(1 row)
```

- GET_STATS_HISTORY_AVAILABILITY

```sql
openGauss=# call gms_stats.get_stats_history_availability();
 get_stats_history_availability
--------------------------------
 2000-01-01 08:00:00+08
(1 row)
```

- GET_STATS_HISTORY_RETENTION

```sql
openGauss=# call gms_stats.get_stats_history_retention();
 get_stats_history_retention
-----------------------------
                          31
(1 row)
```

- PURGE_STATS

```sql
openGauss=# call gms_stats.purge_stats('2025-02-14 00:00:00');
 purge_stats
-------------

(1 row)
```

- RESTORE_SCHEMA_STATS

```sql
openGauss=# call gms_stats.restore_schema_stats('sc_stats', '2025-02-14 00:00:00');
 restore_schema_stats
----------------------

(1 row)
```

- RESTORE_TABLE_STATS

```sql
openGauss=# call gms_stats.restore_table_stats('sc_stats', 't_stats', '2025-02-14 00:00:00');
 restore_table_stats
---------------------

(1 row)
```

- LOCK_SCHEMA_STATS

```sql
openGauss=# call gms_stats.lock_schema_stats('sc_stats');
 lock_schema_stats
-------------------

(1 row)
```

- LOCK_TABLE_STATS

```sql
openGauss=# call gms_stats.lock_table_stats('sc_stats', 't_stats');
 lock_table_stats
------------------

(1 row)
```

- LOCK_PARTITION_STATS

```sql
openGauss=# call gms_stats.lock_partition_stats('sc_stats', 't_part', 't_part_list_r');
 lock_partition_stats
----------------------

(1 row)
```

- UNLOCK_SCHEMA_STATS

```sql
openGauss=# call gms_stats.unlock_schema_stats('sc_stats');
 unlock_schema_stats
---------------------

(1 row)
```

- UNLOCK_TABLE_STATS

```sql
openGauss=# call gms_stats.unlock_table_stats('sc_stats', 't_stats');
 unlock_table_stats
--------------------

(1 row)

```

- UNLOCK_PARTITION_STATS

```sql
openGauss=# call gms_stats.unlock_partition_stats('sc_stats', 't_part', 't_part_list_r');
 unlock_partition_stats
------------------------

(1 row)
```

### 删除Extension<a name="section1587441381220"></a>

在openGauss中删除gms_stats Extension的方法如下所示：

```
openGauss=# DROP Extension gms_stats [CASCADE];
```

>[!NOTE]说明
>
>如果Extension被其它对象依赖，需要加入CASCADE（级联）关键字，删除所有依赖对象。
