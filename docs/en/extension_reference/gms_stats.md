# gms_stats

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-07-30T06:28:06.894Z pushedAt=2026-09-20T09:30:23.792Z -->

## gms_stats Overview

gms_stats is an openGauss-based plugin used to accurately estimate statistics (especially for large partitioned tables), yielding better statistical results and ultimately generating faster SQL execution plans. The currently supported interfaces are:

- `GATHER_SCHEMA_STATS` (used to collect statistics for objects within a schema).
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

## gms_stats Limitations

- Only the CREATE EXTENSION command is supported for loading the plugin.

## gms_stats Installation

openGauss includes gms_stats by default during packaging and compilation. After openGauss is installed, you can directly load the extension by using create extension gms_stats;.

## gms_stats Usage

### Creating an Extension<a name="section21088306113"></a>

To create the gms_stats extension, you can directly use the CREATE Extension command:

```
openGauss=# CREATE Extension gms_stats;
```

### Using Extension<a name="section107391050141118"></a>

#### Declaration

- CREATE_STAT_TABLE(ownname VARCHAR22, stattab VARCHAR22, tblspace VARCHAR22 DEFAULT NULL, global_temporary BOOLEAN DEFAULT FALSE);

  **Description**: This process creates a user table for collecting statistics in the specified schema.

  **Parameter description**:

  - `ownname`: specifies the schema name where the user table is to be created;
  - `stattab`: specifies the name of the user table to be created;
  - `tblspace`: specifies the name of the tablespace used by the user table to be created;
  - `global_temporary`: specifies whether the user table to be created is a global temporary table.

  **Usage notes**: the user requires the privilege to CREATE tables in the specified schema.

- DROP_STAT_TABLE(ownname VARCHAR22, stattab VARCHAR22);

  **Description**: This process is used to delete a user table in the specified schema.

  **Parameter description**:

  - `ownname`: Specifies the schema name where the user table to be deleted resides.
  - `stattab`: Specifies the name of the user table to be deleted.

  **Privileges**: Requires the user to have the DROP table privilege in the specified schema.

- GATHER_DATABASE_STATS(estimate_percent NUMBER DEFAULT NULL, block_sample BOOLEAN DEFAULT FALSE, method_opt VARCHAR2 DEFAULT NULL, degree NUMBER DEFAULT NULL, granularity VARCHAR2 DEFAULT NULL, cascade BOOLEAN DEFAULT NULL, stattab VARCHAR2 DEFAULT NULL, statid VARCHAR2 DEFAULT NULL, options VARCHAR2 DEFAULT 'GATHER', objlist OUT ObjectTab, statown VARCHAR2 DEFAULT NULL, gather_sys BOOLEAN DEFAULT TRUE, no_invalidate BOOLEAN DEFAULT NULL, obj_filter_list ObjectTab DEFAULT NULL);

  **Description**: This process is used to collect statistics information data for all objects in the current database.

  **Parameter description**:

  - **estimate_percent**: Determines the percentage of rows to be sampled, ranging from 0.000001 to 100. This parameter setting is currently not supported.
  - **block_sample**: Whether to use random block sampling instead of random row sampling. This parameter setting is currently not supported.
  - **method_opt**: Method option for collecting statistics. **This parameter setting is currently not supported.**
  - **degree**: Determines the degree of parallelism for statistics collection. **This parameter configuration is currently not supported.**
  - **granularity**: Granularity of statistics to be collected (partitioned table only). **This parameter configuration is currently not supported.**
  - **cascade**: Determines whether to collect index statistics as part of statistics collection. **This parameter configuration is currently not supported.** Uses fixed TRUE.
  - **stattab**: Indicates the name of the user table that stores statistics. If this value is not NULL, then this statistics information is stored to this user table.
  - **statid**: Indicates the OID of the user table that stores statistics. If this value is not NULL, then this statistics information is stored to this user table;
  - **statown**: Indicates the schema of the user table where statistics information is stored. If NULL, then uses the current schema; [Note: "schmea" in the source text is a typo and should be "schema".]
  - **options**: Specifies which objects need to collect statistics. This parameter setting is currently not supported;
  - **objlist**: Specifies a list of outdated objects or is empty. This parameter setting is currently not supported;
  - **no_invalidate**: Controls the invalidation of subordinate cursors during collecting statistics. This parameter setting is currently not supported;
  - **gather_sys**: Whether to collect statistics on system table data;
  - **obj_filter_list**: Object list. If this parameter is specified (with at least one entry), statistics will be collected only for objects in this list.

  **Privileges**: Administrator privileges or ownership of the current database is required to execute this procedure.

- GATHER_SCHEMA_STATS(ownname VARCHAR22, estimate_percent NUMBER DEFAULT 100, block_sample boolean DEFAULT FALSE, method_opt VARCHAR22 DEFAULT 'FOR ALL COLUMNS SIZE AUTO', degree NUMBER DEFAULT NULL, granularity VARCHAR22 DEFAULT 'GLOBAL', cascade boolean DEFAULT FALSE, stattab VARCHAR22 DEFAULT NULL, statid VARCHAR22 DEFAULT NULL, options VARCHAR22 DEFAULT 'GATHER', objlist ObjectTab DEFAULT NULL, statown VARCHAR22 DEFAULT NULL, no_invalidate boolean DEFAULT FALSE, force boolean DEFAULT FALSE, obj_filter_list objecttab DEFAULT NULL);

  **Description**: This procedure is used to collect statistics information data for all objects in the specified schema.

  **Parameter description**:

  - **ownname**: Specifies the name of the schema for which statistics are to be collected;

  - **estimate_percent**: Determines the percentage of rows to be sampled, ranging from 0.000001 to 100. **This parameter setting is currently not supported.**
  - **block_sample**: Whether to use random block sampling instead of random row sampling. **This parameter setting is currently not supported.**
  - **method_opt**: Method options for collecting statistics. **This parameter setting is currently not supported.**
  - **degree**: Determines the degree of parallelism for statistics collection. **This parameter configuration is currently not supported**;
  - **granularity**: The granularity of statistics to be collected (related to partitioned tables only). **This parameter configuration is currently not supported**;
  - **cascade**: Determines whether to collect index statistics as part of statistics collection. **This parameter configuration is currently not supported**, and a fixed value of TRUE is used;
  - **stattab**: Indicates the name of the user table that stores statistics. If this value is not NULL, then this statistics information is stored to this user table;
  - **statid**: Indicates the OID of the user table that stores statistics. If this value is not NULL, then this statistics information is stored to this user table;
  - **statown**: Indicates the schema where the user table for statistics information storage resides. If NULL, the current schema is used;
  - **options**: Specifies which objects need statistics collection. This parameter setting is currently not supported;
  - **objlist**: Specifies a list of obsolete objects or is empty. This parameter setting is currently not supported;
  - **no_invalidate**: Controls the invalidation of subordinate cursors during statistics collection. This parameter setting is currently not supported;
  - **force**: Specifies how to handle locked statistics objects. If TRUE, statistics are collected for all objects; if FALSE, locked objects are skipped;
  - **obj_filter_list**: Object list. If this parameter is specified (which must contain at least one item), only statistics of the objects in this list will be collected.

  **Privileges**: Administrator privileges or ownership of the current database are required to execute this procedure.

- GATHER_TABLE_STATS(ownname VARCHAR22, tabname VARCHAR22, partname VARCHAR22 DEFAULT NULL, estimate_percent NUMBER DEFAULT NULL, block_sample BOOLEAN DEFAULT NULL, method_opt VARCHAR22 DEFAULT NULL, degree NUMBER DEFAULT NULL, granularity VARCHAR22 DEFAULT NULL, cascade BOOLEAN DEFAULT NULL, stattab VARCHAR22 DEFAULT NULL, statid VARCHAR22 DEFAULT NULL, statown VARCHAR22 DEFAULT NULL, no_invalidate BOOLEAN DEFAULT NULL, stattype VARCHAR22 DEFAULT NULL, force BOOLEAN DEFAULT FALSE, context text DEFAULT NULL, options VARCHAR22 DEFAULT NULL);

  **Description**: This procedure is used to collect statistics information data for a specified table or partition.

  **Parameter description**:

  - **ownname**: specifies the schema name for which statistics are to be collected;
  - **tabname**: specifies the table name for which statistics are to be collected;
  - **partname**: specifies the partition name for which statistics are to be collected;

  - **estimate_percent**: determines the percentage of rows to be sampled, ranging from 0.000001 to 100. This parameter setting is currently not supported;
  - **block_sample**: whether to use random block sampling instead of random row sampling. This parameter setting is currently not supported;
  - **method_opt**: Method option for collecting statistics. **This parameter setting is currently not supported.**
  - **degree**: Determines the degree of parallelism for statistics collection. **This parameter configuration is currently not supported.**
  - **granularity**: Granularity of statistics to be collected (relevant to partitioned tables only). **This parameter configuration is currently not supported.**
  - **cascade**: Determines whether to collect index statistics as part of statistics collection. **This parameter configuration is currently not supported.** A fixed value of TRUE is used.
  - **stattab**: Indicates the name of the user table that stores statistics. If this value is not NULL, then this statistics information is stored to this user table.
  - **statid**: Indicates the OID of the user table that stores statistics. If this value is not NULL, then this statistics information is stored to this user table;
  - **statown**: Indicates the schema of the user table where statistics information is stored. If NULL, then uses the current schema;
  - **no_invalidate**: Controls the invalidation of subordinate cursors during collecting statistics. **This parameter setting is currently not supported.**;
  - **stattype**: The current stored procedure only supports the fixed value `DATA`. **This parameter configuration is currently not supported.**
  - **force**: Specifies how to handle a statistics object when it is locked. If TRUE, statistics are collected regardless of whether the object is locked; if FALSE, an error is reported when the object is locked;
  - **context**: Setting this parameter is currently not supported.
  - **options**: Specifies which objects need to have statistics collected. **This parameter setting is currently not supported.**

  **Privileges**: Requires administrator privileges, or the owner of the current database, or the owner of the current table to execute this procedure.

- GATHER_INDEX_STATS(ownname VARCHAR2, indname VARCHAR2, partname VARCHAR2 DEFAULT NULL, estimate_percent NUMBER DEFAULT NULL, stattab VARCHAR2 DEFAULT NULL, statid VARCHAR2 DEFAULT NULL, statown VARCHAR2 DEFAULT NULL, degree NUMBER DEFAULT NULL, granularity VARCHAR2 DEFAULT NULL, no_invalidate BOOLEAN DEFAULT NULL, force BOOLEAN DEFAULT FALSE);

  **Description**: This procedure is used to collect statistics for the table where the specified index resides. The actual effect is consistent with `GATHER_TABLE_STATS`.

  **Parameter description**:

  - **ownname**: specifies the schema name for which statistics are to be collected;
  - **indname**: specifies the index name for which statistics are to be collected;
  - **partname**: specifies the name of the partition for which statistics are to be collected; **this parameter setting is currently not supported**;

  - **estimate_percent**: determines the percentage of rows to be sampled, ranging from 0.000001 to 100. **This parameter setting is currently not supported**;
  - **stattab**: Indicates the name of the user table that stores statistics. If this value is not NULL, then this statistics information is stored to this user table;
  - **statid**: Indicates the OID of the user table that stores statistics. If this value is not NULL, then this statistics information is stored to this user table;
  - **statown**: Indicates the schema where the user table for statistics information storage resides. If NULL, then uses the current schema;
  - **degree**: Determines the degree of parallelism for statistics collection. **This parameter configuration is currently not supported.**;
  - **granularity**: The granularity of statistics to be collected (partitioned table only). **This parameter configuration is currently not supported.**;
  - **no_invalidate**: Controls the invalidation of dependent cursors during statistics collection. **This parameter setting is currently not supported.**
  - **force**: Specifies how to handle a statistics object when it is locked. If TRUE, statistics are collected regardless of whether the object is locked; if FALSE, an error is reported when the object is locked.

  **Privileges**: Requires administrator privileges, or the current database owner, or the owner of the current table to execute this procedure.

  **Usage Notes**: Currently, the actual effect of `GATHER_INDEX_STATS` is the same as that of `GATHER_TABLE_STATS`. It locates the table to which the specified index belongs and then collects statistics for that table.

- DELETE_SCHEMA_STATS(ownname VARCHAR2, stattab VARCHAR2 DEFAULT NULL, statid VARCHAR2 DEFAULT NULL, statown VARCHAR2 DEFAULT NULL, no_invalidate BOOLEAN DEFAULT NULL, force BOOLEAN DEFAULT FALSE, stat_category VARCHAR2 DEFAULT NULL);

  **Description**: This process is used to delete statistics of all objects in the specified schema.

  **Parameter description**:

  - **ownname**: Specifies the name of the schema whose statistics are to be deleted.
  - **stattab**: Indicates the name of the user table that stores statistics. If this value is not NULL, then this deletion removes the statistics in the user table.
  - **statid**: Indicates the OID of the user table that stores statistics. If this value is not NULL, then this deletion removes the statistics in the user table.
  - **statown**: Indicates the schema where the user table for statistics information storage resides. If NULL, then uses the current schema;
  - **no_invalidate**: Controls the invalidation of subordinate cursors during collecting statistics. This parameter setting is currently not supported;
  - **force**: Specifies how to handle locked statistics objects. If TRUE, all object statistics are deleted; if FALSE, locked objects are skipped;
  - **stat_category**: The statistics data to be deleted. This parameter setting is currently not supported.

  **Privileges**: Requires administrator privileges or the owner of the current database to execute this process.

- DELETE_TABLE_STATS(ownname VARCHAR2, tabname VARCHAR2, partname VARCHAR2 DEFAULT NULL, stattab VARCHAR2 DEFAULT NULL, statid VARCHAR2 DEFAULT NULL, cascade_parts BOOLEAN DEFAULT TRUE, cascade_columns BOOLEAN DEFAULT TRUE, cascade_indexes BOOLEAN DEFAULT TRUE, statown VARCHAR2 DEFAULT NULL, no_invalidate BOOLEAN DEFAULT NULL, force BOOLEAN DEFAULT FALSE, stat_category VARCHAR2 DEFAULT NULL);

  **Description**: This process is used to delete statistics of a specified table under a specified schema.

  **Parameter description**:

  - **ownname**: Specifies the name of the schema whose statistics are to be deleted;
  - **tabname**: Specifies the name of the table whose statistics are to be deleted;
  - **partname**: specifies the partition name whose statistics are to be deleted;
  - **stattab**: indicates the name of the user table that stores statistics. If this value is not NULL, then this deletion will remove the statistics in the user table;
  - **statid**: indicates the OID of the user table that stores statistics. If this value is not NULL, then this deletion will remove the statistics in the user table;
  - **cascade_parts**: indicates whether to operate on partitions;
  - **cascade_columns**: indicates whether to delete column-related statistics in the table.
  - **cascade_indexs**: Indicates whether to delete index-related statistics in the table.
  - **statown**: Indicates the schema of the user table where statistics information is stored. If NULL, the current schema is used;
  - **no_invalidate**: Controls the invalidation of dependent cursors during statistics collection. This parameter setting is currently not supported;
  - **force**: Specifies how to handle a locked statistics object. If TRUE, the statistics of the specified object are deleted. If FALSE, an error is reported when the object is locked;
  - **stat_category**: The statistics data to be deleted. This parameter setting is currently not supported.

  **Privileges**: Administrator privileges are required, or the owner of the current database, or the owner of the current table to execute this procedure.

  **Usage Notes**:

  - When **partname** is specified, only the statistics related to the partitioned table pg_partition are deleted, and the partition statistics stored in pg_statistic are not deleted.

- DELETE_COLUMN_STATS(ownname VARCHAR2, tabname VARCHAR2, colname VARCHAR2, partname VARCHAR2 DEFAULT NULL, stattab VARCHAR2 DEFAULT NULL, statid VARCHAR2 DEFAULT NULL, cascade_parts BOOLEAN DEFAULT TRUE, statown VARCHAR2 DEFAULT NULL, no_invalidate BOOLEAN DEFAULT NULL, force BOOLEAN DEFAULT FALSE, col_stat_type VARCHAR22 DEFAULT 'ALL');

  **Description**: This procedure is used to delete the statistics of a specified column in a specified table under a specified schema.

  **Parameter description**:

  - **ownname**: specifies the schema name whose statistics are to be deleted;
  - **tabname**: specifies the table name whose statistics are to be deleted;
  - **partname**: specifies the partition name whose statistics are to be deleted; this parameter setting is currently not supported;
  - **stattab**: indicates the user table name that stores statistics. If this value is not NULL, then this deletion will be applied to the statistics in the user table;
  - **statid**: Indicates the OID of the user table that stores statistics. If this value is not NULL, then this deletion removes the statistics in the user table;
  - **cascade_parts**: Indicates whether to operate on partitions;
  - **statown**: Indicates the schema of the user table where statistics information is stored. If NULL, then uses the current schema;
  - **no_invalidate**: Controls the invalidation of subordinate cursors during statistics collection. This parameter setting is currently not supported;
  - **force**: Specifies how to handle the statistics object when it is locked. If TRUE, the statistics of the specified object are deleted; if FALSE, an error is reported when the object is locked;
  - **col_stat_type**: Column statistics data to be deleted. This parameter setting is currently not supported.

  **Privileges**: Administrator privileges, or ownership of the current database, or ownership of the current table are required to execute this procedure.

- DELETE_INDEX_STATS(ownname VARCHAR2, indname VARCHAR2, partname VARCHAR2 DEFAULT NULL, stattab VARCHAR2 DEFAULT NULL, statid VARCHAR2 DEFAULT NULL, cascade_parts BOOLEAN DEFAULT TRUE, statown VARCHAR2 DEFAULT NULL, no_invalidate BOOLEAN DEFAULT NULL, stattype VARCHAR2 DEFAULT 'ALL', force BOOLEAN DEFAULT FALSE, stat_category VARCHAR2 DEFAULT NULL);

  **Description**: This procedure is used to delete the statistics of a specified index under a specified schema.

  **Parameter description**:

  - **ownname**: Specifies the schema name for which statistics are to be deleted;
  - **indname**: Specifies the index name for which statistics are to be deleted;
  - **partname**: Specifies the partition name for which statistics are to be deleted; this parameter setting is currently not supported;
  - **stattab**: Indicates the name of the user table that stores statistics. If this value is not NULL, then the statistics in this user table will be deleted;
  - **statid**: Indicates the OID of the user table that stores statistics. If this value is not NULL, then the statistics in this user table will be deleted;
  - **cascade_parts**: Whether to operate on partitions;
  - **statown**: Indicates the schema of the user table where statistics information is stored. If NULL, then uses the current schema;
  - **no_invalidate**: Controls the invalidation of subordinate cursors during statistics collection. This parameter setting is currently not supported;
  - **stattype**: The current stored procedure only supports the fixed value `DATA`. This parameter configuration is currently not supported.
  - **force**: Specifies how to handle a locked statistics object. If TRUE, deletes the statistics information of the specified object; if FALSE, reports an error when the object is locked;
  - **stat_category**: Statistics data to be deleted. This parameter setting is currently not supported.

  **Privileges**: Administrator privileges, or ownership of the current database, or ownership of the current table is required to execute this procedure.

  **Usage Notes**:

  - Deleting index statistics currently deletes the statistics of the columns corresponding to the index.

- SET_TABLE_STATS(ownname VARCHAR2, tabname VARCHAR2, partname VARCHAR2 DEFAULT NULL, stattab VARCHAR2 DEFAULT NULL, statid VARCHAR2 DEFAULT NULL, numrows NUMBER DEFAULT NULL, numblks NUMBER DEFAULT NULL, avgrlen NUMBER DEFAULT NULL, flags NUMBER DEFAULT NULL, statown VARCHAR2 DEFAULT NULL, no_invalidate BOOLEAN DEFAULT NULL, cachedblk NUMBER DEFAULT NULL, cachehit NUMBER DEFAULT NULL, force BOOLEAN DEFAULT FALSE, im_imcu_count NUMBER DEFAULT NULL, im_block_count NUMBER DEFAULT NULL, scanrate NUMBER DEFAULT NULL);

  **Description**: This procedure is used to modify statistics in a statistics information table or partition-related statistics.

  **Parameter Description**:

  - **ownname**: Specifies the schema name for which statistics are to be modified;
  - **tabname**: Specifies the table name for which statistics are to be modified;
  - **partname**: Specifies the partition name for which statistics are to be modified;
  - **stattab**: Indicates the name of the user table that stores statistics. If this value is not NULL, then this modification is stored to the user table;
  - **statid**: Indicates the OID of the user table that stores statistics. If this value is not NULL, then this modification is stored to the user table;
  - **numrows**: Specifies the number of rows in the table to be modified;
  - **numblks**: Specifies the number of blocks in the table to be modified;
  - **avgrlen**: Modifies the average row length of the table. This parameter is currently not supported.
  - **flags**: Internal parameter. **This parameter is currently not supported.**
  - **cachedblk**: Internal parameter. **This parameter is currently not supported.**
  - **cachehit**: Internal parameter. **This parameter is currently not supported.**
  - **force**: Specifies how to handle a locked statistics object. If TRUE, the statistics information of the specified object is modified. If FALSE, an error is reported when the object is locked.
  - **im_imcu_count**: Modifies the im_imcu_count column data. **This parameter is currently not supported.**
  - **im_block_count**: Modifies the im_block_count column data. **This parameter is currently not supported.**
  - **scanrate**: The rate at which the database scans external tables, in MB/s. This parameter is only relevant to external tables. **This parameter is currently not supported.**

  **Privileges**: Requires administrator privileges, or the owner of the current database, or the owner of the current table to execute this procedure.

- SET_INDEX_STATS(ownname VARCHAR2, indname VARCHAR2, partname VARCHAR2 DEFAULT NULL, stattab VARCHAR2 DEFAULT NULL, statid VARCHAR2 DEFAULT NULL, numrows NUMBER DEFAULT NULL, numlblks NUMBER DEFAULT NULL, numdist NUMBER DEFAULT NULL, avglblk NUMBER DEFAULT NULL, avgdblk NUMBER DEFAULT NULL, clstfct NUMBER DEFAULT NULL, indlevel NUMBER DEFAULT NULL, flags NUMBER DEFAULT NULL, statown VARCHAR2 DEFAULT NULL, no_invalidate BOOLEAN DEFAULT NULL, guessq NUMBER DEFAULT NULL, cachedblk NUMBER DEFAULT NULL, cachehit NUMBER DEFUALT NULL, force BOOLEAN DEFAULT FALSE);

  **Description**: This procedure is used to modify index-related statistics.

  **Parameter description**:

  - **ownname**: Specifies the schema name for which statistics are to be modified.
  - **indname**: Specifies the index name for which statistics are to be modified.
  - **partname**: Specifies the partition name for which statistics are to be modified. **This parameter is currently not supported.**
  - **stattab**: Indicates the name of the user table that stores statistics. If this value is not NULL, then this modification is stored to this user table.
  - **statid**: Indicates the OID of the user table that stores statistics. If this value is not NULL, then this modification is stored to the user table;
  - **numrows**: Specifies the number of rows in the table to be modified. **This parameter is currently not supported.**
  - **numblks**: Specifies the number of blocks in the table to be modified. **This parameter is currently not supported.**
  - **numdist**: Specifies the number of distinct index keys after deduplication;
  - **avglblk**: For this index (partition), the average integer number of leaf blocks in which each distinct key appears. If not provided, this value is derived from `numlblks` and `numdist`. **This parameter is currently not supported.**
  - **avgdblk**: For this index (partition), the average integer value of a distinct key pointing to data blocks in the table. If not provided, this value is derived from `clstfct` and `numdist`. **This parameter is currently not supported.**
  - **clstfct**: Indicates the degree of ordering of rows in the table based on the index value. **This parameter is currently not supported.**
  - **indlevel**: The height of the index. **This parameter is currently not supported.**
  - **flags**: Internal parameter. **This parameter is currently not supported.**
  - **statown**: Indicates the schema of the user table where statistics information is stored. If NULL, the current schema is used. [Note: "schmea" in the source text is a typo and should be "schema".]
  - **no_invalidate**: Controls the invalidation of subordinate cursors during statistics collection. **This parameter setting is currently not supported.**
  - **guessq**: For secondary indexes on index-organized tables, the percentage of rows that are effectively guessed. **This parameter is currently not supported.**
  - **cachedblk**: Internal parameter. **This parameter is currently not supported.**
  - **cachehit**: Internal parameter. **This parameter is currently not supported.**
  - **force**: Specifies how to handle a statistics object when it is locked. If TRUE, the statistics of the specified object are modified; if FALSE, an error is reported when the object is locked.

  **Privileges**: Requires administrator privileges, or the owner of the current database, or the owner of the current table to execute this process.

  **Usage Notes**:

  - Currently, this process actually modifies the statistics of the columns corresponding to the index.

- SET_COLUMN_STATS(ownname VARCHAR2, tabname VARCHAR2, colname VARCHAR2, partname VARCHAR2 DEFAULT NULL, stattab VARCHAR2 DEFAULT NULL, statid VARCHAR2 DEFAULT NULL, distcnt NUMBER DEFAULT NULL, density NUMBER DEFAULT NULL, nullcnt NUMBER DEFAULT NULL, srec text DEFAULT NULL, avgclen NUMBER DEFAULT NULL, flags NUMBER DEFAULT NULL, statown VARCHAR2 DEFAULT NULL, no_invalidate BOOLEAN DEFAULT NULL, force BOOLEAN DEFAULT FALSE);

  **Description**: This process is used to modify the statistics of a specified column.

  **Parameter description**:

  - **ownname**: Specifies the schema name for which statistics are to be modified.
  - **tabname**: Specifies the table name for which statistics are to be modified.
  - **colname**: Specifies the column name for which statistics are to be modified.
  - **partname**: Specifies the partition name for which statistics are to be modified. **This parameter is currently not supported.**
  - **stattab**: Indicates the name of the user table storing statistics. If this value is not NULL, then this modification is applied to the statistics in the user table;
  - **statid**: Indicates the OID of the user table storing statistics. If this value is not NULL, then this modification is applied to the statistics in the user table;
  - **distcnt**: Specifies the number of distinct rows to be modified;
  - **density**: Column data density. If this value is NULL and `distcnt` is not NULL, the density is derived from distcnt. This parameter is currently not supported;
  - **nullcnt**: Specifies the number of NULL values in the rows to be modified;
  - **srec**: A record of the `StatRec` type, containing column statistics such as minimum and maximum values. **This parameter is currently not supported.**
  - **avgclen**: Modifies the average row length of the table. **This parameter is currently not supported.**
  - **flags**: Internal parameter. **This parameter is currently not supported.**
  - **statown**: Indicates the schema where the user table for statistics information storage resides. If NULL, the current schema is used.
  - **no_invalidate**: Controls the invalidation of subordinate cursors during statistics collection. **This parameter setting is currently not supported.**
  - **force**: Specifies how to handle a locked statistics object. If TRUE, the statistics of the specified object are modified; if FALSE, an error is reported when the object is locked.

  **Privileges**: Administrator privileges, or ownership of the current database, or ownership of the current table are required to execute this procedure.

- IMPORT_SCHEMA_STATS(ownname VARCHAR2, stattab VARCHAR2, statid VARCHAR2 DEFAULT NULL, statown VARCHAR2 DEFAULT NULL, no_invalidate BOOLEAN DEFAULT NULL, force BOOLEAN DEFAULT FALSE, stat_category VARCHAR2 DEFAULT NULL);

  **Description**: This procedure is used to import statistics under a specified schema from a specified user table into the system tables.

  - **ownname**: Specifies the name of the schema whose statistics are to be imported.
  - **stattab**: indicates the name of the user table that stores statistics, from which statistics are imported;
  - **statid**: indicates the OID of the user table that stores statistics. If this value is not NULL, statistics are imported from this user table;
  - **statown**: indicates the schema where the user table storing statistics resides. If NULL, then the current schema is used;
  - **no_invalidate**: controls the invalidation of subordinate cursors during statistics collection. This parameter setting is currently not supported;
  - **force**: specifies how to handle a statistics object when it is locked. If TRUE, the specified object statistics are imported; if FALSE, the object is skipped when locked;
  - **stat_category**: Statistics data to be imported. **This parameter setting is currently not supported.**

  **Privileges**: Administrator privileges or ownership of the current database is required to execute this procedure.

- IMPORT_TABLE_STATS(ownname VARCHAR2, tabname VARCHAR2, partname VARCHAR2 DEFAULT NULL, stattab VARCHAR2, statid VARCHAR2 DEFAULT NULL, cascade BOOLEAN DEFAULT TRUE, statown VARCHAR2 DEFAULT NULL, no_invalidate BOOLEAN DEFAULT NULL, force BOOLEAN DEFAULT FALSE, stat_category VARCHAR2 DEFAULT NULL);

  **Description**: This procedure is used to import statistics of a specified table from a specified user table into the system tables.

  **Parameter description**:

  - **ownname**: specifies the name of the schema whose statistics are to be imported;
  - **tabname**: specifies the name of the table whose statistics are to be imported;
  - **partname**: specifies the name of the partition whose statistics are to be imported;
  - **stattab**: indicates the name of the user table that stores statistics, from which statistics are imported;
  - **statid**: indicates the OID of the user table that stores statistics. If this value is not NULL, statistics are imported from this user table;
  - **cascade**: Whether to export index-related statistics;
  - **statown**: Indicates the schema of the user table where statistics information is stored. If NULL, then uses the current schema;
  - **no_invalidate**: Controls the invalidation of dependent cursors during statistics collection. This parameter setting is currently not supported;
  - **force**: Specifies how to handle the statistics object when it is locked. If TRUE, imports the statistics of the specified object. If FALSE, skips the object when it is locked;
  - **stat_category**: Statistics to be imported. This parameter setting is currently not supported.

  **Privileges**: Administrator privileges, or ownership of the current database, or ownership of the current table are required to execute this procedure.

  **Usage Notes**:

  - If partname is not NULL, partition statistics in the pg_statistic system catalog are not processed.

- IMPORT_INDEX_STATS(ownname VARCHAR2, indname VARCHAR2, partname VARCHAR2 DEFAULT NULL, stattab VARCHAR2, statid VARCHAR2 DEFAULT NULL, statown VARCHAR2 DEFAULT NULL, no_invalidate BOOLEAN DEFAULT NULL, force BOOLEAN DEFAULT FALSE);

  **Description**: This procedure is used to import statistics of a specified index from a specified user table into the system catalog.

  **Parameter description**:

  - **ownname**: specifies the schema name for which statistics are to be imported;
  - **indname**: specifies the name of the index for which statistics are to be imported;
  - **partname**: specifies the name of the partition for which statistics are to be imported; this parameter setting is currently not supported;
  - **stattab**: indicates the name of the user table that stores statistics, from which statistics are imported;
  - **statid**: Indicates the OID of the user table storing statistics. If this value is not NULL, statistics are imported from this user table;
  - **statown**: Indicates the schema where the user table storing statistics information resides. If NULL, then uses the current schema;
  - **no_invalidate**: Controls the invalidation of subordinate cursors during collecting statistics. **This parameter setting is currently not supported.**;
  - **force**: Specifies how to handle the situation when the statistics object is locked. If TRUE, the specified object statistics are imported; if FALSE, the object is skipped when locked;

  **Privileges**: Requires administrator privileges, or the current database owner, or the owner of the current table to execute this process.

- IMPORT_COLUMN_STATS(ownname VARCHAR2, tabname VARCHAR2, colname VARCHAR2, partname VARCHAR2 DEFAULT NULL, stattab VARCHAR2, statid VARCHA2 DEFAULT NULL, statown VARCHAR2 DEFAULT NULL, no_invalidate BOOLEAN DEFAULT NULL, force BOOLEAN DEFAULT FALSE);

  **Description**: This process is used to import the statistics of a specified column from a specified user table into the system table.

  **Parameter description**:

  - **ownname**: Specifies the name of the schema whose statistics are to be imported;
  - **tabname**: Specifies the name of the table whose statistics are to be imported;
  - **colname**: specifies the name of the column to import statistics for;
  - **partname**: specifies the name of the partition to import statistics for; **This parameter setting is currently not supported.**;
  - **stattab**: indicates the name of the user table that stores statistics, from which statistics are imported;
  - **statid**: indicates the OID of the user table that stores statistics; if this value is not NULL, statistics are imported from this user table;
  - **statown**: indicates the schema where the user table storing statistics resides; if NULL, then uses the current schema [Note: "schmea" in the source text is a typo and has been corrected to "schema"];
  - **no_invalidate**: Controls the invalidation of dependent cursors during statistics collection. **This parameter setting is currently not supported.**
  - **force**: Specifies how to handle a statistics object when it is locked. If TRUE, the specified object statistics are imported. If FALSE, the object is skipped when locked.

  **Privileges**: Administrator privileges, or ownership of the current database, or ownership of the current table are required to execute this procedure.

- EXPORT_SCHEMA_STATS(ownname VARCHAR2, stattab VARCHAR2, statid VARCHAR2 DEFAULT NULL, statown VARCHAR2 DEFAULT NULL, stat_category VARCHAR22 DEFAULT NULL);

  **Description**: This procedure is used to export statistics under a specified schema to a specified user table.

  **Parameter description**:

  - **ownname**: Specifies the name of the schema from which statistics are to be imported.
  - **stattab**: Indicates the name of the user table storing statistics. This export retrieves statistics from this user table.
  - **statid**: Indicates the OID of the user table storing statistics. If this value is not NULL, this export retrieves statistics from this user table.
  - **statown**: Indicates the schema where the user table storing statistics resides. If NULL, the current schema is used.
  - **stat_category**: Statistics data to be exported. This parameter setting is currently not supported.

  **Privileges**: Administrator privileges or ownership of the current database is required to execute this process.

- EXPORT_TABLE_STATS(ownname VARCHAR2, tabname VARCHAR2, partname VARCHAR2 DEFAULT NULL, stattab VARCHAR2, statid VARCHA2 DEFAULT NULL, cascade BOOLEAN DEFAULT TRUE, statown VARCHAR2 DEFAULT NULL, stat_category VARCHAR2 DEFAULT NULL);

  **Description**: This process exports the statistics of a specified table under a specified schema to a specified user table.

  **Parameter description**:

  - **ownname**: specifies the name of the schema from which to export statistics;
  - **tabname**: specifies the name of the table from which to export statistics;
  - **partname**: specifies the name of the partition from which to export statistics;
  - **stattab**: indicates the name of the user table that stores statistics information; this export retrieves statistics from this user table;
  - **statid**: indicates the OID of the user table that stores statistics information; if this value is not NULL, then this export retrieves statistics from this user table;
  - **cascade**: Whether to export index-related statistics.
  - **statown**: Indicates the schema of the user table where statistics are stored. If NULL, the current schema is used.
  - **stat_category**: The statistics data to be imported. This parameter setting is currently not supported.

  **Privileges**: Administrator privileges, or ownership of the current database, or ownership of the current table are required to execute this procedure.

  **Usage notes**:

  - If partname is not NULL, the partition statistics in the pg_statistic system table are not processed currently.****

- EXPORT_INDEX_STATS(ownname VARCHAR2, indname VARCHAR2, partname VARCHAR2 DEFAULT NULL, stattab VARCHAR2, statid VARCHAR2 DEFAULT NULL, statown VARCHAR2 DEFAULT NULL);

  **Description**: This procedure exports the statistics of a specified index under a specified schema to a specified user table.

  **Parameter Description**:

  - **ownname**: Specifies the name of the schema whose statistics are to be exported;
  - **indname**: specifies the name of the index whose statistics are to be exported;
  - **partname**: specifies the name of the partition whose statistics are to be exported; **This parameter setting is currently not supported.**;
  - **stattab**: indicates the name of the user table storing statistics, from which the statistics are exported this time;
  - **statid**: indicates the OID of the user table storing statistics; if this value is not NULL, the statistics in this user table are exported this time;
  - **statown**: indicates the schema where the user table storing statistics resides; if NULL, the current schema is used; *(Note: "schmea" in the source text is a typo and should be "schema".)*

  **Privileges**: Requires administrator privileges, or the owner of the current database, or the owner of the current table to execute this process.

  **Usage Notes**:

  - Currently, exporting index statistics actually exports the statistics of the columns corresponding to the index.

- EXPORT_COLUMN_STATS(ownname VARCHAR2, tabname VARCHAR2, colname VARCHAR2, partname VARCHAR2 DEFAULT NULL, stattab VARCHAR2, statid VARCHAR2 DEFAULT NULL, statown VARCHAR2 DEFAULT NULL);

  **Description**: This process exports the statistics of a specified column in a specified table under a specified schema to a specified user table.

  **Parameter description**:

  - **ownname**: specifies the name of the schema from which statistics are to be exported;
  - **tabname**: specifies the name of the table from which statistics are to be exported;
  - **colname**: specifies the name of the column from which statistics are to be exported;
  - **partname**: specifies the name of the partition from which statistics are to be exported; **This parameter setting is currently not supported.**;
  - **stattab**: indicates the name of the user table storing statistics, and this export exports the statistics in the user table;
  - **statid**: indicates the OID of the user table storing statistics. If this value is not NULL, this export exports the statistics in the user table;
  - **statown**: indicates the schema where the user table storing statistics information resides. If NULL, then uses the current schema;

  **Privileges**: requires administrator privileges, or being the owner of the current database, or being the owner of the current table to execute this process.

- GET_STATS_HISTORY_AVAILABILITY() RETURN TIMESTAMP WITH TIMEZONE;

  **Description**: Queries and returns the earliest available statistics timestamp.

- GET_STATS_HISTORY_RETENTION() RETURN NUMBER;

  **Description**: Returns the retention days of statistics history data in the current database.

- PURGE_STATS(before_timestamp TIMESTAMP WITH TIME ZONE);

  **Description**: Deletes statistics history data prior to the specified timestamp from the system table pg_statistic_history.

  **Parameter Description**:

  - **as_of_timestamp**: Statistics versions saved before this timestamp will be purged. If NULL, the automatic purge policy is used (deleting data in the system table pg_statistic_history whose statistics time is earlier than `current time - GET_STATS_HISTORY_RETENTION()`).

  **Privileges**: Administrator privileges or ownership of the current database is required to execute this procedure.

- RESTORE_SCHEMA_STATS(ownname VARCHAR2, as_of_timestamp TIMESTAMP WITH TIME ZONE, force BOOLEAN DEFAULT FALSE, no_invalidate BOOLEAN DEFAULT NULL);

  **Description**: Restores the statistics of all objects in the specified schema to the specified point in time.

  **Parameter description**:

  - **ownname**: specifies the name of the schema whose statistics are to be restored;
  - **as_of_timestamp**: specifies the point in time to which the statistics are to be restored;
  - **force**: specifies how to handle locked statistics objects. If TRUE, the statistics of the specified object are modified; if FALSE, locked objects are skipped;
  - **no_invalidate**: controls the invalidation of dependent cursors during statistics collection. This parameter setting is currently not supported.

  **Privileges**: Requires administrator privileges or the owner of the current database to execute this procedure.

- RESTORE_TABLE_STATS(ownname VARCHAR2, tabname VARCHAR2, as_of_timestamp TIMESTAMP WITH TIME ZONE, restore_cluster_index BOOLEAN DEFAULT FALSE, force BOOLEAN DEFAULT FALSE, no_invalidate BOOLEAN DEFAULT NULL);

  **Description**: Restores the statistics of a specified table in a specified schema to a specified point in time.

  **Parameter description**:

  - **ownname**: Specifies the name of the schema whose statistics are to be restored;
  - **tabname**: specifies the name of the table whose statistics are to be restored;
  - **as_of_timestamp**: specifies the point in time to which statistics are to be restored;
  - **restore_cluster_index**: if the table is part of a cluster and this is set to TRUE, restores the statistics of the cluster index. **This parameter is currently not supported.**
  - **force**: specifies how to handle a statistics object when it is locked. If TRUE, modifies the statistics of the specified object; if FALSE, skips the locked object;
  - **no_invalidate**: controls the invalidation of dependent cursors during statistics collection. **This parameter setting is currently not supported.**

  **Privileges**: Requires administrator privileges, or ownership of the current database, or ownership of the current table to execute this procedure.

  **Usage Notes**:

  - If the table is locked at the restore point, the table remains locked after restoration.

- LOCK_SCHEMA_STATS(ownname VARCHAR2);

  **Description**: This procedure locks all objects in the specified schema for statistics collection operations.

  **Parameter description**:

  - **ownname**: specifies the name of the schema whose statistics are to be locked;

- LOCK_TABLE_STATS(ownname VARCHAR2, tabname VARCHAR2);

  **Description**: This process locks the statistics operation on a specified table under a specified schema.

  **Parameter description**:

  - **ownname**: Specifies the name of the schema whose statistics are to be locked.
  - **tabname**: Specifies the name of the table whose statistics are to be locked.

- LOCK_PARTITION_STATS(ownname VARCHAR2, tabname VARCHAR2, partname VARCHAR2);

  **Description**: This process locks the statistics operations on a specified partition under a specified table in a specified schema.

  **Parameter Description**:

  - **ownname**: Specifies the name of the schema whose statistics are to be locked.
  - **tabname**: Specifies the name of the table whose statistics are to be locked.
  - **partname**: Specifies the name of the partition whose statistics are to be locked.

- UNLOCK_SCHEMA_STATS(ownname VARCHAR2);

  **Description**: This process unlocks the statistics collection operation for all objects in the specified schema.

  **Parameter description**:

  - **ownname**: specifies the schema name for which statistics information is to be unlocked;

- UNLOCK_TABLE_STATS(ownname VARCHAR2, tabname VARCHAR2);

  **Description**: this process unlocks the statistics information operation for a specified table under a specified schema.

  **Parameter description**:

  - **ownname**: Specifies the name of the schema whose statistics are to be unlocked;
  - **tabname**: Specifies the name of the table whose statistics are to be unlocked;

- UNLOCK_PARTITION_STATS(ownname VARCHAR2, tabname VARCHAR2, partname VARCHAR2);

  **Description**: This process unlocks the statistics operations for a specified partition under a specified table in a specified schema.

  **Parameter description**:

  - **ownname**: specifies the name of the schema whose statistics are to be unlocked;
  - **tabname**: specifies the name of the table whose statistics are to be unlocked;
  - **partname**: specifies the name of the partition whose statistics are to be unlocked;

#### Usage

- Prepare data

```sql
create schema sc_stats;
set current_schema = sc_stats;

create table t_stats (id int, c2 text, c3 char(1), constraint t_stats_pk primary key (id));
insert into t_stats values (generate_series(1, 100), 'aabbcc', 'Y');
insert into t_stats values (generate_series(101, 200), '123dfg', 'N');
insert into t_stats values (generate_series(201, 300), 'Peach blossoms reflect each other's crimson hue', 'N');
insert into t_stats values (generate_series(301, 400), 'fortunate', 'Y');
insert into t_stats values (generate_series(401, 500), 'open@gauss', 'Y');
insert into t_stats values (generate_series(501, 600), '127.0.0.1', 'N');
insert into t_stats values (generate_series(601, 700), '!@#$!%#!', 'N');
insert into t_stats values (generate_series(701, 800), '[1,2,3,4]', 'Y');
insert into t_stats values (generate_series(801, 900), '{"name":"Zhang San","age":18}'e":18}', 'Y');
insert into t_stats values (generate_series(901, 1000), '', 'N');

create table t_part(c1 int, c2 char(1), c3 text)
partition by list(c2) (
    partition t_part_list_r values ('r'),
    partition t_part_list_v values ('v'),
    partition t_part_list_i values ('i')
);
insert into t_part values (generate_series(1, 100), 'r', 'aabbcc');
insert into t_part values (generate_series(101, 200), 'v', '123dfg');
insert into t_part values (generate_series(201, 300), 'i', 'The face and peach blossoms reflect each other's red.');
insert into t_part values (generate_series(301, 400), 'r', 'fortunate');
insert into t_part values (generate_series(401, 500), 'v', 'open@gauss');
insert into t_part values (generate_series(501, 600), 'i', '127.0.0.1');
insert into t_part values (generate_series(601, 700), 'r', '!@#$!%#!');
insert into t_part values (generate_series(701, 800), 'v', '{"name":"Zhang San","age":18}'e":18}');
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
insert into t_sub_part values (generate_series(201, 300), 'i', 'The face and peach blossoms reflect each other's red.');
insert into t_sub_part values (generate_series(301, 400), 'r', 'fortunate');
insert into t_sub_part values (generate_series(401, 500), 'v', 'open@gauss');
insert into t_sub_part values (generate_series(501, 600), 'i', '127.0.0.1');
insert into t_sub_part values (generate_series(601, 700), 'r', '!@#$!%#!');
insert into t_sub_part values (generate_series(701, 800), 'v', '{"name":"Zhang San","age":18}'e":18}');
insert into t_sub_part values (generate_series(801, 900), 'i', '');
insert into t_sub_part values (generate_series(901, 920), 'r', 'Hello');
insert into t_sub_part values (generate_series(921, 960), 'v', 'Kitty');
insert into t_sub_part values (generate_series(961, 1000), 'v', 'Cats');
insert into t_sub_part values (1001, 'i', 'Dog');

create table t_stats_us (c1 int, c2 text, c3 char(1), constraint t_stats_us_pk primary key (c1))
with (storage_type=ustore);
insert into t_stats_us values (generate_series(1, 100), 'aabbcc', 'Y');
insert into t_stats_us values (generate_series(101, 200), '123dfg', 'N');
insert into t_stats_us values (generate_series(201, 300), 'Her face and peach blossoms reflect each other's glow', 'N');
insert into t_stats_us values (generate_series(301, 400), 'fortunate', 'Y');
insert into t_stats_us values (generate_series(401, 500), 'open@gauss', 'Y');
insert into t_stats_us values (generate_series(501, 600), '127.0.0.1', 'N');
insert into t_stats_us values (generate_series(601, 700), '!@#$!%#!', 'N');
insert into t_stats_us values (generate_series(701, 800), '[1,2,3,4]', 'Y');
insert into t_stats_us values (generate_series(801, 900), '{"name":"Zhang San","age":18}'e":18}', 'Y');
insert into t_stats_us values (generate_series(901, 1000), '', 'N');

create table t_stats_col (c1 int, c2 text, c3 char(1), constraint t_stats_col_pk primary key (c1))
with (orientation = column);

insert into t_stats_col values (generate_series(1, 100), 'aabbcc', 'Y');
insert into t_stats_col values (generate_series(101, 200), '123dfg', 'N');
insert into t_stats_col values (generate_series(201, 300), 'Her face and peach blossoms reflect each other's glow', 'N');
insert into t_stats_col values (generate_series(301, 400), 'fortunate', 'Y');
insert into t_stats_col values (generate_series(401, 500), 'open@gauss', 'Y');
insert into t_stats_col values (generate_series(501, 600), '127.0.0.1', 'N');
insert into t_stats_col values (generate_series(601, 700), '!@#$!%#!', 'N');
insert into t_stats_col values (generate_series(701, 800), '[1,2,3,4]', 'Y');
insert into t_stats_col values (generate_series(801, 900), '{"name":"Zhang San","age":18}'e":18}', 'Y');
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

### Deleting an Extension<a name="section1587441381220"></a>

The method for deleting the gms_stats extension in openGauss is as follows:

```
openGauss=# DROP Extension gms_stats [CASCADE];
```

>[!NOTE] Note
>
>If the extension is depended on by other objects, the CASCADE keyword must be added to delete all dependent objects.