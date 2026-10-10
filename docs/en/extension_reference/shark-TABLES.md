# TABLES

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-09-13T02:23:01.214Z pushedAt=2026-09-29T02:34:56.687Z -->

User-defined tables in all schemas.

**Table 1** TABLES

<table aria-label="表1" class="table table-sm margin-top-none">
    <thead>
        <tr>
            <th>Column Name</th>
            <th>Type</th>
            <th>Description</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>name</td>
            <td>name</td>
            <td>Object name.</td>
        </tr>
        <tr>
            <td>object_id</td>
            <td>oid</td>
            <td>Object ID.</td>
        </tr>
        <tr>
            <td>principal_id</td>
            <td>oid</td>
            <td>OID of the object owner.<br>If the current owner is the same as the schema owner, NULL is returned.</td>
        </tr>
        <tr>
            <td>schema_id</td>
            <td>oid</td>
            <td>ID of the schema to which the table belongs.</td>
        </tr>
        <tr>
            <td>parent_object_id</td>
            <td>oid</td>
            <td>Returns the parent object ID that the object belongs to.</td>
        </tr>
        <tr>
            <td>type</td>
            <td>char(2)</td>
            <td>Object type. Always U.</td>
        </tr>
        <tr>
            <td>type_desc</td>
            <td>nvarchar(60)</td>
            <td>Object type description. Always USER_TABLE.</td>
        </tr>
        <tr>
            <td>create_date</td>
            <td>timestamp</td>
            <td>Object creation date.</td>
        </tr>
        <tr>
            <td>modify_date</td>
            <td>timestamp</td>
            <td>Object modification date.</td>
        </tr>
        <tr>
            <td>is_ms_shipped</td>
            <td>bit</td>
            <td>Whether the object is an internal system object.<br>Returns `1` for system tables, views, etc.<br>Returns `0` for user tables, etc.</td>
        </tr>
        <tr>
            <td>is_published</td>
            <td>bit</td>
            <td>Whether the object is published.</td>
        </tr>
        <tr>
            <td>is_schema_published</td>
            <td>bit</td>
            <td>Whether only the schema is published.</td>
        </tr>
  <tr>
            <td>lob_data_space_id</td>
            <td>oid</td>
            <td>ID of the corresponding TOAST table.</td>
        </tr>
  <tr>
            <td>filestream_data_space_id</td>
            <td>int</td>
            <td>Returns NULL.</td>
        </tr>
  <tr>
            <td>max_column_id_used</td>
            <td>int</td>
            <td>Maximum column ID.</td>
        </tr>
  <tr>
            <td>lock_on_bulk_load</td>
            <td>bit</td>
            <td>Returns 0.</td>
        </tr>
  <tr>
            <td>uses_ansi_nulls</td>
            <td>bit</td>
            <td>Returns 1.</td>
        </tr>
  <tr>
            <td>is_replicated</td>
            <td>bit</td>
            <td>1 indicates a transaction-based publication.</td>
        </tr>
  <tr>
            <td>has_replication_filter</td>
            <td>bit</td>
            <td>Returns 0.</td>
        </tr>
  <tr>
            <td>is_merge_published</td>
            <td>bit</td>
            <td>Returns 0.</td>
        </tr>
  <tr>
            <td>is_sync_tran_subscribed</td>
            <td>bit</td>
            <td>Returns 0.</td>
        </tr>
  <tr>
            <td>has_unchecked_assembly_data</td>
            <td>bit</td>
            <td>Returns 0.</td>
        </tr>
  <tr>
            <td>text_in_row_limit</td>
            <td>int</td>
            <td>Returns 0.</td>
        </tr>
  <tr>
            <td>large_value_types_out_of_row</td>
            <td>bit</td>
            <td>Returns 0.</td>
        </tr>
  <tr>
            <td>is_tracked_by_cdc</td>
            <td>tinyint</td>
            <td>Returns 0.</td>
        </tr>
  <tr>
            <td>lock_escalation</td>
            <td>tinyint</td>
            <td>Returns 1.</td>
        </tr>
  <tr>
            <td>lock_escalation_desc</td>
            <td>nvarchar(60)</td>
            <td>Returns DISABLE.</td>
        </tr>
  <tr>
            <td>is_filetable</td>
            <td>bit</td>
            <td>Returns 0.</td>
        </tr>
  <tr>
            <td>is_memory_optimized</td>
            <td>bit</td>
            <td>1 indicates that the table is an MOT table.</td>
        </tr>
  <tr>
            <td>durability</td>
            <td>tinyint</td>
            <td>Returns 0.</td>
        </tr>
  <tr>
            <td>durability_desc</td>
            <td>nvarchar(60)</td>
            <td>Returns SCHEMA_AND_DATA.</td>
        </tr>
  <tr>
            <td>temporal_type</td>
            <td>tinyint</td>
            <td>2: temporal tables<br>0: otherwise</td>
        </tr>
  <tr>
            <td>temporal_type_desc</td>
            <td>nvarchar(60)</td>
            <td>SYSTEM_VERSIONED_TEMPORAL_TABLE: temporal tables<br>NON_TEMPORAL_TABLE: otherwise</td>
        </tr>
  <tr>
            <td>history_table_id</td>
            <td>int</td>
            <td>Returns NULL.</td>
        </tr>
  <tr>
            <td>is_remote_data_archive_enabled</td>
            <td>bit</td>
            <td>Returns 0.</td>
        </tr>
  <tr>
            <td>is_external</td>
            <td>bit</td>
            <td>1 indicates an external table.</td>
        </tr>
  <tr>
            <td>history_retention_period</td>
            <td>int</td>
            <td>Returns 0.</td>
        </tr>
  <tr>
            <td>history_retention_period_unit</td>
            <td>int</td>
            <td>Returns -1.</td>
        </tr>
  <tr>
            <td>history_retention_period_unit_desc</td>
            <td>nvarchar(10)</td>
            <td>Returns INFINITE.</td>
        </tr>
  <tr>
            <td>is_node</td>
            <td>bit</td>
            <td>Node table of the graph database.</td>
        </tr>
  <tr>
            <td>is_edge</td>
            <td>bit</td>
            <td>Edge table of the graph database.</td>
        </tr>
    </tbody>
</table>
