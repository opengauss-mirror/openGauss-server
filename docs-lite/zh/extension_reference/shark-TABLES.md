# TABLES

所有架构范围内的用户定义表。

**表1** TABLES

<table aria-label="表1" class="table table-sm margin-top-none">
    <thead>
        <tr>
            <th>列名称</th>
            <th>类型</th>
            <th>说明</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>name</td>
            <td>name</td>
            <td>对象名称</td>
        </tr>
        <tr>
            <td>object_id</td>
            <td>oid</td>
            <td>对象 ID</td>
        </tr>
        <tr>
            <td>principal_id</td>
            <td>oid</td>
            <td>对象 owner 的oid。<br/>如果当前owner与schema为同一个owner，则返回NULL。</td>
        </tr>
        <tr>
            <td>schema_id</td>
            <td>oid</td>
            <td>所属 schema 的 ID</td>
        </tr>
        <tr>
            <td>parent_object_id</td>
            <td>oid</td>
            <td>返回对象所属的 parent 对象 ID</td>
        </tr>
        <tr>
            <td>type</td>
            <td>char(2)</td>
            <td>对象类型。固定返回 U</td>
        </tr>
        <tr>
            <td>type_desc</td>
            <td>nvarchar(60)</td>
            <td>对象类型描述。固定返回 USER_TABLE</td>
        </tr>
        <tr>
            <td>create_date</td>
            <td>timestamp</td>
            <td>对象创建日期</td>
        </tr>
        <tr>
            <td>modify_date</td>
            <td>timestamp</td>
            <td>对象修改日期</td>
        </tr>
        <tr>
            <td>is_ms_shipped</td>
            <td>bit</td>
            <td>是否为系统内部对象<br/>如系统表、视图等返回 1<br>用户表等返回 0</td>
        </tr>
        <tr>
            <td>is_published</td>
            <td>bit</td>
            <td>对象是否发布</td>
        </tr>
        <tr>
            <td>is_schema_published</td>
            <td>bit</td>
            <td>是否只发布架构</td>
        </tr>
  <tr>
            <td>lob_data_space_id</td>
            <td>oid</td>
            <td>对应的 toast 表的 ID</td>
        </tr>
  <tr>
            <td>filestream_data_space_id</td>
            <td>int</td>
            <td>返回 NULL</td>
        </tr>
  <tr>
            <td>max_column_id_used</td>
            <td>int</td>
            <td>列的最大 ID</td>
        </tr>
  <tr>
            <td>lock_on_bulk_load</td>
            <td>bit</td>
            <td>返回 0</td>
        </tr>
  <tr>
            <td>uses_ansi_nulls</td>
            <td>bit</td>
            <td>返回 1</td>
        </tr>
  <tr>
            <td>is_replicated</td>
            <td>bit</td>
            <td>1 表示为基于事务的发布</td>
        </tr>
  <tr>
            <td>has_replication_filter</td>
            <td>bit</td>
            <td>返回 0</td>
        </tr>
  <tr>
            <td>is_merge_published</td>
            <td>bit</td>
            <td>返回 0</td>
        </tr>
  <tr>
            <td>is_sync_tran_subscribed</td>
            <td>bit</td>
            <td>返回 0</td>
        </tr>
  <tr>
            <td>has_unchecked_assembly_data</td>
            <td>bit</td>
            <td>返回 0</td>
        </tr>
  <tr>
            <td>text_in_row_limit</td>
            <td>int</td>
            <td>返回 0</td>
        </tr>
  <tr>
            <td>large_value_types_out_of_row</td>
            <td>bit</td>
            <td>返回 0</td>
        </tr>
  <tr>
            <td>is_tracked_by_cdc</td>
            <td>tinyint</td>
            <td>返回 0</td>
        </tr>
  <tr>
            <td>lock_escalation</td>
            <td>tinyint</td>
            <td>返回 1</td>
        </tr>
  <tr>
            <td>lock_escalation_desc</td>
            <td>nvarchar(60)</td>
            <td>返回 DISABLE</td>
        </tr>
  <tr>
            <td>is_filetable</td>
            <td>bit</td>
            <td>返回 0</td>
        </tr>
  <tr>
            <td>is_memory_optimized</td>
            <td>bit</td>
            <td>1 表示该表为MOT表</td>
        </tr>
  <tr>
            <td>durability</td>
            <td>tinyint</td>
            <td>返回 0</td>
        </tr>
  <tr>
            <td>durability_desc</td>
            <td>nvarchar(60)</td>
            <td>返回 SCHEMA_AND_DATA</td>
        </tr>
  <tr>
            <td>temporal_type</td>
            <td>tinyint</td>
            <td>2 为临时表<br/>0 其它情况</td>
        </tr>
  <tr>
            <td>temporal_type_desc</td>
            <td>nvarchar(60)</td>
            <td>SYSTEM_VERSIONED_TEMPORAL_TABLE 为临时表<br/>NON_TEMPORAL_TABLE 其它情况</td>
        </tr>
  <tr>
            <td>history_table_id</td>
            <td>int</td>
            <td>返回 NULL</td>
        </tr>
  <tr>
            <td>is_remote_data_archive_enabled</td>
            <td>bit</td>
            <td>返回 0</td>
        </tr>
  <tr>
            <td>is_external</td>
            <td>bit</td>
            <td>1 表示外部表</td>
        </tr>
  <tr>
            <td>history_retention_period</td>
            <td>int</td>
            <td>返回 0</td>
        </tr>
  <tr>
            <td>history_retention_period_unit</td>
            <td>int</td>
            <td>返回 -1</td>
        </tr>
  <tr>
            <td>history_retention_period_unit_desc</td>
            <td>nvarchar(10)</td>
            <td>返回 INFINITE</td>
        </tr>
  <tr>
            <td>is_node</td>
            <td>bit</td>
            <td>图数据库的节点表</td>
        </tr>
  <tr>
            <td>is_edge</td>
            <td>bit</td>
            <td>图数据库的边表</td>
        </tr>
    </tbody>
</table>
