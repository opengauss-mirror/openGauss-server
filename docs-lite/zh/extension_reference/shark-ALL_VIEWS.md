# ALL_VIEWS

返回所有系统视图和用户视图相关的信息。

**表1** ALL_VIEWS

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
            <td>视图名称</td>
        </tr>
        <tr>
            <td>object_id</td>
            <td>oid</td>
            <td>视图 ID</td>
        </tr>
        <tr>
            <td>principal_id</td>
            <td>oid</td>
            <td>对象owner的oid，如果视图的owner与视图所属schema的owner相同，则返回NULL，否则返回视图的owner</td>
        </tr>
        <tr>
            <td>schema_id</td>
            <td>oid</td>
            <td>所属 schema 的 ID</td>
        </tr>
        <tr>
            <td>parent_object_id</td>
            <td>oid</td>
            <td>对象所属的 parent 对象 ID，取值恒为0</td>
        </tr>
        <tr>
            <td>type</td>
            <td>char(2)</td>
            <td>对象类型，取值恒为V = VIEW</td>
        </tr>
        <tr>
            <td>type_desc</td>
            <td>nvarchar(60)</td>
            <td>对象类型描述，取值恒为VIEW</td>
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
            <td>是否为系统内部对象，取值恒为0</td>
        </tr>
        <tr>
            <td>is_published</td>
            <td>bit</td>
            <td>对象是否发布，取值恒为0</td>
        </tr>
        <tr>
            <td>is_schema_published</td>
            <td>bit</td>
            <td>是否只发布架构，取值恒为0</td>
        </tr>
        <tr>
            <td>is_replicated</td>
            <td>bit</td>
            <td>视图是否已复制，取值恒为0</td>
        </tr>
        <tr>
            <td>has_replication_filter</td>
            <td>bit</td>
            <td>视图是否具有复制筛选器，取值恒为0</td>
        </tr>
        <tr>
            <td>has_opaque_metadata</td>
            <td>bit</td>
            <td>视图是否指定了VIEW_METADATA选项，取值恒为0</td>
        </tr>
        <tr>
            <td>has_unchecked_assembly_data</td>
            <td>bit</td>
            <td>视图是否存在未经校验的程序集数据，取值恒为0</td>
        </tr>
        <tr>
            <td>with_check_option</td>
            <td>bit</td>
            <td>视图是否指定了WITH CHECK OPTION选项，1表示包含该选项，0表示不包含该选项</td>
        </tr>
        <tr>
            <td>is_date_correlation_view</td>
            <td>bit</td>
            <td>是否为系统自动创建视图，以存储datetime列之间的相关信息，取值恒为0</td>
        </tr>
        <tr>
            <td>is_tracked_by_cdc</td>
            <td>bit</td>
            <td>视图依赖的某个基表是否正在被CDC(数据变更捕获)跟踪，取值恒为0</td>
        </tr>
    </tbody>
</table>
