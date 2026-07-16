# VIEWS

所有架构范围内的用户定义视图。

**表1** VIEWS

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
            <td>对象类型</td>
        </tr>
        <tr>
            <td>type_desc</td>
            <td>nvarchar(60)</td>
            <td>对象类型描述</td>
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
            <td>is_replicated</td>
            <td>bit</td>
            <td>返回 0</td>
        </tr>
        <tr>
            <td>has_replication_filter</td>
            <td>bit</td>
            <td>返回 0</td>
        </tr>
        <tr>
            <td>has_opaque_metadata</td>
            <td>bit</td>
            <td>返回 0</td>
        </tr>
        <tr>
            <td>has_unchecked_assembly_data</td>
            <td>bit</td>
            <td>返回 0</td>
        </tr>
        <tr>
            <td>with_check_option</td>
            <td>bit</td>
            <td>1 视图存在 WITH CHECK OPTION 选项</td>
        </tr>
        <tr>
            <td>is_date_correlation_view</td>
            <td>bit</td>
            <td>返回 0</td>
        </tr>
        <tr>
            <td>is_tracked_by_cdc</td>
            <td>bit</td>
            <td>视图依赖的某个基表是否正在被CDC(数据变更捕获)跟踪，取值恒为0</td>
        </tr>
    </tbody>
</table>
