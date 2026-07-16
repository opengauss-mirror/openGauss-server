# TABLES

TABLES视图返回数据库中的表或视图信息。

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
            <td>TABLE_CATALOG</td>
            <td>nvarchar(128)</td>
            <td>表限定符</td>
        </tr>
        <tr>
            <td>TABLE_SCHEMA</td>
            <td>nvarchar(128)</td>
            <td>包含该表的架构的名称</td>
        </tr>
        <tr>
            <td>TABLE_NAME</td>
            <td>name</td>
            <td>表或视图名称</td>
        </tr>
        <tr>
            <td>TABLE_TYPE</td>
            <td>varchar（10）</td>
            <td>表的类型。 可以是 VIEW 或 BASE TABLE。</td>
        </tr>
    </tbody>
</table>
