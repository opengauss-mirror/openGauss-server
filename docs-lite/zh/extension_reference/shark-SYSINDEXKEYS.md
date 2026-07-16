# SYSINDEXKEYS

SYSINDEXKEYS视图包含有关数据库的索引中列的信息。

**表1** SYSINDEXKEYS视图字段

<table aria-label="表1" class="table table-sm margin-top-none">
    <thead>
        <tr>
            <th>列名称</th>
            <th>数据类型</th>
            <th>说明</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>id</td>
            <td>oid</td>
            <td>表的 ID。</td>
        </tr>
        <tr>
            <td>indid</td>
            <td>oid</td>
            <td>索引的 ID。</td>
        </tr>
        <tr>
            <td>colid</td>
            <td>smallint</td>
            <td>列的 ID。</td>
        </tr>
        <tr>
            <td>keyno</td>
            <td>smallint</td>
            <td>该列在索引中的位置。</td>
        </tr>
    </tbody>
</table>
