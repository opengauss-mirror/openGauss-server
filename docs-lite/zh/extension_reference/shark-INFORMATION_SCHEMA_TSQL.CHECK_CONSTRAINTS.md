# CHECK_CONSTRAINTS

CHECK_CONSTRAINTS视图返回数据库中的检查约束信息。

**表1** CHECK_CONSTRAINTS

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
            <td>CONSTRAINT_CATALOG</td>
            <td>nvarchar(128)</td>
            <td>约束限定符</td>
        </tr>
        <tr>
            <td>CONSTRAINT_SCHEMA</td>
            <td>nvarchar(128)</td>
            <td>约束所属架构的名称</td>
        </tr>
        <tr>
            <td>CONSTRAINT_NAME</td>
            <td>name</td>
            <td>约束名称</td>
        </tr>
        <tr>
            <td>CHECK_CLAUSE</td>
            <td>nvarchar（4000）</td>
            <td>Transact-SQL 定义语句的实际文本</td>
        </tr>
    </tbody>
</table>
