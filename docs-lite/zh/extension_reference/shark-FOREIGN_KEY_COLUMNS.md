# FOREIGN_KEY_COLUMNS

返回外键约束相关的信息。

**表1** FOREIGN_KEY_COLUMNS

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
            <td>constraint_object_id</td>
            <td>oid</td>
            <td>外键约束的id</td>
        </tr>
        <tr>
            <td>constraint_column_id</td>
            <td>int</td>
            <td>组成外键的列或列集的id(1...n，其中n为列数)</td>
        </tr>
        <tr>
            <td>parent_object_id</td>
            <td>oid</td>
            <td>外键约束所在的表的oid</td>
        </tr>
        <tr>
            <td>parent_column_id</td>
            <td>int</td>
            <td>外键约束对应的列的编号</td>
        </tr>
        <tr>
            <td>referenced_object_id</td>
            <td>oid</td>
            <td>外键约束所引用的表的oid</td>
        </tr>
        <tr>
            <td>referenced_column_id</td>
            <td>int</td>
            <td>外键约束所参考的列的编号</td>
        </tr>
    </tbody>
</table>
