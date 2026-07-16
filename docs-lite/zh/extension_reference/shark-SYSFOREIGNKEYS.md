# SYSFOREIGNKEYS

返回外键约束相关的信息。

**表1** SYSFOREIGNKEYS

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
            <td>constid</td>
            <td>oid</td>
            <td>外键约束的id</td>
        </tr>
        <tr>
            <td>fkeyid</td>
            <td>oid</td>
            <td>外键约束所在的表的oid</td>
        </tr>
        <tr>
            <td>rkeyid</td>
            <td>oid</td>
            <td>外键约束所引用的表的oid</td>
        </tr>
        <tr>
            <td>fkey</td>
            <td>smallint</td>
            <td>外键约束对应的列的编号</td>
        </tr>
        <tr>
            <td>rkey</td>
            <td>smallint</td>
            <td>外键约束所参考的列的编号</td>
        </tr>
        <tr>
            <td>keyno</td>
            <td>smallint</td>
            <td>该列在外键约束对应列的位置，取值自1开始至外键约束列的个数</td>
        </tr>
    </tbody>
</table>
