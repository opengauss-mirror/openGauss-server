# SYSDATABASES

SYSDATABASES视图返回数据库的信息。

**表1** SYSDATABASES

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
            <td>数据库名称</td>
        </tr>
        <tr>
            <td>dbid</td>
            <td>smallint</td>
            <td>数据库 ID</td>
        </tr>
        <tr>
            <td>sid</td>
            <td>varbinary(85)</td>
            <td>数据库创建者的系统 ID</td>
        </tr>
        <tr>
            <td>mode</td>
            <td>smallint</td>
            <td>返回0</td>
        </tr>
        <tr>
            <td>status</td>
            <td>int</td>
            <td>返回0</td>
        </tr>
        <tr>
            <td>status2</td>
            <td>int</td>
            <td>返回0</td>
        </tr>
        <tr>
            <td>crdate</td>
            <td>timestamp</td>
            <td>返回1900-01-01 00：00：00.000</td>
        </tr>
        <tr>
            <td>reserved</td>
            <td>timestamp</td>
            <td>保留供将来使用 。返回1900-01-01 00：00：00.000</td>
        </tr>
        <tr>
            <td>category</td>
            <td>int</td>
            <td>返回0</td>
        </tr>
        <tr>
            <td>cmptlevel</td>
            <td>tinyint</td>
            <td>返回0</td>
        </tr>
        <tr>
            <td>filename</td>
            <td>nvarchar(260)</td>
            <td>返回NULL</td>
        </tr>
        <tr>
            <td>version</td>
            <td>smallint</td>
            <td>返回NULL</td>
        </tr>
    </tbody>
</table>
