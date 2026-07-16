# SYSCONFIGURES

返回参数相关的信息。

**表1** SYSCONFIGURES

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
            <td>value</td>
            <td>sql_variant</td>
            <td>参数的当前值</td>
        </tr>
        <tr>
            <td>config</td>
            <td>int</td>
            <td>参数的id，取值恒为NULL</td>
        </tr>
        <tr>
            <td>comment</td>
            <td>nvarchar(255)</td>
            <td>参数的简单描述</td>
        </tr>
        <tr>
            <td>status</td>
            <td>smallint</td>
            <td>参数的状态类型，0表示静态，1表示动态<br/>
            针对postmaster和internal级别的参数，取值为0<br/>
            针对sighup、backend、suset、userset级别的参数，取值为1
            </td>
        </tr>
    </tbody>
</table>
