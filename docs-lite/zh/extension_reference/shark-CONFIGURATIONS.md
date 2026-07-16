# CONFIGURATIONS

返回参数相关的信息。

**表1** CONFIGURATIONS

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
            <td>configuration_id</td>
            <td>int</td>
            <td>参数的id，取值恒为NULL</td>
        </tr>
        <tr>
            <td>name</td>
            <td>nvarchar(35)</td>
            <td>参数名称</td>
        </tr>
        <tr>
            <td>value</td>
            <td>sql_variant</td>
            <td>参数的当前值</td>
        </tr>
        <tr>
            <td>minimum</td>
            <td>sql_variant</td>
            <td>参数的最小值</td>
        </tr>
        <tr>
            <td>maximum</td>
            <td>sql_variant</td>
            <td>参数的最大值</td>
        </tr>
        <tr>
            <td>value_in_use</td>
            <td>sql_variant</td>
            <td>参数的当前值</td>
        </tr>
        <tr>
            <td>description</td>
            <td>nvarchar(255)</td>
            <td>参数的简单描述</td>
        </tr>
        <tr>
            <td>is_dynamic</td>
            <td>bit</td>
            <td>是否为动态生效的参数，1表示动态生效，0表示非动态生效<br/>
            针对postmaster和internal级别的参数，取值为0<br/>
            针对sighup、backend、suset、userset级别的参数，取值为1
            </td>
        </tr>
        <tr>
            <td>is_advanced</td>
            <td>bit</td>
            <td>是否为高级参数，取值恒为0</td>
        </tr>
    </tbody>
</table>
