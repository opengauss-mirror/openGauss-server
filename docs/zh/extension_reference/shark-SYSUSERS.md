# SYSUSERS

SYSUSERS视图返回数据库中的用户信息。

**表1** SYSUSERS

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
            <td>uid</td>
            <td>smallint</td>
            <td>用户 ID，在此数据库中是唯一的</td>
        </tr>
        <tr>
            <td>status</td>
            <td>smallint</td>
            <td>标识为仅供参考。 不支持。 不保证以后的兼容性。返回0</td>
        </tr>
        <tr>
            <td>name</td>
            <td>name</td>
            <td>用户名或组名，在此数据库中是唯一的。</td>
        </tr>
        <tr>
            <td>sid</td>
            <td>varbinary(85)</td>
            <td>返回NULL</td>
        </tr>
        <tr>
            <td>roles</td>
            <td>varbinary(2048)</td>
            <td>标识为仅供参考。 不支持。 不保证以后的兼容性。返回NULL</td>
        </tr>
        <tr>
            <td>createdate</td>
            <td>date</td>
            <td>返回NULL</td>
        </tr>
        <tr>
            <td>updatedate</td>
            <td>date</td>
            <td>返回NULL</td>
        </tr>
        <tr>
            <td>altuid</td>
            <td>smallint</td>
            <td>标识为仅供参考。 不支持。 不保证以后的兼容性。返回0</td>
        </tr>
        <tr>
            <td>password</td>
            <td>varbinary(256)</td>
            <td>标识为仅供参考。 不支持。 不保证以后的兼容性。返回NULL</td>
        </tr>
        <tr>
            <td>gid</td>
            <td>smallint</td>
            <td>返回0</td>
        </tr>
        <tr>
            <td>environ</td>
            <td>varchar(255)</td>
            <td>保留。返回NULL</td>
        </tr>
        <tr>
            <td>hasdbaccess</td>
            <td>int</td>
            <td>1 = 帐户具有数据库访问权。</td>
        </tr>
        <tr>
            <td>islogin</td>
            <td>int</td>
            <td>1 = 帐户具有登录权限。</td>
        </tr>
        <tr>
            <td>isntname</td>
            <td>int</td>
            <td>返回0</td>
        </tr>
        <tr>
            <td>isntgroup</td>
            <td>int</td>
            <td>返回0</td>
        </tr>
        <tr>
            <td>isntuser</td>
            <td>int</td>
            <td>返回0</td>
        </tr>
        <tr>
            <td>issqluser</td>
            <td>int</td>
            <td>1 = 帐户具是SQL用户。</td>
        </tr>
        <tr>
            <td>isaliased</td>
            <td>int</td>
            <td>返回0</td>
        </tr>
        <tr>
            <td>issqlrole</td>
            <td>int</td>
            <td>1 = 帐户具是SQL角色。</td>
        </tr>
        <tr>
            <td>isapprole</td>
            <td>int</td>
            <td>返回0</td>
        </tr>
    </tbody>
</table>
