# SERVER_PRINCIPALS

该视图中的每一行都对应一个服务器级主体

**表1** SERVER_PRINCIPALS

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
            <td>主体名称, 返回pg_roles.rolname</td>
        </tr>
        <tr>
            <td>principal_id</td>
            <td>oid</td>
            <td>主体 ID, 返回pg_roles.oid</td>
        </tr>
        <tr>
            <td>sid</td>
            <td>varbinary(85)</td>
            <td>主体 Security Identifier, 返回varbinary格式的pg_roles.oid</td>
        </tr>
        <tr>
            <td>type</td>
            <td>char(1)</td>
            <td>主体类型, S为SQL LOGIN, R为SERVER ROLE</td>
        </tr>
        <tr>
            <td>type_desc</td>
            <td>nvarchar2(60)</td>
            <td>主体类型的具体描述, 主体类型为S则该列为SQL_LOGIN, 主体类型为R则该列为SERVER ROLE</td>
        </tr>
        <tr>
            <td>is_disabled</td>
            <td>int</td>
            <td>主体是否禁止登录, pg_roles.rolcanlogin为0时该项为1,否则为0</td>
        </tr>
        <tr>
            <td>create_date</td>
            <td>timestamp</td>
            <td>返回NULL</td>
        </tr>
        <tr>
            <td>modify_date</td>
            <td>timestamp</td>
            <td>返回NULL</td>
        </tr>
        <tr>
            <td>default_database_name</td>
            <td>name</td>
            <td>返回NULL</td>
        </tr>
        <tr>
            <td>default_language_name</td>
            <td>name</td>
            <td>返回'english'</td>
        </tr>
        <tr>
            <td>credential_id</td>
            <td>int</td>
            <td>返回-1</td>
        </tr>
        <tr>
            <td>owning_principal_id</td>
            <td>int</td>
            <td>返回-1</td>
        </tr>
        <tr>
            <td>is_fixed_role</td>
            <td>int</td>
            <td>返回-1</td>
        </tr>
    </tbody>
</table>

>[!NOTE]说明 
>
>type和type\_desc仅有'SQL\_LOGIN'和'SERVER\_ROLE'两种类型, 当用户为审计管理员(rolauditadmin)、系统管理员(rolsystemadmin)、 监控管理员(rolmonitoradmin)、 运维管理员(roloperatoradmin)和安全策略管理员(rolpolicyadmin)其中一个或者多个时, 该用户为'SERVER_ROLE'。当用户不为上述任一角色且能够登录数据库，即'rolcanlogin'为't'时，该用户为'SQL\_LOGIN'。
