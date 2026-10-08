# SERVER_PRINCIPALS

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-07-30T06:34:37.163Z pushedAt=2026-09-22T07:16:10.921Z -->

Each row in this view corresponds to a server-level principal.

**Table 1** SERVER_PRINCIPALS

<table aria-label="Table 1" class="table table-sm margin-top-none">
    <thead>
        <tr>
            <th>Column Name</th>
            <th>Type</th>
            <th>Description</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>name</td>
            <td>name</td>
            <td>Principal name, returns pg_roles.rolname</td>
        </tr>
        <tr>
            <td>principal_id</td>
            <td>oid</td>
            <td>Principal ID, returns pg_roles.oid</td>
        </tr>
        <tr>
            <td>sid</td>
            <td>varbinary(85)</td>
            <td>Principal Security Identifier, returns pg_roles.oid in varbinary format</td>
        </tr>
        <tr>
            <td>type</td>
            <td>char(1)</td>
            <td>Principal Type. S indicates SQL_LOGIN, and R indicates SERVER_ROLE.</td>
        </tr>
        <tr>
            <td>type_desc</td>
            <td>nvarchar2(60)</td>
            <td>Specific description of the principal type. If the principal type is S, this column is SQL_LOGIN; if the principal type is R, this column is SERVER_ROLE.</td>
        </tr>
        <tr>
            <td>is_disabled</td>
            <td>int</td>
            <td>Whether the principal is prohibited from logging in. When pg_roles.rolcanlogin is 0, this item is 1; otherwise, it is 0.</td>
        </tr>
        <tr>
            <td>create_date</td>
            <td>timestamp</td>
            <td>Returns NULL</td>
        </tr>
        <tr>
            <td>modify_date</td>
            <td>timestamp</td>
            <td>Returns NULL</td>
        </tr>
        <tr>
            <td>default_database_name</td>
            <td>name</td>
            <td>Returns NULL</td>
        </tr>
        <tr>
            <td>default_language_name</td>
            <td>name</td>
            <td>Returns 'english'</td>
        </tr>
        <tr>
            <td>credential_id</td>
            <td>int</td>
            <td>Returns -1</td>
        </tr>
        <tr>
            <td>owning_principal_id</td>
            <td>int</td>
            <td>Returns -1</td>
        </tr>
        <tr>
            <td>is_fixed_role</td>
            <td>int</td>
            <td>Returns -1</td>
        </tr>
    </tbody>
</table>

>[!NOTE]Note
>
>type and type_desc have only two types: 'SQL_LOGIN' and 'SERVER_ROLE'. When a user holds one or more of the following roles: audit administrator (rolauditadmin), system administrator (rolsystemadmin), monitoring administrator (rolmonitoradmin), O&M administrator (roloperatoradmin), or security policy administrator (rolpolicyadmin), the user is classified as 'SERVER_ROLE'. When a user does not hold any of the above roles and can log in to the database, that is, 'rolcanlogin' is 't', the user is classified as 'SQL_LOGIN'.