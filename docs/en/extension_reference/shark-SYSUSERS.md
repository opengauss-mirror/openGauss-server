# SYSUSERS

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-09-13T02:20:22.411Z pushedAt=2026-09-29T02:34:56.684Z -->

Returns information about users in the database.

**Table 1** SYSUSERS

<table aria-label="表1" class="table table-sm margin-top-none">
    <thead>
        <tr>
            <th>Column</th>
            <th>Type</th>
            <th>Description</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>uid</td>
            <td>smallint</td>
            <td>User ID, which is unique in the database.</td>
        </tr>
        <tr>
            <td>status</td>
            <td>smallint</td>
            <td>For internal reference only. Not supported. Future compatibility is not guaranteed. Returns 0.</td>
        </tr>
        <tr>
            <td>name</td>
            <td>name</td>
            <td>Username or group name, unique in the database.</td>
        </tr>
        <tr>
            <td>sid</td>
            <td>varbinary(85)</td>
            <td>Returns NULL.</td>
        </tr>
        <tr>
            <td>roles</td>
            <td>varbinary(2048)</td>
            <td>For reference only. Not supported. Future compatibility is not guaranteed. Returns NULL.</td>
        </tr>
        <tr>
            <td>createdate</td>
            <td>date</td>
            <td>Returns NULL.</td>
        </tr>
        <tr>
            <td>updatedate</td>
            <td>date</td>
            <td>Returns NULL.</td>
        </tr>
        <tr>
            <td>altuid</td>
            <td>smallint</td>
            <td>For reference only. Not supported. Future compatibility is not guaranteed. Returns 0.</td>
        </tr>
        <tr>
            <td>password</td>
            <td>varbinary(256)</td>
            <td>For reference only. Not supported. Future compatibility is not guaranteed. Returns NULL.</td>
        </tr>
        <tr>
            <td>gid</td>
            <td>smallint</td>
            <td>Returns 0.</td>
        </tr>
        <tr>
            <td>environ</td>
            <td>varchar(255)</td>
            <td>Reserved. Returns NULL.</td>
        </tr>
        <tr>
            <td>hasdbaccess</td>
            <td>int</td>
            <td>1 = The account has database access permission.</td>
        </tr>
        <tr>
            <td>islogin</td>
            <td>int</td>
            <td>1 = The account has login permission.</td>
        </tr>
        <tr>
            <td>isntname</td>
            <td>int</td>
            <td>Returns 0.</td>
        </tr>
        <tr>
            <td>isntgroup</td>
            <td>int</td>
            <td>Returns 0.</td>
        </tr>
        <tr>
            <td>isntuser</td>
            <td>int</td>
            <td>Returns 0.</td>
        </tr>
        <tr>
            <td>issqluser</td>
            <td>int</td>
            <td>1 = The account is an SQL user.</td>
        </tr>
        <tr>
            <td>isaliased</td>
            <td>int</td>
            <td>Returns 0.</td>
        </tr>
        <tr>
            <td>issqlrole</td>
            <td>int</td>
            <td>1 = The account is an SQL role.</td>
        </tr>
        <tr>
            <td>isapprole</td>
            <td>int</td>
            <td>Returns 0.</td>
        </tr>
    </tbody>
</table>
