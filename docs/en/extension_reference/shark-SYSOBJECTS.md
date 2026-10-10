# SYSOBJECTS

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-09-13T02:11:40.071Z pushedAt=2026-09-29T02:34:56.675Z -->

Returns every object created in the database (for example, tables, views, indexes, constraints, defaults, and stored procedures).

**Table 1** SYSOBJECTS

<table aria-label="表 1" class="table table-sm margin-top-none">
    <thead>
        <tr>
            <th>Column Name</th>
            <th>Data Type</th>
            <th>Description</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>name</td>
            <td>name</td>
            <td>Object name.</td>
        </tr>
        <tr>
            <td>id</td>
            <td>oid</td>
            <td>OID of the object.</td>
        </tr>
        <tr>
            <td>xtype</td>
            <td>char(2)</td>
            <td>Supported types:<br>
                AF: aggregate function<br>
                C: check constraint<br>
                D: default or DEFAULT constraint<br>
                F: FOREIGN KEY constraint<br>
                FN: function<br>
                P: stored procedure<br>
                PK: primary key constraint<br>
                S: system table<br>
                SN: synonym<br>
                SO: sequence<br>
                TR: trigger<br>
                U: user table<br>
                UQ: UNIQUE constraint<br>
                V: view<br>
            </td>
        </tr>
        <tr>
            <td>uid</td>
            <td>oid</td>
            <td>Schema ID of the object.</td>
        </tr>
        <tr>
            <td>info</td>
            <td>smallint</td>
            <td>Returns a fixed value of 0.</td>
        </tr>
        <tr>
            <td>status</td>
            <td>int</td>
            <td>Returns a fixed value of 0.</td>
        </tr>
        <tr>
            <td>base_schema_ver</td>
            <td>int</td>
            <td>Returns a fixed value of 0.</td>
        </tr>
        <tr>
            <td>replinfo</td>
            <td>int</td>
            <td>Returns a fixed value of 0.</td>
        </tr>
        <tr>
            <td>parent_obj</td>
            <td>oid</td>
            <td>Object ID of the parent object.</td>
        </tr>
        <tr>
            <td>crdate</td>
            <td>timestamp(3)</td>
            <td>Returns the fixed value NULL.</td>
        </tr>
        <tr>
            <td>ftcatid</td>
            <td>smallint</td>
            <td>Returns the fixed value 0.</td>
        </tr>
        <tr>
            <td>schema_ver</td>
            <td>int</td>
            <td>Version number that is incremented each time the schema of a table changes. Always returns 0.</td>
        </tr>
        <tr>
            <td>stats_schema_ver</td>
            <td>int</td>
            <td>Returns a fixed value of 0.</td>
        </tr>
        <tr>
            <td>type</td>
            <td>char(2)</td>
            <td>Object type. The following values are supported:<br>
                AF: aggregate function<br>
                C: check constraint<br>
                D: default or DEFAULT constraint<br>
                F: FOREIGN KEY constraint<br>
                FN: function<br>
                K: primary key or unique constraint<br>
                P: stored procedure<br>
                S: system table<br>
                SN: synonym<br>
                TR: trigger<br>
                U: user table<br>
                V: view<br>
            </td>
        </tr>
        <tr>
            <td>userstat</td>
            <td>smallint</td>
            <td>Returns the fixed value 0.</td>
        </tr>
        <tr>
            <td>sysstat</td>
            <td>smallint</td>
            <td>Returns the fixed value 0.</td>
        </tr>
        <tr>
            <td>indexdel</td>
            <td>smallint</td>
            <td>Returns the fixed value 0.</td>
        </tr>
        <tr>
            <td>refdate</td>
            <td>timestamp</td>
            <td>Returns the fixed value NULL.</td>
        </tr>
        <tr>
            <td>version</td>
            <td>int</td>
            <td>Returns the fixed value 0.</td>
        </tr>
        <tr>
            <td>deltrig</td>
            <td>int</td>
            <td>Returns the fixed value 0.</td>
        </tr>
        <tr>
            <td>instrig</td>
            <td>int</td>
            <td>Returns the fixed value 0.</td>
        </tr>
        <tr>
            <td>updtrig</td>
            <td>int</td>
            <td>Returns the fixed value 0.</td>
        </tr>
        <tr>
            <td>seltrig</td>
            <td>int</td>
            <td>Returns the fixed value 0.</td>
        </tr>
        <tr>
            <td>category</td>
            <td>int</td>
            <td>Used for publication, constraint, and identification. Returns the fixed value 0.</td>
        </tr>
        <tr>
            <td>cache</td>
            <td>smallint</td>
            <td>Returns the fixed value 0.</td>
        </tr>
    </tbody>
</table>
