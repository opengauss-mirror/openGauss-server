# SYSFOREIGNKEYS

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-09-13T02:05:55.226Z pushedAt=2026-09-29T02:34:56.666Z -->

Returns information about foreign key constraints.

**Table 1** SYSFOREIGNKEYS

<table aria-label="表1" class="table table-sm margin-top-none">
    <thead>
        <tr>
            <th>Column Name</th>
            <th>Type</th>
            <th>Description</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>constid</td>
            <td>oid</td>
            <td>ID of the foreign key constraint.</td>
        </tr>
        <tr>
            <td>fkeyid</td>
            <td>oid</td>
            <td>OID of the table containing the foreign key constraint.</td>
        </tr>
        <tr>
            <td>rkeyid</td>
            <td>oid</td>
            <td>OID of the table referenced by the foreign key constraint.</td>
        </tr>
        <tr>
            <td>fkey</td>
            <td>smallint</td>
            <td>Number of the column corresponding to the foreign key constraint.</td>
        </tr>
        <tr>
            <td>rkey</td>
            <td>smallint</td>
            <td>ID of the column referenced by the foreign key constraint.</td>
        </tr>
        <tr>
            <td>keyno</td>
            <td>smallint</td>
            <td>Position of the column in the foreign key constraint columns, ranging from 1 to the number of foreign key constraint columns.</td>
        </tr>
    </tbody>
</table>
