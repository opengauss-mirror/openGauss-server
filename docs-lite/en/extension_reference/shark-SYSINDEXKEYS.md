# SYSINDEXKEYS

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-09-13T02:08:31.361Z pushedAt=2026-09-29T02:34:56.672Z -->

Contains information about columns in the indexes of the database.

**Table 1** SYSINDEXKEYS view fields

<table aria-label="表1" class="table table-sm margin-top-none">
    <thead>
        <tr>
            <th>Column Name</th>
            <th>Data Type</th>
            <th>Description</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>id</td>
            <td>oid</td>
            <td>ID of the table.</td>
        </tr>
        <tr>
            <td>indid</td>
            <td>oid</td>
            <td>ID of the index.</td>
        </tr>
        <tr>
            <td>colid</td>
            <td>smallint</td>
            <td>ID of the column.</td>
        </tr>
        <tr>
            <td>keyno</td>
            <td>smallint</td>
            <td>Position of the column in the index.</td>
        </tr>
    </tbody>
</table>
