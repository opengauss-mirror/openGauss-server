# SYSDATABASES

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-09-13T02:04:12.563Z pushedAt=2026-09-29T02:34:56.663Z -->

Returns information about databases.

**Table 1** SYSDATABASES

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
            <td>name</td>
            <td>name</td>
            <td>Database name.</td>
        </tr>
        <tr>
            <td>dbid</td>
            <td>smallint</td>
            <td>Database ID.</td>
        </tr>
        <tr>
            <td>sid</td>
            <td>varbinary(85)</td>
            <td>System ID of the database creator.</td>
        </tr>
        <tr>
            <td>mode</td>
            <td>smallint</td>
            <td>Returns 0.</td>
        </tr>
        <tr>
            <td>status</td>
            <td>int</td>
            <td>Returns 0.</td>
        </tr>
        <tr>
            <td>status2</td>
            <td>int</td>
            <td>Returns 0.</td>
        </tr>
        <tr>
            <td>crdate</td>
            <td>timestamp</td>
            <td>Returns 1900-01-01 00:00:00.000.</td>
        </tr>
        <tr>
            <td>reserved</td>
            <td>timestamp</td>
            <td>Reserved for future use. Returns 1900-01-01 00:00:00.000.</td>
        </tr>
        <tr>
            <td>category</td>
            <td>int</td>
            <td>Returns 0.</td>
        </tr>
        <tr>
            <td>cmptlevel</td>
            <td>tinyint</td>
            <td>Returns 0.</td>
        </tr>
        <tr>
            <td>filename</td>
            <td>nvarchar(260)</td>
            <td>Returns NULL.</td>
        </tr>
        <tr>
            <td>version</td>
            <td>smallint</td>
            <td>Returns NULL.</td>
        </tr>
    </tbody>
</table>
