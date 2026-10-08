# TABLES

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-07-30T06:33:05.006Z pushedAt=2026-09-22T03:52:57.905Z -->

The TABLES view returns information about tables or views in the database.

**Table 1** TABLES

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
            <td>TABLE_CATALOG</td>
            <td>nvarchar(128)</td>
            <td>Table qualifier</td>
        </tr>
        <tr>
            <td>TABLE_SCHEMA</td>
            <td>nvarchar(128)</td>
            <td>Name of the schema that contains the table</td>
        </tr>
        <tr>
            <td>TABLE_NAME</td>
            <td>name</td>
            <td>Table or view name</td>
        </tr>
        <tr>
            <td>TABLE_TYPE</td>
            <td>varchar(10)</td>
            <td>Type of the table. Can be VIEW or BASE TABLE.</td>
        </tr>
    </tbody>
</table>