# SCHEMAS

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-07-30T06:33:57.017Z pushedAt=2026-09-22T06:45:15.656Z -->

The SCHEMAS view returns namespace information in the database.

**Table 1** SCHEMAS

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
            <td>Name of the schema</td>
        </tr>
        <tr>
            <td>schema_id</td>
            <td>int</td>
            <td>ID of the schema</td>
        </tr>
        <tr>
            <td>principal_id</td>
            <td>int</td>
            <td>ID of the principal database to which this schema belongs</td>
        </tr>
    </tbody>
</table>