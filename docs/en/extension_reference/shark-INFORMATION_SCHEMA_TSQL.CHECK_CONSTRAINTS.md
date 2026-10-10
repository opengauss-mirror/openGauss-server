# CHECK_CONSTRAINTS

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-07-30T06:32:56.363Z pushedAt=2026-09-22T03:44:21.049Z -->

The CHECK_CONSTRAINTS view returns check constraint information in the database.

**Table 1** CHECK_CONSTRAINTS

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
            <td>CONSTRAINT_CATALOG</td>
            <td>nvarchar(128)</td>
            <td>Constraint qualifier</td>
        </tr>
        <tr>
            <td>CONSTRAINT_SCHEMA</td>
            <td>nvarchar(128)</td>
            <td>Name of the schema to which the constraint belongs</td>
        </tr>
        <tr>
            <td>CONSTRAINT_NAME</td>
            <td>name</td>
            <td>Constraint name</td>
        </tr>
        <tr>
            <td>CHECK_CLAUSE</td>
            <td>nvarchar(4000)</td>
            <td>Actual text of the Transact-SQL definition statement</td>
        </tr>
    </tbody>
</table>