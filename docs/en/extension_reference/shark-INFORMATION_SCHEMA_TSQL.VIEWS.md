# VIEWS

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-07-30T06:33:20.753Z pushedAt=2026-09-22T03:55:55.835Z -->

The VIEWS view returns view information in the database.

**Table 1** VIEWS

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
            <td>Name of the schema containing the table</td>
        </tr>
        <tr>
            <td>TABLE_NAME</td>
            <td>nvarchar(128)</td>
            <td>View name</td>
        </tr>
        <tr>
            <td>VIEW_DEFINITION</td>
            <td>nvarchar(4000)</td>
            <td>If the definition length exceeds nvarchar(4000), this column is truncated at 4000. Otherwise, this column contains the view definition text.</td>
        </tr>
         <tr>
            <td>CHECK_OPTION</td>
            <td>varchar(7)</td>
            <td>The type of WITH CHECK OPTION. If the original view was created using WITH CHECK OPTION, the value is CASCADE. Otherwise, NONE is returned.</td>
        </tr>
         <tr>
            <td>IS_UPDATABLE</td>
            <td>varchar(2)</td>
            <td>Specifies whether the view is updatable. 1=TRUE, 0=FALSE</td>
        </tr>
    </tbody>
</table>