# FOREIGN_KEY_COLUMNS

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-07-30T06:32:23.497Z pushedAt=2026-09-22T02:10:09.455Z -->

Returns information related to foreign key constraints.

**Table 1** FOREIGN_KEY_COLUMNS

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
            <td>constraint_object_id</td>
            <td>oid</td>
            <td>ID of the foreign key constraint</td>
        </tr>
        <tr>
            <td>constraint_column_id</td>
            <td>int</td>
            <td>ID of the column or column set that makes up the foreign key (1...n, where n is the number of columns)</td>
        </tr>
        <tr>
            <td>parent_object_id</td>
            <td>oid</td>
            <td>OID of the table where the foreign key constraint resides</td>
        </tr>
        <tr>
            <td>parent_column_id</td>
            <td>int</td>
            <td>Number of the column corresponding to the foreign key constraint</td>
        </tr>
        <tr>
            <td>referenced_object_id</td>
            <td>oid</td>
            <td>OID of the table referenced by the foreign key constraint</td>
        </tr>
        <tr>
            <td>referenced_column_id</td>
            <td>int</td>
            <td>Number of the column referenced by the foreign key constraint</td>
        </tr>
    </tbody>
</table>