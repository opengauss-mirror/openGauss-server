# CHECK_CONSTRAINTS

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-07-30T06:30:22.691Z pushedAt=2026-09-21T09:44:35.353Z -->

Returns information about CHECK constraints.

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
            <td>name</td>
            <td>name</td>
            <td>CHECK constraint name</td>
        </tr>
        <tr>
            <td>object_id</td>
            <td>oid</td>
            <td>ID of the CHECK constraint</td>
        </tr>
        <tr>
            <td>principal_id</td>
            <td>oid</td>
            <td>OID of the object owner, the value is always NULL</td>
        </tr>
        <tr>
            <td>schema_id</td>
            <td>oid</td>
            <td>ID of the schema to which it belongs</td>
        </tr>
        <tr>
            <td>parent_object_id</td>
            <td>oid</td>
            <td>OID of the table where the CHECK constraint resides</td>
        </tr>
        <tr>
            <td>type</td>
            <td>char(2)</td>
            <td>Object type. The value is always C = CHECK_CONSTRAINT</td>
        </tr>
        <tr>
            <td>type_desc</td>
            <td>nvarchar(60)</td>
            <td>Object type description, the value is always CHECK_CONSTRAINT</td>
        </tr>
        <tr>
            <td>create_date</td>
            <td>timestamp</td>
            <td>Object creation date, the value is always NULL</td>
        </tr>
        <tr>
            <td>modify_date</td>
            <td>timestamp</td>
            <td>Modification date of the object. The value is always NULL.</td>
        </tr>
        <tr>
            <td>is_ms_shipped</td>
            <td>bit</td>
            <td>Whether it is a system internal object. The value is always 0.</td>
        </tr>
        <tr>
            <td>is_published</td>
            <td>bit</td>
            <td>Whether the object is published. The value is always 0.</td>
        </tr>
        <tr>
            <td>is_schema_published</td>
            <td>bit</td>
            <td>Whether only the schema is published. The value is always 0.</td>
        </tr>
        <tr>
            <td>is_disabled</td>
            <td>bit</td>
            <td>Whether the check constraint is disabled. 1 indicates that the CHECK constraint is disabled, and 0 indicates that the CHECK constraint is not disabled.</td>
        </tr>
        <tr>
            <td>is_not_for_replication</td>
            <td>bit</td>
            <td>Whether the CHECK constraint was created with the not for replication option. The value is always 0.</td>
        </tr>
        <tr>
            <td>is_not_trusted</td>
            <td>bit</td>
            <td>Whether the constraint is valid. 1 indicates the constraint is invalid, and 0 indicates the constraint is valid.</td>
        </tr>
        <tr>
            <td>parent_column_id</td>
            <td>int</td>
            <td>For a single-column CHECK constraint, returns the number of the column where the constraint resides; for a multi-column CHECK constraint, returns 0.</td>
        </tr>
        <tr>
            <td>definition</td>
            <td>text</td>
            <td>SQL expression of the CHECK constraint</td>
        </tr>
        <tr>
            <td>uses_database_collation</td>
            <td>bit</td>
            <td>Whether the constraint uses the default collation of the database. The value is always 1.</td>
        </tr>
        <tr>
            <td>is_system_named</td>
            <td>bit</td>
            <td>Whether the constraint name is system-generated. 1 indicates system-generated, 0 indicates user-defined. The value is always 1.</td>
        </tr>
    </tbody>
</table>