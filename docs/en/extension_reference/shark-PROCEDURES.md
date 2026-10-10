# PROCEDURES

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-07-30T06:33:59.869Z pushedAt=2026-09-22T06:40:07.763Z -->

All stored procedures

**Table 1** PROCEDURES

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
            <td>Object Name</td>
        </tr>
        <tr>
            <td>object_id</td>
            <td>oid</td>
            <td>Object ID</td>
        </tr>
        <tr>
            <td>principal_id</td>
            <td>oid</td>
            <td>OID of the object owner.<br>If the current owner is the same as the schema owner, NULL is returned.<br>For the following types, NULL is also returned directly:<br>C<br>D<br>F<br>PK<br>TR<br>UQ</td>
        </tr>
        <tr>
            <td>schema_id</td>
            <td>oid</td>
            <td>ID of the schema to which the object belongs</td>
        </tr>
        <tr>
            <td>parent_object_id</td>
            <td>oid</td>
            <td>Returns the parent object ID to which the object belongs</td>
        </tr>
        <tr>
            <td>type</td>
            <td>char(2)</td>
            <td>Returns P</td>
        </tr>
        <tr>
            <td>type_desc</td>
            <td>nvarchar(60)</td>
            <td>Returns SQL_STORED_PROCEDURE</td>
        </tr>
        <tr>
            <td>create_date</td>
            <td>timestamp</td>
            <td>Object creation date</td>
        </tr>
        <tr>
            <td>modify_date</td>
            <td>timestamp</td>
            <td>Object modification date</td>
        </tr>
        <tr>
            <td>is_ms_shipped</td>
            <td>bit</td>
            <td>Whether it is a system internal object.<br>Returns 1 for system tables, views, etc.<br>Returns 0 for user tables, etc.</td>
        </tr>
        <tr>
            <td>is_published</td>
            <td>bit</td>
            <td>Whether the object is published</td>
        </tr>
        <tr>
            <td>is_schema_published</td>
            <td>bit</td>
            <td>Whether to publish only the schema</td>
        </tr>
        <tr>
            <td>is_auto_executed</td>
            <td>bit</td>
            <td>Returns 0</td>
        </tr>
        <tr>
            <td>is_execution_replicated</td>
            <td>bit</td>
            <td>Returns 0</td>
        </tr>
        <tr>
            <td>is_repl_serializable_only</td>
            <td>bit</td>
            <td>Returns 0</td>
        </tr>
        <tr>
            <td>skips_repl_constraints</td>
            <td>bit</td>
            <td>Returns 0</td>
        </tr>
    </tbody>
</table>