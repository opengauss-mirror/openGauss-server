# ALL_OBJECTS

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-07-30T06:29:56.342Z pushedAt=2026-09-21T08:36:14.149Z -->

A collection of user-defined objects and system objects within all schemas.

**Table 1** ALL_OBJECTS

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
            <td>Object name</td>
        </tr>
        <tr>
            <td>object_id</td>
            <td>oid</td>
            <td>Object ID</td>
        </tr>
        <tr>
            <td>principal_id</td>
            <td>oid</td>
            <td>OID of the object owner.<br>If the current owner and the schema are the same owner, NULL is returned.<br>For the following types, NULL is also returned directly: <br>C<br>D<br>F<br>PK<br>TR<br>UQ</td>
        </tr>
        <tr>
            <td>schema_id</td>
            <td>oid</td>
            <td>ID of the owning schema</td>
        </tr>
        <tr>
            <td>parent_object_id</td>
            <td>oid</td>
            <td>Returns the ID of the parent object to which the object belongs</td>
        </tr>
        <tr>
            <td>type</td>
            <td>char(2)</td>
            <td>            Object type. Currently supported types:<br>
            AF = AGGREGATE_FUNCTION<br>
            C = CHECK_CONSTRAINT<br>
            D = DEFAULT<br>
            F = FOREIGN_KEY_CONSTRAINT<br>
            FN = SQL_SCALAR_FUNCTION<br>
            P = SQL_STORED_PROCEDURE<br>
            PK = PRIMARY_KEY_CONSTRAINT<br>
            S = SYSTEM_BASE_TABLE<br>
            SN = SYNONYM<br>
            SO = SEQUENCE_OBJECT<br>
            U = USER_TABLE<br>
            V = VIEW<br>
            TR = SQL DML trigger<br>
            UQ = UNIQUE_CONSTRAINT
            </td>
        </tr>
        <tr>
            <td>type_desc</td>
            <td>nvarchar(60)</td>
            <td>            Object type description. Currently supported types:<br>
            AGGREGATE_FUNCTION<br>
            CHECK_CONSTRAINT<br>
            DEFAULT<br>
            FOREIGN_KEY_CONSTRAINT<br>
            SQL_SCALAR_FUNCTION<br>
            SQL_STORED_PROCEDURE<br>
            PRIMARY_KEY_CONSTRAINT<br>
            SYSTEM_BASE_TABLE<br>
            SYNONYM<br>
            SEQUENCE_OBJECT<br>
            USER_TABLE<br>
            VIEW<br>
            SQL DML trigger<br>
            UNIQUE_CONSTRAINT
            </td>
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
            <td>Indicates whether the object is a system internal object.<br>Returns 1 for system tables, views, etc., and 0 for user tables, etc.</td>
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
    </tbody>
</table>