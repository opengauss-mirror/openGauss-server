# SYSTEM_OBJECTS

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-09-13T02:18:54.804Z pushedAt=2026-09-29T02:34:56.681Z -->

Returns information about system objects. In D database, information_schema, pg_catalog, sys, and information_schema_tsql are the four system schemas. This view returns objects in the system schemas and the views in the dbe_perf schema.

**Table 1** SYSTEM_OBJECTS

<table aria-label="表1" class="table table-sm margin-top-none">
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
            <td>Object name.</td>
        </tr>
        <tr>
            <td>object_id</td>
            <td>oid</td>
            <td>Object ID.</td>
        </tr>
        <tr>
            <td>principal_id</td>
            <td>oid</td>
            <td>OID of the object owner.<br>If the current owner is the same as the schema owner, NULL is returned.<br>If the object type is one of the following, NULL is also returned: <br>C<br>D<br>F<br>PK<br>TR<br>UQ</td>
        </tr>
        <tr>
            <td>schema_id</td>
            <td>oid</td>
            <td>ID of the schema to which the object belongs.</td>
        </tr>
        <tr>
            <td>parent_object_id</td>
            <td>oid</td>
            <td>Object ID of the parent object to which the object belongs.</td>
        </tr>
        <tr>
            <td>type</td>
            <td>char(2)</td>
            <td>Object type. Supported types:<br>
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
            <td>Object type description. Supported types:<br>
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
            <td>Object creation date.</td>
        </tr>
        <tr>
            <td>modify_date</td>
            <td>timestamp</td>
            <td>Date when the object was modified.</td>
        </tr>
        <tr>
            <td>is_ms_shipped</td>
            <td>bit</td>
            <td>Whether the object is an internal system object.</td>
        </tr>
        <tr>
            <td>is_published</td>
            <td>bit</td>
            <td>Whether the object is published.</td>
        </tr>
        <tr>
            <td>is_schema_published</td>
            <td>bit</td>
            <td>Whether only the schema is published.</td>
        </tr>
    </tbody>
</table>
