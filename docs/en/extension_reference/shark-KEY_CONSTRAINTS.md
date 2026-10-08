# KEY_CONSTRAINTS

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-07-30T06:33:36.242Z pushedAt=2026-09-22T03:57:41.362Z -->

Returns information about primary keys and unique constraints.

**Table 1** KEY_CONSTRAINTS

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
            <td>Name of the primary key or unique constraint</td>
        </tr>
        <tr>
            <td>object_id</td>
            <td>oid</td>
            <td>ID of the primary key or unique constraint</td>
        </tr>
        <tr>
            <td>principal_id</td>
            <td>oid</td>
            <td>OID of the object owner. The value is always NULL.</td>
        </tr>
        <tr>
            <td>schema_id</td>
            <td>oid</td>
            <td>ID of the schema it belongs to</td>
        </tr>
        <tr>
            <td>parent_object_id</td>
            <td>oid</td>
            <td>OID of the table where the primary key or unique constraint resides</td>
        </tr>
        <tr>
            <td>type</td>
            <td>char(2)</td>
            <td>Object type. Possible values are as follows:<br>
            PK = PRIMARY_KEY_CONSTRAINT<br>
            UQ = UNIQUE_CONSTRAINT
            </td>
        </tr>
        <tr>
            <td>type_desc</td>
            <td>nvarchar(60)</td>
            <td>Object type description. Possible values:<br>
            PRIMARY_KEY_CONSTRAIN<br>
            UNIQUE_CONSTRAINT
            </td>
        </tr>
        <tr>
            <td>create_date</td>
            <td>timestamp</td>
            <td>Object creation date. The value is always NULL.</td>
        </tr>
        <tr>
            <td>modify_date</td>
            <td>timestamp</td>
            <td>Object modification date. The value is always NULL.</td>
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
            <td>unique_index_id</td>
            <td>oid</td>
            <td>Index ID corresponding to the constraint.</td>
        </tr>
        <tr>
            <td>is_system_named</td>
            <td>bit</td>
            <td>Whether the constraint name is system-generated. 1 indicates system-generated, 0 indicates user-defined.<br>
            Primary key constraint names match the format `table_name_pkey`.<br>
            Unique constraint names match the format `table_name_col1_col2_..._key`.<br>
            If the name matches the above formats, it is system-generated; otherwise, it is user-defined.<br>
            When a user-defined name happens to be identical to a system-generated name, it is treated as a system-generated name.
            </td>
        </tr>
        <tr>
            <td>is_enforced</td>
            <td>bit</td>
            <td>Whether the primary key or unique constraint is enforced. 1 indicates enforced, and 0 indicates not enforced.</td>
        </tr>
    </tbody>
</table>