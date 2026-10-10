# FOREIGN_KEYS

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-07-30T06:32:34.932Z pushedAt=2026-09-22T02:13:59.107Z -->

Returns information related to foreign key constraints.

**Table 1** FOREIGN_KEYS

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
            <td>foreign key constraint name</td>
        </tr>
        <tr>
            <td>object_id</td>
            <td>oid</td>
            <td>ID of the foreign key constraint</td>
        </tr>
        <tr>
            <td>principal_id</td>
            <td>oid</td>
            <td>OID of the object owner, always NULL</td>
        </tr>
        <tr>
            <td>schema_id</td>
            <td>oid</td>
            <td>ID of the schema to which it belongs</td>
        </tr>
        <tr>
            <td>parent_object_id</td>
            <td>oid</td>
            <td>OID of the table containing the foreign key constraint</td>
        </tr>
        <tr>
            <td>type</td>
            <td>char(2)</td>
            <td>Object type, always F = FOREIGN_KEY_CONSTRAINT</td>
        </tr>
        <tr>
            <td>type_desc</td>
            <td>nvarchar(60)</td>
            <td>Description of the object type. The value is always FOREIGN_KEY_CONSTRAINT.</td>
        </tr>
        <tr>
            <td>create_date</td>
            <td>timestamp</td>
            <td>Creation date of the object. The value is always NULL.</td>
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
            <td>Whether only the schema is published; the value is always 0</td>
        </tr>
        <tr>
            <td>referenced_object_id</td>
            <td>oid</td>
            <td>OID of the table referenced by the foreign key constraint</td>
        </tr>
        <tr>
            <td>key_index_id</td>
            <td>oid</td>
            <td>Index ID associated with the foreign key constraint</td>
        </tr>
        <tr>
            <td>is_disabled</td>
            <td>bit</td>
            <td>Whether the foreign key constraint is disabled. 1 indicates that the foreign key constraint is disabled, and 0 indicates that it is not disabled.</td>
        </tr>
        <tr>
            <td>is_not_for_replication</td>
            <td>bit</td>
            <td>Whether the foreign key constraint is created with the NOT FOR REPLICATION option. The value is always 0.</td>
        </tr>
        <tr>
            <td>is_not_trusted</td>
            <td>bit</td>
            <td>Whether the constraint is valid. 1 indicates the constraint is invalid, and 0 indicates the constraint is valid.</td>
        </tr>
        <tr>
            <td>delete_referential_action</td>
            <td>tinyint</td>
            <td>The referential action declared for this foreign key constraint when a delete is performed. Values are as follows:<br>
            0 = No action (NO_ACTION)<br>
            1 = Cascade (CASCADE)<br>
            2 = Set NULL (SET_NULL)<br>
            3 = Set default (SET_DEFAULT)
            </td>
        </tr>
        <tr>
            <td>delete_referential_action_desc</td>
            <td>nvarchar(60)</td>
            <td>Description of the referential action declared for this foreign key constraint when a delete is performed. Possible values:<br>
            NO_ACTION<br>
            CASCADE<br>
            SET_NULL<br>
            SET_DEFAULT
            </td>
        </tr>
        <tr>
            <td>update_referential_action</td>
            <td>tinyint</td>
            <td>Referential action declared for this foreign key constraint when an update is performed, with the following values:<br>
            0=No action (NO_ACTION)<br>
            1=Cascade (CASCADE)<br>
            2=Set NULL (SET_NULL)<br>
            3=Set default (SET_DEFAULT)
            </td>
        </tr>
        <tr>
            <td>update_referential_action_desc</td>
            <td>nvarchar(60)</td>
            <td>Description of the referential action declared for this foreign key constraint when an update is performed, with the following values:<br>
            NO_ACTION<br>
            CASCADE<br>
            SET_NULL<br>
            SET_DEFAULT
            </td>
        </tr>
        <tr>
            <td>is_system_named</td>
            <td>bit</td>
            <td>Whether the constraint name is system-generated. 1 indicates system-generated, and 0 indicates user-defined. The value is always 1.</td>
        </tr>
    </tbody>
</table>