# DATABASE_PRINCIPALS

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-07-30T06:31:17.471Z pushedAt=2026-09-21T09:54:09.112Z -->

Returns information about database roles.

**Table 1** DATABASE_PRINCIPALS

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
            <td>Role name</td>
        </tr>
        <tr>
            <td>principal_id</td>
            <td>oid</td>
            <td>ID of the role, whose value is the OID corresponding to the role</td>
        </tr>
        <tr>
            <td>type</td>
            <td>char(1)</td>
            <td>Role type. The value is always R.</td>
        </tr>
        <tr>
            <td>type_desc</td>
            <td>nvarchar(60)</td>
            <td>Description of the role type. The value is always DATABASE_ROLE.</td>
        </tr>
        <tr>
            <td>default_schema_name</td>
            <td>name</td>
            <td>Default schema name of the role. The value is always NULL.</td>
        </tr>
        <tr>
            <td>create_date</td>
            <td>timestamp</td>
            <td>Creation time of the role. The value is always NULL.</td>
        </tr>
        <tr>
            <td>modify_date</td>
            <td>timestamp</td>
            <td>Modification time of the role, the value is always NULL</td>
        </tr>
        <tr>
            <td>owning_principal_id</td>
            <td>int</td>
            <td>ID of the role that owns this role, the value is always 10, representing the initial role</td>
        </tr>
        <tr>
            <td>sid</td>
            <td>varbinary(85)</td>
            <td>SID of the role, whose value is the OID corresponding to the role converted to the corresponding type</td>
        </tr>
        <tr>
            <td>is_fixed_role</td>
            <td>bit</td>
            <td>Whether it is a system role. 1 indicates a system role (including initial roles), and 0 indicates a non-system role.</td>
        </tr>
        <tr>
            <td>authentication_type</td>
            <td>int</td>
            <td>Authentication type of the role. The value is always 0.</td>
        </tr>
        <tr>
            <td>authentication_type_desc</td>
            <td>nvarchar(60)</td>
            <td>Authentication type description of the role. The value is always NONE.</td>
        </tr>
        <tr>
            <td>default_language_name</td>
            <td>name</td>
            <td>Default language of the role. The value is always NULL.</td>
        </tr>
        <tr>
            <td>default_language_lcid</td>
            <td>int</td>
            <td>Default LCID (Locale Identifier) of the role. The value is always NULL.</td>
        </tr>
        <tr>
            <td>allow_encrypted_value_modifications</td>
            <td>bit</td>
            <td>Whether direct modification of the ciphertext of an encrypted column is allowed. 1 indicates allowed, 0 indicates not allowed. The value is always 0.</td>
        </tr>
    </tbody>
</table>