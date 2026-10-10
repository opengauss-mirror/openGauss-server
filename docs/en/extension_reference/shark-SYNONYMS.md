# SYNONYMS

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-07-30T06:35:01.839Z pushedAt=2026-09-22T07:18:36.129Z -->

Returns information about synonyms.

**Table 1** SYNONYMS

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
            <td>Synonym name</td>
        </tr>
        <tr>
            <td>object_id</td>
            <td>oid</td>
            <td>ID of the synonym</td>
        </tr>
        <tr>
            <td>principal_id</td>
            <td>oid</td>
            <td>OID of the object owner. If the owner of the synonym is the same as the owner of the schema to which the synonym belongs, NULL is returned; otherwise, the owner of the synonym is returned.</td>
        </tr>
        <tr>
            <td>schema_id</td>
            <td>oid</td>
            <td>ID of the schema to which the synonym belongs</td>
        </tr>
        <tr>
            <td>parent_object_id</td>
            <td>oid</td>
            <td>ID of the parent object to which the synonym belongs. The value is always 0.</td>
        </tr>
        <tr>
            <td>type</td>
            <td>char(2)</td>
            <td>Object type. The value is always SN = Synonym.</td>
        </tr>
        <tr>
            <td>type_desc</td>
            <td>nvarchar(60)</td>
            <td>Object type description, the value is always Synonym</td>
        </tr>
        <tr>
            <td>create_date</td>
            <td>timestamp</td>
            <td>Object creation date, the value is always NULL</td>
        </tr>
        <tr>
            <td>modify_date</td>
            <td>timestamp</td>
            <td>Object modification date, the value is always NULL</td>
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
            <td>base_object_name</td>
            <td>nvarchar(1035)</td>
            <td>The fully qualified reference name of the object corresponding to the synonym, in the format schema_name.object_name</td>
        </tr>
    </tbody>
</table>