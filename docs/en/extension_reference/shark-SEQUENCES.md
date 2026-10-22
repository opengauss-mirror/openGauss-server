# SEQUENCES

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-07-30T06:34:32.646Z pushedAt=2026-09-22T07:15:21.609Z -->

Returns sequence-related information.

**Table 1** SEQUENCES

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
            <td>Sequence name</td>
        </tr>
        <tr>
            <td>object_id</td>
            <td>oid</td>
            <td>Sequence ID</td>
        </tr>
        <tr>
            <td>principal_id</td>
            <td>oid</td>
            <td>OID of the object owner. If the sequence owner is the same as the owner of the schema to which the sequence belongs, NULL is returned; otherwise, the sequence owner is returned.</td>
        </tr>
        <tr>
            <td>schema_id</td>
            <td>oid</td>
            <td>ID of the schema to which the sequence belongs</td>
        </tr>
        <tr>
            <td>parent_object_id</td>
            <td>oid</td>
            <td>OID of the parent object to which the sequence belongs, always has the value 0</td>
        </tr>
        <tr>
            <td>type</td>
            <td>char(2)</td>
            <td>Object type, always has the value SO = SEQUENCE_OBJECT</td>
        </tr>
        <tr>
            <td>type_desc</td>
            <td>nvarchar(60)</td>
            <td>Object type description, always has the value SEQUENCE_OBJECT</td>
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
            <td>Whether it is a system internal object, always has the value 0</td>
        </tr>
        <tr>
            <td>is_published</td>
            <td>bit</td>
            <td>Whether the object is published, always has the value 0</td>
        </tr>
        <tr>
            <td>is_schema_published</td>
            <td>bit</td>
            <td>Whether only the schema is published; always has the value 0</td>
        </tr>
        <tr>
            <td>start_value</td>
            <td>sql_variant</td>
            <td>Start value of the sequence</td>
        </tr>
        <tr>
            <td>increment</td>
            <td>sql_variant</td>
            <td>Step size of the sequence</td>
        </tr>
        <tr>
            <td>minimum_value</td>
            <td>sql_variant</td>
            <td>Minimum value of the sequence</td>
        </tr>
        <tr>
            <td>maximum_value</td>
            <td>sql_variant</td>
            <td>Maximum value of the sequence</td>
        </tr>
        <tr>
            <td>is_cycling</td>
            <td>bit</td>
            <td>Whether the sequence is cyclic. 1 indicates a cyclic sequence (CYCLE specified during sequence creation), and 0 indicates a non-cyclic sequence (NO CYCLE specified during sequence creation).</td>
        </tr>
        <tr>
            <td>is_cached</td>
            <td>bit</td>
            <td>Whether CACHE is specified during sequence creation. This column always has the value 1.</td>
        </tr>
        <tr>
            <td>cache_size</td>
            <td>sql_variant</td>
            <td>Cache size specified during sequence creation</td>
        </tr>
        <tr>
            <td>system_type_id</td>
            <td>tinyint</td>
            <td>System type ID. For a regular sequence, it has the value 20 (representing the int8 type); for a LARGE sequence, it has the value 34 (representing the int16 type).</td>
        </tr>
        <tr>
            <td>user_type_id</td>
            <td>int</td>
            <td>User type ID. For a regular sequence, it has the value 20 (representing the int8 type); for a LARGE sequence, it has the value 34 (representing the int16 type).</td>
        </tr>
        <tr>
            <td>precision</td>
            <td>tinyint</td>
            <td>Maximum precision of the sequence type. For a normal sequence, it has the value 19 (representing the int8 type); for a LARGE sequence, it has the value 39 (representing the int16 type).</td>
        </tr>
        <tr>
            <td>scale</td>
            <td>tinyint</td>
            <td>Maximum scale of the sequence type, which always has the value 0.</td>
        </tr>
        <tr>
            <td>current_value</td>
            <td>sql_variant</td>
            <td>Current value of the sequence. If the sequence has not been used, returns start_value.</td>
        </tr>
        <tr>
            <td>is_exhausted</td>
            <td>bit</td>
            <td>Whether the sequence is exhausted. 1 indicates that the sequence is exhausted and cannot generate new values; 0 indicates that the sequence is not exhausted and can generate new values.</td>
        </tr>
        <tr>
            <td>last_used_value</td>
            <td>sql_variant</td>
            <td>The last used value of the sequence. If the sequence has not been used, NULL is returned.</td>
        </tr>
    </tbody>
</table>