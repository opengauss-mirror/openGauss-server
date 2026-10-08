# ALL_COLUMNS

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-07-30T06:29:55.808Z pushedAt=2026-09-21T08:35:51.365Z -->

A collection of all columns of user-defined objects and system objects.

**Table 1** ALL_COLUMNS

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
            <td>object_id</td>
            <td>oid</td>
            <td>ID of the object to which it belongs</td>
        </tr>
        <tr>
            <td>name</td>
            <td>name</td>
            <td>Column name</td>
        </tr>
        <tr>
            <td>column_id</td>
            <td>int</td>
            <td>Column ID</td>
        </tr>
        <tr>
            <td>system_type_id</td>
            <td>oid</td>
            <td>Data type ID of the column</td>
        </tr>
        <tr>
            <td>user_type_id</td>
            <td>oid</td>
            <td>Data type ID of the column</td>
        </tr>
        <tr>
            <td>max_length</td>
            <td>smallint</td>
            <td>Maximum byte length of the column</td>
        </tr>
        <tr>
            <td>precision</td>
            <td>smallint</td>
            <td>If the type is based on numeric, returns the corresponding precision.<br>Otherwise, returns 0.</td>
        </tr>
        <tr>
            <td>scale</td>
            <td>smallint</td>
            <td>If the type is based on numeric, returns the corresponding scale.<br>Otherwise, returns 0.</td>
        </tr>
        <tr>
            <td>collation_name</td>
            <td>name</td>
            <td>Character collation name of the column</td>
        </tr>
        <tr>
            <td>is_nullable</td>
            <td>bit</td>
            <td>Whether the column allows null values</td>
        </tr>
        <tr>
            <td>is_ansi_padded</td>
            <td>bit</td>
            <td>Returns 0</td>
        </tr>
        <tr>
            <td>is_rowguidcol</td>
            <td>bit</td>
            <td>Returns 0</td>
        </tr>
        <tr>
            <td>is_identity</td>
            <td>bit</td>
            <td>Returns 0</td>
        </tr>
        <tr>
            <td>is_computed</td>
            <td>bit</td>
            <td>1 indicates a computed column</td>
        </tr>
        <tr>
            <td>is_filestream</td>
            <td>bit</td>
            <td>Returns 0</td>
        </tr>
        <tr>
            <td>is_replicated</td>
            <td>bit</td>
            <td>1 if the column is published. If the table corresponding to the column is published, all columns of that table are published.</td>
        </tr>
        <tr>
            <td>is_non_sql_subscribed</td>
            <td>bit</td>
            <td>Returns 0</td>
        </tr>
        <tr>
            <td>is_merge_published</td>
            <td>bit</td>
            <td>Returns 0</td>
        </tr>
        <tr>
            <td>is_dts_replicated</td>
            <td>bit</td>
            <td>Returns 0</td>
        </tr>
        <tr>
            <td>is_xml_document</td>
            <td>bit</td>
            <td>Returns 0</td>
        </tr>
        <tr>
            <td>xml_collection_id</td>
            <td>oid</td>
            <td>Returns 0</td>
        </tr>
        <tr>
            <td>default_object_id</td>
            <td>oid</td>
            <td>ID of the default value of the column</td>
        </tr>
        <tr>
            <td>rule_object_id</td>
            <td>int</td>
            <td>Returns 0</td>
        </tr>
        <tr>
            <td>is_sparse</td>
            <td>bit</td>
            <td>Returns 0</td>
        </tr>
        <tr>
            <td>is_column_set</td>
            <td>bit</td>
            <td>Returns 0</td>
        </tr>
        <tr>
            <td>generated_always_type</td>
            <td>tinyint</td>
            <td>Returns 0</td>
        </tr>
        <tr>
            <td>generated_always_type_desc</td>
            <td>nvarchar(60)</td>
            <td>Returns NOT_APPLICABLE</td>
        </tr>
    </tbody>
</table>