# COLUMNS

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-07-30T06:30:53.846Z pushedAt=2026-09-21T09:45:07.433Z -->

All columns of user-defined objects.

**Table 1** COLUMNS

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
            <td>ID of the owning object</td>
        </tr>
        <tr>
            <td>name</td>
            <td>name</td>
            <td>Column Name</td>
        </tr>
        <tr>
            <td>column_id</td>
            <td>int</td>
            <td>ID of the column</td>
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
            <td>If the type is based on numeric, the corresponding precision is returned.<br>Otherwise, 0 is returned.</td>
        </tr>
        <tr>
            <td>scale</td>
            <td>smallint</td>
            <td>If the type is based on numeric, the corresponding scale is returned.<br>Otherwise, 0 is returned.</td>
        </tr>
        <tr>
            <td>collation_name</td>
            <td>name</td>
            <td>Character collation name of the column</td>
        </tr>
        <tr>
            <td>is_nullable</td>
            <td>bit</td>
            <td>Whether the column allows null values.</td>
        </tr>
        <tr>
            <td>is_ansi_padded</td>
            <td>bit</td>
            <td>Returns 0.</td>
        </tr>
        <tr>
            <td>is_rowguidcol</td>
            <td>bit</td>
            <td>Returns 0.</td>
        </tr>
        <tr>
            <td>is_identity</td>
            <td>bit</td>
            <td>Whether it is an identity column.<br>1 = identity column<br>0 = non-identity column</td>
        </tr>
        <tr>
            <td>is_computed</td>
            <td>bit</td>
            <td>Whether it is a computed column<br>1 = Computed column<br>0 = Non-computed column</td>
        </tr>
        <tr>
            <td>is_filestream</td>
            <td>bit</td>
            <td>Returns 0.</td>
        </tr>
        <tr>
            <td>is_replicated</td>
            <td>bit</td>
            <td>1: The column is published. If the table corresponding to the column is published, all columns of the table are published.</td>
        </tr>
        <tr>
            <td>is_non_sql_subscribed</td>
            <td>bit</td>
            <td>Returns 0.</td>
        </tr>
        <tr>
            <td>is_merge_published</td>
            <td>bit</td>
            <td>Returns 0.</td>
        </tr>
        <tr>
            <td>is_dts_replicated</td>
            <td>bit</td>
            <td>Returns 0</td>
        </tr>
        <tr>
            <td>is_xml_document</td>
            <td>bit</td>
            <td>Returns 0.</td>
        </tr>
        <tr>
            <td>xml_collection_id</td>
            <td>oid</td>
            <td>Returns 0.</td>
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
            <td>Returns 0.</td>
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
        <tr>
            <td>encryption_type</td>
            <td>int</td>
            <td>Encryption type of the column<br>1 = Deterministic encryption<br>2 = Randomized encryption</td>
        </tr>
        <tr>
            <td>encryption_type_desc</td>
            <td>nvarchar(64)</td>
            <td>Description of the column's encryption type<br>Deterministic encryption<br>Randomized encryption</td>
        </tr>
        <tr>
            <td>encryption_algorithm_name</td>
            <td>name</td>
            <td>Algorithm for column encryption</td>
        </tr>
        <tr>
            <td>column_encryption_key_id</td>
            <td>oid</td>
            <td>ID of the key for the encrypted column</td>
        </tr>
        <tr>
            <td>column_encryption_key_database_name</td>
            <td>name</td>
            <td>Returns NULL.</td>
        </tr>
        <tr>
            <td>is_hidden</td>
            <td>bit</td>
            <td>Returns 0.</td>
        </tr>
        <tr>
            <td>is_masked</td>
            <td>bit</td>
            <td>Returns 0</td>
        </tr>
        <tr>
            <td>graph_type</td>
            <td>int</td>
            <td>Returns NULL.</td>
        </tr>
        <tr>
            <td>graph_type_desc</td>
            <td>nvarchar(60)</td>
            <td>Returns NULL.</td>
        </tr>
    </tbody>
</table>