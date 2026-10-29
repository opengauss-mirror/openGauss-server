# INDEXS

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-07-30T06:33:04.434Z pushedAt=2026-09-22T03:36:28.300Z -->

Returns all indexes.

**Table 1** INDEXS

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
            <td>ID of the associated object</td>
        </tr>
        <tr>
            <td>name</td>
            <td>name</td>
            <td>Index Name</td>
        </tr>
        <tr>
            <td>index_id</td>
            <td>oid</td>
            <td>Index ID</td>
        </tr>
        <tr>
            <td>type</td>
            <td>tinyint</td>
            <td>Currently supported:<br>2 = Nonclustered rowstore (B-tree)<br>6 = Nonclustered columnstore index<br>7 = Nonclustered hash index.</td>
        </tr>
        <tr>
            <td>type_desc</td>
            <td>nvarchar(60)</td>
            <td>Description of type. Currently supported: <br>NONCLUSTERED<br>NONCLUSTERED COLUMNSTORE<br>NONCLUSTERED HASH</td>
        </tr>
        <tr>
            <td>is_unique</td>
            <td>bit</td>
            <td>Whether it is a unique index</td>
        </tr>
        <tr>
            <td>data_space_id</td>
            <td>oid</td>
            <td>Data space of the index. The tablespace corresponding to the index.</td>
        </tr>
        <tr>
            <td>ignore_dup_key</td>
            <td>bit</td>
            <td>Returns 0.</td>
        </tr>
        <tr>
            <td>is_primary_key</td>
            <td>bit</td>
            <td>Whether it is a primary key</td>
        </tr>
        <tr>
            <td>is_unique_constraint</td>
            <td>bit</td>
            <td>Whether it is a unique constraint</td>
        </tr>
        <tr>
            <td>fill_factor</td>
            <td>tinyint</td>
            <td>Fill factor</td>
        </tr>
        <tr>
            <td>is_padded</td>
            <td>bit</td>
            <td>Returns 0</td>
        </tr>
        <tr>
            <td>is_disabled</td>
            <td>bit</td>
            <td>Whether the index is disabled</td>
        </tr>
        <tr>
            <td>is_hypothetical</td>
            <td>bit</td>
            <td>Returns 0</td>
        </tr>
        <tr>
            <td>allow_row_locks</td>
            <td>bit</td>
            <td>Returns 1</td>
        </tr>
        <tr>
            <td>allow_page_locks</td>
            <td>bit</td>
            <td>Returns 1</td>
        </tr>
        <tr>
            <td>has_filter</td>
            <td>bit</td>
            <td>Whether it is a partial index</td>
        </tr>
        <tr>
            <td>filter_definition</td>
            <td>nvarchar</td>
            <td>Partial index definition</td>
        </tr>
        <tr>
            <td>compression_delay</td>
            <td>int</td>
            <td>Returns 0</td>
        </tr>
        <tr>
            <td>suppress_dup_key_messages</td>
            <td>bit</td>
            <td>Returns 0</td>
        </tr>
    </tbody>
</table>