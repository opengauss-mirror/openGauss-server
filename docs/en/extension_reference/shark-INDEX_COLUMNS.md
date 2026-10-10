# INDEX_COLUMNS

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-07-30T06:32:34.839Z pushedAt=2026-09-22T03:31:44.935Z -->

Returns index-related information.

**Table 1** INDEX_COLUMNS

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
            <td>ID of the object on which the index is defined</td>
        </tr>
        <tr>
            <td>index_id</td>
            <td>oid</td>
            <td>ID of the index</td>
        </tr>
        <tr>
            <td>index_column_id</td>
            <td>int</td>
            <td>ID of the index column, ranging from 1 to the number of index columns</td>
        </tr>
        <tr>
            <td>column_id</td>
            <td>int</td>
            <td>Ordinal position of the index column within the object it is defined on</td>
        </tr>
        <tr>
            <td>key_ordinal</td>
            <td>tinyint</td>
            <td>Ordinal position of the index column participating in the index scan, ranging from 1 to the number of columns participating in the index scan</td>
        </tr>
        <tr>
            <td>partition_ordinal</td>
            <td>tinyint</td>
            <td>Ordinal position within the partition column set. For a regular table or a non-one-dimensional partition column of a partitioned table, the value is 0.<br>
            For a one-dimensional partition key of a partitioned table, the value is the ordinal position within the partition key set.
            </td>
        </tr>
        <tr>
            <td>is_descending_key</td>
            <td>bit</td>
            <td>Whether the index key column uses descending sort order. 1 indicates descending order, and 0 indicates ascending order.</td>
        </tr>
        <tr>
            <td>is_included_column</td>
            <td>bit</td>
            <td>Whether the column is a non-key column added by the INCLUDE clause. 1 indicates a non-key column (not participating in index queries), and 0 indicates a column that participates in index queries.</td>
        </tr>
    </tbody>
</table>