# SYSINDEXES

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-09-13T02:07:26.009Z pushedAt=2026-09-29T02:34:56.669Z -->

Contains one index and one table in one row in the current database. This view does not support XML indexes, nor partitioned tables and indexes.

**Table 1** SYSINDEXES view fields

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
            <td>id</td>
            <td>oid</td>
            <td>ID of the table that the index matches.</td>
        </tr>
        <tr>
            <td>status</td>
            <td>int</td>
            <td>Returns NULL.</td>
        </tr>
        <tr>
            <td>first</td>
            <td>bytea</td>
            <td>Returns NULL.</td>
        </tr>
        <tr>
            <td>indid</td>
            <td>oid</td>
            <td>Index ID.</td>
        </tr>
        <tr>
            <td>root</td>
            <td>bytea</td>
            <td>Returns NULL.</td>
        </tr>
        <tr>
            <td>minlen</td>
            <td>smallint</td>
            <td>Minimum size of the row. Returns 0.</td>
        </tr>
        <tr>
            <td>keycnt</td>
            <td>smallint</td>
            <td>Number of keys. Returns 0.</td>
        </tr>
        <tr>
            <td>groupid</td>
            <td>smallint</td>
            <td>Returns 0.</td>
        </tr>
        <tr>
            <td>dpages</td>
            <td>int</td>
            <td>Returns 0.</td>
        </tr>
        <tr>
            <td>reserved</td>
            <td>int</td>
            <td>Returns 0.</td>
        </tr>
        <tr>
            <td>used</td>
            <td>int</td>
            <td>Returns 0.</td>
        </tr>
        <tr>
            <td>rowcnt</td>
            <td>bigint</td>
            <td>Returns 0.</td>
        </tr>
        <tr>
            <td>rowmodctr</td>
            <td>int</td>
            <td>Returns 0.</td>
        </tr>
        <tr>
            <td>reserved3</td>
            <td>int</td>
            <td>Returns 0.</td>
        </tr>
        <tr>
            <td>reserved4</td>
            <td>int</td>
            <td>Returns 0.</td>
        </tr>
        <tr>
            <td>xmaxlen</td>
            <td>smallint</td>
            <td>Maximum row size. Returns 0.</td>
        </tr>
        <tr>
            <td>maxirow</td>
            <td>smallint</td>
            <td>Returns NULL.</td>
        </tr>
        <tr>
            <td>OrigFillFactor</td>
            <td>tinyint</td>
            <td>Original fill factor used during index creation. </td>
        </tr>
        <tr>
            <td>StatVersion</td>
            <td>tinyint</td>
            <td>Returns 0.</td>
        </tr>
        <tr>
            <td>reserved2</td>
            <td>int</td>
            <td>Returns 0.</td>
        </tr>
        <tr>
            <td>FirstIAM</td>
            <td>bytea</td>
            <td>Returns NULL.</td>
        </tr>
        <tr>
            <td>impid</td>
            <td>smallint</td>
            <td>Index implementation flag. Returns 0.</td>
        </tr>
        <tr>
            <td>lockflags</td>
            <td>smallint</td>
            <td>Lock granularity considered for the index. Returns 0.</td>
        </tr>
        <tr>
            <td>pgmodctr</td>
            <td>int</td>
            <td>Returns 0.</td>
        </tr>
        <tr>
            <td>keys</td>
            <td>bytea</td>
            <td>List of IDs of the columns that constitute the index key.<br>Returns NULL.<br>To display the index key columns, use [sysindexkeys](shark-SYSINDEXKEYS.md).</td>
        </tr>
        <tr>
            <td>name</td>
            <td>name</td>
            <td>Name of the index.</td>
        </tr>
        <tr>
            <td>statblob</td>
            <td>blob</td>
            <td>Binary large object (BLOB) of statistics.<br>Returns NULL.</td>
        </tr>
        <tr>
            <td>maxlen</td>
            <td>int</td>
            <td>Returns 0.</td>
        </tr>
        <tr>
            <td>rows</td>
            <td>int</td>
            <td>Returns 0.</td>
        </tr>
    </tbody>
</table>
