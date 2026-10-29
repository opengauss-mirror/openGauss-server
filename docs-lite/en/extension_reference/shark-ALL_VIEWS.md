# ALL_VIEWS

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-07-30T06:30:12.539Z pushedAt=2026-09-21T08:41:07.516Z -->

Returns information about all system views and user views.

**Table 1** ALL_VIEWS

<table aria-label="Table 1" class="table table-sm margin-top-none">
    <thead>
        <tr>
            <th>Column name</th>
            <th>Type</th>
            <th>Description</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>name</td>
            <td>name</td>
            <td>View name</td>
        </tr>
        <tr>
            <td>object_id</td>
            <td>oid</td>
            <td>View ID</td>
        </tr>
        <tr>
            <td>principal_id</td>
            <td>oid</td>
            <td>OID of the object owner. If the view owner is the same as the owner of the schema to which the view belongs, NULL is returned. Otherwise, the view owner is returned.</td>
        </tr>
        <tr>
            <td>schema_id</td>
            <td>oid</td>
            <td>ID of the schema to which it belongs</td>
        </tr>
        <tr>
            <td>parent_object_id</td>
            <td>oid</td>
            <td>Parent object ID of the object. The value is always 0.</td>
        </tr>
        <tr>
            <td>type</td>
            <td>char(2)</td>
            <td>Object type. The value is always V = VIEW.</td>
        </tr>
        <tr>
            <td>type_desc</td>
            <td>nvarchar(60)</td>
            <td>Object type description. The value is always VIEW.</td>
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
            <td>is_replicated</td>
            <td>bit</td>
            <td>Whether the view is replicated. The value is always 0.</td>
        </tr>
        <tr>
            <td>has_replication_filter</td>
            <td>bit</td>
            <td>Whether the view has a replication filter. The value is always 0.</td>
        </tr>
        <tr>
            <td>has_opaque_metadata</td>
            <td>bit</td>
            <td>Whether the view specifies the VIEW_METADATA option. The value is always 0.</td>
        </tr>
        <tr>
            <td>has_unchecked_assembly_data</td>
            <td>bit</td>
            <td>Whether the view has unchecked assembly data. The value is always 0.</td>
        </tr>
        <tr>
            <td>with_check_option</td>
            <td>bit</td>
            <td>Whether the view specifies the WITH CHECK OPTION option. 1 indicates that the option is included, and 0 indicates that it is not included.</td>
        </tr>
        <tr>
            <td>is_date_correlation_view</td>
            <td>bit</td>
            <td>Whether the view is automatically created by the system to store related information between datetime columns. The value is always 0.</td>
        </tr>
        <tr>
            <td>is_tracked_by_cdc</td>
            <td>bit</td>
            <td>Whether a base table on which the view depends is being tracked by CDC (change data capture). The value is always 0.</td>
        </tr>
    </tbody>
</table>