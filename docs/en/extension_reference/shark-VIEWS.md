# VIEWS

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-09-13T02:35:35.245Z pushedAt=2026-09-29T02:34:56.702Z -->

All schema-scoped user-defined views.

**Table 1** VIEWS

<table aria-label="表1" class="table table-sm margin-top-none">
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
            <td>Object name.</td>
        </tr>
        <tr>
            <td>object_id</td>
            <td>oid</td>
            <td>Object ID.</td>
        </tr>
        <tr>
            <td>principal_id</td>
            <td>oid</td>
            <td>oid of the object owner.<br>Returns NULL if the current owner is the same as the schema owner.</td>
        </tr>
        <tr>
            <td>schema_id</td>
            <td>oid</td>
            <td>ID of the schema to which the view belongs.</td>
        </tr>
        <tr>
            <td>parent_object_id</td>
            <td>oid</td>
            <td>ID of the parent object to which the object belongs.</td>
        </tr>
        <tr>
            <td>type</td>
            <td>char(2)</td>
            <td>Type of the object.</td>
        </tr>
        <tr>
            <td>type_desc</td>
            <td>nvarchar(60)</td>
            <td>Description of the object type.</td>
        </tr>
        <tr>
            <td>create_date</td>
            <td>timestamp</td>
            <td>Date when the object was created.</td>
        </tr>
        <tr>
            <td>modify_date</td>
            <td>timestamp</td>
            <td>Object modification date.</td>
        </tr>
        <tr>
            <td>is_ms_shipped</td>
            <td>bit</td>
            <td>Whether the object is an internal system object.<br>Returns 1 for system tables, views, etc.<br>Returns 0 for user tables, etc.</td>
        </tr>
        <tr>
            <td>is_published</td>
            <td>bit</td>
            <td>Whether the object is published.</td>
        </tr>
        <tr>
            <td>is_schema_published</td>
            <td>bit</td>
            <td>Whether only the schema is published.</td>
        </tr>
        <tr>
            <td>is_replicated</td>
            <td>bit</td>
            <td>Returns 0.</td>
        </tr>
        <tr>
            <td>has_replication_filter</td>
            <td>bit</td>
            <td>Returns 0.</td>
        </tr>
        <tr>
            <td>has_opaque_metadata</td>
            <td>bit</td>
            <td>Returns 0.</td>
        </tr>
        <tr>
            <td>has_unchecked_assembly_data</td>
            <td>bit</td>
            <td>Returns 0.</td>
        </tr>
        <tr>
            <td>with_check_option</td>
            <td>bit</td>
            <td>Returns 1 if the view has the WITH CHECK OPTION.</td>
        </tr>
        <tr>
            <td>is_date_correlation_view</td>
            <td>bit</td>
            <td>Returns 0.</td>
        </tr>
        <tr>
            <td>is_tracked_by_cdc</td>
            <td>bit</td>
            <td>Whether a base table that the view depends on is being tracked by CDC (change data capture). The value is always 0.</td>
        </tr>
    </tbody>
</table>
