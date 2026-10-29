# TRIGGERS

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-09-13T02:25:56.659Z pushedAt=2026-09-29T02:34:56.692Z -->

Returns information about DDL and DML triggers.

**Table 1** TRIGGERS

<table aria-label="表1" class="table table-sm margin-top-none">
    <thead>
        <tr>
            <th>Name</th>
            <th>Type</th>
            <th>Description</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>name</td>
            <td>name</td>
            <td>Trigger name.</td>
        </tr>
        <tr>
            <td>object_id</td>
            <td>oid</td>
            <td>Trigger ID.</td>
        </tr>
        <tr>
            <td>parent_class</td>
            <td>tinyint</td>
            <td>The trigger's parent class. `0` indicates a DDL trigger, and `1` indicates a DML trigger.</td>
        </tr>
        <tr>
            <td>parent_class_desc</td>
            <td>nvarchar(60)</td>
            <td>Description of the trigger's parent class. `DDL` indicates a DDL trigger, and `OBJECT_OR_COLUMN` indicates a DML trigger.</td>
        </tr>
        <tr>
            <td>parent_id</td>
            <td>oid</td>
            <td>ID of the trigger's parent class. `0` indicates a DDL trigger. For a DML trigger, the value is the ID of the table containing the trigger.</td>
        </tr>
        <tr>
            <td>type</td>
            <td>char(2)</td>
            <td>Trigger type. Always TR, which corresponds to an SQL trigger.</td>
        </tr>
        <tr>
            <td>type_desc</td>
            <td>nvarchar(60)</td>
            <td>Description of the trigger type. Always SQL_TRIGGER.</td>
        </tr>
        <tr>
            <td>create_date</td>
            <td>timestamp</td>
            <td>Trigger creation time. Always NULL.</td>
        </tr>
        <tr>
            <td>modify_date</td>
            <td>timestamp</td>
            <td>Trigger modification time. Always NULL.</td>
        </tr>
        <tr>
            <td>is_ms_shipped</td>
            <td>bit</td>
            <td>Whether the object is an internal system object. Always 0.</td>
        </tr>
        <tr>
            <td>is_disabled</td>
            <td>bit</td>
            <td>Whether the trigger is disabled. `1`: disabled. `0`: not disabled.</td>
        </tr>
        <tr>
            <td>is_not_for_replication</td>
            <td>bit</td>
            <td>Whether the trigger is created with the `not for replication` option. Always 0.</td>
        </tr>
        <tr>
            <td>is_instead_of_trigger</td>
            <td>tinyint</td>
            <td>Whether it is an INSTEAD OF trigger. Value options:<br>
            1 = INSTEAD OF trigger<br>
            0 = AFTER trigger or DDL trigger<br>
            2 = BEFORE trigger
            </td>
        </tr>
    </tbody>
</table>
