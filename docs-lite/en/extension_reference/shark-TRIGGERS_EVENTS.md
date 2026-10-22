# TRIGGER_EVENTS

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-09-13T02:24:24.248Z pushedAt=2026-09-29T02:34:56.690Z -->

Returns information about DDL and DML triggers.

**Table 1** TRIGGER_EVENTS

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
            <td>object_id</td>
            <td>oid</td>
            <td>Trigger ID.</td>
        </tr>
        <tr>
            <td>type</td>
            <td>int</td>
            <td>Type of the event that fires the trigger. The value options are as follows:<br>
            1 = INSERT<br>
            2 = UPDATE<br>
            3 = DELETE<br>
            4 = TRUNCATE<br>
            5 = DDL_COMMAND_START<br>
            6 = DDL_COMMAND_STOP<br>
            7 = TABLE_REWRITE<br>
            8 = SQL_DROP
            </td>
        </tr>
        <tr>
            <td>type_desc</td>
            <td>nvarchar(60)</td>
            <td>Description of the type of the event that fires the trigger. The value options are as follows:<br>
            INSERT<br>
            UPDATE<br>
            DELETE<br>
            TRUNCATE<br>
            DDL_COMMAND_START<br>
            DDL_COMMAND_STOP<br>
            TABLE_REWRITE<br>
            SQL_DROP
            </td>
        </tr>
        <tr>
            <td>is_first</td>
            <td>bit</td>
            <td>Whether the trigger is marked as the first to fire for this event. Always 0.</td>
        </tr>
        <tr>
            <td>is_last</td>
            <td>bit</td>
            <td>Whether the trigger is marked as the last to fire in this event. Always 0.</td>
        </tr>
        <tr>
            <td>event_group_type</td>
            <td>int</td>
            <td>Event group in which the trigger was created. Always NULL.</td>
        </tr>
        <tr>
            <td>event_group_type_desc</td>
            <td>nvarchar(60)</td>
            <td>Description of the event group in which the trigger was created. Always NULL.</td>
        </tr>
        <tr>
            <td>is_trigger_event</td>
            <td>bit</td>
            <td>Whether it is a trigger event. Always 1.</td>
        </tr>
    </tbody>
</table>
