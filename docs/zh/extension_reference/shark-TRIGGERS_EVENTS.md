# TRIGGER_EVENTS

返回DDL和DML触发器相关的信息。

**表1** TRIGGER_EVENTS

<table aria-label="表1" class="table table-sm margin-top-none">
    <thead>
        <tr>
            <th>列名称</th>
            <th>类型</th>
            <th>说明</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>object_id</td>
            <td>oid</td>
            <td>触发器id</td>
        </tr>
        <tr>
            <td>type</td>
            <td>int</td>
            <td>导致触发器触发的事件类型，取值如下：<br/>
            1 = INSERT<br/>
            2 = UPDATE<br/>
            3 = DELETE<br/>
            4 = TRUNCATE<br/>
            5 = DDL_COMMAND_START<br/>
            6 = DDL_COMMAND_STOP<br/>
            7 = TABLE_REWRITE<br/>
            8 = SQL_DROP
            </td>
        </tr>
        <tr>
            <td>type_desc</td>
            <td>nvarchar(60)</td>
            <td>导致触发器触发的事件类型描述，取值如下：<br/>
            INSERT<br/>
            UPDATE<br/>
            DELETE<br/>
            TRUNCATE<br/>
            DDL_COMMAND_START<br/>
            DDL_COMMAND_STOP<br/>
            TABLE_REWRITE<br/>
            SQL_DROP
            </td>
        </tr>
        <tr>
            <td>is_first</td>
            <td>bit</td>
            <td>触发器被标记为在此事件中首先触发，取值恒为0</td>
        </tr>
        <tr>
            <td>is_last</td>
            <td>bit</td>
            <td>触发器被标记为在此事件中最后触发，取值恒为0</td>
        </tr>
        <tr>
            <td>event_group_type</td>
            <td>int</td>
            <td>触发器创建所在的事件组，取值恒为NULL</td>
        </tr>
        <tr>
            <td>event_group_type_desc</td>
            <td>nvarchar(60)</td>
            <td>触发器创建所在的事件组的描述，取值恒为NULL</td>
        </tr>
        <tr>
            <td>is_trigger_event</td>
            <td>bit</td>
            <td>是否为触发器事件，取值恒为1</td>
        </tr>
    </tbody>
</table>
