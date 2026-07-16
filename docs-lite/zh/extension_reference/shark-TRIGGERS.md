# TRIGGERS

返回DDL和DML触发器相关的信息。

**表1** TRIGGERS

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
            <td>name</td>
            <td>name</td>
            <td>触发器名称</td>
        </tr>
        <tr>
            <td>object_id</td>
            <td>oid</td>
            <td>触发器id</td>
        </tr>
        <tr>
            <td>parent_class</td>
            <td>tinyint</td>
            <td>触发器的父类，0表示DDL触发器，1表示DML触发器</td>
        </tr>
        <tr>
            <td>parent_class_desc</td>
            <td>nvarchar(60)</td>
            <td>触发器的父类的描述，DDL表示DDL触发器，OBJECT_OR_COLUMN表示DML触发器</td>
        </tr>
        <tr>
            <td>parent_id</td>
            <td>oid</td>
            <td>触发器的父类的id，0表示DDL触发器，针对DML触发器，取值为触发器所在表的id</td>
        </tr>
        <tr>
            <td>type</td>
            <td>char(2)</td>
            <td>触发器类型，取值恒为TR，对应SQL触发器</td>
        </tr>
        <tr>
            <td>type_desc</td>
            <td>nvarchar(60)</td>
            <td>触发器类型的说明，取值恒为SQL_TRIGGER</td>
        </tr>
        <tr>
            <td>create_date</td>
            <td>timestamp</td>
            <td>触发器的创建时间，取值恒为NULL</td>
        </tr>
        <tr>
            <td>modify_date</td>
            <td>timestamp</td>
            <td>触发器的修改时间，取值恒为NULL</td>
        </tr>
        <tr>
            <td>is_ms_shipped</td>
            <td>bit</td>
            <td>是否为系统内部对象，取值恒为0</td>
        </tr>
        <tr>
            <td>is_disabled</td>
            <td>bit</td>
            <td>触发器是否被禁用，1表示触发器被禁用，0表示触发器未被禁用</td>
        </tr>
        <tr>
            <td>is_not_for_replication</td>
            <td>bit</td>
            <td>触发器是否通过not for replication选项创建，取值恒为0</td>
        </tr>
        <tr>
            <td>is_instead_of_trigger</td>
            <td>tinyint</td>
            <td>是否为INSTEAD OF触发器，取值如下：<br/>
            1 = INSTEAD OF 触发器<br/>
            0 = AFTER 触发器或者 DDL 触发器<br/>
            2 = BEFORE 触发器
            </td>
        </tr>
    </tbody>
</table>
