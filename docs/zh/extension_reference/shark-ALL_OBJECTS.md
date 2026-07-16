# ALL_OBJECTS

所有架构范围内的用户定义对象和系统对象的集合。

**表1** ALL_OBJECTS

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
            <td>对象名称</td>
        </tr>
        <tr>
            <td>object_id</td>
            <td>oid</td>
            <td>对象 ID</td>
        </tr>
        <tr>
            <td>principal_id</td>
            <td>oid</td>
            <td>对象 owner 的oid。<br/>如果当前owner与schema为同一个owner，则返回NULL<br/>如果是以下类型，则也直接返回NULL值： <br/>C<br/>D<br/>F<br/>PK<br/>TR<br/>UQ</td>
        </tr>
        <tr>
            <td>schema_id</td>
            <td>oid</td>
            <td>所属 schema 的 ID</td>
        </tr>
        <tr>
            <td>parent_object_id</td>
            <td>oid</td>
            <td>返回对象所属的 parent 对象 ID</td>
        </tr>
        <tr>
            <td>type</td>
            <td>char(2)</td>
            <td>
            对象类型。当前支持类型：<br/>
            AF = AGGREGATE_FUNCTION<br/>
            C = CHECK_CONSTRAINT<br/>
            D = DEFAULT<br/>
            F = FOREIGN_KEY_CONSTRAINT<br/>
            FN = SQL_SCALAR_FUNCTION<br/>
            P = SQL_STORED_PROCEDURE<br/>
            PK = PRIMARY_KEY_CONSTRAINT<br/>
            S = SYSTEM_BASE_TABLE<br/>
            SN = SYNONYM<br/>
            SO = SEQUENCE_OBJECT<br/>
            U = USER_TABLE<br/>
            V = VIEW<br/>
            TR = SQL DML trigger<br/>
            UQ = UNIQUE_CONSTRAINT
            </td>
        </tr>
        <tr>
            <td>type_desc</td>
            <td>nvarchar(60)</td>
            <td>
            对象类型描述。当前支持类型：<br/>
            AGGREGATE_FUNCTION<br/>
            CHECK_CONSTRAINT<br/>
            DEFAULT<br/>
            FOREIGN_KEY_CONSTRAINT<br/>
            SQL_SCALAR_FUNCTION<br/>
            SQL_STORED_PROCEDURE<br/>
            PRIMARY_KEY_CONSTRAINT<br/>
            SYSTEM_BASE_TABLE<br/>
            SYNONYM<br/>
            SEQUENCE_OBJECT<br/>
            USER_TABLE<br/>
            VIEW<br/>
            SQL DML trigger<br/>
            UNIQUE_CONSTRAINT
            </td>
        </tr>
        <tr>
            <td>create_date</td>
            <td>timestamp</td>
            <td>对象创建日期</td>
        </tr>
        <tr>
            <td>modify_date</td>
            <td>timestamp</td>
            <td>对象修改日期</td>
        </tr>
        <tr>
            <td>is_ms_shipped</td>
            <td>bit</td>
            <td>是否为系统内部对象<br/>如系统表、视图等返回 1，用户表等返回 0</td>
        </tr>
        <tr>
            <td>is_published</td>
            <td>bit</td>
            <td>对象是否发布</td>
        </tr>
        <tr>
            <td>is_schema_published</td>
            <td>bit</td>
            <td>是否只发布架构</td>
        </tr>
    </tbody>
</table>
