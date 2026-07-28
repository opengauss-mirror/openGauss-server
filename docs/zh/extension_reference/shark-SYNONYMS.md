# SYNONYMS

返回同义词相关的信息。

**表1** SYNONYMS

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
            <td>同义词名称</td>
        </tr>
        <tr>
            <td>object_id</td>
            <td>oid</td>
            <td>同义词的id</td>
        </tr>
        <tr>
            <td>principal_id</td>
            <td>oid</td>
            <td>对象owner的oid，如果同义词的owner与同义词所属schema的owner相同，则返回NULL，否则返回同义词的owner</td>
        </tr>
        <tr>
            <td>schema_id</td>
            <td>oid</td>
            <td>所属schema的id</td>
        </tr>
        <tr>
            <td>parent_object_id</td>
            <td>oid</td>
            <td>同义词所属的 parent 对象 ID，取值恒为0</td>
        </tr>
        <tr>
            <td>type</td>
            <td>char(2)</td>
            <td>对象类型，取值恒为SN = Synonym</td>
        </tr>
        <tr>
            <td>type_desc</td>
            <td>nvarchar(60)</td>
            <td>对象类型描述，取值恒为Synonym</td>
        </tr>
        <tr>
            <td>create_date</td>
            <td>timestamp</td>
            <td>对象创建日期，取值恒为NULL</td>
        </tr>
        <tr>
            <td>modify_date</td>
            <td>timestamp</td>
            <td>对象修改日期，取值恒为NULL</td>
        </tr>
        <tr>
            <td>is_ms_shipped</td>
            <td>bit</td>
            <td>是否为系统内部对象，取值恒为0</td>
        </tr>
        <tr>
            <td>is_published</td>
            <td>bit</td>
            <td>对象是否发布，取值恒为0</td>
        </tr>
        <tr>
            <td>is_schema_published</td>
            <td>bit</td>
            <td>是否只发布架构，取值恒为0</td>
        </tr>
        <tr>
            <td>base_object_name</td>
            <td>nvarchar(1035)</td>
            <td>同义词对应的对象的完整引用名称，格式为schema_name.object_name</td>
        </tr>
    </tbody>
</table>
