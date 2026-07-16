# ALL_COLUMNS

用户定义对象和系统对象的所有列的集合。

**表1** ALL_COLUMNS

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
            <td>所属对象的 ID</td>
        </tr>
        <tr>
            <td>name</td>
            <td>name</td>
            <td>列名称</td>
        </tr>
        <tr>
            <td>column_id</td>
            <td>int</td>
            <td>列的 ID</td>
        </tr>
        <tr>
            <td>system_type_id</td>
            <td>oid</td>
            <td>列的数据类型 ID</td>
        </tr>
        <tr>
            <td>user_type_id</td>
            <td>oid</td>
            <td>列的数据类型 ID</td>
        </tr>
        <tr>
            <td>max_length</td>
            <td>smallint</td>
            <td>列的最大字节长度</td>
        </tr>
        <tr>
            <td>precision</td>
            <td>smallint</td>
            <td>如果是基于 numeric 的类型，则返回对应的 precision<br/>否则返回 0</td>
        </tr>
        <tr>
            <td>scale</td>
            <td>smallint</td>
            <td>如果是基于 numeric 的类型，则返回对应的 scale<br/>否则返回 0</td>
        </tr>
        <tr>
            <td>collation_name</td>
            <td>name</td>
            <td>列的字符排序名称</td>
        </tr>
        <tr>
            <td>is_nullable</td>
            <td>bit</td>
            <td>列是否允许 null 值</td>
        </tr>
        <tr>
            <td>is_ansi_padded</td>
            <td>bit</td>
            <td>返回 0</td>
        </tr>
        <tr>
            <td>is_rowguidcol</td>
            <td>bit</td>
            <td>返回 0</td>
        </tr>
        <tr>
            <td>is_identity</td>
            <td>bit</td>
            <td>返回 0</td>
        </tr>
        <tr>
            <td>is_computed</td>
            <td>bit</td>
            <td>1 列为计算列</td>
        </tr>
        <tr>
            <td>is_filestream</td>
            <td>bit</td>
            <td>返回 0</td>
        </tr>
        <tr>
            <td>is_replicated</td>
            <td>bit</td>
            <td>1 列已发布。如果列对应的表发布，则该表的所有列都发布。</td>
        </tr>
        <tr>
            <td>is_non_sql_subscribed</td>
            <td>bit</td>
            <td>返回 0</td>
        </tr>
        <tr>
            <td>is_merge_published</td>
            <td>bit</td>
            <td>返回 0</td>
        </tr>
        <tr>
            <td>is_dts_replicated</td>
            <td>bit</td>
            <td>返回 0</td>
        </tr>
        <tr>
            <td>is_xml_document</td>
            <td>bit</td>
            <td>返回 0</td>
        </tr>
        <tr>
            <td>xml_collection_id</td>
            <td>oid</td>
            <td>返回 0</td>
        </tr>
        <tr>
            <td>default_object_id</td>
            <td>oid</td>
            <td>列的默认值的 ID</td>
        </tr>
        <tr>
            <td>rule_object_id</td>
            <td>int</td>
            <td>返回 0</td>
        </tr>
        <tr>
            <td>is_sparse</td>
            <td>bit</td>
            <td>返回 0</td>
        </tr>
        <tr>
            <td>is_column_set</td>
            <td>bit</td>
            <td>返回 0</td>
        </tr>
        <tr>
            <td>generated_always_type</td>
            <td>tinyint</td>
            <td>返回 0</td>
        </tr>
        <tr>
            <td>generated_always_type_desc</td>
            <td>nvarchar(60)</td>
            <td>返回 NOT_APPLICABLE</td>
        </tr>
    </tbody>
</table>
