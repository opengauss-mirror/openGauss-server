# INDEXS

返回所有索引。

**表1** INDEXS

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
            <td>索引名称</td>
        </tr>
        <tr>
            <td>index_id</td>
            <td>oid</td>
            <td>索引 ID</td>
        </tr>
        <tr>
            <td>type</td>
            <td>tinyint</td>
            <td>当前支持：<br/>2 = Nonclustered rowstore (B-tree)<br/>6 = Nonclustered columnstore index<br/>7 = Nonclustered hash index. </td>
        </tr>
        <tr>
            <td>type_desc</td>
            <td>nvarchar(60)</td>
            <td>type的描述信息。当前支持：<br/>NONCLUSTERED<br/>NONCLUSTERED COLUMNSTORE<br/>NONCLUSTERED HASH</td>
        </tr>
        <tr>
            <td>is_unique</td>
            <td>bit</td>
            <td>是否唯一索引</td>
        </tr>
        <tr>
            <td>data_space_id</td>
            <td>oid</td>
            <td>索引的数据空间。索引对应的tablespace。</td>
        </tr>
        <tr>
            <td>ignore_dup_key</td>
            <td>bit</td>
            <td>返回 0</td>
        </tr>
        <tr>
            <td>is_primary_key</td>
            <td>bit</td>
            <td>是否为主键</td>
        </tr>
        <tr>
            <td>is_unique_constraint</td>
            <td>bit</td>
            <td>是否唯一约束</td>
        </tr>
        <tr>
            <td>fill_factor</td>
            <td>tinyint</td>
            <td>填充因子</td>
        </tr>
        <tr>
            <td>is_padded</td>
            <td>bit</td>
            <td>返回 0</td>
        </tr>
        <tr>
            <td>is_disabled</td>
            <td>bit</td>
            <td>是否禁用索引</td>
        </tr>
        <tr>
            <td>is_hypothetical</td>
            <td>bit</td>
            <td>返回 0</td>
        </tr>
        <tr>
            <td>allow_row_locks</td>
            <td>bit</td>
            <td>返回 1</td>
        </tr>
        <tr>
            <td>allow_page_locks</td>
            <td>bit</td>
            <td>返回 1</td>
        </tr>
        <tr>
            <td>has_filter</td>
            <td>bit</td>
            <td>是否为部分索引</td>
        </tr>
        <tr>
            <td>filter_definition</td>
            <td>nvarchar</td>
            <td>部分索引定义</td>
        </tr>
        <tr>
            <td>compression_delay</td>
            <td>int</td>
            <td>返回 0</td>
        </tr>
        <tr>
            <td>suppress_dup_key_messages</td>
            <td>bit</td>
            <td>返回 0</td>
        </tr>
    </tbody>
</table>
