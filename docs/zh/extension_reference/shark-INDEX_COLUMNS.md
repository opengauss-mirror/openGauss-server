# INDEX_COLUMNS

返回索引相关的信息。

**表1** INDEX_COLUMNS

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
            <td>索引所定义的对象的id</td>
        </tr>
        <tr>
            <td>index_id</td>
            <td>oid</td>
            <td>索引的id</td>
        </tr>
        <tr>
            <td>index_column_id</td>
            <td>int</td>
            <td>索引列的id，取值为1至索引列的个数</td>
        </tr>
        <tr>
            <td>column_id</td>
            <td>int</td>
            <td>索引列在其所定义的对象对应列的编号</td>
        </tr>
        <tr>
            <td>key_ordinal</td>
            <td>tinyint</td>
            <td>参与索引查询的索引列的序数，取值为1至参与索引查询的列的个数</td>
        </tr>
        <tr>
            <td>partition_ordinal</td>
            <td>tinyint</td>
            <td>分区列集内的序数，针对普通表或者分区表的非一维分区列，取值为0；<br/>
            针对分区表一维分区键，取值为分区键集的序数
            </td>
        </tr>
        <tr>
            <td>is_descending_key</td>
            <td>bit</td>
            <td>索引键列是否为降序排序方向，1表示降序排序，0表示升序排序</td>
        </tr>
        <tr>
            <td>is_included_column</td>
            <td>bit</td>
            <td>是否为include子句添加的非键列，1表示非键列(不参与索引查询)，0表示参与索引查询的列</td>
        </tr>
    </tbody>
</table>
