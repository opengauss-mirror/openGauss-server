# SYSINDEXES

SYSINDEXES视图为当前数据库中的每个索引和表各对应一行。此视图不支持 XML 索引。 此视图中不支持已分区表和索引。

**表1** SYSINDEXES视图字段

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
            <td>id</td>
            <td>oid</td>
            <td>索引所属表的 ID</td>
        </tr>
        <tr>
            <td>status</td>
            <td>int</td>
            <td>返回 NULL</td>
        </tr>
        <tr>
            <td>first</td>
            <td>bytea</td>
            <td>返回 NULL</td>
        </tr>
        <tr>
            <td>indid</td>
            <td>oid</td>
            <td>索引 ID</td>
        </tr>
        <tr>
            <td>root</td>
            <td>bytea</td>
            <td>返回 NULL</td>
        </tr>
        <tr>
            <td>minlen</td>
            <td>smallint</td>
            <td>行的最小大小。返回 0</td>
        </tr>
        <tr>
            <td>keycnt</td>
            <td>smallint</td>
            <td>键数。返回0</td>
        </tr>
        <tr>
            <td>groupid</td>
            <td>smallint</td>
            <td>返回 0</td>
        </tr>
        <tr>
            <td>dpages</td>
            <td>int</td>
            <td>返回 0</td>
        </tr>
        <tr>
            <td>reserved</td>
            <td>int</td>
            <td>返回 0</td>
        </tr>
        <tr>
            <td>used</td>
            <td>int</td>
            <td>返回 0</td>
        </tr>
        <tr>
            <td>rowcnt</td>
            <td>bigint</td>
            <td>返回 0</td>
        </tr>
        <tr>
            <td>rowmodctr</td>
            <td>int</td>
            <td>返回 0</td>
        </tr>
        <tr>
            <td>reserved3</td>
            <td>int</td>
            <td>返回 0</td>
        </tr>
        <tr>
            <td>reserved4</td>
            <td>int</td>
            <td>返回 0</td>
        </tr>
        <tr>
            <td>xmaxlen</td>
            <td>smallint</td>
            <td>行的最大大小。返回 0</td>
        </tr>
        <tr>
            <td>maxirow</td>
            <td>smallint</td>
            <td>返回 NULL</td>
        </tr>
        <tr>
            <td>OrigFillFactor</td>
            <td>tinyint</td>
            <td>创建索引时使用的初始填充因子值。 </td>
        </tr>
        <tr>
            <td>StatVersion</td>
            <td>tinyint</td>
            <td>返回 0。</td>
        </tr>
        <tr>
            <td>reserved2</td>
            <td>int</td>
            <td>返回 0。</td>
        </tr>
        <tr>
            <td>FirstIAM</td>
            <td>bytea</td>
            <td>返回 NULL</td>
        </tr>
        <tr>
            <td>impid</td>
            <td>smallint</td>
            <td>索引实现标志。 返回 0。</td>
        </tr>
        <tr>
            <td>lockflags</td>
            <td>smallint</td>
            <td>用于约束经过考虑的索引锁粒度。 返回 0。</td>
        </tr>
        <tr>
            <td>pgmodctr</td>
            <td>int</td>
            <td>返回 0。</td>
        </tr>
        <tr>
            <td>keys</td>
            <td>bytea</td>
            <td>组成索引键的列 ID 列表。<br> 返回 NULL。<br> 若要显示索引键列，请使用[sysindexkeys](shark-SYSINDEXKEYS.md)。</td>
        </tr>
        <tr>
            <td>name</td>
            <td>name</td>
            <td>索引的名称。</td>
        </tr>
        <tr>
            <td>statblob</td>
            <td>blob</td>
            <td>统计信息二进制大型对象 (BLOB)。<br> 返回 NULL。</td>
        </tr>
        <tr>
            <td>maxlen</td>
            <td>int</td>
            <td>返回 0</td>
        </tr>
        <tr>
            <td>rows</td>
            <td>int</td>
            <td>返回 0</td>
        </tr>
    </tbody>
</table>
