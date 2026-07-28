# SYSCOLUMNS

SYSCOLUMNS视图为每个表和视图中的每列返回一行，并为数据库中的存储过程的每个参数返回一行。

**表1** SYSCOLUMNS视图字段

<table aria-label="表 1" class="table table-sm margin-top-none">
    <thead>
        <tr>
            <th>列名称</th>
            <th>数据类型</th>
            <th>说明</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>name</td>
            <td>name</td>
            <td>列或过程参数的名称。</td>
        </tr>
        <tr>
            <td>id</td>
            <td>oid</td>
            <td>此列所属表的对象 ID，或者与此参数关联的存储过程的 ID。</td>
        </tr>
        <tr>
            <td>xtype</td>
            <td>oid</td>
            <td>类型 ID</td>
        </tr>
        <tr>
            <td>typestat</td>
            <td>tinyint</td>
            <td>直接返回0</td>
        </tr>
        <tr>
            <td>xusertype</td>
            <td>smallint</td>
            <td>类型 ID</td>
        </tr>
        <tr>
            <td>length</td>
            <td>smallint</td>
            <td>sys 的最大物理存储长度。类型。</td>
        </tr>
        <tr>
            <td>xprec</td>
            <td>tinyint</td>
            <td>直接返回 0</td>
        </tr>
        <tr>
            <td>xscale</td>
            <td>tinyint</td>
            <td>直接返回 0</td>
        </tr>
        <tr>
            <td>colid</td>
            <td>smallint</td>
            <td>列 ID 或参数 ID。</td>
        </tr>
        <tr>
            <td>xoffset</td>
            <td>smallint</td>
            <td>直接返回 0</td>
        </tr>
        <tr>
            <td>bitpos</td>
            <td>tinyint</td>
            <td>直接返回 0</td>
        </tr>
        <tr>
            <td>reserved</td>
            <td>tinyint</td>
            <td>直接返回 0</td>
        </tr>
        <tr>
            <td>colstat</td>
            <td>smallint</td>
            <td>直接返回 0</td>
        </tr>
        <tr>
            <td>cdefault</td>
            <td>oid</td>
            <td>此列的默认值的 ID。</td>
        </tr>
        <tr>
            <td>domain</td>
            <td>oid</td>
            <td>此列的规则或 CHECK 约束的 ID。</td>
        </tr>
        <tr>
            <td>number</td>
            <td>smallint</td>
            <td>过程分组时的子过程号。直接返回 0</td>
        </tr>
        <tr>
            <td>colorder</td>
            <td>smallint</td>
            <td>直接返回 0</td>
        </tr>
        <tr>
            <td>autoval</td>
            <td>bytea</td>
            <td>直接返回 null</td>
        </tr>
        <tr>
            <td>offset</td>
            <td>smallint</td>
            <td>此列所在行的偏移量。直接返回 0</td>
        </tr>
        <tr>
            <td>collationid</td>
            <td>oid</td>
            <td>列的排序规则的 ID。 对于非字符列，此值为 NULL。</td>
        </tr>
        <tr>
            <td>status</td>
            <td>tinyint</td>
            <td>用于说明列或参数的属性的位图：<br> 0x08 = 列允许空值。<br> 0x40 = 参数为 OUTPUT 参数。</td>
        </tr>
        <tr>
            <td>type</td>
            <td>oid</td>
            <td>类型 ID</td>
        </tr>
        <tr>
            <td>usertype</td>
            <td>oid</td>
            <td>所属架构 ID</td>
        </tr>
        <tr>
            <td>printfmt</td>
            <td>varchar(255)</td>
            <td>直接返回 null</td>
        </tr>
        <tr>
            <td>prec</td>
            <td>smallint</td>
            <td>此列的精度级别。<br> -1 = xml 或大值类型。</td>
        </tr>
        <tr>
            <td>scale</td>
            <td>int</td>
            <td>列的 scale <br><br> NULL = 数据类型不是数值。</td>
        </tr>
        <tr>
            <td>iscomputed</td>
            <td>int</td>
            <td>指示列是否为计算列的标志：<br><br> 0 = 非计算列。<br><br> 1 = 计算列。</td>
        </tr>
        <tr>
            <td>isoutparam</td>
            <td>int</td>
            <td>指示过程参数是否为输出参数：<br><br> 1 = True<br><br> 0 = False</td>
        </tr>
        <tr>
            <td>isnullable</td>
            <td>int</td>
            <td>指示列是否允许空值：<br><br> 1 = True<br><br> 0 = False</td>
        </tr>
        <tr>
            <td>collation</td>
            <td>name</td>
            <td>列的排序规则的名称。 如果不是基于字符的列，则为 NULL。</td>
        </tr>
    </tbody>
</table>
