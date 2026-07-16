# COLUMNS

COLUMNS视图返回数据库中的列信息。

**表1** COLUMNS

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
            <td>TABLE_CATALOG</td>
            <td>nvarchar(128)</td>
            <td>表限定符</td>
        </tr>
        <tr>
            <td>TABLE_SCHEMA</td>
            <td>nvarchar(128)</td>
            <td>表所属架构的名称</td>
        </tr>
        <tr>
            <td>TABLE_NAME</td>
            <td>nvarchar(128)</td>
            <td>表名</td>
        </tr>
        <tr>
            <td>COLUMN_NAME</td>
            <td>nvarchar(128)</td>
            <td>列名</td>
        </tr>
        <tr>
            <td>ORDINAL_POSITION</td>
            <td>int</td>
            <td>列标识号</td>
        </tr>
        <tr>
            <td>COLUMN_DEFAULT</td>
            <td>nvarchar（4000）</td>
            <td>列的默认值</td>
        </tr>
        <tr>
            <td>IS_NULLABLE</td>
            <td>varchar(3)</td>
            <td>列的为空性。 如果列允许 NULL，则该列将返回 YES。 否则，返回 NO。</td>
        </tr>
        <tr>
            <td>DATA_TYPE</td>
            <td>nvarchar(128)</td>
            <td>系统提供的数据类型</td>
        </tr>
        <tr>
            <td>CHARACTER_MAXIMUM_LENGTH</td>
            <td>int</td>
            <td>二进制数据、字符数据或文本和图像数据的最大长度（字符）。-1 表示 xml 和大值类型数据。 否则，返回 NULL。 </td>
        </tr>
        <tr>
            <td>CHARACTER_OCTET_LENGTH</td>
            <td>int</td>
            <td>二进制数据、字符数据或文本和图像数据的最大长度（字节）。-1 表示 xml 和大值类型数据。</td>
        </tr>
        <tr>
            <td>NUMERIC_PRECISION</td>
            <td>tinyint</td>
            <td>近似数字数据、精确数字数据、整数数据或货币数据的精度。 否则，返回 NULL。</td>
        </tr>
        <tr>
            <td>NUMERIC_PRECISION_RADIX</td>
            <td>smallint</td>
            <td>近似数字数据、精确数字数据、整数数据或货币数据的精度基数。 否则，返回 NULL。</td>
        </tr>
        <tr>
            <td>NUMERIC_SCALE</td>
            <td>int</td>
            <td>近似数字数据、精确数字数据、整数数据或货币数据的小数位数。 否则，返回 NULL。</td>
        </tr>
        <tr>
            <td>DATETIME_PRECISION</td>
            <td>smallint</td>
            <td>日期时间和 ISO 间隔数据类型的子类型代码。 对于其他数据类型，返回 NULL。</td>
        </tr>
        <tr>
            <td>CHARACTER_SET_CATALOG</td>
            <td>nvarchar(128)</td>
            <td>始终返回 NULL。</td>
        </tr>
        <tr>
            <td>CHARACTER_SET_SCHEMA</td>
            <td>nvarchar(128)</td>
            <td>始终返回 NULL。</td>
        </tr>
        <tr>
            <td>CHARACTER_SET_NAME</td>
            <td>nvarchar(128)</td>
            <td>如果此列是字符数据或 文本 数据类型，则返回字符集的唯一名称。 否则，返回 NULL。</td>
        </tr>
        <tr>
            <td>COLLATION_CATALOG</td>
            <td>nvarchar(128)</td>
            <td>始终返回 NULL。</td>
        </tr>
        <tr>
            <td>COLLATION_SCHEMA</td>
            <td>nvarchar(128)</td>
            <td>始终返回 NULL。</td>
        </tr>
        <tr>
            <td>COLLATION_NAME</td>
            <td>nvarchar(128)</td>
            <td>如果列是字符数据或 文本 数据类型，则返回排序规则的唯一名称。 否则，返回 NULL。</td>
        </tr>
        <tr>
            <td>DOMAIN_CATALOG</td>
            <td>nvarchar(128)</td>
            <td>如果此列是别名数据类型，则此列是在其中创建用户定义数据类型的数据库的名称。 否则，返回 NULL。</td>
        </tr>
        <tr>
            <td>DOMAIN_SCHEMA</td>
            <td>nvarchar(128)</td>
            <td>如果列是用户定义数据类型，则此列将返回该用户定义数据类型的架构名称。 否则，返回 NULL。</td>
        </tr>
        <tr>
            <td>DOMAIN_NAME</td>
            <td>nvarchar(128)</td>
            <td>如果列是用户定义数据类型，则此列是该用户定义数据类型的名称。 否则，返回 NULL。</td>
        </tr>
    </tbody>
</table>
