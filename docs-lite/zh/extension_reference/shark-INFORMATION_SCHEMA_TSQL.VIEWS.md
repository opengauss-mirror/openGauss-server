# VIEWS 

VIEWS视图返回数据库中的视图信息。

**表1** VIEWS 

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
            <td>包含该表的架构的名称</td>
        </tr>
        <tr>
            <td>TABLE_NAME</td>
            <td>nvarchar(128)</td>
            <td>视图名称</td>
        </tr>
        <tr>
            <td>VIEW_DEFINITION</td>
            <td>nvarchar(4000)</td>
            <td>如果定义的长度大于 nvarchar(4000)，则该列在 4000 处截断。 否则，该列是视图定义文本。</td>
        </tr>
         <tr>
            <td>CHECK_OPTION</td>
            <td>varchar(7)</td>
            <td>WITH CHECK OPTION 的类型。 如果最初的视图是使用 WITH CHECK OPTION 创建的，那么就为 CASCADE。 否则，返回 NONE。</td>
        </tr>
         <tr>
            <td>IS_UPDATABLE</td>
            <td>varchar(2)</td>
            <td>指定视图是否可更新。1=TRUE,0=FALSE</td>
        </tr>
    </tbody>
</table>
