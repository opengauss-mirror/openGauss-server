# SYSCOLUMNS

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-07-30T06:35:19.594Z pushedAt=2026-09-22T07:18:57.303Z -->

The SYSCOLUMNS view returns one row for each column in every table and view, and one row for each parameter of a stored procedure in the database.

**Table 1** SYSCOLUMNS view fields

<table aria-label="Table 1" class="table table-sm margin-top-none">
    <thead>
        <tr>
            <th>Column Name</th>
            <th>Data Type</th>
            <th>Description</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>name</td>
            <td>name</td>
            <td>Name of the column or procedure parameter.</td>
        </tr>
        <tr>
            <td>id</td>
            <td>oid</td>
            <td>The object ID of the table to which this column belongs, or the ID of the stored procedure associated with this parameter.</td>
        </tr>
        <tr>
            <td>xtype</td>
            <td>oid</td>
            <td>Type ID</td>
        </tr>
        <tr>
            <td>typestat</td>
            <td>tinyint</td>
            <td>Direct Return 0</td>
        </tr>
        <tr>
            <td>xusertype</td>
            <td>smallint</td>
            <td>Type ID</td>
        </tr>
        <tr>
            <td>length</td>
            <td>smallint</td>
            <td>Maximum physical storage length of the type in sys.</td>
        </tr>
        <tr>
            <td>xprec</td>
            <td>tinyint</td>
            <td>Direct Return 0</td>
        </tr>
        <tr>
            <td>xscale</td>
            <td>tinyint</td>
            <td>Direct Return 0</td>
        </tr>
        <tr>
            <td>colid</td>
            <td>smallint</td>
            <td>Column ID or parameter ID.</td>
        </tr>
        <tr>
            <td>xoffset</td>
            <td>smallint</td>
            <td>Direct Return 0</td>
        </tr>
        <tr>
            <td>bitpos</td>
            <td>tinyint</td>
            <td>Direct Return 0</td>
        </tr>
        <tr>
            <td>reserved</td>
            <td>tinyint</td>
            <td>Direct Return 0</td>
        </tr>
        <tr>
            <td>colstat</td>
            <td>smallint</td>
            <td>Direct Return 0</td>
        </tr>
        <tr>
            <td>cdefault</td>
            <td>oid</td>
            <td>ID of the default value for this column.</td>
        </tr>
        <tr>
            <td>domain</td>
            <td>oid</td>
            <td>ID of the rule or CHECK constraint for this column.</td>
        </tr>
        <tr>
            <td>number</td>
            <td>smallint</td>
            <td>Subprocedure number when procedures are grouped. Direct Return 0.</td>
        </tr>
        <tr>
            <td>colorder</td>
            <td>smallint</td>
            <td>Direct Return 0</td>
        </tr>
        <tr>
            <td>autoval</td>
            <td>bytea</td>
            <td>Direct Return null</td>
        </tr>
        <tr>
            <td>offset</td>
            <td>smallint</td>
            <td>The offset of the row where this column resides. Direct Return 0.</td>
        </tr>
        <tr>
            <td>collationid</td>
            <td>oid</td>
            <td>ID of the collation for the column. For non-character columns, this value is NULL.</td>
        </tr>
        <tr>
            <td>status</td>
            <td>tinyint</td>
            <td>Bitmap indicating the attributes of the column or parameter:<br> 0x08 = Column allows null values.<br> 0x40 = Parameter is an OUTPUT parameter.</td>
        </tr>
        <tr>
            <td>type</td>
            <td>oid</td>
            <td>Type OID</td>
        </tr>
        <tr>
            <td>usertype</td>
            <td>oid</td>
            <td>Schema OID</td>
        </tr>
        <tr>
            <td>printfmt</td>
            <td>varchar(255)</td>
            <td>Direct Return null</td>
        </tr>
        <tr>
            <td>prec</td>
            <td>smallint</td>
            <td>The precision level of this column.<br> -1 = xml or large value types.</td>
        </tr>
        <tr>
            <td>scale</td>
            <td>int</td>
            <td>The scale of the column.<br><br> NULL = the data type is not numeric.</td>
        </tr>
        <tr>
            <td>iscomputed</td>
            <td>int</td>
            <td>Flag indicating whether the column is a computed column:<br><br> 0 = Non-computed column.<br><br> 1 = Computed column.</td>
        </tr>
        <tr>
            <td>isoutparam</td>
            <td>int</td>
            <td>Indicates whether the procedure parameter is an output parameter:<br><br> 1 = True<br><br> 0 = False</td>
        </tr>
        <tr>
            <td>isnullable</td>
            <td>int</td>
            <td>Indicates whether the column allows null values:<br><br> 1 = True<br><br> 0 = False</td>
        </tr>
        <tr>
            <td>collation</td>
            <td>name</td>
            <td>Name of the column's collation. NULL if it is not a character-based column.</td>
        </tr>
    </tbody>
</table>