# COLUMNS

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-07-30T06:33:11.012Z pushedAt=2026-09-22T03:52:44.448Z -->

The COLUMNS view returns column information in the database.

**Table 1** COLUMNS

<table aria-label="Table 1" class="table table-sm margin-top-none">
    <thead>
        <tr>
            <th>Column Name</th>
            <th>Type</th>
            <th>Description</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>TABLE_CATALOG</td>
            <td>nvarchar(128)</td>
            <td>Table qualifier</td>
        </tr>
        <tr>
            <td>TABLE_SCHEMA</td>
            <td>nvarchar(128)</td>
            <td>Name of the schema to which the table belongs</td>
        </tr>
        <tr>
            <td>TABLE_NAME</td>
            <td>nvarchar(128)</td>
            <td>Table name</td>
        </tr>
        <tr>
            <td>COLUMN_NAME</td>
            <td>nvarchar(128)</td>
            <td>Column name</td>
        </tr>
        <tr>
            <td>ORDINAL_POSITION</td>
            <td>int</td>
            <td>Column identifier</td>
        </tr>
        <tr>
            <td>COLUMN_DEFAULT</td>
            <td>nvarchar(4000)</td>
            <td>Default value of the column</td>
        </tr>
        <tr>
            <td>IS_NULLABLE</td>
            <td>varchar(3)</td>
            <td>Nullability of the column. If the column allows NULL, this column returns YES. Otherwise, it returns NO.</td>
        </tr>
        <tr>
            <td>DATA_TYPE</td>
            <td>nvarchar(128)</td>
            <td>System-supplied data type</td>
        </tr>
        <tr>
            <td>CHARACTER_MAXIMUM_LENGTH</td>
            <td>int</td>
            <td>Maximum length in characters of binary data, character data, or text and image data. -1 for xml and large value type data. Otherwise, returns NULL.</td>
        </tr>
        <tr>
            <td>CHARACTER_OCTET_LENGTH</td>
            <td>int</td>
            <td>Maximum length in bytes of binary data, character data, or text and image data. -1 indicates xml and large value type data.</td>
        </tr>
        <tr>
            <td>NUMERIC_PRECISION</td>
            <td>tinyint</td>
            <td>Precision of approximate numeric data, exact numeric data, integer data, or monetary data. Otherwise, returns NULL.</td>
        </tr>
        <tr>
            <td>NUMERIC_PRECISION_RADIX</td>
            <td>smallint</td>
            <td>Radix of precision for approximate numeric data, exact numeric data, integer data, or monetary data. Otherwise, NULL is returned.</td>
        </tr>
        <tr>
            <td>NUMERIC_SCALE</td>
            <td>int</td>
            <td>Scale of approximate numeric data, exact numeric data, integer data, or monetary data. Otherwise, NULL is returned.</td>
        </tr>
        <tr>
            <td>DATETIME_PRECISION</td>
            <td>smallint</td>
            <td>Subtype code for datetime and ISO interval data types. For other data types, returns NULL.</td>
        </tr>
        <tr>
            <td>CHARACTER_SET_CATALOG</td>
            <td>nvarchar(128)</td>
            <td>Always returns NULL.</td>
        </tr>
        <tr>
            <td>CHARACTER_SET_SCHEMA</td>
            <td>nvarchar(128)</td>
            <td>Always returns NULL.</td>
        </tr>
        <tr>
            <td>CHARACTER_SET_NAME</td>
            <td>nvarchar(128)</td>
            <td>If the column is a character data or text data type, returns the unique name of the character set. Otherwise, returns NULL.</td>
        </tr>
        <tr>
            <td>COLLATION_CATALOG</td>
            <td>nvarchar(128)</td>
            <td>Always returns NULL.</td>
        </tr>
        <tr>
            <td>COLLATION_SCHEMA</td>
            <td>nvarchar(128)</td>
            <td>Always returns NULL.</td>
        </tr>
        <tr>
            <td>COLLATION_NAME</td>
            <td>nvarchar(128)</td>
            <td>If the column is character data or text data type, returns the unique name of the collation. Otherwise, returns NULL.</td>
        </tr>
        <tr>
            <td>DOMAIN_CATALOG</td>
            <td>nvarchar(128)</td>
            <td>If the column is an alias data type, this column is the name of the database in which the user-defined data type was created. Otherwise, returns NULL.</td>
        </tr>
        <tr>
            <td>DOMAIN_SCHEMA</td>
            <td>nvarchar(128)</td>
            <td>If the column is a user-defined data type, this column returns the schema name of the user-defined data type. Otherwise, returns NULL.</td>
        </tr>
        <tr>
            <td>DOMAIN_NAME</td>
            <td>nvarchar(128)</td>
            <td>If the column is a user-defined data type, this column is the name of the user-defined data type. Otherwise, returns NULL.</td>
        </tr>
    </tbody>
</table>