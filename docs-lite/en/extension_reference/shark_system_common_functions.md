# shark-System Common Functions

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-07-30T06:29:45.414Z pushedAt=2026-09-24T11:10:35.109Z -->

This section only contains the system common functions added by the shark plugin.

- rand()

    Description: Returns a random number between 0.0 and 1.0. Equivalent to random.

    Return Value Type: double precision

    Example:

    ```
    openGauss=# SELECT rand();
            rand
    -------------------
    0.254671605769545
    (1 row)
    ```

- rand(seed int)

    Description: Sets the random number seed based on the input parameter, and then generates a random number between 0.0 and 1.0. Equivalent to `setseed` + `random`. The valid range of the seed is [-2 ^ 31, 2 ^ 31 - 1].

    Return Value Type: double precision

    Example:

    ```
    openGauss=# SELECT rand(1);
            rand
    -------------------
    0.0416303444653749
    (1 row)
    ```

- day(timestamp)

    Description: Obtains the value of the day component from a date/time value.

    Return Value Type: double precision

    Example:

    ```
    openGauss=# SELECT day(timestamp '2001-02-16 20:38:40');
    day
    -----------
        16
    (1 row)

    openGauss=# SELECT day('2002-4-25'::date);
    day
    -----------
        25
    (1 row)

    openGauss=# SELECT day('2025-02-28 00:00:01'::timestamp(0) without time zone);
    day
    -----------
        28
    (1 row)
    ```

- ERROR_NUMBER()

    Description: Returns the error number of the exception in a PL procedure.

    Parameter Type: None

    Return Value Type: int

    Example: See the example of ERROR_MESSAGE().

- ERROR_SEVERITY()

    Description: Returns the severity value of an exception in a PL procedure.

    Parameter Type: None

    Return Value Type: int

    Example: See the example of ERROR_MESSAGE().

- ERROR_STATE()

    Description: Returns the status number of the error message for an exception in a PL process.

    Parameter Type: None

    Return Value Type: int

    Example: See the ERROR_MESSAGE() example.

- ERROR_PROCEDURE()

    Description: Returns the name of the stored procedure or trigger where the error occurred during PL execution.

    Parameter Type: None

    Return Value Type: text

    Example: See the ERROR_MESSAGE() example.

- ERROR_LINE()

    Description: Returns the line number where an error occurs during PL execution.

    Parameter Type: None

    Return Value Type: int

    Example: See the ERROR_MESSAGE() example.

- ERROR_MESSAGE()

    Description: Returns the complete text of the error message during a PL procedure.

    Parameter Type: None

    Return Value Type: text

    Example:

```
    openGauss=#CREATE TABLE test(a int);
    openGauss=#CREATE OR REPLACE PROCEDURE p1()
               AS
               BEGIN
                  select 1/0;
               END;
               /
    openGauss=#CREATE OR REPLACE PROCEDURE p2()
               AS
               BEGIN
                   BEGIN TRY
                       delete from test;
                       insert into test values(1);
                       insert into test values(2);
                       call p1();
                       insert into test values(3);
                   END TRY
                   BEGIN CATCH
                       insert into test values(4);
                       RAISE NOTICE 'ERROR_NUMBER() is %', ERROR_NUMBER();
                       RAISE NOTICE 'ERROR_SEVERITY() is %', ERROR_SEVERITY();
                       RAISE NOTICE 'ERROR_STATE() is %', ERROR_STATE();
                       RAISE NOTICE 'ERROR_PROCEDURE() is %', ERROR_PROCEDURE();
                       RAISE NOTICE 'ERROR_LINE() is %', ERROR_LINE();
                       RAISE NOTICE 'ERROR_MESSAGE() is %', ERROR_MESSAGE();
                   END CATCH;
                END;
                /
    openGauss=#CALL p2();
    NOTICE:  ERROR_NUMBER() is 33816706
    NOTICE:  ERROR_SEVERITY() is 20
    NOTICE:  ERROR_STATE() is 1
    NOTICE:  ERROR_PROCEDURE() is p1()
    NOTICE:  ERROR_LINE() is 2
    NOTICE:  ERROR_MESSAGE() is division by zero

```

- ident_current(table_or_view)

    Description: Returns the last identity value generated for the specified table or view. The last identity value generated can be for any session and any scope.

    Parameter Type: table_or_view is the name of the table or view, of type nvarchar(128).

    Return Value Type: numeric(38, 0)

    Example:

    ```
    openGauss=# CREATE TABLE employees(id int identity, name varchar(100) NOT NULL);
    CREATE TABLE
    
    -- Table containing an identity column, with no data inserted yet.
    openGauss=# SELECT ident_current('employees');
    ident_current 
    ---------------
                1
    (1 row)
    
    -- Table containing an identity column, with the sequence value updated.
    openGauss=# INSERT INTO employees(name) VALUES('alice');
    INSERT 0 1
    openGauss=# INSERT INTO employees(name) VALUES('bob');
    INSERT 0 1
    openGauss=# SELECT ident_current('employees');
    ident_current 
    ---------------
                2
    (1 row)
    ```

- dateadd(datepart , number , date)

    Description: Returns the time obtained by adding number to the datepart portion of date.

    Parameter Description:

    - datepart:

      The date part to which the number is added.

      | *datepart*  | **Abbreviation Form** |
      | ----------- | ------------ |
      | year        | yy, yyyy     |
      | quarter     | qq, q        |
      | month       | mm, m        |
      | dayofyear   | dy, y        |
      | day         | dd, d        |
      | week        | wk, ww       |
      | weekday     | dw, w        |
      | hour        | hh           |
      | minute      | mi, n        |
      | second      | ss, s        |
      | millisecond | ms           |
      | microsecond | mcs          |
      | nanosecond  | ns           |

    - number:

      The amount to be added.

    - date:

      A valid date, specifying the start time. Supported data types: date, time, timetz, timestamp, and timestamptz.

    Example:

    ```
    select dateadd(hh,1,timestamp'1997-12-31 23:59:59');
             dateadd          
    --------------------------
     Thu Jan 01 00:59:59 1998
    (1 row)
    
    select dateadd(dd,1,timestamp'1997-12-31 23:59:59');
             dateadd          
    --------------------------
     Thu Jan 01 23:59:59 1998
    (1 row)
    ```

- datepart(datepart , date)

    Description: Returns an integer representing the specified datepart of date.

    Parameter Description:

    - datepart:

      Specifies the specific part to return.

      | *datepart*  | **Abbreviation Form** |
      | ----------- | --------------------- |
      | year        | yy, yyyy              |
      | quarter     | qq, q                 |
      | month       | mm, m                 |
      | dayofyear   | dy, y                 |
      | day         | dd, d                 |
      | week        | wk, ww                |
      | weekday     | dw, w                 |
      | hour        | hh                    |
      | minute      | mi, n                 |
      | second      | ss, s                 |
      | millisecond | ms                    |
      | microsecond | mcs                   |
      | nanosecond  | ns                    |
      | TZoffset    | tz                    |
      | ISO_WEEK    | ISOWK, ISOWW          |

    - date:

      A valid date that specifies the time. Supported data types: date, time, timetz, timestamp, and timestamptz.

    Example:

    ```
    SELECT DATEPART(year,timestamp'2007-10-30 12:15:32.1234567');
     datepart 
    ----------
         2007
    (1 row)
    
    SELECT DATEPART(quarter,timestamp'2007-10-30 12:15:32.1234567');
     datepart 
    ----------
            4
    (1 row)
    ```

- datename (datepart, date)

    Description: Returns a string representing the specified datepart of date.

    Parameters

    - datepart:

      Specifies the specific part to return.

      | *datepart*  | **Abbreviation Form** |
      | ----------- | ------------ |
      | year        | yy, yyyy     |
      | quarter     | qq, q        |
      | month       | mm, m        |
      | dayofyear   | dy, y        |
      | day         | dd, d        |
      | week        | wk, ww       |
      | weekday     | dw, w        |
      | hour        | hh           |
      | minute      | mi, n        |
      | second      | ss, s        |
      | millisecond | ms           |
      | microsecond | mcs          |
      | nanosecond  | ns           |
      | TZoffset    | tz           |
      | ISO_WEEK    | ISOWK, ISOWW |

    - date:

      A valid date that specifies the start time. Supported data types: date, time, timetz, timestamp, and timestamptz.

    Examples:

    ```
    SELECT DATENAME(mm,timestamp'2007-1-30 12:15:32.1234567');
     datename 
    ----------
     January
    (1 row)
    
    SELECT DATENAME(m,timestamp'2007-2-28 12:15:32.1234567');
     datename 
    ----------
     February
    (1 row)
    ```

- getdate()

    Description: Obtains the current system time.

    Example:

    ```
    select getdate();
             getdate
    -------------------------
     2025-08-22 06:05:14.853
    (1 row)
    ```

- len(expr)

    Description: Returns the data length.

    Example:

    ```
    SELECT LEN('abc');
     len 
    -----
       3
    (1 row)
    ```

- log10(float_expression)

    Description: Accepts a floating-point expression and calculates the base-10 logarithm.

    Parameter Type: double precision

    Return Value Type: double precision

    Example:

    ```
    openGauss=# select log(100);
     log 
    -----
       2
    (1 row)
    ```

- isnull(check_expression, replacement_value)

    Description: Returns the first non-NULL value.

    Parameter Type: check_expression can be of any type, and replacement_value must be of a type that can be implicitly or explicitly converted to the type of check_expression.

    Return Value Type: The return value type is the same as the type of check_expression.

    Example:

    ```
    openGauss=# select isnull(1, NULL);
     isnull 
    --------
          1
    (1 row)

    openGauss=# select isnull(NULL, 'abc');
     isnull 
    --------
     abc
    (1 row)
    ```

- atn2(float_expression, float_expression)

    Description: Returns the angle in radians between the positive X-axis and the ray from the origin to the point (y, x), where x and y are the values of the two specified floating-point expressions.

    Parameter Type: double precision

    Return Value Type: double precision

    Example:

    ```
    openGauss=# select atan2(1.2, 2.5);
         atan2      
    -----------------
     .44751997515717
    (1 row)
    ```

- charindex(expressionToFind, expressionToSearch [, start_location])

    Description: Searches for the first occurrence of expressionToFind in expressionToSearch. If start_location is specified, the search starts from start_location.

    Parameter Type: expressionToFind and expressionToSearch are of the text type, and start_location is of the int type.

    Return Value Type: int

    Example:

    ```
    openGauss=# select charindex('aaa', 'aaa bbb ccc aaa');
     charindex 
    -----------
             1
    (1 row)

    openGauss=# select charindex('aaa', 'aaa bbb ccc aaa', 4);
     charindex 
    -----------
            13
    (1 row)
    ```

- datediff(datepart, startdate, enddate)

    Description: Returns the difference between enddate and startdate in the unit specified by datepart.

    Parameter Type: datepart specifies the date unit. See the following table for details. startdate and enddate are of the timestamp type.

    Return Value Type: int

    datepart Type:

    | *datepart*  | **Abbreviation Form** |
    | ----------- | ------------ |
    | year        | yy, yyyy     |
    | quarter     | qq, q        |
    | month       | mm, m        |
    | dayofyear   | dy, y        |
    | day         | dd, d        |
    | week        | wk, ww       |
    | weekday     | dw, w        |
    | hour        | hh           |
    | minute      | mi, n        |
    | second      | ss, s        |
    | millisecond | ms           |
    | microsecond | mcs          |
    | nanosecond  | ns           |

    Example:

    ```
    openGauss=# select datediff(day, timestamp'1997-12-31 23:59:59', timestamp'1998-12-31 23:59:59');
     datediff 
    ----------
          365
    (1 row)
    ```

- datediff_big(datepart, startdate, enddate)

    Description: Returns the difference between enddate and startdate in the unit specified by datepart.

    Parameter Type: datepart is the specified date unit, same as datediff. startdate and enddate are of timestamp type

    Return Value Type: bigint

    Example

    ```
    openGauss=# select datediff_big(second, timestamp'1997-12-31 23:59:59', timestamp'1998-12-31 23:59:59');
     datediff_big 
    --------------
         31536000
    (1 row)
    ```

- cast(expression AS data_type[(length)])

    Description: Converts an expression to the specified type

    Parameter Type: expression is an expression of any type, data_type is a type keyword, and length is of int type

    Return Value Type: the specified data_type type

    Remarks:
    - In D databases, the default value of length is 30. This default value mainly applies to string-related types. Currently, in D compatibility mode, both `char` and `varchar` and their alias types are subject to this length default value.

    Example:

    ```
    openGauss=# select cast(123456789 AS char) as result;
             result             
    --------------------------------
     123456789                     
    (1 row)
    ```

- try_cast(expression AS data_type[(length)])

    Description: Converts an expression to a specified type. An error is reported if an unsupported type conversion is performed. NULL is returned if the type conversion is supported but fails.

    Parameter Type: expression is an expression of any type, data_type is a type keyword, and length is of int type.

    Return Value Type: The specified data_type.

    Remarks:
    - In D database, the default value of length is 30, which is mainly applicable to string-related types. Currently, in D compatibility mode, both `char` and `varchar` and their alias types apply this length default value.

    ```
    openGauss=# select try_cast(123456789 AS smallint) as result;
     result 
    --------
            
    (1 row)
    ```

- convert(data_type[(length)], expression[, style])

    Description: Converts an expression to a specified type.

    Parameter Type: expression is an expression of any type, data_type is a type keyword, and length is of int type.

    Return Value Type: The specified data_type type.

    Remarks:
    - For different type conversions, style can have one of the values shown in the following table. Other values are treated as 0.
    - Currently, only styles involving dates, times, floating-point numbers, and money are supported.

    >[!NOTE] Note Description
    >When using `cast` and `convert`, if the output involves character-based month/day-of-week representations, ensure that the `lc_time` of the current database is consistent with the system. You can use `show lc_time` to view the current database `lc_time`, and `set lc_time` to modify the current database `lc_time`. You can use the command `locale` to view the `lang` and `locale` information of the current operating system, and `locale -a` to list the `locale` values supported by the current operating system.

    **Table 1** Date and time styles

    <table aria-label="Table 1" class="table table-sm margin-top-none">
        <thead>
            <tr>
                <th>Without century digits</th>
                <th>With century digits</th>
                <th>Standard</th>
                <th>Input/Output</th>
            </tr>
        </thead>
        <tbody>
            <tr>
                <td>-</td>
                <td>0 or 100</td>
                <td>Default Value</td>
                <td>mon dd yyyy hh:miAM</td>
            </tr>
            <tr>
                <td>1</td>
                <td>101</td>
                <td>US</td>
                <td>1 = mm/dd/yy<br>101 = mm/dd/yyyy</td>
            </tr>
            <tr>
                <td>2</td>
                <td>102</td>
                <td>ANSI</td>
                <td>2 = yy.mm.dd<br>102 = yyyy.mm.dd</td>
            </tr>
            <tr>
                <td>3</td>
                <td>103</td>
                <td>UK/France</td>
                <td>3 = dd/mm/yy<br>103 = dd/mm/yyyy</td>
            </tr>
            <tr>
                <td>4</td>
                <td>104</td>
                <td>Germany</td>
                <td>4 = dd.mm.yy<br>104 = dd.mm.yyyy</td>
            </tr>
            <tr>
                <td>5</td>
                <td>105</td>
                <td>Italy</td>
                <td>5 = dd-mm-yy<br>105 = dd-mm-yyyy</td>
            </tr>
            <tr>
                <td>6</td>
                <td>106</td>
                <td>-</td>
                <td>6 = dd mon yy<br>106 = dd mon yyyy</td>
            </tr>
            <tr>
                <td>7</td>
                <td>107</td>
                <td>-</td>
                <td>7 = Mon dd, yy<br>107 = Mon dd, yyyy</td>
            </tr>
            <tr>
                <td>8 or 24</td>
                <td>108</td>
                <td>-</td>
                <td>hh:mi:ss</td>
            </tr>
            <tr>
                <td>-</td>
                <td>9 or 109</td>
                <td>Default format + milliseconds</td>
                <td>9 = mon dd yyyy<br>109 = hh:mi:ss:mmmAM(PM)</td>
            </tr>
            <tr>
                <td>10</td>
                <td>110</td>
                <td>US</td>
                <td>11 = yy/mm/dd<br>111 = yyyy/mm/dd</td>
            </tr>
            <tr>
                <td>11</td>
                <td>111</td>
                <td>Japan</td>
                <td>11 = yy/mm/dd<br>111 = yyyy/mm/dd</td>
            </tr>
            <tr>
                <td>12</td>
                <td>112</td>
                <td>ISO</td>
                <td>12 = yymmdd<br>112 = yyyymmdd</td>
            </tr>
            <tr>
                <td>-</td>
                <td>13 or 113</td>
                <td>European default format + milliseconds</td>
                <td>dd mon yyyy hh:mi:ss:mmm (24-hour format)</td>
            </tr>
            <tr>
                <td>14</td>
                <td>114</td>
                <td>-</td>
                <td>hh:mi:ss:mmm(24-Hour Format)</td>
            </tr>
            <tr>
                <td>-</td>
                <td>20 or 120</td>
                <td>ODBC canonical</td>
                <td>yyyy-mm-dd hh:mi:ss</td>
            </tr>
            <tr>
                <td>-</td>
                <td>21 or 25 or 121</td>
                <td>ODBC canonical (millisecond identifier) default value for time, date, datetime2, and<br>datetimeoffset</td>
                <td>yyyy-mm-dd hh:mi:ss.mmm (24-hour format)</td>
            </tr>
            <tr>
                <td>22</td>
                <td>-</td>
                <td>US Time</td>
                <td>mm/dd/yy hh:mi:ss AM(PM)</td>
            </tr>
            <tr>
                <td>-</td>
                <td>23</td>
                <td>ISO8601</td>
                <td>yyyy-mm-dd</td>
            </tr>
            <tr>
                <td>-</td>
                <td>126</td>
                <td>ISO8601</td>
                <td>yyyy-mm-ddThh:mi:ss.mmm</td>
            </tr>
            <tr>
                <td>-</td>
                <td>127</td>
                <td>ISO8601 with time zone Z</td>
                <td>yyy-MM-ddThh:mm:ss.fffZ</td>
            </tr>
            <tr>
                <td>-</td>
                <td>130</td>
                <td>Hijri</td>
                <td>dd mon yyyy<br>hh:mi:ss:mmmAM</td>
            </tr>
            <tr>
                <td>-</td>
                <td>131</td>
                <td>Hijri</td>
                <td>dd/mm/yyyy<br>hi:mi:ss:mmmAM</td>
            </tr>
        </tbody>
    </table>

    **Table 2** float and real styles

    <table aria-label="Table 2" class="table table-sm margin-top-none">
        <thead>
            <tr>
                <th>Value</th>
                <th>Output</th>
            </tr>
        </thead>
        <tbody>
            <tr>
                <td>0</td>
                <td>Contains up to 6 digits, using scientific notation as needed.</td>
            </tr>
            <tr>
                <td>1</td>
                <td>Always an 8-digit value, using scientific notation as needed.</td>
            </tr>
            <tr>
                <td>2</td>
                <td>Always a 16-digit value, using scientific notation as needed.</td>
            </tr>
            <tr>
                <td>3</td>
                <td>Always a 17-digit value, used for lossless conversion.</td>
            </tr>
        </tbody>
    </table>

    **Table 3** money style

    <table aria-label="Table 3" class="table table-sm margin-top-none">
        <thead>
            <tr>
                <th>Value</th>
                <th>Output</th>
            </tr>
        </thead>
        <tbody>
            <tr>
                <td>0</td>
                <td>No comma is used to separate every three digits to the left of the decimal point, and two digits are taken to the right of the decimal point.</td>
            </tr>
            <tr>
                <td>1</td>
                <td>A comma is used to separate every three digits to the left of the decimal point, and two digits are taken to the right of the decimal point.</td>
            </tr>
            <tr>
                <td>2</td>
                <td>Digits to the left of the decimal point are not separated by commas every three digits, and four digits are taken to the right of the decimal point.</td>
            </tr>
            <tr>
                <td>126</td>
                <td>When converting to char(n) or varchar(n), equivalent to style 2.</td>
            </tr>
        </tbody>
    </table>

    Example:

    ```
    openGauss=# select convert(varchar, timestamp'2012-03-23 00:12:23', 1) as result;
      result  
    ----------
     03/23/12
    (1 row)
    ```

- try_convert(data_type[(length)], expression[, style])

    Description: Converts an expression to a specified type. An error is reported if an unsupported type conversion is performed. NULL is returned if the type conversion is supported but fails.

    Parameter Type: expression is an expression of any type, data_type is a type keyword, and length is of int type.

    Return Value Type: The specified data_type type.

    Example:

    ```
    openGauss=# select try_convert(smallint, 123456789) as result;
     result 
    --------
       
    (1 row)
    ```

- newid()

    Description: Generates a globally unique identifier.

    Parameter Type: None

    Return Value Type: uuid

    Note: Generates a globally unique identifier based on the UUID v1 (timestamp + MAC address) version. The implementation is the same as the uuid() function in the openGauss B-compatible dolphin plugin, with the difference being the return value type: the uuid() function in the dolphin plugin returns varchar, while the newid() function in the shark plugin returns uuid.

    Example:

    ```
    openGauss=# select newid();
                    newid
    --------------------------------------
     53018234-09ed-11cf-8676-f82e3f373370
    (1 row)

    openGauss=# select pg_typeof(newid());
    pg_typeof
    -----------
    uuid
    (1 row)

    ```

- object_name(object_id int [, database_id int])

    Description: Returns the name of an object based on object_id. database_id is an optional parameter that can be omitted or passed as the OID of the current database; otherwise, NULL is always returned.

    Parameter Type: Both object_id and database_id are parameters of the int type.

    Return Value Type: nvarchar

    Note: If the object is a table, trigger, or constraint, the SELECT privilege on the object is required. If the object is a stored procedure or function, the EXECUTE privilege is required. If the object is a type, the USAGE privilege is required.

    Example:

    ```sql
    openGauss=# CREATE TABLE students (
    openGauss(#     id SERIAL PRIMARY KEY,
    openGauss(#     name VARCHAR(100) NOT NULL,
    openGauss(#     age INT DEFAULT 0,
    openGauss(#     grade DECIMAL(5, 2)
    openGauss(# );
    NOTICE:  CREATE TABLE will create implicit sequence "students_id_seq" for serial column "students.id"
    NOTICE:  CREATE TABLE / PRIMARY KEY will create implicit index "students_pkey" for table "students"
    CREATE TABLE
    openGauss=#
    openGauss=# select object_name(object_id('students'));
     object_name
    -------------
     students
    (1 row)
    ```

- object_schema_name(object_id int [, database_id int])

    Description: Returns the schema name of the object based on object_id. database_id is an optional parameter, which can be omitted or passed as the OID of the current database; otherwise, NULL is returned.

    Parameter Type: Both object_id and database_id are parameters of the int type.

    Return Value Type: nvarchar

    Note: If object is a table, trigger, or constraint, you need the SELECT privilege on the object. If object is a stored procedure or function, you need the EXECUTE privilege. If object is a type, you need the USAGE privilege.

    Example:

    ```sql
    openGauss=# select object_schema_name(object_id('students'));
     object_schema_name
    --------------------
     public
    (1 row)
    ```

- object_definition(object_id int)

    Description: Returns the definition of an object based on object_id. Only supports obtaining the definitions of views, check constraints, functions, and triggers.

    Parameter Type: int

    Return Value Type: nvarchar

    NOTE
If object is a table, trigger, or constraint, the SELECT privilege on the object is required. If object is a stored procedure or function, the EXECUTE privilege is required. If object is a type, the USAGE privilege is required.

    Example:

    ```sql
    openGauss=# create view view1 as select * from students;
    CREATE VIEW
    openGauss=# select object_definition(object_id('view1'));
        object_definition
    --------------------------
     SELECT  * FROM students;
    (1 row)
    ```

- objectpropertyex(object_id int, property varchar)

    Description: Obtains the attribute information of the input property based on object_id.

    Parameter Type: id is of int type, and property is of varchar type.

    Return Value Type: sql_variant

    Note: property currently supports only "basetype". Other attribute results are consistent with the objectproperty function.

    Example:

    ```sql
    openGauss=# select objectpropertyex(object_id('students'), 'BaseType');
     objectpropertyex
    ------------------
     U
    (1 row)
    ```

- col_length(object_name text, column_name text)

    Description: Returns the length of the type of the specified column based on object_name and column_name.

    Parameter Type: Both object_name and column_name are of the text type.

    Return Value Type: smallint

    Example:

    ```sql
    openGauss=# SELECT COL_LENGTH('students', 'age');
     col_length
    ------------
              4
    (1 row)
    ```

- col_name(object_id int, column_id int)

    Description: Obtains the column name specified by column_id based on object_id and column_id.

    Parameter Type: Both object_id and column_id are of int type.

    Return Value Type: text

    Example:

    ```sql
    openGauss=# select col_name(object_id('students'), 1);
     col_name
    ----------
     id
    (1 row)
    ```

- columnproperty(object_id int, column_name text, property_name text)

    Description: Obtains the specified attribute information based on object_id and column_name for the given property.

    Parameter Type: object_id is of int type, and both column_name and property_name are of text type.

    Return Value Type: int

    Note: property_name currently supports only "charmaxlen", "allowsnull", "iscomputed", "columnid", "ishidden", "isidentity", "ordinal", "precision", and "scale".

    Example:

    ```sql
    openGauss=# SELECT sys.columnproperty(OBJECT_ID('students'), 'name', 'charmaxlen');
     columnproperty
    ----------------
                100
    (1 row)
    ```

- year(input ANYELEMENT)

    Description: Returns the year information of a date type.

    Parameter Type: Any type

    Return Value Type: int

    Example:

    ```sql
    openGauss=# SELECT YEAR('20251010');
     year
    ------
     2025
    (1 row)
    ```

- month(input ANYELEMENT)

    Description: Returns the month information of a date type.

    Parameter Type: ANYELEMENT

    Return Value Type: int

    Example:

    ```sql
    openGauss=# SELECT MONTH('20251010');
     month
    -------
        10
    (1 row)
    ```

- day(input ANYELEMENT)

    Description: Returns the day information of a date type.

    Parameter Type: any type

    Return Value Type: int

    Example:

    ```sql
    openGauss=# SELECT day('20251010');
     month
    -------
        10
    (1 row)
    ```

- isdate(input text)

    Description: Returns whether the input string is a valid datetime, date, or time type.

    Parameter Type: text

    Return Value Type: int

    NOTE
    For dates with millisecond precision greater than 3, false is returned.

    Example:

    ```sql
    openGauss=# SELECT ISDATE('2023-10-05 14:30:00');
     isdate
    --------
          1
    (1 row)
    ```

- eomonth(start_date date, month_to_add int DEFAULT 0)

    Description: Returns the last day of the month for the specified date, with an optional offset.

    Parameter Type: start_date is of date type, and month_to_add is of int type.

    Return Value Type: date

    Example:

    ```sql
    openGauss=# SELECT EOMONTH('2023-11-10', 2);
      eomonth
    ------------
     2024-01-31
    (1 row)
    ```

- sysdatetime()

    Description: Returns the current time.

    Return Value Type: timestamptz

    Example:

    ```sql
    openGauss=# select sysdatetime();
              sysdatetime
    -------------------------------
     2025-12-29 14:19:58.594762+08
    (1 row)
    ```

- square(num float8)

    Description: Returns the square of the input number.

    Parameter Type: float8

    Return Value Type: float8

    Example:

    ```sql
    openGauss=# select square(2.4);
     square
    --------
       5.76
    (1 row)
    ```

- isnumeric(expr ANYELEMENT)

    Description: Determines whether the input string is a valid number or money type.

    Return Value Type: int

    Example:

    ```sql
    openGauss=# select isnumeric('123456');
     isnumeric
    -----------
             1
    (1 row)
    ```

- patindex(pattern varchar, expression varchar)

    Description: Returns the starting position of the match for a regular expression pattern in the input string.

    Parameter Type: Both pattern and expression are of the varchar type.

    Return Value Type: bigint

    **NOTE** patindex performs pattern matching based on the substring function, so the range of patterns supported by patindex is consistent with that of substring.

    Example:

    ```sql
    openGauss=# SELECT PATINDEX('%abc%', 'xyzabc123');
     patindex
    ----------
            4
    (1 row)
    ```

- stuff(character_expression varchar, start int, length int, replace_with_expression varchar)

    Description: Deletes characters of length length from the start position of character_expression, and then inserts replace_with_expression at the start position of the first string.

    Parameter Type: character_expression and replace_with_expression are both of varchar type, and start and length are both of int type.

    Return Value Type: varchar

    Example:

    ```sql
    openGauss=# SELECT STUFF('abcdefg', 2, 3, 'XYZ');
      stuff
    ---------
     aXYZefg
    (1 row)
    ```

- str(float_expression numeric, length int, decimal int)

    Description: Returns character data converted from numeric data, with support for specifying length and decimal precision.

    Parameter Type: float_expression is of numeric type, and length and decimal are of int type.

    Return Value Type: varchar

    Example:

    ```sql
    openGauss=# SELECT STR(123.45, 6, 1);
      str
    --------
      123.5
    (1 row)
    ```

- replicate(string_expression text, integer_expression int)

    Description: Repeats a string a specified number of times.

    Parameter Type: string_expression is of the text type, and integer_expression is of the int type.

    Return Value Type: varchar

    Example:

    ```sql
    openGauss=# SELECT REPLICATE('abc', 2);
     replicate
    -----------
     abcabc
    (1 row)
    ```

 - string_split(string_expression varchar, delimiter char)

    Description: Splits a string into rows of substrings based on the specified delimiter.

    Parameter Type: string_expression is of varchar type, and delimiter is of char type.

    Return Value Type: setof

    Example:

    ```sql
    openGauss=# SELECT value FROM STRING_SPLIT('nice to meet you.', ' ');
     value
    -------
     nice
     to
     meet
     you.
    (4 rows)
    ```

 - quotename(string_expression varchar [, quote_character char] )

    Description: Wraps string_expression with quote_character. The default value of quote_character is "[]".

    Parameter Type: string_expression is of varchar type, and quote_character is of char type.

    Return Value Type: varchar

    Example:

    ```sql
    openGauss=# SELECT quotename('abcd', ']');
     quotename
    -----------
     [abcd]
    (1 row)
    ```

 - trim([characters varchar FROM ] string_expression)

    Description: Removes leading and trailing spaces or other specified characters from a string.

    Parameter Type: string_expression is of varchar type, and characters is of varchar type.

    Return Value Type: varchar

    Example:

    ```sql
    openGauss=# select trim(' abc ');
     btrim
    -------
     abc
    (1 row)
    openGauss=#
    openGauss=# select trim('a' from 'aabca');
     btrim
    -------
     bc
    (1 row)
    ```

 - sql_variant_property(sql_variant_expression sql_variant, property varchar)

    Description: Returns the attribute information of sql_variant.

    Parameter Type: sql_variant_expression is of sql_variant type, and property is of varchar type.

    Return Value Type: sql_variant

    Note: property_name currently supports only "basetype", "precision", "scale", "totalbytes", and "maxlength". Other properties return empty.

    Example:

    ```sql
    openGauss=# select SQL_VARIANT_PROPERTY(cast(cast('a' as nvarchar) as sql_variant), 'BaseType');
     sql_variant_property
    ----------------------
     nvarchar
    (1 row)
    
    openGauss=# select SQL_VARIANT_PROPERTY(cast(cast('a' as nvarchar) as sql_variant), 'precision');
     sql_variant_property
    ----------------------
     5
    (1 row)
    ```