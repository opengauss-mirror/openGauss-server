# shark-系统常用函数

本章节只包含shark插件新增的系统常用函数。

- rand()

    描述：0.0到1.0之间的随机数。等价于random。

    返回值类型：double precision

    示例：

    ```
    openGauss=# SELECT rand();
            rand
    -------------------
    0.254671605769545
    (1 row)
    ```

- rand(seed int)

    描述：根据入参设置随机数种子，随后生成 0.0 到 1.0 之间的随机数。等价于 `setseed` + `random`。种子的有效值范围为 [-2 ^ 31, 2 ^ 31 - 1]。

    返回值类型：double precision

    示例：

    ```
    openGauss=# SELECT rand(1);
            rand
    -------------------
    0.0416303444653749
    (1 row)
    ```

- day(timestamp)

    描述：获取日期/时间值中天数的值。

    返回值类型：double precision

    示例：

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

    描述：返回PL过程中异常的错误号。

    参数类型: 无

    返回值类型：int

    示例：参见ERROR_MESSAGE()示例

- ERROR_SEVERITY()

    描述：返回PL过程中异常的严重性值。

    参数类型: 无

    返回值类型：int

    示例：参见ERROR_MESSAGE()示例

- ERROR_STATE()

    描述：返回PL过程中异常的错误消息的状态号

    参数类型: 无

    返回值类型：int

    示例：参见ERROR_MESSAGE()示例

- ERROR_PROCEDURE()

    描述：返回PL过程中生成错误的存储过程或触发器的名称

    参数类型: 无

    返回值类型：text

    示例：参见ERROR_MESSAGE()示例

- ERROR_LINE()

    描述：返回PL过程中出现错误的行号

    参数类型: 无

    返回值类型：int

    示例：参见ERROR_MESSAGE()示例

- ERROR_MESSAGE()

    描述：返回PL过程中错误消息的完整文本

    参数类型: 无

    返回值类型：text

    示例：

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

    描述：返回为指定的表或视图生成的最后一个标识值。所生成的最有一个标识值可以针对任何会话和任何作用域。

    参数类型: table_or_view为表或视图的名称，类型为nvarchar(128)。

    返回值类型：numeric(38, 0)

    示例：
    
    ```
    openGauss=# CREATE TABLE employees(id int identity, name varchar(100) NOT NULL);
    CREATE TABLE
    
    -- 包含identity列的表，还未插入数据
    openGauss=# SELECT ident_current('employees');
    ident_current 
    ---------------
                1
    (1 row)

    -- 包含identity列的表，更新了序列值
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

    描述：返回将number添加到date的datepart部分后得到的时间。

    参数说明：

    - datepart：

      要增加number的日期部分。
    
      | *datepart*  | **缩写形式** |
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
    
    - number：
    
      要增加的数量。
    
    - date：
    
      合法的日期，指定起始时间。支持数据类型：date、time、timetz、timestamp和timestamptz。
    
    示例：
    
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

    描述：返回表示 date 的指定 datepart 的整数。

    参数说明：

    - datepart：

      指定返回的特定部分。
    
      | *datepart*  | **缩写形式** |
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
    
    - date：
    
      合法的日期，指定时间。支持数据类型：date、time、timetz、timestamp和timestamptz。
    
    示例：
    
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
    
- datename（datepart , date）

    描述：返回表示 date 的指定 datepart 的字符串。

    参数：

    - datepart ：

      指定返回的特定部分。
    
      | *datepart*  | **缩写形式** |
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
    
    - date：
    
      合法的日期，指定起始时间。支持数据类型：date、time、timetz、timestamp和timestamptz。
    
    示例：
    
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

    描述：获取当前系统时间。

    示例：

    ```
    select getdate();
             getdate
    -------------------------
     2025-08-22 06:05:14.853
    (1 row)
    ```
    
- len(expr)

    描述：返回数据长度。

    示例：

    ```
    SELECT LEN('abc');
     len 
    -----
       3
    (1 row)
    ```

- log10(float_expression)

    描述：接受一个浮点数表达式，计算10为底的对数

    参数类型：double precision

    返回值类型：double precision

    示例：
    
    ```
    openGauss=# select log(100);
     log 
    -----
       2
    (1 row)
    ```

- isnull(check_expression, replacement_value)

    描述：返回第一个非NULL值

    参数类型: check_expression可为任意类型，replacement_value需为可以隐式或显式地转换为check_expression的类型

    返回值类型：返回值类型与check_expression类型相同

    示例：

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

    描述：返回以弧度表示的角，该角位于正X轴和原点至点(y, x)的射线之间，其中x和y是两个指定的浮点数表达式的值

    参数类型：double precision

    返回值类型：double precision

    示例：

    ```
    openGauss=# select atan2(1.2, 2.5);
         atan2      
    -----------------
     .44751997515717
    (1 row)
    ```

- charindex(expressionToFind, expressionToSearch [, start_location])

    描述：在expressionToSearch中搜索expressionToFind第一次出现的位置，如果start_location存在的话，则从start_location开始

    参数类型：expressionToFind与expressionToSearch为text类型，start_location为int类型

    返回值类型：int

    示例：

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

    描述：返回指定datepart单位的enddate和startdate之间的差值

    参数类型：datepart为指定的日期单位，详情参见下表。startdate和enddate为timestamp类型

    返回值类型：int

    datepart类型：

    | *datepart*  | **缩写形式** |
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

    示例：

    ```
    openGauss=# select datediff(day, timestamp'1997-12-31 23:59:59', timestamp'1998-12-31 23:59:59');
     datediff 
    ----------
          365
    (1 row)
    ```

- datediff_big(datepart, startdate, enddate)

    描述：返回指定datepart单位的enddate和startdate之间的差值

    参数类型：datepart为指定的日期单位，同datediff。startdate和enddate为timestamp类型

    返回值类型：bigint

    示例

    ```
    openGauss=# select datediff_big(second, timestamp'1997-12-31 23:59:59', timestamp'1998-12-31 23:59:59');
     datediff_big 
    --------------
         31536000
    (1 row)
    ```

- cast(expression AS data_type[(length)])

    描述：将表达式转换为指定类型

    参数类型：expression为任意类型表达式，data_type为类型关键字，length为int类型

    返回值类型：指定的data_type类型

    备注：
    - D库中，length默认值为30，该默认值主要适用于字符串相关类型，目前D兼容模式下`char`和`varchar`及其别名类型都适用该length默认值

    示例：

    ```
    openGauss=# select cast(123456789 AS char) as result;
             result             
    --------------------------------
     123456789                     
    (1 row)
    ```

- try_cast(expression AS data_type[(length)])

    描述：将表达式转换为指定类型，如进行不支持的类型转换则报错, 支持的类型转换但是转换失败情况下返回NULL

    参数类型：expression为任意类型表达式，data_type为类型关键字，length为int类型

    返回值类型：指定的data_type类型

    备注：
    - D库中，length默认值为30，该默认值主要适用于字符串相关类型，目前D兼容模式下`char`和`varchar`及其别名类型都适用该length默认值

    ```
    openGauss=# select try_cast(123456789 AS smallint) as result;
     result 
    --------
            
    (1 row)
    ```

- convert(data_type[(length)], expression[, style])

    描述：将表达式转换为指定类型

    参数类型：expression为任意类型表达式，data_type为类型关键字，length为int类型

    返回值类型：指定的data_type类型

    备注：
    - 针对不容的类型转换，style可以具有下表所表示的某个值，其他值作为0进行处理
    - 目前仅支持涉及日期，时间和浮点数，货币(money)的样式

    >[!NOTE]说明
    >使用`cast`以及`convert`时，涉及输出包含字符月份/星期的情况，请确保当前数据库的`lc_time`同系统一致，可以使用`show lc_time`查看当前数据库`lc_time`，`set lc_time`来修改当前数据库`lc_time`。可以使用命令`locale`查看当前操作系统的`lang`以及`locale`信息，使用`locale -a`列出当前操作系统所支持的`locale`。

    **表1** 日期和时间样式

    <table aria-label="表 1" class="table table-sm margin-top-none">
        <thead>
            <tr>
                <th>不带世纪位数</th>
                <th>带世纪位数</th>
                <th>标准</th>
                <th>输入/输出</th>
            </tr>
        </thead>
        <tbody>
            <tr>
                <td>-</td>
                <td>0或100</td>
                <td>默认值</td>
                <td>mon dd yyyy hh:miAM</td>
            </tr>
            <tr>
                <td>1</td>
                <td>101</td>
                <td>美国</td>
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
                <td>英国/法国</td>
                <td>3 = dd/mm/yy<br>103 = dd/mm/yyyy</td>
            </tr>
            <tr>
                <td>4</td>
                <td>104</td>
                <td>德国</td>
                <td>4 = dd.mm.yy<br>104 = dd.mm.yyyy</td>
            </tr>
            <tr>
                <td>5</td>
                <td>105</td>
                <td>意大利</td>
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
                <td>8或24</td>
                <td>108</td>
                <td>-</td>
                <td>hh:mi:ss</td>
            </tr>
            <tr>
                <td>-</td>
                <td>9或109</td>
                <td>默认格式 + 毫秒</td>
                <td>9 = mon dd yyyy<br>109 = hh:mi:ss:mmmAM(PM)</td>
            </tr>
            <tr>
                <td>10</td>
                <td>110</td>
                <td>美国</td>
                <td>11 = yy/mm/dd<br>111 = yyyy/mm/dd</td>
            </tr>
            <tr>
                <td>11</td>
                <td>111</td>
                <td>日本</td>
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
                <td>13或113</td>
                <td>欧洲默认格式 + 毫秒</td>
                <td>dd mon yyyy hh:mi:ss:mmm(24小时制)</td>
            </tr>
            <tr>
                <td>14</td>
                <td>114</td>
                <td>-</td>
                <td>hh:mi:ss:mmm(24小时制)</td>
            </tr>
            <tr>
                <td>-</td>
                <td>20或120</td>
                <td>ODBC规范</td>
                <td>yyyy-mm-dd hh:mi:ss</td>
            </tr>
            <tr>
                <td>-</td>
                <td>21或25或121</td>
                <td>time、date、datetime2和<br>datetimeoffset的ODBC规范(毫秒标识)默认值</td>
                <td>yyyy-mm-dd hh:mi:ss.mmm(24小时制)</td>
            </tr>
            <tr>
                <td>22</td>
                <td>-</td>
                <td>美国</td>
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
                <td>包括时区Z的ISO8601</td>
                <td>yyy-MM-ddThh:mm:ss.fffZ</td>
            </tr>
            <tr>
                <td>-</td>
                <td>130</td>
                <td>回历</td>
                <td>dd mon yyyy<br>hh:mi:ss:mmmAM</td>
            </tr>
            <tr>
                <td>-</td>
                <td>131</td>
                <td>回历</td>
                <td>dd/mm/yyyy<br>hi:mi:ss:mmmAM</td>
            </tr>
        </tbody>
    </table>

    **表2** float和real样式

    <table aria-label="表 2" class="table table-sm margin-top-none">
        <thead>
            <tr>
                <th>值</th>
                <th>输出</th>
            </tr>
        </thead>
        <tbody>
            <tr>
                <td>0</td>
                <td>最多包含6位，根据需要使用科学计数法。</td>
            </tr>
            <tr>
                <td>1</td>
                <td>始终为8位值，根据需要使用科学计数法。</td>
            </tr>
            <tr>
                <td>2</td>
                <td>始终为16位值，根据需要使用科学计数法。</td>
            </tr>
            <tr>
                <td>3</td>
                <td>始终为17位值，用于无损转换。</td>
            </tr>
        </tbody>
    </table>

    **表3** money样式

    <table aria-label="表 3" class="table table-sm margin-top-none">
        <thead>
            <tr>
                <th>值</th>
                <th>输出</th>
            </tr>
        </thead>
        <tbody>
            <tr>
                <td>0</td>
                <td>小数点左侧每三位数字之间不以逗号分隔，小数点右侧取两位数。</td>
            </tr>
            <tr>
                <td>1</td>
                <td>小数点左侧每三位之间以逗号分隔，小数点右侧取两位数。</td>
            </tr>
            <tr>
                <td>2</td>
                <td>小数点左侧每三位数字之间不以逗号分隔，小数点右侧取四位数。</td>
            </tr>
            <tr>
                <td>126</td>
                <td>转换为char(n)或varchar(n)时，等同于样式2。</td>
            </tr>
        </tbody>
    </table>

    示例：

    ```
    openGauss=# select convert(varchar, timestamp'2012-03-23 00:12:23', 1) as result;
      result  
    ----------
     03/23/12
    (1 row)
    ```

- try_convert(data_type[(length)], expression[, style])

    描述：将表达式转换为指定类型，如进行不支持的类型转换则报错, 支持的类型转换但是转换失败情况下返回NULL

    参数类型：expression为任意类型表达式，data_type为类型关键字，length为int类型

    返回值类型：指定的data_type类型

    示例：
    
    ```
    openGauss=# select try_convert(smallint, 123456789) as result;
     result 
    --------
       
    (1 row)
    ```

- newid()

    描述：生成一个全局唯一标识符

    参数类型：无

    返回值类型：uuid

    说明：基于uuid v1（时间+MAC地址）版本生成一个全局唯一标识符，同openGauss B兼容库dolphin插件uuid()函数实现，差异在于返回值类型，dolphin插件的uuid()函数返回值为varchar，shark插件的newid()函数返回值为uuid

    示例：
    
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

    描述：根据object_id返回object的名称，database_id为可选参数，可以不传或者传入当前数据库的oid，否则固定返回NULL。

    参数类型：object_id和database_id均为int类型的参数

    返回值类型：nvarchar

    说明：如果object为表，触发器或者约束，需要有对象的select权限。如果object为存储过程或者函数，需要有execute权限。如果object为类型，需要有uasge权限。

    示例：
    
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

    描述：根据object_id返回object的schema名称，database_id为可选参数，可以不传或者传入当前数据库的oid，否则固定返回NULL。

    参数类型：object_id和database_id均为int类型的参数。

    返回值类型：nvarchar

    说明：如果object为表，触发器或者约束，需要有对象的select权限。如果object为存储过程或者函数，需要有execute权限。如果object为类型，需要有uasge权限。

    示例：
    
    ```sql
    openGauss=# select object_schema_name(object_id('students'));
     object_schema_name
    --------------------
     public
    (1 row)
    ```

- object_definition(object_id int)

    描述：根据object_id返回object的定义。只支持获取视图，检查约束，函数，触发器的定义。

    参数类型：int

    返回值类型：nvarchar

    说明：如果object为表，触发器或者约束，需要有对象的select权限。如果object为存储过程或者函数，需要有execute权限。如果object为类型，需要有uasge权限。

    示例：
    
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

    描述：根据object_id获取输入属性的属性信息。

    参数类型：id为int类型，property为varchar类型。

    返回值类型：sql_variant

    说明：property当前只支持"basetype"，其他的属性结果与objectproperty函数一致。

    示例：
    
    ```sql
    openGauss=# select objectpropertyex(object_id('students'), 'BaseType');
     objectpropertyex
    ------------------
     U
    (1 row)
    ```

- col_length(object_name text, column_name text)

    描述：根据object_name和column_name返回指定列的类型的长度。

    参数类型：object_name和column_name均为text类型。

    返回值类型：smallint

    示例：
    
    ```sql
    openGauss=# SELECT COL_LENGTH('students', 'age');
     col_length
    ------------
              4
    (1 row)
    ```

- col_name(object_id int, column_id int)

    描述：根据object_id和column_id获取到column_id指定的列名。

    参数类型：object_id和column_id均为int类型。

    返回值类型：text

    示例：
    
    ```sql
    openGauss=# select col_name(object_id('students'), 1);
     col_name
    ----------
     id
    (1 row)
    ```

- columnproperty(object_id int, column_name text, property_name text)

    描述：根据object_id和column_name获取到指定的property指定的属性信息。

    参数类型：object_id为int类型，column_name和property_name均为text类型。

    返回值类型：int

    说明：property_name当前只支持"charmaxlen"、"allowsnull"、"iscomputed"、"columnid"、"ishidden"、"isidentity"、"ordinal"、"precision"、 "scale"。

    示例：
    
    ```sql
    openGauss=# SELECT sys.columnproperty(OBJECT_ID('students'), 'name', 'charmaxlen');
     columnproperty
    ----------------
                100
    (1 row)
    ```

- year(input ANYELEMENT)

    描述：返回一个日期类型的年份信息。

    参数类型：任意类型

    返回值类型：int

    示例：
    
    ```sql
    openGauss=# SELECT YEAR('20251010');
     year
    ------
     2025
    (1 row)
    ```

- month(input ANYELEMENT)

    描述：返回一个日期类型的月份信息。

    参数类型：任意类型

    返回值类型：int

    示例：
    
    ```sql
    openGauss=# SELECT MONTH('20251010');
     month
    -------
        10
    (1 row)
    ```

- day(input ANYELEMENT)

    描述：返回一个日期类型的天信息。

    参数类型：任意类型

    返回值类型：int

    示例：
    
    ```sql
    openGauss=# SELECT day('20251010');
     month
    -------
        10
    (1 row)
    ```

- isdate(input text)

    描述：返回输入的字符串是否为合法的datetime或者date或者time类型。

    参数类型：text

    返回值类型：int

    说明：对于毫秒部分精度大于3的日期，返回false。

    示例：
    
    ```sql
    openGauss=# SELECT ISDATE('2023-10-05 14:30:00');
     isdate
    --------
          1
    (1 row)
    ```

- eomonth(start_date date, month_to_add int DEFAULT 0)

    描述：返回指定日期所在月份的最后一天，可以指定偏移。

    参数类型：start_date为date类型，month_to_add为int类型。

    返回值类型：date

    示例：
    
    ```sql
    openGauss=# SELECT EOMONTH('2023-11-10', 2);
      eomonth
    ------------
     2024-01-31
    (1 row)
    ```

- sysdatetime()

    描述：返回当前时间。

    返回值类型：timestamptz

    示例：
    
    ```sql
    openGauss=# select sysdatetime();
              sysdatetime
    -------------------------------
     2025-12-29 14:19:58.594762+08
    (1 row)
    ```

- square(num float8)

    描述：返回入参数字的平方。

    参数类型：float8

    返回值类型：float8

    示例：
    
    ```sql
    openGauss=# select square(2.4);
     square
    --------
       5.76
    (1 row)
    ```

- isnumeric(expr ANYELEMENT)

    描述：判断输入的字符串是否是有效的数字或者money类型。

    返回值类型：int

    示例：
    
    ```sql
    openGauss=# select isnumeric('123456');
     isnumeric
    -----------
             1
    (1 row)
    ```

- patindex(pattern varchar, expression varchar)

    描述：返回输入字符串中匹配正则表达式匹配项的起始位置。

    参数类型：pattern和expression均为varchar类型。

    返回值类型：bigint

    说明：patindex基于substring函数进行模式匹配，因此pattern支持的范围与substring一致。

    示例：
    
    ```sql
    openGauss=# SELECT PATINDEX('%abc%', 'xyzabc123');
     patindex
    ----------
            4
    (1 row)
    ```

- stuff(character_expression varchar, start int, length int, replace_with_expression varchar)

    描述：从character_expression的start位置删除length长度的字符，然后将replace_with_expression插入到第一个字符串的开始位置。

    参数类型：character_expression和replace_with_expression均为varchar类型，start和length均为int类型。

    返回值类型：varchar

    示例：
    
    ```sql
    openGauss=# SELECT STUFF('abcdefg', 2, 3, 'XYZ');
      stuff
    ---------
     aXYZefg
    (1 row)
    ```

- str(float_expression numeric, length int, decimal int)

    描述：返回由数字数据转换而来的字符数据，支持指定长度和10进制精度。

    参数类型：float_expression为numeric类型，length和decimal为int类型。

    返回值类型：varchar

    示例：
    
    ```sql
    openGauss=# SELECT STR(123.45, 6, 1);
      str
    --------
      123.5
    (1 row)
    ```

- replicate(string_expression text, integer_expression int)

    描述：将一个字符串重复指定的次数。

    参数类型：string_expression为text类型，integer_expression为int类型。

    返回值类型：varchar

    示例：
    
    ```sql
    openGauss=# SELECT REPLICATE('abc', 2);
     replicate
    -----------
     abcabc
    (1 row)
    ```

- string_split(string_expression varchar, delimiter char)

    描述：根据指定的分隔符将字符串拆分为子字符串行。

    参数类型：string_expression为varchar类型，delimiter为char类型。

    返回值类型：setof

    示例：
    
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

    描述：使用quote_character包裹string_expression，quote_character默认为"[]"。

    参数类型：string_expression为varchar类型，quote_character为char类型。

    返回值类型：varchar

    示例：
    
    ```sql
    openGauss=# SELECT quotename('abcd', ']');
     quotename
    -----------
     [abcd]
    (1 row)
    ```

- trim([characters varchar FROM ] string_expression)

    描述：去掉字符串首尾的空格或者其他的指定字符。

    参数类型：string_expression为varchar类型，characters为varchar类型。

    返回值类型：varchar

    示例：
    
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

    描述：返回sql_variant的属性信息。

    参数类型：sql_variant_expression为sql_variant类型，property为varchar类型。

    返回值类型：sql_variant

    说明：property_name当前只支持"basetype"、"precision"、"scale"、"totalbytes"、"maxlength"，其他属性返回空。

    示例：
    
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
