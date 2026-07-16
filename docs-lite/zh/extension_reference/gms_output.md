# gms_output

## gms_output概述

gms_output是一个基于openGauss的插件，为用户提供了将文本行写入内存、供以后提取和显示的功能。目前支持的接口有：GMS_OUTPUT.ENABLE、GMS_OUTPUT.GET_LINE、GMS_OUTPUT.GET_LINES、GMS_OUTPUT.NEW_LINE、GMS_OUTPUT.PUT、GMS_OUTPUT.PUT_LINE、GMS_OUTPUT.DISABLE。

## gms_output限制

- 仅支持Create extension命令方式加载插件。
- gms_output仅支持A模式下使用。

## gms_output安装

openGauss打包编译时默认已经包含了gms_output, 可以在安装完openGauss后，直接通过create extension gms_output;加载插件。

## gms_output使用

### 创建Extension<a name="section21088306113"></a>

创建gms_output Extension可直接使用CREATE Extension命令进行创建：

```
openGauss=# CREATE Extension gms_output;
```

### 使用Extension<a name="section107391050141118"></a>

#### 函数声明

- ENABLE(buffer_size IN INTEGER DEFAULT 20000)
  描述：预先申请空间，使用GMS_OUTPUT之前，必须执行GMS_OUTPUT.ENABLE。
  参数详解：设置预申请空间的buff_size，最大值1000000，最小值2000，默认值为20000，单位为字节。
- DISABLE()
  描述：销毁申请的空间。
- GET_LINE(line INOUT text, status INOUT INTEGER)
  描述：该函数从缓冲区中检索行数组，并读取一行信息，其中行数据结束标志由’\0’来区分。
  参数详解：line：用于接收返回的行信息；status：若获取成功，该参数返回0，否则返回1。
- GET_LINES(lines INOUT text[], numlines INOUT INTEGER)
  描述：该函数从缓冲区中检索行数组，读取指定行数的文本信息。
  参数详解：lines：用于接收返回的行信息；numlines表示真正获取到的文本数据的行数。
  > 注意：GMS_OUTPUT.GET_LINE和GMS_OUTPUT.GET_LINES之后，缓冲区清空，获取的数据为空。
- PUT(item IN VARCHAR2)
  描述：该函数输出部分行到缓冲区。
  参数详解：item：表示向缓冲区写入的内容。
- PUT_LINE(item IN VARCHAR2)
  描述：该函数输出一行信息到缓冲区。
  参数详解：item：表示向缓冲区写入的内容。
- NEW_LINE()
  描述：向行缓冲区中放置一个行结束标记。

#### 函数使用

测试enable、disable函数

```sql
openGauss=# create schema gms_output_test;
CREATE SCHEMA
openGauss=# set search_path=gms_output_test;
SET
openGauss=# select gms_output.disable;
 disable 
---------
 
(1 row)

openGauss=# select gms_output.enable(20000);
 enable 
--------
 
(1 row)

```

测试get_line、get_lines函数

```sql
openGauss=# begin
openGauss$#   gms_output.enable;
openGauss$#   gms_output.put_line('This ');
openGauss$# end;
openGauss$# /
This
ANONYMOUS BLOCK EXECUTE
openGauss=# select gms_output.get_line(0,1);
  get_line
-------------
 ("This ",0)
(1 row)

openGauss=# begin
openGauss$#  gms_output.enable(100);
openGauss$#  gms_output.PUT_LINE('{131231321312313},{dhsfsdjfsdf}');
openGauss$#  gms_output.PUT_LINE('{好还是打发士大夫},{dhsfsdjfsdf}');
openGauss$# end;
openGauss$# /
WARNING:  Limit increased to 2000 bytes.
CONTEXT:  SQL statement "CALL gms_output.enable(100)"
PL/pgSQL function inline_code_block line 2 at PERFORM
{131231321312313},{dhsfsdjfsdf}
{好还是打发士大夫},{dhsfsdjfsdf}
ANONYMOUS BLOCK EXECUTE
openGauss=# select  gms_output.get_lines('{lines}',3);
                                    get_lines
----------------------------------------------------------------------------------
 ("{""{131231321312313},{dhsfsdjfsdf}"",""{好还是打发士大夫},{dhsfsdjfsdf}""}",2)
(1 row)

```

测试put、put_line函数

```sql
openGauss=# begin
openGauss$#  gms_output.enable(4000);
openGauss$#  gms_output.put('123');
openGauss$#  gms_output.put_line('YYY');
openGauss$# end;
openGauss$# /
123
YYY
ANONYMOUS BLOCK EXECUTE
```

测试new_line函数

```sql
openGauss=# begin
openGauss$#  gms_output.put('44');
openGauss$#  gms_output.new_line();
openGauss$# end;
openGauss$# /
44
ANONYMOUS BLOCK EXECUTE
```

### 删除Extension<a name="section1587441381220"></a>

在openGauss中删除gms_output Extension的方法如下所示：

```
openGauss=# DROP Extension gms_output [CASCADE];
```

>[!NOTE]说明
>
>如果Extension被其它对象依赖，需要加入CASCADE（级联）关键字，删除所有依赖对象。
