# gms_xmlgen

## gms_xmlgen概述

gms_xmlgen是一个基于openGauss的插件，用于将sql查询结果转换成规范的xml格式。支持sql query字符串或者游标作为输入，并将结果以clob类型或者xmltype类型返回。目前支持的接口有：gms_xmlgen.closecontext、gms_xmlgen.getxmltype、gms_xmlgen.newcontextfromhierarchy、gms_xmlgen.convert、gms_xmlgen.getnumrowsprocessed、gms_xmlgen.getxml、gms_xmlgen.getxmltype、gms_xmlgen.newcontext、gms_xmlgen.restartquery、gms_xmlgen.setconvertspecialchars、gms_xmlgen.setmaxrows、gms_xmlgen.setnullhandling、gms_xmlgen.setrowsettag、gms_xmlgen.setrowtag、gms_xmlgen.setskiprows、gms_xmlgen.useitemtagsforcoll、gms_xmlgen.usenullattributeindicator。

## gms_xmlgen限制

- 仅支持在A模式数据库创建。
- 仅支持`create extension`命令方式加载插件。
- gms_xmlgen插件依赖libxml，而轻量版openGauss不支持libxml，故轻量版openGauss不支持该插件。
- gms_xmlgen插件依赖xmltype类型，该类型于7.0.0-RC1版本引入，对数据库进行升级或回滚操作时，须先删除该插件，否则回滚升级脚本中存在依赖，升级将失败报错。
- 涉及xmltype的接口需要设置GUC参数bind_procedure_searchpath。

## gms_xmlgen安装

openGauss打包编译时默认已经包含了gms_xmlgen，可以在安装完openGauss后，直接通过`create extension gms_xmlgen;`加载插件。

## gms_xmlgen使用

### 创建Extension<a name="section21088306113"></a>

创建gms_xmlgen extension可直接使用`create extension`命令进行创建：

```
openGauss=# create extension gms_xmlgen;
```

### 使用Extension<a name="section107391050141118"></a>
  
创建插件，借助gms_output插件显示结果，准备示例数据。涉及xmltype的接口需要设置GUC参数bind_procedure_searchpath。

```
openGauss=# create extension gms_xmlgen;
CREATE EXTENSION
openGauss=# create extension gms_output;
CREATE EXTENSION
openGauss=# select gms_output.enable(100000);
 enable 
--------
 
(1 row)

openGauss=# set behavior_compat_options = 'bind_procedure_searchpath';
SET
openGauss=# create table t_types (
openGauss(#     "integer" integer,
openGauss(#     "float" float,
openGauss(#     "numeric" numeric(20, 6),
openGauss(#     "boolean" boolean,
openGauss(#     "char" char(20),
openGauss(#     "varchar" varchar(20),
openGauss(#     "text" text,
openGauss(#     "blob" blob,
openGauss(#     "raw" raw,
openGauss(#     "date" date,
openGauss(#     "time" time,
openGauss(#     "timestamp" timestamp,
openGauss(#     "json" json,
openGauss(#     "varchar_array" varchar(20)[]
openGauss(# );
CREATE TABLE
openGauss=# insert into t_types
openGauss-# values(
openGauss(#         1,
openGauss(#         1.23456,
openGauss(#         1.234567,
openGauss(#         true,
openGauss(#         '"''<>&char test',
openGauss(#         'varchar"''<>&test',
openGauss(#         'text test"''<>&',
openGauss(#         'ff',
openGauss(#         hextoraw('ABCD'),
openGauss(#         '2024-01-02',
openGauss(#         '18:01:02',
openGauss(#         '2024-02-03 19:03:04',
openGauss(#         '{"a" : 1, "b" : 2}',
openGauss(#         array['abc', '"''<>&', '你好']
openGauss(#     ),
openGauss-#     (
openGauss(#         null,
openGauss(#         null,
openGauss(#         null,
openGauss(#         null,
openGauss(#         null,
openGauss(#         null,
openGauss(#         null,
openGauss(#         null,
openGauss(#         null,
openGauss(#         null,
openGauss(#         null,
openGauss(#         null,
openGauss(#         null,
openGauss(#         null
openGauss(#     );
INSERT 0 2
```

- gms_xmlgen.getxml(queryString in varchar2)
  
  通过sql query获取xml结果。
  
```
openGauss=# DECLARE
openGauss-# xml_output clob;
openGauss-# BEGIN
openGauss$# xml_output := gms_xmlgen.getxml('select * from t_types');
openGauss$# gms_output.put_line(xml_output);
openGauss$# END;
openGauss$# /
<?xml version="1.0"?>
<ROWSET>
  <ROW>
    <integer>1</integer>
    <float>1.23456</float>
    <numeric>1.234567</numeric>
    <boolean>true</boolean>
    <char>&quot;&apos;&lt;&gt;&amp;char test      </char>
    <varchar>varchar&quot;&apos;&lt;&gt;&amp;test</varchar>
    <text>text test&quot;&apos;&lt;&gt;&amp;</text>
    <blob>FF</blob>
    <raw>ABCD</raw>
    <date>2024-01-02T00:00:00</date>
    <time>18:01:02</time>
    <timestamp>2024-02-03T19:03:04</timestamp>
    <json>{&quot;a&quot; : 1, &quot;b&quot; : 2}</json>
    <varchar_array>
      <varchar>abc</varchar>
      <varchar>&quot;&apos;&lt;&gt;&amp;</varchar>
      <varchar>你好</varchar>
    </varchar_array>
  </ROW>
  <ROW>
  </ROW>
</ROWSET>

ANONYMOUS BLOCK EXECUTE
```

- gms_xmlgen.newcontext(queryString in varchar2)

  gms_xmlgen.getxml(ctx in gms_xmlgen.ctxhandle)
  
  通过创建gms_xmlgen context获取xml结果。
  
```
openGauss=# DECLARE
openGauss-# xml_output clob;
openGauss-# xml_cxt gms_xmlgen.ctxhandle;
openGauss-# BEGIN
openGauss$# xml_cxt := gms_xmlgen.newcontext('select * from t_types');
openGauss$# xml_output := gms_xmlgen.getxml(xml_cxt);
openGauss$# gms_output.put_line(xml_output);
openGauss$# gms_xmlgen.closecontext(xml_cxt);
openGauss$# END;
openGauss$# /
<?xml version="1.0"?>
<ROWSET>
  <ROW>
    <integer>1</integer>
    <float>1.23456</float>
    <numeric>1.234567</numeric>
    <boolean>true</boolean>
    <char>&quot;&apos;&lt;&gt;&amp;char test      </char>
    <varchar>varchar&quot;&apos;&lt;&gt;&amp;test</varchar>
    <text>text test&quot;&apos;&lt;&gt;&amp;</text>
    <blob>FF</blob>
    <raw>ABCD</raw>
    <date>2024-01-02T00:00:00</date>
    <time>18:01:02</time>
    <timestamp>2024-02-03T19:03:04</timestamp>
    <json>{&quot;a&quot; : 1, &quot;b&quot; : 2}</json>
    <varchar_array>
      <varchar>abc</varchar>
      <varchar>&quot;&apos;&lt;&gt;&amp;</varchar>
      <varchar>你好</varchar>
    </varchar_array>
  </ROW>
  <ROW>
  </ROW>
</ROWSET>

ANONYMOUS BLOCK EXECUTE
```

- gms_xmlgen.newcontext(queryString in sys_refcursor)
  
  gms_xmlgen.getxml(ctx in gms_xmlgen.ctxhandle)

  通过cursor获取xml结果。

```
openGauss=# DECLARE
openGauss-# cursor xc is select * from t_types;
openGauss-# xml_output clob;
openGauss-# xml_cxt gms_xmlgen.ctxhandle;
openGauss-# BEGIN
openGauss$# open xc;
openGauss$# xml_cxt := gms_xmlgen.newcontext(xc);
openGauss$# xml_output := gms_xmlgen.getxml(xml_cxt);
openGauss$# gms_output.put_line(xml_output);
openGauss$# close xc;
openGauss$# gms_xmlgen.closecontext(xml_cxt);
openGauss$# END;
openGauss$# /
<?xml version="1.0"?>
<ROWSET>
  <ROW>
    <integer>1</integer>
    <float>1.23456</float>
    <numeric>1.234567</numeric>
    <boolean>true</boolean>
    <char>&quot;&apos;&lt;&gt;&amp;char test      </char>
    <varchar>varchar&quot;&apos;&lt;&gt;&amp;test</varchar>
    <text>text test&quot;&apos;&lt;&gt;&amp;</text>
    <blob>FF</blob>
    <raw>ABCD</raw>
    <date>2024-01-02T00:00:00</date>
    <time>18:01:02</time>
    <timestamp>2024-02-03T19:03:04</timestamp>
    <json>{&quot;a&quot; : 1, &quot;b&quot; : 2}</json>
    <varchar_array>
      <varchar>abc</varchar>
      <varchar>&quot;&apos;&lt;&gt;&amp;</varchar>
      <varchar>你好</varchar>
    </varchar_array>
  </ROW>
  <ROW>
  </ROW>
</ROWSET>

ANONYMOUS BLOCK EXECUTE
```

- gms_xmlgen.restartquery(ctx in gms_xmlgen.ctxhandle)
  
  在gms_xmlgen context中通过getxml再次获取结果需要先执行restartquery。

```
openGauss=# DECLARE
openGauss-# xml_output clob;
openGauss-# xml_cxt gms_xmlgen.ctxhandle;
openGauss-# BEGIN
openGauss$# xml_cxt := gms_xmlgen.newcontext('select * from t_types');
openGauss$# xml_output := gms_xmlgen.getxml(xml_cxt);
openGauss$# gms_output.put_line(xml_output);
openGauss$# gms_xmlgen.restartquery(xml_cxt);
openGauss$# xml_output := gms_xmlgen.getxml(xml_cxt);
openGauss$# gms_output.put_line(xml_output);
openGauss$# gms_xmlgen.closecontext(xml_cxt);
openGauss$# END;
openGauss$# /
<?xml version="1.0"?>
<ROWSET>
  <ROW>
    <integer>1</integer>
    <float>1.23456</float>
    <numeric>1.234567</numeric>
    <boolean>true</boolean>
    <char>&quot;&apos;&lt;&gt;&amp;char test      </char>
    <varchar>varchar&quot;&apos;&lt;&gt;&amp;test</varchar>
    <text>text test&quot;&apos;&lt;&gt;&amp;</text>
    <blob>FF</blob>
    <raw>ABCD</raw>
    <date>2024-01-02T00:00:00</date>
    <time>18:01:02</time>
    <timestamp>2024-02-03T19:03:04</timestamp>
    <json>{&quot;a&quot; : 1, &quot;b&quot; : 2}</json>
    <varchar_array>
      <varchar>abc</varchar>
      <varchar>&quot;&apos;&lt;&gt;&amp;</varchar>
      <varchar>你好</varchar>
    </varchar_array>
  </ROW>
  <ROW>
  </ROW>
</ROWSET>

<?xml version="1.0"?>
<ROWSET>
  <ROW>
    <integer>1</integer>
    <float>1.23456</float>
    <numeric>1.234567</numeric>
    <boolean>true</boolean>
    <char>&quot;&apos;&lt;&gt;&amp;char test      </char>
    <varchar>varchar&quot;&apos;&lt;&gt;&amp;test</varchar>
    <text>text test&quot;&apos;&lt;&gt;&amp;</text>
    <blob>FF</blob>
    <raw>ABCD</raw>
    <date>2024-01-02T00:00:00</date>
    <time>18:01:02</time>
    <timestamp>2024-02-03T19:03:04</timestamp>
    <json>{&quot;a&quot; : 1, &quot;b&quot; : 2}</json>
    <varchar_array>
      <varchar>abc</varchar>
      <varchar>&quot;&apos;&lt;&gt;&amp;</varchar>
      <varchar>你好</varchar>
    </varchar_array>
  </ROW>
  <ROW>
  </ROW>
</ROWSET>

ANONYMOUS BLOCK EXECUTE
```

- gms_xmlgen.getxmltype(queryString in varchar2)
  
  获取xmltype类型的xml结果。

```
openGauss=# DECLARE
openGauss-# xml_cxt gms_xmlgen.ctxhandle;
openGauss-# xml_type xmltype;
openGauss-# BEGIN
openGauss$# xml_type := gms_xmlgen.getxmltype('select * from t_types');
openGauss$# gms_output.put_line(xml_type::text);
openGauss$# gms_xmlgen.closecontext(xml_cxt);
openGauss$# END;
openGauss$# /
<?xml version="1.0"?>
<ROWSET>
  <ROW>
    <integer>1</integer>
    <float>1.23456</float>
    <numeric>1.234567</numeric>
    <boolean>true</boolean>
    <char>&quot;&apos;&lt;&gt;&amp;char test      </char>
    <varchar>varchar&quot;&apos;&lt;&gt;&amp;test</varchar>
    <text>text test&quot;&apos;&lt;&gt;&amp;</text>
    <blob>FF</blob>
    <raw>ABCD</raw>
    <date>2024-01-02T00:00:00</date>
    <time>18:01:02</time>
    <timestamp>2024-02-03T19:03:04</timestamp>
    <json>{&quot;a&quot; : 1, &quot;b&quot; : 2}</json>
    <varchar_array>
      <varchar>abc</varchar>
      <varchar>&quot;&apos;&lt;&gt;&amp;</varchar>
      <varchar>你好</varchar>
    </varchar_array>
  </ROW>
  <ROW>
  </ROW>
</ROWSET>

ANONYMOUS BLOCK EXECUTE
```

- gms_xmlgen.getxmltype(ctx in gms_xmlgen.ctxhandle)

  通过gms_xmlgen context获取xmltype类型的xml结果。

```
openGauss=# DECLARE
openGauss-# xml_cxt gms_xmlgen.ctxhandle;
openGauss-# xml_type xmltype;
openGauss-# BEGIN
openGauss$# xml_cxt := gms_xmlgen.newcontext('select * from t_types');
openGauss$# xml_type := gms_xmlgen.getxmltype(xml_cxt);
openGauss$# gms_output.put_line(xml_type::text);
openGauss$# gms_xmlgen.closecontext(xml_cxt);
openGauss$# END;
openGauss$# /
<?xml version="1.0"?>
<ROWSET>
  <ROW>
    <integer>1</integer>
    <float>1.23456</float>
    <numeric>1.234567</numeric>
    <boolean>true</boolean>
    <char>&quot;&apos;&lt;&gt;&amp;char test      </char>
    <varchar>varchar&quot;&apos;&lt;&gt;&amp;test</varchar>
    <text>text test&quot;&apos;&lt;&gt;&amp;</text>
    <blob>FF</blob>
    <raw>ABCD</raw>
    <date>2024-01-02T00:00:00</date>
    <time>18:01:02</time>
    <timestamp>2024-02-03T19:03:04</timestamp>
    <json>{&quot;a&quot; : 1, &quot;b&quot; : 2}</json>
    <varchar_array>
      <varchar>abc</varchar>
      <varchar>&quot;&apos;&lt;&gt;&amp;</varchar>
      <varchar>你好</varchar>
    </varchar_array>
  </ROW>
  <ROW>
  </ROW>
</ROWSET>

ANONYMOUS BLOCK EXECUTE
```

- gms_xmlgen.newcontextfromhierarchy(queryString in varchar2)
  
  获取分层查询的xml结果，结果第1列必须是int或者number类型，第2列必须是xml或者xmltype类型。QueryString必须是包含CONNECT BY子句的分层查询。

```
openGauss=# DECLARE
openGauss-# xml_output clob;
openGauss-# xml_cxt_from_hierarchy gms_xmlgen.ctxhandle;
openGauss-# BEGIN
openGauss$# xml_cxt_from_hierarchy := gms_xmlgen.newcontextfromhierarchy('
openGauss$# SELECT "integer", xmltype(gms_xmlgen.getxml(''select * from t_types''))
openGauss$# FROM t_types
openGauss$# START WITH "integer" = 1 OR "integer" = 2
openGauss$# CONNECT BY nocycle "integer" = PRIOR "integer"');
openGauss$# xml_output := gms_xmlgen.getxml(xml_cxt_from_hierarchy);
openGauss$# gms_output.put_line(xml_output);
openGauss$# gms_xmlgen.closecontext(xml_cxt_from_hierarchy);
openGauss$# END;
openGauss$# /
<?xml version="1.0" encoding="utf-8"?>
<ROWSET>
  <ROW>
    <integer>1</integer>
    <float>1.23456</float>
    <numeric>1.234567</numeric>
    <boolean>true</boolean>
    <char>"'&lt;&gt;&amp;char test      </char>
    <varchar>varchar"'&lt;&gt;&amp;test</varchar>
    <text>text test"'&lt;&gt;&amp;</text>
    <blob>FF</blob>
    <raw>ABCD</raw>
    <date>2024-01-02T00:00:00</date>
    <time>18:01:02</time>
    <timestamp>2024-02-03T19:03:04</timestamp>
    <json>{"a" : 1, "b" : 2}</json>
    <varchar_array>
      <varchar>abc</varchar>
      <varchar>"'&lt;&gt;&amp;</varchar>
      <varchar>你好</varchar>
    </varchar_array>
  </ROW>
  <ROW>
  </ROW>
</ROWSET>

ANONYMOUS BLOCK EXECUTE
```

- gms_xmlgen.setconvertspecialchars(ctx in gms_xmlgen.ctxhandle, flag in BOOLEAN)
  
  设置是否对上下文数据中的特殊符号&、<、>、"、'进行转义，false不转义，true转义，NULL等同于false，默认true。

```
openGauss=# DECLARE
openGauss-# xml_output clob;
openGauss-# xml_cxt gms_xmlgen.ctxhandle;
openGauss-# BEGIN
openGauss$# xml_cxt := gms_xmlgen.newcontext('select * from t_types');
openGauss$# gms_xmlgen.setconvertspecialchars(xml_cxt, false);
openGauss$# xml_output := gms_xmlgen.getxml(xml_cxt);
openGauss$# gms_output.put_line(xml_output);
openGauss$# gms_xmlgen.setconvertspecialchars(xml_cxt, true);
openGauss$# gms_xmlgen.restartquery(xml_cxt);
openGauss$# xml_output := gms_xmlgen.getxml(xml_cxt);
openGauss$# gms_output.put_line(xml_output);
openGauss$# gms_xmlgen.closecontext(xml_cxt);
openGauss$# END;
openGauss$# /
<?xml version="1.0"?>
<ROWSET>
  <ROW>
    <integer>1</integer>
    <float>1.23456</float>
    <numeric>1.234567</numeric>
    <boolean>true</boolean>
    <char>"'<>&char test      </char>
    <varchar>varchar"'<>&test</varchar>
    <text>text test"'<>&</text>
    <blob>FF</blob>
    <raw>ABCD</raw>
    <date>2024-01-02T00:00:00</date>
    <time>18:01:02</time>
    <timestamp>2024-02-03T19:03:04</timestamp>
    <json>{"a" : 1, "b" : 2}</json>
    <varchar_array>
      <varchar>abc</varchar>
      <varchar>"'<>&</varchar>
      <varchar>你好</varchar>
    </varchar_array>
  </ROW>
  <ROW>
  </ROW>
</ROWSET>

<?xml version="1.0"?>
<ROWSET>
  <ROW>
    <integer>1</integer>
    <float>1.23456</float>
    <numeric>1.234567</numeric>
    <boolean>true</boolean>
    <char>&quot;&apos;&lt;&gt;&amp;char test      </char>
    <varchar>varchar&quot;&apos;&lt;&gt;&amp;test</varchar>
    <text>text test&quot;&apos;&lt;&gt;&amp;</text>
    <blob>FF</blob>
    <raw>ABCD</raw>
    <date>2024-01-02T00:00:00</date>
    <time>18:01:02</time>
    <timestamp>2024-02-03T19:03:04</timestamp>
    <json>{&quot;a&quot; : 1, &quot;b&quot; : 2}</json>
    <varchar_array>
      <varchar>abc</varchar>
      <varchar>&quot;&apos;&lt;&gt;&amp;</varchar>
      <varchar>你好</varchar>
    </varchar_array>
  </ROW>
  <ROW>
  </ROW>
</ROWSET>

ANONYMOUS BLOCK EXECUTE
```

- gms_xmlgen.convert(string in varchar2)

  对特殊符号&、<、>、"、'进行转义或者反转义，默认转义。

```
openGauss=# select GMS_XMLGEN.CONVERT('"''<>&');
          convert          
---------------------------
 &quot;&apos;&lt;&gt;&amp;
(1 row)

openGauss=# select GMS_XMLGEN.CONVERT('"''<>&', 0);
          convert          
---------------------------
 &quot;&apos;&lt;&gt;&amp;
(1 row)

openGauss=# select GMS_XMLGEN.CONVERT('"''<>&', 1);
 convert 
---------
 "'<>&
(1 row)

openGauss=# select GMS_XMLGEN.CONVERT('&quot;&apos;&lt;&gt;&amp;');
                    convert                    
-----------------------------------------------
 &amp;quot;&amp;apos;&amp;lt;&amp;gt;&amp;amp;
(1 row)

openGauss=# select GMS_XMLGEN.CONVERT('&quot;&apos;&lt;&gt;&amp;', 0);
                    convert                    
-----------------------------------------------
 &amp;quot;&amp;apos;&amp;lt;&amp;gt;&amp;amp;
(1 row)

openGauss=# select GMS_XMLGEN.CONVERT('&quot;&apos;&lt;&gt;&amp;', 1);
 convert 
---------
 "'<>&
(1 row)

```

- gms_xmlgen.setmaxrows(ctx in gms_xmlgen.ctxhandle, maxrows in NUMBER)
  
  设置转换的最大行数。

```
openGauss=# DECLARE
openGauss-# xml_output clob;
openGauss-# xml_cxt gms_xmlgen.ctxhandle;
openGauss-# BEGIN
openGauss$# xml_cxt := gms_xmlgen.newcontext('select * from t_types');
openGauss$# gms_xmlgen.setmaxrows(xml_cxt, 1);
openGauss$# xml_output := gms_xmlgen.getxml(xml_cxt);
openGauss$# gms_output.put_line(xml_output);
openGauss$# gms_xmlgen.closecontext(xml_cxt);
openGauss$# END;
openGauss$# /
<?xml version="1.0"?>
<ROWSET>
  <ROW>
    <integer>1</integer>
    <float>1.23456</float>
    <numeric>1.234567</numeric>
    <boolean>true</boolean>
    <char>&quot;&apos;&lt;&gt;&amp;char test      </char>
    <varchar>varchar&quot;&apos;&lt;&gt;&amp;test</varchar>
    <text>text test&quot;&apos;&lt;&gt;&amp;</text>
    <blob>FF</blob>
    <raw>ABCD</raw>
    <date>2024-01-02T00:00:00</date>
    <time>18:01:02</time>
    <timestamp>2024-02-03T19:03:04</timestamp>
    <json>{&quot;a&quot; : 1, &quot;b&quot; : 2}</json>
    <varchar_array>
      <varchar>abc</varchar>
      <varchar>&quot;&apos;&lt;&gt;&amp;</varchar>
      <varchar>你好</varchar>
    </varchar_array>
  </ROW>
</ROWSET>

ANONYMOUS BLOCK EXECUTE
```

- gms_xmlgen.setskiprows(ctx in gms_xmlgen.ctxhandle, skiprows in NUMBER)
  
  设置从开头跳过的行数。

```
openGauss=# DECLARE
openGauss-# xml_output clob;
openGauss-# xml_cxt gms_xmlgen.ctxhandle;
openGauss-# BEGIN
openGauss$# xml_cxt := gms_xmlgen.newcontext('select * from t_types');
openGauss$# gms_xmlgen.setskiprows(xml_cxt, 0);
openGauss$# xml_output := gms_xmlgen.getxml(xml_cxt);
openGauss$# gms_output.put_line(xml_output);
openGauss$# gms_xmlgen.closecontext(xml_cxt);
openGauss$# END;
openGauss$# /
<?xml version="1.0"?>
<ROWSET>
  <ROW>
    <integer>1</integer>
    <float>1.23456</float>
    <numeric>1.234567</numeric>
    <boolean>true</boolean>
    <char>&quot;&apos;&lt;&gt;&amp;char test      </char>
    <varchar>varchar&quot;&apos;&lt;&gt;&amp;test</varchar>
    <text>text test&quot;&apos;&lt;&gt;&amp;</text>
    <blob>FF</blob>
    <raw>ABCD</raw>
    <date>2024-01-02T00:00:00</date>
    <time>18:01:02</time>
    <timestamp>2024-02-03T19:03:04</timestamp>
    <json>{&quot;a&quot; : 1, &quot;b&quot; : 2}</json>
    <varchar_array>
      <varchar>abc</varchar>
      <varchar>&quot;&apos;&lt;&gt;&amp;</varchar>
      <varchar>你好</varchar>
    </varchar_array>
  </ROW>
  <ROW>
  </ROW>
</ROWSET>

ANONYMOUS BLOCK EXECUTE
```

- gms_xmlgen.setrowsettag(ctx in gms_xmlgen.ctxhandle, rowsettagname in VARCAHR2)
  
  设置数据集合的标签名。

```
openGauss=# DECLARE
openGauss-# xml_output clob;
openGauss-# xml_cxt gms_xmlgen.ctxhandle;
openGauss-# BEGIN
openGauss$# xml_cxt := gms_xmlgen.newcontext('select * from t_types');
openGauss$# gms_xmlgen.setrowsettag(xml_cxt, 'test');
openGauss$# xml_output := gms_xmlgen.getxml(xml_cxt);
openGauss$# gms_output.put_line(xml_output);
openGauss$# gms_xmlgen.closecontext(xml_cxt);
openGauss$# END;
openGauss$# /
<?xml version="1.0"?>
<test>
  <ROW>
    <integer>1</integer>
    <float>1.23456</float>
    <numeric>1.234567</numeric>
    <boolean>true</boolean>
    <char>&quot;&apos;&lt;&gt;&amp;char test      </char>
    <varchar>varchar&quot;&apos;&lt;&gt;&amp;test</varchar>
    <text>text test&quot;&apos;&lt;&gt;&amp;</text>
    <blob>FF</blob>
    <raw>ABCD</raw>
    <date>2024-01-02T00:00:00</date>
    <time>18:01:02</time>
    <timestamp>2024-02-03T19:03:04</timestamp>
    <json>{&quot;a&quot; : 1, &quot;b&quot; : 2}</json>
    <varchar_array>
      <varchar>abc</varchar>
      <varchar>&quot;&apos;&lt;&gt;&amp;</varchar>
      <varchar>你好</varchar>
    </varchar_array>
  </ROW>
  <ROW>
  </ROW>
</test>

ANONYMOUS BLOCK EXECUTE
```

- gms_xmlgen.setrowtag(ctx in gms_xmlgen.ctxhandle, rowtagname in varchar2)
  
  设置每行数据的标签名。

```
openGauss=# DECLARE
openGauss-# xml_output clob;
openGauss-# xml_cxt gms_xmlgen.ctxhandle;
openGauss-# BEGIN
openGauss$# xml_cxt := gms_xmlgen.newcontext('select * from t_types');
openGauss$# gms_xmlgen.setrowtag(xml_cxt, 'test');
openGauss$# xml_output := gms_xmlgen.getxml(xml_cxt);
openGauss$# gms_output.put_line(xml_output);
openGauss$# gms_xmlgen.closecontext(xml_cxt);
openGauss$# END;
openGauss$# /
<?xml version="1.0"?>
<ROWSET>
  <test>
    <integer>1</integer>
    <float>1.23456</float>
    <numeric>1.234567</numeric>
    <boolean>true</boolean>
    <char>&quot;&apos;&lt;&gt;&amp;char test      </char>
    <varchar>varchar&quot;&apos;&lt;&gt;&amp;test</varchar>
    <text>text test&quot;&apos;&lt;&gt;&amp;</text>
    <blob>FF</blob>
    <raw>ABCD</raw>
    <date>2024-01-02T00:00:00</date>
    <time>18:01:02</time>
    <timestamp>2024-02-03T19:03:04</timestamp>
    <json>{&quot;a&quot; : 1, &quot;b&quot; : 2}</json>
    <varchar_array>
      <varchar>abc</varchar>
      <varchar>&quot;&apos;&lt;&gt;&amp;</varchar>
      <varchar>你好</varchar>
    </varchar_array>
  </test>
  <test>
  </test>
</ROWSET>

ANONYMOUS BLOCK EXECUTE
```

- gms_xmlgen.SETNULLHANDLING(ctx in gms_xmlgen.ctxhandle, flag in NUMBER)
  
  设置空数据的表现方式，0是默认不显示，1是显示标签空属性，2是显示空标签。

```
openGauss=# DECLARE
openGauss-# xml_output clob;
openGauss-# xml_cxt gms_xmlgen.ctxhandle;
openGauss-# BEGIN
openGauss$# xml_cxt := gms_xmlgen.newcontext('select * from t_types');
openGauss$# gms_xmlgen.setnullhandling(xml_cxt, 0);
openGauss$# xml_output := gms_xmlgen.getxml(xml_cxt);
openGauss$# gms_output.put_line(xml_output);
openGauss$# gms_xmlgen.setnullhandling(xml_cxt, 1);
openGauss$# gms_xmlgen.restartquery(xml_cxt);
openGauss$# xml_output := gms_xmlgen.getxml(xml_cxt);
openGauss$# gms_output.put_line(xml_output);
openGauss$# gms_xmlgen.setnullhandling(xml_cxt, 2);
openGauss$# gms_xmlgen.restartquery(xml_cxt);
openGauss$# xml_output := gms_xmlgen.getxml(xml_cxt);
openGauss$# gms_output.put_line(xml_output);
openGauss$# gms_xmlgen.closecontext(xml_cxt);
openGauss$# END;
openGauss$# /
<?xml version="1.0"?>
<ROWSET>
  <ROW>
    <integer>1</integer>
    <float>1.23456</float>
    <numeric>1.234567</numeric>
    <boolean>true</boolean>
    <char>&quot;&apos;&lt;&gt;&amp;char test      </char>
    <varchar>varchar&quot;&apos;&lt;&gt;&amp;test</varchar>
    <text>text test&quot;&apos;&lt;&gt;&amp;</text>
    <blob>FF</blob>
    <raw>ABCD</raw>
    <date>2024-01-02T00:00:00</date>
    <time>18:01:02</time>
    <timestamp>2024-02-03T19:03:04</timestamp>
    <json>{&quot;a&quot; : 1, &quot;b&quot; : 2}</json>
    <varchar_array>
      <varchar>abc</varchar>
      <varchar>&quot;&apos;&lt;&gt;&amp;</varchar>
      <varchar>你好</varchar>
    </varchar_array>
  </ROW>
  <ROW>
  </ROW>
</ROWSET>

<?xml version="1.0"?>
<ROWSET xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <ROW>
    <integer>1</integer>
    <float>1.23456</float>
    <numeric>1.234567</numeric>
    <boolean>true</boolean>
    <char>&quot;&apos;&lt;&gt;&amp;char test      </char>
    <varchar>varchar&quot;&apos;&lt;&gt;&amp;test</varchar>
    <text>text test&quot;&apos;&lt;&gt;&amp;</text>
    <blob>FF</blob>
    <raw>ABCD</raw>
    <date>2024-01-02T00:00:00</date>
    <time>18:01:02</time>
    <timestamp>2024-02-03T19:03:04</timestamp>
    <json>{&quot;a&quot; : 1, &quot;b&quot; : 2}</json>
    <varchar_array>
      <varchar>abc</varchar>
      <varchar>&quot;&apos;&lt;&gt;&amp;</varchar>
      <varchar>你好</varchar>
    </varchar_array>
  </ROW>
  <ROW>
    <integer xsi:nil="true"/>
    <float xsi:nil="true"/>
    <numeric xsi:nil="true"/>
    <boolean xsi:nil="true"/>
    <char xsi:nil="true"/>
    <varchar xsi:nil="true"/>
    <text xsi:nil="true"/>
    <blob xsi:nil="true"/>
    <raw xsi:nil="true"/>
    <date xsi:nil="true"/>
    <time xsi:nil="true"/>
    <timestamp xsi:nil="true"/>
    <json xsi:nil="true"/>
    <varchar_array xsi:nil="true"/>
  </ROW>
</ROWSET>

<?xml version="1.0"?>
<ROWSET>
  <ROW>
    <integer>1</integer>
    <float>1.23456</float>
    <numeric>1.234567</numeric>
    <boolean>true</boolean>
    <char>&quot;&apos;&lt;&gt;&amp;char test      </char>
    <varchar>varchar&quot;&apos;&lt;&gt;&amp;test</varchar>
    <text>text test&quot;&apos;&lt;&gt;&amp;</text>
    <blob>FF</blob>
    <raw>ABCD</raw>
    <date>2024-01-02T00:00:00</date>
    <time>18:01:02</time>
    <timestamp>2024-02-03T19:03:04</timestamp>
    <json>{&quot;a&quot; : 1, &quot;b&quot; : 2}</json>
    <varchar_array>
      <varchar>abc</varchar>
      <varchar>&quot;&apos;&lt;&gt;&amp;</varchar>
      <varchar>你好</varchar>
    </varchar_array>
  </ROW>
  <ROW>
    <integer/>
    <float/>
    <numeric/>
    <boolean/>
    <char/>
    <varchar/>
    <text/>
    <blob/>
    <raw/>
    <date/>
    <time/>
    <timestamp/>
    <json/>
    <varchar_array/>
  </ROW>
</ROWSET>

ANONYMOUS BLOCK EXECUTE
```

- gms_xmlgen.useitemtagsforcoll(ctx in gms_xmlgen.ctxhandle)

  给数组中的子项标签加上"_ITEM"的后缀。

```
openGauss=# DECLARE
openGauss-# xml_output clob;
openGauss-# xml_cxt gms_xmlgen.ctxhandle;
openGauss-# BEGIN
openGauss$# xml_cxt := gms_xmlgen.newcontext('select * from t_types');
openGauss$# gms_xmlgen.useitemtagsforcoll(xml_cxt);
openGauss$# xml_output := gms_xmlgen.getxml(xml_cxt);
openGauss$# gms_output.put_line(xml_output);
openGauss$# gms_xmlgen.closecontext(xml_cxt);
openGauss$# END;
openGauss$# /
<?xml version="1.0"?>
<ROWSET>
  <ROW>
    <integer>1</integer>
    <float>1.23456</float>
    <numeric>1.234567</numeric>
    <boolean>true</boolean>
    <char>&quot;&apos;&lt;&gt;&amp;char test      </char>
    <varchar>varchar&quot;&apos;&lt;&gt;&amp;test</varchar>
    <text>text test&quot;&apos;&lt;&gt;&amp;</text>
    <blob>FF</blob>
    <raw>ABCD</raw>
    <date>2024-01-02T00:00:00</date>
    <time>18:01:02</time>
    <timestamp>2024-02-03T19:03:04</timestamp>
    <json>{&quot;a&quot; : 1, &quot;b&quot; : 2}</json>
    <varchar_array>
      <varchar_ITEM>abc</varchar_ITEM>
      <varchar_ITEM>&quot;&apos;&lt;&gt;&amp;</varchar_ITEM>
      <varchar_ITEM>你好</varchar_ITEM>
    </varchar_array>
  </ROW>
  <ROW>
  </ROW>
</ROWSET>

ANONYMOUS BLOCK EXECUTE
```

- gms_xmlgen.usenullattributeindicator(ctx in gms_xmlgen.ctxhandle)

  设置空数据的表现方式为显示空属性，是gms_xmlgen.SETNULLHANDLING参数取1的快捷方法。该方法第二个参数取NULL、true或者false时结果一致。

```
openGauss=# DECLARE
openGauss-# xml_output clob;
openGauss-# xml_cxt gms_xmlgen.ctxhandle;
openGauss-# BEGIN
openGauss$# xml_cxt := gms_xmlgen.newcontext('select * from t_types');
openGauss$# gms_xmlgen.usenullattributeindicator(xml_cxt);
openGauss$# xml_output := gms_xmlgen.getxml(xml_cxt);
openGauss$# gms_output.put_line(xml_output);
openGauss$# gms_xmlgen.closecontext(xml_cxt);
openGauss$# END;
openGauss$# /
<?xml version="1.0"?>
<ROWSET xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <ROW>
    <integer>1</integer>
    <float>1.23456</float>
    <numeric>1.234567</numeric>
    <boolean>true</boolean>
    <char>&quot;&apos;&lt;&gt;&amp;char test      </char>
    <varchar>varchar&quot;&apos;&lt;&gt;&amp;test</varchar>
    <text>text test&quot;&apos;&lt;&gt;&amp;</text>
    <blob>FF</blob>
    <raw>ABCD</raw>
    <date>2024-01-02T00:00:00</date>
    <time>18:01:02</time>
    <timestamp>2024-02-03T19:03:04</timestamp>
    <json>{&quot;a&quot; : 1, &quot;b&quot; : 2}</json>
    <varchar_array>
      <varchar>abc</varchar>
      <varchar>&quot;&apos;&lt;&gt;&amp;</varchar>
      <varchar>你好</varchar>
    </varchar_array>
  </ROW>
  <ROW>
    <integer xsi:nil="true"/>
    <float xsi:nil="true"/>
    <numeric xsi:nil="true"/>
    <boolean xsi:nil="true"/>
    <char xsi:nil="true"/>
    <varchar xsi:nil="true"/>
    <text xsi:nil="true"/>
    <blob xsi:nil="true"/>
    <raw xsi:nil="true"/>
    <date xsi:nil="true"/>
    <time xsi:nil="true"/>
    <timestamp xsi:nil="true"/>
    <json xsi:nil="true"/>
    <varchar_array xsi:nil="true"/>
  </ROW>
</ROWSET>

ANONYMOUS BLOCK EXECUTE
```

- gms_xmlgen.getnumrowsprocessed(ctx in gms_xmlgen.ctxhandle)

  获取已转换的行数。

```
openGauss=# DECLARE
openGauss-# processed_row number;
openGauss-# xml_output clob;
openGauss-# xml_cxt gms_xmlgen.ctxhandle;
openGauss-# BEGIN
openGauss$# xml_cxt := gms_xmlgen.newcontext('select * from t_types');
openGauss$# processed_row := gms_xmlgen.getnumrowsprocessed(xml_cxt);
openGauss$# gms_output.put_line(processed_row);
openGauss$# xml_output := gms_xmlgen.getxml(xml_cxt);
openGauss$# gms_output.put_line(xml_output);
openGauss$# processed_row := gms_xmlgen.getnumrowsprocessed(xml_cxt);
openGauss$# gms_output.put_line(processed_row);
openGauss$# gms_xmlgen.closecontext(xml_cxt);
openGauss$# END;
openGauss$# /
0
<?xml version="1.0"?>
<ROWSET>
  <ROW>
    <integer>1</integer>
    <float>1.23456</float>
    <numeric>1.234567</numeric>
    <boolean>true</boolean>
    <char>&quot;&apos;&lt;&gt;&amp;char test      </char>
    <varchar>varchar&quot;&apos;&lt;&gt;&amp;test</varchar>
    <text>text test&quot;&apos;&lt;&gt;&amp;</text>
    <blob>FF</blob>
    <raw>ABCD</raw>
    <date>2024-01-02T00:00:00</date>
    <time>18:01:02</time>
    <timestamp>2024-02-03T19:03:04</timestamp>
    <json>{&quot;a&quot; : 1, &quot;b&quot; : 2}</json>
    <varchar_array>
      <varchar>abc</varchar>
      <varchar>&quot;&apos;&lt;&gt;&amp;</varchar>
      <varchar>你好</varchar>
    </varchar_array>
  </ROW>
  <ROW>
  </ROW>
</ROWSET>

2
ANONYMOUS BLOCK EXECUTE
```

### 删除Extension<a name="section1587441381220"></a>

在openGauss中删除gms_xmlgen extension的方法如下所示：

```
openGauss=# drop extension gms_xmlgen;
```

>[!NOTE]说明
>
>gms_xmlgen插件仅支持在A模式数据库创建。
>gms_xmlgen插件依赖libxml，而轻量版openGauss不支持libxml，故轻量版openGauss不支持该插件。
>gms_xmlgen插件依赖xmltype类型，该类型于7.0.0-RC1版本引入，对数据库进行升级或回滚操作时，须先删除该插件，否则回滚升级脚本中存在依赖，升级将失败报错。
>涉及xmltype的接口需要设置GUC参数bind_procedure_searchpath。
