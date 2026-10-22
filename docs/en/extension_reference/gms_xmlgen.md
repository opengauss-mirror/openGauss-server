# gms_xmlgen

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-07-30T06:26:59.116Z pushedAt=2026-09-21T02:56:45.759Z -->

## gms_xmlgen Overview

gms_xmlgen is an openGauss-based plugin used to convert SQL query results into a standardized XML format. It supports SQL query strings or cursors as input and returns results as CLOB type or XMLTYPE type. The currently supported interfaces are: gms_xmlgen.closecontext, gms_xmlgen.getxmltype, gms_xmlgen.newcontextfromhierarchy, gms_xmlgen.convert, gms_xmlgen.getnumrowsprocessed, gms_xmlgen.getxml, gms_xmlgen.getxmltype, gms_xmlgen.newcontext, gms_xmlgen.restartquery, gms_xmlgen.setconvertspecialchars, gms_xmlgen.setmaxrows, gms_xmlgen.setnullhandling, gms_xmlgen.setrowsettag, gms_xmlgen.setrowtag, gms_xmlgen.setskiprows, gms_xmlgen.useitemtagsforcoll, gms_xmlgen.usenullattributeindicator.

## gms_xmlgen Limitations

- It is only supported to be created in an A-mode database.
- The plugin can only be loaded via the `create extension` command.
- The gms_xmlgen plugin depends on libxml, whereas the lightweight edition of openGauss does not support libxml; therefore, the lightweight edition of openGauss does not support this plugin.
- The gms_xmlgen plugin depends on the xmltype type, which was introduced in version 7.0.0-RC1. When upgrading or rolling back the database, the plugin must be dropped first; otherwise, dependencies will exist in the rollback or upgrade scripts, causing the upgrade to fail with an error.
- Interfaces involving xmltype require setting the GUC parameter bind_procedure_searchpath.

## gms_xmlgen Installation

openGauss already includes gms_xmlgen by default during packaging and compilation. After installing openGauss, the extension can be loaded directly by executing `create extension gms_xmlgen;`.

## gms_xmlgen Usage

### Creating an Extension<a name="section21088306113"></a>

To create the gms_xmlgen extension, you can directly use the <code>create extension</code> command:

```
openGauss=# create extension gms_xmlgen;
```

### Using the Extension<a name="section107391050141118"></a>

Create the extension, use the gms_output extension to display results, and prepare sample data. Interfaces involving xmltype require setting the GUC parameter bind_procedure_searchpath.

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
openGauss(#         array['abc', '"''<>&', 'Hello']
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

  Obtains XML results through an SQL query.

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

  Obtain XML results by creating a gms_xmlgen context.

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

  Obtain the XML result through a cursor.

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

  Before retrieving the result again via getxml in the gms_xmlgen context, restartquery must be executed first.

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

  Obtain the XML result of the xmltype type.

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

  Obtains the XML result of xmltype type through the gms_xmlgen context.

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

  Obtains the XML result of a hierarchical query. The first column of the result must be of int or number type, and the second column must be of xml or xmltype type. QueryString must be a hierarchical query that includes a CONNECT BY clause.

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

  Sets whether to escape the special characters &, <, >, ", and ' in the context data. false means no escaping, true means escaping, NULL is equivalent to false, and the default is true.

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

  Escapes or unescapes the special characters &, <, >, ", and '. The default is escaping.

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

  Sets the maximum number of rows for conversion.

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

  Sets the number of rows to skip from the beginning.

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

- gms_xmlgen.setrowsettag(ctx in gms_xmlgen.ctxhandle, rowsettagname in VARCHAR2)

  Sets the tag name for the row set.

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

- gms_xmlgen.setrowtag(ctx in gms_xmlgen.ctxhandle, rowtagname in VARCHAR2)

Sets the tag name for each row of data.

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

Sets how null data is represented: 0 means not displayed by default, 1 means displaying the tag with an empty attribute, and 2 means displaying an empty tag.

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

Appends the "_ITEM" suffix to the subitem tags in an array.

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

  Sets the representation of null data to display a null attribute. This is a shortcut method for setting the gms_xmlgen.SETNULLHANDLING parameter to 1. The result is consistent when the second parameter of this method is NULL, true, or false.

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

  Obtains the number of rows that have been converted.

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

### Removing an Extension<a name="section1587441381220"></a>

The method for removing the gms_xmlgen extension in openGauss is as follows:

```
openGauss=# drop extension gms_xmlgen;
```

>[!NOTE] Note
>
>The gms_xmlgen plugin is only supported for creation in A-compatibility mode databases.
>The gms_xmlgen plugin depends on libxml, and the lightweight edition of openGauss does not support libxml; therefore, the lightweight edition of openGauss does not support this plugin.
>The gms_xmlgen plugin depends on the xmltype type, which was introduced in version 7.0.0-RC1. When upgrading or rolling back the database, this plugin must be removed first; otherwise, dependencies will exist in the rollback or upgrade scripts, causing the upgrade to fail with an error.
>Interfaces involving xmltype require setting the GUC parameter bind_procedure_searchpath.