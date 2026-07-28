# gms_xmlparser

## gms_xmlparser概述

`gms_xmlparser` 是 openGauss 提供的 XML 解析扩展，用于创建解析器句柄、解析 XML 字符串或 XML 文件，并返回 `gms_xmldom.domdocument` 文档对象。该扩展主要面向 Oracle 兼容场景，当前支持的接口如下：

- `gms_xmlparser.newparser`
- `gms_xmlparser.parse`
- `gms_xmlparser.parsebuffer`
- `gms_xmlparser.parseclob`
- `gms_xmlparser.getdocument`
- `gms_xmlparser.freeparser`
- `gms_xmldom.freedocument`

安装扩展后会自动创建 `gms_xmlparser` 和 `gms_xmldom` 两个 schema。

## gms_xmlparser限制

- 仅支持通过 `CREATE EXTENSION` 方式加载扩展。
- 扩展依赖 `libxml`，轻量版 openGauss 不支持该扩展。
- `gms_xmlparser.parser` 和 `gms_xmldom.domdocument` 为内部句柄类型，不支持用户直接构造输入输出值。
- `gms_xmlparser.parse(varchar2)` 的输入会优先按 XML 字符串解析；若不是合法 XML，则按本地文件路径处理。
- 解析器句柄和文档句柄使用完成后应及时释放，避免占用会话内存。

## gms_xmlparser安装

openGauss 打包编译时已包含 `gms_xmlparser`，安装数据库后可直接执行如下命令加载扩展：

```sql
openGauss=# CREATE EXTENSION gms_xmlparser;
```

## gms_xmlparser使用

### 创建 Extension

```sql
openGauss=# CREATE EXTENSION gms_xmlparser;
CREATE EXTENSION
```

### 直接解析 XML 字符串

- `gms_xmlparser.parse(str in varchar2) return gms_xmldom.domdocument`

  **描述**：直接解析 XML 字符串，返回文档对象。

  **参数说明**：

  - `str`：XML 字符串，或可访问的 XML 文件路径。

  **返回类型**：`gms_xmldom.domdocument`

  **示例**：

```sql
openGauss=# DECLARE
openGauss-#   xml_data varchar2(2000);
openGauss-#   dom_doc gms_xmldom.domdocument;
openGauss-# BEGIN
openGauss$#   xml_data := '<?xml version="1.0" encoding="UTF-8"?>
openGauss$# <messege>
openGauss$# <warning>
openGauss$#
openGauss$# Hello World!
openGauss$# </warning>
openGauss$# </messege>';
openGauss$#   dom_doc := gms_xmlparser.parse(xml_data);
openGauss$#   gms_xmldom.freedocument(dom_doc);
openGauss$# END;
openGauss$# /
ANONYMOUS BLOCK EXECUTE
```

### 创建解析器并解析

- `gms_xmlparser.newparser() return gms_xmlparser.parser`

  **描述**：创建 XML 解析器句柄。

- `gms_xmlparser.parse(parser in gms_xmlparser.parser, str in varchar2)`

  **描述**：使用指定解析器解析 XML 字符串或 XML 文件。

- `gms_xmlparser.getdocument(parser in gms_xmlparser.parser) return gms_xmldom.domdocument`

  **描述**：获取解析器当前持有的文档对象。

- `gms_xmlparser.freeparser(parser in gms_xmlparser.parser)`

  **描述**：释放解析器句柄。

  **调用关系**：通常先通过 `newparser` 创建解析器，再调用 `parse` / `parsebuffer` / `parseclob` 进行解析，然后通过 `getdocument` 获取文档对象，最后分别调用 `gms_xmldom.freedocument` 和 `freeparser` 释放资源。

  **示例**：

```sql
openGauss=# DECLARE
openGauss-#   parser gms_xmlparser.parser;
openGauss-#   xml_data varchar2(2000);
openGauss-#   dom_doc gms_xmldom.domdocument;
openGauss-# BEGIN
openGauss$#   xml_data := '<?xml version="1.0" encoding="UTF-8"?>
openGauss$# <messege>
openGauss$# <warning>
openGauss$#
openGauss$# Hello World!
openGauss$# </warning>
openGauss$# </messege>';
openGauss$#   parser := gms_xmlparser.newparser();
openGauss$#   gms_xmlparser.parse(parser, xml_data);
openGauss$#   dom_doc := gms_xmlparser.getdocument(parser);
openGauss$#   gms_xmldom.freedocument(dom_doc);
openGauss$#   gms_xmlparser.freeparser(parser);
openGauss$# END;
openGauss$# /
ANONYMOUS BLOCK EXECUTE
```

### 解析缓冲区内容

- `gms_xmlparser.parsebuffer(parser in gms_xmlparser.parser, str in varchar2)`

  **描述**：将输入字符串作为 XML 缓冲区内容进行解析，并把结果保存到解析器中。

  **示例**：

```sql
openGauss=# DECLARE
openGauss-#   parser gms_xmlparser.parser;
openGauss-#   xml_data varchar2(2000);
openGauss-#   dom_doc gms_xmldom.domdocument;
openGauss-# BEGIN
openGauss$#   xml_data := '<?xml version="1.0" encoding="UTF-8"?>
openGauss$# <messege>
openGauss$# <warning>
openGauss$#
openGauss$# Hello World!
openGauss$# </warning>
openGauss$# </messege>';
openGauss$#   parser := gms_xmlparser.newparser();
openGauss$#   gms_xmlparser.parsebuffer(parser, xml_data);
openGauss$#   dom_doc := gms_xmlparser.getdocument(parser);
openGauss$#   gms_xmldom.freedocument(dom_doc);
openGauss$#   gms_xmlparser.freeparser(parser);
openGauss$# END;
openGauss$# /
ANONYMOUS BLOCK EXECUTE
```

### 解析 CLOB 内容

- `gms_xmlparser.parseclob(parser in gms_xmlparser.parser, str in varchar2)`

  **描述**：将输入的大文本内容按 XML 文档解析，并把结果保存到解析器中。

  **示例**：

```sql
openGauss=# DECLARE
openGauss-#   parser gms_xmlparser.parser;
openGauss-#   lob_data CLOB;
openGauss-#   dom_doc gms_xmldom.domdocument;
openGauss-# BEGIN
openGauss$#   lob_data := '<?xml version="1.0" encoding="UTF-8"?>
openGauss$# <messege>
openGauss$# <warning>
openGauss$#
openGauss$# Hello World!
openGauss$# </warning>
openGauss$# </messege>';
openGauss$#   parser := gms_xmlparser.newparser();
openGauss$#   gms_xmlparser.parseclob(parser, lob_data);
openGauss$#   dom_doc := gms_xmlparser.getdocument(parser);
openGauss$#   gms_xmldom.freedocument(dom_doc);
openGauss$#   gms_xmlparser.freeparser(parser);
openGauss$# END;
openGauss$# /
ANONYMOUS BLOCK EXECUTE
```

### 释放文档对象

- `gms_xmldom.freedocument(doc in gms_xmldom.domdocument)`

  **描述**：释放文档对象句柄。

  **示例**：

```sql
openGauss=# DECLARE
openGauss-#   xml_data varchar2(2000);
openGauss-#   dom_doc gms_xmldom.domdocument;
openGauss-# BEGIN
openGauss$#   xml_data := '<?xml version="1.0" encoding="UTF-8"?>
openGauss$# <messege>
openGauss$# <warning>
openGauss$#
openGauss$# Hello World!
openGauss$# </warning>
openGauss$# </messege>';
openGauss$#   dom_doc := gms_xmlparser.parse(xml_data);
openGauss$#   gms_xmldom.freedocument(dom_doc);
openGauss$# END;
openGauss$# /
ANONYMOUS BLOCK EXECUTE
```

### 删除 Extension

```sql
openGauss=# DROP EXTENSION gms_xmlparser;
```

>[!NOTE]说明
>
>- 若扩展对象被其他对象依赖，删除时需要先解除依赖关系。
>- `gms_xmlparser.parser` 和 `gms_xmldom.domdocument` 为内部资源句柄，建议在同一业务流程中创建、使用并释放。
