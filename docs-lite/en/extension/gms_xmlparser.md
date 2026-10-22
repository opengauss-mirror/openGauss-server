# gms_xmlparser

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-07-30T06:27:20.822Z pushedAt=2026-09-21T03:28:00.565Z -->

## gms_xmlparser Overview

`gms_xmlparser` is an XML parsing extension provided by openGauss. It is used to create parser handles, parse XML strings or XML files, and return `gms_xmldom.domdocument` document objects. This extension is primarily intended for Oracle compatibility scenarios. The currently supported interfaces are as follows:

- `gms_xmlparser.newparser`
- `gms_xmlparser.parse`
- `gms_xmlparser.parsebuffer`
- `gms_xmlparser.parseclob`
- `gms_xmlparser.getdocument`
- `gms_xmlparser.freeparser`
- `gms_xmldom.freedocument`

After installing the extension, two schemas, `gms_xmlparser` and `gms_xmldom`, are automatically created.

## gms_xmlparser Restrictions

- Only supports loading the extension via `CREATE EXTENSION`.
- The extension depends on `libxml`, and the lightweight edition of openGauss does not support this extension.
- `gms_xmlparser.parser` and `gms_xmldom.domdocument` are internal handle types and do not support direct construction of input/output values by users.
- The input to `gms_xmlparser.parse(varchar2)` is first parsed as an XML string. If it is not valid XML, it is then processed as a local file path.
- Parser handles and document handles should be released promptly after use to avoid occupying session memory.

## gms_xmlparser Installation

gms_xmlparser is already included during openGauss packaging and compilation. After installing the database, you can directly execute the following command to load the extension:

```sql
openGauss=# CREATE EXTENSION gms_xmlparser;
```

## gms_xmlparser Usage

### Creating an Extension

```sql
openGauss=# CREATE EXTENSION gms_xmlparser;
CREATE EXTENSION
```

### Directly Parsing an XML String

- `gms_xmlparser.parse(str in varchar2) return gms_xmldom.domdocument`

  **Description**: Directly parses an XML string and returns a document object.

  **Parameter Description**:

  - `str`: An XML string, or an accessible XML file path.

  **Return type**: `gms_xmldom.domdocument`

  **Example**:

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

### Creating a Parser and Parsing

- `gms_xmlparser.newparser() return gms_xmlparser.parser`

  **Description**: Creates an XML parser handle.

- `gms_xmlparser.parse(parser in gms_xmlparser.parser, str in varchar2)`

  **Description**: Parses an XML string or XML file using the specified parser.

- `gms_xmlparser.getdocument(parser in gms_xmlparser.parser) return gms_xmldom.domdocument`

  **Description**: Obtains the document object currently held by the parser.

- `gms_xmlparser.freeparser(parser in gms_xmlparser.parser)`

  **Description**: Releases the parser handle.

  **Calling sequence**: Typically, first create a parser via `newparser`, then call `parse` / `parsebuffer` / `parseclob` to perform parsing, then obtain the document object via `getdocument`, and finally release resources by calling `gms_xmldom.freedocument` and `freeparser` respectively.

  **Example**:

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

### Parsing Buffer Content

- `gms_xmlparser.parsebuffer(parser in gms_xmlparser.parser, str in varchar2)`

  **Description**: Parses the input string as XML buffer content and saves the result into the parser.

  **Example**:

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

### Parsing CLOB Content

- `gms_xmlparser.parseclob(parser in gms_xmlparser.parser, str in varchar2)`

  **Description**: Parses the input large text content as an XML document and saves the result into the parser.

  **Example**:

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

### Freeing a Document Object

- `gms_xmldom.freedocument(doc in gms_xmldom.domdocument)`

  **Description**: Frees the document object handle.

  **Example**:

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

### Dropping the Extension

```sql
openGauss=# DROP EXTENSION gms_xmlparser;
```

>[!NOTE]Note
>
>- If an extension object is depended on by other objects, you must first remove the dependency before dropping it.
>- `gms_xmlparser.parser` and `gms_xmldom.domdocument` are internal resource handles. It is recommended to create, use, and release them within the same business process.