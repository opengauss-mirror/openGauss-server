# gms_xmldom

## gms_xmldom Overview

gms_xmldom is a built-in feature of openGauss implemented based on the PL/Python language. It encapsulates underlying Python XML DOM operations into PL/pgSQL functions that comply with Oracle specifications. The package defines a series of custom data types used to represent different DOM objects at the SQL level, such as DOMDocument, DOMNode, and DOMElement.

## gms_xmldom Data Types

**Table 1** Description of gms_xmldom data types

<a name="table1011513101687"></a>
<table><tbody><tr id="row201685101086"><td class="cellrowborder" valign="top" width="30.383038303830386%"><p id="p7168210483"><a name="p7168210483"></a><a name="p7168210483"></a><strong id="b1316817109817"><a name="b1316817109817"></a><a name="b1316817109817"></a>Type Name</strong></p>
</td>
<td class="cellrowborder" valign="top" width="30.243024302430243%"><p id="p1816817101585"><a name="p1816817101585"></a><a name="p1816817101585"></a><strong id="b1016820101589"><a name="b1016820101589"></a><a name="b1016820101589"></a>Description</strong></p>
</td>
<td class="cellrowborder" valign="top" width="39.373937393739375%"><p id="p111687101286"><a name="p111687101286"></a><a name="p111687101286"></a><strong id="b1716911015819"><a name="b1716911015819"></a><a name="b1716911015819"></a>Type</strong></p>
</td>
</tr>
<tr id="row81692010682"><td class="cellrowborder" valign="top" width="30.383038303830386%"><p id="p916919107811"><a name="p916919107811"></a><a name="p916919107811"></a>DOMNode</p>
</td>
<td class="cellrowborder" valign="top" width="30.243024302430243%"><p id="p216911100815"><a name="p216911100815"></a><a name="p216911100815"></a>Represents a single node in the XML document tree, which can refer to any type of node.</p>
</td>
<td class="cellrowborder" valign="top" width="39.373937393739375%"><p id="p382419359375"><a name="p382419359375"></a><a name="p382419359375"></a>Node</p>
</td>
</tr>
<tr id="row413211712177"><td class="cellrowborder" valign="top" width="30.383038303830386%"><p id="p813487181720"><a name="p813487181720"></a><a name="p813487181720"></a>DOMDocument Type</p>
</td>
<td class="cellrowborder" valign="top" width="30.243024302430243%"><p id="p1013416713174"><a name="p1013416713174"></a><a name="p1013416713174"></a>Document node. Represents the entire XML document, serves as the root of the document tree, and provides the top-level entry point for accessing document data.</p>
</td>
<td class="cellrowborder" valign="top" width="39.373937393739375%"><p id="p173511513616"><a name="p173511513616"></a><a name="p173511513616"></a>Document</p>
</td>
</tr>
<tr id="row413211712177"><td class="cellrowborder" valign="top" width="30.383038303830386%"><p id="p813487181720"><a name="p813487181720"></a><a name="p813487181720"></a>DOMElement</p>
</td>
<td class="cellrowborder" valign="top" width="30.243024302430243%"><p id="p1013416713174"><a name="p1013416713174"></a><a name="p1013416713174"></a>Element node, representing an element in the XML document. An element can contain attributes, nest other elements, or contain text.</p>
</td>
<td class="cellrowborder" valign="top" width="39.373937393739375%"><p id="p173511513616"><a name="p173511513616"></a><a name="p173511513616"></a>Element</p>
</td>
</tr>
<tr id="row413211712177"><td class="cellrowborder" valign="top" width="30.383038303830386%"><p id="p813487181720"><a name="p813487181720"></a><a name="p813487181720"></a>DOMAttr</p>
</td>
<td class="cellrowborder" valign="top" width="30.243024302430243%"><p id="p1013416713174"><a name="p1013416713174"></a><a name="p1013416713174"></a>Attr node. Represents an attribute in an Element node.</p>
</td>
<td class="cellrowborder" valign="top" width="39.373937393739375%"><p id="p173511513616"><a name="p173511513616"></a><a name="p173511513616"></a>Attribute</p>
</td>
</tr>
<tr id="row413211712177"><td class="cellrowborder" valign="top" width="30.383038303830386%"><p id="p813487181720"><a name="p813487181720"></a><a name="p813487181720"></a>DOMText</p>
</td>
<td class="cellrowborder" valign="top" width="30.243024302430243%"><p id="p1013416713174"><a name="p1013416713174"></a><a name="p1013416713174"></a>Text node, representing the text content of an element or attribute.</p>
</td>
<td class="cellrowborder" valign="top" width="39.373937393739375%"><p id="p173511513616"><a name="p173511513616"></a><a name="p173511513616"></a>Text</p>
</td>
</tr>
<tr id="row413211712177"><td class="cellrowborder" valign="top" width="30.383038303830386%"><p id="p813487181720"><a name="p813487181720"></a><a name="p813487181720"></a>DOMCDATASection</p>
</td>
<td class="cellrowborder" valign="top" width="30.243024302430243%"><p id="p1013416713174"><a name="p1013416713174"></a><a name="p1013416713174"></a>CDATASection node, representing a CDATA section in the XML document. A CDATA section is a block of text that will not be parsed by the parser.</p>
</td>
<td class="cellrowborder" valign="top" width="39.373937393739375%"><p id="p173511513616"><a name="p173511513616"></a><a name="p173511513616"></a>Section</p>
</td>
</tr>
<tr id="row413211712177"><td class="cellrowborder" valign="top" width="30.383038303830386%"><p id="p813487181720"><a name="p813487181720"></a><a name="p813487181720"></a>DOMComment</p>
</td>
<td class="cellrowborder" valign="top" width="30.243024302430243%"><p id="p1013416713174"><a name="p1013416713174"></a><a name="p1013416713174"></a>Comment node. Represents the content of a comment node in the XML document.</p>
</td>
<td class="cellrowborder" valign="top" width="39.373937393739375%"><p id="p173511513616"><a name="p173511513616"></a><a name="p173511513616"></a>Comment</p>
</td>
</tr>
<tr id="row413211712177"><td class="cellrowborder" valign="top" width="30.383038303830386%"><p id="p813487181720"><a name="p813487181720"></a><a name="p813487181720"></a>DOMEntity</p>
</td>
<td class="cellrowborder" valign="top" width="30.243024302430243%"><p id="p1013416713174"><a name="p1013416713174"></a><a name="p1013416713174"></a>Entity node. When a specific piece of data is frequently used in an XML document, an "alias" for this data, i.e., an Entity, can be predefined and then invoked where needed in the document.</p>
</td>
<td class="cellrowborder" valign="top" width="39.373937393739375%"><p id="p173511513616"><a name="p173511513616"></a><a name="p173511513616"></a>Entity</p>
</td>
</tr>
<tr id="row413211712177"><td class="cellrowborder" valign="top" width="30.383038303830386%"><p id="p813487181720"><a name="p813487181720"></a><a name="p813487181720"></a>DOMDocumentFragment</p>
</td>
<td class="cellrowborder" valign="top" width="30.243024302430243%"><p id="p1013416713174"><a name="p1013416713174"></a><a name="p1013416713174"></a>DocumentFragment node, a part of the document, representing one or more adjacent Document nodes and all their descendant nodes. Note that a DocumentFragment node does not belong to the document tree.</p>
</td>
<td class="cellrowborder" valign="top" width="39.373937393739375%"><p id="p173511513616"><a name="p173511513616"></a><a name="p173511513616"></a>Fragment</p>
</td>
</tr>
<tr id="row413211712177"><td class="cellrowborder" valign="top" width="30.383038303830386%"><p id="p813487181720"><a name="p813487181720"></a><a name="p813487181720"></a>DOMNotation</p>
</td>
<td class="cellrowborder" valign="top" width="30.243024302430243%"><p id="p1013416713174"><a name="p1013416713174"></a><a name="p1013416713174"></a>Notation element. A Notation element describes the format of non-XML data in an XML document.</p>
</td>
<td class="cellrowborder" valign="top" width="39.373937393739375%"><p id="p173511513616"><a name="p173511513616"></a><a name="p173511513616"></a>Notation</p>
</td>
</tr>
<tr id="row413211712177"><td class="cellrowborder" valign="top" width="30.383038303830386%"><p id="p813487181720"><a name="p813487181720"></a><a name="p813487181720"></a>DOMProcessingInstruction</p>
</td>
<td class="cellrowborder" valign="top" width="30.243024302430243%"><p id="p1013416713174"><a name="p1013416713174"></a><a name="p1013416713174"></a>ProcessingInstruction node, representing a processing instruction in the XML document.</p>
</td>
<td class="cellrowborder" valign="top" width="39.373937393739375%"><p id="p173511513616"><a name="p173511513616"></a><a name="p173511513616"></a>ProcessingInstruction</p>
</td>
</tr>
<tr id="row413211712177"><td class="cellrowborder" valign="top" width="30.383038303830386%"><p id="p813487181720"><a name="p813487181720"></a><a name="p813487181720"></a>DOMDocumentType</p>
</td>
<td class="cellrowborder" valign="top" width="30.243024302430243%"><p id="p1013416713174"><a name="p1013416713174"></a><a name="p1013416713174"></a>DocumentType node. Every XML document has a DOCTYPE attribute, whose value can be null or a DocumentType object. The DocumentType object provides an interface for entities defined in the XML.</p>
</td>
<td class="cellrowborder" valign="top" width="39.373937393739375%"><p id="p173511513616"><a name="p173511513616"></a><a name="p173511513616"></a>DocumentType</p>
</td>
</tr>
</tbody>
</table>

## gms_xmldom Supported Interfaces

+ gms_xmldom.makeNode
  - makeNode(t gms_xmldom.DOMText)(t gms_xmldom.DOMText)
  - makeNode(t gms_xmldom.DOMText)(com gms_xmldom.DOMComment)
  - makeNode(t gms_xmldom.DOMText)(cds gms_xmldom.DOMCDATASection)
  - makeNode(t gms_xmldom.DOMText)(dt gms_xmldom.DOMDocumentType)
  - makeNode(t gms_xmldom.DOMText)(n gms_xmldom.DOMNotation)
  - makeNode(t gms_xmldom.DOMText)(ent gms_xmldom.DOMEntity)
  - makeNode(t gms_xmldom.DOMText)(pi gms_xmldom.DOMProcessingInstruction)
  - makeNode(t gms_xmldom.DOMText)(df gms_xmldom.DOMDocumentFragment)
  - makeNode(t gms_xmldom.DOMText)(doc gms_xmldom.DOMDocument)
  - makeNode(t gms_xmldom.DOMText)(elem gms_xmldom.DOMElement)
+ gms_xmldom.isNull
  - isNull(n gms_xmldom.DOMNode)
  - isNull(di gms_xmldom.DOMImplementation)
  - isNull(nl gms_xmldom.DOMNodeList)
  - isNull(nnm gms_xmldom.DOMNamedNodeMap)
  - isNull(cd gms_xmldom.DOMCharacterData)
  - isNull(a gms_xmldom.DOMAttr)
  - isNull(elem gms_xmldom.DOMElement)
  - isNull(t gms_xmldom.DOMText)
  - isNull(com gms_xmldom.DOMComment)
  - isNull(cds gms_xmldom.DOMCDATASection)
  - isNull(dt gms_xmldom.DOMDocumentType)
  - isNull(n gms_xmldom.DOMNotation)
  - isNull(ent gms_xmldom.DOMEntity)
  - isNull(pi gms_xmldom.DOMProcessingInstruction)
  - isNull(df gms_xmldom.DOMDocumentFragment)
  - isNull(doc gms_xmldom.DOMDocument)
+ gms_xmldom.freeNode
  - freeNode(n gms_xmldom.DOMNode)
+ gms_xmldom.freeNodeList
  - freeNodeList(nl gms_xmldom.DOMNodeList)
+ gms_xmldom.freeDocument
  - freeDocument(doc gms_xmldom.DOMDocument)
+ gms_xmldom.getFirstChild
  - getFirstChild(n gms_xmldom.DOMNode)
+ gms_xmldom.getLocalName
  - getLocalName(a gms_xmldom.DOMAttr)
  - getLocalName(elem gms_xmldom.DOMElement)
  - getLocalName(n gms_xmldom.DOMNode, data OUT VARCHAR2)
+ gms_xmldom.getNodeType
  - getNodeType(n gms_xmldom.DOMNode)
+ gms_xmldom.writeToClob
  - writeToClob(n gms_xmldom.DOMNode, cl IN OUT CLOB)
  - writeToClob(n gms_xmldom.DOMNode, cl IN OUT CLOB, pflag IN NUMBER, indent IN NUMBER)
  - writeToClob(doc gms_xmldom.DOMDocument, cl IN OUT CLOB)
  - writeToClob(doc gms_xmldom.DOMDocument, cl IN OUT CLOB, pflag IN NUMBER, indent IN NUMBER)
+ gms_xmldom.writeToBuffer
  - writeToBuffer(n gms_xmldom.DOMNode, buffer IN OUT VARCHAR2)
  - writeToBuffer(doc gms_xmldom.DOMDocument, buffer IN OUT VARCHAR2)
  - writeToBuffer(df gms_xmldom.DOMDocumentFragment, buffer IN OUT VARCHAR2)
+ gms_xmldom.getChildNodes
  - getChildNodes(n gms_xmldom.DOMNode)
+ gms_xmldom.getLength
  - getLength(nl gms_xmldom.DOMNodeList)
  - getLength(nnm gms_xmldom.DOMNamedNodeMap)
  - getLength(cd gms_xmldom.DOMCharacterData)
+ gms_xmldom.item
  - item(nl gms_xmldom.DOMNodeList, idx IN PLS_INTEGER)
  - item(nnm gms_xmldom.DOMNamedNodeMap, idx IN PLS_INTEGER)
+ gms_xmldom.makeElement
  - makeElement(n gms_xmldom.DOMNode)
+ gms_xmldom.getElementsByTagName
  - getElementsByTagName(elem gms_xmldom.DOMElement, name IN VARCHAR2)
  - getElementsByTagName(elem gms_xmldom.DOMElement, name IN VARCHAR2, ns VARCHAR2)
  - getElementsByTagName(doc gms_xmldom.DOMDocument, tagname IN VARCHAR2)
+ gms_xmldom.cloneNode
  - cloneNode(n gms_xmldom.DOMNode, deep BOOLEAN)
+ gms_xmldom.getNodeName
  - getNodeName(n gms_xmldom.DOMNode)
+ gms_xmldom.createDocument
  - createDocument(namespaceuri IN VARCHAR2, qualifiedname IN VARCHAR2, doctype IN gms_xmldom.DOMDocumentType := NULL)
+ gms_xmldom.createElement
  - createElement(doc gms_xmldom.DOMDocument, tagname IN VARCHAR2)
  - createElement(doc gms_xmldom.DOMDocument, tagname IN VARCHAR2, ns IN VARCHAR2)
+ gms_xmldom.createDocumentFragment
  - createDocumentFragment(doc gms_xmldom.DOMDocument)
+ gms_xmldom.createTextNode
  - createTextNode(doc gms_xmldom.DOMDocument, data IN VARCHAR2)
+ gms_xmldom.createComment
  - createComment(doc gms_xmldom.DOMDocument, data IN VARCHAR2)
+ gms_xmldom.createCDATASection
  - createCDATASection(doc gms_xmldom.DOMDocument, data IN VARCHAR2)
+ gms_xmldom.createProcessingInstruction
  - createProcessingInstruction(doc gms_xmldom.DOMDocument, target IN VARCHAR2, data IN VARCHAR2)
+ gms_xmldom.createAttribute
  - createAttribute(doc gms_xmldom.DOMDocument, name IN VARCHAR2)
  - createAttribute(doc gms_xmldom.DOMDocument, name IN VARCHAR2, ns IN VARCHAR2)
+ gms_xmldom.appendChild
  - appendChild(n gms_xmldom.DOMNode, newchild IN gms_xmldom.DOMNode)
+ gms_xmldom.getDocumentElement
  - getDocumentElement(doc gms_xmldom.DOMDocument)
+ gms_xmldom.setAttribute
  - setAttribute(elem gms_xmldom.DOMElement, name IN VARCHAR2, newvalue IN VARCHAR2)
  - setAttribute(elem gms_xmldom.DOMElement, name IN VARCHAR2, newvalue IN VARCHAR2, ns IN VARCHAR2)
+ gms_xmldom.setAttributeNode
  - setAttributeNode(elem gms_xmldom.DOMElement, newattr IN gms_xmldom.DOMAttr)
  - setAttributeNode(elem gms_xmldom.DOMElement, newattr IN gms_xmldom.DOMAttr, ns IN VARCHAR2)
+ gms_xmldom.getAttributes
  - getAttributes(n gms_xmldom.DOMNode)
+ gms_xmldom.getNodeValue
  - getNodeValue(n gms_xmldom.DOMNode)
+ gms_xmldom.getNodeValueAsClob
  - getNodeValueAsClob(n gms_xmldom.DOMNode)
+ gms_xmldom.getChildrenByTagName
  - getChildrenByTagName(elem gms_xmldom.DOMElement, name VARCHAR2)
  - getChildrenByTagName(elem gms_xmldom.DOMElement, name VARCHAR2, ns VARCHAR2)
+ gms_xmldom.getOwnerDocument
  - getOwnerDocument(n gms_xmldom.DOMNode)
+ gms_xmldom.newDOMDocument
  - newDOMDocument()
  - newDOMDocument(xmldoc IN XMLTYPE)
  - newDOMDocument(cl IN CLOB)
+ gms_xmldom.hasChildNodes
  - hasChildNodes(n gms_xmldom.DOMNode)
+ gms_xmldom.setVersion
  - setVersion(doc gms_xmldom.DOMDocument, version VARCHAR2)
+ gms_xmldom.makeCharacterData
  - makeCharacterData(n gms_xmldom.DOMNode)

## Precautions for Using gms_xmldom

Since the underlying implementation of gms_xmldom depends on the `plpython3u` extension, the following applies:

1. Python 3 (version > 3.7) must be installed in the openGauss compilation environment or integrated into the third-party toolset relied upon for compilation.
2. When using automake to configure compilation parameters for openGauss, the `--with-python` parameter must be added.
3. When using CMake to configure parameters for openGauss, the `-DENABLE_PYTHON3=ON` parameter must be set.
4. After installing openGauss, the `PYTHONHOME` environment variable must be set to the `python` directory under `GAUSSHOME`. *(Note: Fixed typo `PYTHONHOE` from the original text)*
5. After installing openGauss, the directory `$GAUSSHOME/python/lib64` must be added to the `LD_LIBRARY_PATH` environment variable.
6. The miniaturized (lightweight) version of openGauss does not support the `plpython3u` extension and therefore cannot use the `gms_xmldom API package`.
7. The `plpython3u` extension does not support the `set schema` operation; any related operations will result in an error indicating that it is unsupported.

## gms_xmldom Installation

To install `gms_xmldom`, you only need to install the `plpython3u` extension to use the corresponding set of interfaces.

```
CREATE EXTENSION plpython3u;

```

## gms_xmldom Uninstallation

To uninstall `gms_xmldom`, you only need to drop the plpython3u extension to disable the corresponding set of interfaces.

```
DROP EXTENSION plpython3u;

```
## gms_xmldom Examples

### Case 1: Create an empty XML document and insert an element node to build the document

```
CREATE EXTENSION plpython3u;

DECLARE
    doc gms_xmldom.DOMDocument;
    elem gms_xmldom.DOMElement;
    root gms_xmldom.DOMNode;
    elemNode gms_xmldom.DOMNode;
    cl CLOB;
    appResNode gms_xmldom.DOMNode;
BEGIN
    SET serveroutput ON;
    doc := gms_xmldom.newDomDocument;
    root := gms_xmldom.makeNode(doc);
    elem := gms_xmldom.createElement(doc, 'root');
    elemNode := gms_xmldom.makeNode(elem);
    appResNode := gms_xmldom.appendChild(root, elemNode);
    cl := gms_xmldom.writeToClob(doc, cl);
    gms_output.put_line(cl);
END;
/

Output:
<?xml version="1.0" ?>
<root/>

```

### Case 2: Construct an XML document based on a manually input CLOB or XMLTYPE string

```
CREATE EXTENSION plpython3u;

DECLARE
    doc gms_xmldom.DOMDocument;
    cl CLOB;
    x XMLTYPE;
BEGIN
    SET serveroutput ON;
    x := XMLTYPE('<PERSON><NAME>ramesh</NAME></PERSON>');
    doc := gms_xmldom.newDomDocument(x);
    cl := gms_xmldom.writeToClob(doc, cl);
    gms_output.put_line(cl);
END;
/

Output:
<?xml version="1.0" ?>
<PERSON>
  <NAME>ramesh</NAME>
</PERSON>

```

### Case 3: Construct an XML document containing a namespace and insert nodes

```
CREATE EXTENSION plpython3u;

DECLARE
    doc gms_xmldom.DOMDocument;
    rootElem gms_xmldom.DOMElement;
    rootNode gms_xmldom.DOMNode;
    elem gms_xmldom.DOMElement;
    elemNode gms_xmldom.DOMNode;
    wclob CLOB;
    resNode gms_xmldom.DOMNode;
BEGIN
    doc := gms_xmldom.createDocument('http://www.runoob.com/xml/', 'xml', NULL);
    rootElem := gms_xmldom.getDocumentElement(doc);
    rootNode := gms_xmldom.makeNode(rootElem);
    elem := gms_xmldom.createElement(doc, 'head', 'http://www.runoob.com/xml/');
    PERFORM gms_xmldom.setAttribute(elem, 'id', 'headDoc', 'http://www.runoob.com/xml/');
    elemNode := gms_xmldom.makeNode(elem);
    resNode := gms_xmldom.appendChild(rootNode, elemNode);
    
    elem := gms_xmldom.createElement(doc, 'body', 'http://www.runoob.com/xml/');
    PERFORM gms_xmldom.setAttribute(elem, 'id', 'bodyDoc', 'http://www.runoob.com/xml/');
    elemNode := gms_xmldom.makeNode(elem);
    resNode := gms_xmldom.appendChild(rootNode, elemNode);
    wclob := gms_xmldom.writeToClob(doc, wclob);
    -- Output clob content  
    gms_output.put_line(wclob);
END;
/

Output:
<?xml version="1.0" ?>
<xml>
  <head id="headDoc"/>
  <body id="bodyDoc"/>
</xml>

```

### Case 4: Create nodes and insert them into an XML document

```
CREATE EXTENSION plpython3u;

DECLARE
    var XMLTYPE;
    doc gms_xmldom.DOMDocument;
    docNode gms_xmldom.DOMNode;
    bookListNode gms_xmldom.DOMNode;
    nodeList gms_xmldom.DOMNodeList;
    node gms_xmldom.DOMNode;
    comment gms_xmldom.DOMComment;    
    procInstruc gms_xmldom.DOMProcessingInstruction;
    elem gms_xmldom.DOMElement;
    txt gms_xmldom.DOMText;
    attr gms_xmldom.DOMAttr;
    wclob CLOB;
    isNull BOOLEAN;
    makeNode1 gms_xmldom.DOMNode;
    makeNode2 gms_xmldom.DOMNode;
    resNode gms_xmldom.DOMNode;
BEGIN
    var := XMLTYPE('<booklist type="science and engineering">
  <book category="math">
    <title>learning math</title>
    <author>Zhang San</author>
    <pageNumber>561</pageNumber>
  </book>
</booklist>');
    doc := gms_xmldom.newDOMDocument(var);
    docNode := gms_xmldom.makeNode(doc);
    bookListNode := gms_xmldom.getFirstChild(docNode);
    nodeList := gms_xmldom.getElementsByTagName(doc, 'book');
    node := gms_xmldom.item(nodeList, 0);
    -- Create and insert a comment node
    comment := gms_xmldom.createComment(doc, 'This is the introduction of books');
    isNull := gms_xmldom.isNull(comment);
    gms_output.put_line('DOMComment : ' || CASE WHEN isNull THEN 'Y' ELSE 'N' END);
    makeNode1 := gms_xmldom.makeNode(comment);
    resNode := gms_xmldom.insertBefore(bookListNode, makeNode1, node);
    -- Create and insert a ProcessingInstruction node
    procInstruc := gms_xmldom.createProcessingInstruction(doc, 'xml', 'version="2.0"');
    makeNode1 := gms_xmldom.makeNode(procInstruc);
    resNode := gms_xmldom.insertBefore(docNode, makeNode1, bookListNode);
    -- Create and insert a text node
    txt := gms_xmldom.createTextNode(doc, 'learning python');
    makeNode2 := gms_xmldom.makeNode(txt);
    elem := gms_xmldom.createElement(doc, 'title');
    makeNode1 := gms_xmldom.makeNode(elem);
    resNode := gms_xmldom.appendChild(makeNode1, makeNode2);
    elem := gms_xmldom.createElement(doc, 'book');
    attr := gms_xmldom.createAttribute(doc,'category');
    PERFORM gms_xmldom.setAttributeNode(elem, attr);
    makeNode2 := gms_xmldom.makeNode(elem);
    resNode := gms_xmldom.appendChild(makeNode2, makeNode1);
    resNode := gms_xmldom.appendChild(bookListNode, makeNode2);
    wclob := gms_xmldom.writeToClob(doc, wclob);
    -- Output the modified clob content  
    gms_output.put_line(wclob);
END;
/

Output:
DOMComment : N
<?xml version="1.0" ?>
<?xml version="2.0"?>
<booklist type="science and engineering">
  <!--This is the introduction of books-->
  <book category="math">
    <title>learning math</title>
    <author>Zhang San</author>
    <pageNumber>561</pageNumber>
  </book>
  <book category="">
    <title>learning python</title>
  </book>
</booklist>

```
### Case 5: Retrieve node information based on an existing XML document

```
CREATE EXTENSION plpython3u;

DECLARE
    var XMLTYPE;
    doc gms_xmldom.DOMDocument;
    docNode gms_xmldom.DOMNode;
    bookListNode gms_xmldom.DOMNode;
    nodeList gms_xmldom.DOMNodeList;
    node gms_xmldom.DOMNode;
    titleNode gms_xmldom.DOMNode;
    elemNode gms_xmldom.DOMElement;
    txt gms_xmldom.DOMText;
    textNode gms_xmldom.DOMNode;
    wclob CLOB;
    llen INTEGER;
    n INTEGER := 0;
BEGIN
    var := XMLTYPE('<booklist type="science and engineering">
  <!--This is the first book node-->
  <book category="math">
    <title>learning math</title>
    <author>Zhang San</author>
    <pageNumber>561</pageNumber>
  </book>
  <!--This is the second book node-->
  <book category="Python">
    <title>learning Python</title>
    <author>Li Si</author>
    <pageNumber>600</pageNumber>
  </book>
  <!--This is the third book node-->
  <book category="C++">
    <title>learning C++</title>
    <author>Wang Er</author>
    <pageNumber>500</pageNumber>
  </book>
</booklist>');
    doc := gms_xmldom.newDOMDocument(var);
    docNode := gms_xmldom.makeNode(doc);
    wclob := gms_xmldom.writeToClob(doc, wclob);
    gms_output.put_line('XML content is:' || wclob);
    -- getDocumentElement
    elemNode := gms_xmldom.getDocumentElement(doc);
    bookListNode := gms_xmldom.getFirstChild(docNode);
    -- getChildrenByTagName
    nodeList := gms_xmldom.getChildrenByTagName(elemNode, 'book');
    node := gms_xmldom.item(nodeList, 0);
    -- getFirstChild, getNodeName
    titleNode := gms_xmldom.getFirstChild(node);
    wclob := gms_xmldom.writeToClob(titleNode, wclob);
    gms_output.put_line(wclob);
    gms_output.put_line('The nodeName is:' || gms_xmldom.getNodeName(titleNode));
    -- The nodeValue of an element node is empty
    gms_output.put_line('The nodeValue is:' || gms_xmldom.getNodeValue(titleNode));
    txt := gms_xmldom.getFirstChild(titleNode);
    textNode := gms_xmldom.makeNode(txt);
    gms_output.put_line('The nodeValue is:' || gms_xmldom.getNodeValue(textNode));
    -- getChildNodes
    nodeList := gms_xmldom.getChildNodes(bookListNode);
    llen := gms_xmldom.getLength(nodeList);
    gms_output.put_line('The length of booklist child nodes is:' || llen );
    FOR i IN 0..(llen-1) LOOP
        node := gms_xmldom.item(nodeList, i);
        -- getNodeType
        IF gms_xmldom.getNodeType(node) = gms_xmldom.COMMENT_NODE THEN
            n := n + 1;
            -- The nodeValue of a comment node
            gms_output.put_line('The ' || n || 'st comment is:' || gms_xmldom.getNodeValue(node));
        END IF;
    END LOOP;
END;
/

Output:
XML content is:<?xml version="1.0" ?>
<booklist type="science and engineering">
  <!--This is the first book node-->
  <book category="math">
    <title>learning math</title>
    <author>Zhang San</author>
    <pageNumber>561</pageNumber>
  </book>
  <!--This is the second book node-->
  <book category="Python">
    <title>learning Python</title>
    <author>Li Si</author>
    <pageNumber>600</pageNumber>
  </book>
  <!--This is the third book node-->
  <book category="C++">
    <title>learning C++</title>
    <author>Wang Er</author>
    <pageNumber>500</pageNumber>
  </book>
</booklist>

<title>learning math</title>

The nodeName is:title
The nodeValue is:
The nodeValue is:learning math
The length of booklist child nodes is:6
The 1st comment is:This is the first book node
The 2nd comment is:This is the second book node
The 3rd comment is:This is the third book node
```