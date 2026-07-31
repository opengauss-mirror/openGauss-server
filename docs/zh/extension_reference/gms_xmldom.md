# gms_xmldom

## gms_xmldom概述

gms_xmldom为openGauss内置基于PL/Python语言实现，将底层的Python XML DOM操作封装成符合Oracle规范的PL/pgSQL函数。包内定义了一系列自定义数据类型，用于在SQL层面表示不同的DOM对象，如DOMDocument, DOMNode, DOMElement等

## gms_xmldom数据类型

**表 1** gms_xmldom数据类型说明

<a name="table1011513101687"></a>
<table><tbody><tr id="row201685101086"><td class="cellrowborder" valign="top" width="30.383038303830386%"><p id="p7168210483"><a name="p7168210483"></a><a name="p7168210483"></a><strong id="b1316817109817"><a name="b1316817109817"></a><a name="b1316817109817"></a>类型名称</strong></p>
</td>
<td class="cellrowborder" valign="top" width="30.243024302430243%"><p id="p1816817101585"><a name="p1816817101585"></a><a name="p1816817101585"></a><strong id="b1016820101589"><a name="b1016820101589"></a><a name="b1016820101589"></a>描述</strong></p>
</td>
<td class="cellrowborder" valign="top" width="39.373937393739375%"><p id="p111687101286"><a name="p111687101286"></a><a name="p111687101286"></a><strong id="b1716911015819"><a name="b1716911015819"></a><a name="b1716911015819"></a>类型</strong></p>
</td>
</tr>
<tr id="row81692010682"><td class="cellrowborder" valign="top" width="30.383038303830386%"><p id="p916919107811"><a name="p916919107811"></a><a name="p916919107811"></a>DOMNode</p>
</td>
<td class="cellrowborder" valign="top" width="30.243024302430243%"><p id="p216911100815"><a name="p216911100815"></a><a name="p216911100815"></a>代表xml文档树中一个单独的节点，可以泛指任何一种类型节点</p>
</td>
<td class="cellrowborder" valign="top" width="39.373937393739375%"><p id="p382419359375"><a name="p382419359375"></a><a name="p382419359375"></a>Node</p>
</td>
</tr>
<tr id="row413211712177"><td class="cellrowborder" valign="top" width="30.383038303830386%"><p id="p813487181720"><a name="p813487181720"></a><a name="p813487181720"></a>DOMDocument类型</p>
</td>
<td class="cellrowborder" valign="top" width="30.243024302430243%"><p id="p1013416713174"><a name="p1013416713174"></a><a name="p1013416713174"></a>Document节点。代表整个xml文档，是文档树的根，并提供了对文档数据访问的顶层入口</p>
</td>
<td class="cellrowborder" valign="top" width="39.373937393739375%"><p id="p173511513616"><a name="p173511513616"></a><a name="p173511513616"></a>Document</p>
</td>
</tr>
<tr id="row413211712177"><td class="cellrowborder" valign="top" width="30.383038303830386%"><p id="p813487181720"><a name="p813487181720"></a><a name="p813487181720"></a>DOMElement</p>
</td>
<td class="cellrowborder" valign="top" width="30.243024302430243%"><p id="p1013416713174"><a name="p1013416713174"></a><a name="p1013416713174"></a>Element节点，代表xml文档中的一个元素，元素可以包含属性，嵌套其它元素或文本</p>
</td>
<td class="cellrowborder" valign="top" width="39.373937393739375%"><p id="p173511513616"><a name="p173511513616"></a><a name="p173511513616"></a>Element</p>
</td>
</tr>
<tr id="row413211712177"><td class="cellrowborder" valign="top" width="30.383038303830386%"><p id="p813487181720"><a name="p813487181720"></a><a name="p813487181720"></a>DOMAttr</p>
</td>
<td class="cellrowborder" valign="top" width="30.243024302430243%"><p id="p1013416713174"><a name="p1013416713174"></a><a name="p1013416713174"></a>Attr节点。表示Element节点中的属性</p>
</td>
<td class="cellrowborder" valign="top" width="39.373937393739375%"><p id="p173511513616"><a name="p173511513616"></a><a name="p173511513616"></a>Attribute</p>
</td>
</tr>
<tr id="row413211712177"><td class="cellrowborder" valign="top" width="30.383038303830386%"><p id="p813487181720"><a name="p813487181720"></a><a name="p813487181720"></a>DOMText</p>
</td>
<td class="cellrowborder" valign="top" width="30.243024302430243%"><p id="p1013416713174"><a name="p1013416713174"></a><a name="p1013416713174"></a>Text节点，表示元素或属性的文本内容</p>
</td>
<td class="cellrowborder" valign="top" width="39.373937393739375%"><p id="p173511513616"><a name="p173511513616"></a><a name="p173511513616"></a>Text</p>
</td>
</tr>
<tr id="row413211712177"><td class="cellrowborder" valign="top" width="30.383038303830386%"><p id="p813487181720"><a name="p813487181720"></a><a name="p813487181720"></a>DOMCDATASection</p>
</td>
<td class="cellrowborder" valign="top" width="30.243024302430243%"><p id="p1013416713174"><a name="p1013416713174"></a><a name="p1013416713174"></a>CDATASection节点，表示xml文档中的CDATA区段，CDATA区段时一段不会被解析器解析的文本</p>
</td>
<td class="cellrowborder" valign="top" width="39.373937393739375%"><p id="p173511513616"><a name="p173511513616"></a><a name="p173511513616"></a>Section</p>
</td>
</tr>
<tr id="row413211712177"><td class="cellrowborder" valign="top" width="30.383038303830386%"><p id="p813487181720"><a name="p813487181720"></a><a name="p813487181720"></a>DOMComment</p>
</td>
<td class="cellrowborder" valign="top" width="30.243024302430243%"><p id="p1013416713174"><a name="p1013416713174"></a><a name="p1013416713174"></a>Comment节点。表示xml文档中注释节点的内容</p>
</td>
<td class="cellrowborder" valign="top" width="39.373937393739375%"><p id="p173511513616"><a name="p173511513616"></a><a name="p173511513616"></a>Comment</p>
</td>
</tr>
<tr id="row413211712177"><td class="cellrowborder" valign="top" width="30.383038303830386%"><p id="p813487181720"><a name="p813487181720"></a><a name="p813487181720"></a>DOMEntity</p>
</td>
<td class="cellrowborder" valign="top" width="30.243024302430243%"><p id="p1013416713174"><a name="p1013416713174"></a><a name="p1013416713174"></a>Entity节点，在xml文档中频繁使用某一条数据时，可以预定义一个这条数据的“别名”，即一个Entity，然后在文档中需要的地方进行调用</p>
</td>
<td class="cellrowborder" valign="top" width="39.373937393739375%"><p id="p173511513616"><a name="p173511513616"></a><a name="p173511513616"></a>Entity</p>
</td>
</tr>
<tr id="row413211712177"><td class="cellrowborder" valign="top" width="30.383038303830386%"><p id="p813487181720"><a name="p813487181720"></a><a name="p813487181720"></a>DOMDocumentFragment</p>
</td>
<td class="cellrowborder" valign="top" width="30.243024302430243%"><p id="p1013416713174"><a name="p1013416713174"></a><a name="p1013416713174"></a>DocumentFragment节点，文档中的一部分，表示一个或多个邻接的Document节点和它们的所有子孙节点，注意DocumentFragment节点不属于文档树</p>
</td>
<td class="cellrowborder" valign="top" width="39.373937393739375%"><p id="p173511513616"><a name="p173511513616"></a><a name="p173511513616"></a>Fragment</p>
</td>
</tr>
<tr id="row413211712177"><td class="cellrowborder" valign="top" width="30.383038303830386%"><p id="p813487181720"><a name="p813487181720"></a><a name="p813487181720"></a>DOMNotation</p>
</td>
<td class="cellrowborder" valign="top" width="30.243024302430243%"><p id="p1013416713174"><a name="p1013416713174"></a><a name="p1013416713174"></a>Notation元素，Notation元素描述xml文档中非xml数据的格式</p>
</td>
<td class="cellrowborder" valign="top" width="39.373937393739375%"><p id="p173511513616"><a name="p173511513616"></a><a name="p173511513616"></a>Notation</p>
</td>
</tr>
<tr id="row413211712177"><td class="cellrowborder" valign="top" width="30.383038303830386%"><p id="p813487181720"><a name="p813487181720"></a><a name="p813487181720"></a>DOMProcessingInstruction</p>
</td>
<td class="cellrowborder" valign="top" width="30.243024302430243%"><p id="p1013416713174"><a name="p1013416713174"></a><a name="p1013416713174"></a>ProcessingInstruction节点，表示xml文档中的一个处理指令</p>
</td>
<td class="cellrowborder" valign="top" width="39.373937393739375%"><p id="p173511513616"><a name="p173511513616"></a><a name="p173511513616"></a>ProcessingInstruction
</p>
</td>
</tr>
<tr id="row413211712177"><td class="cellrowborder" valign="top" width="30.383038303830386%"><p id="p813487181720"><a name="p813487181720"></a><a name="p813487181720"></a>DOMDocumentType</p>
</td>
<td class="cellrowborder" valign="top" width="30.243024302430243%"><p id="p1013416713174"><a name="p1013416713174"></a><a name="p1013416713174"></a>DocumentType节点，每个xml文档均有一个DOCTYPE属性，此属性的值可为空，也可以是一个DocumentType对象。 DocumentType对象为xml定义的实体提供接口</p>
</td>
<td class="cellrowborder" valign="top" width="39.373937393739375%"><p id="p173511513616"><a name="p173511513616"></a><a name="p173511513616"></a>DocumentType</p>
</td>
</tr>
</tbody>
</table>

## gms_xmldom支持接口

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
  - freeNode(n gms_xmldom.DOMnode)
+ gms_xmldom.freeNodeList
  - freeNodeList(nl gms_xmldom.DOMNodeList)
+ gms_xmldom.freeDocument
  - freeDocument(doc gms_xmldom.DOMDocument)
+ gms_xmldom.getFirstChild
  - getFirstChild(n gms_xmldom.DOMNode)
+ gms_xmldom.getLocalName
  - getLocalName(a gms_xmldom.DOMAttr)
  - getLocalName(elem gms_xmldom.DOMElement)
  - getLocalName(n gms_xmldom.DOMnode, data OUT VARCHAR2)
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
  - getElementsByTagName(elem gms_xmldom.DOMElement, name IN VARCHAR2, ns varchar2)
  - getElementsByTagName(doc gms_xmldom.DOMDocument, tagname IN VARCHAR2)
+ gms_xmldom.cloneNode
  - cloneNode(n gms_xmldom.DOMNode, deep boolean)
+ gms_xmldom.getNodeName
  - getNodeName(n gms_xmldom.DOMNode)
+ gms_xmldom.createDocument
  - createDocument(namespaceuri IN VARCHAR2, qualifiedname IN VARCHAR2, doctype IN gms_xmldom.DOMType:= NULL)
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
  - getNodeValueAsClob(n gms_xmldom.Domnode)
+ gms_xmldom.getChildrenByTagName
  - getChildrenByTagName(elem gms_xmldom.DOMElement, name varchar2)
  - getChildrenByTagName(elem gms_xmldom.DOMElement, name varchar2, ns varchar2)
+ gms_xmldom.getOwnerDocument
  - getOwnerDocument(n gms_xmldom.DOMNode)
+ gms_xmldom.newDOMDocument
  - newDOMDocument()
  - newDOMDocument(xmldoc IN xmltype)
  - newDOMDocument(cl IN clob)
+ gms_xmldom.hasChildNodes
  - hasChildNodes(n gms_xmldom.DOMNode)
+ gms_xmldom.setVersion
  - setVersion(doc gms_xmldom.DOMDocument, version VARCHAR2)
+ gms_xmldom.makeCharacterData
  - makeCharacterData(gms_xmldom.DOMNode)

## gms_xmldom应用注意事项

由于gms_xmldom底层实现依赖于plpython3u插件，所以 

1. openGauss编译环境中需安装或在编译依赖的第三方工具集集成`python3`,且版本大于3.7
2. openGauss使用automake配置编译参数时需新增`--with-python`参数
3. openGauss使用cmake配置参数需设置`-DENABLE_PYTHON3=ON`参数
4. 安装openGauss后，需指定环境变量`PYTHONHOME`为`GAUSSHOME`目录下的`python`
5. 安装openGauss后，环境变量`LD_LIBRARY_PATH`中需新增目录`$GAUSSHOME/python/lib64`
6. openGauss的小型化版本不支持`plpython3u`插件，也无法使用`gms_xmldom API package`
7. plpython3u插件不支持`set schema`操作， 任何相关操作均会报错，显示不支持

## gms_xmldom 安装

对于`gms_xmldom`的安装只需安装 `plpython3u`即可使用对应的接口集

```
create extension plpython3u;

```

## gms_xmldom 卸载

对于`gms_xmldom`的卸载只需卸载 `plpython3u`即可屏蔽对应的接口集

```
drop extension plpython3u;

```

## gms_xmldom 示例

### case 1 创建一个空的xml文档并插入元素节点构建文档
```
create extension plpython3u;

DECLARE
    doc gms_xmldom.DOMDocument;
    elem gms_xmldom.DOMElement;
    root gms_xmldom.DOMNode;
    elemNode gms_xmldom.DOMNode;
    cl clob;
    appResNode gms_xmldom.DOMNode;
BEGIN
    set serveroutput on;
    doc := gms_xmldom.newDomDocument;
    root := gms_xmldom.makeNode(doc);
    elem := gms_xmldom.createElement(doc, 'root');
    elemNode := gms_xmldom.makeNode(elem);
    appResNode := gms_xmldom.appendChild(root, elemNode);
    cl := gms_xmldom.writeToClob(doc, cl);
    gms_output.put_line(cl);
END;
/

输出结果：
<?xml version="1.0" ?>
<root/>
```
### case 2 根据手动输入的clob或xmltype类型的字符串，构造xml文档

```
create extension plpython3u;

DECLARE
    doc gms_xmldom.DOMDocument;
    cl clob;
    x xmltype;
BEGIN
    set serveroutput on;
    x := xmltype('<PERSON><NAME>ramesh</NAME></PERSON>');
    doc := gms_xmldom.newDomDocument(x);
    cl := gms_xmldom.writeToClob(doc, cl);
    gms_output.put_line(cl);
END;
/

输出结果：
<?xml version="1.0" ?>
<PERSON>
  <NAME>ramesh</NAME>
</PERSON>
```

### case 3 构造一个包含namespace的xml文档，并插入节点

```
create extension plpython3u;

DECLARE
    doc gms_xmldom.DOMDocument;
    rootElem gms_xmldom.DOMElement;
    rootNode gms_xmldom.DOMNode;
    elem gms_xmldom.DOMElement;
    elemNode gms_xmldom.DOMNode;
    wclob clob;
    resNode gms_xmldom.DOMNode;
BEGIN
    doc := gms_xmldom.createDocument('http://www.runoob.com/xml/', 'xml', null);
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
    wclob :=gms_xmldom.writeToClob(doc, wclob);
    --输出clob内容  
    gms_output.put_line(wclob);
END;
/

输出结果：
<?xml version="1.0" ?>
<xml>
  <head id="headDoc"/>
  <body id="bodyDoc"/>
</xml>

```

### case 4 创建节点，并插入到xml文档中

```
create extension plpython3u;

DECLARE
    var xmltype;
    doc gms_xmldom.DOMDocument;
    docNode gms_xmldom.DOMNode;
    bookListNode gms_xmldom.DOMNode;
    nodeList gms_xmldom.DOMNodelist;
    node gms_xmldom.DOMNODE;
    comment gms_xmldom.DOMComment;    
    procInstruc gms_xmldom.DOMProcessingInstruction;
    elem gms_xmldom.DOMElement;
    txt gms_xmldom.DOMText;
    attr gms_xmldom.DOMAttr;
    wclob clob;
    isNull boolean;
    makeNode1 gms_xmldom.DOMNode;
    makeNode2 gms_xmldom.DOMNode;
    resNode gms_xmldom.DOMNode;
BEGIN
    var := xmltype('<booklist type="science and engineering">
  <book category="math">
    <title>learning math</title>
    <author>张三</author>
    <pageNumber>561</pageNumber>
  </book>
</booklist>');
    doc := gms_xmldom.newDOMDocument(var);
    docNode := gms_xmldom.makeNode(doc);
    bookListNode := gms_xmldom.getFirstChild(docNode);
    nodeList := gms_xmldom.getElementsByTagName(doc, 'book');
    node := gms_xmldom.item(nodeList, 0);
    --创建和插入comment节点
    comment := gms_xmldom.createComment(doc, 'This is the introduction of books');
    isNull := gms_xmldom.isNull(comment);
    gms_output.put_line('DOMComment : ' || case when isNull then 'Y' else 'N' end);
    makeNode1 := gms_xmldom.makeNode(comment);
    resNode := gms_xmldom.insertBefore(bookListNode, makeNode1, node);
    --创建和插入ProcessingInstruction节点
    procInstruc := gms_xmldom.createProcessingInstruction(doc, 'xml', 'version="2.0"');
    makeNode1 := gms_xmldom.makeNode(procInstruc);
    resNode := gms_xmldom.insertBefore(docNode, makeNode1, bookListNode);
    --创建和插入text节点
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
    --输出修改后的clob内容  
    gms_output.put_line(wclob);
END;
/

输出结果：

DOMComment : N
<?xml version="1.0" ?>
<?xml version="2.0"?>
<booklist type="science and engineering">
  <!--This is the introduction of books-->
  <book category="math">
    <title>learning math</title>
    <author>张三</author>
    <pageNumber>561</pageNumber>
  </book>
  <book category="">
    <title>learning python</title>
  </book>
</booklist>

```

### case 5 根据现有的xml文档，获取节点信息

```
create extension plpython3u;

DECLARE
    var xmltype;
    doc gms_xmldom.DOMDocument;
    docNode gms_xmldom.DOMNode;
    bookListNode gms_xmldom.DOMNode;
    nodeList gms_xmldom.DOMNodeList;
    node gms_xmldom.DOMNode;
    titleNode gms_xmldom.DOMNode;
    elemNode gms_xmldom.DOMElement;
    txt gms_xmldom.DOMText;
    textNode gms_xmldom.DOMNode;
    wclob clob;
    llen integer;
    n integer := 0;
BEGIN
    var := xmltype('<booklist type="science and engineering">
  <!--这是第一个book节点-->
  <book category="math">
    <title>learning math</title>
    <author>张三</author>
    <pageNumber>561</pageNumber>
  </book>
  <!--这是第二个book节点-->
  <book category="Python">
    <title>learning Python</title>
    <author>李四</author>
    <pageNumber>600</pageNumber>
  </book>
  <!--这是第三个book节点-->
  <book category="C++">
    <title>learning C++</title>
    <author>王二</author>
    <pageNumber>500</pageNumber>
  </book>
</booklist>');
    doc := gms_xmldom.newDOMDocument(var);
    docNode := gms_xmldom.makeNode(doc);
    wclob := gms_xmldom.writeToClob(doc, wclob);
    gms_output.put_line('xml内容是:' || wclob);
    --getDocumentElement
    elemNode := gms_xmldom.getDocumentElement(doc);
    bookListNode := gms_xmldom.getFirstChild(docNode);
    --getChildrenByTagName
    nodeList := gms_xmldom.getChildrenByTagName(elemNode, 'book');
    node := gms_xmldom.item(nodeList, 0);
    --getFirstChild，getNodeName
    titleNode := gms_xmldom.getFirstChild(node);
    wclob := gms_xmldom.writeToClob(titleNode, wclob);
    gms_output.put_line(wclob);
    gms_output.put_line('The nodeName is:' || gms_xmldom.getNodeName(titleNode));
    --element节点的nodeValue，为空
    gms_output.put_line('The nodeValue is:' || gms_xmldom.getNodeValue(titleNode));
    txt := gms_xmldom.getFirstChild(titleNode);
    textNode := gms_xmldom.makeNode(txt);
    gms_output.put_line('The nodeValue is:' || gms_xmldom.getNodeValue(textNode));
    --getChildNodes
    nodeList := gms_xmldom.getChildNodes(bookListNode);
    llen := gms_xmldom.getLength(nodeList);
    gms_output.put_line('booklist子节点长度为:' || llen );
    for i in 0..(llen-1) loop
        node := gms_xmldom.item(nodeList, i);
        --getNodeType
        if gms_xmldom.getNodeType(node) = gms_xmldom.COMMENT_NODE then
            n := n+1;
            --comment节点的nodeValue
            gms_output.put_line('第'|| n || '个备注为：'||gms_xmldom.getNodeValue(node));
        end if;
    end loop;
END;
/

输出结果：

xml内容是:<?xml version="1.0" ?>
<booklist type="science and engineering">
  <!--这是第一个book节点-->
  <book category="math">
    <title>learning math</title>
    <author>张三</author>
    <pageNumber>561</pageNumber>
  </book>
  <!--这是第二个book节点-->
  <book category="Python">
    <title>learning Python</title>
    <author>李四</author>
    <pageNumber>600</pageNumber>
  </book>
  <!--这是第三个book节点-->
  <book category="C++">
    <title>learning C++</title>
    <author>王二</author>
    <pageNumber>500</pageNumber>
  </book>
</booklist>

<title>learning math</title>

The nodeName is:title
The nodeValue is:
The nodeValue is:learning math
booklist子节点长度为:6
第1个备注为：这是第一个book节点
第2个备注为：这是第二个book节点
第3个备注为：这是第三个book节点
```
