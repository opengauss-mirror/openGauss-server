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

gms_xmldom 共提供 38 个接口（含多个重载），按功能可分为节点转换、节点/文档创建、节点查询、节点修改、资源管理与输出 6 类，接口归类如下：

| 分类 | 接口 |
| --- | --- |
| 节点转换 | makeNode、makeElement、makeCharacterData |
| 节点/文档创建 | createDocument、createElement、createDocumentFragment、createTextNode、createComment、createCDATASection、createProcessingInstruction、createAttribute、newDOMDocument |
| 节点查询 | getFirstChild、getChildNodes、getElementsByTagName、getChildrenByTagName、getAttributes、getNodeName、getNodeValue、getNodeValueAsClob、getLocalName、getNodeType、getLength、item、getDocumentElement、getOwnerDocument、hasChildNodes |
| 节点修改 | appendChild、insertBefore、setAttribute、setAttributeNode、setVersion、setnodevalue、cloneNode |
| 资源管理 | isNull、freeNode、freeNodeList、freeDocument |
| 输出 | writeToClob、writeToBuffer |

> 说明：gms_xmldom 中 Document、Element、Attr 等类型被定义为 Node 的子类型，而 PL/Python 不支持类型继承，因此所有节点操作统一通过 `makeNode` 转换为 `DOMNode` 后进行，需要特定类型能力时再通过 `makeElement`、`makeCharacterData` 等转换回原类型。

### gms_xmldom.makeNode
**功能描述**：将其他 DOM 节点类型转换成 `DOMNode` 类型，以便统一调用 DOMNode 的方法。对各类 DOM 类型调用 `makeNode` 可将这些类型转换为一个 `DOMNode`，是所有节点操作的前提。

**语法格式**：
```auto
makeNode(t gms_xmldom.DOMText) RETURN gms_xmldom.DOMNode;
makeNode(com gms_xmldom.DOMComment) RETURN gms_xmldom.DOMNode;
makeNode(cds gms_xmldom.DOMCDATASection) RETURN gms_xmldom.DOMNode;
makeNode(dt gms_xmldom.DOMDocumentType) RETURN gms_xmldom.DOMNode;
makeNode(n gms_xmldom.DOMNotation) RETURN gms_xmldom.DOMNode;
makeNode(ent gms_xmldom.DOMEntity) RETURN gms_xmldom.DOMNode;
makeNode(pi gms_xmldom.DOMProcessingInstruction) RETURN gms_xmldom.DOMNode;
makeNode(df gms_xmldom.DOMDocumentFragment) RETURN gms_xmldom.DOMNode;
makeNode(doc gms_xmldom.DOMDocument) RETURN gms_xmldom.DOMNode;
makeNode(elem gms_xmldom.DOMElement) RETURN gms_xmldom.DOMNode;
```

**参数说明**：
- 输入（均为待转换的对应类型对象）：
  - `t`：待转换的 DOMText 对象
  - `com`：待转换的 DOMComment 对象
  - `cds`：待转换的 DOMCDATASection 对象
  - `dt`：待转换的 DOMDocumentType 对象
  - `n`：待转换的 DOMNotation 对象
  - `ent`：待转换的 DOMEntity 对象
  - `pi`：待转换的 DOMProcessingInstruction 对象
  - `df`：待转换的 DOMDocumentFragment 对象
  - `doc`：待转换的 DOMDocument 对象
  - `elem`：待转换的 DOMElement 对象

**返回值**：`gms_xmldom.DOMNode`，转换后的 DOMNode 对象。

### gms_xmldom.isNull
**功能描述**：检查输入的对象是否为空（NULL），为空返回 `TRUE`，否则返回 `FALSE`。常用于判断节点/文档对象是否创建成功。

**语法格式**（对每种 DOM 类型均有重载）：
```auto
isNull(n gms_xmldom.DOMNode) RETURN BOOLEAN;
isNull(di gms_xmldom.DOMImplementation) RETURN BOOLEAN;
isNull(nl gms_xmldom.DOMNodeList) RETURN BOOLEAN;
isNull(nnm gms_xmldom.DOMNamedNodeMap) RETURN BOOLEAN;
isNull(cd gms_xmldom.DOMCharacterData) RETURN BOOLEAN;
isNull(a gms_xmldom.DOMAttr) RETURN BOOLEAN;
isNull(elem gms_xmldom.DOMElement) RETURN BOOLEAN;
isNull(t gms_xmldom.DOMText) RETURN BOOLEAN;
isNull(com gms_xmldom.DOMComment) RETURN BOOLEAN;
isNull(cds gms_xmldom.DOMCDATASection) RETURN BOOLEAN;
isNull(dt gms_xmldom.DOMDocumentType) RETURN BOOLEAN;
isNull(n gms_xmldom.DOMNotation) RETURN BOOLEAN;
isNull(ent gms_xmldom.DOMEntity) RETURN BOOLEAN;
isNull(pi gms_xmldom.DOMProcessingInstruction) RETURN BOOLEAN;
isNull(df gms_xmldom.DOMDocumentFragment) RETURN BOOLEAN;
isNull(doc gms_xmldom.DOMDocument) RETURN BOOLEAN;
```

**参数说明**：
- 输入：`n` / `di` / `nl` / `nnm` / `cd` / `a` / `elem` / `t` / `com` / `cds` / `dt` / `ent` / `pi` / `df` / `doc`——待检查的对应类型对象。

**返回值**：`BOOLEAN`。对象为空返回 `TRUE`，非空返回 `FALSE`。

### gms_xmldom.freeNode
**功能描述**：释放与 `DOMNode` 关联的所有资源。节点使用完毕后建议调用，避免资源泄漏。

**语法格式**：
```auto
freeNode(n gms_xmldom.DOMNode);
```

**参数说明**：
- 输入：`n`——待释放的 DOMNode 对象。

**返回值**：无（过程）。

### gms_xmldom.freeNodeList
**功能描述**：释放与节点列表 `DOMNodeList` 关联的所有资源。

**语法格式**：
```auto
freeNodeList(nl gms_xmldom.DOMNodeList);
```

**参数说明**：
- 输入：`nl`——待释放的节点列表对象。

**返回值**：无（过程）。

### gms_xmldom.freeDocument
**功能描述**：释放一个 `DOMDocument` 对象的所有资源。

**语法格式**：
```auto
freeDocument(doc gms_xmldom.DOMDocument);
```

**参数说明**：
- 输入：`doc`——待释放的 XML 文档对象。

**返回值**：无（过程）。

### gms_xmldom.getFirstChild
**功能描述**：返回此节点的第一个子节点；如果没有子节点，则返回 `NULL`。

**语法格式**：
```auto
getFirstChild(n gms_xmldom.DOMNode) RETURN gms_xmldom.DOMNode;
```

**参数说明**：
- 输入：`n`——目标 DOMNode 节点。

**返回值**：`gms_xmldom.DOMNode`，第一个子节点；无子节点时返回 `NULL`。

### gms_xmldom.getLocalName
**功能描述**：返回节点名称的本地部分（不含命名空间前缀的部分）。

**语法格式**：
```auto
getLocalName(a gms_xmldom.DOMAttr) RETURN VARCHAR2;
getLocalName(elem gms_xmldom.DOMElement) RETURN VARCHAR2;
getLocalName(n gms_xmldom.DOMNode, data OUT VARCHAR2);
```

**参数说明**：
- 输入：
  - `a`：DOMAttr 对象
  - `elem`：DOMElement 对象
  - `n`：DOMNode 对象
- 输出（第 3 个重载）：`data OUT VARCHAR2`——节点名称的本地部分。

**返回值**：`VARCHAR2`，节点名称的本地部分。

### gms_xmldom.getNodeType
**功能描述**：返回节点类型（对应各节点类型常量，如 `ELEMENT_NODE`、`TEXT_NODE`、`COMMENT_NODE` 等）。

**语法格式**：
```auto
getNodeType(n gms_xmldom.DOMNode) RETURN INTEGER;
```

**参数说明**：
- 输入：`n`——目标 DOMNode 对象。

**返回值**：`INTEGER`，节点类型。

### gms_xmldom.writeToClob
**功能描述**：使用数据库字符集将 XML 节点或 XML 文档写入指定的 CLOB 对象。

**语法格式**：
```auto
writeToClob(n gms_xmldom.DOMNode, cl IN OUT CLOB);
writeToClob(n gms_xmldom.DOMNode, cl IN OUT CLOB, pflag IN NUMBER, indent IN NUMBER);
writeToClob(doc gms_xmldom.DOMDocument, cl IN OUT CLOB);
writeToClob(doc gms_xmldom.DOMDocument, cl IN OUT CLOB, pflag IN NUMBER, indent IN NUMBER);
```

**参数说明**：
- 输入：
  - `n`：DOMNode 节点对象（待输出的节点）
  - `doc`：XML 文档对象（待输出的文档）
  - `cl`：指定的 CLOB 对象（IN OUT，输出内容写入该变量）
  - `pflag`：换行标识。取值范围为 0-72 的整数，所有低三位为 4 或 5 的值表示不换行，其他值表示换行。该参数在 Oracle 官方文档中无明确说明，此处取值范围与行为经实测确认。
  - `indent`：缩进长度。取值范围为 0-12 的整数，数值大小表示缩进的空格个数。缩进只在 pflag 为换行时生效。

**返回值**：`CLOB`，写入后的 clob 对象（内容通过 IN OUT 参数返回）。

### gms_xmldom.writeToBuffer
**功能描述**：使用数据库字符集将 XML 节点、XML 文档或文档片段写入指定的缓冲区（VARCHAR2）。

**语法格式**：
```auto
writeToBuffer(n gms_xmldom.DOMNode, buffer IN OUT VARCHAR2);
writeToBuffer(doc gms_xmldom.DOMDocument, buffer IN OUT VARCHAR2);
writeToBuffer(df gms_xmldom.DOMDocumentFragment, buffer IN OUT VARCHAR2);
```

**参数说明**：
- 输入：
  - `n`：DOMNode 节点对象
  - `doc`：XML 文档对象
  - `df`：DOMDocumentFragment 对象
  - `buffer`：指定的缓冲区（IN OUT，输出内容写入该变量）

**返回值**：`VARCHAR2`，写入后的缓冲区内容（通过 IN OUT 参数返回）。

### gms_xmldom.getChildNodes
**功能描述**：返回当前节点的所有子节点，类型为 `DOMNodeList`。

**语法格式**：
```auto
getChildNodes(n gms_xmldom.DOMNode) RETURN gms_xmldom.DOMNodeList;
```

**参数说明**：
- 输入：`n`——目标 DOMNode 节点对象。

**返回值**：`gms_xmldom.DOMNodeList`，当前节点的所有子节点。

### gms_xmldom.getLength
**功能描述**：返回当前对象的长度。对 `DOMNodeList`/`DOMNamedNodeMap` 返回节点数量；对 `DOMCharacterData` 返回字符数据的长度。

**语法格式**：
```auto
getLength(nl gms_xmldom.DOMNodeList) RETURN PLS_INTEGER;
getLength(nnm gms_xmldom.DOMNamedNodeMap) RETURN PLS_INTEGER;
getLength(cd gms_xmldom.DOMCharacterData) RETURN PLS_INTEGER;
```

**参数说明**：
- 输入：
  - `nl`：DOMNodeList 对象
  - `nnm`：DOMNamedNodeMap 对象
  - `cd`：DOMCharacterData 对象

**返回值**：`PLS_INTEGER`，指定对象的长度。

### gms_xmldom.item
**功能描述**：返回节点列表或 DOMNamedNodeMap 无序列表中 `idx` 参数对应的项。如果 `idx` 大于或等于列表中的节点数，则返回空。

**语法格式**：
```auto
item(nl gms_xmldom.DOMNodeList, idx IN PLS_INTEGER) RETURN gms_xmldom.DOMNode;
item(nnm gms_xmldom.DOMNamedNodeMap, idx IN PLS_INTEGER) RETURN gms_xmldom.DOMNode;
```

**参数说明**：
- 输入：
  - `nl`：节点列表 DOMNodeList
  - `nnm`：DOMNamedNodeMap 无序列表
  - `idx`：在列表中要检索的索引值（从 0 开始）

**返回值**：`gms_xmldom.DOMNode`，索引值对应的节点；索引越界返回空。

### gms_xmldom.makeElement
**功能描述**：将 `DOMNode` 节点转换成 `DOMElement` 节点类型。该函数主要用于将 `DOMElement` 转换成的 `DOMNode` 转换回 `DOMElement`，因此除了节点类型为 `ELEMENT_NODE` 的节点，其他节点类型不能调用该函数。

**语法格式**：
```auto
makeElement(n gms_xmldom.DOMNode) RETURN gms_xmldom.DOMElement;
```

**参数说明**：
- 输入：`n`——DOMNode 对象（须为 ELEMENT_NODE 类型）。

**返回值**：`gms_xmldom.DOMElement`，转换后的 DOMElement 对象。

### gms_xmldom.getElementsByTagName
**功能描述**：返回包含指定名称的所有 `DOMElement` 的 `DOMNodeList`（按标签名在整棵子树中查找，含嵌套子节点）。

**语法格式**：
```auto
getElementsByTagName(elem gms_xmldom.DOMElement, name IN VARCHAR2) RETURN gms_xmldom.DOMNodeList;
getElementsByTagName(elem gms_xmldom.DOMElement, name IN VARCHAR2, ns VARCHAR2) RETURN gms_xmldom.DOMNodeList;
getElementsByTagName(doc gms_xmldom.DOMDocument, tagname IN VARCHAR2) RETURN gms_xmldom.DOMNodeList;
```

**参数说明**：
- 输入：
  - `elem`：DOMElement 对象（搜索起点元素）
  - `doc`：DOMDocument 对象（搜索起点文档）
  - `name` / `tagname`：指定名称（标签名）
  - `ns`：指定的命名空间 URI

**返回值**：`gms_xmldom.DOMNodeList`，包含指定名称的节点列表。

### gms_xmldom.cloneNode
**功能描述**：返回此节点的副本，并用作节点的通用复制构造函数。拷贝出的节点没有父节点，父节点为空。

**语法格式**：
```auto
cloneNode(n gms_xmldom.DOMNode, deep BOOLEAN) RETURN gms_xmldom.DOMNode;
```

**参数说明**：
- 输入：
  - `n`：DOMNode 对象（待拷贝节点）
  - `deep`：是否拷贝子节点。TRUE 为深拷贝（连同子孙节点一起拷贝），FALSE 为浅拷贝（仅拷贝节点本身）。

**返回值**：`gms_xmldom.DOMNode`，拷贝后的 DOMNode 节点。

### gms_xmldom.getNodeName
**功能描述**：返回节点的节点名称。

**语法格式**：
```auto
getNodeName(n gms_xmldom.DOMNode) RETURN VARCHAR2;
```

**参数说明**：
- 输入：`n`——目标 DOMNode 对象。

**返回值**：`VARCHAR2`，节点名称。

### gms_xmldom.createDocument
**功能描述**：通过指定的命名空间 URI、根元素名和 doctype 创建一个 XML 文档。

**语法格式**：
```auto
createDocument(
    namespaceuri IN VARCHAR2,
    qualifiedname IN VARCHAR2,
    doctype IN gms_xmldom.DOMType := NULL
) RETURN gms_xmldom.DOMDocument;
```

**参数说明**：
- 输入：
  - `namespaceuri`：命名空间 URI（可传 NULL）
  - `qualifiedname`：根元素名
  - `doctype`：documentType 对象，默认 NULL。

**返回值**：`gms_xmldom.DOMDocument`，新建的 XML 文档对象。

### gms_xmldom.createElement
**功能描述**：用于在指定文档中创建一个 `DOMElement` 节点（创建后尚未挂入文档树，需通过 appendChild 等挂载）。

**语法格式**：
```auto
createElement(doc gms_xmldom.DOMDocument, tagname IN VARCHAR2) RETURN gms_xmldom.DOMElement;
createElement(doc gms_xmldom.DOMDocument, tagname IN VARCHAR2, ns IN VARCHAR2) RETURN gms_xmldom.DOMElement;
```

**参数说明**：
- 输入：
  - `doc`：XML 文档对象（节点所属文档）
  - `tagname`：DOMElement 节点的名称（标签名）
  - `ns`：命名空间 URI

**返回值**：`gms_xmldom.DOMElement`，新建的 DOMElement 对象。

### gms_xmldom.createDocumentFragment
**功能描述**：用于创建一个 `DOMDocumentFragment` 节点（轻量级容器，可暂存一部分文档结构；注意该容器不属于主文档树，但可包含节点及其子孙）。

**语法格式**：
```auto
createDocumentFragment(doc gms_xmldom.DOMDocument) RETURN gms_xmldom.DOMDocumentFragment;
```

**参数说明**：
- 输入：`doc`——XML 文档对象。

**返回值**：`gms_xmldom.DOMDocumentFragment`，新建的 DocumentFragment 对象。

### gms_xmldom.createTextNode
**功能描述**：用于创建一个 `DOMText` 节点（文本节点，表示元素的文本内容）。

**语法格式**：
```auto
createTextNode(doc gms_xmldom.DOMDocument, data IN VARCHAR2) RETURN gms_xmldom.DOMText;
```

**参数说明**：
- 输入：
  - `doc`：XML 文档对象
  - `data`：DOMText 节点的内容（文本）

**返回值**：`gms_xmldom.DOMText`，新建的 DOMText 对象。

### gms_xmldom.createComment
**功能描述**：用于创建一个 `DOMComment` 节点（注释节点）。

**语法格式**：
```auto
createComment(doc gms_xmldom.DOMDocument, data IN VARCHAR2) RETURN gms_xmldom.DOMComment;
```

**参数说明**：
- 输入：
  - `doc`：XML 文档对象
  - `data`：DOMComment 节点的内容（注释文本）

**返回值**：`gms_xmldom.DOMComment`，新建的 DOMComment 对象。

### gms_xmldom.createCDATASection
**功能描述**：用于创建一个 `DOMCDATASection` 节点（CDATA 区段，内容不会被 XML 解析器解析，可包含 `<`、`>`、`&` 等特殊字符）。

**语法格式**：
```auto
createCDATASection(doc gms_xmldom.DOMDocument, data IN VARCHAR2) RETURN gms_xmldom.DOMCDATASection;
```

**参数说明**：
- 输入：
  - `doc`：XML 文档对象
  - `data`：DOMCDATASection 节点的内容

**返回值**：`gms_xmldom.DOMCDATASection`，新建的 CDATASection 对象。

### gms_xmldom.createProcessingInstruction
**功能描述**：用于创建一个 `DOMProcessingInstruction` 节点（处理指令，向处理 XML 的应用程序传递信息或指令）。

**语法格式**：
```auto
createProcessingInstruction(doc gms_xmldom.DOMDocument, target IN VARCHAR2, data IN VARCHAR2) RETURN gms_xmldom.DOMProcessingInstruction;
```

**参数说明**：
- 输入：
  - `doc`：XML 文档对象
  - `target`：处理指令的目标
  - `data`：处理指令的内容文本

**返回值**：`gms_xmldom.DOMProcessingInstruction`，新建的处理指令对象。

### gms_xmldom.createAttribute
**功能描述**：用于创建一个 `DOMAttr` 属性节点（创建后可配合 setAttributeNode 挂到元素上）。

**语法格式**：
```auto
createAttribute(doc gms_xmldom.DOMDocument, name IN VARCHAR2) RETURN gms_xmldom.DOMAttr;
createAttribute(doc gms_xmldom.DOMDocument, name IN VARCHAR2, ns IN VARCHAR2) RETURN gms_xmldom.DOMAttr;
```

**参数说明**：
- 输入：
  - `doc`：XML 文档对象
  - `name`：属性的名称
  - `ns`：命名空间 URI

**返回值**：`gms_xmldom.DOMAttr`，新建的属性节点对象。

### gms_xmldom.appendChild
**功能描述**：用于将节点 `newchild` 添加到该节点子节点列表的末尾，并返回新添加的节点。如果 `newchild` 已经在树中，则先删除再添加（即移动节点）。

**语法格式**：
```auto
appendChild(n gms_xmldom.DOMNode, newchild IN gms_xmldom.DOMNode) RETURN gms_xmldom.DOMNode;
```

**参数说明**：
- 输入：
  - `n`：父 DOMNode 节点（目标节点）
  - `newchild`：待添加的子节点

**返回值**：`gms_xmldom.DOMNode`，新添加的子节点。

### gms_xmldom.getDocumentElement
**功能描述**：用于返回 XML 文档的根元素节点。

**语法格式**：
```auto
getDocumentElement(doc gms_xmldom.DOMDocument) RETURN gms_xmldom.DOMElement;
```

**参数说明**：
- 输入：`doc`——XML 文档对象。

**返回值**：`gms_xmldom.DOMElement`，文档的根元素节点。

### gms_xmldom.setAttribute
**功能描述**：用于通过名称设置 `DOMElement` 属性的值（属性不存在则创建，存在则覆盖）。

**语法格式**：
```auto
setAttribute(elem gms_xmldom.DOMElement, name IN VARCHAR2, newvalue IN VARCHAR2);
setAttribute(elem gms_xmldom.DOMElement, name IN VARCHAR2, newvalue IN VARCHAR2, ns IN VARCHAR2);
```

**参数说明**：
- 输入：
  - `elem`：DOMElement 节点（目标元素）
  - `name`：属性名
  - `newvalue`：待设置的值
  - `ns`：命名空间 URI

**返回值**：无（过程）。

### gms_xmldom.setAttributeNode
**功能描述**：用于将一个属性节点添加到指定的元素节点中。如果该元素已经存在同名属性，则会替换旧的属性。

**语法格式**：
```auto
setAttributeNode(elem gms_xmldom.DOMElement, newattr IN gms_xmldom.DOMAttr);
setAttributeNode(elem gms_xmldom.DOMElement, newattr IN gms_xmldom.DOMAttr, ns IN VARCHAR2);
```

**参数说明**：
- 输入：
  - `elem`：DOMElement 节点（目标元素）
  - `newattr`：要添加或替换的属性节点
  - `ns`：命名空间 URI

**返回值**：无（过程）。

### gms_xmldom.getAttributes
**功能描述**：用于获取指定元素节点的所有属性，返回一个 `DOMNamedNodeMap` 对象，其中包含该元素的所有属性节点（可按名称或索引访问）。

**语法格式**：
```auto
getAttributes(n gms_xmldom.DOMNode) RETURN gms_xmldom.DOMNamedNodeMap;
```

**参数说明**：
- 输入：`n`——目标 DOMNode 节点（元素节点）。

**返回值**：`gms_xmldom.DOMNamedNodeMap`，该元素所有属性节点的集合。

### gms_xmldom.getNodeValue
**功能描述**：用于获取指定节点的值。通常用于读取文本节点的内容或其他类型节点的值：
- 对于文本节点，返回其文本内容；
- 对于属性节点，返回属性的值；
- 对于其他类型的节点（如元素节点、注释节点等），返回值可能为 `NULL`。

**语法格式**：
```auto
getNodeValue(n gms_xmldom.DOMNode) RETURN VARCHAR2;
```

**参数说明**：
- 输入：`n`——目标节点，表示要获取值的节点。

**返回值**：`VARCHAR2`，节点的值（文本内容 / 属性值 / NULL）。

### gms_xmldom.getNodeValueAsClob
**功能描述**：用于获取指定节点的值，并以 `CLOB` 类型返回。适用于处理大文本内容（例如长字符串或大段 XML 数据），因为 `CLOB` 可以存储比 `VARCHAR2` 更大的数据。取值规则与 `getNodeValue` 一致：文本节点返回文本内容，属性节点返回属性值，其他类型可能为 `NULL`。

**语法格式**：
```auto
getNodeValueAsClob(n gms_xmldom.DOMNode) RETURN CLOB;
```

**参数说明**：
- 输入：`n`——目标节点，表示要获取值的节点。

**返回值**：`CLOB`，节点的值。

### gms_xmldom.getChildrenByTagName
**功能描述**：用于获取指定父节点下具有特定标签名的所有直接子节点，返回一个 `DOMNodeList` 对象。与 `getElementsByTagName` 的区别在于：`getChildrenByTagName` 只搜索直接子节点，不递归。

**语法格式**：
```auto
getChildrenByTagName(elem gms_xmldom.DOMElement, name VARCHAR2) RETURN gms_xmldom.DOMNodeList;
getChildrenByTagName(elem gms_xmldom.DOMElement, name VARCHAR2, ns VARCHAR2) RETURN gms_xmldom.DOMNodeList;
```

**参数说明**：
- 输入：
  - `elem`：父节点（要搜索的目标节点）
  - `name`：要查找的标签名（元素名）。如果为 `"*"`，则匹配所有标签名
  - `ns`：命名空间 URI。如果为 `NULL` 或空字符串，则忽略命名空间

**返回值**：`gms_xmldom.DOMNodeList`，匹配的子节点列表。

### gms_xmldom.getOwnerDocument
**功能描述**：用于获取指定节点所属的 DOM 文档对象。每个 DOM 节点都属于某个 DOM 文档，该函数返回节点所在的文档对象。

**语法格式**：
```auto
getOwnerDocument(n gms_xmldom.DOMNode) RETURN gms_xmldom.DOMDocument;
```

**参数说明**：
- 输入：`n`——要检查的目标节点。

**返回值**：`gms_xmldom.DOMDocument`，目标节点所属的 DOM 文档对象。

### gms_xmldom.newDOMDocument
**功能描述**：用于创建一个新的 DOM 文档对象。无参数时创建一个空的 DOM 文档对象（通常用于从头构建 XML 文档）；传入 xmltype 或 clob 时解析该内容构造文档。

**语法格式**：
```auto
newDOMDocument() RETURN gms_xmldom.DOMDocument;
newDOMDocument(xmldoc IN XMLTYPE) RETURN gms_xmldom.DOMDocument;
newDOMDocument(cl IN CLOB) RETURN gms_xmldom.DOMDocument;
```

**参数说明**：
- 输入（可选）：
  - `xmldoc`：XMLTYPE 类型，包含 XML 内容的字符串（XML 格式化文本），函数会解析此内容并返回 DOM 文档对象
  - `cl`：CLOB 类型，DOMDocument 的 clob 源数据

**返回值**：`gms_xmldom.DOMDocument`，DOM 文档对象。

### gms_xmldom.hasChildNodes
**功能描述**：用于检查指定的节点是否包含子节点，返回一个布尔值。

**语法格式**：
```auto
hasChildNodes(n gms_xmldom.DOMNode) RETURN BOOLEAN;
```

**参数说明**：
- 输入：`n`——要检查的目标节点。

**返回值**：`BOOLEAN`。目标节点有子节点返回 `TRUE`，无子节点返回 `FALSE`。

### gms_xmldom.setVersion
**功能描述**：用于设置 XML 文档的版本号（如 `"1.0"`）。

**语法格式**：
```auto
setVersion(doc gms_xmldom.DOMDocument, version VARCHAR2);
```

**参数说明**：
- 输入：
  - `doc`：要设置版本的目标 XML 文档
  - `version`：指定的 XML 版本号，通常是 `"1.0"` 或其他有效的 XML 版本字符串

**返回值**：无（过程）。

### gms_xmldom.makeCharacterData
**功能描述**：用于将指定的 `DOMNode` 转换成 `DOMCharacterData`（Text、Comment、CDATASection 等文本类节点的抽象父类型），并返回 `DOMCharacterData`。

**语法格式**：
```auto
makeCharacterData(n gms_xmldom.DOMNode) RETURN gms_xmldom.DOMCharacterData;
```

**参数说明**：
- 输入：`n`——指定 DOMNode 节点（须为文本类节点类型）。

**返回值**：`gms_xmldom.DOMCharacterData`，转换后的 DOMCharacterData 对象。

### gms_xmldom.insertBefore
**功能描述**：用于在指定的父节点中插入一个新的子节点，并将其放置在现有子节点（refchild）之前。文档示例（case 4）中使用了该接口，因此一并列出。

**语法格式**：
```auto
insertBefore(
    n        IN gms_xmldom.DOMNode,
    newchild IN gms_xmldom.DOMNode,
    refchild IN gms_xmldom.DOMNode
) RETURN gms_xmldom.DOMNode;
```

**参数说明**：
- 输入：
  - `n`：父节点，要插入新子节点的目标节点
  - `newchild`：要插入的新子节点
  - `refchild`：参考节点，新节点将插入到此节点之前。如果为 `NULL`，则新节点将被追加到父节点的末尾

**返回值**：`gms_xmldom.DOMNode`，返回插入的新节点。

### gms_xmldom.setnodevalue
**功能描述**：用于设置 XML 文档中某个节点的值。该函数仅支持 5 种节点类型：`DOMAttr`、`DOMText`、`DOMCDATASection`、`DOMProcessingInstruction`、`DOMComment`。若传入的节点为 `NULL`，操作无效且不会报错；若节点中已有内容，会用新值替换；`nodeValue` 传入 `NULL` 则清除节点内容；若节点位于 DOM 树中，执行结果会反映在 DOM 树上。

**语法格式**：
```auto
setnodevalue(n gms_xmldom.DOMNode, nodeValue IN VARCHAR2);
```

**参数说明**：
- 输入：
  - `n`：要设置值的目标节点（仅支持上述 5 种节点类型）
  - `nodeValue`：`VARCHAR2` 类型，要设置给节点的值（可为普通文本或 XML 片段）

**返回值**：无（过程）。

## gms_xmldom应用注意事项

由于gms_xmldom底层实现依赖于plpython3u插件，所以 

1. openGauss编译环境中需安装或在编译依赖的第三方工具集集成`python3`,且版本大于3.7
2. openGauss使用automake配置编译参数时需新增`--with-python`参数
3. openGauss使用cmake配置参数需设置`-DENABLE_PYTHON3=ON`参数
4. 安装openGauss后，需指定环境变量`PYTHONHOME`为`GAUSSHOME`目录下的`python`
5. 安装openGauss后，环境变量`LD_LIBRARY_PATH`中需新增目录`$GAUSSHOME/python/lib64`
6. openGauss的小型化版本不支持`plpython3u`插件，也无法使用`gms_xmldom API package`
7. plpython3u插件不支持`set schema`操作， 任何相关操作均会报错，显示不支持
8. `gms_xmldom`基于Python实现，使用前需确保数据库所使用的字符集为**Python可识别的字符集**（如`UTF-8`、`GBK`等）。若数据库字符集不被Python识别（如部分自定义或特殊字符集），XML文档的解析、节点操作及输出将失败或出现乱码，无法正常使用。可通过查询`server_encoding`、`client_encoding`确认当前字符集，建议将数据库字符集设置为`UTF-8`

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
