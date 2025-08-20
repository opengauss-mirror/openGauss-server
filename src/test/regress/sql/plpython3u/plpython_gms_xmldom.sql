--(1)newDOMDocument
--(2makeNode(doc DOMDocument))
--(3)writeToClob(doc DOMDocument, cl IN OUT CLOB)
DECLARE
    doc gms_xmldom.DOMDocument;
    elem gms_xmldom.DOMElement;
    root gms_xmldom.DOMNode;
    elemNode gms_xmldom.DOMNode;
    cl clob;
    appResNode gms_xmldom.DOMNode;
BEGIN
    doc := gms_xmldom.newDomDocument;
    root := gms_xmldom.makeNode(doc);
    elem := gms_xmldom.createElement(doc, 'root');
    elemNode := gms_xmldom.makeNode(elem);
    appResNode := gms_xmldom.appendChild(root, elemNode);
    cl := gms_xmldom.writeToClob(doc, cl);
    raise notice '%', cl;
END;
/
--(4)newDOMDocument(xmldoc IN xml)
DECLARE
    doc gms_xmldom.DOMDocument;
    cl clob;
    x xml;
BEGIN
    x := xml('<PERSON><NAME>ramesh</NAME></PERSON>');
    doc := gms_xmldom.newDomDocument(x);
    cl := gms_xmldom.writeToClob(doc, cl);
    raise notice '%', cl;
END;
/
--(5)newDOMDocument(xmldoc clob)
DECLARE
    doc gms_xmldom.DOMDocument;
    cl clob;
    s clob;
BEGIN
    s := '<PERSON><NAME>ramesh</NAME></PERSON>';
    doc := gms_xmldom.newDomDocument(s);
    cl := gms_xmldom.writeToClob(doc, cl);
    raise notice '%', cl;
END;
/
--(6)isNull(com DOMDocument)
--(7)isNull(n DOMNode)
--(8)createElement(doc DOMDocument, tagname IN VARCHAR2)
--(9)isNull(elem DOMElement)
--(10)makeNode(elem DOMElement)
--(11)createTextNode(doc DOMDocument, data IN VARCHAR2)
--(12)isNull(t DOMText)
--(13)makeNode(t DOMText)
--(14)setAttribute(elem DOMElement, name IN VARCHAR2, newvalue IN VARCHAR2)
--(15)writeToBuffer(n DOMDocument, buffer IN OUT VARCHAR2)
DECLARE
    doc gms_xmldom.DOMDocument;
    root gms_xmldom.DOMNode;
    
    booklist gms_xmldom.DOMElement;
    listNode gms_xmldom.DOMNode;
    
    bookElem gms_xmldom.DOMElement;
    bookElemNode gms_xmldom.DOMNode;
    
    titleElem gms_xmldom.DOMElement;
    titleElemNode gms_xmldom.DOMNode;
    titleText gms_xmldom.DOMText;
    titleTextNode gms_xmldom.DOMNode;
    
    authorElem gms_xmldom.DOMElement;
    authorElemNode gms_xmldom.DOMNode;
    authorText gms_xmldom.DOMText;
    authorTextNode gms_xmldom.DOMNode;
    
    pageElem gms_xmldom.DOMElement;
    pageElemNode gms_xmldom.DOMNode;
    pageText gms_xmldom.DOMText;
    pageTextNode gms_xmldom.DOMNode;
    
    resnode gms_xmldom.DOMNode;
    
    buffer varchar2;
    isNull boolean;
BEGIN
    /*root*/
    doc := gms_xmldom.newDOMDocument;
    isNull := gms_xmldom.isNull(doc);
    raise notice '%', ('DOMDocument : ' || case when isNull then 'Y' else 'N' end);
    root := gms_xmldom.makeNode(doc);
    isNull := gms_xmldom.isNull(root);
    raise notice '%', ('DOMNode : ' || case when isNull then 'Y' else 'N' end);
    /*booklist*/
    booklist := gms_xmldom.createElement(doc, 'booklist');
    isNull := gms_xmldom.isNull(booklist);
    raise notice '%', ('DOMElement : ' || case when isNull then 'Y' else 'N' end);
    gms_xmldom.setAttribute(booklist, 'type', 'science and engineering');
    listNode := gms_xmldom.makeNode(booklist);

    /*book*/
    bookElem := gms_xmldom.createElement(doc, 'book');
    gms_xmldom.setAttribute(bookElem, 'category', 'python');
    bookElemNode := gms_xmldom.makeNode(bookElem);
    /*title*/
    titleElem := gms_xmldom.createElement(doc, 'title');
    titleElemNode := gms_xmldom.makeNode(titleElem);
    /*attribute of title*/
    titleText := gms_xmldom.createTextNode(doc, 'learning python');
    isNull := gms_xmldom.isNull(titleText);
    raise notice '%', ('DOMText : ' || case when isNull then 'Y' else 'N' end);
    titleTextNode := gms_xmldom.makeNode(titleText);
    resnode := gms_xmldom.appendChild(titleElemNode, titleTextNode);
    /*author*/
    authorElem := gms_xmldom.createElement(doc, 'author');
    authorElemNode := gms_xmldom.makeNode(authorElem);
    /*attribute of author*/
    authorText := gms_xmldom.createTextNode(doc, '张三');
    authorTextNode := gms_xmldom.makeNode(authorText);
    authorTextNode := gms_xmldom.appendChild(authorElemNode, authorTextNode);
    /*pageNumber*/
    pageElem := gms_xmldom.createElement(doc, 'pageNumber');
    pageElemNode := gms_xmldom.makeNode(pageElem);
    /*attribute of pageNumber*/
    pageText := gms_xmldom.createTextNode(doc, '600');
    pageTextNode := gms_xmldom.makeNode(pageText);
    resnode := gms_xmldom.appendChild(pageElemNode, pageTextNode);
    
    resnode := gms_xmldom.appendChild(bookElemNode, titleElemNode);
    resnode := gms_xmldom.appendChild(bookElemNode, authorElemNode);
    resnode := gms_xmldom.appendChild(bookElemNode, pageElemNode);
    resnode := gms_xmldom.appendChild(listNode, bookElemNode);
    resnode := gms_xmldom.appendChild(root, listNode);
    
    buffer := gms_xmldom.writeToBuffer(doc, buffer);
    raise notice '%', buffer;
END;
/
--(16)createComment(doc DOMDocument, data IN VARCHAR2)
--(17)isNull(com DOMComment)
--(18)makeNode(com DOMComment)
--(19)createProcessingInstruction(doc DOMDocument, target IN VARCHAR2, data IN VARCHAR2)
--(20)isNull(pi DOMProcessingInstruction)
--(21)makeNode(pi DOMProcessingInstruction)
DECLARE
    var xml;
    doc gms_xmldom.DOMDocument;
    docNode gms_xmldom.DOMNode;
    bookListNode gms_xmldom.DOMNode;
    nodeList gms_xmldom.DOMNodelist;
    node1 gms_xmldom.DOMNODE;
    comment gms_xmldom.DOMComment;
    commentNode gms_xmldom.DOMNode;
    resNode gms_xmldom.DOMNode;
    
    procInstruc gms_xmldom.DOMProcessingInstruction;
    procInstrucNode gms_xmldom.DOMNode;
    wclob clob;
    isNull boolean;
BEGIN
    var := xml('<booklist type="science and engineering">
  <book category="math">
    <title>learning math</title>
    <author>张三</author>
    <pageNumber>561</pageNumber>
  </book>
  <book category="Python">
    <title>learning Python</title>
    <author>李四</author>
    <pageNumber>600</pageNumber>
  </book>
  <book category="C++">
    <title>learning C++</title>
    <author>王二</author>
    <pageNumber>500</pageNumber>
  </book>
</booklist>');
    doc := gms_xmldom.newDOMDocument(var);
    docNode := gms_xmldom.makeNode(doc);
    bookListNode := gms_xmldom.getFirstChild(docNode);
    
    nodeList := gms_xmldom.getElementsByTagName(doc, 'book');
    node1 := gms_xmldom.item(nodeList, 0);
    
    --创建和插入comment节点
    comment := gms_xmldom.createComment(doc, 'This is the introduction of books');
    isNull := gms_xmldom.isNull(comment);
    raise notice '%', ('DOMComment : ' || case when isNull then 'Y' else 'N' end);
    commentNode := gms_xmldom.makeNode(comment);
    resNode := gms_xmldom.insertBefore(bookListNode, commentNode, node1);
    
    --创建和插入ProcessingInstruction节点
    procInstruc := gms_xmldom.createProcessingInstruction(doc, 'xml', 'version="2.0"');
    isNull := gms_xmldom.isNull(procInstruc);
    raise notice '%', ('DOMProcessingInstruction : ' || case when isNull then 'Y' else 'N' end);
    procInstrucNode := gms_xmldom.makeNode(procInstruc);
    resNode := gms_xmldom.insertBefore(docNode, procInstrucNode, bookListNode);
    
    wclob := gms_xmldom.writeToClob(doc, wclob);
    --输出修改后的clob内容  
    raise notice '%', wclob;
END;
/
--(22)createCDATASection(doc gms_xmldom.DOMDocument, data IN VARCHAR2)
--(23)isNull(cds DOMCDATASection)
--(24)makeNode(cds DOMCDATASection)
DECLARE
    var xml;
    doc gms_xmldom.DOMDocument;
    docNode gms_xmldom.DOMNode;
    bookListNode gms_xmldom.DOMNode;
    nodeList gms_xmldom.DOMNodelist;
    node1 gms_xmldom.DOMNODE;
    redNode gms_xmldom.DOMNODE;
    wclob clob;
    
    cds gms_xmldom.DOMCDataSection;
    cdsNode gms_xmldom.DOMNode;
    isNull boolean;
BEGIN
    var := xml('<booklist type="science and engineering">
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
    node1 := gms_xmldom.item(nodeList, 0);
    
    cds := gms_xmldom.createCDataSection(doc, '<>&');
    isNull := gms_xmldom.isNull(cds);
    raise notice '%', ('DOMCDataSection : ' || case when isNull then 'Y' else 'N' end);
    cdsNode := gms_xmldom.makeNode(cds);
    redNode := gms_xmldom.appendChild(node1, cdsNode);
    
    cds := gms_xmldom.createCDataSection(doc, '&');
    cdsNode := gms_xmldom.makeNode(cds);
    redNode := gms_xmldom.appendChild(node1, cdsNode);
    
    cds := gms_xmldom.createCDataSection(doc, ']]');
    cdsNode := gms_xmldom.makeNode(cds);
    redNode := gms_xmldom.appendChild(node1, cdsNode);
    
    wclob := gms_xmldom.writeToClob(doc, wclob);
    --输出修改后的clob内容  
    raise notice '%', wclob;
END;
/
--插入失败
DECLARE
    var xml;
    doc gms_xmldom.DOMDocument;
    docNode gms_xmldom.DOMNode;
    bookListNode gms_xmldom.DOMNode;
    nodeList gms_xmldom.DOMNodelist;
    node1 gms_xmldom.DOMNODE;
    redNode gms_xmldom.DOMNODE;
    wclob clob;
    
    cds gms_xmldom.DOMCDataSection;
    cdsNode gms_xmldom.DOMNode;
BEGIN
    var := xml('<booklist type="science and engineering">
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
    node1 := gms_xmldom.item(nodeList, 0);
    
    cds := gms_xmldom.createCDataSection(doc, ']]>');
    cdsNode := gms_xmldom.makeNode(cds);
    redNode := gms_xmldom.appendChild(node1, cdsNode);
    
    wclob := gms_xmldom.writeToClob(doc, wclob);
    --输出修改后的clob内容  
    raise notice '%', wclob;
END;
/

--(25)createDocument(namespaceuri IN VARCHAR2, qualifiedname IN VARCHAR2, doctype IN DOMType:= NULL)
--(26)createElement(doc gms_xmldom.DOMDocument, tagname IN VARCHAR2, ns IN VARCHAR2)
--(27)setAttribute(elem gms_xmldom.DOMElement, name IN VARCHAR2, newvalue IN VARCHAR2, ns IN VARCHAR2)
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
    gms_xmldom.setAttribute(elem, 'id', 'headDoc', 'http://www.runoob.com/xml/');
    elemNode := gms_xmldom.makeNode(elem);
    resNode := gms_xmldom.appendChild(rootNode, elemNode);
    
    elem := gms_xmldom.createElement(doc, 'body', 'http://www.runoob.com/xml/');
    gms_xmldom.setAttribute(elem, 'id', 'bodyDoc', 'http://www.runoob.com/xml/');
    elemNode := gms_xmldom.makeNode(elem);
    resNode := gms_xmldom.appendChild(rootNode, elemNode);
    
    wclob :=gms_xmldom.writeToClob(doc, wclob);
    --输出clob内容  
    raise notice '%', wclob;
END;
/
--(28)makeElement(n DOMNode)
--(29)getElementsByTagName(elem DOMElement, name IN VARCHAR2, ns varchar2)
--(30)createAttribute(doc DOMDocument, name IN VARCHAR2, ns IN VARCHAR2)
--(31)setAttributeNode(elem DOMElement, newattr IN DOMAttr, ns IN VARCHAR2)
--(32)getChildrenByTagName(elem DOMElement, name varchar2, ns varchar2)
--(33)item(nl DOMNodeList, idx IN PLS_INTEGER)
DECLARE
    doc gms_xmldom.DOMDocument;
    root gms_xmldom.DOMNode;
    rootElem gms_xmldom.DOMElement;
    bookElem gms_xmldom.DOMElement;
    bookElemNode gms_xmldom.DOMNode;
    titleElem gms_xmldom.DOMElement;
    titleElemNode gms_xmldom.DOMNode;
    titleText gms_xmldom.DOMText;
    titleTextNode gms_xmldom.DOMNode;
    authorElem gms_xmldom.DOMElement;
    authorElemNode gms_xmldom.DOMNode;
    authorText gms_xmldom.DOMText;
    authorTextNode gms_xmldom.DOMNode;
    pageElem gms_xmldom.DOMElement;
    pageElemNode gms_xmldom.DOMNode;
    pageText gms_xmldom.DOMText;
    pageTextNode gms_xmldom.DOMNode;
    resnode gms_xmldom.DOMNode;
    cl clob;
    
    bookListNode gms_xmldom.DOMNode;
    bookListElem gms_xmldom.DOMElement;
    nodeList gms_xmldom.DOMNodelist;
    node gms_xmldom.DOMNode;
    nodeElem gms_xmldom.DOMElement;
    attr gms_xmldom.DOMAttr;
    resAttr gms_xmldom.DOMAttr;
    len integer;
BEGIN
    --booklist
    doc := gms_xmldom.createDocument('http://www.runoob.com/xml/', 'booklist', null);
    rootElem := gms_xmldom.getDocumentElement(doc);
    gms_xmldom.setAttribute(rootElem, 'type', 'science and engineering', 'http://www.runoob.com/xml/');
    root := gms_xmldom.makeNode(rootElem);
    --book
    bookElem := gms_xmldom.createElement(doc, 'book', 'http://www.runoob.com/xml/');
    gms_xmldom.setAttribute(bookElem, 'category', 'python', 'http://www.runoob.com/xml/');
    bookElemNode := gms_xmldom.makeNode(bookElem);
    --title
    titleElem := gms_xmldom.createElement(doc, 'book', 'http://www.runoob.com/xml/');
    titleElemNode := gms_xmldom.makeNode(titleElem);
    --attribute of title
    titleText := gms_xmldom.createTextNode(doc, 'learning python');
    titleTextNode := gms_xmldom.makeNode(titleText);
    resnode := gms_xmldom.appendChild(titleElemNode, titleTextNode);
    --author
    authorElem := gms_xmldom.createElement(doc, 'author', 'http://www.runoob.com/xml/');
    authorElemNode := gms_xmldom.makeNode(authorElem);
    --attribute of author
    authorText := gms_xmldom.createTextNode(doc, '张三');
    authorTextNode := gms_xmldom.makeNode(authorText);
    authorTextNode := gms_xmldom.appendChild(authorElemNode, authorTextNode);
    --pageNumber
    pageElem := gms_xmldom.createElement(doc, 'pageNumber', 'http://www.runoob.com/xml/');
    pageElemNode := gms_xmldom.makeNode(pageElem);
    --attribute of pageNumber
    pageText := gms_xmldom.createTextNode(doc, '600');
    pageTextNode := gms_xmldom.makeNode(pageText);
    resnode := gms_xmldom.appendChild(pageElemNode, pageTextNode);
    
    resnode := gms_xmldom.appendChild(bookElemNode, titleElemNode);
    resnode := gms_xmldom.appendChild(bookElemNode, authorElemNode);
    resnode := gms_xmldom.appendChild(bookElemNode, pageElemNode);
    resnode := gms_xmldom.appendChild(root, bookElemNode);
    
    cl := gms_xmldom.writeToClob(doc, cl);
    raise notice '%', cl;
    
    root := gms_xmldom.makeNode(doc);
    bookListNode := gms_xmldom.getFirstChild(root);
    bookListElem := gms_xmldom.makeElement(bookListNode);
    nodeList := gms_xmldom.getChildrenByTagName(bookListElem, 'book', 'http://www.runoob.com/xml/');
    --nodeList := gms_xmldom.getElementsByTagName(bookListElem, 'book', 'http://www.runoob.com/xml/');
    len := gms_xmldom.getLength(nodeList);
    for i in 0..len-1 loop
      node := gms_xmldom.item(nodeList,i);
      nodeElem := gms_xmldom.makeElement(node);
      attr := gms_xmldom.createAttribute(doc, 'num', 'http://www.runoob.com/xml/');
      resAttr := gms_xmldom.setAttributeNode(nodeElem, attr, 'http://www.runoob.com/xml/');
    end loop;
    --输出修改后的clob内容
    cl := gms_xmldom.writetoclob(doc,cl);
    raise notice '%', ('设置属性名后：' || cl);
END;
/

--(34)createAttribute(doc DOMDocument, name IN VARCHAR2)
--(35)isNull(a DOMAttr)
--(36)setAttributeNode(elem DOMElement, newattr IN DOMAttr)
--(37)getElementsByTagName(doc DOMElement, tagname IN VARCHAR2)
DECLARE
    var xml;
    doc gms_xmldom.DOMDocument;
    docElem gms_xmldom.DOMElement;
    nodeList gms_xmldom.DOMNodelist;
    node gms_xmldom.DOMNODE;
    wclob clob;
    elemNode gms_xmldom.DOMElement;
    attr1   gms_XMLDOM.DOMAttr;
    attr2   gms_XMLDOM.DOMAttr;
    isNull boolean;
BEGIN
    var := xml('<booklist type="science and engineering">
  <book>
    <title>learning math</title>
    <author>张三</author>
    <pageNumber>561</pageNumber>
  </book>
</booklist>');
    doc := gms_xmldom.newDOMDocument(var);
    --打印设置属性名前的wclob
    wclob := gms_xmldom.writeToClob(doc, wclob);
    raise notice '%', ('设置属性名前：' || wclob);
    
    docElem := gms_xmldom.getDocumentElement(doc);
    nodeList := gms_xmldom.getElementsByTagName(doc, 'book');
    node := gms_XMLDOM.item(nodeList, 0);
    elemNode := gms_XMLDOM.makeElement(node);
    --创建DOMAttr属性节点
    attr1 := gms_xmldom.createAttribute(doc,'category');
    isNull := gms_xmldom.isNull(attr1);
    raise notice '%', ('DOMAttr : ' || case when isNull then 'Y' else 'N' end);
    attr2 := gms_xmldom.setAttributeNode(elemNode, attr1);
    
    wclob := gms_xmldom.writetoclob(doc,wclob);
    --输出修改后的clob内容  
    raise notice '%', ('设置属性名后：' || wclob);
END;
/
--(38)getElementsByTagName(doc DOMDocument, tagname IN VARCHAR2)
DECLARE
    var xml;
    doc gms_xmldom.DOMDocument;
    nodeList gms_xmldom.DOMNodelist;
    node gms_xmldom.DOMNODE;
    wclob clob;
    elemNode gms_xmldom.DOMElement;
    attr1   gms_XMLDOM.DOMAttr;
    attr2   gms_XMLDOM.DOMAttr;
BEGIN
    var := xml('<booklist type="science and engineering">
  <book>
    <title>learning math</title>
    <author>张三</author>
    <pageNumber>561</pageNumber>
  </book>
</booklist>');
    doc := gms_xmldom.newDOMDocument(var);
    --打印设置属性名前的wclob
    wclob := gms_xmldom.writeToClob(doc, wclob);
    raise notice '%', ('设置属性名前：' || wclob);
    
    nodeList := gms_xmldom.getElementsByTagName(doc, 'book');
    node := gms_XMLDOM.item(nodeList, 0);
    elemNode := gms_XMLDOM.makeElement(node);
    --创建DOMAttr属性节点
    attr1 := gms_xmldom.createAttribute(doc,'category');
    attr2 := gms_xmldom.setAttributeNode(elemNode, attr1);
    
    wclob := gms_xmldom.writetoclob(doc,wclob);
    --输出修改后的clob内容  
    raise notice '%', ('设置属性名后：' || wclob);
END;
/

--(39)createDocumentFragment(doc DOMDocument)
--(40)isNull(df DOMDocumentFragment)
--(41)makeNode(df DOMDocumentFragment)
--(42)writeToBuffer(n DOMDocumentFragment, buffer IN OUT VARCHAR2)
DECLARE
    doc gms_XMLDOM.DOMDocument;
    docfragment   gms_XMLDOM.DOMDocumentFragment;
    docfragmentnode   gms_XMLDOM.DOMnode;
    createelem gms_XMLDOM.DOMElement;
    elemnode gms_XMLDOM.DOMNode;  
    text gms_XMLDOM.DOMText;
    textnode gms_XMLDOM.DOMNode;
    resNode  gms_XMLDOM.DOMNode;
    buf varchar2(4000);
    isNull boolean;
BEGIN
    --返回新的document实例
    doc := gms_XMLDOM.newDOMDocument();
    docfragment  := gms_XMLDOM.createDocumentFragment(doc);
    isNull := gms_xmldom.isNull(docfragment);
    raise notice '%', ('DOMDocumentFragment : ' || case when isNull then 'Y' else 'N' end);
    docfragmentnode := gms_XMLDOM.makeNode(docfragment );
    --在文档片段添加内容
    --创建DOMElement对象
    createelem := gms_XMLDOM.createElement(doc,'test');
    elemnode := gms_XMLDOM.makeNode(createelem);
    --创建内容
    text  := gms_XMLDOM.createTextnode(doc,'testtext');
    textnode := gms_XMLDOM.makeNode(text);
    --添加到指定位置
    resNode := gms_XMLDOM.appendChild(elemnode,textnode);
    resNode := gms_XMLDOM.appendChild(docfragmentnode,elemnode);
    --写入buffer
    buf := gms_xmldom.writetobuffer(docfragment,buf);
    --输出修改后的clob内容  
    raise notice '%', ('文档片段为:' ||buf);
END;
/
--(43)writeToClob(n DOMNode, cl IN OUT CLOB)
--(44)writeToClob(n DOMNode, cl IN OUT CLOB, pflag IN NUMBER, indent IN NUMBER)
--(45)writeToClob(doc DOMDocument, cl IN OUT CLOB, pflag IN NUMBER, indent IN NUMBER)
--(46)writeToBuffer(n DOMNode, buffer IN OUT VARCHAR2)
DECLARE
    var xml;
    doc gms_xmldom.DOMDocument;
    docNode gms_xmldom.DOMNode;
    bookListNode gms_xmldom.DOMNode;
    nodeList gms_xmldom.DOMNodelist;
    node1 gms_xmldom.DOMNODE;

    wclob clob;
    buffer varchar2;
BEGIN
    var := xml('<booklist type="science and engineering">
  <book category="math">
    <title>learning math</title>
    <author>张三</author>
    <pageNumber>561</pageNumber>
  </book>
  <book category="Python">
    <title>learning Python</title>
    <author>李四</author>
    <pageNumber>600</pageNumber>
  </book>
  <book category="C++">
    <title>learning C++</title>
    <author>王二</author>
    <pageNumber>500</pageNumber>
  </book>
</booklist>');
    doc := gms_xmldom.newDOMDocument(var);
    docNode := gms_xmldom.makeNode(doc);
    bookListNode := gms_xmldom.getFirstChild(docNode);
    
    nodeList := gms_xmldom.getElementsByTagName(doc, 'book');
    node1 := gms_xmldom.item(nodeList, 0);
    wclob := gms_xmldom.writeToClob(node1, wclob);
    raise notice '%', ('writeToClob(DOMNode, clob):' || wclob);
    
    wclob := gms_xmldom.writeToClob(node1, wclob, 4, 0);
    raise notice '%', ('writeToClob(DOMNode, clob, number, number):' || wclob);
    
    wclob := gms_xmldom.writeToClob(doc, wclob, 4, 0);
    raise notice '%', ('writeToClob(DOMDocument, clob, number, number):' || wclob);
    
    buffer := gms_xmldom.writeToBuffer(node1, buffer);
    raise notice '%', ('writeToBuffer(DOMNode, varchar2):' || buffer);
END;
/
--(47)setVersion(doc DOMDocument, version VARCHAR2)
DECLARE
  doc gms_xmldom.DOMDocument;
  cl clob;
  x xml;
BEGIN
  x := xml('<PERSON><NAME>ramesh</NAME></PERSON>');
  doc := gms_xmldom.newDomDocument(x);
  gms_xmldom.setVersion(doc, '2.0');
  cl := gms_xmldom.writeToClob(doc, cl);
  raise notice '%', (cl);
END;
/
--(48)getFirstChild(n DOMNode)
--(49)getNodeName(n DOMNode)
--(50)getChildrenByTagName(elem DOMElement, name varchar2)
--(51)getDocumentElement(doc DOMDocument)
--(52)getNodeValue(n DOMNode)
--(53)getChildNodes(n DOMNode)
--(54)getLength(nl DOMNodeList)
--(55)getNodeType(n DOMNode)
DECLARE
    var xml;
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
    var := xml('<booklist type="science and engineering">
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
    raise notice '%', ('xml内容是:' || wclob);

    --getDocumentElement
    elemNode := gms_xmldom.getDocumentElement(doc);
    bookListNode := gms_xmldom.getFirstChild(docNode);
    --getChildrenByTagName
    nodeList := gms_xmldom.getChildrenByTagName(elemNode, 'book');
    node := gms_xmldom.item(nodeList, 0);
    --getFirstChild，getNodeName
    titleNode := gms_xmldom.getFirstChild(node);
    wclob := gms_xmldom.writeToClob(titleNode, wclob);
    raise notice '%', (wclob);
    raise notice '%', ('The nodeName is:' || gms_xmldom.getNodeName(titleNode));
    --element节点的nodeValue，为空
    raise notice '%', ('The nodeValue is:' || gms_xmldom.getNodeValue(titleNode));
    txt := gms_xmldom.getFirstChild(titleNode);
    textNode := gms_xmldom.makeNode(txt);
    raise notice '%', ('The nodeValue is:' || gms_xmldom.getNodeValue(textNode));
    --getChildNodes
    nodeList := gms_xmldom.getChildNodes(bookListNode);
    llen := gms_xmldom.getLength(nodeList);
    raise notice '%', ('booklist子节点长度为:' || llen );

    for i in 0..(llen-1) loop
        node := gms_xmldom.item(nodeList, i);
        --getNodeType
        if gms_xmldom.getNodeType(node) = gms_xmldom.COMMENT_NODE then
            n := n+1;
            --comment节点的nodeValue
            raise notice '%', ('第'||'个备注为：'||gms_xmldom.getNodeValue(node));
        end if;
    end loop;

END;
/
--(56)getLocalName(a DOMAttr)
--(57)getLocalName(elem DOMElement)
--(58)getLocalName(n DOMnode, data OUT VARCHAR2)
DECLARE
    var xml;
    doc gms_xmldom.DOMDocument;
    docNode gms_xmldom.DOMNode;
    bookListNode gms_xmldom.DOMNode;
    bookNode gms_xmldom.DOMNode;
    titleElem gms_xmldom.DOMElement;
    nodeList gms_xmldom.DOMNodelist;
    
    attr gms_xmldom.DOMAttr;

    wclob clob;
    localName varchar2;
BEGIN
    var := xml('<booklist type="science and engineering">
  <book category="math">
    <title>learning math</title>
    <author>张三</author>
    <pageNumber>561</pageNumber>
  </book>
  <book category="Python">
    <title>learning Python</title>
    <author>李四</author>
    <pageNumber>600</pageNumber>
  </book>
  <book category="C++">
    <title>learning C++</title>
    <author>王二</author>
    <pageNumber>500</pageNumber>
  </book>
</booklist>');
    doc := gms_xmldom.newDOMDocument(var);
    docNode := gms_xmldom.makeNode(doc);
    
    wclob := gms_xmldom.writeToClob(doc, wclob);
    raise notice '%', ('xml内容是:' || wclob);
    
    bookListNode := gms_xmldom.getFirstChild(docNode);
    bookNode := gms_xmldom.getFirstChild(bookListNode);
    nodeList := gms_xmldom.getChildNodes(bookNode);
    titleElem := gms_xmldom.item(nodeList, 0);
    
    --element的localName
    localName := gms_xmldom.getLocalName(titleElem);
    raise notice '%', ('element的localName:' || localName);
    --node的localName
    localName := gms_xmldom.getLocalName(bookNode);
    raise notice '%', ('node的localName:' || localName);
    
    --attr的LocalName
    attr := gms_xmldom.createAttribute(doc, 'name');
    localName := gms_xmldom.getLocalName(attr);
    raise notice '%', ('attr的LocalName:' || localName);
END;
/
--(59)hasChildNodes(n DOMNode)
DECLARE
    var xml;
    doc gms_XMLDOM.DOMDocument;
    docNode gms_XMLDOM.DOMNode;
    docelem gms_XMLDOM.DOMElement;
    node gms_XMLDOM.DOMNode;
    childnode gms_XMLDOM.DOMNode;
    nodelist gms_XMLDOM.DOMNodelist;
    wclob  clob;
    haschild  boolean;
BEGIN
    var := xml('<booklist type="science and engineering">
  <book category="math">
    <title>learning math</title>
    <author>张三</author>
    <pageNumber>561</pageNumber>
  </book>
  <book category="Python">
    <title>learning Python</title>
    <author>李四</author>
    <pageNumber>600</pageNumber>
  </book>
  <book category="C++">
    <title>learning C++</title>
    <author>王二</author>
    <pageNumber>500</pageNumber>
  </book>
</booklist>');
    --返回新的document实例
    doc := gms_XMLDOM.newDOMDocument(var);
    docNode := gms_XMLDOM.makeNode(doc);
    --写入clob  
    wclob := gms_xmldom.writetoclob(doc,wclob);
    --输出修改前的内容  
    raise notice '%', ('xml内容是:' || wclob);

    --获取存在子节点的节点
    nodelist := gms_XMLDOM.getElementsByTagName(doc, 'book');
    node := gms_XMLDOM.item(nodelist,1);
    --判断该节点是否有子节点
    haschild := gms_XMLDOM.hasChildNodes(node);
    raise notice '%', ('The result is：' || case when haschild then 'Y' else 'N' end);

    --获取不存在子节点的节点
    nodelist := gms_XMLDOM.getElementsByTagName(doc, 'press');
    node := gms_XMLDOM.item(nodelist,0);
    --判断该节点是否有子节点
    haschild := gms_XMLDOM.hasChildNodes(node);
    raise notice '%', ('The result is：' || case when haschild then 'Y' else 'N' end);
END;
/

--(60)cloneNode(n DOMNode, deep boolean)(无子项)
DECLARE
    var xml;
    doc gms_XMLDOM.DOMDocument;
    docNode gms_XMLDOM.DOMNode;
    docelem gms_XMLDOM.DOMElement;
    node gms_XMLDOM.DOMNode;
    childnode gms_XMLDOM.DOMNode;
    nodelist gms_XMLDOM.DOMNodelist;
    clonenode   gms_XMLDOM.DOMNode;
    resNode  gms_XMLDOM.DOMNode;
    insertnode gms_XMLDOM.DOMNode;
    wclob  clob;
BEGIN
    var := xml('<booklist type="science and engineering">
  <book category="math">
    <title>learning math</title>
    <author>张三</author>
    <pageNumber>561</pageNumber>
  </book>
  <book category="Python">
    <title>learning Python</title>
    <author>李四</author>
    <pageNumber>600</pageNumber>
  </book>
  <book category="C++">
    <title>learning C++</title>
    <author>王二</author>
    <pageNumber>500</pageNumber>
  </book>
</booklist>');
    --返回新的document实例
    doc := gms_XMLDOM.newDOMDocument(var);
    --写入clob  
    wclob := gms_xmldom.writetoclob(doc,wclob);
    --输出修改前的内容  
    raise notice '%', ('克隆前:' ||wclob);
    
    docelem := gms_xmldom.getDocumentElement(doc);
    docNode := gms_XMLDOM.makeNode(docelem);
    --获取对应子元素
    nodelist := gms_XMLDOM.getElementsByTagName(doc, 'book');
    node := gms_XMLDOM.item(nodelist,0);
    --克隆节点，不克隆节点的子节点
    clonenode := gms_XMLDOM.clonenode(node,false);
    insertnode := gms_XMLDOM.insertbefore(docNode,clonenode,node);
    --写入clob
    wclob := gms_xmldom.writetoclob(doc,wclob);
    --输出修改后的clob内容  
    raise notice '%', ('克隆（无子项）后:' ||wclob);
END;
/
--cloneNode(n DOMNode, deep boolean)(有子项)
DECLARE
    var xml;
    doc gms_XMLDOM.DOMDocument;
    docNode gms_XMLDOM.DOMNode;
    docelem gms_XMLDOM.DOMElement;
    node gms_XMLDOM.DOMNode;  
    childnode gms_XMLDOM.DOMNode;
    nodelist gms_XMLDOM.DOMNodelist;
    clonenode   gms_XMLDOM.DOMNode;
    insertnode gms_XMLDOM.DOMNode;
    n gms_XMLDOM.DOMNode;
    wclob  clob;
BEGIN
    var := xml('<booklist type="science and engineering">
  <book category="math">
    <title>learning math</title>
    <author>张三</author>
    <pageNumber>561</pageNumber>
  </book>
  <book category="Python">
    <title>learning Python</title>
    <author>李四</author>
    <pageNumber>600</pageNumber>
  </book>
  <book category="C++">
    <title>learning C++</title>
    <author>王二</author>
    <pageNumber>500</pageNumber>
  </book>
</booklist>');
    --返回新的document实例
    doc := gms_XMLDOM.newDOMDocument(var);
    --写入clob  
    wclob := gms_xmldom.writetoclob(doc,wclob);
    --输出修改前的内容  
    raise notice '%', ('克隆前:' ||wclob);
    
    docelem := gms_xmldom.getDocumentElement(doc);
    docNode := gms_XMLDOM.makeNode(docelem);
    --获取对应子元素
    nodelist := gms_XMLDOM.getElementsByTagName(doc, 'book');
    node := gms_XMLDOM.item(nodelist,0);
    --克隆节点，克隆节点的子节点
    clonenode := gms_XMLDOM.clonenode(node, true);
    insertnode := gms_XMLDOM.insertbefore(docNode,clonenode,node);
    --写入clob
    wclob := gms_xmldom.writetoclob(doc,wclob);
    --输出修改后的clob内容  
    raise notice '%', ('克隆（有子项）后:' ||wclob);
END;
/

--(61)getAttributes(n DOMNode)
--(62)isNull(nnm DOMNamedNodeMap)
--(63)getLength(nnm DOMNamedNodeMap)
--(64)item(nnm DOMNamedNodeMap, idx IN PLS_INTEGER)
DECLARE
    var xml;
    doc gms_XMLDOM.DOMDocument;
    docNode gms_XMLDOM.DOMNode;
    node gms_XMLDOM.DOMNode;
    docelem gms_XMLDOM.DOMElement;
    nodelist gms_XMLDOM.DOMNodelist;  
    attrmap gms_XMLDOM.DOMNamedNodeMap;  
    length integer;
    isNull boolean;
    attr gms_xmldom.DOMAttr;
BEGIN
    var := xml('<booklist type="science and engineering">
  <book category="math" lang="ch" year="2005">
    <title>learning math</title>
    <author>张三</author>
    <pageNumber>561</pageNumber>
  </book>
</booklist>');
    --返回新的document实例
    doc := gms_xmldom.newDOMDocument(var);
    docNode := gms_xmldom.makeNode(doc );
    docelem := gms_xmldom.getDocumentElement(doc);
    nodelist := gms_xmldom.getElementsByTagName(docelem,'book');
    node := gms_xmldom.item(nodelist,0);
    --获取第一个EMP节点的属性
    attrmap := gms_xmldom.getAttributes(node);
    isNull := gms_xmldom.isNull(attrmap);
    raise notice '%', ('isNull(DOMNamedNodeMap):' || case when isNull then 'Y' else 'N' end);

    --查看map的长度验证结果
    length := gms_xmldom.getlength(attrmap);
    raise notice '%', ('第一个book节点的属性长度为:' || length );
    
    --item(DOMNamedNodeMap)
    attr := gms_xmldom.item(attrmap, 0);
END;
/

--(65)freeDocument(doc DOMDocument)
--(66)freeNodeList(nl DOMNodeList)
--(67)isNull(nl DOMNodeList)
DECLARE
    var xml;
    doc gms_XMLDOM.DOMDocument;
    nodeList gms_xmldom.DOMNodelist;
    wclob  clob;
    isNull boolean;
    node gms_XMLDOM.DOMNode;
BEGIN
    var := xml('<booklist type="science and engineering">
  <book category="math">
    <title>learning math</title>
    <author>张三</author>
    <pageNumber>561</pageNumber>
  </book>
</booklist>');
    --返回一个新document实例
    doc := gms_XMLDOM.newDOMDocument(var);
    --设置XML文档信息
    wclob := gms_xmldom.writetoclob(doc,wclob);
    --输出clob内容  
    raise notice '%', (wclob);
    
    nodeList := gms_xmldom.getElementsByTagName(doc, 'book');
    node := gms_xmldom.item(nodeList, 0);
    --释放node
    gms_xmldom.freeDocument(doc);
    --检查node是否释放成功
    isNull := gms_xmldom.isNull(doc);
    raise notice '%', ('The result is : ' || case when isNull then 'Y' else 'N' end);
    --释放了父节点，子节点是否释放
    isNull := gms_xmldom.isNull(nodeList);
    raise notice '%', ('The result is : ' || case when isNull then 'Y' else 'N' end);
    isNull := gms_xmldom.isNull(node);
    raise notice '%', ('The result is : ' || case when isNull then 'Y' else 'N' end);
    
    node := gms_xmldom.item(nodeList, 0);
    wclob := gms_xmldom.writetoclob(node, wclob);
    raise notice '%', (wclob);
    
    gms_xmldom.freeNodeList(nodeList);
    isNull := gms_xmldom.isNull(nodeList);
    raise notice '%', ('The result is : ' || case when isNull then 'Y' else 'N' end);
    isNull := gms_xmldom.isNull(node);
    raise notice '%', ('The result is : ' || case when isNull then 'Y' else 'N' end);
    
    wclob := gms_xmldom.writetoclob(node, wclob);
    raise notice '%', (wclob);
END;
/

--(68)freeNode(nl DOMNode)
DECLARE
    var xml;
    doc gms_XMLDOM.DOMDocument;
    nodeList gms_xmldom.DOMNodelist;
    wclob  clob;
    isNull boolean;
    node gms_XMLDOM.DOMNode;
BEGIN
    var := xml('<booklist type="science and engineering">
  <book category="math">
    <title>learning math</title>
    <author>张三</author>
    <pageNumber>561</pageNumber>
  </book>
</booklist>');
    --返回一个新document实例
    doc := gms_XMLDOM.newDOMDocument(var);
    --设置XML文档信息
    wclob := gms_xmldom.writetoclob(doc,wclob);
    --输出clob内容  
    raise notice '%', (wclob);
    
    nodeList := gms_xmldom.getElementsByTagName(doc, 'book');
    node := gms_xmldom.item(nodeList, 0);
    --检查node是否释放成功
    isNull := gms_xmldom.isNull(doc);
    raise notice '%', ('The document is : ' || case when isNull then 'Y' else 'N' end);
    isNull := gms_xmldom.isNull(node);
    raise notice '%', ('The node is : ' || case when isNull then 'Y' else 'N' end);
    wclob := gms_xmldom.writetoclob(node, wclob);
    raise notice '%', (wclob);

    gms_xmldom.freeNode(node);
    isNull := gms_xmldom.isNull(node);
    raise notice '%', ('The node is : ' || case when isNull then 'Y' else 'N' end);
END;
/

--(69)insertBefore(n DOMNode, newchild IN DOMNode)
--在同一位置插入同名同内容的节点
DECLARE
    var xml;
    doc gms_XMLDOM.DOMDocument;
    docNode gms_XMLDOM.DOMNode;
    docelem gms_XMLDOM.DOMElement;
    node gms_XMLDOM.DOMNode;
    elemnode gms_XMLDOM.DOMNode;  
    bookListNode gms_XMLDOM.DOMNode;
    nodelist gms_XMLDOM.DOMNodelist;
    createelem gms_XMLDOM.DOMElement;
    resNode  gms_XMLDOM.DOMNode;
    text gms_XMLDOM.DOMText;
    textnode gms_XMLDOM.DOMNode;
    buf varchar2(4000);
BEGIN
    var := xml('<booklist type="science and engineering">
  <book category="math">
    <title>learning math</title>
    <author>张三</author>
    <pageNumber>561</pageNumber>
  </book>
</booklist>');
    --返回新的document实例
    doc := gms_XMLDOM.newDOMDocument(var);
    docNode := gms_xmldom.makeNode(doc);
    bookListNode := gms_xmldom.getfirstchild(docNode);
    
    --写入buffer
    buf := gms_xmldom.writetobuffer(doc,buf); 
    raise notice '%', ('XML内容:' ||buf);
    
    --获取对应子元素
    nodelist := gms_XMLDOM.getElementsByTagName(doc, 'book');
    node := gms_XMLDOM.item(nodelist,0);
    --输出buf内容 
    buf := gms_xmldom.writetobuffer(node,buf); 
    raise notice '%', ('book内容:' ||buf);

    --创建DOMElement对象
    createelem := gms_XMLDOM.createelement(doc,'test');
    elemnode := gms_XMLDOM.makenode(createelem);
    --创建内容
    text  := gms_XMLDOM.createTextnode(doc,'testtext');
    textnode := gms_XMLDOM.makenode(text);
    resNode := gms_XMLDOM.appendchild(elemnode,textnode);

    --添加到指定位置
    resNode := gms_XMLDOM.insertbefore(bookListNode, elemnode, node);
    --写入buffer
    buf := gms_xmldom.writetobuffer(doc, buf);
    --输出buf内容 
    raise notice '%', ('第一次appendchild后:' ||buf);

    createelem := gms_XMLDOM.createelement(doc,'test');
    elemnode := gms_XMLDOM.makenode(createelem);
    --创建内容
    text  := gms_XMLDOM.createTextnode(doc,'testtext');
    textnode := gms_XMLDOM.makenode(text);
    resNode := gms_XMLDOM.appendchild(elemnode,textnode);
    resNode := gms_XMLDOM.insertbefore(bookListNode, elemnode, node);
    --写入buffer
    buf := gms_xmldom.writetobuffer(doc, buf);
    --输出buf内容 
    raise notice '%', ('第一次appendchild后:' ||buf);
END;
/
--(70)appendChild(n DOMNode, newchild IN DOMNode)
--插入同名同内容节点
DECLARE
    var xml;
    doc gms_XMLDOM.DOMDocument;
    docNode gms_XMLDOM.DOMNode;
    docelem gms_XMLDOM.DOMElement;
    node gms_XMLDOM.DOMNode;
    elemnode gms_XMLDOM.DOMNode;  
    bookListNode gms_XMLDOM.DOMNode;
    nodelist gms_XMLDOM.DOMNodelist;
    createelem gms_XMLDOM.DOMElement;
    resNode  gms_XMLDOM.DOMNode;
    text gms_XMLDOM.DOMText;
    textnode gms_XMLDOM.DOMNode;
    buf varchar2(4000);
BEGIN
    var := xml('<booklist type="science and engineering">
  <book category="math">
    <title>learning math</title>
    <author>张三</author>
    <pageNumber>561</pageNumber>
    <test>testtext</test>
  </book>
</booklist>');
    --返回新的document实例
    doc := gms_XMLDOM.newDOMDocument(var);
    docNode := gms_xmldom.makeNode(doc);
    bookListNode := gms_xmldom.getfirstchild(docNode);
    --写入buffer
    buf := gms_xmldom.writetobuffer(doc,buf);
    --输出buf内容  
    raise notice '%', ('XML内容:' ||buf);
    --获取对应子元素
    nodelist := gms_XMLDOM.getElementsByTagName(doc, 'book');
    node := gms_XMLDOM.item(nodelist,0);

    --创建DOMElement对象
    createelem := gms_XMLDOM.createelement(doc,'test');
    elemnode := gms_XMLDOM.makenode(createelem);
    --创建内容
    text  := gms_XMLDOM.createTextnode(doc,'testtext');
    textnode := gms_XMLDOM.makenode(text);
    resNode := gms_XMLDOM.appendchild(elemnode,textnode);

    --添加到指定位置
    resNode := gms_XMLDOM.appendchild(node,elemnode);
    --写入buffer
    buf := gms_xmldom.writetobuffer(doc,buf);
    --输出buf内容 
    raise notice '%', ('appendchild后:' ||buf);
END;
/

--(71)getNodeValueAsClob(n domnode)
--获取CData片段节点的值
DECLARE
    var clob; 
    doc gms_XMLDOM.DOMDocument;
    ndoc gms_xmldom.DOMNode;
    node gms_XMLDOM.DOMNode;
    nodelist gms_XMLDOM.DOMNodelist;
    listlength integer;
    buf varchar2(4000);
    n integer := 0;
BEGIN
    var := xml('<booklist type="science and engineering">
  <![CDATA[testcdata1]]>
  <book category="math">
    <title>learning math</title>
    <author>张三</author>
    <pageNumber>561</pageNumber>
  </book>
  <![CDATA[testcdata2]]>
  <book category="Python">
    <title>learning Python</title>
    <author>李四</author>
    <pageNumber>600</pageNumber>
  </book>
  <![CDATA[testcdata3]]>
  <book category="C++">
    <title>learning C++</title>
    <author>王二</author>
    <pageNumber>500</pageNumber>
  </book>
</booklist>');
    --返回新的document实例
    doc := gms_XMLDOM.newDOMDocument(var);
    ndoc := gms_xmldom.makenode(doc);
    --写入buffer
    buf := gms_xmldom.writetobuffer(doc,buf);
    raise notice '%', ('XML内容:' ||buf);
    
    node := gms_xmldom.getfirstchild(ndoc);
    --获取第一层子节点列表
    nodelist := gms_xmldom.getchildnodes(node);
    listlength := gms_XMLDOM.getlength(nodelist);
    --检索第一层子节点
    FOR i in 0..(listlength-1) loop
        node := gms_XMLDOM.item(nodelist,i);
        --筛选CDATA片段节点
        IF gms_xmldom.getnodetype(node) = gms_xmldom.cdata_section_node THEN
            n := n+1;
            --输出CDATA片段节点的值
            raise notice '%', ('第' || n || '个CDATA片段值为' || gms_XMLDOM.getNodeValueAsClob(node) );
        END IF;
    END LOOP;
END;
/

--(72)getOwnerDocument(n DOMNode)
DECLARE
    var clob;
    doc gms_XMLDOM.DOMDocument;
    doc1 gms_XMLDOM.DOMDocument;  
    ndoc gms_xmldom.DOMNode;
    node gms_XMLDOM.DOMNode;
    nodelist gms_XMLDOM.DOMNodelist;  
    buf varchar2(4000); 
BEGIN
    var := '<booklist type="science and engineering">
  <!--comment-->
  <![CDATA[cdata]]>
  <book category="math">
    <title>learning math</title>
    <author>张三</author>
    <pageNumber>561</pageNumber>
  </book>
  <book category="Python">
    <title>learning Python</title>
    <author>李四</author>
    <pageNumber>600</pageNumber>
  </book>
  <book category="C++">
    <title>learning C++</title>
    <author>王二</author>
    <pageNumber>500</pageNumber>
  </book>
</booklist>';
    --返回新的document
    doc :=   gms_XMLDOM.newDOMDocument(var);
    ndoc := gms_xmldom.makenode(doc);
    --无关联document的节点
    doc1 := gms_XMLDOM.getownerdocument(node);
    --写入buffer
    buf := gms_xmldom.writetobuffer(doc1,buf);
    --输出buf内容
    raise notice '%', ('无关联document的节点:' ||buf); 
    --获取节点列表
    node := gms_XMLDOM.getfirstchild(ndoc);
    nodelist := gms_XMLDOM.getchildnodes(node);
    node := gms_XMLDOM.item(nodelist,0);
    --有关联document的节点
    doc1 := gms_XMLDOM.getownerdocument(node);
    --写入buffer
    buf := gms_xmldom.writetobuffer(doc1,buf);
    --输出buf内容
    raise notice '%', ('有关联document的节点:' ||buf); 
END;
/
--(73)gms_xmlparser.newParser
--(74)gms_xmlparser.parseClob(p gms_xmlparser.Parser, doc CLOB)
--(75)gms_xmlparser.getDocument(p gms_xmlparser.Parser)
DECLARE
    var clob;
    parser gms_XMLPARSER.parser;
    doc gms_XMLDOM.DOMDocument;
    buf varchar2(4000);
BEGIN
    var := '<booklist type="science and engineering">
  <book category="math">
    <title>learning math</title>
    <author>张三</author>
    <pageNumber>561</pageNumber>
  </book>
  <book category="Python">
    <title>learning Python</title>
    <author>李四</author>
    <pageNumber>600</pageNumber>
  </book>
  <book category="C++">
    <title>learning C++</title>
    <author>王二</author>
    <pageNumber>500</pageNumber>
  </book>
</booklist>';
    parser := gms_XMLPARSER.newParser();
    gms_XMLPARSER.parseClob(parser, var);
    doc := gms_XMLPARSER.getDocument(parser);
    --写入buffer
    buf := gms_xmldom.writetobuffer(doc,buf);
    --输出buf内容
    raise notice '%', ('XML内容:' ||buf); 
END;
/

--(76)gms_xmlparser.parseBuffer(p gms_xmlparser.Parser,doc VARCHAR2)
DECLARE
    var varchar2(4000);
    parser gms_XMLPARSER.parser;
    doc gms_XMLDOM.DOMDocument;
    buf varchar2(4000);
BEGIN
    var := '<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE COMPANY [
    <!ELEMENT COMPANY (NAME, ADDRESS)>
    <!ELEMENT NAME (#PCDATA)>
    <!ELEMENT ADDRESS (#PCDATA)>
    <!ENTITY name "青岛">
    <!ENTITY address "宁夏路">
]>
<COMPANY>
    <NAME>&name; </NAME>
    <ADDRESS>&address; </ADDRESS>
</COMPANY>';
    parser := gms_XMLPARSER.newParser();
    gms_XMLPARSER.parsebuffer(parser,var);
    doc := gms_XMLPARSER.getDocument(parser);
    --写入buffer
    buf := gms_xmldom.writetobuffer(doc,buf);
    --输出buf内容
    raise notice '%', ('XML内容:' ||buf); 
END;
/

DECLARE
    var xml;
    doc gms_xmldom.DOMDocument;
    wclob clob;
BEGIN
    var := xml('<booklist type="science and engineering">
  <book category="math">
    <title>learning math</title>

    <author>张三</author>
    <pageNumber>561</pageNumber>


  </book>
</booklist>');
    doc := gms_xmldom.newDOMDocument(var);

    wclob := gms_xmldom.writeToClob(doc, wclob);
    --输出修改后的clob内容  
    raise notice '%', ('xml内容为：' || wclob);
END;
/

DECLARE
  l_dom gms_xmldom.DOMDocument;
  l_clob clob := '<jwg><abc></abc></jwg>';
  v_element gms_xmldom.domelement;
  l_nodelist1 gms_xmldom.domnodelist;
  l_nodelist2 gms_xmldom.domnodelist;
  l_node gms_xmldom.DOMNode;
begin
  l_dom := gms_xmldom.newDomDocument(l_clob);
  l_nodelist1 := gms_xmldom.getelementsbytagname(l_dom,'jwg'); 
  l_node := gms_xmldom.item(l_nodelist1,0);
  v_element := gms_xmldom.makeelement(l_node);
  l_nodelist2 := gms_xmldom.getelementsbytagname(l_dom,'abc');
  l_node := gms_xmldom.item(l_nodelist2,0);
  raise notice '%', (gms_xmldom.getnodetype(gms_xmldom.getfirstchild(l_node)));
end;
/
DECLARE
  l_dom gms_xmldom.DOMDocument;
  l_clob clob := '<jwg><abc></abc></jwg>';
  v_element gms_xmldom.domelement;
  l_nodelist gms_xmldom.domnodelist;
  l_node gms_xmldom.DOMNode;
begin
  l_dom := gms_xmldom.newDomDocument(l_clob);
  l_nodelist := gms_xmldom.getelementsbytagname(l_dom,'abc');
  l_node := gms_xmldom.item(l_nodelist,0);
  gms_xmldom.freeNode(gms_xmldom.getfirstchild(l_node)); 
end;
/

--makeCharacterData函数
declare 
	v_clob clob; 
	v_doc gms_xmldom.domdocument; 
	v_nodelist gms_xmldom.domnodelist; 
	v_node1 gms_xmldom.domnode; 
	v_chardata1 gms_xmldom.DOMCharacterData;
	v_char1 varchar2(100);
begin 
	v_clob:='<?xml version="1.0" encoding="UTF-8"?> 
<document> 
	<row> 
	<organization>手动测试</organization> 
	<phones>测试勣</phones> 
	<persons>中文测试</persons>
	</row>
</document> ';

	v_doc := gms_xmldom.newdomdocument(v_clob); 
	v_nodelist := gms_xmldom.getelementsbytagname(v_doc, 'persons'); 
	v_node1 := gms_xmldom.getfirstchild(gms_xmldom.item(v_nodelist, 0)); 
	v_char1 := gms_xmldom.getnodevalue(v_node1); 
	raise notice '%', (v_char1);

	v_chardata1 := gms_xmldom.makeCharacterData(v_node1);
	raise notice '%', (gms_XMLDOM.GETLENGTH(v_chardata1));
end; 
/