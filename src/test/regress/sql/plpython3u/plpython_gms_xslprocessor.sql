----gms_xslprocessor.selectnodes
----gms_xslprocessor.valueof

declare
  x xml;
  doc gms_xmldom.DOMDocument;
  node gms_xmldom.DOMNODE;
  nodelist gms_XMLDOM.DOMNodelist;
  len int;
  title varchar2(100);
  author varchar2(100);
  outstr text;
begin
  x := xml(
      '<bookstore>
        <book category="COOKING">
          <title lang="en">Everyday Italian</title>
          <author>Giada De Laurentiis</author>
          <year>2005</year>
          <price>30.00</price>
        </book>
        <book category="CHILDREN">
          <title lang="en">Harry Potter</title>
          <author>J.K. Rowling</author>
          <year>2005</year>
          <price>29.99</price>
        </book>
      </bookstore>'
    );
  doc := gms_xmldom.newDomDocument(x);
  node := gms_xmldom.makenode(gms_xmldom.getDocumentElement(doc));
  nodelist := gms_xslprocessor.selectnodes(node,'book');
  len := gms_xmldom.getLength(nodelist);
  for i in 0..len-1 loop
      node := gms_xmldom.item(nodelist, i);
      gms_xslprocessor.valueof(node, 'title/text()', title, null);
      gms_xslprocessor.valueof(node, 'author/text()', author, null);
      outstr := 'title: ' || title || ', ' || 'author: ' || author;
      raise notice '%', outstr;
    end loop;
end;
/

declare
  x xml;
  doc gms_xmldom.DOMDocument;
  node gms_xmldom.DOMNODE;
  nodelist gms_XMLDOM.DOMNodelist;
  len int;
  title varchar2(100);
  author varchar2(100);
  outstr text;
begin
  x := xml(
      '<bookstore>
        <book category="COOKING">
          <title lang="en">Everyday Italian</title>
          <author>Giada De Laurentiis</author>
          <year>2005</year>
          <price>30.00</price>
        </book>
        <book category="CHILDREN">
          <title lang="en">Harry Potter</title>
          <author>J.K. Rowling</author>
          <year>2005</year>
          <price>29.99</price>
        </book>
      </bookstore>'
    );
  doc := gms_xmldom.newDomDocument(x);
  node := gms_xmldom.makenode(gms_xmldom.getDocumentElement(doc));
  nodelist := gms_xslprocessor.selectnodes(node,'//*[not(*)]');
  len := gms_xmldom.getLength(nodelist);
  for i in 0..len-1 loop
      node := gms_xmldom.item(nodelist, i);
      gms_xslprocessor.valueof(node, 'title/text()', title, null);
      gms_xslprocessor.valueof(node, 'author/text()', author, null);
      outstr := 'title: ' || title || ', ' || 'author: ' || author;
      raise notice '%', outstr;
    end loop;
end;
/

--路径错误
declare
  x xml;
  doc gms_xmldom.DOMDocument;
  node gms_xmldom.DOMNODE;
  nodelist gms_XMLDOM.DOMNodelist;
  len int;
  title varchar2(100);
  author varchar2(100);
  outstr text;
begin
  x := xml(
      '<bookstore>
        <book category="COOKING">
          <title lang="en">Everyday Italian</title>
          <author>Giada De Laurentiis</author>
          <year>2005</year>
          <price>30.00</price>
        </book>
        <book category="CHILDREN">
          <title lang="en">Harry Potter</title>
          <author>J.K. Rowling</author>
          <year>2005</year>
          <price>29.99</price>
        </book>
      </bookstore>'
    );
  doc := gms_xmldom.newDomDocument(x);
  node := gms_xmldom.makenode(gms_xmldom.getDocumentElement(doc));
  nodelist := gms_xslprocessor.selectnodes(node,'book/author/year');
  len := gms_xmldom.getLength(nodelist);
  for i in 0..len-1 loop
      node := gms_xmldom.item(nodelist, i);
      gms_xslprocessor.valueof(node, 'title/text()', title, null);
      gms_xslprocessor.valueof(node, 'author/text()', author, null);
      outstr := 'title: ' || title || ', ' || 'author: ' || author;
      raise notice '%', outstr;
    end loop;
end;
/

--入参为null
declare
  doc gms_xmldom.DOMDocument;
  node gms_xmldom.DOMNODE;
  nodelist gms_XMLDOM.DOMNodelist;
  len int;
  title varchar2(100);
  author varchar2(100);
  outstr text;
begin
  nodelist := gms_xslprocessor.selectnodes(NULL,'book/author/year');
  len := gms_xmldom.getLength(nodelist);
  for i in 0..len-1 loop
      node := gms_xmldom.item(nodelist, i);
      gms_xslprocessor.valueof(node, 'title/text()', title, null);
      gms_xslprocessor.valueof(node, 'author/text()', author, null);
      outstr := 'title: ' || title || ', ' || 'author: ' || author;
      raise notice '%', outstr;
    end loop;
end;
/

declare
  x xml;
  doc gms_xmldom.DOMDocument;
  node gms_xmldom.DOMNODE;
  nodelist gms_XMLDOM.DOMNodelist;
  len int;
  title varchar2(100);
  author varchar2(100);
  outstr text;
begin
  x := xml(
      '<bookstore>
        <book category="COOKING">
          <title lang="en">Everyday Italian</title>
          <author>Giada De Laurentiis</author>
          <year>2005</year>
          <price>30.00</price>
        </book>
        <book category="CHILDREN">
          <title lang="en">Harry Potter</title>
          <author>J.K. Rowling</author>
          <year>2005</year>
          <price>29.99</price>
        </book>
      </bookstore>'
    );
  doc := gms_xmldom.newDomDocument(x);
  node := gms_xmldom.makenode(gms_xmldom.getDocumentElement(doc));
  nodelist := gms_xslprocessor.selectnodes(node,'');
  len := gms_xmldom.getLength(nodelist);
  for i in 0..len-1 loop
      node := gms_xmldom.item(nodelist, i);
      gms_xslprocessor.valueof(node, 'title/text()', title, null);
      gms_xslprocessor.valueof(node, 'author/text()', author, null);
      outstr := 'title: ' || title || ', ' || 'author: ' || author;
      raise notice '%', outstr;
    end loop;
end;
/

--指定namespace
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

    nodelist := gms_xslprocessor.selectnodes(root,'book','http://www.runoob.com/xml/');
    len := gms_xmldom.getLength(nodeList);
    for i in 0..len-1 loop
      node := gms_xmldom.item(nodeList,i);
      nodeElem := gms_xmldom.makeElement(node);
      attr := gms_xmldom.createAttribute(doc, 'num', 'http://www.runoob.com/xml/');
      resAttr := gms_xmldom.setAttributeNode(nodeElem, attr, 'http://www.runoob.com/xml/');
    end loop;
END;
/
