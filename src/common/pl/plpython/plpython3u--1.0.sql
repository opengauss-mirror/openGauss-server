/* src/pl/plpython/plpython3u--1.0.sql */

/*
 * Currently, all the interesting stuff is done by CREATE LANGUAGE.
 * Later we will probably "dumb down" that command and put more of the
 * knowledge into this script.
 */

CREATE PROCEDURAL LANGUAGE plpython3u;

COMMENT ON PROCEDURAL LANGUAGE plpython3u IS 'PL/Python3U untrusted procedural language';

--"
--gms_xmldom
create schema gms_xmldom;
GRANT USAGE ON SCHEMA gms_xmldom TO public;

create domain gms_xmldom.DOMType as text;
create type gms_xmldom.DOMNode as (id text);
create type gms_xmldom.DOMNamedNodeMap as (id text);
create type gms_xmldom.DOMNodeList as (id text);
create type gms_xmldom.DOMAttr as (id text);
create type gms_xmldom.DOMCDataSection as (id text);
create type gms_xmldom.DOMCharacterData as (id text);
create type gms_xmldom.DOMComment as (id text);
create type gms_xmldom.DOMDocumentFragment as (id text);
create type gms_xmldom.DOMElement as (id text);
create type gms_xmldom.DOMEntity as (id text);
create type gms_xmldom.DOMNotation as (id text);
create type gms_xmldom.DOMProcessingInstruction as (id text);
create type gms_xmldom.DOMText as (id text);
create type gms_xmldom.DOMImplementation as (id text);
create type gms_xmldom.DOMDocumentType as (id text);
create type gms_xmldom.DOMDocument as (id text);

--gms_xmldom.makeNode
create function gms_xmldom.makeNode(t gms_xmldom.DOMText)
returns gms_xmldom.DOMNode
as $$
    return t
$$ language plpython3u package;
create function gms_xmldom.makeNode(com gms_xmldom.DOMComment)
returns gms_xmldom.DOMNode
as $$
    return com
$$ language plpython3u package;
create function gms_xmldom.makeNode(cds gms_xmldom.DOMCDATASection)
returns gms_xmldom.DOMNode
as $$
    return cds
$$ language plpython3u package;
create function gms_xmldom.makeNode(dt gms_xmldom.DOMDocumentType)
returns gms_xmldom.DOMNode
as $$
    return dt
$$ language plpython3u package;
create function gms_xmldom.makeNode(n gms_xmldom.DOMNotation)
returns gms_xmldom.DOMNode
as $$
    return n
$$ language plpython3u package;
create function gms_xmldom.makeNode(ent gms_xmldom.DOMEntity)
returns gms_xmldom.DOMNode
as $$
    return ent
$$ language plpython3u package;
create function gms_xmldom.makeNode(pi gms_xmldom.DOMProcessingInstruction)
returns gms_xmldom.DOMNode
as $$
    return pi
$$ language plpython3u package;
create function gms_xmldom.makeNode(df gms_xmldom.DOMDocumentFragment)
returns gms_xmldom.DOMNode
as $$
    return df
$$ language plpython3u package;
create function gms_xmldom.makeNode(doc gms_xmldom.DOMDocument)
returns gms_xmldom.DOMNode
as $$
    return doc
$$ language plpython3u package;
create function gms_xmldom.makeNode(elem gms_xmldom.DOMElement)
returns gms_xmldom.DOMNode
as $$
    return elem
$$ language plpython3u package;

--gms_xmldom.isNull(n DOMNode)
create function gms_xmldom.isNull(n gms_xmldom.DOMNode)
returns boolean
as $$
    if n["id"] not in GD:
        return True
    else:
        return False
$$ language plpython3u package;
--gms_xmldom.isNull(di DOMImplementation)
create function gms_xmldom.isNull(di gms_xmldom.DOMImplementation)
returns boolean
as $$
    if di["id"] not in GD:
        return True
    else:
        return False
$$ language plpython3u package;
--gms_xmldom.isNull(nl DOMNodeList)
create function gms_xmldom.isNull(nl gms_xmldom.DOMNodeList)
returns boolean
as $$
    if nl["id"] not in GD:
        return True
    else:
        return False
$$ language plpython3u package;
--gms_xmldom.isNull(nnm DOMNamedNodeMap)
create function gms_xmldom.isNull(nnm gms_xmldom.DOMNamedNodeMap)
returns boolean
as $$
    if nnm["id"] not in GD:
        return True
    else:
        return False
$$ language plpython3u package;
--gms_xmldom.isNull(cd DOMCharacterData)
create function gms_xmldom.isNull(cd gms_xmldom.DOMCharacterData)
returns boolean
as $$
    if cd["id"] not in GD:
        return True
    else:
        return False
$$ language plpython3u package;
--gms_xmldom.isNull(a DOMAttr)
create function gms_xmldom.isNull(a gms_xmldom.DOMAttr)
returns boolean
as $$
    if a["id"] not in GD:
        return True
    else:
        return False
$$ language plpython3u package;
--gms_xmldom.isNull(elem DOMElement)
create function gms_xmldom.isNull(elem gms_xmldom.DOMElement)
returns boolean
as $$
    if elem["id"] not in GD:
        return True
    else:
        return False
$$ language plpython3u package;
--gms_xmldom.isNull(t DOMText)
create function gms_xmldom.isNull(t gms_xmldom.DOMText)
returns boolean
as $$
    if t["id"] not in GD:
        return True
    else:
        return False
$$ language plpython3u package;
--gms_xmldom.isNull(com DOMComment)
create function gms_xmldom.isNull(com gms_xmldom.DOMComment)
returns boolean
as $$
    if com["id"] not in GD:
        return True
    else:
        return False
$$ language plpython3u package;
--gms_xmldom.isNull(cds DOMCDATASection)
create function gms_xmldom.isNull(cds gms_xmldom.DOMCDATASection)
returns boolean
as $$
    if cds["id"] not in GD:
        return True
    else:
        return False
$$ language plpython3u package;
--gms_xmldom.isNull(dt DOMDocumentType)
create function gms_xmldom.isNull(dt gms_xmldom.DOMDocumentType)
returns boolean
as $$
    if dt["id"] not in GD:
        return True
    else:
        return False
$$ language plpython3u package;
--gms_xmldom.isNull(n DOMNotation)
create function gms_xmldom.isNull(n gms_xmldom.DOMNotation)
returns boolean
as $$
    if n["id"] not in GD:
        return True
    else:
        return False
$$ language plpython3u package;
--gms_xmldom.isNull(ent DOMEntity)
create function gms_xmldom.isNull(ent gms_xmldom.DOMEntity)
returns boolean
as $$
    if ent["id"] not in GD:
        return True
    else:
        return False
$$ language plpython3u package;
--gms_xmldom.isNull(pi DOMProcessingInstruction)
create function gms_xmldom.isNull(pi gms_xmldom.DOMProcessingInstruction)
returns boolean
as $$
    if pi["id"] not in GD:
        return True
    else:
        return False
$$ language plpython3u package;
--gms_xmldom.isNull(df DOMDocumentFragment)
create function gms_xmldom.isNull(df gms_xmldom.DOMDocumentFragment)
returns boolean
as $$
    if df["id"] not in GD:
        return True
    else:
        return False
$$ language plpython3u package;
--gms_xmldom.isNull(doc DOMDocument)
create function gms_xmldom.isNull(doc gms_xmldom.DOMDocument)
returns boolean
as $$
    if doc["id"] not in GD:
        return True
    else:
        return False
$$ language plpython3u package;

--gms_xmldom.freeNode(n DOMnode)
create function gms_xmldom.freeNode(n gms_xmldom.DOMnode)
returns void
as $$
    from xml.dom import minidom
    if n["id"] not in GD or not GD[n["id"]]:
        return
    domNode = GD[n["id"]]
    stack = [domNode]
    while stack:
        root = stack.pop()
        tmpid = str(id(root))
        if tmpid in GD:
            GD.pop(tmpid)
        for child in root.childNodes:
            stack.append(child)
    domNode.unlink()
$$ language plpython3u package;

--gms_xmldom.freeNodeList(nl DOMNodeList)
create function gms_xmldom.freeNodeList(nl gms_xmldom.DOMNodeList)
returns void
as $$
    from xml.dom import minidom
    if nl["id"] not in GD or not GD[nl["id"]]:
        return
    nodeList = GD[nl["id"]]
    for node in nodeList:
        strid = str(id(node))
        if strid in GD:
            GD.pop(strid)
        node.unlink()
    GD.pop(nl["id"])
$$ language plpython3u package;
--gms_xmldom.freeDocument
create function gms_xmldom.freeDocument(doc gms_xmldom.DOMDocument)
returns void
as $$
    from xml.dom import minidom
    if doc["id"] not in GD or not GD[doc["id"]]:
        return
    docNode = GD[doc["id"]]
    stack = [docNode]
    while stack:
        root = stack.pop()
        tmpid = str(id(root))
        if tmpid in GD:
            GD.pop(tmpid)
        for child in root.childNodes:
            stack.append(child)
    docNode.unlink()
$$ language plpython3u package;

--gms_xmldom.getFirstChild(n DOMNode)
create function gms_xmldom.getFirstChild(n gms_xmldom.DOMNode)
returns gms_xmldom.DOMNode
as $$
    from xml.dom import minidom
    if n["id"] not in GD or not GD[n["id"]]:
        return [""]
    domNode = GD[n["id"]]
    if domNode.hasChildNodes == False:
        return [""]
    childNode = domNode.firstChild
    resid = str(id(childNode))
    if resid not in GD:
        GD[resid] = childNode
    return [resid]
$$ language plpython3u package;

--gms_xmldom.getLocalName
create function gms_xmldom.getLocalName(a gms_xmldom.DOMAttr)
returns varchar2
as $$
    from xml.dom import minidom
    if a["id"] not in GD or not GD[a["id"]]:
        return ""
    attrNode = GD[a["id"]]
    return attrNode.localName
$$ language plpython3u package;
create function gms_xmldom.getLocalName(elem gms_xmldom.DOMElement)
returns varchar2
as $$
    from xml.dom import minidom
    if elem["id"] not in GD or not GD[elem["id"]]:
        return ""
    attrNode = GD[elem["id"]]
    return attrNode.localName
$$ language plpython3u package;
create function gms_xmldom.getLocalName(n gms_xmldom.DOMnode, data OUT VARCHAR2)
as $$
    from xml.dom import minidom
    if n["id"] not in GD or not GD[n["id"]]:
        return ""
    attrNode = GD[n["id"]]
    data = attrNode.localName
    return data
$$ language plpython3u package;

--gms_xmldom.getNodeType
create function gms_xmldom.getNodeType(n gms_xmldom.DOMNode)
returns INTEGER
as $$
    from xml.dom import minidom
    if n["id"] not in GD or not GD[n["id"]]:
        return None
    domNode = GD[n["id"]]
    return domNode.nodeType
$$ language plpython3u package;

--gms_xmldom.internal_writexml(DOMNode, VARCHAR2)
create function gms_xmldom.internal_writexml(n gms_xmldom.DOMNode, dbencoding VARCHAR2)
returns clob
as $$
    from xml.dom import minidom
    from xml.dom import Node
    if n["id"] not in GD or not GD[n["id"]]:
        return ""
    domNode = GD[n["id"]]
    cl = ''
    #If the document node needs to be written in the specified character set
    if domNode.nodeType == Node.DOCUMENT_NODE:
        #If a character set is specified, the specified character set is used. 
        #If no character set is specified, the database character set is used
        tarEncoding = dbencoding
        if domNode.encoding != None:
            tarEncoding = domNode.encoding
        cl = domNode.toprettyxml(indent="  ", newl="\n", encoding=tarEncoding)
        cl = cl.decode(encoding=tarEncoding)
        if domNode.version != None and domNode.version != "1.0":
            versionInfo = "version=\"" + domNode.version + "\""
            cl = cl.replace("version=\"1.0\"", versionInfo)
    else:
        cl = domNode.toprettyxml(indent="  ")
    return cl
$$ language plpython3u package;
--gms_xmldom.internal_writexml(DOMDocument, VARCHAR2)
create function gms_xmldom.internal_writexml(doc gms_xmldom.DOMDocument, dbencoding varchar2)
returns clob
as $$
    from xml.dom import minidom
    if doc["id"] not in GD or not GD[doc["id"]]:
        return ""
    docNode = GD[doc["id"]]
    #If a character set is specified, the specified character set is used. 
    #If no character set is specified, the database character set is used
    tarEncoding = dbencoding
    if docNode.encoding != None:
        tarEncoding = docNode.encoding
    cl = docNode.toprettyxml(indent="  ", newl="\n", encoding=tarEncoding)
    cl = cl.decode(encoding=tarEncoding)
    #modify version
    if docNode.version != None and docNode.version != "1.0":
        versionInfo = "version=\"" + docNode.version + "\""
        cl = cl.replace("version=\"1.0\"", versionInfo)
    return cl
$$ language plpython3u package;
--gms_xmldom.internal_writexml(DOMNode, number, number, varchar2)
create function gms_xmldom.internal_writexml(
    n gms_xmldom.DOMNode,
    pflag IN NUMBER, 
    indent IN NUMBER,
    dbencoding varchar2)
returns clob
as $$
    from xml.dom import minidom
    from xml.dom import Node
    iPflag = int(pflag)
    iIndent = int(indent)
    if iPflag < 0 or iPflag > 72 or iIndent < 0 or iIndent > 12:
        raise plpy.Error("The specified printing option is invalid")
    if n["id"] not in GD or not GD[n["id"]]:
        return ""
    domNode = GD[n["id"]]

    indentstr = ""
    newl = "\n"

    lowPflag = iPflag & 7
    # According to Oracle, a low three digit of 4 or 5 indicates no line break
    if lowPflag == 4 or lowPflag == 5:
        newl = ""
    else:
        i = 0
        while i < iIndent:
            indentstr += " "
            i += 1
    cl = ''
    if domNode.nodeType == Node.DOCUMENT_NODE:
        tarEncoding = dbencoding
        if domNode.encoding != None:
            tarEncoding = domNode.encoding
        cl = domNode.toprettyxml(indentstr, newl, tarEncoding)
        cl = cl.decode(encoding=tarEncoding)
        if domNode.version != None and domNode.version != "1.0":
            versionInfo = "version=\"" + domNode.version + "\""
            cl = cl.replace("version=\"1.0\"", versionInfo)
    else:
        cl = domNode.toprettyxml(indentstr, newl)
    return cl
$$ language plpython3u package;
--gms_xmldom.internal_writexml(DOMDocument, number, number, varchar2)
create function gms_xmldom.internal_writexml(
    doc gms_xmldom.DOMDocument, 
    pflag IN NUMBER, 
    indent IN NUMBER,
    dbencoding varchar2)
returns clob
as $$
    from xml.dom import minidom
    iPflag = int(pflag)
    iIndent = int(indent)
    if iPflag < 0 or iPflag > 72 or iIndent < 0 or iIndent > 12:
        raise plpy.Error("The specified printing option is invalid")
    if doc["id"] not in GD or not GD[doc["id"]]:
        return ""
    docNode = GD[doc["id"]]

    indentstr = ""
    newl = "\n"

    lowPflag = iPflag & 7
    # According to Oracle, a low three digit of 4 or 5 indicates no line break
    if lowPflag == 4 or lowPflag == 5:
        newl = ""
    else:
        i = 0
        while i < iIndent:
            indentstr += " "
            i += 1
    tarEncoding = dbencoding
    if docNode.encoding != None:
        tarEncoding = docNode.encoding
    cl = docNode.toprettyxml(indentstr, newl, tarEncoding)
    cl = cl.decode(encoding=tarEncoding)
    if docNode.version != None and docNode.version != "1.0":
        versionInfo = "version=\"" + docNode.version + "\""
        cl = cl.replace("version=\"1.0\"", versionInfo)
    return cl
$$ language plpython3u package;
--gms_xmldom.writeToClob(DOMNode, clob)
create function gms_xmldom.writeToClob(n gms_xmldom.DOMNode, cl IN OUT CLOB)
as $$
declare
    dbencoding varchar2;
begin
    show server_encoding into dbencoding;
    cl := gms_xmldom.internal_writexml(n, dbencoding);
    return cl;
end;
$$ language plpgsql package;
--gms_xmldom.writeToClob(DOMNode, clob, number, number)
create function gms_xmldom.writeToClob(n gms_xmldom.DOMNode, cl IN OUT CLOB, pflag IN NUMBER, indent IN NUMBER)
as $$
declare
    dbencoding varchar2;
begin
    show server_encoding into dbencoding;
    cl := gms_xmldom.internal_writexml(n, pflag, indent, dbencoding);
    return cl;
end;
$$ language plpgsql package;
--gms_xmldom.writeToClob(DOMDocument, clob)
create function gms_xmldom.writeToClob(doc gms_xmldom.DOMDocument, cl IN OUT CLOB)
as $$
declare
    dbencoding varchar2;
begin
    show server_encoding into dbencoding;
    cl := gms_xmldom.internal_writexml(doc, dbencoding);
    return cl;
end;
$$ language plpgsql package;
--gms_xmldom.writeToClob(DOMDocument, clob, number, number)
create function gms_xmldom.writeToClob(doc gms_xmldom.DOMDocument, cl IN OUT CLOB, pflag IN NUMBER, indent IN NUMBER)
as $$
declare
    dbencoding varchar2;
begin
    show server_encoding into dbencoding;
    cl := gms_xmldom.internal_writexml(doc, pflag, indent, dbencoding);
    return cl;
end;
$$ language plpgsql package;

--gms_xmldom.writeToBuffer(DOMNode, VARCHAR2)
create function gms_xmldom.writeToBuffer(n gms_xmldom.DOMNode, buffer IN OUT VARCHAR2)
as $$
declare
    db_encoding varchar2;
begin
    show server_encoding into db_encoding;
    buffer := gms_xmldom.internal_writexml(n, db_encoding);
    return buffer;
end;
$$ language plpgsql package;
--gms_xmldom.writeToBuffer(DOMDocument, VARCHAR2)
create function gms_xmldom.writeToBuffer(doc gms_xmldom.DOMDocument, buffer IN OUT VARCHAR2)
as $$
declare
    db_encoding varchar2;
begin
    show server_encoding into db_encoding;
    buffer := gms_xmldom.internal_writexml(doc, db_encoding);
    return buffer;
end;
$$ language plpgsql package;
--gms_xmldom.writeToBuffer(DOMDocumentFragment, VARCHAR2)
create function gms_xmldom.writeToBuffer(df gms_xmldom.DOMDocumentFragment, buffer IN OUT VARCHAR2)
as $$
    import os
    from xml.dom import minidom
    import io
    if df["id"] not in GD or not GD[df["id"]]:
        return ""
    dfNode = GD[df["id"]]
    writer = io.StringIO()
    for node in dfNode.childNodes:
        node.writexml(writer, "  ", "", "")
    buffer = writer.getvalue()
    return buffer
$$ language plpython3u package;

--gms_xmldom.getChildNodes
create function gms_xmldom.getChildNodes(n gms_xmldom.DOMNode)
returns gms_xmldom.DOMNodeList
as $$
    from xml.dom import minidom
    if n["id"] not in GD or not GD[n["id"]]:
        return [""]
    domNode = GD[n["id"]]
    childNodes = domNode.childNodes
    resid = str(id(childNodes))
    if resid not in GD:
        GD[resid] = childNodes
    return [resid]
$$ language plpython3u package;

--gms_xmldom.getLength
create function gms_xmldom.getLength(nl gms_xmldom.DOMNodeList)
returns integer
as $$
    from xml.dom import minidom
    if nl["id"] not in GD or not GD[nl["id"]]:
        return 0
    nodeList = GD[nl["id"]]
    return nodeList.length
$$ language plpython3u package;
create function gms_xmldom.getLength(nnm gms_xmldom.DOMNamedNodeMap)
returns integer
as $$
    from xml.dom import minidom
    if nnm["id"] not in GD or not GD[nnm["id"]]:
        return 0
    nnmNode = GD[nnm["id"]]
    return nnmNode.length
$$ language plpython3u package;
create function gms_xmldom.getLength(cd gms_xmldom.DOMCharacterData)
returns integer
as $$
    from xml.dom import minidom
    if cd["id"] not in GD or not GD[cd["id"]]:
        return 0
    cdNode = GD[cd["id"]]
    return cdNode.length
$$ language plpython3u package;

--gms_xmldom.item
create function gms_xmldom.item(nl gms_xmldom.DOMNodeList, idx IN INTEGER)
returns gms_xmldom.DOMNode
as $$
    from xml.dom import minidom
    if nl["id"] not in GD or not GD[nl["id"]]:
        return [""]
    nodeList = GD[nl["id"]]
    domNode = nodeList.item(idx)
    if domNode == None:
        return [""]
    resid = str(id(domNode))
    if resid not in GD:
        GD[resid] = domNode
    return [resid]
$$ language plpython3u package;
create function gms_xmldom.item(nnm gms_xmldom.DOMNamedNodeMap, idx IN INTEGER)
returns gms_xmldom.DOMNode
as $$
    from xml.dom import minidom
    if nnm["id"] not in GD or not GD[nnm["id"]]:
        return [""]
    nnmNode = GD[nnm["id"]]
    domNode = nnmNode.item(idx)
    if domNode == None:
        return [""]
    resid = str(id(domNode))
    if resid not in GD:
        GD[resid] = domNode
    return [resid]
$$ language plpython3u package;

--gms_xmldom.makeElement
create function gms_xmldom.makeElement(n gms_xmldom.DOMNode)
returns gms_xmldom.DOMElement
as $$
    from xml.dom import minidom
    from xml.dom import Node
    if n["id"] not in GD or not GD[n["id"]]:
        return [""]
    domNode = GD[n["id"]]
    if domNode.nodeType == Node.ELEMENT_NODE:
        return n
    else:
        nodeName = ""
        if domNode.nodeType == Node.DOCUMENT_FRAGMENT_NODE:
            nodeName = "DocumentFragment"
        elif domNode.nodeType == Node.ATTRIBUTE_NODE:
            nodeName = "Attribute"
        elif domNode.nodeType == Node.PROCESSING_INSTRUCTION_NODE:
            nodeName = "ProcessingInstruction"
        elif domNode.nodeType == Node.TEXT_NODE:
            nodeName = "Text"
        elif domNode.nodeType == Node.COMMENT_NODE:
            nodeName = "Comment"
        elif domNode.nodeType == Node.CDATA_SECTION_NODE:
            nodeName = "CDATASection"
        elif domNode.nodeType == Node.DOCUMENT_TYPE_NODE:
            nodeName = "DocumentType"
        elif domNode.nodeType == Node.ENTITY_NODE:
            nodeName = "Entity"
        elif domNode.nodeType == Node.NOTATION_NODE:
            nodeName = "Notation"
        elif domNode.nodeType == Node.DOCUMENT_NODE:
            nodeName = "Document"
        raise plpy.Error("Node type %s cannot be converted to the target type" %nodeName)
$$ language plpython3u package;

--gms_xmldom.getElementsByTagName
create function gms_xmldom.getElementsByTagName(elem gms_xmldom.DOMElement, name IN VARCHAR2)
returns gms_xmldom.DOMNodeList
as $$
    from xml.dom import minidom
    if elem["id"] not in GD or not GD[elem["id"]]:
        return [""]
    elemNode = GD[elem["id"]]
    nlist = elemNode.getElementsByTagName(name)
    resid = str(id(nlist))
    if resid not in GD:
        GD[resid] = nlist
    return [resid]
$$ language plpython3u package;
create function gms_xmldom.getElementsByTagName(elem gms_xmldom.DOMElement, name IN VARCHAR2, ns varchar2)
returns gms_xmldom.DOMNodeList
as $$
    from xml.dom import minidom
    if elem["id"] not in GD or not GD[elem["id"]]:
        return [""]
    elemNode = GD[elem["id"]]
    nlist = elemNode.getElementsByTagNameNS(ns, name)
    resid = str(id(nlist))
    if resid not in GD:
        GD[resid] = nlist
    return [resid]
$$ language plpython3u package;
create function gms_xmldom.getElementsByTagName(doc gms_xmldom.DOMDocument, tagname IN VARCHAR2)
returns gms_xmldom.DOMNodeList
as $$
    from xml.dom import minidom
    if doc["id"] not in GD or not GD[doc["id"]]:
        return [""]
    docNode = GD[doc["id"]]
    nlist = docNode.getElementsByTagName(tagname)
    resid = str(id(nlist))
    if resid not in GD:
        GD[resid] = nlist
    return [resid]
$$ language plpython3u package;

--gms_xmldom.cloneNode
create function gms_xmldom.cloneNode(n gms_xmldom.DOMNode, deep boolean)
returns gms_xmldom.DOMNode
as $$
    from xml.dom import minidom
    if n["id"] not in GD or not GD[n["id"]]:
        return [""]
    domNode = GD[n["id"]]
    clnNode = domNode.cloneNode(deep)
    resid = str(id(clnNode))
    if resid not in GD:
        GD[resid] = clnNode
    return [resid]
$$ language plpython3u package;

--gms_xmldom.getNodeName
create function gms_xmldom.getNodeName(n gms_xmldom.DOMNode)
returns VARCHAR2
as $$
    from xml.dom import minidom
    if n["id"] not in GD or not GD[n["id"]]:
        return ""
    domNode = GD[n["id"]]
    return domNode.nodeName
$$ language plpython3u package;

--gms_xmldom.createDocument
create function gms_xmldom.createDocument(
    namespaceuri IN VARCHAR2, 
    qualifiedname IN VARCHAR2, 
    doctype IN gms_xmldom.DOMType:= NULL)
returns gms_xmldom.DOMDocument
as $$
    from xml.dom import minidom
    strdoctype = None
    if doctype != None and doctype in GD:
        strdoctype = GD[doctype]
    domNode = minidom.getDOMImplementation()
    docNode = domNode.createDocument(namespaceuri, qualifiedname, strdoctype)
    resid = str(id(docNode))
    if resid not in GD:
        GD[resid] = docNode
    return [resid]
$$ language plpython3u package;

--gms_xmldom.createElement
create function gms_xmldom.createElement(doc gms_xmldom.DOMDocument, tagname IN VARCHAR2)
returns gms_xmldom.DOMElement
as $$
    from xml.dom import minidom
    if doc["id"] not in GD or not GD[doc["id"]]:
        raise plpy.Error("DOMDocument is empty or invalid")
    docNode = GD[doc["id"]]
    elemNode = docNode.createElement(tagname)
    resid = str(id(elemNode))
    if resid not in GD:
        GD[resid] = elemNode
    return [resid]
$$ language plpython3u package;
create function gms_xmldom.createElement(doc gms_xmldom.DOMDocument, tagname IN VARCHAR2, ns IN VARCHAR2)
returns gms_xmldom.DOMElement
as $$
    from xml.dom import minidom
    if doc["id"] not in GD or not GD[doc["id"]]:
        raise plpy.Error("DOMDocument is empty or invalid")
    docNode = GD[doc["id"]]
    elemNode = docNode.createElementNS(ns, tagname)
    resid = str(id(elemNode))
    if resid not in GD:
        GD[resid] = elemNode
    return [resid]
$$ language plpython3u package;

--gms_xmldom.createDocumentFragment
create function gms_xmldom.createDocumentFragment(doc gms_xmldom.DOMDocument)
returns gms_xmldom.DOMDocumentFragment
as $$
    from xml.dom import minidom
    if doc["id"] not in GD or not GD[doc["id"]]:
        raise plpy.Error("DOMDocument is empty or invalid")
    docNode = GD[doc["id"]]
    dfNode = docNode.createDocumentFragment()
    resid = str(id(dfNode))
    if resid not in GD:
        GD[resid] = dfNode
    return [resid]
$$ language plpython3u package;

--gms_xmldom.createTextNode
create function gms_xmldom.createTextNode(doc gms_xmldom.DOMDocument, data IN VARCHAR2)
returns gms_xmldom.DOMText
as $$
    from xml.dom import minidom
    if doc["id"] not in GD or not GD[doc["id"]]:
        raise plpy.Error("DOMDocument is empty or invalid")
    docNode = GD[doc["id"]]
    txtNode = docNode.createTextNode(data)
    resid = str(id(txtNode))
    if resid not in GD:
        GD[resid] = txtNode
    return [resid]
$$ language plpython3u package;

--gms_xmldom.createComment
create function gms_xmldom.createComment(doc gms_xmldom.DOMDocument, data IN VARCHAR2)
returns gms_xmldom.DOMComment
as $$
    from xml.dom import minidom
    if doc["id"] not in GD or not GD[doc["id"]]:
        raise plpy.Error("DOMDocument is empty or invalid")
    docNode = GD[doc["id"]]
    comNode = docNode.createComment(data)
    resid = str(id(comNode))
    if resid not in GD:
        GD[resid] = comNode
    return [resid]
$$ language plpython3u package;

--gms_xmldom.createCDATASection
create function gms_xmldom.createCDATASection(doc gms_xmldom.DOMDocument, data IN VARCHAR2)
returns gms_xmldom.DOMCDATASection
as $$
    from xml.dom import minidom
    if doc["id"] not in GD or not GD[doc["id"]]:
        raise plpy.Error("DOMDocument is empty or invalid")
    docNode = GD[doc["id"]]
    cdsNode = docNode.createCDATASection(data)
    resid = str(id(cdsNode))
    if resid not in GD:
        GD[resid] = cdsNode
    return [resid]
$$ language plpython3u package;

--gms_xmldom.createProcessingInstruction
create function gms_xmldom.createProcessingInstruction(doc gms_xmldom.DOMDocument, target IN VARCHAR2, data IN VARCHAR2)
returns gms_xmldom.DOMProcessingInstruction
as $$
    from xml.dom import minidom
    if doc["id"] not in GD or not GD[doc["id"]]:
        raise plpy.Error("DOMDocument is empty or invalid")
    docNode = GD[doc["id"]]
    piNode = docNode.createProcessingInstruction(target, data)
    resid = str(id(piNode))
    if resid not in GD:
        GD[resid] = piNode
    return [resid]
$$ language plpython3u package;

--gms_xmldom.createAttribute
create function gms_xmldom.createAttribute(doc gms_xmldom.DOMDocument, name IN VARCHAR2)
returns gms_xmldom.DOMAttr
as $$
    from xml.dom import minidom
    if doc["id"] not in GD or not GD[doc["id"]]:
        raise plpy.Error("DOMDocument is empty or invalid")
    docNode = GD[doc["id"]]
    attrNode = docNode.createAttribute(name)
    resid = str(id(attrNode))
    if resid not in GD:
        GD[resid] = attrNode
    return [resid]
$$ language plpython3u package;
create function gms_xmldom.createAttribute(doc gms_xmldom.DOMDocument, name IN VARCHAR2, ns IN VARCHAR2)
returns gms_xmldom.DOMAttr
as $$
    from xml.dom import minidom
    if doc["id"] not in GD or not GD[doc["id"]]:
        raise plpy.Error("DOMDocument is empty or invalid")
    docNode = GD[doc["id"]]
    attrNode = docNode.createAttributeNS(ns, name)
    resid = str(id(attrNode))
    if resid not in GD:
        GD[resid] = attrNode
    return [resid]
$$ language plpython3u package;

--gms_xmldom.appendChild
create function gms_xmldom.appendChild(n gms_xmldom.DOMNode, newchild IN gms_xmldom.DOMNode)
returns gms_xmldom.DOMNode
as $$
    from xml.dom import minidom
    if n["id"] not in GD or not GD[n["id"]]:
        raise plpy.Error("DOMNode is empty or invalid")
    domNode = GD[n["id"]]
    if newchild["id"] not in GD or not GD[newchild["id"]]:
        raise plpy.Error("DOMNode is empty or invalid")
    newNode = GD[newchild["id"]]
    resNode = domNode.appendChild(newNode)
    resid = str(id(resNode))
    if resid not in GD:
        GD[resid] = resNode
    return [resid]
$$ language plpython3u package;

--gms_xmldom.getDocumentElement
create function gms_xmldom.getDocumentElement(doc gms_xmldom.DOMDocument)
returns gms_xmldom.DOMElement
as $$
    from xml.dom import minidom
    if doc["id"] not in GD or not GD[doc["id"]]:
        return [""]
    docNode = GD[doc["id"]]
    elemNode = docNode.documentElement
    resid = str(id(elemNode))
    if resid not in GD:
        GD[resid] = elemNode
    return [resid]
$$ language plpython3u package;

--gms_xmldom.setAttribute
create function gms_xmldom.setAttribute(elem gms_xmldom.DOMElement, name IN VARCHAR2, newvalue IN VARCHAR2)
returns void
as $$
    from xml.dom import minidom
    if elem["id"] not in GD or not GD[elem["id"]]:
        return
    elemNode = GD[elem["id"]]
    elemNode.setAttribute(name, newvalue)
$$ language plpython3u package;
create function gms_xmldom.setAttribute(elem gms_xmldom.DOMElement, name IN VARCHAR2, newvalue IN VARCHAR2, ns IN VARCHAR2)
returns void
as $$
    from xml.dom import minidom
    if elem["id"] not in GD or not GD[elem["id"]]:
        return
    elemNode = GD[elem["id"]]
    elemNode.setAttributeNS(ns, name, newvalue)
$$ language plpython3u package;

--gms_xmldom.setAttributeNode
create function gms_xmldom.setAttributeNode(elem gms_xmldom.DOMElement, newattr IN gms_xmldom.DOMAttr)
returns gms_xmldom.DOMAttr
as $$
    from xml.dom import minidom
    if elem["id"] not in GD or not GD[elem["id"]]:
        raise plpy.Error("DOMElement is empty or invalid")
    elemNode = GD[elem["id"]]
    if newattr["id"] not in GD or not GD[newattr["id"]]:
        raise plpy.Error("DOMAttr is empty or invalid")
    attrNode = GD[newattr["id"]]
    resAttrNode = elemNode.setAttributeNode(attrNode)
    resid = str(id(resAttrNode))
    if resid not in GD:
        GD[resid] = resAttrNode
    return [resid]
$$ language plpython3u package;
create function gms_xmldom.setAttributeNode(elem gms_xmldom.DOMElement, newattr IN gms_xmldom.DOMAttr, ns IN VARCHAR2)
returns gms_xmldom.DOMAttr
as $$
    from xml.dom import minidom
    if elem["id"] not in GD or not GD[elem["id"]]:
        raise plpy.Error("DOMElement is empty or invalid")
    elemNode = GD[elem["id"]]
    if newattr["id"] not in GD or not GD[newattr["id"]]:
        raise plpy.Error("DOMAttr is empty or invalid")
    attrNode = GD[newattr["id"]]
    attrNode.namespaceURI = ns
    resAttrNode = elemNode.setAttributeNode(attrNode)
    resid = str(id(resAttrNode))
    if resid not in GD:
        GD[resid] = resAttrNode
    return [resid]
$$ language plpython3u package;

--gms_xmldom.getAttributes
create function gms_xmldom.getAttributes(n gms_xmldom.DOMNode)
returns gms_xmldom.DOMNamedNodeMap
as $$
    from xml.dom import minidom
    if n["id"] not in GD or not GD[n["id"]]:
        return [""]
    domNode = GD[n["id"]]
    nnmNode = domNode.attributes
    resid = str(id(nnmNode))
    if resid not in GD:
        GD[resid] = nnmNode
    return [resid]
$$ language plpython3u package;

--gms_xmldom.getNodeValue
create function gms_xmldom.getNodeValue(n gms_xmldom.DOMNode)
returns varchar2
as $$
    from xml.dom import minidom
    if n["id"] not in GD or not GD[n["id"]]:
        return ""
    domNode = GD[n["id"]]
    return domNode.nodeValue
$$ language plpython3u package;
create function gms_xmldom.getNodeValueAsClob(n gms_xmldom.domnode)
returns clob
as $$
    from xml.dom import minidom
    if n["id"] not in GD or not GD[n["id"]]:
        return ""
    domNode = GD[n["id"]]
    return domNode.nodeValue
$$ language plpython3u package;

--gms_xmldom.getChildrenByTagName
create function gms_xmldom.getChildrenByTagName(elem gms_xmldom.DOMElement, name varchar2)
returns gms_xmldom.DOMNodeList
as $$
    from xml.dom import minidom
    from xml.dom.minicompat import NodeList
    from xml.dom import Node
    if elem["id"] not in GD or not GD[elem["id"]]:
        return [""]
    elemNode = GD[elem["id"]]
    nodeList = NodeList()
    for node in elemNode.childNodes:
        if node.nodeType == Node.ELEMENT_NODE:
            if name == "*" or name == node.tagName:
                nodeList.append(node)
    resid = str(id(nodeList))
    if resid not in GD:
        GD[resid] = nodeList
    return [resid]
$$ language plpython3u package;
create function gms_xmldom.getChildrenByTagName(elem gms_xmldom.DOMElement, name varchar2, ns varchar2)
returns gms_xmldom.DOMNodeList
as $$
    from xml.dom import minidom
    from xml.dom.minicompat import NodeList
    from xml.dom import Node
    if elem["id"] not in GD or not GD[elem["id"]]:
        return [""]
    elemNode = GD[elem["id"]]
    nodeList = NodeList()
    for node in elemNode.childNodes:
        if node.nodeType == Node.ELEMENT_NODE:
            if(name == "*" or name == node.tagName) and (ns == "*" or node.namespaceURI == ns):
                nodeList.append(node)
    resid = str(id(nodeList))
    if resid not in GD:
        GD[resid] = nodeList
    return [resid]
$$ language plpython3u package;

--gms_xmldom.getOwnerDocument
create function gms_xmldom.getOwnerDocument(n gms_xmldom.DOMNode)
returns gms_xmldom.DOMDocument
as $$
    from xml.dom import minidom
    if n["id"] not in GD or not GD[n["id"]]:
        return [""]
    dom = GD[n["id"]]
    document = dom.ownerDocument
    resid = str(id(document))
    if resid not in GD:
        GD[resid] = document
    return [resid]
$$ language plpython3u package;
--gms_xmldom.newDOMDocument
create function gms_xmldom.newDOMDocument()
returns gms_xmldom.DOMDocument
as $$
    from xml.dom import minidom
    docNode = minidom.Document()
    resid = str(id(docNode))
    if resid not in GD:
        GD[resid] = docNode
    return [resid]
$$ language plpython3u package;
create function gms_xmldom.newDOMDocument(xmldoc IN xml)
returns gms_xmldom.DOMDocument
as $$
    from xml.dom import minidom
    import os
    import io
    #Remove extra Spaces and newlines from the string
    strxml = ''
    buffer = io.StringIO(xmldoc)
    lines = buffer.readlines()
    for line in lines:
        tmpLine = line.replace('\n', '')
        tmpLine = tmpLine.strip()
        if tmpLine == '':
            continue
        strxml += tmpLine
    docNode = minidom.parseString(strxml)
    resid = str(id(docNode))
    if resid not in GD:
        GD[resid] = docNode
    return [resid]
$$ language plpython3u package;
create function gms_xmldom.newDOMDocument(xmldoc clob)
returns gms_xmldom.DOMDocument
as $$
    from xml.dom import minidom
    import io
    #Remove extra Spaces and newlines from the string
    strxml = ''
    buffer = io.StringIO(xmldoc)
    lines = buffer.readlines()
    for line in lines:
        tmpLine = line.replace('\n', '')
        tmpLine = tmpLine.strip()
        if tmpLine == '':
            continue
        strxml += tmpLine
    docNode = minidom.parseString(strxml)
    resid = str(id(docNode))
    if resid not in GD:
        GD[resid] = docNode
    return [resid]
$$ language plpython3u package;
create function gms_xmldom.newDOMDocument(filename text)
returns gms_xmldom.DOMDocument
as $$
    from xml.dom import minidom
    #Remove extra Spaces and newlines from the file
    strxml = ''
    with open(filename, 'r') as fp:
        lines = fp.readlines()
        for line in lines:
            tmpLine = line.replace('\n', '')
            tmpLine = tmpLine.strip()
            if tmpLine == '':
                continue
            strxml += tmpLine
    docNode = minidom.parseString(strxml)
    resid = str(id(docNode))
    if resid not in GD:
        GD[resid] = docNode
    return [resid]
$$ language plpython3u package;

--gms_xmldom.hasChildNodes
create function gms_xmldom.hasChildNodes(n gms_xmldom.DOMNode)
returns boolean
as $$
    from xml.dom import minidom
    if n["id"] not in GD or not GD[n["id"]]:
        return False
    domNode = GD[n["id"]]
    return domNode.hasChildNodes()
$$ language plpython3u package;

--gms_xmldom.insertBefore
create function gms_xmldom.insertBefore(
    n gms_xmldom.DOMNode, 
    newchild IN gms_xmldom.DOMNode, 
    refchild IN gms_xmldom.DOMNode)
returns gms_xmldom.DOMNode
as $$
    from xml.dom import minidom
    if n["id"] not in GD or not GD[n["id"]]:
        raise plpy.Error("DOMNode is empty or invalid")
    domNode = GD[n["id"]]
    if newchild["id"] not in GD or not GD[newchild["id"]]:
        raise plpy.Error("newChild is empty or invalid")
    newDom = GD[newchild["id"]]
    if refchild["id"] not in GD or not GD[refchild["id"]]:
        raise plpy.Error("refChild is empty or invalid")
    refDom = GD[refchild["id"]]
    res = domNode.childNodes.index(refDom)
    newDom = domNode.insertBefore(newDom, refDom)
    resid = str(id(newDom))
    if resid not in GD:
        GD[resid] = newDom
    return [resid]
$$ language plpython3u package;

create function gms_xmldom.setVersion(doc gms_xmldom.DOMDocument, version VARCHAR2)
returns void
as $$
    from xml.dom import minidom
    if doc["id"] not in GD or not GD[doc["id"]]:
        raise plpy.Error("DOMDocument is empty or invalid")
    docNode = GD[doc["id"]]
    docNode.version = version
$$ language plpython3u package;

create function gms_xmldom.ELEMENT_NODE() returns INTEGER
as $$
    from xml.dom import Node
    return Node.ELEMENT_NODE
$$language plpython3u;
create function gms_xmldom.ATTRIBUTE_NODE() returns INTEGER
as $$
    from xml.dom import Node
    return Node.ATTRIBUTE_NODE
$$language plpython3u;
create function gms_xmldom.TEXT_NODE() returns INTEGER
as $$
    from xml.dom import Node
    return Node.TEXT_NODE
$$language plpython3u;
create function gms_xmldom.CDATA_SECTION_NODE() returns INTEGER
as $$
    from xml.dom import Node
    return Node.CDATA_SECTION_NODE
$$language plpython3u;
create function gms_xmldom.ENTITY_REFERENCE_NODE() returns INTEGER
as $$
    from xml.dom import Node
    return Node.ENTITY_REFERENCE_NODE
$$language plpython3u;
create function gms_xmldom.ENTITY_NODE() returns INTEGER
as $$
    from xml.dom import Node
    return Node.ENTITY_NODE
$$language plpython3u;
create function gms_xmldom.PROCESSING_INSTRUCTION_NODE() returns INTEGER
as $$
    from xml.dom import Node
    return Node.PROCESSING_INSTRUCTION_NODE
$$language plpython3u;
create function gms_xmldom.COMMENT_NODE() returns INTEGER
as $$
    from xml.dom import Node
    return Node.COMMENT_NODE
$$language plpython3u;
create function gms_xmldom.DOCUMENT_NODE() returns INTEGER
as $$
    from xml.dom import Node
    return Node.DOCUMENT_NODE
$$language plpython3u;
create function gms_xmldom.DOCUMENT_TYPE_NODE() returns INTEGER
as $$
    from xml.dom import Node
    return Node.DOCUMENT_TYPE_NODE
$$language plpython3u;
create function gms_xmldom.DOCUMENT_FRAGMENT_NODE() returns INTEGER
as $$
    from xml.dom import Node
    return Node.DOCUMENT_FRAGMENT_NODE
$$language plpython3u;
create function gms_xmldom.NOTATION_NODE() returns INTEGER
as $$
    from xml.dom import Node
    return Node.NOTATION_NODE
$$language plpython3u;

create function gms_xmldom.makeCharacterData(n gms_xmldom.DOMNode)
returns gms_xmldom.DOMCharacterData
as $$
    if n["id"] not in GD or not GD[n["id"]]:
        return ""
    return [n["id"]]
$$ language plpython3u package;

--gms_xmlparser
create schema gms_xmlparser;
GRANT USAGE ON SCHEMA gms_xmlparser TO public;
create type gms_xmlparser.Parser as (id text);

create function gms_xmlparser.newParser() returns gms_xmlparser.Parser
as $$
    from xml.dom.expatbuilder import ExpatBuilder
    builder = ExpatBuilder()
    resid = str(id(builder))
    if resid not in GD:
        GD[resid] = builder
    return [resid]
$$language plpython3u package;

create  function gms_xmlparser.getDocument(p gms_xmlparser.Parser)
returns gms_xmldom.DOMDocument
as $$
    from xml.dom.expatbuilder import ExpatBuilder
    if p["id"] not in GD or not GD[p["id"]]:
        return [""]
    builder = GD[p["id"]]
    docNode = builder.document
    resid = str(id(docNode))
    if resid not in GD:
        GD[resid] = docNode
    return [resid]
$$language plpython3u package;

create function gms_xmlparser.parseClob(p gms_xmlparser.Parser, doc CLOB)
returns void
as $$
    from xml.dom.expatbuilder import ExpatBuilder
    import io
    if p["id"] not in GD or not GD[p["id"]]:
        return
    builder = GD[p["id"]]
    #Remove extra Spaces and newlines from the string
    strxml = ''
    buffer = io.StringIO(doc)
    lines = buffer.readlines()
    for line in lines:
        tmpLine = line.replace('\n', '')
        tmpLine = tmpLine.strip()
        if tmpLine == '':
            continue
        strxml += tmpLine
    docNode = builder.parseString(strxml)
    builder.document = docNode
$$language plpython3u package;

create function gms_xmlparser.parseBuffer(p gms_xmlparser.Parser,doc VARCHAR2)
returns void
as $$
    from xml.dom.expatbuilder import ExpatBuilder
    import io
    if p["id"] not in GD or not GD[p["id"]]:
        return
    builder = GD[p["id"]]
    #Remove extra Spaces and newlines from the string
    strxml = ''
    buffer = io.StringIO(doc)
    lines = buffer.readlines()
    for line in lines:
        tmpLine = line.replace('\n', '')
        tmpLine = tmpLine.strip()
        if tmpLine == '':
            continue
        strxml += tmpLine
    docNode = builder.parseString(strxml)
    builder.document = docNode
$$language plpython3u package;

create or replace function gms_xmlparser.getDirPathByDirectory(dir VARCHAR2)
returns VARCHAR2
as $$
declare
    canread boolean;
    dirpath varchar2;
begin
    canread := has_directory_privilege(current_user, dir, 'READ');
    if canread = true then
        select p.dirpath into dirpath from pg_directory p where p.dirname = dir;
        if dirpath = NULL then
            raise exception 'directory "%" does not exist', dir;
        end if;
    else
        raise exception 'You do not have privileges for the directory';
    end if;
    return dirpath;
end;
$$language plpgsql;

create or replace function gms_xmlparser.setbasedir(p gms_xmlparser.Parser, dir VARCHAR2)
returns void
as $$
    if p is None or p["id"] is None or dir is None:
        return
    keystr = p["id"] + 'dir'
    GD[keystr] = dir
$$language plpython3u package;

create or replace function gms_xmlparser.splitDirAndFilename(
    p gms_xmlparser.Parser, 
    filepath VARCHAR2, 
    dir out VARCHAR2, 
    xmlfile out VARCHAR2)
returns setof record
as $$
    import os
    head_tail = os.path.split(filepath)
    if head_tail[0] != '':
        dir = head_tail[0]
        xmlfile = head_tail[1]
    else:
        if p != None:
            keystr = p["id"] + 'dir'
            if keystr not in GD:
                raise plpy.error('Invalid resource handle or path name "%s"' %head_tail[1])
            dir = GD[keystr]
            xmlfile = head_tail[1]
        else:
            raise plpy.error('Invalid resource handle or path name "%s"' %head_tail[1])
    return [(dir, xmlfile)]
$$language plpython3u package;

create or replace function gms_xmlparser.parseFileWithParser(p gms_xmlparser.Parser, file VARCHAR2)
returns void
as $$
    from xml.dom.expatbuilder import ExpatBuilder
    if p["id"] not in GD or not GD[p["id"]]:
        return
    builder = GD[p["id"]]
    #Remove extra Spaces and newlines from the file
    strxml = ''
    with open(file, 'r') as fp:
        lines = fp.readlines()
        for line in lines:
            tmpLine = line.replace('\n', '')
            tmpLine = tmpLine.strip()
            if tmpLine == '':
                continue
            strxml += tmpLine
    docNode = builder.parseString(strxml)
    builder.document = docNode
$$language plpython3u package;

create or replace function gms_xmlparser.parse(p gms_xmlparser.Parser, file VARCHAR2)
returns void
as $$
declare
    dirname VARCHAR2;
    filename VARCHAR2;
    filepath VARCHAR2;
begin
    if p is null or file is null then
        raise exception 'input arguments cannot be null';
    end if;
    select dir, xmlfile into dirname, filename from gms_xmlparser.splitDirAndFilename(p, file);
    filepath := gms_xmlparser.getDirPathByDirectory(dirname);
    gms_xmlparser.parseFileWithParser(p, filepath || '/' || filename);
end;
$$language plpgsql package;

create or replace function gms_xmlparser.parseFile(file VARCHAR2)
returns gms_xmldom.DOMDocument
as $$
    from xml.dom import expatbuilder
    #Remove extra Spaces and newlines from the file
    strxml = ''
    with open(file, 'r') as fp:
        lines = fp.readlines()
        for line in lines:
            tmpLine = line.replace('\n', '')
            tmpLine = tmpLine.strip()
            if tmpLine == '':
                continue
            strxml += tmpLine
    docNode = expatbuilder.parseString(strxml)
    resid = str(id(docNode))
    if resid not in GD:
        GD[resid] = docNode
    return [resid]
$$language plpython3u package;

create or replace function gms_xmlparser.parse(file VARCHAR2)
returns gms_xmldom.DOMDocument
as $$
declare
    dirname VARCHAR2;
    filename VARCHAR2;
    filepath VARCHAR2;
    doc gms_xmldom.DOMDocument;
begin
    if file is null then
        raise exception 'input arguments cannot be null';
    end if;
    select dir, xmlfile into dirname, filename from gms_xmlparser.splitDirAndFilename(null, file);
    filepath := gms_xmlparser.getDirPathByDirectory(dirname);
    doc := gms_xmlparser.parseFile(filepath || '/' || filename);
    return doc;
end;
$$language plpgsql package;

create or replace function gms_xmlparser.freeparser(p gms_xmlparser.Parser)
returns void
as $$
    from xml.dom.expatbuilder import ExpatBuilder
    if p is None or p["id"] is None or p["id"] not in GD:
        return
    builder = GD[p["id"]]
    del builder
    GD.pop(p["id"])
$$language plpython3u package;

create schema gms_xslprocessor;
GRANT USAGE ON SCHEMA gms_xslprocessor TO public;

create or replace function gms_xslprocessor.valueofbuffer(
    xmldata VARCHAR2,
    pattern VARCHAR2,
    namespace VARCHAR2)
returns VARCHAR2
as $$
    import xml.etree.ElementTree as ET
    global pattern
    initpattern = pattern
    root = ET.fromstring(xmldata)
    #remove'/text()'
    pattern = pattern.replace('/text()', '')
    if pattern[len(pattern) - 1] == '/':
        plpy.error("XML parse failed. Invalid token in: '%s'"%initpattern)
    if pattern[0] == '/':
        return ''
    value = root.findtext(pattern, namespace)
    root.clear()
    del root
    return value
$$language plpython3u package;

create or replace function gms_xslprocessor.valueof(
    n gms_XMLDOM.DOMNODE,
    pattern VARCHAR2,
    val OUT VARCHAR2,
    namespace VARCHAR2 default NULL)
as $$
declare
    xmlbuf VARCHAR2;
    compatibility char(1);
begin
    select datcompatibility into compatibility from pg_database where datname=current_database();
    if compatibility != 'A' THEN
        raise exception 'gms_xslprocessor package only supported in database which dbcompatibility=''A''.';
    end if;
    if n is null then
        return '';
    end if;
    if pattern is null then
        raise exception 'XPath compilation failed: Xpath is null';
    end if;
    gms_xmldom.writeToBuffer(n, xmlbuf);
    val := gms_xslprocessor.valueofbuffer(xmlbuf, pattern, namespace);
end;
$$language plpgsql package;

create or replace function gms_xslprocessor.selectnodesbuffer(
    xmldata VARCHAR2,
    xpath_expr VARCHAR2,
    namespace VARCHAR2 default NULL)
returns gms_xmldom.DOMNodeList
as $$
    import xml.dom.minidom as dom
    from xml.dom.minicompat import NodeList
    doc = dom.parseString(xmldata)
    root = doc.documentElement
    if namespace:
        nodes = root.getElementsByTagNameNS(namespace, xpath_expr)
    else:
        nodes = root.getElementsByTagName(xpath_expr)

    nodelist = NodeList()
    for node in nodes:
        nodelist.append(node)

    resid = str(id(nodelist))
    if resid not in GD:
        GD[resid] = nodelist
    return [resid]
$$language plpython3u package;

create or replace function gms_xslprocessor.selectnodes(
    n gms_XMLDOM.DOMNODE,
    pattern     VARCHAR2,
    namespace   VARCHAR2 default NULL)
return gms_xmldom.DOMNodeList
as
declare
    xmlbuf VARCHAR2;
    res gms_xmldom.DOMNodeList;
    compatibility       char(1);
begin
    select datcompatibility into compatibility from pg_database where datname=current_database();
    if compatibility != 'A' THEN
        raise exception 'gms_xslprocessor package only supported in database which dbcompatibility=''A''.';
    end if;
    if n is null then
        return NULL;
    end if;
    if pattern is null then
        raise exception 'XPath compilation failed: Xpath is null';
    end if;
    gms_xmldom.writeToBuffer(n, xmlbuf);
    res := gms_xslprocessor.selectnodesbuffer(xmlbuf,pattern,namespace);
    return res;
end;
;

set search_path to public;
create function generate_oracle_package_sql()
returns text
as $c_sql$
c_sql = '''
/* "
 * ----------------------------------- utl_encode ---------------------------------------
 */
CREATE SCHEMA utl_encode;
GRANT USAGE ON SCHEMA utl_encode TO public;

/* base64_encode internal fun*/
CREATE OR REPLACE FUNCTION utl_encode.__BASE64_ENCODE(
    b   IN BYTEA
) RETURNS BYTEA AS $$

if 'base64' in SD:
    base64 = SD['base64']
else:
    import base64
    SD['base64'] = base64

res = base64.b64encode(b)

return res
END;
$$ LANGUAGE plpython3u;

CREATE OR REPLACE FUNCTION utl_encode.BASE64_ENCODE(
    r   IN RAW
) RETURNS RAW AS $$
DECLARE
    b BYTEA;
    res RAW;
BEGIN
    --check parameter
    if r is NULL then
        raise exception USING ERRCODE = 'P0019', message = 'bad argument';
    end if;
    
    b := utl_encode.__BASE64_ENCODE(rawtobytea(r));
    res := byteatoraw(b);
    return res;
END;
$$ LANGUAGE PLPGSQL;

/* base64_decode internal fun*/
CREATE OR REPLACE FUNCTION utl_encode.__BASE64_DECODE(
    b   IN BYTEA
) RETURNS BYTEA AS $$

if 'base64' in SD:
    base64 = SD['base64']
else:
    import base64
    SD['base64'] = base64

try:
    #-- b'===' is used for "binascii.Error: Incorrect padding"
    res = base64.b64decode(b + b'===')
except:
    #-- for "binascii.Error: Invalid base64-encoded string"
    buf = b.rstrip(b'=')[:-1]
    res = base64.b64decode(buf + b'===')

return res
END;
$$ LANGUAGE plpython3u;

CREATE OR REPLACE FUNCTION utl_encode.BASE64_DECODE(
    r   IN RAW
) RETURNS RAW AS $$
DECLARE
    b BYTEA;
    res RAW;
BEGIN
    --check parameter
    if r is NULL then
        raise exception USING ERRCODE = 'P0019', message = 'bad argument';
    end if;
    
    b := utl_encode.__BASE64_DECODE(rawtobytea(r));
    res := byteatoraw(b);
    return res;
END;
$$ LANGUAGE PLPGSQL;

/* 
 * mimeheader_encode internal fun
 * return '=?<charset>?<encoding>?<encoded text>?='
 */
CREATE OR REPLACE FUNCTION utl_encode.__MIMEHEADER_ENCODE(
    buf             IN     VARCHAR2,
    encode_charset  IN     VARCHAR2,
    encoding        IN     INTEGER,
    res_buf         OUT    VARCHAR2,
    errcode         OUT    VARCHAR2,
    errmsg          OUT    VARCHAR2
) RETURNS SETOF RECORD AS $$
#--import
if 'base64' in SD:
    base64 = SD['base64']
else:
    import base64
    SD['base64'] = base64

if 'quopri' in SD:
    quopri = SD['quopri']
else:
    import quopri
    SD['quopri'] = quopri

#--check parameter
if encoding not in [1,2]:
    return [(None, 'P0019', 'bad argument')]
#--not support character set
try:
    'a'.encode(encode_charset)
except LookupError:
    return [(None, 'P0020', 'invalid Character set')]
if ('utf-16' == encode_charset or 'utf16' == encode_charset or 'UTF-16' == encode_charset 
    or 'UTF16' == encode_charset or 'utf-32' == encode_charset or 'utf32' == encode_charset
    or 'UTF-32' == encode_charset or 'UTF32' == encode_charset):
    return [(None, 'P0020', 'invalid Character set')]

#--base64
if 1 == encoding:
    encoded_text = base64.b64encode(buf.encode(encode_charset));
    encoding_str = 'B'
#--quoted_printable
else:
    encoded_text = quopri.encodestring(buf.encode(encode_charset));
    encoding_str = 'Q'
#--Concatenate the resulting string
res = '=?' + str(encode_charset) + '?' + encoding_str + '?' + str(encoded_text)[2:-1] + '?='
return [(res, '0', '0')]
$$ LANGUAGE plpython3u;

CREATE OR REPLACE FUNCTION utl_encode.MIMEHEADER_ENCODE(
    buf             IN VARCHAR2,
    encode_charset  IN VARCHAR2 DEFAULT NULL,
    encoding        IN INTEGER DEFAULT NULL
) RETURNS VARCHAR2 AS $$
DECLARE
    type read_type is record(
        res         VARCHAR2,
        errcode     VARCHAR2,
        errmsg      VARCHAR2
    );
    read_type_res read_type;
BEGIN
    --check parameter
    if buf is NULL then
        raise exception USING ERRCODE = 'P0019', message = 'bad argument';
    end if;
    --get db charset
    if encode_charset is NULL then
        show server_encoding into encode_charset;
    end if;
    -- null is quoted_printable
    if encoding is NULL then
        encoding = 2;
    end if;
    read_type_res := utl_encode.__MIMEHEADER_ENCODE(buf, encode_charset, encoding);
    if read_type_res.errcode != '0' then
        raise exception USING ERRCODE = read_type_res.errcode, message = read_type_res.errmsg;
    end if;
    return read_type_res.res;
END;
$$ LANGUAGE PLPGSQL;


/* quoted_printable_decode internal fun */
CREATE OR REPLACE FUNCTION utl_encode.__QUOTED_PRINTABLE_DECODE(
    b   IN BYTEA
) RETURNS BYTEA AS $$

if 'quopri' in SD:
    quopri = SD['quopri']
else:
    import quopri
    SD['quopri'] = quopri

res = quopri.decodestring(b)
return res
END;
$$ LANGUAGE plpython3u;

CREATE OR REPLACE FUNCTION utl_encode.QUOTED_PRINTABLE_DECODE(
    r   IN RAW
) RETURNS RAW AS $$
DECLARE
    b BYTEA;
    res RAW;
BEGIN
    --check parameter
    if r is NULL then
        raise exception USING ERRCODE = 'P0019', message = 'bad argument';
    end if;
    b := utl_encode.__QUOTED_PRINTABLE_DECODE(rawtobytea(r));
    res := byteatoraw(b);
    return res;
END;
$$ LANGUAGE PLPGSQL;


/* text_decode internal fun*/
CREATE OR REPLACE FUNCTION utl_encode.__TEXT_DECODE(
    buf             IN     VARCHAR2,
    encode_charset  IN     VARCHAR2,
    encoding        IN     INTEGER,
    res_buf         OUT    VARCHAR2,
    errcode         OUT    VARCHAR2,
    errmsg          OUT    VARCHAR2
) RETURNS SETOF RECORD AS $$

if 'base64' in SD:
    base64 = SD['base64']
else:
    import base64
    SD['base64'] = base64

if 'quopri' in SD:
    quopri = SD['quopri']
else:
    import quopri
    SD['quopri'] = quopri

#--check parameter
if encoding not in [1,2]:
    return [(None, 'P0019', 'bad argument')]
#--not support character set
try:
    'a'.encode(encode_charset)
except LookupError:
    return [(None, 'P0020', 'invalid Character set')]
if ('utf-16' == encode_charset or 'utf16' == encode_charset or 'UTF-16' == encode_charset 
    or 'UTF16' == encode_charset or 'utf-32' == encode_charset or 'utf32' == encode_charset
    or 'UTF-32' == encode_charset or 'UTF32' == encode_charset):
    return [(None, 'P0020', 'invalid Character set')]

#--base64
if 1 == encoding:
    b_buf = buf.encode('utf8')
    try:
        #-- b'===' is used for "binascii.Error: Incorrect padding"
        res = base64.b64decode(b_buf + b'===')
    except:
        #-- for "binascii.Error: Invalid base64-encoded string"
        b = b_buf.rstrip(b'=')[:-1]
        res = base64.b64decode(b + b'===')
    res = res.decode(encode_charset, errors = 'replace');
#--quoted_printable
else:
    #--str to bytes
    b_buf = buf.encode('utf8')
    res = quopri.decodestring(b_buf).decode(encode_charset, errors = 'replace');
return [(res, '0', '0')]
$$ LANGUAGE plpython3u;

CREATE OR REPLACE FUNCTION utl_encode.TEXT_DECODE(
    buf             IN VARCHAR2,
    encode_charset  IN VARCHAR2 DEFAULT NULL,
    encoding        IN INTEGER DEFAULT NULL
) RETURNS VARCHAR2 AS $$
DECLARE
    type read_type is record(
        res         VARCHAR2,
        errcode     VARCHAR2,
        errmsg      VARCHAR2
    );
    read_type_res read_type;
BEGIN
    --check parameter
    if buf is NULL then
        raise exception USING ERRCODE = 'P0019', message = 'bad argument';
    end if;
    --get db charset
    if encode_charset is NULL then
        show server_encoding into encode_charset;
    end if;
    -- null is quoted_printable
    if encoding is NULL then
        encoding = 2;
    end if;
    read_type_res := utl_encode.__TEXT_DECODE(buf, encode_charset, encoding);
    if read_type_res.errcode != '0' then
        raise exception USING ERRCODE = read_type_res.errcode, message = read_type_res.errmsg;
    end if;
    return read_type_res.res;
END;
$$ LANGUAGE PLPGSQL;

/* uudecode internal fun*/
CREATE OR REPLACE FUNCTION utl_encode.__UUDECODE(
    r       IN  BYTEA,
    b_res   OUT BYTEA,
    errcode OUT VARCHAR2,
    errmsg  OUT VARCHAR2
) RETURNS SETOF RECORD AS $$

if 'binascii' in SD:
    binascii = SD['binascii']
else:
    import binascii
    SD['binascii'] = binascii

if 'BytesIO' in SD:
    BytesIO = SD['BytesIO']
else:
    from io import BytesIO
    SD['BytesIO'] = BytesIO

buf = BytesIO(r)
res = b""
while 1:
    s = buf.readline()
    if not s or s == b'end\\n' or s == b'end':
        break
    if b'begin' in s:
        continue
    #--skip the blank line
    if b'\\n' == s:
        continue
    try:
        data = binascii.a2b_uu(s)
    except binascii.Error:
        #--Workaround for broken uuencoders by /Fredrik Lundh
        nbytes = (((s[0]-32) & 63) * 4 + 5) // 3
        try:
            data = binascii.a2b_uu(s[:nbytes])
        except:
            if res.__len__() == 0:
                return [(None, 'P0021', 'invalid encoded string')]
            else:
                break
    res = res + data
#--need to clear the terminator
res = res.replace(b'\\x00', b'')
return [(res, '0', '0')]
END;
$$ LANGUAGE plpython3u;

CREATE OR REPLACE FUNCTION utl_encode.UUDECODE(
    r   IN RAW
) RETURNS RAW AS $$
DECLARE
    type read_type is record(
        res         BYTEA,
        errcode     VARCHAR2,
        errmsg      VARCHAR2
    );
    read_type_res read_type;
    res RAW;
BEGIN
    --check parameter
    if r is NULL then
        raise exception USING ERRCODE = 'P0019', message = 'bad argument';
    end if;
    read_type_res := utl_encode.__UUDECODE(rawtobytea(r));
    if read_type_res.errcode != '0' then
        raise exception USING ERRCODE = read_type_res.errcode, message = read_type_res.errmsg;
    end if;
    res := byteatoraw(read_type_res.res);
    return res;
END;
$$ LANGUAGE PLPGSQL;

/* 
 * ----------------------------------- utl_tcp ------------------------------------------
 * --create package needs to be public
 */
set search_path to public;
CREATE OR REPLACE PACKAGE UTL_TCP
AS
    TYPE connection IS RECORD(
    remote_host    VARCHAR2(255),
    remote_port    INTEGER,
    local_host     VARCHAR2(255),
    local_port     INTEGER,
    charset        VARCHAR2(30),
    newline        VARCHAR2(2),
    tx_timeout     INTEGER,
    private_sd     INTEGER 
    );

	CRLF CONSTANT VARCHAR2(2) := E'\\r\\n';

    FUNCTION OPEN_CONNECTION  (remote_host      IN VARCHAR2,
        remote_port      IN INTEGER,
        local_host       IN VARCHAR2 DEFAULT NULL,
        local_port       IN INTEGER DEFAULT NULL,
        in_buffer_size   IN INTEGER DEFAULT NULL,
        out_buffer_size  IN INTEGER DEFAULT NULL,
        charset          IN VARCHAR2 DEFAULT NULL,
        newline          IN VARCHAR2 DEFAULT E'\\r\\n',
        tx_timeout       IN INTEGER DEFAULT NULL) RETURN connection;
    FUNCTION CLOSE_CONNECTION (c IN OUT connection) RETURN connection;
    FUNCTION CLOSE_ALL_CONNECTIONS RETURN VOID;
    FUNCTION AVAILABLE (
        c        IN connection, 
        timeout  IN INTEGER DEFAULT 0) RETURN INTEGER;
    --raw
    FUNCTION READ_RAW (c     IN OUT  connection,
        data      IN OUT        RAW,
        data_len  OUT           INTEGER,
        len       IN            INTEGER DEFAULT 1,
        peek      IN            BOOLEAN     DEFAULT FALSE
        ) RETURN SETOF RECORD;
    FUNCTION WRITE_RAW (c    IN  connection,
        data IN            RAW,
        len  IN            INTEGER DEFAULT NULL) RETURN INTEGER;
    FUNCTION GET_RAW (c     IN  connection,
        len   IN            INTEGER DEFAULT 1,
        peek  IN            BOOLEAN     DEFAULT FALSE) RETURN RAW;
    --text
    FUNCTION READ_TEXT (c     IN OUT  connection,
        data      IN OUT        VARCHAR2,
        data_len  OUT           INTEGER,
        len       IN            INTEGER DEFAULT 1,
        peek      IN            BOOLEAN     DEFAULT FALSE
        ) RETURN SETOF RECORD;
    FUNCTION WRITE_TEXT (c    IN  connection,
        data IN            VARCHAR2,
        len  IN            INTEGER DEFAULT NULL) RETURN INTEGER;
    FUNCTION GET_TEXT (c     IN connection,
        len   IN            INTEGER DEFAULT 1,
        peek  IN            BOOLEAN     DEFAULT FALSE) RETURN VARCHAR2;
    --line
    FUNCTION READ_LINE (c     IN OUT  connection,
        data          IN OUT        VARCHAR2,
        data_len      OUT           INTEGER,
        remove_crlf   IN            BOOLEAN   DEFAULT FALSE,
        peek          IN            BOOLEAN     DEFAULT FALSE
        ) RETURN SETOF RECORD;
    FUNCTION WRITE_LINE (c    IN  connection,
        data IN            VARCHAR2 DEFAULT NULL) RETURN INTEGER;
    FUNCTION GET_LINE (c     IN connection,
        remove_crlf IN         BOOLEAN DEFAULT FALSE,
        peek        IN         BOOLEAN     DEFAULT FALSE) RETURN VARCHAR2;
    FUNCTION FLUSH (c IN OUT connection) RETURN connection;
END UTL_TCP;
;
----------------------------------------
--utl_tcp internal function
----------------------------------------
create schema utl_tcp_internal;
GRANT USAGE ON SCHEMA utl_tcp_internal TO public;
set search_path to utl_tcp_internal;
--open_connection
--  io.BufferedReader:
--     When there is data in the buffer, the length is n. 
--     Call peek to read data longer than n, the buffer will not be refreshed, 
--     only n data can be read
--  _pyio.BufferedReader:
--     Only flush buffers blocking
--  And when buffer_size=0, the SocketIO cannot use peek
--  So implement a BufferReader
CREATE OR REPLACE FUNCTION UTL_TCP__OPEN_CONNECTION  (remote_host      IN VARCHAR2,
    remote_port      IN INTEGER,
    local_host       IN VARCHAR2 DEFAULT NULL,
    local_port       IN INTEGER DEFAULT NULL,
    in_buffer_size   IN INTEGER DEFAULT NULL,
    out_buffer_size  IN INTEGER DEFAULT NULL,
    charset          IN VARCHAR2 DEFAULT NULL,
    newline          IN VARCHAR2 DEFAULT E'\\r\\n',
    tx_timeout       IN INTEGER DEFAULT NULL,
    c                OUT public.utl_tcp.connection,
    errcode          OUT VARCHAR2,
    errmsg           OUT VARCHAR2)
RETURNS SETOF RECORD
AS $$
#--"check parameter"
global in_buffer_size
global out_buffer_size
global newline
global charset

#--"user don't care about buffer"
if None == in_buffer_size:
    in_buffer_size = 4096
if None == out_buffer_size:
    out_buffer_size = 4096

#--"set default charset"
if None == charset:
    charset = 'utf8'
#--"Python does not support the Oracle character set US7ASCII"
if 'US7ASCII' == charset:
    charset = 'GBK'

if (remote_host == None or remote_port == None or in_buffer_size < 0 
        or in_buffer_size > 32767 or out_buffer_size <  0
        or out_buffer_size > 32767):
    return [(None, 'P0015', 'bad argument')]
if tx_timeout != None and tx_timeout < 0:
    return [(None, 'P0015', 'bad argument')]

newline = '\\r\\n'

#--"Actully, oracle's minimun buffer size is 5"
if in_buffer_size > 0 and in_buffer_size < 5:
    in_buffer_size = 5
if out_buffer_size > 0 and out_buffer_size < 5:
    out_buffer_size = 5

#--"not support character set, mainly because of the bytes of newline"
try:
    'a'.encode(charset)
except LookupError:
    return [(None, 'P0015', 'unsupported character set')]
if ('utf-16' == charset or 'utf16' == charset or 'UTF-16' == charset 
    or 'UTF16' == charset or 'utf-32' == charset or 'utf32' == charset
    or 'UTF-32' == charset or 'UTF32' == charset):
    return [(None, 'P0015', 'unsupported character set')]

#--"the maximum number of connections is 15"
if 'utl_tcp_connections' in GD:
    if GD['utl_tcp_connections'] >= 15:
        return [(None, 'P0017', 'too many open connections')]
    else:
        GD['utl_tcp_connections'] = GD['utl_tcp_connections'] + 1
else:
    GD['utl_tcp_connections'] = 1


if 'socket' in SD:
    socket = SD['socket']
else:
    import socket
    SD['socket'] = socket

#--"to save every connection's socket"
if 'utl_tcp_con' not in GD:
    GD['utl_tcp_con'] = {}

#--"to save buffer"
if 'utl_tcp_inbuffer' not in GD:
    GD['utl_tcp_inbuffer'] = {}

if 'utl_tcp_outbuffer' not in GD:
    GD['utl_tcp_outbuffer'] = {}

#--"every connection's id"
if 'utl_tcp_sd_num' in GD:
    private_sd = GD['utl_tcp_sd_num'] + 1
    GD['utl_tcp_sd_num'] = private_sd
else:
    GD['utl_tcp_sd_num'] = 0
    private_sd = 0

socket_fd = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)

#--"bind loacl host and port"
if local_host != None and local_port != None:
    socket_fd.bind((local_host, local_port))

socket_fd.settimeout(tx_timeout)
try:
    socket_fd.connect((remote_host, remote_port))
except socket.gaierror:
    return [(None, 'P0014', 'network error: Connect failed because target host or object does not exist')]
except socket.error:
    return [(None, 'P0014', 'network error: TNS:no listener')]

#--"set buffer, 0 mean not used"
outbuffer = socket_fd.makefile('wb', buffering = out_buffer_size)

GD['utl_tcp_inbuffer'][private_sd] = {}
GD['utl_tcp_inbuffer'][private_sd]['buf'] = b""
GD['utl_tcp_inbuffer'][private_sd]['size'] = in_buffer_size
GD['utl_tcp_inbuffer'][private_sd]['pos'] = 0

GD['utl_tcp_outbuffer'][private_sd] = {}
GD['utl_tcp_outbuffer'][private_sd]['buf'] = outbuffer
GD['utl_tcp_outbuffer'][private_sd]['size'] = out_buffer_size

GD['utl_tcp_con'][private_sd] = socket_fd

return [([remote_host, remote_port, local_host, local_port, charset, newline, 
            tx_timeout, private_sd], '0', '0')]
$$ LANGUAGE plpython3u;

--close
CREATE OR REPLACE FUNCTION UTL_TCP__CLOSE_CONNECTION (c IN OUT public.utl_tcp.connection)
AS $$
if 'socket' in SD:
    socket = SD['socket']
else:
    import socket;
    SD['socket'] = socket

private_sd = c['private_sd']
#--"clear buffer"
if 'utl_tcp_inbuffer' in GD and private_sd in GD['utl_tcp_inbuffer']:
    del GD['utl_tcp_inbuffer'][private_sd]['buf']
    del GD['utl_tcp_inbuffer'][private_sd]['size']
    del GD['utl_tcp_inbuffer'][private_sd]['pos']
    del GD['utl_tcp_inbuffer'][private_sd]

if 'utl_tcp_outbuffer' in GD and private_sd in GD['utl_tcp_outbuffer']:
    GD['utl_tcp_outbuffer'][private_sd]['buf'].flush()
    GD['utl_tcp_outbuffer'][private_sd]['buf'].__del__()
    del GD['utl_tcp_outbuffer'][private_sd]['buf']
    del GD['utl_tcp_outbuffer'][private_sd]['size']
    del GD['utl_tcp_outbuffer'][private_sd]

#--"close socket"
if 'utl_tcp_con' in GD and private_sd in GD['utl_tcp_con']:
    socket_fd = GD['utl_tcp_con'][private_sd]
    socket_fd.close()
    del GD['utl_tcp_con'][private_sd]

#--"clear connection"
c['remote_host'] = None
c['remote_port'] = None
c['local_host'] = None
c['local_port'] = None
c['charset'] = None
c['newline'] = None
c['tx_timeout'] = None
c['private_sd'] = None

#--"reduce connection number"
if 'utl_tcp_connections' in GD:
    GD['utl_tcp_connections'] = GD['utl_tcp_connections'] - 1

$$ LANGUAGE plpython3u;


-- close all connection
CREATE OR REPLACE FUNCTION UTL_TCP__CLOSE_ALL_CONNECTIONS
RETURNS VOID
AS $$
if 'socket' in SD:
    socket = SD['socket']
else:
    import socket
    SD['socket'] = socket

#--"clear buffer"
if 'utl_tcp_inbuffer' in GD:
    for sd in GD['utl_tcp_inbuffer']:
        try:
            del GD['utl_tcp_inbuffer'][sd]['buf']
            del GD['utl_tcp_inbuffer'][sd]['size']
            del GD['utl_tcp_inbuffer'][sd]['pos']
        except:
            break
    del GD['utl_tcp_inbuffer']

if 'utl_tcp_outbuffer' in GD:
    for sd in GD['utl_tcp_outbuffer']:
        try:
            GD['utl_tcp_outbuffer'][sd]['buf'].flush()
            GD['utl_tcp_outbuffer'][sd]['buf'].__del__()
            del GD['utl_tcp_inbuffer'][sd]['buf']
            del GD['utl_tcp_inbuffer'][sd]['size']
        except:
            break
    del GD['utl_tcp_outbuffer']

#--"close socket"
if 'utl_tcp_con' in GD:
    for sd in GD['utl_tcp_con']:
        GD['utl_tcp_con'][sd].close()
    del GD['utl_tcp_con']

#--"clear the id of the connection"
if 'utl_tcp_sd_num' in GD:
    del GD['utl_tcp_sd_num']

#--"clear connection number"
if 'utl_tcp_connections' in GD:
    del GD['utl_tcp_connections']

return None
$$ LANGUAGE plpython3u;


--available
CREATE OR REPLACE FUNCTION UTL_TCP__AVAILABLE (
   c        IN public.utl_tcp.connection, 
   timeout  IN INTEGER DEFAULT 0)
RETURNS INTEGER
AS $$
if 'socket' in SD:
    socket = SD['socket']
else:
    import socket;
    SD['socket'] = socket

private_sd = c['private_sd']
tx_timeout = c['tx_timeout']
if ('utl_tcp_con' in GD and private_sd in GD['utl_tcp_con'] 
        and 'utl_tcp_inbuffer' in GD and private_sd in GD['utl_tcp_inbuffer']):
    socket_fd = GD['utl_tcp_con'][private_sd]
    inbuff = GD['utl_tcp_inbuffer'][private_sd]

    #--"not using buff"
    if inbuff['size'] == 0:
        socket_fd.settimeout(timeout)
        recv_buff = socket_fd.getsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF)
        try:
            data = socket_fd.recv(recv_buff, socket.MSG_PEEK)
        except:
            return 0
        finally:
            socket_fd.settimeout(tx_timeout)
        ava_len = data.__len__()
        if ava_len == 0:
            ava_len = 1
        return ava_len

    #--"Implement java-like input stream available"
    socket_fd.settimeout(timeout)
    if inbuff['buf'].__len__() > 0:
        socket_fd.settimeout(tx_timeout)
        return inbuff['buf'].__len__()
    hava = inbuff['buf'].__len__() - inbuff['pos']
    try:
        data = socket_fd.recv(inbuff['size'] - hava)
    except:
        return 0
    finally:
        socket_fd.settimeout(tx_timeout)
    inbuff['buf'] = inbuff['buf'][inbuff['pos']:] + data
    inbuff['pos'] = 0
    ava_len = inbuff['buf'].__len__()
    if ava_len == 0:
        ava_len = 1
    return ava_len
#--"network error"
else:
    return -1
$$ LANGUAGE plpython3u;

--read_raw
CREATE OR REPLACE FUNCTION UTL_TCP__READ_RAW (c     IN OUT  public.utl_tcp.connection,
                  data      IN OUT        BYTEA,
                  len       IN            INTEGER DEFAULT 1,
                  peek      IN            BOOLEAN     DEFAULT FALSE,
                  data_len  OUT           INTEGER,
                  errcode   OUT           VARCHAR2,
                  errmsg    OUT           VARCHAR2)
RETURNS SETOF RECORD
AS $$
#--"check parameter"
global len
if len < 0:
    return [(c, None, None, 'P0015', 'bad argument')]
if len == 0:
    return [(c, b'', 0, '0', '0')]

if 'socket' in SD:
    socket = SD['socket']
else:
    import socket;
    SD['socket'] = socket

private_sd = c['private_sd']
if ('utl_tcp_con' in GD and private_sd in GD['utl_tcp_con'] 
        and 'utl_tcp_inbuffer' in GD and private_sd in GD['utl_tcp_inbuffer']):
    socket_fd = GD['utl_tcp_con'][private_sd]
    inbuff = GD['utl_tcp_inbuffer'][private_sd]

    hava = inbuff['buf'].__len__() - inbuff['pos']
    want = min(inbuff['size'] - hava, len)
    if peek:
        #--"not using buff"
        if inbuff['size'] == 0:
            read_len = 0
            data = b""
            while read_len < len:
                try:
                    data = data + socket_fd.recv(len, socket.MSG_PEEK)
                except socket.timeout:
                    #--timeout, but has data
                    if data.__len__() != 0:
                        return [(c, data, data_len, '0', '0')]
                    else:
                        return [(c, None, None, 'P0016', 'transfer timeout')]
                #--"no more data, server close socket"
                if read_len == data.__len__():
                    break
                read_len = data.__len__()
            #--"no data read"
            if read_len == 0:
                return [(c, None, None, 'P0013', 'end-of-input reached')]
            return [(c, data, read_len, '0', '0')]
        
        #--using buff
        if len > inbuff['size']:
            return [(c, None, None, 'P0012', 'buffer too small')]
        if len > hava:
            data_b = b''
            try:
                data_b = socket_fd.recv(want)
            except socket.timeout:
                if hava == 0:
                    return [(c, None, None, 'P0016', 'transfer timeout')]
                else:
                    data_b = b""
            inbuff['buf'] = inbuff['buf'][inbuff['pos']:] + data_b
            inbuff['pos'] = 0
        data = inbuff['buf'][:len]

    else:
        #--"not using buff"
        if inbuff['size'] == 0:
            read_len = 0
            data = b""
            while read_len < len:
                try:
                    data = data + socket_fd.recv(len)
                except socket.timeout:
                    #--timeout, but has data
                    if data.__len__() != 0:
                        return [(c, data, data_len, '0', '0')]
                    else:
                        return [(c, None, None, 'P0016', 'transfer timeout')]
                #--"no more data, server close socket"
                if read_len == data.__len__():
                    break
                read_len = data.__len__()
            #--"no data read"
            if read_len == 0:
                return [(c, None, None, 'P0013', 'end-of-input reached')]
            return [(c, data, read_len, '0', '0')]

        #--"using buff, read buffer first"
        if hava > 0:
            #--"Not enough data in buffer, read from socket"
            if len > hava:
                data = inbuff['buf'][inbuff['pos']:]
                read_len = 0
                need_len = len - hava
                data_b = b""
                while read_len < need_len:
                    #--"wanner to read one more buffer size data"
                    try:
                        data_b_tmp = socket_fd.recv(need_len + inbuff['size'])
                    except socket.timeout:
                        if data.__len__() == 0:
                            return [(c, None, None, 'P0016', 'transfer timeout')]
                    #--"no more data"
                    if data_b_tmp.__len__() == 0:
                        len = data.__len__()
                        inbuff['buf'] = b""
                        inbuff['pos'] = 0
                        return [(c, data, data.__len__(), '0', '0')]
                    data = data + data_b_tmp[:len - hava]
                    data_b = data_b + data_b_tmp
                    read_len = data_b.__len__()
                len = data.__len__()
                #--"refresh buffer"
                inbuff['buf'] = data_b[len - hava:]
                inbuff['pos'] = 0
            else:
                #--"read from buffer"
                len_tmp = inbuff['pos'] + len
                data = inbuff['buf'][inbuff['pos']:len_tmp]
                inbuff['pos'] = len_tmp
        #--"buffer has no data"
        else:
            read_len = 0
            need_len = len
            data_b = b""
            while read_len < need_len:
                try:
                    data_b = socket_fd.recv(len + inbuff['size'])
                except socket.timeout:
                    if read_len == 0:
                        return [(c, None, None, 'P0016', 'transfer timeout')]
                    else:
                        break
                read_len = data_b.__len__()
            data = data_b[:len]
            len = data.__len__()
            #--"refresh buffer"
            if data_b.__len__() > len:
                inbuff['buf'] = data_b[len:]
            else:
                inbuff['buf'] = b""
            inbuff['pos'] = 0

    if data.__len__() == 0:
        return [(c, None, None, 'P0013', 'end-of-input reached')]
    #--"len is parameter, can not use len(data)"
    return [(c, data, data.__len__(), '0', '0')]
else:
    return [(c, None, None, 'P0014', 'network error: not connected')]
$$ LANGUAGE plpython3u;

--write_raw
CREATE OR REPLACE FUNCTION UTL_TCP__WRITE_RAW (c    IN  public.utl_tcp.connection,
                   data      IN            BYTEA,
                   len       IN            INTEGER DEFAULT NULL,
                   data_len  OUT           INTEGER,
                   errcode   OUT           VARCHAR2,
                   errmsg    OUT           VARCHAR2)
RETURNS SETOF RECORD
AS $$
#--"check parameter"
global len
if len < 0:
    return [(None, 'P0015', 'bad argument')]
if data.__len__() < len:
    return [(None, 'P0015', 'bad argument')]

if 'socket' in SD:
    socket = SD['socket']
else:
    import socket;
    SD['socket'] = socket

private_sd = c['private_sd']
if ('utl_tcp_con' in GD and private_sd in GD['utl_tcp_con'] 
        and 'utl_tcp_outbuffer' in GD and private_sd in GD['utl_tcp_outbuffer']):
    outbuffer = GD['utl_tcp_outbuffer'][private_sd]['buf']
    write_len = outbuffer.write(data[:len])

    return [(write_len, '0', '0')]
else:
    return [(None, 'P0014', 'network error: not connected')]
$$ LANGUAGE plpython3u;

--read_text
--  Now "len" means that the character length is not the byte length, 
--  so it needs to be decoded to get the corresponding length
 
--  Read the buffer first, 
--  and then read from the socket if the data is insufficient
CREATE OR REPLACE FUNCTION UTL_TCP__READ_TEXT (c     IN OUT  public.utl_tcp.connection,
                  data      IN OUT        VARCHAR2,
                  len       IN            INTEGER DEFAULT 1,
                  peek      IN            BOOLEAN     DEFAULT FALSE,
                  data_len  OUT           INTEGER,
                  errcode   OUT           VARCHAR2,
                  errmsg    OUT           VARCHAR2)
RETURNS SETOF RECORD
AS $$
#--"check parameter"
global len

if len < 0:
    return [(c, None, None, 'P0015', 'bad argument')]
if len == 0:
    return [(c, '', 0, '0', '0')]

if 'socket' in SD:
    socket = SD['socket']
else:
    import socket;
    SD['socket'] = socket

private_sd = c['private_sd']
charset = c['charset']
if ('utl_tcp_con' in GD and private_sd in GD['utl_tcp_con'] 
        and 'utl_tcp_inbuffer' in GD and private_sd in GD['utl_tcp_inbuffer']):
    socket_fd = GD['utl_tcp_con'][private_sd]
    inbuff = GD['utl_tcp_inbuffer'][private_sd]

    hava = inbuff['buf'].__len__() - inbuff['pos']
    want = inbuff['size'] - hava
    if len > 32767:
        len = 32767
    if peek:
        #--"not using buffer"
        if inbuff['size'] == 0:
            read_len = 0
            data_b = b""
            #--"want to read enough data"
            recv_buff = len * 4
            data_b_tmp = b""
            while read_len < len:
                try:
                    data_b_tmp = socket_fd.recv(recv_buff, socket.MSG_PEEK)
                except socket.timeout:
                    if read_len == 0:
                        return [(c, None, None, 'P0016', 'transfer timeout')]
                    else:
                        data_b_tmp = b""
                #--"no more data"
                if data_b_tmp.__len__() == 0:
                    break
                data_b = data_b + data_b_tmp
                data = data_b.decode(charset, errors='ignore')
                read_len = data.__len__()

                if data_b_tmp.__len__() == recv_buff:
                    break
            #--"no data read"
            if data.__len__() == 0:
                return [(c, None, None, 'P0013', 'end-of-input reached')]
            if read_len > len:
                data = data[:len]
                read_len = len
            return [(c, data, read_len, '0', '0')]

        #--"read from buff"
        buff = inbuff['buf'][inbuff['pos']:]
        if len > inbuff['size']:
            return [(c, None, None, 'P0012', 'buffer too small')]
        #--"read buffer first. Len now means character length, decode data"
        while True:
            try:
                data = buff.decode(charset)
                break
            except UnicodeError:
                buff = buff[:-1]
                if buff.__len__() == 0:
                    data = ''
                    break
        #--"no need to reflash buffer"
        if data.__len__() >= len:
            data = data[:len]
            return [(c, data, data.__len__(), '0', '0')]

        #--"reflash  buffer"
        else:
            #--"Prefix decoding error"
            prefix_err = False
            if data.__len__() == 0 and hava > 2:
                prefix_err = True
                buff = inbuff['buf'][inbuff['pos']:]
                data = buff.decode(charset,errors='ignore')
                if data.__len__() >= len:
                    data = data[:len]
                    return [(c, data, len, '0', '0')]
            
            #--"now read data from socket"
            data_b = b""
            try:
                data_b = socket_fd.recv(want)
            except socket.timeout:
                if data.__len__() == 0:
                    return [(c, None, None, 'P0016', 'transfer timeout')]
                else:
                    data_b = b""
            #--"no more data"
            if data_b.__len__() == 0:
                #--"no data read"
                if data.__len__() == 0:
                    buff = inbuff['buf'][inbuff['pos']:]
                    if buff.__len__() == 0:
                        return [(c, None, None, 'P0013', 'end-of-input reached')]
                    return [(c, None, None, 'P0018', 'partial multibyte character')]
                #--"Returns the data previously read from buffer"
                else:
                    len = data.__len__()
                    return [(c, data, len, '0', '0')]
            #--"refresh buffer"
            inbuff['buf'] = inbuff['buf'][inbuff['pos']:] + data_b
            inbuff['pos'] = 0
            buff = inbuff['buf']
            #--"read from buffer"
            while True and not(prefix_err):
                try:
                    data = buff.decode(charset)
                    break
                except UnicodeError:
                    buff = buff[:-1]
                    if buff.__len__() == 0:
                        return [(c, None, None, 'P0013', 'end-of-input reached')]
            if prefix_err:
                data = buff.decode(charset,errors='ignore')
            if data.__len__() >= len:
                data = data[:len]
                return [(c, data, len, '0', '0')]
            else:
                return [(c, None, None, 'P0012', 'buffer too small')]
    #--"peek = false"
    else:
        #--"read buff first"
        buff = inbuff['buf'][inbuff['pos']:]
        longer = True
        while True:
            try:
                data= buff.decode(charset)
                break
            except UnicodeError:
                if buff.__len__() == 0:
                    break
                buff = buff[:-1]
        if data.__len__() >= len:
            longer = False
            data = data[:len]
            len = data.__len__()

        #--"update buff read pos"
        read_len = data.encode(charset).__len__()
        inbuff['pos'] = inbuff['pos'] + read_len
        
        #--"the data want to read > the data in the buffer"
        if longer:
            data_tmp = ''
            need_len = len
            #--"prefix decoding error"
            prefix_err = False
            if data.__len__() == 0 and hava > 5:
                prefix_err = True
            #--"the rest of the data can't be decode"
            
            buff = inbuff['buf'][inbuff['pos']:]
            read_len = data.__len__()
            want_len = inbuff['size']
            if want_len == 0:
                want_len = len
            i = 1
            data_b_tmp = buff[:5]
            data_b = b''
            while read_len < need_len:
                #--"if has prefix decoding error, do not refresh the buffer first"
                if not(prefix_err):
                    #--"Read buffer size data to refresh buffer"
                    if inbuff['pos'] >= inbuff['buf'].__len__() or buff.__len__() != 0:
                        try:
                            data_b = socket_fd.recv(want_len)
                        except socket.timeout:
                            if data.__len__() == 0:
                                return [(c, None, None, 'P0016', 'transfer timeout')]
                            else:
                                data_b = b""
                    #--"no more data"
                    if data_b.__len__() == 0:
                        if data_b_tmp == buff:
                            break
                        while True:
                            try:
                                data_tmp = data_b_tmp.decode(charset)
                                data = data + data_tmp
                                len = data.__len__()
                                break
                            except UnicodeError:
                                data_b_tmp = data_b_tmp[:-1]
                                if data_b_tmp.__len__() == 0:
                                    len = data.__len__()
                                    break
                        break
                    #--"refresh buffer"
                    inbuff['buf'] = buff + data_b
                    inbuff['pos'] = 0
                #--"read from buffer"
                while True:
                    #--"deocding data"
                    try:
                        #--"get bytes of data from buffer first"
                        if data_b_tmp.__len__() == 0:
                            raise UnicodeError

                        #--"prefix decoding error"
                        if data_b_tmp.__len__() == 5:
                            data_tmp = data_b_tmp.decode(charset, errors = 'ignore')
                            i = 6
                            need_len = need_len
                            prefix_err = False
                        #--"normal decode"
                        else:
                            data_tmp = data_b_tmp.decode(charset)
                        
                        #--"update buffer read pos"
                        inbuff['pos'] = inbuff['pos'] + i - 1
                        data = data + data_tmp
                        read_len = data.__len__()
                        i = 1
                        #--"enough data has been read"
                        if read_len == need_len:
                            len = read_len
                            return [(c, data, len, '0', '0')]
                        #--"read_len < need_len, read more data to decode"
                        else:
                            raise UnicodeError
                    except UnicodeError:
                        #--"the buffer is not enough data and needs to be refreshed"
                        if inbuff['pos'] + i > inbuff['buf'].__len__():
                            buff = data_b_tmp
                            break
                        #--"In order to read the exact number of bytes,
                        #-- read data without flushing the buffer"
                        data_b_tmp = inbuff['buf'][inbuff['pos']:inbuff['pos'] + i]
                        i = i + 1


    if data.__len__() == 0:
        buff = inbuff['buf'][inbuff['pos']:]
        if buff.__len__() == 0:
            return [(c, None, None, 'P0013', 'end-of-input reached')]
        return [(c, None, None, 'P0018', 'partial multibyte character')]

    return [(c, data, len, '0', '0')]
else:
    return [(c, None, None, 'P0014', 'network error: not connected')]
$$ LANGUAGE plpython3u;

--write_text
CREATE OR REPLACE FUNCTION UTL_TCP__WRITE_TEXT (c    IN OUT  public.utl_tcp.connection,
                   data      IN            VARCHAR2,
                   len       IN            INTEGER DEFAULT NULL,
                   w_len     OUT           INTEGER,
                   errcode   OUT           VARCHAR2,
                   errmsg    OUT           VARCHAR2)
RETURNS SETOF RECORD
AS $$
global len
global data
#--"check parameter"
if len < 0:
    return [(c, None, 'P0015', 'bad argument')]

if 'socket' in SD:
    socket = SD['socket']
else:
    import socket;
    SD['socket'] = socket

private_sd = c['private_sd']
charset = c['charset']
if ('utl_tcp_con' in GD and private_sd in GD['utl_tcp_con'] 
        and 'utl_tcp_outbuffer' in GD and private_sd in GD['utl_tcp_outbuffer']):
    if str(type(data)) == "<class 'NoneType'>":
        data = ''
    socket_fd = GD['utl_tcp_con'][private_sd]
    outbuffer = GD['utl_tcp_outbuffer'][private_sd]['buf']
    if len > data.__len__():
        return [(c, None, 'P0015', 'bad argument')]
    data = data[:len]
    data_bytes = data.encode(charset)
    write_len = outbuffer.write(data_bytes)
    return [(c, len, '0', '0')]
else:
    return [(c, None, 'P0014', 'network error: not connected')]

$$ LANGUAGE plpython3u;

--read_line
-- read enough data first, and then find the newline
CREATE OR REPLACE FUNCTION UTL_TCP__READ_LINE (c     IN OUT  public.utl_tcp.connection,
                  data          IN OUT        VARCHAR2,
                  remove_crlf   IN            BOOLEAN   DEFAULT FALSE,
                  peek          IN            BOOLEAN   DEFAULT FALSE,
                  data_len      OUT           INTEGER,
                  errcode       OUT           VARCHAR2,
                  errmsg        OUT           VARCHAR2)
RETURNS SETOF RECORD
AS $$
global remove_crlf
global peek
if remove_crlf == None:
    return [(c, None, None, 'P0015', 'bad argument')]
if peek == None:
    return [(c, None, None, 'P0015', 'bad argument')]

if 'socket' in SD:
    socket = SD['socket']
else:
    import socket;
    SD['socket'] = socket

private_sd = c['private_sd']
charset = c['charset']
if ('utl_tcp_con' in GD and private_sd in GD['utl_tcp_con'] 
        and 'utl_tcp_inbuffer' in GD and private_sd in GD['utl_tcp_inbuffer']):
    socket_fd = GD['utl_tcp_con'][private_sd]
    inbuff = GD['utl_tcp_inbuffer'][private_sd]

    len = 0
    newline_b_crlf = c['newline'].encode(charset)
    newline_b_cr = '\\r'.encode(charset)
    newline_b_lf = '\\n'.encode(charset)
    newline_b = newline_b_crlf
    #--"not using buffer"
    if inbuff['size'] == 0:
        pos = -1
        data_b = b""
        while pos == -1:
            #--"want to read enough data"
            recv_buff = 32767 * 4
            try:
                data_b_tmp = socket_fd.recv(recv_buff, socket.MSG_PEEK)
            except socket.timeout:
                if data_b.__len__() == 0:
                    return [(c, None, None, 'P0016', 'transfer timeout')]
                else:
                    data_b_tmp = b""
            data_b = data_b + data_b_tmp
            pos = data_b.find(newline_b_crlf)
            if pos == -1:
                pos = data_b.find(newline_b_cr)
                newline_b = newline_b_cr
            if pos == -1:
                pos = data_b.find(newline_b_lf)
                newline_b = newline_b_lf
            #--"no more data"
            if data_b_tmp.__len__() == 0 or data_b_tmp.__len__() == recv_buff:
                break
        #--"newline found"
        if pos != -1:
            pos = pos + newline_b.__len__()
            data_b = data_b[:pos]
        #--"Intercept the required data"
        if not(peek):
            socket_fd.recv(data_b.__len__())
        #--"remove crlf"
        if pos != -1 and remove_crlf:
            data_b = data_b[:-newline_b.__len__()]

        data = data_b.decode(charset, errors='ignore')
        len = data.__len__()
        
        if len == 0:
            return [(c, None, None, 'P0013', 'end-of-input reached')]

        return [(c, data, len, '0', '0')]
    
    #--"find newline in buffer"
    read_data = inbuff['buf'][inbuff['pos']:]
    pos = read_data.find(newline_b_crlf)
    if pos == -1:
        pos = read_data.find(newline_b_cr)
        newline_b = newline_b_cr
    if pos == -1:
        pos = read_data.find(newline_b_lf)
        newline_b = newline_b_lf

    #--"in oracle 12c, Only buffer-sized data can be read, 
    #-- otherwise a "numeric or value error" error will be reported. 
    #-- This is implemented as a "buffer too small" error"

    #--"read all data in buff or need to load data to buff"
    while pos == -1:
        hava = inbuff['buf'].__len__() - inbuff['pos']
        want = inbuff['size'] - hava
        if want > 0:
            try:
                recv_data = socket_fd.recv(want)
            except socket.timeout:
                if hava == 0:
                    return [(c, None, None, 'P0016', 'transfer timeout')]
                else:
                    recv_data = b""
            #--"refresh buffer"
            inbuff['buf'] = inbuff['buf'][inbuff['pos']:] + recv_data
            inbuff['pos'] = 0
            read_data = inbuff['buf']
        #--"no newlines in buffer"
        else:
            #--"want to read enough data"
            recv_buff = 32767 * 4
            try:
                recv_data = socket_fd.recv(recv_buff, socket.MSG_PEEK)
            except socket.timeout:
                if hava == 0:
                    return [(c, None, None, 'P0016', 'transfer timeout')]
                else:
                    recv_data = b""
            if recv_data.__len__() != 0:
                return [(c, None, None, 'P0012', 'buffer too small')]

        #--"no data can be read"
        if read_data.__len__() == 0:
            return [(c, None, None, 'P0013', 'end-of-input reached')]
        pos = read_data.find(newline_b_crlf)
        if pos == -1:
            pos = read_data.find(newline_b_cr)
            newline_b = newline_b_cr
        if pos == -1:
            pos = read_data.find(newline_b_lf)
            newline_b = newline_b_lf
        if peek or recv_data.__len__() == 0:
            break
    #--"read all data"
    if pos == -1:
        data = read_data.decode(charset, errors = 'replace')
        len = read_data.__len__()
        if peek == False:
            inbuff['buf'] = b""
            inbuff['pos'] = 0
        return [(c, data, len, '0', '0')]
    
    if peek == False:
        inbuff['pos'] = inbuff['pos'] + pos + newline_b.__len__()
    if remove_crlf == False:
        pos = pos + newline_b.__len__()
    data = read_data[:pos].decode(charset, errors = 'replace')
    len = pos
    return [(c, data, len, '0', '0')]
else:
    return [(c, None, None, 'P0014', 'network error: not connected')]
$$ LANGUAGE plpython3u;

--write_line
CREATE OR REPLACE FUNCTION UTL_TCP__WRITE_LINE (c    IN OUT  public.utl_tcp.connection,
                   data      IN            VARCHAR2,
                   w_len     OUT           INTEGER,
                   errcode   OUT           VARCHAR2,
                   errmsg    OUT           VARCHAR2)
RETURNS SETOF RECORD
AS $$
global data
if 'socket' in SD:
    socket = SD['socket']
else:
    import socket;
    SD['socket'] = socket

private_sd = c['private_sd']
charset = c['charset']
newline = c['newline']
if ('utl_tcp_con' in GD and private_sd in GD['utl_tcp_con'] 
        and 'utl_tcp_outbuffer' in GD and private_sd in GD['utl_tcp_outbuffer']):
    if str(type(data)) == "<class 'NoneType'>":
        data = ''
    outbuffer = GD['utl_tcp_outbuffer'][private_sd]['buf']
    data_tmp = data + newline
    data_bytes = data_tmp.encode(charset)
    write_len = outbuffer.write(data_bytes)
    return [(c, write_len, '0', '0')]
else:
    return [(c, None, 'P0014', 'network error: not connected')]

$$ LANGUAGE plpython3u;

--flush
CREATE OR REPLACE FUNCTION UTL_TCP__FLUSH (c IN OUT public.utl_tcp.connection,
                            errcode   OUT  VARCHAR2,
                            errmsg    OUT  VARCHAR2)
RETURNS SETOF RECORD                         
AS $$

private_sd = c['private_sd']
if ('utl_tcp_con' in GD and private_sd in GD['utl_tcp_con'] 
        and 'utl_tcp_outbuffer' in GD and private_sd in GD['utl_tcp_outbuffer']):
    outbuffer = GD['utl_tcp_outbuffer'][private_sd]['buf']
    outbuffer.flush()
    return [(c, '0' ,'0')]
else:
    return [(c, 'P0014', 'network error: not connected')]

$$ LANGUAGE plpython3u;
----------------------------------------
--create package body utl_tcp
----------------------------------------
set search_path to public;
CREATE OR REPLACE PACKAGE BODY UTL_TCP
AS
    --open_connection
    FUNCTION OPEN_CONNECTION  (remote_host      IN VARCHAR2,
        remote_port      IN INTEGER,
        local_host       IN VARCHAR2 DEFAULT NULL,
        local_port       IN INTEGER DEFAULT NULL,
        in_buffer_size   IN INTEGER DEFAULT NULL,
        out_buffer_size  IN INTEGER DEFAULT NULL,
        charset          IN VARCHAR2 DEFAULT NULL,
        newline          IN VARCHAR2 DEFAULT E'\\r\\n',
        tx_timeout       IN INTEGER DEFAULT NULL) RETURN connection AS 
    DECLARE
        type read_type is record(
            c           public.utl_tcp.connection,
            errcode     VARCHAR2,
            errmsg      VARCHAR2
        );
        read_type_res read_type;
        c           public.utl_tcp.connection;
        errcode     VARCHAR2;
        errmsg      VARCHAR2;
    BEGIN
        read_type_res := utl_tcp_internal.UTL_TCP__OPEN_CONNECTION(remote_host, remote_port, local_host, local_port, 
                                                    in_buffer_size, out_buffer_size, charset, newline,
                                                    tx_timeout);
        c := read_type_res.c;
        errcode := read_type_res.errcode;
        errmsg := read_type_res.errmsg;
        if errcode != '0' then
            raise exception USING ERRCODE = errcode,message = errmsg;
        end if;
        return c;
    END;
    --close_connection
    FUNCTION CLOSE_CONNECTION (c IN OUT connection) RETURN connection AS
    BEGIN
        c := utl_tcp_internal.UTL_TCP__CLOSE_CONNECTION(c);
    END;
    --close_all_connection
    FUNCTION CLOSE_ALL_CONNECTIONS RETURN VOID AS
    BEGIN
        utl_tcp_internal.UTL_TCP__CLOSE_ALL_CONNECTIONS();
    END;
    -- available
    FUNCTION AVAILABLE (c        IN public.utl_tcp.connection, 
        timeout  IN INTEGER DEFAULT 0) RETURN INTEGER AS
    DECLARE
        data_len    INTEGER;
    BEGIN
        if timeout < 0 then
            CLOSE_CONNECTION(c);
            raise exception USING ERRCODE = 'P0015',message = 'bad argument';
        end if;
        data_len := utl_tcp_internal.UTL_TCP__AVAILABLE(c, timeout);
        if data_len < 0 then
            CLOSE_CONNECTION(c);
            raise exception using ERRCODE = 'P0014',message = 'network error: not connected';
        end if;
        return data_len;
    END;

    -- read_raw
    FUNCTION READ_RAW (c     IN OUT  connection,
        data      IN OUT        RAW,
        data_len  OUT           INTEGER,
        len       IN            INTEGER DEFAULT 1,
        peek      IN            BOOLEAN     DEFAULT FALSE
        ) RETURN SETOF RECORD AS
    DECLARE
        type read_type is record(
            c           connection,
            data        BYTEA,
            data_len    INTEGER,
            errcode     VARCHAR2,
            errmsg      VARCHAR2
        );
        read_type_res read_type;
        errcode     VARCHAR2;
        errmsg      VARCHAR2;
    BEGIN
        read_type_res := utl_tcp_internal.UTL_TCP__READ_RAW(c, read_type_res.data, len, peek);

        errcode := read_type_res.errcode;
        errmsg := read_type_res.errmsg;
        if errcode != '0' then
            CLOSE_CONNECTION(c);
            raise exception USING ERRCODE = errcode,message = errmsg;
        end if;

        c := read_type_res.c;

        data := byteatoraw(read_type_res.data);
        data_len := read_type_res.data_len;
        return next;
    END;

    --write_raw
    FUNCTION WRITE_RAW (c    IN  connection,
        data IN            RAW,
        len  IN            INTEGER DEFAULT NULL) RETURN INTEGER AS
    DECLARE
        type read_type is record(
            data_len    INTEGER,
            errcode     VARCHAR2,
            errmsg      VARCHAR2
        );
        read_type_res read_type;
        errcode     VARCHAR2;
        errmsg      VARCHAR2;
        data_tmp BYTEA;
    BEGIN
        data_tmp := rawtobytea(data);
        if len is NULL then
            len = length(data_tmp);
        end if;
        read_type_res := utl_tcp_internal.UTL_TCP__WRITE_RAW(c, data_tmp, len);

        errcode := read_type_res.errcode;
        errmsg := read_type_res.errmsg;
        if errcode != '0' then
            CLOSE_CONNECTION(c);
            raise exception USING ERRCODE = errcode,message = errmsg;
        end if;

        return read_type_res.data_len;
    END;

    --get_raw
    FUNCTION GET_RAW (c     IN  connection,
                 len   IN            INTEGER DEFAULT 1,
                 peek  IN            BOOLEAN     DEFAULT FALSE) RETURN RAW AS
    DECLARE
        type read_type is record(
            c           connection,
            data        BYTEA,
            data_len    INTEGER,
            errcode     VARCHAR2,
            errmsg      VARCHAR2
        );
        read_type_res read_type;
        errcode     VARCHAR2;
        errmsg      VARCHAR2;
        data    RAW;
    BEGIN
        read_type_res := utl_tcp_internal.UTL_TCP__READ_RAW(c, read_type_res.data, len, peek);

        errcode := read_type_res.errcode;
        errmsg := read_type_res.errmsg;
        if errcode != '0' then
            CLOSE_CONNECTION(c);
            raise exception USING ERRCODE = errcode,message = errmsg;
        end if;

        c := read_type_res.c;

        data := byteatoraw(read_type_res.data);
        return data;
    END;

    --read_text
    FUNCTION READ_TEXT (c     IN OUT  connection,
        data      IN OUT        VARCHAR2,
        data_len  OUT           INTEGER,
        len       IN            INTEGER DEFAULT 1,
        peek      IN            BOOLEAN     DEFAULT FALSE
        ) RETURN SETOF RECORD AS
    DECLARE
        type read_type is record(
            c           connection,
            data        VARCHAR2,
            data_len    INTEGER,
            errcode     VARCHAR2,
            errmsg      VARCHAR2
        );
        read_type_res read_type;
        errcode     VARCHAR2;
        errmsg      VARCHAR2;
    BEGIN
        read_type_res := utl_tcp_internal.UTL_TCP__READ_TEXT(c, data, len, peek);

        errcode := read_type_res.errcode;
        errmsg := read_type_res.errmsg;
        if errcode != '0' then
            CLOSE_CONNECTION(c);
            raise exception USING ERRCODE = errcode,message = errmsg;
        end if;

        c := read_type_res.c;
        data := read_type_res.data;
        data_len := read_type_res.data_len;
        return NEXT;
    END;

    --write_text
    FUNCTION WRITE_TEXT (c    IN  connection,
        data IN            VARCHAR2,
        len  IN            INTEGER DEFAULT NULL) RETURN INTEGER AS
    DECLARE
        type read_type is record(
            c           connection,
            data_len    INTEGER,
            errcode     VARCHAR2,
            errmsg      VARCHAR2
        );
        read_type_res read_type;
        errcode     VARCHAR2;
        errmsg      VARCHAR2;
        data_tmp BYTEA;
    BEGIN
        if len is NULL then
            if data is NULL then
                len := 0;
            else
                len := length(data);
            end if;
        end if;
        read_type_res := utl_tcp_internal.UTL_TCP__WRITE_TEXT(c, data, len);

        errcode := read_type_res.errcode;
        errmsg := read_type_res.errmsg;
        if errcode != '0' then
            CLOSE_CONNECTION(c);
            raise exception USING ERRCODE = errcode,message = errmsg;
        end if;

        return read_type_res.data_len;
    END;

    --get_text:call utl_tcp__read_text function
    FUNCTION GET_TEXT (c     IN connection,
        len   IN            INTEGER DEFAULT 1,
        peek  IN            BOOLEAN     DEFAULT FALSE) RETURN VARCHAR2 AS
    DECLARE
        type read_type is record(
            c           connection,
            data        VARCHAR2,
            data_len    INTEGER,
            errcode     VARCHAR2,
            errmsg      VARCHAR2
        );
        read_type_res read_type;
        errcode     VARCHAR2;
        errmsg      VARCHAR2;
    BEGIN
        read_type_res := utl_tcp_internal.UTL_TCP__READ_TEXT(c, read_type_res.data, len, peek);

        errcode := read_type_res.errcode;
        errmsg := read_type_res.errmsg;
        if errcode != '0' then
            CLOSE_CONNECTION(c);
            raise exception USING ERRCODE = errcode,message = errmsg;
        end if;

        return read_type_res.data;
    END;

    --read_line
    FUNCTION READ_LINE (c     IN OUT  connection,
        data          IN OUT        VARCHAR2,
        data_len      OUT           INTEGER,
        remove_crlf   IN            BOOLEAN   DEFAULT FALSE,
        peek          IN            BOOLEAN     DEFAULT FALSE
        ) RETURN SETOF RECORD AS
    DECLARE
        type read_type is record(
            c           connection,
            data        VARCHAR2,
            data_len    INTEGER,
            errcode     VARCHAR2,
            errmsg      VARCHAR2
        );
        read_type_res read_type;
        errcode     VARCHAR2;
        errmsg      VARCHAR2;
    BEGIN
        read_type_res := utl_tcp_internal.UTL_TCP__READ_LINE(c, data, remove_crlf, peek);

        errcode := read_type_res.errcode;
        errmsg := read_type_res.errmsg;
        if errcode != '0' then
            CLOSE_CONNECTION(c);
            raise exception USING ERRCODE = errcode,message = errmsg;
        end if;

        c := read_type_res.c;
        data := read_type_res.data;
        data_len := read_type_res.data_len;
        return NEXT;
    END;

    --write_line
    FUNCTION WRITE_LINE (c    IN  connection,
        data IN            VARCHAR2 DEFAULT NULL) RETURN INTEGER AS
    DECLARE
        type read_type is record(
            c           connection,
            data_len    INTEGER,
            errcode     VARCHAR2,
            errmsg      VARCHAR2
        );
        read_type_res read_type;
        errcode     VARCHAR2;
        errmsg      VARCHAR2;
        data_tmp BYTEA;
    BEGIN
        read_type_res := utl_tcp_internal.UTL_TCP__WRITE_LINE(c, data);

        errcode := read_type_res.errcode;
        errmsg := read_type_res.errmsg;
        if errcode != '0' then
            CLOSE_CONNECTION(c);
            raise exception USING ERRCODE = errcode,message = errmsg;
        end if;

        return read_type_res.data_len;
    END;

    /*--get_line: call utl_tcp__read_line*/
    FUNCTION GET_LINE (c     IN connection,
        remove_crlf IN         BOOLEAN DEFAULT FALSE,
        peek        IN         BOOLEAN     DEFAULT FALSE) RETURN VARCHAR2 AS
    DECLARE
        type read_type is record(
            c           connection,
            data        VARCHAR2,
            data_len    INTEGER,
            errcode     VARCHAR2,
            errmsg      VARCHAR2
        );
        read_type_res read_type;
        errcode     VARCHAR2;
        errmsg      VARCHAR2;
    BEGIN
        read_type_res := utl_tcp_internal.UTL_TCP__READ_LINE(c, read_type_res.data, remove_crlf, peek);

        errcode := read_type_res.errcode;
        errmsg := read_type_res.errmsg;
        if errcode != '0' then
            CLOSE_CONNECTION(c);
            raise exception USING ERRCODE = errcode,message = errmsg;
        end if;

        return read_type_res.data;
    END;

    --flush
    FUNCTION FLUSH (c IN OUT connection) RETURN connection AS
    DECLARE
        type read_type is record(
            c           connection,
            errcode     VARCHAR2,
            errmsg      VARCHAR2
        );
        read_type_res read_type;
        errcode     VARCHAR2;
        errmsg      VARCHAR2;
    BEGIN
        read_type_res := utl_tcp_internal.utl_tcp__flush(c);
        errcode := read_type_res.errcode;
        errmsg := read_type_res.errmsg;
        if errcode != '0' then
            CLOSE_CONNECTION(c);
            raise exception USING ERRCODE = errcode, message = errmsg;
        end if;

        return c;
    END;

end UTL_TCP;
'''
return c_sql
$c_sql$ language plpython3u;


DO LANGUAGE 'plpgsql'
$BODY$
declare
    dbcmpt varchar(10);
    c_sql text;
BEGIN
    select datcompatibility into dbcmpt from pg_database where datname=current_database();
    if dbcmpt = 'A' then -- if compatibility = 'A', execute oracle package sql file
        select generate_oracle_package_sql() into c_sql;
        execute c_sql;
    end if;
END  
$BODY$;

drop function generate_oracle_package_sql();
set search_path to default;
