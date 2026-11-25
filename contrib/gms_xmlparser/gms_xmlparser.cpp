/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * openGauss-server is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 *
 * --------------------------------------------------------------------------------------
 *
 * gms_xmlparser.cpp
 *  gms_xmlparser provides gms_xmlparser subprograms.
 *
 *
 * IDENTIFICATION
 *        contrib/gms_xmlparser/gms_xmlparser.cpp
 * 
 * --------------------------------------------------------------------------------------
 */
#include "xmlparser.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(gms_xmlparser_newparser);
PG_FUNCTION_INFO_V1(gms_xmlparser_parser_in);
PG_FUNCTION_INFO_V1(gms_xmlparser_parser_out);
PG_FUNCTION_INFO_V1(gms_xmlparser_getdocument);
PG_FUNCTION_INFO_V1(gms_xmlparser_parsebuffer);
PG_FUNCTION_INFO_V1(gms_xmlparser_parseclob);
PG_FUNCTION_INFO_V1(gms_xmlparser_parse4func);
PG_FUNCTION_INFO_V1(gms_xmlparser_parse4proc);
PG_FUNCTION_INFO_V1(gms_xmlparser_freeparser);
PG_FUNCTION_INFO_V1(document_in);
PG_FUNCTION_INFO_V1(document_out);
PG_FUNCTION_INFO_V1(freedocument);

/****************************************************************
 * gms_XMLPARSER.NEWPARSER
 *
 * Syntax:
 *   FUNCTION NEWPARSER() RETURNS Parser;
 * Purpouse:
 *   Create a new XML parser.
 ****************************************************************/

Datum gms_xmlparser_newparser(PG_FUNCTION_ARGS)
{
    MemoryContext old_context = MemoryContextSwitchTo(t_thrd.mem_cxt.cur_transaction_mem_cxt);
    XmlCtxt* ctxt = (XmlCtxt*)palloc(sizeof(XmlCtxt));
    if (!ctxt) {
        ereport(WARNING, (errmsg("Failed to malloc memory")));
        PG_RETURN_NULL();
    }

    ctxt->parser = xmlNewParserCtxt();
    MemoryContextSwitchTo(old_context);
    if (!ctxt->parser) {
        ereport(WARNING, (errmsg("Failed to create a new parser")));
        PG_RETURN_NULL();
    }

    PG_RETURN_POINTER(ctxt);
}

Datum gms_xmlparser_parser_in(PG_FUNCTION_ARGS)
{
    ereport(WARNING, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("parser input is not supported")));
    PG_RETURN_NULL();
}

Datum gms_xmlparser_parser_out(PG_FUNCTION_ARGS)
{
    ereport(WARNING, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("parser output is not supported")));
    PG_RETURN_NULL();
}

/****************************************************************
 * gms_XMLPARSER.FREEPARSER
 *
 * Syntax:
 *   PROCEDURE FREEPARSER() RETURNS Parser;
 * Purpouse:
 *   Free a XML parser.
 ****************************************************************/

Datum gms_xmlparser_freeparser(PG_FUNCTION_ARGS)
{
    XmlCtxt* ctxt = (XmlCtxt*) PG_GETARG_POINTER(0);
    if (!ctxt || !ctxt->parser) {
        ereport(WARNING, (errmsg("The input parser is invalid when freeparser")));
        PG_RETURN_NULL();
    }

    MemoryContext old_context = MemoryContextSwitchTo(t_thrd.mem_cxt.cur_transaction_mem_cxt);
    xmlFreeParserCtxt(ctxt->parser);
    ctxt->parser = NULL;
    pfree(ctxt);

    MemoryContextSwitchTo(old_context);
    PG_RETURN_VOID();
}

/****************************************************************
 * gms_XMLPARSER.PARSER
 *
 * Syntax:
 *   PROCEDURE  PARSER(p Parser, url VARHAR2)
 *   FUNCTION   PARSER(url VARCHAR) RETURNS DOMDocument;
 * Purpouse:
 *   Parse XML stored in the given URL or file.
 * Note:
     before using gms_xmlparser.parse(), you should call
     gms_xmlparser.newparser firstly. 
 ****************************************************************/

static bool is_xml_str(char* str)
{
    xmlDocPtr doc = xmlReadMemory(str, strlen(str), NULL, NULL, 0);
    if (doc != NULL) {
        xmlFreeDoc(doc);
        doc = NULL;
        return true;
    } else {
        xmlResetLastError();
        return false;
    }
}

Datum gms_xmlparser_parse4func_xml(char* xmlStr)
{
    MemoryContext old_context = MemoryContextSwitchTo(t_thrd.mem_cxt.cur_transaction_mem_cxt);
    xmlParserCtxtPtr parser = xmlNewParserCtxt();
    int len = strlen(xmlStr);
    xmlDocPtr doc = xmlCtxtReadMemory(parser, xmlStr, len, NULL, NULL, 0);    
    if (!doc) {
        xmlFreeParserCtxt(parser);
        MemoryContextSwitchTo(old_context);
        ereport(WARNING, (errmsg("Failed to parse XML string\n        %s", xmlStr)));
        return (Datum)0;
    }

    xmlFreeParserCtxt(parser);
    XmlDomDocument dom_doc = (XmlDomDocument) palloc(sizeof(_XmlDomDocument));
    if (!dom_doc) {
        ereport(WARNING, (errmsg("Failed to malloc memory")));
        return (Datum)0;
    }
    dom_doc->doc = doc;
    dom_doc->isfreed = false;

    MemoryContextSwitchTo(old_context);
    PG_RETURN_POINTER(dom_doc);
}

Datum gms_xmlparser_parse4func_file(char* filename)
{
    MemoryContext old_context = MemoryContextSwitchTo(t_thrd.mem_cxt.cur_transaction_mem_cxt);
    xmlParserCtxtPtr parser = xmlNewParserCtxt();
    xmlDocPtr doc = xmlCtxtReadFile(parser, filename, NULL, 0);    if (!doc) {
        xmlFreeParserCtxt(parser);
        MemoryContextSwitchTo(old_context);
        ereport(WARNING, (errmsg("Failed to parse XML file\n        %s", filename)));
        return (Datum)0;
    }

    xmlFreeParserCtxt(parser);
    XmlDomDocument dom_doc = (XmlDomDocument) palloc(sizeof(_XmlDomDocument));
    if (!dom_doc) {
        ereport(WARNING, (errmsg("Failed to malloc XmlDomDocument when parse_file")));
        return (Datum)0;
    }
    dom_doc->doc = doc;
    dom_doc->isfreed = false;

    MemoryContextSwitchTo(old_context);
    PG_RETURN_POINTER(dom_doc);
}


/* for function, there is only 1 argument. */
Datum gms_xmlparser_parse4func(PG_FUNCTION_ARGS)
{
    MemoryContext old_context = MemoryContextSwitchTo(t_thrd.mem_cxt.cur_transaction_mem_cxt);
    char* str = PG_ARGISNULL(0) ? NULL : text_to_cstring(PG_GETARG_TEXT_P(0));
    if (str == NULL || strcmp(str, "") == 0) {
        MemoryContextSwitchTo(old_context);
        ereport(WARNING,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("The resource handle or path name '' is invalid")));
        PG_RETURN_NULL();
    }

    if (is_xml_str(str)) {
        MemoryContextSwitchTo(old_context);
        return gms_xmlparser_parse4func_xml(str);
    }

    /* if the string is not xml string, we consider it as file url */
    if (access(str, F_OK) != 0) {
        MemoryContextSwitchTo(old_context);
        ereport(WARNING,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("The path %s is invalid, this xml file path does not exist", str)));
        PG_RETURN_NULL();
    }
    MemoryContextSwitchTo(old_context);
    return gms_xmlparser_parse4func_file(str);
}

Datum gms_xmlparser_parse4proc_xml(xmlParserCtxtPtr parser, char* xmlStr)
{
    MemoryContext old_context = MemoryContextSwitchTo(t_thrd.mem_cxt.cur_transaction_mem_cxt);
    int len = strlen(xmlStr);
    xmlDocPtr doc = xmlCtxtReadMemory(parser, xmlStr, len, NULL, NULL, 0);
    if (!doc) {
        char* errMsg = (char*)parser->lastError.message;
        xmlResetLastError();
        ereport(WARNING, (errmsg("Failed to parse XML string\n        %s", errMsg)));
        return (Datum)0;
    }

    xmlFreeDoc(doc);
    doc = NULL;

    MemoryContextSwitchTo(old_context);
    PG_RETURN_POINTER(parser);
}

Datum gms_xmlparser_parse4proc_file(xmlParserCtxtPtr parser, char* filename)
{
    MemoryContext old_context = MemoryContextSwitchTo(t_thrd.mem_cxt.cur_transaction_mem_cxt);
    xmlDocPtr doc = xmlCtxtReadFile(parser, filename, NULL, 0);
    if (!doc) {
        char* errMsg = (char*)parser->lastError.message;
        xmlResetLastError();
        ereport(WARNING, (errmsg("Failed to parse XML file\n        %s", errMsg)));
        return (Datum)0;
    }

    xmlFreeDoc(doc);
    doc = NULL;

    MemoryContextSwitchTo(old_context);
    PG_RETURN_POINTER(parser);
}

/* for procdure, there are 2 arguments. */
Datum gms_xmlparser_parse4proc(PG_FUNCTION_ARGS)
{
    MemoryContext old_context = MemoryContextSwitchTo(t_thrd.mem_cxt.cur_transaction_mem_cxt);
    XmlCtxt* ctxt = PG_ARGISNULL(0) ? NULL : (XmlCtxt*)PG_GETARG_POINTER(0);
    if (!ctxt || !ctxt->parser) {
        ereport(WARNING, (errmsg("The input parser is invalid in parse")));
        MemoryContextSwitchTo(old_context);
        PG_RETURN_NULL();
    }

    char* str = PG_ARGISNULL(1) ? NULL : text_to_cstring(PG_GETARG_TEXT_P(1));
    if (str == NULL || strcmp(str, "") == 0) {
        pfree_ext(str);
        MemoryContextSwitchTo(old_context);
        ereport(WARNING,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("The resource handle or path name '' is invalid")));
        PG_RETURN_NULL();
    }

    if (is_xml_str(str)) {
        MemoryContextSwitchTo(old_context);
        Datum result_xml = gms_xmlparser_parse4proc_xml(ctxt->parser, str);
        pfree_ext(str);
        return result_xml;
    }

    /* if the string is not xml string, we consider it as file url */
    if (access(str, F_OK) != 0) {
        pfree_ext(str);
        MemoryContextSwitchTo(old_context);
        ereport(WARNING,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("The path %s is invalid, this xml file path does not exist", str)));
        PG_RETURN_NULL();
    }
    MemoryContextSwitchTo(old_context);
    Datum result = gms_xmlparser_parse4proc_file(ctxt->parser, str);
    pfree_ext(str);
    return result;
}

/****************************************************************
 * gms_XMLPARSER.PARSEBUFFER
 *
 * Syntax:
 *   PROCEDURE PARSEBUFFER(p Parser, doc VARCHAR2);
 * Purpouse:
 *   Parses XML stored in the given buffer.
 ****************************************************************/  

Datum gms_xmlparser_parsebuffer(PG_FUNCTION_ARGS)
{
    XmlCtxt* ctxt = (XmlCtxt*) PG_GETARG_POINTER(0);
    if (!ctxt || !ctxt->parser) {
        ereport(WARNING, (errmsg("The input parser is invalid in parsebuffer")));
        PG_RETURN_NULL();
    }

    MemoryContext old_context = MemoryContextSwitchTo(t_thrd.mem_cxt.cur_transaction_mem_cxt);
    char* buffer;  
    if (PG_ARGISNULL(1)) {
        buffer = pstrdup("");
    } else {
        buffer = text_to_cstring(PG_GETARG_TEXT_P(1));
    }

    int size = strlen(buffer);
    xmlDocPtr doc = xmlCtxtReadMemory(ctxt->parser, buffer, size, NULL, NULL, 0);
    if (!doc) {
        char* errMsg = (char*)ctxt->parser->lastError.message;
        xmlResetLastError();
        ereport(WARNING, (errmsg("Invalid XML syntax in buffer\n        %s", errMsg)));
        pfree_ext(buffer);
        PG_RETURN_NULL();
    }
    xmlFreeDoc(doc);
    doc = NULL;
    pfree_ext(buffer);

    MemoryContextSwitchTo(old_context);
    PG_RETURN_NULL();
}

/****************************************************************
 * gms_XMLPARSER.PARSECLOB
 *
 * Syntax:
 *   PROCEDURE PARSECLOB(p Parser, doc CLOB);
 * Purpouse:
 *   Parses XML stored in the given clob. 
 ****************************************************************/

Datum gms_xmlparser_parseclob(PG_FUNCTION_ARGS)
{
    XmlCtxt* ctxt = (XmlCtxt*)PG_GETARG_POINTER(0);
    if (!ctxt || !ctxt->parser) {
        ereport(WARNING, (errmsg("The input parser is invalid in parseclob")));
        PG_RETURN_NULL();
    }

    MemoryContext old_context = MemoryContextSwitchTo(t_thrd.mem_cxt.cur_transaction_mem_cxt);
    char* buffer;
    int size;
    if (PG_ARGISNULL(1)) {
        buffer = "";
        size = 0;
    } else {
        text* lobtxt = PG_GETARG_TEXT_P(1);
        size = VARSIZE(lobtxt) - VARHDRSZ;
        buffer = (char*)palloc(size + 1);
        errno_t rc = memcpy_s(buffer, size + 1, VARDATA(lobtxt), size);
        securec_check(rc, "\0", "\0");
        buffer[size] = '\0';
    }

    xmlDocPtr doc = xmlCtxtReadMemory(ctxt->parser, buffer, size, NULL, NULL, 0);
    if (!doc) {
        char* errMsg = (char*)ctxt->parser->lastError.message;
        xmlResetLastError();
        ereport(WARNING, 
                (errmsg("Invalid XML syntax in clob\n        %s", errMsg)));
        PG_RETURN_NULL();
    }
    xmlFreeDoc(doc);
    doc = NULL;
    
    MemoryContextSwitchTo(old_context);
    PG_RETURN_NULL();
}

/****************************************************************
 * gms_XMLPARSER.GETDOCUMENT
 *
 * Syntax:
 *   FUNCTION DOCUMENT(p Parser) RETURNS DOMDocument;
 * Purpouse:
 *   Return the document node of a DOM tree document built by the
     parser, this function must by called after a document is passed.
 ****************************************************************/

Datum gms_xmlparser_getdocument(PG_FUNCTION_ARGS)
{
    XmlCtxt* ctxt = (XmlCtxt*)PG_GETARG_POINTER(0);
    if (!ctxt || !ctxt->parser || !ctxt->parser->input) {
        ereport(WARNING, (errmsg("The input parser is invalid when get document")));
        PG_RETURN_NULL();
    }
    
    MemoryContext old_context = MemoryContextSwitchTo(t_thrd.mem_cxt.cur_transaction_mem_cxt);
    XmlDomDocument dom_doc = (XmlDomDocument) palloc(sizeof(_XmlDomDocument));
    if (!dom_doc) {
        ereport(WARNING, (errmsg("Failed to alloc memory")));
        PG_RETURN_NULL();
    }

    xmlDocPtr doc = xmlParseDoc((xmlChar*)ctxt->parser->input->buf->context);    if (!doc) {
        xmlResetLastError();
        char* errMsg = (char*)ctxt->parser->lastError.message;
        ereport(WARNING, (errmsg("Failed to get xml document\n        %s", errMsg)));
        PG_RETURN_NULL();
    }
    dom_doc->doc = doc;
    dom_doc->isfreed = false;

    MemoryContextSwitchTo(old_context);
    PG_RETURN_POINTER(dom_doc);
}

Datum document_in(PG_FUNCTION_ARGS)
{
    char* str = PG_GETARG_CSTRING(0);
    XmlDomDocument domdoc = (XmlDomDocument)palloc(sizeof(_XmlDomDocument)); 
    if (!domdoc) {
        ereport(ERROR, (errmsg("Failed to alloc memory")));
    }
    if (domdoc->doc == NULL) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("The input string does not represent a valid XML str for document_in")));
    }
    domdoc->doc = xmlReadMemory(str, strlen(str), NULL, NULL, 0);
    domdoc->isfreed = false;

    PG_RETURN_POINTER(domdoc);
}

Datum document_out(PG_FUNCTION_ARGS)
{
    XmlDomDocument domdoc = (XmlDomDocument)PG_GETARG_POINTER(0);
    if (domdoc ==  NULL || domdoc->doc == NULL || domdoc->isfreed == true) {
        ereport(ERROR, (errmsg("Failed to output freed document")));
    }
    xmlChar* xmlbuff;
    int buffersize;
    xmlDocDumpFormatMemory(domdoc->doc, &xmlbuff, &buffersize, 1);
    int len = strlen((char*)xmlbuff);
    if (len == 0) {
        PG_RETURN_NULL();
    }
    char* result = (char*)palloc0(len + 1);
    errno_t rc = memcpy_s(result, len, (char*)xmlbuff, len);
    securec_check(rc, "\0", "\0");
    xmlFree(xmlbuff);

    PG_RETURN_CSTRING(result);
}

Datum freedocument(PG_FUNCTION_ARGS)
{
    XmlDomDocument domdoc = (XmlDomDocument)PG_GETARG_POINTER(0);
    if (domdoc ==  NULL || domdoc->doc == NULL || domdoc->isfreed == true) {
        PG_RETURN_VOID();
    }
    if (domdoc && domdoc->doc && domdoc->isfreed == false) {
        xmlFreeDoc(domdoc->doc);
        domdoc->doc = NULL;
        domdoc->isfreed = true;
    }

    PG_RETURN_VOID();
}