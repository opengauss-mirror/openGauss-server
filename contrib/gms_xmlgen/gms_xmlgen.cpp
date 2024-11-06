#include "postgres.h"
#include "knl/knl_variable.h"
#include "fmgr.h"
#include "funcapi.h"
#include "libpq/pqformat.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/int16.h"
#include "utils/int8.h"
#include "utils/memutils.h"
#include "utils/numeric.h"
#include "utils/numeric_gs.h"
#include "utils/portal.h"
#include "utils/xml.h"
#include "executor/spi.h"
#include "tcop/tcopprot.h"
#include "commands/extension.h"
#include <libxml/chvalid.h>
#include <libxml/parser.h>
#include <libxml/parserInternals.h>
#include <libxml/tree.h>
#include <libxml/uri.h>
#include <libxml/xmlerror.h>
#include <libxml/xmlversion.h>
#include <libxml/xmlwriter.h>
#include <libxml/xpath.h>
#include <libxml/xpathInternals.h>
#include "gms_xmlgen.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(ctxhandle_in);
PG_FUNCTION_INFO_V1(ctxhandle_out);
PG_FUNCTION_INFO_V1(close_context);
PG_FUNCTION_INFO_V1(convert_xml);
PG_FUNCTION_INFO_V1(convert_clob);
PG_FUNCTION_INFO_V1(get_num_rows_processed);
PG_FUNCTION_INFO_V1(get_xml_by_ctx_id);
PG_FUNCTION_INFO_V1(get_xml_by_query);
PG_FUNCTION_INFO_V1(new_context_by_query);
PG_FUNCTION_INFO_V1(new_context_by_cursor);
PG_FUNCTION_INFO_V1(new_context_from_hierarchy);
PG_FUNCTION_INFO_V1(restart_query);
PG_FUNCTION_INFO_V1(set_convert_special_chars);
PG_FUNCTION_INFO_V1(set_max_rows);
PG_FUNCTION_INFO_V1(set_null_handling);
PG_FUNCTION_INFO_V1(set_row_set_tag);
PG_FUNCTION_INFO_V1(set_row_tag);
PG_FUNCTION_INFO_V1(set_skip_rows);
PG_FUNCTION_INFO_V1(use_item_tags_for_coll);

static uint32 g_xmlgen_extension_id;

#define TOP_CONTEXT ((XMLGenTopContext *)(u_sess->attr.attr_common.extension_session_vars_array[g_xmlgen_extension_id]))

void set_extension_index(uint32 index)
{
    g_xmlgen_extension_id = index;
}

void init_session_vars(void)
{
    RepallocSessionVarsArrayIfNecessary();

    XMLGenTopContext *g_xmlgen_mem_ctx =
        (XMLGenTopContext *)MemoryContextAllocZero(u_sess->self_mem_cxt, sizeof(XMLGenTopContext));

    g_xmlgen_mem_ctx->memctx = u_sess->self_mem_cxt;
    g_xmlgen_mem_ctx->xmlgen_context_list = NIL;
    g_xmlgen_mem_ctx->xmlgen_context_id = 0;

    u_sess->attr.attr_common.extension_session_vars_array[g_xmlgen_extension_id] = g_xmlgen_mem_ctx;
}

/**
 * get XMLGenContext by ctx_id
 * @param {int64} ctx_id
 * @return {XMLGenContext} ctx
 */
static XMLGenContext *get_xmlgen_context(int64 ctx_id)
{
    if (TOP_CONTEXT->xmlgen_context_list == NIL) {
        return NULL;
    }

    ListCell *lc = NULL;
    foreach (lc, TOP_CONTEXT->xmlgen_context_list) {
        XMLGenContext *ctx = (XMLGenContext *)lfirst(lc);
        if (ctx->ctx_id == ctx_id) {
            return ctx;
        }
    }

    return NULL;
}

/**
 * free XMLGenContext
 * @param {XMLGenContext*} ctx - XMLGenContext pointer
 * @return {void}
 */
static void free_xmlgen_context(XMLGenContext *ctx)
{
    if (ctx->memctx != NULL) {
        MemoryContextDelete(ctx->memctx);
    }

    MemoryContext oldcontext = MemoryContextSwitchTo(TOP_CONTEXT->memctx);

    TOP_CONTEXT->xmlgen_context_list = list_delete(TOP_CONTEXT->xmlgen_context_list, (void *)ctx);

    MemoryContextSwitchTo(oldcontext);
    MemoryContextReset(ctx->memctx);
    pfree_ext(ctx);
}

/**
 * unescape XML data
 * @param {const char *} str - escaped string data
 * @return {char *} result - unescaped string data
 */
static char *convert_decode(const char *str)
{
    StringInfoData buf;
    const char *p = NULL;
    int charlen = 0;

    initStringInfo(&buf);
    p = str;
    while (*p) {
        if (*p == '&') {
            if (strncmp(p, "&lt;", 4) == 0) {
                appendStringInfoString(&buf, "<");
                charlen = 4;
            } else if (strncmp(p, "&gt;", 4) == 0) {
                appendStringInfoString(&buf, ">");
                charlen = 4;
            } else if (strncmp(p, "&quot;", 6) == 0) {
                appendStringInfoString(&buf, "\"");
                charlen = 6;
            } else if (strncmp(p, "&apos;", 6) == 0) {
                appendStringInfoString(&buf, "'");
                charlen = 6;
            } else if (strncmp(p, "&amp;", 5) == 0) {
                appendStringInfoString(&buf, "&");
                charlen = 5;
            } else {
                appendStringInfoString(&buf, "&");
                charlen = 1;
            }
        } else {
            charlen = pg_mblen(p);
            for (int i = 0; i < charlen; i++) {
                appendStringInfoCharMacro(&buf, p[i]);
            }
        }
        p += charlen;
    }
    return buf.data;
}

/**
 * escape XML data
 * @param {const char *} str - unescaped string data
 * @return {char *} result - escaped string data
 */
static char *convert_encode(const char *str)
{
    StringInfoData buf;
    const char *p = NULL;
    int charlen = 0;

    initStringInfo(&buf);
    p = str;
    while (*p) {
        charlen = pg_mblen(p);
        switch (*p) {
            case '&':
                appendStringInfoString(&buf, "&amp;");
                break;
            case '<':
                appendStringInfoString(&buf, "&lt;");
                break;
            case '>':
                appendStringInfoString(&buf, "&gt;");
                break;
            case '"':
                appendStringInfoString(&buf, "&quot;");
                break;
            case '\'':
                appendStringInfoString(&buf, "&apos;");
                break;
            default: {
                for (int i = 0; i < charlen; i++) {
                    appendStringInfoCharMacro(&buf, p[i]);
                }
                break;
            }
        }
        p += charlen;
    }
    return buf.data;
}

/**
 * map date value to xml value
 * @param {Datum} value - bytea datum value
 * @return {char *} result - xml value
 */
static char *map_date_value_to_xml(Datum value)
{
    DateADT date;
    struct pg_tm tm;
    char buf[MAXDATELEN + 1];

    date = DatumGetDateADT(value);
    if (DATE_NOT_FINITE(date)) {
        ereport(ERROR, (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE), errmsg("date out of range"),
                        errdetail("XML does not support infinite date values.")));
    }
    j2date(date + POSTGRES_EPOCH_JDATE, &(tm.tm_year), &(tm.tm_mon), &(tm.tm_mday));
    EncodeDateOnly(&tm, USE_XSD_DATES, buf);

    return pstrdup(buf);
}

/**
 * map timestamp value to xml value
 * @param {Datum} value - bytea datum value
 * @return {char *} result - xml value
 */
static char *map_timestamp_value_to_xml(Datum value, bool is_tz)
{
    TimestampTz timestamp;
    struct pg_tm tm;
    int tz;
    fsec_t fsec;
    const char *tzn = NULL;
    char buf[MAXDATELEN + 1];

    timestamp = DatumGetTimestamp(value);

    if (TIMESTAMP_NOT_FINITE(timestamp)) {
        ereport(ERROR, (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE), errmsg("timestamp out of range"),
                        errdetail("XML does not support infinite timestamp values.")));
    } else if (timestamp2tm(timestamp, is_tz ? &tz : NULL, &tm, &fsec, is_tz ? &tzn : NULL, NULL) == 0) {
        EncodeDateTime(&tm, fsec, is_tz, is_tz ? tz : 0, is_tz ? tzn : NULL, USE_XSD_DATES, buf);
    } else {
        ereport(ERROR, (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE), errmsg("timestamp out of range")));
    }

    return pstrdup(buf);
}

/**
 * map bytea value to xml value
 * @param {Datum} value - bytea datum value
 * @return {char *} result - xml value
 */
static char *map_bytea_value_to_xml(Datum value)
{
    bytea *bstr = DatumGetByteaPP(value);
    volatile xmlBufferPtr buf = NULL;
    volatile xmlTextWriterPtr writer = NULL;
    char *result = NULL;

    PG_TRY();
    {
        buf = xmlBufferCreate();
        if (buf == NULL) {
            ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("could not allocate xmlBuffer")));
        }
        writer = xmlNewTextWriterMemory(buf, 0);
        if (writer == NULL) {
            xmlBufferFree(buf);
            ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("could not allocate xmlTextWriter")));
        }
        if (u_sess->attr.attr_common.xmlbinary == XMLBINARY_BASE64) {
            xmlTextWriterWriteBase64(writer, VARDATA_ANY(bstr), 0, VARSIZE_ANY_EXHDR(bstr));
        } else {
            xmlTextWriterWriteBinHex(writer, VARDATA_ANY(bstr), 0, VARSIZE_ANY_EXHDR(bstr));
        }
        /* we MUST do this now to flush data out to the buffer */
        xmlFreeTextWriter(writer);
        writer = NULL;

        result = pstrdup((const char *)xmlBufferContent(buf));
    }
    PG_CATCH();
    {
        if (writer) {
            xmlFreeTextWriter(writer);
        }
        if (buf) {
            xmlBufferFree(buf);
        }
        PG_RE_THROW();
    }
    PG_END_TRY();

    xmlBufferFree(buf);

    return result;
}

/**
 * map array value to xml value
 * @param {Datum} value - array datum value
 * @param {bool} xml_escape_strings - is convert special chars
 * @param {char *} array_tag_suffix - array tag suffix
 * @param {int} indent_level - indentation level
 * @return {char *} result - xml value
 */
char *map_array_value_to_xml(Datum value, bool xml_escape_strings, char *array_tag_suffix, int indent_level)
{
    ArrayType *array = NULL;
    Oid elmtype;
    int16 elmlen;
    bool elmbyval = false;
    char elmalign;
    int num_elems;
    Datum *elem_values = NULL;
    bool *elem_nulls = NULL;
    StringInfoData buf;
    int i;

    array = DatumGetArrayTypeP(value);
    elmtype = ARR_ELEMTYPE(array);
    get_typlenbyvalalign(elmtype, &elmlen, &elmbyval, &elmalign);
    const char *type_name = get_typename(elmtype);
    const char *item_suffix = array_tag_suffix == NULL ? "" : array_tag_suffix;

    deconstruct_array(array, elmtype, elmlen, elmbyval, elmalign, &elem_values, &elem_nulls, &num_elems);

    initStringInfo(&buf);

    for (i = 0; i < num_elems; i++) {
        if (elem_nulls[i])
            continue;
        appendStringInfoString(&buf, "\n");
        appendStringInfoSpaces(&buf, (indent_level + 1) * SPACE_PER_INDENTATION);
        appendStringInfo(&buf, "<%s%s>", type_name, item_suffix);
        appendStringInfoString(&buf, map_sql_value_to_xml(elem_values[i], elmtype, xml_escape_strings, array_tag_suffix,
                                                          indent_level + 1));
        appendStringInfo(&buf, "</%s%s>", type_name, item_suffix);
    }
    appendStringInfoString(&buf, "\n");
    appendStringInfoSpaces(&buf, indent_level * SPACE_PER_INDENTATION);
    pfree_ext(elem_values);
    pfree_ext(elem_nulls);

    return buf.data;
}

/**
 * map sql column value to xml value
 * @param {Datum} value - column value
 * @param {Oid} type - column type
 * @param {bool} xml_escape_strings - is convert special chars
 * @param {char *} array_tag_suffix - array tag suffix
 * @param {int} indent_level - indentation level
 * @return {char *} result - xml value
 */
static char *map_sql_value_to_xml(Datum value, Oid type, bool xml_escape_strings, char *array_tag_suffix,
                                  int indent_level)
{
    if (type_is_array_domain(type)) {
        return map_array_value_to_xml(value, xml_escape_strings, array_tag_suffix, indent_level);
    } else {
        switch (type) {
            case BOOLOID:
                return (char *)(DatumGetBool(value) ? "true" : "false");
            case DATEOID:
                return map_date_value_to_xml(value);
            case TIMESTAMPOID:
                return map_timestamp_value_to_xml(value, false);
            case TIMESTAMPTZOID:
                return map_timestamp_value_to_xml(value, true);
            case BYTEAOID:
                return map_bytea_value_to_xml(value);
            default: {
                Oid typeOut;
                bool isvarlena = false;
                char *str = NULL;

                getTypeOutputInfo(type, &typeOut, &isvarlena);
                str = OidOutputFunctionCall(typeOut, value);

                if (type == XMLOID || !xml_escape_strings) {
                    return str;
                }

                return convert_encode(str);
            }
        }
    }
}

/**
 * convert datum to int
 * @param {Datum} datum
 * @return {Oid} type - datum type
 */
static int convert_datum_to_int(Datum datum, Oid type)
{
    Oid type_out;
    bool is_varlena = false;
    getTypeOutputInfo(type, &type_out, &is_varlena);
    char *str = OidOutputFunctionCall(type_out, datum);
    int result = 0;
    return sscanf_s(str, "%d", &result) == 1 ? result : -1;
}

/**
 * add node to hierarchy xml
 * @param {xmlNodePtr} pre_node
 * @param {xmlNodePtr} cur_node
 * @param {int} pre_level
 * @param {int} cur_level
 * @return {xmlNodePtr} return cur_node, or NULL otherwise
 */
static xmlNodePtr add_node_to_hierarchy_xml(xmlNodePtr pre_node, xmlNodePtr cur_node, int pre_level, int cur_level)
{
    if (pre_node == NULL) {
        return NULL;
    }
    xmlNodePtr pre_node_inner_pointer = pre_node;
    int pre_level_inner = pre_level;
    if (cur_level > pre_level_inner + 1) {
        return NULL;
    }
    int64 i = MAX_DEPTH;
    while (i > 0) {
        if (cur_level == pre_level_inner + 1) {
            xmlAddChild(pre_node_inner_pointer, cur_node);
            return cur_node;
        }
        if (pre_node_inner_pointer->parent == NULL) {
            return NULL;
        }
        pre_node_inner_pointer = pre_node_inner_pointer->parent;
        pre_level_inner -= 1;
        i--;
    }
    return NULL;
}

/**
 * convert hierarchy sql row to xml element
 * @param {int64} count_rows
 * @return {xmlDocPtr} result
 */
static xmlDocPtr SPI_hierarchy_sql_row_to_xml_element(int64 count_rows)
{
    xmlDocPtr result = xmlNewDoc(BAD_CAST "1.0");
    bool is_first_node = true;
    bool is_null = false;
    int pre_level = -1;
    xmlNodePtr pre_node = NULL;
    int cur_level = -1;
    xmlNodePtr cur_node = NULL;
    Datum level_datum = 0;

    for (int64 i = 0; i < count_rows; i++) {
        level_datum = SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1, &is_null);
        if (is_null) {
            if (result != NULL) {
                xmlFreeDoc(result);
            }
            ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("invalid hierarchy query result")));
        }
        cur_level = convert_datum_to_int(level_datum, SPI_gettypeid(SPI_tuptable->tupdesc, 1));
        if (cur_level < 0) {
            if (result != NULL) {
                xmlFreeDoc(result);
            }
            ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("invalid hierarchy query level")));
        }

        Datum xml_node = SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 2, &is_null);
        if (is_null) {
            char *xml_name = map_sql_identifier_to_xml_name(SPI_fname(SPI_tuptable->tupdesc, 2), true, false);
            cur_node = xmlNewNode(0, BAD_CAST xml_name);
        } else {
            char *xml_value = map_sql_value_to_xml(xml_node, SPI_gettypeid(SPI_tuptable->tupdesc, 2), false);
            xmlDocPtr doc_from_xml_value = xmlParseMemory(xml_value, (int)(strlen(xml_value)));
            cur_node = xmlCopyNode(xmlDocGetRootElement(doc_from_xml_value), 1);
            xmlFreeDoc(doc_from_xml_value);
        }
        if (is_first_node) {
            xmlNodePtr old_node = xmlDocSetRootElement(result, cur_node);
            xmlFreeNode(old_node);
            is_first_node = false;
        } else {
            xmlNodePtr cur_node_tmp = add_node_to_hierarchy_xml(pre_node, cur_node, pre_level, cur_level);
            if (cur_node_tmp == NULL) {
                xmlFreeNode(cur_node);
                xmlFreeDoc(result);
                ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("invalid hierarchy query node")));
            }
            cur_node = cur_node_tmp;
        }
        pre_node = cur_node;
        pre_level = cur_level;
    }
    return result;
}

/**
 * convert hierarchy sql row to string
 * @param {int64} count_rows
 * @param {StringInfo} result
 * @param {char *} row_set_tag - row-set tag name
 * @return {void}
 */
static void SPI_hierarchy_sql_row_to_string(int64 count_rows, StringInfo result, char *row_set_tag)
{
    xmlDocPtr xml_doc = SPI_hierarchy_sql_row_to_xml_element(count_rows);
    if (row_set_tag != NULL) {
        xmlNodePtr cur_node = xmlNewNode(0, BAD_CAST row_set_tag);
        xmlNodePtr old_node = xmlDocSetRootElement(xml_doc, cur_node);
        xmlAddChild(cur_node, old_node);
    }
    xmlChar *out_buf = NULL;
    int out_len = 0;
    xmlDocDumpFormatMemoryEnc(xml_doc, &out_buf, &out_len, "utf-8", 1);
    appendStringInfo(result, "%s", out_buf);
    xmlFree(out_buf);
    xmlFreeDoc(xml_doc);
}

/**
 * handle query to xml hierarchy
 * @param {XMLGenContext *} ctx
 * @return {StringInfo} result
 */
static StringInfo query_to_xml_hierarchy(XMLGenContext *ctx)
{
    StringInfo result = makeStringInfo();
    const char *query = (const char *)ctx->query_string;
    if (SPI_connect() != SPI_OK_CONNECT) {
        SPI_finish();
        ereport(ERROR, (errcode(ERRCODE_SPI_CONNECTION_FAILURE), errmsg("SPI_connect failed")));
    }

    if (SPI_execute(query, true, 0) != SPI_OK_SELECT) {
        SPI_finish();
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("fail to execute query")));
    }

    if (SPI_tuptable->tupdesc->natts != 2 ||
        (SPI_gettypeid(SPI_tuptable->tupdesc, 1) != NUMERICOID && SPI_gettypeid(SPI_tuptable->tupdesc, 1) != INT4OID) ||
        (SPI_gettypeid(SPI_tuptable->tupdesc, 2) != XMLOID &&
         strcasecmp(SPI_gettype(SPI_tuptable->tupdesc, 2), XMLTYPE_STR) != 0)) {
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("invalid query result")));
    }

    int64 count_rows = (int64)SPI_processed;
    SPI_hierarchy_sql_row_to_string(count_rows, result,
                                    ctx->is_hierarchy_set_row_set_tag ? ctx->row_set_tag_name : NULL);
    ctx->processed_rows = count_rows;
    SPI_finish();
    return result;
}

/**
 * generate xml doc root element start
 * @param {StringInfo} result
 * @param {const char*} eltname - root element name
 * @param {int64} null_flag
 * @return {void}
 */
static void xml_root_element_start(StringInfo result, const char *eltname, int64 null_flag)
{
    if (eltname == NULL) {
        return;
    }
    appendStringInfo(result, XML_HEADER);
    appendStringInfo(result, "<%s", eltname);
    if (null_flag == NULL_FLAG_NULL_ATTR) {
        appendStringInfo(result, " %s", XML_XSI_ATTR);
    }

    appendStringInfo(result, ">\n");
}

/**
 * generate xml doc root element end
 * @param {StringInfo} result
 * @param {const char*} eltname - root element name
 * @return {void}
 */
static void xml_root_element_end(StringInfo result, const char *eltname)
{
    if (eltname == NULL) {
        return;
    }
    appendStringInfo(result, "</%s>\n", eltname);
}

/**
 * convert sql row to xml elemeny by SPI
 * @param {int64} rownum - convert data from this rownum
 * @param {StringInfo} result
 * @param {int64} null_flag - how to display null element
 * @param {char *} row_tag - row tag name
 * @param {bool} is_convert_special_chars - is convert special chars
 * @param {char *} item_tag - tag suffix for array item
 * @param {int64} indentation_level - indentations before data
 * @return {void}
 */
static void SPI_sql_row_to_xml_element(int64 rownum, StringInfo result, int64 null_flag, char *row_tag,
                                       bool is_convert_special_chars, char *item_tag, int64 indentation_level)
{
    if (row_tag != NULL) {
        if (result->len == 0) {
            appendStringInfo(result, XML_HEADER);
            appendStringInfo(result, "<%s", row_tag);
            if (null_flag == NULL_FLAG_NULL_ATTR) {
                appendStringInfo(result, " %s", XML_XSI_ATTR);
            }
            appendStringInfo(result, ">\n");
        } else {
            appendStringInfoSpaces(result, indentation_level * SPACE_PER_INDENTATION);
            appendStringInfo(result, "<%s>\n", row_tag);
        }
    }

    for (int i = 1; i <= SPI_tuptable->tupdesc->natts; i++) {
        char *colname = map_sql_identifier_to_xml_name(SPI_fname(SPI_tuptable->tupdesc, i), true, false);
        bool isnull = false;
        Datum colval = SPI_getbinval(SPI_tuptable->vals[rownum], SPI_tuptable->tupdesc, i, &isnull);

        if (isnull) {
            if (colname == NULL) {
                continue;
            }
            if (null_flag == NULL_FLAG_NULL_ATTR) {
                appendStringInfoSpaces(result, indentation_level * SPACE_PER_INDENTATION);
                appendStringInfo(result, "%s%s<%s%s xsi:nil=\"true\"/>\n", result->len == 0 ? XML_HEADER : "",
                                 row_tag == NULL ? "" : "  ", result->len == 0 ? XML_XSI_ATTR : "", colname);
            } else if (null_flag == NULL_FLAG_EMPTY_TAG) {
                appendStringInfoSpaces(result, indentation_level * SPACE_PER_INDENTATION);
                appendStringInfo(result, "%s%s<%s/>\n", result->len == 0 ? XML_HEADER : "", row_tag == NULL ? "" : "  ",
                                 colname);
            }
        } else if (colname != NULL) {
            appendStringInfoSpaces(result, indentation_level * SPACE_PER_INDENTATION);
            appendStringInfo(result, "%s%s<%s%s>%s</%s>\n", result->len == 0 ? XML_HEADER : "",
                             row_tag == NULL ? "" : "  ", colname,
                             result->len == 0 && null_flag == NULL_FLAG_NULL_ATTR ? XML_XSI_ATTR : "",
                             map_sql_value_to_xml(colval, SPI_gettypeid(SPI_tuptable->tupdesc, i),
                                                  is_convert_special_chars, item_tag, indentation_level + 1),
                             colname);
        }
    }

    if (row_tag != NULL) {
        appendStringInfoSpaces(result, indentation_level * SPACE_PER_INDENTATION);
        appendStringInfo(result, "</%s>\n", row_tag);
    }
}

/**
 * check the xml is valid, fail if has no root element or has multiple ones
 * @param {const XMLGenContext *} ctx - xmlgen contxt pointer
 * @param {int64} rows - rows in xml
 * @param {int64} columns - columns in a row
 * @return {void}
 */
static void check_xml_valid(const XMLGenContext *ctx, int64 rows, int64 columns)
{
    if (ctx->row_set_tag_name == NULL && (rows != 1 || (ctx->row_tag_name == NULL && columns != 1))) {
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("the xml has multiple root nodes")));
    }
}

/**
 * convert query to xml
 * @param {const XMLGenContext *} ctx - xmlgen contxt pointer
 * @return {StringInfo} result
 */
static StringInfo query_to_xml_flat(XMLGenContext *ctx)
{
    StringInfo result = NULL;
    int64 skip_rows = ctx->skip_rows;
    errno_t et = EOK;
    char *row_tag = NULL;
    char *row_set_tag = NULL;
    Portal portal = NULL;

    if (ctx->row_tag_name != NULL) {
        row_tag = static_cast<char *>(palloc(sizeof(char) * (strlen(ctx->row_tag_name) + 1)));
        et = strcpy_s(row_tag, strlen(ctx->row_tag_name) + 1, ctx->row_tag_name);
        securec_check(et, "\0", "\0");
    }

    if (ctx->row_set_tag_name != NULL) {
        row_set_tag = static_cast<char *>(palloc(sizeof(char) * (strlen(ctx->row_set_tag_name) + 1)));
        et = strcpy_s(row_set_tag, strlen(ctx->row_set_tag_name) + 1, ctx->row_set_tag_name);
        securec_check(et, "\0", "\0");
    }

    int64 null_flag = ctx->null_flag;
    int64 processed_rows = 0;
    int64 count_rows;
    ctx->processed_rows = 0;
    result = makeStringInfo();

    if (SPI_connect() != SPI_OK_CONNECT) {
        pfree_ext(row_tag);
        pfree_ext(row_set_tag);
        SPI_finish();
        ereport(ERROR, (errcode(ERRCODE_SPI_CONNECTION_FAILURE), errmsg("SPI_connect failed")));
    }

    if (ctx->is_cursor || ctx->skip_rows != 0 || ctx->max_rows != 0) {
        if (ctx->is_cursor) {
            portal = SPI_cursor_find((const char *)ctx->query_string);
        } else {
            SPIPlanPtr plan = NULL;
            if ((plan = SPI_prepare((const char *)ctx->query_string, 0, NULL)) == NULL) {
                SPI_finish();
                ereport(ERROR, (errcode(ERRCODE_SPI_PREPARE_FAILURE), errmsg("SPI_prepare failed")));
            }

            if ((portal = SPI_cursor_open(NULL, plan, NULL, NULL, false)) == NULL) {
                SPI_finish();
                ereport(ERROR, (errcode(ERRCODE_SPI_CONNECTION_FAILURE), errmsg("SPI_cursor_open failed")));
            }
        }
        if (portal == NULL) {
            pfree_ext(row_tag);
            pfree_ext(row_set_tag);
            ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_CURSOR), errmsg("cursor \"%s\" does not exist", ctx->query_string)));
        }

        if (ctx->max_rows < 0) {
            SPI_cursor_fetch(portal, true, LONG_MAX);
        } else {
            SPI_cursor_fetch(portal, true, ctx->skip_rows + ctx->max_rows);
        }
    } else {
        if (SPI_execute(ctx->query_string, true, 0) != SPI_OK_SELECT) {
            pfree_ext(row_tag);
            pfree_ext(row_set_tag);
            SPI_finish();
            ereport(ERROR, (errcode(ERRCODE_SPI_CONNECTION_FAILURE), errmsg("fail to execute query")));
        }
    }

    count_rows = (int64)SPI_processed;
    check_xml_valid(ctx, count_rows, SPI_tuptable->tupdesc->natts);
    int64 start_now = skip_rows + ctx->current_row;
    if (ctx->max_rows != 0 && count_rows > start_now) {
        xml_root_element_start(result, row_set_tag == NULL ? row_tag : row_set_tag, null_flag);
        for (int64 i = skip_rows; i < count_rows; i++) {
            SPI_sql_row_to_xml_element(i, result, null_flag, (row_set_tag == NULL || row_tag == NULL) ? NULL : row_tag,
                                       ctx->is_convert_special_chars, ctx->item_tag_name,
                                       (row_set_tag == NULL && row_tag == NULL) ? 0 : 1);
            processed_rows++;
        }
        ctx->processed_rows = processed_rows;
        ctx->current_row += processed_rows + skip_rows;
        xml_root_element_end(result, row_set_tag == NULL ? row_tag : row_set_tag);
    }

    if (!ctx->is_cursor && portal != NULL) {
        SPI_cursor_close(portal);
    }
    SPI_finish();
    pfree_ext(row_tag);
    pfree_ext(row_set_tag);

    return result;
}

/**
 * top functin to handle converting query to xml
 * @param {const XMLGenContext *} ctx - xmlgen contxt pointer
 * @return {StringInfo} result
 */
static StringInfo query_to_xml_internal(XMLGenContext *ctx)
{
    if (ctx->is_from_hierarchy) {
        return query_to_xml_hierarchy(ctx);
    } else {
        return query_to_xml_flat(ctx);
    }
}

/**
 * top functin to handle converting query to xml
 * @param {const XMLGenContext *} ctx - xmlgen contxt pointer
 * @return {StringInfo} result
 */
static void check_query_string_valid(char *query_str)
{
    if (query_str == NULL) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid query string")));
    }
    List *queryTree = pg_parse_query(query_str);
    if (queryTree == NULL || queryTree->length != 1) {
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("can not parse sql: %s", query_str)));
    }
}

/**
 * create a new XMLGenContext with specific id
 * @param {int64} ctx_id
 * @param {XMLGenContext *} result
 */
static XMLGenContext *create_xmlgen_context(int64 ctx_id)
{

    if (TOP_CONTEXT == NULL) {
        init_session_vars();
    }

    MemoryContext oldcontext = MemoryContextSwitchTo(TOP_CONTEXT->memctx);
    XMLGenContext *ctx = static_cast<XMLGenContext *>(palloc0(sizeof(XMLGenContext)));
    ctx->memctx = AllocSetContextCreate(TOP_CONTEXT->memctx, "XMLGENContextMemory", ALLOCSET_DEFAULT_MINSIZE,
                                        ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);
    MemoryContextSwitchTo(oldcontext);

    oldcontext = MemoryContextSwitchTo(ctx->memctx);

    ctx->ctx_id = ctx_id;
    ctx->query_string = NULL;
    ctx->null_flag = 0;
    ctx->row_tag_name = "ROW";
    ctx->row_set_tag_name = "ROWSET";
    ctx->is_convert_special_chars = true;
    ctx->max_rows = -1;
    ctx->current_row = 0;
    ctx->skip_rows = 0;
    ctx->processed_rows = 0;
    ctx->is_from_hierarchy = false;
    ctx->is_hierarchy_set_row_set_tag = false;
    ctx->is_cursor = false;
    ctx->is_query = false;
    ctx->item_tag_name = NULL;

    MemoryContextSwitchTo(oldcontext);
    return ctx;
}

/**
 * create a new XMLGenContext by sequnce, and add it to global context list
 * @param {char *} query_str
 * @param {bool} is_from_herarchy
 * @param {bool} is_cursor
 */
static int64 new_xmlgen_context(char *query_str, bool is_from_hierarchy, bool is_cursor)
{
    MemoryContext oldcontext = NULL;
    int64 new_ctx_id = ++(TOP_CONTEXT->xmlgen_context_id);
    check_uint_valid(new_ctx_id);
    XMLGenContext *ctx = create_xmlgen_context(new_ctx_id);

    oldcontext = MemoryContextSwitchTo(ctx->memctx);
    char *qs = static_cast<char *>(palloc(sizeof(char) * strlen(query_str) + 1));
    errno_t et = strcpy_s(qs, strlen(query_str) + 1, query_str);
    securec_check(et, "\0", "\0");
    ctx->query_string = qs;
    MemoryContextSwitchTo(oldcontext);
    ctx->is_from_hierarchy = is_from_hierarchy;
    ctx->is_cursor = is_cursor;
    if (TOP_CONTEXT->xmlgen_context_list != NIL && !IsA(TOP_CONTEXT->xmlgen_context_list, List)) {
        TOP_CONTEXT->xmlgen_context_list = NIL;
    }

    oldcontext = MemoryContextSwitchTo(TOP_CONTEXT->memctx);
    TOP_CONTEXT->xmlgen_context_list = lappend(TOP_CONTEXT->xmlgen_context_list, (void *)ctx);
    MemoryContextSwitchTo(oldcontext);

    return new_ctx_id;
}

/**
 * cast string to ctxhandle
 * @param {string} str
 * @return {ctxhandle} result
 */
Datum ctxhandle_in(PG_FUNCTION_ARGS)
{
    char *str = PG_GETARG_CSTRING(0);
    int64 result;
    (void)scanint8(str, false, &result);

    PG_RETURN_NUMERIC(convert_int64_to_numeric(result, 0));
}

/**
 * cast ctxhandle to string
 * @param {ctxhandle} val
 * @return {string} result
 */
Datum ctxhandle_out(PG_FUNCTION_ARGS)
{
    Numeric num = PG_GETARG_NUMERIC(0);
    char *str = DatumGetCString(DirectFunctionCall1(numeric_out, NumericGetDatum(num)));
    PG_RETURN_CSTRING(str);
}

/**
 * close xmlgen context and release all resources, including the SQL cursor and bind and define buffers.
 * @param {ctxhandle} ctx - the context handle to close.
 * @return {void}
 */
Datum close_context(PG_FUNCTION_ARGS)
{
    CHECK_XML_SUPPORT();

    if (PG_ARGISNULL(0)) {
        PG_RETURN_VOID();
    }
    int64 ctx_id = (int64)numeric_int16_internal(PG_GETARG_NUMERIC(0));
    check_uint_valid(ctx_id);

    XMLGenContext *ctx = get_xmlgen_context(ctx_id);

    if (ctx != NULL) {
        free_xmlgen_context(ctx);
    }
    PG_RETURN_VOID();
}

/**
 * converts the xml data into the escaped or unescapes XML equivalent,
 * and returns XML clob data in endcoded or decoded format.
 * @param {varchar2} xmlData - the xml clob data to encoded or decoded.
 * @param {number} flag - the flag setting. 1 for decode, others for encode(default).
 * @return {void}
 */
Datum convert_xml(PG_FUNCTION_ARGS)
{
    CHECK_XML_SUPPORT();

    if (PG_ARGISNULL(0)) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid parameter")));
    }
    int64 flag = 0;
    if (!PG_ARGISNULL(1)) {
        flag = (int64)numeric_int16_internal(PG_GETARG_NUMERIC(1));
    }

    if (flag < 0 || flag > UINT_MAX) {
        PG_RETURN_NULL();
    }

    char *xmlstr = text_to_cstring(PG_GETARG_TEXT_PP(0));
    char *str = NULL;

    if (flag == 1) {
        str = convert_decode((const char *)xmlstr);
    } else {
        str = convert_encode((const char *)xmlstr);
    }

    VarChar *result = (VarChar *)cstring_to_text((const char *)str);
    pfree(str);
    pfree(xmlstr);
    PG_RETURN_VARCHAR_P(result);
}

/**
 * converts the xml data into the escaped or unescapes XML equivalent,
 * and returns XML clob data in endcoded or decoded format.
 * @param {clob} xmlData - the xml clob data to encoded or decoded.
 * @param {number} flag - the flag setting; 1 for decode, others for encode.
 * @return {clob} result
 */
Datum convert_clob(PG_FUNCTION_ARGS)
{
    CHECK_XML_SUPPORT();

    if (PG_ARGISNULL(0)) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid parameter")));
    }
    int64 flag = 0;
    if (!PG_ARGISNULL(1)) {
        flag = (int64)numeric_int16_internal(PG_GETARG_NUMERIC(1));
    }

    char *xmlstr = text_to_cstring((const text *)PG_GETARG_BYTEA_PP(0));
    if (flag < 0 || flag > UINT_MAX) {
        pfree(xmlstr);
        PG_RETURN_NULL();
    }

    char *str = NULL;

    if (flag == 1) {
        str = convert_decode((const char *)xmlstr);
    } else {
        str = convert_encode((const char *)xmlstr);
    }

    xmltype *result = (xmltype *)cstring_to_text((const char *)str);
    pfree(str);
    pfree(xmlstr);
    PG_RETURN_XML_P(result);
}

/**
 * retrieves the number of SQL rows processed when generating the xml using the getxml functions call.
 * @param {ctxhandle} ctx_id
 * @return {number} result
 */
Datum get_num_rows_processed(PG_FUNCTION_ARGS)
{
    CHECK_XML_SUPPORT();

    int64 ctx_id = (int64)numeric_int16_internal(PG_GETARG_NUMERIC(0));
    check_uint_valid(ctx_id);
    XMLGenContext *ctx = get_xmlgen_context(ctx_id);
    if (ctx == NULL) {
        PG_RETURN_NULL();
    }
    PG_RETURN_NUMERIC(convert_int64_to_numeric(ctx->processed_rows, 0));
}

/**
 * gets xml document by context id
 * @param {ctxhandle} ctx_id
 * @param {number} dtdOrSchema - a flag to generate a DTD or a schema, default 0
 * @return {clob} result - xml data
 */
Datum get_xml_by_ctx_id(PG_FUNCTION_ARGS)
{
    CHECK_XML_SUPPORT();

    if (PG_ARGISNULL(0)) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid gms_xmlgen context id")));
    }

    int64 ctx_id = (int64)numeric_int16_internal(PG_GETARG_NUMERIC(0));
    check_uint_valid(ctx_id);
    int64 dtd_or_schema = (int64)numeric_int16_internal(PG_GETARG_NUMERIC(1));
    check_uint_valid(dtd_or_schema);
    XMLGenContext *ctx = get_xmlgen_context(ctx_id);
    if (ctx == NULL || ctx->query_string == NULL) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid gms_xmlgen context found")));
    }
    StringInfo si = NULL;
    xmltype *res = NULL;
    si = query_to_xml_internal(ctx);
    res = stringinfo_to_xmltype(si);
    DestroyStringInfo(si);
    PG_RETURN_XML_P(res);
}

/**
 * gets xml document by query string
 * @param {varchar2} query string
 * @param {number} dtdOrSchema - a flag to generate a DTD or a schema, default 0
 * @return {clob} result - xml data
 */
Datum get_xml_by_query(PG_FUNCTION_ARGS)
{
    CHECK_XML_SUPPORT();

    if (PG_ARGISNULL(0)) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid query string")));
    }
    char *query_str = text_to_cstring(PG_GETARG_TEXT_PP(0));
    check_query_string_valid(query_str);
    int64 dtd_or_schema = (int64)numeric_int16_internal(PG_GETARG_NUMERIC(1));
    check_uint_valid(dtd_or_schema);
    XMLGenContext *ctx = create_xmlgen_context(0);
    ctx->query_string = query_str;
    StringInfo si = query_to_xml_internal(ctx);
    xmltype *res = stringinfo_to_xmltype(si);
    DestroyStringInfo(si);
    pfree_ext(query_str);
    free_xmlgen_context(ctx);
    PG_RETURN_XML_P(res);
}

/**
 * generate and return a new context handle from a query string
 * @param {varchar2} query string
 * @return {ctxhandle} result - context id
 */
Datum new_context_by_query(PG_FUNCTION_ARGS)
{
    CHECK_XML_SUPPORT();

    if (PG_ARGISNULL(0)) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid query string")));
    }
    char *query_str = text_to_cstring(PG_GETARG_TEXT_PP(0));
    int64 result = new_xmlgen_context(query_str);
    PG_RETURN_NUMERIC(convert_int64_to_numeric((int64)result, 0));
}

/**
 * generate and return a new context handle from a query string in the form of a PL/SQL ref cursor
 * @param {sys_refcursor} query string
 * @return {ctxhandle} result - context id
 */
Datum new_context_by_cursor(PG_FUNCTION_ARGS)
{
    CHECK_XML_SUPPORT();

    char *query_str = text_to_cstring(PG_GETARG_TEXT_PP(0));
    int64 result = new_xmlgen_context(query_str, false, true);
    PG_RETURN_NUMERIC(convert_int64_to_numeric((int64)result, 0));
}

/**
 * generate and return a new hierarchy context handle from query string
 * @param {varchar2} query string
 * @return {ctxhandle} result - context id
 */
Datum new_context_from_hierarchy(PG_FUNCTION_ARGS)
{
    CHECK_XML_SUPPORT();

    if (PG_ARGISNULL(0)) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid query string")));
    }
    char *query_str = text_to_cstring(PG_GETARG_TEXT_PP(0));
    int64 result = new_xmlgen_context(query_str, true);
    PG_RETURN_NUMERIC(convert_int64_to_numeric((int64)result, 0));
}

/**
 * restart the query and generate the XML from the first row
 * used to start executing the query again, without having to create a new context
 * @param {ctxhandle} ctx_id
 * @return {void}
 */
Datum restart_query(PG_FUNCTION_ARGS)
{
    CHECK_XML_SUPPORT();

    if (PG_ARGISNULL(0)) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid gms_xmlgen context id")));
    }

    int64 ctx_id = (int64)numeric_int16_internal(PG_GETARG_NUMERIC(0));
    check_uint_valid(ctx_id);
    XMLGenContext *ctx = get_xmlgen_context(ctx_id);
    if (ctx == NULL) {
        PG_RETURN_NULL();
    }

    if (ctx->is_cursor) {
        if (SPI_connect() != SPI_OK_CONNECT) {
            SPI_finish();
            ereport(ERROR, (errcode(ERRCODE_SPI_CONNECTION_FAILURE), errmsg("SPI_connect failed")));
        }

        Portal portal = SPI_cursor_find((const char *)ctx->query_string);
        if (portal == NULL) {
            SPI_finish();
            ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_CURSOR), errmsg("cursor \"%s\" does not exist", ctx->query_string)));
        }

        SPI_cursor_move(portal, false,
                        ctx->current_row + (ctx->max_rows < 0 || ctx->processed_rows < ctx->max_rows ? 1 : 0));

        SPI_finish();
    }
    ctx->current_row = 0;
    PG_RETURN_NULL();
}

/**
 * set whether or not special characters in the XML data must be converted into their escaped XML equivalent
 * @param {ctxhandle} ctx_id
 * @param {boolean} is_convert
 * @return {void}
 */
Datum set_convert_special_chars(PG_FUNCTION_ARGS)
{
    CHECK_XML_SUPPORT();

    if (PG_ARGISNULL(0)) {
        PG_RETURN_VOID();
    }
    int64 ctx_id = (int64)numeric_int16_internal(PG_GETARG_NUMERIC(0));
    check_uint_valid(ctx_id);
    XMLGenContext *ctx = get_xmlgen_context(ctx_id);
    bool is_convert = PG_GETARG_BOOL(1);
    if (ctx == NULL) {
        PG_RETURN_VOID();
    }
    ctx->is_convert_special_chars = is_convert;
    PG_RETURN_VOID();
}

/**
 * set the maximum number of rows to fetch from the SQL query result for every invocation of the GETXML functions call
 * @param {ctxhandle} ctx_id
 * @param {number} max_rows
 * @return {void}
 */
Datum set_max_rows(PG_FUNCTION_ARGS)
{
    CHECK_XML_SUPPORT();

    if (PG_ARGISNULL(0)) {
        PG_RETURN_VOID();
    }
    int64 ctx_id = (int64)numeric_int16_internal(PG_GETARG_NUMERIC(0));
    check_uint_valid(ctx_id);
    int64 max_rows = (int64)numeric_int16_internal(PG_GETARG_NUMERIC(1));
    check_uint_valid(max_rows);
    XMLGenContext *ctx = get_xmlgen_context(ctx_id);
    if (ctx == NULL) {
        PG_RETURN_VOID();
    }
    if (ctx->is_from_hierarchy) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("this operation is invalid in the hierarchy context")));
    }

    ctx->max_rows = max_rows;
    PG_RETURN_VOID();
}

/**
 * set null handling options, handled through the flag parameter setting
 * @param {ctxhandle} ctx_id
 * @param {number} flag - 0 for drop nulls(default); 1 for null attribute; 2 for empty tags
 * @return {void}
 */
Datum set_null_handling(PG_FUNCTION_ARGS)
{
    CHECK_XML_SUPPORT();

    int64 ctx_id = (int64)numeric_int16_internal(PG_GETARG_NUMERIC(0));
    check_uint_valid(ctx_id);
    int64 flag = (int64)numeric_int16_internal(PG_GETARG_NUMERIC(1));
    check_uint_valid(flag);
    XMLGenContext *ctx = get_xmlgen_context(ctx_id);
    if (ctx != NULL) {
        ctx->null_flag = flag;
    }
    PG_RETURN_VOID();
}

/**
 * set the name of the root element of the xml
 * @param {ctxhandle} ctx_id
 * @param {varchar2} row_set_tag_name
 * @return {void}
 */
Datum set_row_set_tag(PG_FUNCTION_ARGS)
{
    CHECK_XML_SUPPORT();

    if (PG_ARGISNULL(0)) {
        PG_RETURN_VOID();
    }
    int64 ctx_id = (int64)numeric_int16_internal(PG_GETARG_NUMERIC(0));
    check_uint_valid(ctx_id);
    XMLGenContext *ctx = get_xmlgen_context(ctx_id);
    if (ctx == NULL) {
        PG_RETURN_VOID();
    }
    if (ctx->is_from_hierarchy) {
        ctx->is_hierarchy_set_row_set_tag = true;
    }

    if (PG_ARGISNULL(1)) {
        ctx->row_set_tag_name = NULL;
        PG_RETURN_VOID();
    }

    char *row_set_tag_name = text_to_cstring(PG_GETARG_TEXT_PP(1));
    MemoryContext oldcontext = MemoryContextSwitchTo(ctx->memctx);
    char *row_set_tag_inner = static_cast<char *>(palloc(sizeof(char) * strlen(row_set_tag_name) + 1));
    errno_t et = strcpy_s(row_set_tag_inner, strlen(row_set_tag_name) + 1, row_set_tag_name);
    securec_check(et, "\0", "\0");
    ctx->row_set_tag_name = map_sql_identifier_to_xml_name(row_set_tag_inner, false, false);
    pfree_ext(row_set_tag_inner);
    MemoryContextSwitchTo(oldcontext);
    pfree_ext(row_set_tag_name);
    PG_RETURN_VOID();
}

/**
 * set the name of the element separating all the rows.
 * @param {ctxhandle} ctx_id
 * @param {varchar2} row_tag_name
 * @return {void}
 */
Datum set_row_tag(PG_FUNCTION_ARGS)
{
    CHECK_XML_SUPPORT();

    if (PG_ARGISNULL(0)) {
        PG_RETURN_VOID();
    }
    int64 ctx_id = (int64)numeric_int16_internal(PG_GETARG_NUMERIC(0));
    check_uint_valid(ctx_id);
    XMLGenContext *ctx = get_xmlgen_context(ctx_id);
    if (ctx == NULL) {
        PG_RETURN_VOID();
    }
    if (ctx->is_from_hierarchy) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("this operation is invalid in the hierarchy context")));
    }

    if (PG_ARGISNULL(1)) {
        ctx->row_tag_name = NULL;
        PG_RETURN_VOID();
    }

    char *row_tag_name = text_to_cstring(PG_GETARG_TEXT_PP(1));
    MemoryContext oldcontext = MemoryContextSwitchTo(ctx->memctx);

    char *row_tag_inner = static_cast<char *>(palloc(sizeof(char) * strlen(row_tag_name) + 1));
    errno_t et = strcpy_s(row_tag_inner, strlen(row_tag_name) + 1, row_tag_name);
    securec_check(et, "\0", "\0");
    ctx->row_tag_name = map_sql_identifier_to_xml_name(row_tag_inner, false, false);
    pfree_ext(row_tag_inner);
    MemoryContextSwitchTo(oldcontext);
    pfree_ext(row_tag_name);
    PG_RETURN_VOID();
}

/**
 * skip a given number of rows before generating the XML output for every call to the getxml functions
 * @param {ctxhandle} ctx_id
 * @param {number} skip_rows
 * @return {void}
 */
Datum set_skip_rows(PG_FUNCTION_ARGS)
{
    CHECK_XML_SUPPORT();

    if (PG_ARGISNULL(0)) {
        PG_RETURN_VOID();
    }
    int64 ctx_id = (int64)numeric_int16_internal(PG_GETARG_NUMERIC(0));
    check_uint_valid(ctx_id);
    int64 skip_rows = (int64)numeric_int16_internal(PG_GETARG_NUMERIC(1));
    check_uint_valid(skip_rows);
    XMLGenContext *ctx = get_xmlgen_context(ctx_id);
    if (ctx == NULL) {
        PG_RETURN_VOID();
    }
    if (ctx->is_from_hierarchy) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("this operation is invalid in the hierarchy context")));
    }

    ctx->skip_rows = skip_rows;
    PG_RETURN_VOID();
}

/**
 * override the default name of the collection elements.
 * @param {ctxhandle} ctx_id
 * @return {void}
 */
Datum use_item_tags_for_coll(PG_FUNCTION_ARGS)
{
    CHECK_XML_SUPPORT();

    if (PG_ARGISNULL(0)) {
        PG_RETURN_VOID();
    }
    int64 ctx_id = (int64)numeric_int16_internal(PG_GETARG_NUMERIC(0));
    check_uint_valid(ctx_id);
    XMLGenContext *ctx = get_xmlgen_context(ctx_id);
    if (ctx == NULL) {
        PG_RETURN_VOID();
    }

    ctx->item_tag_name = "_ITEM";
    PG_RETURN_VOID();
}
