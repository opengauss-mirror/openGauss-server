#ifndef GMS_XMLGEN_H
#define GMS_XMLGEN_H

#ifdef USE_LIBXML
#define CHECK_XML_SUPPORT()
#else
#define CHECK_XML_SUPPORT()                                                                    \
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("unsupported XML feature"), \
                    errdetail("This functionality requires the server to be built with libxml support.")))
#endif

// non-negative number between 0 ~ 4294967295
#define check_uint_valid(ctx_id)                                                                           \
    ((ctx_id < 0 || ctx_id > UINT_MAX)                                                                     \
         ? ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("value is out of range."))) \
         : (void)0)

#define SPACE_PER_INDENTATION 2
#define NULL_FLAG_NULL_ATTR 1
#define NULL_FLAG_EMPTY_TAG 2
#define MAX_DEPTH 65535
#define XMLTYPE_STR "xmltype"
#define XML_HEADER "<?xml version=\"1.0\"?>\n"
#define XML_XSI_ATTR "xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\""

typedef struct XMLGenContext {
    MemoryContext memctx;               // xmlgen memory context
    int64 ctx_id;                       // context id for XMLGenContext
    bool is_query;                      // is a query or not
    bool is_cursor;                     // is a cursor or not
    char *query_string;                 // query for xmlgen
    bool is_from_hierarchy;             // is in a hierarchy context
    bool is_hierarchy_set_row_set_tag;  // is set row_set_tag_name in a hierarchy context
    bool is_convert_special_chars;      // is need to encode special chars
    int64 max_rows;                     // max row for the xml
    int64 null_flag;                    // empty fields display, 0 for null, 1 for nil, 2 for self-close. default 0.
    char *row_tag_name;                 // element tag name for root
    char *row_set_tag_name;             // element tag name for each row
    int64 skip_rows;                    // skip the first n lines of the query result. default 0.
    int64 processed_rows;               // processed rows when generate xml data.
    int64 current_row;                  // gen xml from current_row. default 0.
    char *item_tag_name;                // element tag name suffix for array items
} XMLGenContext;

typedef struct XMLGenTopContext {
    MemoryContext memctx;
    List *xmlgen_context_list;
    int64 xmlgen_context_id;
} XMLGenTopContext;

static int64 new_xmlgen_context(char *query_str, bool is_from_hierarchy = false, bool is_cursor = false);
static char *map_sql_value_to_xml(Datum value, Oid type, bool xml_escape_strings, char *array_tag_suffix = "",
                                  int indent_level = 0);

extern "C" Datum ctxhandle_in(PG_FUNCTION_ARGS);
extern "C" Datum ctxhandle_out(PG_FUNCTION_ARGS);
extern "C" Datum close_context(PG_FUNCTION_ARGS);
extern "C" Datum convert_xml(PG_FUNCTION_ARGS);
extern "C" Datum convert_clob(PG_FUNCTION_ARGS);
extern "C" Datum get_num_rows_processed(PG_FUNCTION_ARGS);
extern "C" Datum get_xml_by_ctx_id(PG_FUNCTION_ARGS);
extern "C" Datum get_xml_by_query(PG_FUNCTION_ARGS);
extern "C" Datum new_context_by_query(PG_FUNCTION_ARGS);
extern "C" Datum new_context_by_cursor(PG_FUNCTION_ARGS);
extern "C" Datum new_context_from_hierarchy(PG_FUNCTION_ARGS);
extern "C" Datum restart_query(PG_FUNCTION_ARGS);
extern "C" Datum set_convert_special_chars(PG_FUNCTION_ARGS);
extern "C" Datum set_max_rows(PG_FUNCTION_ARGS);
extern "C" Datum set_null_handling(PG_FUNCTION_ARGS);
extern "C" Datum set_row_set_tag(PG_FUNCTION_ARGS);
extern "C" Datum set_row_tag(PG_FUNCTION_ARGS);
extern "C" Datum set_skip_rows(PG_FUNCTION_ARGS);
extern "C" Datum use_item_tags_for_coll(PG_FUNCTION_ARGS);
extern "C" void set_extension_index(uint32 index);
extern "C" void init_session_vars(void);

#endif  // GMS_XMLGEN_H
