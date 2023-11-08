/*-------------------------------------------------------------------------
 *
 * ddljson.cpp
 *    JSON code related to DDL command deparsing
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * NOTES
 *
 * Each JSONB object is supposed to have a "fmt" which will tell expansion
 * routines how JSONB can be expanded to construct ddl command. One example
 * snippet from JSONB object for 'ALTER TABLE sales ADD col1 int':
 *
 * { *1-level*
 *   "fmt": "ALTER %{objtype}s %{only}s %{identity}D %{subcmds:, }s",
 *   "only": "",
 *  "objtype": "TABLE",
 *  "identity": {"objname": "sales", "schemaname": "public"}
 *  "subcmds": [
 *      { *2-level*
 *          "fmt": "ADD %{objtype}s %{if_not_exists}s %{definition}s",
 *          "type": "add column",
 *          "objtype": "COLUMN",
 *          "definition": {}
 *           ...
 *      }
 *      ...
 * }
 *
 * From above, we can see different key-value pairs.
 * level-1 represents ROOT object with 'fmt', 'only', 'objtype','identity',
 * 'subcmds' as the keys with the values appended after ":" with each key.
 * Value can be string, bool, numeric, array or any nested object.  As an
 * example, "objtype" has string value while "subcmds" has nested-object
 * as its value which can further have multiple key-value pairs.
 *
 * The value of "fmt" tells us how the expansion will be carried on. The
 * value of "fmt"  may contain zero or more %-escapes, which consist of key
 * name enclosed in { }, followed by a conversion specifier which tells us
 * how the value for that particular key should be expanded.
 * Possible conversion specifiers are:
 * %            expand to a literal %
 * I            expand as a single, non-qualified identifier
 * D            expand as a possibly-qualified identifier
 * T            expand as a type name
 * L            expand as a string literal (quote using single quotes)
 * s            expand as a simple string (no quoting)
 * n            expand as a simple number (no quoting)
 *
 * In order to build a DDL command, it will first extract "fmt" node in
 * jsonb string and will read each key name enclosed in { } in fmt-string
 * and will replace it with its value. For each name mentioned in { } in
 * fmt string, there must be a key-value pair, in absence of which, the
 * expansion will error out. While doing this expansion, it will consider
 * the conversion-specifier maintained with each key in fmt string to figure
 * out how value should actually be represented. This is how DDL command can
 * be constructed back from the jsonb-string.
 *
 * IDENTIFICATION
 *    src/gausskernel/optimizer/commands/ddljson.cpp
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "knl/knl_thread.h"
#include "tcop/ddldeparse.h"
#include "utils/builtins.h"
#include "lib/stringinfo.h"
#include "utils/jsonb.h"

#define ADVANCE_PARSE_POINTER(ptr,end_ptr) \
    do { \
        if (++(ptr) >= (end_ptr)) \
            ereport(ERROR, \
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE), \
                    errmsg("unterminated format specifier"))); \
    } while (0)

/*
 * Conversion specifier which determines how to expand the JSON element
 * into a string.
 */
typedef enum
{
    SpecDottedName,
    SpecIdentifier,
    SpecNumber,
    SpecString,
    SpecStringLiteral,
    SpecTypeName
} convSpecifier;

/*
 * A ternary value that represents a boolean type JsonbValue.
 */
typedef enum
{
    tv_absent,
    tv_true,
    tv_false
} json_trivalue;

static bool expand_one_jsonb_element(StringInfo buf, char *param,
                                     JsonbValue *jsonval, convSpecifier specifier,
                                     const char *fmt);
static void expand_jsonb_array(StringInfo buf, char *param,
                               JsonbValue *jsonarr, char *arraysep,
                               convSpecifier specifier, const char *fmt);
static void fmtstr_error_callback(void *arg);

static void expand_fmt_recursive(StringInfo buf, JsonbSuperHeader sheader);
/*
 * Given a JsonbContainer, find the JsonbValue with the given key name in it.
 * If it's of a type other than jbvBool, an error is raised. If it doesn't
 * exist, tv_absent is returned; otherwise return the actual json_trivalue.
 */
static json_trivalue
find_bool_in_jsonbcontainer(JsonbSuperHeader sheader, char *keyname)
{
    JsonbValue  key;
    JsonbValue *value;
    json_trivalue result;

    key.type = jbvString;
    key.string.val = keyname;
    key.string.len = strlen(keyname);
    value = findJsonbValueFromSuperHeader(sheader,
                                        JB_FOBJECT, NULL, &key);
    if (value == NULL)
        return tv_absent;
    if (value->type != jbvBool)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("element \"%s\" is not of type boolean", keyname)));
    result = value->boolean ? tv_true : tv_false;
    pfree(value);

    return result;
}

/*
 * Given a JsonbContainer, find the JsonbValue with the given key name in it.
 * If it's of a type other than jbvString, an error is raised.  If it doesn't
 * exist, an error is raised unless missing_ok; otherwise return NULL.
 *
 * If it exists and is a string, a freshly palloc'ed copy is returned.
 *
 * If *length is not NULL, it is set to the length of the string.
 */
static char *
find_string_in_jsonbcontainer(JsonbSuperHeader sheader, char *keyname,
                              bool missing_ok, int *length)
{
    JsonbValue  key;
    JsonbValue *value;
    char       *str;

    /* XXX verify that this is an object, not an array */

    key.type = jbvString;
    key.string.val = keyname;
    key.string.len = strlen(keyname);
    value = findJsonbValueFromSuperHeader(sheader,
                                        JB_FOBJECT, NULL, &key);
    if (value == NULL) {
        if (missing_ok)
            return NULL;
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("missing element \"%s\" in JSON object", keyname)));
    }

    if (value->type != jbvString)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("element \"%s\" is not of type string", keyname)));

    str = pnstrdup(value->string.val, value->string.len);
    if (length)
        *length = value->string.len;
    pfree(value);
    return str;
}

/*
 * Expand a json value as a quoted identifier.  The value must be of type string.
 */
static void
expand_jsonval_identifier(StringInfo buf, JsonbValue *jsonval)
{
    char       *str;

    Assert(jsonval->type == jbvString);

    str = pnstrdup(jsonval->string.val, jsonval->string.len);
    appendStringInfoString(buf, quote_identifier(str));
    pfree(str);
}


/*
 * Expand a json value as a dot-separated-name.  The value must be of type
 * binary and may contain elements "schemaname" (optional), "objname"
 * (mandatory), "attrname" (optional).  Double quotes are added to each element
 * as necessary, and dot separators where needed.
 *
 * One day we might need a "catalog" element as well, but no current use case
 * needs that.
 */
static void
expand_jsonval_dottedname(StringInfo buf, JsonbValue *jsonval)
{
    char *str;
    JsonbSuperHeader data = jsonval->binary.data;

    Assert(jsonval->type == jbvBinary);

    str = find_string_in_jsonbcontainer(data, "schemaname", true, NULL);
    if (str) {
        appendStringInfo(buf, "%s.", quote_identifier(str));
        pfree(str);
    }

    str = find_string_in_jsonbcontainer(data, "objname", false, NULL);
    appendStringInfo(buf, "%s", quote_identifier(str));
    pfree(str);

    str = find_string_in_jsonbcontainer(data, "attrname", true, NULL);
    if (str) {
        appendStringInfo(buf, ".%s", quote_identifier(str));
        pfree(str);
    }
}

/*
 * Expand a JSON value as a type name.
 */
static void
expand_jsonval_typename(StringInfo buf, JsonbValue *jsonval)
{
    char *schema = NULL;
    char *typname = NULL;
    char *typmodstr = NULL;
    json_trivalue is_array;
    char *array_decor = NULL;
    JsonbSuperHeader data = jsonval->binary.data;

    /*
     * We omit schema-qualifying the output name if the schema element is
     * either the empty string or NULL; the difference between those two cases
     * is that in the latter we quote the type name, in the former we don't.
     * This allows for types with special typmod needs, such as interval and
     * timestamp (see format_type_detailed), while at the same time allowing
     * for the schema name to be omitted from type names that require quotes
     * but are to be obtained from a user schema.
     */

    schema = find_string_in_jsonbcontainer(data, "schemaname", true, NULL);
    typname = find_string_in_jsonbcontainer(data, "typename", false, NULL);
    typmodstr = find_string_in_jsonbcontainer(data, "typmod", true, NULL);
    is_array = find_bool_in_jsonbcontainer(data, "typarray");
    switch (is_array) {
        case tv_true:
            array_decor = "[]";
            break;

        case tv_false:
            array_decor = "";
            break;

        case tv_absent:
        default:
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("missing typarray element")));
    }

    if (schema == NULL)
        appendStringInfo(buf, "%s", quote_identifier(typname));
    else if (schema[0] == '\0')
        appendStringInfo(buf, "%s", typname);  /* Special typmod needs */
    else
        appendStringInfo(buf, "%s.%s", quote_identifier(schema),
                         quote_identifier(typname));

    appendStringInfo(buf, "%s%s", typmodstr ? typmodstr : "", array_decor);

    if (schema)
        pfree(schema);
    if (typname)
        pfree(typname);
    if (typmodstr)
        pfree(typmodstr);
}

/*
 * Expand a JSON value as a string.  The value must be of type string or of
 * type Binary.  In the latter case, it must contain a "fmt" element which will
 * be recursively expanded; also, if the object contains an element "present"
 * and it is set to false, the expansion is the empty string.
 *
 * Returns false if no actual expansion was made due to the "present" flag
 * being set to "false".
 *
 * The caller is responsible to check jsonval is of type jbvString or jbvBinary.
 */
static bool
expand_jsonval_string(StringInfo buf, JsonbValue *jsonval)
{
    bool expanded = false;

    Assert((jsonval->type == jbvString) || (jsonval->type == jbvBinary));

    if (jsonval->type == jbvString) {
        appendBinaryStringInfo(buf, jsonval->string.val,
                               jsonval->string.len);
        expanded = true;
    } else if (jsonval->type == jbvBinary) {
        json_trivalue present;

        present = find_bool_in_jsonbcontainer((JsonbSuperHeader)jsonval->binary.data,
                                              "present");

        /*
         * If "present" is set to false, this element expands to empty;
         * otherwise (either true or absent), expand "fmt".
         */
        if (present != tv_false) {
            expand_fmt_recursive(buf, (JsonbSuperHeader)jsonval->binary.data);
            expanded = true;
        }
    }

    return expanded;
}

/*
 * Expand a JSON value as a string literal.
 */
static void
expand_jsonval_strlit(StringInfo buf, JsonbValue *jsonval)
{
    char       *str;
    StringInfoData dqdelim;
    static const char dqsuffixes[] = "_XYZZYX_";
    int         dqnextchar = 0;

    Assert(jsonval->type == jbvString);

    str = pnstrdup(jsonval->string.val, jsonval->string.len);

    /* Easy case: if there are no ' and no \, just use a single quote */
    if (strpbrk(str, "\'\\") == NULL) {
        appendStringInfo(buf, "'%s'", str);
        pfree(str);
        return;
    }

    /* Otherwise need to find a useful dollar-quote delimiter */
    initStringInfo(&dqdelim);
    appendStringInfoString(&dqdelim, "$");
    while (strstr(str, dqdelim.data) != NULL) {
        appendStringInfoChar(&dqdelim, dqsuffixes[dqnextchar++]);
        dqnextchar = dqnextchar % (sizeof(dqsuffixes) - 1);
    }
    /* Add trailing $ */
    appendStringInfoChar(&dqdelim, '$');

    /* And finally produce the quoted literal into the output StringInfo */
    appendStringInfo(buf, "%s%s%s", dqdelim.data, str, dqdelim.data);
    pfree(dqdelim.data);
    pfree(str);
}

/*
 * Expand a JSON value as an integer quantity.
 */
static void
expand_jsonval_number(StringInfo buf, JsonbValue *jsonval)
{
    char *strdatum;

    Assert(jsonval->type == jbvNumeric);

    strdatum = DatumGetCString(DirectFunctionCall1(numeric_out,
                                                   NumericGetDatum(jsonval->numeric)));
    appendStringInfoString(buf, strdatum);
    pfree(strdatum);
}

/*
 * Expand one JSON element into the output StringInfo according to the
 * conversion specifier.  The element type is validated, and an error is raised
 * if it doesn't match what we expect for the conversion specifier.
 *
 * Returns true, except for the formatted string case if no actual expansion
 * was made (due to the "present" flag being set to "false").
 */
static bool
expand_one_jsonb_element(StringInfo buf, char *param, JsonbValue *jsonval,
                         convSpecifier specifier, const char *fmt)
{
    bool string_expanded = true;
    ErrorContextCallback sqlerrcontext;

    /* If we were given a format string, setup an ereport() context callback */
    if (fmt) {
        sqlerrcontext.callback = fmtstr_error_callback;
        sqlerrcontext.arg = (void *) fmt;
        sqlerrcontext.previous = t_thrd.log_cxt.error_context_stack;
        t_thrd.log_cxt.error_context_stack = &sqlerrcontext;
    }

    if (!jsonval)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("element \"%s\" not found", param)));

    switch (specifier) {
        case SpecIdentifier:
            if (jsonval->type != jbvString)
                ereport(ERROR,
                        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("expected JSON string for %%I element \"%s\", got %d",
                               param, jsonval->type)));
            expand_jsonval_identifier(buf, jsonval);
            break;

        case SpecDottedName:
            if (jsonval->type != jbvBinary)
                ereport(ERROR,
                        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("expected JSON struct for %%D element \"%s\", got %d",
                               param, jsonval->type)));
            expand_jsonval_dottedname(buf, jsonval);
            break;

        case SpecString:
            if (jsonval->type != jbvString && jsonval->type != jbvBinary)
                ereport(ERROR,
                        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("expected JSON string or struct for %%s element \"%s\", got %d",
                               param, jsonval->type)));
            string_expanded = expand_jsonval_string(buf, jsonval);
            break;

        case SpecStringLiteral:
            if (jsonval->type != jbvString)
                ereport(ERROR,
                        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("expected JSON string for %%L element \"%s\", got %d",
                               param, jsonval->type)));
            expand_jsonval_strlit(buf, jsonval);
            break;

        case SpecTypeName:
            if (jsonval->type != jbvBinary)
                ereport(ERROR,
                        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("expected JSON struct for %%T element \"%s\", got %d",
                               param, jsonval->type)));
            expand_jsonval_typename(buf, jsonval);
            break;

        case SpecNumber:
            if (jsonval->type != jbvNumeric)
                ereport(ERROR,
                        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("expected JSON numeric for %%n element \"%s\", got %d",
                               param, jsonval->type)));
            expand_jsonval_number(buf, jsonval);
            break;
    }

    if (fmt)
        t_thrd.log_cxt.error_context_stack = sqlerrcontext.previous;

    return string_expanded;
}

/*
 * Recursive helper for deparse_ddl_json_to_string.
 *
 * Find the "fmt" element in the given container, and expand it into the
 * provided StringInfo.
 */
static void
expand_fmt_recursive(StringInfo buf, JsonbSuperHeader sheader)
{
    JsonbValue key;
    JsonbValue *value;
    const char *cp;
    const char *start_ptr;
    const char *end_ptr;
    int len;

    start_ptr = find_string_in_jsonbcontainer(sheader, "fmt", false, &len);
    end_ptr = start_ptr + len;

    for (cp = start_ptr; cp < end_ptr; cp++) {
        convSpecifier specifier = SpecString;
        bool is_array = false;
        char *param = NULL;
        char *arraysep = NULL;

        if (*cp != '%') {
            appendStringInfoCharMacro(buf, *cp);
            continue;
        }

        ADVANCE_PARSE_POINTER(cp, end_ptr);

        /* Easy case: %% outputs a single % */
        if (*cp == '%') {
            appendStringInfoCharMacro(buf, *cp);
            continue;
        }

        /*
         * Scan the mandatory element name.  Allow for an array separator
         * (which may be the empty string) to be specified after a colon.
         */
        if (*cp == '{') {
            StringInfoData parbuf;
            StringInfoData arraysepbuf;
            StringInfo appendTo;

            initStringInfo(&parbuf);
            appendTo = &parbuf;

            ADVANCE_PARSE_POINTER(cp, end_ptr);
            while (cp < end_ptr) {
                if (*cp == ':') {
                    /*
                     * Found array separator delimiter; element name is now
                     * complete, start filling the separator.
                     */
                    initStringInfo(&arraysepbuf);
                    appendTo = &arraysepbuf;
                    is_array = true;
                    ADVANCE_PARSE_POINTER(cp, end_ptr);
                    continue;
                }

                if (*cp == '}') {
                    ADVANCE_PARSE_POINTER(cp, end_ptr);
                    break;
                }
                appendStringInfoCharMacro(appendTo, *cp);
                ADVANCE_PARSE_POINTER(cp, end_ptr);
            }
            param = parbuf.data;
            if (is_array)
                arraysep = arraysepbuf.data;
        }
        if (param == NULL)
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("missing conversion name in conversion specifier")));

        switch (*cp) {
            case 'I':
                specifier = SpecIdentifier;
                break;
            case 'D':
                specifier = SpecDottedName;
                break;
            case 's':
                specifier = SpecString;
                break;
            case 'L':
                specifier = SpecStringLiteral;
                break;
            case 'T':
                specifier = SpecTypeName;
                break;
            case 'n':
                specifier = SpecNumber;
                break;
            default:
                ereport(ERROR,
                        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("invalid conversion specifier \"%c\"", *cp)));
        }

        /*
         * Obtain the element to be expanded.
         */
        key.type = jbvString;
        key.string.val = param;
        key.string.len = strlen(param);

        value = findJsonbValueFromSuperHeader(sheader, JB_FOBJECT, NULL, &key);
        Assert(value != NULL);

        /*
         * Expand the data (possibly an array) into the output StringInfo.
         */
        if (is_array)
            expand_jsonb_array(buf, param, value, arraysep, specifier, start_ptr);
        else
            expand_one_jsonb_element(buf, param, value, specifier, start_ptr);

        pfree(value);
    }
}

/*
 * Iterate on the elements of a JSON array, expanding each one into the output
 * StringInfo per the given conversion specifier, separated by the given
 * separator.
 */
static void
expand_jsonb_array(StringInfo buf, char *param,
                   JsonbValue *jsonarr, char *arraysep, convSpecifier specifier,
                   const char *fmt)
{
    ErrorContextCallback sqlerrcontext;
    JsonbIterator *it;
    JsonbValue v;
    int type;
    bool first = true;
    StringInfoData arrayelem;

    /* If we were given a format string, setup an ereport() context callback */
    if (fmt) {
        sqlerrcontext.callback = fmtstr_error_callback;
        sqlerrcontext.arg = (void *) fmt;
        sqlerrcontext.previous = t_thrd.log_cxt.error_context_stack;
        t_thrd.log_cxt.error_context_stack = &sqlerrcontext;
    }

    if (!jsonarr)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("element \"%s\" not found", param)));

    if (jsonarr->type != jbvBinary)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("element \"%s\" is not a JSON array", param)));

    JsonbSuperHeader container = jsonarr->binary.data;
    if (!((*(uint32*)container) & JB_FARRAY))
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("element \"%s\" is not a JSON array", param)));

    initStringInfo(&arrayelem);

    it = JsonbIteratorInit(container);
    while ((type = JsonbIteratorNext(&it, &v, true)) != WJB_DONE) {
        if (type == WJB_ELEM) {
            resetStringInfo(&arrayelem);

            if (expand_one_jsonb_element(&arrayelem, param, &v, specifier, NULL)) {
                if (!first)
                    appendStringInfoString(buf, arraysep);

                appendBinaryStringInfo(buf, arrayelem.data, arrayelem.len);
                first = false;
            }
        }
    }

    if (fmt)
        t_thrd.log_cxt.error_context_stack = sqlerrcontext.previous;
}

char *
deparse_ddl_json_to_string(char *json_str, char** owner)
{
    Datum d;
    Jsonb *jsonb;
    StringInfo buf = (StringInfo) palloc0(sizeof(StringInfoData));

    initStringInfo(buf);

    d = DirectFunctionCall1(jsonb_in, PointerGetDatum(json_str));
    jsonb = (Jsonb *) DatumGetPointer(d);

    if (owner != NULL) {
        JsonbValue key;
        key.type = jbvString;
        key.string.val = "myowner";
        key.string.len = strlen(key.string.val);
        
        JsonbValue *value;
        JsonbSuperHeader data = VARDATA(jsonb);
        value = findJsonbValueFromSuperHeader(data, JB_FOBJECT, NULL, &key);
        if (value) {
            errno_t rc = 0;
            char *str = NULL;

            /* value->string.val may not be NULL terminated */
            str = (char*)palloc(value->string.len + 1);
            rc = memcpy_s(str, value->string.len, value->string.val, value->string.len);
            securec_check(rc, "\0", "\0");
            str[value->string.len] = '\0';
            *owner = str;
        }
        else
            /* myowner is not given in this jsonb, e.g. for Drop Commands */
            *owner = NULL;
    }

    expand_fmt_recursive(buf, VARDATA(jsonb));

    return buf->data;
}

/*
 * Error context callback for JSON format string expansion.
 *
 * XXX: indicate which element we're expanding, if applicable.
 */
static void
fmtstr_error_callback(void *arg)
{
    errcontext("while expanding format string \"%s\"", (char *) arg);
}
