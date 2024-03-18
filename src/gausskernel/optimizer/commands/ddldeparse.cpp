/*-------------------------------------------------------------------------
 *
 * ddldeparse.cpp
 *    Functions to convert utility commands to machine-parseable
 *    representation
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * NOTES
 *
 * This is intended to provide JSON blobs representing DDL commands, which can
 * later be re-processed into plain strings by well-defined sprintf-like
 * expansion.  These JSON objects are intended to allow for machine-editing of
 * the commands, by replacing certain nodes within the objects.
 *
 * Much of the information in the output blob actually comes from system
 * catalogs, not from the command parse node, as it is impossible to reliably
 * construct a fully-specified command (i.e. one not dependent on search_path
 * etc) looking only at the parse node.
 *
 * Deparse object tree is created by using:
 *  a) new_objtree("know contents") where the complete tree content is known or
 *     the initial tree content is known.
 *  b) new_objtree("") for the syntax where the object tree will be derived
 *     based on some conditional checks.
 *  c) new_objtree_VA where the complete tree can be derived using some fixed
 *     content and/or some variable arguments.
 *
 * IDENTIFICATION
 *    src/backend/commands/ddl_deparse.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/amapi.h"
#include "catalog/namespace.h"
#include "catalog/pg_am.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_cast.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_conversion.h"
#include "catalog/pg_depend.h"
#include "catalog/pg_extension.h"
#include "catalog/pg_foreign_data_wrapper.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_inherits.h"
#include "catalog/pg_language.h"
#include "catalog/pg_largeobject.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_opfamily.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_range.h"
#include "catalog/pg_rewrite.h"
#include "catalog/pg_statistic_ext.h"
#include "catalog/pg_ts_config.h"
#include "catalog/pg_ts_dict.h"
#include "catalog/pg_ts_parser.h"
#include "catalog/pg_ts_template.h"
#include "catalog/pg_type.h"
#include "catalog/pg_user_mapping.h"
#include "catalog/heap.h"
#include "commands/defrem.h"
#include "commands/sequence.h"
#include "commands/tablespace.h"
#include "foreign/foreign.h"
#include "funcapi.h"
#include "mb/pg_wchar.h"
#include "nodes/nodeFuncs.h"
#include "nodes/parsenodes.h"
#include "parser/parse_type.h"
#include "rewrite/rewriteHandler.h"
#include "tcop/ddldeparse.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/jsonb.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"

/* Estimated length of the generated jsonb string */
static const int JSONB_ESTIMATED_LEN = 128;

/*
 * Before they are turned into JSONB representation, each command is
 * represented as an object tree, using the structs below.
 */
typedef enum
{
    ObjTypeNull,
    ObjTypeBool,
    ObjTypeString,
    ObjTypeArray,
    ObjTypeInteger,
    ObjTypeFloat,
    ObjTypeObject
} ObjType;

/*
 * Represent the command as an object tree.
 */
typedef struct ObjTree
{
    slist_head  params;         /* Object tree parameters */
    int numParams;      /* Number of parameters in the object tree */
    StringInfo fmtinfo;        /* Format string of the ObjTree */
    bool present; /* Indicates if boolean value should be stored */
} ObjTree;

/*
 * An element of an object tree (ObjTree).
 */
typedef struct ObjElem
{
    char *name;           /* Name of object element */
    ObjType objtype;        /* Object type */

    union {
        bool boolean;
        char *string;
        int64 integer;
        float8 flt;
        ObjTree *object;
        List *array;
    } value;          /* Store the object value based on the object
                                 * type */
    slist_node node;           /* Used in converting back to ObjElem
                                 * structure */
} ObjElem;

/*
 * Reduce some unnecessary strings from the output json when verbose
 * and "present" member is false. This means these strings won't be merged into
 * the last DDL command.
 */

static void append_format_string(ObjTree *tree, char *sub_fmt);
static void append_array_object(ObjTree *tree, char *sub_fmt, List *array);
static void append_object_object(ObjTree *tree, char *sub_fmt, ObjTree *value);
static char *append_object_to_format_string(ObjTree *tree, const char *sub_fmt);
static void append_premade_object(ObjTree *tree, ObjElem *elem);
static void append_string_object(ObjTree *tree, char *sub_fmt, char *name,
                                 const char *value);
static void append_int_object(ObjTree *tree, char *sub_fmt, int32 value);
static void format_type_detailed(Oid type_oid, int32 typemod,
                                 Oid *nspid, char **typname, char **typemodstr,
                                 bool *typarray);
static ObjElem *new_object(ObjType type, char *name);
static ObjTree *new_objtree_for_qualname_id(Oid classId, Oid objectId);
static ObjTree *new_objtree(const char *fmt);
static ObjElem *new_object_object(ObjTree *value);

static ObjTree *new_objtree_VA(const char *fmt, int numobjs,...);

static JsonbValue *objtree_to_jsonb_rec(ObjTree *tree, JsonbParseState *state, char *owner);
static void pg_get_indexdef_detailed(Oid indexrelid, bool global,
                                     char **index_am,
                                     char **definition,
                                     char **reloptions,
                                     char **tablespace,
                                     char **whereClause,
                                     bool *invisible);
static char *RelationGetColumnDefault(Relation rel, AttrNumber attno,
                                      List *dpcontext, List **exprs);

static ObjTree *deparse_ColumnDef(Relation relation, List *dpcontext,
                                  ColumnDef *coldef, List **exprs);

static ObjTree *deparse_DefElem(DefElem *elem, bool is_reset);

static inline ObjElem *deparse_Seq_Cache(sequence_values *seqdata, bool alter_table);
static inline ObjElem *deparse_Seq_Cycle(sequence_values * seqdata, bool alter_table);
static inline ObjElem *deparse_Seq_IncrementBy(sequence_values * seqdata, bool alter_table);
static inline ObjElem *deparse_Seq_Minvalue(sequence_values * seqdata, bool alter_table);
static inline ObjElem *deparse_Seq_Maxvalue(sequence_values * seqdata, bool alter_table);
static inline ObjElem *deparse_Seq_Restart(char *last_value);
static inline ObjElem *deparse_Seq_Startwith(sequence_values * seqdata, bool alter_table);
static ObjElem *deparse_Seq_OwnedBy(Oid sequenceId);
static inline ObjElem *deparse_Seq_Order(DefElem *elem);
static inline ObjElem *deparse_Seq_As(DefElem *elem);

static List *deparse_TableElements(Relation relation, List *tableElements, List *dpcontext);

/*
 * Append an int32 parameter to a tree.
 */
static void
append_int_object(ObjTree *tree, char *sub_fmt, int32 value)
{
    ObjElem *param;
    char *object_name;

    Assert(sub_fmt);

    object_name = append_object_to_format_string(tree, sub_fmt);

    param = new_object(ObjTypeInteger, object_name);
    param->value.integer = value;
    append_premade_object(tree, param);
}

/*
 * Append a NULL-or-quoted-literal clause. Userful for COMMENT and SECURITY
 * LABEL.
 * 
 * Verbose syntax
 * %{null}s %{literal}s
 */
static void
append_literal_or_null(ObjTree *parent, char *elemname, char *value)
{
    ObjTree     *top;
    ObjTree     *part;

    top = new_objtree("");
    part = new_objtree_VA("NULL", 1,
                            "present", ObjTypeBool, !value);
    append_object_object(top, "%{null}s", part);

    part = new_objtree_VA("", 1,
                            "present", ObjTypeBool, value != NULL);

    if (value)
        append_string_object(part, "%{value}L", "value", value);
    append_object_object(top, "%{literal}s", part);

    append_object_object(parent, elemname, top);
}

/*
 * Append an array parameter to a tree.
 */
static void
append_array_object(ObjTree *tree, char *sub_fmt, List *array)
{
    ObjElem *param;
    char *object_name;

    Assert(sub_fmt);

    if (!array || list_length(array) == 0)
        return;

    ListCell   *lc;

    /* Remove elements where present flag is false */
    foreach(lc, array) {
        ObjElem *elem = (ObjElem *) lfirst(lc);

        if (elem->objtype != ObjTypeObject && elem->objtype != ObjTypeString) {
            elog(ERROR, "ObjectTypeArray %s elem need to be object or string", sub_fmt);
        }
    }

    object_name = append_object_to_format_string(tree, sub_fmt);

    param = new_object(ObjTypeArray, object_name);
    param->value.array = array;
    append_premade_object(tree, param);
}


/*
 * Append the input format string to the ObjTree.
 */
static void
append_format_string(ObjTree *tree, char *sub_fmt)
{
    int len;
    char *fmt;

    if (tree->fmtinfo == NULL)
        return;

    fmt = tree->fmtinfo->data;
    len = tree->fmtinfo->len;

    /* Add a separator if necessary */
    if (len > 0 && fmt[len - 1] != ' ')
        appendStringInfoSpaces(tree->fmtinfo, 1);

    appendStringInfoString(tree->fmtinfo, sub_fmt);
}

/*
 * Append an object parameter to a tree.
 */
static void
append_object_object(ObjTree *tree, char *sub_fmt, ObjTree *value)
{
    ObjElem    *param;
    char       *object_name;

    Assert(sub_fmt);

    if (!value)
        return;

    object_name = append_object_to_format_string(tree, sub_fmt);

    param = new_object(ObjTypeObject, object_name);
    param->value.object = value;
    append_premade_object(tree, param);
}

/*
 * Return the object name which is extracted from the input "*%{name[:.]}*"
 * style string. And append the input format string to the ObjTree.
 */
static char *
append_object_to_format_string(ObjTree *tree, const char *sub_fmt)
{
    StringInfoData object_name;
    const char *end_ptr, *start_ptr;
    int length;
    char *tmp_str;

    if (tree->fmtinfo == NULL)
        elog(ERROR, "object fmtinfo not found");

    initStringInfo(&object_name);

    start_ptr = strchr(sub_fmt, '{');
    end_ptr = strchr(sub_fmt, ':');
    if (end_ptr == NULL)
        end_ptr = strchr(sub_fmt, '}');

    if (start_ptr != NULL && end_ptr != NULL) {
        error_t rc = 0;
        length = end_ptr - start_ptr - 1;
        tmp_str = (char *) palloc(length + 1);
        rc = strncpy_s(tmp_str, length + 1, start_ptr + 1, length);
        securec_check_c(rc, "\0", "\0");
        tmp_str[length] = '\0';
        appendStringInfoString(&object_name, tmp_str);
        pfree(tmp_str);
    }

    if (object_name.len == 0)
        elog(ERROR, "object name not found");

    append_format_string(tree, pstrdup(sub_fmt));

    return object_name.data;
}

/*
 * Append a preallocated parameter to a tree.
 */
static inline void
append_premade_object(ObjTree *tree, ObjElem *elem)
{
    slist_push_head(&tree->params, &elem->node);
    tree->numParams++;
}

/*
 * Append a string parameter to a tree.
 */
static void
append_string_object(ObjTree *tree, char *sub_fmt, char *name,
                     const char *value)
{
    ObjElem    *param;

    Assert(sub_fmt);

    if (value == NULL || value[0] == '\0')
        return;

    append_format_string(tree, sub_fmt);
    param = new_object(ObjTypeString, name);
    param->value.string = pstrdup(value);
    append_premade_object(tree, param);
}

/*
 * Similar to format_type_extended, except we return each bit of information
 * separately:
 *
 * - nspid is the schema OID.  For certain SQL-standard types which have weird
 *   typmod rules, we return InvalidOid; the caller is expected to not schema-
 *   qualify the name nor add quotes to the type name in this case.
 *
 * - typname is set to the type name, without quotes
 *
 * - typemodstr is set to the typemod, if any, as a string with parentheses
 *
 * - typarray indicates whether []s must be added
 *
 * We don't try to decode type names to their standard-mandated names, except
 * in the cases of types with unusual typmod rules.
 */
static void
format_type_detailed(Oid type_oid, int32 typemod,
                     Oid *nspid, char **typname, char **typemodstr,
                     bool *typearray)
{
    HeapTuple tuple;
    Form_pg_type typeform;
    Oid array_base_type;

    tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(type_oid));
    if (!HeapTupleIsValid(tuple))
        elog(ERROR, "cache lookup failed for type with OID %u", type_oid);

    typeform = (Form_pg_type) GETSTRUCT(tuple);

    /*
     * Check if it's a regular (variable length) array type.  As above,
     * fixed-length array types such as "name" shouldn't get deconstructed.
     */
    array_base_type = typeform->typelem;

    *typearray = (typeform->typstorage != 'p') && (OidIsValid(array_base_type));

    if (*typearray) {
        /* Switch our attention to the array element type */
        ReleaseSysCache(tuple);
        tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(array_base_type));
        if (!HeapTupleIsValid(tuple))
            elog(ERROR, "cache lookup failed for type with OID %u", type_oid);

        typeform = (Form_pg_type) GETSTRUCT(tuple);
        type_oid = array_base_type;
    }

    /*
     * Special-case crock for types with strange typmod rules where we put
     * typemod at the middle of name (e.g. TIME(6) with time zone). We cannot
     * schema-qualify nor add quotes to the type name in these cases.
     */
    *nspid = InvalidOid;

    switch (type_oid) {
        case INTERVALOID:
            *typname = pstrdup("INTERVAL");
            break;
        case TIMESTAMPTZOID:
            if (typemod < 0)
                *typname = pstrdup("TIMESTAMP WITH TIME ZONE");
            else
                /* otherwise, WITH TZ is added by typmod. */
                *typname = pstrdup("TIMESTAMP");
            break;
        case TIMESTAMPOID:
            *typname = pstrdup("TIMESTAMP");
            break;
        case TIMETZOID:
            if (typemod < 0)
                *typname = pstrdup("TIME WITH TIME ZONE");
            else
                /* otherwise, WITH TZ is added by typmod. */
                *typname = pstrdup("TIME");
            break;
        case TIMEOID:
            *typname = pstrdup("TIME");
            break;
        default:

            /*
             * No additional processing is required for other types, so get
             * the type name and schema directly from the catalog.
             */
            *nspid = typeform->typnamespace;
            *typname = pstrdup(NameStr(typeform->typname));
    }

    if (typemod >= 0)
        *typemodstr = printTypmod("", typemod, typeform->typmodout);
    else
        *typemodstr = pstrdup("");

    ReleaseSysCache(tuple);
}

/*
 * Return the string representation of the given RELPERSISTENCE value.
 */
static inline char *
get_persistence_str(char persistence)
{
    switch (persistence) {
        case RELPERSISTENCE_TEMP:
            return "TEMPORARY";
        case RELPERSISTENCE_UNLOGGED:
            return "UNLOGGED";
        case RELPERSISTENCE_GLOBAL_TEMP:
            return "GLOBAL TEMPORARY";
        case RELPERSISTENCE_PERMANENT:
            return "";
        default:
            elog(ERROR, "unexpected persistence marking %c", persistence);
            return "";          /* make compiler happy */
    }
}

/*
 * Return the string representation of the given storagetype value.
 */
static inline char *
get_type_storage(char storagetype)
{
    switch (storagetype) {
        case 'p':
            return "plain";
        case 'e':
            return "external";
        case 'x':
            return "extended";
        case 'm':
            return "main";
        default:
            elog(ERROR, "invalid storage specifier %c", storagetype);
    }
}

/*
 * Allocate a new parameter.
 */
static ObjElem *
new_object(ObjType type, char *name)
{
    ObjElem *param;

    param = (ObjElem*)palloc0(sizeof(ObjElem));
    param->name = name;
    param->objtype = type;

    return param;
}

/*
 * Allocate a new object parameter.
 */
static ObjElem *
new_object_object(ObjTree *value)
{
    ObjElem *param;

    param = new_object(ObjTypeObject, NULL);
    param->value.object = value;

    return param;
}

/*
 * Allocate a new object tree to store parameter values.
 */
static ObjTree *
new_objtree(const char *fmt)
{
    ObjTree    *params;

    params = (ObjTree*)palloc0(sizeof(ObjTree));
    slist_init(&params->params);

    if (fmt)
    {
        params->fmtinfo = makeStringInfo();
        appendStringInfoString(params->fmtinfo, fmt);
    }

    return params;
}

/*
 * A helper routine to set up %{}D and %{}O elements.
 *
 * Elements "schema_name" and "obj_name" are set.  If the namespace OID
 * corresponds to a temp schema, that's set to "pg_temp".
 *
 * The difference between those two element types is whether the obj_name will
 * be quoted as an identifier or not, which is not something that this routine
 * concerns itself with; that will be up to the expand function.
 */
static ObjTree *
new_objtree_for_qualname(Oid nspid, char *name)
{
    ObjTree *qualified;
    char *namespc;

    if (isAnyTempNamespace(nspid))
        namespc = pstrdup("pg_temp");
    else
        namespc = get_namespace_name(nspid);

    qualified = new_objtree_VA(NULL, 2,
                               "schemaname", ObjTypeString, namespc,
                               "objname", ObjTypeString, pstrdup(name));

    return qualified;
}

/*
 * A helper routine to set up %{}D and %{}O elements, with the object specified
 * by classId/objId.
 */
static ObjTree *
new_objtree_for_qualname_id(Oid classId, Oid objectId)
{
    ObjTree *qualified;
    Relation catalog;
    HeapTuple catobj;
    Datum obj_nsp;
    Datum obj_name;
    AttrNumber Anum_name;
    AttrNumber Anum_namespace;
    bool isnull;

    catalog = relation_open(classId, AccessShareLock);

    catobj = get_catalog_object_by_oid(catalog, objectId);
    if (!catobj)
        elog(ERROR, "cache lookup failed for object with OID %u of catalog \"%s\"",
             objectId, RelationGetRelationName(catalog));
    Anum_name = get_object_attnum_name(classId);
    Anum_namespace = get_object_attnum_namespace(classId);

    obj_nsp = heap_getattr(catobj, Anum_namespace, RelationGetDescr(catalog),
                          &isnull);
    if (isnull)
        elog(ERROR, "null namespace for object %u", objectId);

    obj_name = heap_getattr(catobj, Anum_name, RelationGetDescr(catalog),
                           &isnull);
    if (isnull)
        elog(ERROR, "null attribute name for object %u", objectId);

    qualified = new_objtree_for_qualname(DatumGetObjectId(obj_nsp),
                                         NameStr(*DatumGetName(obj_name)));
    relation_close(catalog, AccessShareLock);

    return qualified;
}

/*
 * A helper routine to setup %{}T elements.
 */
static ObjTree *
new_objtree_for_type(Oid typeId, int32 typmod)
{
    Oid typnspid;
    char *type_nsp;
    char *type_name = NULL;
    char *typmodstr;
    bool type_array;

    format_type_detailed(typeId, typmod,
                         &typnspid, &type_name, &typmodstr, &type_array);

    if (OidIsValid(typnspid))
        type_nsp = get_namespace_name_or_temp(typnspid);
    else
        type_nsp = pstrdup("");

    return new_objtree_VA(NULL, 4,
                          "schemaname", ObjTypeString, type_nsp,
                          "typename", ObjTypeString, type_name,
                          "typmod", ObjTypeString, typmodstr,
                          "typarray", ObjTypeBool, type_array);
}

/*
 * Allocate a new object tree to store parameter values -- varargs version.
 *
 * The "fmt" argument is used to append as a "fmt" element in the output blob.
 * numobjs indicates the number of extra elements to append; for each one, a
 * name (string), type (from the ObjType enum) and value must be supplied.  The
 * value must match the type given; for instance, ObjTypeInteger requires an
 * int64, ObjTypeString requires a char *, ObjTypeArray requires a list (of
 * ObjElem), ObjTypeObject requires an ObjTree, and so on.  Each element type *
 * must match the conversion specifier given in the format string, as described
 * in ddl_deparse_expand_command, q.v.
 *
 * Note we don't have the luxury of sprintf-like compiler warnings for
 * malformed argument lists.
 */
static ObjTree *
new_objtree_VA(const char *fmt, int numobjs,...)
{
    ObjTree *tree;
    va_list args;
    int i;

    /* Set up the toplevel object and its "fmt" */
    tree = new_objtree(fmt);

    /* And process the given varargs */
    va_start(args, numobjs);
    for (i = 0; i < numobjs; i++) {
        char *name;
        ObjType type;
        ObjElem *elem;

        name = va_arg(args, char *);
        type =(ObjType)va_arg(args, int);
        elem = new_object(type, NULL);

        /*
         * For all param types other than ObjTypeNull, there must be a value in
         * the varargs. Fetch it and add the fully formed subobject into the
         * main object.
         */
        switch (type) {
            case ObjTypeNull:
                /* Null params don't have a value (obviously) */
                break;
            case ObjTypeBool:
                elem->value.boolean = va_arg(args, int);
                break;
            case ObjTypeString:
                elem->value.string = va_arg(args, char *);
                break;
            case ObjTypeArray:
                elem->value.array = va_arg(args, List *);
                break;
            case ObjTypeInteger:
                elem->value.integer = va_arg(args, int);
                break;
            case ObjTypeFloat:
                elem->value.flt = va_arg(args, double);
                break;
            case ObjTypeObject:
                elem->value.object = va_arg(args, ObjTree *);
                break;
            default:
                elog(ERROR, "invalid ObjTree element type %d", type);
        }

        elem->name = name;
        append_premade_object(tree, elem);
    }

    va_end(args);
    return tree;
}

/*
 * Return the given object type as a string.
 *
 * If isgrant is true, then this function is called while deparsing GRANT
 * statement and some object names are replaced.
 */
static const char *
string_objtype(ObjectType objtype, bool isgrant)
{
    switch (objtype) {
        case OBJECT_COLUMN:
            return isgrant ? "TABLE" : "COLUMN";
        case OBJECT_DOMAIN:
            return "DOMAIN";
        case OBJECT_INDEX:
            return "INDEX";
        case OBJECT_SEQUENCE:
            return "SEQUENCE";
        case OBJECT_LARGE_SEQUENCE:
            return "LARGE SEQUENCE";
        case OBJECT_TABLE:
            return "TABLE";
        case OBJECT_TYPE:
            return "TYPE";
        default:
            elog(WARNING, "unsupported object type %d for string", objtype);
    }

    return "???"; /* keep compiler quiet */
}

/*
 * Process the pre-built format string from the ObjTree into the output parse
 * state.
 */
static void
objtree_fmt_to_jsonb_element(JsonbParseState *state, ObjTree *tree)
{
    JsonbValue  key;
    JsonbValue  val;

    if (tree->fmtinfo == NULL)
        return;

    /* Push the key first */
    key.type = jbvString;
    key.string.val = "fmt";
    key.string.len = strlen(key.string.val);
    key.estSize = sizeof(JEntry) + key.string.len;
    pushJsonbValue(&state, WJB_KEY, &key);

    /* Then process the pre-built format string */
    val.type = jbvString;
    val.string.len = tree->fmtinfo->len;
    val.string.val = tree->fmtinfo->data;
    val.estSize = sizeof(JEntry) + val.string.len;
    pushJsonbValue(&state, WJB_VALUE, &val);
}

/*
 * Process the role string into the output parse state.
 */
static void
role_to_jsonb_element(JsonbParseState *state, char *owner)
{
    JsonbValue key;
    JsonbValue val;

    if (owner == NULL)
        return;

    /* Push the key first */
    key.type = jbvString;
    key.string.val = "myowner";
    key.string.len = strlen(key.string.val);
    key.estSize = sizeof(JEntry) + key.string.len;
    pushJsonbValue(&state, WJB_KEY, &key);

    /* Then process the role string */
    val.type = jbvString;
    val.string.len = strlen(owner);
    val.string.val = owner;
    val.estSize = sizeof(JEntry) + val.string.len;
    pushJsonbValue(&state, WJB_VALUE, &val);
}

/*
 * Create a JSONB representation from an ObjTree and its owner (if given).
 */
static Jsonb *
objtree_to_jsonb(ObjTree *tree, char *owner)
{
    JsonbValue *value;

    value = objtree_to_jsonb_rec(tree, NULL, owner);
    return JsonbValueToJsonb(value);
}

/*
 * Helper for objtree_to_jsonb: process an individual element from an object or
 * an array into the output parse state.
 */
static void
objtree_to_jsonb_element(JsonbParseState *state, ObjElem *object,
                         int elem_token)
{
    JsonbValue val;

    switch (object->objtype) {
        case ObjTypeNull:
            val.type = jbvNull;
            val.estSize = sizeof(JEntry);
            pushJsonbValue(&state, elem_token, &val);
            break;

        case ObjTypeString:
            val.type = jbvString;
            if (!object->value.string) {
                elog(ERROR, "ObjTypeString value should not be empty");
            }
            val.string.len = strlen(object->value.string);
            val.string.val = object->value.string;
            val.estSize = sizeof(JEntry) + val.string.len;
            pushJsonbValue(&state, elem_token, &val);
            break;

        case ObjTypeInteger:
            val.type = jbvNumeric;
            val.numeric = (Numeric)
                DatumGetNumeric(DirectFunctionCall1(int8_numeric,
                                                    object->value.integer));
            val.estSize = 2 * sizeof(JEntry) + VARSIZE_ANY(val.numeric);
            pushJsonbValue(&state, elem_token, &val);
            break;

        case ObjTypeFloat:
            val.type = jbvNumeric;
            val.numeric = (Numeric)
                DatumGetNumeric(DirectFunctionCall1(float8_numeric,
                                                    object->value.flt));
            val.estSize = 2 * sizeof(JEntry) + VARSIZE_ANY(val.numeric);
            pushJsonbValue(&state, elem_token, &val);
            break;

        case ObjTypeBool:
            val.type = jbvBool;
            val.boolean = object->value.boolean;
            val.estSize = sizeof(JEntry);
            pushJsonbValue(&state, elem_token, &val);
            break;

        case ObjTypeObject:
            /* Recursively add the object into the existing parse state */
            if (!object->value.object) {
                elog(ERROR, "ObjTypeObject value should not be empty");
            }
            objtree_to_jsonb_rec(object->value.object, state, NULL);
            break;

        case ObjTypeArray: {
                ListCell *cell;
                if (!object->value.array) {
                    elog(ERROR, "ObjTypeArray value should not be empty");
                }
                pushJsonbValue(&state, WJB_BEGIN_ARRAY, NULL);
                foreach(cell, object->value.array) {
                    ObjElem *elem = (ObjElem*)lfirst(cell);

                    objtree_to_jsonb_element(state, elem, WJB_ELEM);
                }
                pushJsonbValue(&state, WJB_END_ARRAY, NULL);
            }
            break;

        default:
            elog(ERROR, "unrecognized object type %d", object->objtype);
            break;
    }
}

/*
 * Recursive helper for objtree_to_jsonb.
 */
static JsonbValue *
objtree_to_jsonb_rec(ObjTree *tree, JsonbParseState *state, char *owner)
{
    slist_iter iter;

    pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);

    role_to_jsonb_element(state, owner);
    objtree_fmt_to_jsonb_element(state, tree);

    slist_foreach(iter, &tree->params) {
        ObjElem *object = slist_container(ObjElem, node, iter.cur);
        JsonbValue key;

        /* Push the key first */
        key.type = jbvString;
        key.string.len = strlen(object->name);
        key.string.val = object->name;
        key.estSize = sizeof(JEntry) + key.string.len;
        pushJsonbValue(&state, WJB_KEY, &key);

        /* Then process the value according to its type */
        objtree_to_jsonb_element(state, object, WJB_VALUE);
    }

    return pushJsonbValue(&state, WJB_END_OBJECT, NULL);
}

/*
 * Subroutine for CREATE TABLE/CREATE DOMAIN deparsing.
 *
 * Given a table OID or domain OID, obtain its constraints and append them to
 * the given elements list.  The updated list is returned.
 *
 * This works for typed tables, regular tables, and domains.
 *
 * Note that CONSTRAINT_FOREIGN constraints are always ignored.
 */
static List *
obtainConstraints(List *elements, Oid relationId)
{
    Relation conRel;
    ScanKeyData skey[1];
    SysScanDesc scan;
    HeapTuple tuple;
    ObjTree *constr;

    /* Only one may be valid */
    Assert(OidIsValid(relationId));

    /*
     * Scan pg_constraint to fetch all constraints linked to the given
     * relation.
     */
    conRel = relation_open(ConstraintRelationId, AccessShareLock);
    ScanKeyInit(&skey[0], Anum_pg_constraint_conrelid, BTEqualStrategyNumber, 
            F_OIDEQ, ObjectIdGetDatum(relationId));
    scan = systable_beginscan(conRel, ConstraintRelidIndexId, true, NULL, 1, skey);

    /*
     * For each constraint, add a node to the list of table elements.  In
     * these nodes we include not only the printable information ("fmt"), but
     * also separate attributes to indicate the type of constraint, for
     * automatic processing.
     */
    while (HeapTupleIsValid(tuple = systable_getnext(scan)))
    {
        Form_pg_constraint constrForm;
        char *contype = NULL;

        constrForm = (Form_pg_constraint) GETSTRUCT(tuple);

        switch (constrForm->contype) {
            case CONSTRAINT_CHECK:
                contype = "check";
                break;
            case CONSTRAINT_FOREIGN:
                continue;       /* not here */
            case CONSTRAINT_PRIMARY:
                contype = "primary key";
                break;
            case CONSTRAINT_UNIQUE:
                contype = "unique";
                break;
            case CONSTRAINT_TRIGGER:
                contype = "trigger";
                break;
            case CONSTRAINT_EXCLUSION:
                contype = "exclusion";
                break;
            default:
                elog(ERROR, "unrecognized constraint type %c", constrForm->contype);
        }

        /*
         * "type" and "contype" are not part of the printable output, but are
         * useful to programmatically distinguish these from columns and among
         * different constraint types.
         *
         * XXX it might be useful to also list the column names in a PK, etc.
         */
        constr = new_objtree_VA("CONSTRAINT %{name}I %{definition}s", 4,
                                "type", ObjTypeString, "constraint",
                                "contype", ObjTypeString, contype,
                                "name", ObjTypeString, NameStr(constrForm->conname),
                                "definition", ObjTypeString,
                                pg_get_constraintdef_part_string(HeapTupleGetOid(tuple)));

        if (constrForm->conindid &&
            (constrForm->contype == CONSTRAINT_PRIMARY ||
             constrForm->contype == CONSTRAINT_UNIQUE ||
             constrForm->contype == CONSTRAINT_EXCLUSION)) {
            Oid tblspc = get_rel_tablespace(constrForm->conindid);

            if (OidIsValid(tblspc)) {
                char *tblspcname = get_tablespace_name(tblspc);

                if (!tblspcname) {
                    elog(ERROR, "cache lookup failed for tablespace %u", tblspc);
                }

                append_string_object(constr, "USING INDEX TABLESPACE %{tblspc}s", "tblspc", tblspcname);
            }
        }

        elements = lappend(elements, new_object_object(constr));
    }

    systable_endscan(scan);
    relation_close(conRel, AccessShareLock);

    return elements;
}

/*
 * Return an index definition, split into several pieces.
 *
 * A large amount of code is duplicated from  pg_get_indexdef_worker, but
 * control flow is different enough that it doesn't seem worth keeping them
 * together.
 */
static void
pg_get_indexdef_detailed(Oid indexrelid, bool global,
                         char **index_am,
                         char **definition,
                         char **reloptions,
                         char **tablespace,
                         char **whereClause,
                         bool *invisible)
{
    HeapTuple ht_idx;
    HeapTuple ht_idxrel;
    HeapTuple ht_am;
    Form_pg_index idxrec;
    Form_pg_class idxrelrec;
    Form_pg_am amrec;
    List *indexprs;
    ListCell *indexpr_item;
    List *context;
    Oid indrelid;
    int keyno;
    Datum indcollDatum;
    Datum indclassDatum;
    Datum indoptionDatum;
    bool isnull;
    oidvector *indcollation;
    oidvector *indclass;
    int2vector *indoption;
    StringInfoData definitionBuf;
    int indnkeyatts;

    *tablespace = NULL;
    *whereClause = NULL;

    /* Fetch the pg_index tuple by the Oid of the index */
    ht_idx = SearchSysCache1(INDEXRELID, ObjectIdGetDatum(indexrelid));
    if (!HeapTupleIsValid(ht_idx))
        elog(ERROR, "cache lookup failed for index with OID %u", indexrelid);
    idxrec = (Form_pg_index) GETSTRUCT(ht_idx);

    indrelid = idxrec->indrelid;
    Assert(indexrelid == idxrec->indexrelid);
    indnkeyatts = GetIndexKeyAttsByTuple(NULL, ht_idx);

    /* Must get indcollation, indclass, and indoption the hard way */
    indcollDatum = SysCacheGetAttr(INDEXRELID, ht_idx,
                                   Anum_pg_index_indcollation, &isnull);
    Assert(!isnull);
    indcollation = (oidvector *) DatumGetPointer(indcollDatum);

    indclassDatum = SysCacheGetAttr(INDEXRELID, ht_idx,
                                    Anum_pg_index_indclass, &isnull);
    Assert(!isnull);
    indclass = (oidvector *) DatumGetPointer(indclassDatum);

    indoptionDatum = SysCacheGetAttr(INDEXRELID, ht_idx,
                                     Anum_pg_index_indoption, &isnull);
    Assert(!isnull);
    indoption = (int2vector *) DatumGetPointer(indoptionDatum);

    /* Fetch the pg_class tuple of the index relation */
    ht_idxrel = SearchSysCache1(RELOID, ObjectIdGetDatum(indexrelid));
    if (!HeapTupleIsValid(ht_idxrel))
        elog(ERROR, "cache lookup failed for relation with OID %u", indexrelid);
    idxrelrec = (Form_pg_class) GETSTRUCT(ht_idxrel);

    /* Fetch the pg_am tuple of the index' access method */
    ht_am = SearchSysCache1(AMOID, ObjectIdGetDatum(idxrelrec->relam));
    if (!HeapTupleIsValid(ht_am))
        elog(ERROR, "cache lookup failed for access method with OID %u",
             idxrelrec->relam);
    amrec = (Form_pg_am) GETSTRUCT(ht_am);

    /*
     * Get the index expressions, if any.  (NOTE: we do not use the relcache
     * versions of the expressions and predicate, because we want to display
     * non-const-folded expressions.)
     */
    if (!heap_attisnull(ht_idx, Anum_pg_index_indexprs, NULL)) {
        Datum exprsDatum;
        char *exprsString;

        exprsDatum = SysCacheGetAttr(INDEXRELID, ht_idx,
                                     Anum_pg_index_indexprs, &isnull);
        Assert(!isnull);
        exprsString = TextDatumGetCString(exprsDatum);
        indexprs = (List *) stringToNode(exprsString);
        pfree(exprsString);
    }
    else {
        indexprs = NIL;
    }

    indexpr_item = list_head(indexprs);

    context = deparse_context_for(get_rel_name(indrelid), indrelid);

    initStringInfo(&definitionBuf);

    /* Output index AM */
    *index_am = pstrdup(quote_identifier(NameStr(amrec->amname)));

    /*
     * Output index definition.  Note the outer parens must be supplied by
     * caller.
     */
    bool add_global = false;
    appendStringInfoString(&definitionBuf, "(");
    for (keyno = 0; keyno < idxrec->indnatts; keyno++) {
        AttrNumber attnum = idxrec->indkey.values[keyno];
        int16 opt = indoption->values[keyno];
        Oid keycoltype;
        Oid keycolcollation;

        /* Print INCLUDE to divide key and non-key attrs. */
        if (keyno == indnkeyatts) {
            if (global) {
                appendStringInfoString(&definitionBuf, ") GLOBAL ");
                add_global = true;
            } else {
                appendStringInfoString(&definitionBuf, ") ");
            }
            appendStringInfoString(&definitionBuf, "INCLUDE (");
        }
        else {
            appendStringInfoString(&definitionBuf, keyno == 0 ? "" : ", ");
        }

        if (attnum != 0) {
            /* Simple index column */
            char *attname;
            int32 keycoltypmod;

            attname = get_attname(indrelid, attnum);
            appendStringInfoString(&definitionBuf, quote_identifier(attname));
            get_atttypetypmodcoll(indrelid, attnum,
                                  &keycoltype, &keycoltypmod,
                                  &keycolcollation);
        } else {
            /* Expressional index */
            Node *indexkey;
            char *str;

            if (indexpr_item == NULL)
                elog(ERROR, "too few entries in indexprs list");
            indexkey = (Node *) lfirst(indexpr_item);
            indexpr_item = lnext(indexpr_item);

            /* Deparse */
            str = deparse_expression(indexkey, context, false, false);

            /* Need parens if it's not a bare function call */
            if (indexkey && IsA(indexkey, FuncExpr) &&
                ((FuncExpr *) indexkey)->funcformat == COERCE_EXPLICIT_CALL) {
                appendStringInfoString(&definitionBuf, str);
            } else {
                appendStringInfo(&definitionBuf, "(%s)", str);
            }

            keycoltype = exprType(indexkey);
            keycolcollation = exprCollation(indexkey);
        }

        /* Print additional decoration for (selected) key columns, even if default */
        if (keyno < indnkeyatts) {
            Oid indcoll = indcollation->values[keyno];
            if (OidIsValid(indcoll))
                appendStringInfo(&definitionBuf, " COLLATE %s",
                                generate_collation_name((indcoll)));

            /* Add the operator class name, even if default */
            get_opclass_name(indclass->values[keyno], InvalidOid, &definitionBuf);

            /* Add options if relevant */
            if (amrec->amcanorder) {
                /* If it supports sort ordering, report DESC and NULLS opts */
                if (opt & INDOPTION_DESC) {
                    appendStringInfoString(&definitionBuf, " DESC");
                    /* NULLS FIRST is the default in this case */
                    if (!(opt & INDOPTION_NULLS_FIRST))
                        appendStringInfoString(&definitionBuf, " NULLS LAST");
                } else {
                    if (opt & INDOPTION_NULLS_FIRST)
                        appendStringInfoString(&definitionBuf, " NULLS FIRST");
                }
            }

            /* XXX excludeOps thingy was here; do we need anything? */
        }
    }

    appendStringInfoString(&definitionBuf, ")");
    if (!add_global && global) {
        appendStringInfoString(&definitionBuf, "GLOBAL");
    }

    if (idxrelrec->parttype == PARTTYPE_PARTITIONED_RELATION &&
        idxrelrec->relkind != RELKIND_GLOBAL_INDEX) {
        pg_get_indexdef_partitions(indexrelid, idxrec, true, &definitionBuf, true, true, true);
    }

    *definition = definitionBuf.data;

    /* Output reloptions */
    *reloptions = flatten_reloptions(indexrelid);

    /* Output tablespace */
    {
        Oid         tblspc;

        tblspc = get_rel_tablespace(indexrelid);
        if (OidIsValid(tblspc))
            *tablespace = pstrdup(quote_identifier(get_tablespace_name(tblspc)));
    }

    *invisible = false;
    if (!GetIndexVisibleStateByTuple(ht_idx)) {
        *invisible = true;
    }

    /* Report index predicate, if any */
    if (!heap_attisnull(ht_idx, Anum_pg_index_indpred, NULL))
    {
        Node *node;
        Datum predDatum;
        char *predString;

        /* Convert text string to node tree */
        predDatum = SysCacheGetAttr(INDEXRELID, ht_idx,
                                    Anum_pg_index_indpred, &isnull);
        Assert(!isnull);
        predString = TextDatumGetCString(predDatum);
        node = (Node *) stringToNode(predString);
        pfree(predString);

        /* Deparse */
        *whereClause = deparse_expression(node, context, false, false);
    }

    /* Clean up */
    ReleaseSysCache(ht_idx);
    ReleaseSysCache(ht_idxrel);
    ReleaseSysCache(ht_am);
}

/*
 * Obtain the deparsed default value for the given column of the given table.
 *
 * Caller must have set a correct deparse context.
 */
static char *
RelationGetColumnDefault(Relation rel, AttrNumber attno, List *dpcontext,
                         List **exprs)
{
    Node       *defval;
    char       *defstr;

    defval = build_column_default(rel, attno);
    defstr = deparse_expression(defval, dpcontext, false, false);

    /* Collect the expression for later replication safety checks */
    if (exprs)
        *exprs = lappend(*exprs, defval);

    return defstr;
}

static char *RelationGetColumnOnUpdate(Node *update_expr, List *dpcontext, List **exprs)
{
    StringInfoData buf;

    initStringInfo(&buf);

    if (IsA(update_expr, FuncCall)) {
        FuncCall *n = (FuncCall*)update_expr;
        Value *funcname = (Value*)(lsecond(n->funcname));
        if (!pg_strcasecmp(strVal(funcname), "text_date")) {
            appendStringInfo(&buf, "CURRENT_DATE");
        } else if (!pg_strcasecmp(strVal(funcname), "pg_systimestamp")) {
            appendStringInfo(&buf, "CURRENT_TIMESTAMP");
        } else if (!pg_strcasecmp(strVal(funcname), "sysdate")) {
            appendStringInfo(&buf, "SYSDATE");
        } else if (u_sess->attr.attr_sql.dolphin) {
            appendStringInfo(&buf, "%s", n->colname);
        }
    } else if (IsA(update_expr, TypeCast)) {
        TypeCast *n = (TypeCast*)update_expr;
        TypeName *t = n->typname;
        Value *typname = (Value*)(lsecond(t->names));
        if (!pg_strcasecmp(strVal(typname), "timetz")) {
            appendStringInfo(&buf, "CURRENT_TIME");
        } else if (!pg_strcasecmp(strVal(typname), "timestamptz")) {
            appendStringInfo(&buf, "CURRENT_TIMESTAMP");
        } else if (!pg_strcasecmp(strVal(typname), "time")) {
            appendStringInfo(&buf, "LOCALTIME");
        } else if (!pg_strcasecmp(strVal(typname), "timestamp")) {
            appendStringInfo(&buf, "LOCALTIMESTAMP");
        }

        if (t->typmods) {
            A_Const* typmod = (A_Const*)(linitial(t->typmods));
            appendStringInfo(&buf, "(%ld)", intVal(&typmod->val));
        }
    } else if (IsA(update_expr, A_Expr)) {
        A_Expr *a = (A_Expr*)update_expr;
        Value *n = (Value*)linitial(a->name);
        if (a->kind == AEXPR_OP && (!pg_strcasecmp(strVal(n), "+") ||
        !pg_strcasecmp(strVal(n), "-"))) {
            if (a->rexpr && IsA(a->rexpr, A_Const)) {
                char *larg = RelationGetColumnOnUpdate(a->lexpr, dpcontext, exprs);
                A_Const *con = (A_Const*)a->rexpr;
                Value *v = &con->val;
                if (nodeTag(v) == T_Integer) {
                    appendStringInfo(&buf, "%s %s %ld", larg, strVal(n), intVal(v));
                } else if (nodeTag(v) == T_String) {
                    appendStringInfo(&buf, "%s %s '%s'", larg, strVal(n), strVal(v));
                }
            }
        }
    }

    return buf.data;
}

/*
 * Deparse a ColumnDef node within a regular (non-typed) table creation.
 *
 * NOT NULL constraints in the column definition are emitted directly in the
 * column definition by this routine; other constraints must be emitted
 * elsewhere (the info in the parse node is incomplete anyway).
 *
 * Verbose syntax
 * %{name}I %{coltype}T %{compression}s %{default}s %{not_null}s %{collation}s
 */
static ObjTree *
deparse_ColumnDef(Relation relation, List *dpcontext,
                  ColumnDef *coldef, List **exprs)
{
    ObjTree *ret;
    Oid relid = RelationGetRelid(relation);
    HeapTuple attrTup;
    Form_pg_attribute attrForm;
    Oid typid;
    int32 typmod;
    Oid typcollation;
    bool saw_notnull;
    bool saw_autoincrement;
    char *onupdate = NULL;
    ListCell *cell;

    /*
     * Inherited columns without local definitions must not be emitted.
     *
     * XXX maybe it is useful to have them with "present = false" or some
     * such?
     */
    if (!coldef->is_local)
        return NULL;

    attrTup = SearchSysCacheAttName(relid, coldef->colname);
    if (!HeapTupleIsValid(attrTup))
        elog(ERROR, "could not find cache entry for column \"%s\" of relation %u",
             coldef->colname, relid);
    attrForm = (Form_pg_attribute) GETSTRUCT(attrTup);

    get_atttypetypmodcoll(relid, attrForm->attnum,
                          &typid, &typmod, &typcollation);

    ret = new_objtree_VA("%{name}I %{coltype}T", 3,
                         "type", ObjTypeString, "column",
                         "name", ObjTypeString, coldef->colname,
                         "coltype", ObjTypeObject,
                         new_objtree_for_type(typid, typmod));


    
    if (OidIsValid(typcollation)) {
        append_object_object(ret, "COLLATE %{collate}D",
                             new_objtree_for_qualname_id(CollationRelationId,
                                                         typcollation));
    }
    
    /*
        * Emit a NOT NULL declaration if necessary.  Note that we cannot
        * trust pg_attribute.attnotnull here, because that bit is also set
        * when primary keys are specified; we must not emit a NOT NULL
        * constraint in that case, unless explicitly specified.  Therefore,
        * we scan the list of constraints attached to this column to
        * determine whether we need to emit anything. (Fortunately, NOT NULL
        * constraints cannot be table constraints.)
        *
        * In the ALTER TABLE cases, we also add a NOT NULL if the colDef is
        * marked is_not_null.
        */
    saw_notnull = false;
    saw_autoincrement = false;

    foreach(cell, coldef->constraints) {
        Constraint *constr = (Constraint *) lfirst(cell);

        if (constr->contype == CONSTR_NOTNULL) {
            saw_notnull = true;
        } else if (constr->contype == CONSTR_AUTO_INCREMENT) {
            saw_autoincrement = true;
        } else if (constr->contype == CONSTR_DEFAULT && constr->update_expr) {
            onupdate = RelationGetColumnOnUpdate(constr->update_expr, dpcontext, exprs);
        }
    }

    append_string_object(ret, "%{auto_increment}s", "auto_increment", 
                    saw_autoincrement ? "AUTO_INCREMENT" : "");

    

    append_string_object(ret, "%{not_null}s", "not_null",
                            saw_notnull ? "NOT NULL" : "");

    /* GENERATED COLUMN EXPRESSION */
    if (coldef->generatedCol == ATTRIBUTE_GENERATED_STORED) {
        char *defstr = RelationGetColumnDefault(relation, attrForm->attnum, dpcontext, exprs);
        append_string_object(ret, "GENERATED ALWAYS AS (%{generation_expr}s) STORED", 
                        "generation_expr", defstr);
    }

    if (attrForm->atthasdef &&
        coldef->generatedCol != ATTRIBUTE_GENERATED_STORED &&
        !saw_autoincrement) {
        char *defstr;

        defstr = RelationGetColumnDefault(relation, attrForm->attnum,
                                            dpcontext, exprs);

        append_string_object(ret, "DEFAULT %{default}s", "default", defstr);
    }

    append_string_object(ret, "ON UPDATE %{on_update}s", "on_update", onupdate ? onupdate : "");
        
    ReleaseSysCache(attrTup);

    return ret;
}


/*
 * Deparse DefElems
 *
 * Verbose syntax
 * %{label}s = %{value}L
 */
static ObjTree *
deparse_DefElem(DefElem *elem, bool is_reset)
{
    ObjTree *ret;
    ObjTree *optname = new_objtree("");

    if (elem->defnamespace != NULL)
        append_string_object(optname, "%{schema}I.", "schema",
                             elem->defnamespace);

    append_string_object(optname, "%{label}I", "label", elem->defname);

    ret = new_objtree_VA("%{label}s", 1,
                         "label", ObjTypeObject, optname);

    if (!is_reset) {
        if (!elem->arg) {
            append_string_object(ret, "= %{value}s", "value", defGetBoolean(elem) ? "TRUE" : "FALSE");
        } else {
            if (nodeTag(elem->arg) == T_String) {
                append_string_object(ret, "= %{value}L", "value", defGetString(elem));
            } else {
                append_string_object(ret, "= %{value}s", "value", defGetString(elem));
            }
        }
    }
    return ret;
}

/*
 * Deparse the ON COMMIT ... clause for CREATE ... TEMPORARY ...
 *
 * Verbose syntax
 * ON COMMIT %{on_commit_value}s
 */
static ObjTree *
deparse_OnCommitClause(OnCommitAction option)
{
    ObjTree *ret  = new_objtree("ON COMMIT");
    switch (option) {
        case ONCOMMIT_DROP:
            append_string_object(ret, "%{on_commit_value}s",
                                 "on_commit_value", "DROP");
            break;

        case ONCOMMIT_DELETE_ROWS:
            append_string_object(ret, "%{on_commit_value}s",
                                 "on_commit_value", "DELETE ROWS");
            break;

        case ONCOMMIT_PRESERVE_ROWS:
            append_string_object(ret, "%{on_commit_value}s",
                                 "on_commit_value", "PRESERVE ROWS");
            break;

        case ONCOMMIT_NOOP:
            ret = NULL;
            break;
    }

    return ret;
}

/*
 * Deparse the sequence CACHE option.
 *
 * Verbose syntax
 * SET CACHE %{value}s
 * OR
 * CACHE %{value}
 */
static inline ObjElem *
deparse_Seq_Cache(sequence_values * seqdata, bool alter_table)
{
    ObjTree     *ret;
    const char  *fmt;

    fmt = alter_table ? "SET CACHE %{value}s" : "CACHE %{value}s";
    
    ret = new_objtree_VA(fmt, 2,
                         "clause", ObjTypeString, "cache",
                         "value", ObjTypeString, seqdata->cache_value);

    return new_object_object(ret);
}

/*
 * Deparse the sequence CYCLE option.
 *
 * Verbose syntax
 * SET %{no}s CYCLE
 * OR
 * %{no}s CYCLE
 */
static inline ObjElem *
deparse_Seq_Cycle(sequence_values * seqdata, bool alter_table)
{
    ObjTree     *ret;
    const char  *fmt;

    fmt = alter_table ? "SET %{no}s CYCLE" : "%{no}s CYCLE";
    
    ret = new_objtree_VA(fmt, 2,
                         "clause", ObjTypeString, "cycle",
                         "no", ObjTypeString,
                         seqdata->is_cycled ? "" : "NO");

    return new_object_object(ret);
}

/*
 * Deparse the sequence INCREMENT BY option.
 *
 * Verbose syntax
 * SET INCREMENT BY %{value}s
 * OR
 * INCREMENT BY %{value}s
 */
static inline ObjElem *
deparse_Seq_IncrementBy(sequence_values * seqdata, bool alter_table)
{
    ObjTree     *ret;
    const char  *fmt;

    fmt = alter_table ? "SET INCREMENT BY %{value}s" : "INCREMENT BY %{value}s";    

    ret = new_objtree_VA(fmt, 2,
                         "clause", ObjTypeString, "seqincrement",
                         "value", ObjTypeString, seqdata->increment_by);

    return new_object_object(ret);
}

/*
 * Deparse the sequence MAXVALUE option.
 *
 * Verbose syntax
 * SET MAXVALUE %{value}s
 * OR
 * MAXVALUE %{value}s
 */
static inline ObjElem *
deparse_Seq_Maxvalue(sequence_values * seqdata, bool alter_table)
{
    ObjTree     *ret;
    const char  *fmt;

    fmt = alter_table ? "SET MAXVALUE %{value}s" : "MAXVALUE %{value}s";

    ret = new_objtree_VA(fmt, 2,
                         "clause", ObjTypeString, "maxvalue",
                         "value", ObjTypeString, seqdata->max_value);

    return new_object_object(ret);
}

/*
 * Deparse the sequence MINVALUE option.
 *
 * Verbose syntax
 * SET MINVALUE %{value}s
 * OR
 * MINVALUE %{value}s
 */
static inline ObjElem *
deparse_Seq_Minvalue(sequence_values * seqdata, bool alter_table)
{
    ObjTree     *ret;
    const char  *fmt;

    fmt = alter_table ? "SET MINVALUE %{value}s" : "MINVALUE %{value}s";  

    ret = new_objtree_VA(fmt, 2,
                         "clause", ObjTypeString, "minvalue",
                         "value", ObjTypeString, seqdata->min_value);

    return new_object_object(ret);
}

/*
 * Deparse the sequence OWNED BY command.
 * 
 * Verbose syntax
 * OWNED BY %{owner}D
 */
static ObjElem *
deparse_Seq_OwnedBy(Oid sequenceId)
{
    ObjTree     *ret = NULL;
    Relation    depRel;
    SysScanDesc scan;
    ScanKeyData keys[3];
    HeapTuple   tuple;

    depRel = relation_open(DependRelationId, AccessShareLock);
    ScanKeyInit(&keys[0],
                Anum_pg_depend_classid,
                BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(RelationRelationId));
    ScanKeyInit(&keys[1],
                Anum_pg_depend_objid,
                BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(sequenceId));
    ScanKeyInit(&keys[2],
                Anum_pg_depend_objsubid,
                BTEqualStrategyNumber, F_INT4EQ,
                Int32GetDatum(0));

    scan = systable_beginscan(depRel, DependDependerIndexId, true, 
                                NULL, 3, keys);

    while (HeapTupleIsValid(tuple = systable_getnext(scan))) {
        Oid             ownerId;
        Form_pg_depend  depform;
        ObjTree         *tmp_obj;
        char            *colname;
        
        depform = (Form_pg_depend) GETSTRUCT(tuple);

        /* Only consider AUTO dependencies on pg_class */
        if (depform->deptype != DEPENDENCY_AUTO)
            continue;
        if (depform->refclassid != RelationRelationId)
            continue;
        if(depform->refobjsubid <= 0)
            continue;

        ownerId = depform->refobjid;
        colname = get_attname(ownerId, depform->refobjsubid);
        if (colname == NULL)
            continue;

        tmp_obj = new_objtree_for_qualname_id(RelationRelationId, ownerId);
        append_string_object(tmp_obj, "attrname", "attrname", colname);
        ret = new_objtree_VA("OWNED BY %{owner}D", 2, 
                            "clause", ObjTypeString, "owned",
                            "owner", ObjTypeObject, tmp_obj);
    }
    
    systable_endscan(scan);
    relation_close(depRel, AccessShareLock);

    /*
     * If there's no owner column, emit an empty OWNED BY element, set up so
     * that it won't print anything.
     */
    if (!ret)
        /* XXX this shouldn't happen  */
        ret = new_objtree_VA("OWNED BY %{owner}D", 3,
                            "clause", ObjTypeString, "owned",
                            "owner", ObjTypeNull,
                            "present", ObjTypeBool, false);

    return new_object_object(ret); 
}

/*
 * Deparse the sequence ORDER option.
 */
static inline ObjElem *
deparse_Seq_Order(DefElem *elem)
{
    ObjTree *ret;
    
    ret = new_objtree_VA("%{order}s", 2,
                         "clause", ObjTypeString, "order",
                         "order", ObjTypeString, defGetBoolean(elem) ? "ORDER" : "NOORDER");

    return new_object_object(ret);
}


/*
 * Deparse the sequence RESTART option.
 *
 * Verbose syntax
 * RESTART %{value}s
 */
static inline ObjElem *
deparse_Seq_Restart(char *last_value)
{
    ObjTree *ret;
    ret = new_objtree_VA("RESTART %{value}s", 2,
                         "clause", ObjTypeString, "restart",
                         "value", ObjTypeString, last_value);

    return new_object_object(ret);
}

/*
 * Deparse the sequence AS option.
 *
 * Verbose syntax
 * AS %{identity}D
 */
static inline ObjElem *
deparse_Seq_As(DefElem *elem)
{
    ObjTree *ret;
    Type likeType;
    Form_pg_type likeForm;

    likeType = typenameType(NULL, defGetTypeName(elem), NULL);
    likeForm = (Form_pg_type) GETSTRUCT(likeType);

    ret = new_objtree_VA("AS %{identity}D", 1,
                         "identity", ObjTypeObject,
                         new_objtree_for_qualname(likeForm->typnamespace,
                                                  NameStr(likeForm->typname)));
    ReleaseSysCache(likeType);

    return new_object_object(ret);
}

/*
 * Deparse the sequence START WITH option.
 *
 * Verbose syntax
 * SET START WITH %{value}s
 * OR
 * START WITH %{value}s
 */
static inline ObjElem *
deparse_Seq_Startwith(sequence_values *seqdata, bool alter_table)
{
    ObjTree *ret;
    const char  *fmt;

    fmt = alter_table ? "SET START WITH %{value}s" : "START WITH %{value}s";  

    ret = new_objtree_VA(fmt, 2,
                         "clause", ObjTypeString, "start",
                         "value", ObjTypeString, seqdata->start_value);

    return new_object_object(ret);
}

/*
 * Subroutine for CREATE TABLE deparsing.
 *
 * Deal with all the table elements (columns and constraints).
 *
 * Note we ignore constraints in the parse node here; they are extracted from
 * system catalogs instead.
 */
static List *
deparse_TableElements(Relation relation, List *tableElements, List *dpcontext)
{
    List *elements = NIL;
    ListCell *lc;

    foreach(lc, tableElements) {
        Node *elt = (Node *) lfirst(lc);

        switch (nodeTag(elt)) {
            case T_ColumnDef: {
                    ObjTree    *tree;
                    tree = deparse_ColumnDef(relation, dpcontext, (ColumnDef *) elt, NULL);
                    if (tree != NULL)
                        elements = lappend(elements, new_object_object(tree));
                }
                break;
            case T_Constraint:
                break;
            default:
                elog(ERROR, "invalid node type %d", nodeTag(elt));
        }
    }

    return elements;
}

/*
 * Deparse a CreateSeqStmt.
 *
 * Given a sequence OID and the parse tree that created it, return an ObjTree
 * representing the creation command.
 *
 * Verbose syntax
 * CREATE %{persistence}s SEQUENCE %{identity}D
 */
static ObjTree *
deparse_CreateSeqStmt(Oid objectId, Node *parsetree)
{
    ObjTree *ret;
    Relation relation;
    List *elems = NIL;
    sequence_values *seqvalues;
    CreateSeqStmt *createSeqStmt = (CreateSeqStmt *) parsetree;

    if (createSeqStmt->is_autoinc)
        return NULL;

    seqvalues = get_sequence_values(objectId);

    /* Definition elements */
    elems = lappend(elems, deparse_Seq_Cache(seqvalues, false));
    elems = lappend(elems, deparse_Seq_Cycle(seqvalues, false));
    elems = lappend(elems, deparse_Seq_IncrementBy(seqvalues, false));
    elems = lappend(elems, deparse_Seq_Minvalue(seqvalues, false));
    elems = lappend(elems, deparse_Seq_Maxvalue(seqvalues, false));
    elems = lappend(elems, deparse_Seq_Startwith(seqvalues, false));
    elems = lappend(elems, deparse_Seq_Restart(seqvalues->last_value));

    /* We purposefully do not emit OWNED BY here */

    relation = relation_open(objectId, AccessShareLock);

    ret = new_objtree_VA("CREATE %{persistence}s %{large}s SEQUENCE %{identity}D %{definition: }s", 4,
                         "persistence", ObjTypeString, get_persistence_str(relation->rd_rel->relpersistence),
                         "large", ObjTypeString, seqvalues->large ? "LARGE" : "",
                         "identity", ObjTypeObject, new_objtree_for_qualname(relation->rd_rel->relnamespace,
                                                                    RelationGetRelationName(relation)),
                         "definition", ObjTypeArray, elems);

    relation_close(relation, AccessShareLock);

    return ret;
}

/*
 * Deparse an AlterSeqStmt
 * Given a sequence OID and a parse tree that modified it, return an ObjTree
 * representing the alter command
 *
 * Verbose syntax
 * ALTER SEQUENCE %{identity}D %{definition: }s
 */
static ObjTree *
deparse_AlterSeqStmt(Oid objectId, Node *parsetree)
{
    ObjTree         *ret;
    Relation        relation;
    List            *elems = NIL;
    ListCell        *cell;
    sequence_values *seqvalues;
    AlterSeqStmt    *alterSeqStmt = (AlterSeqStmt *) parsetree;

    if (alterSeqStmt->is_autoinc) {
        return NULL;
    }

    if (!alterSeqStmt->options) {
        return NULL;
    }

    seqvalues = get_sequence_values(objectId);

    foreach(cell, alterSeqStmt->options) {
        DefElem *elem = (DefElem *) lfirst(cell);
        ObjElem *newelm = NULL;

        if (strcmp(elem->defname, "cache") == 0)
            newelm = deparse_Seq_Cache(seqvalues, false);
        else if (strcmp(elem->defname, "cycle") == 0)
            newelm = deparse_Seq_Cycle(seqvalues, false);
        else if (strcmp(elem->defname, "increment") == 0)
            newelm = deparse_Seq_IncrementBy(seqvalues, false);
        else if (strcmp(elem->defname, "minvalue") == 0)
            newelm = deparse_Seq_Minvalue(seqvalues, false);
        else if (strcmp(elem->defname, "maxvalue") == 0)
            newelm = deparse_Seq_Maxvalue(seqvalues, false);
        else if (strcmp(elem->defname, "start") == 0)
            newelm = deparse_Seq_Startwith(seqvalues, false);
        else if (strcmp(elem->defname, "restart") == 0)
            newelm = deparse_Seq_Restart(seqvalues->last_value);
        else if (strcmp(elem->defname, "owned_by") == 0)
            newelm = deparse_Seq_OwnedBy(objectId);
        else if (strcmp(elem->defname, "as") == 0)
            newelm = deparse_Seq_As(elem);
        else if (strcmp(elem->defname, "order") == 0)
            newelm = deparse_Seq_Order(elem);
        else
            elog(WARNING, "unsupport sequence option %s for replication", elem->defname);

        elems = lappend(elems, newelm);
    }

    relation = relation_open(objectId, AccessShareLock);

    ret = new_objtree_VA("ALTER %{large}s SEQUENCE %{identity}D %{definition: }s", 3,
                        "large", ObjTypeString, seqvalues->large ? "LARGE" : "",
                        "identity", ObjTypeObject,
                        new_objtree_for_qualname(relation->rd_rel->relnamespace,
                                                RelationGetRelationName(relation)),
                        "definition", ObjTypeArray, elems);

    relation_close(relation, AccessShareLock);

    return ret;    
}

/*
 * Deparse a CommentStmt when it pertains to a constraint.
 *
 * Verbose syntax
 * COMMENT ON CONSTRAINT %{identity}s ON [DOMAIN] %{parentobj}s IS %{comment}s
 */
static ObjTree *
deparse_CommentOnConstraintSmt(Oid objectId, Node *parsetree)
{
    CommentStmt *node = (CommentStmt *) parsetree;
    ObjTree     *ret;
    HeapTuple   constrTup;
    Form_pg_constraint constrForm;
    ObjectAddress addr;

    Assert(node->objtype == OBJECT_TABCONSTRAINT || node->objtype == OBJECT_DOMCONSTRAINT);

    constrTup = SearchSysCache1(CONSTROID, objectId);
    if (!HeapTupleIsValid(constrTup))
        elog(ERROR, "cache lookup failed for constraint with OID %u", objectId);
    constrForm = (Form_pg_constraint) GETSTRUCT(constrTup);

    if (OidIsValid(constrForm->conrelid))
        ObjectAddressSet(addr, RelationRelationId, constrForm->conrelid);
    else
        ObjectAddressSet(addr, TypeRelationId, constrForm->contypid);

    ret = new_objtree_VA("COMMENT ON CONSTRAINT %{identity}s ON %{domain}s %{parentobj}s", 3,
                        "identity", ObjTypeString, pstrdup(NameStr(constrForm->conname)),
                        "domain", ObjTypeString,
                        (node->objtype == OBJECT_DOMCONSTRAINT) ? "DOMAIN" : "",
                        "parentobj", ObjTypeString,
                        getObjectIdentity(&addr));

    /* Add the comment clause */
    append_literal_or_null(ret, "IS %{comment}s", node->comment);

    ReleaseSysCache(constrTup);
    return ret;
}

/*
 * Deparse an CommentStmt (COMMENT ON ...).
 *
 * Given the object address and the parse tree that created it, return an
 * ObjTree representing the comment command.
 *
 * Verbose syntax
 * COMMENT ON %{objtype}s %{identity}s IS %{comment}s
 */
static ObjTree *
deparse_CommentStmt(ObjectAddress address, Node *parsetree)
{
    CommentStmt *node = (CommentStmt *) parsetree;
    ObjTree     *ret;
    char        *identity;

    /* Comment on subscription is not supported */
    if (node->objtype == OBJECT_SUBSCRIPTION)
        return NULL;

    /*
     * Constraints are sufficiently different that it is easier to handle them
     * separately.
     */
    if (node->objtype == OBJECT_DOMCONSTRAINT ||
        node->objtype == OBJECT_TABCONSTRAINT) {
        Assert(address.classId == ConstraintRelationId);
        return deparse_CommentOnConstraintSmt(address.objectId, parsetree);
    }

    ret = new_objtree_VA("COMMENT ON %{objtype}s", 1,
                        "objtype", ObjTypeString,
                        (char *) string_objtype(node->objtype, false));

    /*
     * Add the object identity clause.  For zero argument aggregates we need
     * to add the (*) bit; in all other cases we can just use
     * getObjectIdentity.
     *
     * XXX shouldn't we instead fix the object identities for zero-argument
     * aggregates?
     */
    if (node->objtype == OBJECT_AGGREGATE) {
        HeapTuple       procTup;
        Form_pg_proc    procForm;

        procTup = SearchSysCache1(PROCOID, ObjectIdGetDatum(address.objectId));
        if (!HeapTupleIsValid(procTup))
            elog(ERROR, "cache lookup failed for procedure with OID %u",
                address.objectId);
        procForm = (Form_pg_proc) GETSTRUCT(procTup);
        if (procForm->pronargs == 0)
            identity = psprintf("%s(*)",
                                quote_qualified_identifier(get_namespace_name(procForm->pronamespace),
                                                            NameStr(procForm->proname)));
        else
            identity = getObjectIdentity(&address);
        ReleaseSysCache(procTup);
    } else {
        identity = getObjectIdentity(&address);
    }

    append_string_object(ret, "%{identity}s", "identity", identity);

    /* Add the comment clause; can be either NULL or a quoted literal. */
    append_literal_or_null(ret, "IS %{comment}s", node->comment);

    return ret;
}

/*
 * Deparse an IndexStmt.
 *
 * Given an index OID and the parse tree that created it, return an ObjTree
 * representing the creation command.
 *
 * If the index corresponds to a constraint, NULL is returned.
 *
 * Verbose syntax
 * CREATE %{unique}s INDEX %{concurrently}s %{if_not_exists}s %{name}I ON
 * %{table}D USING %{index_am}s %{definition}s %{with}s %{tablespace}s
 * %{where_clause}s
 */
static ObjTree *
deparse_IndexStmt(Oid objectId, Node *parsetree)
{
    IndexStmt *node = (IndexStmt *) parsetree;
    ObjTree *ret;
    Relation idxrel;
    Relation heaprel;
    char *index_am;
    char *definition;
    char *reloptions;
    char *tablespace;
    char *whereClause;
    bool invisible;

    if (node->primary || node->isconstraint)
    {
        /*
         * Indexes for PRIMARY KEY and other constraints are output
         * separately; return empty here.
         */
        return NULL;
    }

    idxrel = relation_open(objectId, AccessShareLock);
    heaprel = relation_open(idxrel->rd_index->indrelid, AccessShareLock);

    pg_get_indexdef_detailed(objectId, node->isGlobal,
                             &index_am, &definition, &reloptions,
                             &tablespace, &whereClause, &invisible);

    ret = new_objtree_VA("CREATE %{unique}s INDEX %{concurrently}s %{name}I ON %{table}D USING %{index_am}s %{definition}s", 6,
                         "unique", ObjTypeString, node->unique ? "UNIQUE" : "",
                         "concurrently", ObjTypeString, node->concurrent ? "CONCURRENTLY" : "",
                         "name", ObjTypeString, RelationGetRelationName(idxrel),
                         "table", ObjTypeObject, 
                                new_objtree_for_qualname(heaprel->rd_rel->relnamespace, RelationGetRelationName(heaprel)),
                         "index_am", ObjTypeString, index_am,
                         "definition", ObjTypeString, definition);

    /* reloptions */
    if (reloptions)
        append_string_object(ret, "WITH (%{opts}s)", "opts", reloptions);

    /* tablespace */
    if (tablespace)
        append_string_object(ret, "TABLESPACE %{tablespace}s", "tablespace", tablespace);

    if (invisible)
        append_format_string(ret, "INVISIBLE");

    /* WHERE clause */

    if (whereClause)
        append_string_object(ret, "WHERE %{where}s", "where", whereClause);

    relation_close(idxrel, AccessShareLock);
    relation_close(heaprel, AccessShareLock);

    return ret;
}

/*
 * Deparse a CreateStmt (CREATE TABLE).
 *
 * Given a table OID and the parse tree that created it, return an ObjTree
 * representing the creation command.
 *
 * Verbose syntax
 * CREATE %{persistence}s TABLE %{if_not_exists}s %{identity}D [OF
 * %{of_type}T | PARTITION OF %{parent_identity}D] %{table_elements}s
 * %{inherits}s %{partition_by}s %{access_method}s %{with_clause}s
 * %{on_commit}s %{tablespace}s
 */
static ObjTree *
deparse_CreateStmt(Oid objectId, Node *parsetree)
{
    CreateStmt *node = (CreateStmt *) parsetree;
    Relation relation = relation_open(objectId, AccessShareLock);
    List *dpcontext;
    ObjTree *ret;
    ObjTree *tmp_obj;
    List *list = NIL;
    ListCell *cell;

    ret = new_objtree_VA("CREATE %{persistence}s TABLE %{if_not_exists}s %{identity}D", 3,
                         "persistence", ObjTypeString, get_persistence_str(relation->rd_rel->relpersistence),
                         "if_not_exists", ObjTypeString, node->if_not_exists ? "IF NOT EXISTS" : "",
                         "identity", ObjTypeObject, new_objtree_for_qualname(relation->rd_rel->relnamespace,
                                                  RelationGetRelationName(relation)));

    dpcontext = deparse_context_for(RelationGetRelationName(relation),
                                    objectId);

    if (node->ofTypename) {
        tmp_obj = new_objtree_for_type(relation->rd_rel->reloftype, -1);
        append_object_object(ret, "OF %{of_type}T", tmp_obj);
    } else {
        List *tableelts = NIL;

        /*
         * There is no need to process LIKE clauses separately; they have
         * already been transformed into columns and constraints.
         */

        /*
         * Process table elements: column definitions and constraints.  Only
         * the column definitions are obtained from the parse node itself.  To
         * get constraints we rely on pg_constraint, because the parse node
         * might be missing some things such as the name of the constraints.
         */
        tableelts = deparse_TableElements(relation, node->tableElts, dpcontext);
        tableelts = obtainConstraints(tableelts, objectId);

        if (tableelts)
            append_array_object(ret, "(%{table_elements:, }s)", tableelts);
        else
            append_format_string(ret, "()");
    }

    /* AUTO_INCREMENT */
    if (node->autoIncStart) {
        DefElem *defel = (DefElem*)node->autoIncStart;
        if (IsA(defel->arg, Integer)) {
            append_int_object(ret, "AUTO_INCREMENT = %{autoincstart}n", (int32)intVal(defel->arg));
        } else {
            append_string_object(ret, "AUTO_INCREMENT = %{autoincstart}s", "autoincstart", strVal(defel->arg));
        }
    }

    /* WITH clause */
    foreach (cell, node->options) {
        ObjTree *tmp_obj2;
        DefElem *opt = (DefElem *)lfirst(cell);

        /* filter out some options which we won't set in subscription */
        if (!pg_strcasecmp(opt->defname, "parallel_workers")) {
            continue;
        }

        tmp_obj2 = deparse_DefElem(opt, false);
        if (tmp_obj2)
            list = lappend(list, new_object_object(tmp_obj2));
    }
    if (list)
        append_array_object(ret, "WITH (%{with:, }s)", list);

    if (node->oncommit != ONCOMMIT_NOOP)
        append_object_object(ret, "%{on_commit}s",
                         deparse_OnCommitClause(node->oncommit));

    if (node->tablespacename)
        append_string_object(ret, "TABLESPACE %{tablespace}I", "tablespace",
                             node->tablespacename);

    /* opt_table_options (COMMENT = XX) */
    List *table_options = NIL;
    foreach(cell, node->tableOptions) {
        if (IsA(lfirst(cell), CommentStmt)) {
            CommentStmt *commentStmt = (CommentStmt *)lfirst(cell);
            if (commentStmt->comment) {
                /* order in reverse */
                ObjTree *option_obj = new_objtree("");
                append_string_object(option_obj, "COMMENT=%{comment}L", "comment", commentStmt->comment);
                table_options = lcons(new_object_object(option_obj), table_options);
            }
        }
    }
    if (table_options) 
        append_array_object(ret, "%{options:, }s", table_options);

    /* opt_table_partitioning_clause */
    if (node->partTableState && relation->rd_rel->parttype != PARTTYPE_NON_PARTITIONED_RELATION) {
        append_string_object(ret, "%{partition_by}s", "partition_by", pg_get_partkeydef_string(relation));
    }

    relation_close(relation, AccessShareLock);

    return ret;
}

/*
 * Handle deparsing of DROP commands.
 *
 * Verbose syntax
 * DROP %s IF EXISTS %%{objidentity}s %{cascade}s
 */
char *
deparse_drop_command(const char *objidentity, const char *objecttype,
                     Node *parsetree)
{
    DropStmt *node = (DropStmt*)parsetree;
    StringInfoData str;
    char *command;
    char *identity = (char *) objidentity;
    ObjTree *stmt;
    Jsonb *jsonb;
    bool concurrent = false;

    initStringInfo(&str);

    if ((!strcmp(objecttype, "index") && node->concurrent))
        concurrent = true;

    stmt = new_objtree_VA("DROP %{objtype}s %{concurrently}s %{if_exists}s %{objidentity}s %{cascade}s", 5,
                          "objtype", ObjTypeString, objecttype,
                          "concurrently", ObjTypeString, concurrent ? "CONCURRENTLY" : "",
                          "if_exists", ObjTypeString, node->missing_ok ? "IF EXISTS" : "",
                          "objidentity", ObjTypeString, identity,
                          "cascade", ObjTypeString, node->behavior == DROP_CASCADE ? "CASCADE" : "");

    jsonb = objtree_to_jsonb(stmt, NULL);
    command = JsonbToCString(&str, VARDATA(jsonb), JSONB_ESTIMATED_LEN);

    return command;
}

/*
 * Handle deparsing of simple commands.
 *
 * This function should cover all cases handled in ProcessUtilitySlow.
 */
static ObjTree *
deparse_simple_command(CollectedCommand *cmd, bool *include_owner)
{
    Oid objectId;
    Node *parsetree;

    Assert(cmd->type == SCT_Simple);

    parsetree = cmd->parsetree;
    objectId = cmd->d.simple.address.objectId;

    if (cmd->in_extension && (nodeTag(parsetree) != T_CreateExtensionStmt))
        return NULL;

    /* This switch needs to handle everything that ProcessUtilitySlow does */
    switch (nodeTag(parsetree)) {
        case T_CreateStmt:
            return deparse_CreateStmt(objectId, parsetree);

        case T_IndexStmt:
            return deparse_IndexStmt(objectId, parsetree);

        case T_CreateSeqStmt:
            return deparse_CreateSeqStmt(objectId, parsetree);

        case T_AlterSeqStmt:
            *include_owner = false;
            return deparse_AlterSeqStmt(objectId, parsetree);

        case T_CommentStmt:
            *include_owner = false;
            return deparse_CommentStmt(cmd->d.simple.address, parsetree);

        default:
            elog(INFO, "unrecognized node type in deparse command: %d",
                 (int) nodeTag(parsetree));
    }

    return NULL;
}

/*
 * Workhorse to deparse a CollectedCommand.
 */
char *
deparse_utility_command(CollectedCommand *cmd, ddl_deparse_context *context)
{
    OverrideSearchPath *overridePath;
    MemoryContext oldcxt;
    MemoryContext tmpcxt;
    ObjTree *tree;
    char *command = NULL;
    StringInfoData str;

    /*
     * Allocate everything done by the deparsing routines into a temp context,
     * to avoid having to sprinkle them with memory handling code, but
     * allocate the output StringInfo before switching.
     */
    initStringInfo(&str);
    tmpcxt = AllocSetContextCreate(CurrentMemoryContext,
                                   "deparse ctx",
                                   ALLOCSET_DEFAULT_MINSIZE,
                                   ALLOCSET_DEFAULT_INITSIZE,
                                   ALLOCSET_DEFAULT_MAXSIZE);
    oldcxt = MemoryContextSwitchTo(tmpcxt);

    /*
     * Many routines underlying this one will invoke ruleutils.c functionality
     * to obtain deparsed versions of expressions.  In such results, we want
     * all object names to be qualified, so that results are "portable" to
     * environments with different search_path settings.  Rather than inject
     * what would be repetitive calls to override search path all over the
     * place, we do it centrally here.
     */
    overridePath = GetOverrideSearchPath(CurrentMemoryContext);
    overridePath->schemas = NIL;
    overridePath->addCatalog = false;
    overridePath->addTemp = true;
    PushOverrideSearchPath(overridePath);

    switch (cmd->type) {
        case SCT_Simple:
            tree = deparse_simple_command(cmd, &context->include_owner);
            break;

        default:
            elog(ERROR, "unexpected deparse node type %d", cmd->type);
    }

    PopOverrideSearchPath();

    if (tree) {
        Jsonb *jsonb;

        jsonb = objtree_to_jsonb(tree, context->include_owner ? cmd->role : NULL);
        command = JsonbToCString(&str, VARDATA(jsonb), JSONB_ESTIMATED_LEN);
    }

    /*
     * Clean up.  Note that since we created the StringInfo in the caller's
     * context, the output string is not deleted here.
     */
    MemoryContextSwitchTo(oldcxt);
    MemoryContextDelete(tmpcxt);

    return command;
}
