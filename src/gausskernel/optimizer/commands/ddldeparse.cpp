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
#include "catalog/pg_trigger.h"
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
#include "utils/guc_tables.h"
#include "utils/guc.h"
#include "utils/jsonb.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/partitionkey.h"
#include "utils/syscache.h"
#include "optimizer/clauses.h"

/* Estimated length of the generated jsonb string */
static const int JSONB_ESTIMATED_LEN = 128;

/* copy from ruleutils.cpp */
#define  BEGIN_P_STR      " BEGIN_B_PROC " /* used in dolphin type proc body */
#define  BEGIN_P_LEN      14
#define  BEGIN_N_STR      "    BEGIN     " /* BEGIN_P_STR to same length */

/*
 * Mark the max_volatility flag for an expression in the command.
 */
static void mark_function_volatile(ddl_deparse_context* context, Node* expr)
{
    if (context->max_volatility == PROVOLATILE_VOLATILE) {
        return;
    }

    if (contain_volatile_functions(expr)) {
        context->max_volatility = PROVOLATILE_VOLATILE;
        return;
    }

    if (context->max_volatility == PROVOLATILE_IMMUTABLE &&
        contain_mutable_functions(expr)) {
            context->max_volatility = PROVOLATILE_STABLE;
    }
}

static void check_alter_table_rewrite_replident_change(Relation r, int attno, const char *cmd)
{
    Oid replidindex = RelationGetReplicaIndex(r);
    if (!OidIsValid(replidindex)) {
        ereport(ERROR,
            (errmsg("cannot use %s command without replident index because it cannot be replicated in DDL replication",
                    cmd)));
    }

    if (IsRelationReplidentKey(r, attno)) {
        ereport(ERROR,
            (errmsg("cannot use %s command to replica index attr because it cannot be replicated in DDL replication",
                    cmd)));
    }
}

static void check_alter_table_replident(Relation rel)
{
    if (rel->relreplident != REPLICA_IDENTITY_FULL &&
        !OidIsValid(RelationGetReplicaIndex(rel))) {
        elog(ERROR, "this ALTER TABLE command will cause a table rewritting, "
                    "but the table does not have a replica identity, it cannot be replicated in DDL replication");
    }
}

void table_close(Relation relation, LOCKMODE lockmode)
{
    relation_close(relation, lockmode);
}

/*
 * Reduce some unnecessary strings from the output json when verbose
 * and "present" member is false. This means these strings won't be merged into
 * the last DDL command.
 */

static char* append_object_to_format_string(ObjTree *tree, const char *sub_fmt);
static void append_premade_object(ObjTree *tree, ObjElem *elem);
static void append_int_object(ObjTree *tree, char *sub_fmt, int32 value);
static void append_float_object(ObjTree *tree, char *sub_fmt, float8 value);
static void format_type_detailed(Oid type_oid, int32 typemod,
                                 Oid *nspid, char **typname, char **typemodstr,
                                 bool *typarray);
static ObjElem* new_object(ObjType type, char *name);
static ObjTree* new_objtree_for_qualname_id(Oid classId, Oid objectId);
static ObjTree* new_objtree(const char *fmt);
static ObjElem* new_object_object(ObjTree *value);

ObjTree* new_objtree_VA(const char *fmt, int numobjs, ...);
ObjElem* new_string_object(char *value);

static JsonbValue* objtree_to_jsonb_rec(ObjTree *tree, JsonbParseState *state, char *owner);
static void pg_get_indexdef_detailed(Oid indexrelid, bool global,
                                     char **index_am,
                                     char **definition,
                                     char **reloptions,
                                     char **tablespace,
                                     char **whereClause,
                                     bool *invisible);
static char* RelationGetColumnDefault(Relation rel, AttrNumber attno,
                                      List *dpcontext, List **exprs);

static ObjTree* deparse_ColumnDef(Relation relation, List *dpcontext, bool composite,
                                  ColumnDef *coldef, bool is_alter, List **exprs);
static ObjTree* deparse_ColumnSetOptions(AlterTableCmd *subcmd);

static ObjTree* deparse_DefElem(DefElem *elem, bool is_reset);
static ObjTree* deparse_OnCommitClause(OnCommitAction option);
static ObjTree* deparse_add_subpartition(ObjTree* ret, Oid partoid,
                                         List *subPartitionDefState, int parkeynum, Oid* partkey_types);
static List* deparse_partition_boudaries(Oid parentoid, char reltype, char strategy, const char* partition_name,
                                         Oid* partoid, int parkeynum, Oid* partkey_types);
static int get_partition_key_types(Oid reloid, char parttype, Oid **partkey_types);
static List* get_range_partition_maxvalues(List *boundary);
static List* get_list_partition_maxvalues(List *boundary);
static ObjTree* deparse_RelSetOptions(AlterTableCmd *subcmd);

static inline ObjElem* deparse_Seq_Cache(sequence_values *seqdata, bool alter_table);
static inline ObjElem* deparse_Seq_Cycle(sequence_values *seqdata, bool alter_table);
static inline ObjElem* deparse_Seq_IncrementBy(sequence_values *seqdata, bool alter_table);
static inline ObjElem* deparse_Seq_Minvalue(sequence_values *seqdata, bool alter_table);
static inline ObjElem* deparse_Seq_Maxvalue(sequence_values *seqdata, bool alter_table);
static inline ObjElem* deparse_Seq_Restart(char *last_value);
static inline ObjElem* deparse_Seq_Startwith(sequence_values *seqdata, bool alter_table);
static ObjElem* deparse_Seq_OwnedBy(Oid sequenceId);
static inline ObjElem* deparse_Seq_Order(DefElem *elem);
static inline ObjElem* deparse_Seq_As(DefElem *elem);
static ObjTree* deparse_CreateFunction(Oid objectId, Node *parsetree);
static ObjTree* deparse_FunctionSet(VariableSetKind kind, char *name, char *value);
static ObjTree* deparse_CreateTrigStmt(Oid objectId, Node *parsetree);
static ObjTree* deparse_AlterTrigStmt(Oid objectId, Node *parsetree);
static ObjTree* deparse_AlterFunction(Oid objectId, Node *parsetree);

static List* deparse_TableElements(Relation relation, List *tableElements, List *dpcontext, bool composite);
extern char* pg_get_trigger_whenclause(Form_pg_trigger trigrec, Node* whenClause, bool pretty);
extern char* pg_get_functiondef_string(Oid funcid);

/*
 * Append a boolean parameter to a tree.
 */
static void append_bool_object(ObjTree *tree, char *sub_fmt, bool value)
{
    ObjElem    *param;
    char       *object_name = sub_fmt;
    bool        is_present_flag = false;

    Assert(sub_fmt);

    /*
     * Check if the format string is 'present' and if yes, store the boolean
     * value
     */
    if (strcmp(sub_fmt, "present") == 0) {
        is_present_flag = true;
        tree->present = value;
    }

    if (!is_present_flag)
        object_name = append_object_to_format_string(tree, sub_fmt);

    param = new_object(ObjTypeBool, object_name);
    param->value.boolean = value;
    append_premade_object(tree, param);
}

/*
 * Append an int32 parameter to a tree.
 */
static void append_int_object(ObjTree *tree, char *sub_fmt, int32 value)
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
 * Append a float8 parameter to a tree.
 */
static void append_float_object(ObjTree *tree, char *sub_fmt, float8 value)
{
    ObjElem    *param;
    char       *object_name;

    Assert(sub_fmt);

    object_name = append_object_to_format_string(tree, sub_fmt);

    param = new_object(ObjTypeFloat, object_name);
    param->value.flt = value;
    append_premade_object(tree, param);
}

/*
 * Append a NULL-or-quoted-literal clause. Userful for COMMENT and SECURITY
 * LABEL.
 *
 * Verbose syntax
 * %{null}s %{literal}s
 */
static void append_literal_or_null(ObjTree *parent, char *elemname, char *value)
{
    ObjTree     *top;
    ObjTree     *part;

    top = new_objtree("");
    part = new_objtree_VA("NULL", 1,
                          "present", ObjTypeBool, !value);
    append_object_object(top, "%{null}s", part);

    part = new_objtree_VA("", 1,
                          "present", ObjTypeBool, value != NULL);

    if (value) {
        append_string_object(part, "%{value}L", "value", value);
    }

    append_object_object(top, "%{literal}s", part);

    append_object_object(parent, elemname, top);
}

/*
 * Append an array parameter to a tree.
 */
void append_array_object(ObjTree *tree, char *sub_fmt, List *array)
{
    ObjElem *param;
    char *object_name;

    Assert(sub_fmt);

    if (!array || list_length(array) == 0) {
        return;
    }

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
void append_format_string(ObjTree *tree, char *sub_fmt)
{
    int len;
    char *fmt;

    if (tree->fmtinfo == NULL) {
        return;
    }

    fmt = tree->fmtinfo->data;
    len = tree->fmtinfo->len;

    /* Add a separator if necessary */
    if (len > 0 && fmt[len - 1] != ' ') {
        appendStringInfoSpaces(tree->fmtinfo, 1);
    }
    appendStringInfoString(tree->fmtinfo, sub_fmt);
}

/*
 * Append present as false to a tree.
 * If sub_fmt is passed and verbose mode is ON,
 * append sub_fmt as well to tree.
 *
 * Example:
 * in non-verbose mode, element will be like:
 * "collation": {"fmt": "COLLATE", "present": false}
 * in verbose mode:
 * "collation": {"fmt": "COLLATE %{name}D", "present": false}
 */
static void append_not_present(ObjTree *tree, char *sub_fmt)
{
    if (sub_fmt) {
        append_format_string(tree, sub_fmt);
    }

    append_bool_object(tree, "present", false);
}

/*
 * Append an object parameter to a tree.
 */
void append_object_object(ObjTree *tree, char *sub_fmt, ObjTree *value)
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
static char* append_object_to_format_string(ObjTree *tree, const char *sub_fmt)
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
static inline void append_premade_object(ObjTree *tree, ObjElem *elem)
{
    slist_push_head(&tree->params, &elem->node);
    tree->numParams++;
}

/*
 * Append a string parameter to a tree.
 */
void append_string_object(ObjTree *tree, char *sub_fmt, char *name,
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
static void format_type_detailed(Oid type_oid, int32 typemod,
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
static inline char* get_persistence_str(char persistence)
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
static inline char* get_type_storage(char storagetype)
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
static ObjElem* new_object(ObjType type, char *name)
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
static ObjElem* new_object_object(ObjTree *value)
{
    ObjElem *param;

    param = new_object(ObjTypeObject, NULL);
    param->value.object = value;

    return param;
}

/*
 * Allocate a new object tree to store parameter values.
 */
static ObjTree* new_objtree(const char *fmt)
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
static ObjTree* new_objtree_for_qualname(Oid nspid, char *name)
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
static ObjTree* new_objtree_for_qualname_id(Oid classId, Oid objectId)
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

static ObjTree* new_objtree_for_qualname_rangevar(RangeVar* rv)
{
    ObjTree    *qualified = NULL;
    if (rv->schemaname) {
        qualified = new_objtree_VA(NULL, 2,
                                   "schemaname", ObjTypeString, rv->schemaname,
                                   "objname", ObjTypeString, pstrdup(rv->relname));
    } else {
        /* serachpath has no schema set in deparse_utility_command */
        Oid reloid = RangeVarGetRelid(rv, AccessExclusiveLock, false);
        qualified = new_objtree_for_qualname_id(RelationRelationId, reloid);
    }
    return qualified;
}

/*
 * A helper routine to setup %{}T elements.
 */
static ObjTree* new_objtree_for_type(Oid typeId, int32 typmod)
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
ObjTree* new_objtree_VA(const char *fmt, int numobjs, ...)
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
 * Allocate a new string object.
 */
ObjElem* new_string_object(char *value)
{
    ObjElem    *param;

    Assert(value);

    param = new_object(ObjTypeString, NULL);
    param->value.string = value;

    return param;
}

static ObjTree* deparse_AlterSchemaStmt(Oid objectId, Node *parsetree)
{
    ObjTree    *ret;
    AlterSchemaStmt *stmt = (AlterSchemaStmt *) parsetree;

    bool setblockchain = false;

    if (stmt->charset == PG_INVALID_ENCODING && !stmt->collate) {
        setblockchain = true;
    }

    ret = new_objtree_VA("ALTER SCHEMA %{schemaname}I", 1,
                         "schemaname", ObjTypeString, stmt->schemaname);

    if (setblockchain) {
        append_string_object(ret, "%{with}s BLOCKCHAIN", "with", stmt->hasBlockChain ?
                             "WITH" : "WITHOUT");
    } else {
        if (stmt->charset != PG_INVALID_ENCODING) {
            append_string_object(ret, "CHARACTER SET = %{charset}s", "charset",
                                 pg_encoding_to_char(stmt->charset));
        }

        if (stmt->collate) {
            append_string_object(ret, "COLLATE = %{collate}s", "collate", stmt->collate);
        }
    }

    return ret;
}


/*
 * Deparse a CreateSchemaStmt.
 *
 * Given a schema OID and the parse tree that created it, return an ObjTree
 * representing the creation command.
 *
 * Verbose syntax
 * CREATE SCHEMA %{if_not_exists}s %{name}I %{authorization}s
*/
static ObjTree* deparse_CreateSchemaStmt(Oid objectId, Node *parsetree, bool *include_owner)
{
    CreateSchemaStmt *node = (CreateSchemaStmt *) parsetree;
    ObjTree    *ret;
    ObjTree    *auth;
    ObjTree    *blockchain;
    ret = new_objtree_VA("CREATE SCHEMA %{if_not_exists}s %{name}I", 2,
                         "if_not_exists", ObjTypeString,
                         node->missing_ok ? "IF NOT EXISTS" : "",
                         "name", ObjTypeString,
                         node->schemaname ? node->schemaname : "");

    auth = new_objtree("AUTHORIZATION");
    if (node->authid) {
        append_string_object(auth, "%{authorization_role}I",
                             "authorization_role",
                             node->authid);
        *include_owner =  false;
    } else {
        append_not_present(auth, "%{authorization_role}I");
    }

    append_object_object(ret, "%{authorization}s", auth);

    blockchain = new_objtree("WITH BLOCKCHAIN");
    if (!node->hasBlockChain)
        append_not_present(blockchain, "%{blockchain}s");
    append_object_object(ret, "%{blockchain}s", blockchain);

    return ret;
}

/*
 * Return the given object type as a string.
 *
 * If isgrant is true, then this function is called while deparsing GRANT
 * statement and some object names are replaced.
 */
const char* string_objtype(ObjectType objtype, bool isgrant)
{
    switch (objtype) {
        case OBJECT_COLUMN:
            return isgrant ? "TABLE" : "COLUMN";
        case OBJECT_DOMAIN:
            return "DOMAIN";
        case OBJECT_FUNCTION:
            return "FUNCTION";
        case OBJECT_INDEX:
            return "INDEX";
        case OBJECT_SCHEMA:
            return "SCHEMA";
        case OBJECT_SEQUENCE:
            return "SEQUENCE";
        case OBJECT_LARGE_SEQUENCE:
            return "LARGE SEQUENCE";
        case OBJECT_TABLE:
            return "TABLE";
        case OBJECT_TABLESPACE:
            return "TABLESPACE";
        case OBJECT_TRIGGER:
            return "TRIGGER";
        case OBJECT_TYPE:
            return "TYPE";
        case OBJECT_VIEW:
            return "VIEW";
        default:
            elog(WARNING, "unsupported object type %d for string", objtype);
    }

    return "???"; /* keep compiler quiet */
}

/*
 * Process the pre-built format string from the ObjTree into the output parse
 * state.
 */
static void objtree_fmt_to_jsonb_element(JsonbParseState *state, ObjTree *tree)
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
static void role_to_jsonb_element(JsonbParseState *state, char *owner)
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
static Jsonb* objtree_to_jsonb(ObjTree *tree, char *owner)
{
    JsonbValue *value;

    value = objtree_to_jsonb_rec(tree, NULL, owner);
    return JsonbValueToJsonb(value);
}

/*
 * Helper for objtree_to_jsonb: process an individual element from an object or
 * an array into the output parse state.
 */
static void objtree_to_jsonb_element(JsonbParseState *state, ObjElem *object,
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
static JsonbValue* objtree_to_jsonb_rec(ObjTree *tree, JsonbParseState *state, char *owner)
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
static List* obtainConstraints(List *elements, Oid relationId)
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
static void pg_get_indexdef_detailed(Oid indexrelid, bool global,
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
            } else if (indexkey && IsA(indexkey, PrefixKey)) {
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
static char* RelationGetColumnDefault(Relation rel, AttrNumber attno, List *dpcontext,
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

/* used by AT_ModifyColumn */
static ObjTree* deparse_ColumnDef_constraints(ObjTree *ret, Relation relation,
                                              ColumnDef *coldef, List *dpcontext, List **exprs)
{
    ObjTree    *tmp_obj;
    Oid        relid = RelationGetRelid(relation);
    ListCell   *cell;
    HeapTuple  attrTup;
    Form_pg_attribute attrForm;

    bool saw_notnull = false;
    bool saw_autoincrement = false;
    char*      onupdate = NULL;
    attrTup = SearchSysCacheAttName(relid, coldef->colname);
    if (!HeapTupleIsValid(attrTup))
        elog(ERROR, "could not find cache entry for column \"%s\" of relation %u",
             coldef->colname, relid);
    attrForm = (Form_pg_attribute) GETSTRUCT(attrTup);

    foreach(cell, coldef->constraints) {
        Constraint *constr = (Constraint *) lfirst(cell);

        if (constr->contype == CONSTR_NOTNULL) {
            saw_notnull = true;
        } else if (constr->contype == CONSTR_AUTO_INCREMENT) {
            saw_autoincrement = true;
            check_alter_table_rewrite_replident_change(relation, attrForm->attnum, "MODIFY COLUMN AUTO_INCREMENT");
        } else if (constr->contype == CONSTR_DEFAULT && constr->update_expr) {
            onupdate = RelationGetColumnOnUpdate(constr->update_expr, dpcontext, exprs);
        }
    }

    if (coldef->is_not_null)
        saw_notnull = true;

    if (saw_autoincrement) {
        ReleaseSysCache(attrTup);
        return ret;
    }

    append_string_object(ret, "%{auto_increment}s", "auto_increment",
                         saw_autoincrement ? "AUTO_INCREMENT" : "");

    /* ON UPDATE */
    append_string_object(ret, "ON UPDATE %{on_update}s", "on_update", onupdate ? onupdate : "");

    append_string_object(ret, "%{not_null}s", "not_null",
                         saw_notnull ? "NOT NULL" : saw_autoincrement ? "NULL" : "");

    /* GENERATED COLUMN EXPRESSION */
    tmp_obj = new_objtree("GENERATED ALWAYS AS");
    if (coldef->generatedCol == ATTRIBUTE_GENERATED_STORED) {
        char       *defstr;

        defstr = RelationGetColumnDefault(relation, attrForm->attnum, dpcontext, exprs);
        append_string_object(tmp_obj, "(%{generation_expr}s) STORED", "generation_expr", defstr);
    } else {
        append_not_present(tmp_obj, "(%{generation_expr}s) STORED");
    }

    append_object_object(ret, "%{generated_column}s", tmp_obj);

    tmp_obj = new_objtree("DEFAULT");

    if (attrForm->atthasdef &&
        coldef->generatedCol != ATTRIBUTE_GENERATED_STORED &&
        !saw_autoincrement &&
        !onupdate) {
        char       *defstr;

        defstr = RelationGetColumnDefault(relation, attrForm->attnum,
                                          dpcontext, exprs);

        append_string_object(tmp_obj, "%{default}s", "default", defstr);
    } else {
        append_not_present(tmp_obj, "%{default}s");
    }
    append_object_object(ret, "%{default}s", tmp_obj);

    ReleaseSysCache(attrTup);
    return ret;
}

static bool istypestring(Oid typid);
/*
 * Deparse a ColumnDef node within a regular (non-typed) table creation.
 *
 * NOT NULL constraints in the column definition are emitted directly in the
 * column definition by this routine; other constraints must be emitted
 * elsewhere (the info in the parse node is incomplete anyway).
 *
 * Verbose syntax
 * %{name}I %{coltype}T %{auto_increment} %{default}s %{not_null}s %{collation}s
 */
static ObjTree* deparse_ColumnDef(Relation relation, List *dpcontext, bool composite,
                                  ColumnDef *coldef, bool is_alter, List **exprs)
{
    ObjTree    *ret;
    ObjTree    *tmp_obj;
    Oid        relid = RelationGetRelid(relation);
    HeapTuple  attrTup;
    Form_pg_attribute attrForm;
    Oid        typid;
    int32      typmod;
    Oid        typcollation;
    bool       saw_notnull;
    bool       saw_autoincrement;
    char*      onupdate = NULL;
    ListCell   *cell;

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

    tmp_obj = new_objtree("COLLATE");
    if (OidIsValid(typcollation)) {
        append_object_object(tmp_obj, "%{name}D",
                             new_objtree_for_qualname_id(CollationRelationId,
                                                         typcollation));
    } else {
        append_not_present(tmp_obj, "%{name}D");
    }
    append_object_object(ret, "%{collation}s", tmp_obj);

    if (!composite) {
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

        if (is_alter && coldef->is_not_null)
            saw_notnull = true;

        if (is_alter && !saw_autoincrement && coldef->raw_default &&
            IsA(coldef->raw_default, AutoIncrement)) {
            saw_autoincrement = true;
        }

        if (is_alter && saw_autoincrement) {
            check_alter_table_rewrite_replident_change(relation, attrForm->attnum, "ADD COLUMN AUTO_INCREMENT");
            /* auto_increment will be set with constraint when rewrite finish */
            ReleaseSysCache(attrTup);
            return ret;
        }

        append_string_object(ret, "%{auto_increment}s", "auto_increment",
                             saw_autoincrement ? "AUTO_INCREMENT" : "");

        tmp_obj = new_objtree("DEFAULT");

        if (attrForm->atthasdef &&
            coldef->generatedCol != ATTRIBUTE_GENERATED_STORED &&
            !saw_autoincrement) {
            char       *defstr = NULL;

            /* initdefval intend that default value is a constant expr,
             * if the default can not get from initdefval, then need output dml change
             * and set default after rewrite
             */
            if (is_alter) {
                if (coldef->initdefval) {
                    StringInfoData defvalbuf;
                    initStringInfo(&defvalbuf);
                    if (istypestring(typid))
                        appendStringInfo(&defvalbuf, "\'%s\'", coldef->initdefval);
                    else
                        appendStringInfo(&defvalbuf, "%s", coldef->initdefval);
                    defstr = pstrdup(defvalbuf.data);
                } else {
                    /* if coldef->initdefval not exist, then default is not a constant
                     * handle it after rewrite finish
                     */
                    append_not_present(tmp_obj, "%{default}s");
                }
                append_string_object(tmp_obj, "%{default}s", "default", defstr);
            } else {
                defstr = RelationGetColumnDefault(relation, attrForm->attnum, dpcontext, exprs);
                if (defstr == NULL || defstr[0] == '\0') {
                    append_not_present(tmp_obj, "%{default}s");
                } else {
                    append_string_object(tmp_obj, "%{default}s", "default", defstr);
                }
            }
        } else {
            append_not_present(tmp_obj, "%{default}s");
        }
        append_object_object(ret, "%{default}s", tmp_obj);

        /* ON UPDATE */
        if ((!onupdate || !strlen(onupdate)) && coldef->update_default) {
            onupdate = RelationGetColumnOnUpdate(coldef->update_default, dpcontext, exprs);
        }
        append_string_object(ret, "ON UPDATE %{on_update}s", "on_update", onupdate ? onupdate : "");

        if (!is_alter || saw_autoincrement)
            append_string_object(ret, "%{not_null}s", "not_null", saw_notnull ? "NOT NULL" : "");

        /* GENERATED COLUMN EXPRESSION */
        tmp_obj = new_objtree("GENERATED ALWAYS AS");
        if (coldef->generatedCol == ATTRIBUTE_GENERATED_STORED) {
            char       *defstr;

            defstr = RelationGetColumnDefault(relation, attrForm->attnum,
                                              dpcontext, exprs);
            append_string_object(tmp_obj, "(%{generation_expr}s) STORED",
                                 "generation_expr", defstr);
        } else {
            append_not_present(tmp_obj, "(%{generation_expr}s) STORED");
        }
        append_object_object(ret, "%{generated_column}s", tmp_obj);
    }

    ReleaseSysCache(attrTup);

    return ret;
}


/*
 * Deparse DefElems
 *
 * Verbose syntax
 * %{label}s = %{value}L
 */
static ObjTree* deparse_DefElem(DefElem *elem, bool is_reset)
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
static ObjTree* deparse_OnCommitClause(OnCommitAction option)
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
static inline ObjElem* deparse_Seq_Cache(sequence_values *seqdata, bool alter_table)
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
static inline ObjElem* deparse_Seq_Cycle(sequence_values *seqdata, bool alter_table)
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
static inline ObjElem* deparse_Seq_IncrementBy(sequence_values *seqdata, bool alter_table)
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
static inline ObjElem* deparse_Seq_Maxvalue(sequence_values *seqdata, bool alter_table)
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
static inline ObjElem* deparse_Seq_Minvalue(sequence_values *seqdata, bool alter_table)
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
static ObjElem* deparse_Seq_OwnedBy(Oid sequenceId)
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
static inline ObjElem* deparse_Seq_Order(DefElem *elem)
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
static inline ObjElem* deparse_Seq_Restart(char *last_value)
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
static inline ObjElem* deparse_Seq_As(DefElem *elem)
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
static inline ObjElem* deparse_Seq_Startwith(sequence_values *seqdata, bool alter_table)
{
    ObjTree *ret;
    const char  *fmt;

    fmt = alter_table ? "SET START WITH %{value}s" : "START WITH %{value}s";

    ret = new_objtree_VA(fmt, 2,
                         "clause", ObjTypeString, "start",
                         "value", ObjTypeString, seqdata->start_value);

    return new_object_object(ret);
}

static bool istypestring(Oid typid)
{
    switch (typid) {
        case INT2OID:
        case INT4OID:
        case INT8OID:
        case FLOAT4OID:
        case FLOAT8OID:
        case NUMERICOID:
            /* Here we ignore infinity and NaN */
            return false;
        default:
            /* All other types are regarded as string. */
            return true;
    }
}

/*
 * Deparse the INHERITS relations.
 *
 * Given a table OID, return a schema-qualified table list representing
 * the parent tables.
 */
static List* deparse_InhRelations(Oid objectId)
{
    List       *parents = NIL;
    Relation    inhRel;
    SysScanDesc scan;
    ScanKeyData key;
    HeapTuple   tuple;

    inhRel = table_open(InheritsRelationId, RowExclusiveLock);

    ScanKeyInit(&key,
                Anum_pg_inherits_inhrelid,
                BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(objectId));

    scan = systable_beginscan(inhRel, InheritsRelidSeqnoIndexId,
                              true, NULL, 1, &key);

    while (HeapTupleIsValid(tuple = systable_getnext(scan))) {
        ObjTree    *parent;
        Form_pg_inherits formInh = (Form_pg_inherits) GETSTRUCT(tuple);

        parent = new_objtree_for_qualname_id(RelationRelationId,
                                             formInh->inhparent);
        parents = lappend(parents, new_object_object(parent));
    }

    systable_endscan(scan);
    table_close(inhRel, RowExclusiveLock);

    return parents;
}

/*
 * Subroutine for CREATE TABLE deparsing.
 *
 * Deal with all the table elements (columns and constraints).
 *
 * Note we ignore constraints in the parse node here; they are extracted from
 * system catalogs instead.
 */
static List* deparse_TableElements(Relation relation, List *tableElements, List *dpcontext, bool composite)
{
    List *elements = NIL;
    ListCell *lc;

    foreach(lc, tableElements) {
        Node *elt = (Node *) lfirst(lc);

        switch (nodeTag(elt)) {
            case T_ColumnDef: {
                    ObjTree    *tree;
                    tree = deparse_ColumnDef(relation, dpcontext,
                                             composite, (ColumnDef *) elt,
                                             false, NULL);
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
static ObjTree* deparse_CreateSeqStmt(Oid objectId, Node *parsetree)
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
static ObjTree* deparse_AlterSeqStmt(Oid objectId, Node *parsetree)
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
 * deparse_ViewStmt
 *      deparse a ViewStmt
 *
 * Given a view OID and the parse tree that created it, return an ObjTree
 * representing the creation command.
 *
 * Verbose syntax
 * CREATE %{or_replace}s %{persistence}s VIEW %{identity}D AS %{query}s
 */
static ObjTree* deparse_ViewStmt(Oid objectId, Node *parsetree)
{
    ViewStmt   *node = (ViewStmt *) parsetree;
    ObjTree    *ret;
    Relation    relation;

    relation = relation_open(objectId, AccessShareLock);

    ret = new_objtree_VA("CREATE %{or_replace}s %{persistence}s VIEW %{identity}D AS %{query}s", 4,
                         "or_replace", ObjTypeString,
                         node->replace ? "OR REPLACE" : "",
                         "persistence", ObjTypeString,
                         get_persistence_str(relation->rd_rel->relpersistence),
                         "identity", ObjTypeObject,
                         new_objtree_for_qualname(relation->rd_rel->relnamespace,
                                                  RelationGetRelationName(relation)),
                         "query", ObjTypeString,
                         pg_get_viewdef_string(objectId));

    relation_close(relation, AccessShareLock);
    return ret;
}

/*
 * Deparse a RenameStmt.
 */
static ObjTree* deparse_RenameStmt(ObjectAddress address, Node *parsetree)
{
    RenameStmt *node = (RenameStmt *) parsetree;
    ObjTree    *ret;
    Relation    relation;
    Oid         schemaId;

    if (node->is_modifycolumn) {
        /* modify column in dbcompatibility B */
        return NULL;
    }

    if (node->renameTableflag) {
        /* rename table syntax in dbcompatibility B */
        ListCell *cell = NULL;
        List     *renamelist = NIL;
        foreach (cell, node->renameTargetList) {
            RenameCell* renameInfo = (RenameCell*)lfirst(cell);
            RangeVar *cur = NULL;
            RangeVar *ori = NULL;
            Oid nspoid = InvalidOid;
            Oid tbloid = InvalidOid;
            ObjTree *tmp_obj = NULL;

            ori = renameInfo->original_name;
            cur = renameInfo->modify_name;

            if (!cur->schemaname || !ori->schemaname) {
                continue;
            }

            nspoid = get_namespace_oid(cur->schemaname, false);
            tbloid = get_relname_relid(cur->relname, nspoid);
            if (!OidIsValid(tbloid)) {
                elog(ERROR, "can not find the table %s.%s for deparse rename table",
                     cur->schemaname, cur->relname);
            }
            if (!relation_support_ddl_replication(tbloid, false)) {
                continue;
            }

            tmp_obj = new_objtree_VA("%{ori}D TO %{modify}D", 2,
                                     "ori", ObjTypeObject, new_objtree_for_qualname_rangevar(ori),
                                     "modify", ObjTypeObject, new_objtree_for_qualname_rangevar(cur));

            renamelist = lappend(renamelist, new_object_object(tmp_obj));
        }

        if (renamelist) {
            ret = new_objtree_VA("RENAME TABLE %{renamelist:, }s", 1,
                                 "renamelist", ObjTypeArray, renamelist);
            return ret;
        } else {
            return NULL;
        }
    }

    /*
     * In an ALTER .. RENAME command, we don't have the original name of the
     * object in system catalogs: since we inspect them after the command has
     * executed, the old name is already gone.  Therefore, we extract it from
     * the parse node.  Note we still extract the schema name from the catalog
     * (it might not be present in the parse node); it cannot possibly have
     * changed anyway.
     */
    switch (node->renameType) {
        case OBJECT_TABLE:
        case OBJECT_INDEX:
        case OBJECT_SEQUENCE:
        case OBJECT_LARGE_SEQUENCE:
        case OBJECT_VIEW:
        case OBJECT_MATVIEW:
            relation = relation_open(address.objectId, AccessShareLock);
            schemaId = RelationGetNamespace(relation);
            ret = new_objtree_VA("ALTER %{objtype}s %{if_exists}s %{identity}D RENAME TO %{newname}I", 4,
                                 "objtype", ObjTypeString,
                                 string_objtype(node->renameType, false),
                                 "if_exists", ObjTypeString,
                                 node->missing_ok ? "IF EXISTS" : "",
                                 "identity", ObjTypeObject,
                                 new_objtree_for_qualname(schemaId,
                                                          node->relation->relname),
                                 "newname", ObjTypeString,
                                 node->newname);
            relation_close(relation, AccessShareLock);
            break;

        case OBJECT_ATTRIBUTE:
        case OBJECT_COLUMN:
            relation = relation_open(address.objectId, AccessShareLock);
            schemaId = RelationGetNamespace(relation);

            if (node->renameType == OBJECT_ATTRIBUTE) {
                ret = new_objtree_VA("ALTER TYPE %{identity}D RENAME ATTRIBUTE %{colname}I", 2,
                                     "identity", ObjTypeObject,
                                     new_objtree_for_qualname(schemaId,
                                                              node->relation->relname),
                                     "colname", ObjTypeString, node->subname);
            } else {
                ret = new_objtree_VA("ALTER %{objtype}s", 1,
                                     "objtype", ObjTypeString,
                                     string_objtype(node->relationType, false));

                /* Composite types do not support IF EXISTS */
                if (node->renameType == OBJECT_COLUMN)
                    append_string_object(ret, "%{if_exists}s",
                                         "if_exists",
                                         node->missing_ok ? "IF EXISTS" : "");

                append_object_object(ret, "%{identity}D",
                                     new_objtree_for_qualname(schemaId,
                                                              node->relation->relname));
                append_string_object(ret, "RENAME COLUMN %{colname}I",
                                     "colname", node->subname);
            }

            append_string_object(ret, "TO %{newname}I", "newname", node->newname);

            if (node->renameType == OBJECT_ATTRIBUTE)
                append_object_object(ret, "%{cascade}s",
                                     new_objtree_VA("CASCADE", 1,
                                                    "present", ObjTypeBool,
                                                    node->behavior == DROP_CASCADE));

            relation_close(relation, AccessShareLock);
            break;

        case OBJECT_SCHEMA:
            ret = new_objtree_VA("ALTER SCHEMA %{identity}I RENAME TO %{newname}I", 2,
                                 "identity", ObjTypeString, node->subname,
                                 "newname", ObjTypeString, node->newname);
            break;
        case OBJECT_TABCONSTRAINT: {
                HeapTuple    constrtup;
                Form_pg_constraint constform;

                constrtup = SearchSysCache1(CONSTROID,
                                            ObjectIdGetDatum(address.objectId));
                if (!HeapTupleIsValid(constrtup))
                    elog(ERROR, "cache lookup failed for constraint with OID %u",
                         address.objectId);
                constform = (Form_pg_constraint) GETSTRUCT(constrtup);

                ret = new_objtree_VA("ALTER TABLE %{identity}D RENAME CONSTRAINT %{oldname}I TO %{newname}I", 3,
                                     "identity", ObjTypeObject,
                                     new_objtree_for_qualname_id(RelationRelationId,
                                                                 constform->conrelid),
                                     "oldname", ObjTypeString, node->subname,
                                     "newname", ObjTypeString, node->newname);
                ReleaseSysCache(constrtup);
            }
            break;
        case OBJECT_TYPE: {
                HeapTuple    typtup;
                Form_pg_type typform;
                Oid nspid;

                List* names = node->object;
                char *typeName;
                char *schemaname;
                char *pkgName;
                DeconstructQualifiedName(names, &schemaname, &typeName, &pkgName);

                typtup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(address.objectId));
                if (!HeapTupleIsValid(typtup))
                    elog(ERROR, "cache lookup failed for type with OID %u", address.objectId);
                typform = (Form_pg_type) GETSTRUCT(typtup);
                nspid = typform->typnamespace;

                ret = new_objtree_VA("ALTER TYPE %{identity}D RENAME TO %{newname}I", 2,
                                     "identity", ObjTypeObject,
                                     new_objtree_for_qualname(nspid,
                                                              typeName),
                                     "newname", ObjTypeString, node->newname);

                ReleaseSysCache(typtup);
            }
            break;
        default:
            elog(WARNING, "unsupported RenameStmt object type %d", node->renameType);
            return NULL;
    }

    return ret;
}

/*
 * Deparse a CommentStmt when it pertains to a constraint.
 *
 * Verbose syntax
 * COMMENT ON CONSTRAINT %{identity}s ON [DOMAIN] %{parentobj}s IS %{comment}s
 */
static ObjTree* deparse_CommentOnConstraintSmt(Oid objectId, Node *parsetree)
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
static ObjTree* deparse_CommentStmt(ObjectAddress address, Node *parsetree)
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
 * Deparse a CompositeTypeStmt (CREATE TYPE AS)
 *
 * Given a Composite type OID and the parse tree that created it, return an
 * ObjTree representing the creation command.
 *
 * Verbose syntax
 * CREATE TYPE %{identity}D AS (%{columns:, }s)
 */
static ObjTree* deparse_CompositeTypeStmt(Oid objectId, Node *parsetree)
{
    CompositeTypeStmt *node = (CompositeTypeStmt *) parsetree;
    HeapTuple    typtup;
    Form_pg_type typform;
    Relation    typerel;
    List       *dpcontext;
    List       *tableelts = NIL;

    /* Find the pg_type entry and open the corresponding relation */
    typtup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(objectId));
    if (!HeapTupleIsValid(typtup))
        elog(ERROR, "cache lookup failed for type with OID %u", objectId);

    typform = (Form_pg_type) GETSTRUCT(typtup);
    typerel = relation_open(typform->typrelid, AccessShareLock);

    dpcontext = deparse_context_for(RelationGetRelationName(typerel),
                                    RelationGetRelid(typerel));

    tableelts = deparse_TableElements(typerel, node->coldeflist, dpcontext,
                                      true);    /* composite type */

    table_close(typerel, AccessShareLock);
    ReleaseSysCache(typtup);

    return new_objtree_VA("CREATE TYPE %{identity}D AS (%{columns:, }s)", 2,
                          "identity", ObjTypeObject,
                          new_objtree_for_qualname_id(TypeRelationId, objectId),
                          "columns", ObjTypeArray, tableelts);
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
static ObjTree* deparse_IndexStmt(Oid objectId, Node *parsetree)
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

    ret = new_objtree_VA(
        "CREATE %{unique}s INDEX %{concurrently}s %{name}I ON %{table}D USING %{index_am}s %{definition}s", 6,
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
static ObjTree* deparse_CreateStmt(Oid objectId, Node *parsetree)
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
        tableelts = deparse_TableElements(relation, node->tableElts, dpcontext,
                                          false);    /* not composite */
        tableelts = obtainConstraints(tableelts, objectId);

        tmp_obj = new_objtree("");
        if (tableelts)
            append_array_object(tmp_obj, "(%{elements:, }s)", tableelts);
        else
            append_format_string(tmp_obj, "()");

        append_object_object(ret, "%{table_elements}s", tmp_obj);

        /*
         * Add inheritance specification.  We cannot simply scan the list of
         * parents from the parser node, because that may lack the actual
         * qualified names of the parent relations.  Rather than trying to
         * re-resolve them from the information in the parse node, it seems
         * more accurate and convenient to grab it from pg_inherits.
         */
        tmp_obj = new_objtree("INHERITS");
        if (node->inhRelations != NIL) {
            append_array_object(tmp_obj, "(%{parents:, }D)", deparse_InhRelations(objectId));
        } else {
            append_not_present(tmp_obj, "(%{parents:, }D)");
        }
        append_object_object(ret, "%{inherits}s", tmp_obj);
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
 *
declare
null_cnt int;
start_num int;
sql text;
begin
select count(*) from %{identity}D where %{colname}I is null into null_cnt;
start_num := intmax - null_cnt;
sql := 'create large sequence %{seqname}D start with ' || start_num;
execute sql;
update %{identity}D set %{colname}I=pg_catalog.nextval('%{seqname}D') WEHRE %{colname}I IS NULL;
DROP large SEQUENCE %{seqname}D;
end;
/
 */

static char ADAPT_SUBSCTIPTION_AUTOINCREMENT_FMT[] =
"DECLARE\n"
"null_cnt pg_catalog.%{typname}s;\n"
"start_num pg_catalog.%{typname}s;\n"
"sql pg_catalog.text;\n"
"BEGIN\n"
"SELECT COUNT(*) FROM %{identity}D WHERE %{colname}I IS NULL INTO null_cnt;\n"
"SELECT pg_catalog.min(%{colname}I) - 1 - null_cnt FROM %{identity}D INTO start_num;\n"
"sql := 'CREATE LARGE SEQUENCE %{seqname}D START WITH ' || start_num || ' MINVALUE ' || start_num;\n"
"EXECUTE sql;\n"
"UPDATE %{identity}D SET %{colname}I = pg_catalog.nextval('%{seqname}D') WHERE %{colname}I IS NULL;\n"
"DROP LARGE SEQUENCE %{seqname}D;\n"
"END;\n";

static ObjTree* adapt_subscription_autoincrement_null_value(Relation rel, ColumnDef *coldef, Oid typid)
{
    ObjTree *tmp_obj = NULL;
    char *maxvalue = NULL;
    char *seqname = NULL;
    char *typname = NULL;
    sequence_values *seqvalues = get_sequence_values(RelAutoIncSeqOid(rel));
    StringInfoData string_buf;

    initStringInfo(&string_buf);
    appendStringInfo(&string_buf, "ddl_replication_%s", seqvalues->sequence_name);
    seqname = pstrdup(string_buf.data);

    resetStringInfo(&string_buf);

    switch (typid) {
        case BOOLOID:
            maxvalue = pstrdup("1");
            typname = pstrdup("bool");
            break;
        case INT1OID:
            appendStringInfo(&string_buf, "%u", UCHAR_MAX);
            maxvalue = pstrdup(string_buf.data);
            typname = pstrdup("int1");
            break;
        case INT2OID:
            appendStringInfo(&string_buf, "%d", SHRT_MAX);
            maxvalue = pstrdup(string_buf.data);
            typname = pstrdup("int2");
            break;
        case INT4OID:
            appendStringInfo(&string_buf, "%d", INT_MAX);
            maxvalue = pstrdup(string_buf.data);
            typname = pstrdup("int4");
            break;
        case INT8OID :
        case FLOAT4OID :
        case FLOAT8OID : {
                char buf[MAXINT8LEN + 1];
                pg_lltoa(PG_INT64_MAX, buf);
                maxvalue = pstrdup(buf);
                /* just use int8 for create sequence */
                typname = pstrdup("int8");
            }
            break;
        case INT16OID: {
                const int MAXINT16LEN = 45;
                char buf[MAXINT16LEN + 1];
                pg_i128toa(PG_INT128_MAX, buf, MAXINT16LEN + 1);
                maxvalue = pstrdup(buf);
                typname = pstrdup("int16");
            }
            break;
        default : {
            appendStringInfo(&string_buf, "%d", INT_MAX);
            maxvalue = pstrdup(string_buf.data);
            typname = pstrdup("int4");
            break;
        }
    }

    tmp_obj = new_objtree_VA(ADAPT_SUBSCTIPTION_AUTOINCREMENT_FMT, 4,
        "seqname", ObjTypeObject, new_objtree_for_qualname(rel->rd_rel->relnamespace, seqname),
        "identity", ObjTypeObject, new_objtree_for_qualname(rel->rd_rel->relnamespace, RelationGetRelationName(rel)),
        "colname", ObjTypeString, coldef->colname,
        "typname", ObjTypeString, typname);

    FreeStringInfo(&string_buf);
    return tmp_obj;
}

static List* deparse_AlterRelation_add_column_default(CollectedCommand *cmd)
{
    ObjTree    *ret = NULL;
    ObjTree    *tmp_obj = NULL;
    List       *dpcontext;
    Relation    rel;
    List       *subcmds = NIL;
    ListCell   *cell;
    List       *exprs = NIL;
    Oid            relId = cmd->d.alterTable.objectId;
    AlterTableStmt *stmt = NULL;
    bool        isonly = false;
    bool        isrewrite = false;

    List *tree_list = NIL;
    List *not_null_list = NIL;
    List *constraint_list = NIL;

    isrewrite = cmd->d.alterTable.rewrite;

    rel = relation_open(relId, AccessShareLock);
    dpcontext = deparse_context_for(RelationGetRelationName(rel),
                                    relId);
    stmt = (AlterTableStmt *) cmd->parsetree;

    if (rel->rd_rel->relkind != RELKIND_RELATION ||
        rel->rd_rel->relpersistence != RELPERSISTENCE_PERMANENT) {
        relation_close(rel, AccessShareLock);
        return NIL;
    }

    if (stmt->relation && stmt->relation->inhOpt == INH_NO) {
        isonly = true;
    }

    foreach(cell, cmd->d.alterTable.subcmds) {
        CollectedATSubcmd *sub = (CollectedATSubcmd *) lfirst(cell);
        AlterTableCmd *subcmd = (AlterTableCmd *) sub->parsetree;

        if (subcmd->recursing)
            continue;

        switch (subcmd->subtype) {
            case AT_AddColumn:
            case AT_AddColumnRecurse: {
                    ColumnDef *coldef = (ColumnDef*)subcmd->def;
                    HeapTuple    attrTup;
                    Form_pg_attribute attrForm;
                    Oid          typid;
                    int32        typmod;
                    Oid          typcollation;

                    /* do nothing */
                    if (coldef->generatedCol == ATTRIBUTE_GENERATED_STORED) {
                        break;
                    }

                    attrTup = SearchSysCacheAttName(relId, coldef->colname);
                    if (!HeapTupleIsValid(attrTup))
                        elog(ERROR, "could not find cache entry for column \"%s\" of relation %u",
                             coldef->colname, relId);
                    attrForm = (Form_pg_attribute) GETSTRUCT(attrTup);
                    if (!attrForm->atthasdef) {
                        ReleaseSysCache(attrTup);
                        break;
                    }

                    get_atttypetypmodcoll(relId, attrForm->attnum, &typid, &typmod, &typcollation);

                    /* for auto_increment, construct the modify column clause after rewrite */
                    if (coldef->raw_default && IsA(coldef->raw_default, AutoIncrement)) {
                        if (attrForm->attnotnull) {
                            tmp_obj = adapt_subscription_autoincrement_null_value(rel, coldef, typid);
                            if (tmp_obj) {
                                tree_list = lappend(tree_list, tmp_obj);
                            }
                        }

                        tmp_obj = new_objtree_VA("MODIFY COLUMN %{colname}I %{coltype}T AUTO_INCREMENT", 2,
                            "colname", ObjTypeString, coldef->colname,
                            "coltype", ObjTypeObject, new_objtree_for_type(typid, typmod));
                        if (!coldef->is_not_null) {
                            append_format_string(tmp_obj, "NULL");
                        }
                        constraint_list = lappend(constraint_list, new_object_object(tmp_obj));
                        ReleaseSysCache(attrTup);

                        break;
                    }

                    if (coldef->is_not_null) {
                        tmp_obj =  new_objtree_VA(
                            "ALTER TABLE %{only}s %{identity}D ALTER COLUMN %{name}I SET NOT NULL", 3,
                            "only", ObjTypeString, isonly ? "ONLY" : "",
                            "identity", ObjTypeObject,
                        new_objtree_for_qualname(rel->rd_rel->relnamespace,
                                                 RelationGetRelationName(rel)),
                            "name", ObjTypeString, coldef->colname);
                        not_null_list = lappend(not_null_list, tmp_obj);
                    }

                    char *defstr = NULL;
                    char *initdefval = NULL;

                    if (coldef->initdefval) {
                        StringInfoData defvalbuf;
                        initStringInfo(&defvalbuf);
                        appendStringInfo(&defvalbuf, "\'%s\'", coldef->initdefval);
                        initdefval = pstrdup(defvalbuf.data);
                    }
                    defstr = RelationGetColumnDefault(rel, attrForm->attnum, dpcontext, &exprs);
                    if (!coldef->initdefval) {
                        check_alter_table_replident(rel);
                        ObjTree *update_ret = new_objtree_VA(
                            "UPDATE %{identity}D SET %{name}I = %{default}s WHERE %{name}I IS NULL", 3,
                            "identity", ObjTypeObject, new_objtree_for_qualname(rel->rd_rel->relnamespace,
                                                                                RelationGetRelationName(rel)),
                            "name", ObjTypeString, coldef->colname,
                            "default", ObjTypeString, initdefval ? initdefval : defstr);
                        tree_list = lappend(tree_list, update_ret);
                    }

                    ret = new_objtree_VA(
                        "ALTER TABLE %{only}s %{identity}D", 2, "only", ObjTypeString, isonly ? "ONLY" : "", "identity",
                        ObjTypeObject,
                        new_objtree_for_qualname(rel->rd_rel->relnamespace, RelationGetRelationName(rel)));
                    tmp_obj = new_objtree_VA("ALTER COLUMN %{name}I SET DEFAULT %{default}s", 2, "name", ObjTypeString,
                                             coldef->colname, "default", ObjTypeString, defstr);
                    subcmds = lappend(subcmds, new_object_object(tmp_obj));
                    append_array_object(ret, "%{subcmds:, }s", subcmds);
                    tree_list = lappend(tree_list, ret);

                    ReleaseSysCache(attrTup);
                }
                break;
            case AT_ModifyColumn: {
                    /* handle auto_increment attribute */
                    AttrNumber attnum;
                    Oid            typid;
                    int32        typmod;
                    Oid            typcollation;
                    ColumnDef    *coldef = (ColumnDef *) subcmd->def;
                    ListCell     *lc2 = NULL;

                    HeapTuple    attrTup;
                    Form_pg_attribute attrForm;

                    bool saw_autoincrement = false;
                    bool saw_null = false;
                    attrTup = SearchSysCacheAttName(relId, coldef->colname);
                    if (!HeapTupleIsValid(attrTup))
                        elog(ERROR, "could not find cache entry for column \"%s\" of relation %u",
                             coldef->colname, relId);
                    attrForm = (Form_pg_attribute) GETSTRUCT(attrTup);
                    if (!attrForm->atthasdef) {
                        ReleaseSysCache(attrTup);
                        break;
                    }
                    attnum = attrForm->attnum;
                    get_atttypetypmodcoll(RelationGetRelid(rel), attnum, &typid, &typmod, &typcollation);

                    foreach(lc2, coldef->constraints) {
                        Constraint *constr = (Constraint *) lfirst(lc2);

                        if (constr->contype == CONSTR_AUTO_INCREMENT) {
                            saw_autoincrement = true;
                        } else if (constr->contype == CONSTR_NULL) {
                            saw_null = true;
                        }
                    }

                    if (!saw_autoincrement) {
                        break;
                    }

                    if (attrForm->attnotnull) {
                        tmp_obj = adapt_subscription_autoincrement_null_value(rel, coldef, typid);
                        if (tmp_obj)
                            tree_list = lappend(tree_list, tmp_obj);
                    }

                    tmp_obj = new_objtree_VA("MODIFY COLUMN %{colname}I %{coltype}T AUTO_INCREMENT", 2,
                        "colname", ObjTypeString, coldef->colname,
                        "coltype", ObjTypeObject, new_objtree_for_type(typid, typmod));
                    if (saw_null) {
                        append_format_string(tmp_obj, "NULL");
                    }
                    constraint_list = lappend(constraint_list, new_object_object(tmp_obj));
                    ReleaseSysCache(attrTup);
                }
                break;
            case AT_AddIndex: {
                    Oid            idxOid = sub->address.objectId;
                    IndexStmt  *istmt;
                    Relation    idx;
                    const char *idxname;
                    Oid            constrOid;
                    istmt = (IndexStmt *) subcmd->def;
                    if (!istmt->isconstraint || !isrewrite)
                        break;

                    idx = relation_open(idxOid, AccessShareLock);
                    idxname = RelationGetRelationName(idx);

                    constrOid = get_relation_constraint_oid(cmd->d.alterTable.objectId, idxname, false);

                    tmp_obj = new_objtree_VA("ADD CONSTRAINT %{name}I %{definition}s", 3,
                                             "type", ObjTypeString, "add constraint",
                                             "name", ObjTypeString, idxname,
                                             "definition", ObjTypeString,
                                             pg_get_constraintdef_part_string(constrOid));
                    constraint_list = lappend(constraint_list, new_object_object(tmp_obj));

                    relation_close(idx, AccessShareLock);
                }
                break;
            default:
                break;
        }
    }

    if (not_null_list) {
        tree_list = list_concat(tree_list, not_null_list);
    }

    if (constraint_list) {
        tmp_obj = new_objtree_VA("ALTER TABLE %{only}s %{identity}D", 2,
                                 "only", ObjTypeString, isonly ? "ONLY" : "",
                                 "identity", ObjTypeObject,
                                 new_objtree_for_qualname(rel->rd_rel->relnamespace,
                                                          RelationGetRelationName(rel)));
        append_array_object(tmp_obj, "%{subcmds:, }s", constraint_list);
        tree_list = lappend(tree_list, tmp_obj);
    }

    relation_close(rel, AccessShareLock);
    return tree_list;
}

List* deparse_altertable_end(CollectedCommand *cmd)
{
    OverrideSearchPath *overridePath;
    MemoryContext oldcxt;
    MemoryContext tmpcxt;
    ObjTree    *tree;

    List *command_list = NIL;
    List *tree_list = NIL;
    List *res = NIL;
    StringInfoData str;

    if (cmd->type != SCT_AlterTable || !IsA(cmd->parsetree, AlterTableStmt)) {
        return NIL;
    }

    initStringInfo(&str);
    tmpcxt = AllocSetContextCreate(CurrentMemoryContext,
                                   "deparse ctx",
                                   ALLOCSET_DEFAULT_MINSIZE,
                                   ALLOCSET_DEFAULT_INITSIZE,
                                   ALLOCSET_DEFAULT_MAXSIZE);
    oldcxt = MemoryContextSwitchTo(tmpcxt);
    overridePath = GetOverrideSearchPath(CurrentMemoryContext);
    overridePath->schemas = NIL;
    overridePath->addCatalog = false;
    overridePath->addTemp = true;
    PushOverrideSearchPath(overridePath);

    tree_list = deparse_AlterRelation_add_column_default(cmd);
    ListCell* lc = NULL;
    foreach(lc, tree_list) {
        tree = (ObjTree*)lfirst(lc);
        Jsonb       *jsonb;
        char       *command = NULL;
        jsonb = objtree_to_jsonb(tree, NULL);
        command = JsonbToCString(&str, VARDATA(jsonb), JSONB_ESTIMATED_LEN);

        command_list = lappend(command_list, MemoryContextStrdup(oldcxt, command));
        resetStringInfo(&str);
    }

    PopOverrideSearchPath();

    MemoryContextSwitchTo(oldcxt);
    res = list_copy(command_list);
    MemoryContextDelete(tmpcxt);

    return res;
}


/*
 * Handle deparsing of DROP commands.
 *
 * Verbose syntax
 * DROP %s IF EXISTS %%{objidentity}s %{cascade}s
 */
char* deparse_drop_command(const char *objidentity, const char *objecttype,
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
 * Deparse a CreateEnumStmt (CREATE TYPE AS ENUM)
 *
 * Given a Enum type OID and the parse tree that created it, return an ObjTree
 * representing the creation command.
 *
 * Verbose syntax
 * CREATE TYPE %{identity}D AS ENUM (%{values:, }L)
 */
static ObjTree* deparse_CreateEnumStmt(Oid objectId, Node *parsetree)
{
    CreateEnumStmt *node = (CreateEnumStmt *) parsetree;
    List       *values = NIL;
    ListCell   *cell;

    foreach(cell, node->vals) {
        Value *val = (Value*)lfirst(cell);
        values = lappend(values, new_string_object(strVal(val)));
    }

    return new_objtree_VA("CREATE TYPE %{identity}D AS ENUM (%{values:, }L)", 2,
                          "identity", ObjTypeObject,
                          new_objtree_for_qualname_id(TypeRelationId, objectId),
                          "values", ObjTypeArray, values);
}

/*
 * Deparse an AlterObjectSchemaStmt (ALTER ... SET SCHEMA command)
 *
 * Given the object address and the parse tree that created it, return an
 * ObjTree representing the alter command.
 *
 * Verbose syntax
 * ALTER %s %{identity}s SET SCHEMA %{newschema}I
 */
static ObjTree* deparse_AlterObjectSchemaStmt(ObjectAddress address, Node *parsetree,
                                              ObjectAddress old_schema)
{
    AlterObjectSchemaStmt *node = (AlterObjectSchemaStmt *) parsetree;
    char       *identity;
    char       *new_schema = node->newschema;
    char       *old_schname;
    char       *ident;

    /*
     * Since the command has already taken place from the point of view of
     * catalogs, getObjectIdentity returns the object name with the already
     * changed schema.  The output of our deparsing must return the original
     * schema name, however, so we chop the schema name off the identity
     * string and then prepend the quoted schema name.
     *
     * XXX This is pretty clunky. Can we do better?
     */
    identity = getObjectIdentity(&address);
    old_schname = get_namespace_name(old_schema.objectId);
    if (!old_schname)
        elog(ERROR, "cache lookup failed for schema with OID %u",
             old_schema.objectId);

    ident = psprintf("%s%s", quote_identifier(old_schname),
                     identity + strlen(quote_identifier(new_schema)));

    return new_objtree_VA("ALTER %{objtype}s %{identity}s SET SCHEMA %{newschema}I", 3,
                          "objtype", ObjTypeString,
                          string_objtype(node->objectType, false),
                          "identity", ObjTypeString, ident,
                          "newschema", ObjTypeString, new_schema);
}


static ObjTree* deparse_CreateEventStmt(Oid objectId, Node *parsetree)
{
    ObjTree *ret;
    ObjTree *tmp_obj;
    CreateEventStmt *stmt = (CreateEventStmt *) parsetree;
    StringInfoData string_buf;
    initStringInfo(&string_buf);

    char *ev_status = NULL;
    if (stmt->def_name) {
        appendStringInfo(&string_buf, "DEFINER = %s", stmt->def_name);
    }

    if (stmt->event_status == EVENT_DISABLE) {
        ev_status = pstrdup("DISABLE");
    } else if (stmt->event_status == EVENT_DISABLE_ON_SLAVE) {
        ev_status = pstrdup("DISABLE ON SLAVE");
    }

    char *event_name_str = stmt->event_name->relname;
    char *schema_name_str = stmt->event_name->schemaname;

    if (stmt->interval_time) {
        ret = new_objtree_VA(
            "CREATE %{definer_opt}s EVENT %{if_not_exits}s %{schema}s.%{eventname}s "
            "ON SCHEDULE EVERY %{every_interval}s STARTS '%{start_expr}s'", 6,
            "definer_opt", ObjTypeString, stmt->def_name ? pstrdup(string_buf.data) : "",
            "if_not_exits", ObjTypeString, stmt->if_not_exists ? "IF NOT EXISTS" : "",
            "schema", ObjTypeString, schema_name_str,
            "eventname", ObjTypeString, event_name_str,
            "every_interval", ObjTypeString, parseIntervalExprString(stmt->interval_time),
            "start_expr", ObjTypeString, parseTimeExprString(stmt->start_time_expr));
    } else {
        ret = new_objtree_VA(
            "CREATE %{definer_opt}s EVENT %{if_not_exits}s %{schema}s.%{eventname}s "
            "ON SCHEDULE STARTS '%{start_expr}s'", 5,
            "definer_opt", ObjTypeString, stmt->def_name ? pstrdup(string_buf.data) : "",
            "if_not_exits", ObjTypeString, stmt->if_not_exists ? "IF NOT EXISTS" : "",
            "schema", ObjTypeString, schema_name_str,
            "eventname", ObjTypeString, event_name_str,
            "start_expr", ObjTypeString, parseTimeExprString(stmt->start_time_expr));
    }
    if (stmt->end_time_expr) {
        append_string_object(ret, "ENDS '%{end_expr}s'",
                             "end_expr", parseTimeExprString(stmt->end_time_expr));
    }

    tmp_obj = new_objtree_VA("%{opt_ev_on_completion}s %{opt_ev_status}s COMMENT '%{comment_opt}s' DO %{ev_body}s", 4,
        "opt_ev_on_completion", ObjTypeString,
        stmt->complete_preserve ? "ON COMPLETION NOT PRESERVE" : "ON COMPLETION PRESERVE",
        "opt_ev_status", ObjTypeString, ev_status ? ev_status : "",
        "comment_opt", ObjTypeString, stmt->event_comment_str ? stmt->event_comment_str : "",
        "ev_body", ObjTypeString, stmt->event_query_str);

    append_object_object(ret, "%{event_body}s", tmp_obj);

    FreeStringInfo(&string_buf);
    return ret;
}

static ObjTree* deparse_AlterEventStmt(Oid objectId, Node *parsetree)
{
    ObjTree *ret;
    AlterEventStmt *stmt = (AlterEventStmt *) parsetree;
    StringInfoData string_buf;
    initStringInfo(&string_buf);

    char *event_name_str = stmt->event_name->relname;
    char *schema_name_str = stmt->event_name->schemaname;

    if (stmt->def_name) {
        Value *definerVal = (Value *)stmt->def_name->arg;
        appendStringInfo(&string_buf, "DEFINER = %s", strVal(definerVal));
    }

    ret = new_objtree_VA("ALTER %{definer_opt}s EVENT %{schema}s.%{eventname}s ", 3,
        "definer_opt", ObjTypeString, stmt->def_name ? pstrdup(string_buf.data) : "",
        "schema", ObjTypeString, schema_name_str,
        "eventname", ObjTypeString, event_name_str);

    if (stmt->interval_time || stmt->start_time_expr || stmt->end_time_expr) {
        append_format_string(ret, "ON SCHEDULE ");
        if (stmt->interval_time && stmt->interval_time->arg) {
            append_string_object(ret, "EVERY %{every_interval}s ",
                "every_interval", parseIntervalExprString(stmt->interval_time->arg));
        }
        if (stmt->start_time_expr && stmt->start_time_expr->arg) {
            if (stmt->interval_time && stmt->interval_time->arg) {
                append_string_object(ret, "STARTS '%{start_expr}s' ",
                    "start_expr", parseTimeExprString(stmt->start_time_expr->arg));
            } else {
                append_string_object(ret, "AT '%{start_expr}s' ",
                    "start_expr", parseTimeExprString(stmt->start_time_expr->arg));
            }
        }
        if (stmt->end_time_expr && stmt->end_time_expr->arg) {
            append_string_object(ret, "ENDS '%{end_expr}s' ",
                "end_expr", parseTimeExprString(stmt->end_time_expr->arg));
        }
    }

    /* preserve_opt rename_opt status_opt comments_opt action_opt */
    if (stmt->complete_preserve && stmt->complete_preserve->arg) {
        Value *arg = (Value *)stmt->complete_preserve->arg;
        if (!intVal(arg)) {
            append_format_string(ret, "ON COMPLETION PRESERVE ");
        } else {
            append_format_string(ret, "ON COMPLETION NOT PRESERVE ");
        }
    }

    if (stmt->new_name && stmt->new_name->arg) {
        Value *arg = (Value *)stmt->new_name->arg;
        append_string_object(ret, "RENAME TO %{new_name}s ",
            "new_name", strVal(arg));
    }

    if (stmt->event_status && stmt->event_status->arg) {
        Value *arg = (Value *)stmt->event_status->arg;
        EventStatus ev_status = (EventStatus)intVal(arg);
        if (ev_status == EVENT_ENABLE) {
            append_format_string(ret, "ENABLE ");
        } else if (ev_status == EVENT_DISABLE) {
            append_format_string(ret, "DISABLE ");
        } else if (ev_status == EVENT_DISABLE_ON_SLAVE) {
            append_format_string(ret, "DISABLE ON SLAVE ");
        }
    }
    if (stmt->event_comment_str && stmt->event_comment_str->arg) {
        Value *arg = (Value *)stmt->event_comment_str->arg;
        append_string_object(ret, "COMMENT '%{comment}s' ",
            "comment", strVal(arg));
    }
    if (stmt->event_query_str && stmt->event_query_str) {
        Value *arg = (Value *)stmt->event_query_str->arg;
        append_string_object(ret, "DO %{action}s ",
            "action", strVal(arg));
    }

    return ret;
}

static ObjTree* deparse_DropEventStmt(Oid objectId, Node *parsetree)
{
    ObjTree *ret;
    DropEventStmt *stmt = (DropEventStmt *) parsetree;
    char *event_name_str = stmt->event_name->relname;
    char *schema_name_str = stmt->event_name->schemaname;

    ret = new_objtree_VA("DROP EVENT %{if_exists}s %{schema}s.%{eventname}s", 3,
        "if_exists", ObjTypeString, stmt->missing_ok ? "IF EXISTS" : "",
        "schema", ObjTypeString, schema_name_str,
        "eventname", ObjTypeString, event_name_str);

    return ret;
}

/*
 * Handle deparsing of simple commands.
 *
 * This function should cover all cases handled in ProcessUtilitySlow.
 */
static ObjTree* deparse_simple_command(CollectedCommand *cmd, bool *include_owner)
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
        case T_AlterFunctionStmt:
            *include_owner = false;
            return deparse_AlterFunction(objectId, parsetree);
        case T_AlterObjectSchemaStmt:
            *include_owner = false;
            return deparse_AlterObjectSchemaStmt(cmd->d.simple.address,
                                                 parsetree,
                                                 cmd->d.simple.secondaryObject);
        case T_AlterSchemaStmt:
            *include_owner = false;
            return deparse_AlterSchemaStmt(objectId, parsetree);
        case T_AlterSeqStmt:
            *include_owner = false;
            return deparse_AlterSeqStmt(objectId, parsetree);

        case T_CommentStmt:
            *include_owner = false;
            return deparse_CommentStmt(cmd->d.simple.address, parsetree);

        case T_CompositeTypeStmt:
            return deparse_CompositeTypeStmt(objectId, parsetree);

        case T_CreateEnumStmt:    /* CREATE TYPE AS ENUM */
            return deparse_CreateEnumStmt(objectId, parsetree);

        case T_CreateFunctionStmt:
            return deparse_CreateFunction(objectId, parsetree);

        case T_CreateSchemaStmt:
            return deparse_CreateSchemaStmt(objectId, parsetree, include_owner);

        case T_CreateSeqStmt:
            return deparse_CreateSeqStmt(objectId, parsetree);
        case T_CreateStmt:
            return deparse_CreateStmt(objectId, parsetree);
        case T_CreateTrigStmt:
            return deparse_CreateTrigStmt(objectId, parsetree);

        case T_IndexStmt:
            return deparse_IndexStmt(objectId, parsetree);
        case T_RenameStmt:
            *include_owner = false;
            return deparse_RenameStmt(cmd->d.simple.address, parsetree);
        case T_ViewStmt:
            return deparse_ViewStmt(objectId, parsetree);
        case T_CreateEventStmt:
            return deparse_CreateEventStmt(objectId, parsetree);
        case T_AlterEventStmt:
            return deparse_AlterEventStmt(objectId, parsetree);
        case T_DropEventStmt:
            return deparse_DropEventStmt(objectId, parsetree);
        default:
            if (u_sess->hook_cxt.deparseCollectedCommandHook != NULL) {
                return (ObjTree*)((deparseCollectedCommand)(u_sess->hook_cxt.deparseCollectedCommandHook))
                                  (DEPARSE_SIMPLE_COMMAND, cmd, NULL, NULL);
            }
            elog(LOG, "unrecognized node type in deparse command: %d",
                 (int) nodeTag(parsetree));
    }

    return NULL;
}


/*
 * ... ALTER COLUMN ... SET/RESET (...)
 *
 * Verbose syntax
 * ALTER COLUMN %{column}I RESET|SET (%{options:, }s)
 */
static ObjTree* deparse_ColumnSetOptions(AlterTableCmd *subcmd)
{
    List       *sets = NIL;
    ListCell   *cell;
    ObjTree    *ret;
    bool        is_reset = subcmd->subtype == AT_ResetOptions;

    ret = new_objtree_VA("ALTER COLUMN %{column}I %{option}s", 2,
                         "column", ObjTypeString, subcmd->name,
                         "option", ObjTypeString, is_reset ? "RESET" : "SET");

    foreach(cell, (List *) subcmd->def) {
        DefElem    *elem;
        ObjTree    *set;

        elem = (DefElem *) lfirst(cell);
        set = deparse_DefElem(elem, is_reset);
        sets = lappend(sets, new_object_object(set));
    }

    Assert(sets);
    append_array_object(ret, "(%{options:, }s)", sets);

    return ret;
}

/*
 * ... ALTER COLUMN ... SET/RESET (...)
 *
 * Verbose syntax
 * RESET|SET (%{options:, }s)
 */
static ObjTree* deparse_RelSetOptions(AlterTableCmd *subcmd)
{
    List       *sets = NIL;
    ListCell   *cell;
    bool        is_reset = subcmd->subtype == AT_ResetRelOptions;

    foreach(cell, (List *) subcmd->def) {
        DefElem    *elem;
        ObjTree    *set;

        elem = (DefElem *) lfirst(cell);
        set = deparse_DefElem(elem, is_reset);
        sets = lappend(sets, new_object_object(set));
    }

    Assert(sets);

    return new_objtree_VA("%{set_reset}s (%{options:, }s)", 2,
                          "set_reset", ObjTypeString, is_reset ? "RESET" : "SET",
                          "options", ObjTypeArray, sets);
}

/*
 * Deparse all the collected subcommands and return an ObjTree representing the
 * alter command.
 *
 * Verbose syntax
 * ALTER reltype %{only}s %{identity}D %{subcmds:, }s
 */
static ObjTree* deparse_AlterRelation(CollectedCommand *cmd, ddl_deparse_context *context)
{
    ObjTree    *ret;
    ObjTree    *tmp_obj = NULL;
    ObjTree    *tmp_obj2;
    List       *dpcontext;
    Relation    rel;
    List       *subcmds = NIL;
    ListCell   *cell;
    const char *reltype = NULL;
    bool        istype = false;
    bool        istable = false;
    bool        isrewrite = false;
    List       *exprs = NIL;
    Oid            relId = cmd->d.alterTable.objectId;
    AlterTableStmt *stmt = NULL;
    bool        isonly = false;
    ObjTree *loc_obj;

    Assert(cmd->type == SCT_AlterTable);

    stmt = (AlterTableStmt *) cmd->parsetree;
    if (stmt && !IsA(stmt, AlterTableStmt)) {
        return NULL;
    }

    isrewrite = cmd->d.alterTable.rewrite;

    /*
     * ALTER TABLE subcommands generated for TableLikeClause is processed in
     * the top level CREATE TABLE command; return empty here.
     */
    rel = relation_open(relId, AccessShareLock);
    dpcontext = deparse_context_for(RelationGetRelationName(rel),
                                    relId);

    switch (rel->rd_rel->relkind) {
        case RELKIND_RELATION:
            reltype = "TABLE";
            istable = true;
            if (stmt->relation->inhOpt == INH_NO) {
                isonly = true;
            }
            break;
        case RELKIND_INDEX:
        case RELKIND_GLOBAL_INDEX:
            reltype = "INDEX";
            break;
        case RELKIND_VIEW:
            reltype = "VIEW";
            break;
        case RELKIND_COMPOSITE_TYPE:
            reltype = "TYPE";
            istype = true;
            break;
        case RELKIND_FOREIGN_TABLE:
            reltype = "FOREIGN TABLE";
            break;
        case RELKIND_MATVIEW:
            reltype = "MATERIALIZED VIEW";
            break;

        case RELKIND_SEQUENCE:
            reltype = "SEQUENCE";
            break;
        case RELKIND_LARGE_SEQUENCE:
            reltype = "LARGE SEQUENCE";
            break;
        default:
            elog(ERROR, "unexpected relkind %d", rel->rd_rel->relkind);
    }

    ret = new_objtree_VA("ALTER %{objtype}s %{only}s %{identity}D", 3,
                         "objtype", ObjTypeString, reltype,
                         "only", ObjTypeString, isonly ? "ONLY" : "",
                         "identity", ObjTypeObject,
                         new_objtree_for_qualname(rel->rd_rel->relnamespace,
                                                  RelationGetRelationName(rel)));

    foreach(cell, cmd->d.alterTable.subcmds) {
        CollectedATSubcmd *sub = (CollectedATSubcmd *) lfirst(cell);
        AlterTableCmd *subcmd = (AlterTableCmd *) sub->parsetree;
        ObjTree    *tree;

        Assert(IsA(subcmd, AlterTableCmd));

       /*
        * Skip deparse of the subcommand if the objectId doesn't match the
        * target relation ID. It can happen for inherited tables when
        * subcommands for inherited tables and the parent table are both
        * collected in the ALTER TABLE command for the parent table. With the
        * exception of the internally generated AddConstraint (for
        * ALTER TABLE ADD CONSTRAINT FOREIGN KEY REFERENCES) where the
        * objectIds could mismatch (forein table id and the referenced table
        * id).
        */
        if (subcmd->recursing)
            continue;

        switch (subcmd->subtype) {
            case AT_AddColumn:
            case AT_AddColumnRecurse:
                /* XXX need to set the "recurse" bit somewhere? */
                Assert(IsA(subcmd->def, ColumnDef));

                if (istype && subcmd->subtype == AT_AddColumnRecurse) {
                    /* maybe is recurse subcmd for alter type command */
                    break;
                }

                tree = deparse_ColumnDef(rel, dpcontext, false,
                                         (ColumnDef *) subcmd->def, true, &exprs);
                mark_function_volatile(context, (Node*)exprs);
                tmp_obj = new_objtree_VA("ADD %{objtype}s %{if_not_exists}s %{definition}s %{cascade}s", 5,
                                         "objtype", ObjTypeString,
                                         istype ? "ATTRIBUTE" : "COLUMN",
                                         "type", ObjTypeString, "add column",
                                         "if_not_exists", ObjTypeString,
                                         subcmd->missing_ok ? "IF NOT EXISTS" : "",
                                         "definition", ObjTypeObject, tree,
                                         "cascade", ObjTypeString, subcmd->behavior == DROP_CASCADE ? "CASCADE" : "");

                if (subcmd->is_first) {
                    loc_obj = new_objtree("FIRST");
                    append_object_object(tmp_obj, "%{add_first}s", loc_obj);
                } else if (subcmd->after_name) {
                    loc_obj = new_objtree_VA("AFTER %{name}I", 1, "name", ObjTypeString, subcmd->after_name);
                    append_object_object(tmp_obj, "%{add_after_name}s", loc_obj);
                }
                subcmds = lappend(subcmds, new_object_object(tmp_obj));

                break;
            case AT_AddIndex: {
                    Oid            idxOid = sub->address.objectId;
                    IndexStmt  *istmt;
                    Relation    idx;
                    const char *idxname;
                    Oid            constrOid;

                    Assert(IsA(subcmd->def, IndexStmt));
                    istmt = (IndexStmt *) subcmd->def;

                    if (!istmt->isconstraint)
                        break;

                    if (isrewrite)
                        break;

                    idx = relation_open(idxOid, AccessShareLock);
                    idxname = RelationGetRelationName(idx);

                    constrOid = get_relation_constraint_oid(cmd->d.alterTable.objectId, idxname, false);

                    tmp_obj = new_objtree_VA("ADD CONSTRAINT %{name}I %{definition}s", 3,
                                             "type", ObjTypeString, "add constraint",
                                             "name", ObjTypeString, idxname,
                                             "definition", ObjTypeString,
                                             pg_get_constraintdef_part_string(constrOid));
                    subcmds = lappend(subcmds, new_object_object(tmp_obj));

                    relation_close(idx, AccessShareLock);
                }
                break;
            case AT_AddIndexConstraint: {
                    IndexStmt  *istmt;
                    Relation    idx;
                    Oid            constrOid = sub->address.objectId;
                    char         *indexname = NULL;
                    ListCell    *lcell;
                    Assert(IsA(subcmd->def, IndexStmt));
                    istmt = (IndexStmt *) subcmd->def;

                    Assert(istmt->isconstraint && istmt->unique);

                    idx = relation_open(istmt->indexOid, AccessShareLock);

                    foreach (lcell, istmt->options) {
                        DefElem* def = (DefElem*)lfirst(lcell);
                        if (pg_strcasecmp(def->defname, "origin_indexname") == 0) {
                            indexname = defGetString(def);
                            break;
                        }
                    }

                    /*
                        * Verbose syntax
                        *
                        * ADD CONSTRAINT %{name}I %{constraint_type}s USING INDEX
                        * %index_name}I %{deferrable}s %{init_deferred}s
                        */
                    tmp_obj = new_objtree_VA(
                        "ADD CONSTRAINT %{name}I %{constraint_type}s "
                        "USING INDEX %{index_name}I %{deferrable}s %{init_deferred}s", 6,
                        "type", ObjTypeString, "add constraint using index",
                        "name", ObjTypeString, get_constraint_name(constrOid),
                        "constraint_type", ObjTypeString,
                        istmt->primary ? "PRIMARY KEY" : "UNIQUE",
                        "index_name", ObjTypeString,
                        indexname ? indexname : RelationGetRelationName(idx),
                        "deferrable", ObjTypeString,
                        istmt->deferrable ? "DEFERRABLE" : "NOT DEFERRABLE",
                        "init_deferred", ObjTypeString,
                        istmt->initdeferred ? "INITIALLY DEFERRED" : "INITIALLY IMMEDIATE");

                    subcmds = lappend(subcmds, new_object_object(tmp_obj));

                    relation_close(idx, AccessShareLock);
                }
                break;

            case AT_ReAddIndex:
            case AT_ReAddConstraint:
            case AT_ReplaceRelOptions:
                /* Subtypes used for internal operations; nothing to do here */
                break;

            case AT_AddColumnToView:
                /* CREATE OR REPLACE VIEW -- nothing to do here */
                break;

            case AT_ColumnDefault:
                if (subcmd->def == NULL) {
                    tmp_obj = new_objtree_VA("ALTER COLUMN %{column}I DROP DEFAULT", 2,
                                             "type", ObjTypeString, "drop default",
                                             "column", ObjTypeString, subcmd->name);
                } else {
                    List       *dpcontext_rel;
                    HeapTuple    attrtup;
                    AttrNumber    attno;

                    tmp_obj = new_objtree_VA("ALTER COLUMN %{column}I SET DEFAULT", 2,
                                             "type", ObjTypeString, "set default",
                                             "column", ObjTypeString, subcmd->name);

                    dpcontext_rel = deparse_context_for(RelationGetRelationName(rel),
                                                        RelationGetRelid(rel));
                    attrtup = SearchSysCacheAttName(RelationGetRelid(rel), subcmd->name);
                    attno = ((Form_pg_attribute) GETSTRUCT(attrtup))->attnum;
                    append_string_object(tmp_obj, "%{definition}s", "definition",
                                         RelationGetColumnDefault(rel, attno,
                                                                  dpcontext_rel,
                                                                  NULL));
                    ReleaseSysCache(attrtup);
                }

                subcmds = lappend(subcmds, new_object_object(tmp_obj));
                break;

            case AT_DropNotNull:
                tmp_obj = new_objtree_VA("ALTER COLUMN %{column}I DROP NOT NULL", 2,
                                         "type", ObjTypeString, "drop not null",
                                         "column", ObjTypeString, subcmd->name);
                subcmds = lappend(subcmds, new_object_object(tmp_obj));
                break;

            case AT_SetNotNull:
                tmp_obj = new_objtree_VA("ALTER COLUMN %{column}I SET NOT NULL", 2,
                                         "type", ObjTypeString, "set not null",
                                         "column", ObjTypeString, subcmd->name);
                subcmds = lappend(subcmds, new_object_object(tmp_obj));
                break;

            case AT_SetStatistics: {
                    Assert(IsA(subcmd->def, Integer));
                    if (subcmd->additional_property) {
                        tmp_obj = new_objtree_VA("ALTER COLUMN %{column}I SET STATISTICS %{statistics}n", 3,
                                                 "type", ObjTypeString, "set statistics",
                                                 "column", ObjTypeString, subcmd->name,
                                                 "statistics", ObjTypeInteger,
                                                 intVal((Value *)subcmd->def));
                    } else {
                        tmp_obj = new_objtree_VA("ALTER COLUMN %{column}n SET STATISTICS PERCENT %{statistics}n", 4,
                                                 "type", ObjTypeString, "set statistics",
                                                 "column", ObjTypeString, subcmd->name,
                                                 "statistics", ObjTypeInteger,
                                                 intVal((Value *)subcmd->def));
                    }
                    subcmds = lappend(subcmds, new_object_object(tmp_obj));
                }
                break;
            case AT_SetOptions:
            case AT_ResetOptions:
                subcmds = lappend(subcmds, new_object_object(deparse_ColumnSetOptions(subcmd)));
                break;

            case AT_SetStorage:
                Assert(IsA(subcmd->def, String));
                tmp_obj = new_objtree_VA("ALTER COLUMN %{column}I SET STORAGE %{storage}s", 3,
                                         "type", ObjTypeString, "set storage",
                                         "column", ObjTypeString, subcmd->name,
                                         "storage", ObjTypeString,
                                         strVal((Value *)subcmd->def));
                subcmds = lappend(subcmds, new_object_object(tmp_obj));
                break;

            case AT_SET_COMPRESS:
                tmp_obj = new_objtree_VA("SET %{compression}s", 2,
                                         "type", ObjTypeString, "set compression",
                                         "compression", ObjTypeString, subcmd->name);
                subcmds = lappend(subcmds, new_object_object(tmp_obj));
                break;
            case AT_EnableRowMoveMent:
                tmp_obj = new_objtree_VA("ENABLE ROW MOVEMENT", 1,
                                         "type", ObjTypeString, "enable row movement");
                subcmds = lappend(subcmds, new_object_object(tmp_obj));
                break;
            case AT_DisableRowMoveMent:
                tmp_obj = new_objtree_VA("DISABLE ROW MOVEMENT", 1,
                                         "type", ObjTypeString, "disable row movement");
                subcmds = lappend(subcmds, new_object_object(tmp_obj));
                break;
            case AT_SetAutoIncrement:
                tmp_obj = new_objtree_VA("AUTO_INCREMENT", 1,
                                         "type", ObjTypeString, "auto_increment");
                if (subcmd->def && IsA(subcmd->def, Integer)) {
                    append_int_object(tmp_obj, "%{autoincstart}n", (int32)intVal(subcmd->def));
                } else {
                    append_string_object(tmp_obj, "%{autoincstart}s", "autoincstart", strVal(subcmd->def));
                }
                subcmds = lappend(subcmds, new_object_object(tmp_obj));
                break;
            case AT_COMMENTS:
                tmp_obj = new_objtree_VA("COMMENT=%{comment}L", 2,
                                         "type", ObjTypeString, "comment",
                                         "comment", ObjTypeString, subcmd->name);
                subcmds = lappend(subcmds, new_object_object(tmp_obj));
                break;
            case AT_DropColumn:
            case AT_DropColumnRecurse:
                if (istype && subcmd->subtype == AT_DropColumnRecurse) {
                    /* maybe is recurse subcmd for alter type command */
                    break;
                }
                tmp_obj = new_objtree_VA("DROP %{objtype}s %{if_exists}s %{column}I", 4,
                                         "objtype", ObjTypeString,
                                         istype ? "ATTRIBUTE" : "COLUMN",
                                         "type", ObjTypeString, "drop column",
                                         "if_exists", ObjTypeString,
                                         subcmd->missing_ok ? "IF EXISTS" : "",
                                         "column", ObjTypeString, subcmd->name);
                tmp_obj2 = new_objtree_VA("CASCADE", 1,
                                          "present", ObjTypeBool, subcmd->behavior);
                append_object_object(tmp_obj, "%{cascade}s", tmp_obj2);

                subcmds = lappend(subcmds, new_object_object(tmp_obj));
                break;

            case AT_AddConstraint:
            case AT_AddConstraintRecurse: {
                    /* XXX need to set the "recurse" bit somewhere? */
                    Oid            constrOid = sub->address.objectId;
                    bool        isnull;
                    HeapTuple    tup;
                    Datum        val;
                    Constraint *constr;

                    /* Skip adding constraint for inherits table sub command */
                    if (!constrOid)
                        continue;

                    Assert(IsA(subcmd->def, Constraint));
                    constr = castNode(Constraint, subcmd->def);
                    if (!constr->skip_validation) {
                        tup = SearchSysCache1(CONSTROID, ObjectIdGetDatum(constrOid));
                        if (HeapTupleIsValid(tup)) {
                            char       *conbin;

                            /* Fetch constraint expression in parsetree form */
                            val = SysCacheGetAttr(CONSTROID, tup,
                                                  Anum_pg_constraint_conbin, &isnull);

                            if (!isnull) {
                                conbin = TextDatumGetCString(val);
                                exprs = lappend(exprs, stringToNode(conbin));
                                mark_function_volatile(context, (Node *)exprs);
                            }

                            ReleaseSysCache(tup);
                        }
                    }

                    tmp_obj = new_objtree_VA("ADD CONSTRAINT %{name}I %{definition}s", 3,
                                             "type", ObjTypeString, "add constraint",
                                             "name", ObjTypeString, get_constraint_name(constrOid),
                                             "definition", ObjTypeString,
                                             pg_get_constraintdef_part_string(constrOid));
                    subcmds = lappend(subcmds, new_object_object(tmp_obj));
                }
                break;

            case AT_ValidateConstraint:
            case AT_ValidateConstraintRecurse:
                tmp_obj = new_objtree_VA("VALIDATE CONSTRAINT %{constraint}I", 2,
                                         "type", ObjTypeString, "validate constraint",
                                         "constraint", ObjTypeString, subcmd->name);
                subcmds = lappend(subcmds, new_object_object(tmp_obj));
                break;

            case AT_DropConstraint:
            case AT_DropConstraintRecurse:
                if (subcmd->name == NULL && u_sess->attr.attr_sql.dolphin) {
                    tmp_obj = new_objtree_VA("DROP PRIMARY KEY", 1,
                                             "type", ObjTypeString, "drop primary key");
                    subcmds = lappend(subcmds, new_object_object(tmp_obj));
                } else {
                    tmp_obj = new_objtree_VA("DROP CONSTRAINT %{if_exists}s %{constraint}I %{cascade}s", 4,
                                             "type", ObjTypeString, "drop constraint",
                                             "if_exists", ObjTypeString,
                                             subcmd->missing_ok ? "IF EXISTS" : "",
                                             "constraint", ObjTypeString, subcmd->name,
                                             "cascade", ObjTypeString,
                                             subcmd->behavior == DROP_CASCADE ? "CASCADE" : "");
                    subcmds = lappend(subcmds, new_object_object(tmp_obj));
                }
                break;

            case AT_AlterColumnType: {
                    Form_pg_attribute att;
                    ColumnDef *def;
                    HeapTuple heapTup;
                    int attnum = 0;
                    if (istype && (relId != sub->address.objectId)) {
                        /* recurse to cascade table for alter type */
                        break;
                    }
                    /* attrnum may be change by modify */
                    heapTup = SearchSysCacheCopyAttName(RelationGetRelid(rel), subcmd->name);
                    if (!HeapTupleIsValid(heapTup)) /* shouldn't happen */
                        ereport(ERROR,
                                (errmsg("column \"%s\" of relation \"%s\" does not exist",
                                    subcmd->name, RelationGetRelationName(rel))));
                    att = (Form_pg_attribute) GETSTRUCT(heapTup);
                    attnum = att->attnum;

                    def = (ColumnDef *) subcmd->def;
                    Assert(IsA(def, ColumnDef));

                    /*
                     * Verbose syntax
                     *
                     * Composite types: ALTER reltype %{column}I SET DATA TYPE
                     * %{datatype}T %{collation}s ATTRIBUTE %{cascade}s
                     *
                     * Normal types: ALTER reltype %{column}I SET DATA TYPE
                     * %{datatype}T %{collation}s COLUMN %{using}s
                     */
                    tmp_obj = new_objtree_VA("ALTER %{objtype}s %{column}I SET DATA TYPE %{datatype}T", 4,
                                             "objtype", ObjTypeString,
                                             istype ? "ATTRIBUTE" : "COLUMN",
                                             "type", ObjTypeString, "alter column type",
                                             "column", ObjTypeString, subcmd->name,
                                             "datatype", ObjTypeObject,
                                             new_objtree_for_type(att->atttypid,
                                                                  att->atttypmod));

                    /* Add a COLLATE clause, if needed */
                    tmp_obj2 = new_objtree("COLLATE");
                    if (OidIsValid(att->attcollation)) {
                        ObjTree    *collname;

                        collname = new_objtree_for_qualname_id(CollationRelationId,
                                                               att->attcollation);
                        append_object_object(tmp_obj2, "%{name}D", collname);
                    } else {
                        append_not_present(tmp_obj2, "%{name}D");
                    }
                    append_object_object(tmp_obj, "%{collation}s", tmp_obj2);

                    /* If not a composite type, add the USING clause */
                    if (!istype) {
                        /*
                         * If there's a USING clause, transformAlterTableStmt
                         * ran it through transformExpr and stored the
                         * resulting node in cooked_default, which we can use
                         * here.
                         */
                        tmp_obj2 = new_objtree("USING");
                        if (def->raw_default && sub->usingexpr) {
                            mark_function_volatile(context, def->cooked_default);

                            if (contain_mutable_functions(def->cooked_default)) {
                                /*
                                 * allow using modify the idntity value only if
                                 * the value is stable, or need to use replident identity
                                 * attr dml change to output change.
                                 */
                                check_alter_table_rewrite_replident_change(rel, attnum, "ALTER TYPE USING");
                            }
                            append_string_object(tmp_obj2, "%{expression}s",
                                                 "expression",
                                                 sub->usingexpr);
                        } else {
                            append_not_present(tmp_obj2, "%{expression}s");
                        }

                        append_object_object(tmp_obj, "%{using}s", tmp_obj2);
                    }

                    /* If it's a composite type, add the CASCADE clause */
                    if (istype) {
                        tmp_obj2 = new_objtree("CASCADE");
                        if (subcmd->behavior != DROP_CASCADE)
                            append_not_present(tmp_obj2, NULL);
                        append_object_object(tmp_obj, "%{cascade}s", tmp_obj2);
                    }

                    subcmds = lappend(subcmds, new_object_object(tmp_obj));
                }
                break;

#ifdef TODOLIST
            case AT_AlterColumnGenericOptions:
                tmp_obj = deparse_FdwOptions((List *) subcmd->def,
                                             subcmd->name);
                subcmds = lappend(subcmds, new_object_object(tmp_obj));
                break;
#endif
            case AT_ChangeOwner:
                tmp_obj = new_objtree_VA("OWNER TO %{owner}I", 2,
                                         "type", ObjTypeString, "change owner",
                                         "owner", ObjTypeString,
                                         "subcmd->newowner");
                subcmds = lappend(subcmds, new_object_object(tmp_obj));
                break;

            case AT_ClusterOn:
                tmp_obj = new_objtree_VA("CLUSTER ON %{index}I", 2,
                                         "type", ObjTypeString, "cluster on",
                                         "index", ObjTypeString, subcmd->name);
                subcmds = lappend(subcmds, new_object_object(tmp_obj));
                break;

            case AT_DropCluster:
                tmp_obj = new_objtree_VA("SET WITHOUT CLUSTER", 1,
                                         "type", ObjTypeString, "set without cluster");
                subcmds = lappend(subcmds, new_object_object(tmp_obj));
                break;

            case AT_DropOids:
                tmp_obj = new_objtree_VA("SET WITHOUT OIDS", 1,
                                         "type", ObjTypeString, "set without oids");
                subcmds = lappend(subcmds, new_object_object(tmp_obj));
                break;

            case AT_SetTableSpace:
                tmp_obj = new_objtree_VA("SET TABLESPACE %{tablespace}I", 2,
                                         "type", ObjTypeString, "set tablespace",
                                         "tablespace", ObjTypeString, subcmd->name);
                subcmds = lappend(subcmds, new_object_object(tmp_obj));
                break;

            case AT_SetRelOptions:
            case AT_ResetRelOptions:
                subcmds = lappend(subcmds, new_object_object(deparse_RelSetOptions(subcmd)));
                break;

            case AT_EnableTrig:
                tmp_obj = new_objtree_VA("ENABLE TRIGGER %{trigger}I", 2,
                                         "type", ObjTypeString, "enable trigger",
                                         "trigger", ObjTypeString, subcmd->name);
                subcmds = lappend(subcmds, new_object_object(tmp_obj));
                break;

            case AT_EnableAlwaysTrig:
                tmp_obj = new_objtree_VA("ENABLE ALWAYS TRIGGER %{trigger}I", 2,
                                         "type", ObjTypeString, "enable always trigger",
                                         "trigger", ObjTypeString, subcmd->name);
                subcmds = lappend(subcmds, new_object_object(tmp_obj));
                break;

            case AT_EnableReplicaTrig:
                tmp_obj = new_objtree_VA("ENABLE REPLICA TRIGGER %{trigger}I", 2,
                                         "type", ObjTypeString, "enable replica trigger",
                                         "trigger", ObjTypeString, subcmd->name);
                subcmds = lappend(subcmds, new_object_object(tmp_obj));
                break;

            case AT_DisableTrig:
                tmp_obj = new_objtree_VA("DISABLE TRIGGER %{trigger}I", 2,
                                         "type", ObjTypeString, "disable trigger",
                                         "trigger", ObjTypeString, subcmd->name);
                subcmds = lappend(subcmds, new_object_object(tmp_obj));
                break;

            case AT_EnableTrigAll:
                tmp_obj = new_objtree_VA("ENABLE TRIGGER ALL", 1,
                                         "type", ObjTypeString, "enable trigger all");
                subcmds = lappend(subcmds, new_object_object(tmp_obj));
                break;

            case AT_DisableTrigAll:
                tmp_obj = new_objtree_VA("DISABLE TRIGGER ALL", 1,
                                         "type", ObjTypeString, "disable trigger all");
                subcmds = lappend(subcmds, new_object_object(tmp_obj));
                break;

            case AT_EnableTrigUser:
                tmp_obj = new_objtree_VA("ENABLE TRIGGER USER", 1,
                                         "type", ObjTypeString, "enable trigger user");
                subcmds = lappend(subcmds, new_object_object(tmp_obj));
                break;

            case AT_DisableTrigUser:
                tmp_obj = new_objtree_VA("DISABLE TRIGGER USER", 1,
                                         "type", ObjTypeString, "disable trigger user");
                subcmds = lappend(subcmds, new_object_object(tmp_obj));
                break;

            case AT_EnableRule:
                tmp_obj = new_objtree_VA("ENABLE RULE %{rule}I", 2,
                                         "type", ObjTypeString, "enable rule",
                                         "rule", ObjTypeString, subcmd->name);
                subcmds = lappend(subcmds, new_object_object(tmp_obj));
                break;

            case AT_EnableAlwaysRule:
                tmp_obj = new_objtree_VA("ENABLE ALWAYS RULE %{rule}I", 2,
                                         "type", ObjTypeString, "enable always rule",
                                         "rule", ObjTypeString, subcmd->name);
                subcmds = lappend(subcmds, new_object_object(tmp_obj));
                break;

            case AT_EnableReplicaRule:
                tmp_obj = new_objtree_VA("ENABLE REPLICA RULE %{rule}I", 2,
                                         "type", ObjTypeString, "enable replica rule",
                                         "rule", ObjTypeString, subcmd->name);
                subcmds = lappend(subcmds, new_object_object(tmp_obj));
                break;

            case AT_DisableRule:
                tmp_obj = new_objtree_VA("DISABLE RULE %{rule}I", 2,
                                         "type", ObjTypeString, "disable rule",
                                         "rule", ObjTypeString, subcmd->name);
                subcmds = lappend(subcmds, new_object_object(tmp_obj));
                break;

            case AT_AddInherit:
                tmp_obj = new_objtree_VA("INHERIT %{parent}D", 2,
                                         "type", ObjTypeString, "inherit",
                                         "parent", ObjTypeObject,
                                         new_objtree_for_qualname_id(RelationRelationId,
                                                                     sub->address.objectId));
                subcmds = lappend(subcmds, new_object_object(tmp_obj));
                break;

            case AT_DropInherit:
                tmp_obj = new_objtree_VA("NO INHERIT %{parent}D", 2,
                                         "type", ObjTypeString, "drop inherit",
                                         "parent", ObjTypeObject,
                                         new_objtree_for_qualname_id(RelationRelationId,
                                                                     sub->address.objectId));
                subcmds = lappend(subcmds, new_object_object(tmp_obj));
                break;

            case AT_AddOf:
                tmp_obj = new_objtree_VA("OF %{type_of}T", 2,
                                         "type", ObjTypeString, "add of",
                                         "type_of", ObjTypeObject,
                                         new_objtree_for_type(sub->address.objectId, -1));
                subcmds = lappend(subcmds, new_object_object(tmp_obj));
                break;

            case AT_DropOf:
                tmp_obj = new_objtree_VA("NOT OF", 1,
                                         "type", ObjTypeString, "not of");
                subcmds = lappend(subcmds, new_object_object(tmp_obj));
                break;

            case AT_ReplicaIdentity:
                tmp_obj = new_objtree_VA("REPLICA IDENTITY", 1, "type", ObjTypeString, "replica identity");
                switch (((ReplicaIdentityStmt *)subcmd->def)->identity_type) {
                    case REPLICA_IDENTITY_DEFAULT:
                        append_string_object(tmp_obj, "%{ident}s", "ident", "DEFAULT");
                        break;
                    case REPLICA_IDENTITY_FULL:
                        append_string_object(tmp_obj, "%{ident}s", "ident", "FULL");
                        break;
                    case REPLICA_IDENTITY_NOTHING:
                        append_string_object(tmp_obj, "%{ident}s", "ident", "NOTHING");
                        break;
                    case REPLICA_IDENTITY_INDEX:
                        tmp_obj2 = new_objtree_VA("USING INDEX %{index}I", 1, "index", ObjTypeString,
                                                  ((ReplicaIdentityStmt *)subcmd->def)->name);
                        append_object_object(tmp_obj, "%{ident}s", tmp_obj2);
                        break;
                }
                subcmds = lappend(subcmds, new_object_object(tmp_obj));
                break;
            case AT_EnableRls:
                tmp_obj = new_objtree_VA("ENABLE ROW LEVEL SECURITY", 1,
                                         "type", ObjTypeString, "enable row security");
                subcmds = lappend(subcmds, new_object_object(tmp_obj));
                break;
            case AT_DisableRls:
                tmp_obj = new_objtree_VA("DISABLE ROW LEVEL SECURITY", 1,
                                         "type", ObjTypeString, "disable row security");
                subcmds = lappend(subcmds, new_object_object(tmp_obj));
                break;

            case AT_ForceRls:
                tmp_obj = new_objtree_VA("FORCE ROW LEVEL SECURITY", 1,
                                         "type", ObjTypeString, "disable row security");
                subcmds = lappend(subcmds, new_object_object(tmp_obj));
                break;

            case AT_NoForceRls:
                tmp_obj = new_objtree_VA("NO FORCE ROW LEVEL SECURITY", 1,
                                         "type", ObjTypeString, "disable row security");
                subcmds = lappend(subcmds, new_object_object(tmp_obj));
                break;
            case AT_InvisibleIndex:
                tmp_obj = new_objtree_VA("ALTER INDEX %{name}I INVISIBLE", 2,
                                         "type", ObjTypeString, "alter index invisible",
                                         "name", ObjTypeString, subcmd->name);
                subcmds = lappend(subcmds, new_object_object(tmp_obj));
                break;
            case AT_VisibleIndex:
                tmp_obj = new_objtree_VA("ALTER INDEX %{name}I VISIBLE", 2,
                                         "type", ObjTypeString, "alter index visible",
                                         "name", ObjTypeString, subcmd->name);
                subcmds = lappend(subcmds, new_object_object(tmp_obj));
                break;
            case AT_SetCharsetCollate: {
                    CharsetCollateOptions *n = (CharsetCollateOptions *) subcmd->def;
                    if (n->charset != PG_INVALID_ENCODING) {
                        tmp_obj = new_objtree_VA("DEFAULT CHARACTER SET = %{charset}s", 2, "type", ObjTypeString,
                                                 "default character set", "charset", ObjTypeString,
                                                 pg_encoding_to_char(n->charset));
                        subcmds = lappend(subcmds, new_object_object(tmp_obj));
                    }
                    if (n->collate) {
                        tmp_obj = new_objtree_VA("COLLATE = %{collate}s", 2, "type", ObjTypeString, "collate",
                                                 "collate", ObjTypeString, n->collate);
                        subcmds = lappend(subcmds, new_object_object(tmp_obj));
                    }
                }
                break;
            case AT_ConvertCharset: {
                CharsetCollateOptions *cc = (CharsetCollateOptions *)subcmd->def;
                if (cc->charset != PG_INVALID_ENCODING) {
                    tmp_obj =
                        new_objtree_VA("CONVERT TO CHARACTER SET %{charset}s", 2, "type", ObjTypeString,
                                       "convert charset", "charset", ObjTypeString, pg_encoding_to_char(cc->charset));
                    subcmds = lappend(subcmds, new_object_object(tmp_obj));
                }
                    if (cc->collate) {
                        tmp_obj = new_objtree_VA("COLLATE = %{collate}s", 2,
                                                 "type", ObjTypeString, "collate",
                                                 "collate", ObjTypeString, cc->collate);
                        subcmds = lappend(subcmds, new_object_object(tmp_obj));
                    }
                }
                break;
            case AT_ModifyColumn: {
                    AttrNumber attnum;
                    Oid            typid;
                    int32        typmod;
                    Oid            typcollation;
                    ColumnDef    *def = (ColumnDef *) subcmd->def;

                    attnum = get_attnum(RelationGetRelid(rel), def->colname);
                    if (attnum == InvalidAttrNumber) {
                        ereport(ERROR,
                            (errcode(ERRCODE_UNDEFINED_COLUMN),
                                errmsg("column \"%s\" of relation \"%s\" does not exist",
                                       def->colname, RelationGetRelationName(rel))));
                    }

                    get_atttypetypmodcoll(RelationGetRelid(rel), attnum, &typid, &typmod, &typcollation);

                    /* MODIFY_P COLUMN ColId Typename opt_charset ColQualList opt_column_options add_column_first_after
                     */
                    if (pg_strcasecmp(subcmd->name, def->colname) == 0) {
                        tmp_obj = new_objtree_VA("MODIFY COLUMN %{column}I", 2,
                                                 "type", ObjTypeString, "modify column",
                                                 "column", ObjTypeString, def->colname);
                    } else {
                        tmp_obj = new_objtree_VA("CHANGE %{ori_column}I %{column}I", 3,
                                                 "type", ObjTypeString, "change coulumn",
                                                 "ori_column", ObjTypeString, subcmd->name,
                                                 "column", ObjTypeString, def->colname);
                    }
                    append_object_object(tmp_obj, "%{datatype}T",
                                         new_objtree_for_type(typid, typmod));

                    if (def->typname->charset != PG_INVALID_ENCODING)
                        append_string_object(tmp_obj, "CHARACTER SET %{charset}s", "charset",
                            pg_encoding_to_char(def->typname->charset));

                    deparse_ColumnDef_constraints(tmp_obj, rel, def, dpcontext, &exprs);

                    if (def->collClause) {
                        append_object_object(tmp_obj, "COLLATE %{name}D",
                            new_objtree_for_qualname_id(CollationRelationId, typcollation));
                    }

                    /* column_options comment will be set by commentStmt in alist */

                    if (subcmd->is_first) {
                        append_format_string(tmp_obj, "FIRST");
                    } else if (subcmd->after_name) {
                        append_string_object(tmp_obj, "AFTER %{col}I", "col", subcmd->after_name);
                    }

                    subcmds = lappend(subcmds, new_object_object(tmp_obj));
                }
                break;
            case AT_AddPartition: {
                    /* ADD PARTITION name  */
                    AddPartitionState *s = (AddPartitionState*)subcmd->def;
                    ListCell *lc;

                    Oid *partkey_types = NULL;
                    Oid *subpartkey_types = NULL;
                    int partkey_num, subpartkey_num = 0;
                    partkey_num = get_partition_key_types(relId, RELKIND_RELATION, &partkey_types);

                    foreach (lc, s->partitionList) {
                        Node* partition = (Node*)lfirst(lc);

                        if (IsA(partition, RangePartitionDefState)) {
                            /* RangePartitionStartEndDefState will transform the RangePartitionDefState list */
                            RangePartitionDefState *p = (RangePartitionDefState *)partition;

                            tmp_obj = new_objtree_VA("ADD PARTITION %{name}I VALUES LESS THAN", 2,
                                                     "type", ObjTypeString, "add partition",
                                                     "name", ObjTypeString, p->partitionName);

                            //----------- parse for maxValueList expr
                            Oid partoid = InvalidOid;
                            List *boundlist = deparse_partition_boudaries(
                                relId, PARTTYPE_PARTITIONED_RELATION, PART_STRATEGY_RANGE,
                                p->partitionName, &partoid, partkey_num, partkey_types);

                            append_array_object(tmp_obj, "(%{maxvalues:, }s)", boundlist);

                            if (p->tablespacename) {
                                append_string_object(tmp_obj, "TABLESPACE %{tblspc}s", "tblspc", p->tablespacename);
                            }

                            if (p->subPartitionDefState) {
                                int subpartkey_num = get_partition_key_types(relId,
                                    PARTTYPE_PARTITIONED_RELATION, &subpartkey_types);

                                deparse_add_subpartition(tmp_obj, partoid, p->subPartitionDefState,
                                    subpartkey_num, subpartkey_types);
                            }
                        } else if (IsA(partition, ListPartitionDefState)) {
                            ListPartitionDefState *p = (ListPartitionDefState*)partition;
                            tmp_obj = new_objtree_VA("ADD PARTITION %{name}I VALUES", 2,
                                                     "type", ObjTypeString, "add partition",
                                                     "name", ObjTypeString, p->partitionName);
                            Oid partoid = InvalidOid;

                            List *boundlist =
                                deparse_partition_boudaries(relId, PARTTYPE_PARTITIONED_RELATION, PART_STRATEGY_LIST,
                                                            p->partitionName, &partoid, partkey_num, partkey_types);
                            append_array_object(tmp_obj, "(%{maxvalues:, }s)", boundlist);

                            if (p->tablespacename) {
                                append_string_object(tmp_obj, "TABLESPACE %{tblspc}s", "tblspc", p->tablespacename);
                            }
                            if (p->subPartitionDefState) {
                                subpartkey_num = get_partition_key_types(relId, PARTTYPE_PARTITIONED_RELATION,
                                    &subpartkey_types);

                                deparse_add_subpartition(tmp_obj, partoid, p->subPartitionDefState,
                                    subpartkey_num, subpartkey_types);
                            }
                        } else if (IsA(partition, HashPartitionDefState)) {
                            HashPartitionDefState* p = (HashPartitionDefState*)partition;
                            tmp_obj = new_objtree_VA("ADD PARTITION %{name}I", 2,
                                                     "type", ObjTypeString, "add partition",
                                                     "name", ObjTypeString, p->partitionName);
                            if (p->tablespacename) {
                                append_string_object(tmp_obj, "TABLESPACE %{tblspc}s", "tblspc", p->tablespacename);
                            }
                        } else {
                            elog(WARNING, "unsupported AddPartitionState %d for partition table", nodeTag(partition));
                            break;
                        }

                        subcmds = lappend(subcmds, new_object_object(tmp_obj));
                    }
                }
                break;
            case AT_AddSubPartition: {
                    AddSubPartitionState *s = (AddSubPartitionState*)subcmd->def;
                    ListCell* lc;

                    tmp_obj = new_objtree_VA("MODIFY PARTITION %{name}I", 2,
                                             "type", ObjTypeString, "modify partition add subpartition",
                                             "name", ObjTypeString, s->partitionName);
                    foreach(lc, s->subPartitionList) {
                        if (IsA(lfirst(lc), RangePartitionDefState)) {
                            RangePartitionDefState *p = (RangePartitionDefState*)lfirst(lc);
                            append_string_object(tmp_obj, "ADD SUBPARTITION %{subpart}I", "subpart", p->partitionName);
                            List *maxvalues = get_range_partition_maxvalues(p->boundary);
                            append_array_object(tmp_obj, "VALUES LESS THAN (%{maxvalues:, }s)", maxvalues);
                            if (p->tablespacename) {
                                append_string_object(tmp_obj, "TABLESPACE %{tblspc}s", "tblspc", p->tablespacename);
                            }
                        } else {
                            ListPartitionDefState *p = (ListPartitionDefState*)lfirst(lc);
                            append_string_object(tmp_obj, "ADD SUBPARTITION %{subpart}I", "subpart", p->partitionName);
                            List *maxvalues = get_list_partition_maxvalues(p->boundary);
                            append_array_object(tmp_obj, "VALUES (%{maxvalues:, }s)", maxvalues);
                            if (p->tablespacename) {
                                append_string_object(tmp_obj, "TABLESPACE %{tblspc}s", "tblspc", p->tablespacename);
                            }
                        }

                        subcmds = lappend(subcmds, new_object_object(tmp_obj));
                    }
                }
                break;

            case AT_DropPartition:
            case AT_DropSubPartition: {
                    tmp_obj = new_objtree_VA(subcmd->subtype == AT_DropPartition ?
                                             "DROP PARTITION" : "DROP SUBPARTITION", 1,
                                             "type", ObjTypeString, "drop partition");
                    if (subcmd->name) {
                        append_string_object(tmp_obj, "%{name}I", "name", subcmd->name);
                    } else {
                        RangePartitionDefState* state = (RangePartitionDefState*)subcmd->def;
                        /* transformPartitionValue for maxvalues const */
                        List *maxvalues = get_range_partition_maxvalues(state->boundary);

                        append_array_object(tmp_obj, "FOR (%{maxvalues:, }s)", maxvalues);
                    }

                    if (subcmd->alterGPI) {
                        append_format_string(tmp_obj, "UPDATE GLOBAL INDEX");
                    }

                    subcmds = lappend(subcmds, new_object_object(tmp_obj));
                }
                break;
            case AT_SetPartitionTableSpace: {
                    if (!subcmd->def) {
                        break;
                    }
                    if (IsA(subcmd->def, RangePartitionDefState)) {
                        RangePartitionDefState *p = (RangePartitionDefState*)subcmd->def;
                        List *maxvalues = get_range_partition_maxvalues(p->boundary);
                        tmp_obj = new_objtree_VA("MOVE PARTITION FOR (%{maxvalues:, }s) TABLESPACE %{tblspc}s", 3,
                                                 "type", ObjTypeString, "move partition",
                                                 "maxvalues", ObjTypeArray, maxvalues,
                                                 "tblspc", ObjTypeString, subcmd->name);
                    } else if (IsA(subcmd->def, RangeVar)) {
                        tmp_obj = new_objtree_VA("MOVE PARTITION %{partition}I TABLESPACE %{tblspc}s", 3,
                                                 "type", ObjTypeString, "move partition",
                                                 "partition", ObjTypeString, ((RangeVar*)subcmd->def)->relname,
                                                 "tblspc", ObjTypeString, subcmd->name);
                    }
                    subcmds = lappend(subcmds, new_object_object(tmp_obj));
                }
                break;
            case AT_TruncatePartition: {
                    if (subcmd->def) {
                        RangePartitionDefState *p = (RangePartitionDefState*)subcmd->def;
                        List *maxvalues = get_range_partition_maxvalues(p->boundary);
                        tmp_obj = new_objtree_VA("TRUNCATE PARTITION FOR (%{maxvalues:, }s)", 2,
                                                 "type", ObjTypeString, "truncate partition",
                                                 "maxvalues", ObjTypeArray, maxvalues);
                    } else {
                        tmp_obj = new_objtree_VA("TRUNCATE PARTITION %{partition}s", 2,
                                                 "type", ObjTypeString, "truncate partition",
                                                 "partition", ObjTypeString, subcmd->name);
                    }

                    if (subcmd->alterGPI) {
                        append_format_string(tmp_obj, "UPDATE GLOBAL INDEX");
                    }

                    subcmds = lappend(subcmds, new_object_object(tmp_obj));
                }
                break;
            case AT_TruncateSubPartition: {
                    if (subcmd->def && IsA(subcmd->def, RangePartitionDefState)) {
                        RangePartitionDefState *p = (RangePartitionDefState*)subcmd->def;
                        List *maxvalues = get_range_partition_maxvalues(p->boundary);

                        tmp_obj = new_objtree_VA("TRUNCATE SUBPARTITION FOR (%{maxvalues:, }s) %{gpi}s", 3,
                                                 "type", ObjTypeString, "truncate subpartition",
                                                 "maxvalues", ObjTypeArray, maxvalues,
                                                 "gpi", ObjTypeString, subcmd->alterGPI ? "UPDATE GLOBAL INDEX" : "");
                    } else {
                        tmp_obj = new_objtree_VA("TRUNCATE SUBPARTITION %{partition}s %{gpi}s", 3,
                                                 "type", ObjTypeString, "truncate subpartition",
                                                 "partition", ObjTypeString, subcmd->name,
                                                 "gpi", ObjTypeString, subcmd->alterGPI ? "UPDATE GLOBAL INDEX" : "");
                    }
                    subcmds = lappend(subcmds, new_object_object(tmp_obj));
                }
                break;
            case AT_ExchangePartition: {
                    if (subcmd->def) {
                        RangePartitionDefState *p = (RangePartitionDefState*)subcmd->def;
                        List *maxvalues = get_range_partition_maxvalues(p->boundary);
                        tmp_obj = new_objtree_VA(
                            "EXCHANGE %{issub}s FOR (%{maxvalues:, }s) WITH TABLE %{exchange_tbl}D", 4,
                            "type", ObjTypeString, "exchange partition",
                            "issub", ObjTypeString,
                            "PARTITION",
                            "maxvalues", ObjTypeArray, maxvalues,
                            "exchange_tbl", ObjTypeObject,
                            new_objtree_for_qualname_rangevar(subcmd->exchange_with_rel));
                    } else {
                        tmp_obj = new_objtree_VA(
                            "EXCHANGE %{issub}s (%{partition}I) WITH TABLE %{exchange_tbl}D", 4,
                            "type", ObjTypeString, "exchange partition",
                            "issub", ObjTypeString,
                            "PARTITION",
                            "partition", ObjTypeString, subcmd->name,
                            "exchange_tbl", ObjTypeObject,
                            new_objtree_for_qualname_rangevar(subcmd->exchange_with_rel));
                    }

                    if (!subcmd->check_validation) {
                        append_format_string(tmp_obj, "WITHOUT VALIDATION");
                    }

                    if (subcmd->exchange_verbose) {
                        append_format_string(tmp_obj, "VERBOSE");
                    }
                    if (subcmd->alterGPI) {
                        append_format_string(tmp_obj, "UPDATE GLOBAL INDEX");
                    }

                    subcmds = lappend(subcmds, new_object_object(tmp_obj));
                }
                break;
            case AT_MergePartition: {
                    ListCell *lc;
                    List *name_list = NIL;
                    foreach(lc, (List*)subcmd->def) {
                        Value* v = (Value*)lfirst(lc);
                        char *name = pstrdup(v->val.str);
                        name_list = lappend(name_list, new_string_object(name));
                    }

                    tmp_obj = new_objtree_VA("MERGE PARTITIONS %{name_list:, }s INTO PARTITION %{partition}s", 3,
                                             "type", ObjTypeString, "merge partition",
                                             "name_list", ObjTypeArray, name_list,
                                             "partition", ObjTypeString, subcmd->name);
                    if (subcmd->target_partition_tablespace) {
                        append_string_object(tmp_obj, "TABLESPACE %{tblspc}s",
                            "tblspc", subcmd->target_partition_tablespace);
                    }
                    if (subcmd->alterGPI) {
                        append_format_string(tmp_obj, "UPDATE GLOBAL INDEX");
                    }

                    subcmds = lappend(subcmds, new_object_object(tmp_obj));
                }
                break;

            case AT_SplitPartition: {
                    SplitPartitionState *s = (SplitPartitionState*)subcmd->def;
                    bool two_partiiton = false;

                    tmp_obj = new_objtree_VA("SPLIT PARTITION", 1,
                                             "type", ObjTypeString, "split partition");
                    if (s->src_partition_name) {
                        append_string_object(tmp_obj, "%{partition}s", "partition",
                                             s->src_partition_name);
                    } else if (s->partition_for_values) {
                        List *maxvalues = get_range_partition_maxvalues(s->partition_for_values);
                        append_array_object(tmp_obj, "FOR (%{maxvalues:, }s)", maxvalues);
                    }

                    if (s->split_point) {
                        List *maxvalues = get_range_partition_maxvalues(s->split_point);
                        append_array_object(tmp_obj, "AT (%{maxvalues:, }s)", maxvalues);

                        if (list_length(s->dest_partition_define_list) == 2) {
                            RangePartitionDefState *p1 =
                                (RangePartitionDefState*)linitial(s->dest_partition_define_list);
                            RangePartitionDefState *p2 =
                                (RangePartitionDefState*)lsecond(s->dest_partition_define_list);

                            ObjTree *split_obj = new_objtree_VA(
                                "PARTITION %{name1}s %{tblspc1}s, PARTITION %{name2}s %{tblspc2}s", 4,
                                "name1", ObjTypeString, p1->partitionName,
                                "tblspc1", ObjTypeString, p1->tablespacename ? p1->tablespacename : "",
                                "name2", ObjTypeString, p2->partitionName,
                                "tblspc2", ObjTypeString, p2->tablespacename ? p2->tablespacename : "");

                            append_object_object(tmp_obj, "INTO (%{split_list}s)", split_obj);
                            two_partiiton = true;
                        }
                    }

                    if (!two_partiiton) {
                        ListCell *lc;
                        List *partlist = NIL;
                        Oid *partkey_types = NULL;
                        int partkey_num = get_partition_key_types(relId, RELKIND_RELATION, &partkey_types);

                        foreach(lc, s->dest_partition_define_list) {
                            ObjTree *part_obj;
                            RangePartitionDefState *p = (RangePartitionDefState*)lfirst(lc);
                            Oid partoid = InvalidOid;
                            part_obj = new_objtree_VA("PARTITION %{partname}s", 1,
                                                      "partname", ObjTypeString, p->partitionName);

                            List *boundlist = deparse_partition_boudaries(relId, PARTTYPE_PARTITIONED_RELATION,
                                PART_STRATEGY_RANGE, p->partitionName, &partoid, partkey_num, partkey_types);

                            append_array_object(part_obj, "VALUES LESS THAN (%{maxvalues:, }s)", boundlist);
                            if (p->tablespacename) {
                                append_string_object(part_obj, "TABLESPACE %{tblspc}s", "tblspc", p->tablespacename);
                            }

                            partlist = lappend(partlist, new_object_object(part_obj));
                        }

                        append_array_object(tmp_obj, "INTO (%{partlist:, }s)", partlist);
                    }

                    if (subcmd->alterGPI) {
                        append_format_string(tmp_obj, "UPDATE GLOBAL INDEX");
                    }

                    subcmds = lappend(subcmds, new_object_object(tmp_obj));
                }
                break;
            case AT_SplitSubPartition: {
                    SplitPartitionState *s = (SplitPartitionState*)subcmd->def;
                    ObjTree *define_list = NULL;
                    tmp_obj = new_objtree_VA("SPLIT SUBPARTITION %{name}s", 2,
                                             "type", ObjTypeString, "split subpartition",
                                             "name", ObjTypeString, s->src_partition_name);

                    if (s->splitType == LISTSUBPARTITIION) {
                        List *maxvalues = get_list_partition_maxvalues(s->newListSubPartitionBoundry);

                        append_array_object(tmp_obj, "VALUES (%{maxvalues:, }s)", maxvalues);

                        if (list_length(s->dest_partition_define_list) == 2) {
                            ListPartitionDefState *p1 = (ListPartitionDefState*)linitial(s->dest_partition_define_list);
                            ListPartitionDefState *p2 = (ListPartitionDefState*)lsecond(s->dest_partition_define_list);

                            define_list = new_objtree_VA(
                                "SUBPARTITION %{name1}s %{tblspc1}s, SUBPARTITION %{name2}s %{tblspc2}s", 4,
                                "name1", ObjTypeString, p1->partitionName,
                                "tblspc1", ObjTypeString, p1->tablespacename ? p1->tablespacename : "",
                                "name2", ObjTypeString, p2->partitionName,
                                "tblspc2", ObjTypeString, p2->tablespacename ? p2->tablespacename : "");
                        }
                        if (define_list) {
                            append_object_object(tmp_obj, "INTO (%{define_list}s)", define_list);
                        }
                    } else if (s->splitType == RANGESUBPARTITIION) {
                        List *maxvalues = get_range_partition_maxvalues(s->split_point);
                        append_array_object(tmp_obj, "AT (%{maxvalues:, }s)", maxvalues);

                        if (list_length(s->dest_partition_define_list) == 2) {
                            RangePartitionDefState *p1 =
                                (RangePartitionDefState*)linitial(s->dest_partition_define_list);
                            RangePartitionDefState *p2 =
                                (RangePartitionDefState*)lsecond(s->dest_partition_define_list);

                            define_list = new_objtree_VA(
                                "SUBPARTITION %{name1}s %{tblspc1}s, SUBPARTITION %{name2}s %{tblspc2}s", 4,
                                "name1", ObjTypeString, p1->partitionName,
                                "tblspc1", ObjTypeString, p1->tablespacename ? p1->tablespacename : "",
                                "name2", ObjTypeString, p2->partitionName,
                                "tblspc2", ObjTypeString, p2->tablespacename ? p2->tablespacename : "");
                            if (define_list) {
                                append_object_object(tmp_obj, "INTO (%{define_list}s)", define_list);
                            }
                        }
                    }

                    if (subcmd->alterGPI) {
                        append_format_string(tmp_obj, "UPDATE GLOBAL INDEX");
                    }

                    subcmds = lappend(subcmds, new_object_object(tmp_obj));
                }
                break;
            case AT_ResetPartitionno: {
                    tmp_obj = new_objtree_VA("RESET PARTITION", 1,
                                             "type", ObjTypeString, "reset partition");
                    subcmds = lappend(subcmds, new_object_object(tmp_obj));
                }
                break;
            case AT_UnusableIndex:
                tmp_obj = new_objtree_VA("UNUSABLE", 1,
                                         "type", ObjTypeString, "unusable index");
                subcmds = lappend(subcmds, new_object_object(tmp_obj));

                break;
            case AT_UnusableIndexPartition:
                tmp_obj = new_objtree_VA("MODIFY PARTITION %{partition_identity}I UNUSABLE ", 2,
                                         "type", ObjTypeString, "unusable partition index",
                                         "partition_identity", ObjTypeString, subcmd->name);
                subcmds = lappend(subcmds, new_object_object(tmp_obj));
                break;
            case AT_UnusableAllIndexOnPartition: {
                    if (!subcmd->def) {
                        tmp_obj = new_objtree_VA("MODIFY %{parttype} %{partition_identity}I UNUSABLE LOCAL INDEXES ", 3,
                                                 "type", ObjTypeString, "unusable all partition index",
                                                 "parttype", ObjTypeString, "PARTITION",
                                                 "partition_identity", ObjTypeString, subcmd->name);
                    } else if (IsA(subcmd->def, RangePartitionDefState)) {
                        RangePartitionDefState *p = (RangePartitionDefState *)subcmd->def;
                        List *maxvalues = get_range_partition_maxvalues(p->boundary);

                        tmp_obj = new_objtree_VA("MODIFY %{parttype} FOR (%{maxvalues:, }s) UNUSABLE LOCAL INDEXES ", 3,
                                                 "type", ObjTypeString, "unusable all partition index",
                                                 "parttype", ObjTypeString, "PARTITION",
                                                 "maxvalues", ObjTypeArray, maxvalues);
                    }
                }
                break;
            case AT_RebuildIndex:
                tmp_obj = new_objtree_VA("REBUILD", 1,
                                         "type", ObjTypeString, "rebuild index");
                subcmds = lappend(subcmds, new_object_object(tmp_obj));
                break;
            case AT_RebuildIndexPartition:
                tmp_obj = new_objtree_VA("REBUILD PARTITION %{partition_identity}I", 2,
                                         "type", ObjTypeString, "rebuild partition index",
                                         "partition_identity", ObjTypeString, subcmd->name);
                subcmds = lappend(subcmds, new_object_object(tmp_obj));
                break;
            case AT_RebuildAllIndexOnPartition:
                tmp_obj = new_objtree_VA("MODIFY PARTITION %{partition_identity}I REBUILD ALL INDEX", 2,
                                         "type", ObjTypeString, "rebuild partition all index",
                                         "partition_identity", ObjTypeString, subcmd->name);
                subcmds = lappend(subcmds, new_object_object(tmp_obj));
                break;
#ifdef TODOLIST
            case AT_GenericOptions:
                tmp_obj = deparse_FdwOptions((List *) subcmd->def, NULL);
                subcmds = lappend(subcmds, new_object_object(tmp_obj));
                break;
#endif
            default:
                if (u_sess->hook_cxt.deparseCollectedCommandHook != NULL) {
                    tmp_obj = (ObjTree*)((deparseCollectedCommand)(u_sess->hook_cxt.deparseCollectedCommandHook))
                        (ALTER_RELATION_SUBCMD, cmd, sub, context);
                    if (tmp_obj) {
                        subcmds = lappend(subcmds, new_object_object(tmp_obj));
                        break;
                   }
                }

                elog(WARNING, "unsupported alter table subtype %d for ddl logical replication",
                    subcmd->subtype);

                break;
        }

        /*
         * We don't support replicating ALTER TABLE which contains volatile
         * functions because It's possible the functions contain DDL/DML in
         * which case these operations will be executed twice and cause
         * duplicate data. In addition, we don't know whether the tables being
         * accessed by these DDL/DML are published or not. So blindly allowing
         * such functions can allow unintended clauses like the tables
         * accessed in those functions may not even exist on the subscriber.
         */
        if (contain_volatile_functions((Node *) exprs))
            elog(ERROR, "ALTER TABLE command using volatile function cannot be replicated");

        /*
         * Clean the list as we already confirmed there is no volatile
         * function.
         */
        list_free(exprs);
        exprs = NIL;
    }

    table_close(rel, AccessShareLock);

    if (list_length(subcmds) == 0)
        return NULL;

    append_array_object(ret, "%{subcmds:, }s", subcmds);

    return ret;
}

/*
 * Workhorse to deparse a CollectedCommand.
 */
char* deparse_utility_command(CollectedCommand *cmd, ddl_deparse_context *context)
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
        case SCT_AlterTable:
            tree = deparse_AlterRelation(cmd, context);
            context->include_owner = false;
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


static char* get_maxvalue_from_const(Const* maxvalue_item, char* default_str)
{
    int16 typlen = 0;
    bool typbyval = false;
    char typalign;
    char typdelim;
    Oid typioparam = InvalidOid;
    Oid outfunc = InvalidOid;
    char* maxvalue;
    char* maxvalue_out;

    if (constIsNull(maxvalue_item)) {
        maxvalue = pstrdup("NULL");
    } else if (constIsMaxValue(maxvalue_item)) {
        maxvalue = pstrdup(default_str);
    } else {
        get_type_io_data(maxvalue_item->consttype,
            IOFunc_output,
            &typlen,
            &typbyval,
            &typalign,
            &typdelim,
            &typioparam,
            &outfunc);
        maxvalue_out =
            DatumGetCString(OidFunctionCall1Coll(outfunc, maxvalue_item->constcollid, maxvalue_item->constvalue));

        if (istypestring(maxvalue_item->consttype)) {
            int nret = 0;
            size_t len = strlen(maxvalue_out) + 3;
            maxvalue = (char*)palloc0(len * sizeof(char));
            nret = snprintf_s(maxvalue, len, len - 1, "\'%s\'", maxvalue_out);
            securec_check_ss(nret, "\0", "\0");
        } else {
            maxvalue = pstrdup(maxvalue_out);
        }
    }

    return maxvalue;
}

static List *get_range_partition_maxvalues(List* boundary)
{
    ListCell *lc;
    List *maxvalues = NIL;

    foreach (lc, boundary) {
        Const *maxvalue_item = (Const*)lfirst(lc);
        char *maxvalue = get_maxvalue_from_const(maxvalue_item, "MAXVALUE");
        if (maxvalue)
            maxvalues = lappend(maxvalues, new_string_object(maxvalue));
    }

    return maxvalues;
}

/* see transformListPartitionValue */
static List *get_list_partition_maxvalues(List *boundary)
{
    ListCell *lc;
    List *maxvalues = NIL;

    foreach (lc, boundary) {
        if (IsA(lfirst(lc), RowExpr)) {
            /* for multi-keys list partition boundary, ((xx,xx),(xx,xx))
             * subpartition's partition key's length should be 1
             */
            RowExpr *r = (RowExpr*)lfirst(lc);
            ListCell *lc2;

            StringInfoData tmpbuf;
            int i = 0;
            initStringInfo(&tmpbuf);

            foreach(lc2, r->args) {
                Const *maxvalue_item = (Const*)lfirst(lc);

                if (i++ > 0) {
                    appendStringInfo(&tmpbuf, ",");
                }

                char *maxvalue = get_maxvalue_from_const(maxvalue_item, "NULL");
                if (maxvalue) {
                    appendStringInfo(&tmpbuf, "%s", maxvalue);
                }
            }
            appendStringInfo(&tmpbuf, ")");
            char* rowstr = pstrdup(tmpbuf.data);
            maxvalues = lappend(maxvalues, new_string_object(rowstr));
        } else {
            /* singel key */
            Const *maxvalue_item = (Const*)lfirst(lc);
            char *maxvalue = get_maxvalue_from_const(maxvalue_item, "DEFAULT");
            if (maxvalue)
                maxvalues = lappend(maxvalues, new_string_object(maxvalue));
        }
    }

    return maxvalues;
}

static int get_partition_key_types(Oid reloid, char parttype, Oid **partkey_types)
{
    Relation relation = NULL;
    ScanKeyData key[2];
    SysScanDesc scan = NULL;
    HeapTuple tuple = NULL;
    bool isnull = false;
    bool isPartExprKeyNull = false;
    int partkeynum = 0;

    relation = heap_open(PartitionRelationId, AccessShareLock);

    ScanKeyInit(&key[0], Anum_pg_partition_parttype, BTEqualStrategyNumber, F_CHAREQ, CharGetDatum(parttype));
    ScanKeyInit(&key[1], Anum_pg_partition_parentid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(reloid));

    scan = systable_beginscan(relation, PartitionParentOidIndexId, true, NULL, 2, key);
    if (HeapTupleIsValid(tuple = systable_getnext(scan))) {
        Datum datum = 0;
        datum = SysCacheGetAttr(PARTRELID, tuple, Anum_pg_partition_partkeyexpr, &isPartExprKeyNull);

        if (!isPartExprKeyNull) {
            Node *partkeyexpr = NULL;
            char *partkeystr = pstrdup(TextDatumGetCString(datum));
            if (partkeystr)
                partkeyexpr = (Node *)stringToNode_skip_extern_fields(partkeystr);
            else
                ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                                errmsg("The partkeystr can't be NULL while getting partition key types")));
            if (!partkeyexpr)
                ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                                errmsg("The partkeyexpr can't be NULL while getting partition key types")));

            partkeynum = 1;
            *partkey_types = (Oid*)palloc0(partkeynum * sizeof(Oid));
            if (partkeyexpr->type == T_OpExpr)
                (*partkey_types)[0] = ((OpExpr*)partkeyexpr)->opresulttype;
            else if (partkeyexpr->type == T_FuncExpr)
                (*partkey_types)[0] = ((FuncExpr*)partkeyexpr)->funcresulttype;
            else
                ereport(ERROR,
                    (errcode(ERRCODE_NODE_ID_MISSMATCH),
                        errmsg("The node type %d is wrong, it must be T_OpExpr or T_FuncExpr", partkeyexpr->type)));
        } else {
            Oid *iPartboundary = NULL;
            datum = SysCacheGetAttr(PARTRELID, tuple, Anum_pg_partition_partkey, &isnull);

            if (isnull) {
                ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                                errmsg("can not find partkey while getting partition key types")));
            } else {
                int2vector *partVec = (int2vector *)DatumGetPointer(datum);
                partkeynum = partVec->dim1;
                iPartboundary = (Oid *)palloc0(partkeynum * sizeof(Oid));
                for (int i = 0; i < partVec->dim1; i++) {
                    iPartboundary[i] = get_atttype(reloid, partVec->values[i]);
                }
                *partkey_types = iPartboundary;
            }
        }
    } else {
        systable_endscan(scan);
        heap_close(relation, AccessShareLock);
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
        errmsg("could not find tuple for partition relation %u", reloid)));
    }

    systable_endscan(scan);
    heap_close(relation, AccessShareLock);

    return partkeynum;
}

static ObjTree *deparse_add_subpartition(ObjTree *ret, Oid partoid, List *subPartitionDefState, int partkeynum,
                                         Oid *partkey_types)
{
    ListCell *subcell;
    List *sublist = NIL;
    ObjTree *subobj = NULL;
    Oid suboid = InvalidOid;

    foreach(subcell, subPartitionDefState) {
        if (IsA((Node*)lfirst(subcell), ListPartitionDefState)) {
            ListPartitionDefState *n = (ListPartitionDefState*)lfirst(subcell);

            List *subboundlist =
                deparse_partition_boudaries(partoid, PARTTYPE_SUBPARTITIONED_RELATION, PART_STRATEGY_LIST,
                                            n->partitionName, &suboid, partkeynum, partkey_types);

            subobj = new_objtree_VA("SUBPARTITION %{name}I VALUES (%{maxvalue:, }s)", 2,
                                    "name", ObjTypeString, n->partitionName,
                                    "maxvalue", ObjTypeArray, subboundlist);
            if (n->tablespacename) {
                append_string_object(subobj, "TABLESPACE %{tblspc}s", "tblspc", n->tablespacename);
            }
        } else if (IsA((Node*)lfirst(subcell), HashPartitionDefState)) {
            HashPartitionDefState *n = (HashPartitionDefState*)lfirst(subcell);

            subobj = new_objtree_VA("SUBPARTITION %{name}I", 1,
                                    "name", ObjTypeString, n->partitionName);
            if (n->tablespacename) {
                append_string_object(subobj, "TABLESPACE %{tblspc}s", "tblspc", n->tablespacename);
            }
        } else if (IsA((Node*)lfirst(subcell), RangePartitionDefState)) {
            RangePartitionDefState *n = (RangePartitionDefState*)lfirst(subcell);

            List *subboundlist =
                deparse_partition_boudaries(partoid, PARTTYPE_SUBPARTITIONED_RELATION, PART_STRATEGY_RANGE,
                                            n->partitionName, &suboid, partkeynum, partkey_types);

            subobj = new_objtree_VA("SUBPARTITION %{name}I VALUES LESS THAN (%{maxvalue:, }s)", 2,
                                    "name", ObjTypeString, n->partitionName,
                                    "maxvalue", ObjTypeArray, subboundlist);
            if (n->tablespacename) {
                append_string_object(subobj, "TABLESPACE %{tblspc}s", "tblspc", n->tablespacename);
            }
        } else {
            elog(ERROR, "unrecognize subpartiiton definition type %d", nodeTag((Node*)lfirst(subcell)));
        }
        sublist = lappend(sublist, new_object_object(subobj));
    }

    append_array_object(ret, "(%{subpartitions:, }s)", sublist);
    return ret;
}

static List *deparse_partition_boudaries(Oid parentoid, char reltype, char strategy, const char *partition_name,
                                         Oid *partoid, int partkeynum, Oid *partkey_types)
{
    List *boundlist = NIL;
    Relation relation = NULL;
    ScanKeyData key[2];
    SysScanDesc scan = NULL;
    HeapTuple tuple = NULL;
    Oid thisoid = InvalidOid;
    char *maxvalue_str = NULL;

    relation = heap_open(PartitionRelationId, AccessShareLock);

    ScanKeyInit(&key[0], Anum_pg_partition_parttype, BTEqualStrategyNumber, F_CHAREQ, CharGetDatum(reltype));
    ScanKeyInit(&key[1], Anum_pg_partition_parentid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(parentoid));
    scan = systable_beginscan(relation, PartitionParentOidIndexId, true, NULL, 2, key);
    while (HeapTupleIsValid(tuple = systable_getnext(scan))) {
        bool datum_is_null;
        Form_pg_partition foundpart = (Form_pg_partition)GETSTRUCT(tuple);
        if (pg_strcasecmp(foundpart->relname.data, partition_name)) {
            continue;
        }

        Datum boundary_datum = SysCacheGetAttr(PARTRELID, tuple, Anum_pg_partition_boundaries, &datum_is_null);
        if (datum_is_null) {
            if (strategy == PART_STRATEGY_LIST) {
                maxvalue_str = pstrdup("DEFAULT");
                boundlist = lappend(boundlist, new_string_object(maxvalue_str));
                thisoid = HeapTupleGetOid(tuple);
                break;
            } else {
                systable_endscan(scan);
                heap_close(relation, AccessShareLock);
                ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                    errmsg("could not find boundaries for partition %s", partition_name)));
            }
        }

        if (strategy == PART_STRATEGY_LIST && partkeynum > 1) {
            /* see unnest function */
            ArrayType* arr = DatumGetArrayTypeP(boundary_datum);

            Datum dt = 0;
            bool isnull = false;

            ArrayIterator it = array_create_iterator(arr, 0);
            while (array_iterate(it, &dt, &isnull)) {
                if (isnull) {
                    // default
                    maxvalue_str = pstrdup("DEFAULT");
                    boundlist = lappend(boundlist, new_string_object(maxvalue_str));
                    continue;
                }

                StringInfoData tmpbuf;
                initStringInfo(&tmpbuf);

                char* partvalue_str = TextDatumGetCString(dt);
                Type targetType = typeidType(TEXTARRAYOID);
                Datum partvalue_array_datum = stringTypeDatum(targetType, partvalue_str, TEXTOID, true);
                ReleaseSysCache(targetType);

                ArrayType* partvalue_array = DatumGetArrayTypeP(partvalue_array_datum);

                Datum partkey_dt = 0;
                bool partkey_isnull = false;
                int it_index = 0;

                appendStringInfo(&tmpbuf, "(");
                ArrayIterator partkey_it = array_create_iterator(partvalue_array, 0);
                while (array_iterate(partkey_it, &partkey_dt, &partkey_isnull)) {
                    // OidInputFunctionCall
                    if (it_index > 0) {
                        appendStringInfo(&tmpbuf, ",");
                    }

                    if (partkey_isnull) {
                        appendStringInfo(&tmpbuf, "NULL");
                        continue;
                    }

                    char *svalue = TextDatumGetCString(partkey_dt);

                    if (istypestring(partkey_types[it_index])) {
                        appendStringInfo(&tmpbuf, "'%s'", svalue);
                    } else {
                        appendStringInfo(&tmpbuf, "%s", svalue);
                    }
                    ++it_index;
                }
                appendStringInfo(&tmpbuf, ")");
                maxvalue_str = pstrdup(tmpbuf.data);
                if (maxvalue_str) {
                    boundlist = lappend(boundlist, new_string_object(maxvalue_str));
                }
            }
        } else {
            List* boundary = untransformPartitionBoundary(boundary_datum);
            ListCell *bcell;
            int i = 0;
            foreach (bcell, boundary) {
                Value* maxvalue = (Value*)lfirst(bcell);

                if (i >= partkeynum) {
                    break;
                }

                if (!PointerIsValid(maxvalue->val.str)) {
                    if (strategy == PART_STRATEGY_RANGE) {
                        maxvalue_str = pstrdup("MAXVALUE");
                    } else {
                        maxvalue_str = pstrdup("DEFAULT");
                    }
                } else if (istypestring(partkey_types[i])) {
                    int nret = 0;
                    size_t len = strlen(maxvalue->val.str) + 3;
                    maxvalue_str = (char *)palloc0(len * sizeof(char));
                    nret = snprintf_s(maxvalue_str, len, len - 1, "\'%s\'", maxvalue->val.str);
                    securec_check_ss(nret, "\0", "\0");
                } else {
                    maxvalue_str = pstrdup(maxvalue->val.str);
                }
                if (strategy == PART_STRATEGY_RANGE)
                    ++i;

                if (maxvalue_str)
                    boundlist = lappend(boundlist, new_string_object(maxvalue_str));
            }
        }

        thisoid = HeapTupleGetOid(tuple);
        break;
    }

    systable_endscan(scan);
    heap_close(relation, AccessShareLock);

    if (!OidIsValid(thisoid)) {
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
        errmsg("could not find subpartition tuple for partition relation %s", partition_name)));
    }

    if (partoid)
        *partoid = thisoid;

    return boundlist;
}

/*
 * Handle deparsing setting of Function
 *
 * Verbose syntax
 * RESET ALL
 * OR
 * SET %{set_name}I TO %{set_value}s
 * OR
 * RESET %{set_name}I
 */
static ObjTree* deparse_FunctionSet(VariableSetKind kind, char *name, char *value)
{
    ObjTree                 *ret;
    struct config_generic   *record;

    if (kind == VAR_RESET_ALL) {
        ret = new_objtree("RESET ALL");
    } else if (kind == VAR_SET_VALUE) {
        ret = new_objtree_VA("SET %{set_name}I", 1,
                             "set_name", ObjTypeString, name);

        /*
         * Some GUC variable names are 'LIST' type and hence must not be
         * quoted.
         */
        record = find_option(name, false, ERROR);
        if (record && (record->flags & GUC_LIST_QUOTE))
            append_string_object(ret, "TO %{set_value}s", "set_value", value);
        else
            append_string_object(ret, "TO %{set_value}L", "set_value", value);
    } else {
        ret = new_objtree_VA("RESET %{set_name}I", 1,
                             "set_name", ObjTypeString, name);
    }

    return ret;
}

/*
 * Deparse a CreateFunctionStmt (CREATE FUNCTION)
 *
 * Given a function OID and the parse tree that created it, return an ObjTree
 * representing the creation command.
 *
 * Verbose syntax
 *
 * CREATE %{or_replace}s FUNCTION %{signature}s RETURNS %{return_type}s
 * LANGUAGE %{transform_type}s %{language}I %{window}s %{volatility}s
 * %{parallel_safety}s %{leakproof}s %{strict}s %{security_definer}s
 * %{cost}s %{rows}s %{support}s %{set_options: }s AS %{objfile}L,
 * %{symbol}L
 */
static ObjTree* deparse_CreateFunction(Oid objectId, Node *parsetree)
{
    ObjTree     *ret;
    char        *func_str;
    size_t      len;

    func_str = pg_get_functiondef_string(objectId);
    len = strlen(func_str);
    if (func_str[len - 1] == '\n' && func_str[len - 2] == '/'
        && func_str[len - 3] == '\n' && func_str[len - 4] == ';') {
        func_str[len - 2] = '\0';
        func_str[len - 1] = '\0';
    }

    ret = new_objtree_VA("%{function}s", 1,
                         "function", ObjTypeString,
                         func_str);

    return ret;
}

/*
 * Deparse an AlterFunctionStmt (ALTER FUNCTION/ROUTINE/PROCEDURE)
 *
 * Given a function OID and the parse tree that created it, return an ObjTree
 * representing the alter command.
 *
 * Verbose syntax:
 * ALTER FUNCTION/ROUTINE/PROCEDURE %{signature}s %{definition: }s
 */
static ObjTree* deparse_AlterFunction(Oid objectId, Node *parsetree)
{
    AlterFunctionStmt *node = (AlterFunctionStmt *) parsetree;
    ObjTree    *ret;
    ObjTree    *sign;
    HeapTuple   procTup;
    Form_pg_proc procForm;
    List       *params = NIL;
    List       *elems = NIL;
    ListCell   *cell;
    int         i;

    /* Get the pg_proc tuple */
    procTup = SearchSysCache1(PROCOID, objectId);
    if (!HeapTupleIsValid(procTup))
        elog(ERROR, "cache lookup failed for function with OID %u", objectId);
    procForm = (Form_pg_proc) GETSTRUCT(procTup);

    if (node->isProcedure)
        ret = new_objtree("ALTER PROCEDURE");
    else
        ret = new_objtree("ALTER FUNCTION");

    /*
     * ALTER FUNCTION does not change signature so we can use catalog to get
     * input type Oids.
     */
    for (i = 0; i < procForm->pronargs; i++) {
        ObjTree    *tmp_obj;

        tmp_obj = new_objtree_VA("%{type}T", 1,
                                 "type", ObjTypeObject,
                                 new_objtree_for_type(procForm->proargtypes.values[i], -1));
        params = lappend(params, new_object_object(tmp_obj));
    }

    sign = new_objtree_VA("%{identity}D (%{arguments:, }s)", 2,
                          "identity", ObjTypeObject,
                          new_objtree_for_qualname_id(ProcedureRelationId, objectId),
                          "arguments", ObjTypeArray, params);

    append_object_object(ret, "%{signature}s", sign);

    foreach(cell, node->actions) {
        DefElem    *defel = (DefElem *) lfirst(cell);
        ObjTree    *tmp_obj = NULL;

        if (strcmp(defel->defname, "volatility") == 0) {
            tmp_obj = new_objtree(strVal(defel->arg));
        } else if (strcmp(defel->defname, "strict") == 0) {
            tmp_obj = new_objtree(intVal(defel->arg) ?
                                  "RETURNS NULL ON NULL INPUT" :
                                  "CALLED ON NULL INPUT");
        } else if (strcmp(defel->defname, "security") == 0) {
            tmp_obj = PLSQL_SECURITY_DEFINER ? new_objtree(intVal(defel->arg) ?
                                                           "AUTHID DEFINER" : "AUTHID CURRENT_USER") :
                                               new_objtree(intVal(defel->arg) ?
                                               "SECURITY DEFINER" : "SECURITY INVOKER");
        } else if (strcmp(defel->defname, "leakproof") == 0) {
            tmp_obj = new_objtree(intVal(defel->arg) ?
                                  "LEAKPROOF" : "NOT LEAKPROOF");
        } else if (strcmp(defel->defname, "cost") == 0) {
            tmp_obj = new_objtree_VA("COST %{cost}n", 1,
                                     "cost", ObjTypeFloat,
                                     defGetNumeric(defel));
        } else if (strcmp(defel->defname, "rows") == 0) {
            tmp_obj = new_objtree("ROWS");
            if (defGetNumeric(defel) == 0)
                append_not_present(tmp_obj, "%{rows}n");
            else
                append_float_object(tmp_obj, "%{rows}n",
                                    defGetNumeric(defel));
        } else if (strcmp(defel->defname, "set") == 0) {
            VariableSetStmt *sstmt = (VariableSetStmt *) defel->arg;
            char       *value = ExtractSetVariableArgs(sstmt);

            tmp_obj = deparse_FunctionSet(sstmt->kind, sstmt->name, value);
        } else if (strcmp(defel->defname, "parallel") == 0) {
            tmp_obj = new_objtree_VA("PARALLEL %{value}s", 1,
                                     "value", ObjTypeString, strVal(defel->arg));
        }

        elems = lappend(elems, new_object_object(tmp_obj));
    }

    append_array_object(ret, "%{definition: }s", elems);

    ReleaseSysCache(procTup);

    return ret;
}

/*
 * Deparse a CreateTrigStmt (CREATE TRIGGER)
 *
 * Given a trigger OID and the parse tree that created it, return an ObjTree
 * representing the creation command.
 *
 * Verbose syntax
 * CREATE %{constraint}s TRIGGER %{name}I %{time}s %{events: OR }s ON
 * %{relation}D %{from_table}s %{constraint_attrs: }s %{referencing: }s
 * FOR EACH %{for_each}s %{when}s EXECUTE PROCEDURE %{function}s
 */
static ObjTree* deparse_CreateTrigStmt(Oid objectId, Node *parsetree)
{
    CreateTrigStmt *node = (CreateTrigStmt *) parsetree;
    Relation    pg_trigger;
    HeapTuple   trigTup;
    Form_pg_trigger trigForm;
    ObjTree    *ret;
    ObjTree    *tmp_obj;
    int         tgnargs;
    List       *list = NIL;
    List       *events;
    char       *trigtiming;
    Datum       value;
    bool        isnull;
    char        *bodySrc;

    pg_trigger = table_open(TriggerRelationId, AccessShareLock);

    trigTup = get_catalog_object_by_oid(pg_trigger, objectId);
    trigForm = (Form_pg_trigger) GETSTRUCT(trigTup);

    trigtiming = (char*)(node->timing == TRIGGER_TYPE_BEFORE ? "BEFORE" :
        node->timing == TRIGGER_TYPE_AFTER ? "AFTER" :
        node->timing == TRIGGER_TYPE_INSTEAD ? "INSTEAD OF" :
        NULL);
    if (!trigtiming)
        elog(ERROR, "unrecognized trigger timing type %d", node->timing);

    /* DEFINER clause */
    tmp_obj = new_objtree("DEFINER");
    if (node->definer)
        append_string_object(tmp_obj, "=%{definer}s", "definer",
                             node->definer);
    else
        append_not_present(tmp_obj, "=%{definer}s");

    ret = new_objtree_VA("CREATE %{definer}s %{constraint}s TRIGGER %{if_not_exists}s %{name}I %{time}s", 5,
                         "definer", ObjTypeObject, tmp_obj,
                         "constraint", ObjTypeString, node->isconstraint ? "CONSTRAINT" : "",
                         "if_not_exists", ObjTypeString, node->if_not_exists ? "IF NOT EXISTS" : "",
                         "name", ObjTypeString, node->trigname,
                         "time", ObjTypeString, trigtiming);

    /*
     * Decode the events that the trigger fires for.  The output is a list; in
     * most cases it will just be a string with the event name, but when
     * there's an UPDATE with a list of columns, we return a JSON object.
     */
    events = NIL;
    if (node->events & TRIGGER_TYPE_INSERT)
        events = lappend(events, new_string_object("INSERT"));
    if (node->events & TRIGGER_TYPE_DELETE)
        events = lappend(events, new_string_object("DELETE"));
    if (node->events & TRIGGER_TYPE_TRUNCATE)
        events = lappend(events, new_string_object("TRUNCATE"));
    if (node->events & TRIGGER_TYPE_UPDATE) {
        if (node->columns == NIL) {
            events = lappend(events, new_string_object("UPDATE"));
        } else {
            ObjTree    *update;
            ListCell   *cell;
            List       *cols = NIL;

            /*
             * Currently only UPDATE OF can be objects in the output JSON, but
             * we add a "kind" element so that user code can distinguish
             * possible future new event types.
             */
            update = new_objtree_VA("UPDATE OF", 1,
                                    "kind", ObjTypeString, "update_of");

            foreach(cell, node->columns) {
                char       *colname = strVal(lfirst(cell));

                cols = lappend(cols, new_string_object(colname));
            }

            append_array_object(update, "%{columns:, }I", cols);

            events = lappend(events, new_object_object(update));
        }
    }
    append_array_object(ret, "%{events: OR }s", events);

    tmp_obj = new_objtree_for_qualname_id(RelationRelationId,
                                          trigForm->tgrelid);
    append_object_object(ret, "ON %{relation}D", tmp_obj);

    tmp_obj = new_objtree("FROM");
    if (trigForm->tgconstrrelid) {
        ObjTree    *rel;

        rel = new_objtree_for_qualname_id(RelationRelationId,
                                          trigForm->tgconstrrelid);
        append_object_object(tmp_obj, "%{relation}D", rel);
    } else {
        append_not_present(tmp_obj, "%{relation}D");
    }
    append_object_object(ret, "%{from_table}s", tmp_obj);

    if (node->isconstraint) {
        if (!node->deferrable)
            list = lappend(list, new_string_object("NOT"));
        list = lappend(list, new_string_object("DEFERRABLE INITIALLY"));
        if (node->initdeferred)
            list = lappend(list, new_string_object("DEFERRED"));
        else
            list = lappend(list, new_string_object("IMMEDIATE"));
    }
    append_array_object(ret, "%{constraint_attrs: }s", list);

    append_string_object(ret, "FOR EACH %{for_each}s", "for_each",
                         node->row ? "ROW" : "STATEMENT");

    tmp_obj = new_objtree("WHEN");
    if (node->whenClause) {
        Node       *whenClause;

        value = fastgetattr(trigTup, Anum_pg_trigger_tgqual,
                            RelationGetDescr(pg_trigger), &isnull);
        if (isnull)
            elog(ERROR, "null tgqual for trigger \"%s\"",
                 NameStr(trigForm->tgname));

        whenClause = (Node*)stringToNode(TextDatumGetCString(value));

        append_string_object(tmp_obj, "(%{clause}s)", "clause",
                             pg_get_trigger_whenclause(trigForm, whenClause, false));
    } else {
        append_not_present(tmp_obj, "%{clause}s");
    }
    append_object_object(ret, "%{when}s", tmp_obj);

    if (node->funcname && !node->funcSource) {
        tmp_obj = new_objtree_VA("%{funcname}D", 1, "funcname", ObjTypeObject,
                                 new_objtree_for_qualname_id(ProcedureRelationId, trigForm->tgfoid));
        list = NIL;
        tgnargs = trigForm->tgnargs;
        if (tgnargs > 0) {
            bytea      *tgargs;
            char       *argstr;
            int         findx;
            int         lentgargs;
            char       *p;

            tgargs = DatumGetByteaP(fastgetattr(trigTup,
                                                Anum_pg_trigger_tgargs,
                                                RelationGetDescr(pg_trigger),
                                                &isnull));
            if (isnull)
                elog(ERROR, "null tgargs for trigger \"%s\"", NameStr(trigForm->tgname));
            argstr = (char *)VARDATA(tgargs);
            lentgargs = VARSIZE_ANY_EXHDR(tgargs);

            p = argstr;
            for (findx = 0; findx < tgnargs; findx++) {
                size_t tlen;

                /* Verify that the argument encoding is correct */
                tlen = strlen(p);
                if (p + tlen >= argstr + lentgargs) {
                    elog(ERROR, "invalid argument string (%s) for trigger \"%s\"", argstr, NameStr(trigForm->tgname));
                }
                list = lappend(list, new_string_object(p));
                p += tlen + 1;
            }
        }

        append_format_string(tmp_obj, "(");
        append_array_object(tmp_obj, "%{args:, }L", list);  /* might be NIL */
        append_format_string(tmp_obj, ")");

        append_object_object(ret, "EXECUTE PROCEDURE %{function}s", tmp_obj);
    }

    if (node->funcSource && node->funcSource->bodySrc) {
        bodySrc = pstrdup(node->funcSource->bodySrc);
        if (u_sess->attr.attr_sql.sql_compatibility == B_FORMAT
            && strlen(bodySrc) > BEGIN_P_LEN
            && pg_strncasecmp(bodySrc, BEGIN_P_STR, BEGIN_P_LEN) == 0) {
            errno_t rc = memcpy_s(bodySrc, strlen(bodySrc), BEGIN_N_STR, BEGIN_P_LEN);
            securec_check(rc, "\0", "\0")
        }
        tmp_obj = new_objtree_VA("%{bodysrc}s", 1,
                                 "bodysrc", ObjTypeString, bodySrc);
    } else {
        tmp_obj = new_objtree_VA("", 1,
                                 "present", ObjTypeBool, false);
    }
    append_object_object(ret, "%{bodysrc}s", tmp_obj);

    table_close(pg_trigger, AccessShareLock);

    return ret;
}