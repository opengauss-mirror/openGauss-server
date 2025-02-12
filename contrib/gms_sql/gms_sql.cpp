#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "commands/extension.h"

#if PG_VERSION_NUM < 120000

#include "access/heapam.h"
#include "access/printtup.h"

#endif
#include "access/transam.h"
#include "access/tupconvert.h"
#include "lib/stringinfo.h"
#include "parser/scansup.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/elog.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/typcache.h"
#include "executor/spi_priv.h"
#include "libpq/libpq.h"
#include "gms_sql.h"

PG_MODULE_MAGIC;

static char *next_token(char *str, char **start, size_t *len, ofTokenType *typ, char **sep, size_t *seplen);

PG_FUNCTION_INFO_V1(gms_sql_is_open);
PG_FUNCTION_INFO_V1(gms_sql_open_cursor);
PG_FUNCTION_INFO_V1(gms_sql_close_cursor);
PG_FUNCTION_INFO_V1(gms_sql_parse);
PG_FUNCTION_INFO_V1(gms_sql_bind_variable);
PG_FUNCTION_INFO_V1(gms_sql_bind_variable_f);
PG_FUNCTION_INFO_V1(gms_sql_bind_array_3);
PG_FUNCTION_INFO_V1(gms_sql_bind_array_5);
PG_FUNCTION_INFO_V1(gms_sql_define_column);
PG_FUNCTION_INFO_V1(gms_sql_define_array);
PG_FUNCTION_INFO_V1(gms_sql_execute);
PG_FUNCTION_INFO_V1(gms_sql_fetch_rows);
PG_FUNCTION_INFO_V1(gms_sql_execute_and_fetch);
PG_FUNCTION_INFO_V1(gms_sql_column_value);
PG_FUNCTION_INFO_V1(gms_sql_column_value_f);
PG_FUNCTION_INFO_V1(gms_sql_last_row_count);
PG_FUNCTION_INFO_V1(gms_sql_describe_columns);
PG_FUNCTION_INFO_V1(gms_sql_describe_columns_f);
PG_FUNCTION_INFO_V1(gms_sql_debug_cursor);
PG_FUNCTION_INFO_V1(gms_sql_return_result);
PG_FUNCTION_INFO_V1(gms_sql_return_result_i);

static uint32 gms_sql_index;

static THR_LOCAL uint64 last_row_count = 0;
static THR_LOCAL TransactionId last_lxid = InvalidTransactionId;
static THR_LOCAL int result_no = 0;

void set_extension_index(uint32 index)
{
    gms_sql_index = index;
}

void init_session_vars(void)
{
    RepallocSessionVarsArrayIfNecessary();

    GmssqlContext* psc =
        (GmssqlContext*)MemoryContextAllocZero(u_sess->self_mem_cxt, sizeof(GmssqlContext));
    u_sess->attr.attr_common.extension_session_vars_array[gms_sql_index] = psc;

    psc->gms_sql_cxt = NULL;
    psc->gms_sql_cursors = NULL;

}

GmssqlContext* get_session_context()
{
    if (u_sess->attr.attr_common.extension_session_vars_array[gms_sql_index] == NULL) {
        init_session_vars();
    }
    return (GmssqlContext*)u_sess->attr.attr_common.extension_session_vars_array[gms_sql_index];
}

static void
create_cursors()
{
    MemoryContext persist_cxt = get_session_context()->gms_sql_cxt;

    if (!persist_cxt) {
        persist_cxt = AllocSetContextCreate(u_sess->top_mem_cxt,
                                            "gms_sql persist context",
                                            ALLOCSET_DEFAULT_SIZES);
        get_session_context()->gms_sql_cxt = persist_cxt;
        if (!get_session_context()->gms_sql_cursors) {
            get_session_context()->gms_sql_cursors = (CursorData*)MemoryContextAllocZero(persist_cxt, u_sess->attr.attr_common.maxOpenCursorCount * sizeof(CursorData));
        }
    }
}
static void
open_cursor(CursorData *cursor, int cid)
{
    cursor->cid = cid;
    MemoryContext persist_cxt = get_session_context()->gms_sql_cxt;

    cursor->cursor_cxt = AllocSetContextCreate(persist_cxt,
                                                "gms_sql cursor context",
                                                ALLOCSET_DEFAULT_SIZES);
    cursor->assigned = true;
}

/*
 * FUNCTION gms_sql.open_cursor() RETURNS int
 */
Datum
gms_sql_open_cursor(PG_FUNCTION_ARGS)
{
    int    i;

    (void)    fcinfo;
    CursorData    *cursors;

    if (get_session_context()->gms_sql_cursors == NULL)
        create_cursors();

    cursors = get_session_context()->gms_sql_cursors;
    /* find and initialize first free slot */
    for (i = 0; i < u_sess->attr.attr_common.maxOpenCursorCount; i++) {
        if(!cursors[i].assigned) {
            open_cursor(&cursors[i], i);

            PG_RETURN_INT32(i);
        }
    }

    ereport(ERROR,
            (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
             errmsg("too many opened cursors"),
             errdetail("There is not free slot for new gms_sql's cursor."),
             errhint("You should to close unused cursors")));

    /* be msvc quiet */
    PG_RETURN_VOID();
}

static CursorData *
get_cursor(FunctionCallInfo fcinfo, bool should_be_assigned)
{
    CursorData        *cursors;
    CursorData       *cursor;
    int                cid;

    if (PG_ARGISNULL(0))
        ereport(ERROR,
                (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                 errmsg("cursor id is NULL")));

    cid = PG_GETARG_INT32(0);
    if (cid < 0 || cid >= u_sess->attr.attr_common.maxOpenCursorCount)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("cursor %d value of cursor id is out of range", cid)));

    cursors = get_session_context()->gms_sql_cursors;
    if (cursors  == NULL)
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_CURSOR),
                 errmsg("cursor is not open")));

    cursor = &cursors[cid];
    if (!cursor->assigned && should_be_assigned)
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_CURSOR),
                 errmsg("cursor is not valid")));

    return cursor;
}

/*
 * CREATE FUNCTION gms_sql.is_open(c int) RETURNS bool;
 */
Datum
gms_sql_is_open(PG_FUNCTION_ARGS)
{
    CursorData    *cursor;

    cursor = get_cursor(fcinfo, false);

    PG_RETURN_BOOL(cursor->assigned);
}

/*
 * Release all sources assigned to cursor
 */
static void
close_cursor(CursorData *cursor)
{
    if (cursor->executed && cursor->portal)
        SPI_cursor_close(cursor->portal);

    /* release all assigned memory */
    if (cursor->cursor_cxt)
        MemoryContextDelete(cursor->cursor_cxt);

    if (cursor->cursor_xact_cxt)
        MemoryContextDelete(cursor->cursor_xact_cxt);

    if (cursor->plan)
        SPI_freeplan(cursor->plan);

    memset_s(cursor, sizeof(CursorData), 0, sizeof(CursorData));

}

/*
 * PROCEDURE gms_sql.close_cursor(c int)
 */
Datum
gms_sql_close_cursor(PG_FUNCTION_ARGS)
{
    CursorData    *cursor;

    cursor = get_cursor(fcinfo, false);

    close_cursor(cursor);

    PG_RETURN_VOID();
}

/*
 * Print state of cursor - just for debug purposes
 */
Datum
gms_sql_debug_cursor(PG_FUNCTION_ARGS)
{
    CursorData    *cursor;
    ListCell        *lc;

    cursor = get_cursor(fcinfo, false);

    if (cursor->assigned) {
        if (cursor->original_query)
            elog(NOTICE, "orig query: \"%s\"", cursor->original_query);

        if (cursor->parsed_query)
            elog(NOTICE, "parsed query: \"%s\"", cursor->parsed_query);

    } else
        elog(NOTICE, "cursor is not assigned");

    foreach(lc, cursor->variables) {
        VariableData *var = (VariableData *) lfirst(lc);

        if (var->typoid != InvalidOid) {
            Oid    typOutput;
            bool    isVarlena;
            char   *str;

            getTypeOutputInfo(var->typoid, &typOutput, &isVarlena);
            str = OidOutputFunctionCall(typOutput, var->value);

            elog(NOTICE, "variable \"%s\" is assigned to \"%s\"", var->refname, str);
        } else
            elog(NOTICE, "variable \"%s\" is not assigned", var->refname);
    }

    foreach(lc, cursor->columns)
    {
        ColumnData *col = (ColumnData *) lfirst(lc);

        elog(NOTICE, "column definition for position %d is %s",
                      col->position,
                      format_type_with_typemod(col->typoid, col->typmod));
    }

    PG_RETURN_VOID();
}

/*
 * Search a variable in cursor's variable list
 */
static VariableData *
get_var(CursorData *cursor, char *refname, int position, bool append)
{
    ListCell        *lc;
    VariableData    *nvar;
    MemoryContext    oldcxt;

    foreach(lc, cursor->variables) {
        VariableData *var = (VariableData *) lfirst(lc);

        if (strcmp(var->refname, refname) == 0)
            return var;
    }

    if (append) {
        oldcxt = MemoryContextSwitchTo(cursor->cursor_cxt);
        nvar = (VariableData*)palloc0(sizeof(VariableData));

        nvar->refname = pstrdup(refname);
        nvar->varno = cursor->nvariables + 1;
        nvar->position = position;

        cursor->variables = lappend(cursor->variables, nvar);
        cursor->nvariables += 1;

        MemoryContextSwitchTo(oldcxt);

        return nvar;
    } else
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_PARAMETER),
                 errmsg("variable \"%s\" doesn't exists", refname)));

    /* be msvc quite */
    return NULL;
}

/*
 * PROCEDURE gms_sql.parse(c int, stmt varchar)
 */
Datum
gms_sql_parse(PG_FUNCTION_ARGS)
{
    char    *query,*ptr;
    char    *start;
    size_t    len;
    ofTokenType    typ;
    StringInfoData    sinfo;
    CursorData         *cursor;
    MemoryContext         oldcxt;

    cursor = get_cursor(fcinfo, true);

    if (PG_ARGISNULL(1))
        ereport(ERROR,
                (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                 errmsg("parsed query string is NULL")));

    if (cursor->parsed_query) {
        int    cid = cursor->cid;

        close_cursor(cursor);
        open_cursor(cursor, cid);
    }

    query = text_to_cstring(PG_GETARG_TEXT_P(1));
    ptr = query;

    initStringInfo(&sinfo);

    while (ptr) {
        char    *startsep;
        char    *next_ptr;
        size_t    seplen;

        next_ptr = next_token(ptr, &start, &len, &typ, &startsep, &seplen);
        if (next_ptr) {
            if (typ == TOKEN_DOLAR_STR) {
                appendStringInfo(&sinfo, "%.*s", (int) seplen, startsep);
                appendStringInfo(&sinfo, "%.*s", (int) len, start);
                appendStringInfo(&sinfo, "%.*s", (int) seplen, startsep);
            } else if (typ == TOKEN_BIND_VAR) {
                char       *name = downcase_truncate_identifier(start, (int) len, false);
                VariableData *var = get_var(cursor, name, (int) (ptr - query), true);

                appendStringInfo(&sinfo, "$%d", var->varno);

                pfree(name);
            } else if (typ == TOKEN_EXT_STR) {
                appendStringInfo(&sinfo, "e\'%.*s\'", (int) len, start);
            } else if (typ == TOKEN_STR) {
                appendStringInfo(&sinfo, "\'%.*s\'", (int) len, start);
            } else if (typ == TOKEN_QIDENTIF) {
                appendStringInfo(&sinfo, "\"%.*s\"", (int) len, start);
            } else if (typ != TOKEN_NONE) {
                appendStringInfo(&sinfo, "%.*s", (int) len, start);
            }
        }

        ptr = next_ptr;
    }

    /* save result to persist context */
    oldcxt = MemoryContextSwitchTo(cursor->cursor_cxt);
    cursor->original_query = pstrdup(query);
    cursor->parsed_query = pstrdup(sinfo.data);

    MemoryContextSwitchTo(oldcxt);

    pfree(query);
    pfree(sinfo.data);

    PG_RETURN_VOID();
}

/*
 * Calling procedure can be slow, so there is a function alternative
 */
static Datum
bind_variable(PG_FUNCTION_ARGS)
{
    CursorData    *cursor;
    VariableData    *var;
    char *varname, *varname_downcase;
    Oid            valtype;
    bool            is_unknown = false;

    cursor = get_cursor(fcinfo, true);

    if (PG_ARGISNULL(1))
        ereport(ERROR,
                (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                 errmsg("name of bind variable is NULL")));

    varname = text_to_cstring(PG_GETARG_TEXT_P(1));
    if (*varname == ':')
        varname += 1;

    varname_downcase = downcase_truncate_identifier(varname, (int) strlen(varname), false);
    var = get_var(cursor, varname_downcase, -1, false);

    valtype = get_fn_expr_argtype(fcinfo->flinfo, 2);
    if (valtype == RECORDOID)
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("cannot to bind a value of record type")));

    valtype = getBaseType(valtype);
    if (valtype == UNKNOWNOID) {
        is_unknown = true;
        valtype = TEXTOID;
    }

    if (var->typoid != InvalidOid) {
        if (!var->typbyval)
            pfree(DatumGetPointer(var->value));

        var->isnull = true;
    }

    var->typoid = valtype;

    if (!PG_ARGISNULL(2)) {
        MemoryContext    oldcxt;

        get_typlenbyval(var->typoid, &var->typlen, &var->typbyval);

        oldcxt = MemoryContextSwitchTo(cursor->cursor_cxt);

        if (is_unknown)
            var->value = CStringGetTextDatum(DatumGetPointer(PG_GETARG_DATUM(2)));
        else
            var->value = datumCopy(PG_GETARG_DATUM(2), var->typbyval, var->typlen);

        var->isnull = false;

        MemoryContextSwitchTo(oldcxt);
    } else
        var->isnull = true;

    PG_RETURN_VOID();
}

/*
 * CREATE PROCEDURE gms_sql.bind_variable(c int, name varchar2, value "any");
 */
Datum
gms_sql_bind_variable(PG_FUNCTION_ARGS)
{
    return bind_variable(fcinfo);
}

/*
 * CREATE FUNCTION gms_sql.bind_variable_f(c int, name varchar2, value "any") RETURNS void;
 */
Datum
gms_sql_bind_variable_f(PG_FUNCTION_ARGS)
{
    return bind_variable(fcinfo);
}

static void
bind_array(FunctionCallInfo fcinfo, int index1, int index2)
{
    CursorData     *cursor;
    VariableData     *var;
    char *varname, *varname_downcase;
    Oid            valtype;
    Oid            elementtype;
    bool            is_unknown = false;

    cursor = get_cursor(fcinfo, true);

    if (PG_ARGISNULL(1))
        ereport(ERROR,
                (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                 errmsg("name of bind variable is NULL")));

    varname = text_to_cstring(PG_GETARG_TEXT_P(1));
    if (*varname == ':')
        varname += 1;

    varname_downcase = downcase_truncate_identifier(varname, (int) strlen(varname), false);
    var = get_var(cursor, varname_downcase, -1, false);

    valtype = get_fn_expr_argtype(fcinfo->flinfo, 2);
    if (valtype == RECORDOID)
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("cannot to bind a value of record type")));

    valtype = getBaseType(valtype);
    elementtype = get_element_type(valtype);

    if (!OidIsValid(elementtype))
        ereport(ERROR,
                (errcode(ERRCODE_DATATYPE_MISMATCH),
                 errmsg("value is not a array")));

    var->is_array = true;
    var->typoid = valtype;
    var->typelemid = elementtype;

    get_typlenbyval(elementtype, &var->typelemlen, &var->typelembyval);

    if (!PG_ARGISNULL(2)) {
        MemoryContext    oldcxt;

        get_typlenbyval(var->typoid, &var->typlen, &var->typbyval);

        oldcxt = MemoryContextSwitchTo(cursor->cursor_cxt);

        if (is_unknown)
            var->value = CStringGetTextDatum(DatumGetPointer(PG_GETARG_DATUM(2)));
        else
            var->value = datumCopy(PG_GETARG_DATUM(2), var->typbyval, var->typlen);

        var->isnull = false;

        MemoryContextSwitchTo(oldcxt);
    } else
        var->isnull = true;

    var->index1 = index1;
    var->index2 = index2;
}

/*
 * CREATE PROCEDURE gms_sql.bind_array(c int, name varchar2, value anyarray);
 */
Datum
gms_sql_bind_array_3(PG_FUNCTION_ARGS)
{
    bind_array(fcinfo, -1, -1);

    PG_RETURN_VOID();
}

/*
 * CREATE PROCEDURE gms_sql.bind_array(c int, name varchar2, value anyarray, index1 int, index2 int);
 */
Datum
gms_sql_bind_array_5(PG_FUNCTION_ARGS)
{
    int    index1, index2;

    if (PG_ARGISNULL(3) || PG_ARGISNULL(4))
        ereport(ERROR,
                (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                 errmsg("index is NULL")));

    index1 = PG_GETARG_INT32(3);
    index2 = PG_GETARG_INT32(4);

    if (index1 < 0 || index2 < 0)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("index is below zero")));

    if (index1 > index2)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("index1 is greater than index2")));

    bind_array(fcinfo, index1, index2);

    PG_RETURN_VOID();
}

static ColumnData *
get_col(CursorData *cursor, int position, bool append)
{
    ListCell        *lc;
    ColumnData        *ncol;
    MemoryContext        oldcxt;

    foreach(lc, cursor->columns) {
        ColumnData *col = (ColumnData *) lfirst(lc);

        if (col->position == position)
            return col;
    }

    if (append) {
        oldcxt = MemoryContextSwitchTo(cursor->cursor_cxt);
        ncol = (ColumnData*)palloc0(sizeof(ColumnData));

        ncol->position = position;
        if (cursor->max_colpos < position)
            cursor->max_colpos = position;

        cursor->columns = lappend(cursor->columns, ncol);

        MemoryContextSwitchTo(oldcxt);

        return ncol;
    } else
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_COLUMN),
                 errmsg("column no %d is not defined", position)));

    /* be msvc quite */
    return NULL;
}

/*
 * CREATE PROCEDURE gms_sql.define_column(c int, col int, value "any", column_size int DEFAULT -1);
 */
Datum
gms_sql_define_column(PG_FUNCTION_ARGS)
{
    CursorData    *cursor;
    ColumnData    *col;
    Oid            valtype;
    Oid        basetype;
    int        position;
    int        colsize;
    TYPCATEGORY    category;
    bool        ispreferred;

    cursor = get_cursor(fcinfo, true);

    if (PG_ARGISNULL(1))
        ereport(ERROR,
                (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                 errmsg("column position (number) is NULL")));

    position = PG_GETARG_INT32(1);
    col = get_col(cursor, position, true);

    valtype = get_fn_expr_argtype(fcinfo->flinfo, 2);
    if (valtype == RECORDOID)
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("cannot to define a column of record type")));

    if (valtype == UNKNOWNOID)
        valtype = TEXTOID;

    basetype = getBaseType(valtype);

    if (col->typoid != InvalidOid)
        ereport(ERROR,
                (errcode(ERRCODE_DUPLICATE_COLUMN),
                 errmsg("column is defined already")));

    col->typoid = valtype;

    if (PG_ARGISNULL(3))
        ereport(ERROR,
                (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                 errmsg("column_size is NULL")));

    colsize = PG_GETARG_INT32(3);

    get_type_category_preferred(basetype, &category, &ispreferred);
    col->typisstr = category == TYPCATEGORY_STRING;
    col->typmod = (col->typisstr && colsize != -1) ? colsize + 4 : -1;

    get_typlenbyval(basetype, &col->typlen, &col->typbyval);

    col->rowcount = 1;

    PG_RETURN_VOID();
}

/*
 * CREATE PROCEDURE gms_sql.define_array(c int, col int, value "anyarray", rowcount int, index1 int);
 */
Datum
gms_sql_define_array(PG_FUNCTION_ARGS)
{
    CursorData    *cursor;
    ColumnData    *col;
    Oid            valtype;
    Oid            basetype;
    int            position;
    int            rowcount;
    int            index1;
    Oid            elementtype;
    TYPCATEGORY    category;
    bool            ispreferred;

    cursor = get_cursor(fcinfo, true);

    if (PG_ARGISNULL(1))
        ereport(ERROR,
                (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                 errmsg("column position (number) is NULL")));

    position = PG_GETARG_INT32(1);
    col = get_col(cursor, position, true);

    valtype = get_fn_expr_argtype(fcinfo->flinfo, 2);
    if (valtype == RECORDOID)
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("cannot to define a column of record type")));

    get_type_category_preferred(valtype, &category, &ispreferred);
    if (category != TYPCATEGORY_ARRAY)
        elog(ERROR, "defined value is not array");

    col->typarrayoid = valtype;

    basetype = getBaseType(valtype);
    elementtype = get_element_type(basetype);

    if (!OidIsValid(elementtype))
        ereport(ERROR,
                (errcode(ERRCODE_DATATYPE_MISMATCH),
                 errmsg("column is not a array")));

    if (col->typoid != InvalidOid)
        ereport(ERROR,
                (errcode(ERRCODE_DUPLICATE_COLUMN),
                 errmsg("column is defined already")));

    col->typoid = elementtype;

    if (PG_ARGISNULL(3))
        ereport(ERROR,
                (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                 errmsg("cnt is NULL")));

    rowcount = PG_GETARG_INT32(3);
    if (rowcount <= 0)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("cnt is less or equal to zero")));

    col->rowcount = (uint64) rowcount;

    if (PG_ARGISNULL(4))
        ereport(ERROR,
                (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                 errmsg("lower_bnd is NULL")));

    index1 = PG_GETARG_INT32(4);
    if (index1 < 1)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("lower_bnd is less than one")));

    if (index1 != 1)
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("lower_bnd can be only only \"1\"")));

    col->index1 = index1;

    get_typlenbyval(col->typoid, &col->typlen, &col->typbyval);

    PG_RETURN_VOID();
}

static void
cursor_xact_cxt_deletion_callback(std::shared_ptr<void> arg)
{
    CursorData    *cur = std::static_pointer_cast<CursorData>(arg).get();

    cur->cursor_xact_cxt = NULL;
    cur->tuples_cxt = NULL;

    cur->processed = 0;
    cur->nread = 0;
    cur->executed = false;
    cur->tupdesc = NULL;
    cur->coltupdesc = NULL;
    cur->casts = NULL;
    cur->array_columns = NULL;
}
static void prepare_and_execute_cursor(CursorData *cursor)
{
    Datum    *values;
    Oid        *types;
    char        *nulls;
    ListCell    *lc;
    int        i;
    MemoryContext    oldcxt;
    uint64      batch_rows = 0;

    oldcxt = MemoryContextSwitchTo(cursor->cursor_xact_cxt);

    /* prepare query arguments */
    values = (Datum*)palloc(sizeof(Datum) * cursor->nvariables);
    types = (Oid*)palloc(sizeof(Oid) * cursor->nvariables);
    nulls = (char*)palloc(sizeof(char) * cursor->nvariables);

    i = 0;
    foreach(lc, cursor->variables) {
        VariableData *var = (VariableData *) lfirst(lc);

        if (var->is_array)
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("a array (bulk) variable can be used only when no column is defined")));

        if (!var->isnull) {
            /* copy a value to xact memory context, to be independent on a outside */
            values[i] = datumCopy(var->value, var->typbyval, var->typlen);
            nulls[i] = ' ';
        } else
            nulls[i] = 'n';

        if (var->typoid == InvalidOid)
            ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_PARAMETER),
                 errmsg("variable \"%s\" has not a value", var->refname)));

        types[i] = var->typoid;
        i += 1;
    }

    /* prepare or refresh target tuple descriptor, used for final tupconversion */
    if (cursor->tupdesc)
        FreeTupleDesc(cursor->tupdesc);

#if PG_VERSION_NUM >= 120000

    cursor->coltupdesc = CreateTemplateTupleDesc(cursor->max_colpos);

#else

    cursor->coltupdesc = CreateTemplateTupleDesc(cursor->max_colpos, false);

#endif

    /* prepare current result column tupdesc */
    for (i = 1; i <= cursor->max_colpos; i++) {
        ColumnData *col = get_col(cursor, i, false);
        char genname[32];

        snprintf_s(genname, 32, 31, "col%d", i);

        Assert(col->rowcount > 0);

        if (col->typarrayoid) {
            if (batch_rows != 0)
                batch_rows = col->rowcount < batch_rows ? col->rowcount : batch_rows;
            else
                batch_rows = col->rowcount;
            cursor->array_columns = bms_add_member(cursor->array_columns, i);
        } else {
            /* in this case we cannot do batch of rows */
            batch_rows = 1;
        }

        TupleDescInitEntry(cursor->coltupdesc, (AttrNumber) i, genname, col->typoid, col->typmod, 0);
    }

    cursor->batch_rows = batch_rows;
    Assert(cursor->coltupdesc->natts >= 0);
    cursor->casts = (CastCacheData*)palloc0(sizeof(CastCacheData) * ((unsigned int) cursor->coltupdesc->natts));

    MemoryContextSwitchTo(oldcxt);

    snprintf_s(cursor->cursorname, sizeof(cursor->cursorname), sizeof(cursor->cursorname) - 1, "__orafce_gms_sql_cursor_%d", cursor->cid);
    if (SPI_connect() != SPI_OK_CONNECT)
        elog(ERROR, "SPI_connact failed");

    cursor->portal = SPI_cursor_open_with_args(cursor->cursorname,
                          cursor->parsed_query,
                          (int) cursor->nvariables,
                          types,
                          values,
                          nulls,
                          false,
                          0);

    /* internal error */
    if (cursor->portal == NULL)
        elog(ERROR,
         "could not open cursor for query \"%s\": %s",
         cursor->parsed_query,
         SPI_result_code_string(SPI_result));

    SPI_finish();

    /* Describe portal and prepare cast cache */
    if (cursor->portal->tupDesc) {
        int    natts = 0;
        TupleDesc tupdesc = cursor->portal->tupDesc;

        for (i = 0; i < tupdesc->natts; i++) {
            Form_pg_attribute att = TupleDescAttr(tupdesc, i);

            if (att->attisdropped)
                continue;

            natts += 1;
        }

        if (natts != cursor->coltupdesc->natts)
            ereport(ERROR,
                (errcode(ERRCODE_DATA_EXCEPTION),
                errmsg("number of defined columns is different than number of query's columns")));
    }

    cursor->executed = true;
}


static uint64 execute_spi_plan(CursorData *cursor)
{
    MemoryContext    oldcxt;
    Datum            *values;
    char            *nulls;
    ArrayIterator    *iterators;
    bool    has_iterator = false;
    bool    has_value = true;
    int        max_index1 = -1;
    int        min_index2 = -1;
    int        max_rows = -1;
    uint64        result = 0;
    ListCell    *lc;
    int        i;

    if (SPI_connect() != SPI_OK_CONNECT)
        elog(ERROR, "SPI_connact failed");

    /* prepare, or reuse cached plan */
    if (!cursor->plan) {
        Oid          *types = NULL;
        SPIPlanPtr    plan;

        types = (Oid*)palloc(sizeof(Oid) * cursor->nvariables);

        i = 0;
        foreach(lc, cursor->variables) {
            VariableData *var = (VariableData *) lfirst(lc);

            if (var->typoid == InvalidOid)
                ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_PARAMETER),
                    errmsg("variable \"%s\" has not a value", var->refname)));

            types[i++] = var->is_array ? var->typelemid : var->typoid;
        }

        plan = SPI_prepare(cursor->parsed_query, (int) cursor->nvariables, types);

        if (!plan)
            /* internal error */
            elog(ERROR, "cannot to prepare plan");

        if (types)
            pfree(types);

        SPI_keepplan(plan);
        cursor->plan = plan;
    }

    oldcxt = MemoryContextSwitchTo(cursor->result_cxt);
    /* prepare query arguments */
    values = (Datum*)palloc(sizeof(Datum) * cursor->nvariables);
    nulls = (char*)palloc(sizeof(char) * cursor->nvariables);
    iterators = (ArrayIterator*)palloc(sizeof(ArrayIterator *) * cursor->nvariables);
    has_value = true;

    i = 0;
    foreach(lc, cursor->variables) {
        VariableData *var = (VariableData *) lfirst(lc);

        if (var->is_array) {
            if (!var->isnull) {
                iterators[i] = array_create_iterator(DatumGetArrayTypeP(var->value), 0);
                /* search do lowest common denominator */
                if (var->index1 != -1) {
                    if (max_index1 != -1) {
                        max_index1 = max_index1 < var->index1 ? var->index1 : max_index1;
                        min_index2 = min_index2 > var->index2 ? var->index2 : min_index2;
                    } else {
                        max_index1 = var->index1;
                        min_index2 = var->index2;
                    }
                }

                has_iterator = true;
            } else {
                /* cannot to read data from NULL array */
                has_value = false;
                break;
            }
        } else {
            values[i] = var->value;
            nulls[i] = var->isnull ? 'n' : ' ';
        }
        i += 1;
    }

    if (has_iterator) {
        if (has_value) {
            if (max_index1 != -1) {
                max_rows = min_index2 - max_index1 + 1;
                has_value = max_rows > 0;

                if (has_value && max_index1 > 1) {
                    i = 0;
                    foreach(lc, cursor->variables) {
                        VariableData *var = (VariableData *) lfirst(lc);
                        if (var->is_array) {
                            int    j;

                            Assert(iterators[i]);
                            for (j = 1; j < max_index1; j++) {
                                Datum    value;
                                bool    isnull;

                                has_value = array_iterate(iterators[i], &value, &isnull);
                                if (!has_value)
                                    break;
                            }

                            if (!has_value)
                                break;
                        }
                        i += 1;
                    }
                }
            }
        }
        while (has_value && (max_rows == -1 || max_rows > 0)) {
            int    rc;
            i = 0;
            foreach(lc, cursor->variables) {
                VariableData *var = (VariableData *) lfirst(lc);
                if (var->is_array) {
                    Datum    value;
                    bool    isnull;

                    has_value = array_iterate(iterators[i], &value, &isnull);
                    if (!has_value)
                        break;

                    values[i] = value;
                    nulls[i] = isnull ? 'n' : ' ';
                }
                i += 1;
            }
            if (!has_value)
                break;
            rc = SPI_execute_plan(cursor->plan, values, nulls, false, 0);
            if (rc < 0)
                /* internal error */
                elog(ERROR, "cannot to execute a query");

            result += SPI_processed;

            if (max_rows > 0)
                max_rows -= 1;
        }
        MemoryContextReset(cursor->result_cxt);
    } else {
        int    rc = SPI_execute_plan(cursor->plan, values, nulls, false, 0);
        if (rc < 0)
            /* internal error */
            elog(ERROR, "cannot to execute a query");

        result = SPI_processed;
    }

    SPI_finish();
    MemoryContextSwitchTo(oldcxt);
    return result;
}

static uint64
execute_query(CursorData *cursor)
{
    last_row_count = 0;

    /* clean space with saved result */
    if (!cursor->cursor_xact_cxt) {
        MemoryContextCallback *mcb;
        MemoryContext oldcxt;

        cursor->cursor_xact_cxt = AllocSetContextCreate(u_sess->top_transaction_mem_cxt,
                                                   "gms_sql transaction context",
                                                   ALLOCSET_DEFAULT_SIZES);

        oldcxt = MemoryContextSwitchTo(cursor->cursor_xact_cxt);
        mcb = (MemoryContextCallback*)palloc0(sizeof(MemoryContextCallback));
        mcb->func = cursor_xact_cxt_deletion_callback;
        mcb->arg = std::shared_ptr<void>(cursor);
        MemoryContextRegisterResetCallback(cursor->cursor_xact_cxt, mcb);

        MemoryContextSwitchTo(oldcxt);
    } else {
        MemoryContext    save_cxt = cursor->cursor_xact_cxt;
        /* free allocated memory in cursor_xact_cxt */
        MemoryContextReset(cursor->cursor_xact_cxt);
        cursor->cursor_xact_cxt = save_cxt;

        cursor->casts = NULL;
        cursor->tupdesc = NULL;
        cursor->tuples_cxt = NULL;
    }

    cursor->result_cxt = AllocSetContextCreate(cursor->cursor_xact_cxt,
                                          "gms_sql short life context",
                                          ALLOCSET_DEFAULT_SIZES);

    /*
     * When column definitions are available, build final query
     * and open cursor for fetching. When column definitions are
     * missing, then the statement can be called with high frequency
     * etc INSERT, UPDATE, so use cached plan.
     */

    if (cursor->columns) {
        prepare_and_execute_cursor(cursor);
    } else {
        return execute_spi_plan(cursor);
    }   
    return 0L;
}

/*
 * CREATE FUNCTION gms_sql.execute(c int) RETURNS bigint;
 */
Datum
gms_sql_execute(PG_FUNCTION_ARGS)
{
    CursorData    *cursor;

    cursor = get_cursor(fcinfo, true);

    PG_RETURN_INT64((int64) execute_query(cursor));
}

static uint64
fetch_rows(CursorData *cursor, bool exact)
{
    uint64    can_read_rows;

    if (!cursor->executed)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_CURSOR_STATE),
                 errmsg("cursor is not executed")));

    if (!cursor->portal)
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                 errmsg("cursor has not portal")));

    if (cursor->nread == cursor->processed) {
        MemoryContext    oldcxt;
        uint64            i;
        int    batch_rows;

        if (!exact) {
            if (cursor->array_columns)
                batch_rows = (1000 / cursor->batch_rows) * cursor->batch_rows;
            else
                batch_rows = 1000;
        } else
            batch_rows = 2;

        /* create or reset context for tuples */
        if (!cursor->tuples_cxt)
            cursor->tuples_cxt = AllocSetContextCreate(cursor->cursor_xact_cxt,
                                                  "gms_sql tuples context",
                                                  ALLOCSET_DEFAULT_SIZES);
        else
            MemoryContextReset(cursor->tuples_cxt);

        if (SPI_connect() != SPI_OK_CONNECT)
            elog(ERROR, "SPI_connact failed");

        /* try to fetch data from cursor */
        SPI_cursor_fetch(cursor->portal, true, batch_rows);

        if (SPI_tuptable == NULL)
            elog(ERROR, "cannot fetch data");

        if (exact && SPI_processed > 1)
            ereport(ERROR,
                    (errcode(ERRCODE_TOO_MANY_ROWS),
                     errmsg("too many rows"),
                     errdetail("In exact mode only one row is expected")));

        if (exact && SPI_processed == 0)
            ereport(ERROR,
                    (errcode(ERRCODE_NO_DATA_FOUND),
                     errmsg("no data found"),
                     errdetail("In exact mode only one row is expected")));

        oldcxt = MemoryContextSwitchTo(cursor->tuples_cxt);

        cursor->tupdesc = CreateTupleDescCopy(SPI_tuptable->tupdesc);

        for (i = 0; i < SPI_processed; i++)
            cursor->tuples[i] = heap_copytuple(SPI_tuptable->vals[i]);

        MemoryContextSwitchTo(oldcxt);

        cursor->processed = SPI_processed;
        cursor->nread = 0;

        SPI_finish();
    }

    if (cursor->processed - cursor->nread >= cursor->batch_rows)
        can_read_rows = cursor->batch_rows;
    else
        can_read_rows = cursor->processed - cursor->nread;

    cursor->start_read = cursor->nread;
    cursor->nread += can_read_rows;
    last_row_count = can_read_rows;
    return can_read_rows;
}

/*
 * CREATE FUNCTION gms_sql.fetch_rows(c int) RETURNS int;
 */
Datum
gms_sql_fetch_rows(PG_FUNCTION_ARGS)
{
    CursorData    *cursor;

    cursor = get_cursor(fcinfo, true);

    PG_RETURN_INT32(fetch_rows(cursor, false));
}

/*
 * CREATE FUNCTION gms_sql.execute_and_fetch(c int, exact bool DEFAULT false) RETURNS int;
 */
Datum
gms_sql_execute_and_fetch(PG_FUNCTION_ARGS)
{
    CursorData    *cursor;
    bool            exact;

    cursor = get_cursor(fcinfo, true);

    if (PG_ARGISNULL(1))
        ereport(ERROR,
                (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                 errmsg("exact option is NULL")));

    exact = PG_GETARG_BOOL(1);
    execute_query(cursor);
    PG_RETURN_INT32(fetch_rows(cursor, exact));
}

/*
 * CREATE FUNCTION gms_sql.last_row_count() RETURNS int;
 */
Datum
gms_sql_last_row_count(PG_FUNCTION_ARGS)
{
    (void) fcinfo;
    PG_RETURN_INT32(last_row_count);
}

/*
 * Initialize cast case entry.
 */
static void
init_cast_cache_entry(CastCacheData *ccast,
                      Oid targettypid,
                      int32 targettypmod,
                      Oid sourcetypid)
{
    Oid    funcoid;
    Oid    basetypid;

    basetypid = getBaseType(targettypid);

    if (targettypid != basetypid)
        ccast->targettypid = targettypid;
    else
        ccast->targettypid = InvalidOid;

    ccast->targettypmod = targettypmod;

    if (sourcetypid == targettypid)
        ccast->without_cast = targettypmod == -1;
    else
        ccast->without_cast = false;

    if (!ccast->without_cast) {
        ccast->path = find_coercion_pathway(basetypid,
                                            sourcetypid,
                                            COERCION_ASSIGNMENT,
                                            &funcoid);

        if (ccast->path == COERCION_PATH_NONE)
            ereport(ERROR,
                (errcode(ERRCODE_CANNOT_COERCE),
                 errmsg("cannot to find cast from source type \"%s\" to target type \"%s\"",
                         format_type_be(sourcetypid),
                         format_type_be(basetypid))));

        if (ccast->path == COERCION_PATH_FUNC) {
            fmgr_info(funcoid, &ccast->finfo);
        } else if (ccast->path == COERCION_PATH_COERCEVIAIO) {
            bool    typisvarlena;

            getTypeOutputInfo(sourcetypid, &funcoid, &typisvarlena);
            fmgr_info(funcoid, &ccast->finfo_out);
            getTypeInputInfo(targettypid, &funcoid, &ccast->typIOParam);
            fmgr_info(funcoid, &ccast->finfo_in);
        }

        if (targettypmod != -1) {
            ccast->path_typmod = find_typmod_coercion_function(targettypid,
                                                               &funcoid);
            if (ccast->path_typmod == COERCION_PATH_FUNC)
                fmgr_info(funcoid, &ccast->finfo_typmod);
        }
    }

    ccast->isvalid = true;
}

/*
 * Apply cast rules to a value
 */
static Datum
cast_value(CastCacheData *ccast, Datum value, bool isnull)
{
    if (!isnull && !ccast->without_cast) {
        if (ccast->path == COERCION_PATH_FUNC) {
            value = FunctionCall1(&ccast->finfo, value);
        } else if (ccast->path == COERCION_PATH_RELABELTYPE) {
            value = value;
        } else if (ccast->path == COERCION_PATH_COERCEVIAIO) {
            char    *str;
            str = OutputFunctionCall(&ccast->finfo_out, value);
            value = InputFunctionCall(&ccast->finfo_in,
                                      str,
                                      ccast->typIOParam,
                                      ccast->targettypmod);
        } else
            ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                     errmsg("unsupported cast path yet %d", ccast->path)));

        if (ccast->targettypmod != -1 && ccast->path_typmod == COERCION_PATH_FUNC)
            value = FunctionCall3(&ccast->finfo_typmod,
                                  value,
                                  Int32GetDatum(ccast->targettypmod),
                                  BoolGetDatum(true));
    }
    if (ccast->targettypid != InvalidOid)
        domain_check(value, isnull, ccast->targettypid, NULL, NULL);

    return value;
}

/*
 * CALL statement is relatily slow in PLpgSQL - due repated parsing, planning.
 * So I wrote two variant of this routine. When spi_transfer is true, then
 * the value is copyied to SPI outer memory context.
 */
static Datum
column_value(CursorData *cursor, int pos, Oid targetTypeId, bool *isnull, bool spi_transfer)
{
    Datum    value;
    int32    columnTypeMode;
    Oid        columnTypeId;
    CastCacheData    *ccast;

    if (!cursor->executed)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_CURSOR_STATE),
                 errmsg("cursor is not executed")));

    if (!cursor->tupdesc)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_CURSOR_STATE),
                 errmsg("cursor is not fetched")));

    if (!cursor->coltupdesc)
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_COLUMN),
                 errmsg("no column is defined")));

    if (pos < 1 && pos > cursor->coltupdesc->natts)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("column position is of of range [1, %d]",
                        cursor->coltupdesc->natts)));

    columnTypeId = (TupleDescAttr(cursor->coltupdesc, pos - 1))->atttypid;
    columnTypeMode = (TupleDescAttr(cursor->coltupdesc, pos - 1))->atttypmod;

    Assert(cursor->casts);
    ccast = &cursor->casts[pos - 1];

    if (!ccast->isvalid) {
        Oid    basetype = getBaseType(targetTypeId);
    
        init_cast_cache_entry(ccast,
                              columnTypeId,
                              columnTypeMode,
                              SPI_gettypeid(cursor->tupdesc, pos));

        ccast->is_array = bms_is_member(pos, cursor->array_columns);

        if (ccast->is_array) {
            ccast->array_targettypid = basetype != targetTypeId ? targetTypeId : InvalidOid;

            if (get_array_type(getBaseType(columnTypeId)) != basetype)
                ereport(ERROR,
                        (errcode(ERRCODE_DATATYPE_MISMATCH),
                         errmsg("unexpected target type \"%s\" (expected type \"%s\")",
                                format_type_be(basetype),
                                format_type_be(get_array_type(getBaseType(columnTypeId))))));
        } else
            ccast->array_targettypid = InvalidOid;

        get_typlenbyval(basetype, &ccast->typlen, &ccast->typbyval);
    }

    if (ccast->is_array) {
        ArrayBuildState    *abs = NULL;
        uint64    idx;
        uint64    i;

        idx = cursor->start_read;

        for (i = 0; i < cursor->batch_rows; i++) {
            if (idx < cursor->processed) {
                value = SPI_getbinval(cursor->tuples[idx], cursor->tupdesc, pos, isnull);
                value = cast_value(ccast, value, *isnull);
                abs = accumArrayResult(abs,
                                       value,
                                       *isnull,
                                       columnTypeId,
                                       CurrentMemoryContext);

                idx += 1;
            }
        }

        value = makeArrayResult(abs, CurrentMemoryContext);

        if (ccast->array_targettypid != InvalidOid)
            domain_check(value, isnull, ccast->array_targettypid, NULL, NULL);
    } else {
        /* Maybe it can be solved by uncached slower cast */
        if (targetTypeId != columnTypeId)
                ereport(ERROR,
                        (errcode(ERRCODE_DATATYPE_MISMATCH),
                         errmsg("unexpected target type \"%s\" (expected type \"%s\")",
                                format_type_be(targetTypeId),
                                format_type_be(columnTypeId))));

        value = SPI_getbinval(cursor->tuples[cursor->start_read], cursor->tupdesc, pos, isnull);

        value = cast_value(ccast, value, *isnull);
    }

    if (spi_transfer)
        value = SPI_datumTransfer(value, ccast->typbyval, ccast->typlen);

    return value;
}

/*
 * CREATE PROCEDURE gms_sql.column_value(c int, pos int, INOUT value "any");
 * Note - CALL statement is slow from PLpgSQL block (against function execution).
 * This is reason why this routine is in function form too.
 */
Datum
gms_sql_column_value(PG_FUNCTION_ARGS)
{
    CursorData    *cursor;
    Datum        value;
    int        pos;
    bool        isnull;
    Oid        targetTypeId;
    MemoryContext    oldcxt;

    if (SPI_connect() != SPI_OK_CONNECT)
        elog(ERROR, "SPI_connact failed");

    cursor = get_cursor(fcinfo, true);

    if (PG_ARGISNULL(1))
        ereport(ERROR,
                (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                 errmsg("column position (number) is NULL")));

    pos = PG_GETARG_INT32(1);
    oldcxt = MemoryContextSwitchTo(cursor->result_cxt);
    targetTypeId = get_fn_expr_argtype(fcinfo->flinfo, 2);
    value = column_value(cursor, pos, targetTypeId, &isnull, true);

    SPI_finish();
    MemoryContextSwitchTo(oldcxt);
    MemoryContextReset(cursor->result_cxt);

    PG_RETURN_DATUM(value);
}

/*
 * CREATE FUNCTION gms_sql.column_value(c int, pos int, value anyelement) RETURNS anyelement;
 * Note - CALL statement is slow from PLpgSQL block (against function execution).
 * This is reason why this routine is in function form too.
 */
Datum
gms_sql_column_value_f(PG_FUNCTION_ARGS)
{
    return gms_sql_column_value(fcinfo);
}

/******************************************************************
 * Simple parser - just for replacement of bind variables by
 * PostgreSQL $ param placeholders.
 *
 ******************************************************************
 */

/*
 * It doesn't work for multibyte encodings, but same implementation
 * is in Postgres too.
 */
static bool
is_identif(unsigned char c)
{
    if (c >= 'a' && c <= 'z')
        return true;
    else if (c >= 'A' && c <= 'Z')
        return true;
    else if (c >= 0200)
        return true;
    else
        return false;
}

/*
 * simple parser to detect :identif symbols in query
 */
static char *
next_token(char *str, char **start, size_t *len, ofTokenType *typ, char **sep, size_t *seplen)
{
    if (*str == '\0') {
        *typ = TOKEN_NONE;
        return NULL;
    }

    /* reduce spaces */
    if (*str == ' ') {
        *start = str++;
        while (*str == ' ')
            str++;

        *typ = TOKEN_SPACES;
        *len = 1;
        return str;
    }

    /* Postgres's dolar strings */
    if (*str == '$' && (str[1] == '$' ||
        is_identif((unsigned char) str[1]) || str[1] == '_')) {
        char       *aux = str + 1;
        char       *endstr;
        bool        is_valid = false;
        char       *buffer;

        /* try to find end of separator */
        while (*aux) {
            if (*aux == '$') {
                is_valid = true;
                aux++;
                break;
            } else if (is_identif((unsigned char) *aux) ||
                     isdigit(*aux) ||
                     *aux == '_') {
                aux++;
            } else
                break;
        }

        if (!is_valid) {
            *typ = TOKEN_OTHER;
            *len = 1;
            *start = str;
            return str + 1;
        }

        /* now it looks like correct $ separator */
        *start = aux; *sep = str;
        Assert(aux >= str);
        *seplen = (size_t) (aux - str);
        *typ = TOKEN_DOLAR_STR;

        /* try to find second instance */
        buffer = (char*)palloc(*seplen + 1);
        memcpy(buffer, *sep, *seplen);
        buffer[*seplen] = '\0';

        endstr = strstr(aux, buffer);
        if (endstr) {
            Assert(endstr >= *start);
            *len = (size_t) (endstr - *start);
            return endstr + *seplen;
        } else {
            while (*aux)
                aux++;

            Assert(aux >= *start);
            *len = (size_t) (aux - *start);
            return aux;
        }

        return aux;
    }

    /* Pair comments */
    if (*str == '/' && str[1] == '*') {
        *start = str; str += 2;
        while (*str) {
            if (*str == '*' && str[1] == '/') {
                str += 2;
                break;
            }
            str++;
        }
        *typ = TOKEN_COMMENT;
        Assert(str >= *start);
        *len = (size_t) (str - *start);
        return str;
    }

    /* Number */
    if (isdigit(*str) || (*str == '.' && isdigit(str[1]))) {
        bool    point = *str == '.';

        *start = str++;
        while (*str) {
            if (isdigit(*str))
                str++;
            else if (*str == '.' && !point) {
                str++; point = true;
            } else
                break;
        }
        *typ = TOKEN_NUMBER;
        Assert(str >= *start);
        *len = (size_t) (str - *start);
        return str;
    }

    /* Double colon :: */
    if (*str == ':' && str[1] == ':') {
        *start = str;
        *typ = TOKEN_DOUBLE_COLON;
        *len = 2;
        return str + 2;
    }

    /* Bind variable placeholder */
    if (*str == ':' &&
        (is_identif((unsigned char) str[1]) || str[1] == '_')) {
        *start = &str[1]; str += 2;
        while (*str) {
            if (is_identif((unsigned char) *str) ||
                isdigit(*str) ||
                *str == '_')
                str++;
            else
                break;
        }
        *typ = TOKEN_BIND_VAR;
        Assert(str >= *start);
        *len = (size_t) (str - *start);
        return str;
    }

    /* Extended string literal */
    if ((*str == 'e' || *str == 'E') && str[1] == '\'') {
        *start = &str[2]; str += 2;
        while (*str) {
            if (*str == '\'') {
                *typ = TOKEN_EXT_STR;
                Assert(str >= *start);
                *len = (size_t) (str - *start);
                return str + 1;
            }
            if (*str == '\\' && str[1] == '\'')
                str += 2;
            else if (*str == '\\' && str[1] == '\\')
                str += 2;
            else
                str += 1;
        }

        *typ = TOKEN_EXT_STR;
        Assert(str >= *start);
        *len = (size_t) (str - *start);
        return str;
    }

    /* String literal */
    if (*str == '\'') {
        *start = &str[1]; str += 1;
        while (*str) {
            if (*str == '\'') {
                if (str[1] != '\'') {
                    *typ = TOKEN_STR;
                    Assert(str >= *start);
                    *len = (size_t) (str - *start);
                    return str + 1;
                }
                str += 2;
            } else
                str += 1;
        }
        *typ = TOKEN_STR;
        Assert(str >= *start);
        *len = (size_t) (str - *start);
        return str;
    }

    /* Quoted identifier */
    if (*str == '"') {
        *start = &str[1]; str += 1;
        while (*str) {
            if (*str == '"') {
                if (str[1] != '"') {
                    *typ = TOKEN_QIDENTIF;
                    Assert(str >= *start);
                    *len = (size_t) (str - *start);
                    return str + 1;
                }
                str += 2;
            } else
                str += 1;
        }
        *typ = TOKEN_QIDENTIF;
        Assert(str >= *start);
        *len = (size_t) (str - *start);
        return str;
    }

    /* Identifiers */
    if (is_identif((unsigned char) *str) || *str == '_') {
        *start = str++;
        while (*str) {
            if (is_identif((unsigned char) *str) ||
                isdigit(*str) ||
                *str == '_')
                str++;
            else
                break;
        }
        *typ = TOKEN_IDENTIF;
        Assert(str >= *start);
        *len = (size_t) (str - *start);
        return str;
    }

    /* Others */
    *typ = TOKEN_OTHER;
    *start = str;
    *len = 1;
    return str + 1;
}

typedef struct {
    int    type_id;
    int    type_col_num;
    char*    type_name;
} gms_sql_desc_rec_type;

static gms_sql_desc_rec_type desc_rec_type_table[] = {
        {1, 11, "desc_rec", },
        {2, 11, "desc_rec2"},
        {3, 13, "desc_rec3"},
        {4, 13, "desc_rec4"}
};

static 
gms_sql_desc_rec_type* gms_sql_search_desc_rec_type(Oid typid)
{
    Oid    nspOid;
    Oid    typOid;
    int    i;
    nspOid = get_namespace_oid("gms_sql", false);
    for (i = 0; i < 4; i++) {
        typOid = GetSysCacheOid2(TYPENAMENSP, CStringGetDatum(desc_rec_type_table[i].type_name), ObjectIdGetDatum(nspOid));
        if (!OidIsValid(typOid)) {
            ereport(ERROR,
                 (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                 errmsg("gms_sql.%s type does not exist.", desc_rec_type_table[i].type_name)));
        }
        if (typid == typOid)
            return &desc_rec_type_table[i];
    }
    ereport(ERROR,
            (errcode(ERRCODE_WRONG_OBJECT_TYPE),
            errmsg("typeid %u is not gms_sql.desc_rec.", typid)));
    return NULL;
}

static int
map_type_code(Oid type_id)
{
    int    code = 109;

    switch (type_id) {
    case VARCHAROID:
    case NVARCHAR2OID:
        code =  1;
        break;
    case NUMERICOID:
    case INT1OID:
    case INT2OID:
    case INT4OID:
    case INT8OID:
    case INT16OID:
    case FLOAT4OID:
        code = 2;
        break;
    case DATEOID:
        code = 12;
        break;
    case FLOAT8OID:
        code = 101;
        break;
    case RAWOID:
        code =  23;
        break;
    case CHAROID:
        code =  96;
        break;
    case CLOBOID:
        code =  112;
        break;
    case BLOBOID:
        code =  113;
        break;
    case JSONOID:
        code =  119;
        break;
    case TIMESTAMPOID:
        code = 180;
        break;
    case TIMESTAMPTZOID:
        code = 181;
        break;
    case INTERVALOID:
        code =  183;
        break;
    default:
        break;
    }
    return code;
}
/*
 * CREATE PROCEDURE gms_sql.describe_columns(c int, OUT col_cnt int, OUT desc_t gms_sql.desc_rec[])
 *
 * Returns an array of column's descriptions. Attention, the typid are related to PostgreSQL type
 * system.
 */
Datum
gms_sql_describe_columns(PG_FUNCTION_ARGS)
{
    CursorData    *cursor;
    Datum        *values;
    bool        *nulls;
    TupleDesc    tupdesc;
    TupleDesc    desc_rec_tupdesc;
    TupleDesc    cursor_tupdesc;
    HeapTuple    tuple;
    Oid            arraytypid;
    Oid            desc_rec_typid;
    Oid           *types = NULL;
    ArrayBuildState *abuilder = NULL;
    SPIPlanPtr        plan;
    CachedPlanSource *plansource = NULL;
    int            ncolumns = 0;
    int            rc;
    int            i = 0;
    bool        nonatomic;
    gms_sql_desc_rec_type*    rec_type;
    MemoryContext    callercxt = CurrentMemoryContext;

    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
        elog(ERROR, "return type must be a row type");

    arraytypid = TupleDescAttr(tupdesc, 1)->atttypid;
    desc_rec_typid = get_element_type(arraytypid);
    if (!OidIsValid(desc_rec_typid))
        elog(ERROR, "second output field must be an array");

    rec_type = gms_sql_search_desc_rec_type(desc_rec_typid);
    desc_rec_tupdesc = lookup_rowtype_tupdesc_copy(desc_rec_typid, -1);
    values = (Datum*)palloc0(rec_type->type_col_num * sizeof(Datum));
    nulls = (bool*)palloc0(rec_type->type_col_num * sizeof(bool));
    cursor = get_cursor(fcinfo, true);
    if (cursor->variables) {
        ListCell *lc;
        types = (Oid*)palloc(sizeof(Oid) * cursor->nvariables);
        i = 0;
        foreach(lc, cursor->variables) {
            VariableData *var = (VariableData *) lfirst(lc);
            if (var->typoid == InvalidOid)
                ereport(ERROR,
                        (errcode(ERRCODE_UNDEFINED_PARAMETER),
                         errmsg("variable \"%s\" has not a value", var->refname)));

            types[i++] = var->is_array ? var->typelemid : var->typoid;
        }
    }
    /*
     * Connect to SPI manager
     */
    nonatomic = fcinfo->context && IsA(fcinfo->context, FunctionScanState) &&
        !castNode(FunctionScanState, fcinfo->context)->atomic;

    if ((rc = SPI_connect_ext(DestSPI, NULL, NULL, nonatomic ? SPI_OPT_NONATOMIC : 0)) != SPI_OK_CONNECT)
        elog(ERROR, "SPI_connect failed: %s", SPI_result_code_string(rc));

    plan = SPI_prepare(cursor->parsed_query, (int) cursor->nvariables, types);
    if (!plan || plan->magic != _SPI_PLAN_MAGIC)
        elog(ERROR, "plan is not valid");

    if (list_length(plan->plancache_list) != 1)
        elog(ERROR, "plan is not single execution plany");

    plansource = (CachedPlanSource *) linitial(plan->plancache_list);
    cursor_tupdesc = plansource->resultDesc;
    ncolumns = cursor_tupdesc->natts;
    for (i = 0; i < ncolumns; i++) {
        HeapTuple    tp;
        Form_pg_type    typtup;
        int    type_code;
        text     *attname = NULL;
        text     *schname = NULL;
        text     *typname = NULL;

        Form_pg_attribute attr = TupleDescAttr(cursor_tupdesc, i);

        /*
         * 0. col_type            BINARY_INTEGER := 0,
         * 1. col_max_len         BINARY_INTEGER := 0,
         * 2. col_name            VARCHAR2(32)   := '',
         * 3. col_name_len        BINARY_INTEGER := 0,
         * 4. col_schema_name     VARCHAR2(32)   := '',
         * 5. col_schema_name_len BINARY_INTEGER := 0,
         * 6. col_precision       BINARY_INTEGER := 0,
         * 7. col_scale           BINARY_INTEGER := 0,
         * 8. col_charsetid       BINARY_INTEGER := 0,
         * 9. col_charsetform     BINARY_INTEGER := 0,
         * 10. col_null_ok        BOOLEAN        := TRUE
         * 11. col_type_name      varchar2       := '',
         * 12. col_type_name_len  BINARY_INTEGER := 0 );
         */
        memset_s(values, rec_type->type_col_num * sizeof(Datum), 0, rec_type->type_col_num * sizeof(Datum));
        memset_s(nulls, rec_type->type_col_num * sizeof(bool), 0, rec_type->type_col_num * sizeof(bool));
        type_code = map_type_code(attr->atttypid);
        values[0] = Int32GetDatum(type_code);
        tp = SearchSysCache1(TYPEOID, ObjectIdGetDatum(attr->atttypid));
        if (!HeapTupleIsValid(tp))
            elog(ERROR, "cache lookup failed for type %u", attr->atttypid);

        typtup = (Form_pg_type) GETSTRUCT(tp);
        values[1] = Int32GetDatum(0);
        values[6] = Int32GetDatum(0);
        values[7] = Int32GetDatum(0);
        if (attr->attlen != -1)
            values[1] = Int32GetDatum(attr->attlen);
        else if (typtup->typcategory == 'S' && attr->atttypmod > VARHDRSZ)
            values[1] = Int32GetDatum(attr->atttypmod - VARHDRSZ);
        else if (attr->atttypid == NUMERICOID && attr->atttypmod > VARHDRSZ) {
            values[6] = Int32GetDatum(((attr->atttypmod - VARHDRSZ) >> 16) & 0xffff);
            values[7] = Int32GetDatum((((attr->atttypmod - VARHDRSZ) & 0x7ff) ^ 1024) - 1024);
        }

        attname = cstring_to_text(NameStr(attr->attname));
        values[2] = PointerGetDatum(attname);
        values[3] = DirectFunctionCall1(textlen, PointerGetDatum(attname));
        if (rec_type->type_id == 1 && DatumGetInt32(values[3]) > 32)
            elog(ERROR, "desc_rec.col_name(%d) is more than 32", DatumGetInt32(values[3]));

        schname = cstring_to_text(get_namespace_name(typtup->typnamespace));
        values[4] = PointerGetDatum(schname);
        values[5] = DirectFunctionCall1(textlen, PointerGetDatum(schname));
        values[8] = Int32GetDatum(0);
        values[9] = Int32GetDatum(0);
        values[10] = BoolGetDatum(true);

        if (attr->attnotnull)
            values[10] = BoolGetDatum(false);
        else if (typtup->typnotnull)
            values[10] = BoolGetDatum(false);

        if (rec_type->type_id > 2) {
            if (type_code == 109) {
                typname = cstring_to_text(NameStr(typtup->typname));
                values[11] = PointerGetDatum(typname);
                values[12] = DirectFunctionCall1(textlen, PointerGetDatum(typname));
            } else {
                nulls[11] = true;
                nulls[12] = true;
            }
        }

        tuple = heap_form_tuple(desc_rec_tupdesc, values, nulls);
        abuilder = accumArrayResult(abuilder,
                                    HeapTupleGetDatum(tuple),
                                    false,
                                    desc_rec_typid,
                                    CurrentMemoryContext);
        ReleaseSysCache(tp);
        pfree_ext(attname);
        pfree_ext(schname);
        pfree_ext(typname);
    }
    memset_s(values, rec_type->type_col_num * sizeof(Datum), 0, rec_type->type_col_num * sizeof(Datum));
    memset_s(nulls, rec_type->type_col_num * sizeof(bool), 0, rec_type->type_col_num * sizeof(bool));
    values[0] = Int32GetDatum(ncolumns);
    nulls[0] = false;
    values[1] = makeArrayResult(abuilder, callercxt);
    nulls[1] = false;
    SPI_freeplan(plan);
    if ((rc = SPI_finish()) != SPI_OK_FINISH)
        elog(ERROR, "SPI_finish failed: %s", SPI_result_code_string(rc));
    MemoryContextSwitchTo(callercxt);
    tuple = heap_form_tuple(tupdesc, values, nulls);
    pfree(values);
    pfree(nulls);

    PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}

Datum
gms_sql_describe_columns_f(PG_FUNCTION_ARGS)
{
    return gms_sql_describe_columns(fcinfo);
}

static bool
type_is_right_align(Oid type)
{
    switch (type) {
        case INT2OID:
        case INT4OID:
        case INT8OID:
        case INT16OID:
        case FLOAT4OID:
        case FLOAT8OID:
        case NUMERICOID:
        case OIDOID:
        case XIDOID:
        case CIDOID:
        case CASHOID:
            return true;
        default:
            return false;
        }
 }

static int *
set_maxwidth(int nrows, TupleDesc desc, HeapTuple* tuples)
{
    int natts = desc->natts;
    int *max_width;
    char *value;
    int i, j;

    /* get max_width from header */
    max_width = (int*) palloc0(natts * sizeof(int));
    for (i = 0; i < natts; i++) {
        char *attrname = SPI_fname(desc, i + 1);
        text *attrtext= cstring_to_text(attrname);
        max_width[i] = DatumGetInt32(DirectFunctionCall1(textlen, PointerGetDatum(attrtext)));
        pfree(attrname);
        pfree(attrtext);
    }

    /* get max_width from value */
    for (i = 0; i < nrows; i++) {
        int len;

        for (j = 0; j < natts; j++) {
            value = SPI_getvalue(tuples[i], desc, j + 1);
            if (value) {
                text *attrtext= cstring_to_text(value);
                len = DatumGetInt32(DirectFunctionCall1(textlen, PointerGetDatum(attrtext)));
                if (len > max_width[j])
                    max_width[j] = len;
                pfree(value);
                pfree(attrtext);
            }
        }
    }
    return max_width;
}

static void
print_buf_to_client(StringInfo buf, char *message)
{
    resetStringInfo(buf);
    pq_beginmessage(buf, 'N');

    if (PG_PROTOCOL_MAJOR(FrontendProtocol) >= 3) {
        pq_sendbyte(buf, PG_DIAG_MESSAGE_PRIMARY);
        pq_sendstring(buf, message);
        pq_sendbyte(buf, '\0');
    } else {
        pq_sendstring(buf, message);
    }
    pq_endmessage_reuse(buf);
    pq_flush();
    resetStringInfo(buf);
}

static void
print_header_to_client(int *max_width, TupleDesc desc, StringInfo buf, StringInfo outbuf)
{
    int i, j;
    TransactionId curlxid;
    /* result number increase in transaction */
    curlxid = SPI_get_top_transaction_id();
    if (curlxid == last_lxid)
        result_no++;
    else
        result_no = 1;
    last_lxid = curlxid;
    /* print ResultSet */
    appendStringInfo(buf, "ResultSet #%d\n", result_no);
    print_buf_to_client(outbuf, buf->data);
    /* print from header */
    resetStringInfo(buf);
    for (i = 0; i < desc->natts; i++) {
        char *attname = SPI_fname(desc, i + 1);
        int nspace = max_width[i] - pg_mbstrlen(attname) + 2;
        appendStringInfo(buf, "%-*s%s%-*s", nspace/2, "", attname, (nspace + 1) / 2, "");
        if (i < desc->natts - 1)
            appendStringInfo(buf, "%s", "|");
    }
    print_buf_to_client(outbuf, buf->data);
    /* print horizontal line */
    resetStringInfo(buf);
    for (i = 0; i < desc->natts; i++) {
        for (j = 0; j < (max_width[i] + 2); j++)
            appendStringInfo(buf, "%s", "-");

        if (i < desc->natts - 1)
            appendStringInfo(buf, "%s", "+");
    }
    print_buf_to_client(outbuf, buf->data);
    resetStringInfo(buf);
}

static void
print_row_to_client(int *max_width, TupleDesc desc, HeapTuple tuple, StringInfo buf, StringInfo outbuf)
{
    char    *value;
    Datum     datum;
    int     i;
    int     natts = desc->natts;
    text     *spacetext = cstring_to_text(" ");
    for (i = 0; i < natts; i++) {
        value = SPI_getvalue(tuple, desc, i + 1);
        if (value) {
            Oid typid = SPI_gettypeid(desc, i + 1);
            text *attrtext= cstring_to_text(value);
            if (type_is_right_align(typid)) {
                datum = DirectFunctionCall3(rpad, PointerGetDatum(attrtext), max_width[i], PointerGetDatum(spacetext));
            } else {
                datum = DirectFunctionCall3(lpad, PointerGetDatum(attrtext), max_width[i], PointerGetDatum(spacetext));
            }
            char *line = DatumGetCString(DirectFunctionCall1(textout, datum));
            appendStringInfo(buf, "%s%s%s", " ", line, ((i < natts - 1) ? " |" : " "));
            pfree(value);
            pfree(attrtext);
            pfree(DatumGetPointer(datum));
        } else {
            appendStringInfo(buf, "%*s", max_width[i] + 2, " ");
        }
    }
    pfree(spacetext);
    print_buf_to_client(outbuf, buf->data);
    resetStringInfo(buf);
}

static void
print_footer_to_client(unsigned int rows, StringInfo buf, StringInfo outbuf)
{
    appendStringInfo(buf, "(%u row%s)\n", rows, (rows > 1 ? "s" : ""));
    print_buf_to_client(outbuf, buf->data);
    resetStringInfo(buf);
}

static void
return_result_to_client(int nrows, TupleDesc desc, HeapTuple* tuples)
{
    StringInfo    buf;
    StringInfo    outbuf;
    int    *max_width;
    int    i;

    buf = makeStringInfo();
    outbuf = makeStringInfo();
    /* compute col width */
    max_width = set_maxwidth(nrows, desc, tuples);
    print_header_to_client(max_width, desc, buf, outbuf);
    /* print rows */
    for (i = 0; i < nrows; i++) {
        print_row_to_client(max_width, desc, tuples[i], buf, outbuf);
    }
    /* print footer */
    print_footer_to_client(nrows, buf, outbuf);
    pfree_ext(max_width);
    FreeStringInfo(buf);
    FreeStringInfo(outbuf);
}
Datum
gms_sql_return_result(PG_FUNCTION_ARGS)
{
    char* name = text_to_cstring(PG_GETARG_TEXT_PP(0));
    Portal portal;
    int rc = 0;
    /*
     * Connect to SPI manager
     */
    SPI_STACK_LOG("connect", NULL, NULL);
    if ((rc = SPI_connect() != SPI_OK_CONNECT))
        ereport(ERROR,
            (errmodule(MOD_OPT),
                (errcode(ERRCODE_SPI_CONNECTION_FAILURE),
                    errmsg("SPI_connect failed: %s", SPI_result_code_string(rc)))));

    portal = SPI_cursor_find(name);
    if (portal == NULL)
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_CURSOR), errmsg("cursor \"%s\" does not exist", name)));

    SPI_cursor_fetch(portal, true, FETCH_ALL);
    SPI_STACK_LOG("finish", portal->sourceText, NULL);
    return_result_to_client(SPI_processed, SPI_tuptable->tupdesc, SPI_tuptable->vals);
    SPI_finish();
    PG_RETURN_VOID();
}

Datum
gms_sql_return_result_i(PG_FUNCTION_ARGS)
{
    CursorData    *cursor;
    int    rc = 0;
    Datum    Values;
    char    Nulls;

    cursor = get_cursor(fcinfo, true);
    /*
     * Connect to SPI manager
     */
    if ((rc = SPI_connect() != SPI_OK_CONNECT))
        ereport(ERROR,
            (errmodule(MOD_OPT),
                (errcode(ERRCODE_SPI_CONNECTION_FAILURE),
                    errmsg("SPI_connect failed: %s", SPI_result_code_string(rc)))));

    if (cursor->plan)
           SPI_execute_plan(cursor->plan, &Values, &Nulls, false, 0);
    else if (cursor->parsed_query)
           SPI_execute(cursor->parsed_query, false, 0);
    else
        ereport(ERROR,
            (errmodule(MOD_OPT),
                (errcode(ERRCODE_SPI_EXECUTE_FAILURE),
                    errmsg("sql is not parsed: %s", SPI_result_code_string(rc)))));

    return_result_to_client(SPI_processed, SPI_tuptable->tupdesc, SPI_tuptable->vals);
    SPI_finish();
    PG_RETURN_VOID();
}
