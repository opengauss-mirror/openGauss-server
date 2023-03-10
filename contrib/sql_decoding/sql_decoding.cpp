/*
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2012-2014, PostgreSQL Global Development Group
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * ---------------------------------------------------------------------------------------
 *
 * sql_decoding.cpp
 *        logical decoding output plugin (sql)
 *
 *
 *
 * IDENTIFICATION
 *        contrib/sql_decoding/sql_decoding.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/sysattr.h"
#include "access/ustore/knl_utuple.h"

#include "catalog/pg_class.h"
#include "catalog/pg_type.h"

#include "nodes/parsenodes.h"
#include "replication/logical.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/syscache.h"
#include "utils/typcache.h"
#include "replication/output_plugin.h"
#include "replication/logical.h"

PG_MODULE_MAGIC;

/* These must be available to pg_dlsym() */
extern "C" void _PG_init(void);
extern "C" void _PG_output_plugin_init(OutputPluginCallbacks* cb);

static void pg_decode_startup(LogicalDecodingContext* ctx, OutputPluginOptions* opt, bool is_init);
static void pg_decode_shutdown(LogicalDecodingContext* ctx);
static void pg_decode_begin_txn(LogicalDecodingContext* ctx, ReorderBufferTXN* txn);
static void pg_decode_commit_txn(LogicalDecodingContext* ctx, ReorderBufferTXN* txn, XLogRecPtr commit_lsn);
static void pg_decode_abort_txn(LogicalDecodingContext* ctx, ReorderBufferTXN* txn);
static void pg_decode_change(
    LogicalDecodingContext* ctx, ReorderBufferTXN* txn, Relation rel, ReorderBufferChange* change);
static bool pg_decode_filter(LogicalDecodingContext* ctx, RepOriginId origin_id);

typedef struct {
    MemoryContext context;
    bool include_xids;
    bool include_timestamp;
    bool skip_empty_xacts;
    bool xact_wrote_changes;
    bool only_local;
} TestDecodingData;

/* specify output plugin callbacks */
void _PG_output_plugin_init(OutputPluginCallbacks* cb)
{
    AssertVariableIsOfType(&_PG_output_plugin_init, LogicalOutputPluginInit);

    cb->startup_cb = pg_decode_startup;
    cb->begin_cb = pg_decode_begin_txn;
    cb->change_cb = pg_decode_change;
    cb->commit_cb = pg_decode_commit_txn;
    cb->abort_cb = pg_decode_abort_txn;
    cb->filter_by_origin_cb = pg_decode_filter;
    cb->shutdown_cb = pg_decode_shutdown;
}

void _PG_init(void)
{
    /* other plugins can perform things here */
}

/* initialize this plugin */
static void pg_decode_startup(LogicalDecodingContext* ctx, OutputPluginOptions* opt, bool is_init = true)
{
    ListCell* option = NULL;

    TestDecodingData *data = (TestDecodingData*)palloc0(sizeof(TestDecodingData));
    data->context = AllocSetContextCreate(ctx->context,
        "text conversion context", ALLOCSET_DEFAULT_SIZES);
    data->include_xids = true;
    data->include_timestamp = false;
    data->skip_empty_xacts = false;
    data->only_local = true;

    ctx->output_plugin_private = data;

    opt->output_type = OUTPUT_PLUGIN_TEXTUAL_OUTPUT;

    foreach (option, ctx->output_plugin_options) {
        DefElem* elem = (DefElem*)lfirst(option);

        Assert(elem->arg == NULL || IsA(elem->arg, String));

        if (strcmp(elem->defname, "include-xids") == 0) {
            /* if option does not provide a value, it means its value is true */
            if (elem->arg == NULL) {
                data->include_xids = true;
            } else if (!parse_bool(strVal(elem->arg), &data->include_xids)) {
                ereport(ERROR, (errmodule(MOD_LOGICAL_DECODE), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("could not parse value \"%s\" for parameter \"%s\"", strVal(elem->arg), elem->defname),
                    errdetail("N/A"),  errcause("Wrong input value"), erraction("Input \"on\" or \"off\"")));
            }
        } else if (strcmp(elem->defname, "include-timestamp") == 0) {
            if (elem->arg == NULL) {
                data->include_timestamp = true;
            } else if (!parse_bool(strVal(elem->arg), &data->include_timestamp)) {
                ereport(ERROR, (errmodule(MOD_LOGICAL_DECODE), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("could not parse value \"%s\" for parameter \"%s\"", strVal(elem->arg), elem->defname),
                    errdetail("N/A"),  errcause("Wrong input value"), erraction("Input \"on\" or \"off\"")));
            }
        } else if (strcmp(elem->defname, "skip-empty-xacts") == 0) {
            if (elem->arg == NULL) {
                data->skip_empty_xacts = true;
            } else if (!parse_bool(strVal(elem->arg), &data->skip_empty_xacts)) {
                ereport(ERROR, (errmodule(MOD_LOGICAL_DECODE), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("could not parse value \"%s\" for parameter \"%s\"", strVal(elem->arg), elem->defname),
                    errdetail("N/A"),  errcause("Wrong input value"), erraction("Input \"on\" or \"off\"")));
            }
        } else if (strcmp(elem->defname, "only-local") == 0) {
            if (elem->arg == NULL) {
                data->only_local = true;
            } else if (!parse_bool(strVal(elem->arg), &data->only_local)) {
                ereport(ERROR, (errmodule(MOD_LOGICAL_DECODE), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("could not parse value \"%s\" for parameter \"%s\"", strVal(elem->arg), elem->defname),
                    errdetail("N/A"),  errcause("Wrong input value"), erraction("Input \"on\" or \"off\"")));
            }
        } else {
            ereport(ERROR, (errmodule(MOD_LOGICAL_DECODE), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("option \"%s\" = \"%s\" is unknown", elem->defname, elem->arg ? strVal(elem->arg) : "(null)"),
                errdetail("N/A"),  errcause("Wrong input option"),
                erraction("Check the product documentation for legal options")));
        }
    }
}

/* cleanup this plugin's resources */
static void pg_decode_shutdown(LogicalDecodingContext* ctx)
{
    TestDecodingData* data = (TestDecodingData*)ctx->output_plugin_private;

    /* cleanup our own resources via memory context reset */
    MemoryContextDelete(data->context);
}

/*
 * Prepare output plugin.
 */
void pg_output_begin(LogicalDecodingContext* ctx, TestDecodingData* data, ReorderBufferTXN* txn, bool last_write)
{
    OutputPluginPrepareWrite(ctx, last_write);
    appendStringInfo(ctx->out, "BEGIN %lu", txn->csn);
    OutputPluginWrite(ctx, last_write);
}

/* BEGIN callback */
static void pg_decode_begin_txn(LogicalDecodingContext* ctx, ReorderBufferTXN* txn)
{
    TestDecodingData* data = (TestDecodingData*)ctx->output_plugin_private;

    data->xact_wrote_changes = false;
    if (data->skip_empty_xacts) {
        return;
    }
    pg_output_begin(ctx, data, txn, true);
}

/* COMMIT callback */
static void pg_decode_commit_txn(LogicalDecodingContext* ctx, ReorderBufferTXN* txn, XLogRecPtr commit_lsn)
{
    TestDecodingData* data = (TestDecodingData*)ctx->output_plugin_private;

    if (data->skip_empty_xacts && !data->xact_wrote_changes) {
        return;
    }

    OutputPluginPrepareWrite(ctx, true);
    appendStringInfoString(ctx->out, "COMMIT");
        appendStringInfo(ctx->out, " (at %s)", timestamptz_to_str(txn->commit_time));
    appendStringInfo(ctx->out, " %lu", txn->csn);
    OutputPluginWrite(ctx, true);
}

/* ABORT callback */
static void pg_decode_abort_txn(LogicalDecodingContext* ctx, ReorderBufferTXN* txn)
{
    TestDecodingData* data = (TestDecodingData*)ctx->output_plugin_private;

    if (data->skip_empty_xacts && !data->xact_wrote_changes) {
        return;
    }
    OutputPluginPrepareWrite(ctx, true);
    if (data->include_xids) {
        appendStringInfo(ctx->out, "ABORT %lu", txn->xid);
    } else {
        appendStringInfoString(ctx->out, "ABORT");
    }

    if (data->include_timestamp) {
        appendStringInfo(ctx->out, " (at %s)", timestamptz_to_str(txn->commit_time));
    }
    OutputPluginWrite(ctx, true);
}

static bool pg_decode_filter(LogicalDecodingContext* ctx, RepOriginId origin_id)
{
    TestDecodingData* data = (TestDecodingData*)ctx->output_plugin_private;

    if (data->only_local && origin_id != InvalidRepOriginId) {
        return true;
    }
    return false;
}

/*
 * Print literal `outputstr' already represented as string of type `typid'
 * into stringbuf `s'.
 *
 * Some builtin types aren't quoted, the rest is quoted. Escaping is done as
 * if u_sess->parser_cxt.standard_conforming_strings were enabled.
 */
static void print_literal(StringInfo s, Oid typid, char* outputstr)
{
    const char* valptr = NULL;

    switch (typid) {
        case FLOAT4OID:
        case FLOAT8OID:
        case NUMERICOID:
        case INT1OID:
        case INT2OID:
        case INT4OID:
        case INT8OID:
        case OIDOID:

            /* NB: We don't care about Inf, NaN et al. */
            appendStringInfoString(s, outputstr);
            break;

        case BITOID:
        case VARBITOID:
            appendStringInfo(s, "B'%s'", outputstr);
            break;

        case BOOLOID:
            if (strcmp(outputstr, "t") == 0) {
                appendStringInfoString(s, "true");
            } else {
                appendStringInfoString(s, "false");
            }
            break;

        default:
            appendStringInfoChar(s, '\'');
            for (valptr = outputstr; *valptr; valptr++) {
                char ch = *valptr;

                if (SQL_STR_DOUBLE(ch, false)) {
                    appendStringInfoChar(s, ch);
                }
                appendStringInfoChar(s, ch);
            }
            appendStringInfoChar(s, '\'');
            break;
    }
}

/*
 * Decode tuple into stringinfo.
 */
static void TupleToStringinfo(StringInfo s, TupleDesc tupdesc, HeapTuple tuple)
{
    Assert(tuple != NULL);
    if ((tuple->tupTableType == HEAP_TUPLE) && (HEAP_TUPLE_IS_COMPRESSED(tuple->t_data) ||
        (int)HeapTupleHeaderGetNatts(tuple->t_data, tupdesc) > tupdesc->natts)) {
        return;
    }

    appendStringInfoChar(s, '(');
    /* print all columns individually */
    for (int natt = 0; natt < tupdesc->natts; natt++) {
        bool isnull = false;        /* column is null? */
        bool typisvarlena = false;
        Oid typoutput = 0;          /* output function */
        Datum origval = 0;          /* possibly toasted Datum */

        Form_pg_attribute attr = &tupdesc->attrs[natt]; /* the attribute itself */

        if (attr->attisdropped || attr->attnum < 0) {
            continue;
        }

        /* get Datum from tuple */
        if (tuple->tupTableType == HEAP_TUPLE) {
            origval = heap_getattr(tuple, natt + 1, tupdesc, &isnull);
        } else {
            origval = uheap_getattr((UHeapTuple)tuple, natt + 1, tupdesc, &isnull);
        }

        /* query output function */
        Oid typid = attr->atttypid; /* type of current attribute */
        getTypeOutputInfo(typid, &typoutput, &typisvarlena);

        /* print data */
        if (isnull) {
            appendStringInfoString(s, "null");
        } else if (!typisvarlena) {
            print_literal(s, typid, OidOutputFunctionCall(typoutput, origval));
        } else {
            Datum val = PointerGetDatum(PG_DETOAST_DATUM(origval));
            print_literal(s, typid, OidOutputFunctionCall(typoutput, val));
        }
        if (natt < tupdesc->natts - 1) {
            appendStringInfoString(s, ", ");
        }
    }
    appendStringInfoChar(s, ')');
}

/*
 * Decode tuple into stringinfo.
 * This function is used for UPDATE or DELETE statements.
 */
static void TupleToStringinfoUpd(Relation relation, StringInfo s, TupleDesc tupdesc, HeapTuple tuple)
{
    if ((tuple->tupTableType == HEAP_TUPLE) && (HEAP_TUPLE_IS_COMPRESSED(tuple->t_data) ||
        (int)HeapTupleHeaderGetNatts(tuple->t_data, tupdesc) > tupdesc->natts)) {
        return;
    }

    bool isFirstAtt = true;
    /* print all columns individually */
    for (int natt = 0; natt < tupdesc->natts; natt++) {
        Oid typoutput = 0;          /* output function */
        Datum origval = 0;          /* possibly toasted Datum */
        bool isnull = false;        /* column is null? */
        bool typisvarlena = false;

        Form_pg_attribute attr = &tupdesc->attrs[natt]; /* the attribute itself */

        if (attr->attisdropped || attr->attnum < 0 || !IsRelationReplidentKey(relation, attr->attnum)) {
            continue;
        }

        /* get Datum from tuple */
        if (tuple->tupTableType == HEAP_TUPLE) {
            origval = heap_getattr(tuple, natt + 1, tupdesc, &isnull);
        } else {
            origval = uheap_getattr((UHeapTuple)tuple, natt + 1, tupdesc, &isnull);
        }

        if (!isFirstAtt) {
            appendStringInfoString(s, " and ");
        } else {
            isFirstAtt = false;
        }
        /* print attribute name */
        appendStringInfoString(s, quote_identifier(NameStr(attr->attname)));
        appendStringInfoString(s, " = ");
        /* query output function */
        Oid typid = attr->atttypid;
        getTypeOutputInfo(typid, &typoutput, &typisvarlena);

        /* print data */
        if (isnull) {
            appendStringInfoString(s, "null");
        } else if (!typisvarlena) {
            print_literal(s, typid, OidOutputFunctionCall(typoutput, origval));
        } else {
            Datum val = PointerGetDatum(PG_DETOAST_DATUM(origval));
            print_literal(s, typid, OidOutputFunctionCall(typoutput, val));
        }
    }
}

/*
 * Callback for handle decoded tuple.
 * Additional info will be added if the tuple is found null.
 */
static void TupleHandler(Relation relation, StringInfo s, TupleDesc tupdesc, ReorderBufferChange* change,
    bool isHeap, bool isNewTuple)
{
    if (isHeap && isNewTuple) {
        if (change->data.tp.newtuple == NULL) {
            appendStringInfoString(s, " (no-tuple-data)");
        } else {
            TupleToStringinfo(s, tupdesc, &change->data.tp.newtuple->tuple);
        }
    } else if (isHeap && !isNewTuple) {
        if (change->data.tp.oldtuple == NULL) {
            appendStringInfoString(s, " (no-tuple-data)");
        } else {
            TupleToStringinfoUpd(relation, s, tupdesc, &change->data.tp.oldtuple->tuple);
        }
    } else if (!isHeap && isNewTuple) {
        if (change->data.utp.newtuple == NULL) {
            appendStringInfoString(s, " (no-tuple-data)");
        } else {
            TupleToStringinfo(s, tupdesc, (HeapTuple)(&change->data.utp.newtuple->tuple));
        }
    } else {
        if (change->data.utp.oldtuple == NULL) {
            appendStringInfoString(s, " (no-tuple-data)");
        } else {
            TupleToStringinfoUpd(relation, s, tupdesc, (HeapTuple)(&change->data.utp.oldtuple->tuple));
        }
    }
}


/*
 * Callback for individual changed tuples.
 */
static void pg_decode_change(
    LogicalDecodingContext* ctx, ReorderBufferTXN* txn, Relation relation, ReorderBufferChange* change)
{
    Form_pg_class class_form = NULL;
    TupleDesc tupdesc = NULL;
    TestDecodingData* data = (TestDecodingData*)ctx->output_plugin_private;
    u_sess->attr.attr_common.extra_float_digits = 0;
    bool isHeap = true;
    /* output BEGIN if we haven't yet */
    if (txn != NULL && data->skip_empty_xacts && !data->xact_wrote_changes) {
        pg_output_begin(ctx, data, txn, false);
    }
    data->xact_wrote_changes = true;

        class_form = RelationGetForm(relation);
        tupdesc = RelationGetDescr(relation);
    /* Avoid leaking memory by using and resetting our own context */
    MemoryContext old = MemoryContextSwitchTo(data->context);

    char *schema = NULL;
    char *table = NULL;
        schema = get_namespace_name(class_form->relnamespace);
        table = NameStr(class_form->relname);

    OutputPluginPrepareWrite(ctx, true);

    switch (change->action) {
        case REORDER_BUFFER_CHANGE_INSERT:
        case REORDER_BUFFER_CHANGE_UINSERT:
            appendStringInfoString(ctx->out, "insert into ");

            appendStringInfoString(ctx->out, quote_qualified_identifier(schema, table));
            if (change->action == REORDER_BUFFER_CHANGE_UINSERT) {
                isHeap = false;
            }
            appendStringInfoString(ctx->out, " values ");
            TupleHandler(relation, ctx->out, tupdesc, change, isHeap, true);
            break;
        case REORDER_BUFFER_CHANGE_UPDATE:
        case REORDER_BUFFER_CHANGE_UUPDATE:

            appendStringInfoString(ctx->out, "delete from ");
            appendStringInfoString(ctx->out, quote_qualified_identifier(schema, table));
            if (change->action == REORDER_BUFFER_CHANGE_UUPDATE) {
                isHeap = false;
            }
            appendStringInfoString(ctx->out, " where ");
            TupleHandler(relation, ctx->out, tupdesc, change, isHeap, false);
            appendStringInfoChar(ctx->out, ';');
            appendStringInfoString(ctx->out, "insert into ");
            appendStringInfoString(ctx->out, quote_qualified_identifier(schema, table));
            appendStringInfoString(ctx->out, " values ");
            TupleHandler(relation, ctx->out, tupdesc, change, isHeap, true);

            break;

        case REORDER_BUFFER_CHANGE_DELETE:
        case REORDER_BUFFER_CHANGE_UDELETE:
            appendStringInfoString(ctx->out, "delete from ");

            appendStringInfoString(ctx->out, quote_qualified_identifier(schema, table));
            if (change->action == REORDER_BUFFER_CHANGE_UDELETE) {
                isHeap = false;
            }
            appendStringInfoString(ctx->out, " where ");

            TupleHandler(relation, ctx->out, tupdesc, change, isHeap, false);
            break;
        default:
            Assert(false);
    }

    appendStringInfoChar(ctx->out, ';');
    MemoryContextSwitchTo(old);
    MemoryContextReset(data->context);

    OutputPluginWrite(ctx, true);
}
