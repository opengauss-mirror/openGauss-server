/*-------------------------------------------------------------------------
 *
 * test_decoding.c
 *		  example logical decoding output plugin
 *
 * Copyright (c) 2012-2014, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  contrib/test_decoding/test_decoding.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/sysattr.h"

#include "catalog/pg_class.h"
#include "catalog/pg_type.h"

#include "nodes/parsenodes.h"

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
static void pg_output_begin(
    LogicalDecodingContext* ctx, PluginTestDecodingData* data, ReorderBufferTXN* txn, bool last_write);
static void pg_decode_commit_txn(LogicalDecodingContext* ctx, ReorderBufferTXN* txn, XLogRecPtr commit_lsn);
static void pg_decode_abort_txn(LogicalDecodingContext* ctx, ReorderBufferTXN* txn);
static void pg_decode_prepare_txn(LogicalDecodingContext* ctx, ReorderBufferTXN* txn);

static void pg_decode_change(
    LogicalDecodingContext* ctx, ReorderBufferTXN* txn, Relation rel, ReorderBufferChange* change);
static bool pg_decode_filter(LogicalDecodingContext* ctx, RepOriginId origin_id);

void _PG_init(void)
{
    /* other plugins can perform things here */
}

/* specify output plugin callbacks */
void _PG_output_plugin_init(OutputPluginCallbacks* cb)
{
    AssertVariableIsOfType(&_PG_output_plugin_init, LogicalOutputPluginInit);

    cb->startup_cb = pg_decode_startup;
    cb->begin_cb = pg_decode_begin_txn;
    cb->change_cb = pg_decode_change;
    cb->commit_cb = pg_decode_commit_txn;
    cb->abort_cb = pg_decode_abort_txn;
    cb->prepare_cb = pg_decode_prepare_txn;
    cb->filter_by_origin_cb = pg_decode_filter;
    cb->shutdown_cb = pg_decode_shutdown;
}

/* initialize this plugin */
static void pg_decode_startup(LogicalDecodingContext* ctx, OutputPluginOptions* opt, bool is_init)
{
    ListCell* option = NULL;
    PluginTestDecodingData* data = NULL;

    data = (PluginTestDecodingData*)palloc0(sizeof(PluginTestDecodingData));
    data->context = AllocSetContextCreate(ctx->context,
        "text conversion context",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);
    data->include_xids = true;
    data->include_timestamp = true;
    data->skip_empty_xacts = false;
    data->only_local = true;
    data->tableWhiteList = NIL;

    ctx->output_plugin_private = data;

    opt->output_type = OUTPUT_PLUGIN_TEXTUAL_OUTPUT;

    foreach (option, ctx->output_plugin_options) {
        ParseDecodingOptionPlugin(option, data, opt);
    }
}

/* cleanup this plugin's resources */
static void pg_decode_shutdown(LogicalDecodingContext* ctx)
{
    PluginTestDecodingData* data = (PluginTestDecodingData*)ctx->output_plugin_private;

    /* cleanup our own resources via memory context reset */
    MemoryContextDelete(data->context);
}

/* BEGIN callback */
static void pg_decode_begin_txn(LogicalDecodingContext* ctx, ReorderBufferTXN* txn)
{
    PluginTestDecodingData* data = (PluginTestDecodingData*)ctx->output_plugin_private;

    data->xact_wrote_changes = false;
    if (data->skip_empty_xacts) {
        return;
    }

    pg_output_begin(ctx, data, txn, true);
}

static void pg_output_begin(LogicalDecodingContext* ctx, PluginTestDecodingData* data, ReorderBufferTXN* txn,
    bool last_write)
{
    OutputPluginPrepareWrite(ctx, last_write);
    if (data->include_xids)
        appendStringInfo(ctx->out, "BEGIN %lu", txn->xid);
    else
        appendStringInfoString(ctx->out, "BEGIN");
    OutputPluginWrite(ctx, last_write);
}

/* COMMIT callback */
static void pg_decode_commit_txn(LogicalDecodingContext* ctx, ReorderBufferTXN* txn, XLogRecPtr commit_lsn)
{
    PluginTestDecodingData* data = (PluginTestDecodingData*)ctx->output_plugin_private;

    if (data->skip_empty_xacts && !data->xact_wrote_changes)
        return;

    OutputPluginPrepareWrite(ctx, true);
    if (data->include_xids)
        appendStringInfo(ctx->out, "COMMIT %lu", txn->xid);
    else
        appendStringInfoString(ctx->out, "COMMIT");

    if (data->include_timestamp)
        appendStringInfo(ctx->out, " (at %s)", timestamptz_to_str(txn->commit_time));
    appendStringInfo(ctx->out, " CSN %lu", txn->csn);

    OutputPluginWrite(ctx, true);
}

/* ABORT callback */
static void pg_decode_abort_txn(LogicalDecodingContext* ctx, ReorderBufferTXN* txn)
{
    PluginTestDecodingData* data = (PluginTestDecodingData*)ctx->output_plugin_private;

    if (data->skip_empty_xacts && !data->xact_wrote_changes)
        return;

    OutputPluginPrepareWrite(ctx, true);
    if (data->include_xids)
        appendStringInfo(ctx->out, "ABORT %lu", txn->xid);
    else
        appendStringInfoString(ctx->out, "ABORT");

    if (data->include_timestamp)
        appendStringInfo(ctx->out, " (at %s)", timestamptz_to_str(txn->commit_time));

    OutputPluginWrite(ctx, true);
}

/* PREPARE callback */
static void pg_decode_prepare_txn(LogicalDecodingContext* ctx, ReorderBufferTXN* txn)
{
    PluginTestDecodingData* data = (PluginTestDecodingData*)ctx->output_plugin_private;

    if (data->skip_empty_xacts && !data->xact_wrote_changes)
        return;

    OutputPluginPrepareWrite(ctx, true);
    if (data->include_xids)
        appendStringInfo(ctx->out, "PREPARE %lu", txn->xid);
    else
        appendStringInfoString(ctx->out, "PREPARE");

    if (data->include_timestamp)
        appendStringInfo(ctx->out, " (at %s)", timestamptz_to_str(txn->commit_time));

    OutputPluginWrite(ctx, true);
}


static bool pg_decode_filter(LogicalDecodingContext* ctx, RepOriginId origin_id)
{
    PluginTestDecodingData* data = (PluginTestDecodingData*)ctx->output_plugin_private;

    if (data->only_local && origin_id != InvalidRepOriginId)
        return true;
    return false;
}
static void tuple_to_stringinfo(StringInfo s, TupleDesc tupdesc, HeapTuple tuple, bool skip_nulls)
{
    if (HEAP_TUPLE_IS_COMPRESSED(tuple->t_data))
        return;

    if ((int)HeapTupleHeaderGetNatts(tuple->t_data, tupdesc) != tupdesc->natts)
        return;

    int natt;
    Oid oid;

    /* print oid of tuple, it's not included in the TupleDesc */
    if ((oid = HeapTupleHeaderGetOid(tuple->t_data)) != InvalidOid) {
        appendStringInfo(s, " oid[oid]:%u", oid);
    }

    /* print all columns individually */
    for (natt = 0; natt < tupdesc->natts; natt++) {
        Form_pg_attribute attr; /* the attribute itself */
        Oid typid;              /* type of current attribute */
        Oid typoutput;          /* output function */
        bool typisvarlena = false;
        Datum origval;      /* possibly toasted Datum */
        bool isnull = true; /* column is null? */

        attr = tupdesc->attrs[natt];

        /*
         * don't print dropped columns, we can't be sure everything is
         * available for them
         */
        if (attr->attisdropped)
            continue;

        /*
         * Don't print system columns, oid will already have been printed if
         * present.
         */
        if (attr->attnum < 0)
            continue;

        typid = attr->atttypid;

        /* get Datum from tuple */
        origval = heap_getattr(tuple, natt + 1, tupdesc, &isnull);

        if (isnull && skip_nulls)
            continue;

        /* print attribute name */
        appendStringInfoChar(s, ' ');
        appendStringInfoString(s, quote_identifier(NameStr(attr->attname)));

        /* print attribute type */
        appendStringInfoChar(s, '[');
        char* type_name = format_type_be(typid);
        if (strlen(type_name) == strlen("clob") && strncmp(type_name, "clob", strlen("clob")) == 0) {
            errno_t rc = strcpy_s(type_name, sizeof("clob"), "text");
            securec_check_c(rc, "\0", "\0");
        }
        appendStringInfoString(s, type_name);
        appendStringInfoChar(s, ']');

        /* query output function */
        getTypeOutputInfo(typid, &typoutput, &typisvarlena);

        /* print separator */
        appendStringInfoChar(s, ':');

        /* print data */
        if (isnull)
            appendStringInfoString(s, "null");
        else if (typisvarlena && VARATT_IS_EXTERNAL_ONDISK_B(origval))
            appendStringInfoString(s, "unchanged-toast-datum");
        else if (!typisvarlena)
            PrintLiteral(s, typid, OidOutputFunctionCall(typoutput, origval));
        else {
            Datum val; /* definitely detoasted Datum */
            val = PointerGetDatum(PG_DETOAST_DATUM(origval));
            PrintLiteral(s, typid, OidOutputFunctionCall(typoutput, val));
        }
    }
}
/*
 * callback for individual changed tuples
 */
static void pg_decode_change(
    LogicalDecodingContext* ctx, ReorderBufferTXN* txn, Relation relation, ReorderBufferChange* change)
{
    PluginTestDecodingData* data = NULL;
    Form_pg_class class_form;
    TupleDesc tupdesc;
    MemoryContext old;

    data = (PluginTestDecodingData*)ctx->output_plugin_private;

    /* output BEGIN if we haven't yet */
    if (data->skip_empty_xacts && !data->xact_wrote_changes) {
        pg_output_begin(ctx, data, txn, false);
    }
    data->xact_wrote_changes = true;

    class_form = RelationGetForm(relation);
    tupdesc = RelationGetDescr(relation);

    /* Avoid leaking memory by using and resetting our own context */
    old = MemoryContextSwitchTo(data->context);

    char *schema = get_namespace_name(class_form->relnamespace);
    char *table = NameStr(class_form->relname);
    if (data->tableWhiteList != NIL && !CheckWhiteList(data->tableWhiteList, schema, table)) {
        (void)MemoryContextSwitchTo(old);
        MemoryContextReset(data->context);
        return;
    }

    OutputPluginPrepareWrite(ctx, true);

    appendStringInfoString(ctx->out, "table ");
    appendStringInfoString(ctx->out, quote_qualified_identifier(schema, table));
    appendStringInfoString(ctx->out, ":");

    switch (change->action) {
        case REORDER_BUFFER_CHANGE_INSERT:
            appendStringInfoString(ctx->out, " INSERT:");
            if (change->data.tp.newtuple == NULL)
                appendStringInfoString(ctx->out, " (no-tuple-data)");
            else
                tuple_to_stringinfo(ctx->out, tupdesc, &change->data.tp.newtuple->tuple, false);
            break;
        case REORDER_BUFFER_CHANGE_UPDATE:
            appendStringInfoString(ctx->out, " UPDATE:");
            if (change->data.tp.oldtuple != NULL) {
                appendStringInfoString(ctx->out, " old-key:");
                tuple_to_stringinfo(ctx->out, tupdesc, &change->data.tp.oldtuple->tuple, true);
                appendStringInfoString(ctx->out, " new-tuple:");
            }

            if (change->data.tp.newtuple == NULL)
                appendStringInfoString(ctx->out, " (no-tuple-data)");
            else
                tuple_to_stringinfo(ctx->out, tupdesc, &change->data.tp.newtuple->tuple, false);
            break;
        case REORDER_BUFFER_CHANGE_DELETE:
            appendStringInfoString(ctx->out, " DELETE:");

            /* if there was no PK, we only know that a delete happened */
            if (change->data.tp.oldtuple == NULL)
                appendStringInfoString(ctx->out, " (no-tuple-data)");
            /* In DELETE, only the replica identity is present; display that */
            else
                tuple_to_stringinfo(ctx->out, tupdesc, &change->data.tp.oldtuple->tuple, true);
            break;
        default:
            Assert(false);
    }

    MemoryContextSwitchTo(old);
    MemoryContextReset(data->context);

    OutputPluginWrite(ctx, true);
}
