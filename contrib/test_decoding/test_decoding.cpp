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
#include "tcop/ddldeparse.h"

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
static void pg_decode_truncate(LogicalDecodingContext *ctx,
                               ReorderBufferTXN *txn,
                               int nrelations, Relation relations[],
                               ReorderBufferChange *change);
static void pg_decode_ddl(LogicalDecodingContext* ctx, ReorderBufferTXN* txn, XLogRecPtr message_lsn,
    const char *prefix, Oid relid, DeparsedCommandType cmdtype, Size sz, const char *message);

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
    cb->truncate_cb = pg_decode_truncate;
    cb->commit_cb = pg_decode_commit_txn;
    cb->abort_cb = pg_decode_abort_txn;
    cb->prepare_cb = pg_decode_prepare_txn;
    cb->filter_by_origin_cb = pg_decode_filter;
    cb->shutdown_cb = pg_decode_shutdown;
    cb->ddl_cb = pg_decode_ddl;
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
                tuple_to_stringinfo(relation, ctx->out, tupdesc, &change->data.tp.newtuple->tuple, false);
            break;
        case REORDER_BUFFER_CHANGE_UINSERT:
            appendStringInfoString(ctx->out, " INSERT:");
            if (change->data.utp.newtuple == NULL)
                appendStringInfoString(ctx->out, " (no-tuple-data)");
            else
                tuple_to_stringinfo(relation, ctx->out, tupdesc, (HeapTuple)(&change->data.utp.newtuple->tuple), false);
            break;
        case REORDER_BUFFER_CHANGE_UPDATE:
            appendStringInfoString(ctx->out, " UPDATE:");
            if (change->data.tp.oldtuple != NULL) {
                appendStringInfoString(ctx->out, " old-key:");
                tuple_to_stringinfo(relation, ctx->out, tupdesc, &change->data.tp.oldtuple->tuple, true);
                appendStringInfoString(ctx->out, " new-tuple:");
            }

            if (change->data.tp.newtuple == NULL)
                appendStringInfoString(ctx->out, " (no-tuple-data)");
            else
                tuple_to_stringinfo(relation, ctx->out, tupdesc, &change->data.tp.newtuple->tuple, false);
            break;
        case REORDER_BUFFER_CHANGE_UUPDATE:
            appendStringInfoString(ctx->out, " UPDATE:");
            if (change->data.utp.oldtuple != NULL) {
                appendStringInfoString(ctx->out, " old-key:");
                tuple_to_stringinfo(relation, ctx->out, tupdesc, (HeapTuple)(&change->data.utp.oldtuple->tuple), true);
                appendStringInfoString(ctx->out, " new-tuple:");
            }

            if (change->data.utp.newtuple == NULL)
                appendStringInfoString(ctx->out, " (no-tuple-data)");
            else
                tuple_to_stringinfo(relation, ctx->out, tupdesc, (HeapTuple)(&change->data.utp.newtuple->tuple), false);
            break;
        case REORDER_BUFFER_CHANGE_DELETE:
            appendStringInfoString(ctx->out, " DELETE:");

            /* if there was no PK, we only know that a delete happened */
            if (change->data.tp.oldtuple == NULL)
                appendStringInfoString(ctx->out, " (no-tuple-data)");
            /* In DELETE, only the replica identity is present; display that */
            else
                tuple_to_stringinfo(relation, ctx->out, tupdesc, &change->data.tp.oldtuple->tuple, true);
            break;
        case REORDER_BUFFER_CHANGE_UDELETE:
            appendStringInfoString(ctx->out, " DELETE:");

            if (change->data.utp.oldtuple == NULL)
                appendStringInfoString(ctx->out, " (no-tuple-data)");
            else
                tuple_to_stringinfo(relation, ctx->out, tupdesc, (HeapTuple)(&change->data.utp.oldtuple->tuple), true);
            break;
        default:
            Assert(false);
    }

    MemoryContextSwitchTo(old);
    MemoryContextReset(data->context);

    OutputPluginWrite(ctx, true);
}

static void pg_decode_truncate(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
                   int nrelations, Relation relations[], ReorderBufferChange *change)
{
    PluginTestDecodingData *data;
    MemoryContext old;
    int            i;

    data = (PluginTestDecodingData*)ctx->output_plugin_private;

    /* output BEGIN if we haven't yet */
    if (data->skip_empty_xacts && !data->xact_wrote_changes) {
        pg_output_begin(ctx, data, txn, false);
    }
    data->xact_wrote_changes = true;

    /* Avoid leaking memory by using and resetting our own context */
    old = MemoryContextSwitchTo(data->context);

    OutputPluginPrepareWrite(ctx, true);

    appendStringInfoString(ctx->out, "table ");

    for (i = 0; i < nrelations; i++) {
        if (i > 0)
            appendStringInfoString(ctx->out, ", ");

        appendStringInfoString(ctx->out,
                               quote_qualified_identifier(get_namespace_name(relations[i]->rd_rel->relnamespace),
                                                          NameStr(relations[i]->rd_rel->relname)));
    }

    appendStringInfoString(ctx->out, ": TRUNCATE:");

    if (change->data.truncate.restart_seqs
        || change->data.truncate.cascade) {
        if (change->data.truncate.restart_seqs)
            appendStringInfo(ctx->out, " restart_seqs");
        if (change->data.truncate.cascade)
            appendStringInfo(ctx->out, " cascade");
    } else
        appendStringInfoString(ctx->out, " (no-flags)");

    MemoryContextSwitchTo(old);
    MemoryContextReset(data->context);

    OutputPluginWrite(ctx, true);
}

static char *deparse_command_type(DeparsedCommandType cmdtype)
{
    switch (cmdtype) {
        case DCT_SimpleCmd:
            return "Simple";
        case DCT_TableDropStart:
            return "Drop Table";
        case DCT_TableDropEnd:
            return "Drop Table End";
        case DCT_TableAlter:
            return "Alter Table";
        case DCT_ObjectCreate:
            return "Create Object";
        case DCT_ObjectDrop:
            return "Drop Object";
        case DCT_TypeDropStart:
            return "Drop Type";
        case DCT_TypeDropEnd:
            return "Drop Type End";
        default:
            Assert(false);
    }
    return NULL;
}

static void pg_decode_ddl(LogicalDecodingContext* ctx, 
                        ReorderBufferTXN* txn, XLogRecPtr message_lsn,
                        const char *prefix, Oid relid, 
                        DeparsedCommandType cmdtype, 
                        Size sz, const char *message)
{
    PluginTestDecodingData* data = NULL;
    MemoryContext old;

    data = (PluginTestDecodingData*)ctx->output_plugin_private;

    /* output BEGIN if we haven't yet */
    if (data->skip_empty_xacts && !data->xact_wrote_changes) {
        pg_output_begin(ctx, data, txn, false);
    }
    data->xact_wrote_changes = true;

    /* Avoid leaking memory by using and resetting our own context */
    old = MemoryContextSwitchTo(data->context);
    OutputPluginPrepareWrite(ctx, true);

    appendStringInfo(ctx->out, "message: prefix %s, relid %u, cmdtype: %s, sz: %lu content: %s",
        prefix,
        relid,
        deparse_command_type(cmdtype),
        sz,
        message);
    if (cmdtype != DCT_TableDropStart) {
        char *tmp = pstrdup(message);
        char *owner = NULL;
        char *decodestring = deparse_ddl_json_to_string(tmp, &owner);
        appendStringInfo(ctx->out, "\ndecode to : %s, [owner %s]", decodestring, owner ? owner : "none");
        pfree(tmp);
    }

    MemoryContextSwitchTo(old);
    MemoryContextReset(data->context);

    OutputPluginWrite(ctx, true);
}