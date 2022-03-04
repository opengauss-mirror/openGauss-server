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
 * mppdb_decoding.cpp
 *        logical decoding output plugin (json)
 *
 *
 *
 * IDENTIFICATION
 *        contrib/mppdb_decoding/mppdb_decoding.cpp
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

#include "replication/output_plugin.h"
#include "replication/logical.h"

#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/syscache.h"
#include "utils/typcache.h"

#include "cjson/cJSON.h"

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
    if (data->skip_empty_xacts)
        return;

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

/* print the tuple 'tuple' into the StringInfo s */
static void TupleToJsoninfo(
    cJSON* cols_name, cJSON* cols_type, cJSON* cols_val, TupleDesc tupdesc, HeapTuple tuple, bool skip_nulls)
{
    if ((tuple->tupTableType == HEAP_TUPLE) && (HEAP_TUPLE_IS_COMPRESSED(tuple->t_data) ||
        (int)HeapTupleHeaderGetNatts(tuple->t_data, tupdesc) > tupdesc->natts)) {
        return;
    }

    /* print all columns individually */
    for (int natt = 0; natt < tupdesc->natts; natt++) {
        Oid typoutput;          /* output function */
        bool typisvarlena = false;
        Datum origval = 0;      /* possibly toasted Datum */
        bool isnull = false; /* column is null? */

        Form_pg_attribute attr = tupdesc->attrs[natt]; /* the attribute itself */

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

        Oid typid = attr->atttypid; /* type of current attribute */

        /* get Datum from tuple */
        if (tuple->tupTableType == HEAP_TUPLE) {
            origval = heap_getattr(tuple, natt + 1, tupdesc, &isnull);
        } else {
            origval = uheap_getattr((UHeapTuple)tuple, natt + 1, tupdesc, &isnull);
        }
        if (isnull && skip_nulls)
            continue;

        /* print attribute name */

        cJSON* col_name = cJSON_CreateString(quote_identifier(NameStr(attr->attname)));
        cJSON_AddItemToArray(cols_name, col_name);

        /* print attribute type */
        if (cols_type != NULL) {
            char* type_name = format_type_be(typid);
            if (strlen(type_name) == strlen("clob") && strncmp(type_name, "clob", strlen("clob")) == 0) {
                errno_t rc = strcpy_s(type_name, sizeof("clob"), "text");
                securec_check_c(rc, "\0", "\0");
            }
            cJSON* col_type = cJSON_CreateString(type_name);
            cJSON_AddItemToArray(cols_type, col_type);
        }

        /* query output function */
        getTypeOutputInfo(typid, &typoutput, &typisvarlena);

        /* print separator */
        StringInfo val_str = makeStringInfo();
        /* print data */
        if (isnull)
            appendStringInfoString(val_str, "null");
        else if (!typisvarlena)
            PrintLiteral(val_str, typid, OidOutputFunctionCall(typoutput, origval));
        else {
            Datum val; /* definitely detoasted Datum */
            val = PointerGetDatum(PG_DETOAST_DATUM(origval));
            PrintLiteral(val_str, typid, OidOutputFunctionCall(typoutput, val));
        }
        cJSON* col_val = cJSON_CreateString(val_str->data);
        cJSON_AddItemToArray(cols_val, col_val);
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
    char* res = NULL;
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

    cJSON* root = cJSON_CreateObject();
    cJSON* table_name = cJSON_CreateString(quote_qualified_identifier(schema, table));
    cJSON_AddItemToObject(root, "table_name", table_name);

    cJSON* op_type = NULL;
    cJSON* columns_val = cJSON_CreateArray();
    cJSON* columns_name = cJSON_CreateArray();
    cJSON* columns_type = cJSON_CreateArray();
    cJSON* old_keys_name = cJSON_CreateArray();
    cJSON* old_keys_val = cJSON_CreateArray();
    cJSON* old_keys_type = cJSON_CreateArray();

    switch (change->action) {
        case REORDER_BUFFER_CHANGE_INSERT:
            op_type = cJSON_CreateString("INSERT");
            if (change->data.tp.newtuple != NULL) {
                TupleToJsoninfo(
                    columns_name, columns_type, columns_val, tupdesc, &change->data.tp.newtuple->tuple, false);
            }
            break;
        case REORDER_BUFFER_CHANGE_UPDATE:
            op_type = cJSON_CreateString("UPDATE");
            if (change->data.tp.oldtuple != NULL) {
                TupleToJsoninfo(
                    old_keys_name, old_keys_type, old_keys_val, tupdesc, &change->data.tp.oldtuple->tuple, true);
            }

            if (change->data.tp.newtuple != NULL) {
                TupleToJsoninfo(
                    columns_name, columns_type, columns_val, tupdesc, &change->data.tp.newtuple->tuple, false);
            }
            break;
        case REORDER_BUFFER_CHANGE_DELETE:
            op_type = cJSON_CreateString("DELETE");
            if (change->data.tp.oldtuple != NULL) {
                TupleToJsoninfo(
                    old_keys_name, old_keys_type, old_keys_val, tupdesc, &change->data.tp.oldtuple->tuple, true);
            }
            /* if there was no PK, we only know that a delete happened */
            break;
        case REORDER_BUFFER_CHANGE_UINSERT:
            op_type = cJSON_CreateString("INSERT");
            if (change->data.utp.newtuple != NULL) {
                TupleToJsoninfo(columns_name, columns_type, columns_val, tupdesc,
                    (HeapTuple)(&change->data.utp.newtuple->tuple), false);
            }
            break;
        case REORDER_BUFFER_CHANGE_UDELETE:
            op_type = cJSON_CreateString("DELETE");
            if (change->data.utp.oldtuple != NULL) {
                TupleToJsoninfo(old_keys_name, old_keys_type, old_keys_val, tupdesc,
                    (HeapTuple)(&change->data.utp.oldtuple->tuple), true);
            }
            break;
        case REORDER_BUFFER_CHANGE_UUPDATE:
            op_type = cJSON_CreateString("UPDATE");
            if (change->data.utp.oldtuple != NULL) {
                TupleToJsoninfo(old_keys_name, old_keys_type, old_keys_val, tupdesc,
                    (HeapTuple)(&change->data.utp.oldtuple->tuple), true);
            }

            if (change->data.utp.newtuple != NULL) {
                TupleToJsoninfo(columns_name, columns_type, columns_val, tupdesc,
                    (HeapTuple)&change->data.utp.newtuple->tuple, false);
            }
            break;
        default:
            Assert(false);
    }
    cJSON_AddItemToObject(root, "op_type", op_type);
    cJSON_AddItemToObject(root, "columns_name", columns_name);
    cJSON_AddItemToObject(root, "columns_type", columns_type);
    cJSON_AddItemToObject(root, "columns_val", columns_val);
    cJSON_AddItemToObject(root, "old_keys_name", old_keys_name);
    cJSON_AddItemToObject(root, "old_keys_type", old_keys_type);
    cJSON_AddItemToObject(root, "old_keys_val", old_keys_val);
    res = cJSON_PrintUnformatted(root);
    if (res != NULL)
        appendStringInfoString(ctx->out, res);
    cJSON_Delete(root);
    MemoryContextSwitchTo(old);
    MemoryContextReset(data->context);
    OutputPluginWrite(ctx, true);
}
