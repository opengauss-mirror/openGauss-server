/*
 * Copyright (c) 2026 Huawei Technologies Co.,Ltd.
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
 * -------------------------------------------------------------------------
 *
 * neon_oggit.cpp
 *
 * IDENTIFICATION
 *        contrib/neon_oggit/neon_oggit.cpp
 * NOTES
 * The plugin emits one compact JSON object per logical event.  It is intended
 * for the oggit worker, which persists these structured events into the oggit
 * metadata tables used by incremental branch diff/merge.
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/htup.h"
#include "access/sysattr.h"
#include "access/ustore/knl_utuple.h"
#include "access/xlogdefs.h"
#include "catalog/pg_class.h"
#include "catalog/pg_type.h"
#include "cjson/cJSON.h"
#include "nodes/parsenodes.h"
#include "replication/logical.h"
#include "replication/output_plugin.h"
#include "tcop/ddldeparse.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/relcache.h"

PG_MODULE_MAGIC;

extern "C" void _PG_init(void);
extern "C" void _PG_output_plugin_init(OutputPluginCallbacks *cb);

#ifndef ENABLE_NEON

void _PG_init(void)
{
}

void _PG_output_plugin_init(OutputPluginCallbacks *cb)
{
    (void)cb;
    ereport(ERROR, (errmsg("neon_oggit requires openGauss to be built with ENABLE_NEON")));
}

#else

typedef struct NeonOggitData {
    MemoryContext context;
    bool include_xids;
    bool include_timestamp;
    bool only_local;
    bool skip_empty_xacts;
    bool xact_wrote_changes;
    char *current_merge_id;
    char *current_merge_origin;
} NeonOggitData;

static void neon_oggit_startup(LogicalDecodingContext *ctx, OutputPluginOptions *opt, bool is_init);
static void neon_oggit_shutdown(LogicalDecodingContext *ctx);
static void neon_oggit_begin_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn);
static void neon_oggit_commit_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn, XLogRecPtr commit_lsn);
static void neon_oggit_abort_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn);
static void neon_oggit_prepare_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn);
static void neon_oggit_change(LogicalDecodingContext *ctx, ReorderBufferTXN *txn, Relation rel,
    ReorderBufferChange *change);
static void neon_oggit_truncate(LogicalDecodingContext *ctx, ReorderBufferTXN *txn, int nrelations,
    Relation relations[], ReorderBufferChange *change);
static void neon_oggit_ddl(LogicalDecodingContext *ctx, ReorderBufferTXN *txn, XLogRecPtr message_lsn,
    const char *prefix, Oid relid, DeparsedCommandType cmdtype, Size sz, const char *message);
static bool neon_oggit_filter(LogicalDecodingContext *ctx, RepOriginId origin_id);

void _PG_init(void)
{
}

void _PG_output_plugin_init(OutputPluginCallbacks *cb)
{
    AssertVariableIsOfType(&_PG_output_plugin_init, LogicalOutputPluginInit);

    cb->startup_cb = neon_oggit_startup;
    cb->begin_cb = neon_oggit_begin_txn;
    cb->change_cb = neon_oggit_change;
    cb->truncate_cb = neon_oggit_truncate;
    cb->commit_cb = neon_oggit_commit_txn;
    cb->abort_cb = neon_oggit_abort_txn;
    cb->prepare_cb = neon_oggit_prepare_txn;
    cb->shutdown_cb = neon_oggit_shutdown;
    cb->filter_by_origin_cb = neon_oggit_filter;
    cb->ddl_cb = neon_oggit_ddl;
}

static char *neon_oggit_lsn_to_cstring(XLogRecPtr lsn)
{
    char *result = (char *)palloc(32);

    errno_t rc = snprintf_s(result, 32, 31, "%X/%X", LSN_FORMAT_ARGS(lsn));
    securec_check_ss(rc, "\0", "\0");
    return result;
}

static char *neon_oggit_u64_to_cstring(uint64 value)
{
    char *result = (char *)palloc(32);

    errno_t rc = snprintf_s(result, 32, 31, UINT64_FORMAT, value);
    securec_check_ss(rc, "\0", "\0");
    return result;
}

static const char *neon_oggit_cmdtype_name(DeparsedCommandType cmdtype)
{
    switch (cmdtype) {
        case DCT_SimpleCmd:
            return "simple";
        case DCT_TableDropStart:
            return "table_drop_start";
        case DCT_TableDropEnd:
            return "table_drop_end";
        case DCT_TableAlter:
            return "table_alter";
        case DCT_ObjectCreate:
            return "object_create";
        case DCT_ObjectDrop:
            return "object_drop";
        case DCT_TypeDropStart:
            return "type_drop_start";
        case DCT_TypeDropEnd:
            return "type_drop_end";
        case DCT_NewPub:
            return "new_publication";
        default:
            return "unknown";
    }
}

static void neon_oggit_add_lsn(cJSON *root, const char *name, XLogRecPtr lsn)
{
    cJSON_AddStringToObject(root, name, neon_oggit_lsn_to_cstring(lsn));
}

static void neon_oggit_add_txn(cJSON *root, ReorderBufferTXN *txn)
{
    if (txn == NULL) {
        return;
    }

    cJSON_AddStringToObject(root, "xid", neon_oggit_u64_to_cstring(txn->xid));
    neon_oggit_add_lsn(root, "txn_first_lsn", txn->first_lsn);
    neon_oggit_add_lsn(root, "commit_lsn", txn->final_lsn);
    neon_oggit_add_lsn(root, "end_lsn", txn->end_lsn);
    cJSON_AddStringToObject(root, "csn", neon_oggit_u64_to_cstring(txn->csn));
    if (txn->commit_time != 0) {
        cJSON_AddStringToObject(root, "commit_time", timestamptz_to_str(txn->commit_time));
    }
}

static void neon_oggit_add_merge_origin(cJSON *root, NeonOggitData *data)
{
    if (data != NULL && data->current_merge_id != NULL && data->current_merge_id[0] != '\0') {
        cJSON_AddStringToObject(root, "merge_id", data->current_merge_id);
        if (data->current_merge_origin != NULL && data->current_merge_origin[0] != '\0') {
            cJSON_AddStringToObject(root, "merge_origin", data->current_merge_origin);
        }
    }
}

static void neon_oggit_emit_json(LogicalDecodingContext *ctx, cJSON *root)
{
    char *json = cJSON_PrintUnformatted(root);

    OutputPluginPrepareWrite(ctx, true);
    if (json != NULL) {
        appendStringInfoString(ctx->out, json);
        cJSON_free(json);
    }
    OutputPluginWrite(ctx, true);
}

static bool neon_oggit_is_internal_schema(const char *schema)
{
    return schema != NULL && (strcmp(schema, "oggit") == 0 || strcmp(schema, "_oggit") == 0);
}

static void neon_oggit_startup(LogicalDecodingContext *ctx, OutputPluginOptions *opt, bool is_init)
{
    NeonOggitData *data = (NeonOggitData *)palloc0(sizeof(NeonOggitData));

    (void)is_init;
    data->context = AllocSetContextCreate(ctx->context,
        "neon_oggit conversion context",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);
    data->include_xids = true;
    data->include_timestamp = true;
    data->only_local = true;
    data->skip_empty_xacts = true;
    data->xact_wrote_changes = false;
    data->current_merge_id = NULL;
    data->current_merge_origin = NULL;

    ctx->output_plugin_private = data;
    opt->output_type = OUTPUT_PLUGIN_TEXTUAL_OUTPUT;
}

static void neon_oggit_shutdown(LogicalDecodingContext *ctx)
{
    NeonOggitData *data = (NeonOggitData *)ctx->output_plugin_private;

    if (data != NULL) {
        MemoryContextDelete(data->context);
    }
}

static void neon_oggit_begin_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn)
{
    NeonOggitData *data = (NeonOggitData *)ctx->output_plugin_private;

    (void)txn;
    data->xact_wrote_changes = false;
    data->current_merge_id = NULL;
    data->current_merge_origin = NULL;
}

static void neon_oggit_commit_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn, XLogRecPtr commit_lsn)
{
    NeonOggitData *data = (NeonOggitData *)ctx->output_plugin_private;
    MemoryContext old;
    cJSON *root = NULL;

    if (data->skip_empty_xacts && !data->xact_wrote_changes) {
        return;
    }

    old = MemoryContextSwitchTo(data->context);
    root = cJSON_CreateObject();
    cJSON_AddNumberToObject(root, "format_version", 1);
    cJSON_AddStringToObject(root, "event", "commit");
    neon_oggit_add_txn(root, txn);
    neon_oggit_add_merge_origin(root, data);
    neon_oggit_add_lsn(root, "callback_commit_lsn", commit_lsn);
    neon_oggit_emit_json(ctx, root);
    cJSON_Delete(root);
    MemoryContextSwitchTo(old);
    MemoryContextReset(data->context);
}

static void neon_oggit_abort_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn)
{
    (void)ctx;
    (void)txn;
}

static void neon_oggit_prepare_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn)
{
    (void)ctx;
    (void)txn;
}

static bool neon_oggit_filter(LogicalDecodingContext *ctx, RepOriginId origin_id)
{
    NeonOggitData *data = (NeonOggitData *)ctx->output_plugin_private;

    return data->only_local && origin_id != InvalidRepOriginId;
}

static bool neon_oggit_is_merge_event_marker(Relation relation, const char *schema, const char *table)
{
    (void)relation;
    return schema != NULL && table != NULL &&
        strcmp(schema, "oggit") == 0 &&
        strcmp(table, "merge_event_marker") == 0;
}

static char *neon_oggit_extract_merge_marker_field(Relation relation, HeapTuple tuple, const char *field_name)
{
    TupleDesc tupdesc = RelationGetDescr(relation);

    if (tuple == NULL) {
        return NULL;
    }

    for (int natt = 0; natt < tupdesc->natts; natt++) {
        Form_pg_attribute attr = &tupdesc->attrs[natt];
        Datum value = 0;
        bool isnull = false;
        Oid typoutput;
        bool typisvarlena = false;

        if (attr->attisdropped || strcmp(NameStr(attr->attname), field_name) != 0) {
            continue;
        }

        if (tuple->tupTableType == HEAP_TUPLE) {
            value = heap_getattr(tuple, natt + 1, tupdesc, &isnull);
        } else {
            value = uheap_getattr((UHeapTuple)tuple, natt + 1, tupdesc, &isnull);
        }
        if (isnull) {
            return NULL;
        }

        getTypeOutputInfo(attr->atttypid, &typoutput, &typisvarlena);
        if (typisvarlena) {
            value = PointerGetDatum(PG_DETOAST_DATUM(value));
        }
        return OidOutputFunctionCall(typoutput, value);
    }

    return NULL;
}

static const char *neon_oggit_identity_kind(Relation relation)
{
    if (relation->relreplident == REPLICA_IDENTITY_FULL) {
        return "replica_identity_full";
    }
    if (OidIsValid(RelationGetPrimaryKeyIndex(relation))) {
        return "primary_key";
    }
    if (OidIsValid(RelationGetReplicaIndex(relation))) {
        return "unique_key";
    }
    return "unsupported";
}

static bool neon_oggit_is_key_column(Relation relation, int attnum)
{
    Oid key_index = RelationGetPrimaryKeyIndex(relation);
    if (OidIsValid(key_index)) {
        Relation index_relation = RelationIdGetRelation(key_index);
        if (!RelationIsValid(index_relation)) {
            ereport(ERROR, (errmsg("could not open primary key index %u", key_index)));
        }
        for (int index_att = 0;
             index_att < IndexRelationGetNumberOfKeyAttributes(index_relation);
             index_att++) {
            if (index_relation->rd_index->indkey.values[index_att] == attnum) {
                RelationClose(index_relation);
                return true;
            }
        }
        RelationClose(index_relation);
        return false;
    }
    return IsRelationReplidentKey(relation, attnum);
}

static void neon_oggit_add_tuple_column(cJSON *row, cJSON *changed_cols, Relation relation, TupleDesc tupdesc,
    HeapTuple tuple, HeapTuple old_tuple, int natt, bool identity_only)
{
    Form_pg_attribute attr = &tupdesc->attrs[natt];
    Oid typoutput;
    bool typisvarlena = false;
    bool isnull = false;
    bool old_isnull = false;
    Datum origval = 0;
    Datum oldval = 0;
    cJSON *col = NULL;
    char *value = NULL;
    char *old_value = NULL;

    if (attr->attisdropped || attr->attnum < 0) {
        return;
    }
    if (identity_only && !neon_oggit_is_key_column(relation, attr->attnum)) {
        return;
    }

    if (tuple->tupTableType == HEAP_TUPLE) {
        origval = heap_getattr(tuple, natt + 1, tupdesc, &isnull);
    } else {
        origval = uheap_getattr((UHeapTuple)tuple, natt + 1, tupdesc, &isnull);
    }

    col = cJSON_CreateObject();
    cJSON_AddStringToObject(col, "type", format_type_be(attr->atttypid));
    cJSON_AddBoolToObject(col, "is_key", neon_oggit_is_key_column(relation, attr->attnum));
    if (isnull) {
        cJSON_AddNullToObject(col, "value");
    } else {
        getTypeOutputInfo(attr->atttypid, &typoutput, &typisvarlena);
        if (typisvarlena) {
            origval = PointerGetDatum(PG_DETOAST_DATUM(origval));
        }
        value = OidOutputFunctionCall(typoutput, origval);
        cJSON_AddStringToObject(col, "value", value);
    }
    cJSON_AddItemToObject(row, NameStr(attr->attname), col);

    if (changed_cols == NULL || old_tuple == NULL) {
        return;
    }

    if (old_tuple->tupTableType == HEAP_TUPLE) {
        oldval = heap_getattr(old_tuple, natt + 1, tupdesc, &old_isnull);
    } else {
        oldval = uheap_getattr((UHeapTuple)old_tuple, natt + 1, tupdesc, &old_isnull);
    }

    if (isnull != old_isnull) {
        cJSON_AddItemToArray(changed_cols, cJSON_CreateString(NameStr(attr->attname)));
        return;
    }
    if (isnull) {
        return;
    }

    getTypeOutputInfo(attr->atttypid, &typoutput, &typisvarlena);
    if (typisvarlena) {
        oldval = PointerGetDatum(PG_DETOAST_DATUM(oldval));
    }
    old_value = OidOutputFunctionCall(typoutput, oldval);
    if (value == NULL || strcmp(value, old_value) != 0) {
        cJSON_AddItemToArray(changed_cols, cJSON_CreateString(NameStr(attr->attname)));
    }
}

static cJSON *neon_oggit_tuple_to_json(Relation relation, TupleDesc tupdesc, HeapTuple tuple, bool identity_only)
{
    cJSON *row = cJSON_CreateObject();

    if (tuple == NULL) {
        return row;
    }
    if ((tuple->tupTableType == HEAP_TUPLE) && (HEAP_TUPLE_IS_COMPRESSED(tuple->t_data) ||
        (int)HeapTupleHeaderGetNatts(tuple->t_data, tupdesc) > tupdesc->natts)) {
        return row;
    }

    for (int natt = 0; natt < tupdesc->natts; natt++) {
        neon_oggit_add_tuple_column(row, NULL, relation, tupdesc, tuple, NULL, natt, identity_only);
    }
    return row;
}

static cJSON *neon_oggit_changed_cols(TupleDesc tupdesc, ReorderBufferChange *change)
{
    cJSON *changed_cols = cJSON_CreateArray();

    if (change->action != REORDER_BUFFER_CHANGE_UPDATE || !change->data.tp.changed_attrs_valid) {
        return changed_cols;
    }

    for (uint16 idx = 0; idx < change->data.tp.nchanged_attrs; idx++) {
        AttrNumber attnum = change->data.tp.changed_attrs[idx];

        if (attnum <= 0 || attnum > tupdesc->natts) {
            continue;
        }

        Form_pg_attribute attr = &tupdesc->attrs[attnum - 1];
        if (attr->attisdropped) {
            continue;
        }

        cJSON_AddItemToArray(changed_cols, cJSON_CreateString(NameStr(attr->attname)));
    }

    return changed_cols;
}

static void neon_oggit_change(LogicalDecodingContext *ctx, ReorderBufferTXN *txn, Relation relation,
    ReorderBufferChange *change)
{
    NeonOggitData *data = (NeonOggitData *)ctx->output_plugin_private;
    Form_pg_class class_form = RelationGetForm(relation);
    TupleDesc tupdesc = RelationGetDescr(relation);
    MemoryContext old;
    cJSON *root = NULL;
    HeapTuple new_tuple = NULL;
    HeapTuple old_tuple = NULL;
    const char *op = NULL;
    char *schema = get_namespace_name(class_form->relnamespace);
    char *table = NameStr(class_form->relname);
    const char *identity_kind;

    switch (change->action) {
        case REORDER_BUFFER_CHANGE_INSERT:
            op = "INSERT";
            new_tuple = change->data.tp.newtuple == NULL ? NULL : &change->data.tp.newtuple->tuple;
            break;
        case REORDER_BUFFER_CHANGE_UPDATE:
            op = "UPDATE";
            old_tuple = change->data.tp.oldtuple == NULL ? NULL : &change->data.tp.oldtuple->tuple;
            new_tuple = change->data.tp.newtuple == NULL ? NULL : &change->data.tp.newtuple->tuple;
            break;
        case REORDER_BUFFER_CHANGE_DELETE:
            op = "DELETE";
            old_tuple = change->data.tp.oldtuple == NULL ? NULL : &change->data.tp.oldtuple->tuple;
            break;
        case REORDER_BUFFER_CHANGE_UINSERT:
            op = "INSERT";
            new_tuple = change->data.utp.newtuple == NULL ? NULL : (HeapTuple)(&change->data.utp.newtuple->tuple);
            break;
        case REORDER_BUFFER_CHANGE_UUPDATE:
            op = "UPDATE";
            old_tuple = change->data.utp.oldtuple == NULL ? NULL : (HeapTuple)(&change->data.utp.oldtuple->tuple);
            new_tuple = change->data.utp.newtuple == NULL ? NULL : (HeapTuple)(&change->data.utp.newtuple->tuple);
            break;
        case REORDER_BUFFER_CHANGE_UDELETE:
            op = "DELETE";
            old_tuple = change->data.utp.oldtuple == NULL ? NULL : (HeapTuple)(&change->data.utp.oldtuple->tuple);
            break;
        default:
            return;
    }

    if (neon_oggit_is_merge_event_marker(relation, schema, table)) {
        HeapTuple marker_tuple = new_tuple != NULL ? new_tuple : old_tuple;
        char *merge_id = neon_oggit_extract_merge_marker_field(relation, marker_tuple, "merge_id");
        char *merge_origin = neon_oggit_extract_merge_marker_field(relation, marker_tuple, "merge_origin");

        if (merge_id != NULL && merge_id[0] != '\0') {
            data->current_merge_id = MemoryContextStrdup(ctx->context, merge_id);
            data->current_merge_origin = MemoryContextStrdup(ctx->context,
                merge_origin != NULL && merge_origin[0] != '\0' ? merge_origin : "0");
        }
        return;
    }

    if (neon_oggit_is_internal_schema(schema)) {
        return;
    }

    identity_kind = neon_oggit_identity_kind(relation);
    data->xact_wrote_changes = true;
    old = MemoryContextSwitchTo(data->context);

    root = cJSON_CreateObject();
    cJSON_AddNumberToObject(root, "format_version", 1);
    cJSON_AddStringToObject(root, "event", "change");
    neon_oggit_add_txn(root, txn);
    neon_oggit_add_merge_origin(root, data);
    neon_oggit_add_lsn(root, "change_lsn", change->lsn);
    cJSON_AddStringToObject(root, "op", op);
    cJSON_AddStringToObject(root, "schema", schema);
    cJSON_AddStringToObject(root, "table", table);
    cJSON_AddNumberToObject(root, "relid", RelationGetRelid(relation));
    cJSON_AddStringToObject(root, "identity_kind", identity_kind);
    cJSON_AddItemToObject(root, "key",
        strcmp(identity_kind, "primary_key") == 0 || strcmp(identity_kind, "unique_key") == 0
            ? neon_oggit_tuple_to_json(relation, tupdesc, old_tuple != NULL ? old_tuple : new_tuple, true)
            : cJSON_CreateObject());
    cJSON_AddItemToObject(root, "old_row", neon_oggit_tuple_to_json(relation, tupdesc, old_tuple, false));
    cJSON_AddItemToObject(root, "new_row", neon_oggit_tuple_to_json(relation, tupdesc, new_tuple, false));
    cJSON_AddItemToObject(root, "changed_cols", neon_oggit_changed_cols(tupdesc, change));

    neon_oggit_emit_json(ctx, root);
    cJSON_Delete(root);
    MemoryContextSwitchTo(old);
    MemoryContextReset(data->context);
}

static void neon_oggit_truncate(LogicalDecodingContext *ctx, ReorderBufferTXN *txn, int nrelations,
    Relation relations[], ReorderBufferChange *change)
{
    NeonOggitData *data = (NeonOggitData *)ctx->output_plugin_private;
    MemoryContext old;
    cJSON *root = NULL;
    cJSON *rel_array = NULL;
    bool wrote_relation = false;

    old = MemoryContextSwitchTo(data->context);
    root = cJSON_CreateObject();
    rel_array = cJSON_CreateArray();
    cJSON_AddNumberToObject(root, "format_version", 1);
    cJSON_AddStringToObject(root, "event", "truncate");
    neon_oggit_add_txn(root, txn);
    neon_oggit_add_merge_origin(root, data);
    neon_oggit_add_lsn(root, "change_lsn", change->lsn);
    cJSON_AddBoolToObject(root, "restart_seqs", change->data.truncate.restart_seqs);
    cJSON_AddBoolToObject(root, "cascade", change->data.truncate.cascade);

    for (int i = 0; i < nrelations; i++) {
        cJSON *rel = NULL;
        char *schema = get_namespace_name(relations[i]->rd_rel->relnamespace);

        if (neon_oggit_is_internal_schema(schema)) {
            continue;
        }
        rel = cJSON_CreateObject();
        cJSON_AddStringToObject(rel, "schema", schema);
        cJSON_AddStringToObject(rel, "table", NameStr(relations[i]->rd_rel->relname));
        cJSON_AddNumberToObject(rel, "relid", RelationGetRelid(relations[i]));
        cJSON_AddItemToArray(rel_array, rel);
        wrote_relation = true;
    }

    if (wrote_relation) {
        data->xact_wrote_changes = true;
        cJSON_AddItemToObject(root, "relations", rel_array);
        neon_oggit_emit_json(ctx, root);
    } else {
        cJSON_Delete(rel_array);
    }
    cJSON_Delete(root);
    MemoryContextSwitchTo(old);
    MemoryContextReset(data->context);
}

static void neon_oggit_ddl(LogicalDecodingContext *ctx, ReorderBufferTXN *txn, XLogRecPtr message_lsn,
    const char *prefix, Oid relid, DeparsedCommandType cmdtype, Size sz, const char *message)
{
    NeonOggitData *data = (NeonOggitData *)ctx->output_plugin_private;
    MemoryContext old;
    cJSON *root = NULL;
    char *message_text = NULL;
    Oid namespace_id = InvalidOid;
    char *schema = NULL;

    old = MemoryContextSwitchTo(data->context);
    if (message != NULL) {
        message_text = pnstrdup(message, sz);
    }
    if (OidIsValid(relid)) {
        namespace_id = get_rel_namespace(relid);
        if (OidIsValid(namespace_id)) {
            schema = get_namespace_name(namespace_id);
            if (neon_oggit_is_internal_schema(schema)) {
                MemoryContextSwitchTo(old);
                MemoryContextReset(data->context);
                return;
            }
        }
    }
    if (message_text != NULL &&
        (strstr(message_text, "\"schemaname\":\"oggit\"") != NULL ||
         strstr(message_text, "\"schemaname\":\"_oggit\"") != NULL ||
         strstr(message_text, "\"schema\":\"oggit\"") != NULL ||
         strstr(message_text, "\"schema\":\"_oggit\"") != NULL)) {
        MemoryContextSwitchTo(old);
        MemoryContextReset(data->context);
        return;
    }

    data->xact_wrote_changes = true;
    root = cJSON_CreateObject();
    cJSON_AddNumberToObject(root, "format_version", 1);
    cJSON_AddStringToObject(root, "event", "ddl");
    neon_oggit_add_txn(root, txn);
    neon_oggit_add_merge_origin(root, data);
    neon_oggit_add_lsn(root, "message_lsn", message_lsn);
    cJSON_AddStringToObject(root, "prefix", prefix == NULL ? "" : prefix);
    cJSON_AddNumberToObject(root, "relid", relid);
    cJSON_AddStringToObject(root, "cmdtype", neon_oggit_cmdtype_name(cmdtype));
    cJSON_AddNumberToObject(root, "message_size", sz);
    cJSON_AddStringToObject(root, "message", message_text == NULL ? "" : message_text);
    neon_oggit_emit_json(ctx, root);
    cJSON_Delete(root);
    MemoryContextSwitchTo(old);
    MemoryContextReset(data->context);
}

#endif
