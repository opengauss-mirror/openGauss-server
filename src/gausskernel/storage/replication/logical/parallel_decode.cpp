/* ---------------------------------------------------------------------------------------
 *
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
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
 * parallel_decode.cpp
 *        This module decodes WAL records read using xlogreader.h's APIs for the
 *        purpose of parallel logical decoding by passing logical log information to the
 *        parallel_reorderbuffer.
 *        It mainly involves the logic of reading data by reader thread and parsing
 *        logic log by decoder thread.
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/replication/logical/parallel_decode.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/heapam.h"
#include "access/transam.h"
#include "access/xact.h"
#include "access/xlog_internal.h"
#include "access/xlogreader.h"

#include "catalog/gs_matview.h"
#include "catalog/pg_control.h"

#include "libpq/pqformat.h"

#include "storage/standby.h"
#include "utils/lsyscache.h"
#include "utils/builtins.h"

#include "utils/acl.h"
#include "utils/memutils.h"
#include "utils/relfilenodemap.h"
#include "utils/atomic.h"
#include "cjson/cJSON.h"

#include "replication/decode.h"
#include "replication/logical.h"
#include "replication/parallel_decode.h"
#include "replication/parallel_reorderbuffer.h"
#include "replication/reorderbuffer.h"
#include "replication/snapbuild.h"

/* RMGR Handlers */
ParallelReorderBufferTXN *ParallelReorderBufferGetOldestTXN(ParallelReorderBuffer *rb)
{
    ParallelReorderBufferTXN *txn = NULL;

    if (dlist_is_empty(&rb->toplevel_by_lsn))
        return NULL;

    txn = dlist_head_element(ParallelReorderBufferTXN, node, &rb->toplevel_by_lsn);

    Assert(!txn->is_known_as_subxact);
    Assert(!XLByteEQ(txn->first_lsn, InvalidXLogRecPtr));
    return txn;
}

void tuple_to_stringinfo(Relation relation, StringInfo s, TupleDesc tupdesc, HeapTuple tuple, bool isOld,
    bool printOid)
{
    if ((tuple->tupTableType == HEAP_TUPLE) && (HEAP_TUPLE_IS_COMPRESSED(tuple->t_data) ||
        (int)HeapTupleHeaderGetNatts(tuple->t_data, tupdesc) > tupdesc->natts)) {
        return;
    }

    if (printOid) {
        Oid oid;

        /* print oid of tuple, it's not included in the TupleDesc */
        if ((oid = HeapTupleHeaderGetOid(tuple->t_data)) != InvalidOid) {
            appendStringInfo(s, " oid[oid]:%u", oid);
        }
    }

    /* print all columns individually */
    for (int natt = 0; natt < tupdesc->natts; natt++) {
        Form_pg_attribute attr; /* the attribute itself */
        Oid typid;              /* type of current attribute */
        Oid typoutput;          /* output function */
        bool typisvarlena = false;
        Datum origval;      /* possibly toasted Datum */
        bool isnull = true; /* column is null? */

        attr = &tupdesc->attrs[natt];

        /*
         * Don't print dropped columns, we can't be sure everything is
         * available for them.
         * Don't print system columns, oid will already have been printed if
         * present.
         */
        if (attr->attisdropped || attr->attnum < 0 || (isOld && !IsRelationReplidentKey(relation, attr->attnum)))
            continue;

        typid = attr->atttypid;

        /* get Datum from tuple */
        if (tuple->tupTableType == HEAP_TUPLE) {
            origval = heap_getattr(tuple, natt + 1, tupdesc, &isnull);
        } else {
            origval = uheap_getattr((UHeapTuple)tuple, natt + 1, tupdesc, &isnull);
        }

        /* print attribute name */
        appendStringInfoChar(s, ' ');
        appendStringInfoString(s, quote_identifier(NameStr(attr->attname)));

        /* print attribute type */
        appendStringInfoChar(s, '[');
        char* type_name = format_type_be(typid);
        if (strlen(type_name) == strlen("clob") && strncmp(type_name, "clob", strlen("clob")) == 0) {
            errno_t rc = strcpy_s(type_name, sizeof("text"), "text");
            securec_check_c(rc, "\0", "\0");
        }
        appendStringInfoString(s, type_name);
        appendStringInfoChar(s, ']');

        /* query output function */
        getTypeOutputInfo(typid, &typoutput, &typisvarlena);

        /* print separator */
        appendStringInfoChar(s, ':');

        /* print data */
        if (isnull) {
            appendStringInfoString(s, "null");
        } else if (typisvarlena && VARATT_IS_EXTERNAL_ONDISK_B(origval)) {
            appendStringInfoString(s, "unchanged-toast-datum");
        } else if (!typisvarlena) {
            PrintLiteral(s, typid, OidOutputFunctionCall(typoutput, origval));
        } else {
            Datum val = PointerGetDatum(PG_DETOAST_DATUM(origval));
            PrintLiteral(s, typid, OidOutputFunctionCall(typoutput, val));
        }
    }
}

/*
 * Print literal `outputstr' already represented as string of type `typid'
 * into stringbuf `s'.
 *
 * Some builtin types aren't quoted, the rest is quoted. Escaping is done as
 * if u_sess->parser_cxt.standard_conforming_strings were enabled.
 */
void PrintLiteral(StringInfo s, Oid typid, char* outputstr)
{
    const char* valptr = NULL;

    switch (typid) {
        case INT1OID:
        case INT2OID:
        case INT4OID:
        case INT8OID:
        case OIDOID:
        case FLOAT4OID:
        case FLOAT8OID:
        case NUMERICOID:
            /* NB: We don't care about Inf, NaN et al. */
            appendStringInfoString(s, outputstr);
            break;

        case BITOID:
        case VARBITOID:
            appendStringInfo(s, "B'%s'", outputstr);
            break;

        case BOOLOID:
            if (strcmp(outputstr, "t") == 0)
                appendStringInfoString(s, "true");
            else
                appendStringInfoString(s, "false");
            break;

        default:
            appendStringInfoChar(s, '\'');
            for (valptr = outputstr; *valptr; valptr++) {
                char ch = *valptr;

                if (SQL_STR_DOUBLE(ch, false))
                    appendStringInfoChar(s, ch);
                appendStringInfoChar(s, ch);
            }
            appendStringInfoChar(s, '\'');
            break;
    }
}

/* parallel decoding filter results by white list */
static bool FilterWhiteList(const char *schema, const char *table, int slotId, MemoryContext old, MemoryContext ctx)
{
    if (g_Logicaldispatcher[slotId].pOptions.tableWhiteList != NIL &&
        !CheckWhiteList(g_Logicaldispatcher[slotId].pOptions.tableWhiteList, schema, table)) {
        (void)MemoryContextSwitchTo(old);
        MemoryContextReset(ctx);
        return true;
    }
    return false;
}

/* parallel logical decoding callback with decode style: text */
void parallel_decode_change_to_text(Relation relation, ParallelReorderBufferChange* change, logicalLog *logChange,
    ParallelLogicalDecodingContext* ctx, int slotId)
{
    Form_pg_class class_form;
    TupleDesc tupdesc;
    logChange->type = LOGICAL_LOG_DML;
    logChange->lsn = change->lsn;
    logChange->xid = change->xid;
    MemoryContext old;
    ParallelDecodingData *data = (ParallelDecodingData *)ctx->output_plugin_private;
    old = MemoryContextSwitchTo(data->context);

    class_form = RelationGetForm(relation);
    tupdesc = RelationGetDescr(relation);

    char *schema = get_namespace_name(class_form->relnamespace);
    char *table = NameStr(class_form->relname);
    if (FilterWhiteList(schema, table, slotId, old, data->context)) {
        logChange->type = LOGICAL_LOG_EMPTY;
        return;
    }

    int curPos = logChange->out->len;
    uint32 changeLen = 0;
    if (g_Logicaldispatcher[slotId].pOptions.sending_batch > 0) {
        pq_sendint32(logChange->out, changeLen);
        pq_sendint64(logChange->out, change->lsn);
    }

    appendStringInfo(logChange->out, "table %s %s", schema, table);

    switch (change->action) {
        case PARALLEL_REORDER_BUFFER_CHANGE_INSERT:
        case PARALLEL_REORDER_BUFFER_CHANGE_UINSERT:
            appendStringInfoString(logChange->out, " INSERT:");
            if (change->data.tp.newtuple == NULL)
                appendStringInfoString(logChange->out, " (no-tuple-data)");
            else
                tuple_to_stringinfo(relation, logChange->out, tupdesc, &change->data.tp.newtuple->tuple, false);
            break;

        case PARALLEL_REORDER_BUFFER_CHANGE_UPDATE:
        case PARALLEL_REORDER_BUFFER_CHANGE_UUPDATE:
            appendStringInfoString(logChange->out, " UPDATE:");
            if (change->data.tp.oldtuple != NULL) {
                appendStringInfoString(logChange->out, " old-key:");
                tuple_to_stringinfo(relation, logChange->out, tupdesc, &change->data.tp.oldtuple->tuple, true);
                appendStringInfoString(logChange->out, " new-tuple:");
            }

            if (change->data.tp.newtuple == NULL)
                appendStringInfoString(logChange->out, " (no-tuple-data)");
            else
                tuple_to_stringinfo(relation, logChange->out, tupdesc, &change->data.tp.newtuple->tuple, false);
            break;

        case PARALLEL_REORDER_BUFFER_CHANGE_DELETE:
        case PARALLEL_REORDER_BUFFER_CHANGE_UDELETE:
            appendStringInfoString(logChange->out, " DELETE:");

            /* if there was no PK, we only know that a delete happened */
            if (change->data.tp.oldtuple == NULL)
                appendStringInfoString(logChange->out, " (no-tuple-data)");
            /* In DELETE, only the replica identity is present; display that */
            else
                tuple_to_stringinfo(relation, logChange->out, tupdesc, &change->data.tp.oldtuple->tuple, true);
            break;

        default:
            break;
    }

    if (g_Logicaldispatcher[slotId].pOptions.sending_batch > 0) {
        changeLen = htonl((uint32)(logChange->out->len - curPos) - (uint32)sizeof(uint32));
        errno_t rc = memcpy_s(logChange->out->data + curPos, sizeof(uint32), &changeLen, sizeof(uint32));
        securec_check(rc, "", "");
    }
    MemoryContextSwitchTo(old);
    MemoryContextReset(data->context);
}

static void TupleToJsoninfo(Relation relation, cJSON* cols_name, cJSON* cols_type, cJSON* cols_val, TupleDesc tupdesc,
    HeapTuple tuple, bool isOld)
{
    if ((tuple->tupTableType == HEAP_TUPLE) && (HEAP_TUPLE_IS_COMPRESSED(tuple->t_data) ||
        (int)HeapTupleHeaderGetNatts(tuple->t_data, tupdesc) > tupdesc->natts)) {
        return;
    }

    /* print all columns individually */
    for (int natt = 0; natt < tupdesc->natts; natt++) {
        Form_pg_attribute attr = &tupdesc->attrs[natt]; /* the attribute itself */
        if (attr->attisdropped || attr->attnum < 0 || (isOld && !IsRelationReplidentKey(relation, attr->attnum))) {
            continue;
        }

        Oid typid = attr->atttypid; /* type of current attribute */
        Datum origval = 0;      /* possibly toasted Datum */

        /* get Datum from tuple */
        bool isnull = false; /* column is null? */
        if (tuple->tupTableType == HEAP_TUPLE) {
            origval = heap_getattr(tuple, natt + 1, tupdesc, &isnull);
        } else {
            origval = uheap_getattr((UHeapTuple)tuple, natt + 1, tupdesc, &isnull);
        }

        /* print attribute name */

        cJSON* colName = cJSON_CreateString(quote_identifier(NameStr(attr->attname)));
        cJSON_AddItemToArray(cols_name, colName);

        /* print attribute type */
        if (cols_type != NULL) {
            char* typeName = format_type_be(typid);
            if (strlen(typeName) == strlen("clob") && strncmp(typeName, "clob", strlen("clob")) == 0) {
                errno_t rc = strcpy_s(typeName, sizeof("clob"), "text");
                securec_check_c(rc, "\0", "\0");
            }
            cJSON* colType = cJSON_CreateString(typeName);
            cJSON_AddItemToArray(cols_type, colType);
        }

        /* query output function */
        Oid typoutput = 0;
        bool typisvarlena = false;
        getTypeOutputInfo(typid, &typoutput, &typisvarlena);

        /* print separator */
        StringInfo val_str = makeStringInfo();
        /* print data */
        if (isnull) {
            appendStringInfoString(val_str, "null");
        } else if (!typisvarlena) {
            PrintLiteral(val_str, typid, OidOutputFunctionCall(typoutput, origval));
        } else {
            Datum val = PointerGetDatum(PG_DETOAST_DATUM(origval));
            PrintLiteral(val_str, typid, OidOutputFunctionCall(typoutput, val));
        }
        cJSON* col_val = cJSON_CreateString(val_str->data);
        cJSON_AddItemToArray(cols_val, col_val);
    }
}

/* parallel logical decoding callback with decode style: json */
void parallel_decode_change_to_json(Relation relation, ParallelReorderBufferChange* change, logicalLog *logChange,
    ParallelLogicalDecodingContext* ctx, int slotId)
{
    Form_pg_class class_form = NULL;
    TupleDesc tupdesc = NULL;
    MemoryContext old;
    char* res = NULL;

    logChange->type = LOGICAL_LOG_DML;
    logChange->lsn = change->lsn;
    logChange->xid = change->xid;

    ParallelDecodingData *data = (ParallelDecodingData*)ctx->output_plugin_private;

    data->pOptions.xact_wrote_changes = true;

    class_form = RelationGetForm(relation);
    tupdesc = RelationGetDescr(relation);

    /* Avoid leaking memory by using and resetting our own context */
    old = MemoryContextSwitchTo(data->context);

    char *schema = get_namespace_name(class_form->relnamespace);
    char *table = NameStr(class_form->relname);
    if (FilterWhiteList(schema, table, slotId, old, data->context)) {
        logChange->type = LOGICAL_LOG_EMPTY;
        return;
    }

    uint32 changeLen = 0;
    int curPos = logChange->out->len;
    if (g_Logicaldispatcher[slotId].pOptions.sending_batch > 0) {
        pq_sendint32(logChange->out, changeLen);
        pq_sendint64(logChange->out, change->lsn);
    }

    cJSON* root = cJSON_CreateObject();
    cJSON* tableName = NULL;
    cJSON* opType = NULL;
    cJSON* columnsVal = NULL;
    cJSON* columnsName = NULL;
    cJSON* columnsType = NULL;
    cJSON* oldKeysName = NULL;
    cJSON* oldKeysVal = NULL;
    cJSON* oldKeysType = NULL;
    tableName = cJSON_CreateString(quote_qualified_identifier(schema, table));
    cJSON_AddItemToObject(root, "table_name", tableName);

    columnsVal = cJSON_CreateArray();
    columnsName = cJSON_CreateArray();
    columnsType = cJSON_CreateArray();
    oldKeysName = cJSON_CreateArray();
    oldKeysVal = cJSON_CreateArray();
    oldKeysType = cJSON_CreateArray();

    switch (change->action) {
        case PARALLEL_REORDER_BUFFER_CHANGE_INSERT:
        case PARALLEL_REORDER_BUFFER_CHANGE_UINSERT:
            opType = cJSON_CreateString("INSERT");
            if (change->data.tp.newtuple != NULL) {
                TupleToJsoninfo(relation, columnsName, columnsType, columnsVal, tupdesc,
                    &change->data.tp.newtuple->tuple, false);
            }
            break;
        case PARALLEL_REORDER_BUFFER_CHANGE_UPDATE:
        case PARALLEL_REORDER_BUFFER_CHANGE_UUPDATE:
            opType = cJSON_CreateString("UPDATE");
            if (change->data.tp.oldtuple != NULL) {
                TupleToJsoninfo(relation, oldKeysName, oldKeysType, oldKeysVal, tupdesc,
                    &change->data.tp.oldtuple->tuple, true);
            }

            if (change->data.tp.newtuple != NULL) {
                TupleToJsoninfo(relation, columnsName, columnsType, columnsVal, tupdesc,
                    &change->data.tp.newtuple->tuple, false);
            }
            break;
        case PARALLEL_REORDER_BUFFER_CHANGE_DELETE:
        case PARALLEL_REORDER_BUFFER_CHANGE_UDELETE:
            opType = cJSON_CreateString("DELETE");
            if (change->data.tp.oldtuple != NULL) {
                TupleToJsoninfo(relation, oldKeysName, oldKeysType, oldKeysVal, tupdesc,
                    &change->data.tp.oldtuple->tuple, true);
            }
            /* if there was no PK, we only know that a delete happened */
            break;

        default:
            Assert(false);
    }

    cJSON_AddItemToObject(root, "op_type", opType);
    cJSON_AddItemToObject(root, "columns_name", columnsName);
    cJSON_AddItemToObject(root, "columns_type", columnsType);
    cJSON_AddItemToObject(root, "columns_val", columnsVal);
    cJSON_AddItemToObject(root, "old_keys_name", oldKeysName);
    cJSON_AddItemToObject(root, "old_keys_type", oldKeysType);
    cJSON_AddItemToObject(root, "old_keys_val", oldKeysVal);

    res = cJSON_PrintUnformatted(root);
    if (res != NULL) {
        appendStringInfoString(logChange->out, res);
    }

    if (g_Logicaldispatcher[slotId].pOptions.sending_batch > 0) {
        changeLen = htonl((uint32)(logChange->out->len - curPos) - (uint32)sizeof(uint32));
        errno_t rc = memcpy_s(logChange->out->data + curPos, sizeof(uint32), &changeLen, sizeof(uint32));
        securec_check(rc, "", "");
    }

    MemoryContextSwitchTo(old);
    MemoryContextReset(data->context);
}

/* append schema and table info */
static void AppendRelation(StringInfo s, TupleDesc tupdesc, const char * schema, const char * table)
{
    pq_sendint16(s, (uint16)strlen(schema));
    appendStringInfoString(s, schema);
    pq_sendint16(s, (uint16)strlen(table));
    appendStringInfoString(s, table);
}

/* handle circumstances that should not be decoded */
static inline bool AppendInvalidations(StringInfo s, TupleDesc tupdesc, HeapTuple tuple)
{
    if ((tuple->tupTableType == HEAP_TUPLE) && (HEAP_TUPLE_IS_COMPRESSED(tuple->t_data) ||
        (int)HeapTupleHeaderGetNatts(tuple->t_data, tupdesc) > tupdesc->natts)) {
        pq_sendint16(s, 0);
        return true;
    }
    return false;
}

/* decode a tuple into binary style */
static void AppendTuple(Relation relation, StringInfo s, TupleDesc tupdesc, HeapTuple tuple, bool isOld)
{
    if (AppendInvalidations(s, tupdesc, tuple)) {
        return;
    }
    int curPos = s->len;
    uint16 attrNum = 0;
    pq_sendint16(s, (uint16)(tupdesc->natts));
    for (int natt = 0; natt < tupdesc->natts; natt++) {
        Form_pg_attribute attr = &tupdesc->attrs[natt];
        if (attr->attisdropped || attr->attnum < 0 || (isOld && !IsRelationReplidentKey(relation, attr->attnum))) {
            continue;
        }

        Oid typid = attr->atttypid;
        bool isnull = false;
        Datum origval = 0;
        if (tuple->tupTableType == HEAP_TUPLE) {
            origval = heap_getattr(tuple, natt + 1, tupdesc, &isnull);
        } else {
            origval = uheap_getattr((UHeapTuple)tuple, natt + 1, tupdesc, &isnull);
        }
        attrNum++;
        const char *columnName = quote_identifier(NameStr(attr->attname));
        pq_sendint16(s, (uint16)strlen(columnName));
        appendStringInfoString(s, columnName);
        pq_sendint32(s, typid);
        Oid typoutput = 0;
        bool typisvarlena = false;
        getTypeOutputInfo(typid, &typoutput, &typisvarlena);
        const uint32 nullTag = 0xFFFFFFFF;
        if (isnull) {
            pq_sendint32(s, nullTag);
        } else if (!typisvarlena) {
            char *data = OidOutputFunctionCall(typoutput, origval);
            pq_sendint32(s, strlen(data));
            appendStringInfoString(s, data);
        } else {
            Datum val = PointerGetDatum(PG_DETOAST_DATUM(origval));
            char *data = OidOutputFunctionCall(typoutput, val);
            pq_sendint32(s, strlen(data));
            appendStringInfoString(s, data);
        }
    }
    attrNum = ntohs(attrNum);
    errno_t rc = memcpy_s(s->data + curPos, sizeof(uint16), &attrNum, sizeof(uint16));
    securec_check(rc, "", "");
}

/* parallel logical decoding callback with decode style: binary */
void parallel_decode_change_to_bin(Relation relation, ParallelReorderBufferChange* change, logicalLog *logChange,
    ParallelLogicalDecodingContext* ctx, int slotId)
{
    logChange->type = LOGICAL_LOG_DML;
    logChange->lsn = change->lsn;
    logChange->xid = change->xid;
    ParallelDecodingData *data = (ParallelDecodingData *)ctx->output_plugin_private;
    MemoryContext old = MemoryContextSwitchTo(data->context);

    Form_pg_class class_form = RelationGetForm(relation);
    TupleDesc tupdesc = RelationGetDescr(relation);

    char *schema = get_namespace_name(class_form->relnamespace);
    char *table = NameStr(class_form->relname);
    if (FilterWhiteList(schema, table, slotId, old, data->context)) {
        logChange->type = LOGICAL_LOG_EMPTY;
        return;
    }

    int curPos = logChange->out->len;
    uint32 changeLen = 0;
    pq_sendint32(logChange->out, changeLen);
    pq_sendint64(logChange->out, change->lsn);
    switch (change->action) {
        case PARALLEL_REORDER_BUFFER_CHANGE_INSERT:
        case PARALLEL_REORDER_BUFFER_CHANGE_UINSERT:
            appendStringInfoChar(logChange->out, 'I');
            AppendRelation(logChange->out, tupdesc, schema, table);
            if (change->data.tp.newtuple != NULL) {
                appendStringInfoChar(logChange->out, 'N');
                AppendTuple(relation, logChange->out, tupdesc, &change->data.tp.newtuple->tuple, false);
            }
            break;

        case PARALLEL_REORDER_BUFFER_CHANGE_UPDATE:
        case PARALLEL_REORDER_BUFFER_CHANGE_UUPDATE:
            appendStringInfoChar(logChange->out, 'U');
            AppendRelation(logChange->out, tupdesc, schema, table);

            if (change->data.tp.newtuple != NULL) {
                appendStringInfoChar(logChange->out, 'N');
                AppendTuple(relation, logChange->out, tupdesc, &change->data.tp.newtuple->tuple, false);
            }
            if (change->data.tp.oldtuple != NULL) {
                appendStringInfoChar(logChange->out, 'O');
                AppendTuple(relation, logChange->out, tupdesc, &change->data.tp.oldtuple->tuple, true);
            }
            break;

        case PARALLEL_REORDER_BUFFER_CHANGE_DELETE:
        case PARALLEL_REORDER_BUFFER_CHANGE_UDELETE:
            appendStringInfoChar(logChange->out, 'D');
            AppendRelation(logChange->out, tupdesc, schema, table);
            /* if there was no PK, we only know that a delete happened */
            if (change->data.tp.oldtuple != NULL) {
                appendStringInfoChar(logChange->out, 'O');
                AppendTuple(relation, logChange->out, tupdesc, &change->data.tp.oldtuple->tuple, true);
            }
            break;

        default:
            break;
    }
    changeLen = htonl((uint32)(logChange->out->len - curPos) - (uint32)sizeof(uint32));
    errno_t rc = memcpy_s(logChange->out->data + curPos, sizeof(uint32), &changeLen, sizeof(uint32));
    securec_check(rc, "", "");
    MemoryContextSwitchTo(old);
    MemoryContextReset(data->context);
}

/*
 * Use caching to reduce frequent memory requests and releases.
 * Use worker->freegetlogicalloghead to store logchanges that should be free.
 * logicalLog is requested in the reader thread and free in the decoder thread.
 */
logicalLog* GetLogicalLog(ParallelDecodeWorker *worker)
{
    logicalLog *logChange = NULL;
    MemoryContext oldCtx;
    int slotId = worker->slotId;
    do {
        if (worker->freeGetLogicalLogHead!= NULL) {
            logChange = worker->freeGetLogicalLogHead;
            worker->freeGetLogicalLogHead = worker->freeGetLogicalLogHead->freeNext;
        } else {
            logicalLog *head = (logicalLog *)pg_atomic_exchange_uintptr(
                (uintptr_t *)&g_Logicaldispatcher[slotId].freeLogicalLogHead, (uintptr_t)NULL);
            if (head != NULL) {
                logChange = head;
                worker->freeGetLogicalLogHead = head->freeNext;
            } else {
                (void)pg_atomic_add_fetch_u32(&g_Logicaldispatcher[slotId].curLogNum, 1);
                oldCtx = MemoryContextSwitchTo(g_instance.comm_cxt.pdecode_cxt[slotId].logicalLogCtx);
                logChange = (logicalLog *)palloc0(sizeof(logicalLog));
                logChange->out = NULL;
                logChange->freeNext = NULL;
                MemoryContextSwitchTo(oldCtx);
            }
        }
    } while (logChange == NULL);

    logChange->type = LOGICAL_LOG_EMPTY;
    logChange->toast_hash = NULL;
    logChange->nsubxacts = 0;
    logChange->subXids = NULL;
    if (logChange->out != NULL && logChange->out->data != NULL) {
        resetStringInfo(logChange->out);
    } else if (logChange->out != NULL && logChange->out->data == NULL) {
        oldCtx = MemoryContextSwitchTo(g_instance.comm_cxt.pdecode_cxt[slotId].logicalLogCtx);
        initStringInfo(logChange->out);
        MemoryContextSwitchTo(oldCtx);
    } else {
        oldCtx = MemoryContextSwitchTo(g_instance.comm_cxt.pdecode_cxt[slotId].logicalLogCtx);
        logChange->out = makeStringInfo();
        MemoryContextSwitchTo(oldCtx);
    }
    return logChange;
}

static bool IsLogicalLogStored(logicalLog *logChange)
{
    bool res = false;
    switch (logChange->type) {
        case LOGICAL_LOG_DML:
        case LOGICAL_LOG_NEW_CID:
        case LOGICAL_LOG_MISSING_CHUNK:
            res = true;
            break;
        default:
            break;
    }
    return res;
}

/*
 * Set logical log to cache.
 */
void FreeLogicalLog(ParallelReorderBuffer *rb, logicalLog *logChange, int slotId, bool nocache)
{
    if (logChange->toast_hash != NULL) {
        ParallelReorderBufferToastReset(&logChange->toast_hash, slotId);
    }

    if (logChange->nsubxacts != 0 && logChange->subXids != NULL) {
        pfree_ext(logChange->subXids);
        logChange->nsubxacts = 0;
    }
    /* a half of GB */
    const uint32 halfGB = 1024L * 1024L * 1024L / 2;
    uint32 curLogNum = g_Logicaldispatcher[slotId].curLogNum;

    /* If the palloced memory exceeds the threshold, we just free it instead of cache it. */
    if (curLogNum >= max_decode_cache_num || nocache ||
        (g_Logicaldispatcher[slotId].pOptions.max_reorderbuffer_in_memory > 0 &&
        rb->size >= (Size)g_Logicaldispatcher[slotId].pOptions.max_reorderbuffer_in_memory * halfGB)) {
        if (IsLogicalLogStored(logChange)) {
            ParallelReorderBufferUpdateMemory(rb, logChange, slotId, false);
        }
        if (logChange->out != NULL) {
            DestroyStringInfo(logChange->out);
        }
        pfree(logChange);
        (void)pg_atomic_sub_fetch_u32(&g_Logicaldispatcher[slotId].curLogNum, 1);
        return;
    }
    logicalLog *oldHead = NULL;
    do {
        oldHead =
            (logicalLog *)pg_atomic_read_uintptr((uintptr_t *)&g_Logicaldispatcher[slotId].freeLogicalLogHead);
        if (logChange->out != NULL) {
            if (IsLogicalLogStored(logChange)) {
                ParallelReorderBufferUpdateMemory(rb, logChange, slotId, false);
            }
            int curSize = logChange->out->maxlen;
            const int initSize = 1024;
            if (curSize > initSize) {
                FreeStringInfo(logChange->out);
                logChange->out->maxlen = 0;
            }
        }
        logChange->type = LOGICAL_LOG_EMPTY;
        logChange->freeNext = oldHead;
    } while (!pg_atomic_compare_exchange_uintptr((uintptr_t *)&(g_Logicaldispatcher[slotId].freeLogicalLogHead),
        (uintptr_t *)&oldHead, (uintptr_t)logChange));
}

Snapshot GetLocalSnapshot(MemoryContext ctx)
{
    Size ssize = sizeof(SnapshotData);
    Snapshot snapshot = (Snapshot)MemoryContextAllocZero(ctx, ssize);
    snapshot->satisfies = SNAPSHOT_DECODE_MVCC;

    snapshot->xmin = FirstNormalTransactionId;
    snapshot->xmax = MaxTransactionId;

    snapshot->suboverflowed = false;
    snapshot->takenDuringRecovery = false;
    snapshot->copied = false;
    snapshot->curcid = FirstCommandId;
    snapshot->active_count = 0;
    snapshot->regd_count = 0;

    snapshot->xip = NULL;
    snapshot->xcnt = 0;

    snapshot->subxcnt = 0;
    snapshot->subxip = NULL;
    return snapshot;
}

/*
 * Decode insert,update,delete record to logical log.
 * Put logical log to queue and waiting for walsender thread send it.
 */
logicalLog* getIUDLogicalLog(ParallelReorderBufferChange* change, ParallelLogicalDecodingContext* ctx,
    ParallelDecodeWorker *worker)
{
    Oid partitionReltoastrelid = InvalidOid;
    Relation relation = NULL;
    int slotId = worker->slotId;

    if (u_sess->utils_cxt.HistoricSnapshot == NULL) {
        u_sess->utils_cxt.HistoricSnapshot = GetLocalSnapshot(ctx->context);
    }
    u_sess->utils_cxt.HistoricSnapshot->snapshotcsn = change->data.tp.snapshotcsn;
    bool isSegment = IsSegmentFileNode(change->data.tp.relnode);
    Oid reloid = HeapGetRelid(change->data.tp.relnode.spcNode, change->data.tp.relnode.relNode,
        partitionReltoastrelid, NULL, isSegment);
    logicalLog* logChange = NULL;

    if (change->missingChunk) {
        logChange = GetLogicalLog(worker);
        logChange->type = LOGICAL_LOG_MISSING_CHUNK;
        logChange->xid = change->xid;
        logChange->lsn = change->lsn;
        return logChange;
    }
    /*
     * Catalog tuple without data, emitted while catalog was
     * in the process of being rewritten.
     */
    if (reloid == InvalidOid) {
        /*
         * description:
         * When we try to decode a table who is already dropped.
         * Maybe we could not find it relnode.In this time, we will undecode this log.
         * However, we still set an empty logical logqueue, because we need to ensure
         * that the logical logs obtained by the walsender are in order.
         */
        ereport(DEBUG1, (errmsg("could not lookup relation %s", relpathperm(change->data.tp.relnode, MAIN_FORKNUM))));
        logChange = GetLogicalLog(worker);
        return logChange;
    }
    /*
     * Do not decode private tables, otherwise there will be security problems.
     */
    if (is_role_independent(FindRoleid(reloid))) {
        logChange = GetLogicalLog(worker);
        return logChange;
    }

    relation = RelationIdGetRelation(reloid);
    if (relation == NULL) {
        ereport(DEBUG1, (errmsg("could open relation descriptor %s",
            relpathperm(change->data.tp.relnode, MAIN_FORKNUM))));
        logChange = GetLogicalLog(worker);
        return logChange;
    }

    if (CSTORE_NAMESPACE == get_rel_namespace(RelationGetRelid(relation))) {
        RelationClose(relation);
        logChange = GetLogicalLog(worker);
        return logChange;
    }

    if (RelationIsLogicallyLogged(relation)) {
        /*
         * For now ignore sequence changes entirely. Most of
         * the time they don't log changes using records we
         * understand, so it doesn't make sense to handle the
         * few cases we do.
         */
         
        if (relation->rd_rel->relkind == RELKIND_SEQUENCE) {
        } else if (!IsToastRelation(relation)) { /* user-triggered change */
            logChange = GetLogicalLog(worker);
            g_Logicaldispatcher[slotId].pOptions.decode_change(relation, change, logChange, ctx, slotId);
            RelationClose(relation);
            return logChange;
        }
    }

    RelationClose(relation);
    logChange = GetLogicalLog(worker);
    return logChange;
}

/*
 * Decode commit or abort change.
 */
static logicalLog* ParallelDecodeCommitOrAbort(ParallelReorderBufferChange* change, ParallelDecodeWorker *worker,
    LogicalLogType logType)
{
    logicalLog *logChange = GetLogicalLog(worker);
    logChange->lsn = change->lsn;
    logChange->xid = change->xid;
    logChange->type = logType;
    logChange->csn = change->csn;
    logChange->finalLsn = change->finalLsn;
    logChange->endLsn = change->endLsn;
    logChange->nsubxacts = change->nsubxacts;
    logChange->commitTime = change->commitTime;
    logChange->origin_id = change->origin_id;
    int slotId = worker->slotId;
    Size subXidSize = sizeof(TransactionId) * change->nsubxacts;
    if (subXidSize > 0 && change->subXids != NULL) {
        MemoryContext oldCtx = MemoryContextSwitchTo(g_instance.comm_cxt.pdecode_cxt[slotId].logicalLogCtx);
        logChange->subXids = (TransactionId *)palloc0(subXidSize);
        MemoryContextSwitchTo(oldCtx);
        errno_t rc = memcpy_s(logChange->subXids, subXidSize, change->subXids, subXidSize);
        securec_check(rc, "", "");
        pfree(change->subXids);
        change->subXids = NULL;
    }
    return logChange;
}

/*
 * Decode subtransactions assigment.
 */
logicalLog *ParallelDecodeAssignment(ParallelReorderBufferChange* change, ParallelDecodeWorker *worker)
{
    logicalLog *logChange = GetLogicalLog(worker);
    logChange->xid = change->xid;
    logChange->lsn = change->lsn;
    logChange->nsubxacts = change->nsubxacts;
    logChange->type = LOGICAL_LOG_ASSIGNMENT;
    int slotId = worker->slotId;
    if (change->nsubxacts > 0 && change->subXids != NULL) {
        Size subXidSize = sizeof(TransactionId) * INT2SIZET(change->nsubxacts);
        logChange->subXids = (TransactionId *)MemoryContextAllocZero(
            g_instance.comm_cxt.pdecode_cxt[slotId].logicalLogCtx, subXidSize);
        errno_t rc = memcpy_s(logChange->subXids, subXidSize, change->subXids, subXidSize);
        securec_check(rc, "", "");
        pfree(change->subXids);
        change->subXids = NULL;
    }
    return logChange;
}

/*
 * Decode change tuple, put it into logical queue.
 */
logicalLog* ParallelDecodeChange(ParallelReorderBufferChange* change, ParallelLogicalDecodingContext* ctx,
    ParallelDecodeWorker *worker)
{
    u_sess->attr.attr_common.extra_float_digits = LOGICAL_DECODE_EXTRA_FLOAT_DIGITS;
    logicalLog* logChange = NULL;
    switch (change->action) {
        case PARALLEL_REORDER_BUFFER_INVALIDATIONS_MESSAGE: {
            for (int i = 0; i < change->ninvalidations; i++) {
                LocalExecuteThreadAndSessionInvalidationMessage(&change->invalidations[i]);
            }
            if (change->ninvalidations > 0) {
                pfree(change->invalidations);
                change->invalidations = NULL;
            }
            break;
        }

        case PARALLEL_REORDER_BUFFER_CHANGE_COMMIT: {
            logChange = ParallelDecodeCommitOrAbort(change, worker, LOGICAL_LOG_COMMIT);
            break;
        }

        case PARALLEL_REORDER_BUFFER_CHANGE_ABORT: {
            logChange = ParallelDecodeCommitOrAbort(change, worker, LOGICAL_LOG_ABORT);
            break;
        }

        case PARALLEL_REORDER_BUFFER_CHANGE_RUNNING_XACT: {
            logChange = GetLogicalLog(worker);
            logChange->lsn = change->lsn;
            logChange->xid = change->xid;
            logChange->oldestXmin = change->oldestXmin;
            logChange->type = LOGICAL_LOG_RUNNING_XACTS;
            logChange->csn = change->csn;
            break;
        }

        case PARALLEL_REORDER_BUFFER_CHANGE_INSERT:
        case PARALLEL_REORDER_BUFFER_CHANGE_UPDATE:
        case PARALLEL_REORDER_BUFFER_CHANGE_DELETE:
        case PARALLEL_REORDER_BUFFER_CHANGE_UINSERT:
        case PARALLEL_REORDER_BUFFER_CHANGE_UUPDATE:
        case PARALLEL_REORDER_BUFFER_CHANGE_UDELETE: {
            logChange = getIUDLogicalLog(change, ctx, worker);
            break;
        }

        case PARALLEL_REORDER_BUFFER_CHANGE_CONFIRM_FLUSH: {
            logChange = GetLogicalLog(worker);
            logChange->lsn = change->lsn;
            logChange->type = LOGICAL_LOG_CONFIRM_FLUSH;
            break;
        }
        case PARALLEL_REORDER_BUFFER_NEW_CID: {
            logChange = GetLogicalLog(worker);
            logChange->xid = change->xid;
            logChange->lsn = change->lsn;
            logChange->type = LOGICAL_LOG_NEW_CID;
            break;
        }
        case PARALLEL_REORDER_BUFFER_ASSIGNMENT: {
            logChange = ParallelDecodeAssignment(change, worker);
            break;
        }
    }
    return logChange;
}

ParallelStatusData *GetParallelDecodeStatus(uint32 *num)
{
    const uint32 slotNum = 20;
    const uint32 upperPart = 32;
    ParallelStatusData *result = (ParallelStatusData *)palloc0(slotNum * sizeof(ParallelStatusData));
    uint32 id = 0;
    for (uint32 i = 0; i < slotNum; i++) {
        if (!g_Logicaldispatcher[i].active) {
            continue;
        }

        errno_t rc = memcpy_s(result[id].slotName, NAMEDATALEN, g_Logicaldispatcher[i].slotName, NAMEDATALEN);
        securec_check(rc, "", "");
        result[id].parallelDecodeNum = g_Logicaldispatcher[i].totalWorkerCount;
        StringInfoData readQueueLen;
        StringInfoData decodeQueueLen;
        initStringInfo(&readQueueLen);
        initStringInfo(&decodeQueueLen);
        bool escape = false;
        knl_g_parallel_decode_context *pDecodeCxt = &g_instance.comm_cxt.pdecode_cxt[i];

        if (!g_Logicaldispatcher[i].active || g_Logicaldispatcher[i].abnormal) {
            FreeStringInfo(&readQueueLen);
            FreeStringInfo(&decodeQueueLen);
            continue;
        }
        for (int j = 0; j < result[id].parallelDecodeNum; j++) {
            SpinLockAcquire(&pDecodeCxt->rwlock);
            ParallelDecodeWorker *worker = g_Logicaldispatcher[i].decodeWorkers[j];
            if (!g_Logicaldispatcher[i].active || g_Logicaldispatcher[i].abnormal || worker == NULL) {
                escape = true;
                SpinLockRelease(&pDecodeCxt->rwlock);
                break;
            }
            LogicalQueue *readQueue = worker->changeQueue;
            if (!g_Logicaldispatcher[i].active || g_Logicaldispatcher[i].abnormal || readQueue == NULL) {
                escape = true;
                SpinLockRelease(&pDecodeCxt->rwlock);
                break;
            }
            uint32 rmask = readQueue->mask;
            uint32 readHead = pg_atomic_read_u32(&readQueue->writeHead);
            uint32 readTail = pg_atomic_read_u32(&readQueue->readTail);
            uint32 readCnt = COUNT(readHead, readTail, rmask);
            SpinLockRelease(&pDecodeCxt->rwlock);
            appendStringInfo(&readQueueLen, "queue%d: %u", j, readCnt);

            SpinLockAcquire(&pDecodeCxt->rwlock);
            LogicalQueue *decodeQueue = worker->LogicalLogQueue;
            if (!g_Logicaldispatcher[i].active || g_Logicaldispatcher[i].abnormal || decodeQueue == NULL) {
                escape = true;
                SpinLockRelease(&pDecodeCxt->rwlock);
                break;
            }
            uint32 dmask = decodeQueue->mask;
            uint32 decodeHead = pg_atomic_read_u32(&decodeQueue->writeHead);
            uint32 decodeTail = pg_atomic_read_u32(&decodeQueue->readTail);
            uint32 decodeCnt = COUNT(decodeHead, decodeTail, dmask);
            SpinLockRelease(&pDecodeCxt->rwlock);
            appendStringInfo(&decodeQueueLen, "queue%d: %u", j, decodeCnt);

            if (j < result[id].parallelDecodeNum - 1) {
                appendStringInfoString(&readQueueLen, ", ");
                appendStringInfoString(&decodeQueueLen, ", ");
            }
        }
        if (escape) {
            FreeStringInfo(&readQueueLen);
            FreeStringInfo(&decodeQueueLen);
            continue;
        }
        rc = memcpy_s(result[id].readQueueLen, QUEUE_RESULT_LEN, readQueueLen.data, readQueueLen.len);
        securec_check(rc, "", "");
        rc = memcpy_s(result[id].decodeQueueLen, QUEUE_RESULT_LEN, decodeQueueLen.data, decodeQueueLen.len);
        securec_check(rc, "", "");
        FreeStringInfo(&readQueueLen);
        FreeStringInfo(&decodeQueueLen);

        rc =  snprintf_s(result[id].readerLsn, MAXFNAMELEN, MAXFNAMELEN - 1, "%X/%X",
            (uint32)(g_Logicaldispatcher[i].sentPtr >> upperPart), (uint32)g_Logicaldispatcher[i].sentPtr);
        securec_check_ss(rc, "\0", "\0");
        result[id].workingTxnCnt = g_Logicaldispatcher[i].workingTxnCnt;
        result[id].workingTxnMemory = g_Logicaldispatcher[i].workingTxnMemory;
        id++;
    }
    *num = id;
    return result;
}

