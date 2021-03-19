/* -------------------------------------------------------------------------
 *
 * slotfuncs.cpp
 *	   Support functions for replication slots
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Copyright (c) 2012-2014, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/replication/slotfuncs.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "funcapi.h"
#include "miscadmin.h"
#include "replication/slot.h"
#include "replication/logical.h"
#include "replication/logicalfuncs.h"
#include "replication/decode.h"
#include "access/transam.h"
#include "utils/builtins.h"
#include "access/xlog_internal.h"
#include "utils/inval.h"
#include "utils/resowner.h"
#include "utils/pg_lsn.h"
#include "access/xlog.h"
#include "postgres.h"
#include "knl/knl_variable.h"
#include "replication/replicainternal.h"
#include "replication/walsender.h"
#include "replication/syncrep.h"

#define AllSlotInUse(a, b) ((a) == (b))
extern void *internal_load_library(const char *libname);
extern bool PMstateIsRun(void);
static void redo_slot_create(const ReplicationSlotPersistentData *slotInfo, char* extra_content = NULL);
static XLogRecPtr create_physical_replication_slot_for_backup(const char* slot_name, bool is_dummy, char* extra);
static XLogRecPtr create_physical_replication_slot_for_archive(const char* slot_name, bool is_dummy, char* extra);
static void slot_advance(const char* slotname, XLogRecPtr &moveto, NameData &database, char *EndLsn, bool for_backup = false);


void log_slot_create(const ReplicationSlotPersistentData *slotInfo, char* extra_content)
{
    if (!u_sess->attr.attr_sql.enable_slot_log || !PMstateIsRun()) {
        return;
    }

    ReplicationSlotPersistentData xlrec;
    XLogRecPtr recptr;
    int rc = memcpy_s(&xlrec, ReplicationSlotPersistentDataConstSize, slotInfo,
                      ReplicationSlotPersistentDataConstSize);
    securec_check(rc, "\0", "\0");
    START_CRIT_SECTION();

    XLogBeginInsert();
    XLogRegisterData((char *)&xlrec, ReplicationSlotPersistentDataConstSize);
    if (extra_content != NULL && strlen(extra_content) != 0) {
        XLogRegisterData(extra_content, strlen(extra_content) + 1);
    }
    recptr = XLogInsert(RM_SLOT_ID, XLOG_SLOT_CREATE);
    XLogWaitFlush(recptr);
    if (g_instance.attr.attr_storage.max_wal_senders > 0)
        WalSndWakeup();

    END_CRIT_SECTION();

    if (u_sess->attr.attr_storage.guc_synchronous_commit > SYNCHRONOUS_COMMIT_LOCAL_FLUSH)
        SyncRepWaitForLSN(recptr);
}

void log_slot_advance(const ReplicationSlotPersistentData *slotInfo)
{
    if (!u_sess->attr.attr_sql.enable_slot_log || !PMstateIsRun()) {
        return;
    }

    ReplicationSlotPersistentData xlrec;
    XLogRecPtr Ptr;
    int rc = memcpy_s(&xlrec, ReplicationSlotPersistentDataConstSize, slotInfo,
                      ReplicationSlotPersistentDataConstSize);
    securec_check(rc, "\0", "\0");
    START_CRIT_SECTION();

    XLogBeginInsert();
    XLogRegisterData((char *)&xlrec, ReplicationSlotPersistentDataConstSize);

    Ptr = XLogInsert(RM_SLOT_ID, XLOG_SLOT_ADVANCE);
    XLogWaitFlush(Ptr);
    if (g_instance.attr.attr_storage.max_wal_senders > 0)
        WalSndWakeup();

    END_CRIT_SECTION();

#ifndef ENABLE_MULTIPLE_NODES
    if (u_sess->attr.attr_storage.guc_synchronous_commit > SYNCHRONOUS_COMMIT_LOCAL_FLUSH)
        SyncRepWaitForLSN(Ptr);
#endif
}

void log_slot_drop(const char *name)
{
    if (!u_sess->attr.attr_sql.enable_slot_log || !PMstateIsRun())
        return;
    XLogRecPtr Ptr;
    ReplicationSlotPersistentData xlrec;

    int rc = memcpy_s(xlrec.name.data, NAMEDATALEN, name, NAMEDATALEN);
    securec_check(rc, "\0", "\0");
    START_CRIT_SECTION();
    XLogBeginInsert();
    XLogRegisterData((char *)&xlrec, ReplicationSlotPersistentDataConstSize);

    Ptr = XLogInsert(RM_SLOT_ID, XLOG_SLOT_DROP);
    XLogWaitFlush(Ptr);
    if (g_instance.attr.attr_storage.max_wal_senders > 0)
        WalSndWakeup();
    END_CRIT_SECTION();
    if (u_sess->attr.attr_storage.guc_synchronous_commit > SYNCHRONOUS_COMMIT_LOCAL_FLUSH)
        SyncRepWaitForLSN(Ptr);
}

void LogCheckSlot()
{
    XLogRecPtr recptr;
    Size size;
    LogicalPersistentData *LogicalSlot = NULL;
    size = GetAllLogicalSlot(LogicalSlot);

    if (!u_sess->attr.attr_sql.enable_slot_log || !PMstateIsRun())
        return;
    START_CRIT_SECTION();

    XLogBeginInsert();
    XLogRegisterData((char *)LogicalSlot, size);

    recptr = XLogInsert(RM_SLOT_ID, XLOG_SLOT_CHECK);
    XLogWaitFlush(recptr);
    if (g_instance.attr.attr_storage.max_wal_senders > 0)
        WalSndWakeup();

    END_CRIT_SECTION();

    if (u_sess->attr.attr_storage.guc_synchronous_commit > SYNCHRONOUS_COMMIT_LOCAL_FLUSH) {
        SyncRepWaitForLSN(recptr);
        g_instance.comm_cxt.localinfo_cxt.set_term = true;
    }
}

Size GetAllLogicalSlot(LogicalPersistentData *&LogicalSlot)
{
    int i;
    int NumLogicalSlot = 0;
    Size size;
    /* Search for the named slot and mark it active if we find it. */
    (void)LWLockAcquire(ReplicationSlotAllocationLock, LW_SHARED);
    (void)LWLockAcquire(ReplicationSlotControlLock, LW_SHARED);
    (void)LWLockAcquire(LogicalReplicationSlotPersistentDataLock, LW_SHARED);
    for (i = 0; i < g_instance.attr.attr_storage.max_replication_slots; i++) {
        ReplicationSlot *s = &t_thrd.slot_cxt.ReplicationSlotCtl->replication_slots[i];

        if (s->in_use && s->data.database != InvalidOid && GET_SLOT_PERSISTENCY(s->data) == RS_PERSISTENT) {
            NumLogicalSlot++;
        }
    }
    size = offsetof(LogicalPersistentData, replication_slots) + NumLogicalSlot * sizeof(ReplicationSlotPersistentData);
    LogicalSlot = (LogicalPersistentData *)palloc(size);
    LogicalSlot->SlotNum = NumLogicalSlot;

    ReplicationSlotPersistentData *slotPoint = &LogicalSlot->replication_slots[0];
    for (i = 0; i < g_instance.attr.attr_storage.max_replication_slots; i++) {
        ReplicationSlot *s = &t_thrd.slot_cxt.ReplicationSlotCtl->replication_slots[i];

        if (s->in_use && s->data.database != InvalidOid && GET_SLOT_PERSISTENCY(s->data) == RS_PERSISTENT) {
            errno_t ret = memcpy_s(slotPoint, sizeof(ReplicationSlotPersistentData), &s->data,
                                   sizeof(ReplicationSlotPersistentData));
            securec_check(ret, "", "");
            slotPoint += 1;
        }
    }

    LWLockRelease(LogicalReplicationSlotPersistentDataLock);
    LWLockRelease(ReplicationSlotControlLock);
    LWLockRelease(ReplicationSlotAllocationLock);

    return size;
}

/*
 * SQL function for creating a new physical (streaming replication)
 * replication slot.
 */
Datum pg_create_physical_replication_slot(PG_FUNCTION_ARGS)
{
    Name name = PG_GETARG_NAME(0);
    bool isDummyStandby = PG_GETARG_BOOL(1);
    const int TUPLE_FIELDS = 2;
    Datum values[TUPLE_FIELDS];
    bool nulls[TUPLE_FIELDS];
    TupleDesc tupdesc;
    HeapTuple tuple;
    Datum result;

    ValidateName(NameStr(*name));

    check_permissions();

    CheckSlotRequirements();

    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE) {
        ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH), errmsg("return type must be a row type")));
    }
    /* acquire replication slot, this will check for conflicting names */
    ReplicationSlotCreate(NameStr(*name), RS_PERSISTENT, isDummyStandby, InvalidOid, InvalidXLogRecPtr);

    values[0] = CStringGetTextDatum(NameStr(t_thrd.slot_cxt.MyReplicationSlot->data.name));

    nulls[0] = false;
    nulls[1] = true;

    tuple = heap_form_tuple(tupdesc, values, nulls);
    result = HeapTupleGetDatum(tuple);

    ReplicationSlotRelease();
    PG_RETURN_DATUM(result);
}

/*
 * SQL function for creating a new physical (streaming replication)
 * replication slot.
 */
Datum pg_create_physical_replication_slot_extern(PG_FUNCTION_ARGS)
{
    Name name = PG_GETARG_NAME(0);
    bool isDummyStandby = PG_GETARG_BOOL(1);
    text* extra_content_text = NULL;
    char* extra_content = NULL;
    const int TUPLE_FIELDS = 2;
    Datum values[TUPLE_FIELDS];
    bool nulls[TUPLE_FIELDS];
    TupleDesc tupdesc;
    HeapTuple tuple;
    Datum result;
    XLogRecPtr restart_lsn = InvalidXLogRecPtr;
    char restart_lsn_str[MAXFNAMELEN] = {0};
    bool for_backup = false;
    int ret;

    ValidateName(NameStr(*name));

    for_backup = (strncmp(NameStr(*name), "gs_roach", strlen("gs_roach")) == 0);

    if (!PG_ARGISNULL(2)) {
        extra_content_text = PG_GETARG_TEXT_P(2);
        extra_content = text_to_cstring(extra_content_text);
    }

    t_thrd.slot_cxt.MyReplicationSlot = NULL;

    CheckSlotRequirements();

    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE) {
        ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH), errmsg("return type must be a row type")));
    }

    if (for_backup) {
        restart_lsn = create_physical_replication_slot_for_backup(NameStr(*name), isDummyStandby, extra_content);
    } else {
        restart_lsn = create_physical_replication_slot_for_archive(NameStr(*name), isDummyStandby, extra_content);
    }

    values[0] = CStringGetTextDatum(NameStr(*name));
    nulls[0] = false;

    if (XLogRecPtrIsInvalid(restart_lsn)) {
        nulls[1] = true;
    } else {
        ret = snprintf_s(restart_lsn_str, sizeof(restart_lsn_str), sizeof(restart_lsn_str) - 1, "%X/%X",
            (uint32)(restart_lsn >> 32), (uint32)restart_lsn);
        securec_check_ss(ret, "\0", "\0");
        values[1] = CStringGetTextDatum(restart_lsn_str);
    }

    tuple = heap_form_tuple(tupdesc, values, nulls);
    result = HeapTupleGetDatum(tuple);

    t_thrd.slot_cxt.MyReplicationSlot = NULL;

    pfree_ext(extra_content);
    PG_RETURN_DATUM(result);
}

void create_logical_replication_slot(const Name name, Name plugin, bool isDummyStandby, Oid databaseId,
                                     NameData *databaseName, char *str_tmp_lsn, int str_length)
{
    LogicalDecodingContext *ctx = NULL;
    CheckLogicalDecodingRequirements(databaseId);
    int rc = 0;
    char *fullname = NULL;
    fullname = expand_dynamic_library_name(NameStr(*plugin));

    Assert(!t_thrd.slot_cxt.MyReplicationSlot);

    if (RecoveryInProgress() == true)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("cannot create replication slot when recovery is in progress")));

    /* Load the shared library, unless we already did */
    (void)internal_load_library(fullname);

    /*
     * Acquire a logical decoding slot, this will check for conflicting
     * names.
     */
    ReplicationSlotCreate(NameStr(*name), RS_EPHEMERAL, isDummyStandby, databaseId, InvalidXLogRecPtr);

    /*
     * Create logical decoding context, to build the initial snapshot.
     */
    ctx = CreateInitDecodingContext(NameStr(*plugin), NIL, false, /* do not build snapshot */
                                    logical_read_local_xlog_page, NULL, NULL);
    /* build initial snapshot, might take a while */
    if (ctx != NULL) {
        DecodingContextFindStartpoint(ctx);
    }
    if (databaseName != NULL) {
        rc = snprintf_s(databaseName->data, NAMEDATALEN, NAMEDATALEN - 1, "%s",
                        t_thrd.slot_cxt.MyReplicationSlot->data.name.data);
        securec_check_ss(rc, "", "");
    }
    if (str_tmp_lsn != NULL) {
        rc = snprintf_s(str_tmp_lsn, str_length, str_length - 1, "%X/%X",
                        (uint32)(t_thrd.slot_cxt.MyReplicationSlot->data.confirmed_flush >> 32),
                        (uint32)t_thrd.slot_cxt.MyReplicationSlot->data.confirmed_flush);
        securec_check_ss(rc, "", "");
    }

    if (t_thrd.logical_cxt.sendFd >= 0) {
        (void)close(t_thrd.logical_cxt.sendFd);
        t_thrd.logical_cxt.sendFd = -1;
    }

    /* don't need the decoding context anymore */
    if (ctx != NULL) {
        FreeDecodingContext(ctx);
    }
    /* ok, slot is now fully created, mark it as persistent */
    ReplicationSlotPersist();
    log_slot_create(&t_thrd.slot_cxt.MyReplicationSlot->data);
    ReplicationSlotRelease();
}

void redo_slot_create(const ReplicationSlotPersistentData *slotInfo, char* extra_content)
{
    Assert(!t_thrd.slot_cxt.MyReplicationSlot);
    /*
     * Acquire a logical decoding slot, this will check for conflicting
     * names.
     */
    ReplicationSlotCreate(NameStr(slotInfo->name), RS_EPHEMERAL, slotInfo->isDummyStandby, slotInfo->database,
                          InvalidXLogRecPtr, extra_content, true);
    int rc = memcpy_s(&t_thrd.slot_cxt.MyReplicationSlot->data, sizeof(ReplicationSlotPersistentData), slotInfo,
                      sizeof(ReplicationSlotPersistentData));
    securec_check(rc, "\0", "\0");
    t_thrd.slot_cxt.MyReplicationSlot->effective_xmin = t_thrd.slot_cxt.MyReplicationSlot->data.xmin;
    t_thrd.slot_cxt.MyReplicationSlot->effective_catalog_xmin = t_thrd.slot_cxt.MyReplicationSlot->data.catalog_xmin;
    /* ok, slot is now fully created, mark it as persistent */
    ReplicationSlotMarkDirty();
    ReplicationSlotsComputeRequiredXmin(false);
    ReplicationSlotsComputeRequiredLSN(NULL);
    ReplicationSlotSave();
    ReplicationSlotRelease();
    if (extra_content != NULL && strlen(extra_content) != 0) {
        markObsSlotOperate(1);
    }
}

/*
 * SQL function for creating a new logical replication slot.
 */
Datum pg_create_logical_replication_slot(PG_FUNCTION_ARGS)
{
    Name name = PG_GETARG_NAME(0);
    Name plugin = PG_GETARG_NAME(1);
    errno_t rc = EOK;

    TupleDesc tupdesc;
    HeapTuple tuple;
    Datum result;
    const int TUPLE_FIELDS = 2;
    Datum values[TUPLE_FIELDS];
    bool nulls[TUPLE_FIELDS];
    char *str_tmp_lsn = NULL;
    NameData databaseName;

    ValidateName(NameStr(*name));
    ValidateName(NameStr(*plugin));
    str_tmp_lsn = (char *)palloc0(128);

    check_permissions();
    if (RecoveryInProgress())
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("Standby mode doesn't support create logical slot")));
    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
        ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH), errmsg("return type must be a row type")));

    create_logical_replication_slot(name, plugin, false, u_sess->proc_cxt.MyDatabaseId, &databaseName, str_tmp_lsn,
                                    128);

    values[0] = CStringGetTextDatum(NameStr(databaseName));
    values[1] = CStringGetTextDatum(str_tmp_lsn);
    pfree(str_tmp_lsn);
    str_tmp_lsn = NULL;
    rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
    securec_check(rc, "\0", "\0");

    tuple = heap_form_tuple(tupdesc, values, nulls);
    result = HeapTupleGetDatum(tuple);
    PG_RETURN_DATUM(result);
}

/*
 * SQL function for dropping a replication slot.
 */
Datum pg_drop_replication_slot(PG_FUNCTION_ARGS)
{
    Name name = PG_GETARG_NAME(0);
    bool for_backup = false;

    ValidateName(NameStr(*name));

    /* for gs_roach backup, acl is different and we always log slot drop */
    if (strncmp(NameStr(*name), "gs_roach", strlen("gs_roach")) == 0) {
        for_backup = true;
    }

    check_permissions(for_backup);

    CheckSlotRequirements();

    ReplicationSlotDrop(NameStr(*name), for_backup);

    PG_RETURN_VOID();
}

/*
 * pg_get_replication_slots - SQL SRF showing active replication slots.
 */
Datum pg_get_replication_slots(PG_FUNCTION_ARGS)
{
#define PG_GET_REPLICATION_SLOTS_COLS 9
    ReturnSetInfo *rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;
    TupleDesc tupdesc;
    Tuplestorestate *tupstore = NULL;
    MemoryContext per_query_ctx;
    MemoryContext oldcontext;
    int slotno;
    errno_t rc = EOK;
    int nRet = 0;

    /* check to see if caller supports us returning a tuplestore */
    if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("set-valued function called in context that cannot accept a set")));
    if (!(rsinfo->allowedModes & SFRM_Materialize))
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("materialize mode required, but it is not "
                                                                       "allowed in this context")));

    /* Build a tuple descriptor for our result type */
    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
        ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH), errmsg("return type must be a row type")));

    /*
     * We don't require any special permission to see this function's data
     * because nothing should be sensitive. The most critical being the slot
     * name, which shouldn't contain anything particularly sensitive.
     */
    per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
    oldcontext = MemoryContextSwitchTo(per_query_ctx);

    tupstore = tuplestore_begin_heap(true, false, u_sess->attr.attr_memory.work_mem);
    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult = tupstore;
    rsinfo->setDesc = tupdesc;

    (void)MemoryContextSwitchTo(oldcontext);

    for (slotno = 0; slotno < g_instance.attr.attr_storage.max_replication_slots; slotno++) {
        ReplicationSlot *slot = &t_thrd.slot_cxt.ReplicationSlotCtl->replication_slots[slotno];
        Datum values[PG_GET_REPLICATION_SLOTS_COLS];
        bool nulls[PG_GET_REPLICATION_SLOTS_COLS];

        TransactionId xmin;
        TransactionId catalog_xmin;
        XLogRecPtr restart_lsn;
        bool active = false;
        bool isDummyStandby = false;
        Oid database;
        const char *slot_name = NULL;

        char restart_lsn_s[MAXFNAMELEN];
        const char *plugin = NULL;
        int i;

        SpinLockAcquire(&slot->mutex);
        if (!slot->in_use) {
            SpinLockRelease(&slot->mutex);
            continue;
        } else {
            xmin = slot->data.xmin;
            catalog_xmin = slot->data.catalog_xmin;
            database = slot->data.database;
            restart_lsn = slot->data.restart_lsn;
            slot_name = pstrdup(NameStr(slot->data.name));

            plugin = pstrdup(NameStr(slot->data.plugin));
            active = slot->active;
            isDummyStandby = slot->data.isDummyStandby;
        }
        SpinLockRelease(&slot->mutex);

        rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "\0", "\0");

        nRet = snprintf_s(restart_lsn_s, sizeof(restart_lsn_s), sizeof(restart_lsn_s) - 1, "%X/%X",
                          (uint32)(restart_lsn >> 32), (uint32)restart_lsn);
        securec_check_ss(nRet, "\0", "\0");

        i = 0;
        values[i++] = CStringGetTextDatum(slot_name);
        if (database == InvalidOid)
            nulls[i++] = true;
        else
            values[i++] = CStringGetTextDatum(plugin);
        if (database == InvalidOid)
            values[i++] = CStringGetTextDatum("physical");
        else
            values[i++] = CStringGetTextDatum("logical");
        values[i++] = database;
        values[i++] = BoolGetDatum(active);
        if (xmin != InvalidTransactionId)
            values[i++] = TransactionIdGetDatum(xmin);
        else
            nulls[i++] = true;
        if (catalog_xmin != InvalidTransactionId)
            values[i++] = TransactionIdGetDatum(catalog_xmin);
        else
            nulls[i++] = true;
        if (!XLByteEQ(restart_lsn, InvalidXLogRecPtr))
            values[i++] = CStringGetTextDatum(restart_lsn_s);
        else
            nulls[i++] = true;

        values[i++] = BoolGetDatum(isDummyStandby);

        tuplestore_putvalues(tupstore, tupdesc, values, nulls);
    }

    tuplestore_donestoring(tupstore);

    return (Datum)0;
}

/*
 * pg_get_cur_replication_slot_name - SQL SRF showing replication slot name.
 */
Datum pg_get_replication_slot_name(PG_FUNCTION_ARGS)
{
    char *slotname = NULL;
    slotname = get_my_slot_name();
    text *t = cstring_to_text(slotname);
    pfree(slotname);
    slotname = NULL;
    PG_RETURN_TEXT_P(t);
}

/*
 * Helper function for advancing physical replication slot forward.
 * The LSN position to move to is compared simply to the slot's
 * restart_lsn, knowing that any position older than that would be
 * removed by successive checkpoints.
 */
static XLogRecPtr pg_physical_replication_slot_advance(XLogRecPtr moveto)
{
    XLogRecPtr startlsn = t_thrd.slot_cxt.MyReplicationSlot->data.restart_lsn;
    XLogRecPtr retlsn = startlsn;

    if (XLByteLT(startlsn, moveto)) {
        SpinLockAcquire(&t_thrd.slot_cxt.MyReplicationSlot->mutex);
        t_thrd.slot_cxt.MyReplicationSlot->data.restart_lsn = moveto;
        SpinLockRelease(&t_thrd.slot_cxt.MyReplicationSlot->mutex);
        retlsn = moveto;
    }

    return retlsn;
}

/*
 * Helper function for advancing logical replication slot forward.
 * The slot's restart_lsn is used as start point for reading records,
 * while confirmed_lsn is used as base point for the decoding context.
 * The LSN position to move to is checked by doing a per-record scan and
 * logical decoding which makes sure that confirmed_lsn is updated to a
 * LSN which allows the future slot consumer to get consistent logical
 * changes.
 */
static XLogRecPtr pg_logical_replication_slot_advance(XLogRecPtr moveto)
{
    LogicalDecodingContext *ctx = NULL;
    ResourceOwner old_resowner = t_thrd.utils_cxt.CurrentResourceOwner;
    XLogRecPtr startlsn = t_thrd.slot_cxt.MyReplicationSlot->data.restart_lsn;
    XLogRecPtr retlsn = t_thrd.slot_cxt.MyReplicationSlot->data.confirmed_flush;

    PG_TRY();
    {
        /* restart at slot's confirmed_flush */
        ctx = CreateDecodingContext(InvalidXLogRecPtr, NIL, true, logical_read_local_xlog_page, NULL, NULL);

        t_thrd.utils_cxt.CurrentResourceOwner = ResourceOwnerCreate(t_thrd.utils_cxt.CurrentResourceOwner,
                                                                    "logical decoding", MEMORY_CONTEXT_STORAGE);

        /* invalidate non-timetravel entries */
        if (!RecoveryInProgress())
            InvalidateSystemCaches();

        /* Decode until we run out of records */
        while ((!XLByteEQ(startlsn, InvalidXLogRecPtr) && XLByteLT(startlsn, moveto)) ||
               (!XLByteEQ(ctx->reader->EndRecPtr, InvalidXLogRecPtr) && XLByteLT(ctx->reader->EndRecPtr, moveto))) {
            XLogRecord *record = NULL;
            char *errm = NULL;

            record = XLogReadRecord(ctx->reader, startlsn, &errm);

            if (errm != NULL)
                ereport(ERROR, (errcode(ERRCODE_LOGICAL_DECODE_ERROR),
                                errmsg("Stopped to parse any valid XLog Record at %X/%X: %s.",
                                       (uint32)(ctx->reader->EndRecPtr >> 32), (uint32)ctx->reader->EndRecPtr, errm)));

            /*
             * Now that we've set up the xlog reader state, subsequent calls
             * pass InvalidXLogRecPtr to say "continue from last record"
             */
            startlsn = InvalidXLogRecPtr;

            /*
             * The {begin_txn,change,commit_txn}_wrapper callbacks above will
             * store the description into our tuplestore.
             */
            if (record != NULL)
                LogicalDecodingProcessRecord(ctx, ctx->reader);

            /* Stop once the moving point wanted by caller has been reached */
            if (XLByteLE(moveto, ctx->reader->EndRecPtr))
                break;

            CHECK_FOR_INTERRUPTS();
        }
        t_thrd.utils_cxt.CurrentResourceOwner = old_resowner;

        if (!XLByteEQ(ctx->reader->EndRecPtr, InvalidXLogRecPtr)) {
            LogicalConfirmReceivedLocation(moveto);

            /*
             * If only the confirmed_flush_lsn has changed the slot won't get
             * marked as dirty by the above. Callers on the walsender
             * interface are expected to keep track of their own progress and
             * don't need it written out. But SQL-interface users cannot
             * specify their own start positions and it's harder for them to
             * keep track of their progress, so we should make more of an
             * effort to save it for them.
             *
             * Dirty the slot so it's written out at the next checkpoint.
             * We'll still lose its position on crash, as documented, but it's
             * better than always losing the position even on clean restart.
             */
            ReplicationSlotMarkDirty();
        }

        retlsn = t_thrd.slot_cxt.MyReplicationSlot->data.confirmed_flush;

        /* free context, call shutdown callback */
        FreeDecodingContext(ctx);

        if (!RecoveryInProgress())
            InvalidateSystemCaches();
    }
    PG_CATCH();
    {
        /* clear all timetravel entries */
        if (!RecoveryInProgress())
            InvalidateSystemCaches();

        PG_RE_THROW();
    }
    PG_END_TRY();

    return retlsn;
}

void slot_advance(const char *slotname, XLogRecPtr &moveto, NameData &database, char *EndLsn, bool for_backup)
{
    XLogRecPtr endlsn;
    XLogRecPtr minlsn;
    errno_t rc;
    int ret = 0;

    Assert(!t_thrd.slot_cxt.MyReplicationSlot);

    /*
     * We can't move slot past what's been flushed/replayed so clamp the
     * target possition accordingly.
     */
    if (!RecoveryInProgress()) {
        XLogRecPtr FlushRecPtr = GetFlushRecPtr();
        /* for backup, we may advance restart_lsn to max lsn after copy xlog */
        if (XLByteLT(FlushRecPtr, moveto) && !for_backup) {
            moveto = FlushRecPtr;
        }
    } else {
        XLogRecPtr XLogReplayRecPtr = GetXLogReplayRecPtr(&t_thrd.xlog_cxt.ThisTimeLineID);
        if (XLByteLT(XLogReplayRecPtr, moveto))
            moveto = XLogReplayRecPtr;
    }

    if (XLogRecPtrIsInvalid(moveto))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid target wal lsn")));

    /* Acquire the slot so we "own" it */
    ReplicationSlotAcquire(slotname, false);

    /*
     * Check if the slot is not moving backwards.  Physical slots rely simply
     * on restart_lsn as a minimum point, while logical slots have confirmed
     * consumption up to confirmed_lsn, meaning that in both cases data older
     * than that is not available anymore.
     */
    if (OidIsValid(t_thrd.slot_cxt.MyReplicationSlot->data.database))
        minlsn = t_thrd.slot_cxt.MyReplicationSlot->data.confirmed_flush;
    else
        minlsn = t_thrd.slot_cxt.MyReplicationSlot->data.restart_lsn;

    /* for roach backup, we may reset restart lsn to InvalidXLogRecPtr after xlog copy */
    if (XLByteLT(moveto, minlsn)) {
        if (RecoveryInProgress()) {
            ReplicationSlotRelease();
            return;
        }
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("cannot move slot to %X/%X, minimum is %X/%X", (uint32)(moveto >> 32), (uint32)moveto,
                               (uint32)(minlsn >> 32), (uint32)(minlsn))));
    }

    /* Do the actual slot update, depending on the slot type */
    if (OidIsValid(t_thrd.slot_cxt.MyReplicationSlot->data.database))
        endlsn = pg_logical_replication_slot_advance(moveto);
    else
        endlsn = pg_physical_replication_slot_advance(moveto);

    if (!for_backup) {
        moveto = t_thrd.slot_cxt.MyReplicationSlot->data.confirmed_flush;
    }

    rc = memcpy_s((char *)database.data, NAMEDATALEN, t_thrd.slot_cxt.MyReplicationSlot->data.name.data, NAMEDATALEN);
    securec_check_c(rc, "\0", "\0");

    /* Update the on disk state when lsn was updated. */
    if (XLogRecPtrIsInvalid(endlsn) || for_backup) {
        ReplicationSlotMarkDirty();
        ReplicationSlotsComputeRequiredXmin(false);
        ReplicationSlotsComputeRequiredLSN(NULL);
        ReplicationSlotSave();
    }
    if (!RecoveryInProgress())
        log_slot_advance(&t_thrd.slot_cxt.MyReplicationSlot->data);
    ReplicationSlotRelease();

    /* Return the reached position. */
    ret = snprintf_s(EndLsn, NAMEDATALEN, NAMEDATALEN - 1, "%x/%x", (uint32)(endlsn >> 32), (uint32)endlsn);
    securec_check_ss(ret, "\0", "\0");
}

/*
 * SQL function for moving the position in a replication slot.
 */
Datum pg_replication_slot_advance(PG_FUNCTION_ARGS)
{
    Name slotname = PG_GETARG_NAME(0);
    XLogRecPtr moveto;
    TupleDesc tupdesc;
    HeapTuple tuple;
    Datum values[2];
    bool nulls[2];
    Datum result;
    NameData database;
    char EndLsn[NAMEDATALEN];
    bool for_backup = false;

    ValidateName(NameStr(*slotname));

    for_backup = (strncmp(NameStr(*slotname), "gs_roach", strlen("gs_roach")) == 0);

    if (RecoveryInProgress()) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_OPERATION), errmsg("couldn't advance in recovery")));
    }
    if (PG_ARGISNULL(1)) {
        if (!RecoveryInProgress())
            moveto = GetFlushRecPtr();
        else
            moveto = GetXLogReplayRecPtr(NULL);
    } else {
        const char *str_upto_lsn = TextDatumGetCString(PG_GETARG_DATUM(1));
        ValidateName(str_upto_lsn);
        if (!AssignLsn(&moveto, str_upto_lsn)) {
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION), errmsg("invalid input syntax for type lsn: \"%s\" "
                                                                          "of start_lsn", str_upto_lsn)));
        }
    }

    /* Build a tuple descriptor for our result type */
    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
        ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH), errmsg("return type must be a row type")));

    check_permissions(for_backup);
    if (!for_backup) {
        CheckLogicalDecodingRequirements(u_sess->proc_cxt.MyDatabaseId);
    }
    slot_advance(NameStr(*slotname), moveto, database, EndLsn, for_backup);
    values[0] = NameGetDatum(&database);
    nulls[0] = false;
    values[1] = CStringGetTextDatum(EndLsn);
    nulls[1] = false;
    tuple = heap_form_tuple(tupdesc, values, nulls);

    result = HeapTupleGetDatum(tuple);

    PG_RETURN_DATUM(result);
}

void redo_slot_advance(const ReplicationSlotPersistentData *slotInfo)
{
    errno_t rc;

    Assert(!t_thrd.slot_cxt.MyReplicationSlot);

    /*
     * If logical replication slot is active on the current standby, the current
     * standby notify the primary to advance the logical replication slot.
     * Thus, we do not redo the slot_advance log.
     */
#ifndef ENABLE_MULTIPLE_NODES
    if (IsReplicationSlotActive(NameStr(slotInfo->name))) {
        return;
    }
#endif

    /* Acquire the slot so we "own" it */
    ReplicationSlotAcquire(NameStr(slotInfo->name), false);
    rc = memcpy_s(&t_thrd.slot_cxt.MyReplicationSlot->data, sizeof(ReplicationSlotPersistentData), slotInfo,
                  sizeof(ReplicationSlotPersistentData));
    securec_check(rc, "\0", "\0");
    t_thrd.slot_cxt.MyReplicationSlot->effective_xmin = slotInfo->xmin;
    t_thrd.slot_cxt.MyReplicationSlot->effective_catalog_xmin = slotInfo->catalog_xmin;
    ReplicationSlotMarkDirty();
    ReplicationSlotsComputeRequiredXmin(false);
    ReplicationSlotsComputeRequiredLSN(NULL);
    ReplicationSlotSave();
    ReplicationSlotRelease();
}

void LogicalSlotCheckDelete(const LogicalPersistentData *LogicalSlot)
{
    int DeleteNum = 0;
    char SlotName[g_instance.attr.attr_storage.max_replication_slots][NAMEDATALEN];
    LWLockAcquire(ReplicationSlotControlLock, LW_SHARED);
    for (int i = 0; i < g_instance.attr.attr_storage.max_replication_slots; i++) {
        ReplicationSlot *s = &t_thrd.slot_cxt.ReplicationSlotCtl->replication_slots[i];
        if (s->in_use && s->data.database != InvalidOid) {
            bool shouldDelete = true;
            for (int j = 0; j < LogicalSlot->SlotNum; j++) {
                if (strcmp(NameStr(s->data.name), NameStr(LogicalSlot->replication_slots[j].name)) == 0) {
                    shouldDelete = false;
                    break;
                }
            }
            if (shouldDelete == true) {
                errno_t ret = memcpy_s(SlotName[DeleteNum], NAMEDATALEN, NameStr(s->data.name), NAMEDATALEN);
                securec_check(ret, "", "");
                DeleteNum++;
            }
        }
    }
    LWLockRelease(ReplicationSlotControlLock);
    for (int i = 0; i < DeleteNum; i++) {
        ReplicationSlotDrop(SlotName[i]);
    }
}
void LogicalSlotCheckAdd(LogicalPersistentData *logicalSlot)
{
    for (int i = 0; i < logicalSlot->SlotNum; i++) {
        if (!ReplicationSlotFind(logicalSlot->replication_slots[i].name.data)) {
            redo_slot_create(&logicalSlot->replication_slots[i]);
        }
    }
}
void LogicalSlotCheck(LogicalPersistentData *LogicalSlot)
{
    LogicalSlotCheckDelete(LogicalSlot);
    LogicalSlotCheckAdd(LogicalSlot);
}

/*
 * Get the current number of slots in use
 */
int get_in_use_slot_number()
{
    int SlotCount = 0;
    LWLockAcquire(ReplicationSlotControlLock, LW_SHARED);
    for (int i = 0; i < g_instance.attr.attr_storage.max_replication_slots; i++) {
        ReplicationSlot *s = &t_thrd.slot_cxt.ReplicationSlotCtl->replication_slots[i];
        if (s->in_use) {
            SlotCount++;
        }
    }
    LWLockRelease(ReplicationSlotControlLock);
    return SlotCount;
}

void slot_redo(XLogReaderState *record)
{
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    ReplicationSlotPersistentData *xlrec = (ReplicationSlotPersistentData *)XLogRecGetData(record);
    LogicalPersistentData *LogicalSlot = (LogicalPersistentData *)XLogRecGetData(record);

    /* Backup blocks are not used in xlog records */
    Assert(!XLogRecHasAnyBlockRefs(record));
    switch (info) {
        /*
         * Rmgrs we care about for logical decoding. Add new rmgrs in
         * rmgrlist.h's order.
         */
        case XLOG_SLOT_CREATE:
            if (!ReplicationSlotFind(xlrec->name.data)) {
                /*
                 * If the current slot number of the standby machine is equal to max_replication_slots,
                 * and this is the redo log of type XLOG_SLOT_CREATE,
                 * the program directly breaks and no longer executes.
                 * Because this XLOG must be a historical log, and there must be a xlog of type XLOG_SLOT_DROP after it.
                 */
                int SlotCount = get_in_use_slot_number();
                if (AllSlotInUse(SlotCount, g_instance.attr.attr_storage.max_replication_slots)) {
                    break;
                } else {
                    char* extra_content = NULL;
                    if (GET_SLOT_EXTRA_DATA_LENGTH(*xlrec) != 0) {
                        extra_content = (char*)XLogRecGetData(record) + ReplicationSlotPersistentDataConstSize;
                        Assert(strlen(extra_content) == (uint32)(GET_SLOT_EXTRA_DATA_LENGTH(*xlrec)));
                    }
                    redo_slot_create(xlrec, extra_content);
                }
            } else {
                redo_slot_reset_for_backup(xlrec);
            }
            break;
        case XLOG_SLOT_ADVANCE:
            if (ReplicationSlotFind(xlrec->name.data))
                redo_slot_advance(xlrec);
            else
                redo_slot_create(xlrec);
            break;
        case XLOG_SLOT_DROP:
            if (ReplicationSlotFind(xlrec->name.data))
                ReplicationSlotDrop(NameStr(xlrec->name));
            break;
        case XLOG_SLOT_CHECK:
            LogicalSlotCheck(LogicalSlot);
            break;
        default:
            break;
    }
}
void write_term_log(uint32 term)
{
    XLogRecPtr recptr;
    START_CRIT_SECTION();

    XLogBeginInsert();
    XLogRegisterData((char *)&term, sizeof(uint32));

    recptr = XLogInsert(RM_SLOT_ID, XLOG_TERM_LOG);
    XLogWaitFlush(recptr);
    if (g_instance.attr.attr_storage.max_wal_senders > 0) {
        WalSndWakeup();
    }
    END_CRIT_SECTION();

    if (u_sess->attr.attr_storage.guc_synchronous_commit > SYNCHRONOUS_COMMIT_LOCAL_FLUSH) {
        SyncRepWaitForLSN(recptr);
    }
}

bool is_archive_slot(ReplicationSlotPersistentData data)
{
    return GET_SLOT_EXTRA_DATA_LENGTH(data) != 0;
}

XLogRecPtr create_physical_replication_slot_for_archive(const char* slot_name, bool is_dummy, char* extra_content)
{
    /* before upgrade commit, we can not use new slot extra field */
    if (t_thrd.proc->workingVersionNum < EXTRA_SLOT_VERSION_NUM) {
        ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH), errmsg("version must be bigger than 92260")));
    }

    check_permissions();

    /* create persistent replication slot with extra archive configuration */
    ReplicationSlotCreate(slot_name, RS_PERSISTENT, is_dummy, InvalidOid, InvalidXLogRecPtr, extra_content);
    /* log slot creation */
    log_slot_create(&t_thrd.slot_cxt.MyReplicationSlot->data, t_thrd.slot_cxt.MyReplicationSlot->extra_content);
    markObsSlotOperate(1);

    /* To make sure slot log lsn is less newest checkpoint.redo */
    if (u_sess->attr.attr_sql.enable_slot_log == true) {
        RequestCheckpoint(CHECKPOINT_FORCE | CHECKPOINT_WAIT | CHECKPOINT_IMMEDIATE);
    }

    return InvalidXLogRecPtr;
}

XLogRecPtr create_physical_replication_slot_for_backup(const char* slot_name, bool is_dummy, char* extra_content)
{
    XLogRecPtr restart_lsn = InvalidXLogRecPtr;
    NameData database;
    char end_lsn[NAMEDATALEN];
    bool backup_started_in_recovery = RecoveryInProgress();

    check_permissions(true);

    if (backup_started_in_recovery) {
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("recovery is in progress"),
                       errhint("roach backup cannot be executed during recovery.")));
    }

    (void)LWLockAcquire(ControlFileLock, LW_SHARED);
    restart_lsn = t_thrd.shemem_ptr_cxt.ControlFile->checkPointCopy.redo;
    LWLockRelease(ControlFileLock);

    /*
    * For backup, we advance slot restart lsn to current checkpoint redo point.
    * Later, pg_start_backup will be called, in which the same or subsequent checkpoint redo point will be
    * returned as backup start lsn. After this call, lsn larger than current or subsequent redo point will
    * nolonger be recycled.
    */
    ReplicationSlotCreate(slot_name, RS_BACKUP, is_dummy, InvalidOid, InvalidXLogRecPtr, extra_content);
    /* log slot creation, in which quorum replication will be confirmed. */
    log_slot_create(&t_thrd.slot_cxt.MyReplicationSlot->data, NULL);
    ReplicationSlotRelease();
    slot_advance(slot_name, restart_lsn, database, end_lsn, true);

    ereport(LOG,
            (errmsg("advance restart lsn for initial backup slot, slotname: %s, restartlsn: %s", slot_name, end_lsn)));

    return restart_lsn;
}
