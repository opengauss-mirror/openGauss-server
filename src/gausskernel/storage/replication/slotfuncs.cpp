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
#include "replication/walreceiver.h"
#include "replication/walsender.h"
#include "replication/syncrep.h"
#include "replication/archive_walreceiver.h"

#define AllSlotInUse(a, b) ((a) == (b))
extern void *internal_load_library(const char *libname);
static void redo_slot_create(const ReplicationSlotPersistentData *slotInfo, char* extra_content = NULL);
#ifndef ENABLE_LITE_MODE
static XLogRecPtr create_physical_replication_slot_for_backup(const char* slot_name, bool is_dummy, char* extra);
static XLogRecPtr create_physical_replication_slot_for_archive(const char* slot_name, bool is_dummy, char* extra,
    XLogRecPtr currFlushPtr = InvalidXLogRecPtr);
#endif
static void slot_advance(const char* slotname, XLogRecPtr &moveto, NameData &database, char *EndLsn, bool for_backup = false);


void log_slot_create(const ReplicationSlotPersistentData *slotInfo, char* extra_content)
{
    if (!u_sess->attr.attr_sql.enable_slot_log || RecoveryInProgress()) {
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

    if (u_sess->attr.attr_storage.guc_synchronous_commit > SYNCHRONOUS_COMMIT_LOCAL_FLUSH) {
        if (g_instance.attr.attr_storage.dcf_attr.enable_dcf) {
            SyncPaxosWaitForLSN(recptr);
        } else {
            SyncRepWaitForLSN(recptr);
        }
    }
}

void log_slot_advance(const ReplicationSlotPersistentData *slotInfo, char* extra_content)
{
    if ((!u_sess->attr.attr_sql.enable_slot_log && t_thrd.role != ARCH) || RecoveryInProgress()) {
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
    if (extra_content != NULL && strlen(extra_content) != 0) {
        XLogRegisterData(extra_content, strlen(extra_content) + 1);
    }
    Ptr = XLogInsert(RM_SLOT_ID, XLOG_SLOT_ADVANCE);
    XLogWaitFlush(Ptr);
    if (g_instance.attr.attr_storage.max_wal_senders > 0)
        WalSndWakeup();

    END_CRIT_SECTION();

#ifndef ENABLE_MULTIPLE_NODES
    if (u_sess->attr.attr_storage.guc_synchronous_commit > SYNCHRONOUS_COMMIT_LOCAL_FLUSH) {
        if (g_instance.attr.attr_storage.dcf_attr.enable_dcf) {
            SyncPaxosWaitForLSN(Ptr);
        } else {
            SyncRepWaitForLSN(Ptr);
        }
    }
#endif
}

void log_slot_drop(const char *name)
{
    if (!u_sess->attr.attr_sql.enable_slot_log || RecoveryInProgress())
        return;
    XLogRecPtr Ptr;
    ReplicationSlotPersistentData xlrec;

    errno_t rc = memset_s(&xlrec, sizeof(ReplicationSlotPersistentData), 0, sizeof(ReplicationSlotPersistentData));
    securec_check(rc, "\0", "\0");

    rc = memcpy_s(xlrec.name.data, NAMEDATALEN, name, strlen(name));
    securec_check(rc, "\0", "\0");
    START_CRIT_SECTION();
    XLogBeginInsert();
    XLogRegisterData((char *)&xlrec, ReplicationSlotPersistentDataConstSize);

    Ptr = XLogInsert(RM_SLOT_ID, XLOG_SLOT_DROP);
    XLogWaitFlush(Ptr);
    if (g_instance.attr.attr_storage.max_wal_senders > 0)
        WalSndWakeup();
    END_CRIT_SECTION();
    if (u_sess->attr.attr_storage.guc_synchronous_commit > SYNCHRONOUS_COMMIT_LOCAL_FLUSH) {
        if (g_instance.attr.attr_storage.dcf_attr.enable_dcf) {
            SyncPaxosWaitForLSN(Ptr);
        } else {
            SyncRepWaitForLSN(Ptr);
        }
    }
}

void LogCheckSlot()
{
    XLogRecPtr recptr;
    Size size;
    LogicalPersistentData *LogicalSlot = NULL;
    size = GetAllLogicalSlot(LogicalSlot);

    if (!u_sess->attr.attr_sql.enable_slot_log || RecoveryInProgress())
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
        if (g_instance.attr.attr_storage.dcf_attr.enable_dcf) {
            SyncPaxosWaitForLSN(recptr);
        } else {
            SyncRepWaitForLSN(recptr);
            g_instance.comm_cxt.localinfo_cxt.set_term = true;
        }
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

    ValidateInputString(NameStr(*name));

    check_permissions();

    CheckSlotRequirements();

    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE) {
        ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH), errmsg("return type must be a row type")));
    }

    if (SS_IN_REFORM || SS_NORMAL_STANDBY) {
        ereport(ERROR, (errmsg("Operation can't be excuted during reform or on DMS standby node!")));
    }

    /* acquire replication slot, this will check for conflicting names */
    ReplicationSlotCreate(NameStr(*name), RS_PERSISTENT, isDummyStandby, InvalidOid, InvalidXLogRecPtr,
        InvalidXLogRecPtr);

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
#ifndef ENABLE_LITE_MODE
    Name name = PG_GETARG_NAME(0);
    bool isDummyStandby = PG_GETARG_BOOL(1);
    XLogRecPtr currFlushPtr = InvalidXLogRecPtr;
    bool isNeedRecycleXlog = false;
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

    ValidateInputString(NameStr(*name));

    for_backup = (strncmp(NameStr(*name), "gs_roach", strlen("gs_roach")) == 0);

    if (!PG_ARGISNULL(2)) {
        extra_content_text = PG_GETARG_TEXT_P(2);
        extra_content = text_to_cstring(extra_content_text);
    }
    if (!PG_ARGISNULL(3)) {
        isNeedRecycleXlog = PG_GETARG_BOOL(3);
    }

    t_thrd.slot_cxt.MyReplicationSlot = NULL;

    CheckSlotRequirements();

    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE) {
        ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH), errmsg("return type must be a row type")));
    }

    if (SS_IN_REFORM || SS_NORMAL_STANDBY) {
        ereport(ERROR, (errmsg("Operation can't be excuted during reform or on DMS standby node!")));
    }

    if (for_backup) {
        restart_lsn = create_physical_replication_slot_for_backup(NameStr(*name), isDummyStandby, extra_content);
    } else {
        if (isNeedRecycleXlog) {
            ArchiveConfig* archive_config = formArchiveConfigFromStr(extra_content, false);
            XLogRecPtr switchPtr = InvalidXLogRecPtr;
            XLogRecPtr waitPtr = InvalidXLogRecPtr;
            switchPtr = waitPtr = RequestXLogSwitch();
            waitPtr += XLogSegSize - 1;
            waitPtr -= waitPtr % XLogSegSize;
            XLogWaitFlush(waitPtr);
            currFlushPtr = GetFlushRecPtr();
            ereport(LOG, (errmsg("push slot to switch point %lu, wait point %lu, flush point %lu",
                switchPtr, waitPtr, currFlushPtr)));
            (void)archive_replication_cleanup(currFlushPtr, archive_config, true);
            pfree_ext(archive_config);
        } else {
            currFlushPtr = GetFlushRecPtr();
        }
        currFlushPtr -= currFlushPtr % XLogSegSize;
        ereport(LOG, (errmsg("create archive slot, start point %lu", currFlushPtr)));
        restart_lsn = create_physical_replication_slot_for_archive(NameStr(*name), isDummyStandby, extra_content,
            currFlushPtr);
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
#else
    FEATURE_ON_LITE_MODE_NOT_SUPPORTED();
    PG_RETURN_DATUM(0);
#endif
}

void create_logical_replication_slot(const Name name, Name plugin, bool isDummyStandby, Oid databaseId,
                                     NameData *databaseName, char *str_tmp_lsn, int str_length,
                                     XLogRecPtr restart_lsn, XLogRecPtr confirmed_lsn)
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
    ReplicationSlotCreate(NameStr(*name), RS_EPHEMERAL, isDummyStandby, databaseId, restart_lsn, confirmed_lsn);

    /* if restart_lsn and confirmed_lsh are specified, no need to create logical decoding context */
    if (XLogRecPtrIsValid(restart_lsn) && XLogRecPtrIsValid(confirmed_lsn)) {
        ReplicationSlot *slot = t_thrd.slot_cxt.MyReplicationSlot;

        /* register output plugin name with slot */
        SpinLockAcquire(&slot->mutex);
        rc = strncpy_s(NameStr(slot->data.plugin), NAMEDATALEN, NameStr(*plugin), NAMEDATALEN - 1);
        securec_check(rc, "\0", "\0");
        NameStr(slot->data.plugin)[NAMEDATALEN - 1] = '\0';
        SpinLockRelease(&slot->mutex);

        goto create_done;
    }

    /*
     * Create logical decoding context, to build the initial snapshot.
     */
    ctx = CreateInitDecodingContext(NameStr(*plugin), NIL, false, /* do not build snapshot */
                                    logical_read_local_xlog_page, NULL, NULL);
    /* build initial snapshot, might take a while */
    if (ctx != NULL) {
        DecodingContextFindStartpoint(ctx);
    }

create_done:
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
                          InvalidXLogRecPtr, InvalidXLogRecPtr, extra_content, true);
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
    if (extra_content != NULL && strlen(extra_content) != 0) {
        MarkArchiveSlotOperate();
        add_archive_slot_to_instance(t_thrd.slot_cxt.MyReplicationSlot);
    }
    ReplicationSlotRelease();
}

static void check_and_assign_lsn(PG_FUNCTION_ARGS, XLogRecPtr *restart_lsn, XLogRecPtr *confirmed_lsn)
{
#define RESTART_IDX 2
#define CONFIRM_IDX 3
    if (PG_ARGISNULL(RESTART_IDX) || PG_ARGISNULL(CONFIRM_IDX)) {
        ereport(ERROR, (errmsg("restart_lsn or confirmed_flush should not be NULL")));
    }
    const char *str_restart_lsn = TextDatumGetCString(PG_GETARG_DATUM(RESTART_IDX));
    ValidateInputString(str_restart_lsn);
    if (!AssignLsn(restart_lsn, str_restart_lsn)) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION), errmsg("invalid input syntax for type lsn: \"%s\" "
                                                                      "of start_lsn", str_restart_lsn)));
    }
    const char *str_confirmed_lsn = TextDatumGetCString(PG_GETARG_DATUM(CONFIRM_IDX));
    ValidateInputString(str_confirmed_lsn);
    if (!AssignLsn(confirmed_lsn, str_confirmed_lsn)) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION), errmsg("invalid input syntax for type lsn: \"%s\" "
                                                                      "of confirmed_flush",
                                                                      str_confirmed_lsn)));
    }
}

/*
 * SQL function for creating a new logical replication slot.
 */
Datum pg_create_logical_replication_slot_with_lsn(PG_FUNCTION_ARGS)
{
    if (SS_IN_REFORM || SS_NORMAL_STANDBY) {
        ereport(ERROR, (errmsg("Operation can't be excuted during reform or on DMS standby node!")));
    }

    Name name = PG_GETARG_NAME(0);
    Name plugin = PG_GETARG_NAME(1);
    errno_t rc = EOK;
    XLogRecPtr restart_lsn = InvalidXLogRecPtr;
    XLogRecPtr confirmed_lsn = InvalidXLogRecPtr;
    TupleDesc tupdesc;
    HeapTuple tuple;
    Datum result;
    const int TUPLE_FIELDS = 2;
    Datum values[TUPLE_FIELDS];
    bool nulls[TUPLE_FIELDS];
    char str_tmp_lsn[128] = {0};
    NameData databaseName;

    ValidateInputString(NameStr(*name));
    ValidateInputString(NameStr(*plugin));

    check_and_assign_lsn(fcinfo, &restart_lsn, &confirmed_lsn);

    Oid userId = GetUserId();
    CheckLogicalPremissions(userId);
    if (RecoveryInProgress())
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("Standby mode doesn't support create logical slot")));
    /* we are about to start streaming switch over, stop any xlog insert. */
    if (t_thrd.xlog_cxt.LocalXLogInsertAllowed == 0 && g_instance.streaming_dr_cxt.isInSwitchover == true) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("cannot create logical slot during streaming disaster recovery")));
    }
    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
        ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH), errmsg("return type must be a row type")));

    create_logical_replication_slot(name, plugin, false, u_sess->proc_cxt.MyDatabaseId, &databaseName, str_tmp_lsn,
                                    128, restart_lsn, confirmed_lsn);

    values[0] = CStringGetTextDatum(NameStr(databaseName));
    values[1] = CStringGetTextDatum(str_tmp_lsn);
    rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
    securec_check(rc, "\0", "\0");

    tuple = heap_form_tuple(tupdesc, values, nulls);
    result = HeapTupleGetDatum(tuple);
    PG_RETURN_DATUM(result);
}

/*
 * SQL function for creating a new logical replication slot.
 */
Datum pg_create_logical_replication_slot(PG_FUNCTION_ARGS)
{
    if (SS_IN_REFORM || SS_NORMAL_STANDBY) {
        ereport(ERROR, (errmsg("Operation can't be excuted during reform or on DMS standby node!")));
    }

    Name name = PG_GETARG_NAME(0);
    Name plugin = PG_GETARG_NAME(1);
    errno_t rc = EOK;

    TupleDesc tupdesc;
    HeapTuple tuple;
    Datum result;
    const int TUPLE_FIELDS = 2;
    Datum values[TUPLE_FIELDS];
    bool nulls[TUPLE_FIELDS];
    char str_tmp_lsn[128] = {0};
    NameData databaseName;

    ValidateInputString(NameStr(*name));
    ValidateInputString(NameStr(*plugin));

    Oid userId = GetUserId();
    CheckLogicalPremissions(userId);
    if (RecoveryInProgress())
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("Standby mode doesn't support create logical slot")));
    /* we are about to start streaming switch over, stop any xlog insert. */
    if (t_thrd.xlog_cxt.LocalXLogInsertAllowed == 0 && g_instance.streaming_dr_cxt.isInSwitchover == true) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("cannot create logical slot during streaming disaster recovery")));
    }
    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
        ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH), errmsg("return type must be a row type")));

    create_logical_replication_slot(name, plugin, false, u_sess->proc_cxt.MyDatabaseId, &databaseName, str_tmp_lsn,
                                    128);

    values[0] = CStringGetTextDatum(NameStr(databaseName));
    values[1] = CStringGetTextDatum(str_tmp_lsn);
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
    if (SS_IN_REFORM || SS_NORMAL_STANDBY) {
        ereport(ERROR, (errmsg("Operation can't be excuted during reform or on DMS standby node!")));
    }
    
    Name name = PG_GETARG_NAME(0);
    bool for_backup = false;
    bool isLogical = false;

    ValidateInputString(NameStr(*name));

    /* for gs_roach backup, acl is different and we always log slot drop */
    if (strncmp(NameStr(*name), "gs_roach", strlen("gs_roach")) == 0) {
        for_backup = true;
    }

    check_permissions(for_backup);

    isLogical = IsLogicalReplicationSlot(NameStr(*name));
    if (isLogical) {
        Oid userId = GetUserId();
        CheckLogicalPremissions(userId);
    }
    if (isLogical && RecoveryInProgress())
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("Standby mode doesn't support drop logical slot.")));

    /* we are about to start streaming switch over, stop any xlog insert. */
    if (t_thrd.xlog_cxt.LocalXLogInsertAllowed == 0 && g_instance.streaming_dr_cxt.isInSwitchover == true) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("cannot drop logical slot during streaming disaster recovery")));
    }

    CheckSlotRequirements();

    ReplicationSlotDrop(NameStr(*name), for_backup);

    PG_RETURN_VOID();
}

/*
 * pg_get_replication_slots - SQL SRF showing active replication slots.
 */
Datum pg_get_replication_slots(PG_FUNCTION_ARGS)
{
#define PG_GET_REPLICATION_SLOTS_COLS 10
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
        XLogRecPtr confirmed_lsn;
        bool active = false;
        bool isDummyStandby = false;
        Oid database;
        const char *slot_name = NULL;

        char lsn_buf[MAXFNAMELEN];
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
            confirmed_lsn = slot->data.confirmed_flush;
            slot_name = pstrdup(NameStr(slot->data.name));

            plugin = pstrdup(NameStr(slot->data.plugin));
            active = slot->active;
            isDummyStandby = slot->data.isDummyStandby;
        }
        SpinLockRelease(&slot->mutex);

        rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "\0", "\0");

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

        const uint32 upperPart = 32;
        /* fill restart_lsn */
        nRet = snprintf_s(lsn_buf, sizeof(lsn_buf), sizeof(lsn_buf) - 1, "%X/%X",
                          (uint32)(restart_lsn >> upperPart), (uint32)restart_lsn);
        securec_check_ss(nRet, "\0", "\0");
        if (!XLByteEQ(restart_lsn, InvalidXLogRecPtr))
            values[i++] = CStringGetTextDatum(lsn_buf);
        else
            nulls[i++] = true;

        /* fill dummy_standby */
        values[i++] = BoolGetDatum(isDummyStandby);

        /* fill confirmed_lsn */
        nRet = snprintf_s(lsn_buf, sizeof(lsn_buf), sizeof(lsn_buf) - 1, "%X/%X",
                          (uint32)(confirmed_lsn >> upperPart), (uint32)confirmed_lsn);
        securec_check_ss(nRet, "\0", "\0");
        if (!XLByteEQ(confirmed_lsn, InvalidXLogRecPtr))
            values[i++] = CStringGetTextDatum(lsn_buf);
        else
            nulls[i++] = true;

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
            "logical decoding", THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE));

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

    ValidateInputString(NameStr(*slotname));

    for_backup = (strncmp(NameStr(*slotname), "gs_roach", strlen("gs_roach")) == 0);

    if (RecoveryInProgress()) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_OPERATION), errmsg("couldn't advance in recovery")));
    }

    /* we are about to start streaming switch over, stop any xlog insert. */
    if (t_thrd.xlog_cxt.LocalXLogInsertAllowed == 0 && g_instance.streaming_dr_cxt.isInSwitchover == true) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_OPERATION),
                errmsg("cannot advance slot during streaming disaster recovery")));
    }

    if (PG_ARGISNULL(1)) {
        if (!RecoveryInProgress())
            moveto = GetFlushRecPtr();
        else
            moveto = GetXLogReplayRecPtr(NULL);
    } else {
        const char *str_upto_lsn = TextDatumGetCString(PG_GETARG_DATUM(1));
        ValidateInputString(str_upto_lsn);
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
        Oid userId = GetUserId();
        CheckLogicalPremissions(userId);
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

static void updateRedoSlotPersistentData(const ReplicationSlotPersistentData *slotInfo)
{
    TransactionId xmin = t_thrd.slot_cxt.MyReplicationSlot->data.xmin;
    TransactionId catalog_xmin = t_thrd.slot_cxt.MyReplicationSlot->data.catalog_xmin;
    XLogRecPtr restart_lsn = t_thrd.slot_cxt.MyReplicationSlot->data.restart_lsn;
    XLogRecPtr confirmed_flush = t_thrd.slot_cxt.MyReplicationSlot->data.confirmed_flush;

    errno_t rc = memcpy_s(&t_thrd.slot_cxt.MyReplicationSlot->data, sizeof(ReplicationSlotPersistentData), slotInfo,
        sizeof(ReplicationSlotPersistentData));
    securec_check(rc, "\0", "\0");

    if (t_thrd.slot_cxt.MyReplicationSlot->data.database == InvalidOid) {
        return;
    }

    if (TransactionIdPrecedes(t_thrd.slot_cxt.MyReplicationSlot->data.xmin, xmin)) {
        t_thrd.slot_cxt.MyReplicationSlot->data.xmin = xmin;
    }
    if (TransactionIdPrecedes(t_thrd.slot_cxt.MyReplicationSlot->data.catalog_xmin, catalog_xmin)) {
        t_thrd.slot_cxt.MyReplicationSlot->data.catalog_xmin = catalog_xmin;
    }
    if (XLByteLT(t_thrd.slot_cxt.MyReplicationSlot->data.restart_lsn, restart_lsn)) {
        t_thrd.slot_cxt.MyReplicationSlot->data.restart_lsn = restart_lsn;
    }
    if (XLByteLT(t_thrd.slot_cxt.MyReplicationSlot->data.confirmed_flush, confirmed_flush)) {
        t_thrd.slot_cxt.MyReplicationSlot->data.confirmed_flush = confirmed_flush;
    }
}

void redo_slot_advance(const ReplicationSlotPersistentData *slotInfo)
{
    Assert(!t_thrd.slot_cxt.MyReplicationSlot);

    /*
     * If logical replication slot is active on the current standby, the current
     * standby notify the primary to advance the logical replication slot.
     * Thus, we do not redo the slot_advance log.
     */
    if (IsReplicationSlotActive(NameStr(slotInfo->name))) {
        return;
    }

    /* Acquire the slot so we "own" it */
    ReplicationSlotAcquire(NameStr(slotInfo->name), false);
    updateRedoSlotPersistentData(slotInfo);

    if (t_thrd.slot_cxt.MyReplicationSlot->data.database == InvalidOid ||
        TransactionIdPrecedes(t_thrd.slot_cxt.MyReplicationSlot->effective_xmin, slotInfo->xmin)) {
        t_thrd.slot_cxt.MyReplicationSlot->effective_xmin = slotInfo->xmin;
    }
    if (t_thrd.slot_cxt.MyReplicationSlot->data.database == InvalidOid ||
        TransactionIdPrecedes(t_thrd.slot_cxt.MyReplicationSlot->effective_catalog_xmin, slotInfo->catalog_xmin)) {
        t_thrd.slot_cxt.MyReplicationSlot->effective_catalog_xmin = slotInfo->catalog_xmin;
    }
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
    char* extra_content = NULL;
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    ReplicationSlotPersistentData *xlrec = (ReplicationSlotPersistentData *)XLogRecGetData(record);
    LogicalPersistentData *LogicalSlot = (LogicalPersistentData *)XLogRecGetData(record);

    if (info == XLOG_TERM_LOG) {
        /* nothing to replay for term log */
        return;
    }

    if ((IS_MULTI_DISASTER_RECOVER_MODE && (info == XLOG_SLOT_CREATE || info == XLOG_SLOT_ADVANCE)) ||
        IsRoachRestore() || t_thrd.xlog_cxt.recoveryTarget == RECOVERY_TARGET_TIME_OBS) {
        return;
    }

    /* Backup blocks are not used in xlog records */
    Assert(!XLogRecHasAnyBlockRefs(record));
    if (info != XLOG_SLOT_CHECK && GET_SLOT_EXTRA_DATA_LENGTH(*xlrec) != 0) {
        extra_content = (char*)XLogRecGetData(record) + ReplicationSlotPersistentDataConstSize;
        Assert(strlen(extra_content) == (uint32)(GET_SLOT_EXTRA_DATA_LENGTH(*xlrec)));
    }
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
                    redo_slot_create(xlrec, extra_content);
                }
            } else {
                redo_slot_reset_for_backup(xlrec);
            }
            break;
        case XLOG_SLOT_ADVANCE:
            if (!ReplicationSlotFind(xlrec->name.data) && extra_content != NULL &&
                GET_SLOT_PERSISTENCY(*xlrec) != RS_BACKUP) {
                return;
            }
            if (ReplicationSlotFind(xlrec->name.data)) {
                redo_slot_advance(xlrec);
            }
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
        if (g_instance.attr.attr_storage.dcf_attr.enable_dcf) {
            SyncPaxosWaitForLSN(recptr);
        } else {
            SyncRepWaitForLSN(recptr);
        }
    }
}

bool is_archive_slot(ReplicationSlotPersistentData data)
{
    return GET_SLOT_EXTRA_DATA_LENGTH(data) != 0;
}

#ifndef ENABLE_LITE_MODE
XLogRecPtr create_physical_replication_slot_for_archive(const char* slot_name, bool is_dummy, char* extra_content,
    XLogRecPtr currFlushPtr)
{
    /* before upgrade commit, we can not use new slot extra field */
    if (t_thrd.proc->workingVersionNum < EXTRA_SLOT_VERSION_NUM) {
        ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH), errmsg("version must be bigger than 92260")));
    }

    check_permissions();

    /* create persistent replication slot with extra archive configuration */
    ReplicationSlotCreate(slot_name, RS_PERSISTENT, is_dummy, InvalidOid, currFlushPtr, InvalidXLogRecPtr,
                          extra_content);
    /* log slot creation */
    log_slot_create(&t_thrd.slot_cxt.MyReplicationSlot->data, t_thrd.slot_cxt.MyReplicationSlot->extra_content);
    MarkArchiveSlotOperate();
    add_archive_slot_to_instance(t_thrd.slot_cxt.MyReplicationSlot);

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

    /* we are about to start streaming switch over, stop any xlog insert. */
    if (t_thrd.xlog_cxt.LocalXLogInsertAllowed == 0 && g_instance.streaming_dr_cxt.isInSwitchover == true) {
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg("cannot create physical replication slot during streaming disaster recovery")));
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
    ReplicationSlotCreate(slot_name, RS_BACKUP, is_dummy, InvalidOid, InvalidXLogRecPtr, InvalidXLogRecPtr,
                          extra_content);
    /* log slot creation, in which quorum replication will be confirmed. */
    log_slot_create(&t_thrd.slot_cxt.MyReplicationSlot->data, NULL);
    ReplicationSlotRelease();
    slot_advance(slot_name, restart_lsn, database, end_lsn, true);

    ereport(LOG,
            (errmsg("advance restart lsn for initial backup slot, slotname: %s, restartlsn: %s", slot_name, end_lsn)));

    return restart_lsn;
}
#endif

void init_instance_slot()
{
    errno_t rc = EOK;
    MemoryContext curr;
    curr = MemoryContextSwitchTo(INSTANCE_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE));
    g_instance.archive_obs_cxt.archive_status = (ArchiveTaskStatus *)palloc0(
        sizeof(ArchiveTaskStatus) * g_instance.attr.attr_storage.max_replication_slots);
    rc = memset_s(g_instance.archive_obs_cxt.archive_status, 
        sizeof(ArchiveTaskStatus) * g_instance.attr.attr_storage.max_replication_slots, 0, 
        sizeof(ArchiveTaskStatus) * g_instance.attr.attr_storage.max_replication_slots);
    securec_check(rc, "", "");

    g_instance.archive_obs_cxt.archive_config = (ArchiveSlotConfig *)palloc0(
        sizeof(ArchiveSlotConfig) * g_instance.attr.attr_storage.max_replication_slots);

    rc = memset_s(g_instance.archive_obs_cxt.archive_config, 
        sizeof(ArchiveSlotConfig) * g_instance.attr.attr_storage.max_replication_slots, 0, 
        sizeof(ArchiveSlotConfig) * g_instance.attr.attr_storage.max_replication_slots);
    securec_check(rc, "", "");
    MemoryContextSwitchTo(curr);
}

void init_instance_slot_thread()
{
    errno_t rc = EOK;
    MemoryContext curr;
    curr = MemoryContextSwitchTo(INSTANCE_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE));

    g_instance.archive_thread_info.obsArchPID = (ThreadId*)palloc0(
        sizeof(ThreadId) * g_instance.attr.attr_storage.max_replication_slots);
    rc = memset_s(g_instance.archive_thread_info.obsArchPID, 
        sizeof(ThreadId) * g_instance.attr.attr_storage.max_replication_slots, 0, 
        sizeof(ThreadId) * g_instance.attr.attr_storage.max_replication_slots);
    securec_check(rc, "", "");
    
    g_instance.archive_thread_info.obsBarrierArchPID = (ThreadId*)palloc0(
        sizeof(ThreadId) * g_instance.attr.attr_storage.max_replication_slots);
    rc = memset_s(g_instance.archive_thread_info.obsBarrierArchPID, 
        sizeof(ThreadId) * g_instance.attr.attr_storage.max_replication_slots, 0, 
        sizeof(ThreadId) * g_instance.attr.attr_storage.max_replication_slots);
    securec_check(rc, "", "");

    g_instance.archive_thread_info.slotName = (char **)palloc0(sizeof(char *) * 
        g_instance.attr.attr_storage.max_replication_slots);

    for (int i = 0; i < g_instance.attr.attr_storage.max_replication_slots; i++) {
        g_instance.archive_thread_info.slotName[i] = (char *)palloc0(sizeof(char) * NAMEDATALEN);
        rc = memset_s(g_instance.archive_thread_info.slotName[i], sizeof(char) * NAMEDATALEN, 0, 
            sizeof(char) * NAMEDATALEN);
        securec_check(rc, "", "");
    }
    MemoryContextSwitchTo(curr);
}


void add_archive_slot_to_instance(ReplicationSlot *slot) 
{
    MemoryContext curr;
    errno_t rc = EOK;
    curr = MemoryContextSwitchTo(INSTANCE_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE));
    
    /* find a unused archive task status slot */
    for (int i = 0; i < g_instance.attr.attr_storage.max_replication_slots; i++) {
        ArchiveTaskStatus *archive_status = &g_instance.archive_obs_cxt.archive_status[i];
        SpinLockAcquire(&archive_status->mutex);
        if (archive_status->in_use == false) {
            rc = memset_s(archive_status, sizeof(ArchiveTaskStatus), 0, sizeof(ArchiveTaskStatus));
            securec_check(rc, "\0", "\0");
            archive_status->in_use = true;
            rc = memcpy_s(archive_status->slotname, NAMEDATALEN, slot->data.name.data, NAMEDATALEN);
            securec_check(rc, "\0", "\0");
            SpinLockRelease(&archive_status->mutex);
            break;
        }
        SpinLockRelease(&archive_status->mutex);
    }

    for (int i = 0; i < g_instance.attr.attr_storage.max_replication_slots; i++) {
        ArchiveSlotConfig *archive_config = &g_instance.archive_obs_cxt.archive_config[i];
        SpinLockAcquire(&archive_config->mutex);
        if (archive_config->in_use == false) {
            rc = memset_s(archive_config, sizeof(ArchiveTaskStatus), 0, sizeof(ArchiveTaskStatus));
            securec_check(rc, "\0", "\0");
            archive_config->in_use = true;
            rc = memcpy_s(archive_config->slotname, NAMEDATALEN, slot->data.name.data, NAMEDATALEN);
            securec_check(rc, "\0", "\0");
            archive_config->slot_tline = g_instance.archive_obs_cxt.slot_tline;
            if (slot->archive_config != NULL) {
                archive_config->archive_config.media_type = slot->archive_config->media_type;
                if (slot->archive_config->conn_config != NULL) {
                    archive_config->archive_config.conn_config =
                        (ArchiveConnConfig *)palloc0(sizeof(ArchiveConnConfig));
                    archive_config->archive_config.conn_config->obs_address =
                        pstrdup(slot->archive_config->conn_config->obs_address);
                    archive_config->archive_config.conn_config->obs_bucket =
                        pstrdup(slot->archive_config->conn_config->obs_bucket);
                    archive_config->archive_config.conn_config->obs_ak =
                        pstrdup(slot->archive_config->conn_config->obs_ak);
                    archive_config->archive_config.conn_config->obs_sk =
                        pstrdup(slot->archive_config->conn_config->obs_sk);
                }
                archive_config->archive_config.archive_prefix = pstrdup(slot->archive_config->archive_prefix);
                archive_config->archive_config.is_recovery = slot->archive_config->is_recovery;
            }
            SpinLockRelease(&archive_config->mutex);
            break;
        }
        SpinLockRelease(&archive_config->mutex);
    }
    MemoryContextSwitchTo(curr);
}

void remove_archive_slot_from_instance_list(const char *slot_name)
{
    /* find a unused archive task status slot */
    for (int i = 0; i < g_instance.attr.attr_storage.max_replication_slots; i++) {
        ArchiveTaskStatus *archive_status = &g_instance.archive_obs_cxt.archive_status[i];
        SpinLockAcquire(&archive_status->mutex);
        if (archive_status->in_use == true && strcmp(archive_status->slotname, slot_name) == 0) {
            archive_status->in_use = false;
            SpinLockRelease(&archive_status->mutex);
            break;
        }
        SpinLockRelease(&archive_status->mutex);
    }

    for (int i = 0; i < g_instance.attr.attr_storage.max_replication_slots; i++) {
        ArchiveSlotConfig *archive_config = &g_instance.archive_obs_cxt.archive_config[i];
        SpinLockAcquire(&archive_config->mutex);
        if (archive_config->in_use == true && strcmp(archive_config->slotname, slot_name) == 0) {
            archive_config->slot_tline = 0;
            if (archive_config->archive_config.conn_config != NULL) {
                pfree_ext(archive_config->archive_config.conn_config->obs_address);
                pfree_ext(archive_config->archive_config.conn_config->obs_bucket);
                pfree_ext(archive_config->archive_config.conn_config->obs_ak);
                pfree_ext(archive_config->archive_config.conn_config->obs_sk);
                pfree_ext(archive_config->archive_config.conn_config);
            }
            pfree_ext(archive_config->archive_config.archive_prefix);

            archive_config->in_use = false;
            SpinLockRelease(&archive_config->mutex);
            break;
        }
        SpinLockRelease(&archive_config->mutex);
    }
}

ArchiveSlotConfig* find_archive_slot_from_instance_list(const char* name)
{
    ArchiveSlotConfig* result = NULL;
    for (int i = 0; i < g_instance.attr.attr_storage.max_replication_slots; i++) {
        ArchiveSlotConfig *archive_config = &g_instance.archive_obs_cxt.archive_config[i];
        SpinLockAcquire(&archive_config->mutex);
        if (archive_config->in_use == true && strcmp(archive_config->slotname, name) == 0) {
            result = archive_config;
            SpinLockRelease(&archive_config->mutex);
            break;
        }
        SpinLockRelease(&archive_config->mutex);
    }
    return result;
}

void release_archive_slot(ArchiveSlotConfig** archive_conf)
{
    if (archive_conf == NULL || (*archive_conf) == NULL) {
        return;
    }
    if ((*archive_conf)->archive_config.conn_config != NULL) {
        pfree_ext((*archive_conf)->archive_config.conn_config->obs_address);
        pfree_ext((*archive_conf)->archive_config.conn_config->obs_bucket);
        pfree_ext((*archive_conf)->archive_config.conn_config->obs_ak);
        pfree_ext((*archive_conf)->archive_config.conn_config->obs_sk);
        pfree_ext((*archive_conf)->archive_config.conn_config);
    }
    pfree_ext((*archive_conf)->archive_config.archive_prefix);
    pfree_ext((*archive_conf));
}

ArchiveSlotConfig* copy_archive_slot(ArchiveSlotConfig* archive_conf_origin)
{
    if (archive_conf_origin == NULL) {
        return NULL;
    }

    ArchiveSlotConfig* archive_conf_copy = (ArchiveSlotConfig*)palloc0(sizeof(ArchiveSlotConfig));
    int rc = memcpy_s(archive_conf_copy->slotname, NAMEDATALEN, archive_conf_origin->slotname, NAMEDATALEN);
    securec_check(rc, "\0", "\0");
    archive_conf_copy->archive_config.media_type = archive_conf_origin->archive_config.media_type;
    if (archive_conf_origin->archive_config.conn_config != NULL) {
        archive_conf_copy->archive_config.conn_config = (ArchiveConnConfig *)palloc0(sizeof(ArchiveConnConfig));
        rc = memcpy_s(archive_conf_copy->archive_config.conn_config, sizeof(ArchiveConnConfig),
                        archive_conf_origin->archive_config.conn_config, sizeof(ArchiveConnConfig));
        securec_check(rc, "\0", "\0");
    }
    archive_conf_copy->archive_config.archive_prefix = pstrdup(archive_conf_origin->archive_config.archive_prefix);
    return archive_conf_copy;
}

ArchiveTaskStatus* find_archive_task_status(const char* name)
{
    if (g_instance.archive_obs_cxt.archive_slot_num == 0) {
        return NULL;
    }
    for (int i = 0; i < g_instance.attr.attr_storage.max_replication_slots; i++) {
        ArchiveTaskStatus *archive_status = &g_instance.archive_obs_cxt.archive_status[i];
        if (archive_status->in_use && strcmp(archive_status->slotname, name) == 0) {
            return archive_status;
        }
    }
    return NULL;
}

ArchiveTaskStatus* find_archive_task_status(int *idx)
{
    Assert(t_thrd.role == ARCH);
    if (g_instance.archive_obs_cxt.archive_slot_num == 0) {
        return NULL;
    }

    ArchiveTaskStatus *archive_status = &g_instance.archive_obs_cxt.archive_status[*idx];
    SpinLockAcquire(&archive_status->mutex);
    if (archive_status->in_use && strcmp(archive_status->slotname, t_thrd.arch.slot_name) == 0) {
        SpinLockRelease(&archive_status->mutex);
        return archive_status;
    }
    SpinLockRelease(&archive_status->mutex);

    for (int i = 0; i < g_instance.attr.attr_storage.max_replication_slots; i++) {
        ArchiveTaskStatus *archive_status = &g_instance.archive_obs_cxt.archive_status[i];
        SpinLockAcquire(&archive_status->mutex);
        if (archive_status->in_use == true && strcmp(archive_status->slotname, t_thrd.arch.slot_name) == 0) {
            SpinLockRelease(&archive_status->mutex);
            *idx = i;
            return archive_status;
        }
        SpinLockRelease(&archive_status->mutex);
    }
    ereport(ERROR, (errmsg("can not find archive task, may be slot is remove")));
    return NULL;
}

ArchiveTaskStatus* walreceiver_find_archive_task_status(unsigned int expected_pitr_task_status)
{
    if (g_instance.archive_obs_cxt.archive_slot_num == 0) {
        return NULL;
    }

    for (int i = 0; i < g_instance.attr.attr_storage.max_replication_slots; i++) {
        ArchiveTaskStatus *archive_status = &g_instance.archive_obs_cxt.archive_status[i];
        volatile unsigned int *pitr_task_status = &archive_status->pitr_task_status;
        if (archive_status->in_use && *pitr_task_status == expected_pitr_task_status) {
            return archive_status;
        }
    }
    return NULL;
}

Datum gs_get_parallel_decode_status(PG_FUNCTION_ARGS)
{
    FuncCallContext* funcctx = NULL;
    MemoryContext oldcontext = NULL;
    ParallelStatusData *entry = NULL;
    const int columnNum = 7;

    if (SRF_IS_FIRSTCALL()) {
        funcctx = SRF_FIRSTCALL_INIT();
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
        TupleDesc tupdesc = CreateTemplateTupleDesc(columnNum, false);

        TupleDescInitEntry(tupdesc, (AttrNumber)ARR_1, "slot_name", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)ARR_2, "parallel_decode_num", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)ARR_3, "read_change_queue_length", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)ARR_4, "decode_change_queue_length", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)ARR_5, "reader_lsn", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)ARR_6, "working_txn_cnt", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)ARR_7, "working_txn_memory", INT8OID, -1, 0);
        funcctx->tuple_desc = BlessTupleDesc(tupdesc);
        funcctx->user_fctx = (void *)GetParallelDecodeStatus(&(funcctx->max_calls));
        (void)MemoryContextSwitchTo(oldcontext);
    }

    funcctx = SRF_PERCALL_SETUP();
    entry = (ParallelStatusData *)funcctx->user_fctx;
    if (funcctx->call_cntr < funcctx->max_calls) {
        Datum values[columnNum];
        bool nulls[columnNum] = {false};
        HeapTuple tuple = NULL;
        errno_t rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");
        rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "\0", "\0");

        entry += funcctx->call_cntr;
        values[ARG_0] = CStringGetTextDatum(entry->slotName);
        values[ARG_1] = Int32GetDatum(entry->parallelDecodeNum);
        values[ARG_2] = CStringGetTextDatum(entry->readQueueLen);
        values[ARG_3] = CStringGetTextDatum(entry->decodeQueueLen);
        values[ARG_4] = CStringGetTextDatum(entry->readerLsn);
        values[ARG_5] = Int64GetDatum(entry->workingTxnCnt);
        values[ARG_6] = Int64GetDatum(entry->workingTxnMemory);
        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);

        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    }
    SRF_RETURN_DONE(funcctx);
}

