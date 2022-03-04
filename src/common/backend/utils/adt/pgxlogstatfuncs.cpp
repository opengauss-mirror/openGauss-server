/* -------------------------------------------------------------------------
 *
 * pgstatfuncs.c
 * 	  Functions for accessing the statistics collector data
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 * 	  src/backend/utils/adt/pgxlogstatfuncs.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"
#include <time.h>

#include "access/transam.h"
#include "access/tableam.h"
#include "access/redo_statistic.h"
#include "access/xlog.h"
#include "connector.h"
#include "catalog/namespace.h"
#include "catalog/pg_database.h"
#include "catalog/pg_tablespace.h"
#include "catalog/pg_type.h"
#include "catalog/pg_partition_fn.h"
#include "catalog/pg_namespace.h"
#include "commands/dbcommands.h"
#include "commands/user.h"
#include "commands/vacuum.h"
#include "funcapi.h"
#include "gaussdb_version.h"
#include "libpq/ip.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/globalplancache.h"
#include "utils/inet.h"
#include "utils/timestamp.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/memprot.h"
#include "utils/typcache.h"
#include "utils/syscache.h"
#include "pgxc/pgxc.h"
#include "pgxc/nodemgr.h"
#include "postmaster/autovacuum.h"
#include "postmaster/postmaster.h"
#include "storage/lock/lwlock.h"
#include "postgres.h"
#include "knl/knl_variable.h"
#include "storage/smgr/segment.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "storage/buf/buf_internals.h"
#include "workload/cpwlm.h"
#include "workload/workload.h"
#include "pgxc/pgxcnode.h"
#include "access/hash.h"
#include "libcomm/libcomm.h"
#include "pgxc/poolmgr.h"
#include "pgxc/execRemote.h"
#include "utils/elog.h"
#include "utils/memtrace.h"
#include "commands/user.h"
#include "instruments/gs_stat.h"
#include "instruments/list.h"
#include "replication/rto_statistic.h"
#include "storage/lock/lock.h"

const int STAT_XLOG_TBLENTRY_COLS = 4;
const int STAT_XLOG_TEXT_BUFFER_SIZE = 1024;
const int STAT_XLOG_FLUSH_LOCATION = 12;
const int STAT_XLOG_FLUSH_STAT = 16;
const int STAT_XLOG_FLUSH_STAT_OFF = -1;
const int STAT_XLOG_FLUSH_STAT_ON = 0;
const int STAT_XLOG_FLUSH_STAT_GET = 1;
const int STAT_XLOG_FLUSH_STAT_CLEAR = 2;

static void ReadAllWalInsertStatusTable(int64 walInsertStatusEntryCount, TupleDesc *tupleDesc,
    Tuplestorestate *tupstore)
{
    Assert(tupleDesc != NULL);
    Assert(tupstore != NULL);

    volatile WALInsertStatusEntry *entry_ptr = NULL;
    for (int64 idx = 0; idx < walInsertStatusEntryCount; idx++) {
        entry_ptr = &g_instance.wal_cxt.walInsertStatusTable[GET_STATUS_ENTRY_INDEX(idx)];

        bool nulls[STAT_XLOG_TBLENTRY_COLS] = {false};
        Datum values[STAT_XLOG_TBLENTRY_COLS];

        values[ARR_0] = UInt64GetDatum(idx);
        values[ARR_1] = UInt64GetDatum(entry_ptr->endLSN);
        values[ARR_2] = Int32GetDatum(entry_ptr->LRC);
        values[ARR_3] = UInt32GetDatum(entry_ptr->status);
        tuplestore_putvalues(tupstore, *tupleDesc, values, nulls);
    }

    tuplestore_donestoring(tupstore);
}

static void ReadWalInsertStatusTableByTldx(int64 idx, TupleDesc *tupleDesc, Tuplestorestate *tupstore)
{
    Assert(tupleDesc != NULL);
    Assert(tupstore != NULL);

    volatile WALInsertStatusEntry *entry_ptr = NULL;
    entry_ptr = &g_instance.wal_cxt.walInsertStatusTable[GET_STATUS_ENTRY_INDEX(idx)];
    bool nulls[STAT_XLOG_TBLENTRY_COLS] = {false};
    Datum values[STAT_XLOG_TBLENTRY_COLS];
    values[ARR_0] = UInt64GetDatum(idx);
    values[ARR_1] = UInt64GetDatum(entry_ptr->endLSN);
    values[ARR_2] = Int32GetDatum(entry_ptr->LRC);
    values[ARR_3] = UInt32GetDatum(entry_ptr->status);
    tuplestore_putvalues(tupstore, *tupleDesc, values, nulls);

    tuplestore_donestoring(tupstore);
}

Datum gs_stat_wal_entrytable(PG_FUNCTION_ARGS)
{
    int64 idx = PG_GETARG_INT64(0); // -1: all walInsertStatus; n:walInsertStatusTable[n]

    int64 walInsertStatusEntryCount =
        GET_WAL_INSERT_STATUS_ENTRY_CNT(g_instance.attr.attr_storage.wal_insert_status_entries_power);
    ReturnSetInfo *rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;
    TupleDesc tupDesc;
    Tuplestorestate *tupstore = NULL;
    MemoryContext per_query_ctx;
    MemoryContext oldcontext;

    if (idx >= walInsertStatusEntryCount || idx < -1) {
        elog(ERROR, "The idx out of range.");
        PG_RETURN_VOID();
    }

    if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo)) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("set-valued function called in context that cannot accept a set")));
        PG_RETURN_VOID();
    }
    if (!(rsinfo->allowedModes & SFRM_Materialize)) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("materialize mode required, but it is not allowed in this context")));
        PG_RETURN_VOID();
    }

    if (get_call_result_type(fcinfo, NULL, &tupDesc) != TYPEFUNC_COMPOSITE) {
        elog(ERROR, "return type must be a row type");
        PG_RETURN_VOID();
    }

    per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
    oldcontext = MemoryContextSwitchTo(per_query_ctx);
    tupstore = tuplestore_begin_heap(true, false, u_sess->attr.attr_memory.work_mem);
    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult = tupstore;
    rsinfo->setDesc = tupDesc;
    MemoryContextSwitchTo(oldcontext);

    if (idx == -1) {
        ReadAllWalInsertStatusTable(walInsertStatusEntryCount, &tupDesc, tupstore);
    } else {
        ReadWalInsertStatusTableByTldx(idx, &tupDesc, tupstore);
    }

    PG_RETURN_VOID();
}

static void ReadFlushLocation(TupleDesc *tupleDesc, Tuplestorestate *tupstore)
{
    Assert(tupleDesc != NULL);
    Assert(tupstore != NULL);

    bool nulls[STAT_XLOG_FLUSH_LOCATION] = {false};
    Datum values[STAT_XLOG_FLUSH_LOCATION];

    volatile XLogCtlInsert *Insert = &t_thrd.shemem_ptr_cxt.XLogCtl->Insert;
    volatile XLogwrtRqst *LogwrtRqst = &t_thrd.shemem_ptr_cxt.XLogCtl->LogwrtRqst;
    volatile XLogwrtResult *LogwrtResult = &t_thrd.shemem_ptr_cxt.XLogCtl->LogwrtResult;

    values[ARR_0] = Int32GetDatum(g_instance.wal_cxt.lastWalStatusEntryFlushed);
    values[ARR_1] = Int32GetDatum(g_instance.wal_cxt.lastLRCScanned);
    values[ARR_2] = Int32GetDatum(Insert->CurrLRC);
    values[ARR_3] = UInt64GetDatum(Insert->CurrBytePos);
    values[ARR_4] = UInt32GetDatum(Insert->PrevByteSize);
    values[ARR_5] = UInt64GetDatum(g_instance.wal_cxt.flushResult);
    values[ARR_6] = UInt64GetDatum(g_instance.wal_cxt.sentResult);
    values[ARR_7] = UInt64GetDatum(LogwrtRqst->Write);
    values[ARR_8] = UInt64GetDatum(LogwrtRqst->Flush);
    values[ARR_9] = UInt64GetDatum(LogwrtResult->Write);
    values[ARR_10] = UInt64GetDatum(LogwrtResult->Flush);
    values[ARR_11] = TimestampTzGetDatum(GetCurrentTimestamp());

    tuplestore_putvalues(tupstore, *tupleDesc, values, nulls);
    tuplestore_donestoring(tupstore);
}


static void GetWalwriterFlushStat(TupleDesc *tupleDesc, Tuplestorestate *tupstore)
{
    Assert(tupleDesc != NULL);
    Assert(tupstore != NULL);

    bool nulls[STAT_XLOG_FLUSH_STAT] = {false};
    Datum values[STAT_XLOG_FLUSH_STAT];

    if (g_instance.wal_cxt.xlogFlushStats->writeTimes != 0) {
        g_instance.wal_cxt.xlogFlushStats->avgActualWriteBytes =
            g_instance.wal_cxt.xlogFlushStats->totalActualXlogSyncBytes / g_instance.wal_cxt.xlogFlushStats->writeTimes;
        g_instance.wal_cxt.xlogFlushStats->avgWriteTime =
            g_instance.wal_cxt.xlogFlushStats->totalWriteTime / g_instance.wal_cxt.xlogFlushStats->writeTimes;
        g_instance.wal_cxt.xlogFlushStats->avgWriteBytes =
            g_instance.wal_cxt.xlogFlushStats->totalXlogSyncBytes / g_instance.wal_cxt.xlogFlushStats->writeTimes;
    }

    if (g_instance.wal_cxt.xlogFlushStats->syncTimes != 0) {
        g_instance.wal_cxt.xlogFlushStats->avgSyncTime =
            g_instance.wal_cxt.xlogFlushStats->totalSyncTime / g_instance.wal_cxt.xlogFlushStats->syncTimes;
        g_instance.wal_cxt.xlogFlushStats->avgSyncBytes =
            g_instance.wal_cxt.xlogFlushStats->totalXlogSyncBytes / g_instance.wal_cxt.xlogFlushStats->syncTimes;
        g_instance.wal_cxt.xlogFlushStats->avgActualSyncBytes =
            g_instance.wal_cxt.xlogFlushStats->totalActualXlogSyncBytes / g_instance.wal_cxt.xlogFlushStats->syncTimes;
    }

    values[ARR_0] = UInt64GetDatum(g_instance.wal_cxt.xlogFlushStats->writeTimes);
    values[ARR_1] = UInt64GetDatum(g_instance.wal_cxt.xlogFlushStats->syncTimes);
    values[ARR_2] = UInt64GetDatum(g_instance.wal_cxt.xlogFlushStats->totalXlogSyncBytes);
    values[ARR_3] = UInt64GetDatum(g_instance.wal_cxt.xlogFlushStats->totalActualXlogSyncBytes);
    values[ARR_4] = UInt32GetDatum(g_instance.wal_cxt.xlogFlushStats->avgWriteBytes);
    values[ARR_5] = UInt32GetDatum(g_instance.wal_cxt.xlogFlushStats->avgActualWriteBytes);
    values[ARR_6] = UInt32GetDatum(g_instance.wal_cxt.xlogFlushStats->avgSyncBytes);
    values[ARR_7] = UInt32GetDatum(g_instance.wal_cxt.xlogFlushStats->avgActualSyncBytes);
    values[ARR_8] = UInt64GetDatum(g_instance.wal_cxt.xlogFlushStats->totalWriteTime);
    values[ARR_9] = UInt64GetDatum(g_instance.wal_cxt.xlogFlushStats->totalSyncTime);
    values[ARR_10] = UInt64GetDatum(g_instance.wal_cxt.xlogFlushStats->avgWriteTime);
    values[ARR_11] = UInt64GetDatum(g_instance.wal_cxt.xlogFlushStats->avgSyncTime);
    values[ARR_12] = UInt64GetDatum(GetNewestXLOGSegNo(t_thrd.proc_cxt.DataDir));
    values[ARR_13] = UInt64GetDatum(g_instance.wal_cxt.xlogFlushStats->currOpenXlogSegNo);
    values[ARR_14] = TimestampTzGetDatum(g_instance.wal_cxt.xlogFlushStats->lastRestTime);
    values[ARR_15] = TimestampTzGetDatum(GetCurrentTimestamp());
    tuplestore_putvalues(tupstore, *tupleDesc, values, nulls);
    tuplestore_donestoring(tupstore);
}


static void ClearWalwriterFlushStat(TupleDesc *tupleDesc, Tuplestorestate *tupstore)
{
    Assert(tupleDesc != NULL);
    Assert(tupstore != NULL);

    g_instance.wal_cxt.xlogFlushStats->writeTimes = 0;
    g_instance.wal_cxt.xlogFlushStats->syncTimes = 0;
    g_instance.wal_cxt.xlogFlushStats->totalXlogSyncBytes = 0;
    g_instance.wal_cxt.xlogFlushStats->totalActualXlogSyncBytes = 0;
    g_instance.wal_cxt.xlogFlushStats->avgWriteBytes = 0;
    g_instance.wal_cxt.xlogFlushStats->avgActualWriteBytes = 0;
    g_instance.wal_cxt.xlogFlushStats->avgSyncBytes = 0;
    g_instance.wal_cxt.xlogFlushStats->avgActualSyncBytes = 0;
    g_instance.wal_cxt.xlogFlushStats->totalWriteTime = 0;
    g_instance.wal_cxt.xlogFlushStats->totalSyncTime = 0;
    g_instance.wal_cxt.xlogFlushStats->avgWriteTime = 0;
    g_instance.wal_cxt.xlogFlushStats->avgSyncTime = 0;
    g_instance.wal_cxt.xlogFlushStats->currOpenXlogSegNo = 0;
    g_instance.wal_cxt.xlogFlushStats->lastRestTime = GetCurrentTimestamp();

    GetWalwriterFlushStat(tupleDesc, tupstore);
}

Datum gs_walwriter_flush_position(PG_FUNCTION_ARGS)
{
    ReturnSetInfo *rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;
    TupleDesc tupDesc;
    Tuplestorestate *tupstore = NULL;
    MemoryContext per_query_ctx;
    MemoryContext oldcontext;

    if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo)) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("set-valued function called in context that cannot accept a set")));
        PG_RETURN_VOID();
    }
    if (!(rsinfo->allowedModes & SFRM_Materialize)) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("materialize mode required, but it is not allowed in this context")));
        PG_RETURN_VOID();
    }

    if (get_call_result_type(fcinfo, NULL, &tupDesc) != TYPEFUNC_COMPOSITE) {
        elog(ERROR, "return type must be a row type");
        PG_RETURN_VOID();
    }

    per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
    oldcontext = MemoryContextSwitchTo(per_query_ctx);
    tupstore = tuplestore_begin_heap(true, false, u_sess->attr.attr_memory.work_mem);
    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult = tupstore;
    rsinfo->setDesc = tupDesc;
    MemoryContextSwitchTo(oldcontext);

    ReadFlushLocation(&tupDesc, tupstore);

    PG_RETURN_VOID();
}

Datum gs_walwriter_flush_stat(PG_FUNCTION_ARGS)
{
    int operation = PG_GETARG_INT32(0);

    ReturnSetInfo *rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;
    TupleDesc tupDesc;
    Tuplestorestate *tupstore = NULL;
    MemoryContext per_query_ctx;
    MemoryContext oldcontext;

    if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo)) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("set-valued function called in context that cannot accept a set")));
        PG_RETURN_VOID();
    }
    if (!(rsinfo->allowedModes & SFRM_Materialize)) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("materialize mode required, but it is not allowed in this context")));
        PG_RETURN_VOID();
    }

    if (get_call_result_type(fcinfo, NULL, &tupDesc) != TYPEFUNC_COMPOSITE) {
        elog(ERROR, "return type must be a row type");
        PG_RETURN_VOID();
    }

    per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
    oldcontext = MemoryContextSwitchTo(per_query_ctx);
    tupstore = tuplestore_begin_heap(true, false, u_sess->attr.attr_memory.work_mem);
    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult = tupstore;
    rsinfo->setDesc = tupDesc;
    MemoryContextSwitchTo(oldcontext);

    if (operation == STAT_XLOG_FLUSH_STAT_OFF) {
        g_instance.wal_cxt.xlogFlushStats->statSwitch = false;
        elog(INFO, "The xlogFlushStats switch is turned off.");
        GetWalwriterFlushStat(&tupDesc, tupstore);
    } else if (operation == STAT_XLOG_FLUSH_STAT_ON) {
        g_instance.wal_cxt.xlogFlushStats->statSwitch = true;
        elog(INFO, "The xlogFlushStats switch is turned on.");
        GetWalwriterFlushStat(&tupDesc, tupstore);
    } else if (operation == STAT_XLOG_FLUSH_STAT_GET) {
        GetWalwriterFlushStat(&tupDesc, tupstore);
    } else if (operation == STAT_XLOG_FLUSH_STAT_CLEAR) {
        ClearWalwriterFlushStat(&tupDesc, tupstore);
    } else {
        elog(ERROR, "Parameter \"operation\" out of range.");
    }

    PG_RETURN_VOID();
}