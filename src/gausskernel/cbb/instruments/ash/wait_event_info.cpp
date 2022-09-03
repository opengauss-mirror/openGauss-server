/*
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
 * -------------------------------------------------------------------------
 *
 * wait_event_info.cpp
 *   functions for wait_event_info
 *
 * IDENTIFICATION
 *    src/gausskernel/cbb/instruments/ash/wait_event_info.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "utils/builtins.h"
#include "funcapi.h"

typedef struct EventInfo {
    char* module;
    char* type;
    char* event;
} EventInfo;
#define WAIT_EVENT_SIZE 257
struct EventInfo waitEventInfo[WAIT_EVENT_SIZE] = {
    {"none", "STATUS", "none"},
    {"LWLock", "STATUS", "acquire lwlock"},
    {"Lock", "STATUS", "acquire lock"},
    {"I/O", "STATUS", "wait io"},
    {"COMM", "STATUS", "wait cmd"},
    {"COMM", "STATUS", "wait pooler get conn"},
    {"COMM", "STATUS", "wait pooler abort conn"},
    {"COMM", "STATUS", "wait pooler clean conn"},
    {"COMM", "STATUS", "pooler create conn"},
    {"COMM", "STATUS", "get conn"},
    {"COMM", "STATUS", "set cmd"},
    {"COMM", "STATUS", "reset cmd"},
    {"COMM", "STATUS", "cancel query"},
    {"COMM", "STATUS", "stop query"},
    {"COMM", "STATUS", "wait node"},
    {"Transaction", "STATUS", "wait transaction sync"},
    {"Transaction", "STATUS", "wait wal sync"},
    {"Transaction", "STATUS", "wait data sync"},
    {"Transaction", "STATUS", "wait data sync queue"},
    {"COMM", "STATUS", "flush data"},
    {"Transaction", "STATUS", "wait reserve td"},
    {"Transaction", "STATUS", "wait td rollback"},
    {"Transaction", "STATUS", "wait available td"},
    {"Transaction", "STATUS", "wait transaction rollback"},
    {"Data file", "STATUS", "prune table"},
    {"Data file", "STATUS", "prune index"},
    {"COMM", "STATUS", "stream get conn"},
    {"COMM", "STATUS", "wait producer ready"},
    {"Stream", "STATUS", "synchronize quit"},
    {"Stream", "STATUS", "wait stream group destroy"},
    {"WLM", "STATUS", "wait active statement"},
    {"WLM", "STATUS", "wait memory"},
    {"Executor", "STATUS", "Sort"},
    {"Executor", "STATUS", "Sort - write file"},
    {"Executor", "STATUS", "Material"},
    {"Executor", "STATUS", "Material - write file"},
    {"Executor", "STATUS", "HashJoin - build hash"},
    {"Executor", "STATUS", "HashJoin - write file"},
    {"Executor", "STATUS", "HashAgg - build hash"},
    {"Executor", "STATUS", "HashAgg - write file"},
    {"Executor", "STATUS", "HashSetop - build hash"},
    {"Executor", "STATUS", "HashSetop - write file"},
    {"Executor", "STATUS", "NestLoop"},
    {"DDL/DCL", "STATUS", "create index"},
    {"DDL/DCL", "STATUS", "analyze"},
    {"DDL/DCL", "STATUS", "vacuum"},
    {"DDL/DCL", "STATUS", "vacuum full"},
    {"GTM", "STATUS", "gtm connect"},
    {"GTM", "STATUS", "gtm reset xmin"},
    {"GTM", "STATUS", "gtm get xmin"},
    {"GTM", "STATUS", "gtm get gxid"},
    {"GTM", "STATUS", "gtm get csn"},
    {"GTM", "STATUS", "gtm get snapshot"},
    {"GTM", "STATUS", "gtm begin trans"},
    {"GTM", "STATUS", "gtm commit trans"},
    {"GTM", "STATUS", "gtm rollback trans"},
    {"GTM", "STATUS", "gtm start preprare trans"},
    {"GTM", "STATUS", "gtm prepare trans"},
    {"GTM", "STATUS", "gtm open sequence"},
    {"GTM", "STATUS", "gtm close sequence"},
    {"GTM", "STATUS", "gtm create sequence"},
    {"GTM", "STATUS", "gtm alter sequence"},
    {"GTM", "STATUS", "gtm get sequence val"},
    {"GTM", "STATUS", "gtm set sequence val"},
    {"GTM", "STATUS", "gtm drop sequence"},
    {"GTM", "STATUS", "gtm rename sequence"},
    {"Executor", "STATUS", "wait sync consumer next step"},
    {"Executor", "STATUS", "wait sync producer next step"},
    /* IO_EVENT */
    {"Temp File", "IO_EVENT", "BufFileRead"},
    {"Pg_control", "IO_EVENT", "BufFileWrite"},
    {"Pg_control", "IO_EVENT", "ControlFileRead"},
    {"Pg_control", "IO_EVENT", "ControlFileSync"},
    {"Pg_control", "IO_EVENT", "ControlFileSyncUpdate"},
    {"Pg_control", "IO_EVENT", "ControlFileWrite"},
    {"Pg_control", "IO_EVENT", "ControlFileWriteUpdate"},
    {"File", "IO_EVENT", "CopyFileRead"},
    {"File", "IO_EVENT", "CopyFileWrite"},
    {"File", "IO_EVENT", "DataFileExtend"},
    {"Data file", "IO_EVENT", "DataFileImmediateSync"},
    {"Data file", "IO_EVENT", "DataFilePrefetch"},
    {"Data file", "IO_EVENT", "DataFileRead"},
    {"Data file", "IO_EVENT", "DataFileSync"},
    {"Data file", "IO_EVENT", "DataFileTruncate"},
    {"Data file", "IO_EVENT", "DataFileWrite"},
    {"postmaster.pid", "IO_EVENT", "LockFileAddToDataDirRead"},
    {"postmaster.pid", "IO_EVENT", "LockFileAddToDataDirSync"},
    {"postmaster.pid", "IO_EVENT", "LockFileAddToDataDirWrite"},
    {"Pid File", "IO_EVENT", "LockFileCreateRead"},
    {"Pid File", "IO_EVENT", "LockFileCreateSync"},
    {"Pid File", "IO_EVENT", "LockFileCreateWRITE"},
    {"System table mapping file", "IO_EVENT", "RelationMapRead"},
    {"System table mapping file", "IO_EVENT", "RelationMapSync"},
    {"System table mapping file", "IO_EVENT", "RelationMapWrite"},
    {"Streaming replication", "IO_EVENT", "ReplicationSlotRead"},
    {"Streaming replication", "IO_EVENT", "ReplicationSlotRestoreSync"},
    {"Streaming replication", "IO_EVENT", "ReplicationSlotSync"},
    {"Streaming replication", "IO_EVENT", "ReplicationSlotWrite"},
    {"Clog", "IO_EVENT", "SLRUFlushSync"},
    {"Clog", "IO_EVENT", "SLRURead"},
    {"Clog", "IO_EVENT", "SLRUSync"},
    {"Clog", "IO_EVENT", "SLRUWrite"},
    {"Timelinehistory", "IO_EVENT", "TimelineHistoryRead"},
    {"Timelinehistory", "IO_EVENT", "TimelineHistorySync"},
    {"Timelinehistory", "IO_EVENT", "TimelineHistoryWrite"},
    {"pg_twophase", "IO_EVENT", "TwophaseFileRead"},
    {"pg_twophase", "IO_EVENT", "TwophaseFileSync"},
    {"pg_twophase", "IO_EVENT", "TwophaseFileWrite"},
    {"WAL", "IO_EVENT", "WALBootstrapSync"},
    {"WAL", "IO_EVENT", "WALBootstrapWrite"},
    {"WAL", "IO_EVENT", "WALCopyRead"},
    {"WAL", "IO_EVENT", "WALCopySync"},
    {"WAL", "IO_EVENT", "WALCopyWrite"},
    {"WAL", "IO_EVENT", "WALInitSync"},
    {"WAL", "IO_EVENT", "WALInitWrite"},
    {"WAL", "IO_EVENT", "WALRead"},
    {"WAL", "IO_EVENT", "WALSyncMethodAssign"},
    {"WAL", "IO_EVENT", "WALWrite"},
    {"Double write", "IO_EVENT", "DoubleWriteFileRead"},
    {"Double write", "IO_EVENT", "DoubleWriteFileSync"},
    {"Double write", "IO_EVENT", "DoubleWriteFileWrite"},
    {"replication", "IO_EVENT", "PredoProcessPending"},
    {"replication", "IO_EVENT", "PredoApply"},
    {"replication", "IO_EVENT", "DisableConnectFileRead"},
    {"replication", "IO_EVENT", "DisableConnectFileSync"},
    {"replication", "IO_EVENT", "DisableConnectFileWrite"},
    /* LOCK_EVENT */
    {"Relation", "LOCK_EVENT", "relation"},
    {"Relation", "LOCK_EVENT", "extend"},
    {"Relation", "LOCK_EVENT", "partition"},
    {"Relation", "LOCK_EVENT", "partition_seq"},
    {"Page", "LOCK_EVENT", "page"},
    {"Tuple", "LOCK_EVENT", "tuple"},
    {"Transaction", "LOCK_EVENT", "transactionid"},
    {"Transaction", "LOCK_EVENT", "virtualxid"},
    {"Object", "LOCK_EVENT", "object"},
    {"Column store", "LOCK_EVENT", "cstore_freespace"},
    {"User", "LOCK_EVENT", "userlock"},
    {"Advisor", "LOCK_EVENT", "advisory"},
    /* LWLOCK_EVENT */ 
    {"SharedMemory", "LWLOCK_EVENT", "BufFreelistLock"},
    {"Cache", "LWLOCK_EVENT", "CUSlotListLock"},
    {"SharedMemory", "LWLOCK_EVENT", "ShmemIndexLock"},
    {"Transaction", "LWLOCK_EVENT", "OidGenLock"},
    {"Transaction", "LWLOCK_EVENT", "XidGenLock"},
    {"Transaction", "LWLOCK_EVENT", "ProcArrayLock"},
    {"Relation", "LWLOCK_EVENT", "SInvalReadLock"},
    {"Relation", "LWLOCK_EVENT", "SInvalWriteLock"},
    {"WAL", "LWLOCK_EVENT", "WALBufMappingLock"},
    {"WAL", "LWLOCK_EVENT", "WALWriteLock"},
    {"Pg_control", "LWLOCK_EVENT", "ControlFileLock"},
    {"Checkpoint", "LWLOCK_EVENT", "CheckpointLock"},
    {"Clog", "LWLOCK_EVENT", "CLogControlLock"},
    {"Transaction", "LWLOCK_EVENT", "SubtransControlLock"},
    {"Transaction", "LWLOCK_EVENT", "MultiXactGenLock"},
    {"Clog", "LWLOCK_EVENT", "MultiXactOffsetControlLock"},
    {"Clog", "LWLOCK_EVENT", "MultiXactMemberControlLock"},
    {"Relation", "LWLOCK_EVENT", "RelCacheInitLock"},
    {"Checkpoint", "LWLOCK_EVENT", "CheckpointerCommLock"},
    {"Transaction", "LWLOCK_EVENT", "TwoPhaseStateLock"},
    {"Relation", "LWLOCK_EVENT", "TablespaceCreateLock"},
    {"Vaccum", "LWLOCK_EVENT", "BtreeVacuumLock"},
    {"SharedMemory", "LWLOCK_EVENT", "AddinShmemInitLock"},
    {"Vaccum", "LWLOCK_EVENT", "AutovacuumLock"},
    {"Vaccum", "LWLOCK_EVENT", "AutovacuumScheduleLock"},
    {"Analyze", "LWLOCK_EVENT", "AutoanalyzeLock"},
    {"Data file", "LWLOCK_EVENT", "SyncScanLock"},
    {"Barrier", "LWLOCK_EVENT", "BarrierLock"},
    {"Cluster", "LWLOCK_EVENT", "NodeTableLock"},
    {"Concurrency", "LWLOCK_EVENT", "PoolerLock"},
    {"COMM", "LWLOCK_EVENT", "AlterPortLock"},
    {"Data file", "LWLOCK_EVENT", "RelationMappingLock"},
    {"PG_NOTIFY", "LWLOCK_EVENT", "AsyncCtlLock"},
    {"Concurrency", "LWLOCK_EVENT", "AsyncQueueLock"},
    {"Transaction", "LWLOCK_EVENT", "SerializableXactHashLock"},
    {"instrTransaction", "LWLOCK_EVENT", "SerializableFinishedListLock"},
    {"Transaction", "LWLOCK_EVENT", "SerializablePredicateLockListLock"},
    {"Transaction", "LWLOCK_EVENT", "OldSerXidLock"},
    {"Statistics file", "LWLOCK_EVENT", "FileStatLock"},
    {"Master-slave replication", "LWLOCK_EVENT", "SyncRepLock"},
    {"Master-slave replication", "LWLOCK_EVENT", "DataSyncRepLock"},
    {"Data file", "LWLOCK_EVENT", "DataFileIdCacheLock"},
    {"Column store", "LWLOCK_EVENT", "CStoreColspaceCacheLock"},
    {"Column store", "LWLOCK_EVENT", "CStoreCUCacheSweepLock"},
    {"Metadata", "LWLOCK_EVENT", "MetaCacheSweepLock"},
    {"FDW", "LWLOCK_EVENT", "FdwPartitionCacheLock"},
    {"HDFS", "LWLOCK_EVENT", "DfsConnectorCacheLock"},
    {"Speed up the cluster", "LWLOCK_EVENT", "dummyServerInfoCacheLock"},
    {"ODBC", "LWLOCK_EVENT", "ExtensionConnectorLibLock"},
    {"Speed up the cluster", "LWLOCK_EVENT", "SearchServerLibLock"},
    {"HDFS", "LWLOCK_EVENT", "DfsUserLoginLock"},
    {"HDFS", "LWLOCK_EVENT", "DfsSpaceCacheLock"},
    {"Master-slave replication", "LWLOCK_EVENT", "LsnXlogChkFileLock"},
    {"GTM", "LWLOCK_EVENT", "GTMHostInfoLock"},
    {"Streaming replication", "LWLOCK_EVENT", "ReplicationSlotAllocationLock"},
    {"Streaming replication", "LWLOCK_EVENT", "ReplicationSlotControlLock"},
    {"WAL", "LWLOCK_EVENT", "FullBuildXlogCopyStartPtrLock"},
    {"Streaming replication", "LWLOCK_EVENT", "LogicalReplicationSlotPersistentDataLock"},
    {"Resource manage", "LWLOCK_EVENT", "ResourcePoolHashLock"},
    {"Resource manage", "LWLOCK_EVENT", "WorkloadStatHashLock"},
    {"Resource manage", "LWLOCK_EVENT", "WorkloadIoStatHashLock"},
    {"Resource manage", "LWLOCK_EVENT", "WorkloadCGroupHashLock"},
    {"Resource manage", "LWLOCK_EVENT", "WorkloadSessionInfoLock"},
    {"OBS", "LWLOCK_EVENT", "OBSGetPathLock"},
    {"Resource manage", "LWLOCK_EVENT", "WorkloadUserInfoLock"},
    {"Resource manage", "LWLOCK_EVENT", "WorkloadRecordLock"},
    {"Resource manage", "LWLOCK_EVENT", "WorkloadIOUtilLock"},
    {"Resource manage", "LWLOCK_EVENT", "WorkloadNodeGroupLock"},
    {"MPP is compatible with ORACLE scheduled task function", "LWLOCK_EVENT", "JobShmemLock"},
    {"OBS", "LWLOCK_EVENT", "OBSRuntimeLock"},
    {"LLVM", "LWLOCK_EVENT", "LLVMDumpIRLock"},
    {"LLVM", "LWLOCK_EVENT", "LLVMParseIRLock"},
    {"Speed up the cluster", "LWLOCK_EVENT", "RPNumberLock"},
    {"Speed up the cluster", "LWLOCK_EVENT", "ClusterRPLock"},
    {"query history information statistics", "LWLOCK_EVENT", "WaitCountHashLock"},
    {"instrumentatiion", "LWLOCK_EVENT", "InstrWorkloadLock"},
    {"FDW", "LWLOCK_EVENT", "PgfdwLock"},
    {"CBM", "LWLOCK_EVENT", "CBMParseXlogLock"},
    {"DDL/DCL", "LWLOCK_EVENT", "DelayDDLLock"},
    {"Relation", "LWLOCK_EVENT", "RelfilenodeReuseLock"},
    {"BadBlock", "LWLOCK_EVENT", "BadBlockStatHashLock"},
    {"Streaming replication", "LWLOCK_EVENT", "RowPageReplicationLock"},
    {"WAL", "LWLOCK_EVENT", "RcvWriteLock"},
    {"Instrumentation", "LWLOCK_EVENT", "InstanceTimeLock"},
    {"instrumentatiion", "LWLOCK_EVENT", "PercentileLock"},
    {"Xlog", "LWLOCK_EVENT", "XlogRemoveSegLock"},
    {"Workload", "LWLOCK_EVENT", "DnUsedSpaceHashLock"},
    {"Shared buffer", "LWLOCK_EVENT", "BufMappingLock"},
    {"Lmgr", "LWLOCK_EVENT", "LockMgrLock"},
    {"Transaction", "LWLOCK_EVENT", "PredicateLockMgrLock"},
    {"Operator history information statistics", "LWLOCK_EVENT", "OperatorRealTLock"},
    {"Operator history information statistics", "LWLOCK_EVENT", "OperatorHistLock"},
    {"query history information statistics", "LWLOCK_EVENT", "SessionRealTLock"},
    {"query history information statistics", "LWLOCK_EVENT", "SessionHistLock"},
    {"Workload", "LWLOCK_EVENT", "InstanceRealTLock"},
    {"Column store", "LWLOCK_EVENT", "CacheSlotMappingLock"},
    {"CSN", "LWLOCK_EVENT", "CSNBufMappingLock"},
    {"Clog", "LWLOCK_EVENT", "CLogBufMappingLock"},
    {"instrumentatiion", "LWLOCK_EVENT", "UniqueSQLMappingLock"},
    {"instrumentatiion", "LWLOCK_EVENT", "InstrUserLockId"},
    {"BufferPool", "LWLOCK_EVENT", "BufferIOLock"},
    {"BufferPool", "LWLOCK_EVENT", "BufferContentLock"},
    {"Cache", "LWLOCK_EVENT", "DataCacheLock"},
    {"Cache", "LWLOCK_EVENT", "MetaCacheLock"},
    {"Lmgr", "LWLOCK_EVENT", "PGPROCLock"},
    {"Replication", "LWLOCK_EVENT", "ReplicationSlotLock"},
    {"AsyncQueueControl", "LWLOCK_EVENT", "Async Ctl"},
    {"Clog", "LWLOCK_EVENT", "CLOG Ctl"},
    {"CSN", "LWLOCK_EVENT", "CSNLOG Ctl"},
    {"Transaction", "LWLOCK_EVENT", "MultiXactOffset Ctl"},
    {"Transaction", "LWLOCK_EVENT", "MultiXactMember Ctl"},
    {"Transaction", "LWLOCK_EVENT", "OldSerXid SLRU Ctl"},
    {"WAL", "LWLOCK_EVENT", "WALInsertLock"},
    {"DoubleWrite", "LWLOCK_EVENT", "DoubleWriteLock"},
    {"Dbmind", "LWLOCK_EVENT", "HypoIndexLock"},
    {"Plugin", "LWLOCK_EVENT", "GeneralExtendedLock"},
    {"plugin", "LWLOCK_EVENT", "extension"},
    {"plugin", "LWLOCK_EVENT", "extension"},
    {"Transaction", "LWLOCK_EVENT", "TwoPhaseStatePartLock"},
    {"Relation", "LWLOCK_EVENT", "NgroupDestoryLock"}
};

Datum get_wait_event_info(PG_FUNCTION_ARGS)
{
    FuncCallContext* funcctx = NULL;
    errno_t rc = 0;

    if (SRF_IS_FIRSTCALL()) {
        MemoryContext oldcontext;
        TupleDesc tupdesc;
        funcctx = SRF_FIRSTCALL_INIT();

        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        tupdesc = CreateTemplateTupleDesc(3, false);
        TupleDescInitEntry(tupdesc, (AttrNumber)1, "module", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)2, "type", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)3, "event", TEXTOID, -1, 0);

        funcctx->tuple_desc = BlessTupleDesc(tupdesc);

        funcctx->user_fctx = palloc0(sizeof(int));
        funcctx->max_calls = WAIT_EVENT_SIZE;
        MemoryContextSwitchTo(oldcontext);
    }

    /* stuff done on every call of the function */
    funcctx = SRF_PERCALL_SETUP();

    if (funcctx->call_cntr < funcctx->max_calls) {
        /* for each row */
        Datum values[3];
        bool nulls[3] = {false};
        HeapTuple tuple = NULL;

        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");
        rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "\0", "\0");
        if (waitEventInfo[funcctx->call_cntr].module == NULL)
            nulls[0] = true;
        else
            values[0] = CStringGetTextDatum(waitEventInfo[funcctx->call_cntr].module);
        values[1] = CStringGetTextDatum(waitEventInfo[funcctx->call_cntr].type);
        values[2] = CStringGetTextDatum(waitEventInfo[funcctx->call_cntr].event);
        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);

        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    } else {
        /* nothing left */
        SRF_RETURN_DONE(funcctx);
    }
}

