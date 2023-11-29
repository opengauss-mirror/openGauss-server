/* -------------------------------------------------------------------------
 *
 * barrier.c
 *
 *	  Barrier handling for PITR
 *
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * IDENTIFICATION
 *	  $$
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"
#include "access/cbmparsexlog.h"
#include "access/gtm.h"
#include "access/xlog.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "pgxc/barrier.h"
#include "postmaster/barrier_creator.h"
#include "postmaster/barrier_preparse.h"
#include "pgxc/execRemote.h"
#include "pgxc/locator.h"
#include "pgxc/pgxc.h"
#include "nodes/nodes.h"
#include "pgxc/pgxcnode.h"
#include "storage/lock/lwlock.h"
#include "tcop/dest.h"
#include "securec_check.h"
#include "utils/elog.h"
#include "replication/walreceiver.h"
#include "replication/archive_walreceiver.h"

#define atolsn(x) ((XLogRecPtr)strtoul((x), NULL, 0))

#ifdef ENABLE_MULTIPLE_NODES
static void PrepareBarrier(PGXCNodeAllHandles* prepared_handles, const char* id, bool isSwitchoverBarrier);
static void ExecuteBarrier(const char* id, bool isSwitchoverBarrier = false);
static void EndBarrier(PGXCNodeAllHandles* handles, const char* id, bool isSwitchoverBarrier = false);
static void CommitBarrier(PGXCNodeAllHandles* prepared_handles, const char* id);
static void WriteBarrierLSNFile(XLogRecPtr barrierLSN, const char* barrier_id);
static void replace_barrier_id_compatible(const char* id, char** log_id);
static void RequestXLogStreamForBarrier();
static void barrier_redo_pause(char* barrierId);
static bool TryBarrierLockWithTimeout();
static void CheckBarrierCommandStatus(PGXCNodeAllHandles* conn_handles, const char* id, const char* command, bool isCn,
    bool isSwitchoverBarrier = false);
#endif
static const int BARRIER_LOCK_TIMEOUT_MS = 2000; // 2S
/*
 * Prepare ourselves for an incoming BARRIER. We must disable all new 2PC
 * commits and let the ongoing commits to finish. We then remember the
 * barrier id (so that it can be matched with the final END message) and
 * tell the driving Coordinator to proceed with the next step.
 *
 * A simple way to implement this is to grab a lock in an exclusive mode
 * while all other backend starting a 2PC will grab the lock in shared
 * mode. So as long as we hold the exclusive lock, no other backend start a
 * new 2PC and there can not be any 2PC in-progress. This technique would
 * rely on assumption that an exclusive lock requester is not starved by
 * share lock requesters.
 *
 * Note: To ensure that the 2PC are not blocked for a long time, we should
 * set a timeout. The lock should be release after the timeout and the
 * barrier should be canceled.
 */
void ProcessCreateBarrierPrepare(const char* id, bool isSwitchoverBarrier)
{
#ifndef ENABLE_MULTIPLE_NODES
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return;
#else
    StringInfoData buf;

    ereport(DEBUG1,
        (errmsg("Receive CREATE BARRIER <%s> PREPARE message on Coordinator", id)));
    if (!IS_PGXC_COORDINATOR || !IsConnFromCoord())
        ereport(ERROR,
            (errcode(ERRCODE_OPERATE_NOT_SUPPORTED),
                errmsg("The CREATE BARRIER PREPARE message is expected to "
                       "arrive at a Coordinator from another Coordinator")));
    if (isSwitchoverBarrier) {
        if (!LWLockHeldByMe(BarrierLock))
            LWLockAcquire(BarrierLock, LW_EXCLUSIVE);
    } else {
        if (!TryBarrierLockWithTimeout()) {
            ereport(ERROR,
                (errcode(ERRCODE_OPERATE_NOT_SUPPORTED),
                    errmsg("Wait Barrier lock timeout barrierId:%s", id)));
        }
    }

    pq_beginmessage(&buf, 'b');
    pq_sendstring(&buf, id);
    pq_endmessage(&buf);
    pq_flush();
#endif
}

/*
 * Mark the completion of an on-going barrier. We must have remembered the
 * barrier ID when we received the CREATE BARRIER PREPARE command
 */
void ProcessCreateBarrierEnd(const char* id)
{
#ifndef ENABLE_MULTIPLE_NODES
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return;
#else
    StringInfoData buf;
    ereport(DEBUG1,
        (errmsg("Receive CREATE BARRIER <%s> END message on Coordinator", id)));
    if (!IS_PGXC_COORDINATOR || !IsConnFromCoord())
        ereport(ERROR,
            (errcode(ERRCODE_OPERATE_NOT_SUPPORTED),
                errmsg("The CREATE BARRIER END message is expected to "
                       "arrive at a Coordinator from another Coordinator")));
    if (LWLockHeldByMe(BarrierLock))
        LWLockRelease(BarrierLock);

    pq_beginmessage(&buf, 'b');
    pq_sendstring(&buf, id);
    pq_endmessage(&buf);
    pq_flush();
#endif
}


void ProcessCreateBarrierCommit(const char* id)
{
#ifndef ENABLE_MULTIPLE_NODES
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return;
#else

    StringInfoData buf;
    ereport(DEBUG1,
        (errmsg("Receive CREATE BARRIER <%s> COMMIT message on Coordinator", id)));
    if (!IS_PGXC_COORDINATOR || !IsConnFromCoord())
        ereport(ERROR,
            (errcode(ERRCODE_OPERATE_NOT_SUPPORTED),
                errmsg("The CREATE BARRIER COMMIT message is expected to "
                       "arrive at a Coordinator from another Coordinator")));
    XLogBeginInsert();
    XLogRegisterData((char*)id, strlen(id) + 1);

    XLogRecPtr recptr = XLogInsert(RM_BARRIER_ID, XLOG_BARRIER_COMMIT, InvalidBktId);
    XLogWaitFlush(recptr);


    pq_beginmessage(&buf, 'b');
    pq_sendstring(&buf, id);
    pq_endmessage(&buf);
    pq_flush();
#endif
}
/*
 * Execute the CREATE BARRIER command. Write a BARRIER WAL record and flush the
 * WAL buffers to disk before returning to the caller. Writing the WAL record
 * does not guarantee successful completion of the barrier command.
 */
void ProcessCreateBarrierExecute(const char* id, bool isSwitchoverBarrier)
{
#ifndef ENABLE_MULTIPLE_NODES
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return;
#else
    StringInfoData buf;
    XLogRecPtr recptr;
    char* log_id = (char*)id;
    char barrierLsn[BARRIER_LSN_LENGTH];
    int rc;

    ereport(DEBUG1,
        (errmsg("Receive CREATE BARRIER <%s> EXECUTE message on Coordinator or Datanode", id)));
    if (!IsConnFromCoord())
        ereport(ERROR,
            (errcode(ERRCODE_OPERATE_NOT_SUPPORTED),
                errmsg("The CREATE BARRIER EXECUTE message is expected to "
                       "arrive from a Coordinator")));

    if (unlikely(t_thrd.proc->workingVersionNum < BACKUP_SLOT_VERSION_NUM)) {
        replace_barrier_id_compatible(id, &log_id);
    }

    if (IS_CSN_BARRIER(id)) {
        CommitSeqNo csn = CsnBarrierNameGetCsn(id);
        if (csn == 0) {
            ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                    (errmsg("Wrong barrier CSN."))));
        }
        UpdateNextMaxKnownCSN(csn);
    }

    if (isSwitchoverBarrier == true) {
        ereport(LOG, (errmsg("Handling DISASTER RECOVERY SWITCHOVER BARRIER:<%s>.", id)));
        ShutdownForDRSwitchover();
        // The access of all users is not blocked temporarily.
        /*
         * Force a checkpoint before starting the switchover. This will force dirty
         * buffers out to disk, to ensure source database is up-to-date on disk
         */
        RequestCheckpoint(CHECKPOINT_IMMEDIATE | CHECKPOINT_FORCE | CHECKPOINT_WAIT);
    }

    XLogBeginInsert();
    XLogRegisterData((char*)log_id, strlen(log_id) + 1);

    if (u_sess->attr.attr_storage.enable_cbm_tracking && IS_ROACH_BARRIER(id)) {
        LWLockAcquire(CBMParseXlogLock, LW_EXCLUSIVE);
    }
    recptr = XLogInsert(RM_BARRIER_ID, isSwitchoverBarrier? XLOG_BARRIER_SWITCHOVER : XLOG_BARRIER_CREATE,
        InvalidBktId);
    XLogWaitFlush(recptr);

    if (IS_CSN_BARRIER(id) && !isSwitchoverBarrier) {
        SpinLockAcquire(&g_instance.streaming_dr_cxt.mutex);
        rc = strncpy_s((char *)g_instance.streaming_dr_cxt.currentBarrierId, MAX_BARRIER_ID_LENGTH,
            id, MAX_BARRIER_ID_LENGTH - 1);
        securec_check(rc, "\0", "\0");
        SpinLockRelease(&g_instance.streaming_dr_cxt.mutex);
        ereport(DEBUG4, (errmodule(MOD_RTO_RPO), errmsg("refresh currentBarrier, barrier id %s", id)));
    }

    // record disaster recovery barrier lsn
    if (IS_CSN_BARRIER(id) || IS_HADR_BARRIER(id)) {
        pg_atomic_init_u64(&g_instance.archive_obs_cxt.barrierLsn, recptr);
        rc = snprintf_s(barrierLsn, BARRIER_LSN_LENGTH, BARRIER_LSN_LENGTH - 1, "0x%lx", recptr);
        securec_check_ss_c(rc, "\0", "\0");
    } else {
        if (u_sess->attr.attr_storage.enable_cbm_tracking) {
            (void)ForceTrackCBMOnce(recptr, 0, false, true);
        }
        WriteBarrierLSNFile(recptr, id);
    }

    if (isSwitchoverBarrier) {
        g_instance.streaming_dr_cxt.switchoverBarrierLsn = recptr;
    }

    pq_beginmessage(&buf, 'b');
    pq_sendstring(&buf, id);
    pq_sendstring(&buf, barrierLsn);
    pq_endmessage(&buf);
    pq_flush();

    if (unlikely(log_id != id)) {
        pfree_ext(log_id);
    }
#endif
}

#ifdef ENABLE_MULTIPLE_NODES
static void ExecBarrierOnFirstExecCnNode(const char* id, const char* firstExecNode, char* completionTag)
{
    int rc = 0;
    char queryString[MAX_BARRIER_SQL_LENGTH] = {0};

    rc = sprintf_s(queryString, MAX_BARRIER_SQL_LENGTH, "create barrier '%s';", id);
    securec_check_ss(rc, "\0", "\0");
    ereport(LOG, (errmsg("Send <%s> to First Exec Coordinator <%s>", queryString, firstExecNode)));

    if (!IsConnFromCoord()) {
        RemoteQuery* step = makeNode(RemoteQuery);
        step->combine_type = COMBINE_TYPE_SAME;
        step->sql_statement = pstrdup(queryString);
        step->force_autocommit = false;
        step->exec_type = EXEC_ON_COORDS;
        step->is_temp = false;
        ExecRemoteUtilityParallelBarrier(step, firstExecNode);
        pfree_ext(step->sql_statement);
        pfree_ext(step);
        if (completionTag != NULL) {
            rc = sprintf_s(completionTag, COMPLETION_TAG_BUFSIZE, "BARRIER %s", id);
            securec_check_ss(rc, "\0", "\0");
        }
    }
}
#endif

void RequestBarrier(char* id, char* completionTag, bool isSwitchoverBarrier)
{
#ifndef ENABLE_MULTIPLE_NODES
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return;
#else
    if (id == NULL)
        ereport(ERROR,
            (errcode(ERRCODE_OPERATE_NOT_SUPPORTED), errmsg("CREATE BARRIER with no barrier name.")));
    PGXCNodeAllHandles* coord_handles = NULL;
    char* barrier_id = id;
    bool isCsnBarrier = (strcmp(id, CSN_BARRIER_NAME) == 0);
    int rc = 0;

    ereport(DEBUG1, (errmsg("CREATE BARRIER request received")));
    /*
     * Ensure that we are a Coordinator and the request is not from another
     * coordinator
     */
    if (!IS_PGXC_COORDINATOR)
        ereport(ERROR,
            (errcode(ERRCODE_OPERATE_NOT_SUPPORTED), errmsg("CREATE BARRIER command must be sent to a Coordinator")));

    /* only superuser or operation-admin user can create barrier */
    if (!superuser() && !(isOperatoradmin(GetUserId()) && u_sess->attr.attr_security.operation_mode))
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                (errmsg("Must be system admin or operator admin in operation mode to create barrier."))));

    ereport(LOG, (errmsg("CREATE BARRIER <%s>", barrier_id)));

    /*
     * Ensure all barrier commond execuet on first coordinator
     */
    char* firstExecNode = find_first_exec_cn();
    bool isFirstNode = (strcmp(firstExecNode, g_instance.attr.attr_common.PGXCNodeName) == 0);
    if (isFirstNode == false) {
        ExecBarrierOnFirstExecCnNode(barrier_id, firstExecNode, completionTag);
        return;
    }

    /*
     * Step One. Prepare all Coordinators for upcoming barrier request, in case of HADR or Roach or GTM-FREE process.
     */
    coord_handles =  get_handles(NIL, GetAllCoordNodes(), true);
    if (t_thrd.postmaster_cxt.HaShmData->is_cross_region || isSwitchoverBarrier ||
        IS_ROACH_BARRIER(id) || GTM_FREE_MODE) {
        PrepareBarrier(coord_handles, barrier_id, isSwitchoverBarrier);
    }
    if (isCsnBarrier)
        GetCsnBarrierName(barrier_id, isSwitchoverBarrier);

    /*
     * Step two. Issue BARRIER command to all involved components, including
     * Coordinators and Datanodes
     */
    ExecuteBarrier(barrier_id, isSwitchoverBarrier);

    /*
     * Step three. Inform Coordinators to release barrier lock, in case of HADR or Roach or GTM-FREE process.
     */
    if (t_thrd.postmaster_cxt.HaShmData->is_cross_region || isSwitchoverBarrier ||
        IS_ROACH_BARRIER(id) || GTM_FREE_MODE) {
        EndBarrier(coord_handles, barrier_id, isSwitchoverBarrier);
    }

    /*
     * Step four. Inform Coordinators about a successfully completed barrier
     */
    if (!isSwitchoverBarrier)
        CommitBarrier(coord_handles, barrier_id);

    /* Finally report the barrier to GTM to backup its restart point */
    ReportBarrierGTM(barrier_id);

    /* Free the handles */
    pfree_pgxc_all_handles(coord_handles);

    if (completionTag != NULL) {
        rc = sprintf_s(completionTag, COMPLETION_TAG_BUFSIZE, "BARRIER %s", barrier_id);
        securec_check_ss(rc, "\0", "\0");
    }
    ereport(u_sess->attr.attr_storage.HaModuleDebug ? LOG : DEBUG2, (errmsg("Create Barrier Success %s", barrier_id)));
#endif
}

void DisasterRecoveryRequestBarrier(const char* id, bool isSwitchoverBarrier)
{
    XLogRecPtr recptr;
    errno_t rc = 0;
    ereport(LOG, (errmsg("DISASTER RECOVERY CREATE BARRIER <%s> request received", id)));
    /*
     * Ensure that we are a DataNode 
     */
    if (!IS_PGXC_DATANODE)
        ereport(ERROR,
            (errcode(ERRCODE_OPERATE_NOT_SUPPORTED), 
                errmsg("DISASTER RECOVERY CREATE BARRIER command must be sent to a DataNode")));

    ereport(DEBUG1, (errmsg("DISASTER RECOVERY CREATE BARRIER <%s>", id)));

    LWLockAcquire(BarrierLock, LW_EXCLUSIVE);
    if (isSwitchoverBarrier == true) {
        ereport(LOG, (errmsg("This is DISASTER RECOVERY SWITCHOVER BARRIER:<%s>.", id)));
        // The access of all users is not blocked temporarily.
        /*
         * Force a checkpoint before starting the switchover. This will force dirty
         * buffers out to disk, to ensure source database is up-to-date on disk
         */
        RequestCheckpoint(CHECKPOINT_IMMEDIATE | CHECKPOINT_FORCE | CHECKPOINT_WAIT);
    }

    XLogBeginInsert();
    XLogRegisterData((char*)id, strlen(id) + 1);

    recptr = XLogInsert(RM_BARRIER_ID, XLOG_BARRIER_CREATE, InvalidBktId);
    XLogWaitFlush(recptr);
#ifndef ENABLE_LITE_MODE
    if (t_thrd.role == BARRIER_CREATOR) {
        UpdateGlobalBarrierListOnMedia(id, g_instance.attr.attr_common.PGXCNodeName);
    }
#endif
    SpinLockAcquire(&g_instance.archive_obs_cxt.barrier_lock);
    pg_atomic_init_u64(&g_instance.archive_obs_cxt.barrierLsn, recptr);
    rc = memcpy_s(g_instance.archive_obs_cxt.barrierName, MAX_BARRIER_ID_LENGTH, id, strlen(id));
    securec_check(rc, "\0", "\0");
    SpinLockRelease(&g_instance.archive_obs_cxt.barrier_lock);

    LWLockRelease(BarrierLock);
}

void CreateHadrSwitchoverBarrier()
{
    XLogRecPtr recptr;
    char barrier_id[MAX_BARRIER_ID_LENGTH] = HADR_SWITCHOVER_BARRIER_ID;
    ereport(LOG, (errmsg("DISASTER RECOVERY CREATE SWITCHOVER BARRIER <%s>", barrier_id)));

    /*
     * Ensure that we are a DataNode 
     */
    if (!IS_PGXC_DATANODE)
        ereport(ERROR,
            (errcode(ERRCODE_OPERATE_NOT_SUPPORTED), 
                errmsg("DISASTER RECOVERY CREATE BARRIER command must be sent to a DataNode")));

    ShutdownForDRSwitchover();
    // The access of all users is not blocked temporarily.
    /*
     * Force a checkpoint before starting the switchover. This will force dirty
     * buffers out to disk, to ensure source database is up-to-date on disk
     */
    RequestCheckpoint(CHECKPOINT_IMMEDIATE | CHECKPOINT_FORCE | CHECKPOINT_WAIT);

    XLogBeginInsert();
    XLogRegisterData((char*)barrier_id, strlen(barrier_id) + 1);

    recptr = XLogInsert(RM_BARRIER_ID, XLOG_BARRIER_SWITCHOVER, InvalidBktId);
    XLogWaitFlush(recptr);

    g_instance.streaming_dr_cxt.switchoverBarrierLsn = recptr;
}

void barrier_redo(XLogReaderState* record)
{
#ifdef ENABLE_MULTIPLE_NODES
    int rc = 0;
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    if (info == XLOG_BARRIER_COMMIT)
        return;
    Assert(!XLogRecHasAnyBlockRefs(record));
    volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;

    /* Nothing to do */
    XLogRecPtr barrierLSN = record->EndRecPtr;
    char* barrierId = XLogRecGetData(record);
    if (IS_HADR_BARRIER(barrierId) && IS_MULTI_DISASTER_RECOVER_MODE) {
        ereport(WARNING, (errmsg("The HADR barrier %s is not for streaming standby cluster", barrierId)));
        return;
    }
    SpinLockAcquire(&walrcv->mutex);
    if (BARRIER_LE(barrierId, (char *)walrcv->lastRecoveredBarrierId)) {
        ereport(WARNING, (errmodule(MOD_REDO), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("The new redo barrier is smaller than the last one.")));
    } 
    SpinLockRelease(&walrcv->mutex);

    if ((strncmp((barrierId + BARRIER_ID_WITHOUT_TIMESTAMP_LEN),
        HADR_SWITCHOVER_BARRIER_TAIL, strlen(HADR_SWITCHOVER_BARRIER_TAIL)) == 0) &&
        (info == XLOG_BARRIER_SWITCHOVER)) {
        walrcv->lastSwitchoverBarrierLSN = barrierLSN;
        ereport(LOG, (errmsg("GET SWITCHOVER BARRIER <%s>, LSN <%X/%X>", barrierId,
                (uint32)(walrcv->lastSwitchoverBarrierLSN >> 32),
                (uint32)(walrcv->lastSwitchoverBarrierLSN))));
    } 

    SpinLockAcquire(&walrcv->mutex);
    walrcv->lastRecoveredBarrierLSN = barrierLSN;
    rc = strncpy_s((char *)walrcv->lastRecoveredBarrierId, MAX_BARRIER_ID_LENGTH, barrierId, MAX_BARRIER_ID_LENGTH - 1);
    securec_check_ss(rc, "\0", "\0");
    SpinLockRelease(&walrcv->mutex);

    if (info == XLOG_BARRIER_CREATE) {
        WriteBarrierLSNFile(barrierLSN, barrierId);
    }

    if (!GTM_FREE_MODE && IS_CSN_BARRIER(barrierId)) {
        CommitSeqNo csn = CsnBarrierNameGetCsn(barrierId);
        UpdateXLogMaxCSN(csn);
        if (t_thrd.xact_cxt.ShmemVariableCache->nextCommitSeqNo < csn + 1)
            t_thrd.xact_cxt.ShmemVariableCache->nextCommitSeqNo = csn + 1;
    }

    if (!IS_MULTI_DISASTER_RECOVER_MODE || XLogRecPtrIsInvalid(t_thrd.xlog_cxt.minRecoveryPoint) ||
        XLByteLT(barrierLSN, t_thrd.xlog_cxt.minRecoveryPoint) ||
        t_thrd.shemem_ptr_cxt.ControlFile->backupEndRequired) {
        return;
    }

    if (IS_BARRIER_HASH_INIT) {
        LWLockAcquire(g_instance.csn_barrier_cxt.barrier_hashtbl_lock, LW_EXCLUSIVE);
        BarrierCacheDeleteBarrierId(barrierId);
        LWLockRelease(g_instance.csn_barrier_cxt.barrier_hashtbl_lock);
        ereport(LOG, (errmsg("remove barrierID %s from hash table", barrierId)));
    }

    SetXLogReplayRecPtr(record->ReadRecPtr, record->EndRecPtr);
    CheckRecoveryConsistency();
    UpdateMinRecoveryPoint(barrierLSN, false);

    barrier_redo_pause(barrierId);
#else
    int rc = 0;
    volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;
    /* Nothing to do */
    XLogRecPtr barrierLSN = record->EndRecPtr;
    char* barrierId = XLogRecGetData(record);
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    if (info == XLOG_BARRIER_COMMIT)
        return;
    if (BARRIER_LE(barrierId, (char *)walrcv->lastRecoveredBarrierId)) {
            ereport(WARNING, (errmodule(MOD_REDO), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("The new redo barrier is smaller than the last one.")));
    }

    if (BARRIER_EQ(barrierId, HADR_SWITCHOVER_BARRIER_ID) && (info == XLOG_BARRIER_SWITCHOVER)) {
        walrcv->lastSwitchoverBarrierLSN = barrierLSN;
        ereport(LOG, (errmsg("GET SWITCHOVER BARRIER <%s>, LSN <%X/%X>", barrierId,
                (uint32)(walrcv->lastSwitchoverBarrierLSN >> 32),
                (uint32)(walrcv->lastSwitchoverBarrierLSN))));
    }
    SpinLockAcquire(&walrcv->mutex);
    walrcv->lastRecoveredBarrierLSN = barrierLSN;
    rc = strncpy_s((char *)walrcv->lastRecoveredBarrierId, MAX_BARRIER_ID_LENGTH, barrierId, MAX_BARRIER_ID_LENGTH - 1);
    securec_check_ss(rc, "\0", "\0");
    SpinLockRelease(&walrcv->mutex);
#endif
}

bool is_barrier_pausable(const char* id)
{
    if (IS_CSN_BARRIER(id))
        return true;
    else
        return false;
}

#ifdef ENABLE_MULTIPLE_NODES


static void SaveAllNodeBarrierLsnInfo(const char* id, const PGXCNodeAllHandles* connHandles)
{
    int conn;
    for (conn = 0; conn < connHandles->co_conn_count + connHandles->dn_conn_count; conn++) {
        PGXCNodeHandle* handle = NULL;
        if (conn < connHandles->co_conn_count)
            handle = connHandles->coord_handles[conn];
        else
            handle = connHandles->datanode_handles[conn - connHandles->co_conn_count];
        
        if (handle == NULL || handle->inBuffer == NULL) {
            ereport(WARNING, (errmsg("SaveAllNodeBarrierLsnInfo get handle is NULL, conn: %d", conn)));
            g_instance.archive_obs_cxt.barrier_lsn_info[conn].barrierLsn = 0;
            g_instance.archive_obs_cxt.barrier_lsn_info[conn].nodeoid = 0;
            break;
        }
        
        char* lsn = handle->inBuffer + 5 + strlen(id) + 1;
        g_instance.archive_obs_cxt.barrier_lsn_info[conn].barrierLsn = atolsn(lsn);
        g_instance.archive_obs_cxt.barrier_lsn_info[conn].nodeoid = handle->nodeoid;
    }
    errno_t errorno = memcpy_s(g_instance.archive_obs_cxt.barrierName, MAX_BARRIER_ID_LENGTH, id, strlen(id) + 1);
    securec_check(errorno, "\0", "\0");
}

void barrier_desc(StringInfo buf, uint8 xl_info, char* rec)
{
    Assert(xl_info == XLOG_BARRIER_CREATE);
    appendStringInfo(buf, "BARRIER %s", rec);
}

static void SendBarrierRequestToCns(PGXCNodeAllHandles* coord_handles, const char* id, const char* cmd, char type)
{
    int conn;
    int msglen;
    int barrier_idlen;
    errno_t rc;

    /* Ensure that get all coordinators cnt not include current node itself */
    if (u_sess->pgxc_cxt.NumCoords == coord_handles->co_conn_count) {
        ereport(ERROR,
                (errcode(ERRCODE_OPERATE_FAILED),
                    errmsg("Failed to send %s request"
                        "get all cn_conn: %d", cmd,
                        coord_handles->co_conn_count)));
    }    

    for (conn = 0; conn < coord_handles->co_conn_count; conn++) {
        PGXCNodeHandle* handle = coord_handles->coord_handles[conn];

        /* Invalid connection state, return error */
        if (handle->state != DN_CONNECTION_STATE_IDLE) {
            ereport(ERROR,
                (errcode(ERRCODE_OPERATE_FAILED),
                    errmsg("Failed to send %s request "
                           "to the node", cmd)));
        }

        barrier_idlen = strlen(id) + 1;

        msglen = 4; /* for the length itself */
        msglen += barrier_idlen;
        msglen += 1; /* for barrier command itself */

        /* msgType + msgLen */
        ensure_out_buffer_capacity(1 + msglen, handle);

        Assert(handle->outBuffer != NULL);
        handle->outBuffer[handle->outEnd++] = 'b';
        msglen = htonl(msglen);
        rc = memcpy_s(handle->outBuffer + handle->outEnd, handle->outSize - handle->outEnd, &msglen, sizeof(int));
        securec_check(rc, "\0", "\0");
        handle->outEnd += 4;

        handle->outBuffer[handle->outEnd++] = type;

        rc = memcpy_s(handle->outBuffer + handle->outEnd, handle->outSize - handle->outEnd, id, barrier_idlen);
        securec_check(rc, "\0", "\0");
        handle->outEnd += barrier_idlen;

        handle->state = DN_CONNECTION_STATE_QUERY;

        pgxc_node_flush(handle);
    }
}

static void CheckBarrierCommandStatus(PGXCNodeAllHandles* conn_handles, const char* id, const char* command, bool isCn,
    bool isSwitchoverBarrier)
{
    int conn;
    int count = isCn? conn_handles->co_conn_count : conn_handles->dn_conn_count;
    RemoteQueryState* combiner = NULL;
    struct timeval timeout;
    timeout.tv_sec = ERROR_CHECK_TIMEOUT;
    timeout.tv_usec = 0;
    ereport(DEBUG1, (errmsg("Check CREATE BARRIER <%s> %s command status", id, command)));

    combiner = CreateResponseCombiner(count, COMBINE_TYPE_NONE);

    for (conn = 0; conn < count; conn++) {
        PGXCNodeHandle* handle = NULL;

        if (isCn)
            handle = conn_handles->coord_handles[conn];
        else
            handle = conn_handles->datanode_handles[conn];

        if (pgxc_node_receive(1, &handle, (isCn && !isSwitchoverBarrier)? &timeout : NULL))
            ereport(
                ERROR, (errcode(ERRCODE_OPERATE_FAILED), errmsg("Failed to receive response from the remote side")));
        if (handle_response(handle, combiner) != RESPONSE_BARRIER_OK)
            ereport(ERROR,
                (errcode(ERRCODE_OPERATE_FAILED),
                    errmsg("CREATE BARRIER command %s failed with error %s", command, handle->error)));
    }
    CloseCombiner(combiner);

    ereport(DEBUG1,
        (errmsg("Successfully completed CREATE BARRIER <%s> %s command on "
                "all nodes",
            id,
            command)));
}

static bool TryBarrierLockWithTimeout()
{
    bool getLock = false;
    if (LWLockHeldByMe(BarrierLock)) {
        getLock = true;
    } else {
        TimestampTz start_time = GetCurrentTimestamp();
        do {
            if(LWLockConditionalAcquire(BarrierLock, LW_EXCLUSIVE)) {
                getLock = true;
                break;
            }
            pg_usleep(1000L);
        } while (ComputeTimeStamp(start_time) < BARRIER_LOCK_TIMEOUT_MS);
    }
    return getLock;
}

/*
 * Prepare all Coordinators for barrier. During this step all the Coordinators
 * are informed to suspend any new 2PC transactions. The Coordinators should
 * disable new 2PC transactions and then wait for the existing transactions to
 * complete. Once all "in-flight" 2PC transactions are over, the Coordinators
 * respond back.
 *
 * That completes the first step in barrier generation
 *
 * Any errors will be reported via ereport.
 */
static void PrepareBarrier(PGXCNodeAllHandles* coord_handles, const char* id, bool isSwitchoverBarrier)
{
    ereport(DEBUG1, (errmsg("Preparing Coordinators for BARRIER")));
    /*
     * Send a CREATE BARRIER PREPARE message to all the Coordinators. We should
     * send an asynchronous request so that we can disable local commits and
     * then wait for the remote Coordinators to finish the work
     */
    SendBarrierRequestToCns(coord_handles, id, isSwitchoverBarrier? "CREATE SWITCHOVER BARRIER PREPARE" :
        "CREATE BARRIER PREPARE", isSwitchoverBarrier? CREATE_SWITCHOVER_BARRIER_PREPARE : CREATE_BARRIER_PREPARE);

    /*
     * Disable local commits
     */

    if (isSwitchoverBarrier) {
        if (!LWLockHeldByMe(BarrierLock))
            LWLockAcquire(BarrierLock, LW_EXCLUSIVE);
    } else {
        if (!TryBarrierLockWithTimeout()) {
            ereport(ERROR,
                (errcode(ERRCODE_OPERATE_NOT_SUPPORTED),
                    errmsg("Wait Barrier lock timeout barrierId:%s", id)));
        }
    }

    ereport(DEBUG2, (errmsg("Disabled 2PC commits originating at the driving Coordinator")));

    /*
     * future Start a timer to cancel the barrier request in case of a timeout
     */

    /*
     * Local in-flight commits are now over. Check status of the remote
     * Coordinators
     */
    CheckBarrierCommandStatus(coord_handles, id, "PREPARE", true, isSwitchoverBarrier);
}


static void ExecuteBarrierOnNodes(const char* id, bool isSwitchoverBarrier, PGXCNodeAllHandles* conn_handles, bool isCn)
{
    int msglen;
    int barrier_idlen;
    errno_t rc = 0;
     /*
     * Send a CREATE BARRIER request to all nodes
     */
    int handleNum = isCn ? conn_handles->co_conn_count : conn_handles->dn_conn_count;
    for (int conn = 0; conn < handleNum; conn++) {
        PGXCNodeHandle* handle = NULL;

        if (isCn)
            handle = conn_handles->coord_handles[conn];
        else
            handle = conn_handles->datanode_handles[conn];

        /* Invalid connection state, return error */
        if (handle->state != DN_CONNECTION_STATE_IDLE)
            ereport(ERROR,
                (errcode(ERRCODE_OPERATE_FAILED),
                    errmsg("Failed to send CREATE BARRIER EXECUTE request "
                           "to the node")));

        barrier_idlen = strlen(id) + 1;

        msglen = 4; /* for the length itself */
        msglen += barrier_idlen;
        msglen += 1; /* for barrier command itself */

        /* msgType + msgLen */
        ensure_out_buffer_capacity(1 + msglen, handle);

        Assert(handle->outBuffer != NULL);
        handle->outBuffer[handle->outEnd++] = 'b';
        msglen = htonl(msglen);
        rc = memcpy_s(handle->outBuffer + handle->outEnd, handle->outSize - handle->outEnd, &msglen, sizeof(int));
        securec_check(rc, "\0", "\0");
        handle->outEnd += 4;

        if (isSwitchoverBarrier) {
            handle->outBuffer[handle->outEnd++] = CREATE_SWITCHOVER_BARRIER_EXECUTE;
        } else {
            handle->outBuffer[handle->outEnd++] = CREATE_BARRIER_EXECUTE;
        }

        rc = memcpy_s(handle->outBuffer + handle->outEnd, handle->outSize - handle->outEnd, id, barrier_idlen);
        securec_check(rc, "\0", "\0");
        handle->outEnd += barrier_idlen;

        handle->state = DN_CONNECTION_STATE_QUERY;
        pgxc_node_flush(handle);
    }

    CheckBarrierCommandStatus(conn_handles, id, "EXECUTE", isCn, isSwitchoverBarrier);
}


/*
 * Execute the barrier command on all the components, including Datanodes and
 * Coordinators.
 */
static void ExecuteBarrier(const char* id, bool isSwitchoverBarrier)
{
    List* barrierDataNodeList = GetAllDataNodes();
    List* barrierCoordList = GetAllCoordNodes();
    PGXCNodeAllHandles* conn_handles = NULL;
    XLogRecPtr recptr;
    char* log_id = (char*)id;
    int connCnt;
    int rc;
    conn_handles = get_handles(barrierDataNodeList, barrierCoordList, false);
    connCnt = conn_handles->co_conn_count + conn_handles->dn_conn_count;

    ereport(DEBUG1,
        (errmsg("Sending CREATE BARRIER <%s> EXECUTE message to "
                "Datanodes and Coordinator",
            id)));
    // first write barrier xlog to all dns
    ExecuteBarrierOnNodes(id, isSwitchoverBarrier, conn_handles, false);
    // then write barrier xlog to all other cns
    ExecuteBarrierOnNodes(id, isSwitchoverBarrier, conn_handles, true);

    if (unlikely(t_thrd.proc->workingVersionNum < BACKUP_SLOT_VERSION_NUM)) {
        replace_barrier_id_compatible(id, &log_id);
    }

    if (IS_CSN_BARRIER(id)) {
        CommitSeqNo csn = CsnBarrierNameGetCsn(id);
        UpdateNextMaxKnownCSN(csn);
    }

    if (isSwitchoverBarrier == true) {
        ereport(LOG, (errmsg("Sending DISASTER RECOVERY SWITCHOVER BARRIER:<%s>.", id)));
        ShutdownForDRSwitchover();
        // The access of all users is not blocked temporarily.
        /*
         * Force a checkpoint before starting the switchover. This will force dirty
         * buffers out to disk, to ensure source database is up-to-date on disk
         */
        RequestCheckpoint(CHECKPOINT_IMMEDIATE | CHECKPOINT_FORCE | CHECKPOINT_WAIT);
    }

    /*
     * Also WAL log the BARRIER locally and flush the WAL buffers to disk
     */
    XLogBeginInsert();
    XLogRegisterData((char*)log_id, strlen(log_id) + 1);

    if (u_sess->attr.attr_storage.enable_cbm_tracking && IS_ROACH_BARRIER(id)) {
        LWLockAcquire(CBMParseXlogLock, LW_EXCLUSIVE);
    }
    recptr = XLogInsert(RM_BARRIER_ID, isSwitchoverBarrier? XLOG_BARRIER_SWITCHOVER : XLOG_BARRIER_CREATE,
        InvalidBktId);
    XLogWaitFlush(recptr);

    if (IS_CSN_BARRIER(id) && !isSwitchoverBarrier) {
        SpinLockAcquire(&g_instance.streaming_dr_cxt.mutex);
        rc = strncpy_s((char *)g_instance.streaming_dr_cxt.currentBarrierId, MAX_BARRIER_ID_LENGTH,
            id, MAX_BARRIER_ID_LENGTH - 1);
        securec_check(rc, "\0", "\0");
        SpinLockRelease(&g_instance.streaming_dr_cxt.mutex);
        ereport(DEBUG4, (errmodule(MOD_RTO_RPO), errmsg("refresh currentBarrier, barrier id %s", id)));
    }

    if (IS_ROACH_BARRIER(id)) {
        if (u_sess->attr.attr_storage.enable_cbm_tracking) {
            (void)ForceTrackCBMOnce(recptr, 0, false, true);
        }
        WriteBarrierLSNFile(recptr, id);
    }

    /* Only obs-based disaster recovery needs the following processing */
    if (g_instance.archive_obs_cxt.archive_slot_num != 0 && g_instance.archive_obs_cxt.barrier_lsn_info != NULL) {
#ifdef ENABLE_MULTIPLE_NODES
        if (IS_HADR_BARRIER(id) || IS_CSN_BARRIER(id)) {
            SpinLockAcquire(&g_instance.archive_obs_cxt.barrier_lock);
            SaveAllNodeBarrierLsnInfo(id, conn_handles);
            g_instance.archive_obs_cxt.barrier_lsn_info[connCnt].barrierLsn = recptr;
            SpinLockRelease(&g_instance.archive_obs_cxt.barrier_lock);
        }
#endif

        if (t_thrd.role == BARRIER_CREATOR && !isSwitchoverBarrier) {
            UpdateGlobalBarrierListOnMedia(id, g_instance.attr.attr_common.PGXCNodeName);
        }
    }

    if (isSwitchoverBarrier) {
        g_instance.streaming_dr_cxt.switchoverBarrierLsn = recptr;
    }

    list_free(barrierCoordList);
    list_free(barrierDataNodeList);
    pfree_pgxc_all_handles(conn_handles);
    if (unlikely(log_id != id)) {
        pfree_ext(log_id);
    }
}

void CleanupBarrierLock()
{
    List* barrierCoordList = GetAllCoordNodes();
    PGXCNodeAllHandles* conn_handles = NULL;
    char* id = "cleanup";

    conn_handles = get_handles(NULL, barrierCoordList, false);

    if (LWLockHeldByMe(BarrierLock))
        LWLockRelease(BarrierLock);

    SendBarrierRequestToCns(conn_handles, id, "CREATE BARRIER CLEANUP", CREATE_BARRIER_END);

    CheckBarrierCommandStatus(conn_handles, id, "CLEANUP", true);
    pfree_pgxc_all_handles(conn_handles);
}


static void CommitBarrier(PGXCNodeAllHandles* prepared_handles, const char* id)
{
    SendBarrierRequestToCns(prepared_handles, id, "CREATE BARRIER COMMIT", CREATE_BARRIER_COMMIT);
    CheckBarrierCommandStatus(prepared_handles, id, "COMMIT", true);
    XLogBeginInsert();
    XLogRegisterData((char*)id, strlen(id) + 1);

    XLogRecPtr recptr = XLogInsert(RM_BARRIER_ID, XLOG_BARRIER_COMMIT, InvalidBktId);
    XLogWaitFlush(recptr);
}

/*
 * Resume 2PC commits on the local as well as remote Coordinators.
 */
static void EndBarrier(PGXCNodeAllHandles* prepared_handles, const char* id, bool isSwitchoverBarrier)
{
    /* Resume 2PC locally */
    LWLockRelease(BarrierLock);

    SendBarrierRequestToCns(prepared_handles, id, "CREATE BARRIER END", CREATE_BARRIER_END);

    CheckBarrierCommandStatus(prepared_handles, id, "END", true, isSwitchoverBarrier);
}

static void WriteBarrierLSNFile(XLogRecPtr barrier_lsn, const char* barrier_id)
{
    char filename[MAXPGPATH] = {0};
    const char *prefix = NULL;
    errno_t errorno = EOK;
    FILE* fp = NULL;

    if (strncmp(barrier_id, ROACH_FULL_BAK_PREFIX, strlen(ROACH_FULL_BAK_PREFIX)) == 0) {
        prefix = ROACH_FULL_BAK_PREFIX;
    } else if (strncmp(barrier_id, ROACH_INC_BAK_PREFIX, strlen(ROACH_INC_BAK_PREFIX)) == 0) {
        prefix = ROACH_INC_BAK_PREFIX;
    } else {
        return;
    }

    errorno = snprintf_s(filename, sizeof(filename), sizeof(filename) - 1, "%s.%s", BARRIER_LSN_FILE, prefix);
    securec_check_ss(errorno, "\0", "\0");

    fp = AllocateFile(filename, PG_BINARY_W);

    if (fp == NULL)
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not create file \"%s\": %m", filename)));

    if (fprintf(fp, "%08X/%08X", (uint32)(barrier_lsn >> 32), (uint32)barrier_lsn) != BARRIER_LSN_FILE_LENGTH ||
        fflush(fp) != 0 || pg_fsync(fileno(fp)) != 0 || ferror(fp) || FreeFile(fp))
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not write file \"%s\": %m", filename)));
}

/*
 * v5r1c20 barrier id: gs_roach_full_backupkey/gs_roach_inc_backupkey
 * before: roach_barrier_backupkey
 * To be compatible, during upgrade to v5r1c20, we still use old barrier name.
 */
void replace_barrier_id_compatible(const char* id, char** log_id) {
    const char *prefix = NULL;
    char *tmp_id = NULL;
    int rc;
    int len;

    if (strncmp(id, ROACH_FULL_BAK_PREFIX, strlen(ROACH_FULL_BAK_PREFIX)) == 0) {
        prefix = ROACH_FULL_BAK_PREFIX;
    } else if (strncmp(id, ROACH_INC_BAK_PREFIX, strlen(ROACH_INC_BAK_PREFIX)) == 0) {
        prefix = ROACH_INC_BAK_PREFIX;
    } else {
        return;
    }

    len = strlen(id) + strlen("roach_barrier");
    tmp_id = (char *)palloc0(len);
    rc = snprintf_s(tmp_id, len, len - 1, "%s%s", "roach_barrier", id + strlen(prefix));
    securec_check_ss(rc, "", "");

    *log_id = tmp_id;
}

static void RequestXLogStreamForBarrier()
{
    XLogRecPtr replayEndPtr = GetXLogReplayRecPtr(NULL);
    if (t_thrd.xlog_cxt.is_cascade_standby && (CheckForSwitchoverTrigger() || CheckForFailoverTrigger())) {
        HandleCascadeStandbyPromote(&replayEndPtr);
        return;
    }
    if (!WalRcvInProgress() && g_instance.pid_cxt.WalReceiverPID == 0) {
        volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;
        SpinLockAcquire(&walrcv->mutex);
        walrcv->receivedUpto = 0;
        SpinLockRelease(&walrcv->mutex);
        if (t_thrd.xlog_cxt.readFile >= 0) {
            (void)close(t_thrd.xlog_cxt.readFile);
            t_thrd.xlog_cxt.readFile = -1;
        }

        RequestXLogStreaming(&replayEndPtr, t_thrd.xlog_cxt.PrimaryConnInfo, REPCONNTARGET_PRIMARY,
                             u_sess->attr.attr_storage.PrimarySlotName);
    }
}

static void barrier_redo_pause(char* barrierId)
{
    if (!is_barrier_pausable(barrierId) || t_thrd.xlog_cxt.recoveryTarget == RECOVERY_TARGET_TIME_OBS) {
        return;
    }
    volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;

    while (true) {
        SpinLockAcquire(&walrcv->mutex);
        if (BARRIER_LT((char *)walrcv->lastRecoveredBarrierId, (char *)walrcv->recoveryTargetBarrierId) ||
            BARRIER_LE((char *)walrcv->lastRecoveredBarrierId, (char *)walrcv->recoveryStopBarrierId)||
            BARRIER_EQ((char *)walrcv->lastRecoveredBarrierId, (char *)walrcv->recoverySwitchoverBarrierId)) {
            walrcv->isPauseByTargetBarrier = false;
            SpinLockRelease(&walrcv->mutex);
            break;
        } else {
            walrcv->isPauseByTargetBarrier = true;
            SpinLockRelease(&walrcv->mutex);
            pg_usleep(1000L);
            RedoInterruptCallBack();
            if (IS_OBS_DISASTER_RECOVER_MODE) {
                update_recovery_barrier();
            } else if (IS_MULTI_DISASTER_RECOVER_MODE) {
                RequestXLogStreamForBarrier();
            }
            ereport(DEBUG4, ((errmodule(MOD_REDO), errcode(ERRCODE_LOG), 
                    errmsg("Sleeping to get a new target global barrier %s;"
                            "lastRecoveredBarrierId is %s; lastRecoveredBarrierLSN is %X/%X;"
                            "local minRecoveryPoint %X/%X; recoveryStopBarrierId is %s;"
                            "recoverySwitchoverBarrierId is %s",
                            (char *)walrcv->recoveryTargetBarrierId,
                            (char *)walrcv->lastRecoveredBarrierId,
                            (uint32)(walrcv->lastRecoveredBarrierLSN >> 32),
                            (uint32)(walrcv->lastRecoveredBarrierLSN),
                            (uint32)(t_thrd.xlog_cxt.minRecoveryPoint >> 32),
                            (uint32)(t_thrd.xlog_cxt.minRecoveryPoint),
                            (char *)walrcv->recoveryStopBarrierId,
                            (char *)walrcv->recoverySwitchoverBarrierId))));
        }
    }
}
#endif
