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

#ifdef ENABLE_MULTIPLE_NODES
static const char* generate_barrier_id(const char* id);
static PGXCNodeAllHandles* PrepareBarrier(const char* id);
static void ExecuteBarrier(const char* id);
static void EndBarrier(PGXCNodeAllHandles* handles, const char* id);
static void WriteBarrierLSNFile(XLogRecPtr barrierLSN);
#endif
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
void ProcessCreateBarrierPrepare(const char* id)
{
#ifndef ENABLE_MULTIPLE_NODES
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return;
#else

    StringInfoData buf;

    if (!IS_PGXC_COORDINATOR || !IsConnFromCoord())
        ereport(ERROR,
            (errcode(ERRCODE_INTERNAL_ERROR),
                errmsg("The CREATE BARRIER PREPARE message is expected to "
                       "arrive at a Coordinator from another Coordinator")));

    (void)LWLockAcquire(BarrierLock, LW_EXCLUSIVE);

    pq_beginmessage(&buf, 'b');
    pq_sendstring(&buf, id);
    pq_endmessage(&buf);
    pq_flush();

    /*
     * TODO Start a timer to terminate the pending barrier after a specified
     * timeout
     */
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

    if (!IS_PGXC_COORDINATOR || !IsConnFromCoord())
        ereport(ERROR,
            (errcode(ERRCODE_INTERNAL_ERROR),
                errmsg("The CREATE BARRIER END message is expected to "
                       "arrive at a Coordinator from another Coordinator")));

    LWLockRelease(BarrierLock);

    pq_beginmessage(&buf, 'b');
    pq_sendstring(&buf, id);
    pq_endmessage(&buf);
    pq_flush();

    /*
     * TODO Stop the timer
     */
#endif
}

/*
 * Execute the CREATE BARRIER command. Write a BARRIER WAL record and flush the
 * WAL buffers to disk before returning to the caller. Writing the WAL record
 * does not guarantee successful completion of the barrier command.
 */
void ProcessCreateBarrierExecute(const char* id)
{
#ifndef ENABLE_MULTIPLE_NODES
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return;
#else

    StringInfoData buf;

    if (!IsConnFromCoord())
        ereport(ERROR,
            (errcode(ERRCODE_INTERNAL_ERROR),
                errmsg("The CREATE BARRIER EXECUTE message is expected to "
                       "arrive from a Coordinator")));
    {
        XLogRecData rdata[1];
        XLogRecPtr recptr;

        rdata[0].data = (char*)id;
        rdata[0].len = strlen(id) + 1;
        rdata[0].buffer = InvalidBuffer;
        rdata[0].next = NULL;

        recptr = XLogInsert(RM_BARRIER_ID, XLOG_BARRIER_CREATE, rdata);
        XLogWaitFlush(recptr);
    }

    pq_beginmessage(&buf, 'b');
    pq_sendstring(&buf, id);
    pq_endmessage(&buf);
    pq_flush();
#endif
}

void RequestBarrier(const char* id, char* completionTag)
{
#ifndef ENABLE_MULTIPLE_NODES
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return;
#else

    PGXCNodeAllHandles* prepared_handles = NULL;
    const char* barrier_id = NULL;

    elog(DEBUG1, "CREATE BARRIER request received");
    /*
     * Ensure that we are a Coordinator and the request is not from another
     * coordinator
     */
    if (!IS_PGXC_COORDINATOR)
        ereport(
            ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("CREATE BARRIER command must be sent to a Coordinator")));

    if (IsConnFromCoord())
        ereport(ERROR,
            (errcode(ERRCODE_INTERNAL_ERROR),
                errmsg("CREATE BARRIER command is not expected from another Coordinator")));

    /*
     * Get a barrier id if the user has not supplied it
     */
    barrier_id = generate_barrier_id(id);

    elog(DEBUG1, "CREATE BARRIER <%s>", barrier_id);

    /*
     * Step One. Prepare all Coordinators for upcoming barrier request
     */
    prepared_handles = PrepareBarrier(barrier_id);

    /*
     * Step two. Issue BARRIER command to all involved components, including
     * Coordinators and Datanodes
     */
    ExecuteBarrier(barrier_id);

    /*
     * Step three. Inform Coordinators about a successfully completed barrier
     */
    EndBarrier(prepared_handles, barrier_id);
    /* Finally report the barrier to GTM to backup its restart point */
    ReportBarrierGTM((char*)barrier_id);

    /* Free the handles */
    pfree_pgxc_all_handles(prepared_handles);

    if (completionTag) {
        int rc = snprintf_s(completionTag, COMPLETION_TAG_BUFSIZE, COMPLETION_TAG_BUFSIZE - 1, "BARRIER %s", barrier_id);
        securec_check_ss(rc, "", "");
    }
    
#endif
}

static void WaitBarrierArchived()
{
    int cnt = 0;
    ereport(LOG,
            (errmsg("Query barrier lsn: 0x%lx", g_instance.archive_obs_cxt.barrierLsn)));
    do {
        if (NULL == getObsReplicationSlot()) {
            ereport(ERROR, (errcode(ERRCODE_OPERATE_NOT_SUPPORTED), errmsg("Archived thread shut down.")));
        }
        if (XLByteLE(pg_atomic_read_u64(&g_instance.archive_obs_cxt.barrierLsn),
            pg_atomic_read_u64(&g_instance.archive_obs_cxt.archive_task.targetLsn))) {
            break;
        }
        CHECK_FOR_INTERRUPTS();
        pg_usleep(100000L);
        cnt++;
        /* timeout 10 minute */
        if (cnt > WAIT_ARCHIVE_TIMEOUT) {
            ereport(ERROR, (errcode(ERRCODE_OPERATE_NOT_SUPPORTED), errmsg("Wait archived timeout.")));
        }
    } while (1);
    ereport(LOG,
            (errmsg("Query archive lsn: 0x%lx", g_instance.archive_obs_cxt.archive_task.targetLsn)));
}


void DisasterRecoveryRequestBarrier(const char* id)
{
    XLogRecPtr recptr;
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

    XLogBeginInsert();
    XLogRegisterData((char*)id, strlen(id) + 1);

    recptr = XLogInsert(RM_BARRIER_ID, XLOG_BARRIER_CREATE);
    XLogWaitFlush(recptr);

    pg_atomic_init_u64(&g_instance.archive_obs_cxt.barrierLsn, recptr);

    LWLockRelease(BarrierLock);

    WaitBarrierArchived();
}


static void barrier_redo_pause()
{
    volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;

    while (IS_DISASTER_RECOVER_MODE) {
        RedoInterruptCallBack();
        if (strcmp((char *)walrcv->lastRecoveredBarrierId, (char *)walrcv->recoveryTargetBarrierId) < 0 ||
            strcmp((char *)walrcv->lastRecoveredBarrierId, (char *)walrcv->recoveryStopBarrierId) == 0) {
            break;
        } else {
            pg_usleep(1000L);
            update_recovery_barrier();
        }
    }
}

void barrier_redo(XLogReaderState* record)
{
    ereport(LOG, (errmsg("barrier_redo begin.")));
    int rc = 0;
    volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;
    /* Nothing to do */
    XLogRecPtr barrierLSN = record->EndRecPtr;
    char* barrierId = XLogRecGetData(record);
    walrcv->lastRecoveredBarrierLSN = barrierLSN;
    rc = strncpy_s((char *)walrcv->lastRecoveredBarrierId, MAX_BARRIER_ID_LENGTH, barrierId, MAX_BARRIER_ID_LENGTH - 1);
    securec_check_ss(rc, "\0", "\0");
    barrier_redo_pause();
    return;
}

#ifdef ENABLE_MULTIPLE_NODES
// 我们删除了的pg源码, 或者编译有问题的死代码

static const char* generate_barrier_id(const char* id)
{
    static const int LEN_GEN_ID = 1024;
    char genid[LEN_GEN_ID];
    TimestampTz ts;

    /*
     * If the caller can passed a NULL value, generate an id which is
     * guaranteed to be unique across the cluster. We use a combination of
     * the Coordinator node id and current timestamp.
     */

    if (id)
        return id;

    ts = GetCurrentTimestamp();
#ifdef HAVE_INT64_TIMESTAMP
    int rc = snprintf_s(genid, LEN_GEN_ID, LEN_GEN_ID - 1, "%s_" INT64_FORMAT, PGXCNodeName, ts);
#else
    int rc = snprintf_s(genid, LEN_GEN_ID, LEN_GEN_ID - 1, "%s_%.0f", PGXCNodeName, ts);
#endif
    securec_check_ss(rc, "", "");
    return pstrdup(genid);
}

void barrier_desc(StringInfo buf, uint8 xl_info, char* rec)
{
    Assert(xl_info == XLOG_BARRIER_CREATE);
    appendStringInfo(buf, "BARRIER %s", rec);
}

static PGXCNodeAllHandles* SendBarrierPrepareRequest(List* coords, const char* id)
{
    PGXCNodeAllHandles* coord_handles;
    int conn;
    int msglen;
    int barrier_idlen;

    coord_handles = get_handles(NIL, coords, true);

    for (conn = 0; conn < coord_handles->co_conn_count; conn++) {
        PGXCNodeHandle* handle = coord_handles->coord_handles[conn];

        /* Invalid connection state, return error */
        if (handle->state != DN_CONNECTION_STATE_IDLE)
            ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                    errmsg("Failed to send CREATE BARRIER PREPARE request "
                           "to the node")));

        barrier_idlen = strlen(id) + 1;

        msglen = 4; /* for the length itself */
        msglen += barrier_idlen;
        msglen += 1; /* for barrier command itself */

        /* msgType + msgLen */
        if (ensure_out_buffer_capacity(handle->outEnd + 1 + msglen, handle) != 0) {
            ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("Out of memory")));
        }

        handle->outBuffer[handle->outEnd++] = 'b';
        msglen = htonl(msglen);
        int rc = memcpy_s(handle->outBuffer + handle->outEnd, handle->outSize - handle->outEnd, &msglen, 4);
        securec_check(rc, "\0", "\0");
        handle->outEnd += 4;

        handle->outBuffer[handle->outEnd++] = CREATE_BARRIER_PREPARE;

        rc = memcpy_s(handle->outBuffer + handle->outEnd, handle->outSize - handle->outEnd, id, barrier_idlen);
        securec_check(rc, "\0", "\0");
        handle->outEnd += barrier_idlen;

        handle->state = DN_CONNECTION_STATE_QUERY;

        pgxc_node_flush(handle);
    }

    return coord_handles;
}

static void CheckBarrierCommandStatus(PGXCNodeAllHandles* conn_handles, const char* id, const char* command)
{
    int conn;
    int count = conn_handles->co_conn_count + conn_handles->dn_conn_count;

    elog(DEBUG1, "Check CREATE BARRIER <%s> %s command status", id, command);

    for (conn = 0; conn < count; conn++) {
        PGXCNodeHandle* handle = NULL;

        if (conn < conn_handles->co_conn_count)
            handle = conn_handles->coord_handles[conn];
        else
            handle = conn_handles->datanode_handles[conn - conn_handles->co_conn_count];

        if (pgxc_node_receive(1, &handle, NULL))
            ereport(
                ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("Failed to receive response from the remote side")));

        if (handle_response(handle, NULL) != RESPONSE_BARRIER_OK)
            ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                    errmsg("CREATE BARRIER PREPARE command failed "
                           "with error %s",
                        handle->error)));
    }

    elog(DEBUG1,
        "Successfully completed CREATE BARRIER <%s> %s command on "
        "all nodes",
        id,
        command);
}

static void SendBarrierEndRequest(PGXCNodeAllHandles* coord_handles, const char* id)
{
    int conn;
    int msglen;
    int barrier_idlen;

    elog(DEBUG1, "Sending CREATE BARRIER <%s> END command to all Coordinators", id);

    for (conn = 0; conn < coord_handles->co_conn_count; conn++) {
        PGXCNodeHandle* handle = coord_handles->coord_handles[conn];

        /* Invalid connection state, return error */
        if (handle->state != DN_CONNECTION_STATE_IDLE)
            ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                    errmsg("Failed to send CREATE BARRIER PREPARE request "
                           "to the node")));

        barrier_idlen = strlen(id) + 1;

        msglen = 4; /* for the length itself */
        msglen += barrier_idlen;
        msglen += 1; /* for barrier command itself */

        /* msgType + msgLen */
        if (ensure_out_buffer_capacity(handle->outEnd + 1 + msglen, handle) != 0) {
            ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("Out of memory")));
        }

        handle->outBuffer[handle->outEnd++] = 'b';
        msglen = htonl(msglen);
        int rc = memcpy_s(handle->outBuffer + handle->outEnd, handle->outSize - handle->outEnd, &msglen, 4);
        securec_check(rc, "\0", "\0");
        handle->outEnd += 4;

        handle->outBuffer[handle->outEnd++] = CREATE_BARRIER_END;

        rc = memcpy_s(handle->outBuffer + handle->outEnd, handle->outSize - handle->outEnd, id, barrier_idlen);
        securec_check(rc, "\0", "\0");
        handle->outEnd += barrier_idlen;

        handle->state = DN_CONNECTION_STATE_QUERY;
        pgxc_node_flush(handle);
    }
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
static PGXCNodeAllHandles* PrepareBarrier(const char* id)
{
    PGXCNodeAllHandles* coord_handles = NULL;

    elog(DEBUG1, "Preparing Coordinators for BARRIER");

    /*
     * Send a CREATE BARRIER PREPARE message to all the Coordinators. We should
     * send an asynchronous request so that we can disable local commits and
     * then wait for the remote Coordinators to finish the work
     */
    coord_handles = SendBarrierPrepareRequest(GetAllCoordNodes(), id);

    /*
     * Disable local commits
     */
    LWLockAcquire(BarrierLock, LW_EXCLUSIVE);

    elog(DEBUG2, "Disabled 2PC commits originating at the driving Coordinator");

    /*
     * TODO Start a timer to cancel the barrier request in case of a timeout
     */

    /*
     * Local in-flight commits are now over. Check status of the remote
     * Coordinators
     */
    CheckBarrierCommandStatus(coord_handles, id, "PREPARE");

    return coord_handles;
}

/*
 * Execute the barrier command on all the components, including Datanodes and
 * Coordinators.
 */
static void ExecuteBarrier(const char* id)
{
    List* barrierDataNodeList = GetAllDataNodes();
    List* barrierCoordList = GetAllCoordNodes();
    PGXCNodeAllHandles* conn_handles;
    int conn;
    int msglen;
    int barrier_idlen;

    conn_handles = get_handles(barrierDataNodeList, barrierCoordList, false);

    elog(DEBUG1,
        "Sending CREATE BARRIER <%s> EXECUTE message to "
        "Datanodes and Coordinator",
        id);
    /*
     * Send a CREATE BARRIER request to all the Datanodes and the Coordinators
     */
    for (conn = 0; conn < conn_handles->co_conn_count + conn_handles->dn_conn_count; conn++) {
        PGXCNodeHandle* handle = NULL;

        if (conn < conn_handles->co_conn_count)
            handle = conn_handles->coord_handles[conn];
        else
            handle = conn_handles->datanode_handles[conn - conn_handles->co_conn_count];

        /* Invalid connection state, return error */
        if (handle->state != DN_CONNECTION_STATE_IDLE)
            ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                    errmsg("Failed to send CREATE BARRIER EXECUTE request "
                           "to the node")));

        barrier_idlen = strlen(id) + 1;

        msglen = 4; /* for the length itself */
        msglen += barrier_idlen;
        msglen += 1; /* for barrier command itself */

        /* msgType + msgLen */
        if (ensure_out_buffer_capacity(handle->outEnd + 1 + msglen, handle) != 0) {
            ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("Out of memory")));
        }

        handle->outBuffer[handle->outEnd++] = 'b';
        msglen = htonl(msglen);
        int rc = memcpy_s(handle->outBuffer + handle->outEnd, handle->outSize - handle->outEnd, &msglen, 4);
        securec_check(rc, "\0", "\0");
        handle->outEnd += 4;

        handle->outBuffer[handle->outEnd++] = CREATE_BARRIER_EXECUTE;

        rc = memcpy_s(handle->outBuffer + handle->outEnd, handle->outSize - handle->outEnd, id, barrier_idlen);
        securec_check(rc, "\0", "\0");
        handle->outEnd += barrier_idlen;

        handle->state = DN_CONNECTION_STATE_QUERY;
        pgxc_node_flush(handle);
    }

    CheckBarrierCommandStatus(conn_handles, id, "EXECUTE");

    pfree_pgxc_all_handles(conn_handles);

    /*
     * Also WAL log the BARRIER locally and flush the WAL buffers to disk
     */
    {
        XLogRecData rdata[1];
        XLogRecPtr recptr;

        rdata[0].data = (char*)id;
        rdata[0].len = strlen(id) + 1;
        rdata[0].buffer = InvalidBuffer;
        rdata[0].next = NULL;

        recptr = XLogInsert(RM_BARRIER_ID, XLOG_BARRIER_CREATE, rdata);
        XLogWaitFlush(recptr);
    }
}

/*
 * Resume 2PC commits on the local as well as remote Coordinators.
 */
static void EndBarrier(PGXCNodeAllHandles* prepared_handles, const char* id)
{
    /* Resume 2PC locally */
    LWLockRelease(BarrierLock);

    SendBarrierEndRequest(prepared_handles, id);

    CheckBarrierCommandStatus(prepared_handles, id, "END");
}

static void WriteBarrierLSNFile(XLogRecPtr barrierLSN)
{
    FILE* fp = NULL;

    fp = AllocateFile(BARRIER_LSN_FILE, PG_BINARY_W);
    if (fp == NULL)
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not create file \"%s\": %m", BARRIER_LSN_FILE)));

    if (fprintf(fp, "%08X/%08X", (uint32)(barrierLSN >> 32), (uint32)barrierLSN) != BARRIER_LSN_FILE_LENGTH ||
        fflush(fp) != 0 || pg_fsync(fileno(fp)) != 0 || ferror(fp) || FreeFile(fp))
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not write file \"%s\": %m", BARRIER_LSN_FILE)));
}

#endif

