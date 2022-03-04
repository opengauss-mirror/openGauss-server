/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
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
 * barrier_arch.cpp
 *    Wait barrier arch.
 *
 * IDENTIFICATION
 *        src/gausskernel/process/postmaster/barrier_arch.cpp
 *
 * -------------------------------------------------------------------------
 */

#define __STDC_FORMAT_MACROS
#include <inttypes.h>
#include <string.h>
#include <stdint.h>
#include "postgres.h"
#include "miscadmin.h"
#include "utils/memutils.h"
#include "knl/knl_variable.h"
#include "storage/ipc.h"
#include "pgxc/nodemgr.h"
#include "pgxc/barrier.h"
#include "postmaster/barrier_creator.h"
#include "access/obs/obs_am.h"
#include "access/archive/archive_am.h"
#include "tcop/tcopprot.h"
#include "replication/slot.h"
#include "replication/archive_walreceiver.h"
#include "securec.h"
#include "port.h"
#include "utils/postinit.h"
#include "utils/resowner.h"
#include "catalog/pg_database.h"
#include "pgxc/pgxcnode.h"
#include "libpq/pqformat.h"
#include "libpq/libpq.h"
#include "pgxc/execRemote.h"
#include "pgxc/poolutils.h"

#define atolsn(x) ((XLogRecPtr)strtoul((x), NULL, 0))

static void write_barrier_id_to_obs(const char* barrier_name, ArchiveConfig *archive_obs)
{
    errno_t rc = 0;
    ArchiveConfig obsConfig;
    char pathPrefix[MAXPGPATH] = {0};

    ereport(LOG, (errmsg("Write barrierId <%s> to obs start", barrier_name)));

    /* copy OBS configs to temporary variable for customising file path */
    rc = memcpy_s(&obsConfig, sizeof(ArchiveConfig), archive_obs, sizeof(ArchiveConfig));
    securec_check(rc, "", "");

    if (!IS_PGXC_COORDINATOR) {
        rc = strcpy_s(pathPrefix, MAXPGPATH, obsConfig.archive_prefix);
        securec_check(rc, "\0", "\0");
        char *p = strrchr(pathPrefix, '/');
        if (p == NULL) {
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("Obs path prefix is invalid")));
        }
        *p = '\0';
        obsConfig.archive_prefix = pathPrefix;
    }

    ArchiveWrite(BARRIER_FILE, barrier_name, MAX_BARRIER_ID_LENGTH - 1, &obsConfig);
}

static void WaitBarrierArch(XLogRecPtr barrierLsn, const char *slotName) 
{
    ereport(LOG,
            (errmsg("WaitBarrierArch start: 0x%lx", barrierLsn)));
    
    int cnt = 0;
    const int interval = 100;
    do {
        ArchiveTaskStatus *archive_task_status = NULL;
        archive_task_status = find_archive_task_status(slotName);
        if (NULL == archive_task_status) {
            ereport(ERROR, (errcode(ERRCODE_OPERATE_NOT_SUPPORTED), errmsg("Obs slot <%s> not exist.", slotName)));
        }
        
        if (XLByteLE(pg_atomic_read_u64(&barrierLsn),
            pg_atomic_read_u64(&archive_task_status->archived_lsn))) {
            break;
        }
        
        CHECK_FOR_INTERRUPTS();
        /* Also check stop flag */
        if (t_thrd.barrier_arch.ready_to_stop) {
            ereport(ERROR, (errcode(ERRCODE_ADMIN_SHUTDOWN), errmsg("[BarrierArch] terminating barrier arch"
                " due to administrator command")));
        }
        pg_usleep(100000L);
        if (t_thrd.barrier_arch.lastArchiveLoc == pg_atomic_read_u64(&archive_task_status->archived_lsn)) {
            cnt++;
            if ((cnt % interval) == 0) {
                ereport(WARNING, (errmsg("[WaitBarrierArch] arch thread now archived"
                    " lsn: %08X/%08X, timeout count: %d", (uint32)(t_thrd.barrier_arch.lastArchiveLoc >> 32),
                    (uint32)t_thrd.barrier_arch.lastArchiveLoc, cnt)));
            }
        } else {
            cnt = 0;
        }
        t_thrd.barrier_arch.lastArchiveLoc = pg_atomic_read_u64(&archive_task_status->archived_lsn);
        if (cnt > WAIT_ARCHIVE_TIMEOUT) {
            ereport(ERROR, (errcode(ERRCODE_OPERATE_NOT_SUPPORTED), errmsg("Wait archived timeout.")));
        }
    } while (1);

    ereport(LOG, (errmsg("WaitBarrierArch archive lsn end")));
}

/*
 * Make sure the current BARRIER WAL record has been archived.If not, wait until 
 * the BARRIER WAL record has been archived.
 */
void ProcessBarrierQueryArchive(char* id)
{
    ereport(LOG,
        (errmsg("Receive BARRIER <%s> QUERY message on Coordinator or Datanode", id)));
    
    StringInfoData buf;
    char *slotName;
    char *lsn;

    lsn = strtok_r(id, ":", &slotName);
    if (lsn == NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_OPERATE_NOT_SUPPORTED),
                errmsg("The BARRIER QUERY ARCHIVE target lsn is null")));
    } 
    XLogRecPtr barrierTargetLsn = atolsn(lsn);
    ereport(LOG,
        (errmsg("Receive LSN <%lx> message on Coordinator or Datanode", barrierTargetLsn)));
    
    ereport(LOG,
        (errmsg("Receive BARRIER QUERY message slotname: %s", slotName)));
    
    if (!IsConnFromCoord())
        ereport(ERROR,
            (errcode(ERRCODE_OPERATE_NOT_SUPPORTED),
                errmsg("The BARRIER QUERY ARCHIVE message is expected to "
                       "arrive from a Coordinator")));

    if (!IS_PGXC_COORDINATOR) {
        WaitBarrierArch(barrierTargetLsn, slotName);
    }
    
    pq_beginmessage(&buf, 'b');
    pq_sendstring(&buf, id);
    pq_endmessage(&buf);
    pq_flush();
}

static void BarrierArchWakenStop(SIGNAL_ARGS)
{
    t_thrd.barrier_arch.ready_to_stop = true;
}

static void BarrierArchSighupHandler(SIGNAL_ARGS)
{
    int save_errno = errno;
    t_thrd.barrier_arch.got_SIGHUP = true;
    errno = save_errno;
}

/* Reset some signals that are accepted by postmaster but not here */
static void BarrierArchSetupSignalHook(void)
{
    (void)gspqsignal(SIGHUP, BarrierArchSighupHandler);
    (void)gspqsignal(SIGINT, SIG_IGN);
    (void)gspqsignal(SIGTERM, die);
    (void)gspqsignal(SIGQUIT, quickdie);
    (void)gspqsignal(SIGALRM, SIG_IGN);
    (void)gspqsignal(SIGPIPE, SIG_IGN);
    (void)gspqsignal(SIGUSR1, procsignal_sigusr1_handler);
    (void)gspqsignal(SIGUSR2, BarrierArchWakenStop);

    (void)gspqsignal(SIGCHLD, SIG_DFL);
    (void)gspqsignal(SIGTTIN, SIG_DFL);
    (void)gspqsignal(SIGTTOU, SIG_DFL);
    (void)gspqsignal(SIGCONT, SIG_DFL);
    (void)gspqsignal(SIGWINCH, SIG_DFL);

    /* We allow SIGQUIT (quickdie) at all times */
    (void)sigdelset(&t_thrd.libpq_cxt.BlockSig, SIGQUIT);
}

#ifdef ENABLE_MULTIPLE_NODES
static PGXCNodeAllHandles* GetAllNodesHandles() 
{
    List* barrierDataNodeList = GetAllDataNodes();
    List* barrierCoordList = GetAllCoordNodes();
    PGXCNodeAllHandles* conn_handles = NULL;

    conn_handles = get_handles(barrierDataNodeList, barrierCoordList, false);

    list_free(barrierCoordList);
    list_free(barrierDataNodeList);

    return conn_handles;
}

static void SendBarrierArchRequest(const PGXCNodeAllHandles* handles, int count, ArchiveBarrierLsnInfo *barrierLsnInfo)
{
    int conn;
    int msglen;
    int barrier_idlen;
    errno_t rc;
    char barrierInfo[BARRIER_ARCH_INFO_LEN];

    ereport(LOG, (errmsg("Start to send barrier arch request to all nodes.")));

    for (conn = 0; conn < count; conn++) {
        PGXCNodeHandle* handle = NULL;

        if (conn < handles->co_conn_count)
            handle = handles->coord_handles[conn];
        else
            handle = handles->datanode_handles[conn - handles->co_conn_count];

        /* Invalid connection state, return error */
        if (handle->state != DN_CONNECTION_STATE_IDLE) {
            ereport(ERROR,
                (errcode(ERRCODE_OPERATE_FAILED),
                    errmsg("Failed to send BARRIER request to the node")));
        }

        for (int i = 0; i < count; i++) {
            if (barrierLsnInfo[i].nodeoid == handle->nodeoid) {
                rc = snprintf_s(barrierInfo, BARRIER_ARCH_INFO_LEN, BARRIER_ARCH_INFO_LEN - 1, "0x%lx:%s", 
                    barrierLsnInfo[i].barrierLsn, t_thrd.barrier_arch.slot_name);
            }
        }

        barrier_idlen = strlen(barrierInfo) + 1;

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

        handle->outBuffer[handle->outEnd++] = BARRIER_QUERY_ARCHIVE;

        rc = memcpy_s(handle->outBuffer + handle->outEnd, handle->outSize - handle->outEnd, barrierInfo, barrier_idlen);
        securec_check(rc, "\0", "\0");
        handle->outEnd += barrier_idlen;

        handle->state = DN_CONNECTION_STATE_QUERY;

        pgxc_node_flush(handle);
    }
}

static void CheckBarrierArchCommandStatus(const PGXCNodeAllHandles* conn_handles, int count, const char *id)
{
    int conn;
    RemoteQueryState* combiner = NULL;

    ereport(DEBUG1, (errmsg("Check BARRIER ARCH QUERY <%s> command status", id)));

    combiner = CreateResponseCombiner(count, COMBINE_TYPE_NONE);

    for (conn = 0; conn < count; conn++) {
        PGXCNodeHandle* handle = NULL;

        if (conn < conn_handles->co_conn_count)
            handle = conn_handles->coord_handles[conn];
        else
            handle = conn_handles->datanode_handles[conn - conn_handles->co_conn_count];

        if (pgxc_node_receive(1, &handle, NULL))
            ereport(
                ERROR, (errcode(ERRCODE_OPERATE_FAILED), errmsg("Failed to receive response from the remote side")));
        if (handle_response(handle, combiner) != RESPONSE_BARRIER_OK)
            ereport(ERROR,
                (errcode(ERRCODE_OPERATE_FAILED),
                    errmsg("BARRIER ARCH QUERY failed with error %s", handle->error)));
    }
    CloseCombiner(combiner);

    ereport(LOG,
        (errmsg("Successfully completed BARRIER ARCH QUERY <%s> command on "
                "all nodes",
            id)));
}

static void QueryBarrierArch(PGXCNodeAllHandles* handles, ArchiveConfig *archive_obs)
{
    int connCnt = handles->co_conn_count + handles->dn_conn_count;
    SpinLockAcquire(&g_instance.archive_obs_cxt.barrier_lock);
    int archivMaxNodeCnt = g_instance.archive_obs_cxt.max_node_cnt;
    if (connCnt >= archivMaxNodeCnt) {
        SpinLockRelease(&g_instance.archive_obs_cxt.barrier_lock);
        ereport(DEBUG2, (errmsg("current cn get connCnt: <%d> max than cluster connCnt: <%d>", 
            connCnt, archivMaxNodeCnt)));
        return;
    }
    
    ArchiveBarrierLsnInfo barrierLsnInfo[g_instance.archive_obs_cxt.max_node_cnt];

    if (strncmp(t_thrd.barrier_arch.barrierName, g_instance.archive_obs_cxt.barrierName, 
        strlen(g_instance.archive_obs_cxt.barrierName)) == 0) {
        SpinLockRelease(&g_instance.archive_obs_cxt.barrier_lock);
        return;
    }

    errno_t errorno = memcpy_s(t_thrd.barrier_arch.barrierName, MAX_BARRIER_ID_LENGTH,
                               g_instance.archive_obs_cxt.barrierName, 
                               sizeof(g_instance.archive_obs_cxt.barrierName));
    securec_check(errorno, "\0", "\0");

    for (int i = 0; i < connCnt + 1; i++) {
        if (g_instance.archive_obs_cxt.barrier_lsn_info[i].barrierLsn == 0x0) {
            SpinLockRelease(&g_instance.archive_obs_cxt.barrier_lock);
            return;
        }
    }
    errorno = memcpy_s(&barrierLsnInfo, sizeof(ArchiveBarrierLsnInfo) * g_instance.archive_obs_cxt.max_node_cnt, 
                       g_instance.archive_obs_cxt.barrier_lsn_info, 
                       sizeof(ArchiveBarrierLsnInfo) * g_instance.archive_obs_cxt.max_node_cnt);
    SpinLockRelease(&g_instance.archive_obs_cxt.barrier_lock);
    securec_check(errorno, "\0", "\0");
    
    SendBarrierArchRequest(handles, connCnt, barrierLsnInfo);

    CheckBarrierArchCommandStatus(handles, connCnt, t_thrd.barrier_arch.barrierName);

    WaitBarrierArch(barrierLsnInfo[connCnt].barrierLsn, t_thrd.barrier_arch.slot_name);

    write_barrier_id_to_obs(t_thrd.barrier_arch.barrierName, archive_obs);
}

#else
static void SingleBarrierArch(ArchiveConfig *archive_obs)
{
    XLogRecPtr barrierLsn;
    
    SpinLockAcquire(&g_instance.archive_obs_cxt.barrier_lock);
    if (strncmp(t_thrd.barrier_arch.barrierName, g_instance.archive_obs_cxt.barrierName, 
        strlen(g_instance.archive_obs_cxt.barrierName)) == 0) {
        SpinLockRelease(&g_instance.archive_obs_cxt.barrier_lock);
        return;
    }
        
    errno_t errorno = memcpy_s(t_thrd.barrier_arch.barrierName, MAX_BARRIER_ID_LENGTH, 
                                g_instance.archive_obs_cxt.barrierName, 
                                sizeof(g_instance.archive_obs_cxt.barrierName));
    securec_check(errorno, "\0", "\0");
    
    barrierLsn = g_instance.archive_obs_cxt.barrierLsn;
    SpinLockRelease(&g_instance.archive_obs_cxt.barrier_lock);

    WaitBarrierArch(barrierLsn, t_thrd.barrier_arch.slot_name);

    write_barrier_id_to_obs(t_thrd.barrier_arch.barrierName, archive_obs);
}
#endif

NON_EXEC_STATIC void BarrierArchMain(knl_thread_arg* arg)
{
    ArchiveSlotConfig *obsArchiveSlot = NULL;
    MemoryContext barrierArchContext;
    sigjmp_buf localSigjmpBuf;
    char username[NAMEDATALEN];
    char *dbname = (char *)pstrdup(DEFAULT_DATABASE);

    SetProcessingMode(InitProcessing);

    t_thrd.role = BARRIER_ARCH;
    t_thrd.proc_cxt.MyProgName = "BarrierArch";
    t_thrd.proc_cxt.MyProcPid = gs_thread_self();
    t_thrd.barrier_arch.slot_name = pstrdup((char *)arg->payload);
    u_sess->attr.attr_common.application_name = pstrdup("BarrierArch");

    ereport(LOG, (errmsg("[BarrierArch] barrier arch thread starts. slot name: %s", t_thrd.barrier_arch.slot_name)));

    on_shmem_exit(PGXCNodeCleanAndRelease, 0);

    BarrierArchSetupSignalHook();

    BaseInit();
    
    t_thrd.proc_cxt.PostInit->SetDatabaseAndUser(dbname, InvalidOid, username);
    t_thrd.proc_cxt.PostInit->InitBarrierCreator();

    t_thrd.utils_cxt.CurrentResourceOwner = ResourceOwnerCreate(NULL, "BarrierArch",
        THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE));

    /*
     * Create a memory context that we will do all our work in.  We do this so
     * that we can reset the context during error recovery and thereby avoid
     * possible memory leaks.  Formerly this code just ran in
     * t_thrd.top_mem_cxt, but resetting that would be a really bad idea.
     */
    barrierArchContext = AllocSetContextCreate(t_thrd.top_mem_cxt,
        "BarrierArch",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);
    MemoryContextSwitchTo(barrierArchContext);

    /*
     * If an exception is encountered, processing resumes here.
     * See notes in postgres.c about the design of this coding.
     */
    int curTryCounter;
    int* oldTryCounter = NULL;
    if (sigsetjmp(localSigjmpBuf, 1) != 0) {
        gstrace_tryblock_exit(true, oldTryCounter);

        /* Since not using PG_TRY, must reset error stack by hand */
        t_thrd.log_cxt.error_context_stack = NULL;

        /* Prevent interrupts while cleaning up */
        HOLD_INTERRUPTS();

        /* Report the error to the server log */
        EmitErrorReport();

        /* release resource held by lsc */
        AtEOXact_SysDBCache(false);

        /* release resource */
        LWLockReleaseAll();

        /*
         * Now return to normal top-level context and clear ErrorContext for
         * next time.
         */
        MemoryContextSwitchTo(barrierArchContext);
        FlushErrorState();
        MemoryContextResetAndDeleteChildren(barrierArchContext);

        /* Now we can allow interrupts again */
        RESUME_INTERRUPTS();

        /*
         * Sleep at least 1 second after any error.  A write error is likely
         * to be repeated, and we don't want to be filling the error logs as
         * fast as we can.
         */
        pg_usleep(1000000L);
    }
    destroy_handles();
    oldTryCounter = gstrace_tryblock_entry(&curTryCounter);
    /* We can now handle ereport(ERROR) */
    t_thrd.log_cxt.PG_exception_stack = &localSigjmpBuf;

    /*
     * Unblock signals (they were blocked when the postmaster forked us)
     */
    gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);
    (void)gs_signal_unblock_sigusr2();

    SetProcessingMode(NormalProcessing);

    pg_usleep_retry(1000000L, 0);
    
    obsArchiveSlot = getArchiveReplicationSlotWithName(t_thrd.barrier_arch.slot_name);
    if (obsArchiveSlot == NULL) {
        t_thrd.barrier_arch.ready_to_stop = true;
        ereport(WARNING, (errmsg("[BarrierArch] obs slot not created.")));
        return;
    }

    exec_init_poolhandles();
#ifdef ENABLE_MULTIPLE_NODES
    do {
        if (IsFirstCn())
            break;
        
        ereport(DEBUG1, (errmsg("[BarrierArch] Current node is not first node: %s",
            g_instance.attr.attr_common.PGXCNodeName)));
        if (IsGotPoolReload()) {
            processPoolerReload();
            ResetGotPoolReload(false);
        }
        CHECK_FOR_INTERRUPTS();
        pg_usleep(10000000L);
    } while (1);

    SpinLockAcquire(&g_instance.archive_obs_cxt.barrier_lock);
    if (g_instance.archive_obs_cxt.barrier_lsn_info == NULL ||
        g_instance.archive_obs_cxt.max_node_cnt == 0) {
        SpinLockRelease(&g_instance.archive_obs_cxt.barrier_lock);
        ereport(WARNING, (errmsg("[BarrierArch] barrier_lsn_info not alloc.")));
        return;
    }
    SpinLockRelease(&g_instance.archive_obs_cxt.barrier_lock);
#endif
    ereport(DEBUG1,
        (errmsg("[BarrierArch] Init connections with CN/DN, dn count : %d, cn count : %d",
            u_sess->pgxc_cxt.NumDataNodes, u_sess->pgxc_cxt.NumCoords)));

    while (!t_thrd.barrier_arch.ready_to_stop) {
        CHECK_FOR_INTERRUPTS();
        
        if (g_instance.archive_obs_cxt.barrierName == NULL || strlen(g_instance.archive_obs_cxt.barrierName) == 0) {
            ereport(WARNING, (errmsg("[BarrierArch] barrierName is null.")));
            break;
        }

#ifdef ENABLE_MULTIPLE_NODES
        if (IsGotPoolReload()) {
            processPoolerReload();
            ResetGotPoolReload(false);
            if (!IsFirstCn())
                break;
        }

        PGXCNodeAllHandles* handles = GetAllNodesHandles();

        QueryBarrierArch(handles, &obsArchiveSlot->archive_config);
        
        pfree_pgxc_all_handles(handles);
#else
        SingleBarrierArch(&obsArchiveSlot->archive_config);
#endif
    }
    ereport(LOG, (errmsg("[BarrierArch] barrier arch thread exits.")));
}
