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
 * barrier_creator.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/process/postmaster/barrier_creator.cpp
 *
 * -------------------------------------------------------------------------
 */

#define __STDC_FORMAT_MACROS
#include <inttypes.h>
#include <string.h>
#include <stdint.h>
#include "postgres.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "utils/memutils.h"
#include "knl/knl_variable.h"
#include "storage/ipc.h"
#include "pgxc/nodemgr.h"
#include "pgxc/barrier.h"
#include "postmaster/barrier_creator.h"
#include "access/obs/obs_am.h"
#include "tcop/tcopprot.h"
#include "replication/slot.h"
#include "replication/archive_walreceiver.h"
#include "securec.h"
#include "port.h"
#include "utils/postinit.h"
#include "utils/resowner.h"
#include "catalog/pg_database.h"
#include "pgxc/pgxcnode.h"
#include "tcop/utility.h"
#include "pgxc/poolutils.h"
#include "access/gtm.h"

#define TIME_GET_MILLISEC(t) (((long)(t).tv_sec * 1000) + ((long)(t).tv_usec) / 1000)
const int BARRIER_NAME_LEN = 40;
const char* CSN_BARRIER_PATTREN_STR = "csn_%021lu_%013ld";
const char* CSN_SWITCHOVER_BARRIER_PATTREN_STR = "csn_%021lu_dr_switchover";

void GetCsnBarrierName(char* barrierRet, bool isSwitchoverBarrier)
{
    int rc;
    CommitSeqNo csn;
    GTM_Timestamp timestamp;

    if (t_thrd.proc->workingVersionNum >= CSN_TIME_BARRIER_VERSION) {
        csn = CommitCSNGTM(false, &timestamp);
    } else {
        csn = CommitCSNGTM(false, NULL);
        struct timeval tv;
        gettimeofday(&tv, NULL);
        timestamp = TIME_GET_MILLISEC(tv);
    }

    if (isSwitchoverBarrier) {
        rc = snprintf_s(barrierRet, BARRIER_NAME_LEN, BARRIER_NAME_LEN - 1, CSN_SWITCHOVER_BARRIER_PATTREN_STR, csn);
    } else {
        rc = snprintf_s(barrierRet, BARRIER_NAME_LEN, BARRIER_NAME_LEN - 1, CSN_BARRIER_PATTREN_STR, csn, timestamp);
    }
    securec_check_ss_c(rc, "\0", "\0");
    elog(DEBUG1, "GetCsnBarrierName csn = %lu, barrier_name = %s", csn, barrierRet);
}

CommitSeqNo CsnBarrierNameGetCsn(const char *csnBarrier)
{
    CommitSeqNo csn;
    long ts = 0;
    if ((strstr(csnBarrier, "_dr_switchover") != NULL &&
        sscanf_s(csnBarrier, CSN_SWITCHOVER_BARRIER_PATTREN_STR, &csn) == 1) ||
        sscanf_s(csnBarrier, CSN_BARRIER_PATTREN_STR, &csn, &ts) == 2) {
        return csn;
    }
    return 0;
}

int64 CsnBarrierNameGetTimeStamp(const char *csnBarrier)
{
    CommitSeqNo csn;
    int64 ts = 0;
    if (sscanf_s(csnBarrier, CSN_BARRIER_PATTREN_STR, &csn, &ts) == 2) {
        return ts;
    }
    return 0;
}

bool IsSwitchoverBarrier(const char *csnBarrier)
{
    if (!IS_CSN_BARRIER(csnBarrier) || (strstr(csnBarrier, "_dr_switchover") == NULL)) {
        return false;
    }
    return true;
}

bool IsFirstCn()
{
    char *firstExecNode = find_first_exec_cn();
    return (strcmp(firstExecNode, g_instance.attr.attr_common.PGXCNodeName) == 0);
}

void barrier_creator_thread_shutdown(void)
{
    g_instance.barrier_creator_cxt.stop = true;
    ereport(LOG, (errmsg("[BarrierCreator] barrier creator thread shutting down.")));
}

static void barrier_creator_sighup_handler(SIGNAL_ARGS)
{
    int save_errno = errno;
    t_thrd.barrier_creator_cxt.got_SIGHUP = true;
    errno = save_errno;
}

/* Reset some signals that are accepted by postmaster but not here */
static void barrier_creator_setup_signal_hook(void)
{
    (void)gspqsignal(SIGHUP, barrier_creator_sighup_handler);
    (void)gspqsignal(SIGINT, SIG_IGN);
    (void)gspqsignal(SIGTERM, die);
    (void)gspqsignal(SIGQUIT, quickdie);
    (void)gspqsignal(SIGALRM, SIG_IGN);
    (void)gspqsignal(SIGPIPE, SIG_IGN);
    (void)gspqsignal(SIGUSR1, procsignal_sigusr1_handler);
    (void)gspqsignal(SIGUSR2, SIG_IGN);

    (void)gspqsignal(SIGCHLD, SIG_DFL);
    (void)gspqsignal(SIGTTIN, SIG_DFL);
    (void)gspqsignal(SIGTTOU, SIG_DFL);
    (void)gspqsignal(SIGCONT, SIG_DFL);
    (void)gspqsignal(SIGWINCH, SIG_DFL);
    (void)gspqsignal(SIGURG, print_stack);

    /* We allow SIGQUIT (quickdie) at all times */
    (void)sigdelset(&t_thrd.libpq_cxt.BlockSig, SIGQUIT);
}

static uint64_t read_barrier_id_from_obs(const char *slotName, long *currBarrierTime)
{
    char barrier_name[BARRIER_NAME_LEN];
    int ret;
    uint64_t barrier_id;

    LWLockAcquire(DropArchiveSlotLock, LW_SHARED);
    if (ArchiveReplicationReadFile(BARRIER_FILE, (char *)barrier_name, MAX_BARRIER_ID_LENGTH, slotName)) {
        LWLockRelease(DropArchiveSlotLock);
        barrier_name[BARRIER_NAME_LEN - 1] = '\0';
        ereport(LOG, (errmsg("[BarrierCreator] read barrier id from obs %s", barrier_name)));
    } else {
        LWLockRelease(DropArchiveSlotLock);
        ereport(LOG, (errmsg("[BarrierCreator] failed to read barrier id from obs, start barrier from 0")));
        return 0;
    }

#ifdef ENABLE_MULTIPLE_NODES
    ret = sscanf_s(barrier_name, "csn_%021" PRIu64 "_%013ld", &barrier_id, currBarrierTime);
#else
    ret = sscanf_s(barrier_name, "hadr_%020" PRIu64 "_%013ld", &barrier_id, currBarrierTime);
#endif

    if (ret == 2) {
        barrier_id++;
        return barrier_id;
    }
    return 0;
}

uint64_t GetObsBarrierIndex(const List *archiveSlotNames, long *last_barrier_time)
{
    uint64_t maxIndex = 0;
    long maxBarrierTime = 0;
    foreach_cell(cell, archiveSlotNames) {
        long currBarrierTime = 0;
        char* slotName = (char*)lfirst(cell);
        if (slotName == NULL || strlen(slotName) == 0) {
            continue;
        }
        uint64_t readIndex = read_barrier_id_from_obs(slotName, &currBarrierTime);
        maxIndex = (readIndex > maxIndex) ? readIndex : maxIndex;
        maxBarrierTime = (currBarrierTime > maxBarrierTime) ? currBarrierTime : maxBarrierTime;
    }
    *last_barrier_time = maxBarrierTime;

    return maxIndex;
}

uint64 GetObsFirstCNBarrierTimeline(const List *archiveSlotNames)
{
    uint64 timeline = 0;

    foreach_cell(cell, archiveSlotNames) {
        char* slotName = (char*)lfirst(cell);
        if (slotName == NULL || strlen(slotName) == 0) {
            continue;
        }
        LWLockAcquire(DropArchiveSlotLock, LW_SHARED);
        timeline = ReadBarrierTimelineRecordFromObs(slotName);
        LWLockRelease(DropArchiveSlotLock);
        break;
    }
    return timeline;
}

#ifdef ENABLE_MULTIPLE_NODES
static void AllocBarrierLsnInfo(int nodeSize)
{
    int rc;
    g_instance.archive_obs_cxt.barrier_lsn_info = (ArchiveBarrierLsnInfo *)palloc0(
        sizeof(ArchiveBarrierLsnInfo) * nodeSize);
    rc = memset_s(g_instance.archive_obs_cxt.barrier_lsn_info, 
        sizeof(ArchiveBarrierLsnInfo) * nodeSize, 0, 
        sizeof(ArchiveBarrierLsnInfo) * nodeSize);
    securec_check(rc, "", "");
}
#endif

#ifdef ENABLE_MULTIPLE_NODES
static void BarrierCreatorPoolerReload(void) 
{
    destroy_handles();
    processPoolerReload();
    ereport(LOG,
        (errmsg("[BarrierCreatorPoolerReload] Reload connections with CN/DN, dn count : %d, cn count : %d",
            u_sess->pgxc_cxt.NumDataNodes,
            u_sess->pgxc_cxt.NumCoords)));
    if (g_instance.archive_obs_cxt.archive_slot_num == 0) {
        return;
    }

    int maxNodeCnt = *t_thrd.pgxc_cxt.shmemNumCoords + *t_thrd.pgxc_cxt.shmemNumDataNodes;
    if (maxNodeCnt > g_instance.archive_obs_cxt.max_node_cnt) {
        SpinLockAcquire(&g_instance.archive_obs_cxt.barrier_lock);
        g_instance.archive_obs_cxt.max_node_cnt = 0;
        SpinLockRelease(&g_instance.archive_obs_cxt.barrier_lock);
        int nodeSize = maxNodeCnt;
        if (g_instance.archive_obs_cxt.barrier_lsn_info != NULL) {
            pfree_ext(g_instance.archive_obs_cxt.barrier_lsn_info);
        }
        AllocBarrierLsnInfo(nodeSize);
        SpinLockAcquire(&g_instance.archive_obs_cxt.barrier_lock);
        g_instance.archive_obs_cxt.max_node_cnt = nodeSize;
        SpinLockRelease(&g_instance.archive_obs_cxt.barrier_lock);
    }
}
#endif

static void FreeBarrierLsnInfo()
{
    SpinLockAcquire(&g_instance.archive_obs_cxt.barrier_lock);
    g_instance.archive_obs_cxt.max_node_cnt = 0;
    SpinLockRelease(&g_instance.archive_obs_cxt.barrier_lock);
    pfree_ext(g_instance.archive_obs_cxt.barrier_lsn_info);
}

void barrier_creator_main(void)
{
    uint64_t index = 0;
    long last_barrier_time = 0;
    struct timeval tv;
    int rc;
    char barrier_name[BARRIER_NAME_LEN];
    List* archiveSlotNames;
    MemoryContext barrier_creator_context;
    sigjmp_buf local_sigjmp_buf;
    t_thrd.barrier_creator_cxt.is_first_barrier = true;
    char username[NAMEDATALEN];
    char *dbname = (char *)pstrdup(DEFAULT_DATABASE);
    bool startCsnBarrier = g_instance.attr.attr_storage.auto_csn_barrier;
    // use InnerMaintenanceTools mode to avoid deadlock with thread pool
    u_sess->proc_cxt.IsInnerMaintenanceTools  = true;
    u_sess->proc_cxt.IsNoMaskingInnerTools = true;
    ereport(LOG, (errmsg("[BarrierCreator] barrier creator started")));
    g_instance.archive_obs_cxt.max_node_cnt = 0;
    SetProcessingMode(InitProcessing);

    t_thrd.role = BARRIER_CREATOR;
    t_thrd.proc_cxt.MyProgName = "BarrierCreator";
    t_thrd.proc_cxt.MyProcPid = gs_thread_self();
    u_sess->attr.attr_common.application_name = pstrdup("BarrierCreator");
    g_instance.barrier_creator_cxt.stop = false;

    on_shmem_exit(PGXCNodeCleanAndRelease, 0);

    barrier_creator_setup_signal_hook();

    BaseInit();

    /*
     * Unblock signals (they were blocked when the postmaster forked us)
     */
    gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);
    (void)gs_signal_unblock_sigusr2();

    t_thrd.proc_cxt.PostInit->SetDatabaseAndUser(dbname, InvalidOid, username);
    t_thrd.proc_cxt.PostInit->InitBarrierCreator();

    t_thrd.utils_cxt.CurrentResourceOwner = ResourceOwnerCreate(NULL, "BarrierCreator",
        THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE));

    /*
     * Create a memory context that we will do all our work in.  We do this so
     * that we can reset the context during error recovery and thereby avoid
     * possible memory leaks.  Formerly this code just ran in
     * t_thrd.top_mem_cxt, but resetting that would be a really bad idea.
     */
    barrier_creator_context = AllocSetContextCreate(t_thrd.top_mem_cxt,
        "BarrierCreator",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);
    MemoryContextSwitchTo(barrier_creator_context);

    /*
     * If an exception is encountered, processing resumes here.
     * See notes in postgres.c about the design of this coding.
     */
    int curTryCounter;
    int *oldTryCounter = NULL;
    if (sigsetjmp(local_sigjmp_buf, 1) != 0) {
        destroy_handles();
        gstrace_tryblock_exit(true, oldTryCounter);

        /* Since not using PG_TRY, must reset error stack by hand */
        t_thrd.log_cxt.error_context_stack = NULL;

        t_thrd.log_cxt.call_stack = NULL;

        /* Prevent interrupts while cleaning up */
        HOLD_INTERRUPTS();

        /* Report the error to the server log */
        EmitErrorReport();

        /* release resource held by lsc */
        AtEOXact_SysDBCache(false);

        /* release resource */
        LWLockReleaseAll();

        FreeBarrierLsnInfo();
        /*
         * Now return to normal top-level context and clear ErrorContext for
         * next time.
         */
        MemoryContextSwitchTo(barrier_creator_context);
        FlushErrorState();
        MemoryContextResetAndDeleteChildren(barrier_creator_context);

        /* Now we can allow interrupts again */
        RESUME_INTERRUPTS();
        return;
    }
    oldTryCounter = gstrace_tryblock_entry(&curTryCounter);
    /* We can now handle ereport(ERROR) */
    t_thrd.log_cxt.PG_exception_stack = &local_sigjmp_buf;

    SetProcessingMode(NormalProcessing);
    pgstat_report_activity(STATE_IDLE, NULL);

    exec_init_poolhandles();

#ifdef ENABLE_MULTIPLE_NODES
    /*
     * Ensure all barrier commond execuet on first coordinator
     */
    do {
        if (IsFirstCn())
            break;

        ereport(DEBUG1, (errmsg("[BarrierCreator] Current node is not first node: %s",
            g_instance.attr.attr_common.PGXCNodeName)));
        if (IsGotPoolReload()) {
            BarrierCreatorPoolerReload();
            ResetGotPoolReload(false);
        }
        CHECK_FOR_INTERRUPTS();
        pg_usleep(1000000L);
    } while (1);
#endif
    ereport(DEBUG1,
        (errmsg("[BarrierCreator] Init connections with CN/DN, dn count : %d, cn count : %d",
            u_sess->pgxc_cxt.NumDataNodes, u_sess->pgxc_cxt.NumCoords)));
    ereport(LOG, (errmsg("[BarrierCreator] %s is barrier creator", g_instance.attr.attr_common.PGXCNodeName)));
    g_instance.barrier_creator_cxt.stop = false;
    if (g_instance.archive_obs_cxt.archive_slot_num != 0) {
        t_thrd.barrier_creator_cxt.archive_slot_names = GetAllArchiveSlotsName();
        if (t_thrd.barrier_creator_cxt.archive_slot_names == NIL ||
            t_thrd.barrier_creator_cxt.archive_slot_names->length == 0) {
            return;
        }
        index = GetObsBarrierIndex(t_thrd.barrier_creator_cxt.archive_slot_names, &last_barrier_time);
        t_thrd.barrier_creator_cxt.first_cn_timeline =
            GetObsFirstCNBarrierTimeline(t_thrd.barrier_creator_cxt.archive_slot_names);
        /*
         * If the barrier creator node has changed, the time of the current node may be earlier than the barrier time,
         * wait for a while to prevent barrier time rollback.
         */
        do {
            gettimeofday(&tv, NULL);
            long current_time = TIME_GET_MILLISEC(tv);
            if (last_barrier_time < current_time) {
                break;
            }
            long time_diff = last_barrier_time - current_time;
            ereport(LOG, (errmsg("[BarrierCreator] current time %ld is smaller than barrier time %ld, and sleep %ld ms",
                                 current_time, last_barrier_time, time_diff)));
            CHECK_FOR_INTERRUPTS();
            pg_usleep(time_diff * 1000L);
        } while (1);
        if (t_thrd.barrier_creator_cxt.is_first_barrier) {
            gettimeofday(&tv, NULL);
            LWLockAcquire(DropArchiveSlotLock, LW_SHARED);
            WriteGlobalBarrierListStartTimeOnMedia(TIME_GET_MILLISEC(tv));
            LWLockRelease(DropArchiveSlotLock);
        }
#ifdef ENABLE_MULTIPLE_NODES
        while (!START_AUTO_CSN_BARRIER) {
            CHECK_FOR_INTERRUPTS();
            pg_usleep(1000000L);
        }
#endif
    }

#ifdef ENABLE_MULTIPLE_NODES
    CleanupBarrierLock();
#endif

    TimestampTz preCreatedTime = GetCurrentTimestamp();
    while (!g_instance.barrier_creator_cxt.stop) {
        if (t_thrd.barrier_creator_cxt.got_SIGHUP) {
            t_thrd.barrier_preparse_cxt.got_SIGHUP = false;
            ProcessConfigFile(PGC_SIGHUP);
            startCsnBarrier = g_instance.attr.attr_storage.auto_csn_barrier;
        }

        /* in hadr switchover, barrier creator thread stop creating new barriers during service truncate.*/
        if (g_instance.archive_obs_cxt.archive_slot_num != 0 &&
            g_instance.archive_obs_cxt.in_service_truncate == true) {
            continue;
        }
        if (g_instance.archive_obs_cxt.archive_slot_num != 0) {
            if (t_thrd.barrier_creator_cxt.barrier_update_last_time_info == NULL) {
                t_thrd.barrier_creator_cxt.barrier_update_last_time_info = (BarrierUpdateLastTimeInfo*)palloc0(
                    sizeof(BarrierUpdateLastTimeInfo) * g_instance.attr.attr_storage.max_replication_slots);
            }
#ifdef ENABLE_MULTIPLE_NODES
            if (g_instance.archive_obs_cxt.barrier_lsn_info == NULL) {
                int nodeSize = *t_thrd.pgxc_cxt.shmemNumCoords + *t_thrd.pgxc_cxt.shmemNumDataNodes;
                AllocBarrierLsnInfo(nodeSize);
                SpinLockAcquire(&g_instance.archive_obs_cxt.barrier_lock);
                g_instance.archive_obs_cxt.max_node_cnt = nodeSize;
                SpinLockRelease(&g_instance.archive_obs_cxt.barrier_lock);
            }
#endif
            archiveSlotNames = GetAllArchiveSlotsName();
            if (archiveSlotNames == NIL || archiveSlotNames->length == 0) {
                ereport(WARNING, (errmsg("[BarrierCreator] could not get archive slot name when barrier start")));
                return;
            }
            if (t_thrd.barrier_creator_cxt.archive_slot_names == NIL) {
                t_thrd.barrier_creator_cxt.archive_slot_names = archiveSlotNames;
                t_thrd.barrier_creator_cxt.first_cn_timeline =
                    GetObsFirstCNBarrierTimeline(t_thrd.barrier_creator_cxt.archive_slot_names);
            }
            if (archiveSlotNames->length > t_thrd.barrier_creator_cxt.archive_slot_names->length) {
                list_free_deep(t_thrd.barrier_creator_cxt.archive_slot_names);
                t_thrd.barrier_creator_cxt.archive_slot_names = archiveSlotNames;
                t_thrd.barrier_creator_cxt.is_first_barrier = true;
                gettimeofday(&tv, NULL);
                LWLockAcquire(DropArchiveSlotLock, LW_SHARED);
                WriteGlobalBarrierListStartTimeOnMedia(TIME_GET_MILLISEC(tv));
                LWLockRelease(DropArchiveSlotLock);
            } else if (archiveSlotNames->length < t_thrd.barrier_creator_cxt.archive_slot_names->length) {
                list_free_deep(t_thrd.barrier_creator_cxt.archive_slot_names);
                t_thrd.barrier_creator_cxt.archive_slot_names = archiveSlotNames;
            } else if (t_thrd.barrier_creator_cxt.archive_slot_names != archiveSlotNames) {
                list_free_deep(archiveSlotNames);
            }
        }
        long timeDiff = GetCurrentTimestamp() - preCreatedTime;
        if (timeDiff <= 500000L) {
            pg_usleep_retry(500000L - timeDiff, 0);
        }
        preCreatedTime = GetCurrentTimestamp();
        if (!startCsnBarrier && g_instance.archive_obs_cxt.archive_slot_num == 0) {
            g_instance.barrier_creator_cxt.stop = true;
            for (int i = 0; i < g_instance.attr.attr_storage.max_replication_slots; i++) {
                if (g_instance.archive_thread_info.obsBarrierArchPID[i] != 0) {
                    signal_child(g_instance.archive_thread_info.obsBarrierArchPID[i], SIGUSR2, -1);
                }
            }
            break;
        }
        gettimeofday(&tv, NULL);
        /* create barrier with increasing index */

#ifdef ENABLE_MULTIPLE_NODES
        if (IsGotPoolReload()) {
            BarrierCreatorPoolerReload();
            ResetGotPoolReload(false);
            if (!IsFirstCn())
                break;
        }

        ereport(DEBUG1, (errmsg("[BarrierCreator] auto_csn_barrier: %d", startCsnBarrier)));
        if (startCsnBarrier) {
            rc = snprintf_s(barrier_name, BARRIER_NAME_LEN, BARRIER_NAME_LEN - 1, CSN_BARRIER_NAME);
            securec_check_ss_c(rc, "\0", "\0");

            RequestBarrier(barrier_name, NULL);
            ereport(LOG, (errmsg("[BarrierCreator]barrier %s created", barrier_name)));
        }
#else
        rc = snprintf_s(barrier_name, BARRIER_NAME_LEN, BARRIER_NAME_LEN - 1, "hadr_%020" PRIu64 "_%013ld", index,
                        TIME_GET_MILLISEC(tv));
        securec_check_ss_c(rc, "\0", "\0");

        DisasterRecoveryRequestBarrier(barrier_name);
        ereport(LOG, (errmsg("[BarrierCreator]  barrier %s created", barrier_name)));
#endif
        index++;
    }
    ereport(LOG, (errmsg("[BarrierCreator] barrier creator thread exits.")));
    if (t_thrd.barrier_creator_cxt.barrier_update_last_time_info != 0) {
        for (int i = 0; i < g_instance.attr.attr_storage.max_replication_slots; i++) {
            if (t_thrd.barrier_creator_cxt.barrier_update_last_time_info[i].archiveSlotName != NULL) {
                pfree_ext(t_thrd.barrier_creator_cxt.barrier_update_last_time_info[i].archiveSlotName);
            }
        }
        pfree_ext(t_thrd.barrier_creator_cxt.barrier_update_last_time_info);
    }
    destroy_handles();
    FreeBarrierLsnInfo();
    return;
}
