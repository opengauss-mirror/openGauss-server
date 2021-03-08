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
#include "utils/memutils.h"
#include "knl/knl_variable.h"
#include "storage/ipc.h"
#include "pgxc/nodemgr.h"
#include "pgxc/barrier.h"
#include "postmaster/barrier_creator.h"
#include "access/obs/obs_am.h"
#include "tcop/tcopprot.h"
#include "replication/slot.h"
#include "replication/obswalreceiver.h"
#include "securec.h"
#include "port.h"
#include "utils/postinit.h"
#include "utils/resowner.h"
#include "catalog/pg_database.h"
#include "pgxc/pgxcnode.h"

const int BARRIER_NAME_LEN = 40;
#define TIME_GET_MILLISEC(t) (((long)(t).tv_sec * 1000) + ((long)(t).tv_usec) / 1000)

#define BARRIER_FILE "hadr_barrier_id"

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

    /* We allow SIGQUIT (quickdie) at all times */
    (void)sigdelset(&t_thrd.libpq_cxt.BlockSig, SIGQUIT);
}

static void write_barrier_id_to_obs(const char* barrier_name)
{
    obsWrite(BARRIER_FILE, barrier_name, BARRIER_NAME_LEN - 1);
}

static uint64_t read_barrier_id_from_obs(const ReplicationSlot *obs_archive_slot)
{
    char barrier_name[BARRIER_NAME_LEN];
    int ret;
    uint64_t barrier_id;
    long ts = 0;

    if (obs_replication_read_barrier(BARRIER_FILE, (char *)barrier_name)) {
        barrier_name[BARRIER_NAME_LEN - 1] = '\0';
        ereport(LOG, (errmsg("[BarrierCreator] read barrier id from obs %s", barrier_name)));
    } else {
        ereport(LOG, (errmsg("[BarrierCreator] failed to read barrier id from obs, start barrier from 0")));
        return 0;
    }

    ret = sscanf_s(barrier_name, "hadr_%020" PRIu64 "_%013ld", &barrier_id, &ts);
    if (ret == 2) {
        barrier_id++;
        return barrier_id;
    }
    return 0;
}

static void BarrierPoolerReload(void) 
{
    MemoryContext oldContext;
    
    /* Now session information is reset in correct memory context */
    oldContext = MemoryContextSwitchTo(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_COMMUNICATION));

    /* Reinitialize session, while old pooler connection is active */
    InitMultinodeExecutor(true);
    /* And reconnect to pool manager */
    PoolManagerReconnect();

    MemoryContextSwitchTo(oldContext);
    ereport(LOG,
        (errmsg("[barrierPoolerReload] Reload connections with CN/DN, dn count : %d, cn count : %d",
            u_sess->pgxc_cxt.NumDataNodes,
            u_sess->pgxc_cxt.NumCoords)));
}

void barrier_creator_main(void)
{
    ReplicationSlot *obs_archive_slot = NULL;
    uint64_t index = 0;
    char barrier_name[BARRIER_NAME_LEN];
    MemoryContext barrier_creator_context;
    sigjmp_buf local_sigjmp_buf;
    int rc;
    struct timeval tv;
    char username[NAMEDATALEN];
    char *dbname = (char *)pstrdup(DEFAULT_DATABASE);

    ereport(LOG, (errmsg("[BarrierCreator] barrier creator thread starts.")));
    SetProcessingMode(InitProcessing);

    t_thrd.role = BARRIER_CREATOR;
    t_thrd.proc_cxt.MyProgName = "BarrierCreator";
    t_thrd.proc_cxt.MyProcPid = gs_thread_self();
    u_sess->attr.attr_common.application_name = pstrdup("BarrierCreator");
    g_instance.barrier_creator_cxt.stop = false;

    barrier_creator_setup_signal_hook();

    BaseInit();
    
    t_thrd.proc_cxt.PostInit->SetDatabaseAndUser(dbname, InvalidOid, username);
    t_thrd.proc_cxt.PostInit->InitBarrierCreator();

    t_thrd.utils_cxt.CurrentResourceOwner = ResourceOwnerCreate(NULL, "BarrierCreator", MEMORY_CONTEXT_STORAGE);

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
    int* oldTryCounter = NULL;
    if (sigsetjmp(local_sigjmp_buf, 1) != 0) {
        gstrace_tryblock_exit(true, oldTryCounter);

        /* Since not using PG_TRY, must reset error stack by hand */
        t_thrd.log_cxt.error_context_stack = NULL;

        /* Prevent interrupts while cleaning up */
        HOLD_INTERRUPTS();

        /* Report the error to the server log */
        EmitErrorReport();

        /* release resource */
        LWLockReleaseAll();

        /*
         * Now return to normal top-level context and clear ErrorContext for
         * next time.
         */
        MemoryContextSwitchTo(barrier_creator_context);
        FlushErrorState();
        MemoryContextResetAndDeleteChildren(barrier_creator_context);

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
    t_thrd.log_cxt.PG_exception_stack = &local_sigjmp_buf;

    /*
     * Unblock signals (they were blocked when the postmaster forked us)
     */
    gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);
    (void)gs_signal_unblock_sigusr2();

    int count = 0;
    while (count++ < 10) {
        if (g_instance.barrier_creator_cxt.stop) {
            return;
        }
        pg_usleep(USECS_PER_SEC);
    }
    
    exec_init_poolhandles();
    ereport(LOG,
        (errmsg("[BarrierCreator] Init connections with CN/DN, dn count : %d, cn count : %d",
            u_sess->pgxc_cxt.NumDataNodes,
            u_sess->pgxc_cxt.NumCoords)));

    SetProcessingMode(NormalProcessing);
    obs_archive_slot = getObsReplicationSlot();
    if (obs_archive_slot == NULL) {
        g_instance.barrier_creator_cxt.stop = true;
        ereport(LOG, (errmsg("[BarrierCreator] barrier creator thread exits.")));
        return;
    }
    g_instance.barrier_creator_cxt.stop = false;
    index = read_barrier_id_from_obs(obs_archive_slot);

    while (!g_instance.barrier_creator_cxt.stop) {
        pg_usleep_retry(1000000L, 0);
        if (!XLogArchivingActive()) {
            g_instance.barrier_creator_cxt.stop = true;
            break;
        }

        if (u_sess->sig_cxt.got_PoolReload) {
            BarrierPoolerReload();
            u_sess->sig_cxt.got_PoolReload = false;
        }
        gettimeofday(&tv, NULL);

        /* create barrier with increasing index */
        ereport(LOG, (errmsg("[BarrierCreator] %s is barrier creator", g_instance.attr.attr_common.PGXCNodeName)));
        rc = snprintf_s(barrier_name, BARRIER_NAME_LEN, BARRIER_NAME_LEN - 1, "hadr_%020" PRIu64 "_%013ld", index, 
            TIME_GET_MILLISEC(tv));
        securec_check_ss_c(rc, "\0", "\0");
        ereport(LOG, (errmsg("[BarrierCreator] creating barrier %s", barrier_name)));
#ifdef ENABLE_MULTIPLE_NODES
        RequestBarrier(barrier_name, NULL);
#else
        DisasterRecoveryRequestBarrier(barrier_name);
#endif
        write_barrier_id_to_obs(barrier_name);
        index++;
    }
    ereport(LOG, (errmsg("[BarrierCreator] barrier creator thread exits.")));
}
