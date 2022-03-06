/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 * http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * -------------------------------------------------------------------------
 *
 * globalstats.cpp
 *
 * IDENTIFICATION
 * src/gausskernel/process/postmaster/globalstats.cpp
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#include <signal.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>
#ifdef HAVE_SYS_STAT_H
#include <sys/stat.h>
#endif

#include "access/hash.h"
#include "commands/dbcommands.h"
#include "executor/spi.h"
#include "funcapi.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/fork_process.h"
#include "postmaster/postmaster.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/pmsignal.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "storage/procsignal.h"
#include "storage/sinvaladt.h"
#include "utils/postinit.h"
#include "utils/ps_status.h"
#include "utils/timestamp.h"
#include "commands/user.h"
#include "gssignal/gs_signal.h"

static void GlobalstatsSighupHandler(SIGNAL_ARGS);
static void GlobalstatsSigusr2Handler(SIGNAL_ARGS);
static void GlobalstatsSigtermHandler(SIGNAL_ARGS);

/* SIGHUP: set flag to re-read config file at next convenient time */
static void GlobalstatsSighupHandler(SIGNAL_ARGS)
{
    int saveErrno = errno;

    t_thrd.gstat_cxt.got_SIGHUP = true;
    if (t_thrd.proc)
        SetLatch(&t_thrd.proc->procLatch);

    errno = saveErrno;
}

/* SIGUSR2: a worker is up and running, or just finished, or failed to fork */
static void GlobalstatsSigusr2Handler(SIGNAL_ARGS)
{
    int saveErrno = errno;

    t_thrd.gstat_cxt.got_SIGUSR2 = true;
    if (t_thrd.proc)
        SetLatch(&t_thrd.proc->procLatch);

    errno = saveErrno;
}

/* SIGTERM: time to die */
static void GlobalstatsSigtermHandler(SIGNAL_ARGS)
{
    int saveErrno = errno;

    t_thrd.gstat_cxt.got_SIGTERM = true;
    if (t_thrd.proc)
        SetLatch(&t_thrd.proc->procLatch);

    errno = saveErrno;
}

static void PrepareStatsHashForSwitch()
{
    Assert(pg_atomic_read_u64(&g_instance.stat_cxt.tableStat->quiesce) == 0);
    pg_atomic_exchange_u64(&g_instance.stat_cxt.tableStat->quiesce, 1);

    uint64 readers = pg_atomic_read_u64(&g_instance.stat_cxt.tableStat->readers);
    Assert(readers < (uint64) GLOBAL_ALL_PROCS);

    /* Wait until all readers are done */
    while (readers > 0) {
        pg_usleep(10000L);
        readers = pg_atomic_read_u64(&g_instance.stat_cxt.tableStat->readers);
    }
}

static void CompleteStatsHashSwitch()
{
    /* We better be in the correct state */
    Assert(pg_atomic_read_u64(&g_instance.stat_cxt.tableStat->quiesce) == 1);

    pg_atomic_exchange_u64(&g_instance.stat_cxt.tableStat->quiesce, 0);
}

static int MatchDictItem(const void* left, const void* right, Size keysize)
{
    const PgStat_StartBlockTableKey* leftItem = (PgStat_StartBlockTableKey*)left;
    const PgStat_StartBlockTableKey* rightItem = (PgStat_StartBlockTableKey*)right;
    Assert(leftItem != NULL && rightItem != NULL);

    /* we just care whether the result is 0 or not. */
    if (leftItem->relid != rightItem->relid || leftItem->parentid != rightItem->parentid ||
        leftItem->dbid != rightItem->dbid) {
        return 1;
    }

    return 0;
}

static uint32 HashDictItem(const void* key, Size keysize)
{
    return DatumGetUInt32(hash_any((const unsigned char*)key, sizeof(PgStat_StartBlockTableKey)));
}

void GlobalStatsTrackerInit()
{
    /* Setup a shared memory context that other backends can access.
     * This will hold the Global Statistics Hash
     */
    g_instance.stat_cxt.tableStat->global_stats_cxt =
        AllocSetContextCreate((MemoryContext)g_instance.instance_context, "GlobalStatisticsContext",
            ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE, SHARED_CONTEXT);

    HASHCTL hashCtrl;
    errno_t rc = memset_s(&hashCtrl, sizeof(hashCtrl), 0, sizeof(hashCtrl));
    securec_check(rc, "", "");
    hashCtrl.hash = (HashValueFunc)HashDictItem;
    hashCtrl.match = (HashCompareFunc)MatchDictItem;
    hashCtrl.keysize = (Size)sizeof(PgStat_StartBlockTableKey);
    hashCtrl.entrysize = (Size)sizeof(PgStat_StartBlockTableEntry);
    hashCtrl.hcxt = g_instance.stat_cxt.tableStat->global_stats_cxt;
    hashCtrl.num_partitions = NUM_STARTBLOCK_PARTITIONS;

    int flags = (HASH_FUNCTION | HASH_COMPARE | HASH_ELEM | HASH_SHRCTX | HASH_PARTITION);
    g_instance.stat_cxt.tableStat->blocks_map =
        hash_create("Candidate Blocks for Pruning Hash", NUM_STARTBLOCK_PARTITIONS, &hashCtrl, flags);
}

bool IsGlobalStatsTrackerProcess()
{
    return t_thrd.role == GLOBALSTATS_THREAD;
}

NON_EXEC_STATIC void GlobalStatsTrackerMain()
{
    sigjmp_buf localSigjmpBuf;

    /* we are a postmaster subprocess now */
    IsUnderPostmaster = true;
    t_thrd.role = GLOBALSTATS_THREAD;
    /* reset t_thrd.proc_cxt.MyProcPid */
    t_thrd.proc_cxt.MyProcPid = gs_thread_self();
    /* record Start Time for logging */
    t_thrd.proc_cxt.MyStartTime = time(NULL);

    t_thrd.proc_cxt.MyProgName = "StatsTracker";

    Assert(t_thrd.proc->pid == t_thrd.proc_cxt.MyProcPid);
    init_ps_display("global stats process", "", "", "");
    ereport(LOG, (errmsg("global stats collector started")));

    SetProcessingMode(InitProcessing);

    /*
     * Set up signal handlers.  We operate on databases much like a regular
     * backend, so we use the same signal handling.  See equivalent code in
     * tcop/postgres.c.
     */
    gspqsignal(SIGHUP, GlobalstatsSighupHandler);
    gspqsignal(SIGINT, StatementCancelHandler);
    gspqsignal(SIGTERM, GlobalstatsSigtermHandler);

    gspqsignal(SIGQUIT, quickdie);
    gspqsignal(SIGALRM, handle_sig_alarm);

    gspqsignal(SIGPIPE, SIG_IGN);
    gspqsignal(SIGUSR1, procsignal_sigusr1_handler);
    gspqsignal(SIGUSR2, GlobalstatsSigusr2Handler);
    gspqsignal(SIGFPE, FloatExceptionHandler);
    gspqsignal(SIGCHLD, SIG_DFL);

    /* Early initialization */
    BaseInit();

    /*
     * Create a per-backend PGPROC struct in shared memory, except in the
     * EXEC_BACKEND case where this was done in SubPostmasterMain. We must do
     * this before we can use LWLocks (and in the EXEC_BACKEND case we already
     * had to do some stuff with LWLocks).
     */
#ifndef EXEC_BACKEND
    InitProcess();
#endif

    SetProcessingMode(NormalProcessing);

    /* Unblock signals (they were blocked when the postmaster forked us) */
    gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);
    (void)gs_signal_unblock_sigusr2();

    /*
     * If an exception is encountered, processing resumes here.
     *
     * This code is a stripped down version of PostgresMain error recovery.
     */
    if (sigsetjmp(localSigjmpBuf, 1) != 0) {
        /* since not using PG_TRY, must reset error stack by hand */
        t_thrd.log_cxt.error_context_stack = NULL;

        t_thrd.log_cxt.call_stack = NULL;

        /* Prevents interrupts while cleaning up */
        HOLD_INTERRUPTS();

        /* Report the error to the server log */
        EmitErrorReport();

        /* release resource held by lsc */
        AtEOXact_SysDBCache(false);

        FlushErrorState();

        /* Now we can allow interrupts again */
        RESUME_INTERRUPTS();

        /* if in shutdown mode, no need for anything further; just go away */
        if (t_thrd.gstat_cxt.got_SIGTERM)
            goto shutdown;

        /*
         * Sleep at least 1 second after any error.  We don't want to be
         * filling the error logs as fast as we can.
         */
        pg_usleep(1000000L);
    }

    while (!t_thrd.gstat_cxt.got_SIGTERM) {
        /* backup the old one so we can delete it when nobody needs it anymore */
        MemoryContext oldStatLocalContext = u_sess->stat_cxt.pgStatLocalContext;

        u_sess->stat_cxt.pgStatLocalContext =
            AllocSetContextCreate(u_sess->top_mem_cxt, "Global Statistics snapshot",
                ALLOCSET_SMALL_MINSIZE, ALLOCSET_SMALL_INITSIZE, ALLOCSET_SMALL_MAXSIZE);
        
        pgstat_fetch_global();

        /* switch global_stats_map to point to the newly loaded statistics */
        PrepareStatsHashForSwitch();
        g_instance.stat_cxt.tableStat->global_stats_map = u_sess->stat_cxt.pgStatDBHash;
        CompleteStatsHashSwitch();

        /* Now destroy the old one */
        if (oldStatLocalContext) {
            MemoryContextDelete(oldStatLocalContext);
        }

        u_sess->stat_cxt.pgStatDBHash = NULL;

        pg_usleep(u_sess->attr.attr_storage.ustats_tracker_naptime * 1000000L);
    }

shutdown:
    /*
     * Before the thread exits, set global_stats_map to NULL to prevent core dump when the
     * backend thread accesses the released memory during the prune operation.
     */
    PrepareStatsHashForSwitch();
    g_instance.stat_cxt.tableStat->global_stats_map = NULL;
    CompleteStatsHashSwitch();
    ereport(LOG, (errmsg("global stats shutting down")));
    proc_exit(0);
}


/*
 * Get the statistics from the global statistics hash for this relation at the particular db.
 * relid is the partition id for a Partitioned table, otherwise it's the Relation id.
 * parentid is the actual Relation id for a Partitioned table, otherwise it's InvalidOid.
*/
bool GetTableGstats(Oid dbid, Oid relid, Oid parentid, PgStat_StatTabEntry *tableentry)
{
    /* return if stats is not ready */
    if (g_instance.stat_cxt.tableStat->global_stats_map == NULL ||
        pg_atomic_read_u64(&g_instance.stat_cxt.tableStat->quiesce) == 1) {
        return false;
    }

    uint64 readers PG_USED_FOR_ASSERTS_ONLY = pg_atomic_add_fetch_u64(&g_instance.stat_cxt.tableStat->readers, 1);
    Assert(readers < (uint64) GLOBAL_ALL_PROCS);

    /* recheck in case quiesce is updated between first check and increment readers */
    if (pg_atomic_read_u64(&g_instance.stat_cxt.tableStat->quiesce) == 1) {
        pg_atomic_sub_fetch_u64(&g_instance.stat_cxt.tableStat->readers, 1);
        return false;
    }

    Assert(g_instance.stat_cxt.tableStat->global_stats_map != NULL);
    PgStat_StatDBEntry *dbentry = NULL;
    PgStat_StatTabEntry *tabentry = NULL;
    errno_t rc = 0;
    bool result = false;

    PgStat_StatTabKey tabkey;
    tabkey.statFlag = parentid;
    tabkey.tableid = relid;

    dbentry = (PgStat_StatDBEntry*)hash_search(g_instance.stat_cxt.tableStat->global_stats_map,
                                               (void*)&dbid, HASH_FIND, NULL);
    if (dbentry == NULL) {
        goto done;
    }

    tabentry = (PgStat_StatTabEntry*)hash_search(dbentry->tables, (void*)(&tabkey), HASH_FIND, NULL);
    if (tabentry == NULL) {
        goto done;
    }

    rc =  memcpy_s(tableentry, sizeof(PgStat_StatTabEntry),
                   tabentry, sizeof(PgStat_StatTabEntry));
    securec_check(rc, "", "");
    result = true;

done:
    readers = pg_atomic_sub_fetch_u64(&g_instance.stat_cxt.tableStat->readers, 1);
    Assert(readers < (uint64) GLOBAL_ALL_PROCS);

    return result;
}

static LWLock *LockStartBlockHashTablePartition(PgStat_StartBlockTableKey *tabkey, LWLockMode mode)
{
    uint32 hashValue = get_hash_value(g_instance.stat_cxt.tableStat->blocks_map, tabkey);
    uint32 partition = hashValue % (NUM_STARTBLOCK_PARTITIONS);
    uint32 lockid = (uint32)(FirstStartBlockMappingLock + partition);
    LWLock* lock = &t_thrd.shemem_ptr_cxt.mainLWLockArray[lockid].lock;

    LWLockAcquire(lock, mode);

    return lock;
}

PgStat_StartBlockTableEntry *
StartBlockHashTableLookup(PgStat_StartBlockTableKey *tabkey)
{
    PgStat_StartBlockTableEntry *result = NULL;
    bool found = false;

    LWLock* lock = LockStartBlockHashTablePartition(tabkey, LW_SHARED);
    result = (PgStat_StartBlockTableEntry *) hash_search(g_instance.stat_cxt.tableStat->blocks_map,
                                                         tabkey, HASH_FIND, &found);
    LWLockRelease(lock);

    return result;
}

PgStat_StartBlockTableEntry *
StartBlockHashTableAdd(PgStat_StartBlockTableKey *tabkey)
{
    bool found = true;
    PgStat_StartBlockTableEntry *result = NULL;

    LWLock* lock = LockStartBlockHashTablePartition(tabkey, LW_EXCLUSIVE);

    result = (PgStat_StartBlockTableEntry *)hash_search(g_instance.stat_cxt.tableStat->blocks_map,
                                                        tabkey, HASH_ENTER, &found);

    if (!found) {
        for (int i = 0; i < START_BLOCK_ARRAY_SIZE; i++) {
            result->starting_blocks[i] = i;
        }
    }
    LWLockRelease(lock);

    return result;
}

PgStat_StartBlockTableEntry *
GetStartBlockHashEntry(PgStat_StartBlockTableKey *tabkey)
{
    PgStat_StartBlockTableEntry *result = NULL;
    result = StartBlockHashTableLookup(tabkey);
    /* not found, add it */
    if (result == NULL) {
        result = StartBlockHashTableAdd(tabkey);
    }

    Assert(result);
    return result;
}
