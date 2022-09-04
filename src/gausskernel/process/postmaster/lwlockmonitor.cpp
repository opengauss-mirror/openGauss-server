/* -------------------------------------------------------------------------
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 *
 *
 * lwlockmonitor.cpp
 *	 Automatically detect lwlock deadlock
 *
 * This thread will detect the occurrence of a lwlock deadlock,
 * and do the post-processing.
 *
 * IDENTIFICATION
 *	  src/gausskernel/process/postmaster/lwlockmonitor.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include <sys/stat.h>

#include "gssignal/gs_signal.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "pgxc/pgxc.h"
#include "port.h"
#include "pg_trace.h"
#include "postmaster/autovacuum.h"
#include "postmaster/postmaster.h"
#include "access/hash.h"
#include "storage/ipc.h"
#include "storage/lmgr.h"
#include "storage/lock/lock.h"
#include "storage/proc.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"

#define DEFAULT_HOLDERS_NUM 8

typedef struct {
    LWLockAddr lock_addr; // Use LWLockAddr instead of LWLock* to avoid misuse during key hash
    int holders_curnum;
    int holders_maxnum;
    int waiters_curnum;
    int waiters_maxnum;
    holding_lockmode* holders;
    lock_entry_id* waiters;
} lock_entry;

typedef struct {
    lock_entry_id be_tid;
    int be_idx;
    LWLockAddr want_lwlock;
} thread_entry;

typedef struct {
    LWLock *lock;
    lock_entry_id waiter;
    lock_entry_id blocker;
    int waiter_index;
    /* whether blocker is holding in X mode */
    LWLockMode block_xmode;
} lw_deadlock_info;

typedef struct {
    lock_entry_id* entry_ids;
    int cur_num;
    int max_num;
} lwm_visited_thread;

typedef struct {
    lw_deadlock_info* info;
    int max_num;
    int start; /* range [start, end) */
    int end;
} lwm_deadlock;

/* Signal handlers */
static void LWLockMonitorSigHupHandler(SIGNAL_ARGS);
static void LWLockMonitorShutdownHandler(SIGNAL_ARGS);

/*
 * maybe a thread is blocked when these hold,
 * 1) the same thread id,
 * 2) unchanged light-weight change-count,
 * 3) this thread is valid and active.
 */
static inline bool maybe_this_thread_blocked(lwm_light_detect* ver1, lwm_light_detect* ver2)
{
    return (0 != ver2->entry_id.thread_id && ver2->entry_id.thread_id == ver1->entry_id.thread_id &&
            ver2->entry_id.st_sessionid == ver1->entry_id.st_sessionid && ver2->lw_count == ver1->lw_count);
}

/*
 * compare two light detect data and check whether any thread may
 * be blocked by others or itself. may be deadlock happend ?
 */
bool lwm_compare_light_detect(lwm_light_detect* olds, lwm_light_detect* news)
{
    const int n = BackendStatusArray_size;
    bool found = false;

    for (int i = 0; i < n; ++i) {
        if (maybe_this_thread_blocked(olds++, news++)) {
            found = true;
            break;
        }
    }
    return found;
}

/* find all the candidates, remember their positions and number */
int* lwm_find_candidates(lwm_light_detect* olds, lwm_light_detect* news, int* out_num)
{
    const int n = BackendStatusArray_size;
    int tmp_num = 0;
    int* candidates_pos = (int*)palloc(sizeof(int) * n);

    /* the first assignment */
    *out_num = 0;
    for (int i = 0; i < n; ++i) {
        if (maybe_this_thread_blocked(olds++, news++)) {
            /* remember candidates of lwlock deadlock */
            candidates_pos[tmp_num++] = i;
        }
    }
    /* remember the number of candidates */
    *out_num = tmp_num;
    return candidates_pos;
}

/* init entry if it's the first insert */
static inline void init_entry_in_the_first_insert(lock_entry* entry, LWLockAddr *entry_key)
{
    entry->lock_addr.lock = entry_key->lock;
    entry->holders = (holding_lockmode*)palloc(sizeof(holding_lockmode) * DEFAULT_HOLDERS_NUM);
    entry->waiters = (lock_entry_id*)palloc(sizeof(lock_entry_id) * DEFAULT_HOLDERS_NUM);
    entry->holders_curnum = entry->waiters_curnum = 0;
    entry->holders_maxnum = entry->waiters_maxnum = DEFAULT_HOLDERS_NUM;
}

/*
 * remember the holders of this lock.
 * now the holder holds the lockids whose number is n.
 */
static inline void map_from_lock_to_holder(HTAB* map, lwlock_id_mode* held_lwlocks, int n, lock_entry_id* holder)
{
    lock_entry* entry = NULL;
    lwlock_id_mode* holding_lwlock = NULL;
    int i = 0;
    bool found = false;

    for (i = 0, holding_lwlock = held_lwlocks; i < n; ++i, ++holding_lwlock) {
        entry = (lock_entry*)hash_search(map, &(holding_lwlock->lock_addr), HASH_ENTER, &found);
        if (!found) {
            init_entry_in_the_first_insert(entry, &(holding_lwlock->lock_addr));
        }
        if (unlikely(entry->holders_curnum >= entry->holders_maxnum)) {
            int new_size = entry->holders_maxnum * 2;
            entry->holders = (holding_lockmode*)repalloc(entry->holders, sizeof(holding_lockmode) * new_size);
            entry->holders_maxnum = new_size;
        }
        /* remember the holders of this lock */
        entry->holders[entry->holders_curnum].holder_tid.thread_id = holder->thread_id;
        entry->holders[entry->holders_curnum].holder_tid.st_sessionid = holder->st_sessionid;
        entry->holders[entry->holders_curnum].lock_sx = holding_lwlock->lock_sx;
        entry->holders_curnum++;
    }
}

/*
 * build the hash map for the two relations
 * 1) lwlock id and its waiters;
 * 2) lwlock id and its holders;
 */
static void build_holder_and_waiter_map(HTAB* map, lwm_lwlocks* candidates, int num_candidates)
{
    lwm_lwlocks* lock = NULL;
    lock_entry* entry = NULL;
    bool found = false;

    lock = candidates;
    for (int i = 0; i < num_candidates; ++i, ++lock) {
        if (0 == lock->be_tid.thread_id) {
            /* skip this lock if thread id is not valid */
            continue;
        }

        /* build map from lock address to waiters */
        entry = (lock_entry*)hash_search(map, &(lock->want_lwlock), HASH_ENTER, &found);
        if (!found) {
            init_entry_in_the_first_insert(entry, &(lock->want_lwlock));
        }
        if (unlikely(entry->waiters_curnum >= entry->waiters_maxnum)) {
            int new_size = entry->waiters_maxnum * 2;
            entry->waiters = (lock_entry_id*)repalloc(entry->waiters, sizeof(lock_entry_id) * new_size);
            entry->waiters_maxnum = new_size;
        }
        /* remember all the waiters (thread id) about this lwlock */
        entry->waiters[entry->waiters_curnum].thread_id = lock->be_tid.thread_id;
        entry->waiters[entry->waiters_curnum].st_sessionid = lock->be_tid.st_sessionid;
        entry->waiters_curnum++;

        /* build map from lockid to holders */
        map_from_lock_to_holder(map, lock->held_lwlocks, lock->lwlocks_num, &(lock->be_tid));
    }
}

/* destroy lock hash table */
static void destroy_lock_hashtbl(HTAB* lock_map)
{
    lock_entry* entry = NULL;
    HASH_SEQ_STATUS hseq_stat;

    hash_seq_init(&hseq_stat, lock_map);
    while ((entry = (lock_entry*)hash_seq_search(&hseq_stat)) != NULL) {
        pfree_ext(entry->waiters);
        pfree_ext(entry->holders);
    }
    hash_destroy(lock_map);
}

/* build map between thread id and its acquired lwlock */
static void build_map_from_threadid_to_lockid(HTAB* map, lwm_lwlocks* candidates, int num_candidates)
{
    thread_entry* entry = NULL;
    lwm_lwlocks* lock = NULL;
    bool found = false;

    lock = candidates;
    for (int i = 0; i < num_candidates; ++i, ++lock) {
        if (0 == lock->be_tid.thread_id) {
            /* skip this lock if its thread id is not valid */
            continue;
        }

        entry = (thread_entry*)hash_search(map, &(lock->be_tid), HASH_ENTER, &found);
        if (!found) {
            /* one thread acquires only one lwlock */
            entry->want_lwlock = lock->want_lwlock;
            entry->be_idx = lock->be_idx;
        } else {
            /*
             * see LWLockQueueSelf().
             * one thread cannot once wait more than one lwlock.
             */
            Assert(0);
        }
    }
}

/*
 * recure version for finding lock cycle.
 * check_thread [IN]: thread to check this loop
 * lock_map [IN]: map relation between lock and its waiter && holder
 * tid_map [IN]: map relation between thread id and wanted lwlock.
 * visited [IN/OUT]: global infor about visited threads
 * deadlock [IN/OUT]: global infor about deadlock cycle
 * depth [IN]: recure depth
 */
static bool find_lock_cycle_recurse(thread_entry* check_thread, HTAB* lock_map, HTAB* tid_map,
    lwm_visited_thread* visited, lwm_deadlock* deadlock, int depth)
{
    lock_entry* entry = NULL;
    thread_entry* next_check = NULL;
    lock_entry_id* holder = NULL;
    const ThreadId check_threadid = check_thread->be_tid.thread_id;
    const uint64 check_sessionid = check_thread->be_tid.st_sessionid;
    int i = 0;
    bool found = false;

    /* check whether this thread id occurs within visited threads list */
    for (i = 0; i < visited->cur_num; ++i) {
        if (check_threadid == visited->entry_ids[i].thread_id &&
            check_sessionid == visited->entry_ids[i].st_sessionid) {
            /* remember the end position */
            deadlock->end = depth;
            /* deadlock detected */
            return true;
        }
    }

    /* expand if needed and remember this thread */
    if (visited->cur_num >= visited->max_num) {
        const int new_size = visited->max_num * 2;
        visited->entry_ids = (lock_entry_id*)repalloc(visited->entry_ids, sizeof(lock_entry_id) * new_size);
        visited->max_num = new_size;
    }
    visited->entry_ids[visited->cur_num].thread_id = check_threadid;
    visited->entry_ids[visited->cur_num].st_sessionid = check_sessionid;
    visited->cur_num++;

    entry = (lock_entry*)hash_search(lock_map, &(check_thread->want_lwlock), HASH_FIND, &found);
    AssertEreport(found && entry != NULL, MOD_ALL, "the wanted lock is not found in cache");
    const int holder_num = entry->holders_curnum;
    if (holder_num == 0) {
        /* there is no any holder, so no deadlock loop */
        return false;
    }

    /* handle each holder of this lock */
    holding_lockmode* entryHolder = entry->holders;
    for (i = 0; i < holder_num; ++i, ++entryHolder) {
        holder = &(entryHolder->holder_tid);
        next_check = (thread_entry*)hash_search(tid_map, holder, HASH_FIND, &found);
        if (next_check && find_lock_cycle_recurse(next_check, lock_map, tid_map, visited, deadlock, depth + 1)) {
            /* expand if needed */
            if (depth >= deadlock->max_num) {
                int new_size = deadlock->max_num * 2;
                deadlock->info = (lw_deadlock_info*)repalloc(deadlock->info, sizeof(lw_deadlock_info) * new_size);
                deadlock->max_num = new_size;
            }

            /* record deadlock details */
            lw_deadlock_info* detail = deadlock->info + depth;
	    detail->lock = check_thread->want_lwlock.lock;
            detail->blocker.thread_id = holder->thread_id;
            detail->blocker.st_sessionid = holder->st_sessionid;
            detail->waiter.thread_id = check_threadid;
            detail->waiter.st_sessionid = check_sessionid;
            detail->waiter_index = check_thread->be_idx;
            detail->block_xmode = entry->holders->lock_sx;

            return true;
        }
    }
    /* No conflict detected here. */
    return false;
}

/* find the start point for this deadlock loop */
static bool find_cycle_start_point(lwm_deadlock* deadlock)
{
    lw_deadlock_info* first = deadlock->info;
    lw_deadlock_info* last = deadlock->info + deadlock->end - 1;
    lw_deadlock_info* curr = NULL;

    /* reset start position */
    deadlock->start = -1;

    /*
     * scan backward until the current waiter is the holder of
     * the last lock. this is the start point of deadlock loop.
     */
    for (curr = last; curr >= first; --curr) {
        if (last->blocker.thread_id == curr->waiter.thread_id &&
            last->blocker.st_sessionid == curr->waiter.st_sessionid) {
            deadlock->start = (curr - first);
            return true;
        }
    }
    return false; /* maybe something wrong */
}

/* enter point to find deadlock cycle */
static bool find_lock_cycle(
    lwm_lwlocks* lock, HTAB* lock_map, HTAB* tid_map, lwm_visited_thread* visited, lwm_deadlock* deadlock)
{
    /* reset the current number of visited threads */
    visited->cur_num = 0;

    /* reset the current range info of deadlock data */
    deadlock->end = 0;

    bool found = false;
    thread_entry* check_thread = (thread_entry*)hash_search(tid_map, &(lock->be_tid), HASH_FIND, &found);
    if (found) {
        if (find_lock_cycle_recurse(check_thread, lock_map, tid_map, visited, deadlock, 0)) {
            return find_cycle_start_point(deadlock);
        }
    }
    return false;
}

/*
 * Report a detected deadlock, with available details.
 */
void lwm_deadlock_report(lwm_deadlock* deadlock)
{
    StringInfoData clientbuf; /* errdetail for client */
    StringInfoData logbuf;    /* errdetail for server log */
    int i = 0;

    initStringInfo(&clientbuf);
    initStringInfo(&logbuf);

    /* Generate the "waits for" lines sent to the client */
    for (i = deadlock->start; i < deadlock->end; ++i) {
        lw_deadlock_info* info = deadlock->info + i;
        if (i > 0) {
            appendStringInfoChar(&clientbuf, '\n');
        }
        appendStringInfo(&clientbuf,
            _("thread %lu , session %lu waits for LWLOCK (%s), but blocked by thread %lu, session id %lu\
				 lock mode is %d(Exclusive 0, Shared 1, wait 2). lock index is %d"),
            info->waiter.thread_id,
            info->waiter.st_sessionid,
            T_NAME(info->lock),
            info->blocker.thread_id,
            info->blocker.st_sessionid,
            info->block_xmode,
            info->waiter_index);
    }

    /* Duplicate all the above for the server ... */
    appendStringInfoString(&logbuf, clientbuf.data);

    /* ... and add info about query strings */
    for (i = deadlock->start; i < deadlock->end; ++i) {
        lw_deadlock_info* info = deadlock->info + i;

        appendStringInfoChar(&logbuf, '\n');
        appendStringInfo(&logbuf,
            _("thread %lu: %s"),
            info->waiter.thread_id,
            pgstat_get_backend_current_activity(info->waiter.thread_id, false));
    }

    ereport(LOG,
        (errcode(ERRCODE_T_R_DEADLOCK_DETECTED),
            errmsg("lwlock deadlock detected"),
            errdetail_internal("%s", clientbuf.data),
            errdetail_log("%s", logbuf.data)));

    pfree_ext(clientbuf.data);
    pfree_ext(logbuf.data);
}

/*
 * lwlock deadlock check and report if needed.
 * deadlock [OUT]: deadlock details to return
 * candidates [IN]: candidate threads who may be in deadlock loop.
 * num_candidates [IN]: number of candidates
 */
bool lwm_heavy_diagnosis(lwm_deadlock* deadlock, lwm_lwlocks* candidates, int num_candidates)
{
    HASHCTL hctl;
    int ret = memset_s(&hctl, sizeof(HASHCTL), 0, sizeof(HASHCTL));
    securec_check(ret, "\0", "\0");
    hctl.keysize = sizeof(LWLockAddr);
    hctl.entrysize = sizeof(lock_entry);
    /* LWLockAddr is not suitable for Oid type, so use tab_hash */
    hctl.hash = tag_hash;

    HTAB* lock_map = hash_create("LWLOCK holder and waiter",
        1024, /* default node number */
        &hctl,
        HASH_ELEM | HASH_FUNCTION);
    /* build maps about holders and acquirers */
    build_holder_and_waiter_map(lock_map, candidates, num_candidates);

    ret = memset_s(&hctl, sizeof(HASHCTL), 0, sizeof(HASHCTL));
    securec_check(ret, "\0", "\0");
    hctl.keysize = sizeof(lock_entry_id);
    hctl.entrysize = sizeof(thread_entry);
    hctl.hash = tag_hash;

    HTAB* thread_map = hash_create("thread and want lock",
        1024, /* default node number */
        &hctl,
        HASH_ELEM | HASH_FUNCTION);
    /* build maps between thread id and lock to require */
    build_map_from_threadid_to_lockid(thread_map, candidates, num_candidates);

    lwm_visited_thread visited;
    visited.cur_num = 0;
    visited.max_num = 8;
    visited.entry_ids = (lock_entry_id*)palloc(sizeof(lock_entry_id) * visited.max_num);

    deadlock->start = -1;
    deadlock->end = 0;
    deadlock->max_num = 8;
    deadlock->info = (lw_deadlock_info*)palloc(sizeof(lw_deadlock_info) * deadlock->max_num);

    /* check and find deadlock cycle */
    bool found = false;
    for (int i = 0; i < num_candidates; ++i) {
        if (find_lock_cycle(candidates + i, lock_map, thread_map, &visited, deadlock)) {
            /* print deadlock detail message */
            lwm_deadlock_report(deadlock);
            found = true;
            break;
        }
    }

    pfree_ext(visited.entry_ids);
    destroy_lock_hashtbl(lock_map);
    lock_map = NULL;
    hash_destroy(thread_map);
    thread_map = NULL;
    return found;
}

/*
 * choose one victim for this deadlock loop.
 * deadlock [IN]: deadlock details
 * out_idx [OUT]: backend position of this victim
 */
static int choose_one_victim(lwm_deadlock* deadlock, int* out_idx)
{
    Assert(deadlock->start >= 0 && deadlock->start < deadlock->end);

    TimestampTz nearest_xact_tm = 0;
    TimestampTz tmp_xact_tm = 0;
    int backend_index = -1;
    int auxproc_index = NotAnAuxProcess;

    int i = deadlock->start;
    int be_index = deadlock->info[i].waiter_index;
    /* init the victim */
    if (be_index < MAX_BACKEND_SLOT) {
        backend_index = be_index;
        nearest_xact_tm = pgstat_read_xact_start_tm(be_index);
    } else {
        auxproc_index = be_index;
    }

    /* init the position of the first victim */
    *out_idx = i;

    /* begin from (deadlock->start + 1) */
    for (++i; i < deadlock->end; ++i) {
        be_index = deadlock->info[i].waiter_index;
        if (be_index < MAX_BACKEND_SLOT) {
            tmp_xact_tm = pgstat_read_xact_start_tm(be_index);
            /* prefer the shortest exclusive lock run time,equal condition used to test case for xact tz is 0 */
            if (timestamptz_cmp_internal(tmp_xact_tm, nearest_xact_tm) >= 0 && 0 == deadlock->info[i].block_xmode) {
                nearest_xact_tm = tmp_xact_tm;
                backend_index = be_index;
                *out_idx = i;
#ifdef HAVE_INT64_TIMESTAMP
                ereport(LOG,
                    (errmsg("choose_one_victim,lock_mode is %d,tran time is %lu,out index is %d,backend index is %d",
                        deadlock->info[i].block_xmode,
                        nearest_xact_tm,
                        *out_idx,
                        backend_index)));
#else
               ereport(LOG,
                    (errmsg("choose_one_victim,lock_mode is %d,tran time is %le,out index is %d,backend index is %d",
                        deadlock->info[i].block_xmode,
                        nearest_xact_tm,
                        *out_idx,
                        backend_index)));
#endif
            }
        } else {
            /*
             * this is a Auxiliary thread, see pgstat_initialize().
             * guess from AuxProcType that, much bigger be_index is,
             * less important this thread role is.
             */
            auxproc_index = Max(auxproc_index, be_index);
        }
    }

    /* prefer the worker thread to Auxiliary thread */
    return ((backend_index >= 0) ? backend_index : ((auxproc_index >= MAX_BACKEND_SLOT) ? auxproc_index : -1));
}

void lw_deadlock_auto_healing(lwm_deadlock* deadlock)
{
    /* choose one thread to be victim */
    int info_idx = 0;
    int backend_victim = choose_one_victim(deadlock, &info_idx);

    if (backend_victim >= 0) {
        if (backend_victim >= MAX_BACKEND_SLOT) {
            ereport(PANIC, (errmsg("process suicides because the victim of lwlock deadlock is an auxiliary thread")));
            return;
        }
        /* wake up this victim */
        lw_deadlock_info* info = deadlock->info + info_idx;
        wakeup_victim(info->lock, info->waiter.thread_id);
    } else {
        /* LOG, maybe deadlock disappear */
        ereport(LOG, (errmsg("victim not found, maybe lwlock deadlock disappear")));
    }
}

NON_EXEC_STATIC void FaultMonitorMain()
{
    sigjmp_buf localSigjmpBuf;
    MemoryContext lwm_context = NULL;

    lwm_light_detect* prev_snapshot = NULL;
    lwm_light_detect* curr_snapshot = NULL;
    long cur_timeout = 0;

    /* we are a postmaster subprocess now */
    IsUnderPostmaster = true;

    t_thrd.proc_cxt.MyProcPid = gs_thread_self();

    ereport(DEBUG5, (errmsg("lwlockmonitor process is started: %lu", t_thrd.proc_cxt.MyProcPid)));
    (void)gspqsignal(SIGURG, print_stack);
    (void)gspqsignal(SIGHUP, LWLockMonitorSigHupHandler);    /* set flag to read config file */
    (void)gspqsignal(SIGINT, LWLockMonitorShutdownHandler);  /* request shutdown */
    (void)gspqsignal(SIGTERM, LWLockMonitorShutdownHandler); /* request shutdown */
    (void)gspqsignal(SIGQUIT, SIG_IGN);
    (void)gspqsignal(SIGALRM, SIG_IGN);
    (void)gspqsignal(SIGPIPE, SIG_IGN);
    (void)gspqsignal(SIGUSR1, SIG_IGN);
    (void)gspqsignal(SIGUSR2, SIG_IGN); /* not used */

    /* Reset some signals that are accepted by postmaster but not here */
    (void)gspqsignal(SIGCHLD, SIG_DFL);
    (void)gspqsignal(SIGTTIN, SIG_DFL);
    (void)gspqsignal(SIGTTOU, SIG_DFL);
    (void)gspqsignal(SIGCONT, SIG_DFL);
    (void)gspqsignal(SIGWINCH, SIG_DFL);

    /* We allow SIGQUIT (quickdie) at all times */
    (void)sigdelset(&t_thrd.libpq_cxt.BlockSig, SIGQUIT);

    /*
     * Create a memory context that we will do all our work in.  We do this so
     * that we can reset the context during error recovery and thereby avoid
     * possible memory leaks.  Formerly this code just ran in
     * t_thrd.top_mem_cxt, but resetting that would be a really bad idea.
     */
    lwm_context = AllocSetContextCreate(t_thrd.top_mem_cxt,
        "LWLock Monitor",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);
    (void)MemoryContextSwitchTo(lwm_context);

    int curTryCounter;
    int* oldTryCounter = NULL;
    if (sigsetjmp(localSigjmpBuf, 1) != 0) {
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

        /*
         * Now return to normal top-level context and clear ErrorContext for
         * next time.
         */
        (void)MemoryContextSwitchTo(lwm_context);
        FlushErrorState();

        /* Flush any leaked data in the top-level context */
        MemoryContextResetAndDeleteChildren(lwm_context);

        /* Now we can allow interrupts again */
        RESUME_INTERRUPTS();

        /*
         * Sleep at least 1 second after any error.  A write error is likely
         * to be repeated, and we don't want to be filling the error logs as
         * fast as we can.
         */
        pg_usleep(1000000L);
    }
    oldTryCounter = gstrace_tryblock_entry(&curTryCounter);

    /* We can now handle ereport(ERROR) */
    t_thrd.log_cxt.PG_exception_stack = &localSigjmpBuf;

    /* Unblock signals (they were blocked when the postmaster forked us) */
    gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);
    (void)gs_signal_unblock_sigusr2();

    pgstat_report_appname("LWLock Monitor");
    pgstat_report_activity(STATE_IDLE, NULL);

    /* set current monitor timeout */
    cur_timeout = (long)u_sess->attr.attr_common.fault_mon_timeout * 60 * 1000;
    prev_snapshot = NULL;
    curr_snapshot = NULL;

    for (;;) {
        int rc = 0;

        /* Clear any already-pending wakeups */
        ResetLatch(&t_thrd.proc->procLatch);

        /* Process any requests or signals received recently. */
        if (t_thrd.lwm_cxt.got_SIGHUP) {
            t_thrd.lwm_cxt.got_SIGHUP = false;
            ProcessConfigFile(PGC_SIGHUP);

            long newTimeout = (long)u_sess->attr.attr_common.fault_mon_timeout * 60 * 1000;

            if (newTimeout != cur_timeout) {
                /* for lwlock debug info */
                DumpLWLockInfoToServerLog();
            }

            /* update monitor timeout */
            cur_timeout = newTimeout;
        }

        if (t_thrd.lwm_cxt.shutdown_requested) {
            /* Normal exit from the lwlockmonitor is here */
            proc_exit(0);
        }

        /* disable this feature if set u_sess->attr.attr_common.fault_mon_timeout be 0 */
        if (u_sess->attr.attr_common.fault_mon_timeout > 0) {
            /* start to do main work */
            if (NULL != prev_snapshot) {
                lwm_deadlock deadlock = {NULL, 0, 0, 0};
                bool continue_next = false;

                /* phase 1: light-weight detect using fast changcount */
                curr_snapshot = pgstat_read_light_detect();
                continue_next = lwm_compare_light_detect(prev_snapshot, curr_snapshot);

                if (continue_next) {
                    /* phase 2 if needed: heavy-weight diagnosis for lwlock deadlock */
                    int candidates_num = 0;
                    int* candidates_pos = lwm_find_candidates(prev_snapshot, curr_snapshot, &candidates_num);
                    lwm_lwlocks* backend_locks =
                        pgstat_read_diagnosis_data(curr_snapshot, candidates_pos, candidates_num);
                    pfree_ext(candidates_pos);
                    continue_next = lwm_heavy_diagnosis(&deadlock, backend_locks, candidates_num);

                    /* clean up. */
                    for (int i = 0; i < candidates_num; i++) {
                        lwm_lwlocks* lwlock = backend_locks + i;
                        pfree_ext(lwlock->held_lwlocks);
                    }
                    pfree_ext(backend_locks);
                }

                if (continue_next) {
                    /* phase 3 if needed: auto healing for lwlock deadlock */
                    lw_deadlock_auto_healing(&deadlock);
                }

                /* prepare for next monitor, and keep the current snapshot */
                if (NULL != deadlock.info) {
                    pfree_ext(deadlock.info);
                }
                pfree_ext(prev_snapshot);
                prev_snapshot = curr_snapshot;
                curr_snapshot = NULL;
            } else {
                /* the first time to get snapshot */
                prev_snapshot = pgstat_read_light_detect();
                curr_snapshot = NULL;
            }
        } else {
            /* just set a default timeout: 10min */
            cur_timeout = 10 * 60 * 1000;
        }

        pgstat_report_activity(STATE_IDLE, NULL);
        rc = WaitLatch(&t_thrd.proc->procLatch, WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH, cur_timeout);

        /*
         * Emergency bailout if postmaster has died.  This is to avoid the
         * necessity for manual cleanup of all postmaster children.
         */
        if (((unsigned int)rc) & WL_POSTMASTER_DEATH) {
            gs_thread_exit(1);
        }
    }
}

/* SIGHUP: set flag to re-read config file at next convenient time */
static void LWLockMonitorSigHupHandler(SIGNAL_ARGS)
{
    int save_errno = errno;

    t_thrd.lwm_cxt.got_SIGHUP = true;

    if (t_thrd.proc)
        SetLatch(&t_thrd.proc->procLatch);

    errno = save_errno;
}

/* SIGTERM: set flag to exit normally */
static void LWLockMonitorShutdownHandler(SIGNAL_ARGS)
{
    int save_errno = errno;

    t_thrd.lwm_cxt.shutdown_requested = true;

    if (t_thrd.proc)
        SetLatch(&t_thrd.proc->procLatch);

    errno = save_errno;
}

