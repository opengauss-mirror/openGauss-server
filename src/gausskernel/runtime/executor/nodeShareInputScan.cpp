/* -------------------------------------------------------------------------
 *
 * nodeShareInputScan.cpp
 *
 * A Share Input Scan node is used to share the result of an operation in
 * two different branches in the plan tree.
 *
 * These come in two variants: local, and cross-slice.
 *
 * Local shares
 * ------------
 *
 * In local mode, all the consumers are in the same slice as the producer.
 * In that case, there's no need to communicate across processes, so we
 * rely entirely on data structures in backend-private memory to track the
 * state.
 *
 * In local mode, there is no difference between producer and consumer
 * nodes. In ExecInitShareInputScan(), the producer node stores the
 * PlanState of the shared child node where all the nodes can find it.
 * The first ExecShareInputScan() call initializes the store.
 *
 * A local-mode ShareInputScan is quite similar to PostgreSQL's CteScan,
 * but there are some implementation differences. CteScan uses a special
 * PARAM_EXEC entry to hold the shared state, while ShareInputScan uses
 * an entry in es_sharenode instead.
 *
 * Cross-slice shares
 * ------------------
 *
 * A cross-slice share works basically the same as a local one, except
 * that the producing slice makes the underlying tuplestore available to
 * other processes, by forcing it to be written to a file on disk. The
 * first ExecShareInputScan() call in the producing slice materializes
 * the whole tuplestore, and advertises that it's ready in shared memory.
 * Consumer slices wait for that before trying to read the store.
 *
 * The producer and the consumers communicate the status of the scan using
 * shared memory. There's a hash table in shared memory, containing a
 * 'shareinput_Xslice_state' struct for each shared scan. The producer uses
 * a &state->ready_done_cv.m_mutexcondition variable to wake up consumers, when the tuplestore is fully
 * materialized, and the consumers use the same condition variable to inform
 * the producer when they're done reading it. The producer slice keeps the
 * underlying tuplestore open, until all the consumers have finished.
 *
 * Portions Copyright (c) 2023 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 * 	    src/gausskernel/runtime/executor/nodeShareInputScan.cpp
 *
 * -------------------------------------------------------------------------
 */
#ifdef USE_SPQ
#include "postgres.h"

#include "access/xact.h"
#include "executor/executor.h"
#include "executor/node/nodeShareInputScan.h"
#include "miscadmin.h"
#include "storage/lock/lwlock.h"
#include "storage/lwlocknames.h"
#include "storage/shmem.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "utils/tuplestore.h"
#include "lib/ilist.h"

/*
 * The SharedFileSet deletes any remaining files when the reference count
 * reaches zero, but we don't rely on that mechanism. All the files are
 * held in the same SharedFileSet, so it cannot be recycled until all
 * ShareInputScans in the system have finished, which might never happen if
 * new queries are started continuously. The shareinput_Xslice_state entries
 * are reference counted separately, and we clean up the files backing each
 * individual ShareInputScan whenever its reference count reaches zero.
 */
static SharedFileSet *shareinput_Xslice_fileset;

typedef struct {
    pthread_mutex_t m_mutex;
    pthread_cond_t m_cond;
} ConditionVariable;

/*
 * In a cross-slice ShareinputScan, the producer and consumer processes
 * communicate using shared memory. There's a hash table containing one
 * 'shareinput_share_state' for each in-progress shared input scan.
 *
 * The hash table itself,, and the fields within every entry, are protected
 * by ShareInputScanLock. (Although some operations get away without the
 * lock, when the field is atomic and/or there's only one possible writer.)
 *
 * All producers and consumers that participate in a shared scan hold
 * a reference to the 'shareinput_Xslice_state' entry of the scan, for
 * the whole lifecycle of the node from ExecInitShareInputScan() to
 * ExecEndShareInputScan(). The entry in the hash table is created by
 * the first participant that initializes, which is not necessarily the
 * producer! When the last participant releases the entry, it is removed
 * from the hash table.
 */
typedef struct shareinput_tag {
    uint64 session_id;
    int32 share_id;
    int32 dop_id;
} shareinput_tag;

typedef struct shareinput_Xslice_state {
    shareinput_tag tag; /* hash key */

    int refcount; /* reference count of this entry */
    bool ready;   /* is the input fully materialized and ready to be read? */
    int ndone;    /* # of consumers that have finished the scan */

    /*
     * ready_done_cv is used for signaling when the scan becomes "ready", and
     * when it becomes "done". The producer wakes up everyone waiting on this
     * condition variable when it sets ready = true. Also, when the last
     * consumer finishes the scan (ndone reaches nconsumers), it wakes up the
     * producer using this same condition variable.
     */
    ConditionVariable ready_done_cv;
} shareinput_Xslice_state;

/*
 * 'shareinput_reference' represents a reference or "lease" to an entry
 * in the shared memory hash table. It is used for garbage collection of
 * the entries, on transaction abort.
 *
 */
typedef struct shareinput_Xslice_reference {
    int share_id;
    shareinput_Xslice_state *xslice_state;

    ResourceOwner owner;

    dlist_node node;
} shareinput_Xslice_reference;

/*
 * For local (i.e. intra-slice) variants, we use a 'shareinput_local_state'
 * to track the status. It is analogous to 'shareinput_share_state' used for
 * cross-slice scans, but we don't need to keep it in shared memory. These
 * are held in estate->es_sharenode, indexed by share_id.
 */
typedef struct shareinput_local_state {
    bool ready;
    bool closed;
    int ndone;
    int nsharers;

    /*
     * This points to the child node that's being shared. Set by
     * ExecInitShareInputScan() of the instance that has the child.
     */
    PlanState *childState;

    /* Tuplestore that holds the result */
    Tuplestorestate *ts_state;
} shareinput_local_state;

static shareinput_Xslice_reference *get_shareinput_reference(int share_id);
static void release_shareinput_reference(shareinput_Xslice_reference *ref);
static void shareinput_create_bufname_prefix(char *p, int size, int share_id, int dop_id);

static void shareinput_writer_notifyready(shareinput_Xslice_reference *ref);
static void shareinput_reader_waitready(shareinput_Xslice_reference *ref);
static void shareinput_reader_notifydone(shareinput_Xslice_reference *ref, int nconsumers);
static void shareinput_writer_waitdone(shareinput_Xslice_reference *ref, int nconsumers);


/*
 * init_tuplestore_state
 * Initialize the tuplestore state for the Shared node if the state
 * is not initialized.
 */
static void init_tuplestore_state(ShareInputScanState *node)
{
    EState *estate = node->ss.ps.state;
    ShareInputScan *sisc = (ShareInputScan *)node->ss.ps.plan;
    shareinput_local_state *local_state = node->local_state;
    Tuplestorestate *ts;
    int tsptrno;
    TupleTableSlot *outerslot;

    Assert(!node->isready);
    Assert(node->ts_state == NULL);
    Assert(node->ts_pos == -1);

    if (!node->ref)
        elog(ERROR, "cannot execute ShareInputScan that was not initialized");

    if (!local_state->ready) {
        if (t_thrd.spq_ctx.current_id == sisc->producer_slice_id || estate->es_plannedstmt->num_streams == 1) {
            char rwfile_prefix[100];

            ts = tuplestore_begin_heap(true, /* randomAccess */
                false,                       /* interXact */
                10);                         /* maxKBytes FIXME */

            shareinput_create_bufname_prefix(rwfile_prefix, sizeof(rwfile_prefix), sisc->share_id,
                u_sess->stream_cxt.smp_id);

            tuplestore_make_shared(ts, get_shareinput_fileset(), rwfile_prefix);

            for (;;) {
                outerslot = ExecProcNode(local_state->childState);
                if (TupIsNull(outerslot))
                    break;
                tuplestore_puttupleslot(ts, outerslot);
            }

            tuplestore_freeze(ts);
            shareinput_writer_notifyready(node->ref);

            tuplestore_rescan(ts);
        } else {
            /*
             * We are a consumer slice. Wait for the producer to create the
             * tuplestore.
             */
            char rwfile_prefix[100];

            shareinput_reader_waitready(node->ref);

            shareinput_create_bufname_prefix(rwfile_prefix, sizeof(rwfile_prefix), sisc->share_id,
                u_sess->stream_cxt.smp_id);
            ts = tuplestore_open_shared(get_shareinput_fileset(), rwfile_prefix);
        }
        local_state->ts_state = ts;
        local_state->ready = true;
        tsptrno = 0;
    } else {
        /* Another local reader */
        ts = local_state->ts_state;
        tsptrno = tuplestore_alloc_read_pointer(ts, (EXEC_FLAG_BACKWARD | EXEC_FLAG_REWIND));

        tuplestore_select_read_pointer(ts, tsptrno);
        tuplestore_rescan(ts);
    }

    node->ts_state = ts;
    node->ts_pos = tsptrno;

    node->isready = true;
}


/* ------------------------------------------------------------------
 * 	ExecShareInputScan
 * 	Retrieve a tuple from the ShareInputScan
 * ------------------------------------------------------------------
 */
TupleTableSlot *ExecShareInputScan(PlanState *pstate)
{
    ShareInputScanState *node = castNode(ShareInputScanState, pstate);
    ShareInputScan *sisc = (ShareInputScan *)pstate->plan;
    EState *estate;
    ScanDirection dir;
    bool forward;
    TupleTableSlot *slot;

    /*
     * get state info from node
     */
    estate = pstate->state;
    dir = estate->es_direction;
    forward = ScanDirectionIsForward(dir);

    if (sisc->this_slice_id != t_thrd.spq_ctx.current_id && estate->es_plannedstmt->num_streams != 1)
        elog(ERROR, "cannot execute alien Share Input Scan");

    /* if first time call, need to initialize the tuplestore state.  */
    if (!node->isready)
        init_tuplestore_state(node);

    slot = node->ss.ps.ps_ResultTupleSlot;

    Assert(!node->local_state->closed);

    tuplestore_select_read_pointer(node->ts_state, node->ts_pos);
    while (1) {
        bool gotOK;

        gotOK = tuplestore_gettupleslot(node->ts_state, forward, false, slot);

        if (!gotOK)
            return NULL;

        return slot;
    }

    Assert(!"should not be here");
    return NULL;
}

/*  ------------------------------------------------------------------
 * 	ExecInitShareInputScan
 * ------------------------------------------------------------------
 */
ShareInputScanState *ExecInitShareInputScan(ShareInputScan *node, EState *estate, int eflags)
{
    ShareInputScanState *sisstate;
    Plan *outerPlan;
    PlanState *childState;
    shareinput_local_state *local_state;

    Assert(innerPlan(node) == NULL);

    /* create state data structure */
    sisstate = makeNode(ShareInputScanState);
    sisstate->ss.ps.plan = (Plan *)node;
    sisstate->ss.ps.state = estate;
    sisstate->ss.ps.ExecProcNode = ExecShareInputScan;
    sisstate->ts_state = NULL;
    sisstate->ts_pos = -1;

    /*
     * init child node.
     * if outerPlan is NULL, this is no-op (so that the ShareInput node will be
     * only init-ed once).
     */

    /*
     * initialize child nodes
     *
     * Like a Material node, we shield the child node from the need to support
     * BACKWARD, or MARK/RESTORE.
     */
    eflags &= ~(EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK);

    outerPlan = outerPlan(node);
    childState = ExecInitNode(outerPlan, estate, eflags);
    outerPlanState(sisstate) = childState;

    Assert(node->scan.plan.qual == NULL);
    sisstate->ss.ps.qual = NULL;

    /* Misc initialization
     *
     * Create expression context
     */
    ExecAssignExprContext(estate, &sisstate->ss.ps);

    /*
     * Initialize result slot and type.
     */
    ExecInitResultTupleSlot(estate, &sisstate->ss.ps);
    ExecAssignResultTypeFromTL(&sisstate->ss.ps);

    sisstate->ss.ps.ps_ProjInfo = NULL;

    /*
     * When doing EXPLAIN only, we won't actually execute anything, so don't
     * bother initializing the state. This isn't merely an optimization:
     * closing a cross-slice ShareInputScan waits for the consumers to finish,
     * but if we don't execute anything, it will hang forever.
     *
     * We could also exit here immediately if this is an "alien" node, i.e.
     * a node that doesn't execute in this slice, but we can't easily
     * detect that here.
     */
    if ((eflags & EXEC_FLAG_EXPLAIN_ONLY) != 0)
        return sisstate;

    /* expand the list if necessary */
    while (list_length(estate->es_sharenode) <= node->share_id) {
        local_state = (shareinput_local_state *)palloc0(sizeof(shareinput_local_state));
        local_state->ready = false;

        estate->es_sharenode = lappend(estate->es_sharenode, local_state);
    }

    local_state = (shareinput_local_state *)list_nth(estate->es_sharenode, node->share_id);

    /*
     * only the consumer ShareInputScan nodes executed in current
     * slice are counted, since only consumers would increase
     * "ndone" in local_state, and compare "ndone" with "nsharers"
     * to judge whether to notify producer.
     */
    if (t_thrd.spq_ctx.current_id == node->this_slice_id && t_thrd.spq_ctx.current_id != node->producer_slice_id)
        local_state->nsharers++;

    if (childState)
        local_state->childState = childState;
    sisstate->local_state = local_state;

    /* Get a lease on the shared state */
    sisstate->ref = get_shareinput_reference(node->share_id);

    return sisstate;
}

/* ------------------------------------------------------------------
 * 	ExecEndShareInputScan
 * ------------------------------------------------------------------
 */
void ExecEndShareInputScan(ShareInputScanState *node)
{
    EState *estate = node->ss.ps.state;
    ShareInputScan *sisc = (ShareInputScan *)node->ss.ps.plan;
    shareinput_local_state *local_state = node->local_state;

    /* clean up tuple table */
    ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);

    if (node->ref) {
        if (sisc->this_slice_id == t_thrd.spq_ctx.current_id || estate->es_plannedstmt->num_streams == 1) {
            /*
             * The producer needs to wait for all the consumers to finish.
             * Consumers signal the producer that they're done reading,
             * but are free to exit immediately after that.
             */
            if (t_thrd.spq_ctx.current_id == sisc->producer_slice_id) {
                if (!local_state->ready)
                    init_tuplestore_state(node);
                shareinput_writer_waitdone(node->ref, sisc->nconsumers);
            } else {
                if (!local_state->closed) {
                    shareinput_reader_notifydone(node->ref, sisc->nconsumers);
                    local_state->closed = true;
                }
            }
        }
        release_shareinput_reference(node->ref);
        node->ref = NULL;
    }

    if (local_state && local_state->ts_state) {
        tuplestore_end(local_state->ts_state);
        local_state->ts_state = NULL;
    }

    /*
     * shutdown subplan.  First scanner of underlying share input will
     * do the shutdown, all other scanners are no-op because outerPlanState
     * is NULL
     */
    ExecEndNode(outerPlanState(node));
}

/* ------------------------------------------------------------------
 * 	ExecReScanShareInputScan
 * ------------------------------------------------------------------
 */
void ExecReScanShareInputScan(ShareInputScanState *node)
{
    /* On first call, initialize the tuplestore state */
    if (!node->isready)
        init_tuplestore_state(node);

    ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);
    Assert(node->ts_pos != -1);

    tuplestore_select_read_pointer(node->ts_state, node->ts_pos);
    tuplestore_rescan(node->ts_state);
}

/* ************************************************************************
 * IPC, for cross-slice variants.
 * ************************************************************************ */
/*
 * When creating a tuplestore file that will be accessed by
 * multiple processes, shareinput_create_bufname_prefix() is used to
 * construct the name for it.
 */
static void shareinput_create_bufname_prefix(char *p, int size, int share_id, int dop_id)
{
    snprintf(p, size, "SIRW_%lu_%d_%d", t_thrd.spq_ctx.spq_session_id, share_id, u_sess->stream_cxt.smp_id);
}

#define MaxBackends 1
/*
 * Initialization of the shared hash table for cross-slice communication.
 *
 * XXX: Use MaxBackends to size it, on the assumption that max_connections
 * will scale accordingly to query complexity. This is quite fuzzy, you could
 * create a query with tons of cross-slice ShareInputScans but only a few
 * slice, but that ought to be rare enough in practice. This isn't a hard
 * limit anyway, the hash table will use up any "slop" in shared memory if
 * needed.
 */
#define N_SHAREINPUT_SLOTS() (MaxBackends * 5)

Size ShareInputShmemSize(void)
{
    Size size;

    size = hash_estimate_size(N_SHAREINPUT_SLOTS(), sizeof(shareinput_Xslice_state));

    return size;
}

void ShareInputShmemInit(void)
{
    Size size = ShareInputShmemSize();
    bool found = false;

    shareinput_Xslice_fileset = (SharedFileSet *)ShmemInitStruct("ShareInputScan", size, &found);

    if (!found || t_thrd.shemem_ptr_cxt.shareinput_Xslice_hash == nullptr) {
        HASHCTL info;
        errno_t rc = memset_s(&info, sizeof(info), 0, sizeof(info));
        securec_check(rc, "\0", "\0");
        info.keysize = sizeof(shareinput_tag);

        info.entrysize = sizeof(shareinput_Xslice_state);

        t_thrd.shemem_ptr_cxt.shareinput_Xslice_hash = ShmemInitHash("ShareInputScan notifications",
            N_SHAREINPUT_SLOTS(), N_SHAREINPUT_SLOTS(), &info, HASH_ELEM | HASH_BLOBS);
    }
}

/*
 * Get reference to the SharedFileSet used to hold all the tuplestore files.
 *
 * This is exported so that it can also be used by the INITPLAN function
 * tuplestores.
 */
SharedFileSet *get_shareinput_fileset(void)
{
    LWLockAcquire(ShareInputScanLock, LW_EXCLUSIVE);

    if (shareinput_Xslice_fileset->refcnt == 0)
        SharedFileSetInit(shareinput_Xslice_fileset);
    else
        SharedFileSetAttach(shareinput_Xslice_fileset);

    LWLockRelease(ShareInputScanLock);

    return shareinput_Xslice_fileset;
}

/*
 * Get a reference to slot in shared memory for this shared scan.
 *
 * If the slot doesn't exist yet, it is created and initialized into
 * "not ready" state.
 *
 * The reference is tracked by the current ResourceOwner, and will be
 * automatically released on abort.
 */
static shareinput_Xslice_reference *get_shareinput_reference(int share_id)
{
    shareinput_tag tag;
    shareinput_Xslice_state *xslice_state;
    bool found;
    shareinput_Xslice_reference *ref;

    ref = (shareinput_Xslice_reference *)palloc0(sizeof(shareinput_Xslice_reference));

    LWLockAcquire(ShareInputScanLock, LW_EXCLUSIVE);

    tag.session_id = t_thrd.spq_ctx.spq_session_id;
    tag.share_id = share_id;
    tag.dop_id = u_sess->stream_cxt.smp_id;
    xslice_state = (shareinput_Xslice_state *)hash_search(t_thrd.shemem_ptr_cxt.shareinput_Xslice_hash, &tag,
        HASH_ENTER_NULL, &found);
    if (!found) {
        if (xslice_state == NULL) {
            pfree(ref);
            ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of cross-slice ShareInputScan slots")));
        }

        xslice_state->refcount = 0;
        xslice_state->ready = false;
        xslice_state->ndone = 0;

        pthread_mutex_init(&xslice_state->ready_done_cv.m_mutex, NULL);
        pthread_cond_init(&xslice_state->ready_done_cv.m_cond, NULL);
    }

    xslice_state->refcount++;

    ref->share_id = share_id;
    ref->xslice_state = xslice_state;
    ref->owner = t_thrd.utils_cxt.CurrentResourceOwner;

    LWLockRelease(ShareInputScanLock);

    return ref;
}

/*
 * Release reference to a shared scan.
 *
 * The reference count in the shared memory slot is decreased, and if
 * it reaches zero, it is destroyed.
 */
static void release_shareinput_reference(shareinput_Xslice_reference *ref)
{
    shareinput_Xslice_state *state = ref->xslice_state;

    LWLockAcquire(ShareInputScanLock, LW_EXCLUSIVE);

    if (state->refcount == 1) {
        bool found;

        (void)hash_search(t_thrd.shemem_ptr_cxt.shareinput_Xslice_hash, &state->tag, HASH_REMOVE, &found);
        Assert(found);
    } else
        state->refcount--;

    // dlist_delete(&ref->node);

    LWLockRelease(ShareInputScanLock);

    pfree(ref);
}

/*
 * shareinput_reader_waitready
 *
 * Called by the reader (consumer) to wait for the writer (producer) to produce
 * all the tuples and write them to disk.
 *
 * This is a blocking operation.
 */
static void shareinput_reader_waitready(shareinput_Xslice_reference *ref)
{
    shareinput_Xslice_state *state = ref->xslice_state;

    pthread_mutex_lock(&state->ready_done_cv.m_mutex);
    if (!state->ready) {
        pthread_cond_wait(&state->ready_done_cv.m_cond, &state->ready_done_cv.m_mutex);
    }
    pthread_mutex_unlock(&state->ready_done_cv.m_mutex);

    /* it's ready now */
}

/*
 * shareinput_writer_notifyready
 *
 * Called by the writer (producer) once it is done producing all tuples and
 * writing them to disk. It notifies all the readers (consumers) that tuples
 * are ready to be read from disk.
 */
static void shareinput_writer_notifyready(shareinput_Xslice_reference *ref)
{
    shareinput_Xslice_state *state = ref->xslice_state;

    /* we're the only writer, so no need to acquire the lock. */
    Assert(!state->ready);
    pthread_mutex_lock(&state->ready_done_cv.m_mutex);
    state->ready = true;

    pthread_cond_broadcast(&state->ready_done_cv.m_cond);
    pthread_mutex_unlock(&state->ready_done_cv.m_mutex);
}

/*
 * shareinput_reader_notifydone
 *
 * Called by the reader (consumer) to notify the writer (producer) that
 * it is done reading tuples from disk.
 *
 * This is a non-blocking operation.
 */
static void shareinput_reader_notifydone(shareinput_Xslice_reference *ref, int nconsumers)
{
    shareinput_Xslice_state *state = ref->xslice_state;
    int ndone;

    pthread_mutex_lock(&state->ready_done_cv.m_mutex);
    state->ndone++;
    ndone = state->ndone;

    /* If we were the last consumer, wake up the producer. */
    if (ndone >= nconsumers)
        pthread_cond_broadcast(&state->ready_done_cv.m_cond);
    pthread_mutex_unlock(&state->ready_done_cv.m_mutex);
}

/*
 * shareinput_writer_waitdone
 *
 * Called by the writer (producer) to wait for the "done" notification from
 * all readers (consumers).
 *
 * This is a blocking operation.
 */
static void shareinput_writer_waitdone(shareinput_Xslice_reference *ref, int nconsumers)
{
    shareinput_Xslice_state *state = ref->xslice_state;

    if (!state->ready)
        elog(ERROR, "shareinput_writer_waitdone() called without creating the tuplestore");

    int ndone;
    pthread_mutex_lock(&state->ready_done_cv.m_mutex);
    ndone = state->ndone;
    if (ndone < nconsumers) {
        pthread_cond_wait(&state->ready_done_cv.m_cond, &state->ready_done_cv.m_mutex);
    }
    pthread_mutex_unlock(&state->ready_done_cv.m_mutex);
    if (ndone > nconsumers)
        elog(WARNING, "%d sharers of ShareInputScan reported to be done, but only %d were expected", ndone, nconsumers);

    /* it's all done now */
}
#endif /* USE_SPQ */
