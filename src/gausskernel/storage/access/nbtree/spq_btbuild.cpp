/* -------------------------------------------------------------------------
 *
 * spq_btbuild.cpp
 *	  Build btree using SPQ.
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/access/nbtree/spq_btbuild.cpp
 *
 * -------------------------------------------------------------------------
 */
 
#ifdef USE_SPQ
#include "access/nbtree.h"
#include "utils/snapmgr.h"
#include "postmaster/bgworker.h"
#include "executor/spi.h"
#include "utils/portal.h"
#include "commands/tablecmds.h"
#include "executor/executor.h"

void spq_scansort(BTBuildState *buildstate, Relation heap, Relation index, bool isconcurrent);

void spq_build_main(const BgWorkerContext *bwc);

SPQSharedContext *spq_init_shared(Relation heap, Relation index, bool isconcurrent);

SPQWorkerState *spq_worker_init(Relation heap, Relation index, SPQSharedContext *shared);
 
void spq_worker_produce(SPQWorkerState *workstate);
 
void spq_init_buffer(IndexTupleBuffer *buffer);
 
bool index_form_tuple_buffer(SPQWorkerState *workstate, IndexTupleBuffer *buffer, uint64 begin);
 
void spq_worker_finish(SPQWorkerState *workstate);
 
void spq_leafbuild(SPQLeaderState *spqleader);

void spq_leader_finish(SPQLeaderState *spqleader);

void condition_time_wait(SPQSharedContext *shared);
 
void condition_signal(SPQSharedContext *shared);
 
bool enable_spq_btbuild(Relation rel)
{
    if ((rel)->rd_options && (rel)->rd_rel->relkind == RELKIND_INDEX && (rel)->rd_rel->relam == BTREE_AM_OID) {
        if (((StdRdOptions *)(rel)->rd_options)->spq_bt_build_offset != 0) {
            return (strcmp((char *)(rel)->rd_options + ((StdRdOptions *)(rel)->rd_options)->spq_bt_build_offset,
                           "on") == 0);
        } else {
            return false;
        }
    } else {
        return false;
    }
}
bool enable_spq_btbuild_cic(Relation rel)
{
    return u_sess->attr.attr_spq.spq_enable_btbuild_cic && enable_spq_btbuild(rel);
}

IndexTuple index_form_tuple_allocated(TupleDesc tuple_descriptor, Datum *values, bool *isnull, char *start, Size free,
                                      Size *used)
{
    char *tp = NULL;         /* tuple pointer */
    IndexTuple tuple = NULL; /* return tuple */
    Size size, data_size, hoff;
    int i;
    unsigned short infomask = 0;
    bool hasnull = false;
    uint16 tupmask = 0;
    int attributeNum = tuple_descriptor->natts;
 
    Size (*computedatasize_tuple)(TupleDesc tuple_desc, Datum * values, const bool *isnull);
    void (*filltuple)(TupleDesc tuple_desc, Datum * values, const bool *isnull, char *data, Size data_size,
                      uint16 *infomask, bits8 *bit);
 
    computedatasize_tuple = &heap_compute_data_size;
    filltuple = &heap_fill_tuple;
 
#ifdef TOAST_INDEX_HACK
    Datum untoasted_values[INDEX_MAX_KEYS];
    bool untoasted_free[INDEX_MAX_KEYS];
#endif
 
    if (attributeNum > INDEX_MAX_KEYS)
        ereport(ERROR, (errcode(ERRCODE_TOO_MANY_COLUMNS),
                        errmsg("number of index columns (%d) exceeds limit (%d)", attributeNum, INDEX_MAX_KEYS)));
 
#ifdef TOAST_INDEX_HACK
    uint32 toastTarget = TOAST_INDEX_TARGET;
    if (tuple_descriptor->tdTableAmType == TAM_USTORE) {
        toastTarget = UTOAST_INDEX_TARGET;
    }
    for (i = 0; i < attributeNum; i++) {
        Form_pg_attribute att = tuple_descriptor->attrs[i];
 
        untoasted_values[i] = values[i];
        untoasted_free[i] = false;
 
        /* Do nothing if value is NULL or not of varlena type */
        if (isnull[i] || att->attlen != -1)
            continue;
 
        /*
         * If value is stored EXTERNAL, must fetch it so we are not depending
         * on outside storage.	This should be improved someday.
         */
        Pointer val = DatumGetPointer(values[i]);
        checkHugeToastPointer((varlena *)val);
        if (VARATT_IS_EXTERNAL(val)) {
            untoasted_values[i] = PointerGetDatum(heap_tuple_fetch_attr((struct varlena *)DatumGetPointer(values[i])));
            untoasted_free[i] = true;
        }
 
        /*
         * If value is above size target, and is of a compressible datatype,
         * try to compress it in-line.
         */
        if (!VARATT_IS_EXTENDED(DatumGetPointer(untoasted_values[i])) &&
            VARSIZE(DatumGetPointer(untoasted_values[i])) > toastTarget &&
            (att->attstorage == 'x' || att->attstorage == 'm')) {
            Datum cvalue = toast_compress_datum(untoasted_values[i]);
            if (DatumGetPointer(cvalue) != NULL) {
                /* successful compression */
                if (untoasted_free[i])
                    pfree(DatumGetPointer(untoasted_values[i]));
                untoasted_values[i] = cvalue;
                untoasted_free[i] = true;
            }
        }
    }
#endif
 
    for (i = 0; i < attributeNum; i++) {
        if (isnull[i]) {
            hasnull = true;
            break;
        }
    }
 
    if (hasnull)
        infomask |= INDEX_NULL_MASK;
 
    hoff = IndexInfoFindDataOffset(infomask);
#ifdef TOAST_INDEX_HACK
    data_size = computedatasize_tuple(tuple_descriptor, untoasted_values, isnull);
#else
    data_size = computedatasize_tuple(tuple_descriptor, values, isnull);
#endif
    size = hoff + data_size;
    size = MAXALIGN(size); /* be conservative */
 
    *used = size;
 
    if (size > free)
        return NULL;
 
    tp = start;
    tuple = (IndexTuple)tp;
 
    filltuple(tuple_descriptor,
#ifdef TOAST_INDEX_HACK
              untoasted_values,
#else
              values,
#endif
              isnull, (char *)tp + hoff, data_size, &tupmask, (hasnull ? (bits8 *)tp + sizeof(IndexTupleData) : NULL));
 
#ifdef TOAST_INDEX_HACK
    for (i = 0; i < attributeNum; i++) {
        if (untoasted_free[i])
            pfree(DatumGetPointer(untoasted_values[i]));
    }
#endif
 
    /*
     * We do this because heap_fill_tuple wants to initialize a "tupmask"
     * which is used for HeapTuples, but we want an indextuple infomask. The
     * only relevant info is the "has variable attributes" field. We have
     * already set the hasnull bit above.
     */
    if (tupmask & HEAP_HASVARWIDTH)
        infomask |= INDEX_VAR_MASK;
 
    /*
     * Here we make sure that the size will fit in the field reserved for it
     * in t_info.
     */
    if ((size & INDEX_SIZE_MASK) != size)
        ereport(ERROR,
                (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED), errmsg("index row requires %lu bytes, maximum size is %lu",
                                                                 (unsigned long)size, (unsigned long)INDEX_SIZE_MASK)));
 
    infomask |= size;
 
    /*
     * initialize metadata
     */
    tuple->t_info = infomask;
    return tuple;
}
/*
 *  spq based btree build
 */
Datum spqbtbuild(Relation heap, Relation index, IndexInfo *indexInfo)
{
    IndexBuildResult *result = NULL;
    BTBuildState buildstate;
    double *allPartTuples = NULL;
 
    /*
     * check if it is expr index
     */
    if (indexInfo->ii_Expressions)
        ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED), errmsg("spq btree build does not support expr index.")));
 
    buildstate.isUnique = indexInfo->ii_Unique;
    buildstate.haveDead = false;
    buildstate.heapRel = heap;
    buildstate.spool = NULL;
    buildstate.spool2 = NULL;
    buildstate.indtuples = 0;
    buildstate.btleader = NULL;
    buildstate.spqleader = NULL;
 
#ifdef BTREE_BUILD_STATS
    if (u_sess->attr.attr_resource.log_btree_build_stats) {
        ResetUsage();
    }
#endif /* BTREE_BUILD_STATS */
 
    /* We expect to be called exactly once for any index relation. If that's
     * not the case, big trouble's what we have. */
    if (RelationGetNumberOfBlocks(index) != 0) {
        ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
                        errmsg("index \"%s\" already contains data", RelationGetRelationName(index))));
    }
 
    /*
     * scan & sort using spi
     */
    spq_scansort(&buildstate, heap, index, indexInfo->ii_Concurrent);

    spq_leafbuild(buildstate.spqleader);

    spq_leader_finish(buildstate.spqleader);

    // Return statistics
    result = (IndexBuildResult *)palloc(sizeof(IndexBuildResult));
    result->heap_tuples = buildstate.spqleader->processed;
    result->index_tuples = buildstate.indtuples;
    result->all_part_tuples = allPartTuples;

    if (!indexInfo->ii_Concurrent)
        spq_btbuild_update_pg_class(heap, index);

    PG_RETURN_POINTER(result);
}

void spq_leader_finish(SPQLeaderState *spqleader)
{
    /* Shutdown worker processes */
    BgworkerListSyncQuit();
    /* Free last reference to MVCC snapshot, if one was used */
    if (IsMVCCSnapshot(spqleader->snapshot)) {
        UnregisterSnapshot(spqleader->snapshot);
    }
}

/* scan & sort using spi */
void spq_scansort(BTBuildState *buildstate, Relation heap, Relation index, bool isconcurrent)
{
    SPQLeaderState *spqleader;

    SPQSharedContext *shared;

    Snapshot snapshot;

    if (!isconcurrent)
        snapshot = SnapshotAny;
    else
        snapshot = RegisterSnapshot(GetTransactionSnapshot());

    shared = spq_init_shared(heap, index, isconcurrent);

    /* Launch workers */
    LaunchBackgroundWorkers(1, shared, spq_build_main, NULL);
 
    buildstate->spqleader = spqleader = (SPQLeaderState *)palloc0(sizeof(SPQLeaderState));
 
    spqleader->heap = heap;
    spqleader->index = index;
    spqleader->shared = shared;
    spqleader->buffer = NULL;
    spqleader->processed = 0;
    spqleader->snapshot = snapshot;
}
 
/* SPQSharedContext initialization */
SPQSharedContext *spq_init_shared(Relation heap, Relation index, bool isconcurrent)
{
    Size sharedsize;
    SPQSharedContext *shared;

    /* Calculate shared size */
    sharedsize = sizeof(SPQSharedContext);

    sharedsize += SPQ_SHARED_SIZE;

    shared = (SPQSharedContext *)MemoryContextAllocZero(INSTANCE_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), sharedsize);

    shared->heaprelid = RelationGetRelid(heap);
    shared->indexrelid = RelationGetRelid(index);
    shared->isunique = false;
    shared->isconcurrent = isconcurrent;
    shared->dop = SET_DOP(u_sess->opt_cxt.query_dop);
    /* Initialize mutable state */
    SpinLockInit(&shared->mutex);
    pthread_mutex_init(&shared->m_mutex, NULL);
    pthread_cond_init(&shared->m_cond, NULL);

    shared->bufferidx = 0;
    shared->done = false;
    shared->consumer = -1;
    shared->producer = -1;
    shared->snapshot = GetActiveSnapshot();
    shared->num_nodes = t_thrd.spq_ctx.num_nodes;
    return shared;
}

/* SPQ btree build worker thread */
void spq_build_main(const BgWorkerContext *bwc)
{
    SPQSharedContext *shared = (SPQSharedContext *)bwc->bgshared;
    SPQWorkerState *worker;

    LOCKMODE heapLockmode = NoLock;
    LOCKMODE indexLockmode = NoLock;

    if (shared->isconcurrent) {
        heapLockmode = ShareUpdateExclusiveLock;
        indexLockmode = RowExclusiveLock;
    }

    /* Open relations within worker. */
    Relation heap = heap_open(shared->heaprelid, heapLockmode);
    Relation index = index_open(shared->indexrelid, indexLockmode);

    /* add snapshot for spi */
    PushActiveSnapshot(shared->snapshot);
    worker = spq_worker_init(heap, index, shared);
    worker->shared = shared;
    spq_worker_produce(worker);
    spq_worker_finish(worker);

#ifdef BTREE_BUILD_STATS
    if (u_sess->attr.attr_resource.log_btree_build_stats) {
        ResetUsage();
    }
#endif /* BTREE_BUILD_STATS */

    index_close(index, indexLockmode);
    heap_close(heap, heapLockmode);
    return;
}

SPQWorkerState *spq_worker_init(Relation heap, Relation index, SPQSharedContext* shared)
{
    SPQWorkerState *worker = (SPQWorkerState *)palloc0(sizeof(SPQWorkerState));
    worker->all_fetched = true;
    worker->heap = heap;
    worker->index = index;
    worker->sql = makeStringInfo();
    worker->shared = shared;

    bool old_enable_spq = u_sess->attr.attr_spq.gauss_enable_spq;
    bool old_spq_enable_index_scan = u_sess->attr.attr_spq.spq_optimizer_enable_indexscan;
    bool old_spq_enable_indexonly_scan = u_sess->attr.attr_spq.spq_optimizer_enable_indexonlyscan;
    bool old_spq_tx = u_sess->attr.attr_spq.spq_enable_transaction;
    bool old_spq_dop = u_sess->opt_cxt.query_dop;
    int old_num_nodes = t_thrd.spq_ctx.num_nodes;

    /* generate sql */
    {
        StringInfo attrs = makeStringInfo();     /* attrs in SELECT clause */
        StringInfo sortattrs = makeStringInfo(); /* attrs in ORDER BY clause */
        TupleDesc tupdes = RelationGetDescr(index);
        int natts = tupdes->natts;
        ScanKey scankey = _bt_mkscankey_nodata(index);
        Assert(natts > 0);

        for (int i = 0; i < natts; i++, scankey++) {
            Form_pg_attribute att = TupleDescAttr(tupdes, i);
            appendStringInfo(attrs, ", %s", NameStr(att->attname));

            appendStringInfo(sortattrs, "%s %s %s", NameStr(att->attname),
                             ((scankey->sk_flags & SK_BT_DESC) != 0) ? "desc" : "",
                             ((scankey->sk_flags & SK_BT_NULLS_FIRST) != 0) ? "nulls first" : "nulls last");
            if (i != natts - 1)
                appendStringInfo(sortattrs, ", ");
        }
        appendStringInfo(worker->sql, "select _root_ctid %s from %s order by %s, _root_ctid", attrs->data, RelationGetRelationName(heap),
                         sortattrs->data);

        elog(INFO, "sql: %s", worker->sql->data);
    }

    u_sess->attr.attr_spq.gauss_enable_spq = true;
    u_sess->attr.attr_spq.spq_optimizer_enable_indexscan = false;
    u_sess->attr.attr_spq.spq_optimizer_enable_indexonlyscan = false;
    u_sess->attr.attr_spq.spq_enable_transaction = true;
    u_sess->opt_cxt.query_dop = shared->dop;
    t_thrd.spq_ctx.num_nodes = shared->num_nodes;

    SPI_connect();

    if ((worker->plan = SPI_prepare_spq(worker->sql->data, 0, NULL)) == NULL)
        ereport(ERROR,
                (errcode(ERRCODE_SPI_PREPARE_FAILURE),
                 errmsg("SPI_prepare(\"%s\") failed: %s", worker->sql->data, SPI_result_code_string(SPI_result))));

    if ((worker->portal = SPI_cursor_open(NULL, worker->plan, NULL, NULL, true)) == NULL)
        ereport(ERROR,
                (errcode(ERRCODE_SPI_CURSOR_OPEN_FAILURE),
                 errmsg("SPI_cursor_open(\"%s\") failed: %s", worker->sql->data, SPI_result_code_string(SPI_result))));

    u_sess->attr.attr_spq.gauss_enable_spq = old_enable_spq;
    u_sess->attr.attr_spq.spq_optimizer_enable_indexscan = old_spq_enable_index_scan;
    u_sess->attr.attr_spq.spq_optimizer_enable_indexonlyscan = old_spq_enable_indexonly_scan;
    u_sess->attr.attr_spq.spq_enable_transaction = old_spq_tx;
    u_sess->opt_cxt.query_dop = old_spq_dop;
    t_thrd.spq_ctx.num_nodes = old_num_nodes;

    return worker;
}
 
void spq_worker_produce(SPQWorkerState *worker)
{
    SPQSharedContext *shared = worker->shared;
    IndexTupleBuffer *buffer = NULL;
 
    for (;;) {
        {
            while (true) {
                SpinLockAcquire(&shared->mutex);
                if (shared->bufferidx < SPQ_QUEUE_SIZE) {
                    int nextidx = GET_IDX(shared->producer);
                    shared->producer = nextidx;
                    SpinLockRelease(&shared->mutex);
                    buffer = GET_BUFFER(shared, nextidx);
                    elog(DEBUG3, "spq btbuild worker get buffer ok %d", nextidx);
                    break;
                }
                SpinLockRelease(&shared->mutex);
                condition_time_wait(shared);
            }
        }
 
        Assert(buffer);
        spq_init_buffer(buffer);
 
        {
            if (!worker->all_fetched) {
                /* save itup from last SPI_cursor_fetch at worker->processed */
                if (!index_form_tuple_buffer(worker, buffer, worker->processed)) {
                    goto produce_buffer;
                } else {
                    SPI_freetuptable(SPI_tuptable);
                }
            }
 
            /* fetch SPQ_BATCH_SIZE nums of tuples from portal */
            SPI_cursor_fetch(worker->portal, true, SPQ_BATCH_SIZE);
 
            if (SPI_processed == 0 && worker->all_fetched) {
                SpinLockAcquire(&shared->mutex);
                shared->done = true;
                SpinLockRelease(&shared->mutex);
                condition_signal(shared);
                return;
            }
 
            /* reset worker status, save itup from the beginning */
            worker->processed = 0;
            worker->all_fetched = true;
            if (!index_form_tuple_buffer(worker, buffer, 0)) {
                goto produce_buffer;
            } else {
                SPI_freetuptable(SPI_tuptable);
            }
 
        produce_buffer:
            SpinLockAcquire(&shared->mutex);
            shared->bufferidx++;
            SpinLockRelease(&shared->mutex);
            condition_signal(shared);
        }
    }
}
 
void spq_init_buffer(IndexTupleBuffer *buffer)
{
    buffer->queue_size = 0;
    buffer->offset = 0;
    buffer->idx = 0;
}
 
bool index_form_tuple_buffer(SPQWorkerState *worker, IndexTupleBuffer *buffer, uint64 begin)
{
    for (uint64 i = begin; i < SPI_processed; i++) {
        Datum values[INDEX_MAX_KEYS + 1];
        bool nulls[INDEX_MAX_KEYS + 1];
        IndexTuple ituple;
        ItemPointer ip;
        Size used;
        HeapTuple tup = SPI_tuptable->vals[i];
        heap_deform_tuple(tup, SPI_tuptable->tupdesc, values, nulls);
 
        ip = (ItemPointer)values[0];
        ituple =
            index_form_tuple_allocated(RelationGetDescr(worker->index), values + 1, nulls + 1,
                                       GET_BUFFER_MEM(buffer) + buffer->offset, SPQ_MEM_SIZE - buffer->offset, &used);
 
        if (ituple == NULL) {
            worker->processed = i;
            worker->all_fetched = false;
            return false;
        }
 
        SPQSharedContext *shared = worker->shared;
 
        elog(DEBUG5, "spq btbuild worker, put index tuple, buffer %d, offset %d, citd (%u, %u)", shared->producer,
             buffer->offset, ItemPointerGetBlockNumber(ip), ItemPointerGetOffsetNumber(ip));
 
        ituple->t_tid = *ip;
        buffer->addr[buffer->queue_size++] = buffer->offset;
        buffer->offset += used;
    }
    return true;
}
 
void spq_worker_finish(SPQWorkerState *worker)
{
    SPI_freetuptable(SPI_tuptable);
    SPI_cursor_close(worker->portal);
    SPI_freeplan(worker->plan);
    SPI_finish();
    PopActiveSnapshot();
}
 
void spq_leafbuild(SPQLeaderState *spqleader)
{
    BTWriteState wstate;

#ifdef BTREE_BUILD_STATS
    if (u_sess->attr.attr_resource.log_btree_build_stats) {
        ShowUsage("BTREE BUILD (Spool) STATISTICS");
        ResetUsage();
    }
#endif /* BTREE_BUILD_STATS */

    wstate.spqleader = spqleader;
    wstate.heap = spqleader->heap;
    wstate.index = spqleader->index;
    wstate.inskey = _bt_mkscankey(wstate.index, NULL);
    wstate.inskey->allequalimage = btree_allequalimage(wstate.index, true);
 
    /*
     * We need to log index creation in WAL iff WAL archiving/streaming is
     * enabled UNLESS the index isn't WAL-logged anyway.
     */
    wstate.btws_use_wal = XLogIsNeeded() && RelationNeedsWAL(wstate.index);
 
    /* reserve the metapage */
    wstate.btws_pages_alloced = BTREE_METAPAGE + 1;
    wstate.btws_pages_written = 0;
    wstate.btws_zeropage = NULL; /* until needed */
 
    spq_load(wstate);
 
    if (wstate.btws_zeropage != NULL) {
        pfree(wstate.btws_zeropage);
        wstate.btws_zeropage = NULL;
    }
}
 
IndexTuple spq_consume(SPQLeaderState *spqleader)
{
    SPQSharedContext *shared = spqleader->shared;
    IndexTupleBuffer *buffer = spqleader->buffer;
 
    for (;;) {
        if (buffer && buffer->idx < buffer->queue_size) {
            Size offset = buffer->addr[buffer->idx++];
            IndexTuple itup = (IndexTuple)(GET_BUFFER_MEM(buffer) + offset);
            elog(DEBUG5, "spq btbuild leader, get index tuple, buffer %d, offset %lu, citd (%u, %u)", shared->consumer,
                 offset, ItemPointerGetBlockNumber(&itup->t_tid), ItemPointerGetOffsetNumber(&itup->t_tid));
            return itup;
        }
 
        /* notify producer */
        if (buffer) {
            SpinLockAcquire(&shared->mutex);
            shared->bufferidx--;
            SpinLockRelease(&shared->mutex);
            condition_signal(shared);
            spqleader->processed++;
        }
 
        /* get buffer */
        {
            while (true) {
                SpinLockAcquire(&shared->mutex);
 
                if (shared->bufferidx > 0) {
                    int next = GET_IDX(shared->consumer);
                    shared->consumer = next;
                    SpinLockRelease(&shared->mutex);
                    elog(DEBUG3, "spq btbuild leader get buffer %d", next);
                    spqleader->buffer = buffer = GET_BUFFER(shared, next);
                    break;
                }
 
                if (shared->done) {
                    SpinLockRelease(&shared->mutex);
                    return NULL;
                }
                SpinLockRelease(&shared->mutex);
                condition_time_wait(shared);
            }
        }
    }
}
 
void condition_time_wait(SPQSharedContext *shared)
{
    struct timespec time_to_wait;
    (void)pthread_mutex_lock(&shared->m_mutex);
    (void)clock_gettime(CLOCK_MONOTONIC, &time_to_wait);
 
    time_to_wait.tv_nsec += 100 * NANOSECONDS_PER_MILLISECOND;
    if (time_to_wait.tv_nsec >= NANOSECONDS_PER_SECOND) {
        time_to_wait.tv_nsec -= NANOSECONDS_PER_SECOND;
        time_to_wait.tv_sec += 1;
    }
    int res = pthread_cond_timedwait(&shared->m_cond, &shared->m_mutex, &time_to_wait);
    if (res != 0 && res != ETIMEDOUT) {
        ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED), errmsg("spq btbuild error.")));
    }
    (void)pthread_mutex_unlock(&shared->m_mutex);
}
 
void condition_signal(SPQSharedContext *shared)
{
    (void)pthread_mutex_lock(&shared->m_mutex);
    (void)pthread_cond_signal(&shared->m_cond);
    (void)pthread_mutex_unlock(&shared->m_mutex);
}
#endif
