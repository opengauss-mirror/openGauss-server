/* ---------------------------------------------------------------------------------------
 *
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
 * ---------------------------------------------------------------------------------------
 *
 * parallel_reorderbuffer.cpp
 *        openGauss parallel decoding reorder buffer management
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/replication/logical/parallel_reorderbuffer.cpp
 *
 * NOTES
 *        This module handles the same question with reorderbuffer.cpp,
 *        under the circumstances of parallel decoding.
 * ---------------------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include <unistd.h>
#include <sys/stat.h>

#include "access/heapam.h"
#include "miscadmin.h"

#include "access/rewriteheap.h"
#include "access/transam.h"
#include "access/tuptoaster.h"
#include "access/xact.h"

#include "catalog/catalog.h"
#include "catalog/pg_namespace.h"
#include "lib/binaryheap.h"
#include "libpq/pqformat.h"

#include "replication/logical.h"
#include "replication/reorderbuffer.h"
#include "replication/parallel_decode.h"
#include "replication/logical.h"
#include "replication/slot.h"
#include "replication/snapbuild.h" /* just for SnapBuildSnapDecRefcount */
#include "access/xlog_internal.h"
#include "utils/atomic.h"

#include "storage/buf/bufmgr.h"
#include "storage/smgr/fd.h"
#include "storage/sinval.h"

#include "utils/lsyscache.h"
#include "utils/builtins.h"
#include "utils/combocid.h"
#include "utils/memutils.h"
#include "utils/relcache.h"

#include "utils/relfilenodemap.h"

static void ParallelReorderBufferSerializeReserve(ParallelReorderBuffer *rb, Size sz);
static void ParallelReorderBufferCheckSerializeTXN(ParallelReorderBuffer *rb, ParallelReorderBufferTXN *txn,
    int slotId);
static void ParallelReorderBufferSerializeTXN(ParallelReorderBuffer *rb, ParallelReorderBufferTXN *txn, int slotId);
static void ParallelReorderBufferSerializeChange(ParallelReorderBuffer *rb, ParallelReorderBufferTXN *txn, int fd,
    logicalLog *change);
static Size ParallelReorderBufferRestoreChanges(ParallelReorderBuffer *rb, ParallelReorderBufferTXN *txn, int *fd,
    XLogSegNo *segno, int slotId);
static void ParallelReorderBufferRestoreChange(ParallelReorderBuffer *rb, ParallelReorderBufferTXN *txn, char *data,
    int slotId);


/* Parallel decoding batch sending unit length is set to 1MB. */
static const int g_batch_unit_length = 1 * 1024 * 1024;

/* Parallel decoding caches at most 512 transactions. */
static const Size g_max_cached_txns = 512;

void ParallelReorderBufferQueueChange(ParallelReorderBuffer *rb, logicalLog *change, int slotId)
{
    ParallelReorderBufferTXN *txn = NULL;
    txn = ParallelReorderBufferTXNByXid(rb, change->xid, true, NULL, change->lsn, true);

    dlist_push_tail(&txn->changes, &change->node);
    txn->nentries++;
    txn->nentries_mem++;

    ParallelReorderBufferCheckSerializeTXN(rb, txn, slotId);
}

void ParallelFreeTuple(ReorderBufferTupleBuf *tuple, int slotId)
{
    uint32 curTupleNum = g_Logicaldispatcher[slotId].curTupleNum;
    if (curTupleNum >= max_decode_cache_num ||
        tuple->alloc_tuple_size > Max(MaxHeapTupleSize, MaxPossibleUHeapTupleSize)) {
        pfree(tuple);
        (void)pg_atomic_sub_fetch_u32(&g_Logicaldispatcher[slotId].curTupleNum, 1);
        return;
    }

    ReorderBufferTupleBuf *oldHead =
        (ReorderBufferTupleBuf *)pg_atomic_read_uintptr((uintptr_t *)&g_Logicaldispatcher[slotId].freeTupleHead);
    do {
        tuple->freeNext = oldHead;
    } while (!pg_atomic_compare_exchange_uintptr((uintptr_t *)&g_Logicaldispatcher[slotId].freeTupleHead,
        (uintptr_t *)&oldHead, (uintptr_t)tuple));
}

void ParallelFreeChange(ParallelReorderBufferChange *change, int slotId)
{
    ParallelReorderBufferToastReset(change, slotId);
    uint32 curChangeNum = g_Logicaldispatcher[slotId].curChangeNum;

    if (change->data.tp.newtuple) {
        ParallelFreeTuple(change->data.tp.newtuple, slotId);
    }
    if (change->data.tp.oldtuple) {
        ParallelFreeTuple(change->data.tp.oldtuple, slotId);
    }

    if (curChangeNum >= max_decode_cache_num) {
        pfree(change);
        (void)pg_atomic_sub_fetch_u32(&g_Logicaldispatcher[slotId].curChangeNum, 1);
        return;
    }

    ParallelReorderBufferChange *oldHead =
        (ParallelReorderBufferChange *)pg_atomic_read_uintptr((uintptr_t *)&g_Logicaldispatcher[slotId].freeChangeHead);

    do {
        change->freeNext = oldHead;
    } while (!pg_atomic_compare_exchange_uintptr((uintptr_t *)&g_Logicaldispatcher[slotId].freeChangeHead,
        (uintptr_t *)&oldHead, (uintptr_t)change));
}

/*
 * Get a unused ParallelReorderBufferChange, which may be preallocated in a global resource list.
 */
ParallelReorderBufferChange* ParallelReorderBufferGetChange(ParallelReorderBuffer *rb, int slotId)
{
    ParallelReorderBufferChange *change = NULL;
    MemoryContext oldCtx = NULL;

    errno_t rc = 0;
    do {
        if (g_Logicaldispatcher[slotId].freeGetChangeHead != NULL) {
            change = g_Logicaldispatcher[slotId].freeGetChangeHead;
            g_Logicaldispatcher[slotId].freeGetChangeHead = g_Logicaldispatcher[slotId].freeGetChangeHead->freeNext;
        } else {
            ParallelReorderBufferChange *head =
                (ParallelReorderBufferChange *)pg_atomic_exchange_uintptr(
                (uintptr_t *)&g_Logicaldispatcher[slotId].freeChangeHead,
                (uintptr_t)NULL);
            if (head != NULL) {
                change = head;
                g_Logicaldispatcher[slotId].freeGetChangeHead = head->freeNext;
            } else {
                (void)pg_atomic_add_fetch_u32(&g_Logicaldispatcher[slotId].curChangeNum, 1);
                oldCtx = MemoryContextSwitchTo(g_instance.comm_cxt.pdecode_cxt[slotId].parallelDecodeCtx);
                change = (ParallelReorderBufferChange *)palloc0(sizeof(ParallelReorderBufferChange));
                rc = memset_s(change, sizeof(ParallelReorderBufferChange), 0, sizeof(ParallelReorderBufferChange));
                securec_check(rc, "", "");
                MemoryContextSwitchTo(oldCtx);
            }
        }
    } while (change == NULL);
    rc = memset_s(&change->data, sizeof(change->data), 0, sizeof(change->data));
    securec_check(rc, "", "");
    change->ninvalidations = 0;
    change->invalidations = NULL;
    change->toast_hash = NULL;
    return change;
}

ReorderBufferTupleBuf *ParallelReorderBufferGetTupleBuf(ParallelReorderBuffer *rb, Size tuple_len,
    ParallelDecodeReaderWorker *worker, bool isHeapTuple)
{
    int slotId = worker->slotId;
    ReorderBufferTupleBuf *tuple = NULL;
    Size allocLen = 0;
    Size maxSize = 0;

    allocLen = tuple_len + (isHeapTuple ? SizeofHeapTupleHeader : SizeOfUHeapDiskTupleData);
    maxSize = Max(MaxHeapTupleSize, MaxPossibleUHeapTupleSize);
    MemoryContext oldCtx = NULL;

    /*
     * Most tuples are below maxSize, so we use a slab allocator for those.
     * Thus always allocate at least maxSize. Note that tuples generated for
     * oldtuples can be bigger, as they don't have out-of-line toast columns.
     */
    if (allocLen < maxSize) {
        allocLen = maxSize;
    }

    /* if small enough, check the slab cache */
    if (allocLen <= maxSize) {
        do {
            if (g_Logicaldispatcher[slotId].freeGetTupleHead != NULL) {
                tuple = g_Logicaldispatcher[slotId].freeGetTupleHead;
                g_Logicaldispatcher[slotId].freeGetTupleHead = g_Logicaldispatcher[slotId].freeGetTupleHead->freeNext;
                continue;
            }
            ReorderBufferTupleBuf *head = (ReorderBufferTupleBuf *)pg_atomic_exchange_uintptr(
                (uintptr_t *)&g_Logicaldispatcher[slotId].freeTupleHead, (uintptr_t)NULL);
            if (head != NULL) {
                tuple = head;
                g_Logicaldispatcher[slotId].freeGetTupleHead = head->freeNext;
            } else {
                (void)pg_atomic_add_fetch_u32(&g_Logicaldispatcher[slotId].curTupleNum, 1);
                oldCtx = MemoryContextSwitchTo(g_instance.comm_cxt.pdecode_cxt[slotId].parallelDecodeCtx);
                tuple = (ReorderBufferTupleBuf *)palloc0(sizeof(ReorderBufferTupleBuf) + allocLen);
                MemoryContextSwitchTo(oldCtx);
                tuple->alloc_tuple_size = allocLen;
                tuple->tuple.t_data = isHeapTuple ? ReorderBufferTupleBufData(tuple) :
                    (HeapTupleHeader)ReorderBufferUTupleBufData((ReorderBufferUTupleBuf *)tuple);
            }
        } while (tuple == NULL);
    } else {
        oldCtx = MemoryContextSwitchTo(g_instance.comm_cxt.pdecode_cxt[slotId].parallelDecodeCtx);
        tuple = (ReorderBufferTupleBuf *)palloc0(sizeof(ReorderBufferTupleBuf) + allocLen);
        MemoryContextSwitchTo(oldCtx);
        tuple->alloc_tuple_size = allocLen;
        tuple->tuple.t_data = isHeapTuple ? ReorderBufferTupleBufData(tuple) :
            (HeapTupleHeader)ReorderBufferUTupleBufData((ReorderBufferUTupleBuf *)tuple);
    }
    tuple->tuple.tupTableType = isHeapTuple ? HEAP_TUPLE : UHEAP_TUPLE;
    return tuple;
}

ParallelReorderBufferTXN *ParallelReorderBufferGetTXN(ParallelReorderBuffer *rb)
{
    ParallelReorderBufferTXN *txn = NULL;
    int rc = 0;
    /* check the slab cache */
    if (rb->nr_cached_transactions > 0) {
        rb->nr_cached_transactions--;
        txn = (ParallelReorderBufferTXN *)dlist_container(ParallelReorderBufferTXN, node,
                                                  dlist_pop_head_node(&rb->cached_transactions));
    } else {
        txn = (ParallelReorderBufferTXN *)palloc(sizeof(ParallelReorderBufferTXN));
    }

    rc = memset_s(txn, sizeof(ParallelReorderBufferTXN), 0, sizeof(ParallelReorderBufferTXN));
    securec_check(rc, "", "");

    dlist_init(&txn->changes);
    dlist_init(&txn->tuplecids);
    dlist_init(&txn->subtxns);

    return txn;
}

void ParallelReorderBufferReturnTXN(ParallelReorderBuffer *rb, ParallelReorderBufferTXN *txn)
{
    /* clean the lookup cache if we were cached (quite likely) */
    if (rb->by_txn_last_xid == txn->xid) {
        rb->by_txn_last_xid = InvalidTransactionId;
        rb->by_txn_last_txn = NULL;
    }

    /* free data that's contained */
    if (txn->tuplecid_hash != NULL) {
        hash_destroy(txn->tuplecid_hash);
        txn->tuplecid_hash = NULL;
    }

    if (txn->invalidations) {
        pfree(txn->invalidations);
        txn->invalidations = NULL;
    }

    txn->nentries = 0;
    txn->nentries_mem = 0;
    /* check whether to put into the slab cache */
    if (rb->nr_cached_transactions < g_max_cached_txns) {
        rb->nr_cached_transactions++;
        dlist_push_head(&rb->cached_transactions, &txn->node);
    } else {
        pfree(txn);
    }
}
/* ---------------------------------------
 * toast reassembly support
 * ---------------------------------------
 *
 *
 * Initialize per tuple toast reconstruction support.
 */
static void parallelReorderBufferToastInitHash(ParallelReorderBuffer *rb, ParallelReorderBufferTXN *txn)
{
    HASHCTL hash_ctl;
    int rc = 0;
    Assert(txn->toast_hash == NULL);

    rc = memset_s(&hash_ctl, sizeof(hash_ctl), 0, sizeof(hash_ctl));
    securec_check(rc, "", "");

    hash_ctl.keysize = sizeof(Oid);
    hash_ctl.entrysize = sizeof(ReorderBufferToastEnt);
    hash_ctl.hash = tag_hash;
    hash_ctl.hcxt = rb->context;
    const long toastHashNelem = 5;
    txn->toast_hash = hash_create("ParallelReorderBufferToastHash", toastHashNelem, &hash_ctl,
        HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT | HASH_SHRCTX);
}

void ToastTupleAppendChunk(ParallelReorderBuffer *rb, Relation relation,
    ParallelReorderBufferChange *change)
{
    ReorderBufferToastEnt *ent = NULL;
    ReorderBufferTupleBuf *newtup = NULL;
    bool found = false;
    int32 chunksize = 0;
    bool isnull = false;
    Pointer chunk = NULL;
    TupleDesc desc = RelationGetDescr(relation);
    Oid chunk_id = InvalidOid;
    Oid chunk_seq = InvalidOid;
    ParallelReorderBufferTXN *txn = ParallelReorderBufferTXNByXid(rb, change->xid, true, NULL, change->lsn, true);
    if (txn->toast_hash == NULL) {
        parallelReorderBufferToastInitHash(rb, txn);
    }

    Assert(IsToastRelation(relation));

    const int chunkIdIndex = 1;
    const int chunkSeqIndex = 2;
    const int chunkIndex = 3;
    newtup = change->data.tp.newtuple;
    if (change->action == PARALLEL_REORDER_BUFFER_CHANGE_INSERT) {
        chunk_id = DatumGetObjectId(fastgetattr(&newtup->tuple, chunkIdIndex, desc, &isnull));
        Assert(!isnull);
        chunk_seq = DatumGetInt32(fastgetattr(&newtup->tuple, chunkSeqIndex, desc, &isnull));
        Assert(!isnull);
    } else {
        chunk_id = DatumGetObjectId(UHeapFastGetAttr((UHeapTuple)&newtup->tuple, 1, desc, &isnull));
        Assert(!isnull);
        chunk_seq = DatumGetInt32(UHeapFastGetAttr((UHeapTuple)&newtup->tuple, 2, desc, &isnull));
        Assert(!isnull);
    }

    ent = (ReorderBufferToastEnt *)hash_search(txn->toast_hash, (void *)&chunk_id, HASH_ENTER, &found);

    if (!found) {
        Assert(ent->chunk_id == chunk_id);
        ent->num_chunks = 0;
        ent->last_chunk_seq = 0;
        ent->size = 0;
        ent->reconstructed = NULL;
        dlist_init(&ent->chunks);

        if (chunk_seq != 0) {
            ereport(ERROR, (errmodule(MOD_LOGICAL_DECODE), errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("got sequence entry %u for toast chunk %u instead of seq 0", chunk_seq, chunk_id),
                errdetail("N/A"), errcause("Toast file damaged."),
                erraction("Contact engineer to recover toast files.")));
        }
    } else if (found && chunk_seq != (Oid)ent->last_chunk_seq + 1) {
        ereport(ERROR, (errmodule(MOD_LOGICAL_DECODE), errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("got sequence entry %u for toast chunk %u instead of seq %d", chunk_seq, chunk_id,
                ent->last_chunk_seq + 1),
            errdetail("N/A"), errcause("Toast file damaged."),
            erraction("Contact engineer to recover toast files.")));
    }
    if (change->action == PARALLEL_REORDER_BUFFER_CHANGE_INSERT) {
        chunk = DatumGetPointer(fastgetattr(&newtup->tuple, chunkIndex, desc, &isnull));
    } else {
        chunk = DatumGetPointer(UHeapFastGetAttr((UHeapTuple)&newtup->tuple, chunkIndex, desc, &isnull));
     }
    Assert(!isnull);
    if (isnull) {
        ereport(ERROR, (errmodule(MOD_LOGICAL_DECODE), errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("fail to get toast chunk"),
            errdetail("N/A"), errcause("Toast file damaged."),
            erraction("Contact engineer to recover toast files.")));
    }
    checkHugeToastPointer((varlena *)chunk);
    /* calculate size so we can allocate the right size at once later */
    if (!VARATT_IS_EXTENDED(chunk)) {
        chunksize = VARSIZE(chunk) - VARHDRSZ;
    } else if (VARATT_IS_SHORT(chunk)) {
        /* could happen due to heap_form_tuple doing its thing */
        chunksize = VARSIZE_SHORT(chunk) - VARHDRSZ_SHORT;
    } else {
        ereport(ERROR, (errmodule(MOD_LOGICAL_DECODE), errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("unexpected type of toast chunk"),
            errdetail("N/A"), errcause("Toast file damaged."),
            erraction("Contact engineer to recover toast files.")));
    }

    ent->size += chunksize;
    ent->last_chunk_seq = chunk_seq;
    ent->num_chunks++;
    ereport(LOG, (errmsg("applying mapping: %lX %lX %lX %lX", uint64(ent), change->xid, change->lsn,
        uint64(change->data.tp.newtuple))));
    dlist_push_tail(&ent->chunks, &change->node);
}

/*
 * Free all resources allocated for toast reconstruction.
 */
void ParallelReorderBufferToastReset(ParallelReorderBufferChange *change, int slotId)
{
    HASH_SEQ_STATUS hstat;
    ReorderBufferToastEnt *ent = NULL;

    if (change->toast_hash == NULL) {
        return;
    }

    /* sequentially walk over the hash and free everything */
    hash_seq_init(&hstat, change->toast_hash);
    while ((ent = (ReorderBufferToastEnt *)hash_seq_search(&hstat)) != NULL) {
        dlist_mutable_iter it;

        if (ent->reconstructed != NULL) {
            pfree(ent->reconstructed);
            ent->reconstructed = NULL;
        }

        dlist_foreach_modify(it, &ent->chunks)
        {
            ParallelReorderBufferChange *change = dlist_container(ParallelReorderBufferChange, node, it.cur);

            dlist_delete(&change->node);
            ParallelFreeChange(change, slotId);
        }
    }

    hash_destroy(change->toast_hash);
    change->toast_hash = NULL;
}

/*
 * Splice toast tuple.
 */
void ToastTupleSplicing(Datum *attrs, TupleDesc desc, bool *isnull, TupleDesc toast_desc, ParallelReorderBufferTXN *txn,
    bool isHeap, bool *free)
{
    int rc = 0;
    int natt = 0;
    const int toast_index = 3; /* toast index in tuple is 3 */

    for (natt = 0; natt < desc->natts; natt++) {
        Form_pg_attribute attr = desc->attrs[natt];
        ReorderBufferToastEnt *ent = NULL;
        struct varlena *varlena = NULL;

        /* va_rawsize is the size of the original datum -- including header */
        struct varatt_external toast_pointer;
        struct varatt_indirect redirect_pointer;
        struct varlena *new_datum = NULL;
        struct varlena *reconstructed = NULL;
        dlist_iter it;
        Size data_done = 0;
        int2 bucketid;

        /* system columns aren't toasted */
        if (attr->attnum < 0)
            continue;

        if (attr->attisdropped)
            continue;

        /* not a varlena datatype */
        if (attr->attlen != -1)
            continue;

        /* no data */
        if (isnull[natt])
            continue;

        /* ok, we know we have a toast datum */
        varlena = (struct varlena *)DatumGetPointer(attrs[natt]);
        checkHugeToastPointer(varlena);
        /* no need to do anything if the tuple isn't external */
        if (!VARATT_IS_EXTERNAL(varlena))
            continue;

        VARATT_EXTERNAL_GET_POINTER_B(toast_pointer, varlena, bucketid);

        /*
         * Check whether the toast tuple changed, replace if so.
         */
        ent = (ReorderBufferToastEnt *)hash_search(txn->toast_hash, (void *)&toast_pointer.va_valueid, HASH_FIND, NULL);
        if (ent == NULL)
            continue;

        new_datum = (struct varlena *)palloc0(INDIRECT_POINTER_SIZE);

        free[natt] = true;

        reconstructed = (struct varlena *)palloc0(toast_pointer.va_rawsize);

        ent->reconstructed = reconstructed;

        /* stitch toast tuple back together from its parts */
        dlist_foreach(it, &ent->chunks)
        {
            bool isnul = false;
            ParallelReorderBufferChange *cchange = NULL;
            ReorderBufferTupleBuf *ctup = NULL;
            Pointer chunk = NULL;
            
            cchange = dlist_container(ParallelReorderBufferChange, node, it.cur);
            ctup = cchange->data.tp.newtuple;
            if (isHeap) {
                chunk = DatumGetPointer(fastgetattr(&ctup->tuple, toast_index, toast_desc, &isnul));
            } else {
                chunk = DatumGetPointer(UHeapFastGetAttr((UHeapTuple)&ctup->tuple, toast_index, toast_desc, &isnul));
            }
            if (chunk == NULL) {
                continue;
            }
            Assert(!isnul);
            Assert(!VARATT_IS_EXTERNAL(chunk));
            Assert(!VARATT_IS_SHORT(chunk));
            rc = memcpy_s(VARDATA(reconstructed) + data_done, VARSIZE(chunk) - VARHDRSZ, VARDATA(chunk),
                VARSIZE(chunk) - VARHDRSZ);
            securec_check(rc, "", "");
            data_done += VARSIZE(chunk) - VARHDRSZ;
        }
        Assert(data_done == (Size)toast_pointer.va_extsize);

        /* make sure its marked as compressed or not */
        if (VARATT_EXTERNAL_IS_COMPRESSED(toast_pointer)) {
            SET_VARSIZE_COMPRESSED(reconstructed, data_done + VARHDRSZ);
        } else {
            SET_VARSIZE(reconstructed, data_done + VARHDRSZ);
        }

        rc = memset_s(&redirect_pointer, sizeof(redirect_pointer), 0, sizeof(redirect_pointer));
        securec_check(rc, "", "");
        redirect_pointer.pointer = reconstructed;
    
        SET_VARTAG_EXTERNAL(new_datum, VARTAG_INDIRECT);
        rc = memcpy_s(VARDATA_EXTERNAL(new_datum), sizeof(redirect_pointer), &redirect_pointer,
            sizeof(redirect_pointer));
        securec_check(rc, "", "");

        attrs[natt] = PointerGetDatum(new_datum);
    }
}
/*
 * Query whether toast tuples have been stored in the current Txn->toast_hash before.
 * If so, merge the tuples previously stored in the current Txn->toast_hash with the toast tuples
 * in the current table into a complete toast tuple
 */
void ToastTupleReplace(ParallelReorderBuffer *rb, Relation relation, ParallelReorderBufferChange *change,
    Oid partationReltoastrelid, ParallelDecodeReaderWorker *worker, bool isHeap)
{
    TupleDesc desc = NULL;
    Datum *attrs = NULL;
    bool *isnull = NULL;
    bool *free = NULL;
    HeapTuple tmphtup = NULL;
    Relation toast_rel = NULL;
    TupleDesc toast_desc = NULL;
    MemoryContext oldcontext = NULL;
    ReorderBufferTupleBuf *newtup = NULL;
    ParallelReorderBufferTXN *txn;
    int rc = 0;

    txn = ParallelReorderBufferTXNByXid(rb, change->xid, false, NULL, change->lsn, true);
    if (txn == NULL) {
        return;
    }
    /* no toast tuples changed */
    if (txn->toast_hash == NULL)
        return;
    oldcontext = MemoryContextSwitchTo(rb->context);

    /* we should only have toast tuples in an INSERT or UPDATE */
    Assert(change->data.tp.newtuple);

    desc = RelationGetDescr(relation);
    if (relation->rd_rel->reltoastrelid != InvalidOid)
        toast_rel = RelationIdGetRelation(relation->rd_rel->reltoastrelid);
    else
        toast_rel = RelationIdGetRelation(partationReltoastrelid);
    if (toast_rel == NULL) {
        ereport(ERROR, (errcode(ERRCODE_LOGICAL_DECODE_ERROR), errmodule(MOD_MEM),
            errmsg("toast_rel should not be NULL!"),
            errdetail("N/A"), errcause("Toast file damaged."),
            erraction("Contact engineer to recover toast files.")));
    }
    toast_desc = RelationGetDescr(toast_rel);

    /* should we allocate from stack instead? */
    attrs = (Datum *)palloc0(sizeof(Datum) * desc->natts);
    isnull = (bool *)palloc0(sizeof(bool) * desc->natts);
    free = (bool *)palloc0(sizeof(bool) * desc->natts);

    newtup = change->data.tp.newtuple;

    if (isHeap) {
        heap_deform_tuple(&newtup->tuple, desc, attrs, isnull);
    } else {
        UHeapDeformTuple((UHeapTuple)&newtup->tuple, desc, attrs, isnull);
    }

    ToastTupleSplicing(attrs, desc, isnull, toast_desc, txn, isHeap, free);

    /*
     * Build tuple in separate memory & copy tuple back into the tuplebuf
     * passed to the output plugin. We can't directly heap_fill_tuple() into
     * the tuplebuf because attrs[] will point back into the current content.
     */
    if (isHeap) {
        tmphtup = heap_form_tuple(desc, attrs, isnull);
    } else {
        tmphtup = (HeapTuple)UHeapFormTuple(desc, attrs, isnull);
    }

    Assert(ReorderBufferTupleBufData(newtup) == newtup->tuple.t_data);

    rc = memcpy_s(newtup->tuple.t_data, newtup->alloc_tuple_size, tmphtup->t_data, tmphtup->t_len);
    securec_check(rc, "", "");
    newtup->tuple.t_len = tmphtup->t_len;

    /*
     * free resources we won't further need, more persistent stuff will be
     * free'd in ReorderBufferToastReset().
     */
    RelationClose(toast_rel);
    pfree(tmphtup);
    tmphtup = NULL;
    for (int natt = 0; natt < desc->natts; natt++) {
        if (free[natt]) {
            pfree(DatumGetPointer(attrs[natt]));
        }
    }
    pfree(attrs);
    attrs = NULL;
    pfree(isnull);
    isnull = NULL;
    pfree(free);
    free = NULL;

    (void)MemoryContextSwitchTo(oldcontext);
    change->toast_hash = txn->toast_hash;
    txn->toast_hash = NULL;
}

/*
 * Remove all on-disk stored logical log in parallel decoding.
 */
static void ParallelReorderBufferRestoreCleanup(ParallelReorderBufferTXN *txn, XLogRecPtr lsn = InvalidXLogRecPtr)
{
    if (txn->final_lsn == InvalidXLogRecPtr) {
        txn->final_lsn = lsn;
    }
    Assert(!XLByteEQ(txn->first_lsn, InvalidXLogRecPtr));

    XLogSegNo first = (txn->first_lsn) / XLogSegSize;
    XLogSegNo last = (txn->final_lsn) / XLogSegSize;

    /* iterate over all possible filenames, and delete them */
    for (XLogSegNo cur = first; cur <= last; cur++) {
        char path[MAXPGPATH];
        XLogRecPtr recptr;
        recptr = (cur * XLOG_SEG_SIZE);
        errno_t rc = sprintf_s(path, sizeof(path), "pg_replslot/%s/snap/xid-%lu-lsn-%X-%X.snap",
            t_thrd.walsender_cxt.slotname, txn->xid, (uint32)(recptr >> 32), uint32(recptr));
        securec_check_ss(rc, "", "");
        if (unlink(path) != 0 && errno != ENOENT) {
            ereport(ERROR, (errcode_for_file_access(), errmsg("could not unlink file \"%s\": %m", path)));
        }
    }
}

/*
 * Parallel decoding cleanup txn.
 */
void ParallelReorderBufferCleanupTXN(ParallelReorderBuffer *rb, ParallelReorderBufferTXN *txn,
    XLogRecPtr lsn = InvalidXLogRecPtr)
{
    bool found = false;
    dlist_mutable_iter iter;
    dlist_foreach_modify(iter, &txn->subtxns)
    {
        ParallelReorderBufferTXN *subtxn = NULL;

        subtxn = dlist_container(ParallelReorderBufferTXN, node, iter.cur);

        /*
         * Subtransactions are always associated to the toplevel TXN, even if
         * they originally were happening inside another subtxn, so we won't
         * ever recurse more than one level deep here.
         */
        Assert(subtxn->is_known_as_subxact);
        Assert(subtxn->nsubtxns == 0);
        ParallelReorderBufferCleanupTXN(rb, subtxn, lsn);
    }
    /*
     * Remove TXN from its containing list.
     *
     * Note: if txn->is_known_as_subxact, we are deleting the TXN from its
     * parent's list of known subxacts; this leaves the parent's nsubxacts
     * count too high, but we don't care.  Otherwise, we are deleting the TXN
     * from the LSN-ordered list of toplevel TXNs.
     */
    dlist_delete(&txn->node);

    /* now remove reference from buffer */
    (void)hash_search(rb->by_txn, (void *)&txn->xid, HASH_REMOVE, &found);
    Assert(found);

    /* remove entries spilled to disk */
    if (txn->serialized) {
        ParallelReorderBufferRestoreCleanup(txn, lsn);
    }

    /* deallocate */
    ParallelReorderBufferReturnTXN(rb, txn);
}

/*
 * ---------------------------------------
 * Disk serialization support
 * ---------------------------------------
 *
 *
 * Ensure the IO buffer is >= sz.
 */
static void ParallelReorderBufferSerializeReserve(ParallelReorderBuffer *rb, Size sz)
{
    if (!rb->outbufsize) {
        rb->outbuf = (char *)MemoryContextAlloc(rb->context, sz);
        rb->outbufsize = sz;
    } else if (rb->outbufsize < sz) {
        rb->outbuf = (char *)repalloc(rb->outbuf, sz);
        rb->outbufsize = sz;
    }
}

/*
 * Check whether the transaction tx should spill its data to disk.
 */
static void ParallelReorderBufferCheckSerializeTXN(ParallelReorderBuffer *rb, ParallelReorderBufferTXN *txn, int slotId)
{
    if (txn->nentries_mem >= (unsigned)g_instance.attr.attr_common.max_changes_in_memory) {
        ParallelReorderBufferSerializeTXN(rb, txn, slotId);
        Assert(txn->nentries_mem == 0);
    }
}

/*
 * Spill data of a large transaction (and its subtransactions) to disk.
 */

static void ParallelReorderBufferSerializeTXN(ParallelReorderBuffer *rb, ParallelReorderBufferTXN *txn, int slotId)
{
    dlist_iter subtxn_i;
    dlist_mutable_iter change_i;
    int fd = -1;
    XLogSegNo curOpenSegNo = 0;

    Size spilled = 0;
    char path[MAXPGPATH];
    int nRet = 0;

    if (!RecoveryInProgress()) {
        ereport(DEBUG2, (errmsg("spill %u changes in tx %lu to disk", (uint32)txn->nentries_mem, txn->xid)));
    }

    /* do the same to all child TXs */
    if (&txn->subtxns == NULL) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("txn->subtxns is illegal point")));
    }
    dlist_foreach(subtxn_i, &txn->subtxns)
    {
        ParallelReorderBufferTXN *subtxn = NULL;

        subtxn = dlist_container(ParallelReorderBufferTXN, node, subtxn_i.cur);
        ParallelReorderBufferSerializeTXN(rb, subtxn, slotId);
    }

    /* serialize changestream */
    dlist_foreach_modify(change_i, &txn->changes)
    {
        logicalLog *change = NULL;

        change = dlist_container(logicalLog, node, change_i.cur);
        /*
         * store in segment in which it belongs by start lsn, don't split over
         * multiple segments tho
         */
        if (fd == -1 || !((change->lsn) / XLogSegSize == curOpenSegNo)) {
            XLogRecPtr recptr;

            if (fd != -1) {
                (void)CloseTransientFile(fd);
            }
            curOpenSegNo = (change->lsn) / XLogSegSize;

            recptr = (curOpenSegNo * XLogSegSize);

            /*
             * No need to care about TLIs here, only used during a single run,
             * so each LSN only maps to a specific WAL record.
             */

            nRet = sprintf_s(path, MAXPGPATH, "pg_replslot/%s/snap/xid-%lu-lsn-%X-%X.snap",
                             t_thrd.walsender_cxt.slotname, txn->xid, (uint32)(recptr >> 32),
                             (uint32)recptr);

            securec_check_ss(nRet, "", "");
            /* open segment, create it if necessary */
            fd = OpenTransientFile(path, O_CREAT | O_WRONLY | O_APPEND | PG_BINARY, S_IRUSR | S_IWUSR);
            if (fd < 0) {
                ereport(ERROR, (errcode_for_file_access(), errmsg("could not open file \"%s\": %m", path)));
            }
        }

        ParallelReorderBufferSerializeChange(rb, txn, fd, change);
        dlist_delete(&change->node);
        FreeLogicalLog(change, slotId);
        spilled++;
    }

    Assert(spilled == txn->nentries_mem);
    Assert(dlist_is_empty(&txn->changes));
    txn->serialized = true;
    txn->nentries_mem = 0;

    if (fd != -1) {
        (void)CloseTransientFile(fd);
    }
}

/*
 * Serialize individual change to disk.
 */
static void ParallelReorderBufferSerializeChange(ParallelReorderBuffer *rb, ParallelReorderBufferTXN *txn, int fd,
    logicalLog *change)
{
    Size sz = sizeof(sz) + sizeof(XLogRecPtr) + change->out->len;
    ParallelReorderBufferSerializeReserve(rb, sz);

    char* tmp = rb->outbuf;
    errno_t rc = memcpy_s(tmp, sizeof(Size), &sz, sizeof(Size));
    securec_check(rc, "", "");
    tmp += sizeof(Size);

    rc = memcpy_s(tmp, sizeof(XLogRecPtr), &change->lsn, sizeof(XLogRecPtr));
    securec_check(rc, "", "");
    tmp += sizeof(XLogRecPtr);
    if (change->out->len != 0) {
        rc = memcpy_s(tmp, change->out->len, change->out->data, change->out->len);
        securec_check(rc, "", "");
    }

    if ((Size)(write(fd, rb->outbuf, sz)) != sz) {
        (void)CloseTransientFile(fd);
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not write to xid %lu's data file: %m", txn->xid)));
    }
}

/*
 * Restore changes from disk.
 */
static Size ParallelReorderBufferRestoreChanges(ParallelReorderBuffer *rb, ParallelReorderBufferTXN *txn, int *fd,
    XLogSegNo *segno, int slotId)
{
    Size restored = 0;
    XLogSegNo last_segno;
    int rc = 0;
    dlist_mutable_iter cleanup_iter;

    Assert(!XLByteEQ(txn->first_lsn, InvalidXLogRecPtr));
    Assert(!XLByteEQ(txn->final_lsn, InvalidXLogRecPtr));

    /* free current entries, so we have memory for more */
    dlist_foreach_modify(cleanup_iter, &txn->changes)
    {
        logicalLog *cleanup = dlist_container(logicalLog, node, cleanup_iter.cur);

        dlist_delete(&cleanup->node);
        FreeLogicalLog(cleanup, slotId);
    }
    txn->nentries_mem = 0;
    Assert(dlist_is_empty(&txn->changes));

    last_segno = (txn->final_lsn) / XLogSegSize;
    while (restored < (unsigned)g_instance.attr.attr_common.max_changes_in_memory && *segno <= last_segno) {
        int readBytes = 0;

        if (*fd == -1) {
            XLogRecPtr recptr;
            char path[MAXPGPATH];

            /* first time in */
            if (*segno == 0) {
                *segno = (txn->first_lsn) / XLogSegSize;
            }

            Assert(*segno != 0 || dlist_is_empty(&txn->changes));

            recptr = (*segno * XLOG_SEG_SIZE);

            /*
             * No need to care about TLIs here, only used during a single run,
             * so each LSN only maps to a specific WAL record.
             */

            rc = sprintf_s(path, sizeof(path), "pg_replslot/%s/snap/xid-%lu-lsn-%X-%X.snap",
                           t_thrd.walsender_cxt.slotname, txn->xid, (uint32)(recptr >> 32),
                           (uint32)recptr);

            securec_check_ss(rc, "", "");
            *fd = OpenTransientFile(path, O_RDONLY | PG_BINARY, 0);
            if (*fd < 0 && errno == ENOENT) {
                *fd = -1;
                (*segno)++;
                continue;
            } else if (*fd < 0)
                ereport(ERROR, (errcode_for_file_access(), errmsg("could not open file \"%s\": %m", path)));
        }

        /*
         * Read the statically sized part of a change which has information
         * about the total size. If we couldn't read a record, we're at the
         * end of this file.
         */
        ParallelReorderBufferSerializeReserve(rb, sizeof(Size) + sizeof(XLogRecPtr));
        readBytes = read(*fd, rb->outbuf, sizeof(Size) + sizeof(XLogRecPtr));
        /* eof */
        if (readBytes == 0) {
            (void)CloseTransientFile(*fd);
            *fd = -1;
            (*segno)++;
            continue;
        } else if (readBytes < 0) {
            ereport(ERROR, (errcode_for_file_access(), errmsg("could not read from reorderbuffer spill file: %m")));
        } else if (readBytes != sizeof(Size) + sizeof(XLogRecPtr)) {
            ereport(ERROR, (errcode_for_file_access(),
                            errmsg("incomplete read from reorderbuffer spill file: read %d instead of %lu", readBytes,
                                   sizeof(Size) + sizeof(XLogRecPtr))));
        }

        Size ondiskSize = *(Size *)rb->outbuf;
        ParallelReorderBufferSerializeReserve(rb, ondiskSize);

        if (ondiskSize == sizeof(Size) + sizeof(XLogRecPtr)) {
            /* Nothing serialized on disk, so skip it */
            restored++;
            continue;
        }
        readBytes = read(*fd, rb->outbuf + sizeof(Size) + sizeof(XLogRecPtr),
            ondiskSize - sizeof(Size) - sizeof(XLogRecPtr));
        if (readBytes < 0) {
            ereport(ERROR, (errcode_for_file_access(), errmsg("could not read from reorderbuffer spill file: %m")));
        } else if (INT2SIZET(readBytes) != ondiskSize - sizeof(Size) - sizeof(XLogRecPtr)) {
            ereport(ERROR, (errcode_for_file_access(),
                            errmsg("could not read from reorderbuffer spill file: read %d instead of %lu", readBytes,
                                   ondiskSize - sizeof(Size) - sizeof(XLogRecPtr))));
        }

        /*
         * ok, read a full change from disk, now restore it into proper
         * in-memory format
         */
        ParallelReorderBufferRestoreChange(rb, txn, rb->outbuf, slotId);
        restored++;
    }

    return restored;
}

/*
 * Use caching to reduce frequent memory requests and releases.
 * Use worker->freegetlogicalloghead to store logchanges that should be free.
 * logicalLog is requested in the reader thread and free in the decoder thread.
 */
logicalLog* RestoreLogicalLog(int slotId) {
    logicalLog *logChange = NULL;
    MemoryContext oldCtx;
    oldCtx = MemoryContextSwitchTo(g_instance.comm_cxt.pdecode_cxt[slotId].parallelDecodeCtx);

    do {
        if (t_thrd.walsender_cxt.restoreLogicalLogHead != NULL) {
            logChange = t_thrd.walsender_cxt.restoreLogicalLogHead;
            t_thrd.walsender_cxt.restoreLogicalLogHead = t_thrd.walsender_cxt.restoreLogicalLogHead->freeNext;
        } else {
            logicalLog *head = (logicalLog *)pg_atomic_exchange_uintptr(
                (uintptr_t *)&g_Logicaldispatcher[slotId].freeLogicalLogHead, (uintptr_t)NULL);
            if (head != NULL) {
                logChange = head;
                t_thrd.walsender_cxt.restoreLogicalLogHead = head->freeNext;
            } else {
                logChange = (logicalLog *)palloc(sizeof(logicalLog));
                logChange->out = NULL;
                logChange->freeNext = NULL;
            }
        }
    } while (logChange == NULL);

    logChange->type = LOGICAL_LOG_EMPTY;

    if (logChange->out) {
        resetStringInfo(logChange->out);
    } else {
        logChange->out = makeStringInfo();
    }

    MemoryContextSwitchTo(oldCtx);
    return logChange;
}

/*
 * Convert change from its on-disk format to in-memory format and queue it onto
 * the TXN's ->changes list.
 */
static void ParallelReorderBufferRestoreChange(ParallelReorderBuffer *rb, ParallelReorderBufferTXN *txn, char *data,
    int slotId)
{
    logicalLog *change = NULL;
    ParallelDecodingData* pdata = (ParallelDecodingData*)t_thrd.walsender_cxt.parallel_logical_decoding_ctx->
        output_plugin_private;

    change = RestoreLogicalLog(slotId);
    Size changeSize = 0;
    errno_t rc = memcpy_s(&changeSize, sizeof(Size), data, sizeof(Size));
    securec_check(rc, "", "");
    data += sizeof(Size);

    rc = memcpy_s(&change->lsn, sizeof(XLogRecPtr), data, sizeof(XLogRecPtr));
    securec_check(rc, "", "");
    data += sizeof(XLogRecPtr);

    /* copy static part */
    MemoryContext oldCtx = MemoryContextSwitchTo(pdata->context);
    change->freeNext = NULL;
    appendBinaryStringInfo(change->out, data, changeSize - sizeof(Size) - sizeof(XLogRecPtr));
    dlist_push_tail(&txn->changes, &change->node);
    txn->nentries_mem++;
    MemoryContextSwitchTo(oldCtx);
}

static int ParallelReorderBufferIterCompare(Datum a, Datum b, void *arg)
{
    ParallelReorderBufferIterTXNState *state = (ParallelReorderBufferIterTXNState *)arg;
    XLogRecPtr pos_a = state->entries[DatumGetInt32(a)].lsn;
    XLogRecPtr pos_b = state->entries[DatumGetInt32(b)].lsn;
    if (XLByteLT(pos_a, pos_b))
        return 1;
    else if (XLByteEQ(pos_a, pos_b))
        return 0;
    return -1;
}

/*
 * Allocate & initialize an iterator which iterates in lsn order over a transaction
 * and all its subtransactions under parallel decoding circumstances.
 */
static ParallelReorderBufferIterTXNState *ParallelReorderBufferIterTXNInit
    (ParallelReorderBuffer *rb, ParallelReorderBufferTXN *txn, int slotId)
{
    Size nr_txns = 0;
    ParallelReorderBufferIterTXNState *state = NULL;
    dlist_iter cur_txn_i;
    Size off;

    if (txn->nentries > 0) {
        nr_txns++;
    }

    dlist_foreach(cur_txn_i, &txn->subtxns)
    {
        ParallelReorderBufferTXN *cur_txn = NULL;

        cur_txn = dlist_container(ParallelReorderBufferTXN, node, cur_txn_i.cur);
        if (cur_txn->nentries > 0)
            nr_txns++;
    }

    state = (ParallelReorderBufferIterTXNState *)MemoryContextAllocZero(rb->context,
        sizeof(ParallelReorderBufferIterTXNState) + sizeof(ParallelReorderBufferIterTXNEntry) * nr_txns);

    state->nr_txns = nr_txns;
    dlist_init(&state->old_change);

    for (off = 0; off < state->nr_txns; off++) {
        state->entries[off].fd = -1;
        state->entries[off].segno = 0;
    }

    /* allocate heap */
    state->heap = binaryheap_allocate(state->nr_txns, ParallelReorderBufferIterCompare, state);

    /*
     * Now insert items into the binary heap, in an unordered fashion.  (We
     * will run a heap assembly step at the end; this is more efficient.)
     */
    off = 0;
    /* add toplevel transaction if it contains changes */
    if (txn->nentries > 0) {
        logicalLog *cur_change = NULL;

        if (txn->serialized) {
            /* serialize remaining changes */
            ParallelReorderBufferSerializeTXN(rb, txn, slotId);
            (void)ParallelReorderBufferRestoreChanges(rb, txn, &state->entries[off].fd,
                &state->entries[off].segno, slotId);
        }

        if (!dlist_is_empty(&txn->changes)) {
            cur_change = dlist_head_element(logicalLog, node, &txn->changes);

            state->entries[off].lsn = cur_change->lsn;
            state->entries[off].change = cur_change;
            state->entries[off].txn = txn;

            binaryheap_add_unordered(state->heap, Int32GetDatum(off++));
        }
    }

    /* add subtransactions if they contain changes */
    dlist_foreach(cur_txn_i, &txn->subtxns)
    {
        ParallelReorderBufferTXN *cur_txn = NULL;

        cur_txn = dlist_container(ParallelReorderBufferTXN, node, cur_txn_i.cur);
        if (cur_txn->nentries > 0) {
            logicalLog *cur_change = NULL;

            if (cur_txn->serialized) {
                /* serialize remaining changes */
                ParallelReorderBufferSerializeTXN(rb, cur_txn, slotId);
                (void)ParallelReorderBufferRestoreChanges(rb, cur_txn, &state->entries[off].fd,
                    &state->entries[off].segno, slotId);
            }

            if (!dlist_is_empty(&txn->changes)) {
                cur_change = dlist_head_element(logicalLog, node, &cur_txn->changes);

                state->entries[off].lsn = cur_change->lsn;
                state->entries[off].change = cur_change;
                state->entries[off].txn = cur_txn;

                binaryheap_add_unordered(state->heap, Int32GetDatum(off++));
            }
        }
    }

    /* assemble a valid binary heap */
    binaryheap_build(state->heap);

    return state;
}

/*
 * Return the next change when iterating over a transaction and its
 * subtransactions.
 *
 * Returns NULL when no further changes exist.
 */
static logicalLog *ParallelReorderBufferIterTXNNext(ParallelReorderBuffer *rb,
    ParallelReorderBufferIterTXNState *state, int slotId)
{
    logicalLog *change = NULL;
    ParallelReorderBufferIterTXNEntry *entry = NULL;
    int32 off = 0;

    /* nothing there anymore */
    if (state->heap->bh_size == 0)
        return NULL;

    off = DatumGetInt32(binaryheap_first(state->heap));
    entry = &state->entries[off];

    if (!dlist_is_empty(&state->old_change)) {
        change = dlist_container(logicalLog, node,  dlist_pop_head_node(&state->old_change));
        FreeLogicalLog(change, slotId);
        /* We should find atmost one old_change here */
        Assert(dlist_is_empty(&state->old_change));
    }

    change = entry->change;

    /*
     * update heap with information about which transaction has the next
     * relevant change in LSN order
     *
     * there are in-memory changes
     */
    if (dlist_has_next(&entry->txn->changes, &entry->change->node)) {
        dlist_node *next = dlist_next_node(&entry->txn->changes, &change->node);
        logicalLog *next_change = dlist_container(logicalLog, node, next);

        /* txn stays the same */
        state->entries[off].lsn = next_change->lsn;
        state->entries[off].change = next_change;

        binaryheap_replace_first(state->heap, Int32GetDatum(off));
        return change;
    }

    /* try to load changes from disk */
    if (entry->txn->nentries != entry->txn->nentries_mem) {
        /*
         * Ugly: restoring changes will reuse *Change records, thus delete the
         * current one from the per-tx list and only free in the next call.
         */
        dlist_delete(&change->node);
        dlist_push_tail(&state->old_change, &change->node);

        if (ParallelReorderBufferRestoreChanges(rb, entry->txn, &entry->fd, &state->entries[off].segno, slotId)) {
            /* successfully restored changes from disk */
            logicalLog *next_change = dlist_head_element(logicalLog, node, &entry->txn->changes);

            if (!RecoveryInProgress())
                ereport(DEBUG2, (errmsg("restored %u/%u changes from disk", (uint32)entry->txn->nentries_mem,
                                        (uint32)entry->txn->nentries)));

            Assert(entry->txn->nentries_mem);
            /* txn stays the same */
            state->entries[off].lsn = next_change->lsn;
            state->entries[off].change = next_change;
            binaryheap_replace_first(state->heap, Int32GetDatum(off));

            return change;
        }
    }

    /* ok, no changes there anymore, remove */
    (void)binaryheap_remove_first(state->heap);

    return change;
}

/*
 * Deallocate the iterator
 */
static void ParallelReorderBufferIterTXNFinish(ParallelReorderBufferIterTXNState *state, int slotId)
{
    for (Size off = 0; off < state->nr_txns; off++) {
        if (state->entries[off].fd != -1) {
            (void)CloseTransientFile(state->entries[off].fd);
        }
    }

    /* free memory we might have "leaked" in the last *Next call */
    if (!dlist_is_empty(&state->old_change)) {
        logicalLog *change = NULL;
        change = dlist_container(logicalLog, node, dlist_pop_head_node(&state->old_change));
        FreeLogicalLog(change, slotId);
        Assert(dlist_is_empty(&state->old_change));
    }

    binaryheap_free(state->heap);
    pfree(state);
}

/*
 * Forget the contents of a transaction if we aren't interested in it's
 * contents. Needs to be first called for subtransactions and then for the
 * toplevel xid.
 */
void ParallelReorderBufferForget(ParallelReorderBuffer *rb, int slotId, ParallelReorderBufferTXN *txn)
{
    logicalLog *logChange = NULL;
    if (txn == NULL)
        return;
    ParallelReorderBufferIterTXNState *volatile iterstate = NULL;

    iterstate = ParallelReorderBufferIterTXNInit(rb, txn, slotId);
    while ((logChange = ParallelReorderBufferIterTXNNext(rb, iterstate, slotId)) != NULL) {
        dlist_delete(&logChange->node);
        FreeLogicalLog(logChange, slotId);
    }
    ParallelReorderBufferCleanupTXN(rb, txn);
    ParallelReorderBufferIterTXNFinish(iterstate, slotId);
}

/*
 * Parallel decoding check whether this batch should prepare write.
 */
static void ParallelCheckPrepare(StringInfo out, logicalLog *change, ParallelDecodingData *pdata, int slotId,
    MemoryContext *oldCtxPtr)
{
    if (!g_Logicaldispatcher[slotId].remainPatch) {
        WalSndPrepareWriteHelper(out, change->lsn, change->xid, true);
    }
    *oldCtxPtr = MemoryContextSwitchTo(pdata->context);
}
    
/*
 * Parallel decoding check whether this batch should be sent.
 */
static void ParallelCheckBatch(StringInfo out, ParallelDecodingData *pdata, int slotId,
    MemoryContext *oldCtxPtr, bool emptyXact)
{
    if (emptyXact) {
        MemoryContextSwitchTo(*oldCtxPtr);
        return;
    }
    if (out->len > g_Logicaldispatcher[slotId].pOptions.sending_batch * g_batch_unit_length) {
        if (pdata->pOptions.decode_style == 'b') {
            appendStringInfoChar(out, 'F'); // the finishing char
        } else if (g_Logicaldispatcher[slotId].pOptions.sending_batch > 0) {
            pq_sendint32(out, 0);
        }
        MemoryContextSwitchTo(*oldCtxPtr);
        WalSndWriteDataHelper(out, 0, 0, false);
        g_Logicaldispatcher[slotId].decodeTime = GetCurrentTimestamp();
        g_Logicaldispatcher[slotId].remainPatch = false;
    } else {
        if (pdata->pOptions.decode_style == 'b') {
            appendStringInfoChar(out, 'P');
        }
        g_Logicaldispatcher[slotId].remainPatch = true;
        MemoryContextSwitchTo(*oldCtxPtr);
    }
}

/*
 * Parallel decoding deal with batch sending.
 */
static inline void ParallelHandleBatch(StringInfo out, logicalLog *change, ParallelDecodingData *pdata, int slotId,
    MemoryContext *oldCtxPtr)
{
    ParallelCheckBatch(out, pdata, slotId, oldCtxPtr, false);
    ParallelCheckPrepare(out, change, pdata, slotId, oldCtxPtr);
}

/*
 * Parallel decoding output begin message.
 */
static void ParallelOutputBegin(StringInfo out, logicalLog *change, ParallelDecodingData *pdata,
    ParallelReorderBufferTXN *txn, bool batchSending)
{
    if (pdata->pOptions.decode_style == 'b') {
        int curPos = out->len;
        const uint32 beginBaseLen = 25; /* this length does not include the seperator 'P' */
        pq_sendint32(out, beginBaseLen);
        pq_sendint64(out, txn->first_lsn);
        appendStringInfoChar(out, 'B');
        pq_sendint64(out, change->csn);
        pq_sendint64(out, txn->first_lsn);
        if (pdata->pOptions.include_timestamp) {
            appendStringInfoChar(out, 'T');
            const char *timeStamp = timestamptz_to_str(txn->commit_time);
            pq_sendint32(out, (uint32)(strlen(timeStamp)));
            appendStringInfoString(out, timeStamp);
            uint32 beginLen = htonl(beginBaseLen + 1 + sizeof(uint32) + strlen(timeStamp));
            errno_t rc = memcpy_s(out->data + curPos, sizeof(uint32), &beginLen, sizeof(uint32));
            securec_check(rc, "", "");
        }
    } else {
        int curPos = out->len;
        uint32 beginLen = 0;
        if (batchSending) {
            pq_sendint32(out, beginLen);
            pq_sendint64(out, txn->first_lsn);
        }
        const uint32 upperPart = 32;
        appendStringInfo(out, "BEGIN CSN: %lu commit_lsn: %X/%X", change->csn, (uint32)(txn->first_lsn >> upperPart),
            (uint32)(txn->first_lsn));
        if (pdata->pOptions.include_timestamp) {
            const char *timeStamp = timestamptz_to_str(txn->commit_time);
            appendStringInfo(out, " commit_time: %s", timeStamp);
        }
        if (batchSending) {
            beginLen = htonl((uint32)(out->len - curPos) - (uint32)sizeof(uint32));
            errno_t rc = memcpy_s(out->data + curPos, sizeof(uint32), &beginLen, sizeof(uint32));
            securec_check(rc, "", "");
        }
    }
}

/*
 * Parallel decoding output commit message.
 */
static void ParallelOutputCommit(StringInfo out, logicalLog *change, ParallelDecodingData *pdata,
    ParallelReorderBufferTXN *txn, bool batchSending)
{
    if (pdata->pOptions.decode_style == 'b') {
        int curPos = out->len;
        uint32 commitLen = 0;
        pq_sendint32(out, commitLen);
        pq_sendint64(out, change->endLsn);
        appendStringInfoChar(out, 'C');
        commitLen += sizeof(uint64) + 1;
        if (pdata->pOptions.include_xids) {
            appendStringInfoChar(out, 'X');
            pq_sendint64(out, change->xid);
            commitLen += 1 + sizeof(uint64);
        }
        if (pdata->pOptions.include_timestamp) {
            appendStringInfoChar(out, 'T');
            const char *timeStamp = timestamptz_to_str(txn->commit_time);
            pq_sendint32(out, (uint32)(strlen(timeStamp)));
            appendStringInfoString(out, timeStamp);
            commitLen += 1 + sizeof(uint32) + strlen(timeStamp);
        }

        commitLen = htonl(commitLen);
        errno_t rc = memcpy_s(out->data + curPos, sizeof(uint32), &commitLen, sizeof(uint32));
        securec_check(rc, "", "");
    } else {
        int curPos = out->len;
        uint32 commitLen = 0;
        if (batchSending) {
            pq_sendint32(out, commitLen);
            pq_sendint64(out, change->endLsn);
        }
        if (pdata->pOptions.include_xids) {
            appendStringInfo(out, "commit xid: %lu", change->xid);
        } else {
            appendStringInfoString(out, "commit");
        }
        if (pdata->pOptions.include_timestamp) {
            appendStringInfo(out, " (at %s)", timestamptz_to_str(txn->commit_time));
        }
        if (batchSending) {
            commitLen = htonl((uint32)(out->len - curPos) - (uint32)sizeof(uint32));
            errno_t rc = memcpy_s(out->data + curPos, sizeof(uint32), &commitLen, sizeof(uint32));
            securec_check(rc, "", "");
        }
    }
}

/*
 * Get and send all the logical logs in the ParallelReorderBufferTXN linked list.
 */
void ParallelReorderBufferCommit(ParallelReorderBuffer *rb, logicalLog *change, int slotId,
    ParallelReorderBufferTXN *txn)
{
    logicalLog *logChange = NULL;

    /* unknown transaction, nothing to replay */
    if (txn == NULL) {
        return;
    }

    ParallelLogicalDecodingContext *ctx = (ParallelLogicalDecodingContext *)rb->private_data;
    ParallelDecodingData* pdata = (ParallelDecodingData*)t_thrd.walsender_cxt.
        parallel_logical_decoding_ctx->output_plugin_private;
    pdata->pOptions.xact_wrote_changes = false;

    MemoryContext oldCtx = NULL;
    ParallelCheckPrepare(ctx->out, change, pdata, slotId, &oldCtx);

    if (!pdata->pOptions.skip_empty_xacts) {
        ParallelOutputBegin(ctx->out, change, pdata, txn, g_Logicaldispatcher[slotId].pOptions.sending_batch > 0);
        ParallelHandleBatch(ctx->out, change, pdata, slotId, &oldCtx);
    }

    ParallelReorderBufferIterTXNState *volatile iterstate = NULL;
    iterstate = ParallelReorderBufferIterTXNInit(rb, txn, slotId);
    bool stopDecode = false;
    while ((logChange = ParallelReorderBufferIterTXNNext(rb, iterstate, slotId)) != NULL) {
        if (logChange->type == LOGICAL_LOG_NEW_CID && !stopDecode) {
            stopDecode = true;
        }
        if (!stopDecode && logChange->out != NULL && logChange->out->len != 0) {
            if (pdata->pOptions.skip_empty_xacts && !pdata->pOptions.xact_wrote_changes) {
                ParallelOutputBegin(ctx->out, change, pdata, txn,
                    g_Logicaldispatcher[slotId].pOptions.sending_batch > 0);
                ParallelHandleBatch(ctx->out, change, pdata, slotId, &oldCtx);
                pdata->pOptions.xact_wrote_changes = true;
            }
            appendBinaryStringInfo(ctx->out, logChange->out->data, logChange->out->len);
            ParallelHandleBatch(ctx->out, change, pdata, slotId, &oldCtx);
        }
        dlist_delete(&logChange->node);

        FreeLogicalLog(logChange, slotId);
    }

    if (!pdata->pOptions.skip_empty_xacts || pdata->pOptions.xact_wrote_changes) {
        ParallelOutputCommit(ctx->out, change, pdata, txn,
            g_Logicaldispatcher[slotId].pOptions.sending_batch > 0);
    }
    ParallelReorderBufferCleanupTXN(rb, txn);
    ParallelReorderBufferIterTXNFinish(iterstate, slotId);

    ParallelCheckBatch(ctx->out, pdata, slotId, &oldCtx,
        (pdata->pOptions.skip_empty_xacts && !pdata->pOptions.xact_wrote_changes));
    MemoryContextReset(pdata->context);
}

/*
 * Return the ParallelReorderBufferTXN from the given buffer, specified by Xid.
 * If create is true, and a transaction doesn't already exist, create it
 * (with the given LSN, and as top transaction if that's specified);
 * when this happens, is_new is set to true.
 */
ParallelReorderBufferTXN *ParallelReorderBufferTXNByXid(ParallelReorderBuffer *rb, TransactionId xid,
    bool create, bool *is_new, XLogRecPtr lsn, bool create_as_top)
{
    ParallelReorderBufferTXN *txn = NULL;
    ParallelReorderBufferTXNByIdEnt *ent = NULL;
    bool found = false;

    Assert(TransactionIdIsValid(xid));

    /*
     * Check the one-entry lookup cache first
     */
    if (TransactionIdIsValid(rb->by_txn_last_xid) && rb->by_txn_last_xid == xid) {
        txn = rb->by_txn_last_txn;

        if (txn != NULL) {
            /* found it, and it's valid */
            if (is_new != NULL) {
                *is_new = false;
            }
            return txn;
        }

        /*
         * cached as non-existant, and asked not to create? Then nothing else
         * to do.
         */
        if (!create) {
            return NULL;
        }
        /* otherwise fall through to create it */
    }

    /*
     * If the cache wasn't hit or it yielded an "does-not-exist" and we want
     * to create an entry.
     *
     * search the lookup table
     */
    ent = (ParallelReorderBufferTXNByIdEnt *)hash_search(rb->by_txn, (void *)&xid, create ? HASH_ENTER : HASH_FIND,
        &found);
    if (found) {
        txn = ent->txn;
    } else if (create) {
        /* initialize the new entry, if creation was requested */
        Assert(ent != NULL);
        Assert(lsn != InvalidXLogRecPtr);
        ent->txn = ParallelReorderBufferGetTXN(rb);
        ent->txn->xid = xid;
        txn = ent->txn;
        txn->first_lsn = lsn;
        txn->restart_decoding_lsn = rb->current_restart_decoding_lsn;
        txn->oldestXid = rb->lastRunningXactOldestXmin;

        if (create_as_top) {
            dlist_push_tail(&rb->toplevel_by_lsn, &txn->node);
        }
    } else {
        txn = NULL; /* not found and not asked to create */
    }

    /* update cache */
    rb->by_txn_last_txn = txn;
    rb->by_txn_last_xid = xid;

    if (is_new != NULL) {
        *is_new = !found;
    }

    Assert(!create || !!txn);
    return txn;
}

ParallelReorderBuffer *ParallelReorderBufferAllocate(int slotId)
{
    /* allocate memory in own context, to have better accountability */
    MemoryContext new_ctx = g_instance.comm_cxt.pdecode_cxt[slotId].parallelDecodeCtx;

    ParallelReorderBuffer *buffer =
        (ParallelReorderBuffer *)MemoryContextAllocZero(new_ctx, sizeof(ParallelReorderBuffer));

    HASHCTL hash_ctl;
    errno_t rc = memset_s(&hash_ctl, sizeof(hash_ctl), 0, sizeof(hash_ctl));
    securec_check(rc, "", "");

    buffer->context = new_ctx;

    hash_ctl.keysize = sizeof(TransactionId);
    hash_ctl.entrysize = sizeof(ReorderBufferTXNByIdEnt);
    hash_ctl.hash = tag_hash;
    hash_ctl.hcxt = buffer->context;

    const long reorderBufferNelem = 1000;
    buffer->by_txn = hash_create("ParallelReorderBufferByXid", reorderBufferNelem,
        &hash_ctl, HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

    buffer->by_txn_last_xid = InvalidTransactionId;
    buffer->by_txn_last_txn = NULL;

    buffer->nr_cached_transactions = 0;
    buffer->nr_cached_tuplebufs = 0;
    buffer->nr_cached_changes = 0;

    buffer->current_restart_decoding_lsn = InvalidXLogRecPtr;
    buffer->outbuf = NULL;
    buffer->outbufsize = 0;

    dlist_init(&buffer->toplevel_by_lsn);
    dlist_init(&buffer->txns_by_base_snapshot_lsn);
    dlist_init(&buffer->cached_changes);
    slist_init(&buffer->cached_tuplebufs);

    return buffer;
}

