/*
* Copyright (c) 2025 Huawei Technologies Co.,Ltd.
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
* diskanndelete.cpp
*
* IDENTIFICATION
*        src/gausskernel/storage/access/datavec/diskanndelete.cpp
*
* -------------------------------------------------------------------------
*/
#include "postgres.h"
#include "knl/knl_variable.h"

#include "catalog/pg_partition_fn.h"
#include "nodes/execnodes.h"
#include "access/tableam.h"
#include "executor/executor.h"
#include "access/generic_xlog.h"
#include "commands/vacuum.h"
#include "storage/buf/bufmgr.h"
#include "storage/item/itemid.h"
#include "storage/lmgr.h"
#include "access/datavec/diskann.h"
#include "access/datavec/diskannv2.h"
#include "access/datavec/vector_storage.h"
static constexpr bool IsPartitionedRelation(char parttype)
{
    return ((parttype) == PARTTYPE_PARTITIONED_RELATION ||
            (parttype) == PARTTYPE_SUBPARTITIONED_RELATION ||
            (parttype) == PARTTYPE_VALUE_PARTITIONED_RELATION);
}

static bool isTupleEqual(IndexTuple indexTuple1, IndexTuple indexTuple2)
{
    if (indexTuple1 == NULL || indexTuple2 == NULL) {
        return false;
    }
    Size size1 = IndexTupleSize(indexTuple1);
    Size size2 = IndexTupleSize(indexTuple2);
    if (size1 != size2 || size1 == 0) {
        return false;
    }

    return memcmp(indexTuple1, indexTuple2, size1) == 0;
}

static bool CheckIndexBuilding(Relation index)
{
    Relation pgIndex = heap_open(IndexRelationId, RowExclusiveLock);
    Oid indexOid = RelationGetRelid(index);
    if (RelationIsPartitioned(index)) {
        indexOid = GetBaseRelOidOfParition(index);
    }
    HeapTuple indexTuple = SearchSysCache1(INDEXRELID, ObjectIdGetDatum(indexOid));
    if (!HeapTupleIsValid(indexTuple)) {
        heap_close(pgIndex, RowExclusiveLock);
        ereport(ERROR, (errmsg("search system cache for index %u failed", indexOid)));
    }

    Form_pg_index indexForm = (Form_pg_index)GETSTRUCT(indexTuple);
    bool building = false;
    if (!indexForm->indisvalid && indexForm->indisready) {
        building = true;
    }
    ReleaseSysCache(indexTuple);
    heap_close(pgIndex, RowExclusiveLock);
    return building;
}

Buffer DiskannGetSameIndexTuple(Relation rel, DiskAnnScanOpaque so, IndexTuple indexTuple, DiskAnnMetaPage metapage)
{
    if (so->curpos > 0) {
        --so->curpos;
    }
    BlockNumber target = InvalidBlockNumber;
    for (; so->curpos < so->candidates.size(); ++so->curpos) {
        BlockNumber blkno = so->candidates[so->curpos].id;
        if (blkno == metapage->frozenBlkno[0]) {
            continue;
        }
        Buffer buf = ReadBuffer(rel, blkno);
        LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
        GenericXLogState *state = GenericXLogStart(rel);
        Page page = GenericXLogRegisterBuffer(state, buf, 0);
        IndexTuple currIndexTuple = DiskAnnPageGetIndexTuple(page);
        DiskAnnNodePage ntup = DiskAnnPageGetNode(currIndexTuple);
        bool sameHeap = ItemPointerEquals(&(indexTuple->t_tid), &(currIndexTuple->t_tid));
        bool sameTuple = metapage->enableVectorStorage ? sameHeap :
            (sameHeap && isTupleEqual(indexTuple, currIndexTuple));
        if (sameTuple) {
            target = blkno;

            GenericXLogUnregister(state, buf);
            GenericXLogAbort(state);
            UnlockReleaseBuffer(buf);
            continue;
        }

        uint8 count = 0;
        for (uint8 curr = 0; curr < ntup->heaptidsLength; ++curr) {
            if (ItemPointerEquals(&indexTuple->t_tid, &ntup->heaptids[curr])) {
                continue;
            }
            ntup->heaptids[count] = ntup->heaptids[curr];
            ++count;
        }
        if (ntup->heaptidsLength != count) {
            ntup->heaptidsLength = count;
            GenericXLogFinish(state);
        } else {
            GenericXLogUnregister(state, buf);
            GenericXLogAbort(state);
        }
        UnlockReleaseBuffer(buf);
    }

    if (target != InvalidBlockNumber) {
        Buffer buf = ReadBuffer(rel, target);
        LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
        return buf;
    }
    return InvalidBuffer;
}

void EraseEdgeFromGraph(Relation rel, BlockNumber node, BlockNumber target, DiskAnnMetaPage metaPage)
{
    Buffer buf;
    Page page;
    DiskAnnNodePage ntup;
    DiskAnnEdgePage etup;

    buf = ReadBuffer(rel, node);
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

    GenericXLogState *state = GenericXLogStart(rel);
    page = GenericXLogRegisterBuffer(state, buf, 0);
    ntup = DiskAnnPageGetNode(DiskAnnPageGetIndexTuple(page));
    etup = (DiskAnnEdgePage)((uint8_t*)ntup + metaPage->nodeSize);

    uint16 count = 0;
    for (uint16 curr = 0; curr < etup->count; ++curr) {
        if (etup->nexts[curr] == target) {
            continue;
        }
        etup->nexts[count] = etup->nexts[curr];
        etup->distance[count] = etup->distance[curr];
        ++count;
    }
    etup->count = count;

    GenericXLogFinish(state);
    UnlockReleaseBuffer(buf);
}

/* Reconnect neighbors without searching through the node being removed. */
class DiskAnnDeleteGraphStore : public DiskAnnPageGraphStore {
public:
    DiskAnnDeleteGraphStore(Relation index, BlockNumber excluded)
        : DiskAnnPageGraphStore(index), m_excluded(excluded)
    {}

    bool IsLiveNeighbor(BlockNumber blkno) const override
    {
        /* The caller holds the excluded node's content lock exclusively. */
        return blkno != m_excluded && DiskAnnPageGraphStore::IsLiveNeighbor(blkno);
    }

private:
    BlockNumber m_excluded;
};

/*
 * buf is already exclusive/cleanup-locked. Relink its neighbors before
 * unlinking it: a rolled-back insertion can be the only bridge to a later
 * live insertion. Merely removing its edges would make that live node
 * unreachable from the frozen entry point after VACUUM.
 */
static void DiskAnnKillLockedNode(Relation rel, Buffer buf, DiskAnnMetaPage metaPage)
{
    DiskAnnNodePage ntup;
    DiskAnnEdgePage etup;
    GenericXLogState *state;
    Page page;
    BlockNumber deletedBlk = BufferGetBlockNumber(buf);

    state = GenericXLogStart(rel);
    page = GenericXLogRegisterBuffer(state, buf, 0);
    ntup = DiskAnnPageGetNode(DiskAnnPageGetIndexTuple(page));
    etup = (DiskAnnEdgePage)((uint8_t *)ntup + metaPage->nodeSize);

    DiskAnnDeleteGraphStore graphStore(rel, deletedBlk);
    for (uint16 curr = 0; curr < etup->count; curr++) {
        BlockNumber neighbor = etup->nexts[curr];
        if (neighbor == deletedBlk || neighbor == metaPage->frozenBlkno[0] || IsMarkDeleted(rel, neighbor)) {
            continue;
        }
        DiskAnnGraph graph(rel, metaPage->dimensions, metaPage->frozenBlkno[0], &graphStore);
        graph.Link(neighbor, metaPage->indexSize, false);
    }

    for (uint16 curr = 0; curr < etup->count; ++curr) {
        if (etup->nexts[curr] != deletedBlk) {
            EraseEdgeFromGraph(rel, etup->nexts[curr], deletedBlk, metaPage);
        }
    }
    etup->count = 0;
    ntup->deleted = 1;

    ItemId itemid = PageGetItemId(page, FirstOffsetNumber);
    ItemIdMarkDead(itemid);

    GenericXLogFinish(state);
    UnlockReleaseBuffer(buf);

    if (metaPage->enableVectorStorage) {
        ItemPointerData ownerTid;
        ItemPointerSet(&ownerTid, deletedBlk, FirstOffsetNumber);
        VecPayloadRecycle(rel, MAIN_FORKNUM, &ownerTid);
    }
}

void DiskAnnMarkDead(Relation rel, Datum* values, ItemPointer tid)
{
    if (rel == NULL || CheckIndexBuilding(rel)) {
        return;
    }
    if (DiskAnnPeekFormatVersion(rel) == DISKANN_VERSION_V2) {
        /* RaBitQ format: DELETE only touches the heap, VACUUM removes the index entry (no upgrade gate here) */
        return;
    }

    LockPage(rel, DISKANN_GRAPH_LOCK, ExclusiveLock);
    IndexScanDesc scanDesc = diskannbeginscan_internal(rel, 0, 1);
    Datum dest = PointerGetDatum(PG_DETOAST_DATUM(values[0]));
    ScanKeyInit(scanDesc->orderByData, 0, BTEqualStrategyNumber, F_OIDEQ, dest);
    DiskAnnScanOpaque so = (DiskAnnScanOpaque)scanDesc->opaque;
    Vector *target;
    if (so->normprocinfo != NULL) {
        target = (Vector *)DirectFunctionCall1Coll(l2_normalize, so->collation, dest);
    } else {
        target = (Vector *)DatumGetPointer(dest);
    }

    Datum value[1];
    bool isnull[1] = { false };
    value[0] = PointerGetDatum(target);
    so->delSearch = true;
    diskannrescan_internal(scanDesc, NULL, 0, NULL, 0);
    diskanngettuple_internal(scanDesc, ForwardScanDirection);
    IndexTuple indexTuple = index_form_tuple(RelationGetDescr(rel), value, isnull);
    indexTuple->t_tid = *tid;

    DiskAnnMetaPageData metapage;
    DiskANNGetMetaPageInfo(rel, &metapage);

    Buffer buf = DiskannGetSameIndexTuple(rel, so, indexTuple, &metapage);
    if (buf == InvalidBuffer) {
        diskannendscan_internal(scanDesc);
        UnlockPage(rel, DISKANN_GRAPH_LOCK, ExclusiveLock);
        return;
    }

    DiskAnnKillLockedNode(rel, buf, &metapage);
    diskannendscan_internal(scanDesc);
    UnlockPage(rel, DISKANN_GRAPH_LOCK, ExclusiveLock);
}

static void CheckAndDeleteFromIndex(Relation actualIndex, IndexInfo* indexInfo, ItemPointer tid, EState* estate)
{
    if (actualIndex == NULL || indexInfo == NULL) {
        return;
    }

    ExprContext* econtext = GetPerTupleExprContext(estate);
    TupleTableSlot* slot = econtext->ecxt_scantuple;
    if (indexInfo->ii_Predicate != NIL) {
        List* predicate = indexInfo->ii_Predicate;
        if (predicate == NIL) {
            if (estate->es_is_flt_frame) {
                predicate = (List*)ExecPrepareQualByFlatten(indexInfo->ii_Predicate, estate);
            } else {
                predicate = (List*)ExecPrepareExpr((Expr *)indexInfo->ii_Predicate, estate);
            }
            indexInfo->ii_Predicate = predicate;
        }

        if (!ExecQual(predicate, econtext)) {
            return;
        }
    }

    if (actualIndex->rd_rel->relam != DISKANN_AM_OID) {
        return;
    }

    Datum values[INDEX_MAX_KEYS];
    bool isnull[INDEX_MAX_KEYS];

    FormIndexDatum(indexInfo, slot, estate, values, isnull);
    DiskAnnMarkDead(actualIndex, values, tid);
    return;
}

static Relation GetRealIndexRelation(Relation indexRel, EState* estate,
                                     Partition p, List* &indexOidList)
{
    Relation actualIndex = NULL;
    Partition indexPartition = NULL;
    Oid idxPartitionId = InvalidOid;

    Oid partitionedIndexId = RelationGetRelid(indexRel);
    if (indexOidList == NIL) {
        indexOidList = PartitionGetPartIndexList(p);
    }

    if (indexOidList == NIL) {
        return NULL;
    }

    idxPartitionId = searchPartitionIndexOid(partitionedIndexId, indexOidList);
    if (idxPartitionId == InvalidOid) {
        return NULL;
    }

    searchFakeReationForPartitionOid(estate->esfRelations, estate->es_query_cxt, indexRel, idxPartitionId,
                                     INVALID_PARTITION_NO, actualIndex, indexPartition, RowExclusiveLock);
    if (indexPartition != NULL && indexPartition->pd_part != NULL &&  !indexPartition->pd_part->indisusable) {
        return NULL;
    }
    return actualIndex;
}

void DeleteDiskAnnIndexTuples(TupleTableSlot* slot, ItemPointer tid, EState* estate, Partition p)
{
    ResultRelInfo* relInfo = estate->es_result_relation_info;
    if (relInfo->ri_NumIndices == 0) {
        return;
    }
    tableam_tslot_getallattrs(slot);
    if (slot->tts_nvalid == 0) {
        return;
    }

    ExprContext* econtext = GetPerTupleExprContext(estate);
    econtext->ecxt_scantuple = slot;

    Relation rel = relInfo->ri_RelationDesc;
    List* indexOidList = NIL;
    for (int i = 0; i < relInfo->ri_NumIndices; ++i) {
        Relation indexRel = relInfo->ri_IndexRelationDescs[i];
        if (indexRel == NULL) {
            continue;
        }

        IndexInfo* indexInfo = relInfo->ri_IndexRelationInfo[i];
        if (indexInfo == NULL) {
            continue;
        }

        if (!indexInfo->ii_ReadyForInserts || !IndexIsReady(indexRel->rd_index)) {
            continue;
        }

        if (!IndexIsUsable(indexRel->rd_index) || !IndexIsLive(indexRel->rd_index)) {
            continue;
        }

        Relation actualIndex = indexRel;
        if (IsPartitionedRelation(relInfo->ri_RelationDesc->rd_rel->parttype) && RelationIsGlobalIndex(indexRel)) {
            actualIndex = GetRealIndexRelation(indexRel, estate, p, indexOidList);
        }

        if (actualIndex == NULL) {
            continue;
        }
        CheckAndDeleteFromIndex(actualIndex, indexInfo, tid, estate);
    }

    list_free_ext(indexOidList);
}

static bool DiskAnnPageIsGraphNode(Page page)
{
    ItemId itemid;

    if (page == NULL || PageIsNew(page) || PageIsEmpty(page)) {
        return false;
    }
    if (PageGetSpecialSize(page) != MAXALIGN(sizeof(DiskAnnPageOpaqueData))) {
        return false;
    }
    if (DiskAnnPageGetOpaque(page)->pageId != DISKANN_PAGE_ID) {
        return false;
    }
    itemid = PageGetItemId(page, FirstOffsetNumber);
    return ItemIdIsUsed(itemid);
}

static bool DiskAnnPageIsLiveGraphNode(Page page)
{
    ItemId itemid;

    if (!DiskAnnPageIsGraphNode(page)) {
        return false;
    }
    itemid = PageGetItemId(page, FirstOffsetNumber);
    return !ItemIdIsDead(itemid);
}

static bool DiskAnnBlknoIsFrozen(const DiskAnnMetaPageData *meta, BlockNumber blkno)
{
    uint16 i;

    if (meta == NULL) {
        return false;
    }
    for (i = 0; i < meta->nfrozen && i < FROZEN_POINT_SIZE; i++) {
        if (meta->frozenBlkno[i] == blkno) {
            return true;
        }
    }
    return false;
}

/*
 * VACUUM's ambulkdelete: walk graph pages, drop nodes whose heap tid is dead,
 * recycle vector-storage payload slots onto payloadFreeHead.
 */
IndexBulkDeleteResult *diskannbulkdelete_internal(IndexVacuumInfo *info, IndexBulkDeleteResult *stats,
                                                  IndexBulkDeleteCallback callback, void *callbackState)
{
    if (DiskAnnGetFormatVersion(info->index) == DISKANN_VERSION_V2) {
        return DiskAnnV2BulkDelete(info, stats, callback, callbackState);
    }

    Relation index = info->index;
    DiskAnnMetaPageData meta;
    BufferAccessStrategy bstrategy;
    BlockNumber nblocks;
    BlockNumber blkno;

    if (stats == NULL) {
        stats = (IndexBulkDeleteResult *)palloc0(sizeof(IndexBulkDeleteResult));
    }
    if (callback == NULL) {
        return stats;
    }

    LockPage(index, DISKANN_GRAPH_LOCK, ExclusiveLock);
    DiskANNGetMetaPageInfo(index, &meta);
    bstrategy = GetAccessStrategy(BAS_BULKREAD);
    nblocks = RelationGetNumberOfBlocks(index);

    for (blkno = 1; blkno < nblocks; blkno++) {
        Buffer buf;
        Page page;
        IndexTuple itup;

        vacuum_delay_point();
        if (DiskAnnBlknoIsFrozen(&meta, blkno)) {
            continue;
        }

        buf = ReadBufferExtended(index, MAIN_FORKNUM, blkno, RBM_NORMAL, bstrategy);
        LockBufferForCleanup(buf);
        page = BufferGetPage(buf);
        if (!DiskAnnPageIsGraphNode(page)) {
            UnlockReleaseBuffer(buf);
            continue;
        }

        /*
         * Already-dead node: neighbors were unlinked, but payload recycle may
         * have been interrupted. Recycle from the retained owner reference.
         */
        if (!DiskAnnPageIsLiveGraphNode(page)) {
            UnlockReleaseBuffer(buf);
            if (meta.enableVectorStorage) {
                ItemPointerData ownerTid;
                ItemPointerSet(&ownerTid, blkno, FirstOffsetNumber);
                VecPayloadRecycle(index, MAIN_FORKNUM, &ownerTid);
            }
            continue;
        }

        itup = DiskAnnPageGetIndexTuple(page);
        DiskAnnNodePage node = DiskAnnPageGetNode(itup);
        ItemPointerData liveTids[DISKANN_HEAPTIDS];
        int nlive = 0;
        if (node->heaptidsLength > DISKANN_HEAPTIDS) {
            ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED), errmsg("invalid diskann heap TID count")));
        }
        for (int i = 0; i < node->heaptidsLength; i++) {
            if (!callback(&node->heaptids[i], callbackState, InvalidOid, InvalidBktId)) {
                liveTids[nlive++] = node->heaptids[i];
            } else {
                stats->tuples_removed++;
            }
        }
        if (nlive > 0) {
            stats->num_index_tuples += nlive;
            if (nlive != node->heaptidsLength) {
                GenericXLogState *state = GenericXLogStart(index);
                page = GenericXLogRegisterBuffer(state, buf, 0);
                itup = DiskAnnPageGetIndexTuple(page);
                node = DiskAnnPageGetNode(itup);
                itup->t_tid = liveTids[0];
                node->heaptidsLength = (uint8)nlive;
                for (int i = 0; i < DISKANN_HEAPTIDS; i++) {
                    if (i < nlive) {
                        node->heaptids[i] = liveTids[i];
                    } else {
                        ItemPointerSetInvalid(&node->heaptids[i]);
                    }
                }
                GenericXLogFinish(state);
            }
            UnlockReleaseBuffer(buf);
            continue;
        }

        DiskAnnKillLockedNode(index, buf, &meta);
    }

    FreeAccessStrategy(bstrategy);
    UnlockPage(index, DISKANN_GRAPH_LOCK, ExclusiveLock);
    return stats;
}
