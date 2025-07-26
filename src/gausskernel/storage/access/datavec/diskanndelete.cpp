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

#include "access/datavec/diskann.h"
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

Buffer DiskannGetSameIndexTuple(Relation rel, DiskAnnScanOpaque so, IndexTuple indexTuple)
{
    if (so->curpos > 0) {
        --so->curpos;
    }
    for (; so->curpos < so->candidates.size(); ++so->curpos) {
        BlockNumber blkno = so->candidates[so->curpos].id;
        Buffer buf = ReadBuffer(rel, blkno);
        LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
        Page page = BufferGetPage(buf);
        IndexTuple currIndexTuple = DiskAnnPageGetIndexTuple(page);
        if (ItemPointerEquals(&(indexTuple->t_tid), &(currIndexTuple->t_tid)) &&
            isTupleEqual(indexTuple, currIndexTuple)) {
            return buf;
        }
        UnlockReleaseBuffer(buf);
    }
    return InvalidBuffer;
}

void DiskAnnMarkDead(Relation rel, Datum* values, ItemPointer tid)
{
    if (rel == NULL || CheckIndexBuilding(rel)) {
        return;
    }

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

    Buffer buf = DiskannGetSameIndexTuple(rel, so, indexTuple);
    if (buf != InvalidBuffer) {
        Page page = BufferGetPage(buf);
        ItemId itemid = PageGetItemId(page, FirstOffsetNumber);
        ItemIdMarkDead(itemid);
        MarkBufferDirty(buf);
        UnlockReleaseBuffer(buf);
    }

    diskannendscan_internal(scanDesc);
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