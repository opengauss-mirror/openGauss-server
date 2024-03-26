/* index only scan section */
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
 * ---------------------------------------------------------------------------------------
 *
 * opfusion_indexonlyscan.cpp
 *        Definition of class IndexOnlyScanFusion for bypass executor.
 *
 * IDENTIFICATION
 *        src/gausskernel/runtime/opfusion/opfusion_indexonlyscan.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "opfusion/opfusion_indexonlyscan.h"

#include "access/tableam.h"
#include "access/visibilitymap.h"
#include "opfusion/opfusion_util.h"
#include "utils/knl_partcache.h"
#include "access/multi_redo_api.h"


IndexOnlyScanFusion::IndexOnlyScanFusion(IndexOnlyScan* node, PlannedStmt* planstmt, ParamListInfo params)
    : IndexFusion(params, planstmt)
{
    m_isnull = NULL;
    m_values = NULL;
    m_epq_indexqual = NULL;
    m_index = NULL;
    m_tmpvals = NULL;
    m_scandesc = NULL;
    m_tmpisnull = NULL;
    m_parentRel = NULL;
    m_partRel = NULL;

    m_node = node;

    m_keyInit = false;
    m_keyNum = list_length(node->indexqual);
    m_scanKeys = (ScanKey)palloc0(m_keyNum * sizeof(ScanKeyData));

    /* init params */
    m_paramLoc = NULL;
    m_paramNum = 0;
    if (params != NULL) {
        m_paramLoc = (ParamLoc*)palloc0(m_keyNum * sizeof(ParamLoc));

        ListCell* lc = NULL;
        int i = 0;
        foreach (lc, node->indexqual) {
            if (IsA(lfirst(lc), NullTest)) {
                i++;
                continue;
            }

            Assert(IsA(lfirst(lc), OpExpr));

            OpExpr* opexpr = (OpExpr*)lfirst(lc);
            Expr* var = (Expr*)lsecond(opexpr->args);

            if (IsA(var, RelabelType)) {
                var = ((RelabelType*)var)->arg;
            }

            if (IsA(var, Param)) {
                Param* param = (Param*)var;
                m_paramLoc[m_paramNum].paramId = param->paramid;
                m_paramLoc[m_paramNum++].scanKeyIndx = i;
            }
            i++;
        }
    }
    if (m_node->scan.isPartTbl) {
        Oid parentRelOid = getrelid(m_node->scan.scanrelid, planstmt->rtable);
        m_parentRel = heap_open(parentRelOid, AccessShareLock);
        m_reloid = GetRelOidForPartitionTable(m_node->scan, m_parentRel, params);

        /* get partition relation */
        InitPartitionRelationInFusion(m_reloid, m_parentRel, &m_partRel, &m_rel);

        /* get Partition index */
        Oid parentIndexOid = m_node->indexid;
        m_index = InitPartitionIndexInFusion(parentIndexOid, m_reloid, &m_partIndex, &m_parentIndex, m_rel);
    } else {
        m_reloid = getrelid(m_node->scan.scanrelid, planstmt->rtable);
        m_index = index_open(m_node->indexid, AccessShareLock);
    }
    m_targetList = m_node->scan.plan.targetlist;
    m_reloid = getrelid(m_node->scan.scanrelid, planstmt->rtable);
    m_tupDesc = ExecCleanTypeFromTL(m_targetList, false);
    m_reslot = MakeSingleTupleTableSlot(m_tupDesc);
    m_direction = (ScanDirection*)palloc0(sizeof(ScanDirection));
    m_VMBuffer = InvalidBuffer;

    Relation rel = m_index;
    m_attrno = (int16*)palloc(m_tupDesc->natts * sizeof(int16));
    m_values = (Datum*)palloc(RelationGetDescr(rel)->natts * sizeof(Datum));
    m_tmpvals = (Datum*)palloc(m_tupDesc->natts * sizeof(Datum));
    m_isnull = (bool*)palloc(RelationGetDescr(rel)->natts * sizeof(bool));
    m_tmpisnull = (bool*)palloc(m_tupDesc->natts * sizeof(bool));
    setAttrNo();
    ExeceDoneInIndexFusionConstruct(m_node->scan.isPartTbl, &m_parentRel, &m_partRel, &m_index, &m_rel);
    if (m_node->scan.isPartTbl) {
        partitionClose(m_parentIndex, m_partIndex, AccessShareLock);
        index_close(m_parentIndex, AccessShareLock);
    }
}


void IndexOnlyScanFusion::Init(long max_rows)
{
    if (m_node->scan.isPartTbl) {
        /* get parent Relation */
        Oid parent_relOid = getrelid(m_node->scan.scanrelid, m_planstmt->rtable);
        m_parentRel = heap_open(parent_relOid, AccessShareLock);
        m_reloid = GetRelOidForPartitionTable(m_node->scan, m_parentRel, m_params);

        /* get partition relation */
        InitPartitionRelationInFusion(m_reloid, m_parentRel, &m_partRel, &m_rel);

        /* get partition index */
        Oid parentIndexOid = m_node->indexid;
        m_index = InitPartitionIndexInFusion(parentIndexOid, m_reloid, &m_partIndex, &m_parentIndex, m_rel);
    } else {
        m_rel = heap_open(m_reloid, AccessShareLock);
        m_index = index_open(m_node->indexid, AccessShareLock);
    }

    if (unlikely(!m_keyInit)) {
        IndexFusion::IndexBuildScanKey(m_node->indexqual);
        m_keyInit = true;
    }

    if (m_params != NULL) {
        refreshParameterIfNecessary();
    }

    *m_direction = ForwardScanDirection;
    if (max_rows > 0) {
        if (ScanDirectionIsBackward(m_node->indexorderdir)) {
            *m_direction = BackwardScanDirection;
        }
    } else {
        *m_direction = NoMovementScanDirection;
    }

    m_reslot = MakeSingleTupleTableSlot(m_tupDesc, false, m_tupDesc->td_tam_ops);
    ScanState* scanstate = makeNode(ScanState); // need release
    scanstate->ps.plan =  (Plan *)m_node;

    /*
     * for hash bucket pruning with Global Plan cache, we should build a temp EState object
     * to passing param info which is used to cal buckets in further hbkt_idx_beginscan.
     */
    if (ENABLE_GPC && RELATION_CREATE_BUCKET(m_rel)) {
        EState tmpstate;
        tmpstate.es_param_list_info = m_params;
        scanstate->ps.state = &tmpstate;
        m_scandesc = scan_handler_idx_beginscan(m_rel, m_index, GetActiveSnapshot(), m_keyNum, 0, scanstate); // add scanstate pointer ? // add scanstate pointer ?
        scanstate->ps.state = NULL;
    } else {
        m_scandesc = scan_handler_idx_beginscan(m_rel, m_index, GetActiveSnapshot(), m_keyNum, 0, scanstate); // add scanstate pointer ?
    }

    if (PointerIsValid(m_scandesc)) {
        m_VMBuffer = InvalidBuffer;
        IndexScanDesc indexdesc = GetIndexScanDesc(m_scandesc);
        indexdesc->xs_want_itup = true;
    }

    scan_handler_idx_rescan_local(m_scandesc, m_keyNum > 0 ? m_scanKeys : NULL, m_keyNum, NULL, 0);

    m_epq_indexqual = m_node->indexqual;

}

HeapTuple IndexOnlyScanFusion::getTuple()
{
    /*
     * You will never need this function cause select operator gets tuple through get
     * TupleSlot, and the other operators like update, insert, and delete will directly
     * change the values on heap, there is no need for them to get tuple from index.
     */
    Assert(0);
    return NULL;
}

UHeapTuple IndexOnlyScanFusion::getUTuple()
{
    /*
     * You will never need this function cause select operator gets tuple through get
     * TupleSlot, and the other operators like update, insert, and delete will directly
     * change the values on heap, there is no need for them to get tuple from index.
     */
    Assert(0);
    return NULL;
}

TupleTableSlot *IndexOnlyScanFusion::getTupleSlot()
{
    if (m_scandesc == NULL) {
        return NULL;
    }
    return getTupleSlotInternal();
}

TupleTableSlot *IndexOnlyScanFusion::getTupleSlotInternal()
{
    ItemPointer tid;
    Relation rel = m_index;
    bool isUStore = RelationIsUstoreFormat(m_rel);
    bool bucket_changed = false;
    TupleTableSlot* tmpreslot = NULL;
    tmpreslot = MakeSingleTupleTableSlot(RelationGetDescr(m_scandesc->heapRelation),
        false, m_scandesc->heapRelation->rd_tam_ops);

    while ((tid = scan_handler_idx_getnext_tid(m_scandesc, *m_direction, &bucket_changed)) != NULL) {
        HeapTuple tuple = NULL;

        IndexScanDesc indexdesc = GetIndexScanDesc(m_scandesc);

        if (IndexScanNeedSwitchPartRel(indexdesc)) {
            /* reset VM buffer */
            if (m_VMBuffer != InvalidBuffer) {
                ReleaseBuffer(m_VMBuffer);
                m_VMBuffer = InvalidBuffer;
            }

            /*
             * Change the heapRelation in indexScanDesc to Partition Relation of current index
             */
            if (!GPIGetNextPartRelation(indexdesc->xs_gpi_scan, CurrentMemoryContext, AccessShareLock)) {
                continue;
            }
            indexdesc->heapRelation = indexdesc->xs_gpi_scan->fakePartRelation;
        }

        if (isUStore) {
            /* ustore with multi-version ubtree only recheck IndexTuple when xs_recheck_itup is set */
            if (indexdesc->xs_recheck_itup) {
                if (!IndexFetchUHeap(indexdesc, tmpreslot)) {
                    continue; /* this TID indicate no visible tuple */
                }
                if (!RecheckIndexTuple(indexdesc, tmpreslot)) {
                    continue; /* the visible version not match the IndexTuple */
                }
            }
        } else {
            if (!ExecCBIFixHBktRel(m_scandesc, &m_VMBuffer)) {
                continue;
            }

            /* 
             * For local bucket index, reset vm buffer here if
             * the underlying bucket relation was changed.
             */
            if (bucket_changed && (m_VMBuffer != InvalidBuffer)) {
                ReleaseBuffer(m_VMBuffer);
                m_VMBuffer = InvalidBuffer;
                bucket_changed = false;
            }
            
            /* UStore don't have visibility map */
            if (isUStore || is_index_only_disabled_in_astore() ||
                !visibilitymap_test(indexdesc->heapRelation, ItemPointerGetBlockNumber(tid), &m_VMBuffer)) {
                tuple = (HeapTuple)IndexFetchTuple(indexdesc);
                if (tuple == NULL) {
                    continue; /* no visible tuple, try next index entry */
                }
            }
        }

        if (indexdesc->xs_continue_hot) {
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("non-MVCC snapshots are not supported in index-only scans")));
        }

        /*
         * Fill the scan tuple slot with data from the index.
         */
        IndexTuple tmptup = NULL;
        index_deform_tuple(indexdesc->xs_itup, RelationGetDescr(rel), m_values, m_isnull);
        if (indexdesc->xs_recheck && EpqCheck(m_values, m_isnull)) {
            continue;
        }

        /* mapping */
        for (int i = 0; i < m_tupDesc->natts; i++) {
            Assert(m_attrno[i] > 0);
            m_tmpvals[i] = m_values[m_attrno[i] - 1];
            m_tmpisnull[i] = m_isnull[m_attrno[i] - 1];
        }

        tmptup = index_form_tuple(m_tupDesc, m_tmpvals, m_tmpisnull);
        Assert(tmptup != NULL);
        StoreIndexTuple(m_reslot, tmptup, m_tupDesc);

        tableam_tslot_getsomeattrs(m_reslot, m_tupDesc->natts);
        ExecDropSingleTupleTableSlot(tmpreslot);
        return m_reslot;
    }
    ExecDropSingleTupleTableSlot(tmpreslot);
    return NULL;
}

void IndexOnlyScanFusion::End(bool isCompleted)
{
    if (m_VMBuffer != InvalidBuffer) {
        ReleaseBuffer(m_VMBuffer);
        m_VMBuffer = InvalidBuffer;
    }
    if (!isCompleted)
        return;

    if (m_scandesc != NULL) {
        scan_handler_idx_endscan(m_scandesc);
        m_scandesc = NULL;
    }
    if (m_index != NULL) {
        if (m_node->scan.isPartTbl) {
            partitionClose(m_parentIndex, m_partIndex, NoLock);
            if (!PARTITION_ENABLE_CACHE_OPFUSION) {
                releaseDummyRelation(&m_index);
            } else {
                m_index->rd_refcnt--;
            }
            index_close(m_parentIndex, NoLock);
        } else {
            index_close(m_index, AccessShareLock);
        }
        m_index = NULL;
    }
    if (m_rel != NULL) {
        if (m_node->scan.isPartTbl) {
            partitionClose(m_parentRel, m_partRel, NoLock);
            if (!PARTITION_ENABLE_CACHE_OPFUSION) {
                releaseDummyRelation(&m_rel);
            } else {
                m_rel->rd_refcnt--;
            }
            heap_close(m_parentRel, NoLock);
        } else {
            heap_close(m_rel, AccessShareLock);
        }
        m_rel = NULL;
    }
    if (m_reslot != NULL) {
        (void)ExecClearTuple(m_reslot);
        m_reslot = NULL;
    }
}
