/* IndexScanPart */
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
 * opfusion_indexscan.cpp
 *        Definition of class IndexScanFusion for bypass executor.
 *
 * IDENTIFICATION
 *        src/gausskernel/runtime/opfusion/opfusion_indexscan.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "opfusion/opfusion_indexscan.h"

#include "access/tableam.h"
#include "opfusion/opfusion_util.h"

IndexScanFusion::IndexScanFusion(IndexScan* node, PlannedStmt* planstmt, ParamListInfo params)
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
        InitPartitionRelationInFusion(m_reloid, m_parentRel, &m_partRel, &m_rel);
    } else {
        m_reloid = getrelid(m_node->scan.scanrelid, planstmt->rtable);
        m_rel = heap_open(m_reloid, AccessShareLock);
    }
    m_targetList = m_node->scan.plan.targetlist;
    m_direction = (ScanDirection*)palloc0(sizeof(ScanDirection));

    Relation rel = m_rel;
    m_tupDesc = ExecCleanTypeFromTL(m_targetList, false, rel->rd_tam_type);
    m_attrno = (int16*)palloc(m_tupDesc->natts * sizeof(int16));
    m_values = (Datum*)palloc(RelationGetDescr(rel)->natts * sizeof(Datum));
    m_tmpvals = (Datum*)palloc(m_tupDesc->natts * sizeof(Datum));
    m_isnull = (bool*)palloc(RelationGetDescr(rel)->natts * sizeof(bool));
    m_tmpisnull = (bool*)palloc(m_tupDesc->natts * sizeof(bool));
    setAttrNo();
    Relation dummyIndex = NULL;
    ExeceDoneInIndexFusionConstruct(m_node->scan.isPartTbl, &m_parentRel, &m_partRel, &dummyIndex, &m_rel);
}


void IndexScanFusion::Init(long max_rows)
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

        /* add scanstate pointer ? */
        m_scandesc = scan_handler_idx_beginscan(m_rel, m_index, GetActiveSnapshot(), m_keyNum, 0, scanstate);
        scanstate->ps.state = NULL;
    } else {
        /* add scanstate pointer ? */
        m_scandesc = scan_handler_idx_beginscan(m_rel, m_index, GetActiveSnapshot(), m_keyNum, 0, scanstate);
    }

    if (m_scandesc) {
        scan_handler_idx_rescan_local(m_scandesc,
            m_keyNum > 0 ? m_scanKeys : NULL, m_keyNum, NULL, 0);
    }

    m_epq_indexqual = m_node->indexqualorig;
    m_reslot = MakeSingleTupleTableSlot(m_tupDesc, false, m_rel->rd_tam_type);
}

HeapTuple IndexScanFusion::getTuple()
{
    if (m_scandesc == NULL) {
        return NULL;
    }

    return scan_handler_idx_getnext(m_scandesc, *m_direction);
}

UHeapTuple IndexScanFusion::getUTuple()
{
    UHeapTuple utuple = NULL;
    if (!IndexGetnextSlot(m_scandesc, *m_direction, m_reslot)) {
        return NULL;
    }
    utuple = ExecGetUHeapTupleFromSlot(m_reslot);

    return utuple;
}

TupleTableSlot* IndexScanFusion::getTupleSlot()
{
    if (m_scandesc == NULL) {
        return NULL;
    }
    bool isUstore = false;
    UHeapTuple utuple = NULL;
    HeapTuple tuple = NULL;
    Relation rel = m_rel;
    isUstore = RelationIsUstoreFormat(rel);
    do {
        rel = m_rel;

        // XXXTAM: We needt to use a common API to replace ExecGetUHeapTupleFromSlot()
        // and getTuple() below and then use tableam_tops_deform_tuple() for the 
        // same returned generic tuple
        // ----------------------------------------------------------------------------
        if (isUstore) {
            if (!IndexGetnextSlot(m_scandesc, *m_direction, m_reslot)) {
                return NULL;
            }
            utuple = ExecGetUHeapTupleFromSlot(m_reslot);
            tableam_tops_deform_tuple(utuple, RelationGetDescr(rel), m_values, m_isnull);
        } else {
            tuple = getTuple();
            if (tuple == NULL) {
                return NULL;
            }
            tableam_tops_deform_tuple(tuple, RelationGetDescr(rel), m_values, m_isnull);
        }
        IndexScanDesc indexScan = GetIndexScanDesc(m_scandesc);

        if (indexScan->xs_recheck && EpqCheck(m_values, m_isnull)) {
            continue;
        }

        /* mapping */
        for (int i = 0; i < m_tupDesc->natts; i++) {
            Assert(m_attrno[i] > 0);
            m_tmpvals[i] = m_values[m_attrno[i] - 1];
            m_tmpisnull[i] = m_isnull[m_attrno[i] - 1];
        }

        Tuple tup = tableam_tops_form_tuple(m_tupDesc, m_tmpvals, m_tmpisnull, isUstore ? UHEAP_TUPLE : HEAP_TUPLE);
        Assert(tup != NULL);
        (void)ExecStoreTuple(tup, /* tuple to store */
            m_reslot,             /* slot to store in */
            InvalidBuffer,        /* TO DO: survey */
            false);               /* don't pfree this pointer */
        tableam_tslot_getsomeattrs(m_reslot, m_tupDesc->natts);
        return m_reslot;
    } while (1);
    return NULL;
}

void IndexScanFusion::End(bool isCompleted)
{
    if (!isCompleted)
        return;

    if (m_reslot != NULL) {
        (void)ExecClearTuple(m_reslot);
        m_reslot = NULL;
    }
    if (m_scandesc != NULL) {
        scan_handler_idx_endscan(m_scandesc);
        m_scandesc = NULL;
    }

    if (m_index != NULL) {
        if (m_node->scan.isPartTbl) {
            partitionClose(m_parentIndex, m_partIndex, NoLock);
            if (!PARTITION_ENABLE_CACHE_OPFUSION) {
                releaseDummyRelation(&m_index);
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
            }
            heap_close(m_parentRel, NoLock);
        } else {
            heap_close(m_rel, AccessShareLock);
        }
        m_rel = NULL;
    }

}
