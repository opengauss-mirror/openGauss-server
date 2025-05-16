/* AnnIndexScanPart */
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
 * opfusion_annindexscan.cpp
 *        Definition of class AnnIndexScanFusion for bypass executor.
 *
 * IDENTIFICATION
 *        src/gausskernel/runtime/opfusion/opfusion_annindexscan.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "opfusion/opfusion_annindexscan.h"

#include "access/tableam.h"
#include "opfusion/opfusion_util.h"
#include "utils/knl_partcache.h"

AnnIndexScanFusion::AnnIndexScanFusion(AnnIndexScan* node, PlannedStmt* planstmt, ParamListInfo params)
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
    m_tuple = NULL;

    m_node = node;
    m_keyInit = false;
    m_keyNum = list_length(node->indexqual);
    m_orderbyNum = list_length(node->indexorderby);
    
    m_scanKeys = (ScanKey)palloc0(m_keyNum * sizeof(ScanKeyData));
    m_orderbyKeys = (ScanKey)palloc0(m_orderbyNum * sizeof(ScanKeyData));

    /* init params */
    m_paramLoc = NULL;
    m_paramNum = 0;
    if (params != NULL) {
        m_paramLoc = (ParamLoc*)palloc0(m_orderbyNum * sizeof(ParamLoc));

        ListCell* lc = NULL;
        int i = 0;
        foreach (lc, node->indexorderby) {
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
    m_tupDesc = ExecCleanTypeFromTL(m_targetList, false, rel->rd_tam_ops);
    m_values = (Datum*)palloc(RelationGetDescr(rel)->natts * sizeof(Datum));
    m_tmpvals = (Datum*)palloc(m_tupDesc->natts * sizeof(Datum));
    m_isnull = (bool*)palloc(RelationGetDescr(rel)->natts * sizeof(bool));
    m_tmpisnull = (bool*)palloc(m_tupDesc->natts * sizeof(bool));
    m_isustore = false;
    if (RelationIsUstoreFormat(m_rel)) {
        m_attrno = (int16*)palloc(m_tupDesc->natts * sizeof(int16));
        m_isustore = true;
        setAttrNo();
    }
    Relation dummyIndex = NULL;
    ExeceDoneInIndexFusionConstruct(m_node->scan.isPartTbl, &m_parentRel, &m_partRel, &dummyIndex, &m_rel);
    m_can_reused = false;
    m_limit =false;
}

void AnnIndexScanFusion::SetUseLimit() 
{
    m_limit = true;
}

void AnnIndexScanFusion::Init(long max_rows)
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
        IndexOrderByKey(m_node->indexorderby);
        m_keyInit = true;
    }

    if (m_params != NULL) {
        AnnRefreshParameterIfNecessary();
    }

    *m_direction = ForwardScanDirection;

#ifdef ENABLE_MULTIPLE_NODES
    /*
     * for hash bucket pruning with Global Plan cache, we should build a temp EState object
     * to passing param info which is used to cal buckets in further hbkt_idx_beginscan.
     */
    if (ENABLE_GPC && RELATION_CREATE_BUCKET(m_rel)) {
        ScanState* scanstate = makeNode(ScanState); // need release

        scanstate->ps.plan =  (Plan *)m_node;
        EState tmpstate;
        tmpstate.es_param_list_info = m_params;
        scanstate->ps.state = &tmpstate;

        /* add scanstate pointer ? */
        m_scandesc = scan_handler_idx_beginscan(m_rel, m_index, GetActiveSnapshot(), m_keyNum, 0, scanstate);
        scanstate->ps.state = NULL;
    } else {
#endif
        /* add scanstate pointer ? */
        m_scandesc = scan_handler_idx_beginscan(m_rel, m_index, GetActiveSnapshot(), m_keyNum, m_orderbyNum, NULL);
#ifdef ENABLE_MULTIPLE_NODES
    }
#endif
    if (m_scandesc) {
        scan_handler_idx_rescan_local(m_scandesc,
            m_keyNum > 0 ? m_scanKeys : NULL, m_keyNum, 
            m_orderbyNum > 0 ? m_orderbyKeys : NULL, m_orderbyNum);
    }
    if (m_limit) {
        m_scandesc->count = (int64_t)max_rows;
    } else {
        m_scandesc->count = (int64_t)m_node->annCount;
    }

    m_epq_indexqual = m_node->indexqualorig;
    if (m_can_reused && m_reslot != NULL) {
        ExecSetSlotDescriptor(m_reslot, m_tupDesc); 
    } else {
        m_reslot = MakeSingleTupleTableSlot(m_tupDesc, false, m_rel->rd_tam_ops);
    }
    if (m_isustore) {
        m_tuple = New(CurrentMemoryContext) GetUTuple();
    } else {
        m_tuple = New(CurrentMemoryContext) GetATuple();
        setAttrMap();
        ExecStoreVirtualTuple(m_reslot);
        m_oldvalues = m_reslot->tts_values;
        m_oldisnull = m_reslot->tts_isnull;
        if (m_remap) {
            m_reslot->tts_values = m_tmpvals;
            m_reslot->tts_isnull = m_tmpisnull;
        } else {
            m_reslot->tts_values = m_values;
            m_reslot->tts_isnull = m_isnull; 
        }
    }
}

void AnnIndexScanFusion::setAttrMap() // lite ExecBuildProjectionInfoByRecursion
{
    ListCell* lc = NULL;
    int cur_resno = 0;
    TupleDesc  tup_desc = m_reslot->tts_tupleDescriptor;
    int num_attrs = tup_desc->natts;
    m_lastScanVar = 0;
    m_attrno = (int16*)palloc(m_tupDesc->natts * sizeof(int16));
    m_remap = false;

    foreach (lc, m_targetList) {
        TargetEntry *res = (TargetEntry*)lfirst(lc);
        Var *variable = (Var*)res->expr;
        bool isSimpleVar = false;
        if (variable != NULL && IsA(variable, Var) && variable->varattno > 0) {
            if (variable->varattno <= num_attrs) {
                Form_pg_attribute attr;

                attr = &tup_desc->attrs[variable->varattno - 1];
                if (!attr->attisdropped && variable->vartype == attr->atttypid)
                    isSimpleVar = true;
            }
        }
        if (isSimpleVar) {
            AttrNumber attnum = variable->varattno;
            m_attrno[cur_resno] = attnum;
            if (m_lastScanVar < attnum) {
                m_lastScanVar =  attnum;
            }
            cur_resno++;
        }
    }
    if (num_attrs != cur_resno) {
        m_remap = true;
    }
}
void AnnIndexScanFusion::IndexOrderByKey(List* indexqual)
{
    ListCell* qual_cell = NULL;

    int i = 0;
    foreach (qual_cell, indexqual) {
        Expr* clause = (Expr*)lfirst(qual_cell);
        ScanKey this_scan_key = &m_orderbyKeys[i++];
        Oid opno;              /* operator's OID */
        RegProcedure opfuncid; /* operator proc id used in scan */
        Oid opfamily;          /* opfamily of index column */
        int op_strategy;       /* operator's strategy number */
        Oid op_lefttype;       /* operator's declared input types */
        Oid op_righttype;
        Expr* leftop = NULL;  /* expr on lhs of operator */
        Expr* rightop = NULL; /* expr on rhs ... */
        AttrNumber varattno;  /* att number used in scan */

        if (IsA(clause, NullTest)) {
            BuildNullTestScanKey(clause, leftop, this_scan_key);
            continue;
        }

        Assert(IsA(clause, OpExpr));
        /* indexkey op const or indexkey op expression */
        uint32 flags = 0;
        Datum scan_value;

        opno = ((OpExpr*)clause)->opno;
        opfuncid = ((OpExpr*)clause)->opfuncid;

        /*
         * leftop should be the index key Var, possibly relabeled
         */
        leftop = (Expr*)get_leftop(clause);
        if (leftop && IsA(leftop, RelabelType))
            leftop = ((RelabelType*)leftop)->arg;

        Assert(leftop != NULL);

        if (!(IsA(leftop, Var) && ((Var*)leftop)->varno == INDEX_VAR)) {
            ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED), errmsg("indexqual doesn't have key on left side")));
        }

        varattno = ((Var*)leftop)->varattno;
        if (varattno < 1 || varattno > m_index->rd_index->indnatts) {
            ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED), errmsg("bogus index qualification")));
        }

        /*
         * We have to look up the operator's strategy number.  This
         * provides a cross-check that the operator does match the index.
         */
        opfamily = m_index->rd_opfamily[varattno - 1];

        get_op_opfamily_properties(opno, opfamily, true, &op_strategy, &op_lefttype, &op_righttype);

        /*
         * rightop is the constant or variable comparison value
         */
        rightop = (Expr*)get_rightop(clause);
        if (rightop != NULL && IsA(rightop, RelabelType)) {
            rightop = ((RelabelType*)rightop)->arg;
        }

        Assert(rightop != NULL);

        if (IsA(rightop, Const)) {
            /* OK, simple constant comparison value */
            scan_value = ((Const*)rightop)->constvalue;
            if (((Const*)rightop)->constisnull) {
                flags |= SK_ISNULL;
            }
        } else {
            /* runtime refresh */
            scan_value = 0;
        }
        /*
         * initialize the scan key's fields appropriately
         */
        ScanKeyEntryInitialize(this_scan_key,
            flags,
            varattno,                       /* attribute number to scan */
            op_strategy,                    /* op's strategy */
            op_righttype,                   /* strategy subtype */
            ((OpExpr*)clause)->inputcollid, /* collation */
            opfuncid,                       /* reg proc to use */
            scan_value);                     /* constant */
    }
}

void AnnIndexScanFusion::AnnRefreshParameterIfNecessary()
{
    for (int i = 0; i < m_paramNum; i++) {
        m_orderbyKeys[m_paramLoc[i].scanKeyIndx].sk_argument = m_params->params[m_paramLoc[i].paramId - 1].value;

        if (m_params->params[m_paramLoc[i].paramId - 1].isnull) {
            m_orderbyKeys[m_paramLoc[i].scanKeyIndx].sk_flags |= SK_ISNULL;
        } else {
            m_orderbyKeys[m_paramLoc[i].scanKeyIndx].sk_flags &= ~SK_ISNULL;
        }
    }
}

HeapTuple AnnIndexScanFusion::getTuple()
{
    return scan_handler_idx_getnext(m_scandesc, *m_direction);
}

UHeapTuple AnnIndexScanFusion::getUTuple()
{
    UHeapTuple utuple = NULL;
    if (!IndexGetnextSlot(m_scandesc, *m_direction, m_reslot)) {
        return NULL;
    }
    utuple = ExecGetUHeapTupleFromSlot(m_reslot);

    return utuple;
}

TupleTableSlot* GetATuple::getTuple(IndexFusion* index)
{
    do {
        HeapTuple tuple = index->getTuple();
        if (tuple == NULL) {
            return NULL;
        }
        AnnIndexScanFusion* annindex = (AnnIndexScanFusion*)index;
        heap_deform_tuple_natts(index->m_reslot, tuple, index->m_reslot->tts_tupleDescriptor,
            index->m_values, index->m_isnull, annindex->m_lastScanVar);
        IndexScanDesc indexScan = GetIndexScanDesc(index->m_scandesc);

        if (indexScan->xs_recheck && index->EpqCheck(index->m_values, index->m_isnull)) {
            continue;
        }

        /* remapping */
        if (annindex->m_remap) {
            for (int i = 0; i < index->m_tupDesc->natts; i++) {
                Assert(index->m_attrno[i] > 0);
                index->m_tmpvals[i] = index->m_values[index->m_attrno[i] - 1];
                index->m_tmpisnull[i] = index->m_isnull[index->m_attrno[i] - 1];
            }
        }
        return index->m_reslot;
    } while (true);

    return NULL;
}
TupleTableSlot* GetUTuple::getTuple(IndexFusion* index)
{
    UHeapTuple utuple = NULL;
    do {
        Relation rel = index->m_rel;

        // XXXTAM: We needt to use a common API to replace ExecGetUHeapTupleFromSlot()
        // and getTuple() below and then use tableam_tops_deform_tuple() for the 
        // same returned generic tuple
        // ----------------------------------------------------------------------------
        if (!IndexGetnextSlot(index->m_scandesc, *(index->m_direction), index->m_reslot)) {
            return NULL;
        }
        utuple = ExecGetUHeapTupleFromSlot(index->m_reslot);
        tableam_tops_deform_tuple(utuple, RelationGetDescr(rel), index->m_values, index->m_isnull);
        IndexScanDesc indexScan = GetIndexScanDesc(index->m_scandesc);

        if (indexScan->xs_recheck && index->EpqCheck(index->m_values, index->m_isnull)) {
            continue;
        }

        /* mapping */
        for (int i = 0; i < index->m_tupDesc->natts; i++) {
            Assert(index->m_attrno[i] > 0);
            index->m_tmpvals[i] = index->m_values[index->m_attrno[i] - 1];
            index->m_tmpisnull[i] = index->m_isnull[index->m_attrno[i] - 1];
        }

        Tuple tup = tableam_tops_form_tuple(index->m_tupDesc, index->m_tmpvals, index->m_tmpisnull, TableAmUstore);
        Assert(tup != NULL);
        (void)ExecStoreTuple(tup, /* tuple to store */
            index->m_reslot,             /* slot to store in */
            InvalidBuffer,        /* TO DO: survey */
            false);               /* don't pfree this pointer */
        tableam_tslot_getsomeattrs(index->m_reslot, index->m_tupDesc->natts);
        return index->m_reslot;
    } while (1);
    return NULL;
}

TupleTableSlot* AnnIndexScanFusion::getTupleSlot()
{
    return m_tuple->getTuple(this);
}

void AnnIndexScanFusion::End(bool isCompleted)
{
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
        if (!m_isustore) {
            m_reslot->tts_values =  m_oldvalues;
            m_reslot->tts_isnull = m_oldisnull;
            (void)ExecDropSingleTupleTableSlot(m_reslot);
        }
        m_reslot = NULL;
    }
    if (m_attrno != NULL && !m_isustore) {
        pfree_ext(m_attrno);
        m_attrno = NULL;
    }

}
void AnnIndexScanFusion::ResetAnnIndexScanFusion(AnnIndexScan* node, PlannedStmt* planstmt, ParamListInfo params)
{
    m_node = node;
    m_keyInit = false;
    m_keyNum = list_length(node->indexqual);
    m_orderbyNum =  list_length(node->indexorderby);

    m_scanKeys = (ScanKey)palloc0(m_keyNum * sizeof(ScanKeyData));
    m_orderbyKeys = (ScanKey)palloc0(m_orderbyNum * sizeof(ScanKeyData));

    /* init params */
    m_paramLoc = NULL;
    m_paramNum = 0;
    if (params != NULL) {
        m_paramLoc = (ParamLoc*)palloc0(m_orderbyNum * sizeof(ParamLoc));

        ListCell* lc = NULL;
        int i = 0;
        foreach (lc, node->indexorderby) {
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
    m_targetList = m_node->scan.plan.targetlist;

    setAttrNo();
    m_can_reused = true;
}
