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
 * opfusion_scan.cpp
 *        The implementation for the scan operator of bypass executor.
 *
 * IDENTIFICATION
 *        src/gausskernel/runtime/executor/opfusion_scan.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "opfusion/opfusion_scan.h"
#include "catalog/pg_partition_fn.h"
#include "opfusion/opfusion.h"

#include "access/tableam.h"
#include "access/visibilitymap.h"
#include "executor/nodeIndexscan.h"
#include "optimizer/clauses.h"
#include "parser/parsetree.h"
#include "utils/lsyscache.h"
#include "utils/snapmgr.h"
#include "access/tableam.h"

ScanFusion::ScanFusion(ParamListInfo params, PlannedStmt* planstmt)
{
    m_params = params;
    m_planstmt = planstmt;
    m_rel = NULL;
    m_parentRel = NULL;
    m_tupDesc = NULL;
    m_reslot = NULL;
    m_direction = NULL;
    m_partRel = NULL;
};

ScanFusion* ScanFusion::getScanFusion(Node* node, PlannedStmt* planstmt, ParamListInfo params)
{
    ScanFusion* scan = NULL;

    switch (nodeTag(node)) {
        case T_IndexScan:
            scan = New(CurrentMemoryContext) IndexScanFusion((IndexScan*)node, planstmt, params);
            break;

        case T_IndexOnlyScan:
            scan = New(CurrentMemoryContext) IndexOnlyScanFusion((IndexOnlyScan*)node, planstmt, params);
            break;

        default:
            ereport(ERROR,
                (errmodule(MOD_EXECUTOR),
                    errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmsg("unrecognized node type: %d when executing executor node.", (int)nodeTag(node))));
            break;
    }

    return scan;
}

void ScanFusion::refreshParameter(ParamListInfo params)
{
    m_params = params;
}
/* IndexFetchPart */
IndexFusion::IndexFusion(ParamListInfo params, PlannedStmt* planstmt) : ScanFusion(params, planstmt)
{
    m_direction = NULL;
    m_attrno = NULL;
    m_targetList = NULL;
    m_epq_indexqual = NULL;
    m_tmpvals = NULL;
    m_isnull = NULL;
    m_values = NULL;
    m_reloid = 0;
    m_paramNum = 0;
    m_tmpisnull = NULL;
    m_keyNum = 0;
    m_paramLoc = NULL;
    m_scandesc = NULL;
    m_scanKeys = NULL;
    m_index = NULL;
    m_parentRel = NULL;
    m_partRel = NULL;
    m_parentIndex = NULL;
    m_partIndex = NULL;
    m_keyInit = false;
}

void IndexFusion::refreshParameterIfNecessary()
{
    for (int i = 0; i < m_paramNum; i++) {
        m_scanKeys[m_paramLoc[i].scanKeyIndx].sk_argument = m_params->params[m_paramLoc[i].paramId - 1].value;

        if (m_params->params[m_paramLoc[i].paramId - 1].isnull) {
            m_scanKeys[m_paramLoc[i].scanKeyIndx].sk_flags |= SK_ISNULL;
        }
    }
}

void IndexFusion::BuildNullTestScanKey(Expr* clause, Expr* leftop, ScanKey this_scan_key)
{
    /* indexkey IS NULL or indexkey IS NOT NULL */
    NullTest* ntest = (NullTest*)clause;
    int flags;

    leftop = ntest->arg;

    if (leftop != NULL && IsA(leftop, RelabelType))
        leftop = ((RelabelType*)leftop)->arg;

    Assert(leftop != NULL);

    if (!(IsA(leftop, Var) && ((Var*)leftop)->varno == INDEX_VAR)) {
        ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED), errmsg("NullTest indexqual has wrong key")));
    }

    AttrNumber varattno = ((Var*)leftop)->varattno;

    /*
     * initialize the scan key's fields appropriately
     */
    switch (ntest->nulltesttype) {
        case IS_NULL:
            flags = SK_ISNULL | SK_SEARCHNULL;
            break;
        case IS_NOT_NULL:
            flags = SK_ISNULL | SK_SEARCHNOTNULL;
            break;
        default:
            ereport(ERROR,
                (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmsg("unrecognized nulltesttype: %d", (int)ntest->nulltesttype)));
            flags = 0; /* keep compiler quiet */
            break;
    }

    ScanKeyEntryInitialize(this_scan_key,
        flags,
        varattno,        /* attribute number to scan */
        InvalidStrategy, /* no strategy */
        InvalidOid,      /* no strategy subtype */
        InvalidOid,      /* no collation */
        InvalidOid,      /* no reg proc for this */
        (Datum)0);       /* constant */
}

void IndexFusion::IndexBuildScanKey(List* indexqual)
{
    ListCell* qual_cell = NULL;

    int i = 0;
    foreach (qual_cell, indexqual) {
        Expr* clause = (Expr*)lfirst(qual_cell);
        ScanKey this_scan_key = &m_scanKeys[i++];
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

        get_op_opfamily_properties(opno, opfamily, false, &op_strategy, &op_lefttype, &op_righttype);

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

bool IndexFusion::EpqCheck(Datum* values, const bool* isnull)
{
    ListCell* lc = NULL;
    int i = 0;
    AttrNumber att_num = 0;
    foreach (lc, m_epq_indexqual) {
        if (IsA(lfirst(lc), NullTest)) {
            NullTest* nullexpr = (NullTest*)lfirst(lc);
            NullTestType nulltype = nullexpr->nulltesttype;
            Expr* leftop = nullexpr->arg;
            if (leftop != NULL && IsA(leftop, RelabelType))
                leftop = ((RelabelType*)leftop)->arg;

            Assert(leftop != NULL);
            att_num = ((Var*)leftop)->varattno - 1;
            switch (nulltype) {
                case IS_NULL:
                    if (isnull[att_num] == false) {
                        return false;
                    }
                    break;
                case IS_NOT_NULL:
                    if (isnull[att_num] == true) {
                        return false;
                    }
                    break;
                default:
                    ereport(ERROR,
                        (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                            errmsg("unrecognized nulltesttype: %d", (int)nulltype)));
                    break;
            }
        } else if (IsA(lfirst(lc), OpExpr)) {
            OpExpr* opexpr = (OpExpr*)lfirst(lc);
            Expr* leftop = (Expr*)linitial(opexpr->args);
            if (leftop != NULL && IsA(leftop, RelabelType))
                leftop = ((RelabelType*)leftop)->arg;

            Assert(IsA(leftop, Var));
            att_num = ((Var*)leftop)->varattno - 1;

            if (OidFunctionCall2(opexpr->opfuncid, values[att_num], m_scanKeys[i].sk_argument) == false) {
                return false;
            }
        } else {
            Assert(0);
            ereport(ERROR,
                (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmsg("unsupport bypass indexqual type: %d", (int)(((Node*)(lfirst(lc)))->type))));
        }
        i++;
    }

    return true;
}

void IndexFusion::setAttrNo()
{
    ListCell* lc = NULL;
    int cur_resno = 1;
    foreach (lc, m_targetList) {
        TargetEntry *res = (TargetEntry*)lfirst(lc);
        if (res->resjunk) {
            continue;
        }

        Var *var = (Var*)res->expr;
        m_attrno[cur_resno - 1] = var->varattno;
        cur_resno++;
    }
}

void IndexFusion::UpdateCurrentRel(Relation* rel)
{
    IndexScanDesc indexScan = GetIndexScanDesc(m_scandesc);

    /* Just update rel for global partition index */
    if (!RelationIsPartitioned(m_rel)) {
        return;
    }

    if (indexScan->xs_gpi_scan != NULL) {
        *rel = indexScan->xs_gpi_scan->fakePartRelation;
    } else {
        ereport(ERROR,
            (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                errmsg("partitioned relation dose not use global partition index")));
    }
}
/* init the Partition Oid in construct */
Oid GetRelOidForPartitionTable(Scan scan, const Relation rel, ParamListInfo params)
{
    Oid relOid = InvalidOid;
    if (params != NULL) {
        Param* paramArg = scan.pruningInfo->paramArg;
        relOid = GetPartitionOidByParam(rel, paramArg, &(params->params[paramArg->paramid - 1]));
    } else {
        Assert((list_length(scan.pruningInfo->ls_rangeSelectedPartitions) != 0));
        int partId = lfirst_int(list_head(scan.pruningInfo->ls_rangeSelectedPartitions));
        relOid = getPartitionOidFromSequence(rel, partId);
    }
    return relOid;
}

/* init the index in construct */
Relation InitPartitionIndexInFusion(Oid parentIndexOid, Oid partOid, Partition* partIndex, Relation* parentIndex, Relation rel)
{
    *parentIndex = relation_open(parentIndexOid, AccessShareLock);
    Oid partIndexOid;
    if (list_length(rel->rd_indexlist) == 1) {
        partIndexOid = lfirst_oid(rel->rd_indexlist->head);
    } else {
        partIndexOid = getPartitionIndexOid(parentIndexOid, partOid);
    }
    *partIndex = partitionOpen(*parentIndex, partIndexOid, AccessShareLock);
    Relation index = partitionGetRelation(*parentIndex, *partIndex);
    return index;
}

/* init the Partition in construct */
void InitPartitionRelationInFusion(Oid partOid, Relation parentRel, Partition* partRel, Relation* rel)
{
    *partRel = partitionOpen(parentRel, partOid, AccessShareLock);
    (void)PartitionGetPartIndexList(*partRel);
    *rel = partitionGetRelation(parentRel, *partRel);
}

/* execute the process of done in construct */
static void ExeceDoneInIndexFusionConstruct(bool isPartTbl, Relation* parentRel, Partition* part,
                                            Relation* index, Relation* rel)
{
    if (isPartTbl) {
        partitionClose(*parentRel, *part, AccessShareLock);
        *part = NULL;
        if (*index != NULL) {
            releaseDummyRelation(index);
            *index = NULL;
        }
        releaseDummyRelation(rel);
        heap_close(*parentRel, AccessShareLock);
        *parentRel = NULL;
        *rel = NULL;
    } else {
        heap_close((*index == NULL) ? *rel : *index, AccessShareLock);
        *index = NULL;
        *rel = NULL;
    }
}

/* IndexScanPart */
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

        scan_handler_idx_rescan_local(m_scandesc, m_keyNum > 0 ? m_scanKeys : NULL, m_keyNum, NULL, 0);

        scanstate->ps.state = NULL;
    } else {
        /* add scanstate pointer ? */
        m_scandesc = scan_handler_idx_beginscan(m_rel, m_index, GetActiveSnapshot(), m_keyNum, 0, scanstate);

        scan_handler_idx_rescan_local(m_scandesc, m_keyNum > 0 ? m_scanKeys : NULL, m_keyNum, NULL, 0);
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
    return scan_handler_idx_getnext(m_scandesc, *m_direction);
}

TupleTableSlot* IndexScanFusion::getTupleSlot()
{
    do {
        if (m_scandesc == NULL) {
            return NULL;
        }

        Relation rel = m_rel;
        HeapTuple tuple = getTuple();
        if (tuple == NULL) {
            return NULL;
        }
        IndexScanDesc indexScan = GetIndexScanDesc(m_scandesc);

        tableam_tops_deform_tuple(tuple, RelationGetDescr(rel), m_values, m_isnull);
        if (indexScan->xs_recheck && EpqCheck(m_values, m_isnull)) {
            continue;
        }

        /* mapping */
        for (int i = 0; i < m_tupDesc->natts; i++) {
            Assert(m_attrno[i] > 0);
            m_tmpvals[i] = m_values[m_attrno[i] - 1];
            m_tmpisnull[i] = m_isnull[m_attrno[i] - 1];
        }

        Tuple tmptup = tableam_tops_form_tuple(m_tupDesc, m_tmpvals, m_tmpisnull, HEAP_TUPLE);
        Assert(tmptup != NULL);

        {
            (void)ExecStoreTuple((HeapTuple)tmptup, /* tuple to store */
                m_reslot,                /* slot to store in */
                InvalidBuffer,           /* TO DO: survey */
                false);                  /* don't pfree this pointer */

            tableam_tslot_getsomeattrs(m_reslot, m_tupDesc->natts);
            return m_reslot;
        }
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
            partitionClose(m_parentIndex, m_partIndex, AccessShareLock);
            releaseDummyRelation(&m_index);
            index_close(m_parentIndex, AccessShareLock);
        } else {
            index_close(m_index, AccessShareLock);
        }
        m_index = NULL;
    }
    if (m_rel != NULL) {
        if (m_node->scan.isPartTbl) {
            partitionClose(m_parentRel, m_partRel, AccessShareLock);
            releaseDummyRelation(&m_rel);
            heap_close(m_parentRel, AccessShareLock);
        } else {
            heap_close(m_rel, AccessShareLock);
        }
        m_rel = NULL;
    }
}

/* index only scan section */
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

    m_reslot = MakeSingleTupleTableSlot(m_tupDesc, false, m_rel->rd_tam_type);
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

TupleTableSlot* IndexOnlyScanFusion::getTupleSlot()
{
    ItemPointer tid;
    if (m_scandesc == NULL) {
        return NULL;
    }

    Relation rel = m_index;
    while ((tid = scan_handler_idx_getnext_tid(m_scandesc, *m_direction)) != NULL) {
        HeapTuple tuple = NULL;
        IndexScanDesc indexdesc = GetIndexScanDesc(m_scandesc);
        if (IndexScanNeedSwitchPartRel(indexdesc)) {
            /*
             * Change the heapRelation in indexScanDesc to Partition Relation of current index
             */
            if (!GPIGetNextPartRelation(indexdesc->xs_gpi_scan, CurrentMemoryContext, AccessShareLock)) {
                continue;
            }
            indexdesc->heapRelation = indexdesc->xs_gpi_scan->fakePartRelation;
        }
        if (!visibilitymap_test(indexdesc->heapRelation, ItemPointerGetBlockNumber(tid), &m_VMBuffer)) {
            tuple = index_fetch_heap(indexdesc);
            if (tuple == NULL) {
                continue; /* no visible tuple, try next index entry */
            }

            if (indexdesc->xs_continue_hot) {
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("non-MVCC snapshots are not supported in index-only scans")));
            }
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
        return m_reslot;
    }
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
            partitionClose(m_parentIndex, m_partIndex, AccessShareLock);
            releaseDummyRelation(&m_index);
            index_close(m_parentIndex, AccessShareLock);
        } else {
            index_close(m_index, AccessShareLock);
        }
        m_index = NULL;
    }
    if (m_rel != NULL) {
        if (m_node->scan.isPartTbl) {
            partitionClose(m_parentRel, m_partRel, AccessShareLock);
            releaseDummyRelation(&m_rel);
            heap_close(m_parentRel, AccessShareLock);
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
