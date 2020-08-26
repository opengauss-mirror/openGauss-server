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
    m_tupDesc = NULL;
    m_reslot = NULL;    
    m_direction = NULL;
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

/* IndexScanPart */
IndexScanFusion::IndexScanFusion(IndexScan* node, PlannedStmt* planstmt, ParamListInfo params)
    : IndexFusion(params, planstmt)
{
    m_tmpvals = NULL;
    m_scandesc = NULL;
    m_tmpisnull = NULL;
    m_isnull = NULL;
    m_values = NULL;
    m_index = NULL;
    m_epq_indexqual = NULL;

    m_node = node;
    m_keyInit = false;
    m_keyNum = list_length(node->indexqual);
    ;
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

    m_reloid = getrelid(m_node->scan.scanrelid, planstmt->rtable);
    m_targetList = m_node->scan.plan.targetlist;
    m_tupDesc = ExecCleanTypeFromTL(m_targetList, false);
    m_direction = (ScanDirection*)palloc0(sizeof(ScanDirection));

    m_rel = heap_open(m_reloid, AccessShareLock);
    Relation rel = m_rel;
    m_attrno = (int16*)palloc(m_tupDesc->natts * sizeof(int16));
    m_values = (Datum*)palloc(RelationGetDescr(rel)->natts * sizeof(Datum));
    m_tmpvals = (Datum*)palloc(m_tupDesc->natts * sizeof(Datum));
    m_isnull = (bool*)palloc(RelationGetDescr(rel)->natts * sizeof(bool));
    m_tmpisnull = (bool*)palloc(m_tupDesc->natts * sizeof(bool));
    setAttrNo();
    heap_close(m_rel, AccessShareLock);
}

void IndexScanFusion::Init(long max_rows)
{
    m_index = index_open(m_node->indexid, AccessShareLock);

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

    m_rel = heap_open(m_reloid, AccessShareLock);
    ScanState* scanstate = makeNode(ScanState); // need release
    scanstate->ps.plan =  (Plan *)m_node;
    m_scandesc = (AbsIdxScanDesc)abs_idx_beginscan(m_rel, m_index, GetActiveSnapshot(), m_keyNum, 0, scanstate); // add scanstate pointer ?
    
    abs_idx_rescan_local(m_scandesc, m_keyNum > 0 ? m_scanKeys : NULL, m_keyNum, NULL, 0);
    m_epq_indexqual = m_node->indexqualorig;
    m_reslot = MakeSingleTupleTableSlot(m_tupDesc);
}

HeapTuple IndexScanFusion::getTuple()
{
    return abs_idx_getnext(m_scandesc, *m_direction);
}

TupleTableSlot* IndexScanFusion::getTupleSlot()
{
    do {
        Relation rel = m_rel;
        HeapTuple tuple = getTuple();
        if (tuple == NULL) {
            return NULL;
        }
        IndexScanDesc indexScan = GetIndexScanDesc(m_scandesc);

        heap_deform_tuple(tuple, RelationGetDescr(rel), m_values, m_isnull);
        if (indexScan->xs_recheck && EpqCheck(m_values, m_isnull)) {
            continue;
        }

        /* mapping */
        for (int i = 0; i < m_tupDesc->natts; i++) {
            Assert(m_attrno[i] > 0);
            m_tmpvals[i] = m_values[m_attrno[i] - 1];
            m_tmpisnull[i] = m_isnull[m_attrno[i] - 1];
        }

        HeapTuple tmptup = heap_form_tuple(m_tupDesc, m_tmpvals, m_tmpisnull);
        Assert(tmptup != NULL);

        {
            (void)ExecStoreTuple(tmptup, /* tuple to store */
                m_reslot,                /* slot to store in */
                InvalidBuffer,           /* TO DO: survey */
                false);                  /* don't pfree this pointer */
            slot_getsomeattrs(m_reslot, m_tupDesc->natts);
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
    }
    if (m_scandesc != NULL) {
        abs_idx_endscan(m_scandesc);
    }
    if (m_index != NULL) {
        index_close(m_index, NoLock);
    }
    if (m_rel != NULL) {
        heap_close(m_rel, NoLock);
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
    m_targetList = m_node->scan.plan.targetlist;
    m_reloid = getrelid(m_node->scan.scanrelid, planstmt->rtable);
    m_tupDesc = ExecCleanTypeFromTL(m_targetList, false);
    m_reslot = MakeSingleTupleTableSlot(m_tupDesc);
    m_direction = (ScanDirection*)palloc0(sizeof(ScanDirection));
    m_VMBuffer = InvalidBuffer;

    m_index = index_open(m_node->indexid, AccessShareLock);
    Relation rel = m_index;
    m_attrno = (int16*)palloc(m_tupDesc->natts * sizeof(int16));
    m_values = (Datum*)palloc(RelationGetDescr(rel)->natts * sizeof(Datum));
    m_tmpvals = (Datum*)palloc(m_tupDesc->natts * sizeof(Datum));
    m_isnull = (bool*)palloc(RelationGetDescr(rel)->natts * sizeof(bool));
    m_tmpisnull = (bool*)palloc(m_tupDesc->natts * sizeof(bool));
    setAttrNo();
    index_close(m_index, AccessShareLock);
}

void IndexOnlyScanFusion::Init(long max_rows)
{
    m_index = index_open(m_node->indexid, AccessShareLock);

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

    m_rel = heap_open(m_reloid, AccessShareLock);
    ScanState* scanstate = makeNode(ScanState); // need release
    scanstate->ps.plan =  (Plan *)m_node;

    m_scandesc = (AbsIdxScanDesc)abs_idx_beginscan(m_rel, m_index, GetActiveSnapshot(), m_keyNum, 0, scanstate); // add scanstate pointer ?

    if (PointerIsValid(m_scandesc)) {
        m_VMBuffer = InvalidBuffer;
        IndexScanDesc indexdesc = GetIndexScanDesc(m_scandesc);
        indexdesc->xs_want_itup = true;
    }

    abs_idx_rescan_local(m_scandesc, m_keyNum > 0 ? m_scanKeys : NULL, m_keyNum, NULL, 0);

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
    Relation rel = m_index;
    while ((tid = abs_idx_getnext_tid(m_scandesc, *m_direction)) != NULL) {
        HeapTuple tuple = NULL;
        IndexScanDesc indexdesc = GetIndexScanDesc(m_scandesc);
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
        slot_getsomeattrs(m_reslot, m_tupDesc->natts);
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
        abs_idx_endscan(m_scandesc);
    }
    if (m_index != NULL) {
        index_close(m_index, AccessShareLock);
    }
    if (m_rel != NULL) {
        heap_close(m_rel, AccessShareLock);
    }
    if (m_reslot != NULL) {
        (void)ExecClearTuple(m_reslot);
    }
}

