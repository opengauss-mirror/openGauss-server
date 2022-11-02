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
 * opfusion_index.cpp
 *        Definition of class IndexFusion for bypass executor.
 *
 * IDENTIFICATION
 *        src/gausskernel/runtime/opfusion/opfusion_index.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "opfusion/opfusion_index.h"

#include "access/tableam.h"
#include "opfusion/opfusion_util.h"
#include "optimizer/clauses.h"
#include "utils/lsyscache.h"

void IndexFusion::refreshParameterIfNecessary()
{
    for (int i = 0; i < m_paramNum; i++) {
        m_scanKeys[m_paramLoc[i].scanKeyIndx].sk_argument = m_params->params[m_paramLoc[i].paramId - 1].value;

        if (m_params->params[m_paramLoc[i].paramId - 1].isnull) {
            m_scanKeys[m_paramLoc[i].scanKeyIndx].sk_flags |= SK_ISNULL;
        } else {
            m_scanKeys[m_paramLoc[i].scanKeyIndx].sk_flags &= ~SK_ISNULL;
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