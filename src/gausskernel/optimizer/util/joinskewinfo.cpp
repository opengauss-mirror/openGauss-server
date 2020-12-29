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
 * -------------------------------------------------------------------------
 *
 * joinskewinfo.cpp
 *	  functions for joinskew solution in MPP
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/optimizer/util/joinskewinfo.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include <math.h>

#include "catalog/pg_statistic.h"
#include "distributelayer/streamCore.h"
#include "executor/executor.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/cost.h"
#include "optimizer/dataskew.h"
#include "optimizer/optimizerdebug.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/tlist.h"
#include "optimizer/var.h"
#include "parser/parse_type.h"
#include "parser/parsetree.h"
#include "utils/lsyscache.h"
#include "utils/selfuncs.h"
#include "utils/syscache.h"
#include "utils/typcache.h"
#include "vecexecutor/vecexecutor.h"

#define IS_JOIN_OUTER(jointype)                                                        \
    (JOIN_LEFT == (jointype) || JOIN_RIGHT == (jointype) || JOIN_FULL == (jointype) || \
        JOIN_LEFT_ANTI_FULL == (jointype) || JOIN_RIGHT_ANTI_FULL == (jointype))

#define IS_JOIN_PLAN(plan) (IsA(plan, HashJoin) || IsA(plan, NestLoop) || IsA(plan, MergeJoin))


/* ===================== Functions for join skew info ====================== */
JoinSkewInfo::JoinSkewInfo(
    PlannerInfo* root, RelOptInfo* rel, List* join_clause, JoinType join_type, JoinType save_join_type)
    : SkewInfo(root), m_joinType(join_type), m_saveJoinType(save_join_type), m_joinClause(join_clause)
{
    m_innerStreamInfo = NULL;
    m_outerStreamInfo = NULL;
    m_innerSkewInfo = NIL;
    m_outerSkewInfo = NIL;
    m_skewInfo = NULL;
    m_distribution = NULL;
    m_isOuterStream = false;
    m_rel = rel;
    m_skewType = SKEW_JOIN;
}

/*
 * @Description: destructor function for join skew info.
 */
JoinSkewInfo::~JoinSkewInfo()
{
    m_joinClause = NULL;
    m_distribution = NULL;
    m_innerStreamInfo = NULL;
    m_outerStreamInfo = NULL;
    m_innerSkewInfo = NULL;
    m_outerSkewInfo = NULL;
    m_skewInfo = NULL;
}

/*
 * @Description: set stream info.
 *
 * @param[IN] inner_stream_info: stream info of inner side.
 * @param[IN] outer_stream_info: stream info of outer side.
 * @return void
 */
void JoinSkewInfo::setStreamInfo(
    StreamInfo* inner_stream_info, StreamInfo* outer_stream_info, Distribution* distribution)
{
    m_innerStreamInfo = inner_stream_info;
    m_outerStreamInfo = outer_stream_info;
    m_distribution = distribution;
}

/*
 * @Description: main entrance to find stream skew info.
 *
 * @return void
 */
uint32 JoinSkewInfo::findStreamSkewInfo()
{
    m_oldContext = MemoryContextSwitchTo(m_context);

    /* Check if these kinds of streams are possible to cause skew problem. */
    if (checkSkewPossibility(false) == false && checkSkewPossibility(true) == false) {
        MemoryContextSwitchTo(m_oldContext);
        return SKEW_RES_NONE;
    }

    /* Find skew value from base relation, including null values. */
    findBaseSkewInfo();

    /* Find skew null value generate by outer join (not base relation). */
    findNullSkewInfo();

    /* Add qual cost and check if null data is need. */
    addQualSkewInfo();

    /* Add skew info to stream info. */
    addToStreamInfo();

    /* Get result. */
    uint32 ret = getSkewInfo();

    /* Skew info is reused, so reset the status and memory each time. */
    resetSkewInfo();

    return ret;
}

/*
 * @Description: main entrance to find skew info from base relation.
 *
 * @return void
 */
void JoinSkewInfo::findBaseSkewInfo()
{
    /* Find skew values for both sides. */
    findBaseSkewValues(false);
    findBaseSkewValues(true);

    /* Add skew info for both sides. */
    addSkewInfoBothSides();
}

/*
 * @Description: the main entrance for null skew caused by outer join.
 *               Take 'select A.a1, B.b1 from A left join B on A.a0 = B.b0;'
 *               as an example, when some data in a0 dose not match any data
 *               in b0, then we out put data like (a1, NULL) as result.
 *               Actually there must be many data can not match and generate
 *               NULL result in real situation, which will cause NULL value
 *               skew in later hash redistribution.
 *
 * @return void
 */
void JoinSkewInfo::findNullSkewInfo()
{
    findSubNullSkew(true);
    findSubNullSkew(false);
}

/*
 * @Description: add qual cost and check if null data is need.
 *
 * @return void
 */
void JoinSkewInfo::addQualSkewInfo()
{
    addQualCost(false);
    addQualCost(true);
}

/*
 * @Description: add skew info to stream info.
 *
 * @return void
 */
void JoinSkewInfo::addToStreamInfo()
{
    /*
     * Hybrid stream type is needed for skew optimization which includes
     * PART_REDISTRIBUTE_PART_BROADCAST,
     * PART_REDISTRIBUTE_PART_ROUNDROBIN,
     * PART_REDISTRIBUTE_PART_LOCAL,
     * PART_LOCAL_PART_BROADCAST.
     */
    if (list_length(m_innerSkewInfo) > 0) {
        if (checkRedundant(false)) {
            m_innerStreamInfo->ssinfo = NIL;
            m_innerStreamInfo->type = STREAM_NONE;
        } else {
            m_innerStreamInfo->ssinfo = m_innerSkewInfo;
            m_innerStreamInfo->type = STREAM_HYBRID;
        }
    }

    if (list_length(m_outerSkewInfo) > 0) {
        if (checkRedundant(true)) {
            m_outerStreamInfo->ssinfo = NIL;
            m_outerStreamInfo->type = STREAM_NONE;
        } else {
            m_outerStreamInfo->ssinfo = m_outerSkewInfo;
            m_outerStreamInfo->type = STREAM_HYBRID;
        }
    }

    if (m_outerStreamInfo->type != STREAM_HYBRID && m_innerStreamInfo->type != STREAM_HYBRID) {
        m_hasStatSkew = false;
        m_hasHintSkew = false;
        m_hasRuleSkew = false;
    }
}

/*
 * @Description: find skew values from base relation.
 *
 * @param[IN] stream_outer: true -- this stream is outer side of join.
 * @return void
 */
void JoinSkewInfo::findBaseSkewValues(bool stream_outer)
{
    StreamInfo* sinfo = stream_outer ? m_outerStreamInfo : m_innerStreamInfo;
    m_skewInfo = stream_outer ? &m_outerSkewInfo : &m_innerSkewInfo;
    m_subrel = sinfo->subpath->parent;

    /* Data skew only occur when we hash-redistribute data. */
    if (checkSkewPossibility(stream_outer) == false) {
        *m_skewInfo = NIL;
        m_distributeKeys = NIL;
        return;
    }

    m_distributeKeys = sinfo->stream_keys;
    m_dop = sinfo->smpDesc.consumerDop;
    m_isMultiCol = (list_length(m_distributeKeys) > 1);

    /* When distribute key include multiple column. */
    if (m_isMultiCol) {
        *m_skewInfo = findMultiColSkewValues();
        updateMultiColSkewness(sinfo, *m_skewInfo);
    } else {
        *m_skewInfo = findSingleColSkewValues();
    }

    processSkewValue(*m_skewInfo);
}

/*
 * @Description: find null skew of one side of join caused by outer join.
 *
 * @param[IN] is_outer: true -- is the outer side of join.
 * @return void
 */
void JoinSkewInfo::findSubNullSkew(bool is_outer)
{
    StreamInfo* sinfo = is_outer ? m_outerStreamInfo : m_innerStreamInfo;
    m_distributeKeys = sinfo->stream_keys;
    m_dop = sinfo->smpDesc.consumerDop;
    m_skewInfo = is_outer ? &m_outerSkewInfo : &m_innerSkewInfo;

    if (checkSkewPossibility(is_outer) == false)
        return;

    traverseSubPath(sinfo->subpath);
}

/*
 * @Description: after skew value found in join's distribute column,
 *               we need to transfer the skew values to useful skew
 *               optimization info.
 *
 * @return void
 */
void JoinSkewInfo::addSkewInfoBothSides()
{
    List* inner_equal_keys = NIL;
    List* outer_equal_keys = NIL;
    List* inner_qual_list = NIL;
    List* outer_qual_list = NIL;
    List* tmp_list = NIL;

    if (m_innerSkewInfo == NIL && m_outerSkewInfo == NIL) {
        printSkewOptimizeDetail("No skew info is found in base rel.");
        return;
    }

    /* If we find the same skew value at both side, then delete one of them. */
    deleteDuplicateSkewValue();

    /* Check if we can optimize the skew problem. */
    deleteUnoptimzeSkew(false);
    deleteUnoptimzeSkew(true);

    /* Find keys equal to the skew keys. */
    inner_equal_keys = findOtherSidekeys(true);
    outer_equal_keys = findOtherSidekeys(false);
    if (inner_equal_keys == NIL || outer_equal_keys == NIL) {
        m_innerSkewInfo = NIL;
        m_outerSkewInfo = NIL;
        return;
    }

    /* Create quals for skew side. */
    inner_qual_list = createSkewQuals(false);
    outer_qual_list = createSkewQuals(true);

    /*
     * If we find skew values at inner side, we need also add qual to outer side.
     * Because we need to find the value at outer side to broadcast it.
     */
    tmp_list = addSkewInfoToOtherSide(false, outer_equal_keys);
    outer_qual_list = list_concat(outer_qual_list, tmp_list);

    tmp_list = addSkewInfoToOtherSide(true, inner_equal_keys);
    inner_qual_list = list_concat(inner_qual_list, tmp_list);

    m_innerSkewInfo = inner_qual_list;
    m_outerSkewInfo = outer_qual_list;
}

/*
 * @Description: when both sides of join have the same skew value, we just keep
 *               the more skew side. For example:
 *               select * from t1, t2 where t1.b = t2.b; (t1(hash a), t2(hash a))
 *               Then we find that t1.b has skew value X, and t2.b has skew value
 *               X too. Thus we can only keep one side of skew value X, when we
 *               find that the num of t1.b(X) is more than t2.b(X), then we take
 *               t1.b(X) as skew value and t2.b(X) as non-skew value. So, t2.b(V)
 *               is deleted from skew list.
 *
 * return void
 */
void JoinSkewInfo::deleteDuplicateSkewValue()
{
    if (m_innerSkewInfo == NIL || m_outerSkewInfo == NIL)
        return;

    if (m_isMultiCol)
        deleteDuplicateMultiColSkewValue();
    else
        deleteDuplicateSingleColSkewValue();
}

/*
 * @Description: delete duplicate skew value from skew list.
 *
 * return void
 */
void JoinSkewInfo::deleteDuplicateSingleColSkewValue()
{
    ListCell* lc1 = NULL;
    ListCell* lc2 = NULL;
    ColSkewInfo* cs1 = NULL;
    ColSkewInfo* cs2 = NULL;

    lc1 = m_innerSkewInfo->head;
    while (lc1 != NULL) {
        cs1 = (ColSkewInfo*)lfirst(lc1);
        lc1 = lc1->next;

        /* We have already delete all skew info at outer side. */
        if (m_outerSkewInfo == NIL)
            return;

        /* For null skew, no need to compare. */
        if (needPartBroadcast(cs1) == false)
            continue;

        lc2 = m_outerSkewInfo->head;
        while (lc2 != NULL) {
            cs2 = (ColSkewInfo*)lfirst(lc2);
            lc2 = lc2->next;

            /* For null skew, no need to compare. */
            if (needPartBroadcast(cs2) == false)
                continue;

            /* If match the skew value, then only keep the more skew side. */
            if (isSingleColSkewValueEqual(cs1, cs2)) {
                /* Keep the side which has more skew data ot which is set by hint. */
                if (findMoreSkewSideForSingleCol(cs1, cs2)) {
                    m_outerSkewInfo = list_delete_ptr(m_outerSkewInfo, (void*)cs2);
                    printSkewOptimizeDetail("Duplicate skew value is found at both side of join,"
                                            " and the outer side is less skew, so delete it.");
                } else {
                    m_innerSkewInfo = list_delete_ptr(m_innerSkewInfo, (void*)cs1);
                    printSkewOptimizeDetail("Duplicate skew value is found at both side of join,"
                                            " and the inner side is less skew, so delete it.");
                }
            }
        }
    }
}

/*
 * @Description: delete duplicate skew value from skew list.
 *
 * return void
 */
void JoinSkewInfo::deleteDuplicateMultiColSkewValue()
{
    ListCell* lc1 = NULL;
    ListCell* lc2 = NULL;
    MultiColSkewInfo* mcs1 = NULL;
    MultiColSkewInfo* mcs2 = NULL;
    bool equalconst = false;

    lc1 = m_innerSkewInfo->head;
    while (lc1 != NULL) {
        mcs1 = (MultiColSkewInfo*)lfirst(lc1);
        lc1 = lc1->next;

        /* If there is no skew values, which means it is a null skew. */
        if (needPartBroadcast(mcs1) == false)
            continue;

        /* We have already delete all skew info at outer side. */
        if (m_outerSkewInfo == NIL)
            return;

        lc2 = m_outerSkewInfo->head;
        while (lc2 != NULL) {
            mcs2 = (MultiColSkewInfo*)lfirst(lc2);
            lc2 = lc2->next;

            /* Null skew info. */
            if (needPartBroadcast(mcs2) == false)
                continue;

            /* Try to match all skew values. */
            equalconst = isMultiColSkewValueEqual(mcs1, mcs2);

            /* If all match, then only keep the more skew side. */
            if (equalconst) {
                /* Keep the side which has more skew data or which is set by hint. */
                if (findMoreSkewSideForMultiCol(mcs1, mcs2)) {
                    m_outerSkewInfo = list_delete_ptr(m_outerSkewInfo, (void*)mcs2);
                    printSkewOptimizeDetail("Duplicate skew value is found at both side of join,"
                                            " and the outer side is less skew, so delete it.");
                } else {
                    m_innerSkewInfo = list_delete_ptr(m_innerSkewInfo, (void*)mcs1);
                    printSkewOptimizeDetail("Duplicate skew value is found at both side of join,"
                                            " and the inner side is less skew, so delete it.");
                }
            }
        }
    }
}

/*
 * @Description: Find out which side has more skew data or which is set by hint.
 *
 * return true if cs1 has more skew data, false other way.
 */
bool JoinSkewInfo::findMoreSkewSideForSingleCol(ColSkewInfo* cs1, ColSkewInfo* cs2) const
{
    /* Both tables are set by hint. */
    if (cs1->mcv_ratio < 0 && cs2->mcv_ratio < 0) {
        if ((m_innerStreamInfo->subpath->parent->rows > m_outerStreamInfo->subpath->parent->rows)) {
            cs1->mcv_op_ratio = 1;
            return true;
        } else {
            cs2->mcv_op_ratio = 1;
            return false;
        }
    }
    /* Neither table is set by hint. */
    if (cs1->mcv_ratio >= 0 && cs2->mcv_ratio >= 0) {
        if ((m_innerStreamInfo->subpath->parent->rows * cs1->mcv_ratio >
                m_outerStreamInfo->subpath->parent->rows * cs2->mcv_ratio)) {
            cs1->mcv_op_ratio = cs2->mcv_ratio;
            return true;
        } else {
            cs2->mcv_op_ratio = cs1->mcv_ratio;
            return false;
        }
    }
    /* Only one of the table is set by hint. */
    if (cs1->mcv_ratio < 0) {
        cs1->mcv_op_ratio = cs2->mcv_ratio;
        return true;
    } else {
        cs2->mcv_op_ratio = cs1->mcv_ratio;
        return false;
    }
    return true;
}

/*
 * @Description: Find out which side has more skew data or which is set by hint.
 *
 * return true if mcs1 has more skew data, false other way.
 */
bool JoinSkewInfo::findMoreSkewSideForMultiCol(MultiColSkewInfo* mcs1, MultiColSkewInfo* mcs2) const
{
    /* Both tables are set by hint. */
    if (mcs1->mcv_ratio < 0 && mcs2->mcv_ratio < 0) {
        if ((m_innerStreamInfo->subpath->parent->rows > m_outerStreamInfo->subpath->parent->rows)) {
            mcs1->mcv_op_ratio = 1;
            return true;
        } else {
            mcs2->mcv_op_ratio = 1;
            return false;
        }
    }
    /* Neither table is set by hint. */
    if (mcs1->mcv_ratio >= 0 && mcs2->mcv_ratio >= 0) {
        if ((m_innerStreamInfo->subpath->parent->rows * mcs1->mcv_ratio >
                m_outerStreamInfo->subpath->parent->rows * mcs2->mcv_ratio)) {
            mcs1->mcv_op_ratio = mcs2->mcv_ratio;
            return true;
        } else {
            mcs2->mcv_op_ratio = mcs1->mcv_ratio;
            return false;
        }
    }
    /* Only one of the table is set by hint. */
    if (mcs1->mcv_ratio < 0) {
        mcs1->mcv_op_ratio = mcs2->mcv_ratio;
        return true;
    } else {
        mcs2->mcv_op_ratio = mcs1->mcv_ratio;
        return false;
    }
    return true;
}

/*
 * @Description: Even we find skew values from statistic or hint, we can not
 *               solve this problem now. So delete them from skew list.
 *
 * @param[IN] is_outer: this side is outer side.
 * @return void
 */
void JoinSkewInfo::deleteUnoptimzeSkew(bool is_outer)
{
    List* skewInfo = is_outer ? m_outerSkewInfo : m_innerSkewInfo;
    List* nullSkewInfo = NIL;
    ListCell* lc = NULL;

    if (skewInfo == NIL)
        return;

    if (checkSkewOptimization(is_outer) == false) {
        foreach(lc, skewInfo) {
            if (m_isMultiCol) {
                MultiColSkewInfo* mcsinfo = (MultiColSkewInfo*)lfirst(lc);

                /* Null skew do not need to broadcast the other side, so we can keep it. */
                if (mcsinfo->is_null)
                    nullSkewInfo = lappend(nullSkewInfo, mcsinfo);
            } else {
                ColSkewInfo* csinfo = (ColSkewInfo*)lfirst(lc);
                if (csinfo->is_null)
                    nullSkewInfo = lappend(nullSkewInfo, csinfo);
            }
        }

        if (is_outer)
            m_outerSkewInfo = nullSkewInfo;
        else
            m_innerSkewInfo = nullSkewInfo;
    }
}

/*
 * @Description: we need to compare the input data with the skew value
 *               during execution stage, so we should generate equal
 *               compare expression between the skew column data and
 *               the skew values.
 *
 * @param[IN] is_stream_outer: this side is outer side.
 * @return List*: equal operation list.
 */
List* JoinSkewInfo::createSkewQuals(bool is_stream_outer)
{
    if (m_isMultiCol)
        return createMultiColSkewQuals(is_stream_outer);
    else
        return createSingleColSkewQuals(is_stream_outer);
}

/*
 * @Description: generate equal compare expression for single skew column.
 *
 * @param[IN] is_stream_outer: this side is outer side.
 * @return List*: equal operation list.
 */
List* JoinSkewInfo::createSingleColSkewQuals(bool is_stream_outer)
{
    List* ssinfo = is_stream_outer ? m_outerSkewInfo : m_innerSkewInfo;
    List* skew_quals = NIL;
    List* quals = NIL;
    ListCell* lc = NULL;
    ColSkewInfo* csinfo = NULL;
    QualSkewInfo* qsinfo = NULL;

    if (ssinfo == NIL)
        return NIL;

    foreach(lc, ssinfo) {
        csinfo = (ColSkewInfo*)lfirst(lc);
        if (csinfo->is_null || (csinfo->value && csinfo->value->constisnull)) {
            NullTest* nulltest = NULL;
            nulltest = makeNullTest(IS_NULL, (Expr*)csinfo->var);
            quals = lappend(quals, (void*)nulltest);
        } else {
            OpExpr* op = NULL;
            op = createEqualExprForSkew((Node*)csinfo->var, csinfo->value);
            quals = lappend(quals, (void*)op);
        }
    }

    if (quals != NIL) {
        qsinfo = makeNode(QualSkewInfo);

        if (list_length(quals) > 1) {
            Expr* expr = makeBoolExpr(OR_EXPR, quals, -1);
            qsinfo->skew_quals = lappend(qsinfo->skew_quals, (void*)expr);
        } else {
            qsinfo->skew_quals = quals;
        }

        /*
         * If the producer threads less than consumer threads,
         * we need round robin to make sure data has been transfered to
         * each consumer threads evenly.
         */
        qsinfo->skew_stream_type = PART_REDISTRIBUTE_PART_ROUNDROBIN;
        skew_quals = lappend(skew_quals, (void*)qsinfo);
    }

    return skew_quals;
}

/*
 * @Description: generate equal compare expression for multi skew column.
 *
 * @param[IN] is_stream_outer: this side is outer side.
 * @return List*: equal operation list.
 */
List* JoinSkewInfo::createMultiColSkewQuals(bool is_stream_outer)
{
    List* ssinfo = is_stream_outer ? m_outerSkewInfo : m_innerSkewInfo;
    List* skew_qual_list = NIL;
    List* or_quals = NIL;
    ListCell* lc1 = NULL;
    ListCell* lc2 = NULL;
    MultiColSkewInfo* mcsinfo = NULL;
    QualSkewInfo* qsinfo = NULL;
    Const* con = NULL;
    Node* node = NULL;
    Expr* expr = NULL;

    if (ssinfo == NIL) {
        return NIL;
    }

    foreach(lc1, ssinfo) {
        mcsinfo = (MultiColSkewInfo*)lfirst(lc1);
        NullTest* nulltest = NULL;
        List* and_quals = NIL;
        if (mcsinfo->is_null) {
            foreach(lc2, mcsinfo->vars) {
                nulltest = makeNullTest(IS_NULL, (Expr*)lfirst(lc2));
                and_quals = lappend(and_quals, nulltest);
            }

            expr = makeBoolExpr(AND_EXPR, and_quals, -1);
            or_quals = lappend(or_quals, expr);
        } else {
            OpExpr* op = NULL;
            int nvars = list_length(mcsinfo->vars);
            for (int i = 0; i < nvars; i++) {
                node = (Node*)list_nth(mcsinfo->vars, i);
                con = (Const*)list_nth(mcsinfo->values, i);

                /*
                 * If we combine single skew values to a multiple vale,
                 * one of these values may be null.
                 */
                if (con == NULL || con->constisnull) {
                    nulltest = makeNullTest(IS_NULL, (Expr*)node);
                    and_quals = lappend(and_quals, nulltest);
                } else {
                    op = createEqualExprForSkew(node, con);
                    and_quals = lappend(and_quals, (void*)op);
                }
            }

            expr = makeBoolExpr(AND_EXPR, and_quals, -1);
            or_quals = lappend(or_quals, expr);
        }
    }

    if (or_quals != NIL) {
        qsinfo = makeNode(QualSkewInfo);

        if (list_length(or_quals) > 1) {
            expr = makeBoolExpr(OR_EXPR, or_quals, -1);
            qsinfo->skew_quals = lappend(qsinfo->skew_quals, (void*)expr);
        } else {
            qsinfo->skew_quals = or_quals;
        }

        /*
         * If the producer threads less than consumer threads,
         * we need round robin to make sure data has been transfered to
         * each consumer threads evenly.
         */
        qsinfo->skew_stream_type = PART_REDISTRIBUTE_PART_ROUNDROBIN;
        skew_qual_list = lappend(skew_qual_list, qsinfo);
    }

    return skew_qual_list;
}

/*
 * @Description: when we find a skew value at one side of join,
 *               then we need to add relate info to the other side.
 *               For example, when a skew value A is found in the
 *               outer side of join's column a1, we keep all data
 *               which equal A to local DN. Then the inner side
 *               should broadcast the data equal A to all DNs.
 *
 * @param[IN] is_outer_skew: the skew side is outer side.
 * @return List*: skew info list.
 */
List* JoinSkewInfo::addSkewInfoToOtherSide(bool is_outer_skew, List* other_keys)
{
    List* ssinfo = is_outer_skew ? m_outerSkewInfo : m_innerSkewInfo;

    if (ssinfo == NIL)
        return NIL;

    if (m_isMultiCol)
        return addMultiColSkewInfoToOtherSide(is_outer_skew, other_keys);
    else
        return addSingleColSkewInfoToOtherSide(is_outer_skew, other_keys);
}

/*
 * @Description: add skew info of single column for the other side.
 *
 * @param[IN] is_outer_skew: the skew side is outer side.
 * @return List*: skew info list.
 */
List* JoinSkewInfo::addSingleColSkewInfoToOtherSide(bool is_outer_skew, List* other_keys)
{
    List* ssinfo = is_outer_skew ? m_outerSkewInfo : m_innerSkewInfo;
    StreamInfo* other_sinfo = is_outer_skew ? m_innerStreamInfo : m_outerStreamInfo;
    List* skew_quals = NIL;
    List* quals = NIL;
    ListCell* lc = NULL;
    OpExpr* op = NULL;
    Node* key = NULL;
    ColSkewInfo* csinfo = NULL;
    QualSkewInfo* qsinfo = NULL;
    double broadcast_ratio = 0.0;

    if (ssinfo == NIL)
        return NIL;

    /*
     * If the other side is replicate, we dont need to add skew info.
     * However, there is a special situation when we add a redistribute
     * on replicate table to do hash filter.
     */
    if (other_sinfo->type == STREAM_BROADCAST ||
        (other_sinfo->type == STREAM_NONE && is_replicated_path(other_sinfo->subpath)))
        return NIL;

    if (other_sinfo->type == STREAM_REDISTRIBUTE && other_sinfo->smpDesc.distriType == LOCAL_BROADCAST)
        return NIL;

    key = (Node*)linitial(other_keys);

    foreach(lc, ssinfo) {
        csinfo = (ColSkewInfo*)lfirst(lc);

        /* No need to broadcast skew value when it is null */
        if (needPartBroadcast(csinfo) == false)
            continue;

        broadcast_ratio += csinfo->mcv_op_ratio;
        op = createEqualExprForSkew(key, csinfo->value);
        quals = lappend(quals, op);
    }

    if (quals != NIL) {
        qsinfo = makeNode(QualSkewInfo);
        qsinfo->skew_stream_type = chooseStreamForNoSkewSide(other_sinfo);
        qsinfo->broadcast_ratio = broadcast_ratio;
        if (list_length(quals) > 1) {
            Expr* expr = makeBoolExpr(OR_EXPR, quals, -1);
            qsinfo->skew_quals = lappend(qsinfo->skew_quals, (void*)expr);
        } else {
            qsinfo->skew_quals = quals;
        }

        skew_quals = lappend(skew_quals, (void*)qsinfo);
    }

    return skew_quals;
}

/*
 * @Description: add skew info of multi column for the other side.
 *
 * @param[IN] is_outer_skew: the skew side is outer side.
 * @return List*: skew info list.
 */
List* JoinSkewInfo::addMultiColSkewInfoToOtherSide(bool is_outer_skew, List* other_keys)
{
    List* ssinfo = is_outer_skew ? m_outerSkewInfo : m_innerSkewInfo;
    StreamInfo* other_sinfo = is_outer_skew ? m_innerStreamInfo : m_outerStreamInfo;
    List* skew_quals = NIL;
    List* or_quals = NIL;
    List* and_quals = NIL;
    ListCell* lc1 = NULL;
    NullTest* nulltest = NULL;
    OpExpr* op = NULL;
    Node* equal_var = NULL;
    Const* con = NULL;
    Expr* expr = NULL;
    MultiColSkewInfo* mcsinfo = NULL;
    QualSkewInfo* qsinfo = NULL;
    double broadcast_ratio = 0.0;
    int i;

    if (ssinfo == NIL)
        return NIL;

    /*
     * redistribute(skew) + broadcast will occur in nodegroup.
     * We dont need to do any extra work for the broadcast side.
     */
    if (other_sinfo->type == STREAM_BROADCAST)
        return NIL;

    if (other_sinfo->type == STREAM_REDISTRIBUTE && other_sinfo->smpDesc.distriType == LOCAL_BROADCAST)
        return NIL;

    foreach(lc1, ssinfo) {
        mcsinfo = (MultiColSkewInfo*)lfirst(lc1);

        /* No need to broadcast skew value when it is null */
        if (needPartBroadcast(mcsinfo) == false)
            continue;

        i = 0;
        and_quals = NIL;
        broadcast_ratio += mcsinfo->mcv_op_ratio;

        int nvar = list_length(other_keys);
        for (i = 0; i < nvar; i++) {
            equal_var = (Node*)list_nth(other_keys, i);
            con = (Const*)list_nth(mcsinfo->values, i);
            if (con == NULL || con->constisnull) {
                nulltest = makeNullTest(IS_NULL, (Expr*)equal_var);
                and_quals = lappend(and_quals, nulltest);
            } else {
                op = createEqualExprForSkew(equal_var, con);
                and_quals = lappend(and_quals, op);
            }
        }

        expr = makeBoolExpr(AND_EXPR, and_quals, -1);
        or_quals = lappend(or_quals, expr);
    }

    if (or_quals != NIL) {
        qsinfo = makeNode(QualSkewInfo);
        qsinfo->skew_stream_type = chooseStreamForNoSkewSide(other_sinfo);
        qsinfo->broadcast_ratio = broadcast_ratio;

        if (list_length(or_quals) > 1) {
            expr = makeBoolExpr(OR_EXPR, or_quals, -1);
            qsinfo->skew_quals = lappend(qsinfo->skew_quals, (void*)expr);
        } else {
            qsinfo->skew_quals = or_quals;
        }

        skew_quals = lappend(skew_quals, qsinfo);
    }

    return skew_quals;
}

/*
 * @Description: choose a suitable stream for the opposite side of skew side.
 *
 * @param[IN] sinfo: stream info.
 * @return SkewStreamType: skew stream type.
 */
SkewStreamType JoinSkewInfo::chooseStreamForNoSkewSide(StreamInfo* sinfo) const
{
    SkewStreamType sstype = PART_NONE;

    if (sinfo->type == STREAM_REDISTRIBUTE) {
        /*
         * Stream pair for parallel stream that may cause skew problem:
         * 1. split redistribute(skew) + split redistribute
         * 2. split redistribute(skew) + local redistribute
         * 3. split redistribute(skew) + local broadcast (nodegroup scenario)
         * 4. remote redistribute(skew) + remote redistribute
         * 5. remote redistribute(skew) + local gather
         */
        switch (sinfo->smpDesc.distriType) {
            case PARALLEL_NONE:
                sstype = PART_REDISTRIBUTE_PART_BROADCAST;
                break;
            case REMOTE_DISTRIBUTE:
            case REMOTE_SPLIT_DISTRIBUTE:
                sstype = PART_REDISTRIBUTE_PART_BROADCAST;
                break;
            case LOCAL_DISTRIBUTE:
                /*
                 * Because the executor treat local stream differently
                 * and only connect to consumer in the same datanode,
                 * we need to change local stream to remote redistribute.
                 */
                sstype = PART_REDISTRIBUTE_PART_BROADCAST;
                if (sinfo->smpDesc.consumerDop > 1)
                    sinfo->smpDesc.distriType = REMOTE_SPLIT_DISTRIBUTE;
                else
                    sinfo->smpDesc.distriType = REMOTE_DISTRIBUTE;
                break;
            case LOCAL_ROUNDROBIN:
                sstype = PART_LOCAL_PART_BROADCAST;
                sinfo->smpDesc.distriType = REMOTE_HYBRID;
                break;
            case LOCAL_BROADCAST:
                sstype = PART_NONE;
                break;
            default:
                sstype = PART_NONE;
                break;
        }
    } else if (sinfo->type == STREAM_NONE) {
        if (sinfo->subpath->dop > 1) {
            /*
             * In a case, split redistribute + local redistribute. The local
             * side alredy has been redistribute at the subquery's plan, so
             * we dont need to do local redistribute. However, when we try to
             * solve skew problem, we need a stream.
             */
            sinfo->smpDesc.consumerDop = sinfo->subpath->dop;
            sinfo->smpDesc.producerDop = sinfo->subpath->dop;
            sinfo->smpDesc.distriType = REMOTE_HYBRID;
            sstype = PART_LOCAL_PART_BROADCAST;
        } else {
            sstype = PART_LOCAL_PART_BROADCAST;
        }
    } else if (sinfo->type == STREAM_BROADCAST) {
        sstype = PART_NONE;
    }

    return sstype;
}

void JoinSkewInfo::traverseSubPath(Path* path)
{
    switch (path->type) {
        case T_NestPath:
        case T_MergePath:
        case T_HashPath: {
            JoinPath* jpath = (JoinPath*)path;

            if (IS_JOIN_OUTER(jpath->jointype)) {
                if (checkOuterJoinNulls(path)) {
                    m_hasRuleSkew = true;
                    return;
                }
            }

            traverseSubPath(jpath->outerjoinpath);
            traverseSubPath(jpath->innerjoinpath);
        }

        break;
        case T_AppendPath: {
            AppendPath* apath = (AppendPath*)path;
            ListCell* lc = NULL;
            Path* subpath = NULL;

            foreach(lc, apath->subpaths) {
                subpath = (Path*)lfirst(lc);
                traverseSubPath(subpath);
            }
        } break;
        case T_MergeAppendPath: {
            MergeAppendPath* mpath = (MergeAppendPath*)path;
            ListCell* lc = NULL;
            Path* subpath = NULL;

            foreach(lc, mpath->subpaths) {
                subpath = (Path*)lfirst(lc);
                traverseSubPath(subpath);
            }
        } break;
        case T_ResultPath: {
            ResultPath* rpath = (ResultPath*)path;
            traverseSubPath(rpath->subpath);
        } break;
        case T_UniquePath: {
            UniquePath* upath = (UniquePath*)path;
            traverseSubPath(upath->subpath);
        } break;
        case T_MaterialPath: {
            MaterialPath* mpath = (MaterialPath*)path;
            traverseSubPath(mpath->subpath);
        } break;
        case T_StreamPath: {
            StreamPath* spath = (StreamPath*)path;
            traverseSubPath(spath->subpath);
        } break;
        default:
            break;
    }
}

/*
 * @Description: check the null skew in outer join path.
 *
 * @param[IN] path: outer join path.
 * @return bool: true -- found null skew
 */
bool JoinSkewInfo::checkOuterJoinNulls(Path* jpath)
{
    if (!IS_JOIN_OUTER(((JoinPath*)jpath)->jointype))
        return false;

    List* target_list = jpath->parent->reltargetlist;
    List* subtarget_list = NIL;
    List* join_clauses = NIL;
    List* null_list = NIL;
    List* skew_cols = NIL;
    ListCell* lc = NULL;
    Node* node = NULL;
    QualSkewInfo* qsinfo = NULL;
    NullTest* nulltest = NULL;

    join_clauses = getJoinClause(jpath);
    subtarget_list = getSubTargetList((JoinPath*)jpath);
    null_list = findNullCols(target_list, subtarget_list, join_clauses);

    foreach(lc, m_distributeKeys) {
        node = (Node*)lfirst(lc);
        if (find_node_in_targetlist(node, null_list) >= 0) {
            skew_cols = lappend(skew_cols, (void*)node);
        }
    }

    /* Null skew occurs only when all the distribute key in in null cols. */
    if (skew_cols != NIL && list_length(m_distributeKeys) == list_length(skew_cols)) {
        qsinfo = makeNode(QualSkewInfo);
        qsinfo->skew_stream_type = PART_REDISTRIBUTE_PART_LOCAL;

        foreach(lc, skew_cols) {
            nulltest = makeNullTest(IS_NULL, (Expr*)lfirst(lc));
            qsinfo->skew_quals = lappend(qsinfo->skew_quals, (void*)nulltest);
        }

        *m_skewInfo = lappend(*m_skewInfo, qsinfo);
        printSkewOptimizeDetail("Found null skew caused by outer join.");

        return true;
    }

    return false;
}

/*
 * @Description: get all target list which may need add null value from sub path.
 *
 * @param[IN] jpath: join path.
 * @return List*: potential null skew column list.
 */
List* JoinSkewInfo::getSubTargetList(JoinPath* jpath) const
{
    Path* left_path = jpath->outerjoinpath;
    Path* right_path = jpath->innerjoinpath;
    List* subtarget_list = NIL;

    if (!IS_JOIN_OUTER(jpath->jointype))
        return NIL;

    /* find the target list that may need add null */
    if (jpath->jointype == JOIN_LEFT || jpath->jointype == JOIN_LEFT_ANTI_FULL) {
        subtarget_list = right_path->parent->reltargetlist;
    } else if (jpath->jointype == JOIN_RIGHT || jpath->jointype == JOIN_RIGHT_ANTI_FULL) {
        subtarget_list = left_path->parent->reltargetlist;
    } else if (jpath->jointype == JOIN_FULL) {
        subtarget_list = list_union(left_path->parent->reltargetlist, right_path->parent->reltargetlist);
    }

    return subtarget_list;
}

/*
 * @Description: calculate the cost of skew qual expression for one side.
 *
 * @return void
 */
void JoinSkewInfo::addQualCost(bool is_outer)
{
    List* qualList = is_outer ? m_outerSkewInfo : m_innerSkewInfo;
    ListCell* lc = NULL;
    QualSkewInfo* qsinfo = NULL;

    foreach(lc, qualList) {
        qsinfo = (QualSkewInfo*)lfirst(lc);
        cost_qual_eval(&qsinfo->qual_cost, qsinfo->skew_quals, m_root);
    }
}

/*
 * @Description: check if this stream is possible to cause skew.
 *
 * @param[IN] bool: outer side of join.
 * @return void
 */
bool JoinSkewInfo::checkSkewPossibility(bool is_outer)
{
    StreamInfo* sinfo = is_outer ? m_outerStreamInfo : m_innerStreamInfo;

    if (sinfo == NULL)
        return false;

    if (sinfo->stream_keys == NIL)
        return false;

    if (sinfo->type == STREAM_REDISTRIBUTE) {
        /* No need to check unique side of join. */
        if ((is_outer && m_saveJoinType == JOIN_UNIQUE_OUTER) || (!is_outer && m_saveJoinType == JOIN_UNIQUE_INNER))
            return false;

        /*
         * Handle parallel stream.
         * 1. Local Redistribute.
         *    Since we use sub path's distribute keys as local redistribute
         *    keys, so it will not cause skew problem.
         * 2. Local Broadcast / Local RoundRobin
         *    These kinds of stream won't cause skew problem.
         * 3. Split Redistribute.
         *    Only this kind will cause skew.
         */
        if (sinfo->smpDesc.distriType == PARALLEL_NONE || sinfo->smpDesc.distriType == REMOTE_SPLIT_DISTRIBUTE ||
            sinfo->smpDesc.distriType == REMOTE_DISTRIBUTE)
            return true;
    }

    return false;
}

/*
 * @Description: For skew value(not null), we need to broadcast the other side's
 *               data which equals the skew value to solve the skew problem.
 *               However in some case, we can not use broadcast, so in these
 *               situation, we can not solve skew problem now.
 *
 * @param[IN] is_outer: outer side of join.
 * @return void
 */
bool JoinSkewInfo::checkSkewOptimization(bool is_outer)
{
    /*
     * Forbiden this situation now, in case we do broadcast to outer join's null side.
     * Need solve this situation later.
     */
    if (is_outer) {
        StreamInfo* other_sinfo = m_innerStreamInfo;

        if (!can_broadcast_inner(m_joinType, m_saveJoinType,
                                 is_replicated_path(other_sinfo->subpath),
                                 other_sinfo->subpath->distribute_keys,
                                 other_sinfo->subpath))
            return false;
    } else {
        StreamInfo* other_sinfo = m_outerStreamInfo;

        if (!can_broadcast_outer(m_joinType, m_saveJoinType,
                                 is_replicated_path(other_sinfo->subpath),
                                 other_sinfo->subpath->distribute_keys,
                                 other_sinfo->subpath))
            return false;
    }

    return true;
}

/*
 * @Description: When two neighbor joins have the same join table and join col,
 *               and this col is skew, then we just need to do part roundrobin
 *               at the first join. For example:
 *               t1 inner join t2 on t1.a = t2.b inner join t3 on t1.a = t3.b
 *               and t1.a is a skew column.
 *
 * Example:        Join(t1.a = t3.c)
 *                 /               \
 *  Part RoundRobin(Redundant)   Part Broadcast
 *             /                       \
 *       Join(t1.a = t2.b)               Scan(t3)
 *            /       \
 * Part RoundRobin   Part Broadcast
 *       /                 \
 *   Scan(t1)             scan(t3)
 *
 *
 * @param[IN] is_outer: outer side of join.
 * @return bool: true -- this skew stream is redundant and can be removed.
 */
bool JoinSkewInfo::checkRedundant(bool is_outer)
{
    List* skewInfo = is_outer ? m_outerSkewInfo : m_innerSkewInfo;
    StreamInfo* sinfo = is_outer ? m_outerStreamInfo : m_innerStreamInfo;
    ListCell* lc = NULL;
    QualSkewInfo* qsinfo = NULL;

    foreach(lc, skewInfo) {
        qsinfo = (QualSkewInfo*)lfirst(lc);
        if (qsinfo->skew_stream_type != PART_REDISTRIBUTE_PART_ROUNDROBIN)
            return false;
    }

    return checkPathRedundant(sinfo->stream_keys, sinfo->subpath);
}

/*
 * @Description: Check if there is same part redistribute part roundrobin at
 *               under path.
 *
 * @param[IN] streamKeys: current distribute keys.
 * @param[IN] path: path to be checked.
 * @return bool: true -- this skew stream is redundant and can be removed.
 */
bool JoinSkewInfo::checkPathRedundant(List* streamKeys, Path* path)
{
    bool ret = false;

    switch (path->pathtype) {
        case T_NestLoop:
        case T_MergeJoin:
        case T_HashJoin: {
            JoinPath* jpath = (JoinPath*)path;

            if (path->locator_type == LOCATOR_TYPE_RROBIN) {
                ret = ret || checkPathRedundant(streamKeys, jpath->innerjoinpath);
                ret = ret || checkPathRedundant(streamKeys, jpath->outerjoinpath);
            } else {
                ret = false;
            }
        } break;

        case T_Material: {
            MaterialPath* mpath = (MaterialPath*)path;
            ret = checkPathRedundant(streamKeys, mpath->subpath);
        } break;

        case T_Unique: {
            UniquePath* upath = (UniquePath*)path;
            ret = checkPathRedundant(streamKeys, upath->subpath);
        } break;

        case T_Stream: {
            StreamPath* spath = (StreamPath*)path;
            if (list_length(spath->skew_list) > 0) {
                Distribution *d1, *d2;
                d1 = m_distribution;
                d2 = &spath->consumer_distribution;

                if (d1 == NULL) {
                    ret = false;
                    break;
                }

                /*
                 * There are 3 precondition when we confirm it is redundant stream:
                 * 1. they have the same distribute keys;
                 * 2. they are in the same nodegroup;
                 * 3. they have the same parallel degree.
                 */
                if (equal(streamKeys, path->distribute_keys) && ng_is_same_group(d1, d2) &&
                    m_dop == spath->smpDesc->consumerDop) {
                    ListCell* lc = NULL;
                    QualSkewInfo* qsinfo = NULL;
                    foreach(lc, spath->skew_list) {
                        qsinfo = (QualSkewInfo*)lfirst(lc);
                        if (qsinfo->skew_stream_type != PART_REDISTRIBUTE_PART_ROUNDROBIN)
                            break;
                    }

                    if (lc == NULL)
                        ret = true;
                }
            }
        } break;

        default:
            break;
    }

    return ret;
}

/*
 * @Description: reset member structer for later use.
 *
 * @return void
 */
void JoinSkewInfo::resetSkewInfo()
{
    /* Reset skew info. */
    m_innerSkewInfo = NIL;
    m_outerSkewInfo = NIL;

    m_distributeKeys = NIL;
    m_dop = 1;
    m_isMultiCol = false;

    m_hasStatSkew = false;
    m_hasHintSkew = false;
    m_hasRuleSkew = false;

    /* Switch to original context. */
    MemoryContextSwitchTo(m_oldContext);

    /* Copy skew info to parent context. */
    Assert(m_context != CurrentMemoryContext);
    m_innerStreamInfo->ssinfo = (List*)copyObject(m_innerStreamInfo->ssinfo);
    m_outerStreamInfo->ssinfo = (List*)copyObject(m_outerStreamInfo->ssinfo);

    /* Reset memory. */
    MemoryContextReset(m_context);
}

/*
 * @Description: find the distribute keys at the other side of join.
 *
 *
 * @param[IN] is_outer_skew: the outer side of join.
 * @return void
 */
List* JoinSkewInfo::findOtherSidekeys(bool is_outer_skew)
{
    StreamInfo* sinfo = is_outer_skew ? m_outerStreamInfo : m_innerStreamInfo;
    StreamInfo* otherSinfo = is_outer_skew ? m_innerStreamInfo : m_outerStreamInfo;
    List* equalKeys = NIL;

    /*
     * Try to find if we already have distribute keys at the other side,
     * if not, try to find the equal keys from join clause.
     */
    if (otherSinfo->stream_keys != NIL) {
        equalKeys = otherSinfo->stream_keys;
    } else {
        equalKeys = findEqualVarList(sinfo->stream_keys, sinfo->subpath->parent);
    }

    return equalKeys;
}

/*
 * @Description: find equal var list from join clause.
 *
 * @param[IN] skewList: distribute keys at skew side.
 * @param[IN] rel: rel info for skew side.
 * @return void
 */
List* JoinSkewInfo::findEqualVarList(List* skewList, RelOptInfo* rel)
{
    ListCell* lc = NULL;
    List* equalList = NIL;
    Node* node = NULL;
    Node* equalNode = NULL;

    if (skewList == NIL)
        return NIL;

    foreach(lc, skewList) {
        node = (Node*)lfirst(lc);
        equalNode = findEqualVar(node, rel);
        if (equalNode == NULL) {
            printSkewOptimizeDetail("Can not find equal expr for the skew column.");
            return NIL;
        } else {
            equalList = lappend(equalList, equalNode);
        }
    }

    return equalList;
}

/*
 * @Description: find equal var at the opposite side of join basee on join clauses.
 *
 * @param[IN] var: skew var
 * @param[IN] rel: the relation of skew side
 * @return Var*: equal var
 */
Node* JoinSkewInfo::findEqualVar(Node* var, RelOptInfo* rel)
{
    ListCell* lc = NULL;
    RestrictInfo* restrictinfo = NULL;
    Node* equal_var = NULL;
    Node* leftkey = NULL;
    Node* rightkey = NULL;
    Node* skewkey = NULL;
    Node* otherkey = NULL;
    OpExpr* op = NULL;
    bool skew_is_left = false;
    bool skew_is_right = false;

    foreach(lc, m_joinClause) {
        restrictinfo = (RestrictInfo*)lfirst(lc);
        op = (OpExpr*)restrictinfo->clause;

        leftkey = join_clause_get_join_key((Node*)restrictinfo->clause, true);
        rightkey = join_clause_get_join_key((Node*)restrictinfo->clause, false);
        skew_is_left = bms_is_subset(restrictinfo->left_relids, rel->relids);
        skew_is_right = bms_is_subset(restrictinfo->right_relids, rel->relids);

        if (skew_is_left) {
            skewkey = leftkey;
            otherkey = rightkey;
        } else if (skew_is_right) {
            skewkey = rightkey;
            otherkey = leftkey;
        } else {
            continue;
        }

        if (skewkey == NULL || otherkey == NULL)
            continue;

        /* Check if this key compatible with the skew column. */
        if (judge_node_compatible(m_root, (Node*)var, skewkey)) {
            equal_var = otherkey;
            break;
        } else {
            /* When the join clause is a expr which include the var, try to find  it. */
            if (IsA(var, Var)) {
                List* varList = pull_var_clause(skewkey, PVC_RECURSE_AGGREGATES, PVC_RECURSE_PLACEHOLDERS);

                if (list_length(varList) == 1) {
                    if (_equalSimpleVar(var, linitial(varList)))
                        equal_var = otherkey;
                }
            }
        }
    }

    return equal_var;
}

/* ===================== Functions for agg skew info ====================== */
/*
 * @Description: Constructor func for agg skew judgement and output the skew info.
 *
 * @param[IN] root: planner info for agg.
 * @param[IN] distribute_keys: distribute keys for this agg.
 * @param[IN] subplan: lefttree of agg.
 * @param[IN] rel_info: rel option info of agg.
 */
AggSkewInfo::AggSkewInfo(PlannerInfo* root, Plan* subplan, RelOptInfo* rel_info) : SkewInfo(root), m_subplan(subplan)
{
    m_subrel = rel_info;
    m_skewType = SKEW_AGG;
}

/*
 * @Description: destructor function for agg skew info.
 */
AggSkewInfo::~AggSkewInfo()
{
    m_subplan = NULL;
}

/*
 * @Description: set distribute keys to find skew info.
 *
 * @return void.
 */
void AggSkewInfo::setDistributeKeys(List* distribute_keys)
{
    m_distributeKeys = distribute_keys;
}

/*
 * @Description: main entrance to find stream skew info.
 *
 * @return void
 */
void AggSkewInfo::findStreamSkewInfo()
{
    MemoryContext old_cxt = MemoryContextSwitchTo(m_context);

    /* Skew info is reused, so reset the status each time. */
    resetSkewInfo();

    /* Find skew info from skew hint. */
    findHintSkewInfo();

    /* Find skew null value generate by outer join (not base relation). */
    findNullSkewInfo();

    /* Hint and rule skew info has a higher priority, then try to find statistic info. */
    if (!m_hasHintSkew && !m_hasRuleSkew)
        findStatSkewInfo();

    MemoryContextReset(m_context);
    MemoryContextSwitchTo(old_cxt);
}

/*
 * @Description: Find skew info from statistic for agg.
 *
 * @return void
 */
void AggSkewInfo::findHintSkewInfo()
{
    m_isMultiCol = (list_length(m_distributeKeys) > 1);

    /* Try to find skew info from hint. */
    if (m_isMultiCol) {
        /* When distribute keys include multiple columns. */
        List* rece = findMultiColSkewValuesFromHint(); /* receive List point and free it. */
        list_free_deep(rece);
    } else {
        /* When distribute key only has one column. */
        List* rece = findSingleColSkewValuesFromHint(); /* receive List point and free it. */
        list_free_deep(rece);
    }
}

/*
 * @Description: Find skew info from skew hint for agg.
 *
 * @return void
 */
void AggSkewInfo::findStatSkewInfo()
{
    List* svalues = NIL;
    m_isMultiCol = (list_length(m_distributeKeys) > 1);

    /* Try to find skew info from hint. */
    if (m_isMultiCol) {
        /* When distribute keys include multiple columns. */
        svalues = findMultiColSkewValuesFromStatistic();
    } else {
        /* When distribute key only has one column. */
        svalues = findSingleColSkewValuesFromStatistic();
    }

    if (list_length(svalues) >= 1)
        m_hasStatSkew = true;
}

/*
 * @Description: the main entrance for null skew caused by outer join.
 *               Take 'select A.a1, B.b1 from A left join B on A.a0 = B.b0;'
 *               as an example, when some data in a0 dose not match any data
 *               in b0, then we out put data like (a1, NULL) as result.
 *               Actually there must be many data can not match and generate
 *               NULL result in real situation, which will cause NULL value
 *               skew in later hash redistribution.
 *
 * @return void
 */
void AggSkewInfo::findNullSkewInfo()
{
    traverseSubPlan(m_subplan);
}

/* ======================= Functions for null skew ======================== */
void AggSkewInfo::traverseSubPlan(Plan* plan)
{
    /* Find the join node */
    switch (nodeTag(plan)) {
        case T_NestLoop:
        case T_MergeJoin:
        case T_HashJoin: {
            Join* join = (Join*)plan;
            if (IS_JOIN_OUTER(join->jointype)) {
                if (checkOuterJoinNullsForAgg(plan)) {
                    m_hasRuleSkew = true;
                    return;
                }
            }

            traverseSubPlan(plan->lefttree);
            traverseSubPlan(plan->righttree);
        } break;
        case T_Append: {
            Append* aplan = (Append*)plan;
            ListCell* lc = NULL;
            Plan* subplan = NULL;

            foreach(lc, aplan->appendplans) {
                subplan = (Plan*)lfirst(lc);
                traverseSubPlan(subplan);
            }
        } break;
        case T_MergeAppend: {
            MergeAppend* mplan = (MergeAppend*)plan;
            ListCell* lc = NULL;
            Plan* subplan = NULL;

            foreach(lc, mplan->mergeplans) {
                subplan = (Plan*)lfirst(lc);
                traverseSubPlan(subplan);
            }
        } break;
        case T_SubqueryScan: {
            /* we may need to think about plans in subquery. */
            break;
        }
        default:
            break;
    }
}

/*
 * @Description: check the null skew in outer join plan for agg operation.
 *
 * @param[IN] path: outer join plan.
 * @return bool: true -- found null skew
 */
bool AggSkewInfo::checkOuterJoinNullsForAgg(Plan* jplan) const
{
    if (!IS_JOIN_OUTER(((Join*)jplan)->jointype))
        return false;

    List* target_list = jplan->targetlist;
    List* subtarget_list = NIL;
    List* null_list = NIL;
    List* skew_cols = NIL;
    ListCell* lc = NULL;
    Node* node = NULL;

    /* Get the sub target list of outer join null side. */
    subtarget_list = getSubTargetListByPlan(jplan);

    /* null_list is the intersection of subtarget_list and targetlist. */
    foreach(lc, target_list) {
        node = (Node*)((TargetEntry*)lfirst(lc))->expr;
        if (find_node_in_targetlist(node, subtarget_list) >= 0) {
            null_list = lappend(null_list, (void*)node);
        }
    }

    /* Get the column that both in m_distributeKeys and null_list. */
    foreach(lc, m_distributeKeys) {
        node = (Node*)lfirst(lc);
        if (find_node_in_targetlist(node, null_list) >= 0) {
            skew_cols = lappend(skew_cols, (void*)node);
        }
    }

    /* Null skew occurs only when all the distribute key in in null cols. */
    if (skew_cols == NIL || list_length(m_distributeKeys) != list_length(skew_cols)) {
        list_free(skew_cols);
        return false;
    }

    return true;
}

/*
 * @Description: Get all target list which may need add null value from sub plan.
 *
 * @param[IN] plan: join plan.
 * @return List*: potential null skew column list.
 */
List* AggSkewInfo::getSubTargetListByPlan(Plan* plan) const
{
    if (!IS_JOIN_OUTER(((Join*)plan)->jointype))
        return NIL;

    List* sub_target = NIL;
    Join* join = (Join*)plan;

    /* find the target list that may need add null */
    if (JOIN_LEFT == join->jointype || JOIN_LEFT_ANTI_FULL == join->jointype) {
        sub_target = plan->righttree->targetlist;
    } else if (JOIN_RIGHT == join->jointype || JOIN_RIGHT_ANTI_FULL == join->jointype) {
        sub_target = plan->lefttree->targetlist;
    } else if (JOIN_FULL == join->jointype) {
        sub_target = list_union(plan->righttree->targetlist, plan->lefttree->targetlist);
    }

    return sub_target;
}

/*
 * @Description: reset member structer for later use.
 *
 * @return void
 */
void AggSkewInfo::resetSkewInfo()
{
    /* Reset skew info. */
    m_hasStatSkew = false;
    m_hasHintSkew = false;
    m_hasRuleSkew = false;
    m_isMultiCol = false;
}

/* ======================= Functions for skew exec ======================== */
/*
 * @Description: construct function for skew optimze plan execution.
 *
 * @param[IN] ssinfo: skew info of this stream.
 * @param[IN] estate: working state for an Execution.
 * @param[IN] isVec: if this is vec stream.
 */
StreamSkew::StreamSkew(List* ssinfo, bool isVec)
{
    m_ssinfo = ssinfo;
    m_estate = NULL;
    m_econtext = NULL;
    m_localNodeId = -1;
    m_skewQual = NIL;
}

/*
 * @Description: destructor function for stream skew.
 */
StreamSkew::~StreamSkew()
{
    m_ssinfo = NIL;
    if (m_skewQual != NIL) {
        list_free(m_skewQual);
        m_skewQual = NIL;
    }

    if (m_estate != NULL) {
        FreeExecutorState(m_estate);
        m_estate = NULL;
    }
    m_econtext = NULL;
}

/*
 * @Description: mainly init execution expression state.
 *
 * @param[IN] ssinfo: skew info of this stream.
 * @param[IN] estate: working state for an Execution.
 * @param[IN] isVec: if this is vec stream.
 * @return void
 */
void StreamSkew::init(bool isVec)
{
    QualSkewInfo* qsinfo = NULL;
    QualSkewState* qsstate = NULL;
    ListCell* lc = NULL;

    if (m_ssinfo == NIL)
        return;

    /*
     * We create a estate under the t_thrd.top_mem_cxt of stream thread,
     * we will release it at exec_stream_end.
     */
    MemoryContext cxt = MemoryContextSwitchTo(THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_OPTIMIZER));
    m_estate = CreateExecutorState();
    (void)MemoryContextSwitchTo(m_estate->es_query_cxt);

    if (isVec) {
        m_econtext = CreateExprContext(m_estate);
        ExecAssignVectorForExprEval(m_econtext);
    } else {
        m_econtext = CreateExprContext(m_estate);
    }

    foreach(lc, m_ssinfo) {
        qsinfo = (QualSkewInfo*)lfirst(lc);
        qsstate = (QualSkewState*)palloc0(sizeof(QualSkewState));
        qsstate->skew_stream_type = qsinfo->skew_stream_type;

        if (isVec) {
            qsstate->skew_quals_state = (List*)ExecInitVecExpr((Expr*)(qsinfo->skew_quals), NULL);
        } else {
            qsstate->skew_quals_state = (List*)ExecInitExpr((Expr*)(qsinfo->skew_quals), NULL);
        }

        if (qsstate->skew_quals_state != NIL)
            m_skewQual = lappend(m_skewQual, (void*)qsstate);
    }

    MemoryContextSwitchTo(cxt);
}

/*
 * @Description: check if the input data match skew values,
 *               and choose the suitable stream type for the data.
 *
 * @param[IN] tuple: input data.
 * @return int: stream type.
 */
int StreamSkew::chooseStreamType(TupleTableSlot* tuple)
{
    ListCell* lc = NULL;
    QualSkewState* qsstate = NULL;

    ResetExprContext(m_econtext);
    m_econtext->ecxt_outertuple = tuple;

    qsstate = (QualSkewState*)linitial(m_skewQual);

    foreach(lc, m_skewQual) {
        qsstate = (QualSkewState*)lfirst(lc);
        if (ExecQual(qsstate->skew_quals_state, m_econtext, false)) {
            switch (qsstate->skew_stream_type) {
                case PART_REDISTRIBUTE_PART_BROADCAST:
                case PART_LOCAL_PART_BROADCAST:
                    return STREAM_BROADCAST;
                case PART_REDISTRIBUTE_PART_ROUNDROBIN:
                    return STREAM_ROUNDROBIN;
                case PART_REDISTRIBUTE_PART_LOCAL:
                    return STREAM_LOCAL;
                default:
                    ereport(ERROR,
                        (errmodule(MOD_OPT_SKEW),
                            errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                            errmsg("Invalid skew stream type %d.", qsstate->skew_stream_type)));
            }
        }
    }

    /* If not match, do the original stream. */
    switch (qsstate->skew_stream_type) {
        case PART_REDISTRIBUTE_PART_BROADCAST:
        case PART_REDISTRIBUTE_PART_ROUNDROBIN:
        case PART_REDISTRIBUTE_PART_LOCAL:
            return STREAM_REDISTRIBUTE;
        case PART_LOCAL_PART_BROADCAST:
            return STREAM_LOCAL;
        default:
            ereport(ERROR,
                (errmodule(MOD_OPT_SKEW),
                    errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmsg("Invalid skew stream type %d.", qsstate->skew_stream_type)));
    }

    return -1;
}

/*
 * @Description: check if the input data(vector) match skew values,
 *               and choose the suitable stream type for the data.
 *
 * @param[IN] tuple: input data.
 * @return int: stream type.
 */
void StreamSkew::chooseVecStreamType(VectorBatch* batch, int* skewStream)
{
    int i;
    ListCell* lc = NULL;
    QualSkewState* qsstate = NULL;
    errno_t rc;
    bool select[BatchMaxSize] = {false};

    ResetExprContext(m_econtext);
    m_econtext->ecxt_outerbatch = batch;
    m_econtext->ecxt_scanbatch = batch;

    qsstate = (QualSkewState*)linitial(m_skewQual);

    foreach(lc, m_skewQual) {
        qsstate = (QualSkewState*)lfirst(lc);
        ExecVecQual(qsstate->skew_quals_state, m_econtext, false);

        for (i = 0; i < batch->m_rows; i++) {
            if (batch->m_sel[i]) {
                switch (qsstate->skew_stream_type) {
                    case PART_REDISTRIBUTE_PART_BROADCAST:
                    case PART_LOCAL_PART_BROADCAST:
                        skewStream[i] = STREAM_BROADCAST;
                        break;

                    case PART_REDISTRIBUTE_PART_ROUNDROBIN:
                        skewStream[i] = STREAM_ROUNDROBIN;
                        break;

                    case PART_REDISTRIBUTE_PART_LOCAL:
                        skewStream[i] = STREAM_LOCAL;
                        break;

                    default:
                        ereport(ERROR,
                            (errmodule(MOD_OPT_SKEW),
                                errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                                errmsg("Invalid skew stream type %d.", qsstate->skew_stream_type)));
                }
            }

            select[i] = select[i] || m_econtext->ecxt_scanbatch->m_sel[i];
        }

        rc = memset_s(batch->m_sel, BatchMaxSize * sizeof(bool), 0, BatchMaxSize * sizeof(bool));
        securec_check(rc, "\0", "\0");
    }

    for (i = 0; i < batch->m_rows; i++) {
        if (select[i] == false) {
            switch (qsstate->skew_stream_type) {
                case PART_REDISTRIBUTE_PART_BROADCAST:
                case PART_REDISTRIBUTE_PART_ROUNDROBIN:
                case PART_REDISTRIBUTE_PART_LOCAL:
                    skewStream[i] = STREAM_REDISTRIBUTE;
                    break;

                case PART_LOCAL_PART_BROADCAST:
                    skewStream[i] = STREAM_LOCAL;
                    break;

                default:
                    ereport(ERROR,
                        (errmodule(MOD_OPT_SKEW),
                            errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                            errmsg("Invalid skew stream type %d.", qsstate->skew_stream_type)));
            }
        }
    }
}

/*
 * @Description: Get distribute keys from a plan, especially for skew join.
 *               This function is only used for distinct number estimate.
 *
 *               The skew join has no distribute keys because there is hybrid
 *               stream at one or both sides of join. However, when we try to
 *               estimate local distinct number, we will try to find if the
 *               join's distribute keys equal to base rel's distribute keys,
 *               in this case, if the join plan has no distribute keys, the
 *               estimated local distinct number will be lager the the real number.
 *
 *               Even though there is Hybrid stream under join, the most data
 *               in Hybrid stream is distribute by hash, so we can still try to
 *               get an approximate for local distinct number estimate.
 *
 * @return List*: plan distribute keys
 */
List* find_skew_join_distribute_keys(Plan* plan)
{
    if (!IS_JOIN_PLAN(plan))
        return plan->distributed_keys;

    Join* join = (Join*)plan;
    if (!join->skewoptimize)
        return plan->distributed_keys;

    /*
     * Skew join has no distribute keys, we need to find the keys for
     * distinct value estimate.
     */
    Plan* inner_plan = plan->righttree;
    Plan* outer_plan = plan->lefttree;
    bool is_replicate_inner = is_replicated_plan(inner_plan);
    bool is_replicate_outer = is_replicated_plan(outer_plan);

    if (is_replicate_inner && is_replicate_outer) {
        return NIL;
    } else if (is_replicate_inner || is_replicate_outer) {
        if (is_replicate_outer) {
            return inner_plan->distributed_keys;
        } else {
            return outer_plan->distributed_keys;
        }
    } else {
        if (join->jointype != JOIN_FULL) {
            return locate_distribute_key(
                join->jointype, outer_plan->distributed_keys, inner_plan->distributed_keys, NIL, false);
        }
    }

    return NIL;
}

