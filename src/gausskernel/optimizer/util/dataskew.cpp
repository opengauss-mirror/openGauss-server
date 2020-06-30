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
 * dataskew.cpp
 *	  functions for dataskew solution in MPP
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/optimizer/util/dataskew.cpp
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

/* We only create optimized path only when the skew ratio is large than the limit. */
#define SKEW_RATIO_LIMIT 3.0

/* Too much skew value will cost to much memory and execution time, so we add a limit here. */
#define MAX_SKEW_NUM 10

#define IS_JOIN_OUTER(jointype)                                                        \
    (JOIN_LEFT == (jointype) || JOIN_RIGHT == (jointype) || JOIN_FULL == (jointype) || \
        JOIN_LEFT_ANTI_FULL == (jointype) || JOIN_RIGHT_ANTI_FULL == (jointype))

#define IS_JOIN_PLAN(plan) (IsA(plan, HashJoin) || IsA(plan, NestLoop) || IsA(plan, MergeJoin))

#define IS_JOIN_PATH(path) (IsA(path, HashPath) || IsA(path, NestPath) || IsA(path, MergePath))

extern THR_LOCAL MemoryContext DataSkewContext;

extern Node* coerce_to_target_type(ParseState* pstate, Node* expr, Oid exprtype, Oid targettype, int32 targettypmod,
    CoercionContext ccontext, CoercionForm cformat, int location);

/* ========================== Public functions =========================== */
/*
 * @Description: calculate skew ratio base on the value's proportion.
 *
 * @param[IN] mcv_ratio: common value's proportion.
 * @return double: skew ratio.
 */
inline double cal_skew_ratio(double mcv_ratio, int dop)
{
    /*
     * The skew ratio is related to the comparison between skew node
     * with other node. Except for the skew value, we think the rest
     * data will distribude to all data nodes, which means every node
     * get data proportion: (1 - mcv) / u_sess->pgxc_cxt.NumDataNodes. So the skew node
     * get data proportion: mcv + (1 - mcv) / u_sess->pgxc_cxt.NumDataNodes. Then we can
     * get the skew tatio formula.
     *
     * Formula: skew_ratio = mcv / ((1 - mcv) / NumDataNodes) + 1
     *
     */
    double ratio = 0.0;
    dop = SET_DOP(dop);
    
    if (mcv_ratio > 0) {
        ratio = mcv_ratio / (1 - mcv_ratio) * u_sess->pgxc_cxt.NumDataNodes * dop + 1;
    }

    return ratio;
}

/* ==================== Functions for stream skew info ===================== */
/*
 * @Description: Constructor func.
 *
 * @param[IN] root: planner info for this join.
 * @param[IN] join_clause: join match clause.
 * @param[IN] join_type: join type
 */
SkewInfo::SkewInfo(PlannerInfo* root)
{
    m_root = root;
    m_rel = NULL;
    m_subrel = NULL;
    m_distributeKeys = NIL;
    m_skewKeys = NIL;
    m_dop = 1;
    m_skewType = SKEW_NONE;
    m_isMultiCol = false;
    m_oldContext = NULL;
    m_context = root->glob->plannerContext->dataSkewMemContext;
    Assert(m_context != NULL);

    m_hasStatSkew = false;
    m_hasHintSkew = false;
    m_hasRuleSkew = false;
}

/*
 * @Description: destructor function for skew info.
 */
SkewInfo::~SkewInfo()
{
    m_root = NULL;
    m_rel = NULL;
    m_subrel = NULL;
    m_distributeKeys = NULL;
    m_skewKeys = NULL;
    MemoryContextReset(m_context);
    m_context = NULL;
    m_oldContext = NULL;
}

/* ======================= Functions for base skew ======================== */
/*
 * @Description: find skew values of single distribute key.
 *
 * @return List*: skew value list.
 */
List* SkewInfo::findSingleColSkewValues()
{
    List* skewvalues = NIL;

    /* Firstly try to search skew value from hint. */
    skewvalues = findSingleColSkewValuesFromHint();
    if (skewvalues != NIL) {
        m_hasHintSkew = true;
        printSkewOptimizeDetail("Skew information for single column's"
                                " distribution is found in hint.");
    } else {
        /*
         * For lazy strategy, if the sub relation is not a base relation,
         * then we give up trying to find skew info from statistics.
         */
        if (u_sess->opt_cxt.skew_strategy_opt == SKEW_OPT_LAZY && m_subrel->reloptkind != RELOPT_BASEREL) {
            m_hasStatSkew = false;
            return NIL;
        }

        /* Search statistic info if no related skew value found in hint. */
        skewvalues = findSingleColSkewValuesFromStatistic();
        if (skewvalues != NIL) {
            m_hasStatSkew = true;
            printSkewOptimizeDetail("Skew information for single column's"
                                    " distribution is found in statistic.");
        }
    }

    return skewvalues;
}

/*
 * @Description: find skew values of single distribute key from base rel statistic info.
 *
 * @return List*: skew value list.
 */
List* SkewInfo::findSingleColSkewValuesFromStatistic()
{
    Node* node = NULL;
    List* skewvalues = NIL;

    if (getSkewKeys() == false)
        return NIL;

    /* Check if the distribute keys are base rel column. */
    if (checkDistributekeys(m_skewKeys) == false)
        return NIL;

    node = (Node*)linitial(m_skewKeys);
    skewvalues = getSingleColumnStatistics(node);
    skewvalues = checkRestrict(skewvalues);

    return skewvalues;
}

/*
 * @Description: find skew values of single distribute key from hint info.
 *
 * @return List*: skew value list.
 */
List* SkewInfo::findSingleColSkewValuesFromHint()
{
    HintState* hintState = m_root->parse->hintState;
    SkewHintTransf* skewHint = NULL;
    SkewColumnInfo* colHint = NULL;
    SkewValueInfo* valueHint = NULL;
    List* skewvalues = NIL;
    ListCell* lc1 = NULL;
    ListCell* lc2 = NULL;
    ListCell* lc3 = NULL;
    Node* node = NULL;
    ColSkewInfo* csinfo = NULL;
    int colLoc = 0;
    int valueLoc = 0;
    int colLen = 0;
    int valueLen;

    if (hintState == NULL || hintState->skew_hint == NIL)
        return NIL;

    node = (Node*)linitial(m_distributeKeys);

    /* Find skew info of this rel and col in skew hints. */
    foreach (lc1, hintState->skew_hint) {
        skewHint = (SkewHintTransf*)lfirst(lc1);
        if (list_length(skewHint->column_info_list) == 0)
            continue;

        if (checkSkewRelEqualHintRel(skewHint->rel_info_list)) {
            /*
             * The skew info from hint is construct as:
             * columen info: C1, C2, C3
             * value info: a1, a2, a3, b1, b2, b3....
             * there can be more than one column and one value.
             * From multi column skew hint, we can also find single column
             * skew info:
             * if (C1, C2) is skew, skew value is (a1, a2)
             * then C1 is skew, skew value is a1; C2 is skew, skew value is a2.
             */
            colLoc = 0;
            valueLoc = 0;
            colLen = list_length(skewHint->column_info_list);
            valueLen = list_length(skewHint->value_info_list) / colLen;

            if (getSkewKeys(skewHint->column_info_list, &colLoc, 1) == false)
                continue;

            foreach (lc2, skewHint->column_info_list) {
                colHint = (SkewColumnInfo*)lfirst(lc2);

                /* No skew value is set for skew agg. */
                if (m_skewType == SKEW_AGG || (m_skewType == SKEW_JOIN && valueLen > 0)) {
                    m_hasHintSkew = true;
                    skewHint->before->base.state = HINT_STATE_USED;
                }

                foreach (lc3, skewHint->value_info_list) {
                    if (valueLoc % colLen == colLoc) {
                        valueHint = (SkewValueInfo*)lfirst(lc3);
                        csinfo = (ColSkewInfo*)palloc0(sizeof(ColSkewInfo));
                        csinfo->var = (Node*)linitial(m_skewKeys);
                        csinfo->value = valueHint->const_value;
                        csinfo->is_null = valueHint->const_value->constisnull;
                        /* Use -1 to make this skew info comes from hint. */
                        csinfo->mcv_ratio = -1;
                        skewvalues = lappend(skewvalues, (void*)csinfo);
                    }
                    valueLoc++;
                }
            }
        }
    }

    return skewvalues;
}

/*
 * @Description: find skew values of multi distribute keys.
 *
 * @return List*: skew value list.
 */
List* SkewInfo::findMultiColSkewValues()
{
    List* skewvalues = NIL;

    /* Always try to search the hint infomation at the very beginning. */
    skewvalues = findMultiColSkewValuesFromHint();
    if (skewvalues != NIL) {
        m_hasHintSkew = true;
        printSkewOptimizeDetail("Skew information for multiple columns'"
                                " distribution is found in hint.");
    } else {
        /*
         * For lazy strategy, if the sub relation is not a base relation,
         * then we give up trying to find skew info from statistics.
         */
        if (u_sess->opt_cxt.skew_strategy_opt == SKEW_OPT_LAZY && m_subrel->reloptkind != RELOPT_BASEREL) {
            m_hasStatSkew = false;
            return NIL;
        }

        skewvalues = findMultiColSkewValuesFromStatistic();
        if (skewvalues != NIL) {
            m_hasStatSkew = true;
            printSkewOptimizeDetail("Skew information for multiple columns'"
                                    " distribution is found in statistic.");
        }
    }

    return skewvalues;
}

/*
 * @Description: find skew values of multi distribute keys from hint.
 *
 * @return List*: skew value list.
 */
List* SkewInfo::findMultiColSkewValuesFromHint()
{
    HintState* hintState = m_root->parse->hintState;
    SkewHintTransf* skewHint = NULL;
    SkewValueInfo* valueHint = NULL;
    List* skewvalues = NIL;
    ListCell* lc = NULL;
    MultiColSkewInfo* mcsinfo = NULL;

    if (hintState == NULL || hintState->skew_hint == NIL)
        return NIL;

    int colCount = list_length(m_distributeKeys);
    int i = 0;
    int j = 0;
    int* colLoc = (int*)palloc0(sizeof(int) * list_length(m_distributeKeys));

    foreach (lc, hintState->skew_hint) {
        skewHint = (SkewHintTransf*)lfirst(lc);
        if (!checkSkewRelEqualHintRel(skewHint->rel_info_list))
            continue;

        /*
         * Trasverse the distribute keys and record the locations
         * of each distribute keys in hint.
         */
        if (getSkewKeys(skewHint->column_info_list, colLoc, list_length(m_distributeKeys)) == false)
            continue;

        /*
         * The value of skew hint value info list is construct as:
         * (a0, b0, c0 .. a1, b1, c1 .. a2, b2, c2..)
         * a, b, c .. refer to different column, and 0, 1, 2 ..
         * refer to different tuple.
         */
        int cols = list_length(skewHint->column_info_list);
        /* Check cols. */
        if (cols == 0)
            continue;
        int values = list_length(skewHint->value_info_list) / cols;

        /* No skew value is set for skew agg. */
        if (m_skewType == SKEW_AGG || (m_skewType == SKEW_JOIN && values > 0)) {
            m_hasHintSkew = true;
            skewHint->before->base.state = HINT_STATE_USED;
        }

        for (i = 0; i < values; i++) {
            mcsinfo = (MultiColSkewInfo*)palloc0(sizeof(MultiColSkewInfo));
            mcsinfo->vars = m_skewKeys;
            for (j = 0; j < colCount; j++) {
                valueHint = (SkewValueInfo*)list_nth(skewHint->value_info_list, (i * cols + colLoc[j]));
                mcsinfo->values = lappend(mcsinfo->values, valueHint->const_value);
                mcsinfo->mcv_ratio = -1;
            }
            skewvalues = lappend(skewvalues, (void*)mcsinfo);
        }
    }

    return skewvalues;
}

/*
 * @Description: find skew values of multi distribute keys from base relation statistic.
 *
 * @return List*: skew value list.
 */
List* SkewInfo::findMultiColSkewValuesFromStatistic()
{
    List* skewvalues = NIL;

    if (getSkewKeys() == false)
        return NIL;

    /* Check if the distribute keys are base rel column. */
    if (checkDistributekeys(m_skewKeys) == false)
        return NIL;

    /*
     * Multi-column statistic information is not default provided
     * and need specific designation by user.
     */
    skewvalues = findMultiColSkewValuesFromMultiStatistic();

    /*
     * If we dont have multi-column, try to calculate multi-column statistic
     * from signle-column statistic info.
     */
    if (skewvalues == NIL) {
        skewvalues = findMultiColSkewValuesFromSingleStatistic();
    }

    skewvalues = checkRestrict(skewvalues);
    return skewvalues;
}

/*
 * @Description: find skew values of multi distribute keys
 *               from base relation's multi column statistic.
 *
 * @return List*: skew value list.
 */
List* SkewInfo::findMultiColSkewValuesFromMultiStatistic()
{
    List* multi_skewvalues = NIL;
    List* multistats = NIL;
    ListCell* lc1 = NULL;
    ListCell* lc2 = NULL;
    Var* var = NULL;
    Const* con = NULL;
    ExtendedStats* estats = NULL;
    MultiColSkewInfo* mcsinfo = NULL;
    int keynum;
    int* keyloc_array = NULL;

    /* Get multi colum statistic info. */
    multistats = getMultiColumnStatistics();

    /* Mostly, we dont have multi column statistic information. */
    if (multistats == NIL)
        return NIL;

    keynum = list_length(m_skewKeys);
    keyloc_array = (int*)palloc0(sizeof(int) * keynum);

    foreach (lc1, multistats) {
        int num = 0;
        int x = -1;
        int i = 0;
        int j = 0;
        int* attnum_array = NULL;

        estats = (ExtendedStats*)lfirst(lc1);
        num = bms_num_members(estats->bms_attnum);
        attnum_array = (int*)palloc0(sizeof(int) * num);

        /* get attribute index from multi-column statistics */
        while ((x = bms_next_member(estats->bms_attnum, x)) >= 0)
            attnum_array[i++] = x;

        /* find the location of each distribute keys in multi-column array */
        foreach (lc2, m_skewKeys) {
            var = (Var*)lfirst(lc2);
            for (i = 0; i < num; i++) {
                if (attnum_array[i] == var->varattno) {
                    keyloc_array[j] = i;
                    break;
                }
            }

            /* var is not found */
            if (i == num)
                break;

            j++;
        }

        if (j != keynum)
            continue;

        /* Check null value. */
        if (cal_skew_ratio(estats->nullfrac, m_dop) > SKEW_RATIO_LIMIT) {
            mcsinfo = (MultiColSkewInfo*)palloc0(sizeof(MultiColSkewInfo));
            mcsinfo->vars = m_skewKeys;
            mcsinfo->mcv_ratio = estats->nullfrac;
            mcsinfo->is_null = true;
            multi_skewvalues = lappend(multi_skewvalues, (void*)mcsinfo);
        }

        /* Check other value. */
        for (i = 0; i < estats->mcv_nnumbers; i++) {
            if (cal_skew_ratio(estats->mcv_numbers[i], m_dop) <= SKEW_RATIO_LIMIT)
                break;

            mcsinfo = (MultiColSkewInfo*)palloc0(sizeof(MultiColSkewInfo));
            mcsinfo->vars = m_skewKeys;
            mcsinfo->mcv_ratio = estats->mcv_numbers[i];
            mcsinfo->is_null = false;

            /* extract value of each distribute keys */
            for (j = 0; j < keynum; j++) {
                var = (Var*)list_nth(m_skewKeys, j);
                con = makeConstForSkew(var, estats->mcv_values[i + estats->mcv_nnumbers * keyloc_array[j]]);
                mcsinfo->values = lappend(mcsinfo->values, (void*)con);
            }

            multi_skewvalues = lappend(multi_skewvalues, (void*)mcsinfo);
            if (list_length(multi_skewvalues) >= MAX_SKEW_NUM)
                break;
        }
    }

    return multi_skewvalues;
}

/*
 * @Description: find skew values of multi distribute keys
 *               from base relation's single column statistic.
 *
 * @return List*: skew value list.
 */
List* SkewInfo::findMultiColSkewValuesFromSingleStatistic()
{
    List* skewvalues = NIL;
    List* multi_skewvalues = NIL;
    List* tmp = NIL;
    ListCell* lc1 = NULL;
    ListCell* lc2 = NULL;
    ListCell* lc3 = NULL;
    ColSkewInfo* csinfo = NULL;
    MultiColSkewInfo* mcsinfo = NULL;
    MultiColSkewInfo* new_mcsinfo = NULL;
    Node* node = NULL;
    double mcv_ratio;
    bool is_null = false;

    /*
     * To calculate multi-column skew info from single-column skew info,
     * we need to get skew info of each column first and try to calculate
     * the combination result. Example as:
     * distribute key is : (A, B)
     * 1. get col A's skew values (a1, a2, a3)
     * 2. get col B's skew values (b1, b2)
     * 3. try to combine them as (a1, b1), (a1, b2), (a2, b1), (a2, b2), (a3, b1), (a3, b2).
     * 4. calculate the combination's proportion, if it satisfys skew condition, then keep it.
     */
    foreach (lc1, m_distributeKeys) {
        node = (Node*)lfirst(lc1);
        skewvalues = getSingleColumnStatistics(node);

        /* If any column is not skew, then the multi cols can not be skew. */
        if (skewvalues == NIL) {
            return NIL;
        }

        if (multi_skewvalues == NIL) {
            foreach (lc3, skewvalues) {
                csinfo = (ColSkewInfo*)lfirst(lc3);

                /* Add a new multi skew info. */
                new_mcsinfo = (MultiColSkewInfo*)palloc0(sizeof(MultiColSkewInfo));
                new_mcsinfo->mcv_ratio = csinfo->mcv_ratio;
                new_mcsinfo->vars = m_skewKeys;
                new_mcsinfo->values = lappend(new_mcsinfo->values, (void*)csinfo->value);

                multi_skewvalues = lappend(multi_skewvalues, (void*)new_mcsinfo);
            }
        } else {
            tmp = multi_skewvalues;
            multi_skewvalues = NIL;
            foreach (lc2, tmp) {
                mcsinfo = (MultiColSkewInfo*)lfirst(lc2);
                foreach (lc3, skewvalues) {
                    csinfo = (ColSkewInfo*)lfirst(lc3);

                    if (list_length(multi_skewvalues) >= MAX_SKEW_NUM)
                        break;

                    /*
                     * As we already know p(a1), p(b1), then try to calculate p(a1, b1)
                     * there is to strategy:
                     * 1. normal: p(a1, b1) = min(p(a1), p(b1))
                     * 2. lazy: p(a1, b1) = p(a1) * p(b1)
                     */
                    if (u_sess->opt_cxt.skew_strategy_opt == SKEW_OPT_NORMAL)
                        mcv_ratio = Min(csinfo->mcv_ratio, mcsinfo->mcv_ratio);
                    else
                        mcv_ratio = csinfo->mcv_ratio * mcsinfo->mcv_ratio;

                    if (cal_skew_ratio(mcv_ratio, m_dop) > SKEW_RATIO_LIMIT) {
                        /* Add a new multi skew info. */
                        new_mcsinfo = (MultiColSkewInfo*)palloc0(sizeof(MultiColSkewInfo));
                        new_mcsinfo->mcv_ratio = mcv_ratio;
                        new_mcsinfo->vars = m_skewKeys;
                        new_mcsinfo->values = list_copy(mcsinfo->values);
                        new_mcsinfo->values = lappend(new_mcsinfo->values, (void*)csinfo->value);

                        multi_skewvalues = lappend(multi_skewvalues, (void*)new_mcsinfo);
                    }
                }
            }

            /* Free unuse memory in case memory bloat. */
            list_free_deep(tmp);
        }

        /* Return NIL if any column is not skew. */
        if (multi_skewvalues == NIL)
            return NIL;
    }

    /* If all single value is null, then the nulti col value is null. */
    foreach (lc1, multi_skewvalues) {
        mcsinfo = (MultiColSkewInfo*)lfirst(lc1);

        is_null = true;
        foreach (lc2, mcsinfo->values) {
            is_null = is_null && (NULL == lfirst(lc2));
        }

        if (is_null) {
            mcsinfo->values = NIL;
            mcsinfo->is_null = true;
        }
    }

    return multi_skewvalues;
}

/*
 * @Description: find skew value from mcv(most common value) array.
 *
 * @param[IN] var: column info.
 * @param[IN] mcv_num: the number of most common value.
 * @param[IN] mcv_values: the array of most common value.
 * @param[IN] mcv_ratios: the proportion of the most common value.
 * @return List*: the list of skew values.
 */
List* SkewInfo::findMcvValue(Node* var, int mcv_num, Datum* mcv_values, float4* mcv_ratios) const
{
    int i = 0;
    double ratio = 0;
    List* ret = NIL;

    for (i = 0; i < mcv_num; i++) {
        ratio = cal_skew_ratio((double)mcv_ratios[i], u_sess->opt_cxt.query_dop);
        if (ratio >= SKEW_RATIO_LIMIT) {
            ColSkewInfo* csinfo = (ColSkewInfo*)palloc0(sizeof(ColSkewInfo));
            csinfo->var = var;
            csinfo->value = makeConstForSkew((Var*)var, mcv_values[i]);
            csinfo->mcv_ratio = mcv_ratios[i];
            csinfo->is_null = false;
            ret = lappend(ret, (void*)csinfo);
            if (list_length(ret) >= MAX_SKEW_NUM)
                return ret;
        }
    }

    return ret;
}

/*
 * @Description: make const from skew value.
 *
 * @param[IN] var: column info.
 * @param[IN] value: const value.
 * @return Const*: const structure.
 */
Const* SkewInfo::makeConstForSkew(Var* var, Datum value) const
{
    Const* con = NULL;
    HeapTuple typeTuple = NULL;
    Form_pg_type typeForm;

    /* Get type info. */
    typeTuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(var->vartype));

    /* Check typeTuple. */
    if (typeTuple == NULL)
        return NULL;

    typeForm = (Form_pg_type)GETSTRUCT(typeTuple);

    /* Make const. */
    con = makeConst(var->vartype, var->vartypmod, InvalidOid, typeForm->typlen, value, false, typeForm->typbyval);

    ReleaseSysCache(typeTuple);
    return con;
}

/*
 * @Description: see the statistic table and get most common values info,
 *               then calculate the value proportion to find the skew value.
 *
 * @param[IN] root: planner basic info.
 * @param[IN] node: column.
 * @return List*: the list of skew values.
 */
List* SkewInfo::getSingleColumnStatistics(Node* node)
{
    Form_pg_statistic stats = NULL;
    VariableStatData vardata;
    bool have_mcvs = false;
    Datum* values = NULL;
    float4* numbers = NULL;
    int nvalues = 0;
    int nnumbers = 0;
    List* ret = NIL;

    examine_variable(m_root, node, 0, &vardata);

    if (HeapTupleIsValid(vardata.statsTuple)) {
        stats = (Form_pg_statistic)GETSTRUCT(vardata.statsTuple);

        /* find mcv values */
        have_mcvs = get_attstatsslot(vardata.statsTuple,
            vardata.atttype,
            vardata.atttypmod,
            STATISTIC_KIND_MCV,
            InvalidOid,
            NULL,
            &values,
            &nvalues,
            &numbers,
            &nnumbers);

        ret = findMcvValue(vardata.var, nvalues, values, numbers);

        /* find null fraction */
        if (cal_skew_ratio(stats->stanullfrac, u_sess->opt_cxt.query_dop) > SKEW_RATIO_LIMIT) {
            ColSkewInfo* csinfo = (ColSkewInfo*)palloc0(sizeof(ColSkewInfo));
            csinfo->var = vardata.var;
            csinfo->value = NULL;
            csinfo->mcv_ratio = stats->stanullfrac;
            csinfo->is_null = true;
            ret = lappend(ret, (void*)csinfo);
        }
    }

    ReleaseVariableStats(vardata);
    return ret;
}

/*
 * @Description: get multi-column statistic info.
 *
 * @return List*: multi-column statistic info list.
 */
List* SkewInfo::getMultiColumnStatistics()
{
    List* multistats = NIL;
    RelOptInfo* rel = NULL;
    RangeTblEntry* rte = NULL;
    Var* var = NULL;
    char starelkind;
    int numstats;

    var = (Var*)linitial(m_skewKeys);
    rel = find_base_rel(m_root, var->varno);
    if (rel == NULL)
        return NIL;

    rte = planner_rt_fetch(rel->relid, m_root);

    /* Statistic information is only available for base rel. */
    if (rte == NULL || rte->rtekind != RTE_RELATION)
        return NIL;

    /* Get multi colum statistic info. */
    starelkind = OidIsValid(rte->partitionOid) ? STARELKIND_PARTITION : STARELKIND_CLASS;
    multistats = es_get_multi_column_stats(rte->relid, starelkind, rte->inh, &numstats);

    return multistats;
}

/* ===================== Functions for restrict check ====================== */
/*
 * @Description: check if the skew value can pass restrictions.
 *
 * @param[IN] baseSkewList: skew values list from base rel.
 * @return List*: skew values which pass the restriction.
 */
List* SkewInfo::checkRestrict(List* baseSkewList)
{
    List* result_list = NIL;

    if (baseSkewList == NIL)
        return NIL;

    if (m_isMultiCol)
        result_list = checkRestrictForMultiCol(baseSkewList);
    else
        result_list = checkRestrictForSingleCol(baseSkewList);

    if (result_list == NIL)
        printSkewOptimizeDetail("Skew values is filtered by restrictions.");

    return result_list;
}

/*
 * @Description: check restrictions for single distribute keys.
 *
 * @param[IN] baseSkewList: single column skew values list from base rel.
 * @return List*: skew values which pass the restriction.
 */
List* SkewInfo::checkRestrictForSingleCol(List* baseSkewList)
{
    List* base_clauses = NIL;
    List* distributekeys = NIL;
    List* skew_list = NIL;
    ListCell* lc1 = NULL;
    ListCell* lc2 = NULL;
    ColSkewInfo* csinfo = NULL;
    RelOptInfo* rel = NULL;
    Var* var = NULL;
    Node* node = NULL;

    skew_list = baseSkewList;
    lc1 = skew_list->head;

    while (lc1 != NULL) {
        csinfo = (ColSkewInfo*)lfirst(lc1);
        lc1 = lc1->next;

        var = (Var*)csinfo->var;
        rel = find_base_rel(m_root, var->varno);

        /* No restriction for this rel. */
        if (rel->baserestrictinfo == NIL)
            continue;

        /* No need to check restriction if the skew info comes from hint. */
        if (csinfo->mcv_ratio == -1)
            continue;

        /* Check restrict about this col. */
        base_clauses = extract_actual_clauses(rel->baserestrictinfo, false);

        if (distributekeys == NIL)
            distributekeys = lappend(distributekeys, var);

        foreach (lc2, base_clauses) {
            node = (Node*)lfirst(lc2);
            if (!isColInRestrict(distributekeys, node)) {
                /*
                 * When other col has qual, we think this col does not
                 * have skew when the strategy is conservative.
                 */
                if (u_sess->opt_cxt.skew_strategy_opt == SKEW_OPT_LAZY) {
                    skew_list = list_delete_ptr(skew_list, (void*)csinfo);
                }
                continue;
            }

            if (csinfo->is_null) {
                /*
                 * Delete skew info if this value is null and
                 * we have restrict like 'is not null'.
                 */
                if (IsA(node, NullTest)) {
                    NullTest* ntest = (NullTest*)node;
                    if (ntest->nulltesttype == IS_NOT_NULL)
                        skew_list = list_delete_ptr(skew_list, (void*)csinfo);
                } else {
                    skew_list = list_delete_ptr(skew_list, (void*)csinfo);
                }
            } else {
                /* Delete skew info if this value can not pass qual. */
                if (!canValuePassQual(list_make1(var), list_make1(csinfo->value), (Expr*)node)) {
                    skew_list = list_delete_ptr(skew_list, (void*)csinfo);
                    break;
                }
            }
        }
    }

    return skew_list;
}

/*
 * @Description: check restrictions for multi distribute keys.
 *
 * @param[IN] baseSkewList: multi column skew values list from base rel.
 * @return List*: skew values which pass the restriction.
 */
List* SkewInfo::checkRestrictForMultiCol(List* baseSkewList)
{
    List* base_clauses = NIL;
    List* skew_list = NIL;
    ListCell* lc1 = NULL;
    ListCell* lc2 = NULL;
    MultiColSkewInfo* mcsinfo = NULL;
    RelOptInfo* rel = NULL;
    Var* var = NULL;
    Node* node = NULL;

    skew_list = baseSkewList;
    lc1 = skew_list->head;

    while (lc1 != NULL) {
        mcsinfo = (MultiColSkewInfo*)lfirst(lc1);
        lc1 = lc1->next;

        /*
         * If any attribute value dose not pass the restrict,
         * then this multi column skew value should be delete.
         */
        var = (Var*)linitial(mcsinfo->vars);
        rel = find_base_rel(m_root, var->varno);

        /* No restriction for this rel. */
        if (rel->baserestrictinfo == NIL)
            continue;

        /* No need to check restriction if the skew info comes from hint. */
        if (mcsinfo->mcv_ratio == -1)
            continue;

        base_clauses = extract_actual_clauses(rel->baserestrictinfo, false);
        if (base_clauses == NIL)
            continue;

        foreach (lc2, base_clauses) {
            node = (Node*)lfirst(lc2);
            if (isColInRestrict(mcsinfo->vars, node) == false) {
                /*
                 * No ralate var is found for this qual, so when we choose
                 * lazy strategy, we think that skew value is not exits anymore.
                 */
                if (u_sess->opt_cxt.skew_strategy_opt == SKEW_OPT_LAZY) {
                    skew_list = list_delete_ptr(skew_list, (void*)mcsinfo);
                }
                continue;
            }

            /* Check for null test. */
            if (mcsinfo->is_null) {
                if (IsA(node, NullTest)) {
                    NullTest* ntest = (NullTest*)node;
                    if (ntest->nulltesttype == IS_NOT_NULL)
                        skew_list = list_delete_ptr(skew_list, (void*)mcsinfo);
                } else {
                    skew_list = list_delete_ptr(skew_list, (void*)mcsinfo);
                }
            } else {
                /* Delete skew info if this value can not pass qual. */
                if (!canValuePassQual(mcsinfo->vars, mcsinfo->values, (Expr*)node)) {
                    skew_list = list_delete_ptr(skew_list, (void*)mcsinfo);
                    break;
                }
            }
        }
    }

    return skew_list;
}

/*
 * @Description: check if current relation equal to relation in hint.
 *
 * @param[IN] sinfo: stream info.
 * @return void
 */
bool SkewInfo::checkSkewRelEqualHintRel(List* skewRelHint)
{
    if (list_length(skewRelHint) == 0)
        return false;

    RangeTblEntry* rte = NULL;
    SkewRelInfo* srinfo = NULL;

    switch (m_subrel->reloptkind) {
        case RELOPT_BASEREL: {
            if (list_length(skewRelHint) != 1)
                return false;

            rte = planner_rt_fetch(m_subrel->relid, m_root);
            srinfo = (SkewRelInfo*)linitial(skewRelHint);
            if (compareRelByRTE(rte, srinfo->rte))
                return true;

            break;
        }
        case RELOPT_JOINREL:
        case RELOPT_OTHER_MEMBER_REL: {
            Relids relids = generateRelidsFromRelList(skewRelHint);

            /*
             * When hint indicated one or several rels are the subset of
             * current join rel, then the join rel is skew too.
             */
            if (bms_is_subset(relids, m_subrel->relids))
                return true;
            break;
        }
        default:
            break;
    }

    return false;
}

/*
 * @Description: check if the hint column equal distribute nodes.
 *
 * @param[IN] node: distribute node.
 * @param[IN] colHint: column hint info.
 * @return bool: true -- the hint column equal distribute key.
 */
bool SkewInfo::checkEqualKey(Node* node, SkewColumnInfo* colHint) const
{
    bool ret = false;

    if (IsA(node, Var)) {
        Var* var = (Var*)node;
        if (colHint->attnum == var->varattno)
            ret = true;
    } else if (equal(node, colHint->expr)) {
        ret = true;
    }

    return ret;
}

/*
 * @Description: get skew keys from distribute keys. When there is a type tranfer
 *               in the distribute key, then we need extract the real skew key
 *               to check if it is existed in skew info.
 *
 * @param[IN] skewHints: column skew list.
 * @param[IN] colLoc: array to record the location for each distribute key.
 * @param[IN] colLen: the length of location array.
 * @return bool : true -- we can find all skew key in column hints.
 */
bool SkewInfo::getSkewKeys(List* skewHints, int* colLoc, int colLen)
{
    ListCell* lc1 = NULL;
    ListCell* lc2 = NULL;
    Node* node = NULL;
    Node* tmpnode = NULL;
    int loc = 0;
    int cnt = 0;

    m_skewKeys = NIL;

    if (m_distributeKeys == NIL)
        return false;

    if (skewHints == NULL || colLoc == NULL || colLen <= 0)
        return false;

    foreach (lc1, m_distributeKeys) {
        node = (Node*)lfirst(lc1);
        loc = 0;

        foreach (lc2, skewHints) {
            SkewColumnInfo* colHint = (SkewColumnInfo*)lfirst(lc2);
            tmpnode = node;

            while (tmpnode != NULL) {
                if (checkEqualKey(tmpnode, colHint)) {
                    break;
                } else if (IsA(tmpnode, FuncExpr)) { /* We also support simple data type tranform. */
                    FuncExpr* func = (FuncExpr*)tmpnode;
                    if ((func->funcformat == COERCE_IMPLICIT_CAST || func->funcformat == COERCE_EXPLICIT_CAST) &&
                        list_length(func->args) == 1) {
                        tmpnode = (Node*)linitial(func->args);
                        continue;
                    }
                } else if (IsA(tmpnode, RelabelType)) {
                    RelabelType* rbl = (RelabelType*)tmpnode;
                    if (rbl->relabelformat == COERCE_IMPLICIT_CAST || rbl->relabelformat == COERCE_EXPLICIT_CAST) {
                        tmpnode = (Node*)rbl->arg;
                        continue;
                    }
                }

                tmpnode = NULL;
            }

            if (tmpnode != NULL) {
                m_skewKeys = lappend(m_skewKeys, tmpnode);
                Assert(cnt < colLen);
                colLoc[cnt] = loc;
                break;
            }
            loc++;
        }
        cnt++;
    }

    /* If we can find all distribute keys, set null. */
    if (list_length(m_skewKeys) != list_length(m_distributeKeys)) {
        m_skewKeys = NIL;
        return false;
    }

    return true;
}

/*
 * @Description: get base var from distribute keys if there is type transfe,
 *               olny then we can try to find skew value from statistic.
 *
 * @return bool: true -- we can find base rel column in the distribute key.
 */
bool SkewInfo::getSkewKeys()
{
    ListCell* lc = NULL;
    Node* node = NULL;

    m_skewKeys = NIL;
    if (m_distributeKeys == NIL)
        return false;

    foreach (lc, m_distributeKeys) {
        node = (Node*)lfirst(lc);

        while (node != NULL) {
            if (IsA(node, Var)) {
                break;
            } else if (IsA(node, FuncExpr)) {
                FuncExpr* func = (FuncExpr*)node;
                if ((func->funcformat == COERCE_IMPLICIT_CAST || func->funcformat == COERCE_EXPLICIT_CAST) &&
                    list_length(func->args) == 1) {
                    node = (Node*)linitial(func->args);
                    continue;
                }
            } else if (IsA(node, RelabelType)) {
                RelabelType* rbl = (RelabelType*)node;
                if (rbl->relabelformat == COERCE_IMPLICIT_CAST || rbl->relabelformat == COERCE_EXPLICIT_CAST) {
                    node = (Node*)rbl->arg;
                    continue;
                }
            }

            node = NULL;
        }

        if (node != NULL)
            m_skewKeys = lappend(m_skewKeys, node);
    }

    /* If we can find all distribute keys, set null. */
    if (list_length(m_skewKeys) != list_length(m_distributeKeys)) {
        m_skewKeys = NIL;
        return false;
    }

    return true;
}

/*
 * @Description: calculate the real skew value. When we get a skew value from
 *               statistic or hint, maybe we need to do type transfer to match
 *               distribute key.
 *
 * @param[IN] diskey: the distribute key
 * @param[IN] skewkey: the skew key
 * @param[IN] value: the skew value
 * @return Const: the skew value after transfer.
 */
Const* SkewInfo::getRealSkewValue(Node* diskey, Node* skewkey, Const* value)
{
    if (diskey == NULL || skewkey == NULL || value == NULL)
        return NULL;

    if (equal(diskey, skewkey))
        return value;

    Node* newnode = (Node*)copyObject(diskey);
    Node* tmpnode = newnode;

    while (tmpnode != NULL) {
        if (IsA(tmpnode, FuncExpr)) {
            FuncExpr* func = (FuncExpr*)tmpnode;
            tmpnode = (Node*)linitial(func->args);
            if (equal(tmpnode, skewkey)) {
                linitial(func->args) = value;
                break;
            }
        } else if (IsA(tmpnode, RelabelType)) {
            RelabelType* rbl = (RelabelType*)tmpnode;
            tmpnode = (Node*)rbl->arg;
            if (equal(tmpnode, skewkey)) {
                rbl->arg = (Expr*)value;
                break;
            }
        } else {
            tmpnode = NULL;
        }
    }

    Node* con = eval_const_expressions(m_root, newnode);
    pfree_ext(newnode);

    if (con == NULL || !IsA(con, Const))
        ereport(ERROR,
            (errmodule(MOD_OPT_SKEW),
                errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                errmsg("Can not get valid skew value from hint.")));

    return (Const*)con;
}

/*
 * @Description: preprocess the skew value to match distirbute key's type
 *
 * @param[IN] skewlist: skew info list
 */
void SkewInfo::processSkewValue(List* skewlist)
{
    ListCell* lc = NULL;

    if (skewlist == NIL)
        return;

    if (m_isMultiCol) {
        foreach (lc, skewlist) {
            MultiColSkewInfo* mcsinfo = (MultiColSkewInfo*)lfirst(lc);
            int len = list_length(m_distributeKeys);

            if (mcsinfo->is_null)
                continue;

            for (int i = 0; i < len; i++) {
                Node* diskey = (Node*)list_nth(m_distributeKeys, i);
                Node* skewkey = (Node*)list_nth(mcsinfo->vars, i);
                Const* value = (Const*)list_nth(mcsinfo->values, i);
                ListCell* lcv = list_nth_cell(mcsinfo->values, i);
                lfirst(lcv) = getRealSkewValue(diskey, skewkey, value);
            }
        }
    } else {
        foreach (lc, skewlist) {
            ColSkewInfo* csinfo = (ColSkewInfo*)lfirst(lc);
            Node* diskey = (Node*)linitial(m_distributeKeys);

            if (csinfo->is_null)
                continue;
            csinfo->value = getRealSkewValue(diskey, csinfo->var, csinfo->value);
        }
    }
}

/*
 * @Description: check if all distribute keys are simple var and they come from the same rel.
 *
 * @param[IN] distributekeys: distribute keys' list
 * @return bool
 */
bool SkewInfo::checkDistributekeys(List* distributekeys)
{
    ListCell* lc = NULL;
    Node* node = NULL;
    Index relid = InvalidOid;
    Var* var = NULL;
    RelOptInfo* rel = NULL;
    RangeTblEntry* rte = NULL;

    if (distributekeys == NIL)
        return false;

    foreach (lc, distributekeys) {
        /*
         * Now, we can not find skew values when distribute keys is not regular var type.
         */
        node = (Node*)lfirst(lc);
        if (!IsA(node, Var)) {
            printSkewOptimizeDetail("The distribute keys are not simple relation's column,"
                                    " we can not detect skew value from relation's statistic");
            return false;
        } else {
            var = (Var*)node;
            rel = find_base_rel(m_root, var->varno);
            if (rel == NULL)
                return false;
            rte = planner_rt_fetch(rel->relid, m_root);
            /* Statistic information is only available for base relation. */
            if (rte == NULL || rte->rtekind != RTE_RELATION) {
                printSkewOptimizeDetail("The distribute keys do not come from base rel,"
                                        " we can not detect skew value from relation's statistic");
                return false;
            }

            /*
             * If the distribute keys come from different relation,
             * we are not able to use statistics.
             */
            if (relid == InvalidOid)
                relid = var->varno;
            /* Find var from different rel, return now. */
            if (relid != var->varno) {
                printSkewOptimizeDetail("The distribute keys come from different base rels,"
                                        " we can not detect skew value from relation's statistic");
                return false;
            }
        }
    }

    return true;
}

/*
 * @Description: check if one columen is included in restrict expression.
 *
 * @param[IN] var: column info.
 * @param[IN] clause: restrict expression.
 * @return bool: true -- this colunm is in the expression.
 */
bool SkewInfo::isColInRestrict(List* distributekeys, Node* clause) const
{
    List* varList = NIL;
    ListCell* lc1 = NULL;
    ListCell* lc2 = NULL;
    Var* var1 = NULL;
    Var* var2 = NULL;
    bool ret = false;

    /* Get all related vars from restrict clause. */
    varList = pull_var_clause(clause, PVC_RECURSE_AGGREGATES, PVC_RECURSE_PLACEHOLDERS);
    if (list_length(varList) == 0 || list_length(distributekeys) == 0)
        return false;

    /*
     * When multi columns are included in the clause, and the vars in clause
     * must be a subset of vars in distribute keys. For example:
     *
     * 1. Distribute key is (a, b, c), skew value is (0, 5, 20), and clause
     * is a + b > 10, then the skew value can not pass the clause.
     * 2. Distribute key is (a, b), skew value is (0, 5), and clause is
     * a + b + c > 10, then we are not able to confirm if this skew value will
     * pass the clause.
     */
    if (list_length(distributekeys) < list_length(varList)) {
        list_free(varList);
        return false;
    }

    /* Check if all vars of clause can be found in distribute keys */
    ret = true;
    foreach (lc1, varList) {
        var1 = (Var*)lfirst(lc1);

        foreach (lc2, distributekeys) {
            var2 = (Var*)lfirst(lc2);

            if (_equalSimpleVar(var1, var2))
                break;
        }
        if (lc2 == NULL) {
            ret = false;
            break;
        }
    }

    list_free(varList);
    return ret;
}

/*
 * @Description: check if one value can pass the restrict expression.
 *
 * @param[IN] var: column info.
 * @param[IN] value: skew value to be checked.
 * @param[IN] clause: restrict expression.
 * @return bool: true -- this value can pass the restriction.
 */
bool SkewInfo::canValuePassQual(List* varList, List* valueList, Expr* expr)
{
    RelOptInfo* rel = NULL;
    RangeTblEntry* rte = NULL;
    Relation heaprel;
    TupleDesc tupdesc;
    TupleTableSlot* slot = NULL;
    Var* var = (Var*)linitial(varList);
    Const* con = NULL;
    bool canpass = false;

    /*
     * We can not check if the skew value can pass the qual with param,
     * so we trend to think it can not pass at lazy strategy.
     */
    if (check_param_expr((Node*)expr)) {
        if (u_sess->opt_cxt.skew_strategy_opt == SKEW_OPT_LAZY)
            return false;
        else
            return true;
    }

    /* Make sure any opfuncids are filled in. */
    fix_opfuncids((Node*)expr);

    /* Init a tuple for qual. */
    rel = find_base_rel(m_root, var->varno);
    rte = planner_rt_fetch(rel->relid, m_root);
    heaprel = heap_open(rte->relid, NoLock);
    tupdesc = RelationGetDescr(heaprel);
    slot = MakeSingleTupleTableSlot(tupdesc);
    slot->tts_nvalid = tupdesc->natts;
    heap_close(heaprel, NoLock);

    /* Set slot value. */
    for (int i = 0; i < tupdesc->natts; i++) {
        for (int j = 0; j < list_length(varList); j++) {
            var = (Var*)list_nth(varList, j);
            con = (Const*)list_nth(valueList, j);

            if (tupdesc->attrs[i]->attnum == var->varattno) {
                if (con == NULL) {
                    slot->tts_isnull[i] = true;
                } else {
                    slot->tts_values[i] = con->constvalue;
                    slot->tts_isnull[i] = con->constisnull;
                }

                break;
            }
        }
    }

    canpass = ExecSkewvalExpr(expr, slot);

    /* clear memory */
    ExecDropSingleTupleTableSlot(slot);

    return canpass;
}

/* ======================= Functions to add skew info ======================== */
/*
 * @Description: create equal compare expression for skew values.
 *
 * @param[IN] var: column info.
 * @param[IN] con: skew value.
 * @return OpExpr*: equal operaton expression.
 */
OpExpr* SkewInfo::createEqualExprForSkew(Node* expr, Const* con) const
{
    OpExpr* op = NULL;
    TypeCacheEntry* typentry = NULL;
    Oid collationId = InvalidOid;
    Oid type = exprType(expr);
    Expr* expr1 = (Expr*)expr;
    Expr* expr2 = (Expr*)con;

    if (expr == NULL || con == NULL)
        return NULL;

    if (!is_compatible_type(type, con->consttype)) {
        expr2 = (Expr*)coerce_to_target_type(
            NULL, (Node*)con, con->consttype, type, -1, COERCION_IMPLICIT, COERCE_IMPLICIT_CAST, -1);
    }

    if (expr1 == NULL || expr2 == NULL)
        ereport(ERROR,
            (errmodule(MOD_OPT_SKEW),
                errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                errmsg("Can not generate equal operation for non skew side.")));

    typentry = lookup_type_cache(type, TYPECACHE_EQ_OPR);
    op = (OpExpr*)make_opclause(typentry->eq_opr, BOOLOID, false, expr1, expr2, InvalidOid, collationId);
    set_opfuncid(op);

    return op;
}

/*
 * @Description: execute the equal expression actually.
 *
 * @param[IN] expr: expression to execute.
 * @param[IN] slot: tuple slot.
 * @return bool: if skew value can equal, then return true.
 */
bool SkewInfo::ExecSkewvalExpr(Expr* expr, TupleTableSlot* slot)
{
    ExprState* exprstate = NULL;
    ExprContext* econtext = NULL;
    bool canpass = false;
    bool isNull = false;

    exprstate = ExecInitExpr(expr, NULL);
    econtext = makeNode(ExprContext);
    econtext->ecxt_per_query_memory = m_context;
    econtext->ecxt_per_tuple_memory = m_context;
    econtext->ecxt_scantuple = slot;

    canpass = ExecEvalExpr(exprstate, econtext, &isNull, NULL);

    return canpass;
}

/*
 * @Description: check if skew value can equal for single col.
 *
 * @param[IN] cs_inner: ColSkewInfo of inner.
 * @param[IN] cs_outer: ColSkewInfo of outer.
 * @return bool: if skew value can equal, then return true.
 */
bool SkewInfo::isSingleColSkewValueEqual(ColSkewInfo* cs_inner, ColSkewInfo* cs_outer)
{
    bool can_equal = false;

    if (equal(cs_inner->value, cs_outer->value))
        can_equal = true;
    /* Actually execute expr on skew value. */
    else {
        OpExpr* op = createEqualExprForSkew((Node*)cs_inner->value, cs_outer->value);

        if (ExecSkewvalExpr((Expr*)op, NULL))
            can_equal = true;
    }

    return can_equal;
}

/*
 * @Description: check if skew values can equal for multi col.
 *
 * @param[IN] mcs_inner: MultiColSkewInfo of inner.
 * @param[IN] mcs_outer: MultiColSkewInfo of outer.
 * @return bool: if all skew values can equal, then return true.
 */
bool SkewInfo::isMultiColSkewValueEqual(MultiColSkewInfo* mcs_inner, MultiColSkewInfo* mcs_outer)
{
    bool can_equal = true;
    Const* con1 = NULL;
    Const* con2 = NULL;
    int len = list_length(mcs_inner->values);

    for (int i = 0; i < len; i++) {
        con1 = (Const*)list_nth(mcs_inner->values, i);
        con2 = (Const*)list_nth(mcs_outer->values, i);

        can_equal = can_equal && equal(con1, con2);

        /* Actually execute expr on skew value. */
        if (!can_equal) {
            /* If con is NULL means the skew value is NULL. */
            if (con1 == NULL || con2 == NULL)
                return false;

            OpExpr* op = createEqualExprForSkew((Node*)con1, con2);

            if (ExecSkewvalExpr((Expr*)op, NULL))
                can_equal = true;
            else
                return false;
        }
    }

    return can_equal;
}

/*
 * @Description: get join clause list from join path.
 *
 * @param[IN] path: join path.
 * @return List*: join clause list.
 */
List* SkewInfo::getJoinClause(Path* jpath) const
{
    if (!IS_JOIN_PATH(jpath))
        return NIL;

    if (T_HashPath == jpath->pathtype) {
        HashPath* hpath = (HashPath*)jpath;
        return hpath->path_hashclauses;
    } else if (T_MergePath == jpath->pathtype) {
        MergePath* mpath = (MergePath*)jpath;
        return mpath->path_mergeclauses;
    } else {
        NestPath* npath = (NestPath*)jpath;
        return npath->joinrestrictinfo;
    }
}

/*
 * @Description: find null skew column in outer join.
 *
 * @param[IN] target_list: target list of outer join.
 * @param[IN] subtarget_list: target list of sub path
 * @param[IN] join_clauses: join clauses
 * @return List*: null skew column.
 */
List* SkewInfo::findNullCols(List* target_list, List* subtarget_list, List* join_clauses) const
{
    List* null_target_list = NIL;
    ListCell* lc = NULL;
    Node* node = NULL;

    /*
     * The null target list of outer join should be in the target list of null side.
     */
    foreach (lc, target_list) {
        node = (Node*)lfirst(lc);
        if (find_node_in_targetlist(node, subtarget_list) >= 0) {
            null_target_list = lappend(null_target_list, (void*)node);
        }
    }

    return null_target_list;
}

/*
 * @Description: update multi coulumn's skewness value base on mcv.
 *
 * @param[IN] sinfo: stream info.
 * @return void
 */
void SkewInfo::updateMultiColSkewness(StreamInfo* sinfo, List* skew_list) const
{
    if (skew_list == NIL)
        return;

    MultiColSkewInfo* mcsinfo = NULL;

    mcsinfo = (MultiColSkewInfo*)linitial(skew_list);
    if (mcsinfo->mcv_ratio > 0)
        sinfo->multiple = Max(sinfo->multiple, mcsinfo->mcv_ratio * u_sess->pgxc_cxt.NumDataNodes);
}

/*
 * @Description: generate a relid bitmap from skew rel list.
 *
 * @param[IN] skewRelHint: skew rel list from hint.
 * @return Relids
 */
Relids SkewInfo::generateRelidsFromRelList(List* skewRelHint)
{
    ListCell* lc = NULL;
    Relids relids = NULL;
    SkewRelInfo* srinfo = NULL;
    RelOptInfo* rel = NULL;

    foreach (lc, skewRelHint) {
        srinfo = (SkewRelInfo*)lfirst(lc);

        /* Traverse the rte array to find the same rel. */
        for (int i = 0; i < m_root->simple_rel_array_size; i++) {
            if (compareRelByRTE(srinfo->rte, m_root->simple_rte_array[i])) {
                rel = m_root->simple_rel_array[i];

                /* Check rel. */
                if (rel == NULL)
                    return NULL;

                relids = bms_union(relids, rel->relids);
                break;
            }
        }
    }

    return relids;
}

/*
 * @Description: compare two RTEs to check if they refers to the same base rel or subquery.
 *
 * @param[IN] rte1: RangeTblEntry 1.
 * @param[IN] rte2: RangeTblEntry 2.
 * @return bool
 */
bool SkewInfo::compareRelByRTE(RangeTblEntry* rte1, RangeTblEntry* rte2) const
{
    if (rte1 == NULL || rte2 == NULL)
        return false;

    if (rte1 == rte2)
        return true;

    if (nodeTag(rte1) != nodeTag(rte2))
        return false;

    if (rte1->rtekind != rte2->rtekind)
        return false;

    if (rte1->rtekind == RTE_RELATION) {
        if (rte1->relid == rte2->relid)
            return true;
        else
            return false;
    }

    if (rte1->rtekind == RTE_SUBQUERY) {
        if (rte1->subquery == rte2->subquery)
            return true;
        else
            return false;
    }

    // how about CTE.
    return false;
}

/*
 * @Description: check if we need to broadcast the skew value at the other side.
 *
 * @param[IN] csinfo: sigle col skew info
 * @return bool
 */
bool SkewInfo::needPartBroadcast(ColSkewInfo* csinfo) const
{
    if (csinfo == NULL)
        return false;

    if (csinfo->is_null || csinfo->value == NULL || csinfo->value->constisnull)
        return false;

    return true;
}

/*
 * @Description: check if we need to broadcast the skew value at the other side.
 *
 * @param[IN] mcsinfo: multi col skew info
 * @return bool
 */
bool SkewInfo::needPartBroadcast(MultiColSkewInfo* mcsinfo) const
{
    if (mcsinfo == NULL)
        return false;

    if (list_length(mcsinfo->values) == 0 || mcsinfo->is_null)
        return false;

    ListCell* lc = NULL;
    foreach (lc, mcsinfo->values) {
        Const* con = (Const*)lfirst(lc);
        if (con == NULL || con->constisnull)
            return false;
    }

    return true;
}

/*
 * @Description: get hint skew info.
 *
 * @return int: indicate the skew optimization source.
 */
uint32 SkewInfo::getSkewInfo() const
{
    uint32 ret = SKEW_RES_NONE;

    if (m_hasStatSkew)
        ret |= SKEW_RES_STAT;
    if (m_hasRuleSkew)
        ret |= SKEW_RES_RULE;
    if (m_hasHintSkew)
        ret |= SKEW_RES_HINT;

    return ret;
}

/*
 * @Description: Print detail info of skew oprimization info.
 *
 * @param[IN] msg: skew info message.
 * @return bool
 */
void SkewInfo::printSkewOptimizeDetail(const char* msg)
{
    StringInfoData buf;
    initStringInfo(&buf);

    char* joinRels = NULL;

    if (m_rel != NULL && m_root != NULL)
        joinRels = debug1_print_relids(m_rel->relids, m_root->parse->rtable);

    appendStringInfo(&buf, "[SkewJoin] %s\nJoinRel (%s)", msg, joinRels);
    ereport(DEBUG1, (errmodule(MOD_OPT_SKEW), (errmsg("%s", buf.data))));
}

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
        foreach (lc, skewInfo) {
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

    foreach (lc, ssinfo) {
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

    foreach (lc1, ssinfo) {
        mcsinfo = (MultiColSkewInfo*)lfirst(lc1);
        NullTest* nulltest = NULL;
        List* and_quals = NIL;
        if (mcsinfo->is_null) {
            foreach (lc2, mcsinfo->vars) {
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

    foreach (lc, ssinfo) {
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

    foreach (lc1, ssinfo) {
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

            foreach (lc, apath->subpaths) {
                subpath = (Path*)lfirst(lc);
                traverseSubPath(subpath);
            }
        } break;
        case T_MergeAppendPath: {
            MergeAppendPath* mpath = (MergeAppendPath*)path;
            ListCell* lc = NULL;
            Path* subpath = NULL;

            foreach (lc, mpath->subpaths) {
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

    foreach (lc, m_distributeKeys) {
        node = (Node*)lfirst(lc);
        if (find_node_in_targetlist(node, null_list) >= 0) {
            skew_cols = lappend(skew_cols, (void*)node);
        }
    }

    /* Null skew occurs only when all the distribute key in in null cols. */
    if (skew_cols != NIL && list_length(m_distributeKeys) == list_length(skew_cols)) {
        qsinfo = makeNode(QualSkewInfo);
        qsinfo->skew_stream_type = PART_REDISTRIBUTE_PART_LOCAL;

        foreach (lc, skew_cols) {
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

    foreach (lc, qualList) {
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

    foreach (lc, skewInfo) {
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
                    foreach (lc, spath->skew_list) {
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

    foreach (lc, skewList) {
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

    foreach (lc, m_joinClause) {
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
        (void)findMultiColSkewValuesFromHint();
    } else {
        /* When distribute key only has one column. */
        (void)findSingleColSkewValuesFromHint();
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

            foreach (lc, aplan->appendplans) {
                subplan = (Plan*)lfirst(lc);
                traverseSubPlan(subplan);
            }
        } break;
        case T_MergeAppend: {
            MergeAppend* mplan = (MergeAppend*)plan;
            ListCell* lc = NULL;
            Plan* subplan = NULL;

            foreach (lc, mplan->mergeplans) {
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
    foreach (lc, target_list) {
        node = (Node*)((TargetEntry*)lfirst(lc))->expr;
        if (find_node_in_targetlist(node, subtarget_list) >= 0) {
            null_list = lappend(null_list, (void*)node);
        }
    }

    /* Get the column that both in m_distributeKeys and null_list. */
    foreach (lc, m_distributeKeys) {
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
    MemoryContext cxt = MemoryContextSwitchTo(t_thrd.top_mem_cxt);
    m_estate = CreateExecutorState();
    (void)MemoryContextSwitchTo(m_estate->es_query_cxt);

    if (isVec) {
        m_econtext = CreateExprContext(m_estate);
        ExecAssignVectorForExprEval(m_econtext);
    } else {
        m_econtext = CreateExprContext(m_estate);
    }

    foreach (lc, m_ssinfo) {
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

    foreach (lc, m_skewQual) {
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

    foreach (lc, m_skewQual) {
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

