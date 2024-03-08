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
#include "access/tableam.h"

/* We only create optimized path only when the skew ratio is large than the limit. */
#define SKEW_RATIO_LIMIT 3.0

/* Too much skew value will cost to much memory and execution time, so we add a limit here. */
#define MAX_SKEW_NUM 10

#define IS_JOIN_PATH(path) (IsA(path, HashPath) || IsA(path, NestPath) || IsA(path, MergePath))

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
        /* Divide by 0 when mcv_ratio = 1. Actually, we just need to return a value greater than SKEW_RATIO_LIMIT */
        if (mcv_ratio == 1)
            return SKEW_RATIO_LIMIT + 1;
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
        if (have_mcvs) {
            ret = findMcvValue(vardata.var, nvalues, values, numbers);

            free_attstatsslot(vardata.atttype, values, nvalues, numbers, nnumbers);

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
    slot = MakeSingleTupleTableSlot(tupdesc, false, heaprel->rd_tam_ops);
    slot->tts_nvalid = tupdesc->natts;
    heap_close(heaprel, NoLock);

    /* Set slot value. */
    for (int i = 0; i < tupdesc->natts; i++) {
        for (int j = 0; j < list_length(varList); j++) {
            var = (Var*)list_nth(varList, j);
            con = (Const*)list_nth(valueList, j);

            if (tupdesc->attrs[i].attnum == var->varattno) {
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

    canpass = ExecEvalExpr(exprstate, econtext, &isNull);

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