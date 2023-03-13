/* -------------------------------------------------------------------------
 *
 * es_selectivity.cpp
 *      Routines to compute multi-column selectivities
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *      src/gausskernel/optimizer/path/es_selectivity.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include "catalog/pg_collation.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_statistic.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/var.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "nodes/print.h"
#include "parser/parsetree.h"
#include "utils/extended_statistics.h"

const int TOW_MEMBERS = 2;

ES_SELECTIVITY::ES_SELECTIVITY()
    : es_candidate_list(NULL),
      es_candidate_saved(NULL),
      unmatched_clause_group(NULL),
      root(NULL),
      sjinfo(NULL),
      origin_clauses(NULL),
      path(NULL),
      bucketsize_list(NULL),
      statlist(NULL)
{}

ES_SELECTIVITY::~ES_SELECTIVITY()
{}

bool ES_SELECTIVITY::ContainIndexCols(const es_candidate* es, const IndexOptInfo* index) const
{
    for (int pos = 0; pos < index->ncolumns; pos++) {
        int indexAttNum = index->indexkeys[pos];
        /* 
         * Notice: indexAttNum can be negative. Some indexAttNums of junk column may be negative 
         * since they are located before the first visible column. for example, the indexAttNum 
         * of 'oid' column in system table 'pg_class' is -2.
         */
        if (indexAttNum >= 0 && !bms_is_member(indexAttNum, es->left_attnums))
            return false;
    }

    return true;
}

bool ES_SELECTIVITY::MatchUniqueIndex(const es_candidate* es) const
{
    ListCell* lci = NULL;
    foreach (lci, es->left_rel->indexlist) {
        IndexOptInfo* indexToMatch = (IndexOptInfo*)lfirst(lci);
        if (indexToMatch->relam == BTREE_AM_OID && indexToMatch->unique 
            && ContainIndexCols(es, indexToMatch)) {
            return true;
        }
    }

    return false;
}

/* 
 * check whether the equality constraints match an unique index. 
 * We know the result only has one row if finding a matched unique index.
 */
void ES_SELECTIVITY::CalSelWithUniqueIndex(Selectivity &result)
{
    List* es_candidate_used = NULL;
    ListCell* l = NULL;
    foreach(l, es_candidate_list) {
        es_candidate* temp = (es_candidate*)lfirst(l);
        if (temp->tag == ES_EQSEL && MatchUniqueIndex(temp) && 
            temp->left_rel && temp->left_rel->tuples >= 1.0) {
            result *= 1.0 / temp->left_rel->tuples;
            es_candidate_used = lappend(es_candidate_used, temp);
        }
    }

    /* 
     * Finally, we need to delete es_candidates which have already used. The rests es_candidates 
     * will calculate with statistic info.
     */
    es_candidate_saved = es_candidate_list;
    es_candidate_list = list_difference_ptr(es_candidate_list, es_candidate_used);

    list_free(es_candidate_used);
}

/*
 * @brief        Main entry for using extended statistic to calculate selectivity
 *             root_input can only be NULL when processing group by clauses
 */
Selectivity ES_SELECTIVITY::calculate_selectivity(PlannerInfo* root_input, List* clauses_input,
    SpecialJoinInfo* sjinfo_input, JoinType jointype, JoinPath* path_input, es_type action, STATS_EST_TYPE eType)
{
    Selectivity result = 1.0;
    root = root_input;
    sjinfo = sjinfo_input;
    origin_clauses = clauses_input;
    path = path_input;

    /* group clauselist */
    if (action == ES_GROUPBY) {
        /* group clauselist for group by clauses */
        group_clauselist_groupby(origin_clauses);
    } else {
        group_clauselist(origin_clauses);
    }

    /*
     * Before reading statistic, We check whether the equality constraints match an 
     * unique index. We know the result only has one row if finding a matched unique index.
     */
    CalSelWithUniqueIndex(result);

    /* read statistic */
    read_statistic();

    /* calculate selectivity */
    ListCell* l = NULL;
    Selectivity prob;
    foreach(l, es_candidate_list) {
        es_candidate* temp = (es_candidate*)lfirst(l);
        switch (temp->tag) {
            case ES_EQSEL:
                if (u_sess->attr.attr_sql.enable_ai_stats &&
                    u_sess->attr.attr_sql.multi_stats_type != MCV_OPTION) {
                    prob = cal_eqsel_ai(temp);
                    if (prob >= 0) {
                        result *= prob;
                    } else {
                        result *= cal_eqsel(temp);
                    }
                } else {
                    result *= cal_eqsel(temp);
                }
                break;
            case ES_EQJOINSEL:
                /* compute hash bucket size */
                if (action == ES_COMPUTEBUCKETSIZE) {
                    es_bucketsize* bucket = (es_bucketsize*)palloc(sizeof(es_bucketsize));
                    cal_bucket_size(temp, bucket);
                    bucketsize_list = lappend(bucketsize_list, bucket);
                } else {
                    result *= cal_eqjoinsel(temp, jointype);
                }
                break;
            case ES_GROUPBY:
                build_pseudo_varinfo(temp, eType);
                break;
            default:
                break;
        }
    }

    es_candidate_list = es_candidate_saved;

    return result;
}

/*
 * @brief         group clause by clause type and involving rels, for now, only support eqsel and eqjoinsel
 */
void ES_SELECTIVITY::group_clauselist(List* clauses)
{
    ListCell* l = NULL;
    foreach(l, clauses) {
        Node* clause = (Node*)lfirst(l);

        if (!IsA(clause, RestrictInfo)) {
            unmatched_clause_group = lappend(unmatched_clause_group, clause);
            continue;
        }

        RestrictInfo* rinfo = (RestrictInfo*)clause;
        if (rinfo->pseudoconstant || rinfo->norm_selec > 1 || rinfo->orclause) {
            unmatched_clause_group = lappend(unmatched_clause_group, clause);
            continue;
        }

        if (is_opclause(rinfo->clause)) {
            OpExpr* opclause = (OpExpr*)rinfo->clause;
            Oid opno = opclause->opno;

            /* only handle "=" operator */
            if (get_oprrest(opno) == EQSELRETURNOID) {
                int relid_num = bms_num_members(rinfo->clause_relids);
                if (relid_num == 1) {
                    /* only process clause like t1.a = 1, so only one relid */
                    load_eqsel_clause(rinfo);
                    continue;
                } else if (relid_num == TOW_MEMBERS) {
                    /* only process clause like t1.a = t2.b, so only two relids */
                    load_eqjoinsel_clause(rinfo);
                    continue;
                } else {
                    unmatched_clause_group = lappend(unmatched_clause_group, rinfo);
                    continue;
                }
            }
        } else if ((rinfo->clause) != NULL && IsA(rinfo->clause, NullTest)) {
            NullTest* nullclause = (NullTest*)rinfo->clause;
            int relid_num = bms_num_members(rinfo->clause_relids);
            if (relid_num == 1 && nullclause->nulltesttype == IS_NULL) {
                load_eqsel_clause(rinfo);
                continue;
            }
        }

        unmatched_clause_group = lappend(unmatched_clause_group, clause);
    }

    recheck_candidate_list();
    debug_print();

    return;
}

/*
 * @brief        group groupby-clause by clause type and involving rels, for
 */
void ES_SELECTIVITY::group_clauselist_groupby(List* varinfos)
{
    ListCell* l = NULL;
    foreach(l, varinfos) {
        GroupVarInfo* varinfo = (GroupVarInfo*)lfirst(l);
        if (!is_var_node(varinfo->var)) {
            unmatched_clause_group = lappend(unmatched_clause_group, varinfo);
            continue;
        }

        Var* var = NULL;
        if (IsA(varinfo->var, RelabelType))
            var = (Var*)((RelabelType*)varinfo->var)->arg;
        else
            var = (Var*)varinfo->var;

        ListCell* l2 = NULL;
        bool found_match = false;
        foreach(l2, es_candidate_list) {
            es_candidate* temp = (es_candidate*)lfirst(l2);

            if (temp->tag != ES_GROUPBY)
                continue;

            if (varinfo->rel == temp->left_rel) {
                /* only use left attnums for group by clauses */
                temp->left_attnums = bms_add_member(temp->left_attnums, var->varattno);
                temp->clause_group = lappend(temp->clause_group, varinfo);
                add_clause_map(temp, var->varattno, 0, (Node*)var, NULL);
                found_match = true;
                break;
            }
        }

        /* if not matched, build a new cell in es_candidate_list */
        if (!found_match) {
            es_candidate* es = (es_candidate*)palloc(sizeof(es_candidate));
            RelOptInfo* temp_rel = NULL;

            init_candidate(es);

            es->tag = ES_GROUPBY;
            es->left_rel = varinfo->rel;
            es->left_relids = bms_copy(varinfo->rel->relids);
            es->left_attnums = bms_add_member(es->left_attnums, var->varattno);
            add_clause_map(es, var->varattno, 0, (Node*)var, NULL);
            read_rel_rte(varinfo->var, &temp_rel, &es->left_rte);
            Assert(es->left_rel == temp_rel);
            es->clause_group = lappend(es->clause_group, varinfo);
            es_candidate_list = lappend(es_candidate_list, es);
        }
    }

    recheck_candidate_list();
    debug_print();

    return;
}

/*
 * @brief         initial es_candidate, set all elements to default value or NULL
 */
void ES_SELECTIVITY::init_candidate(es_candidate* es) const
{
    es->tag = ES_EMPTY;
    es->relids = NULL;
    es->left_relids = NULL;
    es->right_relids = NULL;
    es->left_attnums = NULL;
    es->right_attnums = NULL;
    es->left_stadistinct = 0.0;
    es->right_stadistinct = 0.0;
    es->left_first_mcvfreq = 0.0;
    es->right_first_mcvfreq = 0.0;
    es->left_rel = NULL;
    es->right_rel = NULL;
    es->left_rte = NULL;
    es->right_rte = NULL;
    es->clause_group = NIL;
    es->clause_map = NIL;
    es->left_extended_stats = NULL;
    es->right_extended_stats = NULL;
    es->pseudo_clause_list = NIL;
    es->has_null_clause = false;
    return;
}

/*
 * @brief         free memory used in calculate_selectivity except unmatched_clause_group
 */
void ES_SELECTIVITY::clear()
{
    /* delete es_candidate_list */
    ListCell* l = NULL;
    foreach(l, es_candidate_list) {
        es_candidate* temp = (es_candidate*)lfirst(l);
        bms_free_ext(temp->relids);
        bms_free_ext(temp->left_relids);
        bms_free_ext(temp->right_relids);
        bms_free_ext(temp->left_attnums);
        bms_free_ext(temp->right_attnums);
        temp->left_rel = NULL;
        temp->right_rel = NULL;
        temp->left_rte = NULL;
        temp->right_rte = NULL;
        list_free_ext(temp->clause_group);
        list_free_deep(temp->clause_map);
        temp->left_extended_stats = NULL;
        temp->right_extended_stats = NULL;
        list_free_ext(temp->pseudo_clause_list);
    }
    list_free_deep(es_candidate_list);

    clear_extended_stats_list(statlist);

    /*
     * unmatched_clause_group need to be free manually after
     * it is used in clause_selectivity().
     */
    root = NULL;
    sjinfo = NULL;
    origin_clauses = NULL;
    return;
}

/*
 * @brief        free memory used by saving extended_stats after calculation
 */
void ES_SELECTIVITY::clear_extended_stats(ExtendedStats* extended_stats) const
{
#ifndef ENABLE_MULTIPLE_NODES
    if (extended_stats && extended_stats->dependencies) {
        MVDependencies* dependencies = extended_stats->dependencies;
        for (int i = 0; i < dependencies->ndeps; i++) {
            if (dependencies->deps[i]) {
                pfree_ext(dependencies->deps[i]);
            }
        }
        pfree_ext(dependencies);
    }
#endif

    if (extended_stats) {
        bms_free_ext(extended_stats->bms_attnum);
        if (extended_stats->mcv_numbers)
            pfree_ext(extended_stats->mcv_numbers);
        if (extended_stats->mcv_values)
            pfree_ext(extended_stats->mcv_values);
        if (extended_stats->mcv_nulls)
            pfree_ext(extended_stats->mcv_nulls);
        if (extended_stats->other_mcv_numbers)
            pfree_ext(extended_stats->other_mcv_numbers);
        if (extended_stats->bayesnet_model)
            pfree_ext(extended_stats->bayesnet_model);
        pfree_ext(extended_stats);
        extended_stats = NULL;
    }
    return;
}

/*
 * @brief        free memory of extended_stats_list by calling clear_extended_stats
 */
void ES_SELECTIVITY::clear_extended_stats_list(List* stats_list) const
{
    if (stats_list) {
        ListCell* lc = NULL;
        foreach(lc, stats_list) {
            ExtendedStats* extended_stats = (ExtendedStats*)lfirst(lc);
            clear_extended_stats(extended_stats);
        }
        list_free_ext(stats_list);
    }
    return;
}

/*
 * @brief        copy the original pointer, repoint it to something else
 *             in order to avoid failure when using list_free
 * @param         ListCell* l
 * @return
 * @exception   None
 */
ExtendedStats* ES_SELECTIVITY::copy_stats_ptr(ListCell* l) const
{
    ExtendedStats* result = (ExtendedStats*)lfirst(l);
    lfirst(l) = NULL;
    return result;
}

/*
 * @brief       add an eqjsel clause to es_candidate_list and group by relid
 *            we should have bms_num_members(clause->clause_relids) == 1
 */
void ES_SELECTIVITY::load_eqsel_clause(RestrictInfo* clause)
{
    /* group clause by rels, add to es_candidate_list */
    ListCell* l = NULL;
    foreach(l, es_candidate_list) {
        es_candidate* temp = (es_candidate*)lfirst(l);

        if (temp->tag != ES_EQSEL)
            continue;

        if (bms_equal(clause->clause_relids, temp->relids)) {
            if (add_attnum(clause, temp)) {
                temp->clause_group = lappend(temp->clause_group, clause);
                if (IsA(clause->clause, NullTest))
                    temp->has_null_clause = true;
                return;
            }
        }
    }

    /* if not matched, build a new cell in es_candidate_list */
    if (!build_es_candidate(clause, ES_EQSEL))
        unmatched_clause_group = lappend(unmatched_clause_group, clause);

    return;
}

/*
 * @brief        add an eqjoinsel clause to es_candidate_list and group by relid
 */
void ES_SELECTIVITY::load_eqjoinsel_clause(RestrictInfo* clause)
{
    /*
     * the relids in the clause should be as same as sjinfo, so we can avoid parameterized conditon.
     */
    if (sjinfo) {
        if (!bms_overlap(sjinfo->min_lefthand, clause->clause_relids) ||
            !bms_overlap(sjinfo->min_righthand, clause->clause_relids)) {
            unmatched_clause_group = lappend(unmatched_clause_group, clause);
            return;
        }
    }

    /* group clause by rels, add to es_candidate_list */
    if (bms_num_members(clause->left_relids) == 1 && bms_num_members(clause->right_relids) == 1) {
        ListCell* l = NULL;
        foreach(l, es_candidate_list) {
            es_candidate* temp = (es_candidate*)lfirst(l);

            if (temp->tag != ES_EQJOINSEL)
                continue;

            if (bms_equal(clause->clause_relids, temp->relids)) {
                if (add_attnum(clause, temp)) {
                    temp->clause_group = lappend(temp->clause_group, clause);
                    return;
                }
            }
        }

        /* if not matched, build a new cell in es_candidate_list */
        if (!build_es_candidate(clause, ES_EQJOINSEL))
            unmatched_clause_group = lappend(unmatched_clause_group, clause);

        return;
    }

    unmatched_clause_group = lappend(unmatched_clause_group, clause);

    return;
}

/*
 * @brief       make a combination of es->right_attnums or es->left_attnums with input attnum by clause map
 * @param    left: true:  add to es->right_attnums; false: add to es->left_attnums
 * @return     combination of Bitmapset
 * @exception   None
 */
Bitmapset* ES_SELECTIVITY::make_attnums_by_clause_map(es_candidate* es, Bitmapset* attnums, bool left) const
{
    ListCell* lc_clause_map = NULL;
    Bitmapset* result = NULL;
    foreach(lc_clause_map, es->clause_map) {
        es_clause_map* clause_map = (es_clause_map*)lfirst(lc_clause_map);
        if (left && bms_is_member(clause_map->left_attnum, attnums))
            result = bms_add_member(result, clause_map->right_attnum);
        else if (!left && bms_is_member(clause_map->right_attnum, attnums))
            result = bms_add_member(result, clause_map->left_attnum);
    }
    return result;
}

/*
 * @brief       find the matched extended stats in stats_list
 * @param    es :proving mathing conditions including relids , attnums
 * @param    stats_list : the extended statistic list
 * @param    left : true : match with the left_rel; false: match with the right_rel
 * @return    None
 * @exception   None
 */
void ES_SELECTIVITY::match_extended_stats(es_candidate* es, List* stats_list, bool left)
{
    int max_matched = 0;
    int num_members = bms_num_members(es->left_attnums);
    char other_side_starelkind;
    RangeTblEntry* other_side_rte = NULL;
    Bitmapset* this_side_attnums = NULL;
    if (left) {
        /* this side is left and the other side is right */
        other_side_starelkind = OidIsValid(es->right_rte->partitionOid) ? STARELKIND_PARTITION : STARELKIND_CLASS;
        other_side_rte = es->right_rte;
        this_side_attnums = es->left_attnums;
    } else {
        /* this side is right and other side is left */
        other_side_starelkind = OidIsValid(es->left_rte->partitionOid) ? STARELKIND_PARTITION : STARELKIND_CLASS;
        other_side_rte = es->left_rte;
        this_side_attnums = es->right_attnums;
    }

    /* best_matched_listcell use to save the best match from stats list */
    ListCell* best_matched_listcell = NULL;
    /* best_matched_stats use to save the best match from es_get_multi_column_stats */
    ListCell* best_matched_stats = (ListCell*)palloc(sizeof(ListCell));
    lfirst(best_matched_stats) = NULL;
    ListCell* lc = NULL;
    foreach(lc, stats_list) {
        ExtendedStats* extended_stats = (ExtendedStats*)lfirst(lc);
        ExtendedStats* other_side_extended_stats = NULL;
        if (bms_is_subset(extended_stats->bms_attnum, this_side_attnums)) {
            int matched = bms_num_members(extended_stats->bms_attnum);

            Bitmapset* other_side_attnums = make_attnums_by_clause_map(es, extended_stats->bms_attnum, left);
            other_side_extended_stats = es_get_multi_column_stats(
                other_side_rte->relid, other_side_starelkind, other_side_rte->inh, other_side_attnums);
            if (other_side_extended_stats != NULL && matched == num_members) {
                /* all attnums have extended stats, leave */
                if (left) {
                    es->left_extended_stats = copy_stats_ptr(lc);
                    es->right_extended_stats = other_side_extended_stats;
                } else {
                    es->right_extended_stats = copy_stats_ptr(lc);
                    es->left_extended_stats = other_side_extended_stats;
                }
                clear_extended_stats((ExtendedStats*)lfirst(best_matched_stats));
                lfirst(best_matched_stats) = NULL;
                break;
            } else if (other_side_extended_stats != NULL && matched > max_matched) {
                /* not all attnums have extended stats, find the first maximum match */
                best_matched_listcell = lc;
                clear_extended_stats((ExtendedStats*)lfirst(best_matched_stats));
                lfirst(best_matched_stats) = other_side_extended_stats;
                max_matched = matched;
            } else
                clear_extended_stats(other_side_extended_stats);
        }
    }

    if (best_matched_listcell && lfirst(best_matched_stats)) {
        if (left) {
            es->left_extended_stats = copy_stats_ptr(best_matched_listcell);
            es->right_extended_stats = (ExtendedStats*)lfirst(best_matched_stats);
        } else {
            es->right_extended_stats = copy_stats_ptr(best_matched_listcell);
            es->left_extended_stats = (ExtendedStats*)lfirst(best_matched_stats);
        }
        lfirst(best_matched_stats) = NULL;
        /* remove members not in the multi-column stats */
        if (max_matched != num_members) {
            Bitmapset* tmpset = bms_difference(es->left_attnums, es->left_extended_stats->bms_attnum);
            int dump_attnum;
            while ((dump_attnum = bms_first_member(tmpset)) > 0) {
                es->left_attnums = bms_del_member(es->left_attnums, dump_attnum);
                remove_attnum(es, dump_attnum);
            }
            bms_free_ext(tmpset);
        }
    }
    pfree_ext(best_matched_stats);
    return;
}

/*
 * @brief       modify distinct value using possion model
 * @param    es : proving the distinct value to modify
 * @param    left  :true : modify the left distinct value; false: modify the right one
 * @param    sjinfo :join infos from inputs of calculate_selecitvity, can be NULL for eqsel
 * @return    None
 * @exception   None
 */
void ES_SELECTIVITY::modify_distinct_by_possion_model(es_candidate* es, bool left, SpecialJoinInfo* spjinfo) const
{
    bool enablePossion = false;
    double varratio = 1.0;
    ListCell* lc = NULL;
    VariableStatData vardata;
    float4 distinct = 0.0;
    double tuples = 0.0;

    /* build vardata */
    vardata.enablePossion = true;
    if (left && es->left_rel->tuples > 0) {
        vardata.rel = es->left_rel;
        distinct = es->left_stadistinct;
        tuples = es->left_rel->tuples;
        foreach(lc, es->clause_map) {
            es_clause_map* map = (es_clause_map*)lfirst(lc);
            vardata.var = (Node*)map->left_var;
            enablePossion = can_use_possion(&vardata, spjinfo, &varratio);
            if (!enablePossion)
                break;
        }
    } else if (!left && es->right_rel->tuples > 0) {
        vardata.rel = es->right_rel;
        distinct = es->right_stadistinct;
        tuples = es->right_rel->tuples;
        foreach(lc, es->clause_map) {
            es_clause_map* map = (es_clause_map*)lfirst(lc);
            vardata.var = (Node*)map->right_var;
            enablePossion = can_use_possion(&vardata, spjinfo, &varratio);
            if (!enablePossion)
                break;
        }
    }

    if (enablePossion) {
        double tmp = distinct;
        distinct = NUM_DISTINCT_SELECTIVITY_FOR_POISSON(distinct, tuples, varratio);
        ereport(ES_DEBUG_LEVEL,
            (errmodule(MOD_OPT),
                (errmsg("[ES]The origin distinct value is %f. After using possion model with ntuples=%f and ration=%e \
                    The new distinct value is %f",
                    tmp,
                    tuples,
                    varratio,
                    distinct))));
    }

    if (left && enablePossion) {
        es->left_stadistinct = distinct;
    } else if ((!left) && enablePossion) {
        es->right_stadistinct = distinct;
    }
    return;
}

static bool ClauseIsLegal(es_type type, const Node* left, const Node* right, int leftAttnum, int rightAttnum)
{
    if (leftAttnum < 0 || rightAttnum < 0) {
            return false;
    }

    /* check clause type */
    switch (type) {
        case ES_EQSEL:
            if (!IsA(left, Const) && !IsA(right, Const) && !IsA(left, Param) && !IsA(right, Param))
                return false;
            else if (IsA(left, Const) && ((Const*)left)->constisnull)
                return false;
            else if (IsA(right, Const) && ((Const*)right)->constisnull)
                return false;
            break;
        case ES_EQJOINSEL:
        default:
            break;
    }
    return true;
}

static inline bool RteIsValid(const RangeTblEntry* rte)
{
    return (rte != NULL && rte->rtekind == RTE_RELATION);
}

void ES_SELECTIVITY::setup_es(es_candidate* es, es_type type, RestrictInfo* clause)
{
    es->tag = type;
    es->relids = bms_copy(clause->clause_relids);
    es->clause_group = lappend(es->clause_group, clause);
    es->has_null_clause = IsA(clause->clause, NullTest);
    es->left_first_mcvfreq = 0.0;
    es->right_first_mcvfreq = 0.0;
}

bool ES_SELECTIVITY::build_es_candidate_for_eqsel(es_candidate* es, Node* var, int attnum, bool left,
    RestrictInfo* clause)
{
    read_rel_rte(var, &es->left_rel, &es->left_rte);
    if (!RteIsValid(es->left_rte)) {
        return false;
    }
    es->left_attnums = bms_add_member(es->left_attnums, attnum);
    if (left) {
        es->left_relids =
                    clause->left_relids != NULL ? bms_copy(clause->left_relids) : bms_copy(clause->clause_relids);
    } else {
        es->left_relids =
                    clause->right_relids != NULL ? bms_copy(clause->right_relids) : bms_copy(clause->clause_relids);
    }
    add_clause_map(es, attnum, 0, var, NULL);
    return true;
}

/*
 * @brief       build a new es_candidate and add to es_candidate_list
 */
bool ES_SELECTIVITY::build_es_candidate(RestrictInfo* clause, es_type type)
{
    Node* left = NULL;
    Node* right = NULL;
    int left_attnum = 0;
    int right_attnum = 0;
    bool success = false;

    if (IsA(clause->clause, OpExpr)) {
        OpExpr* opclause = (OpExpr*)clause->clause;

        Assert(list_length(opclause->args) == TOW_MEMBERS);

        left = (Node*)linitial(opclause->args);
        right = (Node*)lsecond(opclause->args);
        left_attnum = read_attnum(left);
        right_attnum = read_attnum(right);
        if (!ClauseIsLegal(type, left, right, left_attnum, right_attnum))
            return false;
    } else {
        Assert(IsA(clause->clause, NullTest));
        NullTest* nullclause = (NullTest*)clause->clause;
        left = (Node*)nullclause->arg;
        left_attnum = read_attnum(left);
        if (left_attnum < 0)
            return false;
    }

    es_candidate* es = (es_candidate*)palloc(sizeof(es_candidate));
    init_candidate(es);

    switch (type) {
        case ES_EQSEL:
            /* only use left side */
            if (left_attnum > 0 && right_attnum == 0) {
                success = build_es_candidate_for_eqsel(es, left, left_attnum, true, clause);
            } else if (right_attnum > 0 && left_attnum == 0) {
                Assert(clause->right_relids != NULL);
                success = build_es_candidate_for_eqsel(es, right, right_attnum, false, clause);
            } else {
                pfree_ext(es);
                return false;
            }
            break;
        case ES_EQJOINSEL:
            if (left_attnum > 0 && right_attnum > 0) {
                read_rel_rte(left, &es->left_rel, &es->left_rte);
                read_rel_rte(right, &es->right_rel, &es->right_rte);
                if (!RteIsValid(es->left_rte) || !RteIsValid(es->right_rte)) {
                    break;
                }
                es->left_relids = bms_copy(clause->left_relids);
                es->right_relids = bms_copy(clause->right_relids);
                es->left_attnums = bms_add_member(es->left_attnums, left_attnum);
                es->right_attnums = bms_add_member(es->right_attnums, right_attnum);
                add_clause_map(es, left_attnum, right_attnum, left, right);
                success = true;
            } else {
                pfree_ext(es);
                return false;
            }
            break;
        default:
            /* for future development, should not reach here now */
            pfree_ext(es);
            return false;
    }

    /* double check */
    if (!success) {
        es->left_rel = NULL;
        es->right_rel = NULL;
        es->left_rte = NULL;
        es->right_rte = NULL;
        pfree_ext(es);
        return false;
    }

    setup_es(es, type, clause);

    es_candidate_list = lappend(es_candidate_list, es);

    return true;
}

/*
 * @brief        remove useless member in es_candidate_list to unmatched_clause_group
 */
void ES_SELECTIVITY::recheck_candidate_list()
{
    if (!es_candidate_list)
        return;
    ListCell* l = NULL;
    bool validate = true;

    /* try to use equivalence_class to re-combinate clauses first */
    foreach(l, es_candidate_list) {
        es_candidate* temp = (es_candidate*)lfirst(l);
        if (temp->tag == ES_EQJOINSEL && list_length(temp->clause_group) == 1 && list_length(es_candidate_list) > 1)
            (void)try_equivalence_class(temp);
    }

    foreach(l, es_candidate_list) {
        es_candidate* temp = (es_candidate*)lfirst(l);
        switch (temp->tag) {
            case ES_EQSEL:
                if (list_length(temp->clause_group) <= 1)
                    validate = false;
                else if (temp->left_rte && bms_num_members(temp->left_attnums) <= 1)
                    validate = false;
                break;
            case ES_EQJOINSEL:
                if (list_length(temp->clause_group) <= 1)
                    validate = false;
                else if (bms_num_members(temp->left_attnums) <= 1 || bms_num_members(temp->right_attnums) <= 1)
                    validate = false;
                break;
            case ES_GROUPBY:
                if (bms_num_members(temp->left_attnums) <= 1)
                    validate = false;
                break;
            default:
                break;
        }
        if (!validate) {
            unmatched_clause_group = list_concat(unmatched_clause_group, temp->clause_group);
            temp->tag = ES_EMPTY;
            temp->clause_group = NULL;
        }
    }
    return;
}

static inline bool IsUnsupportedCases(const EquivalenceClass* ec)
{
    /* only consider var = var situation */
    if (ec->ec_has_const)
        return true;

    /* ignore broken ecs */
    if (ec->ec_broken)
        return true;

    /* if members of ECs are less than two, won't generate any substitute */
    if (list_length(ec->ec_members) <= TOW_MEMBERS)
        return true;

    return false;
}

/*
 * @brief       pre-check the es candidate item is or not in current equivalence class.
 * @return      bool, true or false.
 */
bool ES_SELECTIVITY::IsEsCandidateInEqClass(es_candidate *es, EquivalenceClass *ec)
{
    if (es == NULL || ec == NULL) {
        return false;
    }

    /* Quickly ignore any that don't cover the join */
    if (!bms_is_subset(es->relids, ec->ec_relids)) {
        return false;
    }

    foreach_cell (lc, ec->ec_members) {
        EquivalenceMember *em = (EquivalenceMember *)lfirst(lc);
        Var *emVar = (Var *)LocateOpExprLeafVar((Node *)em->em_expr);

        if (emVar == NULL) {
            continue;
        }

        if (emVar->varattno <= 0) {
            continue;
        }

        /* left or right branch of join es occurs in the current equivalencen member, so the ec is valid. */
        if ((bms_equal(es->left_relids, em->em_relids) && bms_is_member(emVar->varattno, es->left_attnums)) ||
            (bms_equal(es->right_relids, em->em_relids) && bms_is_member(emVar->varattno, es->right_attnums))) {
            return true;
        }
    }

    return false;
}

/*
 * @brief       try to find a substitude clause building from equivalence classes
 * @return    true when find a substitude clause; false when find nothing
 */
bool ES_SELECTIVITY::try_equivalence_class(es_candidate* es)
{
    if (path && path->path.pathtype == T_MergeJoin) {
        /* for mergejoin, do not adjust clause using equivalence class */
        return false;
    }

    ListCell* lc = NULL;
    bool result = false;

    foreach(lc, root->eq_classes) {
        EquivalenceClass* ec = (EquivalenceClass*)lfirst(lc);

        if (IsUnsupportedCases(ec))
            continue;

        /* ignore ec which does not contain the es info. */
        if (!IsEsCandidateInEqClass(es, ec))
            continue;

        Bitmapset* tmpset = bms_copy(ec->ec_relids);
        int ec_relid = 0;
        while ((ec_relid = bms_first_member(tmpset)) >= 0) {
            if (bms_is_member(ec_relid, es->relids))
                continue;
            ListCell* lc2 = NULL;
            foreach(lc2, es_candidate_list) {
                es_candidate* temp = (es_candidate*)lfirst(lc2);
                if (temp->tag != ES_EQJOINSEL)
                    continue;
                if (bms_equal(temp->relids, es->relids))
                    continue;
                if (bms_is_member(ec_relid, temp->relids) && bms_overlap(temp->relids, es->relids)) {
                    Bitmapset* interset_relids = bms_intersect(temp->relids, es->relids);
                    Bitmapset* join_relids = bms_copy(interset_relids);
                    join_relids = bms_add_member(join_relids, ec_relid);
                    Assert(bms_equal(join_relids, temp->relids));
                    Bitmapset* outer_relids = bms_make_singleton(ec_relid);
                    List* pseudo_clauselist =
                        generate_join_implied_equalities_normal(root, ec, join_relids, outer_relids, interset_relids);
                    if (pseudo_clauselist != NULL) {
                        result = match_pseudo_clauselist(pseudo_clauselist, temp, es->clause_group);
                        if (log_min_messages <= ES_DEBUG_LEVEL) {
                            ereport(ES_DEBUG_LEVEL,
                                (errmodule(MOD_OPT_JOIN), errmsg("[ES]Build new clause using equivalence class:)")));
                            print_clauses(pseudo_clauselist);
                            ereport(ES_DEBUG_LEVEL,
                                (errmodule(MOD_OPT_JOIN),
                                    errmsg("[ES]The old clause will be abandoned? %d)", (int)result)));
                            print_clauses(es->clause_group);
                        }
                    }

                    bms_free_ext(interset_relids);
                    bms_free_ext(join_relids);
                    bms_free_ext(outer_relids);
                    if (result) {
                        /* replace the removed clause in clauselist with the new built one */
                        ListCell* lc3 = NULL;
                        foreach(lc3, origin_clauses) {
                            void* clause = (void*)lfirst(lc3);
                            if (clause == linitial(es->clause_group)) {
                                /* maybe cause memory problem as the old clause is not released */
                                lfirst(lc3) = linitial(pseudo_clauselist);
                            }
                        }
                        /* For hashclause, we have to process joinrestrictinfo in path as well */
                        if (path) {
                            foreach(lc3, path->joinrestrictinfo) {
                                void* clause = (void*)lfirst(lc3);
                                if (clause == linitial(es->clause_group)) {
                                    /* maybe cause memory problem as the old clause is not released */
                                    lfirst(lc3) = linitial(pseudo_clauselist);
                                }
                            }
                        }
                        break;
                    }
                }
            }
            if (result) {
                /*
                 * If sucess, the clause has been tranformed and saved in another es_candidate.
                 * So no need to keep this es_candidate anymore.
                 */
                es->tag = ES_EMPTY;
                es->clause_group = NULL;
                break;
            }
        }
        bms_free_ext(tmpset);
        if (result)
            break;
    }
    return result;
}

/*
 * @brief       try to match the newborn clause building by try_equivalence_class() with the existed clause group
 *            like what we do in group_clauselist(), but more simple.
 */
bool ES_SELECTIVITY::match_pseudo_clauselist(List* clauses, es_candidate* es, List* origin_clause)
{
    bool result = false;
    ListCell* lc = NULL;
    foreach(lc, clauses) {
        Node* clause = (Node*)lfirst(lc);

        if (!IsA(clause, RestrictInfo)) {
            continue;
        }

        RestrictInfo* rinfo = (RestrictInfo*)clause;
        if (rinfo->pseudoconstant || rinfo->norm_selec > 1 || rinfo->orclause) {
            continue;
        }

        if (is_opclause(rinfo->clause)) {
            OpExpr* opclause = (OpExpr*)rinfo->clause;
            Oid opno = opclause->opno;

            /* only handle "=" operator */
            if (get_oprrest(opno) == EQSELRETURNOID) {
                Assert(bms_num_members(rinfo->clause_relids) == TOW_MEMBERS);
                if (add_attnum(rinfo, es)) {
                    es->clause_group = lappend(es->clause_group, clause);
                    es->pseudo_clause_list = lappend(es->pseudo_clause_list, clause);
                    es->pseudo_clause_list = list_concat(es->pseudo_clause_list, origin_clause);
                    result = true;
                }
            }
        }
    }
    return result;
}

/*
 * @brief       relpace the original clause in the input clause list with the new clause build by equivalence class,
 *             the memory used by old clause will be release by optimizer context or something esle
 */
void ES_SELECTIVITY::replace_clause(Datum* old_clause, Datum* new_clause) const
{
    ListCell* lc = NULL;
    foreach(lc, origin_clauses) {
        if (lfirst(lc) == old_clause) {
            lfirst(lc) = new_clause;
            break;
        }
    }
}

/*
 * @brief        main entry to read statistic which will be used to calculate selectivity, called by
 * calculate_selectivity
 */
void ES_SELECTIVITY::read_statistic()
{
    if (!es_candidate_list)
        return;
    ListCell* l = NULL;
    foreach(l, es_candidate_list) {
        es_candidate* temp = (es_candidate*)lfirst(l);
        switch (temp->tag) {
            case ES_EQSEL:
            case ES_GROUPBY:
                read_statistic_eqsel(temp);
                break;
            case ES_EQJOINSEL:
                read_statistic_eqjoinsel(temp);
                break;
            default:
                /* empty */
                break;
        }
    }
    return;
}

bool ES_SELECTIVITY::cal_stadistinct_eqjoinsel(es_candidate* es)
{
    /*
     * Since we can not tell how many or which columns are actaully null when nullfrac == 1.0,
     * so we will not use multi-column when nullfrac == 1.0.
     */
    if (es->left_extended_stats &&
        (es->left_extended_stats->nullfrac != 0.0 || es->left_extended_stats->distinct != 0.0 ||
        es->left_extended_stats->mcv_values) &&
        es->right_extended_stats &&
        (es->right_extended_stats->nullfrac != 0.0 || es->right_extended_stats->distinct != 0.0 ||
        es->right_extended_stats->mcv_values)) {
        /* should not recieve empty extended stas here, except all columns are empty */
        if (es->left_extended_stats->distinct < 0)
            es->left_stadistinct = clamp_row_est(-1 * es->left_extended_stats->distinct * es->left_rel->tuples *
                                                 (1.0 - es->left_extended_stats->nullfrac));
        else
            es->left_stadistinct = clamp_row_est(es->left_extended_stats->distinct);

        if (es->right_extended_stats->distinct < 0)
            es->right_stadistinct = clamp_row_est(-1 * es->right_extended_stats->distinct * es->right_rel->tuples *
                                                  (1.0 - es->right_extended_stats->nullfrac));
        else
            es->right_stadistinct = clamp_row_est(es->right_extended_stats->distinct);

        /*
         * Use possion model if satisify condition.
         */
        modify_distinct_by_possion_model(es, true, sjinfo);
        modify_distinct_by_possion_model(es, false, sjinfo);

        /* replace the old clause with the new one */
        if (es->pseudo_clause_list) {
            ListCell* lc2 = NULL;
            foreach(lc2, es->pseudo_clause_list) {
                Datum* new_clause = (Datum*)lfirst(lc2);
                lc2 = lnext(lc2);
                Datum* old_clause = (Datum*)lfirst(lc2);
                /* make sure the new clause is still in the clause group */
                ListCell* lc3 = NULL;
                foreach(lc3, es->clause_group) {
                    if ((Datum*)lfirst(lc3) == new_clause) {
                        replace_clause(old_clause, new_clause);
                        break;
                    }
                }
            }
        }
        return true;
    }
    return false;
}

#define CLEAN_UP_TEMP_OBJECTS(tmp_left, tmp_right, left_stats_list, right_stats_list) \
    do {                                                                \
        bms_free_ext(tmp_left);                                         \
        bms_free_ext(tmp_right);                                        \
        clear_extended_stats_list(left_stats_list);                     \
        clear_extended_stats_list(right_stats_list);                    \
    } while (0)

/*
 * @brief       read extended statistic for eqjoinsel
 *            There are three possible situations:
 *            (1) No statistic data in pg_statistic, then num_stats will be 0
 *            (2) There are over 100 records in pg_statistic when search extended statistics for target table,
 *                  then the returned stats_list will be empty and we have to search manually. In this case,
 *                  we have too many combinations to try so that we will make some compromise and only
 *                  try limited possibilities.
 *            (3) There are less than 100 records and the returned stats_list will be all records. Then we will
 *                  search the list for the best answer.
 */
void ES_SELECTIVITY::read_statistic_eqjoinsel(es_candidate* es)
{
    int left_num_stats = 0;
    int right_num_stats = 0;
    char left_starelkind = OidIsValid(es->left_rte->partitionOid) ? STARELKIND_PARTITION : STARELKIND_CLASS;
    char right_starelkind = OidIsValid(es->right_rte->partitionOid) ? STARELKIND_PARTITION : STARELKIND_CLASS;

    /* read all multi-column statistic from pg_statistic if possible */
    List* left_stats_list =
        es_get_multi_column_stats(es->left_rte->relid, left_starelkind, es->left_rte->inh, &left_num_stats);
    List* right_stats_list =
        es_get_multi_column_stats(es->right_rte->relid, right_starelkind, es->right_rte->inh, &right_num_stats);

    /* no multi-column statistic */
    if (left_num_stats == 0 || right_num_stats == 0) {
        report_no_stats(es->left_rte->relid, es->left_attnums);
        report_no_stats(es->right_rte->relid, es->right_attnums);
        remove_candidate(es);
        return;
    }

    /* save attnums for no analyze list */
    Bitmapset* tmp_left = bms_copy(es->left_attnums);
    Bitmapset* tmp_right = bms_copy(es->right_attnums);

    /* when at least one side return with multi-column statistic list */
    if (left_num_stats <= ES_MAX_FETCH_NUM_OF_INSTANCE && left_num_stats <= right_num_stats) {
        match_extended_stats(es, left_stats_list, true);
    } else if (right_num_stats <= ES_MAX_FETCH_NUM_OF_INSTANCE) {
        match_extended_stats(es, right_stats_list, false);
    } else {
        /*
         * There are too many multi-column statistic, so return null list.
         * We have to search pg_statistic manually with limited combinations.
         * So could lose some matches.
         */
        ereport(ES_DEBUG_LEVEL,
            (errmodule(MOD_OPT_JOIN), errmsg("[ES] Too many multi-column statistic, could lose matches.")));

        while (bms_num_members(es->left_attnums) >= TOW_MEMBERS) {
            ExtendedStats* left_extended_stats =
                es_get_multi_column_stats(es->left_rte->relid, left_starelkind, es->left_rte->inh, es->left_attnums);
            if (left_extended_stats != NULL) {
                ExtendedStats* right_extended_stats = es_get_multi_column_stats(
                    es->right_rte->relid, right_starelkind, es->right_rte->inh, es->right_attnums);
                if (right_extended_stats != NULL) {
                    es->left_extended_stats = left_extended_stats;
                    es->right_extended_stats = right_extended_stats;
                    break;
                } else
                    clear_extended_stats(left_extended_stats);
            }

            /*
             * if not found, delete the first member of attnums and use the rest atts to match.
             * delete clause and clause_map in es.
             */
            remove_attnum(es, bms_first_member(es->left_attnums));
        }
    }

    if (!cal_stadistinct_eqjoinsel(es)) {
        /* no multi-column statistic matched */
        report_no_stats(es->left_rte->relid, tmp_left);
        report_no_stats(es->right_rte->relid, tmp_right);
        remove_candidate(es);
    }

    CLEAN_UP_TEMP_OBJECTS(tmp_left, tmp_right, left_stats_list, right_stats_list);
    return;
}

void ES_SELECTIVITY::remove_members_without_es_stats(int max_matched, int num_members, es_candidate* es)
{
    if (max_matched != num_members) {
        Bitmapset* tmpset = bms_difference(es->left_attnums, es->left_extended_stats->bms_attnum);
        int dump_attnum;
        while ((dump_attnum = bms_first_member(tmpset)) > 0) {
            es->left_attnums = bms_del_member(es->left_attnums, dump_attnum);
            remove_attnum(es, dump_attnum);
        }
        bms_free_ext(tmpset);
    }
}

void ES_SELECTIVITY::cal_stadistinct_eqsel(es_candidate* es)
{
    /*
     * Since we can not tell how many or which columns are actaully null when nullfrac == 1.0,
     * so we will not use multi-column when nullfrac == 1.0.
     */
    if (es->left_extended_stats &&
        (es->left_extended_stats->nullfrac != 0.0 || es->left_extended_stats->distinct != 0.0 ||
        es->left_extended_stats->mcv_values)) {
        /* should not recieve empty extended stas here, except all columns are empty */
        if (es->left_extended_stats->distinct < 0)
            es->left_stadistinct = clamp_row_est(-1 * es->left_extended_stats->distinct * es->left_rel->tuples);
        else
            es->left_stadistinct = clamp_row_est(es->left_extended_stats->distinct);

        /*
         * Use possion model if satisify condition.
         * we don't need possion to estimate distinct because
         * we can't estimate accurate for multiple exprs.
         */
        if (es->tag != ES_GROUPBY)
            modify_distinct_by_possion_model(es, true, NULL);
    } else {
        /* no multi-column statistic matched */
        remove_candidate(es);
    }
}

/*
 * @brief       read extended statistic for eqsel or groupby, details are as same as read_statistic_eqjoinsel
 */
void ES_SELECTIVITY::read_statistic_eqsel(es_candidate* es)
{
    int num_stats = 0;
    char starelkind = OidIsValid(es->left_rte->partitionOid) ? STARELKIND_PARTITION : STARELKIND_CLASS;
    /* read all multi-column statistic from pg_statistic_ext if possible */
    statlist =
        es_get_multi_column_stats(es->left_rte->relid, starelkind, es->left_rte->inh, &num_stats, es->has_null_clause);
    /* no multi-column statistic */
    if (num_stats == 0) {
        report_no_stats(es->left_rte->relid, es->left_attnums);
        remove_candidate(es);
        return;
    }

    es->left_extended_stats = NULL;
    /* return with multi-column statistic list */
    if (num_stats <= ES_MAX_FETCH_NUM_OF_INSTANCE) {
        ListCell* l = NULL;
        int max_matched = 0;
        int num_members = bms_num_members(es->left_attnums);
        ListCell* best_matched_stat_ptr = NULL;
        foreach(l, statlist) {
            ExtendedStats* extended_stats = (ExtendedStats*)lfirst(l);
            if (bms_is_subset(extended_stats->bms_attnum, es->left_attnums)) {
                int matched = bms_num_members(extended_stats->bms_attnum);
                if (matched == num_members) {
                    best_matched_stat_ptr = l;
                    max_matched = matched;
                    break;
                } else if (matched > max_matched) {
                    best_matched_stat_ptr = l;
                    max_matched = matched;
                }
            }
        }

        if (es->left_extended_stats == NULL && best_matched_stat_ptr != NULL) {
            es->left_extended_stats = (ExtendedStats *)lfirst(best_matched_stat_ptr);
            /* remove members not in the multi-column stats */
            remove_members_without_es_stats(max_matched, num_members, es);
        }
    } else {
        /*
         * There are too many multi-column statistic, so return null list.
         * We have to search pg_statistic manually with limited combinations.
         * So could lose some matches.
         */
        ereport(ES_DEBUG_LEVEL,
            (errmodule(MOD_OPT_JOIN), errmsg("[ES] Too many multi-column statistic, could lose matches.")));

        while (bms_num_members(es->left_attnums) >= TOW_MEMBERS) {
            ExtendedStats* extended_stats = es_get_multi_column_stats(
                es->left_rte->relid, starelkind, es->left_rte->inh, es->left_attnums, es->has_null_clause);
            if (extended_stats != NULL) {
                es->left_extended_stats = extended_stats;
                break;
            }

            /*
             * if not found, delete one attnum and use the rest to try again.
             * delete clause and clause_map.
             */
            remove_attnum(es, bms_first_member(es->left_attnums));
        }
    }

    cal_stadistinct_eqsel(es);

    return;
}

/*
 * @brief         read attnum from input,
 * @param    node: should be a Var*
 * @return    return -1 if varattno <= 0, return 0 means node is not a var
 * @exception    None
 */
int ES_SELECTIVITY::read_attnum(Node* node) const
{
    Node* basenode = NULL;
    int attnum = -1;
    if (IsA(node, RelabelType))
        basenode = (Node*)((RelabelType*)node)->arg;
    else
        basenode = node;

    if (IsA(basenode, Var)) {
        Var* var = (Var*)basenode;
        attnum = var->varattno;
        if (attnum <= 0)
            attnum = -1;
    } else if (IsA(node, Const) || IsA(node, Param))
        attnum = 0;

    return attnum;
}

static List* BuildNewRelList(Bitmapset* attnumsTmp, Oid relidOid)
{
    List* record = NIL;
    List* relidList = NIL;
    List* attidList = NIL;
    int attnum = bms_first_member(attnumsTmp);
    while (attnum != -1) {
        attidList = lappend_int(attidList, attnum);
        attnum = bms_first_member(attnumsTmp);
    }
    bms_free_ext(attnumsTmp);
    relidList = lappend_oid(relidList, relidOid);
    record = lappend(record, relidList);
    record = lappend(record, attidList);
    return record;
}

/*
 * @brief       save non-analyze multi-column to g_NoAnalyzeRelNameList
 * @param    relid_oid, relation oid
 * @param    attnums, multi-column attribute number
 * @return
 * @exception   None
 */
void ES_SELECTIVITY::report_no_stats(Oid relid_oid, Bitmapset* attnums) const
{
    /* We don't save non-analyze multi-column to g_NoAnalyzeRelNameList when resource_track_log=summary. */
    if (u_sess->attr.attr_storage.resource_track_log == SUMMARY || relid_oid == 0)
        return;

    /*
     * We should not save the relation to non-analyze list if is under analyzing,
     * because it will create temp table and execute some query, the temp table
     * don't be analyzed when 2% analyzing.
     */
    if (u_sess->analyze_cxt.is_under_analyze)
        return;

    Assert(bms_num_members(attnums) >= TOW_MEMBERS);
    MemoryContext oldcontext = MemoryContextSwitchTo(t_thrd.mem_cxt.msg_mem_cxt);
    Bitmapset* attnums_tmp = bms_copy(attnums);

    ListCell* lc = NULL;
    bool found = false;
    if (t_thrd.postgres_cxt.g_NoAnalyzeRelNameList != NIL) {
        foreach(lc, t_thrd.postgres_cxt.g_NoAnalyzeRelNameList) {
            List* record = (List*)lfirst(lc);
            if (relid_oid == linitial_oid((List*)linitial(record))) {
                ListCell* sublist = NULL;
                for (sublist = lnext(list_head(record)); sublist != NULL; sublist = lnext(sublist)) {
                    List* attid_list = (List*)lfirst(sublist);
                    if (list_length(attid_list) == bms_num_members(attnums_tmp)) {
                        Bitmapset* attnums_from_list = NULL;
                        ListCell* cell = attid_list->head;
                        while (cell != NULL) {
                            attnums_from_list = bms_add_member(attnums_from_list, (int)lfirst_int(cell));
                            cell = cell->next;
                        }
                        if (bms_equal(attnums_from_list, attnums_tmp))
                            found = true;
                        bms_free_ext(attnums_from_list);
                    }
                }
                if (!found) {
                    List* attid_list = NIL;
                    int attnum = bms_first_member(attnums_tmp);
                    while (attnum != -1) {
                        attid_list = lappend_int(attid_list, attnum);
                        attnum = bms_first_member(attnums_tmp);
                    }
                    bms_free_ext(attnums_tmp);
                    record = lappend(record, attid_list);
                    found = true;
                }
            }
        }
    }

    if (!found) {
        /* add a new rel list */
        List* record = BuildNewRelList(attnums_tmp, relid_oid);

        /* Add a new rel list into g_NoAnalyzeRelNameList. */
        t_thrd.postgres_cxt.g_NoAnalyzeRelNameList = lappend(t_thrd.postgres_cxt.g_NoAnalyzeRelNameList, record);
    }

    (void)MemoryContextSwitchTo(oldcontext);
    return;
}

static bool MatchOnSameSide(RestrictInfo* clause, es_candidate* temp, int leftAttnum, int rightAttnum)
{
    return bms_equal(clause->left_relids, temp->left_relids) &&
            bms_equal(clause->right_relids, temp->right_relids) &&
            !bms_is_member(leftAttnum, temp->left_attnums) && !bms_is_member(rightAttnum, temp->right_attnums);
}

static bool MatchOnOtherSide(RestrictInfo* clause, es_candidate* temp, int leftAttnum, int rightAttnum)
{
    return bms_equal(clause->right_relids, temp->left_relids) &&
            bms_equal(clause->left_relids, temp->right_relids) &&
            !bms_is_member(rightAttnum, temp->left_attnums) &&
            !bms_is_member(leftAttnum, temp->right_attnums);
}

static inline bool AttnumIsInvalid(int leftAttnum, int rightAttnum)
{
    return (leftAttnum < 0 || rightAttnum < 0 || (leftAttnum == 0 && rightAttnum == 0));
}

void ES_SELECTIVITY::add_attnum_for_eqsel(es_candidate* temp, int attnum, Node* arg) const
{
    temp->left_attnums = bms_add_member(temp->left_attnums, attnum);
    add_clause_map(temp, attnum, 0, arg, NULL);
}

/*
 * @brief         read and add attnums to es_candidate
 * @return    true if success
 */
bool ES_SELECTIVITY::add_attnum(RestrictInfo* clause, es_candidate* temp) const
{
    Node* left = NULL;
    Node* right = NULL;
    int left_attnum = 0;
    int right_attnum = 0;

    if (IsA(clause->clause, OpExpr)) {
        OpExpr* opclause = (OpExpr*)clause->clause;
        Assert(list_length(opclause->args) == TOW_MEMBERS);
        left = (Node*)linitial(opclause->args);
        right = (Node*)lsecond(opclause->args);
        left_attnum = read_attnum(left);
        right_attnum = read_attnum(right);
    } else {
        Assert(IsA(clause->clause, NullTest));
        left = (Node*)((NullTest*)clause->clause)->arg;
        left_attnum = read_attnum(left);
    }

    if (AttnumIsInvalid(left_attnum, right_attnum))
        return false;

    switch (temp->tag) {
        case ES_EQSEL:
            /*
             * We wouldn't have clauses like: t1.a = 1 and t1.a = 2 here
             * because optimizor will find the conflict first.
             */
            if (left_attnum > 0 && right_attnum > 0)
                return false;

            if (bms_equal(clause->left_relids, temp->left_relids) ||
                (IsA(clause->clause, NullTest) && bms_equal(clause->clause_relids, temp->left_relids))) {
                add_attnum_for_eqsel(temp, left_attnum, left);
            } else if (bms_equal(clause->right_relids, temp->left_relids)) {
                add_attnum_for_eqsel(temp, right_attnum, right);
            } else
                return false;

            break;
        case ES_EQJOINSEL:
            /*
             * Normally, we shouldn't have clauses like:  t1.a = t2.a and t1.b = t2.a here
             * because clause is generated from equivalence class, in this case,
             * t1.a, t2.a and t1.b is in one equivalence class and will only generate
             * one clause.
             * However, if we try to build an es_candidate using equivalence class such as tpch Q9,
             * there could be some scenarios we have not forseen now.
             * So, for safety, we still check whether the clauses is something
             * like: t1.a = t2.a and t1.b = t2.a
             */
            if (left_attnum == 0 || right_attnum == 0)
                return false;

            if (MatchOnSameSide(clause, temp, left_attnum, right_attnum)) {
                temp->left_attnums = bms_add_member(temp->left_attnums, left_attnum);
                temp->right_attnums = bms_add_member(temp->right_attnums, right_attnum);
                add_clause_map(temp, left_attnum, right_attnum, left, right);
            } else if (MatchOnOtherSide(clause, temp, left_attnum, right_attnum)) {
                temp->left_attnums = bms_add_member(temp->left_attnums, right_attnum);
                temp->right_attnums = bms_add_member(temp->right_attnums, left_attnum);
                add_clause_map(temp, right_attnum, left_attnum, right, left);
            } else
                return false;

            break;
        default:
            /* should not reach here */
            return false;
    }

    return true;
}

/*
 * @brief       add clause_map to es_candidate, clause_map is a map which can be used to find the right arg
 *            using an attnum of left arg in a clause. So we don't have to parse the clause again.
 */
void ES_SELECTIVITY::add_clause_map(
    es_candidate* es, int left_attnum, int right_attnum, Node* left_arg, Node* right_arg) const
{
    es_clause_map* clause_map = (es_clause_map*)palloc(sizeof(es_clause_map));
    clause_map->left_attnum = left_attnum;
    clause_map->right_attnum = right_attnum;
    if (left_arg) {
        if (IsA(left_arg, RelabelType))
            clause_map->left_var = (Var*)((RelabelType*)left_arg)->arg;
        else
            clause_map->left_var = (Var*)left_arg;
    }

    if (right_arg) {
        if (IsA(right_arg, RelabelType))
            clause_map->right_var = (Var*)((RelabelType*)right_arg)->arg;
        else
            clause_map->right_var = (Var*)right_arg;
    }
    es->clause_map = lappend(es->clause_map, clause_map);
    return;
}

/*
 * @brief         read RelOptInfo and RangeTblEntry, save to es_candidate
 * @param    node: should be a Var*
 */
void ES_SELECTIVITY::read_rel_rte(Node* node, RelOptInfo** rel, RangeTblEntry** rte)
{
    Node* basenode = NULL;

    if (IsA(node, RelabelType))
        basenode = (Node*)((RelabelType*)node)->arg;
    else
        basenode = node;

    if (IsA(basenode, Var)) {
        Assert(root != NULL);
        Assert(root->parse != NULL);

        Var* var = (Var*)basenode;
        *rel = find_base_rel(root, var->varno);
        *rte = rt_fetch((*rel)->relid, root->parse->rtable);
    }

    return;
}

void ES_SELECTIVITY::save_selectivity(
    es_candidate* es, double left_join_ratio, double right_join_ratio, bool save_semi_join)
{
    VariableStatData vardata;
    RatioType type;
    if (es->tag == ES_EQSEL)
        type = RatioType_Filter;
    else if (es->tag == ES_EQJOINSEL)
        type = RatioType_Join;
    else
        return;

    if (es->left_rel) {
        vardata.rel = es->left_rel;
        ListCell* lc = NULL;
        foreach(lc, es->clause_map) {
            es_clause_map* map = (es_clause_map*)lfirst(lc);
            vardata.var = (Node*)map->left_var;
            if (!save_semi_join)
                set_varratio_after_calc_selectivity(&vardata, type, left_join_ratio, sjinfo);
            /* Bloom filter can be used only when var = var. */
            if (es->tag == ES_EQJOINSEL) {
                VariableStatData vardata2;
                vardata2.rel = es->right_rel;
                vardata2.var = (Node*)map->right_var;
                /* Set var's ratio which will be used by bloom filter set. */
                set_equal_varratio(&vardata, vardata2.rel->relids, left_join_ratio, sjinfo);
                if (!save_semi_join) {
                    set_equal_varratio(&vardata2, vardata.rel->relids, right_join_ratio, sjinfo);
                    set_varratio_after_calc_selectivity(&vardata2, type, right_join_ratio, sjinfo);
                }
            }
        }
    }
    return;
}

/*
 * @brief       calculate bucket size, actually only save results for estimate_hash_bucketsize to use
 */
void ES_SELECTIVITY::cal_bucket_size(es_candidate* es, es_bucketsize* bucket) const
{
    double tuples;
    RelOptInfo* rel = NULL;
    ListCell* lc = NULL;

    bucket->left_relids = bms_copy(es->left_relids);
    bucket->right_relids = bms_copy(es->right_relids);
    bucket->left_rel = es->left_rel;
    bucket->right_rel = es->right_rel;
    bucket->left_distinct = es->left_stadistinct;
    bucket->right_distinct = es->right_stadistinct;
    bucket->left_mcvfreq = es->left_first_mcvfreq;
    bucket->right_mcvfreq = es->right_first_mcvfreq;

    if (es->left_extended_stats->dndistinct > 0)
        bucket->left_dndistinct = es->left_extended_stats->dndistinct;
    else {
        rel = es->left_rel;
        tuples = get_local_rows(
            rel->tuples, rel->multiple, IsLocatorReplicated(rel->locator_type), ng_get_dest_num_data_nodes(rel));
        bucket->left_dndistinct = clamp_row_est(-1 * es->left_extended_stats->dndistinct * tuples);
    }

    if (es->right_extended_stats->dndistinct > 0)
        bucket->right_dndistinct = es->right_extended_stats->dndistinct;
    else {
        rel = es->right_rel;
        tuples = get_local_rows(
            rel->tuples, rel->multiple, IsLocatorReplicated(rel->locator_type), ng_get_dest_num_data_nodes(rel));
        bucket->right_dndistinct = clamp_row_est(-1 * es->right_extended_stats->dndistinct * tuples);
    }

    bucket->left_hashkeys = NIL;
    bucket->right_hashkeys = NIL;
    foreach(lc, es->clause_map) {
        es_clause_map* map = (es_clause_map*)lfirst(lc);
        bucket->left_hashkeys = lappend(bucket->left_hashkeys, map->left_var);
        bucket->right_hashkeys = lappend(bucket->right_hashkeys, map->right_var);
    }

    return;
}

Selectivity ES_SELECTIVITY::estimate_hash_bucketsize(
    es_bucketsize* es_bucket, double* distinctnum, bool left, Path* inner_path, double nbuckets)
{
    double estfract, ndistinct, mcvfreq, avgfreq;
    RelOptInfo* rel = left ? es_bucket->left_rel : es_bucket->right_rel;
    List* hashkeys = left ? es_bucket->left_hashkeys : es_bucket->right_hashkeys;

    ndistinct = estimate_local_numdistinct(es_bucket, left, inner_path);
    *distinctnum = ndistinct;

    /* Compute avg freq of all distinct data values in raw relation */
    avgfreq = 1.0 / ndistinct;

    /*
     * Initial estimate of bucketsize fraction is 1/nbuckets as long as the
     * number of buckets is less than the expected number of distinct values;
     * otherwise it is 1/ndistinct.
     */
    if (ndistinct > nbuckets)
        estfract = 1.0 / nbuckets;
    else {
        if (ndistinct < 1.0)
            ndistinct = 1.0;
        estfract = 1.0 / ndistinct;
    }

    /*
     * Look up the frequency of the most common value, if available.
     */
    mcvfreq = left ? es_bucket->left_mcvfreq : es_bucket->right_mcvfreq;

    /* We should adjust mcvfreq with selectivity because mcvfreq is changed after joined or joined. */
    mcvfreq /= (rel->rows / rel->tuples);
    ereport(DEBUG1,
        (errmodule(MOD_OPT_JOIN),
            errmsg("[ES]rows=%.lf, tuples=%.lf, multiple=%.lf", rel->rows, rel->tuples, rel->multiple)));

    /*
     * Adjust estimated bucketsize upward to account for skewed distribution.
     */
    if (avgfreq > 0.0 && mcvfreq > avgfreq) {
        /* if hashkey contains distribute key, mcv freq should be multiplied by dn number */
        double multiple = 1.0;
        /* for now, only consider one distribute key situation */
        if (list_length(hashkeys) >= list_length(rel->distribute_keys) &&
            list_is_subset(rel->distribute_keys, hashkeys))
            multiple = u_sess->pgxc_cxt.NumDataNodes;

        estfract *= mcvfreq / avgfreq;
        /* if adjusted selectivity is larger than mcvfreq, then the estimate is too far off,
        take the mcvfreq instead. */
        if (estfract > mcvfreq * multiple)
            estfract = mcvfreq * multiple;
    }

    ereport(DEBUG1,
        (errmodule(MOD_OPT_JOIN),
            errmsg("[ES]ndistinct=%.lf, avgfreq=%.10f, mcvfreq=%.10f, estfract=%.10f",
                ndistinct,
                avgfreq,
                mcvfreq,
                estfract)));

    /*
     * Clamp bucketsize to sane range (the above adjustment could easily
     * produce an out-of-range result).  We set the lower bound a little above
     * zero, since zero isn't a very sane result.
     * We should adjust the lower bound as 1.0e-7 because the distinct value
     * may be larger than 1000000 as the increasing of work_mem.
     */
    if (estfract < 1.0e-7)
        estfract = 1.0e-7;
    else if (estfract > 1.0) {
        if (mcvfreq > 0.0)
            estfract = mcvfreq;
        else
            estfract = 1.0;
    }

    return (Selectivity)estfract;
}

/*
 * @brief       mostly as same as estimate_local_numdistinct
 */
double ES_SELECTIVITY::estimate_local_numdistinct(es_bucketsize* bucket, bool left, Path* pathnode)
{
    VariableStatData vardata;
    bool usesinglestats = true;
    double ndistinct = left ? bucket->left_dndistinct : bucket->right_dndistinct;
    double global_distinct = left ? bucket->left_distinct : bucket->right_distinct;
    unsigned int num_datanodes = ng_get_dest_num_data_nodes(pathnode);
    List* hashkeys = left ? bucket->left_hashkeys : bucket->right_hashkeys;

    vardata.rel = left ? bucket->left_rel : bucket->right_rel;

    /* we should adjust local distinct if there is no tuples in dn1 for global stats. */
    if ((ndistinct * num_datanodes) < global_distinct)
        ndistinct = get_local_rows(global_distinct, vardata.rel->multiple, false, num_datanodes);

    /* Adjust global distinct values for STREAM_BROADCAST and STREAM_REDISTRIBUTE. */
    if (IsA(pathnode, StreamPath) || IsA(pathnode, HashPath) || IsLocatorReplicated(pathnode->locator_type)) {
        ndistinct =
            estimate_hash_num_distinct(root, hashkeys, pathnode, &vardata, ndistinct, global_distinct, &usesinglestats);
    }

    /*
     * Adjust ndistinct to account for restriction clauses.  Observe we are
     * assuming that the data distribution is affected uniformly by the
     * restriction clauses!
     *
     * XXX Possibly better way, but much more expensive: multiply by
     * selectivity of rel's restriction clauses that mention the target Var.
     *
     * Only single stat need multiple the ratio as rows/tuples, because
     * possion for global stat.
     * Else if we have use possion, we don't need multiple the ratio.
     */
    return ndistinct;
}

Selectivity ES_SELECTIVITY::cal_eqjoinsel(es_candidate* es, JoinType jointype)
{
    Selectivity result = 1.0;
    RelOptInfo* inner_rel = NULL;
    bool inner_on_left = false;
    switch (jointype) {
        case JOIN_INNER:
        case JOIN_LEFT:
        case JOIN_FULL:
            result *= cal_eqjoinsel_inner(es);
            break;
        case JOIN_SEMI:
        case JOIN_ANTI:
        case JOIN_LEFT_ANTI_FULL:
            /*
                * Look up the join's inner relation.  min_righthand is sufficient
                * information because neither SEMI nor ANTI joins permit any
                * reassociation into or out of their RHS, so the righthand will
                * always be exactly that set of rels.
                */
            inner_rel = find_join_input_rel(root, sjinfo->min_righthand);

            /* inner_rel could be a join rel */
            inner_on_left = (bms_is_subset(es->left_rel->relids, inner_rel->relids));
            if (!inner_on_left) {
                Assert(bms_is_subset(es->right_rel->relids, inner_rel->relids));
            }
            result *= cal_eqjoinsel_semi(es, inner_rel, inner_on_left);
            break;
        default:
            /* other values not expected here */
            break;
    }
    return result;
}

/*
 * @brief       calculate selectivity for eqsel using multi-column statistics
 */
Selectivity ES_SELECTIVITY::cal_eqsel(es_candidate* es)
{
    Selectivity result = 1.0;
    ListCell* lc = NULL;
    int i = 0;
    int j = 0;
    int column_count = 0;
    bool match = false;

    Assert(es->left_extended_stats);

    /* if all clauses are null, just use nullfrac */
    if (es->has_null_clause) {
        foreach(lc, es->clause_group) {
            RestrictInfo* rinfo = (RestrictInfo*)lfirst(lc);
            if (!IsA(rinfo->clause, NullTest))
                break;
        }
        if (lc == NULL) {
            result = es->left_extended_stats->nullfrac;
            CLAMP_PROBABILITY(result);
            save_selectivity(es, result, 0.0);
            return result;
        }
    }

    /* if there is no MCV, just use distinct */
    if (!es->left_extended_stats->mcv_values) {
        result = (result - es->left_extended_stats->nullfrac) / es->left_stadistinct;
        CLAMP_PROBABILITY(result);
        save_selectivity(es, result, 0.0);
        ereport(ES_DEBUG_LEVEL,
            (errmodule(MOD_OPT),
                (errmsg("[ES]extended statistic is used to calculate eqsel selectivity as %e", result))));
        return result;
    }

    /* try to use MCV */
    column_count = es->clause_group->length;

    Assert(column_count == bms_num_members(es->left_extended_stats->bms_attnum));

    /* set up attnum order */
    int* attnum_order = (int*)palloc(column_count * sizeof(int));
    set_up_attnum_order(es, attnum_order, true);

    Assert(es->left_extended_stats->mcv_nvalues / column_count == es->left_extended_stats->mcv_nnumbers);

    /* match MCV with const value from clauses */
    double sum_mcv_numbers = 0.0;
    for (i = 0; i < es->left_extended_stats->mcv_nnumbers; i++) {
        match = false;
        j = 0;
        /* process clause one by one */
        foreach(lc, es->clause_group) {
            FmgrInfo eqproc;
            Datum const_value;
            bool var_on_left = false;

            /* set up eqproc */
            RestrictInfo* clause = (RestrictInfo*)lfirst(lc);
            int mcv_position = attnum_order[j] * es->left_extended_stats->mcv_nnumbers + i;

            if (IsA(clause->clause, OpExpr)) {
                OpExpr* opclause = (OpExpr*)clause->clause;
                Oid opno = opclause->opno;
                fmgr_info(get_opcode(opno), &eqproc);

                /* set up const value */
                Node* left = (Node*)linitial(opclause->args);
                Node* right = (Node*)lsecond(opclause->args);
                if (IsA(left, Const)) {
                    const_value = ((Const*)left)->constvalue;
                    var_on_left = false;
                } else if (IsA(right, Const)) {
                    const_value = ((Const*)right)->constvalue;
                    var_on_left = true;
                }

                Datum mcv_value = es->left_extended_stats->mcv_values[mcv_position];
                if (var_on_left)
                    match = DatumGetBool(FunctionCall2Coll(&eqproc, DEFAULT_COLLATION_OID, mcv_value, const_value));
                else
                    match = DatumGetBool(FunctionCall2Coll(&eqproc, DEFAULT_COLLATION_OID, const_value, mcv_value));
            } else {
                Assert(IsA(clause->clause, NullTest));
                match = es->left_extended_stats->mcv_nulls[mcv_position];
            }

            if (!match)
                break;
            j++;
        }

        if (match) {
            result = es->left_extended_stats->mcv_numbers[i];
            break;
        } else
            sum_mcv_numbers += es->left_extended_stats->mcv_numbers[i];
    }

    if (!match) {
        double sum_other_mcv_numbers = 0.0;
        for (int index = 0; index < es->left_extended_stats->other_mcv_nnumbers; index++)
            sum_other_mcv_numbers += es->left_extended_stats->other_mcv_numbers[index];
        result = 1.0 - sum_mcv_numbers - sum_other_mcv_numbers - es->left_extended_stats->nullfrac;
        CLAMP_PROBABILITY(result);
        float4 other_distinct = clamp_row_est(es->left_stadistinct - es->left_extended_stats->mcv_nnumbers);
        result /= other_distinct;

        /*
         * Another cross-check: selectivity shouldn't be estimated as more
         * than the least common "most common value".
         */
        int last_mcv_member = es->left_extended_stats->mcv_nnumbers - 1;
        float4 least_common_value = es->left_extended_stats->mcv_numbers[last_mcv_member];
        if (result > least_common_value)
            result = least_common_value;
    }

    pfree_ext(attnum_order);

    CLAMP_PROBABILITY(result);
    save_selectivity(es, result, 0.0);
    ereport(ES_DEBUG_LEVEL,
        (errmodule(MOD_OPT), (errmsg("[ES]extended statistic is used to calculate eqsel selectivity as %e", result))));
    return result;
}

/*
 * @brief        calculate selectivity for join using multi-column statistics
 */
Selectivity ES_SELECTIVITY::cal_eqjoinsel_inner(es_candidate* es)
{
    Assert(es->left_extended_stats);
    Selectivity result = 1.0;
    int i;

    /* update nullfrac to contain null mcv fraction */
    for (i = 0; i < es->left_extended_stats->other_mcv_nnumbers; i++)
        es->left_extended_stats->nullfrac += es->left_extended_stats->other_mcv_numbers[i];
    for (i = 0; i < es->right_extended_stats->other_mcv_nnumbers; i++)
        es->right_extended_stats->nullfrac += es->right_extended_stats->other_mcv_numbers[i];

    /* if there is no MCV, just use distinct */
    if (!es->left_extended_stats->mcv_values || !es->right_extended_stats->mcv_values) {
        result *= (1.0 - es->left_extended_stats->nullfrac) * (1.0 - es->right_extended_stats->nullfrac);
        result /= es->left_stadistinct > es->right_stadistinct ? es->left_stadistinct : es->right_stadistinct;
        CLAMP_PROBABILITY(result);
        double left_ratio = es->right_stadistinct / es->left_stadistinct * (1.0 - es->left_extended_stats->nullfrac);
        double right_ratio = es->left_stadistinct / es->right_stadistinct * (1.0 - es->right_extended_stats->nullfrac);
        save_selectivity(es, left_ratio, right_ratio);
        ereport(ES_DEBUG_LEVEL,
            (errmodule(MOD_OPT),
                (errmsg("[ES]extended statistic is used to calculate eqjoinsel_inner selectivity as %e", result))));
        return result;
    }

    /* try to use MCV */
    int column_count = es->clause_group->length;
    Assert(column_count == bms_num_members(es->left_extended_stats->bms_attnum));

    /* set up attnum order */
    int* left_attnum_order = (int*)palloc(column_count * sizeof(int));
    int* right_attnum_order = (int*)palloc(column_count * sizeof(int));
    set_up_attnum_order(es, left_attnum_order, true);
    set_up_attnum_order(es, right_attnum_order, false);

    ListCell* lc = NULL;
    FmgrInfo* eqproc = (FmgrInfo*)palloc(column_count * sizeof(FmgrInfo));
    bool* left_var_on_clause_leftside = (bool*)palloc(column_count * sizeof(bool));
    i = 0;
    foreach(lc, es->clause_group) {
        /* set up eqproc */
        RestrictInfo* clause = (RestrictInfo*)lfirst(lc);
        OpExpr* opclause = (OpExpr*)clause->clause;
        Oid opno = opclause->opno;
        fmgr_info(get_opcode(opno), &(eqproc[i]));

        /* set up left_var_on_clause_leftside */
        if (bms_equal(clause->left_relids, es->left_relids))
            left_var_on_clause_leftside[i] = true;
        else
            left_var_on_clause_leftside[i] = false;

        i++;
    }

    /* prepare to match MCVs */
    double left_relfrac = es->left_rel->rows / es->left_rel->tuples;
    double right_relfrac = es->right_rel->rows / es->right_rel->tuples;
    CLAMP_PROBABILITY(left_relfrac);
    CLAMP_PROBABILITY(right_relfrac);
    double relfrac = left_relfrac * right_relfrac;

    int left_mcv_nums = es->left_extended_stats->mcv_nnumbers;
    int right_mcv_nums = es->right_extended_stats->mcv_nnumbers;
    bool* left_match = (bool*)palloc0(left_mcv_nums * sizeof(bool));
    bool* right_match = (bool*)palloc0(right_mcv_nums * sizeof(bool));

    /*
     * The calculation logic here is as same as that in eqjoinsel_inner:
     * Note we assume that each MCV will match at most one member of the
     * other MCV list.    If the operator isn't really equality, there could
     * be multiple matches --- but we don't look for them, both for speed
     * and because the math wouldn't add up...
     */
    double matchprodfreq = 0.0;
    int nmatches = 0;
    for (i = 0; i < left_mcv_nums; i++) {
        int j;

        for (j = 0; j < right_mcv_nums; j++) {
            if (right_match[j])
                continue;
            bool all_match = false;
            int k = 0;
            /* process clause one by one */
            foreach(lc, es->clause_group) {
                int left_mcv_position = left_attnum_order[k] * left_mcv_nums + i;
                int right_mcv_position = right_attnum_order[k] * right_mcv_nums + j;
                Datum left_mcv_value = es->left_extended_stats->mcv_values[left_mcv_position];
                Datum right_mcv_value = es->right_extended_stats->mcv_values[right_mcv_position];
                if (left_var_on_clause_leftside[k])
                    all_match = DatumGetBool(
                        FunctionCall2Coll(&(eqproc[k]), DEFAULT_COLLATION_OID, left_mcv_value, right_mcv_value));
                else
                    all_match = DatumGetBool(
                        FunctionCall2Coll(&(eqproc[k]), DEFAULT_COLLATION_OID, right_mcv_value, left_mcv_value));
                if (!all_match)
                    break;
                k++;
            }

            if (all_match) {
                left_match[i] = right_match[j] = true;
                matchprodfreq += es->left_extended_stats->mcv_numbers[i] * es->right_extended_stats->mcv_numbers[j];
                nmatches++;
                break;
            }
        }
    }

    /* adjust match freq according to relation's filter fraction */
    double left_nullfrac = es->left_extended_stats->nullfrac;
    double right_nullfrac = es->right_extended_stats->nullfrac;
    double left_matchfreq, right_matchfreq, left_unmatchfreq, right_unmatchfreq, left_otherfreq, right_otherfreq,
        left_totalsel, right_totalsel;
    int tmp_nmatches = (int)ceil((double)nmatches * relfrac);
    if (nmatches != 0)
        matchprodfreq *= (double)tmp_nmatches / nmatches;
    CLAMP_PROBABILITY(matchprodfreq);
    /* Sum up frequencies of matched and unmatched MCVs */
    left_matchfreq = left_unmatchfreq = 0.0;
    for (i = 0; i < left_mcv_nums; i++) {
        if (left_match[i])
            left_matchfreq += es->left_extended_stats->mcv_numbers[i];
        else
            left_unmatchfreq += es->left_extended_stats->mcv_numbers[i];
    }
    CLAMP_PROBABILITY(left_matchfreq);
    CLAMP_PROBABILITY(left_unmatchfreq);
    right_matchfreq = right_unmatchfreq = 0.0;
    for (i = 0; i < right_mcv_nums; i++) {
        if (right_match[i])
            right_matchfreq += es->right_extended_stats->mcv_numbers[i];
        else
            right_unmatchfreq += es->right_extended_stats->mcv_numbers[i];
    }
    CLAMP_PROBABILITY(right_matchfreq);
    CLAMP_PROBABILITY(right_unmatchfreq);
    pfree_ext(left_match);
    pfree_ext(right_match);
    pfree_ext(left_attnum_order);
    pfree_ext(right_attnum_order);

    /*
     * Compute total frequency of non-null values that are not in the MCV
     * lists.
     */
    left_otherfreq = 1.0 - left_nullfrac - left_matchfreq - left_unmatchfreq;
    right_otherfreq = 1.0 - right_nullfrac - right_matchfreq - right_unmatchfreq;
    CLAMP_PROBABILITY(left_otherfreq);
    CLAMP_PROBABILITY(right_otherfreq);

    /*
     * We can estimate the total selectivity from the point of view of
     * relation 1 as: the known selectivity for matched MCVs, plus
     * unmatched MCVs that are assumed to match against random members of
     * relation 2's non-MCV population, plus non-MCV values that are
     * assumed to match against random members of relation 2's unmatched
     * MCVs plus non-MCV values.
     */
    int left_nvalues_frac = (int)ceil((double)left_mcv_nums * left_relfrac);
    int right_nvalues_frac = (int)ceil((double)right_mcv_nums * right_relfrac);
    left_totalsel = matchprodfreq;
    if (es->right_extended_stats->distinct > right_nvalues_frac)
        left_totalsel +=
            left_unmatchfreq * right_otherfreq / (es->right_extended_stats->distinct - right_nvalues_frac) * relfrac;
    if (es->right_extended_stats->distinct > tmp_nmatches)
        left_totalsel += left_otherfreq * (right_otherfreq + right_unmatchfreq) /
                         (es->right_extended_stats->distinct - tmp_nmatches) * relfrac;
    /* Same estimate from the point of view of relation 2. */
    right_totalsel = matchprodfreq;
    if (es->left_extended_stats->distinct > left_nvalues_frac)
        right_totalsel +=
            right_unmatchfreq * left_otherfreq / (es->left_extended_stats->distinct - left_nvalues_frac) * relfrac;
    if (es->left_extended_stats->distinct > tmp_nmatches)
        right_totalsel += right_otherfreq * (left_otherfreq + left_unmatchfreq) /
                          (es->left_extended_stats->distinct - tmp_nmatches) * relfrac;

    /*
     * Use the smaller of the two estimates.  This can be justified in
     * essentially the same terms as given below for the no-stats case: to
     * a first approximation, we are estimating from the point of view of
     * the relation with smaller nd.
     */
    if (relfrac == 0)
        result = 0;
    else
        result = (left_totalsel < right_totalsel) ? left_totalsel / relfrac : right_totalsel / relfrac;

    /*
     * calculate join ratio for both two tables, admitting that smaller distinct
     * values will be all joined out
     */
    double left_join_ratio = 0.0;
    double right_join_ratio = 0.0;

    if (nmatches != 0 && left_relfrac != 0 && right_relfrac != 0) {
        left_join_ratio = right_matchfreq * tmp_nmatches / (nmatches * left_relfrac);
        right_join_ratio = right_matchfreq * tmp_nmatches / (nmatches * right_relfrac);
    }

    if (es->left_extended_stats->distinct > es->right_extended_stats->distinct) {
        if (es->left_extended_stats->distinct != tmp_nmatches) {
            left_join_ratio += left_otherfreq * (es->right_extended_stats->distinct - tmp_nmatches) /
                               (es->left_extended_stats->distinct - tmp_nmatches);
        }
        right_join_ratio += right_otherfreq;
    } else if (es->left_extended_stats->distinct < es->right_extended_stats->distinct) {
        if (es->right_extended_stats->distinct != tmp_nmatches) {
            right_join_ratio += right_otherfreq * (es->left_extended_stats->distinct - tmp_nmatches) /
                                (es->right_extended_stats->distinct - tmp_nmatches);
        }
        left_join_ratio += left_otherfreq;
    }
    CLAMP_PROBABILITY(left_join_ratio);
    CLAMP_PROBABILITY(right_join_ratio);
    CLAMP_PROBABILITY(result);

    save_selectivity(es, left_join_ratio, right_join_ratio);
    ereport(ES_DEBUG_LEVEL,
        (errmodule(MOD_OPT),
            (errmsg("[ES]extended statistic is used to calculate eqjoinsel selectivity as %e", result))));

    /* save mcv freq to calculate skew or hash bucket size */
    es->left_first_mcvfreq = es->left_extended_stats->mcv_numbers[0];
    es->right_first_mcvfreq = es->right_extended_stats->mcv_numbers[0];

    return result;
}

Selectivity ES_SELECTIVITY::cal_eqjoinsel_semi(es_candidate* es, RelOptInfo* inner_rel, bool inner_on_left)
{
    Assert(es->left_extended_stats);
    Selectivity result = 1.0;
    double nullfrac = inner_on_left ? es->right_extended_stats->nullfrac : es->left_extended_stats->nullfrac;
    double inner_distinct = inner_on_left ? es->left_stadistinct : es->right_stadistinct;
    double outer_distinct = inner_on_left ? es->right_stadistinct : es->left_stadistinct;

    /*
     * Clamp inner_distinct to be not more than what we estimate the inner relation's
     * size to be, especially when inner_rel can be a joined rel.
     */
    inner_distinct = Min(inner_distinct, inner_rel->rows);

    /* if there is no MCV, just use distinct */
    if (!es->left_extended_stats->mcv_values || !es->right_extended_stats->mcv_values) {
        result *= (1.0 - nullfrac);
        if (inner_distinct < outer_distinct)
            result *= inner_distinct / outer_distinct;
    } else {
        /* try to use MCV */
        int column_count = es->clause_group->length;
        Assert(column_count == bms_num_members(es->left_extended_stats->bms_attnum));

        /* set up attnum order */
        int* left_attnum_order = (int*)palloc(column_count * sizeof(int));
        int* right_attnum_order = (int*)palloc(column_count * sizeof(int));
        set_up_attnum_order(es, left_attnum_order, true);
        set_up_attnum_order(es, right_attnum_order, false);

        ListCell* lc = NULL;
        FmgrInfo* eqproc = (FmgrInfo*)palloc(column_count * sizeof(FmgrInfo));
        bool* left_var_on_clause_leftside = (bool*)palloc(column_count * sizeof(bool));
        int i = 0;
        foreach(lc, es->clause_group) {
            /* set up eqproc */
            RestrictInfo* clause = (RestrictInfo*)lfirst(lc);
            OpExpr* opclause = (OpExpr*)clause->clause;
            Oid opno = opclause->opno;
            fmgr_info(get_opcode(opno), &(eqproc[i]));

            /* set up left_var_on_clause_leftside */
            if (bms_equal(clause->left_relids, es->left_relids))
                left_var_on_clause_leftside[i] = true;
            else
                left_var_on_clause_leftside[i] = false;

            i++;
        }

        /* prepare to match MCVs */
        int left_mcv_nums = es->left_extended_stats->mcv_nnumbers;
        int right_mcv_nums = es->right_extended_stats->mcv_nnumbers;
        bool* left_match = (bool*)palloc0(left_mcv_nums * sizeof(bool));
        bool* right_match = (bool*)palloc0(right_mcv_nums * sizeof(bool));

        /*
         * The calculation logic here is as same as that in eqjoinsel_inner:
         * Note we assume that each MCV will match at most one member of the
         * other MCV list.    If the operator isn't really equality, there could
         * be multiple matches --- but we don't look for them, both for speed
         * and because the math wouldn't add up...
         */
        double matchprodfreq = 0.0;
        int nmatches = 0;
        for (i = 0; i < left_mcv_nums; i++) {
            int j;

            for (j = 0; j < right_mcv_nums; j++) {
                if (right_match[j])
                    continue;
                bool all_match = false;
                int k = 0;
                /* process clause one by one */
                foreach(lc, es->clause_group) {
                    int left_mcv_position = left_attnum_order[k] * left_mcv_nums + i;
                    int right_mcv_position = right_attnum_order[k] * right_mcv_nums + j;
                    Datum left_mcv_value = es->left_extended_stats->mcv_values[left_mcv_position];
                    Datum right_mcv_value = es->right_extended_stats->mcv_values[right_mcv_position];
                    if (left_var_on_clause_leftside[k])
                        all_match = DatumGetBool(
                            FunctionCall2Coll(&(eqproc[k]), DEFAULT_COLLATION_OID, left_mcv_value, right_mcv_value));
                    else
                        all_match = DatumGetBool(
                            FunctionCall2Coll(&(eqproc[k]), DEFAULT_COLLATION_OID, right_mcv_value, left_mcv_value));
                    if (!all_match)
                        break;
                    k++;
                }

                if (all_match) {
                    left_match[i] = right_match[j] = true;
                    nmatches++;
                    break;
                }
            }
        }

        pfree_ext(left_attnum_order);
        pfree_ext(right_attnum_order);
        matchprodfreq = 1.0;
        int mcv_num = inner_on_left ? left_mcv_nums : right_mcv_nums;
        for (i = 0; i < mcv_num; i++) {
            if (inner_on_left && right_match[i])
                matchprodfreq += es->right_extended_stats->mcv_numbers[i];
            else if (!inner_on_left && left_match[i])
                matchprodfreq += es->left_extended_stats->mcv_numbers[i];
        }
        CLAMP_PROBABILITY(matchprodfreq);

        /*
         * Now we need to estimate the fraction of relation 1 that has at
         * least one join partner.    We know for certain that the matched MCVs
         * do, so that gives us a lower bound, but we're really in the dark
         * about everything else.  Our crude approach is: if nd1 <= nd2 then
         * assume all non-null rel1 rows have join partners, else assume for
         * the uncertain rows that a fraction nd2/nd1 have join partners. We
         * can discount the known-matched MCVs from the distinct-values counts
         * before doing the division.
         *
         * Crude as the above is, it's completely useless if we don't have
         * reliable ndistinct values for both sides.  Hence, if either nd1 or
         * nd2 is default, punt and assume half of the uncertain rows have
         * join partners.
         */
        inner_distinct -= nmatches;
        outer_distinct -= nmatches;
        double uncertainfrac, uncertain;
        if (inner_distinct >= outer_distinct || inner_distinct < 0)
            uncertainfrac = 1.0;
        else
            uncertainfrac = inner_distinct / outer_distinct;
        uncertain = 1.0 - matchprodfreq - nullfrac;
        CLAMP_PROBABILITY(uncertain);
        result = matchprodfreq + uncertainfrac * uncertain;
    }

    CLAMP_PROBABILITY(result);
    save_selectivity(es, result, 0.0, true);
    ereport(ES_DEBUG_LEVEL,
        (errmodule(MOD_OPT),
            (errmsg("[ES]extended statistic is used to calculate eqjoinsel_semi selectivity as %e", result))));
    return result;
}

/*
 * @brief       calculate distinct for groupby using multi-column statistics,
 *              in order to use the result in estimate_num_groups(),
 *             build a varinfo and save it in unmatched_clause_group.
 */
void ES_SELECTIVITY::build_pseudo_varinfo(es_candidate* es, STATS_EST_TYPE eType)
{
    /* build pseudo varinfo here with multi-column distinct */
    GroupVarInfo* varinfo = (GroupVarInfo*)palloc(sizeof(GroupVarInfo));
    varinfo->var = NULL;
    varinfo->rel = es->left_rel;
    if (eType == STATS_TYPE_GLOBAL)
        varinfo->ndistinct = es->left_stadistinct;
    else {
        /* get local distinct */
        if (es->left_extended_stats->dndistinct < 0) {
            double ntuples = get_local_rows(es->left_rel->tuples,
                es->left_rel->multiple,
                IsLocatorReplicated(es->left_rel->locator_type),
                ng_get_dest_num_data_nodes(es->left_rel));
            varinfo->ndistinct = clamp_row_est(
                -1 * es->left_extended_stats->dndistinct * ntuples * (1.0 - es->left_extended_stats->nullfrac));
        } else
            varinfo->ndistinct = clamp_row_est(es->left_extended_stats->dndistinct);
    }
    varinfo->isdefault = false;
    varinfo->es_is_used = true;
    varinfo->es_attnums = bms_copy(es->left_attnums);
    unmatched_clause_group = lappend(unmatched_clause_group, varinfo);
    ereport(ES_DEBUG_LEVEL,
        (errmodule(MOD_OPT),
            (errmsg("[ES]extended statistic is used to calculate groupby distinct as %f", varinfo->ndistinct))));

    return;
}

/*
 * @brief       delete an invalid es_candidate, the memory will be free in clear().
 */
void ES_SELECTIVITY::remove_candidate(es_candidate* es)
{
    switch (es->tag) {
        case ES_EQSEL:
        case ES_EQJOINSEL:
        case ES_GROUPBY:
            unmatched_clause_group = list_concat(unmatched_clause_group, es->clause_group);
            break;
        default:
            break;
    }
    es->tag = ES_EMPTY;
    es->clause_group = NULL;

    return;
}

/*
 * @brief       remove corresponding stuff from the left_attnums of es_candidate according dump attnum,
 *              dump attnum should be delete outside this function.
 */
void ES_SELECTIVITY::remove_attnum(es_candidate* es, int dump_attnum)
{
    /* delete clause and clause_map according to dump */
    ListCell* lc_clause = list_head(es->clause_group);
    ListCell* lc_clause_map = NULL;
    ListCell* prev_clause = NULL;
    ListCell* prev_clause_map = NULL;
    foreach(lc_clause_map, es->clause_map) {
        es_clause_map* clause_map = (es_clause_map*)lfirst(lc_clause_map);

        if (clause_map->left_attnum == dump_attnum) {
            if (es->tag == ES_EQJOINSEL) {
                Assert(bms_is_member(clause_map->right_attnum, es->right_attnums));
                es->right_attnums = bms_del_member(es->right_attnums, clause_map->right_attnum);
            }

            /* if found, delete clause map from the list */
            pfree_ext(clause_map);
            lfirst(lc_clause_map) = NULL;
            es->clause_map = list_delete_cell(es->clause_map, lc_clause_map, prev_clause_map);

            /* delete clause from list and add to unmatch list */
            unmatched_clause_group = lappend(unmatched_clause_group, lfirst(lc_clause));
            lfirst(lc_clause) = NULL;
            es->clause_group = list_delete_cell(es->clause_group, lc_clause, prev_clause);

            /* no need to continue, just try to find next matched stats */
            break;
        } else {
            prev_clause = lc_clause;
            prev_clause_map = lc_clause_map;
            lc_clause = lc_clause->next;
        }
    }

    return;
}

/*
 * @brief       Set up attnum order according to clause map, so that we can use this order
 *              to locate the corresponding clause when we go through the bitmap of attnums.
 *                We need this order because the clause is ordered by its position in clauselist and mcv
 *              in extended stats are ordered by attnum.
 *              left == true : set up left attnum order ; left == false : set up right attnum order .
 */
void ES_SELECTIVITY::set_up_attnum_order(es_candidate* es, int* attnum_order, bool left) const
{
    int i;
    int j = 0;
    ListCell* lc = NULL;
    foreach(lc, es->clause_map) {
        i = 0;
        int attnum = 0;
        es_clause_map* clause_map = (es_clause_map*)lfirst(lc);
        Bitmapset* tmpset = left ? bms_copy(es->left_attnums) : bms_copy(es->right_attnums);
        while ((attnum = bms_first_member(tmpset)) >= 0) {
            if ((left && attnum == clause_map->left_attnum) || (!left && attnum == clause_map->right_attnum)) {
                attnum_order[j] = i;
                break;
            }
            i++;
        }
        pfree_ext(tmpset);
        j++;
    }
    return;
}

/*
 * @brief        print debug info including ES type, involving rels and clauses
 */
void ES_SELECTIVITY::debug_print()
{
    if (log_min_messages > ES_DEBUG_LEVEL)
        return;
    ListCell* l = NULL;
    foreach(l, es_candidate_list) {
        es_candidate* temp = (es_candidate*)lfirst(l);
        ereport(DEBUG1,
            (errmodule(MOD_OPT_JOIN),
                errmsg("[ES]ES_TYPE = %d (0:empty; 1:eqsel; 2:eqjoinsel; 3:group by)", (int)temp->tag)));
        if (temp->tag == ES_EMPTY)
            continue;

        print_relids(temp->relids, "All rels:");

        if (temp->left_rte) {
            print_relids(temp->left_relids, "Left rels:");
            print_relids(temp->left_attnums, "Left attnums:");
            print_rel(temp->left_rte);
        }

        if (temp->right_rte) {
            print_relids(temp->right_relids, "Right rels:");
            print_relids(temp->right_attnums, "Right attnums:");
            print_rel(temp->right_rte);
        }

        switch (temp->tag) {
            case ES_EQSEL:
            case ES_EQJOINSEL:
                print_clauses(temp->clause_group);
                break;
            default:
                break;
        }
    }
    return;
}

void ES_SELECTIVITY::print_rel(RangeTblEntry* rel) const
{
    StringInfoData buf;
    initStringInfo(&buf);
    appendStringInfo(&buf, "%s, relkind: %c, inheritance or not:%d", rel->relname, rel->relkind, (int)rel->inh);
    ereport(DEBUG1, (errmodule(MOD_OPT_JOIN), (errmsg("[ES]%s", buf.data))));
    pfree_ext(buf.data);
    return;
}

void ES_SELECTIVITY::print_relids(Bitmapset* relids, const char* str) const
{
    StringInfoData buf;
    initStringInfo(&buf);
    Relids tmprelids = bms_copy(relids);
    int x;
    appendStringInfoString(&buf, str);
    while ((x = bms_first_member(tmprelids)) >= 0) {
        appendStringInfo(&buf, "%d, ", x);
    }
    bms_free_ext(tmprelids);
    ereport(DEBUG1, (errmodule(MOD_OPT_JOIN), (errmsg("[ES]%s", buf.data))));
    pfree_ext(buf.data);
    return;
}

void ES_SELECTIVITY::print_clauses(List* clauses) const
{
    if (root == NULL || list_length(clauses) == 0)
        return;

    ListCell* l = NULL;
    StringInfoData buf;
    initStringInfo(&buf);

    appendStringInfo(&buf, "Clause length:%d, clause list:", list_length(clauses));

    foreach(l, clauses) {
        RestrictInfo* c = (RestrictInfo*)lfirst(l);
        char* expr = print_expr((Node*)c->clause, root->parse->rtable);
        appendStringInfoString(&buf, expr);
        pfree_ext(expr);
        if (lnext(l))
            appendStringInfoString(&buf, ", ");
    }

    ereport(DEBUG1, (errmodule(MOD_OPT_JOIN), errmsg("[ES]%s", buf.data)));

    pfree_ext(buf.data);

    return;
}

char* ES_SELECTIVITY::print_expr(const Node* expr, const List* rtable) const
{
    return ExprToString(expr, rtable);
}
