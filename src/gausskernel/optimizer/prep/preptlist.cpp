/* -------------------------------------------------------------------------
 *
 * preptlist.cpp
 *	  Routines to preprocess the parse tree target list
 *
 * For INSERT and UPDATE queries, the targetlist must contain an entry for
 * each attribute of the target relation in the correct order.	For all query
 * types, we may need to add junk tlist entries for Vars used in the RETURNING
 * list and row ID information needed for EvalPlanQual checking.
 *
 * NOTE: the rewriter's rewriteTargetListIU and rewriteTargetListUD
 * routines also do preprocessing of the targetlist.  The division of labor
 * between here and there is a bit arbitrary and historical.
 *
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/gausskernel/optimizer/prep/preptlist.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/heapam.h"
#include "access/sysattr.h"
#include "catalog/pg_type.h"
#include "foreign/fdwapi.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/prep.h"
#include "optimizer/streamplan.h"
#include "optimizer/tlist.h"
#include "parser/parsetree.h"
#include "parser/parse_coerce.h"
#include "parser/parse_merge.h"
#include "pgxc/pgxc.h"
#include "rewrite/rewriteHandler.h"
#include "tcop/utility.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"

static List* expand_targetlist(List* tlist, int command_type, Index result_relation, List* range_table);
#ifdef STREAMPLAN
static List* add_distribute_column(List* tlist, Index result_relation, List* range_table);
#endif

/*
 * preprocess_targetlist
 *	  Driver for preprocessing the parse tree targetlist.
 *
 *	  Returns the new targetlist.
 */
List* preprocess_targetlist(PlannerInfo* root, List* tlist)
{
    Query* parse = root->parse;
    int result_relation = parse->resultRelation;
    List* range_table = parse->rtable;
    CmdType command_type = parse->commandType;
    ListCell* lc = NULL;
    RangeTblEntry* rte = NULL;

    /*
     * Sanity check: if there is a result relation, it'd better be a real
     * relation not a subquery.  Else parser or rewriter messed up.
     */
    if (result_relation) {
        rte = rt_fetch(result_relation, range_table);

        if (rte->subquery != NULL || rte->relid == InvalidOid)
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                    (errmsg("subquery cannot be result relation"))));
    }

    /*
     * for heap_form_tuple to work, the targetlist must match the exact order
     * of the attributes. We also need to fill in any missing attributes. -ay
     * 10/94
     */
    if (command_type == CMD_INSERT || command_type == CMD_UPDATE)
        tlist = expand_targetlist(tlist, command_type, result_relation, range_table);
#ifdef STREAMPLAN
    else if (IS_STREAM_PLAN && command_type == CMD_DELETE)
        tlist = add_distribute_column(tlist, result_relation, range_table);
#endif

    if (command_type == CMD_MERGE) {
        ListCell* l = NULL;
        bool is_qual_expanded = false;

        /*
         * For MERGE, add any junk column(s) needed to allow the executor to
         * identify the rows to be updated or deleted, with different
         * handling for partitioned tables.
         */
        rewriteTargetListMerge(parse, result_relation, range_table);

        /*
         * For MERGE command, handle targetlist of each MergeAction separately.
         * Give the same treatment to MergeAction->targetList as we would have
         * given to a regular INSERT/UPDATE/DELETE.
         */
        foreach (l, parse->mergeActionList) {
            MergeAction* action = (MergeAction*)lfirst(l);

            switch (action->commandType) {
                case CMD_INSERT:
                case CMD_UPDATE:
                    action->targetList =
                        expand_targetlist(action->targetList, action->commandType, result_relation, range_table);
                    if (action->pulluped_targetList != NIL)
                        action->pulluped_targetList = expand_targetlist(
                            action->pulluped_targetList, action->commandType, result_relation, range_table);

                    break;
                case CMD_DELETE:
                    break;
                case CMD_NOTHING:
                    break;
                default:
                    ereport(ERROR, ((errcode(ERRCODE_CASE_NOT_FOUND), errmsg("unknown action in MERGE WHEN clause"))));
            }
            /* are there any quals to handle the quals for pgxc */
            if (!IS_STREAM_PLAN && action->qual != NULL && !IS_SINGLE_NODE) {
                if (!is_qual_expanded) {
                    /* expand vars in action's qual to make sure we can later reference it */
                    parse->targetList = expandQualTL(parse->targetList, parse);
                    /* we only expand once */
                    is_qual_expanded = true;
                }
                /* make action's qual and targetlist refer to plan's targetlist */
                action->qual = substitute_var(action->qual, parse->targetList);
            }
        }

        if (!IS_PGXC_DATANODE && !IS_STREAM_PLAN && !IS_SINGLE_NODE) {
            /* add action's targetlist in the query targetlist */
            parse->targetList = expandActionTL(parse->targetList, parse);
        }
    }

    /*
     * Add necessary junk columns for rowmarked rels.  These values are needed
     * for locking of rels selected FOR UPDATE/SHARE, and to do EvalPlanQual
     * rechecking.	See comments for PlanRowMark in plannodes.h.
     */
    foreach (lc, root->rowMarks) {
        PlanRowMark* rc = (PlanRowMark*)lfirst(lc);
        Var* var = NULL;
        char resname[32]; /* 32 is the max length unique identifier for resjunk columns */
        TargetEntry* tle = NULL;
        RangeTblEntry* rte = NULL;
        int ret = 0;

        /* child rels use the same junk attrs as their parents */
        if (rc->rti != rc->prti)
            continue;
        rte = rt_fetch(rc->rti, range_table);

        if (rc->markType != ROW_MARK_COPY) {
            /* It's a regular table, so fetch its TID */
            var = makeVar(rc->rti, SelfItemPointerAttributeNumber, TIDOID, -1, InvalidOid, 0);
            ret = snprintf_s(resname, sizeof(resname), sizeof(resname) - 1, "ctid%u", rc->rowmarkId);
            securec_check_ss(ret, "", "");

            tle = makeTargetEntry((Expr*)var, list_length(tlist) + 1, pstrdup(resname), true);
            tlist = lappend(tlist, tle);

            /* if parent of inheritance tree, need the tableoid too */
            bool need_oid = rc->isParent || rte->ispartrel || rte->orientation == REL_COL_ORIENTED;
            if (need_oid) {
                var = makeVar(rc->rti, TableOidAttributeNumber, OIDOID, -1, InvalidOid, 0);
                ret = snprintf_s(resname, sizeof(resname), sizeof(resname) - 1, "tableoid%u", rc->rowmarkId);
                securec_check_ss(ret, "", "");
                tle = makeTargetEntry((Expr*)var, list_length(tlist) + 1, pstrdup(resname), true);
                tlist = lappend(tlist, tle);
            }
            if (rte->relhasbucket) {
                var = makeVar(rc->rti, BucketIdAttributeNumber, INT2OID, -1, InvalidBktId, 0);
                ret = snprintf_s(resname, sizeof(resname), sizeof(resname) - 1, "tablebucketid%u", rc->rowmarkId);
                securec_check_ss(ret, "", "");
                tle = makeTargetEntry((Expr*)var, list_length(tlist) + 1, pstrdup(resname), true);
                tlist = lappend(tlist, tle);
            }

            /*
             * Add table's distribute columns to targetlist for FOR UPDATE/SHARE
             * in stream plan.
             */
            if (IS_STREAM_PLAN && (rc->markType == ROW_MARK_EXCLUSIVE || rc->markType == ROW_MARK_SHARE)) {
                tlist = add_distribute_column(tlist, rc->rti, range_table);
            }
        } else {
            bool is_hdfs_ftbl = false;
            List* sub_tlist = NIL;
            if (rte->relkind == RELKIND_FOREIGN_TABLE || rte->relkind == RELKIND_STREAM) {
                if (isObsOrHdfsTableFormTblOid(rte->relid) || IS_OBS_CSV_TXT_FOREIGN_TABLE(rte->relid)) {
                    is_hdfs_ftbl = true;
                }
            } else if (rte->rtekind == RTE_SUBQUERY && (IS_STREAM_PLAN || IS_SINGLE_NODE)) {
                sub_tlist = rte->subquery->targetList;
            }

            if (!is_hdfs_ftbl && sub_tlist == NIL) {
                /* Not a table, so we need the whole row as a junk var */
                var = makeWholeRowVar(rt_fetch(rc->rti, range_table), rc->rti, 0, false);

                if (var == NULL) {
                    ereport(ERROR,(errcode(ERRCODE_UNDEFINED_FILE),
                            errmsg("could not get the whole row to build a junk var.")));
                    return NULL;
                }

                int rcode = snprintf_s(resname, sizeof(resname), sizeof(resname) - 1, "wholerow%u", rc->rowmarkId);
                securec_check_ss(rcode, "\0", "\0");
                tle = makeTargetEntry((Expr*)var, list_length(tlist) + 1, pstrdup(resname), true);
                tlist = lappend(tlist, tle);
            } else {
                /* Convert wholerow(table.*) to table.col1, table.col2, ...., table.coln. */
                int varattno = 0;
                Relation rel = NULL;
                int maxattrs;
                TupleDesc tupdesc = NULL;
                ListCell* lc2 = NULL;

                if (is_hdfs_ftbl) {
                    rel = relation_open(rte->relid, AccessShareLock);
                    tupdesc = rel->rd_att;
                    maxattrs = tupdesc->natts;
                } else {
                    maxattrs = list_length(sub_tlist);
                    lc2 = list_head(sub_tlist);
                }

                for (varattno = 0; varattno < maxattrs; varattno++) {
                    Oid vartype = InvalidOid;
                    int32 vartypmod = -1;
                    Oid varCollid = InvalidOid;
                    if (is_hdfs_ftbl) {
                        Form_pg_attribute attr = tupdesc->attrs[varattno];
                        vartype = attr->atttypid;
                        varCollid = attr->attcollation;
                        vartypmod = attr->atttypmod;
                    } else {
                        if (varattno != 0)
                            lc2 = lnext(lc2);
                        TargetEntry* tleLocal = (TargetEntry*)lfirst(lc2);
                        Expr* expr = tleLocal->expr;
                        if (tleLocal->resjunk)
                            continue;
                        vartype = exprType((Node*)expr);
                        vartypmod = exprTypmod((Node*)expr);
                        varCollid = exprCollation((Node*)expr);
                    }
                    /* set var collation to ensure var equation when finding sort pathkey. */
                    var = makeVar(rc->rti, varattno + 1, vartype, vartypmod, varCollid, 0);

                    ret = snprintf_s(resname, sizeof(resname), sizeof(resname) - 1, "wholerow%u", rc->rowmarkId);
                    securec_check_ss(ret, "\0", "\0");
                    tle = makeTargetEntry((Expr*)var, list_length(tlist) + 1, pstrdup(resname), true);

                    tlist = lappend(tlist, tle);
                }

                rc->markType = ROW_MARK_COPY_DATUM;
                rc->numAttrs = maxattrs;
                if (is_hdfs_ftbl)
                    relation_close(rel, AccessShareLock);
            }
#ifdef STREAMPLAN
            if (IS_STREAM_PLAN && var->vartype == RECORDOID) {
                if (rte->relname != NULL) {
                    errno_t sprintf_rc = sprintf_s(u_sess->opt_cxt.not_shipping_info->not_shipping_reason,
                        NOTPLANSHIPPING_LENGTH,
                        "Type of Record in %s that is not a real table can not be shipped",
                        rte->relname);
                    securec_check_ss_c(sprintf_rc, "\0", "\0");
                } else {
                    errno_t sprintf_rc = sprintf_s(u_sess->opt_cxt.not_shipping_info->not_shipping_reason,
                        NOTPLANSHIPPING_LENGTH,
                        "Type of Record in non-real table can not be shipped");
                    securec_check_ss_c(sprintf_rc, "\0", "\0");
                }
                mark_stream_unsupport();
            }
#endif
        }
    }

    /*
     * If the query has a RETURNING list, add resjunk entries for any Vars
     * used in RETURNING that belong to other relations.  We need to do this
     * to make these Vars available for the RETURNING calculation.	Vars that
     * belong to the result rel don't need to be added, because they will be
     * made to refer to the actual heap tuple.
     */
    if (parse->returningList && list_length(parse->rtable) > 1) {
        List* vars = NIL;
        ListCell* l = NULL;

        vars = pull_var_clause((Node*)parse->returningList, PVC_RECURSE_AGGREGATES, PVC_INCLUDE_PLACEHOLDERS);
        foreach (l, vars) {
            Var* var = (Var*)lfirst(l);
            TargetEntry* tle = NULL;

            if (IsA(var, Var) && var->varno == (unsigned int)result_relation)
                continue; /* don't need it */

            if (tlist_member((Node*)var, tlist))
                continue; /* already got it */

            tle = makeTargetEntry((Expr*)var, list_length(tlist) + 1, NULL, true);

            tlist = lappend(tlist, tle);
        }
        list_free_ext(vars);
    }

    return tlist;
}

/*****************************************************************************
 *
 *		TARGETLIST EXPANSION
 *
 *****************************************************************************/
/*
 * expand_targetlist
 *    Given a target list as generated by the parser and a result relation,
 *    add targetlist entries for any missing attributes, and ensure the
 *    non-junk attributes appear in proper field order.
 */
static List* expand_targetlist(List* tlist, int command_type, Index result_relation, List* range_table)
{
    List* new_tlist = NIL;
    ListCell* tlist_item = NULL;
    Relation rel;
    int attrno, numattrs;
    int hash_col_cnt = 0;
    bool is_ledger_rel = false;

    tlist_item = list_head(tlist);

    /*
     * The rewriter should have already ensured that the TLEs are in correct
     * order; but we have to insert TLEs for any missing attributes.
     *
     * Scan the tuple description in the relation's relcache entry to make
     * sure we have all the user attributes in the right order.  We assume
     * that the rewriter already acquired at least AccessShareLock on the
     * relation, so we need no lock here.
     */
    rel = heap_open(getrelid(result_relation, range_table), NoLock);

    is_ledger_rel = rel->rd_isblockchain;

    is_ledger_rel = rel->rd_isblockchain;

    numattrs = RelationGetNumberOfAttributes(rel);

    for (attrno = 1; attrno <= numattrs; attrno++) {
        Form_pg_attribute att_tup = rel->rd_att->attrs[attrno - 1];
        TargetEntry* new_tle = NULL;

        if (tlist_item != NULL) {
            TargetEntry* old_tle = (TargetEntry*)lfirst(tlist_item);

            if (!old_tle->resjunk && old_tle->resno == attrno) {
                new_tle = old_tle;
                tlist_item = lnext(tlist_item);
            }
        }

        if (new_tle == NULL) {
            /*
             * Didn't find a matching tlist entry, so make one.
             *
             * For INSERT, generate a NULL constant.  (We assume the rewriter
             * would have inserted any available default value.) Also, if the
             * column isn't dropped, apply any domain constraints that might
             * exist --- this is to catch domain NOT NULL.
             *
             * For UPDATE, generate a Var reference to the existing value of
             * the attribute, so that it gets copied to the new tuple. But
             * generate a NULL for dropped columns (we want to drop any old
             * values).
             *
             * When generating a NULL constant for a dropped column, we label
             * it INT4 (any other guaranteed-to-exist datatype would do as
             * well). We can't label it with the dropped column's datatype
             * since that might not exist anymore.	It does not really matter
             * what we claim the type is, since NULL is NULL --- its
             * representation is datatype-independent.	This could perhaps
             * confuse code comparing the finished plan to the target
             * relation, however.
             */
            Oid atttype = att_tup->atttypid;
            int32 atttypmod = att_tup->atttypmod;
            Oid attcollation = att_tup->attcollation;
            Node* new_expr = NULL;
            if (is_ledger_rel && strcmp(NameStr(att_tup->attname), "hash") == 0) {
                hash_col_cnt = 1;
                continue;
            }
            switch (command_type) {
                case CMD_INSERT:
                    if (!att_tup->attisdropped) {
                        new_expr = (Node*)makeConst(atttype,
                            -1,
                            attcollation,
                            att_tup->attlen,
                            (Datum)0,
                            true, /* isnull */
                            att_tup->attbyval);
                        new_expr =
                            coerce_to_domain(new_expr, InvalidOid, -1, atttype, COERCE_IMPLICIT_CAST, -1, false, false);
                    } else {
                        /* Insert NULL for dropped column */
                        new_expr = (Node*)makeConst(INT4OID,
                            -1,
                            InvalidOid,
                            sizeof(int32),
                            (Datum)0,
                            true, /* isnull */
                            true /* byval */);
                    }
                    break;
                case CMD_MERGE:
                case CMD_UPDATE:
                    if (!att_tup->attisdropped) {
                        new_expr = (Node*)makeVar(result_relation, attrno - hash_col_cnt, atttype,
                                                  atttypmod, attcollation, 0);
                    } else {
                        /* Insert NULL for dropped column */
                        new_expr = (Node*)makeConst(INT4OID,
                            -1,
                            InvalidOid,
                            sizeof(int32),
                            (Datum)0,
                            true, /* isnull */
                            true /* byval */);
                    }
                    break;
                default:
                    ereport(ERROR,
                        (errmodule(MOD_OPT),
                            errcode(ERRCODE_DATA_EXCEPTION),
                            (errmsg("unrecognized command_type: %d", (int)command_type))));
                    new_expr = NULL; /* keep compiler quiet */
                    break;
            }

            new_tle = makeTargetEntry((Expr*)new_expr, attrno, pstrdup(NameStr(att_tup->attname)), false);
        }

        new_tlist = lappend(new_tlist, new_tle);
    }

    attrno -= hash_col_cnt;
    /*
     * The remaining tlist entries should be resjunk; append them all to the
     * end of the new tlist, making sure they have resnos higher than the last
     * real attribute.	(Note: although the rewriter already did such
     * renumbering, we have to do it again here in case we are doing an UPDATE
     * in a table with dropped columns, or an inheritance child table with
     * extra columns.)
     */
    while (tlist_item != NULL) {
        TargetEntry* old_tle = (TargetEntry*)lfirst(tlist_item);

        if (!old_tle->resjunk)
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                    (errmsg("targetlist is not sorted correctly"))));
        /* Get the resno right, but don't copy unnecessarily */
        if (old_tle->resno != attrno) {
            old_tle = flatCopyTargetEntry(old_tle);
            old_tle->resno = attrno;
        }
        new_tlist = lappend(new_tlist, old_tle);
        attrno++;
        tlist_item = lnext(tlist_item);
    }

    heap_close(rel, NoLock);

    return new_tlist;
}

/*
 * Locate PlanRowMark for given RT index, or return NULL if none
 *
 * This probably ought to be elsewhere, but there's no very good place
 */
PlanRowMark* get_plan_rowmark(List* rowmarks, Index rtindex)
{
    ListCell* l = NULL;

    foreach (l, rowmarks) {
        PlanRowMark* rc = (PlanRowMark*)lfirst(l);

        if (rc->rti == rtindex)
            return rc;
    }
    return NULL;
}

#ifdef STREAMPLAN
/*
 * add_distribute_column
 *	  Given a target list as generated by the parser and a result relation,
 *	  add distribute entries to the target list if the attribute is missing, and
 *	  ensure it appears in the final target list for stream distribution.
 */
static List* add_distribute_column(List* tlist, Index result_relation, List* range_table)
{
    Relation rel = heap_open(getrelid(result_relation, range_table), NoLock);
    int2vector* disattrno = NULL;
    ListCell* lc = NULL;

    if (rel->rd_rel->relkind == RELKIND_RELATION)
        disattrno = get_baserel_distributekey_no(getrelid(result_relation, range_table));

    if (disattrno == NULL) {
        heap_close(rel, NoLock);
        return tlist;
    }

    int no = 0;
    int len = disattrno->dim1;
    bool* isExist = (bool*)palloc0(len * sizeof(bool));

    for (int i = 0; i < len; i++) {
        no = disattrno->values[i];
        foreach (lc, tlist) {
            TargetEntry* tle = (TargetEntry*)lfirst(lc);
            Expr* node = tle->expr;

            if (IsA(node, Var) && ((Var*)node)->varno == result_relation && ((Var*)node)->varattno == no) {
                isExist[i] = true;
                break;
            }
        }
    }

    /* if distribute column is not in targetlist, then add it */
    for (int i = 0; i < len; i++) {
        if (isExist[i] == false) {
            no = disattrno->values[i];
            Form_pg_attribute att_tup = rel->rd_att->attrs[no - 1];
            Node* new_expr =
                (Node*)makeVar(result_relation, no, att_tup->atttypid, att_tup->atttypmod, att_tup->attcollation, 0);
            TargetEntry* tle =
                makeTargetEntry((Expr*)new_expr, list_length(tlist) + 1, pstrdup(NameStr(att_tup->attname)), true);
            tlist = lappend(tlist, tle);
        }
    }

    heap_close(rel, NoLock);

    return tlist;
}

List* preprocess_upsert_targetlist(List* tlist, int result_relation, List* range_table)
{
    return expand_targetlist(tlist, CMD_UPDATE, result_relation, range_table);
}
#endif

