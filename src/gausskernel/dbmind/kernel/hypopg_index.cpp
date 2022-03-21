/* -------------------------------------------------------------------------
 *
 * hypopg_index.cpp: Implementation of hypothetical indexes for openGauss
 *
 * This file contains all the internal code related to hypothetical indexes
 * support.
 *
 * This program is open source, licensed under the PostgreSQL license.
 * For license terms, see the LICENSE file.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Copyright (C) 2015-2018: Julien Rouhaud
 *
 *
 * IDENTIFICATION
 * 	  src/gausskernel/dbmind/kernel/hypopg_index.cpp
 *
 * -------------------------------------------------------------------------
 */
#include <unistd.h>
#include <math.h>
#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "access/gist.h"
#include "access/nbtree.h"
#include "access/reloptions.h"
#include "access/spgist.h"
#include "access/spgist_private.h"
#include "access/sysattr.h"
#include "access/xlog.h"
#include "catalog/namespace.h"
#include "catalog/pg_am.h"
#include "catalog/pg_amproc.h"
#include "catalog/pg_class.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "dbmind/hypopg_index.h"
#include "optimizer/clauses.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/plancat.h"
#include "optimizer/var.h"
#include "parser/parse_utilcmd.h"
#include "parser/parser.h"
#include "storage/buf/bufmgr.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"

#define HYPO_INDEX_NB_COLS 4     /* # of column hypopg() returns */
#define HYPO_INDEX_CREATE_COLS 2 /* # of column hypopg_create_index() */
#define HYPO_BTMaxItemSize MAXALIGN_DOWN(         \
        (BLCKSZ - MAXALIGN(SizeOfPageHeaderData + 3 * sizeof(ItemIdData)) - MAXALIGN(sizeof(BTPageOpaqueData))) / 3)
#define isExplain (u_sess->attr.attr_sql.hypopg_is_explain)

static ProcessUtility_hook_type prev_utility_hook = NULL;
static ExecutorEnd_hook_type prev_ExecutorEnd_hook = NULL;
static get_relation_info_hook_type prev_get_relation_info_hook = NULL;
static explain_get_index_name_hook_type prev_explain_get_index_name_hook = NULL;

extern Oid GetIndexOpClass(List *opclass, Oid attrType, const char *accessMethodName, Oid accessMethodId);
extern void CheckPredicate(Expr *predicate);
extern bool CheckMutability(Expr *expr);
static void hypo_utility_hook(Node *parsetree, const char *queryString, ParamListInfo params, bool isTopLevel,
    DestReceiver *dest, bool sentToRemote, char *completionTag, bool isCtas);
static void hypo_executorEnd_hook(QueryDesc *queryDesc);
static void hypo_get_relation_info_hook(PlannerInfo *root, Oid relationObjectId, bool inhparent, RelOptInfo *rel);
static const char *hypo_explain_get_index_name_hook(Oid indexId);
static Oid hypo_getNewOid(Oid relid);
static bool hypo_index_match_table(hypoIndex *entry, Oid relid);
static bool hypo_query_walker(Node *node);
static void hypo_addIndex(hypoIndex *entry);
static bool hypo_can_return(hypoIndex *entry, Oid atttype, int i, char *amname);
static void hypo_estimate_index_simple(hypoIndex *entry, BlockNumber *pages, double *tuples);
static void hypo_estimate_index(hypoIndex *entry, RelOptInfo *rel);
static int hypo_estimate_index_colsize(hypoIndex *entry, int col);
static void hypo_index_pfree(hypoIndex *entry);
static bool hypo_index_remove(Oid indexid);
static void hypo_handle_predicate(IndexStmt *node, hypoIndex *volatile entry);
static void hypo_process_attr(IndexStmt *node, hypoIndex *volatile entry, StringInfoData &indexRelationName, Oid relid,
    int &ind_avg_width);
static const hypoIndex *hypo_index_store_parsetree(IndexStmt *node, const char *queryString);
static hypoIndex *hypo_newIndex(Oid relid, char *accessMethod, int nkeycolumns, int ninccolumns, List *options);
static void hypo_set_indexname(hypoIndex *entry, const char *indexname);
static void hypo_index_reset(void);
static void hypo_injectHypotheticalIndex(PlannerInfo *root, Oid relationObjectId, bool inhparent, RelOptInfo *rel,
    Relation relation, hypoIndex *entry);
static List *get_table_indexes(Oid oid);
static List *get_index_attrnum(Oid oid);

void InitHypopg()
{
    // init memory context
    if (g_instance.hypo_cxt.HypopgContext == NULL) {
        g_instance.hypo_cxt.HypopgContext = AllocSetContextCreate(g_instance.instance_context, "HypopgContext", 
            ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE, SHARED_CONTEXT);
    }
    isExplain = false;
}

/*
 * This function is used for setting prev_utility_hook to rewrite
 * standard_ProcessUtility by extension.
 */
void set_hypopg_prehook(ProcessUtility_hook_type func)
{
    prev_utility_hook = func;
}

void hypopg_register_hook()
{
    // register hooks
    set_hypopg_prehook(ProcessUtility_hook);
    ProcessUtility_hook = hypo_utility_hook;

    prev_ExecutorEnd_hook = ExecutorEnd_hook;
    ExecutorEnd_hook = hypo_executorEnd_hook;

    prev_get_relation_info_hook = get_relation_info_hook;
    get_relation_info_hook = hypo_get_relation_info_hook;

    prev_explain_get_index_name_hook = explain_get_index_name_hook;
    explain_get_index_name_hook = hypo_explain_get_index_name_hook;
}

/* ---------------------------------
 * Wrapper around GetNewRelFileNode
 * Return a new OID for an hypothetical index.
 */
static Oid hypo_getNewOid(Oid relid)
{
    Relation pg_class;
    Relation relation;
    Oid newoid;
    Oid reltablespace;
    char relpersistence;

    /* Open the relation on which we want a new OID */
    relation = heap_open(relid, AccessShareLock);

    reltablespace = relation->rd_rel->reltablespace;
    relpersistence = relation->rd_rel->relpersistence;

    /* Close the relation and release the lock now */
    heap_close(relation, AccessShareLock);

    /* Open pg_class to aks a new OID */
    pg_class = heap_open(RelationRelationId, RowExclusiveLock);

    /* ask for a new relfilenode */
    newoid = GetNewRelFileNode(reltablespace, pg_class, relpersistence);

    /* Close pg_class and release the lock now */
    heap_close(pg_class, RowExclusiveLock);

    return newoid;
}

/* This function setup the "isExplain" flag for next hooks.
 * If this flag is setup, we can add hypothetical indexes.
 */
void hypo_utility_hook(Node *parsetree, const char *queryString, ParamListInfo params, bool isTopLevel,
    DestReceiver *dest, bool sentToRemote, char *completionTag, bool isCtas)
{
    isExplain = query_or_expression_tree_walker(parsetree, (bool (*)())hypo_query_walker, NULL, 0);

    if (prev_utility_hook) {
        prev_utility_hook(parsetree, queryString, params, isTopLevel, dest, sentToRemote, completionTag, isCtas);
    } else {
        standard_ProcessUtility(parsetree, queryString, params, isTopLevel, dest, sentToRemote, completionTag, isCtas);
    }
}

static bool hypo_index_match_table(hypoIndex *entry, Oid relid)
{
    /* Hypothetical index on the exact same relation, use it. */
    if (entry->relid == relid) {
        return true;
    }

    return false;
}

/* Detect if the current utility command is compatible with hypothetical indexes
 * i.e. an EXPLAIN, no ANALYZE
 */
static bool hypo_query_walker(Node *parsetree)
{
    if (parsetree == NULL) {
        return false;
    }
    if (nodeTag(parsetree) == T_ExplainStmt) {
        ListCell *lc;

        foreach (lc, ((ExplainStmt *)parsetree)->options) {
            DefElem *opt = (DefElem *)lfirst(lc);

            if (strcmp(opt->defname, "analyze") == 0)
                return false;
        }
        return true;
    }
    return false;
}

/* Reset the isExplain flag after each query */
static void hypo_executorEnd_hook(QueryDesc *queryDesc)
{
    isExplain = false;

    if (prev_ExecutorEnd_hook) {
        prev_ExecutorEnd_hook(queryDesc);
    } else {
        standard_ExecutorEnd(queryDesc);
    }
}
List *get_table_indexes(Oid oid)
{
    Relation rel = heap_open(oid, NoLock);
    List *indexes = RelationGetIndexList(rel);
    heap_close(rel, NoLock);
    return indexes;
}

/* Return the names of all the columns involved in the index. */
List *get_index_attrnum(Oid index_oid)
{
    HeapTuple index_tup = SearchSysCache1(INDEXRELID, ObjectIdGetDatum(index_oid));
    if (!HeapTupleIsValid(index_tup))
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("cache lookup failed for index %u", index_oid)));

    Form_pg_index index_form = (Form_pg_index)GETSTRUCT(index_tup);
    int2vector *attnums = &(index_form->indkey);

    // get attrnum from table oid.
    List *attrnum = NIL;
    int i;
    for (i = 0; i < attnums->dim1; i++) {
        attrnum = lappend_int(attrnum, attnums->values[i]);
    }

    ReleaseSysCache(index_tup);
    return attrnum;
}

/*
 * This function will execute the "hypo_injectHypotheticalIndex" for every
 * hypothetical index found for each relation if the isExplain flag is setup.
 */
static void hypo_get_relation_info_hook(PlannerInfo *root, Oid relationObjectId, bool inhparent, RelOptInfo *rel)
{
    if (u_sess->attr.attr_sql.enable_hypo_index && isExplain) {
        Relation relation;

        /* Open the current relation */
        relation = heap_open(relationObjectId, AccessShareLock);

        if (relation->rd_rel->relkind == RELKIND_RELATION) {
            ListCell *lc;

            LWLockAcquire(HypoIndexLock, LW_SHARED);

            foreach (lc, g_instance.hypo_cxt.hypo_index_list) {
                hypoIndex *entry = (hypoIndex *)lfirst(lc);

                if (hypo_index_match_table(entry, RelationGetRelid(relation))) {
                    /*
                     * hypothetical index found, add it to the relation's
                     * indextlist
                     */
                    List *indexes = get_table_indexes(entry->relid);
                    ListCell *index = NULL;
                    bool match_flag = false;
                    foreach (index, indexes) {
                        List *attrnums = get_index_attrnum(lfirst_oid(index));
                        if (attrnums == NIL) {
                            break;
                        }
                        if (entry->ncolumns > attrnums->length) {
                            continue;
                        }
                        match_flag = true;
                        for (int i = 0; i < entry->ncolumns; i++) {
                            if (entry->indexkeys[i] != list_nth_int(attrnums, i)) {
                                match_flag = false;
                                break;
                            }
                        }
                        // the suggested index has existed
                        if (match_flag) {
                            break;
                        }
                    }
                    if (!match_flag) {
                        hypo_injectHypotheticalIndex(root, relationObjectId, inhparent, rel, relation, entry);
                    }
                }
            }

            LWLockRelease(HypoIndexLock);
        }

        /* Close the relation release the lock now */
        heap_close(relation, AccessShareLock);
    }

    if (prev_get_relation_info_hook)
        prev_get_relation_info_hook(root, relationObjectId, inhparent, rel);
}

/*
 * palloc a new hypoIndex, and give it a new OID, and some other global stuff.
 * This function also parse index storage options (if any) to check if they're
 * valid.
 */
static hypoIndex *hypo_newIndex(Oid relid, char *accessMethod, int nkeycolumns, int ninccolumns, List *options)
{
    /* must be declared "volatile", because used in a PG_CATCH() */
    hypoIndex *volatile entry;
    MemoryContext oldcontext;
    HeapTuple tuple;
    Oid oid;
    RegProcedure amoptions;

    tuple = SearchSysCache1(AMNAME, PointerGetDatum(accessMethod));

    if (!HeapTupleIsValid(tuple)) {
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("hypopg: access method \"%s\" does not exist", accessMethod)));
    }

    oid = HeapTupleGetOid(tuple);

    oldcontext = MemoryContextSwitchTo(g_instance.hypo_cxt.HypopgContext);

    entry = (hypoIndex *)palloc0(sizeof(hypoIndex));

    entry->relam = oid;

    /* Up to 9.5, all information is available in the pg_am tuple */
    entry->amcostestimate = ((Form_pg_am)GETSTRUCT(tuple))->amcostestimate;
    entry->amcanreturn = ((Form_pg_am)GETSTRUCT(tuple))->amcanreturn;
    entry->amcanorderbyop = ((Form_pg_am)GETSTRUCT(tuple))->amcanorderbyop;
    entry->amoptionalkey = ((Form_pg_am)GETSTRUCT(tuple))->amoptionalkey;
    entry->amsearcharray = ((Form_pg_am)GETSTRUCT(tuple))->amsearcharray;
    entry->amsearchnulls = ((Form_pg_am)GETSTRUCT(tuple))->amsearchnulls;
    entry->amhasgettuple = OidIsValid(((Form_pg_am)GETSTRUCT(tuple))->amgettuple);
    entry->amhasgetbitmap = OidIsValid(((Form_pg_am)GETSTRUCT(tuple))->amgetbitmap);
    entry->amcanunique = ((Form_pg_am)GETSTRUCT(tuple))->amcanunique;
    entry->amcanmulticol = ((Form_pg_am)GETSTRUCT(tuple))->amcanmulticol;
    amoptions = ((Form_pg_am)GETSTRUCT(tuple))->amoptions;
    entry->amcanorder = ((Form_pg_am)GETSTRUCT(tuple))->amcanorder;

    ReleaseSysCache(tuple);
    entry->indexname = (char *)palloc0(NAMEDATALEN);
    /* palloc all arrays */
    entry->indexkeys = (short int *)palloc0(sizeof(short int) * (nkeycolumns + ninccolumns));
    entry->indexcollations = (Oid *)palloc0(sizeof(Oid) * nkeycolumns);
    entry->opfamily = (Oid *)palloc0(sizeof(Oid) * nkeycolumns);
    entry->opclass = (Oid *)palloc0(sizeof(Oid) * nkeycolumns);
    entry->opcintype = (Oid *)palloc0(sizeof(Oid) * nkeycolumns);
    /* only palloc sort related fields if needed */
    if (OID_IS_BTREE(entry->relam) || (entry->amcanorder)) {
        if (!OID_IS_BTREE(entry->relam)) {
            entry->sortopfamily = (Oid *)palloc0(sizeof(Oid) * nkeycolumns);
        }
        entry->reverse_sort = (bool *)palloc0(sizeof(bool) * nkeycolumns);
        entry->nulls_first = (bool *)palloc0(sizeof(bool) * nkeycolumns);
    } else {
        entry->sortopfamily = NULL;
        entry->reverse_sort = NULL;
        entry->nulls_first = NULL;
    }
    entry->indexprs = NIL;
    entry->indpred = NIL;
    entry->options = (List *)copyObject(options);

    MemoryContextSwitchTo(oldcontext);

    entry->oid = hypo_getNewOid(relid);
    entry->relid = relid;
    entry->immediate = true;

    if (options != NIL) {
        Datum reloptions;

        /*
         * Parse AM-specific options, convert to text array form, validate.
         */
        reloptions = transformRelOptions((Datum)0, options, NULL, NULL, false, false);

        (void)index_reloptions(amoptions, reloptions, true);
    }

    PG_TRY();
    {
        /*
         * reject unsupported am. It could be done earlier but it's simpler
         * (and was previously done) here.
         */
        if (!OID_IS_BTREE(entry->relam)) {
            /*
             * do not store hypothetical indexes with access method not
             * supported
             */
            elog(ERROR, "hypopg: access method \"%s\" is not supported", accessMethod);
            break;
        }

        /* No more elog beyond this point. */
    }
    PG_CATCH();
    {
        /* Free what was palloc'd in HypoMemoryContext */
        hypo_index_pfree(entry);

        PG_RE_THROW();
    }
    PG_END_TRY();

    return entry;
}

/* Add an hypoIndex to hypo_index_list */
static void hypo_addIndex(hypoIndex *entry)
{
    MemoryContext oldcontext;

    oldcontext = MemoryContextSwitchTo(g_instance.hypo_cxt.HypopgContext);

    LWLockAcquire(HypoIndexLock, LW_EXCLUSIVE);

    g_instance.hypo_cxt.hypo_index_list = lappend(g_instance.hypo_cxt.hypo_index_list, entry);

    LWLockRelease(HypoIndexLock);

    MemoryContextSwitchTo(oldcontext);
}

/*
 * Remove cleanly all hypothetical indexes by calling hypo_index_remove() on
 * each entry. hypo_index_remove() function pfree all allocated memory
 */
void hypo_index_reset(void)
{
    ListCell *lc;

    LWLockAcquire(HypoIndexLock, LW_EXCLUSIVE);

    while ((lc = list_head(g_instance.hypo_cxt.hypo_index_list)) != NULL) {
        hypoIndex *entry = (hypoIndex *)lfirst(lc);

        g_instance.hypo_cxt.hypo_index_list = list_delete_ptr(g_instance.hypo_cxt.hypo_index_list, entry);
        hypo_index_pfree(entry);
    }

    list_free(g_instance.hypo_cxt.hypo_index_list);
    g_instance.hypo_cxt.hypo_index_list = NIL;

    LWLockRelease(HypoIndexLock);

    return;
}

static void hypo_handle_predicate(IndexStmt *node, hypoIndex *volatile entry)
{
    if (node->whereClause) {
        MemoryContext oldcontext;
        List *pred;

        CheckPredicate((Expr *)node->whereClause);

        pred = make_ands_implicit((Expr *)node->whereClause);
        oldcontext = MemoryContextSwitchTo(g_instance.hypo_cxt.HypopgContext);

        entry->indpred = (List *)copyObject(pred);
        MemoryContextSwitchTo(oldcontext);
    } else {
        entry->indpred = NIL;
    }
}

static void hypo_process_attr(IndexStmt *node, hypoIndex *volatile entry, StringInfoData &indexRelationName, Oid relid,
    int &ind_avg_width)
{
    Form_pg_attribute attform;
    HeapTuple tuple;
    ListCell *lc;
    int attn;

    attn = 0;
    foreach (lc, node->indexParams) {
        IndexElem *attribute = (IndexElem *)lfirst(lc);
        Oid atttype = InvalidOid;
        Oid opclass;

        appendStringInfo(&indexRelationName, "_");

        /*
         * Process the column-or-expression to be indexed.
         */
        if (attribute->name != NULL) {
            /* Simple index attribute */
            appendStringInfo(&indexRelationName, "%s", attribute->name);
            /* get the attribute catalog info */
            tuple = SearchSysCacheAttName(relid, attribute->name);

            if (!HeapTupleIsValid(tuple)) {
                elog(ERROR, "hypopg: column \"%s\" does not exist", attribute->name);
            }
            attform = (Form_pg_attribute)GETSTRUCT(tuple);

            /* setup the attnum */
            entry->indexkeys[attn] = attform->attnum;

            /* setup the collation */
            entry->indexcollations[attn] = attform->attcollation;

            /* get the atttype */
            atttype = attform->atttypid;

            ReleaseSysCache(tuple);
        } else {
            /* ---------------------------
             * handle index on expression
             *
             * Adapted from DefineIndex() and ComputeIndexAttrs()
             *
             * Statistics on expression index will be really wrong, since
             * they're only computed when a real index exists (selectivity
             * and average width).
             */
            MemoryContext oldcontext;
            Node *expr = attribute->expr;

            Assert(expr != NULL);
            entry->indexcollations[attn] = exprCollation(attribute->expr);
            atttype = exprType(attribute->expr);

            appendStringInfo(&indexRelationName, "expr");

            /*
             * Strip any top-level COLLATE clause.  This ensures that we
             * treat "x COLLATE y" and "(x COLLATE y)" alike.
             */
            while (IsA(expr, CollateExpr)) {
                expr = (Node *)((CollateExpr *)expr)->arg;
            }

            if (IsA(expr, Var) && ((Var *)expr)->varattno != InvalidAttrNumber) {
                /*
                 * User wrote "(column)" or "(column COLLATE something)".
                 * Treat it like simple attribute anyway.
                 */
                entry->indexkeys[attn] = ((Var *)expr)->varattno;

                /*
                 * Generated index name will have _expr instead of attname
                 * in generated index name, and error message will also be
                 * slighty different in case on unexisting column from a
                 * simple attribute, but that's how ComputeIndexAttrs()
                 * proceed.
                 */
            } else {
                /*
                 * transformExpr() should have already rejected
                 * subqueries, aggregates, and window functions, based on
                 * the EXPR_KIND_ for an index expression.
                 */

                /*
                 * An expression using mutable functions is probably
                 * wrong, since if you aren't going to get the same result
                 * for the same data every time, it's not clear what the
                 * index entries mean at all.
                 */
                if (CheckMutability((Expr *)expr)) {
                    ereport(ERROR, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                        errmsg("hypopg: functions in index expression must be marked IMMUTABLE")));
                }

                entry->indexkeys[attn] = 0; /* marks expression */

                oldcontext = MemoryContextSwitchTo(g_instance.hypo_cxt.HypopgContext);
                entry->indexprs = lappend(entry->indexprs, (Node *)copyObject(attribute->expr));
                MemoryContextSwitchTo(oldcontext);
            }
        }

        ind_avg_width += hypo_estimate_index_colsize(entry, attn);

        /*
         * Apply collation override if any
         */
        if (attribute->collation) {
            entry->indexcollations[attn] = get_collation_oid(attribute->collation, false);
        }

        /*
         * Check we have a collation iff it's a collatable type.  The only
         * expected failures here are (1) COLLATE applied to a
         * noncollatable type, or (2) index expression had an unresolved
         * collation.  But we might as well code this to be a complete
         * consistency check.
         */
        if (type_is_collatable(atttype)) {
            if (!OidIsValid(entry->indexcollations[attn])) {
                ereport(ERROR, (errcode(ERRCODE_INDETERMINATE_COLLATION),
                    errmsg("hypopg: could not determine which collation to use for index expression"),
                    errhint("Use the COLLATE clause to set the collation explicitly.")));
            }
        } else {
            if (OidIsValid(entry->indexcollations[attn])) {
                ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
                    errmsg("hypopg: collations are not supported by type %s", format_type_be(atttype))));
            }
        }

        /* get the opclass */
        opclass = GetIndexOpClass(attribute->opclass, atttype, node->accessMethod, entry->relam);
        entry->opclass[attn] = opclass;
        /* setup the opfamily */
        entry->opfamily[attn] = get_opclass_family(opclass);

        entry->opcintype[attn] = get_opclass_input_type(opclass);

        /* setup the sort info if am handles it */
        if (entry->amcanorder) {
            /* setup NULLS LAST, NULLS FIRST cases are handled below */
            entry->nulls_first[attn] = false;
            /* default ordering is ASC */
            entry->reverse_sort[attn] = (attribute->ordering == SORTBY_DESC);
            /* default null ordering is LAST for ASC, FIRST for DESC */
            if (attribute->nulls_ordering == SORTBY_NULLS_DEFAULT) {
                if (attribute->ordering == SORTBY_DESC) {
                    entry->nulls_first[attn] = true;
                }
            } else if (attribute->nulls_ordering == SORTBY_NULLS_FIRST) {
                entry->nulls_first[attn] = true;
            }
        }
        /*
         * OIS info is global for the index before 9.5, so look for the
         * information only once in that case.
         */
        if (attn == 0) {
            /*
             * specify first column, but it doesn't matter as this will
             * only be used with GiST am, which cannot do IOS prior pg 9.5
             */
            entry->canreturn = hypo_can_return(entry, atttype, 0, node->accessMethod);
        }
        attn++;
    }
}

/*
 * Create an hypothetical index from its CREATE INDEX parsetree.  This function
 * is where all the hypothetic index creation is done, except the index size
 * estimation.
 */
static const hypoIndex *hypo_index_store_parsetree(IndexStmt *node, const char *queryString)
{
    /* must be declared "volatile", because used in a PG_CATCH() */
    hypoIndex *volatile entry;
    Oid relid;
    StringInfoData indexRelationName;
    int nkeycolumns, ninccolumns;
    int attn;

    relid = RangeVarGetRelid(node->relation, AccessShareLock, false);

    /* Some sanity checks */
    if (get_rel_relkind(relid) != RELKIND_RELATION) {
        elog(ERROR, "hypopg: \"%s\" is not an ordinary table", node->relation->relname);
    }

    /* Run parse analysis ... */
    node = transformIndexStmt(relid, node, queryString);

    nkeycolumns = list_length(node->indexParams);
    ninccolumns = 0;

    if (nkeycolumns > INDEX_MAX_KEYS) {
        elog(ERROR, "hypopg: cannot use more thant %d columns in an index", INDEX_MAX_KEYS);
    }

    initStringInfo(&indexRelationName);
    appendStringInfoString(&indexRelationName, node->accessMethod);
    appendStringInfoString(&indexRelationName, "_");
    if (node->isGlobal) {
        appendStringInfoString(&indexRelationName, "global");
        appendStringInfoString(&indexRelationName, "_");
    } else if (node->isPartitioned) {
        appendStringInfoString(&indexRelationName, "local");
        appendStringInfoString(&indexRelationName, "_");
    }
    if (node->relation->schemaname != NULL && (strcmp(node->relation->schemaname, "public") != 0)) {
        appendStringInfoString(&indexRelationName, node->relation->schemaname);
        appendStringInfoString(&indexRelationName, "_");
    }

    appendStringInfoString(&indexRelationName, node->relation->relname);

    /* now create the hypothetical index entry */
    entry = hypo_newIndex(relid, node->accessMethod, nkeycolumns, ninccolumns, node->options);

    PG_TRY();
    {
        int ind_avg_width = 0;

        if (node->unique && !entry->amcanunique) {
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("hypopg: access method \"%s\" does not support unique indexes", node->accessMethod)));
        }
        if (nkeycolumns > 1 && !entry->amcanmulticol) {
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("hypopg: access method \"%s\" does not support multicolumn indexes", node->accessMethod)));
        }

        entry->unique = node->unique;
        entry->ncolumns = nkeycolumns + ninccolumns;
        entry->nkeycolumns = nkeycolumns;
        entry->isGlobal = node->isGlobal;
        entry->ispartitionedindex = node->isPartitioned;
        /* handle predicate if present */
        hypo_handle_predicate(node, entry);

        /*
         * process attributeList
         */
        hypo_process_attr(node, entry, indexRelationName, relid, ind_avg_width);

        /*
         * We disallow indexes on system columns other than OID.  They would
         * not necessarily get updated correctly, and they don't seem useful
         * anyway.
         */
        for (attn = 0; attn < nkeycolumns; attn++) {
            AttrNumber attno = entry->indexkeys[attn];

            if (attno < 0 && attno != ObjectIdAttributeNumber) {
                ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("hypopg: index creation on system columns is not supported")));
            }
        }

        /*
         * Also check for system columns used in expressions or predicates.
         */
        if (entry->indexprs || entry->indpred) {
            Bitmapset *indexattrs = NULL;
            int i;

            pull_varattnos((Node *)entry->indexprs, 1, &indexattrs);
            pull_varattnos((Node *)entry->indpred, 1, &indexattrs);

            for (i = FirstLowInvalidHeapAttributeNumber + 1; i < 0; i++) {
                if (i != ObjectIdAttributeNumber && bms_is_member(i - FirstLowInvalidHeapAttributeNumber, indexattrs)) {
                    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("hypopg: index creation on system columns is not supported")));
                }
            }
        }

        /* Check if the average size fits in a btree index */
        if (OID_IS_BTREE(entry->relam)) {
            if (ind_avg_width >= (int)HYPO_BTMaxItemSize) {
                ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                    errmsg("hypopg: estimated index row size %d "
                    "exceeds maximum %ld",
                    ind_avg_width, HYPO_BTMaxItemSize),
                    errhint("Values larger than 1/3 of a buffer page "
                    "cannot be indexed.\nConsider a function index "
                    " of an MD5 hash of the value, or use full text "
                    "indexing\n(which is not yet supported by hypopg).")));
                /* Warn about posssible error with a 80% avg size */
            } else if (ind_avg_width >= HYPO_BTMaxItemSize * .8) {
                ereport(WARNING, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                    errmsg("hypopg: estimated index row size %d "
                    "is close to maximum %ld",
                    ind_avg_width, HYPO_BTMaxItemSize),
                    errhint("Values larger than 1/3 of a buffer page "
                    "cannot be indexed.\nConsider a function index "
                    " of an MD5 hash of the value, or use full text "
                    "indexing\n(which is not yet supported by hypopg).")));
            }
        }
    }
    PG_CATCH();
    {
        /* Free what was palloc'd in HypoMemoryContext */
        hypo_index_pfree(entry);

        PG_RE_THROW();
    }
    PG_END_TRY();

    /*
     * Fetch the ordering information for the index, if any. Adapted from
     * plancat.c - get_relation_info().
     */
    if (!OID_IS_BTREE(entry->relam) && entry->amcanorder) {
        /*
         * Otherwise, identify the corresponding btree opfamilies by trying to
         * map this index's "<" operators into btree.  Since "<" uniquely
         * defines the behavior of a sort order, this is a sufficient test.
         *
         * XXX This method is rather slow and also requires the undesirable
         * assumption that the other index AM numbers its strategies the same
         * as btree.  It'd be better to have a way to explicitly declare the
         * corresponding btree opfamily for each opfamily of the other index
         * type.  But given the lack of current or foreseeable amcanorder
         * index types, it's not worth expending more effort on now.
         */
        for (attn = 0; attn < nkeycolumns; attn++) {
            Oid ltopr;
            Oid btopfamily;
            Oid btopcintype;
            int16 btstrategy;

            ltopr = get_opfamily_member(entry->opfamily[attn], entry->opcintype[attn], entry->opcintype[attn],
                BTLessStrategyNumber);
            if (OidIsValid(ltopr) && get_ordering_op_properties(ltopr, &btopfamily, &btopcintype, &btstrategy) &&
                btopcintype == entry->opcintype[attn] && btstrategy == BTLessStrategyNumber) {
                /* Successful mapping */
                entry->sortopfamily[attn] = btopfamily;
            } else {
                /* Fail ... quietly treat index as unordered */
                /* also pfree allocated memory */
                pfree(entry->sortopfamily);
                pfree(entry->reverse_sort);
                pfree(entry->nulls_first);

                entry->sortopfamily = NULL;
                entry->reverse_sort = NULL;
                entry->nulls_first = NULL;

                break;
            }
        }
    }

    hypo_set_indexname(entry, indexRelationName.data);

    hypo_addIndex(entry);

    return entry;
}

/*
 * Remove an hypothetical index from the list of hypothetical indexes.
 * pfree (by calling hypo_index_pfree) all memory that has been allocated.
 */
static bool hypo_index_remove(Oid indexid)
{
    ListCell *lc;

    LWLockAcquire(HypoIndexLock, LW_EXCLUSIVE);

    foreach (lc, g_instance.hypo_cxt.hypo_index_list) {
        hypoIndex *entry = (hypoIndex *)lfirst(lc);

        if (entry->oid == indexid) {
            g_instance.hypo_cxt.hypo_index_list = list_delete_ptr(g_instance.hypo_cxt.hypo_index_list, entry);
            hypo_index_pfree(entry);

            LWLockRelease(HypoIndexLock);

            return true;
        }
    }

    LWLockRelease(HypoIndexLock);

    return false;
}

/* pfree all allocated memory for within an hypoIndex and the entry itself. */
static void hypo_index_pfree(hypoIndex *entry)
{
    /* pfree all memory that has been allocated */
    pfree(entry->indexname);
    pfree(entry->indexkeys);
    pfree(entry->indexcollations);
    pfree(entry->opfamily);
    pfree(entry->opclass);
    pfree(entry->opcintype);
    if (OID_IS_BTREE(entry->relam) || entry->amcanorder) {
        if (!OID_IS_BTREE(entry->relam) && entry->sortopfamily) {
            pfree(entry->sortopfamily);
        }
        if (entry->reverse_sort) {
            pfree(entry->reverse_sort);
        }
        if (entry->nulls_first) {
            pfree(entry->nulls_first);
        }
    }
    if (entry->indexprs) {
        list_free_deep(entry->indexprs);
    }
    if (entry->indpred) {
        pfree(entry->indpred);
    }
    /* finally pfree the entry */
    pfree(entry);
}

/* --------------------------------------------------
 * Add an hypothetical index to the list of indexes.
 * Caller should have check that the specified hypoIndex does belong to the
 * specified relation.  This function also assume that the specified entry
 * already contains every needed information, so we just basically need to copy
 * it from the hypoIndex to the new IndexOptInfo.  Every specific handling is
 * done at store time (ie.  hypo_index_store_parsetree).  The only exception is
 * the size estimation, recomputed verytime, as it needs up to date statistics.
 */
static void hypo_injectHypotheticalIndex(PlannerInfo *root, Oid relationObjectId, bool inhparent, RelOptInfo *rel,
    Relation relation, hypoIndex *entry)
{
    IndexOptInfo *index;
    int ncolumns, nkeycolumns, i;


    /* create a node */
    index = makeNode(IndexOptInfo);

    index->relam = entry->relam;

    /* General stuff */
    index->indexoid = entry->oid;
    index->reltablespace = rel->reltablespace;
    index->rel = rel;
    index->ncolumns = ncolumns = entry->ncolumns;
    index->nkeycolumns = nkeycolumns = entry->nkeycolumns;

    index->indexkeys = (int *)palloc(sizeof(int) * ncolumns);
    index->indexcollations = (Oid *)palloc(sizeof(int) * ncolumns);
    index->opfamily = (Oid *)palloc(sizeof(int) * ncolumns);
    index->opcintype = (Oid *)palloc(sizeof(int) * ncolumns);

    if (OID_IS_BTREE(index->relam) || entry->amcanorder) {
        if (!OID_IS_BTREE(index->relam)) {
            index->sortopfamily = (Oid *)palloc0(sizeof(Oid) * ncolumns);
        }

        index->reverse_sort = (bool *)palloc(sizeof(bool) * ncolumns);
        index->nulls_first = (bool *)palloc(sizeof(bool) * ncolumns);
    } else {
        index->sortopfamily = NULL;
        index->reverse_sort = NULL;
        index->nulls_first = NULL;
    }

    for (i = 0; i < ncolumns; i++) {
        index->indexkeys[i] = entry->indexkeys[i];
    }

    for (i = 0; i < nkeycolumns; i++) {
        index->opfamily[i] = entry->opfamily[i];
        index->opcintype[i] = entry->opcintype[i];
        index->indexcollations[i] = entry->indexcollations[i];
    }

    /*
     * Fetch the ordering information for the index, if any. This is handled
     * in hypo_index_store_parsetree(). Again, adapted from plancat.c -
     * get_relation_info()
     */
    if (OID_IS_BTREE(entry->relam)) {
        /*
         * If it's a btree index, we can use its opfamily OIDs directly as the
         * sort ordering opfamily OIDs.
         */
        index->sortopfamily = index->opfamily;

        for (i = 0; i < ncolumns; i++) {
            index->reverse_sort[i] = entry->reverse_sort[i];
            index->nulls_first[i] = entry->nulls_first[i];
        }
    } else if (entry->amcanorder) {
        if (entry->sortopfamily) {
            for (i = 0; i < ncolumns; i++) {
                index->sortopfamily[i] = entry->sortopfamily[i];
                index->reverse_sort[i] = entry->reverse_sort[i];
                index->nulls_first[i] = entry->nulls_first[i];
            }
        } else {
            index->sortopfamily = NULL;
            index->reverse_sort = NULL;
            index->nulls_first = NULL;
        }
    }

    index->unique = entry->unique;

    index->amcostestimate = entry->amcostestimate;
    index->immediate = entry->immediate;
    index->canreturn = entry->canreturn;
    index->amcanorderbyop = entry->amcanorderbyop;
    index->amoptionalkey = entry->amoptionalkey;
    index->amsearcharray = entry->amsearcharray;
    index->amsearchnulls = entry->amsearchnulls;
    index->amhasgettuple = entry->amhasgettuple;
    index->amhasgetbitmap = entry->amhasgetbitmap;

    /* these has already been handled in hypo_index_store_parsetree() if any */
    index->indexprs = list_copy(entry->indexprs);
    index->indpred = list_copy(entry->indpred);
    index->predOK = false; /* will be set later in indxpath.c */

    /*
     * Build targetlist using the completed indexprs data. copied from
     * PostgreSQL
     */
    index->indextlist = build_index_tlist(root, index, relation);

    /*
     * estimate most of the hypothyetical index stuff, more exactly: tuples,
     * pages and tree_height (9.3+)
     */
    hypo_estimate_index(entry, rel);

    index->pages = entry->pages;
    index->tuples = entry->tuples;
    index->ispartitionedindex = entry->ispartitionedindex;
    index->partitionindex = InvalidOid;
    index->isGlobal = entry->isGlobal;
    /*
     * obviously, setup this tag. However, it's only checked in
     * selfuncs.c/get_actual_variable_range, so we still need to add
     * hypothetical indexes *ONLY* in an explain-no-analyze command.
     */
    index->hypothetical = true;

    /* add our hypothetical index in the relation's indexlist */
    rel->indexlist = lcons(index, rel->indexlist);
}

/* Return the hypothetical index name is indexId is ours, NULL otherwise, as
 * this is what explain_get_index_name expects to continue his job.
 */
const char *hypo_explain_get_index_name_hook(Oid indexId)
{
    char *ret = NULL;

    if (isExplain) {
        /*
         * we're in an explain-only command. Return the name of the
         * hypothetical index name if it's one of ours, otherwise return NULL
         */
        ListCell *lc;

        LWLockAcquire(HypoIndexLock, LW_SHARED);

        foreach (lc, g_instance.hypo_cxt.hypo_index_list) {
            hypoIndex *entry = (hypoIndex *)lfirst(lc);

            if (entry->oid == indexId) {
                ret = entry->indexname;
            }
        }

        LWLockRelease(HypoIndexLock);
    }

    if (ret) {
        return ret;
    }

    if (prev_explain_get_index_name_hook)
        return prev_explain_get_index_name_hook(indexId);

    return NULL;
}

/*
 * List created hypothetical indexes
 */
Datum hypopg_display_index(PG_FUNCTION_ARGS)
{
    ReturnSetInfo *rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;
    MemoryContext per_query_ctx;
    MemoryContext oldcontext;
    TupleDesc tupdesc;
    Tuplestorestate *tupstore;
    ListCell *lc;
    errno_t rc = EOK;

    /* check to see if caller supports us returning a tuplestore */
    if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo)) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("set-valued function called in context that cannot accept a set")));
    }
    if (!(rsinfo->allowedModes & SFRM_Materialize)) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("materialize mode required, but it is not "
            "allowed in this context")));
    }

    per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
    oldcontext = MemoryContextSwitchTo(per_query_ctx);

    /* Build a tuple descriptor for our result type */
    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE) {
        elog(ERROR, "return type must be a row type");
    }

    tupstore = tuplestore_begin_heap(true, false, 1024);
    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult = tupstore;
    rsinfo->setDesc = tupdesc;

    MemoryContextSwitchTo(oldcontext);

    LWLockAcquire(HypoIndexLock, LW_SHARED);

    foreach (lc, g_instance.hypo_cxt.hypo_index_list) {
        hypoIndex *entry = (hypoIndex *)lfirst(lc);
        Datum values[HYPO_INDEX_NB_COLS];
        bool nulls[HYPO_INDEX_NB_COLS];
        StringInfoData index_columns;
        char *rel_name = NULL;
        int i = 0;
        int keyno;

        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");
        rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "\0", "\0");

        rel_name = get_rel_name(entry->relid);
        if (rel_name == NULL) {
            break;
        }
        values[i++] = CStringGetTextDatum(entry->indexname);
        values[i++] = ObjectIdGetDatum(entry->oid);
        values[i++] = CStringGetTextDatum(rel_name);

        initStringInfo(&index_columns);
        appendStringInfo(&index_columns, "(");
        for (keyno = 0; keyno < entry->nkeycolumns; keyno++) {
            if (keyno != 0) {
                appendStringInfo(&index_columns, ", ");
            }
            appendStringInfo(&index_columns, "%s", get_attname(entry->relid, entry->indexkeys[keyno]));
        }
        appendStringInfo(&index_columns, ")");
        values[i++] = CStringGetTextDatum(index_columns.data);

        tuplestore_putvalues(tupstore, tupdesc, values, nulls);
    }

    LWLockRelease(HypoIndexLock);

    /* clean up and return the tuplestore */
    tuplestore_donestoring(tupstore);

    return (Datum)0;
}

/*
 * SQL wrapper to create an hypothetical index with his parsetree
 */
Datum hypopg_create_index(PG_FUNCTION_ARGS)
{
    char *sql = TextDatumGetCString(PG_GETARG_TEXT_PP(0));
    List *parsetree_list;
    ListCell *parsetree_item;
    ReturnSetInfo *rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;
    MemoryContext per_query_ctx;
    MemoryContext oldcontext;
    TupleDesc tupdesc;
    Tuplestorestate *tupstore;
    errno_t rc = EOK;
    int i = 1;

    /* check to see if caller supports us returning a tuplestore */
    if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo)) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("set-valued function called in context that cannot accept a set")));
    }
    if (!(rsinfo->allowedModes & SFRM_Materialize)) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("materialize mode required, but it is not "
            "allowed in this context")));
    }

    per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
    oldcontext = MemoryContextSwitchTo(per_query_ctx);

    /* Build a tuple descriptor for our result type */
    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE) {
        elog(ERROR, "return type must be a row type");
    }

    tupstore = tuplestore_begin_heap(true, false, 1024);
    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult = tupstore;
    rsinfo->setDesc = tupdesc;

    MemoryContextSwitchTo(oldcontext);

    parsetree_list = pg_parse_query(sql);

    foreach (parsetree_item, parsetree_list) {
        Node *parsetree = (Node *)lfirst(parsetree_item);
        Datum values[HYPO_INDEX_CREATE_COLS];
        bool nulls[HYPO_INDEX_CREATE_COLS];
        const hypoIndex *entry;

        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");
        rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "\0", "\0");

        if (nodeTag(parsetree) != T_IndexStmt) {
            ereport(ERROR, 
                (errcode(ERRCODE_SYNTAX_ERROR), errmsg("hypopg: SQL order #%d is not a CREATE INDEX statement", i)));
        } else {
            entry = hypo_index_store_parsetree((IndexStmt *)parsetree, sql);
            if (entry != NULL) {
                if (entry->indexname == NULL) {
                    ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("hypopg: a field value is corrupted")));
                }
                values[0] = ObjectIdGetDatum(entry->oid);
                values[1] = CStringGetTextDatum(entry->indexname);

                tuplestore_putvalues(tupstore, tupdesc, values, nulls);
            }
        }
        i++;
    }

    /* clean up and return the tuplestore */
    tuplestore_donestoring(tupstore);

    return (Datum)0;
}

/*
 * SQL wrapper to drop an hypothetical index.
 */
Datum hypopg_drop_index(PG_FUNCTION_ARGS)
{
    Oid indexid = PG_GETARG_OID(0);

    PG_RETURN_BOOL(hypo_index_remove(indexid));
}

/*
 * SQL Wrapper around the hypothetical index size estimation
 */
Datum hypopg_estimate_size(PG_FUNCTION_ARGS)
{
    BlockNumber pages;
    double tuples;
    Oid indexid = PG_GETARG_OID(0);
    ListCell *lc;

    LWLockAcquire(HypoIndexLock, LW_SHARED);

    pages = 0;
    tuples = 0;
    foreach (lc, g_instance.hypo_cxt.hypo_index_list) {
        hypoIndex *entry = (hypoIndex *)lfirst(lc);

        if (entry->oid == indexid) {
            hypo_estimate_index_simple(entry, &pages, &tuples);
        }
    }

    LWLockRelease(HypoIndexLock);

    PG_RETURN_INT64(pages * 1.0L * BLCKSZ);
}

/*
 * SQL wrapper to remove all declared hypothetical indexes.
 */
Datum hypopg_reset_index(PG_FUNCTION_ARGS)
{
    hypo_index_reset();
    PG_RETURN_VOID();
}

/* Simple function to set the indexname, dealing with max name length, and the
 * ending \0
 */
static void hypo_set_indexname(hypoIndex *entry, const char *indexname)
{
    char oid[12] = {0}; /* store <oid>, oid shouldn't be more than 9999999999 */
    int totalsize;
    errno_t rc = EOK;

    rc = snprintf_s(oid, sizeof(oid), sizeof(oid) - 1, "<%u>", entry->oid);
    securec_check_ss(rc, "\0", "\0");

    /* we'll prefix the given indexname with the oid, and reserve a final \0 */
    totalsize = strlen(oid) + strlen(indexname) + 1;

    /* final index name must not exceed NAMEDATALEN */
    if (totalsize > NAMEDATALEN) {
        totalsize = NAMEDATALEN;
    }

    /* eventually truncate the given indexname at NAMEDATALEN-1 if needed */
    rc = strcpy_s(entry->indexname, NAMEDATALEN, oid);
    securec_check(rc, "\0", "\0");
    rc = strncat_s(entry->indexname, NAMEDATALEN, indexname, totalsize - strlen(oid) - 1);
    securec_check(rc, "\0", "\0");
}

/*
 * Fill the pages and tuples information for a given hypoIndex.
 */
static void hypo_estimate_index_simple(hypoIndex *entry, BlockNumber *pages, double *tuples)
{
    RelOptInfo *rel;
    Relation relation;

    /*
     * retrieve number of tuples and pages of the related relation, adapted
     * from plancat.c/get_relation_info().
     */

    rel = makeNode(RelOptInfo);

    /* Open the hypo index' relation */
    relation = heap_open(entry->relid, AccessShareLock);

    if (!RelationNeedsWAL(relation) && RecoveryInProgress()) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("hypopg: cannot access temporary or unlogged relations during recovery")));
    }

    rel->min_attr = FirstLowInvalidHeapAttributeNumber + 1;
    rel->max_attr = RelationGetNumberOfAttributes(relation);
    rel->reltablespace = RelationGetForm(relation)->reltablespace;

    Assert(rel->max_attr >= rel->min_attr);
    rel->attr_needed = (Relids *)palloc0((rel->max_attr - rel->min_attr + 1) * sizeof(Relids));
    rel->attr_widths = (int32 *)palloc0((rel->max_attr - rel->min_attr + 1) * sizeof(int32));

    estimate_rel_size(relation, rel->attr_widths - rel->min_attr, &rel->pages, &rel->tuples, &rel->allvisfrac, NULL);

    /* Close the relation and release the lock now */
    heap_close(relation, AccessShareLock);

    hypo_estimate_index(entry, rel);
    *pages = entry->pages;
    *tuples = entry->tuples;
}

/*
 * Fill the pages and tuples information for a given hypoIndex and a given
 * RelOptInfo
 */
static void hypo_estimate_index(hypoIndex *entry, RelOptInfo *rel)
{
    int i, ind_avg_width = 0;
    int usable_page_size;
    int line_size;
    double bloat_factor;
    int fillfactor = 0; /* for B-tree, hash, GiST and SP-Gist */
    int additional_bloat = 20;
    ListCell *lc;

    for (i = 0; i < entry->ncolumns; i++) {
        ind_avg_width += hypo_estimate_index_colsize(entry, i);
    }

    if (entry->indpred == NIL) {
        /* No predicate, as much tuples as estmated on its relation */
        entry->tuples = rel->tuples;
    } else {
        /*
         * We have a predicate. Find it's selectivity and setup the estimated
         * number of line according to it
         */
        Selectivity selectivity;
        PlannerInfo *root;
        PlannerGlobal *glob;
        Query *parse;
        List *rtable = NIL;
        RangeTblEntry *rte;

        /* create a fake minimal PlannerInfo */
        root = makeNode(PlannerInfo);

        glob = makeNode(PlannerGlobal);
        glob->boundParams = NULL;
        root->glob = glob;

        /* only 1 table: the one related to this hypothetical index */
        rte = makeNode(RangeTblEntry);
        rte->relkind = RTE_RELATION;
        rte->relid = entry->relid;
        rte->inh = false; /* don't include inherited children */
        rtable = lappend(rtable, rte);

        parse = makeNode(Query);
        parse->rtable = rtable;
        root->parse = parse;

        /*
         * allocate simple_rel_arrays and simple_rte_arrays. This function
         * will also setup simple_rte_arrays with the previous rte.
         */
        setup_simple_rel_arrays(root);
        /* also add our table info */
        root->simple_rel_array[1] = rel;

        /*
         * per comment on clause_selectivity(), JOIN_INNER must be passed if
         * the clause isn't a join clause, which is our case, and passing 0 to
         * varRelid is appropriate for restriction clause.
         */
        selectivity = clauselist_selectivity(root, entry->indpred, 0, JOIN_INNER, NULL);

        elog(DEBUG1, "hypopg: selectivity for index \"%s\": %lf", entry->indexname, selectivity);

        entry->tuples = selectivity * rel->tuples;
    }

    /* handle index storage parameters */
    foreach (lc, entry->options) {
        DefElem *elem = (DefElem *)lfirst(lc);

        if (strcmp(elem->defname, "fillfactor") == 0) {
            fillfactor = (int32)intVal(elem->arg);
        }
    }

    if (OID_IS_BTREE(entry->relam)) {
        /* -------------------------------
         * quick estimating of index size:
         *
         * sizeof(PageHeader) : 24 (1 per page)
         * sizeof(BTPageOpaqueData): 16 (1 per page)
         * sizeof(IndexTupleData): 8 (1 per tuple, referencing heap)
         * sizeof(ItemIdData): 4 (1 per tuple, storing the index item)
         * default fillfactor: 90%
         * no NULL handling
         * fixed additional bloat: 20%
         *
         * I'll also need to read more carefully nbtree code to check if
         * this is accurate enough.
         *
         */
        line_size = ind_avg_width + +(sizeof(IndexTupleData) * entry->ncolumns) +
            MAXALIGN(sizeof(ItemIdData) * entry->ncolumns);

        usable_page_size = BLCKSZ - SizeOfPageHeaderData - sizeof(BTPageOpaqueData);
        bloat_factor = (200.0 - (fillfactor == 0 ? BTREE_DEFAULT_FILLFACTOR : fillfactor) + additional_bloat) / 100;

        entry->pages = (BlockNumber)(entry->tuples * line_size * bloat_factor / usable_page_size);
    } else {
        /* we shouldn't raise this error */
        elog(WARNING, "hypopg: access method %d is not supported", entry->relam);
    }

    /* make sure the index size is at least one block */
    if (entry->pages <= 0) {
        entry->pages = 1;
    }
}

/*
 * Estimate a single index's column of an hypothetical index.
 */
static int hypo_estimate_index_colsize(hypoIndex *entry, int col)
{
    int i, pos;
    Node *expr;

    /* If simple attribute, return avg width */
    if (entry->indexkeys[col] != 0) {
        return get_attavgwidth(entry->relid, entry->indexkeys[col], false);
    }

    /* It's an expression */
    pos = 0;

    for (i = 0; i < col; i++) {
        /* get the position in the expression list */
        if (entry->indexkeys[i] == 0) {
            pos++;
        }
    }

    expr = (Node *)list_nth(entry->indexprs, pos);

    if (IsA(expr, Var) && ((Var *)expr)->varattno != InvalidAttrNumber) {
        return get_attavgwidth(entry->relid, ((Var *)expr)->varattno, false);
    }

    if (IsA(expr, FuncExpr)) {
        FuncExpr *funcexpr = (FuncExpr *)expr;

        switch (funcexpr->funcid) {
            case 2311: {
                /* md5 */
                return 32;
                break;
            }
            case 870:
            case 871: {
                /* lower and upper, detect if simple attr */
                Var *var;

                if (IsA(linitial(funcexpr->args), Var)) {
                    var = (Var *)linitial(funcexpr->args);

                    if (var->varattno > 0) {
                        return get_attavgwidth(entry->relid, var->varattno, false);
                    }
                }
                break;
            }
            default:
                /* default fallback estimate will be used */
                break;
        }
    }

    return 50; /* default fallback estimate */
}

/*
 * canreturn should been checked with the amcanreturn proc, but this
 * can't be done without a real Relation, so try to find it out
 */
static bool hypo_can_return(hypoIndex *entry, Oid atttype, int i, char *amname)
{
    /* no amcanreturn entry, am does not handle IOS */
    if (!RegProcedureIsValid(entry->amcanreturn)) {
        return false;
    }

    switch (entry->relam) {
        case BTREE_AM_OID:
        case UBTREE_AM_OID: {
            /* btree always support Index-Only scan */
            return true;
            break;
        }
        case GIST_AM_OID: {
            return false;
            break;
        }
        case SPGIST_AM_OID: {
            SpGistCache *cache;
            spgConfigIn in;
            HeapTuple tuple;
            Oid funcid;
            bool res = false;

            /* support function 1 tells us if IOS is supported */
            tuple =
                SearchSysCache4(AMPROCNUM, ObjectIdGetDatum(entry->opfamily[i]), ObjectIdGetDatum(entry->opcintype[i]),
                ObjectIdGetDatum(entry->opcintype[i]), Int8GetDatum(SPGIST_CONFIG_PROC));

            /* just in case */
            if (!HeapTupleIsValid(tuple)) {
                return false;
            }

            funcid = ((Form_pg_amproc)GETSTRUCT(tuple))->amproc;
            ReleaseSysCache(tuple);

            in.attType = atttype;
            cache = (SpGistCache *)palloc0(sizeof(SpGistCache));

            OidFunctionCall2Coll(funcid, entry->indexcollations[i], PointerGetDatum(&in),
                PointerGetDatum(&cache->config));

            res = cache->config.canReturnData;
            pfree(cache);

            return res;

            break;
        }
        default: {
            /* all specific case should have been handled */
            elog(WARNING,
                "hypopg: access method \"%s\" looks like it may"
                " support Index-Only Scan, but it's unexpected.\n"
                "Feel free to warn developper.",
                amname);
            return false;
            break;
        }
    }
}
