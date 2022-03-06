/* -------------------------------------------------------------------------
 *
 * setrefs.cpp
 *	  Post-processing of a completed plan tree: fix references to subplan
 *	  vars, compute regproc values for operators, etc
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/optimizer/plan/setrefs.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/transam.h"
#include "catalog/pg_type.h"
#include "catalog/pg_partition_fn.h"
#include "catalog/pg_proc.h"
#include "executor/node/nodeRecursiveunion.h"
#include "foreign/foreign.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/dataskew.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/planner.h"
#include "optimizer/tlist.h"
#include "tcop/utility.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#ifdef PGXC
#include "pgxc/pgxc.h"
#include "optimizer/pgxcplan.h"
#endif
#include "optimizer/streamplan.h"
#include "optimizer/stream_remove.h"

typedef struct {
    Index varno;         /* RT index of Var */
    AttrNumber varattno; /* attr number of Var */
    AttrNumber resno;    /* TLE position of Var */
} tlist_vinfo;

typedef struct {
    List* tlist;       /* underlying target list */
    int num_vars;      /* number of plain Var tlist entries */
    bool has_ph_vars;  /* are there PlaceHolderVar entries? */
    bool has_non_vars; /* are there other entries? */
    bool return_const; /* return const or Var to const. */
    /* array of num_vars entries: */
    tlist_vinfo vars[1]; /* VARIABLE LENGTH ARRAY */
} indexed_tlist;         /* VARIABLE LENGTH STRUCT */

typedef struct {
    PlannerInfo* root;
    int rtoffset;
} fix_scan_expr_context;

typedef struct {
    PlannerInfo* root;
    indexed_tlist* outer_itlist;
    indexed_tlist* inner_itlist;
    Index acceptable_rel;
    int rtoffset;
} fix_join_expr_context;

typedef struct {
    PlannerInfo* root;
    indexed_tlist* subplan_itlist;
    Index newvarno;
    int rtoffset;
} fix_upper_expr_context;

typedef struct {
    PlannerGlobal* glob;
    indexed_tlist* base_itlist;
    int rtoffset;
    Index relid;
    bool return_non_base_vars; /* Should we reject or return vars not found in base_itlist */
} fix_remote_expr_context;

/*
 * Check if a Const node is a regclass value.  We accept plain OID too,
 * since a regclass Const will get folded to that type if it's an argument
 * to oideq or similar operators.  (This might result in some extraneous
 * values in a plan's list of relation dependencies, but the worst result
 * would be occasional useless replans.)
 */
#define ISREGCLASSCONST(con) (((con)->consttype == REGCLASSOID || (con)->consttype == OIDOID) && !(con)->constisnull)

#define fix_scan_list(root, lst, rtoffset) ((List*)fix_scan_expr(root, (Node*)(lst), rtoffset))

static Plan* set_plan_refs(PlannerInfo* root, Plan* plan, int rtoffset);
static Plan* set_indexonlyscan_references(PlannerInfo* root, IndexOnlyScan* plan, int rtoffset);
static Plan* set_subqueryscan_references(PlannerInfo* root, SubqueryScan* plan, int rtoffset);
static void set_extensibleplan_references(PlannerInfo* root, ExtensiblePlan* cscan, int rtoffset);
static Node* fix_scan_expr(PlannerInfo* root, Node* node, int rtoffset);
/**
 * @Description: Because zero out RangeTblEntry fields that are not useful to the
 * the executor in optimizer phase, Adjust varno of Var in DfsPrivateItem node to be
 * consistent with the flat rangetable.
 * @in root: the PlannerInfo struct.
 * @in rtoffset: the index offset of rangetable.
 * @in item: the DfsPrivateItem to be adjusted.
 * @return None.
 */
static void fix_dfs_private_item(PlannerInfo* root, int rte_offset, DfsPrivateItem* item);
static Node* fix_scan_expr_mutator(Node* node, fix_scan_expr_context* context);
static bool fix_scan_expr_walker(Node* node, fix_scan_expr_context* context);
static void set_join_references(PlannerInfo* root, Join* join, int rtoffset);
static void set_upper_references(PlannerInfo* root, Plan* plan, int rtoffset);
static void set_dummy_tlist_references(Plan* plan, int rtoffset);
static indexed_tlist* build_tlist_index(List* tlist);
static Var* search_indexed_tlist_for_var(Var* var, indexed_tlist* itlist, Index newvarno, int rtoffset);
static Var* search_indexed_tlist_for_non_var(Node* node, indexed_tlist* itlist, Index newvarno);
static Var* search_indexed_tlist_for_sortgroupref(
    Node* node, Index sortgroupref, indexed_tlist* itlist, Index newvarno);
static List* fix_join_expr(PlannerInfo* root, List* clauses, indexed_tlist* outer_itlist, indexed_tlist* inner_itlist,
    Index acceptable_rel, int rtoffset);
static Node* fix_join_expr_mutator(Node* node, fix_join_expr_context* context);
static Node* fix_upper_expr(PlannerInfo* root, Node* node, indexed_tlist* subplan_itlist, Index newvarno, int rtoffset);
static Node* fix_upper_expr_mutator(Node* node, fix_upper_expr_context* context);
static List* set_returning_clause_references(
    PlannerInfo* root, List* rlist, Plan* topplan, Index resultRelation, int rtoffset);
static bool fix_opfuncids_walker(Node* node, void* context);
static bool extract_query_dependencies_walker(Node* node, PlannerInfo* context);
static void fix_skew_quals(PlannerInfo* root, Plan* plan, indexed_tlist* subplan_itlist, int rtoffset);

#ifdef PGXC
/* References for remote plans */
static List* fix_remote_expr(PlannerInfo* root, List* clauses, indexed_tlist* base_itlist, Index newrelid, int rtoffset,
    bool return_non_base_vars);
static Node* fix_remote_expr_mutator(Node* node, fix_remote_expr_context* context);
static void set_remote_references(PlannerInfo* root, RemoteQuery* rscan, int rtoffset);
static void pgxc_set_agg_references(PlannerInfo* root, Agg* aggplan);
static List* set_remote_returning_refs(PlannerInfo* root, List* rlist, Plan* topplan, Index relid, int rtoffset);
#endif

/*****************************************************************************
 *
 *		SUBPLAN REFERENCES
 *
 *****************************************************************************/
/*
 * set_plan_references
 *
 * This is the final processing pass of the planner/optimizer.	The plan
 * tree is complete; we just have to adjust some representational details
 * for the convenience of the executor:
 *
 * 1. We flatten the various subquery rangetables into a single list, and
 * zero out RangeTblEntry fields that are not useful to the executor.
 *
 * 2. We adjust Vars in scan nodes to be consistent with the flat rangetable.
 *
 * 3. We adjust Vars in upper plan nodes to refer to the outputs of their
 * subplans.
 *
 * 4. We compute regproc OIDs for operators (ie, we look up the function
 * that implements each op).
 *
 * 5. We create lists of specific objects that the plan depends on.
 * This will be used by plancache.c to drive invalidation of cached plans.
 * Relation dependencies are represented by OIDs, and everything else by
 * PlanInvalItems (this distinction is motivated by the shared-inval APIs).
 * Currently, relations and user-defined functions are the only types of
 * objects that are explicitly tracked this way.
 *
 * We also perform one final optimization step, which is to delete
 * SubqueryScan plan nodes that aren't doing anything useful (ie, have
 * no qual and a no-op targetlist).  The reason for doing this last is that
 * it can't readily be done before set_plan_references, because it would
 * break set_upper_references: the Vars in the subquery's top tlist
 * wouldn't match up with the Vars in the outer plan tree.  The SubqueryScan
 * serves a necessary function as a buffer between outer query and subquery
 * variable numbering ... but after we've flattened the rangetable this is
 * no longer a problem, since then there's only one rtindex namespace.
 *
 * set_plan_references recursively traverses the whole plan tree.
 *
 * The return value is normally the same Plan node passed in, but can be
 * different when the passed-in Plan is a SubqueryScan we decide isn't needed.
 *
 * The flattened rangetable entries are appended to root->glob->finalrtable.
 * Also, rowmarks entries are appended to root->glob->finalrowmarks, and the
 * RT indexes of ModifyTable result relations to root->glob->resultRelations.
 * Plan dependencies are appended to root->glob->relationOids (for relations)
 * and root->glob->invalItems (for everything else).
 *
 * Notice that we modify Plan nodes in-place, but use expression_tree_mutator
 * to process targetlist and qual expressions.	We can assume that the Plan
 * nodes were just built by the planner and are not multiply referenced, but
 * it's not so safe to assume that for expression tree nodes.
 */
Plan* set_plan_references(PlannerInfo* root, Plan* plan)
{
    PlannerGlobal* glob = root->glob;
    int rtoffset = list_length(glob->finalrtable);
    ListCell* lc = NULL;

    /*
     * In the flat rangetable, we zero out substructure pointers that are not
     * needed by the executor; this reduces the storage space and copying cost
     * for cached plans.  We keep only the alias and eref Alias fields, which
     * are needed by EXPLAIN, and the selectedCols and modifiedCols bitmaps,
     * which are needed for executor-startup permissions checking and for
     * trigger event checking.
     */
    foreach (lc, root->parse->rtable) {
        RangeTblEntry* rte = (RangeTblEntry*)lfirst(lc);
        RangeTblEntry* newrte = NULL;

        /* flat copy to duplicate all the scalar fields */
        newrte = (RangeTblEntry*)palloc(sizeof(RangeTblEntry));
        errno_t rc = memcpy_s(newrte, sizeof(RangeTblEntry), rte, sizeof(RangeTblEntry));
        securec_check(rc, "\0", "\0");

        /*
         * zap unneeded sub-structure, keep nerte->securityQuals here
         * because we need this information when execute explain query.
         */
        newrte->subquery = NULL;
        newrte->joinaliasvars = NIL;
        newrte->funcexpr = NULL;
        newrte->funccoltypes = NIL;
        newrte->funccoltypmods = NIL;
        newrte->funccolcollations = NIL;
        newrte->values_lists = NIL;
        newrte->values_collations = NIL;
        newrte->ctecoltypes = NIL;
        newrte->ctecoltypmods = NIL;
        newrte->ctecolcollations = NIL;

        glob->finalrtable = lappend(glob->finalrtable, newrte);

        /*
         * If it's a plain relation RTE, add the table to relationOids.
         *
         * We do this even though the RTE might be unreferenced in the plan
         * tree; this would correspond to cases such as views that were
         * expanded, child tables that were eliminated by constraint
         * exclusion, etc.	Schema invalidation on such a rel must still force
         * rebuilding of the plan.
         *
         * Note we don't bother to avoid duplicate list entries.  We could,
         * but it would probably cost more cycles than it would save.
         */
        if (newrte->rtekind == RTE_RELATION)
            glob->relationOids = lappend_oid(glob->relationOids, newrte->relid);
    }

    /*
     * Check for RT index overflow; it's very unlikely, but if it did happen,
     * the executor would get confused by varnos that match the special varno
     * values.
     */
    if (IS_SPECIAL_VARNO(list_length(glob->finalrtable)))
        ereport(ERROR,
            (errmodule(MOD_OPT),
                (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                    errmsg("too many range table entries when set plan reference."))));

    /*
     * Adjust RT indexes of PlanRowMarks and add to final rowmarks list
     */
    foreach (lc, root->rowMarks) {
        PlanRowMark* rc = (PlanRowMark*)lfirst(lc);
        PlanRowMark* newrc = NULL;

        AssertEreport(IsA(rc, PlanRowMark), MOD_OPT, "type plan row mark is required.");

        /* flat copy is enough since all fields are scalars */
        newrc = (PlanRowMark*)palloc(sizeof(PlanRowMark));
        errno_t ret = memcpy_s(newrc, sizeof(PlanRowMark), rc, sizeof(PlanRowMark));
        securec_check(ret, "\0", "\0");
        /* adjust indexes ... but *not* the rowmarkId */
        newrc->rti += rtoffset;
        newrc->prti += rtoffset;

        glob->finalrowmarks = lappend(glob->finalrowmarks, newrc);
    }

    /* Now fix the Plan tree */
    return set_plan_refs(root, plan, rtoffset);
}

/*
 * set_plan_refs: recurse through the Plan nodes of a single subquery level
 */
static Plan* set_plan_refs(PlannerInfo* root, Plan* plan, int rtoffset)
{
    ListCell* l = NULL;

    if (plan == NULL)
        return NULL;

    /*
     * Plan-type-specific fixes
     */
    switch (nodeTag(plan)) {
        case T_SeqScan:
#ifdef ENABLE_MULTIPLE_NODES
        case T_TsStoreScan:
#endif   /* ENABLE_MULTIPLE_NODES */
        case T_CStoreScan: {
            Scan* splan = (Scan*)plan;
            splan->scanrelid += rtoffset;
            splan->plan.targetlist = fix_scan_list(root, splan->plan.targetlist, rtoffset);
            splan->plan.qual = fix_scan_list(root, splan->plan.qual, rtoffset);
            if (splan->plan.distributed_keys != NIL) {
                splan->plan.distributed_keys = fix_scan_list(root, splan->plan.distributed_keys, rtoffset);
            }
            if (splan->tablesample) {
                splan->tablesample = (TableSampleClause*)fix_scan_expr(root, (Node*)splan->tablesample, rtoffset);
            }
        } break;
        case T_DfsScan: {
            DfsScan* splan = (DfsScan*)plan;

            splan->scanrelid += rtoffset;
            splan->plan.targetlist = fix_scan_list(root, splan->plan.targetlist, rtoffset);
            if (splan->plan.distributed_keys != NIL) {
                splan->plan.distributed_keys = fix_scan_list(root, splan->plan.distributed_keys, rtoffset);
            }
            splan->plan.var_list = fix_scan_list(root, splan->plan.var_list, rtoffset);
            splan->plan.qual = fix_scan_list(root, splan->plan.qual, rtoffset);
            DfsPrivateItem* item = (DfsPrivateItem*)((DefElem*)linitial(splan->privateData))->arg;
            fix_dfs_private_item(root, rtoffset, item);
        } break;
        case T_IndexScan: {
            IndexScan* splan = (IndexScan*)plan;

            splan->scan.scanrelid += rtoffset;
            splan->scan.plan.targetlist = fix_scan_list(root, splan->scan.plan.targetlist, rtoffset);
            if (splan->scan.plan.distributed_keys != NIL) {
                splan->scan.plan.distributed_keys = fix_scan_list(root, splan->scan.plan.distributed_keys, rtoffset);
            }
            splan->scan.plan.qual = fix_scan_list(root, splan->scan.plan.qual, rtoffset);
            splan->indexqual = fix_scan_list(root, splan->indexqual, rtoffset);
            splan->indexqualorig = fix_scan_list(root, splan->indexqualorig, rtoffset);
            splan->indexorderby = fix_scan_list(root, splan->indexorderby, rtoffset);
            splan->indexorderbyorig = fix_scan_list(root, splan->indexorderbyorig, rtoffset);
        } break;
        case T_IndexOnlyScan: {
            IndexOnlyScan* splan = (IndexOnlyScan*)plan;
            if (splan->scan.plan.distributed_keys != NIL) {
                splan->scan.plan.distributed_keys = fix_scan_list(root, splan->scan.plan.distributed_keys, rtoffset);
            }
            return set_indexonlyscan_references(root, splan, rtoffset);
        } break;
        case T_CStoreIndexScan: {
            CStoreIndexScan* splan = (CStoreIndexScan*)plan;

            splan->scan.scanrelid += rtoffset;
            splan->scan.plan.targetlist = fix_scan_list(root, splan->scan.plan.targetlist, rtoffset);
            if (splan->scan.plan.distributed_keys != NIL) {
                splan->scan.plan.distributed_keys = fix_scan_list(root, splan->scan.plan.distributed_keys, rtoffset);
            }
            splan->scan.plan.qual = fix_scan_list(root, splan->scan.plan.qual, rtoffset);
            splan->indexqual = fix_scan_list(root, splan->indexqual, rtoffset);
            splan->indexqualorig = fix_scan_list(root, splan->indexqualorig, rtoffset);
            splan->indexorderby = fix_scan_list(root, splan->indexorderby, rtoffset);
            splan->indexorderbyorig = fix_scan_list(root, splan->indexorderbyorig, rtoffset);
            splan->cstorequal = fix_scan_list(root, splan->cstorequal, rtoffset);
            splan->baserelcstorequal = fix_scan_list(root, splan->baserelcstorequal, rtoffset);
            splan->indextlist = fix_scan_list(root, splan->indextlist, rtoffset);
        } break;
        case T_DfsIndexScan: {
            DfsIndexScan* splan = (DfsIndexScan*)plan;

            splan->scan.scanrelid += rtoffset;
            splan->scan.plan.targetlist = fix_scan_list(root, splan->scan.plan.targetlist, rtoffset);
            if (splan->scan.plan.distributed_keys != NIL) {
                splan->scan.plan.distributed_keys = fix_scan_list(root, splan->scan.plan.distributed_keys, rtoffset);
            }
            splan->indextlist = fix_scan_list(root, splan->indextlist, rtoffset);
            splan->scan.plan.qual = fix_scan_list(root, splan->scan.plan.qual, rtoffset);
            splan->indexqual = fix_scan_list(root, splan->indexqual, rtoffset);
            splan->indexqualorig = fix_scan_list(root, splan->indexqualorig, rtoffset);
            splan->indexorderby = fix_scan_list(root, splan->indexorderby, rtoffset);
            splan->indexorderbyorig = fix_scan_list(root, splan->indexorderbyorig, rtoffset);
            splan->cstorequal = fix_scan_list(root, splan->cstorequal, rtoffset);
            splan->indexScantlist = fix_scan_list(root, splan->indexScantlist, rtoffset);
            splan->dfsScan = (DfsScan*)set_plan_refs(root, (Plan*)splan->dfsScan, rtoffset);
        } break;
        case T_BitmapIndexScan: {
            BitmapIndexScan* splan = (BitmapIndexScan*)plan;

            splan->scan.scanrelid += rtoffset;
            /* no need to fix targetlist and qual */
            AssertEreport(splan->scan.plan.targetlist == NIL && splan->scan.plan.qual == NIL,
                MOD_OPT,
                "targetlist and qual should be null");
            if (splan->scan.plan.distributed_keys != NIL) {
                splan->scan.plan.distributed_keys = fix_scan_list(root, splan->scan.plan.distributed_keys, rtoffset);
            }
            splan->indexqual = fix_scan_list(root, splan->indexqual, rtoffset);
            splan->indexqualorig = fix_scan_list(root, splan->indexqualorig, rtoffset);
        } break;
        case T_BitmapHeapScan: {
            BitmapHeapScan* splan = (BitmapHeapScan*)plan;

            splan->scan.scanrelid += rtoffset;
            splan->scan.plan.targetlist = fix_scan_list(root, splan->scan.plan.targetlist, rtoffset);
            if (splan->scan.plan.distributed_keys != NIL) {
                splan->scan.plan.distributed_keys = fix_scan_list(root, splan->scan.plan.distributed_keys, rtoffset);
            }
            splan->scan.plan.qual = fix_scan_list(root, splan->scan.plan.qual, rtoffset);
            splan->bitmapqualorig = fix_scan_list(root, splan->bitmapqualorig, rtoffset);
        } break;
        case T_CStoreIndexCtidScan: {
            CStoreIndexCtidScan* splan = (CStoreIndexCtidScan*)plan;

            splan->scan.scanrelid += rtoffset;
            /* no need to fix targetlist and qual */
            AssertEreport(splan->scan.plan.targetlist == NIL && splan->scan.plan.qual == NIL,
                MOD_OPT,
                "targetlist and quals hould be null");
            if (splan->scan.plan.distributed_keys != NIL) {
                splan->scan.plan.distributed_keys = fix_scan_list(root, splan->scan.plan.distributed_keys, rtoffset);
            }
            splan->indexqual = fix_scan_list(root, splan->indexqual, rtoffset);
            splan->cstorequal = fix_scan_list(root, splan->cstorequal, rtoffset);
            splan->indexqualorig = fix_scan_list(root, splan->indexqualorig, rtoffset);
            splan->indextlist = fix_scan_list(root, splan->indextlist, rtoffset);
        } break;
        case T_CStoreIndexHeapScan: {
            CStoreIndexHeapScan* splan = (CStoreIndexHeapScan*)plan;

            splan->scan.scanrelid += rtoffset;
            splan->scan.plan.targetlist = fix_scan_list(root, splan->scan.plan.targetlist, rtoffset);
            if (splan->scan.plan.distributed_keys != NIL) {
                splan->scan.plan.distributed_keys = fix_scan_list(root, splan->scan.plan.distributed_keys, rtoffset);
            }
            splan->scan.plan.qual = fix_scan_list(root, splan->scan.plan.qual, rtoffset);
            splan->bitmapqualorig = fix_scan_list(root, splan->bitmapqualorig, rtoffset);
        } break;
        case T_TidScan: {
            TidScan* splan = (TidScan*)plan;

            splan->scan.scanrelid += rtoffset;
            splan->scan.plan.targetlist = fix_scan_list(root, splan->scan.plan.targetlist, rtoffset);
            splan->scan.plan.qual = fix_scan_list(root, splan->scan.plan.qual, rtoffset);
            splan->tidquals = fix_scan_list(root, splan->tidquals, rtoffset);
        } break;
        case T_SubqueryScan:
        case T_VecSubqueryScan:
            /* Needs special treatment, see comments below */
            return set_subqueryscan_references(root, (SubqueryScan*)plan, rtoffset);
        case T_FunctionScan: {
            FunctionScan* splan = (FunctionScan*)plan;

            splan->scan.scanrelid += rtoffset;
            splan->scan.plan.targetlist = fix_scan_list(root, splan->scan.plan.targetlist, rtoffset);
            splan->scan.plan.qual = fix_scan_list(root, splan->scan.plan.qual, rtoffset);
            splan->funcexpr = fix_scan_expr(root, splan->funcexpr, rtoffset);
        } break;
        case T_ValuesScan: {
            ValuesScan* splan = (ValuesScan*)plan;

            splan->scan.scanrelid += rtoffset;
            splan->scan.plan.targetlist = fix_scan_list(root, splan->scan.plan.targetlist, rtoffset);
            splan->scan.plan.qual = fix_scan_list(root, splan->scan.plan.qual, rtoffset);
            splan->values_lists = fix_scan_list(root, splan->values_lists, rtoffset);
        } break;
        case T_CteScan: {
            CteScan* splan = (CteScan*)plan;

            splan->scan.scanrelid += rtoffset;
            splan->scan.plan.targetlist = fix_scan_list(root, splan->scan.plan.targetlist, rtoffset);
            if (splan->scan.plan.distributed_keys) {
                splan->scan.plan.distributed_keys = fix_scan_list(root, splan->scan.plan.distributed_keys, rtoffset);
            }
            splan->scan.plan.qual = fix_scan_list(root, splan->scan.plan.qual, rtoffset);

            /* set plan ref for the subplan of recursive union */
            if (STREAM_RECURSIVECTE_SUPPORTED) {
                CteScan* cte_scan = (CteScan*)plan;

                RecursiveUnion* ru_plan = (RecursiveUnion*)list_nth(root->glob->subplans, cte_scan->ctePlanId - 1);
                PlannerInfo* subroot = (PlannerInfo*)list_nth(root->glob->subroots, cte_scan->ctePlanId - 1);

                if (IsA(ru_plan, RecursiveUnion)) {
                    set_plan_references(subroot, (Plan*)ru_plan);
                }
            }
        } break;
        case T_WorkTableScan: {
            WorkTableScan* splan = (WorkTableScan*)plan;

            splan->scan.scanrelid += rtoffset;
            splan->scan.plan.targetlist = fix_scan_list(root, splan->scan.plan.targetlist, rtoffset);
            splan->scan.plan.qual = fix_scan_list(root, splan->scan.plan.qual, rtoffset);
        } break;
#ifdef PGXC
        case T_RemoteQuery:
        case T_VecRemoteQuery: {
            RemoteQuery* splan = (RemoteQuery*)plan;

            /*
             * If base_tlist is set, it means that we have a reduced remote
             * query plan. So need to set the var references accordingly.
             */
            if (splan->base_tlist)
                set_remote_references(root, splan, rtoffset);
#ifdef STREAMPLAN
            if (splan->is_simple && !splan->rq_need_proj) {
                set_dummy_tlist_references(plan, rtoffset);
            } else {
                splan->scan.plan.targetlist = fix_scan_list(root, splan->scan.plan.targetlist, rtoffset);
            }
#endif
            splan->scan.plan.qual = fix_scan_list(root, splan->scan.plan.qual, rtoffset);
            splan->base_tlist = fix_scan_list(root, splan->base_tlist, rtoffset);
            splan->scan.scanrelid += rtoffset;
        } break;
#endif
        case T_ExtensiblePlan:
            set_extensibleplan_references(root, (ExtensiblePlan*)plan, rtoffset);
            break;
        case T_ForeignScan:
        case T_VecForeignScan: {
            ForeignScan* splan = (ForeignScan*)plan;

            splan->scan.scanrelid += rtoffset;
            splan->scan.plan.targetlist = fix_scan_list(root, splan->scan.plan.targetlist, rtoffset);
            if (splan->scan.plan.distributed_keys != NIL) {
                splan->scan.plan.distributed_keys = fix_scan_list(root, splan->scan.plan.distributed_keys, rtoffset);
            }
            splan->scan.plan.var_list = fix_scan_list(root, splan->scan.plan.var_list, rtoffset);
            splan->scan.plan.qual = fix_scan_list(root, splan->scan.plan.qual, rtoffset);
            splan->fdw_exprs = fix_scan_list(root, splan->fdw_exprs, rtoffset);

            if (isObsOrHdfsTableFormTblOid(splan->scan_relid)) {
                DefElem* private_data = (DefElem*)linitial(splan->fdw_private);
                DfsPrivateItem* item = (DfsPrivateItem*)private_data->arg;
                fix_dfs_private_item(root, rtoffset, item);
            }
        } break;
        case T_NestLoop:
        case T_VecNestLoop:
        case T_MergeJoin:
        case T_VecMergeJoin:
        case T_HashJoin:
        case T_VecHashJoin:
            set_join_references(root, (Join*)plan, rtoffset);
            break;

        case T_PartIterator:
        case T_VecPartIterator: {
            PartIterator* splan = (PartIterator*)plan;
            switch (nodeTag(splan->plan.lefttree)) {
                case T_SeqScan:
                case T_CStoreScan:
#ifdef ENABLE_MULTIPLE_NODES
                case T_TsStoreScan:
#endif   /* ENABLE_MULTIPLE_NODES */
                case T_IndexScan:
                case T_IndexOnlyScan:
                case T_BitmapHeapScan:
                case T_TidScan:
                case T_CStoreIndexScan:
                case T_CStoreIndexCtidScan:
                case T_CStoreIndexHeapScan:
                    splan->plan.targetlist = fix_scan_list(root, splan->plan.targetlist, rtoffset);
                    if (splan->plan.distributed_keys != NIL) {
                        splan->plan.distributed_keys = fix_scan_list(root, splan->plan.distributed_keys, rtoffset);
                    }
                    break;
                default:
                    set_dummy_tlist_references(plan, rtoffset);
            }
        } break;

        case T_StartWithOp: {
            set_dummy_tlist_references(plan, rtoffset);
        } break;

        case T_Hash:
        case T_Material:
        case T_VecMaterial:
        case T_Sort:
        case T_VecSort:
        case T_Unique:
        case T_VecUnique:
        case T_SetOp:
        case T_VecSetOp:
        case T_VecToRow:
        case T_RowToVec:

            /*
             * These plan types don't actually bother to evaluate their
             * targetlists, because they just return their unmodified input
             * tuples.	Even though the targetlist won't be used by the
             * executor, we fix it up for possible use by EXPLAIN (not to
             * mention ease of debugging --- wrong varnos are very confusing).
             */
            set_dummy_tlist_references(plan, rtoffset);

            /*
             * Since these plan types don't check quals either, we should not
             * find any qual expression attached to them.
             */
            AssertEreport(plan->qual == NIL, MOD_OPT, "qual should be null");
            break;
        case T_LockRows: {
            LockRows* splan = (LockRows*)plan;

            /*
             * Like the plan types above, LockRows doesn't evaluate its
             * tlist or quals.	But we have to fix up the RT indexes in
             * its rowmarks.
             */
            set_dummy_tlist_references(plan, rtoffset);
            AssertEreport(splan->plan.qual == NIL, MOD_OPT, "qual should be null");

            foreach (l, splan->rowMarks) {
                PlanRowMark* rc = (PlanRowMark*)lfirst(l);

                rc->rti += rtoffset;
                rc->prti += rtoffset;
            }
        } break;
        case T_Limit:
        case T_VecLimit: {
            Limit* splan = (Limit*)plan;

            /*
             * Like the plan types above, Limit doesn't evaluate its tlist
             * or quals.  It does have live expressions for limit/offset,
             * however; and those cannot contain subplan variable refs, so
             * fix_scan_expr works for them.
             */
            set_dummy_tlist_references(plan, rtoffset);
            AssertEreport(splan->plan.qual == NIL, MOD_OPT, "qual should be null");

            splan->limitOffset = fix_scan_expr(root, splan->limitOffset, rtoffset);
            splan->limitCount = fix_scan_expr(root, splan->limitCount, rtoffset);
        } break;
        case T_Agg:
        case T_VecAgg:
#ifdef PGXC
            /* If the lower plan is RemoteQuery plan, adjust the aggregates */
            if (IS_STREAM_PLAN) {
                set_upper_references(root, plan, rtoffset);
                break;
            } else
                pgxc_set_agg_references(root, (Agg*)plan);
                /* Fall through */
#endif /* PGXC */
        case T_Group:
        case T_VecGroup:
            set_upper_references(root, plan, rtoffset);
            break;
        case T_VecWindowAgg:
        case T_WindowAgg: {
            WindowAgg* wplan = (WindowAgg*)plan;

            set_upper_references(root, plan, rtoffset);

            /*
             * Like Limit node limit/offset expressions, WindowAgg has
             * frame offset expressions, which cannot contain subplan
             * variable refs, so fix_scan_expr works for them.
             */
            wplan->startOffset = fix_scan_expr(root, wplan->startOffset, rtoffset);
            wplan->endOffset = fix_scan_expr(root, wplan->endOffset, rtoffset);
        } break;
        case T_BaseResult: {
            BaseResult* splan = (BaseResult*)plan;

            /*
             * Result may or may not have a subplan; if not, it's more
             * like a scan node than an upper node.
             */
            if (splan->plan.lefttree != NULL)
                set_upper_references(root, plan, rtoffset);
            else {
                splan->plan.targetlist = fix_scan_list(root, splan->plan.targetlist, rtoffset);
                splan->plan.qual = fix_scan_list(root, splan->plan.qual, rtoffset);
            }
            /* resconstantqual can't contain any subplan variable refs */
            splan->resconstantqual = fix_scan_expr(root, splan->resconstantqual, rtoffset);
        } break;
        case T_VecResult: {
            VecResult* splan = (VecResult*)plan;

            /*
             * Result may or may not have a subplan; if not, it's more
             * like a scan node than an upper node.
             */
            if (splan->plan.lefttree != NULL)
                set_upper_references(root, plan, rtoffset);
            else {
                splan->plan.targetlist = fix_scan_list(root, splan->plan.targetlist, rtoffset);
                splan->plan.qual = fix_scan_list(root, splan->plan.qual, rtoffset);
            }
            /* resconstantqual can't contain any subplan variable refs */
            splan->resconstantqual = fix_scan_expr(root, splan->resconstantqual, rtoffset);
        } break;
        case T_ModifyTable:
        case T_VecModifyTable: {
            ModifyTable* splan = (ModifyTable*)plan;
#ifdef PGXC
            int n = 0;
            List* firstRetList = NULL; /* First returning list required for
                                        * setting up visible plan target list
                                        */
#endif

            AssertEreport(
                splan->plan.targetlist == NIL && splan->plan.qual == NIL, MOD_OPT, "targelist and qual should be null");

            if (splan->returningLists) {
                List* newRL = NIL;
                ListCell* lcrl = NULL;
                ListCell* lcrr = NULL;
                ListCell* lcp = NULL;

                /*
                 * Pass each per-subplan returningList
                 * through set_returning_clause_references().
                 */
                AssertEreport(list_length(splan->returningLists) == list_length(splan->resultRelations) &&
                                  list_length(splan->returningLists) == list_length(splan->plans),
                    MOD_OPT,
                    "inconsistent state in subplan returningList");
                forthree(lcrl, splan->returningLists, lcrr, splan->resultRelations, lcp, splan->plans)
                {
                    List* rlist = (List*)lfirst(lcrl);
                    Index resultrel = lfirst_int(lcrr);
                    Plan* subplan = (Plan*)lfirst(lcp);
#ifdef PGXC
                    RemoteQuery* rq = NULL;

                    if (n == 0) {
                        /*
                         * Set up first returning list before we change
                         * var references to point to RTE_REMOTE_DUMMY
                         */
                        firstRetList = set_returning_clause_references(root, rlist, subplan, resultrel, rtoffset);
                        /* Restore the returning list changed by the above call */
                        rlist = (List*)lfirst(lcrl);
                    }

                    if (splan->remote_plans)
                        rq = (RemoteQuery*)list_nth(splan->remote_plans, n);
                    n++;

                    if (rq != NULL && IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
                        /*
                         * Set references of returning clause by adjusting
                         * varno/varattno according to target list in
                         * remote query node
                         */
                        rlist = set_remote_returning_refs(root, rlist, (Plan*)rq, rq->scan.scanrelid, rtoffset);
                        /*
                         * The next call to set_returning_clause_references
                         * should skip the vars already taken care of by
                         * the above call to set_remote_returning_refs
                         */
                        resultrel = rq->scan.scanrelid;
                    }
#endif
                    rlist = set_returning_clause_references(root, rlist, subplan, resultrel, rtoffset);
                    newRL = lappend(newRL, rlist);
                }
                splan->returningLists = newRL;

#ifdef PGXC
                /*
                 * In XC we do not need to set the target list as the
                 * first RETURNING list from the finalized list because
                 * it can contain vars referring to RTE_REMOTE_DUMMY.
                 * We therefore create a list before fixing
                 * remote returning references and use that here.
                 */
                splan->plan.targetlist = (List*)copyObject(firstRetList);
#else
                /*
                 * Set up the visible plan targetlist as being the same as
                 * the first RETURNING list. This is for the use of
                 * EXPLAIN; the executor won't pay any attention to the
                 * targetlist.	We postpone this step until here so that
                 * we don't have to do set_returning_clause_references()
                 * twice on identical targetlists.
                 */
                splan->plan.targetlist = copyObject(linitial(newRL));
#endif
            }

            /*
             * The MERGE statement produces the target rows by performing a
             * right join between the target relation and the source
             * relation (which could be a plain relation or a subquery).
             * The INSERT and UPDATE actions of the MERGE statement
             * requires access to the columns from the source relation. We
             * arrange things so that the source relation attributes are
             * available as INNER_VAR and the target relation attributes
             * are available from the scan tuple.
             */
            if (splan->mergeActionList != NIL && (IS_STREAM_PLAN || IS_SINGLE_NODE || IS_PGXC_DATANODE)) {
                /*
                 * mergeSourceTargetList is already setup correctly to
                 * include all Vars coming from the source relation. So we
                 * fix the targetList of individual action nodes by
                 * ensuring that the source relation Vars are referenced
                 * as INNER_VAR. Note that for this to work correctly,
                 * during execution, the ecxt_innertuple must be set to
                 * the tuple obtained from the source relation.
                 *
                 * We leave the Vars from the result relation (i.e. the
                 * target relation) unchanged i.e. those Vars would be
                 * picked from the scan slot. So during execution, we must
                 * ensure that ecxt_scantuple is setup correctly to refer
                 * to the tuple from the target relation.
                 */
                indexed_tlist* itlist = NULL;

                itlist = build_tlist_index(splan->mergeSourceTargetList);

                splan->mergeTargetRelation += rtoffset;

                foreach (l, splan->mergeActionList) {
                    MergeAction* action = (MergeAction*)lfirst(l);

                    /* Fix targetList of each action. */
                    action->targetList = fix_join_expr(
                        root, action->targetList, NULL, itlist, linitial_int(splan->resultRelations), rtoffset);

                    /* Fix quals too. */
                    action->qual = (Node*)fix_join_expr(
                        root, (List*)action->qual, NULL, itlist, linitial_int(splan->resultRelations), rtoffset);
                }
            }

            if (splan->updateTlist != NIL) {
                indexed_tlist* itlist;
                itlist = build_tlist_index(splan->exclRelTlist);
                splan->updateTlist = fix_join_expr(root, splan->updateTlist, NULL,
                    itlist, linitial_int(splan->resultRelations), rtoffset);
                splan->upsertWhere = (Node*)fix_join_expr(root, (List*)splan->upsertWhere, NULL,
                    itlist, linitial_int(splan->resultRelations), rtoffset);
                splan->exclRelTlist =
                        fix_scan_list(root, splan->exclRelTlist, rtoffset);
            }

            splan->exclRelRTIndex += rtoffset;

            foreach (l, splan->resultRelations) {
                lfirst_int(l) += rtoffset;
            }
            foreach (l, splan->rowMarks) {
                PlanRowMark* rc = (PlanRowMark*)lfirst(l);

                rc->rti += rtoffset;
                rc->prti += rtoffset;
            }
            foreach (l, splan->plans) {
                lfirst(l) = set_plan_refs(root, (Plan*)lfirst(l), rtoffset);
            }

            /*
             * Append this ModifyTable node's final result relation RT
             * index(es) to the global list for the plan, and set its
             * resultRelIndex to reflect their starting position in the
             * global list.
             */
            splan->resultRelIndex = list_length(root->glob->resultRelations);
            root->glob->resultRelations = list_concat(root->glob->resultRelations, list_copy(splan->resultRelations));

#ifdef PGXC
            /* Adjust references of remote query nodes in ModifyTable node */
            if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
                ListCell* elt = NULL;
                RemoteQuery* rq = NULL;

                foreach (elt, splan->remote_plans) {
                    rq = (RemoteQuery*)lfirst(elt);

                    if (rq != NULL) {
                        /*
                         * If base_tlist is set, it means that we have a reduced remote
                         * query plan. So need to set the var references accordingly.
                         */
                        if (rq->base_tlist)
                            set_remote_references(root, rq, rtoffset);
                        rq->scan.plan.targetlist = fix_scan_list(root, rq->scan.plan.targetlist, rtoffset);
                        rq->scan.plan.qual = fix_scan_list(root, rq->scan.plan.qual, rtoffset);
                        rq->base_tlist = fix_scan_list(root, rq->base_tlist, rtoffset);
                        rq->scan.scanrelid += rtoffset;
                    }
                }
            }
#endif
        } break;
        case T_Append:
        case T_VecAppend: {
            Append* splan = (Append*)plan;

            /*
             * Append, like Sort et al, doesn't actually evaluate its
             * targetlist or check quals.
             */
            set_dummy_tlist_references(plan, rtoffset);
            AssertEreport(splan->plan.qual == NIL, MOD_OPT, "qual should be null");
            foreach (l, splan->appendplans) {
                lfirst(l) = set_plan_refs(root, (Plan*)lfirst(l), rtoffset);
            }
            delete_redundant_streams_of_append_plan(splan);
        } break;
        case T_MergeAppend: {
            MergeAppend* splan = (MergeAppend*)plan;

            /*
             * MergeAppend, like Sort et al, doesn't actually evaluate its
             * targetlist or check quals.
             */
            set_dummy_tlist_references(plan, rtoffset);
            AssertEreport(splan->plan.qual == NIL, MOD_OPT, "qual should be null");
            foreach (l, splan->mergeplans) {
                lfirst(l) = set_plan_refs(root, (Plan*)lfirst(l), rtoffset);
            }
        } break;
        case T_RecursiveUnion:
            /* This doesn't evaluate targetlist or check quals either */
            set_dummy_tlist_references(plan, rtoffset);
            AssertEreport(plan->qual == NIL, MOD_OPT, "qual should be null");
            break;
        case T_BitmapAnd: {
            BitmapAnd* splan = (BitmapAnd*)plan;

            /* BitmapAnd works like Append, but has no tlist */
            AssertEreport(splan->plan.targetlist == NIL && splan->plan.qual == NIL,
                MOD_OPT,
                "targetlist and qual should be null");
            foreach (l, splan->bitmapplans) {
                lfirst(l) = set_plan_refs(root, (Plan*)lfirst(l), rtoffset);
            }
        } break;
        case T_BitmapOr: {
            BitmapOr* splan = (BitmapOr*)plan;

            /* BitmapOr works like Append, but has no tlist */
            AssertEreport(splan->plan.targetlist == NIL && splan->plan.qual == NIL,
                MOD_OPT,
                "targetlist and qual should be null");
            foreach (l, splan->bitmapplans) {
                lfirst(l) = set_plan_refs(root, (Plan*)lfirst(l), rtoffset);
            }
        } break;
        case T_CStoreIndexAnd: {
            CStoreIndexAnd* splan = (CStoreIndexAnd*)plan;

            /* BitmapAnd works like Append, but has no tlist */
            AssertEreport(splan->plan.targetlist == NIL && splan->plan.qual == NIL,
                MOD_OPT,
                "targetlist and qual should be null");
            foreach (l, splan->bitmapplans) {
                lfirst(l) = set_plan_refs(root, (Plan*)lfirst(l), rtoffset);
            }
        } break;
        case T_CStoreIndexOr: {
            CStoreIndexOr* splan = (CStoreIndexOr*)plan;

            /* BitmapOr works like Append, but has no tlist */
            AssertEreport(splan->plan.targetlist == NIL && splan->plan.qual == NIL, MOD_OPT, "It should be null");
            foreach (l, splan->bitmapplans) {
                lfirst(l) = set_plan_refs(root, (Plan*)lfirst(l), rtoffset);
            }
        } break;
#ifdef STREAMPLAN
        case T_Stream:
        case T_VecStream: {

            /*
             * These plan types don't actually bother to evaluate their
             * targetlists, because they just return their unmodified input
             * tuples.	Even though the targetlist won't be used by the
             * executor, we fix it up for possible use by EXPLAIN (not to
             * mention ease of debugging --- wrong varnos are very confusing).
             */
            Stream* stream = (Stream*)plan;
            Plan* subplan = plan->lefttree;
            indexed_tlist* subplan_itlist = NULL;

            /* Remove LOCAL GATHER for Recursive, which is just lefttree of another normal Stream */
            if (IsA(plan->lefttree, Stream) && ((Stream*)plan->lefttree)->is_recursive_local) {
                plan->lefttree = plan->lefttree->lefttree;
            }

#ifdef USE_ASSERT_CHECKING
            if (plan->plan_node_id > 0) {
                ListCell* cell1 = NULL;
                ListCell* cell2 = NULL;

                forboth(cell1, plan->targetlist, cell2, plan->lefttree->targetlist)
                {
                    TargetEntry* te1 = (TargetEntry*)lfirst(cell1);
                    TargetEntry* te2 = (TargetEntry*)lfirst(cell2);

                    if (!equal(te1->expr, te2->expr)) {
                        ereport(ERROR,
                            (errmodule(MOD_OPT),
                                errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                                errmsg("targetlist of stream node with plan_node_id %d should "
                                       "be equal to its child's targetlist",
                                    plan->plan_node_id)));
                    }
                }
            }
#endif

            set_dummy_tlist_references(plan, rtoffset);
            subplan_itlist = build_tlist_index(subplan->targetlist);

            /* Not only redistribute stream, but also hybrid stream needs distribute keys. */
            if (stream->distribute_keys != NIL) {
                stream->distribute_keys =
                    (List*)fix_upper_expr(root, (Node*)stream->distribute_keys, subplan_itlist, OUTER_VAR, rtoffset);
            }

            if (stream->skew_list != NIL)
                fix_skew_quals(root, plan, subplan_itlist, rtoffset);
        } break;
#endif
        default: {
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmsg("unrecognized node type in set plan refs: %d", (int)nodeTag(plan))));
        } break;
    }

    /*
     * Now recurse into child plans, if any
     *
     * NOTE: it is essential that we recurse into child plans AFTER we set
     * subplan references in this plan's tlist and quals.  If we did the
     * reference-adjustments bottom-up, then we would fail to match this
     * plan's var nodes against the already-modified nodes of the children.
     */
    plan->lefttree = set_plan_refs(root, plan->lefttree, rtoffset);
    plan->righttree = set_plan_refs(root, plan->righttree, rtoffset);

    return plan;
}

/*
 * set_indexonlyscan_references
 *		Do set_plan_references processing on an IndexOnlyScan
 *
 * This is unlike the handling of a plain IndexScan because we have to
 * convert Vars referencing the heap into Vars referencing the index.
 * We can use the fix_upper_expr machinery for that, by working from a
 * targetlist describing the index columns.
 */
static Plan* set_indexonlyscan_references(PlannerInfo* root, IndexOnlyScan* plan, int rtoffset)
{
    indexed_tlist* index_itlist = NULL;

    index_itlist = build_tlist_index(plan->indextlist);

    plan->scan.scanrelid += rtoffset;
    plan->scan.plan.targetlist =
        (List*)fix_upper_expr(root, (Node*)plan->scan.plan.targetlist, index_itlist, INDEX_VAR, rtoffset);
    plan->scan.plan.qual = (List*)fix_upper_expr(root, (Node*)plan->scan.plan.qual, index_itlist, INDEX_VAR, rtoffset);
    /* indexqual is already transformed to reference index columns */
    plan->indexqual = fix_scan_list(root, plan->indexqual, rtoffset);
    /* indexorderby is already transformed to reference index columns */
    plan->indexorderby = fix_scan_list(root, plan->indexorderby, rtoffset);
    /* indextlist must NOT be transformed to reference index columns */
    plan->indextlist = fix_scan_list(root, plan->indextlist, rtoffset);

    pfree_ext(index_itlist);

    return (Plan*)plan;
}

/*
 * set_subqueryscan_references
 *		Do set_plan_references processing on a SubqueryScan
 *
 * We try to strip out the SubqueryScan entirely; if we can't, we have
 * to do the normal processing on it.
 */
static Plan* set_subqueryscan_references(PlannerInfo* root, SubqueryScan* plan, int rtoffset)
{
    RelOptInfo* rel = NULL;
    Plan* result = NULL;
    PlannerInfo* subroot = NULL;

    /* Need to look up the subquery's RelOptInfo, since we need its subroot */
    rel = find_base_rel(root, plan->scan.scanrelid);

    /*
     * Fallback/vectorize plan may cause the difference between rel->subplan
     * and plan->subplan. If the plan->subplan is a VecToRow or RowToVec node,
     * check the lefttree of plan->subplan instead.
     *
     * We can skip the check when the plan is under recursive cte, since we may
     * copy the plan and use the original planner info, in this case the plans are
     * definitely different
     */
    if (!IsA(plan->subplan, VecToRow) && !IsA(plan->subplan, RowToVec)) {
        /*
         * We can skip the check when the plan is under recursive cte, since we may
         * copy the plan and use the original planner info, in this case the plans are
         * definitely different
         */
        AssertEreport(root->is_under_recursive_cte || rel->subplan == plan->subplan ||
                    (((RelOptInfo*)linitial(rel->alternatives))->subplan == plan->subplan),
                    MOD_OPT,
                    "inconsistent state after fallback/vectorize plan");
    } else {
        AssertEreport(root->is_under_recursive_cte || rel->subplan == plan->subplan->lefttree ||
                    (((RelOptInfo*)linitial(rel->alternatives))->subplan == plan->subplan->lefttree),
            MOD_OPT,
            "inconsistent state after fallback/vectorize plan");
    }

    /* Recursively process the subplan */
    if (rel->alternatives != NIL && ((RelOptInfo*)linitial(rel->alternatives))->subplan == plan->subplan) {
        subroot = ((RelOptInfo*)linitial(rel->alternatives))->subroot;
    } else {
        subroot = rel->subroot;
    }
    plan->subplan = set_plan_references(subroot, plan->subplan);

    if (trivial_subqueryscan(plan)) {
        /*
         * We can omit the SubqueryScan node and just pull up the subplan.
         */
        ListCell* lp = NULL;
        ListCell* lc = NULL;

        result = plan->subplan;

        /* We have to be sure we don't lose any initplans */
        result->initPlan = list_concat(plan->scan.plan.initPlan, result->initPlan);

        /*
         * We also have to transfer the SubqueryScan's result-column names
         * into the subplan, else columns sent to client will be improperly
         * labeled if this is the topmost plan level.  Copy the "source
         * column" information too.
         */
        forboth(lp, plan->scan.plan.targetlist, lc, result->targetlist)
        {
            TargetEntry* ptle = (TargetEntry*)lfirst(lp);
            TargetEntry* ctle = (TargetEntry*)lfirst(lc);

            ctle->resname = ptle->resname;
            ctle->resorigtbl = ptle->resorigtbl;
            ctle->resorigcol = ptle->resorigcol;
        }
    } else {
        /*
         * Keep the SubqueryScan node.	We have to do the processing that
         * set_plan_references would otherwise have done on it.  Notice we do
         * not do set_upper_references() here, because a SubqueryScan will
         * always have been created with correct references to its subplan's
         * outputs to begin with.
         */
        plan->scan.scanrelid += rtoffset;
        plan->scan.plan.targetlist = fix_scan_list(root, plan->scan.plan.targetlist, rtoffset);
        plan->scan.plan.qual = fix_scan_list(root, plan->scan.plan.qual, rtoffset);

        result = (Plan*)plan;
    }

    return result;
}

static void set_tlist_qual_extensible_exprs_of_extensibleplan(PlannerInfo* root, ExtensiblePlan* cscan, int rtoffset)
{
    if (cscan->extensible_plan_tlist != NIL || cscan->scan.scanrelid == 0) {
        /* Adjust tlist, qual, extensible_exprs to reference extensible scan tuple */
        indexed_tlist* itlist = build_tlist_index(cscan->extensible_plan_tlist);

        cscan->scan.plan.targetlist =
            (List*)fix_upper_expr(root, (Node*)cscan->scan.plan.targetlist, itlist, INDEX_VAR, rtoffset);
        cscan->scan.plan.qual = (List*)fix_upper_expr(root, (Node*)cscan->scan.plan.qual, itlist, INDEX_VAR, rtoffset);
        cscan->extensible_exprs =
            (List*)fix_upper_expr(root, (Node*)cscan->extensible_exprs, itlist, INDEX_VAR, rtoffset);
        pfree(itlist);
        /* extensible_plan_tlist itself just needs fix_scan_list() adjustments */
        cscan->extensible_plan_tlist = fix_scan_list(root, cscan->extensible_plan_tlist, rtoffset);
    } else {
        /* Adjust tlist, qual, extensible_exprs in the standard way */
        cscan->scan.plan.targetlist = fix_scan_list(root, cscan->scan.plan.targetlist, rtoffset);
        cscan->scan.plan.qual = fix_scan_list(root, cscan->scan.plan.qual, rtoffset);
        cscan->extensible_exprs = fix_scan_list(root, cscan->extensible_exprs, rtoffset);
    }
}
/*
 * set_extensibleplan_references
 *	   Do set_plan_references processing on a ExtensiblePlan
 */
static void set_extensibleplan_references(PlannerInfo* root, ExtensiblePlan* cscan, int rtoffset)
{
    ListCell* lc = NULL;

    /* Adjust scanrelid if it's valid */
    if (cscan->scan.scanrelid > 0)
        cscan->scan.scanrelid += (unsigned int)rtoffset;

    set_tlist_qual_extensible_exprs_of_extensibleplan(root, cscan, rtoffset);
    /* Adjust child plan-nodes recursively, if needed */
    foreach (lc, cscan->extensible_plans) {
        lfirst(lc) = set_plan_refs(root, (Plan*)lfirst(lc), rtoffset);
    }

    /* Adjust extensible_relids if needed */
    if (rtoffset > 0) {
        Bitmapset* tempset = NULL;
        int x = -1;

        while ((x = bms_next_member(cscan->extensible_relids, x)) >= 0)
            tempset = bms_add_member(tempset, x + rtoffset);
        cscan->extensible_relids = tempset;
    }
}

/*
 * Tell if the plan is a broadcast stream.
 */
static bool is_plan_a_broadcast_stream(Plan* plan)
{
    if (plan == NULL)
        return false;

    if (IsA(plan, Stream) || IsA(plan, VecStream)) {
        Stream* stream_plan = (Stream*)plan;

        if (is_broadcast_stream(stream_plan))
            return true;
    }

    return false;
}

/*
 * trivial_subqueryscan
 *		Detect whether a SubqueryScan can be deleted from the plan tree.
 *
 * We can delete it if it has no qual to check and the targetlist just
 * regurgitates the output of the child plan.
 */
bool trivial_subqueryscan(SubqueryScan* plan)
{
    int attrno;
    ListCell* lp = NULL;
    ListCell* lc = NULL;

    if (plan->scan.plan.qual != NIL)
        return false;

    /*
     * remove the subquery upon broadcast stream, while later add a Gather
     * would produce a Gather->Broadcast plan which is illegal
     */
    if (is_plan_a_broadcast_stream(plan->subplan))
        return false;

    if (list_length(plan->scan.plan.targetlist) != list_length(plan->subplan->targetlist))
        return false; /* tlists not same length */

    attrno = 1;
    forboth(lp, plan->scan.plan.targetlist, lc, plan->subplan->targetlist)
    {
        TargetEntry* ptle = (TargetEntry*)lfirst(lp);
        TargetEntry* ctle = (TargetEntry*)lfirst(lc);

        if (ptle->resjunk != ctle->resjunk)
            return false; /* tlist doesn't match junk status */

        /*
         * We accept either a Var referencing the corresponding element of the
         * subplan tlist, or a Const equaling the subplan element. See
         * generate_setop_tlist() for motivation.
         */
        if (ptle->expr && IsA(ptle->expr, Var)) {
            Var* var = (Var*)ptle->expr;

            AssertEreport(var->varno == plan->scan.scanrelid && var->varlevelsup == 0,
                MOD_OPT,
                "var should be at current level and varno should be equal plan's scanrelid");
            if (var->varattno != attrno)
                return false; /* out of order */
        } else if (ptle->expr && IsA(ptle->expr, Const)) {
            if (!equal(ptle->expr, ctle->expr))
                return false;
        } else
            return false;

        attrno++;
    }

    return true;
}

/*
 * copyVar
 *		Copy a Var node.
 *
 * fix_scan_expr and friends do this enough times that it's worth having
 * a bespoke routine instead of using the generic copyObject() function.
 */
static inline Var* copyVar(Var* var)
{
    Var* newvar = (Var*)palloc(sizeof(Var));

    *newvar = *var;
    return newvar;
}

/*
 * fix_expr_common
 *		Do generic set_plan_references processing on an expression node
 *
 * This is code that is common to all variants of expression-fixing.
 * We must look up operator opcode info for OpExpr and related nodes,
 * add OIDs from regclass Const nodes into root->glob->relationOids, and
 * add catalog TIDs for user-defined functions into root->glob->invalItems.
 *
 * We assume it's okay to update opcode info in-place.  So this could possibly
 * scribble on the planner's input data structures, but it's OK.
 */
static void fix_expr_common(PlannerInfo* root, Node* node)
{
    /* We assume callers won't call us on a NULL pointer */
    if (IsA(node, Aggref)) {
        record_plan_function_dependency(root, ((Aggref*)node)->aggfnoid);
    } else if (IsA(node, WindowFunc)) {
        record_plan_function_dependency(root, ((WindowFunc*)node)->winfnoid);
    } else if (IsA(node, FuncExpr)) {
        record_plan_function_dependency(root, ((FuncExpr*)node)->funcid);
    } else if (IsA(node, OpExpr)) {
        set_opfuncid((OpExpr*)node);
        record_plan_function_dependency(root, ((OpExpr*)node)->opfuncid);
    } else if (IsA(node, DistinctExpr)) {
        set_opfuncid((OpExpr*)node); /* rely on struct equivalence */
        record_plan_function_dependency(root, ((DistinctExpr*)node)->opfuncid);
    } else if (IsA(node, NullIfExpr)) {
        set_opfuncid((OpExpr*)node); /* rely on struct equivalence */
        record_plan_function_dependency(root, ((NullIfExpr*)node)->opfuncid);
    } else if (IsA(node, ScalarArrayOpExpr)) {
        set_sa_opfuncid((ScalarArrayOpExpr*)node);
        record_plan_function_dependency(root, ((ScalarArrayOpExpr*)node)->opfuncid);
    } else if (IsA(node, ArrayCoerceExpr)) {
        if (OidIsValid(((ArrayCoerceExpr*)node)->elemfuncid))
            record_plan_function_dependency(root, ((ArrayCoerceExpr*)node)->elemfuncid);
    } else if (IsA(node, Const)) {
        Const* con = (Const*)node;

        /* Check for regclass reference */
        if (ISREGCLASSCONST(con))
            root->glob->relationOids = lappend_oid(root->glob->relationOids, DatumGetObjectId(con->constvalue));
    } else if (IsA(node, GroupingFunc)) {
        GroupingFunc* g = (GroupingFunc*)node;
        AttrNumber* grouping_map = root->grouping_map;

        /* If there are no grouping sets, we don't need this. */
        AssertEreport(grouping_map || g->cols == NIL, MOD_OPT, "If there are no grouping sets, we don't need this.");

        if (grouping_map != NULL) {
            ListCell* lc = NULL;
            List* cols = NIL;

            foreach (lc, g->refs) {
                cols = lappend_int(cols, grouping_map[lfirst_int(lc)]);
            }

            AssertEreport(g->cols == NIL || equal(cols, g->cols), MOD_OPT, "inconsistent g->cols state.");

            if (!g->cols)
                g->cols = cols;
        }
    }
}

/*
 * fix_scan_expr
 *		Do set_plan_references processing on a scan-level expression
 *
 * This consists of incrementing all Vars' varnos by rtoffset,
 * looking up operator opcode info for OpExpr and related nodes,
 * and adding OIDs from regclass Const nodes into root->glob->relationOids.
 */
static Node* fix_scan_expr(PlannerInfo* root, Node* node, int rtoffset)
{
    fix_scan_expr_context context;

    context.root = root;
    context.rtoffset = rtoffset;

    if (rtoffset != 0 || root->glob->lastPHId != 0) {
        return fix_scan_expr_mutator(node, &context);
    } else {
        /*
         * If rtoffset == 0, we don't need to change any Vars, and if there
         * are no placeholders anywhere we won't need to remove them.  Then
         * it's OK to just scribble on the input node tree instead of copying
         * (since the only change, filling in any unset opfuncid fields, is
         * harmless).  This saves just enough cycles to be noticeable on
         * trivial queries.
         */
        (void)fix_scan_expr_walker(node, &context);
        return node;
    }
}

/**
 * @Description: Because zero out RangeTblEntry fields that are not useful to the
 * the executor in optimizer phase, Adjust varno of Var in DfsPrivateItem node to be
 * consistent with the flat rangetable.
 */
static void fix_dfs_private_item(PlannerInfo* root, int rte_offset, DfsPrivateItem* item)
{
    if (item == NULL) {
        return;
    }
    item->opExpressionList = fix_scan_list(root, item->opExpressionList, rte_offset);
    item->hdfsQual = fix_scan_list(root, item->hdfsQual, rte_offset);
    item->columnList = fix_scan_list(root, item->columnList, rte_offset);
    item->restrictColList = fix_scan_list(root, item->restrictColList, rte_offset);
    item->targetList = fix_scan_list(root, item->targetList, rte_offset);
}

static Node* fix_scan_expr_mutator(Node* node, fix_scan_expr_context* context)
{
    if (node == NULL)
        return NULL;
    if (IsA(node, Var)) {
        Var* var = copyVar((Var*)node);

        AssertEreport(var->varlevelsup == 0, MOD_OPT, "unexpected var's varlevelsup value");

        /*
         * We should not see any Vars marked INNER_VAR or OUTER_VAR.  But an
         * indexqual expression could contain INDEX_VAR Vars.
         */
        AssertEreport(var->varno != INNER_VAR && var->varno != OUTER_VAR,
            MOD_OPT,
            "should not any Vars marked INNER_VAR or OUTER_VAR");
        if (!IS_SPECIAL_VARNO(var->varno))
            var->varno += context->rtoffset;
        if (var->varnoold > 0)
            var->varnoold += context->rtoffset;
        return (Node*)var;
    }
    if (IsA(node, CurrentOfExpr)) {
        CurrentOfExpr* cexpr = (CurrentOfExpr*)copyObject(node);

        AssertEreport(cexpr->cvarno != INNER_VAR && cexpr->cvarno != OUTER_VAR,
            MOD_OPT,
            "should not any Vars marked INNER_VAR or OUTER_VAR");
        if (!IS_SPECIAL_VARNO(cexpr->cvarno))
            cexpr->cvarno += context->rtoffset;
        return (Node*)cexpr;
    }
    if (IsA(node, PlaceHolderVar)) {
        /* At scan level, we should always just evaluate the contained expr */
        PlaceHolderVar* phv = (PlaceHolderVar*)node;

        return fix_scan_expr_mutator((Node*)phv->phexpr, context);
    }
    fix_expr_common(context->root, node);
    return expression_tree_mutator(node, (Node* (*)(Node*, void*)) fix_scan_expr_mutator, (void*)context);
}

static bool fix_scan_expr_walker(Node* node, fix_scan_expr_context* context)
{
    if (node == NULL)
        return false;
    AssertEreport(!IsA(node, PlaceHolderVar), MOD_OPT, "unexpected node type.");
    fix_expr_common(context->root, node);
    return expression_tree_walker(node, (bool (*)())fix_scan_expr_walker, (void*)context);
}

/*
 * set_join_references
 *	  Modify the target list and quals of a join node to reference its
 *	  subplans, by setting the varnos to OUTER_VAR or INNER_VAR and setting
 *	  attno values to the result domain number of either the corresponding
 *	  outer or inner join tuple item.  Also perform opcode lookup for these
 *	  expressions. and add regclass OIDs to root->glob->relationOids.
 */
static void set_join_references(PlannerInfo* root, Join* join, int rtoffset)
{
    Plan* outer_plan = join->plan.lefttree;
    Plan* inner_plan = join->plan.righttree;
    indexed_tlist* outer_itlist = NULL;
    indexed_tlist* inner_itlist = NULL;

    outer_itlist = build_tlist_index(outer_plan->targetlist);
    inner_itlist = build_tlist_index(inner_plan->targetlist);

    /*
     * First process the joinquals (including merge or hash clauses).  These
     * are logically below the join so they can always use all values
     * available from the input tlists.  It's okay to also handle
     * NestLoopParams now, because those couldn't refer to nullable
     * subexpressions.
     */
    join->joinqual = fix_join_expr(root, join->joinqual, outer_itlist, inner_itlist, (Index)0, rtoffset);
    join->nulleqqual = fix_join_expr(root, join->nulleqqual, outer_itlist, inner_itlist, (Index)0, rtoffset);
    /*
     * Now we need to fix up the targetlist and qpqual, which are logically
     * above the join.  This means they should not re-use any input expression
     * that was computed in the nullable side of an outer join.  Vars and
     * PlaceHolderVars are fine, so we can implement this restriction just by
     * clearing has_non_vars in the indexed_tlist structs.
     *
     * XXX This is a grotty workaround for the fact that we don't clearly
     * distinguish between a Var appearing below an outer join and the "same"
     * Var appearing above it.  If we did, we'd not need to hack the matching
     * rules this way.
     */
    switch (join->jointype) {
        case JOIN_LEFT:
        case JOIN_SEMI:
        case JOIN_ANTI:
        case JOIN_LEFT_ANTI_FULL:
            inner_itlist->has_non_vars = false;
            break;

        case JOIN_RIGHT:
        case JOIN_RIGHT_SEMI:
        case JOIN_RIGHT_ANTI:
        case JOIN_RIGHT_ANTI_FULL:
            outer_itlist->has_non_vars = false;
            break;

        case JOIN_FULL:
            outer_itlist->has_non_vars = false;
            inner_itlist->has_non_vars = false;
            break;
        default:
            break;
    }

    /* All join plans have tlist, qual, and joinqual */
    join->plan.targetlist = fix_join_expr(root, join->plan.targetlist, outer_itlist, inner_itlist, (Index)0, rtoffset);
    join->plan.var_list = fix_join_expr(root, join->plan.var_list, outer_itlist, inner_itlist, (Index)0, rtoffset);
    join->plan.qual = fix_join_expr(root, join->plan.qual, outer_itlist, inner_itlist, (Index)0, rtoffset);

    /* Now do join-type-specific stuff */
    if (IsA(join, NestLoop) || IsA(join, VecNestLoop)) {
        NestLoop* nl = (NestLoop*)join;
        ListCell* lc = NULL;

        foreach (lc, nl->nestParams) {
            NestLoopParam* nlp = (NestLoopParam*)lfirst(lc);

            nlp->paramval = (Var*)fix_upper_expr(root, (Node*)nlp->paramval, outer_itlist, OUTER_VAR, rtoffset);
            /* Check we replaced any PlaceHolderVar with simple Var */
            if (!(IsA(nlp->paramval, Var) && nlp->paramval->varno == OUTER_VAR))
                ereport(ERROR,
                    (errmodule(MOD_OPT),
                        errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                        (errmsg("NestLoopParam was not reduced to a simple Var"))));
        }
    } else if (IsA(join, MergeJoin) || IsA(join, VecMergeJoin)) {
        MergeJoin* mj = (MergeJoin*)join;

        mj->mergeclauses = fix_join_expr(root, mj->mergeclauses, outer_itlist, inner_itlist, (Index)0, rtoffset);
    } else if (IsA(join, HashJoin) || IsA(join, VecHashJoin)) {
        HashJoin* hj = (HashJoin*)join;

        hj->hashclauses = fix_join_expr(root, hj->hashclauses, outer_itlist, inner_itlist, (Index)0, rtoffset);
    }

    pfree_ext(outer_itlist);
    pfree_ext(inner_itlist);
}

#ifndef ENABLE_MULTIPLE_NODES
static bool has_same_groupby(Plan* plan, Plan* subplan)
{
    if (!IsA(plan, Agg) || !IsA(subplan, Agg)) {
        return true;
    }

    Agg* agg = (Agg*)plan;
    Agg* subagg = (Agg*)subplan;

    if (agg->numCols != subagg->numCols)
        return false;

    for (int16 i = 0; i < agg->numCols; i++) {
        if (agg->grpColIdx[i] != subagg->grpColIdx[i])
            return false;
    }
    return true;
}
#endif

/*
 * set_upper_references
 *	  Update the targetlist and quals of an upper-level plan node
 *	  to refer to the tuples returned by its lefttree subplan.
 *	  Also perform opcode lookup for these expressions, and
 *	  add regclass OIDs to root->glob->relationOids.
 *
 * This is used for single-input plan types like Agg, Group, Result.
 *
 * In most cases, we have to match up individual Vars in the tlist and
 * qual expressions with elements of the subplan's tlist (which was
 * generated by flatten_tlist() from these selfsame expressions, so it
 * should have all the required variables).  There is an important exception,
 * however: GROUP BY and ORDER BY expressions will have been pushed into the
 * subplan tlist unflattened.  If these values are also needed in the output
 * then we want to reference the subplan tlist element rather than recomputing
 * the expression.
 */
static void set_upper_references(PlannerInfo* root, Plan* plan, int rtoffset)
{
    Plan* subplan = plan->lefttree;
    indexed_tlist* subplan_itlist = NULL;
    List* output_targetlist = NIL;
    ListCell* l = NULL;
#ifndef ENABLE_MULTIPLE_NODES
    bool groupby_same = has_same_groupby(plan, subplan);
#endif

    subplan_itlist = build_tlist_index(subplan->targetlist);

    output_targetlist = NIL;
    foreach (l, plan->targetlist) {
        TargetEntry* tle = (TargetEntry*)lfirst(l);
        Node* newexpr = NULL;

        /* If it's a non-Var sort/group item, first try to match by sortref */
#ifdef ENABLE_MULTIPLE_NODES
        if (tle->ressortgroupref != 0 && !IsA(tle->expr, Var)) {
#else
        if (tle->ressortgroupref != 0 && !IsA(tle->expr, Var) && groupby_same) {
#endif
            newexpr = (Node*)search_indexed_tlist_for_sortgroupref(
                (Node*)tle->expr, tle->ressortgroupref, subplan_itlist, OUTER_VAR);
            if (newexpr == NULL)
                newexpr = fix_upper_expr(root, (Node*)tle->expr, subplan_itlist, OUTER_VAR, rtoffset);
        } else
            newexpr = fix_upper_expr(root, (Node*)tle->expr, subplan_itlist, OUTER_VAR, rtoffset);
        tle = flatCopyTargetEntry(tle);
        tle->expr = (Expr*)newexpr;
        output_targetlist = lappend(output_targetlist, tle);
    }
    plan->targetlist = output_targetlist;

    /*
     * For Agg, qual's const can not replace with Var, because const can be set to NULL
     * in Ap function.
     */
    if (IsA(plan, Agg) || IsA(plan, VecAgg)) {
        subplan_itlist->return_const = true;
    }

    plan->qual = (List*)fix_upper_expr(root, (Node*)plan->qual, subplan_itlist, OUTER_VAR, rtoffset);

    pfree_ext(subplan_itlist);
}

/*
 * set_dummy_tlist_references
 *	  Replace the targetlist of an upper-level plan node with a simple
 *	  list of OUTER_VAR references to its child.
 *
 * This is used for plan types like Sort and Append that don't evaluate
 * their targetlists.  Although the executor doesn't care at all what's in
 * the tlist, EXPLAIN needs it to be realistic.
 *
 * Note: we could almost use set_upper_references() here, but it fails for
 * Append for lack of a lefttree subplan.  Single-purpose code is faster
 * anyway.
 */
static void set_dummy_tlist_references(Plan* plan, int rtoffset)
{
    List* output_targetlist = NIL;
    ListCell* l = NULL;

    output_targetlist = NIL;
    foreach (l, plan->targetlist) {
        TargetEntry* tle = (TargetEntry*)lfirst(l);
        Var* oldvar = (Var*)tle->expr;
        Var* newvar = NULL;

        newvar = makeVar(
            OUTER_VAR, tle->resno, exprType((Node*)oldvar), exprTypmod((Node*)oldvar), exprCollation((Node*)oldvar), 0);
        if (IsA(oldvar, Var)) {
            newvar->varnoold = oldvar->varno + rtoffset;
            newvar->varoattno = oldvar->varattno;
        } else {
            newvar->varnoold = 0; /* wasn't ever a plain Var */
            newvar->varoattno = 0;
        }

        tle = flatCopyTargetEntry(tle);
        tle->expr = (Expr*)newvar;
        output_targetlist = lappend(output_targetlist, tle);
    }
    plan->targetlist = output_targetlist;

    /* We don't touch plan->qual here */
}

/*
 * build_tlist_index --- build an index data structure for a child tlist
 *
 * In most cases, subplan tlists will be "flat" tlists with only Vars,
 * so we try to optimize that case by extracting information about Vars
 * in advance.	Matching a parent tlist to a child is still an O(N^2)
 * operation, but at least with a much smaller constant factor than plain
 * tlist_member() searches.
 *
 * The result of this function is an indexed_tlist struct to pass
 * to search_indexed_tlist_for_var() or search_indexed_tlist_for_non_var().
 * When done, the indexed_tlist may be freed with a single pfree_ext().
 */
static indexed_tlist* build_tlist_index(List* tlist)
{
    indexed_tlist* itlist = NULL;
    tlist_vinfo* vinfo = NULL;
    ListCell* l = NULL;

    /* Create data structure with enough slots for all tlist entries */
    itlist = (indexed_tlist*)palloc(offsetof(indexed_tlist, vars) + list_length(tlist) * sizeof(tlist_vinfo));

    itlist->tlist = tlist;
    itlist->has_ph_vars = false;
    itlist->has_non_vars = false;
    itlist->return_const = false;

    /* Find the Vars and fill in the index array */
    vinfo = itlist->vars;
    foreach (l, tlist) {
        TargetEntry* tle = (TargetEntry*)lfirst(l);

        if (tle->expr && IsA(tle->expr, Var)) {
            Var* var = (Var*)tle->expr;

            vinfo->varno = var->varno;
            vinfo->varattno = var->varattno;
            vinfo->resno = tle->resno;
            vinfo++;
        } else if (tle->expr && IsA(tle->expr, PlaceHolderVar))
            itlist->has_ph_vars = true;
        else
            itlist->has_non_vars = true;
    }

    itlist->num_vars = (vinfo - itlist->vars);

    return itlist;
}

/*
 * build_tlist_index_other_vars --- build a restricted tlist index
 *
 * This is like build_tlist_index, but we only index tlist entries that
 * are Vars belonging to some rel other than the one specified.  We will set
 * has_ph_vars (allowing PlaceHolderVars to be matched), but not has_non_vars
 * (so nothing other than Vars and PlaceHolderVars can be matched).
 */
static indexed_tlist* build_tlist_index_other_vars(List* tlist, Index ignore_rel)
{
    indexed_tlist* itlist = NULL;
    tlist_vinfo* vinfo = NULL;
    ListCell* l = NULL;

    /* Create data structure with enough slots for all tlist entries */
    itlist = (indexed_tlist*)palloc(offsetof(indexed_tlist, vars) + list_length(tlist) * sizeof(tlist_vinfo));

    itlist->tlist = tlist;
    itlist->has_ph_vars = false;
    itlist->has_non_vars = false;
    itlist->return_const = false;

    /* Find the desired Vars and fill in the index array */
    vinfo = itlist->vars;
    foreach (l, tlist) {
        TargetEntry* tle = (TargetEntry*)lfirst(l);

        if (tle->expr && IsA(tle->expr, Var)) {
            Var* var = (Var*)tle->expr;

            if (var->varno != ignore_rel) {
                vinfo->varno = var->varno;
                vinfo->varattno = var->varattno;
                vinfo->resno = tle->resno;
                vinfo++;
            }
        } else if (tle->expr && IsA(tle->expr, PlaceHolderVar))
            itlist->has_ph_vars = true;
    }

    itlist->num_vars = (vinfo - itlist->vars);

    return itlist;
}

/*
 * search_indexed_tlist_for_var --- find a Var in an indexed tlist
 *
 * If a match is found, return a copy of the given Var with suitably
 * modified varno/varattno (to wit, newvarno and the resno of the TLE entry).
 * Also ensure that varnoold is incremented by rtoffset.
 * If no match, return NULL.
 */
static Var* search_indexed_tlist_for_var(Var* var, indexed_tlist* itlist, Index newvarno, int rtoffset)
{
    Index varno = var->varno;
    AttrNumber varattno = var->varattno;
    tlist_vinfo* vinfo = NULL;
    int i;

    vinfo = itlist->vars;
    i = itlist->num_vars;
    while (i-- > 0) {
        if (vinfo->varno == varno && vinfo->varattno == varattno) {
            /* Found a match */
            Var* newvar = copyVar(var);

            newvar->varno = newvarno;
            newvar->varattno = vinfo->resno;
            if (newvar->varnoold > 0)
                newvar->varnoold += rtoffset;
            return newvar;
        }
        vinfo++;
    }
    return NULL; /* no match */
}

/*
 * search_indexed_tlist_for_non_var --- find a non-Var in an indexed tlist
 *
 * If a match is found, return a Var constructed to reference the tlist item.
 * If no match, return NULL.
 *
 * NOTE: it is a waste of time to call this unless itlist->has_ph_vars or
 * itlist->has_non_vars. Furthermore, set_join_references() relies on being
 * able to prevent matching of non-Vars by clearing itlist->has_non_vars,
 * so there's a correctness reason not to call it unless that's set.
 */
static Var* search_indexed_tlist_for_non_var(Node* node, indexed_tlist* itlist, Index newvarno)
{
    TargetEntry* tle = NULL;
    bool nested_agg = false;
    bool nested_relabeltype = false;

    /* Only used this const rather than Var. */
    if (itlist->return_const && IsA(node, Const)) {
        return NULL;
    }

#ifdef ENABLE_MULTIPLE_NODES
    if (IS_PGXC_COORDINATOR && IS_STREAM) {
#else
    if (IS_STREAM) {
#endif
        tle = tlist_member_except_aggref(node, itlist->tlist, &nested_agg, &nested_relabeltype);
    } else {
        tle = tlist_member(node, itlist->tlist);
    }
    if (tle != NULL) {
        /* Found a matching subplan output expression */
        Var* newvar = NULL;

        newvar = makeVarFromTargetEntry(newvarno, tle);
        newvar->varnoold = 0; /* wasn't ever a plain Var */
        newvar->varoattno = 0;
#ifdef STREAMPLAN
        if (nested_agg) {
            Aggref* agg_node = (Aggref*)node;
            TargetEntry* new_tle = NULL;
            TargetEntry* sec_tle = NULL;
            char* resname = tle->resname != NULL ? pstrdup(tle->resname) : NULL;

            AssertEreport(IsA(node, Aggref), MOD_OPT, "node type aggref is required.");
            if (agg_node->args != NULL) {
                /*
                 * Supplement the second argument for string_agg at top level in
                 * nested agg scene.
                 * e.g. string_agg((string_agg((id1)::text, ','::text)), ','::text)
                 */
                if (agg_node->aggfnoid == STRINGAGGFUNCOID && IsA(lsecond(agg_node->args), TargetEntry))
                    sec_tle = flatCopyTargetEntry((TargetEntry*)lsecond(agg_node->args));

                list_free_deep(agg_node->args);
            }

            new_tle = makeTargetEntry((Expr*)newvar, 1, resname, tle->resjunk);

            if (sec_tle == NULL)
                agg_node->args = list_make1(new_tle);
            else
                agg_node->args = list_make2(new_tle, sec_tle);
            return (Var*)agg_node;
        } else if (nested_relabeltype) {
            /*
             * We should make sure the the return type of the node unchanging, so wrap the Var node
             * with the origin relabeltype node.
             */
            RelabelType* newnode = (RelabelType*)node;
            newnode->arg = (Expr*)newvar;
            return (Var*)newnode;
        } else
#endif
            return newvar;
    }

    return NULL; /* no match */
}

/*
 * search_indexed_tlist_for_sortgroupref --- find a sort/group expression
 *		(which is assumed not to be just a Var)
 *
 * If a match is found, return a Var constructed to reference the tlist item.
 * If no match, return NULL.
 *
 * This is needed to ensure that we select the right subplan TLE in cases
 * where there are multiple textually-equal()-but-volatile sort expressions.
 * And it's also faster than search_indexed_tlist_for_non_var.
 */
static Var* search_indexed_tlist_for_sortgroupref(Node* node, Index sortgroupref, indexed_tlist* itlist, Index newvarno)
{
    ListCell* lc = NULL;

    foreach (lc, itlist->tlist) {
        TargetEntry* tle = (TargetEntry*)lfirst(lc);

        /* The equal() check should be redundant, but let's be paranoid */
        if (tle->ressortgroupref == sortgroupref && equal(node, tle->expr)) {
            /* Found a matching subplan output expression */
            Var* newvar = NULL;

            newvar = makeVarFromTargetEntry(newvarno, tle);
            newvar->varnoold = 0; /* wasn't ever a plain Var */
            newvar->varoattno = 0;
            return newvar;
        }
    }
    return NULL; /* no match */
}

/*
 * fix_join_expr
 *	   Create a new set of targetlist entries or join qual clauses by
 *	   changing the varno/varattno values of variables in the clauses
 *	   to reference target list values from the outer and inner join
 *	   relation target lists.  Also perform opcode lookup and add
 *	   regclass OIDs to root->glob->relationOids.
 *
 * This is used in two different scenarios: a normal join clause, where all
 * the Vars in the clause *must* be replaced by OUTER_VAR or INNER_VAR
 * references; and a RETURNING clause, which may contain both Vars of the
 * target relation and Vars of other relations.  In the latter case we want
 * to replace the other-relation Vars by OUTER_VAR references, while leaving
 * target Vars alone.
 *
 * For a normal join, acceptable_rel should be zero so that any failure to
 * match a Var will be reported as an error.  For the RETURNING case, pass
 * inner_itlist = NULL and acceptable_rel = the ID of the target relation.
 *
 * 'clauses' is the targetlist or list of join clauses
 * 'outer_itlist' is the indexed target list of the outer join relation
 * 'inner_itlist' is the indexed target list of the inner join relation,
 *		or NULL
 * 'acceptable_rel' is either zero or the rangetable index of a relation
 *		whose Vars may appear in the clause without provoking an error
 * 'rtoffset': how much to increment varnoold by
 *
 * Returns the new expression tree.  The original clause structure is
 * not modified.
 */
static List* fix_join_expr(PlannerInfo* root, List* clauses, indexed_tlist* outer_itlist, indexed_tlist* inner_itlist,
    Index acceptable_rel, int rtoffset)
{
    fix_join_expr_context context;

    context.root = root;
    context.outer_itlist = outer_itlist;
    context.inner_itlist = inner_itlist;
    context.acceptable_rel = acceptable_rel;
    context.rtoffset = rtoffset;
    return (List*)fix_join_expr_mutator((Node*)clauses, &context);
}

static Node* fix_join_expr_mutator(Node* node, fix_join_expr_context* context)
{
    Var* newvar = NULL;

    if (node == NULL)
        return NULL;
    if (IsA(node, Var)) {
        Var* var = (Var*)node;

        /* First look for the var in the input tlists */
        if (context->outer_itlist != NULL) {
            newvar = search_indexed_tlist_for_var(var, context->outer_itlist, OUTER_VAR, context->rtoffset);
            if (newvar != NULL)
                return (Node*)newvar;
        }
        if (context->inner_itlist != NULL) {
            newvar = search_indexed_tlist_for_var(var, context->inner_itlist, INNER_VAR, context->rtoffset);
            if (newvar != NULL)
                return (Node*)newvar;
        }

        /* If it's for acceptable_rel, adjust and return it */
        if (var->varno == context->acceptable_rel) {
            var = copyVar(var);
            var->varno += context->rtoffset;
            if (var->varnoold > 0)
                var->varnoold += context->rtoffset;
            return (Node*)var;
        }

        /* No referent found for Var */
        ereport(ERROR,
            (errmodule(MOD_OPT),
                errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                (errmsg("variable not found in subplan target lists"))));
    }
    if (IsA(node, PlaceHolderVar)) {
        PlaceHolderVar* phv = (PlaceHolderVar*)node;

        /* See if the PlaceHolderVar has bubbled up from a lower plan node */
        if (context->outer_itlist != NULL && context->outer_itlist->has_ph_vars) {
            newvar = search_indexed_tlist_for_non_var((Node*)phv, context->outer_itlist, OUTER_VAR);
            if (newvar != NULL)
                return (Node*)newvar;
        }
        if (context->inner_itlist && context->inner_itlist->has_ph_vars) {
            newvar = search_indexed_tlist_for_non_var((Node*)phv, context->inner_itlist, INNER_VAR);
            if (newvar != NULL)
                return (Node*)newvar;
        }

        /* If not supplied by input plans, evaluate the contained expr */
        return fix_join_expr_mutator((Node*)phv->phexpr, context);
    }
    /* Try matching more complex expressions too, if tlists have any */
    if (context->outer_itlist && context->outer_itlist->has_non_vars) {
        newvar = search_indexed_tlist_for_non_var(node, context->outer_itlist, OUTER_VAR);
        if (newvar != NULL)
            return (Node*)newvar;
    }
    if (context->inner_itlist && context->inner_itlist->has_non_vars) {
        newvar = search_indexed_tlist_for_non_var(node, context->inner_itlist, INNER_VAR);
        if (newvar != NULL)
            return (Node*)newvar;
    }
    fix_expr_common(context->root, node);
    return expression_tree_mutator(node, (Node* (*)(Node*, void*)) fix_join_expr_mutator, (void*)context);
}

/*
 * fix_upper_expr
 *		Modifies an expression tree so that all Var nodes reference outputs
 *		of a subplan.  Also performs opcode lookup, and adds regclass OIDs to
 *		root->glob->relationOids.
 *
 * This is used to fix up target and qual expressions of non-join upper-level
 * plan nodes, as well as index-only scan nodes.
 *
 * An error is raised if no matching var can be found in the subplan tlist
 * --- so this routine should only be applied to nodes whose subplans'
 * targetlists were generated via flatten_tlist() or some such method.
 *
 * If itlist->has_non_vars is true, then we try to match whole subexpressions
 * against elements of the subplan tlist, so that we can avoid recomputing
 * expressions that were already computed by the subplan.  (This is relatively
 * expensive, so we don't want to try it in the common case where the
 * subplan tlist is just a flattened list of Vars.)
 *
 * 'node': the tree to be fixed (a target item or qual)
 * 'subplan_itlist': indexed target list for subplan (or index)
 * 'newvarno': varno to use for Vars referencing tlist elements
 * 'rtoffset': how much to increment varnoold by
 *
 * The resulting tree is a copy of the original in which all Var nodes have
 * varno = newvarno, varattno = resno of corresponding targetlist element.
 * The original tree is not modified.
 */
static Node* fix_upper_expr(PlannerInfo* root, Node* node, indexed_tlist* subplan_itlist, Index newvarno, int rtoffset)
{
    fix_upper_expr_context context;

    context.root = root;
    context.subplan_itlist = subplan_itlist;
    context.newvarno = newvarno;
    context.rtoffset = rtoffset;
    return fix_upper_expr_mutator(node, &context);
}

static Node* fix_upper_expr_mutator(Node* node, fix_upper_expr_context* context)
{
    Var* newvar = NULL;

    if (node == NULL)
        return NULL;
    if (IsA(node, Var)) {
        Var* var = (Var*)node;

        newvar = search_indexed_tlist_for_var(var, context->subplan_itlist, context->newvarno, context->rtoffset);
        if (newvar == NULL)
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                    (errmsg("variable not found in subplan target list"))));
        return (Node*)newvar;
    }
    if (IsA(node, PlaceHolderVar)) {
        PlaceHolderVar* phv = (PlaceHolderVar*)node;

        /* See if the PlaceHolderVar has bubbled up from a lower plan node */
        if (context->subplan_itlist->has_ph_vars) {
            newvar = search_indexed_tlist_for_non_var((Node*)phv, context->subplan_itlist, context->newvarno);
            if (newvar != NULL)
                return (Node*)newvar;
        }
        /* If not supplied by input plan, evaluate the contained expr */
        return fix_upper_expr_mutator((Node*)phv->phexpr, context);
    }
    /* Try matching more complex expressions too, if tlist has any */
    if (context->subplan_itlist->has_non_vars) {
        newvar = search_indexed_tlist_for_non_var(node, context->subplan_itlist, context->newvarno);
        if (newvar != NULL)
            return (Node*)newvar;
    }
    fix_expr_common(context->root, node);
    return expression_tree_mutator(node, (Node* (*)(Node*, void*)) fix_upper_expr_mutator, (void*)context);
}

/*
 * set_returning_clause_references
 *		Perform setrefs.c's work on a RETURNING targetlist
 *
 * If the query involves more than just the result table, we have to
 * adjust any Vars that refer to other tables to reference junk tlist
 * entries in the top subplan's targetlist.  Vars referencing the result
 * table should be left alone, however (the executor will evaluate them
 * using the actual heap tuple, after firing triggers if any).	In the
 * adjusted RETURNING list, result-table Vars will have their original
 * varno (plus rtoffset), but Vars for other rels will have varno OUTER_VAR.
 *
 * We also must perform opcode lookup and add regclass OIDs to
 * root->glob->relationOids.
 *
 * 'rlist': the RETURNING targetlist to be fixed
 * 'topplan': the top subplan node that will be just below the ModifyTable
 *		node (note it's not yet passed through set_plan_refs)
 * 'resultRelation': RT index of the associated result relation
 * 'rtoffset': how much to increment varnos by
 *
 * Note: the given 'root' is for the parent query level, not the 'topplan'.
 * This does not matter currently since we only access the dependency-item
 * lists in root->glob, but it would need some hacking if we wanted a root
 * that actually matches the subplan.
 *
 * Note: resultRelation is not yet adjusted by rtoffset.
 */
static List* set_returning_clause_references(
    PlannerInfo* root, List* rlist, Plan* topplan, Index resultRelation, int rtoffset)
{
    indexed_tlist* itlist = NULL;

    /*
     * We can perform the desired Var fixup by abusing the fix_join_expr
     * machinery that formerly handled inner indexscan fixup.  We search the
     * top plan's targetlist for Vars of non-result relations, and use
     * fix_join_expr to convert RETURNING Vars into references to those tlist
     * entries, while leaving result-rel Vars as-is.
     *
     * PlaceHolderVars will also be sought in the targetlist, but no
     * more-complex expressions will be.  Note that it is not possible for a
     * PlaceHolderVar to refer to the result relation, since the result is
     * never below an outer join.  If that case could happen, we'd have to be
     * prepared to pick apart the PlaceHolderVar and evaluate its contained
     * expression instead.
     */
    itlist = build_tlist_index_other_vars(topplan->targetlist, resultRelation);

    rlist = fix_join_expr(root, rlist, itlist, NULL, resultRelation, rtoffset);

    pfree_ext(itlist);

    return rlist;
}

/*****************************************************************************
 *					OPERATOR REGPROC LOOKUP
 *****************************************************************************/
/*
 * fix_opfuncids
 *	  Calculate opfuncid field from opno for each OpExpr node in given tree.
 *	  The given tree can be anything expression_tree_walker handles.
 *
 * The argument is modified in-place.  (This is OK since we'd want the
 * same change for any node, even if it gets visited more than once due to
 * shared structure.)
 */
void fix_opfuncids(Node* node)
{
    /* This tree walk requires no special setup, so away we go... */
    (void)fix_opfuncids_walker(node, NULL);
}

static bool fix_opfuncids_walker(Node* node, void* context)
{
    if (node == NULL)
        return false;
    if (IsA(node, OpExpr))
        set_opfuncid((OpExpr*)node);
    else if (IsA(node, DistinctExpr))
        set_opfuncid((OpExpr*)node); /* rely on struct equivalence */
    else if (IsA(node, NullIfExpr))
        set_opfuncid((OpExpr*)node); /* rely on struct equivalence */
    else if (IsA(node, ScalarArrayOpExpr))
        set_sa_opfuncid((ScalarArrayOpExpr*)node);
    return expression_tree_walker(node, (bool (*)())fix_opfuncids_walker, context);
}

/*
 * set_opfuncid
 *		Set the opfuncid (procedure OID) in an OpExpr node,
 *		if it hasn't been set already.
 *
 * Because of struct equivalence, this can also be used for
 * DistinctExpr and NullIfExpr nodes.
 */
void set_opfuncid(OpExpr* opexpr)
{
    if (opexpr->opfuncid == InvalidOid)
        opexpr->opfuncid = get_opcode(opexpr->opno);
}

/*
 * set_sa_opfuncid
 *		As above, for ScalarArrayOpExpr nodes.
 */
void set_sa_opfuncid(ScalarArrayOpExpr* opexpr)
{
    if (opexpr->opfuncid == InvalidOid)
        opexpr->opfuncid = get_opcode(opexpr->opno);
}

/*****************************************************************************
 *					QUERY DEPENDENCY MANAGEMENT
 *****************************************************************************/
/*
 * record_plan_function_dependency
 *		Mark the current plan as depending on a particular function.
 *
 * This is exported so that the function-inlining code can record a
 * dependency on a function that it's removed from the plan tree.
 */
void record_plan_function_dependency(PlannerInfo* root, Oid funcid)
{
    /*
     * For performance reasons, we don't bother to track built-in functions;
     * we just assume they'll never change (or at least not in ways that'd
     * invalidate plans using them).  For this purpose we can consider a
     * built-in function to be one with OID less than FirstBootstrapObjectId.
     * Note that the OID generator guarantees never to generate such an OID
     * after startup, even at OID wraparound.
     */
    if (funcid >= (Oid)FirstBootstrapObjectId) {
        PlanInvalItem* inval_item = makeNode(PlanInvalItem);

        /*
         * It would work to use any syscache on pg_proc, but the easiest is
         * PROCOID since we already have the function's OID at hand.  Note
         * that plancache.c knows we use PROCOID.
         */
        inval_item->cacheId = PROCOID;
        inval_item->hashValue = GetSysCacheHashValue1(PROCOID, ObjectIdGetDatum(funcid));

        root->glob->invalItems = lappend(root->glob->invalItems, inval_item);
    }
}

/*
 * extract_query_dependencies
 *		Given a not-yet-planned query or queries (i.e. a Query node or list
 *		of Query nodes), extract dependencies just as set_plan_references
 *		would do.
 *
 * This is needed by plancache.c to handle invalidation of cached unplanned
 * queries.
 */
void extract_query_dependencies(
    Node* query, List** relationOids, List** invalItems, bool* hasRowSecurity, bool* hasHdfs)
{
    PlannerGlobal glob;
    PlannerInfo root;

    /* Make up dummy planner state so we can use this module's machinery */
    errno_t errorno;
    errorno = memset_s(&glob, sizeof(PlannerGlobal), 0, sizeof(glob));
    securec_check(errorno, "\0", "\0");
    glob.type = T_PlannerGlobal;
    glob.relationOids = NIL;
    glob.invalItems = NIL;
    /* Hack: we use glob.dependsOnRole to collect hasRowSecurity flags */
    glob.dependsOnRole = false;

    errorno = memset_s(&root, sizeof(PlannerInfo), 0, sizeof(root));
    securec_check(errorno, "\0", "\0");
    root.type = T_PlannerInfo;
    root.glob = &glob;

    (void)extract_query_dependencies_walker(query, &root);

    *relationOids = glob.relationOids;
    *invalItems = glob.invalItems;
    *hasRowSecurity = glob.dependsOnRole;
    /* Here reuse vectorized to indicate if has hdfs table */
    *hasHdfs = glob.vectorized;
}

/*
 * Tree walker for extract_query_dependencies.
 *
 * This is exported so that expression_planner_with_deps can call it on
 * simple expressions (post-planning, not before planning, in that case).
 * In that usage, glob.dependsOnRole isn't meaningful, but the relationOids
 * and invalItems lists are added to as needed.
 */
static bool extract_query_dependencies_walker(Node* node, PlannerInfo* context)
{
    if (node == NULL)
        return false;
    AssertEreport(!IsA(node, PlaceHolderVar), MOD_OPT, "place holder var is required.");
    /* Extract function dependencies and check for regclass Consts */
    fix_expr_common(context, node);
    if (IsA(node, Query)) {
        Query* query = (Query*)node;
        ListCell* lc = NULL;

        if (query->commandType == CMD_UTILITY) {
            /*
             * Ignore utility statements, except those (such as EXPLAIN) that
             * contain a parsed-but-not-planned query.
             */
            query = UtilityContainsQuery(query->utilityStmt);
            if (query == NULL)
                return false;
        }

        /* Remember if any Query has RLS quals applied by rewriter */
        if (query->hasRowSecurity)
            context->glob->dependsOnRole = true;

        /* Collect relation OIDs in this Query's rtable */
        foreach (lc, query->rtable) {
            RangeTblEntry* rte = (RangeTblEntry*)lfirst(lc);

            if (rte->rtekind == RTE_RELATION) {
                context->glob->relationOids = lappend_oid(context->glob->relationOids, rte->relid);

                /* use glob.vectorized here to mark if we contain hdfs table */
                if (REL_PAX_ORIENTED == rte->orientation)
                    context->glob->vectorized = true;

                /* To partition table, need append all partition Oids to relationOids.
                 * When partition tables are altered, partitionId will be append to transInvalInfo in
                 * function RegisterPartcacheInvalidation.
                 * On transaction start, if this table has been altered. Plansource->is_valid will be set
                 * to false.
                 */
                if (rte->ispartrel) {
                    List* partitionOid = getPartitionObjectIdList(rte->relid, PART_OBJ_TYPE_TABLE_PARTITION);
                    if (partitionOid != NULL) {
                        context->glob->relationOids = list_concat(context->glob->relationOids, partitionOid);
                    }
                }
            }
        }

        /* And recurse into the query's subexpressions */
        return query_tree_walker(query, (bool (*)())extract_query_dependencies_walker, (void*)context, 0);
    }
    return expression_tree_walker(node, (bool (*)())extract_query_dependencies_walker, (void*)context);
}
/*
 * fix_remote_expr
 *	   Create a new set of targetlist entries or qual clauses by
 *	   changing the varno/varattno values of variables in the clauses
 *	   to reference target list values from the base
 *	   relation target lists.  Also perform opcode lookup and add
 *	   regclass OIDs to glob->relationOids.
 *
 * 'clauses' is the targetlist or list of clauses
 * 'base_itlist' is the indexed target list of the base referenced relations
 *
 * 'return_non_base_vars' lets the caller decide whether to reject
 * or return vars not found in base_itlist
 *
 * Returns the new expression tree.  The original clause structure is
 * not modified.
 */
static List* fix_remote_expr(PlannerInfo* root, List* clauses, indexed_tlist* base_itlist, Index newrelid, int rtoffset,
    bool return_non_base_vars)
{
    fix_remote_expr_context context;

    context.glob = root->glob;
    context.base_itlist = base_itlist;
    context.relid = newrelid;
    context.rtoffset = rtoffset;
    context.return_non_base_vars = return_non_base_vars;

    return (List*)fix_remote_expr_mutator((Node*)clauses, &context);
}

static Node* fix_remote_expr_mutator(Node* node, fix_remote_expr_context* context)
{
    Var* newvar = NULL;

    if (node == NULL)
        return NULL;

    if (IsA(node, Var)) {
        Var* var = (Var*)node;

        /* First look for the var in the input base tlists */
        newvar = search_indexed_tlist_for_var(var, context->base_itlist, context->relid, context->rtoffset);
        if (newvar != NULL)
            return (Node*)newvar;

        /* If it's not found in base_itlist, return it if required */
        if (context->return_non_base_vars && var->varno != context->relid)
            return (Node*)var;

        /* No reference found for Var */
        ereport(ERROR,
            (errmodule(MOD_OPT),
                errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                (errmsg("variable not found in base remote scan target lists"))));
    }
    /* Try matching more complex expressions too, if tlists have any */
    if (context->base_itlist->has_non_vars) {
        newvar = search_indexed_tlist_for_non_var(node, context->base_itlist, context->relid);
        if (newvar != NULL)
            return (Node*)newvar;
    }

    return expression_tree_mutator(node, (Node* (*)(Node*, void*)) fix_remote_expr_mutator, context);
}

/*
 * set_remote_references
 *
 *	  Modify the target list and quals of a remote scan node to reference its
 *	  base rels, by setting the varnos to DUMMY (even OUTER is fine) setting attno
 *	  values to the result domain number of the base rels.
 *	  Also perform opcode lookup for these expressions. and add regclass
 *	  OIDs to glob->relationOids.
 */
static void set_remote_references(PlannerInfo* root, RemoteQuery* rscan, int rtoffset)
{
    indexed_tlist* base_itlist = NULL;

    if (!rscan->base_tlist)
        return;

    base_itlist = build_tlist_index(rscan->base_tlist);

    /* All remotescan plans have tlist, and quals */
    rscan->scan.plan.targetlist =
        fix_remote_expr(root, rscan->scan.plan.targetlist, base_itlist, rscan->scan.scanrelid, rtoffset, false);

    rscan->scan.plan.qual =
        fix_remote_expr(root, rscan->scan.plan.qual, base_itlist, rscan->scan.scanrelid, rtoffset, false);

    pfree_ext(base_itlist);
}

/*
 * set_remote_returning_refs
 *
 * Fix references of remote returning list to point
 * to reference target list values from the base
 * relation target lists
 */
static List* set_remote_returning_refs(PlannerInfo* root, List* rlist, Plan* topplan, Index relid, int rtoffset)
{
    indexed_tlist* base_itlist = NULL;

    base_itlist = build_tlist_index(topplan->targetlist);

    rlist = fix_remote_expr(root, rlist, base_itlist, relid, rtoffset, true);

    pfree_ext(base_itlist);

    return rlist;
}

/*
 * fix_skew_quals
 *
 * Fix references of skew qual
 * to reference target list values from the base
 * relation target lists
 */
static void fix_skew_quals(PlannerInfo* root, Plan* plan, indexed_tlist* subplan_itlist, int rtoffset)
{
    Stream* stream = NULL;

    if (IsA(plan, Stream) || IsA(plan, VecStream))
        stream = (Stream*)plan;
    else
        return;

    if (stream->skew_list != NIL) {
        ListCell* lc = NULL;
        QualSkewInfo* qsinfo = NULL;
        foreach (lc, stream->skew_list) {
            qsinfo = (QualSkewInfo*)lfirst(lc);

            /*
             * When consumer nodes do not equal producer nodes,
             * then we need roundrobin rather than local.
             */
            if (equal(stream->consumer_nodes->nodeList, plan->exec_nodes->nodeList) == false) {
                if (qsinfo->skew_stream_type == PART_REDISTRIBUTE_PART_LOCAL)
                    qsinfo->skew_stream_type = PART_REDISTRIBUTE_PART_ROUNDROBIN;
            }

            qsinfo->skew_quals = fix_join_expr(root, qsinfo->skew_quals, subplan_itlist, NULL, (Index)0, rtoffset);
        }
    }
}

#ifdef PGXC
/*
 * For Agg plans, if the lower scan plan is a RemoteQuery node, adjust the
 * Aggref nodes to pull the transition results from the datanodes. We do while
 * setting planner references so that the upper nodes will find the nodes that
 * they expect in Agg plans.
 */
void pgxc_set_agg_references(PlannerInfo* root, Agg* aggplan)
{
    RemoteQuery* rqplan = (RemoteQuery*)aggplan->plan.lefttree;
    Sort* srtplan = NULL;
    List* aggs_n_vars = NIL;
    ListCell* lcell = NULL;
    List* nodes_to_modify = NIL;
    List* rq_nodes_to_modify = NIL;
    List* srt_nodes_to_modify = NIL;

    /* Lower plan tree can be Sort->RemoteQuery or RemoteQuery */
    if (IsA(rqplan, Sort)) {
        srtplan = (Sort*)rqplan;
        rqplan = (RemoteQuery*)srtplan->plan.lefttree;
    } else
        srtplan = NULL;

    if (!IsA(rqplan, RemoteQuery))
        return;

    Assert(IS_PGXC_COORDINATOR && !IsConnFromCoord());
    /*
     * If there are not transition results expected from lower plans, nothing to
     * be done here.
     */
    if (!aggplan->is_final)
        return;

    /* Gather all the aggregates from all the targetlists that need fixing */
    nodes_to_modify = list_copy(aggplan->plan.targetlist);
    nodes_to_modify = list_concat(nodes_to_modify, aggplan->plan.qual);
    aggs_n_vars = pull_var_clause((Node*)nodes_to_modify, PVC_INCLUDE_AGGREGATES, PVC_RECURSE_PLACEHOLDERS);
    rq_nodes_to_modify = NIL;
    srt_nodes_to_modify = NIL;
    /*
     * For every aggregate, find corresponding aggregate in the lower plan and
     * modify it correctly.
     */
    foreach (lcell, aggs_n_vars) {
        Aggref* aggref = (Aggref*)lfirst(lcell);
        TargetEntry* tle = NULL;
        Aggref* rq_aggref = NULL;
        Aggref* srt_aggref = NULL;
        Aggref* arg_aggref = NULL; /* Aggref to be set as Argument to the
                                    * aggref in the Agg plan */
        /* Only Aggref expressions need modifications */
        if (!IsA(aggref, Aggref) && !IsA(aggref, GroupingFunc) && !IsA(aggref, GroupingId)) {
            AssertEreport(IsA(aggref, Var), MOD_OPT, "var is required.");
            continue;
        }

        tle = tlist_member((Node*)aggref, rqplan->scan.plan.targetlist);
        if (tle == NULL)
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                    (errmsg("Could not find the Aggref node when setting agg plan refernece."))));
        rq_aggref = (Aggref*)tle->expr;
        AssertEreport(equal(rq_aggref, aggref), MOD_OPT, "aggref should be equal.");
        /*
         * Remember the Aggref nodes of which we need to modify. This is done so
         * that, if there multiple copies of same aggregate, we will match all
         * of them
         */
        rq_nodes_to_modify = list_append_unique(rq_nodes_to_modify, rq_aggref);
        arg_aggref = rq_aggref;

        /*
         * If there is a Sort plan, get corresponding expression from there as
         * well and remember it to be modified.
         */
        if (srtplan != NULL) {
            tle = tlist_member((Node*)rq_aggref, srtplan->plan.targetlist);
            if (tle == NULL)
                ereport(ERROR,
                    (errmodule(MOD_OPT),
                        errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                        (errmsg("Could not find the Aggref node when setting agg plan reference."))));
            srt_aggref = (Aggref*)tle->expr;
            AssertEreport(equal(srt_aggref, rq_aggref), MOD_OPT, "aggref should be equal.");
            srt_nodes_to_modify = list_append_unique(srt_nodes_to_modify, srt_aggref);
            arg_aggref = srt_aggref;
        }

        /*
         * The transition result from the datanodes acts as an input to the
         * Aggref node on coordinator.
         */
        aggref->args = list_make1(makeTargetEntry((Expr*)arg_aggref, 1, NULL, false));
    }

    /* Modify the transition types now */
    foreach (lcell, rq_nodes_to_modify) {
        Aggref* rq_aggref = (Aggref*)lfirst(lcell);
        AssertEreport(IsA(rq_aggref, Aggref), MOD_OPT, "aggref is required");
        rq_aggref->aggtype = rq_aggref->aggtrantype;
    }
    foreach (lcell, srt_nodes_to_modify) {
        Aggref* srt_aggref = (Aggref*)lfirst(lcell);
        AssertEreport(IsA(srt_aggref, Aggref), MOD_OPT, "aggref is required.");
        srt_aggref->aggtype = srt_aggref->aggtrantype;
    }

    /*
     * We have modified the targetlist of the RemoteQuery plan below the Agg
     * plan. Adjust its targetlist as well.
     */
    pgxc_rqplan_adjust_tlist(root, rqplan, rqplan->is_simple ? false : true);

    return;
}
#endif /* PGXC */
