/* -------------------------------------------------------------------------
 *
 * relnode.cpp
 *	  Relation-node lookup/construction routines
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/optimizer/util/relnode.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"
#include <limits.h>
#include "catalog/pg_proc.h"
#include "nodes/nodeFuncs.h"
#include "nodes/print.h"
#include "parser/parse_hint.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/placeholder.h"
#include "optimizer/plancat.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/var.h"
#include "utils/guc.h"
#include "utils/hsearch.h"
#include "optimizer/streamplan.h"
#include "access/transam.h"
#include "utils/selfuncs.h"
#include "utils/lsyscache.h"

#ifdef PGXC
#include "pgxc/pgxc.h"
#endif

typedef struct JoinHashEntry {
    Relids join_relids; /* hash key --- MUST BE FIRST */
    RelOptInfo* join_rel;
} JoinHashEntry;

static void build_joinrel_tlist(PlannerInfo* root, RelOptInfo* joinrel, const RelOptInfo* input_rel);
static List* build_joinrel_restrictlist(
    PlannerInfo* root, RelOptInfo* joinrel, RelOptInfo* outer_rel, RelOptInfo* inner_rel);
static void build_joinrel_joinlist(RelOptInfo* joinrel, RelOptInfo* outer_rel, RelOptInfo* inner_rel);
static List* subbuild_joinrel_restrictlist(const RelOptInfo* joinrel, const List* joininfo_list, List* new_restrictlist);
static List* subbuild_joinrel_joinlist(const RelOptInfo* joinrel, const List* joininfo_list, List* new_joininfo);
static void build_joinrel_itst_diskeys(
    PlannerInfo* root, RelOptInfo* joinrel, RelOptInfo* outerrel, RelOptInfo* innerrel, JoinType jointype);
static void add_eqjoin_diskey_for_ec(
    const RelOptInfo* rel, const RelOptInfo* siderel, const EquivalenceClass* ec, List** join_diskey_list, List** relid_list);

/*
 *  * Description: For the function of build/drop/delete UDF, we need not to cache in hash
 *   *                      for compiling of plpgsql, because the function will get current schema internal.
 *    *
 *     * Parameters:
 *      *      @in funcid: function oid.
 *       *      @in func_name: function name.
 *        *
 *         * Returns: bool
 *          */
bool is_func_need_cache(Oid funcid, const char* func_name)
{
    if (func_name == NULL) {
        return true;
    }

    /* All function belone to pg_catalog for simsearch. */
    Oid nspoid = get_func_namespace(funcid);
    if (PG_CATALOG_NAMESPACE != nspoid) {
        return true;
    }

    if (0 != strcmp(func_name, "build_vector_config_env") && 0 != strcmp(func_name, "drop_vector_config_env") &&
       0 != strcmp(func_name, "delete_vector_gpu_by_date")) {
        return true;
    }

    return false;
}

/*
 *  * Description: Set the function of feature searching as distribute by hash.
 *   *
 *    * Parameters:
 *     *      @in funcid: function oid.
 *      *      @in func_name: function name.
 *       *
 *        * Returns: bool
 *         */
bool is_func_need_hash(Oid funcid)
{
    const char* func_name = get_func_name(funcid);
    if (func_name == NULL) {
        return false;
    }

    /* All function belone to pg_catalog for simsearch. */
    Oid nspoid = get_func_namespace(funcid);
    if (PG_CATALOG_NAMESPACE != nspoid) {
        return false;
    }

    if (0 == strcmp(func_name, "short_feature_search")) {
        return true;
    }

    return false;
}

/*
 * setup_simple_rel_arrays
 *	  Prepare the arrays we use for quickly accessing base relations.
 */
void setup_simple_rel_arrays(PlannerInfo* root)
{
    Index rti;
    ListCell* lc = NULL;

    /* Arrays are accessed using RT indexes (1..N) */
    root->simple_rel_array_size = list_length(root->parse->rtable) + 1;

    /* simple_rel_array is initialized to all NULLs */
    root->simple_rel_array = (RelOptInfo**)palloc0(root->simple_rel_array_size * sizeof(RelOptInfo*));

    /* simple_rte_array is an array equivalent of the rtable list */
    root->simple_rte_array = (RangeTblEntry**)palloc0(root->simple_rel_array_size * sizeof(RangeTblEntry*));
    rti = 1;
    foreach (lc, root->parse->rtable) {
        RangeTblEntry* rte = (RangeTblEntry*)lfirst(lc);

        root->simple_rte_array[rti++] = rte;
    }
}

/*
 * build_simple_rel
 *	  Construct a new RelOptInfo for a base relation or 'other' relation.
 */
RelOptInfo* build_simple_rel(PlannerInfo* root, int relid, RelOptKind reloptkind)
{
    RelOptInfo* rel = NULL;
    RangeTblEntry* rte = NULL;

    /* Rel should not exist already */
    AssertEreport(relid > 0, MOD_OPT, "Expected positive relid, run into exception.");
    AssertEreport(relid < root->simple_rel_array_size,
        MOD_OPT,
        "Expected relid to be < relation array size, run into exception.");
    if (root->simple_rel_array[relid] != NULL)
        ereport(ERROR,
            (errmodule(MOD_OPT),
                errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                errmsg("rel %d already exists", relid)));

    /* Fetch RTE for relation */
    rte = root->simple_rte_array[relid];
    AssertEreport(rte != NULL, MOD_OPT, "Unexpected NULL pointer for rte.");

    rel = makeNode(RelOptInfo);
    rel->is_ustore = rte->is_ustore;
    rel->reloptkind = reloptkind;
    rel->relids = bms_make_singleton(relid);
    rel->isPartitionedTable = rte->ispartrel;
    rel->partflag = PARTITION_NONE;
    rel->rows = 0;
    rel->width = 0;
    rel->encodedwidth = 0;
    rel->encodednum = 0;
    rel->reltargetlist = NIL;
    rel->alternatives = NIL;
    rel->base_rel = NULL;
    rel->pathlist = NIL;
    rel->ppilist = NIL;
    rel->cheapest_gather_path = NULL;
    rel->cheapest_startup_path = NULL;
    rel->cheapest_total_path = NIL;
    rel->cheapest_unique_path = NULL;
    rel->cheapest_parameterized_paths = NIL;
    rel->relid = relid;
    rel->rtekind = rte->rtekind;
    /* min_attr, max_attr, attr_needed, attr_widths are set below */
    rel->lateral_vars = NIL;
    rel->lateral_relids = NULL;
    rel->indexlist = NIL;
    rel->pages = 0;
    rel->tuples = 0;
    rel->multiple = 0;
    rel->allvisfrac = 0;
    rel->pruning_result = NULL;
    rel->pruning_result_for_index_usable = NULL;
    rel->pruning_result_for_index_unusable = NULL;
    rel->partItrs = -1;
    rel->partItrs_for_index_usable = -1;
    rel->partItrs_for_index_unusable = -1;
    rel->subplan = NULL;
    rel->subroot = NULL;
    rel->subplan_params = NIL;
    rel->fdwroutine = NULL;
    rel->fdw_private = NULL;
    rel->baserestrictinfo = NIL;
    rel->baserestrictcost.startup = 0;
    rel->baserestrictcost.per_tuple = 0;
    rel->baserestrict_min_security = UINT_MAX;
    rel->joininfo = NIL;
    rel->has_eclass_joins = false;
    rel->varratio = NIL;
#ifdef STREAMPLAN
    if (rel->rtekind == RTE_RELATION) {
        rel->locator_type = GetLocatorType(rte->relid);
        rel->distribute_keys = build_baserel_distributekey(rte, relid);
        rel->rangelistOid = (IsLocatorDistributedBySlice(rel->locator_type)) ? rte->relid : InvalidOid;
    } else if (rel->rtekind == RTE_CTE) {
        Index levelsup = rte->ctelevelsup;
        PlannerInfo* cteroot = root;
        while (levelsup-- > 0) {
            cteroot = cteroot->parent_root;
            if (cteroot == NULL) /* shouldn't happen */
                ereport(ERROR,
                    (errmodule(MOD_OPT),
                        errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                        errmsg("bad levelsup for CTE \"%s\"", rte->ctename)));
        }

        /*
         * Set RelOptInfo's locator_type according to CTE, while CTE's locator_type is set
         * in SS_process_ctes before.
         * Here we use cteList to replace rtable, so we do not need to walk through the parse tree
         * to set rtable's locator_type.
         */
        ListCell* lc = NULL;
        foreach (lc, cteroot->parse->cteList) {
            CommonTableExpr* cte = (CommonTableExpr*)lfirst(lc);
            if (strcmp(cte->ctename, rte->ctename) == 0) {
                rel->locator_type = cte->locator_type;
                break;
            }
        }
    } else if (rel->rtekind == RTE_FUNCTION || rel->rtekind == RTE_VALUES) {
        rel->distribute_keys = NIL;
        rel->locator_type = LOCATOR_TYPE_REPLICATED;

        /*
         * For the function of vector search with the scene of gpu acceleration,
         * the locator_type should be set as distribute by hash.
         */
        if (rte->rtekind == RTE_FUNCTION && is_func_need_hash(((FuncExpr*)rte->funcexpr)->funcid)) {
            rel->locator_type = LOCATOR_TYPE_HASH;
        }
        if (IS_EC_FUNC(rte)) {
            rel->locator_type = LOCATOR_TYPE_HASH;
        }
    }
    if (!rel->locator_type)
        rel->locator_type = LOCATOR_TYPE_NONE;
#endif

    /* Check type of rtable entry */
    switch (rte->rtekind) {
        case RTE_RELATION:
            if (rte->ispartrel) {
                rel->partflag = PARTITION_ANCESOR;
            }
            /* Table --- retrieve statistics from the system catalogs */
            get_relation_info(root, rte->relid, rte->inh, rel);
            break;
        case RTE_SUBQUERY:
        case RTE_FUNCTION:
        case RTE_VALUES:
        case RTE_CTE:

            /*
             * Subquery, function, or values list --- set up attr range and
             * arrays
             *
             * Note: 0 is included in range to support whole-row Vars
             */
            rel->min_attr = 0;
            rel->max_attr = list_length(rte->eref->colnames);
            rel->attr_needed = (Relids*)palloc0((rel->max_attr - rel->min_attr + 1) * sizeof(Relids));
            rel->attr_widths = (int32*)palloc0((rel->max_attr - rel->min_attr + 1) * sizeof(int32));
            rel->orientation = rte->orientation;
            break;
        case RTE_RESULT:
            /* RTE_RESULT has no columns, nor could it have whole-row Var */
            rel->min_attr = 0;
            rel->max_attr = -1;
            rel->attr_needed = NULL;
            rel->attr_widths = NULL;
            break;
        default:
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmsg("unrecognized RTE kind: %d", (int)rte->rtekind)));
            break;
    }

    /* Save the finished struct in the query's simple_rel_array */
    root->simple_rel_array[relid] = rel;
    if (rel->rtekind == RTE_RELATION)
        set_local_rel_size(root, rel);

    /*
     * This is a convenient spot at which to note whether rels participating
     * in the query have any securityQuals attached.  If so, increase
     * root->qualSecurityLevel to ensure it's larger than the maximum
     * security level needed for securityQuals.
     */
    if (rte->securityQuals)
        root->qualSecurityLevel = Max((int)(root->qualSecurityLevel), list_length(rte->securityQuals));

    /*
     * If this rel is an appendrel parent, recurse to build "other rel"
     * RelOptInfos for its children.  They are "other rels" because they are
     * not in the main join tree, but we will need RelOptInfos to plan access
     * to them.
     */
    if (rte->inh) {
        ListCell* l = NULL;

        foreach (l, root->append_rel_list) {
            AppendRelInfo* appinfo = (AppendRelInfo*)lfirst(l);

            /* append_rel_list contains all append rels; ignore others */
            if (appinfo->parent_relid != (unsigned int)relid)
                continue;

            (void)build_simple_rel(root, appinfo->child_relid, RELOPT_OTHER_MEMBER_REL);
        }
    }

    return rel;
}

/*
 * find_base_rel
 *	  Find a base or other relation entry, which must already exist.
 */
RelOptInfo* find_base_rel(PlannerInfo* root, int relid)
{
    RelOptInfo* rel = NULL;

    AssertEreport(relid > 0, MOD_OPT, "Expected positive relid, run into exception.");

    if (relid < root->simple_rel_array_size) {
        rel = root->simple_rel_array[relid];
        if (rel != NULL)
            return rel;
    }

    ereport(ERROR,
        (errmodule(MOD_OPT),
            errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
            errmsg("no relation entry for relid %d", relid)));

    return NULL; /* keep compiler quiet */
}

/*
 * build_join_rel_hash
 *	  Construct the auxiliary hash table for join relations.
 */
static void build_join_rel_hash(PlannerInfo* root)
{
    HTAB* hashtab = NULL;
    HASHCTL hash_ctl;
    ListCell* l = NULL;
    errno_t rc;

    /* Create the hash table */
    rc = memset_s(&hash_ctl, sizeof(hash_ctl), '\0', sizeof(hash_ctl));
    securec_check(rc, "", "");

    hash_ctl.keysize = sizeof(Relids);
    hash_ctl.entrysize = sizeof(JoinHashEntry);
    hash_ctl.hash = bitmap_hash;
    hash_ctl.match = bitmap_match;
    hash_ctl.hcxt = CurrentMemoryContext;
    hashtab = hash_create("JoinRelHashTable", 256L, &hash_ctl, HASH_ELEM | HASH_FUNCTION | HASH_COMPARE | HASH_CONTEXT);

    /* Insert all the already-existing joinrels */
    foreach (l, root->join_rel_list) {
        RelOptInfo* rel = (RelOptInfo*)lfirst(l);
        JoinHashEntry* hentry = NULL;
        bool found = false;

        hentry = (JoinHashEntry*)hash_search(hashtab, &(rel->relids), HASH_ENTER, &found);
        Assert(!found);
        hentry->join_rel = rel;
    }

    root->join_rel_hash = hashtab;
}

/*
 * find_join_rel
 *	  Returns relation entry corresponding to 'relids' (a set of RT indexes),
 *	  or NULL if none exists.  This is for join relations.
 */
RelOptInfo* find_join_rel(PlannerInfo* root, Relids relids)
{
    /*
     * Switch to using hash lookup when list grows "too long".	The threshold
     * is arbitrary and is known only here.
     */
    if (!root->join_rel_hash && list_length(root->join_rel_list) > 32)
        build_join_rel_hash(root);

    /*
     * Use either hashtable lookup or linear search, as appropriate.
     *
     * Note: the seemingly redundant hashkey variable is used to avoid taking
     * the address of relids; unless the compiler is exceedingly smart, doing
     * so would force relids out of a register and thus probably slow down the
     * list-search case.
     */
    if (root->join_rel_hash) {
        Relids hashkey = relids;
        JoinHashEntry* hentry = NULL;

        hentry = (JoinHashEntry*)hash_search(root->join_rel_hash, &hashkey, HASH_FIND, NULL);
        if (hentry != NULL)
            return hentry->join_rel;
    } else {
        ListCell* l = NULL;

        foreach (l, root->join_rel_list) {
            RelOptInfo* rel = (RelOptInfo*)lfirst(l);

            if (bms_equal(rel->relids, relids))
                return rel;
        }
    }

    return NULL;
}

void remove_join_rel(PlannerInfo *root, RelOptInfo *rel)
{
    root->join_rel_level[root->join_cur_level] =
        list_delete_ptr(root->join_rel_level[root->join_cur_level], rel);
    if (!root->join_rel_hash) {
        root->join_rel_list = list_delete_ptr(root->join_rel_list, rel);
        return;
    }

    Relids hashkey = rel->relids;
    hash_search(root->join_rel_hash, &hashkey, HASH_REMOVE, NULL);
    return;
}

/*
 * @Description: Search rows hint of this joinrel and set it's rows according to this hint.
 * @in root: Global information for planning/optimization.
 * @in joinrel: Join rel infomation.
 */
void adjust_rows_according_to_hint(HintState* hstate, RelOptInfo* rel, Relids subrelids)
{
    if (hstate == NULL) {
        return;
    }

    Relids joinrelids = rel->relids;

    List* row_hints = NIL;
    ListCell* lc = NULL;

    /* Search for applicable rows hint for this join node */
    foreach (lc, hstate->row_hint) {
        RowsHint* hint = (RowsHint*)lfirst(lc);
        bool available = false;
        bool allmatched = false;

        if (subrelids == NULL || hint->value_type != RVT_MULTI || bms_num_members(hint->joinrelids) != 2) {
            /* exact match for single relation */
            if (bms_equal(joinrelids, hint->joinrelids)) {
                available = true;
                allmatched = true;
            }
        } else {
            /* joinrel of hint exists in both outer and inner is ok */
            if (bms_is_subset(hint->joinrelids, joinrelids) && bms_overlap(hint->joinrelids, subrelids) &&
                !bms_is_subset(hint->joinrelids, subrelids))
                available = true;
        }
        if (available) {
            /* for all matched, we expect to use only one hint */
            if (allmatched) {
                list_free_ext(row_hints);
                row_hints = NIL;
            }
            row_hints = lappend(row_hints, hint);
            hint->base.state = HINT_STATE_USED;
            if (allmatched)
                break;
        }
    }

    foreach (lc, row_hints) {
        RowsHint* row_hint = (RowsHint*)lfirst(lc);

        switch (row_hint->value_type) {
            case RVT_ABSOLUTE:
                rel->rows = row_hint->rows;
                break;
            case RVT_ADD:
                rel->rows += row_hint->rows;
                break;
            case RVT_SUB:
                rel->rows -= row_hint->rows;
                break;
            case RVT_MULTI:
                rel->rows *= row_hint->rows;
                break;
            default:
                elog(WARNING, "unrecognized row hint type: %d", (int)row_hint->value_type);
                break;
        }

        rel->rows = clamp_row_est(rel->rows);
    }
}

/*
 * build_join_rel
 *	  Returns relation entry corresponding to the union of two given rels,
 *	  creating a new relation entry if none already exists.
 *
 * 'joinrelids' is the Relids set that uniquely identifies the join
 * 'outer_rel' and 'inner_rel' are relation nodes for the relations to be
 *		joined
 * 'sjinfo': join context info
 * 'restrictlist_ptr': result variable.  If not NULL, *restrictlist_ptr
 *		receives the list of RestrictInfo nodes that apply to this
 *		particular pair of joinable relations.
 *
 * restrictlist_ptr makes the routine's API a little grotty, but it saves
 * duplicated calculation of the restrictlist...
 */
RelOptInfo* build_join_rel(PlannerInfo* root, Relids joinrelids, RelOptInfo* outer_rel, RelOptInfo* inner_rel,
    SpecialJoinInfo* sjinfo, List** restrictlist_ptr)
{
    RelOptInfo* joinrel = NULL;
    List* restrictlist = NIL;

    /*
     * See if we already have a joinrel for this set of base rels.
     */
    joinrel = find_join_rel(root, joinrelids);
    if (joinrel != NULL) {
        /*
         * Yes, so we only need to figure the restrictlist for this particular
         * pair of component relations.
         */
        if (restrictlist_ptr != NULL)
            *restrictlist_ptr = build_joinrel_restrictlist(root, joinrel, outer_rel, inner_rel);
        /* set interesting distribute keys */
        build_joinrel_itst_diskeys(root, joinrel, outer_rel, inner_rel, sjinfo->jointype);

        return joinrel;
    }

    /*
     * Nope, so make one.
     */
    joinrel = makeNode(RelOptInfo);
    joinrel->reloptkind = RELOPT_JOINREL;
    joinrel->relids = bms_copy(joinrelids);
    joinrel->isPartitionedTable = false;
    joinrel->partflag = PARTITION_NONE;
    joinrel->rows = 0;
    joinrel->width = 0;
    joinrel->encodedwidth = 0;
    joinrel->encodednum = 0;
    joinrel->reltargetlist = NIL;
    joinrel->pathlist = NIL;
    joinrel->ppilist = NIL;
    joinrel->cheapest_gather_path = NULL;
    joinrel->cheapest_startup_path = NULL;
    joinrel->cheapest_total_path = NIL;
    joinrel->cheapest_unique_path = NULL;
    joinrel->cheapest_parameterized_paths = NIL;
    joinrel->relid = 0; /* indicates not a baserel */
    joinrel->rtekind = RTE_JOIN;
    joinrel->min_attr = 0;
    joinrel->max_attr = 0;
    joinrel->attr_needed = NULL;
    joinrel->attr_widths = NULL;
    joinrel->lateral_vars = NIL;
    joinrel->lateral_relids = NULL;
    joinrel->indexlist = NIL;
    joinrel->pages = 0.0;
    joinrel->tuples = 0;
    joinrel->multiple = 1;
    joinrel->allvisfrac = 0;
    joinrel->pruning_result = NULL;
    joinrel->pruning_result_for_index_usable = NULL;
    joinrel->pruning_result_for_index_unusable = NULL;
    joinrel->partItrs = -1;
    joinrel->partItrs_for_index_usable = -1;
    joinrel->partItrs_for_index_unusable = -1;
    joinrel->subplan = NULL;
    joinrel->subroot = NULL;
    joinrel->subplan_params = NIL;
    joinrel->fdwroutine = NULL;
    joinrel->fdw_private = NULL;
    joinrel->baserestrictinfo = NIL;
    joinrel->baserestrictcost.startup = 0;
    joinrel->baserestrictcost.per_tuple = 0;
    joinrel->baserestrict_min_security = UINT_MAX;
    joinrel->joininfo = NIL;
    joinrel->has_eclass_joins = false;
    joinrel->varratio = NIL;
    if (IsLocatorReplicated(inner_rel->locator_type) && IsLocatorReplicated(outer_rel->locator_type))
        joinrel->locator_type = LOCATOR_TYPE_REPLICATED;
    else
        joinrel->locator_type = LOCATOR_TYPE_NONE;

    /*
     * Create a new tlist containing just the vars that need to be output from
     * this join (ie, are needed for higher joinclauses or final output).
     *
     * NOTE: the tlist order for a join rel will depend on which pair of outer
     * and inner rels we first try to build it from.  But the contents should
     * be the same regardless.
     */
    build_joinrel_tlist(root, joinrel, outer_rel);
    build_joinrel_tlist(root, joinrel, inner_rel);
    add_placeholders_to_joinrel(root, joinrel);

    /*
     * Construct restrict and join clause lists for the new joinrel. (The
     * caller might or might not need the restrictlist, but I need it anyway
     * for set_joinrel_size_estimates().)
     */
    restrictlist = build_joinrel_restrictlist(root, joinrel, outer_rel, inner_rel);
    if (restrictlist_ptr != NULL)
        *restrictlist_ptr = restrictlist;
    build_joinrel_joinlist(joinrel, outer_rel, inner_rel);

    /*
     * This is also the right place to check whether the joinrel has any
     * pending EquivalenceClass joins.
     */
    joinrel->has_eclass_joins = has_relevant_eclass_joinclause(root, joinrel);

    /*
     * Set estimates of the joinrel's size.
     */
    set_joinrel_size_estimates(root, joinrel, outer_rel, inner_rel, sjinfo, restrictlist);

    /*
     * Add the joinrel to the query's joinrel list, and store it into the
     * auxiliary hashtable if there is one.  NB: GEQO requires us to append
     * the new joinrel to the end of the list!
     */
    root->join_rel_list = lappend(root->join_rel_list, joinrel);

    if (root->join_rel_hash) {
        JoinHashEntry* hentry = NULL;
        bool found = false;

        hentry = (JoinHashEntry*)hash_search(root->join_rel_hash, &(joinrel->relids), HASH_ENTER, &found);
        Assert(!found);
        hentry->join_rel = joinrel;
    }

    /*
     * Also, if dynamic-programming join search is active, add the new joinrel
     * to the appropriate sublist.	Note: you might think the Assert on number
     * of members should be for equality, but some of the level 1 rels might
     * have been joinrels already, so we can only assert <=.
     */
    if (root->join_rel_level) {
        AssertEreport(root->join_cur_level > 0, MOD_OPT, "Expected positive index of list, run into exception.");
        AssertEreport(root->join_cur_level <= bms_num_members(joinrel->relids),
            MOD_OPT,
            "Expected index of list to be less than number of relids, run into exception.");
        root->join_rel_level[root->join_cur_level] = lappend(root->join_rel_level[root->join_cur_level], joinrel);
    }

    /* set interesting distribute keys */
    build_joinrel_itst_diskeys(root, joinrel, outer_rel, inner_rel, sjinfo->jointype);

    /* Adjust joinrel globle_rows according hint. */
    adjust_rows_according_to_hint(root->parse->hintState, joinrel, outer_rel->relids);

    return joinrel;
}

/*
 * build_joinrel_tlist
 *	  Builds a join relation's target list from an input relation.
 *	  (This is invoked twice to handle the two input relations.)
 *
 * The join's targetlist includes all Vars of its member relations that
 * will still be needed above the join.  This subroutine adds all such
 * Vars from the specified input rel's tlist to the join rel's tlist.
 *
 * We also compute the expected width of the join's output, making use
 * of data that was cached at the baserel level by set_rel_width().
 */
static void build_joinrel_tlist(PlannerInfo* root, RelOptInfo* joinrel, const RelOptInfo* input_rel)
{
    Relids relids = joinrel->relids;
    ListCell* vars = NULL;

    foreach (vars, input_rel->reltargetlist) {
        Var *var = (Var *) lfirst(vars);
        RelOptInfo* baserel = NULL;
        int ndx;

        /*
         * Ignore PlaceHolderVars in the input tlists; we'll make our own
         * decisions about whether to copy them.
         */
        if (IsA(var, PlaceHolderVar))
            continue;

        /*
         * We can't run into any child RowExprs here, but we could find a
         * whole-row Var with a ConvertRowtypeExpr atop it.
         */
        if (!IsA(var, Var))
            elog(ERROR, "unexpected node type in reltargetlist: %d",
                 (int) nodeTag(var));


        /* Get the Var's original base rel */
        baserel = find_base_rel(root, var->varno);
        if (baserel == NULL)
            ereport(
                ERROR, (errmodule(MOD_OPT), errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED), errmsg("Fail to find base rel.")));

        /* Is it still needed above this joinrel? */
        ndx = var->varattno - baserel->min_attr;
        if (bms_nonempty_difference(baserel->attr_needed[ndx], relids)) {
            /* Yup, add it to the output */
            joinrel->reltargetlist = lappend(joinrel->reltargetlist, var);
            joinrel->width += baserel->attr_widths[ndx];
            if (root->glob->vectorized) {
                joinrel->encodedwidth += columnar_get_col_width(exprType((Node *)var), baserel->attr_widths[ndx]);
                joinrel->encodednum++;
            }
        }
    }
}

/*
 * build_joinrel_restrictlist
 * build_joinrel_joinlist
 *	  These routines build lists of restriction and join clauses for a
 *	  join relation from the joininfo lists of the relations it joins.
 *
 *	  These routines are separate because the restriction list must be
 *	  built afresh for each pair of input sub-relations we consider, whereas
 *	  the join list need only be computed once for any join RelOptInfo.
 *	  The join list is fully determined by the set of rels making up the
 *	  joinrel, so we should get the same results (up to ordering) from any
 *	  candidate pair of sub-relations.	But the restriction list is whatever
 *	  is not handled in the sub-relations, so it depends on which
 *	  sub-relations are considered.
 *
 *	  If a join clause from an input relation refers to base rels still not
 *	  present in the joinrel, then it is still a join clause for the joinrel;
 *	  we put it into the joininfo list for the joinrel.  Otherwise,
 *	  the clause is now a restrict clause for the joined relation, and we
 *	  return it to the caller of build_joinrel_restrictlist() to be stored in
 *	  join paths made from this pair of sub-relations.	(It will not need to
 *	  be considered further up the join tree.)
 *
 *	  In many case we will find the same RestrictInfos in both input
 *	  relations' joinlists, so be careful to eliminate duplicates.
 *	  Pointer equality should be a sufficient test for dups, since all
 *	  the various joinlist entries ultimately refer to RestrictInfos
 *	  pushed into them by distribute_restrictinfo_to_rels().
 *
 * 'joinrel' is a join relation node
 * 'outer_rel' and 'inner_rel' are a pair of relations that can be joined
 *		to form joinrel.
 *
 * build_joinrel_restrictlist() returns a list of relevant restrictinfos,
 * whereas build_joinrel_joinlist() stores its results in the joinrel's
 * joininfo list.  One or the other must accept each given clause!
 *
 * NB: Formerly, we made deep(!) copies of each input RestrictInfo to pass
 * up to the join relation.  I believe this is no longer necessary, because
 * RestrictInfo nodes are no longer context-dependent.	Instead, just include
 * the original nodes in the lists made for the join relation.
 */
static List* build_joinrel_restrictlist(
    PlannerInfo* root, RelOptInfo* joinrel, RelOptInfo* outer_rel, RelOptInfo* inner_rel)
{
    List* result = NIL;
    List* implied_result = NIL;

    /*
     * Collect all the clauses that syntactically belong at this level,
     * eliminating any duplicates (important since we will see many of the
     * same clauses arriving from both input relations).
     */
    result = subbuild_joinrel_restrictlist(joinrel, outer_rel->joininfo, NIL);
    result = subbuild_joinrel_restrictlist(joinrel, inner_rel->joininfo, result);

    /*
     * Add on any clauses derived from EquivalenceClasses.	These cannot be
     * redundant with the clauses in the joininfo lists, so don't bother
     * checking.
     */
    implied_result = generate_join_implied_equalities(root, joinrel->relids, outer_rel->relids, inner_rel);

#ifdef STREAMPLAN
    /*
     * For stream plan, we generate implied join clause, but we only want to keep one.
     * Also, we need to add implied join clause to sub plan target list
     */
    if (IS_STREAM_PLAN) {
        ListCell* lc = NULL;
        RestrictInfo* first = NULL;

        for (lc = list_head(implied_result); lc != NULL;) {
            RestrictInfo* rinfo = (RestrictInfo*)lfirst(lc);
            lc = lnext(lc);
            if (rinfo->pseudoconstant) {
                if (first == NULL)
                    first = rinfo;
                implied_result = list_delete_ptr(implied_result, rinfo);
            }
        }
        if (result == NIL && implied_result == NIL && first != NULL) {
            /* disable hashjoin and mergejoin for such restrictinfo with only one value */
            first->hashjoinoperator = InvalidOid;
            first->mergeopfamilies = NIL;
            implied_result = list_make1(first);
        }
    }
#endif

    result = list_concat(result, implied_result);

    return result;
}

static void build_joinrel_joinlist(RelOptInfo* joinrel, RelOptInfo* outer_rel, RelOptInfo* inner_rel)
{
    List* result = NIL;

    /*
     * Collect all the clauses that syntactically belong above this level,
     * eliminating any duplicates (important since we will see many of the
     * same clauses arriving from both input relations).
     */
    result = subbuild_joinrel_joinlist(joinrel, outer_rel->joininfo, NIL);
    result = subbuild_joinrel_joinlist(joinrel, inner_rel->joininfo, result);

    joinrel->joininfo = result;
}

static List* subbuild_joinrel_restrictlist(const RelOptInfo* joinrel, const List* joininfo_list, List* new_restrictlist)
{
    ListCell* l = NULL;

    foreach (l, joininfo_list) {
        RestrictInfo* rinfo = (RestrictInfo*)lfirst(l);

        if (bms_is_subset(rinfo->required_relids, joinrel->relids)) {
            /*
             * This clause becomes a restriction clause for the joinrel, since
             * it refers to no outside rels.  Add it to the list, being
             * careful to eliminate duplicates. (Since RestrictInfo nodes in
             * different joinlists will have been multiply-linked rather than
             * copied, pointer equality should be a sufficient test.)
             */
            new_restrictlist = list_append_unique_ptr(new_restrictlist, rinfo);
        } else {
            /*
             * This clause is still a join clause at this level, so we ignore
             * it in this routine.
             */
        }
    }

    return new_restrictlist;
}

static List* subbuild_joinrel_joinlist(const RelOptInfo* joinrel, const List* joininfo_list, List* new_joininfo)
{
    ListCell* l = NULL;

    foreach (l, joininfo_list) {
        RestrictInfo* rinfo = (RestrictInfo*)lfirst(l);

        if (bms_is_subset(rinfo->required_relids, joinrel->relids)) {
            /*
             * This clause becomes a restriction clause for the joinrel, since
             * it refers to no outside rels.  So we can ignore it in this
             * routine.
             */
        } else {
            /*
             * This clause is still a join clause at this level, so add it to
             * the new joininfo list, being careful to eliminate duplicates.
             * (Since RestrictInfo nodes in different joinlists will have been
             * multiply-linked rather than copied, pointer equality should be
             * a sufficient test.)
             */
            new_joininfo = list_append_unique_ptr(new_joininfo, rinfo);
        }
    }

    return new_joininfo;
}

/*
 * find_childrel_appendrelinfo
 *		Get the AppendRelInfo associated with an appendrel child rel.
 *
 * This search could be eliminated by storing a link in child RelOptInfos,
 * but for now it doesn't seem performance-critical.
 */
AppendRelInfo* find_childrel_appendrelinfo(PlannerInfo* root, RelOptInfo* rel)
{
    Index relid = rel->relid;
    ListCell* lc = NULL;

    /* Should only be called on child rels */
    AssertEreport(rel->reloptkind == RELOPT_OTHER_MEMBER_REL, MOD_OPT, "");

    foreach (lc, root->append_rel_list) {
        AppendRelInfo* appinfo = (AppendRelInfo*)lfirst(lc);

        if (appinfo->child_relid == relid)
            return appinfo;
    }
    /* should have found the entry ... */
    ereport(ERROR,
        (errmodule(MOD_OPT),
            errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
            errmsg("child rel %u not found in append_rel_list", relid)));
    return NULL; /* not reached */
}

/*
 * get_baserel_parampathinfo
 *		Get the ParamPathInfo for a parameterized path for a base relation,
 *		constructing one if we don't have one already.
 *
 * This centralizes estimating the rowcounts for parameterized paths.
 * We need to cache those to be sure we use the same rowcount for all paths
 * of the same parameterization for a given rel.  This is also a convenient
 * place to determine which movable join clauses the parameterized path will
 * be responsible for evaluating.
 */
ParamPathInfo* get_baserel_parampathinfo(PlannerInfo* root, RelOptInfo* baserel,
            Relids required_outer, Bitmapset *upper_params)
{
    ParamPathInfo* ppi = NULL;
    Relids joinrelids;
    List* pclauses = NIL;
    double rows;
    ListCell* lc = NULL;

    /* Unparameterized paths have no ParamPathInfo */
    if (bms_is_empty(required_outer) && bms_is_empty(upper_params))
        return NULL;

    Assert(!bms_overlap(baserel->relids, required_outer));

    /* If we already have a PPI for this parameterization, just return it */
    foreach (lc, baserel->ppilist) {
        ppi = (ParamPathInfo*)lfirst(lc);
        if (bms_equal(ppi->ppi_req_outer, required_outer))
            return ppi;
    }

    /*
     * Identify all joinclauses that are movable to this base rel given this
     * parameterization.
     */
    joinrelids = bms_union(baserel->relids, required_outer);
    pclauses = NIL;
    foreach (lc, baserel->joininfo) {
        RestrictInfo* rinfo = (RestrictInfo*)lfirst(lc);

        if (join_clause_is_movable_into(rinfo, baserel->relids, joinrelids))
            pclauses = lappend(pclauses, rinfo);
    }

    if (!(baserel->rtekind == RTE_SUBQUERY && ENABLE_PRED_PUSH_ALL(root)))
    {
        /*
         * Add in joinclauses generated by EquivalenceClasses, too.  (These
         * necessarily satisfy join_clause_is_movable_into.)
         */
        pclauses = list_concat(pclauses, generate_join_implied_equalities(root, joinrelids, required_outer, baserel));
    }

    /* Estimate the number of global rows returned by the parameterized scan */
    rows = get_parameterized_baserel_size(root, baserel, pclauses);

    /* And now we can build the ParamPathInfo */
    ppi = makeNode(ParamPathInfo);
    ppi->ppi_req_outer = required_outer;
    ppi->ppi_req_upper = upper_params;
    ppi->ppi_rows = rows;
    ppi->ppi_clauses = pclauses;
    baserel->ppilist = lappend(baserel->ppilist, ppi);

    return ppi;
}

/*
 * get_subquery_parampathinfo
 *		Get the ParamPathInfo for a parameterized path for a sbuquery,
 *		constructing one if we don't have one already, only used for reparameterize_path.
 */
ParamPathInfo* get_subquery_parampathinfo(PlannerInfo* root, RelOptInfo* baserel,
            Relids required_outer, Bitmapset *upper_params)
{
    ParamPathInfo* ppi = NULL;
    Relids joinrelids;
    List* pclauses = NIL;
    double rows;
    ListCell* lc = NULL;

    /* Unparameterized paths have no ParamPathInfo */
    if (bms_is_empty(required_outer) && bms_is_empty(upper_params))
        return NULL;

    Assert(!bms_overlap(baserel->relids, required_outer));

    /* If we already have a PPI for this parameterization, just return it */
    foreach (lc, baserel->ppilist) {
        ppi = (ParamPathInfo*)lfirst(lc);
        if (bms_equal(ppi->ppi_req_outer, required_outer))
            return ppi;
    }

    /*
     * Identify all joinclauses that are movable to this base rel given this
     * parameterization.
     */
    joinrelids = bms_union(baserel->relids, required_outer);
    pclauses = NIL;
    foreach (lc, baserel->joininfo) {
        RestrictInfo* rinfo = (RestrictInfo*)lfirst(lc);

        if (join_clause_is_movable_into(rinfo, baserel->relids, joinrelids))
            pclauses = lappend(pclauses, rinfo);
    }

    Assert(baserel->rtekind == RTE_SUBQUERY);
    /*
     * Add in joinclauses generated by EquivalenceClasses, too.  (These
     * necessarily satisfy join_clause_is_movable_into.)
     */
    pclauses = list_concat(pclauses, generate_join_implied_equalities(root, joinrelids, required_outer, baserel));

    /* Estimate the number of global rows returned by the parameterized scan */
    rows = get_parameterized_baserel_size(root, baserel, pclauses);

    /* And now we can build the ParamPathInfo */
    ppi = makeNode(ParamPathInfo);
    ppi->ppi_req_outer = required_outer;
    ppi->ppi_req_upper = upper_params;
    ppi->ppi_rows = rows;
    ppi->ppi_clauses = pclauses;
    baserel->ppilist = lappend(baserel->ppilist, ppi);

    return ppi;
}

/*
 * get_joinrel_parampathinfo
 *		Get the ParamPathInfo for a parameterized path for a join relation,
 *		constructing one if we don't have one already.
 *
 * This centralizes estimating the rowcounts for parameterized paths.
 * We need to cache those to be sure we use the same rowcount for all paths
 * of the same parameterization for a given rel.  This is also a convenient
 * place to determine which movable join clauses the parameterized path will
 * be responsible for evaluating.
 *
 * outer_path and inner_path are a pair of input paths that can be used to
 * construct the join, and restrict_clauses is the list of regular join
 * clauses (including clauses derived from EquivalenceClasses) that must be
 * applied at the join node when using these inputs.
 *
 * Unlike the situation for base rels, the set of movable join clauses to be
 * enforced at a join varies with the selected pair of input paths, so we
 * must calculate that and pass it back, even if we already have a matching
 * ParamPathInfo.  We handle this by adding any clauses moved down to this
 * join to *restrict_clauses, which is an in/out parameter.  (The addition
 * is done in such a way as to not modify the passed-in List structure.)
 *
 * Note: when considering a nestloop join, the caller must have removed from
 * restrict_clauses any movable clauses that are themselves scheduled to be
 * pushed into the right-hand path.  We do not do that here since it's
 * unnecessary for other join types.
 */
ParamPathInfo* get_joinrel_parampathinfo(PlannerInfo* root, RelOptInfo* joinrel, Path* outer_path, Path* inner_path,
    SpecialJoinInfo* sjinfo, Relids required_outer, List** restrict_clauses)
{
    ParamPathInfo* ppi = NULL;
    Relids join_and_req;
    Relids outer_and_req;
    Relids inner_and_req;
    List* pclauses = NIL;
    List* eclauses = NIL;
    List* dropped_ecs = NIL;
    double rows;
    ListCell* lc = NULL;
    Bitmapset *upper_params = NULL;

    /* Unparameterized paths have no ParamPathInfo or extra join clauses */
    if (bms_is_empty(required_outer) &&
        bms_is_empty(PATH_REQ_UPPER(outer_path)) &&
        bms_is_empty(PATH_REQ_UPPER(inner_path)))
        return NULL;

    AssertEreport(!bms_overlap(joinrel->relids, required_outer), MOD_OPT, "");

    /*
     * Identify all joinclauses that are movable to this join rel given this
     * parameterization.  These are the clauses that are movable into this
     * join, but not movable into either input path.  Treat an unparameterized
     * input path as not accepting parameterized clauses (because it won't,
     * per the shortcut exit above), even though the joinclause movement rules
     * might allow the same clauses to be moved into a parameterized path for
     * that rel.
     */
    join_and_req = bms_union(joinrel->relids, required_outer);
    if (outer_path->param_info)
        outer_and_req = bms_union(outer_path->parent->relids, PATH_REQ_OUTER(outer_path));
    else
        outer_and_req = NULL; /* outer path does not accept parameters */
    if (inner_path->param_info)
        inner_and_req = bms_union(inner_path->parent->relids, PATH_REQ_OUTER(inner_path));
    else
        inner_and_req = NULL; /* inner path does not accept parameters */

    pclauses = NIL;
    foreach (lc, joinrel->joininfo) {
        RestrictInfo* rinfo = (RestrictInfo*)lfirst(lc);

        if (join_clause_is_movable_into(rinfo, joinrel->relids, join_and_req) &&
            !join_clause_is_movable_into(rinfo, outer_path->parent->relids, outer_and_req) &&
            !join_clause_is_movable_into(rinfo, inner_path->parent->relids, inner_and_req))
            pclauses = lappend(pclauses, rinfo);
    }

    /* Consider joinclauses generated by EquivalenceClasses, too */
    eclauses = generate_join_implied_equalities(root, join_and_req, required_outer, joinrel);
    /* We only want ones that aren't movable to lower levels */
    dropped_ecs = NIL;
    foreach (lc, eclauses) {
        RestrictInfo* rinfo = (RestrictInfo*)lfirst(lc);

        /*
         * In principle, join_clause_is_movable_into() should accept anything
         * returned by generate_join_implied_equalities(); but because its
         * analysis is only approximate, sometimes it doesn't.  So we
         * currently cannot use this Assert; instead just assume it's okay to
         * apply the joinclause at this level.
         */
#ifdef NOT_USED
        Assert(join_clause_is_movable_into(rinfo, joinrel->relids, join_and_req));
#endif
        if (join_clause_is_movable_into(rinfo, outer_path->parent->relids, outer_and_req))
            continue; /* drop if movable into LHS */
        if (join_clause_is_movable_into(rinfo, inner_path->parent->relids, inner_and_req)) {
            /* drop if movable into RHS, but remember EC for use below */
            AssertEreport(rinfo->left_ec == rinfo->right_ec, MOD_OPT, "");
            dropped_ecs = lappend(dropped_ecs, rinfo->left_ec);
            continue;
        }
        pclauses = lappend(pclauses, rinfo);
    }

    /*
     * EquivalenceClasses are harder to deal with than we could wish, because
     * of the fact that a given EC can generate different clauses depending on
     * context.  Suppose we have an EC {X.X, Y.Y, Z.Z} where X and Y are the
     * LHS and RHS of the current join and Z is in required_outer, and further
     * suppose that the inner_path is parameterized by both X and Z.  The code
     * above will have produced either Z.Z = X.X or Z.Z = Y.Y from that EC,
     * and in the latter case will have discarded it as being movable into the
     * RHS.  However, the EC machinery might have produced either Y.Y = X.X or
     * Y.Y = Z.Z as the EC enforcement clause within the inner_path; it will
     * not have produced both, and we can't readily tell from here which one
     * it did pick.  If we add no clause to this join, we'll end up with
     * insufficient enforcement of the EC; either Z.Z or X.X will fail to be
     * constrained to be equal to the other members of the EC.  (When we come
     * to join Z to this X/Y path, we will certainly drop whichever EC clause
     * is generated at that join, so this omission won't get fixed later.)
     *
     * To handle this, for each EC we discarded such a clause from, try to
     * generate a clause connecting the required_outer rels to the join's LHS
     * ("Z.Z = X.X" in the terms of the above example).  If successful, and if
     * the clause can't be moved to the LHS, add it to the current join's
     * restriction clauses.  (If an EC cannot generate such a clause then it
     * has nothing that needs to be enforced here, while if the clause can be
     * moved into the LHS then it should have been enforced within that path.)
     *
     * Note that we don't need similar processing for ECs whose clause was
     * considered to be movable into the LHS, because the LHS can't refer to
     * the RHS so there is no comparable ambiguity about what it might
     * actually be enforcing internally.
     */
    if (dropped_ecs != NULL) {
        Relids real_outer_and_req;

        real_outer_and_req = bms_union(outer_path->parent->relids, required_outer);
        eclauses = generate_join_implied_equalities_for_ecs(
            root, dropped_ecs, real_outer_and_req, required_outer, outer_path->parent);
        foreach (lc, eclauses) {
            RestrictInfo* rinfo = (RestrictInfo*)lfirst(lc);

            /* As above, can't quite assert this here */
#ifdef NOT_USED
            Assert(join_clause_is_movable_into(rinfo, outer_path->parent->relids, real_outer_and_req));
#endif
            if (!join_clause_is_movable_into(rinfo, outer_path->parent->relids, outer_and_req))
                pclauses = lappend(pclauses, rinfo);
        }
    }

    /*
     * Now, attach the identified moved-down clauses to the caller's
     * restrict_clauses list.  By using list_concat in this order, we leave
     * the original list structure of restrict_clauses undamaged.
     */
    *restrict_clauses = list_concat(pclauses, *restrict_clauses);

    /* If we already have a PPI for this parameterization, just return it */
    foreach (lc, joinrel->ppilist) {
        ppi = (ParamPathInfo*)lfirst(lc);
        if (bms_equal(ppi->ppi_req_outer, required_outer))
            return ppi;
    }

    /* Estimate the number of rows returned by the parameterized join */
    rows = get_parameterized_joinrel_size(root, joinrel, outer_path->rows, inner_path->rows, sjinfo, *restrict_clauses);

    /*
     * And now we can build the ParamPathInfo.	No point in saving the
     * input-pair-dependent clause list, though.
     *
     * Note: in GEQO mode, we'll be called in a temporary memory context, but
     * the joinrel structure is there too, so no problem.
     */
    upper_params = bms_union(PATH_REQ_UPPER(outer_path), PATH_REQ_UPPER(inner_path));
    ppi = makeNode(ParamPathInfo);
    ppi->ppi_req_outer = required_outer;
    ppi->ppi_rows = rows;
    ppi->ppi_clauses = NIL;
    ppi->ppi_req_upper = upper_params;
    joinrel->ppilist = lappend(joinrel->ppilist, ppi);

    return ppi;
}

/*
 * get_appendrel_parampathinfo
 *		Get the ParamPathInfo for a parameterized path for an append relation.
 *
 * For an append relation, the rowcount estimate will just be the sum of
 * the estimates for its children.	However, we still need a ParamPathInfo
 * to flag the fact that the path requires parameters.	So this just creates
 * a suitable struct with zero ppi_rows (and no ppi_clauses either, since
 * the Append node isn't responsible for checking quals).
 */
ParamPathInfo* get_appendrel_parampathinfo(RelOptInfo* appendrel, Relids required_outer, Bitmapset* upper_params)
{
    ParamPathInfo* ppi = NULL;
    ListCell* lc = NULL;

    /* Unparameterized paths have no ParamPathInfo */
    if (bms_is_empty(required_outer) && bms_is_empty(upper_params))
        return NULL;

    Assert(!bms_overlap(appendrel->relids, required_outer));

    /* If we already have a PPI for this parameterization, just return it */
    foreach (lc, appendrel->ppilist) {
        ppi = (ParamPathInfo*)lfirst(lc);
        if (bms_equal(ppi->ppi_req_outer, required_outer))
            return ppi;
    }

    /* Else build the ParamPathInfo */
    ppi = makeNode(ParamPathInfo);
    ppi->ppi_req_outer = required_outer;
    ppi->ppi_rows = 0;
    ppi->ppi_clauses = NIL;
    ppi->ppi_req_upper = upper_params;
    appendrel->ppilist = lappend(appendrel->ppilist, ppi);

    return ppi;
}

/*
 * add_eq_item_to_list
 * 	add expr to join_dis_key_list, partitioned by relids in other_expr
 *
 * Parameters:
 * 	join_dis_key_list(out): further join superset key list
 * 	relid_list(out): relid list to partition by superset key list, should has same members as join_dis_key_list
 * 	expr: the expression to add to join superset key list
 * 	other_expr: the expression to extract relids
 */
static void add_eq_item_to_list(List** join_dis_key_list, List** relid_list, Node* expr, Node* other_expr)
{
    Relids var_nos;
    ListCell* lc1 = NULL;
    ListCell* lc2 = NULL;
    var_nos = pull_varnos(other_expr);
    /* find if there's same relids already exist, if so, just add expr in corresponding list */
    forboth(lc1, *relid_list, lc2, *join_dis_key_list)
    {
        Relids tmp_varnos = (Relids)lfirst(lc1);
        if (bms_equal(var_nos, tmp_varnos)) {
            List* current_dis_keys = (List*)lfirst(lc2);
            current_dis_keys = list_append_unique(current_dis_keys, expr);
            bms_free_ext(var_nos);
            break;
        }
    }
    /* if not exists, just build a new item in the list */
    if (lc1 == NULL) {
        Assert(lc2 == NULL);
        *relid_list = lappend(*relid_list, var_nos);
        *join_dis_key_list = lappend(*join_dis_key_list, list_make1(expr));
    }
}

/* build_joinrel_itst_diskeys
 * 	Build interesting distribute key sets for each join relation
 *
 * Parameters:
 * 	root: planner info structure for current query level
 * 	joinrel: target relation to build interesting super set key and matching key
 * 	outerrel, innerrel: two branch relation for current joinrel
 * 	jointype: jointype between outerrel and innerrel
 */
static void build_joinrel_itst_diskeys(
    PlannerInfo* root, RelOptInfo* joinrel, RelOptInfo* outerrel, RelOptInfo* innerrel, JoinType jointype)
{
    /* first free out all the interesting keys of last relation */
    if (joinrel->rel_dis_keys.superset_keys) {
        ListCell* lc = NULL;
        foreach (lc, joinrel->rel_dis_keys.superset_keys) {
            list_free((List*)lfirst(lc));
        }
        list_free_ext(joinrel->rel_dis_keys.superset_keys);
        joinrel->rel_dis_keys.superset_keys = NIL;
    }
    joinrel->rel_dis_keys.matching_keys = NIL;

    /* for distribute key of target table, we should find exact matches */
    if (root->dis_keys.matching_keys != NIL) {
        Relids var_nos = pull_varnos((Node*)root->dis_keys.matching_keys);
        if ((LHS_join(jointype) && bms_is_subset(var_nos, outerrel->relids)) ||
            (RHS_join(jointype) && bms_is_subset(var_nos, innerrel->relids)) ||
            (jointype == JOIN_INNER && bms_is_subset(var_nos, joinrel->relids)))
            joinrel->rel_dis_keys.matching_keys = root->dis_keys.matching_keys;
        bms_free_ext(var_nos);
    }

    joinrel->rel_dis_keys.superset_keys = build_superset_keys_for_rel(root, joinrel, outerrel, innerrel, jointype);

    /* debug info for global path, printing out built superset key and matching key */
    if (log_min_messages <= DEBUG1) {
        StringInfoData ds;
        initStringInfo(&ds);
        appendBitmapsetToString(&ds, joinrel->relids);
        ereport(DEBUG1, (errmodule(MOD_OPT_JOIN), errmsg("joinrel: %s", ds.data)));
        pfree(ds.data);
        ds.data = NULL;
        elog_node_display(DEBUG1, "[OPT_JOIN] matching keys", joinrel->rel_dis_keys.matching_keys, true);
        elog_node_display(DEBUG1, "[OPT_JOIN] superset keys", joinrel->rel_dis_keys.superset_keys, true);
    }
}

/*
 * build_superset_keys_for_rel
 *	build list of superset keys list for the rel, with each item be the join key to every
 *	different rel, and then group by key. The function is applied to both joinrel and simple rel
 * Parameters:
 *	@in root: planner info of current query level
 *	@in rel: input rel, can be join rel or simple rel
 *	@in outerrel: outer rel of join rel, only valid when rel is join rel
 *	@in innerrel: inner rel of join rel, only valid when rel is join rel
 *	@in jointyp: join type of join rel, only valid when rel is join rel
 * Return:
 *	built list of superset key list
 */
List* build_superset_keys_for_rel(
    PlannerInfo* root, RelOptInfo* rel, RelOptInfo* outerrel, RelOptInfo* innerrel, JoinType jointype)
{
    ListCell* lc = NULL;
    ListCell* lc2 = NULL;
    List* relid_list = NIL;        /* further join relids to group in super key list */
    List* join_dis_key_list = NIL; /* further join superset key list */
    List* agg_dis_key_list = NIL;  /* agg superset key list */

    /*
     * for join condition, we find the further possible join columns to be super set key,
     * grouping by relids of other join expr. For example, join relation is (t1,t2), and further
     * join condition is: t1.a = t3.a and t2.a = t3.b and t1.c=t4.d, so the super set key
     * should be: 1. (t1,a, t2.a), will be joined with t3; 2. (t1.c), will be joined with t4.
     */
    foreach (lc, rel->joininfo) {
        RestrictInfo* restrictinfo = (RestrictInfo*)lfirst(lc);
        Expr* clause = restrictinfo->clause;
        Node* expr = NULL;
        Node* other_expr = NULL;
        RelOptInfo* currel[2];

        /*
         * only equal comparison hashable condition is cared
         * we should also add mergejoinable conditions later
         */
        if (!restrictinfo->can_join || restrictinfo->hashjoinoperator == InvalidOid)
            continue;

        /*
         * Search restrictinfo to find relids for cur rel and other rel. Loop 2
         * times for outerrel and innerrel for joinrel, and 1 times for base rel.
         */
        if (rel->reloptkind == RELOPT_JOINREL) {
            /* we only care outer join's expr to be interesting key */
            currel[0] = LHS_join(jointype) ? outerrel : NULL;
            currel[1] = RHS_join(jointype) ? innerrel : NULL;
        } else {
            currel[0] = rel;
            currel[1] = NULL;
        }

        for (int i = 0; i < 2; i++) {
            if (currel[i] == NULL)
                continue;

            if (bms_is_subset(restrictinfo->left_relids, currel[i]->relids)) {
                expr = (Node*)linitial(((OpExpr*)clause)->args);
                other_expr = (Node*)lsecond(((OpExpr*)clause)->args);
            } else if (bms_is_subset(restrictinfo->right_relids, currel[i]->relids)) {
                expr = (Node*)lsecond(((OpExpr*)clause)->args);
                other_expr = (Node*)linitial(((OpExpr*)clause)->args);
            }
        }

        /* add the found expr to potential super key list */
        if (other_expr != NULL)
            add_eq_item_to_list(&join_dis_key_list, &relid_list, expr, other_expr);
    }

    /*
     * For join clauses not in joininfo, also add them to adsired join list. This is mostly
     * for simple natural join case
     */
    if (rel->has_eclass_joins) {
        /* loop over all the equivalence class of current query level */
        foreach (lc, root->eq_classes) {
            EquivalenceClass* ec = (EquivalenceClass*)lfirst(lc);

            /* Skip if there's only one or const member in equivalence class */
            if (list_length(ec->ec_members) <= 1 || ec->ec_has_const)
                continue;

            /* Add inner equal join clause for current ec to itst join dis key list */
            if (rel->reloptkind == RELOPT_JOINREL) {
                if (LHS_join(jointype))
                    add_eqjoin_diskey_for_ec(rel, outerrel, ec, &join_dis_key_list, &relid_list);
                if (RHS_join(jointype))
                    add_eqjoin_diskey_for_ec(rel, innerrel, ec, &join_dis_key_list, &relid_list);
            } else
                add_eqjoin_diskey_for_ec(rel, rel, ec, &join_dis_key_list, &relid_list);
        }
    }

    /* for group by column, we search sub set within current scope */
    foreach (lc, root->dis_keys.superset_keys) {
        List* dis_keys = NIL; /* super key list from root's superset keys */
        List* superset_keys = (List*)lfirst(lc);

        foreach (lc2, superset_keys) {
            Node* expr = (Node*)lfirst(lc2);
            Relids var_nos = pull_varnos(expr);
            if (!bms_is_empty(var_nos)) {
                /* For joinrel, only outer side rel should be cared */
                if (rel->reloptkind == RELOPT_JOINREL) {
                    if ((LHS_join(jointype) && bms_is_subset(var_nos, outerrel->relids)) ||
                        (RHS_join(jointype) && bms_is_subset(var_nos, innerrel->relids)))
                        dis_keys = list_append_unique(dis_keys, expr);
                } else if (bms_is_subset(var_nos, rel->relids)) {
                    /* for cross subquery var pass down, only consider base vars */
                    Var* var = locate_distribute_var((Expr*)expr);
                    if (var != NULL)
                        dis_keys = list_append_unique(dis_keys, var);
                }
            }
            bms_free_ext(var_nos);
        }
        if (dis_keys != NIL)
            agg_dis_key_list = lappend(agg_dis_key_list, dis_keys);
    }

    join_dis_key_list = list_concat(join_dis_key_list, agg_dis_key_list);

    return remove_duplicate_superset_keys(join_dis_key_list);
}

/*
 * add_eqjoin_diskey_for_ec
 *	For each equivalence class, find eq join key to different rel, and add
 *	it in join diskey list group by relids
 * Parameter:
 *	@in rel: input rel, can be join rel or simple rel
 *	@in siderel: rel that expected to cover ec, should be one side of rel
 *			for join rel, or simple rel
 *	@in ec: equivalence class to find eq join key
 *	@out join_diskey_list: updated join diskey list when find eq join diskey
 *	@out relid_list: updated relid list when find eq join diskey
 */
static void add_eqjoin_diskey_for_ec(
    const RelOptInfo* rel, const RelOptInfo* siderel, const EquivalenceClass* ec, List** join_diskey_list, List** relid_list)
{
    ListCell* lc = NULL;
    ListCell* lc2 = NULL;

    /*
     * If there's possibility to find equal condition, we should proceed further. That
     * is, the relids for current equi-class is not only outerrel's, and it should overlaps
     * with outerrel.
     */
    if (bms_overlap(siderel->relids, ec->ec_relids) && !bms_is_subset(ec->ec_relids, rel->relids)) {
        Node* node = NULL;
        EquivalenceMember* em = NULL;
        /* loop equi-members to check if outer join rel's vars in equi-members */
        foreach (lc, ec->ec_members) {
            em = (EquivalenceMember*)lfirst(lc);
            if (bms_is_subset(em->em_relids, siderel->relids)) {
                if (IsTypeDistributable(exprType((Node*)em->em_expr)))
                    break;
            }
        }
        /* find an outer join rel's var in equi-class, and further find it belongs to further join */
        if (lc != NULL) {
            node = (Node*)em->em_expr;
            foreach (lc2, ec->ec_members) {
                em = (EquivalenceMember*)lfirst(lc2);
                /* find an equal condition */
                if (!bms_overlap(em->em_relids, rel->relids))
                    add_eq_item_to_list(join_diskey_list, relid_list, node, (Node*)em->em_expr);
            }
        }
    }
}

/*
 * remove_duplicate_superset_keys
 *	remove duplicate superset keys from the superset candidate list.
 *	Since we don't care the sequence of keys, so list with same of
 *	subset items will be removed
 * Parameters:
 *	@in superset_key_list: candidate superset key list pending remove
 * Return:
 *	final superset key list without duplication
 */
List* remove_duplicate_superset_keys(List* superset_key_list)
{
    ListCell* lc = NULL;
    ListCell* lc2 = NULL;
    List* final_dis_key_list = NIL;

    /* Get rid of the duplicate or subset superset keys. We use the same strategy in add_path(). */
    foreach (lc, superset_key_list) {
        List* dis_keys = (List*)lfirst(lc);
        List* final_dis_keys = NIL;
        List* removed_dis_keys = NIL;
        bool accept_new = true;

        foreach (lc2, final_dis_key_list) {
            final_dis_keys = (List*)lfirst(lc2);
            List* new_diff = list_difference(dis_keys, final_dis_keys);
            List* old_diff = list_difference(final_dis_keys, dis_keys);
            if (new_diff != NIL && old_diff == NIL)                           /* old is subset of new */
                removed_dis_keys = lappend(removed_dis_keys, final_dis_keys); /* remove old */
            else if (new_diff == NIL)                                         /* new is subset of old */
                accept_new = false;                                           /* accept new */
            list_free_ext(new_diff);
            list_free_ext(old_diff);
        }
        if (accept_new)
            final_dis_key_list = lappend(final_dis_key_list, dis_keys);
        foreach (lc2, removed_dis_keys) {
            List* dis_key = (List*)lfirst(lc2);
            final_dis_key_list = list_delete(final_dis_key_list, dis_key);
        }
        list_free_ext(removed_dis_keys);
    }
    list_free_ext(superset_key_list);
    return final_dis_key_list;
}
