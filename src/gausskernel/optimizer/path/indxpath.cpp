/* -------------------------------------------------------------------------
 *
 * indxpath.cpp
 *	  Routines to determine which indexes are usable for scanning a
 *	  given relation, and create Paths accordingly.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/optimizer/path/indxpath.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include <math.h>

#include "access/skey.h"
#include "access/sysattr.h"
#include "access/multi_redo_api.h"
#include "catalog/index.h"
#include "catalog/pg_am.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_opfamily.h"
#include "catalog/pg_type.h"
#include "catalog/pg_proc.h"
#include "nodes/makefuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/planner.h"
#include "optimizer/predtest.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/var.h"
#include "parser/parse_hint.h"
#include "parser/parsetree.h"
#include "utils/builtins.h"
#include "utils/bytea.h"
#include "utils/lsyscache.h"
#include "utils/pg_locale.h"
#include "utils/selfuncs.h"
#include "optimizer/gplanmgr.h"
#include "instruments/instr_statement.h"
#include "utils/expr_distinct.h"
#include "catalog/gs_collation.h"

#define IsBooleanOpfamily(opfamily) ((opfamily) == BOOL_BTREE_FAM_OID || \
    (opfamily) == BOOL_HASH_FAM_OID || (opfamily) == BOOL_UBTREE_FAM_OID)

#define BTREE_AM_OID 403

#define IndexCollMatchesExprColl(idxcollation, exprcollation) \
    ((idxcollation) == InvalidOid || (idxcollation) == (exprcollation))

#define PrefixKeyColumnMatched(indexkey, operand) \
    ((indexkey) && IsA((indexkey), PrefixKey) && IsA(((PrefixKey*)(indexkey))->arg, Var) &&        \
     (operand) && IsA((operand), Var) &&                                                           \
     ((Var*)((PrefixKey*)(indexkey))->arg)->varno == ((Var*)(operand))->varno &&                   \
     ((Var*)((PrefixKey*)(indexkey))->arg)->varattno == ((Var*)(operand))->varattno)

/* Whether to use ScalarArrayOpExpr to build index qualifications */
typedef enum {
    SAOP_PER_AM, /* Use ScalarArrayOpExpr if amsearcharray */
    SAOP_ALLOW,  /* Use ScalarArrayOpExpr for all indexes */
    SAOP_REQUIRE /* Require ScalarArrayOpExpr to be used */
} SaOpControl;

/* Whether we are looking for plain indexscan, bitmap scan, or either */
typedef enum {
    ST_INDEXSCAN,  /* must support amgettuple */
    ST_BITMAPSCAN, /* must support amgetbitmap */
    ST_ANYSCAN     /* either is okay */
} ScanTypeControl;

/* Data structure for collecting qual clauses that match an index */
typedef struct {
    bool nonempty; /* True if lists are not all empty */
    /* Lists of RestrictInfos, one per index column */
    List* indexclauses[INDEX_MAX_KEYS];
} IndexClauseSet;

/* Per-path data used within choose_bitmap_and() */
typedef struct {
    Path* path;           /* IndexPath, BitmapAndPath, or BitmapOrPath */
    List* quals;          /* the WHERE clauses it uses */
    List* preds;          /* predicates of its partial index(es) */
    Bitmapset* clauseids; /* quals+preds represented as a bitmapset */
} PathClauseUsage;

/* Per-choose-bitmapand data used within ChooseBitmapAndWithMultiIndex() */
typedef struct {
    Cost costsofar;           /* path cost for multi-index-path */
    List* qualsofar;          /* contain quals for multi-index-path */
    Bitmapset* clauseidsofar; /* contain clause set  for multi-index-path */
    ListCell* lastcell;       /* lastcell in paths for quick deletions */
    List* paths;              /* path list for multi-index-path */
    int startPath;            /* check value for "AND group leader" */
} ChooseBitmapAndInfo;

/* Used to store the content needed for filling when converting the index conditions for operator like. */
typedef struct {
    int maxBufLen;
    char maxSortBuf[4];
    int minBufLen;
    char minSortBuf[4];
} PadContent;

static void consider_index_join_clauses(PlannerInfo* root, RelOptInfo* rel, IndexOptInfo* index,
    IndexClauseSet* rclauseset, IndexClauseSet* jclauseset, IndexClauseSet* eclauseset, List** bitindexpaths);
static void consider_index_join_outer_rels(PlannerInfo* root, RelOptInfo* rel, IndexOptInfo* index,
    IndexClauseSet* rclauseset, IndexClauseSet* jclauseset, IndexClauseSet* eclauseset, List** bitindexpaths,
    List* indexjoinclauses, int considered_clauses, List** considered_relids);
static void get_join_index_paths(PlannerInfo* root, RelOptInfo* rel, IndexOptInfo* index, IndexClauseSet* rclauseset,
    IndexClauseSet* jclauseset, IndexClauseSet* eclauseset, List** bitindexpaths, Relids relids,
    List** considered_relids);
static bool eclass_already_used(EquivalenceClass* parent_ec, Relids oldrelids, List* indexjoinclauses);
static bool bms_equal_any(Relids relids, List* relids_list);
static void get_index_paths(
    PlannerInfo* root, RelOptInfo* rel, IndexOptInfo* index, IndexClauseSet* clauses, List** bitindexpaths);
static List* build_index_paths(PlannerInfo* root, RelOptInfo* rel, IndexOptInfo* index, IndexClauseSet* clauses,
    bool useful_predicate, SaOpControl saop_control, ScanTypeControl scantype);
static List* build_paths_for_OR(
    PlannerInfo* root, RelOptInfo* rel, List* clauses, List* other_clauses, IndexFeature idx_feature);
static List* drop_indexable_join_clauses(RelOptInfo* rel, List* clauses);
static Path* choose_bitmap_and(PlannerInfo* root, RelOptInfo* rel, List* paths, List* globalPartIndexPaths = NIL);
static int path_usage_comparator(const void* a, const void* b);
static Cost bitmap_scan_cost_est(PlannerInfo* root, RelOptInfo* rel, Path* ipath);
static Cost bitmap_and_cost_est(PlannerInfo* root, RelOptInfo* rel, List* paths);
static PathClauseUsage* classify_index_clause_usage(Path* path, List** clauselist);
static Relids get_bitmap_tree_required_outer(Path* bitmapqual);
static Bitmapset* get_bitmap_tree_required_upper(Path* bitmapqual);
static void find_indexpath_quals(Path* bitmapqual, List** quals, List** preds);
static int find_list_position(Node* node, List** nodelist);
static bool check_index_only(PlannerInfo* root, RelOptInfo* rel, IndexOptInfo* index);
static double get_loop_count(PlannerInfo* root, Relids outer_relids);
static void match_restriction_clauses_to_index(RelOptInfo* rel, IndexOptInfo* index, IndexClauseSet* clauseset);
static void match_join_clauses_to_index(
    PlannerInfo* root, RelOptInfo* rel, IndexOptInfo* index,
    IndexClauseSet* clauseset, List** joinorclauses);
static void match_eclass_clauses_to_index(PlannerInfo* root, IndexOptInfo* index, IndexClauseSet* clauseset);
static void match_clauses_to_index(IndexOptInfo* index, List* clauses, IndexClauseSet* clauseset);
static void match_clause_to_index(IndexOptInfo* index, RestrictInfo* rinfo, IndexClauseSet* clauseset);
static bool match_clause_to_indexcol(IndexOptInfo* index, int indexcol, RestrictInfo* rinfo);
static bool is_indexable_operator(Oid expr_op, Oid opfamily, bool indexkey_on_left);
static bool match_rowcompare_to_indexcol(
    IndexOptInfo* index, int indexcol, Oid opfamily, Oid idxcollation, RowCompareExpr* clause);
static void match_pathkeys_to_index(
    IndexOptInfo* index, List* pathkeys, List** orderby_clauses_p, List** clause_columns_p);
static Expr* match_clause_to_ordering_op(IndexOptInfo* index, int indexcol, Expr* clause, Oid pk_opfamily);
static bool match_boolean_index_clause(Node* clause, int indexcol, IndexOptInfo* index);
static bool match_special_index_operator(Expr* clause, Oid opfamily, Oid idxcollation,
    bool indexkey_on_left, IndexOptInfo* index, int indexcol);
static Expr* expand_boolean_index_clause(Node* clause, int indexcol, IndexOptInfo* index);
static List* expand_indexqual_opclause(IndexOptInfo* index, RestrictInfo* rinfo, Oid opfamily, Oid idxcollation,
    int indexcol);
static RestrictInfo* expand_indexqual_rowcompare(RestrictInfo* rinfo, IndexOptInfo* index, int indexcol);
static List* prefix_quals(Node* leftop, Oid opfamily, Oid collation, Const* prefix,
    Pattern_Prefix_Status pstatus, int prefixkey_len);
static List* prefix_quals_with_encdoing(Node* leftop, Oid opfamily, Oid collation, Const* prefix,
    Pattern_Prefix_Status pstatus, int prefixkey_len);
static List* network_prefix_quals(Node* leftop, Oid expr_op, Oid opfamily, Datum rightop);
static Datum string_to_datum(const char* str, Oid datatype);
static Const* string_to_const(const char* str, Oid datatype);
static int get_index_column_prefix_lenth(IndexOptInfo *index, int indexcol);
static Const* prefix_const_node(Const* con, int prefix_len, Oid datatype);
static RestrictInfo* rewrite_opclause_for_prefixkey(
    RestrictInfo *rinfo, IndexOptInfo* index, Oid opfamily, int prefix_len);
static Const *pad_string_in_like(PadContent content, const Const *strConst, int length, bool isPadMax);
static int get_pad_length(Node *leftop, int prefixLen);
static PadContent get_pad_content(Oid collation);
static bool scalar_array_can_match_prefixkey(Node *saop_rexpr);
RestrictInfo *expand_indexqual_scalar_array_op_expr(IndexOptInfo *index, RestrictInfo *rinfo,
    Oid opfamily, int indexcol);
void check_report_cause_type(FuncExpr *funcExpr, int indkey);
Node* match_first_var_to_indkey(Node* node, int indkey);


/*
 * create_index_paths
 *
 *	  Generate all interesting index paths for the given relation.
 *	  Candidate paths are added to the rel's pathlist (using add_path).
 *
 * To be considered for an index scan, an index must match one or more
 * restriction clauses or join clauses from the query's qual condition,
 * or match the query's ORDER BY condition, or have a predicate that
 * matches the query's qual condition.
 *
 * There are two basic kinds of index scans.  A "plain" index scan uses
 * only restriction clauses (possibly none at all) in its indexqual,
 * so it can be applied in any context.  A "parameterized" index scan uses
 * join clauses (plus restriction clauses, if available) in its indexqual.
 * When joining such a scan to one of the relations supplying the other
 * variables used in its indexqual, the parameterized scan must appear as
 * the inner relation of a nestloop join; it can't be used on the outer side,
 * nor in a merge or hash join.  In that context, values for the other rels'
 * attributes are available and fixed during any one scan of the indexpath.
 *
 * An IndexPath is generated and submitted to add_path() for each plain or
 * parameterized index scan this routine deems potentially interesting for
 * the current query.
 *
 * 'rel' is the relation for which we want to generate index paths
 *
 * Note: check_partial_indexes() must have been run previously for this rel.
 *
 * Note: in cases involving LATERAL references in the relation's tlist, it's
 * possible that rel->lateral_relids is nonempty.  Currently, we include
 * lateral_relids into the parameterization reported for each path, but don't
 * take it into account otherwise.  The fact that any such rels *must* be
 * available as parameter sources perhaps should influence our choices of
 * index quals ... but for now, it doesn't seem worth troubling over.
 * In particular, comments below about "unparameterized" paths should be read
 * as meaning "unparameterized so far as the indexquals are concerned".
 */
void create_index_paths(PlannerInfo* root, RelOptInfo* rel)
{
    List* indexpaths = NIL;
    List* bitindexpaths = NIL;
    List* bitjoinpaths = NIL;
    List* joinorclauses = NIL;
    IndexClauseSet rclauseset;
    IndexClauseSet jclauseset;
    IndexClauseSet eclauseset;
    ListCell* lc = NULL;
    Bitmapset* required_upper = NULL;
    RangeTblEntry *rte = NULL;

    /* Skip the whole mess if no indexes */
    if (rel->indexlist == NIL)
        return;

    /*
     * Ignore index scan path when time capsule is enabled in base rel for correctess issue
     */
    rte = planner_rt_fetch(rel->relid, root);
    if (rel->is_ustore && rte->timecapsule != NULL) {
        ereport(DEBUG2, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("Unsupported IndexScan timecapsule-enabled base rel, relName:%s relOid:%u",
                rte->relname, rte->relid)));

        return;
    }

    /* Bitmap paths are collected and then dealt with at the end */
    bitindexpaths = bitjoinpaths = joinorclauses = NIL;

    /* Examine each index in turn */
    foreach (lc, rel->indexlist) {
        IndexOptInfo* index = (IndexOptInfo*)lfirst(lc);

        /* Protect limited-size array in IndexClauseSets */
        AssertEreport(index->ncolumns <= INDEX_MAX_KEYS, MOD_OPT, "Index column number is incorrect");

        /*
         * Ignore partial indexes that do not match the query.
         * (generate_bitmap_or_paths() might be able to do something with
         * them, but that's of no concern here.)
         */
        if (index->indpred != NIL && !index->predOK) {
            continue;
        }

        /*
         * Build paths with global indexes only for un-bounded partition tables.
         * The partition bounded tables should be handled by partition iterator
         * or local indexes.
         */
        if (index->isGlobal && rte && list_length(rte->partitionOidList) > 0) {
            continue;
        }

        /*
         * Identify the restriction clauses that can match the index.
         */
        errno_t errorno = EOK;
        errorno = memset_s(&rclauseset, sizeof(IndexClauseSet), 0, sizeof(rclauseset));
        securec_check(errorno, "\0", "\0");
        match_restriction_clauses_to_index(rel, index, &rclauseset);

        /*
         * Build index paths from the restriction clauses.	These will be
         * non-parameterized paths.  Plain paths go directly to add_path(),
         * bitmap paths are added to bitindexpaths to be handled below.
         */
        get_index_paths(root, rel, index, &rclauseset, &bitindexpaths);

        /* we also test the pushdown clauses */
        if (SUBQUERY_IS_PARAM(root) && rel->subplanrestrictinfo != NULL) {
            match_clauses_to_index(index, rel->subplanrestrictinfo, &rclauseset);
            get_index_paths(root, rel, index, &rclauseset, &bitindexpaths);
        }

        /*
         * Identify the join clauses that can match the index.	For the moment
         * we keep them separate from the restriction clauses.	Note that this
         * step finds only "loose" join clauses that have not been merged into
         * EquivalenceClasses.	Also, collect join OR clauses for later.
         */
        errorno = memset_s(&jclauseset, sizeof(IndexClauseSet), 0, sizeof(jclauseset));
        securec_check(errorno, "\0", "\0");
        match_join_clauses_to_index(root, rel, index, &jclauseset, &joinorclauses);

        /*
         * Look for EquivalenceClasses that can generate joinclauses matching
         * the index.
         */
        errorno = memset_s(&eclauseset, sizeof(IndexClauseSet), 0, sizeof(eclauseset));
        securec_check(errorno, "\0", "\0");
        match_eclass_clauses_to_index(root, index, &eclauseset);

        /*
         * If we found any plain or eclass join clauses, build parameterized
         * index paths using them.
         */
        if ((jclauseset.nonempty || eclauseset.nonempty)
#ifdef ENABLE_MULTIPLE_NODES
            && !WITHIN_SUBQUERY(root, rte)
#endif   /* ENABLE_MULTIPLE_NODES */
            )
            consider_index_join_clauses(root, rel, index, &rclauseset, &jclauseset, &eclauseset, &bitjoinpaths);
    }

    /*
     * Generate BitmapOrPaths for any suitable OR-clauses present in the
     * restriction list.  Add these to bitindexpaths.
     */
    indexpaths = generate_bitmap_or_paths(root, rel, rel->baserestrictinfo, NIL, false);
    bitindexpaths = list_concat(bitindexpaths, indexpaths);

    /*
     * Likewise, generate BitmapOrPaths for any suitable OR-clauses present in
     * the joinclause list.  Add these to bitjoinpaths.
     */
    indexpaths = generate_bitmap_or_paths(root, rel, joinorclauses, rel->baserestrictinfo, false);
    bitjoinpaths = list_concat(bitjoinpaths, indexpaths);

    /*
     * Generate BitmapOrPaths for any suitable OR-clauses present in the
     * restriction list and joinorclauses just use featured index.
     * Add these to bitindexpaths.
     */
    IndexFeature indexFeature = getIndexFeature(rel->isPartitionedTable, (rel->bucketInfo != NULL));
    if (indexFeature != NONFEATURED_INDEX) {
        indexpaths = GenerateBitmapOrPathsWithFeaturedIndex(root, rel, rel->baserestrictinfo, NIL, false, indexFeature);
        bitindexpaths = list_concat(bitindexpaths, indexpaths);

        indexpaths = GenerateBitmapOrPathsWithFeaturedIndex(root, rel, joinorclauses, rel->baserestrictinfo, false,
            indexFeature);
        bitjoinpaths = list_concat(bitjoinpaths, indexpaths);
    }

    /*
     * If we found anything usable, generate a BitmapHeapPath for the most
     * promising combination of restriction bitmap index paths.  Note there
     * will be only one such path no matter how many indexes exist.  This
     * should be sufficient since there's basically only one figure of merit
     * (total cost) for such a path.
     * Dfs table does not support bitmapscan now.
     */
    if (bitindexpaths != NIL && rel->orientation != REL_PAX_ORIENTED) {
        Path* bitmapqual = NULL;
        BitmapHeapPath* bpath = NULL;

        bitmapqual = choose_bitmap_and(root, rel, bitindexpaths);
        required_upper = get_bitmap_tree_required_upper(bitmapqual);
        bpath = create_bitmap_heap_path(root, rel, bitmapqual, rel->lateral_relids, required_upper, 1.0);
        add_path(root, rel, (Path*)bpath);
    }

    /*
     * Likewise, if we found anything usable, generate BitmapHeapPaths for the
     * most promising combinations of join bitmap index paths.	Our strategy
     * is to generate one such path for each distinct parameterization seen
     * among the available bitmap index paths.	This may look pretty
     * expensive, but usually there won't be very many distinct
     * parameterizations.  (This logic is quite similar to that in
     * consider_index_join_clauses, but we're working with whole paths not
     * individual clauses.)
     * Dfs table does not support bitmapscan now.
     */
    if (bitjoinpaths != NIL && rel->orientation != REL_PAX_ORIENTED) {
        List* path_outer = NIL;
        List* all_path_outers = NIL;
        ListCell* lc = NULL;

        /*
         * path_outer holds the parameterization of each path in bitjoinpaths
         * (to save recalculating that several times), while all_path_outers
         * holds all distinct parameterization sets.
         */
        path_outer = all_path_outers = NIL;
        foreach (lc, bitjoinpaths) {
            Path* path = (Path*)lfirst(lc);
            Relids required_outer;

            required_outer = get_bitmap_tree_required_outer(path);
            path_outer = lappend(path_outer, required_outer);
            if (!bms_equal_any(required_outer, all_path_outers))
                all_path_outers = lappend(all_path_outers, required_outer);
        }

        /* Now, for each distinct parameterization set ... */
        foreach (lc, all_path_outers) {
            Relids max_outers = (Relids)lfirst(lc);
            List* this_path_set = NIL;
            Path* bitmapqual = NULL;
            Relids required_outer = NULL;
            double loop_count = 0;
            BitmapHeapPath* bpath = NULL;
            ListCell* lcp = NULL;
            ListCell* lco = NULL;

            /* Identify all the bitmap join paths needing no more than that */
            this_path_set = NIL;
            forboth(lcp, bitjoinpaths, lco, path_outer)
            {
                Path* path = (Path*)lfirst(lcp);
                Relids p_outers = (Relids)lfirst(lco);
                if (bms_is_subset(p_outers, max_outers))
                    this_path_set = lappend(this_path_set, path);
            }

            /*
             * Add in restriction bitmap paths, since they can be used
             * together with any join paths.
             */
            this_path_set = list_concat(this_path_set, bitindexpaths);

            /* Select best AND combination for this parameterization */
            bitmapqual = choose_bitmap_and(root, rel, this_path_set);

            /* And push that path into the mix */
            required_outer = get_bitmap_tree_required_outer(bitmapqual);
            required_upper = get_bitmap_tree_required_upper(bitmapqual);
            loop_count = get_loop_count(root, required_outer);
            bpath = create_bitmap_heap_path(root, rel, bitmapqual, required_outer,
                                            required_upper, loop_count);
            add_path(root, rel, (Path*)bpath);
        }
    }
}

/*
 * getIndexFeature
 * Get index feature base on baserel feature.
 */
IndexFeature getIndexFeature(bool isPartitioned, bool hasBucket) {
    return (IndexFeature)((int)isPartitioned | (((int)hasBucket) << 1));
}

/*
 * consider_index_join_clauses
 *	  Given sets of join clauses for an index, decide which parameterized
 *	  index paths to build.
 *
 * Plain indexpaths are sent directly to add_path, while potential
 * bitmap indexpaths are added to *bitindexpaths for later processing.
 *
 * 'rel' is the index's heap relation
 * 'index' is the index for which we want to generate paths
 * 'rclauseset' is the collection of indexable restriction clauses
 * 'jclauseset' is the collection of indexable simple join clauses
 * 'eclauseset' is the collection of indexable clauses from EquivalenceClasses
 * '*bitindexpaths' is the list to add bitmap paths to
 */
static void consider_index_join_clauses(PlannerInfo* root, RelOptInfo* rel, IndexOptInfo* index,
    IndexClauseSet* rclauseset, IndexClauseSet* jclauseset, IndexClauseSet* eclauseset, List** bitindexpaths)
{
    int considered_clauses = 0;
    List* considered_relids = NIL;
    int indexcol;

    /*
     * The strategy here is to identify every potentially useful set of outer
     * rels that can provide indexable join clauses.  For each such set,
     * select all the join clauses available from those outer rels, add on all
     * the indexable restriction clauses, and generate plain and/or bitmap
     * index paths for that set of clauses.  This is based on the assumption
     * that it's always better to apply a clause as an indexqual than as a
     * filter (qpqual); which is where an available clause would end up being
     * applied if we omit it from the indexquals.
     *
     * This looks expensive, but in most practical cases there won't be very
     * many distinct sets of outer rels to consider.  As a safety valve when
     * that's not true, we use a heuristic: limit the number of outer rel sets
     * considered to a multiple of the number of clauses considered.  (We'll
     * always consider using each individual join clause, though.)
     *
     * For simplicity in selecting relevant clauses, we represent each set of
     * outer rels as a maximum set of clause_relids --- that is, the indexed
     * relation itself is also included in the relids set.  considered_relids
     * lists all relids sets we've already tried.
     */
    for (indexcol = 0; indexcol < index->nkeycolumns; indexcol++) {
        /* Consider each applicable simple join clause */
        considered_clauses += list_length(jclauseset->indexclauses[indexcol]);
        consider_index_join_outer_rels(root,
            rel,
            index,
            rclauseset,
            jclauseset,
            eclauseset,
            bitindexpaths,
            jclauseset->indexclauses[indexcol],
            considered_clauses,
            &considered_relids);
        /* Consider each applicable eclass join clause */
        considered_clauses += list_length(eclauseset->indexclauses[indexcol]);
        consider_index_join_outer_rels(root,
            rel,
            index,
            rclauseset,
            jclauseset,
            eclauseset,
            bitindexpaths,
            eclauseset->indexclauses[indexcol],
            considered_clauses,
            &considered_relids);
    }
}

/*
 * consider_index_join_outer_rels
 *	  Generate parameterized paths based on clause relids in the clause list.
 *
 * Workhorse for consider_index_join_clauses; see notes therein for rationale.
 *
 * 'rel', 'index', 'rclauseset', 'jclauseset', 'eclauseset', and
 *		'bitindexpaths' as above
 * 'indexjoinclauses' is a list of RestrictInfos for join clauses
 * 'considered_clauses' is the total number of clauses considered (so far)
 * '*considered_relids' is a list of all relids sets already considered
 */
static void consider_index_join_outer_rels(PlannerInfo* root, RelOptInfo* rel, IndexOptInfo* index,
    IndexClauseSet* rclauseset, IndexClauseSet* jclauseset, IndexClauseSet* eclauseset, List** bitindexpaths,
    List* indexjoinclauses, int considered_clauses, List** considered_relids)
{
    ListCell* lc = NULL;

    /* Examine relids of each joinclause in the given list */
    foreach (lc, indexjoinclauses) {
        RestrictInfo* rinfo = (RestrictInfo*)lfirst(lc);
        Relids clause_relids = rinfo->clause_relids;
        ListCell* lc2 = NULL;

        /* If we already tried its relids set, no need to do so again */
        if (bms_equal_any(clause_relids, *considered_relids))
            continue;

        /*
         * Generate the union of this clause's relids set with each
         * previously-tried set.  This ensures we try this clause along with
         * every interesting subset of previous clauses.  However, to avoid
         * exponential growth of planning time when there are many clauses,
         * limit the number of relid sets accepted to 10 * considered_clauses.
         *
         * Note: get_join_index_paths adds entries to *considered_relids, but
         * it prepends them to the list, so that we won't visit new entries
         * during the inner foreach loop.  No real harm would be done if we
         * did, since the subset check would reject them; but it would waste
         * some cycles.
         */
        foreach (lc2, *considered_relids) {
            Relids oldrelids = (Relids)lfirst(lc2);

            /*
             * If either is a subset of the other, no new set is possible.
             * This isn't a complete test for redundancy, but it's easy and
             * cheap.  get_join_index_paths will check more carefully if we
             * already generated the same relids set.
             */
            if (bms_subset_compare(clause_relids, oldrelids) != BMS_DIFFERENT)
                continue;

            /*
             * If this clause was derived from an equivalence class, the
             * clause list may contain other clauses derived from the same
             * eclass.  We should not consider that combining this clause with
             * one of those clauses generates a usefully different
             * parameterization; so skip if any clause derived from the same
             * eclass would already have been included when using oldrelids.
             */
            if (rinfo->parent_ec && eclass_already_used(rinfo->parent_ec, oldrelids, indexjoinclauses))
                continue;

            /*
             * If the number of relid sets considered exceeds our heuristic
             * limit, stop considering combinations of clauses.  We'll still
             * consider the current clause alone, though (below this loop).
             */
            if (list_length(*considered_relids) >= 10 * considered_clauses)
                break;

            /* OK, try the union set */
            get_join_index_paths(root,
                rel,
                index,
                rclauseset,
                jclauseset,
                eclauseset,
                bitindexpaths,
                bms_union(clause_relids, oldrelids),
                considered_relids);
        }

        /* Also try this set of relids by itself */
        get_join_index_paths(
            root, rel, index, rclauseset, jclauseset, eclauseset, bitindexpaths, clause_relids, considered_relids);
    }
}

/*
 * get_join_index_paths
 *	  Generate index paths using clauses from the specified outer relations.
 *	  In addition to generating paths, relids is added to *considered_relids
 *	  if not already present.
 *
 * Workhorse for consider_index_join_clauses; see notes therein for rationale.
 *
 * 'rel', 'index', 'rclauseset', 'jclauseset', 'eclauseset',
 *		'bitindexpaths', 'considered_relids' as above
 * 'relids' is the current set of relids to consider (the target rel plus
 *		one or more outer rels)
 */
static void get_join_index_paths(PlannerInfo* root, RelOptInfo* rel, IndexOptInfo* index, IndexClauseSet* rclauseset,
    IndexClauseSet* jclauseset, IndexClauseSet* eclauseset, List** bitindexpaths, Relids relids,
    List** considered_relids)
{
    IndexClauseSet clauseset;
    int indexcol;

    /* If we already considered this relids set, don't repeat the work */
    if (bms_equal_any(relids, *considered_relids))
        return;

    /* Identify indexclauses usable with this relids set */
    errno_t errorno = EOK;
    errorno = memset_s(&clauseset, sizeof(IndexClauseSet), 0, sizeof(clauseset));
    securec_check(errorno, "\0", "\0");

    for (indexcol = 0; indexcol < index->nkeycolumns; indexcol++) {
        ListCell* lc = NULL;

        /* First find applicable simple join clauses */
        foreach (lc, jclauseset->indexclauses[indexcol]) {
            RestrictInfo* rinfo = (RestrictInfo*)lfirst(lc);

            if (bms_is_subset(rinfo->clause_relids, relids))
                clauseset.indexclauses[indexcol] = lappend(clauseset.indexclauses[indexcol], rinfo);
        }

        /*
         * Add applicable eclass join clauses.  The clauses generated for each
         * column are redundant (cf generate_implied_equalities_for_indexcol),
         * so we need at most one.  This is the only exception to the general
         * rule of using all available index clauses.
         */
        foreach (lc, eclauseset->indexclauses[indexcol]) {
            RestrictInfo* rinfo = (RestrictInfo*)lfirst(lc);

            if (bms_is_subset(rinfo->clause_relids, relids)) {
                clauseset.indexclauses[indexcol] = lappend(clauseset.indexclauses[indexcol], rinfo);
                break;
            }
        }

        /* Add restriction clauses (this is nondestructive to rclauseset) */
        clauseset.indexclauses[indexcol] =
            list_concat(clauseset.indexclauses[indexcol], rclauseset->indexclauses[indexcol]);

        if (clauseset.indexclauses[indexcol] != NIL)
            clauseset.nonempty = true;
    }

    /* We should have found something, else caller passed silly relids */
    Assert(clauseset.nonempty);

    /* Build index path(s) using the collected set of clauses */
    get_index_paths(root, rel, index, &clauseset, bitindexpaths);

    /*
     * Remember we considered paths for this set of relids.  We use lcons not
     * lappend to avoid confusing the loop in consider_index_join_outer_rels.
     */
    *considered_relids = lcons(relids, *considered_relids);
}

/*
 * eclass_already_used
 *		True if any join clause usable with oldrelids was generated from
 *		the specified equivalence class.
 */
static bool eclass_already_used(EquivalenceClass* parent_ec, Relids oldrelids, List* indexjoinclauses)
{
    ListCell* lc = NULL;

    foreach (lc, indexjoinclauses) {
        RestrictInfo* rinfo = (RestrictInfo*)lfirst(lc);

        if (rinfo->parent_ec == parent_ec && bms_is_subset(rinfo->clause_relids, oldrelids))
            return true;
    }
    return false;
}

/*
 * bms_equal_any
 *		True if relids is bms_equal to any member of relids_list
 *
 * Perhaps this should be in bitmapset.c someday.
 */
static bool bms_equal_any(Relids relids, List* relids_list)
{
    ListCell* lc = NULL;

    foreach (lc, relids_list) {
        if (bms_equal(relids, (Relids)lfirst(lc)))
            return true;
    }
    return false;
}

/*
 * get_index_paths
 *	  Given an index and a set of index clauses for it, construct IndexPaths.
 *
 * Plain indexpaths are sent directly to add_path, while potential
 * bitmap indexpaths are added to *bitindexpaths for later processing.
 *
 * This is a fairly simple frontend to build_index_paths().  Its reason for
 * existence is mainly to handle ScalarArrayOpExpr quals properly.	If the
 * index AM supports them natively, we should just include them in simple
 * index paths.  If not, we should exclude them while building simple index
 * paths, and then make a separate attempt to include them in bitmap paths.
 */
static void get_index_paths(
    PlannerInfo* root, RelOptInfo* rel, IndexOptInfo* index, IndexClauseSet* clauses, List** bitindexpaths)
{
    List* indexpaths = NIL;
    ListCell* lc = NULL;

    /*
     * Build simple index paths using the clauses.	Allow ScalarArrayOpExpr
     * clauses only if the index AM supports them natively.
     */
    indexpaths = build_index_paths(root, rel, index, clauses, index->predOK, SAOP_PER_AM, ST_ANYSCAN);

    /*
     * Submit all the ones that can form plain IndexScan plans to add_path. (A
     * plain IndexPath can represent either a plain IndexScan or an
     * IndexOnlyScan, but for our purposes here that distinction does not
     * matter.	However, some of the indexes might support only bitmap scans,
     * and those we mustn't submit to add_path here.)
     *
     * Also, pick out the ones that are usable as bitmap scans.  For that, we
     * must discard indexes that don't support bitmap scans, and we also are
     * only interested in paths that have some selectivity; we should discard
     * anything that was generated solely for ordering purposes.
     */
    foreach (lc, indexpaths) {
        IndexPath* ipath = (IndexPath*)lfirst(lc);

        if (index->amhasgettuple)
            add_path(root, rel, (Path*)ipath);

        if (index->amhasgetbitmap && (ipath->path.pathkeys == NIL || ipath->indexselectivity < 1.0))
            *bitindexpaths = lappend(*bitindexpaths, ipath);
    }

    /*
     * If the index doesn't handle ScalarArrayOpExpr clauses natively, check
     * to see if there are any such clauses, and if so generate bitmap scan
     * paths relying on executor-managed ScalarArrayOpExpr.
     */
    if (!index->amsearcharray) {
        indexpaths = build_index_paths(root, rel, index, clauses, false, SAOP_REQUIRE, ST_BITMAPSCAN);
        *bitindexpaths = list_concat(*bitindexpaths, indexpaths);
    }
}

static inline bool index_relation_has_bucket(IndexOptInfo* index)
{
    Assert(index != NULL);
    Oid heapOid = IndexGetRelation(index->indexoid, NoLock);
    Relation rel = heap_open(heapOid, NoLock);
    bool hasBucket = RELATION_HAS_BUCKET(rel);
    heap_close(rel, NoLock);
    return hasBucket;
}

inline bool IsEqRestrict(const RestrictInfo* rinfo)
{
    Expr* clause = rinfo->clause;
    return is_opclause(clause) && get_oprrest(((OpExpr*)clause)->opno) == EQSELRETURNOID;
}

/*
 * Check whether the indexqualcols in indexpath contain the given attNum and the 
 * constraint condition on this attNum is equality constraints.
 */
inline bool HasAttNumAndEqRestrict(const IndexPath* newPath, const int attNum)
{
    ListCell* lc1 = NULL;
    ListCell* lc2 = NULL;
    IndexOptInfo* newPathIndex = (IndexOptInfo*)newPath->indexinfo;
    forboth (lc1, newPath->indexqualcols, lc2, newPath->indexquals) {
        int i = lfirst_int(lc1);
        RestrictInfo* rinfo = (RestrictInfo*)lfirst(lc2);
        if (newPathIndex->indexkeys[i] == attNum && IsEqRestrict(rinfo))
            return true;
    }
    return false;
}

/* 
 * Check whether index path contain all the index columns and the constraint conditions 
 * are equality constraints.
 */
inline bool ContainAllColsAndEqRestrict(const IndexPath* newPath, const IndexOptInfo* index)
{
    for (int pos = 0; pos < index->ncolumns; pos++) {
        if (!HasAttNumAndEqRestrict(newPath, index->indexkeys[pos]))
            return false;
    }
    return true;
}

/*
 * For the given index, we want to mark whether the index contains the columns come from an 
 * unique index and the constraint conditions on these columns are equality constraints. This 
 * mark will be used in unique index first rule during path generation.
 */
void MarkUniqueIndexFirstRule(const RelOptInfo* rel, const IndexOptInfo* index, List* result)
{
    if (!ENABLE_SQL_BETA_FEATURE(NO_UNIQUE_INDEX_FIRST) && OID_IS_BTREE(index->relam)) {
        ListCell* lcr = NULL;
        foreach (lcr, result) {
            IndexPath* newPath = (IndexPath*)lfirst(lcr);
            ListCell* lci = NULL;
            foreach (lci, rel->indexlist) {
                IndexOptInfo* indexToMatch = (IndexOptInfo*)lfirst(lci);
                if (OID_IS_BTREE(indexToMatch->relam) && indexToMatch->unique && 
                    ContainAllColsAndEqRestrict(newPath, indexToMatch)) {
                        newPath->rulesforindexgen |= BTREE_INDEX_CONTAIN_UNIQUE_COLS;
                        break;
                }
            }
        }
    }
}

/*
 * build_index_paths
 *	  Given an index and a set of index clauses for it, construct zero
 *	  or more IndexPaths.
 *
 * We return a list of paths because (1) this routine checks some cases
 * that should cause us to not generate any IndexPath, and (2) in some
 * cases we want to consider both a forward and a backward scan, so as
 * to obtain both sort orders.	Note that the paths are just returned
 * to the caller and not immediately fed to add_path().
 *
 * At top level, useful_predicate should be exactly the index's predOK flag
 * (ie, true if it has a predicate that was proven from the restriction
 * clauses).  When working on an arm of an OR clause, useful_predicate
 * should be true if the predicate required the current OR list to be proven.
 * Note that this routine should never be called at all if the index has an
 * unprovable predicate.
 *
 * saop_control indicates whether ScalarArrayOpExpr clauses can be used.
 * When it's SAOP_REQUIRE, index paths are created only if we found at least
 * one ScalarArrayOpExpr clause.
 *
 * scantype indicates whether we want to create plain indexscans, bitmap
 * indexscans, or both.  When it's ST_BITMAPSCAN, we will not consider
 * index ordering while deciding if a Path is worth generating.
 *
 * 'rel' is the index's heap relation
 * 'index' is the index for which we want to generate paths
 * 'clauses' is the collection of indexable clauses (RestrictInfo nodes)
 * 'useful_predicate' indicates whether the index has a useful predicate
 * 'saop_control' indicates whether ScalarArrayOpExpr clauses can be used
 * 'scantype' indicates whether we need plain or bitmap scan support
 */
static List* build_index_paths(PlannerInfo* root, RelOptInfo* rel, IndexOptInfo* index, IndexClauseSet* clauses,
    bool useful_predicate, SaOpControl saop_control, ScanTypeControl scantype)
{
    List* result = NIL;
    IndexPath* ipath = NULL;
    List* index_clauses = NIL;
    List* clause_columns = NIL;
    Relids outer_relids = NULL;
    Bitmapset *upper_params = NULL;
    double loop_count;
    List* orderbyclauses = NIL;
    List* orderbyclausecols = NIL;
    List* index_pathkeys = NIL;
    List* useful_pathkeys = NIL;
    bool found_clause = false;
    bool found_lower_saop_clause = false;
    bool pathkeys_possibly_useful = false;
    bool index_is_ordered = false;
    bool index_only_scan = false;
    int indexcol;
    bool can_parallel = IS_STREAM_PLAN && (u_sess->opt_cxt.query_dop > 1) && (ST_BITMAPSCAN != scantype) &&
                        (!rel->isPartitionedTable) && !index->rel->is_ustore;

    if (index->isAnnIndex && IsExtremeRedo()) {
        if (ST_BITMAPSCAN != scantype) {
            ereport(NOTICE, (errmsg("Ann Index does not support extreme RTO"),
                             errhint("This will show as Seq Scan")));
        }
        return NIL;
    }

    /*
     * Check that index supports the desired scan type(s)
     */
    switch (scantype) {
        case ST_INDEXSCAN:
            if (!index->amhasgettuple)
                return NIL;
            break;
        case ST_BITMAPSCAN:
            if (!index->amhasgetbitmap)
                return NIL;
            break;
        case ST_ANYSCAN:
            /* either or both are OK */
            break;
        default:
            break;
    }

    /*
     * 1. Collect the index clauses into a single list.
     *
     * We build a list of RestrictInfo nodes for clauses to be used with this
     * index, along with an integer list of the index column numbers (zero
     * based) that each clause should be used with.  The clauses are ordered
     * by index key, so that the column numbers form a nondecreasing sequence.
     * (This order is depended on by btree and possibly other places.)	The
     * lists can be empty, if the index AM allows that.
     *
     * found_clause is set true only if there's at least one index clause; and
     * if saop_control is SAOP_REQUIRE, it has to be a ScalarArrayOpExpr
     * clause.
     *
     * found_lower_saop_clause is set true if there's a ScalarArrayOpExpr
     * index clause for a non-first index column.  This prevents us from
     * assuming that the scan result is ordered.  (Actually, the result is
     * still ordered if there are equality constraints for all earlier
     * columns, but it seems too expensive and non-modular for this code to be
     * aware of that refinement.)
     *
     * We also build a Relids set showing which outer rels are required by the
     * selected clauses.
     */
    index_clauses = NIL;
    clause_columns = NIL;
    found_clause = false;
    found_lower_saop_clause = false;
    outer_relids = bms_copy(rel->lateral_relids);
    for (indexcol = 0; indexcol < index->nkeycolumns; indexcol++) {
        ListCell* lc = NULL;

        foreach (lc, clauses->indexclauses[indexcol]) {
            RestrictInfo* rinfo = (RestrictInfo*)lfirst(lc);

            if (IsA(rinfo->clause, ScalarArrayOpExpr)) {
                /* Ignore if not supported by index */
                if (saop_control == SAOP_PER_AM && !index->amsearcharray)
                    continue;
                found_clause = true;
                if (indexcol > 0)
                    found_lower_saop_clause = true;
            } else {
                if (saop_control != SAOP_REQUIRE)
                    found_clause = true;
            }
            index_clauses = lappend(index_clauses, rinfo);
            clause_columns = lappend_int(clause_columns, indexcol);
            outer_relids = bms_add_members(outer_relids, rinfo->clause_relids);
        }

        /*
         * If no clauses match the first index column, check for amoptionalkey
         * restriction.  We can't generate a scan over an index with
         * amoptionalkey = false unless there's at least one index clause.
         * (When working on columns after the first, this test cannot fail. It
         * is always okay for columns after the first to not have any
         * clauses.)
         */
        if (index_clauses == NIL && !index->amoptionalkey)
            return NIL;
    }

    /* We do not want the index's rel itself listed in outer_relids */
    outer_relids = bms_del_member(outer_relids, rel->relid);
    /* Enforce convention that outer_relids is exactly NULL if empty */
    if (bms_is_empty(outer_relids))
        outer_relids = NULL;

    /* Compute loop_count for cost estimation purposes */
    loop_count = get_loop_count(root, outer_relids);

    /*
     * 2. Compute pathkeys describing index's ordering, if any, then see how
     * many of them are actually useful for this query.  This is not relevant
     * if we are only trying to build bitmap indexscans, nor if we have to
     * assume the scan is unordered.
     */
    pathkeys_possibly_useful =
        (scantype != ST_BITMAPSCAN && !found_lower_saop_clause && has_useful_pathkeys(root, rel));
    index_is_ordered = (index->sortopfamily != NULL);
    if (index_is_ordered && pathkeys_possibly_useful) {
        index_pathkeys = build_index_pathkeys(root, index, ForwardScanDirection);
        useful_pathkeys = truncate_useless_pathkeys(root, rel, index_pathkeys);
        orderbyclauses = NIL;
        orderbyclausecols = NIL;
    } else if (index->amcanorderbyop && pathkeys_possibly_useful) {
        /* see if we can generate ordering operators for query_pathkeys */
        match_pathkeys_to_index(index, root->query_pathkeys, &orderbyclauses, &orderbyclausecols);
        if (orderbyclauses != NULL)
            useful_pathkeys = root->query_pathkeys;
        else
            useful_pathkeys = NIL;
    } else {
        useful_pathkeys = NIL;
        orderbyclauses = NIL;
        orderbyclausecols = NIL;
    }

    bool relHasbkt = false;
    if (u_sess->attr.attr_sql.enable_hypo_index == false && index_relation_has_bucket(index)) {
        relHasbkt = true;
    }
    /*
     * 3. Check if an index-only scan is possible.	If we're not building
     * plain indexscans, this isn't relevant since bitmap scans don't support
     * index data retrieval anyway.
     */
    index_only_scan = (scantype != ST_BITMAPSCAN && check_index_only(root, rel, index));

    /* get the upper param IDs */
    if (SUBQUERY_PREDPUSH(root)) {
        upper_params = collect_param_clause((Node*)index_clauses);
    }

    /*
     * 4. Generate an indexscan path if there are relevant restriction clauses
     * in the current clauses, OR the index ordering is potentially useful for
     * later merging or final output ordering, OR the index has a useful
     * predicate, OR an index-only scan is possible.
     */
    if (found_clause || useful_pathkeys != NIL || useful_predicate || index_only_scan) {
        if ((relHasbkt && !index->crossbucket)) {
            useful_pathkeys = NIL;
        }
        ipath = create_index_path(root,
            index,
            index_clauses,
            clause_columns,
            orderbyclauses,
            orderbyclausecols,
            useful_pathkeys,
            index_is_ordered ? ForwardScanDirection : NoMovementScanDirection,
            index_only_scan,
            outer_relids,
            upper_params,
            loop_count);
        result = lappend(result, ipath);
        if (can_parallel) {
            ipath = create_index_path(root,
                index,
                index_clauses,
                clause_columns,
                NIL,
                NIL,
                useful_pathkeys,
                index_is_ordered ? ForwardScanDirection : NoMovementScanDirection,
                index_only_scan,
                outer_relids,
                upper_params,
                loop_count,
                u_sess->opt_cxt.query_dop);
            result = lappend(result, ipath);
        }
    }

    /*
     * 5. If the index is ordered, a backwards scan might be interesting.
     */
    if (index_is_ordered && pathkeys_possibly_useful) {
        index_pathkeys = build_index_pathkeys(root, index, BackwardScanDirection);
        useful_pathkeys = truncate_useless_pathkeys(root, rel, index_pathkeys);
        if (useful_pathkeys != NIL) {
            if ((relHasbkt && !index->crossbucket)) {
                useful_pathkeys = NIL;
            }
            ipath = create_index_path(root,
                index,
                index_clauses,
                clause_columns,
                NIL,
                NIL,
                useful_pathkeys,
                BackwardScanDirection,
                index_only_scan,
                outer_relids,
                upper_params,
                loop_count);
            result = lappend(result, ipath);

            if (can_parallel) {
                ipath = create_index_path(root,
                    index,
                    index_clauses,
                    clause_columns,
                    NIL,
                    NIL,
                    useful_pathkeys,
                    BackwardScanDirection,
                    index_only_scan,
                    outer_relids,
                    upper_params,
                    loop_count,
                    u_sess->opt_cxt.query_dop);
                result = lappend(result, ipath);
            }
        }
    }

    /*
     * 6. Mark whether unique index fisrt rule satisfied in current btree index path. 
     * The rules will be used for selecting paths. We will check whether current index path 
     * contains a unique btree columns and the constraint conditions are equality constraints.
     */
    MarkUniqueIndexFirstRule(rel, index, result);

    return result;
}

/*
 * build_paths_for_OR
 *	  Given a list of restriction clauses from one arm of an OR clause,
 *	  construct all matching IndexPaths for the relation.
 *
 * Here we must scan all indexes of the relation, since a bitmap OR tree
 * can use multiple indexes.
 *
 * The caller actually supplies two lists of restriction clauses: some
 * "current" ones and some "other" ones.  Both lists can be used freely
 * to match keys of the index, but an index must use at least one of the
 * "current" clauses to be considered usable.  The motivation for this is
 * examples like
 *		WHERE (x = 42) AND (... OR (y = 52 AND z = 77) OR ....)
 * While we are considering the y/z subclause of the OR, we can use "x = 42"
 * as one of the available index conditions; but we shouldn't match the
 * subclause to any index on x alone, because such a Path would already have
 * been generated at the upper level.  So we could use an index on x,y,z
 * or an index on x,y for the OR subclause, but not an index on just x.
 * When dealing with a partial index, a match of the index predicate to
 * one of the "current" clauses also makes the index usable.
 *
 * 'rel' is the relation for which we want to generate index paths
 * 'clauses' is the current list of clauses (RestrictInfo nodes)
 * 'other_clauses' is the list of additional upper-level clauses
 */
static List* build_paths_for_OR(
    PlannerInfo* root, RelOptInfo* rel, List* clauses, List* other_clauses, IndexFeature idx_feature)
{
    List* result = NIL;
    List* all_clauses = NIL; /* not computed till needed */
    ListCell* lc = NULL;

    foreach (lc, rel->indexlist) {
        IndexOptInfo* index = (IndexOptInfo*)lfirst(lc);
        IndexClauseSet clauseset;
        List* indexpaths = NIL;
        bool useful_predicate = false;

        /* Ignore index if it doesn't support bitmap scans */
        if (!index->amhasgetbitmap)
            continue;

        /* Use indexes based on its feature */
        IndexFeature feature = getIndexFeature(index->isGlobal, index->crossbucket);
        if (feature != idx_feature) {
            continue;
        }

        /*
         * Build paths with global indexes only for un-bounded partition tables.
         * The partition bounded tables should be handled by partition iterator
         * or local indexes.
         */
        RangeTblEntry* rte = planner_rt_fetch(rel->relid, root);
        if (index->isGlobal && rte && list_length(rte->partitionOidList) > 0)  {
            continue;
        }

        /*
         * Ignore partial indexes that do not match the query.	If a partial
         * index is marked predOK then we know it's OK.  Otherwise, we have to
         * test whether the added clauses are sufficient to imply the
         * predicate. If so, we can use the index in the current context.
         *
         * We set useful_predicate to true iff the predicate was proven using
         * the current set of clauses.	This is needed to prevent matching a
         * predOK index to an arm of an OR, which would be a legal but
         * pointlessly inefficient plan.  (A better plan will be generated by
         * just scanning the predOK index alone, no OR.)
         */
        if (index->indpred != NIL) {
            if (index->predOK) {
                /* Usable, but don't set useful_predicate */
            } else {
                /* Form all_clauses if not done already */
                if (all_clauses == NIL)
                    all_clauses = list_concat(list_copy(clauses), other_clauses);

                if (!predicate_implied_by(index->indpred, all_clauses))
                    continue; /* can't use it at all */

                if (!predicate_implied_by(index->indpred, other_clauses))
                    useful_predicate = true;
            }
        }

        /*
         * Identify the restriction clauses that can match the index.
         */
        errno_t errorno = EOK;
        errorno = memset_s(&clauseset, sizeof(IndexClauseSet), 0, sizeof(clauseset));
        securec_check(errorno, "\0", "\0");
        match_clauses_to_index(index, clauses, &clauseset);

        /*
         * If no matches so far, and the index predicate isn't useful, we
         * don't want it.
         */
        if (!clauseset.nonempty && !useful_predicate)
            continue;

        /*
         * Add "other" restriction clauses to the clauseset.
         */
        match_clauses_to_index(index, other_clauses, &clauseset);

        /*
         * Construct paths if possible.
         */
        indexpaths = build_index_paths(root, rel, index, &clauseset, useful_predicate, SAOP_ALLOW, ST_BITMAPSCAN);
        result = list_concat(result, indexpaths);
    }

    return result;
}

/*
 * generate_bitmap_or_paths
 *		Look through the list of clauses to find OR clauses, and generate
 *		a BitmapOrPath for each one we can handle that way.  Return a list
 *		of the generated BitmapOrPaths.
 *
 * other_clauses is a list of additional clauses that can be assumed true
 * for the purpose of generating indexquals, but are not to be searched for
 * ORs.  (See build_paths_for_OR() for motivation.)
 *
 * If restriction_only is true, ignore OR elements that are join clauses.
 * When using this feature it is caller's responsibility that neither clauses
 * nor other_clauses contain any join clauses that are not ORs, as we do not
 * re-filter those lists.
 */
List* generate_bitmap_or_paths(
    PlannerInfo* root, RelOptInfo* rel, List* clauses, List* other_clauses, bool restriction_only)
{
    List* result = NIL;
    List* all_clauses = NIL;
    ListCell* lc = NULL;

    /*
     * We can use both the current and other clauses as context for
     * build_paths_for_OR; no need to remove ORs from the lists.
     */
    all_clauses = list_concat(list_copy(clauses), other_clauses);

    foreach (lc, clauses) {
        RestrictInfo* rinfo = (RestrictInfo*)lfirst(lc);
        List* pathlist = NIL;
        Path* bitmapqual = NULL;
        ListCell* j = NULL;

        AssertEreport(IsA(rinfo, RestrictInfo), MOD_OPT, "Restriction clause is incorrect");
        /* Ignore RestrictInfos that aren't ORs */
        if (!restriction_is_or_clause(rinfo))
            continue;

        /*
         * We must be able to match at least one index to each of the arms of
         * the OR, else we can't use it.
         */
        pathlist = NIL;
        foreach (j, ((BoolExpr*)rinfo->orclause)->args) {
            Node* orarg = (Node*)lfirst(j);
            List* indlist = NIL;
            List* globalIndexList = NIL;

            /* OR arguments should be ANDs or sub-RestrictInfos */
            if (and_clause(orarg)) {
                List* andargs = ((BoolExpr*)orarg)->args;

                if (restriction_only)
                    andargs = drop_indexable_join_clauses(rel, andargs);

                indlist = build_paths_for_OR(root, rel, andargs, all_clauses, NONFEATURED_INDEX);

                /* Recurse in case there are sub-ORs */
                indlist = list_concat(
                    indlist, generate_bitmap_or_paths(root, rel, andargs, all_clauses, restriction_only));

                /* If nothing matched this arm, we can't do anything with this OR clause */
                if (indlist == NIL) {
                    pathlist = NIL;
                    break;
                }

                IndexFeature indexFeature = getIndexFeature(rel->isPartitionedTable, (rel->bucketInfo != NULL));
                if (indexFeature != NONFEATURED_INDEX) {
                    globalIndexList = build_paths_for_OR(root, rel, andargs, all_clauses, indexFeature);
                    /* Recurse in case there are sub-ORs */
                    globalIndexList = list_concat(globalIndexList,
                        GenerateBitmapOrPathsWithFeaturedIndex(root, rel, andargs, all_clauses, restriction_only,
                            indexFeature));
                }
            } else {
                List* orargs = NIL;

                AssertEreport(IsA(orarg, RestrictInfo), MOD_OPT, "Restriction clause is incorrect");

                AssertEreport(restriction_is_or_clause((RestrictInfo*)orarg) == false,
                    MOD_OPT,
                    "Restriction clause does not contain OR");
                orargs = list_make1(orarg);

                if (restriction_only)
                    orargs = drop_indexable_join_clauses(rel, orargs);

                indlist = build_paths_for_OR(root, rel, orargs, all_clauses, NONFEATURED_INDEX);

                /* If nothing matched this arm, we can't do anything with this OR clause */
                if (indlist == NIL) {
                    pathlist = NIL;
                    break;
                }

                IndexFeature indexFeature = getIndexFeature(rel->isPartitionedTable, (rel->bucketInfo != NULL));
                if (indexFeature != NONFEATURED_INDEX) {
                    globalIndexList = build_paths_for_OR(root, rel, orargs, all_clauses, indexFeature);
                }
            }

            /*
             * OK, pick the most promising AND combination, and add it to
             * pathlist.
             */
            bitmapqual = choose_bitmap_and(root, rel, indlist, globalIndexList);
            pathlist = lappend(pathlist, bitmapqual);
        }

        /*
         * If we have a match for every arm, then turn them into a
         * BitmapOrPath, and add to result list.
         */
        if (pathlist != NIL) {
            bitmapqual = (Path*)create_bitmap_or_path(root, rel, pathlist);
            result = lappend(result, bitmapqual);
        }
    }

    return result;
}

/*
 * GenerateBitmapOrPathsWithFeaturedIndex
 * Generate BitmapOr paths With Special indexes,
 * Look through the list of clauses to find OR clauses, and generate
 * a BitmapOrPath for each one we can handle that way.  Return a list
 * of the generated BitmapOrPaths.
 *
 * other_clauses is a list of additional clauses that can be assumed true
 * for the purpose of generating indexquals, but are not to be searched for
 * ORs.  (See build_paths_for_OR() for motivation.)
 *
 * If restriction_only is true, ignore OR elements that are join clauses.
 * When using this feature it is caller's responsibility that neither clauses
 * nor other_clauses contain any join clauses that are not ORs, as we do not
 * re-filter those lists.
 *
 * Notes: For partition/hashbucket table and use featured index.
 */
List* GenerateBitmapOrPathsWithFeaturedIndex(PlannerInfo* root, RelOptInfo* rel, const List* clauses,
    List* other_clauses, bool restriction_only, IndexFeature idx_feature)
{
    List* result = NIL;
    List* all_clauses = NIL;
    ListCell* lc = NULL;

    /*
     * We can use both the current and other clauses as context for
     * build_paths_for_OR; no need to remove ORs from the lists.
     */
    all_clauses = list_concat(list_copy(clauses), other_clauses);

    foreach (lc, clauses) {
        RestrictInfo* rinfo = (RestrictInfo*)lfirst(lc);
        List* pathlist = NIL;
        Path* bitmapqual = NULL;
        ListCell* j = NULL;

        AssertEreport(IsA(rinfo, RestrictInfo), MOD_OPT, "Restriction clause is incorrect");
        /* Ignore RestrictInfos that aren't ORs */
        if (!restriction_is_or_clause(rinfo))
            continue;

        /*
         * We must be able to match at least one index to each of the arms of
         * the OR, else we can't use it.
         */
        pathlist = NIL;
        foreach (j, ((BoolExpr*)rinfo->orclause)->args) {
            Node* orarg = (Node*)lfirst(j);
            List* globalIndexList = NIL;

            /* OR arguments should be ANDs or sub-RestrictInfos */
            if (and_clause(orarg)) {
                List* andargs = ((BoolExpr*)orarg)->args;

                if (restriction_only)
                    andargs = drop_indexable_join_clauses(rel, andargs);

                globalIndexList = build_paths_for_OR(root, rel, andargs, all_clauses, idx_feature);
                /* Recurse in case there are sub-ORs */
                globalIndexList = list_concat(
                    globalIndexList,
                    GenerateBitmapOrPathsWithFeaturedIndex(root, rel, andargs, all_clauses, restriction_only,
                        idx_feature));
            } else {
                List* orargs = NIL;

                AssertEreport(IsA(orarg, RestrictInfo), MOD_OPT, "Restriction clause is incorrect");

                AssertEreport(restriction_is_or_clause((RestrictInfo*)orarg) == false,
                    MOD_OPT,
                    "Restriction clause does not contain OR");
                orargs = list_make1(orarg);

                if (restriction_only)
                    orargs = drop_indexable_join_clauses(rel, orargs);

                globalIndexList = build_paths_for_OR(root, rel, orargs, all_clauses, idx_feature);
            }

            /*
             * If nothing matched this arm, we can't do anything with this OR
             * clause.
             */
            if (globalIndexList == NIL) {
                pathlist = NIL;
                break;
            }

            /*
             * OK, pick the most promising AND combination, and add it to
             * pathlist.
             */
            bitmapqual = choose_bitmap_and(root, rel, globalIndexList, NIL);
            pathlist = lappend(pathlist, bitmapqual);
        }

        /*
         * If we have a match for every arm, then turn them into a
         * BitmapOrPath, and add to result list.
         */
        if (pathlist != NIL) {
            bitmapqual = (Path*)create_bitmap_or_path(root, rel, pathlist);
            result = lappend(result, bitmapqual);
        }
    }

    return result;
}

/*
 * drop_indexable_join_clauses
 *		Remove any indexable join clauses from the list.
 *
 * This is a helper for generate_bitmap_or_paths().  We leave OR clauses
 * in the list whether they are joins or not, since we might be able to
 * extract a restriction item from an OR list.	It's safe to leave such
 * clauses in the list because match_clauses_to_index() will ignore them,
 * so there's no harm in passing such clauses to build_paths_for_OR().
 */
static List* drop_indexable_join_clauses(RelOptInfo* rel, List* clauses)
{
    List* result = NIL;
    ListCell* lc = NULL;

    foreach (lc, clauses) {
        RestrictInfo* rinfo = (RestrictInfo*)lfirst(lc);

        AssertEreport(IsA(rinfo, RestrictInfo), MOD_OPT, "Restriction clause is incorrect");
        if (restriction_is_or_clause(rinfo) || bms_is_subset(rinfo->clause_relids, rel->relids))
            result = lappend(result, rinfo);
    }
    return result;
}

/*
 * As a heuristic, we first check for paths using exactly the same sets of
 * WHERE clauses + index predicate conditions, and reject all but the
 * cheapest-to-scan in any such group.	This primarily gets rid of indexes
 * that include the interesting columns but also irrelevant columns.  (In
 * situations where the DBA has gone overboard on creating variant
 * indexes, this can make for a very large reduction in the number of
 * paths considered further.)
 */
static PathClauseUsage** GetPathClauseUsage(List* paths, List* clauselist, int* npaths)
{
    int tmpPaths = list_length(paths);
    PathClauseUsage** pathinfoarray = NULL;
    PathClauseUsage* pathinfo = NULL;
    int i;
    ListCell* l = NULL;

    /* Input paths is NIL */
    if (tmpPaths == 0) {
        *npaths = 0;
        return NULL;
    }

    pathinfoarray = (PathClauseUsage**)palloc(tmpPaths * sizeof(PathClauseUsage*));
    tmpPaths = 0;
    foreach (l, paths) {
        Path* ipath = (Path*)lfirst(l);

        pathinfo = classify_index_clause_usage(ipath, &clauselist);
        for (i = 0; i < tmpPaths; i++) {
            if (bms_equal(pathinfo->clauseids, pathinfoarray[i]->clauseids))
                break;
        }
        if (i < tmpPaths) {
            /* duplicate clauseids, keep the cheaper one */
            Cost ncost;
            Cost ocost;
            Selectivity nselec;
            Selectivity oselec;

            cost_bitmap_tree_node(pathinfo->path, &ncost, &nselec);
            cost_bitmap_tree_node(pathinfoarray[i]->path, &ocost, &oselec);
            if (ncost < ocost)
                pathinfoarray[i] = pathinfo;
        } else {
            /* not duplicate clauseids, add to array */
            pathinfoarray[tmpPaths++] = pathinfo;
        }
    }

    *npaths = tmpPaths;

    return pathinfoarray;
}

/*
 * For each surviving index, consider it as an "AND group leader", and see
 * whether adding on any of the later indexes results in an AND path with
 * cheaper total cost than before. Then take the cheapest AND group.
 */
static void ChooseBitmapAndWithMultiIndex(
    PlannerInfo* root, RelOptInfo* rel, ChooseBitmapAndInfo* chooseInfo, PathClauseUsage** pathInfos, int npaths)
{
    PathClauseUsage* pathinfo = NULL;
    ListCell* l = NULL;

    for (int j = chooseInfo->startPath; j < npaths; j++) {
        Cost newcost;

        pathinfo = pathInfos[j];
        /* Check for redundancy */
        if (bms_overlap(pathinfo->clauseids, chooseInfo->clauseidsofar))
            continue; /* consider it redundant */
        if (pathinfo->preds != NIL) {
            bool redundant = false;

            /* we check each predicate clause separately */
            foreach (l, pathinfo->preds) {
                Node* np = (Node*)lfirst(l);

                if (predicate_implied_by(list_make1(np), chooseInfo->qualsofar)) {
                    redundant = true;
                    break; /* out of inner foreach loop */
                }
            }
            if (redundant)
                continue;
        }
        /* tentatively add new path to paths, so we can estimate cost */
        chooseInfo->paths = lappend(chooseInfo->paths, pathinfo->path);
        newcost = bitmap_and_cost_est(root, rel, chooseInfo->paths);
        if (newcost < chooseInfo->costsofar || u_sess->attr.attr_sql.force_bitmapand) {
            /* keep new path in paths, update subsidiary variables */
            chooseInfo->costsofar = newcost;
            chooseInfo->qualsofar = list_concat(chooseInfo->qualsofar, list_copy(pathinfo->quals));
            chooseInfo->qualsofar = list_concat(chooseInfo->qualsofar, list_copy(pathinfo->preds));
            chooseInfo->clauseidsofar = bms_add_members(chooseInfo->clauseidsofar, pathinfo->clauseids);
            chooseInfo->lastcell = lnext(chooseInfo->lastcell);
        } else {
            /* reject new path, remove it from paths list */
            chooseInfo->paths = list_delete_cell(chooseInfo->paths, lnext(chooseInfo->lastcell), chooseInfo->lastcell);
        }
        AssertEreport(lnext(chooseInfo->lastcell) == NULL, MOD_OPT, "Last cell is NULL");
    }
}

/*
 * choose_bitmap_and
 *		Given a nonempty list of bitmap paths, AND them into one path.
 *
 * This is a nontrivial decision since we can legally use any subset of the
 * given path set.	We want to choose a good tradeoff between selectivity
 * and cost of computing the bitmap.
 *
 * The result is either a single one of the inputs, or a BitmapAndPath
 * combining multiple inputs.
 */
static Path* choose_bitmap_and(PlannerInfo* root, RelOptInfo* rel, List* paths, List* globalPartIndexPaths)
{
    int npaths = list_length(paths);
    PathClauseUsage** pathinfoarray;
    PathClauseUsage* pathinfo = NULL;
    List* clauselist = NIL;
    List* bestpaths = NIL;
    Cost bestcost = 0;
    int i;
    int globalPartPaths = list_length(globalPartIndexPaths);
    PathClauseUsage** globalPathinfoarray;

    AssertEreport(npaths > 0, MOD_OPT, "Path number is incorrect");
    if (npaths == 1 && globalPartPaths == 0)
        return (Path*)linitial(paths); /* easy case */

    /*
     * In theory we should consider every nonempty subset of the given paths.
     * In practice that seems like overkill, given the crude nature of the
     * estimates, not to mention the possible effects of higher-level AND and
     * OR clauses.	Moreover, it's completely impractical if there are a large
     * number of paths, since the work would grow as O(2^N).
     *
     * As a heuristic, we first check for paths using exactly the same sets of
     * WHERE clauses + index predicate conditions, and reject all but the
     * cheapest-to-scan in any such group.	This primarily gets rid of indexes
     * that include the interesting columns but also irrelevant columns.  (In
     * situations where the DBA has gone overboard on creating variant
     * indexes, this can make for a very large reduction in the number of
     * paths considered further.)
     *
     * We then sort the surviving paths with the cheapest-to-scan first, and
     * for each path, consider using that path alone as the basis for a bitmap
     * scan.  Then we consider bitmap AND scans formed from that path plus
     * each subsequent (higher-cost) path, adding on a subsequent path if it
     * results in a reduction in the estimated total scan cost. This means we
     * consider about O(N^2) rather than O(2^N) path combinations, which is
     * quite tolerable, especially given than N is usually reasonably small
     * because of the prefiltering step.  The cheapest of these is returned.
     *
     * We will only consider AND combinations in which no two indexes use the
     * same WHERE clause.  This is a bit of a kluge: it's needed because
     * costsize.c and clausesel.c aren't very smart about redundant clauses.
     * They will usually double-count the redundant clauses, producing a
     * too-small selectivity that makes a redundant AND step look like it
     * reduces the total cost.	Perhaps someday that code will be smarter and
     * we can remove this limitation.  (But note that this also defends
     * against flat-out duplicate input paths, which can happen because
     * match_join_clauses_to_index will find the same OR join clauses that
     * extract_restriction_or_clauses has pulled OR restriction clauses out of.)
     *
     * For the same reason, we reject AND combinations in which an index
     * predicate clause duplicates another clause.	Here we find it necessary
     * to be even stricter: we'll reject a partial index if any of its
     * predicate clauses are implied by the set of WHERE clauses and predicate
     * clauses used so far.  This covers cases such as a condition "x = 42"
     * used with a plain index, followed by a clauseless scan of a partial
     * index "WHERE x >= 40 AND x < 50".  The partial index has been accepted
     * only because "x = 42" was present, and so allowing it would partially
     * double-count selectivity.  (We could use predicate_implied_by on
     * regular qual clauses too, to have a more intelligent, but much more
     * expensive, check for redundancy --- but in most cases simple equality
     * seems to suffice.)
     */
    /*
     * Extract clause usage info and detect any paths that use exactly the
     * same set of clauses; keep only the cheapest-to-scan of any such groups.
     * The surviving paths are put into an array for qsort'ing.
     */
    pathinfoarray = GetPathClauseUsage(paths, clauselist, &npaths);;

    /* Global part index path and local part index path use same clauselist */
    globalPathinfoarray = GetPathClauseUsage(globalPartIndexPaths, clauselist, &globalPartPaths);

    /* If only one surviving path, we're done */
    if (npaths == 1 && globalPartPaths == 0)
        return pathinfoarray[0]->path;

    /* Sort the surviving paths by index access cost */
    qsort(pathinfoarray, npaths, sizeof(PathClauseUsage*), path_usage_comparator);
    /*
     * For each surviving index, consider it as an "AND group leader", and see
     * whether adding on any of the later indexes results in an AND path with
     * cheaper total cost than before.	Then take the cheapest AND group.
     */
    for (i = 0; i < npaths; i++) {
        ChooseBitmapAndInfo chooseInfo;
        
        pathinfo = pathinfoarray[i];
        chooseInfo.paths = list_make1(pathinfo->path);
        chooseInfo.costsofar = bitmap_scan_cost_est(root, rel, pathinfo->path);
        chooseInfo.qualsofar = list_concat(list_copy(pathinfo->quals), list_copy(pathinfo->preds));
        chooseInfo.clauseidsofar = bms_copy(pathinfo->clauseids);
        chooseInfo.startPath = i + 1;
        chooseInfo.lastcell = list_head(chooseInfo.paths); /* for quick deletions */

        ChooseBitmapAndWithMultiIndex(root, rel, &chooseInfo, pathinfoarray, npaths);

        /*
         * The local partition index and global partition index form bitmapAnd,
         * the final result is the local partition index.
         *
         * Notes: For global partition index, the start judgment point is 0.
         */
        if (globalPartPaths > 0) {
            chooseInfo.startPath = 0;
            ChooseBitmapAndWithMultiIndex(root, rel, &chooseInfo, globalPathinfoarray, globalPartPaths);
        }

        /* Keep the cheapest AND-group (or singleton) */
        if (i == 0 || chooseInfo.costsofar < bestcost) {
            bestpaths = chooseInfo.paths;
            bestcost = chooseInfo.costsofar;
        }

        /* some easy cleanup (we don't try real hard though) */
        list_free_ext(chooseInfo.qualsofar);

        if (u_sess->attr.attr_sql.force_bitmapand)
            break;
    }

    if (list_length(bestpaths) == 1)
        return (Path*)linitial(bestpaths); /* no need for AND */
    return (Path*)create_bitmap_and_path(root, rel, bestpaths);
}

/* qsort comparator to sort in increasing index access cost order */
static int path_usage_comparator(const void* a, const void* b)
{
    PathClauseUsage* pa = *(PathClauseUsage* const*)a;
    PathClauseUsage* pb = *(PathClauseUsage* const*)b;
    Cost acost;
    Cost bcost;
    Selectivity aselec;
    Selectivity bselec;

    cost_bitmap_tree_node(pa->path, &acost, &aselec);
    cost_bitmap_tree_node(pb->path, &bcost, &bselec);

    /*
     * If costs are the same, sort by selectivity.
     */
    if (acost < bcost)
        return -1;
    if (acost > bcost)
        return 1;

    if (aselec < bselec)
        return -1;
    if (aselec > bselec)
        return 1;

    return 0;
}

/*
 * Estimate the cost of actually executing a bitmap scan with a single
 * index path (no BitmapAnd, at least not at this level; but it could be
 * a BitmapOr).
 */
static Cost bitmap_scan_cost_est(PlannerInfo* root, RelOptInfo* rel, Path* ipath)
{
    BitmapHeapPath bpath;
    Relids required_outer;

    /* Identify required outer rels, in case it's a parameterized scan */
    required_outer = get_bitmap_tree_required_outer(ipath);

    /* Set up a dummy BitmapHeapPath */
    bpath.path.type = T_BitmapHeapPath;
    bpath.path.pathtype = T_BitmapHeapScan;
    bpath.path.parent = rel;
    bpath.path.pathtarget = rel->reltarget;
    bpath.path.param_info = get_baserel_parampathinfo(root, rel, required_outer);
    bpath.path.pathkeys = NIL;
    bpath.bitmapqual = ipath;

    cost_bitmap_heap_scan(&bpath.path, root, rel, bpath.path.param_info, ipath, get_loop_count(root, required_outer));

    return bpath.path.total_cost;
}

/*
 * Estimate the cost of actually executing a BitmapAnd scan with the given
 * inputs.
 */
static Cost bitmap_and_cost_est(PlannerInfo* root, RelOptInfo* rel, List* paths)
{
    BitmapAndPath apath;
    BitmapHeapPath bpath;
    Relids required_outer;

    /* Set up a dummy BitmapAndPath */
    apath.path.type = T_BitmapAndPath;
    apath.path.pathtype = T_BitmapAnd;
    apath.path.parent = rel;
    bpath.path.pathtarget = rel->reltarget;
    apath.path.param_info = NULL; /* not used in bitmap trees */
    apath.path.pathkeys = NIL;
    apath.bitmapquals = paths;
    cost_bitmap_and_node(&apath, root);

    /* Identify required outer rels, in case it's a parameterized scan */
    required_outer = get_bitmap_tree_required_outer((Path*)&apath);

    /* Set up a dummy BitmapHeapPath */
    bpath.path.type = T_BitmapHeapPath;
    bpath.path.pathtype = T_BitmapHeapScan;
    bpath.path.parent = rel;
    bpath.path.pathtarget = rel->reltarget;
    bpath.path.param_info = get_baserel_parampathinfo(root, rel, required_outer);
    bpath.path.pathkeys = NIL;
    bpath.bitmapqual = (Path*)&apath;

    /* Now we can do cost_bitmap_heap_scan */
    cost_bitmap_heap_scan(
        &bpath.path, root, rel, bpath.path.param_info, (Path*)&apath, get_loop_count(root, required_outer));

    return bpath.path.total_cost;
}

/*
 * classify_index_clause_usage
 *		Construct a PathClauseUsage struct describing the WHERE clauses and
 *		index predicate clauses used by the given indexscan path.
 *		We consider two clauses the same if they are equal().
 *
 * At some point we might want to migrate this info into the Path data
 * structure proper, but for the moment it's only needed
 * within choose_bitmap_and().
 *
 * *clauselist is used and expanded as needed to identify all the distinct
 * clauses seen across successive calls.  Caller must initialize it to NIL
 * before first call of a set.
 */
static PathClauseUsage* classify_index_clause_usage(Path* path, List** clauselist)
{
    PathClauseUsage* result = NULL;
    Bitmapset* clauseids = NULL;
    ListCell* lc = NULL;

    result = (PathClauseUsage*)palloc(sizeof(PathClauseUsage));
    result->path = path;

    /* Recursively find the quals and preds used by the path */
    result->quals = NIL;
    result->preds = NIL;
    find_indexpath_quals(path, &result->quals, &result->preds);

    /* Build up a bitmapset representing the quals and preds */
    clauseids = NULL;
    foreach (lc, result->quals) {
        Node* node = (Node*)lfirst(lc);

        clauseids = bms_add_member(clauseids, find_list_position(node, clauselist));
    }
    foreach (lc, result->preds) {
        Node* node = (Node*)lfirst(lc);

        clauseids = bms_add_member(clauseids, find_list_position(node, clauselist));
    }
    result->clauseids = clauseids;

    return result;
}

/*
 * get_bitmap_tree_required_outer
 *		Find the required outer rels for a bitmap tree (index/and/or)
 *
 * We don't associate any particular parameterization with a BitmapAnd or
 * BitmapOr node; however, the IndexPaths have parameterization info, in
 * their capacity as standalone access paths.  The parameterization required
 * for the bitmap heap scan node is the union of rels referenced in the
 * child IndexPaths.
 */
static Relids get_bitmap_tree_required_outer(Path* bitmapqual)
{
    Relids result = NULL;
    ListCell* lc = NULL;

    if (IsA(bitmapqual, IndexPath)) {
        return bms_copy(PATH_REQ_OUTER(bitmapqual));
    } else if (IsA(bitmapqual, BitmapAndPath)) {
        foreach (lc, ((BitmapAndPath*)bitmapqual)->bitmapquals) {
            result = bms_join(result, get_bitmap_tree_required_outer((Path*)lfirst(lc)));
        }
    } else if (IsA(bitmapqual, BitmapOrPath)) {
        foreach (lc, ((BitmapOrPath*)bitmapqual)->bitmapquals) {
            result = bms_join(result, get_bitmap_tree_required_outer((Path*)lfirst(lc)));
        }
    } else {
        ereport(ERROR,
            (errmodule(MOD_OPT),
                errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                errmsg("unrecognized node type when find the required outer rels for a bitmap tree: %d",
                    nodeTag(bitmapqual))));
    }

    return result;
}

/*
 * get_bitmap_tree_required_upper
 *		Find the required outer rels which is outside of this query block
 */
static Bitmapset* get_bitmap_tree_required_upper(Path* bitmapqual)
{
    Bitmapset* result = NULL;
    ListCell* lc = NULL;

    if (IsA(bitmapqual, IndexPath)) {
        return bms_copy(PATH_REQ_UPPER(bitmapqual));
    } else if (IsA(bitmapqual, BitmapAndPath)) {
        foreach (lc, ((BitmapAndPath*)bitmapqual)->bitmapquals) {
            result = bms_join(result, get_bitmap_tree_required_upper((Path*)lfirst(lc)));
        }
    } else if (IsA(bitmapqual, BitmapOrPath)) {
        foreach (lc, ((BitmapOrPath*)bitmapqual)->bitmapquals) {
            result = bms_join(result, get_bitmap_tree_required_upper((Path*)lfirst(lc)));
        }
    } else {
        ereport(ERROR,
            (errmodule(MOD_OPT),
                errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                errmsg("unrecognized node type when find the required upper rels for a bitmap tree: %d",
                    nodeTag(bitmapqual))));
    }

    return result;
}

/*
 * find_indexpath_quals
 *
 * Given the Path structure for a plain or bitmap indexscan, extract lists
 * of all the indexquals and index predicate conditions used in the Path.
 * These are appended to the initial contents of *quals and *preds (hence
 * caller should initialize those to NIL).
 *
 * This is sort of a simplified version of make_restrictinfo_from_bitmapqual;
 * here, we are not trying to produce an accurate representation of the AND/OR
 * semantics of the Path, but just find out all the base conditions used.
 *
 * The result lists contain pointers to the expressions used in the Path,
 * but all the list cells are freshly built, so it's safe to destructively
 * modify the lists (eg, by concat'ing with other lists).
 */
static void find_indexpath_quals(Path* bitmapqual, List** quals, List** preds)
{
    if (IsA(bitmapqual, BitmapAndPath)) {
        BitmapAndPath* apath = (BitmapAndPath*)bitmapqual;
        ListCell* l = NULL;

        foreach (l, apath->bitmapquals) {
            find_indexpath_quals((Path*)lfirst(l), quals, preds);
        }
    } else if (IsA(bitmapqual, BitmapOrPath)) {
        BitmapOrPath* opath = (BitmapOrPath*)bitmapqual;
        ListCell* l = NULL;

        foreach (l, opath->bitmapquals) {
            find_indexpath_quals((Path*)lfirst(l), quals, preds);
        }
    } else if (IsA(bitmapqual, IndexPath)) {
        IndexPath* ipath = (IndexPath*)bitmapqual;

        *quals = list_concat(*quals, get_actual_clauses(ipath->indexclauses));
        *preds = list_concat(*preds, list_copy(ipath->indexinfo->indpred));
    } else {
        ereport(ERROR,
            (errmodule(MOD_OPT),
                errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                errmsg("unrecognized node type when find indexpath quals: %d", nodeTag(bitmapqual))));
    }
}

/*
 * find_list_position
 *		Return the given node's position (counting from 0) in the given
 *		list of nodes.	If it's not equal() to any existing list member,
 *		add it at the end, and return that position.
 */
static int find_list_position(Node* node, List** nodelist)
{
    int i;
    ListCell* lc = NULL;

    i = 0;
    foreach (lc, *nodelist) {
        Node* oldnode = (Node*)lfirst(lc);

        if (equal(node, oldnode))
            return i;
        i++;
    }

    *nodelist = lappend(*nodelist, node);

    return i;
}

/*
 * check_index_only
 *		Determine whether an index-only scan is possible for this index.
 */
static bool check_index_only(PlannerInfo* root, RelOptInfo* rel, IndexOptInfo* index)
{
    bool result = false;
    Bitmapset* attrs_used = NULL;
    Bitmapset* index_attrs = NULL;
    ListCell* lc = NULL;
    int i;
    List* hint = NIL;

    /* Index-only scans must be enabled, and index must be capable of them */
    hint = find_specific_scan_hint(root->parse->hintState, rel->relids, HINT_KEYWORD_INDEXONLYSCAN);
    if (!u_sess->attr.attr_sql.enable_indexonlyscan && hint == NIL)
        return false;
    list_free_ext(hint);
    if (!index->canreturn)
        return false;

    /*
     * Check that all needed attributes of the relation are available from the
     * index.
     *
     * XXX this is overly conservative for partial indexes, since we will
     * consider attributes involved in the index predicate as required even
     * though the predicate won't need to be checked at runtime.  (The same is
     * true for attributes used only in index quals, if we are certain that
     * the index is not lossy.)  However, it would be quite expensive to
     * determine that accurately at this point, so for now we take the easy
     * way out.
     */
    /*
     * Add all the attributes needed for joins or final output.  Note: we must
     * look at rel's targetlist, not the attr_needed data, because attr_needed
     * isn't computed for inheritance child rels.
     */
    pull_varattnos((Node*)rel->reltarget->exprs, rel->relid, &attrs_used);

    /* Add all the attributes used by restriction clauses. */
    foreach (lc, rel->baserestrictinfo) {
        RestrictInfo* rinfo = (RestrictInfo*)lfirst(lc);

        pull_varattnos((Node*)rinfo->clause, rel->relid, &attrs_used);
    }

    /* Construct a bitmapset of columns stored in the index. */
    for (i = 0; i < index->ncolumns; i++) {
        int attno = index->indexkeys[i];

        /*
         * For the moment, we just ignore index expressions.  It might be nice
         * to do something with them, later.
         */
        if (attno == 0)
            continue;

        index_attrs = bms_add_member(index_attrs, attno - FirstLowInvalidHeapAttributeNumber);
    }

    /* Do we have all the necessary attributes? */
    result = bms_is_subset(attrs_used, index_attrs);

    bms_free_ext(attrs_used);
    bms_free_ext(index_attrs);

    return result;
}

/*
 * get_loop_count
 *		Choose the loop count estimate to use for costing a parameterized path
 *		with the given set of outer relids.
 *
 * Since we produce parameterized paths before we've begun to generate join
 * relations, it's impossible to predict exactly how many times a parameterized
 * path will be iterated; we don't know the size of the relation that will be
 * on the outside of the nestloop.	However, we should try to account for
 * multiple iterations somehow in costing the path.  The heuristic embodied
 * here is to use the rowcount of the smallest other base relation needed in
 * the join clauses used by the path.  (We could alternatively consider the
 * largest one, but that seems too optimistic.)  This is of course the right
 * answer for single-other-relation cases, and it seems like a reasonable
 * zero-order approximation for multiway-join cases.
 *
 * Note: for this to work, allpaths.c must establish all baserel size
 * estimates before it begins to compute paths, or at least before it
 * calls create_index_paths().
 */
static double get_loop_count(PlannerInfo* root, Relids outer_relids)
{
    double result = 1.0;

    /* For a non-parameterized path, just return 1.0 quickly */
    if (outer_relids != NULL) {
        int relid;

        /* Need a working copy since bms_first_member is destructive */
        outer_relids = bms_copy(outer_relids);
        while ((relid = bms_first_member(outer_relids)) >= 0) {
            RelOptInfo* outer_rel = NULL;

            /* Paranoia: ignore bogus relid indexes */
            if (relid >= root->simple_rel_array_size)
                continue;
            outer_rel = root->simple_rel_array[relid];
            if (outer_rel == NULL)
                continue;
            AssertEreport(outer_rel->relid == (uint)relid, MOD_OPT_JOIN, "Outer relid is inconsistent");

            /* Other relation could be proven empty, if so ignore */
            if (IS_DUMMY_REL(outer_rel))
                continue;

            /* deal with it special for partitioned table because of its logical */
            if (!outer_rel->isPartitionedTable) {
                /* Otherwise, rel's rows estimate should be valid by now */
                AssertEreport(outer_rel->rows > 0, MOD_OPT_JOIN, "Outer rel row count is incorrect");
            }

            /* Remember smallest row count estimate among the outer rels */
            if (result == 1.0 || result > RELOPTINFO_LOCAL_FIELD(root, outer_rel, rows))
                result = RELOPTINFO_LOCAL_FIELD(root, outer_rel, rows);
        }
        bms_free_ext(outer_relids);
    }
    return result;
}

/****************************************************************************
 *				----  ROUTINES TO CHECK QUERY CLAUSES  ----
 ****************************************************************************/
/*
 * match_restriction_clauses_to_index
 *	  Identify restriction clauses for the rel that match the index.
 *	  Matching clauses are added to *clauseset.
 */
static void match_restriction_clauses_to_index(RelOptInfo* rel, IndexOptInfo* index, IndexClauseSet* clauseset)
{
    match_clauses_to_index(index, rel->baserestrictinfo, clauseset);
}

/*
 * match_join_clauses_to_index
 *	  Identify join clauses for the rel that match the index.
 *	  Matching clauses are added to *clauseset.
 *	  Also, add any potentially usable join OR clauses to *joinorclauses.
 */
static void match_join_clauses_to_index(
    PlannerInfo* root, RelOptInfo* rel, IndexOptInfo* index,
    IndexClauseSet* clauseset, List** joinorclauses)
{
    ListCell* lc = NULL;

    /* Scan the rel's join clauses */
    foreach (lc, rel->joininfo) {
        RestrictInfo* rinfo = (RestrictInfo*)lfirst(lc);

        /* Check if clause can be moved to this rel */
        if (!join_clause_is_movable_to(rinfo, rel))
            continue;

        /* Potentially usable, so see if it matches the index or is an OR */
        if (restriction_is_or_clause(rinfo))
            *joinorclauses = lappend(*joinorclauses, rinfo);
        else
            match_clause_to_index(index, rinfo, clauseset);
    }
}

/*
 * match_eclass_clauses_to_index
 *	  Identify EquivalenceClass join clauses for the rel that match the index.
 *	  Matching clauses are added to *clauseset.
 */
static void match_eclass_clauses_to_index(PlannerInfo* root, IndexOptInfo* index, IndexClauseSet* clauseset)
{
    int indexcol;

    /* No work if rel is not in any such ECs */
    if (!index->rel->has_eclass_joins)
        return;

    for (indexcol = 0; indexcol < index->nkeycolumns; indexcol++) {
        List* clauses = NIL;

        /* Generate clauses, skipping any that join to lateral_referencers */
        clauses = generate_implied_equalities_for_indexcol(root, index, indexcol, index->rel->lateral_referencers);

        /*
         * We have to check whether the results actually do match the index,
         * since for non-btree indexes the EC's equality operators might not
         * be in the index opclass (cf eclass_member_matches_indexcol).
         */
        match_clauses_to_index(index, clauses, clauseset);
    }
}

/*
 * match_clauses_to_index
 *	  Perform match_clause_to_index() for each clause in a list.
 *	  Matching clauses are added to *clauseset.
 */
static void match_clauses_to_index(IndexOptInfo* index, List* clauses, IndexClauseSet* clauseset)
{
    ListCell* lc = NULL;

    foreach (lc, clauses) {
        RestrictInfo* rinfo = (RestrictInfo*)lfirst(lc);

        AssertEreport(IsA(rinfo, RestrictInfo), MOD_OPT, "Restriction clause is incorrect");
        match_clause_to_index(index, rinfo, clauseset);
    }
}

/*
 * match_clause_to_index
 *	  Test whether a qual clause can be used with an index.
 *
 * If the clause is usable, add it to the appropriate list in *clauseset.
 * *clauseset must be initialized to zeroes before first call.
 *
 * Note: in some circumstances we may find the same RestrictInfos coming from
 * multiple places.  Defend against redundant outputs by refusing to add a
 * clause twice (pointer equality should be a good enough check for this).
 *
 * Note: it's possible that a badly-defined index could have multiple matching
 * columns.  We always select the first match if so; this avoids scenarios
 * wherein we get an inflated idea of the index's selectivity by using the
 * same clause multiple times with different index columns.
 */
static void match_clause_to_index(IndexOptInfo* index, RestrictInfo* rinfo, IndexClauseSet* clauseset)
{
    int indexcol;

    /*
     * Never match pseudoconstants to indexes.  (Normally a match could not
     * happen anyway, since a pseudoconstant clause couldn't contain a Var,
     * but what if someone builds an expression index on a constant? It's not
     * totally unreasonable to do so with a partial index, either.)
     */
    if (rinfo->pseudoconstant)
        return;

    /*
     * If clause can't be used as an indexqual because it must wait till after
     * some lower-security-level restriction clause, reject it.
     */
    if (!restriction_is_securely_promotable(rinfo, index->rel))
        return;

    /*
     * The rel is column store but index qual not supported by vec engine
     */
    if (index->rel->orientation == REL_COL_ORIENTED &&
        vector_engine_unsupport_expression_walker((Node *)rinfo->clause)) {
        return;
    }

    /* OK, check each index column for a match */
    for (indexcol = 0; indexcol < index->nkeycolumns; indexcol++) {
        if (match_clause_to_indexcol(index, indexcol, rinfo)) {
            clauseset->indexclauses[indexcol] = list_append_unique_ptr(clauseset->indexclauses[indexcol], rinfo);
            clauseset->nonempty = true;
            return;
        }
    }
}

/*
 * match_clause_to_indexcol
 *
 *	  Determines whether a restriction clause matches a column of an index.
 *
 *	  To match an index normally, the clause:
 *
 *	  (1)  must be in the form (indexkey op const) or (const op indexkey);
 *		   and
 *	  (2)  must contain an operator which is in the same family as the index
 *		   operator for this column, or is a "special" operator as recognized
 *		   by match_special_index_operator();
 *		   and
 *	  (3)  must match the collation of the index, if collation is relevant.
 *
 *	  Our definition of "const" is exceedingly liberal: we allow anything that
 *	  doesn't involve a volatile function or a Var of the index's relation.
 *	  In particular, Vars belonging to other relations of the query are
 *	  accepted here, since a clause of that form can be used in a
 *	  parameterized indexscan.	It's the responsibility of higher code levels
 *	  to manage restriction and join clauses appropriately.
 *
 *	  Note: we do need to check for Vars of the index's relation on the
 *	  "const" side of the clause, since clauses like (a.f1 OP (b.f2 OP a.f3))
 *	  are not processable by a parameterized indexscan on a.f1, whereas
 *	  something like (a.f1 OP (b.f2 OP c.f3)) is.
 *
 *	  Presently, the executor can only deal with indexquals that have the
 *	  indexkey on the left, so we can only use clauses that have the indexkey
 *	  on the right if we can commute the clause to put the key on the left.
 *	  We do not actually do the commuting here, but we check whether a
 *	  suitable commutator operator is available.
 *
 *	  If the index has a collation, the clause must have the same collation.
 *	  For collation-less indexes, we assume it doesn't matter; this is
 *	  necessary for cases like "hstore ? text", wherein hstore's operators
 *	  don't care about collation but the clause will get marked with a
 *	  collation anyway because of the text argument.  (This logic is
 *	  embodied in the macro IndexCollMatchesExprColl.)
 *
 *	  It is also possible to match RowCompareExpr clauses to indexes (but
 *	  currently, only btree indexes handle this).  In this routine we will
 *	  report a match if the first column of the row comparison matches the
 *	  target index column.	This is sufficient to guarantee that some index
 *	  condition can be constructed from the RowCompareExpr --- whether the
 *	  remaining columns match the index too is considered in
 *	  adjust_rowcompare_for_index.
 *
 *	  It is also possible to match ScalarArrayOpExpr clauses to indexes, when
 *	  the clause is of the form "indexkey op ANY (arrayconst)".
 *
 *	  For boolean indexes, it is also possible to match the clause directly
 *	  to the indexkey; or perhaps the clause is (NOT indexkey).
 *
 * 'index' is the index of interest.
 * 'indexcol' is a column number of 'index' (counting from 0).
 * 'rinfo' is the clause to be tested (as a RestrictInfo node).
 *
 * Returns true if the clause can be used with this index key.
 *
 * NOTE:  returns false if clause is an OR or AND clause; it is the
 * responsibility of higher-level routines to cope with those.
 */
static bool match_clause_to_indexcol(IndexOptInfo* index, int indexcol, RestrictInfo* rinfo)
{
    Expr* clause = rinfo->clause;
    Index index_relid = index->rel->relid;
    Oid opfamily;
    Oid idxcollation;
    Node* leftop = NULL;
    Node* rightop = NULL;
    Relids left_relids;
    Relids right_relids;
    Oid expr_op;
    Oid expr_coll;
    bool plain_op = false;
    bool isMatchPrefixKey = true;

    Assert(indexcol < index->nkeycolumns);
    opfamily = index->opfamily[indexcol];
    idxcollation = index->indexcollations[indexcol];
    /*
     * Never match pseudoconstants to indexes.	(Normally this could not
     * happen anyway, since a pseudoconstant clause couldn't contain a Var,
     * but what if someone builds an expression index on a constant? It's not
     * totally unreasonable to do so with a partial index, either.)
     */
    if (rinfo->pseudoconstant)
        return false;

    /* First check for boolean-index cases. */
    if (IsBooleanOpfamily(opfamily)) {
        if (match_boolean_index_clause((Node*)clause, indexcol, index))
            return true;
    }

    /*
     * Clause must be a binary opclause, or possibly a ScalarArrayOpExpr
     * (which is always binary, by definition).  Or it could be a
     * RowCompareExpr, which we pass off to match_rowcompare_to_indexcol().
     * Or, if the index supports it, we can handle IS NULL/NOT NULL clauses.
     */
    if (is_opclause(clause)) {
        leftop = get_leftop(clause);
        rightop = get_rightop(clause);
        if (leftop == NULL || rightop == NULL)
            return false;
        left_relids = rinfo->left_relids;
        right_relids = rinfo->right_relids;
        expr_op = ((OpExpr*)clause)->opno;
        expr_coll = ((OpExpr*)clause)->inputcollid;
        plain_op = true;
    } else if (clause && IsA(clause, ScalarArrayOpExpr)) {
        ScalarArrayOpExpr* saop = (ScalarArrayOpExpr*)clause;

        /* We only accept ANY clauses, not ALL */
        if (!saop->useOr)
            return false;
        leftop = (Node*)linitial(saop->args);
        rightop = (Node*)lsecond(saop->args);
        left_relids = NULL; /* not actually needed */
        right_relids = pull_varnos(rightop);
        expr_op = saop->opno;
        expr_coll = saop->inputcollid;
        plain_op = false;
        isMatchPrefixKey = scalar_array_can_match_prefixkey(rightop);
    } else if (clause && IsA(clause, RowCompareExpr)) {
        return match_rowcompare_to_indexcol(index, indexcol, opfamily, idxcollation, (RowCompareExpr*)clause);
    } else if (index->amsearchnulls && IsA(clause, NullTest)) {
        NullTest* nt = (NullTest*)clause;

        if (!nt->argisrow && match_index_to_operand((Node*)nt->arg, indexcol, index, true))
            return true;
        return false;
    } else
        return false;

    /*
     * Check for clauses of the form: (indexkey operator constant) or
     * (constant operator indexkey).  See above notes about const-ness.
     */
    if (match_index_to_operand(leftop, indexcol, index, isMatchPrefixKey) &&
        !bms_is_member(index_relid, right_relids) && !contain_volatile_functions(rightop)) {
        if (IndexCollMatchesExprColl(idxcollation, expr_coll) && is_indexable_operator(expr_op, opfamily, true))
            return true;

        /*
         * If we didn't find a member of the index's opfamily, see whether it
         * is a "special" indexable operator.
         */
        if (plain_op && match_special_index_operator(clause, opfamily, idxcollation, true, index, indexcol) &&
            (IndexCollMatchesExprColl(idxcollation, expr_coll) ||
                (!COLLATION_IN_B_FORMAT(idxcollation) && !COLLATION_IN_B_FORMAT(expr_coll)))) {
            return true;
        }
        return false;
    }

    if (plain_op && match_index_to_operand(rightop, indexcol, index, isMatchPrefixKey) &&
        !bms_is_member(index_relid, left_relids) && !contain_volatile_functions(leftop)) {
        if (IndexCollMatchesExprColl(idxcollation, expr_coll) && is_indexable_operator(expr_op, opfamily, false))
            return true;

        /*
         * If we didn't find a member of the index's opfamily, see whether it
         * is a "special" indexable operator.
         */
        if (match_special_index_operator(clause, opfamily, idxcollation, false, index, indexcol) &&
            (IndexCollMatchesExprColl(idxcollation, expr_coll) ||
                (!COLLATION_IN_B_FORMAT(idxcollation) && !COLLATION_IN_B_FORMAT(expr_coll)))) {
            return true;
        }
        return false;
    }

    return false;
}

/*
 * is_indexable_operator
 *	  Does the operator match the specified index opfamily?
 *
 * If the indexkey is on the right, what we actually want to know
 * is whether the operator has a commutator operator that matches
 * the opfamily.
 */
static bool is_indexable_operator(Oid expr_op, Oid opfamily, bool indexkey_on_left)
{
    /* Get the commuted operator if necessary */
    if (!indexkey_on_left) {
        expr_op = get_commutator(expr_op);
        if (expr_op == InvalidOid)
            return false;
    }
    if (expr_op == INT4EQOID && opfamily == INTEGER_BTREE_FAM_OID) {
        return true;
    }
    /* OK if the (commuted) operator is a member of the index's opfamily */
    return op_in_opfamily(expr_op, opfamily);
}

/*
 * match_rowcompare_to_indexcol
 *
 *	  Handles the RowCompareExpr case for match_clause_to_indexcol(),
 *	  which see for comments.
 */
static bool match_rowcompare_to_indexcol(
    IndexOptInfo* index, int indexcol, Oid opfamily, Oid idxcollation, RowCompareExpr* clause)
{
    Index index_relid = index->rel->relid;
    Node* leftop = NULL;
    Node* rightop = NULL;
    Oid expr_op;
    Oid expr_coll;

    /* Forget it if we're not dealing with a btree index */
    if (!OID_IS_BTREE(index->relam))
        return false;

    /*
     * We could do the matching on the basis of insisting that the opfamily
     * shown in the RowCompareExpr be the same as the index column's opfamily,
     * but that could fail in the presence of reverse-sort opfamilies: it'd be
     * a matter of chance whether RowCompareExpr had picked the forward or
     * reverse-sort family.  So look only at the operator, and match if it is
     * a member of the index's opfamily (after commutation, if the indexkey is
     * on the right).  We'll worry later about whether any additional
     * operators are matchable to the index.
     */
    leftop = (Node*)linitial(clause->largs);
    rightop = (Node*)linitial(clause->rargs);
    expr_op = linitial_oid(clause->opnos);
    expr_coll = linitial_oid(clause->inputcollids);

    /* Collations must match, if relevant */
    if (!IndexCollMatchesExprColl(idxcollation, expr_coll))
        return false;

    /*
     * These syntactic tests are the same as in match_clause_to_indexcol()
     */
    if (match_index_to_operand(leftop, indexcol, index) && !bms_is_member(index_relid, pull_varnos(rightop)) &&
        !contain_volatile_functions(rightop)) {
        /* OK, indexkey is on left */
    } else if (match_index_to_operand(rightop, indexcol, index) && !bms_is_member(index_relid, pull_varnos(leftop)) &&
               !contain_volatile_functions(leftop)) {
        /* indexkey is on right, so commute the operator */
        expr_op = get_commutator(expr_op);
        if (expr_op == InvalidOid)
            return false;
    } else
        return false;

    /* We're good if the operator is the right type of opfamily member */
    switch (get_op_opfamily_strategy(expr_op, opfamily)) {
        case BTLessStrategyNumber:
        case BTLessEqualStrategyNumber:
        case BTGreaterEqualStrategyNumber:
        case BTGreaterStrategyNumber:
            return true;
        default:
            break;
    }

    return false;
}

/****************************************************************************
 *				----  ROUTINES TO CHECK ORDERING OPERATORS	----
 ****************************************************************************/
/*
 * match_pathkeys_to_index
 *		Test whether an index can produce output ordered according to the
 *		given pathkeys using "ordering operators".
 *
 * If it can, return a list of suitable ORDER BY expressions, each of the form
 * "indexedcol operator pseudoconstant", along with an integer list of the
 * index column numbers (zero based) that each clause would be used with.
 * NIL lists are returned if the ordering is not achievable this way.
 *
 * On success, the result list is ordered by pathkeys, and in fact is
 * one-to-one with the requested pathkeys.
 */
static void match_pathkeys_to_index(
    IndexOptInfo* index, List* pathkeys, List** orderby_clauses_p, List** clause_columns_p)
{
    List* orderby_clauses = NIL;
    List* clause_columns = NIL;
    ListCell* lc1 = NULL;

    *orderby_clauses_p = NIL; /* set default results */
    *clause_columns_p = NIL;

    /* Only indexes with the amcanorderbyop property are interesting here */
    if (!index->amcanorderbyop)
        return;

    foreach (lc1, pathkeys) {
        PathKey* pathkey = (PathKey*)lfirst(lc1);
        bool found = false;
        ListCell* lc2 = NULL;

        /*
         * Note: for any failure to match, we just return NIL immediately.
         * There is no value in matching just some of the pathkeys.
         */
        /* Pathkey must request default sort order for the target opfamily */
        if (index->relam == BM25_AM_OID) {
            if (pathkey->pk_strategy != BTGreaterStrategyNumber) {
                return;
            }
        } else if (pathkey->pk_strategy != BTLessStrategyNumber || pathkey->pk_nulls_first)
            return;

        /* If eclass is volatile, no hope of using an indexscan */
        if (pathkey->pk_eclass->ec_has_volatile)
            return;

        /*
         * Try to match eclass member expression(s) to index.  Note that child
         * EC members are considered, but only when they belong to the target
         * relation.  (Unlike regular members, the same expression could be a
         * child member of more than one EC.  Therefore, the same index could
         * be considered to match more than one pathkey list, which is OK
         * here.  See also get_eclass_for_sort_expr.)
         */
        foreach (lc2, pathkey->pk_eclass->ec_members) {
            EquivalenceMember* member = (EquivalenceMember*)lfirst(lc2);
            int indexcol;

            /* No possibility of match if it references other relations */
            if (!bms_equal(member->em_relids, index->rel->relids))
                continue;

            /*
             * We allow any column of the index to match each pathkey; they
             * don't have to match left-to-right as you might expect.  This is
             * correct for GiST, which is the sole existing AM supporting
             * amcanorderbyop.	We might need different logic in future for
             * other implementations.
             */
            for (indexcol = 0; indexcol < index->nkeycolumns; indexcol++) {
                Expr* expr = NULL;

                expr = match_clause_to_ordering_op(index, indexcol, member->em_expr, pathkey->pk_opfamily);
                if (expr != NULL) {
                    orderby_clauses = lappend(orderby_clauses, expr);
                    clause_columns = lappend_int(clause_columns, indexcol);
                    found = true;
                    break;
                }
            }

            if (found) /* don't want to look at remaining members */
                break;
        }

        if (!found) /* fail if no match for this pathkey */
            return;
    }

    *orderby_clauses_p = orderby_clauses; /* success! */
    *clause_columns_p = clause_columns;
}

/*
 * match_clause_to_ordering_op
 *	  Determines whether an ordering operator expression matches an
 *	  index column.
 *
 *	  This is similar to, but simpler than, match_clause_to_indexcol.
 *	  We only care about simple OpExpr cases.  The input is a bare
 *	  expression that is being ordered by, which must be of the form
 *	  (indexkey op const) or (const op indexkey) where op is an ordering
 *	  operator for the column's opfamily.
 *
 * 'index' is the index of interest.
 * 'indexcol' is a column number of 'index' (counting from 0).
 * 'clause' is the ordering expression to be tested.
 * 'pk_opfamily' is the btree opfamily describing the required sort order.
 *
 * Note that we currently do not consider the collation of the ordering
 * operator's result.  In practical cases the result type will be numeric
 * and thus have no collation, and it's not very clear what to match to
 * if it did have a collation.	The index's collation should match the
 * ordering operator's input collation, not its result.
 *
 * If successful, return 'clause' as-is if the indexkey is on the left,
 * otherwise a commuted copy of 'clause'.  If no match, return NULL.
 */
static Expr* match_clause_to_ordering_op(IndexOptInfo* index, int indexcol, Expr* clause, Oid pk_opfamily)
{
    Oid opfamily;
    Oid idxcollation;
    Node* leftop = NULL;
    Node* rightop = NULL;
    Oid expr_op;
    Oid expr_coll;
    Oid sortfamily;
    bool commuted = false;

    Assert(indexcol < index->nkeycolumns);
    opfamily = index->opfamily[indexcol];
    idxcollation = index->indexcollations[indexcol];

    /*
     * Clause must be a binary opclause.
     */
    if (!is_opclause(clause))
        return NULL;
    leftop = get_leftop(clause);
    rightop = get_rightop(clause);
    if (leftop == NULL || rightop == NULL)
        return NULL;
    expr_op = ((OpExpr*)clause)->opno;
    expr_coll = ((OpExpr*)clause)->inputcollid;

    /*
     * We can forget the whole thing right away if wrong collation.
     */
    if (!IndexCollMatchesExprColl(idxcollation, expr_coll))
        return NULL;

    /*
     * Check for clauses of the form: (indexkey operator constant) or
     * (constant operator indexkey).
     */
    if (match_index_to_operand(leftop, indexcol, index) && !contain_var_clause(rightop) &&
        !contain_volatile_functions(rightop)) {
        commuted = false;
    } else if (match_index_to_operand(rightop, indexcol, index) && !contain_var_clause(leftop) &&
               !contain_volatile_functions(leftop)) {
        /* Might match, but we need a commuted operator */
        expr_op = get_commutator(expr_op);
        if (expr_op == InvalidOid)
            return NULL;
        commuted = true;
    } else
        return NULL;

    /*
     * Is the (commuted) operator an ordering operator for the opfamily? And
     * if so, does it yield the right sorting semantics?
     */
    sortfamily = get_op_opfamily_sortfamily(expr_op, opfamily);
    if (sortfamily != pk_opfamily)
        return NULL;

    /* We have a match.  Return clause or a commuted version thereof. */
    if (commuted) {
        OpExpr* newclause = makeNode(OpExpr);

        /* flat-copy all the fields of clause */
        errno_t rc = memcpy_s(newclause, sizeof(OpExpr), clause, sizeof(OpExpr));
        securec_check(rc, "\0", "\0");

        /* commute it */
        newclause->opno = expr_op;
        newclause->opfuncid = InvalidOid;
        newclause->args = list_make2(rightop, leftop);

        clause = (Expr*)newclause;
    }

    return clause;
}

/****************************************************************************
 *				----  ROUTINES TO DO PARTIAL INDEX PREDICATE TESTS	----
 ****************************************************************************/
/*
 * check_partial_indexes
 *		Check each partial index of the relation, and mark it predOK if
 *		the index's predicate is satisfied for this query.
 *
 * Note: it is possible for this to get re-run after adding more restrictions
 * to the rel; so we might be able to prove more indexes OK.  We assume that
 * adding more restrictions can't make an index not OK.
 */
void check_partial_indexes(PlannerInfo* root, RelOptInfo* rel)
{
    List* clauselist = NIL;
    bool have_partial = false;
    Relids otherrels;
    ListCell* lc = NULL;

    /*
     * Frequently, there will be no partial indexes, so first check to make
     * sure there's something useful to do here.
     */
    have_partial = false;
    foreach (lc, rel->indexlist) {
        IndexOptInfo* index = (IndexOptInfo*)lfirst(lc);

        if (index->indpred == NIL)
            continue; /* ignore non-partial indexes */

        if (index->predOK)
            continue; /* don't repeat work if already proven OK */

        have_partial = true;
        break;
    }
    if (!have_partial)
        return;

    /*
     * Construct a list of clauses that we can assume true for the purpose
     * of proving the index(es) usable.  Restriction clauses for the rel are
     * always usable, and so are any join clauses that are "movable to" this
     * rel.  Also, we can consider any EC-derivable join clauses (which must
     * be "movable to" this rel, by definition).
     */
    clauselist = list_copy(rel->baserestrictinfo);

    /* Scan the rel's join clauses */
    foreach (lc, rel->joininfo) {
        RestrictInfo* rinfo = (RestrictInfo*)lfirst(lc);

        /* Check if clause can be moved to this rel */
        if (!join_clause_is_movable_to(rinfo, rel))
            continue;

        clauselist = lappend(clauselist, rinfo);
    }

    /*
     * Add on any equivalence-derivable join clauses.  Computing the correct
     * relid sets for generate_join_implied_equalities is slightly tricky
     * because the rel could be a child rel rather than a true baserel, and
     * in that case we must remove its parent's relid from all_baserels.
     */
    if (rel->reloptkind == RELOPT_OTHER_MEMBER_REL) {
        /* Lookup parent->child translation data */
        AppendRelInfo* appinfo = find_childrel_appendrelinfo(root, rel);

        otherrels = bms_difference(root->all_baserels, bms_make_singleton(appinfo->parent_relid));
    } else
        otherrels = bms_difference(root->all_baserels, rel->relids);

    if (!bms_is_empty(otherrels))
        clauselist = list_concat(
            clauselist, generate_join_implied_equalities(root, bms_union(rel->relids, otherrels), otherrels, rel));

    /* Now try to prove each index predicate true */
    foreach (lc, rel->indexlist) {
        IndexOptInfo* index = (IndexOptInfo*)lfirst(lc);

        if (index->indpred == NIL)
            continue; /* ignore non-partial indexes */

        if (index->predOK)
            continue; /* don't repeat work if already proven OK */

        if (ENABLE_CACHEDPLAN_MGR && root->glob->boundParams != NULL &&
            root->glob->boundParams->uParamInfo == PARAM_VAL_SELECTIVITY_INFO){
                root->glob->boundParams->params_lazy_bind = false;
                clauselist = eval_const_clauses_params(root, clauselist);
                root->glob->boundParams->params_lazy_bind = true;
            }

        index->predOK = predicate_implied_by(index->indpred, clauselist);
    }
}

/****************************************************************************
 *				----  ROUTINES TO CHECK EXTERNALLY-VISIBLE CONDITIONS  ----
 ****************************************************************************/
/*
 * eclass_member_matches_indexcol
 *	  Test whether an EquivalenceClass member matches an index column.
 *
 * This is exported for use by generate_implied_equalities_for_indexcol.
 */
bool eclass_member_matches_indexcol(EquivalenceClass* ec, EquivalenceMember* em, IndexOptInfo* index, int indexcol)
{
    Oid curFamily;
    Oid curCollation;

    Assert(indexcol < index->nkeycolumns);
    curFamily = index->opfamily[indexcol];
    curCollation = index->indexcollations[indexcol];

    /*
     * If it's a btree index, we can reject it if its opfamily isn't
     * compatible with the EC, since no clause generated from the EC could be
     * used with the index.  For non-btree indexes, we can't easily tell
     * whether clauses generated from the EC could be used with the index, so
     * don't check the opfamily.  This might mean we return "true" for a
     * useless EC, so we have to recheck the results of
     * generate_implied_equalities_for_indexcol; see
     * match_eclass_clauses_to_index.
     */
    if (OID_IS_BTREE(index->relam) && !list_member_oid(ec->ec_opfamilies, curFamily))
        return false;

    /* We insist on collation match for all index types, though */
    if (!IndexCollMatchesExprColl(curCollation, ec->ec_collation))
        return false;

    return match_index_to_operand((Node*)em->em_expr, indexcol, index, true);
}

static bool relation_has_unique_index_for_no_index(PlannerInfo* root, RelOptInfo* rel)
{
    /* Each row in the delete-delta table is unique even if there is no index exists */
    if (u_sess->attr.attr_sql.enable_cluster_resize &&
        strncmp(root->simple_rte_array[rel->relid]->relname,
        REDIS_DELETE_DELTA_TABLE_PREFIX, strlen(REDIS_DELETE_DELTA_TABLE_PREFIX)) == 0) {
        return true;
    }
    return false;
}

/*
 * relation_has_unique_index_for
 *	  Determine whether the relation provably has at most one row satisfying
 *	  a set of equality conditions, because the conditions constrain all
 *	  columns of some unique index.
 *
 * The conditions can be represented in either or both of two ways:
 * 1. A list of RestrictInfo nodes, where the caller has already determined
 * that each condition is a mergejoinable equality with an expression in
 * this relation on one side, and an expression not involving this relation
 * on the other.  The transient outer_is_left flag is used to identify which
 * side we should look at: left side if outer_is_left is false, right side
 * if it is true.
 * 2. A list of expressions in this relation, and a corresponding list of
 * equality operators. The caller must have already checked that the operators
 * represent equality.	(Note: the operators could be cross-type; the
 * expressions should correspond to their RHS inputs.)
 *
 * The caller need only supply equality conditions arising from joins;
 * this routine automatically adds in any usable baserestrictinfo clauses.
 * (Note that the passed-in restrictlist will be destructively modified!)
 */
bool relation_has_unique_index_for(
    PlannerInfo* root, RelOptInfo* rel, List* restrictlist, List* exprlist, List* oprlist)
{
    ListCell* ic = NULL;

    AssertEreport(
        list_length(exprlist) == list_length(oprlist), MOD_OPT, "Exprlist and oprlist are not equal in length");

    /* Short-circuit if no indexes... */
    if (rel->indexlist == NIL) {
        return relation_has_unique_index_for_no_index(root, rel);
    }
    /*
     * Examine the rel's restriction clauses for usable var = const clauses
     * that we can add to the restrictlist.
     */
    foreach (ic, rel->baserestrictinfo) {
        RestrictInfo* restrictinfo = (RestrictInfo*)lfirst(ic);

        /*
         * Note: can_join won't be set for a restriction clause, but
         * mergeopfamilies will be if it has a mergejoinable operator and
         * doesn't contain volatile functions.
         */
        if (restrictinfo->mergeopfamilies == NIL)
            continue; /* not mergejoinable */

        /*
         * The clause certainly doesn't refer to anything but the given rel.
         * If either side is pseudoconstant then we can use it.
         */
        if (bms_is_empty(restrictinfo->left_relids)) {
            /* righthand side is inner */
            restrictinfo->outer_is_left = true;
        } else if (bms_is_empty(restrictinfo->right_relids)) {
            /* lefthand side is inner */
            restrictinfo->outer_is_left = false;
        } else
            continue;

        /* OK, add to list */
        restrictlist = lappend(restrictlist, restrictinfo);
    }

    /* Short-circuit the easy case */
    if (restrictlist == NIL && exprlist == NIL)
        return false;

    /* Examine each index of the relation ... */
    foreach (ic, rel->indexlist) {
        IndexOptInfo* ind = (IndexOptInfo*)lfirst(ic);
        int c;

        /*
         * If the index is not unique, or not immediately enforced, or if it's
         * a partial index that doesn't match the query, it's useless here.
         */
        if (!ind->unique || !ind->immediate || (ind->indpred != NIL && !ind->predOK))
            continue;

        /*
         * Try to find each index column in the lists of conditions.  This is
         * O(N^2) or worse, but we expect all the lists to be short.
         */
        for (c = 0; c < ind->nkeycolumns; c++) {
            bool matched = false;
            ListCell* lc = NULL;
            ListCell* lc2 = NULL;

            foreach (lc, restrictlist) {
                RestrictInfo* rinfo = (RestrictInfo*)lfirst(lc);
                Node* rexpr = NULL;

                /*
                 * The condition's equality operator must be a member of the
                 * index opfamily, else it is not asserting the right kind of
                 * equality behavior for this index.  We check this first
                 * since it's probably cheaper than match_index_to_operand().
                 */
                if (!list_member_oid(rinfo->mergeopfamilies, ind->opfamily[c]))
                    continue;

                /*
                 * XXX at some point we may need to check collations here too.
                 * For the moment we assume all collations reduce to the same
                 * notion of equality.
                 */
                /* OK, see if the condition operand matches the index key */
                if (rinfo->outer_is_left)
                    rexpr = get_rightop(rinfo->clause);
                else
                    rexpr = get_leftop(rinfo->clause);

                if (match_index_to_operand(rexpr, c, ind, true)) {
                    matched = true; /* column is unique */
                    break;
                }
            }

            if (matched)
                continue;

            forboth(lc, exprlist, lc2, oprlist)
            {
                Node* expr = (Node*)lfirst(lc);
                Oid opr = lfirst_oid(lc2);

                /* See if the expression matches the index key */
                if (!match_index_to_operand(expr, c, ind, true))
                    continue;

                /*
                 * The equality operator must be a member of the index
                 * opfamily, else it is not asserting the right kind of
                 * equality behavior for this index.  We assume the caller
                 * determined it is an equality operator, so we don't need to
                 * check any more tightly than this.
                 */
                if (!op_in_opfamily(opr, ind->opfamily[c]))
                    continue;

                /*
                 * XXX at some point we may need to check collations here too.
                 * For the moment we assume all collations reduce to the same
                 * notion of equality.
                 */
                matched = true; /* column is unique */
                break;
            }

            if (!matched)
                break; /* no match; this index doesn't help us */
        }

        /* Matched all columns of this index? */
        if (c == ind->nkeycolumns)
            return true;
    }

    return false;
}

/****************************************************************************
 *				----  ROUTINES TO CHECK OPERANDS  ----
 ****************************************************************************/
/*
 * match_index_to_operand
 *
 *	  Generalized test for a match between an index's key
 *	  and the operand on one side of a restriction or join clause.
 *
 * operand: the nodetree to be compared to the index
 * indexcol: the column number of the index (counting from 0)
 * index: the index of interest
 * match_prefixkey: if match prefix key to column
 *
 * Note that we aren't interested in collations here; the caller must check
 * for a collation match, if it's dealing with an operator where that matters.
 *
 * This is exported for use in selfuncs.c.
 */
bool match_index_to_operand(Node* operand, int indexcol, IndexOptInfo* index, bool match_prefixkey)
{
    int indkey;

    /*
     * Ignore any RelabelType node above the operand.	This is needed to be
     * able to apply indexscanning in binary-compatible-operator cases. Note:
     * we can assume there is at most one RelabelType node;
     * eval_const_expressions() will have simplified if more than one.
     */
    if (operand && IsA(operand, RelabelType))
        operand = (Node*)((RelabelType*)operand)->arg;

    indkey = index->indexkeys[indexcol];
    if (indkey != 0) {
        /*
         * Simple index column; operand must be a matching Var.
         */
        if (operand && IsA(operand, Var) && index->rel->relid == ((Var*)operand)->varno &&
            indkey == ((Var*)operand)->varattno)
            return true;
    } else {
        /*
         * Index expression; find the correct expression.  (This search could
         * be avoided, at the cost of complicating all the callers of this
         * routine; doesn't seem worth it.)
         */
        ListCell* indexpr_item = NULL;
        int i;
        Node* indexkey = NULL;

        indexpr_item = list_head(index->indexprs);
        for (i = 0; i < indexcol; i++) {
            if (index->indexkeys[i] == 0) {
                if (indexpr_item == NULL)
                    ereport(ERROR,
                        (errmodule(MOD_OPT),
                            errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                            (errmsg("wrong number of index expressions when match index to operand"))));
                indexpr_item = lnext(indexpr_item);
            }
        }
        if (indexpr_item == NULL)
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                    (errmsg("wrong number of index expressions when match index to operand"))));
        indexkey = (Node*)lfirst(indexpr_item);

        /*
         * Does it match the operand?  Again, strip any relabeling.
         */
        if (indexkey && IsA(indexkey, RelabelType))
            indexkey = (Node*)((RelabelType*)indexkey)->arg;
        /*
         * PrefixKey is not a strict expression. It can be matched to column.
         */
        if (match_prefixkey && PrefixKeyColumnMatched(indexkey, operand)) {
            return true;
        }
        if (equal(indexkey, operand))
            return true;
    }

    /*
     * if FuncExpr, check whether there are risks caused by type conversion.
     */
    if (IsA(operand, FuncExpr))
        check_report_cause_type((FuncExpr*)operand, indkey);

    return false;
}

/****************************************************************************
 *			----  ROUTINES FOR "SPECIAL" INDEXABLE OPERATORS  ----
 ****************************************************************************/
/*
 * These routines handle special optimization of operators that can be
 * used with index scans even though they are not known to the executor's
 * indexscan machinery.  The key idea is that these operators allow us
 * to derive approximate indexscan qual clauses, such that any tuples
 * that pass the operator clause itself must also satisfy the simpler
 * indexscan condition(s).	Then we can use the indexscan machinery
 * to avoid scanning as much of the table as we'd otherwise have to,
 * while applying the original operator as a qpqual condition to ensure
 * we deliver only the tuples we want.	(In essence, we're using a regular
 * index as if it were a lossy index.)
 *
 * An example of what we're doing is
 *			textfield LIKE 'abc%'
 * from which we can generate the indexscanable conditions
 *			textfield >= 'abc' AND textfield < 'abd'
 * which allow efficient scanning of an index on textfield.
 * (In reality, character set and collation issues make the transformation
 * from LIKE to indexscan limits rather harder than one might think ...
 * but that's the basic idea.)
 *
 * Another thing that we do with this machinery is to provide special
 * smarts for "boolean" indexes (that is, indexes on boolean columns
 * that support boolean equality).	We can transform a plain reference
 * to the indexkey into "indexkey = true", or "NOT indexkey" into
 * "indexkey = false", so as to make the expression indexable using the
 * regular index operators.  (As of Postgres 8.1, we must do this here
 * because constant simplification does the reverse transformation;
 * without this code there'd be no way to use such an index at all.)
 *
 * Three routines are provided here:
 *
 * match_special_index_operator() is just an auxiliary function for
 * match_clause_to_indexcol(); after the latter fails to recognize a
 * restriction opclause's operator as a member of an index's opfamily,
 * it asks match_special_index_operator() whether the clause should be
 * considered an indexqual anyway.
 *
 * match_boolean_index_clause() similarly detects clauses that can be
 * converted into boolean equality operators.
 *
 * expand_indexqual_conditions() converts a list of RestrictInfo nodes
 * (with implicit AND semantics across list elements) into a list of clauses
 * that the executor can actually handle.  For operators that are members of
 * the index's opfamily this transformation is a no-op, but clauses recognized
 * by match_special_index_operator() or match_boolean_index_clause() must be
 * converted into one or more "regular" indexqual conditions.
 */
/*
 * match_boolean_index_clause
 *	  Recognize restriction clauses that can be matched to a boolean index.
 *
 * This should be called only when IsBooleanOpfamily() recognizes the
 * index's operator family.  We check to see if the clause matches the
 * index's key.
 */
static bool match_boolean_index_clause(Node* clause, int indexcol, IndexOptInfo* index)
{
    /* Direct match? */
    if (match_index_to_operand(clause, indexcol, index))
        return true;
    /* NOT clause? */
    if (not_clause(clause)) {
        if (match_index_to_operand((Node*)get_notclausearg((Expr*)clause), indexcol, index)) {
            return true;
        }            
    } else if (clause && IsA(clause, BooleanTest)) {
        /*
         * Since we only consider clauses at top level of WHERE, we can convert
         * indexkey IS TRUE and indexkey IS FALSE to index searches as well. The
         * different meaning for NULL isn't important.
         */
        BooleanTest* btest = (BooleanTest*)clause;

        if (btest->booltesttype == IS_TRUE || btest->booltesttype == IS_FALSE)
            if (match_index_to_operand((Node*)btest->arg, indexcol, index))
                return true;
    }
    return false;
}


static bool can_be_applyed_in_b_format(Oid idxcollation, IndexOptInfo* index, int indexcol)
{
    if (!is_b_format_collation(idxcollation)) {
        return false;
    }
    TargetEntry* tle = (TargetEntry*)list_nth(index->indextlist, indexcol);
    bool hasPrefix = false;
    bool hasLength = false;
    if (IsA(tle->expr, PrefixKey)) {
        hasPrefix = true;
    } else if (IsA(tle->expr, Var)) {
        Var *var = (Var *)tle->expr;
        hasLength = var->vartypmod > 0;
    }
    return (hasLength || hasPrefix);
}

static bool is_valid_expr_like_op_in_b_format(Oid expr_op)
{
    if (u_sess->attr.attr_sql.sql_compatibility != B_FORMAT) {
        return false;
    }
    Oid binaryOid = get_typeoid(PG_CATALOG_NAMESPACE, "binary");
    Oid varbinaryOid = get_typeoid(PG_CATALOG_NAMESPACE, "varbinary");

    return (expr_op == OpernameGetOprid(list_make1(makeString("~~")), binaryOid, binaryOid) ||
            expr_op == OpernameGetOprid(list_make1(makeString("~~")), varbinaryOid, varbinaryOid));
}

/*
 * match_special_index_operator
 *	  Recognize restriction clauses that can be used to generate
 *	  additional indexscanable qualifications.
 *
 * The given clause is already known to be a binary opclause having
 * the form (indexkey OP pseudoconst) or (pseudoconst OP indexkey),
 * but the OP proved not to be one of the index's opfamily operators.
 * Return 'true' if we can do something with it anyway.
 */
static bool match_special_index_operator(Expr* clause,
    Oid opfamily, Oid idxcollation, bool indexkey_on_left, IndexOptInfo* index, int indexcol)
{
    bool isIndexable = false;
    Node* rightop = NULL;
    Oid expr_op;
    Oid expr_coll;
    Const* patt = NULL;
    Const* prefix = NULL;
    Pattern_Prefix_Status pstatus = Pattern_Prefix_None;

    /*
     * Currently, all known special operators require the indexkey on the
     * left, but this test could be pushed into the switch statement if some
     * are added that do not...
     */
    if (!indexkey_on_left) {
        return false;
    }

    /* we know these will succeed */
    rightop = get_rightop(clause);
    expr_op = ((OpExpr*)clause)->opno;
    expr_coll = ((OpExpr*)clause)->inputcollid;

    /* again, required for all current special ops: */
    if (rightop == NULL || !IsA(rightop, Const) || ((Const*)rightop)->constisnull)
        return false;
    patt = (Const*)rightop;

    switch (expr_op) {
        case OID_TEXT_LIKE_OP:
        case OID_BPCHAR_LIKE_OP:
        case OID_NAME_LIKE_OP:
            /* the right-hand const is type text for all of these */
            pstatus = pattern_fixed_prefix(patt, Pattern_Type_Like, expr_coll, &prefix, NULL);
            isIndexable = (pstatus != Pattern_Prefix_None);
            break;

        case OID_BYTEA_LIKE_OP:
            pstatus = pattern_fixed_prefix(patt, Pattern_Type_Like, expr_coll, &prefix, NULL);
            isIndexable = (pstatus != Pattern_Prefix_None);
            break;

        case OID_TEXT_ICLIKE_OP:
        case OID_BPCHAR_ICLIKE_OP:
        case OID_NAME_ICLIKE_OP:
            /* the right-hand const is type text for all of these */
            pstatus = pattern_fixed_prefix(patt, Pattern_Type_Like_IC, expr_coll, &prefix, NULL);
            isIndexable = (pstatus != Pattern_Prefix_None);
            break;

        case OID_TEXT_REGEXEQ_OP:
        case OID_BPCHAR_REGEXEQ_OP:
        case OID_NAME_REGEXEQ_OP:
            /* the right-hand const is type text for all of these */
            pstatus = pattern_fixed_prefix(patt, Pattern_Type_Regex, expr_coll, &prefix, NULL);
            isIndexable = (pstatus != Pattern_Prefix_None);
            break;

        case OID_TEXT_ICREGEXEQ_OP:
        case OID_BPCHAR_ICREGEXEQ_OP:
        case OID_NAME_ICREGEXEQ_OP:
            /* the right-hand const is type text for all of these */
            pstatus = pattern_fixed_prefix(patt, Pattern_Type_Regex_IC, expr_coll, &prefix, NULL);
            isIndexable = (pstatus != Pattern_Prefix_None);
            break;

        case OID_INET_SUB_OP:
        case OID_INET_SUBEQ_OP:
            isIndexable = true;
            break;

        default:
            if (is_valid_expr_like_op_in_b_format(expr_op)) {
                pstatus = pattern_fixed_prefix(patt, Pattern_Type_Like, expr_coll, &prefix, NULL);
                isIndexable = (pstatus != Pattern_Prefix_None);
            }
            break;
    }

    if (prefix != NULL) {
        pfree(DatumGetPointer(prefix->constvalue));
        pfree_ext(prefix);
    }

    /* done if the expression doesn't look indexable */
    if (!isIndexable) {
        return false;
    }

    /*
     * Must also check that index's opfamily supports the operators we will
     * want to apply.  (A hash index, for example, will not support ">=".)
     * Currently, only btree and spgist support the operators we need.
     *
     * Note: actually, in the Pattern_Prefix_Exact case, we only need "=" so a
     * hash index would work.  Currently it doesn't seem worth checking for
     * that, however.
     *
     * We insist on the opfamily being the specific one we expect, else we'd
     * do the wrong thing if someone were to make a reverse-sort opfamily with
     * the same operators.
     *
     * The non-pattern opclasses will not sort the way we need in most non-C
     * locales.  We can use such an index anyway for an exact match (simple
     * equality), but not for prefix-match cases.  Note that here we are
     * looking at the index's collation, not the expression's collation --
     * this test is *not* dependent on the LIKE/regex operator's collation.
     */
    switch (expr_op) {
        case OID_TEXT_LIKE_OP:
        case OID_TEXT_ICLIKE_OP:
        case OID_TEXT_REGEXEQ_OP:
        case OID_TEXT_ICREGEXEQ_OP:
            isIndexable =
                (opfamily == TEXT_PATTERN_BTREE_FAM_OID) || (opfamily == TEXT_SPGIST_FAM_OID) ||
                (opfamily == TEXT_PATTERN_UBTREE_FAM_OID) ||
                ((opfamily == TEXT_BTREE_FAM_OID || opfamily == TEXT_UBTREE_FAM_OID) &&
                 (pstatus == Pattern_Prefix_Exact || lc_collate_is_c(idxcollation) ||
                 can_be_applyed_in_b_format(idxcollation, index, indexcol)));
            break;

        case OID_BPCHAR_LIKE_OP:
        case OID_BPCHAR_ICLIKE_OP:
        case OID_BPCHAR_REGEXEQ_OP:
        case OID_BPCHAR_ICREGEXEQ_OP:
            isIndexable = (opfamily == BPCHAR_PATTERN_BTREE_FAM_OID || opfamily == BPCHAR_PATTERN_UBTREE_FAM_OID) ||
                          ((opfamily == BPCHAR_BTREE_FAM_OID || opfamily == BPCHAR_UBTREE_FAM_OID) &&
                              (pstatus == Pattern_Prefix_Exact || lc_collate_is_c(idxcollation)||
                              can_be_applyed_in_b_format(idxcollation, index, indexcol)));
            break;

        case OID_NAME_LIKE_OP:
        case OID_NAME_ICLIKE_OP:
        case OID_NAME_REGEXEQ_OP:
        case OID_NAME_ICREGEXEQ_OP:
            /* name uses locale-insensitive sorting */
            isIndexable = (opfamily == NAME_BTREE_FAM_OID || opfamily == NAME_UBTREE_FAM_OID);
            break;

        case OID_BYTEA_LIKE_OP:
            isIndexable = (opfamily == BYTEA_BTREE_FAM_OID || opfamily == BYTEA_UBTREE_FAM_OID);
            break;

        case OID_INET_SUB_OP:
        case OID_INET_SUBEQ_OP:
            isIndexable = (opfamily == NETWORK_BTREE_FAM_OID || opfamily == NETWORK_UBTREE_FAM_OID);
            break;
        default:
            if (is_valid_expr_like_op_in_b_format(expr_op)) {
                isIndexable =
                    (OpfamilynameGetOpfid(BTREE_AM_OID, "binary_ops") == opfamily ||
                        OpfamilynameGetOpfid(BTREE_AM_OID, "varbinary_ops") == opfamily);
            }
            break;
    }

    return isIndexable;
}

/*
 * expand_indexqual_conditions
 *	  Given a list of RestrictInfo nodes, produce a list of directly usable
 *	  index qual clauses.
 *
 * Standard qual clauses (those in the index's opfamily) are passed through
 * unchanged.  Boolean clauses and "special" index operators are expanded
 * into clauses that the indexscan machinery will know what to do with.
 * RowCompare clauses are simplified if necessary to create a clause that is
 * fully checkable by the index.
 *
 * In addition to the expressions themselves, there are auxiliary lists
 * of the index column numbers that the clauses are meant to be used with;
 * we generate an updated column number list for the result.  (This is not
 * the identical list because one input clause sometimes produces more than
 * one output clause.)
 *
 * The input clauses are sorted by column number, and so the output is too.
 * (This is depended on in various places in both planner and executor.)
 */
void expand_indexqual_conditions(
    IndexOptInfo* index, List* indexclauses, List* indexclausecols, List** indexquals_p, List** indexqualcols_p)
{
    List* indexquals = NIL;
    List* indexqualcols = NIL;
    ListCell* lcc = NULL;
    ListCell* lci = NULL;

    forboth(lcc, indexclauses, lci, indexclausecols)
    {
        RestrictInfo* rinfo = (RestrictInfo*)lfirst(lcc);
        int indexcol = lfirst_int(lci);
        Expr* clause = rinfo->clause;
        Oid curFamily;
        Oid curCollation;
        Assert(indexcol < index->nkeycolumns);
        curFamily = index->opfamily[indexcol];
        curCollation = index->indexcollations[indexcol];

        /* First check for boolean cases */
        if (IsBooleanOpfamily(curFamily)) {
            Expr* boolqual = NULL;

            boolqual = expand_boolean_index_clause((Node*)clause, indexcol, index);
            if (boolqual != NULL) {
                indexquals = lappend(indexquals, make_simple_restrictinfo(boolqual));
                indexqualcols = lappend_int(indexqualcols, indexcol);
                continue;
            }
        }

        /*
         * Else it must be an opclause (usual case), ScalarArrayOp,
         * RowCompare, or NullTest
         */
        if (is_opclause(clause)) {
            indexquals = list_concat(indexquals,
                expand_indexqual_opclause(index, rinfo, curFamily, curCollation, indexcol));
            /* expand_indexqual_opclause can produce multiple clauses */
            while (list_length(indexqualcols) < list_length(indexquals))
                indexqualcols = lappend_int(indexqualcols, indexcol);
        } else if (IsA(clause, ScalarArrayOpExpr)) {
            /* no extra work at this time */
            indexquals = lappend(indexquals, expand_indexqual_scalar_array_op_expr(index, rinfo, curFamily, indexcol));
            indexqualcols = lappend_int(indexqualcols, indexcol);
        } else if (IsA(clause, RowCompareExpr)) {
            indexquals = lappend(indexquals, expand_indexqual_rowcompare(rinfo, index, indexcol));
            indexqualcols = lappend_int(indexqualcols, indexcol);
        } else if (IsA(clause, NullTest)) {
            Assert(index->amsearchnulls);
            indexquals = lappend(indexquals, rinfo);
            indexqualcols = lappend_int(indexqualcols, indexcol);
        } else
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    (errmsg("unsupported indexqual type when expand indexqual conditions: %d", (int)nodeTag(clause)))));
    }

    *indexquals_p = indexquals;
    *indexqualcols_p = indexqualcols;
}

/*
 * expand_boolean_index_clause
 *	  Convert a clause recognized by match_boolean_index_clause into
 *	  a boolean equality operator clause.
 *
 * Returns NULL if the clause isn't a boolean index qual.
 */
static Expr* expand_boolean_index_clause(Node* clause, int indexcol, IndexOptInfo* index)
{
    /* Direct match? */
    if (match_index_to_operand(clause, indexcol, index)) {
        /* convert to indexkey = TRUE */
        return make_opclause(BooleanEqualOperator,
            BOOLOID,
            false,
            (Expr*)clause,
            (Expr*)makeBoolConst(true, false),
            InvalidOid,
            InvalidOid);
    }
    /* NOT clause? */
    if (not_clause(clause)) {
        Node* arg = (Node*)get_notclausearg((Expr*)clause);

        /* It must have matched the indexkey */
        AssertEreport(match_index_to_operand(arg, indexcol, index), MOD_OPT, "Index key and operand are not matched");
        /* convert to indexkey = FALSE */
        return make_opclause(BooleanEqualOperator,
            BOOLOID,
            false,
            (Expr*)arg,
            (Expr*)makeBoolConst(false, false),
            InvalidOid,
            InvalidOid);
    }
    if (clause && IsA(clause, BooleanTest)) {
        BooleanTest* btest = (BooleanTest*)clause;
        Node* arg = (Node*)btest->arg;

        /* It must have matched the indexkey */
        AssertEreport(match_index_to_operand(arg, indexcol, index), MOD_OPT, "Index key and operand are not matched");
        if (btest->booltesttype == IS_TRUE) {
            /* convert to indexkey = TRUE */
            return make_opclause(BooleanEqualOperator,
                BOOLOID,
                false,
                (Expr*)arg,
                (Expr*)makeBoolConst(true, false),
                InvalidOid,
                InvalidOid);
        }
        if (btest->booltesttype == IS_FALSE) {
            /* convert to indexkey = FALSE */
            return make_opclause(BooleanEqualOperator,
                BOOLOID,
                false,
                (Expr*)arg,
                (Expr*)makeBoolConst(false, false),
                InvalidOid,
                InvalidOid);
        }
        /* Oops */
        Assert(false);
    }

    return NULL;
}

/*
 * expand_indexqual_opclause --- expand a single indexqual condition
 *		that is an operator clause
 *
 * The input is a single RestrictInfo, the output a list of RestrictInfos.
 *
 * In the base case this is just list_make1(), but we have to be prepared to
 * expand special cases that were accepted by match_special_index_operator().
 */
static List* expand_indexqual_opclause(IndexOptInfo* index, RestrictInfo* rinfo, Oid opfamily, Oid idxcollation,
    int indexcol)
{
    Expr* clause = rinfo->clause;

    /* we know these will succeed */
    Node* leftop = get_leftop(clause);
    Node* rightop = get_rightop(clause);
    Oid expr_op = ((OpExpr*)clause)->opno;
    Oid expr_coll = ((OpExpr*)clause)->inputcollid;
    Const* patt = (Const*)rightop;
    Const* prefix = NULL;
    Pattern_Prefix_Status pstatus;
    int prefixkey_len = get_index_column_prefix_lenth(index, indexcol);

    if (patt == NULL) {
        ereport(ERROR, (errmodule(MOD_OPT), errmsg("right operator can not be NULL")));
    }

    /*
     * LIKE and regex operators are not members of any btree index opfamily,
     * but they can be members of opfamilies for more exotic index types such
     * as GIN.	Therefore, we should only do expansion if the operator is
     * actually not in the opfamily.  But checking that requires a syscache
     * lookup, so it's best to first see if the operator is one we are
     * interested in.
     */
    switch (expr_op) {
        case OID_TEXT_LIKE_OP:
        case OID_BPCHAR_LIKE_OP:
        case OID_NAME_LIKE_OP:
        case OID_BYTEA_LIKE_OP:
            if (!op_in_opfamily(expr_op, opfamily)) {
                pstatus = pattern_fixed_prefix(patt, Pattern_Type_Like, expr_coll, &prefix, NULL);
                return prefix_quals_with_encdoing(leftop, opfamily, idxcollation, prefix, pstatus, prefixkey_len);
            }
            break;

        case OID_TEXT_ICLIKE_OP:
        case OID_BPCHAR_ICLIKE_OP:
        case OID_NAME_ICLIKE_OP:
            if (!op_in_opfamily(expr_op, opfamily)) {
                /* the right-hand const is type text for all of these */
                pstatus = pattern_fixed_prefix(patt, Pattern_Type_Like_IC, expr_coll, &prefix, NULL);
                return prefix_quals_with_encdoing(leftop, opfamily, idxcollation, prefix, pstatus, prefixkey_len);
            }
            break;

        case OID_TEXT_REGEXEQ_OP:
        case OID_BPCHAR_REGEXEQ_OP:
        case OID_NAME_REGEXEQ_OP:
            if (!op_in_opfamily(expr_op, opfamily)) {
                /* the right-hand const is type text for all of these */
                pstatus = pattern_fixed_prefix(patt, Pattern_Type_Regex, expr_coll, &prefix, NULL);
                return prefix_quals_with_encdoing(leftop, opfamily, idxcollation, prefix, pstatus, prefixkey_len);
            }
            break;

        case OID_TEXT_ICREGEXEQ_OP:
        case OID_BPCHAR_ICREGEXEQ_OP:
        case OID_NAME_ICREGEXEQ_OP:
            if (!op_in_opfamily(expr_op, opfamily)) {
                /* the right-hand const is type text for all of these */
                pstatus = pattern_fixed_prefix(patt, Pattern_Type_Regex_IC, expr_coll, &prefix, NULL);
                return prefix_quals_with_encdoing(leftop, opfamily, idxcollation, prefix, pstatus, prefixkey_len);
            }
            break;

        case OID_INET_SUB_OP:
        case OID_INET_SUBEQ_OP:
            if (!op_in_opfamily(expr_op, opfamily)) {
                return network_prefix_quals(leftop, expr_op, opfamily, patt->constvalue);
            }
            break;
        default:
            if (prefixkey_len > 0 && op_in_opfamily(expr_op, opfamily)) {
                return list_make1(rewrite_opclause_for_prefixkey(rinfo, index, opfamily, prefixkey_len));
            }
            if (is_valid_expr_like_op_in_b_format(expr_op)) {
                pstatus = pattern_fixed_prefix(patt, Pattern_Type_Like, expr_coll, &prefix, NULL);
                return prefix_quals_with_encdoing(leftop, opfamily, idxcollation, prefix, pstatus, prefixkey_len);
            }
            break;
    }

    /* Default case: just make a list of the unmodified indexqual */
    return list_make1(rinfo);
}

/*
 * expand_indexqual_rowcompare --- expand a single indexqual condition
 *		that is a RowCompareExpr
 *
 * This is a thin wrapper around adjust_rowcompare_for_index; we export the
 * latter so that createplan.c can use it to re-discover which columns of the
 * index are used by a row comparison indexqual.
 */
static RestrictInfo* expand_indexqual_rowcompare(RestrictInfo* rinfo, IndexOptInfo* index, int indexcol)
{
    RowCompareExpr* clause = (RowCompareExpr*)rinfo->clause;
    Expr* newclause = NULL;
    List* indexcolnos = NIL;
    bool var_on_left = false;

    newclause = adjust_rowcompare_for_index(clause, index, indexcol, &indexcolnos, &var_on_left);
    AssertEreport(newclause != NULL, MOD_OPT, "Newclause can not be NULL");
    /*
     * If we didn't have to change the RowCompareExpr, return the original
     * RestrictInfo.
     */
    if (newclause == (Expr*)clause)
        return rinfo;

    /* Else we need a new RestrictInfo */
    return make_simple_restrictinfo(newclause);
}

/*
 * adjust_rowcompare_for_index --- expand a single indexqual condition
 *		that is a RowCompareExpr
 *
 * It's already known that the first column of the row comparison matches
 * the specified column of the index.  We can use additional columns of the
 * row comparison as index qualifications, so long as they match the index
 * in the "same direction", ie, the indexkeys are all on the same side of the
 * clause and the operators are all the same-type members of the opfamilies.
 * If all the columns of the RowCompareExpr match in this way, we just use it
 * as-is.  Otherwise, we build a shortened RowCompareExpr (if more than one
 * column matches) or a simple OpExpr (if the first-column match is all
 * there is).  In these cases the modified clause is always "<=" or ">="
 * even when the original was "<" or ">" --- this is necessary to match all
 * the rows that could match the original.	(We are essentially building a
 * lossy version of the row comparison when we do this.)
 *
 * *indexcolnos receives an integer list of the index column numbers (zero
 * based) used in the resulting expression.  The reason we need to return
 * that is that if the index is selected for use, createplan.c will need to
 * call this again to extract that list.  (This is a bit grotty, but row
 * comparison indexquals aren't used enough to justify finding someplace to
 * keep the information in the Path representation.)  Since createplan.c
 * also needs to know which side of the RowCompareExpr is the index side,
 * we also return *var_on_left_p rather than re-deducing that there.
 */
Expr* adjust_rowcompare_for_index(
    RowCompareExpr* clause, IndexOptInfo* index, int indexcol, List** indexcolnos, bool* var_on_left_p)
{
    bool var_on_left = false;
    int op_strategy;
    Oid op_lefttype;
    Oid op_righttype;
    int matching_cols;
    Oid expr_op;
    List* opfamilies = NIL;
    List* lefttypes = NIL;
    List* righttypes = NIL;
    List* new_ops = NIL;
    ListCell* largs_cell = NULL;
    ListCell* rargs_cell = NULL;
    ListCell* opnos_cell = NULL;
    ListCell* collids_cell = NULL;

    /* We have to figure out (again) how the first col matches */
    var_on_left = match_index_to_operand((Node*)linitial(clause->largs), indexcol, index);
    AssertEreport(var_on_left == true || match_index_to_operand((Node*)linitial(clause->rargs), indexcol, index),
        MOD_OPT,
        "index key and operand are mismatched on either left or right");
    *var_on_left_p = var_on_left;

    expr_op = linitial_oid(clause->opnos);
    if (!var_on_left)
        expr_op = get_commutator(expr_op);
    get_op_opfamily_properties(expr_op, index->opfamily[indexcol], false, &op_strategy, &op_lefttype, &op_righttype);

    /* Initialize returned list of which index columns are used */
    *indexcolnos = list_make1_int(indexcol);

    /* Build lists of the opfamilies and operator datatypes in case needed */
    opfamilies = list_make1_oid(index->opfamily[indexcol]);
    lefttypes = list_make1_oid(op_lefttype);
    righttypes = list_make1_oid(op_righttype);

    /*
     * See how many of the remaining columns match some index column in the
     * same way.  As in match_clause_to_indexcol(), the "other" side of any
     * potential index condition is OK as long as it doesn't use Vars from the
     * indexed relation.
     */
    matching_cols = 1;
    largs_cell = lnext(list_head(clause->largs));
    rargs_cell = lnext(list_head(clause->rargs));
    opnos_cell = lnext(list_head(clause->opnos));
    collids_cell = lnext(list_head(clause->inputcollids));

    while (largs_cell != NULL) {
        Node* varop = NULL;
        Node* constop = NULL;
        int i;

        expr_op = lfirst_oid(opnos_cell);
        if (var_on_left) {
            varop = (Node*)lfirst(largs_cell);
            constop = (Node*)lfirst(rargs_cell);
        } else {
            varop = (Node*)lfirst(rargs_cell);
            constop = (Node*)lfirst(largs_cell);
            /* indexkey is on right, so commute the operator */
            expr_op = get_commutator(expr_op);
            if (expr_op == InvalidOid)
                break; /* operator is not usable */
        }
        if (bms_is_member(index->rel->relid, pull_varnos(constop)))
            break; /* no good, Var on wrong side */
        if (contain_volatile_functions(constop))
            break; /* no good, volatile comparison value */

        /*
         * The Var side can match any column of the index.
         */
        for (i = 0; i < index->nkeycolumns; i++) {
            if (match_index_to_operand(varop, i, index) &&
                get_op_opfamily_strategy(expr_op, index->opfamily[i]) == op_strategy &&
                IndexCollMatchesExprColl(index->indexcollations[i], lfirst_oid(collids_cell)))
                break;
        }
        if (i >= index->nkeycolumns)
            break; /* no match found */

        /* Add column number to returned list */
        *indexcolnos = lappend_int(*indexcolnos, i);

        /* Add opfamily and datatypes to lists */
        get_op_opfamily_properties(expr_op, index->opfamily[i], false, &op_strategy, &op_lefttype, &op_righttype);
        opfamilies = lappend_oid(opfamilies, index->opfamily[i]);
        lefttypes = lappend_oid(lefttypes, op_lefttype);
        righttypes = lappend_oid(righttypes, op_righttype);

        /* This column matches, keep scanning */
        matching_cols++;
        largs_cell = lnext(largs_cell);
        rargs_cell = lnext(rargs_cell);
        opnos_cell = lnext(opnos_cell);
        collids_cell = lnext(collids_cell);
    }

    /* Return clause as-is if it's all usable as index quals */
    if (matching_cols == list_length(clause->opnos))
        return (Expr*)clause;

    /*
     * We have to generate a subset rowcompare (possibly just one OpExpr). The
     * painful part of this is changing < to <= or > to >=, so deal with that
     * first.
     */
    if (op_strategy == BTLessEqualStrategyNumber || op_strategy == BTGreaterEqualStrategyNumber) {
        /* easy, just use the same operators */
        new_ops = list_truncate(list_copy(clause->opnos), matching_cols);
    } else {
        ListCell* opfamilies_cell = NULL;
        ListCell* lefttypes_cell = NULL;
        ListCell* righttypes_cell = NULL;

        if (op_strategy == BTLessStrategyNumber)
            op_strategy = BTLessEqualStrategyNumber;
        else if (op_strategy == BTGreaterStrategyNumber)
            op_strategy = BTGreaterEqualStrategyNumber;
        else
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                    (errmsg("unexpected strategy number %d when expand a single indexqual condition", op_strategy))));
        new_ops = NIL;
        lefttypes_cell = list_head(lefttypes);
        righttypes_cell = list_head(righttypes);
        foreach (opfamilies_cell, opfamilies) {
            Oid opfam = lfirst_oid(opfamilies_cell);
            Oid lefttype = lfirst_oid(lefttypes_cell);
            Oid righttype = lfirst_oid(righttypes_cell);

            expr_op = get_opfamily_member(opfam, lefttype, righttype, op_strategy);
            if (!OidIsValid(expr_op)) /* should not happen */
                ereport(ERROR,
                    (errmodule(MOD_OPT),
                        errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                        (errmsg(
                            "when expand a single indexqual condition could not find member %d(%u,%u) of opfamily %u",
                            op_strategy,
                            lefttype,
                            righttype,
                            opfam))));
            if (!var_on_left) {
                expr_op = get_commutator(expr_op);
                if (!OidIsValid(expr_op)) /* should not happen */
                    ereport(ERROR,
                        (errmodule(MOD_OPT),
                            errcode(ERRCODE_INVALID_OPERATION),
                            (errmsg("when expand a single indexqual condition could not find commutator of member "
                                    "%d(%u,%u) of opfamily %u",
                                op_strategy,
                                lefttype,
                                righttype,
                                opfam))));
            }
            new_ops = lappend_oid(new_ops, expr_op);
            lefttypes_cell = lnext(lefttypes_cell);
            righttypes_cell = lnext(righttypes_cell);
        }
    }

    /* If we have more than one matching col, create a subset rowcompare */
    if (matching_cols > 1) {
        RowCompareExpr* rc = makeNode(RowCompareExpr);

        if (var_on_left)
            rc->rctype = (RowCompareType)op_strategy;
        else
            rc->rctype = (op_strategy == BTLessEqualStrategyNumber) ? ROWCOMPARE_GE : ROWCOMPARE_LE;
        rc->opnos = new_ops;
        rc->opfamilies = list_truncate(list_copy(clause->opfamilies), matching_cols);
        rc->inputcollids = list_truncate(list_copy(clause->inputcollids), matching_cols);
        rc->largs = list_truncate((List*)copyObject(clause->largs), matching_cols);
        rc->rargs = list_truncate((List*)copyObject(clause->rargs), matching_cols);
        return (Expr*)rc;
    } else {
        return make_opclause(linitial_oid(new_ops),
            BOOLOID,
            false,
            (Expr*)copyObject(linitial(clause->largs)),
            (Expr*)copyObject(linitial(clause->rargs)),
            InvalidOid,
            linitial_oid(clause->inputcollids));
    }
}


static PadContent get_pad_content(Oid collation)
{
    unsigned long int maxSortCode;
    unsigned long int minSortCode;
    PadContent content;
    switch (collation) {
        case GBK_CHINESE_CI_COLLATION_OID: {
            maxSortCode = 0xA967;
            minSortCode = 0;
            content.minBufLen = 1;
            content.minSortBuf[0] = 0;
            content.maxBufLen = 2;
            content.maxSortBuf[0] = (char)(maxSortCode >> 8);
            content.maxSortBuf[1] = maxSortCode & 0xFF;
            break;
        }
        case GBK_BIN_COLLATION_OID: {
            maxSortCode = 0xFEFE;
            minSortCode = 0;
            content.minBufLen = 1;
            content.minSortBuf[0] = 0;
            content.maxBufLen = 2;
            content.maxSortBuf[0] = (char)(maxSortCode >> 8);
            content.maxSortBuf[1] = maxSortCode & 0xFF;
            break;
        }
        case UTF8_UNICODE_CI_COLLATION_OID:
        case UTF8MB4_UNICODE_CI_COLLATION_OID: {
            maxSortCode = 0xFFFF;
            minSortCode = 0;
            content.minBufLen = 1;
            content.minSortBuf[0] = 9;
            content.maxBufLen = 3;
            content.maxSortBuf[2] = (unsigned char) (0x80 | (maxSortCode & 0x3f));
            maxSortCode = maxSortCode >> 6;
            maxSortCode |= 0x800;
            content.maxSortBuf[1] = (unsigned char) (0x80 | (maxSortCode & 0x3f));
            maxSortCode = maxSortCode >> 6;
            maxSortCode |= 0xc0;
            content.maxSortBuf[0] = (unsigned char)maxSortCode;
            break;
        }
        case UTF8MB4_GENERAL_CI_COLLATION_OID:
        case UTF8MB4_BIN_COLLATION_OID:
        case UTF8_GENERAL_CI_COLLATION_OID:
        case UTF8_BIN_COLLATION_OID: {
            maxSortCode = 0xFFFF;
            minSortCode = 0;
            content.minBufLen = 1;
            content.minSortBuf[0] = 0;
            content.maxBufLen = 3;
            content.maxSortBuf[2] = (unsigned char) (0x80 | (maxSortCode & 0x3f));
            maxSortCode = maxSortCode >> 6;
            maxSortCode |= 0x800;
            content.maxSortBuf[1] = (unsigned char) (0x80 | (maxSortCode & 0x3f));
            maxSortCode = maxSortCode >> 6;
            maxSortCode |= 0xc0;
            content.maxSortBuf[0] = (unsigned char)maxSortCode;
            break;
        }
        case GB18030_CHINESE_CI_COLLATION_OID: {
            maxSortCode = 0xFE39FE39;
            content.minBufLen = 1;
            content.minSortBuf[0] = 0;
            content.maxSortBuf[0] = (maxSortCode >> 24) & 0xFF;
            content.maxSortBuf[1] = (maxSortCode >> 16) & 0xFF;
            content.maxSortBuf[2] = (maxSortCode >> 8) & 0xFF;
            content.maxSortBuf[3] = maxSortCode & 0xFF;
            content.maxBufLen = 4;
            break;
        }
        case GB18030_BIN_COLLATION_OID: {
            maxSortCode = 0xFEFEFEFE;
            content.minBufLen = 1;
            content.minSortBuf[0] = 0;
            content.maxSortBuf[0] = (maxSortCode >> 24) & 0xFF;
            content.maxSortBuf[1] = (maxSortCode >> 16) & 0xFF;
            content.maxSortBuf[2] = (maxSortCode >> 8) & 0xFF;
            content.maxSortBuf[3] = maxSortCode & 0xFF;
            content.maxBufLen = 4;
            break;
        }
        default:
        /* shouldn't get here. */
            ereport(ERROR, (errmodule(MOD_OPT), errcode(ERRCODE_CASE_NOT_FOUND),
                (errmsg(
                    "Invalid collation oid during getting prefix quals in B format"))));
            break;
        }
    return content;
}

static int get_pad_length(Node* leftop, int prefixLen)
{
    int len;
    switch (nodeTag(leftop)) {
        case T_Var: {
            Var *var = (Var *)leftop;
            len = var->vartypmod;
            break;
        }
        case T_RelabelType: {
            RelabelType *expr = (RelabelType *)leftop;
            Var *var = (Var *)expr->arg;
            len = var->vartypmod;
            break;
        }
        default:
            return 0;
    }
    if (len < 0) {
        return prefixLen;
    } else {
        len -= VARHDRSZ;
        if (prefixLen <= 0) {
            return len;
        }
        return (len < prefixLen) ? len : prefixLen;
    }
}

static Const* pad_string_in_like(PadContent content, const Const* strConst, int length, bool isPadMax)
{
    if (length <= 0) {
        return NULL;
    }
    Oid datatype = strConst->consttype;
    char* workstr = NULL;
    workstr = TextDatumGetCString(strConst->constvalue);
    int curLen = strlen(workstr);
    char* buf = isPadMax ? content.maxSortBuf : content.minSortBuf;
    int buflen = isPadMax ? content.maxBufLen : content.minBufLen;
    int padLen = length - pg_mbstrlen_with_len(workstr, curLen);
    /* When the pad length is less than or equal to zero,
     * we don't generate the index condition.
     */
    if (padLen <= 0) {
        return NULL;
    }

    errno_t rc = EOK;
    
    int totalLen = curLen + padLen * buflen;
    char *originstr = (char*)palloc(totalLen + 1);
    rc = memcpy_s(originstr, totalLen, workstr, curLen);
    securec_check(rc, "\0", "\0");
    
    while ((curLen + buflen) <= totalLen) {
        rc = memcpy_s(originstr + curLen, totalLen - curLen, buf, buflen);
        securec_check(rc, "\0", "\0");
        curLen += buflen;
    }
    originstr[curLen] = '\0';
    Const *workstr_const = string_to_const(originstr, datatype);
    pfree(workstr);
    pfree(originstr);
    return workstr_const;
}


/*
 * Given a fixed prefix that all the "leftop" values must have,
 * generate suitable indexqual condition(s).  opfamily is the index
 * operator family; we use it to deduce the appropriate comparison
 * operators and operand datatypes.  collation is the input collation to use.
 */
static List* prefix_quals(Node* leftop, Oid opfamily, Oid collation, Const* prefix_const,
    Pattern_Prefix_Status pstatus, int prefixkey_len)
{
    List* result = NIL;
    Oid datatype;
    Oid oproid;
    Expr* expr = NULL;
    FmgrInfo ltproc;
    Const* greaterstr = NULL;

    AssertEreport(pstatus != Pattern_Prefix_None, MOD_OPT, "Pattern prefix is none");

    switch (opfamily) {
        case TEXT_BTREE_FAM_OID:
        case TEXT_PATTERN_BTREE_FAM_OID:
        case TEXT_SPGIST_FAM_OID:
        case TEXT_UBTREE_FAM_OID:
        case TEXT_PATTERN_UBTREE_FAM_OID:
            datatype = TEXTOID;
            break;

        case BPCHAR_BTREE_FAM_OID:
        case BPCHAR_PATTERN_BTREE_FAM_OID:
        case BPCHAR_UBTREE_FAM_OID:
        case BPCHAR_PATTERN_UBTREE_FAM_OID:
            datatype = BPCHAROID;
            break;

        case NAME_BTREE_FAM_OID:
        case NAME_UBTREE_FAM_OID:
            datatype = NAMEOID;
            break;

        case BYTEA_BTREE_FAM_OID:
        case BYTEA_UBTREE_FAM_OID:
            datatype = BYTEAOID;
            break;

        default:
            if (u_sess->attr.attr_sql.sql_compatibility == B_FORMAT) {
                if (OpfamilynameGetOpfid(BTREE_AM_OID, "binary_ops") == opfamily) {
                    datatype = get_typeoid(PG_CATALOG_NAMESPACE, "binary");
                    break;
                } else if (OpfamilynameGetOpfid(BTREE_AM_OID, "varbinary_ops") == opfamily) {
                    datatype = get_typeoid(PG_CATALOG_NAMESPACE, "varbinary");
                    break;
                }
            }
            /* shouldn't get here */
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                    (errmsg("unexpected opfamily when generate indexqual condition by prefix quals: %u", opfamily))));
            return NIL;
    }

    /*
     * If necessary, coerce the prefix constant to the right type. The given
     * prefix constant is either text or bytea type.
     */
    if (prefix_const->consttype != datatype && (u_sess->attr.attr_sql.sql_compatibility != B_FORMAT
            || (datatype != get_typeoid(PG_CATALOG_NAMESPACE, "binary") &&
            datatype != get_typeoid(PG_CATALOG_NAMESPACE, "varbinary")))) {
        char* prefix = NULL;

        switch (prefix_const->consttype) {
            case TEXTOID:
                prefix = TextDatumGetCString(prefix_const->constvalue);
                break;
            case BYTEAOID:
                prefix = DatumGetCString(DirectFunctionCall1(byteaout, prefix_const->constvalue));
                break;
            default:
                ereport(ERROR,
                    (errmodule(MOD_OPT),
                        errcode(ERRCODE_CASE_NOT_FOUND),
                        (errmsg("unexpected const type when generate indexqual condition by prefix quals: %u",
                            prefix_const->consttype))));
                return NIL;
        }
        prefix_const = string_to_const(prefix, datatype);
        pfree_ext(prefix);
    }
    /*
     * If matched key is prefix key, try generate an "=" indexqual firstly.
     */
    if (prefixkey_len > 0) {
        /* Prefix key is not supported for "name" type, ignore it. */
        int prefix_const_len = (datatype == BYTEAOID) ?
                (int)VARSIZE_ANY_EXHDR(DatumGetPointer(prefix_const->constvalue)) :
                text_length(prefix_const->constvalue);
        if (prefixkey_len <= prefix_const_len) {
            prefix_const = prefix_const_node(prefix_const, prefixkey_len, datatype);
            oproid = get_opfamily_member(opfamily, datatype, datatype, BTEqualStrategyNumber);
            if (oproid == InvalidOid)
                ereport(ERROR,
                    (errmodule(MOD_OPT),
                        errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                        (errmsg("no = operator for opfamily %u when generate indexqual condition by prefix quals",
                            opfamily))));
            expr = make_opclause(oproid, BOOLOID, false, (Expr*)leftop, (Expr*)prefix_const, InvalidOid, collation);
            result = list_make1(make_simple_restrictinfo(expr));
            return result;
        }
    }
    /*
     * If we found an exact-match pattern, generate an "=" indexqual.
     */
    if (pstatus == Pattern_Prefix_Exact) {
        oproid = get_opfamily_member(opfamily, datatype, datatype, BTEqualStrategyNumber);
        if (oproid == InvalidOid)
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                    (errmsg(
                        "no = operator for opfamily %u when generate indexqual condition by prefix quals", opfamily))));
        expr = make_opclause(oproid, BOOLOID, false, (Expr*)leftop, (Expr*)prefix_const, InvalidOid, collation);
        result = list_make1(make_simple_restrictinfo(expr));
        return result;
    }

    /*
     * In B format, we use a more universally applicable approach.
     * We find the maximum and minimum characters sorted in the corresponding collation,
     * and convert the index condition to x >= prefix\min\min... and x <= prefix\max\max.
     * This approach eliminates the need to consider whether the character obtained from make_greater_string is larger,
     * allowing us to use the index with confidence. Currently,
     * this approach is only implemented for B format collations,
     * but we will consider opening it up to all collations in the future.
    */
    if (is_b_format_collation(collation)) {
        int padLen = get_pad_length(leftop, prefixkey_len);
        if (padLen > 0) {
            PadContent content = get_pad_content(collation);
            Const *minstr = NULL;
            oproid = get_opfamily_member(opfamily, datatype, datatype, BTGreaterEqualStrategyNumber);
            if (oproid == InvalidOid)
                ereport(ERROR,
                    (errmodule(MOD_OPT),
                        errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                        (errmsg(
                            "no >= operator for opfamily %u when generate indexqual condition by prefix quals",
                            opfamily))));
            minstr = pad_string_in_like(content, prefix_const, padLen, false);
            if (minstr != NULL) {
                expr = make_opclause(oproid, BOOLOID, false, (Expr*)leftop, (Expr*)minstr, InvalidOid, collation);
                result = lappend(result, make_simple_restrictinfo(expr));
            }
            
            oproid = get_opfamily_member(opfamily, datatype, datatype, BTLessEqualStrategyNumber);
            if (oproid == InvalidOid)
                ereport(ERROR,
                    (errmodule(MOD_OPT),
                        errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                        (errmsg(
                            "no <= operator for opfamily %u when generate indexqual condition by prefix quals",
                            opfamily))));
            greaterstr = pad_string_in_like(content, prefix_const, padLen, true);
            if (greaterstr != NULL) {
                expr = make_opclause(oproid, BOOLOID, false, (Expr*)leftop, (Expr*)greaterstr, InvalidOid, collation);
                result = lappend(result, make_simple_restrictinfo(expr));
            }
        }
    } else {
        /*
        * Otherwise, we have a nonempty required prefix of the values.
        *
        * We can always say "x >= prefix".
        */
        oproid = get_opfamily_member(opfamily, datatype, datatype, BTGreaterEqualStrategyNumber);
        if (oproid == InvalidOid)
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                    (errmsg(
                        "no >= operator for opfamily %u when generate indexqual condition by prefix quals",
                        opfamily))));
        expr = make_opclause(oproid, BOOLOID, false, (Expr*)leftop, (Expr*)prefix_const, InvalidOid, collation);
        result = list_make1(make_simple_restrictinfo(expr));

        /* -------
        * If we can create a string larger than the prefix, we can say
        * "x < greaterstr".  NB: we rely on make_greater_string() to generate
        * a guaranteed-greater string, not just a probably-greater string.
        * In general this is only guaranteed in C locale, so we'd better be
        * using a C-locale index collation.
        * -------
        */

        oproid = get_opfamily_member(opfamily, datatype, datatype, BTLessStrategyNumber);
        if (oproid == InvalidOid)
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                    (errmsg(
                        "no < operator for opfamily %u when generate indexqual condition by prefix quals",
                        opfamily))));
        fmgr_info(get_opcode(oproid), &ltproc);
        greaterstr = make_greater_string(prefix_const, &ltproc, collation);
        if (greaterstr != NULL) {
            expr = make_opclause(oproid, BOOLOID, false, (Expr*)leftop, (Expr*)greaterstr, InvalidOid, collation);
            result = lappend(result, make_simple_restrictinfo(expr));
        }
    }

    return result;
}


static List* prefix_quals_with_encdoing(Node* leftop, Oid opfamily, Oid collation, Const* prefix_const,
    Pattern_Prefix_Status pstatus, int prefixkey_len)
{
    List* result = NULL;
    int tmp_encoding = get_valid_charset_by_collation(prefix_const->constcollid);
    int db_encoding = GetDatabaseEncoding();

    if (tmp_encoding == db_encoding) {
        return prefix_quals(leftop, opfamily, collation, prefix_const, pstatus, prefixkey_len);
    }

    DB_ENCODING_SWITCH_TO(tmp_encoding);
    result = prefix_quals(leftop, opfamily, collation, prefix_const, pstatus, prefixkey_len);
    DB_ENCODING_SWITCH_BACK(db_encoding);
    return result;
}

/*
 * Given a leftop and a rightop, and a inet-family sup/sub operator,
 * generate suitable indexqual condition(s).  expr_op is the original
 * operator, and opfamily is the index opfamily.
 */
static List* network_prefix_quals(Node* leftop, Oid expr_op, Oid opfamily, Datum rightop)
{
    bool is_eq = false;
    Oid datatype;
    Oid opr1oid;
    Oid opr2oid;
    Datum opr1right;
    Datum opr2right;
    List* result = NIL;
    Expr* expr = NULL;

    switch (expr_op) {
        case OID_INET_SUB_OP:
            datatype = INETOID;
            is_eq = false;
            break;
        case OID_INET_SUBEQ_OP:
            datatype = INETOID;
            is_eq = true;
            break;
        default:
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    errcode(ERRCODE_CASE_NOT_FOUND),
                    (errmsg("unexpected operator when generate indexqual condition by network prefix quals: %u",
                        expr_op))));
            return NIL;
    }

    /*
     * create clause "key >= network_scan_first( rightop )", or ">" if the
     * operator disallows equality.
     */
    if (is_eq) {
        opr1oid = get_opfamily_member(opfamily, datatype, datatype, BTGreaterEqualStrategyNumber);
        if (opr1oid == InvalidOid)
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                    (errmsg("no >= operator for opfamily %u when generate indexqual condition by network prefix quals",
                        opfamily))));
    } else {
        opr1oid = get_opfamily_member(opfamily, datatype, datatype, BTGreaterStrategyNumber);
        if (opr1oid == InvalidOid)
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                    (errmsg("no > operator for opfamily %u when generate indexqual condition by network prefix quals",
                        opfamily))));
    }

    opr1right = network_scan_first(rightop);

    expr = make_opclause(opr1oid,
        BOOLOID,
        false,
        (Expr*)leftop,
        (Expr*)makeConst(datatype,
            -1,
            InvalidOid, /* not collatable */
            -1,
            opr1right,
            false,
            false),
        InvalidOid,
        InvalidOid);
    result = list_make1(make_simple_restrictinfo(expr));

    /* create clause "key <= network_scan_last( rightop )" */
    opr2oid = get_opfamily_member(opfamily, datatype, datatype, BTLessEqualStrategyNumber);
    if (opr2oid == InvalidOid)
        ereport(ERROR,
            (errmodule(MOD_OPT),
                errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                (errmsg("no <= operator for opfamily %u when generate indexqual condition by network prefix quals",
                    opfamily))));

    opr2right = network_scan_last(rightop);

    expr = make_opclause(opr2oid,
        BOOLOID,
        false,
        (Expr*)leftop,
        (Expr*)makeConst(datatype,
            -1,
            InvalidOid, /* not collatable */
            -1,
            opr2right,
            false,
            false),
        InvalidOid,
        InvalidOid);
    result = lappend(result, make_simple_restrictinfo(expr));

    return result;
}

/*
 * Handy subroutines for match_special_index_operator() and friends.
 */
/*
 * Generate a Datum of the appropriate type from a C string.
 * Note that all of the supported types are pass-by-ref, so the
 * returned value should be pfree'd if no longer needed.
 */
static Datum string_to_datum(const char* str, Oid datatype)
{
    /*
     * We cheat a little by assuming that CStringGetTextDatum() will do for
     * bpchar and varchar constants too...
     */
    if (datatype == NAMEOID)
        return DirectFunctionCall1(namein, CStringGetDatum(str));
    else if (datatype == BYTEAOID)
        return DirectFunctionCall1(byteain, CStringGetDatum(str));
    else if (datatype == BYTEAWITHOUTORDERWITHEQUALCOLOID)
        return DirectFunctionCall1(byteawithoutorderwithequalcolin, CStringGetDatum(str));
    else if (datatype == BYTEAWITHOUTORDERCOLOID)
        return DirectFunctionCall1(byteawithoutordercolin, CStringGetDatum(str));
    else
        return CStringGetTextDatum(str);
}

/*
 * Generate a Const node of the appropriate type from a C string.
 */
static Const* string_to_const(const char* str, Oid datatype)
{
    Datum conval = string_to_datum(str, datatype);
    Oid collation;
    int constlen;

    /*
     * We only need to support a few datatypes here, so hard-wire properties
     * instead of incurring the expense of catalog lookups.
     */
    switch (datatype) {
        case TEXTOID:
        case VARCHAROID:
        case BPCHAROID:
            collation = DEFAULT_COLLATION_OID;
            constlen = -1;
            break;

        case NAMEOID:
            collation = InvalidOid;
            constlen = NAMEDATALEN;
            break;

        case BYTEAOID:
            collation = InvalidOid;
            constlen = -1;
            break;

        default:
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    errcode(ERRCODE_CASE_NOT_FOUND),
                    (errmsg("unexpected datatype in string_to_const: %u", datatype))));
            return NULL;
    }

    return makeConst(datatype, -1, collation, constlen, conval, false, false);
}

/*
 * get_index_column_prefix_lenth
 *	  Get the prefix length of a specified index column.
 *	  Returns length if it is a prefix key, otherwise returns 0.
 */
static int get_index_column_prefix_lenth(IndexOptInfo *index, int indexcol)
{
    if (index->indexkeys[indexcol] != 0) {
        /* It's a simple index column, not a prefix key. */
        return 0;
    }

    Node* indexkey = NULL;
    ListCell* indexpr_item = list_head(index->indexprs);
    if (indexpr_item == NULL) {
        return 0;
    }

    for (int i = 0; i < indexcol; i++) {
        if (index->indexkeys[i] != 0) {
            continue;
        }

        indexpr_item = lnext(indexpr_item);
        if (indexpr_item == NULL) {
            return 0;
        }
    }

    indexkey = (Node*)lfirst(indexpr_item);
    if (indexkey && IsA(indexkey, RelabelType)) {
        indexkey = (Node*)((RelabelType*)indexkey)->arg;
    }

    if (indexkey && IsA(indexkey, PrefixKey)) {
        return ((PrefixKey*)indexkey)->length;
    }

    return 0;
}

static Const* prefix_const_node(Const* con, int prefix_len, Oid datatype)
{
    int prefix_const_len;
    Datum prefix_value;
    if (datatype == BYTEAOID || datatype == RAWOID || datatype == BLOBOID) {
        /* length of bytes */
        prefix_const_len = VARSIZE_ANY_EXHDR(DatumGetPointer(con->constvalue));
        if (prefix_len < prefix_const_len) {
            prefix_value = PointerGetDatum(bytea_substring(con->constvalue, 1, prefix_len, false));
            return makeConst(datatype, -1, con->constcollid, con->constlen, prefix_value, false, false);
        }
    } else {
        int charset = get_valid_charset_by_collation(con->constcollid);
        /* length of characters */
        prefix_const_len = text_length_with_encoding(con->constvalue, charset);
        if (prefix_len < prefix_const_len) {
            prefix_value = PointerGetDatum(text_substring_with_encoding(con->constvalue, 1, prefix_len, false, charset));
            return makeConst(datatype, -1, con->constcollid, con->constlen, prefix_value, false, false);
        }
    }
    return con;
}

/*
 * rewrite_opclause_for_prefixkey
 *    Rewrite an indexqual for prefix key.
 *
 * The prefix key stores only the prefix of a column. Index scan using the original
 * indexqual may miss some keys we need. The indexqual for index scanning must
 * include all possible keys.
 *
 * For example:
 * If index prefix key length is 3, we cannot use clause (prefixkey > 'ABC123') to
 * approach index key 'ABC'. However, the column value corresponding to this key value
 * may meets the clause. We can find right keys in index scan by converting the
 * clause to (prefixkey >= 'ABC').
 */
static RestrictInfo* rewrite_opclause_for_prefixkey(RestrictInfo *rinfo, IndexOptInfo* index, Oid opfamily,
    int prefix_len)
{
    OpExpr* op = (OpExpr*)rinfo->clause;
    Oid oproid = op->opno;
    Node *leftop = NULL;
    Node *rightop = NULL;
    Node *chgnode = NULL;
    Expr* newop = NULL;
    int strategy;

    /*
     * An index with a prefix key must be a btree index.
     * We only need to rewrite binary OpExpr.
     */
    if (list_length(op->args) != 2) {
        return rinfo;
    }
    leftop = (Node*)linitial(op->args);
    rightop = (Node*)lsecond(op->args);

    /*
     * Check where indexkey is, rewrite the expression on the other side.
     */
    chgnode = (bms_equal(rinfo->left_relids, index->rel->relids)) ? rightop : leftop;

    if (IsA(chgnode, Const)) {
        /*
         * Prefixes the Const if its length is longger than prefix length.
         */
        chgnode = (Node*)prefix_const_node((Const*)chgnode, prefix_len, ((Const*)chgnode)->consttype);
    } else {
        /*
         * Add PrefixeKey node on the expression.
         */
        PrefixKey *pexpr = makeNode(PrefixKey);
        pexpr->length = prefix_len;
        pexpr->arg = (Expr*)chgnode;
        chgnode = (Node*)pexpr;
    }
    if (bms_equal(rinfo->left_relids, index->rel->relids)) {
        rightop = chgnode;
    } else {
        leftop = chgnode;
    }

    /*
     * Operators "> and "<" may cause required keys to be skipped.
     * Replace them with ">=" or "<=".
     */
    strategy = get_op_opfamily_strategy(oproid, opfamily);
    if (strategy == BTGreaterStrategyNumber) {
        oproid = get_opfamily_member(opfamily, exprType(leftop), exprType(rightop), BTGreaterEqualStrategyNumber);
    } else if (strategy == BTLessStrategyNumber) {
        oproid = get_opfamily_member(opfamily, exprType(leftop), exprType(rightop), BTLessEqualStrategyNumber);
    }
    if (oproid == InvalidOid)
        ereport(ERROR,
            (errmodule(MOD_OPT),
                errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                (errmsg(
                    "no >= or <= operator for opfamily %u when generate indexqual for prefix key", opfamily))));
    newop = make_opclause(oproid, BOOLOID, op->opretset, (Expr*)leftop, (Expr*)rightop, op->opcollid, op->inputcollid);

    return make_simple_restrictinfo(newop);
}

/*
 * Check whether there are risks caused by type conversion.
 * If yes, report cause_type.
 */
void check_report_cause_type(FuncExpr* funcExpr, int indkey)
{
    Node* varNode = NULL;
    ListCell* argsCell = NULL;
    if (list_length(funcExpr->args) != 1) {
        return;
    }
    
    argsCell = list_head(funcExpr->args);
    Node* node = (Node*)lfirst(argsCell);
    if (IsA(node, Var)) {
        varNode = node;
    } else if (IsA(node, FuncExpr)) {
        varNode = match_first_var_to_indkey(node, indkey);
    }

    /* Type conversion in g_typeCastFuncOids with only one parameter is supported. */
    if (IsFunctionTransferNumDistinct(funcExpr) && varNode != NULL && IsA(varNode, Var) &&
        indkey == ((Var*)varNode)->varattno) {
        instr_stmt_report_cause_type(NUM_F_TYPECASTING);
    }
}

/*
 * return the first var that matches the index column
 * return NULL if not exist
 */
Node* match_first_var_to_indkey(Node* node, int indkey)
{
    Node* lastNode = NULL;

    List* varList = pull_var_clause(node, PVC_RECURSE_AGGREGATES, PVC_RECURSE_PLACEHOLDERS, PVC_RECURSE_SPECIAL_EXPR);
    if (varList != NULL) {
        ListCell* var_cell = NULL;
        foreach (var_cell, varList) {
            Node* var = (Node*)lfirst(var_cell);
            if (indkey == ((Var*)var)->varattno) {
                lastNode = var;
                break;
            }
        }
    }
    return lastNode;
}

static Node *strip_array_coercion(Node *node)
{
    while (node) {
        if (IsA(node, ArrayCoerceExpr) && ((ArrayCoerceExpr*)node)->elemfuncid == InvalidOid) {
            node = (Node*)((ArrayCoerceExpr*)node)->arg;
        } else if (IsA(node, RelabelType)) {
            node = (Node*)((RelabelType*)node)->arg;
        } else {
            break;
        }
    }
 
    return node;
}
 
/* To avoid error in expand_indexqual_scalar_array_op_expr, we cannot match prefix key with this condition. */
static bool scalar_array_can_match_prefixkey(Node *saop_rexpr)
{
    Node* rexpr = strip_array_coercion(saop_rexpr);
    if (!rexpr) {
        return false;
    }
    if (IsA(rexpr, Const) ||
        (IsA(rexpr, ArrayExpr) && !((ArrayExpr*)rexpr)->multidims)) {
        return true;
    }
 
    /* We cannot rewrite ArrayCoerceExpr and other type node now. */
    return false;
}

Datum get_prefix_datum(Datum src, int prefix_len, Oid datatype)
{
    int datum_len;
    if (datatype == BYTEAOID || datatype == RAWOID || datatype == BLOBOID) {
        /* length of bytes */
        datum_len = VARSIZE_ANY_EXHDR(DatumGetPointer(src));
        if (prefix_len < datum_len) {
            return PointerGetDatum(bytea_substring(src, 1, prefix_len, false));
        }
    } else {
        /* length of characters */
        datum_len = text_length(src);
        if (prefix_len < datum_len) {
            return PointerGetDatum(text_substring(src, 1, prefix_len, false));
        }
    }
    return src;
}

void prefix_array_const_items(Const* arr_const, int prefixkey_len)
{
    ArrayBuildState* astate = NULL;
    Datum arraydatum = arr_const->constvalue;
    ArrayType* arrayval = DatumGetArrayTypeP(arraydatum);
    Oid elem_type = ARR_ELEMTYPE(arrayval);
    int16 typlen;
    bool typbyval;
    char typalign;
    Datum* elem_values = NULL;
    bool* elem_nulls = NULL;
    int num_elems;
 
    get_typlenbyvalalign(elem_type, &typlen, &typbyval, &typalign);
    deconstruct_array(arrayval, elem_type, typlen, typbyval, typalign, &elem_values, &elem_nulls, &num_elems);
 
    /* prefix every item */
    Datum new_item;
    for (int i = 0; i < num_elems; i++) {
        if (elem_nulls[i]) {
            new_item = 0;
        } else {
             new_item = get_prefix_datum(elem_values[i], prefixkey_len, elem_type);
        }
        astate = accumArrayResult(astate, PointerGetDatum(new_item), elem_nulls[i], elem_type, CurrentMemoryContext);
    }
 
    /* Replaced by prefixed array const. */
    arr_const->constvalue = makeArrayResult(astate, CurrentMemoryContext);
    pfree_ext(elem_nulls);
    pfree_ext(elem_values);
}

void prefix_array_expr_items(ArrayExpr* arr_expr, int prefixkey_len)
{
    Node* chgnode = NULL;
 
    foreach_cell(lc, arr_expr->elements) {
        chgnode = (Node*)lfirst(lc);
        if (IsA(chgnode, Const)) {
            chgnode = (Node*)prefix_const_node((Const*)chgnode, prefixkey_len, ((Const*)chgnode)->consttype);
        } else {
            PrefixKey* pexpr = makeNode(PrefixKey);
            pexpr->length = prefixkey_len;
            pexpr->arg = (Expr*)chgnode;
            chgnode = (Node*)pexpr;
        }
 
        lfirst(lc) = chgnode;
    }
}

static Oid replace_operator_by_strategy(Oid op_oid, Oid opfamily, int16 strategy)
{
    Oid ltype;
    Oid rtype;
    get_operator_types(op_oid, &ltype, &rtype);
    return get_opfamily_member(opfamily, ltype, rtype, strategy);
}

RestrictInfo* expand_indexqual_scalar_array_op_expr(IndexOptInfo* index, RestrictInfo* rinfo,
    Oid opfamily, int indexcol)
{
    int prefixkey_len = get_index_column_prefix_lenth(index, indexcol);
    /* Not a prefix key, keep original condition. */
    if (prefixkey_len == 0) {
        return rinfo;
    }
 
    /* Copy condition as new. Rewrite it later. */
    RestrictInfo* new_rinfo = (RestrictInfo*)copyObject((Node*)rinfo);
    ScalarArrayOpExpr* saop = (ScalarArrayOpExpr*)new_rinfo->clause;
    Oid op_oid = saop->opno;
    Node* rexpr = (Node*)lsecond(saop->args); /* 2nd argument of ScalarArrayOpExpr should not null */
 
    /* Skip labals node. */
    rexpr = strip_array_coercion(rexpr);
    if (rexpr && IsA(rexpr, Const)) {
        prefix_array_const_items((Const*)rexpr, prefixkey_len);
    } else if (rexpr && IsA(rexpr, ArrayExpr) && !((ArrayExpr*)rexpr)->multidims) {
        prefix_array_expr_items((ArrayExpr*)rexpr, prefixkey_len);
    } else {
        /* Should not happen. */
        ereport(ERROR, (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
            errmsg("unsupported indexqual type when expand indexqual conditions: %d", (int)nodeTag(saop)),
            errdetail("Cannot rewrite expression %d for prefix key index.", rexpr ? (int)nodeTag(rexpr) : 0)));
    }
 
    /*
     * Operators "> and "<" may cause required keys to be skipped.
     * Replace them with ">=" or "<=".
     */
    int strategy = get_op_opfamily_strategy(op_oid, opfamily);
    if (strategy == BTGreaterStrategyNumber) {
        saop->opno = replace_operator_by_strategy(op_oid, opfamily, BTGreaterStrategyNumber);
    } else if (strategy == BTLessStrategyNumber) {
        saop->opno = replace_operator_by_strategy(op_oid, opfamily, BTLessStrategyNumber);
    }
 
    if (saop->opno == InvalidOid) {
        ereport(ERROR, (errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
            errmsg("no >= or <= operator for opfamily %u when generate indexqual for prefix key", opfamily)));
    }
    saop->opfuncid = get_opcode(saop->opno);
    return new_rinfo;
}
