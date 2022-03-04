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
 * IDENTIFICATION
 *        contrib/gauss_connector/gc_fdw.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "gc_fdw.h"

#include "access/htup.h"
#include "access/sysattr.h"
#include "access/transam.h"
#include "catalog/pg_class.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "commands/vacuum.h"
#include "foreign/fdwapi.h"
#include "funcapi.h"
#include "libpq/pqformat.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/cost.h"
#include "optimizer/clauses.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/var.h"
#include "optimizer/tlist.h"
#include "parser/parsetree.h"
#include "pgxc/pgxcnode.h"
#include "pgxc/pgFdwRemote.h"
#include "storage/buf/block.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/selfuncs.h"

/* Default CPU cost to start up a foreign query. */
#define DEFAULT_FDW_STARTUP_COST 100.0

/* Default CPU cost to process 1 row (above and beyond u_sess->attr.attr_sql.cpu_tuple_cost). */
#define DEFAULT_FDW_TUPLE_COST 0.01

/* If no remote estimates, assume a sort costs 20% extra */
#define DEFAULT_FDW_SORT_MULTIPLIER 1.2

/*
 * Execution state of a foreign scan using gc_fdw.
 */
typedef struct GcFdwScanState {
    Relation rel;             /* relcache entry for the foreign table. NULL
                               * for a foreign join scan. */
    TupleDesc tupdesc;        /* tuple descriptor of scan */
    AttInMetadata* attinmeta; /* attribute datatype conversion metadata */

    /* extracted fdw_private data */
    char* query;           /* text of SELECT command */
    List* retrieved_attrs; /* list of retrieved attribute numbers */

    /* for remote query execution */
    PGconn* conn; /* connection for the scan */
    PGXCNodeAllHandles* pgxc_handle;
    unsigned int cursor_number; /* quasi-unique ID for my cursor */
    bool cursor_exists;         /* have we created the cursor? */
    int numParams;              /* number of parameters passed to query */
    FmgrInfo* param_flinfo;     /* output conversion functions for them */
    List* param_exprs;          /* executable expressions for param values */
    const char** param_values;  /* textual values of query parameters */

    /* for storing result tuples */
    HeapTuple* tuples; /* array of currently-retrieved tuples */
    int num_tuples;    /* # of tuples in array */
    int next_tuple;    /* index of next one to return */

    /* batch-level state, for optimizing rewinds and avoiding useless fetch */
    int fetch_ct_2;   /* Min(# of fetches done, 2) */
    bool eof_reached; /* true if last fetch reached EOF */

    /* working memory contexts */
    MemoryContext batch_cxt; /* context holding current batch of tuples */
    MemoryContext temp_cxt;  /* context for per-tuple temporary data */

    int fetch_size; /* number of tuples per fetch */

    int num_datanode; /* number of execute datanode */

    int current_idx;           /* current index of datanode */
    int cycle_idx;             /* cycle index of datanode */
    int max_idx;               /* max index of datanode */
    bool have_remote_encoding; /* remote encoding */
    int remote_encoding;

    PgFdwRemoteInfo* remoteinfo; /* remote table info */

    IterateForeignScan_function IterateForeignScan;
    VecIterateForeignScan_function VecIterateForeignScan;
    ReScanForeignScan_function ReScanForeignScan;
    Oid serverid;

    bool hasagg; /* agg in sql? */
    bool has_array;

    RemoteQueryState* remotestate;

    TupleTableSlot* resultSlot;
    TupleTableSlot* scanSlot;
} GcFdwScanState;

/*
 * Identify the attribute where data conversion fails.
 */
typedef struct ConversionLocation {
    Relation rel;         /* foreign table's relcache entry. */
    AttrNumber cur_attno; /* attribute number being processed, or 0 */

    /*
     * In case of foreign join push down, fdw_scan_tlist is used to identify
     * the Var node corresponding to the error location and
     * fsstate->ss.ps.state gives access to the RTEs of corresponding relation
     * to get the relation name and attribute name.
     */
    ForeignScanState* fsstate;
} ConversionLocation;

/* Callback argument for ec_member_matches_foreign */
typedef struct {
    Expr* current;      /* current expr, or NULL if not yet found */
    List* already_used; /* expressions already dealt with */
} ec_member_foreign_arg;

/*
 * SQL functions
 */
PG_FUNCTION_INFO_V1(gc_fdw_handler);

/*
 * FDW callback routines
 */
static void gcGetForeignRelSize(PlannerInfo* root, RelOptInfo* baserel, Oid foreigntableid);
static void gcGetForeignPaths(PlannerInfo* root, RelOptInfo* baserel, Oid foreigntableid);
static ForeignScan *gcGetForeignPlan(PlannerInfo *root,
					   RelOptInfo *baserel,
					   Oid foreigntableid,
					   ForeignPath *best_path,
					   List *tlist,
					   List *scan_clauses/*,
					   Plan *outer_plan*/);
static void gcBeginForeignScan(ForeignScanState* node, int eflags);
static TupleTableSlot* gcIterateForeignScan(ForeignScanState* node);
static VectorBatch* gcIterateVecForeignScan(VecForeignScanState* node);
static void gcReScanForeignScan(ForeignScanState* node);
static TupleTableSlot* gcIterateNormalForeignScan(ForeignScanState* node);
static VectorBatch* gcIterateNormalVecForeignScan(VecForeignScanState* node);
static TupleTableSlot* gcIteratePBEForeignScan(ForeignScanState* node);
static VectorBatch* gcIteratePBEVecForeignScan(VecForeignScanState* node);
static void gcReScanNormalForeignScan(ForeignScanState* node);
static void gcReScanPBEForeignScan(ForeignScanState* node);
static void gcEndForeignScan(ForeignScanState* node);
static void gcExplainForeignScan(ForeignScanState* node, ExplainState* es);
static bool gcAnalyzeForeignTable(Relation relation, AcquireSampleRowsFunc* func, BlockNumber* totalpages,
    void* additionalData, bool estimate_table_rownum);

/*
 * Helper functions
 */
static void gc_estimate_path_cost_size(PlannerInfo* root, RelOptInfo* baserel, List* join_conds, List* pathkeys,
    double* p_rows, int* p_width, Cost* p_startup_cost, Cost* p_total_cost);
static void create_cursor(ForeignScanState* node);
static void fetch_more_data(ForeignScanState* node);
static int gcfdw_send_and_fetch_version(PGXCNodeAllHandles* pgxc_handles);
static void gcfdw_fetch_remote_table_info(
    PGXCNodeAllHandles* pgxc_handles, ForeignTable* table, void* remote_info, PgFdwMessageTag tag);
static bool gcfdw_get_table_encode(ForeignTable* table, int* remote_encoding);
static void gcfdw_send_remote_encode(PGXCNodeAllHandles* pgxc_handles, int remote_encoding);
static void gcfdw_send_remote_query_param(GcFdwScanState* fsstate);
static void gcfdw_get_datanode_idx(ForeignScanState* node);
static void close_cursor(PGconn* conn, unsigned int cursor_number);
static void prepare_query_params(PlanState* node, List* fdw_exprs, int numParams, FmgrInfo** param_flinfo,
    List** param_exprs, const char*** param_values);
static void process_query_params(
    ExprContext* econtext, FmgrInfo* param_flinfo, List* param_exprs, const char** param_values);
static HeapTuple make_tuple_from_result_row(PGresult* res, int row, Relation rel, AttInMetadata* attinmeta,
    List* retrieved_attrs, ForeignScanState* fsstate, MemoryContext temp_context);
static HeapTuple make_tuple_from_agg_result(PGresult* res, int row, Relation rel, AttInMetadata* attinmeta,
    List* retrieved_attrs, ForeignScanState* fsstate, MemoryContext temp_context);

static void conversion_error_callback(void* arg);
static void gcValidateTableDef(Node* Obj);
static bool CheckForeignExtOption(List* extOptList, const char* str);
static bool GcFdwCanSkip(GcFdwScanState* fsstate);
static void GcFdwCopyRemoteInfo(PgFdwRemoteInfo* new_remote_info, PgFdwRemoteInfo* ori_remote_info);
extern bool hasSpecialArrayType(TupleDesc desc);
extern Snapshot CopySnapshotByCurrentMcxt(Snapshot snapshot);

/*
 * Foreign-data wrapper handler function: return a struct with pointers
 * to my callback routines.
 */
Datum gc_fdw_handler(PG_FUNCTION_ARGS)
{
    FdwRoutine* routine = makeNode(FdwRoutine);

    /* Functions for scanning foreign tables */
    routine->GetForeignRelSize = gcGetForeignRelSize;
    routine->GetForeignPaths = gcGetForeignPaths;
    routine->GetForeignPlan = gcGetForeignPlan;

    routine->BeginForeignScan = gcBeginForeignScan;
    routine->IterateForeignScan = gcIterateForeignScan;
    routine->VecIterateForeignScan = gcIterateVecForeignScan;
    routine->ReScanForeignScan = gcReScanForeignScan;
    routine->EndForeignScan = gcEndForeignScan;

    /* Support functions for EXPLAIN */
    routine->ExplainForeignScan = gcExplainForeignScan;

    /* Support functions for ANALYZE */
    routine->AnalyzeForeignTable = gcAnalyzeForeignTable;

    /* Check create/alter foreign table */
    routine->ValidateTableDef = gcValidateTableDef;

    PG_RETURN_POINTER(routine);
}

/*
 * gcGetForeignRelSize
 *		Estimate # of rows and width of the result of the scan
 *
 * We should consider the effect of all baserestrictinfo clauses here, but
 * not any join clauses.
 */
static void gcGetForeignRelSize(PlannerInfo* root, RelOptInfo* baserel, Oid foreigntableid)
{
    if (isRestoreMode) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmodule(MOD_COOP_ANALYZE),
                errmsg("cooperation analysis: can't execute the query in restore mode.")));
    }

    if (u_sess->attr.attr_common.upgrade_mode != 0) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmodule(MOD_COOP_ANALYZE),
                errmsg("cooperation analysis: can't execute the query in upgrade mode.")));
    }

    if (IS_PGXC_DATANODE) {
        Relation relation = RelationIdGetRelation(foreigntableid);
        if (RelationIsValid(relation)) {
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmodule(MOD_COOP_ANALYZE),
                    errmsg("Query on datanode is not "
                           "supported currently for the foreign table: %s.",
                        RelationGetRelationName(relation))));
        } else {
            ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_TABLE),
                    errmodule(MOD_COOP_ANALYZE),
                    errmsg("could not open relation with OID %u", foreigntableid)));
        }
    }

    GcFdwRelationInfo* fpinfo = NULL;
    ListCell* lc = NULL;
    RangeTblEntry* rte = planner_rt_fetch(baserel->relid, root);
    const char* relnamespace = NULL;
    const char* relname = NULL;
    const char* refname = NULL;

    /*
     * We use GcFdwRelationInfo to pass various information to subsequent
     * functions.
     */
    fpinfo = (GcFdwRelationInfo*)palloc0(sizeof(GcFdwRelationInfo));
    baserel->fdw_private = (void*)fpinfo;

    /* Base foreign tables need to be pushed down always. */
    fpinfo->pushdown_safe = true;

    /* Look up foreign-table catalog info. */
    fpinfo->table = GetForeignTable(foreigntableid);
    fpinfo->server = GetForeignServer(fpinfo->table->serverid);

    /*
     * Extract user-settable option values.
     */
    fpinfo->fdw_startup_cost = DEFAULT_FDW_STARTUP_COST;
    fpinfo->fdw_tuple_cost = DEFAULT_FDW_TUPLE_COST;
    fpinfo->shippable_extensions = NIL;
    fpinfo->fetch_size = 1000;

    PGXCNodeAllHandles* pgxc_handle = NULL;

    int remote_encoding;
    bool have_remote_encoding = gcfdw_get_table_encode(fpinfo->table, &remote_encoding);

    (void)GetConnection(fpinfo->table->serverid, &pgxc_handle, true, have_remote_encoding);
    PgFdwRemoteInfo* remote_info = &fpinfo->remote_info;

    u_sess->pgxc_cxt.gc_fdw_run_version = gcfdw_send_and_fetch_version(pgxc_handle);
    if (u_sess->pgxc_cxt.gc_fdw_run_version >= GCFDW_VERSION_V1R8C10_1 && have_remote_encoding == true) {
        gcfdw_send_remote_encode(pgxc_handle, remote_encoding);
    }

    gcfdw_fetch_remote_table_info(pgxc_handle, fpinfo->table, remote_info, PGFDW_GET_TABLE_INFO);
    Assert(remote_info->snapsize > 0);
    if (remote_info->snapsize <= 0) {
        ereport(ERROR,
            (errcode(ERRCODE_NO_DATA_FOUND),
                errmsg("cooperation analysis: not receive the snapshot from remote cluster")));
    }

    /*
     * Identify which baserestrictinfo clauses can be sent to the remote
     * server and which can't.
     */
    classifyConditions(root, baserel, baserel->baserestrictinfo, &fpinfo->remote_conds, &fpinfo->local_conds);

    /*
     * Identify which attributes will need to be retrieved from the remote
     * server.  These include all attrs needed for joins or final output, plus
     * all attrs used in the local_conds.  (Note: if we end up using a
     * parameterized scan, it's possible that some of the join clauses will be
     * sent to the remote and thus we wouldn't really need to retrieve the
     * columns used in them.  Doesn't seem worth detecting that case though.)
     */
    fpinfo->attrs_used = NULL;
    pull_varattnos((Node*)baserel->reltargetlist, baserel->relid, &fpinfo->attrs_used);
    foreach (lc, fpinfo->local_conds) {
        RestrictInfo* rinfo = lfirst_node(RestrictInfo, lc);

        pull_varattnos((Node*)rinfo->clause, baserel->relid, &fpinfo->attrs_used);
    }

    /*
     * Compute the selectivity and cost of the local_conds, so we don't have
     * to do it over again for each path.  The best we can do for these
     * conditions is to estimate selectivity on the basis of local statistics.
     */
    fpinfo->local_conds_sel = clauselist_selectivity(root, fpinfo->local_conds, baserel->relid, JOIN_INNER, NULL);

    cost_qual_eval(&fpinfo->local_conds_cost, fpinfo->local_conds, root);

    /*
     * Set cached relation costs to some negative value, so that we can detect
     * when they are set to some sensible costs during one (usually the first)
     * of the calls to gc_estimate_path_cost_size().
     */
    fpinfo->rel_startup_cost = -1;
    fpinfo->rel_total_cost = -1;

    /*
     * estimate using whatever statistics we
     * have locally, in a way similar to ordinary tables.
     */
    {
        /*
         * If the foreign table has never been ANALYZEd, it will have relpages
         * and reltuples equal to zero, which most likely has nothing to do
         * with reality.  We can't do a whole lot about that if we're not
         * allowed to consult the remote server, but we can use a hack similar
         * to plancat.c's treatment of empty relations: use a minimum size
         * estimate of 10 pages, and divide by the column-datatype-based width
         * estimate to get the corresponding number of tuples.
         */
        if (baserel->pages == 0 && baserel->tuples == 0) {
            baserel->pages = 10;
            const int BLOCK_NUM = 10;
            baserel->tuples =
                (double)(BLOCK_NUM * BLCKSZ) / (baserel->width + MAXALIGN(offsetof(HeapTupleHeaderData, t_bits)));
        }

        /* Estimate baserel size as best we can with local statistics. */
        set_baserel_size_estimates(root, baserel);

        /* Fill in basically-bogus cost estimates for use later. */
        gc_estimate_path_cost_size(
            root, baserel, NIL, NIL, &fpinfo->rows, &fpinfo->width, &fpinfo->startup_cost, &fpinfo->total_cost);
    }

    /*
     * Set the name of relation in fpinfo, while we are constructing it here.
     * It will be used to build the string describing the join relation in
     * EXPLAIN output. We can't know whether VERBOSE option is specified or
     * not, so always schema-qualify the foreign table name.
     */
    fpinfo->relation_name = makeStringInfo();
    relnamespace = get_namespace_name(get_rel_namespace(foreigntableid));
    relname = get_rel_name(foreigntableid);
    refname = rte->eref->aliasname;
    const char* quote_relnamespace = (relnamespace != NULL) ? quote_identifier(relnamespace) : "\"Unknown\"";
    const char* quote_relname = (relname != NULL) ? quote_identifier(relname) : "\"Unknown\"";
    appendStringInfo(fpinfo->relation_name, "%s.%s", quote_relnamespace, quote_relname);
    if (*refname && strcmp(refname, relname) != 0)
        appendStringInfo(fpinfo->relation_name, " %s", quote_identifier(rte->eref->aliasname));

    /* No outer and inner relations. */
    fpinfo->make_outerrel_subquery = false;
    fpinfo->make_innerrel_subquery = false;
    fpinfo->lower_subquery_rels = NULL;
    /* Set the relation index. */
    fpinfo->relation_index = baserel->relid;

    fpinfo->reloid = foreigntableid;
}

/*
 * gcGetForeignPaths
 *		Create possible scan paths for a scan on the foreign table
 */
static void gcGetForeignPaths(PlannerInfo* root, RelOptInfo* baserel, Oid foreigntableid)
{
    GcFdwRelationInfo* fpinfo = (GcFdwRelationInfo*)baserel->fdw_private;
    ForeignPath* path = NULL;

    /*
     * Create simplest ForeignScan path node and add it to baserel.  This path
     * corresponds to SeqScan path of regular tables (though depending on what
     * baserestrict conditions we were able to send to remote, there might
     * actually be an indexscan happening there).  We already did all the work
     * to estimate cost and size of this path.
     */
    path = create_foreignscan_path(root,
        baserel,
        fpinfo->startup_cost,
        fpinfo->total_cost,
        NIL,  /* no pathkeys */
        NULL, /* no outer rel either */
        NIL,  /* no fdw_private list */
        u_sess->opt_cxt.query_dop);
    add_path(root, baserel, (Path*)path);
}

/*
 * gcGetForeignPlan
 *		Create ForeignScan plan node which implements selected best path
 */
static ForeignScan* gcGetForeignPlan(PlannerInfo* root, RelOptInfo* foreignrel, Oid foreigntableid,
    ForeignPath* best_path, List* tlist, List* scan_clauses)
{
    GcFdwRelationInfo* fpinfo = (GcFdwRelationInfo*)foreignrel->fdw_private;
    Index scan_relid;
    List* fdw_private = NIL;
    List* remote_exprs = NIL;
    List* local_exprs = NIL;
    List* params_list = NIL;
    List* fdw_scan_tlist = NIL;
    List* fdw_recheck_quals = NIL;
    List* retrieved_attrs = NIL;
    StringInfoData sql;
    ListCell* lc = NULL;

    if (IS_SIMPLE_REL(foreignrel)) {
        /*
         * For base relations, set scan_relid as the relid of the relation.
         */
        scan_relid = foreignrel->relid;

        /*
         * In a base-relation scan, we must apply the given scan_clauses.
         *
         * Separate the scan_clauses into those that can be executed remotely
         * and those that can't.  baserestrictinfo clauses that were
         * previously determined to be safe or unsafe by classifyConditions
         * are found in fpinfo->remote_conds and fpinfo->local_conds. Anything
         * else in the scan_clauses list will be a join clause, which we have
         * to check for remote-safety.
         *
         * Note: the join clauses we see here should be the exact same ones
         * previously examined by gcGetForeignPaths.  Possibly it'd be
         * worth passing forward the classification work done then, rather
         * than repeating it here.
         *
         * This code must match "extract_actual_clauses(scan_clauses, false)"
         * except for the additional decision about remote versus local
         * execution.
         */
        foreach (lc, scan_clauses) {
            RestrictInfo* rinfo = lfirst_node(RestrictInfo, lc);

            /* Ignore any pseudoconstants, they're dealt with elsewhere */
            if (rinfo->pseudoconstant)
                continue;

            if (list_member_ptr(fpinfo->remote_conds, rinfo)) {
                remote_exprs = lappend(remote_exprs, rinfo->clause);
            } else if (list_member_ptr(fpinfo->local_conds, rinfo)) {
                local_exprs = lappend(local_exprs, rinfo->clause);
            } else if (is_foreign_expr(root, foreignrel, rinfo->clause)) {
                remote_exprs = lappend(remote_exprs, rinfo->clause);
            } else {
                local_exprs = lappend(local_exprs, rinfo->clause);
            }
        }

        /*
         * For a base-relation scan, we have to support EPQ recheck, which
         * should recheck all the remote quals.
         */
        fdw_recheck_quals = remote_exprs;
    } else {
        /*
         * Join relation or upper relation - set scan_relid to 0.
         */
        scan_relid = 0;

        /*
         * For a join rel, baserestrictinfo is NIL and we are not considering
         * parameterization right now, so there should be no scan_clauses for
         * a joinrel or an upper rel either.
         */
        Assert(!scan_clauses);

        /*
         * Instead we get the conditions to apply from the fdw_private
         * structure.
         */
        remote_exprs = extract_actual_clauses(fpinfo->remote_conds, false);
        local_exprs = extract_actual_clauses(fpinfo->local_conds, false);

        /*
         * We leave fdw_recheck_quals empty in this case, since we never need
         * to apply EPQ recheck clauses.  In the case of a joinrel, EPQ
         * recheck is handled elsewhere --- see postgresGetForeignJoinPaths().
         * If we're planning an upperrel (ie, remote grouping or aggregation)
         * then there's no EPQ to do because SELECT FOR UPDATE wouldn't be
         * allowed, and indeed we *can't* put the remote clauses into
         * fdw_recheck_quals because the unaggregated Vars won't be available
         * locally.
         */

        /* Build the list of columns to be fetched from the foreign server. */
        fdw_scan_tlist = build_tlist_to_deparse(foreignrel);
    }

    /*
     * Build the query string to be sent for execution, and identify
     * expressions to be sent as parameters.
     */
    initStringInfo(&sql);
    gcDeparseSelectStmtForRel(&sql,
        root,
        foreignrel,
        fdw_scan_tlist,
        remote_exprs,
        best_path->path.pathkeys,
        false,
        &retrieved_attrs,
        &params_list);

    /* Remember remote_exprs for possible use by postgresPlanDirectModify */
    fpinfo->final_remote_exprs = remote_exprs;

    /*
     * Build the fdw_private list that will be available to the executor.
     * Items in the list must match order in enum FdwScanPrivateIndex.
     * index:
     * FdwScanPrivateSelectSql, FdwScanPrivateRetrievedAttrs, FdwScanPrivateFetchSize,
     */
    fdw_private = list_make3(makeString(sql.data), retrieved_attrs, makeInteger(fpinfo->fetch_size));

    /* FdwScanPrivateRemoteInfo */
    PgFdwRemoteInfo* remote_info = makeNode(PgFdwRemoteInfo);
    remote_info->snapsize = fpinfo->remote_info.snapsize;
    remote_info->snapshot = (Snapshot)palloc0(remote_info->snapsize);
    GcFdwCopyRemoteInfo(remote_info, &fpinfo->remote_info);
    fdw_private = lappend(fdw_private, remote_info);

    /* FdwScanPrivateStrTargetlist */
    fdw_private = lappend(fdw_private, NIL);

    /* FdwScanPrivateAggResultTargetlist */
    fdw_private = lappend(fdw_private, NIL);

    /* FdwScanPrivateAggScanTargetlist */
    fdw_private = lappend(fdw_private, NIL);

    /* FdwScanPrivateAggColmap */
    fdw_private = lappend(fdw_private, NIL);

    /* FdwScanPrivateRemoteQuals */
    fdw_private = lappend(fdw_private, remote_exprs);

    /* FdwScanPrivateParamList */
    fdw_private = lappend(fdw_private, params_list);

    if (IS_JOIN_REL(foreignrel))
        fdw_private = lappend(fdw_private, makeString(fpinfo->relation_name->data));

    /*
     * Create the ForeignScan node for the given relation.
     *
     * Note that the remote parameter expressions are stored in the fdw_exprs
     * field of the finished plan node; we can't keep them in private state
     * because then they wouldn't be subject to later planner processing.
     */
    return make_foreignscan(tlist, local_exprs, scan_relid, params_list, fdw_private, EXEC_ON_DATANODES);
}

/*
 * gcBeginForeignScan
 *		Initiate an executor scan of a foreign openGauss table.
 */
static void gcBeginForeignScan(ForeignScanState* node, int eflags)
{
    ForeignScan* fsplan = (ForeignScan*)node->ss.ps.plan;
    EState* estate = node->ss.ps.state;
    GcFdwScanState* fsstate = NULL;
    RangeTblEntry* rte = NULL;
    ForeignTable* table = NULL;
    int rtindex;
    int numParams;

    /*
     * Do nothing in EXPLAIN (no ANALYZE) case.  node->fdw_state stays NULL.
     */
    if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
        return;

    /*
     * We'll save private state in node->fdw_state.
     */
    fsstate = (GcFdwScanState*)palloc0(sizeof(GcFdwScanState));
    node->fdw_state = (void*)fsstate;

    /*
     * Identify which user to do the remote access as.  This should match what
     * ExecCheckRTEPerms() does.  In case of a join or aggregate, use the
     * lowest-numbered member RTE as a representative; we would get the same
     * result from any.
     */
    Assert(fsplan->scan.scanrelid > 0);

    rtindex = fsplan->scan.scanrelid;
    rte = rt_fetch(rtindex, estate->es_range_table);

    /* Get info about foreign table. */
    table = GetForeignTable(rte->relid);

    if (IS_PGXC_COORDINATOR) {
        return;
    }

    /* Get private info created by planner functions. */
    fsstate->query = strVal(list_nth(fsplan->fdw_private, FdwScanPrivateSelectSql));
    fsstate->retrieved_attrs = (List*)list_nth(fsplan->fdw_private, FdwScanPrivateRetrievedAttrs);
    fsstate->fetch_size = intVal(list_nth(fsplan->fdw_private, FdwScanPrivateFetchSize));

    fsstate->remoteinfo = (PgFdwRemoteInfo*)list_nth(fsplan->fdw_private, FdwScanPrivateRemoteInfo);

    /* Create contexts for batches of tuples and per-tuple temp workspace. */
    fsstate->batch_cxt = AllocSetContextCreate(estate->es_query_cxt,
        "gc_fdw tuple data",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);
    fsstate->temp_cxt = AllocSetContextCreate(t_thrd.mem_cxt.msg_mem_cxt,
        "gc_fdw temporary data",
        ALLOCSET_SMALL_MINSIZE,
        ALLOCSET_SMALL_INITSIZE,
        ALLOCSET_SMALL_MAXSIZE);

    /*
     * Get info we'll need for converting data fetched from the foreign server
     * into local representation and error reporting during that process.
     */
    List* agg_result_tlist = (List*)list_nth(fsplan->fdw_private, FdwScanPrivateAggResultTargetlist);

    if (NIL == agg_result_tlist) /* remote sql without agg func */
    {
        fsstate->rel = node->ss.ss_currentRelation;
        fsstate->tupdesc = RelationGetDescr(fsstate->rel);

        TupleDesc scan_desc = CreateTemplateTupleDesc(list_length(fsstate->retrieved_attrs), false);

        TupleTableSlot* slot = node->ss.ss_ScanTupleSlot;

        for (int i = 0; i < list_length(fsstate->retrieved_attrs); i++) {
            int attidx = list_nth_int(fsstate->retrieved_attrs, i);
            Form_pg_attribute attr = slot->tts_tupleDescriptor->attrs[attidx - 1];
            const char* attname = (const char*)(attr->attname.data);
            TupleDescInitEntry(scan_desc, i + 1, attname, attr->atttypid, attr->atttypmod, 0);
        }

        fsstate->scanSlot = MakeSingleTupleTableSlot(scan_desc);
        fsstate->resultSlot = node->ss.ss_ScanTupleSlot;
    } else /* remote sql with agg func */
    {
        fsstate->hasagg = true;

        TupleDesc resultDesc = ExecTypeFromTL(agg_result_tlist, false);

        if (hasSpecialArrayType(resultDesc))
            fsstate->has_array = true;

        ExecAssignResultType(&node->ss.ps, resultDesc);

        node->ss.ps.qual = NIL;
        node->ss.ps.ps_ProjInfo = NULL;

        if (true == fsstate->has_array) {
            List* agg_scan_tlist = (List*)list_nth(fsplan->fdw_private, FdwScanPrivateAggScanTargetlist);
            TupleDesc scanDesc = ExecTypeFromTL(agg_scan_tlist, false);

            ExecSetSlotDescriptor(node->ss.ss_ScanTupleSlot, resultDesc);
            fsstate->tupdesc = scanDesc;

            fsstate->scanSlot = MakeSingleTupleTableSlot(scanDesc);
            fsstate->resultSlot = node->ss.ss_ScanTupleSlot;
        } else {
            ExecSetSlotDescriptor(node->ss.ss_ScanTupleSlot, resultDesc);
            fsstate->tupdesc = resultDesc;

            fsstate->scanSlot = node->ss.ss_ScanTupleSlot;
            fsstate->resultSlot = node->ss.ss_ScanTupleSlot;
        }

        fsstate->rel = NULL;
    }

    for (int i = 0; i < fsstate->resultSlot->tts_tupleDescriptor->natts; i++) {
        fsstate->resultSlot->tts_isnull[i] = true;
    }

    fsstate->resultSlot->tts_isempty = false;
    fsstate->scanSlot->tts_isempty = false;

    fsstate->attinmeta = TupleDescGetAttInMetadata(fsstate->tupdesc);

    /*
     * Prepare for processing of parameters used in remote query, if any.
     */
    numParams = list_length(fsplan->fdw_exprs);
    fsstate->numParams = numParams;
    if (numParams > 0) {
        prepare_query_params((PlanState *)node,
            fsplan->fdw_exprs,
            numParams,
            &fsstate->param_flinfo,
            &fsstate->param_exprs,
            &fsstate->param_values);
    }

    /* get encoding */
    fsstate->have_remote_encoding = gcfdw_get_table_encode(table, &fsstate->remote_encoding);

    /*
     * Get connection to the foreign server.  Connection manager will
     * establish new connection if necessary.
     */
    fsstate->conn = GetConnection(
        table->serverid, &fsstate->pgxc_handle, numParams > 0 ? true : false, fsstate->have_remote_encoding);
    u_sess->pgxc_cxt.gc_fdw_run_version = gcfdw_send_and_fetch_version(fsstate->pgxc_handle);

    fsstate->serverid = table->serverid;

    /* Assign a unique ID for my cursor */
    fsstate->cursor_number = GetCursorNumber(fsstate->conn);
    fsstate->cursor_exists = false;

    if (numParams > 0) {
        fsstate->IterateForeignScan = gcIteratePBEForeignScan;
        fsstate->VecIterateForeignScan = gcIteratePBEVecForeignScan;
        fsstate->ReScanForeignScan = gcReScanPBEForeignScan;
    } else {
        fsstate->IterateForeignScan = gcIterateNormalForeignScan;
        fsstate->VecIterateForeignScan = gcIterateNormalVecForeignScan;
        fsstate->ReScanForeignScan = gcReScanNormalForeignScan;
    }

    /* get datanode information */
    fsstate->num_datanode = fsstate->remoteinfo->datanodenum;

    /* set datanode index */
    gcfdw_get_datanode_idx(node);

    /* send index and snapshot to remote */
    gcfdw_send_remote_query_param(fsstate);
}

/*
 * gcIterateForeignScan
 *		Retrieve next row from the result set, or clear tuple slot to indicate
 *		EOF.
 */
static TupleTableSlot* gcIterateForeignScan(ForeignScanState* node)
{
    if (IS_PGXC_COORDINATOR) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("EXECUTE DIRECT cannot execute SELECT query with foreign table on coordinator")));
    }
    GcFdwScanState* fsstate = (GcFdwScanState*)node->fdw_state;

    return fsstate->IterateForeignScan(node);
}

/*
 * postgresIteratelVecForeignScan
 *		Retrieve next VectorBatch from the result set.
 */
static VectorBatch* gcIterateVecForeignScan(VecForeignScanState* node)
{
    ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Retrieve next vector batch is not supported for Foreign table.")));
    return NULL;
}

/*
 * gcReScanForeignScan
 *		Restart the scan.
 */
static void gcReScanForeignScan(ForeignScanState* node)
{
    if (IS_PGXC_COORDINATOR) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("EXECUTE DIRECT cannot execute SELECT query with foreign table on coordinator")));
    }
    GcFdwScanState* fsstate = (GcFdwScanState*)node->fdw_state;
    return fsstate->ReScanForeignScan(node);
}

static void postgresConstructResultSlotWithArray(ForeignScanState* node)
{
    GcFdwScanState* fsstate = (GcFdwScanState*)node->fdw_state;
    ForeignScan* fsplan = (ForeignScan*)node->ss.ps.plan;

    List* colmap = (List*)list_nth(fsplan->fdw_private, FdwScanPrivateAggColmap);

    TupleTableSlot* resultSlot = fsstate->resultSlot;
    TupleTableSlot* scanSlot = fsstate->scanSlot;

    TupleDesc resultDesc = resultSlot->tts_tupleDescriptor;

    long scanAttr, resultAttr, map;

    for (scanAttr = 0, resultAttr = 0; resultAttr < resultDesc->natts; resultAttr++, scanAttr += map) {
        Assert(list_length(colmap) == resultDesc->natts);

        Oid typoid = resultDesc->attrs[resultAttr]->atttypid;
        Value* val = (Value*)list_nth(colmap, resultAttr);
        map = val->val.ival;

        if (1 == map) /* other type */
        {
            resultSlot->tts_isnull[resultAttr] = scanSlot->tts_isnull[scanAttr];
            resultSlot->tts_values[resultAttr] = scanSlot->tts_values[scanAttr];
        } else /* array type*/
        {
            const int MAX_TRANSDATUMS = 6;
            Datum transdatums[MAX_TRANSDATUMS];
            ArrayType* result = NULL;
            long attrnum, i;

            if (map > MAX_TRANSDATUMS) {
                ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("colmap value is not correct.")));
            }

            if (INT8ARRAYOID == typoid) {
                Assert(2 == map);

                attrnum = scanAttr;
                for (attrnum = scanAttr, i = 0; i < map; i++, attrnum++)
                    transdatums[i] = scanSlot->tts_isnull[attrnum] ? Int64GetDatum(0) : scanSlot->tts_values[attrnum];

                result = construct_array(transdatums, map, INT8OID, 8, true, 's');
            } else if (FLOAT4ARRAYOID == typoid) {
                for (attrnum = scanAttr, i = 0; i < map; i++, attrnum++)
                    transdatums[i] =
                        scanSlot->tts_isnull[attrnum] ? Float4GetDatumFast(0) : scanSlot->tts_values[attrnum];

                result = construct_array(transdatums, map, FLOAT8OID, sizeof(float8), FLOAT8PASSBYVAL, 'd');
            } else if (FLOAT8ARRAYOID == typoid) {
                for (attrnum = scanAttr, i = 0; i < map; i++, attrnum++)
                    transdatums[i] =
                        scanSlot->tts_isnull[attrnum] ? Float8GetDatumFast(0) : scanSlot->tts_values[attrnum];

                result = construct_array(transdatums, map, FLOAT8OID, sizeof(float8), FLOAT8PASSBYVAL, 'd');
            } else if (NUMERICARRAY == typoid) {
                Datum d = Int64GetDatum(0);
                for (attrnum = scanAttr, i = 0; i < map; i++, attrnum++)
                    transdatums[i] = scanSlot->tts_isnull[attrnum] ? DirectFunctionCall1(int8_numeric, d)
                                                                   : scanSlot->tts_values[attrnum];

                result = construct_array(transdatums, map, NUMERICOID, -1, false, 'i');
            } else
                ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH), errmsg("unsupport data type in agg pushdown.")));

            resultSlot->tts_isnull[resultAttr] = false;
            resultSlot->tts_values[resultAttr] = PointerGetDatum(result);
        }
    }

    resultSlot->tts_nvalid = resultDesc->natts;
    resultSlot->tts_isempty = false;
}

static void postgresMapResultFromScanSlot(ForeignScanState* node)
{
    GcFdwScanState* fsstate = (GcFdwScanState*)node->fdw_state;

    if (fsstate->hasagg == false) {
        int j = 0;
        ListCell* lc = NULL;
        foreach (lc, fsstate->retrieved_attrs) {
            int i = lfirst_int(lc);

            fsstate->resultSlot->tts_isnull[i - 1] = fsstate->scanSlot->tts_isnull[j];
            fsstate->resultSlot->tts_values[i - 1] = fsstate->scanSlot->tts_values[j];
            j++;
        }

        fsstate->resultSlot->tts_nvalid = fsstate->resultSlot->tts_tupleDescriptor->natts;
    } else {
        if (false == fsstate->has_array) {
            // do nothing
        } else {
            postgresConstructResultSlotWithArray(node);
        }
    }
}

/*
 * postgresNormalIterateForeignScan
 *		Retrieve next row from the result set, or clear tuple slot to indicate
 *		EOF.
 */
static TupleTableSlot* gcIterateNormalForeignScan(ForeignScanState* node)
{
    GcFdwScanState* fsstate = (GcFdwScanState*)node->fdw_state;

    /* reset tupleslot on the begin */
    (void)ExecClearTuple(fsstate->resultSlot);
    fsstate->resultSlot->tts_isempty = false;

    TupleTableSlot* slot = node->ss.ss_ScanTupleSlot;

    /*
     * If this is the first call after Begin or ReScan, we need to create the
     * cursor on the remote side.
     */
    if (!fsstate->cursor_exists) {
        AutoContextSwitch memGuard(fsstate->batch_cxt);

        fsstate->cursor_exists = true;

        if (GcFdwCanSkip(fsstate)) {
            return ExecClearTuple(slot);
        }

        fsstate->num_tuples = 0;

        pgfdw_send_query(fsstate->pgxc_handle, fsstate->query, &fsstate->remotestate);
    }

    MemoryContextReset(fsstate->temp_cxt);
    MemoryContext oldcontext = MemoryContextSwitchTo(fsstate->temp_cxt);

    if (!PgfdwGetTuples(fsstate->pgxc_handle->dn_conn_count,
            fsstate->pgxc_handle->datanode_handles,
            fsstate->remotestate,
            fsstate->scanSlot)) {
        fsstate->eof_reached = true;
        pgfdw_node_report_error(fsstate->remotestate);
        MemoryContextSwitchTo(oldcontext);
        return NULL;
    }

    fsstate->num_tuples++;
    postgresMapResultFromScanSlot(node);

    pgfdw_node_report_error(fsstate->remotestate);
    MemoryContextSwitchTo(oldcontext);

    return fsstate->resultSlot;
}

/*
 * gcIteratePBEForeignScan
 *		Retrieve next row from the result set, or clear tuple slot to indicate
 *		EOF.
 */
static TupleTableSlot* gcIteratePBEForeignScan(ForeignScanState* node)
{
    GcFdwScanState* fsstate = (GcFdwScanState*)node->fdw_state;
    TupleTableSlot* slot = node->ss.ss_ScanTupleSlot;

    /*
     * If this is the first call after Begin or ReScan, we need to create the
     * cursor on the remote side.
     */
    if (!fsstate->cursor_exists) {
        if (GcFdwCanSkip(fsstate)) {
            return ExecClearTuple(slot);
        }
        create_cursor(node);
    }

    /*
     * Get some more tuples, if we've run out.
     */
    if (fsstate->next_tuple >= fsstate->num_tuples) {
        /* No point in another fetch if we already detected EOF, though. */
        if (!fsstate->eof_reached) {
            fetch_more_data(node);
        }

        /* If we didn't get any tuples, must be end of data. */
        if (fsstate->next_tuple >= fsstate->num_tuples) {
            return ExecClearTuple(slot);
        }
    }

    HeapTuple tuple = fsstate->tuples[fsstate->next_tuple++];

    if (fsstate->hasagg && fsstate->has_array) {
        MemoryContext old = CurrentMemoryContext;
        MemoryContextSwitchTo(fsstate->batch_cxt);

        TupleDesc scanDesc = fsstate->scanSlot->tts_tupleDescriptor;
        TupleDesc resultDesc = fsstate->resultSlot->tts_tupleDescriptor;

        heap_deform_tuple(tuple, scanDesc, fsstate->scanSlot->tts_values, fsstate->scanSlot->tts_isnull);
        postgresMapResultFromScanSlot(node);
        tuple = heap_form_tuple(resultDesc, fsstate->resultSlot->tts_values, fsstate->resultSlot->tts_isnull);

        MemoryContextSwitchTo(old);
    }

    /*
     * Return the next tuple.
     */
    (void)ExecStoreTuple(tuple, slot, InvalidBuffer, false);

    return slot;
}

/*
 * gcIterateNormalVecForeignScan
 *		Retrieve next VectorBatch from the result set.
 */
static VectorBatch* gcIterateNormalVecForeignScan(VecForeignScanState* node)
{
    GcFdwScanState* fsstate = (GcFdwScanState*)node->fdw_state;
    TupleTableSlot* slot = node->ss.ss_ScanTupleSlot;
    VectorBatch* batch = node->m_pScanBatch;
    Datum* values = node->m_values;
    bool* nulls = node->m_nulls;
    MemoryContext scanMcxt = node->scanMcxt;

    MemoryContextReset(fsstate->temp_cxt);
    MemoryContext oldcontext = MemoryContextSwitchTo(fsstate->temp_cxt);

    /*
     * If this is the first call after Begin or ReScan, we need to create the
     * cursor on the remote side.
     */
    if (!fsstate->cursor_exists) {
        AutoContextSwitch memGuard(fsstate->batch_cxt);

        fsstate->cursor_exists = true;
        if (GcFdwCanSkip(fsstate)) {
            return NULL;
        }

        fsstate->num_tuples = 0;

        pgfdw_send_query(fsstate->pgxc_handle, fsstate->query, &fsstate->remotestate);
    }

    MemoryContextSwitchTo(oldcontext);

    MemoryContextReset(scanMcxt);
    oldcontext = MemoryContextSwitchTo(scanMcxt);

    /* init vectorbatch */
    batch->Reset(true);

    /* get vectorbatch from tuple */
    for (batch->m_rows = 0; batch->m_rows < BatchMaxSize; batch->m_rows++) {
        for (int i = 0; i < slot->tts_tupleDescriptor->natts; i++) {
            slot->tts_isnull[i] = true;
        }

        if (!PgfdwGetTuples(fsstate->pgxc_handle->dn_conn_count,
                fsstate->pgxc_handle->datanode_handles,
                fsstate->remotestate,
                fsstate->scanSlot)) {
            fsstate->eof_reached = true;
            pgfdw_node_report_error(fsstate->remotestate);
            break;
        }

        pgfdw_node_report_error(fsstate->remotestate);

        postgresMapResultFromScanSlot(node);

        values = fsstate->resultSlot->tts_values;
        nulls = fsstate->resultSlot->tts_isnull;

        int rows = batch->m_rows;
        for (int i = 0; i < batch->m_cols; i++) {
            ScalarVector* vec = &(batch->m_arr[i]);
            if (nulls[i]) {
                vec->m_rows++;
                vec->SetNull(rows);
                continue;
            }

            if (vec->m_desc.encoded)
                vec->AddVar(values[i], rows);
            else
                vec->m_vals[rows] = values[i];

            vec->m_rows++;
        }

        fsstate->next_tuple++;
        if (fsstate->next_tuple >= fsstate->num_tuples) {
            batch->m_rows++;
            break;
        }
    }

    MemoryContextSwitchTo(oldcontext);

    return batch;
}

/*
 * gcIteratePBEVecForeignScan
 *      Retrieve next VectorBatch from the result set.
 */
static VectorBatch* gcIteratePBEVecForeignScan(VecForeignScanState* node)
{
    GcFdwScanState* fsstate = (GcFdwScanState*)node->fdw_state;
    VectorBatch* batch = node->m_pScanBatch;
    Datum* values = node->m_values;
    bool* nulls = node->m_nulls;
    MemoryContext scanMcxt = node->scanMcxt;
    TupleDesc tupdesc = fsstate->tupdesc;
    MemoryContext oldMemoryContext = NULL;

    /*
     * If this is the first call after Begin or ReScan, we need to create the
     * cursor on the remote side.
     */
    if (!fsstate->cursor_exists) {
        if (GcFdwCanSkip(fsstate)) {
            return NULL;
        }
        create_cursor(node);
    }

    /*
     * Get some more tuples, if we've run out.
     */
    if (fsstate->next_tuple >= fsstate->num_tuples) {
        /* No point in another fetch if we already detected EOF, though. */
        if (!fsstate->eof_reached)
            fetch_more_data(node);
        /* If we didn't get any tuples, must be end of data. */
        if (fsstate->next_tuple >= fsstate->num_tuples) {
            return NULL;
        }
    }

    MemoryContextReset(scanMcxt);
    oldMemoryContext = MemoryContextSwitchTo(scanMcxt);

    /* init vectorbatch */
    batch->Reset(true);

    /* get vectorbatch from tuple */
    for (batch->m_rows = 0; batch->m_rows < BatchMaxSize; batch->m_rows++) {
        HeapTuple tuple = fsstate->tuples[fsstate->next_tuple];

        if (fsstate->hasagg && fsstate->has_array) {
            TupleDesc scanDesc = fsstate->scanSlot->tts_tupleDescriptor;
            TupleDesc resultDesc = fsstate->resultSlot->tts_tupleDescriptor;

            heap_deform_tuple(tuple, scanDesc, fsstate->scanSlot->tts_values, fsstate->scanSlot->tts_isnull);
            postgresMapResultFromScanSlot(node);
            tuple = heap_form_tuple(resultDesc, fsstate->resultSlot->tts_values, fsstate->resultSlot->tts_isnull);
        }

        heap_deform_tuple(tuple, tupdesc, values, nulls);

        int rows = batch->m_rows;
        for (int i = 0; i < batch->m_cols; i++) {
            ScalarVector* vec = &(batch->m_arr[i]);
            if (nulls[i]) {
                vec->m_rows++;
                vec->SetNull(rows);
                continue;
            }

            if (vec->m_desc.encoded)
                vec->AddVar(values[i], rows);
            else
                vec->m_vals[rows] = values[i];

            vec->m_rows++;
        }

        fsstate->next_tuple++;
        if (fsstate->next_tuple >= fsstate->num_tuples) {
            batch->m_rows++;
            break;
        }
    }

    MemoryContextSwitchTo(oldMemoryContext);

    return batch;
}

/*
 * gcReScanNormalForeignScan
 *		Restart the scan.
 */
static void gcReScanNormalForeignScan(ForeignScanState* node)
{
    GcFdwScanState* fsstate = (GcFdwScanState*)node->fdw_state;

    /* If we haven't created the cursor yet, nothing to do. */
    if (!fsstate->cursor_exists)
        return;

    fsstate->cursor_exists = false;

    // recontected
    DirectReleaseConnection(fsstate->conn, fsstate->pgxc_handle, false);

    fsstate->conn = GetConnection(fsstate->serverid, &fsstate->pgxc_handle, false, fsstate->have_remote_encoding);
    gcfdw_send_remote_query_param(fsstate);

    /* Now force a fresh FETCH. */
    fsstate->tuples = NULL;
    fsstate->num_tuples = 0;
    fsstate->next_tuple = 0;
    fsstate->eof_reached = false;
}

/*
 * gcReScanPBEForeignScan
 *		Restart the scan.
 */
static void gcReScanPBEForeignScan(ForeignScanState* node)
{
    GcFdwScanState* fsstate = (GcFdwScanState*)node->fdw_state;
    char sql[64] = {0};
    PGresult* res = NULL;

    /* If we haven't created the cursor yet, nothing to do. */
    if (!fsstate->cursor_exists)
        return;

    fsstate->cursor_exists = false;
    errno_t rc = snprintf_s(sql, sizeof(sql), sizeof(sql) - 1, "CLOSE c%u", fsstate->cursor_number);
    securec_check_ss(rc, "", "");

    // recontected
    DirectReleaseConnection(fsstate->conn, fsstate->pgxc_handle, true);

    fsstate->conn = GetConnection(fsstate->serverid, &fsstate->pgxc_handle, true, fsstate->have_remote_encoding);
    gcfdw_send_remote_query_param(fsstate);

    /*
     * We don't use a PG_TRY block here, so be careful not to throw error
     * without releasing the PGresult.
     */
    res = pgfdw_exec_query(fsstate->conn, sql);
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        pgfdw_report_error(ERROR, res, fsstate->conn, true, sql);
    }
    PQclear(res);

    /* Now force a fresh FETCH. */
    fsstate->tuples = NULL;
    fsstate->num_tuples = 0;
    fsstate->next_tuple = 0;
    fsstate->eof_reached = false;
}

/*
 * gcEndForeignScan
 *		Finish scanning foreign table and dispose objects used for this scan
 */
static void gcEndForeignScan(ForeignScanState* node)
{
    if (IS_PGXC_COORDINATOR) {
        return;
    }

    GcFdwScanState* fsstate = (GcFdwScanState*)node->fdw_state;

    /* if fsstate is NULL, we are in EXPLAIN; nothing to do */
    if (fsstate == NULL)
        return;

    /* Close the cursor if open, to prevent accumulation of cursors */
    if (fsstate->cursor_exists && fsstate->numParams > 0) {
        close_cursor(fsstate->conn, fsstate->cursor_number);
        DirectReleaseConnection(fsstate->conn, fsstate->pgxc_handle, true);
    } else {
        DirectReleaseConnection(fsstate->conn, fsstate->pgxc_handle, false);
    }

    /* MemoryContexts will be deleted automatically. */
}

/*
 * gcExplainForeignScan
 *		Produce extra output for EXPLAIN of a ForeignScan on a foreign table
 */
static void gcExplainForeignScan(ForeignScanState* node, ExplainState* es)
{
    List* fdw_private = NIL;
    char* sql = NULL;

    fdw_private = ((ForeignScan*)node->ss.ps.plan)->fdw_private;

    /*
     * Add remote query, when VERBOSE option is specified.
     */
    if (es->verbose) {
        sql = strVal(list_nth(fdw_private, FdwScanPrivateSelectSql));

        if (t_thrd.explain_cxt.explain_perf_mode != EXPLAIN_NORMAL && es->planinfo->m_verboseInfo) {
            appendStringInfoSpaces(es->planinfo->m_verboseInfo->info_str, 8);
            appendStringInfo(es->planinfo->m_verboseInfo->info_str, "Remote SQL: %s\n", sql);
        } else {
            ExplainPropertyText("Remote SQL", sql, es);
        }
    }
}

static void GetJoinRelCostInfo(
    const GcFdwRelationInfo *fpinfo, double retrieved_rows, PlannerInfo *root, Cost *startup_cost, Cost *run_cost)
{
    GcFdwRelationInfo *fpinfo_i = NULL;
    GcFdwRelationInfo *fpinfo_o = NULL;
    QualCost join_cost;
    QualCost remote_conds_cost;
    double nrows;

    /* For join we expect inner and outer relations set */
    Assert(fpinfo->innerrel && fpinfo->outerrel);
    fpinfo_i = (GcFdwRelationInfo *)fpinfo->innerrel->fdw_private;
    fpinfo_o = (GcFdwRelationInfo *)fpinfo->outerrel->fdw_private;

    /* Estimate of number of rows in cross product */
    nrows = fpinfo_i->rows * fpinfo_o->rows;
    /* Clamp retrieved rows estimate to at most size of cross product */
    retrieved_rows = Min(retrieved_rows, nrows);

    /*
     * The cost of foreign join is estimated as cost of generating
     * rows for the joining relations + cost for applying quals on the
     * rows.
     */

    /* Calculate the cost of clauses pushed down to the foreign server */
    cost_qual_eval(&remote_conds_cost, fpinfo->remote_conds, root);
    /* Calculate the cost of applying join clauses */
    cost_qual_eval(&join_cost, fpinfo->joinclauses, root);

    /*
     * Startup cost includes startup cost of joining relations and the
     * startup cost for join and other clauses. We do not include the
     * startup cost specific to join strategy (e.g. setting up hash
     * tables) since we do not know what strategy the foreign server
     * is going to use.
     */
    *startup_cost = fpinfo_i->rel_startup_cost + fpinfo_o->rel_startup_cost;
    *startup_cost += join_cost.startup;
    *startup_cost += remote_conds_cost.startup;
    *startup_cost += fpinfo->local_conds_cost.startup;

    /*
     * Run time cost includes:
     *
     * 1. Run time cost (total_cost - startup_cost) of relations being
     * joined
     *
     * 2. Run time cost of applying join clauses on the cross product
     * of the joining relations.
     *
     * 3. Run time cost of applying pushed down other clauses on the
     * result of join
     *
     * 4. Run time cost of applying nonpushable other clauses locally
     * on the result fetched from the foreign server.
     */
    *run_cost = fpinfo_i->rel_total_cost - fpinfo_i->rel_startup_cost;
    *run_cost += fpinfo_o->rel_total_cost - fpinfo_o->rel_startup_cost;
    *run_cost += nrows * join_cost.per_tuple;
    nrows = clamp_row_est(nrows * fpinfo->joinclause_sel);
    *run_cost += nrows * remote_conds_cost.per_tuple;
    *run_cost += fpinfo->local_conds_cost.per_tuple * retrieved_rows;
}

/*
 * gc_estimate_path_cost_size
 *		Get cost and size estimates for a foreign scan on given foreign relation
 *		either a base relation or a join between foreign relations or an upper
 *		relation containing foreign relations.
 *
 * param_join_conds are the parameterization clauses with outer relations.
 * pathkeys specify the expected sort order if any for given path being costed.
 *
 * The function returns the cost and size estimates in p_row, p_width,
 * p_startup_cost and p_total_cost variables.
 */
static void gc_estimate_path_cost_size(PlannerInfo* root, RelOptInfo* foreignrel, List* param_join_conds,
    List* pathkeys, double* p_rows, int* p_width, Cost* p_startup_cost, Cost* p_total_cost)
{
    GcFdwRelationInfo* fpinfo = (GcFdwRelationInfo*)foreignrel->fdw_private;
    double rows;
    double retrieved_rows;
    int width;
    Cost startup_cost;
    Cost total_cost;
    Cost cpu_per_tuple;

    /*
     * estimate rows using whatever statistics we have locally, in a way
     * similar to ordinary tables.
     */
    {
        Cost run_cost = 0;
        /*
         * We don't support join conditions in this mode (hence, no
         * parameterized paths can be made).
         */
        Assert(param_join_conds == NIL);

        /*
         * Use rows/width estimates made by set_baserel_size_estimates() for
         * base foreign relations and set_joinrel_size_estimates() for join
         * between foreign relations.
         */
        rows = foreignrel->rows;
        width = foreignrel->width;

        /* Back into an estimate of the number of retrieved rows. */
        retrieved_rows = clamp_row_est(rows / fpinfo->local_conds_sel);

        /*
         * We will come here again and again with different set of pathkeys
         * that caller wants to cost. We don't need to calculate the cost of
         * bare scan each time. Instead, use the costs if we have cached them
         * already.
         */
        if (fpinfo->rel_startup_cost > 0 && fpinfo->rel_total_cost > 0) {
            startup_cost = fpinfo->rel_startup_cost;
            run_cost = fpinfo->rel_total_cost - fpinfo->rel_startup_cost;
        } else if (IS_JOIN_REL(foreignrel)) {
            GetJoinRelCostInfo(fpinfo, retrieved_rows, root, &startup_cost, &run_cost);
        } else {
            /* Clamp retrieved rows estimates to at most foreignrel->tuples. */
            retrieved_rows = Min(retrieved_rows, foreignrel->tuples);

            /*
             * Cost as though this were a seqscan, which is pessimistic.  We
             * effectively imagine the local_conds are being evaluated
             * remotely, too.
             */
            startup_cost = 0;
            run_cost = 0;
            run_cost += u_sess->attr.attr_sql.seq_page_cost * foreignrel->pages;

            startup_cost += foreignrel->baserestrictcost.startup;
            cpu_per_tuple = u_sess->attr.attr_sql.cpu_tuple_cost + foreignrel->baserestrictcost.per_tuple;
            run_cost += cpu_per_tuple * foreignrel->tuples;
        }

        /*
         * Without remote estimates, we have no real way to estimate the cost
         * of generating sorted output.  It could be free if the query plan
         * the remote side would have chosen generates properly-sorted output
         * anyway, but in most cases it will cost something.  Estimate a value
         * high enough that we won't pick the sorted path when the ordering
         * isn't locally useful, but low enough that we'll err on the side of
         * pushing down the ORDER BY clause when it's useful to do so.
         */
        if (pathkeys != NIL) {
            startup_cost *= DEFAULT_FDW_SORT_MULTIPLIER;
            run_cost *= DEFAULT_FDW_SORT_MULTIPLIER;
        }

        total_cost = startup_cost + run_cost;
    }
    /*
     * Cache the costs for scans without any pathkeys or parameterization
     * before adding the costs for transferring data from the foreign server.
     * These costs are useful for costing the join between this relation and
     * another foreign relation or to calculate the costs of paths with
     * pathkeys for this relation, when the costs can not be obtained from the
     * foreign server. This function will be called at least once for every
     * foreign relation without pathkeys and parameterization.
     */
    if (pathkeys == NIL && param_join_conds == NIL) {
        fpinfo->rel_startup_cost = startup_cost;
        fpinfo->rel_total_cost = total_cost;
    }

    /*
     * Add some additional cost factors to account for connection overhead
     * (fdw_startup_cost), transferring data across the network
     * (fdw_tuple_cost per retrieved row), and local manipulation of the data
     * (cpu_tuple_cost per retrieved row).
     */
    startup_cost += fpinfo->fdw_startup_cost;
    total_cost += fpinfo->fdw_startup_cost;
    total_cost += fpinfo->fdw_tuple_cost * retrieved_rows;
    total_cost += u_sess->attr.attr_sql.cpu_tuple_cost * retrieved_rows;

    /* Return results. */
    *p_rows = rows;
    *p_width = width;
    *p_startup_cost = startup_cost;
    *p_total_cost = total_cost;
}

/*
 * Create cursor for node's query with current parameter values.
 */
static void create_cursor(ForeignScanState* node)
{
    GcFdwScanState* fsstate = (GcFdwScanState*)node->fdw_state;
    ExprContext* econtext = node->ss.ps.ps_ExprContext;
    int numParams = fsstate->numParams;
    const char** values = fsstate->param_values;
    PGconn* conn = fsstate->conn;
    StringInfoData buf;
    PGresult* res = NULL;

    /*
     * Construct array of query parameter values in text format.  We do the
     * conversions in the short-lived per-tuple context, so as not to cause a
     * memory leak over repeated scans.
     */
    if (numParams > 0) {
        MemoryContext oldcontext = NULL;

        oldcontext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);

        process_query_params(econtext, fsstate->param_flinfo, fsstate->param_exprs, values);

        MemoryContextSwitchTo(oldcontext);
    }

    /* Construct the DECLARE CURSOR command */
    initStringInfo(&buf);
    appendStringInfo(&buf, "DECLARE c%u CURSOR FOR\n%s", fsstate->cursor_number, fsstate->query);

    /*
     * Notice that we pass NULL for paramTypes, thus forcing the remote server
     * to infer types for all parameters.  Since we explicitly cast every
     * parameter (see deparse.c), the "inference" is trivial and will produce
     * the desired result.  This allows us to avoid assuming that the remote
     * server has the same OIDs we do for the parameters' types.
     */
    res = PQexecParams(conn, buf.data, numParams, NULL, values, NULL, NULL, 0);
    if (res == NULL) {
        pgfdw_report_error(ERROR, NULL, conn, false, buf.data);
    }

    /*
     * Get the result, and check for success.
     *
     * We don't use a PG_TRY block here, so be careful not to throw error
     * without releasing the PGresult.
     */
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        pgfdw_report_error(ERROR, res, conn, true, fsstate->query);
    }
    PQclear(res);

    /* Mark the cursor as created, and show no tuples have been retrieved */
    fsstate->cursor_exists = true;
    fsstate->tuples = NULL;
    fsstate->num_tuples = 0;
    fsstate->next_tuple = 0;
    fsstate->eof_reached = false;

    /* Clean up */
    pfree(buf.data);
}

/*
 * Fetch some more rows from the node's cursor.
 */
static void fetch_more_data(ForeignScanState* node)
{
    GcFdwScanState* fsstate = (GcFdwScanState*)node->fdw_state;
    PGresult* volatile res = NULL;
    MemoryContext oldcontext = NULL;

    /*
     * We'll store the tuples in the batch_cxt.  First, flush the previous
     * batch.
     */
    fsstate->tuples = NULL;
    MemoryContextReset(fsstate->batch_cxt);
    oldcontext = MemoryContextSwitchTo(fsstate->batch_cxt);

    /* PGresult must be released before leaving this function. */
    PG_TRY();
    {
        PGconn* conn = fsstate->conn;
        char sql[64] = {0};
        int numrows;
        int i;

        errno_t rc = snprintf_s(
            sql, sizeof(sql), sizeof(sql) - 1, "FETCH %d FROM c%u", fsstate->fetch_size, fsstate->cursor_number);
        securec_check_ss(rc, "", "");

        res = pgfdw_exec_query(conn, sql);
        /* On error, report the original query, not the FETCH. */
        if (PQresultStatus(res) != PGRES_TUPLES_OK) {
            pgfdw_report_error(ERROR, res, conn, false, fsstate->query);
        }

        /* Convert the data into HeapTuples */
        numrows = PQntuples(res);
        if (numrows == 0) {
            fsstate->tuples = NULL;
        } else {
            fsstate->tuples = (HeapTuple*)palloc0(numrows * sizeof(HeapTuple));
        }
        fsstate->num_tuples = numrows;
        fsstate->next_tuple = 0;

        for (i = 0; i < numrows; i++) {
            Assert(IsA(node->ss.ps.plan, ForeignScan) || IsA(node->ss.ps.plan, VecForeignScan));

            if (!fsstate->hasagg) {
                fsstate->tuples[i] = make_tuple_from_result_row(
                    res, i, fsstate->rel, fsstate->attinmeta, fsstate->retrieved_attrs, node, fsstate->temp_cxt);
            } else {
                fsstate->tuples[i] = make_tuple_from_agg_result(
                    res, i, fsstate->rel, fsstate->attinmeta, fsstate->retrieved_attrs, node, fsstate->temp_cxt);
            }
        }

        /* Must be EOF if we didn't get as many tuples as we asked for. */
        fsstate->eof_reached = (numrows < fsstate->fetch_size);

        PQclear(res);
        res = NULL;
    }
    PG_CATCH();
    {
        if (res != NULL) {
            PQclear(res);
        }
        PG_RE_THROW();
    }
    PG_END_TRY();

    MemoryContextSwitchTo(oldcontext);
}

/*
 * Fetch remote version
 */
void pgfdw_fetch_remote_version(PGconn* conn)
{
    PGresult* volatile res = NULL;

    /* PGresult must be released before leaving this function. */
    PG_TRY();
    {
        char sql[64] = {0};
        int numrows;
        int result_count = 0;

        errno_t rc =
            snprintf_s(sql, sizeof(sql), sizeof(sql) - 1, "SELECT count(1) FROM pg_extension WHERE extname='gc_fdw';");
        securec_check_ss(rc, "", "");

        res = pgfdw_exec_query(conn, sql);
        /* On error, report the original query, not the FETCH. */
        if (PQresultStatus(res) != PGRES_TUPLES_OK)
            pgfdw_report_error(ERROR, res, conn, false, NULL);

        /* Convert the data into HeapTuples */
        numrows = PQntuples(res);

        result_count = atoi(PQgetvalue(res, 0, 0));
        if (result_count <= 0) {
            ereport(ERROR, (errcode(ERRCODE_NO_DATA_FOUND), errmsg("gc_fdw: the remote vesion is not match.")));
        }

        PQclear(res);
        res = NULL;
    }
    PG_CATCH();
    {
        if (res != NULL)
            PQclear(res);
        PG_RE_THROW();
    }
    PG_END_TRY();
}

/*
 * Fetch remote version.
 */
static int gcfdw_send_and_fetch_version(PGXCNodeAllHandles* pgxc_handles)
{
    StringInfoData retbuf;
    initStringInfo(&retbuf);

    pq_sendint(&retbuf, GCFDW_VERSION, 4);

    PgFdwRemoteSender(pgxc_handles, retbuf.data, retbuf.len, PGFDW_GET_VERSION);

    int version = 0;
    PgFdwRemoteReceiver(pgxc_handles, &version, sizeof(PgFdwRemoteInfo));
    Assert(version >= GCFDW_VERSION_V1R8C10);

    pfree(retbuf.data);
    retbuf.data = NULL;

    return version;
}

/*
 * Fetch remote table information.
 */
static void gcfdw_fetch_remote_table_info(
    PGXCNodeAllHandles* pgxc_handles, ForeignTable* table, void* remote_info, PgFdwMessageTag tag)
{
    const char* nspname = NULL;
    const char* relname = NULL;
    ListCell* lc = NULL;

    /*
     * Use value of FDW options if any, instead of the name of object itself.
     */
    foreach (lc, table->options) {
        DefElem* def = (DefElem*)lfirst(lc);

        if (strcmp(def->defname, "schema_name") == 0)
            nspname = defGetString(def);
        else if (strcmp(def->defname, "table_name") == 0)
            relname = defGetString(def);
    }

    Relation rel = heap_open(table->relid, AccessShareLock);

    if (nspname == NULL)
        nspname = get_namespace_name(RelationGetNamespace(rel));
    if (relname == NULL)
        relname = RelationGetRelationName(rel);

    Assert(nspname != NULL && relname != NULL);

    int nsplen = strlen(nspname);
    int rellen = strlen(relname);

    /*
     * write scheme name and table name.
     */
    StringInfoData retbuf;
    initStringInfo(&retbuf);

    pq_sendint(&retbuf, nsplen, 1);
    pq_sendbytes(&retbuf, nspname, nsplen);
    pq_sendint(&retbuf, rellen, 1);
    pq_sendbytes(&retbuf, relname, rellen);

    /*
     * write column name and column type name.
     */
    TupleDesc tupdesc = RelationGetDescr(rel);
    int att_name_len;
    int type_name_len;
    char* type_name = NULL;

    pq_sendint(&retbuf, tupdesc->natts, 4);

    for (int i = 0; i < tupdesc->natts; i++) {
        att_name_len = strlen(tupdesc->attrs[i]->attname.data);
        pq_sendint(&retbuf, att_name_len, 4);
        pq_sendbytes(&retbuf, tupdesc->attrs[i]->attname.data, att_name_len);

        Assert(InvalidOid != tupdesc->attrs[i]->atttypid);

        type_name = get_typename(tupdesc->attrs[i]->atttypid);
        type_name_len = strlen(type_name);
        pq_sendint(&retbuf, type_name_len, 4);
        pq_sendbytes(&retbuf, type_name, type_name_len);
        pq_sendint(&retbuf, tupdesc->attrs[i]->atttypmod, 4);
        pfree(type_name);
    }

    relation_close(rel, AccessShareLock);

    PgFdwRemoteSender(pgxc_handles, retbuf.data, retbuf.len, tag);

    if (tag == PGFDW_GET_TABLE_INFO) {
        PgFdwRemoteInfo* info = (PgFdwRemoteInfo*)remote_info;
        PgFdwRemoteReceiver(pgxc_handles, info, sizeof(PgFdwRemoteInfo));
    } else /* For analyze */
    {
        PGFDWTableAnalyze* info = (PGFDWTableAnalyze*)remote_info;
        PgFdwRemoteReceiver(pgxc_handles, info, sizeof(PGFDWTableAnalyze));
    }

    pfree(retbuf.data);
    retbuf.data = NULL;
}

/*
 * get foreign table encode from option
 */
static bool gcfdw_get_table_encode(ForeignTable* table, int* remote_encoding)
{
    bool have_remote_encoding = false;
    ListCell* lc = NULL;
    DefElem* def = NULL;
    foreach (lc, table->options) {
        def = (DefElem*)lfirst(lc);

        if (strcmp(def->defname, "encoding") == 0) {
            *remote_encoding = pg_char_to_encoding(defGetString(def));

            if (*remote_encoding == -1) {
                ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("%s is not a valid encoding name", defGetString(def))));
            }

            have_remote_encoding = true;
        }
    }

    return have_remote_encoding;
}

/*
 * Send remote encode information.
 */
static void gcfdw_send_remote_encode(PGXCNodeAllHandles* pgxc_handles, int remote_encoding)
{
    StringInfoData retbuf;
    initStringInfo(&retbuf);

    pq_sendint(&retbuf, GetDatabaseEncoding(), 4);
    pq_sendint(&retbuf, remote_encoding, 4);

    PgFdwRemoteSender(pgxc_handles, retbuf.data, retbuf.len, PGFDW_GET_ENCODE);
    PgFdwRemoteReceiver(pgxc_handles, NULL, 0);

    pfree(retbuf.data);
    retbuf.data = NULL;
}

/*
 * Send remote query param.
 */
static void gcfdw_send_remote_query_param(GcFdwScanState* fsstate)
{
    if (u_sess->pgxc_cxt.gc_fdw_run_version >= GCFDW_VERSION_V1R8C10_1 && fsstate->have_remote_encoding == true) {
        gcfdw_send_remote_encode(fsstate->pgxc_handle, fsstate->remote_encoding);
    }

    StringInfoData retbuf;
    initStringInfo(&retbuf);

    pq_sendint(&retbuf, fsstate->current_idx, 4);
    pq_sendint(&retbuf, fsstate->cycle_idx, 4);

    PgFdwSendSnapshot(&retbuf, fsstate->remoteinfo->snapshot, fsstate->remoteinfo->snapsize);

    PgFdwRemoteSender(fsstate->pgxc_handle, retbuf.data, retbuf.len, PGFDW_QUERY_PARAM);
    PgFdwRemoteReceiver(fsstate->pgxc_handle, NULL, 0);

    pfree(retbuf.data);
    retbuf.data = NULL;
}

/*
 * Get datanode idx.
 */
static void gcfdw_get_datanode_idx(ForeignScanState* node)
{
    Plan* fsplan = node->ss.ps.plan;
    GcFdwScanState* fsstate = (GcFdwScanState*)node->fdw_state;

    if (IS_PGXC_DATANODE) {
        fsstate->max_idx = fsstate->num_datanode;
        fsstate->cycle_idx = list_length(fsplan->exec_nodes->nodeList) * SET_DOP(fsplan->dop);
        fsstate->current_idx = 0;

        bool found = false;
        ListCell* lc = NULL;
        foreach (lc, fsplan->exec_nodes->nodeList) {
            if (lfirst_int(lc) == u_sess->pgxc_cxt.PGXCNodeId) {
                found = true;
                break;
            }
            fsstate->current_idx++;
        }

        if (found) {
            fsstate->current_idx =
                u_sess->stream_cxt.smp_id * list_length(fsplan->exec_nodes->nodeList) + fsstate->current_idx;
        } else {
            fsstate->current_idx = -1;
        }
    } else {
        fsstate->max_idx = 1;
        fsstate->cycle_idx = 1;
        fsstate->current_idx = 0;
    }

    Assert(fsstate->current_idx < fsstate->cycle_idx);
}

/*
 * Force assorted GUC parameters to settings that ensure that we'll output
 * data values in a form that is unambiguous to the remote server.
 *
 * This is rather expensive and annoying to do once per row, but there's
 * little choice if we want to be sure values are transmitted accurately;
 * we can't leave the settings in place between rows for fear of affecting
 * user-visible computations.
 *
 * We use the equivalent of a function SET option to allow the settings to
 * persist only until the caller calls reset_transmission_modes().  If an
 * error is thrown in between, guc.c will take care of undoing the settings.
 *
 * The return value is the nestlevel that must be passed to
 * reset_transmission_modes() to undo things.
 */
int set_transmission_modes(void)
{
    int nestlevel = NewGUCNestLevel();

    /*
     * The values set here should match what pg_dump does.  See also
     * configure_remote_session in connection.c.
     */
    if (u_sess->time_cxt.DateStyle != USE_ISO_DATES)
        (void)set_config_option("datestyle", "ISO", PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SAVE, true, 0, false);
    if (u_sess->attr.attr_common.IntervalStyle != INTSTYLE_POSTGRES)
        (void)set_config_option(
            "intervalstyle", "postgres", PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SAVE, true, 0, false);
    if (u_sess->attr.attr_common.extra_float_digits < 3)
        (void)set_config_option("extra_float_digits", "3", PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SAVE, true, 0, false);

    return nestlevel;
}

/*
 * Undo the effects of set_transmission_modes().
 */
void reset_transmission_modes(int nestlevel)
{
    AtEOXact_GUC(true, nestlevel);
}

/*
 * Utility routine to close a cursor.
 */
static void close_cursor(PGconn* conn, unsigned int cursor_number)
{
    char sql[64] = {0};
    PGresult* res = NULL;

    errno_t rc = snprintf_s(sql, sizeof(sql), sizeof(sql) - 1, "CLOSE c%u", cursor_number);
    securec_check_ss(rc, "", "");

    /*
     * We don't use a PG_TRY block here, so be careful not to throw error
     * without releasing the PGresult.
     */
    res = pgfdw_exec_query(conn, sql);
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
        pgfdw_report_error(ERROR, res, conn, true, sql);
    PQclear(res);
}

/*
 * Prepare for processing of parameters used in remote query.
 */
static void prepare_query_params(PlanState* node, List* fdw_exprs, int numParams, FmgrInfo** param_flinfo,
    List** param_exprs, const char*** param_values)
{
    int i = 0;
    ListCell* lc = NULL;

    Assert(numParams > 0);

    /* Prepare for output conversion of parameters used in remote query. */
    *param_flinfo = (FmgrInfo *)palloc0(sizeof(FmgrInfo) * numParams);

    foreach (lc, fdw_exprs) {
        Node *param_expr = (Node *)lfirst(lc);
        Oid typefnoid = InvalidOid;
        bool isvarlena = false;

        getTypeOutputInfo(exprType(param_expr), &typefnoid, &isvarlena);
        fmgr_info(typefnoid, &(*param_flinfo)[i]);
        i++;
    }

    /*
     * Prepare remote-parameter expressions for evaluation.  (Note: in
     * practice, we expect that all these expressions will be just Params, so
     * we could possibly do something more efficient than using the full
     * expression-eval machinery for this.  But probably there would be little
     * benefit, and it'd require gc_fdw to know more than is desirable
     * about Param evaluation.)
     */
    *param_exprs = (List *)ExecInitExpr((Expr *)fdw_exprs, node);

    /* Allocate buffer for text form of query parameters. */
    *param_values = (const char **)palloc0(numParams * sizeof(char *));
}

/*
 * Construct array of query parameter values in text format.
 */
static void process_query_params(
    ExprContext* econtext, FmgrInfo* param_flinfo, List* param_exprs, const char** param_values)
{
    int i = 0;
    ListCell* lc = NULL;

    int nestlevel = set_transmission_modes();
    foreach (lc, param_exprs) {
        ExprState *expr_state = (ExprState *)lfirst(lc);
        Datum expr_value;
        bool isNull = false;

        /* Evaluate the parameter expression */
        expr_value = ExecEvalExpr(expr_state, econtext, &isNull, NULL);

        /*
         * Get string representation of each parameter value by invoking
         * type-specific output function, unless the value is null.
         */
        if (isNull) {
            param_values[i] = NULL;
        } else {
            param_values[i] = OutputFunctionCall(&param_flinfo[i], expr_value);
        }
        i++;
    }

    reset_transmission_modes(nestlevel);
}

/*
 * gcAnalyzeForeignTable
 *		Test whether analyzing this foreign table is supported
 */
static bool gcAnalyzeForeignTable(Relation relation, AcquireSampleRowsFunc* func, BlockNumber* totalpages,
    void* additionalData, bool estimate_table_rownum)
{
    PGFDWTableAnalyze* info = (PGFDWTableAnalyze*)additionalData;
    Oid relid = info->relid;
    ForeignTable* table = GetForeignTable(relid);

    int remote_encoding;
    bool have_remote_encoding = gcfdw_get_table_encode(table, &remote_encoding);

    (void)GetConnection(table->serverid, &info->pgxc_handles, false, have_remote_encoding);

    u_sess->pgxc_cxt.gc_fdw_run_version = gcfdw_send_and_fetch_version(info->pgxc_handles);
    if (u_sess->pgxc_cxt.gc_fdw_run_version >= GCFDW_VERSION_V1R8C10_1 && have_remote_encoding == true) {
        gcfdw_send_remote_encode(info->pgxc_handles, remote_encoding);
    }

    gcfdw_fetch_remote_table_info(info->pgxc_handles, table, info, PGFDW_ANALYZE_TABLE);

    return true;
}

/*
 * Create a tuple from the specified row of the PGresult.
 *
 * rel is the local representation of the foreign table, attinmeta is
 * conversion data for the rel's tupdesc, and retrieved_attrs is an
 * integer list of the table column numbers present in the PGresult.
 * temp_context is a working context that can be reset after each tuple.
 */
static HeapTuple make_tuple_from_result_row(PGresult* res, int row, Relation rel, AttInMetadata* attinmeta,
    List* retrieved_attrs, ForeignScanState* fsstate, MemoryContext temp_context)
{
    HeapTuple tuple = NULL;
    TupleDesc tupdesc = NULL;
    Datum* values = NULL;
    bool* nulls = NULL;
    ItemPointer ctid = NULL;
    Oid oid = InvalidOid;
    ConversionLocation errpos;
    ErrorContextCallback errcallback;
    MemoryContext oldcontext = NULL;
    ListCell* lc = NULL;
    int j = 0;

    Assert(row < PQntuples(res));

    /*
     * Do the following work in a temp context that we reset after each tuple.
     * This cleans up not only the data we have direct access to, but any
     * cruft the I/O functions might leak.
     */
    oldcontext = MemoryContextSwitchTo(temp_context);

    if (rel) {
        tupdesc = RelationGetDescr(rel);
    } else {
        GcFdwScanState *fdw_sstate = NULL;

        Assert(fsstate != NULL);
        fdw_sstate = (GcFdwScanState *)fsstate->fdw_state;
        tupdesc = fdw_sstate->tupdesc;
    }

    values = (Datum*)palloc0(tupdesc->natts * sizeof(Datum));
    nulls = (bool*)palloc(tupdesc->natts * sizeof(bool));
    /* Initialize to nulls for any columns not present in result */
    errno_t rc = memset_s(nulls, tupdesc->natts * sizeof(bool), true, tupdesc->natts * sizeof(bool));
    securec_check(rc, "\0", "\0");

    /*
     * Set up and install callback to report where conversion error occurs.
     */
    errpos.rel = rel;
    errpos.cur_attno = 0;
    errpos.fsstate = fsstate;
    errcallback.callback = conversion_error_callback;
    errcallback.arg = (void*)&errpos;
    errcallback.previous = t_thrd.log_cxt.error_context_stack;
    t_thrd.log_cxt.error_context_stack = &errcallback;

    /*
     * i indexes columns in the relation, j indexes columns in the PGresult.
     */
    foreach (lc, retrieved_attrs) {
        int i = lfirst_int(lc);
        char* valstr = NULL;

        /* fetch next column's textual value */
        if (PQgetisnull(res, row, j)) {
            valstr = NULL;
        } else {
            valstr = PQgetvalue(res, row, j);
        }

        /*
         * convert value to internal representation
         *
         * Note: we ignore system columns other than ctid and oid in result
         */
        errpos.cur_attno = i;
        if (i > 0) {
            /* ordinary column */
            Assert(i <= tupdesc->natts);
            if (valstr == NULL) {
                nulls[i - 1] = true;
            } else {
                nulls[i - 1] = false;
            }
            /* Apply the input function even to nulls, to support domains */
            values[i - 1] = InputFunctionCall(
                &attinmeta->attinfuncs[i - 1], valstr, attinmeta->attioparams[i - 1], attinmeta->atttypmods[i - 1]);
        } else if (i == SelfItemPointerAttributeNumber) {
            /* ctid */
            if (valstr != NULL) {
                Datum datum = DirectFunctionCall1(tidin, CStringGetDatum(valstr));
                ctid = (ItemPointer)DatumGetPointer(datum);
            }
        } else if (i == ObjectIdAttributeNumber) {
            /* oid */
            if (valstr != NULL) {
                Datum datum = DirectFunctionCall1(oidin, CStringGetDatum(valstr));
                oid = DatumGetObjectId(datum);
            }
        }
        errpos.cur_attno = 0;

        j++;
    }

    /* Uninstall error context callback. */
    t_thrd.log_cxt.error_context_stack = errcallback.previous;

    /*
     * Check we got the expected number of columns.  Note: j == 0 and
     * PQnfields == 1 is expected, since deparse emits a NULL if no columns.
     */
    if (j > 0 && j != PQnfields(res))
        ereport(ERROR,
            (errcode(ERRCODE_OPERATE_RESULT_NOT_EXPECTED),
                errmsg("remote query result does not match the foreign table")));

    /*
     * Build the result tuple in caller's memory context.
     */
    MemoryContextSwitchTo(oldcontext);

    tuple = heap_form_tuple(tupdesc, values, nulls);

    /*
     * If we have a CTID to return, install it in both t_self and t_ctid.
     * t_self is the normal place, but if the tuple is converted to a
     * composite Datum, t_self will be lost; setting t_ctid allows CTID to be
     * preserved during EvalPlanQual re-evaluations (see ROW_MARK_COPY code).
     */
    if (ctid)
        tuple->t_self = tuple->t_data->t_ctid = *ctid;

    /*
     * Stomp on the xmin, xmax, and cmin fields from the tuple created by
     * heap_form_tuple.  heap_form_tuple actually creates the tuple with
     * DatumTupleFields, not HeapTupleFields, but the executor expects
     * HeapTupleFields and will happily extract system columns on that
     * assumption.  If we don't do this then, for example, the tuple length
     * ends up in the xmin field, which isn't what we want.
     */

    /*
     * If we have an OID to return, install it.
     */
    if (OidIsValid(oid))
        HeapTupleSetOid(tuple, oid);

    /* Clean up */
    MemoryContextReset(temp_context);

    return tuple;
}

static HeapTuple make_tuple_from_agg_result(PGresult* res, int row, Relation rel, AttInMetadata* attinmeta,
    List* retrieved_attrs, ForeignScanState* fsstate, MemoryContext temp_context)
{
    HeapTuple tuple = NULL;
    TupleDesc tupdesc = NULL;
    Datum* values = NULL;
    bool* nulls = NULL;
    MemoryContext oldcontext = NULL;
    int j = 0;

    Assert(row < PQntuples(res));

    /*
     * Do the following work in a temp context that we reset after each tuple.
     * This cleans up not only the data we have direct access to, but any
     * cruft the I/O functions might leak.
     */
    oldcontext = MemoryContextSwitchTo(temp_context);

    GcFdwScanState *fdw_sstate = NULL;

    Assert(fsstate != NULL);
    fdw_sstate = (GcFdwScanState *)fsstate->fdw_state;
    tupdesc = fdw_sstate->tupdesc;

    values = (Datum *)palloc0(tupdesc->natts * sizeof(Datum));
    nulls = (bool *)palloc(tupdesc->natts * sizeof(bool));

    /* Initialize to nulls for any columns not present in result */
    errno_t rc = memset_s(nulls, tupdesc->natts * sizeof(bool), true, tupdesc->natts * sizeof(bool));
    securec_check(rc, "\0", "\0");

    /*
     * i indexes columns in the relation, j indexes columns in the PGresult.
     */
    for (int i = 1; i <= tupdesc->natts; i++) {
        char *valstr = NULL;

        /* fetch next column's textual value */
        if (PQgetisnull(res, row, j)) {
            valstr = NULL;
        } else {
            valstr = PQgetvalue(res, row, j);
        }

        nulls[i - 1] = (valstr == NULL);

        /* Apply the input function even to nulls, to support domains */
        values[i - 1] = InputFunctionCall(
            &attinmeta->attinfuncs[i - 1], valstr, attinmeta->attioparams[i - 1], attinmeta->atttypmods[i - 1]);
        j++;
    }

    /*
     * Check we got the expected number of columns.  Note: j == 0 and
     * PQnfields == 1 is expected, since deparse emits a NULL if no columns.
     */
    if (j > 0 && j != PQnfields(res))
        ereport(ERROR,
            (errcode(ERRCODE_OPERATE_RESULT_NOT_EXPECTED),
                errmsg("remote query result does not match the foreign table")));

    /*
     * Build the result tuple in caller's memory context.
     */
    MemoryContextSwitchTo(oldcontext);

    tuple = heap_form_tuple(tupdesc, values, nulls);

    /*
     * Stomp on the xmin, xmax, and cmin fields from the tuple created by
     * heap_form_tuple.  heap_form_tuple actually creates the tuple with
     * DatumTupleFields, not HeapTupleFields, but the executor expects
     * HeapTupleFields and will happily extract system columns on that
     * assumption.  If we don't do this then, for example, the tuple length
     * ends up in the xmin field, which isn't what we want.
     */

    /* Clean up */
    MemoryContextReset(temp_context);

    return tuple;
}

/*
 * Callback function which is called when error occurs during column value
 * conversion.  Print names of column and relation.
 */
static void conversion_error_callback(void *arg)
{
    const char *attname = NULL;
    const char *relname = NULL;
    bool is_wholerow = false;
    ConversionLocation *errpos = (ConversionLocation *)arg;

    if (errpos->rel) {
        /* error occurred in a scan against a foreign table */
        TupleDesc tupdesc = RelationGetDescr(errpos->rel);

        if (errpos->cur_attno > 0 && errpos->cur_attno <= tupdesc->natts) {
            attname = NameStr(tupdesc->attrs[errpos->cur_attno - 1]->attname);
        } else if (errpos->cur_attno == SelfItemPointerAttributeNumber) {
            attname = "ctid";
        } else if (errpos->cur_attno == ObjectIdAttributeNumber) {
            attname = "oid";
        }

        relname = RelationGetRelationName(errpos->rel);
    } else {
        /* error occurred in a scan against a foreign join */
        errcontext("processing expression at position %d in select list", errpos->cur_attno);
    }

    if (relname != NULL) {
        if (is_wholerow) {
            errcontext("whole-row reference to foreign table \"%s\"", relname);
        } else if (attname != NULL) {
            errcontext("column \"%s\" of foreign table \"%s\"", attname, relname);
        }
    }
}

/*
 * Find an equivalence class member expression, all of whose Vars, come from
 * the indicated relation.
 */
extern Expr* find_em_expr_for_rel(EquivalenceClass* ec, RelOptInfo* rel)
{
    ListCell* lc_em = NULL;

    foreach (lc_em, ec->ec_members) {
        EquivalenceMember* em = (EquivalenceMember*)lfirst(lc_em);

        if (bms_is_subset(em->em_relids, rel->relids)) {
            /*
             * If there is more than one equivalence member whose Vars are
             * taken entirely from this relation, we'll be content to choose
             * any one of those.
             */
            return em->em_expr;
        }
    }

    /* We didn't find any suitable equivalence class expression */
    return NULL;
}

/*
 * gcValidateTableDef
 *		Check create/alter foreign table
 */
static void gcValidateTableDef(Node* Obj)
{
    if (NULL == Obj)
        return;

    switch (nodeTag(Obj)) {
        case T_AlterTableStmt: {
            List* cmds = ((AlterTableStmt*)Obj)->cmds;
            ListCell* lcmd = NULL;

            foreach (lcmd, cmds) {
                AlterTableCmd* cmd = (AlterTableCmd*)lfirst(lcmd);

                if (cmd->subtype != AT_AlterColumnType && cmd->subtype != AT_GenericOptions &&
                    cmd->subtype != AT_AlterColumnGenericOptions && cmd->subtype != AT_ChangeOwner &&
                    cmd->subtype != AT_AddNodeList && cmd->subtype != AT_DeleteNodeList &&
                    cmd->subtype != AT_UpdateSliceLike) {
                    ereport(ERROR,
                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmodule(MOD_COOP_ANALYZE),
                            errmsg("Un-support feature"),
                            errdetail("target table is a foreign table")));
                }

                if (cmd->subtype == AT_GenericOptions && nodeTag(cmd->def) == T_List) {
                    List* defs = (List*)cmd->def;
                    ListCell* lc = NULL;

                    foreach (lc, defs) {
                        DefElem* def = (DefElem*)lfirst(lc);
                        if (def->defaction == DEFELEM_ADD || def->defaction == DEFELEM_DROP) {
                            ereport(ERROR,
                                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                    errmodule(MOD_COOP_ANALYZE),
                                    errmsg("Un-support feature"),
                                    errdetail("target table is a foreign table")));
                        }
                    }
                }
            }
            break;
        }
        case T_CreateForeignTableStmt: {
            DistributeBy* DisByOp = ((CreateStmt*)Obj)->distributeby;

            /* Start a node by isRestoreMode when adding a node, do not regist distributeby
             * info for pgxc_class. So distributeby clause is not needed.
             */
            if (IS_PGXC_COORDINATOR && !isRestoreMode) {
                if (NULL == DisByOp) {
                    ereport(ERROR,
                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmodule(MOD_COOP_ANALYZE),
                            errmsg("Need DISTRIBUTE BY clause for the foreign table.")));
                }

                if (DISTTYPE_ROUNDROBIN != DisByOp->disttype) {
                    ereport(ERROR,
                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmodule(MOD_COOP_ANALYZE),
                            errmsg("Unsupport distribute type."),
                            errdetail("Supported option values are \"roundrobin\".")));
                }

                if (((CreateForeignTableStmt*)Obj)->part_state) {
                    /* as for gc_fdw foreign table, unsupport parition table. */
                    ereport(ERROR,
                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("The gc_fdw partition foreign table is not supported.")));
                }
            }

            /*if the error_relation exists for a gc_fdw foreign table ,the warning reminds that it is not support*/
            if (((CreateForeignTableStmt*)Obj)->error_relation != NULL &&
                IsA(((CreateForeignTableStmt*)Obj)->error_relation, RangeVar)) {
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmodule(MOD_COOP_ANALYZE),
                        errmsg("The error_relation of the foreign table is not support.")));
            }

            /* check write only */
            if (((CreateForeignTableStmt*)Obj)->write_only) {
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmodule(MOD_COOP_ANALYZE),
                        errmsg("Unsupport write only.")));
            }

            /* check optLogRemote and optRejectLimit */
            if (CheckForeignExtOption(((CreateForeignTableStmt*)Obj)->extOptions, optLogRemote)) {
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmodule(MOD_COOP_ANALYZE),
                        errmsg("The REMOTE LOG of gc_fdw foreign table is not support.")));
            }

            if (CheckForeignExtOption(((CreateForeignTableStmt*)Obj)->extOptions, optRejectLimit)) {
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmodule(MOD_COOP_ANALYZE),
                        errmsg("The PER NODE REJECT LIMIT of gc_fdw foreign table is not support.")));
            }

            break;
        }
        default:
            ereport(ERROR,
                (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmodule(MOD_COOP_ANALYZE),
                    errmsg("unrecognized node type: %d", (int)nodeTag(Obj))));
    }
}

/*
 * @Description: check log_remote and reject_limit of extOption
 * @IN OptList: The foreign table option list.
 * @Return: true if log_remote or reject_limit, false for other
 * @See also:
 */
static bool CheckForeignExtOption(List* extOptList, const char* str)
{
    bool CheckExtOption = false;
    ListCell* cell = NULL;
    foreach (cell, extOptList) {
        DefElem* Opt = (DefElem*)lfirst(cell);

        if (0 == pg_strcasecmp(Opt->defname, str)) {
            CheckExtOption = true;
            break;
        }
    }

    return CheckExtOption;
}

static bool GcFdwCanSkip(GcFdwScanState* fsstate)
{
    if (fsstate->current_idx == -1 || fsstate->current_idx >= fsstate->max_idx) {
        return true;
    }

    if (fsstate->remoteinfo->reltype == LOCATOR_TYPE_REPLICATED && fsstate->current_idx != 0) {
        return true;
    }

    return false;
}

static void GcFdwCopyRemoteInfo(PgFdwRemoteInfo* new_remote_info, PgFdwRemoteInfo* ori_remote_info)
{
    new_remote_info->reltype = ori_remote_info->reltype;
    new_remote_info->datanodenum = ori_remote_info->datanodenum;

    errno_t rc = memcpy_s((char*)new_remote_info->snapshot,
        new_remote_info->snapsize,
        (char*)ori_remote_info->snapshot,
        ori_remote_info->snapsize);

    if (rc != 0) {
        Assert(0);
    }

    securec_check_c(rc, "\0", "\0");
}

bool hasSpecialArrayType(TupleDesc desc)
{
    for (int i = 0; i < desc->natts; i++) {
        Oid typoid = desc->attrs[i]->atttypid;

        if (INT8ARRAYOID == typoid || FLOAT8ARRAYOID == typoid || FLOAT4ARRAYOID == typoid || NUMERICARRAY == typoid) {
            return true;
        }
    }

    return false;
}

// end of file
