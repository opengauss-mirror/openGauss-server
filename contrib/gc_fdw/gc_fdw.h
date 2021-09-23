/*-------------------------------------------------------------------------
 *
 * gc_fdw.h
 *		  Foreign-data wrapper for remote openGauss servers
 *
 * IDENTIFICATION
 *		  contrib/gc_fdw/gc_fdw.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef GC_FDW_H
#define GC_FDW_H

#include "foreign/foreign.h"
#include "lib/stringinfo.h"
#include "nodes/relation.h"
#include "pgxc/execRemote.h"
#include "pgxc/pgFdwRemote.h"
#include "utils/relcache.h"

#include "libpq/libpq-fe.h"

const Oid NUMERICARRAY = 1231;

/*
 * Indexes of FDW-private information stored in fdw_private lists.
 *
 * These items are indexed with the enum FdwScanPrivateIndex, so an item
 * can be fetched with list_nth().  For example, to get the SELECT statement:
 *		sql = strVal(list_nth(fdw_private, FdwScanPrivateSelectSql));
 */
enum FdwScanPrivateIndex {
    /* SQL statement to execute remotely (as a String node) */
    FdwScanPrivateSelectSql,
    /* Integer list of attribute numbers retrieved by the SELECT */
    FdwScanPrivateRetrievedAttrs,
    /* Integer representing the desired fetch_size */
    FdwScanPrivateFetchSize,

    /* remote useful information */
    FdwScanPrivateRemoteInfo,

    /* string list of targetlist for "Output: " of explain */
    FdwScanPrivateStrTargetlist,

    /* agg result targetlist for foreignscan on agg pushdown */
    FdwScanPrivateAggResultTargetlist,

    /* agg scan targetlist for foreignscan on agg pushdown */
    FdwScanPrivateAggScanTargetlist,

    /* agg colmap for foreignscan on agg pushdown */
    FdwScanPrivateAggColmap,

    /* remote quals for agg pushdown */
    FdwScanPrivateRemoteQuals,

    /* param_list for agg pushdown */
    FdwScanPrivateParamList,

    /*
     * String describing join i.e. names of relations being joined and types
     * of join, added when the scan is join
     */
    FdwScanPrivateRelations
};

/*
 * FDW-specific planner information kept in RelOptInfo.fdw_private for a
 * gc_fdw foreign table.  For a baserel, this struct is created by
 * postgresGetForeignRelSize, although some fields are not filled till later.
 * postgresGetForeignJoinPaths creates it for a joinrel, and
 * postgresGetForeignUpperPaths creates it for an upperrel.
 */
typedef struct GcFdwRelationInfo {
    /*
     * True means that the relation can be pushed down. Always true for simple
     * foreign scan.
     */
    bool pushdown_safe;

    /*
     * Restriction clauses, divided into safe and unsafe to pushdown subsets.
     * All entries in these lists should have RestrictInfo wrappers; that
     * improves efficiency of selectivity and cost estimation.
     */
    List* remote_conds;
    List* local_conds;

    /* Actual remote restriction clauses for scan (sans RestrictInfos) */
    List* final_remote_exprs;

    /* Bitmap of attr numbers we need to fetch from the remote server. */
    Bitmapset* attrs_used;

    /* Cost and selectivity of local_conds. */
    QualCost local_conds_cost;
    Selectivity local_conds_sel;

    /* Selectivity of join conditions */
    Selectivity joinclause_sel;

    /* Estimated size and cost for a scan or join. */
    double rows;
    int width;
    Cost startup_cost;
    Cost total_cost;
    /* Costs excluding costs for transferring data from the foreign server */
    Cost rel_startup_cost;
    Cost rel_total_cost;

    /* Options extracted from catalogs. */
    Cost fdw_startup_cost;
    Cost fdw_tuple_cost;
    List* shippable_extensions; /* OIDs of whitelisted extensions */

    /* Cached catalog information. */
    ForeignTable* table;
    ForeignServer* server;

    int fetch_size; /* fetch size for this remote table */

    /*
     * Name of the relation while EXPLAINing ForeignScan. It is used for join
     * relations but is set for all relations. For join relation, the name
     * indicates which foreign tables are being joined and the join type used.
     */
    StringInfo relation_name;

    /* Join information */
    RelOptInfo* outerrel;
    RelOptInfo* innerrel;
    JoinType jointype;
    /* joinclauses contains only JOIN/ON conditions for an outer join */
    List* joinclauses; /* List of RestrictInfo */

    /* Grouping information */
    List* grouped_tlist;

    /* Subquery information */
    bool make_outerrel_subquery; /* do we deparse outerrel as a
                                  * subquery? */
    bool make_innerrel_subquery; /* do we deparse innerrel as a
                                  * subquery? */
    Relids lower_subquery_rels;  /* all relids appearing in lower
                                  * subqueries */

    /*
     * Index of the relation.  It is used to create an alias to a subquery
     * representing the relation.
     */
    int relation_index;

    /*
     * the informanction from remote.
     */
    PgFdwRemoteInfo remote_info;

    Oid reloid;
} GcFdwRelationInfo;

/* in gc_fdw.c */
extern int set_transmission_modes(void);
extern void reset_transmission_modes(int nestlevel);

/* in connection.c */
extern PGconn* GetConnection(
    Oid serverid, PGXCNodeAllHandles** pgxc_handle, bool is_start_transaction, bool have_remote_encoding);
extern PGXCNodeAllHandles* PgFdwGetPgxcNodeHandle(PGconn* conn);
extern void PgFdwReleasePgxcNodeHandle(PGXCNodeAllHandles* pgxc_handles);
extern void DirectReleaseConnection(PGconn* conn, PGXCNodeAllHandles* pgxc_handles, bool is_commit);
extern unsigned int GetCursorNumber(PGconn* conn);
extern PGresult* pgfdw_exec_query(PGconn* conn, const char* query);
extern void pgfdw_report_error(int elevel, PGresult* res, PGconn* conn, bool clear, const char* sql);

/* in option.c */
extern int ExtractConnectionOptions(
    List* defelems, const char** keywords, const char** values, int* addr_idx, List** addr_list);
extern List* ExtractExtensionList(const char* extensionsString, bool warnOnMissing);

/* in deparse.c */
extern void classifyConditions(
    PlannerInfo* root, RelOptInfo* baserel, List* input_conds, List** remote_conds, List** local_conds);
extern bool is_foreign_expr(PlannerInfo* root, RelOptInfo* baserel, Expr* expr);
extern void gcDeparseInsertSql(StringInfo buf, PlannerInfo* root, Index rtindex, Relation rel, List* targetAttrs,
    bool doNothing, List* returningList, List** retrieved_attrs);
extern void gcDeparseUpdateSql(StringInfo buf, PlannerInfo* root, Index rtindex, Relation rel, List* targetAttrs,
    List* returningList, List** retrieved_attrs);
extern void gcDeparseDirectUpdateSql(StringInfo buf, PlannerInfo* root, Index rtindex, Relation rel, List* targetlist,
    List* targetAttrs, List* remote_conds, List** params_list, List* returningList, List** retrieved_attrs);
extern void gcDeparseDeleteSql(
    StringInfo buf, PlannerInfo* root, Index rtindex, Relation rel, List* returningList, List** retrieved_attrs);
extern void gcDeparseDirectDeleteSql(StringInfo buf, PlannerInfo* root, Index rtindex, Relation rel, List* remote_conds,
    List** params_list, List* returningList, List** retrieved_attrs);
extern void gcDeparseAnalyzeSizeSql(StringInfo buf, Relation rel);
extern void gcDeparseAnalyzeSql(StringInfo buf, Relation rel, List** retrieved_attrs);
extern void gcDeparseStringLiteral(StringInfo buf, const char* val);
extern Expr* find_em_expr_for_rel(EquivalenceClass* ec, RelOptInfo* rel);
extern List* build_tlist_to_deparse(RelOptInfo* foreignrel);
extern void gcDeparseSelectStmtForRel(StringInfo buf, PlannerInfo* root, RelOptInfo* foreignrel, List* tlist,
    List* remote_conds, List* pathkeys, bool is_subquery, List** retrieved_attrs, List** params_list);

/* in shippable.c */
extern bool is_builtin(Oid objectId);
extern bool is_shippable(Oid objectId, Oid classId, GcFdwRelationInfo* fpinfo);
extern void pgfdw_fetch_remote_version(PGconn* conn);

/* Function declarations for foreign data wrapper */
extern "C" Datum gc_fdw_handler(PG_FUNCTION_ARGS);
extern "C" Datum gc_fdw_validator(PG_FUNCTION_ARGS);

#endif /* GC_FDW_H */
