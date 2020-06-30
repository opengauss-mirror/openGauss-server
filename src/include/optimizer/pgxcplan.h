/* -------------------------------------------------------------------------
 *
 * pgxcplan.h
 *		Postgres-XC specific planner interfaces and structures.
 *
 *
 * Portions Copyright (c) 1996-2011, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * src/include/optimizer/pgxcplan.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef PGXCPLANNER_H
#define PGXCPLANNER_H

#include "fmgr.h"
#include "lib/stringinfo.h"
#include "nodes/params.h"
#include "nodes/parsenodes.h"
#include "nodes/plannodes.h"
#include "nodes/primnodes.h"
#include "pgxc/locator.h"
#include "tcop/dest.h"
#include "nodes/relation.h"

typedef enum {
    COMBINE_TYPE_NONE, /* it is known that no row count, do not parse */
    COMBINE_TYPE_SUM,  /* sum row counts (partitioned, round robin) */
    COMBINE_TYPE_SAME  /* expect all row counts to be the same (replicated write) */
} CombineType;

/* For sorting within RemoteQuery handling */
/*
 * It is pretty much like Sort, but without Plan. We may use Sort later.
 */
typedef struct {
    NodeTag type;
    int numCols;            /* number of sort-key columns */
    AttrNumber* sortColIdx; /* their indexes in the target list */
    Oid* sortOperators;     /* OIDs of operators to sort them by */
    Oid* sortCollations;
    bool* nullsFirst; /* NULLS FIRST/LAST directions */
    bool sortToStore; /* If should store tuples in sortstore into tuplestore. */
                      /* For with hold cursor, we should do so. */
} SimpleSort;

typedef struct {
    int numCols;            /* number of sort-key columns */
    AttrNumber* sortColIdx; /* their indexes in the target list */
    Oid* sortOperators;     /* OIDs of operators to sort them by */
    Oid* sortCollations;
    bool* nullsFirst; /* NULLS FIRST/LAST directions */
} StreamSort;

typedef enum {
    EXEC_DIRECT_NONE,
    EXEC_DIRECT_LOCAL,
    EXEC_DIRECT_LOCAL_UTILITY,
    EXEC_DIRECT_UTILITY,
    EXEC_DIRECT_SELECT,
    EXEC_DIRECT_INSERT,
    EXEC_DIRECT_UPDATE,
    EXEC_DIRECT_DELETE
} ExecDirectType;

/*
 * Contains instructions on processing a step of a query.
 * In the prototype this will be simple, but it will eventually
 * evolve into a GridSQL-style QueryStep.
 */
typedef struct {
    Scan scan;
    ExecDirectType exec_direct_type; /* track if remote query is execute direct and what type it is */
    char* sql_statement;
    char* execute_statement;   /* execute statement */
    ExecNodes* exec_nodes;     /* List of Datanodes where to launch query */
    CombineType combine_type;
    bool read_only;            /* do not use 2PC when committing read only steps */
    bool force_autocommit;     /* some commands like VACUUM require autocommit mode */
    char* statement;           /* if specified use it as a PreparedStatement name on Datanodes */
    char* cursor;              /* if specified use it as a Portal name on Datanodes */
    int rq_num_params;         /* number of parameters present in remote statement */
    Oid* rq_param_types;       /* parameter types for the remote statement */
    bool rq_params_internal;   /* Do the query params refer to the source data
                                * plan as against user-supplied params ?
                                */

    RemoteQueryExecType exec_type;
    bool is_temp;              /* determine if this remote node is based
                                * on a temporary objects (no 2PC) */

    bool rq_finalise_aggs;     /* Aggregates should be finalised at the
                                * Datanode
                                */
    bool rq_sortgroup_colno;   /* Use resno for sort group references
                                * instead of expressions
                                */
    Query* remote_query;       /* Query structure representing the query to be
                                * sent to the datanodes
                                */
    List* base_tlist;          /* the targetlist representing the result of
                                * the query to be sent to the datanode
                                */
    /*
     * Reference targetlist of Vars to match the Vars in the plan nodes on
     * coordinator to the corresponding Vars in the remote_query. These
     * targetlists are used to while replacing/adding targetlist and quals in
     * the remote_query.
     */
    List* coord_var_tlist;
    List* query_var_tlist;
    bool has_row_marks;        /* Did SELECT had FOR UPDATE/SHARE? */
    bool rq_save_command_id;   /* Save the command ID to be used in
                                * some special cases */

    bool is_simple;            /* true if generate by gauss distributed framework, act as gather node in coordinator,
                                false if generate by pgxc distributed framework */
    bool rq_need_proj;         /* @hdfs
                                * if is_simple is false, this value is always true; if is_simple is true,  rq_need_proj
                                * is true for hdfs foreign table and false on other conditions.
                                */
    bool mergesort_required;   /* true if the underlying node is sorted, default false;	*/
    bool spool_no_data;        /* true if it do not need buffer all the data from data node */

    bool poll_multi_channel;   /* poll receive data from multi channel randomly	*/

    int num_stream;            /* num of stream below remote query */

    int num_gather;            /* num of gather(scan_gather plan_router) below remote query */

    SimpleSort* sort;

    /* @hdfs
     * The variable rte_ref will be used when the remotequery is executed by converting to stream.
     * this variable store RangeTblRef struct, every of which corresponds the rte order number in
     * rtable of root(PlannerInfo).
     */
    List* rte_ref;

    RemoteQueryType position;  /* the position where the RemoteQuery node will run. */
    bool is_send_bucket_map;   /* send bucket map to dn or not */
} RemoteQuery;

typedef struct VecRemoteQuery : public RemoteQuery {
} VecRemoteQuery;

extern PlannedStmt* pgxc_planner(Query* query, int cursorOptions, ParamListInfo boundParams);
extern List* AddRemoteQueryNode(List* stmts, const char* queryString, RemoteQueryExecType remoteExecType, bool is_temp);
extern bool pgxc_query_contains_temp_tables(List* queries);
extern bool pgxc_query_contains_utility(List* queries);
extern void pgxc_rqplan_adjust_tlist(PlannerInfo* root, RemoteQuery* rqplan, bool gensql = true);

extern Plan* pgxc_make_modifytable(PlannerInfo* root, Plan* topplan);
extern List* pgxc_get_dist_var(Index varno, RangeTblEntry* rte, List* tlist);
extern ExecNodes* pgxc_is_join_shippable(ExecNodes* inner_en, ExecNodes* outer_en, bool inner_unshippable_tlist,
    bool outer_shippable_tlist, JoinType jointype, Node* join_quals);
extern bool contains_temp_tables(List* rtable);
extern List* process_agg_targetlist(PlannerInfo* root, List** local_tlist);
extern bool containing_ordinary_table(Node* node);
extern Param* pgxc_make_param(int param_num, Oid param_type);

extern void preprocess_const_params(PlannerInfo* root, Node* jtnode);

#endif /* PGXCPLANNER_H */

