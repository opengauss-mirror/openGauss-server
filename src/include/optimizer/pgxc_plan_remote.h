/* -------------------------------------------------------------------------
 *
 * pgxc_plan_remote.h
 *		openGauss specific planner interfaces and structures for remote query.
 *
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2011, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * src/include/optimizer/pgxc_plan_remote.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef PGXC_PLANN_REMOTE_H
#define PGXC_PLANN_REMOTE_H

#include "fmgr.h"
#include "lib/stringinfo.h"
#include "nodes/params.h"
#include "nodes/parsenodes.h"
#include "nodes/plannodes.h"
#include "nodes/primnodes.h"
#include "pgxc/locator.h"
#include "tcop/dest.h"
#include "nodes/relation.h"

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

typedef enum {
    COMBINE_TYPE_NONE, /* it is known that no row count, do not parse */
    COMBINE_TYPE_SUM,  /* sum row counts (partitioned, round robin) */
    COMBINE_TYPE_SAME  /* expect all row counts to be the same (replicated write) */
} CombineType;

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
    int stmt_idx;               /* for cngpc, save idx of stmt_name in CachedPlan */
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
    /* If execute direct on multi nodes, ExecRemoteQuery will call function query next according to this flag */
    bool is_remote_function_query;
    bool isCustomPlan = false;
    bool isFQS = false;

    /*
     * Notice: be careful to use relationOids as it may contain non-table OID
     * in some scenarios, e.g. assignment of relationOids in fix_expr_common.
     */
    List* relationOids; /* contain OIDs of relations the plan depends on */
#ifdef USE_SPQ
    int streamID; /* required by AMS  */
    int nodeCount;
#endif
} RemoteQuery;

extern Plan* create_remote_mergeinto_plan(PlannerInfo* root, Plan* topplan, CmdType cmdtyp, MergeAction* action);
extern void pgxc_rqplan_adjust_tlist(PlannerInfo* root, RemoteQuery* rqplan, bool gensql);
extern void pgxc_rqplan_build_statement(RemoteQuery* rqplan);

extern Plan* create_remotegrouping_plan(PlannerInfo* root, Plan* local_plan);
extern Plan* create_remotedml_plan(PlannerInfo* root, Plan* topplan, CmdType cmdtyp);
extern Plan* create_remotelimit_plan(PlannerInfo* root, Plan* local_plan);
extern Plan* create_remotequery_plan(PlannerInfo* root, RemoteQueryPath* best_path);
extern Plan* create_remotesort_plan(PlannerInfo* root, Plan* local_plan, List* pathkeys);

#endif /* PGXC_PLANN_REMOTE_H */
