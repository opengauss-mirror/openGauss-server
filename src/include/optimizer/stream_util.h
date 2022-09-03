/* ---------------------------------------------------------------------------------------
 * 
 * stream_util.h
 *        prototypes for stream plan utilities.
 * 
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.  
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *        src/include/optimizer/stream_util.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef STREAM_UTIL_H
#define STREAM_UTIL_H

#include "catalog/pgxc_class.h"
#include "nodes/parsenodes.h"
#include "optimizer/pgxcplan.h"
#include "optimizer/pgxcship.h"
#include "optimizer/paths.h"
#include "pgxc/nodemgr.h"
#include "pgxc/pgxc.h"

#define update_scan_list(root, lst, fromRTI, toRTI, rtiSize) \
    ((List*)update_scan_expr(root, (Node*)(lst), fromRTI, toRTI, rtiSize))

typedef struct {
    Relids outer_relids;         /* relids to judge if supplying parameters in subpath */
    Bitmapset *upper_params;
    bool only_check_stream;      /* if we should only check stream or also check paramterized path */
    bool under_materialize_all;  /* if traversing under rescan-avoid materialize */
    bool has_stream;             /* if there's stream node in the path */
    bool has_parameterized_path; /* If parameter is passed in the path */
    bool has_cstore_index_delta; /* if there is cstore index scan with delta data */
} ContainStreamContext;

typedef enum {
    CPLN_DEFAULT = 0x00,
    CPLN_ONE_WAY = 0x01,           /* Search in one way without go into operators with many branchs */
    CPLN_NO_IGNORE_MATERIAL = 0x10 /* search all plannode, including material */
} ContainPlanNodeMode;

typedef struct {
    bool collect_vars;
    List* aggs;
    List* vars;
} foreign_qual_context;

/* the context for all subplans referenced by current node. */
typedef struct {
    List* org_subplans;     /* subplans for planned stmt */
    List* org_initPlan;     /* initPlan for planned stmt */
    List* subplan_plan_ids; /* subplans referenced by current node */
} set_node_ref_subplan_context;

typedef struct {
    PlannerInfo* root;
    Index* fromRTIs;
    Index* toRTIs;
    int rtiSize;
} update_scan_expr_context;

extern void finalize_node_id(Plan* result_plan, int* plan_node_id, int* parent_node_id, int* num_streams,
    int* num_plannodes, int* total_num_streams, int* max_push_sql_num, int* gather_count, List* subplans,
    List* subroots, List** initplans, int* subplan_ids, bool is_under_stream, bool is_under_ctescan,
    bool is_data_node_exec, bool is_read_only, NodeGroupInfoContext* node_group_info_context);
extern bool has_subplan(
    Plan* result_plan, Plan* parent, ListCell* cell, bool is_left, List** initplans, bool is_search);
extern void stream_path_walker(Path* path, ContainStreamContext* context);
extern bool contain_special_plan_node(Plan* plan, NodeTag planTag, ContainPlanNodeMode mode = CPLN_DEFAULT);
extern void SerializePlan(Plan* node, PlannedStmt* planned_stmt, StringInfoData* str, int num_stream, int num_gather,
    bool push_subplan = true);
extern char* DecompressSerializedPlan(const char* comp_plan_string, int cLen, int oLen);
extern char* CompressSerializedPlan(const char* plan_string, int* cLen);
extern List* contains_specified_func(Node* node, contain_func_context* context);
extern bool is_local_redistribute_needed(Plan* subplan);
extern bool foreign_qual_walker(Node* node, foreign_qual_context* context);
extern Oid get_hash_type(Oid type_in);
extern bool is_type_cast_hash_compatible(FuncExpr* func);
extern Plan* update_plan_refs(PlannerInfo* root, Plan* plan, Index* fromRTI, Index* toRTI, int rtiSize);
extern void set_node_ref_subplan_walker(Plan* result_plan, set_node_ref_subplan_context* context);
extern void StreamPlanWalker(PlannedStmt *pstmt, Plan *plan, bool *need);
extern void mark_distribute_setop_remotequery(PlannerInfo* root, Node* node, Plan* plan, List* subPlans);
#endif /* STREAM_UTIL_H */