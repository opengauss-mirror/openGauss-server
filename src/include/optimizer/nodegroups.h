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
 * nodegroups.h
 *        Variables and functions used in multiple node group optimizer
 * 
 * 
 * IDENTIFICATION
 *        src/include/optimizer/nodegroups.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef NODEGROUPS_H
#define NODEGROUPS_H

#include "postgres.h"
#include "knl/knl_variable.h"

#include "nodes/bitmapset.h"
#include "nodes/nodes.h"
#include "nodes/plannodes.h"
#include "nodes/pg_list.h"
#include "nodes/relation.h"
#include "pgxc/locator.h"
#include "optimizer/streamplan.h"
/*
 * Computing node group options
 */
#define CNG_OPTION_OPTIMAL "optimal"
#define CNG_OPTION_QUERY "query"
#define CNG_OPTION_INSTALLATION "installation"

/*
 * virtual cluster node group options
 */
#define VNG_OPTION_ELASTIC_GROUP "elastic_group"

/*
 * Forbidden cost for agg and join unique
 */
#define NG_FORBIDDEN_COST -100.0

typedef struct NGroupInfo
{
    Oid         oid;
    Bitmapset*  bms_nodeids;
}NGroupInfo;

/*
 * Modes for computing node group
 *     (1) optimal : the optimal mode in compute permission scale
 *     (2) query   : the cost based mode in query scale
 *     (3) expect  : the cost based mode with a expect node group
 *     (4) force   : forced mode which compute in a specific node group
 */
typedef enum ComputingNodeGroupMode {
    CNG_MODE_COSTBASED_OPTIMAL,
    CNG_MODE_COSTBASED_QUERY,
    CNG_MODE_COSTBASED_EXPECT,
    CNG_MODE_FORCE
} ComputingNodeGroupMode;

/*-------------------------------------------------------------------------*/
/*  Variables                                                              */
/*-------------------------------------------------------------------------*/

/*-------------------------------------------------------------------------*/
/* Public functions                                                        */
/*      including:                                                         */
/* (1) General functions                                                   */
/* (2) Get general node groups                                             */
/* (3) Get distribution information of a node group                        */
/* (4) Get distribution information of a base relation                     */
/* (5) Get distribution information of a path or a plan                    */
/* (6) Distribution data structure management                              */
/* (7) Convert functions                                                   */
/* (8) Heuristic methods                                                   */
/* (9) Cost based algorithms                                               */
/* (10) Distribution management for specific operators                     */
/* (11) Do shuffle between node groups if needed                           */
/* (12) Compare functions, judge functions                                 */
/* (13) Other functions                                                    */
/*-------------------------------------------------------------------------*/

/* ----------
 * General functions
 * ----------
 */
extern void ng_init_nodegroup_optimizer(Query* query);
extern void ng_backup_nodegroup_options(bool* p_is_multiple_nodegroup_scenario,
    int* p_different_nodegroup_count, Distribution** p_in_redistribution_group_distribution,
    Distribution** p_compute_permission_group_distribution, Distribution** p_query_union_set_group_distribution,
    Distribution** p_single_node_distribution);
extern void ng_restore_nodegroup_options(bool p_is_multiple_nodegroup_scenario,
    int p_different_nodegroup_count, Distribution* p_in_redistribution_group_distribution,
    Distribution* p_compute_permission_group_distribution, Distribution* p_query_union_set_group_distribution,
    Distribution* p_single_node_distribution);
extern ComputingNodeGroupMode ng_get_computing_nodegroup_mode();

/* ----------
 * Get general node groups
 * ----------
 */
/* installation group */
extern char* ng_get_installation_group_name();
extern Oid ng_get_installation_group_oid();
extern Bitmapset* ng_get_installation_group_nodeids();
extern Distribution* ng_get_installation_group_distribution();
extern ExecNodes* ng_get_installation_group_exec_node();
/* compute permission group */
extern Distribution* ng_get_compute_permission_group_distribution();
/* query union set group */
extern Distribution* ng_get_query_union_set_group_distribution(List* baserel_rte_list, bool* baserels_in_same_group);
extern Distribution* ng_get_query_union_set_group_distribution();
/* expected computing group */
extern Distribution* ng_get_expected_computing_group_distribution();
/* in_redistribution group */
extern Oid ng_get_in_redistribution_group_oid();
extern Distribution* ng_get_in_redistribution_group_distribution();
extern Oid ng_get_redist_dest_group_oid();
/* functional node group */
extern Distribution* ng_get_default_computing_group_distribution();
extern ExecNodes* ng_get_default_computing_group_exec_node();
extern Distribution* ng_get_correlated_subplan_group_distribution();
extern Distribution* ng_get_max_computable_group_distribution();
extern Distribution* ng_get_single_node_distribution();
/* ----------
 * Get distribution information of a node group
 * ----------
 */
extern Oid ng_get_group_groupoid(const char* group_name);
extern char* ng_get_group_group_name(Oid group_oid);
extern char* ng_get_dist_group_name(Distribution *distribution);
extern Bitmapset* ng_get_group_nodeids(const Oid groupoid);
extern Distribution* ng_get_group_distribution(const Oid groupoid);
extern Distribution* ng_get_group_distribution(const char* group_name);

/* ----------
 * Get distribution information of a base relation
 * ----------
 */
extern Oid ng_get_baserel_groupoid(Oid tableoid, char relkind);
extern Bitmapset* ng_get_baserel_data_nodeids(Oid tableoid, char relkind);
extern Distribution* ng_get_baserel_data_distribution(Oid tableoid, char relkind);
extern unsigned int ng_get_baserel_num_data_nodes(Oid tableoid, char relkind);

/* ----------
 * Get distribution information of a path or a plan
 * ----------
 */
/* distribution of Path */
extern Bitmapset* ng_get_dest_nodeids(Path* path);
extern Distribution* ng_get_dest_distribution(Path* path);
/* distribution of Plan */
extern Bitmapset* ng_get_dest_nodeids(Plan* plan);
extern Distribution* ng_get_dest_distribution(Plan* plan);
extern ExecNodes* ng_get_dest_execnodes(Plan* plan);
/* number of data nodes of Path, Plan and RelOptInfo */
extern unsigned int ng_get_dest_num_data_nodes(Path* path);
extern unsigned int ng_get_dest_num_data_nodes(Plan* plan);
extern unsigned int ng_get_dest_num_data_nodes(RelOptInfo* rel);
extern unsigned int ng_get_dest_num_data_nodes(PlannerInfo* root, RelOptInfo* rel);
/* other information of Plan */
extern char ng_get_dest_locator_type(Plan* plan);
extern List* ng_get_dest_distribute_keys(Plan* plan);

/* ----------
 * Distribution data structure management
 * ----------
 */
extern Distribution* ng_copy_distribution(Distribution* src_distribution);
extern void ng_copy_distribution(Distribution* dest_distribution, const Distribution* src_distribution);
extern void ng_set_distribution(Distribution* dest_distribution, Distribution* src_distribution);
extern Distribution* ng_get_overlap_distribution(Distribution* distribution_1, Distribution* distribution_2);
extern Distribution* ng_get_union_distribution(Distribution* distribution_1, Distribution* distribution_2);
extern Distribution* ng_get_union_distribution_recycle(Distribution* distribution_1,
                                                                Distribution* distribution_2);
extern Distribution* ng_get_random_single_dn_distribution(Distribution* distribution);

/* ----------
 * Convert functions
 * ----------
 */
/* node id (index) and node oid */
extern int ng_convert_to_nodeid(Oid nodeoid);
extern Oid ng_convert_to_nodeoid(int nodeid);
/* node list */
extern List* ng_convert_to_nodeid_list(Bitmapset* bms_nodeids);
/* nodeids */
extern Bitmapset* ng_convert_to_nodeids(ExecNodes* exec_nodes);
extern Bitmapset* ng_convert_to_nodeids(List* nodeid_list);
/* Distribution */
extern Distribution* ng_convert_to_distribution(List* nodeid_list);
extern Distribution* ng_convert_to_distribution(ExecNodes* exec_nodes);
/* ExecNodes */
extern ExecNodes* ng_convert_to_exec_nodes(
    Distribution* distribution, char locator_type, RelationAccessType access_type);


/* For geting suited candidate distributions to reduce search spaces. example:
 * Single node distribution is not good for big data computing.
 * Multi node distribution is not good for small data computing.
 */
enum DistrbutionPreferenceType {
	DPT_ALL,
	DPT_SHUFFLE,
	DPT_SINGLE
};

/*
 * Heuristic methods
 */
/* Join */
extern List* ng_get_join_candidate_distribution_list(Path* outer_path, Path* inner_path, DistrbutionPreferenceType type);
extern List* ng_get_join_candidate_distribution_list(Path* outer_path, Path* inner_path, bool is_correlated, DistrbutionPreferenceType type);
/* Agg */
extern List* ng_get_agg_candidate_distribution_list(Plan* plan, bool is_correlated, DistrbutionPreferenceType type);
/* SetOp */
extern List* ng_get_setop_candidate_distribution_list(List* subPlans, bool is_correlated);

/*
 * Cost based algorithms
 */
extern double ng_get_nodegroup_stream_weight(unsigned int producer_num_dn, unsigned int consumer_num_dn);
extern Cost ng_calculate_setop_branch_stream_cost(
    Plan* subPlan, unsigned int producer_num_datanodes, unsigned int consumer_num_datanodes);

/*
 * Distribution management for specific operators
 */
/* Join */
extern Distribution* ng_get_join_distribution(Path* outer_path, Path* inner_path);
extern Distribution* ng_get_join_data_distribution(Plan* lefttree, Plan* righttree, List* nodeid_list);
/* SetOp */
extern Distribution* ng_get_union_distribution_for_union_all(List* subPlans);
extern Distribution* ng_get_best_setop_distribution(List* subPlans, bool isUnionAll, bool is_correlated);

/*
 * Do shuffle between node groups if needed
 */
extern void ng_stream_side_paths_for_replicate(PlannerInfo* root, Path** outer_path, Path** inner_path,
    JoinType jointype, bool is_mergejoin, Distribution* target_distribution);
extern Path* ng_stream_non_broadcast_side_for_join(PlannerInfo* root, Path* non_stream_path, JoinType save_jointype,
    List* non_stream_pathkeys, bool is_replicate, bool stream_outer, Distribution* target_distribution);
extern Plan* ng_agg_force_shuffle(PlannerInfo* root, List* groupcls, Plan* subplan, List* tlist, Path* subpath);

/*
 * Compare functions, judge functions
 */
extern bool ng_is_multiple_nodegroup_scenario();
extern bool ng_is_all_in_installation_nodegroup_scenario();
extern bool ng_enable_nodegroup_explain();
extern bool ng_is_valid_group_name(const char* group_name);
extern bool ng_is_special_group(Distribution* distribution);
extern bool ng_is_same_group(Bitmapset* bms_nodeids_1, Bitmapset* bms_nodeids_2);
extern bool ng_is_same_group(List* nodeid_list_1, List* nodeid_list_2);
extern bool ng_is_same_group(ExecNodes* exec_nodes, Bitmapset* bms_nodeids);
extern bool ng_is_same_group(Distribution* distribution_1, Distribution* distribution_2);
extern bool ng_is_exec_on_subset_nodes(ExecNodes* en1, ExecNodes* en2);
extern bool ng_is_shuffle_needed(Distribution* current_distribution, Distribution* target_distribution);
extern bool ng_is_shuffle_needed(PlannerInfo* root, Path* path, Distribution* target_distribution);
extern bool ng_is_distribute_key_valid(PlannerInfo* root, List* distribute_key, List* target_list);

/*
 * node group cache hash table interface
 */
extern void ngroup_info_hash_create();
extern Bitmapset*  ngroup_info_hash_search(Oid ngroup_oid);
extern void  ngroup_info_hash_insert(Oid ngroup_oid, Bitmapset * bms_node_ids);
extern void ngroup_info_hash_delete(Oid ngroup_oid, bool is_destory = false);
extern void ngroup_info_hash_destory(void);


/*
 * Other functions
 */
extern Bitmapset* ng_get_single_node_group_nodeids();
extern Distribution* ng_get_single_node_group_distribution();
extern ExecNodes* ng_get_single_node_group_exec_node();
extern char* dist_to_str(Distribution *distribution);
extern void _outBitmapset(StringInfo str, Bitmapset* bms);
extern bool ng_is_single_node_group_distribution(Distribution* distribution);
extern int ng_get_different_nodegroup_count();
#endif /* NODEGROUPS_H */
