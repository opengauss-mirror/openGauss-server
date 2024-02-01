/*
 * Copyright (c) 2023 Huawei Technologies Co.,Ltd.
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
 * knl_session_attr_spq.h
 *        Data struct to store all knl_session_attr_spq GUC variables.
 *
 *   When anyone try to added variable in this file, which means add a guc
 *   variable, there are several rules needed to obey:
 *
 *   add variable to struct 'knl_@level@_attr_@group@'
 *
 *   @level@:
 *   1. instance: the level of guc variable is PGC_POSTMASTER.
 *   2. session: the other level of guc variable.
 *
 *   @group@: sql, storage, security, network, memory, resource, common, spq
 *   select the group according to the type of guc variable.
 * 
 * 
 * IDENTIFICATION
 *        src/include/knl/knl_guc/knl_session_attr_spq.h
 *
 * ---------------------------------------------------------------------------------------
 */
 
#ifndef SRC_INCLUDE_KNL_KNL_SESSION_ATTR_MPP_H_
#define SRC_INCLUDE_KNL_KNL_SESSION_ATTR_MPP_H_
 
#include "knl/knl_guc/knl_guc_common.h"

#define PREALLOC_PAGE_ARRAY_SIZE 16

struct NodeDefinition;
 
typedef struct knl_session_attr_spq {
    /* Optimizer related gucs */
    bool gauss_enable_spq;
    bool spq_optimizer_log;
    int spq_optimizer_minidump;
    int spq_optimizer_cost_model;
    bool spq_optimizer_metadata_caching;
    int spq_optimizer_mdcache_size;
    bool spq_optimizer_use_gauss_allocators;
 
    /* Optimizer debugging GUCs */
    bool spq_optimizer_print_query;
    bool spq_optimizer_print_plan;
    bool spq_optimizer_print_xform;
    bool spq_optimizer_print_xform_results;
    bool spq_optimizer_print_memo_after_exploration;
    bool spq_optimizer_print_memo_after_implementation;
    bool spq_optimizer_print_memo_after_optimization;
    bool spq_optimizer_print_job_scheduler;
    bool spq_optimizer_print_expression_properties;
    bool spq_optimizer_print_group_properties;
    bool spq_optimizer_print_optimization_context;
    bool spq_optimizer_print_optimization_stats;

    bool spq_optimizer_print_optimization_cost;

    /* Optimizer Parallel DML */
    bool spq_enable_insert_select;
    int spq_insert_dop_num;
    bool spq_enable_insert_from_tableless;
    bool spq_enable_insert_order_sensitive;
    bool spq_enable_delete;
    bool spq_enable_remove_delete_redundant_motion;
    bool spq_enable_remove_update_redundant_motion;
    int spq_delete_dop_num;
    bool spq_enable_update;
    int spq_update_dop_num;
 
    /* array of xforms disable flags */
#define OPTIMIZER_XFORMS_COUNT 400 /* number of transformation rules */
    bool spq_optimizer_xforms[OPTIMIZER_XFORMS_COUNT];
 
    /* GUCs to tell Optimizer to enable a physical operator */
    bool spq_optimizer_enable_nljoin;
    bool spq_optimizer_enable_indexjoin;
    bool spq_optimizer_enable_motions_masteronly_queries;
    bool spq_optimizer_enable_motions;
    bool spq_optimizer_enable_motion_broadcast;
    bool spq_optimizer_enable_motion_gather;
    bool spq_optimizer_enable_motion_redistribute;
    bool spq_optimizer_discard_redistribute_hashjoin;
    bool spq_optimizer_enable_sort;
    bool spq_optimizer_enable_materialize;
    bool spq_optimizer_enable_partition_propagation;
    bool spq_optimizer_enable_partition_selection;
    bool spq_optimizer_enable_outerjoin_rewrite;
    bool spq_optimizer_enable_multiple_distinct_aggs;
    bool spq_optimizer_enable_direct_dispatch;
    bool spq_optimizer_enable_hashjoin_redistribute_broadcast_children;
    bool spq_optimizer_enable_broadcast_nestloop_outer_child;
    bool spq_optimizer_enable_streaming_material;
    bool spq_optimizer_enable_gather_on_segment_for_dml;
    bool spq_optimizer_enable_assert_maxonerow;
    bool spq_optimizer_enable_constant_expression_evaluation;
    bool spq_optimizer_enable_bitmapscan;
    bool spq_optimizer_enable_outerjoin_to_unionall_rewrite;
    bool spq_optimizer_enable_ctas;
    bool spq_optimizer_enable_partial_index;
    bool spq_optimizer_enable_dml;
    bool spq_optimizer_enable_dml_triggers;
    bool spq_optimizer_enable_dml_constraints;
    bool spq_optimizer_enable_master_only_queries;
    bool spq_optimizer_enable_hashjoin;
    bool spq_optimizer_enable_dynamictablescan;
    bool spq_optimizer_enable_indexscan;
    bool spq_optimizer_enable_indexonlyscan;
    bool spq_optimizer_enable_tablescan;
    bool spq_optimizer_enable_seqsharescan;
    bool spq_optimizer_enable_shareindexscan;
    bool spq_optimizer_enable_hashagg;
    bool spq_optimizer_enable_groupagg;
    bool spq_optimizer_expand_fulljoin;
    bool spq_optimizer_enable_mergejoin;
    bool spq_optimizer_prune_unused_columns;
    bool spq_optimizer_enable_redistribute_nestloop_loj_inner_child;
    bool spq_optimizer_force_comprehensive_join_implementation;
    bool spq_optimizer_enable_replicated_table;
    bool spq_optimizer_calc_multiple_dop;
 
    /* Optimizer plan enumeration related GUCs */
    bool spq_optimizer_enumerate_plans;
    bool spq_optimizer_sample_plans;
    int spq_optimizer_plan_id;
    int	spq_optimizer_samples_number;
 
    /* Cardinality estimation related GUCs used by the Optimizer */
    bool spq_optimizer_extract_dxl_stats;
    bool spq_optimizer_extract_dxl_stats_all_nodes;
    bool spq_optimizer_print_missing_stats;
    double spq_optimizer_damping_factor_filter;
    double spq_optimizer_damping_factor_join;
    double spq_optimizer_damping_factor_groupby;
    bool spq_optimizer_dpe_stats;
    bool spq_optimizer_enable_derive_stats_all_groups;
 
    /* Costing related GUCs used by the Optimizer */
    int spq_optimizer_segments;
    int spq_optimizer_penalize_broadcast_threshold;
    double spq_optimizer_cost_threshold;
    double spq_optimizer_nestloop_factor;
    double spq_optimizer_sort_factor;
    double spq_optimizer_share_tablescan_factor;
    double spq_optimizer_share_indexscan_factor;
    double spq_optimizer_hashjoin_spilling_mem_threshold;
    double spq_optimizer_hashjoin_inner_cost_factor;
 
    /* Optimizer hints */
    int spq_optimizer_join_arity_for_associativity_commutativity;
    int spq_optimizer_array_expansion_threshold;
    int spq_optimizer_join_order_threshold;
    int spq_optimizer_join_order;
    int spq_optimizer_cte_inlining_bound;
    int spq_optimizer_push_group_by_below_setop_threshold;
    int spq_optimizer_xform_bind_threshold;
    int spq_optimizer_skew_factor;
    bool spq_optimizer_force_multistage_agg;
    bool spq_optimizer_force_three_stage_scalar_dqa;
    bool spq_optimizer_force_expanded_distinct_aggs;
    bool spq_optimizer_force_agg_skew_avoidance;
    bool spq_optimizer_penalize_skew;
    bool spq_enable_left_index_nestloop_join;
    bool spq_optimizer_prune_computed_columns;
    bool spq_optimizer_push_requirements_from_consumer_to_producer;
    bool spq_optimizer_enforce_subplans;
    bool spq_optimizer_use_external_constant_expression_evaluation_for_ints;
    bool spq_optimizer_apply_left_outer_to_union_all_disregarding_stats;
    bool spq_optimizer_remove_superfluous_order;
    bool spq_optimizer_remove_order_below_dml;
    bool spq_optimizer_multilevel_partitioning;
    bool spq_optimizer_parallel_union;
    bool spq_optimizer_array_constraints;
    bool spq_optimizer_cte_inlining;
    bool spq_optimizer_enable_space_pruning;
    bool spq_optimizer_enable_associativity;
    bool spq_optimizer_enable_eageragg;
    bool spq_optimizer_enable_orderedagg;
    bool spq_optimizer_enable_range_predicate_dpe;
 
    bool spq_enable_pre_optimizer_check;
    bool spq_enable_result_hash_filter;
 
    bool spq_debug_print_full_dtm;
    bool spq_debug_cancel_print;
    bool spq_print_direct_dispatch_info;
    bool spq_log_dispatch_stats;
    bool spq_debug_slice_print;

    int spq_scan_unit_size;
    int spq_scan_unit_bit;
    char *gauss_cluster_map;
    double spq_small_table_threshold;
    bool spq_enable_direct_read;
 
    /* enable spq btbuild */
    bool spq_enable_btbuild;
    bool spq_enable_btbuild_cic;
    bool spq_enable_transaction;
    int  spq_batch_size;
    int  spq_mem_size;
    int  spq_queue_size;

    bool spq_enable_adaptive_scan;
} knl_session_attr_spq;

struct RemoteQueryState;

typedef struct NodePagingState {
    bool finish;
    int64_t batch_size; /* The count of unit number which px worker to read */
    /* header or tail may less than batch_size, so remember it */
    int64_t header_unit_begin; /* first slice begin */
    int64_t header_unit_end;
    /* first slice end, abs(header_unit_end - header_unit_begin) = one unit size */
    int64_t tail_unit_begin; /* last slice begin */
    int64_t tail_unit_end; /* last slice end */
    /* last slice end, abs(tail_unit_end - tail_unit_begin) = one unit size */
    int64_t current_page;
} NodePagingState;

typedef struct SpqAdpScanReqState {
    bool this_round_finish;
    int plan_node_id;
    int direction;
    int64_t nblocks;
    int64_t cur_scan_iter_no;
    int64_t scan_start;
    int64_t scan_end;
    int64_t batch_size;
    int node_num;
    NodePagingState* node_states;
} SpqAdpScanReqState;

/* struct for paging state container */
typedef struct SpqScanAdpReqs {
    int size = 0;
    int max;
    SpqAdpScanReqState **req_states;
} SpqScanAdpReqs;

typedef struct spq_qc_ctx {
    pthread_mutex_t spq_pq_mutex;
    pthread_cond_t pq_wait_cv;
    bool is_done;
    bool is_exited;
    uint64 query_id;
    RemoteQueryState* scanState;
    int num_nodes;
    List* connects;
    SpqScanAdpReqs seq_paging_array;
} spq_qc_ctx;
 
typedef struct knl_t_spq_context {
    SpqRole spq_role;
    bool spq_in_processing;
    uint64 spq_session_id;
    int current_id;
    bool skip_direct_distribute_result;
    int num_nodes;
    NodeDefinition* nodesDefinition;

    /* Spq coordinator thread */
    bool adaptive_scan_setup;
    spq_qc_ctx* qc_ctx;
} knl_t_spq_context;
#endif /* SRC_INCLUDE_KNL_KNL_SESSION_ATTR_MPP_H_ */
