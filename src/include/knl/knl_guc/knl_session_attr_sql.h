/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2021, openGauss Contributors
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
 * knl_session_attr_sql.h
 *   Data struct to store knl_session_attr_sql variables.
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
 *   @group@: sql, storage, security, network, memory, resource, common
 *   select the group according to the type of guc variable.
 * 
 * IDENTIFICATION
 *        src/include/knl/knl_guc/knl_session_attr_sql.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef SRC_INCLUDE_KNL_KNL_SESSION_ATTR_SQL
#define SRC_INCLUDE_KNL_KNL_SESSION_ATTR_SQL

#include "knl/knl_guc/knl_guc_common.h"

typedef struct knl_session_attr_sql {
    bool enable_fast_numeric;
    bool enable_global_stats;
    bool enable_hdfs_predicate_pushdown;
    bool enable_absolute_tablespace;
    bool enable_hadoop_env;
    bool enable_valuepartition_pruning;
    bool enable_constraint_optimization;
    bool enable_bloom_filter;
    bool enable_codegen;
    bool enable_codegen_print;
    bool enable_sonic_optspill;
    bool enable_sonic_hashjoin;
    bool enable_sonic_hashagg;
    bool enable_upsert_to_merge;
    bool enable_csqual_pushdown;
    bool enable_change_hjcost;
    bool enable_seqscan;
    bool enable_seqscan_dopcost;
    bool enable_indexscan;
    bool enable_indexonlyscan;
    bool enable_bitmapscan;
    bool force_bitmapand;
    bool enable_union_all_subquery_orderby;
    bool transform_to_numeric_operators;
    bool enable_parallel_ddl;
    bool enable_tidscan;
    bool enable_sort;
    bool enable_compress_spill;
    bool enable_hashagg;
    bool enable_sortgroup_agg;
    bool enable_material;
    bool enable_nestloop;
    bool enable_mergejoin;
    bool enable_hashjoin;
    bool enable_index_nestloop;
    bool under_explain;
    bool enable_nodegroup_debug;
    bool enable_partitionwise;
    bool enable_remotejoin;
    bool enable_fast_query_shipping;
    bool enable_compress_hll;
    bool enable_remotegroup;
    bool enable_remotesort;
    bool enable_remotelimit;
    bool enable_startwith_debug;
    bool gtm_backup_barrier;
    bool explain_allow_multinode;
    bool enable_stream_operator;
    bool enable_stream_concurrent_update;
    bool enable_vector_engine;
    bool enable_force_vector_engine;
    bool enable_random_datanode;
    bool enable_fstream;
    bool enable_geqo;
    bool enable_gtt_concurrent_truncate;
    bool restart_after_crash;
    bool enable_early_free;
    bool enable_kill_query;
    bool log_duration;
    bool Debug_print_parse;
    bool Debug_print_rewritten;
    bool Debug_print_plan;
    bool Debug_pretty_print;
    bool enable_analyze_check;
    bool enable_autoanalyze;
    bool SQL_inheritance;
    bool Transform_null_equals;
    bool check_function_bodies;
    bool Array_nulls;
    bool default_with_oids;
    
#ifndef ENABLE_MULTIPLE_NODES
    bool enable_functional_dependency;
#endif
    bool enable_ai_stats;
    int  multi_stats_type;

#ifdef DEBUG_BOUNDED_SORT
    bool optimize_bounded_sort;
#endif
    bool enable_inner_unique_opt;
    bool escape_string_warning;
    bool standard_conforming_strings;
    bool enable_light_proxy;
    bool enable_pbe_optimization;
    bool enable_cluster_resize;
    bool lo_compat_privileges;
    bool quote_all_identifiers;
    bool enforce_a_behavior;
    bool enable_slot_log;
    bool convert_string_to_digit;
    bool agg_redistribute_enhancement;
    bool enable_broadcast;
    bool ngram_punctuation_ignore;
    bool ngram_grapsymbol_ignore;
    bool enable_fast_allocate;
    bool td_compatible_truncation;
    bool enable_upgrade_merge_lock_mode;
    bool acceleration_with_compute_pool;
    bool enable_extrapolation_stats;
    bool enable_trigger_shipping;
    bool enable_agg_pushdown_for_cooperation_analysis;
    bool enable_online_ddl_waitlock;
    bool show_acce_estimate_detail;
    bool enable_prevent_job_task_startup;
    bool enable_dngather;
    int from_collapse_limit;
    int join_collapse_limit;
    int geqo_threshold;
    int Geqo_effort;
    int Geqo_pool_size;
    int Geqo_generations;
    int hll_default_log2m;
    int hll_default_log2explicit;
    int hll_default_log2sparse;
    int hll_duplicate_check;
    int g_default_regwidth;
    int g_default_sparseon;
    int g_max_sparse;
    int g_planCacheMode;
    int cost_param;
    int schedule_splits_threshold;
    int hashagg_table_size;
    int statement_mem;
    int statement_max_mem;
    int temp_file_limit;
    int effective_cache_size;
    int best_agg_plan;
    int query_dop_tmp;
    int plan_mode_seed;
    int codegen_cost_threshold;
    int acce_min_datasize_per_thread;
    int max_cn_temp_file_size;
    int default_statistics_target;
    /* Memory Limit user could set in session */
    int FencedUDFMemoryLimit;
    int64 g_default_expthresh;
    double seq_page_cost;
    double random_page_cost;
    double cpu_tuple_cost;
    double allocate_mem_cost;
    double cpu_index_tuple_cost;
    double cpu_operator_cost;
    double stream_multiple;
    double dngather_min_rows;
    double cursor_tuple_fraction;
    double Geqo_selection_bias;
    double Geqo_seed;
    double phony_random_seed;
    char* expected_computing_nodegroup;
    char* default_storage_nodegroup;
    char* inlist2join_optmode;
    char* b_format_behavior_compat_string;
    char* behavior_compat_string;
    char* plsql_compile_behavior_compat_string;
    char* connection_info;
    char* retry_errcode_list;
    char* sql_ignore_strategy_string;
    /* the vmoptions to start JVM */
    char* pljava_vmoptions;
    int backslash_quote;
    int constraint_exclusion;
    int rewrite_rule;
    int sql_compatibility;
    int guc_explain_perf_mode;
    int skew_strategy_store;
    int codegen_strategy;

    int application_type;

    bool enable_unshipping_log;
    /*
     * enable to support "with-recursive" stream plan
     */
    bool enable_stream_recursive;
    bool enable_save_datachanged_timestamp;
    int max_recursive_times;
    /* Table skewness warning rows, range from 0 to INT_MAX*/
    int table_skewness_warning_rows;
    /* Table skewness warning threshold, range from 0 to 1, 0 indicates feature disabled*/
    double table_skewness_warning_threshold;
    bool enable_opfusion;
    bool enable_beta_opfusion;
    bool enable_partition_opfusion;
    int opfusion_debug_mode;
    double cost_weight_index;
    double default_limit_rows;

    int sql_beta_feature;
    bool partition_iterator_elimination;
    /* hypo index */
    bool enable_hypo_index;
    bool hypopg_is_explain;

    bool mot_allow_index_on_nullable_column;
    bool enable_default_ustore_table;
    char* ustore_attr;
#ifdef ENABLE_UT
    char* ustore_unit_test;
#endif
    bool create_index_concurrently;
    /* db4ai.snapshots */
    char* db4ai_snapshot_mode;
    char* db4ai_snapshot_version_delimiter;
    char* db4ai_snapshot_version_separator;
    bool  enable_ignore_case_in_dquotes;
    int pldebugger_timeout;
    bool partition_page_estimation;
    bool enable_opfusion_reuse;
#ifndef ENABLE_MULTIPLE_NODES
    bool uppercase_attribute_name;
#endif
    bool var_eq_const_selectivity;
    int vectorEngineStrategy;
#if (!defined(ENABLE_MULTIPLE_NODES)) && (!defined(ENABLE_PRIVATEGAUSS))
    bool enable_custom_parser;
    bool dolphin;
    bool whale;
    bool enable_vector_targetlist;
#endif
} knl_session_attr_sql;

#endif /* SRC_INCLUDE_KNL_KNL_SESSION_ATTR_SQL */


