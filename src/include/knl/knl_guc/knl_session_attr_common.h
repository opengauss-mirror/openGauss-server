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
 * knl_session_attr_common.h
 *        Data struct to store all knl_session_attr_common GUC variables.
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
 * 
 * IDENTIFICATION
 *        src/include/knl/knl_guc/knl_session_attr_common.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef SRC_INCLUDE_KNL_KNL_SESSION_ATTR_COMMON_H_
#define SRC_INCLUDE_KNL_KNL_SESSION_ATTR_COMMON_H_

#include "knl/knl_guc/knl_guc_common.h"

typedef struct knl_session_attr_common {
    bool enable_beta_features;
    bool session_auth_is_superuser;
    bool ignore_checksum_failure;
    bool log_checkpoints;
    bool Log_disconnections;
    bool ExitOnAnyError;
    bool omit_encoding_error;
    bool log_parser_stats;
    bool log_planner_stats;
    bool log_executor_stats;
    bool log_statement_stats;
    bool pgstat_track_activities;
    bool pgstat_track_counts;
    bool pgstat_track_sql_count;
    bool track_io_timing;
    bool update_process_title;
    bool pooler_cache_connection;
    bool Trace_notify;
    bool log_hostname;
    bool XactReadOnly;
    bool Log_truncate_on_rotation;
    bool trace_sort;
    bool integer_datetimes;
    bool XLogArchiveMode;
    bool IsInplaceUpgrade;
    bool allow_concurrent_tuple_update;
    bool IgnoreSystemIndexes;
    bool PersistentConnections;
    bool xc_maintenance_mode;
    bool enable_redistribute;
    bool check_implicit_conversions_for_indexcol;
    bool support_batch_bind;
    bool enable_nls;
    int XLogArchiveTimeout;
    int Log_file_mode;
    int bbox_dump_count;
    int session_sequence_cache;
    int max_stack_depth;
    int max_query_retry_times;
    int StatementTimeout;
    int SessionTimeout;
    int SessionTimeoutCount;
#ifndef ENABLE_MULTIPLE_NODES
    int IdleInTransactionSessionTimeout;
    int IdleInTransactionSessionTimeoutCount;
#endif
    int pgstat_collect_thread_status_interval;
    int extra_float_digits;
    int effective_io_concurrency;
    int backend_flush_after;
    int Log_RotationAge;
    int Log_RotationSize;
    int LogMaxSize;
    int max_function_args;
    int max_user_defined_exception;
    int tcp_keepalives_idle;
    int tcp_keepalives_interval;
    int tcp_keepalives_count;
    int tcp_user_timeout;
    int GinFuzzySearchLimit;
    int server_version_num;
    int log_temp_files;
    int transaction_sync_naptime;
    int ngram_gram_size;
    int gtm_connect_timeout;
    int gtm_rw_timeout;
    int transaction_sync_timeout;
    int fault_mon_timeout;
    int block_encryption_mode;
    int64 group_concat_max_len;
    double ConnectionAlarmRate;
    char* client_encoding_string;
    char* character_set_connection;
    char* collation_connection;
    char* Log_line_prefix;
    char* safe_data_path;
    char* log_timezone_string;
    char* datestyle_string;
    char* Dynamic_library_path;
    char* locale_collate;
    char* locale_ctype;
    char* locale_messages;
    char* locale_monetary;
    char* locale_numeric;
    char* locale_time;
    char* local_preload_libraries_string;
    char* current_logic_cluster_name;
    char* node_group_mode;
    char* namespace_search_path;
    char* nls_timestamp_format_string;
    char* namespace_current_schema;
    char* server_encoding_string;
    char* server_version_string;
    char* role_string;
    char* session_authorization_string;
    char* log_destination_string;
    char* Log_directory;
    char* asp_flush_mode;
    char* asp_log_filename;
    char* Perf_log;
    char* Log_filename;
    char* query_log_file;
    char* syslog_ident_str;
    char* timezone_string;
    struct pg_tz* session_timezone;
    struct pg_tz* log_timezone;
    char* timezone_abbreviations_string;
    char* pgstat_temp_directory;
    char* TSCurrentConfig;
    char* GtmHostInfoStringArray[MAX_GTM_HOST_NUM];
    struct tagGtmHostIP* GtmHostArray[MAX_GTM_HOST_NUM];
    int GtmHostIPNumArray[MAX_GTM_HOST_NUM];
    char* test_param_str;
    char* seg_test_param_str;
    char* application_name;
    char* analysis_options;
    int bytea_output;
    int DefaultXactIsoLevel;
    int IntervalStyle;
    int Log_error_verbosity;
    int backtrace_min_messages;
    int client_min_messages;
    int log_min_messages;
    int log_min_error_statement;
    int log_statement;
    int syslog_facility;
    int SessionReplicationRole;
    int trace_recovery_messages;
    int pgstat_track_functions;
    int xmlbinary;
    int remoteConnType;

    bool enable_bbox_dump;
    char* bbox_dump_path;

    bool assert_enabled;
    bool enable_expr_fusion;
    int AlarmReportInterval;
    int xmloption;
    bool enable_ts_compaction;
    bool enable_ts_kvmerge;
    bool enable_ts_outorder;    
    bool ts_adaptive_threads;
    char* ts_compaction_strategy;
    int ts_consumer_workers;
    int ts_cudesc_threshold;
    int ts_valid_partition;

    /* instrumentation guc parameters */
    int instr_unique_sql_count;
    bool enable_instr_cpu_timer;
    int unique_sql_track_type;
    bool enable_instr_track_wait;
    bool enable_slow_query_log;

    int instr_rt_percentile_interval;
    bool enable_instr_rt_percentile;
    bool track_stmt_parameter;
    char* percentile_values;

    /* instr - full sql/slow sql */
    bool enable_stmt_track;
    int track_stmt_session_slot;
    char *track_stmt_stat_level;
    int64 track_stmt_details_size;
    char* track_stmt_retention_time;
    // using for standby
    char* track_stmt_standby_chain_size;

    bool enable_wdr_snapshot;
    bool enable_set_variable_b_format;
    bool enable_asp;
    bool show_fdw_remote_plan;
    int wdr_snapshot_interval;
    int wdr_snapshot_retention_days;
    int asp_sample_interval;
    int asp_flush_rate;
    int asp_retention_days;
    int max_datanode_for_plan;
    int upgrade_mode;
    int wdr_snapshot_query_timeout;
    int dn_heartbeat_interval;
    bool enable_full_encryption;
    bool enable_proc_coverage;

    char* router_att;
    bool enable_router;
    int backend_version;
#ifdef ENABLE_MULTIPLE_NODES
    bool enable_gpc_grayrelease_mode;
#endif
    int gpc_clean_timeout;
    char* node_name;
#ifndef ENABLE_MULTIPLE_NODES
    bool plsql_show_all_error;
    bool enable_seqscan_fusion;
    bool enable_iud_fusion;
#endif
    uint32 extension_session_vars_array_size;
    void** extension_session_vars_array;
    char* threadpool_reset_percent_item;
    int threadpool_reset_percent_list[2];
    bool enable_indexscan_optimization;
    char* delimiter_name;
    bool b_compatibility_user_host_auth;
    int time_record_level;
} knl_session_attr_common;

#endif /* SRC_INCLUDE_KNL_KNL_SESSION_ATTR_COMMON_H_ */
