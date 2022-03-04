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
 * knl_session_attr_storage.h
 *   Data struct to store all knl_session_attr_storage GUC variables.
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
 *        src/include/knl/knl_guc/knl_session_attr_storage.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef SRC_INCLUDE_KNL_KNL_SESSION_ATTR_STORAGE
#define SRC_INCLUDE_KNL_KNL_SESSION_ATTR_STORAGE

#include "knl/knl_guc/knl_guc_common.h"
#include "datatype/timestamp.h"

typedef struct knl_session_attr_dcf {
    /* parameters can be reloaded while DCF is running */
    int dcf_election_timeout;
    int dcf_auto_elc_priority_en;
    int dcf_election_switch_threshold;
    int dcf_run_mode;
    char* dcf_log_level;
    int dcf_max_log_file_size;
    int dcf_flow_control_cpu_threshold;
    int dcf_flow_control_net_queue_message_num_threshold;
    int dcf_flow_control_disk_rawait_threshold;
    int dcf_log_backup_file_count;
    /* dcf log truncate frequency */
    int dcf_truncate_threshold;
} knl_session_attr_dcf;

typedef struct knl_session_attr_storage {
    bool raise_errors_if_no_files;
    bool enableFsync;
    bool fullPageWrites;
    bool Log_connections;
    bool autovacuum_start_daemon;
#ifdef LOCK_DEBUG
    bool Trace_locks;
    bool Trace_userlocks;
#endif    
    bool Trace_lwlocks;

    /* belong to #ifdef LOCK_DEBUG, but if #ifdef, compile error */
    bool Debug_deadlocks;
    bool log_lock_waits;
    bool phony_autocommit;
    bool DefaultXactReadOnly;
    bool DefaultXactDeferrable;
    bool XactDeferrable;
#ifdef WAL_DEBUG
    bool XLOG_DEBUG;
#endif
    bool synchronize_seqscans;
    bool enable_data_replicate;
    bool HaModuleDebug;
    bool hot_standby_feedback;
    bool enable_stream_replication;
    bool guc_most_available_sync;
    bool enable_show_any_tuples;
    bool enable_debug_vacuum;
    bool enable_adio_debug;
    bool gds_debug_mod;
    bool log_pagewriter;
    bool enable_incremental_catchup;
    bool auto_explain_log_verbose;
    bool enable_candidate_buf_usage_count;
    bool enable_ustore_partial_seqscan;
    int keep_sync_window;
    int wait_dummy_time;
    int DeadlockTimeout;
    int LockWaitTimeout;
    int LockWaitUpdateTimeout;
    int max_standby_archive_delay;
    int max_standby_streaming_delay;
    int wal_receiver_status_interval;
    int wal_receiver_timeout;
    int wal_receiver_connect_timeout;
    int wal_receiver_connect_retries;
    int basebackup_timeout;
    int max_loaded_cudesc;
    int num_temp_buffers;
    int psort_work_mem;
    int bulk_write_ring_size;
    int bulk_read_ring_size;
    int partition_mem_batch;
    int partition_max_cache_size;
    int VacuumCostPageHit;
    int VacuumCostPageMiss;
    int VacuumCostPageDirty;
    int VacuumCostLimit;
    int VacuumCostDelay;
    int autovacuum_vac_cost_delay;
    int autovacuum_vac_cost_limit;
    int gs_clean_timeout;
    int twophase_clean_workers;
#ifdef LOCK_DEBUG
    int Trace_lock_oidmin;
    int Trace_lock_table;
#endif
    int replorigin_sesssion_origin;
    int wal_keep_segments;
    int CheckPointSegments;
    int CheckPointTimeout;
    int fullCheckPointTimeout;
    int incrCheckPointTimeout;
    int CheckPointWarning;
    int checkpoint_flush_after;
    int CheckPointWaitTimeOut;
    int WalWriterDelay;
    int wal_sender_timeout;
    int CommitDelay;
    int partition_lock_upgrade_timeout;
    int CommitSiblings;
    int log_min_duration_statement;
    int Log_autovacuum_min_duration;
    int BgWriterDelay;
    int bgwriter_lru_maxpages;
    int bgwriter_flush_after;
    int max_index_keys;
    int max_identifier_length;
    int block_size;
    int segment_size;
    int wal_block_size;
    int wal_segment_size;
    int autovacuum_naptime;
    int autoanalyze_timeout;
    int autovacuum_vac_thresh;
    int autovacuum_anl_thresh;
    int prefetch_quantity;
    int backwrite_quantity;
    int cstore_prefetch_quantity;
    int cstore_backwrite_max_threshold;
    int cstore_backwrite_quantity;
    int fast_extend_file_size;
    int gin_pending_list_limit;
    int gtm_connect_retries;
    int gtm_conn_check_interval;
    int dfs_max_parsig_length;
    int plog_merge_age;
    int max_redo_log_size;

    int max_io_capacity;
    int default_index_kind;
    int max_buffer_usage_count;

    int64 version_retention_age;
    int64 vacuum_freeze_min_age;
    int64 vacuum_freeze_table_age;
    int64 vacuum_defer_cleanup_age;
    double bgwriter_lru_multiplier;
    double shared_buffers_fraction;
    double autovacuum_vac_scale;
    double autovacuum_anl_scale;
    double CheckPointCompletionTarget;
    double candidate_buf_percent_target;
    double dirty_page_percent_max;
    char* XLogArchiveCommand;
    char* XLogArchiveDest;
    char* default_tablespace;
    char* temp_tablespaces;
    char* XactIsoLevel_string;
    char* SyncRepStandbyNames;
    char* ReplConnInfoArr[GUC_MAX_REPLNODE_NUM];
    char* CrossClusterReplConnInfoArr[GUC_MAX_REPLNODE_NUM];
    char* PrimarySlotName;
    char* logging_module;
    char* Inplace_upgrade_next_system_object_oids;
    char* hadr_super_user_record_path;
    int resource_track_log;
    int guc_synchronous_commit;
    int sync_rep_wait_mode;
    int sync_method;
    int autovacuum_mode;
    int cstore_insert_mode;
    int pageWriterSleep;
    bool enable_cbm_tracking;
    bool enable_copy_server_files;
    int target_rto;
    int time_to_target_rpo;
    int hadr_recovery_time_target;
    int hadr_recovery_point_target;
    bool enable_twophase_commit;
    int ustats_tracker_naptime;
    int umax_search_length_for_prune;
    int archive_interval;

    /*
     * xlog keep for all standbys even through they are not connect and donnot created replslot.
     */
    bool enable_xlog_prune;
    int max_size_for_xlog_prune;
    int defer_csn_cleanup_time;

    bool enable_defer_calculate_snapshot;
    bool enable_hashbucket;
    bool enable_segment;

    /* for GTT */
    int max_active_gtt;
    int vacuum_gtt_defer_check_age;

    /* for undo */
    int undo_space_limit_size;
    int undo_limit_size_transaction;

    bool enable_recyclebin;
    int recyclebin_retention_time;
    int undo_retention_time;
    /*
     * !!!!!!!!!!!     Be Carefull     !!!!!!!!!!!
     * Make sure to use the same value in UHeapCalcTupleDataSize and UheapFillDiskTuple when creating a tuple.
     * So we only read this value at the begining of UheapFormTuple and UheaptoastInsertOrUpdate, then pass the value
     * to UHeapCalcTupleDataSize or UheapFillDiskTuple. The same for toast operations.
     */
    
#ifndef ENABLE_MULTIPLE_NODES
    int recovery_min_apply_delay;
    TimestampTz recoveryDelayUntilTime;
#endif
    bool reserve_space_for_nullable_atts;
    knl_session_attr_dcf dcf_attr;
    int catchup2normal_wait_time;
} knl_session_attr_storage;

#endif /* SRC_INCLUDE_KNL_KNL_SESSION_ATTR_STORAGE */
