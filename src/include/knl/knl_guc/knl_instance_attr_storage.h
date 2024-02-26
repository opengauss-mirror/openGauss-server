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
 * knl_instance_attr_storage.h
 *        Data struct to store all knl_instance_attr_storage GUC variables.
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
 *        src/include/knl/knl_guc/knl_instance_attr_storage.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef SRC_INCLUDE_KNL_KNL_INSTANCE_ATTR_STORAGE_H_
#define SRC_INCLUDE_KNL_KNL_INSTANCE_ATTR_STORAGE_H_

#include "knl/knl_guc/knl_guc_common.h"

/* order should same as lwlock part num desc, see lwlock.h */
enum LWLOCK_PARTITION_ID {
    CLOG_PART = 0,
    CSNLOG_PART = 1,
    LOG2_LOCKTABLE_PART = 2,
    TWOPHASE_PART = 3,
    FASTPATH_PART = 4,
    LWLOCK_PART_KIND
};

typedef struct knl_instance_attr_dcf {
    /* DCF switch */
    bool enable_dcf;
    bool dcf_ssl;
    /* maximum thread number of dcf workers */
    int dcf_max_workers;
    /* Args needed in DCF start */
    char* dcf_config;
    int dcf_node_id;
    char* dcf_data_path;
    char* dcf_log_path;
    /* parameters should be set before DCF start, can not be reloaded */
    int dcf_log_file_permission;
    int dcf_log_path_permission;
    int dcf_mec_agent_thread_num;
    int dcf_mec_reactor_thread_num;
    int dcf_mec_channel_num;
    int dcf_mem_pool_init_size;
    int dcf_mem_pool_max_size;
    int dcf_compress_algorithm;
    int dcf_compress_level;
    int dcf_socket_timeout;
    int dcf_connect_timeout;
    int dcf_rep_append_thread_num;
    int dcf_mec_fragment_size;
    int dcf_stg_pool_init_size;
    int dcf_stg_pool_max_size;
    int dcf_mec_pool_max_size;
    int dcf_mec_batch_size;
} knl_instance_attr_dcf;

typedef struct knl_instance_attr_nvm {
    bool enable_nvm;
    char* nvm_file_path;
    char *nvmBlocks;
    double bypassDram;
    double bypassNvm;
} knl_instance_attr_nvm;

typedef struct knl_instance_attr_dss {
    bool ss_enable_dss;
    char* ss_dss_vg_name;
    char* ss_dss_conn_path;
} knl_instance_attr_dss;

typedef struct knl_instance_attr_dms {
    bool enable_dms;
    bool enable_catalog_centralized;
    bool enable_dss_aio;
    bool enable_verify_page;
    bool enable_ondemand_realtime_build;
    bool enable_ondemand_recovery;
    int ondemand_recovery_mem_size;
    int instance_id;
    int recv_msg_pool_size;
    char* interconnect_url;
    char* interconnect_type;
    char* rdma_work_config;
    char* ock_log_path;
    int channel_count;
    int work_thread_count;
    bool enable_reform;
    bool enable_ssl;
    int inst_count;
    bool enable_log_level;
    bool enable_scrlock;
    bool enable_scrlock_sleep_mode;
    char* scrlock_server_bind_core_config;
    char* scrlock_worker_bind_core_config;
    int scrlock_server_port;
    int scrlock_worker_count;
    int32 sslog_level;
    int32 sslog_backup_file_count;
    int32 sslog_max_file_size; //Unit:KB
    int parallel_thread_num;
    int32 txnstatus_cache_size;
    bool enable_bcast_snapshot;
    char* work_thread_pool_attr;
    int32 work_thread_pool_max_cnt;
} knl_instance_attr_dms;

typedef struct knl_instance_attr_storage {
    bool wal_log_hints;
    bool EnableHotStandby;
    bool enable_mix_replication;
    bool IsRoachStandbyCluster;
    bool enable_gtm_free;
    bool comm_cn_dn_logic_conn;
    bool enable_adio_function;
    bool enableIncrementalCheckpoint;
    bool enable_double_write;
    bool enable_delta_store;
    bool enableWalLsnCheck;
    bool gucMostAvailableSync;
    bool enable_ustore;
    bool auto_csn_barrier;
    bool enable_availablezone;
    bool enable_wal_shipping_compression;
    int WalReceiverBufSize;
    int DataQueueBufSize;
    int NBuffers;
    int NNvmBuffers;
    int NPcaBuffers;
    int NSegBuffers;
    int cstore_buffers;
    int MaxSendSize;
    int max_prepared_xacts;
    int max_locks_per_xact;
    int max_predicate_locks_per_xact;
    int64 walwriter_sleep_threshold;
    int num_xloginsert_locks;
    int walwriter_cpu_bind;
    int wal_file_init_num;
    int XLOGbuffers;
    int max_wal_senders;
    int max_replication_slots;
    int replication_type;
    int autovacuum_max_workers;
    int64 autovacuum_freeze_max_age;
    int wal_level;
    /* User specified maximum number of recovery threads. */
    int max_recovery_parallelism;
    int recovery_parse_workers;
    int recovery_undo_workers;
    int recovery_redo_workers_per_paser_worker;
    int pagewriter_thread_num;
    int dw_file_num;
    int dw_file_size;
    int real_recovery_parallelism;
    int batch_redo_num;
    int remote_read_mode;
    int advance_xlog_file_num;
    int gtm_option;
    int max_undo_workers;
    int enable_update_max_page_flush_lsn;
    int max_keep_log_seg;
    int max_size_for_xlog_receiver;
#ifdef EXTREME_RTO_DEBUG_AB
    int extreme_rto_ab_pos;
    int extreme_rto_ab_type;
    int extreme_rto_ab_count;
#endif

    int max_concurrent_autonomous_transactions;
    char* available_zone;
    knl_instance_attr_dcf dcf_attr;
    knl_instance_attr_nvm nvm_attr;
    knl_instance_attr_dss dss_attr;
    knl_instance_attr_dms dms_attr;
    int num_internal_lock_partitions[LWLOCK_PART_KIND];
    char* num_internal_lock_partitions_str;
    int wal_insert_status_entries_power;
    int undo_zone_count;
    int64 xlog_file_size;
    char* xlog_file_path;
    char* xlog_lock_file_path;
    int wal_flush_timeout;
    int wal_flush_delay;
    int max_logical_replication_workers;
    char *redo_bind_cpu_attr;
    int max_active_gtt;

    /* extreme-rto standby read */
    int base_page_saved_interval;
    double standby_force_recycle_ratio;
    int standby_recycle_interval;
    int standby_max_query_time;
    bool enable_exrto_standby_read_opt;
#ifndef ENABLE_MULTIPLE_NODES
    bool enable_save_confirmed_lsn;
#endif
    bool enable_huge_pages;
    int huge_page_size;
    bool enable_time_report;
    bool enable_batch_dispatch;
    int parallel_recovery_timeout;
    int parallel_recovery_batch;
    bool ss_enable_dorado;
    bool ss_stream_cluster;
    
    bool enable_uwal;
    char* uwal_config;

    int64 uwal_disk_size;
    char* uwal_devices_path;
    char* uwal_log_path;

    bool uwal_rpc_compression_switch;
    bool uwal_rpc_flowcontrol_switch;
    int uwal_rpc_flowcontrol_value;
    bool uwal_async_append_switch;
} knl_instance_attr_storage;

#endif /* SRC_INCLUDE_KNL_KNL_INSTANCE_ATTR_STORAGE_H_ */

