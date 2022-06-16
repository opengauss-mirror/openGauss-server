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

typedef struct knl_instance_attr_storage {
    bool wal_log_hints;
    bool EnableHotStandby;
    bool enable_mix_replication;
    bool IsRoachStandbyCluster;
    bool enable_gtm_free;
    bool comm_cn_dn_logic_conn;
    bool enable_adio_function;
    bool enable_access_server_directory;
    bool enableIncrementalCheckpoint;
    bool enable_double_write;
    bool enable_delta_store;
    bool enableWalLsnCheck;
    bool gucMostAvailableSync;
    bool enable_ustore;
    bool auto_csn_barrier;
    bool enable_wal_shipping_compression;
    int WalReceiverBufSize;
    int DataQueueBufSize;
    int NBuffers;
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
} knl_instance_attr_storage;

#endif /* SRC_INCLUDE_KNL_KNL_INSTANCE_ATTR_STORAGE_H_ */

