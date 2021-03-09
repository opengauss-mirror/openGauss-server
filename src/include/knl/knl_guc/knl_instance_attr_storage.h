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
    int WalReceiverBufSize;
    int DataQueueBufSize;
    int NBuffers;
    int cstore_buffers;
    int MaxSendSize;
    int max_prepared_xacts;
    int max_locks_per_xact;
    int max_predicate_locks_per_xact;
    int64 xlog_idle_flushes_before_sleep;
    int num_xloginsert_locks;
    int wal_writer_cpu;
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
    int recovery_redo_workers_per_paser_worker;
    int pagewriter_thread_num;
    int bgwriter_thread_num;
    int real_recovery_parallelism;
	int batch_redo_num;
    int remote_read_mode;
    int advance_xlog_file_num;
    int gtm_option;
    int enable_update_max_page_flush_lsn;
    int max_keep_log_seg;
    int catchup2normal_wait_time;
#ifdef EXTREME_RTO_DEBUG_AB
    int extreme_rto_ab_pos;
    int extreme_rto_ab_type;
    int extreme_rto_ab_count;
#endif
#ifndef ENABLE_MULTIPLE_NODES
    int max_concurrent_autonomous_transactions;
#endif
    char* available_zone;
} knl_instance_attr_storage;

#endif /* SRC_INCLUDE_KNL_KNL_INSTANCE_ATTR_STORAGE_H_ */
