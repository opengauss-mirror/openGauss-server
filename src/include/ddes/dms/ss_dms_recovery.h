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
 * ss_dms_recovery.h
 * 
 * IDENTIFICATION
 *        src/include/ddes/dms/ss_dms_recovery.h
 *
 * ---------------------------------------------------------------------------------------
 */   
#ifndef SS_DMS_RECOVERY_H
#define SS_DMS_RECOVERY_H

#include "port.h"
#include "ddes/dms/ss_common_attr.h"
#include "replication/ss_disaster_cluster.h"

#define SS_CTRL_PAGE  0
#define REFORM_CTRL_PAGE  DMS_MAX_INSTANCE

#define RECOVERY_WAIT_TIME 10000
#define SS_BEFORE_RECOVERY (ENABLE_DMS && g_instance.dms_cxt.SSReformInfo.in_reform == true \
                            && g_instance.dms_cxt.SSRecoveryInfo.recovery_pause_flag == true)
#define SS_IN_FAILOVER (ENABLE_DMS && g_instance.dms_cxt.SSRecoveryInfo.in_failover == true)
#define SS_IN_ONDEMAND_RECOVERY (ENABLE_DMS && g_instance.dms_cxt.SSRecoveryInfo.in_ondemand_recovery == true)
#define SS_ONDEMAND_BUILD_DONE (ENABLE_DMS && SS_IN_ONDEMAND_RECOVERY \
                                && t_thrd.shemem_ptr_cxt.XLogCtl->IsOnDemandBuildDone == true)
#define SS_ONDEMAND_REDO_DONE (SS_IN_ONDEMAND_RECOVERY \
                               && t_thrd.shemem_ptr_cxt.XLogCtl->IsOnDemandRedoDone == true)
#define SS_REPLAYED_BY_ONDEMAND (ENABLE_DMS && !SS_IN_ONDEMAND_RECOVERY && \
                                 t_thrd.shemem_ptr_cxt.XLogCtl->IsOnDemandBuildDone == true && \
                                 t_thrd.shemem_ptr_cxt.XLogCtl->IsOnDemandRedoDone == true)
#define SS_ONDEMAND_REALTIME_BUILD_DISABLED (ENABLE_DMS && \
    g_instance.dms_cxt.SSRecoveryInfo.ondemand_realtime_build_status == DISABLED)
#define SS_ONDEMAND_REALTIME_BUILD_READY_TO_BUILD (ENABLE_DMS && \
    g_instance.dms_cxt.SSRecoveryInfo.ondemand_realtime_build_status == READY_TO_BUILD)
#define SS_ONDEMAND_REALTIME_BUILD_NORMAL (ENABLE_DMS && \
    g_instance.dms_cxt.SSRecoveryInfo.ondemand_realtime_build_status == BUILD_NORMAL)
#define SS_ONDEMAND_REALTIME_BUILD_SHUTDOWN (ENABLE_DMS && \
    g_instance.dms_cxt.SSRecoveryInfo.ondemand_realtime_build_status == BUILD_TO_DISABLED)
#define SS_ONDEMAND_REALTIME_BUILD_FAILOVER (ENABLE_DMS && \
    g_instance.dms_cxt.SSRecoveryInfo.ondemand_realtime_build_status == BUILD_TO_REDO)

#define SS_ONDEMAND_RECOVERY_PAUSE (ENABLE_DMS && \
    g_instance.dms_cxt.SSRecoveryInfo.ondemand_recovery_pause_status != NOT_PAUSE)
#define SS_ONDEMAND_RECOVERY_HASHMAP_FULL (ENABLE_DMS && \
    g_instance.dms_cxt.SSRecoveryInfo.ondemand_recovery_pause_status == PAUSE_FOR_PRUNE_HASHMAP)
#define SS_ONDEMAND_RECOVERY_TRXN_QUEUE_FULL (ENABLE_DMS && \
    g_instance.dms_cxt.SSRecoveryInfo.ondemand_recovery_pause_status == PAUSE_FOR_PRUNE_TRXN_QUEUE)

#define ENABLE_REALTIME_BUILD_TARGET_RTO (ENABLE_DMS && \
    g_instance.attr.attr_storage.dms_attr.realtime_build_target_rto > 0 && \
    SS_SINGLE_CLUSTER)
#define SS_PRIMARY_ENABLE_TARGET_RTO (ENABLE_REALTIME_BUILD_TARGET_RTO && \
    SS_NORMAL_PRIMARY && g_instance.dms_cxt.SSRecoveryInfo.realtimeBuildLogCtrlStatus > DISABLE)
#define SS_STANDBY_ENABLE_TARGET_RTO (SS_NORMAL_STANDBY && \
    SS_ONDEMAND_REALTIME_BUILD_NORMAL && g_instance.dms_cxt.SSRecoveryInfo.realtimeBuildLogCtrlStatus > DISABLE)
#define REFORM_CTRL_VERSION 1
typedef struct st_reformer_ctrl {
    uint32 version;
    uint64 list_stable; // stable instances list
    int primaryInstId;
    int recoveryInstId; // abandoned in xlog merge
    SSGlobalClusterState clusterStatus;
    ClusterRunMode clusterRunMode;
    pg_crc32c crc;
} ss_reformer_ctrl_t;

typedef struct st_reform_info {
    bool in_reform;
    dms_role_t dms_role;
    dms_reform_type_t reform_type;
    unsigned long long bitmap_nodes;
    unsigned long long bitmap_reconnect;
    TimestampTz reform_start_time;
    TimestampTz reform_end_time;
    uint64 old_bitmap;      // Save the cluster nodes bitmap before REFORM
    uint64 new_bitmap;      // Save the cluster nodes bitmap after REFORM
    
    TimestampTz redo_start_time;
    TimestampTz redo_end_time;
    TimestampTz construct_hashmap;
    uint64 redo_total_bytes;
    bool reform_success;
    bool is_hashmap_constructed;
    TimestampTz reform_ver;
    TimestampTz reform_ver_startup_wait;
    bool switchover_demote_failure_signal_handled;
    bool needSyncConfig;
} ss_reform_info_t;

typedef enum st_reform_ckpt_status {
    NOT_ACTIVE = 0,
    NOT_ALLOW_CKPT,
    ALLOW_CKPT
} reform_ckpt_status_t;

typedef enum st_ondemand_realtime_build_status {
    DISABLED = 0,
    BUILD_NORMAL,
    READY_TO_BUILD,
    BUILD_TO_DISABLED,
    BUILD_TO_REDO
} ondemand_realtime_build_status_t;

typedef enum st_ondemand_recovery_pause_status {
    NOT_PAUSE = 0,
    PAUSE_FOR_SYNC_REDO,
    PAUSE_FOR_PRUNE_HASHMAP,
    PAUSE_FOR_PRUNE_SEG_QUEUE,
    PAUSE_FOR_PRUNE_TRXN_QUEUE
} ondemand_recovery_pause_status_t;

typedef struct ondemand_recovery_stat {
    XLogRecPtr checkpointPtr;
    XLogRecPtr replayedPtr;
    uint32 hmpUsedBlkNum;
    uint32 hmpTotalBlkNum;
    uint32 trxnQueueNum;
    uint32 segQueueNum;
    bool inOndemandRecovery;
    SSGlobalClusterState ondemandRecoveryStatus;
    ondemand_realtime_build_status_t realtimeBuildStatus;
    ondemand_recovery_pause_status_t recoveryPauseStatus;
    uint32 recordItemNum;
    uint64 recordItemMemUsed;
} ondemand_recovery_stat;

#define QUEUE_STAT_MAX_PIPLINES 4
typedef struct realtime_build_queue_stat {
    uint32 batch_num;
    uint32 readline_queue_size;
    uint32 trxn_manager_queue_size;
    uint32 trxn_worker_queue_size;
    uint32 segworker_queue_size;
    uint32 batchredo_queue_size[QUEUE_STAT_MAX_PIPLINES];
    uint32 redomanager_queue_size[QUEUE_STAT_MAX_PIPLINES];
    uint32 hashmap_manager_queue_size[QUEUE_STAT_MAX_PIPLINES];
} realtime_build_queue_stat;

typedef struct realtime_build_log_ctrl {
    int64 currentRTO;
    TimestampTz prevReplyTime;
    TimestampTz prevCalculateTime;
    TimestampTz replyTime;
    uint64 periodTotalBuild;
    uint64 buildRate;
    XLogRecPtr prevBuildPtr;
    XLogRecPtr realtimeBuildPtr;
    int sleepTime;
} realtime_build_ctrl_t;

typedef enum st_realtime_build_log_ctrl_status {
    DISABLE = 0,        // realtime-build log ctrl disable
    ENABLE_LOG_CTRL,    // primary: receive realtime-build ptr, standby: receive enable log ctrl message
    ENABLE_REPORT_PTR   // standby: start to report realtime-build ptr
} realtime_build_log_ctrl_status;

typedef struct ss_recovery_info {
    bool recovery_pause_flag;
    volatile reform_ckpt_status_t reform_ckpt_status;
    volatile SSGlobalClusterState cluster_ondemand_status;
    LWLock* update_seg_lock;
    bool new_primary_reset_walbuf_flag;
    bool ready_to_startup;              // when DB start (except failover), the flag will set true
    bool startup_reform;                // used to judge DB first start, when first reform finshed set false
    bool restart_failover_flag;         // used to indicate do failover when DB start
    bool reform_ready;
    bool in_failover;      // used to detemin failover scenario, especially for the non-promoting node
    bool in_flushcopy;
    bool no_backend_left;
    bool failover_to_job;
    bool startup_need_exit_normally;        //used in alive failover
    bool recovery_trapped_in_page_request;   //used in alive failover
    bool in_ondemand_recovery;
    bool is_disaster_extreme_redo;          // used in ss disaster cluster for is enable extreme redo
    volatile ondemand_realtime_build_status_t ondemand_realtime_build_status;
    bool dorado_sharestorage_inited;        // used in dorado mode
    bool disaster_cluster_promoting;         // standby cluster is promoting
    volatile ondemand_recovery_pause_status_t ondemand_recovery_pause_status;
    bool realtime_build_in_reform; // used to avoid starting realtime build during reform
    volatile realtime_build_log_ctrl_status realtimeBuildLogCtrlStatus;         // used in realtime build log ctrl
    slock_t sleepTimeSyncLock;         // used in realtime build log ctrl
    volatile int globalSleepTime;         // used in realtime build log ctrl
    realtime_build_ctrl_t rtBuildCtrl[DMS_MAX_INSTANCES];         // used in realtime build log ctrl
} ss_recovery_info_t;

typedef struct ondemand_htab_ctrl {
    HTAB *hTab;
    void *nextHTabCtrl;
    XLogRecPtr maxRedoItemPtr;
} ondemand_htab_ctrl_t;

extern bool SSRecoveryNodes();
extern void SSSavePrimaryInstId(int id);
extern void SSInitReformerControlPages(void);
extern bool SSRecoveryApplyDelay();
extern void SShandle_promote_signal();
extern XLogRecPtr SSOndemandRequestPrimaryCkptAndGetRedoLsn();
void StartupOndemandRecovery();
void OndemandRealtimeBuildHandleFailover();


#endif