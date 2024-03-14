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

#define REFORM_CTRL_VERSION 1
typedef struct st_reformer_ctrl {
    uint32 version;
    uint64 list_stable; // stable instances list
    int primaryInstId;
    int recoveryInstId;
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
} ss_reform_info_t;

typedef enum st_failover_ckpt_status {
    NOT_ACTIVE = 0,
    NOT_ALLOW_CKPT,
    ALLOW_CKPT
} failover_ckpt_status_t;

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
} ondemand_recovery_stat;

typedef struct ss_recovery_info {
    bool recovery_pause_flag;
    volatile failover_ckpt_status_t failover_ckpt_status;
    char recovery_xlog_dir[MAXPGPATH];
    int recovery_inst_id;
    volatile SSGlobalClusterState cluster_ondemand_status;
    char xlog_list[DMS_MAX_INSTANCE][MAXPGPATH];;
    LWLock* update_seg_lock;
    bool new_primary_reset_walbuf_flag;
    bool ready_to_startup;              // when DB start (except failover), the flag will set true
    bool startup_reform;                // used to judge DB first start, when first reform finshed set false
    bool restart_failover_flag;         // used to indicate do failover when DB start
    bool reform_ready;
    bool in_failover;      // used to detemin failover scenario, especially for the non-promoting node
    bool in_flushcopy;
    bool no_backend_left;
    bool startup_need_exit_normally;        //used in alive failover
    bool recovery_trapped_in_page_request;   //used in alive failover
    bool in_ondemand_recovery;
    volatile ondemand_realtime_build_status_t ondemand_realtime_build_status;
    bool dorado_sharestorage_inited;        // used in dorado mode
    bool disaster_cluster_promoting;         // standby cluster is promoting
    volatile ondemand_recovery_pause_status_t ondemand_recovery_pause_status;
} ss_recovery_info_t;

typedef struct ondemand_htab_ctrl {
    HTAB *hTab;
    void *nextHTabCtrl;
    XLogRecPtr maxRedoItemPtr;
} ondemand_htab_ctrl_t;

extern bool SSRecoveryNodes();
extern void SSWaitStartupExit();
extern int SSGetPrimaryInstId();
extern void SSSavePrimaryInstId(int id);
extern void SSInitReformerControlPages(void);
extern bool SSRecoveryApplyDelay();
extern void SShandle_promote_signal();
extern void ss_failover_dw_init();
extern void ss_switchover_promoting_dw_init();
extern XLogRecPtr SSOndemandRequestPrimaryCkptAndGetRedoLsn();
void StartupOndemandRecovery();
void OndemandRealtimeBuildHandleFailover();


#endif