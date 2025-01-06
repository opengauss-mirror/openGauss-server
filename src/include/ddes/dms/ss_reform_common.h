/*
 * Copyright (c) 2022 Huawei Technologies Co.,Ltd.
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
 * ss_reform_common.h
 *  include header file for ss reform common.
 * 
 * 
 * IDENTIFICATION
 *        src/include/ddes/dms/ss_reform_common.h
 *
 * ---------------------------------------------------------------------------------------
 */
#include "access/xlog_basic.h"
#include "access/xlogdefs.h"

#define BAK_CTRL_FILE_NUM 2
#define BIT_NUM_INT32 32
#define FAILOVER_PERIOD 50
#define REFORM_WAIT_TIME 10000 /* 0.01 sec */
#define REFORM_WAIT_LONG 100000 /* 0.1 sec */
#define WAIT_REFORM_CTRL_REFRESH_TRIES 1000
#define BACKEND_TYPE_NORMAL 0x0001  /* normal backend */
#define BACKEND_TYPE_AUTOVAC 0x0002 /* autovacuum worker process */

#define WAIT_PMSTATE_UPDATE_TRIES 100

#define REFORM_CTRL_VERSION 1

#define SS_RTO_LIMIT (10 * 1000 * 1000) /* 10 sec */

extern XLogRecPtr lastLsn;

typedef struct SSBroadcastCancelTrx {
    SSBroadcastOp type; // must be first
} SSBroadcastCancelTrx;

// Notice: every step should not be dependent on its Value, Value is only used for distinguish different step
typedef enum en_reform_step {
    DMS_REFORM_STEP_DONE,
    DMS_REFORM_STEP_PREPARE,                        // just sync wait reformer. do nothing
    DMS_REFORM_STEP_START,                          // no need to set last_fail before this step
    DMS_REFORM_STEP_DISCONNECT,
    DMS_REFORM_STEP_RECONNECT,
    DMS_REFORM_STEP_DRC_CLEAN,
    DMS_REFORM_STEP_FULL_CLEAN,
    DMS_REFORM_STEP_MIGRATE,
    DMS_REFORM_STEP_REBUILD,
    DMS_REFORM_STEP_REMASTER,
    DMS_REFORM_STEP_REPAIR,
    DMS_REFORM_STEP_SWITCH_LOCK,
    DMS_REFORM_STEP_SWITCHOVER_DEMOTE,
    DMS_REFORM_STEP_RECOVERY,
    DMS_REFORM_STEP_RECOVERY_OPENGAUSS,
    DMS_REFORM_STEP_DRC_RCY_CLEAN,
    DMS_REFORM_STEP_CTL_RCY_CLEAN,
    DMS_REFORM_STEP_TXN_DEPOSIT,
    DMS_REFORM_STEP_ROLLBACK_PREPARE,
    DMS_REFORM_STEP_ROLLBACK_START,
    DMS_REFORM_STEP_SUCCESS,
    DMS_REFORM_STEP_SELF_FAIL,                      // cause by self
    DMS_REFORM_STEP_REFORM_FAIL,                    // cause by notification from reformer
    DMS_REFORM_STEP_SYNC_WAIT,                      // tips: can not use before reconnect
    DMS_REFORM_STEP_PAGE_ACCESS,                    // set page accessible
    DMS_REFORM_STEP_DW_RECOVERY,                    // recovery the dw area
    DMS_REFORM_STEP_DF_RECOVERY,
    DMS_REFORM_STEP_SPACE_RELOAD,
    DMS_REFORM_STEP_DRC_ACCESS,                     // set drc accessible
    DMS_REFORM_STEP_DRC_INACCESS,                   // set drc inaccessible
    DMS_REFORM_STEP_SWITCHOVER_PROMOTE_OPENGAUSS,
    DMS_REFORM_STEP_FAILOVER_PROMOTE_OPENGAUSS,
    DMS_REFORM_STEP_STARTUP_OPENGAUSS,
    DMS_REFORM_STEP_DONE_CHECK,
    DMS_REFORM_STEP_SET_PHASE,
    DMS_REFORM_STEP_WAIT_DB,
    DMS_REFORM_STEP_FILE_UNBLOCKED,
    DMS_REFORM_STEP_FILE_BLOCKED,
    DMS_REFORM_STEP_UPDATE_SCN,
    DMS_REFORM_STEP_WAIT_CKPT,
    DMS_REFORM_STEP_DRC_VALIDATE,
    DMS_REFORM_STEP_LOCK_INSTANCE,                  // get X mode instance lock for reform
    DMS_REFORM_STEP_PUSH_GCV_AND_UNLOCK,            // push GCV in X instance lock, then unlock X
    DMS_REFORM_STEP_SET_REMOVE_POINT,               // for Gauss100, set rcy point who is removed node after ckpt
    DMS_REFORM_STEP_RESET_USER,
    DMS_REFORM_STEP_RECOVERY_ANALYSE,               // set rcy flag for pages which in redo log
    DMS_REFORM_STEP_XA_DRC_ACCESS,                  // set xa drc access
    DMS_REFORM_STEP_DDL_2PHASE_DRC_ACCESS,
    DMS_REFORM_STEP_DDL_2PHASE_RCY,
    DMS_REFORM_STEP_DRC_LOCK_ALL_ACCESS,
    DMS_REFORM_STEP_SET_CURRENT_POINT,
    DMS_REFORM_STEP_STANDBY_UPDATE_REMOVE_NODE_CTRL,
    DMS_REFORM_STEP_STANDBY_STOP_THREAD,
    DMS_REFORM_STEP_STANDBY_RELOAD_NODE_CTRL,
    DMS_REFORM_STEP_STANDBY_SET_ONLINE_LIST,
    DMS_REFORM_STEP_STOP_SERVER,
    DMS_REFORM_STEP_RESUME_SERVER_FOR_REFORMER,
    DMS_REFORM_STEP_RESUME_SERVER_FOR_PARTNER,
    DMS_REFORM_STEP_START_LRPL,                     // start log replay
    DMS_REFORM_STEP_STOP_LRPL,                      // stop log replay

    DMS_REFORM_STEP_AZ_SWITCH_DEMOTE_PHASE1,        // AZ SWITCHOVER primary to standby
    DMS_REFORM_STEP_AZ_SWITCH_DEMOTE_STOP_CKPT,
    DMS_REFORM_STEP_AZ_SWITCH_DEMOTE_UPDATE_NODE_CTRL,
    DMS_REFORM_STEP_AZ_SWITCH_DEMOTE_CHANGE_ROLE,
    DMS_REFORM_STEP_AZ_SWITCH_DEMOTE_APPROVE,       // AZ SWITCHOVER primary to standby
    DMS_REFORM_STEP_AZ_SWITCH_DEMOTE_PHASE2,        // AZ SWITCHOVER primary to standby
    DMS_REFORM_STEP_AZ_SWITCH_PROMOTE_PREPARE,             // AZ SWITCHOVER standby to primary
    DMS_REFORM_STEP_AZ_SWITCH_PROMOTE_PHASE1,              // AZ SWITCHOVER standby to primary
    DMS_REFORM_STEP_AZ_SWITCH_PROMOTE_PHASE2,              // AZ SWITCHOVER standby to primary
    DMS_REFORM_STEP_AZ_FAILOVER_PROMOTE_PHASE1,     // AZ FAILOVER standby to primary
    DMS_REFORM_STEP_AZ_FAILOVER_PROMOTE_RESETLOG,   // AZ FAILOVER standby to primary
    DMS_REFORM_STEP_AZ_FAILOVER_PROMOTE_PHASE2,     // AZ FAILOVER standby to primary
    DMS_REFORM_STEP_RELOAD_TXN,
    DMS_REFORM_STEP_COUNT
} reform_step_t;

int SSReadXlogInternal(XLogReaderState *xlogreader, XLogRecPtr targetPagePtr, XLogRecPtr targetRecPtr, char *buf,
    int readLen, int readFile);
XLogReaderState *SSXLogReaderAllocate(XLogPageReadCB pagereadfunc, void *private_data, Size alignedSize);
void SSUpdateReformerCtrl();
void SSReadReformerCtrl();
void SSClearSegCache();
int SSXLogFileOpenAnyTLI(XLogSegNo segno, int emode, uint32 sources, char* xlog_path);
void SSStandbySetLibpqswConninfo();
void SSDisasterRefreshMode();
void SSDisasterUpdateHAmode();
bool SSPerformingStandbyScenario();
void SSGrantDSSWritePermission(void);
bool SSPrimaryRestartScenario();
bool SSBackendNeedExitScenario();
void SSWaitStartupExit(bool send_signal = true);
void SSHandleStartupWhenReformStart(dms_reform_start_context_t *rs_cxt);
char *SSGetLogHeaderTypeStr();
void ProcessNoCleanBackendsScenario();
void SSProcessForceExit();