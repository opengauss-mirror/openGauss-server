/* ---------------------------------------------------------------------------------------
 * 
 * cm_msg.h
 * 
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 * 
 * IDENTIFICATION
 *        src/include/cm/cm_msg.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef CM_MSG_H
#define CM_MSG_H

#include "replication/replicainternal.h"
#include "access/xlogdefs.h"
#include "access/redo_statistic_msg.h"
#include "common/config/cm_config.h"
#include <vector>
#include <string>

#define CM_MAX_SENDER_NUM 2
#define LOGIC_CLUSTER_NUMBER (32 + 1)  // max 32 logic + 1 elastic group
#define CM_LOGIC_CLUSTER_NAME_LEN 64
#define CM_MSG_ERR_INFORMATION_LENGTH 1024
#ifndef MAX_INT32
#define MAX_INT32 (2147483600)
#endif
#define CN_INFO_NUM 8
#define RESERVE_NUM 160
#define RESERVE_NUM_USED 4
#define MAX_SYNC_STANDBY_LIST 1024
#define REMAIN_LEN 20

using std::string;
using std::vector;

const uint32 g_barrierSlotVersion = 92380;
const uint32 g_hadrKeyCn = 92381;

const int32 FAILED_SYNC_DATA = 0;
const int32 SUCCESS_SYNC_DATA = 1;

/*
 * Symbols in the following enum are usd in cluster_msg_map_string defined in cm_misc.cpp.
 * Modifictaion to the following enum should be reflected to cluster_msg_map_string as well.
 */
typedef enum CM_MessageType {
    MSG_CTL_CM_SWITCHOVER = 0,
    MSG_CTL_CM_BUILD = 1,
    MSG_CTL_CM_SYNC = 2,
    MSG_CTL_CM_QUERY = 3,
    MSG_CTL_CM_NOTIFY = 4,
    MSG_CTL_CM_BUTT = 5,
    MSG_CM_CTL_DATA_BEGIN = 6,
    MSG_CM_CTL_DATA = 7,
    MSG_CM_CTL_NODE_END = 8,
    MSG_CM_CTL_DATA_END = 9,
    MSG_CM_CTL_COMMAND_ACK = 10,

    MSG_CM_AGENT_SWITCHOVER = 11,
    MSG_CM_AGENT_FAILOVER = 12,
    MSG_CM_AGENT_BUILD = 13,
    MSG_CM_AGENT_SYNC = 14,
    MSG_CM_AGENT_NOTIFY = 15,
    MSG_CM_AGENT_NOTIFY_CN = 16,
    MSG_AGENT_CM_NOTIFY_CN_FEEDBACK = 17,
    MSG_CM_AGENT_CANCEL_SESSION = 18,
    MSG_CM_AGENT_RESTART = 19,
    MSG_CM_AGENT_RESTART_BY_MODE = 20,
    MSG_CM_AGENT_REP_SYNC = 21,
    MSG_CM_AGENT_REP_ASYNC = 22,
    MSG_CM_AGENT_REP_MOST_AVAILABLE = 23,
    MSG_CM_AGENT_BUTT = 24,

    MSG_AGENT_CM_DATA_INSTANCE_REPORT_STATUS = 25,
    MSG_AGENT_CM_COORDINATE_INSTANCE_STATUS = 26,
    MSG_AGENT_CM_GTM_INSTANCE_STATUS = 27,
    MSG_AGENT_CM_BUTT = 28,

    /****************  =====CAUTION=====  ****************:
    If you want to add a new MessageType, you should add at the end ,
    It's forbidden to insert new MessageType at middle,  it will change the other MessageType value.
    The MessageType is transfered between cm_agent and cm_server on different host,
    You should ensure the type value be identical and compatible between old and new versions */

    MSG_CM_CM_VOTE = 29,
    MSG_CM_CM_BROADCAST = 30,
    MSG_CM_CM_NOTIFY = 31,
    MSG_CM_CM_SWITCHOVER = 32,
    MSG_CM_CM_FAILOVER = 33,
    MSG_CM_CM_SYNC = 34,
    MSG_CM_CM_SWITCHOVER_ACK = 35,
    MSG_CM_CM_FAILOVER_ACK = 36,
    MSG_CM_CM_ROLE_CHANGE_NOTIFY = 37,
    MSG_CM_CM_REPORT_SYNC = 38,

    MSG_AGENT_CM_HEARTBEAT = 39,
    MSG_CM_AGENT_HEARTBEAT = 40,
    MSG_CTL_CM_SET = 41,
    MSG_CTL_CM_SWITCHOVER_ALL = 42,
    MSG_CM_CTL_SWITCHOVER_ALL_ACK = 43,
    MSG_CTL_CM_BALANCE_CHECK = 44,
    MSG_CM_CTL_BALANCE_CHECK_ACK = 45,
    MSG_CTL_CM_BALANCE_RESULT = 46,
    MSG_CM_CTL_BALANCE_RESULT_ACK = 47,
    MSG_CTL_CM_QUERY_CMSERVER = 48,
    MSG_CM_CTL_CMSERVER = 49,

    MSG_TYPE_BUTT = 50,
    MSG_CM_AGENT_NOTIFY_CN_CENTRAL_NODE = 51,
    MSG_CM_AGENT_DROP_CN = 52,
    MSG_CM_AGENT_DROPPED_CN = 53,
    MSG_AGENT_CM_FENCED_UDF_INSTANCE_STATUS = 54,
    MSG_CTL_CM_SWITCHOVER_FULL = 55,             /* inform cm agent to do switchover -A */
    MSG_CM_CTL_SWITCHOVER_FULL_ACK = 56,         /* inform cm ctl that cm server is doing swtichover -A */
    MSG_CM_CTL_SWITCHOVER_FULL_DENIED = 57,      /* inform cm ctl that switchover -A is denied by cm server */
    MSG_CTL_CM_SWITCHOVER_FULL_CHECK = 58,       /* cm ctl inform cm server to check if swtichover -A is done */
    MSG_CM_CTL_SWITCHOVER_FULL_CHECK_ACK = 59,   /* inform cm ctl that swtichover -A is done */
    MSG_CTL_CM_SWITCHOVER_FULL_TIMEOUT = 60,     /* cm ctl inform cm server to swtichover -A timed out */
    MSG_CM_CTL_SWITCHOVER_FULL_TIMEOUT_ACK = 61, /* inform cm ctl that swtichover -A stopped */

    MSG_CTL_CM_SETMODE = 62, /* new mode */
    MSG_CM_CTL_SETMODE_ACK = 63,

    MSG_CTL_CM_SWITCHOVER_AZ = 64,             /* inform cm agent to do switchover -zazName */
    MSG_CM_CTL_SWITCHOVER_AZ_ACK = 65,         /* inform cm ctl that cm server is doing swtichover -zazName */
    MSG_CM_CTL_SWITCHOVER_AZ_DENIED = 66,      /* inform cm ctl that switchover -zazName is denied by cm server */
    MSG_CTL_CM_SWITCHOVER_AZ_CHECK = 67,       /* cm ctl inform cm server to check if swtichover -zazName is done */
    MSG_CM_CTL_SWITCHOVER_AZ_CHECK_ACK = 68,   /* inform cm ctl that swtichover -zazName is done */
    MSG_CTL_CM_SWITCHOVER_AZ_TIMEOUT = 69,     /* cm ctl inform cm server to swtichover -zazName timed out */
    MSG_CM_CTL_SWITCHOVER_AZ_TIMEOUT_ACK = 70, /* inform cm ctl that swtichover -zazName stopped */

    MSG_CM_CTL_SET_ACK = 71,
    MSG_CTL_CM_GET = 72,
    MSG_CM_CTL_GET_ACK = 73,
    MSG_CM_AGENT_GS_GUC = 74,
    MSG_AGENT_CM_GS_GUC_ACK = 75,
    MSG_CM_CTL_SWITCHOVER_INCOMPLETE_ACK = 76,
    MSG_CM_CM_TIMELINE = 77, /* when restart cluster , cmserver primary and standy timeline */
    MSG_CM_BUILD_DOING = 78,
    MSG_AGENT_CM_ETCD_CURRENT_TIME = 79, /* etcd clock monitoring message */
    MSG_CM_QUERY_INSTANCE_STATUS = 80,
    MSG_CM_SERVER_TO_AGENT_CONN_CHECK = 81,
    MSG_CTL_CM_GET_DATANODE_RELATION = 82, /* depracated for the removal of quick switchover */
    MSG_CM_BUILD_DOWN = 83,
    MSG_CTL_CM_HOTPATCH = 84,
    MSG_CM_SERVER_REPAIR_CN_ACK = 85,
    MSG_CTL_CM_DISABLE_CN = 86,
    MSG_CTL_CM_DISABLE_CN_ACK = 87,
    MSG_CM_AGENT_LOCK_NO_PRIMARY = 88,
    MSG_CM_AGENT_LOCK_CHOSEN_PRIMARY = 89,
    MSG_CM_AGENT_UNLOCK = 90,
    MSG_CTL_CM_STOP_ARBITRATION = 91,
    MSG_CTL_CM_FINISH_REDO = 92,
    MSG_CM_CTL_FINISH_REDO_ACK = 93,
    MSG_CM_AGENT_FINISH_REDO = 94,
    MSG_CTL_CM_FINISH_REDO_CHECK = 95,
    MSG_CM_CTL_FINISH_REDO_CHECK_ACK = 96,
    MSG_AGENT_CM_KERBEROS_STATUS = 97,
    MSG_CTL_CM_QUERY_KERBEROS = 98,
    MSG_CTL_CM_QUERY_KERBEROS_ACK = 99,
    MSG_AGENT_CM_DISKUSAGE_STATUS = 100,
    MSG_CM_AGENT_OBS_DELETE_XLOG = 101,
    MSG_CM_AGENT_DROP_CN_OBS_XLOG = 102,
    MSG_AGENT_CM_DATANODE_INSTANCE_BARRIER = 103,
    MSG_AGENT_CM_COORDINATE_INSTANCE_BARRIER = 104,
    MSG_CTL_CM_GLOBAL_BARRIER_QUERY = 105,
    MSG_CM_CTL_GLOBAL_BARRIER_DATA = 106,
    MSG_CM_CTL_GLOBAL_BARRIER_DATA_BEGIN = 107,
    MSG_CM_CTL_BARRIER_DATA_END = 108,
    MSG_CM_CTL_BACKUP_OPEN = 109,
    MSG_CM_AGENT_DN_SYNC_LIST = 110,
    MSG_AGENT_CM_DN_SYNC_LIST = 111,
    MSG_CTL_CM_SWITCHOVER_FAST = 112,
    MSG_CM_AGENT_SWITCHOVER_FAST = 113,
    MSG_CTL_CM_RELOAD = 114,
    MSG_CM_CTL_RELOAD_ACK = 115,
    MSG_CM_CTL_INVALID_COMMAND_ACK = 116,
    MSG_AGENT_CM_CN_OBS_STATUS = 117,
    MSG_CM_AGENT_NOTIFY_CN_RECOVER = 118,
    MSG_CM_AGENT_FULL_BACKUP_CN_OBS = 119,
    MSG_AGENT_CM_BACKUP_STATUS_ACK = 120,
    MSG_CM_AGENT_REFRESH_OBS_DEL_TEXT = 121,
    MSG_AGENT_CM_INSTANCE_BARRIER_NEW = 122,
    MSG_CTL_CM_GLOBAL_BARRIER_QUERY_NEW = 123,
    MSG_CM_CTL_GLOBAL_BARRIER_DATA_BEGIN_NEW = 124,
    MSG_CM_AGENT_DATANODE_INSTANCE_BARRIER = 125,
    MSG_CM_AGENT_COORDINATE_INSTANCE_BARRIER = 126,
} CM_MessageType;

#define UNDEFINED_LOCKMODE 0
#define POLLING_CONNECTION 1
#define SPECIFY_CONNECTION 2
#define PROHIBIT_CONNECTION 3

#define INSTANCE_ROLE_INIT 0
#define INSTANCE_ROLE_PRIMARY 1
#define INSTANCE_ROLE_STANDBY 2
#define INSTANCE_ROLE_PENDING 3
#define INSTANCE_ROLE_NORMAL 4
#define INSTANCE_ROLE_UNKNOWN 5
#define INSTANCE_ROLE_DUMMY_STANDBY 6
#define INSTANCE_ROLE_DELETED 7
#define INSTANCE_ROLE_DELETING 8
#define INSTANCE_ROLE_READONLY 9
#define INSTANCE_ROLE_OFFLINE 10
#define INSTANCE_ROLE_MAIN_STANDBY 11
#define INSTANCE_ROLE_CASCADE_STANDBY 12

#define INSTANCE_ROLE_FIRST_INIT 1
#define INSTANCE_ROLE_HAVE_INIT 2

#define INSTANCE_DATA_REPLICATION_SYNC 1
#define INSTANCE_DATA_REPLICATION_ASYNC 2
#define INSTANCE_DATA_REPLICATION_MOST_AVAILABLE 3
#define INSTANCE_DATA_REPLICATION_POTENTIAL_SYNC 4
#define INSTANCE_DATA_REPLICATION_QUORUM 5
#define INSTANCE_DATA_REPLICATION_UNKONWN 6

#define INSTANCE_TYPE_GTM 1
#define INSTANCE_TYPE_DATANODE 2
#define INSTANCE_TYPE_COORDINATE 3
#define INSTANCE_TYPE_FENCED_UDF 4
#define INSTANCE_TYPE_UNKNOWN 5

#define INSTANCE_WALSNDSTATE_STARTUP 0
#define INSTANCE_WALSNDSTATE_BACKUP 1
#define INSTANCE_WALSNDSTATE_CATCHUP 2
#define INSTANCE_WALSNDSTATE_STREAMING 3
#define INSTANCE_WALSNDSTATE_DUMPLOG 4
const int INSTANCE_WALSNDSTATE_NORMAL = 5;
const int INSTANCE_WALSNDSTATE_UNKNOWN = 6;

#define CON_OK 0
#define CON_BAD 1
#define CON_STARTED 2
#define CON_MADE 3
#define CON_AWAITING_RESPONSE 4
#define CON_AUTH_OK 5
#define CON_SETEN 6
#define CON_SSL_STARTUP 7
#define CON_NEEDED 8
#define CON_UNKNOWN 9
#define CON_MANUAL_STOPPED 10
#define CON_DISK_DEMAGED 11
#define CON_PORT_USED 12
#define CON_NIC_DOWN 13
#define CON_GTM_STARTING 14

#define CM_SERVER_UNKNOWN 0
#define CM_SERVER_PRIMARY 1
#define CM_SERVER_STANDBY 2
#define CM_SERVER_INIT 3
#define CM_SERVER_DOWN 4

#define CM_ETCD_UNKNOWN 0
#define CM_ETCD_FOLLOWER 1
#define CM_ETCD_LEADER 2
#define CM_ETCD_DOWN 3

#define SWITCHOVER_UNKNOWN 0
#define SWITCHOVER_FAIL 1
#define SWITCHOVER_SUCCESS 2
#define SWITCHOVER_EXECING 3
#define SWITCHOVER_PARTLY_SUCCESS 4
#define SWITCHOVER_ABNORMAL 5
#define INVALID_COMMAND 6


#define UNKNOWN_BAD_REASON 0
#define PORT_BAD_REASON 1
#define NIC_BAD_REASON 2
#define DISC_BAD_REASON 3
#define STOPPED_REASON 4
#define CN_DELETED_REASON 5

#define KERBEROS_STATUS_UNKNOWN 0
#define KERBEROS_STATUS_NORMAL 1
#define KERBEROS_STATUS_ABNORMAL 2
#define KERBEROS_STATUS_DOWN 3

#define HOST_LENGTH 32
#define BARRIERLEN 40
#define MAX_SLOT_NAME_LEN 64
#define MAX_BARRIER_SLOT_COUNT 5
// the length of cm_serer sync msg  is 9744, msg type is MSG_CM_CM_REPORT_SYNC
#define CM_MSG_MAX_LENGTH (12288 * 2)

#define CMAGENT_NO_CCN "NoCentralNode"

#define OBS_DEL_VERSION_V1 (1)
#define DEL_TEXT_HEADER_LEN_V1 (10)  // version(4->V%3d) + delCount(4->C%3d) + '\n' + '\0'
#define CN_BUILD_TASK_ID_MAX_LEN   (21)      // cnId(4) + cmsId(4) + time(12->yyMMddHH24mmss) + 1
#define MAX_OBS_CN_COUNT   (64)
#define MAX_OBS_DEL_TEXT_LEN  (CN_BUILD_TASK_ID_MAX_LEN * MAX_OBS_CN_COUNT + DEL_TEXT_HEADER_LEN_V1)

extern int g_gtm_phony_dead_times;
extern int g_dn_phony_dead_times[CM_MAX_DATANODE_PER_NODE];
extern int g_cn_phony_dead_times;

typedef enum {DN, CN} GetinstanceType;

typedef struct DatanodeSyncList {
    int count;
    uint32 dnSyncList[CM_PRIMARY_STANDBY_NUM];
    int syncStandbyNum;
    // remain
    int remain;
    char remainStr[DN_SYNC_LEN];
} DatanodeSyncList;

typedef struct cm_msg_type {
    int msg_type;
} cm_msg_type;

typedef struct cm_switchover_incomplete_msg {
    int msg_type;
    char errMsg[CM_MSG_ERR_INFORMATION_LENGTH];
} cm_switchover_incomplete_msg;

typedef struct cm_redo_stats {
    int is_by_query;
    uint64 redo_replayed_speed;
    XLogRecPtr standby_last_replayed_read_Ptr;
} cm_redo_stats;

typedef struct ctl_to_cm_stop_arbitration {
    int msg_type;
} ctl_to_cm_stop_arbitration;

typedef struct ctl_to_cm_switchover {
    int msg_type;
    char azName[CM_AZ_NAME];
    uint32 node;
    uint32 instanceId;
    int instance_type;
    int wait_seconds;
} ctl_to_cm_switchover;

typedef struct ctl_to_cm_failover {
    int msg_type;
    uint32 node;
    uint32 instanceId;
    int instance_type;
    int wait_seconds;
} ctl_to_cm_failover;

typedef struct cm_to_ctl_finish_redo_check_ack {
    int msg_type;
    int finish_redo_count;
} cm_to_ctl_finish_redo_check_ack;

typedef struct ctl_to_cm_finish_redo {
    int msg_type;
} ctl_to_cm_finish_redo;

#define CM_CTL_UNFORCE_BUILD 0
#define CM_CTL_FORCE_BUILD 1

typedef struct ctl_to_cm_build {
    int msg_type;
    uint32 node;
    uint32 instanceId;
    int instance_type;
    int wait_seconds;
    int force_build;
    int full_build;
} ctl_to_cm_build;
typedef struct ctl_to_cm_global_barrier_query {
    int msg_type;
}ctl_to_cm_global_barrier_query;

typedef struct ctl_to_cm_query {
    int msg_type;
    uint32 node;
    uint32 instanceId;
    int instance_type;
    int wait_seconds;
    int detail;
    int relation;
} ctl_to_cm_query;

#define NOTIFY_MSG_RESERVED (32)

typedef struct Cm2AgentNotifyCnRecoverByObs_t {
    int msg_type;
    uint32 instanceId;
    bool changeKeyCn;
    uint32 syncCnId;
    char slotName[MAX_SLOT_NAME_LEN];
} Cm2AgentNotifyCnRecoverByObs;

typedef struct Cm2AgentBackupCn2Obs_t {
    int msg_type;
    uint32 instanceId;
    char slotName[MAX_SLOT_NAME_LEN];
    char taskIdStr[CN_BUILD_TASK_ID_MAX_LEN];
} Cm2AgentBackupCn2Obs;

typedef struct Agent2CMBackupStatusAck_t {
    int msg_type;
    uint32 node;
    uint32 instanceId;
    char slotName[MAX_SLOT_NAME_LEN];
    char taskIdStr[CN_BUILD_TASK_ID_MAX_LEN];
    int32 status;
} Agent2CMBackupStatusAck;

typedef struct Cm2AgentRefreshObsDelText_t {
    int msg_type;
    uint32 instanceId;
    char slotName[MAX_SLOT_NAME_LEN];
    char obsDelCnText[MAX_OBS_DEL_TEXT_LEN];
} Cm2AgentRefreshObsDelText;

typedef struct ctl_to_cm_notify {
    CM_MessageType msg_type;
    ctlToCmNotifyDetail detail;
} ctl_to_cm_notify;

typedef struct ctl_to_cm_disable_cn {
    int msg_type;
    uint32 instanceId;
    int wait_seconds;
} ctl_to_cm_disable_cn;

typedef struct ctl_to_cm_disable_cn_ack {
    int msg_type;
    bool disable_ok;
    char errMsg[CM_MSG_ERR_INFORMATION_LENGTH];
} ctl_to_cm_disable_cn_ack;

typedef enum arbitration_mode {
    UNKNOWN_ARBITRATION = 0,
    MAJORITY_ARBITRATION = 1,
    MINORITY_ARBITRATION = 2
} arbitration_mode;

typedef enum cm_start_mode {
    UNKNOWN_START = 0,
    MAJORITY_START = 1,
    MINORITY_START = 2,
    OTHER_MINORITY_START = 3
} cm_start_mode;

typedef enum switchover_az_mode {
    UNKNOWN_SWITCHOVER_AZ = 0,
    NON_AUTOSWITCHOVER_AZ = 1,
    AUTOSWITCHOVER_AZ = 2
} switchover_az_mode;

typedef enum logic_cluster_restart_mode {
    UNKNOWN_LOGIC_CLUSTER_RESTART = 0,
    INITIAL_LOGIC_CLUSTER_RESTART = 1,
    MODIFY_LOGIC_CLUSTER_RESTART = 2
} logic_cluster_restart_mode;

typedef enum cluster_mode {
    INVALID_CLUSTER_MODE = 0,
    ONE_MASTER_1_SLAVE,
    ONE_MASTER_2_SLAVE,
    ONE_MASTER_3_SLAVE,
    ONE_MASTER_4_SLAVE,
    ONE_MASTER_5_SLAVE
} cluster_mode;

typedef enum synchronous_standby_mode {
    AnyFirstNo = 0, /* don't have */
    AnyAz1,         /* ANY 1(az1) */
    FirstAz1,       /* FIRST 1(az1) */
    AnyAz2,         /* ANY 1(az2) */
    FirstAz2,       /* FIRST 1(az2) */
    Any2Az1Az2,     /* ANY 2(az1,az2) */
    First2Az1Az2,   /* FIRST 2(az1,az2) */
    Any3Az1Az2,     /* ANY 3(az1, az2) */
    First3Az1Az2    /* FIRST 3(az1, az2) */
} synchronous_standby_mode;

typedef enum {
    CLUSTER_PRIMARY = 0,
    CLUSTER_OBS_STANDBY = 1,
    CLUSTER_STREAMING_STANDBY = 2
} ClusterRole;

typedef enum {
    DISASTER_RECOVERY_NULL = 0,
    DISASTER_RECOVERY_OBS = 1,
    DISASTER_RECOVERY_STREAMING = 2
} DisasterRecoveryType;

typedef enum {
    INSTALL_TYPE_DEFAULT = 0,
    INSTALL_TYPE_SHARE_STORAGE = 1,
    INSTALL_TYPE_STREAMING = 2
}ClusterInstallType;

typedef struct ctl_to_cm_set {
    int msg_type;
    int log_level;
    uint32 logic_cluster_delay;
    arbitration_mode cm_arbitration_mode;
    switchover_az_mode cm_switchover_az_mode;
    logic_cluster_restart_mode cm_logic_cluster_restart_mode;
} ctl_to_cm_set, cm_to_ctl_get;

typedef struct cm_to_agent_switchover {
    int msg_type;
    uint32 node;
    uint32 instanceId;
    int instance_type;
    int wait_seconds;
    int role;
    uint32 term;
} cm_to_agent_switchover;

typedef struct cm_to_agent_failover {
    int msg_type;
    uint32 node;
    uint32 instanceId;
    int instance_type;
    int wait_seconds;
    uint32 term;
} cm_to_agent_failover;

typedef struct cm_to_agent_build {
    int msg_type;
    uint32 node;
    uint32 instanceId;
    int instance_type;
    int wait_seconds;
    int role;
    int full_build;
    uint32 term;
} cm_to_agent_build;

typedef struct cm_to_agent_lock1 {
    int msg_type;
    uint32 node;
    uint32 instanceId;
} cm_to_agent_lock1;

typedef struct cm_to_agent_obs_delete_xlog {
    int msg_type;
    uint32 node;
    uint32 instanceId;
    uint64 lsn;
} cm_to_agent_obs_delete_xlog;

typedef struct cm_to_agent_lock2 {
    int msg_type;
    uint32 node;
    uint32 instanceId;
    char disconn_host[HOST_LENGTH];
    uint32 disconn_port;
} cm_to_agent_lock2;

typedef struct cm_to_agent_unlock {
    int msg_type;
    uint32 node;
    uint32 instanceId;
} cm_to_agent_unlock;

typedef struct cm_to_agent_finish_redo {
    int msg_type;
    uint32 node;
    uint32 instanceId;
    bool is_finish_redo_cmd_sent;
} cm_to_agent_finish_redo;

typedef struct cm_to_agent_gs_guc {
    int msg_type;
    uint32 node;
    uint32 instanceId;
    synchronous_standby_mode type;
} cm_to_agent_gs_guc;

typedef struct agent_to_cm_gs_guc_feedback {
    int msg_type;
    uint32 node;
    uint32 instanceId; /* node of this agent */
    synchronous_standby_mode type;
    bool status; /* gs guc command exec status */
} agent_to_cm_gs_guc_feedback;

typedef struct CmToAgentGsGucSyncList {
    int msgType;
    uint32 node;
    uint32 instanceId;
    uint32 groupIndex;
    DatanodeSyncList dnSyncList;
    int instanceNum;
    // remain
    int remain[REMAIN_LEN];
} CmToAgentGsGucSyncList;

typedef struct cm_to_agent_notify {
    int msg_type;
    uint32 node;
    uint32 instanceId;
    int role;
    uint32 term;
} cm_to_agent_notify;

/*
 * msg struct using for cmserver to cmagent
 * including primaryed datanode count and datanode instanceId list.
 */
typedef struct cm_to_agent_notify_cn {
    int msg_type;
    uint32 node;       /* node of this coordinator */
    uint32 instanceId; /* coordinator instance id */
    int datanodeCount; /* current count of datanode got primaryed */
    uint32 coordinatorId;
    int notifyCount;
    /* datanode instance id array */
    uint32 datanodeId[FLEXIBLE_ARRAY_MEMBER]; /* VARIABLE LENGTH ARRAY */
} cm_to_agent_notify_cn;

/*
 * msg struct using for cmserver to cmagent
 * including primaryed datanode count and datanode instanceId list.
 */
typedef struct cm_to_agent_drop_cn {
    int msg_type;
    uint32 node;       /* node of this coordinator */
    uint32 instanceId; /* coordinator instance id */
    uint32 coordinatorId;
    int role;
    bool delay_repair;
} cm_to_agent_drop_cn;

typedef struct cm_to_agent_notify_cn_central_node {
    int msg_type;
    uint32 node;                 /* node of this coordinator */
    uint32 instanceId;           /* coordinator instance id */
    char cnodename[NAMEDATALEN]; /* central node id */
    char nodename[NAMEDATALEN];
} cm_to_agent_notify_cn_central_node;

/*
 * msg struct using for cmserver to cmagent
 * including primaryed datanode count and datanode instanceId list.
 */
typedef struct cm_to_agent_cancel_session {
    int msg_type;
    uint32 node;       /* node of this coordinator */
    uint32 instanceId; /* coordinator instance id */
} cm_to_agent_cancel_session;

/*
 * msg struct using for cmagent to cmserver
 * feedback msg for notify cn.
 */
typedef struct agent_to_cm_notify_cn_feedback {
    int msg_type;
    uint32 node;       /* node of this coordinator */
    uint32 instanceId; /* coordinator instance id */
    bool status;       /* notify command exec status */
    int notifyCount;
} agent_to_cm_notify_cn_feedback;

typedef struct BackupInfo_t {
    uint32 localKeyCnId;
    uint32 obsKeyCnId;
    char slotName[MAX_SLOT_NAME_LEN];
    char obsDelCnText[MAX_OBS_DEL_TEXT_LEN];
} BackupInfo;

typedef struct Agent2CmBackupInfoRep_t {
    int msg_type;
    uint32 instanceId; /* coordinator instance id */
    uint32 slotCount;
    BackupInfo backupInfos[MAX_BARRIER_SLOT_COUNT];
} Agent2CmBackupInfoRep;

typedef struct cm_to_agent_restart {
    int msg_type;
    uint32 node;
    uint32 instanceId;
} cm_to_agent_restart;

typedef struct cm_to_agent_restart_by_mode {
    int msg_type;
    uint32 node;
    uint32 instanceId;
    int role_old;
    int role_new;
} cm_to_agent_restart_by_mode;

typedef struct cm_to_agent_rep_sync {
    int msg_type;
    uint32 node;
    uint32 instanceId;
    int instance_type;
    int sync_mode;
} cm_to_agent_rep_sync;

typedef struct cm_to_agent_rep_async {
    int msg_type;
    uint32 node;
    uint32 instanceId;
    int instance_type;
    int sync_mode;
} cm_to_agent_rep_async;

typedef struct cm_to_agent_rep_most_available {
    int msg_type;
    uint32 node;
    uint32 instanceId;
    int instance_type;
    int sync_mode;
} cm_to_agent_rep_most_available;

typedef struct cm_instance_central_node {
    pthread_rwlock_t rw_lock;
    pthread_mutex_t mt_lock;
    uint32 instanceId;
    uint32 node;
    uint32 recover;
    uint32 isCentral;
    uint32 nodecount;
    char nodename[NAMEDATALEN];
    char cnodename[NAMEDATALEN];
    char* failnodes;
    cm_to_agent_notify_cn_central_node notify;
} cm_instance_central_node;

typedef struct cm_instance_central_node_msg {
    pthread_rwlock_t rw_lock;
    cm_to_agent_notify_cn_central_node notify;
} cm_instance_central_node_msg;

#define MAX_LENGTH_HP_CMD (9)
#define MAX_LENGTH_HP_PATH (256)
#define MAX_LENGTH_HP_RETURN_MSG (1024)

typedef struct cm_hotpatch_msg {
    int msg_type;
    char command[MAX_LENGTH_HP_CMD];
    char path[MAX_LENGTH_HP_PATH];
} cm_hotpatch_msg;

typedef struct cm_hotpatch_ret_msg {
    char msg[MAX_LENGTH_HP_RETURN_MSG];
} cm_hotpatch_ret_msg;

typedef struct cm_to_agent_barrier_info {
    int msg_type;
    uint32 node;
    uint32 instanceId;
    char queryBarrier[BARRIERLEN];
    char targetBarrier[BARRIERLEN];
} cm_to_agent_barrier_info;

#define IP_LEN 64
#define MAX_REPL_CONNINFO_LEN 256
#define MAX_REBUILD_REASON_LEN 256

const int cn_active_unknown = 0;
const int cn_active = 1;
const int cn_inactive = 2;

#define INSTANCE_HA_STATE_UNKONWN 0
#define INSTANCE_HA_STATE_NORMAL 1
#define INSTANCE_HA_STATE_NEED_REPAIR 2
#define INSTANCE_HA_STATE_STARTING 3
#define INSTANCE_HA_STATE_WAITING 4
#define INSTANCE_HA_STATE_DEMOTING 5
#define INSTANCE_HA_STATE_PROMOTING 6
#define INSTANCE_HA_STATE_BUILDING 7
#define INSTANCE_HA_STATE_CATCH_UP 8
#define INSTANCE_HA_STATE_COREDUMP 9
#define INSTANCE_HA_STATE_MANUAL_STOPPED 10
#define INSTANCE_HA_STATE_DISK_DAMAGED 11
#define INSTANCE_HA_STATE_PORT_USED 12
#define INSTANCE_HA_STATE_BUILD_FAILED 13
#define INSTANCE_HA_STATE_HEARTBEAT_TIMEOUT 14
#define INSTANCE_HA_STATE_NIC_DOWN 15
#define INSTANCE_HA_STATE_READ_ONLY 16

#define INSTANCE_HA_DATANODE_BUILD_REASON_NORMAL 0
#define INSTANCE_HA_DATANODE_BUILD_REASON_WALSEGMENT_REMOVED 1
#define INSTANCE_HA_DATANODE_BUILD_REASON_DISCONNECT 2
#define INSTANCE_HA_DATANODE_BUILD_REASON_VERSION_NOT_MATCHED 3
#define INSTANCE_HA_DATANODE_BUILD_REASON_MODE_NOT_MATCHED 4
#define INSTANCE_HA_DATANODE_BUILD_REASON_SYSTEMID_NOT_MATCHED 5
#define INSTANCE_HA_DATANODE_BUILD_REASON_TIMELINE_NOT_MATCHED 6
#define INSTANCE_HA_DATANODE_BUILD_REASON_UNKNOWN 7
#define INSTANCE_HA_DATANODE_BUILD_REASON_USER_PASSWD_INVALID 8
#define INSTANCE_HA_DATANODE_BUILD_REASON_CONNECTING 9
#define INSTANCE_HA_DATANODE_BUILD_REASON_DCF_LOG_LOSS 10

#define UNKNOWN_LEVEL 0

typedef uint64 XLogRecPtr;

typedef enum CM_DCF_ROLE {
    DCF_ROLE_UNKNOWN = 0,
    DCF_ROLE_LEADER,
    DCF_ROLE_FOLLOWER,
    DCF_ROLE_PASSIVE,
    DCF_ROLE_LOGGER,
    DCF_ROLE_PRE_CANDIDATE,
    DCF_ROLE_CANDIDATE,
    DCF_ROLE_CEIL,
} DCF_ROLE;

typedef struct agent_to_cm_coordinate_barrier_status_report {
    int msg_type;
    uint32 node;
    uint32 instanceId;
    int instanceType;
    uint64 ckpt_redo_point;
    char global_barrierId[BARRIERLEN];
    char global_achive_barrierId[BARRIERLEN];
    char barrierID [BARRIERLEN];
    char query_barrierId[BARRIERLEN];
    uint64 barrierLSN;
    uint64 archive_LSN;
    uint64 flush_LSN;
    bool is_barrier_exist;
}agent_to_cm_coordinate_barrier_status_report;

typedef struct GlobalBarrierItem_t {
    char slotname[MAX_SLOT_NAME_LEN];
    char globalBarrierId[BARRIERLEN];
    char globalAchiveBarrierId[BARRIERLEN];
} GlobalBarrierItem;

typedef struct GlobalBarrierStatus_t {
    int slotCount;
    GlobalBarrierItem globalBarriers[MAX_BARRIER_SLOT_COUNT];
} GlobalBarrierStatus;

typedef struct LocalBarrierStatus_t {
    uint64 ckptRedoPoint;
    uint64 barrierLSN;
    uint64 archiveLSN;
    uint64 flushLSN;
    char barrierID[BARRIERLEN];
} LocalBarrierStatus;

typedef struct Agent2CmBarrierStatusReport_t {
    int msg_type;
    uint32 node;
    uint32 instanceId;
    int instanceType;
    LocalBarrierStatus localStatus;
    GlobalBarrierStatus globalStatus;
} Agent2CmBarrierStatusReport;

typedef struct agent_to_cm_datanode_barrier_status_report {
    int msg_type;
    uint32 node;
    uint32 instanceId;
    int instanceType;
    uint64 ckpt_redo_point;
    char barrierID [BARRIERLEN];
    uint64 barrierLSN;
    uint64 archive_LSN;
    uint64 flush_LSN;
}agent_to_cm_datanode_barrier_status_report;

typedef struct cm_local_replconninfo {
    int local_role;
    int static_connections;
    int db_state;
    XLogRecPtr last_flush_lsn;
    int buildReason;
    uint32 term;
    uint32 disconn_mode;
    char disconn_host[HOST_LENGTH];
    uint32 disconn_port;
    char local_host[HOST_LENGTH];
    uint32 local_port;
    bool redo_finished;
} cm_local_replconninfo;

typedef struct cm_sender_replconninfo {
    pid_t sender_pid;
    int local_role;
    int peer_role;
    int peer_state;
    int state;
    XLogRecPtr sender_sent_location;
    XLogRecPtr sender_write_location;
    XLogRecPtr sender_flush_location;
    XLogRecPtr sender_replay_location;
    XLogRecPtr receiver_received_location;
    XLogRecPtr receiver_write_location;
    XLogRecPtr receiver_flush_location;
    XLogRecPtr receiver_replay_location;
    int sync_percent;
    int sync_state;
    int sync_priority;
} cm_sender_replconninfo;

typedef struct cm_receiver_replconninfo {
    pid_t receiver_pid;
    int local_role;
    int peer_role;
    int peer_state;
    int state;
    XLogRecPtr sender_sent_location;
    XLogRecPtr sender_write_location;
    XLogRecPtr sender_flush_location;
    XLogRecPtr sender_replay_location;
    XLogRecPtr receiver_received_location;
    XLogRecPtr receiver_write_location;
    XLogRecPtr receiver_flush_location;
    XLogRecPtr receiver_replay_location;
    int sync_percent;
} cm_receiver_replconninfo;

typedef struct cm_gtm_replconninfo {
    int local_role;
    int connect_status;
    TransactionId xid;
    uint64 send_msg_count;
    uint64 receive_msg_count;
    int sync_mode;
} cm_gtm_replconninfo;

typedef struct cm_coordinate_replconninfo {
    int status;
    int db_state;
} cm_coordinate_replconninfo;

typedef enum cm_coordinate_group_mode {
    GROUP_MODE_INIT,
    GROUP_MODE_NORMAL,
    GROUP_MODE_PENDING,
    GROUP_MODE_BUTT
} cm_coordinate_group_mode;

#define AGENT_TO_INSTANCE_CONNECTION_BAD 0
#define AGENT_TO_INSTANCE_CONNECTION_OK 1

#define INSTANCE_PROCESS_DIED 0
#define INSTANCE_PROCESS_RUNNING 1

const int max_cn_node_num_for_old_version = 16;

typedef struct cluster_cn_info {
    uint32 cn_Id;
    uint32 cn_active;
    bool cn_connect;
    bool drop_success;
} cluster_cn_info;

typedef struct agent_to_cm_coordinate_status_report_old {
    int msg_type;
    uint32 node;
    uint32 instanceId;
    int instanceType;
    int connectStatus;
    int processStatus;
    int isCentral;
    char nodename[NAMEDATALEN];
    char logicClusterName[CM_LOGIC_CLUSTER_NAME_LEN];
    char cnodename[NAMEDATALEN];
    cm_coordinate_replconninfo status;
    cm_coordinate_group_mode group_mode;
    bool cleanDropCnFlag;
    bool isCnDnDisconnected;
    cluster_cn_info cn_active_info[max_cn_node_num_for_old_version];
    int cn_restart_counts;
    int phony_dead_times;
} agent_to_cm_coordinate_status_report_old;

typedef struct agent_to_cm_coordinate_status_report {
    int msg_type;
    uint32 node;
    uint32 instanceId;
    int instanceType;
    int connectStatus;
    int processStatus;
    int isCentral;
    char nodename[NAMEDATALEN];
    char logicClusterName[CM_LOGIC_CLUSTER_NAME_LEN];
    char cnodename[NAMEDATALEN];
    cm_coordinate_replconninfo status;
    cm_coordinate_group_mode group_mode;
    bool cleanDropCnFlag;
    bool isCnDnDisconnected;
    uint32 cn_active_info[CN_INFO_NUM];
    int buildReason;
    char resevered[RESERVE_NUM - RESERVE_NUM_USED];
    int cn_restart_counts;
    int phony_dead_times;
} agent_to_cm_coordinate_status_report;

typedef struct agent_to_cm_coordinate_status_report_v1 {
    int msg_type;
    uint32 node;
    uint32 instanceId;
    int instanceType;
    int connectStatus;
    int processStatus;
    int isCentral;
    char nodename[NAMEDATALEN];
    char logicClusterName[CM_LOGIC_CLUSTER_NAME_LEN];
    char cnodename[NAMEDATALEN];
    cm_coordinate_replconninfo status;
    cm_coordinate_group_mode group_mode;
    bool cleanDropCnFlag;
    bool isCnDnDisconnected;
    uint32 cn_active_info[CN_INFO_NUM];
    int buildReason;
    int cn_dn_disconnect_times;
    char resevered[RESERVE_NUM - (2 * RESERVE_NUM_USED)];
    int cn_restart_counts;
    int phony_dead_times;
} agent_to_cm_coordinate_status_report_v1;

typedef struct agent_to_cm_fenced_UDF_status_report {
    int msg_type;
    uint32 nodeid;
    int status;
} agent_to_cm_fenced_UDF_status_report;

typedef struct agent_to_cm_datanode_status_report {
    int msg_type;
    uint32 node;
    uint32 instanceId;
    int instanceType;
    int connectStatus;
    int processStatus;
    cm_local_replconninfo local_status;
    BuildState build_info;
    cm_sender_replconninfo sender_status[CM_MAX_SENDER_NUM];
    cm_receiver_replconninfo receive_status;
    RedoStatsData parallel_redo_status;
    cm_redo_stats local_redo_stats;
    int dn_restart_counts;
    int phony_dead_times;
    int dn_restart_counts_in_hour;
} agent_to_cm_datanode_status_report;

typedef struct AgentToCmserverDnSyncList {
    int msg_type;
    uint32 node;
    uint32 instanceId;
    int instanceType;
    char dnSynLists[DN_SYNC_LEN];
    // remain
    int remain[REMAIN_LEN];
    char remainStr[DN_SYNC_LEN];
} AgentToCmserverDnSyncList;

typedef struct agent_to_cm_gtm_status_report {
    int msg_type;
    uint32 node;
    uint32 instanceId;
    int instanceType;
    int connectStatus;
    int processStatus;
    cm_gtm_replconninfo status;
    int phony_dead_times;
} agent_to_cm_gtm_status_report;

typedef struct agent_to_cm_current_time_report {
    int msg_type;
    uint32 nodeid;
    long int etcd_time;
} agent_to_cm_current_time_report;

typedef struct AgentToCMS_DiskUsageStatusReport {
    int msgType;
    uint32 instanceId;
    uint32 dataPathUsage;
    uint32 logPathUsage;
} AgentToCMS_DiskUsageStatusReport;

typedef struct agent_to_cm_heartbeat {
    int msg_type;
    uint32 node;
    uint32 instanceId;
    int instanceType;
    int cluster_status_request;
} agent_to_cm_heartbeat;

typedef struct DnStatus_t {
    CM_MessageType barrierMsgType;
    agent_to_cm_datanode_status_report reportMsg;
    union {
        agent_to_cm_coordinate_barrier_status_report barrierMsg;
        Agent2CmBarrierStatusReport barrierMsgNew;
    };
} DnStatus;

typedef struct DnSyncListInfo_t {
    pthread_rwlock_t lk_lock;
    AgentToCmserverDnSyncList dnSyncListMsg;
} DnSyncListInfo;

typedef struct CnStatus_t {
    CM_MessageType barrierMsgType;
    agent_to_cm_coordinate_status_report reportMsg;
    Agent2CmBackupInfoRep backupMsg;
    union {
        agent_to_cm_coordinate_barrier_status_report barrierMsg;
        Agent2CmBarrierStatusReport barrierMsgNew;
    };
} CnStatus;

typedef struct coordinate_status_info {
    pthread_rwlock_t lk_lock;
    CnStatus cnStatus;
} coordinate_status_info;

typedef struct datanode_status_info {
    pthread_rwlock_t lk_lock;
    DnStatus dnStatus;
} datanode_status_info;

typedef struct gtm_status_info {
    pthread_rwlock_t lk_lock;
    agent_to_cm_gtm_status_report report_msg;
} gtm_status_info;

typedef struct cm_to_agent_heartbeat {
    int msg_type;
    uint32 node;
    int type;
    int cluster_status;
    uint32 healthCoorId;
} cm_to_agent_heartbeat;

typedef struct cm_to_cm_vote {
    int msg_type;
    uint32 node;
    uint32 instanceId;
    int role;
} cm_to_cm_vote;

typedef struct cm_to_cm_timeline {
    int msg_type;
    uint32 node;
    uint32 instanceId;
    long timeline;
} cm_to_cm_timeline;

typedef struct cm_to_cm_broadcast {
    int msg_type;
    uint32 node;
    uint32 instanceId;
    int role;
} cm_to_cm_broadcast;

typedef struct cm_to_cm_notify {
    int msg_type;
    int role;
} cm_to_cm_notify;

typedef struct cm_to_cm_switchover {
    int msg_type;
    uint32 node;
    uint32 instanceId;
    int instance_type;
    int wait_seconds;
} cm_to_cm_switchover;

typedef struct cm_to_cm_switchover_ack {
    int msg_type;
    uint32 node;
    uint32 instanceId;
    int instance_type;
    int wait_seconds;
} cm_to_cm_switchover_ack;

typedef struct cm_to_cm_failover {
    int msg_type;
    uint32 node;
    uint32 instanceId;
    int instance_type;
    int wait_seconds;
} cm_to_cm_failover;

typedef struct cm_to_cm_failover_ack {
    int msg_type;
    uint32 node;
    uint32 instanceId;
    int instance_type;
    int wait_seconds;
} cm_to_cm_failover_ack;

typedef struct cm_to_cm_sync {
    int msg_type;
    int role;
} cm_to_cm_sync;

typedef struct cm_instance_command_status {
    int command_status;
    int command_send_status;
    int command_send_times;
    int command_send_num;
    int pengding_command;
    int time_out;
    int role_changed;
    volatile int heat_beat;
    int arbitrate_delay_time_out;
    int arbitrate_delay_set;
    int local_arbitrate_delay_role;
    int peerl_arbitrate_delay_role;
    int full_build;
    int notifyCnCount;
    volatile int keep_heartbeat_timeout;
    int sync_mode;
    int maxSendTimes;
} cm_instance_command_status;

// need to keep consist with cm_to_ctl_instance_datanode_status
typedef struct cm_instance_datanode_report_status {
    cm_local_replconninfo local_status;
    int sender_count;
    BuildState build_info;
    cm_sender_replconninfo sender_status[CM_MAX_SENDER_NUM];
    cm_receiver_replconninfo receive_status;
    RedoStatsData parallel_redo_status;
    cm_redo_stats local_redo_stats;
    synchronous_standby_mode sync_standby_mode;
    int send_gs_guc_time;
    int dn_restart_counts;
    bool arbitrateFlag;
    int failoverStep;
    int failoverTimeout;
    int phony_dead_times;
    int phony_dead_interval;
    int dn_restart_counts_in_hour;
    bool is_finish_redo_cmd_sent;
    uint64 ckpt_redo_point;
    char barrierID[BARRIERLEN];
    char query_barrierId[BARRIERLEN];
    uint64 barrierLSN;
    uint64 archive_LSN;
    uint64 flush_LSN;
    DatanodeSyncList dnSyncList;
    int32 syncDone;
    uint32 arbiTime;
    uint32 sendFailoverTimes;
    bool is_barrier_exist;
} cm_instance_datanode_report_status;

typedef struct cm_instance_gtm_report_status {
    cm_gtm_replconninfo local_status;
    int phony_dead_times;
    int phony_dead_interval;
} cm_instance_gtm_report_status;

/*
 * each coordinator manage a list of datanode notify status
 */
typedef struct cm_notify_msg_status {
    uint32* datanode_instance;
    uint32* datanode_index;
    bool* notify_status;
    bool* have_notified;
    bool* have_dropped;
    bool have_canceled;
    uint32 gtmIdBroadCast;
} cm_notify_msg_status;

#define OBS_BACKUP_INIT         (0)    // not start
#define OBS_BACKUP_PROCESSING   (1)
#define OBS_BACKUP_COMPLETED    (2)
#define OBS_BACKUP_FAILED       (3)
#define OBS_BACKUP_UNKNOWN      (4)    // conn failed, can't get status, will do nothing until it change to other

typedef struct cm_instance_coordinate_report_status {
    cm_coordinate_replconninfo status;
    int isdown;
    int clean;
    uint32 exec_drop_instanceId;
    cm_coordinate_group_mode group_mode;
    cm_notify_msg_status notify_msg;
    char logicClusterName[CM_LOGIC_CLUSTER_NAME_LEN];
    uint32 cn_restart_counts;
    int phony_dead_times;
    int phony_dead_interval;
    bool delay_repair;
    bool isCnDnDisconnected;
    int auto_delete_delay_time;
    int disable_time_out;
    int cma_fault_timeout_to_killcn;
    
    char barrierID [BARRIERLEN];
    char query_barrierId[BARRIERLEN];
    uint64 barrierLSN;
    uint64 archive_LSN;
    uint64 flush_LSN;
    uint64 ckpt_redo_point;
    bool is_barrier_exist;
    int buildReason;
} cm_instance_coordinate_report_status;

typedef struct cm_instance_arbitrate_status {
    int sync_mode;
    bool restarting;
    int promoting_timeout;
} cm_instance_arbitrate_status;

#define MAX_CM_TO_CM_REPORT_SYNC_COUNT_PER_CYCLE 5
typedef struct cm_to_cm_report_sync {
    int msg_type;
    uint32 node[CM_PRIMARY_STANDBY_NUM];
    uint32 instanceId[CM_PRIMARY_STANDBY_NUM];
    int instance_type[CM_PRIMARY_STANDBY_NUM];
    cm_instance_command_status command_member[CM_PRIMARY_STANDBY_NUM];
    cm_instance_datanode_report_status data_node_member[CM_PRIMARY_STANDBY_NUM];
    cm_instance_gtm_report_status gtm_member[CM_PRIMARY_STANDBY_NUM];
    cm_instance_coordinate_report_status coordinatemember;
    cm_instance_arbitrate_status arbitrate_status_member[CM_PRIMARY_STANDBY_NUM];
} cm_to_cm_report_sync;

typedef struct cm_instance_role_status {
    // available zone information
    char azName[CM_AZ_NAME];
    uint32 azPriority;

    uint32 node;
    uint32 instanceId;
    int instanceType;
    int role;
    int dataReplicationMode;
    int instanceRoleInit;
} cm_instance_role_status;

typedef struct cm_instance_role_status_0 {
    uint32 node;
    uint32 instanceId;
    int instanceType;
    int role;
    int dataReplicationMode;
    int instanceRoleInit;
} cm_instance_role_status_0;

#define CM_PRIMARY_STANDBY_MAX_NUM 8    // supprot 1 primary and [1, 7] standby
#define CM_PRIMARY_STANDBY_MAX_NUM_0 3  // support master standby dummy

typedef struct cm_instance_role_group_0 {
    int count;
    cm_instance_role_status_0 instanceMember[CM_PRIMARY_STANDBY_MAX_NUM_0];
} cm_instance_role_group_0;

typedef struct cm_instance_role_group {
    int count;
    cm_instance_role_status instanceMember[CM_PRIMARY_STANDBY_MAX_NUM];
} cm_instance_role_group;

typedef struct cm_to_cm_role_change_notify {
    int msg_type;
    cm_instance_role_group role_change;
} cm_to_cm_role_change_notify;

typedef struct ctl_to_cm_datanode_relation_info {
    int msg_type;
    uint32 node;
    uint32 instanceId;
    int instance_type;
    int wait_seconds;
} ctl_to_cm_datanode_relation_info;

typedef struct cm_to_ctl_get_datanode_relation_ack {
    int command_result;
    int member_index;
    cm_instance_role_status instanceMember[CM_PRIMARY_STANDBY_MAX_NUM];
    cm_instance_gtm_report_status gtm_member[CM_PRIMARY_STANDBY_NUM];
    cm_instance_datanode_report_status data_node_member[CM_PRIMARY_STANDBY_MAX_NUM];
} cm_to_ctl_get_datanode_relation_ack;

// need to keep consist with the struct cm_instance_datanode_report_status
typedef struct cm_to_ctl_instance_datanode_status {
    cm_local_replconninfo local_status;
    int sender_count;
    BuildState build_info;
    cm_sender_replconninfo sender_status[CM_MAX_SENDER_NUM];
    cm_receiver_replconninfo receive_status;
    RedoStatsData parallel_redo_status;
    cm_redo_stats local_redo_stats;
    synchronous_standby_mode sync_standby_mode;
    int send_gs_guc_time;
} cm_to_ctl_instance_datanode_status;

typedef struct cm_to_ctl_instance_gtm_status {
    cm_gtm_replconninfo local_status;
} cm_to_ctl_instance_gtm_status;

typedef struct cm_to_ctl_instance_coordinate_status {
    int status;
    cm_coordinate_group_mode group_mode;
    /* no notify map in ctl */
} cm_to_ctl_instance_coordinate_status;

typedef struct cm_to_ctl_instance_status {
    int msg_type;
    uint32 node;
    uint32 instanceId;
    int instance_type;
    int member_index;
    int is_central;
    int fenced_UDF_status;
    cm_to_ctl_instance_datanode_status data_node_member;
    cm_to_ctl_instance_gtm_status gtm_member;
    cm_to_ctl_instance_coordinate_status coordinatemember;
} cm_to_ctl_instance_status;

typedef struct cm_to_ctl_instance_barrier_info {
    int msg_type;
    uint32 node;
    uint32 instanceId;
    int instance_type;
    int member_index;
    uint64 ckpt_redo_point;
    char barrierID [BARRIERLEN];
    uint64 barrierLSN;
    uint64 archive_LSN;
    uint64 flush_LSN;
} cm_to_ctl_instance_barrier_info;

typedef struct cm_to_ctl_central_node_status {
    uint32 instanceId;
    int node_index;
    int status;
} cm_to_ctl_central_node_status;

#define CM_CAN_PRCESS_COMMAND 0
#define CM_ANOTHER_COMMAND_RUNNING 1
#define CM_INVALID_COMMAND 2
#define CM_DN_NORMAL_STATE 3

typedef struct cm_to_ctl_command_ack {
    int msg_type;
    int command_result;
    uint32 node;
    uint32 instanceId;
    int instance_type;
    int command_status;
    int pengding_command;
    int time_out;
} cm_to_ctl_command_ack;

typedef struct cm_to_ctl_balance_check_ack {
    int msg_type;
    int switchoverDone;
} cm_to_ctl_balance_check_ack;

typedef struct cm_to_ctl_switchover_full_check_ack {
    int msg_type;
    int switchoverDone;
} cm_to_ctl_switchover_full_check_ack, cm_to_ctl_switchover_az_check_ack;

#define MAX_INSTANCES_LEN 512
typedef struct cm_to_ctl_balance_result {
    int msg_type;
    int imbalanceCount;
    uint32 instances[MAX_INSTANCES_LEN];
} cm_to_ctl_balance_result;

#define CM_STATUS_STARTING 0
#define CM_STATUS_PENDING 1
#define CM_STATUS_NORMAL 2
#define CM_STATUS_NEED_REPAIR 3
#define CM_STATUS_DEGRADE 4
#define CM_STATUS_UNKNOWN 5
#define CM_STATUS_NORMAL_WITH_CN_DELETED 6
#define CM_STATUS_UNKNOWN_WITH_BINARY_DAMAGED (7)

typedef struct cm_to_ctl_cluster_status {
    int msg_type;
    int cluster_status;
    bool is_all_group_mode_pending;
    int switchedCount;
    int node_id;
    bool inReloading;
} cm_to_ctl_cluster_status;

typedef struct cm_to_ctl_cluster_global_barrier_info {
    int msg_type;
    char global_barrierId[BARRIERLEN];
    char global_achive_barrierId[BARRIERLEN];
    char globalRecoveryBarrierId[BARRIERLEN];
} cm_to_ctl_cluster_global_barrier_info;

typedef struct cm2CtlGlobalBarrierNew_t {
    int msg_type;
    char globalRecoveryBarrierId[BARRIERLEN];
    GlobalBarrierStatus globalStatus;
} cm2CtlGlobalBarrierNew;

typedef struct cm_to_ctl_logic_cluster_status {
    int msg_type;
    int cluster_status;
    bool is_all_group_mode_pending;
    int switchedCount;
    bool inReloading;

    int logic_cluster_status[LOGIC_CLUSTER_NUMBER];
    bool logic_is_all_group_mode_pending[LOGIC_CLUSTER_NUMBER];
    int logic_switchedCount[LOGIC_CLUSTER_NUMBER];
} cm_to_ctl_logic_cluster_status;

typedef struct cm_to_ctl_cmserver_status {
    int msg_type;
    int local_role;
    bool is_pending;
} cm_to_ctl_cmserver_status;

typedef struct cm_query_instance_status {
    int msg_type;
    uint32 nodeId;
    uint32 instanceType;  // only for etcd and cmserver
    uint32 msg_step;
    uint32 status;
    bool pending;
} cm_query_instance_status;

typedef struct etcd_status_info {
    pthread_rwlock_t lk_lock;
    cm_query_instance_status report_msg;
} etcd_status_info;

/* kerberos information */
#define ENV_MAX 100
#define ENVLUE_NUM 3
#define MAX_BUFF 1024
#define MAXLEN 20
#define KERBEROS_NUM 2

typedef struct agent_to_cm_kerberos_status_report {
    int msg_type;
    uint32 node;
    char kerberos_ip[CM_IP_LENGTH];
    uint32 port;
    uint32 status;
    char role[MAXLEN];
    char nodeName[CM_NODE_NAME];
} agent_to_cm_kerberos_status_report;

typedef struct kerberos_status_info {
    pthread_rwlock_t lk_lock;
    agent_to_cm_kerberos_status_report report_msg;
} kerberos_status_info;

typedef struct cm_to_ctl_kerberos_status_query {
    int msg_type;
    uint32 heartbeat[KERBEROS_NUM];
    uint32 node[KERBEROS_NUM];
    char kerberos_ip[KERBEROS_NUM][CM_IP_LENGTH];
    uint32 port[KERBEROS_NUM]; 
    uint32 status[KERBEROS_NUM];
    char role[KERBEROS_NUM][MAXLEN];
    char nodeName[KERBEROS_NUM][CM_NODE_NAME];
} cm_to_ctl_kerberos_status_query;

typedef struct kerberos_group_report_status {
    pthread_rwlock_t lk_lock;
    cm_to_ctl_kerberos_status_query kerberos_status;
} kerberos_group_report_status;


typedef uint32 ShortTransactionId;

/* ----------------
 *        Special transaction ID values
 *
 * BootstrapTransactionId is the XID for "bootstrap" operations, and
 * FrozenTransactionId is used for very old tuples.  Both should
 * always be considered valid.
 *
 * FirstNormalTransactionId is the first "normal" transaction id.
 * Note: if you need to change it, you must change pg_class.h as well.
 * ----------------
 */
#define InvalidTransactionId ((TransactionId)0)
#define BootstrapTransactionId ((TransactionId)1)
#define FrozenTransactionId ((TransactionId)2)
#define FirstNormalTransactionId ((TransactionId)3)
#define MaxTransactionId ((TransactionId)0xFFFFFFFF)

/* ----------------
 *        transaction ID manipulation macros
 * ----------------
 */
#define TransactionIdIsValid(xid) ((xid) != InvalidTransactionId)
#define TransactionIdIsNormal(xid) ((xid) >= FirstNormalTransactionId)
#define TransactionIdEquals(id1, id2) ((id1) == (id2))
#define TransactionIdStore(xid, dest) (*(dest) = (xid))
#define StoreInvalidTransactionId(dest) (*(dest) = InvalidTransactionId)

/*
 * Macros for comparing XLogRecPtrs
 *
 * Beware of passing expressions with side-effects to these macros,
 * since the arguments may be evaluated multiple times.
 */
#define XLByteLT(a, b) ((a) < (b))
#define XLByteLE(a, b) ((a) <= (b))
#define XLByteEQ(a, b) ((a) == (b))

#define InvalidTerm (0)
#define FirstTerm (1)
#define TermIsInvalid(term) ((term) == InvalidTerm)

#define XLByteLT_W_TERM(a_term, a_logptr, b_term, b_logptr) \
    (((a_term) < (b_term)) || (((a_term) == (b_term)) && ((a_logptr) < (b_logptr))))
#define XLByteLE_W_TERM(a_term, a_logptr, b_term, b_logptr) \
    (((a_term) < (b_term)) || (((a_term) == (b_term)) && ((a_logptr) <= (b_logptr))))
#define XLByteEQ_W_TERM(a_term, a_logptr, b_term, b_logptr) (((a_term) == (b_term)) && ((a_logptr) == (b_logptr)))
#define XLByteWE_W_TERM(a_term, a_logptr, b_term, b_logptr) \
    (((a_term) > (b_term)) || (((a_term) == (b_term)) && ((a_logptr) > (b_logptr))))

#define CM_RESULT_COMM_ERROR (-2) /* Communication error */
#define CM_RESULT_ERROR (-1)
#define CM_RESULT_OK (0)
/*
 * This error is used ion the case where allocated buffer is not large
 * enough to store the errors. It may happen of an allocation failed
 * so it's status is considered as unknown.
 */
#define CM_RESULT_UNKNOWN (1)

typedef struct ResultDataPacked {
    char pad[CM_MSG_MAX_LENGTH];
} ResultDataPacked;

typedef union CM_ResultData {
    ResultDataPacked packed;
} CM_ResultData;

typedef struct CM_Result {
    int gr_msglen;
    int gr_status;
    int gr_type;
    CM_ResultData gr_resdata;
} CM_Result;

extern int query_gtm_status_wrapper(const char pid_path[MAXPGPATH], agent_to_cm_gtm_status_report& agent_to_cm_gtm);
extern int query_gtm_status_for_phony_dead(const char pid_path[MAXPGPATH]);

typedef struct CtlToCMReload {
    int msgType;
} CtlToCMReload;
typedef struct CMToCtlReloadAck {
    int msgType;
    bool reloadOk;
} CMToCtlReloadAck;

#endif
