/* ---------------------------------------------------------------------------------------
 * 
 * replicainternal.h
 *		MPPDB High Available internal declarations
 *
 * Portions Copyright (c) 2010-2012, Postgres-XC Development Group
 * Portions Copyright (c) 1996-2011, PostgreSQL Global Development Group
 * 
 * 
 * IDENTIFICATION
 *        src/include/replication/replicainternal.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef _REPLICA_INTERNAL_H
#define _REPLICA_INTERNAL_H

#ifdef EXEC_BACKEND
#include "storage/spin.h"
#endif

#define IP_LEN 64
#define SSL_MODE_LEN 16
#define PG_PROTOCOL_VERSION "MPPDB"

/* Notice: the value is same sa GUC_MAX_REPLNODE_NUM */
#define MAX_REPLNODE_NUM 9

#define DOUBLE_MAX_REPLNODE_NUM (MAX_REPLNODE_NUM * 2)

#define REPL_IDX_PRIMARY 1
#define REPL_IDX_STANDBY 2

typedef enum {
    NoDemote = 0,
    SmartDemote,
    FastDemote,
    ExtremelyFast
} DemoteMode;

typedef enum {
    UNUSED_LISTEN_SOCKET = 0,
    PSQL_LISTEN_SOCKET,
    HA_LISTEN_SOCKET
} ListenSocketType;

typedef enum {
    POST_PORT_SOCKET = 0,
    POOLER_PORT_SOCKET,
    BOTH_PORT_SOCKETS
} RecreateListenSocketType;

typedef enum {
    UNUSED_LISTEN_CHANEL = 0,
    NORMAL_LISTEN_CHANEL,  /* type of listen_addresses */
    REPL_LISTEN_CHANEL,  /* type of ReplConnArray or CrossClusterReplConnArray */
    EXT_LISTEN_CHANEL,
    DOLPHIN_LISTEN_CHANEL,
    UNKNOWN_LISTEN_CHANEL
} ListenChanelType;

typedef enum {
    UNKNOWN_MODE = 0,
    NORMAL_MODE,
    PRIMARY_MODE,
    STANDBY_MODE,
    CASCADE_STANDBY_MODE,
    PENDING_MODE,
    RECOVERY_MODE,
    STANDBY_CLUSTER_MODE,
    MAIN_STANDBY_MODE
} ServerMode;

typedef enum {
    UNKNOWN_STATE = 0,
    NORMAL_STATE,
    NEEDREPAIR_STATE,
    STARTING_STATE,
    WAITING_STATE,
    DEMOTING_STATE,
    PROMOTING_STATE,
    BUILDING_STATE,
    CATCHUP_STATE,
    COREDUMP_STATE
} DbState;

typedef enum {
    NONE_REBUILD = 0,
    WALSEGMENT_REBUILD,
    CONNECT_REBUILD,
    TIMELINE_REBUILD,
    SYSTEMID_REBUILD,
    VERSION_REBUILD,
    MODE_REBUILD,
    DCF_LOG_LOSS_REBUILD
} HaRebuildReason;

typedef enum {
    NONE_BUILD = 0,
    AUTO_BUILD,
    FULL_BUILD,
    INC_BUILD,
    STANDBY_FULL_BUILD,
    COPY_SECURE_FILES_BUILD,
    CROSS_CLUSTER_FULL_BUILD,
    CROSS_CLUSTER_INC_BUILD,
    CROSS_CLUSTER_STANDBY_FULL_BUILD,
    BUILD_CHECK
} BuildMode;

typedef struct buildstate {
    BuildMode build_mode;
    uint64 total_done;
    uint64 total_size;
    int process_schedule;
    int estimated_time;
} BuildState;

typedef struct gaussstate {
    ServerMode mode;
    int conn_num;
    DbState state;
    bool sync_stat;
    uint64 lsn;
    uint64 term;
    BuildState build_info;
    HaRebuildReason ha_rebuild_reason;
    int current_connect_idx;
} GaussState;

typedef struct newnodeinfo {
    unsigned int stream_id;
    unsigned int node_id;
    char ip[IP_LEN];
    unsigned int port;
    unsigned int role;
    unsigned int wait_timeout_ms;
} NewNodeInfo;

typedef struct runmodeparam {
    uint32 voteNum;
    uint32 xMode;
} RunModeParam;

#ifdef EXEC_BACKEND
/*
 * Indicate one connect channel
 */
typedef struct replconninfo {
    char localhost[IP_LEN];
    int localport;
    int localheartbeatport;
    int remotenodeid;
    char remotehost[IP_LEN];
    int remoteport;
    int remoteheartbeatport;
    char remoteuwalhost[IP_LEN];
    int remoteuwalport;
    bool isCascade;
    bool isCrossRegion;
#ifdef ENABLE_LITE_MODE
    char sslmode[SSL_MODE_LEN];
#endif
} ReplConnInfo;

/*
 * HA share memory struct
 */
typedef struct hashmemdata {
    ServerMode current_mode;
    bool is_cascade_standby;
    HaRebuildReason repl_reason[DOUBLE_MAX_REPLNODE_NUM];
    int disconnect_count[DOUBLE_MAX_REPLNODE_NUM];
    bool is_cross_region;
    bool is_hadr_main_standby;
    int current_repl;
    int prev_repl;
    int repl_list_num;
    int loop_find_times;
    slock_t mutex;
} HaShmemData;

/*
 * state of the node in the high availability cluster
 * NOTES: NODESTATE_XXX_DEMOTE_REQUEST should equal to XXXDemote
 */
typedef enum ClusterNodeState {
    NODESTATE_NORMAL = 0,
    NODESTATE_SMART_DEMOTE_REQUEST,
    NODESTATE_FAST_DEMOTE_REQUEST,
    NODESTATE_EXTRM_FAST_DEMOTE_REQUEST,
    NODESTATE_STANDBY_WAITING,
    NODESTATE_PRIMARY_DEMOTING,
    NODESTATE_PROMOTE_APPROVE,
    NODESTATE_STANDBY_REDIRECT,
    NODESTATE_STANDBY_PROMOTING,
    NODESTATE_STANDBY_FAILOVER_PROMOTING,
    NODESTATE_PRIMARY_DEMOTING_WAIT_CATCHUP,
    NODESTATE_DEMOTE_FAILED,
    NODESTATE_STANDBY_PROMOTED
} ClusterNodeState;
#endif

/*
 * Rec crc for wal's handshake
 */
typedef enum {
    NONE_REC_CRC = 0,
    IGNORE_REC_CRC
} PredefinedRecCrc;

/*
 * replication auth mode
 */
typedef enum replauthmode{
    REPL_AUTH_DEFAULT = 0, /* no extra replication auth */
    REPL_AUTH_UUID /* uuid auth */
} ReplAuthMode;

typedef enum
{
    RESOLVE_ERROR,
    RESOLVE_APPLY_REMOTE,
    RESOLVE_KEEP_LOCAL
} PGLogicalResolveOption;

extern bool data_catchup;
extern bool wal_catchup;
extern BuildMode build_mode;
extern bool is_cross_region_build; /* for stream disaster recovery cluster */
#define IS_CROSS_CLUSTER_BUILD (build_mode == CROSS_CLUSTER_FULL_BUILD || \
                                build_mode == CROSS_CLUSTER_INC_BUILD || \
                                build_mode == CROSS_CLUSTER_STANDBY_FULL_BUILD || \
                                is_cross_region_build)
#endif /* _REPLICA_INTERNAL_H */
