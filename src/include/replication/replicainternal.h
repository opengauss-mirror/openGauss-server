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
#define PG_PROTOCOL_VERSION "MPPDB"

/* Notice: the value is same sa GUC_MAX_REPLNODE_NUM */
#ifdef ENABLE_MULTIPLE_NODES
#define MAX_REPLNODE_NUM 8
#else
#define MAX_REPLNODE_NUM 9
#endif

#define REPL_IDX_PRIMARY 1
#define REPL_IDX_STANDBY 2

typedef enum {
    NoDemote = 0,
    SmartDemote,
    FastDemote
} DemoteMode;

typedef enum {
    UNUSED_LISTEN_SOCKET = 0,
    PSQL_LISTEN_SOCKET,
    HA_LISTEN_SOCKET
} ListenSocketType;

typedef enum {
    UNKNOWN_MODE = 0,
    NORMAL_MODE,
    PRIMARY_MODE,
    STANDBY_MODE,
    CASCADE_STANDBY_MODE,
    PENDING_MODE,
    RECOVERY_MODE
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
    MODE_REBUILD
} HaRebuildReason;

typedef enum { NONE_BUILD = 0, AUTO_BUILD, FULL_BUILD, INC_BUILD } BuildMode;

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
} GaussState;

#ifdef EXEC_BACKEND
/*
 * Indicate one connect channel
 */
typedef struct replconninfo {
    char localhost[IP_LEN];
    int localport;
    int localservice;
    int localheartbeatport;
    char remotehost[IP_LEN];
    int remoteport;
    int remoteservice;
    int remoteheartbeatport;
    bool isCascade;
} ReplConnInfo;

/*
 * HA share memory struct
 */
typedef struct hashmemdata {
    ServerMode current_mode;
    bool is_cascade_standby;
    HaRebuildReason repl_reason[MAX_REPLNODE_NUM];
    int disconnect_count[MAX_REPLNODE_NUM];
    int current_repl;
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
    NODESTATE_STANDBY_WAITING,
    NODESTATE_PRIMARY_DEMOTING,
    NODESTATE_PROMOTE_APPROVE,
    NODESTATE_STANDBY_REDIRECT,
    NODESTATE_STANDBY_PROMOTING,
    NODESTATE_STANDBY_FAILOVER_PROMOTING,
    NODESTATE_PRIMARY_DEMOTING_WAIT_CATCHUP,
    NODESTATE_DEMOTE_FAILED
} ClusterNodeState;
#endif

/*
 * Rec crc for wal's handshake
 */
typedef enum {
    NONE_REC_CRC = 0,
    IGNORE_REC_CRC
} PredefinedRecCrc;

extern bool data_catchup;
extern bool wal_catchup;

#endif /* _REPLICA_INTERNAL_H */
