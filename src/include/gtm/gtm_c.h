/* -------------------------------------------------------------------------
 *
 * gtm_c.h
 *	  Fundamental C definitions.  This is included by every .c file in
 *	  PostgreSQL (via either postgres.h or postgres_fe.h, as appropriate).
 *
 *	  Note that the definitions here are not intended to be exposed to clients
 *	  of the frontend interface libraries --- so we don't worry much about
 *	  polluting the namespace with lots of stuff...
 *
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * $PostgreSQL: pgsql/src/include/c.h,v 1.234 2009/01/01 17:23:55 momjian Exp $
 *
 * -------------------------------------------------------------------------
 */
#ifndef GTM_C_H
#define GTM_C_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stddef.h>
#include <stdarg.h>
#ifdef HAVE_STRINGS_H
#include <strings.h>
#endif
#include <sys/types.h>

#include <errno.h>
#include <pthread.h>
#include "c.h"
#include "cm/etcdapi.h"

#define DR_MAX_NODE_NUM (1024 * 3)
/* disaster cluster info form: "num_cn num_slice num_one_slice (slice_num host host1 port slice_name)..." */
#define MAX_DISASTER_INFO_LEN (DR_MAX_NODE_NUM * 90 + 15)

typedef uint64 GlobalTransactionId; /* 64-bit global transaction ids */
typedef int16 GTMProxy_ConnID;
typedef uint32 GTM_StrLen;

#define InvalidGTMProxyConnID -1

typedef pthread_t GTM_ThreadID;

typedef uint32 GTM_PGXCNodeId;
typedef uint32 GTM_PGXCNodePort;

/* Possible type of nodes for registration */
typedef enum GTM_PGXCNodeType {
    GTM_NODE_ALL = 0, /* For pgxcnode_find_by_type, pass this value means to get all type node */
    GTM_NODE_GTM_PROXY = 1,
    GTM_NODE_GTM_PROXY_POSTMASTER = 2,
    /* Used by Proxy to communicate with GTM and not use Proxy headers */
    GTM_NODE_COORDINATOR = 3,
    GTM_NODE_DATANODE = 4,
    GTM_NODE_GTM_PRIMARY = 5,
    GTM_NODE_GTM_STANDBY = 6,
    GTM_NODE_GTM_CLIENT = 7,
    GTM_NODE_DEFAULT = 8 /* In case nothing is associated to connection */
} GTM_PGXCNodeType;

typedef enum GTM_SnapshotType {
    GTM_SNAPSHOT_TYPE_UNDEFINED,
    GTM_SNAPSHOT_TYPE_LOCAL,
    GTM_SNAPSHOT_TYPE_GLOBAL,
    GTM_SNAPSHOT_TYPE_AUTOVACUUM
} GTM_SnapshotType;

/*
 * A unique handle to identify transaction at the GTM. It could just be
 * an index in an array or a pointer to the structure
 *
 * Note: If we get rid of BEGIN transaction at the GTM, we can use GXID
 * as a handle because we would never have a transaction state at the
 * GTM without assigned GXID.
 */
typedef int32 GTM_TransactionHandle;
typedef uint32 GTM_Timeline;

/* GTM_TransactionKey used to identify a transaction
 * it is composed by handle and timeline
 */
typedef struct GTM_TransactionKey {
    GTM_TransactionHandle txnHandle; /* txn handle in GTM */
    GTM_Timeline txnTimeline;        /* txn timeline in GTM */
} GTM_TransactionKey;

typedef struct GTM_TransactionKey GTM_TransactionKey;

extern GTM_Timeline gtmTimeline;
extern uint32 gtmTransactionsLenWhenExit;
extern bool write_fake_TransLen;

extern uint32 snapshot_num;
extern uint32 snapshot_num_now;
extern uint64 snapshot_totalsize;
extern uint64 snapshot_totalsize_now;
extern uint32 MaxNumThreadActive;

#define InvalidTransactionHandle -1
#define InvalidTransactionTimeline 0

#define GlobalTransactionHandleIsValid(handle) (((GTM_TransactionHandle)(handle)) != InvalidTransactionHandle)
#define GlobalTransactionTimelineIsValid(timeline) (((GTM_Timeline)(timeline)) != InvalidTransactionTimeline)

/*
 * As GTM and openGauss packages are separated, GTM and XC's API
 * use different type names for timestamps and sequences, but they have to be the same!
 */
typedef int64 GTM_Timestamp; /* timestamp data is 64-bit based */

typedef int64 GTM_Sequence; /* a 64-bit sequence */

typedef int64 GTM_UUID; /* a 64-bit UUID */

extern EtcdSession g_etcdSession;

/* Type of sequence name used when dropping it */
typedef enum GTM_SequenceKeyType {
    GTM_SEQ_FULL_NAME,     /* Full sequence key */
    GTM_SEQ_DB_NAME,       /* DB name part of sequence key */
    GTM_SEQ_DB_SCHEMA_NAME /*  Part of sequence key: dbname.schemaname */
} GTM_SequenceKeyType;

typedef struct GTM_SequenceKeyData {
    uint32 gsk_keylen;
    char* gsk_key;
    GTM_SequenceKeyType gsk_type; /* see constants below */
} GTM_SequenceKeyData;            /* Counter key, set by the client */

typedef GTM_SequenceKeyData* GTM_SequenceKey;

typedef struct GTM_DBNameData {
    uint32 gsd_dblen;
    char* gsd_db;
} GTM_DBNameData; /* database that sequence belongs to */

typedef GTM_DBNameData* GTM_DBName;

#define GTM_MAX_SEQKEY_LENGTH 1024

#define InvalidSequenceValue 0x7fffffffffffffffLL
#define SEQVAL_IS_VALID(v) ((v) != InvalidSequenceValue)

#define MaxSequenceValue 0x7fffffffffffffffLL
#define InvalidUUID ((GTM_UUID)0)
#define InitialUUIDValue_Default ((GTM_UUID)1000000)
#define MaxSequenceUUID 0x7fffffffffffffffLL

#define GTM_MAX_GLOBAL_TRANSACTIONS 16384

#define GTM_MAX_ERROR_LENGTH 1024

#define GTM_HOST_FLAG_BASE 100
#define HOST2FLAG(h) ((uint32)((h) + GTM_HOST_FLAG_BASE))
#define FLAG2HOST(f) ((GtmHostIndex)((f) - GTM_HOST_FLAG_BASE))

#ifndef HAVE_INT64_TIMESTAMP
#define GTM_TIMESTAMTP_TO_MILLISEC(ts) ((long)(ts) * 1000)
#else
#define GTM_TIMESTAMTP_TO_MILLISEC(ts) ((long)(ts) / 1000)
#endif

#ifndef FREE_AND_RESET
#define FREE_AND_RESET(ptr) do { \
    if (NULL != (ptr)) { \
        pfree(ptr);      \
        (ptr) = NULL;    \
    }                    \
} while (0)
#endif

typedef enum GTM_IsolationLevel {
    GTM_ISOLATION_SERIALIZABLE, /* serializable txn */
    GTM_ISOLATION_RC            /* read-committed txn */
} GTM_IsolationLevel;

typedef struct GTM_SnapshotData {
    GlobalTransactionId sn_xmin;
    GlobalTransactionId sn_xmax;
    GlobalTransactionId sn_recent_global_xmin;
    uint64 csn;
} GTM_SnapshotData;

typedef GTM_SnapshotData* GTM_Snapshot;

typedef struct GTM_SnapshotStatusData {
    GlobalTransactionId xmin;
    GlobalTransactionId xmax;
    uint64 csn;
    GlobalTransactionId recent_global_xmin;
    GlobalTransactionId next_xid;
    GTM_Timeline timeline;
    uint32 active_thread_num;
    uint32 max_thread_num;
    uint32 snapshot_num;
    Size snapshot_totalsize;
} GTM_SnapshotStatusData;

typedef GTM_SnapshotStatusData* GTM_SnapshotStatus;

/* A struct providing some info of gtm for views */
typedef struct GTMLite_StatusData {
    GlobalTransactionId backup_xid;
    uint64 csn;
} GTMLite_StatusData;

typedef GTMLite_StatusData* GTMLite_Status;

/* Define max size of node name in start up packet */
#define SP_NODE_NAME 64

typedef struct GTM_StartupPacket {
    char sp_node_name[SP_NODE_NAME];
    GTM_PGXCNodeType sp_remotetype;
    bool sp_ispostmaster;
    pthread_t sp_remote_thdpid;
} GTM_StartupPacket;

typedef enum GTM_PortLastCall {
    GTM_LastCall_NONE = 0,
    GTM_LastCall_SEND,
    GTM_LastCall_RECV,
    GTM_LastCall_READ,
    GTM_LastCall_WRITE
} GTM_PortLastCall;

#define InvalidGlobalTransactionId ((GlobalTransactionId)0)

typedef enum GTMServerMode {
    GTMServer_Unknown = 0,
    GTMServer_PrimaryMode,
    GTMServer_StandbyMode,
    GTMServer_PendingMode
} GTMServerMode;

typedef enum GTMConnectionStatus {
    GTMConnection_Unknown = 0,
    GTMConnection_bad,
    GTMConnection_ok
} GTMConnectionStatus;

typedef enum GTMSyncStatus {
    GTMSync_Unknown = 0,
    GTMSync_PrimaryMode,
    GTMSync_StandbyMode
} GTMSyncStatus;

typedef struct tagGtmHostIP {
    char acHostIP[128];
} GTM_HOST_IP;

/* ----------
 * GTM connection states
 *
 * Traditionally, gtm_host_0 stands for primary while
 * gtm_host_1 stands for (promoted) standby.
 * ----------
 */
typedef enum GtmHostIndex {
    GTM_HOST_INVAILD = 0,
    GTM_HOST_0,
    GTM_HOST_1,
    GTM_HOST_2,
    GTM_HOST_3,
    GTM_HOST_4,
    GTM_HOST_5,
    GTM_HOST_6,
    GTM_HOST_7,
} GtmHostIndex;

extern int gtm_max_trans;

extern int gtm_num_threads;

/*
 * Initial GXID value to start with, when -x option is not specified at the first run.
 *
 * This value is supposed to be safe enough.   If initdb involves huge amount of initial
 * statements/transactions, users should consider to tweak this value with explicit
 * -x option.
 */
#define InitialGXIDValue_Default ((GlobalTransactionId)10000)

#define GlobalTransactionIdIsValid(gxid) (((GlobalTransactionId)(gxid)) != InvalidGlobalTransactionId)

#ifdef ENABLE_UT
extern void GTM_CopySnapshot(GTM_Snapshot dst, GTM_Snapshot src);
#endif

extern const char *transfer_snapshot_type(GTM_SnapshotType gtm_snap_type);
#endif /* GTM_C_H */
