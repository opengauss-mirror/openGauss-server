/* -------------------------------------------------------------------------
 *
 * walsender_private.h
 *	  Private definitions from replication/walsender.c.
 *
 * Portions Copyright (c) 2010-2012, PostgreSQL Global Development Group
 *
 * src/include/replication/walsender_private.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef _WALSENDER_PRIVATE_H
#define _WALSENDER_PRIVATE_H

#include "access/xlog.h"
#include "nodes/nodes.h"
#include "replication/replicainternal.h"
#include "replication/syncrep.h"
#include "replication/repl_gramparse.h"
#include "storage/latch.h"
#include "storage/shmem.h"
#include "storage/spin.h"
#include "postgres.h"
#include "knl/knl_variable.h"

typedef enum WalSndState {
    WALSNDSTATE_STARTUP = 0,
    WALSNDSTATE_BACKUP,
    WALSNDSTATE_CATCHUP,
    WALSNDSTATE_STREAMING
} WalSndState;

typedef enum SndRole {
    SNDROLE_PRIMARY_STANDBY = 1,      /* primary to standby */
    SNDROLE_PRIMARY_BUILDSTANDBY = 2, /* primary to gs_ctl when run build command */
    SNDROLE_PRIMARY_DUMMYSTANDBY = 4, /* primary to dummy standby */
    SNDROLE_DUMMYSTANDBY_STANDBY = 8  /* dummy standby to standby */
} SndRole;

typedef struct WSXLogPageReadPrivate {
    TimeLineID tli;
    XLogSegNo xlogreadlogsegno;
    int xlogreadfd;
} WSXLogPageReadPrivate;

/* For log ctrl. Willing let standby flush and apply log under RTO seconds */
typedef struct LogCtrlData {
    int64 sleep_time;
    int64 balance_sleep_time;
    int64 prev_RTO;
    int64 current_RTO;
    uint64 sleep_count;
    int64 sleep_count_limit;
    XLogRecPtr prev_flush;
    XLogRecPtr prev_apply;
    TimestampTz prev_reply_time;
    uint64 pre_rate1;
    uint64 pre_rate2;
    int64 prev_RPO;
    int64 current_RPO;
    bool just_keep_alive;
} LogCtrlData;

/*
 * Each walsender has a WalSnd struct in shared memory.
 */
typedef struct WalSnd {
    ThreadId pid; /* this walsender's process id, or 0 */
    int lwpId;
    WalSndState state;           /* this walsender's state */
    TimestampTz catchupTime[2];  /* time stamp of this walsender's catchup */
    ClusterNodeState node_state; /* state of the node in the cluster */
    SndRole sendRole;            /* role of sender */
    XLogRecPtr sentPtr;          /* WAL has been sent up to this point */
    bool needreload;             /* does currently-open file need to be
                                  * reloaded? */
    bool sendKeepalive;          /* do we send keepalives on this connection? */
    bool replSender;             /* is the walsender a normal replication or building */

    ServerMode peer_role;
    DbState peer_state;
    /*
     * The xlog locations that have been received, written, flushed, and applied by
     * standby-side. These may be invalid if the standby-side has not offered
     * values yet.
     */
    XLogRecPtr receive;
    XLogRecPtr write;
    XLogRecPtr flush;
    XLogRecPtr apply;

    /* if valid means all the required replication data already flushed on the standby */
    XLogRecPtr data_flush;
    /*
     * The xlog locations is used for counting sync percentage in function GetSyncPercent.
     */
    XLogRecPtr syncPercentCountStart;

    ReplConnInfo wal_sender_channel;
    int channel_get_replc;

    /* Protects shared variables shown above. */
    slock_t mutex;

    /*
     * Latch used by backends to wake up this walsender when it has work to
     * do.
     */
    Latch latch;

    /*
     * The priority order of the standby managed by this WALSender, as listed
     * in synchronous_standby_names, or 0 if not-listed. Protected by
     * SyncRepLock.
     */
    int sync_standby_priority;
    int index;
    XLogRecPtr arch_task_lsn;
    int archive_obs_subterm;
    LogCtrlData log_ctrl;
    unsigned int archive_flag;
    Latch* arch_latch;
    bool is_start_archive;
    unsigned int standby_archive_flag;
    XLogRecPtr archive_target_lsn;
    XLogRecPtr arch_task_last_lsn;
    bool arch_finish_result;

    /* 
     * has_sent_arch_lsn indicates whether the walsnd has sent the archive location,
     * and last_send_time is used to record the time when the archive location is sent last time.
     */
    bool has_sent_arch_lsn;
    long last_send_lsn_time;

    /* 
     * lastCalTime is last time calculating catchupRate, and lastCalWrite
     * is last calculating write lsn.
     */
    TimestampTz lastCalTime;
    XLogRecPtr lastCalWrite;
    /*
     * Time needed for synchronous per xlog while catching up.
     */
    double catchupRate;
} WalSnd;

extern THR_LOCAL WalSnd* MyWalSnd;

/* There is one WalSndCtl struct for the whole database cluster */
typedef struct WalSndCtlData {
    /*
     * Synchronous replication queue with one queue per request type.
     * Protected by SyncRepLock.
     */
    SHM_QUEUE SyncRepQueue[NUM_SYNC_REP_WAIT_MODE];

    /*
     * Current location of the head of the queue. All waiters should have a
     * waitLSN that follows this value. Protected by SyncRepLock.
     */
    XLogRecPtr lsn[NUM_SYNC_REP_WAIT_MODE];

    /*
     * Are any sync standbys defined?  Waiting backends can't reload the
     * config file safely, so checkpointer updates this value as needed.
     * Protected by SyncRepLock.
     */
    bool sync_standbys_defined;

    /*
     * 1. Whether master is allowed to switched to standalone if no synchronous
     * Standbys are available; 2. master need not to keep waiting failed synchronous
     * Standbys if most_available_sync is ON.This is copy of GUC variable most_available_sync.
     */
    bool most_available_sync;

    /*
     * Indicates the current running mode of master node. If it is true means
     * there is no synchronous standby available so it is running in standalone
     * mode.
     */
    bool sync_master_standalone;

    /*
     * The demotion of postmaster  Also indicates that all the walsenders
     * should reject any demote requests if postmaster is doning domotion.
     */
    DemoteMode demotion;

    /* Protects shared variables of all walsnds. */
    slock_t mutex;

    WalSnd walsnds[FLEXIBLE_ARRAY_MEMBER]; /* VARIABLE LENGTH ARRAY */
} WalSndCtlData;

extern THR_LOCAL WalSndCtlData* WalSndCtl;
extern volatile bool bSyncStat;
extern volatile bool bSyncStatStatBefore;
extern void WalSndSetState(WalSndState state);

#endif /* _WALSENDER_PRIVATE_H */
