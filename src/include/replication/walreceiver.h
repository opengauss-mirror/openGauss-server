/* -------------------------------------------------------------------------
 *
 * walreceiver.h
 *	  Exports from replication/walreceiverfuncs.c.
 *
 * Portions Copyright (c) 2010-2012, PostgreSQL Global Development Group
 *
 * src/include/replication/walreceiver.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef _WALRECEIVER_H
#define _WALRECEIVER_H

#include "access/xlog.h"
#include "access/xlogdefs.h"
#include "lib/stringinfo.h"
#include "fmgr.h"
#include "pgtime.h"
#include "postgres.h"
#include "knl/knl_variable.h"
#include "replication/dataqueuedefs.h"
#include "replication/replicainternal.h"
#include "replication/libpqwalreceiver.h"
#include "replication/obswalreceiver.h"
#include "storage/latch.h"
#include "storage/spin.h"
#include "pgxc/barrier.h"

/*
 * MAXCONNINFO: maximum size of a connection string.
 *
 * XXX: Should this move to pg_config_manual.h?
 */
#define MAXCONNINFO 1024
#define HIGHEST_PERCENT 100
#define STREAMING_START_PERCENT 90

#define DUMMY_STANDBY_DATADIR "base/dummy_standby"

#define CHECK_MSG_SIZE(msglen, structType, errmsg)\
    if (msglen != sizeof(structType)) \
        ereport(ERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION), \
            errmsg_internal(errmsg)));
/* Can we allow the standby to accept replication connection from another standby? */
#define AllowCascadeReplication() \
    (g_instance.attr.attr_storage.EnableHotStandby && g_instance.attr.attr_storage.max_wal_senders > 0)

#define SETXLOGLOCATION(a, b) \
    do {                      \
        a = b;                \
    } while (0);

/*
 * Values for WalRcv->walRcvState.
 */
typedef enum {
    WALRCV_STOPPED,  /* stopped and mustn't start up again */
    WALRCV_STARTING, /* launched, but the process hasn't initialized yet */
    WALRCV_RUNNING,  /* walreceiver is running */
    WALRCV_STOPPING  /* requested to stop, but still running */
} WalRcvState;

/*
 * @@GaussDB@@
 * Brief		: Indicate the connect error of server try to connect the primary.
 * Description	: NONE_ERROR		(first try to connect the primary)
 *				  CHANNEL_ERROR		(server should try next channel)
 *				  REPL_INFO_ERROR	(server should try next replconnlist)
 */
typedef enum { NONE_ERROR, CHANNEL_ERROR, REPL_INFO_ERROR } WalRcvConnError;

typedef struct WalRcvCtlBlock {
    XLogRecPtr receivePtr; /* last byte + 1 received in the standby. */
    XLogRecPtr writePtr;   /* last byte + 1 written out in the standby */
    XLogRecPtr flushPtr;   /* last byte + 1 flushed in the standby */
    XLogRecPtr walStart;
	XLogRecPtr lastReadPtr;
    int64 walWriteOffset;
    int64 walFreeOffset;
    int64 walReadOffset;
    bool walIsWriting;
    slock_t mutex;

    char walReceiverBuffer[FLEXIBLE_ARRAY_MEMBER];
} WalRcvCtlBlock;

typedef enum { REPCONNTARGET_DEFAULT, REPCONNTARGET_PRIMARY, REPCONNTARGET_DUMMYSTANDBY, REPCONNTARGET_STANDBY, REPCONNTARGET_OBS } ReplConnTarget;

/* Shared memory area for management of walreceiver process */
typedef struct WalRcvData {
    /*
     * PID of currently active walreceiver process, its current state and
     * start time (actually, the time at which it was requested to be
     * started).
     */
    ThreadId pid;
    ThreadId writerPid;
    int lwpId;
    WalRcvState walRcvState;
    ClusterNodeState node_state; /* state of the node in the cluster */
    pg_time_t startTime;

    ServerMode peer_role;
    bool isRuning;
    /*walsender and walreceiver xlog locations*/
    XLogRecPtr sender_sent_location;
    XLogRecPtr sender_write_location;
    XLogRecPtr sender_flush_location;
    XLogRecPtr sender_replay_location;
    XLogRecPtr receiver_received_location;
    XLogRecPtr receiver_write_location;
    XLogRecPtr receiver_flush_location;
    XLogRecPtr receiver_replay_location;

    DbState peer_state;

    /*
     * receiveStart is the first byte position that will be received. When
     * startup process starts the walreceiver, it sets receiveStart to the
     * point where it wants the streaming to begin.
     */
    XLogRecPtr receiveStart;

    /*
     * receivedUpto-1 is the last byte position that has already been
     * received.  At the first startup of walreceiver, receivedUpto is set to
     * receiveStart. After that, walreceiver updates this whenever it flushes
     * the received WAL to disk.
     */
    XLogRecPtr receivedUpto;

    /*
     * The xlog locations is used for counting sync percentage in function GetSyncPercent.
     */
    XLogRecPtr syncPercentCountStart;

    /*
     * latestChunkStart is the starting byte position of the current "batch"
     * of received WAL.  It's actually the same as the previous value of
     * receivedUpto before the last flush to disk.	Startup process can use
     * this to detect whether it's keeping up or not.
     */
    XLogRecPtr latestChunkStart;

    /*
     * position and crc of the latest valid WAL record on the receiver.
     */
    XLogRecPtr latestValidRecord;
    pg_crc32 latestRecordCrc;

    /*
     * Time of send and receive of any message received.
     */
    TimestampTz lastMsgSendTime;
    TimestampTz lastMsgReceiptTime;

    /*
     * Latest reported end of WAL on the sender
     */
    XLogRecPtr latestWalEnd;
    TimestampTz latestWalEndTime;

    /* recvwriter write queue position (local queue) */
    DataQueuePtr local_write_pos;

    int dummyStandbySyncPercent;
    /* Flag if failed to connect to dummy when failover */
    bool dummyStandbyConnectFailed;

    /*
     * connection string; is used for walreceiver to connect with the primary.
     */
    char conninfo[MAXCONNINFO];
    int ntries;

    /*
     * replication slot name; is also used for walreceiver to connect with
     * the primary
     */
    char slotname[NAMEDATALEN];

    WalRcvConnError conn_errno;
    ReplConnInfo conn_channel;
    ReplConnTarget conn_target;

    Latch* walrcvWriterLatch;
    WalRcvCtlBlock* walRcvCtlBlock;
    slock_t mutex; /* locks shared variables shown above */
    slock_t exitLock;
    char recoveryTargetBarrierId[MAX_BARRIER_ID_LENGTH];
    char recoveryStopBarrierId[MAX_BARRIER_ID_LENGTH];
    char lastRecoveredBarrierId[MAX_BARRIER_ID_LENGTH];
    XLogRecPtr lastRecoveredBarrierLSN;
    Latch* obsArchLatch;
    bool archive_enabled;
    XLogRecPtr standby_archive_start_point;
    Latch* arch_latch;
    bool arch_finish_result;
    volatile unsigned int arch_task_status;
    ArchiveXlogMessage archive_task;
} WalRcvData;

typedef struct WalReceiverFunc {
    bool (*walrcv_connect)(char* conninfo, XLogRecPtr* startpoint, char* slotname, int channel_identifier);
    bool (*walrcv_receive)(int timeout, unsigned char* type, char** buffer, int* len);
    void (*walrcv_send)(const char *buffer, int nbytes);
    void (*walrcv_disconnect)();
} WalReceiverFunc;

extern const WalReceiverFunc WalReceiverFuncTable[];

extern XLogRecPtr latestValidRecord;
extern pg_crc32 latestRecordCrc;

extern const char *g_reserve_param[RESERVE_SIZE];
extern bool ws_dummy_data_writer_use_file;
extern THR_LOCAL uint32 ws_dummy_data_read_file_num;

/* prototypes for functions in walreceiver.c */
extern void WalReceiverMain(void);
extern void walrcvWriterMain(void);

/* prototypes for functions in walrcvwriter.cpp */
extern int WalDataRcvWrite(void);

extern void WSDataRcvCheck(char* data_buf, Size nbytes);

/* prototypes for functions in walreceiverfuncs.c */
extern Size WalRcvShmemSize(void);
extern void WalRcvShmemInit(void);
extern void KillWalRcvWriter(void);
extern void ShutdownWalRcv(void);
extern bool WalRcvInProgress(void);
extern void connect_dn_str(char* conninfo, int replIndex);
extern void RequestXLogStreaming(
    XLogRecPtr* recptr, const char* conninfo, ReplConnTarget conn_target, const char* slotname);
extern StringInfo get_rcv_slot_name(void);
extern XLogRecPtr GetWalRcvWriteRecPtr(XLogRecPtr* latestChunkStart);
extern XLogRecPtr GetWalStartPtr();
extern bool WalRcvAllReplayIsDone();
extern bool WalRcvIsDone();
extern int GetReplicationApplyDelay(void);
extern int GetReplicationTransferLatency(void);
extern int GetWalRcvDummyStandbySyncPercent(void);
extern void SetWalRcvDummyStandbySyncPercent(int percent);
extern void CloseWSDataFileOnDummyStandby(void);
extern void InitWSDataNumOnDummyStandby(void);

extern WalRcvCtlBlock* getCurrentWalRcvCtlBlock(void);
extern int walRcvWrite(WalRcvCtlBlock* walrcb);
extern int WSWalRcvWrite(WalRcvCtlBlock* walrcb, char* buf, Size nbytes, XLogRecPtr start_ptr);
extern void WalRcvXLogClose(void);
extern bool WalRcvIsShutdown(void);

extern void ProcessWSRmXLog(void);
extern void ProcessWSRmData(void);
extern void SetWalRcvWriterPID(ThreadId tid);
extern bool WalRcvWriterInProgress(void);
extern bool walRcvCtlBlockIsEmpty(void);
extern void ProcessWalRcvInterrupts(void);
extern ReplConnInfo* GetRepConnArray(int* cur_idx);
extern void XLogWalRcvSendReply(bool force, bool requestReply);
extern int GetSyncPercent(XLogRecPtr startLsn, XLogRecPtr maxLsn, XLogRecPtr nowLsn);
extern const char* wal_get_role_string(ServerMode mode, bool getPeerRole = false);
extern const char* wal_get_rebuild_reason_string(HaRebuildReason reason);
extern Datum pg_stat_get_stream_replications(PG_FUNCTION_ARGS);
extern void GetPrimaryServiceAddress(char* address, size_t address_len);
extern void MakeDebugLog(TimestampTz sendTime, TimestampTz lastMsgReceiptTime, const char* msgFmt);
extern void WalRcvSetPercentCountStartLsn(XLogRecPtr startLsn);
extern void clean_failover_host_conninfo_for_dummy(void);
extern void set_failover_host_conninfo_for_dummy(const char *remote_host, int remote_port);
extern void get_failover_host_conninfo_for_dummy(int *repl);
extern void set_wal_rcv_write_rec_ptr(XLogRecPtr rec_ptr);
extern void setObsArchLatch(const Latch* latch);
extern void SetStandbyArchLatch(const Latch* latch);


static inline void WalRcvCtlAcquireExitLock(void)
{
    /* use volatile pointer to prevent code rearrangement */
    volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;
    SpinLockAcquire(&walrcv->exitLock);
}

static inline void WalRcvCtlReleaseExitLock(void)
{
    /* use volatile pointer to prevent code rearrangement */
    volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;
    SpinLockRelease(&walrcv->exitLock);
}
#endif /* _WALRECEIVER_H */
