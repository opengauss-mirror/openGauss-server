/* -------------------------------------------------------------------------
 *
 * walsender.h
 *	  Exports from replication/walsender.c.
 *
 * Portions Copyright (c) 2010-2012, PostgreSQL Global Development Group
 *
 * src/include/replication/walsender.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef _WALSENDER_H
#define _WALSENDER_H

#include <signal.h>

#include "fmgr.h"
#include "alarm/alarm.h"
#include "replication/replicainternal.h"
#include "utils/tuplestore.h"

#define WS_MAX_SEND_SIZE (uint32)(g_instance.attr.attr_storage.MaxSendSize * 1024)
#define WS_CU_SEND_SLICE_SIZE (512 * 1024)
#define WS_XLOG_HDR_SIZE (sizeof(uint32) + 1 + sizeof(XLogRecPtr))
#define WS_DATA_HDR_SIZE sizeof(DataElementHeaderData)
#define WS_DATA_MSG_HDR_SIZE sizeof(WalDataPageMessageHeader)
#define WS_DATA_MSG_PREFIX_LEN (sizeof(uint32) + 1 + sizeof(XLogRecPtr) * 2)
#define AM_WAL_NORMAL_SENDER (t_thrd.role == WAL_NORMAL_SENDER)
#define AM_WAL_STANDBY_SENDER (t_thrd.role == WAL_STANDBY_SENDER)
#define AM_WAL_DB_SENDER (t_thrd.role == WAL_DB_SENDER)
#define AM_WAL_SENDER (AM_WAL_NORMAL_SENDER || AM_WAL_STANDBY_SENDER || AM_WAL_DB_SENDER)

typedef struct WSXLogJustSendRegion {
    XLogRecPtr start_ptr;
    XLogRecPtr end_ptr;
} WSXLogJustSendRegion;

extern int WalSenderMain(void);
extern void GetPMstateAndRecoveryInProgress(void);
extern void WalSndSignals(void);
extern Size WalSndShmemSize(void);
extern void WalSndShmemInit(void);
extern void WalSndWakeup(void);
extern void WalSndRqstFileReload(void);
extern bool WalSndInProgress(int type);
extern void StandbyOrSecondaryIsAlive(void);
extern void StopAliveBuildSender(void);
extern bool IsAllBuildSenderExit();

extern bool WalSegmemtRemovedhappened;
extern AlarmCheckResult WalSegmentsRemovedChecker(Alarm* alarm, AlarmAdditionalParam* additionalParam);
extern Datum pg_stat_get_wal_senders(PG_FUNCTION_ARGS);
extern Tuplestorestate* BuildTupleResult(FunctionCallInfo fcinfo, TupleDesc* tupdesc);

extern void GetFastestReplayStandByServiceAddress(
    char* fastest_remote_address, char* second_fastest_remote_address, size_t address_len);
extern bool IsPrimaryStandByReadyToRemoteRead(void);
extern void IdentifyMode(void);
extern bool WalSndAllInProgress(int type);
extern bool WalSndQuorumInProgress(int type);
extern XLogSegNo WalGetSyncCountWindow(void);

/*
 * Remember that we want to wakeup walsenders later
 *
 * This is separated from doing the actual wakeup because the writeout is done
 * while holding contended locks.
 */
#define WalSndWakeupRequest()                         \
    do {                                              \
        t_thrd.walsender_cxt.wake_wal_senders = true; \
    } while (0)

/*
 * wakeup walsenders if there is work to be done
 */
#define WalSndWakeupProcessRequests()                             \
    do {                                                          \
        if (t_thrd.walsender_cxt.wake_wal_senders) {              \
            t_thrd.walsender_cxt.wake_wal_senders = false;        \
            if (g_instance.attr.attr_storage.max_wal_senders > 0) \
                WalSndWakeup();                                   \
        }                                                         \
    } while (0)

#define MAX_XLOG_RECORD(a, b) ((XLByteLT((a), (b))) ? (b) : (a))

#endif /* _WALSENDER_H */
