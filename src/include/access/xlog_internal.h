/*
 * xlog_internal.h
 *
 * openGauss transaction log internal declarations
 *
 * NOTE: this file is intended to contain declarations useful for
 * manipulating the XLOG files directly, but it is not supposed to be
 * needed by rmgr routines (redo support for individual record types).
 * So the XLogRecord typedef and associated stuff appear in xlogrecord.h.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/xlog_internal.h
 */
#ifndef XLOG_INTERNAL_H
#define XLOG_INTERNAL_H

#include "access/xlogdefs.h"
#include "access/xlogreader.h"
#include "access/xlog_basic.h"
#include "access/ustore/knl_undorequest.h"
#include "datatype/timestamp.h"
#include "fmgr.h"
#include "pgtime.h"
#include "storage/buf/block.h"
#include "storage/smgr/relfilenode.h"
#include "storage/buf/buf.h"

/*
 * if ValidateXlogRecord Failed in XLOG_FROM_STREAM source, the unexpected
 * walreceiver shutdown will be caused. To avoid this, add retry for readrecord in xlog redo
 */
#define XLOG_STREAM_READREC_MAXTRY 10
#define XLOG_STREAM_READREC_INTERVAL 100 * 1000L

#ifndef FRONTEND
/* Compute the xlog filename with timelineId and segment number.*/
#define XLogFileName(fname, len, tli, logSegNo)                 \
    do {                                                   \
        int nRet;                                          \
        nRet = snprintf_s(fname,                           \
            len,                                   \
            len - 1,                               \
            "%08X%08X%08X",                                \
            tli,                                           \
            (uint32)((logSegNo) / XLogSegmentsPerXLogId),  \
            (uint32)((logSegNo) % XLogSegmentsPerXLogId)); \
        securec_check_ss(nRet, "\0", "\0");                \
    } while (0)

/* compute xlog file path with timelineId and segment number. */
#define XLogFilePath(path, len, tli, logSegNo)                  \
    do {                                                   \
        int nRet;                                          \
        nRet = snprintf_s(path,                            \
            len,                                     \
            len - 1,                                 \
            "%s/%08X%08X%08X",                             \
            SS_XLOGDIR,                                       \
            tli,                                           \
            (uint32)((logSegNo) / XLogSegmentsPerXLogId),  \
            (uint32)((logSegNo) % XLogSegmentsPerXLogId)); \
        securec_check_ss(nRet, "\0", "\0");                \
    } while (0)
#else
#define XLOG_FNAME_LEN	   24
#define IsXLogFileName(fname) \
	(strlen(fname) == XLOG_FNAME_LEN && \
	 strspn(fname, "0123456789ABCDEF") == XLOG_FNAME_LEN)

#define IsPartialXLogFileName(fname)	\
	(strlen(fname) == XLOG_FNAME_LEN + strlen(".partial") &&	\
	 strspn(fname, "0123456789ABCDEF") == XLOG_FNAME_LEN &&		\
	 strcmp((fname) + XLOG_FNAME_LEN, ".partial") == 0)

#define IsTLHistoryFileName(fname)	\
	(strlen(fname) == 8 + strlen(".history") &&		\
	 strspn(fname, "0123456789ABCDEF") == 8 &&		\
	 strcmp((fname) + 8, ".history") == 0)

#define IsBackupHistoryFileName(fname) \
	(strlen(fname) > XLOG_FNAME_LEN && \
	 strspn(fname, "0123456789ABCDEF") == XLOG_FNAME_LEN && \
	 strcmp((fname) + strlen(fname) - strlen(".backup"), ".backup") == 0)

#define XLogFileName(fname, len, tli, logSegNo)                 \
    do {                                                   \
        int nRet;                                          \
        nRet = snprintf_s(fname,                           \
            len,                                   \
            len - 1,                               \
            "%08X%08X%08X",                                \
            tli,                                           \
            (uint32)((logSegNo) / XLogSegmentsPerXLogId),  \
            (uint32)((logSegNo) % XLogSegmentsPerXLogId)); \
        securec_check_ss_c(nRet, "\0", "\0");              \
    } while (0)

#define XLogFilePath(path, len, tli, logSegNo)                  \
    do {                                                   \
        int nRet;                                          \
        nRet = snprintf_s(path,                            \
            len,                                     \
            len - 1,                                 \
            "%s/%08X%08X%08X",                             \
            SS_XLOGDIR,                                       \
            tli,                                           \
            (uint32)((logSegNo) / XLogSegmentsPerXLogId),  \
            (uint32)((logSegNo) % XLogSegmentsPerXLogId)); \
        securec_check_ss_c(nRet, "\0", "\0");              \
    } while (0)
#endif

/* compute the xlog segment number with xlog filename and timelineId. */
#define XLogFromFileName(fname, tli, logSegNo)                  \
    do {                                                        \
        uint32 log;                                             \
        uint32 seg;                                             \
        int ret;                                                \
        ret = sscanf_s(fname, "%08X%08X%08X", tli, &log, &seg); \
        securec_check_for_sscanf_s(ret, 3, "\0", "\0");         \
        *logSegNo = (uint64)log * XLogSegmentsPerXLogId + seg;  \
    } while (0)

/* compute history filename with timelineID. */
#define TLHistoryFileName(fname, len, tli)                                                \
    do {                                                                             \
        int nRet = 0;                                                                \
        nRet = snprintf_s(fname, len, len - 1, "%08X.history", tli); \
        securec_check_ss(nRet, "\0", "\0");                                          \
    } while (0)

/* compute history filepath with timelineID. */
#define TLHistoryFilePath(path, len, tli)                                                     \
    do {                                                                                 \
        int nRet = 0;                                                                    \
        nRet = snprintf_s(path, len, len - 1, XLOGDIR "/%08X.history", tli); \
        securec_check_ss(nRet, "\0", "\0");                                              \
    } while (0)

/* compute status filePath with xlog name and suffix. */
#define StatusFilePath(path, len, xlog, suffix)                                                               \
    do {                                                                                                 \
        int nRet = 0;                                                                                    \
        nRet = snprintf_s(path, len, len - 1, "%s/%s%s", ARCHIVEDIR, xlog, suffix); \
        securec_check_ss(nRet, "\0", "\0");                                                              \
    } while (0)

/*compute backup history filename with timelineID, segment number and offset. */
#define BackupHistoryFileName(fname, len, tli, logSegNo, offset) \
    do {                                                    \
        int nRet = 0;                                       \
        nRet = snprintf_s(fname,                            \
            len,                                    \
            len - 1,                                \
            "%08X%08X%08X.%08X.backup",                     \
            tli,                                            \
            (uint32)((logSegNo) / XLogSegmentsPerXLogId),   \
            (uint32)((logSegNo) % XLogSegmentsPerXLogId),   \
            offset);                                        \
        securec_check_ss(nRet, "\0", "\0");                 \
    } while (0)

/**/
#define BackupHistoryFilePath(path, len, tli, logSegNo, offset) \
    do {                                                   \
        int nRet = 0;                                      \
        nRet = snprintf_s(path,                            \
            len,                                     \
            len - 1,                                 \
            "%s/%08X%08X%08X.%08X.backup",                 \
            SS_XLOGDIR,                                       \
            tli,                                           \
            (uint32)((logSegNo) / XLogSegmentsPerXLogId),  \
            (uint32)((logSegNo) % XLogSegmentsPerXLogId),  \
            offset);                                       \
        securec_check_ss(nRet, "\0", "\0");                \
    } while (0)

/*
 * Information logged when we detect a change in one of the parameters
 * important for Hot Standby.
 */
typedef struct xl_parameter_change {
    int MaxConnections;
    int max_prepared_xacts;
    int max_locks_per_xact;
    int wal_level;
} xl_parameter_change;

/* logs restore point */
typedef struct xl_restore_point {
    TimestampTz rp_time;
    char rp_name[MAXFNAMELEN];
} xl_restore_point;

/*
 * The functions in xloginsert.c construct a chain of XLogRecData structs
 * to represent the final WAL record.
 */
typedef struct XLogRecData {
    struct XLogRecData* next; /* next struct in chain, or NULL */
    char* data;               /* start of rmgr data to include */
    uint32 len;               /* length of rmgr data to include */
    Buffer buffer;            /* buffer associated with data, if any */
} XLogRecData;

/*
 * Method table for resource managers.
 *
 * This struct must be kept in sync with the PG_RMGR definition in
 * rmgr.cpp.
 *
 * RmgrTable[] is indexed by RmgrId values (see rmgrlist.h).
 */
typedef struct RmgrData {
    const char* rm_name;
    void (*rm_redo)(XLogReaderState* record);
    void (*rm_desc)(StringInfo buf, XLogReaderState* record);
    void (*rm_startup)(void);
    void (*rm_cleanup)(void);
    bool (*rm_safe_restartpoint)(void);
    int (*rm_undo)(URecVector *urecvector, int startIdx, int endIdx,
                    TransactionId xid, Oid reloid, Oid partitionoid,
                    BlockNumber blkno, bool isFullChain, 
                    int preRetCode, Oid *preReloid, Oid *prePartitionoid);
    void (*rm_undo_desc) (StringInfo buf, UndoRecord *record);
    const char* (*rm_type_name)(uint8 subtype);
} RmgrData;

/*
 * New XLogCtlInsert Structure.
 */
struct Combined128 {
    uint64 currentBytePos;
    uint32 byteSize;
    int32  LRC;
};
union Union128 {
    uint128_u value;
    struct Combined128 struct128;
};

extern const RmgrData RmgrTable[];

/*
 * Exported to support xlog switching from checkpointer
 */
extern pg_time_t GetLastSegSwitchTime(void);
extern XLogRecPtr RequestXLogSwitch(void);

/*
 * Exported to support xlog archive status setting from WALReceiver
 */
extern void XLogArchiveForceDone(const char* xlog);

/*
 * These aren't in xlog.h because I'd rather not include fmgr.h there.
 */
extern Datum pg_start_backup(PG_FUNCTION_ARGS);
extern Datum pg_stop_backup(PG_FUNCTION_ARGS);
extern Datum gs_roach_stop_backup(PG_FUNCTION_ARGS);
extern Datum pg_switch_xlog(PG_FUNCTION_ARGS);
extern Datum gs_roach_switch_xlog(PG_FUNCTION_ARGS);
extern Datum pg_create_restore_point(PG_FUNCTION_ARGS);
extern Datum pg_current_xlog_location(PG_FUNCTION_ARGS);
extern Datum pg_current_xlog_insert_location(PG_FUNCTION_ARGS);
extern Datum gs_current_xlog_insert_end_location(PG_FUNCTION_ARGS);
extern Datum pg_last_xlog_receive_location(PG_FUNCTION_ARGS);
extern Datum pg_last_xlog_replay_location(PG_FUNCTION_ARGS);
extern Datum pg_last_xact_replay_timestamp(PG_FUNCTION_ARGS);
extern Datum pg_xlogfile_name_offset(PG_FUNCTION_ARGS);
extern Datum pg_xlogfile_name(PG_FUNCTION_ARGS);
extern Datum pg_is_in_recovery(PG_FUNCTION_ARGS);
extern Datum pg_xlog_replay_pause(PG_FUNCTION_ARGS);
extern Datum pg_xlog_replay_resume(PG_FUNCTION_ARGS);
extern Datum pg_is_xlog_replay_paused(PG_FUNCTION_ARGS);
extern Datum pg_xlog_location_diff(PG_FUNCTION_ARGS);
extern int XLogPageRead(XLogReaderState *xlogreader, XLogRecPtr targetPagePtr, int reqLen, XLogRecPtr targetRecPtr,
                 char *readBuf, TimeLineID *readTLI, char* xlog_path = NULL);

bool XLogReadFromWriteBuffer(XLogRecPtr targetStartPtr, int reqLen, char* readBuf, uint32 *rereadlen);
extern void handleRecoverySusPend(XLogRecPtr lsn);

#endif /* XLOG_INTERNAL_H */
