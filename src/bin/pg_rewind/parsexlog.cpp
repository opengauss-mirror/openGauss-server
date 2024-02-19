/* -------------------------------------------------------------------------
 *
 * parsexlog.c
 *	  Functions for reading Write-Ahead-Log
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * -------------------------------------------------------------------------
 */
#define FRONTEND 1
#include "streamutil.h"

#include "postgres_fe.h"
#include "postgres.h"
#include "knl/knl_variable.h"

#include "file_ops.h"
#include "logging.h"
#include "pg_rewind.h"
#include "pg_build.h"

#include "access/htup.h"
#include "access/xlog_internal.h"
#include "storage/smgr/segment.h"
#include "replication/slot.h"
#include "access/xlogreader.h"
#include "catalog/pg_control.h"
#include "storage/file/fio_device.h"
#include <stdlib.h>

#define CONFIG_CASCADE_STANDBY "cascade_standby"
#define CONFIG_NODENAME "pgxc_node_name"
#define INVALID_LINES_IDX (int)(~0)

static int xlogreadfd = -1;

extern uint32 term;

typedef struct XLogPageReadPrivate {
    const char* datadir;
    TimeLineID tli;
} XLogPageReadPrivate;

static void extractPageInfo(XLogReaderState* record);
static TimestampTz localGetCurrentTimestamp(void);
bool checkCommonAncestorByXlog(XLogRecPtr recptr, pg_crc32 standby_reccrc, uint32 term = 0);

/*
 * Read WAL from the datadir/pg_xlog, starting from 'startpoint' on timeline
 * 'tli'. The read process shall not end until error happens and we get an invalid
 * record. We neglect that error just throw a warning 'cause rewind takes offen
 * after an unpleasant shut down in which the record in the tail of the last wal is
 * not predicable. And if record is invalid it means that the records behind would
 * not be correct. On the other hand, we are the same with recovery in startup,
 * so it's ok to rewind the valid records we read.
 *
 * Make note of the data blocks touched by the WAL records, and return them in
 * a page map.
 */
void extractPageMap(const char* datadir, XLogRecPtr startpoint, TimeLineID tli)
{
    XLogRecord* record = NULL;
    XLogReaderState* xlogreader = NULL;
    char* errormsg = NULL;
    XLogPageReadPrivate readprivate;

    readprivate.datadir = datadir;
    readprivate.tli = tli;
    xlogreader = XLogReaderAllocate(&SimpleXLogPageRead, &readprivate);
    if (xlogreader == NULL)
        pg_log(PG_ERROR, "out of memory\n");

    do {
        record = XLogReadRecord(xlogreader, startpoint, &errormsg);
        if (record == NULL) {
            XLogRecPtr errptr;

            if (XLByteEQ(startpoint, InvalidXLogRecPtr))
                errptr = xlogreader->EndRecPtr;
            else
                errptr = startpoint;

            if (errormsg != NULL)
                pg_log(PG_WARNING,
                    "could not read WAL record at %X/%X: %s\n",
                    (uint32)(errptr >> 32),
                    (uint32)errptr,
                    errormsg);
            else
                pg_log(
                    PG_WARNING, "could not read WAL record at %X/%X\n", (uint32)(startpoint >> 32), (uint32)startpoint);
            break;
        }
        extractPageInfo(xlogreader);
        startpoint = InvalidXLogRecPtr; /* continue reading at next record */
    } while (true);

    XLogReaderFree(xlogreader);
    CloseXlogFile();
}

/*
 * Reads one WAL record. Returns the end position of the record, without
 * doing anything with the record itself.
 */
XLogRecPtr readOneRecord(const char* datadir, XLogRecPtr ptr, TimeLineID tli)
{
    XLogRecord* record = NULL;
    XLogReaderState* xlogreader = NULL;
    char* errormsg = NULL;
    XLogPageReadPrivate readprivate;
    XLogRecPtr endptr;

    readprivate.datadir = datadir;
    readprivate.tli = tli;
    xlogreader = XLogReaderAllocate(&SimpleXLogPageRead, &readprivate);
    if (xlogreader == NULL)
        pg_log(PG_ERROR, "out of memory\n");

    record = XLogReadRecord(xlogreader, ptr, &errormsg);
    if (record == NULL) {
        if (errormsg != NULL)
            pg_fatal("could not read WAL record at %X/%X: %s\n", (uint32)(ptr >> 32), (uint32)ptr, errormsg);
        else
            pg_fatal("could not read WAL record at %X/%X\n", (uint32)(ptr >> 32), (uint32)ptr);
    }
    endptr = xlogreader->EndRecPtr;

    XLogReaderFree(xlogreader);
    CloseXlogFile();
    return endptr;
}

BuildErrorCode findCommonCheckpoint(const char* datadir, TimeLineID tli, XLogRecPtr startrec, XLogRecPtr* lastchkptrec,
    TimeLineID* lastchkpttli, XLogRecPtr *lastchkptredo, uint32 term)
{
/* define a time counter, if could not find a same checkpoint within this count number change to full build */
#ifdef HAVE_INT64_TIMESTAMP
    #define TIME_COUNT 60000000
#else
    #define TIME_COUNT 300
#endif
    XLogRecPtr max_lsn;
    char returnmsg[MAX_ERR_MSG_LENTH] = {0};
    pg_crc32 maxLsnCrc = 0;
    XLogRecord* record = NULL;
    XLogRecPtr searchptr;
    XLogReaderState* xlogreader = NULL;
    char* errormsg = NULL;
    XLogPageReadPrivate readprivate;
    errno_t errorno = EOK;
    char pg_conf_file[MAXPGPATH];
    int ret = 0;
    TimestampTz start_time;
    TimestampTz current_time;
    /* maybe have some xlog dir in enable_dss mode */
    char** xlogDirs = NULL;
    int xlogDirNum = 0;

    /*
     * local max lsn must be exists, or change to full build.
     */
    if (ss_instance_config.dss.enable_dss) {
        xlogDirNum = SSInitXlogDir(&xlogDirs);
        if (xlogDirNum <= 0) {
            pg_log(PG_FATAL, "init xlog dirs failed\n");
            return BUILD_FATAL;
        }
        if (ss_instance_config.dss.enable_dorado) {
            max_lsn = startrec;
        } else {
            max_lsn =
                SSFindMaxLSN(datadir_target, returnmsg, XLOG_READER_MAX_MSGLENTH, &maxLsnCrc, xlogDirs, xlogDirNum);
        }
    } else {
        max_lsn = FindMaxLSN(datadir_target, returnmsg, XLOG_READER_MAX_MSGLENTH, &maxLsnCrc);
    }
    if (XLogRecPtrIsInvalid(max_lsn)) {
        pg_fatal("find max lsn fail, errmsg:%s\n", returnmsg);
        FreeXlogDir(xlogDirs);
        return BUILD_FATAL;
    }
    pg_log(PG_PROGRESS, "find max lsn success, %s", returnmsg);

    readprivate.datadir = datadir;
    readprivate.tli = tli;
    xlogreader = XLogReaderAllocate(&SimpleXLogPageRead, &readprivate);
    if (xlogreader == NULL) {
        pg_log(PG_ERROR, "out of memory\n");
        FreeXlogDir(xlogDirs);
        return BUILD_ERROR;
    }

    /* get conn info */
    ret = snprintf_s(pg_conf_file, MAXPGPATH, MAXPGPATH - 1, "%s/postgresql.conf", datadir_target);
    securec_check_ss_c(ret, "\0", "\0");
    get_conninfo(pg_conf_file);

    searchptr = max_lsn;
    start_time = localGetCurrentTimestamp();
    current_time = start_time;
    while (!XLogRecPtrIsInvalid(searchptr)) {
        if (current_time - start_time >= TIME_COUNT) {
            pg_log(PG_FATAL,
                "try 300s, could not find any common checkpoint, need to do full build\n");
            XLogReaderFree(xlogreader);
            CloseXlogFile();
            FreeXlogDir(xlogDirs);
            return BUILD_FATAL;
        }
        uint8 info;

        if (ss_instance_config.dss.enable_dss) {
            record = XLogReadRecordFromAllDir(xlogDirs, xlogDirNum, xlogreader, searchptr, &errormsg);
        } else {
            record = XLogReadRecord(xlogreader, searchptr, &errormsg);
        }
        if (record == NULL) {
            if (errormsg != NULL) {
                pg_fatal("could not find previous WAL record at %X/%X: %s\n",
                    (uint32)(searchptr >> 32),
                    (uint32)searchptr,
                    errormsg);
            } else {
                pg_fatal("could not find previous WAL record at %X/%X\n", (uint32)(searchptr >> 32), (uint32)searchptr);
            }
            XLogReaderFree(xlogreader);
            CloseXlogFile();
            FreeXlogDir(xlogDirs);
            return BUILD_FATAL;
        }

        /*
         * Check if it is a checkpoint record. This checkpoint record needs to
         * be the latest checkpoint before WAL forked and not the checkpoint
         * where the master has been stopped to be rewinded.
         * Check the Common LSN between primary and standby using 'IDENTIFY_CONSISTENCE'
         * The checkpoint should be already finished according to control file of both.
         */
        info = XLogRecGetInfo(xlogreader) & ~XLR_INFO_MASK;
        if (xlogreader->ReadRecPtr <= startrec && XLogRecGetRmid(xlogreader) == RM_XLOG_ID &&
            (info == XLOG_CHECKPOINT_SHUTDOWN || info == XLOG_CHECKPOINT_ONLINE)) {
            if (checkCommonAncestorByXlog(xlogreader->ReadRecPtr, record->xl_crc, term) == true) {
                CheckPoint checkPoint;
                errorno = memcpy_s(&checkPoint, sizeof(CheckPoint), XLogRecGetData(xlogreader), sizeof(CheckPoint));
                securec_check_c(errorno, "", "");
                *lastchkptrec = searchptr;
                *lastchkpttli = checkPoint.ThisTimeLineID;
                *lastchkptredo = checkPoint.redo;
                pg_log(PG_WARNING,
                    _("find common checkpoint %X/%X\n"),
                    (uint32)(xlogreader->ReadRecPtr >> 32),
                    (uint32)xlogreader->ReadRecPtr);
                break;
            }
            /* quick exit */
            if (increment_return_code != BUILD_SUCCESS) {
                XLogReaderFree(xlogreader);
                CloseXlogFile();
                FreeXlogDir(xlogDirs);
                return increment_return_code;
            }
        }
        current_time = localGetCurrentTimestamp();
        /* Walk backwards to previous record. */
        searchptr = record->xl_prev;
    }

    XLogReaderFree(xlogreader);
    CloseXlogFile();
    FreeXlogDir(xlogDirs);
    PG_CHECKBUILD_AND_RETURN();
    /* no common checkpoint between target and source, need full build */
    if (XLogRecPtrIsInvalid(searchptr)) {
        pg_log(PG_FATAL, "could not find any common checkpoint, must to do full build\n");
        return BUILD_FATAL;
    }
    return BUILD_SUCCESS;
}

/*
 * Extract information on which blocks the current record modifies.
 */
static void extractPageInfo(XLogReaderState* record)
{
    XLogRecPtr lsn = record->EndRecPtr;
    XLogRecPtr prev = XLogRecGetPrev(record);
    int block_id = 0;

    Assert(record != NULL);

    pg_log(PG_DEBUG,
        "extract WAL: cur: %X/%X; prev %X/%X; xid %lu; "
        "len/total_len %u/%u; info %u; rmid %u",
        (uint32)(lsn >> 32),
        (uint32)lsn,
        (uint32)(prev >> 32),
        (uint32)prev,
        XLogRecGetXid(record),
        XLogRecGetDataLen(record),
        XLogRecGetTotalLen(record),
        XLogRecGetInfo(record),
        XLogRecGetRmid(record));

    /* do actual block analyze */
    for (block_id = 0; block_id <= record->max_block_id; block_id++) {
        RelFileNode rnode;
        ForkNumber forknum;
        BlockNumber blkno;
        XLogPhyBlock pblk;

        if (!XLogRecGetBlockTag(record, block_id, &rnode, &forknum, &blkno, &pblk))
            continue;

        /* We only care about the main fork; others are copied in toto */
        if (forknum != MAIN_FORKNUM)
            continue;

        pg_log(PG_DEBUG,
            "; block%d: rel %u/%u/%u forknum %u blkno %u physical address[%u, %u]\n",
            block_id,
            rnode.spcNode,
            rnode.dbNode,
            rnode.relNode,
            forknum,
            blkno,
            pblk.relNode,
            pblk.block);

        if (OidIsValid(pblk.relNode)) {
            Assert(PhyBlockIsValid(pblk));
            rnode.relNode = pblk.relNode;
            rnode.bucketNode = SegmentBktId;
            blkno = pblk.block;
        }

        process_block_change(forknum, rnode, blkno);
    }

    pg_log(PG_DEBUG, "\n");
}

/*
 * Local version of GetCurrentTimestamp(), since we are not linked with
 * backend code.
 */
static TimestampTz localGetCurrentTimestamp(void)
{
    TimestampTz result;
    struct timeval tp;

    (void)gettimeofday(&tp, NULL);

    result = (TimestampTz)tp.tv_sec - ((POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY);

#ifdef HAVE_INT64_TIMESTAMP
    result = (result * USECS_PER_SEC) + tp.tv_usec;
#else
    result = result + (tp.tv_usec / 1000000.0);
#endif

    return result;
}

bool checkCommonAncestorByXlog(XLogRecPtr recptr, pg_crc32 standby_reccrc, uint32 term)
{
    char cmd[1024];
    PGconn* conn = NULL;
    PGresult* res = NULL;
    char* primary_reccrc = NULL;
    pg_crc32 reccrc = 0;
    int nRet;
    int havexlog;

    /* find a available conn */
    conn = check_and_conn(standby_connect_timeout, standby_recv_timeout, term);
    if (NULL == conn) {
        pg_fatal("could not connect to server, failed to identify consistency, change to full build.");
        return false;
    }

    /* quick exit when connection lost */
    if (PQstatus(conn) != CONNECTION_OK) {
        pg_fatal("connection lost, failed to identify consistency, change to full build.");
        PQfinish(conn);
        return false;
    }
    nRet = snprintf_s(
        cmd, sizeof(cmd), sizeof(cmd) - 1, "IDENTIFY_CONSISTENCE %X/%X", (uint32)(recptr >> 32), (uint32)recptr);
    securec_check_ss_c(nRet, "\0", "\0");
    res = PQexec(conn, cmd);
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        pg_log(PG_WARNING, _(" could not IDENTIFY_CONSISTENCE system: %s\n"), PQerrorMessage(conn));
        PQclear(res);
        PQfinish(conn);
        return false;
    }
    /* To support grayupgrade, msg with 1 row of 2 or 3 columns are permitted. Will remove later. */
    if ((PQnfields(res) != 3 && PQnfields(res) != 2) || PQntuples(res) != 1) {
        pg_log(PG_WARNING,
            _(" could not IDENTIFY_CONSISTENCE, got %d rows and %d fields\n"),
            PQntuples(res),
            PQnfields(res));
        PQclear(res);
        PQfinish(conn);
        return false;
    }
    primary_reccrc = PQgetvalue(res, 0, 0);
    havexlog = atoi(PQgetvalue(res, 0, 1));

    if (primary_reccrc && sscanf_s(primary_reccrc, "%8X", &reccrc) != 1) {
        PQclear(res);
        PQfinish(conn);
        return false;
    }
    PQclear(res);
    PQfinish(conn);
    pg_log(PG_PROGRESS, _("request lsn is %X/%X and its crc(source, target):[%u, %u]\n"), 
        (uint32)(recptr >> 32), (uint32)recptr, reccrc, standby_reccrc);
    if (reccrc == standby_reccrc) {
        return true;
    }
    return false;
}

bool TransLsn2XlogFileName(XLogRecPtr lsn, TimeLineID lastcommontli, char* xlogName)
{
    XLogSegNo segno;
    int errorno;
    if (XLogRecPtrIsInvalid(lsn)) {
        return false;
    }
    XLByteToSeg(lsn, segno);
    errorno = snprintf_s(xlogName,
        XLOG_FILE_NAME_LENGTH,
        XLOG_FILE_NAME_LENGTH - 1,
        "%08X%08X%08X",
        lastcommontli,
        (uint32)((segno) / XLogSegmentsPerXLogId),
        (uint32)((segno) % XLogSegmentsPerXLogId));
    securec_check_ss_c(errorno, "", "");
    return true;
}

/*
 * When the restart lsn is less than the max lsn in the pg_xlog directory, it means this database instance
 * is running more than common point. Thus the restart lsn must be checked if it is the bound of page in xlogfile.
 */
XLogRecPtr getValidCommonLSN(XLogRecPtr checkLsn, XLogRecPtr maxLsn)
{
    XLogRecord* record = NULL;
    XLogReaderState* xlogreader = NULL;
    char* errormsg = NULL;
    XLogPageReadPrivate readprivate;
    XLogRecPtr searchLsn = maxLsn;
    XLogRecPtr startLsn = InvalidXLogRecPtr;
    XLogRecPtr curLsn = InvalidXLogRecPtr;
    XLogSegNo maxLogSegNo = 0;
    XLogSegNo checkLogSegNo = 0;
    XLogSegNo loopLogSegNo = 0;

    readprivate.datadir = datadir_target;
    readprivate.tli = 1;
    xlogreader = XLogReaderAllocate(&SimpleXLogPageRead, &readprivate);
    if (xlogreader == NULL) {
        pg_log(PG_ERROR, "getValidCommonLSN out of memory\n");
        return InvalidXLogRecPtr;
    }

    /* Check current lsn could be read. */
    if (XRecOffIsValid(checkLsn) && !XLogRecPtrIsInvalid(checkLsn)) {
        record = XLogReadRecord(xlogreader, checkLsn, &errormsg);
        if (record != NULL) {
            XLogReaderFree(xlogreader);
            close(xlogreadfd);
            xlogreadfd = -1;
            return checkLsn;
        }
    }

    /*
     * When check failed, found the latest LSN before checklsn which readend less than checklsn
     * Using this method in order to prevent high IO and long time.
     * e.g, when maxlsn = 256/2492ab3f, checkLsn 245/1248a000,
     * then get the log number of maxlsn is 0x25624, the log number of checkLsn is 0x24512.
     * Thus found the valid lsn from 0x24513 which in the file in 000000010000024500000013.
     * At last, from the nearest file to find the valid common lsn which is lower than the checklsn.
     */
    XLByteToPrevSeg(maxLsn, maxLogSegNo);
    XLByteToPrevSeg(checkLsn, checkLogSegNo);
    if (maxLogSegNo > (checkLogSegNo + 1)) {
        for (loopLogSegNo = (checkLogSegNo + 1); loopLogSegNo <= maxLogSegNo; loopLogSegNo++) {
            startLsn = loopLogSegNo * XLogSegSize;
            curLsn = InvalidXLogRecPtr;
            curLsn = XLogFindNextRecord(xlogreader, startLsn);
            if (!XLogRecPtrIsInvalid(curLsn)) {
                searchLsn = curLsn;
                break;
            }
        }
    }

    while (!XLogRecPtrIsInvalid(searchLsn)) {
        record = XLogReadRecord(xlogreader, searchLsn, &errormsg);
        if (record == NULL) {
            pg_fatal("could not find previous WAL record at %X/%X: %s\n",
                (uint32)(searchLsn >> 32),
                (uint32)searchLsn,
                (errormsg != NULL) ? errormsg : "nothing");
            searchLsn = InvalidXLogRecPtr;
            break;
        }
        if (XLByteLT(searchLsn, checkLsn)) {
            searchLsn = record->xl_prev;
            break;
        }
        searchLsn = record->xl_prev;
    }

    XLogReaderFree(xlogreader);
    close(xlogreadfd);
    xlogreadfd = -1;
    if (XRecOffIsValid(searchLsn) && !XLogRecPtrIsInvalid(searchLsn)) {
        return searchLsn;
    }
    return InvalidXLogRecPtr;
}

void recordReadTest(const char* datadir, XLogRecPtr ptr, TimeLineID tli)
{
    XLogRecord* record = NULL;
    XLogReaderState* xlogreader = NULL;
    char* errormsg = NULL;
    XLogPageReadPrivate readprivate;
    XLogRecPtr endptr;
    uint8 info;
    bool ischeckpoint = false;

    if (!XRecOffIsValid(ptr) || XLogRecPtrIsInvalid(ptr)) {
        pg_log(PG_PRINT, "It's not a valid WAL record start at %X/%X\n", (uint32)(ptr >> 32), (uint32)ptr);
        exit(0);
    }
    readprivate.datadir = datadir;
    readprivate.tli = tli;
    xlogreader = XLogReaderAllocate(&SimpleXLogPageRead, &readprivate);
    if (xlogreader == NULL) {
        pg_log(PG_ERROR, "out of memory\n");
    }

    record = XLogReadRecord(xlogreader, ptr, &errormsg);
    if (record == NULL) {
        pg_log(PG_PRINT, "It's not a valid WAL record start at %X/%X\n", (uint32)(ptr >> 32), (uint32)ptr);
        exit(0);
    }
    endptr = xlogreader->EndRecPtr;

    info = XLogRecGetInfo(xlogreader) & ~XLR_INFO_MASK;
    if (XLogRecGetRmid(xlogreader) == RM_XLOG_ID &&
        (info == XLOG_CHECKPOINT_SHUTDOWN || info == XLOG_CHECKPOINT_ONLINE)) {
        ischeckpoint = true;
    }

    pg_log(PG_PRINT,
        "   Input LSN %X/%X check success, %s\n",
        (uint32)(ptr >> 32),
        (uint32)ptr,
        (ischeckpoint == true) ? "and is a checkpoint" : "and is not a checkpoint");
    XLogReaderFree(xlogreader);
    CloseXlogFile();
    return;
}

static void ReadConfirmedLSNFromDisk(char* buffer, XLogRecPtr *confirmedLsn)
{
    ReplicationSlotOnDisk slot;
    pg_crc32c checksum;

    int rc = memcpy_s(&slot, sizeof(ReplicationSlotOnDisk), buffer, sizeof(ReplicationSlotOnDisk));
    securec_check(rc, "", "");

    INIT_CRC32C(checksum);
    COMP_CRC32C(checksum, &slot.slotdata, sizeof(ReplicationSlotPersistentData));
    FIN_CRC32C(checksum);

    if (!EQ_CRC32C(checksum, slot.checksum) ||
        slot.magic != SLOT_MAGIC ||
        slot.length != ReplicationSlotOnDiskDynamicSize) {
        pg_log(PG_ERROR, "Failed to check CRC/Magic/Lenth of replication slot %s.\n", slot.slotdata.name.data);
        return;
    }

    if (GET_SLOT_PERSISTENCY(slot.slotdata) != RS_PERSISTENT) {
        return;
    }

    if (XLogRecPtrIsValid(slot.slotdata.confirmed_flush) && XLByteLT(*confirmedLsn, slot.slotdata.confirmed_flush)) {
        *confirmedLsn = slot.slotdata.confirmed_flush;
        pg_log(PG_PROGRESS, "Get confirmed lsn %X/%X from replication slot %s.", (uint32)((*confirmedLsn) >> 32),
            (uint32)(*confirmedLsn), slot.slotdata.name.data);
    }
}

bool FindConfirmedLSN(const char* dataDir, XLogRecPtr *confirmedLsn)
{
    DIR *replicationDir = NULL;
    struct dirent *replicationDe = NULL;
    pg_log(PG_PROGRESS, "Start to get the confirmed LSN of the replication slots.\n");
    char replicationDirPath[MAXPGPATH] = { 0 };
    int rc = EOK;
    rc = sprintf_s(replicationDirPath, MAXPGPATH, "%s/pg_replslot", dataDir);
    securec_check_ss_c(rc, "\0", "\0");
    replicationDir = opendir(replicationDirPath);
    if (replicationDir == NULL) {
        pg_log(PG_WARNING, "The DIR pg_replslot does'nt exist. Can't get confirmed LSN.\n");
        return false;
    }

    while ((replicationDe = readdir(replicationDir)) != NULL) {
        char path[MAXPGPATH] = { 0 };
        struct stat fst;
        size_t size;
        char* buffer = NULL;

        if (strcmp(replicationDe->d_name, ".") == 0 || strcmp(replicationDe->d_name, "..") == 0)
            continue;

        rc = snprintf_s(path, sizeof(path), MAXPGPATH - 1, "%s/%s/state", replicationDirPath, replicationDe->d_name);
        securec_check_ss_c(rc, "\0", "\0");

        if (lstat(path, &fst) == 0) {
            buffer = slurpFile("", path, &size);
            if (size != sizeof(ReplicationSlotOnDisk)) {
                pg_log(PG_ERROR, "unexpected slot file size %d, expected %d\n", (int)size,
                    (int)sizeof(ReplicationSlotOnDisk));
                free(buffer);
                continue;
            }
            ReadConfirmedLSNFromDisk(buffer, confirmedLsn);
            free(buffer);
        }
    }
    closedir(replicationDir);

    if (XLogRecPtrIsInvalid(*confirmedLsn)) {
        return false;
    }

    return true;
}

BuildErrorCode CheckConfirmedLSNOnTarget(const char *datadir, TimeLineID tli, XLogRecPtr ckptRedo, XLogRecPtr confirmedLSN,
    uint32 term)
{
    if (XLogRecPtrIsInvalid(confirmedLSN) || XLByteLT(confirmedLSN, ckptRedo)) {
        return BUILD_SUCCESS;
    }

    XLogPageReadPrivate privateReader;
    XLogReaderState *xlogreader = NULL;
    char pg_conf_file[MAXPGPATH];
    char *errormsg = NULL;

    privateReader.datadir = datadir;
    privateReader.tli = tli;
    xlogreader = XLogReaderAllocate(&SimpleXLogPageRead, &privateReader);
    if (xlogreader == NULL) {
        pg_log(PG_ERROR, "out of memory\n");
        return BUILD_FATAL;
    }

    /* get conn info */
    int ret = snprintf_s(pg_conf_file, MAXPGPATH, MAXPGPATH - 1, "%s/postgresql.conf", datadir_target);
    securec_check_ss_c(ret, "\0", "\0");
    get_conninfo(pg_conf_file);

    XLogRecord *record = NULL;
    record = XLogReadRecord(xlogreader, confirmedLSN, &errormsg);
    if (record == NULL) {
        if (errormsg != NULL) {
            pg_fatal("could not find previous WAL record at %X/%X: %s\n", (uint32)(confirmedLSN >> 32),
                (uint32)confirmedLSN, errormsg);
        } else {
            pg_fatal("could not find previous WAL record at %X/%X\n", (uint32)(confirmedLSN >> 32),
                (uint32)confirmedLSN);
        }
        XLogReaderFree(xlogreader);
        CloseXlogFile();
        return BUILD_FATAL;
    }

    if (checkCommonAncestorByXlog(xlogreader->ReadRecPtr, record->xl_crc, term) == false) {
        pg_log(PG_FATAL, "could not find confirmed record %X/%X on source\n", (uint32)(confirmedLSN >> 32),
            (uint32)confirmedLSN);
        XLogReaderFree(xlogreader);
        CloseXlogFile();
        return BUILD_FATAL;
    }

    XLogReaderFree(xlogreader);
    CloseXlogFile();
    pg_log(PG_PROGRESS, "Check confirmed lsn %X/%X at source success.", (uint32)((confirmedLSN) >> 32),
        (uint32)(confirmedLSN));
    return BUILD_SUCCESS;
}

bool CheckIfEanbedSaveSlots()
{
    const char* optname[] = {"enable_save_confirmed_lsn"};
    char config_file[MAXPGPATH] = {0};
    char** optlines = NULL;
    int ret = EOK;
    int lines_index = 0;
    int optvalue_off;
    int optvalue_len;
    char optvalue[MAX_VALUE_LEN] = { 0 };

    ret = snprintf_s(config_file, MAXPGPATH, MAXPGPATH - 1, "%s/postgresql.conf", pg_data);
    securec_check_ss_c(ret, "\0", "\0");
    config_file[MAXPGPATH - 1] = '\0';
    optlines = readfile(config_file);

    if (optlines == NULL) {
        pg_log(PG_WARNING, _("%s cannot be opened.\n"), config_file);
        return false;
    }

    lines_index = find_gucoption((const char **)optlines, (const char *)optname[0], NULL, NULL, &optvalue_off, &optvalue_len);
    if (lines_index != INVALID_LINES_IDX) {
        ret = strncpy_s(optvalue, MAX_VALUE_LEN, optlines[lines_index] + optvalue_off,
            (size_t)Min(optvalue_len, MAX_VALUE_LEN - 1));
        securec_check_c(ret, "", "");
        if ((strcmp(optvalue, "1") == 0) || (strcmp(optvalue, "true") == 0) || (strcmp(optvalue, "on") == 0)) {
            pg_log(PG_PROGRESS, _("Enable saving confirmed_lsn in target %s\n"), config_file);
            return true;
        }
    }

    return false;
}
