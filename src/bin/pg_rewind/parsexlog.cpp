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

#include "filemap.h"
#include "logging.h"
#include "pg_rewind.h"
#include "pg_build.h"

#include "access/htup.h"
#include "access/xlog_internal.h"
#include "access/xlogreader.h"
#include "catalog/pg_control.h"
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
static void extractWalDataInfo(XLogReaderState* record);
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
 * Read WAL from the datadir/pg_xlog, starting from 'startpoint' on timeline
 * 'tli'. The read process shall not end until error happens and we get an invalid
 * record. We neglect that error just throw a warning 'cause rewind takes offen
 * after an unpleasant shut down in which the record in the tail of the last wal is
 * not predicable. And if record is invalid it means that the records behind would
 * not be correct. On the other hand, we are the same with recovery in startup,
 * so it's ok to rewind the valid records we read.
 *
 * Make note of the replication data blocks touched by the logical WAL records, and return them in
 * a page map.
 */
void extractWalDataMap(const char* datadir, XLogRecPtr startpoint, TimeLineID tli, bool& foundWalData)
{
    XLogRecord* record = NULL;
    XLogReaderState* xlogreader = NULL;
    char* errormsg = NULL;
    XLogPageReadPrivate readprivate;
    uint8 info;

    readprivate.datadir = datadir;
    readprivate.tli = tli;
    xlogreader = XLogReaderAllocate(&SimpleXLogPageRead, &readprivate);
    if (xlogreader == NULL) {
        pg_log(PG_ERROR, "xlogreader is null, allocate failed!\n");
    }

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

        /* once found a #ref_xlog and be hurry to deal with it. */
        if (RM_HEAP2_ID == record->xl_rmid) {
            info = XLogRecGetInfo(xlogreader) & ~XLR_INFO_MASK;
            if (XLOG_HEAP2_LOGICAL_NEWPAGE == (info & XLOG_HEAP_OPMASK))
                extractWalDataInfo(xlogreader);

            if (!foundWalData)
                foundWalData = true;
        }
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

/*
 * Find the previous checkpoint preceding given WAL position.
 */
BuildErrorCode findLastCheckpoint(const char* datadir, XLogRecPtr forkptr, TimeLineID tli, XLogRecPtr* lastchkptrec,
    TimeLineID* lastchkpttli, XLogRecPtr* lastchkptredo)
{
    /* Walk backwards, starting from the given record */
    XLogRecord* record = NULL;
    XLogRecPtr searchptr;
    XLogReaderState* xlogreader = NULL;
    char* errormsg = NULL;
    XLogPageReadPrivate readprivate;
    errno_t errorno = EOK;

    /*
     * The given fork pointer points to the end of the last common record,
     * which is not necessarily the beginning of the next record, if the
     * previous record happens to end at a page boundary. Skip over the page
     * header in that case to find the next record.
     * The diveraged LSN may not be a valid LSN, so validate it before
     * ReadRecord.
     */
    if (forkptr % XLOG_BLCKSZ == 0) {
        if (forkptr % XLogSegSize == 0)
            forkptr += SizeOfXLogLongPHD;
        else
            forkptr += SizeOfXLogShortPHD;
    }

    readprivate.datadir = datadir;
    readprivate.tli = tli;
    xlogreader = XLogReaderAllocate(&SimpleXLogPageRead, &readprivate);
    if (xlogreader == NULL) {
        pg_log(PG_ERROR, "out of memory\n");
        return BUILD_ERROR;
    }

    searchptr = forkptr;
    while (!XLogRecPtrIsInvalid(searchptr)) {
        uint8 info;

        record = XLogReadRecord(xlogreader, searchptr, &errormsg);
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
            return BUILD_FATAL;
        }

        /*
         * Check if it is a checkpoint record. This checkpoint record needs to
         * be the latest checkpoint before WAL forked and not the checkpoint
         * where the master has been stopped to be rewinded.
         */
        info = XLogRecGetInfo(xlogreader) & ~XLR_INFO_MASK;
        if (XLByteLT(searchptr, forkptr) && XLogRecGetRmid(xlogreader) == RM_XLOG_ID &&
            (info == XLOG_CHECKPOINT_SHUTDOWN || info == XLOG_CHECKPOINT_ONLINE)) {
            CheckPoint checkPoint;

            errorno = memcpy_s(&checkPoint, sizeof(CheckPoint), XLogRecGetData(xlogreader), sizeof(CheckPoint));
            securec_check_c(errorno, "", "");
            *lastchkptrec = searchptr;
            *lastchkpttli = checkPoint.ThisTimeLineID;
            *lastchkptredo = checkPoint.redo;

            pg_log(PG_PROGRESS,
                "find last common checkpoint at %X/%X on timeline %u, cooresponding redo point at %X/%X\n",
                (uint32)(searchptr >> 32),
                (uint32)searchptr,
                checkPoint.ThisTimeLineID,
                (uint32)(*lastchkptredo >> 32),
                (uint32)*lastchkptredo);
            break;
        }

        /* Walk backwards to previous record. */
        searchptr = record->xl_prev;
    }
    XLogReaderFree(xlogreader);
    CloseXlogFile();
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

        if (!XLogRecGetBlockTag(record, block_id, &rnode, &forknum, &blkno))
            continue;

        /* We only care about the main fork; others are copied in toto */
        if (forknum != MAIN_FORKNUM)
            continue;

        pg_log(PG_DEBUG,
            "; block%d: rel %u/%u/%u forknum %u blkno %u",
            block_id,
            rnode.spcNode,
            rnode.dbNode,
            rnode.relNode,
            forknum,
            blkno);

        process_block_change(forknum, rnode, blkno);
    }

    pg_log(PG_DEBUG, "\n");
}

/*
 * Extract information on which replication blocks the current record carries.
 */
static void extractWalDataInfo(XLogReaderState* record)
{
    XLogRecPtr lsn = record->EndRecPtr;
    XLogRecPtr prev = XLogRecGetPrev(record);
    xl_heap_logical_newpage* xlrec = NULL;
    ForkNumber fork_num;
    uint64 file_offset = 0;

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

    xlrec = (xl_heap_logical_newpage*)XLogRecGetData(record);

    /* collect all the wal data info */
    fork_num = xlrec->forknum;

    if (xlrec->type == ROW_STORE) {
        ; /* do nothing for the Row Store. */
    } else if (xlrec->type == COLUMN_STORE) {
        fork_num = MAX_FORKNUM + xlrec->attid;
        file_offset = xlrec->offset;
    } else {
        pg_fatal("unknown storage type: %d\n", xlrec->type);
    }

    RelFileNode tmp_node;
    tmp_node.spcNode = xlrec->node.spcNode;
    tmp_node.dbNode = xlrec->node.dbNode;
    tmp_node.relNode = xlrec->node.relNode;
    tmp_node.bucketNode = XLogRecGetBucketId(record);

    process_waldata_change(fork_num, tmp_node, xlrec->type, file_offset, xlrec->blockSize);

    pg_log(PG_DEBUG, "\n");
}

/*
 * This function is the last way to Find the common lsn between primary and standby.
 * If no find, must use full build to rebuild the DN node.
 */
BuildErrorCode findLastCommonpoint(
    const char* datadir, XLogRecPtr forkptr, TimeLineID tli, XLogRecPtr* lastcommonrec, uint32 term)
{
    /* Walk backwards, starting from the given record */
    XLogRecord* record = NULL;
    XLogRecPtr searchptr;
    XLogReaderState* xlogreader = NULL;
    char* errormsg = NULL;
    XLogPageReadPrivate readprivate;
    XLogRecPtr resultptr = InvalidXLogRecPtr;

    /*
     * The given fork pointer points to the end of the last common record,
     * which is not necessarily the beginning of the next record, if the
     * previous record happens to end at a page boundary. Skip over the page
     * header in that case to find the next record.
     * The diveraged LSN may not be a valid LSN, so validate it before
     * ReadRecord.
     */
    if (forkptr % XLOG_BLCKSZ == 0) {
        if (forkptr % XLogSegSize == 0)
            forkptr += SizeOfXLogLongPHD;
        else
            forkptr += SizeOfXLogShortPHD;
    }

    readprivate.datadir = datadir;
    readprivate.tli = tli;
    xlogreader = XLogReaderAllocate(&SimpleXLogPageRead, &readprivate);
    if (xlogreader == NULL) {
        pg_fatal("out of memory\n");
        return BUILD_ERROR;
    }

    searchptr = forkptr;
    while (!XLogRecPtrIsInvalid(searchptr)) {
        record = XLogReadRecord(xlogreader, searchptr, &errormsg);
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
            return BUILD_FATAL;
        }

        /*
         * check the Common LSN between the primary with sql 'IDENTIFY_CONSISTENCE'
         */
        if (checkCommonAncestorByXlog(xlogreader->ReadRecPtr, record->xl_crc, term) == true) {
            resultptr = xlogreader->ReadRecPtr;
            pg_log(PG_WARNING,
                _("find common lsn %X/%X\n"),
                (uint32)(xlogreader->ReadRecPtr >> 32),
                (uint32)xlogreader->ReadRecPtr);
            break;
        }
        /* Walk backwards to previous record. */
        searchptr = record->xl_prev;
    }

    XLogReaderFree(xlogreader);
    CloseXlogFile();

    if (streamConn != NULL) {
        PQfinish(streamConn);
        streamConn = NULL;
    }
    PG_CHECKBUILD_AND_RETURN();
    *lastcommonrec = resultptr;
    /* no common lsn between target and source, need full build */
    if (XLogRecPtrIsInvalid(resultptr)) {
        pg_log(PG_FATAL, "could not find any common LSN, need full build\n");
        return BUILD_FATAL;
    }

    return BUILD_SUCCESS;
}

bool checkCommonAncestorByXlog(XLogRecPtr recptr, pg_crc32 standby_reccrc, uint32 term)
{
    char cmd[1024];
    PGresult* res = NULL;
    char* primary_reccrc = NULL;
    pg_crc32 reccrc = 0;
    int nRet;
    int havexlog;
    char pg_conf_file[MAXPGPATH];
    int ret = 0;

    /* fist open */
    if (streamConn == NULL) {
        ret = snprintf_s(pg_conf_file, MAXPGPATH, MAXPGPATH - 1, "%s/postgresql.conf", datadir_target);
        securec_check_ss_c(ret, "\0", "\0");

        get_conninfo(pg_conf_file);
        /* find a available conn */
        streamConn = check_and_conn(standby_connect_timeout, standby_recv_timeout, term);
        if (streamConn == NULL) {
            pg_fatal("could not connect to server.");
            return false;
        }
    }
    if (PQstatus(streamConn) != CONNECTION_OK) {
        pg_fatal("we lose the connect");
        return false;
    }
    nRet = snprintf_s(
        cmd, sizeof(cmd), sizeof(cmd) - 1, "IDENTIFY_CONSISTENCE %X/%X", (uint32)(recptr >> 32), (uint32)recptr);
    securec_check_ss_c(nRet, "\0", "\0");
    res = PQexec(streamConn, cmd);
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        pg_log(PG_WARNING, _(" could not IDENTIFY_CONSISTENCE system: %s\n"), PQerrorMessage(streamConn));
        PQclear(res);
        return false;
    }
    /* To support grayupgrade, msg with 1 row of 2 or 3 columns are permitted. Will remove later. */
    if ((PQnfields(res) != 3 && PQnfields(res) != 2) || PQntuples(res) != 1) {
        pg_log(PG_WARNING,
            _(" could not IDENTIFY_CONSISTENCE, got %d rows and %d fields\n"),
            PQntuples(res),
            PQnfields(res));
        PQclear(res);
        return false;
    }
    primary_reccrc = PQgetvalue(res, 0, 0);
    havexlog = atoi(PQgetvalue(res, 0, 1));

    if (primary_reccrc && sscanf_s(primary_reccrc, "%8X", &reccrc) != 1) {
        PQclear(res);
        return false;
    }
    PQclear(res);
    pg_log(PG_PROGRESS, _("get rec %X/%X crc %x\n"), (uint32)(recptr >> 32), (uint32)recptr, reccrc);
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
            startLsn = loopLogSegNo * XLOG_SEG_SIZE;
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
