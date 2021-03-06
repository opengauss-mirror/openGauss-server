/*
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2010-2012, PostgreSQL Global Development Group
 *
 * obswalreceiver.cpp
 * 
 * Description: This file contains the obs-specific parts of walreceiver. It's
 * loaded as a dynamic module to avoid linking the main server binary with
 * obs.
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/replication/obswalreceiver.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include <sys/time.h>

#include "replication/obswalreceiver.h"
#include "libpq/libpq-int.h"
#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "nodes/pg_list.h"
#include "access/obs/obs_am.h"
#include "utils/timestamp.h"
#include "miscadmin.h"
#include "replication/walreceiver.h"
#include "replication/obswalreceiver.h"
#include "replication/slot.h"
#include "storage/pmsignal.h"
#include "storage/proc.h"
#include "utils/guc.h"
#include "pgxc/pgxc.h"
#define CUR_OBS_FILE_VERSION 1

#ifdef ENABLE_MULTIPLE_NODES
static void convert_dn_barrierid_path(const char *dn_path, char *cn_path)
{
    errno_t rc = EOK;
    size_t len = 0;

    rc = strcpy_s(cn_path, MAXPGPATH, dn_path);
    securec_check_c(rc, "\0", "\0");

    char *p = strrchr(cn_path, '/');
    if (p == NULL) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("Obs path prefix is invalid")));
    }
    /* calc prefix len */
    *p = '\0';
    len = strlen(cn_path);

    /* concatenate prefix and "_cn" */
    rc = sprintf_s(p, MAXPGPATH - len, "/cn");
    securec_check_ss(rc, "\0", "\0");
    return;
}
#endif

bool obs_replication_read_barrier(const char* barrierFile, char* barrierOut)
{
    List *object_list = NIL;
    size_t readLen = 0;
    errno_t rc = 0;
    ObsArchiveConfig obsConfig;
    ObsArchiveConfig *archive_obs = NULL;

    archive_obs = getObsArchiveConfig();
    if (archive_obs == NULL) {
        ereport(LOG, (errmsg("Cannot get obs bucket config from replication slots")));
        return false;
    }

    /* copy OBS configs to temporary variable for customising file path */
    rc = memcpy_s(&obsConfig, sizeof(ObsArchiveConfig), archive_obs, sizeof(ObsArchiveConfig));
    securec_check(rc, "", "");

#ifdef ENABLE_MULTIPLE_NODES
    char pathPrefix[MAXPGPATH] = {0};

    if (IS_PGXC_DATANODE) {
        convert_dn_barrierid_path(obsConfig.obs_prefix, pathPrefix);
        obsConfig.obs_prefix = pathPrefix;
    }
#endif

    object_list = obsList(barrierFile, &obsConfig);
    if (object_list == NIL || object_list->length <= 0) {
        ereport(LOG, (errmsg("The Barrier ID file named %s cannot be found.", barrierFile)));
        return false;
    }

    readLen = obsRead(barrierFile, 0, barrierOut, MAX_BARRIER_ID_LENGTH, &obsConfig);
    if (readLen == 0) {
        ereport(LOG, (errmsg("Cannot read  barrier ID in %s file!", barrierFile)));
        return false;
    }
    return true;
}

void update_stop_barrier()
{
    errno_t rc = EOK;
    /* use volatile pointer to prevent code rearrangement */
    volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;
    char barrier[MAX_BARRIER_ID_LENGTH] = {0};
    if (obs_replication_read_barrier(HADR_STOP_BARRIER_ID_FILE, (char *)barrier)) {
        rc = strncpy_s((char *)walrcv->recoveryStopBarrierId, MAX_BARRIER_ID_LENGTH,
                       (char *)barrier, MAX_BARRIER_ID_LENGTH - 1);
        securec_check(rc, "\0", "\0");
        ereport(LOG, (errmsg("Get stop barrierID %s", (char *)walrcv->recoveryStopBarrierId)));
    }

}

void update_recovery_barrier()
{
    errno_t rc = EOK;
    /* use volatile pointer to prevent code rearrangement */
    volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;
    char barrier[MAX_BARRIER_ID_LENGTH] = {0};
    if (obs_replication_read_barrier(HADR_BARRIER_ID_FILE, (char *)barrier)) {
        rc = strncpy_s((char *)walrcv->recoveryTargetBarrierId, MAX_BARRIER_ID_LENGTH,
                       (char *)barrier, MAX_BARRIER_ID_LENGTH - 1);
        securec_check(rc, "\0", "\0");
    }
}

bool obs_connect(char* conninfo, XLogRecPtr* startpoint, char* slotname, int channel_identifier) 
{
    if (getObsReplicationSlot() == NULL) {
        ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                        errmsg("[walreceiver_connect_obs]could not get obs relication slot")));
        return false;
    }
    volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;
    walrcv->peer_role = PRIMARY_MODE;
    walrcv->peer_state = NORMAL_STATE;
    if (t_thrd.libwalreceiver_cxt.recvBuf != NULL) {
        PQfreemem(t_thrd.libwalreceiver_cxt.recvBuf);
    }
    t_thrd.libwalreceiver_cxt.recvBuf = NULL;  
    uint32 totalLen = sizeof(WalDataMessageHeader) + OBS_XLOG_SLICE_BLOCK_SIZE + 1;
    t_thrd.libwalreceiver_cxt.recvBuf = (char*)malloc(totalLen);
    if (t_thrd.libwalreceiver_cxt.recvBuf == NULL) {
        ereport(LOG, (errmsg("obs_receive:Receive Buffer out of memory.\n")));
        return false;
    }

    return true;
}

bool obs_receive(int timeout, unsigned char* type, char** buffer, int* len) 
{
    int dataLength;
    XLogRecPtr startPtr;
    char* recvBuf = t_thrd.libwalreceiver_cxt.recvBuf;
    errno_t rc = EOK;
 
    // t_thrd.walreceiver_cxt.walRcvCtlBlock->receivePtr will been updated in XLogWalRcvReceive()
    startPtr = t_thrd.walreceiver_cxt.walRcvCtlBlock->receivePtr;
 
    WalDataMessageHeader msghdr;
    // init
    msghdr.dataStart = startPtr;
    msghdr.walEnd = InvalidXLogRecPtr;
    msghdr.sendTime = GetCurrentTimestamp();
    msghdr.sender_sent_location = InvalidXLogRecPtr;
    msghdr.sender_write_location = InvalidXLogRecPtr;
    msghdr.sender_replay_location = InvalidXLogRecPtr;
    msghdr.sender_flush_location = InvalidXLogRecPtr;
    msghdr.catchup = false;
    
    int headLen = sizeof(WalDataMessageHeader);
    int totalLen = headLen + OBS_XLOG_SLICE_BLOCK_SIZE + 1;
    // copy WalDataMessageHeader
    rc = memcpy_s(recvBuf, totalLen, &msghdr, headLen);
    securec_check(rc, "", "");
    // copy xlog from obs
    char* dataLocation = recvBuf + headLen;
    (void)obs_replication_receive(startPtr, &dataLocation, &dataLength, timeout, NULL);
    if (dataLength <= 0) {
        return false;
    }
    
    int validLen = headLen + dataLength;
    recvBuf[validLen] = '\0'; /* Add terminating null */

    elog(LOG,"[obs_receive]get xlog startlsn %08X/%08X, len %X\n",
        (uint32)(startPtr >> 32), (uint32)startPtr, (uint32)validLen);
 
    /* Return received messages to caller */
    *type = 'w';
    *buffer = recvBuf;
    *len = validLen;
    return true;
}


void obs_send(const char *buffer, int nbytes)
{
}

void obs_disconnect(void)
{
    if (t_thrd.libwalreceiver_cxt.recvBuf != NULL) {
        PQfreemem(t_thrd.libwalreceiver_cxt.recvBuf);
    }
    t_thrd.libwalreceiver_cxt.recvBuf = NULL;

    return;
}

static char *obs_replication_get_xlog_prefix(XLogRecPtr recptr, bool onlyPath)
{
    errno_t rc = EOK;
    char xlogfname[MAXFNAMELEN];
    char xlogfpath[MAXPGPATH];
    XLogSegNo xlogSegno = 0;
    TimeLineID timeLine = DEFAULT_TIMELINE_ID;

    rc = memset_s(xlogfname, MAXFNAMELEN, 0, MAXFNAMELEN);
    securec_check_ss_c(rc, "", "");
    rc = memset_s(xlogfpath, MAXPGPATH, 0, MAXPGPATH);
    securec_check_ss_c(rc, "", "");

    /* Generate directory path of pg_xlog on OBS when onlyPath is true */
    if (onlyPath == false) {
        XLByteToSeg(recptr, xlogSegno);
        rc = snprintf_s(xlogfname, MAXFNAMELEN, MAXFNAMELEN - 1, "%08X%08X%08X_%02u", timeLine,
                        (uint32)((xlogSegno) / XLogSegmentsPerXLogId), (uint32)((xlogSegno) % XLogSegmentsPerXLogId),
                        (uint32)((recptr / OBS_XLOG_SLICE_BLOCK_SIZE) & OBS_XLOG_SLICE_NUM_MAX));
        securec_check_ss_c(rc, "", "");
    }
    rc = snprintf_s(xlogfpath, MAXPGPATH, MAXPGPATH - 1, XLOGDIR "/%s", xlogfname);
    securec_check_ss_c(rc, "", "");

    return pstrdup(xlogfpath);
}

static char *path_skip_prefix(char *path)
{
    char *key = path;
    /* Skip path prefix, prefix format:'xxxx/cn/' */
    for (int i = 0; i <= 1; i++) {
        key = strchr(key, '/');
        if (key == NULL) {
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                            errmsg("The xlog file path is invalid")));
        }
        key = key + 1; // Skip character '/'
    }
    return key;
}

static char *get_last_filename_from_list(const List *object_list)
{
    // The list returned from OBS is in lexicographic order.
    ListCell* cell = list_tail(object_list);
    if (cell == NULL || (lfirst(cell) == NULL)) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_FILE),
                        errmsg("The xlog file on obs is not exist")));
    }

    /* Skip path prefix, prefix format:'xxxx/cn/' */
    char *key = path_skip_prefix((char *)lfirst(cell));

    return pstrdup(key);
}

/* 
 * xlog slice name format: {fileNamePrefix}/{timeline}+{LSN/16M/256}+{LSN/16M%256}_{slice}_{term}_{subTerm}
 * samples: obs://{bucket}/xxxx/cn/pg_xlog/000000010000000000000003_08_00000002_00000005
 */
static char *obs_replication_get_last_xlog_slice(XLogRecPtr startPtr, bool onlyPath)
{
    char *fileNamePrefix = NULL;
    char *fileName = NULL;
    List *object_list = NIL;

    fileNamePrefix = obs_replication_get_xlog_prefix(startPtr, onlyPath);

    object_list = obsList(fileNamePrefix);
    if (object_list == NIL || object_list->length <= 0) {
        ereport(LOG, (errmsg("The OBS objects with the prefix %s cannot be found.", fileNamePrefix)));

        pfree(fileNamePrefix);
        return NULL;
    }

    fileName = get_last_filename_from_list(object_list);
    if (fileName == NULL) {
        ereport(LOG, (errmsg("Cannot get xlog file name with prefix:%s, obs list length:%d",
                             fileNamePrefix, object_list->length)));
    }

    pfree(fileNamePrefix);
    list_free_deep(object_list);
    return fileName;
}


/*
 * Read the Xlog file that is calculated using the start LSN and whose name contains the maximum term.
 * Returns the Xlog from the start position to the last.
 */
int obs_replication_receive(XLogRecPtr startPtr, char **buffer, int *bufferLength,
    int timeout_ms, char* inner_buff)
{
    char *fileName = NULL;
    uint32 offset = 0;
    size_t readLen = 0;
    char *xlogBuff = NULL;
    uint32 actualXlogLen = 0;

    errno_t rc = EOK;
    TimestampTz start_time;

    if (buffer == NULL || *buffer == NULL || bufferLength == NULL) {
        ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                        errmsg("Invalid parameter")));
    }

    *bufferLength = 0;

    fileName = obs_replication_get_last_xlog_slice(startPtr, false);
    if (fileName == NULL || strlen(fileName) == 0) {
        ereport(LOG, (errmsg("Cannot find xlog file with LSN: %lu", startPtr)));
        return -1;
    }

    if (inner_buff != NULL) {
        xlogBuff = inner_buff;
    } else {
        xlogBuff = (char*)palloc(OBS_XLOG_SLICE_FILE_SIZE);
        if (xlogBuff == NULL) {
            pfree(fileName);
            return -1;
        }
    }

    rc = memset_s(xlogBuff, OBS_XLOG_SLICE_FILE_SIZE, 0, OBS_XLOG_SLICE_FILE_SIZE);
    securec_check(rc, "", "");

    /* calc begin offset */
    offset = ((uint32)(startPtr % XLogSegSize)) & ((uint32)(OBS_XLOG_SLICE_BLOCK_SIZE - 1));

    /* Start timing */
    start_time = GetCurrentTimestamp();

    do {
        readLen = obsRead(fileName, 0, xlogBuff, OBS_XLOG_SLICE_FILE_SIZE);
        if (readLen < sizeof(int)) {
            ereport(LOG, (errmsg("Cannot get xlog from OBS, object key:%s", fileName)));
            /* retry */
            continue;
        }

        /* Analysis file header to calc the actual file length */
        actualXlogLen = ntohl(*(uint32*)xlogBuff);

        Assert(actualXlogLen + sizeof(int) <= readLen);
        Assert(actualXlogLen <= (int)(OBS_XLOG_SLICE_BLOCK_SIZE));

        if (actualXlogLen > offset && (actualXlogLen + sizeof(int) <= readLen)) {
            *bufferLength = actualXlogLen - offset;
            rc = memcpy_s(*buffer, OBS_XLOG_SLICE_BLOCK_SIZE,
                     xlogBuff + OBS_XLOG_SLICE_HEADER_SIZE + offset,
                     *bufferLength);
            securec_check(rc, "", "");
            break;
        }

        pg_usleep(10 * 1000); // 10ms
    } while (ComputeTimeStamp(start_time) < timeout_ms);

    if (inner_buff == NULL) {
        /* xlogBuff is palloc from this function */
        pfree(xlogBuff);
    }
    pfree(fileName);
    return 0;
}

int obs_replication_archive(const ArchiveXlogMessage *xlogInfo)
{
    errno_t rc = EOK;
    int ret = 0;
    char *fileName = NULL;
    char *fileNamePrefix = NULL;

    int xlogreadfd;
    char xlogfpath[MAXPGPATH];
    char xlogfname[MAXFNAMELEN];

    char *xlogBuff = NULL;
    uint32 actualXlogLen = 0;
    uint offset = 0;

    XLogSegNo xlogSegno = 0;

    xlogBuff = (char *)palloc(OBS_XLOG_SLICE_FILE_SIZE);
    rc = memset_s(xlogBuff, OBS_XLOG_SLICE_FILE_SIZE, 0, OBS_XLOG_SLICE_FILE_SIZE);
    securec_check(rc, "", "");

    /* generate xlog path */
    XLByteToSeg(xlogInfo->targetLsn, xlogSegno);
    if (xlogSegno == InvalidXLogSegPtr) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("Invalid Lsn: %lu", xlogInfo->targetLsn)));
    }
    XLogFileName(xlogfname, DEFAULT_TIMELINE_ID, xlogSegno);

    rc = snprintf_s(xlogfpath, MAXPGPATH, MAXPGPATH - 1, "%s/" XLOGDIR "/%s", t_thrd.proc_cxt.DataDir, xlogfname);
    securec_check_ss(rc, "\0", "\0");

    xlogreadfd = open(xlogfpath, O_RDONLY | PG_BINARY, 0);
    if (xlogreadfd < 0) {
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES),
                        errmsg("Can not open file \"%s\": %s", xlogfpath, strerror(errno))));
    }

    /* Align down to 2M */
    offset = TYPEALIGN_DOWN(OBS_XLOG_SLICE_BLOCK_SIZE, ((xlogInfo->targetLsn) % XLogSegSize));
    if (lseek(xlogreadfd, (off_t)offset, SEEK_SET) < 0) {
        ereport(ERROR, (errcode(ERRCODE_FILE_READ_FAILED),
                        errmsg("Can not locate to offset[%u] of xlog file \"%s\": %s",
                               offset, xlogfpath, strerror(errno))));
    }

    if (read(xlogreadfd, xlogBuff + OBS_XLOG_SLICE_HEADER_SIZE, OBS_XLOG_SLICE_BLOCK_SIZE)
        != OBS_XLOG_SLICE_BLOCK_SIZE) {
        ereport(ERROR, (errcode(ERRCODE_FILE_READ_FAILED),
                        errmsg("Can not read local xlog file \"%s\": %s", xlogfpath, strerror(errno))));
    }

    /* Add xlog slice header for recording the actual xlog length */
    actualXlogLen = (((uint32)((xlogInfo->targetLsn) % XLogSegSize)) & (OBS_XLOG_SLICE_BLOCK_SIZE - 1)) + 1;

    *(uint32*)xlogBuff = htonl(actualXlogLen);

    close(xlogreadfd);

    /* Get xlog slice file path on OBS */
    fileNamePrefix = obs_replication_get_xlog_prefix(xlogInfo->targetLsn, false);
    fileName = (char*)palloc0(MAX_PATH_LEN);

    /* {xlog_name}_{sliece_num}_01(version_num)_00000001{tli}_00000001{subTerm} */
    rc = sprintf_s(fileName, MAX_PATH_LEN, "%s_%02d_%08u_%08u_%08d", fileNamePrefix, 
        CUR_OBS_FILE_VERSION, xlogInfo->term, xlogInfo->tli, xlogInfo->sub_term);
    securec_check_ss(rc, "\0", "\0");

    /* Upload xlog slice file to OBS */
    ret = obsWrite(fileName, xlogBuff, OBS_XLOG_SLICE_FILE_SIZE);

    pfree(xlogBuff);
    pfree(fileNamePrefix);
    pfree(fileName);

    return ret;
}

int obs_replication_cleanup(XLogRecPtr recptr)
{
    char *fileNamePrefix = NULL;
    List *object_list = NIL;
    ListCell *cell = NULL;
    char *key = NULL;

    errno_t rc = EOK;
    int ret = 0;
    char xlogfname[MAXFNAMELEN];
    int maxDelNum = 0;
    size_t len = 0;
    XLogSegNo xlogSegno = 0;

    XLByteToSeg(recptr, xlogSegno);
    rc = snprintf_s(xlogfname, MAXFNAMELEN, MAXFNAMELEN - 1, "%08X%08X%08X_%02u", DEFAULT_TIMELINE_ID,
                    (uint32)((xlogSegno) / XLogSegmentsPerXLogId), (uint32)((xlogSegno) % XLogSegmentsPerXLogId),
                    (uint32)((recptr / OBS_XLOG_SLICE_BLOCK_SIZE) & OBS_XLOG_SLICE_NUM_MAX));
    securec_check_ss_c(rc, "", "");
    len = strlen(xlogfname);

    fileNamePrefix = obs_replication_get_xlog_prefix(recptr, true);

    object_list = obsList(fileNamePrefix);
    if (object_list == NIL || object_list->length <= 0) {
        ereport(LOG, (errmsg("The OBS objects with the prefix %s cannot be found.", fileNamePrefix)));

        pfree(fileNamePrefix);
        return -1;
    }

    /* At least 100 GB-OBS_XLOG_SAVED_FILES_NUM log files must be retained on obs. */
    if (object_list->length <= OBS_XLOG_SAVED_FILES_NUM) {
        return 0;
    } else {
        maxDelNum = object_list->length - OBS_XLOG_SAVED_FILES_NUM;
    }

    foreach (cell, object_list) {
        key = path_skip_prefix((char *)lfirst(cell));
        if (key == NULL) {
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("Invalid OBS object key: %s", (char *)lfirst(cell))));
        }

        if (strncmp(basename(key), xlogfname, len) < 0) {
            /* Ahead of the target lsn, need to delete */
            ret = obsDelete(key);
            if (ret != 0) {
                ereport(WARNING, (errcode(ERRCODE_UNDEFINED_FILE),
                               errmsg("The OBS objects delete fail, ret=%d, key=%s", ret, key)));
            } else {
                /* The number of files to be deleted has reached the maximum. */
                if ((maxDelNum--) <= 0) {
                    break;
                }
            }
        } else {
            /* Reach the target lsn */
            break;
        }
    }

    /* release result list */
    list_free_deep(object_list);
    object_list = NIL;
    pfree(fileNamePrefix);

    return 0;
}

int obs_replication_get_last_xlog(ArchiveXlogMessage *xlogInfo)
{
    char *filePath = NULL;
    char *fileBaseName = NULL;
    errno_t rc = EOK;
    TimeLineID timeLine;
    int xlogSegId;
    int xlogSegOffset;
    int version = 0;
    if (xlogInfo == NULL) {
        return -1;
    }

    filePath = obs_replication_get_last_xlog_slice(0, true);
    if (filePath == NULL) {
        ereport(LOG, (errmsg("Cannot find xlog file on OBS")));
        return -1;
    }

    fileBaseName = basename(filePath);
    ereport(DEBUG1, (errmsg("The last xlog on OBS: %s", filePath)));

    rc = sscanf_s(fileBaseName, "%8X%8X%8X_%2u_%02d_%08u_%08u_%08d", &timeLine, &xlogSegId,
                  &xlogSegOffset, &xlogInfo->slice, &version, &xlogInfo->term, &xlogInfo->tli, &xlogInfo->sub_term);
    securec_check_for_sscanf_s(rc, 6, "\0", "\0");

    ereport(DEBUG1, (errmsg("Parse xlog filename is %8X%8X%8X_%2u_%02d_%08u_%08u_%08d", timeLine, xlogSegId,
                            xlogSegOffset, xlogInfo->slice, version, xlogInfo->term, xlogInfo->tli, xlogInfo->sub_term)));

    XLogSegNoOffsetToRecPtr(xlogSegId * XLogSegmentsPerXLogId + xlogSegOffset, 0, xlogInfo->targetLsn);

    pfree(filePath);
    return 0;
}
