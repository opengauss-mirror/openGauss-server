/*
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2010-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 * archive_walreceiver.cpp
 * 
 * Description: This file contains the obs-specific parts of walreceiver. It's
 * loaded as a dynamic module to avoid linking the main server binary with
 * obs.
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/replication/archive_walreceiver.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include <sys/time.h>

#include "libpq/libpq-int.h"
#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "nodes/pg_list.h"
#include "access/archive/archive_am.h"
#include "access/archive/nas_am.h"
#include "access/obs/obs_am.h"
#include "utils/timestamp.h"
#include "miscadmin.h"
#include "replication/walreceiver.h"
#include "replication/archive_walreceiver.h"
#include "replication/slot.h"
#include "storage/pmsignal.h"
#include "storage/proc.h"
#include "utils/guc.h"
#include "pgxc/pgxc.h"
#define CUR_OBS_FILE_VERSION 1

static char *path_skip_prefix(char *path);
static bool GetOBSArchiveLastStartTime(ArchiveSlotConfig* obsArchiveSlot);

static bool IsArchiveXlogBeyondRequest(XLogRecPtr startPtr, const List *object_list)
{
    char* fileName = NULL;
    char* xlogFileName = NULL;
    char* tempToken = NULL;
    uint32 xlogReadLogid = -1;
    uint32 xlogReadLogSeg = -1;
    TimeLineID tli = 0;
    uint32 startSeg;
    ListCell* cell = NULL;

    if (object_list == NIL || object_list->head->next == NULL) {
        ereport(ERROR, (errmsg("there is no xlog file on archive server.")));
        return false;
    }
    cell = list_head(object_list)->next;
    fileName = (char*)lfirst(cell);
    fileName = strrchr(fileName, '/');
    fileName = fileName + 1;
    xlogFileName = strtok_s(fileName, "_", &tempToken);
    if (xlogFileName == NULL) {
        ereport(ERROR, (errmsg("Failed get xlog file name from fileName %s.", fileName)));
    }
    if (sscanf_s(xlogFileName, "%08X%08X%08X", &tli, &xlogReadLogid, &xlogReadLogSeg) != 3) {
        ereport(ERROR, (errmsg("failed to translate name to xlog: %s\n", xlogFileName)));
    }
    XLByteToSeg(startPtr, startSeg);
    if ((startSeg / XLogSegmentsPerXLogId) < xlogReadLogid ||
                ((startSeg / XLogSegmentsPerXLogId) == xlogReadLogid &&
                (startSeg % XLogSegmentsPerXLogId) < xlogReadLogSeg)) {
        ereport(LOG, (errmsg("the xlog file on archive server is newer than local request, need build.\n")));
        return true;
    }
    return false;
}

bool ReadArchiveReplicationFile(const char* fileName, char *buffer, const int length, ArchiveConfig *archive_config)
{
    List *object_list = NIL;
    int ret = 0;
    char file_path[MAXPGPATH] = {0};
    size_t readLen = 0;
    if (archive_config->media_type == ARCHIVE_OBS) {
        object_list = obsList(fileName, archive_config, false);
        if (object_list == NIL || object_list->length <= 0) {
            ereport(LOG, (errmsg("The file named %s cannot be found.", fileName)));
            return false;
        }

        readLen = obsRead(fileName, 0, buffer, length, archive_config);
        if (readLen == 0) {
            ereport(LOG, (errmsg("Cannot read  content in %s file!", fileName)));
            return false;
        }
    } else if (archive_config->media_type == ARCHIVE_NAS) {
        ret = snprintf_s(file_path, MAXPGPATH, MAXPGPATH - 1, "%s/%s",
            archive_config->archive_prefix, fileName);
        securec_check_ss(ret, "\0", "\0");
        if (!file_exists(file_path)) {
            ereport(LOG, (errmsg("The file named %s cannot be found.", file_path)));
            return false;
        }

        readLen = NasRead(fileName, 0, buffer, length, archive_config);
        if (readLen == 0) {
            ereport(LOG, (errmsg("Cannot read  content in %s file!", fileName)));
            return false;
        }
    } else {
        return false;
    }
    return true;
}


bool ArchiveReplicationReadFile(const char* fileName, char* content, int contentLen, const char *slotName)
{
    errno_t rc = 0;
    ArchiveConfig archive_config_tmp;
    ArchiveConfig *archive_config = NULL;
    char pathPrefix[MAXPGPATH] = {0};
    ArchiveSlotConfig *archiveSlot = NULL;
    if (slotName != NULL) {
        archiveSlot = getArchiveReplicationSlotWithName(slotName);
        if (archiveSlot == NULL) {
            ereport(LOG, (errmsg("Cannot get archive config from replication slots")));
            return false;
        }
        archive_config = &archiveSlot->archive_config;
    } else {
        archive_config = getArchiveConfig();
        if (archive_config == NULL) {
            ereport(LOG, (errmsg("Cannot get archive config from replication slots")));
            return false;
        }
    }

    /* copy archive configs to temporary variable for customising file path */
    rc = memcpy_s(&archive_config_tmp, sizeof(ArchiveConfig), archive_config, sizeof(ArchiveConfig));
    securec_check(rc, "", "");

    if (!IS_PGXC_COORDINATOR) {
        rc = strcpy_s(pathPrefix, MAXPGPATH, archive_config_tmp.archive_prefix);
        securec_check_c(rc, "\0", "\0");

        char *p = strrchr(pathPrefix, '/');
        if (p == NULL) {
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("archive path prefix is invalid")));
        }
        *p = '\0';
        archive_config_tmp.archive_prefix = pathPrefix;
    }

    return ReadArchiveReplicationFile(fileName, content, contentLen, &archive_config_tmp);
}

void update_stop_barrier()
{
    errno_t rc = EOK;
    volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;

    if (IS_DISASTER_RECOVER_MODE && (t_thrd.xlog_cxt.recoveryTarget != RECOVERY_TARGET_TIME_OBS)) {
        bool hasFailoverBarrier = false;
        bool hasSwitchoverBarrier = false;
        char failoverBarrier[MAX_BARRIER_ID_LENGTH] = {0};
        char switchoverBarrier[MAX_BARRIER_ID_LENGTH] = {0};

        // The failover and switchover procedures cannot coexist.
        hasFailoverBarrier = ArchiveReplicationReadFile(HADR_FAILOVER_BARRIER_ID_FILE, 
                                                        (char *)failoverBarrier, MAX_BARRIER_ID_LENGTH);
        hasSwitchoverBarrier = ArchiveReplicationReadFile(HADR_SWITCHOVER_BARRIER_ID_FILE, 
                                                        (char *)switchoverBarrier, MAX_BARRIER_ID_LENGTH);
        if (hasFailoverBarrier == true && hasSwitchoverBarrier == true) {
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                             errmsg("The failover and switchover procedures cannot coexist."
                                    "failover barrierID %s, switchover barrierID %s.",
                                     (char *)failoverBarrier, (char *)switchoverBarrier)));
        }

        if (hasFailoverBarrier == true) {
            rc = strncpy_s((char *)walrcv->recoveryStopBarrierId, MAX_BARRIER_ID_LENGTH,
                           (char *)failoverBarrier, MAX_BARRIER_ID_LENGTH - 1);
            securec_check(rc, "\0", "\0");
            ereport(LOG, (errmsg("Get failover barrierID %s", (char *)walrcv->recoveryStopBarrierId)));
        }

        if (hasSwitchoverBarrier == true) {
            rc = strncpy_s((char *)walrcv->recoverySwitchoverBarrierId, MAX_BARRIER_ID_LENGTH,
                           (char *)switchoverBarrier, MAX_BARRIER_ID_LENGTH - 1);
            securec_check(rc, "\0", "\0");
            ereport(LOG, (errmsg("Get switchover barrierID %s", (char *)walrcv->recoverySwitchoverBarrierId)));
        }
    } else if (IS_SHARED_STORAGE_STANBY_CLUSTER_MODE) {
        if (strlen(g_instance.stopBarrierId) != 0) {
            rc = strncpy_s((char *)walrcv->recoveryStopBarrierId, MAX_BARRIER_ID_LENGTH,
                           (char *)g_instance.stopBarrierId, MAX_BARRIER_ID_LENGTH - 1);
            securec_check(rc, "\0", "\0");
            ereport(LOG, (errmsg("Get stop barrierID %s", (char *)walrcv->recoveryStopBarrierId)));
        }
    }
}

void update_recovery_barrier()
{
    errno_t rc = EOK;
    /* use volatile pointer to prevent code rearrangement */
    volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;
    char barrier[MAX_BARRIER_ID_LENGTH] = {0};
    if (ArchiveReplicationReadFile(HADR_BARRIER_ID_FILE, (char *)barrier, MAX_BARRIER_ID_LENGTH)) {
        if (strcmp((char *)barrier, (char *)walrcv->recoveryTargetBarrierId) < 0) {
                ereport(ERROR, (errmodule(MOD_REDO), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("The new global barrier is smaller than the last one.")));
        } else {
            rc = strncpy_s((char *)walrcv->recoveryTargetBarrierId, MAX_BARRIER_ID_LENGTH,
                       (char *)barrier, MAX_BARRIER_ID_LENGTH - 1);
            securec_check(rc, "\0", "\0");
        }
    }
}

bool archive_connect(char* conninfo, XLogRecPtr* startpoint, char* slotname, int channel_identifier)
{
    volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;
    walrcv->archive_slot = GetArchiveRecoverySlot();
    if (walrcv->archive_slot == NULL) {
        ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                        errmsg("[walreceiver_connect_obs]could not get archive relication slot")));
        return false;
    }

    walrcv->peer_role = PRIMARY_MODE;
    walrcv->peer_state = NORMAL_STATE;
    walrcv->isFirstTimeAccessStorage = true;
    if (t_thrd.libwalreceiver_cxt.recvBuf != NULL) {
        PQfreemem(t_thrd.libwalreceiver_cxt.recvBuf);
    }
    t_thrd.libwalreceiver_cxt.recvBuf = NULL;
    uint32 totalLen = sizeof(WalDataMessageHeader) + OBS_XLOG_SLICE_BLOCK_SIZE + 1;
    t_thrd.libwalreceiver_cxt.recvBuf = (char*)malloc(totalLen);
    if (t_thrd.libwalreceiver_cxt.recvBuf == NULL) {
        ereport(LOG, (errmsg("archive_receive:Receive Buffer out of memory.\n")));
        return false;
    }

    /* HADR only support OBS currently */
    if (walrcv->archive_slot->archive_config.media_type != ARCHIVE_OBS) {
        return true;
    }

    /* The full recovery of disaster recovery scenarios has ended */
    g_instance.roach_cxt.isRoachRestore = false;

    /* Use OBS to complete DR and set the original replication link status to normal. */
    volatile HaShmemData *hashmdata = t_thrd.postmaster_cxt.HaShmData;
    char standbyClusterStat[MAX_DEFAULT_LENGTH] = {0};
    ArchiveReplicationReadFile(HADR_STANDBY_CLUSTER_STAT_FILE, standbyClusterStat, MAX_DEFAULT_LENGTH);

    if (strncmp(standbyClusterStat, HADR_IN_NORMAL, strlen(HADR_IN_NORMAL)) == 0) {
        ereport(WARNING, (errmsg("===obs_connect===\n "
            "The cluster DR relationship has been removed, "
            "but the instance slot still exists. slot name is %s", walrcv->archive_slot->slotname)));
        ReplicationSlotDrop(walrcv->archive_slot->slotname);
        SpinLockAcquire(&hashmdata->mutex);
        hashmdata->repl_reason[hashmdata->current_repl] = WALSEGMENT_REBUILD;
        SpinLockRelease(&hashmdata->mutex);

        SpinLockAcquire(&walrcv->mutex);
        walrcv->conn_errno = REPL_INFO_ERROR;
        SpinLockRelease(&walrcv->mutex);
    } else {
        SpinLockAcquire(&hashmdata->mutex);
        hashmdata->repl_reason[hashmdata->current_repl] = NONE_REBUILD;
        SpinLockRelease(&hashmdata->mutex);

        SpinLockAcquire(&walrcv->mutex);
        walrcv->conn_errno = NONE_ERROR;
        walrcv->node_state = NODESTATE_NORMAL;
        SpinLockRelease(&walrcv->mutex);
    }

    /* Only postmaster can update gaussdb.state file */
    SendPostmasterSignal(PMSIGNAL_UPDATE_HAREBUILD_REASON);
    return true;
}

bool archive_receive(int timeout, unsigned char* type, char** buffer, int* len)
{
    int dataLength = 0;
    XLogRecPtr startPtr;
    char* recvBuf = t_thrd.libwalreceiver_cxt.recvBuf;
    errno_t rc = EOK;
    volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;
    XLogRecPtr lastReplayPtr;

    // The start LSN used for the first access to OBS
    // is the same as that of the streaming replication function.
    if (walrcv->isFirstTimeAccessStorage) {
        startPtr = walrcv->receiveStart;
        walrcv->isFirstTimeAccessStorage = false;
    } else {
        // t_thrd.walreceiver_cxt.walRcvCtlBlock->receivePtr will been updated in XLogWalRcvReceive()
        SpinLockAcquire(&t_thrd.walreceiver_cxt.walRcvCtlBlock->mutex);
        startPtr = t_thrd.walreceiver_cxt.walRcvCtlBlock->receivePtr;
        SpinLockRelease(&t_thrd.walreceiver_cxt.walRcvCtlBlock->mutex);
    }
    /* the unit of max_size_for_xlog_receiver is KB */
    uint64 maxRequestSize = ((uint64)g_instance.attr.attr_storage.max_size_for_xlog_receiver << 10);
    lastReplayPtr = GetXLogReplayRecPtr(NULL);
    if ((startPtr > lastReplayPtr) && (startPtr - lastReplayPtr >= maxRequestSize)) {
        ereport(WARNING, (errmsg("The xlog local requested %08X/%08X is beyond local max xlog size, stop requested",
            (uint32)(startPtr >> 32), (uint32)startPtr)));
        pg_usleep(timeout * 1000);
        return false;
    }

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
    // copy xlog from archive server
    char* dataLocation = recvBuf + headLen;
    (void)archive_replication_receive(startPtr, &dataLocation, &dataLength, timeout, NULL);
    if (dataLength <= 0) {
        return false;
    }

    int validLen = headLen + dataLength;
    recvBuf[validLen] = '\0'; /* Add terminating null */

    elog(LOG,"[archive_receive]get xlog startlsn %08X/%08X, len %X\n",
        (uint32)(startPtr >> 32), (uint32)startPtr, (uint32)validLen);

    /* Return received messages to caller */
    *type = 'w';
    *buffer = recvBuf;
    *len = validLen;
    return true;
}


void archive_send(const char *buffer, int nbytes)
{
}

void archive_disconnect(void)
{
    if (t_thrd.libwalreceiver_cxt.recvBuf != NULL) {
        PQfreemem(t_thrd.libwalreceiver_cxt.recvBuf);
    }
    t_thrd.libwalreceiver_cxt.recvBuf = NULL;

    return;
}

static char *archive_replication_get_xlog_prefix(XLogRecPtr recptr, bool onlyPath)
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
    if (IS_PGXC_COORDINATOR) {
        if (IS_CNDISASTER_RECOVER_MODE) {
            if (get_local_key_cn() == NULL) {
                ereport(ERROR, (errcode(ERRCODE_UNDEFINED_FILE), errmsg("There is no hadr_key_cn")));
                return NULL;
            }
            rc = snprintf_s(xlogfpath, MAXPGPATH, MAXPGPATH - 1, "%s/%s/%s",
                get_local_key_cn(), XLOGDIR, xlogfname);
            securec_check_ss_c(rc, "", "");
        } else {
            rc = snprintf_s(xlogfpath, MAXPGPATH, MAXPGPATH - 1, "%s/%s/%s",
                g_instance.attr.attr_common.PGXCNodeName, XLOGDIR, xlogfname);
            securec_check_ss_c(rc, "", "");
        }
    } else {
        rc = snprintf_s(xlogfpath, MAXPGPATH, MAXPGPATH - 1, XLOGDIR "/%s", xlogfname);
        securec_check_ss_c(rc, "", "");
    }

    return pstrdup(xlogfpath);
}

static char *path_skip_prefix(char *path)
{
    char *key = path;
    /* Skip path prefix, prefix format:'xxxx/cn/' */
    key = strrchr(key, '/');
    if (key == NULL) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                            errmsg("The xlog file path is invalid")));
    }
    key = key + 1; // Skip character '/'
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
    char *key = strstr((char *)lfirst(cell), XLOGDIR);

    return pstrdup(key);
}

/*
 * xlog slice name format: {fileNamePrefix}/{timeline}+{LSN/16M/256}+{LSN/16M%256}_{slice}_{term}_{subTerm}
 * samples: obs://{bucket}/xxxx/cn/pg_xlog/000000010000000000000003_08_00000002_00000005
 */
static char *archive_replication_get_last_xlog_slice(XLogRecPtr startPtr, bool onlyPath,
    bool needUpdateDBState, ArchiveConfig* archive_obs)
{
    char *fileNamePrefix = NULL;
    char *fileName = NULL;
    List *object_list = NIL;
    List *obsXlogList = NIL;
    char xlogfpath[MAXPGPATH];
    errno_t rc = EOK;

    fileNamePrefix = archive_replication_get_xlog_prefix(startPtr, onlyPath);

    if (IS_CNDISASTER_RECOVER_MODE) {
        if (get_local_key_cn() == NULL) {
            ereport(ERROR, (errcode(ERRCODE_UNDEFINED_FILE), errmsg("There is no hadr_key_cn")));
            return NULL;
        }
        rc = snprintf_s(xlogfpath, MAXPGPATH, MAXPGPATH - 1, "%s/%s", get_local_key_cn(), XLOGDIR);
        securec_check_ss_c(rc, "", "");
    } else {
        rc = snprintf_s(xlogfpath, MAXPGPATH, MAXPGPATH - 1, "%s", XLOGDIR);
        securec_check_ss_c(rc, "", "");
    }

    if (needUpdateDBState) {
        obsXlogList = ArchiveList(xlogfpath, archive_obs);
        if (IsArchiveXlogBeyondRequest(startPtr, obsXlogList)) {
            SetObsRebuildReason(WALSEGMENT_REBUILD);
            ereport(ERROR, (errcode(ERRCODE_INVALID_STATUS),
                errmsg("standby's local request lsn[%X/%X] mismatched with remote server",
                (uint32)(startPtr >> 32), (uint32)startPtr)));
        }
        list_free_deep(obsXlogList);
        obsXlogList = NIL;
    }

    object_list = ArchiveList(fileNamePrefix, archive_obs);
    if (object_list == NIL || object_list->length <= 0) {
        ereport(LOG, (errmsg("The OBS objects with the prefix %s cannot be found.", fileNamePrefix)));
        pfree(fileNamePrefix);
        return NULL;
    }

    if (IS_CNDISASTER_RECOVER_MODE) {
        char tmpFileName[MAXPGPATH];
        rc = snprintf_s(tmpFileName, MAXPGPATH, MAXPGPATH - 1, "%s/%s", get_local_key_cn(),
            get_last_filename_from_list(object_list));
        securec_check_ss_c(rc, "", "");
        fileName = pstrdup(tmpFileName);
    } else {
        fileName = get_last_filename_from_list(object_list);
    }
    if (fileName == NULL) {
        ereport(LOG, (errmsg("Cannot get xlog file name with prefix:%s, obs list length:%d",
                             fileNamePrefix, object_list->length)));
    }

    pfree(fileNamePrefix);
    list_free_deep(object_list);
    object_list = NIL;
    return fileName;
}


/*
 * Read the Xlog file that is calculated using the start LSN and whose name contains the maximum term.
 * Returns the Xlog from the start position to the last.
 */
int archive_replication_receive(XLogRecPtr startPtr, char **buffer, int *bufferLength,
    int timeout_ms, char* inner_buff)
{
    char *fileName = NULL;
    uint32 offset = 0;
    size_t readLen = 0;
    char *xlogBuff = NULL;
    uint32 actualXlogLen = 0;
    volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;
    errno_t rc = EOK;
    TimestampTz start_time;

    if (buffer == NULL || *buffer == NULL || bufferLength == NULL) {
        ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                        errmsg("Invalid parameter")));
    }

    *bufferLength = 0;

    fileName = archive_replication_get_last_xlog_slice(startPtr, false, true, &walrcv->archive_slot->archive_config);
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
        readLen = ArchiveRead(fileName, 0, xlogBuff, OBS_XLOG_SLICE_FILE_SIZE, &walrcv->archive_slot->archive_config);
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

int ArchiveReplicationAchiver(const ArchiveXlogMessage *xlogInfo)
{
    errno_t rc = EOK;
    int ret = 0;
    char *fileName = NULL;
    char *fileNamePrefix = NULL;

    int xlogreadfd = -1;
    char xlogfpath[MAXPGPATH];
    char xlogfname[MAXFNAMELEN];

    char *xlogBuff = NULL;
    uint32 actualXlogLen = 0;
    uint offset = 0;

    XLogSegNo xlogSegno = 0;
    ArchiveSlotConfig *archive_slot = NULL;
    archive_slot = getArchiveReplicationSlot();

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

    canonicalize_path(xlogfpath);
    xlogreadfd = open(xlogfpath, O_RDONLY | PG_BINARY, 0);
    if (xlogreadfd < 0) {
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES),
                        errmsg("Can not open file \"%s\": %s", xlogfpath, strerror(errno))));
    }

    /* Align down to 4M */
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

    /* Get xlog slice file path on archive server */
    fileNamePrefix = archive_replication_get_xlog_prefix(xlogInfo->targetLsn, false);
    fileName = (char*)palloc0(MAX_PATH_LEN);

    /* {xlog_name}_{sliece_num}_01(version_num)_00000001{tli}_00000001{subTerm} */
    rc = sprintf_s(fileName, MAX_PATH_LEN, "%s_%02d_%08u_%08u_%08d", fileNamePrefix,
        CUR_OBS_FILE_VERSION, xlogInfo->term, xlogInfo->tli, xlogInfo->sub_term);
    securec_check_ss(rc, "\0", "\0");

    /* Upload xlog slice file to OBS */
    ret = ArchiveWrite(fileName, xlogBuff, OBS_XLOG_SLICE_FILE_SIZE, &archive_slot->archive_config);

    pfree(xlogBuff);
    pfree(fileNamePrefix);
    pfree(fileName);

    return ret;
}

static bool GetOBSArchiveLastStartTime(ArchiveSlotConfig* obsArchiveSlot)
{
    List *objectList = NIL;
    int readLen = 0;
    char buffer[MAXPGPATH] = {0};
    char* lastStartTime;
    char* tempToken = NULL;

    if (!IS_PGXC_COORDINATOR) {
        objectList = ArchiveList(OBS_ARCHIVE_STATUS_FILE, &obsArchiveSlot->archive_config);
        if (objectList == NIL || objectList->length <= 0) {
            ereport(LOG, (errcode(ERRCODE_NO_DATA_FOUND),
                (errmsg("The archive status file could not been found on OBS. Update start time with local time."))));
            return false;
        }
        readLen = ArchiveRead(OBS_ARCHIVE_STATUS_FILE, 0, buffer, MAXPGPATH, &obsArchiveSlot->archive_config);
        if (readLen == 0) {
            ereport(LOG, (errcode(ERRCODE_NO_DATA_FOUND),
                (errmsg("Cannot read OBS archive status file! Update start time with local time."))));
            list_free_deep(objectList);
            objectList = NIL;
            return false;
        }
        buffer[MAXPGPATH - 1] = '\0';
        lastStartTime = strtok_s(buffer, "-", &tempToken);
        if (lastStartTime == NULL) {
            ereport(LOG, (errcode(ERRCODE_NO_DATA_FOUND),
                (errmsg("Get first update start failed when reading status file."))));
            list_free_deep(objectList);
            objectList = NIL;
            return false;
        }
#ifdef HAVE_INT64_TIMESTAMP
        t_thrd.arch.arch_start_timestamp = atol(lastStartTime);
#else
        t_thrd.arch.arch_start_timestamp = atof(lastStartTime);
#endif
        list_free_deep(objectList);
        objectList = NIL;
        return true;
    }
    return false;
}

void update_archive_start_end_location_file(XLogRecPtr endPtr, long endTime)
{
    StringInfoData buffer;
    XLogRecPtr locStartPtr;
    char* fileName = NULL;
    char* obsfileName = NULL;
    char preFileName[MAXPGPATH] = {0};
    char* xlogFileName = NULL;
    char* tempToken = NULL;
    uint32 xlogReadLogid = -1;
    uint32 xlogReadLogSeg = -1;
    TimeLineID tli = 0;
    List *obsXlogList = NIL;
    ListCell* cell = NULL;
    errno_t rc = EOK;

    ArchiveSlotConfig* obs_archive_slot = getArchiveReplicationSlot();
    if (obs_archive_slot == NULL) {
        return;
    }

    if (!IS_PGXC_COORDINATOR) {
        if (t_thrd.arch.arch_start_timestamp == 0 && !GetOBSArchiveLastStartTime(obs_archive_slot)) {
            t_thrd.arch.arch_start_timestamp = endTime;
        }
        initStringInfo(&buffer);
        obsXlogList = ArchiveList(XLOGDIR, &obs_archive_slot->archive_config);
        if (obsXlogList == NIL || obsXlogList->length <= 0) {
            return;
        }
        cell = list_head(obsXlogList);
        fileName = (char*)lfirst(cell);
        obsfileName = strrchr(fileName, '/');
        rc = memcpy_s(preFileName, MAXPGPATH, fileName, strlen(fileName) - strlen(obsfileName));
        securec_check(rc, "", "");
        obsfileName = obsfileName + 1;
        tempToken = NULL;
        xlogFileName = strtok_s(obsfileName, "_", &tempToken);
        if (xlogFileName == NULL) {
            ereport(ERROR, (errmsg("Failed get xlog file name from obsfileName %s.", obsfileName)));
        }
        if (sscanf_s(xlogFileName, "%08X%08X%08X", &tli, &xlogReadLogid, &xlogReadLogSeg) != 3) {
            ereport(ERROR, (errmsg("failed to translate name to xlog: %s\n", xlogFileName)));
        }
        XLogSegNoOffsetToRecPtr(xlogReadLogid * XLogSegmentsPerXLogId + xlogReadLogSeg, 0, locStartPtr);
        appendStringInfo(&buffer, "%ld-%ld_%lu-%lu_00000001_%s\n", t_thrd.arch.arch_start_timestamp, endTime,
            locStartPtr, endPtr, preFileName);

        ArchiveWrite(OBS_ARCHIVE_STATUS_FILE, buffer.data, buffer.len, &obs_archive_slot->archive_config);
        pfree(buffer.data);
        /* release result list */
        list_free_deep(obsXlogList);
        obsXlogList = NIL;
    }
}

int archive_replication_cleanup(XLogRecPtr recptr, ArchiveConfig *archive_config)
{
    char *fileNamePrefix = NULL;
    List *object_list = NIL;
    ListCell *cell = NULL;
    char *key = NULL;

    errno_t rc = EOK;
    int ret = 0;
    char xlogfname[MAXFNAMELEN];
    char obsXlogPath[MAXPGPATH] = {0};
    int maxDelNum = 0;
    size_t len = 0;
    XLogSegNo xlogSegno = 0;
    volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;

    XLByteToSeg(recptr, xlogSegno);
    rc = snprintf_s(xlogfname, MAXFNAMELEN, MAXFNAMELEN - 1, "%08X%08X%08X_%02u", DEFAULT_TIMELINE_ID,
                    (uint32)((xlogSegno) / XLogSegmentsPerXLogId), (uint32)((xlogSegno) % XLogSegmentsPerXLogId),
                    (uint32)((recptr / OBS_XLOG_SLICE_BLOCK_SIZE) & OBS_XLOG_SLICE_NUM_MAX));
    securec_check_ss_c(rc, "", "");
    len = strlen(xlogfname);

    fileNamePrefix = archive_replication_get_xlog_prefix(recptr, true);

    ereport(LOG, (errmsg("The OBS objects with the prefix %s ", fileNamePrefix)));
    if (archive_config == NULL) {
        object_list = ArchiveList(fileNamePrefix, &walrcv->archive_slot->archive_config);
    } else {
        object_list = ArchiveList(fileNamePrefix, archive_config);
    }

    if (object_list == NIL || object_list->length <= 0) {
        ereport(LOG, (errmsg("The OBS objects with the prefix %s cannot be found.", fileNamePrefix)));

        pfree(fileNamePrefix);
        return -1;
    }

    /* At least 100 GB-OBS_XLOG_SAVED_FILES_NUM log files must be retained on obs. */
    if (archive_config == NULL) {
        if (object_list->length <= OBS_XLOG_SAVED_FILES_NUM) {
            ereport(LOG, (errmsg("[archive_replication_cleanup]Archive logs do not need to be deleted.")));
            return 0;
        } else {
            maxDelNum = object_list->length - OBS_XLOG_SAVED_FILES_NUM;
            ereport(LOG, (errmsg("[archive_replication_cleanup]Delete archive xlog before %s,"
                "number of deleted files is %d", xlogfname, maxDelNum)));
        }
    }

    foreach (cell, object_list) {
        key = path_skip_prefix((char *)lfirst(cell));
        if (key == NULL) {
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("Invalid OBS object key: %s", (char *)lfirst(cell))));
        }

        if (strncmp(basename(key), xlogfname, len) < 0) {
            /* Ahead of the target lsn, need to delete */
            rc = snprintf_s(obsXlogPath, MAXPGPATH, MAXPGPATH - 1, "%s/%s", XLOGDIR, key);
            securec_check_ss_c(rc, "", "");
            if (archive_config == NULL) {
                ret = ArchiveDelete(obsXlogPath, &walrcv->archive_slot->archive_config);
            } else {
                ret = ArchiveDelete(obsXlogPath, archive_config);
            }
            if (ret != 0) {
                ereport(WARNING, (errcode(ERRCODE_UNDEFINED_FILE),
                               errmsg("The OBS objects delete fail, ret=%d, key=%s", ret, key)));
            } else {
                /* The number of files to be deleted has reached the maximum. */
                if ((maxDelNum--) <= 0 && archive_config == NULL) {
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

int archive_replication_get_last_xlog(ArchiveXlogMessage *xlogInfo, ArchiveConfig* archive_obs)
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

    filePath = archive_replication_get_last_xlog_slice(0, true, false, archive_obs);
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

static void check_danger_character(const char *inputEnvValue)
{
    if (inputEnvValue == NULL) {
        return;
    }

    const char *dangerCharacterList[] = { ";", "`", "\\", "'", "\"", ">", "<", "&", "|", "!", NULL };
    int i = 0;

    for (i = 0; dangerCharacterList[i] != NULL; i++) {
        if (strstr(inputEnvValue, dangerCharacterList[i]) != NULL) {
            ereport(ERROR, (errmsg("Failed to check input value: invalid token \"%s\".\n", dangerCharacterList[i])));
        }
    }
}

char* get_local_key_cn(void)
{
    int ret = 0;
    int fd = -1;
    char* gausshome = NULL;
    char key_cn_file[MAXPGPATH] = {0};
    char key_cn[MAXFNAMELEN] = {0};

    gausshome = getGaussHome();
    if (gausshome == NULL) {
        ereport(ERROR, (errmsg("Failed get gausshome")));
        return NULL;
    }
    ret = snprintf_s(key_cn_file, MAXPGPATH, MAXPGPATH - 1, "%s/bin/hadr_key_cn", gausshome);
    securec_check_ss(ret, "\0", "\0");

    if (!file_exists(key_cn_file)) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_FILE), errmsg("There is no hadr_key_cn")));
        return NULL;
    } else {
        canonicalize_path(key_cn_file);
        fd = open(key_cn_file, O_RDONLY | PG_BINARY, 0);
        if (fd < 0)
            ereport(ERROR, (errcode_for_file_access(), errmsg("could not open file \"%s\"", key_cn_file)));
        off_t size = lseek(fd, 0, SEEK_END);
        if (size == -1 || size > MAXFNAMELEN - 1) {
            close(fd);
            ereport(ERROR, (errcode(ERRCODE_FILE_READ_FAILED), errmsg("Failed to read local hadr_key_cn")));
            return NULL;
        }
        (void)lseek(fd, 0, SEEK_SET);

        ret = read(fd, &key_cn, size);
        if (ret != size) {
            (void)close(fd);
            ereport(ERROR, (errcode(ERRCODE_NO_DATA_FOUND),
                        (errmsg("The file name hadr_key_cn cannot be read now."))));
            return NULL;
        }
        (void)close(fd);
        key_cn[size] = '\0';
        check_danger_character(key_cn);
        return pstrdup(key_cn);
    }
}
