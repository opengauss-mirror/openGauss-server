/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * ---------------------------------------------------------------------------------------
 *
 *  dcf_callbackfuncs.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/replication/dcf/dcf_callbackfuncs.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "c.h"
#include "postgres.h"
#include "knl/knl_variable.h"
#include "postmaster/postmaster.h"
#include "postmaster/startup.h"
#include "access/xlog.h"
#include "replication/dcf_data.h"
#include "replication/dcf_flowcontrol.h"
#include "replication/dcf_replication.h"
#include "replication/walreceiver.h"
#include "replication/syncrep.h"
#include "access/multi_redo_api.h"
#include "storage/pmsignal.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "utils/memutils.h"
#include "utils/guc.h"
#include "utils/timestamp.h"
#include "storage/copydir.h"
#include "cjson/cJSON.h"

#ifndef ENABLE_MULTIPLE_NODES

#ifdef ENABLE_UT
#define static
#endif

bool SyncConfigFile(unsigned int* follower_id = nullptr);

/*
 * Assert DcfContextShmemInit has been called by DN thread.
 * Call DcfCallBackThreadShmemInit to assign PGPROC to DCF call back thread.
 */
void DcfCallBackThreadShmemInit(DcfCallBackType dcfCbType)
{
    if (t_thrd.dcf_cxt.isDcfShmemInited) {
        return;
    }
    IsUnderPostmaster = true;
    EarlyBindingTLSVariables();
    MemoryContextInit();
    knl_thread_init(DCF_WORKER);
    /* Tell InitProcess, I'm dcf call back thread. */
    t_thrd.dcf_cxt.is_dcf_thread = true;
    t_thrd.fake_session = create_session_context(t_thrd.top_mem_cxt, 0);
    t_thrd.fake_session->status = KNL_SESS_FAKE;
    u_sess = t_thrd.fake_session;
    t_thrd.proc_cxt.MyProcPid = gs_thread_self();
    
    switch (dcfCbType) {
        case NON_CALLBACK:
            t_thrd.proc_cxt.MyProgName = "DcfNonCallbackWorker";
            break;
        case CONSENSUS_CALLBACK:
            t_thrd.proc_cxt.MyProgName = "DcfConsensusCallbackWorker";
            break;
        case RECVLOG_CALLBACK:
            t_thrd.proc_cxt.MyProgName = "DcfRecvLogCallbackWorker";
            break;
        case PROMOTE_DEMOTE_CALLBACK:
            t_thrd.proc_cxt.MyProgName = "DcfPromoteDemoteCallbackWorker";
            break;
        case DCF_EXCEPTION_CALLBACK:
            t_thrd.proc_cxt.MyProgName = "DCFExceptionCallbackWorker";
            break;
        case ELECTION_CALLBACK:
            t_thrd.proc_cxt.MyProgName = "ELECTIONCallbackWorker";
            break;
        case PROCESSMSG_CALLBACK:
            t_thrd.proc_cxt.MyProgName = "PROCESSMSGCallbackWorker";
        default:
            break;
    }
    t_thrd.proc_cxt.MyStartTime = time(NULL);
    /* set memory conext */
    SelfMemoryContext = AllocSetContextCreate(NULL, "DCF MEMORY CONTEXT",
                                                ALLOCSET_DEFAULT_MINSIZE,
                                                ALLOCSET_DEFAULT_INITSIZE,
                                                ALLOCSET_DEFAULT_MAXSIZE);
    InitShmemForDcfCallBack();
}

void DcfThreadShmemInit()
{
    DcfCallBackThreadShmemInit(NON_CALLBACK);
}

static void DcfUpdateAppliedRecordIndex(uint64 recordIndex, uint64 recordLsn)
{
    volatile DcfContextInfo* dcfCtx = t_thrd.dcf_cxt.dcfCtxInfo;
    if (dcfCtx == NULL) {
        ereport(FATAL,(errmsg("failed to update applied index, because dcf context is null.")));
    }
    if (dcfCtx->isRecordIdxBlocked)
        return;

    SpinLockAcquire(&dcfCtx->recordDcfIdxMutex);
    if (dcfCtx->dcfRecordIndex != 0)
        t_thrd.shemem_ptr_cxt.dcfData->appliedIndex = dcfCtx->dcfRecordIndex;
    if (dcfCtx->recordLsn != 0)
        dcfCtx->appliedLsn = dcfCtx->recordLsn;
    dcfCtx->dcfRecordIndex = recordIndex;
    dcfCtx->recordLsn = recordLsn;
    dcfCtx->isRecordIdxBlocked = true;
    SpinLockRelease(&dcfCtx->recordDcfIdxMutex);
}

typedef struct BuildReasonMsg {
    char action[1];      /* Q & R */
    char pg_sversion[11];
    char pg_pversion[32];
    char system_id[32];
    char tli[11];
    char server_mode[11];
    char padding[30];
} BuildReasonMsg;

int ReplyBuildInfo(unsigned int src_node_id)
{
    ServerMode localMode = UNKNOWN_MODE;
    volatile HaShmemData *hashmdata = t_thrd.postmaster_cxt.HaShmData;
    int nRet = 0;
    errno_t rc = EOK;
    uint32 crc = 0;

    /* reply build reason query result to dcf follower */
    BuildReasonMsg* ReplyMsg = static_cast<BuildReasonMsg *>(palloc0(sizeof(BuildReasonMsg)));

    ReplyMsg->action[0] = 'R';

    nRet = snprintf_s(ReplyMsg->pg_sversion, sizeof(ReplyMsg->pg_sversion),
                      sizeof(ReplyMsg->pg_sversion) - 1, "%u", PG_VERSION_NUM);
    securec_check_ss(nRet, "\0", "\0");

    rc = strncpy_s(ReplyMsg->pg_pversion, sizeof(ReplyMsg->pg_pversion),
                   PG_PROTOCOL_VERSION, strlen(PG_PROTOCOL_VERSION));
    securec_check(rc, "\0", "\0");
    ReplyMsg->pg_pversion[strlen(PG_PROTOCOL_VERSION)] = '\0';

    rc = snprintf_s(ReplyMsg->system_id, sizeof(ReplyMsg->system_id),
                    sizeof(ReplyMsg->system_id) - 1, UINT64_FORMAT, GetSystemIdentifier());
    securec_check_ss(rc, "\0", "\0");

    rc = snprintf_s(ReplyMsg->tli, sizeof(ReplyMsg->tli), 
                    sizeof(ReplyMsg->tli) - 1, "%u", GetRecoveryTargetTLI());
    securec_check_ss(rc, "\0", "\0");

    SpinLockAcquire(&hashmdata->mutex);
    localMode = hashmdata->current_mode;
    SpinLockRelease(&hashmdata->mutex);

    rc = snprintf_s(ReplyMsg->server_mode, sizeof(ReplyMsg->server_mode), 
                    sizeof(ReplyMsg->server_mode) - 1, "%u", localMode);
    securec_check_ss(rc, "\0", "\0");

    INIT_CRC32C(crc);
    COMP_CRC32C(crc, reinterpret_cast<char*>(ReplyMsg), sizeof(BuildReasonMsg));
    FIN_CRC32C(crc);

    bool sent = (dcf_send_msg(1, src_node_id, reinterpret_cast<char*>(ReplyMsg), sizeof(BuildReasonMsg)) == 0);
    pfree(ReplyMsg);
    if (sent) {
        ereport(LOG, 
                (errmsg("DCF leader send reply to follower %u successfully, crc %u.", src_node_id, crc)));
        return 0;
    } else {
        ereport(WARNING, 
                (errmsg("DCF leader failed to send reply to follower %u.", src_node_id)));
        return -1;
    }
}

/*
 * Regular request from standby to send config file.
 */
void ProcessStandbyFileTimeMessage(unsigned int src_node_id, const char* msg)
{
    ConfigModifyTimeMessage reply_modify_file_time;
    struct stat statbuf;
    errno_t errorno = EOK;

    /* skip first char */
    char* buf = const_cast<char*>(msg) + 1;

    errorno = memcpy_s(&reply_modify_file_time,
                       sizeof(ConfigModifyTimeMessage),
                       buf,
                       sizeof(ConfigModifyTimeMessage));
    securec_check(errorno, "\0", "\0");

    if (lstat(t_thrd.dcf_cxt.dcfCtxInfo->gucconf_file, &statbuf) != 0) {
        if (errno != ENOENT)
            ereport(ERROR, (errcode_for_file_access(),
                            errmsg("could not stat file or directory \"%s\": %m",
                                   t_thrd.dcf_cxt.dcfCtxInfo->gucconf_file)));
    }
    if (reply_modify_file_time.config_modify_time != statbuf.st_mtime) {
        ereport(LOG, (errmsg("DCF leader config file has been modified, so send it to follower")));
        SyncConfigFile(&src_node_id);
    } else {
        ereport(LOG, (errmsg("DCF leader config file has no change")));
    }
}

/* receive xlog archive task response from follower */
static void ProcessArchiveFeedbackMessage(uint32 srcNodeID, const char* msg, uint32 msgSize)
{
    if (msgSize != sizeof(ArchiveXlogResponseMessage) + 1) {
        ereport(WARNING, (errmsg("Corrupted Archive Xlog Feedback Message from follower %u.", srcNodeID)));
        return;
    }
    /* skip first char */
    char* msgData = const_cast<char*>(msg) + 1;
    const ArchiveXlogResponseMessage* reply = reinterpret_cast<ArchiveXlogResponseMessage*>(msgData);

    ereport(LOG,
            (errmsg("ProcessArchiveFeedbackMessage %s : %d %X/%X from follower %u",
                    reply->slot_name, reply->pitr_result,
                    static_cast<uint32>(reply->targetLsn >> 32), static_cast<uint32>(reply->targetLsn),
                    srcNodeID)));

    ArchiveTaskStatus *archive_task_status = nullptr;
    archive_task_status = find_archive_task_status(reply->slot_name);
    if (archive_task_status == nullptr) {
        ereport(ERROR,
                (errmsg("ProcessArchiveFeedbackMessage %s : %d %X/%X, but not find slot",
                        reply->slot_name, reply->pitr_result,
                        static_cast<uint32>(reply->targetLsn >> 32), static_cast<uint32>(reply->targetLsn))));
    }
    SpinLockAcquire(&archive_task_status->mutex);
    archive_task_status->pitr_finish_result = reply->pitr_result;
    archive_task_status->archive_task.targetLsn = reply->targetLsn;
    SpinLockRelease(&archive_task_status->mutex);
    if (archive_task_status->archiver_latch == nullptr) {
        /*
         * slave send feedback message for the arch request that sent during last restart,
         * and arch thread is not start yet, so we ignore this message unti arch thread is normal.
         */
        ereport(WARNING,
                (errmsg("leader received archive feedback message, but arch not work yet %d %X/%X",
                        reply->pitr_result,
                        static_cast<uint32>(reply->targetLsn >> 32), static_cast<uint32>(reply->targetLsn))));
        return;
    }
    SetLatch(archive_task_status->archiver_latch);
}

/* leader action to deal with msg */
void ReplyFollower(unsigned int src_node_id, const char* msg, unsigned int msg_size)
{
    unsigned char type = *msg;
    switch (type) {
        case 'Q': /* reply build info */
        {
            ReplyBuildInfo(src_node_id);
            break;
        }
        case 'A': /* reply config file */
        {
            ProcessStandbyFileTimeMessage(src_node_id, msg);
            break;
        }
        case 'r':
        {
            DCFProcessStandbyReplyMessage(src_node_id, msg, msg_size);
            break;
        }
        case 'a': /* archive xlog msg */
        {
            ProcessArchiveFeedbackMessage(src_node_id, msg, msg_size);
        }
        case 'R':
        case 'm':
        {
            break;
        }
        default:
        {
            ereport(ERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION),
                            errmsg("unexpected message type \"%d\"", type)));
        }
    }
}

void SetBuildState(unsigned int src_node_id, const char* msg, unsigned int msg_size)
{
    uint32 remoteSversion;
    uint32 localSversion;
    char *remotePversion = NULL;
    char *localPversion = NULL;
    char *remoteSysid = NULL;
    char localSysid[32];
    TimeLineID remoteTli;
    TimeLineID localTli;
    ServerMode remoteMode = UNKNOWN_MODE;
    int nRet = 0;
    uint32 crc = 0;

    INIT_CRC32C(crc);
    COMP_CRC32C(crc, const_cast<char*>(msg), msg_size);
    FIN_CRC32C(crc);

    ereport(LOG, 
            (errmsg("DCF follower received reply from leader %u successfully, crc %u.", src_node_id, crc)));

    if (t_thrd.dcf_cxt.dcfCtxInfo->dcf_need_build_set) {
        ereport(LOG, (errmsg("DCF follower already set build reason.")));
        return;
    }

    BuildReasonMsg* ReceivedMsg = reinterpret_cast<BuildReasonMsg*>(const_cast<char*>(msg));
    remoteMode = (ServerMode)pg_strtoint32(ReceivedMsg->server_mode);

    /* report remote server mode. Switchover/Failover may take a long time */
    if (remoteMode != PRIMARY_MODE)
        ereport(WARNING, (errcode(ERRCODE_INVALID_STATUS),
                errmsg("the mode of the remote server must be primary, current is %s",
                       wal_get_role_string(remoteMode))));

    /* Assert status will be set */
    t_thrd.dcf_cxt.dcfCtxInfo->dcf_need_build_set = true;

    /* identify version */
    remoteSversion = pg_strtoint32(ReceivedMsg->pg_sversion);
    localSversion = PG_VERSION_NUM;
    remotePversion = ReceivedMsg->pg_pversion;
    localPversion = pstrdup(PG_PROTOCOL_VERSION);
    if (localPversion == NULL) {
        HaSetRebuildRepInfoError(VERSION_REBUILD);
        ereport(WARNING,
                (errcode(ERRCODE_INVALID_STATUS),
                 errmsg("could not get the local protocal version, make sure the PG_PROTOCOL_VERSION is defined")));
        return;
    }
    /*
     * If the version of the remote server is not the same as the local's, then set error
     * message in WalRcv and rebuild reason in HaShmData
     */
    if (remoteSversion != localSversion || strncmp(remotePversion, localPversion, strlen(PG_PROTOCOL_VERSION)) != 0) {
        HaSetRebuildRepInfoError(VERSION_REBUILD);
        if (remoteSversion != localSversion) {
            ereport(WARNING, (errcode(ERRCODE_INVALID_STATUS),
                            errmsg("database system version is different between the remote and local"),
                            errdetail("The remote's system version is %u, the local's system version is %u.",
                                      remoteSversion, localSversion)));
        } else {
            ereport(WARNING, (errcode(ERRCODE_INVALID_STATUS),
                            errmsg("the remote protocal version %s is not the same as the local protocal version %s.",
                                   remotePversion, localPversion)));
        }
        return;
    }
    if (localPversion != NULL) {
        pfree(localPversion);
        localPversion = NULL;
    }

    /*
     * Confirm that the system identifier of the primary is the same as ours.
     */
    remoteSysid = ReceivedMsg->system_id;
    nRet = snprintf_s(localSysid, sizeof(localSysid), sizeof(localSysid) - 1, UINT64_FORMAT, GetSystemIdentifier());
    securec_check_ss(nRet, "", "");
    if (strcmp(remoteSysid, localSysid) != 0) {
        HaSetRebuildRepInfoError(SYSTEMID_REBUILD);
        ereport(WARNING,
                (errcode(ERRCODE_INVALID_STATUS),
                 errmsg("database system identifier differs between the primary and standby"),
                 errdetail("The primary's identifier is %s, the standby's identifier is %s.",
                           remoteSysid,
                           localSysid)));
        return;
    }

    /*
     * Confirm that the current timeline of the primary is the same as the
     * recovery target timeline.
     */
    remoteTli = pg_strtoint32(ReceivedMsg->tli);
    localTli = GetRecoveryTargetTLI();
    if (remoteTli != localTli) {
        /*
         * If the timeline id different,
         * then set error message in WalRcv and rebuild reason in HaShmData.
         */
        HaSetRebuildRepInfoError(TIMELINE_REBUILD);
        ereport(WARNING, (errcode(ERRCODE_INVALID_STATUS),
                        errmsg("timeline %u of the primary does not match recovery target timeline %u", remoteTli,
                               localTli)));
        return;
    }

    /* Finally, after some check we make sure there is no need to build dn */
    t_thrd.dcf_cxt.dcfCtxInfo->dcf_build_done = true;
    HaSetRebuildRepInfoError(NONE_REBUILD);
    return;
}

bool ProcessConfigFileMessage(char *buf, Size len)
{
    struct stat statbuf;
    ErrCode retcode = CODE_OK;
    ConfFileLock filelock = { NULL, 0 };
    char **reserve_item = NULL;

    char* conf_bak = t_thrd.dcf_cxt.dcfCtxInfo->bak_guc_conf_file;

    if (lstat(t_thrd.dcf_cxt.dcfCtxInfo->gucconf_file, &statbuf) != 0) {
        if (errno != ENOENT)
            ereport(ERROR, (errcode_for_file_access(), errmsg("could not stat file or directory \"%s\": %m",
                                                              t_thrd.dcf_cxt.dcfCtxInfo->gucconf_file)));
        return false;
    }

    reserve_item = alloc_opt_lines(g_reserve_param_num);
    if (reserve_item == NULL) {
        ereport(LOG, (errmsg("Alloc mem for reserved parameters failed")));
        return false;
    }

    /* 1. lock postgresql.conf 
     * Get two locks for postgresql.conf before making changes, please refer to 
     * AlterSystemSetConfigFile() in guc.cpp for detailed explanations.
     */
    if (get_file_lock(t_thrd.dcf_cxt.dcfCtxInfo->gucconf_lock_file, &filelock) != CODE_OK) {
        release_opt_lines(reserve_item);
        ereport(LOG, (errmsg("Modify the postgresql.conf failed : can not get the file lock ")));
        return false;
    }
    LWLockAcquire(ConfigFileLock, LW_EXCLUSIVE);

    /* 2. load reserved parameters to reserve_item(array in memeory) */
    retcode = copy_asyn_lines(t_thrd.dcf_cxt.dcfCtxInfo->gucconf_file, reserve_item, g_reserve_param);
    if (retcode != CODE_OK) {
        release_opt_lines(reserve_item);
        release_file_lock(&filelock);
        LWLockRelease(ConfigFileLock);
        ereport(LOG, (errmsg("copy asynchronization items failed: %s\n", gs_strerror(retcode))));
        return false;
    }

    /* 3. genreate temp files and fill it with content from primary. */
    retcode = generate_temp_file(buf, conf_bak, len);
    if (retcode != CODE_OK) {
        release_opt_lines(reserve_item);
        release_file_lock(&filelock);
        LWLockRelease(ConfigFileLock);
        ereport(LOG, (errmsg("create %s failed: %s\n", conf_bak, gs_strerror(retcode))));
        return false;
    }

    /* 4. adjust the info with reserved parameters, and sync to temp file. */
    retcode = update_temp_file(conf_bak, reserve_item, g_reserve_param);
    if (retcode != CODE_OK) {
        release_file_lock(&filelock);
        LWLockRelease(ConfigFileLock);
        release_opt_lines(reserve_item);
        ereport(LOG, (errmsg("update gaussdb config file failed: %s\n", gs_strerror(retcode))));
        return false;
    } else {
        ereport(LOG, (errmsg("update gaussdb config file success")));
        if (rename(conf_bak, t_thrd.dcf_cxt.dcfCtxInfo->gucconf_file) != 0) {
            release_file_lock(&filelock);
            LWLockRelease(ConfigFileLock);
            release_opt_lines(reserve_item);
            ereport(LOG, (errcode_for_file_access(), errmsg("could not rename \"%s\" to \"%s\": %m", conf_bak,
                                                            t_thrd.dcf_cxt.dcfCtxInfo->gucconf_file)));
            return false;
        }
    }

    /* save the modify time of standby config file */
    if (lstat(t_thrd.dcf_cxt.dcfCtxInfo->gucconf_file, &statbuf) != 0) {
        if (errno != ENOENT) {
            release_file_lock(&filelock);
            LWLockRelease(ConfigFileLock);
            release_opt_lines(reserve_item);
            ereport(ERROR, (errcode_for_file_access(), errmsg("could not stat file or directory \"%s\": %m",
                                                              t_thrd.dcf_cxt.dcfCtxInfo->gucconf_file)));
            return false;
        }
    }
    t_thrd.dcf_cxt.dcfCtxInfo->standby_config_modify_time = statbuf.st_mtime;

    if (statbuf.st_size > 0) {
        copy_file_internal(t_thrd.dcf_cxt.dcfCtxInfo->gucconf_file,
                           t_thrd.dcf_cxt.dcfCtxInfo->temp_guc_conf_file, true);
        ereport(DEBUG1, (errmsg("copy %s to %s success", t_thrd.dcf_cxt.dcfCtxInfo->gucconf_file,
                                t_thrd.dcf_cxt.dcfCtxInfo->temp_guc_conf_file)));
    }

    release_file_lock(&filelock);
    LWLockRelease(ConfigFileLock);
    release_opt_lines(reserve_item);

    /* notify postmaster the config file has changed */
    if (gs_signal_send(PostmasterPid, SIGHUP) != 0) {
        ereport(WARNING, (errmsg("send SIGHUP to PM failed")));
        return false;
    }
    return true;
}

void ProcessConfigFile(unsigned int src_node_id, const char* msg, unsigned int msg_size)
{
    errno_t errorno = EOK;

    /* skip first char */
    char* buf = const_cast<char*>(msg) + 1;
    Size len = static_cast<Size>(msg_size) - 1;

    ereport(LOG,
            (errmsg("DCF follower received config file from leader %u.", src_node_id)));

    if (len < sizeof(ConfigModifyTimeMessage)) {
        ereport(ERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION),
                        errmsg_internal("invalid config file message")));
        return;
    }

    ConfigModifyTimeMessage primary_config_file;
    /* memcpy is required here for alignment reasons */
    errorno = memcpy_s(&primary_config_file, sizeof(ConfigModifyTimeMessage), buf,
                       sizeof(ConfigModifyTimeMessage));
    securec_check(errorno, "\0", "\0");
    t_thrd.dcf_cxt.dcfCtxInfo->Primary_config_modify_time = primary_config_file.config_modify_time;
    buf += sizeof(ConfigModifyTimeMessage);
    len -= sizeof(ConfigModifyTimeMessage);
    ereport(LOG, (errmsg("DCF follower received gaussdb config file size: %lu", len)));
    if (!ProcessConfigFileMessage(buf, len)) {
        ereport(LOG, (errmsg("DCF follower update config file failed")));
    }
}

static void wakeupObsArchLatch()
{
    /* use volatile pointer to prevent code rearrangement */
    volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;
    SpinLockAcquire(&walrcv->mutex);
    if (walrcv->obsArchLatch != NULL) {
        SetLatch(walrcv->obsArchLatch);
    }
    SpinLockRelease(&walrcv->mutex);
}

/* Process xlog archive task received from leader. */
const static int GET_ARCHIVE_XLOG_RETRY_MAX = 50;
const static int ARCHIVE_XLOG_DELAY = 10000;

static void ProcessArchiveXlogMessage(uint32 srcNodeID, const char* msg, uint32 msgSize)
{
    if (msgSize != sizeof(ArchiveXlogMessage) + 1) {
        ereport(WARNING, (errmsg("Corrupted Archive Xlog Message from leader %u.", srcNodeID)));
        return;
    }

    /* skip first char */
    char* msgData = const_cast<char*>(msg) + 1;
    const ArchiveXlogMessage* archive_xlog_message = reinterpret_cast<ArchiveXlogMessage*>(msgData);
    ereport(LOG, (errmsg("get archive xlog message %s :%X/%X from leader %u",
                         archive_xlog_message->slot_name, 
                         static_cast<uint32>(archive_xlog_message->targetLsn >> 32),
                         static_cast<uint32>(archive_xlog_message->targetLsn),
                         srcNodeID)));
    errno_t errorno = EOK;
    ArchiveTaskStatus *archive_task_status = find_archive_task_status(archive_xlog_message->slot_name);
    if (archive_task_status == nullptr) {
        ereport(ERROR, (errmsg("get archive xlog message %s :%X/%X, but task slot not find",
                               archive_xlog_message->slot_name,
                               static_cast<uint32>(archive_xlog_message->targetLsn >> 32),
                               static_cast<uint32>(archive_xlog_message->targetLsn))));
    }
    volatile unsigned int *pitr_task_status = &archive_task_status->pitr_task_status;
    if (archive_xlog_message->targetLsn == InvalidXLogRecPtr) {
        pg_atomic_write_u32(pitr_task_status, PITR_TASK_NONE);
    }
    unsigned int expected = PITR_TASK_NONE;
    int failed_times = 0;
    /* 
     * lock for archiver get PITR_TASK_GET flag, but works on old task.
     * if archiver works between set flag and set task details.
     */
    while (pg_atomic_compare_exchange_u32(pitr_task_status, &expected, PITR_TASK_GET) == false) {
        /* some task arrived before last task done if expected not equal to NONE */
        expected = PITR_TASK_NONE;
        pg_usleep(ARCHIVE_XLOG_DELAY);  // sleep 0.01s
        if (failed_times++ >= GET_ARCHIVE_XLOG_RETRY_MAX) {
            ereport(WARNING, (errmsg("get archive xlog message %s :%X/%X, but not finished",
                                     archive_xlog_message->slot_name,
                                     static_cast<uint32>(archive_xlog_message->targetLsn >> 32),
                                     static_cast<uint32>(archive_xlog_message->targetLsn))));
            return;
        }
    }
    SpinLockAcquire(&archive_task_status->mutex);
    errorno = memcpy_s(&archive_task_status->archive_task,
                       sizeof(ArchiveXlogMessage),
                       archive_xlog_message,
                       sizeof(ArchiveXlogMessage));
    securec_check(errorno, "\0", "\0");
    SpinLockRelease(&archive_task_status->mutex);
    wakeupObsArchLatch();
}

/* follower action to deal with msg */
void CheckLeaderReply(unsigned int src_node_id, const char* msg, unsigned int msg_size)
{
    unsigned char type = *msg;
    switch (type) {
        case 'R': /* check build info */
        {
            SetBuildState(src_node_id, msg, msg_size);
            break;
        }
        case 'm': /* check config file */
        {
            ProcessConfigFile(src_node_id, msg, msg_size);
            break;
        }
        case 'a': /* archive xlog msg */
        {
            ProcessArchiveXlogMessage(src_node_id, msg, msg_size);
        }
        case 'Q':
        case 'r':
        case 'A':
        {
            break;
        }
        default:
            ereport(ERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION),
                            errmsg_internal("invalid replication message type %c", type)));
    }
}

/* 
 * Leader and follower share this func to deal with message.
 * Always return 0 cause dcf currently ignore ret val.
 */
int ProcessMsgCbFunc(unsigned int stream_id, unsigned int src_node_id,
                     const char* msg, unsigned int msg_size)
{
    uint32 leader_id = 0;
    bool is_leader = false;
    bool is_src_node_leader = false;

    DcfCallBackThreadShmemInit(PROCESSMSG_CALLBACK);
    if (!QueryLeaderNodeInfo(&leader_id)) {
        return 0;
    }

    is_leader = (leader_id == static_cast<unsigned int>(g_instance.attr.attr_storage.dcf_attr.dcf_node_id));
    is_src_node_leader = (leader_id == src_node_id);

    if (is_leader)
        ReplyFollower(src_node_id, msg, msg_size);
    else if (is_src_node_leader)
        CheckLeaderReply(src_node_id, msg, msg_size);
    else
        ereport(LOG, (errmsg("DCF follower received reply from old leader %u, ignore it.", src_node_id)));

    return 0;
}

/* Called in ReceiveLogCbFunc */
bool CheckBuildReasons()
{
    uint32 leader_id = 0;

    if (t_thrd.dcf_cxt.dcfCtxInfo->dcf_build_done)
        return true;

    /* once queried primary, never again */
    if (t_thrd.dcf_cxt.dcfCtxInfo->dcf_need_build_set)
        return false;

    if (!QueryLeaderNodeInfo(&leader_id))
        return false;

    if (leader_id == static_cast<uint32>(g_instance.attr.attr_storage.dcf_attr.dcf_node_id))
        return true;

    /* send build reason query to dcf leader */
    BuildReasonMsg *QueryMsg = static_cast<BuildReasonMsg *>(palloc0(sizeof(BuildReasonMsg)));
    QueryMsg->action[0] = 'Q';
    /* maybe short message better */
    bool sent = (dcf_send_msg(1, leader_id, reinterpret_cast<char*>(QueryMsg), sizeof(BuildReasonMsg)) == 0);
    pfree(QueryMsg);
    if (sent) {
        ereport(LOG,
                (errmsg("DCF follower send build reason query to leader %u successfully.", leader_id)));
    } else {
        ereport(WARNING, 
                (errmsg("DCF follower failed to send build reason query to leader %u.", leader_id)));
    }

    /* sleep before next query */
    pg_usleep(DCF_QUERY_IDLE);

    return t_thrd.dcf_cxt.dcfCtxInfo->dcf_build_done;
}

int ReceiveLogCbFunc(unsigned int stream_id, unsigned long long index, 
                     const char* buf, unsigned int len, unsigned long long lsn)
{
    DcfCallBackThreadShmemInit(RECVLOG_CALLBACK);
    ereport(DEBUG1,
            (errmodule(MOD_DCF),
             errmsg("Enter ReceiveLogCbFunc: stream_id = %u, index = %llu, len = %u, lsn = %llu.",
                    stream_id, index, len, lsn)));

    /* Can not fetch xlog unless build reason is same */
    if (!CheckBuildReasons())
        return -1;

    CheckConfigFile();

    /* get nothing from paxos, return directly */
    if (len == 0)
        return 0;

    /* When walreceiver is running and dcfAppliedIndex has been set properly. */
    if (g_instance.pid_cxt.WalReceiverPID == 0 ||
        t_thrd.walreceiverfuncs_cxt.WalRcv->walRcvState != WALRCV_RUNNING ||
        !t_thrd.dcf_cxt.dcfCtxInfo->isWalRcvReady) {
        pg_usleep(100);
        return -1;
    }

    t_thrd.walreceiver_cxt.walRcvCtlBlock = getCurrentWalRcvCtlBlock();

    /* receivePtr has not been set properly */
    if (XLByteEQ(t_thrd.walreceiver_cxt.walRcvCtlBlock->receivePtr, InvalidXLogRecPtr))
        return -1;

    XLogRecPtr paxosStartPtr = static_cast<XLogRecPtr>(lsn - len);
    XLogRecPtr paxosEndPtr = static_cast<XLogRecPtr>(lsn);
    XLogRecPtr receivePtr = t_thrd.walreceiver_cxt.walRcvCtlBlock->receivePtr;
    XLogRecPtr receiveSegPtr = receivePtr - receivePtr % XLogSegSize;
    uint32 alignOffset = 0;

    /* Receive xlog from the head of segment, this alignment needed by Extreme Rto */
    if (XLByteLE(paxosEndPtr, receiveSegPtr)) {
        return 0;
    } else if (XLByteLT(paxosStartPtr, receiveSegPtr)) {
        alignOffset = static_cast<uint32>(receiveSegPtr - paxosStartPtr);
    }

    /* 
     * It will hardly happen as full build is done already.
     * Or DCF lost its data, in that case, just keep all data and debug.
     */
    if (XLByteLT(receivePtr, paxosStartPtr)) {
        /* prevent print too many logs */
        t_thrd.dcf_cxt.dcfCtxInfo->dcf_build_done = false;
        t_thrd.dcf_cxt.dcfCtxInfo->dcf_need_build_set = true;
        ereport(WARNING,
                (errmsg("paxosStartPtr %lu greater than receivePtr %lu, index = %llu, endLsn = %llu.",
                        static_cast<uint64>(paxosStartPtr),
                        static_cast<uint64>(receivePtr),
                        index, lsn)));
        return -1;
    }

    /* len of xlog data that can be received actually */
    uint32 receive_len = len - alignOffset;

    /* copy data from offset of DCF buffer */
    char* copyStart = const_cast<char*>(buf) + alignOffset;
    XLogRecPtr copyStartPtr = paxosStartPtr + alignOffset;

    XLogWalRcvReceive(copyStart, receive_len, copyStartPtr);

    DcfUpdateAppliedRecordIndex(index, lsn);

    ereport(DEBUG1, (errmodule(MOD_DCF), errmsg("Exit ReceiveLogCbFunc.")));
    return 0;
}

/* Leader send config file without check ack */
bool SyncConfigFile(unsigned int* follower_id)
{
    char *buf = NULL;
    char *temp_buf = nullptr;
    char **opt_lines = nullptr;
    int len = 0;
    int temp_buf_len = 0;
    struct stat statbuf;
    ConfFileLock filelock = { NULL, 0 };
    ConfigModifyTimeMessage msgConfigTime;
    errno_t errorno = EOK;
    bool read_guc_file_success = true;
    char* path = t_thrd.dcf_cxt.dcfCtxInfo->gucconf_file;
    bool is_broadcast = (follower_id == nullptr);
    bool sent = false;

    if (lstat(path, &statbuf) < 0 || statbuf.st_size == 0) {
        ereport(LOG, (errcode_for_file_access(), errmsg("could not stat file or directory \"%s\": %m", path)));
        return false;
    }
    if (get_file_lock(t_thrd.dcf_cxt.dcfCtxInfo->gucconf_lock_file, &filelock) != CODE_OK) {
        ereport(LOG, (errmsg("get lock failed when send gaussdb config file to the peer.")));
        return false;
    }
    LWLockAcquire(ConfigFileLock, LW_EXCLUSIVE);
    PG_TRY();
    {
        opt_lines = read_guc_file(path);
    }
    PG_CATCH();
    {
        read_guc_file_success = false;
        EmitErrorReport();
        FlushErrorState();
    }
    PG_END_TRY();
    if (!read_guc_file_success) {
        /* if failed to read guc file, will log the error info in PG_CATCH(), no need to log again. */
        release_file_lock(&filelock);
        LWLockRelease(ConfigFileLock);
        return false;
    }
    if (opt_lines == nullptr) {
        release_file_lock(&filelock);
        LWLockRelease(ConfigFileLock);
        ereport(LOG, (errmsg("the config file has no data, please check it.")));
        return false;
    }
    comment_guc_lines(opt_lines, g_reserve_param);
    temp_buf_len = add_guc_optlines_to_buffer(opt_lines, &temp_buf);
    release_opt_lines(opt_lines);
    Assert(temp_buf_len != 0);
    /* temp_buf_len including last byte '\0' */
    len = 1 + sizeof(ConfigModifyTimeMessage) + temp_buf_len;
    buf = static_cast<char *>(palloc0(len));
    buf[0] = 'm';
    msgConfigTime.config_modify_time = statbuf.st_mtime;
    errorno = memcpy_s(buf + 1, sizeof(ConfigModifyTimeMessage) + temp_buf_len,
                       &msgConfigTime, sizeof(ConfigModifyTimeMessage));
    securec_check(errorno, "\0", "\0");
    errorno = memcpy_s(buf + 1 + sizeof(ConfigModifyTimeMessage), temp_buf_len,
                       temp_buf, temp_buf_len);
    securec_check(errorno, "\0", "\0");
    pfree(temp_buf);
    temp_buf = NULL;
    release_file_lock(&filelock);
    LWLockRelease(ConfigFileLock);

    if (is_broadcast) {
        sent = (dcf_broadcast_msg(1, buf, len) == 0);
    } else {
        sent = (dcf_send_msg(1, *follower_id, buf, len) == 0);
    }

    pfree(buf);
    buf = NULL;
    if (is_broadcast) {
        ereport(LOG,
                (errmsg("DCF leader broadcast config file to all followers: size :%d, success :%d",
                        len, sent)));
    } else {
        ereport(LOG,
                (errmsg("DCF leader send config file to follower %u: size :%d, success :%d",
                        *follower_id, len, sent)));
    }
    return sent;
}

/*
 * ConsensusLogCbFunc called by paxos replication module
 */
int ConsensusLogCbFunc(unsigned int stream_id, unsigned long long paxosIdx, 
                       const char* buf, unsigned int len, unsigned long long lsn, int error_no) 
{
    DcfCallBackThreadShmemInit(CONSENSUS_CALLBACK);
    ereport(DEBUG1,
            (errmodule(MOD_DCF),
             errmsg("Enter ConsensusLogCbFunc: stream_id = %u, paxosIdx = %llu, len = %u, lsn = %llu, errcode = %d.",
                    stream_id, paxosIdx, len, lsn, error_no)));

    /* sync config file to all followers after guc reload */
    if (t_thrd.dcf_cxt.dcfCtxInfo->dcfNeedSyncConfig && SyncConfigFile()) {
        t_thrd.dcf_cxt.dcfCtxInfo->dcfNeedSyncConfig = false;
    }

    /* get nothing from paxos, return directly */
    if (len == 0)
        return 0;

    XLogRecPtr consensusLsn = (XLogRecPtr) lsn;
    DcfUpdateConsensusLsnAndIndex(consensusLsn, paxosIdx);
    DcfUpdateAppliedRecordIndex(paxosIdx, lsn);
    /* wake up all waiting procs */
    SyncPaxosReleaseWaiters(consensusLsn);

    ereport(DEBUG1, (errmodule(MOD_DCF), errmsg("Exit ConsensusLogCbFunc.")));
    return 0;
}

void PromoteCallbackFunc() 
{
    ereport(LOG, (errmsg("Enter PromoteCallbackFunc, isDcfShmemInited = %d", t_thrd.dcf_cxt.isDcfShmemInited)));
    /* prevent build reason check when leader call follower call back func */
    t_thrd.dcf_cxt.dcfCtxInfo->dcf_build_done = true;
    t_thrd.dcf_cxt.dcfCtxInfo->dcf_need_build_set = true;
    if (g_instance.status > NoShutdown) {
        ereport(LOG, (errmsg("Instance can't be promoted during shutdown process.")));
        return;
    }
    if (g_instance.pid_cxt.StartupPID != 0 && (pmState == PM_STARTUP || pmState == PM_RECOVERY ||
        pmState == PM_HOT_STANDBY ||
        pmState == PM_WAIT_READONLY)) {
        gs_lock_test_and_set_64(&g_instance.stat_cxt.NodeStatResetTime, GetCurrentTimestamp());
        if (GetHaShmemMode() != STANDBY_MODE) {
           ereport(LOG, (errmsg("Instance can't be promoted in none standby mode.")));
        } else {
            /* To be leader, either switchover or failover */
            t_thrd.dcf_cxt.dcfCtxInfo->dcf_to_be_leader = true;
           /* Database Security: Support database audit */
           if (t_thrd.walreceiverfuncs_cxt.WalRcv &&
               t_thrd.walreceiverfuncs_cxt.WalRcv->node_state >= NODESTATE_SMART_DEMOTE_REQUEST &&
               t_thrd.walreceiverfuncs_cxt.WalRcv->node_state <= NODESTATE_EXTRM_FAST_DEMOTE_REQUEST) {
               /* Now it's only assumed the primary was demoted successfully but actually it  only succeeded in DCF. */
               t_thrd.walreceiverfuncs_cxt.WalRcv->node_state = NODESTATE_STANDBY_PROMOTING;
               t_thrd.postmaster_cxt.audit_standby_switchover = true;
               /* Tell startup process to finish recovery */
               ereport(LOG, (errmsg("Instance to do switchover.")));
               SendNotifySignal(NOTIFY_SWITCHOVER, g_instance.pid_cxt.StartupPID);
           } else {
               if (t_thrd.walreceiverfuncs_cxt.WalRcv)
                   t_thrd.walreceiverfuncs_cxt.WalRcv->node_state = NODESTATE_STANDBY_FAILOVER_PROMOTING;
               t_thrd.postmaster_cxt.audit_primary_failover = true;
               /* Tell startup process to finish recovery */
               ereport(LOG, (errmsg("Instance to do failover.")));
               SendNotifySignal(NOTIFY_FAILOVER, g_instance.pid_cxt.StartupPID);
           }
        }
    }

    ereport(LOG, (errmsg("Exit PromoteCallbackFunc, isDcfShmemInited = %d", t_thrd.dcf_cxt.isDcfShmemInited)));
}

void DemoteCallbackFunc()
{
    ereport(LOG, (errmsg("Enter DemoteCallbackFunc, isDcfShmemInited = %d", t_thrd.dcf_cxt.isDcfShmemInited)));
    /* Old primary should check build reason of new primary */
    t_thrd.dcf_cxt.dcfCtxInfo->dcf_build_done = false;
    t_thrd.dcf_cxt.dcfCtxInfo->dcf_need_build_set = false;
    ResetDCFNodesInfo();
    SendPostmasterSignal(PMSIGNAL_DEMOTE_PRIMARY);
    ereport(LOG, (errmsg("Exit DemoteCallbackFunc, isDcfShmemInited = %d", t_thrd.dcf_cxt.isDcfShmemInited)));
}

int PromoteOrDemote(unsigned int stream_id, dcf_role_t new_role) 
{
    DcfCallBackThreadShmemInit(PROMOTE_DEMOTE_CALLBACK);
    if (new_role == DCF_ROLE_LEADER) {
        PromoteCallbackFunc();
    } else if (new_role == DCF_ROLE_FOLLOWER) {
        DemoteCallbackFunc();
    }
    return 0;
}

int DCFExceptionCbFunc(unsigned int stream_id, dcf_exception_t dcfException)
{
    DcfCallBackThreadShmemInit(DCF_EXCEPTION_CALLBACK);
    ereport(LOG, (errmsg("Enter DCFExceptionCbFunc and stream_id is %u, dcf exception is %d",
        stream_id, dcfException)));
    /* Tell postmaster to update need_repair to state file */
    if (dcfException == DCF_EXCEPTION_MISSING_LOG) {
        ereport(LOG, (errmsg("DCF missed log and full build is required!")));
        HaSetRebuildRepInfoError(DCF_LOG_LOSS_REBUILD);
    }
    ereport(LOG, (errmsg("Exit DCFExceptionCbFunc")));
    return 0;
}

/* 
 * Every time election happened, every follower call it (old leader heartbeat timeout).
 */
int ElectionCbFunc(unsigned int stream_id, unsigned int new_leader)
{
    DcfCallBackThreadShmemInit(ELECTION_CALLBACK);
    ereport(LOG,
            (errmsg("Enter ElectionCbFunc: stream_id = %u, new_leader = %u.",
                    stream_id, new_leader)));

    /* ASSERT dcf_node_id never change and greater than 0 */
    Assert(new_leader != static_cast<unsigned int>(g_instance.attr.attr_storage.dcf_attr.dcf_node_id));
    /* re-confirm standby's build reason whether same to new primary */
    t_thrd.dcf_cxt.dcfCtxInfo->dcf_build_done = false;
    t_thrd.dcf_cxt.dcfCtxInfo->dcf_need_build_set = false;
    ereport(LOG, (errmsg("Exit ElectionCbFunc.")));
    return 0;
}
#endif
