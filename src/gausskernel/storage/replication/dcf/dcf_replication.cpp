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
 *  dcf_replication.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/replication/dcf/dcf_replication.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>
#include <signal.h>
#include <string>
#include "storage/shmem.h"
#include "replication/dcf_flowcontrol.h"
#include "replication/dcf_replication.h"
#include "replication/walreceiver.h"
#include "utils/timestamp.h"
#include "utils/guc.h"
#include "storage/copydir.h"
#include "postmaster/postmaster.h"
#include "port/pg_crc32c.h"
#include "replication/dcf_data.h"

#ifndef ENABLE_MULTIPLE_NODES

#ifdef ENABLE_UT
#define static
#endif

#define TEMP_CONF_FILE "postgresql.conf.bak"


bool IsDCFReadyOrDisabled(void)
{
    if (g_instance.attr.attr_storage.dcf_attr.enable_dcf) {
        if (!t_thrd.dcf_cxt.dcfCtxInfo->isDcfStarted) {
            ereport(DEBUG1, (errmodule(MOD_DCF), errmsg("DCF thread has not been started.")));
        }
        return t_thrd.dcf_cxt.dcfCtxInfo->isDcfStarted;
    }
    return true;
}
/* The dcf interfaces */
bool DCFSendMsg(uint32 streamID, uint32 destNodeID, const char* msg, uint32 msgSize)
{
    Assert((t_thrd.dcf_cxt.is_dcf_thread && t_thrd.dcf_cxt.isDcfShmemInited) ||
           !t_thrd.dcf_cxt.is_dcf_thread);

    if (dcf_send_msg(streamID, destNodeID, msg, msgSize) == 0) {
        return true;
    }
    return false;
}

static bool SetDCFReplyMsgIfNeed()
{
    TimestampTz now;
    XLogRecPtr receivePtr = InvalidXLogRecPtr;
    XLogRecPtr writePtr = InvalidXLogRecPtr;
    XLogRecPtr flushPtr = InvalidXLogRecPtr;
    XLogRecPtr applyPtr = InvalidXLogRecPtr;
    XLogRecPtr replayReadPtr = InvalidXLogRecPtr;
    int rc = 0;
    volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;
    volatile HaShmemData *hashmdata = t_thrd.postmaster_cxt.HaShmData;
    volatile DcfContextInfo *dcfCtx = t_thrd.dcf_cxt.dcfCtxInfo;
    XLogRecPtr sndFlushPtr;
    applyPtr = GetXLogReplayRecPtr(nullptr, &replayReadPtr);
    SpinLockAcquire(&t_thrd.walreceiver_cxt.walRcvCtlBlock->mutex);
    receivePtr = t_thrd.walreceiver_cxt.walRcvCtlBlock->receivePtr;
    writePtr = t_thrd.walreceiver_cxt.walRcvCtlBlock->writePtr;
    flushPtr = t_thrd.walreceiver_cxt.walRcvCtlBlock->flushPtr;
    SpinLockRelease(&t_thrd.walreceiver_cxt.walRcvCtlBlock->mutex);

    /* Get current timestamp. */
    now = GetCurrentTimestamp();

    int wal_receiver_status_interval = u_sess->attr.attr_storage.wal_receiver_status_interval;
    bool noNeed = (XLByteEQ(dcfCtx->dcf_reply_message->receive, receivePtr) &&
                   XLByteEQ(dcfCtx->dcf_reply_message->write, writePtr) &&
                   XLByteEQ(dcfCtx->dcf_reply_message->flush, flushPtr) &&
                   !(TimestampDifferenceExceeds(dcfCtx->dcf_reply_message->sendTime, now,
                                                wal_receiver_status_interval * DCF_UNIT_S) ||
                     TimestampDifferenceExceeds(now, dcfCtx->dcf_reply_message->sendTime,
                                                wal_receiver_status_interval * DCF_UNIT_S)));
    if (noNeed)
        return false;

    /*
     * This following comment isn't been considered now.
     * We can compare the write and flush positions to the last message we
     * sent without taking any lock, but the apply position requires a spin
     * lock, so we don't check that unless something else has changed or 10
     * seconds have passed.  This means that the apply log position will
     * appear, from the master's point of view, to lag slightly, but since
     * this is only for reporting purposes and only on idle systems, that's
     * probably OK.
     */
    /* Construct a new message */
    char *standbyName = (char *)(dcfCtx->dcf_reply_message->id);
    rc = strncpy_s(standbyName, DCF_STANDBY_NAME_SIZE, u_sess->attr.attr_common.application_name,
                   strlen(u_sess->attr.attr_common.application_name));
    securec_check(rc, "\0", "\0");
    dcfCtx->dcf_reply_message->receive = receivePtr;
    dcfCtx->dcf_reply_message->write = writePtr;
    dcfCtx->dcf_reply_message->flush = flushPtr;
    dcfCtx->dcf_reply_message->apply = applyPtr;
    dcfCtx->dcf_reply_message->applyRead = replayReadPtr;
    dcfCtx->dcf_reply_message->sendTime = now;
    dcfCtx->dcf_reply_message->replyRequested = false;

    SpinLockAcquire(&hashmdata->mutex);
    dcfCtx->dcf_reply_message->peer_role = hashmdata->current_mode;
    SpinLockRelease(&hashmdata->mutex);
    dcfCtx->dcf_reply_message->peer_state = get_local_dbstate();
    SpinLockAcquire(&walrcv->mutex);
    walrcv->receiver_received_location = receivePtr;
    walrcv->receiver_write_location = writePtr;
    walrcv->receiver_flush_location = flushPtr;
    walrcv->receiver_replay_location = dcfCtx->dcf_reply_message->apply;
    sndFlushPtr = walrcv->sender_flush_location;
    SpinLockRelease(&walrcv->mutex);
    return true;
}

/* called by walreceiver to send xlog location to leader. */
bool DCFSendXLogLocation(void)
{
    char buf[sizeof(DCFStandbyReplyMessage) + 1] = {0};
    int rc = 0;
    /* Make sure logger as well as nodes without build don't send xlog location for it doesn't write xlog. */
    if (!t_thrd.dcf_cxt.dcfCtxInfo->dcf_build_done) {
        return false;
    }
    uint32 leaderID = 0;
    char ip[DCF_MAX_IP_LEN] = {0};
    uint32 port = 0;
    bool success = QueryLeaderNodeInfo(&leaderID, ip, DCF_MAX_IP_LEN, &port);
    if (!success) {
        ereport(WARNING, (errmsg("DCF failed to query leader info.")));
        return false;
    }
    ereport(DEBUG1, (errmsg("The lead id is %u", leaderID)));
    /* Leader doesn't need to send xlog location. */
    if ((uint32)g_instance.attr.attr_storage.dcf_attr.dcf_node_id == leaderID) {
        ereport(DEBUG1, (errmsg("Don't send node info to itself!")));
        return false;
    }

    /* if need to send reply, set it */
    if (!SetDCFReplyMsgIfNeed())
        return false;

    if (u_sess->attr.attr_storage.HaModuleDebug) {
        ereport(LOG, (errmsg("HA-XLogWalRcvSendReply: sending receive %X/%X write %X/%X flush %X/%X apply %X/%X",
                             (uint32)(t_thrd.dcf_cxt.dcfCtxInfo->dcf_reply_message->receive >> 32),
                             (uint32)t_thrd.dcf_cxt.dcfCtxInfo->dcf_reply_message->receive,
                             (uint32)(t_thrd.dcf_cxt.dcfCtxInfo->dcf_reply_message->write >> 32),
                             (uint32)t_thrd.dcf_cxt.dcfCtxInfo->dcf_reply_message->write,
                             (uint32)(t_thrd.dcf_cxt.dcfCtxInfo->dcf_reply_message->flush >> 32),
                             (uint32)t_thrd.dcf_cxt.dcfCtxInfo->dcf_reply_message->flush,
                             (uint32)(t_thrd.dcf_cxt.dcfCtxInfo->dcf_reply_message->apply >> 32),
                             (uint32)t_thrd.dcf_cxt.dcfCtxInfo->dcf_reply_message->apply)));
    }

    /* Prepend with the message type and send it. */
    buf[0] = 'r';
    rc = memcpy_s(&buf[1], sizeof(DCFStandbyReplyMessage), t_thrd.dcf_cxt.dcfCtxInfo->dcf_reply_message,
                  sizeof(DCFStandbyReplyMessage));
    securec_check(rc, "\0", "\0");

    bool sent = DCFSendMsg(1, leaderID, buf, sizeof(DCFStandbyReplyMessage) + 1);
    if (!sent) {
        ereport(WARNING, (errmsg("DCF failed to send message!")));
    }
    return sent;
}

/* Report shared memory space needed by DcfContextShmemInit */
Size DcfContextShmemSize(void)
{
    Size size = 0;

    size = add_size(size, sizeof(DcfContextInfo));

    return size;
}

/* Allocate and initialize dcf context info shared memory */
void DcfContextShmemInit(void)
{
    bool found = false;
    t_thrd.dcf_cxt.dcfCtxInfo = (DcfContextInfo *)ShmemInitStruct("Dcf Conext Infos", DcfContextShmemSize(), &found);

    if (!found) {
        errno_t rc = 0;
        /* First time through, so initialize */
        rc = memset_s(t_thrd.dcf_cxt.dcfCtxInfo, DcfContextShmemSize(), 0, DcfContextShmemSize());
        securec_check(rc, "", "");
        t_thrd.dcf_cxt.dcfCtxInfo->isDcfStarted = false;
        SpinLockInit(&t_thrd.dcf_cxt.dcfCtxInfo->dcfStartedMutex);

        t_thrd.dcf_cxt.dcfCtxInfo->isWalRcvReady = false;
        t_thrd.dcf_cxt.dcfCtxInfo->isRecordIdxBlocked = false;
        SpinLockInit(&t_thrd.dcf_cxt.dcfCtxInfo->recordDcfIdxMutex);
        t_thrd.dcf_cxt.dcfCtxInfo->recordLsn = 0;
        t_thrd.dcf_cxt.dcfCtxInfo->dcfRecordIndex = 0;
        t_thrd.dcf_cxt.dcfCtxInfo->appliedLsn = 0;
        t_thrd.dcf_cxt.dcfCtxInfo->truncateDcfIndex = 0;
        t_thrd.dcf_cxt.dcfCtxInfo->dcf_to_be_leader = false;
        t_thrd.dcf_cxt.dcfCtxInfo->dcf_build_done = false;
        t_thrd.dcf_cxt.dcfCtxInfo->dcf_need_build_set = false;
        t_thrd.dcf_cxt.dcfCtxInfo->dcfNeedSyncConfig = false;
        /* do not palloc it under walreceiver thread, or it will be released when thread exist */
        t_thrd.dcf_cxt.dcfCtxInfo->reply_modify_message = 
            static_cast<ConfigModifyTimeMessage*>(palloc0(sizeof(ConfigModifyTimeMessage)));
        t_thrd.dcf_cxt.dcfCtxInfo->dcf_reply_message =
            (DCFStandbyReplyMessage*)palloc0(sizeof(DCFStandbyReplyMessage));
    }
    /* Now dcfCtxInfo has been assigned the existed one or a new one */
    t_thrd.dcf_cxt.isDcfShmemInited = true;
}

void InitAppliedIndex(void)
{
    /* Read paxos index from file if the file exists */
    const int PAXOS_INDEX_FILE_NUM = 2;
    char paxos_index_files[PAXOS_INDEX_FILE_NUM][MAXPGPATH] = {0};
    int ret = snprintf_s(paxos_index_files[0], MAXPGPATH, MAXPGPATH - 1, "%s/paxosindex", t_thrd.proc_cxt.DataDir);
    securec_check_ss_c(ret, "\0", "\0");
    ret = snprintf_s(paxos_index_files[1], MAXPGPATH, MAXPGPATH - 1, "%s/paxosindex.backup", t_thrd.proc_cxt.DataDir);
    securec_check_ss_c(ret, "\0", "\0");
    FILE* paxos_index_fd = NULL;
    DCFData* dcfData = t_thrd.shemem_ptr_cxt.dcfData;
    pg_crc32c crc;
    for (int i = 0; i < PAXOS_INDEX_FILE_NUM; i++) {
        char *paxos_index_file = paxos_index_files[i];
        paxos_index_fd = fopen(paxos_index_file, "rb");
        if (paxos_index_fd == NULL) {
            ereport(FATAL, (errmodule(MOD_DCF),
                            errcode_for_file_access(),
                            errmsg("Open paxos index file %s failed: %m!", paxos_index_file)));
        }
        if (fread(dcfData, sizeof(DCFData), 1, paxos_index_fd) != 1) {
            ereport(PANIC, (errmodule(MOD_DCF),
                            errcode_for_file_access(),
                            errmsg("Read paxos index file %s failed: %m!", paxos_index_file)));
        }
        if (fclose(paxos_index_fd)) {
            ereport(PANIC, (errmodule(MOD_DCF),
                            errcode_for_file_access(),
                            errmsg("Close paxos indes file %s failed: %m!", paxos_index_file)));
        }
        paxos_index_fd = NULL;
        INIT_CRC32C(crc);
        COMP_CRC32C(crc, (char *)dcfData, offsetof(DCFData, crc));
        FIN_CRC32(crc);
        if (!EQ_CRC32C(crc, dcfData->crc)) {
            if (i != PAXOS_INDEX_FILE_NUM - 1) {
                ereport(WARNING, (errmodule(MOD_DCF),
                                  errmsg("incorrect checksum in paxos index file: \"%s\" and try backup.",
                                         paxos_index_file)));
                continue;
            } else {
                ereport(FATAL, (errmodule(MOD_DCF),
                                errmsg("incorrect checksum in paxos index file: \"%s\".", paxos_index_file)));
            }
        }
        if (dcfData->dcfDataVersion != DCF_DATA_VERSION) {
            ereport(FATAL, (errmodule(MOD_DCF),
                            errmsg("DCF data version is incompatible with server"),
                            errdetail("The database cluster was initialized with DCF data version %u,"
                                      " but the server was compiled with DCF data version %u.",
                                      dcfData->dcfDataVersion, DCF_DATA_VERSION)));
        }
        ereport(LOG, (errmodule(MOD_DCF),
                      errmsg("DCF data version, applied index and min applied index read from %s is %u, %lu and %lu.",
                             paxos_index_file, dcfData->dcfDataVersion,
                             dcfData->appliedIndex, dcfData->realMinAppliedIdx)));
        /* Set the position that DCF sync log */
        if (dcf_set_applied_index(1, dcfData->appliedIndex) != 0) {
            ereport(PANIC,
                    (errmodule(MOD_DCF),
                     errmsg("Failed to set applied index %lu, which is read from file %s.",
                            dcfData->appliedIndex,
                            paxos_index_file)));
        }
        return;
    }
    ereport(PANIC, (errmodule(MOD_DCF),
                    errmsg("Read paxos index failed from all files!")));
    return;
}

bool SaveAppliedIndex(void)
{
    errno_t err = EOK;
    const int PAXOS_INDEX_FILE_NUM = 2;
    char paxos_index_files[PAXOS_INDEX_FILE_NUM][MAXPGPATH] = {0};

    int ret = snprintf_s(paxos_index_files[0], MAXPGPATH, MAXPGPATH - 1, "%s/paxosindex.backup", t_thrd.proc_cxt.DataDir);
    securec_check_ss_c(ret, "\0", "\0");
    ret = snprintf_s(paxos_index_files[1], MAXPGPATH, MAXPGPATH - 1, "%s/paxosindex", t_thrd.proc_cxt.DataDir);
    securec_check_ss_c(ret, "\0", "\0");
    int paxos_index_fd = -1;
    DCFData dcfDataCopy;
    int len = sizeof(DCFData);
    err = memcpy_s(&dcfDataCopy, len, t_thrd.shemem_ptr_cxt.dcfData, len);
    securec_check(err, "\0", "\0");
    INIT_CRC32C(dcfDataCopy.crc);
    COMP_CRC32C(dcfDataCopy.crc, (char *)&dcfDataCopy, offsetof(DCFData, crc));
    FIN_CRC32C(dcfDataCopy.crc);
    for (int i = 0; i < PAXOS_INDEX_FILE_NUM; i++) {
        char *paxos_index_file = paxos_index_files[i];
        paxos_index_fd = open(paxos_index_file, O_CREAT | O_RDWR | PG_BINARY, S_IRUSR | S_IWUSR);
        if (paxos_index_fd < 0) {
            ereport(FATAL, (errmodule(MOD_DCF),
                            errcode_for_file_access(),
                            errmsg("Open paxos index file %s failed: %m!", paxos_index_file)));
        }
        if ((write(paxos_index_fd, &dcfDataCopy, len)) != len) {
            close(paxos_index_fd);
            ereport(PANIC, (errmodule(MOD_DCF),
                            errcode_for_file_access(),
                            errmsg("Write paxos index into %s failed: %m!", paxos_index_file)));
        }
        if (fsync(paxos_index_fd)) {
            close(paxos_index_fd);
            ereport(PANIC, (errmodule(MOD_DCF),
                            errcode_for_file_access(), errmsg("could not fsync dcf paxos index file: %m")));
        }

        if (close(paxos_index_fd)) {
            ereport(PANIC, (errmodule(MOD_DCF),
                            errcode_for_file_access(), errmsg("could not close dcf paxos index file: %m")));
        }
        ereport(LOG, (errmodule(MOD_DCF),
                      errmsg("Write dcfData version %u, apply index %lu, min apply index %lu and crc %u into \"%s\"",
                             dcfDataCopy.dcfDataVersion, dcfDataCopy.appliedIndex,
                             dcfDataCopy.realMinAppliedIdx, dcfDataCopy.crc, paxos_index_file)));
    }
    return true;
}

void SetDcfParam(const char* dcfParamName, const char* dcfParamValue)
{
    if (dcf_set_param(dcfParamName, dcfParamValue) != 0)
        ereport(WARNING, (errmsg("Failed to set DCF %s: %s.", 
                                 dcfParamName, dcfParamValue)));
}

void InitDcfSSL()
{
    char* parentdir = NULL;
    KeyMode keymode = SERVER_MODE;
    if (is_absolute_path(g_instance.attr.attr_security.ssl_key_file)) {
        parentdir = pstrdup(g_instance.attr.attr_security.ssl_key_file);
        get_parent_directory(parentdir);
        decode_cipher_files(keymode, NULL, parentdir, u_sess->libpq_cxt.server_key);
    } else {
        decode_cipher_files(keymode, NULL, t_thrd.proc_cxt.DataDir, u_sess->libpq_cxt.server_key);
        parentdir = pstrdup(t_thrd.proc_cxt.DataDir);
    }
    pfree_ext(parentdir);

    /* never give a change to log it */
    dcf_set_param("SSL_PWD_PLAINTEXT", reinterpret_cast<char*>(u_sess->libpq_cxt.server_key));
    /* clear the sensitive info in server_key */
    errno_t errorno = EOK;
    errorno = memset_s(u_sess->libpq_cxt.server_key, CIPHER_LEN + 1, 0, CIPHER_LEN + 1);
    securec_check(errorno, "\0", "\0");

    char ssl_file_path[PATH_MAX + 1] = {0};
    if (NULL != realpath(g_instance.attr.attr_security.ssl_ca_file, ssl_file_path))
        SetDcfParam("SSL_CA", ssl_file_path);

    errorno = memset_s(ssl_file_path, PATH_MAX + 1, 0, PATH_MAX + 1);
    securec_check(errorno, "\0", "\0");

    if (NULL != realpath(g_instance.attr.attr_security.ssl_key_file, ssl_file_path))
        SetDcfParam("SSL_KEY", ssl_file_path);

    errorno = memset_s(ssl_file_path, PATH_MAX + 1, 0, PATH_MAX + 1);
    securec_check(errorno, "\0", "\0");

    if (NULL != realpath(g_instance.attr.attr_security.ssl_crl_file, ssl_file_path))
        SetDcfParam("SSL_CRL", ssl_file_path);

    errorno = memset_s(ssl_file_path, PATH_MAX + 1, 0, PATH_MAX + 1);
    securec_check(errorno, "\0", "\0");

    if (NULL != realpath(g_instance.attr.attr_security.ssl_cert_file, ssl_file_path))
        SetDcfParam("SSL_CERT", ssl_file_path);

    errorno = memset_s(ssl_file_path, PATH_MAX + 1, 0, PATH_MAX + 1);
    securec_check(errorno, "\0", "\0");

    /* to limit line width */
    int dcf_guc_param = 0;

    dcf_guc_param = u_sess->attr.attr_security.ssl_cert_notify_time;
    SetDcfParam("SSL_CERT_NOTIFY_TIME", std::to_string(dcf_guc_param).c_str());

    /* set dcf ssl_cipher to TLS1.2 */
    SetDcfParam("SSL_CIPHER",
        "ECDHE-ECDSA-AES256-GCM-SHA384:"
        "ECDHE-ECDSA-AES128-GCM-SHA256:"
        "ECDHE-RSA-AES256-GCM-SHA384:"
        "ECDHE-RSA-AES128-GCM-SHA256:");
}

bool SetDcfParams()
{
    /* set param for DCF */
    if (dcf_set_param("DATA_PATH", g_instance.attr.attr_storage.dcf_attr.dcf_data_path) != 0) {
        /* data path is neccessary to DCF, thus report WARNING and retry. */
        ereport(WARNING, (errmsg("Failed to set DCF data path: %s.",
                                 g_instance.attr.attr_storage.dcf_attr.dcf_data_path)));
        return false;
    }

    SetDcfParam("LOG_PATH", g_instance.attr.attr_storage.dcf_attr.dcf_log_path);

    /* Init DCF SSL failed is not a PANIC */
    #ifdef USE_SSL
    if (g_instance.attr.attr_storage.dcf_attr.dcf_ssl) {
        InitDcfSSL();
    }
    #endif

    SetDcfParam("LOG_LEVEL", u_sess->attr.attr_storage.dcf_attr.dcf_log_level);

    SetDcfParam("LOG_FILENAME_FORMAT", "1");

    /* to limit line width */
    uint64_t dcf_guc_param = 0;

    dcf_guc_param = u_sess->attr.attr_storage.dcf_attr.dcf_election_timeout;
    SetDcfParam("ELECTION_TIMEOUT", std::to_string(dcf_guc_param).c_str());

    dcf_guc_param = u_sess->attr.attr_storage.dcf_attr.dcf_run_mode;
    SetDcfParam("RUN_MODE", std::to_string(dcf_guc_param).c_str());

    dcf_guc_param = u_sess->attr.attr_storage.dcf_attr.dcf_max_log_file_size;
    SetDcfParam("MAX_LOG_FILE_SIZE", std::to_string(dcf_guc_param).c_str());

    dcf_guc_param = u_sess->attr.attr_storage.dcf_attr.dcf_flow_control_cpu_threshold;
    SetDcfParam("FLOW_CONTROL_CPU_THRESHOLD", std::to_string(dcf_guc_param).c_str());

    dcf_guc_param = u_sess->attr.attr_storage.dcf_attr.dcf_flow_control_net_queue_message_num_threshold;
    SetDcfParam("FLOW_CONTROL_NET_QUEUE_MESSAGE_NUM_THRESHOLD", std::to_string(dcf_guc_param).c_str());

    dcf_guc_param = u_sess->attr.attr_storage.dcf_attr.dcf_flow_control_disk_rawait_threshold;
    SetDcfParam("FLOW_CONTROL_DISK_RAWAIT_THRESHOLD", std::to_string(dcf_guc_param).c_str());

    dcf_guc_param = u_sess->attr.attr_storage.dcf_attr.dcf_log_backup_file_count;
    SetDcfParam("LOG_BACKUP_FILE_COUNT", std::to_string(dcf_guc_param).c_str());

    dcf_guc_param = g_instance.attr.attr_storage.dcf_attr.dcf_log_file_permission;
    int permission = 0;
    if (dcf_guc_param == DCF_LOG_FILE_PERMISSION_600) {
        permission = 600;
    } else if (dcf_guc_param == DCF_LOG_FILE_PERMISSION_640) {
        permission = 640;
    }
    SetDcfParam("LOG_FILE_PERMISSION", std::to_string(permission).c_str());

    dcf_guc_param = g_instance.attr.attr_storage.dcf_attr.dcf_log_path_permission;
    if (dcf_guc_param == DCF_LOG_PATH_PERMISSION_700) {
        permission = 700;
    } else if (dcf_guc_param == DCF_LOG_PATH_PERMISSION_750) {
        permission = 750;
    }
    SetDcfParam("LOG_PATH_PERMISSION", std::to_string(permission).c_str());

    dcf_guc_param = g_instance.attr.attr_storage.dcf_attr.dcf_mec_agent_thread_num;
    SetDcfParam("MEC_AGENT_THREAD_NUM", std::to_string(dcf_guc_param).c_str());

    dcf_guc_param = g_instance.attr.attr_storage.dcf_attr.dcf_mec_reactor_thread_num;
    SetDcfParam("MEC_REACTOR_THREAD_NUM", std::to_string(dcf_guc_param).c_str());

    dcf_guc_param = g_instance.attr.attr_storage.dcf_attr.dcf_mec_channel_num;
    SetDcfParam("MEC_CHANNEL_NUM", std::to_string(dcf_guc_param).c_str());

    dcf_guc_param = g_instance.attr.attr_storage.dcf_attr.dcf_mem_pool_init_size;
    SetDcfParam("MEM_POOL_INIT_SIZE", std::to_string(dcf_guc_param).c_str());

    dcf_guc_param = g_instance.attr.attr_storage.dcf_attr.dcf_mem_pool_max_size;
    SetDcfParam("MEM_POOL_MAX_SIZE", std::to_string(dcf_guc_param).c_str());

    dcf_guc_param = g_instance.attr.attr_storage.dcf_attr.dcf_compress_algorithm;
    SetDcfParam("COMPRESS_ALGORITHM", std::to_string(dcf_guc_param).c_str());

    dcf_guc_param = g_instance.attr.attr_storage.dcf_attr.dcf_compress_level;
    SetDcfParam("COMPRESS_LEVEL", std::to_string(dcf_guc_param).c_str());

    dcf_guc_param = g_instance.attr.attr_storage.dcf_attr.dcf_socket_timeout;
    SetDcfParam("SOCKET_TIMEOUT", std::to_string(dcf_guc_param).c_str());

    dcf_guc_param = g_instance.attr.attr_storage.dcf_attr.dcf_connect_timeout;
    SetDcfParam("CONNECT_TIMEOUT", std::to_string(dcf_guc_param).c_str());

    dcf_guc_param = g_instance.attr.attr_storage.dcf_attr.dcf_rep_append_thread_num;
    SetDcfParam("REP_APPEND_THREAD_NUM", std::to_string(dcf_guc_param).c_str());

    dcf_guc_param = g_instance.attr.attr_storage.dcf_attr.dcf_mec_fragment_size;
    SetDcfParam("MEC_FRAGMENT_SIZE", std::to_string(dcf_guc_param).c_str());

    dcf_guc_param = g_instance.attr.attr_storage.dcf_attr.dcf_stg_pool_init_size;
    SetDcfParam("STG_POOL_INIT_SIZE", std::to_string(dcf_guc_param).c_str());

    dcf_guc_param = g_instance.attr.attr_storage.dcf_attr.dcf_stg_pool_max_size;
    SetDcfParam("STG_POOL_MAX_SIZE", std::to_string(dcf_guc_param).c_str());

    dcf_guc_param = g_instance.attr.attr_storage.dcf_attr.dcf_mec_pool_max_size;
    SetDcfParam("MEC_POOL_MAX_SIZE", std::to_string(dcf_guc_param).c_str());

    dcf_guc_param = g_instance.attr.attr_storage.dcf_attr.dcf_mec_batch_size;
    SetDcfParam("MEC_BATCH_SIZE", std::to_string(dcf_guc_param).c_str());
    return true;
}

bool InitDcfAndStart() 
{
    ResetDCFNodesInfo();
    InitAppliedIndex();
    ereport(LOG,
            (errmsg("Before start DCF module, node_id = %d, dcf_config = %s",
                    g_instance.attr.attr_storage.dcf_attr.dcf_node_id,
                    g_instance.attr.attr_storage.dcf_attr.dcf_config)));
    if (dcf_start(g_instance.attr.attr_storage.dcf_attr.dcf_node_id, 
                  g_instance.attr.attr_storage.dcf_attr.dcf_config) != 0) {
        ereport(WARNING, (errmsg("Failed to start DCF module.")));
        return false;
    }
    ereport(LOG, (errmsg("Start DCF module success.")));
    return true;
}

static bool RegisterDcfCallBacks()
{
    if (dcf_register_after_writer(ConsensusLogCbFunc) != 0) {
        ereport(WARNING, (errmsg("Failed to register ConsensusLogCbFunc.")));
        return false;
    }
    if (dcf_register_consensus_notify(ReceiveLogCbFunc) != 0) {
        ereport(WARNING, (errmsg("Failed to register ReceiveLogCbFunc.")));
        return false;
    }
    if (dcf_register_status_notify(PromoteOrDemote) != 0) {
        ereport(WARNING, (errmsg("Failed to register PromoteOrDemote.")));
        return false;
    }
    if (dcf_register_exception_report(DCFExceptionCbFunc) != 0) {
        ereport(WARNING, (errmsg("Failed to register DCFExceptionCbFunc.")));
        return false;
    }
    if (dcf_register_election_notify(ElectionCbFunc) != 0) {
        ereport(WARNING, (errmsg("Failed to register ElectionCbFunc.")));
        return false;
    }
    if (dcf_register_msg_proc(ProcessMsgCbFunc) != 0) {
        ereport(WARNING, (errmsg("Failed to register ProcessMsgCbFunc.")));
        return false;
    }
    if (dcf_register_thread_memctx_init(DcfThreadShmemInit) != 0) {
        ereport(WARNING, (errmsg("Failed to register DcfThreadShmemInit.")));
        return false;
    }
    return true;
}

void SetThrdLocals()
{
    int nRet = 0;
    t_thrd.dcf_cxt.dcfCtxInfo->dcf_to_be_leader = false;
    t_thrd.dcf_cxt.dcfCtxInfo->dcf_build_done = false;
    t_thrd.dcf_cxt.dcfCtxInfo->dcf_need_build_set = false;
    t_thrd.dcf_cxt.dcfCtxInfo->last_sendfilereply_timestamp = GetCurrentTimestamp();
    t_thrd.dcf_cxt.dcfCtxInfo->check_file_timeout = DCF_CHECK_CONF_IDLE;
    t_thrd.dcf_cxt.dcfCtxInfo->standby_config_modify_time = time(NULL);
    t_thrd.dcf_cxt.dcfCtxInfo->Primary_config_modify_time = 0;
    if (t_thrd.proc_cxt.DataDir) {
        nRet = snprintf_s(t_thrd.dcf_cxt.dcfCtxInfo->gucconf_file,
                          MAXPGPATH, MAXPGPATH - 1, "%s/postgresql.conf",
                          t_thrd.proc_cxt.DataDir);
        securec_check_ss(nRet, "\0", "\0");

        nRet = snprintf_s(t_thrd.dcf_cxt.dcfCtxInfo->temp_guc_conf_file,
                          MAXPGPATH, MAXPGPATH - 1, "%s/%s",
                          t_thrd.proc_cxt.DataDir, TEMP_CONF_FILE);
        securec_check_ss(nRet, "\0", "\0");

        nRet = snprintf_s(t_thrd.dcf_cxt.dcfCtxInfo->bak_guc_conf_file,
                          MAXPGPATH, MAXPGPATH - 1, "%s/%s",
                          t_thrd.proc_cxt.DataDir, CONFIG_BAK_FILENAME);
        securec_check_ss(nRet, "\0", "\0");

        nRet = snprintf_s(t_thrd.dcf_cxt.dcfCtxInfo->gucconf_lock_file,
                          MAXPGPATH, MAXPGPATH - 1, "%s/postgresql.conf.lock",
                          t_thrd.proc_cxt.DataDir);
        securec_check_ss(nRet, "\0", "\0");
    }
}

/* Initialize paxos module */
bool InitPaxosModule()
{
    /* register call back functions for DN in DCF */
    if (!RegisterDcfCallBacks()) {
        return false;
    }
    /* init params for dcf */
    if(!SetDcfParams()) {
        return false;
    }
    /* set thread local variables */
    SetThrdLocals();
    /* start dcf module */
    if(!InitDcfAndStart()) {
        return false;
    }
    return true;
}

void UpdateRecordIdxState()
{
    XLogRecPtr restartRequestPtr = GetXLogReplayRecPtr(nullptr);
    /* According to RequestXLogStreaming */
    if (restartRequestPtr % XLogSegSize != 0) {
        restartRequestPtr -= restartRequestPtr % XLogSegSize;
    } else {
        XLogSegNo _logSeg;
        XLByteToSeg(restartRequestPtr, _logSeg);
        _logSeg--;
        restartRequestPtr = _logSeg * XLogSegSize;
    }
    volatile DcfContextInfo* dcfCtx = t_thrd.dcf_cxt.dcfCtxInfo;
    SpinLockAcquire(&dcfCtx->recordDcfIdxMutex);
    /* 
     * Different to XLogFlushCore, only after redo a whole xlog segment, 
     * it's safe to set it applied in DCF.
     */
    dcfCtx->isRecordIdxBlocked = !XLByteLE(dcfCtx->recordLsn, restartRequestPtr);
    SpinLockRelease(&dcfCtx->recordDcfIdxMutex);
}

/* rewind index to align a valid xlog page boundary, before which xlog records have been replayed */
void RewindDcfIndex()
{
    Assert(t_thrd.shemem_ptr_cxt.dcfData->appliedIndex != 0);
    bool set_ret = (dcf_set_applied_index(1, t_thrd.shemem_ptr_cxt.dcfData->appliedIndex) == 0);
    ereport(LOG, (errmsg("Set applied index %lu with ret %d, appliedLsn is %lu.",
                         t_thrd.shemem_ptr_cxt.dcfData->appliedIndex,
                         set_ret,
                         t_thrd.dcf_cxt.dcfCtxInfo->appliedLsn)));
}

void LaunchPaxos()
{
    volatile DcfContextInfo* dcfCtx = t_thrd.dcf_cxt.dcfCtxInfo;
    if(dcfCtx == NULL) {
        ereport(FATAL, (errmsg("dcf context info is null, please init it.")));
    }

    SpinLockAcquire(&dcfCtx->dcfStartedMutex);
    if (!dcfCtx->isDcfStarted) {
        /* Set it after walreceiver is ready and before dcf start. */
        t_thrd.dcf_cxt.dcfCtxInfo->isWalRcvReady = true;
        dcfCtx->isDcfStarted = InitPaxosModule();
    } else {
        RewindDcfIndex();
        /* Set it after walreceiver is ready and dcf index rewound. */
        t_thrd.dcf_cxt.dcfCtxInfo->isWalRcvReady = true;
    }
    if (!dcfCtx->isDcfStarted) {
        SpinLockRelease(&dcfCtx->dcfStartedMutex);
        ereport(FATAL, (errmsg("Failed to Init DCF.")));
    }
    SpinLockRelease(&dcfCtx->dcfStartedMutex);

    Assert(dcfCtx->isDcfStarted);
    /* Synchronize standby's configure file once the HA build successfully. */
    CheckConfigFile(true);
}

/*
 * Synchronise standby's configure file once the HA build successfully.
 */
void firstSynchStandbyFile(uint32 leader_id)
{
    errno_t errorno = EOK;
    char bufTime[sizeof(ConfigModifyTimeMessage) + 1];

    bufTime[0] = 'A';
    t_thrd.dcf_cxt.dcfCtxInfo->reply_modify_message->config_modify_time = 0;
    errorno = memcpy_s(&bufTime[1], sizeof(ConfigModifyTimeMessage),
                       t_thrd.dcf_cxt.dcfCtxInfo->reply_modify_message,
                       sizeof(ConfigModifyTimeMessage));
    securec_check(errorno, "\0", "\0");

    if (dcf_send_msg(1, leader_id, bufTime, sizeof(ConfigModifyTimeMessage) + 1) != 0) {
        ereport(WARNING,
                (errmsg("DCF follower failed to send ConfigModifyTimeMessage to leader %u.", leader_id)));
    }
    t_thrd.dcf_cxt.dcfCtxInfo->last_sendfilereply_timestamp = GetCurrentTimestamp();
}

/*
 * we check the configure file every check_file_timeout, if
 * the configure has been modified, send the modify time to standy.
 */
void ConfigFileTimer(uint32 leader_id)
{
    struct stat statbuf;
    char bufTime[sizeof(ConfigModifyTimeMessage) + 1];
    TimestampTz nowTime;

    if (t_thrd.dcf_cxt.dcfCtxInfo->check_file_timeout > 0) {
        nowTime = GetCurrentTimestamp();
        if (TimestampDifferenceExceeds(t_thrd.dcf_cxt.dcfCtxInfo->last_sendfilereply_timestamp, nowTime,
                                       t_thrd.dcf_cxt.dcfCtxInfo->check_file_timeout) ||
            TimestampDifferenceExceeds(nowTime, t_thrd.dcf_cxt.dcfCtxInfo->last_sendfilereply_timestamp,
                                       t_thrd.dcf_cxt.dcfCtxInfo->check_file_timeout)) {
            errno_t errorno = EOK;
            ereport(LOG, (errmsg("time is up to send file")));
            if (lstat(t_thrd.dcf_cxt.dcfCtxInfo->gucconf_file, &statbuf) != 0) {
                if (errno != ENOENT) {
                    ereport(ERROR, (errcode_for_file_access(),
                                    errmsg("could not stat file or directory \"%s\": %m",
                                           t_thrd.dcf_cxt.dcfCtxInfo->gucconf_file)));
                }
            }
            /* the configure file in standby has been change yet. */
            if (t_thrd.dcf_cxt.dcfCtxInfo->standby_config_modify_time != statbuf.st_mtime) {
                ereport(LOG,
                        (errmsg("statbuf.st_mtime:%d is not equal to config_modify_time:%d",
                                static_cast<int>(statbuf.st_mtime),
                                static_cast<int>(t_thrd.dcf_cxt.dcfCtxInfo->standby_config_modify_time))));
                t_thrd.dcf_cxt.dcfCtxInfo->reply_modify_message->config_modify_time = 0;
            } else {
                ereport(LOG,
                        (errmsg("the config file of standby has no change:%d",
                                static_cast<int>(statbuf.st_mtime))));
                t_thrd.dcf_cxt.dcfCtxInfo->reply_modify_message->config_modify_time =
                    t_thrd.dcf_cxt.dcfCtxInfo->Primary_config_modify_time;
            }
            bufTime[0] = 'A';
            errorno = memcpy_s(&bufTime[1], sizeof(ConfigModifyTimeMessage),
                               t_thrd.dcf_cxt.dcfCtxInfo->reply_modify_message,
                               sizeof(ConfigModifyTimeMessage));
            securec_check(errorno, "\0", "\0");

            if (dcf_send_msg(1, leader_id, bufTime, sizeof(ConfigModifyTimeMessage) + 1) != 0) {
                ereport(WARNING,
                        (errmsg("DCF follower failed to send ConfigModifyTimeMessage to leader %u.",
                                leader_id)));
            }
            /* save the current timestamp */
            t_thrd.dcf_cxt.dcfCtxInfo->last_sendfilereply_timestamp = GetCurrentTimestamp();
        }
    }
}

/* Check if need to sync config file from leader */
void CheckConfigFile(bool after_build)
{
    uint32 leader_id = 0;

    /* leader call follower call back when promoting, avoid check leader too frequently */
    if (t_thrd.dcf_cxt.dcfCtxInfo->dcf_to_be_leader)
        return;

    if (!QueryLeaderNodeInfo(&leader_id))
        return;

    if (leader_id == static_cast<uint32>(g_instance.attr.attr_storage.dcf_attr.dcf_node_id))
        return;
    
    if (after_build) {
        firstSynchStandbyFile(leader_id);
    } else {
        ConfigFileTimer(leader_id);
    }
}

void SetDcfNeedSyncConfig()
{
    if (g_instance.attr.attr_storage.dcf_attr.enable_dcf &&
        t_thrd.dcf_cxt.dcfCtxInfo != nullptr)
        t_thrd.dcf_cxt.dcfCtxInfo->dcfNeedSyncConfig = true;
}

void StopPaxosModule()
{
    if (t_thrd.dcf_cxt.dcfCtxInfo->isDcfStarted &&
        t_thrd.shemem_ptr_cxt.dcfData->appliedIndex != 0) {
        if (!SaveAppliedIndex())
            ereport(WARNING,
                    (errmsg("Failed to save paxosindex before stop DCF!")));
    }
    bool is_dcf_alive = false;
    SpinLockAcquire(&t_thrd.dcf_cxt.dcfCtxInfo->dcfStartedMutex);
    if (t_thrd.dcf_cxt.dcfCtxInfo->isDcfStarted) {
        is_dcf_alive = true;
        dcf_stop();
        t_thrd.dcf_cxt.dcfCtxInfo->isDcfStarted = false;
    }
    SpinLockRelease(&t_thrd.dcf_cxt.dcfCtxInfo->dcfStartedMutex);
    if (is_dcf_alive)
        ereport(LOG, (errmsg("stop DCF while shutting down XLOG")));
}

void DcfLogTruncate()
{
    volatile DcfContextInfo* dcfCtx = t_thrd.dcf_cxt.dcfCtxInfo;
    Assert(dcfCtx != nullptr);
    /* will never happen, prevent abuse this func */
    if (dcfCtx == NULL) {
        ereport(FATAL, (errmodule(MOD_DCF),
                        errmsg("failed to truncate dcf log, because dcf context is null.")));
    }
    /* prevent updated concurrently */
    uint64 flushedIdx = t_thrd.shemem_ptr_cxt.dcfData->appliedIndex;
    /* it has not been read from file yet */
    if (flushedIdx == 0)
        return;
    if (flushedIdx - dcfCtx->truncateDcfIndex >=
        static_cast<unsigned int>(u_sess->attr.attr_storage.dcf_attr.dcf_truncate_threshold)) {
        unsigned long long minAppliedIdx = 0;
        if (dcf_get_cluster_min_applied_idx(1, &minAppliedIdx) == 0) {
            if (minAppliedIdx > t_thrd.shemem_ptr_cxt.dcfData->realMinAppliedIdx) {
                t_thrd.shemem_ptr_cxt.dcfData->realMinAppliedIdx = minAppliedIdx;
            }
            bool saveSuccess = SaveAppliedIndex();
            if (!saveSuccess) {
                ereport(WARNING,
                        (errmodule(MOD_DCF),
                         errmsg("Failed to write paxosindex into paxosIndex file and don't truncate this time!")));
                return;
            }
            unsigned long long toTruncateIdx = Min(static_cast<uint64>(minAppliedIdx), flushedIdx);
            /*
             * One more DCF log entry should be kept
             * in case not continuous DCF log exception happened.
             */
            const unsigned long long minTruncateIdx = 2;
            if (toTruncateIdx < minTruncateIdx) {
                return;
            }
            toTruncateIdx -= 1;
            if (dcf_truncate(1, toTruncateIdx) == 0) {
                dcfCtx->truncateDcfIndex = toTruncateIdx;
            } else {
                ereport(WARNING,(errmodule(MOD_DCF),
                                 errmsg("Failed to truncate DCF log before index %lld.",
                                        toTruncateIdx)));
            }
        } else {
            ereport(WARNING, (errmodule(MOD_DCF), errmsg("Failed to get cluster min applied index.")));
        }
    }
}

/* Only membered active follower is legal */
static bool GetFollowerSyncRecPtr(uint32 nodeID, XLogRecPtr* receivePtr, XLogRecPtr* writePtr,
                                  XLogRecPtr* flushPtr, XLogRecPtr* replayPtr)
{
    bool found = false;
    for (uint64 i = 0; i < DCF_MAX_NODES; i++) {
        volatile DCFStandbyInfo *nodeinfo = &t_thrd.dcf_cxt.dcfCtxInfo->nodes_info[i];
        if (nodeinfo->isMember && nodeinfo->isActive && nodeinfo->nodeID == nodeID) {
            *receivePtr = nodeinfo->receive;
            *writePtr = nodeinfo->write;
            *flushPtr = nodeinfo->flush;
            *replayPtr = nodeinfo->apply;
            found = true;
            break;
        }
    }
    if (found)
        ereport(DEBUG1,
                (errmodule(MOD_DCF),
                 errmsg("DCF follower %u: receive %X/%X write %X/%X flush %X/%X apply %X/%X",
                        nodeID,
                        static_cast<uint32>(*receivePtr >> 32), static_cast<uint32>(*receivePtr),
                        static_cast<uint32>(*writePtr >> 32), static_cast<uint32>(*writePtr),
                        static_cast<uint32>(*flushPtr >> 32), static_cast<uint32>(*flushPtr),
                        static_cast<uint32>(*replayPtr >> 32), static_cast<uint32>(*replayPtr))));
    return found;
}

/* check if there is any follower alive */
static bool ArchChooseFollower(XLogRecPtr targetLsn)
{
    XLogRecPtr receivePtr;
    XLogRecPtr writePtr;
    XLogRecPtr flushPtr;
    XLogRecPtr replayPtr;
    uint32 nodeID = t_thrd.arch.sync_follower_id;

    /* Assert dcf node id > 0 */
    if (nodeID > 0 &&
        GetFollowerSyncRecPtr(nodeID, &receivePtr, &writePtr, &flushPtr, &replayPtr) &&
        XLogRecPtrIsValid(flushPtr) && XLByteLE(targetLsn, flushPtr)) {
        return true;
    }

    for (uint64 i = 0; i < DCF_MAX_NODES; i++) {
        volatile DCFStandbyInfo *nodeinfo = &t_thrd.dcf_cxt.dcfCtxInfo->nodes_info[i];
        if (nodeinfo->isMember && nodeinfo->isActive && XLByteLE(targetLsn, nodeinfo->flush)) {
            ArchiveTaskStatus *archive_status = nullptr;
            archive_status = find_archive_task_status(t_thrd.arch.slot_name);
            if (archive_status == nullptr) {
                ereport(ERROR,
                        (errmsg("ArchChooseFollower has change from %d to %d, but not find slot",
                                nodeID, nodeinfo->nodeID)));
            }
            t_thrd.arch.sync_follower_id = nodeinfo->nodeID;
            archive_status->sync_walsender_term++;
            ereport(LOG,
                    (errmsg("ArchChooseFollower has change from %d to %d , sub_term:%d",
                            nodeID, nodeinfo->nodeID, archive_status->sync_walsender_term)));
            return true;
        }
    }
    return false;
}

static void SetFollowerInactive(uint32 nodeID)
{
    for (uint64 i = 0; i < DCF_MAX_NODES; i++) {
        volatile DCFStandbyInfo *nodeinfo = &t_thrd.dcf_cxt.dcfCtxInfo->nodes_info[i];
        if (nodeinfo->isMember && nodeinfo->nodeID == nodeID) {
            nodeinfo->isActive = false;
            ereport(WARNING, (errmsg("Set DCF follower %u Inactive.", nodeID)));
            break;
        }
    }
}

/* send archive xlog command */
static void DcfSndArchiveXlog(ArchiveXlogMessage *archive_message)
{
    errno_t errorno = EOK;
    ereport(LOG,
            (errmsg("%s : DcfSndArchiveXlog %X/%X to follower %u", archive_message->slot_name,
                    static_cast<uint32>(archive_message->targetLsn >> 32),
                    static_cast<uint32>(archive_message->targetLsn),
                    t_thrd.arch.sync_follower_id)));

    char bufArchiveTask[sizeof(ArchiveXlogMessage) + 1];

    /* Prepend with the message type and send it. */
    bufArchiveTask[0] = 'a';
    errorno = memcpy_s(bufArchiveTask + 1,
                       sizeof(ArchiveXlogMessage),
                       archive_message,
                       sizeof(ArchiveXlogMessage));
    securec_check(errorno, "\0", "\0");
    bool sent = (dcf_send_msg(1, t_thrd.arch.sync_follower_id, bufArchiveTask, sizeof(ArchiveXlogMessage) + 1) == 0);
    if (!sent) {
        ereport(WARNING,
                (errmsg("DCF leader failed to send ArchiveXlogMessage to follower %u.",
                        t_thrd.arch.sync_follower_id)));
        SetFollowerInactive(t_thrd.arch.sync_follower_id);
    }
}

/* choose a follower to send archive command */
bool DcfArchiveRoachForPitrMaster(XLogRecPtr targetLsn)
{
    ArchiveTaskStatus *archive_task_status = NULL;
    archive_task_status = find_archive_task_status(&t_thrd.arch.archive_task_idx);
    if (archive_task_status == NULL) {
        return false;
    }

    archive_task_status->archive_task.targetLsn = targetLsn;
    archive_task_status->archive_task.tli = get_controlfile_timeline();
    archive_task_status->archive_task.term = Max(g_instance.comm_cxt.localinfo_cxt.term_from_file,
                                                 g_instance.comm_cxt.localinfo_cxt.term_from_xlog);
    /* subterm update when follower changed */
    int rc = memcpy_s(archive_task_status->archive_task.slot_name, NAMEDATALEN, t_thrd.arch.slot_name, NAMEDATALEN);
    securec_check(rc, "\0", "\0");
    ResetLatch(&t_thrd.arch.mainloop_latch);
    ereport(LOG,
            (errmsg("%s : DcfArchiveRoachForPitrMaster %X/%X",
                    t_thrd.arch.slot_name, static_cast<uint32>(targetLsn >> 32),
                    static_cast<uint32>(targetLsn))));
    bool selected = ArchChooseFollower(targetLsn);
    if (!selected) {
        ereport(WARNING,
                (errmsg("DcfArchiveRoachForPitrMaster failed for no health follower %X/%X",
                        static_cast<uint32>(targetLsn >> 32), static_cast<uint32>(targetLsn))));
        return false;
    }
    archive_task_status->archiver_latch = &t_thrd.arch.mainloop_latch;
    /* send archive task and wait on latch */
    DcfSndArchiveXlog(&archive_task_status->archive_task);
    rc = WaitLatch(&t_thrd.arch.mainloop_latch,
                   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
                   static_cast<long>(t_thrd.arch.task_wait_interval));
    if (rc & WL_POSTMASTER_DEATH)
        gs_thread_exit(1);
    if (rc & WL_TIMEOUT)
        return false;

    /* check targetLsn for deal message with wrong order */
    if (archive_task_status->pitr_finish_result == true &&
        XLByteEQ(archive_task_status->archive_task.targetLsn, targetLsn)) {
        archive_task_status->pitr_finish_result = false;
        return true;
    } else {
        return false;
    }
}

/* send archive xlog response message to leader */
void DcfSendArchiveXlogResponse(ArchiveTaskStatus *archive_task_status)
{
    uint32 leader_id = 0;
    if (!QueryLeaderNodeInfo(&leader_id) ||
        leader_id == static_cast<unsigned int>(g_instance.attr.attr_storage.dcf_attr.dcf_node_id)) {
        return;
    }
    if (archive_task_status == nullptr) {
        return;
    }
    char buf[sizeof(ArchiveXlogResponseMessage) + 1];
    ArchiveXlogResponseMessage reply;
    errno_t errorno = EOK;
    reply.pitr_result = archive_task_status->pitr_finish_result;
    reply.targetLsn = archive_task_status->archive_task.targetLsn;
    errorno = memcpy_s(&reply.slot_name, NAMEDATALEN, archive_task_status->slotname, NAMEDATALEN);
    securec_check(errorno, "\0", "\0");

    buf[0] = 'a';
    errorno = memcpy_s(&buf[1],
                       sizeof(ArchiveXlogResponseMessage),
                       &reply,
                       sizeof(ArchiveXlogResponseMessage));
    securec_check(errorno, "\0", "\0");

    bool sent = (dcf_send_msg(1, leader_id, buf, sizeof(ArchiveXlogResponseMessage) + 1) == 0);

    ereport(LOG,
            (errmsg("DcfSendArchiveXlogResponse %s:%d %X/%X to leader %u with result %d",
                    reply.slot_name, reply.pitr_result,
                    static_cast<uint32>(reply.targetLsn >> 32),
                    static_cast<uint32>(reply.targetLsn), leader_id, sent)));
    volatile unsigned int *pitr_task_status = &archive_task_status->pitr_task_status;
    pg_memory_barrier();
    pg_atomic_write_u32(pitr_task_status, PITR_TASK_NONE);
}
#endif
