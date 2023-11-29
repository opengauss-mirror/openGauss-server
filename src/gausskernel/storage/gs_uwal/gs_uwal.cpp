/*
 * Copyright (c) 2023 China Unicom Co.,Ltd.
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
 * -------------------------------------------------------------------------
 *
 * gs_uwal.cpp
 *
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/gs_uwal/gs_uwal.cpp
 *
 * -------------------------------------------------------------------------
 */

#include <cstdlib>
#include "knl/knl_thread.h"
#include "knl/knl_session.h"
#include "replication/walsender_private.h"
#include "storage/gs_uwal/gs_uwal.h"
#include <access/xact.h>
#include <sys/signalfd.h>
#include <signal.h>
#include <unistd.h>
#include <pthread.h>

#define UWAL_PORT_LEN 8
#define UWAL_BYTE_SIZE_LEN 32
#define UWAL_INT_SIZE_LEN 8
#define UWAL_CERT_FILE_OK 1
#define UWAL_CERT_VERIFY_SUCCESS 1
#define UWAL_CERT_VERIFY_FAILED (-1)
#define UWAL_GET_KEY_PASS_SUCCESS 0

int ock_uwal_init(IN const char *path, IN const UwalCfgElem *elems, IN int cnt, IN const char *ulogPath);
void ock_uwal_exit(void);
int ock_uwal_create(IN UwalCreateParam *param, OUT UwalVector *uwals);
int ock_uwal_delete(IN UwalDeleteParam *param);
int ock_uwal_append(IN UwalAppendParam *param, OUT uint64_t *offset, OUT void *result);
int ock_uwal_read(IN UwalReadParam *param, OUT UwalBufferList *bufferList);
int ock_uwal_truncate(IN const UwalId *uwalId, IN uint64_t offset);
int ock_uwal_query(IN UwalQueryParam *param, OUT UwalInfo *info);
int ock_uwal_query_by_user(IN UwalUserType user, IN UwalRouteType route, OUT UwalVector *uwals);
int ock_uwal_set_rewind_point(IN UwalId *uwalId, IN uint64_t offset);
int ock_uwal_register_cert_verify_func(int32_t (*certVerify)(void *certStoreCtx, const char *crlPath),
                                       int32_t (*getKeyPass)(char *keyPassBuff, uint32_t keyPassBuffLen,
                                                             char *keyPassPath));
int32_t ock_uwal_notify_nodelist_change(NodeStateList *nodeList, FinishCbFun cb, void *ctx);
uint32_t ock_uwal_ipv4_inet_to_int(char ipv4[16UL]);
int uwal_init_symbols();
extern void comm_initialize_SSL();

int GsUwalLoadSymbols()
{
    return uwal_init_symbols();
}

static X509_CRL *ock_rpc_load_crl_file(const char *file)
{
    BIO *in;
    X509_CRL *crl;

    in = BIO_new(BIO_s_file());
    if (in == nullptr) {
        return nullptr;
    }

    if (BIO_read_filename(in, file) <= 0) {
        (void)BIO_free(in);
        return nullptr;
    }

    crl = PEM_read_bio_X509_CRL(in, nullptr, nullptr, nullptr);
    if (crl == nullptr) {
        (void)BIO_free(in);
        return nullptr;
    }

    (void)BIO_free(in);

    return crl;
}

static X509_CRL *load_cert_revoke_list_file(const char *crlFile)
{
    X509_CRL *crl;

    if (access(crlFile, 4) != 0) {  // 4 is for read access
        (void)fprintf(stderr, _("crlFile is not access, (%s).\n"), crlFile);
        return nullptr;
    }

    crl = ock_rpc_load_crl_file(crlFile);
    if (crl == nullptr) {
        (void)fprintf(stderr, _("failed to load cert revocation list(%s).\n"), crlFile);
        return nullptr;
    }

    return crl;
}

static int32_t get_expire_and_early_day_from_cert(X509 *cert)
{
    ASN1_TIME *asnExpireTime;
    ASN1_TIME *asnEarlyTime;

    asnExpireTime = X509_get_notAfter(cert);
    if (asnExpireTime == nullptr) {
        (void)fprintf(stderr, _("Failed to get expire time.\n"));

        return UWAL_CERT_VERIFY_FAILED;
    }

    if (X509_cmp_time(asnExpireTime, nullptr) == UWAL_CERT_VERIFY_FAILED) {
        return UWAL_CERT_VERIFY_FAILED;
    }

    asnEarlyTime = X509_get_notBefore(cert);
    if (asnEarlyTime == nullptr) {
        (void)fprintf(stderr, _("Failed to get early time.\n"));

        return UWAL_CERT_VERIFY_FAILED;
    }

    if (X509_cmp_time(asnEarlyTime, nullptr) == UWAL_CERT_VERIFY_FAILED) {
        return UWAL_CERT_VERIFY_FAILED;
    }

    return UWAL_CERT_VERIFY_SUCCESS;
}

int uwal_ock_rpc_verify_cert(void *x509)
{
    int32_t result;

    X509_STORE_CTX *x509ctx = (X509_STORE_CTX *)x509;

    result = X509_verify_cert(x509ctx);
    if (result != UWAL_CERT_FILE_OK) {
        result = X509_STORE_CTX_get_error(x509ctx);
        (void)fprintf(stderr, _("verify cert failed, ret(%d)\n"), result);
        return UWAL_CERT_VERIFY_FAILED;
    }

    X509 *cert = X509_STORE_CTX_get_current_cert(x509ctx);
    if (cert == nullptr) {
        (void)fprintf(stderr, _("get cert failed.\n"));
        return UWAL_CERT_VERIFY_FAILED;
    } else {
        result = get_expire_and_early_day_from_cert(cert);
        if (result != UWAL_CERT_VERIFY_SUCCESS) {
            (void)fprintf(stderr, _("certificate has been expired.\n"));
            return UWAL_CERT_VERIFY_FAILED;
        }
    }

    return UWAL_CERT_VERIFY_SUCCESS;
}

int32_t uwal_cert_verify(void *x509, const char *crlPath)
{
    int32 result;
    X509_STORE_CTX *x509ctx = (X509_STORE_CTX *)x509;

    if (crlPath == nullptr || crlPath[0] == '\0') {
        return uwal_ock_rpc_verify_cert(x509);
    }

    X509_CRL *crl = load_cert_revoke_list_file(crlPath);
    if (crl == nullptr) {
        (void)fprintf(stderr, _("load crl file failed, file(%s).\n"), crlPath);

        return uwal_ock_rpc_verify_cert(x509);
    }

    X509_STORE *x509Store = X509_STORE_CTX_get0_store(x509ctx);
    X509_STORE_CTX_set_flags(x509ctx, (unsigned long)X509_V_FLAG_CRL_CHECK);
    result = X509_STORE_add_crl(x509Store, crl);
    if (result != UWAL_CERT_FILE_OK) {
        (void)fprintf(stderr, _("store add crl failed , file(%s) ret(%d).\n"), crlPath, result);

        X509_CRL_free(crl);
        return UWAL_CERT_VERIFY_FAILED;
    }

    result = X509_verify_cert(x509ctx);
    if (result != UWAL_CERT_FILE_OK) {
        result = X509_STORE_CTX_get_error(x509ctx);
        (void)fprintf(stderr, _("verify cert file failed, ret(%d).\n"), result);
        X509_CRL_free(crl);
        return UWAL_CERT_VERIFY_FAILED;
    }

    X509_CRL_free(crl);

    X509 *cert = X509_STORE_CTX_get_current_cert(x509ctx);
    if (cert == nullptr) {
        (void)fprintf(stderr, _("get cert failed.\n"));
        return UWAL_CERT_VERIFY_FAILED;
    } else {
        result = get_expire_and_early_day_from_cert(cert);
        if (result != UWAL_CERT_VERIFY_SUCCESS) {
            (void)fprintf(stderr, _("certificate has been expired.\n"));
            return UWAL_CERT_VERIFY_FAILED;
        }
    }

    return UWAL_CERT_VERIFY_SUCCESS;
}

int32_t uwal_get_key_pass(char *keyPassBuff, uint32_t keyPassBuffLen, char *keyPassPath)
{
    char parentdir[UWAL_MAX_PATH_LEN];
    strncpy_s(parentdir, UWAL_MAX_PATH_LEN, g_instance.attr.attr_security.ssl_key_file, strlen(g_instance.attr.attr_security.ssl_key_file));
    if (is_absolute_path(g_instance.attr.attr_security.ssl_key_file)) {
        get_parent_directory(parentdir);
        decode_cipher_files(SERVER_MODE, NULL, parentdir, g_instance.attr.attr_network.server_key);
    } else {
        decode_cipher_files(SERVER_MODE, NULL, g_instance.attr.attr_common.data_directory, g_instance.attr.attr_network.server_key);
    }
    char* tempKey = (char*) g_instance.attr.attr_network.server_key;
    strncpy_s(keyPassBuff ,keyPassBuffLen, tempKey, strlen(tempKey));
    /* clear the sensitive info in server_key */
    errno_t errorno = 0;
    errorno = memset_s(g_instance.attr.attr_network.server_key, CIPHER_LEN + 1, 0, CIPHER_LEN + 1);
    securec_check(errorno, "\0", "\0");
    return UWAL_GET_KEY_PASS_SUCCESS;
}

/**
 * must called after SetHaShmemData
 */
int GsUwalInit(ServerMode serverMode)
{
    int ret = -1;
    errno_t rc = EOK;

    if (serverMode == PRIMARY_MODE) {
        for (int i = 0; i < t_thrd.syncrep_cxt.SyncRepConfigGroups; ++i) {
            if (t_thrd.syncrep_cxt.SyncRepConfig[i]->syncrep_method != SYNC_REP_QUORUM) {
                ereport(ERROR, (errmsg("uwal only support ANY sync method")));
                return ret;
            }
        }

        if (u_sess->attr.attr_storage.guc_synchronous_commit != SYNCHRONOUS_COMMIT_LOCAL_FLUSH &&
            u_sess->attr.attr_storage.guc_synchronous_commit != SYNCHRONOUS_COMMIT_REMOTE_FLUSH) {
            ereport(ERROR, (errmsg("uwal only support synchronous_commit is 'local' or 'on'")));
            return ret;
        }
    }

    if (GsUwalLoadSymbols() != 0) {
        ereport(ERROR, (errmsg("failed to dlopen libuwal.so")));
        return ret;
    }

    UwalCfgElem elem[100];

    int index = 0;
    elem[index].substr = "ock.uwal.ip";
    elem[index].value = g_instance.attr.attr_storage.uwal_ip;

    ++index;
    elem[index].substr = "ock.uwal.port";
    char uwal_port_buff[UWAL_PORT_LEN];
    rc = sprintf_s(uwal_port_buff, UWAL_PORT_LEN, "%d\0", g_instance.attr.attr_storage.uwal_port);
    securec_check_ss_c(rc, "", "");
    elem[index].value = uwal_port_buff;

    ++index;
    elem[index].substr = "ock.uwal.protocol";
    elem[index].value = g_instance.attr.attr_storage.uwal_protocol;

    /* uwal disk pool */
    ++index;
    elem[index].substr = "ock.uwal.disk.poolid";
    elem[index].value = "1";

    ++index;
    elem[index].substr = "ock.uwal.disk.size";
    char uwalDiskSizeBuff[UWAL_BYTE_SIZE_LEN];
    rc = sprintf_s(uwalDiskSizeBuff, UWAL_BYTE_SIZE_LEN, "%lld\0", g_instance.attr.attr_storage.uwal_disk_size);
    securec_check_ss_c(rc, "", "");
    elem[index].value = uwalDiskSizeBuff;

    ++index;
    elem[index].substr = "ock.uwal.disk.min.block";
    char uwalDiskMinBlockBuff[UWAL_BYTE_SIZE_LEN];
    rc = sprintf_s(uwalDiskMinBlockBuff, UWAL_BYTE_SIZE_LEN, "%lld\0", g_instance.attr.attr_storage.uwal_disk_block_size);
    securec_check_ss_c(rc, "", "");
    elem[index].value = uwalDiskMinBlockBuff;

    ++index;
    elem[index].substr = "ock.uwal.disk.max.block";
    char uwalDiskMaxBlockBuff[UWAL_BYTE_SIZE_LEN];
    rc = sprintf_s(uwalDiskMaxBlockBuff, UWAL_BYTE_SIZE_LEN, "%lld\0", g_instance.attr.attr_storage.uwal_disk_block_size);
    securec_check_ss_c(rc, "", "");
    elem[index].value = uwalDiskMaxBlockBuff;

    ++index;
    elem[index].substr = "ock.uwal.devices.path";
    elem[index].value = g_instance.attr.attr_storage.uwal_devices_path;

    ++index;
    elem[index].substr = "ock.uwal.rpc.worker.thread.num";
    char uwalRpcWorkerThreadNumBuff[UWAL_INT_SIZE_LEN];
    rc = sprintf_s(uwalRpcWorkerThreadNumBuff, UWAL_INT_SIZE_LEN, "%d\0",
                   g_instance.attr.attr_storage.uwal_rpc_worker_thread_num);
    securec_check_ss_c(rc, "", "");
    elem[index].value = uwalRpcWorkerThreadNumBuff;

    ++index;
    elem[index].substr = "ock.uwal.rpc.timeout";
    char uwalRpcTimeoutBuff[UWAL_INT_SIZE_LEN];
    rc = sprintf_s(uwalRpcTimeoutBuff, UWAL_INT_SIZE_LEN, "%d\0", g_instance.attr.attr_storage.uwal_rpc_timeout);
    securec_check_ss_c(rc, "", "");
    elem[index].value = uwalRpcTimeoutBuff;

    ++index;
    elem[index].substr = "ock.uwal.rpc.rndv.switch";
    elem[index].value = g_instance.attr.attr_storage.uwal_rpc_rndv_switch ? "true" : "false";

    ++index;
    elem[index].substr = "ock.uwal.rpc.compression.switch";
    elem[index].value = g_instance.attr.attr_storage.uwal_rpc_compression_switch ? "true" : "false";

    ++index;
    elem[index].substr = "ock.uwal.rpc.flowcontrol.switch";
    elem[index].value = g_instance.attr.attr_storage.uwal_rpc_flowcontrol_switch ? "true" : "false";

    ++index;
    elem[index].substr = "ock.uwal.rpc.flowcontrol.value";
    char uwalRpcFlowControlValueBuff[UWAL_INT_SIZE_LEN];
    rc = sprintf_s(uwalRpcFlowControlValueBuff, UWAL_INT_SIZE_LEN, "%d\0",
                   g_instance.attr.attr_storage.uwal_rpc_flowcontrol_value);
    securec_check_ss_c(rc, "", "");
    elem[index].value = uwalRpcFlowControlValueBuff;

    if (g_instance.attr.attr_security.EnableSSL) {
        comm_initialize_SSL();

        ock_uwal_register_cert_verify_func(uwal_cert_verify, uwal_get_key_pass);

        ++index;
        elem[index].substr = "ock.uwal.rpc.tls.switch";
        elem[index].value = "true";

        ++index;
        elem[index].substr = "ock.uwal.rpc.tls.ca.cert.path";
        elem[index].value = g_instance.attr.attr_security.ssl_ca_file;

        ++index;
        elem[index].substr = "ock.uwal.rpc.tls.crl.path";
        elem[index].value = g_instance.attr.attr_security.ssl_crl_file;

        ++index;
        elem[index].substr = "ock.uwal.rpc.tls.cert.path";
        elem[index].value = g_instance.attr.attr_security.ssl_cert_file;

        ++index;
        elem[index].substr = "ock.uwal.rpc.tls.key.path";
        elem[index].value = g_instance.attr.attr_security.ssl_key_file;

        ++index;
        elem[index].substr = "ock.uwal.rpc.tls.key.pass.path";
        elem[index].value = g_instance.attr.attr_security.ssl_key_file;

        ++index;
        elem[index].substr = "ock.uwal.rpc.tls.cipher_suites";
        elem[index].value = g_instance.attr.attr_security.SSLCipherSuites;
    }

    ret = ock_uwal_init(NULL, elem, index + 1, g_instance.attr.attr_storage.uwal_log_path);
    if (ret != 0) {
        ereport(LOG, (errmsg("uwal init fail ret code: %d", ret)));
    }

    return ret;
}

void GetLocalStateInfo(OUT NodeStateInfo *nodeStateInfo)
{
    nodeStateInfo->nodeId = g_instance.attr.attr_storage.uwal_nodeid;
    nodeStateInfo->state = NODE_STATE_UP;
    nodeStateInfo->groupId = 0;
    nodeStateInfo->groupLevel = 0;

    NetInfo netInfo;
    netInfo.ipv4Addr = ock_uwal_ipv4_inet_to_int(g_instance.attr.attr_storage.uwal_ip);
    netInfo.port = g_instance.attr.attr_storage.uwal_port;
    netInfo.protocol = NET_PROTOCOL_TCP;
    if (!strcasecmp(g_instance.attr.attr_storage.uwal_protocol, "rdma")) {
        netInfo.protocol = NET_PROTOCOL_RDMA;
    }

    NetList netList;
    netList.num = 1;
    netList.list[0] = netInfo;
    nodeStateInfo->netList = netList;
}

void GsUwalNotifyCallback(void *ctx, int ret)
{
    CBParams *cbParams = (CBParams *)ctx;
    pthread_mutex_lock(&cbParams->mutex);
    cbParams->cbResult = true;
    cbParams->ret = ret;
    pthread_mutex_unlock(&cbParams->mutex);
    pthread_cond_signal(&cbParams->cond);
}

int GsUwalSyncNotify(NodeStateList *nodeList)
{
    CBParams cbParams = {PTHREAD_MUTEX_INITIALIZER, PTHREAD_COND_INITIALIZER, false, 0};
    pthread_mutex_lock(&cbParams.mutex);
    int ret = ock_uwal_notify_nodelist_change(nodeList, GsUwalNotifyCallback, (void *)&cbParams);
    if (ret != 0) {
        pthread_mutex_unlock(&cbParams.mutex);
        return ret;
    }
    uint16 count = 0;
    while (!cbParams.cbResult) {
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_sec += 3;
        ts.tv_nsec = 0;
        int condWaitRet = 0;
        condWaitRet = pthread_cond_timedwait(&cbParams.cond, &cbParams.mutex, &ts);
        if (condWaitRet == ETIMEDOUT) {
            ++count;
            ereport(LOG, (errmsg("UwalNotifyNodeListChange ret 0 for %u count, wait callback ...", count)));
        }
    }
    pthread_mutex_unlock(&cbParams.mutex);
    bool success = (cbParams.ret == 0);
    if (success) {
        ereport(LOG, (errmsg("UwalNotifyNodeListChange success.")));
    }
    return success ? 0 : -1;
}

/**
 * must called in postmaster thread by primary
 * @return
 */
int GsUwalPrimaryInitNotify()
{
    NodeStateInfo primaryStateInfo;
    GetLocalStateInfo(&primaryStateInfo);

    NodeStateList *nodeList = (NodeStateList *)palloc0(sizeof(NodeStateList) + sizeof(NodeStateInfo));
    nodeList->localNodeId = g_instance.attr.attr_storage.uwal_nodeid;
    nodeList->masterNodeId = g_instance.attr.attr_storage.uwal_nodeid;
    nodeList->nodeNum = 1;
    nodeList->nodeList[0] = primaryStateInfo;
    int ret = GsUwalSyncNotify(nodeList);
    pfree(nodeList);
    return ret;
}

bool FindSyncRepConfig(IN const char *applicationName, OUT int *group, OUT uint8 *syncrepMethod, OUT unsigned *numSync)
{
    if (!group && !syncrepMethod && !numSync) {
        return true;
    }
    const char *standbyName = NULL;
    for (int index = 0; index < t_thrd.syncrep_cxt.SyncRepConfigGroups; index++) {
        // standbyName may be: s1\0s2\0s3\0s4\0
        standbyName = t_thrd.syncrep_cxt.SyncRepConfig[index]->member_names;
        for (int priority = 1; priority <= t_thrd.syncrep_cxt.SyncRepConfig[index]->nmembers; ++priority) {
            if (pg_strcasecmp(applicationName, standbyName) == 0) {
                if (group) {
                    *group = index + 1;
                }
                if (syncrepMethod) {
                    *syncrepMethod = t_thrd.syncrep_cxt.SyncRepConfig[index]->syncrep_method;
                }
                if (numSync) {
                    *numSync = t_thrd.syncrep_cxt.SyncRepConfig[index]->num_sync;
                }
                return true;
            }
            standbyName += strlen(standbyName) + 1;
        }
    }
    return false;
}

/**
 * must called in walsender thread
 * @return
 */
int GsUwalWalSenderNotify(bool exceptSelf)
{
    NodeStateList *nodeList = (NodeStateList *)palloc0(
        sizeof(NodeStateList) + sizeof(NodeStateInfo) * g_instance.attr.attr_storage.max_wal_senders);
    nodeList->localNodeId = g_instance.attr.attr_storage.uwal_nodeid;
    nodeList->masterNodeId = g_instance.attr.attr_storage.uwal_nodeid;

    unsigned statSendersGroupLevel[SYNC_REP_MAX_GROUPS] = {0};
    unsigned statSendersGroupNumSync[SYNC_REP_MAX_GROUPS] = {0};
    // iterate over all wal sender
    int count = 0;
    for (int i = 0; i < g_instance.attr.attr_storage.max_wal_senders; i++) {
        /* use volatile pointer to prevent code rearrangement */
        volatile WalSnd *walsnd = &t_thrd.walsender_cxt.WalSndCtl->walsnds[i];
        char remoteApplicationName[NAMEDATALEN];
        int channelGetReplc = 0;

        if (walsnd->pid == 0 ||
            (exceptSelf && t_thrd.walsender_cxt.MyWalSnd && walsnd->pid == t_thrd.walsender_cxt.MyWalSnd->pid)) {
            continue;
        }

        SpinLockAcquire(&walsnd->mutex);
        errno_t rc =
            memcpy_s((void *)remoteApplicationName, NAMEDATALEN, (void *)walsnd->remote_application_name, NAMEDATALEN);
        securec_check(rc, "", "");
        channelGetReplc = walsnd->channel_get_replc;
        SpinLockRelease(&walsnd->mutex);

        int group = 0;
        unsigned numSync = 0;
        NodeStateInfo standbyStateInfo;
        if (strcmp(u_sess->attr.attr_storage.SyncRepStandbyNames, "*") == 0) {
            standbyStateInfo.groupId = 0;
            standbyStateInfo.groupLevel = 1;
        } else if (FindSyncRepConfig(remoteApplicationName, &group, NULL, &numSync)) {
            statSendersGroupLevel[group] += 1;
            statSendersGroupNumSync[group] = numSync;
            standbyStateInfo.groupId = group;
        } else {
            standbyStateInfo.groupId = 0;
            standbyStateInfo.groupLevel = 0;
        }

        ReplConnInfo *replConnInfo = t_thrd.postmaster_cxt.ReplConnArray[channelGetReplc];

        standbyStateInfo.nodeId = replConnInfo->remotenodeid;
        standbyStateInfo.state = NODE_STATE_UP;

        bool findRepeat = false;
        for (int j = 0; j < count; ++j) {
            if (nodeList->nodeList[j].nodeId == standbyStateInfo.nodeId) {
                findRepeat = true;
                break;
            }
        }
        if (findRepeat) {
            continue;
        }
        NetInfo netInfo;
        netInfo.ipv4Addr = ock_uwal_ipv4_inet_to_int((char *)replConnInfo->remoteuwalhost);
        netInfo.port = replConnInfo->remoteuwalport;
        netInfo.protocol = NET_PROTOCOL_TCP;
        if (!strcasecmp(g_instance.attr.attr_storage.uwal_protocol, "rdma")) {
            netInfo.protocol = NET_PROTOCOL_RDMA;
        }

        NetList netList;
        netList.num = 1;
        netList.list[0] = netInfo;
        standbyStateInfo.netList = netList;
        nodeList->nodeList[count++] = standbyStateInfo;
    }

    for (int i = 0; i < count; i++) {
        unsigned group_index = nodeList->nodeList[i].groupId;
        nodeList->nodeList[i].groupLevel = statSendersGroupLevel[group_index] <= statSendersGroupNumSync[group_index]
                                               ? statSendersGroupLevel[group_index]
                                               : statSendersGroupNumSync[group_index];
    }

    NodeStateInfo primaryStateInfo;
    GetLocalStateInfo(&primaryStateInfo);
    nodeList->nodeList[count++] = primaryStateInfo;
    nodeList->nodeNum = count;

    int ret = GsUwalSyncNotify(nodeList);
    pfree(nodeList);
    return ret;
}

/**
 * must called in walreceiver thread, after connected to primary.
 * @return
 */
int GsUwalWalReceiverNotify(bool isConnectedToPrimary)
{
    int walReplIndex = t_thrd.postmaster_cxt.HaShmData->current_repl;
    ReplConnInfo *replConnInfo = t_thrd.postmaster_cxt.ReplConnArray[walReplIndex];

    NodeStateList *nodeList = (NodeStateList *)palloc0(sizeof(NodeStateList) + sizeof(NodeStateInfo) * 2);
    nodeList->nodeNum = 1;
    nodeList->localNodeId = g_instance.attr.attr_storage.uwal_nodeid;

    // local state info
    NodeStateInfo localStateInfo;
    GetLocalStateInfo(&localStateInfo);
    nodeList->nodeList[0] = localStateInfo;

    // primary state info
    if (isConnectedToPrimary) {
        nodeList->masterNodeId = replConnInfo->remotenodeid;
        NodeStateInfo primaryStateInfo;
        primaryStateInfo.nodeId = replConnInfo->remotenodeid;
        primaryStateInfo.state = NODE_STATE_UP;
        primaryStateInfo.groupLevel = 0;
        primaryStateInfo.groupId = 0;

        NetInfo netInfo;
        netInfo.ipv4Addr = ock_uwal_ipv4_inet_to_int((char *)replConnInfo->remotehost);
        netInfo.port = replConnInfo->remoteuwalport;
        netInfo.protocol = NET_PROTOCOL_TCP;
        if (!strcasecmp(g_instance.attr.attr_storage.uwal_protocol, "rdma")) {
            netInfo.protocol = NET_PROTOCOL_RDMA;
        }

        NetList netList;
        netList.num = 1;
        netList.list[0] = netInfo;
        primaryStateInfo.netList = netList;

        nodeList->nodeList[1] = primaryStateInfo;
        nodeList->nodeNum += 1;
    } else {
        nodeList->masterNodeId = NODE_ID_INVALID;
    }

    int ret = GsUwalSyncNotify(nodeList);
    pfree(nodeList);
    return ret;
}

/**
 *  must called in postmaster thread by standby
 * @return
 */
int GsUwalStandbyInitNotify()
{
    NodeStateList *nodeList = (NodeStateList *)palloc0(sizeof(NodeStateList) + sizeof(NodeStateInfo) * 2);
    nodeList->nodeNum = 1;
    nodeList->localNodeId = g_instance.attr.attr_storage.uwal_nodeid;
    nodeList->masterNodeId = NODE_ID_INVALID;

    // local state info
    NodeStateInfo localStateInfo;
    GetLocalStateInfo(&localStateInfo);
    nodeList->nodeList[0] = localStateInfo;

    int ret = GsUwalSyncNotify(nodeList);
    pfree(nodeList);
    return ret;
}

/**
 * called after uwal append
 * @param lsn write log success lsn
 * @param infos uwalAppend() return UwalNodeInfo
 */
void GsUwalUpdateSenderSyncLsn(XLogRecPtr lsn, UwalNodeInfo *infos)
{
    for (int senderIndex = 0; senderIndex < g_instance.attr.attr_storage.max_wal_senders; ++senderIndex) {
        volatile WalSnd *walSnd = &t_thrd.walsender_cxt.WalSndCtl->walsnds[senderIndex];
        if (walSnd->pid == 0 || walSnd->sendRole == SNDROLE_PRIMARY_BUILDSTANDBY) {
            continue;
        }

        // find the replConnInfo->remotenodeid
        int channel_get_replc = walSnd->channel_get_replc;
        uint32_t remotenodeid = (uint32_t)(t_thrd.postmaster_cxt.ReplConnArray[channel_get_replc]->remotenodeid);

        // find the remote node in infos
        bool find = false;
        for (uint32_t nodeIndex = 0; nodeIndex < infos->num; ++nodeIndex) {
            if (infos->status[nodeIndex].nodeId == remotenodeid && infos->status[nodeIndex].ret == U_OK) {
                find = true;
            }
        }

        if (!find) {
            continue;
        }

        walSnd->log_ctrl.prev_send_time = GetCurrentTimestamp();
        SpinLockAcquire(&walSnd->mutex);
        walSnd->sentPtr = lsn;
        walSnd->receive = lsn;
        walSnd->write = lsn;
        walSnd->flush = lsn;
        walSnd->peer_role = STANDBY_MODE;
        walSnd->peer_state = NORMAL_STATE;
        SpinLockRelease(&walSnd->mutex);
    }
}

int GsUwalQueryByUser(TimeLineID thisTimeLineID, bool needHistoryList)
{
    int ret;
    errno_t rc;
    UwalInfoHis *rinfo;

    UwalInfo *uwalInfos = (UwalInfo *)palloc(sizeof(UwalInfo) * UWAL_MAX_NUM);
    UwalUserType user = UWAL_USER_OPENGAUSS;
    UwalVector *vec = (UwalVector *)palloc(sizeof(UwalVector));
    vec->uwals = uwalInfos;

    ret = ock_uwal_query_by_user(user, UWAL_ROUTE_LOCAL, vec);
    if (ret == 0) {
        for (uint32 i = 0; i < vec->cnt; i++) {
            if (vec->uwals->info.startTimeLine == thisTimeLineID) {
                rc = memcpy_s(t_thrd.xlog_cxt.uwalInfo.id.id, UWAL_ID_LEN, (uwalInfos + i)->id.id, UWAL_ID_LEN);
                securec_check(rc, "", "");
                t_thrd.xlog_cxt.uwalInfo.info.startTimeLine = (uwalInfos + i)->info.startTimeLine;
                t_thrd.xlog_cxt.uwalInfo.info.startWriteOffset = (uwalInfos + i)->info.startWriteOffset;
                t_thrd.xlog_cxt.uwalInfo.info.dataSize = (uwalInfos + i)->info.dataSize;
                t_thrd.xlog_cxt.uwalInfo.info.truncateOffset = (uwalInfos + i)->info.truncateOffset;
                t_thrd.xlog_cxt.uwalInfo.info.writeOffset = (uwalInfos + i)->info.writeOffset;
            } else if (needHistoryList) {
                rinfo = (UwalInfoHis *)palloc(sizeof(UwalInfoHis));
                rc = memcpy_s(rinfo->info.id.id, UWAL_ID_LEN, (uwalInfos + i)->id.id, UWAL_ID_LEN);
                securec_check(rc, "", "");
                rinfo->info.info.startTimeLine = (uwalInfos + i)->info.startTimeLine;
                rinfo->info.info.startWriteOffset = (uwalInfos + i)->info.startWriteOffset;
                rinfo->info.info.dataSize = (uwalInfos + i)->info.dataSize;
                rinfo->info.info.truncateOffset = (uwalInfos + i)->info.truncateOffset;
                rinfo->info.info.writeOffset = (uwalInfos + i)->info.writeOffset;
                rinfo->recycle = false;
                t_thrd.xlog_cxt.uwalInfoHis = lappend(t_thrd.xlog_cxt.uwalInfoHis, rinfo);
            }
        }
    } else {
        ereport(LOG, (errmsg("UwalQueryByUser failed, ret: %d", ret)));
    }

    pfree(uwalInfos);
    pfree(vec);
    vec = NULL;
    uwalInfos = NULL;
    return ret;
}

int GsUwalQuery(UwalId id, UwalBaseInfo *info)
{
    int ret;
    UwalInfo *uwalInfos = (UwalInfo *)palloc0(sizeof(UwalInfo));
    UwalQueryParam params = {&id, UWAL_ROUTE_LOCAL, NULL};

    ret = ock_uwal_query(&params, uwalInfos);
    if (ret == 0) {
        info->startTimeLine = uwalInfos->info.startTimeLine;
        info->startWriteOffset = uwalInfos->info.startWriteOffset;
        info->dataSize = uwalInfos->info.dataSize;
        info->truncateOffset = uwalInfos->info.truncateOffset;
        info->writeOffset = uwalInfos->info.writeOffset;
    } else {
        ereport(WARNING, (errmsg("UwalQuery return failed")));
    }
    pfree(uwalInfos);
    return ret;
}

int GsUwalCreate(uint64_t startOffset)
{
    UwalDurability dura;
    dura.azCnt = 1;
    dura.originNum = 1;
    dura.redundancyNum = 0;
    dura.reliabilityType = 1;

    UwalAffinityPolicy affinity;
    affinity.partId = 0;
    affinity.detail.cnt = 0;
    affinity.detail.serverId = NULL;

    UwalDescriptor desc;
    desc.user = UWAL_USER_OPENGAUSS;
    desc.perfType = UWAL_PERF_TYPE_SSD;
    desc.stripe = UWAL_STRIPE_BUTT;
    desc.io = UWAL_IO_RANDOM;
    desc.dataSize = g_instance.attr.attr_storage.uwal_disk_block_size;
    desc.startTimeLine = t_thrd.xlog_cxt.ThisTimeLineID;
    desc.startWriteOffset = startOffset;
    desc.durability = dura;
    desc.flags = UWAL_CREATE_DEGRADE_LOSSANY;
    desc.affinity = affinity;

    UwalCreateParam params = {&desc, 1, NULL};

    UwalVector *vec = (UwalVector *)palloc0(sizeof(UwalVector));
    if (vec == NULL) {
        return -1;
    }
    UwalInfo *uwalInfos = (UwalInfo *)palloc0(sizeof(UwalInfo));
    if (uwalInfos == NULL) {
        return -1;
    }
    vec->uwals = uwalInfos;

    int ret;
    ret = ock_uwal_create(&params, vec);
    if (ret == 0 && vec->cnt > 0) {
        errno_t rc = memcpy_s(t_thrd.xlog_cxt.uwalInfo.id.id, UWAL_ID_LEN, &vec->uwals->id.id, UWAL_ID_LEN);
        securec_check(rc, "", "");
        t_thrd.xlog_cxt.uwalInfo.info.startTimeLine = vec->uwals->info.startTimeLine;
        t_thrd.xlog_cxt.uwalInfo.info.startWriteOffset = vec->uwals->info.startWriteOffset;
        t_thrd.xlog_cxt.uwalInfo.info.dataSize = vec->uwals->info.dataSize;
        t_thrd.xlog_cxt.uwalInfo.info.truncateOffset = vec->uwals->info.truncateOffset;
        t_thrd.xlog_cxt.uwalInfo.info.writeOffset = vec->uwals->info.writeOffset;
    }

    pfree(uwalInfos);
    pfree(vec);
    return ret;
}

int GsUwalRead(UwalId id, XLogRecPtr targetPagePtr, char *readBuf, uint64_t readlen)
{
    errno_t errorno = EOK;

    UwalDataToRead *dataToRead = (UwalDataToRead *)palloc0(sizeof(UwalDataToRead));
    dataToRead->offset = targetPagePtr;
    dataToRead->length = readlen;
    UwalDataToReadVec *datav = (UwalDataToReadVec *)palloc0(sizeof(UwalDataToReadVec));
    datav->cnt = 1;
    datav->dataToRead = dataToRead;
    UwalReadParam params = {&id, UWAL_ROUTE_LOCAL, datav, NULL};

    UwalBufferList *bufflist = (UwalBufferList *)palloc0(sizeof(UwalBufferList));
    bufflist->cnt = 1;
    UwalBuffer *buffers = (UwalBuffer *)palloc0(sizeof(UwalBuffer));
    char *buf = (char *)palloc0(sizeof(char) * XLOG_BLCKSZ);
    errorno = memset_s(buf, XLOG_BLCKSZ, 0, XLOG_BLCKSZ);
    securec_check(errorno, "", "");

    buffers->buf = buf;
    buffers->len = readlen;
    bufflist->buffers = buffers;

    int ret = ock_uwal_read(&params, bufflist);

    errno_t rc = memcpy_s(readBuf, readlen, bufflist->buffers->buf, readlen);
    securec_check(rc, "", "");

    pfree(buf);
    pfree(buffers);
    pfree(bufflist);
    pfree(dataToRead);
    pfree(datav);
    return ret;
}


void GsUwalWriteAsyncCallBack(void *cbCtx, int retCode)
{
    UwalSingleAsyncCbCtx *curCbCtx = (UwalSingleAsyncCbCtx *)cbCtx;
    curCbCtx->ret = retCode;
    UwalAsyncAppendCbCtx *commonCbCtx = curCbCtx->commonCbCtx;
    pthread_mutex_lock(&(commonCbCtx->mutex));
    if (!commonCbCtx->interrupted) {
        // update nodeinfo
        if (commonCbCtx->curCount == 0) {
            commonCbCtx->ret = curCbCtx->ret;
            commonCbCtx->infos->num = curCbCtx->infos->num;
            for (uint32_t i = 0; i < curCbCtx->infos->num; i++) {
                commonCbCtx->infos->status[i].nodeId = curCbCtx->infos->status[i].nodeId;
                commonCbCtx->infos->status[i].ret = curCbCtx->infos->status[i].ret;
            }
        } else if (commonCbCtx->ret == 0 && curCbCtx->ret != 0) {
            commonCbCtx->ret = curCbCtx->ret;
            commonCbCtx->infos->num = curCbCtx->infos->num;
            for (uint32_t i = 0; i < curCbCtx->infos->num; i++) {
                commonCbCtx->infos->status[i].nodeId = curCbCtx->infos->status[i].nodeId;
                commonCbCtx->infos->status[i].ret = curCbCtx->infos->status[i].ret;
            }
        }
        commonCbCtx->curCount += 1;
        if (commonCbCtx->curCount == commonCbCtx->opCount) {
            pthread_cond_signal(&(commonCbCtx->cond));
            pthread_mutex_unlock(&(commonCbCtx->mutex));
        } else {
            pthread_mutex_unlock(&(commonCbCtx->mutex));
        }
    } else {
        commonCbCtx->curCount += 1;
        if (commonCbCtx->curCount == commonCbCtx->interruptCount) {
            pthread_mutex_unlock(&(commonCbCtx->mutex));
            pthread_mutex_destroy(&(commonCbCtx->mutex));
            pthread_cond_destroy(&(commonCbCtx->cond));
            free(commonCbCtx);
        } else {
            pthread_mutex_unlock(&(commonCbCtx->mutex));
        }
    }

    free(curCbCtx->infos);
    free(curCbCtx->appendParam->cb);
    free(curCbCtx->appendParam);
    free(curCbCtx);
}

int GsUwalWriteAsync(UwalId id, int nBytes, char *buf, UwalNodeInfo *infos)
{
    uint64_t offset;
    int bufferOffset = 0;
    int batchIOSize = 0;
    int batchIONumber = 0;
    int batchLastIOSize = 0;
    int ret = 0;

    batchIOSize = g_instance.attr.attr_storage.uwal_batch_io_size;
    batchIONumber = nBytes / batchIOSize;
    if (nBytes % batchIOSize != 0) {
        batchIONumber += 1;
    }
    batchLastIOSize = nBytes - (batchIONumber - 1) * batchIOSize;

    UwalAsyncAppendCbCtx *cbCtx = (UwalAsyncAppendCbCtx *)malloc(sizeof(UwalAsyncAppendCbCtx));
    cbCtx->ret = 0;
    cbCtx->curCount = 0;
    cbCtx->opCount = batchIONumber;
    cbCtx->infos = infos;
    cbCtx->interrupted = false;
    cbCtx->interruptCount = 0;
    pthread_mutex_init(&(cbCtx->mutex), 0);
    pthread_cond_init(&(cbCtx->cond), 0);

    for (int i = 0; i < batchIONumber; i++) {
        UwalAppendParam *appendParam = (UwalAppendParam *)malloc(sizeof(UwalAppendParam));
        UwalNodeInfo *uwalInfos = (UwalNodeInfo *)malloc(sizeof(UwalNodeInfo) + MAX_GAUSS_NODE * sizeof(UwalNodeStatus));
        UwalCallBack *uwalCB = (UwalCallBack *)malloc(sizeof(UwalCallBack));
        UwalSingleAsyncCbCtx *uwalCbCtx = (UwalSingleAsyncCbCtx *)malloc(sizeof(UwalSingleAsyncCbCtx));
        UwalBuffer buffers[1] = {{buf + bufferOffset, batchIOSize}};
        UwalBufferList bufferList = {1, buffers};

        uwalCbCtx->commonCbCtx = cbCtx;
        uwalCbCtx->infos = uwalInfos;
        uwalCbCtx->ret = 0;
        uwalCbCtx->appendParam = appendParam;

        uwalCB->cb = GsUwalWriteAsyncCallBack;
        uwalCB->cbCtx = uwalCbCtx;

        if (i == batchIONumber - 1) {
            buffers[0].len = batchLastIOSize;
            bufferOffset += batchLastIOSize;
        } else {
            bufferOffset += batchIOSize;
        }

        appendParam->uwalId = &id;
        appendParam->bufferList = &bufferList;
        appendParam->cb = uwalCB;

        ret = ock_uwal_append(appendParam, &offset, (void *)uwalInfos);
        if (ret != 0) {
            free(appendParam);
            free(uwalInfos);
            free(uwalCB);
            free(uwalCbCtx);
            pthread_mutex_lock(&(cbCtx->mutex));
            cbCtx->interrupted = true;
            cbCtx->interruptCount = i;
            if (cbCtx->interruptCount == cbCtx->curCount) {
                pthread_mutex_unlock(&(cbCtx->mutex));
                pthread_mutex_destroy(&(cbCtx->mutex));
                pthread_cond_destroy(&(cbCtx->cond));
                free(cbCtx);
            } else {
                pthread_mutex_unlock(&(cbCtx->mutex));
            }
            return ret;
        }
    }
    pthread_mutex_lock(&(cbCtx->mutex));
    while (cbCtx->curCount < cbCtx->opCount) {
        pthread_cond_wait(&(cbCtx->cond), &(cbCtx->mutex));
    }
    pthread_mutex_unlock(&(cbCtx->mutex));
    pthread_mutex_destroy(&(cbCtx->mutex));
    pthread_cond_destroy(&(cbCtx->cond));

    ret = cbCtx->ret;
    free(cbCtx);
    return ret;
}
