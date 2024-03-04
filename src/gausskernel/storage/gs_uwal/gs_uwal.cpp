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
#include "cjson/cJSON.h"
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
#define UWAL_DISK_POOL_ID "1"
#define UWAL_ASYNC_THREAD_NUM "4"
#define UWAL_SYNC_THREAD_NUM "1"
#define UWAL_TIMEOUT "30000"
#define UWAL_MIN_PAGES 4
#define UWAL_WORKER_NUM 4

int64 uwal_size = XLogSegSize;

UwalConfig g_uwalConfig;

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

    if (X509_cmp_time(asnEarlyTime, nullptr) != UWAL_CERT_VERIFY_FAILED) {
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

static void ParseUwalCpuBindInfo(cJSON *uwalConfJSON)
{
    g_uwalConfig.bindCpuSwitch = false;

    cJSON *bindJSON = cJSON_GetObjectItem(uwalConfJSON, "cpu_bind_switch");
    if (bindJSON == nullptr) {
        return;
    }
    if (!strcasecmp(bindJSON->valuestring, "true")) {
        g_uwalConfig.bindCpuSwitch = true;
    }

    if (!g_uwalConfig.bindCpuSwitch) {
        return;
    }

    g_uwalConfig.bindCpuStart = UWAL_CPU_BIND_START_DEF;
    cJSON *bindStartJSON = cJSON_GetObjectItem(uwalConfJSON, "cpu_bind_start");
    if (bindStartJSON != nullptr &&
        (bindStartJSON->valueint >= UWAL_CPU_BIND_START_MIN && bindStartJSON->valueint <= UWAL_CPU_BIND_START_MAX)) {
        g_uwalConfig.bindCpuStart = bindStartJSON->valueint;
    } else {
        ereport(WARNING, (errmsg("cpu_bind_start not provided or not in range [%d, %d], use the default value %d",
            UWAL_CPU_BIND_START_MIN, UWAL_CPU_BIND_START_MAX, UWAL_CPU_BIND_START_DEF)));
    }

    g_uwalConfig.bindCpuNum = UWAL_CPU_BIND_NUM_DEF;
    cJSON *bindNumJSON = cJSON_GetObjectItem(uwalConfJSON, "cpu_bind_num");
    if (bindNumJSON != nullptr &&
        (bindNumJSON->valueint >= UWAL_CPU_BIND_NUM_MIN && bindNumJSON->valueint <= UWAL_CPU_BIND_NUM_MAX)) {
        g_uwalConfig.bindCpuNum = bindNumJSON->valueint;
    } else {
        ereport(WARNING, (errmsg("cpu_bind_num not provided or not in range [%d, %d], use the default value %d",
                UWAL_CPU_BIND_NUM_MIN, UWAL_CPU_BIND_NUM_MAX, UWAL_CPU_BIND_NUM_DEF)));
    }

    return;
}

static bool GsUwalParseConfig(cJSON *uwalConfJSON)
{
    errno_t rc = EOK;
    cJSON *idJSON = cJSON_GetObjectItem(uwalConfJSON, "uwal_nodeid");
    if (idJSON == nullptr) {
        ereport(ERROR, (errmsg("No item uwal_nodeid in uwal_config")));
        return false;
    }
    if (idJSON->valueint < 0 || idJSON->valueint >= MAX_GAUSS_NODE) {
        ereport(ERROR, (errmsg("uwal_nodeid out of range [0, 7]")));
        return false;
    }
    g_uwalConfig.id = idJSON->valueint;

    cJSON *ipJSON = cJSON_GetObjectItem(uwalConfJSON, "uwal_ip");
    if (ipJSON == nullptr) {
        ereport(ERROR, (errmsg("No item uwal_ip in uwal_config")));
        return false;
    }
    rc = strcpy_s(g_uwalConfig.ip, UWAL_IP_LEN, ipJSON->valuestring);
    securec_check(rc, "\0", "\0");

    cJSON *portJSON = cJSON_GetObjectItem(uwalConfJSON, "uwal_port");
    if (portJSON == nullptr) {
        ereport(ERROR, (errmsg("No item uwal_port in uwal_config")));
        return false;
    }
    if (portJSON->valueint < UWAL_PORT_MIN || portJSON->valueint > UWAL_PORT_MAX) {
        ereport(ERROR, (errmsg("uwal_port out of range [%d, %d]", UWAL_PORT_MIN, UWAL_PORT_MAX)));
        return false;
    }
    g_uwalConfig.port = portJSON->valueint;

    cJSON *protocolJSON = cJSON_GetObjectItem(uwalConfJSON, "uwal_protocol");
    if (protocolJSON == nullptr) {
        ereport(WARNING, (errmsg("No item uwal_protocol in uwal_config, will use the default protocol tcp")));
        rc = strcpy_s(g_uwalConfig.protocol, UWAL_PROTOCOL_LEN, "tcp");
        securec_check(rc, "\0", "\0");
    } else if (!strcasecmp(protocolJSON->valuestring, "rdma")) {
        rc = strcpy_s(g_uwalConfig.protocol, UWAL_PROTOCOL_LEN, "rdma");
        securec_check(rc, "\0", "\0");
    } else if (!strcasecmp(protocolJSON->valuestring, "tcp")) {
        rc = strcpy_s(g_uwalConfig.protocol, UWAL_PROTOCOL_LEN, "tcp");
        securec_check(rc, "\0", "\0");
    } else {
        ereport(WARNING, (errmsg("uwal_protocol only support tcp and rdma, will use the default protocol tcp")));
        rc = strcpy_s(g_uwalConfig.protocol, UWAL_PROTOCOL_LEN, "tcp");
        securec_check(rc, "\0", "\0");
    }

    cJSON *replinodesJSON = cJSON_GetObjectItem(uwalConfJSON, "uwal_replinodes");
    if (replinodesJSON == nullptr) {
        ereport(WARNING, (errmsg("No item uwal_replinodes in uwal_config, will use the default protocol tcp")));
        for (int i = 0; i <MAX_NODE_NUM; i++) {
            rc = strcpy_s(g_uwalConfig.repliNodes[i], UWAL_PROTOCOL_LEN, "tcp");
            securec_check(rc, "\0", "\0");
        }
    } else {
        if (cJSON_IsArray(replinodesJSON)) {
            int arrLen = cJSON_GetArraySize(replinodesJSON);
            for (int i = 0; i < arrLen; i++) {
                cJSON *subObj = cJSON_GetArrayItem(replinodesJSON, i);
                if (nullptr == subObj) {
                    continue;
                } else {
                    cJSON *subIdJSON = cJSON_GetObjectItem(subObj, "id");
                    if (subIdJSON == nullptr) {
                        ereport(ERROR, (errmsg("No item uwal_nodeid in uwal_replinodes")));
                        return false;
                    }
                    if (subIdJSON->valueint < 0 || subIdJSON->valueint >= MAX_GAUSS_NODE) {
                        ereport(ERROR, (errmsg("uwal_nodeid out of range [0, 7]")));
                        return false;
                    }
                    int nodeId = subIdJSON->valueint;
                    cJSON *subProtocolJSON = cJSON_GetObjectItem(subObj, "protocol");
                    if (subProtocolJSON == nullptr) {
                        ereport(WARNING, (errmsg("No item protocol in uwal_replinodes, use the default protocol tcp")));
                        rc = strcpy_s(g_uwalConfig.repliNodes[nodeId], UWAL_PROTOCOL_LEN, "tcp");
                        securec_check(rc, "\0", "\0");
                    } else if (strcasecmp(subProtocolJSON->valuestring, "rdma") &&
                               strcasecmp(subProtocolJSON->valuestring, "tcp")) {
                        ereport(WARNING, (errmsg("protocol only support tcp and rdma, use the default protocol tcp")));
                        rc = strcpy_s(g_uwalConfig.repliNodes[nodeId], UWAL_PROTOCOL_LEN, "tcp");
                        securec_check(rc, "\0", "\0");
                    } else {
                        rc = strcpy_s(g_uwalConfig.repliNodes[nodeId], UWAL_PROTOCOL_LEN, subProtocolJSON->valuestring);
                        securec_check(rc, "\0", "\0");
                    }
                }
            }
        }
    }

    ParseUwalCpuBindInfo(uwalConfJSON);
    return true;
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

    cJSON *uwalConfJSON = cJSON_Parse(g_instance.attr.attr_storage.uwal_config);
    if (uwalConfJSON == nullptr) {
        const char* errorPtr = cJSON_GetErrorPtr();
        if (errorPtr != nullptr) {
            ereport(ERROR, (errmsg("Failed to parse uwal config: %s", errorPtr)));
        } else {
            ereport(ERROR, (errmsg("Failed to parse uwal config: unKnown error")));
        }
        return ret;
    }
    if (!GsUwalParseConfig(uwalConfJSON)) {
        cJSON_Delete(uwalConfJSON);
        return ret;
    }
    cJSON_Delete(uwalConfJSON);

    UwalCfgElem elem[100];

    int index = 0;
    elem[index].substr = "ock.uwal.ip";
    elem[index].value = g_uwalConfig.ip;

    elem[++index].substr = "ock.uwal.port";
    char uwal_port_buff[UWAL_PORT_LEN];
    rc = sprintf_s(uwal_port_buff, UWAL_PORT_LEN, "%d\0", g_uwalConfig.port);
    securec_check_ss_c(rc, "", "");
    elem[index].value = uwal_port_buff;

    elem[++index].substr = "ock.uwal.protocol";
    elem[index].value = g_uwalConfig.protocol;

    /* uwal disk pool */
    elem[++index].substr = "ock.uwal.disk.poolid";
    elem[index].value = UWAL_DISK_POOL_ID;

    elem[++index].substr = "ock.uwal.disk.size";
    char uwalDiskSizeBuff[UWAL_BYTE_SIZE_LEN];
    rc = sprintf_s(uwalDiskSizeBuff, UWAL_BYTE_SIZE_LEN, "%lld\0", g_instance.attr.attr_storage.uwal_disk_size);
    securec_check_ss_c(rc, "", "");
    elem[index].value = uwalDiskSizeBuff;

    int64 uwal_aligned_disk_size = g_instance.attr.attr_storage.uwal_disk_size / XLogSegSize * XLogSegSize;
    uwal_size = ((uwal_aligned_disk_size / XLogSegSize - 1) / 2) * XLogSegSize;

    elem[++index].substr = "ock.uwal.disk.min.block";
    char uwalDiskMinBlockBuff[UWAL_BYTE_SIZE_LEN];
    rc = sprintf_s(uwalDiskMinBlockBuff, UWAL_BYTE_SIZE_LEN, "%lu\0", XLogSegSize);
    securec_check_ss_c(rc, "", "");
    elem[index].value = uwalDiskMinBlockBuff;

    elem[++index].substr = "ock.uwal.disk.max.block";
    char uwalDiskMaxBlockBuff[UWAL_BYTE_SIZE_LEN];
    rc = sprintf_s(uwalDiskMaxBlockBuff, UWAL_BYTE_SIZE_LEN, "%lld\0", uwal_size);
    securec_check_ss_c(rc, "", "");
    elem[index].value = uwalDiskMaxBlockBuff;

    elem[++index].substr = "ock.uwal.devices.path";
    elem[index].value = g_instance.attr.attr_storage.uwal_devices_path;

    elem[++index].substr = "ock.uwal.rpc.worker.thread.num";
    if (g_instance.attr.attr_storage.uwal_async_append_switch) {
        elem[index].value = UWAL_ASYNC_THREAD_NUM;
    } else {
        elem[index].value = UWAL_SYNC_THREAD_NUM;
    }

    elem[++index].substr = "ock.uwal.rpc.timeout";
    elem[index].value = UWAL_TIMEOUT;

    elem[++index].substr = "ock.uwal.rpc.rndv.switch";
    elem[index].value = "true";
   
    elem[++index].substr = "ock.uwal.rpc.compression.switch";
    elem[index].value = g_instance.attr.attr_storage.uwal_rpc_compression_switch ? "true" : "false";

    elem[++index].substr = "ock.uwal.rpc.flowcontrol.switch";
    elem[index].value = g_instance.attr.attr_storage.uwal_rpc_flowcontrol_switch ? "true" : "false";

    elem[++index].substr = "ock.uwal.rpc.flowcontrol.value";
    char uwalRpcFlowControlValueBuff[UWAL_INT_SIZE_LEN];
    rc = sprintf_s(uwalRpcFlowControlValueBuff, UWAL_INT_SIZE_LEN, "%d\0",
                   g_instance.attr.attr_storage.uwal_rpc_flowcontrol_value);
    securec_check_ss_c(rc, "", "");
    elem[index].value = uwalRpcFlowControlValueBuff;

    elem[++index].substr = "ock.uwal.devices.split.switch";
    elem[index].value = "true";

    elem[++index].substr = "ock.uwal.devices.split.size";
    char uwalSplitSizeBuff[UWAL_BYTE_SIZE_LEN];
    rc = sprintf_s(uwalSplitSizeBuff, UWAL_BYTE_SIZE_LEN, "%lu\0", XLogSegSize);
    securec_check_ss_c(rc, "", "");
    elem[index].value = uwalSplitSizeBuff;

    elem[++index].substr = "ock.uwal.devices.split.path";
    elem[index].value = g_instance.attr.attr_storage.uwal_devices_path;

    // cpu bind info
    char uwalCpuBindNumBuff[UWAL_INT_SIZE_LEN];
    char uwalCpuBindStartBuff[UWAL_INT_SIZE_LEN];
    if (g_uwalConfig.bindCpuSwitch) {
        elem[++index].substr = "ock.uwal.cpu.bind.switch";
        elem[index].value = "true";

        elem[++index].substr = "ock.uwal.cpu.bind.num";
        rc = sprintf_s(uwalCpuBindNumBuff, UWAL_INT_SIZE_LEN, "%d\0", g_uwalConfig.bindCpuNum);
        securec_check_ss_c(rc, "", "");
        elem[index].value = uwalCpuBindNumBuff;

        elem[++index].substr = "ock.uwal.cpu.bind.start";
        rc = sprintf_s(uwalCpuBindStartBuff, UWAL_INT_SIZE_LEN, "%d\0", g_uwalConfig.bindCpuStart);
        securec_check_ss_c(rc, "", "");
        elem[index].value = uwalCpuBindStartBuff;
    }

    if (g_instance.attr.attr_security.EnableSSL) {
        comm_initialize_SSL();

        ock_uwal_register_cert_verify_func(uwal_cert_verify, uwal_get_key_pass);

        elem[++index].substr = "ock.uwal.rpc.tls.switch";
        elem[index].value = "true";

        elem[++index].substr = "ock.uwal.rpc.tls.ca.cert.path";
        elem[index].value = g_instance.attr.attr_security.ssl_ca_file;

        elem[++index].substr = "ock.uwal.rpc.tls.crl.path";
        elem[index].value = g_instance.attr.attr_security.ssl_crl_file;

        elem[++index].substr = "ock.uwal.rpc.tls.cert.path";
        elem[index].value = g_instance.attr.attr_security.ssl_cert_file;

        elem[++index].substr = "ock.uwal.rpc.tls.key.path";
        elem[index].value = g_instance.attr.attr_security.ssl_key_file;

        elem[++index].substr = "ock.uwal.rpc.tls.key.pass.path";
        elem[index].value = g_instance.attr.attr_security.ssl_key_file;

        elem[++index].substr = "ock.uwal.rpc.tls.cipher_suites";
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
    nodeStateInfo->nodeId = g_uwalConfig.id;
    nodeStateInfo->state = NODE_STATE_UP;
    nodeStateInfo->groupId = 0;
    nodeStateInfo->groupLevel = 0;

    NetInfo netInfo;
    netInfo.ipv4Addr = ock_uwal_ipv4_inet_to_int(g_uwalConfig.ip);
    netInfo.port = g_uwalConfig.port;
    netInfo.protocol = NET_PROTOCOL_TCP;
    if (!strcasecmp(g_uwalConfig.protocol, "rdma")) {
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
    UwalrcvWriterState *uwalrcv = GsGetCurrentUwalRcvState();

    NodeStateList *nodeList = (NodeStateList *)palloc0(sizeof(NodeStateList) + sizeof(NodeStateInfo));
    nodeList->localNodeId = g_uwalConfig.id;
    nodeList->masterNodeId = g_uwalConfig.id;
    nodeList->nodeNum = 1;
    nodeList->nodeList[0] = primaryStateInfo;
    int ret = GsUwalSyncNotify(nodeList);
    if (uwalrcv != NULL) {
        SpinLockAcquire(&uwalrcv->mutex);
        uwalrcv->fullSync = false;
        SpinLockRelease(&uwalrcv->mutex);
        ereport(LOG, (errmsg("GsUwalPrimaryInitNotify fullSync false")));
    }

    pfree(nodeList);
    return ret;
}

bool FindSyncRepConfig(IN const char *applicationName, OUT int *group, OUT uint8 *syncrepMethod, OUT unsigned *numSync)
{
    if (!group && !syncrepMethod && !numSync) {
        return false;
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
    UwalrcvWriterState *uwalrcv = GsGetCurrentUwalRcvState();
    NodeStateList *nodeList = (NodeStateList *)palloc0(
        sizeof(NodeStateList) + sizeof(NodeStateInfo) * g_instance.attr.attr_storage.max_wal_senders);
    nodeList->localNodeId = g_uwalConfig.id;
    nodeList->masterNodeId = g_uwalConfig.id;

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
        if (channelGetReplc == 0) {
            continue;
        }

        int group = 0;
        unsigned numSync = 0;
        NodeStateInfo standbyStateInfo;
        bool standbyInSyncRepConfig = false;
        if (strcmp(u_sess->attr.attr_storage.SyncRepStandbyNames, "*") == 0) {
            standbyStateInfo.groupId = 0;
            standbyStateInfo.groupLevel = 1;
            standbyInSyncRepConfig = true;
        } else if (FindSyncRepConfig(remoteApplicationName, &group, NULL, &numSync)) {
            statSendersGroupLevel[group] += 1;
            statSendersGroupNumSync[group] = numSync;
            standbyStateInfo.groupId = group;
            standbyInSyncRepConfig = true;
        } else {
            standbyStateInfo.groupId = 0;
            standbyStateInfo.groupLevel = 0;
            standbyInSyncRepConfig = false;
        }

        SpinLockAcquire(&walsnd->mutex);
        walsnd->standbyInSyncRepConfig = standbyInSyncRepConfig;
        SpinLockRelease(&walsnd->mutex);

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
        if (!strcasecmp(g_uwalConfig.protocol, "rdma") && !strcasecmp(g_uwalConfig.repliNodes[standbyStateInfo.nodeId], "rdma")) {
            netInfo.protocol = NET_PROTOCOL_RDMA;
        }

        NetList netList;
        netList.num = 1;
        netList.list[0] = netInfo;
        standbyStateInfo.netList = netList;
        nodeList->nodeList[count++] = standbyStateInfo;
    }

    bool fullSync = true;
    for (int i = 0; i < count; i++) {
        unsigned group_index = nodeList->nodeList[i].groupId;
        if (statSendersGroupLevel[group_index] < statSendersGroupNumSync[group_index]) {
            nodeList->nodeList[i].groupLevel = statSendersGroupLevel[group_index];
            fullSync = false;
        } else {
            nodeList->nodeList[i].groupLevel = statSendersGroupNumSync[group_index];
        }
    }
    if (!exceptSelf && uwalrcv != NULL) {
        SpinLockAcquire(&uwalrcv->mutex);
        uwalrcv->fullSync = fullSync;
        SpinLockRelease(&uwalrcv->mutex);
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
    nodeList->localNodeId = g_uwalConfig.id;

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
        if (!strcasecmp(g_uwalConfig.protocol, "rdma") && !strcasecmp(g_uwalConfig.repliNodes[primaryStateInfo.nodeId], "rdma")) {
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
    nodeList->localNodeId = g_uwalConfig.id;
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

        SpinLockAcquire(&walSnd->mutex);
        if (!walSnd->standbyInSyncRepConfig) {
            SpinLockRelease(&walSnd->mutex);
            continue;
        }

        walSnd->sentPtr = lsn;
        walSnd->receive = lsn;
        walSnd->write = lsn;
        walSnd->flush = lsn;
        walSnd->peer_role = STANDBY_MODE;
        walSnd->peer_state = NORMAL_STATE;
        SpinLockRelease(&walSnd->mutex);
        walSnd->log_ctrl.prev_send_time = GetCurrentTimestamp();
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

int GsUwalQuery(UwalId *id, UwalBaseInfo *info)
{
    int ret;
    UwalInfo *uwalInfos = (UwalInfo *)palloc0(sizeof(UwalInfo));
    UwalQueryParam params = {id, UWAL_ROUTE_LOCAL, NULL};

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
    desc.dataSize = uwal_size;
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

int GsUwalWrite(UwalId *id, int nBytes, char *buf, UwalNodeInfo *infos)
{
    UwalBuffer uBuff;
    UwalBufferList buffList;
    uint64_t offset;
    UwalAppendParam params;

    uBuff.buf = buf;
    uBuff.len = nBytes;
    buffList.buffers = &uBuff;
    buffList.cnt = 1;

    params.bufferList = &buffList;
    params.uwalId = id;
    params.cb = NULL;

    int ret = ock_uwal_append(&params, &offset, infos);
    return ret;
}

int GsUwalRead(UwalId *id, XLogRecPtr targetPagePtr, char *readBuf, uint64_t readlen)
{
    UwalDataToRead *dataToRead = (UwalDataToRead *)palloc0(sizeof(UwalDataToRead));
    dataToRead->offset = targetPagePtr;
    dataToRead->length = readlen;
    UwalDataToReadVec *datav = (UwalDataToReadVec *)palloc0(sizeof(UwalDataToReadVec));
    datav->cnt = 1;
    datav->dataToRead = dataToRead;
    UwalReadParam params = {id, UWAL_ROUTE_LOCAL, datav, NULL};

    UwalBufferList *bufflist = (UwalBufferList *)palloc0(sizeof(UwalBufferList));
    bufflist->cnt = 1;
    UwalBuffer *buffers = (UwalBuffer *)palloc0(sizeof(UwalBuffer));
    buffers->buf = readBuf;
    buffers->len = readlen;
    bufflist->buffers = buffers;

    int ret = ock_uwal_read(&params, bufflist);

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
            pthread_cond_signal(&(commonCbCtx->cond));
            pthread_mutex_unlock(&(commonCbCtx->mutex));
        } else {
            pthread_mutex_unlock(&(commonCbCtx->mutex));
        }
    }
}

int GsUwalWriteAsync(UwalId *id, int nBytes, char *buf, UwalNodeInfo *infos)
{
    uint64_t offset;
    int bufferOffset = 0;
    int batchIOSize = 0;
    int batchIONumber = 0;
    int batchLastIOSize = 0;
    int ret = 0;
    int pageSize = XLOG_BLCKSZ;
    int totalPages = nBytes / pageSize;
    int minPages = UWAL_MIN_PAGES;
    int workerThreadNum = UWAL_WORKER_NUM;
    if (nBytes % pageSize > 0) {
        totalPages += 1;
    }

    if (totalPages <= workerThreadNum * minPages) {
        batchIONumber = totalPages / minPages;
        if (totalPages % minPages > 0) {
            batchIONumber += 1;
        }
        batchIOSize = minPages * pageSize;
    } else {
        batchIONumber = workerThreadNum;
        int batchPageNumber = totalPages / batchIONumber;
        if (batchPageNumber > 128 && totalPages % batchIONumber > 0) {
            batchPageNumber += 1;
        }
        batchIOSize = batchPageNumber * pageSize;
    }

    batchLastIOSize = nBytes - (batchIONumber - 1) * batchIOSize;

    UwalAsyncAppendCbCtx cbCtx = {0};
    cbCtx.opCount = batchIONumber;
    cbCtx.infos = infos;
    pthread_mutex_init(&(cbCtx.mutex), 0);
    pthread_cond_init(&(cbCtx.cond), 0);

    UwalNodeInfo uwalInfoList[batchIONumber];
    UwalSingleAsyncCbCtx uwalCbCtxList[batchIONumber];

    for (int i = 0; i < batchIONumber; i++) {
        UwalNodeInfo *uwalInfos = &uwalInfoList[i];
        UwalSingleAsyncCbCtx *uwalCbCtx = &uwalCbCtxList[i];

        UwalBuffer buffers[1] = {{buf + bufferOffset, batchIOSize}};
        UwalBufferList bufferList = {1, buffers};
        UwalCallBack uwalCB = {GsUwalWriteAsyncCallBack, uwalCbCtx};
        UwalAppendParam appendParam = {id, &bufferList, &uwalCB};

        uwalCbCtx->commonCbCtx = &cbCtx;
        uwalCbCtx->infos = uwalInfos;
        uwalCbCtx->ret = 0;
        uwalCbCtx->appendParam = &appendParam;
        if (i == 0) {
            buffers[0].len = batchLastIOSize;
            bufferOffset += batchLastIOSize;
        } else {
            bufferOffset += batchIOSize;
        }

        ret = ock_uwal_append(&appendParam, &offset, (void *)uwalInfos);
        if (ret != 0) {
            pthread_mutex_lock(&(cbCtx.mutex));
            cbCtx.interrupted = true;
            cbCtx.interruptCount = i;
            while (cbCtx.interruptCount < cbCtx.curCount) {
                pthread_cond_wait(&(cbCtx.cond), &(cbCtx.mutex));
            }
            pthread_mutex_unlock(&(cbCtx.mutex));
            pthread_mutex_destroy(&(cbCtx.mutex));
            pthread_cond_destroy(&(cbCtx.cond));
            return ret;
        }
    }
    pthread_mutex_lock(&(cbCtx.mutex));
    while (cbCtx.curCount < cbCtx.opCount) {
        pthread_cond_wait(&(cbCtx.cond), &(cbCtx.mutex));
    }
    pthread_mutex_unlock(&(cbCtx.mutex));
    pthread_mutex_destroy(&(cbCtx.mutex));
    pthread_cond_destroy(&(cbCtx.cond));

    ret = cbCtx.ret;
    return ret;
}

void GsUwalRcvStateUpdate(XLogRecPtr lastWrited)
{
    UwalrcvWriterState *uwalrcv = GsGetCurrentUwalRcvState();
    if (uwalrcv == NULL) {
        return;
    }

    SpinLockAcquire(&uwalrcv->writeMutex);
    uwalrcv->writePtr = lastWrited;
    SpinLockRelease(&uwalrcv->writeMutex);
    return;
}

UwalrcvWriterState *GsGetCurrentUwalRcvState(void)
{
    UwalrcvWriterState *uwalrcv = NULL;
    /* use volatile pointer to prevent code rearrangement */
    volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;
    if (walrcv == NULL) {
        return NULL;
    }
    SpinLockAcquire(&walrcv->mutex);
    uwalrcv = walrcv->uwalRcvState;
    SpinLockRelease(&walrcv->mutex);
    return uwalrcv;
}

void GsUwalRcvFlush()
{
    UwalrcvWriterState *uwalrcv = GsGetCurrentUwalRcvState();
    if (uwalrcv == NULL) {
        return;
    }
    SpinLockAcquire(&uwalrcv->mutex);
    uwalrcv->writeNoWait = true;
    SpinLockRelease(&uwalrcv->mutex);
    return;
}

int GsUwalTruncate(UwalId *id, uint64_t offset)
{
    int ret;
    ret = ock_uwal_truncate(id, offset);
    if (ret != 0) {
        ereport(WARNING, (errmsg("GsUwalTruncate return failed ret: %d",ret)));
    }
    return ret;
}