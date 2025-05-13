/*
* Copyright (c) 2025 Huawei Technologies Co.,Ltd.
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
* libhcom.cpp
*
* IDENTIFICATION
*    src/gausskernel/cbb/communication/lib_hcom4db/libhcom.cpp
*
* -------------------------------------------------------------------------
*/

#include "libhcom.h"
#include <pthread.h>
#include <dlfcn.h>

constexpr int HCOM_RETRY_USLEEP = 10;
constexpr int HCOM_RETRY_TIMES = 10;
constexpr int BASE_INDEX = 1024 * 1024;
constexpr int MAX_CLIENT_ARRAY_SIZE = 256;
int g_queuesize = 1024 * 1024;
const char* serverName = "hcom_server";
OckRpcServer g_server = 0;
ClientPeer* g_clientArray[MAX_CLIENT_ARRAY_SIZE] = {nullptr};
pg_atomic_uint32 clientIndex;
char g_localHost[HOST_ADDRSTRLEN] = {0};
uint16 localPort;

void *g_hcom_dll = nullptr;
LibHcomAdapt g_hcom_adapt;

bool hcom_init_dll(const char* dir)
{
    if (g_hcom_dll != nullptr) {
        return true;
    }
    if (dir == nullptr) {
        LIBCOMM_ELOG(WARNING, "hcom_init_dll dir is null.");
        return false;
    }
    char hcompath[256] = {0};
    errno_t rc = snprintf_s(hcompath, sizeof(hcompath), sizeof(hcompath) - 1, "%s/%s", dir, "libhcom4db.so");
    securec_check(rc, "\0", "\0");
    g_hcom_dll = dlopen(hcompath, RTLD_LAZY);
    if (g_hcom_dll == nullptr) {
        LIBCOMM_ELOG(WARNING, "hcom_init_dll dlopen[%s] error.", hcompath);
        return false;
    }
    g_hcom_adapt.hcom_server_reply = (LibOckRpcServerReply)dlsym(g_hcom_dll, "OckRpcServerReply");
    g_hcom_adapt.hcom_server_clean = (LibOckRpcServerCleanupCtx)dlsym(g_hcom_dll, "OckRpcServerCleanupCtx");
    g_hcom_adapt.hcom_server_create = (LibOckRpcServerCreateWithCfg)dlsym(g_hcom_dll, "OckRpcServerCreateWithCfg");
    g_hcom_adapt.hcom_server_start = (LibOckRpcServerStart)dlsym(g_hcom_dll, "OckRpcServerStart");
    g_hcom_adapt.hcom_server_add = (LibOckRpcServerAddService)dlsym(g_hcom_dll, "OckRpcServerAddService");
    g_hcom_adapt.hcom_client_con = (LibOckRpcClientConnectWithCfg)dlsym(g_hcom_dll, "OckRpcClientConnectWithCfg");
    g_hcom_adapt.hcom_client_call = (LibOckRpcClientCall)dlsym(g_hcom_dll, "OckRpcClientCall");
    g_hcom_adapt.hcom_get_client = (LibOckRpcGetClient)dlsym(g_hcom_dll, "OckRpcGetClient");
    g_hcom_adapt.hcom_set_ctx = (LibOckRpcSetUpCtx)dlsym(g_hcom_dll, "OckRpcSetUpCtx");
    g_hcom_adapt.hcom_get_ctx = (LibOckRpcGetUpCtx)dlsym(g_hcom_dll, "OckRpcGetUpCtx");
    g_hcom_adapt.hcom_set_log = (LibHcom4dbSetLogFunc)dlsym(g_hcom_dll, "Hcom4dbSetLogFunc");
    if (g_hcom_adapt.hcom_server_reply == nullptr || g_hcom_adapt.hcom_server_clean == nullptr ||
        g_hcom_adapt.hcom_server_create == nullptr || g_hcom_adapt.hcom_server_start == nullptr ||
        g_hcom_adapt.hcom_server_add == nullptr || g_hcom_adapt.hcom_client_con == nullptr ||
        g_hcom_adapt.hcom_client_call == nullptr || g_hcom_adapt.hcom_get_client == nullptr ||
        g_hcom_adapt.hcom_set_ctx == nullptr || g_hcom_adapt.hcom_get_ctx == nullptr ||
        g_hcom_adapt.hcom_set_log == nullptr) {
        LIBCOMM_ELOG(WARNING, "hcom_init_dll dlsym error msg[%s].", dlerror());
        (void)dlclose(g_hcom_dll);
        g_hcom_dll = nullptr;
        return false;
    }
    return true;
}

bool get_dll_status()
{
    return g_hcom_dll != nullptr;
}

static ClientPeer* get_new_client_handle(int type, int port = 0, const char* host = nullptr,
                                         const char* node_name = nullptr)
{
    uint32 clientid = pg_atomic_fetch_add_u32(&clientIndex, 1);
    if (clientid >= MAX_CLIENT_ARRAY_SIZE) {
        return nullptr;
    }
    errno_t ss_rc = 0;
    ClientPeer* cPeer = (ClientPeer*)malloc(sizeof(ClientPeer));
    if (cPeer == nullptr) {
        return nullptr;
    }
    cPeer->cindex = clientid;
    cPeer->type = type;
    cPeer->hostPort = port;
    cPeer->hcomClient = 0;
    if (host != nullptr) {
        ss_rc = strcpy_s(cPeer->host, HOST_ADDRSTRLEN, host);
        securec_check_c(ss_rc, "\0", "\0");
    }
    g_clientArray[clientid] = cPeer;
    pg_atomic_init_u32(&(cPeer->currAsyncSendCount), 0);
    return cPeer;
}

// hcom server
static int hcom_handle_accept(OckRpcClient client, uint64_t usrCtx, const char* payLoad)
{
    ClientPeer* cPeer = get_new_client_handle(2);
    if (cPeer == nullptr) {
        return OCK_RPC_ERR;
    }
    errno_t ret = sscanf_s(payLoad, "%d", &cPeer->hostPort);
    securec_check_for_sscanf_s(ret, 1, "\0", "\0");
    cPeer->hcomClient = client;
    g_hcom_adapt.hcom_set_ctx(client, (uint64_t)cPeer);
    return OCK_RPC_OK;
}

static int hcom_handle_close(OckRpcClient client, uint64_t usrCtx, const char* payLoad)
{
    return OCK_RPC_OK;
}

// gs_accept_data_connection
static void hcom_handle_CMD(OckRpcServerContext ctx, OckRpcMessage msg)
{
    hcom_connect_package* connect_pkg = (hcom_connect_package*)msg.data;
    OckRpcClient client;
    g_hcom_adapt.hcom_get_client(ctx, &client);
    ClientPeer* cPeer;
    hcom_accept_package ack_msg = {1, -1};
    OckRpcMessage rsp = {.data = (void*)&ack_msg, .len = sizeof(hcom_accept_package)};
    g_hcom_adapt.hcom_get_ctx(client, (uint64_t*)&cPeer);
    errno_t ss_rc = strcpy_s(cPeer->host, HOST_ADDRSTRLEN, connect_pkg->host);
    securec_check_c(ss_rc, "\0", "\0");
    int node_idx = -1;
    node_idx = gs_get_node_idx(connect_pkg->nodename, true);
    ack_msg.nodeindex = node_idx;
    if (unlikely(node_idx < 0)) {
        g_hcom_adapt.hcom_server_reply(ctx, 0, &rsp, nullptr);
        g_hcom_adapt.hcom_server_clean(ctx);
        return;
    }
    struct sock_id fd_id;
    fd_id.id = cPeer->cindex + BASE_INDEX;
    fd_id.id = 0;
    if (gs_map_sock_id_to_node_idx(fd_id, node_idx) < 0) {
        g_hcom_adapt.hcom_server_reply(ctx, 0, &rsp, nullptr);
        g_hcom_adapt.hcom_server_clean(ctx);
        return;
    }
    LIBCOMM_PTHREAD_RWLOCK_WRLOCK(&g_instance.comm_cxt.g_receivers->receiver_conn[node_idx].rwlock);
    g_instance.comm_cxt.g_receivers->receiver_conn[node_idx].socket = fd_id.fd;
    g_instance.comm_cxt.g_receivers->receiver_conn[node_idx].socket_id = fd_id.id;
    g_instance.comm_cxt.g_receivers->receiver_conn[node_idx].msg_head.type = MSG_NULL;
    g_instance.comm_cxt.g_receivers->receiver_conn[node_idx].head_read_cursor = 0;
    if (g_instance.comm_cxt.g_receivers->receiver_conn[node_idx].iov_item) {
        struct iovec* iov_data = g_instance.comm_cxt.g_receivers->receiver_conn[node_idx].iov_item->element.data;
        iov_data->iov_len = 0;
    }
    LIBCOMM_PTHREAD_RWLOCK_UNLOCK(&g_instance.comm_cxt.g_receivers->receiver_conn[node_idx].rwlock);
    ss_rc = strcpy_s(g_instance.comm_cxt.g_r_node_sock[node_idx].remote_host, HOST_ADDRSTRLEN, connect_pkg->host);
    securec_check_c(ss_rc, "\0", "\0");
    ack_msg.result = 0;
    g_hcom_adapt.hcom_server_reply(ctx, 0, &rsp, nullptr);
    g_hcom_adapt.hcom_server_clean(ctx);
}

static void hcom_handle_recv(OckRpcServerContext ctx, OckRpcMessage msg)
{
    HcomSendBuf* sendbuf = (HcomSendBuf*)msg.data;
    int idx = sendbuf->head.nodeindex;
    int streamid = sendbuf->head.logic_id;
    int version = sendbuf->head.version;
    if (idx < 0 || sendbuf->head.logic_id <= 0) {
        g_hcom_adapt.hcom_server_clean(ctx);
        return;
    }
    struct mc_lqueue_item* iov_item = nullptr;
    while (0 != libcomm_malloc_iov_item_for_hcom(&iov_item)) {
        pg_usleep(HCOM_RETRY_USLEEP);
    }
    memcpy_s(iov_item->element.data->iov_base, IOV_DATA_SIZE, sendbuf->msg, sendbuf->head.msg_len);
    iov_item->element.data->iov_len = sendbuf->head.msg_len;
    struct c_mailbox* cmailbox = &C_MAILBOX(idx, streamid);
    while (gs_push_cmailbox_buffer(cmailbox, iov_item, version, true) < 0) {
        pg_usleep(HCOM_RETRY_USLEEP);
    }
    g_hcom_adapt.hcom_server_clean(ctx);
}

int hcom_server_listener_init(const char* host, int port, const char* type)
{
    if (g_server != 0) {
        LIBCOMM_ELOG(WARNING, "(hcom server) init detail[%s:%d] on type[%s]", host, port, type);
        return (BASE_INDEX - 1);
    }
    OckRpcCreateConfig configs = {0};
    int configNum = 10;
    configs.mask = OCK_RPC_CONFIG_USE_RPC_CONFIGS;
    RpcConfigPair pairs[configNum];
    configs.configs.size = configNum;
    configs.configs.pairs = pairs;
    pairs[0] = (RpcConfigPair){.key = OCK_RPC_STR_WRK_THR_GRP, .value = "4"};
    pairs[1] = (RpcConfigPair){.key = OCK_RPC_STR_SERVER_NAME, .value = serverName};
    pairs[2] = (RpcConfigPair){.key = OCK_RPC_STR_COMMUNICATION_TYPE, .value = type};
    uintptr_t newConnectPtr = (uintptr_t)&hcom_handle_accept;
    pairs[3] = (RpcConfigPair){.key = OCK_RPC_STR_CLIENT_NEW, .value = (char*)newConnectPtr};
    uintptr_t disConnectPtr = (uintptr_t)&hcom_handle_close;
    pairs[4] = (RpcConfigPair){.key = OCK_RPC_STR_CLIENT_BREAK, .value = (char*)disConnectPtr};
    pairs[5] = (RpcConfigPair){.key = OCK_RPC_STR_HEATBEAT_IDLE, .value = "60"};
    pairs[6] = (RpcConfigPair){.key = OCK_RPC_STR_PRE_SEGMENT_COUNT, .value = "2048"};
    pairs[7] = (RpcConfigPair){.key = OCK_RPC_STR_PRE_SEGMENT_SIZE, .value = "33768"};
    pairs[8] = (RpcConfigPair){.key = OCK_RPC_STR_ENABLE_POLL_WORKER, .value = "yes"};
    pairs[9] = (RpcConfigPair){.key = OCK_RPC_STR_WRK_CPU_SET, .value = "1-4"};
    g_hcom_adapt.hcom_set_log(nullptr);
    if (g_hcom_adapt.hcom_server_create(host, port, &g_server, &configs) != OCK_RPC_OK) {
        LIBCOMM_ELOG(WARNING, "(hcom listen)\tFailed detail[%s:%d] on type[%s]", host, port, type);
        return -1;
    }
    if (g_hcom_adapt.hcom_server_start(g_server) != OCK_RPC_OK) {
        LIBCOMM_ELOG(WARNING, "(hcom start server)\tFailed detail[%s:%d] on type[%s]", host, port, type);
        return -1;
    }

    OckRpcService handleCMDPacket = (OckRpcService){.id = 0, .handler = hcom_handle_CMD};
    if (g_hcom_adapt.hcom_server_add(g_server, &handleCMDPacket) != OCK_RPC_OK) {
        LIBCOMM_ELOG(WARNING, "(hcom add server CMD)\tFailed detail[%s:%d] on type[%s]", host, port, type);
        return -1;
    }
    OckRpcService handleRecvPacket = (OckRpcService){.id = 1, .handler = hcom_handle_recv};
    if (g_hcom_adapt.hcom_server_add(g_server, &handleRecvPacket)) {
        LIBCOMM_ELOG(WARNING, "(hcom add server Buf)\tFailed detail[%s:%d] on type[%s]", host, port, type);
        return -1;
    }

    errno_t rc = memcpy_s(g_localHost, HOST_ADDRSTRLEN - 1, host, strlen(host));
    securec_check_c(rc, "\0", "\0");
    localPort = port;
    pg_atomic_init_u32(&clientIndex, 0);
    g_queuesize = (g_instance.comm_cxt.commutil_cxt.g_memory_pool_size / IOV_ITEM_SIZE);
    struct mc_lqueue_item* iov_item[g_queuesize] = {nullptr};
    for (int i = 0; i < g_queuesize; i++) {
        if (libcomm_malloc_iov_item(&(iov_item[i]), IOV_DATA_SIZE) != 0) {
            LIBCOMM_ELOG(WARNING, "hcom_server_listener_init malloc iov_item fail index[%d]", i);
        }
    }
    for (int i = 0; i < g_queuesize; i++) {
        libcomm_free_iov_item(&(iov_item[i]), IOV_DATA_SIZE);
    }
    LIBCOMM_ELOG(LOG, "hcom_Server_listener_init success g_queuesize[%d]", g_queuesize);
    return (BASE_INDEX - 1);
}
// end hcom server

// hcom client
static int hcom_client_connect(const char* host, int port)
{
    ClientPeer *cPeer = get_new_client_handle(1, port, host, g_instance.comm_cxt.localinfo_cxt.g_self_nodename);
    if (cPeer == nullptr) {
        LIBCOMM_ELOG(ERROR, "hcom get_new_client_handle Filed detail[%s:%d]", host, port);
        return -1;
    }
    if (port == localPort && 0 == strcmp(host, g_localHost)) {
        LIBCOMM_ELOG(LOG, "hcom_client_connect connect self[%s:%d] clientid[%d]",
                     host, port, cPeer->cindex + BASE_INDEX);
        return -1;
    }
    if (cPeer->hcomClient != 0) {
        LIBCOMM_ELOG(WARNING, "hcom Client[%d] is not 0", cPeer->cindex);
    }

    OckRpcCreateConfig configs;
    configs.mask = OCK_RPC_CONFIG_USE_RPC_CONFIGS;
    int configPairSize = 5;
    configs.configs.size = configPairSize;
    RpcConfigPair pairs[configPairSize];
    configs.configs.pairs = pairs;

    pairs[0] = (RpcConfigPair){.key = OCK_RPC_STR_SERVER_NAME, .value = serverName};
    char payLoadInfo[6] = {0};
    errno_t rc = snprintf_s(payLoadInfo, sizeof(payLoadInfo), sizeof(payLoadInfo) - 1, "%u", localPort);
    securec_check_ss_c(rc, "\0", "\0");
    pairs[1] = (RpcConfigPair){.key = OCK_RPC_STR_CLIENT_CREATE_INFO, .value = payLoadInfo};
    pairs[2] = (RpcConfigPair){.key = OCK_RPC_STR_ENABLE_SELFPOLLING, .value = "no"};
    pairs[3] = (RpcConfigPair){.key = OCK_RPC_STR_PRE_SEGMENT_COUNT, .value = "0"};
    pairs[4] = (RpcConfigPair){.key = OCK_RPC_STR_PRE_SEGMENT_SIZE, .value = "4096"};
    while (OCK_RPC_OK != g_hcom_adapt.hcom_client_con(host, port, &(cPeer->hcomClient), &configs)) {
        LIBCOMM_ELOG(WARNING, "(hcom connect)\tFailed detail[%s:%d] playinfo[%s].", host, port, payLoadInfo);
    }
    g_hcom_adapt.hcom_set_ctx(cPeer->hcomClient, (uint64_t)cPeer);
    LIBCOMM_ELOG(WARNING, "hcom_client_connect connect success [%s:%d] clientid[%d]",
                 host, port, cPeer->cindex + BASE_INDEX);
    return (cPeer->cindex + BASE_INDEX);
}

static int hcom_client_sendcmd(const int clientid, const char* msg, int msglen)
{
    if (msg == nullptr || msglen <= 0) {
        LIBCOMM_ELOG(WARNING, "hcom hcom_client_sendcmd Failed msg[%s:%d]", msg, msglen);
        return -1;
    }
    int cIndex = clientid - BASE_INDEX;
    if (cIndex < 0 || cIndex >= MAX_CLIENT_ARRAY_SIZE) {
        LIBCOMM_ELOG(WARNING, "hcom hcom_client_sendcmd Failed clientid[%d]", cIndex);
        return -1;
    }
    if (g_clientArray[cIndex]->hcomClient == 0) {
        LIBCOMM_ELOG(WARNING, "hcom hcom_client_sendcmd self[%d]", clientid);
        return 0;
    }

    OckRpcMessage packet = {.data = (void*)msg, .len = msglen};
    struct hcom_accept_package ack_msg;
    OckRpcMessage resp = {.data = (void*)&ack_msg, .len = sizeof(hcom_accept_package)};
    OckRpcStatus rpc_status = g_hcom_adapt.hcom_client_call(g_clientArray[cIndex]->hcomClient, 0, &packet,
                                                            &resp, nullptr);
    for (int retry_count = 0; retry_count < HCOM_RETRY_TIMES && rpc_status != OCK_RPC_OK; ++retry_count) {
        pg_usleep(HCOM_RETRY_USLEEP);
        rpc_status = g_hcom_adapt.hcom_client_call(g_clientArray[cIndex]->hcomClient, 0, &packet, &resp, nullptr);
    }
    if (rpc_status != OCK_RPC_OK) {
        LIBCOMM_ELOG(WARNING, "(hcom OckRpcClientCall) failed remote[%d: %lu]",
                     cIndex, g_clientArray[cIndex]->hcomClient);
        return -1;
    }
    g_clientArray[cIndex]->nodeindex = ack_msg.nodeindex;
    if (ack_msg.result != 0 || ack_msg.nodeindex < 0) {
        LIBCOMM_ELOG(WARNING, "hcom_clinet_sendcmd success remote[%d] resp[%d, %d].",
                     clientid, ack_msg.result, ack_msg.nodeindex);
        return -1;
    }
    return 0;
}

int hcom_build_connection(libcommaddrinfo* libcomm_addrinfo, int node_idx)
{
    struct sock_id fd_id = {-1, -1};
    ip_key addr;
    errno_t ss_rc = 0;
    uint32 cpylen;
    int sock = hcom_client_connect(libcomm_addrinfo->host, libcomm_addrinfo->listen_port);
    if (sock < 0) {
        LIBCOMM_ELOG(WARNING,
            "(s|build hcom connection)\tFailed to build data connection "
            "to %s:%d for node[%d]:%s",
            libcomm_addrinfo->host,
            libcomm_addrinfo->listen_port,
            node_idx,
            g_instance.comm_cxt.g_s_node_sock[node_idx].remote_nodename);
        return -1;
    }
    LIBCOMM_ELOG(LOG, "hcom_client_connect connect success [%s:%d] clientid[%d] nodeid[%d]",
                 libcomm_addrinfo->host, libcomm_addrinfo->listen_port, sock, node_idx);
    g_instance.attr.attr_network.comm_data_channel_conn[node_idx -1]->socket = sock;
    fd_id.fd = sock;
    fd_id.id = 0;
    struct hcom_connect_package connect_package;
    ss_rc = strcpy_s(connect_package.nodename, NAMEDATALEN, g_instance.comm_cxt.localinfo_cxt.g_self_nodename);
    securec_check_c(ss_rc, "\0", "\0");
    ss_rc = strcpy_s(connect_package.host, HOST_ADDRSTRLEN, g_instance.comm_cxt.localinfo_cxt.g_local_host);
    securec_check_c(ss_rc, "\0", "\0");
    if (hcom_client_sendcmd(sock, (char*)&connect_package, sizeof(hcom_connect_package)) != 0) {
        LIBCOMM_ELOG(WARNING,
            "(s|build hcom connection)\tFailed to send assoc id to %s:%d "
            "for node[%d]:%s on socket[%d]",
            libcomm_addrinfo->host,
            libcomm_addrinfo->listen_port,
            node_idx,
            g_instance.comm_cxt.g_s_node_sock[node_idx].remote_nodename);
        return -1;
    }
    if (gs_map_sock_id_to_node_idx(fd_id, node_idx) < 0) {
        LIBCOMM_ELOG(WARNING, "(s|build hcom connection)\tFailed to save sock end sockid.");
        return -1;
    }
    g_instance.comm_cxt.g_senders->sender_conn[node_idx].ip_changed = true;
    LIBCOMM_PTHREAD_RWLOCK_WRLOCK(&g_instance.comm_cxt.g_senders->sender_conn[node_idx].rwlock);
    if (strcmp(g_instance.comm_cxt.g_s_node_sock[node_idx].remote_host, libcomm_addrinfo->host) != 0) {
        g_instance.comm_cxt.g_senders->sender_conn[node_idx].ip_changed = false;
        LIBCOMM_PTHREAD_RWLOCK_UNLOCK(&g_instance.comm_cxt.g_senders->sender_conn[node_idx].rwlock);
        return -1;
    }
    cpylen = comm_get_cpylen(libcomm_addrinfo->host, HOST_ADDRSTRLEN);
    ss_rc = memset_s(
        g_instance.comm_cxt.g_senders->sender_conn[node_idx].remote_host, HOST_ADDRSTRLEN, 0x0, HOST_ADDRSTRLEN);
    securec_check_c(ss_rc, "\0", "\0");
    ss_rc = strncpy_s(g_instance.comm_cxt.g_senders->sender_conn[node_idx].remote_host,
                      HOST_ADDRSTRLEN,
                      libcomm_addrinfo->host,
                      cpylen + 1);
    securec_check_c(ss_rc, "\0", "\0");
    g_instance.comm_cxt.g_senders->sender_conn[node_idx].port = libcomm_addrinfo->listen_port;
    g_instance.comm_cxt.g_senders->sender_conn[node_idx].socket = fd_id.fd;
    g_instance.comm_cxt.g_senders->sender_conn[node_idx].socket_id = fd_id.id;
    g_instance.comm_cxt.g_senders->sender_conn[node_idx].ip_changed = false;
    cpylen = comm_get_cpylen(libcomm_addrinfo->host, HOST_LEN_OF_HTAB);
    ss_rc = memset_s(addr.ip, HOST_LEN_OF_HTAB, 0x0, HOST_LEN_OF_HTAB);
    securec_check_c(ss_rc, "\0", "\0");
    ss_rc = strncpy_s(addr.ip, HOST_LEN_OF_HTAB, libcomm_addrinfo->host, cpylen + 1);
    securec_check_c(ss_rc, "\0", "\0");
    addr.ip[cpylen] = '\0';

    addr.port = libcomm_addrinfo->listen_port;
    /* update connection state to succeed when connect succeed */
    gs_update_connection_state(addr, CONNSTATESUCCEED, true, node_idx);

    LIBCOMM_PTHREAD_RWLOCK_UNLOCK(&g_instance.comm_cxt.g_senders->sender_conn[node_idx].rwlock);
    LIBCOMM_ELOG(LOG,
        "hcom_build_connection Succeed to connect %s:%d with wocket[%d:%d] for node[%d]:%s",
        libcomm_addrinfo->host,
        libcomm_addrinfo->listen_port,
        fd_id.fd,
        fd_id.id,
        node_idx,
        REMOTE_NAME(g_instance.comm_cxt.g_s_node_sock, node_idx));
    return 0;
}

static void hcom_free_sentbuf(OckRpcStatus status, void* arg)
{
    HcomSendBuf* sendbuf = (HcomSendBuf*)arg;
    int cIndex = sendbuf->head.cIndex;
    free(sendbuf);
}

int hcom_client_sendbuf(LibcommSendInfo* sendInfo)
{
    int node_idx = sendInfo->node_idx;
    HcomSendBuf* sendbuf = (HcomSendBuf*)malloc(sizeof(HcomSendBuf));
    if (sendbuf == nullptr) {
        return -1;
    }
    hcom_msg_head* msg_head = &(sendbuf->head);
    msg_head->version = sendInfo->version;
    msg_head->logic_id = sendInfo->streamid;
    msg_head->msg_len = sendInfo->msg_len;
    errno_t ss_rc = memcpy_s(sendbuf->msg, STREAM_BUFFER_SIZE, sendInfo->msg, msg_head->msg_len);
    securec_check_c(ss_rc, "\0", "\0");
    int cIndex = g_instance.comm_cxt.g_senders->sender_conn[node_idx].socket - BASE_INDEX;
    if (cIndex < 0 || cIndex >= MAX_CLIENT_ARRAY_SIZE) {
        LIBCOMM_ELOG(WARNING, "hcom hcom_client_sendbuf Failed clientid[%d]", cIndex);
        return -1;
    }
    if (g_clientArray[cIndex]->hcomClient == 0) {
        LIBCOMM_ELOG(WARNING, "hcom hcom_client_sendbuf self[%d]", cIndex);
        return 0;
    }
    msg_head->nodeindex = g_clientArray[cIndex]->nodeindex;
    msg_head->cIndex = cIndex;
    OckRpcMessage packet = {.data = (void*)sendbuf, .len = (msg_head->msg_len + sizeof(hcom_msg_head))};
    OckRpcCallDone done = {.cb = hcom_free_sentbuf, .arg = (void*)sendbuf};

    while (g_hcom_adapt.hcom_client_call(g_clientArray[cIndex]->hcomClient, 1, &packet, nullptr, &done) != OCK_RPC_OK) {
        LIBCOMM_ELOG(WARNING, "hcom hcom_client_sendbuf failed remote[%d:%lu]",
                     cIndex, g_clientArray[cIndex]->hcomClient);
    }
    LIBCOMM_ELOG(LOG, "hcom hcom_client_sendbuf success remote client[%d] streamid[%d], version[%d], len[%d], [%ld]",
                 (cIndex + BASE_INDEX), sendInfo->streamid, sendInfo->version, sendInfo->msg_len, packet.len);
    return sendInfo->msg_len;
}
// end hcom client