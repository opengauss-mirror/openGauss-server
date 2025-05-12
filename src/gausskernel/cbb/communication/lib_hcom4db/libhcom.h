/*
* Copyright (c) 2025 Huawei Technologies Co.,Ltd.
*
* CBB is licensed under Mulan PSL v2.
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
* libhcom.h
*
*
* IDENTIFICATION
*    src/gausskernel/cbb/communication/lib_hcom4db/libhcom.h
*
* -------------------------------------------------------------------------
 */

#ifndef LIBHCOM_H
#define LIBHCOM_H

#include "hcom4db.h"
#include "../libcomm_common.h"
#include "../libcomm_utils/libcomm_util.h"
#include "../libcomm_utils/libcomm_lqueue.h"
#include "libpq/libpq-be.h"

struct hcom_connect_package {
    char nodename[NAMEDATALEN];
    char host[HOST_ADDRSTRLEN];
};

struct hcom_accept_package {
    uint16 result;
    int nodeindex;
};

struct hcom_msg_head {
    uint16 version;
    uint16 logic_id;
    int nodeindex;
    uint32 msg_len;
    int cIndex;
};

struct HcomSendBuf {
    hcom_msg_head head;
    char msg[STREAM_BUFFER_SIZE];
};

struct ClientPeer {
    int cindex; // 0 ~ 255
    int type;   // 1 connect 2 recv
    OckRpcClient hcomClient;
    char host[HOST_ADDRSTRLEN];
    int hostPort;
    char nodename[NAMEDATALEN];
    int nodeindex;
    pg_atomic_uint32  currAsyncSendCount;
};

using LibOckRpcSetUpCtx = int (*)(OckRpcClient client, uint64_t ctx);
using LibOckRpcGetClient = int (*)(OckRpcServerContext ctx, OckRpcClient *client);
using LibOckRpcGetUpCtx = int (*)(OckRpcClient client, uint64_t *ctx);
using LibOckRpcServerReply = OckRpcStatus (*)(OckRpcServerContext ctx, uint16_t msgId, OckRpcMessage *reply,
                                             OckRpcCallDone *done);
using LibOckRpcServerCleanupCtx = void (*)(OckRpcServerContext ctx);
using LibHcom4dbSetLogFunc = void (*)(OckRpcLogHandler func);
using LibOckRpcServerCreateWithCfg = OckRpcStatus (*)(
    const char *ip, uint16_t port, OckRpcServer *server, OckRpcCreateConfig *configs);
using LibOckRpcServerStart = OckRpcStatus (*)(OckRpcServer server);
using LibOckRpcServerAddService = OckRpcStatus (*)(OckRpcServer server, OckRpcService *service);
using LibOckRpcClientConnectWithCfg = OckRpcStatus (*)(const char *ip, uint16_t port, OckRpcClient *client,
                                                      OckRpcCreateConfig *configs);
using LibOckRpcClientCall = OckRpcStatus (*)(
    OckRpcClient client, uint16_t msgId, OckRpcMessage *request, OckRpcMessage *response, OckRpcCallDone *done);

struct LibHcomAdapt {
    // server
    LibOckRpcServerReply hcom_server_reply;
    LibOckRpcServerCleanupCtx hcom_server_clean;
    LibOckRpcServerCreateWithCfg hcom_server_create;
    LibOckRpcServerStart hcom_server_start;
    LibOckRpcServerAddService hcom_server_add;

    // client
    LibOckRpcClientConnectWithCfg hcom_client_con;
    LibOckRpcClientCall hcom_client_call;

    // common
    LibOckRpcGetClient hcom_get_client;
    LibOckRpcSetUpCtx hcom_set_ctx;
    LibOckRpcGetUpCtx hcom_get_ctx;
    LibHcom4dbSetLogFunc hcom_set_log;
};

// load dll
bool hcom_init_dll(const char *dir);
bool get_dll_status();

// hcom server
int hcom_server_listener_init(const char *host, int port, const char *type = "HCCS");
// end hcom server

// hcom client
int hcom_build_connection(libcommaddrinfo *libcomm_addrinfo, int node_idx);
int hcom_client_sendbuf(LibcommSendInfo *sendInfo);

#endif  // LIBHCOM_H
