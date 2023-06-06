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
 * -------------------------------------------------------------------------
 *
 * rpc.h
 *
 * IDENTIFICATION
 *	  src\include\component\rpc\rpc.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef __RPC_H__
#define __RPC_H__

#include "ndp/ndp_req.h"

#ifndef NDP_CLIENT
#include "component/thread/thread.h"
#include "knl/knl_instance.h"
#endif

constexpr char* LIB_ULOG = "libulog.so";
constexpr char* LIB_SSL = "libssl.so";
constexpr char* LIB_RPC_UCX = "librpc_ucx.so";
constexpr char* LIB_OPENSSL_DL = "libopenssl_dl.so";
constexpr char* LIB_CRYPTO = "libcrypto.so";

typedef enum RpcStatus {
    RPC_ERROR = -1,
    RPC_OK = 0,
} RpcStatus;

typedef enum RpcServiceId {
    RPC_ADMIN_REQ = 0,
    RPC_IO_REQ
} RpcServiceId;

typedef uintptr_t RpcServer;
typedef uintptr_t RpcClient;
typedef uintptr_t RpcServerContext;

/**
 * @brief Message struct.
 */
typedef struct {
    void *data;
    size_t len;
} RpcMessage;

/**
 * @brief Message handler.
 *
 * User can pass the @b ctx and @b msg to other thread, it will remain valid until
 * @ref OckRpcServerCleanupCtx is called. After calling the @ref OckRpcServerReply,
 * user need to call @ref OckRpcServerCleanupCtx to release @b ctx. The lifetime
 * of the memory that @b msg points to is same as @b ctx. So after invoking
 * @ref OckRpcServerCleanupCtx, the @b msg is freed and can not be used anymore.
 */
typedef void(*RpcMsgHandler)(RpcServerContext ctx, RpcMessage msg);

/**
 * @brief RPC call completion callback.
 *
 * @b status is the result of the communication call and @b arg is specified by user.
 */
typedef void(*RpcDoneCallback)(RpcStatus status, void *arg);

/**
 * @brief RPC Service.
 *
 * Each service is a kind of message processing object.
 */
typedef struct {
    uint16_t id; /**  Message ID handled by this service. The range is [0,1024). */
    RpcMsgHandler handler; /**  Message handler. */
} RpcService;

/**
 * @brief RPC call completion handle.
 *
 * This structure should be allocated by the user and can be passed to communication
 * primitives, such as @ref OckRpcClientCall. When the structure object is passed
 * in, the communication routine changes to asynchronous mode. And if the routine
 * returns success, the actual completion result will be notified through this callback.
 */
typedef struct {
    RpcDoneCallback cb; /**  User callback function. */
    void *arg; /**  Argument of callback. */
} RpcCallDone;

typedef struct {
    const char *key;
    const char *value;
} RpcConfigPair;

typedef struct {
    int size;
    RpcConfigPair *pairs;
} RpcConfigs;

typedef struct {
    char *ulogPath;
    char *rpcPath;
    char *sslDLPath;
    char *sslPath;
    char *cryptoPath;
} DependencePath;

#ifdef NDP_CLIENT
RpcStatus RpcClientInit(DependencePath& paths);

RpcStatus RpcClientConnect(char *ip, uint16_t port, RpcClient& clientHandle);
void RpcClientDisconnect(RpcClient clientHandle);

RpcStatus RpcSendAdminReq(NdpAdminRequest* req, NdpAdminResponse* resp, size_t size, RpcClient clientHandle);
RpcStatus RpcSendIOReq(RpcMessage* request, RpcMessage* response, RpcCallDone* done, RpcClient clientHandle);
#else
RpcStatus RpcServerInit(void);

RpcStatus RpcIOTaskHandler(NdpIOTask* task);

RpcStatus SendIOTaskErrReply(NdpIOTask* task, NDP_ERRNO error);
#endif

#endif /* __RPC_H__ */
