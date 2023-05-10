/*
 * Copyright (c) 2021 Huawei Technologies Co.,Ltd.
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
 * rpc.cpp
 *
 * IDENTIFICATION
 *      src\common\component\rpc\rpc.cpp
 *
 * -------------------------------------------------------------------------
 */
#include <unistd.h>
#include "utils/dynloader.h"
#include "component/rpc/rpc.h"

#ifdef NDP_CLIENT
#include "utils/elog.h"
#include "knl/knl_session.h"
#else
#include "utils/log.h"
#include "utils/config.h"
#include "ndp/ndp.h"
#include "securec_check.h"
#endif

#define CHECK_RPC_STATUS(status) if ((status) != STATUS_OK) return RPC_ERROR
#define CHECK_NDP_RPC_STATUS(status) if ((status) != RPC_OK) return RPC_ERROR

#ifdef ENABLE_SSL
#define OCK_RPC_CONFIG_USE_SSL_CALLBACK (1ul << (2))
typedef uintptr_t OckRpcServerContext;
using OckRpcServerCtxBuilderHandler = OckRpcServerContext (*)(RpcServer server);
using OckRpcServerCtxCleanupHandler = void (*)(RpcServer server, OckRpcServerContext ctx);

/** @brief TLS callbacks */
/**
 * @brief Keypass erase function
 * @param keypass       the memory address of keypass
 */
using OckRpcTlsKeypassErase = void (*)(char *keypass);

/**
 * @brief Get private key file's path and length, and get the keypass
 * @param priKeyPath    the path of private key
 * @param keypass       the keypass
 * @param erase         the erase function
 */
using OckRpcTlsGetPrivateKey = void (*)(const char **priKeyPath, char **keypass, OckRpcTlsKeypassErase *erase);
/**
 * @brief Get the certificate file of public key
 * @param certPath      the path of certificate
 */
using OckRpcTlsGetCert = void (*)(const char **certPath);

/**
 * @brief The cert verify function
 * @param x509          the X509_STORE_CTX object of CA
 * @param crlPath       the crl file path
 *
 * @return -1 for failed, and 1 for success
 */
using OckRpcTlsCertVerify = int (*)(void *x509, const char *crlPath);
/**
 * @brief Get the CA and verify
 * @param caPath        the path of CA file
 * @param crlPath       the crl file path
 * @param verify        the verify function
 */
using OckRpcTlsGetCAAndVerify = void (*)(const char **caPath, const char **crlPath, OckRpcTlsCertVerify *verify);

typedef struct {
    /* Must enable special bit before you set config value OckRpcCreateConfigMask */
    uint64_t mask;

    /* Set Key-Value mode to config, must enable OCK_RPC_CONFIG_USE_RPC_CONFIGS */
    RpcConfigs configs;

    /* Set user define Server Ctx build and cleanup handler, must enable OCK_RPC_CONFIG_USE_SERVER_CTX_BUILD */
    OckRpcServerCtxBuilderHandler serverCtxbuilder;
    OckRpcServerCtxCleanupHandler serverCtxCleanup;

    /**
     * Set SSL handler, must enable OCK_RPC_CONFIG_USE_SSL_CALLBACK
     *
     * In Server side getCert and getPriKey can't be nullptr
     * In Client side getCaAndVerify can't be nullptr
     */
    OckRpcTlsGetCAAndVerify getCaAndVerify; /* get the CA path and verify callback. */
    OckRpcTlsGetCert getCert;               /* get the certificate file of public key */
    OckRpcTlsGetPrivateKey getPriKey;       /* get the private key and keypass */
} OckRpcCreateConfig;

#ifdef NDP_CLIENT
using ClientConnectWithCfg = RpcStatus (*)(const char* ip, uint16_t port, RpcClient* client, OckRpcCreateConfig* cfg);
#else
using ServerCreateWithCfg = RpcStatus (*)(const char* ip, uint16_t port, RpcServer* server, OckRpcCreateConfig* cfg);
#endif

#endif

#ifdef NDP_CLIENT
using ClientConnect = RpcStatus (*)(const char *ip, uint16_t port, RpcClient *client);
using ClientDisconnect = void (*)(RpcClient client);
using ClientCall = RpcStatus (*)(RpcClient client, uint16_t msgId, RpcMessage *request, RpcMessage *response,
                                 RpcCallDone *done);
using ClientSetTimeout = void (*)(RpcClient client, int64_t timeout);
typedef struct RpcUcxFunc {
    ClientConnect clientConnect;
#ifdef ENABLE_SSL
    ClientConnectWithCfg clientConnectWithCfg;
#endif
    ClientDisconnect clientDisconnect;
    ClientCall clientCall;
    ClientSetTimeout clientSetTimeout;
} RpcUcxFunc;

#else
using ServerCreate = RpcStatus (*)(const char *ip, uint16_t port, RpcServer *server);
using ServerAddService =  RpcStatus (*)(RpcServer server, RpcService *service);
using ServerStart = RpcStatus (*)(RpcServer server);
using ServerDestroy = void (*)(RpcServer server);
using ServerReply = RpcStatus (*)(RpcServerContext ctx, uint16_t msgId, RpcMessage *reply, RpcCallDone *done);
using ServerCleanupCtx = void (*)(RpcServerContext ctx);
typedef struct RpcUcxFunc {
    ServerCreate serverCreate;
#ifdef ENABLE_SSL
    ServerCreateWithCfg serverCreateWithCfg;
#endif
    ServerAddService serverAddService;
    ServerStart serverStart;
    ServerDestroy serverDestroy;
    ServerReply serverReply;
    ServerCleanupCtx serverCleanCtx;
} RpcUcxFunc;
#endif

using ULOG_Init = void (*)(int x, int y, std::nullptr_t ptr, int z, int i);
using SetOpensslDLopenLibPath = int (*)(const char *ssl, const char *crypto);

constexpr int64_t REPLY_TIMEOUT = 60000;

void *g_rpcUcxDl = nullptr;
RpcUcxFunc g_rpcUcxFunc;

#ifdef ENABLE_SSL
#ifdef NDP_CLIENT
int tlsCertVerify(void *x509, const char *crlPath)
{
    // rpc has basic verify, we don't add extra verify process, so return true directly
    return 1;
}

void GetCAAndVerify(const char **caPath, const char **crlPath, OckRpcTlsCertVerify *verify)
{
    *caPath = u_sess->ndp_cxt.ca_path;
    *crlPath = u_sess->ndp_cxt.crl_path;
    *verify = tlsCertVerify;
    return;
}
#else
void KeypassErase(char *keypass)
{
    if (keypass != nullptr) {
        free(keypass);
    }
}
void GetCert(const char **certPath)
{
    *certPath = configSets->certPath.c_str();
}
void GetPrivateKey(const char **priKeyPath, char **keypass, OckRpcTlsKeypassErase *erase)
{
    *priKeyPath = configSets->priKeyPath.c_str();
    *erase = KeypassErase;
    *keypass = (char*)malloc(configSets->keypass.length() + 1);
    if (*keypass == nullptr) {
        LOG_ERROR << "malloc failed, keypass copy failed.";
    }
    // keypass need encrypt further
    errno_t rc = memcpy_s(*keypass, configSets->keypass.length() + 1, configSets->keypass.c_str(),
                          configSets->keypass.length() + 1);
    securec_check(rc, "", "");
}
#endif

RpcStatus InitSslDl(char *sslDlPath, char* sslPath, char* cryptoPath)
{
    if (sslDlPath == NULL || sslPath == NULL || cryptoPath == NULL) {
#ifdef NDP_CLIENT
        ereport(WARNING, (errmsg("InitRpcDl failed, path is null")));
#else
        LOG_ERROR << "InitRpcDl failed, path is null";
#endif
        return RPC_ERROR;
    }

    if (g_rpcUcxDl != NULL) {
        return RPC_OK;
    }

    /* load ulog */
    void *sslDl;
    CHECK_RPC_STATUS(OpenDl(&sslDl, sslDlPath));

    /* init ulog */
    SetOpensslDLopenLibPath setSSLDlPath;
    CHECK_RPC_STATUS(LoadSymbol(sslDl, "SetOpensslDLopenLibPath", (void **)&setSSLDlPath));
    setSSLDlPath("sslPath", "cryptoPath");

    return RPC_OK;
}

#endif

RpcStatus InitRpcDl(char *path)
{
    if (path == nullptr) {
#ifdef NDP_CLIENT
        ereport(WARNING, (errmsg("dlopen rpc_ucx path is nullptr")));
#else
        LOG_ERROR << "dlopen rpc_ucx path is nullptr";
#endif
        return RPC_ERROR;
    }

    if (g_rpcUcxDl != nullptr) {
        return RPC_OK;
    }

    CHECK_RPC_STATUS(OpenDl(&g_rpcUcxDl, path));

    return RPC_OK;
}

/**
 * load ulog from so, only need to use once, before InitRpcDl
 * @param ulogPath
 * @return
 */
RpcStatus LoadUlog(char* ulogPath)
{
    if (ulogPath == nullptr) {
#ifdef NDP_CLIENT
        ereport(WARNING, (errmsg("dlopen ulog path is nullptr")));
#else
        LOG_ERROR << "dlopen ulog path is nullptr";
#endif
        return RPC_ERROR;
    }

    /* load ulog */
    void *ulog;
    CHECK_RPC_STATUS(OpenDl(&ulog, ulogPath));

    /* init ulog */
    ULOG_Init ulogInit;
    CHECK_RPC_STATUS(LoadSymbol(ulog, "ULOG_Init", (void **)&ulogInit));

    ulogInit(0, 3, nullptr, 0, 0);
    CloseDl(ulog);
    return RPC_OK;
}

RpcStatus InitRpcEnv(DependencePath paths)
{
    CHECK_NDP_RPC_STATUS(LoadUlog(paths.ulogPath));

#ifdef ENABLE_SSL
    CHECK_NDP_RPC_STATUS(InitSslDl(paths.sslDLPath, paths.sslPath, paths.cryptoPath));
#endif

    CHECK_NDP_RPC_STATUS(InitRpcDl(paths.rpcPath));

    return RPC_OK;
}

#ifndef NDP_CLIENT
static RpcStatus RpcServerDlsym(void)
{
    CHECK_RPC_STATUS(LoadSymbol(g_rpcUcxDl, "OckRpcServerCreate", (void **)&g_rpcUcxFunc.serverCreate));

#ifdef ENABLE_SSL
    CHECK_RPC_STATUS(LoadSymbol(g_rpcUcxDl, "OckRpcServerCreateWithCfg", (void **)&g_rpcUcxFunc.serverCreateWithCfg));
#endif

    CHECK_RPC_STATUS(LoadSymbol(g_rpcUcxDl, "OckRpcServerAddService", (void **)&g_rpcUcxFunc.serverAddService));

    CHECK_RPC_STATUS(LoadSymbol(g_rpcUcxDl, "OckRpcServerStart", (void **)&g_rpcUcxFunc.serverStart));

    CHECK_RPC_STATUS(LoadSymbol(g_rpcUcxDl, "OckRpcServerDestroy", (void **)&g_rpcUcxFunc.serverDestroy));

    CHECK_RPC_STATUS(LoadSymbol(g_rpcUcxDl, "OckRpcServerReply", (void **)&g_rpcUcxFunc.serverReply));

    CHECK_RPC_STATUS(LoadSymbol(g_rpcUcxDl, "OckRpcServerCleanupCtx", (void **)&g_rpcUcxFunc.serverCleanCtx));

    return RPC_OK;
}

RpcStatus InitRpcServerConfig()
{
    return RPC_OK;
}

RpcStatus InitRpcServer(KnlRpcContext& ctx, DependencePath paths)
{
    // load dl
    CHECK_NDP_RPC_STATUS(InitRpcEnv(paths));

    // load server functions
    if (RpcServerDlsym() != RPC_OK) {
        LOG_ERROR << "dlsym rpc server func, path";
        CloseDl(g_rpcUcxDl);
        g_rpcUcxDl = nullptr;
        return RPC_ERROR;
    }

    if (ctx.serverHandle != 0) {
        g_rpcUcxFunc.serverDestroy(ctx.serverHandle);
    }

    CHECK_NDP_RPC_STATUS(InitRpcServerConfig());

#ifdef ENABLE_SSL
    OckRpcCreateConfig cfg;
    cfg.mask = OCK_RPC_CONFIG_USE_SSL_CALLBACK;
    cfg.getCaAndVerify = nullptr;
    cfg.getCert = GetCert;
    cfg.getPriKey = GetPrivateKey;
    RpcStatus status = g_rpcUcxFunc.serverCreateWithCfg(ctx.ip, ctx.port, &ctx.serverHandle, &cfg);
#else
    RpcStatus status = g_rpcUcxFunc.serverCreate(ctx.ip, ctx.port, &ctx.serverHandle);
#endif
    if (status != RPC_OK) {
        LOG_ERROR << "OckRpcServerCreate failed, ip " << ctx.ip << "port" << ctx.port;
        CloseDl(g_rpcUcxDl);
        g_rpcUcxDl = nullptr;
        return RPC_ERROR;
    }
    return RPC_OK;
}

static void RpcAdminProc(RpcServerContext handle, RpcMessage msg)
{
    NdpAdminRequest *header = (NdpAdminRequest *)msg.data;
    NdpAdminResponse resp;
    size_t size = offsetof(NdpAdminResponse, queryId);  // just send ret default
    resp.ret = NDP_ILLEGAL;

    NDP_PG_TRY();
    {
        if (!NdpAdminProc(header, resp, size)) {
            LOG_DEBUG << "rpc admin message is received successfully, "
                      << "admin command is " << (int)(header->head.command);
            resp.ret = NDP_OK;
        }
    }
    NDP_PG_CATCH();
    {
        LOG_INFO << "rpc admin message is received failed, "
                 << "admin command is " << (int)(header->head.command);
        resp.ret = NDP_ERR;
    }
    NDP_PG_END_TRY();

    RpcMessage reply = {.data = (void*)&resp, .len = size};

    if (g_rpcUcxFunc.serverReply(handle, RPC_ADMIN_REQ, &reply, nullptr) != RPC_OK) {
        LOG_ERROR << "send reply failed";
    }

    g_rpcUcxFunc.serverCleanCtx(handle);
}

RpcStatus SendIOTaskErrReply(NdpIOTask* task, NDP_ERRNO error)
{
    NdpIOResponse res;
    res.status = error;
    RpcMessage reply = {.data = nullptr, .len = 0};
    reply.data = &res;
    g_rpcUcxFunc.serverReply(task->handle, RPC_IO_REQ, &reply, nullptr);
    g_rpcUcxFunc.serverCleanCtx(task->handle);
    delete task;
}
#ifdef FAULT_INJECT
static void IOInject(NdpIOTask* &task)
{
    auto iter = injectPlanVarMap.find(task->header->taskId);
    if (iter != injectPlanVarMap.end()) {
        iter->second->ioCount.fetch_add(1, std::memory_order_relaxed);
    }
    // timeout inject
    if ((rand() % PERCENTAGE_DIV) < PERCENTAGE) {
        sleep((rand() % PERCENTAGE_DIV));
    }
    SendIOTaskErrReply(task, ERR_AIO_FAILED);
}
#endif

static void RpcIOProc(RpcServerContext handle, RpcMessage msg)
{
    NdpIOTask* task = new NdpIOTask(handle, reinterpret_cast<NdpIORequest*>(msg.data));
#ifdef NDP_ASYNC_CEPH
    if (!SubmitAioReadData(task)) {
        LOG_DEBUG << "rpc IO message is received successfully.";
    } else {
        SendIOTaskErrReply(task, ERR_AIO_FAILED);
    }
#else
    globalWorkerManager->AddTask(task);
#endif
}

RpcStatus RpcIOTaskHandler(NdpIOTask* task)
{
#ifdef FAULT_INJECT
    if ((rand() % PERCENTAGE_DIV) < PERCENTAGE) {
        IOInject(task);
        return RPC_ERROR;
    }
#endif
    RpcServerContext handle = task->handle;
    NdpIORequest *header = task->header;
#ifdef NDP_ASYNC_CEPH
    t_thrd.ndpWorkerCtx->scanPages = task->aioDesc->readBuf;
#endif

    NdpIOResponse res;
    res.status = NDP_ILLEGAL;
    Status ioStatus;

    RpcMessage reply = {.data = nullptr, .len = 0};

    NDP_PG_TRY();
    {
        ioStatus = NdpIOProc(header, &reply);
        if (reply.data) {
            LOG_DEBUG << "ndpworker " << pthread_self() << " successful handle "
                      << reinterpret_cast<NdpIOResponse *>(reply.data)->ndpPageNums << " ndppages.";
            reinterpret_cast<NdpIOResponse *>(reply.data)->status = NDP_OK;
        } else {
            LOG_DEBUG << "ndpworker " << pthread_self() << " handle 0 pages";
        }
    }
    NDP_PG_CATCH();
    {
        ioStatus = STATUS_ERROR;
    }
    NDP_PG_END_TRY();

    if (ioStatus != STATUS_OK) {
        reply.len = sizeof(NdpIOResponse);
        if (reply.data == nullptr) {
            res.status = NDP_ERR;
            reply.data = &res;
        } else {
            reinterpret_cast<NdpIOResponse *>(reply.data)->status = NDP_ERR;
        }
    }

    RpcStatus status = g_rpcUcxFunc.serverReply(handle, RPC_IO_REQ, &reply, nullptr);
    if (status != RPC_OK) {
        LOG_WARN << "send reply failed";
    }

    g_rpcUcxFunc.serverCleanCtx(handle);
    delete task;
    return status;
}

static RpcStatus RegisterRpcProcFunc(void)
{
    RpcServer server = ndp_instance.rpcContext.serverHandle;
    if (server == 0) {
        LOG_ERROR << "register rpc proc func failed, server handler:" << server;
        return RPC_ERROR;
    }

    RpcService adminService = {.id = RPC_ADMIN_REQ, .handler = RpcAdminProc};
    RpcStatus rpcStatus = g_rpcUcxFunc.serverAddService(server, &adminService);
    if (rpcStatus != RPC_OK) {
        LOG_ERROR << "add service RPC_ADMIN_REQ failed, status = " << rpcStatus;
        return RPC_ERROR;
    }

    RpcService ioService = {.id = RPC_IO_REQ, .handler = RpcIOProc};
    rpcStatus = g_rpcUcxFunc.serverAddService(server, &ioService);
    if (rpcStatus != RPC_OK) {
        LOG_ERROR << "add service RPC_IO_REQ failed, status = " << rpcStatus;
        return RPC_ERROR;
    }

    return RPC_OK;
}

RpcStatus RpcServerInit(void)
{
    RpcStatus rpcStatus;

    memset(&ndp_instance, 0, sizeof(ndp_instance));
    DependencePath paths;

    paths.ulogPath = LIB_ULOG;
    paths.rpcPath = LIB_RPC_UCX;
    paths.sslDLPath = LIB_OPENSSL_DL;
    paths.sslPath = LIB_SSL;
    paths.cryptoPath = LIB_CRYPTO;

    strcpy(ndp_instance.rpcContext.ip, configSets->ip.c_str());
    ndp_instance.rpcContext.port = configSets->port;

    CHECK_NDP_RPC_STATUS(InitRpcServer(ndp_instance.rpcContext, paths));

    CHECK_NDP_RPC_STATUS(RegisterRpcProcFunc());

    rpcStatus = g_rpcUcxFunc.serverStart(ndp_instance.rpcContext.serverHandle);
    if (rpcStatus != RPC_OK) {
        LOG_ERROR << "RpcServerStart failed";
        return rpcStatus;
    }

    return rpcStatus;
}
#else

static RpcStatus RpcClientDlsym(void)
{
    CHECK_RPC_STATUS(LoadSymbol(g_rpcUcxDl, "OckRpcClientConnect", (void **)&g_rpcUcxFunc.clientConnect));

#ifdef ENABLE_SSL
    CHECK_RPC_STATUS(LoadSymbol(g_rpcUcxDl, "OckRpcClientConnectWithCfg", (void **)&g_rpcUcxFunc.clientConnectWithCfg));
#endif

    CHECK_RPC_STATUS(LoadSymbol(g_rpcUcxDl, "OckRpcClientDisconnect", (void **)&g_rpcUcxFunc.clientDisconnect));

    CHECK_RPC_STATUS(LoadSymbol(g_rpcUcxDl, "OckRpcClientCall", (void **)&g_rpcUcxFunc.clientCall));

    CHECK_RPC_STATUS(LoadSymbol(g_rpcUcxDl, "OckRpcClientSetTimeout", (void **)&g_rpcUcxFunc.clientSetTimeout));

    return RPC_OK;
}

RpcStatus RpcClientInit(DependencePath& paths)
{
    // load dl
    CHECK_NDP_RPC_STATUS(InitRpcEnv(paths));

    if (RpcClientDlsym() != RPC_OK) {
        CloseDl(g_rpcUcxDl);
        g_rpcUcxDl = nullptr;
        return RPC_ERROR;
    }

    return RPC_OK;
}

RpcStatus RpcClientConnect(char *ip, uint16_t port, RpcClient& clientHandle)
{
#ifdef ENABLE_SSL
    OckRpcCreateConfig cfg;
    cfg.mask = OCK_RPC_CONFIG_USE_SSL_CALLBACK;
    cfg.getCaAndVerify = GetCAAndVerify;
    cfg.getCert = nullptr;
    cfg.getPriKey = nullptr;
    RpcStatus rpcStatus = g_rpcUcxFunc.clientConnectWithCfg(ip, port, &clientHandle, &cfg);
#else
    RpcStatus rpcStatus = g_rpcUcxFunc.clientConnect(ip, port, &clientHandle);
#endif
    if (rpcStatus != RPC_OK) {
        ereport(LOG, (errmsg("RpcClientConnect failed, ip: %s, port: %d", ip, port)));
        return rpcStatus;
    }
    g_rpcUcxFunc.clientSetTimeout(clientHandle, REPLY_TIMEOUT);

    return RPC_OK;
}

void RpcClientDisconnect(RpcClient clientHandle)
{
    g_rpcUcxFunc.clientDisconnect(clientHandle);
    ereport(LOG, (errmsg("RpcClientDisconnect complete.")));
}

// size is for expand NdpAdminResponse
RpcStatus RpcSendAdminReq(NdpAdminRequest* req, NdpAdminResponse* resp, size_t size, RpcClient clientHandle)
{
    RpcMessage request = {.data = (void*)req, .len = req->head.size};
    RpcMessage response = {.data = (void*)resp, .len = size};

    resp->ret = NDP_ILLEGAL;
    RpcStatus rpcStatus = g_rpcUcxFunc.clientCall(clientHandle, RPC_ADMIN_REQ, &request, &response, nullptr);

    return rpcStatus;
}

RpcStatus RpcSendIOReq(RpcMessage* request, RpcMessage* response, RpcCallDone* done, RpcClient clientHandle)
{
    RpcStatus rpcStatus = g_rpcUcxFunc.clientCall(clientHandle, RPC_IO_REQ, request, response, done);
    if (rpcStatus != RPC_OK) {
        ereport(WARNING, (errmsg("RpcSendIOReq failed. Error code: %d", rpcStatus)));
    }
    return rpcStatus;
}

#endif
