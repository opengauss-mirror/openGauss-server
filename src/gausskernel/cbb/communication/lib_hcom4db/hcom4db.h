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
* hcom4db.h
*
*
* IDENTIFICATION
*    src/gausskernel/cbb/communication/lib_hcom4db/hcom4db.h
*
* -------------------------------------------------------------------------
*/

#ifndef HCOM_HCOM4DB_H
#define HCOM_HCOM4DB_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
* To compatible with interfaces of version RDMA ucx,
* Deprecated in current version
*/
typedef enum {
    OCK_RPC_OK = 0,
    OCK_RPC_ERR = -1,
    OCK_RPC_ERR_INVALID_PARAM = -2,
    OCK_RPC_ERR_MISMATCH_MSG_ID = -3,
    OCK_RPC_ERR_CONN_UNCONNECTED = -4,
    OCK_RPC_ERR_SERIALIZE = -5,
    OCK_RPC_ERR_DESERIALIZE = -6,
    OCK_RPC_ERR_NO_MEMORY = -7,
    OCK_RPC_ERR_RDMA_NOT_ENABLE = -8,
    OCK_RPC_ERR_UNPACK_RKEY = -9,
    OCK_RPC_ERR_UCP_TAG_SEND = -10,
    OCK_RPC_ERR_UCP_TAG_RECV = -11,
    OCK_RPC_ERR_UCP_PUT = -12,
    OCK_RPC_ERR_UCP_GET = -13,
    OCK_RPC_ERR_UNSUPPORT = -14,
    OCK_RPC_ERR_SERVICE_EXIST = -15,
    OCK_RPC_ERR_ENCRYPT = -16,
    OCK_RPC_ERR_DECRYPT = -17,
    OCK_RPC_ERR_SINATURE = -18,
    OCK_RPC_ERR_UCP_TIMEOUT = -19,
    OCK_RPC_ERR_UCP_CLOSE = -20,
    OCK_RPC_ERR_CONN_BROKEN = -21,
    OCK_RPC_ERR_INVALID_RKEY = -22,
} OckRpcStatus;

/**
* @brief Keys config strings for configuration of create.
* Value : A string of up to 64 characters
* Default name is "service".
*/
#define OCK_RPC_STR_SERVER_NAME "server.create.name"

/**
* @brief Keys config strings for configuration of port range to bind.
*        You should set this when port is 0 during creating.
* Value : A string of up 16
*
* Default is "4000-5000", use "-" to separate the numbers representing the port range,
* means select idle port from 4000 to 5000.
*/
#define OCK_RPC_STR_SERVER_PORT_RANGE "server.create.range"

/**
* @brief Communication type to use.
* Value : "TCP" for use TCP mode.(only use OckRpcClientCall to send message)
*         "RDMA" for use RDMA mode
*
* Default is "RDMA" if you do not configurate this option.
*/
#define OCK_RPC_STR_COMMUNICATION_TYPE "server.create.type"

/**
* @brief Prepared send and receive segment size.
* Value : max message size send mode
*
* Default is "16384" if you do not configurate this option.
*/
#define OCK_RPC_STR_PRE_SEGMENT_SIZE "server.create.segsize"

/**
* @brief Prepared send and receive segment count.
* Value : max send cache segment count
*
* Default is "16384" if you do not configurate this option.
*/
#define OCK_RPC_STR_PRE_SEGMENT_COUNT "server.create.segcount"

/**
* @brief Keys config strings for configuration of client handle.
* Value : Pointer address in "char *" format.
*         you should convert the function pointer type to "uintptr" and pass in its address.
*         Type OckRpcClientHandler will be used to define these functions.
*
*  Default function works return directly.
*/
#define OCK_RPC_STR_CLIENT_NEW "server.client.new"
#define OCK_RPC_STR_CLIENT_BREAK "server.client.break"

/**
* @brief Separate symbols for multiple groups
*/
#define OCK_RPC_STR_SEP_THR_GRP ","

/**
* The symbol of the number representing the range of the link
*/
#define OCK_RPC_STR_SEP_CPU_SET "-"

/**
* @brief Keys config strings for configuration of polling worker grouping.
* Value : worker groups string of up to 64, for example "1,2,3".
*
* This config must be set.
*/
#define OCK_RPC_STR_WRK_THR_GRP "worker.thread.groups"

/**
* @brief Keys config strings for configuration of polling worker groups cpu set.
* Value : worker cpu set string of up to 128, for example "1-1,2-3,na".
*
* This config must be set. "na" needs to be set if CPU binding is not required.
*/
#define OCK_RPC_STR_WRK_CPU_SET "worker.thread.cpuset"

/**
* @brief Specifies whether to use BUSY_POLLING mode in worker.
* Value : "yes" for use poll mode
*         "no" for use event mode
*
* Default is "no" if you do not configurate this option.
*/
#define OCK_RPC_STR_ENABLE_POLL_WORKER "worker.poll.enable"

/**
* @brief Specifies whether to use rndv to send message.
*        Only set on the receiving side
* Value : "yes" for use rndv
*         "no" for ignoring rndv
*
* Default is "no" if you do not configurate this option.
*/
#define OCK_RPC_STR_ENABLE_SEND_RNDV "server.rndv.enable"

/**
* @brief Keys config strings for configuration of rndv function.
*        Only set when OCK_RPC_STR_ENABLE_SEND_RNDV value is "yes".
* Value : Pointer address in "char *" format, record the address of a function.
*         The function needs to be implemented based on the type of function pointer, which is
*         type OckRpcRndvMemAllocate for allocate and type OckRpcRndvMemFree for free.
*/
#define OCK_RPC_STR_RNDV_MALLOC "server.rndv.malloc"
#define OCK_RPC_STR_RNDV_MFREE "server.rndv.mfree"

/**
* Heartbeat thread idle time, min value is 1 second.
* Value : Heartbeat thread idle time.
*
* Default is "1" if you do not configurate this option.
*/
#define OCK_RPC_STR_HEATBEAT_IDLE "server.heartbeat.idle"

/**
* @brief Specifies whether to use tls.
* Value : "yes" for using tls
*         "no" for ignoring tls
*
* Default is "yes" if you do not configurate this option.
* For "yes", @ref OckRpcCreateConfig @b mask @ref OCK_RPC_CONFIG_USE_SSL_CALLBACK
* and associated certificate loading functions must be registered.
*/
#define OCK_RPC_STR_ENABLE_TLS "server.tls.enable"

#define OCK_RPC_BIT(i) (1UL << (i))

typedef uintptr_t OckRpcServer;
typedef uintptr_t OckRpcClient;
typedef uintptr_t OckRpcServerContext;
typedef uintptr_t OckRpcClientContext;
typedef uintptr_t OckRpcServerRndvContext;

/**
* @brief Callback function definition, the handler will fire when the connection state changes
* 1) new endpoint connected from destinction
* 2) endpoint is broken, called when RDMA qp detction error or broken
*/
typedef int (*OckRpcClientHandler)(OckRpcClient client, uint64_t usrCtx, const char *payLoad);

/** @brief TLS callbacks */
/**
* @brief KeyPass erase function
* @param keypass the memory address of keypass
*/
typedef void (*OckRpcTlsKeypassErase)(char *keypass);

/**
* @brief Get private key file's path and length, and get the keypass
* @param priKeyPath the path of private key
* @param keypass the keypass
* @param erase the erase function
*/
typedef void (*OckRpcTlsGetPrivateKey)(const char **priKeyPath, char **keypass, OckRpcTlsKeypassErase *erase);

/**
* @brief Get the certificate file of public key
* @param certPath the path of certificate
*/
typedef void (*OckRpcTlsGetCert)(const char **certPath);

/**
* @brief The cert verify function
* @param x509 the x509 object of CA
* @param crlPath the crl file path
*
* @return -1 for failed, and 1 for success
*/
typedef int (*OckRpcTlsCertVerify)(void *x509, const char *crlPath);

/**
* @brief Get the CA and verify
* @param caPath the path of CA file
* @param crlPath the crl file path
* @param verify the verify function
*/
typedef void (*OckRpcTlsGetCAAndVerify)(const char **caPath, const char **crlPath, OckRpcTlsCertVerify *verify);

/**
* @brief Message struct.
*/
typedef struct {
    void *data;
    size_t len;
} OckRpcMessage;

typedef struct {
    uintptr_t lAddress;
    uint32_t lKey;
    uint32_t size;
} OckRpcMessageInfo;

typedef struct {
    size_t count;
    OckRpcMessage *msgs;
} OckRpcMessageIov;

typedef struct {
    size_t count;
    OckRpcMessageInfo *reqs;
} OckRpcMessageInfoIov;

typedef struct {
    uint16_t cnt;
    OckRpcMessage msg[4];
} OckRpcRndvMessage;

/**
* @brief Message handler
*
* User can pass @b ctx and @b msg to other thread, it will remain valid until
* OckRpcServerCleanupCtx is called. After calling the OckRpcServerReply,
* user need to call OckRpcServerCleanupCtx to release @b ctx. The lifetime
* of the memory that @b msg points to is same as @b ctx, So after invoking
* OckRpcServerCleanupCtx, the @b msg is freed and can not be used anymore.
*/
typedef void (*OckRpcMsgHandler)(OckRpcServerContext ctx, OckRpcMessage msg);

/**
* @brief Rndv message handler
*
* User can pass @b ctx and @b msg to other thread, it will remain valid until
* OckRpcServerCleanupRndvCtx is called. After calling the OckRpcServerRndvReply,
* user need to call OckRpcServerCleanupRndvCtx to release @b ctx. The lifetime
* of the memory that @b msg points to is same as @b ctx, So after invoking
* OckRpcServerCleanupRndvCtx, the @b msg is freed and can not be used anymore.
*/
typedef void (*OckRpcMsgRndvHandler)(OckRpcServerRndvContext ctx, OckRpcRndvMessage msg);

/**
* @brief RPC call completion callback.
*
* @b status is the result of the communication call and @b arg is specified by user.
*/
typedef void (*OckRpcDoneCallback)(OckRpcStatus status, void *arg);

/**
* To compatible with interfaces of version RDMA ucx.
* Deprecated in current version.
*/
typedef OckRpcServerContext (*OckRpcServerCtxBuilderHandler)(OckRpcServer server);
typedef void (*OckRpcServerCtxCleanupHandler)(OckRpcServer server, OckRpcServerContext ctx);

/**
* To compatible with interfaces of version RDMA ucx.
* Deprecated in current version.
*/
OckRpcServerContext OckRpcServerCtxBuilderThreadLocal(OckRpcServer server);
void OckRpcServerCtxCleanupThreadLocal(OckRpcServer server, OckRpcServerContext ctx);

/**
* @brief rndv protocol memory allocate callback
*
* @b size is specified by user, @b address and @b key are the result of allocate
*/
typedef int (*OckRpcRndvMemAllocate)(uint64_t size, uintptr_t *address, uint32_t *key);

/**
* @brief rndv protocol memory free callback
*
* @b address is specified by user
*/
typedef int (*OckRpcRndvMemFree)(uintptr_t address);

/**
* @brief RPC Service.
*
* Each service is a kind of message processing object.
*/
typedef struct {
    uint16_t id;                /** Message ID handled by this service. The range is [0, 999]. */
    OckRpcMsgHandler handler;   /** Message handler. */
} OckRpcService;

/**
* @brief RPC Rndv Service
*
* Each service is a kind of message processing RNDV object.
*/
typedef struct {
    uint16_t id;                /** Message ID handled by this service. The range is [0, 999]. */
    OckRpcMsgRndvHandler handler;   /** Message handler. */
} OckRpcRndvService;

/**
* @brief RPC call compeletion handle.
*
* This structure should be allocated by the user and can be passed to communication
* primitives, such as @ref OckRpcClientCall. When the structure object is passed in,
* the communication routine changes to asynchronous mode. And if the routine returns
* success, the actual completion result will be notified through this callback.
*
* If you want to handle the returned data asynchronously in your own way, transfer your
* stucture to @b arg transport int the callback.
*/
typedef struct {
    OckRpcDoneCallback cb;  /* User callback function. */
    void *arg;              /* Argment of callback. */
} OckRpcCallDone;

typedef struct {
    const char *key;
    const char *value;
} RpcConfigPair;

typedef struct {
    int size;
    RpcConfigPair *pairs;
} RpcConfigs;

typedef enum {
    OCK_RPC_CONFIG_USE_RPC_CONFIGS = OCK_RPC_BIT(0),
    OCK_RPC_CONFIG_USE_SERVER_CTX_BUILD = OCK_RPC_BIT(1),
    OCK_RPC_CONFIG_USE_SSL_CALLBACK = OCK_RPC_BIT(2),
} OckRpcCreateConfigMask;

typedef struct {
    /** Must enable special bit before you set config value @ref OckRpcCreateConfigMask*/
    uint64_t mask;

    /** Set Key-Value mode to config, must enable @ref OCK_RPC_CONFIG_USE_RPC_CONFIGS */
    RpcConfigs configs;

    /**
     * Set user define Server Ctx build and cleanup handler, must enable @ref OCK_RPC_CONFIG_USE_SERVER_CTX_BUILD,
     * To compatible with interfaces of version RDMA ucx.
     * Deprecated in current version.
     */
    OckRpcServerCtxBuilderHandler serverCtxbuilder;
    OckRpcServerCtxCleanupHandler serverCtxCleanup;

    /**
     * Set SSL handler, must enable @ref OCK_RPC_CONFIG_USE_SSL_CALLBACAK
     *
     * @b getCaAndVerify, @b getCert and @b getPriKey can't be nullptr
     */
    OckRpcTlsGetCAAndVerify getCaAndVerify; /* get the CA path and verify callback */
    OckRpcTlsGetCert getCert;               /* get the certificate file of public key */
    OckRpcTlsGetPrivateKey getPriKey;       /* get the private key and keypass */
} OckRpcCreateConfig;

typedef enum {
    OCK_RPC_CLIENT_CALL_DEFAULT = 0, /* default mask */
} OckcRpcClientCallParamsMask;

typedef struct {
    /** Must enable special bit before you set config value @ref OckcRpcClientCallParamsMask*/
    uint64_t mask;

    OckRpcClient client;

    /**
     * It is recommended that the client context be resused for the high performance, so users can call
     * @ref OckRpcClientCreateCtx() for alloc context.
     *
     * @note But for converience that user can set it to NULL.
     */
    OckRpcClientContext context;

    /** message processing object ID as deifined @b msgId in OckRpcService */
    uint16_t msgId;

    /**
     * messages as @ref OckRpcMessageIov define
     *
     * @note At least has one valid message
     */
    OckRpcMessageIov reqIov;

    /**
     * There are 3 methods to use @b rspIov
     * 1. Don't receive response, so set the rspIov.count to 0;
     * 2. Receive the response but don't know th length of response, so set the rspIov.count to 1 and
     *    set the first @b msg in rspIov.msgs to {.data = NULL, .len = 0}, finally RPC will put responese
     *    to rspIov.msgs[0], and user need to free this memory in rspIov.msgs[0].data.
     * 3. Know the length of response
     */
    OckRpcMessageIov rspIov;

    /**
     * There are 2 methods to use @b done
     * 1. Handle the returned data synchronously, so set @b done to NULL;
     * 2. Handle the returned data synchronously, so set @b done as @ref OckRpcCallDone describe.
     */
    OckRpcCallDone *done;
} OckRpcClientCallParams;

/**
* @brief External log callback function.
*
* @param level     [in] level, 0/1/2/3 represent debug/info/warn/error
* @param msg       [in] message, log message with name : code-line-number
*/
typedef void (*OckRpcLogHandler)(int level, const char *msg);

/**
* @brief Set external logger function.
*
* @param h         [in] the log function ptr
*/
void OckRpcSetExternalLogger(OckRpcLogHandler h);

/**
* @brief Create a rpc server with rpc create config.
*
* @param ip        [in] server IP
* @param port      [in] server port
* @param server    [out] Created serevr
* @param config    [in] server create configurations, please configure the corresponding mask
*
* @note Only create a server object, does not start listening.
*/
OckRpcStatus OckRpcServerCreateWithCfg(const char *ip, uint16_t port, OckRpcServer *server,
                                       OckRpcCreateConfig *configs);

/**
* @brief Get the out of bound ip and port
*
* @param server    [in] the address if server
* @param ipArray   [out] oob ip list
* @param portArray [out] oob port list
* @param length    [out] the length of ipArray and portArray
*
* @return true for success and false fort failure.
*/
bool OckRpcGetOobIpAndPort(OckRpcServer server, char ***ipArray, uint16_t **portArray, int *length);

/**
* @brief Add service.
*
* Register the message handling method to the server. When a message with this
* ID is received, the registered message handler is called.
*
* @param server    [in] server handle
* @param service   [in] message processing object
*
* @return OCK_RPC_OK for sucess and others for failure.
*
* @note use the same id as servce has benne add will replace it
*/
OckRpcStatus OckRpcServerAddService(OckRpcServer server, OckRpcService *service);

/**
* @brief Add Rdnv service, must be enable OCK_RPC_CONFIG_USE_RNDV_SEND.
*
* Register the message handling method to the server. When a message send by RNDV interface with this
* ID is received, the registered message handler is called.
*
* @param server    [in] server handle
* @param service   [in] message processing object
*
* @return OCK_RPC_OK for sucess and others for failure.
*
* @note use the same id as servce has benne add will replace it
*/
OckRpcStatus OckRpcServerAddRndvService(OckRpcServer server, OckRpcRndvService *service);

/**
* @brief Start the server.
*
* This is not blocking routine, it will return after listening the binded address.
*
* @param server    [in] server handle returned by @ref OckRpcServerCreate.
*
* @return OCK_RPC_OK for sucess and others for failure.
*/
OckRpcStatus OckRpcServerStart(OckRpcServer server);

/**
* @brief Stop and destroy the server.
*
* After this routine, the server handle can not be used again.
*
* @param server    [in] server handle returned by @ref OckRpcServerCreate.
*/
void OckRpcServerDestroy(OckRpcServer server);

/**
* @brief Send a reply.
*
* The @b ctx is passed to the user along with the request message through OckRpcMsgHandler.
*
* If @b done is not nullptr, this routine behaves as an asynchronous routine. When
* this routine return OCK_RPC_OK, it only means that the operation is triggered,
* and the real result will be reported to the user through @b done. The @b done
* may be executed in the routine, please pay attention to this. Before the @b done
* excution, the memory in the @b reply can not be released.
*
* If @b done is nullptr, the return value means the real result.
*
* @param ctx       [in] context passed to the user through @ref OckRpcMsgHandler.
* @param msgId     [in] message ID.
* @param reply     [in] reply message.
* @param done      [in] notify the result to user.
* @return OCK_RPC_OK for sucess and others for failure. If @b done is not nullptr,
*         OCK_RPC_OK means it's still inprogress.
*
* @note The message id of this reply must be the same as the request message ID.
*/
OckRpcStatus OckRpcServerReply(OckRpcServerContext ctx, uint16_t msgId, OckRpcMessage *reply, OckRpcCallDone *done);

/**
* @brief Same with OckRpcServerReply, but only use on RDNV interface
*/
OckRpcStatus OckRpcServerReplyRndv(OckRpcServerRndvContext ctx, uint16_t msgId, OckRpcMessage *reply,
                                   OckRpcCallDone *done);

/**
* @brief Get the client from the context
*
* @param ctx       [in] context passed to the user through OckRpcMsgHandler.
* @param client    [out] client handle returned.
*
* @return 0 means OK, other value means failure.
*/
int OckRpcGetClient(OckRpcServerContext ctx, OckRpcClient *client);

/**
* @brief Get the client from the rndv context
*
* @param ctx       [in] context passed to the user through OckRpcMsgRndvHandler.
* @param client    [out] client handle returned.
*
* @return 0 means OK, other value means failure.
*/
int OckRpcGetRndvClient(OckRpcServerRndvContext ctx, OckRpcClient *client);

/**
* @brief Clone service context
*
* @param ctx       [in] context passed in by OckRpcMsgHandlerS
*
* @return valid address of success, 0 if failed
*/
OckRpcServerContext OckRpcCloneCtx(OckRpcServerContext ctx);

/**
* @brief destroy context by cloned
*
* @param ctx       [in] context conled by OckRpcCloneCtx
*/
void OckRpcDeCloneCtx(OckRpcServerContext ctx);

/**
* @brief To compatible with interfaces of version RDMA ucx.
* Deprecated in current version.
*/
void OckRpcServerCleanupCtx(OckRpcServerContext ctx);

/**
* @brief Clean RNDV context up after reply
*
* @param ctx       [in] rndv conetext
*
* @return 0 means OK, other value means failure.
*
* @note function must be called in OckRpcMsgRndvHandler to free OckRpcServerRndvContext memory
*/
int OckRpcServerCleanupRndvCtx(OckRpcServerRndvContext ctx);

/**
* @brief Keys configs string for configuration of client create
* Value : A string of up to 64 characters
*
* Default info is "hello". You can check it in your OckRpcClientHandler.
*/
#define OCK_RPC_STR_CLIENT_CREATE_INFO "client.create.info"

/**
* @brief Specifies whether to use selfpolling mode in worker.
* Value : "yes" for use self polling
*         "no" for use worker polling
*
* Default is "yes" if user don't configurate this option.
*
* @note In selfpolling mode, user can't use async send.
*/
#define OCK_RPC_STR_ENABLE_SELFPOLLING "client.enable.selfpolling"

/**
* @brief Connect to server with config.
*
* Connect to a specified address and return a handler that can be used to rpc call.
* If the value of OCK_RPC_STR_SERVER_NAME of the created server is not the default "service",
* the value of OCK_RPC_STR_SERVER_NAME must be the same as that of server.
*
* @param ip        [in] server IP.
* @param port      [in] server port.
* @param client    [out] client handle that connected to the server.
* @param cfg       [in] the create configs.
*
* @return OCK_RPC_OK for success and others for failure.
*/
OckRpcStatus OckRpcClientConnectWithCfg(const char *ip, uint16_t port, OckRpcClient *client,
                                        OckRpcCreateConfig *configs);

/**
* @brief Disconnect.
*
* If OCK_RPC_STR_CLIENT_BREAK has not been set, after this routine,
* the client handle can not be used again.
*
* @param client    [in] client handle returned by OckRpcClientConnectWithCfg
*/
void OckRpcClientDisconnect(OckRpcClient client);

/**
* @brief Disconnect in function of type OckRpcClientHandler
*
* If OCK_RPC_STR_CLIENT_BREAK has been set, you must call this function
* in function of type OckRpcClientHandler after OckRpcClientDisconnect
*
* @param client    [in] client pass through the function of type OckRpcClientHandler
*/
void OckRpcClientDisconnectBreakHandler(OckRpcClient client);

/**
* @brief Set client up context
*
* @param client    [in] client handle returned by OckRpcClientConnectWithCfg
* @param ctx       [in] user input infomation
*
* @return 0 means success, other value means failure
*/
int OckRpcSetUpCtx(OckRpcClient client, uint64_t ctx);

/**
* @brief Get client up context
*
* @param client    [in] client handle returned by OckRpcClientConnectWithCfg
* @param ctx       [out] user input infomation by OckRpcSetUpCtx
*
* @return 0 means success, other value means failure
*/
int OckRpcGetUpCtx(OckRpcClient client, uint64_t *ctx);

/**
* @brief Send request and receive response(optional).
*
* If @b response is NULL, it just sends the request and returns.
*
* If @b response is not NULL, it will receives a response. If user does not
* know the real length if the reponse, user can set @b response->data to NULL,
* so RPC will allocate enough memory to hold the response, and store the memory
* address to @b response->data, user are required to use free() to free this
* memory. There will be additional overhead.
*
* If @b done is NULL, the returned value means real result.
*
* If @b done is not NULL, this routine behaves as an asynchronous routine,
* and all memory like @b response can not be released until the @b done is invoked.
* @b done will be called to notify the real result. The @b done may be executed
* in the routine, please pay attention to this.
*
* @param client    [in] client handle returned by OckRpcClientConnectWithCfg.
* @param msgId     [in] message processing object ID as deifined @b msgId in OckRpcService.
* @param request   [in] send request.
* @param response  [in/out] reponse.
* @param done      [in] notify the result to user.
*
* @return OCK_RPC_OK for sucess and others for failure. If @b done is not nullptr,
*         OCK_RPC_OK means it's still inprogress.
*/
OckRpcStatus OckRpcClientCall(OckRpcClient client, uint16_t msgId, OckRpcMessage *request,
                              OckRpcMessage *response, OckRpcCallDone *done);

/**
* @brief Send multi request and receive response(optional).
*
* Use according to the description of the OckRpcClientCallParams structure.
*
* @param params    [in] params for call
*
* @return OCK_RPC_OK for sucess and others for failure. If @b params->done is not nullptr,
*         OCK_RPC_OK means it's still inprogress.
*/
OckRpcStatus OckRpcClientCallWithParam(OckRpcClientCallParams *params);

/**
* @brief Send request by RNDV and receive response(optional).
*
* RNDV works well for big-size message.
*
* @b reponse can not be set as NULL.
*
* If @b done is NULL, the returned value means real result.
*
* If @b done is not NULL, this routine behaves as an asynchronous routine,
* and all memory like @b response can not be released until the @b done is invoked.
* @b done will be called to notify the real result. The @b done may be executed
* in the routine, please pay attention to this.
*
* @param client    [in] client handle returned by OckRpcClientConnectWithCfg.
* @param msgId     [in] message processing object ID as deifined @b msgId in OckRpcRndvService.
* @param request   [in] send request.
* @param response  [in/out] reponse.
* @param done      [in] notify the result to user.
*
* @return OCK_RPC_OK for sucess and others for failure. If @b done is not nullptr,
*         OCK_RPC_OK means it's still inprogress.
*/
OckRpcStatus OckRpcClientCallRndv(OckRpcClient client, uint16_t msgId, OckRpcMessageInfo *request,
                                  OckRpcMessage *response, OckRpcCallDone *done);

/**
* @brief Send multi request by RNDV and receive response(optional).
*
* RNDV works well for big-size message.
*
* @b reponse can not be set as NULL.
*
* If @b done is NULL, the returned value means real result.
*
* If @b done is not NULL, this routine behaves as an asynchronous routine,
* and all memory like @b response can not be released until the @b done is invoked.
* @b done will be called to notify the real result. The @b done may be executed
* in the routine, please pay attention to this.
*
* @param client    [in] client handle returned by OckRpcClientConnectWithCfg.
* @param msgId     [in] message processing object ID as deifined @b msgId in OckRpcRndvService.
* @param request   [in] send requests.
* @param response  [in/out] reponse.
* @param done      [in] notify the result to user.
*
* @return OCK_RPC_OK for sucess and others for failure. If @b done is not nullptr,
*         OCK_RPC_OK means it's still inprogress.
*/
OckRpcStatus OckRpcClientCallRndvIov(OckRpcClient client, uint16_t msgId, OckRpcMessageInfoIov *request,
                                     OckRpcMessage *response, OckRpcCallDone *done);

/**
* @brief MemoryRegion, which region memory in RDMA Nic for write/read operation.
*/
typedef uintptr_t OckRpcMemoryRegion;

typedef struct {
    uintptr_t lAddress; /* local memory region address */
    uint32_t lKey;      /* local memory region key */
    uint32_t size;      /* date size */
} OckRpcMemoryRegionInfo;

/**
* @brief Register a memory region, the memory will be allocated internally
*
* @param server    [in] the address of server
* @param size      [in] size of memory region
* @param mr        [out] memory region regitstered to network card
*
* @return 0 for sucess and others for failure.
*/
int OckRpcRegisterMemoryRegion(OckRpcServer server, uint64_t size, OckRpcMemoryRegion *mr);

/**
* @brief Register a memory region, the memory need to passed in
*
* @param server    [in] the address of server
* @param address   [in] memory pointer that needs to be registered
* @param size      [in] size of memory region
* @param mr        [out] memory region regitstered to network card
*
* @return 0 for sucess and others for failure.
*/
int OckRpcRegisterAssignMemoryRegion(OckRpcServer server, uintptr_t address, uint64_t size, OckRpcMemoryRegion *mr);

/**
* @brief Parse the memory region, get info
*
* @param mr        [in] memory region registered
* @param info      [in] memory region info
*
* @return 0 for sucess and others for failure.
*/
int OckRpcGetMemoryRegionInfo(OckRpcMemoryRegion mr, OckRpcMemoryRegionInfo *info);

/**
* @brief Unregister the memory region
*
* @param server    [in] the address of server
* @param mr        [in] memory region registered
*/
void OckRpcDestroyMemoryRegion(OckRpcServer server, OckRpcMemoryRegion mr);

typedef struct {
    uintptr_t lAddress; /* local buffer address, user can free it after invoke callback */
    uintptr_t rAddress; /* remote buffer address, user can free it after invoke callback */
    uint32_t lKey;      /* local memory region key, for rdma etc. */
    uint32_t rKey;      /* remote memory region key, for rdma etc. */
    uint32_t size;      /* buffer size */
} __attribute__((packed)) OckRpcRequest;

typedef struct {
    OckRpcRequest *iov; /* sgl array */
    uint16_t iovCount;  /* sgl array count, max is NUM_16 */
} __attribute__((packed)) OckRpcSglRequest;

/**
* @brief Read a single side read request to peer, the CPU at peer is not aware.
*
* If @b cb is NULL, means read sync, the interface will block worker thread until read complete.
*
* If @b cb is not NULL, means read async, the interface will return after message input worker.
*
* @param client    [in] client handle returned by OckRpcClientConnectWithCfg.
* @param req       [in] request information, including 5 important variables, local/remote address/key and size.
* @param cb        [in] callback for async send to peer.
*
* @return 0 for sucess and others for failure.
*/
int OckRpcRead(OckRpcClient client, OckRpcRequest *req, OckRpcCallDone *cb);

/**
* @brief Read multi single side read request to peer, the CPU at peer is not aware.
*
* If @b cb is NULL, means read sync, the interface will block worker thread until read complete.
*
* If @b cb is not NULL, means read async, the interface will return after message input worker.
*
* @param client    [in] client handle returned by OckRpcClientConnectWithCfg.
* @param req       [in] request information, including 5 important variables, local/remote address/key and size.
* @param cb        [in] callback for async send to peer.
*
* @return 0 for sucess and others for failure.
*/
int OckRpcReadSgl(OckRpcClient client, OckRpcSglRequest *req, OckRpcCallDone *cb);

/**
* @brief Post a single side write request to peer, the CPU at peer is not aware.
*
* If @b cb is NULL, means write sync, the interface will block worker thread until write complete.
*
* If @b cb is not NULL, means write async, the interface will return after message input worker.
*
* @param client    [in] client handle returned by OckRpcClientConnectWithCfg.
* @param req       [in] request information, including 5 important variables, local/remote address/key and size.
* @param cb        [in] callback for async send to peer.
*
* @return 0 for sucess and others for failure.
*/
int OckRpcWrite(OckRpcClient client, OckRpcRequest *req, OckRpcCallDone *cb);

/**
* @brief Post multi single side write request to peer, the CPU at peer is not aware.
*
* If @b cb is NULL, means write sync, the interface will block worker thread until write complete.
*
* If @b cb is not NULL, means write async, the interface will return after message input worker.
*
* @param client    [in] client handle returned by OckRpcClientConnectWithCfg.
* @param req       [in] request information, including 5 important variables, local/remote address/key and size.
* @param cb        [in] callback for async send to peer.
*
* @return 0 for sucess and others for failure.
*/
int OckRpcWriteSgl(OckRpcClient client, OckRpcSglRequest *req, OckRpcCallDone *cb);

/**
* @brief Set timeout
*
* After set timeout by this routine, if OckRpcClientCallWithParam/Read/Write is not
* completed within the specified time, a failure is returned.
*
* 1. timeout = 0: return immediately
* 2. timeout < 0: never timeout, usually set to -1
* 3. timeout > 0: Millisecond precision timeout.
* Default timeout is -1.
*
* @param client    [in] client handle returned by OckRpcClientConnectWithCfg.
* @param timeout   [in] time user set.
*/
void OckRpcClientSetTimeout(OckRpcClient client, int64_t timeout);

/**
* @brief Alllocatir cache tier policy
*/
typedef enum {
    MEM_TIER_TIMES = 0, /* tier by times of min-block-size */
    MEM_TIER_POWER = 1, /* tier by power of min-block-size */
} OckRpcMemoryAllocatorCacheTierPolicy;

/**
* @brief Type of allocator
*/
typedef enum {
    MEM_DYNAMIC_SIZE = 0,            /* allocate dynamic memory size, there is alignment with X KB */
    MEM_DYNAMIC_SIZE_WITH_CACHE = 1, /* allocator with dynamic memory size, with pre-allocate cache for performance */
} OckRpcMemoryAllocatorType;

/**
* @brief Options for memory allocator
*/
typedef struct {
    uintptr_t address;               /* base address if large range of memory for allocator */
    uint64_t size;                   /* size of large memory chunk */
    uint32_t minBlockSize;           /* min size of block, more than 4 KB is required */
    uint32_t bucketCount;            /* default size of hash bucket */
    uint16_t alignedAddress;         /* force to align the memory block allocated, 0 means not align, 1 means align */
    uint16_t cacheTierCount;         /* for MEM_DYNAMIC_SIZE_WITH_CACHE only */
    uint16_t cacheBlockCountPerTier; /* for MEM_DYNAMIC_SIZE_WITH_CACHE only */
    OckRpcMemoryAllocatorCacheTierPolicy cacheTierPolicy; /* tier policy */
} OckRpcMemoryAllocatorOptions;

/**
* @brief memory allocator pointer
*/
typedef uintptr_t OckRpcMemoryAllocator;

/**
* @brief Create a memory allocator
*
* @param t             [in] type of allocator
* @param options       [in] options for creation
* @param allocator     [out] memory allocator created
*
* @return 0 for sucess and others for failure.
*/
int OckRpcMemoryAllocatorCreate(OckRpcMemoryAllocatorType t, OckRpcMemoryAllocatorOptions *options,
                                OckRpcMemoryAllocator *allocator);

/**
* @brief Destroy a memory allocator
*
* @param allocator     [in] memory allocator created by OckRpcMemoryAllocatorCreate
*
* @return 0 for sucess and others for failure.
*/
int OckRpcMemoryAllocatorDestroy(OckRpcMemoryAllocator allocator);

/**
* @brief Set the memory region key
*
* @param allocator     [in] memory allocator created by OckRpcMemoryAllocatorCreate
* @param mrKey         [in] key to access memory region
*
* @return 0 for sucess and others for failure.
*/
int OckRpcMemoryAllocatorSetMrKey(OckRpcMemoryAllocator allocator, uint32_t mrKey);

/**
* @brief Get the address's memory offset in allocator based in base address
*
* @param allocator     [in] memory allocator created by OckRpcMemoryAllocatorCreate
* @param address       [in] user's current address
* @param offset        [out] offset comparing to base address
*
* @return 0 for sucess and others for failure.
*/
int OckRpcMemoryAllocatorMemOffset(OckRpcMemoryAllocator allocator, uintptr_t address, uintptr_t *offset);

/**
* @brief Get free memory size in allocator memory
*
* @param allocator     [in] memory allocator created by OckRpcMemoryAllocatorCreate
* @param size          [out] free memory size
*
* @return 0 for sucess and others for failure.
*/
int OckRpcMemoryAllocatorFreeSize(OckRpcMemoryAllocator allocator, uintptr_t *size);

/**
* @brief Allocate memory area
*
* @param alloactor     [in] memory allocator created by OckRpcMemoryAllocatorCreate
* @param size          [in] memory size user needs
* @param address       [out] allocaoted memory address
* @param key           [out] memory region key set by OckRpcMemoryAllocatorSetMrKey
*
* @return 0 for sucess and others for failure.
*/
int OckRpcMemoryAllocatorAllocate(OckRpcMemoryAllocator allocator, uint64_t size, uintptr_t *address, uint32_t *key);

/**
* @brief Free memory area
*
* @param alloactor     [in] memory allocator created by OckRpcMemoryAllocatorCreate
* @param address       [in] memory address to free, allocated by OckRpcMemoryAllocatorAllocate
*
* @return 0 for sucess and others for failure.
*/
int OckRpcMemoryAllocatorFree(OckRpcMemoryAllocator allocator, uintptr_t address);

/**
* To compatible with interfaces of version RDMA ucx,
* Deprecated in current version
*/
void OckRpcDisableSecureHmac(void);

#ifdef __cplusplus
}
#endif

#endif