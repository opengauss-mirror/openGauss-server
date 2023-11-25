/* -------------------------------------------------------------------------
 *
 * uwal.h
 *        File synchronization management code.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *        src/include/storage/gs_uwal/uwal.h
 * -------------------------------------------------------------------------
 */

#ifndef __UWAL_H__
#define __UWAL_H__

#include <cstdlib>
#include <cstdint>

#define UWAL_PORT_LEN 8
#define UWAL_BYTE_SIZE_LEN 32
#define UWAL_INT_SIZE_LEN 8
#define UWAL_MAX_PATH_LEN 4096
#define UWAL_MAX_NUM 5
#define UWAL_OBJ_ALIGN_LEN (2097152UL)
#define MAX_GAUSS_NODE 8

#ifndef IN
#define IN
#endif

#ifndef OUT
#define OUT
#endif

/* ********
 * return code definition
 * ******* */
typedef enum {
    PRIMARY_APPEND_LOG_FAIL = -100002,
    REPLICA_APPEND_LOG_FAIL = -100001,
    U_BUSY = -100000,               // System busy. Please retry later.
    U_ARGS_INVAILD = -99999,        // Invalid input args.
    U_REQUEST_TIMEOUT = -99998,     // Request timeout.
    U_CAPACITY_NOT_ENOUGH = -99997, // Insufficient capacity.
    U_PERFTYPE_INVALID = -99996,    // Perftype invalid.
    U_UNSERVICEABLE = -99995,       // Service unserviceable.
    U_DISK_FAULT = -99994,          // Disk fault
    U_UWAL_ID_NOT_EXIST = -99993,   // The uwal does not exist.
    U_UWAL_IO_CORRUPTED = -99992,   // The uwal has corrupted permanently and only parts of the data can be obtained.
    U_UWAL_UNAVAILABLE = -99991,    // The uwal is unavailable or pool is fault. Recovery is not guranteed.
    U_UWAL_EXCEED_VALID_RANGE = -99990, // Exceeds the valid range, now only for read operation.
    U_UWAL_DISCONTINUOUS = -99989,  // Cannot append log because of existing append error.
    U_ERROR = -1, // Unknown Error
    U_OK = 0,     // OK
} UwalRetCode;

/* ********
 * uwal id:
 * uwal is a append-only data store unit, and each uwal has an id which is unique in the entire system.
 * ******* */
#pragma pack(push)
#pragma pack(1)

#define UWAL_ID_LEN (24) // uwal id len by bytes

typedef struct {
    char id[UWAL_ID_LEN];
} UwalId;

#pragma pack(pop)

/* ********
 * uwal durability, for example:
 * 3az 20+16       azCnt = 3, originNum = 20 redundancyNum = 16;
 * single az 12+3  azCnt = 1, originNum = 12 redundancyNum = 3;
 * single az 3rep  azCnt = 1, originNum = 1  redundancyNum = 2;
 * 3az rep 3       azCnt = 3, originNum = 1  redundancyNum = 2;
 * ******* */
typedef struct {
    // how many AZs for this uwal layout
    uint8_t azCnt;

    /* origin data num:
     * For X replicas, X replicas are regarded as 1+M, set origin_num as 1.
     * For EC N+M, set origin_num as N. */
    uint8_t originNum;

    /* redundancy data num:
     * For X replicas, X replicas are regarded as 1+M, set redundancy_num as M.
     * For EC N+M set redundancy_num as M. */
    uint8_t redundancyNum;

    uint8_t reliabilityType; /* EC_MODE fill 0, REPLICA_MODE fill 1 */
} UwalDurability;

/* ********
 * uwal Performance type:
 * currently support SCM and NvmeSSD, user only specifies uwal perf type
 * ******* */
typedef enum {
    UWAL_PERF_TYPE_SCM = 1,  // Media type scm
    UWAL_PERF_TYPE_SSD = 2,  // Media type ssd
    UWAL_PERF_TYPE_BUTT
} UwalPerfType;

/* ********
 * affinity policy:
 * the user use the policy to tell uwal to create the uwal with affinity.
 * ******* */
typedef struct {
    uint32_t partId; // partition id
    struct {
        uint32_t cnt;       // server total num
        uint32_t *serverId; // server id array point
    } detail;
} UwalAffinityPolicy;

/* ********
 * uwal user type
 * ******* */
typedef enum {
    UWAL_USER_OPENGAUSS = 0, // opengauss io stream
    UWAL_USER_PLATFORM = 1, // platform io stream
    UWAL_USER_TYPE_BUTT
} UwalUserType;

/* ********
 * uwal io type
 * ******* */
typedef enum {
    UWAL_IO_STRIPE = 1,
    UWAL_IO_RANDOM = 2,
    UWAL_IO_BUTT
} UwalIoType;

typedef enum {
    UWAL_STRIPE_32B = 0,
    UWAL_STRIPE_64B = 1,
    UWAL_STRIPE_128B = 2,
    UWAL_STRIPE_512B = 3,
    UWAL_STRIPE_4KB = 4,
    UWAL_STRIPE_8KB = 5,
    UWAL_STRIPE_32KB = 6,
    UWAL_STRIPE_64KB = 7,
    UWAL_STRIPE_128KB = 8,
    UWAL_STRIPE_256KB = 9,
    UWAL_STRIPE_512KB = 10,
    UWAL_STRIPE_1MB = 11,
    UWAL_STRIPE_2MB = 12,
    UWAL_STRIPE_BUTT
} UwalStripeType;

/* ********
 * uwal create flag
 * ******* */
typedef enum {
    UWAL_CREATE_DEFAULT = 0x0000,         /* default create */
    UWAL_CREATE_DEGRADE_LOSS1 = 0x0001,   /* degrade loss1 create */
    UWAL_CREATE_DEGRADE_LOSS2 = 0x0002,   /* degrade loss2 create */
    UWAL_CREATE_DEGRADE_LOSSANY = 0x0003,   /* degrade active1 create */
    UWAL_CREATE_BUTT
} UwalCreateFlag;

/* ********
 * uwal io route flag
 * ******* */
typedef enum {
    UWAL_ROUTE_LOCAL = 0,
    UWAL_ROUTE_MASTER = 1,
    UWAL_ROUTE_RANDOM = 2,
    UWAL_ROUTE_BUTT
} UwalRouteType;

/* ********
 * uwal descriptor:
 * the user must use the descriptor to tell Persistence to create the uwal with
 * specified ns/durability/perfType/affinity/io/stripe/metaSize/dataSize.
 * ******* */
typedef struct {
    UwalDurability durability;   // Data Redundancy Mode
    UwalPerfType perfType;       // Performace type media type
    UwalAffinityPolicy affinity; // affinity attribute
    UwalUserType user;           // user type
    UwalIoType io;               // io type
    UwalStripeType stripe;       // stripe type
    uint32_t flags;              // create flags
    uint64_t startTimeLine;      // start time line
    uint64_t startWriteOffset;   // start write offset
    uint64_t dataSize;           // log data size(unit bytes), align size is 2MB
} UwalDescriptor;

/* ********
 * call back
 * ******* */
typedef void (*uwalCbFun)(void *cbCtx, int retCode);

typedef struct {
    uwalCbFun cb;
    void *cbCtx;
} UwalCallBack;

/* ********
 * create uwal input param.
 * ******* */
typedef struct {
    const UwalDescriptor *desc;
    const uint32_t cnt;
    UwalCallBack *cb;
} UwalCreateParam;

/* ********
 * delete uwal input param.
 * ******* */
typedef struct {
    const UwalId *uwalId;
    UwalCallBack *cb;
} UwalDeleteParam;

/* ********
 * data to read param
 * ******* */
typedef struct {
    uint64_t offset; // offset
    uint64_t length; // len
} UwalDataToRead;

/* ********
 * read vector. Current only support cnt = 1.
 * ******* */
typedef struct {
    uint16_t cnt;               // cnt
    UwalDataToRead *dataToRead; // data want to read
} UwalDataToReadVec;

/* ********
 * read uwal input param.
 * ******* */
typedef struct {
    const UwalId *uwalId;
    const UwalRouteType route;
    const UwalDataToReadVec *dataV;
    UwalCallBack *cb;
} UwalReadParam;

/* ********
 * IO data buffer list
 * ******* */
typedef struct {
    char *buf;    // data buffer
    uint64_t len; // buffer length
} UwalBuffer;

typedef struct {
    uint16_t cnt;        // cnt
    UwalBuffer *buffers; // bufs
} UwalBufferList;

/* ********
 * append uwal input param.
 * ******* */
typedef struct {
    const UwalId *uwalId;
    const UwalBufferList *bufferList;
    UwalCallBack *cb;
} UwalAppendParam;

/* ********
 * uwal state:
 * ******* */
typedef enum {
    UWAL_STATUS_NORMAL = 1,   // normal
    UWAL_STATUS_DELETING = 2,
    UWAL_STATUS_BUTT
} UwalState;

/* ********
 * struct of return value for the API to query uwal.
 * ******* */
typedef struct {
    uint64_t startTimeLine;
    uint64_t startWriteOffset;
    uint64_t dataSize; // data size(unit bytes)
    uint64_t truncateOffset; // truncate offset(unit bytes)
    uint64_t writeOffset;    // written offset(unit bytes)
} UwalBaseInfo;

/* ********
 * uwal has an id and basic info.
 * ******* */
typedef struct {
    UwalId id;         // id
    UwalBaseInfo info; // basic info of the uwal
} UwalInfo;

/* ********
 * query uwal input param.
 * ******* */
typedef struct {
    const UwalId *uwalId;
    const UwalRouteType route;
    UwalCallBack *cb;
} UwalQueryParam;

/* ********
 * uwal vector:
 * currently, the uwal vector.
 * ******* */
typedef struct {
    uint32_t cnt;    // uwal cnt
    UwalInfo *uwals; // uwals
} UwalVector;

/* ********
 * uwal config elements.
 * ******* */
typedef struct {
    const char *substr;
    const char *value;
} UwalCfgElem;

typedef struct {
    uint32_t nodeId;
    UwalRetCode ret;
} UwalNodeStatus;

typedef struct {
    uint32_t num;
    UwalNodeStatus status[MAX_GAUSS_NODE];
} UwalNodeInfo;

typedef int (*UwalInit)(IN const char *path, IN const UwalCfgElem *elems, IN int cnt, IN const char *ulogPath);

typedef void (*UwalExit)(void);

typedef int (*UwalCreate)(IN UwalCreateParam *param, OUT UwalVector *uwals);

typedef int (*UwalDelete)(IN UwalDeleteParam *param);

typedef int (*UwalAppend)(IN UwalAppendParam *param, OUT uint64_t *offset, OUT void* result);

typedef int (*UwalRead)(IN UwalReadParam *param, OUT UwalBufferList *bufferList);

typedef int (*UwalTruncate)(IN const UwalId *uwalId, IN uint64_t offset);

typedef int (*UwalQuery)(IN UwalQueryParam *param, OUT UwalInfo *info);

typedef int (*UwalQueryByUser)(IN UwalUserType user, IN UwalRouteType route, OUT UwalVector *uwals);

typedef int (*UwalSetRewindPoint)(IN UwalId *uwalId, IN uint64_t offset);

typedef int (*UwalRegisterCertVerifyFunc)(int32_t (*certVerify)(void* certStoreCtx, const char *crlPath),
    int32_t (*getKeyPass)(char *keyPassBuff, uint32_t keyPassBuffLen, char *keyPassPath));

#define MAX_GROUP_NUM     (8)
#define MAX_NODE_NUM      (8)
#define NODE_ID_INVALID   (0xFFFF)
#define NET_LIST_NUM      (4)

typedef enum {
    NET_PROTOCOL_TCP = 0,
    NET_PROTOCOL_RDMA  = 1,
    NET_PROTOCOL_BUTT
} NetProtocol;

typedef enum {
    NODE_STATE_INVALID = 0,
    NODE_STATE_UP      = 1,
    NODE_STATE_DOWN    = 2,
    NODE_STATE_BUTT
} NodeState;

typedef struct {
    uint32_t ipv4Addr;
    uint16_t port;
    uint16_t protocol; /* see NetProtocol */
} NetInfo;

typedef struct {
    uint16_t num;
    uint16_t resv;
    NetInfo list[NET_LIST_NUM];
} NetList;

typedef struct {
    uint64_t sessionId;
    uint16_t nodeId;
    uint16_t state; /* see NodeState */
    uint16_t groupId; /* ANY ID */
    uint16_t groupLevel; /* number of sync standbys that we need to wait for */
    NetList netList;
} NodeStateInfo;

typedef struct {
    uint16_t nodeNum;
    uint16_t masterNodeId;
    uint16_t localNodeId;
    NodeStateInfo nodeList[];
} NodeStateList;

typedef void (*FinishCbFun)(void *ctx, int ret);

typedef int32_t (*UwalNotifyNodeListChange)(NodeStateList *nodeList, FinishCbFun cb, void* ctx);

typedef uint32_t (*UWAL_Ipv4InetToInt)(char ipv4[16UL]);


#endif