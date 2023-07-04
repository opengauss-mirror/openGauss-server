/* -------------------------------------------------------------------------
 *
 * ndp_req.h
 *	  Exports from ndp/ndp_req.cpp
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/ndp/ndp_req.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef NDP_REQ_H_
#define NDP_REQ_H_

#include "common.h"
#include "ndp/ndp_nodes.h"

#include "component/ceph/ceph.h"
#include "component/thread/mpmcqueue.h"

const uint32 NDP_VERSION_NUM = 92899;
const uint32 NDP_LOCAL_VERSION_NUM = 4;

#define DSS_DEFAULT_AU_SIZE (4 * 1024 * 1024)

#define PAGE_NUM_PER_AU (DSS_DEFAULT_AU_SIZE / BLCKSZ)

#ifndef BITS_PER_BYTE
#define BITS_PER_BYTE 8
#endif

#define BITMAP_SIZE_PER_AU_BYTE (PAGE_NUM_PER_AU / BITS_PER_BYTE)
#define BITMAP_SIZE_PER_AU_U64 (BITMAP_SIZE_PER_AU_BYTE / sizeof(uint64))

#define NDPGETBYTE(x, i) (*((char*)(x) + (int)((i) / BITS_PER_BYTE)))
#define NDPCLRBIT(x, i) NDPGETBYTE(x, i) &= ~(0x01 << ((i) % BITS_PER_BYTE))
#define NDPSETBIT(x, i) NDPGETBYTE(x, i) |= (0x01 << ((i) % BITS_PER_BYTE))
#define NDPGETBIT(x, i) ((NDPGETBYTE(x, i) >> ((i) % BITS_PER_BYTE)) & 0x01)
#define NDPMERGEBIT(x, y) ((x << 3) + y)
#define NDPGETARG1(z) (z >> 3)
#define NDPGETARG2(z) (z & 0b0111)

#define TABLE_MAX_NUM 128

typedef enum NdpResult {
    NDP_OK = 0,
    NDP_ERR,
    NDP_ILLEGAL
} NdpResult;

typedef enum NdpCommand {
    NDP_CONNECT = 0,
    NDP_QUERY,
    NDP_PLAN,
    NDP_PLANSTATE,
    NDP_TERMINATE,
    NDP_VERSION
} NdpCommand;

typedef struct AuInfo {
    uint32 phyStartBlockNum;
    int pageNum;
    CephObject object;
} AuInfo;

typedef struct NdpReqHeader {
    uint8 command;
    uint32 size;
} NdpReqHeader;

typedef struct NdpAdminRequest {
    NdpReqHeader head;

    uint16 taskId;
    uint16 tableId;

    char data[0];
} NdpAdminRequest;

// take all here, but send needed
typedef struct NdpAdminResponse {
    NdpResult ret;
    uint16 queryId;
} NdpAdminResponse;

typedef struct NdpIORequest {
    uint16 taskId;
    uint16 tableId;
    AuInfo auInfos[1];
    uint64 pageMap[BITMAP_SIZE_PER_AU_U64];
} NdpIORequest;

typedef struct NdpIOResponse {
    uint16 taskId;
    uint16 status;
    uint32 ndpPageNums;
    uint64 pageMap[BITMAP_SIZE_PER_AU_U64];
    /* statistic */
} NdpIOResponse;

typedef struct AioDesc {
    rbd_completion_t com;
    char* readBuf;
    int len;
} AioDesc;

typedef class NdpIOTask {
public:
    NdpIOTask(uintptr_t h, NdpIORequest* req) : handle(h), header(req), aioDesc(nullptr), aioRet(STATUS_OK) {}

    ~NdpIOTask();

    Status InitBuffer();

    uintptr_t handle;
    NdpIORequest* header;
    AioDesc* aioDesc;
    Status aioRet;
} NdpIOTask;

#ifndef NDP_CLIENT
Status NdpAdminProc(NdpAdminRequest *header, NdpAdminResponse& resp, size_t& len);
Status NdpIOProc(NdpIORequest *header, void *reply);
#endif

init_type InitNdpQueryMgr();
void DestroyNdpQueryMgr();

#endif /* NDP_REQ_H_ */
