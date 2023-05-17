/* -------------------------------------------------------------------------
 * ndpam.h
 *	  prototypes for functions in contrib/ndpplugin/ndpam.cpp
 *
 * Portions Copyright (c) 2022 Huawei Technologies Co.,Ltd.
 *
 * IDENTIFICATION
 *	  contrib/ndpplugin/ndpam.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef NDPAM_H
#define NDPAM_H

#include "component/thread/mpmcqueue.h"

#include "ndpplugin.h"  // for global instance config

#define PARALLEL_SCAN_GAP_AU_ALIGNED ((unsigned)PAGE_NUM_PER_AU << 1)

enum class NdpRetCode {
    // ok
    NDP_OK = 0,
    // for desc
    ALLOC_MC_FAILED,
    ALLOC_MQ_FAILED,
    // for channel
    CONNECT_FAILED,
    CONNECT_UNUSABLE,
    RPC_ADMIN_SEND_FAIL,
    TABLE_ID_INVALID,
    TABLE_MGR_UNUSABLE,
    RPC_ADMIN_SEND_CTX_FAILED,
    RPC_ADMIN_SEND_PLAN_FAILED,
    RPC_ADMIN_SEND_STATE_FAILED,
    RPC_ADMIN_SEND_TERMINATE_FAILED,
    RPC_ADMIN_SEND_VERSION_FAILED,
    RPC_IO_SEND_FAILED,
    RPC_IO_CALLBACK_ERROR,
    // for memory alloc
    ALLOC_RESPONSE_MEMORY_FAILED,
    NDP_RETURN_FAILED,
    NDP_RETURN_STATUS_ERROR,
    NDP_ERROR,
    NDP_CONSTRUCT_FAILED
};

typedef PageHeaderData NdpPageHeaderData;
typedef PageHeaderData* NdpPageHeader;

class NdpIoSlot : public BaseObject {
public:
    explicit NdpIoSlot(void* privateData)
    : priv(privateData), resp(nullptr), respRet(NdpRetCode::NDP_OK), rpcStatus(RPC_OK)
    {
        reqMsg.data = respMsg.data = nullptr;
        reqMsg.len = respMsg.len = 0;
    }
    ~NdpIoSlot() { FreeResp(); }

    int SetReq(RelFileNode& node, uint16 taskId, uint16 tableId, AuInfo& auinfo);
    void SetReq(uint16 taskId) { req.taskId = taskId; }
    NdpRetCode SetResp(int pageNum);
    void SetRespRet(NdpRetCode code) { respRet = code; }
    void SetStartBlockNum(uint32 start) { startBlockNum = start; }
    void* GetPriv(void) { return priv; }
    uint16 GetReqTableId(void) { return req.tableId; }
    NdpRetCode GetResp(NdpPageHeader& pages, int& pageNum, BlockNumber& start, uint64*& map);

    RpcMessage* GetReqMsg(void) { return &reqMsg; }
    RpcMessage* GetRespMsg(void) { return &respMsg; }
    uint32 GetStartBlockNum(void) { return startBlockNum; }
    int GetPushDownPageNum(void) {return req.auInfos[0].pageNum;};
    void SetRPCRet(RpcStatus rpccode) { rpcStatus = rpccode; }
private:
    void FreeResp(void);

    void* priv;
    RpcMessage reqMsg;
    RpcMessage respMsg;
    NdpIORequest req;
    NdpIOResponse* resp;
    NdpRetCode respRet;
    uint32 startBlockNum;
    RpcStatus rpcStatus;
};

/*
 * plan <-> NdpContext
 *   |----ScanNode <-> NdpScanCondition
 *            |----Scan producer <-> NdpScanDescData
 *            |----Scan producer <-> NdpScanDescData
 *   |----ScanNode <-> NdpScanCondition
 */
typedef struct NdpContext {  // for each plan tree
    MemoryContext ccMem;
    pthread_rwlock_t ccLock;
    struct HTAB* channelCache;  // for connector <=> rpc

    uint32 rpcCount;
    uint32 tableCount;  // for oid <=> tableId, also can do in server
    knl_session_context* u_sess; // for statistics without ENABLE_THREAD_POOL
} NdpContext;

class NdpScanDescData : public BaseObject {  // for each scan thread
public:
    TableScanDesc scan;

    int curPageType;
    int curLinesNum;

    NdpIoSlot* curIO;  // for free
    NdpPageHeader curNdpPages;
    int curNdpPagesNum;
    int nextNdpPageIndex;

    NdpPageHeader curNdpPage;  // can unify to id, if pushdown page store after global shared memory
    int curNormalPageId;

    int nextTupleOffset;  // for access tuple in pushdown page
    int nextLineIndex;

#ifdef NDP_ASYNC_RPC
    pg_atomic_uint32 reqCount;
    pg_atomic_uint32 respCount;
    MpmcBoundedQueue<NdpIoSlot*>* respIO{nullptr};
    MpmcBoundedQueue<int>* normalPagesId{nullptr};
#else
    int normalPagesNum;
    int normalPagesId[PAGE_NUM_PER_AU];
#endif

    BlockNumber handledBlock;  // number of handled block
    BlockNumber nBlock;  // block's number of the scan relation

    // for statistics
    int sendFailedN{0};
    int failedIoN{0};
    int normalPageN{0};
    int pushDownPageN{0};
    int sendBackPageN{0};
    int ndpPageAggN{0};
    int ndpPageScanN{0};
    int rev{0};

    NdpScanCondition* cond;  // for Plan
    ScanState* scanState;
    AggState* aggState;
    TupleTableSlot* aggSlot;

    MemoryContext memCtx{nullptr};

    NdpScanDescData() = default;
    ~NdpScanDescData();
    NdpRetCode Init(ScanState* sstate, TableScanDesc sscan);
    void Reset(void);
    void AddToNormal(uint32 start, uint32 end);
    void AddToNormal(uint32 block)
    {
#ifdef NDP_ASYNC_RPC
        if (!normalPagesId->Enqueue(block)) {
            ereport(ERROR, (errmsg("normal page exceed limit.")));
        }
#else
        normalPagesId[normalPagesNum] = block;
        normalPagesNum++;
#endif
        normalPageN++;
    }

    bool HandleSlot(NdpIoSlot* slot);
#ifdef NDP_ASYNC_RPC
    bool GetNextSlot(void);
#endif
    void FreeCurSlot(void)
    {
        if (curIO) {
            delete curIO;
            curIO = nullptr;
        }
    }
};
typedef NdpScanDescData* NdpScanDesc;

enum class NdpScanChannelStatus{
    UNCONNECTED = 0,
    CONNECTED,
    QUERYSENT,
    CLOSED
};

enum class NdpTableStatus {
    INITIAL = 0,
    PLANSENT,
    STATESENT,
    CONSTRUCTFAIL
};

struct NdpTableMgr : public BaseObject {
    volatile NdpTableStatus status = NdpTableStatus::INITIAL;
    pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;  // protect status
    uint16 ioFailed = 0;
    uint16 cmdNdpFailed = 0;
};

// connector <=> channel
struct NdpScanChannel {
    char rpcIp[NDP_RPC_IP_LEN];
    uint32 rpcId;  // handle of rpc, support multi-thread

    volatile NdpScanChannelStatus status;  // atomic access, called by multi-thread
    pthread_mutex_t mutex;

    RpcClient rpcClient;
    uint16 queryId;

    uint32 tableNum;
    NdpTableMgr* tableMgr;  // status to know if condition sent

    uint32 connFailed;
    uint16 cmdFailed;

    NdpTableStatus GetTableStatus(uint16 tableId) 
    {
        if (tableId >= tableNum) {
            return NdpTableStatus::CONSTRUCTFAIL;
        }
        return tableMgr[tableId].status;
    }

    void DestroyChannel()
    {
        DisconnectRpc();
        status = NdpScanChannelStatus::CLOSED;
        if (tableMgr) {
            delete []tableMgr;
        }
        tableNum = 0;
        pthread_mutex_destroy(&mutex);
    }
    // do initialize in init instead of constructor, because allocated by Hash insert
    bool Init(uint32 id, char* ip, uint32 tableN);
    NdpRetCode SendRequest(NdpIoSlot* req, NdpScanDesc ndpScan);  // should support multi-thread
    NdpRetCode SendEnd();
    NdpRetCode SendAdminReq(NdpAdminRequest* req, NdpAdminResponse* resp, size_t size);

    // this function can only be called under mutex locked, need atomic write status
    void DisconnectRpc()
    {
        if (rpcClient) {
            RpcClientDisconnect(rpcClient);
            rpcClient = 0;
            status = NdpScanChannelStatus::UNCONNECTED;
        }
    }

    NdpRetCode SendReq(NdpIoSlot* req, NdpScanDesc ndpScan);
    NdpRetCode SendAdmin(NdpTableMgr* mgr, NdpIoSlot* req, NdpScanDesc ndpScan);
    NdpRetCode SendIo(NdpIoSlot* req, NdpScanDesc ndpScan);
    NdpRetCode SendQuery(NdpScanDesc ndpScan);
    NdpRetCode SendPlan(NdpScanDesc ndpScan);
    NdpRetCode SendState(NdpScanDesc ndpScan);

    NdpAdminRequest* ConstructPlanReq(NdpScanDesc ndpScan);
    bool ExtractTupleDesc(TupleDesc desc, NdpTupleDesc* td);
    bool ExtractRelation(TableScanDesc scan, NdpRelation* rel);
    bool ExtractXact(TableScanDesc scan, NdpXact* xact);
    bool ExtractAggState(NdpScanDesc ndpScan, NdpAggState* aggS);
    NdpPlanState* CreatePlanState(NdpScanDesc ndpScan);
    void DestroyPlanState(NdpPlanState* state);
    NdpAdminRequest* ConstructPlanState(NdpScanDesc ndpScan);
    NdpAdminRequest* ConstructQuery(NdpScanDesc ndpScan);
    NdpAdminRequest* ConstructVersion();
};

struct NdpPageMethod {
    void (*get_pageinfo)(NdpScanDesc ndpScan, BlockNumber page, CephObject *object, char *ip,
                         BlockNumber& end, uint32& phyStartBlockNum);
};

void pm_get_pageinfo(NdpScanDesc ndpScan, BlockNumber page, CephObject *object, char *ip,
                     BlockNumber& end, uint32& phyStartBlockNum);
void md_get_pageinfo(NdpScanDesc ndpScan, BlockNumber page, CephObject *object, char *ip,
                     BlockNumber& end, uint32& phyStartBlockNum);
void seg_get_pageinfo(NdpScanDesc ndpScan, BlockNumber page, CephObject *object, char *ip,
                      BlockNumber& end, uint32& phyStartBlockNum);

static const NdpPageMethod PAGEMETHOD[] {
    {
        md_get_pageinfo,
    },
    {
        seg_get_pageinfo,
    }
};

#endif // NDPAM_H
