/* -------------------------------------------------------------------------
 * ndpam.cpp
 *	  Routines to handle ndp page
 *
 * Portions Copyright (c) 2022 Huawei Technologies Co.,Ltd.
 *
 * IDENTIFICATION
 *	  contrib/ndpplugin/ndpam.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "access/csnlog.h"
#include "access/slru.h"
#include "executor/node/nodeAgg.h"

#include "component/rpc/rpc.h"
#include "storage/smgr/segment_internal.h"
#include "ddes/dms/ss_transaction.h"
#include "algorithm"
#include "storage/dss/fio_dss.h"
#include "storage/smgr/segment.h"

#include "ndpnodes.h"
#include "ndpam.h"

#define NDP_PAGE_QUEUE_SIZE (1u << 10)
#define NDP_NORMAL_QUEUE_SIZE (1u << 12)

#define ClogCtl(n) (&t_thrd.shemem_ptr_cxt.ClogCtl[CBufHashPartition(n)])
#define CsnlogCtl(n) (&t_thrd.shemem_ptr_cxt.CsnlogCtlPtr[CSNBufHashPartition(n)])
#define CSNLOG_XACTS_PER_PAGE (BLCKSZ / sizeof(CommitSeqNo))
#define CSN_LWLOCK_ACQUIRE(pageno, lockmode) ((void)LWLockAcquire(CSNBufMappingPartitionLock(pageno), lockmode))
#define CSN_LWLOCK_RELEASE(pageno) (LWLockRelease(CSNBufMappingPartitionLock(pageno)))

#define TransactionIdToCSNPage(xid) ((xid) / (TransactionId)CSNLOG_XACTS_PER_PAGE)

constexpr int RPC_FAILED_LIMIT = 3;
constexpr int SEND_FAILED_LIMIT = 10;

inline static void SegLogicPageIdToExtentOffset(BlockNumber logicId, uint32* offset)
{
    if (logicId < EXT_SIZE_8_TOTAL_PAGES) {
        *offset = logicId % EXT_SIZE_8;
    } else if (logicId < EXT_SIZE_128_TOTAL_PAGES) {
        logicId -= EXT_SIZE_8_TOTAL_PAGES;
        *offset = logicId % EXT_SIZE_128;
    } else if (logicId < EXT_SIZE_1024_TOTAL_PAGES) {
        logicId -= EXT_SIZE_128_TOTAL_PAGES;
        *offset = logicId % EXT_SIZE_1024;
    } else {
        logicId -= EXT_SIZE_1024_TOTAL_PAGES;
        *offset = logicId % EXT_SIZE_8192;
    }
}

static void md_get_physical_info(Relation rel, ForkNumber forknum, BlockNumber blocknum,
                                 int *handle, off_t *offset)
{
    SMgrRelation reln = rel->rd_smgr;
    MdfdVec *v = NULL;
    v = _mdfd_getseg(reln, forknum, blocknum, false, EXTENSION_FAIL);
    Assert(v != NULL);

    *handle = FileFd(v->mdfd_vfd);
    *offset = DF_OFFSET_TO_SLICE_OFFSET(((off_t)blocknum) * BLCKSZ);
}

void md_get_pageinfo(NdpScanDesc ndpScan, BlockNumber page, CephObject *object, char *ip,
                     BlockNumber& end, uint32& phyStartBlockNum)
{
    // step1:get object and ip.
    int handle;
    off_t offset;
    HeapScanDesc scan = (HeapScanDesc)ndpScan->scan;
    md_get_physical_info(scan->rs_base.rs_rd, 0, page, &handle, &offset);

#ifdef GlobalCache
    dss_get_addr(handle, offset, object->pool, object->image, ip, &object->objId, &object->objOffset);
#endif

    // step2:slice pages, get end and phyStartBlockNum.
    BlockNumber start = ndpScan->handledBlock;
#ifdef GlobalCache
    Assert(object->objOffset >= 0);
    end = Min((uint64_t)ndpScan->nBlock, (uint64_t)(start + PAGE_NUM_PER_AU - object->objOffset/BLCKSZ));
#else
    end = Min((uint64_t)ndpScan->nBlock, (uint64_t)(start + PAGE_NUM_PER_AU));
#endif
    phyStartBlockNum = NDPMERGEBIT(start, 0);
}

static void seg_get_physical_info(Relation rel, ForkNumber forknum, BlockNumber blocknum,
                                  SegPageLocation &loc, int *handle, off_t *offset)
{
    SMgrRelation reln = rel->rd_smgr;
    loc = seg_get_physical_location(rel->rd_node, MAIN_FORKNUM, blocknum);
    SegmentCheck(loc.blocknum != InvalidBlockNumber);

    LockSegmentHeadPartition(reln->seg_space->spcNode, reln->seg_space->dbNode,
                             reln->seg_desc[forknum]->head_blocknum, LW_SHARED);

    SegSpace *spc = reln->seg_space;

    RelFileNode relNode = {.spcNode = spc->spcNode,
                           .dbNode = spc->dbNode,
                           .relNode = EXTENT_SIZE_TO_TYPE(loc.extent_size),
                           .bucketNode = SegmentBktId,
                           .opt = 0};
    int egid = EXTENT_TYPE_TO_GROUPID(relNode.relNode);
    SegExtentGroup *seg = &spc->extent_group[egid][forknum];

    off_t beginoff = ((off_t)loc.blocknum) * BLCKSZ;
    int sliceno = DF_OFFSET_TO_SLICENO(beginoff);
    SegPhysicalFile spf = df_get_physical_file(seg->segfile, sliceno, loc.blocknum);
    *handle = spf.fd;

    *offset = DF_OFFSET_TO_SLICE_OFFSET(beginoff);

    UnlockSegmentHeadPartition(reln->seg_space->spcNode, reln->seg_space->dbNode,
                               reln->seg_desc[forknum]->head_blocknum);
}

void seg_get_pageinfo(NdpScanDesc ndpScan, BlockNumber page, CephObject *object, char *ip,
                      BlockNumber& end, uint32& phyStartBlockNum)
{
    // step1:get object and ip.
    int handle;
    off_t offset;
    SegPageLocation loc;
    HeapScanDesc scan = (HeapScanDesc)ndpScan->scan;
    seg_get_physical_info(scan->rs_base.rs_rd, 0, page, loc, &handle, &offset);

#ifdef GlobalCache
    dss_get_addr(handle, offset, object->pool, object->image, ip, &object->objId, &object->objOffset);
#endif

    // step2:slice pages, get end and phyStartBlockNum.
    uint32 extentOff;
    BlockNumber start = ndpScan->handledBlock;
    SegLogicPageIdToExtentOffset(ndpScan->handledBlock, &extentOff);
    uint64_t blks = Min(loc.extent_size - extentOff,
                        Min(uint32(PAGE_NUM_PER_AU), uint32(ndpScan->nBlock - ndpScan->handledBlock)));
#ifdef GlobalCache
    Assert(object->objOffset >= 0);
    end = start + Min(blks, (uint64_t)(PAGE_NUM_PER_AU - object->objOffset/BLCKSZ));
#else
    end = start + blks;
#endif

    phyStartBlockNum = NDPMERGEBIT(loc.blocknum, EXTENT_SIZE_TO_GROUPID(loc.extent_size) + 1);
}

void pm_get_pageinfo(NdpScanDesc ndpScan, BlockNumber page, CephObject *object, char *ip,
                     BlockNumber& end, uint32& phyStartBlockNum)
{
    HeapScanDesc scan = (HeapScanDesc)ndpScan->scan;
    int which = scan->rs_base.rs_rd->rd_smgr->smgr_which;
    FileType filetype = which == 0 ? MDFILE : (which == 2 ? SEGFILE : INVALIDFILE);
    Assert(filetype != INVALIDFILE);
    PAGEMETHOD[filetype].get_pageinfo(ndpScan, page, object, ip, end, phyStartBlockNum);

    uint64 curAUAligned = ndpScan->handledBlock / PARALLEL_SCAN_GAP_AU_ALIGNED;
    if (scan->dop > 1 && end > (curAUAligned + 1) * PARALLEL_SCAN_GAP_AU_ALIGNED) {
        end = (curAUAligned + 1) * PARALLEL_SCAN_GAP_AU_ALIGNED;
    }
}

void CopyCLog(int64 pageno, char *pageBuffer)
{
    int slotno;
    errno_t rc = EOK;

    /* lock is acquired by SimpleLruReadPage_ReadOnly */
    slotno = SimpleLruReadPage_ReadOnly(ClogCtl(pageno), pageno, FirstNormalTransactionId);
    rc = memcpy_s(pageBuffer, BLCKSZ, ClogCtl(pageno)->shared->page_buffer[slotno], BLCKSZ);
    securec_check(rc, "", "");

    LWLockRelease(ClogCtl(pageno)->shared->control_lock);
}

void CopyCSNLog(int64 pageno, char *pageBuffer)
{
    int slotno;
    errno_t rc = EOK;

    CSN_LWLOCK_ACQUIRE(pageno, LW_SHARED);
    slotno = SimpleLruReadPage_ReadOnly_Locked(CsnlogCtl(pageno), pageno, FirstNormalTransactionId);
    rc = memcpy_s(pageBuffer, BLCKSZ, CsnlogCtl(pageno)->shared->page_buffer[slotno], BLCKSZ);
    securec_check(rc, "", "");
    CSN_LWLOCK_RELEASE(pageno);
}

int NdpIoSlot::SetReq(RelFileNode& node, uint16 taskId, uint16 tableId, AuInfo& auinfo)
{
    int bitCount = 0;

    reqMsg.data = &req;
    reqMsg.len = sizeof(NdpIORequest);
    respMsg.data = nullptr;
    respMsg.len = 0;

    req.taskId = taskId;
    req.tableId = tableId;
    req.auInfos[0].phyStartBlockNum = auinfo.phyStartBlockNum;
    req.auInfos[0].pageNum = auinfo.pageNum;
#ifdef GlobalCache
    errno_t rc2 = memcpy_s(&req.auInfos[0].object, sizeof(CephObject), &auinfo.object, sizeof(CephObject));
    securec_check(rc2, "", "");
#endif
    errno_t rc = memset_s(req.pageMap, BITMAP_SIZE_PER_AU_BYTE, 0, BITMAP_SIZE_PER_AU_BYTE);
    securec_check(rc, "", "");

    for (uint32 i = startBlockNum, offset = 0; i != startBlockNum + auinfo.pageNum; ++i, ++offset) {
        bool cached = IsPageHitBufferPool(node, MAIN_FORKNUM, i);
        if (!cached) {
            NDPSETBIT(req.pageMap, offset);
            ++bitCount;
        }
    }
    if (SS_STANDBY_MODE) {
        SSIsPageHitDms(node, startBlockNum, auinfo.pageNum, req.pageMap, &bitCount);
    }
    return bitCount;
}

NdpRetCode NdpIoSlot::SetResp(int pageNum)
{
#ifdef ENABLE_SSL
    respMsg.len = 0;
    respMsg.data = nullptr;
    return NdpRetCode::NDP_OK;
#else
    respMsg.len = DSS_DEFAULT_AU_SIZE;
    if (g_ndp_instance.pageContext->Dequeue(respMsg.data)) {
        resp = reinterpret_cast<NdpIOResponse*>(respMsg.data);
        return NdpRetCode::NDP_OK;
    }
    return NdpRetCode::ALLOC_RESPONSE_MEMORY_FAILED;
#endif
}

NdpRetCode NdpIoSlot::GetResp(NdpPageHeader& pages, int& pageNum, BlockNumber& start, uint64*& map)
{
    start = startBlockNum;
    map = nullptr;
    pages = nullptr;
    pageNum = 0;

    if (respRet != NdpRetCode::NDP_OK) {
        return respRet;
    }

    auto rpcResp = reinterpret_cast<NdpIOResponse*>(respMsg.data);
    if (rpcResp == nullptr) {
        return NdpRetCode::NDP_RETURN_FAILED;
    }
#ifdef ENABLE_SSL
    resp = reinterpret_cast<NdpIOResponse*>(respMsg.data);
#endif
    if (rpcResp->status != 0) {
        return NdpRetCode::NDP_RETURN_STATUS_ERROR;
    }

    map = rpcResp->pageMap;
    pageNum = rpcResp->ndpPageNums;
    if (pageNum) {
        pages = (NdpPageHeader)((char*)rpcResp + sizeof(NdpIOResponse));
    }

    return NdpRetCode::NDP_OK;
}

void NdpIoSlot::FreeResp()
{
#ifdef ENABLE_SSL
    if (resp != nullptr) {
        free(resp);
        resp = nullptr;
        return;
    }
    if (respMsg.data) {
        free(respMsg.data);
        respMsg.data = nullptr;
        return;
    }
#else
    bool enqueued;
    Assert(g_ndp_instance.pageContext);
    if (resp) {
        enqueued = g_ndp_instance.pageContext->Enqueue(resp);
        if (!enqueued) {
            ereport(WARNING, (errmsg("try enqueue slot memory to queue failed")));
        }
        resp = nullptr;
        return;
    }
    if (respMsg.data) {
        enqueued = g_ndp_instance.pageContext->Enqueue(respMsg.data);
        if (!enqueued) {
            ereport(WARNING, (errmsg("try enqueue slot memory to queue failed")));
        }
        respMsg.data = nullptr;
        return;
    }
#endif
}

NdpRetCode NdpScanDescData::Init(ScanState* sstate, TableScanDesc sscan)
{
    curIO = nullptr;  // necessary, because rescan may be before get next

    // free everything in destructor if alloc failed
#ifdef NDP_ASYNC_RPC
    pg_atomic_init_u32(&reqCount, 0);
    pg_atomic_init_u32(&respCount, 0);

    respIO = new MpmcBoundedQueue<NdpIoSlot*>(NDP_PAGE_QUEUE_SIZE);
    if (respIO == nullptr) {
        ereport(WARNING, (errmsg("Alloc NpdPage Queue failed, size %d", NDP_PAGE_QUEUE_SIZE)));
        return NdpRetCode::ALLOC_MQ_FAILED;
    }
    normalPagesId = new MpmcBoundedQueue<int>(NDP_NORMAL_QUEUE_SIZE);
    if (normalPagesId == nullptr) {
        ereport(WARNING, (errmsg("Alloc Normal Queue failed, size %d", NDP_NORMAL_QUEUE_SIZE)));
        return NdpRetCode::ALLOC_MQ_FAILED;
    }
#endif

    memCtx = AllocSetContextCreate(CurrentMemoryContext, "ThreadNdpScanContext", 0, (4u << 10), (4u << 10));
    if (!memCtx) {
        ereport(WARNING, (errmsg("Create ThreadNdpScanContext failed!")));
        return NdpRetCode::ALLOC_MC_FAILED;
    }

#ifdef FAULT_INJECT
    if ((rand() % PERCENTAGE_DIV) < PERCENTAGE) {
        ereport(WARNING, (errmsg("Fault inject -- Create ThreadNdpScanContext failed")));
        return NdpRetCode::ALLOC_MC_FAILED;
    }
#endif

    cond = (NdpScanCondition*)sstate->ps.plan->ndp_pushdown_condition;
    scan = sscan;
    scanState = sstate;
    aggState = NULL;
    aggSlot = NULL;

    return NdpRetCode::NDP_OK;
}

NdpScanDescData::~NdpScanDescData()
{
    // wait all callback return
    while (pg_atomic_read_u32(&reqCount) != pg_atomic_read_u32(&respCount)) {
        pg_usleep(NDP_RPC_WAIT_USEC);
    }
#ifdef NDP_ASYNC_RPC
    if (respIO) {
        NdpIoSlot* tmpIO = nullptr;
        while (respIO->Dequeue(tmpIO)) {
            delete tmpIO;
        }
        delete respIO;
    }
    if (normalPagesId) {
        delete normalPagesId;
    }
#endif
    if (memCtx) {
        MemoryContextDelete(memCtx);
    }
}

void NdpScanDescData::Reset()
{
    handledBlock = 0;
    curLinesNum = 0;
    nextLineIndex = 0;

#ifdef NDP_ASYNC_RPC
    // add timestamp if we don't want to wait, and remember release all while deleting.
    while (pg_atomic_read_u32(&respCount) < pg_atomic_read_u32(&reqCount)) {
        pg_usleep(NDP_RPC_WAIT_USEC);
    }
#endif

    FreeCurSlot();

#ifdef NDP_ASYNC_RPC
    NdpIoSlot* tmpIO = nullptr;
    int tmpId;

    while (respIO->Dequeue(tmpIO)) {
        delete tmpIO;
    }
    while (normalPagesId->Dequeue(tmpId));
#else
    normalPagesNum = 0;
#endif

    MemoryContextReset(memCtx);
}

void NdpScanDescData::AddToNormal(uint32 start, uint32 end)
{
    end = end > nBlock ? nBlock : end;
    start = start > nBlock ? nBlock : start;
    for (uint32 i = start; i < end; ++i) {
        AddToNormal(i);
    }
}

// return true if set current io slot;
bool NdpScanDescData::HandleSlot(NdpIoSlot* slot)
{
    uint32 start;
    uint64* pageMap;
    NdpPageHeader pages;
    int pageNum;

    NdpRetCode ret = slot->GetResp(pages, pageNum, start, pageMap);
    if (ret != NdpRetCode::NDP_OK) {
        ++failedIoN;
    }

    if (start > nBlock) {
        ereport(ERROR, (errmsg("can not happen start %u is cross the border.", start)));
        return false;
    }

    uint32 end = start + slot->GetPushDownPageNum();
    if (pageMap == nullptr) {  // slot is invalid, add to normal list
        AddToNormal(start, end);
        return false;
    }

    // put unhandled page to normal queue
    int count = 0;
    end = end > nBlock ? nBlock : end;
    start = start > nBlock ? nBlock : start;
    for (uint32 offset = start, i = 0; offset < end; ++i, ++offset) {
        int flag = NDPGETBIT(pageMap, i);
        if (!flag) {
            AddToNormal(offset);
            count++;
        }
    }
    sendBackPageN += count;

    if (scanState->ps.instrument) {
        scanState->ps.instrument->ndp_sendback_page += count;
    }

    if (pages) {
        curIO = slot;
        curNdpPages = pages;
        curNdpPagesNum = pageNum;
        nextNdpPageIndex = 0;
        if (pages->pd_flags == NDP_FILTERED_PAGE) {
            ndpPageScanN += pageNum;
        } else {
            ndpPageAggN += pageNum;
        }
        if (scanState->ps.instrument) {
            scanState->ps.instrument->ndp_handled += pageNum;
        }
        return true;
    } else {
        return false;
    }
}

#ifdef NDP_ASYNC_RPC
bool NdpScanDescData::GetNextSlot(void)
{
    NdpIoSlot* slot = nullptr;
    if (respIO->Dequeue(slot)) {
        if (HandleSlot(slot)) {
            return true;
        } else {
            delete slot;
            return false;
        }
    }
    return false;
}
#endif

#ifdef NDP_ASYNC_RPC
void NdpIoSlotCallDone(RpcStatus status, void *arg)
{
    NdpIoSlot* cbArg = reinterpret_cast<NdpIoSlot*>(arg);
    if (!cbArg) {
        return;
    }

    NdpScanDesc ndpScan = reinterpret_cast<NdpScanDesc>(cbArg->GetPriv());
    if (ndpScan == nullptr) {
        return;
    }

#ifdef FAULT_INJECT
    if ((rand() % PERCENTAGE_DIV) < PERCENTAGE) {
        status = RPC_ERROR;
    }
#endif

    if (status != RPC_OK) {
        cbArg->SetRespRet(NdpRetCode::RPC_IO_CALLBACK_ERROR);
        cbArg->SetRPCRet(status);
    }

    while (!ndpScan->respIO->Enqueue(cbArg)) {
        pg_usleep(NDP_RPC_WAIT_USEC);
    }

    pg_atomic_add_fetch_u32(&ndpScan->respCount, 1);
}
#endif

bool NdpScanChannel::Init(uint32 id, char* ip, uint32 tableN)
{
    rpcId = id;
    errno_t rc = strcpy_s(rpcIp, NDP_RPC_IP_LEN, ip);
    securec_check(rc, "", "");

    status = NdpScanChannelStatus::UNCONNECTED;
    pthread_mutex_init(&mutex, nullptr);

    rpcClient = 0;
    queryId = 0;
    tableNum = tableN;
    tableMgr = New(CurrentMemoryContext) NdpTableMgr[tableNum]();
    if (tableMgr == nullptr) {
        return false;
    }
    connFailed = 0;
    cmdFailed = 0;
    return true;
}

NdpRetCode NdpScanChannel::SendRequest(NdpIoSlot* req, NdpScanDesc ndpScan)
{
    if (status == NdpScanChannelStatus::CLOSED) {
        return NdpRetCode::CONNECT_UNUSABLE;
    }
    if (req->GetReqTableId() >= tableNum) {
        ereport(WARNING, (errmsg("table id %u should be littler then %u.", req->GetReqTableId(), tableNum)));
        return NdpRetCode::TABLE_ID_INVALID;
    }

    if (status == NdpScanChannelStatus::QUERYSENT) {
        return SendReq(req, ndpScan);
    }

    if (pthread_mutex_trylock(&mutex) != 0) {
        return NdpRetCode::CONNECT_UNUSABLE;
    }

    NdpRetCode ret = NdpRetCode::NDP_OK;
    switch (status) {
        case NdpScanChannelStatus::UNCONNECTED: {
            // free old connection
            DisconnectRpc();
            // call rpc connect
            RpcStatus connectRet = RpcClientConnect(rpcIp, u_sess->ndp_cxt.ndp_port, rpcClient);
            if (SECUREC_UNLIKELY(connectRet != RPC_OK)) {
                ++connFailed;
                DisconnectRpc();
                if (connFailed >= RPC_FAILED_LIMIT) {
                    status = NdpScanChannelStatus::CLOSED;
                }
                ereport(LOG, (errmsg("rpc connect (count:%d) failed, ip:port %s:%d. rpc status: %d",
                                     connFailed, rpcIp, u_sess->ndp_cxt.ndp_port, connectRet)));
                ret = NdpRetCode::CONNECT_FAILED;
                break;
            }
            status = NdpScanChannelStatus::CONNECTED;
        }
        case NdpScanChannelStatus::CONNECTED: {
            PG_TRY();
            {
                ret = SendQuery(ndpScan);
            }
            PG_CATCH();
            {
                ereport(WARNING, (errmsg("send query failed, it is possible a palloc failed.")));
                pthread_mutex_unlock(&mutex);
                PG_RE_THROW();
            }
            PG_END_TRY();

            if (SECUREC_UNLIKELY(ret != NdpRetCode::NDP_OK)) {
                ++cmdFailed;
                if (cmdFailed >= RPC_FAILED_LIMIT) {
                    status = NdpScanChannelStatus::CLOSED;
                }
                break;
            }
            status = NdpScanChannelStatus::QUERYSENT;
        }
        case NdpScanChannelStatus::QUERYSENT: {
            pthread_mutex_unlock(&mutex);
            return SendReq(req, ndpScan);
        }
        case NdpScanChannelStatus::CLOSED:
        default: {
            ret = NdpRetCode::CONNECT_UNUSABLE;
            break;
        }
    }
    pthread_mutex_unlock(&mutex);
    return ret;
}

NdpRetCode NdpScanChannel::SendEnd()
{
    NdpRetCode retCode = NdpRetCode::NDP_OK;
    if (status != NdpScanChannelStatus::QUERYSENT) {
        /* If all pages are cached in BufferPool, the channel might be UNCONNECTED */
        ereport(LOG, (errmsg("channel %s is not QUERY_SENT.", rpcIp)));
        return NdpRetCode::CONNECT_UNUSABLE;
    }

    pthread_mutex_lock(&mutex);
    if (status == NdpScanChannelStatus::QUERYSENT) {
        NdpAdminRequest req;
        NdpAdminResponse resp;
        req.head.command = NDP_TERMINATE;
        req.head.size = sizeof(NdpAdminRequest);
        req.taskId = queryId;
        req.tableId = 0;
        NdpRetCode ret = SendAdminReq(&req, &resp, sizeof(resp.ret));
        if (ret != NdpRetCode::NDP_OK) {
            retCode = NdpRetCode::RPC_ADMIN_SEND_TERMINATE_FAILED;
        } else {
            status = NdpScanChannelStatus::CLOSED;
        }
    }
    pthread_mutex_unlock(&mutex);
    return retCode;
}

NdpRetCode NdpScanChannel::SendReq(NdpIoSlot* req, NdpScanDesc ndpScan)
{
    NdpTableMgr* mgr = &tableMgr[req->GetReqTableId()];
    if (mgr->status == NdpTableStatus::CONSTRUCTFAIL) {
        return  NdpRetCode::NDP_CONSTRUCT_FAILED;
    }
    if (mgr->status == NdpTableStatus::STATESENT) {
        return SendIo(req, ndpScan);
    }
    if (mgr->cmdNdpFailed >= RPC_FAILED_LIMIT) {
        return NdpRetCode::RPC_ADMIN_SEND_FAIL;
    }

    if (pthread_mutex_trylock(&mgr->mutex) != 0) {
        return NdpRetCode::TABLE_MGR_UNUSABLE;
    }

    NdpRetCode ret;
    PG_TRY();
    {
        ret = SendAdmin(mgr, req, ndpScan);
    }
    PG_CATCH();
    {
        ereport(WARNING, (errmsg("send failed, it is possible a palloc failed.")));
        ret = NdpRetCode::NDP_ERROR;
    }
    PG_END_TRY();

    pthread_mutex_unlock(&mgr->mutex);
    if (mgr->status == NdpTableStatus::STATESENT) {
        return SendIo(req, ndpScan);
    }

    return ret;
}

NdpRetCode NdpScanChannel::SendAdmin(NdpTableMgr* mgr, NdpIoSlot* req, NdpScanDesc ndpScan)
{
    NdpRetCode ret = NdpRetCode::NDP_OK;
    switch (mgr->status) {
        case NdpTableStatus::INITIAL: {
            ret = SendPlan(ndpScan);
            if (ret != NdpRetCode::NDP_OK) {
                mgr->status = (ret == NdpRetCode::NDP_CONSTRUCT_FAILED) ?
                              NdpTableStatus::CONSTRUCTFAIL : mgr->status;
                break;
            }
            mgr->status = NdpTableStatus::PLANSENT;
        }
        case NdpTableStatus::PLANSENT: {
            ret = SendState(ndpScan);
            if (ret != NdpRetCode::NDP_OK) {
                mgr->status = (ret == NdpRetCode::NDP_CONSTRUCT_FAILED) ?
                              NdpTableStatus::CONSTRUCTFAIL : mgr->status;
                break;
            }
            mgr->status = NdpTableStatus::STATESENT;
        }
        case NdpTableStatus::STATESENT: {
            ret = NdpRetCode::NDP_OK;
            break;
        }
        default: {
            ret = NdpRetCode::NDP_ERROR;
            break;
        }
    }
    if (ret != NdpRetCode::NDP_OK) {
        ++mgr->cmdNdpFailed;
    }
    return ret;
}

NdpRetCode NdpScanChannel::SendQuery(NdpScanDesc ndpScan)
{
    NdpAdminRequest *v = ConstructVersion();
    NdpAdminResponse resp;
    NdpRetCode versionRet = SendAdminReq(v, &resp, sizeof(NdpAdminResponse));
    pfree(v);
    if (versionRet != NdpRetCode::NDP_OK) {
        DisconnectRpc();
        status = NdpScanChannelStatus::CLOSED;
        ereport(LOG, (errmsg("send version admin (count:%d) request failed, ip:port %s:%d.",
                             cmdFailed, rpcIp, u_sess->ndp_cxt.ndp_port)));
        return NdpRetCode::RPC_ADMIN_SEND_VERSION_FAILED;
    }

    NdpAdminRequest *query;
    resp = {};
    query = ConstructQuery(ndpScan);
    NdpRetCode queryRet = SendAdminReq(query, &resp, sizeof(NdpAdminResponse));
    pfree(query);
    if (queryRet != NdpRetCode::NDP_OK) {
        ereport(LOG, (errmsg("send admin (count:%d) request failed, ip:port %s:%d.",
                             cmdFailed, rpcIp, u_sess->ndp_cxt.ndp_port)));
        return NdpRetCode::RPC_ADMIN_SEND_CTX_FAILED;
    }
    queryId = (uint16)resp.queryId;
    return NdpRetCode::NDP_OK;
}

NdpRetCode NdpScanChannel::SendPlan(NdpScanDesc ndpScan)
{
    NdpAdminRequest* planReq = ConstructPlanReq(ndpScan);
    if (planReq == nullptr) {
        return NdpRetCode::NDP_CONSTRUCT_FAILED;
    }
    NdpAdminResponse resp;
    NdpRetCode ret = SendAdminReq(planReq, &resp, sizeof(resp.ret));
    pfree(planReq);
    if (ret != NdpRetCode::NDP_OK) {
        ret = NdpRetCode::RPC_ADMIN_SEND_PLAN_FAILED;
    }
    return ret;
}

NdpRetCode NdpScanChannel::SendAdminReq(NdpAdminRequest* req, NdpAdminResponse* resp, size_t size)
{
    RpcStatus status = RpcSendAdminReq(req, resp, size, rpcClient);
    if (status != RPC_OK) {
        ereport(LOG, (errmsg("RpcSendAdminReq failed. CMD code:%d, Rpc code: %d", req->head.command, status)));
        return NdpRetCode::RPC_ADMIN_SEND_FAIL;
    } else if (resp->ret != NDP_OK) {
        ereport(LOG, (errmsg("AdminReq handle failed.")));
        return NdpRetCode::NDP_ERROR;
    }

    return NdpRetCode::NDP_OK;
}

NdpRetCode NdpScanChannel::SendState(NdpScanDesc ndpScan)
{
    NdpAdminRequest* state = ConstructPlanState(ndpScan);
    if (state == nullptr) {
        return NdpRetCode::NDP_CONSTRUCT_FAILED;
    }
    NdpAdminResponse resp;
    NdpRetCode ret = SendAdminReq(state, &resp, sizeof(resp.ret));
    pfree(state);
    if (ret != NdpRetCode::NDP_OK) {
        ret = NdpRetCode::RPC_ADMIN_SEND_STATE_FAILED;
    }
    return ret;
}

NdpRetCode NdpScanChannel::SendIo(NdpIoSlot* req, NdpScanDesc ndpScan)
{
    req->SetReq(queryId);
    NdpTableMgr* mgr = &tableMgr[req->GetReqTableId()];
    if (mgr->ioFailed >= SEND_FAILED_LIMIT) {
        return NdpRetCode::RPC_IO_SEND_FAILED;
    }

#ifdef NDP_ASYNC_RPC
    RpcCallDone callDone = {.cb = &NdpIoSlotCallDone, .arg = (void*)req };
    pg_atomic_add_fetch_u32(&ndpScan->reqCount, 1);  // before send
    RpcStatus ret = RpcSendIOReq(req->GetReqMsg(), req->GetRespMsg(), &callDone, rpcClient);
    if (ret != RPC_OK) {
        pg_atomic_sub_fetch_u32(&ndpScan->reqCount, 1);  // before send
        mgr->ioFailed++;
        return NdpRetCode::RPC_IO_SEND_FAILED;
    }

#else
    RpcStatus ret = RpcSendIOReq(req->GetReqMsg(), req->GetRespMsg(), NULL, rpcClient);
    if (ret != RPC_OK) {
        mgr->ioFailed++;
        return NdpRetCode::RPC_IO_SEND_FAILED;
    }
#endif
    return NdpRetCode::NDP_OK;
}

NdpAdminRequest* NdpScanChannel::ConstructPlanReq(NdpScanDesc ndpScan)
{
    if (IsA(ndpScan->cond->plan, Agg)) {
        Agg* agg = (Agg*)ndpScan->cond->plan;
        if (agg->grp_collations == NULL && agg->numCols > 0) {
            agg->grp_collations = (unsigned int*)palloc(sizeof(unsigned int) * agg->numCols);
            for (int i = 0; i < agg->numCols; i++) {
                agg->grp_collations[i] = InvalidOid;
            }
        }
    }

    char* str = nodeToString(ndpScan->cond->plan);
    int len = strlen(str) + 1;
    if (len == 1) {
        return nullptr;
    }

    NdpAdminRequest* req = reinterpret_cast<NdpAdminRequest*>(palloc(sizeof(NdpAdminRequest) + len));
    req->head.command = NDP_PLAN;
    req->head.size = sizeof(NdpAdminRequest) + len;
    req->taskId = queryId;
    req->tableId = ndpScan->cond->tableId;
    errno_t rc = memcpy_s(reinterpret_cast<void*>(req + 1), len, str, len);
    securec_check(rc, "", "");
    return req;
}

bool NdpScanChannel::ExtractTupleDesc(TupleDesc desc, NdpTupleDesc* td)
{
    td->natts = desc->natts;
    if (td->natts == 0) {
        td->attrs = NULL;
        return true;
    }
    td->attrs = (NdpPGAttr *)palloc(sizeof(NdpPGAttr) * td->natts);
    for (int i = 0; i < desc->natts; ++i) {
        td->attrs[i].attlen = desc->attrs[i].attlen;
        td->attrs[i].attbyval = desc->attrs[i].attbyval;
        td->attrs[i].attcacheoff = desc->attrs[i].attcacheoff;
        td->attrs[i].attalign = desc->attrs[i].attalign;
        td->attrs[i].attndims = desc->attrs[i].attndims;
        td->attrs[i].attstorage = desc->attrs[i].attstorage;
    }
    td->tdhasoid = desc->tdhasoid;
    td->tdhasuids = desc->tdhasuids;
    td->tdtypeid = desc->tdtypeid;
    td->tdtypmod = desc->tdtypmod;
    return true;
}

bool NdpScanChannel::ExtractRelation(TableScanDesc scan, NdpRelation* rel)
{
    rel->node.spcNode = scan->rs_rd->rd_node.spcNode;
    rel->node.dbNode = scan->rs_rd->rd_node.dbNode;
    rel->node.relNode = scan->rs_rd->rd_node.relNode;
    rel->node.bucketNode = scan->rs_rd->rd_node.bucketNode;
    rel->node.opt = scan->rs_rd->rd_node.opt;

    return ExtractTupleDesc(scan->rs_rd->rd_att, &rel->att);
}

bool NdpScanChannel::ExtractXact(TableScanDesc scan, NdpXact* xact)
{
    xact->comboCids = NULL;
    xact->CLogPageBuffer = NULL;
    xact->CSNLogPageBuffer = NULL;

    /* snapshot */
    NdpSnapshot* snapshot = &xact->snapshot;

    snapshot->satisfies = scan->rs_snapshot->satisfies;
    snapshot->xmin = scan->rs_snapshot->xmin; /* all XID < xmin are visible to me */
    snapshot->xmax = scan->rs_snapshot->xmax; /* all XID >= xmax are invisible to me */
    snapshot->snapshotcsn = scan->rs_snapshot->snapshotcsn;
    snapshot->curcid = scan->rs_snapshot->curcid;

    /* TransactionState */
    xact->transactionId = GetCurrentTransactionIdIfAny();

    /* comboCids */
    xact->usedComboCids = u_sess->utils_cxt.usedComboCids;
    if (xact->usedComboCids > 0) {
        xact->comboCids = (uint32 *)palloc(xact->usedComboCids * 2 * sizeof(uint32));
        uint32 *comboCids = (uint32 *)u_sess->utils_cxt.comboCids;
        for (int i = 0; i < xact->usedComboCids; i++) {
            xact->comboCids[i * 2] = comboCids[i * 2];
            xact->comboCids[i * 2 + 1] = comboCids[i * 2 + 1];
        }
    } else {
        xact->comboCids = NULL;
    }

    /* clog & csnlog */
    /* It's okay to read latestCompletedXid without acquiring ProcArrayLock shared lock
     * because we dont' care if we get a slightly stale value
     */
    xact->latestCompletedXid = t_thrd.xact_cxt.ShmemVariableCache->latestCompletedXid;
    int64 pagenoStart, pagenoEnd, pageno;

    pagenoStart = TransactionIdToPage(FirstNormalTransactionId);
    pagenoEnd = TransactionIdToPage(xact->latestCompletedXid);

    xact->CLogLen = (pagenoEnd - pagenoStart + 1) * BLCKSZ;
    xact->CLogPageBuffer = (char *)palloc(xact->CLogLen);
    for (pageno = pagenoStart; pageno <= pagenoEnd; pageno++) {
        CopyCLog(pageno, xact->CLogPageBuffer + (pageno - pagenoStart) * BLCKSZ);
    }

    pagenoStart = TransactionIdToCSNPage(xact->latestCompletedXid);
    pagenoEnd = TransactionIdToCSNPage(xact->latestCompletedXid);

    xact->CSNLogLen = (pagenoEnd - pagenoStart + 1) * BLCKSZ;
    xact->CSNLogPageBuffer = (char *)palloc(xact->CSNLogLen);
    for (pageno = pagenoStart; pageno <= pagenoEnd; pageno++) {
        CopyCSNLog(pageno, xact->CSNLogPageBuffer + (pageno - pagenoStart) * BLCKSZ);
    }

    return true;
}

bool NdpScanChannel::ExtractAggState(NdpScanDesc ndpScan, NdpAggState* aggS)
{
    aggS->aggTd.natts = 0;
    aggS->aggTd.attrs = nullptr;
    aggS->aggNum = 0;
    aggS->perAggTd = nullptr;
    aggS->numCols = 0;
    aggS->eqFuncOid = nullptr;
    aggS->hashFuncOid = nullptr;

    if (ndpScan->aggState) {
        Assert(ndpScan->aggSlot != nullptr);
        ExtractTupleDesc(ndpScan->aggSlot->tts_tupleDescriptor, &aggS->aggTd);

        aggS->aggNum = ndpScan->aggState->numaggs;
        aggS->perAggTd = (NdpTupleDesc*)palloc(aggS->aggNum * sizeof(NdpTupleDesc));
        for (int aggNo = 0; aggNo < aggS->aggNum; ++aggNo) {
            ExtractTupleDesc(ndpScan->aggState->evaldesc, &aggS->perAggTd[aggNo]);
        }
        Agg* agg = (Agg*)ndpScan->aggState->ss.ps.plan;
        if ((agg->aggstrategy == AGG_HASHED) && (agg->numCols > 0)) {
            aggS->numCols = agg->numCols;
            aggS->eqFuncOid = (unsigned int*)palloc(aggS->numCols * sizeof(unsigned int));
            aggS->hashFuncOid = (unsigned int*)palloc(aggS->numCols * sizeof(unsigned int));
            for (int colNo = 0; colNo < aggS->numCols; ++colNo) {
                aggS->eqFuncOid[colNo] = ndpScan->aggState->phases[0].eqfunctions[colNo].fn_oid;
                aggS->hashFuncOid[colNo] = ndpScan->aggState->hashfunctions[colNo].fn_oid;
            }
        }
    }
    return true;
}

bool CheckExprContext(ExprContext* econtext, NdpParamList* pList)
{
    ParamListInfo paramInfo = econtext->ecxt_param_list_info;
    if (paramInfo->paramFetch != NULL) {
        return false;
    }
    pList->numParams = paramInfo->numParams;
    pList->params = (NdpParamData*)palloc(pList->numParams * sizeof(NdpParamData));
    for (int i = 0; i < pList->numParams; i++) {
        ParamExternData* from =  &paramInfo->params[i];
        NdpParamData* to = &pList->params[i];
        if (from->tabInfo && from->tabInfo->isnestedtable && plpgsql_estate) {
            pfree(pList->params);
            pList->numParams = 0;
            return false;
        }
        to->isnull = from->isnull;
        to->ptype = from->ptype;
        to->value = from->value;
        get_typlenbyval(to->ptype, &to->typlen, &to->typbyval);
        to->value = datumCopy(to->value, to->typbyval, to->typlen);
    }
    return true;
}

bool ExtractParamList(NdpScanDesc ndpScan, NdpParamList* pList)
{
    ExprContext* econtext = ndpScan->scanState->ps.ps_ExprContext;
    if (econtext == nullptr || econtext->ecxt_param_list_info == nullptr ||
        econtext->ecxt_param_list_info->numParams == 0) {
        pList->numParams = 0;
        pList->params = nullptr;
        return true;
    }
    return CheckExprContext(econtext, pList);
}

bool ExtractKnlSessionContext(NdpSessionContext* sess)
{
    if (u_sess != nullptr) {
        sess->sql_compatibility = u_sess->attr.attr_sql.sql_compatibility;
        sess->behavior_compat_flags = u_sess->utils_cxt.behavior_compat_flags;
        sess->encoding = u_sess->mb_cxt.DatabaseEncoding->encoding;
        return true;
    }
    return false;
}

NdpPlanState* NdpScanChannel::CreatePlanState(NdpScanDesc ndpScan)
{
    NdpPlanState* state = (NdpPlanState*)palloc(sizeof(NdpPlanState));

    TableScanDesc scan = ndpScan->scan;
    ExtractRelation(scan, &state->rel);

    ProjectionInfo* proj_info = ndpScan->scanState->ps.ps_ProjInfo;
    if (proj_info != NULL) {
        ExtractTupleDesc(proj_info->pi_slot->tts_tupleDescriptor, &state->scanTd);
    } else {
        ExtractTupleDesc(ndpScan->scanState->ss_ScanTupleSlot->tts_tupleDescriptor, &state->scanTd);
    }
    if (!ExtractAggState(ndpScan, &state->aggState)) {
        DestroyPlanState(state);
        return nullptr;
    }
    if (!ExtractParamList(ndpScan, &state->paramList)) {
        DestroyPlanState(state);
        return nullptr;
    }
    if (!ExtractKnlSessionContext(&state->sess)) {
        DestroyPlanState(state);
        return nullptr;
    }
    return state;
}

void NdpScanChannel::DestroyPlanState(NdpPlanState* state)
{
    if (state == nullptr) return;
    if (state->rel.att.attrs) pfree(state->rel.att.attrs);
    if (state->scanTd.attrs) pfree(state->scanTd.attrs);
    if (state->paramList.params != nullptr) {
        pfree(state->paramList.params);
    }

    NdpAggState* aggS = &state->aggState;
    if (aggS->aggTd.attrs) pfree(aggS->aggTd.attrs);
    if (aggS->perAggTd) {
        for (int i = 0; i < aggS->aggNum; ++i) {
            if (aggS->perAggTd[i].attrs) pfree(aggS->perAggTd[i].attrs);
        }
        pfree(aggS->perAggTd);
    }
    if (aggS->eqFuncOid) pfree(aggS->eqFuncOid);
    if (aggS->hashFuncOid) pfree(aggS->hashFuncOid);

    pfree(state);
}

NdpAdminRequest* NdpScanChannel::ConstructPlanState(NdpScanDesc ndpScan)
{
    NdpPlanState* state = CreatePlanState(ndpScan);

    if (state == nullptr) {
        return nullptr;
    }

    StringInfoData str;
    initStringInfo(&str);
    NdpAdminRequest head;
    appendBinaryStringInfo(&str, (const char*)&head, (int)sizeof(head));
    stateToString(state, &str);
    NdpAdminRequest* ptr = (NdpAdminRequest*)str.data;
    ptr->head.command = NDP_PLANSTATE;
    ptr->head.size = str.len;
    ptr->taskId = queryId;
    ptr->tableId = ndpScan->cond->tableId;

    DestroyPlanState(state);
    return ptr;
}

NdpAdminRequest* NdpScanChannel::ConstructQuery(NdpScanDesc ndpScan)
{
    NdpQuery* query = (NdpQuery*)palloc(sizeof(NdpQuery));
    NdpContext* context = static_cast<NdpContext*>(ndpScan->cond->ctx);
    query->tableNum = context->tableCount;
    ExtractXact(ndpScan->scan, &query->xact);

    StringInfoData str;
    initStringInfo(&str);
    NdpAdminRequest head;
    appendBinaryStringInfo(&str, (const char*)&head, (int)sizeof(head));
    queryToString(query, &str);
    NdpAdminRequest* ptr = (NdpAdminRequest*)str.data;
    ptr->head.command = NDP_QUERY;
    ptr->head.size = str.len;

    if (query->xact.comboCids) pfree(query->xact.comboCids);
    if (query->xact.CLogPageBuffer) pfree(query->xact.CLogPageBuffer);
    if (query->xact.CSNLogPageBuffer) pfree(query->xact.CSNLogPageBuffer);
    pfree(query);
    return ptr;
}

NdpAdminRequest* NdpScanChannel::ConstructVersion()
{
    int len = sizeof(uint64);
    uint64 version = (((uint64)GRAND_VERSION_NUM) << 32) | NDP_LOCAL_VERSION_NUM;
    NdpAdminRequest* req = reinterpret_cast<NdpAdminRequest*>(palloc(sizeof(NdpAdminRequest) + len));
    req->head.command = NDP_VERSION;
    req->head.size = sizeof(NdpAdminRequest) + len;
    req->taskId = -1;
    req->tableId = -1;
    errno_t rc = memcpy_s(reinterpret_cast<void*>(req + 1), len, &version, len);
    securec_check(rc, "", "");
    return req;
}
