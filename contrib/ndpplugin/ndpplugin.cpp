/* -------------------------------------------------------------------------
 * ndpplugin.cpp
 *	  Routines to support ndp executor smart pushdown
 *
 * Portions Copyright (c) 2022 Huawei Technologies Co.,Ltd.
 *
 * IDENTIFICATION
 *	  contrib/ndpplugin/ndpplugin.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "access/valid.h"
#include "access/tableam.h"
#include "executor/node/nodeAgg.h"
#include "component/rpc/rpc.h"
#include "storage/file/fio_device.h"
#include "storage/smgr/segment_internal.h"
#include "funcapi.h"
#include "ndpplugin.h"
#include "ndp_check.h"
#include "ndpam.h"
#include "storage/ipc.h"

PG_MODULE_MAGIC;
PG_FUNCTION_INFO_V1(ndpplugin_invoke);
PG_FUNCTION_INFO_V1(pushdown_statistics);

#define IS_AU_ALIGNED(start) (!((start) & ((unsigned)PAGE_NUM_PER_AU - 1)))
#define NDP_SCAN_CHANNEL_DEFAULT_MAX 128
#define NDP_SCAN_CEPH_DEFAULT_MAX 128
#define NDP_SCAN_TABLE_DEFAULT_MAX 128

void TransitionFunction(AggState* aggstate,
                        AggStatePerAgg peraggstate,
                        AggStatePerGroup pergroupstate,
                        FunctionCallInfoData* fcinfo);
static void NdpAggSlotAppend(AggState* state, AggStatePerGroup pergroup, TupleTableSlot* slot);
static void knl_u_ndp_init(knl_u_ndp_context* ndp_cxt);
static void NdpReInitContext();

THR_LOCAL ndp_pushdown_hook_type backup_ndp_pushdown_hook_type = NULL;
THR_LOCAL TableAmNdpRoutine_hook_type backup_ndp_tableam = NULL;

THR_LOCAL ExecutorStart_hook_type ndp_hook_ExecutorStart = NULL;
THR_LOCAL ExecutorEnd_hook_type ndp_hook_ExecutorEnd = NULL;
THR_LOCAL bool HOOK_INIT = false;

constexpr int NDP_MAX_AWAIT_REQUEST = 64;
constexpr int NDP_MEMORY_POOL_SIZE = 2048;

NdpInstanceContext g_ndp_instance = {
    .mutex = PTHREAD_MUTEX_INITIALIZER,
    .status = UNINITIALIZED,
    .pageContext = new MpmcBoundedQueue<void *>(NDP_MEMORY_POOL_SIZE)
};

void NdpSharedMemoryAlloc()
{
    Assert(g_ndp_instance.pageContext);
    if (!g_ndp_instance.pageContext) {
        pthread_mutex_unlock(&g_ndp_instance.mutex);
        ereport(ERROR, (errmsg("memory pool haven't been init or already released.")));
    }
    MpmcBoundedQueue<void *>* pageContext = g_ndp_instance.pageContext;
    size_t blockSize = DSS_DEFAULT_AU_SIZE;
    void* ptr = malloc(blockSize * NDP_MEMORY_POOL_SIZE);

    if (!ptr) {
        pthread_mutex_unlock(&g_ndp_instance.mutex);
        ereport(ERROR, (errmsg("ndpplugin try alloc memory failed.")));
    }
    g_ndp_instance.pageContextPtr = ptr;

    uintptr_t ptrval = reinterpret_cast<uintptr_t>(ptr);
    for (int i = 0; i < NDP_MEMORY_POOL_SIZE; ++i) {
        pageContext->Enqueue(reinterpret_cast<void*>(ptrval + (i * blockSize)));
    }
}

/*
 * proc_exit callback to free g_ndp_instance
 */
static void NdpInstanceUninit(int status, Datum arg)
{
    pthread_mutex_lock(&g_ndp_instance.mutex);
    if (g_ndp_instance.pageContextPtr) {
        free(g_ndp_instance.pageContextPtr);
        g_ndp_instance.pageContextPtr = nullptr;
        delete g_ndp_instance.pageContext;
        g_ndp_instance.pageContext = nullptr;
    }
    g_ndp_instance.status = UNINITIALIZED;
    pthread_mutex_unlock(&g_ndp_instance.mutex);
}

void NdpInstanceInit()
{
    if (g_ndp_instance.status == INITIALIZED) {
        return;
    }

#ifndef ENABLE_SSL
    // if not using ssl, use memory pool
    NdpSharedMemoryAlloc();
#endif

    g_ndp_instance.status = INITIALIZED;
    /* PostmasterMain(process_shared_preload_libraries) inits g_ndp_instance first */
    on_proc_exit(NdpInstanceUninit, 0);
}

#define IndexGetBuffer(pages, i) ((char*)(pages) + i * BLCKSZ)
#define NdpTupleOffset(t) ((t)->len + offsetof(NdpTupleHeaderData, tuple))

typedef struct NdpTupleHeaderData {
    uint64 len;
    HeapTupleHeaderData tuple;
} NdpTupleHeaderData;
typedef NdpTupleHeaderData* NdpTupleHeader;

/*
 * return channel if rpc channel is ok, otherwise return null;
 */
NdpScanChannel* NdpScanGetChannel(NdpContext* ctx, char* connIp)
{
    bool found = false;
    NdpScanChannel* channel;

    // no need to think about remove, do it when plan is over
    pthread_rwlock_rdlock(&ctx->ccLock);
    channel = (NdpScanChannel*)hash_search(ctx->channelCache, connIp, HASH_FIND, NULL);
    pthread_rwlock_unlock(&ctx->ccLock);

    if (!channel) {
        MemoryContext old = MemoryContextSwitchTo(ctx->ccMem);
        pthread_rwlock_wrlock(&ctx->ccLock);
        channel = (NdpScanChannel*)hash_search(ctx->channelCache, connIp, HASH_ENTER, &found);
        if (!found) {
            if (!channel->Init(ctx->rpcCount++, connIp, ctx->tableCount)) {
                hash_search(ctx->channelCache, connIp, HASH_REMOVE, NULL);
                channel = nullptr;
            }
        }
        pthread_rwlock_unlock(&ctx->ccLock);
        (void)MemoryContextSwitchTo(old);
    }

    return channel;
}

void NdpScanTryPushDownScan(HeapScanDesc scan, NdpScanDesc ndpScan)
{
    BlockNumber start, end, phyStart;
    int bitCount = 0;
    NdpIoSlot* slot;
    NdpRetCode ret;
    AuInfo auinfo;
    char connIp[NDP_RPC_IP_LEN];

    start = ndpScan->handledBlock;

    pm_get_pageinfo(ndpScan, start, &auinfo.object, connIp, end, phyStart);
    NdpContext* context = static_cast<NdpContext*>(ndpScan->cond->ctx);
    NdpScanChannel* channel = NdpScanGetChannel(context, connIp);
    if (!channel || channel->GetTableStatus(ndpScan->cond->tableId) == NdpTableStatus::CONSTRUCTFAIL) {
        ndpScan->AddToNormal(start, end);
        goto next;
    }

    slot = New(CurrentMemoryContext) NdpIoSlot(ndpScan);
    slot->SetStartBlockNum(start);
    auinfo.phyStartBlockNum = phyStart;
    auinfo.pageNum = end - start;

    bitCount = slot->SetReq(scan->rs_base.rs_rd->rd_smgr->smgr_rnode.node, 0, ndpScan->cond->tableId, auinfo);
    // set a threshold int the future
    if (bitCount == 0) {
        delete slot;
        ndpScan->AddToNormal(start, end);
        goto next;
    }

    /* Numbers of Ndp page may greater than pageNum, because of NdpTupleHeader has len.
     * And sometimes there are too much agg. Fix it in the future and SetResp;
     */
    if (slot->SetResp(bitCount) != NdpRetCode::NDP_OK) {
        delete slot;
        ndpScan->AddToNormal(start, end);
        goto next;
    }

    ret = channel->SendRequest(slot, ndpScan);
    if (ret != NdpRetCode::NDP_OK) {
        delete slot;
        ndpScan->AddToNormal(start, end);
        ndpScan->sendFailedN++;
        ereport(DEBUG2, (errmsg("send request failed, error code %d.",
                                static_cast<int>(ret))));
    } else {
#ifndef NDP_ASYNC_RPC
        if (!ndpScan->HandleSlot(slot)) {
            delete slot;
        }
#endif
        ndpScan->pushDownPageN += bitCount;
        if (ndpScan->scanState->ps.instrument) {
            ndpScan->scanState->ps.instrument->ndp_pushdown_page += bitCount;
        }
    }

next:
    ndpScan->handledBlock = end;
    if (scan->dop > 1
        && ((ndpScan->handledBlock - scan->rs_base.rs_startblock) % PARALLEL_SCAN_GAP_AU_ALIGNED == 0)) {
        ndpScan->handledBlock += (scan->dop - 1) * PARALLEL_SCAN_GAP_AU_ALIGNED;
    }
}

static bool NdpScanGetPageIO(NdpScanDesc ndpScan)
{
    for (;;) {
        if (ndpScan->nextNdpPageIndex < ndpScan->curNdpPagesNum) {
            ndpScan->curNdpPage = (NdpPageHeader)IndexGetBuffer(ndpScan->curNdpPages, ndpScan->nextNdpPageIndex);
            Assert(((uintptr_t)ndpScan->curNdpPage & 0x7) == 0);
            ndpScan->nextNdpPageIndex++;

            ndpScan->curPageType = ndpScan->curNdpPage->pd_flags;

            ndpScan->curLinesNum = PageGetMaxOffsetNumber((Page)(ndpScan->curNdpPage));
            ndpScan->nextLineIndex = 0;
            return true;
        } else {
            // all Ndp page has been handled, can be free
            ndpScan->FreeCurSlot();

            // get next rpc page list
#ifdef NDP_ASYNC_RPC
            if (!ndpScan->GetNextSlot()) {
                return false;
            }
#else
            return false;
#endif
        }
    }
}

static bool NdpScanGetPageLocal(NdpScanDesc ndpScan)
{
#ifdef NDP_ASYNC_RPC
    if (ndpScan->normalPagesId->Dequeue(ndpScan->curNormalPageId)) {
#else
    if (ndpScan->normalPagesNum) {
        ndpScan->curNormalPageId = ndpScan->normalPagesId[ndpScan->normalPagesNum - 1];
        ndpScan->normalPagesNum--;
#endif
        ndpScan->curPageType = NORMAL_PAGE;

        heapgetpage(ndpScan->scan, ndpScan->curNormalPageId);

        ndpScan->curLinesNum = ndpScan->scan->rs_ntuples;
        ndpScan->nextLineIndex = 0;
        return true;
    }
    return false;
}

static bool NdpScanGetPageQueue(NdpScanDesc ndpScan)
{
    /*
     * If get page from IO queue first, and most pages of IO was failed,
     * this pages will add to normal queue then will cause normal queue full quickly.
     * Get page from normal queue first.
     * If IO fail a lot but get page from normal first, plugin will not send IO request at a period of time.
     */
    // from normal page
    // normalPagesId depends on NdpIO(resp)
    if (NdpScanGetPageLocal(ndpScan)) {
        return true;
    }
    // from Ndp page
    if (NdpScanGetPageIO(ndpScan)) {
        return true;
    }
    return false;
}

// return false if finished
static bool NdpScanGetPage(NdpScanDesc ndpScan)
{
    for (;;) {
        bool found = NdpScanGetPageQueue(ndpScan);
        if (found) {
            return true;
        }

#ifdef NDP_ASYNC_RPC
        uint32 req = pg_atomic_read_u32(&ndpScan->reqCount);
        uint32 resp = pg_atomic_read_u32(&ndpScan->respCount);
        Assert(req >= resp);

        if (ndpScan->handledBlock < ndpScan->nBlock) {
            if ((req - resp) >= NDP_MAX_AWAIT_REQUEST) {
                pg_usleep(NDP_RPC_WAIT_USEC);
            } else {
                NdpScanTryPushDownScan((HeapScanDesc)ndpScan->scan, ndpScan);
            }
            continue;
        }

        // wait request
        if (resp < req) {
            pg_usleep(NDP_RPC_WAIT_USEC);
        // if normal page finish, io request failed, pages been added to normal queue, can't return directly.
        } else if (ndpScan->normalPagesId->Empty() && ndpScan->respIO->Empty()) {
            return false;
        }
#else
        if (ndpScan->handledBlock < ndpScan->nBlock) {
            NdpScanTryPushDownScan((HeapScanDesc)ndpScan->scan, ndpScan);
        } else {
            return false;
        }
#endif
    }
}

static void NdpScanHandleFilteredTuple(ScanState* scanState, HeapTuple tuple)
{
    ProjectionInfo* proj_info = scanState->ps.ps_ProjInfo;
    if (proj_info) {
        heap_slot_store_heap_tuple(tuple, proj_info->pi_slot,
                                   InvalidBuffer, false, false);
        tableam_tslot_getsomeattrs(proj_info->pi_slot, proj_info->pi_slot->tts_tupleDescriptor->natts);
        proj_info->pi_slot->tts_flags &= ~TTS_FLAG_EMPTY;
    }
}

static void initialize_aggregate(AggState* aggstate, AggStatePerAgg peraggstate, AggStatePerGroup pergroupstate)
{
    Plan* plan = aggstate->ss.ps.plan;
    int64 local_work_mem = SET_NODEMEM(plan->operatorMemKB[0], plan->dop);
    int64 max_mem = (plan->operatorMaxMem > 0) ? SET_NODEMEM(plan->operatorMaxMem, plan->dop) : 0;

    if (peraggstate->numSortCols > 0) {
        /*
         * In case of rescan, maybe there could be an uncompleted sort
         * operation?  Clean it up if so.
         */
        if (peraggstate->sortstates[aggstate->current_set])
            tuplesort_end(peraggstate->sortstates[aggstate->current_set]);

        if (peraggstate->numInputs == 1) {
            peraggstate->sortstates[aggstate->current_set] =
                    tuplesort_begin_datum(peraggstate->evaldesc->attrs[0].atttypid,
                                          peraggstate->sortOperators[0],
                                          peraggstate->sortCollations[0],
                                          peraggstate->sortNullsFirst[0],
                                          local_work_mem,
                                          false);
        } else {
            peraggstate->sortstates[aggstate->current_set] =
                    tuplesort_begin_heap(peraggstate->evaldesc,
                                         peraggstate->numSortCols,
                                         peraggstate->sortColIdx,
                                         peraggstate->sortOperators,
                                         peraggstate->sortCollations,
                                         peraggstate->sortNullsFirst,
                                         local_work_mem,
                                         false,
                                         max_mem,
                                         plan->plan_node_id,
                                         SET_DOP(plan->dop));
        }
    }

    /*
     * (Re)set transValue to the initial value.
     *
     * Note that when the initial value is pass-by-ref, we must copy it
     * (into the aggcontext) since we will pfree the transValue later.
     */
    if (peraggstate->initValueIsNull) {
        pergroupstate->transValue = peraggstate->initValue;
    } else {
        pergroupstate->transValue =
                datumCopy(peraggstate->initValue, peraggstate->transtypeByVal, peraggstate->transtypeLen);
    }
    pergroupstate->transValueIsNull = peraggstate->initValueIsNull;

    pergroupstate->noTransValue = peraggstate->initValueIsNull;

    /*
     * (Re)set collectValue to the initial value.
     *
     * Note that when the initial value is pass-by-ref, we must copy it
     * (into the aggcontext) since we will pfree the collectValue later.
     * collection type is same as transition type.
     */
    if (peraggstate->initCollectValueIsNull) {
        pergroupstate->collectValue = peraggstate->initCollectValue;
    } else {
        pergroupstate->collectValue =
                datumCopy(peraggstate->initCollectValue, peraggstate->transtypeByVal, peraggstate->transtypeLen);
    }
    pergroupstate->collectValueIsNull = peraggstate->initCollectValueIsNull;

    pergroupstate->noCollectValue = peraggstate->initCollectValueIsNull;
}

static void initialize_aggregates(AggState* aggstate, AggStatePerAgg peragg, AggStatePerGroup pergroup)
{
    int numReset = Max(aggstate->phase->numsets, 1);

    for (int aggno = 0; aggno < aggstate->numaggs; aggno++) {
        AggStatePerAgg peraggstate = &peragg[aggno];

        for (int setno = 0; setno < numReset; setno++) {
            AggStatePerGroup pergroupstate = &pergroup[aggno + (setno * (aggstate->numaggs))];

            aggstate->current_set = setno;

            initialize_aggregate(aggstate, peraggstate, pergroupstate);
        }
    }
}

/**
 * look for a hash entry
 * @param state agg state of current plan
 * @param slot slot load from backend
 * @return found hash entry
 */
static AggHashEntry LookForHashEntry(AggState* state, TupleTableSlot* slot)
{
    TupleTableSlot* hashslot = state->hashslot;
    ListCell* l = NULL;
    AggHashEntry entry;
    bool isnew = false;
    AggWriteFileControl* TempFileControl = (AggWriteFileControl*)state->aggTempFileControl;

    if (hashslot->tts_tupleDescriptor == NULL) {
        ExecSetSlotDescriptor(hashslot, state->ss.ss_ScanTupleSlot->tts_tupleDescriptor);
        // Make sure all unused columns are NULLs
        ExecStoreAllNullTuple(hashslot);
    }

    // init hash slot
    tableam_tslot_getsomeattrs(slot, linitial_int(state->hash_needed));
    int counter = slot->tts_nvalid - 1;
    foreach (l, state->hash_needed) {
        int varNumber = lfirst_int(l) - 1;

        hashslot->tts_values[varNumber] = slot->tts_values[counter];
        hashslot->tts_isnull[varNumber] = slot->tts_isnull[counter];
        counter--;
    }

    if (TempFileControl->spillToDisk == false || TempFileControl->finishwrite == true) {
        entry = (AggHashEntry)LookupTupleHashEntry(state->hashtable, hashslot, &isnew, true);
    } else {
        entry = (AggHashEntry)LookupTupleHashEntry(state->hashtable, hashslot, &isnew, false);
    }

    if (!isnew) {
        if (((Agg *)state->ss.ps.plan)->unique_check) {
            ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NODE_STATE), errmsg("find a duplicate plan")));
        }
        return entry;
    }

    // is a new entry
    if (entry) {
        initialize_aggregates(state, state->peragg, entry->pergroup);
        agg_spill_to_disk(TempFileControl, state->hashtable, state->hashslot,
                          ((Agg*)state->ss.ps.plan)->numGroups, true, state->ss.ps.plan->plan_node_id,
                          SET_DOP(state->ss.ps.plan->dop), state->ss.ps.instrument);

        if (TempFileControl->filesource && state->ss.ps.instrument) {
            TempFileControl->filesource->m_spill_size = &state->ss.ps.instrument->sorthashinfo.spill_size;
        }
    } else {
        // find a new entry, but memory is not enough, write tuple to temp file
        Assert(TempFileControl->spillToDisk == true && TempFileControl->finishwrite == false);
        MinimalTuple tuple = ExecFetchSlotMinimalTuple(slot);
        /*
         * Here need switch memorycontext to ecxt_per_tuple_memory, so memory which be applyed in function
         * ComputeHashValue is freed.
         */
        MemoryContext oldContext = MemoryContextSwitchTo(state->tmpcontext->ecxt_per_tuple_memory);
        uint32 hashvalue = ComputeHashValue(state->hashtable);
        MemoryContextSwitchTo(oldContext);
        TempFileControl->filesource->writeTup(tuple, hashvalue & (TempFileControl->filenum - 1));
    }

    return entry;
}

static void NdpHashAgg(AggState* state, TupleTableSlot* slot)
{
    AggHashEntry entry = LookForHashEntry(state, slot);

    if (entry != NULL) {
        // accumulate slot to tuple
        NdpAggSlotAppend(state, entry->pergroup, slot);
    }
}

static void NdpAggSlotAppend(AggState* state, AggStatePerGroup pergroup, TupleTableSlot* slot)
{
    int numGroupingSets = Max(state->phase->numsets, 1);
    int numAggs = state->numaggs;

    int aggno;
    int setno = 0;
    int counter = 0;

    for (aggno = 0; aggno < state->numaggs; aggno++) {
        AggStatePerAgg peraggstate = &state->peragg[aggno];

        AggStatePerGroup pergroupstate = &pergroup[aggno];

        if (pergroupstate->transValueIsNull) {
            if (((Agg*)(state->ss.ps.plan))->aggstrategy == AGG_PLAIN) {
                peraggstate->initValue =
                        datumCopy(slot->tts_values[counter], peraggstate->transtypeByVal, peraggstate->transtypeLen);
                peraggstate->initValueIsNull = slot->tts_isnull[counter];

                pergroupstate->transValue = peraggstate->initValue;
                pergroupstate->transValueIsNull = slot->tts_isnull[counter];
                pergroupstate->noTransValue = false;

                counter++;
                continue;
            }
        }

        FunctionCallInfoData fcinfo;

        // init the number of arguments to a function.
        // fn_nargs = 2, since we only have two inputs, 0th is transvalue, 1th is resultvalue from backend
        InitFunctionCallInfoArgs(fcinfo, 2, 1);

        // add slot value to fcinfo
        fcinfo.arg[1] = slot->tts_values[counter];
        fcinfo.argnull[1] = slot->tts_isnull[counter];
        fcinfo.argTypes[1] = InvalidOid;
        counter++;

        // normally numGroupingSets = 1
        for (setno = 0; setno < numGroupingSets; setno++) {
            AggStatePerGroup pergroupstate = &pergroup[aggno + (setno * numAggs)];
            state->current_set = setno;

            TransitionFunction(state, peraggstate, pergroupstate, &fcinfo);
        }
    }
}

/*
 * check and fill transition value, then call the function
 * */
void TransitionFunction(AggState* aggstate,
                        AggStatePerAgg peraggstate,
                        AggStatePerGroup pergroupstate,
                        FunctionCallInfoData* fcinfo)
{
    Datum newVal;
    if (peraggstate->transfn.fn_strict) {
        /*
         * For a strict transfn, nothing happens when there's a NULL input; we
         * just keep the prior transValue.
         */
        for (int i = 1; i <= peraggstate->numTransInputs; i++) {
            if (fcinfo->argnull[i])
                return;
        }
        if (pergroupstate->noTransValue) {
            /*
             * transValue has not been initialized. This is the first non-NULL
             * input value. We use it as the initial value for transValue. (We
             * already checked that the agg's input type is binary-compatible
             * with its transtype, so straight copy here is OK.)
             *
             * We must copy the datum into aggcontext if it is pass-by-ref. We
             * do not need to pfree the old transValue, since it's NULL.
             */
            pergroupstate->transValue =
                    datumCopy(fcinfo->arg[1], peraggstate->transtypeByVal, peraggstate->transtypeLen);
            pergroupstate->transValueIsNull = false;
            pergroupstate->noTransValue = false;
            return;
        }
        if (pergroupstate->transValueIsNull) {
            /*
             * Don't call a strict function with NULL inputs.  Note it is
             * possible to get here despite the above tests, if the transfn is
             * strict *and* returned a NULL on a prior cycle. If that happens
             * we will propagate the NULL all the way to the end.
             */
            return;
        }
    }

    // set up aggstate->curperagg to allow get aggref
    aggstate->curperagg = peraggstate;

    /*
     * OK to call the collection function
     * fn_nargs = 2, since we only have two inputs, 0th is transvalue, 1th is resultvalue from backend
     */
    InitFunctionCallInfoData(
            *fcinfo, &(peraggstate->collectfn), 2, peraggstate->aggCollation, (Node*)aggstate, NULL);
    fcinfo->arg[0] = pergroupstate->transValue;
    fcinfo->argnull[0] = pergroupstate->transValueIsNull;
    fcinfo->argTypes[0] = InvalidOid;
    fcinfo->isnull = false; /* just in case transfn doesn't set it */

    Node* origin_fcxt = fcinfo->context;
    if (peraggstate->is_avg) {
        Node* fcontext = (Node*)palloc0(sizeof(Node));
#ifdef FAULT_INJECT
        if ((rand() % PERCENTAGE_DIV) < PERCENTAGE) {
            ereport(ERROR, (errmsg("Fault inject -- palloc fail")));
        }
#endif
        fcontext->type = (NodeTag)(peraggstate->is_avg);
        fcinfo->context = fcontext;
    }
    newVal = FunctionCallInvoke(fcinfo);
    aggstate->curperagg = NULL;
    fcinfo->context = origin_fcxt;

    /*
     * If pass-by-ref datatype, must copy the new value into aggcontext and
     * pfree the prior transValue.	But if transfn returned a pointer to its
     * first input, we don't need to do anything.
     */
    if (!peraggstate->transtypeByVal && DatumGetPointer(newVal) != DatumGetPointer(pergroupstate->transValue)) {
        if (!fcinfo->isnull) {
            newVal = datumCopy(newVal, peraggstate->transtypeByVal, peraggstate->transtypeLen);
        }
        if (!pergroupstate->transValueIsNull)
            pfree(DatumGetPointer(pergroupstate->transValue));
    }

    if (((Agg*)(aggstate->ss.ps.plan))->aggstrategy == AGG_PLAIN) {
        peraggstate->initValue = newVal;
        peraggstate->initValueIsNull = fcinfo->isnull;
    }

    pergroupstate->transValue = newVal;
    pergroupstate->transValueIsNull = fcinfo->isnull;
    pergroupstate->noTransValue = pergroupstate->transValueIsNull;
}

static TupleDesc NdpAggTupleDescCreate(AggState* aggState)
{
    Assert(aggState->ss.ps.plan->type == T_Agg);
    Agg* agg = (Agg*)aggState->ss.ps.plan;
    int len = aggState->numaggs + agg->numCols;
    TupleDesc typeInfo = CreateTemplateTupleDesc(len, false, TableAmHeap);
    int curResno = 1;

    for (int aggno = 0; aggno < aggState->numaggs; ++aggno) {
        AggStatePerAgg perAgg = &aggState->peragg[aggno];

        // we don't rely on Aggref::aggtrantype, which is defined in PGXC
        Oid aggTransType = ((FuncExpr*)perAgg->transfn.fn_expr)->funcresulttype;
        int32 typmod = -1;
        int attdim = 0;
        Oid collationid = 0;

        // get from pg_type
        HeapTuple tp;
        tp = SearchSysCache1(TYPEOID, ObjectIdGetDatum(aggTransType));
        if (HeapTupleIsValid(tp)) {
            Form_pg_type typtup = (Form_pg_type)GETSTRUCT(tp);
            typmod = typtup->typtypmod;
            attdim = typtup->typndims;
            collationid = typtup->typcollation;
            ReleaseSysCache(tp);
        }

        TupleDescInitEntry(typeInfo, curResno, NULL, aggTransType, typmod, attdim);
        TupleDescInitEntryCollation(typeInfo, curResno, collationid);

        curResno++;
    }

    for (int i = 0; i < agg->numCols; ++i) {
        AttrNumber att = agg->grpColIdx[i];
        Node* node = (Node*)list_nth(agg->plan.lefttree->targetlist, att - 1);
        Assert(node->type == T_TargetEntry);
        Node* expr = (Node*)(((TargetEntry*)node)->expr);

        TupleDescInitEntry(typeInfo, curResno, ((TargetEntry*)node)->resname,
                           exprType(expr), exprTypmod(expr), 0);
        TupleDescInitEntryCollation(typeInfo, curResno, exprCollation(expr));

        curResno++;
    }

    return typeInfo;
}

static void NdpScanHandleAggTuple(AggState* aggState, TupleTableSlot* slot, HeapTuple tuple)
{
    if (aggState == NULL) {
        ereport(WARNING, (errmsg("Can't happen, ndp page flag is wrong!")));
        return;
    }
    heap_slot_store_heap_tuple(tuple, slot, InvalidBuffer, false, false);
    tableam_tslot_getsomeattrs(slot, slot->tts_tupleDescriptor->natts);  // read tuple

    if (((Agg*)aggState->ss.ps.plan)->aggstrategy == AGG_HASHED) {
        NdpHashAgg(aggState, slot);
    } else {
        NdpAggSlotAppend(aggState, aggState->pergroup, slot);
    }
}

static inline bool NdpScanCheckKey(HeapScanDesc scan)
{
    HeapTuple tuple = &(scan->rs_ctup);
    int nkeys = scan->rs_base.rs_nkeys;
    ScanKey key = scan->rs_base.rs_key;

    if (key != NULL) {
        bool valid = false;
        HeapKeyTest(tuple, (scan->rs_tupdesc), nkeys, key, valid);
        if (valid) {
            return true;
        }
    } else {
        return true;
    }
    return false;
}

static bool NdpScanGetTupleFromStocked(HeapScanDesc scan, NdpScanDesc ndpScan)
{
    HeapTuple tuple = &(scan->rs_ctup);

    while (ndpScan->nextLineIndex < ndpScan->curLinesNum) {
        int curLineIndex = ndpScan->nextLineIndex;
        ndpScan->nextLineIndex++;
        Assert(ndpScan->curPageType != INVALID_PAGE);
        if (ndpScan->curPageType == NORMAL_PAGE) {
            BlockNumber page = ndpScan->curNormalPageId;
            Page dp = (Page)BufferGetPage(scan->rs_base.rs_cbuf);
            OffsetNumber line_off = scan->rs_base.rs_vistuples[curLineIndex];
            ItemId lpp = HeapPageGetItemId(dp, line_off);
            Assert(ItemIdIsNormal(lpp));

            // set tuple
            tuple->t_data = (HeapTupleHeader)PageGetItem((Page)dp, lpp);
            tuple->t_len = ItemIdGetLength(lpp);
            ItemPointerSet(&(tuple->t_self), page, line_off);
            HeapTupleCopyBaseFromPage(tuple, dp);
            if (NdpScanCheckKey(scan)) {
                scan->rs_base.rs_cindex = curLineIndex;
                return true;
            }
        } else {
            ItemId lpp = PageGetItemId((Page)ndpScan->curNdpPage, curLineIndex + 1);
            HeapTupleHeader pushDownTuple = (HeapTupleHeader)((char*)ndpScan->curNdpPage + lpp->lp_off);

            tuple->t_data = pushDownTuple;
            tuple->t_len = (uint32)lpp->lp_len;

            if (ndpScan->curPageType == NDP_FILTERED_PAGE) {
                if (NdpScanCheckKey(scan)) {
                    scan->rs_base.rs_cindex = curLineIndex;
                    NdpScanHandleFilteredTuple(ndpScan->scanState, tuple);
                    return true;
                }
            } else if (ndpScan->curPageType == NDP_AGG_PAGE) {
                NdpScanHandleAggTuple(ndpScan->aggState, ndpScan->aggSlot, tuple);
            }
        }
    }
    return false;
}

static void NdpScanGetCachedTuple(HeapScanDesc scan, NdpScanDesc ndpScan)
{
    CHECK_FOR_INTERRUPTS();
    for(;;) {
        // 1. scan stocked page
        if (NdpScanGetTupleFromStocked(scan, ndpScan)) {
            return;
        }

        // 2. get new page
        if (!NdpScanGetPage(ndpScan)) {
            // free buffer
            if (BufferIsValid(scan->rs_base.rs_cbuf)) {
                ReleaseBuffer(scan->rs_base.rs_cbuf);
            }
            ndpScan->FreeCurSlot();
            scan->rs_base.rs_cbuf = InvalidBuffer;
            scan->rs_base.rs_cblock = InvalidBlockNumber;
            scan->rs_base.rs_inited = false;
            scan->rs_ctup.t_data = NULL;
            return;
        }
    }
}

Tuple NdpScanGetTuple(TableScanDesc sscan, ScanDirection dir, TupleTableSlot* slot)
{
    HeapScanDesc scan = (HeapScanDesc)sscan;
    HeapTuple tuple = &(scan->rs_ctup);

    Assert(ScanDirectionIsForward(dir));

    NdpScanDesc ndpScan = (NdpScanDesc)sscan->ndp_ctx;

    MemoryContext oldMct = MemoryContextSwitchTo(ndpScan->memCtx);

    if (!scan->rs_base.rs_inited) {
        if (scan->rs_base.rs_nblocks == 0) {
            Assert(!BufferIsValid(scan->rs_base.rs_cbuf));
            tuple->t_data = NULL;
            goto out;
        }

        // doesn't support rs_parallel and rs_syncscan
        if (scan->rs_parallel != NULL || scan->rs_base.rs_syncscan) {
            ereport(WARNING, (errmsg("parallel not support %p, syncscan not support %d in NDP scene.",
                                     scan->rs_parallel, scan->rs_base.rs_syncscan)));
        }

        // init NdpScanDesc, rs_startblock must AU aligned in begin_scan
        Assert(IS_AU_ALIGNED(scan->rs_base.rs_startblock));
        ndpScan->handledBlock = scan->rs_base.rs_startblock;
        ndpScan->nBlock = scan->rs_base.rs_nblocks;
        ndpScan->curPageType = INVALID_PAGE;
        ndpScan->curIO = nullptr;

        ndpScan->curLinesNum = 0;
        ndpScan->nextLineIndex = 0;
        ndpScan->curNdpPagesNum = 0;
        ndpScan->nextNdpPageIndex = 0;

#ifndef NDP_ASYNC_RPC
        ndpScan->normalPagesNum = 0;
#endif
        scan->rs_base.rs_inited = true;
    }

    PG_TRY();
    {
        NdpScanGetCachedTuple(scan, ndpScan);
    }
    PG_CATCH();
    {
        delete ndpScan;
        sscan->ndp_ctx = nullptr;
        PG_RE_THROW();
    }
    PG_END_TRY();

    out:
    (void)MemoryContextSwitchTo(oldMct);

    if (scan->rs_ctup.t_data == NULL) {
        ereport(DEBUG2, (errmsg("heap_getnext returning EOS")));
        return NULL;  // Upper doesn't judge t_data, so tuple must return NULL if t_data is NULL.
    }
    return tuple;
}

void NdpScanParallelInit(TableScanDesc sscan, int32 dop, ScanDirection dir)
{
    HeapScanDesc scan = (HeapScanDesc) sscan;

    Assert(!ScanDirectionIsBackward(dir));
    if (!scan || scan->rs_base.rs_nblocks == 0) {
        return;
    }
    if (dop <= 1) {
        return;
    }
    scan->dop = dop;

    uint32 paral_blocks = u_sess->stream_cxt.smp_id * PARALLEL_SCAN_GAP_AU_ALIGNED;
    /* If not enough pages to divide into every worker. */
    if (scan->rs_base.rs_nblocks <= paral_blocks) {
        scan->rs_base.rs_startblock = 0;
        scan->rs_base.rs_nblocks = 0;
        return;
    }
    scan->rs_base.rs_startblock = paral_blocks;
}

// check state after planstate inited
void CheckAndSetNdpScan(Relation relation, Snapshot snapshot, ScanState* sstate, TableScanDesc desc)
{
    if (relation->rd_tam_ops != TableAmHeap) return;  // only support astore currently
    if (IsSystemRelation(relation) || IsCatalogRelation(relation) ||
        IsToastRelation(relation) || RelationIsToast(relation) ||
        isAnyTempNamespace(RelationGetNamespace(relation)) || RELATION_IS_TEMP(relation) ||
        RelationGetRelPersistence(relation) == RELPERSISTENCE_UNLOGGED) return;
    if (RowRelationIsCompressed(relation)) return;
    if (relation->is_compressed) return;

    // check TableScanDesc
    if (desc->rs_snapshot->satisfies != SNAPSHOT_MVCC) {
        return;
    }
    if (!desc->rs_pageatatime) return;
    if (desc->rs_nblocks < (unsigned int)u_sess->ndp_cxt.pushdown_min_blocks) return;
    HeapScanDesc scan = (HeapScanDesc)desc;
    if (scan->rs_parallel != nullptr) {
        ereport(NOTICE, (errmsg("parallel are not supported in NDP scene")));
        return;
    }

    // recheck
    if (sstate->ps.plan->ndp_pushdown_condition == nullptr) {
        ereport(WARNING, (errmsg("Ndp condition should not be NULL")));
        return;
    }

    NdpScanDesc ndpScanDesc = New(CurrentMemoryContext) NdpScanDescData;
    NdpRetCode ret = ndpScanDesc->Init(sstate, desc);
    if (ret != NdpRetCode::NDP_OK) {
        delete ndpScanDesc;
        ereport(ERROR, (errmsg("NdpScanDesc init failed, code %d", static_cast<int>(ret))));
        return;
    }

    desc->ndp_pushdown_optimized = true;
    desc->ndp_ctx = ndpScanDesc;
    desc->rs_syncscan = false;
}

TableScanDesc hook_ndp_beginscan(Relation relation, Snapshot snapshot, int nkeys, ScanKey key,
                                 ScanState* sstate, RangeScanInRedis rangeScanInRedis)
{
    TableScanDesc scanDesc = tableam_scan_begin(relation, snapshot, nkeys, key, rangeScanInRedis);
    if (scanDesc) {
        CheckAndSetNdpScan(relation, snapshot, sstate, scanDesc);
    }
    return scanDesc;
}

void hook_ndp_init_parallel(TableScanDesc sscan, int32 dop, ScanDirection dir)
{
    if (!ScanDirectionIsBackward(dir)) {
        return NdpScanParallelInit(sscan, dop, dir);
    } else {
        return tableam_scan_init_parallel_seqscan(sscan, dop, dir);
    }
}

void hook_ndp_rescan(TableScanDesc sscan, ScanKey key)
{
    tableam_scan_rescan(sscan, key);
    sscan->rs_syncscan = false;
    sscan->ndp_pushdown_optimized = true;
    NdpScanDesc ndpScan = (NdpScanDesc)sscan->ndp_ctx;
    if (ndpScan) {
        ndpScan->Reset();
    }
}
static void SendTerminate(NdpContext* context)
{
    if (context == nullptr) {
        return;
    }
    HASH_SEQ_STATUS status;
    NdpScanChannel* channel;

    // notify rpc server to release query resource
    hash_seq_init(&status, context->channelCache);
    while ((channel = (NdpScanChannel*)hash_seq_search(&status)) != nullptr) {
        NdpRetCode retCode = channel->SendEnd();
        if (retCode != NdpRetCode::NDP_OK) {
            ereport(DEBUG2, (errmsg("SendEnd %s fail code[%d].", channel->rpcIp, static_cast<int>(retCode))));
        }
        channel->DestroyChannel();
        hash_search(context->channelCache, channel->rpcIp, HASH_REMOVE, NULL);
    }
}

void NdpDestroyContext(NdpContext* context)
{
    if (context == nullptr) {
        return;
    }
    hash_destroy(context->channelCache);
    context->channelCache = nullptr;
}

static void NdpReInitContext()
{
    if (u_sess->ndp_cxt.cxt == nullptr) {
        return;
    }
    NdpContext* context = (NdpContext*)u_sess->ndp_cxt.cxt;
    SendTerminate(context);
    NdpDestroyContext(context);

    MemoryContext oldContext = MemoryContextSwitchTo(u_sess->ndp_cxt.mem_cxt);
    context->rpcCount = 0;
    context->tableCount = 0;
    context->u_sess = u_sess;
    HASHCTL ctlConn;
    ctlConn.keysize = NDP_RPC_IP_LEN;
    ctlConn.entrysize = sizeof(NdpScanChannel);
    ctlConn.hash = string_hash;
    context->channelCache = hash_create("Ndp Connector to IPC Channel",
                                        NDP_SCAN_CHANNEL_DEFAULT_MAX, &ctlConn,
                                        HASH_ELEM | HASH_FUNCTION);
    if (context->channelCache == nullptr) {
        pfree(context);
        u_sess->ndp_cxt.cxt = nullptr;
    }
    MemoryContextSwitchTo(oldContext);
}

void hook_ndp_endscan(TableScanDesc sscan)
{
    NdpScanDesc ndpScan = (NdpScanDesc)sscan->ndp_ctx;
    if (ndpScan == nullptr || !sscan->ndp_pushdown_optimized || ndpScan->cond == nullptr) {
        return tableam_scan_end(sscan);
    }
    NdpContext* context = static_cast<NdpContext*>(ndpScan->cond->ctx);
    if (context == nullptr || context->u_sess == nullptr) {
        delete ndpScan;
        return tableam_scan_end(sscan);
    }
    knl_session_context* sess = context->u_sess;
    __atomic_add_fetch(&sess->ndp_cxt.stats->sendFailed, ndpScan->sendFailedN, __ATOMIC_RELAXED);
    __atomic_add_fetch(&sess->ndp_cxt.stats->failedIO, ndpScan->failedIoN, __ATOMIC_RELAXED);
    __atomic_add_fetch(&sess->ndp_cxt.stats->pushDownPage, ndpScan->pushDownPageN, __ATOMIC_RELAXED);
    __atomic_add_fetch(&sess->ndp_cxt.stats->sendBackPage, ndpScan->sendBackPageN, __ATOMIC_RELAXED);
    __atomic_add_fetch(&sess->ndp_cxt.stats->ndpPageAgg, ndpScan->ndpPageAggN, __ATOMIC_RELAXED);
    __atomic_add_fetch(&sess->ndp_cxt.stats->ndpPageScan, ndpScan->ndpPageScanN, __ATOMIC_RELAXED);
    if (!StreamThreadAmI()) {
        __atomic_add_fetch(&sess->ndp_cxt.stats->queryCounter, 1, __ATOMIC_RELAXED);
    }
    delete ndpScan;
    sscan->ndp_ctx = NULL;

    return tableam_scan_end(sscan);
}

Tuple hook_ndp_getnexttuple(TableScanDesc sscan, ScanDirection direction, TupleTableSlot* slot)
{
    if (ScanDirectionIsForward(direction)) {
        return NdpScanGetTuple(sscan, direction, slot);
    } else {
        return heap_getnext(sscan, direction);
    }
}

void hook_ndp_handle_hashaggtuple(AggState* aggstate, HeapTupleData *tts_minhdr)
{
    TupleTableSlot *aggSlot = (TupleTableSlot *)aggstate->ndp_slot;
    if (aggSlot) {
        NdpScanHandleAggTuple(aggstate, aggSlot, tts_minhdr);
    }
}

// shared by multi thread
static TableAmNdpRoutine_hook ndp_tableam_apply = {
    .scan_begin = hook_ndp_beginscan,
    .scan_init_parallel_seqscan = hook_ndp_init_parallel,
    .scan_rescan = hook_ndp_rescan,
    .scan_end = hook_ndp_endscan,
    .scan_getnexttuple = hook_ndp_getnexttuple,
    .handle_hashaggslot = hook_ndp_handle_hashaggtuple
};

void ndpplugin_invoke(void)
{
    ereport(DEBUG2, (errmsg("dummy function to let process load this library.")));
    return;
}

NdpScanCondition* NdpCreateScanCondition(Plan* node)
{
    NdpScanCondition* cond = makeNode(NdpScanCondition);
    cond->plan = node;
    return cond;
}

void NdpDestroyScanCondition(NdpScanCondition* cond)
{
    if (!cond) {
        return;
    }
    pfree((void*)cond);
}

NdpContext* NdpCreateContext()
{
    NdpContext* context = (NdpContext*)palloc(sizeof(NdpContext));
#ifdef FAULT_INJECT
    if ((rand() % PERCENTAGE_DIV) < PERCENTAGE) {
        ereport(ERROR, (errmsg("Fault inject -- palloc fail")));
    }
#endif
    pthread_rwlock_init(&context->ccLock, NULL);

    context->ccMem = CurrentMemoryContext;
    HASHCTL ctlConn;
    ctlConn.keysize = NDP_RPC_IP_LEN;
    ctlConn.entrysize = sizeof(NdpScanChannel);
    ctlConn.hash = string_hash;
    context->channelCache = hash_create("Ndp Connector to IPC Channel",
                                        NDP_SCAN_CHANNEL_DEFAULT_MAX, &ctlConn,
                                        HASH_ELEM | HASH_FUNCTION);
    if (context->channelCache == NULL) {
        pfree(context);
        return NULL;
    }

    context->rpcCount = 0;
    context->tableCount = 0;
    context->u_sess = u_sess;

    DependencePath paths = {
        .ulogPath = LIB_ULOG,
        .rpcPath = LIB_RPC_UCX,
        .sslDLPath = LIB_OPENSSL_DL,
        .sslPath = LIB_SSL,
        .cryptoPath = LIB_CRYPTO
    };
    RpcStatus status = RpcClientInit(paths);
    if (status != RPC_OK) {
        hash_destroy(context->channelCache);
        pfree(context);
        return NULL;
    }
    return context;
}
NdpContext* GetNdpContext()
{
    if (u_sess->ndp_cxt.cxt == nullptr) {
        MemoryContext oldContext = nullptr;
        oldContext = MemoryContextSwitchTo(u_sess->ndp_cxt.mem_cxt);
        u_sess->ndp_cxt.cxt = NdpCreateContext();
        MemoryContextSwitchTo(oldContext);
    } else {
        NdpReInitContext();
    }
    return (NdpContext*)u_sess->ndp_cxt.cxt;
}
// check after create plan
static void CheckAndSetNdpScanPlan(PlannedStmt* stmt, SeqScan* scan, Plan* parent, NdpContext** context)
{
    Plan* pushDownPlan = CheckAndGetNdpPlan(stmt, scan, parent);
    if (pushDownPlan == NULL) {
        return;
    }

    NdpScanCondition* cond = NdpCreateScanCondition(pushDownPlan);
    if (cond == NULL) {
        scan->plan.ndp_pushdown_optimized = false;
        scan->plan.ndp_pushdown_condition = NULL;
        ereport(WARNING, (errmsg("NdpCreateScanCondition failed")));
    } else {
        // store ndp context in ndp_pushdown_condition
        if (*context) {
            cond->ctx = *context;
            cond->tableId = ((*context)->tableCount)++;
            scan->plan.ndp_pushdown_optimized = true;
            scan->plan.ndp_pushdown_condition = (Node*)cond;
        } else {
            *context = GetNdpContext();
            if (!*context) {
                NdpDestroyScanCondition(cond);
                scan->plan.ndp_pushdown_optimized = false;
                scan->plan.ndp_pushdown_condition = NULL;
            } else {
                cond->ctx = *context;
                cond->tableId = ((*context)->tableCount)++;
                scan->plan.ndp_pushdown_optimized = true;
                scan->plan.ndp_pushdown_condition = (Node*)cond;
            }
        }
    }
}

static void TraversePlan(PlannedStmt* stmt, Plan* plan, Plan* parent, NdpContext** context)
{
    if (!plan) return;

    /* filter out the lefttree and righttree of T_MergeJoin which is T_Sort */
    if (IsA(plan, MergeJoin)) {
        TraversePlan(stmt, outerPlan(outerPlan(plan)), plan, context);
        TraversePlan(stmt, outerPlan(innerPlan(plan)), plan, context);
        return;
    } else if (IsA(plan, SeqScan)) {
        CheckAndSetNdpScanPlan(stmt, castNode(SeqScan, plan), parent, context);
    } else if (IsA(plan, SubqueryScan)) {
        TraversePlan(stmt, castNode(SubqueryScan, plan)->subplan, plan, context);
    } else if (IsA(plan, Append)) {
        ListCell* lc = NULL;
        foreach (lc, castNode(Append, plan)->appendplans) {
            Plan* appendPlans = (Plan*)lfirst(lc);
            TraversePlan(stmt, appendPlans, plan, context);
        }
    }

    TraversePlan(stmt, outerPlan(plan), plan, context);
    TraversePlan(stmt, innerPlan(plan), plan, context);
}

static void CheckAndSetNdpPlan(Query* querytree, PlannedStmt* stmt)
{
    knl_u_ndp_init(&u_sess->ndp_cxt);
    if (!CheckNdpSupport(querytree, stmt)) {
        return;
    }
    // travel plan to find scan node
    NdpContext* context = NULL;
    TraversePlan(stmt, stmt->planTree, NULL, &context);
    ListCell *l = NULL;
    foreach (l, stmt->subplans) {
        Plan *subplan = (Plan *)lfirst(l);
        TraversePlan(stmt, subplan, NULL, &context);
    }
}

static void NdpAggInitCollect(AggState* node)
{
    for (int i = 0; i < node->numaggs; i++) {
        AggStatePerAgg peragg = &(node->peragg[i]);
        Aggref* aggref = peragg->aggref;
        Oid collectfn_oid;
        Expr* collectfnexpr = NULL;

        if (OidIsValid(peragg->collectfn_oid)) {
            continue;
        }

        /* Fetch the pg_aggregate row */
        HeapTuple aggTuple = SearchSysCache1(AGGFNOID, ObjectIdGetDatum(aggref->aggfnoid));
        if (!HeapTupleIsValid(aggTuple)) {
            ereport(ERROR,
                    (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                        errmodule(MOD_EXECUTOR),
                        errmsg("cache lookup failed for aggregate %u", aggref->aggfnoid)));
        }
        Form_pg_aggregate aggform = (Form_pg_aggregate)GETSTRUCT(aggTuple);

        /* Check permission to call aggregate function */
        AclResult aclresult = pg_proc_aclcheck(aggref->aggfnoid, GetUserId(), ACL_EXECUTE);
        if (aclresult != ACLCHECK_OK)
            aclcheck_error(aclresult, ACL_KIND_PROC, get_func_name(aggref->aggfnoid));

        peragg->collectfn_oid = collectfn_oid = aggform->aggcollectfn;
        Oid aggtranstype = aggform->aggtranstype;

        if (OidIsValid(collectfn_oid)) {
            /* we expect final function expression to be NULL in call to
             * build_aggregate_fnexprs below, since InvalidOid is passed for
             * finalfn_oid argument. Use a dummy expression to accept that.
             */
            Expr* dummyexpr = NULL;
            /*
             * for XC, we need to setup the collection function expression as well.
             * Use build_aggregate_fnexpr() with invalid final function oid, and collection
             * function information instead of transition function information.
             * We should really be adding this step inside
             * build_aggregate_fnexprs() but this way it becomes easy to merge.
             */
            build_aggregate_fnexprs(&aggtranstype,
                1,
                aggtranstype,
                aggref->aggtype,
                aggref->inputcollid,
                collectfn_oid,
                InvalidOid,
                &collectfnexpr,
                &dummyexpr);
            Assert(!dummyexpr);
        }
        fmgr_info(collectfn_oid, &peragg->collectfn);
        peragg->collectfn.fn_expr = (Node*)collectfnexpr;

        ReleaseSysCache(aggTuple);
    }
}

static void NdpAggInit(AggState* node)
{
    Agg* plan = reinterpret_cast<Agg*>(node->ss.ps.plan);
    if (plan->aggstrategy == AGG_PLAIN) {
        for (int i = 0; i < node->numaggs; i++) {
            AggStatePerGroup pergroup = &(node->pergroup[i]);
            AggStatePerAgg peragg = &(node->peragg[i]);

            pergroup->transValueIsNull = peragg->initValueIsNull;
            if (!peragg->initValueIsNull) {
                pergroup->transValue =
                    datumCopy(peragg->initValue, peragg->transtypeByVal, peragg->transtypeLen);
                pergroup->noTransValue = false;
            } else {
                pergroup->noTransValue = true;
            }
        }
    }

    /*
     * we currently rely on collect function from gaussdb
     */
    if (IS_STREAM_PLAN || StreamThreadAmI()) {
        NdpAggInitCollect(node);
    }
}

static void TraverseState(PlanState* state, PlanState* parent)
{
    if (!state) return;

    if (IsA(state, SeqScanState)) {
        auto seq = reinterpret_cast<SeqScanState*>(state);
        if (!seq->ss_currentScanDesc || !seq->ss_currentScanDesc->ndp_pushdown_optimized) {
            return;
        }
        auto ndpScan = reinterpret_cast<NdpScanDesc>(seq->ss_currentScanDesc->ndp_ctx);
        Assert(ndpScan);

        if (IsA(ndpScan->cond->plan, Agg)) {
            Assert(parent && IsA(parent, AggState));
            ndpScan->aggState = reinterpret_cast<AggState*>(parent);
            TupleDesc desc = NdpAggTupleDescCreate(ndpScan->aggState);
            // should use ExecInitExtraTupleSlot to put in estate->es_tupleTable?
            TupleTableSlot* slot = MakeTupleTableSlot(false, TableAmHeap);
            ExecSetSlotDescriptor(slot, desc);
            ndpScan->aggSlot = slot;
            NdpAggInit(ndpScan->aggState);
            ndpScan->aggState->ndp_slot = slot;
        }
    } else if (IsA(state, SubqueryScanState)) {
        TraverseState(castNode(SubqueryScanState, state)->subplan, state);
    } else if (IsA(state, AppendState)) {
        AppendState* appState = reinterpret_cast<AppendState*>(state);
        for (int i = 0; i < appState->as_nplans; i++) {
            TraverseState(*(appState->appendplans + i), state);
        }
    }

    TraverseState(outerPlanState(state), state);
    TraverseState(innerPlanState(state), state);
}

static void NdpExecutorStart(QueryDesc* queryDesc, int eflags)
{
    knl_u_ndp_init(&u_sess->ndp_cxt);
    if (ndp_hook_ExecutorStart)
        ndp_hook_ExecutorStart(queryDesc, eflags);
    else
        standard_ExecutorStart(queryDesc, eflags);

    TraverseState(queryDesc->planstate, NULL);
    ListCell *l = NULL;
    foreach (l, queryDesc->estate->es_subplanstates) {
        PlanState* subplanstate = (PlanState*)lfirst(l);
        TraverseState(subplanstate, NULL);
    }
}

static void NdpExecutorEnd(QueryDesc* queryDesc)
{
    if (ndp_hook_ExecutorEnd)
        ndp_hook_ExecutorEnd(queryDesc);
    else
        standard_ExecutorEnd(queryDesc);
    if (!StreamThreadAmI()) {
        NdpReInitContext();
    }
}

static void InitializeNdpGUCOptions()
{
    DefineCustomBoolVariable("ndpplugin.enable_ndp",
        "Enable NDP engine",
        NULL,
        &u_sess->ndp_cxt.enable_ndp,
        false,
        PGC_USERSET,
        0,
        NULL,
        NULL,
        NULL);
    DefineCustomIntVariable("ndpplugin.pushdown_min_blocks",
        "Sets the lower limit of pushdown pages..",
        NULL,
        &u_sess->ndp_cxt.pushdown_min_blocks,
        0,
        0,
        INT_MAX / 1000,
        PGC_USERSET,
        GUC_CUSTOM_PLACEHOLDER,
        NULL,
        NULL,
        NULL);
    DefineCustomIntVariable("ndpplugin.ndp_port",
        "Sets the ndp_port of ndp",
        NULL,
        &u_sess->ndp_cxt.ndp_port,
        8000,
        0,
        65535,
        PGC_USERSET,
        GUC_CUSTOM_PLACEHOLDER,
        NULL,
        NULL,
        NULL);
#ifdef ENABLE_SSL
    DefineCustomStringVariable("ndpplugin.ca_path",
       "Client CA path",
       NULL,
       &u_sess->ndp_cxt.ca_path,
       "./",
       PGC_USERSET,
       GUC_LIST_INPUT,
       NULL,
       NULL,
       NULL);
    DefineCustomStringVariable("ndpplugin.crl_path",
       "Client crl path",
       NULL,
       &u_sess->ndp_cxt.crl_path,
       "./",
       PGC_USERSET,
       GUC_LIST_INPUT,
       NULL,
       NULL,
       NULL);
#endif
}

static void knl_u_ndp_init(knl_u_ndp_context* ndp_cxt)
{
    if (ndp_cxt->mem_cxt != nullptr) {
        return;
    }
    ndp_cxt->mem_cxt = AllocSetContextCreate(u_sess->top_mem_cxt,
        "NdpSelfMemoryContext",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);
    MemoryContext oldContext = MemoryContextSwitchTo(u_sess->ndp_cxt.mem_cxt);
    ndp_cxt->stats = (NdpStats*)palloc0(sizeof(NdpStats));
    ndp_cxt->cxt = nullptr;
    InitializeNdpGUCOptions();
    MemoryContextSwitchTo(oldContext);
}

/*
 * Entrypoint of this extension
 */
void _PG_init(void)
{
    ereport(DEBUG2, (errmsg("init ndpplugin.")));

    if (!ENABLE_DSS) {
        ereport(DEBUG2, (errmsg("ndpplugin is not support while DMS and DSS disable.")));
        return;
    }

    pthread_mutex_lock(&g_ndp_instance.mutex);

#ifdef GlobalCache
    long long au_size;
    const char *vg_name = g_instance.attr.attr_storage.dss_attr.ss_dss_vg_name + 1;
    int ret = dss_compare_size(vg_name, &au_size);
    if (ret != 0 || au_size != DSS_DEFAULT_AU_SIZE) {
        pthread_mutex_unlock(&g_ndp_instance.mutex);
        ereport(WARNING, (errmsg("init ndpplugin failed, inconsistency between dss_ausize and ndpplugin_ausize!")));
        return;
    }
#endif
    NdpInstanceInit();
    pthread_mutex_unlock(&g_ndp_instance.mutex);

    if (HOOK_INIT == false) {
        backup_ndp_pushdown_hook_type = ndp_pushdown_hook;
        ndp_pushdown_hook = CheckAndSetNdpPlan;

        backup_ndp_tableam = ndp_tableam;
        ndp_tableam = &ndp_tableam_apply;

        ndp_hook_ExecutorStart = ExecutorStart_hook;
        ExecutorStart_hook = NdpExecutorStart;
        ndp_hook_ExecutorEnd = ExecutorEnd_hook;
        ExecutorEnd_hook = NdpExecutorEnd;
    }
    HOOK_INIT = true;
    knl_u_ndp_init(&u_sess->ndp_cxt);
}

void _PG_fini(void)
{
    ndp_pushdown_hook = backup_ndp_pushdown_hook_type;
    ndp_tableam = backup_ndp_tableam;

    ExecutorStart_hook = ndp_hook_ExecutorStart;
    ExecutorEnd_hook = ndp_hook_ExecutorEnd;
    MemoryContextDelete(u_sess->ndp_cxt.mem_cxt);

    pthread_mutex_lock(&g_ndp_instance.mutex);
    if (g_ndp_instance.pageContextPtr) {
        free(g_ndp_instance.pageContextPtr);
        g_ndp_instance.pageContextPtr = nullptr;
        delete g_ndp_instance.pageContext;
        g_ndp_instance.pageContext = nullptr;
    }
    g_ndp_instance.status = UNINITIALIZED;
    pthread_mutex_unlock(&g_ndp_instance.mutex);
}

/*
 * For test ndpplugin push down functionality
 */
Datum pushdown_statistics(PG_FUNCTION_ARGS)
{
    if (u_sess->ndp_cxt.stats == NULL) {
        ereport(WARNING, (errmsg("ndp init failed, the pushdown statistics can not be viewed")));
        PG_RETURN_NULL();
    }

    const int cols = 7;

    TupleDesc tupdesc = CreateTemplateTupleDesc(cols, true);

    TupleDescInitEntry(tupdesc, (AttrNumber)1, "query", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)2, "total_pushdown_page", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)3, "back_to_gauss", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)4, "received_scan", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)5, "received_agg", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)6, "failed_backend_handle", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)7, "failed_sendback", INT8OID, -1, 0);

    BlessTupleDesc(tupdesc);

    Datum values[cols];
    values[0] = UInt64GetDatum(u_sess->ndp_cxt.stats->queryCounter);
    values[1] = UInt64GetDatum(u_sess->ndp_cxt.stats->pushDownPage);
    values[2] = UInt64GetDatum(u_sess->ndp_cxt.stats->sendBackPage);
    values[3] = UInt64GetDatum(u_sess->ndp_cxt.stats->ndpPageScan);
    values[4] = UInt64GetDatum(u_sess->ndp_cxt.stats->ndpPageAgg);
    values[5] = UInt64GetDatum(u_sess->ndp_cxt.stats->failedIO);
    values[6] = UInt64GetDatum(u_sess->ndp_cxt.stats->sendFailed);

    bool nulls[cols] = {false, false, false, false, false, false, false};

    HeapTuple tuple = heap_form_tuple(tupdesc, values, nulls);
    HeapTupleHeader result = (HeapTupleHeader)palloc(tuple->t_len);
    int rc = memcpy_s(result, tuple->t_len, tuple->t_data, tuple->t_len);
    securec_check_ss(rc, "\0", "\0");
    ReleaseTupleDesc(tupdesc);

    PG_RETURN_HEAPTUPLEHEADER(result);
}

/* test section end */
