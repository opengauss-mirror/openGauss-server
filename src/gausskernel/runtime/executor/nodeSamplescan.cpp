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
 * nodeSamplescan.cpp
 *
 *      Support routines for sample scans of relations (table sampling).
 *
 * IDENTIFICATION
 *      src/gausskernel/runtime/executor/nodeSamplescan.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/hash.h"
#include "access/relscan.h"
#include "access/tableam.h"
#include "commands/vacuum.h"
#include "executor/executor.h"
#include "executor/node/nodeSamplescan.h"
#include "executor/node/nodeSeqscan.h"
#include "miscadmin.h"
#include "pgstat.h"
#ifdef PGXC
#include "pgxc/pgxc.h"
#endif
#include "storage/predicate.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/snapmgr.h"
#include "vecexecutor/vecnodecstorescan.h"
#include "nodes/execnodes.h"
#include "access/ustore/knl_uscan.h"

static double sample_random_fract(void);

/*
 * Description: Initialize relation descriptor for sample table scan.
 *
 * Parameters:
 *	@in scanstate: ScanState information
 *	@in currentRelation: relation being scanned
 *
 * Returns: HeapScanDesc
 */
TableScanDesc InitSampleScanDesc(ScanState* scanstate, Relation currentRelation)
{
    bool allow_sync = false;
    bool use_bulkread = false;
    TableScanDesc current_scan_desc = NULL;
    SampleScanParams* sample_scan_info = &scanstate->sampleScanInfo;

    /* Need scan all block. */
    if (sample_scan_info->sampleType == BERNOULLI_SAMPLE) {
        allow_sync = true;

        /*
         * Use bulkread, since we're scanning all pages. But pagemode visibility
         * checking is a win only at larger sampling fractions.
         */
        use_bulkread = true;
    } else {
        allow_sync = false;

        /*
         * Bulkread buffer access strategy probably makes sense unless we're
         * scanning a very small fraction of the table.
         */
        use_bulkread = (((BaseTableSample*)sample_scan_info->tsm_state)->percent[0] >= 1);
    }

    current_scan_desc = scan_handler_tbl_beginscan_sampling(
        currentRelation, scanstate->ps.state->es_snapshot, 0, NULL, use_bulkread, allow_sync, scanstate);

    return current_scan_desc;
}
static inline HeapTuple SampleFetchNextTuple(SeqScanState* node)
{
    TableScanDesc tableScanDesc = GetTableScanDesc(node->ss_currentScanDesc, node->ss_currentRelation);
    tableScanDesc->rs_ss_accessor = node->ss_scanaccessor;

    /*
     * Get the next tuple for table sample, and return it.
     * Scans the relation using the sampling method and returns
     * the next qualifying tuple. We call the ExecScan() routine and pass it
     * the appropriate access method functions.
     */
    return (((RowTableSample*)node->sampleScanInfo.tsm_state)->scanSample)();
}

static inline UHeapTuple USampleFetchNextTuple(SeqScanState* node)
{
    UHeapScanDesc uheapScanDesc = (UHeapScanDesc)node->ss_currentScanDesc;
    uheapScanDesc->rs_base.rs_ss_accessor = node->ss_scanaccessor;

    /*
     * Get the next tuple for table sample, and return it.
     * Scans the relation using the sampling method and returns
     * the next qualifying tuple. We call the ExecScan() routine and pass it
     * the appropriate access method functions.
     */
    return (((UstoreTableSample*)node->sampleScanInfo.tsm_state)->scanSample)();
}

TupleTableSlot* HeapSeqSampleNext(SeqScanState* node)
{
    TupleTableSlot* slot = node->ss_ScanTupleSlot;
    node->ss_ScanTupleSlot->tts_tupleDescriptor->tdTableAmType = node->ss_currentRelation->rd_tam_type;
    HeapTuple tuple = SampleFetchNextTuple(node);
    return ExecMakeTupleSlot(tuple, GetTableScanDesc(node->ss_currentScanDesc, node->ss_currentRelation), slot, node->ss_currentRelation->rd_tam_type);
}

TupleTableSlot* UHeapSeqSampleNext(SeqScanState* node)
{
    TupleTableSlot* slot = node->ss_ScanTupleSlot;
    UHeapTuple tuple = USampleFetchNextTuple(node);
    if (tuple != NULL) {
        return ExecStoreTuple(tuple, slot, InvalidBuffer, true);
    }

    return ExecClearTuple(slot);
}

/*
 * Description: Get the next tuple from the table for sample scan.
 *
 * Parameters:
 * @in node: ScanState information
 *
 * Returns: TupleTableSlot
 *
 */
TupleTableSlot* SeqSampleNext(SeqScanState* node)
{
    bool isUstore = RelationIsUstoreFormat(node->ss_currentRelation);
    if (!isUstore) {
        return HeapSeqSampleNext(node);
    } else {
        return UHeapSeqSampleNext(node);
    }
}

TupleTableSlot* HbktSeqSampleNext(SeqScanState* node)
{
    TupleTableSlot* slot = node->ss_ScanTupleSlot;
    HeapTuple tuple = NULL;
    TableScanDesc hb_scan = node->ss_currentScanDesc;

    while (hb_scan != NULL) {
        tuple = SampleFetchNextTuple(node);
        if (tuple != NULL) {
            break;
        }

        /* try switch to next partition */
        if (!hbkt_sampling_scan_nextbucket(hb_scan)) {
            break;
        }

        (((RowTableSample*)node->sampleScanInfo.tsm_state)->resetSampleScan)();
    }

    node->ss_ScanTupleSlot->tts_tupleDescriptor->tdTableAmType = node->ss_currentRelation->rd_tam_type;
    return ExecMakeTupleSlot(
            (Tuple) tuple, GetTableScanDesc(node->ss_currentScanDesc, node->ss_currentRelation),
            slot,
            node->ss_currentRelation->rd_tam_type);
}

/*
 * Description: Get seed value.
 *
 * Parameters: null
 *
 * Returns: void
 */
void BaseTableSample::getSeed()
{
    Datum datum;
    bool isnull = false;
    ExprContext* econtext = sampleScanState->ps.ps_ExprContext;
    ExprState* repeatable = sampleScanState->sampleScanInfo.repeatable;

    if (NULL != repeatable) {
        datum = ExecEvalExprSwitchContext(repeatable, econtext, &isnull, NULL);
        if (isnull) {
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_TABLESAMPLE_REPEAT),
                    errmsg("TABLESAMPLE REPEATABLE parameter cannot be null")));
        }

        /*
         * The REPEATABLE parameter has been coerced to float8 by the parser.
         * The reason for using float8 at the SQL level is that it will
         * produce unsurprising results both for users used to databases that
         * accept only integers in the REPEATABLE clause and for those who
         * might expect that REPEATABLE works like setseed() (a float in the
         * range from -1 to 1).
         *
         * We use hashfloat8() to convert the supplied value into a suitable
         * seed. For regression-testing purposes, that has the convenient
         * property that REPEATABLE(0) gives a machine-independent result.
         */
        seed = DatumGetUInt32(DirectFunctionCall1(hashfloat8, datum));
    } else {
        seed = random();
    }

    if (seed > 0) {
        gs_srandom(seed);
    }
}

/*
 * Description: Get percent value.
 * Parameters: null
 * Returns: void
 */
void BaseTableSample::getPercent()
{
    int i = 0;
    ListCell* arg = NULL;
    bool isnull = false;

    ExprContext* econtext = sampleScanState->ps.ps_ExprContext;
    List* args = sampleScanState->sampleScanInfo.args;
    Datum* params = (Datum*)palloc0(list_length(args) * sizeof(Datum));

    Assert(list_length(args));
    percent = (double*)palloc0(SAMPLEARGSNUM * sizeof(double));

    foreach (arg, args) {
        ExprState* argstate = (ExprState*)lfirst(arg);

        params[i] = ExecEvalExprSwitchContext(argstate, econtext, &isnull, NULL);
        if (isnull) {
            ereport(
                ERROR, (errcode(ERRCODE_INVALID_TABLESAMPLE_ARGUMENT), errmsg("TABLESAMPLE parameter cannot be null")));
        }

        percent[i] = DatumGetFloat4(params[i]);

        if (percent[i] < MIN_PERCENT_ARG || percent[i] > MAX_PERCENT_ARG || isnan(percent[i])) {
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_TABLESAMPLE_ARGUMENT), errmsg("sample percentage must be between 0 and 100")));
        }

        i++;
    }
}

/*
 * Description: Get next random block number.
 * Parameters: null
 * Returns: void
 */
void BaseTableSample::system_nextsampleblock()
{
    BlockNumber blockindex = 0;

    /* We should start from currentBlock + 1. */
    for (blockindex = currentBlock + 1; blockindex < totalBlockNum; blockindex++) {
        if (sample_random_fract() < percent[SYSTEM_SAMPLE]) {
            break;
        }
    }

    if (blockindex < totalBlockNum) {
        currentBlock = blockindex;
    } else {
        currentBlock = InvalidBlockNumber;
    }
}

/*
 * Description: Get sequential next offset.
 * Parameters: null
 * Returns: void
 */
void BaseTableSample::system_nextsampletuple()
{
    OffsetNumber tupoffset = currentOffset;

    /* Advance to next possible offset on page */
    if (tupoffset == InvalidOffsetNumber) {
        tupoffset = FirstOffsetNumber;
    } else {
        tupoffset++;
    }

    if (tupoffset > curBlockMaxoffset) {
        tupoffset = InvalidOffsetNumber;
    }

    currentOffset = tupoffset;
}

/*
 * Description: Get sequential next block.
 * Parameters: null
 * Returns: void
 */
void BaseTableSample::bernoulli_nextsampleblock()
{
    if (currentBlock + 1 < totalBlockNum) {
        currentBlock++;
    } else {
        currentBlock = InvalidBlockNumber;
    }
}

/*
 * Description: Get random next block.
 * Parameters: null
 * Returns: void
 */
void BaseTableSample::bernoulli_nextsampletuple()
{
    OffsetNumber tupoffset = currentOffset;

    /* Advance to first/next tuple in block */
    if (tupoffset == InvalidOffsetNumber) {
        tupoffset = FirstOffsetNumber;
    } else {
        tupoffset++;
    }

    /*
     * Loop over tuple offsets until finding suitable TID or reaching end of
     * block.
     */
    for (; tupoffset <= curBlockMaxoffset; tupoffset++) {
        if (sample_random_fract() < percent[BERNOULLI_SAMPLE]) {
            break;
        }
    }

    if (tupoffset > curBlockMaxoffset) {
        tupoffset = InvalidOffsetNumber;
    }

    currentOffset = tupoffset;
}

/*
 * Description: Initialize base tableSample info.
 *
 * Parameters:
 *	@in scanstate: ScanState information
 *
 * Returns: void
 */
BaseTableSample::BaseTableSample(void* scanstate)
    : runState(GETMAXBLOCK),
      totalBlockNum(0),
      currentBlock(InvalidBlockNumber),
      currentOffset(InvalidOffsetNumber),
      curBlockMaxoffset(InvalidOffsetNumber),
      finished(false)
{
    TableSampleType sampleType;
    bool vectorized = ((ScanState*)scanstate)->ps.vectorized;

    sampleScanState = (ScanState*)scanstate;
    sampleType = ((ScanState*)scanstate)->sampleScanInfo.sampleType;
    getPercent();
    getSeed();

    /* Save vecsample ScanState if it is vectorized. */
    if (vectorized) {
        vecsampleScanState = (CStoreScanState*)scanstate;
    }

    /* We can transform hybrid to system or bernoulli for optimize according to value of args. */
    if ((sampleType == BERNOULLI_SAMPLE) ||
        (sampleType == HYBRID_SAMPLE && percent[SYSTEM_SAMPLE] == MAX_PERCENT_ARG)) {
        percent[BERNOULLI_SAMPLE] = (sampleType == BERNOULLI_SAMPLE) ? (percent[0] / MAX_PERCENT_ARG)
                                                                     : (percent[BERNOULLI_SAMPLE] / MAX_PERCENT_ARG);
        nextSampleBlock_function = &BaseTableSample::bernoulli_nextsampleblock;
        nextSampleTuple_function = &BaseTableSample::bernoulli_nextsampletuple;
    } else if ((sampleType == SYSTEM_SAMPLE) ||
               (sampleType == HYBRID_SAMPLE && percent[BERNOULLI_SAMPLE] == MAX_PERCENT_ARG)) {
        percent[SYSTEM_SAMPLE] =
            (sampleType == SYSTEM_SAMPLE) ? (percent[0] / MAX_PERCENT_ARG) : (percent[SYSTEM_SAMPLE] / MAX_PERCENT_ARG);
        nextSampleBlock_function = &BaseTableSample::system_nextsampleblock;
        nextSampleTuple_function = &BaseTableSample::system_nextsampletuple;
    } else {
        Assert(sampleType == HYBRID_SAMPLE);
        percent[SYSTEM_SAMPLE] = percent[SYSTEM_SAMPLE] / MAX_PERCENT_ARG;
        percent[BERNOULLI_SAMPLE] = percent[BERNOULLI_SAMPLE] / MAX_PERCENT_ARG;
        nextSampleBlock_function = &BaseTableSample::system_nextsampleblock;
        nextSampleTuple_function = &BaseTableSample::bernoulli_nextsampletuple;
    }
    scanTupState = 0;
}

BaseTableSample::~BaseTableSample()
{
    sampleScanState = NULL;
    vecsampleScanState = NULL;
    percent = NULL;
}

/*
 * Description: Reset Sample Scan parameter.
 *
 * Parameters: null
 *
 * Returns: void
 */
void BaseTableSample::resetSampleScan()
{
    runState = GETMAXBLOCK;
    totalBlockNum = 0;
    currentOffset = InvalidOffsetNumber;
    currentBlock = InvalidBlockNumber;
    curBlockMaxoffset = InvalidOffsetNumber;
    finished = false;
}

/*
 * Description: Initialize Sample Scan parameter.
 *
 * Parameters:
 *	@in scanstate: ScanState information
 *
 * Returns: void
 */
RowTableSample::RowTableSample(ScanState* scanstate) : BaseTableSample(scanstate)
{}

RowTableSample::~RowTableSample()
{}

/*
 * Description: Get max offset for current block.
 *
 * Parameters: null
 *
 * Returns: void
 */
void RowTableSample::getMaxOffset()
{
    TableScanDesc tablescan = NULL;
    TableScanDesc scan = sampleScanState->ss_currentScanDesc;
    bool pagemode = GetTableScanDesc(scan, sampleScanState->ss_currentRelation)->rs_pageatatime;
    Page page;

    Assert(BlockNumberIsValid(currentBlock));

    if (scanTupState == NEWBLOCK) {
        tableam_scan_getpage(GetTableScanDesc(scan, sampleScanState->ss_currentRelation), currentBlock);
    }

    /*
     * When not using pagemode, we must lock the buffer during tuple
     * visibility checks.
     */
    tablescan = GetTableScanDesc(scan, sampleScanState->ss_currentRelation);
    if (!pagemode) {
        LockBuffer(tablescan->rs_cbuf, BUFFER_LOCK_SHARE);
    }

    page = (Page)BufferGetPage(tablescan->rs_cbuf);
    curBlockMaxoffset = PageGetMaxOffsetNumber(page);

    /* Found visible tuple, return it. */
    if (!pagemode) {
        LockBuffer(tablescan->rs_cbuf, BUFFER_LOCK_UNLOCK);
    }
}

/*
 * Description: Scan tuple according to currentblock and current currentoffset.
 *
 * Parameters: null
 *
 * Returns: ScanValid (the flag which identify the tuple is valid or not)
 */
ScanValid RowTableSample::scanTup()
{
    TableScanDesc scan = GetTableScanDesc(sampleScanState->ss_currentScanDesc, sampleScanState->ss_currentRelation);
    bool pagemode = scan->rs_pageatatime;
    HeapTuple tuple = &(((HeapScanDesc)scan)->rs_ctup);
    Snapshot snapshot = scan->rs_snapshot;
    ItemId itemid;
    Page page;
    bool all_visible = false;
    bool visible = false;

    if (scanTupState == NEWBLOCK) {
        if (BlockNumberIsValid(currentBlock)) {
            /*
             * Report our new scan position for synchronization purposes.
             *
             * Note: we do this before checking for end of scan so that the
             * final state of the position hint is back at the start of the
             * rel.  That's not strictly necessary, but otherwise when you run
             * the same query multiple times the starting position would shift
             * a little bit backwards on every invocation, which is confusing.
             * We don't guarantee any specific ordering in general, though.
             */
            if (scan->rs_syncscan) {
                ss_report_location(scan->rs_rd, currentBlock);
            }
        } else {
            if (scan->rs_inited) {
                if (BufferIsValid(scan->rs_cbuf)) {
                    ReleaseBuffer(scan->rs_cbuf);
                }
                scan->rs_cbuf = InvalidBuffer;
                scan->rs_cblock = InvalidBlockNumber;
                scan->rs_inited = false;
            }

            tuple->t_data = NULL;

            return INVALIDBLOCKNO;
        }

        if (!scan->rs_inited) {
            scan->rs_inited = true;
        }

        scanTupState = NONEWBLOCK;
    }

    Assert(currentBlock < scan->rs_nblocks);

    /* Current block alreadly have be readed.*/
    if (currentOffset == InvalidOffsetNumber) {
        /*
         * If we get here, it means we've exhausted the items on this page and
         * it's time to move to the next.
         */
        if (!pagemode) {
            LockBuffer(scan->rs_cbuf, BUFFER_LOCK_UNLOCK);
        }

        return INVALIDOFFSET;
    }

    /*
     * When not using pagemode, we must lock the buffer during tuple
     * visibility checks.
     */
    if (!pagemode) {
        LockBuffer(scan->rs_cbuf, BUFFER_LOCK_SHARE);
    }

    page = (Page)BufferGetPage(scan->rs_cbuf);
    all_visible = PageIsAllVisible(page) && !(snapshot->takenDuringRecovery);

    /* Skip invalid tuple pointers. */
    itemid = PageGetItemId(page, currentOffset);
    if (!ItemIdIsNormal(itemid)) {
        return NEXTDATA;
    }

    tuple->t_data = (HeapTupleHeader)PageGetItem(page, itemid);
    tuple->t_len = ItemIdGetLength(itemid);
    HeapTupleCopyBaseFromPage(tuple, page);
    ItemPointerSet(&(tuple->t_self), currentBlock, currentOffset);

    if (all_visible) {
        visible = true;
    } else {
        BufferDesc* bufHdr = GetBufferDescriptor(scan->rs_cbuf - 1);
        bool isTmpLock = false;

        if (!LWLockHeldByMe(bufHdr->content_lock)) {
            LockBuffer(scan->rs_cbuf, BUFFER_LOCK_SHARE);
            isTmpLock = true;
        }

        visible = HeapTupleSatisfiesVisibility(tuple, scan->rs_snapshot, scan->rs_cbuf);

        if (isTmpLock) {
            LockBuffer(scan->rs_cbuf, BUFFER_LOCK_UNLOCK);
        }
    }

    /* in pagemode, heapgetpage did this for us */
    if (!pagemode) {
        CheckForSerializableConflictOut(visible, scan->rs_rd, tuple, scan->rs_cbuf, snapshot);
    }

    if (visible) {
        /* Found visible tuple, return it. */
        if (!pagemode) {
            LockBuffer(scan->rs_cbuf, BUFFER_LOCK_UNLOCK);
        }

        /* Count successfully-fetched tuples as heap fetches */
        pgstat_count_heap_getnext(scan->rs_rd);

        elog(DEBUG2,
            "Get one tuple [currentBlock: %u, currentOffset: %u] for relation: %s on %s.",
            currentBlock,
            currentOffset,
            NameStr(scan->rs_rd->rd_rel->relname),
            g_instance.attr.attr_common.PGXCNodeName);

        return VALIDDATA;
    }

    return NEXTDATA;
}

/*
 * Description: Get sample tuple for row table.
 *
 * Parameters: null
 *
 * Returns: HeapTuple
 */
HeapTuple RowTableSample::scanSample()
{
    TableScanDesc scan = GetTableScanDesc(sampleScanState->ss_currentScanDesc, sampleScanState->ss_currentRelation);
    HeapTuple tuple = &(((HeapScanDesc)scan)->rs_ctup);

    if (finished == true) {
        return NULL;
    }

    /* Return NULL if no data or percent value is 0. */
    if ((scan->rs_nblocks == 0) ||
        (sampleScanState->sampleScanInfo.sampleType == BERNOULLI_SAMPLE && percent[0] == 0) ||
        (sampleScanState->sampleScanInfo.sampleType == SYSTEM_SAMPLE && percent[0] == 0) ||
        (sampleScanState->sampleScanInfo.sampleType == HYBRID_SAMPLE && percent[BERNOULLI_SAMPLE] == 0 &&
            percent[SYSTEM_SAMPLE] == 0)) {
        Assert(!BufferIsValid(scan->rs_cbuf));
        tuple->t_data = NULL;
        return NULL;
    }

    for (;;) {
        CHECK_FOR_INTERRUPTS();

        switch (runState) {
            /* Get num of max block. */
            case GETMAXBLOCK: {
                totalBlockNum = scan->rs_nblocks;
                runState = GETBLOCKNO;
                elog(DEBUG2,
                    "Get %u blocks for relation: %s on %s.",
                    totalBlockNum,
                    NameStr(scan->rs_rd->rd_rel->relname),
                    g_instance.attr.attr_common.PGXCNodeName);
                break;
            }
            case GETBLOCKNO: {
                /* Get current block no with method of function. */
                (this->*nextSampleBlock_function)();

                if (BlockNumberIsValid(currentBlock)) {
                    runState = GETMAXOFFSET;
                } else {
                    runState = GETDATA;
                }

                scanTupState = NEWBLOCK;
                break;
            }
            case GETMAXOFFSET: {
                getMaxOffset();
                runState = GETOFFSET;
                elog(DEBUG2,
                    "Get %d tuples in blockno: %u for relation: %s on %s.",
                    curBlockMaxoffset,
                    currentBlock,
                    NameStr(scan->rs_rd->rd_rel->relname),
                    g_instance.attr.attr_common.PGXCNodeName);
                break;
            }
            case GETOFFSET: {
                (this->*nextSampleTuple_function)();

                runState = GETDATA;
                break;
            }
            case GETDATA: {
                ScanValid scanState = scanTup();

                switch (scanState) {
                    case VALIDDATA: {
                        runState = GETOFFSET;
                        return &(((HeapScanDesc)scan)->rs_ctup);
                    }
                    case NEXTDATA: {
                        runState = GETOFFSET;
                        break;
                    }
                    case INVALIDBLOCKNO: {
                        /* All block alreadly be scaned finish.*/
                        finished = true;
                        return NULL;
                    }
                    case INVALIDOFFSET: {
                        runState = GETBLOCKNO;
                        break;
                    }
                    default: {
                        break;
                    }
                }
                break;
            }
            default: {
                break;
            }
        }
    }
}

/*
 * Description: Initialize Sample Scan parameter.
 *
 * Parameters:
 *	@in scanstate: ScanState information
 *
 * Returns: void
 */
UstoreTableSample::UstoreTableSample(ScanState* scanstate) : BaseTableSample(scanstate)
{}

UstoreTableSample::~UstoreTableSample()
{}

/*
 * Description: Get max offset for current block.
 *
 * Parameters: null
 *
 * Returns: void
 */
void UstoreTableSample::getMaxOffset()
{
    TableScanDesc scan = sampleScanState->ss_currentScanDesc;
    UHeapScanDesc uheapscan = (UHeapScanDesc)scan;

    Assert(BlockNumberIsValid(currentBlock));

    Assert (scanTupState == NEWBLOCK);
    UHeapGetPage(scan, currentBlock);

    curBlockMaxoffset = uheapscan->rs_base.rs_ntuples;
}

/*
 * Description: Scan tuple according to currentblock and current currentoffset.
 *
 * Parameters: null
 *
 * Returns: ScanValid (the flag which identify the tuple is valid or not)
 */
ScanValid UstoreTableSample::scanTup()
{
    UHeapScanDesc scan = (UHeapScanDesc)sampleScanState->ss_currentScanDesc;

    if (!BlockNumberIsValid(currentBlock)) {
        return INVALIDBLOCKNO;
    }

    if (currentOffset >= 1 && currentOffset <= curBlockMaxoffset) {
        scan->rs_cutup = scan->rs_visutuples[currentOffset - 1];
        return VALIDDATA;
    }

    return INVALIDOFFSET;
}

/*
 * Description: Get sample tuple for row table.
 *
 * Parameters: null
 *
 * Returns: HeapTuple
 */
UHeapTuple UstoreTableSample::scanSample()
{
    UHeapScanDesc scan = (UHeapScanDesc)sampleScanState->ss_currentScanDesc;

    if (finished == true) {
        return NULL;
    }

    /* Return NULL if no data or percent value is 0. */
    if ((scan->rs_base.rs_nblocks == 0) ||
        (sampleScanState->sampleScanInfo.sampleType == BERNOULLI_SAMPLE && percent[0] == 0) ||
        (sampleScanState->sampleScanInfo.sampleType == SYSTEM_SAMPLE && percent[0] == 0) ||
        (sampleScanState->sampleScanInfo.sampleType == HYBRID_SAMPLE && percent[BERNOULLI_SAMPLE] == 0 &&
            percent[SYSTEM_SAMPLE] == 0)) {
        Assert(!BufferIsValid(scan->rs_base.rs_cbuf));
        return NULL;
    }

    for (;;) {
        CHECK_FOR_INTERRUPTS();

        switch (runState) {
            /* Get num of max block. */
            case GETMAXBLOCK: {
                totalBlockNum = scan->rs_base.rs_nblocks;
                runState = GETBLOCKNO;
                elog(DEBUG2,
                    "Get %u blocks for relation: %s on %s.",
                    totalBlockNum,
                    NameStr(scan->rs_base.rs_rd->rd_rel->relname),
                    g_instance.attr.attr_common.PGXCNodeName);
                break;
            }
            case GETBLOCKNO: {
                /* Get current block no with method of function. */
                (this->*nextSampleBlock_function)();

                if (BlockNumberIsValid(currentBlock)) {
                    runState = GETMAXOFFSET;
                } else {
                    runState = GETDATA;
                }

                scanTupState = NEWBLOCK;
                break;
            }
            case GETMAXOFFSET: {
                getMaxOffset();
                runState = GETOFFSET;
                elog(DEBUG2,
                    "Get %d tuples in blockno: %u for relation: %s on %s.",
                    curBlockMaxoffset,
                    currentBlock,
                    NameStr(scan->rs_base.rs_rd->rd_rel->relname),
                    g_instance.attr.attr_common.PGXCNodeName);
                break;
            }
            case GETOFFSET: {
                (this->*nextSampleTuple_function)();

                runState = GETDATA;
                break;
            }
            case GETDATA: {
                ScanValid scanState = scanTup();

                switch (scanState) {
                    case VALIDDATA: {
                        runState = GETOFFSET;
                        return scan->rs_cutup;
                    }
                    case NEXTDATA: {
                        runState = GETOFFSET;
                        break;
                    }
                    case INVALIDBLOCKNO: {
                        /* All block alreadly be scaned finish. */
                        finished = true;
                        return NULL;
                    }
                    case INVALIDOFFSET: {
                        runState = GETBLOCKNO;
                        break;
                    }
                    default: {
                        break;
                    }
                }
                break;
            }
            default: {
                break;
            }
        }
    }
}

/*
 * Description: Initialize Sample Scan parameter for CStoreScanState.
 *
 * Parameters:
 *	@in scanstate: CStoreScanState information
 *
 * Returns: void
 */
ColumnTableSample::ColumnTableSample(CStoreScanState* scanstate)
    : BaseTableSample(scanstate), currentCuId(0), batchRowCount(0)
{
    offsetIds = (uint16*)palloc0(sizeof(uint16) * BatchMaxSize);
    errno_t rc = memset_s(offsetIds, sizeof(uint16) * BatchMaxSize, 0, sizeof(uint16) * BatchMaxSize);
    securec_check(rc, "", "");

    /* Create new VectorBatch for construct tids to get sample VectorBatch. */
    TupleDesc tupdesc = CreateTemplateTupleDesc(1, false);
    TupleDescInitEntry(tupdesc, (AttrNumber)1, "tids", INT8OID, -1, 0);
    tids = New(CurrentMemoryContext) VectorBatch(CurrentMemoryContext, tupdesc);
}

ColumnTableSample::~ColumnTableSample()
{
    if (offsetIds) {
        pfree_ext(offsetIds);
        offsetIds = NULL;
    }
    if (tids) {
        delete tids;
        tids = NULL;
    }
}

/*
 * Description: Reset Vec Sample Scan parameter.
 *
 * Parameters: null
 *
 * Returns: void
 */
void ColumnTableSample::resetVecSampleScan()
{
    currentCuId = 0;
    batchRowCount = 0;

    /* Reset common parameters for table sample. */
    (((ColumnTableSample*)vecsampleScanState->sampleScanInfo.tsm_state)->resetSampleScan)();

    if (tids) {
        tids->Reset();
    }

    if (offsetIds) {
        errno_t rc = memset_s(offsetIds, sizeof(uint16) * BatchMaxSize, 0, sizeof(uint16) * BatchMaxSize);
        securec_check(rc, "", "");
    }
}

/*
 * Description: Get current block max offset.
 *
 * Parameters: null
 *
 * Returns: void
 */
void ColumnTableSample::getMaxOffset()
{
    CUDesc cu_desc;
    int fstColIdx = 0;
    Assert(BlockNumberIsValid(currentBlock));
    curBlockMaxoffset = InvalidOffsetNumber;

    /* If the first column has dropped, we should change the index of first column. */
    if (vecsampleScanState->ss_currentRelation->rd_att->attrs[0]->attisdropped) {
        fstColIdx = CStoreGetfstColIdx(vecsampleScanState->ss_currentRelation);
    }

    /*
     * Get CUDesc of column according to currentCuId.
     */
    if (vecsampleScanState->m_CStore->GetCUDesc(fstColIdx, currentCuId, &cu_desc, GetActiveSnapshot()) != true) {
        return;
    }

    /*
     * We try our best to keep the rules of acquiring tuples about row relations:
     * 1). ignore to sample tuples dead
     * 2). ignore to sample tuples recently dead
     * 3). ignore to sample tuples being inserted in progress by other transactions
     * 4). ignore to sample tuples being deleted in progress by our transactions
     * 5). ignore to sample tuples being deleted in progress by other transactions
     *  SnapshotNow can satisfy the rule 1) 2) 3) 4), so it's used here.
     */
    vecsampleScanState->m_CStore->GetCUDeleteMaskIfNeed(currentCuId, GetActiveSnapshot());

    /* Quit this loop quickly if all the tuples are dead in this CU unit. */
    if (vecsampleScanState->m_CStore->IsTheWholeCuDeleted(cu_desc.row_count)) {
        return;
    }

    curBlockMaxoffset = cu_desc.row_count;
}

/*
 * Description: Get sample VectorBatch by tids(CuId+offsetId).
 *
 * Parameters:
 *	@in state: CStoreScanState information
 *	@in cuId:  CuId of current CU
 *	@in maxOffset: max Offset of current CU
 *	@in offsetIds: random offsetIds of current CU
 *	@in tids: construct VectorBatch of tids by cuId and offsetIds
 *	@in vbout: return values of VectorBatch
 *
 * Returns: void
 */
void ColumnTableSample::getBatchBySamples(VectorBatch* vbout)
{
    ScalarVector* vec = tids->m_arr;
    tids->Reset();

    /* Fill VectorBatch of tids with CuId and offsetId. */
    for (int j = 0; j < batchRowCount; j++) {
        /* We can be sure it is not dead row. */
        vec->m_vals[j] = 0;
        ItemPointer itemPtr = (ItemPointer)&vec->m_vals[j];

        /* Note that itemPtr->offset start from 1 */
        ItemPointerSet(itemPtr, currentCuId, offsetIds[j]);
    }
    vec->m_rows = batchRowCount;
    tids->m_rows = vec->m_rows;

    /* Scan VectorBatch by tids. */
    if (!BatchIsNull(tids)) {
        CStoreIndexScanState* indexScanState = makeNode(CStoreIndexScanState);
        indexScanState->m_indexOutAttrNo = 0;

        vecsampleScanState->m_CStore->ScanByTids(indexScanState, tids, vbout);
        vecsampleScanState->m_CStore->ResetLateRead();
    }
}

/*
 * Description: Scan each offsets and get sample VectorBatch by tids.
 *
 * Parameters: @in pOutBatch: return values of VectorBatch
 *
 * Returns: ScanValid (the flag which identify the tuple is valid or not)
 */
ScanValid ColumnTableSample::scanBatch(VectorBatch* pOutBatch)
{
    Assert(BlockNumberIsValid(currentBlock));

    /* Current block alreadly have be readed.*/
    if (currentOffset == InvalidOffsetNumber) {
        if (batchRowCount > 0) {
            /*
             * If we get here, it means we've exhausted the items on this CU and
             * it's time to move to the next CU.
             */
            getBatchBySamples(pOutBatch);

            errno_t rc = memset_s(offsetIds, sizeof(uint16) * BatchMaxSize, 0, sizeof(uint16) * BatchMaxSize);
            securec_check(rc, "", "");
        }

        return INVALIDOFFSET;
    }

    if (!vecsampleScanState->m_CStore->IsDeadRow(currentCuId, (uint32)currentOffset)) {
        elog(DEBUG2,
            "Get one tuple [currentCuId: %u, currentOffset: %u] for relation: %s on %s.",
            currentCuId,
            currentOffset,
            NameStr(vecsampleScanState->ss_currentRelation->rd_rel->relname),
            g_instance.attr.attr_common.PGXCNodeName);

        /* Get current row from CU and fill into vector until to finish one batch. */
        offsetIds[batchRowCount++] = currentOffset;
        if (batchRowCount >= BatchMaxSize) {
            getBatchBySamples(pOutBatch);

            batchRowCount = 0;
            errno_t rc = memset_s(offsetIds, sizeof(uint16) * BatchMaxSize, 0, sizeof(uint16) * BatchMaxSize);
            securec_check(rc, "", "");

            return VALIDDATA;
        }
    }

    return NEXTDATA;
}

/*
 * Description: Get sample VectoBatch for column table.
 *
 * Parameters:
 *	@in pOutBatch: return values of VectorBatch
 *
 * Returns: void
 */
void ColumnTableSample::scanVecSample(VectorBatch* pOutBatch)
{
    /* Return NULL if finish scan or percent value is 0. */
    if ((finished == true) || (vecsampleScanState->sampleScanInfo.sampleType == BERNOULLI_SAMPLE && percent[0] == 0) || 
        (vecsampleScanState->sampleScanInfo.sampleType == SYSTEM_SAMPLE && percent[0] == 0) || 
        (vecsampleScanState->sampleScanInfo.sampleType == HYBRID_SAMPLE && percent[BERNOULLI_SAMPLE] == 0 && 
            percent[SYSTEM_SAMPLE] == 0)) {
        return;
    }

    for (;;) {
        CHECK_FOR_INTERRUPTS();

        switch (runState) {
            case GETMAXBLOCK: {
                /* Get num of max CU. */
                totalBlockNum = CStoreRelGetCUNumByNow((CStoreScanDesc)vecsampleScanState);
                runState = GETBLOCKNO;
                elog(DEBUG2,
                    "Get %u CUs for relation: %s on %s.",
                    totalBlockNum,
                    NameStr(vecsampleScanState->ss_currentRelation->rd_rel->relname),
                    g_instance.attr.attr_common.PGXCNodeName);
                break;
            }
            case GETBLOCKNO: {
                /* Get random or sequence CUId as current block. */
                (this->*nextSampleBlock_function)();

                if (!BlockNumberIsValid(currentBlock)) {
                    /* All block alreadly be scaned finish.*/
                    finished = true;
                    return;
                }

                currentCuId = currentBlock + FirstCUID + 1;
                runState = GETMAXOFFSET;
                break;
            }
            case GETMAXOFFSET: {
                getMaxOffset();

                if (InvalidOffsetNumber == curBlockMaxoffset) {
                    runState = GETBLOCKNO;
                } else {
                    runState = GETOFFSET;
                }

                elog(DEBUG2,
                    "Get %d tuples in CUNo: %u for relation: %s on %s.",
                    curBlockMaxoffset,
                    currentBlock,
                    NameStr(vecsampleScanState->ss_currentRelation->rd_rel->relname),
                    g_instance.attr.attr_common.PGXCNodeName);
                break;
            }
            case GETOFFSET: {
                (this->*nextSampleTuple_function)();

                runState = GETDATA;
                break;
            }
            case GETDATA: {
                ScanValid scanState = scanBatch(pOutBatch);

                switch (scanState) {
                    case VALIDDATA: {
                        runState = GETOFFSET;
                        return;
                    }
                    case NEXTDATA: {
                        runState = GETOFFSET;
                        break;
                    }
                    case INVALIDOFFSET: {
                        runState = GETBLOCKNO;

                        /* Return the last batch if filled and get new CU and batch. */
                        if (batchRowCount > 0) {
                            batchRowCount = 0;
                            return;
                        }
                        break;
                    }
                    default: {
                        break;
                    }
                }
                break;
            }
            default: {
                break;
            }
        }
    }
}

static double sample_random_fract(void)
{
    return ((double)gs_random() + 1) / ((double)MAX_RANDOM_VALUE + 2);
}
