/*
* Copyright (c) 2024 Huawei Technologies Co.,Ltd.
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
 * ---------------------------------------------------------------------------------------
 *
 * imIMCStore_am.cpp
 *      routines to support InMemoryColStore
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/htap/imIMCStore_am.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include <fcntl.h>
#include <sys/file.h>
#include "postgres.h"
#include "knl/knl_variable.h"
#include "access/tableam.h"
#include "access/tuptoaster.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "catalog/indexing.h"
#include "utils/aiomem.h"
#include "utils/fmgroids.h"
#include "utils/snapmgr.h"
#include "utils/datum.h"
#include "utils/relcache.h"
#include "pgxc/redistrib.h"
#include "pgstat.h"
#include "catalog/pg_type.h"
#include "access/htap/imcstore_am.h"
#include "storage/custorage.h"
#include "storage/remote_read.h"
#include "utils/builtins.h"
#include "access/nbtree.h"
#include "utils/numeric.h"
#include "utils/numeric_gs.h"
#include "access/htap/imcucache_mgr.h"
#include "access/htap/imcstore_delta.h"
#include "storage/cstore/cstore_compress.h"
#include "storage/smgr/smgr.h"
#include "storage/file/fio_device.h"
#include "access/heapam.h"
#include "access/sysattr.h"
#include "executor/instrument.h"
#include "catalog/pg_partition_fn.h"
#include "access/tableam.h"
#include "utils/date.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "vecexecutor/vecnodes.h"
#include "vecexecutor/vecnoderowtovector.h"
#include "access/cstore_roughcheck_func.h"
#include "utils/snapmgr.h"
#include "catalog/storage.h"
#include "miscadmin.h"
#include "access/htup.h"
#include "access/cstore_rewrite.h"
#include "replication/dataqueue.h"
#include "securec_check.h"
#include "commands/tablespace.h"
#include "workload/workload.h"
#include "executor/executor.h"

IMCStore::IMCStore()
    : m_imcuStorage(NULL),
      m_imcstoreDesc(NULL),
      m_isCtidCU(false),
      m_ctidCol(0),
      m_deltaMaskMax(0),
      m_deltaScanCurr(0)
{
    m_isImcstore = true;
}

void IMCStore::InitScan(CStoreScanState* state, Snapshot snapshot)
{
    Assert(state && state->ps.ps_ProjInfo);

    // first of all, create the private memonry context
    m_scanMemContext = AllocSetContextCreate(CurrentMemoryContext,
                                             "IMCStore scan memory context",
                                             ALLOCSET_DEFAULT_MINSIZE,
                                             ALLOCSET_DEFAULT_INITSIZE,
                                             ALLOCSET_DEFAULT_MAXSIZE);
    m_perScanMemCnxt = AllocSetContextCreate(CurrentMemoryContext,
                                             "IMCStore scan per scan memory context",
                                             ALLOCSET_DEFAULT_MINSIZE,
                                             ALLOCSET_DEFAULT_INITSIZE,
                                             ALLOCSET_DEFAULT_MAXSIZE);

    m_scanFunc = &CStore::CStoreScan;

    // the following spaces will live until deconstructor is called.
    // so use m_scanMemContext which is not freed at all until the end.
    AutoContextSwitch newMemCnxt(m_scanMemContext);
    m_relation = state->ss_currentRelation;

    m_imcstoreDesc = IMCU_CACHE->GetImcsDesc(RelationGetRelid(m_relation));
    UnlockRowGroups();
    u_sess->imcstore_ctx.pinnedRowGroups = NIL;
    m_ctidCol = m_imcstoreDesc->imcsNatts;
    m_deltaScanCurr = 0;
    m_deltaMaskMax = 0;

    m_imcuStorage = (IMCUStorage**)palloc(sizeof(IMCUStorage*) * (m_imcstoreDesc->imcsNatts + 1));
    for (int i = 0; i < m_imcstoreDesc->imcsNatts + 1; ++i) {     // with ctid
        m_firstColIdx = i;
        // Here we must use physical column id
        CFileNode cFileNode(m_relation->rd_node, i, MAIN_FORKNUM);
        m_imcuStorage[i] = New(CurrentMemoryContext) IMCUStorage(cFileNode);
    }

    m_CUDescIdx = (int*)palloc(sizeof(int) * u_sess->attr.attr_storage.max_loaded_cudesc);
    errno_t rc = memset_s((char*)m_CUDescIdx,
                          sizeof(int) * u_sess->attr.attr_storage.max_loaded_cudesc,
                          0xFF,
                          sizeof(int) * u_sess->attr.attr_storage.max_loaded_cudesc);
    securec_check(rc, "\0", "\0");
    m_cursor = 0;
    m_colNum = 0;
    m_NumCUDescIdx = 0;
    m_rowCursorInCU = 0;
    m_prefetch_quantity = 0;
    m_prefetch_threshold =
        Min(IMCU_CACHE->m_cstoreMaxSize / 4, u_sess->attr.attr_storage.cstore_prefetch_quantity * 1024LL);
    m_snapshot = snapshot;
    m_rangeScanInRedis = state->rangeScanInRedis;

    SetScanRange();

    InitFillVecEnv(state);

    InitRoughCheckEnv(state);

    /* remember node id of this plan */
    m_plan_node_id = state->ps.plan->plan_node_id;
}

IMCStore::~IMCStore()
{
    m_fillVectorLateRead = NULL;
    m_scanPosInCU = NULL;
    m_colId = NULL;
    m_lateRead = NULL;
    m_scanMemContext = NULL;
    m_snapshot = NULL;
    m_fillVectorByTids = NULL;
    m_virtualCUDescInfo = NULL;
    m_CUDescInfo = NULL;
    m_perScanMemCnxt = NULL;
    m_RCFuncs = NULL;
    m_CUDescIdx = NULL;
    m_colFillFunArrary = NULL;
    m_imcuStorage = NULL;
    m_relation = NULL;
    m_fillMinMaxFunc = NULL;
    m_sysColId = NULL;
    m_imcstoreDesc = NULL;
    m_cuDelMask = NULL;
}

void IMCStore::Destroy()
{
    UnlockRowGroups();
    if (m_imcstoreDesc != NULL) {
        if (m_imcuStorage) {
            for (int i = 0; i < m_imcstoreDesc->imcsNatts + 1; ++i) {  // with ctid
                if (m_imcuStorage[i]) {
                    DELETE_EX(m_imcuStorage[i]);
                }
            }
        }

        // only access sys columns or const columns
        if (OnlySysOrConstCol()) {
            Assert(m_virtualCUDescInfo);
            DELETE_EX(m_virtualCUDescInfo);
        }

        if (m_CUDescInfo) {
            for (int i = 0; i < m_colNum; ++i) {
                DELETE_EX(m_CUDescInfo[i]);
            }
        }

        /*
         * Important:
         * 1. all objects by NEW() must be freed by DELETE_EX() above;
         * 2. all spaces by palloc()/palloc0() can be freed either pfree() or deleting
         * these memory context following.
         */
        Assert(m_scanMemContext && m_perScanMemCnxt);
        MemoryContextDelete(m_perScanMemCnxt);
        MemoryContextDelete(m_scanMemContext);
    }
}

/* this function will return false iif batch is full */
bool IMCStore::InsertDeltaRowToBatch(
    _in_ IMCStoreScanState* state, ItemPointerData item, _out_ VectorBatch* vecBatchOut)
{
    char tuplebuf[BLCKSZ];
    Buffer buf = InvalidBuffer;

    ExprContext* econtext = state->ps.ps_ExprContext;
    TupleTableSlot* slot = state->ss_ScanTupleSlot;

    if (RelationIsUstoreFormat(m_relation)) {
        UHeapTuple tuple = (UHeapTuple)(&tuplebuf);
        tuple->disk_tuple = (UHeapDiskTuple)(tuplebuf + UHeapTupleDataSize);
        tuple->tupTableType = UHEAP_TUPLE;
        if (UHeapFetch(m_relation, m_snapshot, &item, tuple, &buf, false, false)) {
            ExecStoreTuple(tuple, slot, buf, false);
            ReleaseBuffer(buf);
        } else {
            return true;
        }
    } else {
        // we dont use get_tuple function here, because we don't want palloc extra space here
        HeapTuple tuple = (HeapTuple)(&tuplebuf);
        tuple->tupTableType = HEAP_TUPLE;
        tuple->t_self = item;
        tuple->t_data = (HeapTupleHeader)(tuplebuf + HEAPTUPLESIZE);
        if (heap_fetch(m_relation, m_snapshot, tuple, &buf, false, NULL)) {
            ExecStoreTuple(tuple, slot, buf, false);
            ReleaseBuffer(buf);
        } else {
            return true;
        }
    }

    /* here is different with cstore, we dont have index filter here */
    /* put the tuple into out batch */
    if (FillOneDeltaTuple(state, vecBatchOut, slot, econtext->ecxt_per_tuple_memory)) {
        (void)ExecClearTuple(slot);
        return false;
    }
    (void)ExecClearTuple(slot);
    return true;
}

void IMCStore::FillPerRowGroupDelta(_in_ IMCStoreScanState* state, _in_ uint32 cuid, _out_ VectorBatch* vecBatchOut)
{
    GetCUDeleteMaskIfNeed(cuid, m_snapshot);

    ItemPointerData item;
    while (m_deltaScanCurr <= m_deltaMaskMax) {
        if (m_cuDeltaMask[m_deltaScanCurr >> 3] == 0) {
            // skip this byte
            m_deltaScanCurr = ((m_deltaScanCurr >> 3) + 1) * 8;
            continue;
        }
        if ((m_cuDeltaMask[m_deltaScanCurr >> 3] & (1 << (m_deltaScanCurr % 8))) == 0) {
            ++m_deltaScanCurr;
            continue;
        }
        BlockNumber blk = m_deltaScanCurr / MAX_POSSIBLE_ROW_PER_PAGE;
        OffsetNumber offset = m_deltaScanCurr - blk * MAX_POSSIBLE_ROW_PER_PAGE;
        blk += m_delMaskCUId * MAX_IMCS_PAGES_ONE_CU;
        ItemPointerSetBlockNumber(&item, blk);
        ItemPointerSetOffsetNumber(&item, offset);

        ++m_deltaScanCurr;
        if (!InsertDeltaRowToBatch(state, item, vecBatchOut)) {
            break;
        }
    }
}

bool IMCStore::ImcstoreFillByDeltaScan(_in_ CStoreScanState* state, _out_ VectorBatch* vecBatchOut)
{
    uint32 cuid;
    if (m_NumLoadCUDesc == 0) {
        cuid = m_endCUID;
    } else {
        int idx = m_CUDescIdx[m_cursor];
        CUDesc* cuDescPtr = m_CUDescInfo[0]->cuDescArray + idx;
        cuid = cuDescPtr->cu_id;
    }

    if (m_delMaskCUId == cuid && m_deltaScanCurr > m_deltaMaskMax) {
        return false;
    }

    for (uint32 currid = m_delMaskCUId == InValidCUID ? 0 : m_delMaskCUId; currid <= cuid; ++currid) {
        if (u_sess->stream_cxt.producer_dop > 1 &&
            (currid % u_sess->stream_cxt.producer_dop != (uint32)u_sess->stream_cxt.smp_id))
            continue;
        FillPerRowGroupDelta((IMCStoreScanState*)state, currid, vecBatchOut);

        if (!BatchIsNull(vecBatchOut)) {
            return true;
        }
    }

    if (BatchIsNull(vecBatchOut)) {
        return false;
    }
    return true;
}

/*
 * @Description: Set CU range for Range Scan In Redistribute
 *
 * @return: void
 */
void IMCStore::SetScanRange()
{
    uint32 maxCuId = IMCStore::GetMaxCUID(m_imcstoreDesc);

    m_startCUID = 0;
    m_endCUID = maxCuId;
}

void IMCStore::InitPartReScan(Relation rel)
{
    Assert(m_imcuStorage);

    // change to the new partition relation.
    m_relation = rel;
    UnlockRowGroups();
    for (int i = 0; i < m_imcstoreDesc->imcsNatts + 1; ++i) {
        if (m_imcuStorage[i]) {
            DELETE_EX(m_imcuStorage[i]);
        }
    }
    m_imcstoreDesc = IMCU_CACHE->GetImcsDesc(RelationGetRelid(m_relation));
    m_ctidCol = m_imcstoreDesc->imcsNatts;
    m_deltaScanCurr = 0;
    m_deltaMaskMax = 0;

    // the following spaces will live until deconstructor is called.
    // so use m_scanMemContext which is not freed at all until the end.
    AutoContextSwitch newMemCnxt(m_scanMemContext);

    m_imcuStorage = (IMCUStorage**)repalloc(m_imcuStorage, sizeof(IMCUStorage*) * (m_imcstoreDesc->imcsNatts + 1));
    // because new partition has different file handler, so we must
    // destroy the old *m_cuStorage*, which will close the open fd,
    // and then create an new object for next partition.
    for (int i = 0; i < m_imcstoreDesc->imcsNatts + 1; ++i) {
        // Here we must use physical column id
        CFileNode cFileNode(m_relation->rd_node, i, MAIN_FORKNUM);
        m_imcuStorage[i] = New(CurrentMemoryContext) IMCUStorage(cFileNode);
    }
}

uint32 IMCStore::GetMaxCUID(IMCSDesc* imcstoreDesc)
{
    if (imcstoreDesc == NULL) {
        return 0;
    }
    return imcstoreDesc->curMaxRowGroupId;
}

/*
 * Load CUDesc information of column according to loadInfoPtr
 * LoadCUDescCtrl include maxCUDescNum for this load, because if we load all
 * it need big memory to hold
 * this function is special for adio, third param adio_work control adio like enable_adio_function.
 * because GetLivedRowNumbers should not work in adio model
 */
bool IMCStore::LoadCUDesc(
    _in_ int col, __inout LoadCUDescCtl* loadCUDescInfoPtr, _in_ bool prefetch_control, _in_ Snapshot snapShot)
{
    errno_t rc = EOK;
    bool found = false;

    Assert(col >= 0);
    Assert(loadCUDescInfoPtr);
    if (col >= m_relation->rd_att->natts) {
        ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
            errmsg("col index exceed col number, col:%d, number:%d", col, m_relation->rd_att->natts)));
    }
    /*
     * we will reset m_perScanMemCnxt when switch to the next batch of cudesc data.
     * so the spaces only used for this batch should be managed by m_perScanMemCnxt.
     */
    AutoContextSwitch newMemCnxt(m_perScanMemCnxt);

    ADIO_RUN()
    {
        loadCUDescInfoPtr->lastLoadNum = loadCUDescInfoPtr->curLoadNum;
    }
    ADIO_ELSE()
    {
        loadCUDescInfoPtr->lastLoadNum = 0;
        loadCUDescInfoPtr->curLoadNum = 0;
    }
    ADIO_END();

    UnlockRowGroups();
    if (loadCUDescInfoPtr->nextCUID > m_endCUID) {
        return false;
    }

    CUDesc* cuDescArray = loadCUDescInfoPtr->cuDescArray;
    bool needLengthInfo = false;
    if (col > 0) {
        needLengthInfo = m_relation->rd_att->attrs[col].attlen < 0;
    }

    for (uint32 cuid = loadCUDescInfoPtr->nextCUID; cuid <= m_endCUID && loadCUDescInfoPtr->HasFreeSlot(); ++cuid) {
        /* Parallel scan CU divide. */
        if (u_sess->stream_cxt.producer_dop > 1 &&
            (cuid % u_sess->stream_cxt.producer_dop != (uint32)u_sess->stream_cxt.smp_id))
            continue;

        RowGroup* rowgroup = m_imcstoreDesc->GetRowGroup(cuid);
        if (rowgroup != NULL) {
            pthread_rwlock_rdlock(&rowgroup->m_mutex);
            MemoryContext old = MemoryContextSwitchTo(m_scanMemContext);
            PinnedRowGroup* pinned = New(CurrentMemoryContext)PinnedRowGroup(rowgroup, m_imcstoreDesc);
            u_sess->imcstore_ctx.pinnedRowGroups = lappend(u_sess->imcstore_ctx.pinnedRowGroups, pinned);
            MemoryContextSwitchTo(old);
        }
        char* valPtr = NULL;
        if (rowgroup == NULL || !rowgroup->m_actived) {
            continue;
        }

        int imcsColIndex = m_imcstoreDesc->attmap[col];
        Assert(imcsColIndex >= 0);

        cuDescArray[loadCUDescInfoPtr->curLoadNum] = *rowgroup->m_cuDescs[imcsColIndex];
        loadCUDescInfoPtr->nextCUID = cuid;

        /* Put min value into cudesc->min */
        char* minPtr = cuDescArray[loadCUDescInfoPtr->curLoadNum].cu_min;
        int len_1 = MIN_MAX_LEN;
        valPtr = rowgroup->m_cuDescs[imcsColIndex]->cu_min;
        if (needLengthInfo) {
            *minPtr = valPtr[0];
            minPtr = minPtr + 1;
            len_1 -= 1;
            rc = memcpy_s(minPtr, len_1, valPtr + 1, valPtr[0]);
        } else {
            rc = memcpy_s(minPtr, len_1, valPtr, MIN_MAX_LEN);
        }
        securec_check(rc, "", "");

        /* Put max value into cudesc->max */
        char* maxPtr = cuDescArray[loadCUDescInfoPtr->curLoadNum].cu_max;
        int len_2 = MIN_MAX_LEN;
        valPtr = rowgroup->m_cuDescs[imcsColIndex]->cu_max;
        if (needLengthInfo) {
            *maxPtr = valPtr[0];
            maxPtr = maxPtr + 1;
            len_2 -= 1;
            rc = memcpy_s(maxPtr, len_2, valPtr + 1, valPtr[0]);
        } else {
            rc = memcpy_s(maxPtr, len_2, valPtr, MIN_MAX_LEN);
        }
        securec_check(rc, "", "");

        found = true;
        IncLoadCuDescIdx(*(int*)&loadCUDescInfoPtr->curLoadNum);
    }

    if (found) {
        /* nextCUID must be greater than loaded cudesc */
        loadCUDescInfoPtr->nextCUID++;
        return true;
    }
    return false;
}

int FindRowIDByCtid(CU* cu, ItemPointer item, int rowNumber)
{
    int start = 0, end = rowNumber - 1;
    while (start <= end) {
        int mid = (start + end) / 2;
        ScalarValue val  = cu->GetValue<sizeof(ImcstoreCtid), false>(mid);
        // ImcstoreCtid* imcsctid = (ImcstoreCtid*)(&val)
        // &imcsctid->ctid
        ItemPointer curr = &(((ImcstoreCtid*)(&val))->ctid);
        int32 cmpres = ItemPointerCompare(curr, item);
        if (0 == cmpres) {
            return mid;
        }
        if (cmpres < 0) {
            start = mid + 1;
        } else {
            end = mid - 1;
        }
    }
    return -1;
}

void IMCStore::GetCUDeleteMaskIfNeed(_in_ uint32 cuid, _in_ Snapshot snapShot)
{
    if (m_delMaskCUId == cuid) {
        return;
    }
    m_hasDeadRow = false;
    m_delMaskCUId = cuid;
    errno_t rc = EOK;

    rc = memset_s(m_cuDelMask, MAX_IMCSTORE_DEL_BITMAP_SIZE * sizeof(unsigned char), 0,
        MAX_IMCSTORE_DEL_BITMAP_SIZE * sizeof(unsigned char));
    securec_check(rc, "", "");
    rc = memset_s(m_cuDeltaMask, MAX_IMCSTORE_DEL_BITMAP_SIZE * sizeof(unsigned char), 0,
        MAX_IMCSTORE_DEL_BITMAP_SIZE * sizeof(unsigned char));
    securec_check(rc, "", "");
    m_deltaScanCurr = 0;
    m_deltaMaskMax = 0;

    RowGroup* rowgroup = m_imcstoreDesc->GetRowGroup(cuid);
    if (rowgroup == NULL) {
        return;
    }

    CU* cu = NULL;
    pthread_rwlock_rdlock(&rowgroup->m_mutex);
    int slotId = CACHE_BLOCK_INVALID_IDX;
    if (rowgroup->m_cuDescs[m_ctidCol] != NULL) {
        m_isCtidCU = true;
        cu = GetCUData(rowgroup->m_cuDescs[m_ctidCol], m_ctidCol, sizeof(ImcstoreCtid), slotId);
        m_isCtidCU = false;
    }
    DeltaTableIterator deltaIter = rowgroup->m_delta->ScanInit();
    ItemPointer item = NULL;
    DeltaOperationType ctidtpye;

    while ((item = deltaIter.GetNext(&ctidtpye, nullptr)) != NULL) {
        BlockNumber blockoffset = ItemPointerGetBlockNumber(item) - cuid * MAX_IMCS_PAGES_ONE_CU;
        OffsetNumber offset = ItemPointerGetOffsetNumber(item);
        uint64 idx = blockoffset * MAX_POSSIBLE_ROW_PER_PAGE + offset;
        m_cuDeltaMask[idx >> 3] |= (1 << (idx % 8));
        m_deltaMaskMax = Max(idx, m_deltaMaskMax);
        if (cu == NULL) {
            continue;
        }

        int rowIdx = FindRowIDByCtid(cu, item, rowgroup->m_cuDescs[m_ctidCol]->row_count);
        if (rowIdx < 0) {
            continue;
        }
        m_hasDeadRow = true;
        if ((rowIdx >> 3) > MAX_IMCSTORE_DEL_BITMAP_SIZE) {
            ereport(ERROR, (errmsg(
                "Try build delete mask for hatp error, row index exceeds MAX_IMCSTORE_DEL_BITMAP_SIZE")));
        }
        m_cuDelMask[rowIdx >> 3] |= (1 << (rowIdx % 8));
    }

    pthread_rwlock_unlock(&rowgroup->m_mutex);
    m_imcstoreDesc->UnReferenceRowGroup();
    if (IsValidCacheSlotID(slotId)) {
        IMCU_CACHE->UnPinDataBlock(slotId);
    }
    return;
}

// Put the CU in the cache and return a pointer to the CU data.
// The CU is returned pinned, callers must unpin it when finished.
// 1. Record a fetch (read).
// 2. Look for the CU in the cache via FindDataBlock() first.
//        This should succeed most of the time, and it is fast.
// 3. If FindDataBlock() cannot get the cu then use InsertCU().
// 4. If FindDataBlock() or InsertCU() discover the CU is already in the cache then
//        Record the cache hit, and return the CU buffer and the cache entry.
// 5. If InsertCU() does not find an entry, it reserves memory,
//      a CU decriptor slot, and a CU data slot.
// 6. Load the CU from disk and setup the CU data slot and Check the CRC.
// 7. Uncompress the CU data buffer, if necessary
// 8. Free the compressed buffer.
// 9. Update the memory reservation.
// 10.Resume the busy CUbuffer, wakeup any threads waiting for
//    the cache entry.
CU* IMCStore::GetCUData(CUDesc* cuDescPtr, int colIdx, int valSize, int& slotId)
{
    colIdx = m_isCtidCU ? m_ctidCol : m_imcstoreDesc->attmap[colIdx];

    /*
     * we will reset m_PerScanMemCnxt when switch to the next batch of cudesc data.
     * so the spaces only used for this batch should be managed by m_PerScanMemCnxt,
     * including the peices of space used in the decompression.
     */
    if (colIdx != m_ctidCol && m_relation->rd_att->attrs[colIdx].attisdropped) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_OPERATION),
                 (errmsg("Cannot get CUData for a dropped column \"%s\" of table \"%s\"",
                         NameStr(m_relation->rd_att->attrs[colIdx].attname),
                         RelationGetRelationName(m_relation)))));
    }

    AutoContextSwitch newMemCnxt(this->m_perScanMemCnxt);

    CU* cuPtr = NULL;
    FormData_pg_attribute* attrs = m_relation->rd_att->attrs;
    CUUncompressedRetCode retCode = CU_OK;
    bool hasFound = false;
    DataSlotTag dataSlotTag = IMCU_CACHE->InitCUSlotTag(
        (RelFileNodeOld *)&m_relation->rd_node, colIdx, cuDescPtr->cu_id, cuDescPtr->cu_pointer);

    // Record a fetch (read).
    // The fetch count is the sum of the hits and reads.
    if (m_rowCursorInCU == 0) {
        pgstat_count_buffer_read(m_relation);
    }

RETRY_LOAD_CU:

    // Look for the CU in the cache first, this is quick and
    // should succeed most of the time.
    slotId = IMCU_CACHE->FindDataBlock(&dataSlotTag, (m_rowCursorInCU == 0));

    // If the CU is not in the cache, reserve it.
    // Get a cache slot, reserve memory, and put it in the hashtable.
    // ReserveDataBlock() may block waiting for space or CU Cache slots
    if (IsValidCacheSlotID(slotId)) {
        hasFound = true;
    } else {
        hasFound = false;
        slotId = IMCU_CACHE->ReserveDataBlock(&dataSlotTag, cuDescPtr->cu_size, hasFound);
    }

    // Use the cached CU
    cuPtr = IMCU_CACHE->GetCUBuf(slotId);
    cuPtr->m_inCUCache = true;
    if (colIdx == m_ctidCol) {
        cuPtr->SetAttInfo(valSize, -1, TIDOID);
    } else {
        cuPtr->SetAttInfo(valSize, attrs[colIdx].atttypmod, attrs[colIdx].atttypid);
    }

    // If the CU was already in the cache, return it.
    if (hasFound) {
        // Wait for a read to complete, if still in progress
        if (IMCU_CACHE->DataBlockWaitIO(slotId)) {
            IMCU_CACHE->UnPinDataBlock(slotId);
            ereport(LOG,
                    (errmodule(MOD_CACHE),
                     errmsg("CU wait IO find an error, need to reload! table(%s), column(%s), relfilenode(%u/%u/%u), "
                            "cuid(%u)",
                            RelationGetRelationName(m_relation),
                            (colIdx == m_ctidCol) ? "ctid" : NameStr(m_relation->rd_att->attrs[colIdx].attname),
                            m_relation->rd_node.spcNode,
                            m_relation->rd_node.dbNode,
                            m_relation->rd_node.relNode,
                            cuDescPtr->cu_id)));
            goto RETRY_LOAD_CU;
        }

        // when IMCStore scan first access CU, count mem_hit
        if (m_rowCursorInCU == 0) {
            // Record cache hit.
            pgstat_count_buffer_hit(m_relation);
            // stat CU SSD hit
            pgstatCountCUMemHit4SessionLevel();
            pgstat_count_cu_mem_hit(m_relation);
        }

        if (!cuPtr->m_cache_compressed) {
            CheckConsistenceOfCUData(cuDescPtr, cuPtr, (AttrNumber)(colIdx + 1));
            return cuPtr;
        }
        if (cuPtr->m_cache_compressed) {
            retCode = IMCU_CACHE->StartUncompressCU(
                cuDescPtr, slotId, this->m_plan_node_id, this->m_timing_on, ALIGNOF_CUSIZE);
            if (retCode == CU_RELOADING) {
                IMCU_CACHE->UnPinDataBlock(slotId);
                ereport(LOG, (errmodule(MOD_CACHE),
                              errmsg("The CU is being reloaded by remote read thread. Retry to load CU! table(%s), "
                                     "column(%s), relfilenode(%u/%u/%u), cuid(%u)",
                                     RelationGetRelationName(m_relation),
                                     (colIdx == m_ctidCol)
                                     ? "ctid"
                                     : NameStr(m_relation->rd_att->attrs[colIdx].attname),
                                     m_relation->rd_node.spcNode, m_relation->rd_node.dbNode,
                                     m_relation->rd_node.relNode, cuDescPtr->cu_id)));
                goto RETRY_LOAD_CU;
            } else if (retCode == CU_ERR_ADIO) {
                ereport(ERROR,
                        (errcode(ERRCODE_IO_ERROR),
                         errmodule(MOD_ADIO),
                         errmsg("Load CU failed in adio! table(%s), column(%s), relfilenode(%u/%u/%u), cuid(%u)",
                                RelationGetRelationName(m_relation),
                                (colIdx == m_ctidCol) ? "ctid" : NameStr(m_relation->rd_att->attrs[colIdx].attname),
                                m_relation->rd_node.spcNode,
                                m_relation->rd_node.dbNode,
                                m_relation->rd_node.relNode,
                                cuDescPtr->cu_id)));
            } else if (retCode == CU_ERR_CRC || retCode == CU_ERR_MAGIC) {
                /* Prefech CU contains incorrect checksum */
                addBadBlockStat(&m_imcuStorage[colIdx]->m_cnode.m_rnode,
                    ColumnId2ColForkNum(m_imcuStorage[colIdx]->m_cnode.m_attid));

                // unlogged table can not remote read
                IMCU_CACHE->TerminateCU(true);
                ereport(ERROR,
                        (errcode(ERRCODE_DATA_CORRUPTED),
                         (errmsg("invalid CU in cu_id %u of relation %s file %s offset %lu, prefetch %s",
                                 cuDescPtr->cu_id,
                                 RelationGetRelationName(m_relation),
                                 relcolpath(m_imcuStorage[colIdx]),
                                 cuDescPtr->cu_pointer,
                                 GetUncompressErrMsg(retCode)),
                          errdetail("Can not load imcu. Should unpopulate table and re-populate"
                                    "data."),
                          handle_in_client(true))));
            } else {
                Assert(retCode == CU_OK);
            }
        }

        CheckConsistenceOfCUData(cuDescPtr, cuPtr, (AttrNumber)(colIdx + 1));
        return cuPtr;
    }

    // stat CU hdd sync read
    pgstatCountCUHDDSyncRead4SessionLevel();
    pgstat_count_cu_hdd_sync(m_relation);

    m_imcuStorage[colIdx]->LoadCU(
        cuPtr, cuDescPtr->cu_id, cuDescPtr->cu_size, m_imcstoreDesc);

    ADIO_RUN()
    {
        ereport(DEBUG1,
                (errmodule(MOD_ADIO),
                 errmsg("GetCUData:relation(%s), colIdx(%d), load cuid(%u), slotId(%d)",
                        RelationGetRelationName(m_relation),
                        colIdx,
                        cuDescPtr->cu_id,
                        slotId)));
    }
    ADIO_END();

    // Mark the CU as no longer io busy, and wake any waiters
    IMCU_CACHE->DataBlockCompleteIO(slotId);

    retCode = IMCU_CACHE->StartUncompressCU(cuDescPtr, slotId, this->m_plan_node_id, this->m_timing_on, ALIGNOF_CUSIZE);
    if (retCode == CU_RELOADING) {
        IMCU_CACHE->UnPinDataBlock(slotId);
        ereport(LOG,
                (errmodule(MOD_CACHE),
                 errmsg("The CU is being reloaded by remote read thread. Retry to load CU! table(%s), column(%s), "
                        "relfilenode(%u/%u/%u), cuid(%u)",
                        RelationGetRelationName(m_relation),
                        (colIdx == m_ctidCol) ? "ctid" : NameStr(m_relation->rd_att->attrs[colIdx].attname),
                        m_relation->rd_node.spcNode,
                        m_relation->rd_node.dbNode,
                        m_relation->rd_node.relNode,
                        cuDescPtr->cu_id)));
        goto RETRY_LOAD_CU;
    } else if (retCode == CU_ERR_CRC || retCode == CU_ERR_MAGIC) {
        /* Sync load CU contains incorrect checksum */
        addBadBlockStat(
            &m_imcuStorage[colIdx]->m_cnode.m_rnode, ColumnId2ColForkNum(m_imcuStorage[colIdx]->m_cnode.m_attid));

        // unlogged table can not remote read
        IMCU_CACHE->TerminateCU(true);
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                        (errmsg("invalid CU in cu_id %u of relation %s file %s offset %lu, sync load %s",
                                cuDescPtr->cu_id,
                                RelationGetRelationName(m_relation), relcolpath(m_imcuStorage[colIdx]),
                                cuDescPtr->cu_pointer,
                                GetUncompressErrMsg(retCode)),
                         errdetail("Can not load imcu. Should unpopulate table and re-populate "
                                   "data."))));
    }

    Assert(retCode == CU_OK);

    if (t_thrd.vacuum_cxt.VacuumCostActive) {
        // cu cache misses, so we update vacuum stats
        t_thrd.vacuum_cxt.VacuumCostBalance += u_sess->attr.attr_storage.VacuumCostPageMiss;
    }

    CheckConsistenceOfCUData(cuDescPtr, cuPtr, (AttrNumber)(colIdx + 1));
    return cuPtr;
}

void UnlockRowGroups()
{
    if (!u_sess->imcstore_ctx.pinnedRowGroups) {
        return;
    }
    ListCell *lc = NULL;
    foreach(lc, u_sess->imcstore_ctx.pinnedRowGroups) {
        PinnedRowGroup* pinned = (PinnedRowGroup*)lfirst(lc);
        pthread_rwlock_unlock(&pinned->rowgroup->m_mutex);
        pinned->desc->UnReferenceRowGroup();
        delete pinned;
    }
    list_free_ext(u_sess->imcstore_ctx.pinnedRowGroups);
}
