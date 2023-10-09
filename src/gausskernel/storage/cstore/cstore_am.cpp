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
 * ---------------------------------------------------------------------------------------
 *
 * cstore_am.cpp
 *      routines to support ColStore
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/cstore/cstore_am.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"
#include <fcntl.h>
#include <sys/file.h>
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
#include "pgstat.h"
#include "catalog/pg_type.h"
#include "access/cstore_am.h"
#include "storage/custorage.h"
#include "storage/remote_read.h"
#include "utils/builtins.h"
#include "access/nbtree.h"
#include "utils/numeric.h"
#include "utils/numeric_gs.h"
#include "storage/cucache_mgr.h"
#include "storage/cstore/cstore_compress.h"
#include "storage/smgr/smgr.h"
#include "storage/file/fio_device.h"
#include "access/heapam.h"
#include "access/sysattr.h"
#include "executor/instrument.h"
#include "utils/date.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "access/heapam.h"
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

#ifdef PGXC
    #include "pgxc/pgxc.h"
    #include "pgxc/redistrib.h"
#endif

/* macro for tracing cstore scan */
#define CSTORESCAN_TRACE_START(_desc_id)                   \
    do {                                                   \
        if (unlikely(this->m_timing_on)) {                 \
            TRACK_START(this->m_plan_node_id, (_desc_id)); \
        }                                                  \
    } while (0)

#define CSTORESCAN_TRACE_END(_desc_id)                   \
    do {                                                 \
        if (unlikely(this->m_timing_on)) {               \
            TRACK_END(this->m_plan_node_id, (_desc_id)); \
        }                                                \
    } while (0)

#define CSTORE_MIN_PREFETCH_COUNT 8

#define InitFillColFunction(i, attlen)                                            \
    do {                                                                          \
        m_colFillFunArrary[i].colFillFun[0] = &CStore::FillVector<false, attlen>; \
        m_colFillFunArrary[i].colFillFun[1] = &CStore::FillVector<true, attlen>;  \
        m_fillVectorByTids[i] = &CStore::FillVectorByTids<attlen>;                \
        m_fillVectorLateRead[i] = &CStore::FillVectorLateRead<attlen>;            \
    } while (0)

CStore::CStore()
    : m_relation(NULL),
      m_scanMemContext(NULL),
      m_perScanMemCnxt(NULL),
      m_snapshot(NULL),
      m_colId(NULL),
      m_sysColId(NULL),
      m_lateRead(NULL),
      m_cuStorage(NULL),
      m_CUDescInfo(NULL),
      m_virtualCUDescInfo(NULL),
      m_CUDescIdx(NULL),
      m_lastNumCUDescIdx(0),
      m_prefetch_quantity(0),
      m_prefetch_threshold(0),
      m_load_finish(false),
      m_scanPosInCU(NULL),
      m_RCFuncs(NULL),
      m_fillVectorByTids(NULL),
      m_fillVectorLateRead(NULL),
      m_colFillFunArrary(NULL),
      m_fillMinMaxFunc(NULL),
      m_scanFunc(NULL),
      m_plan_node_id(-1),
      m_colNum(0),
      m_sysColNum(0),
      m_NumLoadCUDesc(0),
      m_NumCUDescIdx(0),
      m_delMaskCUId(InValidCUID),
      m_cursor(0),
      m_rowCursorInCU(0),
      m_startCUID(0),
      m_endCUID(0),
      m_hasDeadRow(false),
      m_needRCheck(false),
      m_onlyConstCol(false),
      m_timing_on(false),
      m_rangeScanInRedis({false,0,0}),
      m_useBtreeIndex(false),
      m_firstColIdx(0),
      m_cuDescIdx(-1),
      m_laterReadCtidColIdx(-1)
{
    // if you intend to allocate any space in cstore constructor/init scan function
    // please remind that you must put the space deallocate in the deconstructor function
    // do not rely on memory context reset
    // there will be memory leak due to cstore index rescan function.!!!!
}

/*
 * @Description: assign to function point according to different data type.
 * @in - proj: Projection information.
 */
void CStore::BindingFp(CStoreScanState* state)
{
    int i = 0;
    Relation rel = state->ss_currentRelation;
    ProjectionInfo* proj = state->ps.ps_ProjInfo;

    if (proj->pi_maxOrmin) {
        Assert(list_length(proj->pi_maxOrmin) == list_length(proj->pi_acessedVarNumbers));
        m_scanFunc = &CStore::CStoreMinMaxScan;
        m_fillMinMaxFunc = (fillMinMaxFuncPtr*)palloc0(sizeof(fillMinMaxFuncPtr) * m_colNum);

        for (i = 0; i < m_colNum; ++i) {
            switch (rel->rd_att->attrs[m_colId[i]].atttypid) {
                case CHAROID:
                case INT2OID:
                case INT4OID:
                case INT8OID:
                case OIDOID:
                case DATEOID:
                case TIMEOID:
                case TIMESTAMPOID: {
                    m_fillMinMaxFunc[i] = &CStore::FillColMinMax;
                    break;
                }
                default: {
                    m_fillMinMaxFunc[i] = NULL;
                    break;
                }
            }
        }
    }

    for (i = 0; i < m_colNum; ++i) {
        m_CUDescInfo[i] = New(CurrentMemoryContext) LoadCUDescCtl(m_startCUID);
        if (m_colId[i] > rel->rd_att->natts - 1 || m_colId[i] < 0) {
            continue;
        }
        switch (rel->rd_att->attrs[m_colId[i]].attlen) {
            case sizeof(char):
                InitFillColFunction(i, (int)sizeof(char));
                break;
            case sizeof(int16):
                InitFillColFunction(i, (int)sizeof(int16));
                break;
            case sizeof(int32):
                InitFillColFunction(i, (int)sizeof(int32));
                break;
            case sizeof(Datum):
                InitFillColFunction(i, (int)sizeof(Datum));
                break;
            case 12:
                InitFillColFunction(i, 12);
                break;
            case 16:
                InitFillColFunction(i, 16);
                break;
            case -1:
                InitFillColFunction(i, -1);
                break;
            case -2:
                InitFillColFunction(i, -2);
                break;
            default:
                ereport(ERROR,
                        (errcode(ERRCODE_DATATYPE_MISMATCH),
                         (errmsg("unsupported data type length %d of column \"%s\" of relation \"%s\" ",
                                 (int)rel->rd_att->attrs[m_colId[i]].attlen,
                                 NameStr(rel->rd_att->attrs[m_colId[i]].attname),
                                 RelationGetRelationName(rel)))));
                break;
        }
    }
}

void CStore::InitFillVecEnv(CStoreScanState* state)
{
    // the following spaces will live until deconstructor is called.
    // so use m_scanMemContext which is not freed at all until the end.
    AutoContextSwitch newMemCnxt(m_scanMemContext);

    ProjectionInfo* proj = state->ps.ps_ProjInfo;
    if (proj->pi_acessedVarNumbers != NIL) {
        List* pColList = proj->pi_acessedVarNumbers;

        m_colNum = list_length(pColList);
        m_colId = (int*)palloc(sizeof(int) * m_colNum);
        m_lateRead = (bool*)palloc0(sizeof(bool) * m_colNum);

        int i = 0;
        ListCell* cell = NULL;

        // Initilize which columns should be accessed
        foreach (cell, pColList) {
            // m_colIdx[] start from zero
            Assert(lfirst_int(cell) > 0);
            int colId = lfirst_int(cell) - 1;
            if (colId >= m_relation->rd_att->natts) {
                ereport(ERROR, (errcode(ERRCODE_UNDEFINED_COLUMN),
                    errmsg("column %d does not exist", colId)));
            }
            
            if (m_relation->rd_att->attrs[colId].attisdropped) {
                ereport(ERROR, (errcode(ERRCODE_UNDEFINED_COLUMN),
                    errmsg("column %s does not exist",
                    NameStr(m_relation->rd_att->attrs[colId].attname))));
            }
            m_colId[i] = colId;
            m_lateRead[i] = false;
            i++;
        }

        // Intilize which columns will be late read
        foreach (cell, proj->pi_lateAceessVarNumbers) {
            int colId = lfirst_int(cell) - 1;
            for (i = 0; i < m_colNum; ++i) {
                if (colId == m_colId[i]) {
                    m_lateRead[i] = true;
                    break;
                }
            }
        }

        m_scanPosInCU = (int*)palloc0(sizeof(int) * m_colNum);
        m_CUDescInfo = (LoadCUDescCtl**)palloc(sizeof(LoadCUDescCtl*) * m_colNum);
        m_colFillFunArrary = (colFillArray*)palloc(sizeof(colFillArray) * m_colNum);
        m_fillVectorByTids = (FillVectorByTidsFun*)palloc(sizeof(FillVectorByTidsFun) * m_colNum);
        m_fillVectorLateRead = (FillVectorLateReadFun*)palloc(sizeof(FillVectorLateReadFun) * m_colNum);

        BindingFp(state);
    }

    // Init sys columns
    if (proj->pi_sysAttrList != NIL) {
        ListCell* cell = NULL;
        List* pSysList = proj->pi_sysAttrList;
        m_sysColNum = list_length(pSysList);
        m_sysColId = (int*)palloc(sizeof(int) * m_sysColNum);
        int i = 0;
        foreach (cell, pSysList) {
            m_sysColId[i++] = lfirst_int(cell);
        }
    }

    m_onlyConstCol = proj->pi_const;

#ifdef USE_ASSERT_CHECKING
    if (m_onlyConstCol) {
        Assert(m_colNum == 0 && m_sysColNum == 0);
    }
#endif

    // only access sys columns or const columns
    if (OnlySysOrConstCol()) {
        m_virtualCUDescInfo = New(CurrentMemoryContext) LoadCUDescCtl(m_startCUID);
    }
}

void CStore::InitRoughCheckEnv(CStoreScanState* state)
{
    // the following spaces will live until deconstructor is called.
    // so use m_scanMemContext which is not freed at all until the end.
    AutoContextSwitch newMemCnxt(m_scanMemContext);

    // Initialize rough check function
    int nkeys = state->csss_NumScanKeys;
    if (nkeys > 0) {
        CStoreScanKey scanKey = state->csss_ScanKeys;
        Relation rel = state->ss_currentRelation;
        FormData_pg_attribute* attrs = rel->rd_att->attrs;

        m_RCFuncs = (RoughCheckFunc*)palloc(sizeof(RoughCheckFunc) * nkeys);
        for (int i = 0; i < nkeys; i++) {
            int colIdx = m_colId[scanKey[i].cs_attno];
            m_RCFuncs[i] = GetRoughCheckFunc(attrs[colIdx].atttypid, scanKey[i].cs_strategy, scanKey[i].cs_collation);
        }
    }
}

void CStore::InitScan(CStoreScanState* state, Snapshot snapshot)
{
    Assert(state && state->ps.ps_ProjInfo);

    // first of all, create the private memonry context
    m_scanMemContext = AllocSetContextCreate(CurrentMemoryContext,
                                             "cstore scan memory context",
                                             ALLOCSET_DEFAULT_MINSIZE,
                                             ALLOCSET_DEFAULT_INITSIZE,
                                             ALLOCSET_DEFAULT_MAXSIZE);
    m_perScanMemCnxt = AllocSetContextCreate(CurrentMemoryContext,
                                             "cstore scan per scan memory context",
                                             ALLOCSET_DEFAULT_MINSIZE,
                                             ALLOCSET_DEFAULT_INITSIZE,
                                             ALLOCSET_DEFAULT_MAXSIZE);

    m_scanFunc = &CStore::CStoreScan;

    // the following spaces will live until deconstructor is called.
    // so use m_scanMemContext which is not freed at all until the end.
    AutoContextSwitch newMemCnxt(m_scanMemContext);
    m_relation = state->ss_currentRelation;
    int attNo = m_relation->rd_att->natts;

    m_cuStorage = (CUStorage**)palloc(sizeof(CUStorage*) * attNo);

    for (int i = 0; i < attNo; ++i) {
        if (m_relation->rd_att->attrs[i].attisdropped) {
            m_cuStorage[i] = NULL;
            continue;
        }
        m_firstColIdx = i;
        // Here we must use physical column id
        CFileNode cFileNode(m_relation->rd_node, m_relation->rd_att->attrs[i].attnum, MAIN_FORKNUM);
        m_cuStorage[i] = New(CurrentMemoryContext) CUStorage(cFileNode);
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
        Min(CUCache->m_cstoreMaxSize / 4, u_sess->attr.attr_storage.cstore_prefetch_quantity * 1024LL);
    m_snapshot = snapshot;
    m_rangeScanInRedis = state->rangeScanInRedis;

    SetScanRange();

    InitFillVecEnv(state);

    InitRoughCheckEnv(state);

    /* remember node id of this plan */
    m_plan_node_id = state->ps.plan->plan_node_id;
}

CStore::~CStore()
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
    m_cuStorage = NULL;
    m_relation = NULL;
    m_fillMinMaxFunc = NULL;
    m_sysColId = NULL;
}

void CStore::Destroy()
{
    if (m_relation != NULL) {
        int attNo = m_relation->rd_att->natts;
        if (m_cuStorage) {
            for (int i = 0; i < attNo; ++i) {
                if (m_cuStorage[i])
                    DELETE_EX(m_cuStorage[i]);
                else {
                    Assert(m_relation->rd_att->attrs[i].attisdropped);
                    if (!m_relation->rd_att->attrs[i].attisdropped) {
                        ereport(WARNING, (errmsg("m_cuStorage[%d] is NULL for a valid column", i)));
                    }
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

/*
 * @Description:  prefetch one cu according by the given column
 * @Param[IN] col: column id
 * @Param[IN] count: prefetch cache count
 * @Param[IN] cudesc: cu describe
 * @Param[IN/OUT] dList: adio dispatch list
 * @See also:
 */
void CStore::CUPrefetch(CUDesc* cudesc, int col, AioDispatchCUDesc_t** dList, int& count, File* vfdList)
{
#ifndef ENABLE_LITE_MODE
    CU* cu_ptr = NULL;
    bool found = false;
    int slotId = CACHE_BLOCK_INVALID_IDX;
    AioDispatchCUDesc_t* aioDescp = NULL;

    // same value CU
    if (cudesc->cu_size == 0 || cudesc->IsNullCU() || cudesc->IsSameValCU()) {
        return;
    }

    /* it is better to check  all delete and do not prefetch, but actually load cu does not take care of it */
    uint64 load_offset = m_cuStorage[col]->GetAlignCUOffset(cudesc->cu_pointer);
    int head_padding_size = cudesc->cu_pointer - load_offset;
    int load_size = m_cuStorage[col]->GetAlignCUSize(head_padding_size + cudesc->cu_size);
    /* need check and add sitution cu store in many files,
        now if we found, jump the cu, we should think about more to deal with the CU in adio module */
    if (!m_cuStorage[col]->IsCUStoreInOneFile(load_offset, load_size)) {
        ereport(LOG,
                (errmodule(MOD_ADIO),
                 errmsg("CUPrefetch: skip cloumn(%d), cuid(%u), offset(%lu), size(%d)  ",
                        col,
                        cudesc->cu_id,
                        cudesc->cu_pointer,
                        cudesc->cu_size)));
        return;
    }

    DataSlotTag dataSlotTag = CUCache->InitCUSlotTag((RelFileNodeOld *)&m_relation->rd_node, col, cudesc->cu_id,
                                                     cudesc->cu_pointer);
    // find whether already in CUCache, ReserveDataBlock can also find CU in cache,
    // but i still add FindDataBlock here for efficient
    // here we ignore the enter block times
    slotId = CUCache->FindDataBlock(&dataSlotTag, false);
    if (IsValidCacheSlotID(slotId)) {
        ereport(DEBUG1,
                (errmodule(MOD_ADIO),
                 errmsg("prefetch find cu cache: relid(%u), column(%d), load cuid(%u)",
                        m_relation->rd_node.relNode,
                        col,
                        cudesc->cu_id)));
        CUCache->UnPinDataBlock(slotId);
        return;
    }

    slotId = CUCache->ReserveDataBlock(&dataSlotTag, cudesc->cu_size, found);
    if (found) {
        CUCache->UnPinDataBlock(slotId);
        return;
    }

    /* ReserveDataBlock, load_buf ,fd, offset allocate before adio_share_alloc becasue these can auto rollback */
    File file = m_cuStorage[col]->GetCUFileFd(load_offset);
    uint64 file_offset = m_cuStorage[col]->GetCUOffsetInFile(load_offset);

    aioDescp = (AioDispatchCUDesc_t*)adio_share_alloc(sizeof(AioDispatchCUDesc_t));

    cu_ptr = CUCache->GetCUBuf(slotId);
    Assert(cu_ptr);

    cu_ptr->m_head_padding_size = head_padding_size;
    cu_ptr->m_adio_error = false;
    cu_ptr->m_inCUCache = true;
    cu_ptr->m_compressedLoadBuf = (char*)CStoreMemAlloc::Palloc(load_size, false);
    cu_ptr->m_compressedBuf = cu_ptr->m_compressedLoadBuf + cu_ptr->m_head_padding_size;
    cu_ptr->m_compressedBufSize = cudesc->cu_size;
    cu_ptr->SetCUSize(cudesc->cu_size);
    cu_ptr->m_cache_compressed = true;

    /* iocb filled in later */
    aioDescp->aiocb.data = 0;
    aioDescp->aiocb.aio_fildes = 0;
    aioDescp->aiocb.aio_lio_opcode = 0;
    aioDescp->aiocb.u.c.buf = 0;
    aioDescp->aiocb.u.c.nbytes = 0;
    aioDescp->aiocb.u.c.offset = 0;

    aioDescp->cuDesc.buf = cu_ptr->m_compressedLoadBuf;
    aioDescp->cuDesc.offset = file_offset;
    aioDescp->cuDesc.size = load_size;
    aioDescp->cuDesc.fd = file;
    vfdList[count] = file;
    aioDescp->cuDesc.io_error = &cu_ptr->m_adio_error;
    aioDescp->cuDesc.slotId = slotId;  // slotId maybe CACHE_BLOCK_INVALID_IDX
    aioDescp->cuDesc.cu_pointer = cudesc->cu_pointer;
    aioDescp->cuDesc.reqType = CUListPrefetchType;
    aioDescp->aiocb.aio_reqprio = CompltrPriority(aioDescp->cuDesc.reqType);

    dList[count] = aioDescp;
    io_prep_pread((struct iocb*)dList[count],
                  aioDescp->cuDesc.fd,
                  aioDescp->cuDesc.buf,
                  aioDescp->cuDesc.size,
                  aioDescp->cuDesc.offset);
    count++;

    CUCache->TerminateCU(false);  // already record in dList[count]

    Assert(IsValidCacheSlotID(slotId));
    CUCache->UnPinDataBlock(slotId);
    CUCache->CULWLockDisown(slotId);

    ereport(DEBUG1,
            (errmodule(MOD_ADIO),
             errmsg("CUPrefetch: relid(%u), slotId(%d), col_id(%d), cu_id(%u), cu_size(%d), cu_point(%lu)",
                    m_relation->rd_node.relNode,
                    slotId,
                    col,
                    cudesc->cu_id,
                    cudesc->cu_size,
                    cudesc->cu_pointer)));

    /* check need submint io */
    if (count >= MAX_CU_PREFETCH_REQSIZ) {
        int tmp_count = count;

        HOLD_INTERRUPTS();
        FileAsyncCURead(dList, count);
        count = 0;
        RESUME_INTERRUPTS();

        FileAsyncCUClose(vfdList, tmp_count);
        // stat cu hdd asyn read
        pgstatCountCUHDDAsynRead4SessionLevel(tmp_count);
        pgstat_count_cu_hdd_asyn(m_relation, tmp_count);
    }
    return;
#endif
}

/*
 * @Description:  cstore scan use this api to prefetch, load CU in vector, preload, organize CU and CU cache,
 *  use io_in_process lock to protect io and leave uncompress for next scan, add flag to know which CU load by ADIO(need
 * uncompress) and when scan it, do crc check, decompress, free compressed buf, set flag no need again  aio completer
 * thread only do unlock io_in_process
 * @See also:
 */
void CStore::CUListPrefetch()
{
    /* cudesc not load, so no need prefetch */
    if (m_lastNumCUDescIdx == m_NumCUDescIdx) {
        return;
    }

    /* virtual cu not need load */
    if (OnlySysOrConstCol()) {
        return;
    }

    t_thrd.cstore_cxt.InProgressAioCUDispatch =
        (AioDispatchCUDesc_t**)palloc(sizeof(AioDispatchCUDesc_t*) * MAX_CU_PREFETCH_REQSIZ);
    AioDispatchCUDesc_t** dList = t_thrd.cstore_cxt.InProgressAioCUDispatch;
    t_thrd.cstore_cxt.InProgressAioCUDispatchCount = 0;

    File* vfdList = (File*)palloc(sizeof(File) * MAX_CU_PREFETCH_REQSIZ);
    errno_t rc =
        memset_s((char*)vfdList, sizeof(File) * MAX_CU_PREFETCH_REQSIZ, 0xFF, sizeof(File) * MAX_CU_PREFETCH_REQSIZ);
    securec_check(rc, "\0", "\0");

    // load CU each column
    int cuDescIdxTmp = 0;
    for (int col = 0; col < m_colNum; col++) {
        /* late read, no need prefetch */
        if (IsLateRead(col)) {
            continue;
        }
        /* vector load cu */
        for (int cuDescIdx = m_lastNumCUDescIdx; cuDescIdx != m_NumCUDescIdx; IncLoadCuDescIdx(cuDescIdx)) {
            cuDescIdxTmp = cuDescIdx;
            CUDesc* cudesc = &(m_CUDescInfo[col]->cuDescArray[m_CUDescIdx[cuDescIdx]]);
            CUPrefetch(cudesc, m_colId[col], dList, t_thrd.cstore_cxt.InProgressAioCUDispatchCount, vfdList);
        }
    }
    if (t_thrd.cstore_cxt.InProgressAioCUDispatchCount > 0) {
#ifndef ENABLE_LITE_MODE
        int tmp_count = t_thrd.cstore_cxt.InProgressAioCUDispatchCount;

        HOLD_INTERRUPTS();
        FileAsyncCURead(dList, t_thrd.cstore_cxt.InProgressAioCUDispatchCount);
        t_thrd.cstore_cxt.InProgressAioCUDispatchCount = 0;
        RESUME_INTERRUPTS();

        FileAsyncCUClose(vfdList, tmp_count);
        // stat cu hdd asyn read
        pgstatCountCUHDDAsynRead4SessionLevel(tmp_count);
        pgstat_count_cu_hdd_asyn(m_relation, tmp_count);
#endif
    }

    pfree(dList);
    pfree(vfdList);
    t_thrd.cstore_cxt.InProgressAioCUDispatch = NULL;
    t_thrd.cstore_cxt.InProgressAioCUDispatchCount = 0;

    ereport(DEBUG1,
            (errmodule(MOD_ADIO),
             errmsg("CUListPrefetch: relation(%s), cloumns(%d), cuid from %u to %u ",
                    RelationGetRelationName(m_relation),
                    m_colNum,
                    m_CUDescInfo[0]->cuDescArray[m_CUDescIdx[m_lastNumCUDescIdx]].cu_id,
                    m_CUDescInfo[0]->cuDescArray[m_CUDescIdx[cuDescIdxTmp]].cu_id)));

    m_lastNumCUDescIdx = m_NumCUDescIdx;
}

/*
 * @Description: aio clean up CU status
 * @See also:
 */
void CUListPrefetchAbort()
{
    int count = t_thrd.cstore_cxt.InProgressAioCUDispatchCount;
    int already_submit_count = u_sess->storage_cxt.AsyncSubmitIOCount;
    AioDispatchCUDesc_t** dList = t_thrd.cstore_cxt.InProgressAioCUDispatch;

    if (t_thrd.cstore_cxt.InProgressAioCUDispatchCount == 0) {
        return;
    }
    ereport(LOG, (errmsg("aio cu prefetch: aio dispatch count(%d)", count)));
    for (int i = already_submit_count; i < count; i++) {
        if (dList[i] == NULL) {
            continue;
        }
        CUCache->AbortCU(dList[i]->cuDesc.slotId);
        adio_share_free(dList[i]);
        dList[i] = NULL;
    }
    t_thrd.cstore_cxt.InProgressAioCUDispatch = NULL;
    t_thrd.cstore_cxt.InProgressAioCUDispatchCount = 0;
    u_sess->storage_cxt.AsyncSubmitIOCount = 0;
}

/*
 * @Description: api function aio clean up CU status
 * @See also:
 */
void CStoreAbortCU()
{
    /* Don't support columnar table in single node mode */
    if (!IS_SINGLE_NODE) {
        CUCache->TerminateCU(true);
        CUListPrefetchAbort();
    }
}

/*
* @Description: AbortCU in verify process.
*/
void VerifyAbortCU()
{
    CUCache->TerminateVerifyCU();
    return;
}

/*
 * @Description: Similiar to CStoreScan, but moving an entire "row" (same CUID) of CU and
 * 			    Corresponding CUDescs and bitmaps.
 *			    Used so far on CStore partition merging.
 *			    Supports ADIO.
 *			    Attention: by using CUStorage->LoadCU we use CStoreMemAlloc::Palloc to
 *					      palloc CU_ptr(s) assigned to BatchCUData->CUptrData. So
 *					      they have to be CStoreMemAlloc::Pfree later when finished.
 * @See also: ATExecCStoreMergePartition
 */
void CStore::CStoreScanWithCU(_in_ CStoreScanState* state, __inout BatchCUData* batchCUData, _in_ bool isVerify)
{
    // step1: The number of holding CUDesc is max_loaded_cudesc
    // if we load all CUDesc once, the memory will not enough.
    // So we load CUdesc once for max_loaded_cudesc
    LoadCUDescIfNeed();

    // step2: Do RoughCheck if need
    // elimiate CU by min/max value of CU.
    // Necessary for ADIO.
    RoughCheckIfNeed(state);

    /*
     * step3: Have CU hitted
     * we will not fill vector because no CU is hitted
     */
    ADIO_RUN()
    {
        if (unlikely(m_cursor == m_NumCUDescIdx)) {
            return;
        }
    }
    ADIO_ELSE()
    {
        if (unlikely(m_NumLoadCUDesc == 0)) {
            return;
        }
    }
    ADIO_END();

    /*
     * step 4:
     * load CUDescs and CUs of the row that is currently being processed
     *
     *   1. Number of processed rows with CUDescs
     *   2. Number of deadrows. Should be 0 because we do not deal with deadrows in copying CUs
     */
    int cuDescIdx = m_CUDescIdx[m_cursor];
    FormData_pg_attribute* attrs = m_relation->rd_att->attrs;
    for (int i = 0; i < m_colNum; ++i) {
        /* colIdx is pysical column id */
        int colIdx = m_colId[i];
        if (attrs[colIdx].attisdropped) {
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_OPERATION),
                     (errmsg("Cannot load CUDesc and CU for a dropped column \"%s\" of table \"%s\"",
                             NameStr(attrs[colIdx].attname),
                             RelationGetRelationName(m_relation)))));
        }

        CUDesc* cuDescPtr = &(m_CUDescInfo[i]->cuDescArray[cuDescIdx]);
        CU* cuDataPtr =
            New(CurrentMemoryContext) CU(attrs[colIdx].attlen, attrs[colIdx].atttypmod, attrs[colIdx].atttypid);

        /* CStoreMemAlloc::Palloc is used in LoadCU. Need toCStoreMemAlloc::Pfree later */
        if (cuDescPtr->cu_size > 0) {
            m_cuStorage[colIdx]->LoadCU(cuDataPtr,
                                        cuDescPtr->cu_pointer,
                                        cuDescPtr->cu_size,
                                        g_instance.attr.attr_storage.enable_adio_function,
                                        false);

            if (cuDataPtr->IsVerified(cuDescPtr->magic) == false) {
                addBadBlockStat(
                    &m_cuStorage[colIdx]->m_cnode.m_rnode, ColumnId2ColForkNum(m_cuStorage[colIdx]->m_cnode.m_attid));

                if (RelationNeedsWAL(m_relation) && CanRemoteRead()) {
                    ereport(WARNING,
                            (errcode(ERRCODE_DATA_CORRUPTED),
                             (errmsg("invalid CU in cu_id %u of relation \"%s\" file %s offset %lu, try to remote read",
                                     cuDescPtr->cu_id,
                                     RelationGetRelationName(m_relation),
                                     relcolpath(m_cuStorage[colIdx]),
                                     cuDescPtr->cu_pointer)),
                             handle_in_client(true)));

                    m_cuStorage[colIdx]->RemoteLoadCU(cuDataPtr,
                                                      cuDescPtr->cu_pointer,
                                                      cuDescPtr->cu_size,
                                                      g_instance.attr.attr_storage.enable_adio_function,
                                                      false);

                    if (cuDataPtr->IsVerified(cuDescPtr->magic)) {
                        m_cuStorage[colIdx]->OverwriteCU(
                            cuDataPtr->m_compressedBuf, cuDescPtr->cu_pointer, cuDescPtr->cu_size, false);
                    } else {
                        ereport(ERROR,
                                (errcode(ERRCODE_DATA_CORRUPTED),
                                 (errmsg("fail to remote read CU, data corrupted in network"))));
                    }
                } else {
                    int elevel = ERROR;
                    if (isVerify) {
                        elevel = WARNING;
                    }
                    ereport(elevel,
                            (errcode(ERRCODE_DATA_CORRUPTED),
                             (errmsg("CU verification failed. The node is %s, invalid CU in cu_id %u of relation %s,"
                                     "file %s offset %lu",
                                     g_instance.attr.attr_common.PGXCNodeName,
                                     cuDescPtr->cu_id,
                                     RelationGetRelationName(m_relation),
                                     relcolpath(m_cuStorage[colIdx]),
                                     cuDescPtr->cu_pointer)),
                             handle_in_client(true)));
                }
            }
        }

        *batchCUData->CUDescData[colIdx] = *cuDescPtr;
        batchCUData->CUptrData[colIdx] = cuDataPtr;
        GetCUDeleteMaskIfNeed(cuDescPtr->cu_id, m_snapshot);
    }

    batchCUData->hasValue = true;

    batchCUData->CopyDelMask(m_hasDeadRow ? m_cuDelMask : NULL);

    // step5: refresh cursor
    // Since we are moving CU in whole, we just move the cursor
    IncLoadCuDescIdx(m_cursor);

    // We should never have dived into rows in CU in this function
    Assert(m_rowCursorInCU == 0);

    // step6: prefetch if need
    ADIO_RUN()
    {
        CUListPrefetch();
    }
    ADIO_END();
}

// CStoreScan
// Scan ColStore table and fill vecBatchOut
void CStore::CStoreScan(_in_ CStoreScanState* state, _out_ VectorBatch* vecBatchOut)
{
    // step1: The number of holding CUDesc is  max_loaded_cudesc
    // if we load all CUDesc once, the memory will not enough.
    // So we load CUdesc once for max_loaded_cudesc
    CSTORESCAN_TRACE_START(LOAD_CU_DESC);
    LoadCUDescIfNeed();
    CSTORESCAN_TRACE_END(LOAD_CU_DESC);

    // step2: Do RoughCheck if need
    // elimiate CU by min/max value of CU.
    CSTORESCAN_TRACE_START(MIN_MAX_CHECK);
    RoughCheckIfNeed(state);
    CSTORESCAN_TRACE_END(MIN_MAX_CHECK);

    // step3: Have CU hitted
    // we will not fill vector because no CU is hitted
    ADIO_RUN()
    {
        if (unlikely(m_cursor == m_NumCUDescIdx)) {
            return;
        }
    }
    ADIO_ELSE()
    {
        if (unlikely(m_NumLoadCUDesc == 0)) {
            return;
        }
    }
    ADIO_END();

    // step4: Fill VecBatch
    CSTORESCAN_TRACE_START(FILL_BATCH);
    int deadRows = FillVecBatch(vecBatchOut);
    CSTORESCAN_TRACE_END(FILL_BATCH);

    // step5: refresh cursor
    RefreshCursor(vecBatchOut->m_rows, deadRows);

    // step6: prefetch if need
    ADIO_RUN()
    {
        CSTORESCAN_TRACE_START(PREFETCH_CU_LIST);
        CUListPrefetch();
        CSTORESCAN_TRACE_END(PREFETCH_CU_LIST);
    }
    ADIO_END();
}

/*
 * @Description: calculate how many cudescs loaded
 * @Param[IN] end: array idx end
 * @Param[IN] start: array idx start
 * @Return: count of cudescs loaded
 * @See also:
 */
int CStore::LoadCudescMinus(int start, int end) const
{
    if (end >= start) {
        return end - start;
    }

    return u_sess->attr.attr_storage.max_loaded_cudesc - start + end;
}

/*
 * @Description:  check whether cudesc array is full
 * @Param[IN] end: array idx end
 * @Param[IN] start: array idx start
 * @Return: true- have empty slot, false no free slot
 * @See also:
 */
bool CStore::HasEnoughCuDescSlot(int start, int end) const
{
    return LoadCudescMinus(start, end) < u_sess->attr.attr_storage.max_loaded_cudesc - 1;
}

/*
 * @Description: check whether meet load cudesc condition and set load cudesc array idx and so on
 * @Param[IN] cudesc_idx: load cudesc array idx
 * @Return: true--need load cudesc, false-- not need
 * @See also:
 */
bool CStore::NeedLoadCUDesc(int32& cudesc_idx)
{
    bool need_load = false;
    ADIO_RUN()
    {
        // check load condition, first not load finish, second not exceed prefetch count
        if (!m_load_finish && (m_cursor == m_NumCUDescIdx || LoadCudescMinus(m_cursor, m_NumCUDescIdx) <=
                               t_thrd.cstore_cxt.cstore_prefetch_count / 2)) {
            need_load = true;
            m_prefetch_quantity = 0;
            cudesc_idx = m_NumCUDescIdx;
        }
    }
    ADIO_ELSE()
    {
        if (m_cursor >= m_NumLoadCUDesc) {
            need_load = true;
            m_cursor = 0;
            cudesc_idx = 0;
        }
    }
    ADIO_END();

    return need_load;
}

// The number of holding CUDesc is  max_loaded_cudesc
// if we load all CUDesc once, the memory will not enough.
// So we load CUdesc once for max_loaded_cudesc
void CStore::LoadCUDescIfNeed()
{
    uint32 last_load_num = 0;
    int32 cudesc_idx = 0;  // cudesc_idx set 0 when  buffer io, and  set to m_NumCUDescIdx when adio

    if (!NeedLoadCUDesc(cudesc_idx)) {
        return;
    }

    m_NumLoadCUDesc = 0;

    Assert(m_perScanMemCnxt);
    // we reset when a batch of CUs have been scanned and handled.
    MemoryContextReset(m_perScanMemCnxt);
#ifdef MEMORY_CONTEXT_CHECKING
    MemoryContextCheck(m_perScanMemCnxt->parent, m_perScanMemCnxt->parent->session_id > 0);
#endif

    // Load CUDesc into m_cuDescInfo for all accessed columns
    if (m_colNum > 0) {
        last_load_num = m_CUDescInfo[0]->curLoadNum;
    }

    do {
        bool found = false;
        for (int i = 0; i < m_colNum; ++i) {
            Assert(m_colId[i] >= 0);
            // if enable adio, load one cu for caculate prefetch quantity
            found =
                LoadCUDesc(m_colId[i], m_CUDescInfo[i], g_instance.attr.attr_storage.enable_adio_function, m_snapshot);
        }

        if (likely(m_colNum > 1 && m_CUDescInfo[0]->curLoadNum > 0)) {
            CheckConsistenceOfCUDescCtl();
            /* check the first CUDesc for all columns */
            CheckConsistenceOfCUDesc(0);
            /* check the last CUDesc for all columns */
            if (m_CUDescInfo[0]->curLoadNum > 1) {
                CheckConsistenceOfCUDesc(m_CUDescInfo[0]->curLoadNum - 1);
            }
        }

        if (m_colNum > 0) {
            for (int j = (int)m_CUDescInfo[0]->lastLoadNum; j != (int)m_CUDescInfo[0]->curLoadNum;
                 IncLoadCuDescIdx(j)) {
                m_CUDescIdx[cudesc_idx] = j;
                IncLoadCuDescIdx(cudesc_idx);
            }
            m_NumLoadCUDesc += LoadCudescMinus(m_CUDescInfo[0]->lastLoadNum, m_CUDescInfo[0]->curLoadNum);
        }

        ADIO_RUN()
        {
            // if found ,we need to check prefetch quantity and decide whether need load more cudesc
            if (found && m_prefetch_quantity < m_prefetch_threshold &&
                HasEnoughCuDescSlot(last_load_num, m_CUDescInfo[0]->curLoadNum)) {
                continue;
            }
            // load finish, set lastLoadNum to backup values
            for (int i = 0; i < m_colNum; ++i) {
                m_CUDescInfo[i]->lastLoadNum = last_load_num;
            }
            if (m_colNum > 0) {
                // give an min prefetch count here,because we need prefetch window to control whether need prefetch
                t_thrd.cstore_cxt.cstore_prefetch_count = Max(m_NumLoadCUDesc, CSTORE_MIN_PREFETCH_COUNT);
                ereport(DEBUG1,
                        (errmodule(MOD_ADIO),
                         errmsg("LoadCUDesc: columns(%d), count(%d), quantity(%d)",
                                m_colNum,
                                m_NumLoadCUDesc,
                                m_prefetch_quantity)));
            }
            break;
        }
        ADIO_ELSE()
        {
            break;
        }
        ADIO_END();
    } while (1);

    // sys columns and const columns
    if (m_colNum > 0 && m_sysColNum != 0) {
        // access normal columns and sys columns, use normal column's CUDesc
        m_virtualCUDescInfo = m_CUDescInfo[0];
    } else if (OnlySysOrConstCol()) {
        // only system columns or const columns, use the first column's CUDesc
        Assert(m_virtualCUDescInfo);
        LoadCUDesc(m_firstColIdx, m_virtualCUDescInfo, false, m_snapshot);

        for (int j = (int)m_virtualCUDescInfo->lastLoadNum; j != (int)m_virtualCUDescInfo->curLoadNum;
             IncLoadCuDescIdx(j)) {
            m_CUDescIdx[cudesc_idx] = j;
            IncLoadCuDescIdx(cudesc_idx);
        }
        m_NumLoadCUDesc = LoadCudescMinus(m_virtualCUDescInfo->lastLoadNum, m_virtualCUDescInfo->curLoadNum);
        // adio used it, but no need add ADIO_RUN(), for buffer io it is no use
        t_thrd.cstore_cxt.cstore_prefetch_count = m_NumLoadCUDesc;
    }

    // Load new CUs need do rough check
    m_needRCheck = true;

    // before RoughCheck, m_NumCUDescIdx is length of loaded CUDesc info
    BFIO_RUN()
    {
        m_NumCUDescIdx = m_NumLoadCUDesc;
    }
    BFIO_END();
    return;
}

/*
 * @Description:  increase load cudesc array idx
 * @Param[IN/OUT] idx: idx of array
 * @See also:
 */
void CStore::IncLoadCuDescIdx(int& idx) const
{
    idx++;

    ADIO_RUN()
    {
        if (idx >= u_sess->attr.attr_storage.max_loaded_cudesc) {
            idx = 0;
        }
    }
    ADIO_END();
    return;
}

/*
 * @Description: Set CU range for Range Scan In Redistribute
 *
 * @return: void
 */
void CStore::SetScanRange()
{
    Oid cudescOid = m_relation->rd_rel->relcudescrelid;
    uint32 maxCuId = CStore::GetMaxCUID(cudescOid, m_relation->rd_att, m_snapshot);
    uint32 CUCount = maxCuId - FirstCUID;

    uint32 startCUID = FirstCUID + 1;
    uint32 endCUID = maxCuId;

    if (m_rangeScanInRedis.isRangeScanInRedis) {
        ItemPointerData start_ctid;
        ItemPointerData end_ctid;

        RelationGetCtids(m_relation, &start_ctid, &end_ctid);

        startCUID = RedisCtidGetBlockNumber(&start_ctid);
        endCUID = RedisCtidGetBlockNumber(&end_ctid);
        CUCount = endCUID - startCUID + 1;
    }

    m_startCUID = startCUID;
    m_endCUID = endCUID;
}

void CStore::RefreshCursor(int row, int deadRows)
{
    int cuRowCount = 0;
    int idx = m_CUDescIdx[m_cursor];

    if (likely(m_CUDescInfo != NULL)) {
        cuRowCount = m_CUDescInfo[0]->cuDescArray[idx].row_count;
    } else {
        Assert(m_virtualCUDescInfo);
        cuRowCount = m_virtualCUDescInfo->cuDescArray[idx].row_count;
    }

    m_rowCursorInCU = m_rowCursorInCU + row + deadRows;

    Assert(m_rowCursorInCU <= cuRowCount);
    if (unlikely(m_rowCursorInCU == cuRowCount)) {
        IncLoadCuDescIdx(m_cursor);
        m_rowCursorInCU = 0;
        if (likely(m_scanPosInCU != NULL)) {
            Assert(m_colNum > 0);
            errno_t rc = memset_s(m_scanPosInCU, sizeof(int) * m_colNum, 0, sizeof(int) * m_colNum);
            securec_check(rc, "", "");
        }
    }
}

/*
 * @Description: cudesc rough check
 * @Param[IN] cuDescIdx:index of load cudesc info
 * @Param[IN] nkeys: keys of scanKey
 * @Param[IN] scanKey: cstore scan key
 * @Return: true--hit, false--not hit
 * @See also:
 */
bool CStore::RoughCheck(CStoreScanKey scanKey, int nkeys, int cuDescIdx)
{
    bool hitCU = true;

    for (int j = 0; j < nkeys; j++) {
        int seq = scanKey[j].cs_attno;
        CUDesc* cudesc = &(m_CUDescInfo[seq]->cuDescArray[cuDescIdx]);
        bool isNullKey = scanKey[j].cs_flags & SK_ISNULL;
        if ((cudesc->IsNullCU() && !isNullKey) || cudesc->IsNoMinMaxCU())
            continue;
        if (isNullKey)
            hitCU = cudesc->CUHasNull() || cudesc->IsNullCU();
        else
            hitCU = m_RCFuncs[j](cudesc, scanKey[j].cs_argument);
        if (!hitCU)
            break;
    }
    return hitCU;
}

void CStore::RoughCheckIfNeed(_in_ CStoreScanState* state)
{
    int nkeys = state->csss_NumScanKeys;
    CStoreScanKey scanKey = state->csss_ScanKeys;
    PlanState* planstate = (PlanState*)state;
    uint32 curLoadNum;
    uint32 lastLoadNum;

    // m_needRCheck is true means these CUs alreay done the rough check
    // m_colNum == 0 means not have normal columns
    if (likely(!m_needRCheck)) {
        return;
    }

    if (likely(nkeys == 0 || scanKey == NULL || m_colNum == 0)) {
        /* when no where condition, we also need set m_lastNumCUDescIdx and m_NumCUDescIdx for prefetch once */
        ADIO_RUN()
        {
            m_NumCUDescIdx = (m_NumCUDescIdx + m_NumLoadCUDesc) % u_sess->attr.attr_storage.max_loaded_cudesc;
            m_needRCheck = false;
        }
        ADIO_END();
        return;
    }

    int pos = 0;
    bool hitCU = true;
    int cudesc_idx_tmp = 0;

    ADIO_RUN()
    {
        /* pos is rough check start point */
        cudesc_idx_tmp = m_NumCUDescIdx;
        pos = m_NumCUDescIdx;
    }
    ADIO_END();

    lastLoadNum = m_CUDescInfo[0]->lastLoadNum;
    curLoadNum = m_CUDescInfo[0]->curLoadNum;
    for (int i = (int)lastLoadNum; i != (int)curLoadNum; IncLoadCuDescIdx(i), IncLoadCuDescIdx(cudesc_idx_tmp)) {
        hitCU = RoughCheck(scanKey, nkeys, i);
        if (hitCU) {
            // fliter CU not hit
            ADIO_RUN()
            {
                m_CUDescIdx[pos] = m_CUDescIdx[cudesc_idx_tmp];
            }
            ADIO_ELSE()
            {
                m_CUDescIdx[pos] = m_CUDescIdx[i];
            }
            ADIO_END();

            IncLoadCuDescIdx(pos);
        }

        if (planstate->instrument) {
            RCInfo* rcPtr = &(planstate->instrument->rcInfo);

            if (!hitCU) {
                int seq = scanKey[0].cs_attno;
                CUDesc *cudesc = &(m_CUDescInfo[seq]->cuDescArray[i]);
                planstate->instrument->nfiltered1 += cudesc->row_count;

                Relation cuDescRel = heap_open(m_relation->rd_rel->relcudescrelid, AccessShareLock);
                Relation idxRel = index_open(cuDescRel->rd_rel->relcudescidx, AccessShareLock);
                ScanKeyData key[2];

                ScanKeyInit(&key[0], (AttrNumber)CUDescColIDAttr, BTEqualStrategyNumber, 
                            F_INT4EQ, Int32GetDatum(VitrualDelColID));
                ScanKeyInit(&key[1], (AttrNumber)CUDescCUIDAttr, BTEqualStrategyNumber, 
                            F_OIDEQ, UInt32GetDatum(cudesc->cu_id));
                SysScanDesc cuDescScan = systable_beginscan_ordered(cuDescRel, idxRel, m_snapshot, 2, key);

                HeapTuple tmpTup = NULL;

                if ((tmpTup = systable_getnext_ordered(cuDescScan, ForwardScanDirection)) != NULL) {
                    bool isNull = false;
                    uint32 deadRowCount = 0;
                    uint32 rowCount = DatumGetUInt32(fastgetattr(tmpTup, CUDescRowCountAttr, 
                        cuDescRel->rd_att, &isNull));
                    Datum v = fastgetattr(tmpTup, CUDescCUPointerAttr, cuDescRel->rd_att, &isNull);
                    if (!isNull) {
                        int8 *bitmap = (int8 *)PG_DETOAST_DATUM(DatumGetPointer(v));
                        unsigned char delBitMap[MaxDelBitmapSize];
                        uint32 nBytes = (rowCount + 7) / 8;
                        errno_t rc = memcpy_s(delBitMap, MaxDelBitmapSize, 
                                              VARDATA_ANY(bitmap), VARSIZE_ANY_EXHDR(bitmap));
                        securec_check(rc, "", "");
                        for (uint32 j = 0; j < nBytes; j++) {
                            deadRowCount += NumberOfBit1Set[delBitMap[j]];
                        }
                        /* because new memory may be created, so we have to check and free in time. */
                        if ((Pointer)bitmap != DatumGetPointer(v)) {
                            pfree_ext(bitmap);
                        }
                    }
                    planstate->instrument->nfiltered1 -= deadRowCount;
                }
                systable_endscan_ordered(cuDescScan);
                index_close(idxRel, AccessShareLock);
                heap_close(cuDescRel, AccessShareLock);

                rcPtr->IncNoneCUNum();
            } else {
                rcPtr->IncSomeCUNum();
            }
            planstate->instrument->needRCInfo = true;
        }
    }

    ADIO_RUN()
    {
        /* m_NumCUDescIdx is rough check end point, ,so need update here */
        if (pos == m_NumCUDescIdx) {
            m_NumCUDescIdx = (m_NumCUDescIdx + m_NumLoadCUDesc) % u_sess->attr.attr_storage.max_loaded_cudesc;
        } else {
            m_NumCUDescIdx = pos;
        }
    }
    ADIO_ELSE()
    {
        m_NumLoadCUDesc = pos;
    }
    ADIO_END();

    // set flag for already done the rought check
    m_needRCheck = false;
}

void CStore::InitReScan()
{
    /* Set scan cu range */
    SetScanRange();
    for (int i = 0; i < m_colNum; ++i) {
        m_CUDescInfo[i]->Reset(m_startCUID);
    }

    int totalSize = 0;
    errno_t rc = 0;
    if (likely(m_scanPosInCU != NULL)) {
        Assert(m_colNum > 0);
        totalSize = sizeof(int) * m_colNum;
        rc = memset_s(m_scanPosInCU, totalSize, 0, totalSize);
        securec_check(rc, "", "");
    }

    m_delMaskCUId = InValidCUID;
    m_hasDeadRow = false;
    m_prefetch_quantity = 0;

    m_load_finish = false;
    if (m_CUDescIdx != NULL) {
        totalSize = sizeof(int) * u_sess->attr.attr_storage.max_loaded_cudesc;
        rc = memset_s(m_CUDescIdx, totalSize, 0xFF, totalSize);
        securec_check(rc, "\0", "\0");
    }

    // only access sys columns or const columns
    if (OnlySysOrConstCol()) {
        Assert(m_virtualCUDescInfo);
        m_virtualCUDescInfo->Reset(m_startCUID);
    }

    // m_sysColNum shouldn't be reset or changed.
    // m_colNum shouldn't be reset or changed.
    m_NumLoadCUDesc = 0;
    m_NumCUDescIdx = 0;
    m_lastNumCUDescIdx = 0;
    m_cursor = 0;
    m_rowCursorInCU = 0;
    m_cuDescIdx = -1;
    m_laterReadCtidColIdx = -1;

    m_needRCheck = false;
}

void CStore::InitPartReScan(Relation rel)
{
    Assert(m_cuStorage);

    // change to the new partition relation.
    m_relation = rel;
    int attNo = m_relation->rd_att->natts;

    // the following spaces will live until deconstructor is called.
    // so use m_scanMemContext which is not freed at all until the end.
    AutoContextSwitch newMemCnxt(m_scanMemContext);

    // because new partition has different file handler, so we must
    // destroy the old *m_cuStorage*, which will close the open fd,
    // and then create an new object for next partition.
    for (int i = 0; i < attNo; ++i) {
        if (m_relation->rd_att->attrs[i].attisdropped)
            continue;
        if (m_cuStorage[i]) {
            DELETE_EX(m_cuStorage[i]);
        }

        // Here we must use physical column id
        CFileNode cFileNode(m_relation->rd_node, m_relation->rd_att->attrs[i].attnum, MAIN_FORKNUM);
        m_cuStorage[i] = New(CurrentMemoryContext) CUStorage(cFileNode);
    }
}

// FORCE_INLINE
bool CStore::IsEndScan() const
{
    // all CUDesc already scanned
    ADIO_RUN()
    {
        return (m_cursor == m_NumCUDescIdx && m_load_finish) ? true : false;
    }
    ADIO_ELSE()
    {
        return (m_NumCUDescIdx == 0) ? true : false;
    }
    ADIO_END();
}

FORCE_INLINE
bool CStore::IsLateRead(int id) const
{
    Assert(m_lateRead);
    return m_lateRead[id];
}

void CStore::ResetLateRead()
{
    for (int i = 0; i < m_colNum; ++i)
        m_lateRead[i] = false;
}

/*
 * @Description: set m_timing_on according state->ps.instrument and its timer
 * @IN state: cstore scan state
 * @Return: true if instrument::need_timer is set true; otherwise return false
 * @See also:
 */
void CStore::SetTiming(CStoreScanState* state)
{
    m_timing_on = (NULL != ((ScanState*)state)->ps.instrument && ((ScanState*)state)->ps.instrument->need_timer);
}

void CStore::ScanByTids(_in_ CStoreIndexScanState* state, _in_ VectorBatch* idxOut, _out_ VectorBatch* vbout)
{
    Assert(state && idxOut && vbout);
    Assert(idxOut->m_cols >= 1);

    CSTORESCAN_TRACE_START(SCAN_BY_TID);

    int* indexOutBaseTabAttr = state->m_indexOutBaseTabAttr;
    int indexOutAttrNo = state->m_indexOutAttrNo;

    /* set if use btree index */
    m_useBtreeIndex = (state->m_indexScan == NULL) ? true : false;

    /*
     * Pre-Step: For const-targetlist, set output rows.
     */
    vbout->m_rows = idxOut->m_rows;

    ScalarVector* tids = idxOut->m_arr + idxOut->m_cols - 1;

    // Step 1: Fill normal column Vector according to tid
    CSTORESCAN_TRACE_START(FILL_VECTOR_BATCH_BY_TID);
    for (int i = 0; i < m_colNum; i++) {
        int idx = m_colId[i];

        // Judge whether this colIdx has been scan in index table scan
        bool isInIndexOut = false;
        for (int j = 0; j < indexOutAttrNo; j++) {
            if (idx == indexOutBaseTabAttr[j] - 1) {
                // copy index table scan to vector out
                // shallow copy
                FillVectorByIndex(idx, tids, idxOut->m_arr + j, vbout->m_arr + idx);
                vbout->m_rows = (vbout->m_arr + idx)->m_rows;
                isInIndexOut = true;
                break;
            }
        }

        if (isInIndexOut)
            continue;

        Assert(m_fillVectorByTids[i]);
        (this->*m_fillVectorByTids[i])(idx, tids, &vbout->m_arr[idx]);
        vbout->m_rows = vbout->m_arr[idx].m_rows;
    }
    CSTORESCAN_TRACE_END(FILL_VECTOR_BATCH_BY_TID);

    // Step 2: Fill syscolum if need
    for (int i = 0; i < m_sysColNum; i++) {
        int sysColIdx = m_sysColId[i];
        ScalarVector* sysVec = vbout->GetSysVector(sysColIdx);

        switch (sysColIdx) {
            case SelfItemPointerAttributeNumber: {
                FillSysVecByTid<SelfItemPointerAttributeNumber>(tids, sysVec);
                break;
            }
            case TableOidAttributeNumber: {
                FillSysVecByTid<TableOidAttributeNumber>(tids, sysVec);
                break;
            }
            case XC_NodeIdAttributeNumber: {
                FillSysVecByTid<XC_NodeIdAttributeNumber>(tids, sysVec);
                break;
            }
            case MinTransactionIdAttributeNumber: {
                FillSysVecByTid<MinTransactionIdAttributeNumber>(tids, sysVec);
                break;
            }
            default: {
                ereport(ERROR,
                        (errcode(ERRCODE_DATATYPE_MISMATCH),
                         (errmsg("Cannot to fill unsupported system column %d for column store table", sysColIdx))));
                break;
            }
        }

        vbout->m_rows = sysVec->m_rows;
    }

    // Step 3: fill const columns if need
    if (unlikely(m_onlyConstCol)) {
        // only set row count
        int liveRows = 0;
        ScalarVector* vec = vbout->m_arr;
        ScalarValue* tidValue = tids->m_vals;
        uint32 curCUId = InValidCUID;
        uint32 thisCUId = InValidCUID;
        uint32 rowOffset = 0;

        for (int i = 0; i < tids->m_rows; i++) {
            ItemPointer tidPtr = (ItemPointer)&tidValue[i];
            thisCUId = ItemPointerGetBlockNumber(tidPtr);

            // Note that tidPointer->rowOffset start from 1
            rowOffset = ItemPointerGetOffsetNumber(tidPtr) - 1;

            // Get CUDesc and delmask if need
            if (curCUId != thisCUId) {
                curCUId = thisCUId;
                GetCUDeleteMaskIfNeed(curCUId, m_snapshot);
            }
            // It is a live row, not a dead row
            if (m_delMaskCUId != InValidCUID && !IsDeadRow(curCUId, rowOffset))
                ++liveRows;
        }

        vec->m_rows = liveRows;
        vbout->m_rows = vec->m_rows;
    }

    CSTORESCAN_TRACE_END(SCAN_BY_TID);
}

// form a cudes tuple for deleting bitmap
HeapTuple CStore::FormVCCUDescTup(
    _in_ TupleDesc cudesc, _in_ const char* delMask, _in_ uint32 cuId, _in_ int32 rowCount, _in_ uint32 magic)
{
    Datum values[CUDescMaxAttrNum] = {0};
    bool nulls[CUDescMaxAttrNum] = {0};
    text* tmpCuPointData = NULL;

    values[CUDescColIDAttr - 1] = Int32GetDatum(VitrualDelColID);
    nulls[CUDescColIDAttr - 1] = false;

    values[CUDescCUIDAttr - 1] = UInt32GetDatum(cuId);
    nulls[CUDescCUIDAttr - 1] = false;

    values[CUDescRowCountAttr - 1] = Int32GetDatum(rowCount);
    nulls[CUDescRowCountAttr - 1] = false;

    values[CUDescCUMagicAttr - 1] = UInt32GetDatum(magic);
    nulls[CUDescCUMagicAttr - 1] = false;

    // deleting bitmap maybe be NULL, when any updating
    // or deleting never happens.
    if (delMask) {
        nulls[CUDescCUPointerAttr - 1] = false;

        int delMaskBytes = (rowCount + 7) / 8;
        tmpCuPointData = cstring_to_text_with_len((const char*)delMask, delMaskBytes);
        values[CUDescCUPointerAttr - 1] = PointerGetDatum(tmpCuPointData);
        Assert(VARSIZE_ANY_EXHDR(PointerGetDatum(tmpCuPointData)) == (uint32)delMaskBytes);
    } else
        nulls[CUDescCUPointerAttr - 1] = true;

    // the other fields are useless, so set them null.
    nulls[CUDescMinAttr - 1] = true;
    nulls[CUDescMaxAttr - 1] = true;
    nulls[CUDescCUModeAttr - 1] = true;
    nulls[CUDescSizeAttr - 1] = true;
    nulls[CUDescCUExtraAttr - 1] = true;

    HeapTuple newTup = (HeapTuple)tableam_tops_form_tuple(cudesc, values, nulls);

    // ok, the temp data has been copied to newTup.
    // now we must free it before returning.
    if (tmpCuPointData != NULL) {
        pfree_ext(tmpCuPointData);
    }
    return newTup;
}

// pCudescTupDesc: Cudesc tuple description.
// pCudesc:  a CUDesc object holding all information about a complete Cudesc Tuple.
// values[]: used during forming tuple.
// nulls[]:  used during forming tuple.
// pColAttr: attribute data of one column, who matches pCudesc above, for column-store table.
HeapTuple CStore::FormCudescTuple(_in_ CUDesc* pCudesc, _in_ TupleDesc pCudescTupDesc,
                                  _in_ Datum pTupVals[CUDescMaxAttrNum], _in_ bool pTupNulls[CUDescMaxAttrNum], _in_ Form_pg_attribute pColAttr)
{
    errno_t rc = memset_s(pTupNulls, CUDescMaxAttrNum, false, CUDescMaxAttrNum);
    securec_check(rc, "\0", "\0");

    pTupVals[CUDescColIDAttr - 1] = Int32GetDatum(pColAttr->attnum);
    pTupVals[CUDescCUIDAttr - 1] = UInt32GetDatum(pCudesc->cu_id);

    int minDataLen = 0, maxDataLen = 0;
    char *minDataPtr = NULL, *maxDataPtr = NULL;

    if (pColAttr->attlen > 0) {
        if (pColAttr->attbyval) {
            // Now we use int8 to store
            minDataLen = maxDataLen = sizeof(Datum);
        } else if (pColAttr->attlen <= MIN_MAX_LEN) {
            minDataLen = maxDataLen = pColAttr->attlen;
        } else {
            Assert(minDataLen == 0 && maxDataLen == 0);
        }

        minDataPtr = pCudesc->cu_min;
        maxDataPtr = pCudesc->cu_max;
    } else {
        Assert(pCudesc->cu_min[0] >= 0 && pCudesc->cu_min[0] < MIN_MAX_LEN);
        Assert(pCudesc->cu_max[0] >= 0 && pCudesc->cu_max[0] < MIN_MAX_LEN);

        minDataLen = pCudesc->cu_min[0];
        minDataPtr = pCudesc->cu_min + 1;

        maxDataLen = pCudesc->cu_max[0];
        maxDataPtr = pCudesc->cu_max + 1;
    }
    pTupVals[CUDescMinAttr - 1] = PointerGetDatum(cstring_to_text_with_len(minDataPtr, minDataLen));
    pTupVals[CUDescMaxAttr - 1] = PointerGetDatum(cstring_to_text_with_len(maxDataPtr, maxDataLen));

    pTupVals[CUDescRowCountAttr - 1] = Int32GetDatum(pCudesc->row_count);
    pTupVals[CUDescCUModeAttr - 1] = Int32GetDatum(pCudesc->cu_mode);
    pTupVals[CUDescSizeAttr - 1] = Int32GetDatum(pCudesc->cu_size);

    text* tmpStr3 = cstring_to_text_with_len((const char*)&pCudesc->cu_pointer, sizeof(CUPointer));
    pTupVals[CUDescCUPointerAttr - 1] = PointerGetDatum(tmpStr3);
    pTupVals[CUDescCUMagicAttr - 1] = UInt32GetDatum(pCudesc->magic);
    Assert(pTupVals[CUDescCUMagicAttr - 1] > 0);

    // add attribute extra and set null flag.
    pTupNulls[CUDescCUExtraAttr - 1] = true;

    return (HeapTuple)tableam_tops_form_tuple(pCudescTupDesc, pTupVals, pTupNulls);
}

/* description: future plan-refact CStore::LoadCUDesc() with DeformCudescTuple(). */
void CStore::DeformCudescTuple(
    _in_ HeapTuple pCudescTup, _in_ TupleDesc pCudescTupDesc, _in_ Form_pg_attribute pColAttr, _out_ CUDesc* pCudesc)
{
    errno_t rc = EOK;
    bool isnull = false;

    pCudesc->cu_id = DatumGetUInt32(fastgetattr(pCudescTup, CUDescCUIDAttr, pCudescTupDesc, &isnull));
    Assert(!isnull);

    // Put min value into cudesc->min
    char* valPtr = DatumGetPointer(fastgetattr(pCudescTup, CUDescMinAttr, pCudescTupDesc, &isnull));
    if (!isnull) {
        if (pColAttr->attlen > 0) {
            if (pColAttr->attbyval) {
                Assert((int)VARSIZE_ANY_EXHDR(valPtr) == sizeof(Datum));
                rc = memcpy_s(pCudesc->cu_min, MIN_MAX_LEN, VARDATA_ANY(valPtr), sizeof(Datum));
                securec_check(rc, "", "");
            } else if (pColAttr->attlen <= MIN_MAX_LEN) {
                Assert((int)VARSIZE_ANY_EXHDR(valPtr) == pColAttr->attlen);
                rc = memcpy_s(pCudesc->cu_min, MIN_MAX_LEN, VARDATA_ANY(valPtr), pColAttr->attlen);
                securec_check(rc, "", "");
            } else {
                Assert(pCudesc->cu_min[0] == 0);
            }
        } else {
            pCudesc->cu_min[0] = VARSIZE_ANY_EXHDR(valPtr);
            if (pCudesc->cu_min[0] > 0) {
                Assert(pCudesc->cu_min[0] < MIN_MAX_LEN);
                rc = memcpy_s(pCudesc->cu_min + 1, (MIN_MAX_LEN - 1), VARDATA_ANY(valPtr), pCudesc->cu_min[0]);
                securec_check(rc, "", "");
            } else {
                Assert(pCudesc->cu_min[0] == 0);
            }
        }
    }

    // Put max value into cudesc->max
    valPtr = DatumGetPointer(fastgetattr(pCudescTup, CUDescMaxAttr, pCudescTupDesc, &isnull));
    if (!isnull) {
        if (pColAttr->attlen > 0) {
            if (pColAttr->attbyval) {
                Assert((int)VARSIZE_ANY_EXHDR(valPtr) == sizeof(Datum));
                rc = memcpy_s(pCudesc->cu_max, MIN_MAX_LEN, VARDATA_ANY(valPtr), sizeof(Datum));
                securec_check(rc, "", "");
            } else if (pColAttr->attlen <= MIN_MAX_LEN) {
                Assert((int)VARSIZE_ANY_EXHDR(valPtr) == pColAttr->attlen);
                rc = memcpy_s(pCudesc->cu_max, MIN_MAX_LEN, VARDATA_ANY(valPtr), pColAttr->attlen);
                securec_check(rc, "", "");
            } else {
                Assert(pCudesc->cu_max[0] == 0);
            }
        } else {
            pCudesc->cu_max[0] = VARSIZE_ANY_EXHDR(valPtr);
            if (pCudesc->cu_max[0] > 0) {
                Assert(pCudesc->cu_max[0] < MIN_MAX_LEN);
                rc = memcpy_s(pCudesc->cu_max + 1, (MIN_MAX_LEN - 1), VARDATA_ANY(valPtr), pCudesc->cu_max[0]);
                securec_check(rc, "", "");
            } else {
                Assert(pCudesc->cu_max[0] == 0);
            }
        }
    }

    pCudesc->row_count = DatumGetInt32(fastgetattr(pCudescTup, CUDescRowCountAttr, pCudescTupDesc, &isnull));
    Assert(!isnull);

    // Put CUMode into cudesc->cumode
    pCudesc->cu_mode = DatumGetInt32(fastgetattr(pCudescTup, CUDescCUModeAttr, pCudescTupDesc, &isnull));
    Assert(!isnull);

    // Put cusize into cudesc->cu_size
    pCudesc->cu_size = DatumGetInt32(fastgetattr(pCudescTup, CUDescSizeAttr, pCudescTupDesc, &isnull));
    Assert(!isnull);

    // Put CUPointer into cudesc->cuPointer
    char* cu_ptr = DatumGetPointer(fastgetattr(pCudescTup, CUDescCUPointerAttr, pCudescTupDesc, &isnull));
    if (!isnull) {
        Assert(VARSIZE_ANY_EXHDR(cu_ptr) == sizeof(CUPointer));
        pCudesc->cu_pointer = *(CUPointer*)VARDATA_ANY(cu_ptr);
    } else
        Assert(pCudesc->cu_pointer == 0);

    // Put magic into cudesc->magic
    pCudesc->magic = DatumGetUInt32(fastgetattr(pCudescTup, CUDescCUMagicAttr, pCudescTupDesc, &isnull));
    Assert(!isnull);
}

bool CStore::IsTheWholeCuDeleted(int rowsInCu)
{
    return m_hasDeadRow && IsTheWholeCuDeleted((char*)m_cuDelMask, rowsInCu);
}

/*
 * compute the factors of an input value and make: n = 64 * a + 8 * b + 1 * c
 */
static inline void compute_factors_of_n(unsigned int n, unsigned int& a, unsigned int& b, unsigned int& c)
{
    a = (n >> 6);        /* explanation of this statement: a = n/64 */
    b = (n & 0x3F) >> 3; /* explanation of this statement: b = ( (n - a * 64) / 8 )  */
    c = (n & 0x07);      /* explanation of this statement: c = ( n - a * 64 - b * 8) */
}

/*
 * @Description: check whether all tuples within this CU have been deleted.
 *               if so, return true; otherwise return false.
 * @IN rowsInCu: how many tuples to hold within this bitmap
 * @IN delBitmapPtr: deleted bitmap
 * @Return: true if all tuples within this bitmap have been deleted.
 *          false if any tuple is live.
 * @See also:
 */
bool CStore::IsTheWholeCuDeleted(char* delBitmapPtr, int rowsInCu)
{
    static const uint8 map[] = {0, 0x01, 0x03, 0x07, 0x0F, 0x1F, 0x3F, 0x7F, 0xFF};
    unsigned int numUint64 = 0;
    unsigned int numUint8 = 0;
    unsigned int mapIdx = 0;

    /*
     * numUint64 means how many Uint64 data to use when the number of values
     * is rowsInCu; that is (rowsInCu/64) because Uint64 holds 64 bits.
     * numUint8 means how many Uint8 data to use excluding (numUint64 * 64)
     * values; that is ( (rowsInCu - numUint64 * 64) / 8 ). also we exclude
     * the last half-byte.
     */
    compute_factors_of_n((unsigned int)rowsInCu, numUint64, numUint8, mapIdx);

    /* compare quickly by taking *delBitmapPtr* as uint64 array. */
    uint64* uint64Item = (uint64*)delBitmapPtr;
    for (unsigned int i = 0; i < numUint64; ++i) {
        if (*uint64Item != 0xFFFFFFFFFFFFFFFF) {
            return false;
        }
        ++uint64Item;
    }

    /* compare the remainings by taking them as char array. */
    uint8* uint8Item = (uint8*)uint64Item;
    for (unsigned int i = 0; i < numUint8; ++i) {
        if (*uint8Item != 0xFF) {
            return false;
        }
        ++uint8Item;
    }

    /*
     * if (rowsInCu != 8*N), the last byte must be handled specially.
     * we will use *map[]* to compare directly and quickly.
     */
    if (mapIdx != 0) {
        return (*uint8Item == map[mapIdx]);
    }

    /* ok, the whole cu is deleted. */
    return true;
}

Datum CStore::CudescTupGetMinMaxDatum(
    _in_ CUDesc* pCudesc, _in_ Form_pg_attribute pColAttr, _in_ bool min, _out_ bool* shouldFree)
{
    Assert(pCudesc->IsSameValCU());
    *shouldFree = false;

    char* value = NULL;
    char* dataPtr = min ? pCudesc->cu_min : pCudesc->cu_max;
    errno_t rc = EOK;

    if (pColAttr->attbyval) {
        // case 1: attlen > 0 && attlen <= sizeof(Datum)
        return (*(Datum*)dataPtr);
    }

    *shouldFree = true;

    if (pColAttr->attlen > (int)sizeof(Datum)) {
        // case 2: attlen > sizeof(Datum) && attlen <= MIN_MAX_LEN
        Assert(pColAttr->attlen <= MIN_MAX_LEN);
        value = (char*)palloc(pColAttr->attlen);
        rc = memcpy_s(value, pColAttr->attlen, dataPtr, pColAttr->attlen);
        securec_check(rc, "", "");
    } else if (pColAttr->attlen == -1) {
        // case 3: attlen == -1, including empty string ( not null string ).
        Assert((int)dataPtr[0] >= 0 && (int)dataPtr[0] < MIN_MAX_LEN);
        value = (char*)palloc(dataPtr[0] + VARHDRSZ_SHORT);
        SET_VARSIZE_SHORT(value, dataPtr[0] + VARHDRSZ_SHORT);
        if (dataPtr[0] > 0) {
            rc = memcpy_s(value + VARHDRSZ_SHORT, dataPtr[0], dataPtr + 1, dataPtr[0]);
            securec_check(rc, "", "");
        }
    } else {
        // case 4: attlen == -2
        Assert((int)dataPtr[0] > 0 && (int)dataPtr[0] < MIN_MAX_LEN);
        Assert(dataPtr[(int)dataPtr[0]] == '\0');
        value = (char*)palloc(dataPtr[0]);
        rc = memcpy_s(value, dataPtr[0], dataPtr + 1, dataPtr[0]);
        securec_check(rc, "", "");
    }

    return PointerGetDatum(value);
}

// set Cudesc Mode after min/max value have been computed.
// return true if need to write cu file. otherwise return false.
bool CStore::SetCudescModeForMinMaxVal(_in_ bool fullNulls, _in_ bool hasMinMaxFunc, _in_ bool hasNull,
                                       _in_ int maxVarStrLen, _in_ int attlen, __inout CUDesc* cuDescPtr)
{
    if (!fullNulls) {
        // if hasNull is true, it's the NULL bitmap that stores the all null info.
        // it must exists. so don't call SetSameValCU() when this CU has null values.
        // attlen should 0 be larger than 0 and smaller or equal to 8
        if ((attlen > 0 && attlen <= (int)sizeof(Datum)) && hasMinMaxFunc && !hasNull &&
            (*((Datum*)(cuDescPtr->cu_min)) == *((Datum*)(cuDescPtr->cu_max)))) {
            cuDescPtr->SetSameValCU();
        } else if ((attlen < 0) && maxVarStrLen < MIN_MAX_LEN && hasMinMaxFunc && !hasNull) {
            Assert(cuDescPtr->cu_min[0] < MIN_MAX_LEN);
            Assert(cuDescPtr->cu_max[0] < MIN_MAX_LEN);

            if (cuDescPtr->cu_min[0] == cuDescPtr->cu_max[0] &&
                (memcmp(cuDescPtr->cu_min + 1, cuDescPtr->cu_max + 1, cuDescPtr->cu_min[0]) == 0)) {
                cuDescPtr->SetSameValCU();
            }
        } else if ((attlen > (int)sizeof(Datum) && attlen <= MIN_MAX_LEN) && hasMinMaxFunc && !hasNull) {
            if (memcmp(cuDescPtr->cu_min, cuDescPtr->cu_max, attlen) == 0) {
                cuDescPtr->SetSameValCU();
            }
        } else {
            if (hasMinMaxFunc) {
                if (hasNull)
                    cuDescPtr->SetCUHasNull();
                else
                    cuDescPtr->SetNormalCU();
            } else
                cuDescPtr->SetNoMinMaxCU();
        }
    } else
        cuDescPtr->SetNullCU();

    return (!cuDescPtr->IsNullCU() && !cuDescPtr->IsSameValCU());
}

// set *cudesc* mode for one column with new value to be added.
// *attlen* is from Form_pg_attribute.attlen.
// *attval* is computed by the DEFAULT expression.
// true returned if new values must be written into cu files,
// otherwise false returned.
bool CStore::SetCudescModeForTheSameVal(
    _in_ bool fullNulls, _in_ FuncSetMinMax SetMinMaxFunc, _in_ int attlen, _in_ Datum attval, __inout CUDesc* cudesc)
{
    if (!fullNulls) {
        /* flag to set the first value */
        bool first = true;

        if (SetMinMaxFunc == NULL) {
            cudesc->SetNoMinMaxCU();
        } else if (attlen > 0 && attlen <= MIN_MAX_LEN) {
            (SetMinMaxFunc)(attval, cudesc, &first);
            cudesc->SetSameValCU();
        } else if (attlen < 0) {
            char* ptr = DatumGetPointer(attval);
            int len = VARSIZE_ANY(ptr);
            if (len < MIN_MAX_LEN) {
                (SetMinMaxFunc)(attval, cudesc, &first);
                cudesc->SetSameValCU();
            } else
                cudesc->SetNormalCU();
        } else {
            cudesc->SetNormalCU();
        }
    } else
        cudesc->SetNullCU();

    return (!cudesc->IsNullCU() && !cudesc->IsSameValCU());
}

// We add a virtual column for marking deleted rows
// The VC is divided into CUs. The cuDesc of VC includes colId, cuid,
// row_count, del_mask, cu_mode, magic.
void CStore::SaveVCCUDesc(Oid cudescOid, uint32 cuId, int rowCount, uint32 magic, int options, const char* delBitmap)
{
    Relation cudescHeapRel = heap_open(cudescOid, RowExclusiveLock);
    Relation cudescIndexRel = index_open(cudescHeapRel->rd_rel->relcudescidx, RowExclusiveLock);
    TupleDesc tupdesc = RelationGetDescr(cudescHeapRel);

    Datum values[CUDescMaxAttrNum];
    bool nulls[CUDescMaxAttrNum];
    text* tmpCuPointData = NULL;

    errno_t rc = memset_s(nulls, CUDescMaxAttrNum, true, CUDescMaxAttrNum);
    securec_check(rc, "\0", "\0");

    values[CUDescColIDAttr - 1] = Int32GetDatum(VitrualDelColID);
    nulls[CUDescColIDAttr - 1] = false;

    values[CUDescCUIDAttr - 1] = UInt32GetDatum(cuId);
    nulls[CUDescCUIDAttr - 1] = false;

    values[CUDescRowCountAttr - 1] = Int32GetDatum(rowCount);
    nulls[CUDescRowCountAttr - 1] = false;

    values[CUDescCUMagicAttr - 1] = UInt32GetDatum(magic);
    nulls[CUDescCUMagicAttr - 1] = false;

    if (delBitmap != NULL) {
        nulls[CUDescCUPointerAttr - 1] = false;

        int delMaskBytes = (rowCount + 7) / 8;
        tmpCuPointData = cstring_to_text_with_len((const char*)delBitmap, delMaskBytes);
        values[CUDescCUPointerAttr - 1] = PointerGetDatum(tmpCuPointData);
        Assert(VARSIZE_ANY_EXHDR(PointerGetDatum(tmpCuPointData)) == (uint32)delMaskBytes);
    }

    HeapTuple tup = (HeapTuple)tableam_tops_form_tuple(tupdesc, values, nulls);

    // We always generate xlog for cudesc tuple
    options &= (~TABLE_INSERT_SKIP_WAL);
    (void)heap_insert(cudescHeapRel, tup, GetCurrentCommandId(true), options, NULL);
    index_insert(cudescIndexRel,
        values,
        nulls,
        &(tup->t_self),
        cudescHeapRel,
        cudescIndexRel->rd_index->indisunique ? UNIQUE_CHECK_YES : UNIQUE_CHECK_NO);

    heap_freetuple(tup);
    tup = NULL;

    index_close(cudescIndexRel, RowExclusiveLock);
    heap_close(cudescHeapRel, RowExclusiveLock);

    if (tmpCuPointData != NULL) {
        pfree_ext(tmpCuPointData);
    }
}

uint32 CStore::GetMaxCUID(Oid cudescHeap, TupleDesc cstoreRelTupDesc, Snapshot snapshotArg)
{
    ScanKeyData key;
    HeapTuple tup;
    bool isnull = false;

    /* Any snapshot is used here to find the max cu id, which includes the aborted or crashed transactions. */
    Snapshot snapshot = NULL;
    snapshot = snapshotArg ? snapshotArg : SnapshotAny;

    // find a column which is not dropped.
    int attrId = 0;
    for (int i = 0; i < cstoreRelTupDesc->natts; ++i) {
        if (!cstoreRelTupDesc->attrs[i].attisdropped) {
            attrId = cstoreRelTupDesc->attrs[i].attnum;
            break;
        }
    }
    Assert(attrId > 0);

    // Open the CUDesc relation and its index
    Relation heapRel = heap_open(cudescHeap, AccessShareLock);
    TupleDesc heapTupDesc = RelationGetDescr(heapRel);
    Relation indexRel = index_open(heapRel->rd_rel->relcudescidx, AccessShareLock);
    uint32 maxCuId = FirstCUID;

    // Setup scan key to fetch from the index by col_id.
    ScanKeyInit(&key, (AttrNumber)CUDescColIDAttr, BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(attrId));

    SysScanDesc cudesc_scan = systable_beginscan_ordered(heapRel, indexRel, snapshot, 1, &key);
    // Use BackwardScanDirection scan to Optimize for geting last CU description of column.
    if ((tup = systable_getnext_ordered(cudesc_scan, BackwardScanDirection)) != NULL) {
        maxCuId = DatumGetUInt32(fastgetattr(tup, CUDescCUIDAttr, heapTupDesc, &isnull));
        Assert(!isnull);
    }

    systable_endscan_ordered(cudesc_scan);

    index_close(indexRel, AccessShareLock);
    heap_close(heapRel, AccessShareLock);

    return maxCuId;
}

static uint32 GetMaxCuIdFromCbtreeIndex(Relation heapRel, Relation idxRel)
{
    uint32 maxCuID = FirstCUID;
    ItemPointer tid;

        IndexScanDesc indexScan = (IndexScanDesc)index_beginscan(heapRel, idxRel, GetActiveSnapshot(), 0, 0);
    index_rescan(indexScan, NULL, 0, NULL, 0);
    while ((tid = index_getnext_tid(indexScan, ForwardScanDirection)) != NULL) {
        uint32 tmpCuID = ItemPointerGetBlockNumber(tid);
        if (tmpCuID > maxCuID) {
            maxCuID = tmpCuID;
        }
    }
    index_endscan(indexScan);

    return maxCuID;
}

/* get max CU id from cgin index relation */
static uint32 GetMaxCuIdFromCginIndex(Relation heapRel, Relation idxRel)
{
    uint32 maxCuID = FirstCUID;

    TupleDesc tidDesc = CreateTemplateTupleDesc(1, false);
    TupleDescInitEntry(tidDesc, 1, "tid", TIDOID, -1, 0);
    VectorBatch *tids = New(CurrentMemoryContext)VectorBatch(CurrentMemoryContext, tidDesc);

    IndexScanDesc indexScan = index_beginscan_bitmap(idxRel, GetActiveSnapshot(), 0);
    /* If sort is NULL, tids only contain 1 row to get the max cu ID */
    int64 nTids = index_column_getbitmap(indexScan, NULL, tids);
    if (nTids == 1) {
        /* get the max cu ID */
        ScalarVector *pVector = &tids->m_arr[0];
        ItemPointer tid = (ItemPointer)(pVector->m_vals);
        maxCuID = ItemPointerGetBlockNumber(tid);
    }
    index_endscan(indexScan);
    FreeTupleDesc(tidDesc);

    return maxCuID;
}

uint32 CStore::GetMaxIndexCUID(Relation heapRel, List *indexRel)
{
    ListCell *lc = NULL;
    uint32 maxCuID = FirstCUID;
    uint32 tmpCuID = 0;
    foreach (lc, indexRel) {
        Relation idxRel = (Relation)lfirst(lc);
        switch (idxRel->rd_rel->relam) {
            case CBTREE_AM_OID: {
                tmpCuID = GetMaxCuIdFromCbtreeIndex(heapRel, idxRel);
                break;
            }
            case CGIN_AM_OID: {
                tmpCuID = GetMaxCuIdFromCginIndex(heapRel, idxRel);
                break;
            }
            default: {
                Assert(0);
                break;
            }
        }
        if (tmpCuID > maxCuID) {
            maxCuID = tmpCuID;
        }
    }
    return maxCuID;
}

// get the max cu pointer form cu desc
CUPointer CStore::GetMaxCUPointerFromDesc(_in_ int attrno, _in_ Oid cudescHeap)
{
    // Open the CUDesc relation and its index.
    Relation cudescHeapRel = heap_open(cudescHeap, AccessShareLock);
    TupleDesc cudescTupDesc = RelationGetDescr(cudescHeapRel);
    Relation cudescIndexRel = index_open(cudescHeapRel->rd_rel->relcudescidx, AccessShareLock);

    // Setup scan key to fetch from the index by col_id.
    ScanKeyData key;
    ScanKeyInit(&key, (AttrNumber)CUDescColIDAttr, BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(attrno));

    // Any snapshot is used to get the newest CU Pointer, which includes the aborted or crashed transactions.
    SysScanDesc cudesc_scan = systable_beginscan_ordered(cudescHeapRel, cudescIndexRel, SnapshotAny, 1, &key);

    // Optimize for geting last CU description of column.
    // Use BackwardScanDirection scan
    CUPointer maxCUPointer = 0;
    HeapTuple tup = NULL;
    while ((tup = systable_getnext_ordered(cudesc_scan, BackwardScanDirection)) != NULL) {
        bool isnull = true;
        uint32 cuSize = DatumGetInt32(fastgetattr(tup, CUDescSizeAttr, cudescTupDesc, &isnull));
        Assert(!isnull);
        char* cuBegin = DatumGetPointer(fastgetattr(tup, CUDescCUPointerAttr, cudescTupDesc, &isnull));
        Assert(!isnull);
        uint64 cuEnd = *((uint64*)VARDATA_ANY(cuBegin)) + cuSize;

        if (cuEnd > maxCUPointer) {
            maxCUPointer = cuEnd;
        }
    }

    systable_endscan_ordered(cudesc_scan);
    cudesc_scan = NULL;

    index_close(cudescIndexRel, AccessShareLock);
    cudescIndexRel = NULL;
    heap_close(cudescHeapRel, AccessShareLock);
    cudescHeapRel = NULL;
    return maxCUPointer;
}

// get the max cu pointer
CUPointer CStore::GetMaxCUPointer(_in_ int attrno, _in_ Relation rel)
{
    CUPointer maxPointerfromCudesc = 0;
    CUPointer maxPointerfromCufile = 0;

    // get max cu pointer from cudesc
    maxPointerfromCudesc = CStore::GetMaxCUPointerFromDesc(attrno, rel->rd_rel->relcudescrelid);

    /* if process is killed when insert CU to the end of CU file,  when process restart the transaction of insert will
     * abort the GetMaxCUPointerFromDesc which using DirtySnapshot will not get the max cu pointer so need check the CU
     * file size and compare which one is bigger
     *
     * get max cu pointer from cu file
     */
    maxPointerfromCufile = GetColDataFileSize(rel, attrno);

    // cu file may partital write when process is killed, so allign the maxPointerfromCufile to ALIGNOF_CUSIZE (8K)
    int align_size = RelationIsTsStore(rel) ? ALIGNOF_TIMESERIES_CUSIZE : ALIGNOF_CUSIZE;
    maxPointerfromCufile = CUAlignUtils::AlignCuSize(maxPointerfromCufile, align_size);

    return Max(maxPointerfromCudesc, maxPointerfromCufile);
}

// This function will save the CU description of CU into CUDesc table
// which is a rowstore table. thus we can leverage the visibility check of
// rowstore. Note that we use attribute number in order to support
// 'alter table add/drop table'.
// attno is physical attribute number
void CStore::SaveCUDesc(_in_ Relation rel, _in_ CUDesc* cuDescPtr, _in_ int col, int options)
{
    Assert(rel != NULL);
    Assert(col >= 0);

    if (rel->rd_att->attrs[col].attisdropped) {
        ereport(PANIC,
                (errmsg("Cannot save CUDesc for a dropped column \"%s\" of table \"%s\"",
                        NameStr(rel->rd_att->attrs[col].attname),
                        RelationGetRelationName(rel))));
    }

    Relation cudesc_rel = heap_open(rel->rd_rel->relcudescrelid, RowExclusiveLock);
    Relation idx_rel = index_open(cudesc_rel->rd_rel->relcudescidx, RowExclusiveLock);

    Datum values[CUDescMaxAttrNum];
    bool nulls[CUDescMaxAttrNum];
    HeapTuple tup = CStore::FormCudescTuple(cuDescPtr, cudesc_rel->rd_att, values, nulls, &rel->rd_att->attrs[col]);

    // We always generate xlog for cudesc tuple
    options &= (~TABLE_INSERT_SKIP_WAL);

    (void)heap_insert(cudesc_rel, tup, GetCurrentCommandId(true), options, NULL);
    index_insert(idx_rel,
                       values,
                       nulls,
                       &(tup->t_self),
                       cudesc_rel,
                       idx_rel->rd_index->indisunique ? UNIQUE_CHECK_YES : UNIQUE_CHECK_NO);

    heap_freetuple(tup);
    pfree(DatumGetPointer(values[CUDescMinAttr - 1]));
    pfree(DatumGetPointer(values[CUDescMaxAttr - 1]));
    pfree(DatumGetPointer(values[CUDescCUPointerAttr - 1]));

    index_close(idx_rel, RowExclusiveLock);
    heap_close(cudesc_rel, RowExclusiveLock);
}

/*
 * Load CUDesc information of column according to loadInfoPtr
 * LoadCUDescCtrl include maxCUDescNum for this load, because if we load all
 * it need big memory to hold
 * this function is special for adio, third param adio_work control adio like enable_adio_function.
 * because GetLivedRowNumbers should not work in adio model
 */
bool CStore::LoadCUDesc(
    _in_ int col, __inout LoadCUDescCtl* loadCUDescInfoPtr, _in_ bool prefetch_control, _in_ Snapshot snapShot)
{
    ScanKeyData key[3];
    HeapTuple tup;
    errno_t rc = EOK;
    bool found = false;
    int loadNum = 0;

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

    CUDesc* cuDescArray = loadCUDescInfoPtr->cuDescArray;
    /*
     * Open the CUDesc relation and its index
     */
    Relation cudesc_rel = heap_open(m_relation->rd_rel->relcudescrelid, AccessShareLock);
    TupleDesc cudesc_tupdesc = cudesc_rel->rd_att;
    Relation idx_rel = index_open(cudesc_rel->rd_rel->relcudescidx, AccessShareLock);
    bool needLengthInfo = m_relation->rd_att->attrs[col].attlen < 0;
    /* Convert logical id is to physical id of attribute */
    int attid = m_relation->rd_att->attrs[col].attnum;

    /*
     * Setup scan key to fetch from the index by attid and CU ID range.
     */
    ScanKeyInit(&key[0], (AttrNumber)CUDescColIDAttr, BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(attid));

    ScanKeyInit(&key[1],
                (AttrNumber)CUDescCUIDAttr,
                BTGreaterEqualStrategyNumber,
                F_OIDGE,
                UInt32GetDatum(loadCUDescInfoPtr->nextCUID));

    ScanKeyInit(&key[2], (AttrNumber)CUDescCUIDAttr, BTLessEqualStrategyNumber, F_OIDLE, UInt32GetDatum(m_endCUID));

    snapShot = (snapShot == NULL) ? GetActiveSnapshot() : snapShot;

    Assert(snapShot != NULL);

    SysScanDesc cudesc_scan = systable_beginscan_ordered(cudesc_rel, idx_rel, snapShot, 3, key);
    /* Scan cudesc tuple order by cuid ascending order */
    while ((tup = systable_getnext_ordered(cudesc_scan, ForwardScanDirection)) != NULL) {
        Datum values[CUDescCUExtraAttr] = {0};
        bool isnull[CUDescCUExtraAttr] = {0};
        char* valPtr = NULL;

        /* here use heap_deform_tuple() because cudesc tupe stored was bad.
         * min and max are var length but store in middle of tuple, if we use  fastgetattr()
         * here may cause high cpu cost.
         * by the way, it is better store tupe in form of
         *   attribute 1: fixed length
         *   attribute 2: fixed length
         *   ......        : fixed length
         *   attribute n: var length
         *   attribute n+1: var length
         *    ......        : var length
         */
        heap_deform_tuple(tup, cudesc_tupdesc, values, isnull);

        uint32 cu_id = DatumGetUInt32(values[CUDescCUIDAttr - 1]);
        Assert(!isnull[CUDescCUIDAttr - 1]);

        if (IsDicVCU(cu_id))
            continue;

        /* Put cusize into cudesc->cu_size */
        int32 cu_size = DatumGetInt32(values[CUDescSizeAttr - 1]);
        Assert(!isnull[CUDescSizeAttr - 1]);

        ADIO_RUN()
        {
            loadNum = (int)loadCUDescInfoPtr->curLoadNum;
            IncLoadCuDescIdx(loadNum);
            /* case1: check whether can load more ;case 2: m_virtualCUDescInfo check here  whether array overflow */
            if (m_CUDescIdx[m_cursor] == loadNum || !HasEnoughCuDescSlot(loadCUDescInfoPtr->lastLoadNum, loadNum)) {
                break;
            }
            m_prefetch_quantity += cu_size;
        }
        ADIO_ELSE()
        {
            if (!loadCUDescInfoPtr->HasFreeSlot())
                break;
        }
        ADIO_END();

        cuDescArray[loadCUDescInfoPtr->curLoadNum].cu_size = cu_size;
        cuDescArray[loadCUDescInfoPtr->curLoadNum].xmin = HeapTupleGetRawXmin(tup);
        cuDescArray[loadCUDescInfoPtr->curLoadNum].cu_id = cu_id;
        loadCUDescInfoPtr->nextCUID = cu_id;

        /* Parallel scan CU divide. */
        if (u_sess->stream_cxt.producer_dop > 1 &&
            (cu_id % u_sess->stream_cxt.producer_dop != (uint32)u_sess->stream_cxt.smp_id))
            continue;

        /* Put min value into cudesc->min */
        if (!isnull[CUDescMinAttr - 1]) {
            char* minPtr = cuDescArray[loadCUDescInfoPtr->curLoadNum].cu_min;
            int len_1 = MIN_MAX_LEN;
            valPtr = DatumGetPointer(values[CUDescMinAttr - 1]);
            if (needLengthInfo) {
                *minPtr = VARSIZE_ANY_EXHDR(valPtr);
                minPtr = minPtr + 1;
                len_1 -= 1;
            }
            rc = memcpy_s(minPtr, len_1, VARDATA_ANY(valPtr), VARSIZE_ANY_EXHDR(valPtr));
            securec_check(rc, "", "");
        }
        /* Put max value into cudesc->max */
        if (!isnull[CUDescMaxAttr - 1]) {
            char* maxPtr = cuDescArray[loadCUDescInfoPtr->curLoadNum].cu_max;
            int len_2 = MIN_MAX_LEN;
            valPtr = DatumGetPointer(values[CUDescMaxAttr - 1]);
            if (needLengthInfo) {
                *maxPtr = VARSIZE_ANY_EXHDR(valPtr);
                maxPtr = maxPtr + 1;
                len_2 -= 1;
            }
            rc = memcpy_s(maxPtr, len_2, VARDATA_ANY(valPtr), VARSIZE_ANY_EXHDR(valPtr));
            securec_check(rc, "", "");
        }

        cuDescArray[loadCUDescInfoPtr->curLoadNum].row_count = DatumGetInt32(values[CUDescRowCountAttr - 1]);
        Assert(!isnull[CUDescRowCountAttr - 1]);

        /* Put CUMode into cudesc->cumode */
        cuDescArray[loadCUDescInfoPtr->curLoadNum].cu_mode = DatumGetInt32(values[CUDescCUModeAttr - 1]);
        Assert(!isnull[CUDescCUModeAttr - 1]);

        /* Put CUPointer into cudesc->cuPointer */
        Assert(col != VitrualDelColID);
        Assert(!isnull[CUDescCUPointerAttr - 1]);
        valPtr = DatumGetPointer(values[CUDescCUPointerAttr - 1]);
        rc = memcpy_s(&cuDescArray[loadCUDescInfoPtr->curLoadNum].cu_pointer,
                      sizeof(CUPointer),
                      VARDATA_ANY(valPtr),
                      sizeof(CUPointer));
        securec_check(rc, "", "");
        Assert(VARSIZE_ANY_EXHDR(valPtr) == sizeof(CUPointer));

        /* Put magic into cudesc->magic */
        cuDescArray[loadCUDescInfoPtr->curLoadNum].magic = DatumGetUInt32(values[CUDescCUMagicAttr - 1]);
        Assert(!isnull[CUDescCUMagicAttr - 1]);

        found = true;

        IncLoadCuDescIdx(*(int*)&loadCUDescInfoPtr->curLoadNum);
        /* only load one cu for adio,because we need caculate cu size for prefetch quantity */
        if (prefetch_control) {
            break;
        }
    }

    systable_endscan_ordered(cudesc_scan);
    index_close(idx_rel, AccessShareLock);
    heap_close(cudesc_rel, AccessShareLock);

    ADIO_RUN()
    {
        if (tup == NULL) {
            /* no tup found means prefetch finish */
            m_load_finish = true;
        }
    }
    ADIO_END();

    if (found) {
        /* nextCUID must be greater than loaded cudesc */
        loadCUDescInfoPtr->nextCUID++;
        return true;
    }
    return false;
}

int CStore::FillVecBatch(_out_ VectorBatch* vecBatchOut)
{
    Assert(vecBatchOut);

    int idx = m_CUDescIdx[m_cursor];
    int deadRows = 0, i;
    this->m_cuDescIdx = idx;
    bool hasCtidForLateRead = false;

    /* Step 1: fill normal columns if need */
    for (i = 0; i < m_colNum; ++i) {
        int colIdx = m_colId[i];

        if (m_relation->rd_att->attrs[colIdx].attisdropped) {
            ereport(PANIC,
                    (errmsg("Cannot fill VecBatch for a dropped column \"%s\" of table \"%s\"",
                            NameStr(m_relation->rd_att->attrs[colIdx].attname),
                            RelationGetRelationName(m_relation))));
        }
        if (likely(colIdx >= 0)) {
            Assert(colIdx < vecBatchOut->m_cols);

            ScalarVector* vec = vecBatchOut->m_arr + colIdx;
            CUDesc* cuDescPtr = m_CUDescInfo[i]->cuDescArray + idx;
            GetCUDeleteMaskIfNeed(cuDescPtr->cu_id, m_snapshot);

            // We can't late read data
            if (!IsLateRead(i)) {
                int funIdx = m_hasDeadRow ? 1 : 0;
                deadRows = (this->*m_colFillFunArrary[i].colFillFun[funIdx])(i, cuDescPtr, vec);
            } else {
                // We haven't fill ctid for late read columns
                if (!hasCtidForLateRead) {
                    if (!m_hasDeadRow)
                        deadRows = FillTidForLateRead<false>(cuDescPtr, vec);
                    else
                        deadRows = FillTidForLateRead<true>(cuDescPtr, vec);

                    hasCtidForLateRead = true;
                    this->m_laterReadCtidColIdx = colIdx;
                } else
                    vec->m_rows = vecBatchOut->m_rows;
            }
            vecBatchOut->m_rows = vec->m_rows;
        }
    }

    // Step 2: fill sys columns if need
    for (i = 0; i < m_sysColNum; ++i) {
        int sysColIdx = m_sysColId[i];
        ScalarVector* sysVec = vecBatchOut->GetSysVector(sysColIdx);
        deadRows = FillSysColVector(sysColIdx, m_virtualCUDescInfo->cuDescArray + idx, sysVec);
        vecBatchOut->m_rows = sysVec->m_rows;
    }

    // Step 3: fill const columns if need
    if (unlikely(m_onlyConstCol)) {
        // We only set row count
        CUDesc* cuDescPtr = m_virtualCUDescInfo->cuDescArray + idx;
        int liveRows = 0, leftSize = cuDescPtr->row_count - m_rowCursorInCU;
        ScalarVector* vec = vecBatchOut->m_arr;
        errno_t rc = memset_s(vec->m_flag, sizeof(uint8) * BatchMaxSize, 0, sizeof(uint8) * BatchMaxSize);
        securec_check(rc, "", "");
        Assert(deadRows == 0 && leftSize > 0);

        GetCUDeleteMaskIfNeed(cuDescPtr->cu_id, m_snapshot);

        for (i = 0; i < leftSize && liveRows < BatchMaxSize; i++) {
            if (IsDeadRow(cuDescPtr->cu_id, i + m_rowCursorInCU))
                ++deadRows;
            else
                ++liveRows;
        }
        vec->m_rows = liveRows;
        vecBatchOut->m_rows = vec->m_rows;
    }
    /* Step 4: fill other columns if need, most likely for the dropped column */
    for (i = 0; i < vecBatchOut->m_cols; i++) {
        if (m_relation->rd_att->attrs[i].attisdropped) {
            ScalarVector* vec = vecBatchOut->m_arr + i;
            vec->m_rows = vecBatchOut->m_rows;
            vec->SetAllNull();
        }
    }

    return deadRows;
}

// Fill vector of column
template <bool hasDeadRow, int attlen>
int CStore::FillVector(_in_ int seq, _in_ CUDesc* cuDescPtr, _out_ ScalarVector* vec)
{
    int colIdx = this->m_colId[seq];
    int pos = 0;
    int deadRows = 0;

    // reset the flag value
    errno_t rc = memset_s(vec->m_flag, sizeof(uint8) * BatchMaxSize, 0, sizeof(uint8) * BatchMaxSize);
    securec_check(rc, "", "");

    // step 1: Caculate how many rows left
    int leftRows = cuDescPtr->row_count - this->m_rowCursorInCU;
    Assert(leftRows > 0);

    // step 2: CU is filled with all NULL values
    if (cuDescPtr->IsNullCU()) {
        for (int i = 0; i < leftRows && pos < BatchMaxSize; ++i) {
            if (hasDeadRow && this->IsDeadRow(cuDescPtr->cu_id, i + this->m_rowCursorInCU)) {
                ++deadRows;
                continue;
            }
            vec->SetNull(pos);
            ++pos;
        }
        vec->m_rows = pos;
        return deadRows;
    }

    // step 3: If min and max are equal, no CU is stored
    if (cuDescPtr->IsSameValCU()) {
        for (int i = 0; i < leftRows && pos < BatchMaxSize; ++i) {
            if (hasDeadRow && this->IsDeadRow(cuDescPtr->cu_id, i + this->m_rowCursorInCU)) {
                ++deadRows;
                continue;
            }

            if (attlen > 0 && attlen <= 8) {
                Datum cuMin = *(Datum*)(cuDescPtr->cu_min);
                vec->m_vals[pos] = cuMin;
            } else if (attlen == 12 || attlen == 16) {
                Datum cuMin = PointerGetDatum(cuDescPtr->cu_min);
                vec->AddVar(cuMin, pos);
            } else {
                Datum cuMin = PointerGetDatum(cuDescPtr->cu_min + 1);
                Size len = (Size)(unsigned char)cuDescPtr->cu_min[0];
                Assert(len < MIN_MAX_LEN);

                // Convert string into varattrib_1b
                // It is safe because len < MIN_MAX_LEN
                char tmpStr[MIN_MAX_LEN + 4];
                if (attlen == -1) {
                    Size varLen = len + VARHDRSZ_SHORT;
                    SET_VARSIZE_SHORT(tmpStr, varLen);
                    rc = memcpy_s(VARDATA_ANY(tmpStr), sizeof(tmpStr) - VARHDRSZ_SHORT, DatumGetPointer(cuMin), len);
                    securec_check(rc, "", "");
                    cuMin = PointerGetDatum(tmpStr);
                }
                vec->AddVar(cuMin, pos);
            }
            ++pos;
        }
        vec->m_rows = pos;
        return deadRows;
    }

    // step 4: Get CU data. Add a 'this' pointer to help sourceinsight understands
    // this is a member function reference.
    int slotId = CACHE_BLOCK_INVALID_IDX;
    CSTORESCAN_TRACE_START(GET_CU_DATA);
    CU* cuPtr = this->GetCUData(cuDescPtr, colIdx, attlen, slotId);
    CSTORESCAN_TRACE_END(GET_CU_DATA);

    // step 5: CUToVector
    pos = cuPtr->ToVector<attlen, hasDeadRow>(
              vec, leftRows, this->m_rowCursorInCU, this->m_scanPosInCU[seq], deadRows, this->m_cuDelMask);

    if (IsValidCacheSlotID(slotId)) {
        // CU is pinned
        CUCache->UnPinDataBlock(slotId);
    } else
        Assert(false);

    vec->m_rows = pos;
    return deadRows;
}

void CStore::FillVectorByIndex(
    _in_ int colIdx, _in_ ScalarVector* tids, _in_ ScalarVector* srcVec, _out_ ScalarVector* destVec)
{
    Assert(colIdx >= 0 && tids && destVec && srcVec);
    uint32 curCUId = InValidCUID, thisCUId, rowOffset;

    ScalarValue* destValue = destVec->m_vals;
    ScalarValue* srcValue = srcVec->m_vals;
    ScalarValue* tidValue = tids->m_vals;

    for (int i = 0; i < tids->m_rows; i++) {
        ItemPointer tidPtr = (ItemPointer)&tidValue[i];
        thisCUId = ItemPointerGetBlockNumber(tidPtr);

        // Note that tidPointer->rowOffset start from 1
        rowOffset = ItemPointerGetOffsetNumber(tidPtr) - 1;

        // Step 1: Get delmask if need
        if (curCUId != thisCUId) {
            curCUId = thisCUId;
            GetCUDeleteMaskIfNeed(curCUId, m_snapshot);
        }

        // Step 2: It is a live row, not a dead row
        // We need fill vector
        if (m_delMaskCUId != InValidCUID && !IsDeadRow(curCUId, rowOffset)) {
            if (srcVec->IsNull(i))
                destVec->SetNull(destVec->m_rows++);
            else
                destValue[destVec->m_rows++] = srcValue[i];
        }
    }
}

template <int sysColOid>
void CStore::FillSysVecByTid(_in_ ScalarVector* tids, _out_ ScalarVector* destVec)
{
    Assert(tids && destVec);
    uint32 curCUId = InValidCUID, thisCUId, rowOffset;
    ScalarValue* destValue = destVec->m_vals;
    ScalarValue* tidValue = tids->m_vals;
    destVec->m_rows = 0;
    TransactionId xmin = InvalidTransactionId;

    for (int i = 0; i < tids->m_rows; i++) {
        ItemPointer tidPtr = (ItemPointer)&tidValue[i];
        thisCUId = ItemPointerGetBlockNumber(tidPtr);

        // Note that tidPointer->rowOffset start from 1
        rowOffset = ItemPointerGetOffsetNumber(tidPtr) - 1;

        // Step 1: Get CUDesc and delmask if need
        if (curCUId != thisCUId) {
            curCUId = thisCUId;
            this->GetCUDeleteMaskIfNeed(curCUId, m_snapshot);

            if (this->m_delMaskCUId != InValidCUID && sysColOid == MinTransactionIdAttributeNumber) {
                xmin = this->GetCUXmin(curCUId);
            }
        }
        // Step 2: It is a live row, not a dead row
        // We need fill vector
        if (this->m_delMaskCUId != InValidCUID && !this->IsDeadRow(curCUId, rowOffset)) {
            switch (sysColOid) {
                case SelfItemPointerAttributeNumber: {
                    destValue[destVec->m_rows++] = *(ScalarValue*)tidPtr;
                    break;
                }
                case XC_NodeIdAttributeNumber: {
                    destValue[destVec->m_rows++] = u_sess->pgxc_cxt.PGXCNodeIdentifier;
                    break;
                }
                case TableOidAttributeNumber: {
                    destValue[destVec->m_rows++] = RelationGetRelid(m_relation);
                    break;
                }
                case MinTransactionIdAttributeNumber: {
                    destValue[destVec->m_rows++] = xmin;
                    break;
                }
                default:
                    ereport(ERROR,
                            (errcode(ERRCODE_DATATYPE_MISMATCH),
                             (errmsg("Cannot to fill unsupported system column %d for column store table", sysColOid))));
                    break;
            }
        }
    }
}

/*
 * @Description: fill vector by tid in cstore scan late read.
 * @in colIdx: the index of the this column.
 * @in tids: the tid vector.
 * @in cuDescPtr: the pointer to the CUDesc.
 * @out vec: the output ScalarVector.
 * @template attlen: the length of this column.
 */
template <int attlen>
void CStore::FillVectorLateRead(
    _in_ int colIdx, _in_ ScalarVector* tids, _in_ CUDesc* cuDescPtr, _out_ ScalarVector* vec)
{
    ScalarValue* tidVals = tids->m_vals;
    ItemPointer tidPtr = NULL;

    uint32 tmpCuId = InValidCUID;
    uint32 tmpOffset = 0;

    int pos = 0;

    // Case 1: It is full of NULL value
    if (cuDescPtr->IsNullCU()) {
        for (int rowCnt = 0; rowCnt < tids->m_rows; ++rowCnt) {
            tidPtr = (ItemPointer)(tidVals + rowCnt);
            tmpCuId = ItemPointerGetBlockNumber(tidPtr);
            tmpOffset = ItemPointerGetOffsetNumber(tidPtr) - 1;
            if (this->IsDeadRow(cuDescPtr->cu_id, tmpOffset)) {
                continue;
            }
            vec->SetNull(pos);
            pos++;
        }

        vec->m_rows = pos;
        return;
    }

    // Case 2: It is full of the same value
    if (cuDescPtr->IsSameValCU()) {
        for (int rowCnt = 0; rowCnt < tids->m_rows; ++rowCnt) {
            tidPtr = (ItemPointer)(tidVals + rowCnt);
            tmpCuId = ItemPointerGetBlockNumber(tidPtr);
            tmpOffset = ItemPointerGetOffsetNumber(tidPtr) - 1;
            if (this->IsDeadRow(cuDescPtr->cu_id, tmpOffset)) {
                continue;
            }

            if (attlen > 0 && attlen <= 8) {
                Datum cuMin = *(Datum*)(cuDescPtr->cu_min);
                vec->m_vals[pos] = cuMin;
            } else if (attlen == 12 || attlen == 16) {
                Datum cuMin = PointerGetDatum(cuDescPtr->cu_min);
                vec->AddVar(cuMin, pos);
            } else {
                Datum cuMin = PointerGetDatum(cuDescPtr->cu_min + 1);
                Size len = (Size)(unsigned char)cuDescPtr->cu_min[0];
                Assert(len < MIN_MAX_LEN);

                // Convert string into varattrib_1b
                // It is safe because len < MIN_MAX_LEN
                char tmpStr[MIN_MAX_LEN + VARHDRSZ];
                if (attlen == -1) {
                    SET_VARSIZE_SHORT(tmpStr, len + VARHDRSZ_SHORT);
                    errno_t rc =
                        memcpy_s(tmpStr + VARHDRSZ_SHORT, sizeof(tmpStr) - VARHDRSZ_SHORT, DatumGetPointer(cuMin), len);
                    securec_check(rc, "\0", "\0");
                    cuMin = PointerGetDatum(tmpStr);
                }

                vec->AddVar(cuMin, pos);
            }
            pos++;
        }

        vec->m_rows = pos;
        return;
    }

    // Case 3: It is a normal CU
    int slotId = CACHE_BLOCK_INVALID_IDX;
    CSTORESCAN_TRACE_START(GET_CU_DATA_LATER_READ);
    CU* cuPtr = this->GetCUData(cuDescPtr, colIdx, attlen, slotId);
    CSTORESCAN_TRACE_END(GET_CU_DATA_LATER_READ);

    if (cuPtr->HasNullValue()) {
        pos = cuPtr->ToVectorLateRead<attlen, true>(tids, vec);
    } else {
        pos = cuPtr->ToVectorLateRead<attlen, false>(tids, vec);
    }

    if (IsValidCacheSlotID(slotId)) {
        // CU is pinned
        CUCache->UnPinDataBlock(slotId);
    } else {
        Assert(false);
    }

    vec->m_rows = pos;
    return;
}

template <int attlen>
void CStore::FillVectorByTids(_in_ int colIdx, _in_ ScalarVector* tids, _out_ ScalarVector* vec)
{
    ScalarValue* tidVals = tids->m_vals;
    ItemPointer tidPtr = NULL;

    uint32 curCUId = InValidCUID;
    uint32 tmpCuId = InValidCUID;

    uint32 tmpOffset = 0;
    uint32 firstOffset = 0;
    uint32 nextOffset = 0;

    CUDesc cuDesc;
    int pos = 0;
    int contiguous = 0;
    bool found = false;

    // we will do only once Pin/Unpin cache within the same cu.
    bool needLoadCu = false;
    int slot = CACHE_BLOCK_INVALID_IDX;
    CU* lastCU = NULL;
    errno_t rc = EOK;

    // Main copy procedure: copy each value into the output vector. Be careful
    // to reuse previous value's CU and CU descriptor.
    for (int rowCnt = 0; rowCnt < tids->m_rows; ++rowCnt) {
        // Note that tidPointer->tmpOffset start from 1
        tidPtr = (ItemPointer)(tidVals + rowCnt);
        tmpCuId = ItemPointerGetBlockNumber(tidPtr);
        tmpOffset = ItemPointerGetOffsetNumber(tidPtr) - 1;

        // Step 1: Get CUDesc and deletion mask if needed
        if (curCUId != tmpCuId) {
            if (lastCU != NULL) {
                // switch to new cu. so at first unpin the
                // previous cu cache as earlier as possible.
                Assert(slot != CACHE_BLOCK_INVALID_IDX);
                CUCache->UnPinDataBlock(slot);

                // reset after unpin action.
                lastCU = NULL;
                slot = CACHE_BLOCK_INVALID_IDX;
            }

            // fetch cudesc tuple and deletion bitmap.
            curCUId = tmpCuId;
            found = this->GetCUDesc(colIdx, curCUId, &cuDesc, this->m_snapshot);
            if (!found) {
                if (m_useBtreeIndex) {
                    m_delMaskCUId = InValidCUID;
                    continue;
                } else {
                    Assert(false);
                    ereport(FATAL,
                            (errmsg("compression unit descriptor not found, table(%s), column(%s), relfilenode(%u/%u/%u), "
                                    "cuid(%u)).",
                                    RelationGetRelationName(this->m_relation),
                                    NameStr(this->m_relation->rd_att->attrs[colIdx].attname),
                                    this->m_relation->rd_node.spcNode,
                                    this->m_relation->rd_node.dbNode,
                                    this->m_relation->rd_node.relNode,
                                    curCUId)));
                }
            } else {
                this->GetCUDeleteMaskIfNeed(curCUId, this->m_snapshot);
            }

            // indicate to load data if needed when switch to a new cu.
            needLoadCu = true;
        }

        // check if the current cu is valid(visible)
        if (m_delMaskCUId == InValidCUID)
            continue;

        // step 2: compute how many data contiguous within the same cu.
        contiguous = 0;
        nextOffset = tmpOffset;
        firstOffset = tmpOffset;

        while (tmpCuId == curCUId                        // within the same cu.
               && tmpOffset == nextOffset                // contiguous offset.
               && !this->IsDeadRow(curCUId, tmpOffset))  // it's a dead data.
        {
            ++contiguous;
            ++nextOffset;

            if (unlikely(++rowCnt == tids->m_rows))
                break;

            // fetch and check the next data
            // contiguous within the same cu.
            tidPtr = (ItemPointer)(tidVals + rowCnt);
            tmpCuId = ItemPointerGetBlockNumber(tidPtr);
            tmpOffset = ItemPointerGetOffsetNumber(tidPtr) - 1;
        }

        if (unlikely(contiguous == 0)) {
            // this is a dead data, so check the next data.
            Assert(this->IsDeadRow(curCUId, tmpOffset));
            continue;
        } else if (tmpCuId != curCUId || !this->IsDeadRow(curCUId, tmpOffset)) {
            // if it's the first data of the next cu,
            // or the first new offset within the same cu is not a
            // dead data, we have to check it again.
            --rowCnt;
        }

        /*
         * step 3: fill the output vector.
         * Case 1: It is full of NULL value
         */
        if (cuDesc.IsNullCU()) {
            for (int k = 0; k < contiguous; ++k)
                vec->SetNull(pos++);
            continue;
        }

        // Case 2: It is full of the same value
        if (cuDesc.IsSameValCU()) {
            if (attlen > 0 && attlen <= 8) {
                Datum cuMin = *(Datum*)(cuDesc.cu_min);
                ScalarValue* dest = vec->m_vals + pos;

                // batch assign cuMin to dest[contiguous].
                for (uint32 k = 0; k < ((uint32)contiguous >> 2); ++k) {
                    *dest++ = cuMin;
                    *dest++ = cuMin;
                    *dest++ = cuMin;
                    *dest++ = cuMin;
                }
                for (int k = 0; k < (contiguous & 0x03); ++k) {
                    *dest++ = cuMin;
                }
                pos += contiguous;
            } else if (attlen == 12 || attlen == 16) {
                // NB: be careful AddVar() will insert 1B header
                // 	 before each value.
                Datum cuMin = PointerGetDatum(cuDesc.cu_min);
                for (int k = 0; k < contiguous; ++k)
                    vec->AddVar(cuMin, pos++);
            } else {
                Datum cuMin = PointerGetDatum(cuDesc.cu_min + 1);
                Size len = (Size)(unsigned char)cuDesc.cu_min[0];
                Assert(len < MIN_MAX_LEN);

                // Convert string into varattrib_1b
                // It is safe because len < MIN_MAX_LEN
                char tmpStr[MIN_MAX_LEN + 4];
                if (attlen == -1) {
                    SET_VARSIZE_SHORT(tmpStr, len + VARHDRSZ_SHORT);
                    rc =
                        memcpy_s(tmpStr + VARHDRSZ_SHORT, sizeof(tmpStr) - VARHDRSZ_SHORT, DatumGetPointer(cuMin), len);
                    securec_check(rc, "", "");
                    cuMin = PointerGetDatum(tmpStr);
                }

                for (int k = 0; k < contiguous; ++k)
                    vec->AddVar(cuMin, pos++);
            }

            continue;
        }

        // Case 3: It is a normal CU
        CU* cuPtr = lastCU;

        if (unlikely(needLoadCu)) {
            Assert(lastCU == NULL);
            Assert(slot == CACHE_BLOCK_INVALID_IDX);

            // load new cu data, and then reset the flag.
            CSTORESCAN_TRACE_START(GET_CU_DATA_FROM_CACHE);
            cuPtr = this->GetCUData(&cuDesc, colIdx, attlen, slot);
            CSTORESCAN_TRACE_END(GET_CU_DATA_FROM_CACHE);
            lastCU = cuPtr;
            needLoadCu = false;
        }
        Assert(cuPtr);

        if (cuPtr->m_nulls == NULL) {
            Assert(!cuPtr->HasNullValue());
            switch (attlen) {
                case sizeof(char):
                case sizeof(int16):
                case sizeof(int32): {
                    // because the source is of 1/2/4 bytes length, and
                    // the destination is 8 bytes, so assign each item
                    // in for loop.
                    ScalarValue* dest = vec->m_vals + pos;
                    for (int k = 0; k < contiguous; ++k)
                        *dest++ = cuPtr->GetValue<attlen, false>(firstOffset++);
                    pos += contiguous;
                    break;
                }
                case sizeof(Datum): {
                    // because the source and the destination are both of 8 bytes
                    // length, so copy all data in one batch by memcpy().
                    rc = memcpy_s((char*)(vec->m_vals + pos),
                                  (size_t)(uint32)contiguous << 3,
                                  ((uint64*)cuPtr->m_srcData + firstOffset),
                                  (size_t)(uint32)contiguous << 3);
                    securec_check(rc, "\0", "\0");
                    pos += contiguous;
                    break;
                }
                case -1:
                case -2: {
                    // Total bytes to be copied is calculated from the offset table.
                    int32* offset = cuPtr->m_offset + firstOffset;
                    int32 obase = offset[0];
                    if (contiguous > cuDesc.row_count || contiguous >= (int)(cuPtr->m_offsetSize / sizeof(int32))) {
                        ereport(defence_errlevel(), (errcode(ERRCODE_DATA_CORRUPTED),
                            errmsg("Tid and CUDesc, CUId: %u, colId: %d, contiguous: %d, row count: %d.",
                                    curCUId, colIdx, contiguous, cuDesc.row_count),
                            errdetail("please reindex the relation. relation info: name \"%s\", namespace id %u, id %u, relfilenode %u/%u/%u",
                                    RelationGetRelationName(this->m_relation), RelationGetNamespace(this->m_relation), RelationGetRelid(this->m_relation),
                                    this->m_relation->rd_node.spcNode, this->m_relation->rd_node.dbNode, this->m_relation->rd_node.relNode)));
                    }
                    int totalBytes = offset[contiguous] - obase;
                    char* src = cuPtr->m_srcData + obase;
                    // We can copy all elements in one batch together and fix the pointer
                    // by adding the memory base. It is manaully unrolled the loop to give
                    // a strong hint to compiler
                    char* base = vec->AddVars(src, totalBytes) - obase;
                    ScalarValue* dest = vec->m_vals + pos;
                    for (uint32 k = 0; k < ((uint32)contiguous >> 2); ++k) {
                        *dest++ = (ScalarValue)(base + *offset++);
                        *dest++ = (ScalarValue)(base + *offset++);
                        *dest++ = (ScalarValue)(base + *offset++);
                        *dest++ = (ScalarValue)(base + *offset++);
                    }
                    for (int k = 0; k < (contiguous & 0x3); k++)
                        *dest++ = (ScalarValue)(base + *offset++);
                    pos += contiguous;
                    break;
                }
                case 12:
                case 16: {
                    // NB: be careful AddVar() will insert 1B header
                    //   before each value.
                    for (int k = 0; k < contiguous; ++k) {
                        ScalarValue value = cuPtr->GetValue<attlen, false>(firstOffset++);
                        vec->AddVar(PointerGetDatum(value), pos++);
                    }
                    break;
                }
                default:
                    Assert(0);
                    ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH), (errmsg("unsupported datatype branch"))));
                    break;
            }
        } else {
            Assert(cuPtr->HasNullValue());

            // in normal case take care null values.
            for (int k = 0; k < contiguous; ++k) {
                if (unlikely(cuPtr->IsNull(firstOffset)))
                    vec->SetNull(pos);
                else {
                    ScalarValue value = cuPtr->GetValue<attlen, true>(firstOffset);
                    switch (attlen) {
                        case sizeof(char):
                        case sizeof(int16):
                        case sizeof(int32):
                        case sizeof(Datum):
                            vec->m_vals[pos] = value;
                            break;
                        case 12:
                        case 16:
                        case -1:
                        case -2:
                            vec->AddVar(PointerGetDatum(value), pos);
                            break;
                        default:
                            Assert(0);
                            ereport(
                                ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH), (errmsg("unsupported datatype branch"))));
                            break;
                    }
                }

                ++firstOffset;
                ++pos;
            }
        }
    }

    if (lastCU != NULL) {
        // Unpin the last used cu cache.
        Assert(slot != CACHE_BLOCK_INVALID_IDX);
        CUCache->UnPinDataBlock(slot);
    }

    vec->m_rows = pos;
}

void CStore::FillScanBatchLateIfNeed(__inout VectorBatch* vecBatch)
{
    ScalarVector* tidVec = NULL;
    int ctidId = -1, colIdx;

    // Step 1: fill the late read columns except the first late read column
    for (int i = 0; i < m_colNum; ++i) {
        colIdx = m_colId[i];
        if (IsLateRead(i) && colIdx >= 0) {
            Assert(colIdx < vecBatch->m_cols);

            if (tidVec != NULL) {
                CUDesc* cuDescPtr = this->m_CUDescInfo[i]->cuDescArray + this->m_cuDescIdx;
                this->GetCUDeleteMaskIfNeed(cuDescPtr->cu_id, this->m_snapshot);
                (this->*m_fillVectorLateRead[i])(colIdx, tidVec, cuDescPtr, vecBatch->m_arr + colIdx);
            } else {
                // The first late read column should be filled with ctid
                tidVec = vecBatch->m_arr + colIdx;
                ctidId = i;
            }
        }
    }

    // Step 2: fill the first late read column
    if (ctidId >= 0) {
        colIdx = m_colId[ctidId];
        Assert(IsLateRead(ctidId) && colIdx >= 0);

        CUDesc* cuDescPtr = this->m_CUDescInfo[ctidId]->cuDescArray + this->m_cuDescIdx;
        this->GetCUDeleteMaskIfNeed(cuDescPtr->cu_id, this->m_snapshot);
        (this->*m_fillVectorLateRead[ctidId])(colIdx, tidVec, cuDescPtr, vecBatch->m_arr + colIdx);
    }
}

// We fill vector with ctid, because these columns can be read as late as possible.
// After finishing qual, read these columns.
template <bool hasDeadRow>
int CStore::FillTidForLateRead(_in_ CUDesc* cuDescPtr, _out_ ScalarVector* vec)
{
    Assert(cuDescPtr && vec);
    uint32 cur_cuid = cuDescPtr->cu_id;
    int leftSize = cuDescPtr->row_count - m_rowCursorInCU;
    int pos = 0, deadRows = 0;
    Assert(leftSize > 0);

    for (int i = 0; i < leftSize && pos < BatchMaxSize; i++) {
        if (unlikely(hasDeadRow && IsDeadRow(cuDescPtr->cu_id, i + m_rowCursorInCU))) {
            ++deadRows;
        } else {
            // because sizeof(*itemPtr) is not the same to
            // sizeof(vec->m_vals[0]), so zero it at first.
            vec->m_vals[pos] = 0;
            ItemPointer itemPtr = (ItemPointer)&vec->m_vals[pos];

            // Note that itemPtr->offset start from 1
            ItemPointerSet(itemPtr, cur_cuid, i + m_rowCursorInCU + 1);
            ++pos;
        }
    }
    vec->m_rows = pos;
    return deadRows;
}

int CStore::FillSysColVector(_in_ int colIdx, _in_ CUDesc* cuDescPtr, _out_ ScalarVector* vec)
{
    Assert(cuDescPtr && vec);
    uint32 cur_cuid = cuDescPtr->cu_id;
    int leftSize = cuDescPtr->row_count - m_rowCursorInCU;
    int pos = 0, deadRows = 0;
    Assert(leftSize > 0);

    errno_t rc = memset_s(vec->m_flag, sizeof(uint8) * BatchMaxSize, 0, sizeof(uint8) * BatchMaxSize);
    securec_check(rc, "", "");
    GetCUDeleteMaskIfNeed(cuDescPtr->cu_id, m_snapshot);

    for (int i = 0; i < leftSize && pos < BatchMaxSize; i++) {
        if (IsDeadRow(cuDescPtr->cu_id, i + m_rowCursorInCU)) {
            ++deadRows;
            continue;
        }
        switch (colIdx) {
            case SelfItemPointerAttributeNumber: {
                /* description: future plan-set vec->m_desc */
                vec->m_desc.typeId = INT8OID;

                vec->m_vals[pos] = 0;
                ItemPointer itemPtr = (ItemPointer)&vec->m_vals[pos];

                // Note that itemPtr->offset start from 1
                ItemPointerSet(itemPtr, cur_cuid, i + m_rowCursorInCU + 1);
                break;
            }
            case XC_NodeIdAttributeNumber: {
                vec->m_vals[pos] = u_sess->pgxc_cxt.PGXCNodeIdentifier;
                break;
            }
            case TableOidAttributeNumber: {
                vec->m_vals[pos] = RelationGetRelid(m_relation);
                break;
            }
            case MinTransactionIdAttributeNumber: {
                vec->m_vals[pos] = cuDescPtr->xmin;
                break;
            }
            default:
                ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH), (errmsg("Column store don't support"))));
                break;
        }
        ++pos;
    }
    vec->m_rows = pos;

    return deadRows;
}

/*
 * Get CUDesc of column according to cuid.
 */
bool CStore::GetCUDesc(_in_ int col, _in_ uint32 cuid, _out_ CUDesc* cuDescPtr, _in_ Snapshot snapShot)
{
    ScanKeyData key[2];
    HeapTuple tup;
    bool found = false;
    errno_t rc = EOK;
    Assert(col >= 0);

    // we will reset m_perScanMemCnxt when switch to the next batch of cudesc data.
    // so the spaces only used for this batch should be managed by m_perScanMemCnxt.
    AutoContextSwitch newMemCnxt(m_perScanMemCnxt);

    /*
     * Open the CUDesc relation and its index
     */
    Relation cudesc_rel = heap_open(m_relation->rd_rel->relcudescrelid, AccessShareLock);
    TupleDesc cudesc_tupdesc = cudesc_rel->rd_att;
    Relation idx_rel = index_open(cudesc_rel->rd_rel->relcudescidx, AccessShareLock);
    bool isFixedLen = m_relation->rd_att->attrs[col].attlen > 0 ? true : false;
    // Convert logical id is to physical id of attribute
    int attid = m_relation->rd_att->attrs[col].attnum;

    /*
     * Setup scan key to fetch from the index by attid.
     */
    ScanKeyInit(&key[0], (AttrNumber)CUDescColIDAttr, BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(attid));

    ScanKeyInit(&key[1], (AttrNumber)CUDescCUIDAttr, BTEqualStrategyNumber, F_OIDEQ, UInt32GetDatum(cuid));

    snapShot = (snapShot == NULL) ? GetActiveSnapshot() : snapShot;
    Assert(snapShot != NULL);

    SysScanDesc cudesc_scan = systable_beginscan_ordered(cudesc_rel, idx_rel, snapShot, 2, key);
    // only loop once
    while ((tup = systable_getnext_ordered(cudesc_scan, ForwardScanDirection)) != NULL) {
        Datum values[CUDescCUExtraAttr] = {0};
        bool isnull[CUDescCUExtraAttr] = {0};
        char* valPtr = NULL;

        heap_deform_tuple(tup, cudesc_tupdesc, values, isnull);

        uint32 cu_id = DatumGetUInt32(values[CUDescCUIDAttr - 1]);
        Assert(!isnull[CUDescCUIDAttr - 1] && cu_id == cuid && found == false);

        cuDescPtr->xmin = HeapTupleGetRawXmin(tup);

        cuDescPtr->cu_id = cu_id;

        // Put min value into cudesc->min
        if (!isnull[CUDescMinAttr - 1]) {
            char* minPtr = cuDescPtr->cu_min;
            char len_1 = MIN_MAX_LEN;
            valPtr = DatumGetPointer(values[CUDescMinAttr - 1]);
            if (!isFixedLen) {
                *minPtr = (char)VARSIZE_ANY_EXHDR(valPtr);
                minPtr = minPtr + 1;
                len_1 -= 1;
            }
            rc = memcpy_s(minPtr, len_1, VARDATA_ANY(valPtr), VARSIZE_ANY_EXHDR(valPtr));
            securec_check(rc, "", "");
        }
        // Put max value into cudesc->max
        if (!isnull[CUDescMaxAttr - 1]) {
            char* maxPtr = cuDescPtr->cu_max;
            char len_2 = MIN_MAX_LEN;
            valPtr = DatumGetPointer(values[CUDescMaxAttr - 1]);
            if (!isFixedLen) {
                *maxPtr = VARSIZE_ANY_EXHDR(valPtr);
                maxPtr = maxPtr + 1;
                len_2 -= 1;
            }
            rc = memcpy_s(maxPtr, len_2, VARDATA_ANY(valPtr), VARSIZE_ANY_EXHDR(valPtr));
            securec_check(rc, "", "");
        }

        cuDescPtr->row_count = DatumGetInt32(values[CUDescRowCountAttr - 1]);
        Assert(!isnull[CUDescRowCountAttr - 1]);

        // Put CUMode into cudesc->cumode
        cuDescPtr->cu_mode = DatumGetInt32(values[CUDescCUModeAttr - 1]);
        Assert(!isnull[CUDescCUModeAttr - 1]);

        // Put cusize into cudesc->cu_size
        cuDescPtr->cu_size = DatumGetInt32(values[CUDescSizeAttr - 1]);
        Assert(!isnull[CUDescSizeAttr - 1]);

        // Put CUPointer into cudesc->cuPointer
        char* cu_ptr = DatumGetPointer(values[CUDescCUPointerAttr - 1]);
        Assert(!isnull[CUDescCUPointerAttr - 1] && cu_ptr);
        rc = memcpy_s(&cuDescPtr->cu_pointer, sizeof(CUPointer), VARDATA_ANY(cu_ptr), sizeof(CUPointer));
        securec_check(rc, "", "");
        Assert(VARSIZE_ANY_EXHDR(cu_ptr) == sizeof(CUPointer));

        cuDescPtr->magic = DatumGetUInt32(values[CUDescCUMagicAttr - 1]);
        Assert(!isnull[CUDescCUMagicAttr - 1]);
        found = true;
    }
    systable_endscan_ordered(cudesc_scan);
    index_close(idx_rel, AccessShareLock);
    heap_close(cudesc_rel, AccessShareLock);

    return found;
}

void CStore::GetCUDeleteMaskIfNeed(_in_ uint32 cuid, _in_ Snapshot snapShot)
{
    ScanKeyData key[2];
    HeapTuple tup;
    bool isnull = false;
    errno_t rc = EOK;
    bool found = false;

    // delete mask has been loaded
    if (m_delMaskCUId == cuid)
        return;

    // we will reset m_perScanMemCnxt when switch to the next batch of cudesc data.
    // so the spaces only used for this batch should be managed by m_perScanMemCnxt.
    AutoContextSwitch newMemCnxt(m_perScanMemCnxt);

    // Open the CUDesc relation and its index
    Relation cudesc_rel = heap_open(m_relation->rd_rel->relcudescrelid, AccessShareLock);
    TupleDesc cudesc_tupdesc = cudesc_rel->rd_att;
    Relation idx_rel = index_open(cudesc_rel->rd_rel->relcudescidx, AccessShareLock);

    // Setup scan key to fetch from the index by attid.
    ScanKeyInit(&key[0], (AttrNumber)CUDescColIDAttr, BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(VitrualDelColID));

    ScanKeyInit(&key[1], (AttrNumber)CUDescCUIDAttr, BTEqualStrategyNumber, F_OIDEQ, UInt32GetDatum(cuid));

    snapShot = (snapShot == NULL) ? GetActiveSnapshot() : snapShot;
    Assert(snapShot != NULL);

    SysScanDesc cudesc_scan = systable_beginscan_ordered(cudesc_rel, idx_rel, snapShot, 2, key);

    if ((tup = systable_getnext_ordered(cudesc_scan, ForwardScanDirection)) != NULL) {
        // Put CUPointer into cudesc->cuPointer
        Datum v = fastgetattr(tup, CUDescCUPointerAttr, cudesc_tupdesc, &isnull);
        if (isnull)
            m_hasDeadRow = false;
        else {
            m_hasDeadRow = true;
            int8* bitmap = (int8*)PG_DETOAST_DATUM(DatumGetPointer(v));
            rc = memcpy_s(m_cuDelMask, MaxDelBitmapSize, VARDATA_ANY(bitmap), VARSIZE_ANY_EXHDR(bitmap));
            securec_check(rc, "", "");

            // because new memory may be created, so we have to check and free in time.
            if ((Pointer)bitmap != DatumGetPointer(v)) {
                pfree_ext(bitmap);
            }
        }

        found = true;
    }

    systable_endscan_ordered(cudesc_scan);
    index_close(idx_rel, AccessShareLock);
    heap_close(cudesc_rel, AccessShareLock);
    if (!found) {
        TransactionId currGlobalXmin = pg_atomic_read_u64(&t_thrd.xact_cxt.ShmemVariableCache->recentGlobalXmin);
        Assert(snapShot->xmin > 0);
        if (TransactionIdPrecedes(snapShot->xmin, currGlobalXmin))
            ereport(ERROR,
                    (errcode(ERRCODE_SNAPSHOT_INVALID),
                     (errmsg("Snapshot too old."),
                      errdetail("Could not get the old version of CUDeleteBitmap, RecentGlobalXmin: %lu, "
                                "snapShot->xmin: %lu, snapShot->xmax: %lu",
                                currGlobalXmin,
                                snapShot->xmin,
                                snapShot->xmax),
                      errhint("This is a safe error report, will not impact data consistency, retry your query if "
                              "needed."))));
        else {
            if (m_useBtreeIndex)
                m_delMaskCUId = InValidCUID;
            else {
                ereport(PANIC,
                        (errmsg("CU Delete bitmap is missing."),
                         errdetail("There might be some issue about cu %u delete bitmap, Please contact HW engineers "
                                   "for support.",
                                   cuid)));
            }
        }
    } else {
        m_delMaskCUId = cuid;
    }

    return;
}

CU* CStore::GetUnCompressCUData(
    Relation rel, int col, uint32 cuid, _out_ int& slotId, ForkNumber forkNum, bool enterCache) const
{
    return NULL;
}

void CStore::CheckConsistenceOfCUData(CUDesc* cuDescPtr, CU* cu, AttrNumber col) const
{
    /*
     * This memory barrier prevents unordered read, which may cause using NOT-uncompress-completed CU.
     * We must add memory barrier before returning cuPtr in every branch of function GetCUData.
     */
#ifdef __aarch64__
    pg_memory_barrier();
#endif

    /* check the src data ptr. */
    if (cu->m_srcData == NULL) {
        ereport(defence_errlevel(),
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("The m_srcData ptr of CU is NULL in CheckConsistenceOfCUData."),
                 errdetail("relation info: name \"%s\", namespace id %u, id %u, relfilenode %u/%u/%u",
                           RelationGetRelationName(m_relation), RelationGetNamespace(m_relation), RelationGetRelid(m_relation),
                           m_relation->rd_node.spcNode, m_relation->rd_node.dbNode, m_relation->rd_node.relNode),
                 errdetail_internal("CU info: table column %d, id %u, offset %lu, size %d, row count %d",
                                    col,
                                    cuDescPtr->cu_id, cuDescPtr->cu_pointer, cuDescPtr->cu_size, cuDescPtr->row_count)));
    }

    /* check the offset ptr. */
    if ((cu->m_eachValSize < 0 && cu->m_offset == NULL) || (cu->HasNullValue() && cu->m_offset == NULL)) {
        ereport(defence_errlevel(),
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("The m_offset ptr of CU is NULL in CheckConsistenceOfCUData."),
                 errdetail("relation info: name \"%s\", namespace id %u, id %u, relfilenode %u/%u/%u",
                           RelationGetRelationName(m_relation), RelationGetNamespace(m_relation), RelationGetRelid(m_relation),
                           m_relation->rd_node.spcNode, m_relation->rd_node.dbNode, m_relation->rd_node.relNode),
                 errdetail_internal("CU info: table column %d, id %u, offset %lu, size %d, row count %d",
                                    col,
                                    cuDescPtr->cu_id, cuDescPtr->cu_pointer, cuDescPtr->cu_size, cuDescPtr->row_count)));
    }

    /* check the magic number */
    if (cu->m_magic != cuDescPtr->magic) {
        ereport(defence_errlevel(),
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("magic mismatch between cached CU data and CUDesc, CUDesc's magic %u, CU's magic %u",
                        cuDescPtr->magic,
                        cu->m_magic),
                 errdetail("relation info: name \"%s\", namespace id %u, id %u, relfilenode %u/%u/%u",
                           RelationGetRelationName(m_relation), RelationGetNamespace(m_relation), RelationGetRelid(m_relation),
                           m_relation->rd_node.spcNode, m_relation->rd_node.dbNode, m_relation->rd_node.relNode),
                 errdetail_internal("CU info: table column %d, id %u, offset %lu, size %d, row count %d",
                                    col,
                                    cuDescPtr->cu_id, cuDescPtr->cu_pointer, cuDescPtr->cu_size, cuDescPtr->row_count)));
    }

    /* check the row number */
    if (cu->m_offsetSize > 0) {
        /* see also CU::FormValuesOffset() */
        if ((cu->m_offsetSize / (int)sizeof(int32)) != (cuDescPtr->row_count + 1)) {
            ereport(defence_errlevel(),
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("row_count mismatch between cached CU data and CUDesc, CUDesc's row_count %d, CU's "
                            "row_count %d",
                            cuDescPtr->row_count,
                            ((cu->m_offsetSize / (int)sizeof(int32)) - 1)),
                     errdetail("relation info: name \"%s\", namespace id %u, id %u, relfilenode %u/%u/%u",
                               RelationGetRelationName(m_relation),
                               RelationGetNamespace(m_relation),
                               RelationGetRelid(m_relation),
                               m_relation->rd_node.spcNode,
                               m_relation->rd_node.dbNode,
                               m_relation->rd_node.relNode),
                     errdetail_internal("CU info: table column %d, id %u, offset %lu, size %d, magic %u",
                                        col,
                                        cuDescPtr->cu_id,
                                        cuDescPtr->cu_pointer,
                                        cuDescPtr->cu_size,
                                        cuDescPtr->magic)));
        }
    }

    /* check cu size */
    if (cu->m_cuSize != (uint32)cuDescPtr->cu_size) {
        ereport(defence_errlevel(),
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("cu_size mismatch between cached CU data and CUDesc, CUDesc's cu_size %u, CU's cu_size %u",
                        (uint32)cuDescPtr->cu_size,
                        cu->m_cuSize),
                 errdetail("relation info: name \"%s\", namespace id %u, id %u, relfilenode %u/%u/%u",
                           RelationGetRelationName(m_relation),
                           RelationGetNamespace(m_relation),
                           RelationGetRelid(m_relation),
                           m_relation->rd_node.spcNode,
                           m_relation->rd_node.dbNode,
                           m_relation->rd_node.relNode),
                 errdetail_internal("CU info: table column %d, id %u, offset %lu, row count %d, magic %u",
                                    col,
                                    cuDescPtr->cu_id,
                                    cuDescPtr->cu_pointer,
                                    cuDescPtr->row_count,
                                    cuDescPtr->magic)));
    }
}

#define GetUncompressErrMsg(ret_code) ((CU_ERR_CRC == (ret_code)) ? "incorrect checksum" : "incorrect magic")

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
CU* CStore::GetCUData(CUDesc* cuDescPtr, int colIdx, int valSize, int& slotId)
{
    /*
     * we will reset m_PerScanMemCnxt when switch to the next batch of cudesc data.
     * so the spaces only used for this batch should be managed by m_PerScanMemCnxt,
     * including the peices of space used in the decompression.
     */
    if (m_relation->rd_att->attrs[colIdx].attisdropped) {
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
    DataSlotTag dataSlotTag =
        CUCache->InitCUSlotTag((RelFileNodeOld *)&m_relation->rd_node, colIdx, cuDescPtr->cu_id, cuDescPtr->cu_pointer);

    // Record a fetch (read).
    // The fetch count is the sum of the hits and reads.
    if (m_rowCursorInCU == 0) {
        pgstat_count_buffer_read(m_relation);
    }

RETRY_LOAD_CU:

    // Look for the CU in the cache first, this is quick and
    // should succeed most of the time.
    slotId = CUCache->FindDataBlock(&dataSlotTag, (m_rowCursorInCU == 0));

    // If the CU is not in the cache, reserve it.
    // Get a cache slot, reserve memory, and put it in the hashtable.
    // ReserveDataBlock() may block waiting for space or CU Cache slots
    if (IsValidCacheSlotID(slotId)) {
        hasFound = true;
    } else {
        hasFound = false;
        slotId = CUCache->ReserveDataBlock(&dataSlotTag, cuDescPtr->cu_size, hasFound);
    }

    // Use the cached CU
    cuPtr = CUCache->GetCUBuf(slotId);
    cuPtr->m_inCUCache = true;
    cuPtr->SetAttInfo(valSize, attrs[colIdx].atttypmod, attrs[colIdx].atttypid);

    // If the CU was already in the cache, return it.
    if (hasFound) {
        // Wait for a read to complete, if still in progress
        if (CUCache->DataBlockWaitIO(slotId)) {
            CUCache->UnPinDataBlock(slotId);
            ereport(LOG,
                    (errmodule(MOD_CACHE),
                     errmsg("CU wait IO find an error, need to reload! table(%s), column(%s), relfilenode(%u/%u/%u), "
                            "cuid(%u)",
                            RelationGetRelationName(m_relation),
                            NameStr(m_relation->rd_att->attrs[colIdx].attname),
                            m_relation->rd_node.spcNode,
                            m_relation->rd_node.dbNode,
                            m_relation->rd_node.relNode,
                            cuDescPtr->cu_id)));
            goto RETRY_LOAD_CU;
        }

        // when cstore scan first access CU, count mem_hit
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
            retCode = CUCache->StartUncompressCU(cuDescPtr, slotId, this->m_plan_node_id, this->m_timing_on, ALIGNOF_CUSIZE);
            if (retCode == CU_RELOADING) {
                CUCache->UnPinDataBlock(slotId);
                ereport(LOG, (errmodule(MOD_CACHE),
                              errmsg("The CU is being reloaded by remote read thread. Retry to load CU! table(%s), "
                                     "column(%s), relfilenode(%u/%u/%u), cuid(%u)",
                                     RelationGetRelationName(m_relation), NameStr(m_relation->rd_att->attrs[colIdx].attname),
                                     m_relation->rd_node.spcNode, m_relation->rd_node.dbNode, m_relation->rd_node.relNode,
                                     cuDescPtr->cu_id)));
                goto RETRY_LOAD_CU;
            } else if (retCode == CU_ERR_ADIO) {
                ereport(ERROR,
                        (errcode(ERRCODE_IO_ERROR),
                         errmodule(MOD_ADIO),
                         errmsg("Load CU failed in adio! table(%s), column(%s), relfilenode(%u/%u/%u), cuid(%u)",
                                RelationGetRelationName(m_relation),
                                NameStr(m_relation->rd_att->attrs[colIdx].attname),
                                m_relation->rd_node.spcNode,
                                m_relation->rd_node.dbNode,
                                m_relation->rd_node.relNode,
                                cuDescPtr->cu_id)));
            } else if (retCode == CU_ERR_CRC || retCode == CU_ERR_MAGIC) {
                /* Prefech CU contains incorrect checksum */
                addBadBlockStat(
                    &m_cuStorage[colIdx]->m_cnode.m_rnode, ColumnId2ColForkNum(m_cuStorage[colIdx]->m_cnode.m_attid));

                if (RelationNeedsWAL(m_relation) && CanRemoteRead()) {
                    /* clear CacheBlockInProgressIO and CacheBlockInProgressUncompress but not free cu buffer */
                    CUCache->TerminateCU(false);
                    ereport(WARNING,
                            (errcode(ERRCODE_DATA_CORRUPTED),
                             (errmsg("invalid CU in cu_id %u of relation %s file %s offset %lu, prefetch %s, try to "
                                     "remote read",
                                     cuDescPtr->cu_id,
                                     RelationGetRelationName(m_relation),
                                     relcolpath(m_cuStorage[colIdx]),
                                     cuDescPtr->cu_pointer,
                                     GetUncompressErrMsg(retCode))),
                             handle_in_client(true)));

                    /* remote load cu */
                    retCode = GetCUDataFromRemote(cuDescPtr, cuPtr, colIdx, valSize, slotId);
                    if (retCode == CU_RELOADING) {
                        /* other thread in remote read */
                        CUCache->UnPinDataBlock(slotId);
                        ereport(LOG, (errmodule(MOD_CACHE),
                                      errmsg("The CU is being reloaded by remote read thread. Retry to load CU! table(%s), "
                                             "column(%s), relfilenode(%u/%u/%u), cuid(%u)",
                                             RelationGetRelationName(m_relation),
                                             NameStr(m_relation->rd_att->attrs[colIdx].attname), m_relation->rd_node.spcNode,
                                             m_relation->rd_node.dbNode, m_relation->rd_node.relNode, cuDescPtr->cu_id)));
                        goto RETRY_LOAD_CU;
                    }
                } else {
                    // unlogged table can not remote read
                    CUCache->TerminateCU(true);
                    ereport(ERROR,
                            (errcode(ERRCODE_DATA_CORRUPTED),
                             (errmsg("invalid CU in cu_id %u of relation %s file %s offset %lu, prefetch %s",
                                     cuDescPtr->cu_id,
                                     RelationGetRelationName(m_relation),
                                     relcolpath(m_cuStorage[colIdx]),
                                     cuDescPtr->cu_pointer,
                                     GetUncompressErrMsg(retCode)),
                              errdetail("Can not remote read for unlogged/temp table. Should truncate table and "
                                        "re-import data."),
                              handle_in_client(true))));
                }
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

    m_cuStorage[colIdx]->LoadCU(
        cuPtr, cuDescPtr->cu_pointer, cuDescPtr->cu_size, g_instance.attr.attr_storage.enable_adio_function, true);

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
    CUCache->DataBlockCompleteIO(slotId);

    retCode = CUCache->StartUncompressCU(cuDescPtr, slotId, this->m_plan_node_id, this->m_timing_on, ALIGNOF_CUSIZE);
    if (retCode == CU_RELOADING) {
        CUCache->UnPinDataBlock(slotId);
        ereport(LOG,
                (errmodule(MOD_CACHE),
                 errmsg("The CU is being reloaded by remote read thread. Retry to load CU! table(%s), column(%s), "
                        "relfilenode(%u/%u/%u), cuid(%u)",
                        RelationGetRelationName(m_relation),
                        NameStr(m_relation->rd_att->attrs[colIdx].attname),
                        m_relation->rd_node.spcNode,
                        m_relation->rd_node.dbNode,
                        m_relation->rd_node.relNode,
                        cuDescPtr->cu_id)));
        goto RETRY_LOAD_CU;
    } else if (retCode == CU_ERR_CRC || retCode == CU_ERR_MAGIC) {
        /* Sync load CU contains incorrect checksum */
        addBadBlockStat(
            &m_cuStorage[colIdx]->m_cnode.m_rnode, ColumnId2ColForkNum(m_cuStorage[colIdx]->m_cnode.m_attid));

        if (RelationNeedsWAL(m_relation) && CanRemoteRead()) {
            /* clear CacheBlockInProgressIO and CacheBlockInProgressUncompress but not free cu buffer */
            CUCache->TerminateCU(false);
            ereport(WARNING,
                    (errcode(ERRCODE_DATA_CORRUPTED),
                     (errmsg(
                          "invalid CU in cu_id %u of relation %s file %s offset %lu, sync load %s, try to remote read",
                          cuDescPtr->cu_id,
                          RelationGetRelationName(m_relation),
                          relcolpath(m_cuStorage[colIdx]),
                          cuDescPtr->cu_pointer,
                          GetUncompressErrMsg(retCode)),
                      handle_in_client(true))));

            /* remote load cu */
            retCode = GetCUDataFromRemote(cuDescPtr, cuPtr, colIdx, valSize, slotId);
            if (retCode == CU_RELOADING) {
                /* other thread in remote read */
                CUCache->UnPinDataBlock(slotId);
                ereport(LOG,
                        (errmodule(MOD_CACHE),
                         errmsg("The CU is being reloaded by remote read thread. Retry to load CU! table(%s), "
                                "column(%s), relfilenode(%u/%u/%u), cuid(%u)",
                                RelationGetRelationName(m_relation), NameStr(m_relation->rd_att->attrs[colIdx].attname),
                                m_relation->rd_node.spcNode, m_relation->rd_node.dbNode, m_relation->rd_node.relNode,
                                cuDescPtr->cu_id)));
                goto RETRY_LOAD_CU;
            }
        } else {
            // unlogged table can not remote read
            CUCache->TerminateCU(true);
            ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                            (errmsg("invalid CU in cu_id %u of relation %s file %s offset %lu, sync load %s", cuDescPtr->cu_id,
                                    RelationGetRelationName(m_relation), relcolpath(m_cuStorage[colIdx]), cuDescPtr->cu_pointer,
                                    GetUncompressErrMsg(retCode)),
                             errdetail("Can not remote read for unlogged/temp table. Should truncate table and re-import "
                                       "data."))));
        }
    }

    Assert(retCode == CU_OK);

    if (t_thrd.vacuum_cxt.VacuumCostActive) {
        // cu cache misses, so we update vacuum stats
        t_thrd.vacuum_cxt.VacuumCostBalance += u_sess->attr.attr_storage.VacuumCostPageMiss;
    }

    CheckConsistenceOfCUData(cuDescPtr, cuPtr, (AttrNumber)(colIdx + 1));
    return cuPtr;
}

/*
 * @Description:  Only call by CStore::GetCUData(),  for remote load cu
 * @IN/OUT cuDescPtr: cu desc ptr
 * @IN/OUT cuPtr: cu ptr
 * @IN/OUT colIdx: columm idx
 * @IN/OUT slotId: slot id, must be pinned
 * @IN/OUT valSize: value size
 * @Return: CU Uncompressed Return Code
 * @See also: CStore::GetCUData
 */
CUUncompressedRetCode CStore::GetCUDataFromRemote(
    CUDesc* cuDescPtr, CU* cuPtr, int colIdx, int valSize, const int& slotId)
{
    FormData_pg_attribute* attrs = m_relation->rd_att->attrs;
    CUUncompressedRetCode retCode = CU_OK;

    /* reuse memory and check if have some other session is updating it concurrently. */
    if (CUCache->ReserveCstoreDataBlockWithSlotId(slotId)) {
        cuPtr = CUCache->GetCUBuf(slotId);
        cuPtr->m_inCUCache = true;
        cuPtr->SetAttInfo(valSize, attrs[colIdx].atttypmod, attrs[colIdx].atttypid);

        /*
         * remote load need CU compressed. (cuPtr->m_compressedLoadBuf != NULL)
         * if CU uncompressed, means  other thread remote read cu already and uncompress it.
         */
        CUCache->AcquireCompressLock(slotId);

        if (cuPtr->m_cache_compressed) {
            m_cuStorage[colIdx]->RemoteLoadCU(cuPtr,
                                              cuDescPtr->cu_pointer,
                                              cuDescPtr->cu_size,
                                              g_instance.attr.attr_storage.enable_adio_function,
                                              true);

            if (cuPtr->IsVerified(cuDescPtr->magic))
                m_cuStorage[colIdx]->OverwriteCU(
                    cuPtr->m_compressedBuf, cuDescPtr->cu_pointer, cuDescPtr->cu_size, false);
        }

        CUCache->ReleaseCompressLock(slotId);

        CUCache->DataBlockCompleteIO(slotId);
    } else {
        if (CUCache->DataBlockWaitIO(slotId)) {
            ereport(ERROR,
                    (errcode(ERRCODE_IO_ERROR),
                     errmodule(MOD_CACHE),
                     errmsg("There is an IO error when remote read CU in cu_id %u of relation %s file %s offset %lu. "
                            "slotId %d, column \"%s\" ",
                            cuDescPtr->cu_id,
                            RelationGetRelationName(m_relation),
                            relcolpath(m_cuStorage[colIdx]),
                            cuDescPtr->cu_pointer,
                            slotId,
                            NameStr(m_relation->rd_att->attrs[colIdx].attname))));
        }
    }

    retCode = CUCache->StartUncompressCU(cuDescPtr, slotId, this->m_plan_node_id, this->m_timing_on, ALIGNOF_CUSIZE);
    if (retCode == CU_ERR_CRC || retCode == CU_ERR_MAGIC) {
        /* remote load crc error */
        CUCache->TerminateCU(true);
        ereport(ERROR,
                (errcode(ERRCODE_DATA_CORRUPTED),
                 (errmsg("invalid CU in cu_id %u of relation %s file %s offset %lu, remote read %s",
                         cuDescPtr->cu_id,
                         RelationGetRelationName(m_relation),
                         relcolpath(m_cuStorage[colIdx]),
                         cuDescPtr->cu_pointer,
                         GetUncompressErrMsg(retCode)))));
    }

    return retCode;
}

/*
 * @Description:  scan virtual cudesc to calculate row count
 * @Param[IN] col: column id
 * @Param[IN/OUT] loadCUDescInfoPtr: load info ptr for cudesc
 * @Param[IN] snapShot: scan snapShot
 * @Return: true -- need load again; false -- load finish
 * @See also: only called by GetLivedRowNumbers
 */
bool CStore::GetCURowCount(_in_ int col, __inout LoadCUDescCtl* loadCUDescInfoPtr, _in_ Snapshot snapShot)
{
    ScanKeyData key[2];
    HeapTuple tup;
    bool isnull = false;
    bool found = false;

    Assert(col >= 0);
    Assert(loadCUDescInfoPtr);

    // we will reset m_perScanMemCnxt when switch to the next batch of cudesc data.
    // so the spaces only used for this batch should be managed by m_perScanMemCnxt.
    AutoContextSwitch newMemCnxt(m_perScanMemCnxt);

    loadCUDescInfoPtr->lastLoadNum = 0;
    loadCUDescInfoPtr->curLoadNum = 0;

    CUDesc* cuDescArray = loadCUDescInfoPtr->cuDescArray;
    int attid = m_relation->rd_att->attrs[col].attnum;
    Relation cudesc_rel = heap_open(m_relation->rd_rel->relcudescrelid, AccessShareLock);
    TupleDesc cudesc_tupdesc = cudesc_rel->rd_att;
    Relation idx_rel = index_open(cudesc_rel->rd_rel->relcudescidx, AccessShareLock);

    ScanKeyInit(&key[0], (AttrNumber)CUDescColIDAttr, BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(attid));
    ScanKeyInit(&key[1],
                (AttrNumber)CUDescCUIDAttr,
                BTGreaterEqualStrategyNumber,
                F_OIDGE,
                UInt32GetDatum(loadCUDescInfoPtr->nextCUID));
    snapShot = (snapShot == NULL) ? GetActiveSnapshot() : snapShot;

    SysScanDesc cudesc_scan = systable_beginscan_ordered(cudesc_rel, idx_rel, snapShot, 2, key);
    while ((tup = systable_getnext_ordered(cudesc_scan, ForwardScanDirection)) != NULL) {
        uint32 cu_id = DatumGetUInt32(fastgetattr(tup, CUDescCUIDAttr, cudesc_tupdesc, &isnull));
        Assert(!isnull);

        if (IsDicVCU(cu_id))
            continue;

        if (!loadCUDescInfoPtr->HasFreeSlot())
            break;

        cuDescArray[loadCUDescInfoPtr->curLoadNum].cu_id = cu_id;
        loadCUDescInfoPtr->nextCUID = cu_id;

        cuDescArray[loadCUDescInfoPtr->curLoadNum].row_count =
            DatumGetInt32(fastgetattr(tup, CUDescRowCountAttr, cudesc_tupdesc, &isnull));
        Assert(!isnull);

        loadCUDescInfoPtr->curLoadNum++;
        found = true;
    }

    systable_endscan_ordered(cudesc_scan);
    index_close(idx_rel, AccessShareLock);
    heap_close(cudesc_rel, AccessShareLock);

    if (found) {
        // nextCUID must be greater than loaded cudesc
        loadCUDescInfoPtr->nextCUID++;
        return true;
    }
    return false;
}

/*
 * Get the lived row numbers of relation.
 */
int64 CStore::GetLivedRowNumbers(int64* totaldeadrows)
{
    int64 rowNumbers = 0;
    LoadCUDescCtl loadInfo(m_startCUID);

    *totaldeadrows = 0;
    while (GetCURowCount(m_firstColIdx, &loadInfo, m_snapshot)) {
        CUDesc* cuDescArray = loadInfo.cuDescArray;
        for (uint32 i = 0; i < loadInfo.curLoadNum; ++i) {
            GetCUDeleteMaskIfNeed(cuDescArray[i].cu_id, m_snapshot);
            rowNumbers += cuDescArray[i].row_count;
            if (m_hasDeadRow) {
                int nBytes = (cuDescArray[i].row_count + 7) / 8;
                for (int j = 0; j < nBytes; ++j) {
                    *totaldeadrows += NumberOfBit1Set[m_cuDelMask[j]];
                    rowNumbers -= NumberOfBit1Set[m_cuDelMask[j]];
                }
            }
        }
    }
    loadInfo.Destroy();

    return rowNumbers;
}

// It is to judge the row whether dead.
bool CStore::IsDeadRow(uint32 cuid, uint32 row) const
{
    Assert(cuid == m_delMaskCUId);

    /* show any tuples including deleted tuples just for analyse */
    if (u_sess->attr.attr_common.XactReadOnly && u_sess->attr.attr_storage.enable_show_any_tuples)
        return false;
    return (m_hasDeadRow && ((m_cuDelMask[row >> 3] & (1 << (row % 8))) != 0));
}

void CStore::RunScan(_in_ CStoreScanState* state, _out_ VectorBatch* vecBatchOut)
{
    (this->*m_scanFunc)(state, vecBatchOut);
}

// unlink cu files: 16385_c1.0  16385_c1.1 16385_c1.2 ...
static void CStoreUnlinkCuDataFiles(CUStorage* cuStorage)
{
    int fileId = 0;
    char tmpFileName[MAXPGPATH];

    while (1) {
        if (!cuStorage->IsDataFileExist(fileId))
            break;

        cuStorage->GetFileName(tmpFileName, MAXPGPATH, fileId);
        if (unlink(tmpFileName)) {
            ereport(WARNING, (errmsg("could not unlink file \"%s\": %m", tmpFileName)));
        }
        ++fileId;
    }
}

void md_register_forget_request(RelFileNode rnode, ForkNumber forknum, BlockNumber segno);

// unlink bcm files: 16385_c1_bcm  16385_c1_bcm.1 16385_c1_bcm.2 ...
static void CStoreUnlinkCuBcmFiles(CUStorage* cuStorage)
{
    int fileId = 0;
    char tmpFileName[MAXPGPATH];

    while (1) {
        if (!cuStorage->IsBcmFileExist(fileId))
            break;

        cuStorage->GetBcmFileName(tmpFileName, fileId);
        
        md_register_forget_request(cuStorage->m_cnode.m_rnode, ColumnId2ColForkNum(cuStorage->m_cnode.m_attid), fileId);

        if (unlink(tmpFileName)) {
            ereport(WARNING, (errmsg("could not unlink file \"%s\": %m", tmpFileName)));
        }
        ++fileId;
    }
}

// invalid column space cache
void CStore::InvalidRelSpaceCache(RelFileNode* rnode)
{
    CFileNode cFileNode(*rnode, VirtualSpaceCacheColID, MAIN_FORKNUM);
    CStoreAllocator::InvalidColSpaceCache(cFileNode);
}

/* unlink data file for one column of a relation. */
void CStore::UnlinkColDataFile(const RelFileNode& rnode, AttrNumber attrnum, bool bcmIncluded)
{
    Assert(attrnum > 0);
    CFileNode cFileNode(rnode, attrnum, MAIN_FORKNUM);

    CStoreAllocator::InvalidColSpaceCache(cFileNode);
    CUCache->DropRelationCUCache(rnode);

    CUStorage cuStorage(cFileNode);

    if (!t_thrd.xact_cxt.xactDelayDDL) {
        /* unlink data files: C1.0, C1.1 ... */
        CStoreUnlinkCuDataFiles(&cuStorage);
    } else {
        ereport(LOG,
                (errmsg(
                     "delay unlinking column file %u/%u/%u att %d", rnode.spcNode, rnode.dbNode, rnode.relNode, attrnum)));
    }

    if (bcmIncluded) {
        /* unlink bcm file: C1_bcm, C1_bcm.1 ... */
        CStoreUnlinkCuBcmFiles(&cuStorage);
    }

    cuStorage.Destroy();
}

/* DONT call in redo */
void CStore::CreateStorage(Relation rel, Oid newRelFileNode)
{
    TupleDesc desc = RelationGetDescr(rel);
    int nattrs = desc->natts;
    FormData_pg_attribute* attrs = desc->attrs;
    char relpersistence = rel->rd_rel->relpersistence;

    RelFileNode rd_node = rel->rd_node;
    if (OidIsValid(newRelFileNode)) {
        // use the new filenode if *newRelFileNode* is valid.
        rd_node.relNode = newRelFileNode;
    }

    for (int i = 0; i < nattrs; i++) {
        if (attrs[i].attisdropped)
            continue;
        int attrid = attrs[i].attnum;

        CFileNode cnode(rd_node, attrid, MAIN_FORKNUM);

        // create cu file in disk.
        CUStorage* custorage = New(CurrentMemoryContext) CUStorage(cnode);
        Assert(custorage);
        custorage->CreateStorage(0, false);
        DELETE_EX(custorage);

        // log and insert into the pending delete list.
        CStoreRelCreateStorage(&rd_node, attrid, relpersistence, rel->rd_rel->relowner);
    }
}

/*
 * @Description: truncate column data files which relation CREATE and TRUNCATE in same XACT block
 * @IN  rel: column relation
 */
void CStore::TruncateStorageInSameXact(Relation rel)
{
    TupleDesc desc = RelationGetDescr(rel);
    int nattrs = desc->natts;
    FormData_pg_attribute* attrs = desc->attrs;
    RelFileNode rd_node = rel->rd_node;
    uint64 totalSize = 0;

    /*
     * This relfilenode will be truncated, so we should invaild the blocks at
     * the bcm element array.
     */
    BCMArrayDropAllBlocks(rd_node);

    /* make CU data cache invalid before truncate its data file */
    CUCache->DropRelationCUCache(rd_node);

    for (int i = 0; i < nattrs; i++) {
        int attrid = attrs[i].attnum;
        CFileNode cnode(rd_node, attrid, MAIN_FORKNUM);

        // calculate each column size
        totalSize += GetSMgrRelSize(&rd_node, rel->rd_backend, (attrid + FirstColForkNum));

        // invalid column space cache
        CStoreAllocator::InvalidColSpaceCache(cnode);

        CUStorage* custorage = New(CurrentMemoryContext) CUStorage(cnode);

        // data file
        custorage->TruncateDataFile();

        // bcm buffer
        RelFileNodeBackend relfilenodebackend = {rd_node, rel->rd_backend};
        DropRelFileNodeBuffers(relfilenodebackend, ColumnId2ColForkNum(attrid), 0);

        // bcm
        custorage->TruncateBcmFile();

        DELETE_EX(custorage);
    }

    // decrease the permanent space on users' record
    perm_space_decrease(rel->rd_rel->relowner, totalSize, RelationUsesSpaceType(rel->rd_rel->relpersistence));
}

/*
 * @Describe: Get max and min value from cu.
 *
 * @in - cuDescPtr Cu information
 * @in - pos current number of rows.
 * @out - vec ScalarVector struct, storage a column data.
 */
void CStore::FillColMinMax(CUDesc* cuDescPtr, ScalarVector* vec, int pos)
{
    if (!cuDescPtr->IsNullCU()) {
        vec->m_vals[pos] = *(ScalarValue*)cuDescPtr->cu_min;
        vec->m_vals[pos + 1] = *(ScalarValue*)cuDescPtr->cu_max;
    } else {
        vec->SetNull(pos);
        vec->SetNull(pos + 1);
    }

    vec->m_rows = pos + 2;
}

/*
 * @Describe: We only read cudesc for getting min/max value if no dead rows
 * Bypass optimization for special SQL case which are like 'select min(col1), max(col2) from t'
 * @in - state  CStore Scan State.
 * @out - vecBatchOut store data struct.
 */
void CStore::CStoreMinMaxScan(_in_ CStoreScanState* state, _out_ VectorBatch* vecBatchOut)
{
    int pos = 0;

    /*
     * Step 1: The number of holding CUDesc is  max_loaded_cudesc
     * if we load all CUDesc once, the memory will not enough.
     * So we load CUdesc once for max_loaded_cudesc
     */
    CSTORESCAN_TRACE_START(LOAD_CU_DESC);
    LoadCUDescIfNeed();
    CSTORESCAN_TRACE_END(LOAD_CU_DESC);

    CSTORESCAN_TRACE_START(MIN_MAX_CHECK);
    RoughCheckIfNeed(state);
    CSTORESCAN_TRACE_END(MIN_MAX_CHECK);

    /* Step 2: Is end of scan ? */
    if (IsEndScan())
        return;

    /*
     * Step 3: Fill min/max if cudesc has min/max, or fill all data.
     * case1. The cudesc of all columns have min/max and no dead rows
     * case2. There are dead rows
     * case3. Some columns don't have min/max at all now. And some columns have.
     *       for example, numeric column.
     * description: future plan-Save min/max for numeric column when load data
     */
    int idx = m_CUDescIdx[m_cursor];
    int maxVecRows = 0;
    bool needFixRows = false, onlyFillMinMax = true;
    int deadRows = 0;

    CSTORESCAN_TRACE_START(FILL_BATCH);
    for (int i = 0; i < m_colNum; ++i) {
        int colIdx = m_colId[i];

        ScalarVector* vec = vecBatchOut->m_arr + colIdx;
        CUDesc* cuDescPtr = m_CUDescInfo[i]->cuDescArray + idx;
        GetCUDeleteMaskIfNeed(cuDescPtr->cu_id, m_snapshot);

        Assert(0 == pos);
        if (!m_hasDeadRow && !cuDescPtr->IsNoMinMaxCU() && this->m_fillMinMaxFunc[i]) {
            (this->*m_fillMinMaxFunc[i])(cuDescPtr, vec, pos);
        } else {
            int funIdx = m_hasDeadRow ? 1 : 0;
            deadRows = (this->*m_colFillFunArrary[i].colFillFun[funIdx])(i, cuDescPtr, vec);
            if (vec->m_rows > 1)
                onlyFillMinMax = false;
        }

        /* We need fix if exist rows be not inconformity, and mark max rows. */
        if (vec->m_rows != maxVecRows) {
            if (0 != maxVecRows)
                needFixRows = true;

            if (vec->m_rows > maxVecRows)
                maxVecRows = vec->m_rows;
        }
    }
    CSTORESCAN_TRACE_END(FILL_BATCH);

    /*
     * Step 4: Fix the m_rows of VecBatch and padding value for some column if need.
     * Because some column only fill min/max and some column fill batch data for some
     * special corner cases
     */
    if (needFixRows) {
        for (int i = 0; i < m_colNum; ++i) {
            int colIdx = m_colId[i];
            ScalarVector* vec = vecBatchOut->m_arr + colIdx;
            if (vec->m_rows != maxVecRows) {
                for (int k = vec->m_rows; k < maxVecRows; ++k) {
                    /* Fix rows, set this value to null which is not affect min/max result. */
                    vec->SetNull(k);
                }
            }
            vec->m_rows = maxVecRows;
        }
    }

    vecBatchOut->m_rows = maxVecRows;

    /* Step 5: Refresh cursor if need */
    if (onlyFillMinMax) {
        IncLoadCuDescIdx(m_cursor);
        m_rowCursorInCU = 0;
        if (likely(m_scanPosInCU != NULL)) {
            Assert(m_colNum > 0);
            errno_t rc = memset_s(m_scanPosInCU, sizeof(int) * m_colNum, 0, sizeof(int) * m_colNum);
            securec_check(rc, "", "");
        }

    } else {
        RefreshCursor(vecBatchOut->m_rows, deadRows);
    }

    pos = vecBatchOut->m_rows;
    Assert(pos <= BatchMaxSize);
}

/*
 * @Description:  get cu xmin  by cu id
 * @IN cuid: cu id
 * @Return: cu xmin
 */
TransactionId CStore::GetCUXmin(uint32 cuid)
{
    // get first not dropped column
    int colid = CStoreGetfstColIdx(m_relation);

    // cu xmin  use the cudec record xmin which column 0
    CUDesc cudesc;
    bool found = this->GetCUDesc(colid, cuid, &cudesc, m_snapshot);
    if (!found) {
        Assert(false);
        ereport(FATAL,
                (errmsg("compression unit descriptor(talbe \"%s\", column \"%s\", cuid %u) not found",
                        RelationGetRelationName(m_relation),
                        NameStr(m_relation->rd_att->attrs[colid].attname),
                        cuid)));
    }

    return cudesc.xmin;
}

/*
 * @Description:  return the col idx which is filled with ctid.
 * @Return: m_laterReadCtidColIdx
 */
int CStore::GetLateReadCtid() const
{
    return m_laterReadCtidColIdx;
}

void CStore::CheckConsistenceOfCUDesc(int cudescIdx) const
{
    CUDesc* firstCUDesc = m_CUDescInfo[0]->cuDescArray + cudescIdx;
    CUDesc* checkCUDesc = NULL;
    for (int col = 1; col < m_colNum; ++col) {
        checkCUDesc = m_CUDescInfo[col]->cuDescArray + cudescIdx;
        if (checkCUDesc->cu_id == firstCUDesc->cu_id && checkCUDesc->row_count == firstCUDesc->row_count) {
            continue;
        }
        ereport(defence_errlevel(),
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg(
                     "Inconsistent of CUDesc(table column, CUDesc index, CU id, number of rows) during batch loading, "
                     "CUDesc[%d] (%d %d %u %d), CUDesc[%d] (%d %d %u %d)",
                     0,
                     (m_colId[0] + 1),
                     cudescIdx,
                     firstCUDesc->cu_id,
                     firstCUDesc->row_count,
                     col,
                     (m_colId[col] + 1),
                     cudescIdx,
                     checkCUDesc->cu_id,
                     checkCUDesc->row_count),
                 errdetail("relation info: name \"%s\", namespace id %u, id %u, relfilenode %u/%u/%u",
                           RelationGetRelationName(m_relation),
                           RelationGetNamespace(m_relation),
                           RelationGetRelid(m_relation),
                           m_relation->rd_node.spcNode,
                           m_relation->rd_node.dbNode,
                           m_relation->rd_node.relNode)));
    }
}

void CStore::CheckConsistenceOfCUDescCtl(void)
{
    LoadCUDescCtl* firstCUDescCtl = m_CUDescInfo[0];
    LoadCUDescCtl* checkCUdescCtl = NULL;

    for (int i = 1; i < m_colNum; ++i) {
        checkCUdescCtl = m_CUDescInfo[i];
        if (checkCUdescCtl->nextCUID == firstCUDescCtl->nextCUID &&
            checkCUdescCtl->lastLoadNum == firstCUDescCtl->lastLoadNum &&
            checkCUdescCtl->curLoadNum == firstCUDescCtl->curLoadNum) {
            continue;
        }
        ereport(defence_errlevel(),
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("Inconsistent of CUDescCtl(table column, next CU ID, last load number, current load number) "
                        "during batch loading, "
                        "CUDescCtl[%d] is (%d %u %u %u), CUDescCtl[%d] is (%d %u %u %u)",
                        0,
                        (m_colId[0] + 1),
                        firstCUDescCtl->nextCUID,
                        firstCUDescCtl->lastLoadNum,
                        firstCUDescCtl->curLoadNum,
                        i,
                        (m_colId[i] + 1),
                        checkCUdescCtl->nextCUID,
                        checkCUdescCtl->lastLoadNum,
                        checkCUdescCtl->curLoadNum),
                 errdetail("relation info: name \"%s\", namespace id %u, id %u, relfilenode %u/%u/%u",
                           RelationGetRelationName(m_relation),
                           RelationGetNamespace(m_relation),
                           RelationGetRelid(m_relation),
                           m_relation->rd_node.spcNode,
                           m_relation->rd_node.dbNode,
                           m_relation->rd_node.relNode)));
    }
}

void CStore::IncLoadCuDescCursor()
{
    m_cursor++;
    return;
}

/*
 * Init the things needed by delta scan
 *
 * @in node: cstore scan state
 * @in snapshot: the snapshot used to scan delta relation
 * @no return
 */
void InitScanDeltaRelation(CStoreScanState* node, Snapshot snapshot)
{
    Relation deltaRelation;
    TableScanDesc deltaScanDesc;
    Relation cstoreRel = node->ss_currentRelation;

    if (node->ss_currentRelation == NULL)
        return;

    deltaRelation = heap_open(cstoreRel->rd_rel->reldeltarelid, AccessShareLock);
    deltaScanDesc = tableam_scan_begin(deltaRelation, snapshot, 0, NULL);

    node->ss_currentDeltaRelation = deltaRelation;
    node->ss_currentDeltaScanDesc = deltaScanDesc;
    node->ss_deltaScan = false;
    node->ss_deltaScanEnd = false;
    ExecAssignScanType(node, RelationGetDescr(deltaRelation));
}

/*
 * Fill the junk(system) columns for delta scan
 *
 * @in sysIdx: the idx of system column
 * @inout outBatch: the vector batch to fill
 * @in slot: the tuple slot from delta relation
 * @in deltaRelation: the delta relation
 * @no return
 */
static void FillDeltaSysColumn(int sysIdx, VectorBatch* outBatch, TupleTableSlot* slot, Relation deltaRelation)
{
    ScalarVector* destVec = outBatch->GetSysVector(sysIdx);
    ScalarValue* destValue = destVec->m_vals;
    int idx = outBatch->m_rows;
    switch (sysIdx) {
        case SelfItemPointerAttributeNumber: {
            destValue[idx] = 0;
            ItemPointer destTid = (ItemPointer)(destValue + idx);
            *destTid = ((HeapTuple) slot->tts_tuple)->t_self;
            break;
        }
        case XC_NodeIdAttributeNumber: {
            destValue[idx] = u_sess->pgxc_cxt.PGXCNodeIdentifier;
            break;
        }
        case TableOidAttributeNumber: {
            destValue[idx] = RelationGetRelid(deltaRelation);
            break;
        }
        case MinTransactionIdAttributeNumber: {
            destValue[idx] = HeapTupleGetRawXmin((HeapTuple)slot->tts_tuple);
            break;
        }
        default:
            ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH), errmsg("Column store don't support")));
            break;
    }
    destVec->m_rows = idx + 1;
}

/*
 * put one tuple slot into the out vector batch
 *
 * @in node: cstore scan state
 * @inout outBatch: the vector batch to fill
 * @in slot: the tuple slot from delta relation
 * @in tmpContext: the context which is used for per tuple
 * @Return true if outBatch is full, else return false.
 */
static bool FillOneDeltaTuple(
    CStoreScanState* node, VectorBatch* outBatch, TupleTableSlot* slot, MemoryContext tmpContext)
{
    int sysIndex = 0;
    ListCell* lc = NULL;
    List* sysVarList = node->ps.ps_ProjInfo->pi_sysAttrList;

    /* Fill the sys columns */
    foreach (lc, sysVarList) {
        sysIndex = lfirst_int(lc);
        FillDeltaSysColumn(sysIndex, outBatch, slot, node->ss_currentDeltaRelation);
    }

    /* Fill the normal columns */
    return VectorizeOneTuple(outBatch, slot, tmpContext);
}

/*
 * scan the delta relation and fill the vector batch
 *
 * @in node: cstore scan state
 * @inout outBatch: the vector batch to fill
 * @in indexqual: the original qual on index columns used
 * @no return
 */
void ScanDeltaStore(CStoreScanState* node, VectorBatch* outBatch, List* indexqual)
{
    if (node->ss_deltaScanEnd)
        return;

    /* For SMP, only the first thread scan delta table. */
    if (u_sess->stream_cxt.smp_id != 0)
        return;

    bool hasIndexFilter = (list_length(indexqual) > 0);
    HeapTuple tuple = NULL;
    TupleTableSlot* slot = node->ss_ScanTupleSlot;
    TableScanDesc scandesc = (TableScanDesc)(node->ss_currentDeltaScanDesc);
    ExprContext* econtext = node->ps.ps_ExprContext;
    ResetExprContext(econtext);

    while (true) {
        tuple = (HeapTuple) tableam_scan_getnexttuple(scandesc, ForwardScanDirection);

        if (tuple != NULL) {
            (void)ExecStoreTuple(tuple, /* tuple to store */
                                 slot,                   /* slot to store in */
                                 scandesc->rs_cbuf,      /* buffer associated with this tuple */
                                 false);                 /* pfree this pointer */

            /* If there is index qual, use it to filter the delta rows before */
            if (hasIndexFilter) {
                econtext->ecxt_scantuple = slot;
                if (!ExecQual(indexqual, econtext)) {
                    (void)ExecClearTuple(slot);
                    continue;
                }
            }

            /* put the tuple into out batch */
            if (FillOneDeltaTuple(node, outBatch, slot, econtext->ecxt_per_tuple_memory)) {
                (void)ExecClearTuple(slot);
                break;
            }
        } else {
            (void)ExecClearTuple(slot);
            node->ss_deltaScanEnd = true;
            break;
        }
    }

    if (!BatchIsNull(outBatch))
        node->ss_deltaScan = true;
}

/*
 * clean the scan desc and close the delta relation
 *
 * @in node: cstore scan state
 * @no return
 */
void EndScanDeltaRelation(CStoreScanState* node)
{
    if (node->ss_currentDeltaScanDesc) {
        tableam_scan_end((TableScanDesc)(node->ss_currentDeltaScanDesc));
        ExecCloseScanRelation(node->ss_currentDeltaRelation);
    }
}

// CStoreHeapBeginScan	- begin cstore relation scan
// Initialize CStoreScanDesc data structure
CStoreScanDesc CStoreBeginScan(Relation relation, int colNum, int16* colIdx, Snapshot snapshot, bool scanDelta)
{
    Assert(colNum > 0 && colIdx);
    // Step 1: Create CStoreScanState structure
    CStoreScanDesc scanstate;
    scanstate = makeNode(CStoreScanState);
    scanstate->ps.plan = (Plan*)makeNode(CStoreScan);
    scanstate->ps.ps_ProjInfo = makeNode(ProjectionInfo);
    ProjectionInfo* projInfo = scanstate->ps.ps_ProjInfo;

    // Step 2: Construct accessed columns
    List *accessAttrList = NULL;
    List *sysAttrList = NULL;
    for (int i = 0; i < colNum; ++i) {
        /* dropped column and not system column */
        if (colIdx[i] >= 0 && relation->rd_att->attrs[colIdx[i] - 1].attisdropped)
            continue;
        if (colIdx[i] >= 0)
            accessAttrList = lappend_int(accessAttrList, colIdx[i]);
        else
            sysAttrList = lappend_int(sysAttrList, colIdx[i]);
    }
    projInfo->pi_acessedVarNumbers = accessAttrList;
    projInfo->pi_sysAttrList = sysAttrList;
    projInfo->pi_const = false;

    // Step 3: Init CStoreScan
    scanstate->ss_currentRelation = relation;
    scanstate->csss_NumScanKeys = 0;
    scanstate->csss_ScanKeys = NULL;
    /*
     * increment relation ref count while scanning relation
     *
     * This is just to make really sure the relcache entry won't go away while
     * the scan has a pointer to it.  Caller should be holding the rel open
     * anyway, so this is redundant in all normal scenarios...
     */
    RelationIncrementReferenceCount(relation);
    scanstate->m_CStore = New(CurrentMemoryContext) CStore();
    scanstate->m_CStore->InitScan(scanstate, snapshot);

    // Step 4: Initialize scanBatch
    scanstate->m_pScanBatch =
        New(CurrentMemoryContext) VectorBatch(CurrentMemoryContext, scanstate->ss_currentRelation->rd_att);
    if (projInfo->pi_sysAttrList)
        scanstate->m_pScanBatch->CreateSysColContainer(CurrentMemoryContext, projInfo->pi_sysAttrList);

    // Step5: Initialize delta scan
    if (scanDelta) {
        scanstate->ss_ScanTupleSlot = MakeTupleTableSlot();
        ExprContext* econtext = makeNode(ExprContext);
        econtext->ecxt_per_tuple_memory = AllocSetContextCreate(CurrentMemoryContext,
                                                                "cstore delta scan",
                                                                ALLOCSET_DEFAULT_MINSIZE,
                                                                ALLOCSET_DEFAULT_INITSIZE,
                                                                ALLOCSET_DEFAULT_MAXSIZE);
        scanstate->ps.ps_ExprContext = econtext;
        InitScanDeltaRelation(scanstate, (snapshot != NULL ? snapshot : GetActiveSnapshot()));
    } else {
        scanstate->ss_currentDeltaRelation = NULL;
        scanstate->ss_currentDeltaScanDesc = NULL;
        scanstate->ss_deltaScan = false;
        scanstate->ss_deltaScanEnd = true;
    }

    return scanstate;
}

/*
 * GetCStoreNextBatch
 * We can call these function like this: CStoreScanDesc cstoreScanDesc = CStoreBeginScan();
 */
VectorBatch* CStoreGetNextBatch(CStoreScanDesc cstoreScanState)
{
    VectorBatch* vecBatch = cstoreScanState->m_pScanBatch;
    vecBatch->Reset();
    cstoreScanState->m_CStore->RunScan(cstoreScanState, vecBatch);

    /* scan delta table */
    if (cstoreScanState->m_CStore->IsEndScan() && BatchIsNull(vecBatch)) {
        ScanDeltaStore(cstoreScanState, vecBatch, NULL);
        Assert(vecBatch != NULL);
        vecBatch->FixRowCount();
    }

    return vecBatch;
}

/*
 * @Description: Scan an entire "row" (same CUID) of CU and Corresponding CUDescs
 * 			    and bitmaps into BatchCUData.
 *			    Used so far on CStore partition merging.
 *			    Supports ADIO.
 *			    Attention: by using CUStorage->LoadCU we use CStoreMemAlloc::Palloc to
 *					      palloc CU_ptr(s) assigned to BatchCUData->CUptrData. So
 *					      they have to be CStoreMemAlloc::Pfree later when finished.
 * @See also: ATExecCStoreMergePartition
 */
void CStoreScanNextTrunkOfCU(_in_ CStoreScanDesc cstoreScanState, __inout BatchCUData* tmpCUData)
{
    cstoreScanState->m_CStore->CStoreScanWithCU(cstoreScanState, tmpCUData);
}

FORCE_INLINE
bool CStoreIsEndScan(CStoreScanDesc cstoreScanState)
{
    return cstoreScanState->m_CStore->IsEndScan() && cstoreScanState->ss_deltaScanEnd;
}

// Clean up cstoreScanState memory
// NOTICE: VectorBatch must clean by MemoryContext
void CStoreEndScan(CStoreScanDesc cstoreScanState)
{
    ProjectionInfo* projInfo = cstoreScanState->ps.ps_ProjInfo;
    /*
     * decrement relation reference count and free scan descriptor storage
     */
    RelationDecrementReferenceCount(cstoreScanState->ss_currentRelation);

    // Free memory
    Assert(cstoreScanState->m_CStore != NULL);
    Assert(projInfo);
    DELETE_EX(cstoreScanState->m_CStore);
    // NOTICE: will not clean memory which alloc in memeory context
    delete cstoreScanState->m_pScanBatch;
    cstoreScanState->m_pScanBatch = NULL;

    // end scan delta table
    if (cstoreScanState->ss_currentDeltaRelation != NULL) {
        ExecDropSingleTupleTableSlot(cstoreScanState->ss_ScanTupleSlot);
        ExecFreeExprContext(&cstoreScanState->ps);
        EndScanDeltaRelation(cstoreScanState);
    }

    if (projInfo->pi_acessedVarNumbers) {
        list_free(projInfo->pi_acessedVarNumbers);
    }

    if (projInfo->pi_sysAttrList) {
        list_free(projInfo->pi_sysAttrList);
    }

    if (projInfo->pi_PackLateAccessVarNumbers) {
        list_free(projInfo->pi_PackLateAccessVarNumbers);
    }
    pfree_ext(projInfo);
    pfree_ext(cstoreScanState->ps.plan);
}

// CStoreRelGetCUNum
// Get CU numbers of relation by now
uint32 CStoreRelGetCUNumByNow(CStoreScanDesc cstoreScanState)
{
    ScanKeyData key;
    HeapTuple tup;
    bool isnull = false;
    Relation relation = cstoreScanState->m_CStore->m_relation;

    /*
     * Open the CUDesc relation and its index
     */
    Relation cudesc_rel = heap_open(relation->rd_rel->relcudescrelid, AccessShareLock);
    TupleDesc cudesc_tupdesc = cudesc_rel->rd_att;
    Relation idx_rel = index_open(cudesc_rel->rd_rel->relcudescidx, AccessShareLock);

    int attid = relation->rd_att->attrs[0].attnum;

    if (relation->rd_att->attrs[0].attisdropped) {
        int fstColIdx = CStoreGetfstColIdx(relation);
        attid = relation->rd_att->attrs[fstColIdx].attnum;
    }

    /* Setup scan key to fetch from the index by col_id. */
    ScanKeyInit(&key, (AttrNumber)CUDescColIDAttr, BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(attid));

    SysScanDesc cudesc_scan = systable_beginscan_ordered(cudesc_rel, idx_rel, SnapshotNow, 1, &key);

    uint32 max_cuid = FirstCUID;

    /*
     * Optimize for geting last CU description of column.
     * Use BackwardScanDirection scan
     */
    if ((tup = systable_getnext_ordered(cudesc_scan, BackwardScanDirection)) != NULL) {
        max_cuid = DatumGetUInt32(fastgetattr(tup, CUDescCUIDAttr, cudesc_tupdesc, &isnull));
    }
    systable_endscan_ordered(cudesc_scan);
    index_close(idx_rel, AccessShareLock);
    heap_close(cudesc_rel, AccessShareLock);

    return (max_cuid - FirstCUID);
}

/*
 * @Description: Delete the column information from the cudesc table for a CStore table.
 *               It's used for Alter CStore Table Drop Column
 * @Param[IN] rel: the target relation
 * @Param[IN] attrnum: column attrnum to be dropped
 * @Return: void
 * @See also:
 */
void CStoreDropColumnInCuDesc(Relation rel, AttrNumber attrnum)
{
    ScanKeyData key[1];
    SysScanDesc scan;
    HeapTuple tup;
    Oid cudescOid = rel->rd_rel->relcudescrelid;
    Relation cudescHeap = heap_open(cudescOid, RowExclusiveLock);

    int attrno = attrnum;
    ScanKeyInit(&key[0], (AttrNumber)CUDescColIDAttr, BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(attrno));

    scan = systable_beginscan(cudescHeap, rel->rd_rel->relcudescidx, false, NULL, 1, key);

    while (HeapTupleIsValid(tup = systable_getnext(scan))) {
        simple_heap_delete(cudescHeap, &tup->t_self);
    }

    systable_endscan(scan);

    heap_close(cudescHeap, RowExclusiveLock);
}

/*
 * @Description: get the first colum index that is not dropped
 * @Param[IN] rel: the target relation
 * @Return: the first column index that is not dropped
 * @See also:
 */
int CStoreGetfstColIdx(Relation rel)
{
    for (int i = 0; i < rel->rd_att->natts; i++) {
        if (!rel->rd_att->attrs[i].attisdropped)
            return i;
    }
    return 0;
}

/*
 * covert cu pointer to bigint
 *
 * VCU will not covert. and maybe cover to negative value
 */
Datum cupointer_bigint(PG_FUNCTION_ARGS)
{
    /* physical length of a text* */
    Datum text_datum = PG_GETARG_DATUM(0);
    Size text_size = (toast_raw_datum_size(text_datum) - VARHDRSZ);

    CUPointer cu_pointer = 0;

    /* size == sizeof(CUPointer)  AND not toast tuple */
    if (text_size != sizeof(CUPointer) || !VARATT_IS_SHORT(text_datum)) {
        /* not CUPointer */
        PG_RETURN_INT64(0);
    }

    int rc = memcpy_s(&cu_pointer, sizeof(CUPointer), VARDATA_ANY(text_datum), sizeof(CUPointer));
    securec_check(rc, "", "");
    Assert(VARSIZE_ANY_EXHDR(text_datum) == sizeof(CUPointer));

    PG_RETURN_INT64(cu_pointer);
}

CUDescScan::CUDescScan(_in_ Relation relation)
    : m_cudesc(NULL), m_cudescIndex(NULL), m_snapshot(NULL)
{
    m_cudesc = heap_open(relation->rd_rel->relcudescrelid, AccessShareLock);
    m_cudescIndex = index_open(m_cudesc->rd_rel->relcudescidx, AccessShareLock);

    /*
     * m_scanKey[0] = VitrualDelColID, m_scanKey[1] will be set to CUDI when doing scan.
     * We only init m_scanKey[0] = 0 here.
     */
    ScanKeyInit(&m_scanKey[0], (AttrNumber)CUDescColIDAttr, BTEqualStrategyNumber, F_INT4EQ,
        Int32GetDatum(VitrualDelColID));
    ScanKeyInit(&m_scanKey[1], (AttrNumber)CUDescCUIDAttr, BTEqualStrategyNumber, F_OIDEQ, UInt32GetDatum(0));
}

CUDescScan::~CUDescScan()
{
    m_cudesc = NULL;
    m_cudescIndex = NULL;
    m_snapshot = NULL;
}

void CUDescScan::Destroy()
{
    index_close(m_cudescIndex, AccessShareLock);
    heap_close(m_cudesc, AccessShareLock);
}

void CUDescScan::ResetSnapshot(Snapshot snapshot)
{
    m_snapshot = snapshot;
}

bool CUDescScan::CheckItemIsAlive(ItemPointer tid)
{
    uint32 CUId = ItemPointerGetBlockNumber(tid);
    uint32 rownum = ItemPointerGetOffsetNumber(tid) - 1;

    /* We set m_scanKey[0]=VitrualDelColID in constructor func. Here we set m_scanKey[1]=CUID */
    m_scanKey[1].sk_argument = UInt32GetDatum(CUId);

    TupleDesc cudescTupdesc = m_cudesc->rd_att;
    SysScanDesc scanDesc = systable_beginscan_ordered(m_cudesc, m_cudescIndex, m_snapshot, 2, m_scanKey);

    bool isAlive = false;
    HeapTuple tup = NULL;
    while ((tup = systable_getnext_ordered(scanDesc, ForwardScanDirection)) != NULL) {
        /* Put CUPointer into cudesc->cuPointer. */
        bool isnull = false;
        Datum v = fastgetattr(tup, CUDescCUPointerAttr, cudescTupdesc, &isnull);
        if (isnull) {
            /* All rows are alive. */
            isAlive = true;
            break;
        } else {
            int8* bitmap = (int8*) PG_DETOAST_DATUM(DatumGetPointer(v));
            unsigned char* cuDelMask = (unsigned char*) VARDATA_ANY(bitmap);
            isAlive = !IsDeadRow(rownum, cuDelMask);

            /* Because new memory may be created, so we have to check and free in time. */
            if ((Pointer) bitmap != DatumGetPointer(v)) {
                pfree(bitmap);
            }
            /* if CU tuple is alive or is committed */
            if (isAlive || (!TransactionIdIsValid(m_snapshot->xmin) && !TransactionIdIsValid(m_snapshot->xmax))) {
                break;
            }
        }
    }

    systable_endscan_ordered(scanDesc);
    return isAlive;
}

inline bool CUDescScan::IsDeadRow(uint32 row, unsigned char* cuDelMask)
{
    return ((cuDelMask[row >> 3] & (1 << (row % 8))) != 0);
}
