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
 * cstore_am.h
 *        routines to support ColStore
 * 
 * 
 * IDENTIFICATION
 *        src/include/access/cstore_am.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef CSTOREAM_H
#define CSTOREAM_H

#include "access/cstore_roughcheck_func.h"
#include "access/cstore_minmax_func.h"
#include "cstore.h"
#include "storage/cu.h"
#include "storage/custorage.h"
#include "storage/cucache_mgr.h"
#include "utils/snapshot.h"

#define MAX_CU_PREFETCH_REQSIZ (64)

#define MaxDelBitmapSize ((int)DefaultFullCUSize / 8 + 1)

class BatchCUData;

// If we load all CUDesc, the memory will be huge,
// So we define this data structure defining the load CUDesc information
//
struct LoadCUDescCtl : public BaseObject {
    uint32 curLoadNum;
    uint32 lastLoadNum;
    uint32 nextCUID;
    CUDesc* cuDescArray;

    LoadCUDescCtl(uint32 startCUID)
    {
        Reset(startCUID);
        cuDescArray = (CUDesc*)palloc0(sizeof(CUDesc) * u_sess->attr.attr_storage.max_loaded_cudesc);
    }

    virtual ~LoadCUDescCtl()
    {
    }

    virtual void Destroy()
    {
        if (cuDescArray != NULL) {
            pfree(cuDescArray);
            cuDescArray = NULL;
        }
    }

    inline bool HasFreeSlot()
    {
        return curLoadNum < (uint32)u_sess->attr.attr_storage.max_loaded_cudesc;
    }

    inline void Reset(uint32 startCUID)
    {
        curLoadNum = 0;
        lastLoadNum = 0;
        nextCUID = startCUID;
    }
};

struct CStoreScanState;
typedef CStoreScanState *CStoreScanDesc;

struct CStoreIndexScanState;

/*
 * CStore include a set of common API for ColStore.
 * In future, we can add more API.
 */
class CStore : public BaseObject {
    // public static area
public:
    // create data files
    static void CreateStorage(Relation rel, Oid newRelFileNode = InvalidOid);

    // unlink data files
    static void UnlinkColDataFile(const RelFileNode &rnode, AttrNumber attrnum, bool bcmIncluded);
    static void InvalidRelSpaceCache(RelFileNode *rnode);

    // trunccate data files which relation CREATE and TRUNCATE in same XACT block
    static void TruncateStorageInSameXact(Relation rel);

    // form and deform CU Desc tuple
    static HeapTuple FormCudescTuple(_in_ CUDesc *pCudesc, _in_ TupleDesc pCudescTupDesc,
                                     _in_ Datum values[CUDescMaxAttrNum], _in_ bool nulls[CUDescMaxAttrNum],
                                     _in_ Form_pg_attribute pColAttr);

    static void DeformCudescTuple(_in_ HeapTuple pCudescTup, _in_ TupleDesc pCudescTupDesc,
                                  _in_ Form_pg_attribute pColAttr, _out_ CUDesc *pCudesc);

    // Save CU description information into CUDesc table
    static void SaveCUDesc(_in_ Relation rel, _in_ CUDesc *cuDescPtr, _in_ int col, _in_ int options);

    // form and deform VC CU Desc tuple.
    // We add a virtual column for marking deleted rows.
    // The VC is divided into CUs.
    static HeapTuple FormVCCUDescTup(_in_ TupleDesc cudesc, _in_ const char *delMask, _in_ uint32 cuId,
                                     _in_ int32 rowCount, _in_ uint32 magic);

    static void SaveVCCUDesc(_in_ Oid cudescOid, _in_ uint32 cuId, _in_ int rowCount, _in_ uint32 magic,
                             _in_ int options, _in_ const char *delBitmap = NULL);

    static bool IsTheWholeCuDeleted(_in_ char *delBitmap, _in_ int rowsInCu);
    bool IsTheWholeCuDeleted(_in_ int rowsInCu);

    // get Min/Max value from given *pCudesc*.
    // *min* = true, return the Min value.
    // *min* = false, return the Max value.
    static Datum CudescTupGetMinMaxDatum(_in_ CUDesc *pCudesc, _in_ Form_pg_attribute pColAttr, _in_ bool min,
                                         _out_ bool *shouldFree);

    static bool SetCudescModeForMinMaxVal(_in_ bool fullNulls, _in_ bool hasMinMaxFunc, _in_ bool hasNull,
                                          _in_ int maxVarStrLen, _in_ int attlen, __inout CUDesc *cuDescPtr);

    static bool SetCudescModeForTheSameVal(_in_ bool fullNulls, _in_ FuncSetMinMax SetMinMaxFunc, _in_ int attlen,
                                           _in_ Datum attVal, __inout CUDesc *cuDescPtr);

    static uint32 GetMaxCUID(_in_ Oid cudescHeap, _in_ TupleDesc cstoreRelTupDesc, _in_ Snapshot snapshotArg = NULL);
    static uint32 GetMaxIndexCUID(_in_ Relation heapRel, _in_ List *btreeIndex);

    static CUPointer GetMaxCUPointerFromDesc(_in_ int attrno, _in_ Oid cudescHeap);

    static CUPointer GetMaxCUPointer(_in_ int attrno, _in_ Relation rel);

public:
    CStore();
    virtual ~CStore();
    virtual void Destroy();

    // Scan APIs
    void InitScan(CStoreScanState *state, Snapshot snapshot = NULL);
    void InitReScan();
    void InitPartReScan(Relation rel);
    bool IsEndScan() const;

    // late read APIs
    bool IsLateRead(int id) const;
    void ResetLateRead();

    // update cstore scan timing flag
    void SetTiming(CStoreScanState *state);

    // CStore scan : pass vector to VE.
    void ScanByTids(_in_ CStoreIndexScanState *state, _in_ VectorBatch *idxOut, _out_ VectorBatch *vbout);
    void CStoreScanWithCU(_in_ CStoreScanState *state, BatchCUData *tmpCUData, _in_ bool isVerify = false);

    // Load CUDesc information of column according to loadInfoPtr
    // LoadCUDescCtrl include maxCUDescNum for this load, because if we load all
    // it need big memory to hold
    // 
    bool LoadCUDesc(_in_ int col, __inout LoadCUDescCtl *loadInfoPtr, _in_ bool prefetch_control,
                    _in_ Snapshot snapShot = NULL);

    // Get CU description information from CUDesc table
    bool GetCUDesc(_in_ int col, _in_ uint32 cuid, _out_ CUDesc *cuDescPtr, _in_ Snapshot snapShot = NULL);

    // Get tuple deleted information from VC CU description.
    void GetCUDeleteMaskIfNeed(_in_ uint32 cuid, _in_ Snapshot snapShot);

    bool GetCURowCount(_in_ int col, __inout LoadCUDescCtl *loadCUDescInfoPtr, _in_ Snapshot snapShot);
    // Get live row numbers.
    int64 GetLivedRowNumbers(int64 *deadrows);

    // Get CU data.
    // Note that the CU is pinned
    CU *GetCUData(_in_ CUDesc *cuDescPtr, _in_ int colIdx, _in_ int valSize, _out_ int &slotId);

    CU *GetUnCompressCUData(Relation rel, int col, uint32 cuid, _out_ int &slotId, ForkNumber forkNum = MAIN_FORKNUM,
                            bool enterCache = true) const;

    // Fill Vector APIs
    int FillVecBatch(_out_ VectorBatch *vecBatchOut);

    // Fill Vector of column
    template <bool hasDeadRow, int attlen>
    int FillVector(_in_ int colIdx, _in_ CUDesc *cu_desc_ptr, _out_ ScalarVector *vec);

    template <int attlen>
    void FillVectorByTids(_in_ int colIdx, _in_ ScalarVector *tids, _out_ ScalarVector *vec);

    template <int attlen>
    void FillVectorLateRead(_in_ int seq, _in_ ScalarVector *tids, _in_ CUDesc *cuDescPtr, _out_ ScalarVector *vec);

    void FillVectorByIndex(_in_ int colIdx, _in_ ScalarVector *tids, _in_ ScalarVector *srcVec,
                           _out_ ScalarVector *destVec);

    // Fill system column into ScalarVector
    int FillSysColVector(_in_ int colIdx, _in_ CUDesc *cu_desc_ptr, _out_ ScalarVector *vec);

    template <int sysColOid>
    void FillSysVecByTid(_in_ ScalarVector *tids, _out_ ScalarVector *destVec);

    template <bool hasDeadRow>
    int FillTidForLateRead(_in_ CUDesc *cuDescPtr, _out_ ScalarVector *vec);

    void FillScanBatchLateIfNeed(__inout VectorBatch *vecBatch);

    /* Set CU range for scan in redistribute. */
    void SetScanRange();

    // Judge whether dead row
    bool IsDeadRow(uint32 cuid, uint32 row) const;

    void CUListPrefetch();
    void CUPrefetch(CUDesc *cudesc, int col, AioDispatchCUDesc_t **dList, int &count, File *vfdList);

    /* Point to scan function */
    typedef void (CStore::*ScanFuncPtr)(_in_ CStoreScanState *state, _out_ VectorBatch *vecBatchOut);
    void RunScan(_in_ CStoreScanState *state, _out_ VectorBatch *vecBatchOut);

    int GetLateReadCtid() const;
    void IncLoadCuDescCursor();

public:  // public vars
    // Inserted/Scan Relation
    Relation m_relation;

private:  // private methods.
    // CStore scan : pass vector to VE.
    void CStoreScan(CStoreScanState *state, VectorBatch *vecBatchOut);
    void CStoreMinMaxScan(CStoreScanState *state, VectorBatch *vecBatchOut);

    // The number of holding CUDesc is  max_loaded_cudesc
    // if we load all CUDesc once, the memory will not enough.
    // So we load CUdesc once for max_loaded_cudesc
    void LoadCUDescIfNeed();

    // Do RoughCheck if need
    // elimiate CU by min/max value of CU.
    void RoughCheckIfNeed(_in_ CStoreScanState *state);

    // Refresh cursor
    void RefreshCursor(int row, int deadRows);

    void InitRoughCheckEnv(CStoreScanState *state);

    void BindingFp(CStoreScanState *state);
    void InitFillVecEnv(CStoreScanState *state);

    // indicate whether only accessing system column or const column.
    // true, means that m_virtualCUDescInfo is a new and single object.
    // false, means that m_virtualCUDescInfo just a pointer to m_CUDescInfo[0].
    // 
    inline bool OnlySysOrConstCol(void)
    {
        return ((m_colNum == 0 && m_sysColNum != 0) || m_onlyConstCol);
    }

    int LoadCudescMinus(int start, int end) const;
    bool HasEnoughCuDescSlot(int start, int end) const;
    bool NeedLoadCUDesc(int32 &cudesc_idx);
    void IncLoadCuDescIdx(int &idx) const;
    bool RoughCheck(CStoreScanKey scanKey, int nkeys, int cuDescIdx);

    void FillColMinMax(CUDesc *cuDescPtr, ScalarVector *vec, int pos);

    inline TransactionId GetCUXmin(uint32 cuid);

    // only called by GetCUData()
    CUUncompressedRetCode GetCUDataFromRemote(CUDesc *cuDescPtr, CU *cuPtr, int colIdx, int valSize, const int &slotId);

    /* defence functions */
    void CheckConsistenceOfCUDescCtl(void);
    void CheckConsistenceOfCUDesc(int cudescIdx) const;
    void CheckConsistenceOfCUData(CUDesc *cuDescPtr, CU *cu, AttrNumber col) const;

private:
    // control private memory used locally.
    // m_scanMemContext: for objects alive during the whole cstore-scan
    // m_perScanMemCnxt: for memory per heap table scan and temp space
    //    during decompression.
    MemoryContext m_scanMemContext;
    MemoryContext m_perScanMemCnxt;

    // current snapshot to use.
    Snapshot m_snapshot;

    // 1. Accessed user column id
    // 2. Accessed system column id
    // 3. flags for late read
    // 4. each CU storage fro each user column.
    int *m_colId;
    int *m_sysColId;
    bool *m_lateRead;
    CUStorage **m_cuStorage;

    // 1. The CUDesc info of accessed columns
    // 2. virtual CUDesc for sys or const columns
    LoadCUDescCtl **m_CUDescInfo;
    LoadCUDescCtl *m_virtualCUDescInfo;

    // Accessed CUDesc index array
    // After RoughCheck, which CU will be accessed
    // 
    int *m_CUDescIdx;

    // adio param
    int m_lastNumCUDescIdx;
    int m_prefetch_quantity;
    int m_prefetch_threshold;
    bool m_load_finish;

    // Current scan position inside CU
    // 
    int *m_scanPosInCU;

    // Rough Check Functions
    // 
    RoughCheckFunc *m_RCFuncs;

    typedef int (CStore::*m_colFillFun)(int seq, CUDesc *cuDescPtr, ScalarVector *vec);

    typedef struct {
        m_colFillFun colFillFun[2];
    } colFillArray;

    typedef void (CStore::*FillVectorByTidsFun)(_in_ int colIdx, _in_ ScalarVector *tids, _out_ ScalarVector *vec);

    typedef void (CStore::*FillVectorLateReadFun)(_in_ int seq, _in_ ScalarVector *tids, _in_ CUDesc *cuDescPtr,
                                                  _out_ ScalarVector *vec);

    FillVectorByTidsFun *m_fillVectorByTids;
    FillVectorLateReadFun *m_fillVectorLateRead;
    colFillArray *m_colFillFunArrary;

    typedef void (CStore::*fillMinMaxFuncPtr)(CUDesc *cuDescPtr, ScalarVector *vec, int pos);
    fillMinMaxFuncPtr *m_fillMinMaxFunc;

    ScanFuncPtr m_scanFunc;  // cstore scan function ptr

    // node id of this plan
    int m_plan_node_id;

    // 1. Number of accessed user columns
    // 2. Number of accessed system columns.
    int m_colNum;
    int m_sysColNum;

    // 1. length of loaded CUDesc info or virtual CUDesc info
    // 2. length of m_CUDescIdx
    int m_NumLoadCUDesc;
    int m_NumCUDescIdx;

    // 1. CU id of current deleted mask.
    // 2. Current access cursor in m_CUDescIdx
    // 3. Current access row cursor inside CU
    uint32 m_delMaskCUId;
    int m_cursor;
    int m_rowCursorInCU;

    uint32 m_startCUID; /* scan start CU ID. */
    uint32 m_endCUID;   /* scan end CU ID. */

    unsigned char m_cuDelMask[MaxDelBitmapSize];

    // whether dead rows exist
    bool m_hasDeadRow;
    // Is need do rough check
    bool m_needRCheck;
    // Only access const column
    bool m_onlyConstCol;

    bool m_timing_on; /* timing CStoreScan steps */

    RangeScanInRedis m_rangeScanInRedis; /* if it is a range scan at redistribution time */

    // cbtree index flag
    bool m_useBtreeIndex;

    // the first column index, start from 0
    int m_firstColIdx;

    // for late read
    // the cuDesc id of batch in the cuDescArray.
    int m_cuDescIdx;

    // for late read
    // the first late read column idx which is filled with ctid.
    int m_laterReadCtidColIdx;
};

// CStore Scan interface for sequential scan
// 
extern void InitScanDeltaRelation(CStoreScanState *node, Snapshot snapshot);
extern void ScanDeltaStore(CStoreScanState *node, VectorBatch *outBatch, List *indexqual);
extern void EndScanDeltaRelation(CStoreScanState *node);
extern CStoreScanDesc CStoreBeginScan(Relation relation, int colNum, int16 *colIdx, Snapshot snapshot, bool scanDelta);
extern VectorBatch *CStoreGetNextBatch(CStoreScanDesc cstoreScanState);
extern bool CStoreIsEndScan(CStoreScanDesc cstoreScanState);
extern void CStoreEndScan(CStoreScanDesc cstoreScanState);

extern void CStoreScanNextTrunkOfCU(_in_ CStoreScanDesc cstoreScanState, __inout BatchCUData *tmpCUData);

// CStoreRelGetCUNum
// Get CU numbers of relation by now
// 
extern uint32 CStoreRelGetCUNumByNow(CStoreScanDesc cstoreScanState);
// Delete the info of the dropped column from the cudesc for a cstore table
extern void CStoreDropColumnInCuDesc(Relation rel, AttrNumber attrnum);
/* CStoreGet1stUndroppedColIdx get the first colum index that is not dropped */
extern int CStoreGetfstColIdx(Relation rel);

void CStoreAbortCU();
void VerifyAbortCU();

extern void CheckUniqueOnOtherIdx(Relation index, Relation heapRel, Datum* values, const bool* isnull);

/*
 * CUDescScan is used to determine the visibility of CU rows.
 */
class CUDescScan : public BaseObject {
public:
    CUDescScan(_in_ Relation relation);

    virtual ~CUDescScan();
    virtual void Destroy();

    void ResetSnapshot(Snapshot);

    bool CheckItemIsAlive(ItemPointer tid);

private:
    inline bool IsDeadRow(uint32 row, unsigned char* cuDelMask);

    Relation m_cudesc;
    Relation m_cudescIndex;
    ScanKeyData m_scanKey[2];
    Snapshot m_snapshot;
};


#endif
