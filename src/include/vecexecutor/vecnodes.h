/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2021, openGauss Contributors
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
 * vecnodes.h
 *     Definition for vector executor state nodes.
 * 
 * IDENTIFICATION
 *        src/include/vecexecutor/vecnodes.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef VECNODES_H_
#define VECNODES_H_

#include "access/dfs/dfs_am.h"
#include "executor/exec/execStream.h"
#include "nodes/execnodes.h"
#include "access/cstore_am.h"
#include "pgxc/execRemote.h"
#include "access/cstoreskey.h"
#ifdef ENABLE_MULTIPLE_NODES
#include "tsdb/common/data_row.h"
#endif

typedef ScalarVector* (*vecqual_func)(ExprContext* econtext);

/* runtime bloomfilter */
typedef struct BloomFilterRuntime {
    List* bf_var_list;              /* bloomfilter var list. */
    List* bf_filter_index;          /* bloomfilter array index. */
    filter::BloomFilter** bf_array; /* bloomfilter array. */
} BloomFilterRuntime;

typedef struct VecHashJoinState : public HashJoinState {
    int joinState;

    void* hashTbl;

    FmgrInfo* eqfunctions;

    /*
     * function pointer to LLVM machine code if hash join qual
     * can be LLVM optimiezed.
     */
    vecqual_func jitted_joinqual;   /* LLVM IR function pointer to point to
                                     * codegened hash->joinqual expr */
    vecqual_func jitted_hashclause; /* LLVM IR function pointer to point to
                                     * codegened hash clause expr */

    char* jitted_innerjoin;        /* jitted inner hash join */
    char* jitted_matchkey;         /* jitted matchKey for hash join*/
    char* jitted_buildHashTable;   /* jitted buildHashTable*/
    char* jitted_probeHashTable;   /* jitted probeHashTable*/
    int enable_fast_keyMatch;      /* fast path for key match
                                    * 0 : normal keyMatch
                                    * 1 : one hash clause
                                    * 2 : fast path keyMatch
                                    */
    BloomFilterRuntime bf_runtime; /* runtime bloomfilter */
    char* jitted_hashjoin_bfaddLong;
    char* jitted_hashjoin_bfincLong;

    char* jitted_buildHashTable_NeedCopy;
} VecHashJoinState;

typedef enum VecAggType {
    AGG_COUNT = 0,
    AGG_SUM,
    AGG_AVG,
    AGG_MIN,
    AGG_MAX,
} VecAggType;

typedef struct VecAggInfo {
    FunctionCallInfoData vec_agg_function;
    FunctionCallInfoData vec_final_function;
    VectorFunction* vec_agg_cache;
    VectorFunction* vec_sonic_agg_cache;
    PGFunction* vec_agg_final;
} VecAggInfo;

typedef struct VecAggState : public AggState {
    void* aggRun;

    VecAggInfo* aggInfo;
    char* jitted_hashing;         /* LLVM function pointer to point to
                                   * the codegened hashing function */
    char* jitted_sglhashing;      /* LLVM function pointer to point to
                                   * the codegened hashing function for
                                   * only single hash table case */
    char* jitted_batchagg;        /* LLVM function pointer to point to
                                   * the codegened BatchAggregation
                                   * function */
    char* jitted_sonicbatchagg;   /* LLVM function pointer to point to
                                   * the codegened BatchAggregation
                                   * with sonic format */
    char* jitted_SortAggMatchKey; /* LLVM jitted function pointer */

} VecAggState;

typedef struct VecWindowAggState : public WindowAggState {
    void* VecWinAggRuntime;
    VecAggInfo* windowAggInfo;
} VecWindowAggState;

struct VecGroupState : public GroupState {
    void** container;
    void* cap;
    uint16 idx;
    int cellSize;
    bool keySimple;
    FmgrInfo* buildFunc;
    FmgrInfo* buildScanFunc;
    VectorBatch* scanBatch;
    VarBuf* currentBuf;
    VarBuf* bckBuf;
    vecqual_func jitted_vecqual; /* LLVM IR function pointer to point to
                                  * codegened grpstate->ss.ps.qual expr */
};

struct VecUniqueState : public UniqueState {
    bool uniqueDone;
    void** container;
    void* cap;
    uint16 idx;
    int cellSize;
    bool keySimple;
    FmgrInfo* buildFunc;
    FmgrInfo* buildScanFunc;
    VectorBatch* scanBatch;
    VarBuf* currentBuf;
    VarBuf* bckBuf;
};

typedef struct VecSetOpState : public SetOpState {
    void* vecSetOpInfo;
} VecSetOpState;

typedef struct VecSortState : public SortState {
    VectorBatch* m_pCurrentBatch;
    char* jitted_CompareMultiColumn;      /* jitted function for CompareMultiColumn  */
    char* jitted_CompareMultiColumn_TOPN; /* jitted function for CompareMultiColumn used by Top N sort  */
} VecSortState;

typedef struct VecRemoteQueryState : public RemoteQueryState {
    VectorBatch* resultBatch;

} VecRemoteQueryState;

typedef struct VecToRowState VecToRowState;
typedef void (*DevectorizeFun)(VecToRowState* state, ScalarVector* pColumn, int rows, int cols, int i);
typedef struct VecToRowState {
    PlanState ps;

    VectorBatch* m_pCurrentBatch;  // current active batch in outputing
    int m_currentRow;              // position to current row in outpting
    int nattrs;                    // number of attributes
    TupleTableSlot* tts;           // template of the current returning tts
    Datum* m_ttsvalues;            // column values of the active batch
    bool* m_ttsisnull;             // indicator if one column value is null

    int part_id;
    List* subpartitions;
    List* subPartLengthList;
    DevectorizeFun* devectorizeFunRuntime;

} VecToRowState;

typedef struct RowToVecState {
    PlanState ps;

    bool m_fNoMoreRows;            // does it has more rows to output
    VectorBatch* m_pCurrentBatch;  // current active batch in outputing
    bool m_batchMode;
} RowToVecState;

typedef struct VecResultState : public ResultState {
} VecResultState;

struct VecAppendState : public AppendState {
    // Nothing special for vectorized append
};

struct VecStreamState;

typedef void (*assembleBatchFun)(VecStreamState* vsstate, VectorBatch* batch);

typedef struct VecStreamState : public StreamState {
    VectorBatch* m_CurrentBatch;
    assembleBatchFun batchForm;
    bool redistribute;
    int bitNumericLen;
    int bitNullLen;
    uint32* m_colsType;

} VecStreamState;

typedef struct CStoreScanRunTimeKeyInfo {
    CStoreScanKey scan_key;
    ExprState* key_expr;
} CStoreScanRunTimeKeyInfo;

typedef struct CStoreScanState : ScanState {
    Relation ss_currentDeltaRelation;
    Relation ss_partition_parent;
    TableScanDesc ss_currentDeltaScanDesc;
    bool ss_deltaScan;
    bool ss_deltaScanEnd;

    VectorBatch* m_pScanBatch;     // batch to work on
    VectorBatch* m_pCurrentBatch;  // output batch
    CStoreScanRunTimeKeyInfo* m_pScanRunTimeKeys;
    int m_ScanRunTimeKeysNum;
    bool m_ScanRunTimeKeysReady;

    CStore* m_CStore;
    /*Optimizer Information*/
    CStoreScanKey csss_ScanKeys;  // support pushing predicate down to cstore scan.
    int csss_NumScanKeys;

    // Optimization for access pattern
    //
    bool m_fSimpleMap;     // if it is simple without need to invoke projection code
    bool m_fUseColumnRef;  // Use column reference without copy to return data

    vecqual_func jitted_vecqual;

    bool m_isReplicaTable; /* If it is a replication table? */
} CStoreScanState;

typedef struct DfsScanState : ScanState {
    VectorBatch* m_pScanBatch;     // batch to work on
    VectorBatch* m_pCurrentBatch;  // output batch
    MemoryContext m_scanCxt;
    bool m_done;
    dfs::reader::ReaderState* m_readerState;
    dfs::reader::Reader* m_fileReader;
    List* m_splitList;
} DfsScanState;

class TimeRange;
class TagFilters;
class TsStoreSearch;

typedef struct TsStoreScanState : ScanState {
    /* Determined by upper layer */
    int top_key_func_arg;
    bool is_simple_scan;
    bool has_sort;
    int limit;                       // If is limit n
    AttrNumber sort_by_time_colidx;  // If is sort by tstime limit n

#ifdef ENABLE_MULTIPLE_NODES
    TagRows* tag_rows;
#endif
    bool only_scan_tag;             // series function
    /* regular member */
    VectorBatch* scanBatch;          // batch to work on
    VectorBatch* currentBatch;       // output batch
    Relation ss_partition_parent;
    vecqual_func jitted_vecqual;
    /* ts special */
    TsStoreSearch* ts_store_search;
    int tag_id_num;
    bool only_const_col;
    bool early_stop;
    bool tags_scan_done;            // cu data has not been finished
    TimeRange* time_range;          // time range
    int scaned_tuples;
    bool first_scan;

} TsStoreScanState;

class Batchsortstate;

typedef struct IndexSortState {
    VectorBatch* m_tids;
    int64 m_sortCount;
    Batchsortstate* m_sortState;
    TupleDesc m_sortTupleDesc;
    bool m_tidEnd;
} IndexSortState;

typedef struct CBTreeScanState : IndexScanState {
    IndexSortState* m_sort;
    List* m_indexScanTList;
} CBTreeScanState;

typedef struct CBTreeOnlyScanState : IndexOnlyScanState {
    IndexSortState* m_sort;
    List* m_indexScanTList;
} CBTreeOnlyScanState;

struct CStoreIndexScanState;
struct DfsIndexScanState;
typedef VectorBatch* (*cstoreIndexScanFunc)(CStoreIndexScanState* state);
typedef void (*dfsIndexScanFunc)(DfsIndexScanState* state);

typedef struct CstoreBitmapIndexScanState : BitmapIndexScanState {
    IndexSortState* m_sort;
    List* m_indexScanTList;
} CstoreBitmapIndexScanState;

typedef struct DfsIndexScanState : DfsScanState {
    CStoreScanState* m_indexScan;              /* index scan state */
    CBTreeScanState* m_btreeIndexScan;         /* cbtree index scan state */
    CBTreeOnlyScanState* m_btreeIndexOnlyScan; /* cbtree index only scan state */
    IndexSortState* m_sort;
    List* m_dfsPreparedCols; /* the list of prepared columns(index) */
    bool m_indexonly;        /* index only scan flag */
    dfsIndexScanFunc m_dfsIndexScanFunc;
} DfsIndexScanState;

/* ----------------
 *	 CstoreIndexScanState information
 *      A column index scan employees two scan: one for index scan to retrieve
 *      ctids and the other is the base table scan based on ctids.
 */
typedef struct CStoreIndexScanState : CStoreScanState {
    CStoreScanState* m_indexScan;
    CBTreeScanState* m_btreeIndexScan;
    CBTreeOnlyScanState* m_btreeIndexOnlyScan;
    List* m_deltaQual;
    bool index_only_scan;

    // scan index and then output tid, these columns in base table
    //
    int* m_indexOutBaseTabAttr;
    int* m_idxInTargetList;
    int m_indexOutAttrNo;
    cstoreIndexScanFunc m_cstoreIndexScanFunc;
} CStoreIndexScanState;

typedef struct CStoreIndexCtidScanState : CStoreIndexScanState {
    CstoreBitmapIndexScanState* m_cstoreBitmapIndexScan;
} CStoreIndexCtidScanState;

typedef struct CStoreIndexHeapScanState : CStoreIndexScanState {
} CStoreIndexHeapScanState;

typedef struct CStoreIndexAndState : public BitmapAndState {
    StringInfo resultTids;
    uint64 fetchCount;
    VectorBatch* m_resultBatch;
} CStoreIndexAndState;

typedef struct CStoreIndexOrState : public BitmapOrState {
    StringInfo resultTids;
    uint64 fetchCount;
    VectorBatch* m_resultBatch;
} CStoreIndexOrState;

typedef void (*AddVarFunc)(ScalarVector* vec, Datum v, int row);

typedef struct VecForeignScanState : public ForeignScanState {
    VectorBatch* m_pScanBatch;     // batch to work on
    VectorBatch* m_pCurrentBatch;  // output batch
    MemoryContext m_scanCxt;
    bool m_done;
    Datum* m_values;
    bool* m_nulls;
} VecForeignScanState;

typedef struct VecModifyTableState : public ModifyTableState {
    VectorBatch* m_pScanBatch;     // batch to work on
    VectorBatch* m_pCurrentBatch;  // output batch
} VecModifyTableState;

typedef struct VecSubqueryScanState : public SubqueryScanState {
} VecSubqueryScanState;

struct VecNestLoopState : public NestLoopState {
    void* vecNestLoopRuntime;
    vecqual_func jitted_vecqual;
    vecqual_func jitted_joinqual;
};

typedef struct VecPartIteratorState : public PartIteratorState {
} VecPartIteratorState;

class BatchStore;

typedef struct VecMaterialState : public MaterialState {
    VectorBatch* m_pCurrentBatch;
    BatchStore* batchstorestate;
    bool from_memory;
} VecMaterialState;

#endif /* VECNODES_H_ */
