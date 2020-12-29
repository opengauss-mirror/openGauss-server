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
 * cstore_rewrite.h
 *        routinues to support Column Store Rewrite
 *
 *
 * IDENTIFICATION
 *        src/include/access/cstore_rewrite.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef CSTORE_REWRITE_H
#define CSTORE_REWRITE_H

#include "c.h"
#include "postgres.h"
#include "knl/knl_variable.h"
#include "access/cstore_am.h"
#include "access/cstore_minmax_func.h"
#include "access/reloptions.h"
#include "access/transam.h"
#include "access/tupdesc.h"
#include "access/xact.h"
#include "access/xlogutils.h"
#include "catalog/storage.h"
#include "commands/cluster.h"
#include "commands/vacuum.h"
#include "executor/executor.h"
#include "nodes/execnodes.h"
#include "storage/cstore/cstore_compress.h"
#include "storage/custorage.h"
#include "utils/fmgroids.h"
#include "utils/memutils.h"
#include "access/heapam.h"

#define MAX_CSTORE_MERGE_PARTITIONS 31

typedef enum CStoreRewriteType {
    CSRT_ADD_COL = 0,    // --> ADD COLUMN
    CSRT_SET_DATA_TYPE,  // --> ALTER COLUMN SET DATA TYPE
    CSRT_NUM             // add new type above please.
} CStoreRewriteType;

typedef struct ColumnNewValue {
    Expr *expr;            // expression to compute
    ExprState *exprstate;  // execution state
} ColumnNewValue;

typedef struct CStoreRewriteColumn {
    AttrNumber attrno;
    bool isDropped;
    bool isAdded;
    bool notNull;
    ColumnNewValue *newValue;

    static CStoreRewriteColumn *CreateForAddColumn(AttrNumber attNo);
    static CStoreRewriteColumn *CreateForSetDataType(AttrNumber attNo);
    static void Destroy(CStoreRewriteColumn **rewriteInfo);
} CStoreRewriteColumn;

// Bulk Insert For Heap Relation
// 
// Notice: we just bulk-insert heap tuples for this heap relation, without
//   handling its indexes and triggers, etc.
// 
class HeapBulkInsert : public BaseObject {
public:
    HeapBulkInsert(Relation cudescHeapRel);
    virtual ~HeapBulkInsert(){};
    virtual void Destroy();

    // interfaces for bulk-insert heap tuples.
    // BulkInsertCopy() will copy this tuple before appending,
    // but BulkInsert() will not. so call the pair of EnterBulkMemCnxt()
    // and LeaveBulkMemCnxt() before BulkInsert() is invoked.
    // 
    void BulkInsert(HeapTuple tuple);
    void BulkInsertCopy(HeapTuple tuple);
    void Finish();

    FORCE_INLINE void EnterBulkMemCnxt()
    {
        m_OldMemCnxt = MemoryContextSwitchTo(m_MemCnxt);
        Assert(m_OldMemCnxt != NULL);
    }

    FORCE_INLINE void LeaveBulkMemCnxt()
    {
        Assert(m_OldMemCnxt != NULL);
        (void)MemoryContextSwitchTo(m_OldMemCnxt);
        m_OldMemCnxt = NULL;
    }

    // limit private memory context' upmost resource to use.
    static const int MaxBufferedTupNum = 10000;
    static const Size MaxBufferedTupSize = 8192 * 128;

private:
    MemoryContext m_OldMemCnxt;
    MemoryContext m_MemCnxt;
    HeapTuple *m_BufferedTups;
    Size m_BufferedTupsSize;
    int m_BufferedTupsNum;

    Relation m_HeapRel;
    BulkInsertState m_BIState;
    CommandId m_CmdId;
    int m_InsertOpt;

    template <bool needCopy>
    void Append(HeapTuple tuple);

    FORCE_INLINE void Flush();
    FORCE_INLINE bool Full() const;
};

class LoadSingleCu : public BaseObject {
public:
    // load data for single cu.
    // caller should delete CU object returned.
    static CU *LoadSingleCuData(_in_ CUDesc *pCuDesc, _in_ int colIdx, _in_ int colAttrLen, _in_ int colTypeMode,
                                _in_ uint32 colAtttyPid, _in_ Relation rel, __inout CUStorage *pCuStorage);
};

// remember those relation oids
// which ALTER TABLE SET DATATYPE is exec on.
class CStoreAlterRegister : public BaseObject {
public:
    CStoreAlterRegister() : m_Oids(NULL), m_used(0), m_maxs(0)
    {
    }
    virtual ~CStoreAlterRegister()
    {
    }
    virtual void Destroy();

    void Add(Oid relid);
    bool Find(Oid relid) const;

private:
    Oid *m_Oids;  // Oid array
    int m_used;   // Oid used number
    int m_maxs;   // Oid array max number

    bool DynSearch(Oid relid, int *pos) const;
};

//   GetCstoreAlterReg(): get the global instance, and create it the first time.
//   DestroyCstoreAlterReg(): destroy the global instance in COMMIT or ROLLBACK.
extern CStoreAlterRegister *GetCstoreAlterReg();
extern void DestroyCstoreAlterReg();

class CStoreRewriter : public BaseObject {
public:
    CStoreRewriter(Relation oldHeapRel, TupleDesc oldTupDesc, TupleDesc newTupDesc);
    virtual ~CStoreRewriter(){};
    virtual void Destroy();

    void ChangeTableSpace(Relation CUReplicationRel);
    void BeginRewriteCols(_in_ int nRewriteCols, _in_ CStoreRewriteColumn **pRewriteCols,
                          _in_ const int *rewriteColsNum, _in_ bool *rewriteFlags);
    void RewriteColsData();
    void EndRewriteCols();

public:
    template <bool append>
    void HandleCuWithSameValue(_in_ Relation rel, _in_ Form_pg_attribute newColAttr, _in_ Datum newColVal,
                               _in_ bool newColValIsNull, _in_ FuncSetMinMax func, _in_ CUStorage *colStorage,
                               __inout CUDesc *newColCudesc, __inout CUPointer *pOffset);

    static void FormCuDataForTheSameVal(__inout CU *cuPtr, _in_ int rowsCntInCu, _in_ Datum newColVal,
                                        _in_ Form_pg_attribute newColAttr);

    static void CompressCuData(CU *cuPtr, CUDesc *cuDesc, Form_pg_attribute newColAttr, int16 compressing_modes);

    FORCE_INLINE
    static void SaveCuData(_in_ CU *cuPtr, _in_ CUDesc *cuDesc, _in_ CUStorage *cuStorage);

private:
    FORCE_INLINE bool NeedRewrite() const;

    void AddColumns(_in_ uint32 cuId, _in_ int rowsCntInCu, _in_ bool wholeCuIsDeleted);
    void AddColumnInitPhrase1();
    void AddColumnInitPhrase2(_in_ CStoreRewriteColumn *addColInfo, _in_ int idx);
    void AddColumnDestroy();

    void SetDataType(_in_ uint32 cuId, _in_ HeapTuple *cudescTup, _in_ TupleDesc oldCudescTupDesc,
                     _in_ const char *delMaskDataPtr, _in_ int rowsCntInCu, _in_ bool wholeCuIsDeleted);
    void SetDataTypeInitPhase1();
    void SetDataTypeInitPhase2(_in_ CStoreRewriteColumn *sdtColInfo, _in_ int idx);
    void SetDataTypeDestroy();

    FORCE_INLINE
    void SetDataTypeHandleFullNullCu(_in_ CUDesc *oldColCudesc, _out_ CUDesc *newColCudesc);

    void SetDataTypeHandleSameValCu(_in_ int sdtIndex, _in_ CUDesc *oldColCudesc, _out_ CUDesc *newColCudesc);

    void SetDataTypeHandleNormalCu(_in_ int sdtIndex, _in_ const char *delMaskDataPtr, _in_ CUDesc *oldColCudesc,
                                   _out_ CUDesc *newColCudesc);

    void FetchCudescFrozenXid(Relation oldCudescHeap);

    void InsertNewCudescTup(_in_ CUDesc *pCudesc, _in_ TupleDesc pCudescTupDesc, _in_ Form_pg_attribute pColNewAttr);

    void HandleWholeDeletedCu(_in_ uint32 cuId, _in_ int rowsCntInCu, _in_ int nRewriteCols,
                              _in_ CStoreRewriteColumn **rewriteColsInfo);

    void FlushAllCUData() const;

private:
    Relation m_OldHeapRel;
    TupleDesc m_OldTupDesc;
    TupleDesc m_NewTupDesc;

    Oid m_NewCuDescHeap;
    bool *m_ColsRewriteFlag;

    // Set New Tablespace
    bool m_TblspcChanged;
    Oid m_TargetTblspc;
    Oid m_TargetRelFileNode;
    /* user a new relation for CU Replication
     * if tablespace is changed, we used a spicail CU Replication relation for new relfilenode.
     * else heap relation is also available.
     */
    Relation m_CUReplicationRel;
    CUPointer *m_SDTColAppendOffset;

    // ADD COLUMN
    int m_AddColsNum;
    CStoreRewriteColumn **m_AddColsInfo;
    CUStorage **m_AddColsStorage;
    FuncSetMinMax *m_AddColsMinMaxFunc;
    CUPointer *m_AddColsAppendOffset;

    // SET DATA TYPE
    int m_SDTColsNum;
    CStoreRewriteColumn **m_SDTColsInfo;
    CUStorage **m_SDTColsReader;
    FuncSetMinMax *m_SDTColsMinMaxFunc;
    CUStorage **m_SDTColsWriter;
    Datum *m_SDTColValues;
    bool *m_SDTColIsNull;

    // new cudesc relation's
    Relation m_NewCudescRel;
    HeapBulkInsert *m_NewCudescBulkInsert;
    TransactionId m_NewCudescFrozenXid;

    // m_estate is used during rewriting table data.
    // so it cannot be freed or reset before RewriteColsData() is end.
    // m_econtext is the same , but its ecxt_per_tuple_memory
    // must be reset after each data is handled successfully.
    EState *m_estate;
    ExprContext *m_econtext;
};

class BatchCUData : public BaseObject {
public:
    CUDesc **CUDescData;
    CU **CUptrData;
    char *bitmap;  // The delete bitmap that we would keep for the CU that's been moved
    int colNum;
    bool hasValue;

    BatchCUData()
    {
        CUDescData = NULL;
        CUptrData = NULL;
        bitmap = NULL;
        colNum = 0;
        hasValue = false;
    }

    virtual ~BatchCUData()
    {
    }

    void Init(const int &relColNum)
    {
        colNum = relColNum;
        CUDescData = (CUDesc **)palloc0(colNum * sizeof(CUDesc *));
        CUptrData = (CU **)palloc0(colNum * sizeof(CU *));
        for (int i = 0; i < colNum; ++i) {
            CUDescData[i] = New(CurrentMemoryContext) CUDesc;
        }
    }

    void CopyDelMask(unsigned char *cuDelMask)
    {
        bitmap = (char *)cuDelMask;
    }

    bool batchCUIsNULL()
    {
        return !hasValue;
    }

    void reset()
    {
        hasValue = false;
    }
    virtual void Destroy()
    {
        for (int i = 0; i < colNum; ++i) {
            DELETE_EX(CUDescData[i]);
        }
        pfree_ext(CUDescData);
        pfree_ext(CUptrData);
    }
};

extern void CStoreCopyColumnData(Relation CUReplicationRel, Relation rel, AttrNumber attrnum);
extern void CStoreCopyColumnDataEnd(Relation colRel, Oid targetTableSpace, Oid newrelfilenode);
extern Oid CStoreSetTableSpaceForColumnData(Relation colRel, Oid targetTableSpace);
extern void ATExecCStoreMergePartition(Relation partTableRel, AlterTableCmd *cmd);

#endif
