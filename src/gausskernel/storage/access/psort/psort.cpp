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
 *
 * psort.cpp
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/access/psort/psort.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/psort.h"
#include "access/cstore_am.h"
#include "access/cstore_insert.h"
#include "access/reloptions.h"
#include "access/sysattr.h"
#include "access/tableam.h"
#include "catalog/index.h"
#include "miscadmin.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/snapmgr.h"

FORCE_INLINE void ProjectToIndexVector(VectorBatch *scanBatch, VectorBatch *outBatch, IndexInfo *indexInfo);
FORCE_INLINE double InsertToPsort(VectorBatch *scanBatch, VectorBatch *outBatch, IndexInfo *indexInfo,
                                  CStoreInsert *cstoreInsert, double &scanRows);
Datum psortbuild(PG_FUNCTION_ARGS)
{
    Relation heapRel = (Relation)PG_GETARG_POINTER(0);
    Relation indexRel = (Relation)PG_GETARG_POINTER(1);
    IndexInfo *indexInfo = (IndexInfo *)PG_GETARG_POINTER(2);

    double scanRows = 0;
    double insRows = 0;

    // 1. perpare for heap scan
    Snapshot snapshot;
    CStoreScanDesc scanstate = NULL;
    VectorBatch *vecScanBatch = NULL;
    MemInfoArg memInfo;

    // Now we use snapshotNow for check tuple visibility
    // 
    snapshot = SnapshotNow;

    // columns for cstore scan
    int heapScanNumIndexAttrs = indexInfo->ii_NumIndexAttrs + 1;
    AttrNumber *heapScanAttrNumbers = (AttrNumber *)palloc(sizeof(AttrNumber) * heapScanNumIndexAttrs);
    for (int i = 0; i < indexInfo->ii_NumIndexAttrs; i++) {
        heapScanAttrNumbers[i] = indexInfo->ii_KeyAttrNumbers[i];
    }

    // add ctid column for cstore scan
    heapScanAttrNumbers[heapScanNumIndexAttrs - 1] = SelfItemPointerAttributeNumber;

    // 2. perpare for posrt insert
    // get psort relation id from index relation
    Oid psortRelId = indexRel->rd_rel->relcudescrelid;
    Relation psortRel = relation_open(psortRelId, AccessExclusiveLock);

    // psortRel vector
    InsertArg args;
    CStoreInsert::InitInsertArg(psortRel, NULL, true, args);
    args.sortType = BATCH_SORT;
    memInfo.canSpreadmaxMem = indexInfo->ii_desc.query_mem[1];
    memInfo.MemSort = indexInfo->ii_desc.query_mem[0];
    memInfo.partitionNum = 1;
    CStoreInsert *cstoreInsert = New(CurrentMemoryContext) CStoreInsert(psortRel, args, false, NULL, &memInfo);
    VectorBatch *vecOutBatch = New(CurrentMemoryContext) VectorBatch(CurrentMemoryContext, psortRel->rd_att);

    // 3. scan heap
    // for cstore table
    scanstate = CStoreBeginScan(heapRel, heapScanNumIndexAttrs, heapScanAttrNumbers, snapshot, false);

    do {
        vecScanBatch = CStoreGetNextBatch(scanstate);
        insRows += InsertToPsort(vecScanBatch, vecOutBatch, indexInfo, cstoreInsert, scanRows);
    } while (!CStoreIsEndScan(scanstate));

    // 6. end insert
    cstoreInsert->SetEndFlag();
    cstoreInsert->BatchInsert((VectorBatch*)NULL, TABLE_INSERT_FROZEN);

    // 7. end scan
    CStoreEndScan(scanstate);

    DELETE_EX(cstoreInsert);
    delete vecOutBatch;
    CStoreInsert::DeInitInsertArg(args);

    relation_close(psortRel, NoLock);

    // 8. set return value
    IndexBuildResult *result = (IndexBuildResult *)palloc(sizeof(IndexBuildResult));
    result->heap_tuples = scanRows;
    result->index_tuples = insRows;

    PG_RETURN_POINTER(result);
}

Datum psortcanreturn(PG_FUNCTION_ARGS)
{
    PG_RETURN_BOOL(true);
}

Datum psortoptions(PG_FUNCTION_ARGS)
{
    Datum indexRelOptions = PG_GETARG_DATUM(0);
    bool validate = PG_GETARG_BOOL(1);

    bytea *filledOption = default_reloptions(indexRelOptions, validate, RELOPT_KIND_PSORT);
    if (filledOption != NULL)
        PG_RETURN_BYTEA_P(filledOption);

    PG_RETURN_NULL();
}

Datum psortgettuple(PG_FUNCTION_ARGS)
{
    PG_RETURN_NULL();
}

Datum psortgetbitmap(PG_FUNCTION_ARGS)
{
    PG_RETURN_NULL();
}

inline void ProjectToIndexVector(VectorBatch *scanBatch, VectorBatch *outBatch, IndexInfo *indexInfo)
{
    Assert(scanBatch && outBatch && indexInfo);
    int numAttrs = indexInfo->ii_NumIndexAttrs;
    AttrNumber *attrNumbers = indexInfo->ii_KeyAttrNumbers;
    Assert(outBatch->m_cols == (numAttrs + 1));

    // index column
    for (int i = 0; i < numAttrs; i++) {
        AttrNumber attno = attrNumbers[i];
        Assert(attno > 0 && attno <= scanBatch->m_cols);

        // shallow copy
        outBatch->m_arr[i].copy(&scanBatch->m_arr[attno - 1]);
    }

    // ctid column
    // shallow copy
    outBatch->m_arr[numAttrs].copy(scanBatch->GetSysVector(-1));

    outBatch->m_rows = scanBatch->m_rows;
}

/*
 * @Description: project and insert the outBatch into psort table.
 * @IN scanBatch: vector batch
 * @IN outBatch: vector batch to be inserted
 * @IN indexInfo: the index information
 * @IN cstoreInsert: the cstore insert object
 * @IN/OUT scanRows: the rows have been scanned
 * @Return: the number of rows of the current outBatch
 * @See also:
 */
inline double InsertToPsort(VectorBatch *scanBatch, VectorBatch *outBatch, IndexInfo *indexInfo,
                            CStoreInsert *cstoreInsert, double &scanRows)
{
    if (!BatchIsNull(scanBatch)) {
        scanRows += scanBatch->m_rows;

        // project to posrt vector
        ProjectToIndexVector(scanBatch, outBatch, indexInfo);

        // posrt insert
        cstoreInsert->BatchInsert(outBatch, TABLE_INSERT_FROZEN);

        return outBatch->m_rows;
    }

    return 0;
}
