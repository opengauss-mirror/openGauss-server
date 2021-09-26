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
 * cbtree.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/cbtree/cbtree.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/nbtree.h"
#include "access/cbtree.h"
#include "access/cstore_am.h"
#include "access/dfs/dfs_am.h"
#include "access/reloptions.h"
#include "access/sysattr.h"
#include "access/relscan.h"
#include "access/xlog.h"
#include "catalog/index.h"
#include "commands/vacuum.h"
#include "storage/indexfsm.h"
#include "storage/ipc.h"
#include "storage/lmgr.h"
#include "storage/smgr/smgr.h"
#include "tcop/tcopprot.h"
#include "miscadmin.h"
#include "utils/aiomem.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/snapmgr.h"

static void InsertToBtree(VectorBatch *vecScanBatch, BTBuildState &buildstate, IndexInfo *indexInfo, double &reltuples,
                          Datum *values, bool *isnulls, ScalarToDatum *transferFuncs);

Datum cbtreebuild(PG_FUNCTION_ARGS)
{
    Relation heapRel = (Relation)PG_GETARG_POINTER(0);
    Relation indexRel = (Relation)PG_GETARG_POINTER(1);
    IndexInfo *indexInfo = (IndexInfo *)PG_GETARG_POINTER(2);

    IndexBuildResult *result = NULL;
    double reltuples;
    BTBuildState buildstate;
    Datum values[INDEX_MAX_KEYS];
    bool isnull[INDEX_MAX_KEYS];
    ScalarToDatum transferFuncs[INDEX_MAX_KEYS];

    /* 1. perpare for heap scan */
    Snapshot snapshot;
    DfsScanState *dfsScanState = NULL;
    CStoreScanDesc scanstate = NULL;
    VectorBatch *vecScanBatch = NULL;

    /* Now we use snapshotNow for check tuple visibility */
    snapshot = SnapshotNow;

    /* add index columns for cstore/dfs scan */
    int heapScanNumIndexAttrs = indexInfo->ii_NumIndexAttrs + 1;
    AttrNumber *heapScanAttrNumbers = (AttrNumber *)palloc(sizeof(AttrNumber) * heapScanNumIndexAttrs);
    for (int i = 0; i < indexInfo->ii_NumIndexAttrs; i++) {
        heapScanAttrNumbers[i] = indexInfo->ii_KeyAttrNumbers[i];
        if (heapScanAttrNumbers[i] < 1) {
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE), 
                     errmsg("Invalid index column, attribute column index is %d", heapScanAttrNumbers[i])));
        }
        transferFuncs[i] = GetTransferFuncByTypeOid(heapRel->rd_att->attrs[heapScanAttrNumbers[i] - 1]->atttypid);
    }

    /* add ctid column for cstore scan */
    heapScanAttrNumbers[heapScanNumIndexAttrs - 1] = SelfItemPointerAttributeNumber;

    /* 2. perpare for btree pool */
    buildstate.isUnique = indexInfo->ii_Unique;
    buildstate.haveDead = false;
    buildstate.heapRel = heapRel;
    buildstate.spool = NULL;
    buildstate.spool2 = NULL;
    buildstate.indtuples = 0;
    buildstate.spool = _bt_spoolinit(heapRel, indexRel, indexInfo->ii_Unique, false, &indexInfo->ii_desc);

    /* 3. scan heap table and insert tuple into btree */
    if (RelationIsDfsStore(heapRel)) {
        /* for dfs table */
        dfsScanState = dfs::reader::DFSBeginScan(heapRel, NIL, heapScanNumIndexAttrs, heapScanAttrNumbers, snapshot);
        do {
            vecScanBatch = dfs::reader::DFSGetNextBatch(dfsScanState);
            if (vecScanBatch != NULL)
                InsertToBtree(vecScanBatch, buildstate, indexInfo, reltuples, values, isnull, transferFuncs);
        } while (!BatchIsNull(vecScanBatch));
    } else {
        /* for cstore table */
        scanstate = CStoreBeginScan(heapRel, heapScanNumIndexAttrs, heapScanAttrNumbers, snapshot, false);

        do {
            vecScanBatch = CStoreGetNextBatch(scanstate);
            InsertToBtree(vecScanBatch, buildstate, indexInfo, reltuples, values, isnull, transferFuncs);
        } while (!CStoreIsEndScan(scanstate));
    }

    /* 4. end scan */
    if (RelationIsDfsStore(heapRel)) {
        dfs::reader::DFSEndScan(dfsScanState);
    } else {
        CStoreEndScan(scanstate);
    }

    /* 5. clean btree pool */
    /*
     * Finish the build by (1) completing the sort of the spool file, (2)
     * inserting the sorted tuples into btree pages and (3) building the upper
     * levels.
     */
    _bt_leafbuild(buildstate.spool, buildstate.spool2);
    _bt_spooldestroy(buildstate.spool);

    /*
     * Return statistics
     */
    result = (IndexBuildResult *)palloc(sizeof(IndexBuildResult));

    result->heap_tuples = reltuples;
    result->index_tuples = buildstate.indtuples;

    PG_RETURN_POINTER(result);
}

Datum cbtreecanreturn(PG_FUNCTION_ARGS)
{
    PG_RETURN_BOOL(true);
}

Datum cbtreeoptions(PG_FUNCTION_ARGS)
{
    Datum indexRelOptions = PG_GETARG_DATUM(0);
    bool validate = PG_GETARG_BOOL(1);

    bytea *filledOption = default_reloptions(indexRelOptions, validate, RELOPT_KIND_CBTREE);
    if (filledOption != NULL)
        PG_RETURN_BYTEA_P(filledOption);

    PG_RETURN_NULL();
}

Datum cbtreegettuple(PG_FUNCTION_ARGS)
{
    IndexScanDesc scan = (IndexScanDesc)PG_GETARG_POINTER(0);
    ScanDirection dir = (ScanDirection)PG_GETARG_INT32(1);

    if (scan == NULL)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("Invalid arguments for function cbtreegettuple")));

    bool res = _bt_gettuple_internal(scan, dir);

    PG_RETURN_BOOL(res);
}

/*
 * Insert the vector batch into btree pool.
 * @IN param vecScanBatch: the prepared vector batch
 * @OUT param buildstate: include the btree pool to be insert
 * @IN param indexInfo: the index information
 * @OUT param reltuples: the number of tuples which have been inserted into btree
 * @IN param values: the container to use temprarily
 * @IN param isnulls: the container to use temprarily
 * @IN param transferFuncs: the transfer functions array
 */
static void InsertToBtree(VectorBatch *vecScanBatch, BTBuildState &buildstate, IndexInfo *indexInfo, double &reltuples,
                          Datum *values, bool *isnulls, ScalarToDatum *transferFuncs)
{
    int rows = vecScanBatch->m_rows;
    ScalarVector *vec = vecScanBatch->m_arr;
    ScalarVector *sysVec = vecScanBatch->GetSysVector(SelfItemPointerAttributeNumber);
    reltuples += rows;

    for (int rowIdx = 0; rowIdx < rows; rowIdx++) {
        int i;
        ItemPointer tid;

        for (i = 0; i < indexInfo->ii_NumIndexAttrs; i++) {
            int32 colIdx = indexInfo->ii_KeyAttrNumbers[i] - 1;
            if (vec[colIdx].IsNull(rowIdx)) {
                isnulls[i] = true;
            } else {
                isnulls[i] = false;
                values[i] = transferFuncs[i](vec[colIdx].m_vals[rowIdx]);
            }
        }

        tid = (ItemPointer)(&sysVec->m_vals[rowIdx]);

        /* Fill btree spool */
        _bt_spool(buildstate.spool, tid, values, isnulls);
        buildstate.indtuples += 1;
    }
}

/*
 * Check unique on other index.
 * This function is usually used between delta index and CU index.
 * For example, the data tuple is on deta table, but we check uniqe
 * constraint on CU Idx.
 * @IN param index: the index will be checked
 * @IN param heapRel: the relaion owns index
 * @IN param values: values uesd to form index tuple
 * @IN param isull: isull used to form index tuple
 */
void CheckUniqueOnOtherIdx(Relation index, Relation heapRel, Datum* values, const bool* isnull)
{
    bool is_unique = false;
    ScanKey itup_scankey;
    BTStack stack;
    Buffer buf;
    IndexTuple itup;
    CUDescScan* cudescScan = NULL;

    /* Generate an index tuple. */
    itup = index_form_tuple(RelationGetDescr(index), values, isnull);

    /*
     * We set tid to invalid tid, so the index tuple doesn't point to heapRel.
     * For example, the data tuple is on delta table, but we check if the key
     * of data tuple conficts the index on corresponding CU.
     */
    ItemPointerSetInvalid(&(itup->t_tid));

    if (RelationIsCUFormat(heapRel)) {
        cudescScan = (CUDescScan*)New(CurrentMemoryContext) CUDescScan(heapRel);
    }

    BTCheckElement element;
    is_unique = SearchBufferAndCheckUnique(index, itup, UNIQUE_CHECK_YES, heapRel, NULL, NULL, cudescScan, &element);

    buf = element.buffer;
    stack = element.btStack;
    itup_scankey = element.itupScanKey;

    /* release buffer. */
    _bt_relbuf(index, buf);

    /* be tidy */
    _bt_freestack(stack);
    _bt_freeskey(itup_scankey);

    if (cudescScan != NULL) {
        cudescScan->Destroy();
        delete cudescScan;
        cudescScan = NULL;
    }

    pfree(itup);
}
