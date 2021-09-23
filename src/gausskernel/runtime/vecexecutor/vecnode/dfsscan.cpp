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
 * dfsscan.cpp
 *    Support routines for Dfs scans of dfs stores.
 *
 * IDENTIFICATION
 *        Code/src/gausskernel/runtime/vecexecutor/vecnode/dfsscan.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "access/dfs/dfs_query.h"
#include "access/dfs/dfs_query_reader.h"
#include "postgres.h"
#include "knl/knl_variable.h"
#include "access/relscan.h"
#include "executor/exec/execdebug.h"
#include "vecexecutor/vecnodecstorescan.h"
#include "executor/node/nodeSeqscan.h"
#include "storage/cstore/cstore_compress.h"
#include "access/cstore_am.h"
#include "optimizer/clauses.h"
#include "nodes/params.h"
#include "nodes/makefuncs.h"
#include "utils/lsyscache.h"
#include "utils/datum.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "executor/node/nodeSeqscan.h"
#include "access/cstoreskey.h"
#include "catalog/pg_operator.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "access/sysattr.h"
#include "catalog/indexing.h"
#include "commands/tablespace.h"
#include "access/heapam.h"
#include "utils/fmgroids.h"
#include "utils/snapmgr.h"
#include "catalog/pg_partition_fn.h"
#include "catalog/dfsstore_ctlg.h"
#include "dfsdesc.h"

VectorBatch* DfsScanNext(DfsScanState* node);
static bool DfsScanRecheck(ForeignScanState* node, VectorBatch* batch);

/*
 * Brief        : Start to scan using the DfsScan. In fact, the DfsScanNext function will
 *                real scan data from dfs.
 * Input        : None.
 * Output       : None.
 * Return Value : Return the batch if find the relation data, return NULL otherwise.
 * Notes        : None.
 */
VectorBatch* ExecDfsScan(DfsScanState* node)
{
    return ExecVecScan((ScanState*)node, (ExecVecScanAccessMtd)DfsScanNext, (ExecVecScanRecheckMtd)DfsScanRecheck);
}

/*
 * Brief        : Get the next vector batch data.
 * Input        : node, a DfsScanState struct.
 * Output       : None.
 * Return Value : Return the batch if find the relation data, return NULL otherwise.
 * Notes        : None.
 */
VectorBatch* DfsScanNext(DfsScanState* node)
{
    MemoryContext context = node->m_scanCxt;
    MemoryContext oldContext;
    VectorBatch* batch = node->m_pScanBatch;

    if (node->m_done) {
        return NULL;
    }

    if (!node->m_readerState->scanstate->runTimePredicatesReady) {
        BuildRunTimePredicates(node->m_readerState);
        node->m_fileReader->dynamicAssignSplits();
    }

    MemoryContextReset(context);
    oldContext = MemoryContextSwitchTo(context);
    node->m_fileReader->nextBatch(batch);

    if (((Scan*)(node->ps.plan))->predicate_pushdown_optimized && !BatchIsNull(batch)) {
        batch->m_rows = 1;
        batch->FixRowCount();
        node->is_scan_end = true;
    }
    (void)MemoryContextSwitchTo(oldContext);

    return batch;
}

/*
 * Brief        : Reinitialize DfsScanState.
 * Input        : node, a DfsScanState struct.
 * Output       : None.
 * Return Value : None.
 * Notes        : Reinitialize the running predicates and the list of files to be read.
 */
void ExecReScanDfsScan(DfsScanState* node)
{
    DfsScan* dfsScan = (DfsScan*)node->ps.plan;

    if ((NULL == node->m_readerState || NULL == node->m_fileReader) || 0 == list_length(dfsScan->privateData)) {
        return;
    }

    MemoryContextReset(node->m_readerState->rescanCtx);
    AutoContextSwitch newContext(node->m_readerState->rescanCtx);
    node->m_readerState->scanstate->runTimePredicatesReady = false;

    /*
     * Rebuild the filePaths.
     */
    if (0 == list_length(node->m_splitList)) {
        node->m_readerState->splitList = NIL;
    } else {
        node->m_readerState->splitList = (List*)copyObject(node->m_splitList);
    }
    node->m_readerState->currentSplit = NULL;
    node->m_readerState->currentFileID = 0;

    ExecScanReScan((ScanState*)node);
}

/*
 * Brief        : Check the validaty. Currently, we do not nothing here.
 * Input        : None.
 * Output       : None.
 * Return Value : Return true.
 * Notes        : None.
 */
static bool DfsScanRecheck(ForeignScanState* node, VectorBatch* batch)
{
    /*
     * There are no access-method-specific conditions to recheck.
     */
    return true;
}

DfsScanState* ExecInitDfsScan(DfsScan* node, Relation parentHeapRel, EState* estate, int eflags, bool indexFlag)
{
    DfsScanState* scanState = NULL;
    PlanState* planState = NULL;
    ScalarDesc unknownDesc;
    Relation currentRelation;
    StringInfo rootDir;

    /*
     * Dfs store can only be a leaf node.
     */
    Assert(outerPlan(node) == NULL);
    Assert(innerPlan(node) == NULL);

    scanState = makeNode(DfsScanState);
    scanState->m_done = false;
    scanState->ps.plan = (Plan*)node;
    scanState->ps.state = estate;
    DfsPrivateItem* item = (DfsPrivateItem*)((DefElem*)linitial(node->privateData))->arg;

    /* Here we need to adjust the plan qual to avoid double filtering. */
    list_delete_list(&node->plan.qual, item->hdfsQual);

    /*
     * Miscellaneous initialization.
     * Create expression context for node.
     */
    ExecAssignExprContext(estate, &scanState->ps);

    scanState->ps.ps_TupFromTlist = false;

    /*
     * Initialize child expressions.
     */
    scanState->ps.targetlist = (List*)ExecInitVecExpr((Expr*)node->plan.targetlist, (PlanState*)scanState);
    scanState->ps.qual = (List*)ExecInitVecExpr((Expr*)node->plan.qual, (PlanState*)scanState);

    /*
     * Tuple table initialization.
     */
    ExecInitResultTupleSlot(estate, &scanState->ps);
    ExecInitScanTupleSlot(estate, (ScanState*)scanState);

    /*
     * Open the base relation and acquire appropriate lock on it.
     */
    currentRelation = ExecOpenScanRelation(estate, node->scanrelid);
    scanState->ss_currentRelation = currentRelation;

    /*
     * Get the scan type from the relation descriptor.
     */
    ExecAssignScanType(((ScanState*)scanState), RelationGetDescr(currentRelation));

    /*
     * Initialize result tuple type and projection info.
     */
    ExecAssignResultTypeFromTL(&scanState->ps, currentRelation->rd_tam_type);

    scanState->ps.vectorized = true;
    scanState->isPartTbl = node->isPartTbl;
    scanState->partScanDirection = node->partScanDirection;

    scanState->m_scanCxt = AllocSetContextCreate(CurrentMemoryContext,
        "Dfs Scan",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);
    scanState->m_pCurrentBatch = New(CurrentMemoryContext)
        VectorBatch(CurrentMemoryContext, scanState->ps.ps_ResultTupleSlot->tts_tupleDescriptor);
    scanState->m_pScanBatch =
        New(CurrentMemoryContext) VectorBatch(CurrentMemoryContext, scanState->ss_currentRelation->rd_att);
    scanState->m_splitList = NIL;

    planState = &(scanState->ps);
    planState->ps_ProjInfo = ExecBuildVecProjectionInfo(planState->targetlist,
        node->plan.qual,
        planState->ps_ExprContext,
        planState->ps_ResultTupleSlot,
        scanState->ss_ScanTupleSlot->tts_tupleDescriptor);
    if (planState->ps_ProjInfo->pi_sysAttrList) {
        scanState->m_pScanBatch->CreateSysColContainer(CurrentMemoryContext, planState->ps_ProjInfo->pi_sysAttrList);
    }
    ExecAssignVectorForExprEval(planState->ps_ExprContext);

    dfs::reader::ReaderState* readerState = (dfs::reader::ReaderState*)palloc0(sizeof(dfs::reader::ReaderState));

    /*
     * Build the filePaths.
     */
    rootDir = getDfsStorePath(currentRelation);
    readerState->splitList = BuildSplitList(currentRelation, rootDir);
    pfree_ext(rootDir);
    if (0 == list_length(readerState->splitList)) {
        scanState->m_done = true;
        return scanState;
    }

    FillReaderState(readerState, scanState, item);
    scanState->m_splitList = (List*)copyObject(readerState->splitList);

    /*
     * Build the connector to DFS. The controller will be transfered into dfs reader.
     */
    DfsSrvOptions* srvOptions = GetDfsSrvOptions(currentRelation->rd_rel->reltablespace);
    dfs::DFSConnector* conn =
        dfs::createConnector(readerState->persistCtx, srvOptions, currentRelation->rd_rel->reltablespace);
    if (NULL == conn) {
        ereport(
            ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED), errmsg("failed to connect hdfs during init DFS scan")));
    }

    scanState->m_readerState = readerState;
    if (0 == pg_strncasecmp(node->storeFormat, ORIENTATION_ORC, strlen(ORIENTATION_ORC))) {
        scanState->m_fileReader = dfs::reader::createOrcReader(readerState, conn, false);
        scanState->m_readerState->fileReader = scanState->m_fileReader;
    } else {
        /* If the we support the PARQUET or other foramt, this code will be modified. */
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Unsupport store format, only support ORC format for DFS table.")));
    }

    if (u_sess->attr.attr_sql.enable_bloom_filter && list_length(item->hdfsQual) > 0) {
        for (uint32 i = 0; i < readerState->relAttrNum; i++) {
            filter::BloomFilter* blf = readerState->bloomFilters[i];
            if (NULL != blf) {
                scanState->m_fileReader->addBloomFilter(blf, i, false);
            }
        }
    }

    return scanState;
}

/*
 * Brief        : Clear up the state and memory.
 * Input        : node, the DfsScanState struct.
 * Output       : None.
 * Return Value : None.
 * Notes        : None.
 */
void ExecEndDfsScan(DfsScanState* node)
{
    /*
     * Clean out the "dfs scan" context.
     */
    MemoryContextDelete(node->m_scanCxt);

    /*
     * Close the heap relation.
     */
    Relation relation = node->ss_currentRelation;
    ExecCloseScanRelation(relation);

    dfs::reader::ReaderState* readerState = node->m_readerState;
    if (NULL == readerState) {
        return;
    }

    if (NULL != node->m_fileReader) {
        /* remove the reference from file reader list */
        RemoveDfsReadHandler(node->m_fileReader);
        node->m_fileReader->end();
        DELETE_EX(node->m_fileReader);
        readerState->fileReader = NULL;
    }

    Instrumentation* instr = node->ps.instrument;
    if (NULL != instr) {
        instr->dfsType = readerState->dfsType;
        instr->dynamicPrunFiles = readerState->dynamicPrunFiles;
        instr->staticPruneFiles = readerState->staticPruneFiles;
        instr->bloomFilterRows = readerState->bloomFilterRows;
        instr->minmaxFilterRows = readerState->minmaxFilterRows;
        instr->bloomFilterBlocks = readerState->bloomFilterBlocks;
        instr->localBlock = readerState->localBlock;
        instr->remoteBlock = readerState->remoteBlock;
        instr->nnCalls = readerState->nnCalls;
        instr->dnCalls = readerState->dnCalls;
        instr->minmaxCheckFiles = readerState->minmaxCheckFiles;
        instr->minmaxFilterFiles = readerState->minmaxFilterFiles;
        instr->minmaxCheckStripe = readerState->minmaxCheckStripe;
        instr->minmaxFilterStripe = readerState->minmaxFilterStripe;
        instr->minmaxCheckStride = readerState->minmaxCheckStride;
        instr->minmaxFilterStride = readerState->minmaxFilterStride;

        instr->orcMetaCacheBlockCount = readerState->orcMetaCacheBlockCount;
        instr->orcMetaCacheBlockSize = readerState->orcMetaCacheBlockSize;
        instr->orcMetaLoadBlockCount = readerState->orcMetaLoadBlockCount;
        instr->orcMetaLoadBlockSize = readerState->orcMetaLoadBlockSize;
        instr->orcDataCacheBlockCount = readerState->orcDataCacheBlockCount;
        instr->orcDataCacheBlockSize = readerState->orcDataCacheBlockSize;
        instr->orcDataLoadBlockCount = readerState->orcDataLoadBlockCount;
        instr->orcDataLoadBlockSize = readerState->orcDataLoadBlockSize;
    }

    MemoryContextDelete(readerState->persistCtx);
    MemoryContextDelete(readerState->rescanCtx);
    pfree_ext(readerState);

    /*
     * Free the exprcontext.
     */
    ExecFreeExprContext(&node->ps);

    /*
     * Clean out the tuple table.
     */
    if (node->ps.ps_ResultTupleSlot)
        (void)ExecClearTuple(node->ps.ps_ResultTupleSlot);
    if (node->ss_ScanTupleSlot)
        (void)ExecClearTuple(node->ss_ScanTupleSlot);
}

/* the path concats by reliable information in system catalog, but to avoid attack, we still check the information */
static char* canonicalize_dfs_path(const char* path)
{
    char* lrealpath = NULL;
    char* ret_val = NULL;
    const short max_realpath_len = 4096;
    lrealpath = (char*)palloc(max_realpath_len + 1);
    
    ret_val = realpath(path, lrealpath);
    if (ret_val == NULL || lrealpath[0] == '\0') {
        ereport(
            ERROR, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION), errmsg("realpath failed : %s!\n", path)));
    }
    char* shortpath = pstrdup(lrealpath);
    pfree(lrealpath);
    return shortpath;
}

/*
 * Brief        : Build the Dfs table file path List.
 * Input        : rel, the table relation.
 *                rootDir, the root path.
 *                snapshot, the snapshot to read.
 * Output       : None.
 * Return Value : Return the file path list using SplitInfo struct.
 * Notes        : None.
 */
List* BuildSplitList(Relation rel, StringInfo rootDir, Snapshot snapshot)
{
    List* fileSplits = NIL;
    uint32 startId = 0;
    uint32 pLastId = 0;
    uint32 pLoadCnt = 0;
    int colNum = 0;

    Assert(RelationIsDfsStore(rel));
    colNum = RelationGetNumberOfAttributes(rel);
    Assert(OidIsValid(getDfsDescTblOid(RelationGetRelid(rel))));
    Assert(OidIsValid(getDfsDescIndexOid(RelationGetRelid(rel))));

    /*
     * Init the dfsDescHandler.
     */
    DFSDescHandler dfsDescHandler(MAX_LOADED_DFSDESC, colNum, rel);

    /*
     * Init the dfsDesc array.
     */
    DFSDesc* dfsDesc = New(CurrentMemoryContext) DFSDesc[MAX_LOADED_DFSDESC];

    while (true) {
        /*
         * Load DfsDesc tuple.
         */
        int loadSucess = 0;
        loadSucess = dfsDescHandler.Load(
            dfsDesc, MAX_LOADED_DFSDESC, NON_PARTITION_TALBE_PART_NUM, startId, snapshot, &pLoadCnt, &pLastId);
        if (!loadSucess) {
            break;
        }

        /*
         * Now, build the file path list.
         */
        for (uint32 dfsDescIndex = 0; dfsDescIndex < pLoadCnt; dfsDescIndex++) {
            const char* fileName = dfsDesc[dfsDescIndex].GetFileName();
            StringInfo filePath = makeStringInfo();
            appendStringInfo(filePath, "%s/%s", rootDir->data, fileName);
            char* validpath = canonicalize_dfs_path(filePath->data);
            pfree(filePath->data);
            SplitInfo* split = InitFileSplit(validpath, NULL, dfsDesc[dfsDescIndex].GetFileSize());
            fileSplits = lappend(fileSplits, split);
        }

        if (pLoadCnt < MAX_LOADED_DFSDESC) {
            break;
        }

        pLoadCnt = 0;
        startId = pLastId + 1;
    }

    /*
     * Free  resource.
     */
    for (uint32 descIndex = 0; descIndex < MAX_LOADED_DFSDESC; descIndex++) {
        dfsDesc[descIndex].Destroy();
    }
    delete[] dfsDesc;
    dfsDesc = NULL;

    return fileSplits;
}
