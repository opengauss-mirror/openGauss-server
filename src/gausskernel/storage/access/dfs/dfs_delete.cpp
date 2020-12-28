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
 * dfs_delete.cpp
 *    routines to support DFS(such as HDFS, DFS,...)
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/access/dfs/dfs_delete.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/dfs/dfs_delete.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "catalog/dfsstore_ctlg.h"
#include "catalog/indexing.h"
#include "catalog/pg_type.h"
#include "executor/executor.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/fmgroids.h"
#include "utils/numeric.h"
#include "utils/numeric_gs.h"
#include "utils/relcache.h"
#include "utils/snapmgr.h"
#include "access/heapam.h"
#include "utils/typcache.h"

/* max number of desc object for one load */
static const uint32 MaxCount = 1;

DfsDelete::DfsDelete(Relation rel, EState *estate, bool is_update, Plan *plan, MemInfoArg *m_dfsInsertMemInfo)
    : CStoreDelete(rel, estate, is_update, plan, m_dfsInsertMemInfo)
{
    /* dfs init memory will use CStoreDelete to do this function.
     * initialization of handler object of desc table
     */
    m_handler = New(CurrentMemoryContext) DFSDescHandler(MaxCount, RelationGetNumberOfAttributes(rel), rel);
}

DfsDelete::~DfsDelete()
{
    m_handler = NULL;
}

/*
 * save batch to sort cache
 */
void DfsDelete::PutDeleteBatch(VectorBatch *batch, JunkFilter *junkfilter)
{
    /*
     * m_PutDeleteBatchPtr = &CStoreDelete::PutDeleteBatchForTable
     * reuse sort cache in CStoreDelete
     */
    (this->*m_PutDeleteBatchPtr)(batch, junkfilter);

    /* if sort cache is full, do "delete" job with tid in cache */
    if (IsFull()) {
        /* execute delete if parital sort is full */
        m_totalDeleteNum += ExecDeleteForTable();

        /*
         * parital delete need commad id ++,
         * it's very important for the following delete
         */
        CommandCounterIncrement();

        /* reset sort cache */
        ResetSortState();
    }
}

/*
 * do "delete" job really with tids in sort cache, and return the number of
 * deleted rows.
 */
uint64 DfsDelete::ExecDeleteForTable()
{
    Assert(m_relation && m_estate && m_sortBatch && m_deleteSortState && m_rowOffset);

    ereport(DEBUG1, (errmodule(MOD_DFS), errmsg("dfs: %s()", __FUNCTION__)));

    uint64 delTotalRowNum = 0;

    /* begin to delete operation */
    (void)GetCurrentTransactionId();

    /* run sort */
    m_sortBatch->Reset(true);

    batchsort_performsort(m_deleteSortState);
    batchsort_getbatch(m_deleteSortState, true, m_sortBatch);

    /* do "delete" job */
    while (!BatchIsNull(m_sortBatch)) {
        /* get a group of tids */
        ScalarValue *tidValues = m_sortBatch->m_arr[m_ctidIdx - 1].m_vals;

        /* mark deletemap for deleted tuple */
        for (int i = 0; i < m_sortBatch->m_rows; i++) {
            delTotalRowNum += m_handler->SetDelMap((ItemPointer)(tidValues + i), m_isRptRepeatTupErrForUpdate);
        }

        /* get next sorted batch */
        batchsort_getbatch(m_deleteSortState, true, m_sortBatch);
    }
    /*
     * if we do update, we don't count delete num,
     * dead_tuple will increase when commit, see
     * function AtEOXact_PgStat.
     */
    if (!m_isUpdate)
        pgstat_count_dfs_delete(m_relation, delTotalRowNum);
    return delTotalRowNum;
}

/*
 * called on deinitialization, make sure that tids in sort cache can be
 * deleted.
 */
uint64 DfsDelete::ExecDelete()
{
    m_totalDeleteNum += ExecDeleteForTable();
    m_handler->FlushDelMap();

    return m_totalDeleteNum;
}
