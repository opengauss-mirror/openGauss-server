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
 * cstore_update.cpp
 *      routines to support ColStore
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/cstore/cstore_update.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/cstore_update.h"
#include "catalog/pg_partition_fn.h"

int CStoreUpdate::BATCHROW_TIMES = 3;

extern void ExecVecConstraints(ResultRelInfo* resultRelInfo, VectorBatch* batch, EState* estate);

CStoreUpdate::CStoreUpdate(_in_ Relation rel, _in_ EState* estate, _in_ Plan* plan) : m_estate(estate)
{
    Assert(estate);
    m_relation = rel;
    m_resultRelInfo = estate->es_result_relation_info;
    m_isPartition = RELATION_IS_PARTITIONED(rel);
    m_delMemInfo = NULL;
    m_insMemInfo = NULL;

    /* init memory, memory info will be used to init delete and insert. */
    InitUpdateMemArg(plan);

    /* init delete */
    m_delete = New(CurrentMemoryContext) CStoreDelete(rel, estate, true, NULL, m_delMemInfo);
    m_delete->setReportErrorForUpdate(true);

    /* init insert */
    if (!m_isPartition) {
        CStoreInsert::InitInsertArg(rel, m_resultRelInfo, true, m_insert_args);
        m_insert_args.sortType = BATCH_SORT;

        m_insert = New(CurrentMemoryContext) CStoreInsert(rel, m_insert_args, true, NULL, m_insMemInfo);
        m_partionInsert = NULL;
    } else {
        m_partionInsert =
            New(CurrentMemoryContext) CStorePartitionInsert(rel, m_resultRelInfo, TUPLE_SORT, true, NULL, m_insMemInfo);

        /* update using flash cached data when switch partition when insert data  */
        m_partionInsert->SetPartitionCacheStrategy(FLASH_WHEN_SWICH_PARTITION);
        m_insert = NULL;
    }

    m_hasUniqueIdx = CheckHasUniqueIdx();
}

CStoreUpdate::~CStoreUpdate()
{
    m_relation = NULL;
    m_insert = NULL;
    m_resultRelInfo = NULL;
    m_partionInsert = NULL;
    m_estate = NULL;
    m_insMemInfo = NULL;
    m_delete = NULL;
    m_delMemInfo = NULL;
}

void CStoreUpdate::Destroy()
{
    if (m_delete) {
        DELETE_EX(m_delete);
    }

    if (m_insert) {
        DELETE_EX(m_insert);
        CStoreInsert::DeInitInsertArg(m_insert_args);
    }

    if (m_partionInsert) {
        DELETE_EX(m_partionInsert);
    }

    if (m_delMemInfo) {
        pfree_ext(m_delMemInfo);
    }

    if (m_insMemInfo) {
        pfree_ext(m_insMemInfo);
    }
}

/*
 * @Description: init update memory info for cstore update. There are three branches, plan is the optimizer
 *    estimation parameter passed to the storage layer for execution; others is uncontrolled memory.
 *   ArgmemInfo is to execute the operator from the upper layer to pass the parameter to the update (will be added);
 * @IN plan: If update operator is directly used, the plan mem_info is given to execute.
 * @Return: void
 * @See also: InitInsertMemArg
 */
void CStoreUpdate::InitUpdateMemArg(Plan* plan)
{
    int maxbatchRows = RelationGetMaxBatchRows(m_relation);
    int partialClusterRows = RelationGetPartialClusterRows(m_relation);
    int partitionNum = 1;
    List* partitionList = NIL;

    m_delMemInfo = (MemInfoArg*)palloc0(sizeof(struct MemInfoArg));
    m_insMemInfo = (MemInfoArg*)palloc0(sizeof(struct MemInfoArg));
    if (m_isPartition) {
        partitionList = relationGetPartitionList(m_relation, AccessShareLock);
        /* get partition number */
        partitionNum = list_length(partitionList);
        releasePartitionList(m_relation, &partitionList, NoLock);
    }

    /* init mem = delete mem + insert mem. */
    if (plan != NULL && plan->operatorMemKB[0] > 0) {
        m_delMemInfo->canSpreadmaxMem = plan->operatorMaxMem;
        m_delMemInfo->MemInsert = 0;
        m_delMemInfo->spreadNum = 0;
        m_delMemInfo->partitionNum = 1;
        if (m_isPartition) {
            /* for partition insert. */
            m_insMemInfo->canSpreadmaxMem = plan->operatorMaxMem;
            m_insMemInfo->MemInsert = plan->operatorMemKB[0] * 1 / 3;
            m_insMemInfo->MemSort = plan->operatorMemKB[0] - m_insMemInfo->MemInsert;
            m_insMemInfo->spreadNum = 0;
            m_insMemInfo->partitionNum = partitionNum;
            m_delMemInfo->partitionNum = partitionNum;
            m_delMemInfo->MemSort = m_insMemInfo->MemSort;
            MEMCTL_LOG(DEBUG2,
                       "UpdateForCStorePartDelete(init plan):Insert workmem is : %dKB, sort workmem: %dKB,"
                       "parititions totalnum is(%d)can spread maxMem is %dKB.",
                       m_delMemInfo->MemInsert,
                       m_delMemInfo->MemSort,
                       m_delMemInfo->partitionNum,
                       m_delMemInfo->canSpreadmaxMem);
            MEMCTL_LOG(DEBUG2,
                       "UpdateForCStorePartInsert(init plan):Insert workmem is : %dKB, sort workmem: %dKB,"
                       "parititions totalnum is(%d)can spread maxMem is %dKB.",
                       m_insMemInfo->MemInsert,
                       m_insMemInfo->MemSort,
                       m_insMemInfo->partitionNum,
                       m_insMemInfo->canSpreadmaxMem);
        } else {
            m_insMemInfo->canSpreadmaxMem = plan->operatorMaxMem;
            m_insMemInfo->MemInsert =
                static_cast<int>((double)plan->operatorMemKB[0] * (double)(maxbatchRows * BATCHROW_TIMES) /
                                 (double)(maxbatchRows + partialClusterRows));
            m_insMemInfo->MemSort = plan->operatorMemKB[0] - m_insMemInfo->MemInsert;
            m_insMemInfo->spreadNum = 0;
            m_insMemInfo->partitionNum = 1;
            m_delMemInfo->MemSort = m_insMemInfo->MemSort;
            MEMCTL_LOG(DEBUG2,
                       "UpdateForCStoreDelete(init plan):Insert workmem is : %dKB, sort workmem: %dKB,"
                       "parititions totalnum is(%d)can spread maxMem is %dKB.",
                       m_delMemInfo->MemInsert,
                       m_delMemInfo->MemSort,
                       m_delMemInfo->partitionNum,
                       m_delMemInfo->canSpreadmaxMem);
            MEMCTL_LOG(DEBUG2,
                       "UpdateForCStoreInsert(init plan):Insert workmem is : %dKB, sort workmem: %dKB,"
                       "parititions totalnum is(%d)can spread maxMem is %dKB.",
                       m_insMemInfo->MemInsert,
                       m_insMemInfo->MemSort,
                       m_insMemInfo->partitionNum,
                       m_insMemInfo->canSpreadmaxMem);
        }
    } else {
        /*
         * For static load, a single partition of sort Mem is 512MB, and there is no need to subdivide sort Mem.
         * So, set the partitionNum is 1 for all partition table.
         */
        m_delMemInfo->canSpreadmaxMem = 0;
        m_delMemInfo->MemInsert = 0;
        m_delMemInfo->MemSort = u_sess->attr.attr_storage.psort_work_mem;
        m_delMemInfo->spreadNum = 0;
        m_delMemInfo->partitionNum = 1;
        m_insMemInfo->canSpreadmaxMem = 0;
        m_insMemInfo->MemInsert = u_sess->attr.attr_storage.partition_max_cache_size;
        m_insMemInfo->MemSort = u_sess->attr.attr_storage.psort_work_mem;
        m_insMemInfo->spreadNum = 0;
        m_insMemInfo->partitionNum = 1;
    }
}

void CStoreUpdate::InitSortState(TupleDesc sortTupDesc)
{
    Assert(sortTupDesc && m_resultRelInfo && m_delete);

    JunkFilter* junkfilter = m_resultRelInfo->ri_junkFilter;

    // init delete sort state
    m_delete->InitSortState(sortTupDesc, junkfilter->jf_xc_part_id, junkfilter->jf_junkAttNo);
}

uint64 CStoreUpdate::ExecUpdate(_in_ VectorBatch* batch, _in_ int options)
{
    Assert(batch && m_resultRelInfo && m_delete);
    Assert((m_isPartition && m_partionInsert) || (!m_isPartition && m_insert));

    JunkFilter* junkfilter = m_resultRelInfo->ri_junkFilter;

    // delete
    if (!m_hasUniqueIdx) {
        m_delete->PutDeleteBatch(batch, junkfilter);
    }

    int oriCols = batch->m_cols;
    batch->m_cols = junkfilter->jf_cleanTupType->natts;

    // Check the constraints of the batch
    if (m_relation->rd_att->constr)
        ExecVecConstraints(m_resultRelInfo, batch, m_estate);

    // insert then batch
    if (!m_hasUniqueIdx) {
        /*
         * When there has no unique index, the delete threshold(e.g. 4200000)
         * is larger than default insert threshold(e.g. 60000), the relation may
         * firstly insert and then delete.
         */
        if (m_isPartition) {
            m_partionInsert->BatchInsert(batch, options);
        } else {
            m_insert->BatchInsert(batch, options);
        }
    } else {
        /*
         * When there has unique index, we must delete firstly and then insert,
         * or the index key of new data and old data may violate.
        */
        if (m_isPartition) {
            PartitionBatchDeleteAndInsert(batch, oriCols, options, junkfilter);
        } else {
            BatchDeleteAndInsert(batch, oriCols, options, junkfilter);
        }
    }

    batch->m_cols = oriCols;

    return (uint64)(uint32)batch->m_rows;
}

/*
 * @Description: Do batch delete and insert. In this function, the number of rows those are deleted is
 * strictly equals number of rows will be inserted. It is usually used when cstore has unique index.
 */
void CStoreUpdate::BatchDeleteAndInsert(VectorBatch *batch, int oriBatchCols, int options, JunkFilter *junkfilter)
{
    int currentCols = batch->m_cols;
    /* keep memory space from leaking during bulk-insert */
    MemoryContext insertCnxt = m_insert->GetTmpMemCnxt();
    MemoryContext updateCnxt = MemoryContextSwitchTo(insertCnxt);

    CStorePSort* sorter = m_insert->GetSorter();

    if (sorter != NULL) {
        /* Step 1: relation has partial cluster key */
        Assert(batch->m_cols == m_insert->m_relation->rd_att->natts);

        {
            AutoContextSwitch updateContext(updateCnxt);
            batch->m_cols = oriBatchCols;
            m_delete->PutDeleteBatch(batch, junkfilter);
            batch->m_cols = currentCols;
        }

        sorter->PutVecBatch(m_insert->m_relation, batch);

        if (sorter->IsFull()) {
            {
                AutoContextSwitch updateContext(updateCnxt);
                m_delete->PartialDelete();
            }
            m_insert->SortAndInsert(options);
        }
    } else {
        /* Step 2: relation doesn't have partial cluster key */
        bulkload_rows* bufferedBatchRows = m_insert->GetBufferedBatchRows();

        Assert(batch->m_rows <= BatchMaxSize);
        Assert(batch->m_cols && m_insert->m_relation->rd_att->natts);
        Assert(bufferedBatchRows->m_rows_maxnum > 0);
        Assert(bufferedBatchRows->m_rows_maxnum % BatchMaxSize == 0);

        int startIdx = 0;
        int lastStartIdx = startIdx;
        for (;;) {
            /* we need cache data until batchrows is full */
            bool needInsert = bufferedBatchRows->append_one_vector(
                RelationGetDescr(m_relation), batch, &startIdx, m_insert->m_cstorInsertMem);
            if (startIdx > lastStartIdx) {
                AutoContextSwitch updateContext(updateCnxt);
                batch->m_cols = oriBatchCols;
                m_delete->PutDeleteBatchForUpdate(batch, lastStartIdx, startIdx);
                batch->m_cols = currentCols;
            }

            if (needInsert) {
                {
                    AutoContextSwitch updateContext(updateCnxt);
                    m_delete->PartialDelete();
                }
                m_insert->BatchInsertCommon(bufferedBatchRows, options);
                bufferedBatchRows->reset(true);
                lastStartIdx = startIdx;
            } else {
                break;
            }
        }
    }

    MemoryContextReset(insertCnxt);
    (void)MemoryContextSwitchTo(updateCnxt);
}

void CStoreUpdate::PartitionBatchDeleteAndInsert(VectorBatch *batch, int oriBatchCols, int options,
    JunkFilter *junkfilter)
{
    int currentCols = batch->m_cols;
    batch->m_cols = oriBatchCols;

    m_delete->PutDeleteBatch(batch, junkfilter);
    m_delete->PartialDelete();

    batch->m_cols = currentCols;

    m_partionInsert->BatchInsert(batch, options);
}

bool CStoreUpdate::CheckHasUniqueIdx()
{
    bool hasUniqueIdx = false;
    if (m_resultRelInfo && m_resultRelInfo->ri_NumIndices > 0) {
            IndexInfo** indexInfos = m_resultRelInfo->ri_IndexRelationInfo;
            for (int i = 0; i < m_resultRelInfo->ri_NumIndices; ++i) {
                if (indexInfos[i]->ii_Unique) {
                    hasUniqueIdx = true;
                    break;
                }
            }
        }
    return hasUniqueIdx;
}

void CStoreUpdate::EndUpdate(_in_ int options)
{
    Assert(m_delete);
    Assert((m_isPartition && m_partionInsert) || (!m_isPartition && m_insert));

    // end delete
    m_delete->ExecDelete();

    // end insert
    if (m_isPartition) {
        m_partionInsert->EndBatchInsert();
    } else {
        m_insert->SetEndFlag();
        m_insert->BatchInsert((VectorBatch*)NULL, options);
    }
}
