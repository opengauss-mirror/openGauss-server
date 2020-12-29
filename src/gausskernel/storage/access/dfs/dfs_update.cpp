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
 *  dfs_update.cpp
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/access/dfs/dfs_update.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/cstore_insert.h"
#include "access/dfs/dfs_delete.h"
#include "access/dfs/dfs_insert.h"
#include "access/dfs/dfs_update.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"

extern void ExecVecConstraints(ResultRelInfo *resultRelInfo, VectorBatch *batch, EState *estate);

DfsUpdate::DfsUpdate(Relation rel, EState *estate, Plan *plan) : m_estate(estate)
{
    Assert(estate);

    m_relation = rel;
    m_resultRelInfo = estate->es_result_relation_info;
    m_insert = NULL;
    m_dfsUpDelMemInfo = NULL;
    m_dfsUpInsMemInfo = NULL;

    /* init memory, memory info will be used to init delete and insert. */
    InitUpdateMemArg(plan);

    /*
     * initialize for insert
     */
    m_insert = CreateDfsInsert(rel, true, NULL, NULL, m_dfsUpInsMemInfo);
    if (!m_insert) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION), errmodule(MOD_DFS),
                        errmsg("Failed to create DfsInsert handler when updating table.")));
    }

    m_insert->BeginBatchInsert(BATCH_SORT, m_resultRelInfo);

    /*
     * initialize for delete
     */
    m_delete = NULL;
    m_delete = New(CurrentMemoryContext) DfsDelete(rel, estate, true, NULL, m_dfsUpInsMemInfo);
    m_delete->setReportErrorForUpdate(true);
}

DfsUpdate::~DfsUpdate()
{
    this->Destroy();
}

void DfsUpdate::Destroy()
{
    if (m_delete) {
        DELETE_EX(m_delete);
    }

    if (m_insert) {
        DELETE_EX(m_insert);
    }

    if (m_dfsUpDelMemInfo) {
        pfree_ext(m_dfsUpDelMemInfo);
    }

    if (m_dfsUpInsMemInfo) {
        pfree_ext(m_dfsUpInsMemInfo);
    }
}

/*
 * @Description: init update memory info for dfs update.
 *
 *  if isValuePartition, insert mem >2G or (2G-16MB), and sort Mem >=16MB.
 *  if not isValuePartition, insert mem >128MB or (128MB-16MB), and sort Mem >=16MB.
 *  others, Memory management can not be opened.
 * @IN plan: If update operator is directly used, the plan mem_info is used.
 *   		  the mem of delete and insert should be divided caculated.
 * @Return: void
 * @See also: InitUpdateMemArg
 */
void DfsUpdate::InitUpdateMemArg(Plan *plan)
{
    int isValuePartition = RelationIsValuePartitioned(m_relation);
    const int partitionNum = 1;

    m_dfsUpDelMemInfo = (MemInfoArg *)palloc0(sizeof(struct MemInfoArg));
    m_dfsUpInsMemInfo = (MemInfoArg *)palloc0(sizeof(struct MemInfoArg));

    // init mem
    if (plan != NULL && plan->operatorMemKB[0] > 0) {
        m_dfsUpDelMemInfo->canSpreadmaxMem = plan->operatorMaxMem;
        m_dfsUpDelMemInfo->MemInsert = 0;
        m_dfsUpDelMemInfo->spreadNum = 0;
        m_dfsUpDelMemInfo->partitionNum = 1;
        if (isValuePartition) {
            // for partition update. insert mem  must >128MB, max = 2G. the other is used to sort .and sort Mem >= 16MB.
            m_dfsUpInsMemInfo->canSpreadmaxMem = plan->operatorMaxMem;
            m_dfsUpInsMemInfo->MemInsert = plan->operatorMemKB[0] < DFS_MIN_MEM_SIZE ? DFS_MIN_MEM_SIZE
                                                                                        : plan->operatorMemKB[0];
            if (plan->operatorMemKB[0] < DFS_MIN_MEM_SIZE) {
                MEMCTL_LOG(
                    LOG,
                    "DfsUpdate for partition table(init plan) mem is not engough for the basic use: workmem is : %dKB, "
                    "can spread maxMem is %dKB.",
                    plan->operatorMemKB[0], plan->operatorMaxMem);
            }
            if (m_dfsUpInsMemInfo->MemInsert > PARTITION_MAX_SIZE)
                m_dfsUpInsMemInfo->MemInsert = PARTITION_MAX_SIZE;

            m_dfsUpInsMemInfo->MemSort = plan->operatorMemKB[0] - m_dfsUpInsMemInfo->MemInsert;
            if (m_dfsUpInsMemInfo->MemSort < SORT_MIM_MEM) {
                m_dfsUpInsMemInfo->MemSort = SORT_MIM_MEM;
                if (m_dfsUpInsMemInfo->MemInsert >= DFS_MIN_MEM_SIZE)
                    m_dfsUpInsMemInfo->MemInsert = m_dfsUpInsMemInfo->MemInsert - SORT_MIM_MEM;
            }
            m_dfsUpInsMemInfo->spreadNum = 0;
            m_dfsUpInsMemInfo->partitionNum = partitionNum;
            m_dfsUpDelMemInfo->MemSort = m_dfsUpInsMemInfo->MemSort;
            MEMCTL_LOG(DEBUG2,
                       "UpdateForDfsPartDelete(init plan):Insert workmem is : %dKB, sort workmem: %dKB,"
                       "parititions totalnum is(%d)can spread maxMem is %dKB.",
                       m_dfsUpDelMemInfo->MemInsert, m_dfsUpDelMemInfo->MemSort, m_dfsUpDelMemInfo->partitionNum,
                       m_dfsUpDelMemInfo->canSpreadmaxMem);
            MEMCTL_LOG(DEBUG2,
                       "UpdateForDfsPartInsert(init plan):Insert workmem is : %dKB, sort workmem: %dKB,"
                       "parititions totalnum is(%d)can spread maxMem is %dKB.",
                       m_dfsUpInsMemInfo->MemInsert, m_dfsUpInsMemInfo->MemSort, m_dfsUpInsMemInfo->partitionNum,
                       m_dfsUpInsMemInfo->canSpreadmaxMem);
        } else {
            // for usual table update. insert mem = 128MB(128MB-16MB), the other is used to sort . and sort Mem >16MB.
            m_dfsUpInsMemInfo->canSpreadmaxMem = plan->operatorMaxMem;
            m_dfsUpInsMemInfo->MemInsert = plan->operatorMemKB[0] < DFS_MIN_MEM_SIZE ? DFS_MIN_MEM_SIZE
                                                                                        : plan->operatorMemKB[0];
            if (plan->operatorMemKB[0] < DFS_MIN_MEM_SIZE) {
                MEMCTL_LOG(
                    LOG,
                    "DfsUpdate(init plan) mem is not engough for the basic use: workmem is : %dKB, can spread maxMem "
                    "is %dKB.",
                    plan->operatorMemKB[0], plan->operatorMaxMem);
            }
            if (m_dfsUpInsMemInfo->MemInsert > DFS_MIN_MEM_SIZE)
                m_dfsUpInsMemInfo->MemInsert = DFS_MIN_MEM_SIZE;

            m_dfsUpInsMemInfo->MemSort = plan->operatorMemKB[0] - m_dfsUpInsMemInfo->MemInsert;
            if (m_dfsUpInsMemInfo->MemSort < SORT_MIM_MEM) {
                m_dfsUpInsMemInfo->MemSort = SORT_MIM_MEM;
                if (m_dfsUpInsMemInfo->MemInsert >= DFS_MIN_MEM_SIZE)
                    m_dfsUpInsMemInfo->MemInsert = m_dfsUpInsMemInfo->MemInsert - SORT_MIM_MEM;
            }
            m_dfsUpInsMemInfo->spreadNum = 0;
            m_dfsUpInsMemInfo->partitionNum = 1;
            m_dfsUpDelMemInfo->MemSort = m_dfsUpInsMemInfo->MemSort;
            MEMCTL_LOG(DEBUG2,
                       "UpdateForDfsDelete(init plan):Insert workmem is : %dKB, sort workmem: %dKB,"
                       "parititions totalnum is(%d)can spread maxMem is %dKB.",
                       m_dfsUpDelMemInfo->MemInsert, m_dfsUpDelMemInfo->MemSort, m_dfsUpDelMemInfo->partitionNum,
                       m_dfsUpDelMemInfo->canSpreadmaxMem);
            MEMCTL_LOG(DEBUG2,
                       "UpdateForDfsInsert(init plan):Insert workmem is : %dKB, sort workmem: %dKB,"
                       "parititions totalnum is(%d)can spread maxMem is %dKB.",
                       m_dfsUpInsMemInfo->MemInsert, m_dfsUpInsMemInfo->MemSort, m_dfsUpInsMemInfo->partitionNum,
                       m_dfsUpInsMemInfo->canSpreadmaxMem);
        }
    } else {
        /*
         * For static load, a single partition of sort Mem is 512MB, and there is no need to subdivide sort Mem.
         * So, set the partitionNum is 1 for all partition table.
         */
        m_dfsUpDelMemInfo->canSpreadmaxMem = 0;
        m_dfsUpDelMemInfo->MemInsert = 0;
        m_dfsUpDelMemInfo->MemSort = u_sess->attr.attr_storage.psort_work_mem;
        m_dfsUpDelMemInfo->spreadNum = 0;
        m_dfsUpDelMemInfo->partitionNum = 1;
        m_dfsUpInsMemInfo->canSpreadmaxMem = 0;
        m_dfsUpInsMemInfo->MemInsert = u_sess->attr.attr_storage.partition_max_cache_size;
        m_dfsUpInsMemInfo->MemSort = u_sess->attr.attr_storage.psort_work_mem;
        m_dfsUpInsMemInfo->spreadNum = 0;
        m_dfsUpInsMemInfo->partitionNum = 1;
    }
}

/*
 * initialize sort cache with tuple descriptor.
 */
void DfsUpdate::InitSortState(TupleDesc sortTupDesc)
{
    ereport(DEBUG5, (errmodule(MOD_DFS), errmsg("dfs: %s()", __FUNCTION__)));

    Assert(sortTupDesc && m_resultRelInfo && m_delete);

    JunkFilter *junkfilter = m_resultRelInfo->ri_junkFilter;

    /*
     * init delete sort state
     */
    m_delete->InitSortState(sortTupDesc, junkfilter->jf_xc_part_id, junkfilter->jf_junkAttNo);
}

/*
 * do "update" job really, and return the number of updated rows.
 */
uint64 DfsUpdate::ExecUpdate(VectorBatch *batch, int options)
{
    ereport(DEBUG5, (errmodule(MOD_DFS), errmsg("dfs: %s()", __FUNCTION__)));

    Assert(batch && m_resultRelInfo && m_delete);

    JunkFilter *junkfilter = m_resultRelInfo->ri_junkFilter;

    /*
     * put the tid column of the batch into the buffer of DfsDelete
     * the attr NO. of tid column is saved junkfilter, m_delete just get
     * tid column by batch and junkfilter.
     */
    m_delete->PutDeleteBatch(batch, junkfilter);

    /*
     * before insert, m_insert need to know the right number of column
     * that is from junkfilter->jf_cleanTupType->natts;
     */
    int oriCols = batch->m_cols;
    batch->m_cols = junkfilter->jf_cleanTupType->natts;

    /*
     * Check the constraints of the batch
     */
    if (m_relation->rd_att->constr)
        ExecVecConstraints(m_resultRelInfo, batch, m_estate);

    /*
     * insert the batch
     */
    m_insert->BatchInsert(batch, options);

    /*
     * m_cols must be reset to original column number(including tid col),
     * and batch can be release properly with no resource leak.
     */
    batch->m_cols = oriCols;

    return (uint64)((uint32)batch->m_rows);
}

/*
 * called at last step, make sure that data in cache can be deleted and new
 * data can be inserted.
 */
void DfsUpdate::EndUpdate(int options)
{
    ereport(DEBUG1, (errmodule(MOD_DFS), errmsg("dfs: %s()", __FUNCTION__)));

    /*
     * delete row with tids in cache and delete object itself.
     */
    if (m_delete) {
        m_delete->ExecDelete();
    }

    if (m_insert) {
        /*
         * flush updated data in cache
         */
        m_insert->SetEndFlag();
        m_insert->BatchInsert((VectorBatch *)NULL, options);
    }

    Destroy();
}
