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
 * vechashagg.cpp
 *       Routines to handle vector hash aggregae nodes.
 *
 * IDENTIFICATION
 *        Code/src/gausskernel/runtime/vecexecutor/vecnode/vechashagg.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "catalog/pg_aggregate.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "commands/explain.h"
#include "executor/executor.h"
#include "executor/node/nodeAgg.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/cost.h"
#include "optimizer/tlist.h"
#include "parser/parse_agg.h"
#include "parser/parse_coerce.h"
#include "utils/acl.h"
#include "utils/anls_opt.h"
#include "utils/builtins.h"
#include "utils/dynahash.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/syscache.h"
#include "utils/tuplesort.h"
#include "utils/datum.h"
#include "utils/numeric.h"
#include "utils/numeric_gs.h"
#include "vecexecutor/vechashagg.h"
#include "vecexecutor/vechashtable.h"
#include "vecexecutor/vecexecutor.h"
#include "vecexecutor/vecfunc.h"
#include "nodes/execnodes.h"
#include "pgxc/pgxc.h"
#include "utils/int8.h"

/*
 * Define function pointer that pointer to the codegened machine code
 * with respect to jitted_hashing function and jitted_batchagg function.
 */
typedef void (*vechashing_func)(HashAggRunner* tbl, VectorBatch* batch);
typedef void (*vecbatchagg_func)(HashAggRunner* tbl, hashCell** Loc, VectorBatch* batch, int* aggIdx);

extern bool anls_opt_is_on(AnalysisOpt dfx_opt);

/*
 * @Description: constructed function of hash agg. Init information for the hash agg node.
 */
HashAggRunner::HashAggRunner(VecAggState* runtime) : BaseAggRunner(runtime, true)
{
    VecAgg* node = NULL;

    m_rows = 0;
    m_fill_table_rows = 0;
    m_filesource = NULL;
    m_overflowsource = NULL;
    m_strategy = HASH_IN_MEMORY;
    node = (VecAgg*)(runtime->ss.ps.plan);
    m_can_grow = true;
    m_max_hashsize = 0;
    m_grow_threshold = 0;
    m_hashbuild_time = 0.0;
    m_hashagg_time = 0.0;
    m_availmems = 0;
    m_segnum = 0;
    m_spill_times = 0;

    m_totalMem = SET_NODEMEM(node->plan.operatorMemKB[0], node->plan.dop) * 1024L;
    ereport(DEBUG2,
        (errmodule(MOD_VEC_EXECUTOR),
            errmsg("[VecHashAgg(%d)]: operator memory "
                   "uses: %ldKB",
                runtime->ss.ps.plan->plan_node_id,
                m_totalMem / 1024L)));

    /* add one column to store the hash value.*/
    m_cellSize += sizeof(hashVal);

    m_hashseg_max = (MaxAllocSize + 1) / sizeof(hashCell*) / 2;

    m_hashSize = Min(2 * node->numGroups, m_totalMem / m_cellSize);
    BuildHashTable<false, true>(m_hashSize);

    m_statusLog.restore = false;
    m_statusLog.lastIdx = 0;
    m_statusLog.lastCell = NULL;
    m_statusLog.lastSeg = 0;

    m_tmpContext = AllocSetContextCreate(CurrentMemoryContext,
        "TmpHashContext",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);

    m_hashcell_context = AllocSetContextCreate(m_hashContext,
        "HashCellContext",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE,
        STACK_CONTEXT,
        m_totalMem);

    m_tupleCount = m_colWidth = 0;
    if (node->plan.operatorMaxMem > node->plan.operatorMemKB[0])
        m_maxMem = SET_NODEMEM(node->plan.operatorMaxMem, node->plan.dop) * 1024L;
    else
        m_maxMem = 0;
    m_sysBusy = false;
    m_spreadNum = 0;

    BindingFp();

    if (m_runtime->ss.ps.instrument) {
        m_runtime->ss.ps.instrument->sorthashinfo.hashtable_expand_times = 0;
    }
}

void HashAggRunner::BindingFp()
{
    if (m_keySimple) {
        if (((Agg *) m_runtime->ss.ps.plan)->unique_check) {
            m_buildFun = &HashAggRunner::buildAggTbl<true, true>;
        } else {
            m_buildFun = &HashAggRunner::buildAggTbl<true, false>;
        }
    } else {
        if (((Agg *) m_runtime->ss.ps.plan)->unique_check) {
            m_buildFun = &HashAggRunner::buildAggTbl<false, true>;
        } else {
            m_buildFun = &HashAggRunner::buildAggTbl<false, false>;
        }
    }
}

/*
 * @Description: hash agg reset function.
 */
bool HashAggRunner::ResetNecessary(VecAggState* node)
{
    m_statusLog.restore = false;
    m_statusLog.lastIdx = 0;
    m_statusLog.lastCell = NULL;

    VecAgg* aggnode = (VecAgg*)node->ss.ps.plan;

    /*
     * If we do have the hash table, and the subplan does not have any
     * parameter changes, and none of our own parameter changes affect
     * input expressions of the aggregated functions, then we can just
     * rescan the existing hash table, and have not spill to disk;
     * no need to build it again.
     */
    if (m_spillToDisk == false && node->ss.ps.lefttree->chgParam == NULL && aggnode->aggParams == NULL) {
        m_runState = AGG_FETCH;
        return false;
    }

    if (m_spillToDisk) {
        HashAggRunner* vechashTbl = (HashAggRunner*)node->aggRun;
        if (vechashTbl != NULL) {
            vechashTbl->closeFile();
        }
    }

    m_runState = AGG_PREPARE;
    /* Reset context */
    MemoryContextResetAndDeleteChildren(m_hashContext);

    /*
     * MemoryContextResetAndDeleteChildren(m_hashContext)
     * freed the m_hashcell_context, so here should be created once more.
     */
    m_hashcell_context = AllocSetContextCreate(m_hashContext,
        "HashCellContext",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE,
        STACK_CONTEXT,
        m_totalMem);

    m_hashSize = Min(2 * aggnode->numGroups, m_totalMem / m_cellSize);
    BuildHashTable<false, true>(m_hashSize);

    m_rows = 0;
    m_fill_table_rows = 0;
    m_filesource = NULL;
    m_overflowsource = NULL;
    m_availmems = 0;
    m_spillToDisk = false;
    m_finish = false;
    m_strategy = HASH_IN_MEMORY;

    return true;
}

template <bool simple, bool sgltbl>
void HashAggRunner::AllocHashSlot(VectorBatch* batch, int i)
{
    hashCell* cell = NULL;
    hashCell* head_cell = NULL;
    int segs;
    int64 pos;

    if (m_tupleCount >= 0)
        m_tupleCount++;

    switch (m_strategy) {
        case HASH_IN_MEMORY: {
            {
                AutoContextSwitch mem_switch(m_hashcell_context);
                cell = (hashCell*)palloc(m_cellSize);
                if (m_tupleCount >= 0)
                    m_colWidth += m_cols * sizeof(hashVal);
                initCellValue<simple, false>(batch, cell, i);
                /* store the hash value.*/
                cell->m_val[m_cols].val = m_hashVal[i];
                m_Loc[i] = cell;
            }

            /* insert into head.*/
            if (sgltbl) {
                head_cell = m_hashData[0].tbl_data[m_cacheLoc[i]];
                m_hashData[0].tbl_data[m_cacheLoc[i]] = cell;
            } else {
                GetPosbyLoc(m_cacheLoc[i], &segs, &pos);
                head_cell = m_hashData[segs].tbl_data[pos];
                m_hashData[segs].tbl_data[pos] = cell;
            }

            cell->flag.m_next = head_cell;
            JudgeMemoryOverflow("VecHashAgg",
                m_runtime->ss.ps.plan->plan_node_id,
                SET_DOP(m_runtime->ss.ps.plan->dop),
                m_runtime->ss.ps.instrument);
        } break;
        case HASH_IN_DISK: {
            /* spill to disk first time */
            m_Loc[i] = NULL;
            WaitState old_status = pgstat_report_waitstatus(STATE_EXEC_HASHAGG_WRITE_FILE);
            if (unlikely(m_spillToDisk == false)) {
                int file_num = calcFileNum(((VecAgg*)m_runtime->ss.ps.plan)->numGroups);
                ereport(LOG,
                    (errmodule(MOD_VEC_EXECUTOR),
                        errmsg("[VecHashAgg(%d)]: "
                               "first time spill file num: %d.",
                            m_runtime->ss.ps.plan->plan_node_id,
                            file_num)));
                if (m_filesource == NULL)
                    m_filesource = CreateTempFile(m_outerBatch, file_num, &m_runtime->ss.ps);
                else
                    m_filesource->resetVariableMemberIfNecessary(file_num);

                if (m_runtime->ss.ps.instrument) {
                    m_runtime->ss.ps.instrument->sorthashinfo.hash_FileNum = file_num;
                    m_runtime->ss.ps.instrument->sorthashinfo.hash_writefile = true;
                    m_runtime->ss.ps.instrument->sorthashinfo.hash_spillNum = 0;
                }

                pgstat_increase_session_spill();

                m_spillToDisk = true;
                /* Do not grow up hashtable if convert to write file */
                m_can_grow = false;
            }

            /* Compute  the hash for tuple save to disk.*/
            m_filesource->writeBatch(batch, i, DatumGetUInt32(hash_uint32(m_hashVal[i])));
            (void)pgstat_report_waitstatus(old_status);
        } break;
        case HASH_RESPILL: {
            /* respill to disk */
            m_Loc[i] = NULL;
            if (m_overflowsource == NULL) {
                int rows = m_filesource->getCurrentIdxRownum(m_fill_table_rows);
                int file_num = getPower2NextNum(rows / m_fill_table_rows);
                file_num = Max(2, file_num);
                file_num = Min(file_num, HASH_MAX_FILENUMBER);
                int fidx = m_filesource->getCurrentIdx();
                ereport(LOG,
                    (errmodule(MOD_VEC_EXECUTOR),
                        errmsg("[VecHashAgg(%d)]: current "
                               "respill file idx: %d, its file rows: %ld, m_fill_table_rows: %d, all redundant "
                               "rows: %d, respill file num: %d.",
                            m_runtime->ss.ps.plan->plan_node_id,
                            fidx,
                            m_filesource->m_rownum[fidx],
                            m_fill_table_rows,
                            rows,
                            file_num)));
                m_overflowsource = CreateTempFile(m_outerBatch, file_num, &m_runtime->ss.ps);

                m_spill_times++;
                if (m_spill_times == WARNING_SPILL_TIME) {
                    t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->warning |= (1 << WLM_WARN_SPILL_TIMES_LARGE);
                }
                if (m_runtime->ss.ps.instrument) {
                    m_runtime->ss.ps.instrument->sorthashinfo.hash_spillNum++;
                    m_runtime->ss.ps.instrument->sorthashinfo.hash_FileNum += file_num;

                    if (m_spill_times == WARNING_SPILL_TIME) {
                        m_runtime->ss.ps.instrument->warning |= (1 << WLM_WARN_SPILL_TIMES_LARGE);
                    }
                }
            }

            /* Compute  the hash for tuple save to disk.*/
            WaitState old_status = pgstat_report_waitstatus(STATE_EXEC_HASHAGG_WRITE_FILE);
            m_overflowsource->writeBatch(batch, i, DatumGetUInt32(hash_uint32(m_hashVal[i])));
            (void)pgstat_report_waitstatus(old_status);
        } break;
        default:
            ereport(ERROR,
                (errcode(ERRCODE_UNEXPECTED_NODE_STATE),
                    errmodule(MOD_VEC_EXECUTOR),
                    errmsg("Unexpected vector hash aggregation status")));
            break;
    }
}

/*
 * @Description: calculate hash size, build hash table.
 * @in old_size - initial hash size.
 */
template <bool expand, bool logit>
void HashAggRunner::BuildHashTable(int64 old_size)
{
    int table_size = 0;
    m_hashSize = computeHashTableSize<expand, logit>(old_size);

    if (m_hashSize > m_hashseg_max) {
        m_segnum = m_hashSize / m_hashseg_max;
        table_size = m_hashseg_max;
    } else {
        m_segnum = 1;
        table_size = m_hashSize;
    }

    if (logit)
        ereport(DEBUG2,
            (errmodule(MOD_VEC_EXECUTOR),
                errmsg("[VecHashAgg(%d)]:segment_number:%d, hash_size:%ld, maxhashseg:%d",
                    m_runtime->ss.ps.plan->plan_node_id,
                    m_segnum,
                    m_hashSize,
                    m_hashseg_max)));

    /* Hash table memory in hashBufContext*/
    {
        MemoryContext oldContext = MemoryContextSwitchTo(m_hashContext);
        m_hashData = (HashSegTbl*)palloc0(m_segnum * sizeof(HashSegTbl));

        for (int j = 0; j < m_segnum; j++) {
            m_hashData[j].tbl_data = (hashCell**)palloc0(table_size * sizeof(hashCell*));
            m_hashData[j].tbl_size = table_size;
        }

        (void)MemoryContextSwitchTo(oldContext);
    }
}

/*
 * @Description: get batch from lefttree or temp file and insert into hash table.
 */
void HashAggRunner::Build()
{
    VectorBatch* outer_batch = NULL;

    WaitState old_status = pgstat_report_waitstatus(STATE_EXEC_HASHAGG_BUILD_HASH);
    for (;;) {
        outer_batch = m_hashSource->getBatch();
        if (unlikely(BatchIsNull(outer_batch)))
            break;
        (this->*m_buildFun)(outer_batch);
    }
    (void)pgstat_report_waitstatus(old_status);

    if (HAS_INSTR(&m_runtime->ss, false)) {
        if (m_tupleCount > 0)
            m_runtime->ss.ps.instrument->width = (int)(m_colWidth / m_tupleCount);
        else
            m_runtime->ss.ps.instrument->width = (int)m_colWidth;
        m_runtime->ss.ps.instrument->spreadNum = m_spreadNum;
        m_runtime->ss.ps.instrument->sysBusy = m_sysBusy;
        m_runtime->ss.ps.instrument->sorthashinfo.hashagg_time = m_hashagg_time;
        m_runtime->ss.ps.instrument->sorthashinfo.hashbuild_time = m_hashbuild_time;
    }

    m_finish = true;
}

/*
 * @Description: get data from hash table.
 * @return - result of group and agg.
 */
VectorBatch* HashAggRunner::Probe()
{
    int last_idx = 0;
    int last_seg = 0;
    hashCell* cell = NULL;
    VectorBatch* p_res = NULL;
    int i = 0;
    int j;

    m_scanBatch->Reset();

    ResetExprContext(m_runtime->ss.ps.ps_ExprContext);

    while (BatchIsNull(m_scanBatch)) {
        if (m_statusLog.restore) {
            last_idx = m_statusLog.lastIdx;
            cell = m_statusLog.lastCell;
            m_statusLog.restore = false;
            last_seg = m_statusLog.lastSeg;
        }

        for (j = last_seg; j < m_segnum; j++) {
            for (i = last_idx; i < m_hashData[j].tbl_size; i++) {
                if (cell == NULL)
                    cell = m_hashData[j].tbl_data[i];

                while (cell != NULL) {
                    InvokeFp(m_buildScanBatch)(cell);
                    if (m_scanBatch->m_rows == BatchMaxSize) {
                        m_statusLog.restore = true;
                        cell = cell->flag.m_next;
                        if (cell != NULL) {
                            m_statusLog.lastCell = cell;
                            m_statusLog.lastIdx = i;
                            m_statusLog.lastSeg = j;
                        } else {
                            m_statusLog.lastCell = NULL;
                            m_statusLog.lastIdx = i + 1;
                            m_statusLog.lastSeg = j;
                        }

                        goto PRODUCE_BATCH;
                    }

                    cell = cell->flag.m_next;
                }
            }
            m_statusLog.lastIdx = 0;
            last_idx = 0;
        }

    PRODUCE_BATCH:
        if (j == m_segnum) {
            if (m_scanBatch->m_rows > 0) {
                p_res = ProducerBatch();
                m_statusLog.restore = true;
                m_statusLog.lastCell = NULL;
                m_statusLog.lastIdx = i + 1;
                m_statusLog.lastSeg = j;
            } else
                return NULL;  // all end;
        } else {
            if (m_scanBatch->m_rows > 0) {
                p_res = ProducerBatch();
                if (unlikely(BatchIsNull(p_res))) {
                    m_scanBatch->Reset();
                    continue;
                }
            }
        }
    }

    return p_res;
}

/*
 * @Description: hash agg entrance function.
 */
VectorBatch* HashAggRunner::Run()
{
    VectorBatch* p_res = NULL;

    while (true) {
        switch (m_runState) {
            /* Get data source, lefttree or temp file.*/
            case AGG_PREPARE:
                m_hashSource = GetHashSource();
                /* no source, so it is the end. */
                if (m_hashSource == NULL)
                    return NULL;
                m_runState = AGG_BUILD;
                break;
            /* Get data and insert into hash table.*/
            case AGG_BUILD: {
                Build();

                bool can_wlm_warning_statistics = false;
                /* print the hash table information when needed */
                if (anls_opt_is_on(ANLS_HASH_CONFLICT)) {
                    char stats[MAX_LOG_LEN];
                    Profile(stats, &can_wlm_warning_statistics);

                    if (m_spillToDisk == false)
                        ereport(LOG,
                            (errmodule(MOD_VEC_EXECUTOR),
                                errmsg("[VecHashAgg(%d)] %s", m_runtime->ss.ps.plan->plan_node_id, stats)));
                    else
                        ereport(LOG,
                            (errmodule(MOD_VEC_EXECUTOR),
                                errmsg("[VecHashAgg(%d)(temp file:%d)] %s",
                                    m_runtime->ss.ps.plan->plan_node_id,
                                    m_filesource->getCurrentIdx(),
                                    stats)));
                } else if (u_sess->attr.attr_resource.resource_track_level >= RESOURCE_TRACK_QUERY &&
                           u_sess->attr.attr_resource.enable_resource_track && u_sess->exec_cxt.need_track_resource) {
                    char stats[MAX_LOG_LEN];
                    Profile(stats, &can_wlm_warning_statistics);
                }
                if (can_wlm_warning_statistics) {
                    pgstat_add_warning_hash_conflict();
                    if (m_runtime->ss.ps.instrument) {
                        m_runtime->ss.ps.instrument->warning |= (1 << WLM_WARN_HASH_CONFLICT);
                    }
                }

                if (!m_spillToDisk) {
                    /* Early free left tree after hash table built */
                    ExecEarlyFree(outerPlanState(m_runtime));

                    EARLY_FREE_LOG(elog(LOG,
                        "Early Free: Hash Table for Agg"
                        " is built at node %d, memory used %d MB.",
                        (m_runtime->ss.ps.plan)->plan_node_id,
                        getSessionMemoryUsageMB()));
                }
                m_runState = AGG_FETCH;
            } break;

            /* Fetch data from hash table and return result.*/
            case AGG_FETCH:
                p_res = Probe();
                if (BatchIsNull(p_res)) {
                    if (m_spillToDisk == true) {
                        m_strategy = HASH_IN_DISK;
                        m_runState = AGG_PREPARE;
                    } else {
                        return NULL;
                    }
                } else
                    return p_res;
                /* fall through */
            default:
                break;
        }
    }
}

/*
 * @Description: get data source. The first is lefttree, or temp file if has write temp file.
 */
hashSource* HashAggRunner::GetHashSource()
{
    hashSource* ps = NULL;
    switch (m_strategy) {
        case HASH_IN_MEMORY: {
            ps = New(CurrentMemoryContext) hashOpSource(outerPlanState(m_runtime));
        } break;

        case HASH_IN_DISK: {
            /* m_spillToDisk is false means there is no data in disk */
            if (!m_spillToDisk)
                return NULL;
            m_filesource->close(m_filesource->getCurrentIdx());
            if (m_filesource->next()) {
                int file_idx = m_filesource->getCurrentIdx();
                ps = m_filesource;
                m_filesource->rewind(file_idx);

                /* reset the rows in the hash table */
                m_rows = 0;

                /* just reset context to free the memory.*/
                MemoryContextResetAndDeleteChildren(m_hashContext);

                /*
                 * MemoryContextResetAndDeleteChildren(m_hashContext)
                 * freed the m_hashcell_context, so here should be created once more.
                 */
                m_hashcell_context = AllocSetContextCreate(m_hashContext,
                    "HashCellStackContext",
                    ALLOCSET_DEFAULT_MINSIZE,
                    ALLOCSET_DEFAULT_INITSIZE,
                    ALLOCSET_DEFAULT_MAXSIZE,
                    STACK_CONTEXT,
                    m_totalMem);

                m_hashSize = Min(2 * m_filesource->m_rownum[file_idx], m_availmems / m_cellSize);
                BuildHashTable<false, false>(m_hashSize);
                MEMCTL_LOG(DEBUG2,
                    "[VecHashAgg(%d)(temp file %d)]: "
                    "current file rows:%ld, new hash table size:%ld, availMem :%ld, cellSize :%d.",
                    m_runtime->ss.ps.plan->plan_node_id,
                    file_idx,
                    m_filesource->m_rownum[file_idx],
                    m_hashSize,
                    m_availmems,
                    m_cellSize);

                MemoryContextReset(m_filesource->m_context);

                m_statusLog.restore = false;
                m_statusLog.lastIdx = 0;
                m_statusLog.lastCell = NULL;

                // get the right strategy
                m_strategy = HASH_IN_MEMORY;
            } else {
                pfree_ext(m_filesource);
                m_filesource = NULL;
                if (m_overflowsource == NULL)
                    return NULL;
                else {
                    // switch to the overflow hash file source
                    m_filesource = m_overflowsource;
                    m_overflowsource = NULL;
                    return GetHashSource();
                }
            }
        } break;

        default:
            ereport(ERROR,
                (errcode(ERRCODE_UNEXPECTED_NODE_STATE),
                    errmodule(MOD_VEC_EXECUTOR),
                    errmsg("Unexpected vector hashagg status")));
            break;
    }

    return ps;
}

/*
 * @Description: get hash location by hash value.
 * @in idx - hash value
 * @in segs -  segment number
 * @in pos - hash location
 */
FORCE_INLINE
void HashAggRunner::GetPosbyLoc(uint64 idx, int* segs, int64* pos)
{
    Assert(idx < (uint64)m_hashSize);

    *segs = idx / m_hashseg_max;
    *pos = idx % m_hashseg_max;
    Assert(*segs < m_segnum);
}

/*
 * @Description: calculate hash value, keep data to hash table compute agg,
 * or keep to disk if size of use memory exceed work_mem.
 * @in batch - current batch.
 */
template <bool simple, bool unique_check>
void HashAggRunner::buildAggTbl(VectorBatch* batch)
{
    int i;
    int rows;
    hashCell* cell = NULL;
    int mask;
    bool found_match = false;
    instr_time start_time;
    int segs;
    int64 pos;

    INSTR_TIME_SET_CURRENT(start_time);

    /*
     * mark if buildAggTbl has been codegened or not
     */
    if ((m_runtime->jitted_hashing || m_runtime->jitted_sglhashing) || m_runtime->jitted_batchagg) {
        if (HAS_INSTR(&m_runtime->ss, false)) {
            m_runtime->ss.ps.instrument->isLlvmOpt = true;
        }
    }

    /*
     * Do the grow check for each batch, to avoid doing the check inside the for loop.
     * This also lets us avoid having to re-find hash position in the hashtable after
     * resizing.
     */
    if (m_maxMem > 0)
        (void)computeHashTableSize<false, false>(m_hashSize);
    if (m_can_grow && m_rows >= m_grow_threshold) {
        /*
         * Judge memory is enough for hashtable growing up.
         */
        if (m_maxMem > 0 || JudgeMemoryAllowExpand()) {
            HashTableGrowUp();
        } else {
            m_can_grow = false;
        }
    }

    /*
     * Here may be load hashtable that had resized.
     */
    mask = m_hashSize - 1;

    /*
     * If hashing part has been codegened, call the machine code
     */
    if (m_segnum == 1 && m_runtime->jitted_sglhashing != NULL) {
        ereport(DEBUG2,
            (errmodule(MOD_VEC_EXECUTOR),
                errmsg("[VecHashAgg(%d)]: AggHashing with one segment!", m_runtime->ss.ps.plan->plan_node_id)));
        ((vechashing_func)(m_runtime->jitted_sglhashing))(this, batch);
    } else if (m_runtime->jitted_hashing != NULL) {
        ereport(DEBUG2,
            (errmodule(MOD_VEC_EXECUTOR),
                errmsg(
                    "[VecHashAgg(%d)]: AggHashing with %d segments!", m_runtime->ss.ps.plan->plan_node_id, m_segnum)));
        ((vechashing_func)(m_runtime->jitted_hashing))(this, batch);
    } else {
        rows = batch->m_rows;
        hashBatch(batch, m_keyIdx, m_hashVal, m_outerHashFuncs);
        for (i = 0; i < rows; i++) {
            m_cacheLoc[i] = m_hashVal[i] & (unsigned int)mask;
        }

        for (i = 0; i < rows; i++) {
            if (m_segnum == 1) {
                cell = m_hashData[0].tbl_data[m_cacheLoc[i]];
            } else {
                GetPosbyLoc(m_cacheLoc[i], &segs, &pos);
                cell = m_hashData[segs].tbl_data[pos];
            }

            found_match = false;

            while (cell != NULL) {
                /* First compare hash value.*/
                if (m_hashVal[i] == cell->m_val[m_cols].val && match_key<simple>(batch, i, cell)) {
                    m_Loc[i] = cell;
                    found_match = true;
                    break;
                }

                cell = cell->flag.m_next;
            }

            if (found_match == false) {
                if (m_segnum == 1)
                    AllocHashSlot<simple, true>(batch, i);
                else
                    AllocHashSlot<simple, false>(batch, i);
            } else if (unique_check) {
                ereport(ERROR,
                        (errcode(ERRCODE_CARDINALITY_VIOLATION),
                         errmsg("more than one row returned by a subquery used as an expression")));
            }
        }
    }

    m_hashbuild_time += elapsed_time(&start_time);
    INSTR_TIME_SET_CURRENT(start_time);
    if (m_runtime->jitted_batchagg)
        ((vecbatchagg_func)(m_runtime->jitted_batchagg))(this, m_Loc, batch, m_aggIdx);
    else
        BatchAggregation(batch);
    m_hashagg_time += elapsed_time(&start_time);

    /* we can reset the memory safely per batch line*/
    if (m_spillToDisk)
        MemoryContextReset(m_filesource->m_context);
}

/*
 * @Description: get hash value from hashtable
 * @in hashentry - hashtable element
 * @return - hash value
 */
ScalarValue HashAggRunner::getHashValue(hashCell* hashentry)
{
    return hashentry->m_val[m_cols].val;
}

/*
 * @Description	: Analyze current hash table to show the statistics information of hash chains,
 *				  including hash table size, invalid number of hash chains, distribution of the
 *				  length of hash chains.
 * @in stats		: The string used to record all the hash table information.
 */
void HashAggRunner::Profile(char* stats, bool* can_wlm_warning_statistics)
{
    int fill_rows = 0;
    int single_num = 0;
    int double_num = 0;
    int conflict_num = 0;
    int total_num = 0;
    int hash_size = m_hashSize;
    int chain_len = 0;
    int max_chain_len = 0;

    hashCell* cell = NULL;
    for (int j = 0; j < m_segnum; j++) {
        for (int i = 0; i < m_hashData[j].tbl_size; i++) {
            cell = m_hashData[j].tbl_data[i];

            /* record each hash chain's length and accumulate hash element */
            chain_len = 0;
            while (cell != NULL) {
                fill_rows++;
                chain_len++;
                cell = cell->flag.m_next;
            }

            /* record the number of hash chains with length equal to 1 */
            if (chain_len == 1)
                single_num++;

            /* record the number of hash chains with length equal to 2 */
            if (chain_len == 2)
                double_num++;

            /* mark if the length of hash chain is greater than 3, we meet hash confilct */
            if (chain_len >= 3)
                conflict_num++;

            /* record the length of the max hash chain  */
            if (chain_len > max_chain_len)
                max_chain_len = chain_len;

            if (chain_len != 0)
                total_num++;
        }
    }

    /* print the information */
    int rc = sprintf_s(stats,
        MAX_LOG_LEN,
        "Hash Table Profiling: table size: %d,"
        " hash elements: %d, table fill ratio %.2f, max hash chain len: %d,"
        " %d chains have length 1, %d chains have length 2, %d chains have conficts "
        "with length >= 3.",
        hash_size,
        fill_rows,
        (double)fill_rows / hash_size,
        max_chain_len,
        single_num,
        double_num,
        conflict_num);
    securec_check_ss(rc, "", "");

    if (max_chain_len >= WARNING_HASH_CONFLICT_LEN || (total_num != 0 && conflict_num >= total_num / 2)) {
        *can_wlm_warning_statistics = true;
    }
}

/*
 * @Description: Compute sizing parameters for hashtable. Called when creating and growing
 * the hashtable.
 * @in old_size - cost group size for expand is false, old hashtable size for expand is true.
 * @return - new size for building hashtable
 */
template <bool expand, bool logit>
int64 HashAggRunner::computeHashTableSize(int64 old_size)
{
    int64 hash_size;
    int64 mppow2;

    if (expand == false) {
        /* supporting zero sized hashes would complicate matters */
        hash_size = Max(old_size, MIN_HASH_TABLE_SIZE);

        /* round up size to the next power of 2, that's the bucketing works  */
        hash_size = 1UL << (unsigned int)my_log2(hash_size);

        /* max table size that memory allowed */
        m_max_hashsize = m_totalMem / sizeof(hashCell*);

        /* If max_pointers isn't a power of 2, must round it down to one */
        mppow2 = 1UL << (unsigned int)my_log2(m_max_hashsize);
        if (m_max_hashsize != mppow2) {
            m_max_hashsize = mppow2 / 2;
        }

        if (hash_size * HASH_EXPAND_SIZE > m_max_hashsize) {
            m_can_grow = false;
        } else {
            m_can_grow = true;
        }

        if (logit)
            ereport(DEBUG2,
                (errmodule(MOD_VEC_EXECUTOR),
                    errmsg("[VecHashAgg(%d)]: memory allowed max table size is %ld",
                        m_runtime->ss.ps.plan->plan_node_id,
                        m_max_hashsize)));
    } else {
        Assert(old_size * HASH_EXPAND_SIZE <= m_max_hashsize);
        hash_size = old_size * HASH_EXPAND_SIZE;

        if (hash_size * HASH_EXPAND_SIZE > m_max_hashsize) {
            m_can_grow = false;
        } else {
            m_can_grow = true;
        }
    }

    m_grow_threshold = hash_size * HASH_EXPAND_THRESHOLD;

    if (logit)
        ereport(DEBUG2, (errmodule(MOD_VEC_EXECUTOR), errmsg("Hashagg table size is %ld", hash_size)));
    return hash_size;
}

/*
 * @Description: Grow up the hash table to a new size
 * @return - void
 */
void HashAggRunner::HashTableGrowUp()
{
    int old_seg = m_segnum;
    int32 i, j;
    instr_time start_time;
    double total_time = 0;
    HashSegTbl* old_data = m_hashData;

    INSTR_TIME_SET_CURRENT(start_time);

    /*
     * When optimizing, it can be very useful to print these out.
     */
    ereport(DEBUG2,
        (errmodule(MOD_VEC_EXECUTOR),
            errmsg("[VecHashAgg(%d)]: hash table rows is %ld", m_runtime->ss.ps.plan->plan_node_id, m_rows)));

    BuildHashTable<true, true>(m_hashSize);

    /*
     * Copy cells from the old data to newdata.
     * We neither increase the data rows, nor need to compare keys.
     * We just calculate new hash bucket and  append old cell to new cells.	Then free old data.
     */
    for (j = 0; j < old_seg; j++) {
        int old_size = old_data[j].tbl_size;
        hashCell** tmp_data = old_data[j].tbl_data;
        for (i = 0; i < old_size; i++) {
            hashCell* old_cell = tmp_data[i];
            hashCell* next_cell = NULL;

            while (old_cell != NULL) {
                uint32 hash;
                int32 start_elem;
                hashCell* new_cell = NULL;
                int segs;
                int64 pos;

                next_cell = old_cell->flag.m_next;
                old_cell->flag.m_next = NULL;

                hash = getHashValue(old_cell);
                start_elem = get_bucket(hash);

                if (m_segnum == 1) {
                    new_cell = m_hashData[0].tbl_data[start_elem];
                    if (new_cell == NULL) {
                        m_hashData[0].tbl_data[start_elem] = old_cell;
                    } else {
                        old_cell->flag.m_next = new_cell;
                        m_hashData[0].tbl_data[start_elem] = old_cell;
                    }
                } else {
                    GetPosbyLoc(start_elem, &segs, &pos);

                    new_cell = m_hashData[segs].tbl_data[pos];

                    if (new_cell == NULL) {
                        m_hashData[segs].tbl_data[pos] = old_cell;
                    } else {
                        old_cell->flag.m_next = new_cell;
                        m_hashData[segs].tbl_data[pos] = old_cell;
                    }
                }

                old_cell = next_cell;
                CHECK_FOR_INTERRUPTS();
            }
        }
    }

    total_time = elapsed_time(&start_time);

    /* free the old hashtable */
    for (j = 0; j < old_seg; j++) {
        pfree_ext(old_data[j].tbl_data);
    }

    pfree_ext(old_data);

    /* print hashtable grow up time */
    ereport(DEBUG2,
        (errmodule(MOD_VEC_EXECUTOR),
            errmsg("[VecHashAgg(%d)]: hash table grow up time is %.3f ms",
                m_runtime->ss.ps.plan->plan_node_id,
                total_time * 1000)));

    if (m_runtime->ss.ps.instrument) {
        m_runtime->ss.ps.instrument->sorthashinfo.hashtable_expand_times++;
    }
}
