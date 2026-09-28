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
 * Transfer/internal function OIDs for aggregation operations.
 * These OIDs are from pg_proc but lack macro definitions.
 */
#define INT4_SUM_TRANSFUNC_OID      1841    /* int4_sum transfer function */
#define INT8_SUM_TRANSFUNC_OID      1842    /* int8_sum transfer function */
#define NUMERIC_ADD_TRANSFUNC_OID   1724    /* numeric_add transfer function */
#define COUNT_INT4_FUNCOID          2804    /* count(int4) -> int8 */
#define INT8INC_FUNCOID             1219    /* int8inc count transfer function */
#define INT8_AVG_ACCUM_FUNCOID      2746    /* int8_avg_accum transfer function */
#define INT4_AVG_ACCUM_FUNCOID      1963    /* int4_avg_accum transfer function */
#define INT2_AVG_ACCUM_FUNCOID      1962    /* int2_avg_accum transfer function */
#define NUMERIC_AVG_ACCUM_FUNCOID   2858    /* numeric_avg_accum transfer function */
#define INT1_AVG_ACCUM_FUNCOID      5548    /* int1_avg_accum transfer function */

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
    m_totalMem += GetAvailRackMemory(node->plan.dop) * 1024L;
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
        EnableBorrowWorkMemory() ? RACK_CONTEXT : STACK_CONTEXT,
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

    m_dpaContext = AllocSetContextCreate(CurrentMemoryContext,
        "DpaSessionContext",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);
    use_dpa = ENABLE_UADK_AGG && u_sess->attr.attr_sql.enable_dpa_hashagg;
}

HashAggRunner::~HashAggRunner()
{
    DaeSessionCleanup();
    
    if (m_dpaContext != nullptr) {
        MemoryContextDelete(m_dpaContext);
        m_dpaContext = nullptr;
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

    /* Clean up DPA session so Build() will re-initialize from scratch */
    if (use_dpa && DaeIsSessionReady()) {
        DaeSessionCleanup();
    }

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
        EnableBorrowWorkMemory() ? RACK_CONTEXT : STACK_CONTEXT,
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
                m_runtime->ss.ps.instrument,
                true);
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

	// first batch
    if (use_dpa && !DaeIsSessionReady()) {
        outer_batch = m_hashSource->getBatch();
        if (unlikely(BatchIsNull(outer_batch))) {
            use_dpa = false;
            (void)pgstat_report_waitstatus(old_status);
            return;
        }
        
        /*
         * m_aggNum == 0: VecHashAgg node used only for dedup/distinct
         * (e.g. the EXISTS semi-join processing in Q4).  DPA handles
         * aggregation, not dedup; skip it silently at DEBUG1 level so
         * we don't emit a spurious WARNING for every EXISTS query.
         */
        if (m_aggNum == 0) {
            ereport(DEBUG1, (errmsg("DPA: No aggregation functions (dedup node), skipping DPA")));
            use_dpa = false;
            (this->*m_buildFun)(outer_batch);
        } else if (!DaeSessionInit(outer_batch)) {
            ereport(WARNING,
                (errmsg("DPA: Failed to initialize DAE session, fallback to CPU aggregation")));
            use_dpa = false;
            (this->*m_buildFun)(outer_batch);
        } else {
            DaeParallelBuild(outer_batch);
        }
    }
    
    for (;;) {
        outer_batch = m_hashSource->getBatch();
        if (unlikely(BatchIsNull(outer_batch)))
            break;
        
        if (use_dpa) {
            DaeParallelBuild(outer_batch);
        } else {
            (this->*m_buildFun)(outer_batch);
        }
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

    if (use_dpa) {
        DaeParallelProbe();
        while (m_scanBatch->m_rows > 0) {
            p_res = ProducerBatch();
            if (p_res != NULL) {
                return p_res;
            }
            m_scanBatch->Reset();
            DaeParallelProbe();
        }
        return NULL;
    }

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
                    EnableBorrowWorkMemory() ? RACK_CONTEXT : STACK_CONTEXT,
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

// Common constants for DPA numeric precision handling
static const int DPA_NUMERIC_PRECISION_SHIFT = 16;
static const int DPA_NUMERIC_SCALE_MASK = 0xffff;
// AVG aggregation produces 2 outputs (COUNT and SUM)
static const int DPA_AVG_ALG_NUM = 2;

static void DaeCalcHTBLRows(struct wd_dae_hash_table *table, struct wd_dae_hash_table *newTable,
    __u32 estimated_groups)
{
    const __u32 HASH_TABLE_GROWTH_FACTOR = 2;

    if (table) {
        newTable->std_table_row_num = EXT_RATIO * table->std_table_row_num;
        newTable->ext_table_row_num = EXT_RATIO * table->ext_table_row_num;
    } else if (estimated_groups > 0) {
        __u32 required_rows = (__u32)(estimated_groups * HASH_TABLE_LOAD_FACTOR);
        __u32 target_rows = HASH_TABLE_ROW_NUM;
        while (target_rows < required_rows && target_rows < HASH_TABLE_ROW_NUM_MAX) {
            target_rows *= HASH_TABLE_GROWTH_FACTOR;
        }
        newTable->std_table_row_num = target_rows;
        newTable->ext_table_row_num = target_rows;
        ereport(DEBUG1,
            (errmsg("DPA: Hash table sized based on estimated groups: %u -> %u rows (load factor %.1f)",
                    estimated_groups, target_rows, HASH_TABLE_LOAD_FACTOR)));
    } else {
        newTable->std_table_row_num = HASH_TABLE_ROW_NUM;
        newTable->ext_table_row_num = HASH_TABLE_ROW_NUM;
    }
}

static bool DaeCapHTBLRows(struct wd_dae_hash_table *newTable, __u32 row_size)
{
    const __u32 MIN_EXT_ROWS = 1;

    if (row_size == 0) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("DPA: row_size cannot be zero in DaeAllocHTBL")));
        return false;
    }
    __u32 ext_extra_rows = (row_size == EXT_ROW_SIZE1) ? (__u32)EXT_ROW_BYTES1 :
                           (row_size == EXT_ROW_SIZE2) ? (__u32)EXT_ROW_BYTES2 : 0;
    __u32 max_std_rows = (__u32)(MaxAllocSize / row_size);
    __u32 max_ext_rows = (max_std_rows > ext_extra_rows) ? (max_std_rows - ext_extra_rows) : MIN_EXT_ROWS;
    
    if (newTable->std_table_row_num > max_std_rows) {
        ereport(DEBUG1, (errmsg("DPA: capping std_table_row_num from %u to %u (MaxAllocSize constraint)",
                                newTable->std_table_row_num, max_std_rows)));
        newTable->std_table_row_num = max_std_rows;
    }
    if (newTable->ext_table_row_num > max_ext_rows) {
        ereport(DEBUG1, (errmsg("DPA: capping ext_table_row_num from %u to %u (MaxAllocSize constraint)",
                                newTable->ext_table_row_num, max_ext_rows)));
        newTable->ext_table_row_num = max_ext_rows;
    }
    return true;
}

void HashAggRunner::DaeAllocHTBL(struct wd_dae_hash_table *table_, struct wd_dae_hash_table *new_table,
	__u32 row_size, __u32 estimated_groups)
{
    DaeCalcHTBLRows(table_, new_table, estimated_groups);
    new_table->table_row_size = row_size;

    if (!DaeCapHTBLRows(new_table, row_size)) {
        return;
    }

	/* Use DPA context for DPA hash table memory */
    MemoryContext oldContext = MemoryContextSwitchTo(m_dpaContext);
    new_table->std_table = palloc(row_size * new_table->std_table_row_num);
    
    __u64 size;
    if (row_size == EXT_ROW_SIZE1) {
        size = (__u64)row_size * (new_table->ext_table_row_num + EXT_ROW_BYTES1);
    } else if (row_size == EXT_ROW_SIZE2) {
        size = (__u64)row_size * (new_table->ext_table_row_num + EXT_ROW_BYTES2);
    } else {
        size = (__u64)row_size * new_table->ext_table_row_num;
    }

    new_table->ext_table = palloc(size);
    if (row_size == EXT_ROW_SIZE1) {
        new_table->ext_table += EXT_ROW_SIZE1 * EXT_ROW_BYTES1;
    } else if (row_size == EXT_ROW_SIZE2) {
        new_table->ext_table += EXT_ROW_SIZE2 * EXT_ROW_BYTES2;
    }
    new_table->table_row_size = row_size;
    (void)MemoryContextSwitchTo(oldContext);
}

static wd_dae_data_type MapOidToDAEType(Oid oid)
{
    switch (oid) {
        case INT4OID:
            return WD_DAE_INT;
        case INT8OID:
            return WD_DAE_LONG;
        case BPCHAROID:
            return WD_DAE_CHAR;
        case VARCHAROID:
            return WD_DAE_VARCHAR;
        case NUMERICOID:
            return WD_DAE_LONG_DECIMAL;
        case DATEOID:
            return WD_DAE_DATE;
        case TIMESTAMPOID:
            return WD_DAE_LONG;  // Timestamp stored as int64
        default:
            return WD_DAE_DATA_TYPE_MAX;
    }
}

static wd_agg_alg MapAggFuncOidToAlg(Oid aggFuncOid, bool *isAvg = nullptr)
{
    // Initialize isAvg to false by default
    if (isAvg != nullptr) {
        *isAvg = false;
    }
    
    switch (aggFuncOid) {
        case INT4SUMFUNCOID:
        case INT4_SUM_TRANSFUNC_OID:
        case INT8_SUM_TRANSFUNC_OID:
        case NUMERICSUMFUNCOID:
        case NUMERIC_ADD_TRANSFUNC_OID:
            return WD_AGG_SUM;
        case ANYCOUNTOID:
        case COUNT_INT4_FUNCOID:
        case COUNTOID:
        case INT8INC_FUNCOID:
            return WD_AGG_COUNT;
        case INT8AVGFUNCOID:
        case INT4AVGFUNCOID:
        case INT2AVGFUNCOID:
        case NUMERICAVGFUNCOID:
        case INT1AVGFUNCOID:
        case INT8_AVG_ACCUM_FUNCOID:
        case INT4_AVG_ACCUM_FUNCOID:
        case INT2_AVG_ACCUM_FUNCOID:
        case NUMERIC_AVG_ACCUM_FUNCOID:
        case INT1_AVG_ACCUM_FUNCOID:
            if (isAvg != nullptr) {
                *isAvg = true;
            }
            return WD_AGG_SUM;
        default:
            return WD_AGG_ALG_TYPE_MAX;
    }
}

static bool DaeSetupBpcharKeyType(struct wd_key_col_info *colInfo, int4 typeMod)
{
    colInfo->input_data_type = WD_DAE_CHAR;
    
    if (typeMod <= (int32)VARHDRSZ) {
        ereport(WARNING, (errmsg("DPA: Invalid BPCHAR typeMod: %d", typeMod)));
        return false;
    }
    
    int charLen = typeMod - VARHDRSZ;
    if (charLen > DPA_MAX_CHAR_SIZE) {
        ereport(WARNING,
            (errmsg("DPA: CHAR(%d) length exceeds hardware limit %d",
                    charLen, DPA_MAX_CHAR_SIZE)));
        return false;
    }
    
    colInfo->col_data_info = charLen;
    return true;
}

static bool DaeCheckNumericPrecision(int4 typeMod, uint16 *precision, uint16 *scale, const char *context)
{
    if (typeMod >= (int32)VARHDRSZ) {
        int32 tmp_typmod = typeMod - VARHDRSZ;
        *precision = (uint16)((tmp_typmod >> DPA_NUMERIC_PRECISION_SHIFT) & DPA_NUMERIC_SCALE_MASK);
        if (*precision > DPA_MAX_PRECISION) {
            ereport(WARNING,
                (errmsg("DPA: NUMERIC precision %d exceeds UADK 128-bit limit (38) for %s. "
                        "Falling back to native implementation.",
                        *precision, context)));
            return false;
        }
        *scale = (uint16)(tmp_typmod & DPA_NUMERIC_SCALE_MASK);
    } else {
        /*
         * NUMERIC without explicit precision (e.g., from expression like a * b)
         * Use default precision/scale that fits within int128 (38 digits max).
         *
         * Default: precision=38 (max for int128), scale=DPA_DEFAULT_NUMERIC_SCALE
         * This allows expressions like "price * (1 - discount)" to be accelerated.
         */
        *precision = DPA_MAX_PRECISION;
        *scale = DPA_DEFAULT_NUMERIC_SCALE;
        ereport(DEBUG1,
            (errmsg("DPA: NUMERIC without explicit precision for %s, using default (precision=%d, scale=%d)",
                    context, *precision, *scale)));
    }

    return true;
}

bool HashAggRunner::DaeCollectKeyCollInfo(struct wd_key_col_info *key_cols_info, const Oid colType,
    int idx, int4 typeMod)
{
    if (colType == BPCHAROID) {
        return DaeSetupBpcharKeyType(&key_cols_info[idx], typeMod);
    } else if (colType == VARCHAROID) {
        if (typeMod <= (int32)VARHDRSZ) {
            ereport(WARNING,
                (errmsg("DPA: VARCHAR without explicit length not supported for key column %d (UADK limit: %d bytes). "
                        "Please use VARCHAR(n) with n <= %d. Falling back to native implementation.",
                        idx, DPA_MAX_VCHAR_SIZE, DPA_MAX_VCHAR_SIZE)));
            return false;
        }
        int varcharLen = typeMod - VARHDRSZ;
        if (varcharLen > DPA_MAX_VCHAR_SIZE) {
            ereport(WARNING,
                (errmsg("DPA: Key VARCHAR(%d) exceeds hardware limit %d bytes, fallback to CPU",
                        varcharLen, DPA_MAX_VCHAR_SIZE)));
            return false;
        }
        key_cols_info[idx].input_data_type = WD_DAE_VARCHAR;
        key_cols_info[idx].col_data_info = varcharLen;
    } else if (colType == INT8OID) {
        key_cols_info[idx].input_data_type = WD_DAE_LONG;
        key_cols_info[idx].col_data_info = sizeof(int64);
    } else if (colType == INT4OID) {
        key_cols_info[idx].input_data_type = WD_DAE_INT;
        key_cols_info[idx].col_data_info = sizeof(int32);
    } else if (colType == NUMERICOID) {
        uint16 precision = 0;
        uint16 scale = 0;
        if (!DaeCheckNumericPrecision(typeMod, &precision, &scale, "key column")) {
            return false;
        }
        key_cols_info[idx].input_data_type = WD_DAE_LONG_DECIMAL;
        key_cols_info[idx].col_data_info = (scale << CHAR_BITS) | precision;
    } else if (colType == DATEOID) {
        key_cols_info[idx].input_data_type = WD_DAE_DATE;
        key_cols_info[idx].col_data_info = sizeof(int32);
    } else if (colType == TIMESTAMPOID) {
        key_cols_info[idx].input_data_type = WD_DAE_LONG;
        key_cols_info[idx].col_data_info = sizeof(int64);
    } else {
        ereport(WARNING, (errmsg("DPA: Unsupported key column type OID: %u", colType)));
        return false;
    }
    return true;
}

static bool DaeSetupSumInputType(struct wd_agg_col_info *colInfo, Oid inputColType, int4 typeMod)
{
    if (inputColType == INT8OID) {
        colInfo->input_data_type = WD_DAE_LONG;
        colInfo->col_data_info = sizeof(int64);
    } else if (inputColType == INT4OID) {
        colInfo->input_data_type = WD_DAE_LONG;
        colInfo->col_data_info = sizeof(int64);
    } else if (inputColType == NUMERICOID) {
        uint16 precision = 0;
        uint16 scale = 0;
        if (!DaeCheckNumericPrecision(typeMod, &precision, &scale, "SUM aggregation")) {
            return false;
        }
        colInfo->input_data_type = WD_DAE_LONG_DECIMAL;
        colInfo->col_data_info = (scale << CHAR_BITS) | precision;
    } else {
        ereport(WARNING,
			(errmsg("DPA: SUM aggregation only supports INT8/NUMERIC/int4, got type OID: %u", inputColType)));
        return false;
    }
    return true;
}

static bool DaeSetupCountBpcharType(struct wd_agg_col_info *colInfo, int4 typeMod)
{
    colInfo->input_data_type = WD_DAE_CHAR;
    
    if (typeMod <= (int32)VARHDRSZ) {
        ereport(WARNING, (errmsg("DPA: Invalid BPCHAR typeMod: %d for COUNT aggregation", typeMod)));
        return false;
    }
    
    int charLen = typeMod - VARHDRSZ;
    colInfo->col_data_info = charLen;
    return true;
}

template<typename DestType, typename ConvertFunc>
static void DaeBuildColBasicType(
    struct wd_dae_col_addr *col,
    ScalarValue* vals,
    uint8* flags,
    int32 rows,
    ConvertFunc convert)
{
    DestType *dest = (DestType *)col->value;
    uint8 *empty = col->empty;
    
    for (int32 j = 0; j < rows; j++) {
        if (likely(NOT_NULL(flags[j]))) {
            dest[j] = convert(vals[j]);
            empty[j] = 0;
        } else {
            dest[j] = 0;
            empty[j] = 1;
        }
    }
    
    col->value_size = rows * sizeof(DestType);
    col->empty_size = rows * sizeof(__u8);
}

static inline int32 ConvertInt32(ScalarValue val) { return (int32)val; }
static inline int64 ConvertInt64(ScalarValue val) { return (int64)val; }
static inline int64 ConvertInt32ToInt64(ScalarValue val) { return (int64)(int32)val; }
static inline int32 ConvertDate(ScalarValue val) { return DatumGetDateADT(val); }
static inline int64 ConvertTimestamp(ScalarValue val) { return DatumGetTimestamp(val); }

/*
 * DetectCountStarResult - Result of COUNT(*) detection
 */
struct DetectCountStarResult {
    uint32 countStarCount;        // Number of COUNT(*) found
    uint32 regularAggCount;       // Number of regular aggregations
    int countStarAggInfoIdx;      // Index of first COUNT(*) in aggInfo (-1 if none)
};

/*
 * Check if an aggregation is COUNT(*)
 *
 * COUNT(*) is distinguished from COUNT(column) by:
 * 1. Function OID is count function (COUNTOID or INT8INC_FUNCOID)
 * 2. evalproj is NULL (no input column to evaluate)
 *
 * COUNT(column) has the same OID but evalproj != NULL
 */
static inline bool IsCountStar(Oid aggFuncOid, VecAggStatePerAgg per_agg_state)
{
    bool isCountOid = (aggFuncOid == COUNTOID || aggFuncOid == INT8INC_FUNCOID);
    bool hasNoInput = (per_agg_state->evalproj == NULL);
    return isCountOid && hasNoInput;
}

/*
 * DetectCountStarInAggregations - Detect COUNT(*) in aggregations
 *
 * Scans all aggregation functions to identify COUNT(*) operations.
 * UADK hardware only supports a single COUNT(*).
 *
 * @param aggNum: Total number of aggregations
 * @param runtime: Runtime information containing aggInfo and pervecagg
 * @return DetectCountStarResult containing detection statistics
 */
static DetectCountStarResult DetectCountStarInAggregations(uint32 aggNum, VecAggState* runtime)
{
    DetectCountStarResult result = {0, 0, -1};
    
    ereport(DEBUG1, (errmsg("DPA: Phase 1 - Detecting COUNT(*), m_aggNum=%u", aggNum)));
    
    for (uint32 i = 0; i < aggNum; i++) {
        const Oid aggFuncOid = runtime->aggInfo[i].vec_agg_function->flinfo->fn_oid;
        int pervecagg_idx = aggNum - 1 - i;
        
        if (IsCountStar(aggFuncOid, &runtime->pervecagg[pervecagg_idx])) {
            result.countStarCount++;
            if (result.countStarAggInfoIdx < 0) {
                result.countStarAggInfoIdx = i; // Record first COUNT(*)
            }
            ereport(DEBUG1,
				(errmsg("DPA: Phase 1 - Found COUNT(*) #%u at aggInfo[%u]", result.countStarCount, i)));
        } else {
            result.regularAggCount++;
            ereport(DEBUG1,
				(errmsg("DPA: Phase 1 - Regular agg at aggInfo[%u], count=%u", i, result.regularAggCount)));
        }
    }
    
    return result;
}

static inline void DaeTryCaptureNumericScale(Numeric num, int scale, bool *scaleFound, int *outActualScale)
{
    if (*scaleFound || outActualScale == nullptr) {
        return;
    }
    *outActualScale = scale;
    *scaleFound = true;
}

/*
 * DaeBuildColNumeric - Build NUMERIC column for DAE input
 *
 * Following openGauss native approach: extract scale from actual data values
 * rather than relying on typeMod (which is often invalid for expressions).
 *
 * outActualScale: if not NULL, stores the scale extracted from first non-null value
 * scaleAlreadySet: if true, outActualScale is not modified (already captured earlier)
 */
static void DaeBuildColNumeric(struct wd_dae_col_addr *col, ScalarValue* vals, uint8* flags, int32 rows,
                               int* outActualScale = nullptr, bool scaleAlreadySet = false)
{
    __int128 *dest = (__int128 *)col->value;
    uint8 *empty = col->empty;
    bool scaleFound = scaleAlreadySet;
    
    for (int32 j = 0; j < rows; j++) {
        if (!NOT_NULL(flags[j])) {
            dest[j] = 0;
            empty[j] = 1;
            continue;
        }

        Numeric num = DatumGetNumeric(vals[j]);
        if (likely(NUMERIC_IS_BI64(num))) {
            dest[j] = (__int128)NUMERIC_64VALUE(num);
            empty[j] = 0;
            DaeTryCaptureNumericScale(num, NUMERIC_BI_SCALE(num), &scaleFound, outActualScale);
        } else if (NUMERIC_IS_BI128(num)) {
            int128 result;
            errno_t rc = memcpy_s(&result, sizeof(int128), num->choice.n_bi.n_data, sizeof(int128));
            securec_check(rc, "\0", "\0");
            dest[j] = result;
            empty[j] = 0;
            DaeTryCaptureNumericScale(num, NUMERIC_BI_SCALE(num), &scaleFound, outActualScale);
        } else if (NUMERIC_IS_SHORT(num)) {
            int scale = NUMERIC_DSCALE(num);
            int128 result = 0;
            convert_short_numeric_to_int128_byscale(num, scale, result);
            dest[j] = result;
            empty[j] = 0;
            DaeTryCaptureNumericScale(num, scale, &scaleFound, outActualScale);
        } else {
            dest[j] = 0;
            empty[j] = 1;
        }
    }
    
    col->value_size = rows * sizeof(__int128);
    col->empty_size = rows * sizeof(__u8);
}

static bool DaeSetupCountInputType(struct wd_agg_col_info *colInfo, Oid inputColType, int4 typeMod)
{
    if (inputColType == BPCHAROID) {
        return DaeSetupCountBpcharType(colInfo, typeMod);
    } else if (inputColType == VARCHAROID) {
        if (typeMod <= (int32)VARHDRSZ) {
            ereport(WARNING, (errmsg(
                "DPA: VARCHAR without explicit length not supported for COUNT aggregation (UADK limit: %d bytes). "
                "Please use VARCHAR(n) with n <= %d. Falling back to native implementation.",
                DPA_MAX_VCHAR_SIZE, DPA_MAX_VCHAR_SIZE)));
            return false;
        }
        int varcharLen = typeMod - VARHDRSZ;
        if (varcharLen > DPA_MAX_VCHAR_SIZE) {
            ereport(WARNING, (errmsg("DPA: COUNT VARCHAR(%d) exceeds hardware limit %d bytes, fallback to CPU",
                varcharLen, DPA_MAX_VCHAR_SIZE)));
            return false;
        }
        colInfo->input_data_type = WD_DAE_VARCHAR;
        colInfo->col_data_info = varcharLen;
    } else if (inputColType == INT8OID) {
        colInfo->input_data_type = WD_DAE_LONG;
        colInfo->col_data_info = sizeof(int64);
    } else if (inputColType == INT4OID) {
        colInfo->input_data_type = WD_DAE_INT;
        colInfo->col_data_info = sizeof(int32);
    } else if (inputColType == NUMERICOID) {
        uint16 precision = 0;
        uint16 scale = 0;
        if (!DaeCheckNumericPrecision(typeMod, &precision, &scale, "COUNT aggregation")) {
            return false;
        }
        colInfo->input_data_type = WD_DAE_LONG_DECIMAL;
        colInfo->col_data_info = (scale << CHAR_BITS) | precision;
    } else if (inputColType == DATEOID) {
        colInfo->input_data_type = WD_DAE_DATE;
        colInfo->col_data_info = sizeof(int32);
    } else if (inputColType == TIMESTAMPOID) {
        colInfo->input_data_type = WD_DAE_LONG;
        colInfo->col_data_info = sizeof(int64);
    } else {
        ereport(WARNING, (errmsg("DPA: COUNT aggregation only supports INT4/INT8/VARCHAR/CHAR/NUMERIC/DATE/TIMESTAMP, "
            "got type OID: %u", inputColType)));
        return false;
    }
    return true;
}

static enum wd_dae_data_type DaeGetOutputDataType(wd_agg_alg aggAlg, Oid inputColType, Oid outputColType)
{
    if (aggAlg == WD_AGG_SUM && inputColType == INT8OID && outputColType == NUMERICOID) {
        return WD_DAE_LONG;
    }
    return MapOidToDAEType(outputColType);
}

bool HashAggRunner::DaeCollectAggCollInfo(struct wd_agg_col_info *agg_cols_info, Oid inputColType, Oid outputColType,
    int idx, int4 typeMod, Oid aggFuncOid)
{
    bool isAvg = false;
    wd_agg_alg aggAlg = MapAggFuncOidToAlg(aggFuncOid, &isAvg);
    if (aggAlg == WD_AGG_ALG_TYPE_MAX) {
        ereport(WARNING, (errmsg("DPA: Unsupported aggregation function OID: %u", aggFuncOid)));
        return false;
    }
    
    // For AVG operations, we need both COUNT and SUM
    // Note: openGauss stores AVG state as {COUNT, SUM}, not {SUM, COUNT}!
    if (isAvg) {
        agg_cols_info[idx].col_alg_num = DPA_AVG_ALG_NUM;
        
        // Setup input type (same for both COUNT and SUM)
        bool setup_ok = DaeSetupSumInputType(&agg_cols_info[idx], inputColType, typeMod);
        if (!setup_ok) {
            return false;
        }
        
        // Configure first output: COUNT (must be first to match openGauss order!)
        // COUNT always outputs WD_DAE_LONG (int64)
        agg_cols_info[idx].output_data_types[0] = WD_DAE_LONG;
        agg_cols_info[idx].output_col_algs[0] = WD_AGG_COUNT;
        
        // Configure second output: SUM (must be second to match openGauss order!)
        // For AVG, outputColType might be numeric[] (1231), but we need the actual SUM output type
        // We determine it based on the input type:
        // - INT8 input -> INT8 output (WD_DAE_LONG) for intermediate sum
        // - NUMERIC input -> NUMERIC output (WD_DAE_LONG_DECIMAL)
        wd_dae_data_type sumOutputType;
        if (inputColType == INT8OID || inputColType == INT4OID || inputColType == INT2OID) {
            // Integer types: SUM outputs INT8 (which will be converted to NUMERIC later)
            sumOutputType = WD_DAE_LONG;
        } else if (inputColType == NUMERICOID) {
            // NUMERIC type: SUM outputs NUMERIC (stored as int128)
            sumOutputType = WD_DAE_LONG_DECIMAL;
        } else {
            ereport(WARNING, (errmsg("DPA: Unsupported AVG input type OID: %u", inputColType)));
            return false;
        }
        
        agg_cols_info[idx].output_data_types[1] = sumOutputType;
        agg_cols_info[idx].output_col_algs[1] = WD_AGG_SUM;
        return true;
    }
    
    // Regular aggregation (SUM or COUNT only)
    agg_cols_info[idx].col_alg_num = 1;
    
    bool setup_ok = false;
    if (aggAlg == WD_AGG_SUM) {
        setup_ok = DaeSetupSumInputType(&agg_cols_info[idx], inputColType, typeMod);
    } else if (aggAlg == WD_AGG_COUNT) {
        setup_ok = DaeSetupCountInputType(&agg_cols_info[idx], inputColType, typeMod);
    } else {
        ereport(WARNING, (errmsg("DPA: Unknown aggregation algorithm type")));
        return false;
    }
    
    if (!setup_ok) {
        return false;
    }

    for (int j = 0; j < agg_cols_info[idx].col_alg_num; j++) {
        agg_cols_info[idx].output_data_types[j] = DaeGetOutputDataType(aggAlg, inputColType, outputColType);
        if (agg_cols_info[idx].output_data_types[j] == WD_DAE_DATA_TYPE_MAX) {
            ereport(WARNING, (errmsg("DPA: Unsupported aggregation output column type OID: %u", outputColType)));
            return false;
        }
        agg_cols_info[idx].output_col_algs[j] = aggAlg;
    }
    
    return true;
}

void HashAggRunner::DaeFreeHTBL(struct wd_dae_hash_table *table)
{
    if (!table) {
        return;
    }
    
    if (table->std_table) {
        pfree(table->std_table);
        table->std_table = nullptr;
    }
    
    if (table->ext_table) {
        if (table->table_row_size == EXT_ROW_SIZE1) {
            table->ext_table -= EXT_ROW_SIZE1 * EXT_ROW_BYTES1;
        } else if (table->table_row_size == EXT_ROW_SIZE2) {
            table->ext_table -= EXT_ROW_SIZE2 * EXT_ROW_BYTES2;
        }
        pfree(table->ext_table);
        table->ext_table = nullptr;
    }
}

static void DaeFreeValueAddr(struct wd_dae_col_addr *cols, int num)
{
    if (!cols) {
        return;
    }

    for (int i = 0; i < num; i++) {
        if (cols[i].offset) {
            pfree(cols[i].offset);
        }
        pfree(cols[i].empty);
        pfree(cols[i].value);
    }
    pfree(cols);
}

void HashAggRunner::DaeFreeInputOutputMem()
{
    if (daeReq_.key_cols) {
        DaeFreeValueAddr(daeReq_.key_cols, daeReq_.key_cols_num);
        daeReq_.key_cols = nullptr;
    }
    
    if (daeReq_.out_key_cols) {
        DaeFreeValueAddr(daeReq_.out_key_cols, daeReq_.out_key_cols_num);
        daeReq_.out_key_cols = nullptr;
    }
    
    if (daeReq_.agg_cols) {
        DaeFreeValueAddr(daeReq_.agg_cols, daeReq_.agg_cols_num);
        daeReq_.agg_cols = nullptr;
    }
    
    if (daeReq_.out_agg_cols) {
        DaeFreeValueAddr(daeReq_.out_agg_cols, daeReq_.out_agg_cols_num);
        daeReq_.out_agg_cols = nullptr;
    }
    
    errno_t rc = memset_s(&daeReq_, sizeof(daeReq_), 0, sizeof(daeReq_));
    securec_check_c(rc, "\0", "\0");
}

static bool DaeAllocValueAddr(struct wd_dae_col_addr *col, __u32 row_num, enum wd_dae_data_type type,
    __u16 data_info, int index)
{
    col[index].empty = (__u8*)palloc0(sizeof(char) * row_num);
    if (!col[index].empty) {
        return true;
    }
    col[index].empty_size = row_num;
    switch (type) {
        case WD_DAE_INT:
            col[index].value = palloc(sizeof(int) * row_num);
            col[index].value_size = sizeof(int) * row_num;
            break;
        case WD_DAE_LONG:
            col[index].value = palloc(sizeof(long long) * row_num);
            col[index].value_size = sizeof(long long) * row_num;
            break;
        case WD_DAE_LONG_DECIMAL:
            // NUMERIC type: use fixed-size allocation (16 bytes per value)
            // This is sufficient for most NUMERIC(precision, scale) values
            col[index].value = palloc(sizeof(__int128) * row_num);
            col[index].value_size = sizeof(__int128) * row_num;
            break;
        case WD_DAE_CHAR:
            col[index].value = palloc(data_info * row_num);
            col[index].value_size = data_info * row_num;
            break;
        case WD_DAE_VARCHAR:
            col[index].offset_size = sizeof(int) * (row_num + 1);
            col[index].offset = (__u32*)palloc((row_num + 1) * sizeof(int));
            if (!col[index].offset) {
                pfree(col[index].empty);
                col[index].empty = nullptr;
                return true;
            }
            col[index].value = palloc(MAX_CHAR_SIZE * row_num);
            col[index].value_size = MAX_CHAR_SIZE * row_num;
            break;
        default:
            ereport(WARNING,
                (errmsg("DPA: Unsupported data type %d in DaeAllocValueAddr", type)));
            return true;
    }

    if (!col[index].value) {
        if (col[index].offset) {
            pfree(col[index].offset);
            col[index].offset = nullptr;
        }
        pfree(col[index].empty);
        col[index].empty = nullptr;
        return true;
    }
    return false;
}

bool HashAggRunner::DaeInitKeyInputAddress(struct wd_agg_sess_setup *setup, VectorBatch* batch)
{
    struct wd_dae_col_addr *key_in_cols;
    int i;
    bool ret;

    key_in_cols = (wd_dae_col_addr*)palloc(setup->key_cols_num * sizeof(struct wd_dae_col_addr));
    if (!key_in_cols) {
        return true;
    }

    for (i = 0; i < setup->key_cols_num; i++) {
        ret = DaeAllocValueAddr(key_in_cols, DEFAULT_ROW_NUM, setup->key_cols_info[i].input_data_type,
            setup->key_cols_info[i].col_data_info, i);
        if (ret) {
            DaeFreeValueAddr(key_in_cols, i);
            return true;
        }

        key_in_cols[i].empty_size = batch->m_rows * sizeof(key_in_cols[i].empty[0]);
        key_in_cols[i].value_size = batch->m_rows * setup->key_cols_info[i].col_data_info;
        key_in_cols[i].offset_size = sizeof(int);
    }

    daeReq_.key_cols = key_in_cols;
    return false;
}

bool HashAggRunner::DaeInitKeyOutputAddress(struct wd_agg_sess_setup *setup)
{
    struct wd_dae_col_addr *key_out_cols;
    int i;
    bool ret;

    key_out_cols = (wd_dae_col_addr*)palloc(setup->key_cols_num * sizeof(struct wd_dae_col_addr));
    if (!key_out_cols) {
        return true;
    }

    for (i = 0; i < setup->key_cols_num; i++) {
        ret = DaeAllocValueAddr(key_out_cols, DEFAULT_ROW_NUM, setup->key_cols_info[i].input_data_type,
            setup->key_cols_info[i].col_data_info, i);
        if (ret) {
            DaeFreeValueAddr(key_out_cols, i);
            return true;
        }
    }

    daeReq_.out_key_cols = key_out_cols;
    return false;
}

bool HashAggRunner::DaeInitAggInputAddress(struct wd_agg_sess_setup *setup, VectorBatch* batch)
{
    struct wd_dae_col_addr *agg_in_cols;
    int i;
    bool ret;

    agg_in_cols = (wd_dae_col_addr*)palloc(setup->agg_cols_num * sizeof(struct wd_dae_col_addr));
    if (!agg_in_cols) {
        return true;
    }

    for (i = 0; i < setup->agg_cols_num; i++) {
        ret = DaeAllocValueAddr(agg_in_cols, DEFAULT_ROW_NUM, setup->agg_cols_info[i].input_data_type,
            setup->agg_cols_info[i].col_data_info, i);
        if (ret) {
            DaeFreeValueAddr(agg_in_cols, i);
            return true;
        }
        agg_in_cols[i].empty_size = batch->m_rows * sizeof(agg_in_cols[i].empty[0]);
        agg_in_cols[i].value_size = batch->m_rows * setup->agg_cols_info[i].col_data_info;
        agg_in_cols[i].offset_size = sizeof(int);
    }

    daeReq_.agg_cols = agg_in_cols;
    return false;
}

/*
 * Map AVG sub-algorithm output indices (COUNT/SUM) back to aggInfo entries.
 * For AVG, col_alg_num==2: j==0 is COUNT, j==1 is SUM.
 */
void HashAggRunner::DaeMapAvgOutputIdx(int setupIdx, int subIdx, int outColIdx)
{
    for (uint32 aggInfoIdx = 0; aggInfoIdx < m_aggNum; aggInfoIdx++) {
        if (hasCountStar_ && aggInfoIdx == countStarAggInfoIdx_)
            continue;
        int pervecaggIdx = m_aggNum - 1 - aggInfoIdx;
        if (pervecaggIdx >= 0 &&
            pervecaggToRegularIdx_[pervecaggIdx] == setupIdx) {
            if (subIdx == 0) {
                avgCountOutputIdx_[aggInfoIdx] = outColIdx;
                ereport(DEBUG1,
                    (errmsg("DPA: AVG aggInfo[%u] COUNT output at dae_out[%d]", aggInfoIdx, outColIdx)));
            } else if (subIdx == 1) {
                avgSumOutputIdx_[aggInfoIdx] = outColIdx;
                ereport(DEBUG1,
                    (errmsg("DPA: AVG aggInfo[%u] SUM output at dae_out[%d]", aggInfoIdx, outColIdx)));
            }
            break;
        }
    }
}

bool HashAggRunner::DaeInitAggOutputAddress(struct wd_agg_sess_setup *setup)
{
    int i = 0;
    int j = 0;
    int k = 0;
    bool ret;

    int total_out_cols = 0;
    for (i = 0; i < setup->agg_cols_num; i++) {
        total_out_cols += setup->agg_cols_info[i].col_alg_num;
    }
    if (setup->is_count_all) {
        total_out_cols++;
    }

    struct wd_dae_col_addr *agg_out_cols =
        (wd_dae_col_addr*)palloc(total_out_cols * sizeof(struct wd_dae_col_addr));
    if (!agg_out_cols) {
        return true;
    }

    for (i = 0; i < setup->agg_cols_num; i++) {
        for (j = 0; j < setup->agg_cols_info[i].col_alg_num; j++) {
            ret = DaeAllocValueAddr(agg_out_cols, DEFAULT_ROW_NUM, setup->agg_cols_info[i].output_data_types[j],
                setup->agg_cols_info[i].col_data_info, k);
            if (ret) {
                DaeFreeValueAddr(agg_out_cols, k);
                return true;
            }
            if (setup->agg_cols_info[i].col_alg_num == DPA_AVG_ALG_NUM) {
                DaeMapAvgOutputIdx(i, j, k);
            }
            ++k;
        }
    }

    if (setup->is_count_all) {
        ret = DaeAllocValueAddr(agg_out_cols, DEFAULT_ROW_NUM, WD_DAE_LONG, 0, k);
        if (ret) {
            DaeFreeValueAddr(agg_out_cols, k);
            return true;
        }
        ++k;
        ereport(DEBUG1, (errmsg("DPA: Allocated COUNT(*) output at index %d, total_out_cols=%d", k - 1,
            total_out_cols)));
    }

    ereport(DEBUG1, (errmsg("DPA: Total allocated output columns: %d", k)));
    daeReq_.out_agg_cols = agg_out_cols;
    return false;
}

bool HashAggRunner::DaeInitInputOutputMem(struct wd_agg_sess_setup *setup, VectorBatch* batch)
{
    if (DaeInitKeyInputAddress(setup, batch)) {
        return true;
    }
    if (DaeInitKeyOutputAddress(setup)) {
        return true;
    }
    if (DaeInitAggInputAddress(setup, batch)) {
        return true;
    }
    if (DaeInitAggOutputAddress(setup)) {
        return true;
    }

    daeReq_.out_row_count = DEFAULT_ROW_NUM;
    daeReq_.in_row_count = batch->m_rows;
    daeReq_.out_key_cols_num = setup->key_cols_num;
    
    // Calculate out_agg_cols_num by summing up col_alg_num for each agg column
    // This is important for AVG which has col_alg_num=2 (COUNT+SUM)
    __u32 calculated_out_agg_cols = 0;
    for (__u32 i = 0; i < setup->agg_cols_num; i++) {
        calculated_out_agg_cols += setup->agg_cols_info[i].col_alg_num;
    }
    // Add COUNT(*) if present
    if (setup->is_count_all) {
        calculated_out_agg_cols += 1;
    }
    
    daeReq_.out_agg_cols_num = calculated_out_agg_cols;
    daeReq_.key_cols_num = setup->key_cols_num;
    daeReq_.agg_cols_num = setup->agg_cols_num;
    
    ereport(DEBUG1,
		(errmsg("DPA: DaeInitInputOutputMem - agg_cols_num=%u, out_agg_cols_num=%u (calculated), is_count_all=%d",
        	daeReq_.agg_cols_num, daeReq_.out_agg_cols_num, setup->is_count_all)));

    return false;
}

static void DaeBuildColBpchar(struct wd_dae_col_addr *col, ScalarValue* vals, uint8* flags,
    int32 rows, int dataLen)
{
    char *dest = (char *)col->value;
    uint8 *empty = col->empty;
    char *destPtr = dest;
    
    for (int32 j = 0; j < rows; j++) {
        if (likely(NOT_NULL(flags[j]))) {
            // Non-NULL value: copy and pad
            int actual_len = VARSIZE_ANY_EXHDR(vals[j]);
            int copy_len = (actual_len < dataLen) ? actual_len : dataLen;
            
            errno_t rc = memcpy_s(destPtr, dataLen, VARDATA_ANY(vals[j]), copy_len);
            securec_check(rc, "\0", "\0");
            
            // Pad with spaces if needed (BPCHAR requirement)
            if (actual_len < dataLen) {
                rc = memset_s(destPtr + copy_len, dataLen - copy_len, ' ', dataLen - copy_len);
                securec_check(rc, "\0", "\0");
            }
            
            empty[j] = 0;
        } else {
            // NULL value: fill with spaces
            errno_t rc = memset_s(destPtr, dataLen, ' ', dataLen);
            securec_check(rc, "\0", "\0");
            empty[j] = 1;
        }
        
        destPtr += dataLen;
    }
    
    col->value_size = rows * dataLen;
    col->empty_size = rows * sizeof(__u8);
}

static void DaeBuildColVarchar(struct wd_dae_col_addr *col, ScalarValue* vals, uint8* flags, int32 rows)
{
    __u32 *offset = col->offset;
    char *dest = (char *)col->value;
    uint8 *empty = col->empty;
    __u32 total_size = 0;
    
    offset[0] = 0;
    for (int32 j = 0; j < rows; j++) {
        if (likely(NOT_NULL(flags[j]))) {
            // Non-NULL value: copy variable-length data
            int dataLen = VARSIZE_ANY_EXHDR(vals[j]);
            if (dataLen > 0) {
                errno_t rc = memcpy_s(dest + total_size, dataLen, VARDATA_ANY(vals[j]), dataLen);
                securec_check(rc, "\0", "\0");
            }
            total_size += dataLen;
            empty[j] = 0;
        } else {
            // NULL value: no data to copy
            empty[j] = 1;
        }
        offset[j + 1] = total_size;
    }
    col->value_size = total_size;
    col->empty_size = rows * sizeof(__u8);
    col->offset_size = (rows + 1) * sizeof(__u32);
}

bool HashAggRunner::ParallelBuildKeyValue(VectorBatch* batch)
{
    int32 rows = batch->m_rows;
    
    for (uint32 i = 0; i < m_key; i++) {
        ScalarVector& tile = batch->m_arr[m_keyIdx[i]];
        const Oid colType = tile.m_desc.typeId;
        ScalarValue* vals = tile.m_vals;
        uint8* flags = tile.m_flag;

        if (colType == BPCHAROID) {
            int dataLen = (tile.m_desc.typeMod > (int32)VARHDRSZ) ?
                           (tile.m_desc.typeMod - VARHDRSZ) : 0;
            if (dataLen <= 0) {
                elog(WARNING, "Invalid BPCHAR typeMod: %d in key column", tile.m_desc.typeMod);
                return false;
            }
            DaeBuildColBpchar(&daeReq_.key_cols[i], vals, flags, rows, dataLen);
        } else if (colType == VARCHAROID) {
            DaeBuildColVarchar(&daeReq_.key_cols[i], vals, flags, rows);
        } else if (colType == INT4OID) {
            DaeBuildColBasicType<int32>(&daeReq_.key_cols[i], vals, flags, rows, ConvertInt32);
        } else if (colType == INT8OID) {
            DaeBuildColBasicType<int64>(&daeReq_.key_cols[i], vals, flags, rows, ConvertInt64);
        } else if (colType == NUMERICOID) {
            DaeBuildColNumeric(&daeReq_.key_cols[i], vals, flags, rows);
        } else if (colType == DATEOID) {
            DaeBuildColBasicType<int32>(&daeReq_.key_cols[i], vals, flags, rows, ConvertDate);
        } else if (colType == TIMESTAMPOID) {
            DaeBuildColBasicType<int64>(&daeReq_.key_cols[i], vals, flags, rows, ConvertTimestamp);
        } else {
            elog(WARNING, "unsupported type %d", colType);
            return false;
        }
    }
    return true;
}

bool HashAggRunner::ParallelBuildAggValue(VectorBatch* batch)
{
    int32 rows = batch->m_rows;

    for (int i = 0; i < m_aggNum; i++) {
        /*
         * Skip COUNT(*) - it has no input column
         *
         * COUNT(*) is handled by hardware directly, doesn't need input data
         * Note: pervecagg[i] corresponds to aggInfo[m_aggNum - 1 - i]
         */
        int aggInfoIdx = m_aggNum - 1 - i;
        if (hasCountStar_ && aggInfoIdx == countStarAggInfoIdx_) {
            ereport(DEBUG1, (errmsg("DPA: Skipping COUNT(*) (no input column)")));
            continue;
        }
        
        // Use mapping to get DPA setup index (pervecagg_idx → setup_idx)
        // This directly gives us the index in daeReq_.agg_cols and daeAggCols_
        int setupIdx = pervecaggToRegularIdx_[i];
        if (setupIdx < 0) {
            elog(WARNING, "Invalid mapping for pervecagg[%d]", i);
            return false;
        }
        
        VecAggStatePerAgg per_agg_state = &m_runtime->pervecagg[i];
        VectorBatch* agg_batch = NULL;
        ScalarVector* p_vector = NULL;
        ExprContext* econtext = NULL;

        if (per_agg_state->evalproj != NULL) {
            econtext = per_agg_state->evalproj->pi_exprContext;
            econtext->ecxt_outerbatch = batch;
            agg_batch = ExecVecProject(per_agg_state->evalproj);
            p_vector = &agg_batch->m_arr[0];
        } else {
            agg_batch = batch;
            p_vector = &batch->m_arr[0];
        }

        ScalarVector& tile = *p_vector;
        const Oid colType = tile.m_desc.typeId;
        ScalarValue* vals = tile.m_vals;
        uint8* flags = tile.m_flag;

        if (colType == BPCHAROID) {
            int dataLen = (tile.m_desc.typeMod > (int32)VARHDRSZ) ?
                           (tile.m_desc.typeMod - VARHDRSZ) : 0;
            if (dataLen <= 0) {
                elog(WARNING, "Invalid BPCHAR typeMod: %d in agg column", tile.m_desc.typeMod);
                return false;
            }
            DaeBuildColBpchar(&daeReq_.agg_cols[setupIdx], vals, flags, rows, dataLen);
        } else if (colType == VARCHAROID) {
            DaeBuildColVarchar(&daeReq_.agg_cols[setupIdx], vals, flags, rows);
        } else if (colType == INT4OID) {
            // For SUM aggregation, INT4 is auto-converted to INT8
            // Check if this column was set up as INT8 (for SUM)
            if (daeAggCols_[setupIdx].input_data_type == WD_DAE_LONG) {
                // Convert INT4 to INT8 for SUM aggregation
                DaeBuildColBasicType<int64>(&daeReq_.agg_cols[setupIdx], vals, flags, rows, ConvertInt32ToInt64);
            } else {
                // For COUNT or other operations, keep as INT4
                DaeBuildColBasicType<int32>(&daeReq_.agg_cols[setupIdx], vals, flags, rows, ConvertInt32);
            }
        } else if (colType == INT8OID) {
            DaeBuildColBasicType<int64>(&daeReq_.agg_cols[setupIdx], vals, flags, rows, ConvertInt64);
        } else if (colType == NUMERICOID) {
            // Capture actual scale from data (openGauss native approach)
            // This handles expressions like l_extendedprice * (1 - l_discount) where typeMod is invalid
            DaeBuildColNumeric(&daeReq_.agg_cols[setupIdx], vals, flags, rows,
                              &daeAggColActualScales_[i], daeAggColScaleSet_[i]);
            daeAggColScaleSet_[i] = true;  // Mark as set for subsequent batches
        } else if (colType == DATEOID) {
            DaeBuildColBasicType<int32>(&daeReq_.agg_cols[setupIdx], vals, flags, rows, ConvertDate);
        } else if (colType == TIMESTAMPOID) {
            DaeBuildColBasicType<int64>(&daeReq_.agg_cols[setupIdx], vals, flags, rows, ConvertTimestamp);
        } else {
            elog(WARNING, "unsupported type %d", colType);
            return false;
        }

        if (econtext != NULL) {
            ResetExprContext(econtext);
        }
    }
    return true;
}

/*
 * DaeHandleRehash - Expand hash table and migrate data when UADK signals NEED_REHASH.
 *
 * Allocates a new table (4x current size), sets it via UADK, migrates data from
 * the old table, then replaces the old table with the new one.
 * Returns true on success, false on failure (after reporting ERROR).
 */
bool HashAggRunner::DaeHandleRehash(int rehashCount)
{
    ereport(DEBUG1,
        (errmsg("DPA: Hash table needs rehash (attempt %d), current rows: std=%u, ext=%u",
                rehashCount + 1,
                daeHashTable_.std_table_row_num,
                daeHashTable_.ext_table_row_num)));

    struct wd_dae_hash_table new_table;
    errno_t rc = memset_s(&new_table, sizeof(new_table), 0, sizeof(new_table));
    securec_check(rc, "\0", "\0");

    DaeAllocHTBL(&daeHashTable_, &new_table, daeHashTable_.table_row_size, 0);

    int set_ret = UadkAggSetHashTable(daeSess_, &new_table);
    if (set_ret != 0) {
        DaeFreeHTBL(&new_table);
        ereport(ERROR,
            (errcode(ERRCODE_INTERNAL_ERROR),
             errmsg("DPA: wd_agg_set_hash_table failed during rehash (ret=%d). "
                    "Current table: std=%u, ext=%u rows.",
                    set_ret,
                    daeHashTable_.std_table_row_num,
                    daeHashTable_.ext_table_row_num)));
        return false;
    }

    int rehash_ret = UadkAggRehashSync(daeSess_, &daeReq_);
    if (rehash_ret != 0) {
        ereport(ERROR,
            (errcode(ERRCODE_INTERNAL_ERROR),
             errmsg("DPA: wd_agg_rehash_sync failed (ret=%d). "
                    "Estimated groups: %u, new table: std=%u, ext=%u rows.",
                    rehash_ret,
                    (unsigned int)(((VecAgg*)m_runtime->ss.ps.plan)->numGroups),
                    new_table.std_table_row_num,
                    new_table.ext_table_row_num)));
        return false;
    }

    DaeFreeHTBL(&daeHashTable_);
    daeHashTable_ = new_table;

    ereport(DEBUG1,
        (errmsg("DPA: Hash table rehash completed, new rows: std=%u, ext=%u",
                daeHashTable_.std_table_row_num,
                daeHashTable_.ext_table_row_num)));
    return true;
}

/*
 * DaeAddInputWithRetry - Submit aggregation input to UADK with automatic rehash retry.
 *
 * UADK may return WD_AGG_NEED_REHASH (possibly alongside a non-zero ret code)
 * when the hash table is full. State is checked before ret to handle this correctly.
 */
void HashAggRunner::DaeAddInputWithRetry()
{
    const int MAX_REHASH_RETRIES = 5;
    int rehash_count = 0;

    while (true) {
        int ret = UadkAggAddInputSync(daeSess_, &daeReq_);

        ereport(DEBUG1,
            (errmsg("DPA: UadkAggAddInputSync returned ret=%d, state=%u",
                    ret, daeReq_.state)));

        if (daeReq_.state == WD_AGG_NEED_REHASH) {
            if (rehash_count >= MAX_REHASH_RETRIES) {
                ereport(ERROR,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("DPA: Too many rehash attempts (%d), giving up", rehash_count)));
                return;
            }
            if (!DaeHandleRehash(rehash_count)) {
                return;
            }
            rehash_count++;
            continue;
        }

        if (ret != 0 && daeReq_.state != WD_AGG_TASK_DONE && daeReq_.state != WD_AGG_SUM_OVERFLOW) {
            ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("DPA: UadkAggAddInputSync failed, ret = %d, state = %u",
                        ret, daeReq_.state)));
            return;
        }

        if (daeReq_.state == WD_AGG_TASK_DONE || daeReq_.state == WD_AGG_SUM_OVERFLOW) {
            break;
        }

        if (daeReq_.state == WD_AGG_INVALID_HASH_TABLE) {
            ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("DPA: Invalid hash table state (state=%u). Hash table may be corrupted.",
                        daeReq_.state)));
            return;
        }

        ereport(WARNING,
            (errmsg("DPA: Aggregation task returned unexpected state: %u, processed rows: %u",
                    daeReq_.state, daeReq_.real_in_row_count)));
        break;
    }
}

void HashAggRunner::DaeParallelBuild(VectorBatch* batch)
{
    if (!DaeIsSessionReady()) {
        ereport(ERROR,
            (errcode(ERRCODE_INTERNAL_ERROR),
             	errmsg("DPA: Session not ready for build operation")));
        return;
    }

    if (batch->m_rows == 0) {
        ereport(DEBUG1, (errmsg("DPA: Empty input batch (0 rows), skipping")));
        return;
    }

    daeReq_.in_row_count = batch->m_rows;
    if (!ParallelBuildKeyValue(batch)) {
        ereport(ERROR, (errmsg("DPA: Failed to build key values")));
        return;
    }

    if (!ParallelBuildAggValue(batch)) {
        ereport(ERROR, (errmsg("DPA: Failed to build agg values")));
        return;
    }

    DaeAddInputWithRetry();
    m_rows += daeReq_.real_in_row_count;
}

static void DaeProbeOutputBpchar(ScalarValue* vals, uint8* flags, char* srcData, uint8* empty,
    int outRows, int dataLen, bool forceNotNull)
{
    int totalLen = dataLen + VARHDRSZ;
    char *bulkMem = (char *)palloc(totalLen * outRows);
    
    for (int j = 0; j < outRows; j++) {
        char *dest = bulkMem + j * totalLen;
        SET_VARSIZE(dest, totalLen);
        errno_t rc = memcpy_s(VARDATA(dest), dataLen, srcData + j * dataLen, dataLen);
        securec_check(rc, "\0", "\0");
        vals[j] = PointerGetDatum(dest);
        flags[j] = forceNotNull ? 0 : empty[j];
    }
}

static void DaeProbeOutputVarchar(ScalarValue* vals, uint8* flags, char* baseValue, __u32* offset,
    uint8* empty, int outRows, bool forceNotNull)
{
    __u32 baseOff = offset[0];
    __u32 total_data_size = offset[outRows] - baseOff;
    char *bulkMem = (char *)palloc(total_data_size + outRows * VARHDRSZ);
    char *destPtr = bulkMem;
    
    for (int j = 0; j < outRows; j++) {
        int dataLen = offset[j + 1] - offset[j];
        int totalLen = dataLen + VARHDRSZ;
        SET_VARSIZE(destPtr, totalLen);
        if (dataLen > 0) {
            errno_t rc = memcpy_s(VARDATA(destPtr), dataLen, baseValue + offset[j], dataLen);
            securec_check(rc, "\0", "\0");
        }
        vals[j] = PointerGetDatum(destPtr);
        flags[j] = forceNotNull ? 0 : empty[j];
        destPtr += totalLen;
    }
}

static void DaeProbeOutputInt4(ScalarValue* vals, uint8* flags, int32* srcData, uint8* empty,
    int outRows, bool forceNotNull)
{
    for (int j = 0; j < outRows; j++) {
        vals[j] = Int32GetDatum(srcData[j]);
        flags[j] = forceNotNull ? 0 : empty[j];
    }
}

static void DaeProbeOutputInt8(ScalarValue* vals, uint8* flags, int64* srcData, uint8* empty,
    int outRows, bool forceNotNull)
{
    for (int j = 0; j < outRows; j++) {
        vals[j] = Int64GetDatum(srcData[j]);
        flags[j] = forceNotNull ? 0 : empty[j];
    }
}

static void DaeProbeOutputInt8AsNumeric(ScalarValue* vals, uint8* flags, int64* srcData, uint8* empty,
    int outRows, bool forceNotNull)
{
    for (int j = 0; j < outRows; j++) {
        if (!forceNotNull && empty[j]) {
            vals[j] = 0;
            flags[j] = 1;
        } else {
            vals[j] = DirectFunctionCall1(int8_numeric, Int64GetDatum(srcData[j]));
            flags[j] = 0;
        }
    }
}

static void DaeProbeOutputNumeric(ScalarValue* vals, uint8* flags, __int128* srcData, uint8* empty,
    int outRows, bool forceNotNull, int scale)
{
    for (int j = 0; j < outRows; j++) {
        if (!forceNotNull && empty[j]) {
            // NULL value
            vals[j] = 0;
            flags[j] = 1;
        } else {
            // Convert int128 back to Numeric using the saved scale
            int128 val = srcData[j];
            Numeric num = convert_int128_to_numeric(val, scale);
            vals[j] = NumericGetDatum(num);
            flags[j] = 0;
        }
    }
}

static inline int DaeResolveNumericScale(const bool *scaleSet, const int *actualScales,
                                         const int4 *typeMods, uint32 idx)
{
    if (scaleSet[idx] && actualScales[idx] > 0) {
        return actualScales[idx];
    }
    if (typeMods[idx] > (int32)VARHDRSZ) {
        return (typeMods[idx] - VARHDRSZ) & DPA_NUMERIC_SCALE_MASK;
    }
    return DPA_DEFAULT_NUMERIC_SCALE;
}

/*
 * Compute a single SUM datum for AVG division based on the DPA storage type.
 * Returns true on success, false on unsupported type (caller should ereport).
 */
static bool DaeComputeSumDatum(wd_dae_data_type sumDataType,
                               struct wd_dae_col_addr *sumCol, int offset, int j,
                               int scale, Datum *outDatum)
{
    if (sumDataType == WD_DAE_LONG) {
        int64 sumVal = ((int64 *)sumCol->value + offset)[j];
        *outDatum = DirectFunctionCall1(int8_numeric, Int64GetDatum(sumVal));
        return true;
    }
    if (sumDataType == WD_DAE_LONG_DECIMAL) {
        __int128 sumVal = ((__int128 *)sumCol->value + offset)[j];
        Numeric num = convert_int128_to_numeric(sumVal, scale);
        *outDatum = NumericGetDatum(num);
        return true;
    }
    return false;
}

/*
 * Compute AVG = SUM / COUNT for each row and write results to vals/flags.
 * Returns false if an unsupported sumDataType is encountered.
 */
static bool DaeProbeComputeAvgRows(ScalarValue *vals, uint8 *flags, int outRows,
                                   int64 *countValues, uint8 *countEmpty,
                                   wd_dae_data_type sumDataType,
                                   struct wd_dae_col_addr *sumCol, int offset, int scale)
{
    for (int j = 0; j < outRows; j++) {
        int64 countVal = countValues[j];
        if (countVal == 0 || countEmpty[j]) {
            vals[j] = 0;
            flags[j] = 1;
            continue;
        }

        Datum sumDatum;
        if (!DaeComputeSumDatum(sumDataType, sumCol, offset, j, scale, &sumDatum)) {
            return false;
        }

        Datum countDatum = DirectFunctionCall1(int8_numeric, Int64GetDatum(countVal));
        vals[j] = DirectFunctionCall2(numeric_div, sumDatum, countDatum);
        flags[j] = 0;
    }
    return true;
}

void HashAggRunner::DaeParallelProbe()
{
    if (!DaeIsSessionReady()) {
        ereport(ERROR,
            (errcode(ERRCODE_INTERNAL_ERROR),
             errmsg("DPA: Session not ready for probe operation")));
        return;
    }

    /*
     * Batch processing logic:
     * DPA may return more rows than BatchMaxSize (e.g., 11620 rows),
     * but m_scanBatch arrays are limited to BatchMaxSize (1000).
     * We process output in batches, tracking our offset in the DPA buffer.
     */
    
    if (!daeProbeHasPendingRows_) {
        /* No pending rows, fetch new data from DPA */
        int ret = UadkAggGetOutputSync(daeSess_, &daeReq_);
        if (ret != 0) {
            ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("DPA: UadkAggGetOutputSync failed, ret = %d", ret)));
            return;
        }
        
        daeProbeOutputTotalRows_ = daeReq_.real_out_row_count;
        daeProbeOutputOffset_ = 0;
        
        if (daeProbeOutputTotalRows_ <= 0) {
            if (daeReq_.output_done) {
                ereport(DEBUG1, (errmsg("DPA: All output data retrieved")));
            }
            return;
        }
        
        ereport(DEBUG1, (errmsg("DPA: Fetched %d rows from DPA, will process in batches",
                               daeProbeOutputTotalRows_)));
    }
    
    /* Calculate how many rows to process this batch */
    int remaining_rows = daeProbeOutputTotalRows_ - daeProbeOutputOffset_;
    int outRows = (remaining_rows > BatchMaxSize) ? BatchMaxSize : remaining_rows;
    int offset = daeProbeOutputOffset_;
    
    ereport(DEBUG1, (errmsg("DPA: Processing batch: offset=%d, outRows=%d, total=%d",
                           offset, outRows, daeProbeOutputTotalRows_)));
    
    /* Update offset for next batch */
    daeProbeOutputOffset_ += outRows;
    daeProbeHasPendingRows_ = (daeProbeOutputOffset_ < daeProbeOutputTotalRows_);

    for (uint32 i = 0; i < daeReq_.out_key_cols_num; i++) {
        /* Use m_keyIdxInCell to get the correct index in m_scanBatch */
        int scanBatchIdx = m_keyIdxInCell[i];
        
        /* Bounds check for m_scanBatch */
        if (scanBatchIdx < 0 || scanBatchIdx >= m_scanBatch->m_cols) {
            ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("DPA Probe: key scanBatchIdx=%d out of bounds (m_scanBatch->m_cols=%d, i=%u)",
                        scanBatchIdx, m_scanBatch->m_cols, i)));
            return;
        }
        
        ScalarVector* p_vector = &m_scanBatch->m_arr[scanBatchIdx];
        
        /* Update column descriptor with saved type information */
        const Oid colType = daeKeyColTypes_[i];
        const int4 typeMod = daeKeyColTypeMods_[i];
        p_vector->m_desc.typeId = colType;
        p_vector->m_desc.typeMod = typeMod;
        p_vector->m_desc.encoded = COL_IS_ENCODE(colType);
        
        ScalarValue* vals = p_vector->m_vals;
        uint8* flags = p_vector->m_flag;
        /* Apply offset to read from correct position in DPA buffer */
        uint8* empty = daeReq_.out_key_cols[i].empty + offset;
        
        if (colType == BPCHAROID) {
            int dataLen = (typeMod > (int32)VARHDRSZ) ?
                           (typeMod - VARHDRSZ) : 0;
            if (dataLen <= 0) {
                ereport(ERROR, (errmsg("DPA: Invalid BPCHAR typeMod: %d", typeMod)));
                return;
            }
            char* srcData = (char *)daeReq_.out_key_cols[i].value + offset * dataLen;
            DaeProbeOutputBpchar(vals, flags, srcData, empty, outRows, dataLen, false);
        } else if (colType == VARCHAROID) {
            char* base_value = (char *)daeReq_.out_key_cols[i].value;
            __u32* src_offset = daeReq_.out_key_cols[i].offset + offset;
            DaeProbeOutputVarchar(vals, flags, base_value, src_offset, empty, outRows, false);
        } else if (colType == INT4OID) {
            int32* srcData = (int32 *)daeReq_.out_key_cols[i].value + offset;
            DaeProbeOutputInt4(vals, flags, srcData, empty, outRows, false);
        } else if (colType == INT8OID) {
            int64* srcData = (int64 *)daeReq_.out_key_cols[i].value + offset;
            DaeProbeOutputInt8(vals, flags, srcData, empty, outRows, false);
        } else if (colType == NUMERICOID) {
            // Extract scale from typeMod: typmod = ((precision << 16) | scale) + VARHDRSZ
            int scale = (typeMod > (int32)VARHDRSZ) ? ((typeMod - VARHDRSZ) & DPA_NUMERIC_SCALE_MASK) : 0;
            __int128* srcData = (__int128 *)daeReq_.out_key_cols[i].value + offset;
            DaeProbeOutputNumeric(vals, flags, srcData, empty, outRows, false, scale);
        } else if (colType == DATEOID) {
            int32* srcData = (int32 *)daeReq_.out_key_cols[i].value + offset;
            DaeProbeOutputInt4(vals, flags, srcData, empty, outRows, false);
        } else if (colType == TIMESTAMPOID) {
            int64* srcData = (int64 *)daeReq_.out_key_cols[i].value + offset;
            DaeProbeOutputInt8(vals, flags, srcData, empty, outRows, false);
        } else {
            elog(ERROR, "DPA: unsupported key column type %u", colType);
        }
        p_vector->m_rows = outRows;
    }

    // Process regular aggregation columns (excluding COUNT(*) if present)
    uint32 regular_agg_out_count = daeReq_.out_agg_cols_num - (hasCountStar_ ? 1 : 0);
    
    for (uint32 i = 0; i < m_aggNum; i++) {
        // pervecagg[i] corresponds to aggInfo[m_aggNum - 1 - i]
        int aggInfo_idx = m_aggNum - 1 - i;
        
        // Skip COUNT(*) in regular processing - will handle it separately
        if (hasCountStar_ && aggInfo_idx == countStarAggInfoIdx_) {
            ereport(DEBUG1, (errmsg("DPA: Skipping COUNT(*) at pervecagg[%d] in probe", i)));
            continue;
        }
        
        /*
         * Agg columns: reverse order to match ProducerBatch projection expectations
         * out_agg_cols[0] (pervecagg[0]) fills m_scanBatch[m_cellVarLen + m_aggNum - 1]
         * out_agg_cols[1] (pervecagg[1]) fills m_scanBatch[m_cellVarLen + m_aggNum - 2]
         */
        int scanBatchIdx = m_cellVarLen + m_aggNum - 1 - i;
        
        /* Bounds check for m_scanBatch agg column */
        if (scanBatchIdx < 0 || scanBatchIdx >= m_scanBatch->m_cols) {
            ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
					errmsg("DPA Probe: agg scanBatchIdx=%d out of bounds "
						"(m_scanBatch->m_cols=%d, m_cellVarLen=%d, m_aggNum=%u, i=%u)",
                       	scanBatchIdx, m_scanBatch->m_cols, m_cellVarLen, m_aggNum, i)));
            return;
        }
        
        ScalarVector* p_vector = &m_scanBatch->m_arr[scanBatchIdx];
        
        /*
         * Get the actual output type from aggInfo (which is in SELECT order)
         */
        const Oid colType = m_runtime->aggInfo[aggInfo_idx].vec_agg_function->flinfo->fn_rettype;
        
        /* Update type descriptor to match actual data type */
        p_vector->m_desc.typeId = colType;
        p_vector->m_desc.encoded = COL_IS_ENCODE(colType);
        
        ScalarValue* vals = p_vector->m_vals;
        uint8* flags = p_vector->m_flag;
        
        // Check if this is an AVG operation
        bool isAvg = (isAvgOperation_ != nullptr && aggInfo_idx < m_aggNum && isAvgOperation_[aggInfo_idx]);

        if (isAvg) {
            // AVG operation: compute SUM / COUNT from DPA outputs
            // Note: openGauss stores AVG state as {COUNT, SUM}
            ereport(DEBUG1, (errmsg("DPA: Processing AVG for aggInfo[%u]", aggInfo_idx)));
            
            int countIdx = avgCountOutputIdx_[aggInfo_idx];
            int sumIdx = avgSumOutputIdx_[aggInfo_idx];
            
            if (sumIdx < 0 || countIdx < 0) {
                ereport(ERROR, (errmsg("DPA: Invalid AVG output indices for aggInfo[%u]: count=%d, sum=%d",
                                      aggInfo_idx, countIdx, sumIdx)));
                return;
            }
            
            /* Apply offset to read from correct position in DPA buffer */
            uint8* count_empty = daeReq_.out_agg_cols[countIdx].empty + offset;
            uint8* sum_empty = daeReq_.out_agg_cols[sumIdx].empty + offset;
            
            // Get the SUM data type to determine how to read it
            // SUM is at index 1 in output_data_types (COUNT at 0, SUM at 1)
            // Use mapping to access setup.agg_cols_info with correct index
            int setupIdx = pervecaggToRegularIdx_[i];
            if (setupIdx < 0) {
                ereport(ERROR, (errmsg("DPA: Invalid mapping for pervecagg[%u] in AVG processing", i)));
                return;
            }
            wd_dae_data_type sumDataType = daeAggCols_[setupIdx].output_data_types[1];
            
            /* Get base pointers with offset applied */
            int64* count_values = (int64 *)daeReq_.out_agg_cols[countIdx].value + offset;
            
            int scale = DaeResolveNumericScale(daeAggColScaleSet_, daeAggColActualScales_,
                                                daeAggColTypeMods_, i);
            if (!DaeProbeComputeAvgRows(vals, flags, outRows, count_values, count_empty,
                                         sumDataType, &daeReq_.out_agg_cols[sumIdx], offset, scale)) {
                ereport(ERROR, (errmsg("DPA: Unsupported SUM data type %u for AVG", sumDataType)));
                return;
            }
            
            p_vector->m_rows = outRows;

            ereport(DEBUG1, (errmsg("DPA: AVG computed for %d rows", outRows)));
            continue;
        }
        
        // Normal (non-AVG) aggregation processing
        int setupIdx = pervecaggToRegularIdx_[i];
        if (setupIdx < 0) {
            ereport(ERROR, (errmsg("DPA: Invalid mapping for pervecagg[%u] in normal agg processing", i)));
            return;
        }
        
        int daeAggIdx = 0;
        for (int s = 0; s < setupIdx; s++) {
            daeAggIdx += daeAggCols_[s].col_alg_num;
        }
        
        /* Apply offset to read from correct position in DPA buffer */
        uint8* empty = daeReq_.out_agg_cols[daeAggIdx].empty + offset;
        
        bool is_count = (daeAggCols_ && (uint32)setupIdx < regular_agg_out_count &&
                        daeAggCols_[setupIdx].output_col_algs[0] == WD_AGG_COUNT);

        if (colType == BPCHAROID) {
            int dataLen = (p_vector->m_desc.typeMod > (int32)VARHDRSZ) ?
                           (p_vector->m_desc.typeMod - VARHDRSZ) : 0;
            if (dataLen <= 0) {
                ereport(ERROR, (errmsg("DPA: Invalid BPCHAR typeMod: %d", p_vector->m_desc.typeMod)));
                return;
            }
            char* srcData = (char *)daeReq_.out_agg_cols[daeAggIdx].value + offset * dataLen;
            DaeProbeOutputBpchar(vals, flags, srcData, empty, outRows, dataLen, is_count);
        } else if (colType == VARCHAROID) {
            char* base_value = (char *)daeReq_.out_agg_cols[daeAggIdx].value;
            __u32* src_offset = daeReq_.out_agg_cols[daeAggIdx].offset + offset;
            DaeProbeOutputVarchar(vals, flags, base_value, src_offset, empty, outRows, is_count);
        } else if (colType == INT4OID) {
            int32* srcData = (int32 *)daeReq_.out_agg_cols[daeAggIdx].value + offset;
            DaeProbeOutputInt4(vals, flags, srcData, empty, outRows, is_count);
        } else if (colType == INT8OID) {
            int64* srcData = (int64 *)daeReq_.out_agg_cols[daeAggIdx].value + offset;
            DaeProbeOutputInt8(vals, flags, srcData, empty, outRows, is_count);
        } else if (colType == NUMERICOID) {
            // Check actual storage type in UADK
            // SUM(INT8) outputs NUMERIC but stores as WD_DAE_LONG (int64)
            // SUM(NUMERIC) outputs NUMERIC and stores as WD_DAE_LONG_DECIMAL (int128)
            // Use mapping to access setup.agg_cols_info with correct index
            wd_dae_data_type actualType = daeAggCols_[setupIdx].output_data_types[0];
            
            if (actualType == WD_DAE_LONG) {
                int64* srcData = (int64 *)daeReq_.out_agg_cols[daeAggIdx].value + offset;
                DaeProbeOutputInt8AsNumeric(vals, flags, srcData, empty, outRows, is_count);
            } else if (actualType == WD_DAE_LONG_DECIMAL) {
                int scale = DaeResolveNumericScale(daeAggColScaleSet_, daeAggColActualScales_,
                                                   daeAggColTypeMods_, i);
                __int128* srcData = (__int128 *)daeReq_.out_agg_cols[daeAggIdx].value + offset;
                DaeProbeOutputNumeric(vals, flags, srcData, empty, outRows, is_count, scale);
            } else {
                elog(ERROR, "DPA: Unexpected NUMERIC storage type: %u", actualType);
            }
        } else if (colType == DATEOID) {
            int32* srcData = (int32 *)daeReq_.out_agg_cols[daeAggIdx].value + offset;
            DaeProbeOutputInt4(vals, flags, srcData, empty, outRows, is_count);
        } else if (colType == TIMESTAMPOID) {
            int64* srcData = (int64 *)daeReq_.out_agg_cols[daeAggIdx].value + offset;
            DaeProbeOutputInt8(vals, flags, srcData, empty, outRows, is_count);
        } else {
            elog(ERROR, "DPA: unsupported aggregation column type %u", colType);
        }
        p_vector->m_rows = outRows;
    }
    
    /*
     * Handle COUNT(*) separately if present
     *
     * COUNT(*) characteristics:
     * 1. No input column (not processed in ParallelBuildAggValue)
     * 2. DPA output is at the last position in out_agg_cols
     * 3. Result fills m_scanBatch at its original aggInfo position
     * 4. Always returns INT8, never NULL
     */
    if (hasCountStar_) {
        // DPA output index: COUNT(*) is always at the end
        uint32 countStarDpaIdx = daeReq_.out_agg_cols_num - 1;
        
        // scanBatch index: direct calculation from aggInfo index
        // Aggregation columns: scanBatch[m_cellVarLen + aggInfo_idx]
        int scanBatchIdx = m_cellVarLen + countStarAggInfoIdx_;
        
        /* Bounds check for COUNT(*) scanBatchIdx */
        if (scanBatchIdx < 0 || scanBatchIdx >= m_scanBatch->m_cols) {
            ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 	errmsg("DPA Probe: COUNT(*) scanBatchIdx=%d out of bounds "
						"(m_scanBatch->m_cols=%d, m_cellVarLen=%d, countStarAggInfoIdx_=%d)",
                        	scanBatchIdx, m_scanBatch->m_cols, m_cellVarLen, countStarAggInfoIdx_)));
            return;
        }
        
        ereport(DEBUG1,
            (errmsg("DPA: COUNT(*) - aggInfo[%d] → scanBatch[%d], DPA out[%u]",
                    countStarAggInfoIdx_, scanBatchIdx, countStarDpaIdx)));
        
        // Setup output vector
        ScalarVector* p_vector = &m_scanBatch->m_arr[scanBatchIdx];
        p_vector->m_desc.typeId = INT8OID;  // COUNT(*) always returns INT8
        p_vector->m_desc.encoded = COL_IS_ENCODE(INT8OID);
        
        ScalarValue* vals = p_vector->m_vals;
        uint8* flags = p_vector->m_flag;
        
        /* Apply offset to read from correct position in DPA buffer */
        int64* srcData = (int64 *)daeReq_.out_agg_cols[countStarDpaIdx].value + offset;
        uint8* src_empty = daeReq_.out_agg_cols[countStarDpaIdx].empty + offset;
        
        // Output COUNT(*) result (always non-NULL)
        DaeProbeOutputInt8(vals, flags, srcData, src_empty, outRows,
                          true);  // forceNotNull = true (COUNT(*) never returns NULL)
        
        p_vector->m_rows = outRows;
    }

    m_scanBatch->m_rows = outRows;
}

bool HashAggRunner::DaeIsSessionReady()
{
    return daeSessionState_ == DAE_SESS_READY;
}

bool HashAggRunner::DaeSessionInit(VectorBatch* batch)
{
    /* 1. basic check */
    if (daeSessionState_ == DAE_SESS_READY) {
        ereport(WARNING, (errmsg("DPA: Session already initialized")));
        return true;
    }
    
    if (daeSessionState_ == DAE_SESS_INITIALIZING) {
        ereport(WARNING, (errmsg("DPA: Session is being initialized")));
        return false;
    }
    
    daeSessionState_ = DAE_SESS_INITIALIZING;
    daeKeyColsNum_ = m_key;
    daeAggColsNum_ = m_aggNum;
    
    if (daeKeyColsNum_ > DPA_MAX_KEY_COLS) {
        ereport(WARNING,
            (errmsg("DPA: Key columns number %u exceeds hardware limit %d, fallback to CPU",
                    daeKeyColsNum_, DPA_MAX_KEY_COLS)));
        daeSessionState_ = DAE_SESS_UNINIT;
        return false;
    }

    if (daeAggColsNum_ > DPA_MAX_INPUT_COLS) {
        ereport(WARNING,
            (errmsg("DPA: Aggregation columns number %u exceeds hardware limit %d, fallback to CPU",
                    daeAggColsNum_, DPA_MAX_INPUT_COLS)));
        daeSessionState_ = DAE_SESS_UNINIT;
        return false;
    }

    /*
     * daeAggColsNum_ == 0 means this VecHashAgg has no aggregation functions
     * at all (e.g. a dedup/distinct node generated for EXISTS subquery processing).
     * DPA is designed for aggregation; without any agg functions (not even COUNT(*)),
     * passing agg_cols_num=0 and is_count_all=0 to UadkAggAllocSess causes UADK
     * to log "invalid: agg input cols is NULL, num: 0".  Fall back to CPU early.
     */
    if (daeAggColsNum_ == 0) {
        ereport(DEBUG1, (errmsg("DPA: No aggregation functions (dedup/distinct node), fallback to CPU")));
        daeSessionState_ = DAE_SESS_UNINIT;
        return false;
    }

    if (daeKeyColsNum_ == 0) {
        ereport(WARNING, (errmsg("DPA: No key columns for GROUP BY, fallback to CPU")));
        daeSessionState_ = DAE_SESS_UNINIT;
        return false;
    }

    /* 2. allocate arrays */
    MemoryContext oldContext = MemoryContextSwitchTo(m_dpaContext);
    daeKeyCols_ = (struct wd_key_col_info*)palloc0(daeKeyColsNum_ * sizeof(struct wd_key_col_info));
    daeAggCols_ = (struct wd_agg_col_info*)palloc0(daeAggColsNum_ * sizeof(struct wd_agg_col_info));
    daeKeyColTypes_ = (Oid*)palloc0(daeKeyColsNum_ * sizeof(Oid));
    daeKeyColTypeMods_ = (int4*)palloc0(daeKeyColsNum_ * sizeof(int4));
    daeAggColTypeMods_ = (int4*)palloc0(daeAggColsNum_ * sizeof(int4));
    // Allocate arrays for tracking actual scales from input data
    daeAggColActualScales_ = (int*)palloc0(daeAggColsNum_ * sizeof(int));
    daeAggColScaleSet_ = (bool*)palloc0(daeAggColsNum_ * sizeof(bool));
    // Allocate AVG tracking arrays
    isAvgOperation_ = (bool*)palloc0(daeAggColsNum_ * sizeof(bool));
    avgSumOutputIdx_ = (int*)palloc0(daeAggColsNum_ * sizeof(int));
    avgCountOutputIdx_ = (int*)palloc0(daeAggColsNum_ * sizeof(int));
    pervecaggToRegularIdx_ = (int*)palloc0(daeAggColsNum_ * sizeof(int));
    (void)MemoryContextSwitchTo(oldContext);

    errno_t rc;
    rc = memset_s(avgSumOutputIdx_, daeAggColsNum_ * sizeof(int), 0xFF, daeAggColsNum_ * sizeof(int));
    securec_check(rc, "\0", "\0");
    rc = memset_s(avgCountOutputIdx_, daeAggColsNum_ * sizeof(int), 0xFF, daeAggColsNum_ * sizeof(int));
    securec_check(rc, "\0", "\0");
    rc = memset_s(pervecaggToRegularIdx_, daeAggColsNum_ * sizeof(int), 0xFF, daeAggColsNum_ * sizeof(int));
    securec_check(rc, "\0", "\0");

    struct wd_agg_sess_setup setup;
    setup.key_cols_num = daeKeyColsNum_;
    setup.key_cols_info = daeKeyCols_;
    setup.agg_cols_num = daeAggColsNum_;
    setup.agg_cols_info = daeAggCols_;

    /* 3. collect key col info */
    uint32 char_col_count = 0;
    for (uint32 i = 0; i < setup.key_cols_num; i++) {
        const Oid colType = batch->m_arr[m_keyIdx[i]].m_desc.typeId;
        const int4 typeMod = batch->m_arr[m_keyIdx[i]].m_desc.typeMod;
        
        /* Save type information for probe output processing */
        daeKeyColTypes_[i] = colType;
        daeKeyColTypeMods_[i] = typeMod;
        if (colType == BPCHAROID || colType == VARCHAROID) {
            char_col_count++;
        }

        if (char_col_count > DPA_MAX_CHAR_COL_NUM) {
            DaeSessionCleanup();
            ereport(WARNING,
                (errmsg("DPA: Key CHAR/VARCHAR columns number %u exceeds hardware limit %d, fallback to CPU",
                        char_col_count, DPA_MAX_CHAR_COL_NUM)));
            return false;
        }

        if (!DaeCollectKeyCollInfo(setup.key_cols_info, colType, i, typeMod)) {
            DaeSessionCleanup();
            ereport(WARNING, (errmsg("DPA: Failed to collect key column info, index: %u", i)));
            return false;
        }
    }

    /*
     * 3. collect agg col info
     * Phase 1: Detect COUNT(*) and count regular aggregations
     *
     * Why we need this pass:
     * 1. UADK requires accurate agg_cols_num (excluding COUNT(*))
     * 2. COUNT(*) is handled separately by is_count_all flag
     * 3. Regular aggregations need contiguous indices (0, 1, 2, ...)
     *
     * COUNT(*) is special because:
     * - No input column (evalproj = NULL)
     * - Hardware directly counts rows, doesn't need input data
     * - Output is always at the last position in out_agg_cols
     */
    DetectCountStarResult detectResult = DetectCountStarInAggregations(m_aggNum, m_runtime);
    if (detectResult.countStarCount > 1) {
        DaeSessionCleanup();
        ereport(WARNING,
            (errmsg("DPA: Multiple COUNT(*) (%u occurrences) detected. "
                    "UADK hardware only supports single COUNT(*). "
                    "Falling back to native implementation.", detectResult.countStarCount)));
        return false;
    }
    
    hasCountStar_ = (detectResult.countStarCount == 1);
    countStarAggInfoIdx_ = detectResult.countStarAggInfoIdx;
    uint32 regular_agg_count = detectResult.regularAggCount;
    if (hasCountStar_) {
        setup.agg_cols_num = regular_agg_count;
        if (regular_agg_count == 0) {
            /*
             * Pure COUNT(*) with no regular aggregations (e.g. Q4).
             * Keep setup.agg_cols_info pointing to the already-allocated daeAggCols_
             * buffer (palloc0'd above). UADK validates that agg_cols_info != NULL
             * unconditionally — passing NULL here causes:
             *   "invalid: agg input cols is NULL, num: 0"
             * agg_cols_num=0 ensures the library will not dereference the pointer.
             */
            ereport(DEBUG1, (errmsg("DPA: Only COUNT(*), no regular aggregations")));
        }
        ereport(DEBUG1, (errmsg("DPA: COUNT(*) detected, %u regular aggregations", regular_agg_count)));
    }
    
    /*
     * 3. collect agg col info
     * Phase 2: Process regular aggregations (skip COUNT(*))
     *
     * Build mapping: pervecagg index → regular index
     * This mapping is used in ParallelBuildAggValue and DaeParallelProbe
     *
     * Note: COUNT(*) has no input column and doesn't need setup
     */
    ereport(DEBUG1, (errmsg("DPA: Phase 2 - Processing %u regular aggregations", regular_agg_count)));

    uint32 regular_idx = 0;  // Contiguous index for regular aggregations (0, 1, 2, ...)
    for (uint32 i = 0; i < m_aggNum; i++) {
        const Oid aggFuncOid = m_runtime->aggInfo[i].vec_agg_function->flinfo->fn_oid;
        int pervecagg_idx = m_aggNum - 1 - i;
        
        if (IsCountStar(aggFuncOid, &m_runtime->pervecagg[pervecagg_idx])) {
            ereport(DEBUG1, (errmsg("DPA: Phase 2 - Skipping COUNT(*) at aggInfo[%u]", i)));
            continue;
        }
        
        /*
         * aggInfo[i] corresponds to pervecagg[m_aggNum - 1 - i] as per vecagg.cpp:686-691
         * We need to fill agg_cols_info in pervecagg order to match ParallelBuildAggValue
         * Note: pervecagg_idx already calculated above
         */
        
        // Check if this is an AVG operation
        bool isAvg = false;
        wd_agg_alg aggAlg = MapAggFuncOidToAlg(aggFuncOid, &isAvg);
        if (aggAlg == WD_AGG_ALG_TYPE_MAX) {
            DaeSessionCleanup();
            ereport(WARNING, (errmsg("DPA: Unsupported agg function OID: %u, fallback to CPU", aggFuncOid)));
            return false;
        }
        
        isAvgOperation_[i] = isAvg;
        if (m_runtime->pervecagg[pervecagg_idx].evalproj == NULL) {
            DaeSessionCleanup();
            ereport(WARNING, (errmsg("DPA: evalproj is NULL for pervecagg[%d], aggInfo[%u], OID %u",
                pervecagg_idx, i, aggFuncOid)));
            return false;
        }
        
        const Oid inputColType = m_runtime->pervecagg[pervecagg_idx].evalproj->pi_batch->m_arr->m_desc.typeId;
        const int4 inputTypeMod = m_runtime->pervecagg[pervecagg_idx].evalproj->pi_batch->m_arr->m_desc.typeMod;
        const Oid outputColType = m_runtime->aggInfo[i].vec_agg_function->flinfo->fn_rettype;
        
        // CRITICAL FIX: Use regular_idx for setup.agg_cols_info (UADK expects contiguous 0,1,2...)
        // But use pervecagg_idx for daeAggColTypeMods_ (Probe uses pervecagg order)
        if (!DaeCollectAggCollInfo(setup.agg_cols_info, inputColType, outputColType, regular_idx, inputTypeMod,
            aggFuncOid)) {
            DaeSessionCleanup();
            ereport(WARNING, (errmsg("DPA: Failed to collect agg column info, regular_idx: %u, pervecagg_idx: %u",
                regular_idx, pervecagg_idx)));
            return false;
        }
        
        // Store typeMod in pervecagg order to match DaeParallelProbe loop
        // DaeParallelProbe iterates in pervecagg order (i=0 is pervecagg[0])
        daeAggColTypeMods_[pervecagg_idx] = inputTypeMod;
        
        // Store mapping: pervecagg_idx → regular_idx
        pervecaggToRegularIdx_[pervecagg_idx] = regular_idx;
        
        ereport(DEBUG1,
            (errmsg("DPA: Second pass[%u] - regular_idx=%u, pervecagg_idx=%d, OID=%u, inputType=%u, mapping[%d]=%u",
                i, regular_idx, pervecagg_idx, aggFuncOid, inputColType, pervecagg_idx, regular_idx)));
        regular_idx++;
    }
    
    setup.sched_param = nullptr;
    setup.is_count_all = hasCountStar_;
    setup.count_all_data_type = WD_DAE_LONG;
    
    ereport(DEBUG1, (errmsg(
            "DPA: Before UadkAggAllocSess - agg_cols_num=%u, agg_cols_info=%p, is_count_all=%d, key_cols_num=%u",
            setup.agg_cols_num, setup.agg_cols_info, setup.is_count_all, setup.key_cols_num)));
    
    daeSess_ = UadkAggAllocSess(&setup);
    if (!daeSess_) {
        DaeSessionCleanup();
        ereport(WARNING, (errmsg("DPA: Failed to allocate DAE session")));
        return false;
    }
    
    __u32 row_size = UadkAggGetTableRowsize(daeSess_);
    if (row_size <= 0) {
        DaeSessionCleanup();
        ereport(WARNING, (errmsg("DPA: Invalid hash table row size: %u", row_size)));
        return false;
    }
    
    /* Get estimated number of groups from optimizer for hash table sizing */
    VecAgg* agg_plan = (VecAgg*)m_runtime->ss.ps.plan;
    __u32 estimated_groups = (agg_plan && agg_plan->numGroups > 0) ? (__u32)agg_plan->numGroups : 0;
    
    ereport(DEBUG1,
        (errmsg("DPA: Allocating hash table with estimated_groups=%u from optimizer", estimated_groups)));
    
    DaeAllocHTBL(NULL, &daeHashTable_, row_size, estimated_groups);
    if (!daeHashTable_.std_table || !daeHashTable_.ext_table) {
        DaeSessionCleanup();
        ereport(WARNING, (errmsg("DPA: Failed to allocate hash table")));
        return false;
    }
    
    ereport(DEBUG1,
        (errmsg("DPA: Hash table allocated: std_rows=%u, ext_rows=%u",
                daeHashTable_.std_table_row_num, daeHashTable_.ext_table_row_num)));
    
    int ret = UadkAggSetHashTable(daeSess_, &daeHashTable_);
    if (ret != 0) {
        DaeSessionCleanup();
        ereport(WARNING, (errmsg("DPA: Failed to set hash table, ret: %d", ret)));
        return false;
    }
    
    if (DaeInitInputOutputMem(&setup, batch)) {
        DaeSessionCleanup();
        ereport(WARNING, (errmsg("DPA: Failed to initialize input/output memory")));
        return false;
    }
    
    daeSessionState_ = DAE_SESS_READY;
    ereport(DEBUG1,
        (errmsg("DPA: Session initialized successfully, key_cols: %u, agg_cols: %u, row_size: %u",
                daeKeyColsNum_, daeAggColsNum_, row_size)));
    return true;
}

void HashAggRunner::DaeSessionCleanup()
{
    if (daeSessionState_ == DAE_SESS_UNINIT) {
        return;
    }
    
    ereport(DEBUG1, (errmsg("DPA: Cleaning up DAE session")));
    
    DaeFreeInputOutputMem();
    
    if (daeHashTable_.std_table || daeHashTable_.ext_table) {
        DaeFreeHTBL(&daeHashTable_);
        errno_t rc = memset_s(&daeHashTable_, sizeof(daeHashTable_), 0, sizeof(daeHashTable_));
        securec_check_c(rc, "\0", "\0");
    }
    
    if (daeSess_) {
        UadkAggFreeSess(daeSess_);
        daeSess_ = 0;
    }
    
    if (daeKeyCols_) {
        pfree(daeKeyCols_);
        daeKeyCols_ = nullptr;
    }
    
    if (daeAggCols_) {
        pfree(daeAggCols_);
        daeAggCols_ = nullptr;
    }

    if (daeKeyColTypes_) {
        pfree(daeKeyColTypes_);
        daeKeyColTypes_ = nullptr;
    }

    if (daeKeyColTypeMods_) {
        pfree(daeKeyColTypeMods_);
        daeKeyColTypeMods_ = nullptr;
    }

    if (daeAggColTypeMods_) {
        pfree(daeAggColTypeMods_);
        daeAggColTypeMods_ = nullptr;
    }

    if (daeAggColActualScales_) {
        pfree(daeAggColActualScales_);
        daeAggColActualScales_ = nullptr;
    }

    if (daeAggColScaleSet_) {
        pfree(daeAggColScaleSet_);
        daeAggColScaleSet_ = nullptr;
    }

    if (isAvgOperation_) {
        pfree(isAvgOperation_);
        isAvgOperation_ = nullptr;
    }

    if (avgSumOutputIdx_) {
        pfree(avgSumOutputIdx_);
        avgSumOutputIdx_ = nullptr;
    }

    if (avgCountOutputIdx_) {
        pfree(avgCountOutputIdx_);
        avgCountOutputIdx_ = nullptr;
    }

    if (pervecaggToRegularIdx_) {
        pfree(pervecaggToRegularIdx_);
        pervecaggToRegularIdx_ = nullptr;
    }

    daeKeyColsNum_ = 0;
    daeAggColsNum_ = 0;
    daeSessionState_ = DAE_SESS_UNINIT;
}