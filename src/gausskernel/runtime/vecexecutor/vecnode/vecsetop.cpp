/* ---------------------------------------------------------------------------------------
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * vecsetop.cpp
 *
 *
 * IDENTIFICATION
 *        Code/src/gausskernel/runtime/vecexecutor/vecnode/vecsetop.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "commands/explain.h"
#include "executor/executor.h"
#include "executor/node/nodeSetOp.h"
#include "vecexecutor/vecexecutor.h"
#include "vecexecutor/vecsetop.h"
#include "utils/dynahash.h"
#include "utils/memutils.h"
#include "miscadmin.h"
#include "optimizer/cost.h"
#include "pgxc/pgxc.h"

static int fetch_flag(VectorBatch* batch, int col, int row);

static void init_pergroup(SetOpHashCell* cell, bool first_flag);

static void advance_pergroup(SetOpHashCell* cell, bool first_flag);

static int cell_get_output_count(VecSetOpStatePerGroupData* pergroup, int cmd);

VecSetOpState* ExecInitVecSetOp(VecSetOp* node, EState* estate, int eflags)
{
    VecSetOpState* set_op_state = NULL;
    /* check for unsupported flags */
    Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

    /*
     * create state structure
     */
    set_op_state = makeNode(VecSetOpState);
    set_op_state->ps.plan = (Plan*)node;
    set_op_state->ps.state = estate;
    set_op_state->ps.vectorized = true;
    set_op_state->eqfunctions = NULL;
    set_op_state->hashfunctions = NULL;
    set_op_state->setop_done = false;
    set_op_state->numOutput = 0;
    set_op_state->pergroup = NULL;
    set_op_state->grp_firstTuple = NULL;
    set_op_state->hashtable = NULL;
    set_op_state->tableContext = NULL;

    /*
     * Tuple table initialization
     */
    ExecInitResultTupleSlot(estate, &set_op_state->ps);

    /*
     * initialize child nodes
     *
     * If we are hashing then the child plan does not need to handle REWIND
     * efficiently; see ExecReScanSetOp.
     */
    if (node->strategy == SETOP_HASHED)
        eflags = (uint32)eflags & ~EXEC_FLAG_REWIND;
    outerPlanState(set_op_state) = ExecInitNode(outerPlan(node), estate, eflags);

    /*
     * setop nodes do no projections, so initialize projection info for this
     * node appropriately
     */
    ExecAssignResultTypeFromTL(
            &set_op_state->ps,
            ExecGetResultType(outerPlanState(set_op_state))->tdTableAmType);

    set_op_state->ps.ps_ProjInfo = NULL;

    /*
     * Precompute fmgr lookup data for inner loop. We need both equality and
     * hashing functions to do it by hashing, but only equality if not
     * hashing.
     */
    if (node->strategy == SETOP_HASHED)
        execTuplesHashPrepare(node->numCols, node->dupOperators, &set_op_state->eqfunctions, &set_op_state->hashfunctions);
    else
        set_op_state->eqfunctions = execTuplesMatchPrepare(node->numCols, node->dupOperators);

    set_op_state->vecSetOpInfo = NULL;
    if (node->strategy == SETOP_HASHED) {
        set_op_state->table_filled = false;
    }

    return set_op_state;
}

VectorBatch* ExecVecSetOp(VecSetOpState* node)
{
    /* we're done if we are out of groups */
    if (node->setop_done)
        return NULL;

    if (node->vecSetOpInfo == NULL)
        node->vecSetOpInfo = New(CurrentMemoryContext) setOpTbl(node);
    return DO_SETOPERATION(node->vecSetOpInfo);
}

void ExecEndVecSetOp(VecSetOpState* node)
{
    setOpTbl* set_op_tbl = NULL;
    set_op_tbl = (setOpTbl*)node->vecSetOpInfo;
    if (set_op_tbl != NULL) {
        set_op_tbl->closeFile();
        set_op_tbl->freeMemoryContext();
    }

    (void)ExecClearTuple(node->ps.ps_ResultTupleSlot);
    ExecEndNode(outerPlanState(node));
}

void ExecReScanVecSetOp(VecSetOpState* node)
{
    (void)ExecClearTuple(node->ps.ps_ResultTupleSlot);
    node->setop_done = false;
    setOpTbl* tb = (setOpTbl*)node->vecSetOpInfo;
    if (tb != NULL) {
        tb->ResetNecessary(node);
    }
}

void setOpTbl::ResetNecessary(VecSetOpState* node)
{
    m_statusLog.restore = false;
    m_statusLog.numOutput = 0;
    m_statusLog.lastIdx = 0;
    m_statusLog.lastCell = NULL;

    if (((VecSetOp*)node->ps.plan)->strategy == SETOP_HASHED) {
        /*
         * If we do have the hash table and the subplan does not have any
         * parameter changes, then we can just rescan the existing hash table;
         * no need to build it again.
         */
        if (m_spillToDisk == false && node->ps.lefttree->chgParam == NULL) {
            m_runState = SETOP_FETCH;
            return;
        }

        /*
         * Temp files may have already been released, must close temp files
         */
        if (m_spillToDisk)
            closeFile();

        /* And rebuild empty hashtable if needed */
        m_runState = SETOP_PREPARE;
        MemoryContextResetAndDeleteChildren(m_hashContext);

        {
            AutoContextSwitch auto_guard(m_hashContext);
            m_setOpHashData = (SetOpHashCell**)palloc0(m_size * sizeof(SetOpHashCell*));
        }

        m_rows = 0;
        m_spillToDisk = false;
    } else {
        m_lastBatch = NULL;
    }

    /*
     * if chgParam of subnode is not null then plan will be re-scanned by
     * first ExecProcNode.
     */
    if (node->ps.lefttree->chgParam == NULL)
        VecExecReScan(node->ps.lefttree);
}

setOpTbl::setOpTbl(VecSetOpState* runtime) : m_runtime(runtime)
{
    VecSetOp* node = (VecSetOp*)(runtime->ps.plan);
    int i = 0;
    Plan* left_plan = outerPlan(node);
    AttrNumber* dum_col_idx = node->dupColIdx;
    m_totalMem = SET_NODEMEM(node->plan.operatorMemKB[0], node->plan.dop) * 1024L;

    m_tmpContext = AllocSetContextCreate(CurrentMemoryContext,
        "TmpHashContext",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);

    m_hashContext = AllocSetContextCreate(CurrentMemoryContext,
        "HashCacheContext",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE,
        STANDARD_CONTEXT,
        m_totalMem);

    m_keySimple = true;
    m_key = node->numCols;
    m_outerColNum = ExecTargetListLength(left_plan->targetlist);
    m_cols = m_outerColNum;
    m_cellSize = offsetof(SetOpHashCell, m_val) + m_outerColNum * sizeof(hashVal);
    m_junkCol = node->flagColIdx;
    m_cmd = node->cmd;
    m_eqfunctions = runtime->eqfunctions;
    m_outerHashFuncs = runtime->hashfunctions;
    /* indexs of the columns to check for duplicate-ness in the target list */
    m_keyIdx = (int*)palloc(sizeof(int) * m_key);

    m_rows = 0;
    m_size = 0;
    m_filesource = NULL;
    m_hashSource = NULL;
    m_setOpHashData = NULL;
    m_BuildFun = NULL;
    m_lastBatch = NULL;
    m_grow_threshold = 0;
    m_runState = 0;
    m_can_grow = false;
    m_hashSource = NULL;
    m_max_hashsize = 0;

    for (i = 0; i < m_key; i++) {
        m_keyIdx[i] = dum_col_idx[i] - 1;
    }

    TupleDesc desc = outerPlanState(runtime)->ps_ResultTupleSlot->tts_tupleDescriptor;
    Form_pg_attribute* attrs = desc->attrs;

    m_scanBatch = New(CurrentMemoryContext) VectorBatch(CurrentMemoryContext, desc);

    /* Initialize m_keyDesc in vechashtable and check if m_keySimple is false */
    m_keyDesc = (ScalarDesc*)palloc(m_outerColNum * sizeof(ScalarDesc));
    for (i = 0; i < m_key; i++) {
        m_keyDesc[i].typeId = attrs[m_keyIdx[i]]->atttypid;
        m_keyDesc[i].typeMod = attrs[m_keyIdx[i]]->atttypmod;
        m_keyDesc[i].encoded = COL_IS_ENCODE(m_keyDesc[i].typeId);
        if (m_keyDesc[i].encoded)
            m_keySimple = false;
    }

    m_keyDesc[m_outerColNum - 1].encoded = false;

    m_BuildScanBatch = &setOpTbl::BuildScanBatch;

    /* Initialize the data in m_statusLog which is used to store scene of last running */
    m_statusLog.restore = false;
    m_statusLog.numOutput = 0;
    m_statusLog.lastIdx = 0;
    m_statusLog.lastCell = NULL;
    m_firstFlag = node->firstFlag;

    /* Initialize the member only used in hash strategy/sort strategy */
    if (node->strategy == SETOP_HASHED) {
        Assert(m_firstFlag == 0 ||
               (m_firstFlag == 1 && (node->cmd == SETOPCMD_INTERSECT || node->cmd == SETOPCMD_INTERSECT_ALL)));

        m_max_hashsize = 0;
        m_can_grow = false;

        /* max hashsize  is MaxAllocSize */
        int hash_size = (int)Min(2 * node->numGroups, (long)MaxAllocSize);
        m_size = computeHashTableSize<false>(hash_size);

        {
            AutoContextSwitch mem_guard(m_hashContext);
            m_setOpHashData = (SetOpHashCell**)palloc0(m_size * sizeof(SetOpHashCell*));
        }

        m_runState = SETOP_PREPARE;
        m_strategy = HASH_IN_MEMORY;
        m_rows = 0;

        /* add one column to store the hash value.*/
        m_cellSize += sizeof(hashVal);

        /* Binding functions used in hash strategy */
        BindingFp();

        /* Binding the function point Operation in hashSetOpTbl to function setOpTbl::Run */
        setOpTbl::Operation = &setOpTbl::RunHash;
    } else {
        m_firstFlag = 0;
        m_lastBatch = NULL;
        if (m_keySimple) {
            setOpTbl::Operation = &setOpTbl::RunSort<true>;
        } else {
            setOpTbl::Operation = &setOpTbl::RunSort<false>;
        }
    }

    ReplaceEqfunc();

    m_tupleCount = m_colWidth = 0;
    m_sysBusy = false;
    if (node->plan.operatorMaxMem > node->plan.operatorMemKB[0])
        m_maxMem = SET_NODEMEM(node->plan.operatorMaxMem, node->plan.dop) * 1024L;
    else
        m_maxMem = 0;
    m_spreadNum = 0;

    if (m_runtime->ps.instrument) {
        m_runtime->ps.instrument->sorthashinfo.hashtable_expand_times = 0;
    }
}

template <bool simple>
VectorBatch* setOpTbl::RunSort()
{
    VectorBatch* outer_batch = NULL;
    SetOpHashCell* first_cell = NULL;
    int nrows = 0;
    int i = 0;
    int last_idx = 0;
    int first_flag = 0;
    int output_num = 0;
    bool found_match = false;

    m_scanBatch->Reset(true);
    /* If restore is true, read the information stored in m_statusLog, else read the next VectorBatch */
    if (m_statusLog.restore) {
        last_idx = m_statusLog.lastIdx;
        outer_batch = m_lastBatch;
        first_cell = m_statusLog.lastCell;
        output_num = m_statusLog.numOutput;
        m_statusLog.restore = false;
    } else {
        Assert(output_num == 0 && last_idx == 0);
        first_cell = (SetOpHashCell*)palloc0(m_cellSize);
        outer_batch = VectorEngine(outerPlanState(m_runtime));
    }

    while (!m_runtime->setop_done) {

        /*
         * If this is the restore process, outputNum could be more than zero. Then we need
         * to dump the firstCell, which is left by the last process when the scanBatch is full,
         * for the remaining times before Initializing a new firstCell.
         */
        if (output_num > 0 && !DumpOutput(first_cell, outer_batch, output_num, last_idx)) {
            break;
        }

        /*
         * When the outerBatch is NUll, it means all the vectorbatches have been scaned,
         * jump out of the while loop and return.
         */
        if (unlikely(BatchIsNull(outer_batch))) {
            m_runtime->setop_done = true;
            pfree_ext(first_cell);
            continue;
        }

        first_flag = fetch_flag(outer_batch, m_junkCol - 1, last_idx);
        InitCell<simple>(first_cell, outer_batch, last_idx, first_flag);
        last_idx++;

        for (;;) {
            nrows = outer_batch->m_rows;
            for (i = last_idx; i < nrows; i++) {
                first_flag = fetch_flag(outer_batch, m_junkCol - 1, i);
                found_match = MatchKey<simple>(outer_batch, i, first_cell);
                if (found_match) {
                    advance_pergroup(first_cell, first_flag == m_firstFlag);
                } else {
                    output_num = cell_get_output_count(&(first_cell->perGroup), m_cmd);
                    if (output_num > 0 && !DumpOutput(first_cell, outer_batch, output_num, i)) {
                        goto PRODUCE_BATCH;
                    }
                    InitCell<simple>(first_cell, outer_batch, i, first_flag);
                }
            }

            /*
             * Get the next batch. If it is NULL, calculate the outputNum for the last firstCell here
             * and break the loop, If not NULL, reset the lastIdx and continue the loop to check if
             * the next one matches key.
             */
            outer_batch = VectorEngine(outerPlanState(m_runtime));
            if (unlikely(BatchIsNull(outer_batch))) {
                output_num = cell_get_output_count(&(first_cell->perGroup), m_cmd);
                if (output_num > 0 && !DumpOutput(first_cell, outer_batch, output_num, 0)) {
                    goto PRODUCE_BATCH;
                }

                m_runtime->setop_done = true;
                pfree_ext(first_cell);
                break;
            }
            last_idx = 0;
        }
    }
PRODUCE_BATCH:
    /* the current result batch is full, and goto here */
    if (!BatchIsNull(m_scanBatch))
        return m_scanBatch;
    else
        return NULL;
}

/*
 * @Description: Compute sizing parameters for hashtable. Called when creating and growing
 * the hashtable.
 * @in oldsize - cost group size for expand is false, old hashtable size for expand is true.
 * @return - new size for building hashtable
 */
template <bool expand>
int setOpTbl::computeHashTableSize(int oldsize)
{
    int hash_size;
    int mppow2;

    if (expand == false) {
        /* supporting zero sized hashes would complicate matters */
        hash_size = Max(oldsize, MIN_HASH_TABLE_SIZE);

        /* round up size to the next power of 2, that's the bucketing works  */
        hash_size = getPower2NextNum(hash_size);

        m_max_hashsize = (int)Min(m_totalMem / sizeof(SetOpHashCell*), MaxAllocSize / sizeof(SetOpHashCell*));

        /* 	If max_pointers isn't a power of 2, must round it down to one */
        mppow2 = 1UL << (unsigned int)my_log2(m_max_hashsize);
        if (m_max_hashsize != mppow2) {
            m_max_hashsize = mppow2 / 2;
        }

        hash_size = (int)Min(hash_size, m_max_hashsize);
        if (hash_size * HASH_EXPAND_SIZE > m_max_hashsize) {
            m_can_grow = false;
        } else {
            m_can_grow = true;
        }

        ereport(DEBUG2, (errmodule(MOD_VEC_EXECUTOR), errmsg("memory allowed max table size is %d", m_max_hashsize)));
    } else {
        Assert(oldsize * HASH_EXPAND_SIZE <= m_max_hashsize);
        hash_size = oldsize * HASH_EXPAND_SIZE;

        if (hash_size * HASH_EXPAND_SIZE > m_max_hashsize) {
            m_can_grow = false;
        } else {
            m_can_grow = true;
        }
    }

    m_grow_threshold = hash_size * HASH_EXPAND_THRESHOLD;
    ereport(DEBUG2, (errmodule(MOD_VEC_EXECUTOR), errmsg("Hashed setop table size is %d", hash_size)));
    return hash_size;
}

bool setOpTbl::DumpOutput(SetOpHashCell* cell, VectorBatch* batch, int& outputNum, int idx)
{
    while (outputNum > 0) {
        InvokeFp(m_BuildScanBatch)(cell);
        outputNum--;

        if (m_scanBatch->m_rows == BatchMaxSize) {
            m_statusLog.restore = true;
            m_statusLog.lastIdx = idx;
            m_statusLog.numOutput = outputNum;
            m_statusLog.lastCell = cell;
            m_lastBatch = batch;
            return false;
        }
    }
    return true;
}

VectorBatch* setOpTbl::RunHash()
{
    VectorBatch* res = NULL;

    while (true) {
        switch (m_runState) {
            case SETOP_PREPARE:
                m_hashSource = GetHashSource();
                if (m_hashSource == NULL)  // no source, so it is the end;
                    return NULL;
                m_runState = SETOP_BUILD;
                break;

            case SETOP_BUILD:
                Build();
                m_runState = SETOP_FETCH;
                break;

            case SETOP_FETCH:
                res = Probe();
                if (BatchIsNull(res)) {
                    if (m_spillToDisk == true) {
                        m_runState = SETOP_PREPARE;
                    } else {
                        return NULL;
                    }
                } else
                    return res;
                break;
            default:
                ereport(ERROR,
                    (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE), errmsg("unrecognized running state: %d", m_runState)));
        }
    }
    return res;
}

void setOpTbl::Build()
{
    VectorBatch* outer_batch = NULL;
    WaitState old_status = pgstat_report_waitstatus(STATE_EXEC_HASHSETOP_BUILD_HASH);

    for (;;) {
        outer_batch = m_hashSource->getBatch();
        if (unlikely(BatchIsNull(outer_batch)))
            break;

        InvokeFp(m_BuildFun)(outer_batch);
    }
    (void)pgstat_report_waitstatus(old_status);

    if (HAS_INSTR(m_runtime, false)) {
        if (m_tupleCount > 0)
            m_runtime->ps.instrument->width = (int)(m_colWidth / m_tupleCount);
        else
            m_runtime->ps.instrument->width = (int)m_colWidth;
        m_runtime->ps.instrument->spreadNum = m_spreadNum;
        m_runtime->ps.instrument->sysBusy = m_sysBusy;
    }
}

VectorBatch* setOpTbl::Probe()
{
    int last_Idx = 0;
    SetOpHashCell* cell = NULL;
    VectorBatch* res = NULL;
    int i = 0;
    int output_num = 0;

    m_scanBatch->Reset(true);

    while (BatchIsNull(m_scanBatch)) {
        if (m_statusLog.restore) {
            last_Idx = m_statusLog.lastIdx;
            cell = m_statusLog.lastCell;
            output_num = m_statusLog.numOutput;
            m_statusLog.restore = false;
        }

        for (i = last_Idx; i < m_size; i++) {
            if (NULL == cell) {
                cell = m_setOpHashData[i];
            }

            while (NULL != cell) {
                if (0 == output_num) {
                    output_num = cell_get_output_count(&(cell->perGroup), m_cmd);
                }

                while (output_num > 0) {
                    InvokeFp(m_BuildScanBatch)(cell);
                    output_num--;

                    if (m_scanBatch->m_rows == BatchMaxSize) {
                        m_statusLog.restore = true;
                        if (0 == output_num)
                            cell = cell->m_next;

                        if (NULL == cell) {
                            m_statusLog.lastIdx = i + 1;
                            m_statusLog.lastCell = NULL;
                        } else {
                            m_statusLog.lastIdx = i;
                            m_statusLog.lastCell = cell;
                        }
                        m_statusLog.numOutput = output_num;

                        goto PRODUCE_BATCH;
                    }
                }
                cell = cell->m_next;
            }
        }

    PRODUCE_BATCH:
        if (i == m_size) {
            if (m_scanBatch->m_rows > 0) {
                res = m_scanBatch;
                m_statusLog.lastIdx = m_size;
                m_statusLog.restore = true;
            } else
                return NULL;
        } else {
            if (m_scanBatch->m_rows > 0)
                res = m_scanBatch;
        }
    }

    return res;
}

void setOpTbl::BindingFp()
{
    if (m_keySimple) {
        m_BuildFun = &setOpTbl::BuildSetOpTbl<true>;
    } else {
        m_BuildFun = &setOpTbl::BuildSetOpTbl<false>;
    }
}

hashSource* setOpTbl::GetHashSource()
{
    hashSource* ps = NULL;
    m_strategy = HASH_IN_MEMORY;
    if (m_spillToDisk) {
        m_filesource->close(m_filesource->getCurrentIdx());
        if (m_filesource->next()) {
            ps = m_filesource;
            m_filesource->rewind(m_filesource->getCurrentIdx());

            /* reset the rows in the hash table */
            m_rows = 0;

            MemoryContextReset(m_filesource->m_context);
            /* Reset hash table*/
            MemoryContextReset(m_hashContext);

            {
                AutoContextSwitch context_guard(m_hashContext);
                m_setOpHashData = (SetOpHashCell**)palloc0(m_size * sizeof(SetOpHashCell*));
            }

            m_statusLog.restore = false;
            m_statusLog.lastIdx = 0;
            m_statusLog.lastCell = NULL;
        } else
            return NULL;
    } else {
        ps = New(CurrentMemoryContext) hashOpSource(outerPlanState(m_runtime));
    }
    return ps;
}

template <bool simple>
void setOpTbl::AllocHashSlot(VectorBatch* batch, int i, int flag, bool foundMatch, SetOpHashCell* cell)
{
    SetOpHashCell* new_cell = NULL;
    SetOpHashCell* head_cell = NULL;
    int elem_idx;

    if (flag == m_firstFlag) {
        if (false == foundMatch) {
            if (m_tupleCount >= 0)
                m_tupleCount++;

            if (m_strategy == HASH_IN_MEMORY) {
                AutoContextSwitch mem_switch(m_hashContext);
                new_cell = (SetOpHashCell*)palloc0(m_cellSize);
                if (m_tupleCount >= 0)
                    m_colWidth += m_cols * sizeof(hashVal);
                InitCell<simple>(new_cell, batch, i, flag);
                /* store the hash value.*/
                new_cell->m_val[m_outerColNum].val = m_cacheLoc[i];

                elem_idx = get_bucket(m_cacheLoc[i]);
                head_cell = m_setOpHashData[elem_idx];
                m_setOpHashData[elem_idx] = new_cell;
                new_cell->m_next = head_cell;
                /* not respill temp file for setop */
                if (!m_spillToDisk)
                    JudgeMemoryOverflow("VecSetop",
                        m_runtime->ps.plan->plan_node_id,
                        SET_DOP(m_runtime->ps.plan->dop),
                        m_runtime->ps.instrument);
            } else {
                WaitState old_status = pgstat_report_waitstatus(STATE_EXEC_HASHSETOP_WRITE_FILE);
                if (m_filesource == NULL) {
                    int file_num = calcFileNum(((VecSetOp*)m_runtime->ps.plan)->numGroups);
                    if (m_filesource == NULL)
                        m_filesource = CreateTempFile(m_scanBatch, file_num, &m_runtime->ps);
                    else
                        m_filesource->resetVariableMemberIfNecessary(file_num);

                    if (HAS_INSTR(m_runtime, true)) {
                        m_runtime->ps.instrument->sorthashinfo.hash_FileNum = file_num;
                        m_runtime->ps.instrument->sorthashinfo.hash_writefile = true;
                    }

                    /* flag to mark write temp file */
                    m_spillToDisk = true;

                    /* Do not grow up hashtable if convert to write file */
                    m_can_grow = false;
                }
                m_filesource->writeBatch(
                    batch, i, DatumGetUInt32(hash_uint32(m_cacheLoc[i])));  // compute  the hash for tuple save to disk
                pgstat_increase_session_spill();
                (void)pgstat_report_waitstatus(old_status);
            }
        } else
            advance_pergroup(cell, flag == m_firstFlag);
    } else {
        if (true == foundMatch)
            advance_pergroup(cell, flag == m_firstFlag);

        /* There is a boundary condition that we change the m_stragety flag from HASH_IN_MEMORY to HASH_IN_DISK,
         * but actually no spilling happen as it is the last data, so if the spilling structure has not been generated,
         * we can just discade the data.
         */
        if (false == foundMatch && m_spillToDisk && m_strategy == HASH_IN_DISK) {
            /* compute  the hash for tuple save to disk.*/
            
            WaitState old_state = pgstat_report_waitstatus(STATE_EXEC_HASHSETOP_WRITE_FILE);
            m_filesource->writeBatch(batch, i, DatumGetUInt32(hash_uint32(m_cacheLoc[i])));
            (void)pgstat_report_waitstatus(old_state);
        }
    }
}

template <bool simple>
void setOpTbl::BuildSetOpTbl(VectorBatch* batch)
{
    int i = 0;
    int nrows = batch->m_rows;
    int flag = 0;
    SetOpHashCell* cell = NULL;
    bool found_match = false;
    int32 start_elem;

    hashBatch(batch, m_keyIdx, m_cacheLoc, m_outerHashFuncs);

    /*
     * Do the grow check for each batch, to avoid doing the check inside the for loop.
     * This also lets us avoid having to re-find hash position in the hashtable after
     * resizing.
     */
    if (m_maxMem > 0)
        (void)computeHashTableSize<false>(m_size);
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

    for (i = 0; i < nrows; i++) {
        start_elem = get_bucket(m_cacheLoc[i]);
        cell = m_setOpHashData[start_elem];
        found_match = false;

        while (NULL != cell) {
            /* First compare hash value.*/
            if (m_cacheLoc[i] == cell->m_val[m_outerColNum].val && MatchKey<simple>(batch, i, cell)) {
                found_match = true;
                break;
            }
            cell = cell->m_next;
        }

        flag = fetch_flag(batch, m_junkCol - 1, i);
        AllocHashSlot<simple>(batch, i, flag, found_match, cell);
    }

    // we can reset the memory safely per batch line
    if (m_spillToDisk)
        MemoryContextReset(m_filesource->m_context);
}

template <bool simple>
void setOpTbl::InitCell(SetOpHashCell* cell, VectorBatch* batch, int row, int flag)
{
    ScalarVector* vector = NULL;
    int k = 0;
    ScalarVector* arr = batch->m_arr;
    hashVal* val = cell->m_val;

    for (k = 0; k < m_outerColNum; k++) {
        vector = &arr[k];
        if (simple || m_keyDesc[k].encoded == false) {
            val[k].val = vector->m_vals[row];
            val[k].flag = vector->m_flag[row];
        } else {
            if (likely(vector->IsNull(row) == false)) {
                val[k].val = addVariable(m_hashContext, vector->m_vals[row]);
                if (m_tupleCount >= 0)
                    m_colWidth += VARSIZE_ANY(vector->m_vals[row]);
            }

            val[k].flag = vector->m_flag[row];
        }
    }

    init_pergroup(cell, flag == m_firstFlag);
}

template <bool simpleKey>
bool setOpTbl::MatchKey(VectorBatch* batch, int batchIdx, SetOpHashCell* cell)
{
    bool match = true;
    int i = 0;
    ScalarVector* vector = NULL;
    hashVal* hashval = NULL;
    Datum key1;
    Datum key2;
    ScalarVector* arr = batch->m_arr;
    hashVal* val = cell->m_val;

    for (i = 0; i < m_key; i++) {
        vector = &arr[m_keyIdx[i]];
        hashval = &val[m_keyIdx[i]];

        // both null is equal
        if (IS_NULL(vector->m_flag[batchIdx]) && IS_NULL(hashval->flag))
            continue;
        else if (IS_NULL(vector->m_flag[batchIdx]) || IS_NULL(hashval->flag))
            return false;
        else {
            if (simpleKey || m_keyDesc[i].encoded == false) {
                if (vector->m_vals[batchIdx] == hashval->val)
                    continue;
                else
                    return false;
            } else {
                key1 = ScalarVector::Decode(vector->m_vals[batchIdx]);
                key2 = ScalarVector::Decode(hashval->val);

                match = DatumGetBool(FunctionCall2((m_eqfunctions + i), key1, key2));
                if (match == false)
                    return false;
            }
        }
    }

    return true;
}

/*
 * @Description: get hash value from hashtable
 * @in hashentry - hashtable element
 * @return - hash value
 */
ScalarValue setOpTbl::getHashValue(SetOpHashCell* hashentry)
{
    return hashentry->m_val[m_outerColNum].val;
}

/*
 * @Description: Grow up the hash table to a new size
 * @return - void
 */
void setOpTbl::HashTableGrowUp()
{
    int32 oldsize = m_size;
    int32 i;
    instr_time start_time;
    double total_time = 0;
    errno_t rc = EOK;

    INSTR_TIME_SET_CURRENT(start_time);

    ereport(DEBUG2,
        (errmodule(MOD_VEC_EXECUTOR),
            errmsg("node id is %d, hash table rows is %ld", m_runtime->ps.plan->plan_node_id, m_rows)));

    /* compute parameters for new table */
    m_size = computeHashTableSize<true>(oldsize);

    m_setOpHashData = (SetOpHashCell**)repalloc(m_setOpHashData, m_size * sizeof(SetOpHashCell*));
    rc = memset_s(m_setOpHashData + oldsize,
        (m_size - oldsize) * sizeof(SetOpHashCell*),
        0,
        (m_size - oldsize) * sizeof(SetOpHashCell*));
    securec_check(rc, "\0", "\0");

    /*
     * Copy cells from the old data to newdata.
     * We neither increase the data rows, nor need to compare keys.
     * We just calculate new hash bucket and  append old cell to new cells.	Then free old data.
     */
    for (i = 0; i < oldsize; i++) {
        SetOpHashCell* oldentry = m_setOpHashData[i];
        SetOpHashCell* nextentry = NULL;
        m_setOpHashData[i] = NULL;

        while (oldentry != NULL) {
            uint32 hash;
            uint32 startelem;
            SetOpHashCell* newentry = NULL;
            nextentry = oldentry->m_next;
            oldentry->m_next = NULL;

            hash = getHashValue(oldentry);
            startelem = get_bucket(hash);
            newentry = m_setOpHashData[startelem];

            if (newentry == NULL) {
                m_setOpHashData[startelem] = oldentry;
            } else {
                oldentry->m_next = newentry;
                m_setOpHashData[startelem] = oldentry;
            }

            oldentry = nextentry;
            CHECK_FOR_INTERRUPTS();
        }
    }

    total_time = elapsed_time(&start_time);

    /* print hashtable grow up time */
    ereport(
        DEBUG2, (errmodule(MOD_VEC_EXECUTOR), errmsg("Hashed setop table grow up time is %.3f ms", total_time * 1000)));
    if (m_runtime->ps.instrument) {
        m_runtime->ps.instrument->sorthashinfo.hashtable_expand_times++;
    }
}

void setOpTbl::BuildScanBatch(SetOpHashCell* cell)
{
    int i = 0;
    int nrows = m_scanBatch->m_rows;
    ScalarVector* vector = NULL;
    ScalarVector* arr = m_scanBatch->m_arr;
    hashVal* val = cell->m_val;

    for (i = 0; i < m_outerColNum; i++) {
        vector = &(arr[i]);

        vector->m_vals[nrows] = val[i].val;
        vector->m_flag[nrows] = val[i].flag;
        vector->m_rows++;
    }

    m_scanBatch->m_rows++;
}

static int fetch_flag(VectorBatch* batch, int col, int row)
{
    int flag = DatumGetInt32((Datum)batch->m_arr[col].m_vals[row]);
    Assert(flag == 0 || flag == 1);

    return flag;
}

static void init_pergroup(SetOpHashCell* cell, bool first_flag)
{
    if (first_flag) {
        cell->perGroup.numLeft = 1;
        cell->perGroup.numRight = 0;
    } else {
        cell->perGroup.numLeft = 0;
        cell->perGroup.numRight = 1;
    }
}

static void advance_pergroup(SetOpHashCell* cell, bool first_flag)
{
    if (first_flag) {
        cell->perGroup.numLeft++;
    } else {
        cell->perGroup.numRight++;
    }
}

static int cell_get_output_count(VecSetOpStatePerGroupData* pergroup, int cmd)
{
    int num_output = 0;

    switch (cmd) {
        case SETOPCMD_INTERSECT:
            if (pergroup->numLeft > 0 && pergroup->numRight > 0)
                num_output = 1;
            else
                num_output = 0;
            break;
        case SETOPCMD_INTERSECT_ALL:
            num_output = (pergroup->numLeft < pergroup->numRight) ? pergroup->numLeft : pergroup->numRight;
            break;
        case SETOPCMD_EXCEPT:
            if (pergroup->numLeft > 0 && pergroup->numRight == 0)
                num_output = 1;
            else
                num_output = 0;
            break;
        case SETOPCMD_EXCEPT_ALL:
            num_output = (pergroup->numLeft < pergroup->numRight) ? 0 : (pergroup->numLeft - pergroup->numRight);
            break;
        default:
            ereport(ERROR, (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE), errmsg("unrecognized set op: %d", cmd)));
            break;
    }

    return num_output;
}

/*
 * @Description: Early free the memory for VecHashedSetop.
 *
 * @param[IN] node:  vector executor state for HashedSetop
 * @return: void
 */
void ExecEarlyFreeVecHashedSetop(VecSetOpState* node)
{
    PlanState* plan_state = &node->ps;
    VecSetOp* plan = (VecSetOp*)node->ps.plan;

    if (plan_state->earlyFreed)
        return;

    if (plan->strategy == SETOP_HASHED) {
        setOpTbl* vecsetop_tbl = (setOpTbl*)node->vecSetOpInfo;
        if (vecsetop_tbl != NULL) {
            vecsetop_tbl->closeFile();
            vecsetop_tbl->freeMemoryContext();
        }
    }

    /*
     * Free  the expr contexts.
     */
    ExecFreeExprContext(&node->ps);
    ;

    EARLY_FREE_LOG(elog(LOG,
        "Early Free: After early freeing Setop "
        "at node %d, memory used %d MB.",
        plan->plan.plan_node_id,
        getSessionMemoryUsageMB()));

    plan_state->earlyFreed = true;
    ExecEarlyFree(outerPlanState(node));
}
