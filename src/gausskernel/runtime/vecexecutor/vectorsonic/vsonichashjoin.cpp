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
 *
 * -----------------------------------------------------------------------------
 * vsonichashjoin.cpp
 *              Routines to handle vector sonic hashjoin nodes.
 *              Sonic Hash Join nodes are based on the column-based hash table.
 *
 *
 * IDENTIFICATION
 *      Code/src/gausskernel/runtime/vecexecutor/vectorsonic/vsonichashjoin.cpp
 *
 * -----------------------------------------------------------------------------
 */
#include "vectorsonic/vsonichash.h"
#include "vectorsonic/vsonichashjoin.h"
#include "utils/memprot.h"

#define leftrot(x, k) (((x) << (k)) | ((x) >> (32 - (k))))
#define PROFILE_PART(x, sz)                                                                                \
    if (anls_opt_is_on(ANLS_HASH_CONFLICT)) {                                                              \
        profile(true, x, sz);                                                                              \
    } else if (u_sess->attr.attr_resource.resource_track_level >= RESOURCE_TRACK_QUERY &&                  \
               u_sess->attr.attr_resource.enable_resource_track && u_sess->exec_cxt.need_track_resource) { \
        profile(false, x, sz);                                                                             \
    }
#define INSTR (m_runtime->js.ps.instrument)

/*
 * Hash table size: next size + bucket size
 * next size   : 4bytes * (nrows + 1)
 * ignore 1 in estimated result.
 * bucket size : 4bytes * bucket row number
 * Bucket row number is hashfindprime(nrows). But avoid
 * calculating the result every time, we give it an estimated
 * result 1.01 * nrows. It maybe not suitable in some situations.
 *
 * Estimated hash table size:
 * 4 bytes * (nrows + 1.01 nrows) = 8.04 nrows.
 */

#ifdef USE_PRIME
#define GETLOCID(val, mask) ((val) % (mask))
#else
#define GETLOCID(val, mask) ((val) & (mask))
#endif

/*
 * @Description:  Check condition for sonic hash join.
 * 	If return value is true, goto Sonic hash join.
 */
bool isSonicHashJoinEnable(HashJoin* hj)
{
    /* Only support inner join now. */
    return (hj->join.jointype == JOIN_INNER);
}

/*
 * @Description: sonic hash join constructor.
 * 	In hash join constructor, hashContext manages hash table and partition.
 * 	Other variables are under hash join node context.
 */
SonicHashJoin::SonicHashJoin(int size, VecHashJoinState* node)
    : SonicHash(size),
      m_complicatekey(false),
      m_runtime(node),
      m_outRawBatch(NULL),
      m_matchLocIndx(0),
      m_probeIdx(0),
      m_arrayExpandSize(0),
      m_partLoadedOffset(-1),
      m_maxPLevel(3),
      m_isValid(NULL)
{
    ScalarDesc unknown_desc;

    AddControlMemoryContext(m_runtime->js.ps.instrument, m_memControl.hashContext);

    m_build_time = 0.0;
    m_probe_time = 0.0;

    m_buildOp.tupleDesc = innerPlanState(m_runtime)->ps_ResultTupleSlot->tts_tupleDescriptor;
    m_probeOp.tupleDesc = outerPlanState(m_runtime)->ps_ResultTupleSlot->tts_tupleDescriptor;

    m_buildOp.cols = m_buildOp.tupleDesc->natts;
    m_probeOp.cols = m_probeOp.tupleDesc->natts;

    m_buildOp.batch = New(CurrentMemoryContext) VectorBatch(CurrentMemoryContext, m_buildOp.tupleDesc);
    m_probeOp.batch = New(CurrentMemoryContext) VectorBatch(CurrentMemoryContext, m_probeOp.tupleDesc);

    m_eqfunctions = m_runtime->eqfunctions;

    /* init hash key index */
    m_buildOp.keyNum = list_length(m_runtime->hj_InnerHashKeys);
    m_probeOp.keyNum = m_buildOp.keyNum;
    m_buildOp.keyIndx = (uint16*)palloc0(sizeof(uint16) * m_buildOp.keyNum);
    m_probeOp.keyIndx = (uint16*)palloc0(sizeof(uint16) * m_buildOp.keyNum);
    m_buildOp.oKeyIndx = (uint16*)palloc0(sizeof(uint16) * m_buildOp.keyNum);
    m_probeOp.oKeyIndx = (uint16*)palloc0(sizeof(uint16) * m_buildOp.keyNum);

    m_integertype = (bool*)palloc(sizeof(bool) * m_buildOp.keyNum);
    for (int i = 0; i < m_buildOp.keyNum; i++) {
        m_integertype[i] = true;
    }
    setHashIndex(m_buildOp.keyIndx, m_buildOp.oKeyIndx, m_runtime->hj_InnerHashKeys);
    setHashIndex(m_probeOp.keyIndx, m_probeOp.oKeyIndx, m_runtime->hj_OuterHashKeys);

    errno_t rc = memset_s(m_memPartFlag, sizeof(m_memPartFlag), 0, SONIC_PART_MAX_NUM * sizeof(m_memPartFlag[0]));
    securec_check(rc, "\0", "\0");

    initMemoryControl();
    m_memControl.hashContext = AllocSetContextCreate(CurrentMemoryContext,
        "SonicHashJoinContext",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE,
        STANDARD_CONTEXT,
        m_memControl.totalMem);

    /* init hash functions */
    m_buildOp.hashFunc = (hashValFun*)palloc0(sizeof(hashValFun) * m_buildOp.keyNum);
    m_buildOp.hashAtomFunc = (hashValFun*)palloc0(sizeof(hashValFun) * m_buildOp.keyNum);
    m_probeOp.hashFunc = (hashValFun*)palloc0(sizeof(hashValFun) * m_buildOp.keyNum);
    m_probeOp.hashAtomFunc = NULL;

    if (m_complicatekey) {
        bindingFp<true>();
    } else {
        bindingFp<false>();
    }
    initHashFmgr();

    /* init one memory partition for memory hashjoin */
    m_partNum = 1;

    {
        AutoContextSwitch memSwitch(m_memControl.hashContext);
        m_innerPartitions = (SonicHashPartition**)palloc0(sizeof(SonicHashPartition*));
        m_innerPartitions[0] = New(CurrentMemoryContext) SonicHashMemPartition(
            (char*)"innerPartitionContext", m_complicatekey, m_buildOp.tupleDesc, m_memControl.totalMem);
        initPartition<true>(m_innerPartitions[0]);
    }
    m_outerPartitions = NULL;
    calcDatumArrayExpandSize();

    m_hashOpPartition = New(CurrentMemoryContext) SonicHashOpSource(outerPlanState(m_runtime));

    if (m_complicatekey) {
        m_complicate_innerBatch = New(CurrentMemoryContext) VectorBatch(CurrentMemoryContext, m_buildOp.tupleDesc);
        m_complicate_outerBatch = New(CurrentMemoryContext) VectorBatch(CurrentMemoryContext, m_probeOp.tupleDesc);
        m_cjVector = New(CurrentMemoryContext) ScalarVector;
        m_cjVector->init(CurrentMemoryContext, unknown_desc);
    } else {
        replaceEqFunc();
    }

    if (HAS_INSTR(&m_runtime->js, true)) {
        errno_t ret = memset_s(&(INSTR->sorthashinfo), sizeof(INSTR->sorthashinfo), 0, sizeof(struct SortHashInfo));
        securec_check(ret, "\0", "\0");
    }

    m_diskPartNum = 0;
    m_strategy = MEMORY_HASH;
}

/*
 * @Description: Binding some execution functions.
 */
template <bool complicateJoinKey>
void SonicHashJoin::bindingFp()
{
    m_funBuild[0] = &SonicHashJoin::saveToMemory<complicateJoinKey>;
    if (u_sess->attr.attr_sql.enable_sonic_optspill) {
        m_funBuild[1] = &SonicHashJoin::saveToDisk<complicateJoinKey, true>;
    } else {
        m_funBuild[1] = &SonicHashJoin::saveToDisk<complicateJoinKey, false>;
    }
    m_probeFun[0] = &SonicHashJoin::probeMemory;
    m_probeFun[1] = &SonicHashJoin::probeGrace;
    if (u_sess->attr.attr_sql.enable_sonic_optspill) {
        m_saveProbePartition = &SonicHashJoin::saveProbePartition<complicateJoinKey, true>;
    } else {
        m_saveProbePartition = &SonicHashJoin::saveProbePartition<complicateJoinKey, false>;
    }
    if (!complicateJoinKey) {
        initMatchFunc(m_buildOp.tupleDesc, m_buildOp.keyNum);
        initHashFunc(m_buildOp.tupleDesc, (void*)m_buildOp.hashFunc, m_buildOp.keyIndx, false);
        initHashFunc(m_buildOp.tupleDesc, (void*)m_buildOp.hashAtomFunc, m_buildOp.keyIndx, true);
        initHashFunc(m_probeOp.tupleDesc, (void*)m_probeOp.hashFunc, m_probeOp.keyIndx, false);
    }
}

/*
 * @Description: Compute hash key index and check whether the hashkey is simple type.
 * @out keyIndx - Record build side hash key attr number.
 * @out oKeyIndx - Record build side hash key origin attr number.
 * @in hashkeys - keyIndx, oKeyIndx should be allocated by caller.
 */
void SonicHashJoin::setHashIndex(uint16* keyIndx, uint16* oKeyIndx, List* hashKeys)
{
    int i = 0;
    ListCell* lc = NULL;
    ExprState* expr_state = NULL;
    Var* variable = NULL;

    foreach (lc, hashKeys) {
        expr_state = (ExprState*)lfirst(lc);
        if (IsA(expr_state->expr, Var)) {
            variable = (Var*)expr_state->expr;
        } else if (IsA(expr_state->expr, RelabelType)) {
            RelabelType* rel_type = (RelabelType*)expr_state->expr;

            if (IsA(rel_type->arg, Var) && ((Var*)rel_type->arg)->varattno > 0) {
                variable = (Var*)((RelabelType*)expr_state->expr)->arg;
            } else {
                m_complicatekey = true;
                break;
            }
        } else {
            m_complicatekey = true;
            break;
        }

        keyIndx[i] = variable->varattno - 1;
        oKeyIndx[i] = variable->varoattno - 1;
        m_integertype[i] = (unsigned int)(m_integertype[i]) & (unsigned int)integerType(variable->vartype);
        i++;
    }
}

/*
 * @Description: Initial memory control information.
 */
void SonicHashJoin::initMemoryControl()
{
    VecHashJoin* node = (VecHashJoin*)m_runtime->js.ps.plan;

    m_memControl.totalMem = SET_NODEMEM(((Plan*)node)->operatorMemKB[0], ((Plan*)node)->dop) * 1024L;
    if (((Plan*)node)->operatorMaxMem > 0) {
        m_memControl.maxMem = SET_NODEMEM(((Plan*)node)->operatorMaxMem, ((Plan*)node)->dop) * 1024L;
    }

    elog(DEBUG2,
        "SonicHashJoinTbl[%d]: operator memory uses %dKB",
        ((Plan*)node)->plan_node_id,
        (int)(m_memControl.totalMem / 1024L));
}

/*
 * @Description: Initial hash functions info from m_runtime.
 * 	Should be under hashjoin hashContext.
 */
void SonicHashJoin::initHashFmgr()
{
    ListCell* lc = NULL;
    int i = 0;
    m_buildOp.hashFmgr = (FmgrInfo*)palloc(sizeof(FmgrInfo) * m_buildOp.keyNum);
    m_probeOp.hashFmgr = (FmgrInfo*)palloc(sizeof(FmgrInfo) * m_probeOp.keyNum);
    foreach (lc, m_runtime->hj_HashOperators) {
        Oid hashop = lfirst_oid(lc);
        Oid left_hashfn;
        Oid right_hashfn;

        if (!get_op_hash_functions(hashop, &left_hashfn, &right_hashfn)) {
            ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_FUNCTION),
                    errmodule(MOD_VEC_EXECUTOR),
                    errmsg("could not find hash function for hash operator %u", hashop)));
        }
        fmgr_info(left_hashfn, &m_probeOp.hashFmgr[i]);
        fmgr_info(right_hashfn, &m_buildOp.hashFmgr[i]);
        i++;
    }

    if (u_sess->attr.attr_sql.enable_fast_numeric) {
        replace_numeric_hash_to_bi(i, m_probeOp.hashFmgr);
        replace_numeric_hash_to_bi(i, m_buildOp.hashFmgr);
    }
}

/*
 * @Description: initialize one partition,
 * 	for SonicHashMemPartition, it will init the m_data per col,
 * 	for SonicHashFilePartition, it will init the m_file per file.
 * @in partition - partition need to be initialized.
 */
template <bool isInner>
void SonicHashJoin::initPartition(SonicHashPartition* partition)
{

    MemoryContext old_cxt = MemoryContextSwitchTo(partition->m_context);
    DatumDesc desc;
    Form_pg_attribute* attrs = NULL;
    bool doNotCompress = false;
    SonicHashInputOpAttr* attr_op = NULL;
    if (isInner) {
        attrs = m_buildOp.tupleDesc->attrs;
        attr_op = &m_buildOp;
    } else {
        attrs = m_probeOp.tupleDesc->attrs;
        attr_op = &m_probeOp;
    }

    if (m_complicatekey) {
        for (int idx = 0; idx < attr_op->cols; idx++) {
            getDataDesc(&desc, 0, attrs[idx], doNotCompress);
            partition->init(idx, &desc);
        }
    } else {
        for (int idx = 0; idx < attr_op->cols; idx++) {
            doNotCompress = isHashKey(attrs[idx]->atttypid, idx, attr_op->keyIndx, attr_op->keyNum);
            getDataDesc(&desc, 0, attrs[idx], doNotCompress);
            partition->init(idx, &desc);
        }
    }

    (void)MemoryContextSwitchTo(old_cxt);
}

/*
 * @Description: try to load as many inner partitions as possible from files.
 * 	This function is called during GRACE_HASH,
 * 	It sorts the partitions' size,
 * 	and load as many partitions as possible from the disk,
 * 	to do join in memory.
 * 	So that, few partitions are remained to probe.
 * @in memorySize - The total size of partitions should be less than it.
 */
void SonicHashJoin::loadInnerPartitions(uint64 memorySize)
{
    SonicHashMemPartition* mem_partition = NULL;
    sortPartitionSize(memorySize);

    /* Some partition can be loaded into memory, init a memory partition to load the data. */
    if (m_partLoadedOffset >= 0) {
        AutoContextSwitch memSwitch(m_memControl.hashContext);

        mem_partition = New(CurrentMemoryContext) SonicHashMemPartition(
            (char*)"innerPartitionContext", m_complicatekey, m_buildOp.tupleDesc, m_memControl.totalMem);
        initPartition<true>(mem_partition);
    }

    Assert(m_partLoadedOffset >= -1);
    for (uint32 idx = 0; idx < (uint32)(m_partLoadedOffset + 1); ++idx) {
        loadMultiInnerPartition(m_partSizeOrderedIdx[idx], mem_partition);
    }

    /* finish loaded, now save to change tht pointer */
    if (mem_partition != NULL) {
        m_innerPartitions[m_partSizeOrderedIdx[0]] = mem_partition;
    }
}

/*
 * @Description: Load as many partition as possible.
 * @in partIdx - partition index.
 * @in memPartition - store data from file partition to it.
 */
void SonicHashJoin::loadMultiInnerPartition(uint32 partIdx, SonicHashMemPartition* memPartition)
{
    Assert(m_memPartFlag[partIdx]);

    int64 nrows = 0;
    SonicHashFilePartition* file_partition = NULL;

    file_partition = (SonicHashFilePartition*)m_innerPartitions[partIdx];
    Assert(file_partition != NULL);
    Assert(file_partition->m_status == partitionStatusFile);
    nrows = file_partition->m_rows;

    /* If this partition has nothing, release the file handler buffer and handler and return. */
    if (nrows == 0) {
        Assert(file_partition->m_size == 0);
        file_partition->releaseFileHandlerBuffer();
        file_partition->closeFiles();
        file_partition->m_status = partitionStatusFinish;
        return;
    }

    file_partition->prepareFileHandlerBuffer();
    file_partition->rewindFiles();

    /* Load data from file and put them into SonicDatumArray. */
    memPartition->loadPartition((void*)file_partition);
    if (m_complicatekey) {
        /* Load hash value from file if m_complicatekey is true. */
        memPartition->loadHash((void*)file_partition);
    }
    /* Data of the file partition has been loaded, save to free the file partition resource */
    m_innerPartitions[partIdx]->freeResources();
    m_innerPartitions[partIdx] = NULL;
}

/*
 * @Description: load the partIdx-th partition.
 * @in partIdx - partition index.
 */
void SonicHashJoin::loadInnerPartition(uint32 partIdx)
{
    int64 nrows = 0;
    SonicHashFilePartition* file_partition = NULL;
    SonicHashPartition* memPartition = NULL;

    file_partition = (SonicHashFilePartition*)m_innerPartitions[partIdx];
    Assert(file_partition != NULL);
    Assert(file_partition->m_status == partitionStatusFile);

    nrows = file_partition->m_rows;
    /* If this partition has nothing, release the file handler buffer and handler and return */
    if (nrows == 0) {
        Assert(file_partition->m_size == 0);
        file_partition->releaseFileHandlerBuffer();
        file_partition->closeFiles();
        file_partition->m_status = partitionStatusFinish;
        return;
    }

    {
        AutoContextSwitch memSwitch(m_memControl.hashContext);
        memPartition = New(CurrentMemoryContext) SonicHashMemPartition(
            (char*)"innerPartitionContext", m_complicatekey, m_buildOp.tupleDesc, m_memControl.totalMem);
        initPartition<true>(memPartition);

        file_partition->prepareFileHandlerBuffer();
        file_partition->rewindFiles();
    }

    /* Load data from file and put them into SonicDatumArray. */
    ((SonicHashMemPartition*)memPartition)->loadPartition((void*)file_partition);

    /* Load hash value from file if m_complicatekey is true. */
    if (m_complicatekey) {
        ((SonicHashMemPartition*)memPartition)->loadHash((void*)file_partition);
    }

    /*
     * Release file partition resource and
     * make it pointer to memory partition constructed above.
     */
    m_innerPartitions[partIdx]->freeResources();
    m_innerPartitions[partIdx] = memPartition;
}

/*
 * @Description: calculate hashtable head size (m_bucket + m_next).
 * @in rows - rows in memory.
 */
uint64 SonicHashJoin::get_hash_head_size(int64 rows)
{
    int64 hash_size = (int64)calcHashSize(rows);
    int64 sz_hash = Max(hash_size, rows + 1);
    int byte_size = 4;
    uint64 hash_head_size = 0;
    if (((uint64)sz_hash & 0xffff) == (uint64)sz_hash) {
        byte_size = 2;
    }

    bool use_hash_table = (sz_hash * byte_size >= (int64)MaxAllocSize);

    if (!use_hash_table) {
        hash_head_size = (hash_size + rows + 1) * byte_size;
    } else {
        uint64 numSegBucket = (hash_size - 1) / INIT_DATUM_ARRAY_SIZE + 1;
        uint64 numSegNext = rows / INIT_DATUM_ARRAY_SIZE + 1;

        hash_head_size = sizeof(atom*) * INIT_ARR_CONTAINER_SIZE * 2 +
                         INIT_DATUM_ARRAY_SIZE * byte_size * (numSegBucket + numSegNext);
    }

    return hash_head_size;
}

/*
 * @Description: Build side main function.
 */
void SonicHashJoin::Build()
{
    PlanState* inner_node = innerPlanState(m_runtime);
    VectorBatch* batch = NULL;
    instr_time start_time;

    for (;;) {
        batch = VectorEngine(inner_node);
        if (unlikely(BatchIsNull(batch))) {
            if (m_strategy == MEMORY_HASH) {
                (void)INSTR_TIME_SET_CURRENT(start_time);
                uint64 hash_head_size = get_hash_head_size(m_rows);
                judgeMemoryOverflow(hash_head_size);

                if (!hasEnoughMem()) {
                    /*
                     * If the total number of rows reaches SONIC_MAX_ROWS,
                     * spill the data into disk.
                     * Because the hash table can not handle this amount of data.
                     */
                    m_strategy = GRACE_HASH;
                    m_partNum = calcPartitionNum();

                    if (m_complicatekey) {
                        flushToDisk<true>();
                    } else {
                        flushToDisk<false>();
                    }
                    pgstat_increase_session_spill();
                }
                m_build_time += elapsed_time(&start_time);
            }
            break;
        }

        (void)INSTR_TIME_SET_CURRENT(start_time);

        RuntimeBinding(m_funBuild, m_strategy)(batch);

        m_rows += batch->m_rows;
        m_build_time += elapsed_time(&start_time);
    }

    if (m_strategy == GRACE_HASH) {
        /*
         * Done spilling build side,
         * thus record the partition debug info here before loading them.
         */
        HASH_BASED_DEBUG(recordPartitionInfo(true, -1, 0, m_partNum));

        /* release all the temp files in all the inner partitions */
        releaseAllFileHandlerBuffer(true);
    }

    /*
     * Done obtaining build side data,
     * record spill related information here cause them may be loaded into memory later.
     */
    reportSorthashinfo(reportTypeBuild, m_partNum);

    pushDownFilterIfNeed();

    /* prepareProbe, also record the build time and profile */
    prepareProbe();

    /*
     * Done building hash table for build side,
     * record memory and time related information here.
     */
    if (HAS_INSTR(&m_runtime->js, true)) {
        INSTR->sysBusy = m_memControl.sysBusy;
        INSTR->spreadNum = m_memControl.spreadNum;
        INSTR->sorthashinfo.hashbuild_time = m_build_time;
        INSTR->sorthashinfo.spaceUsed = m_memControl.allocatedMem - m_memControl.availMem;
    }
}

/*
 * @Description: save the data to memory.
 * @in batch - Put the data in batch to momory.
 */
template <bool complicateJoinKey>
void SonicHashJoin::saveToMemory(VectorBatch* batch)
{
    int rows = batch->m_rows;
    SonicHashMemPartition* memPartition = (SonicHashMemPartition*)m_innerPartitions[0];

    if (!hasEnoughMem() || (m_rows + rows) > SONIC_MAX_ROWS) {
        /*
         * If the total number of rows reaches SONIC_MAX_ROWS,
         * spill the data into disk.
         * Because the hash table can not handle this amount of data.
         */
        m_strategy = GRACE_HASH;
        m_partNum = calcPartitionNum();

        WaitState oldStatus = pgstat_report_waitstatus(STATE_EXEC_HASHJOIN_WRITE_FILE);
        flushToDisk<complicateJoinKey>();

        RuntimeBinding(m_funBuild, m_strategy)(batch);
        pgstat_increase_session_spill();
        (void)pgstat_report_waitstatus(oldStatus);
        return;
    }

    if (complicateJoinKey) {
        CalcComplicateHashVal(batch, m_runtime->hj_InnerHashKeys, true);
        memPartition->putHash(m_hashVal, rows);
    }

    memPartition->putBatch(batch);

    /*
     * Check the memory utilization to tell
     * whether to spill the next coming batch.
     */
    judgeMemoryOverflow(0);
}

/*
 * @Description: save the data to file.
 * @in batch - Put the data in batch to file.
 */
template <bool complicateJoinKey, bool optspill>
void SonicHashJoin::saveToDisk(VectorBatch* batch)
{

    int idx = 0;
    int row_idx = 0;
    ScalarValue* arr_val = NULL;
    uint8* null_flag = NULL;
    int nrows = batch->m_rows;
    uint32* part_idx = NULL;

    /* First, calclute the hashVal of each row */
    if (complicateJoinKey) {
        CalcComplicateHashVal(batch, m_runtime->hj_InnerHashKeys, true);
    } else {
        hashBatchArray(batch, (void*)m_buildOp.hashFunc, m_buildOp.hashFmgr, m_buildOp.keyIndx, m_hashVal);
    }

    /* Get partition number for hash value. */
    calcPartIdx(m_hashVal, m_partNum, nrows);

    /* Second, save the batch to disk */
    part_idx = m_partIdx;

    WaitState oldStatus = pgstat_report_waitstatus(STATE_EXEC_HASHJOIN_WRITE_FILE);
    for (row_idx = 0; row_idx < nrows; row_idx++, part_idx++) {
        for (idx = 0; idx < m_buildOp.cols; ++idx) {
            arr_val = batch->m_arr[idx].m_vals + row_idx;
            null_flag = batch->m_arr[idx].m_flag + row_idx;

            Assert(m_innerPartitions[*part_idx] != NULL);
            SonicHashFilePartition* innerPartition = (SonicHashFilePartition*)m_innerPartitions[*part_idx];
            innerPartition->putVal<optspill>(arr_val, null_flag, idx);
        }
    }

    part_idx = m_partIdx;
    for (row_idx = 0; row_idx < nrows; row_idx++, part_idx++) {
        m_innerPartitions[*part_idx]->m_rows += 1;
    }

    /* put hash value to disk if complicateJoinKey is true. */
    if (complicateJoinKey) {
        part_idx = m_partIdx;
        for (row_idx = 0; row_idx < nrows; row_idx++, part_idx++) {
            m_innerPartitions[*part_idx]->putHash(&m_hashVal[row_idx]);
        }
    }
    (void)pgstat_report_waitstatus(oldStatus);
}

/*
 * @Description: Flush the data from memory structure to disk.
 */
template <bool complicateJoinKey>
void SonicHashJoin::flushToDisk()
{
    SonicHashPartition** inner_partitions = NULL;

    {
        AutoContextSwitch memSwitch(m_memControl.hashContext);
        /* Init the hash level. */
        m_pLevel = (uint8*)palloc0(sizeof(uint8) * m_partNum);

        /* Init the mark m_isValid[] */
        m_isValid = (bool*)palloc0(sizeof(bool) * m_partNum);

        /* Before flush, rebuild inner partitions. */
        m_partSizeOrderedIdx = (uint32*)palloc0(sizeof(uint32) * m_partNum);

        inner_partitions = (SonicHashPartition**)palloc0(sizeof(SonicHashPartition*) * m_partNum);
        for (uint32 partIdx = 0; partIdx < m_partNum; ++partIdx) {
            inner_partitions[partIdx] = New(CurrentMemoryContext) SonicHashFilePartition(
                (char*)"innerPartitionContext", complicateJoinKey, m_buildOp.tupleDesc, m_memControl.totalMem);
            initPartition<true>(inner_partitions[partIdx]);
            m_pLevel[partIdx] = 1;
            m_isValid[partIdx] = true;
        }
    }

    if (m_runtime->js.ps.instrument) {
        int64 memory_size = 0;
        CalculateContextSize(CurrentMemoryContext, &memory_size);
        if (m_runtime->js.ps.instrument->memoryinfo.peakOpMemory < memory_size) {
            m_runtime->js.ps.instrument->memoryinfo.peakOpMemory = memory_size;
        }

        /*
         * Record the peak control memory right here,
         * since the m_memControl.hashContext is full now.
         */
        memory_size = 0;
        CalculateContextSize(m_memControl.hashContext, &memory_size);
        if (m_runtime->js.ps.instrument->memoryinfo.peakControlMemory < memory_size) {
            m_runtime->js.ps.instrument->memoryinfo.peakControlMemory = memory_size;
        }
    }

    /*
     * When m_innerPartitions[0] hasn't store any data,
     * it means the current memory cannot fill in any batch,
     * so we return and switch to saveToDisk.
     */
    if (m_innerPartitions[0]->m_rows == 0) {
        Assert(m_innerPartitions[0]->m_size == 0);
        m_innerPartitions[0]->freeResources();
        m_innerPartitions = inner_partitions;

        return;
    }

    SonicHashMemPartition* partition = (SonicHashMemPartition*)m_innerPartitions[0];

    /*
     * Start flush:
     * First we need to access the hash value:
     *	for complicate join, get hash value from m_hash,
     *	for non-complicate join, calculate and store the
     *	hash value in m_hashVal[INIT_DATUM_ARRAY_SIZE] per atom.
     * Second, calcuate the partition index for each row
     * for once and stored in array m_partIdx[].
     * Last, write each row into correspoinding partition,
     * and also write the hashVal if it is a complicate join case.
     */
    uint32* hash_val = NULL;
    int arr_num = partition->m_data[0]->m_arrIdx + 1;
    int nrows = 0;  /* total rows in current atom */
    int row_idx = 0; /* row_idx in current atom */
    int arr_idx = 0; /* current array index in m_arr */
    uint32* part_idx = NULL;

    for (arr_idx = 0; arr_idx < arr_num; ++arr_idx) {
        nrows = (arr_idx < arr_num - 1) ? partition->m_data[0]->m_atomSize : partition->m_data[0]->m_atomIdx;
        /* Calcualte hash values for the atom in a non complicate join case. */
        if (!complicateJoinKey) {
            hashAtomArray(partition->m_data,
                nrows,
                arr_idx,
                (void*)m_buildOp.hashAtomFunc,
                m_buildOp.hashFmgr,
                m_buildOp.keyIndx,
                m_hashVal);
            hash_val = m_hashVal;
        } else {
            hash_val = (uint32*)partition->m_hash->m_arr[arr_idx]->data;
        }

        calcPartIdx(hash_val, m_partNum, nrows);

        partition->flushPartition(arr_idx, m_partIdx, inner_partitions, nrows);

        if (complicateJoinKey) {
            part_idx = m_partIdx;
            row_idx = 0;
            if (unlikely(arr_idx == 0)) {
                /* Starts from the second position. */
                part_idx++;
                hash_val++;
                row_idx = 1;
            }

            for (; row_idx < nrows; row_idx++, part_idx++) {
                inner_partitions[*part_idx]->putHash(hash_val++);
            }
        }
    }

    /* Release the partition in memory. */
    partition->freeResources();
    partition = NULL;
    m_innerPartitions = inner_partitions;
}

/*
 * @Description: Prepare for probe process.
 *	Initial probe functions.
 *	build hashtable with build side data.
 *	Load data from file partition if necessary.
 */
void SonicHashJoin::prepareProbe()
{
    int64 max_partition_rows = 0;
    uint64 sz_hash;
    SonicHashMemPartition* mem_partition = NULL;
    instr_time start_time;

    switch (m_strategy) {
        case MEMORY_HASH: {
            /* Do the join in memory */
            (void)INSTR_TIME_SET_CURRENT(start_time);

            m_probeStatus = PROBE_FETCH;

            Assert(m_innerPartitions[m_probeIdx]);
            Assert(m_innerPartitions[m_probeIdx]->m_status == partitionStatusMemory);

            mem_partition = (SonicHashMemPartition*)m_innerPartitions[m_probeIdx];
            max_partition_rows = mem_partition->m_rows;

            /*
             * If sz_hash is less than or equal to 2 bytes,
             * Use 2 bytes for hash table size.
             * Otherwise use 4 bytes for hash table size.
             * Should consider both m_bucket and m_next size.
             */
            m_hashSize = (int64)calcHashSize(max_partition_rows);
            sz_hash = Max(m_hashSize, max_partition_rows + 1);

            WaitState oldStatus = pgstat_report_waitstatus(STATE_EXEC_HASHJOIN_BUILD_HASH);
            if (((uint64)sz_hash & 0xffff) == (uint64)sz_hash) {
                m_bucketTypeSize = 2;
                initHashTable(2, m_probeIdx);
                if (m_complicatekey) {
                    if (mem_partition->m_segHashTable) {
                        m_probeTypeFun = &SonicHashJoin::probeMemoryTable<uint16, true, true>;
                        buildHashTable<uint16, true, true>(m_probeIdx);
                    } else {
                        m_probeTypeFun = &SonicHashJoin::probeMemoryTable<uint16, true, false>;
                        buildHashTable<uint16, true, false>(m_probeIdx);
                    }
                } else {
                    if (mem_partition->m_segHashTable) {
                        m_probeTypeFun = &SonicHashJoin::probeMemoryTable<uint16, false, true>;
                        buildHashTable<uint16, false, true>(m_probeIdx);
                    } else {
                        m_probeTypeFun = &SonicHashJoin::probeMemoryTable<uint16, false, false>;
                        buildHashTable<uint16, false, false>(m_probeIdx);
                    }
                }
            } else {
                m_bucketTypeSize = 4;
                initHashTable(4, m_probeIdx);
                if (m_complicatekey) {
                    if (mem_partition->m_segHashTable) {
                        m_probeTypeFun = &SonicHashJoin::probeMemoryTable<uint32, true, true>;
                        buildHashTable<uint32, true, true>(m_probeIdx);
                    } else {
                        m_probeTypeFun = &SonicHashJoin::probeMemoryTable<uint32, true, false>;
                        buildHashTable<uint32, true, false>(m_probeIdx);
                    }
                } else {
                    if (mem_partition->m_segHashTable) {
                        m_probeTypeFun = &SonicHashJoin::probeMemoryTable<uint32, false, true>;
                        buildHashTable<uint32, false, true>(m_probeIdx);
                    } else {
                        m_probeTypeFun = &SonicHashJoin::probeMemoryTable<uint32, false, false>;
                        buildHashTable<uint32, false, false>(m_probeIdx);
                    }
                }
            }

            (void)pgstat_report_waitstatus(oldStatus);
            m_build_time += elapsed_time(&start_time);
            PROFILE_PART(0, m_bucketTypeSize);
        } break;
        case GRACE_HASH: {
            (void)INSTR_TIME_SET_CURRENT(start_time);
            m_probeStatus = PROBE_PARTITION_MEM;
            m_probePartStatus = PROBE_FETCH;

            /*
             * Load as many inner partitions as possible,
             * so that during the probe side,
             * few outer partitions will be spilled to disk.
             * We also record the start and last partition index to support profile.
             */
            loadInnerPartitions(m_memControl.totalMem);
            initProbePartitions();

            if (m_partLoadedOffset >= 0) {
                /* All data are put in the partition, which is used to be the smallest partition */
                m_probeIdx = m_partSizeOrderedIdx[0];

                Assert(m_innerPartitions[m_probeIdx]);
                Assert(m_innerPartitions[m_probeIdx]->m_status == partitionStatusMemory);

                mem_partition = (SonicHashMemPartition*)m_innerPartitions[m_probeIdx];
                max_partition_rows = mem_partition->m_rows;
                /*
                 * If sz_hash is less than or equal to 2 bytes,
                 * Use 2 bytes for hash table size.
                 * Otherwise use 4 bytes for hash table size.
                 * Should consider both m_bucket and m_next size.
                 */
                m_hashSize = calcHashSize(max_partition_rows);
                sz_hash = Max(m_hashSize, max_partition_rows + 1);
                if (((uint64)sz_hash & 0xffff) == (uint64)sz_hash) {
                    m_bucketTypeSize = 2;
                    initHashTable(2, m_probeIdx);
                    if (m_complicatekey) {
                        if (mem_partition->m_segHashTable) {
                            m_probeTypeFun = &SonicHashJoin::probePartition<uint16, true, true>;
                            buildHashTable<uint16, true, true>(m_probeIdx);
                        } else {
                            m_probeTypeFun = &SonicHashJoin::probePartition<uint16, true, false>;
                            buildHashTable<uint16, true, false>(m_probeIdx);
                        }
                    } else {
                        if (mem_partition->m_segHashTable) {
                            m_probeTypeFun = &SonicHashJoin::probePartition<uint16, false, true>;
                            buildHashTable<uint16, false, true>(m_probeIdx);
                        } else {
                            m_probeTypeFun = &SonicHashJoin::probePartition<uint16, false, false>;
                            buildHashTable<uint16, false, false>(m_probeIdx);
                        }
                    }
                } else {
                    m_bucketTypeSize = 4;
                    initHashTable(4, m_probeIdx);
                    if (m_complicatekey) {
                        if (mem_partition->m_segHashTable) {
                            m_probeTypeFun = &SonicHashJoin::probePartition<uint32, true, true>;
                            buildHashTable<uint32, true, true>(m_probeIdx);
                        } else {
                            m_probeTypeFun = &SonicHashJoin::probePartition<uint32, true, false>;
                            buildHashTable<uint32, true, false>(m_probeIdx);
                        }
                    } else {
                        if (mem_partition->m_segHashTable) {
                            m_probeTypeFun = &SonicHashJoin::probePartition<uint32, false, true>;
                            buildHashTable<uint32, false, true>(m_probeIdx);
                        } else {
                            m_probeTypeFun = &SonicHashJoin::probePartition<uint32, false, false>;
                            buildHashTable<uint32, false, false>(m_probeIdx);
                        }
                    }
                }

                m_build_time += elapsed_time(&start_time);
                if (m_partLoadedOffset >= 0) {
                    PROFILE_PART(m_probeIdx, m_bucketTypeSize);
                }
            } else {
                if (m_complicatekey)
                    m_probeTypeFun = &SonicHashJoin::probePartition<uint16, true, false>;
                else
                    m_probeTypeFun = &SonicHashJoin::probePartition<uint16, false, false>;
            }

        } break;
        default:
            Assert(false);
            break;
    }
    m_runtime->joinState = HASH_PROBE;
}

/*
 * @Description: Initial hash table.
 * @in byteSize - single hash bucket size.
 * @in curPartIdx - Initial hash table in curPartIdx-th partition.
 */
void SonicHashJoin::initHashTable(uint8 byteSize, uint32 curPartIdx)
{
    int64 nrows = 0;
    int64 sz_hash;
    SonicHashMemPartition* mem_partition = NULL;
    mem_partition = (SonicHashMemPartition*)m_innerPartitions[curPartIdx];

    /* This partition hasn't any data. So return directly. */
    if (mem_partition == NULL)
        return;

    AutoContextSwitch memSwitch(mem_partition->m_context);
    nrows = mem_partition->m_rows;

    /*
     * Here we should guarentee that the nrows should be less than SONIC_MAX_ROWS.
     * To avoid bad performance influence,
     * we check this condition in loadPartition() when we load all data from temp files
     * and in SaveToMemory() when put original data into memory.
     */
    Assert(nrows <= SONIC_MAX_ROWS);

    /*
     * We guarentee m_hashSize is less than uint32, because
     * nrows should be less than SONIC_MAX_ROWS.
     */
    mem_partition->m_hashSize = (uint32)m_hashSize;
    mem_partition->m_bucketTypeSize = byteSize;

    sz_hash = Max((int64)mem_partition->m_hashSize, nrows + 1);
    /* Check whether size of one palloc larger than the limit. */
    bool useSegHashTable = (sz_hash * byteSize >= (int64)MaxAllocSize);

    if (!useSegHashTable) {
        mem_partition->m_bucket = (char*)palloc0(byteSize * mem_partition->m_hashSize);
        /* One more cell is need for the zero in next array. */
        mem_partition->m_next = (char*)palloc0(byteSize * (nrows + 1));

        mem_partition->m_segHashTable = false;
    } else {
        uint64 numSegBucket;
        uint64 numSegNext;
        DatumDesc desc;

        if (byteSize == 2) {
            getDataDesc(&desc, 2, NULL, false);
        } else {
            getDataDesc(&desc, 4, NULL, false);
        }

        /* Init m_segBucket */
        mem_partition->m_segBucket =
            AllocateIntArray(mem_partition->m_context, mem_partition->m_context, INIT_DATUM_ARRAY_SIZE, false, &desc);
        mem_partition->m_segBucket->m_atomIdx = 0;

        numSegBucket = (mem_partition->m_hashSize - 1) / INIT_DATUM_ARRAY_SIZE + 1;
        for (uint64 i = 0; i < numSegBucket; i++) {
            mem_partition->m_segBucket->genNewArray(false);
        }

        /* Init m_segNext */
        mem_partition->m_segNext =
            AllocateIntArray(mem_partition->m_context, mem_partition->m_context, INIT_DATUM_ARRAY_SIZE, false, &desc);
        mem_partition->m_segNext->m_atomIdx = 0;

        numSegNext = nrows / INIT_DATUM_ARRAY_SIZE + 1;
        for (uint64 i = 0; i < numSegNext; i++) {
            mem_partition->m_segNext->genNewArray(false);
        }

        mem_partition->m_segHashTable = true;
    }
}

/*
 * @Description: build the curPartIdx-th partition's hash table.
 * @in curPartIdx - Partition index.
 */
template <typename BucketType, bool complicateJoinKey, bool isSegHashTable>
void SonicHashJoin::buildHashTable(uint32 curPartIdx)
{
    int arrSize;
    int arrNum;
    int i, j;
    uint32* hash_res = NULL;
    uint32* hash_val = NULL;
    uint32 tup_idx = 0;
    uint32 loc_id = 0;
    uint32 mask;

    SonicHashMemPartition* mem_partition = (SonicHashMemPartition*)m_innerPartitions[curPartIdx];
    Assert(mem_partition != NULL);

    BucketType* hashBucket = ((BucketType*)mem_partition->m_bucket);
    BucketType* hashNext = ((BucketType*)mem_partition->m_next);

#ifdef USE_PRIME
    mem_partition->m_mask = mem_partition->m_hashSize;
#else
    mem_partition->m_mask = mem_partition->m_hashSize - 1;
#endif

    /* This partition hasn't any data. So return directly. */
    if (mem_partition->m_rows == 0) {
        return;
    }

    if (!complicateJoinKey) {
        hash_res = m_hashVal;
    }

    mask = mem_partition->m_mask;

    arrNum = mem_partition->m_data[0]->m_arrIdx + 1;

    for (i = 0; i < arrNum; i++) {
        arrSize = (i < arrNum - 1) ? m_atomSize : mem_partition->m_data[0]->m_atomIdx;

        if (complicateJoinKey) {
            hash_res = (uint32*)mem_partition->m_hash->m_arr[i]->data;
            mem_partition->m_hash->m_atomIdx = arrSize;
        } else {
            hashAtomArray(mem_partition->m_data,
                arrSize,
                i,
                (void*)m_buildOp.hashAtomFunc,
                m_buildOp.hashFmgr,
                m_buildOp.keyIndx,
                hash_res);
        }

        hash_val = hash_res;

        /* insert tuple index into hash table. */
        for (j = 0; j < arrSize; j++) {
            /* loc_id is bucket index. */
            loc_id = GETLOCID(*hash_val, mask);

            if (!isSegHashTable) {
                hashNext[tup_idx] = hashBucket[loc_id];
                hashBucket[loc_id] = tup_idx;
            } else {
                /* For now, only use uint32 */
                uint32 segBucketPos = (uint32)mem_partition->m_segBucket->getNthDatum(loc_id);
                mem_partition->m_segNext->setNthDatum(tup_idx, (ScalarValue*)&segBucketPos);
                mem_partition->m_segBucket->setNthDatum(loc_id, (ScalarValue*)&tup_idx);
            }

            tup_idx++;
            hash_val++;
        }
    }
}

/*
 * @Description: Probe side main function.
 * 	Call probeMemory or probeGrace by m_strategy.
 */
VectorBatch* SonicHashJoin::Probe()
{
    return RuntimeBinding(m_probeFun, m_strategy)();
}

/*
 * @Description: Probe function when the whole build side
 * 	data can be stored in memory.
 */
VectorBatch* SonicHashJoin::probeMemory()
{
    return (this->*m_probeTypeFun)(m_hashOpPartition);
}

/*
 * @Description: Probe function when the whole build side data
 *	can be stored in memory or from single partition.
 * @in probeP - The data in probe side can be from SonicHashOpSource
 *	or single SonicHashFilePartition.
 */
template <typename BucketType, bool complicateJoinKey, bool isSegHashTable>
inline VectorBatch* SonicHashJoin::probeMemoryTable(SonicHashSource* probeP)
{
    VectorBatch* res_batch = NULL;
    int nrows;
    BucketType loc_id;
    uint16* loc1 = NULL;
    uint32* loc2 = NULL;
    uint32* loc3 = NULL;
    uint32 mask;
    SonicHashMemPartition* mem_partition = (SonicHashMemPartition*)m_innerPartitions[m_probeIdx];
    BucketType* hashBucket = (BucketType*)mem_partition->m_bucket;

    Assert(mem_partition->m_status == partitionStatusMemory);

    mask = mem_partition->m_mask;

    for (;;) {
        switch (m_probeStatus) {
            case PROBE_FETCH: {
                m_outRawBatch = probeP->getBatch();

                /* If no more data from probe side, goto state PROBE_FINAL. */
                if (unlikely(BatchIsNull(m_outRawBatch))) {
                    m_probeStatus = PROBE_FINAL;
                    break;
                }

                nrows = m_outRawBatch->m_rows;

                /* Calcute hash value of probe side data. */
                if (complicateJoinKey) {
                    CalcComplicateHashVal(m_outRawBatch, m_runtime->hj_OuterHashKeys, false);
                } else {
                    hashBatchArray(
                        m_outRawBatch, (void*)m_probeOp.hashFunc, m_probeOp.hashFmgr, m_probeOp.keyIndx, m_hashVal);
                }

                m_selectRows = 0;
                loc3 = m_hashVal;
                loc1 = m_selectIndx;
                loc2 = m_loc;
                /*
                 * Iterate probe data to find whether
                 * the hash value between build and probe is same.
                 */
                for (int i = 0; i < nrows; i++, loc3++) {
                    if (isSegHashTable) {
                        loc_id = (BucketType)mem_partition->m_segBucket->getNthDatum(GETLOCID(*loc3, mask));
                    } else {
                        loc_id = hashBucket[GETLOCID(*loc3, mask)];
                    }

                    /*
                     * If the hash value is same between build and probe,
                     * record both positions.
                     */
                    if (loc_id) {
                        *loc1++ = i;
                        *loc2++ = loc_id;
                        m_match[m_selectRows++] = true;
                    }
                }
                m_probeStatus = PROBE_DATA;
            } break;

            case PROBE_DATA: {
                /* goto join process. */
                res_batch = innerJoin<BucketType, complicateJoinKey, isSegHashTable, false>(m_outRawBatch);
                if (!BatchIsNull(res_batch)) {
                    return res_batch;
                }
            } break;
            case PROBE_FINAL:
                return NULL;
                /* keep compiler quiet */
                break;
            default:
                break;
        }
    }
    return NULL;
}

/*
 * @Description: probe function when there are more than one partition.
 */
VectorBatch* SonicHashJoin::probeGrace()
{
    VectorBatch* res_batch = NULL;
    while (true) {
        switch (m_probeStatus) {
            case PROBE_PARTITION_MEM: {
                /*
                 * Call probePartition() do in-mem probe and join at first.
                 * After it, change state to PROBE_PREPARE_PAIR.
                 */
                res_batch = (this->*m_probeTypeFun)(NULL);
                if (!BatchIsNull(res_batch)) {
                    return res_batch;
                }

                /* Change to single partition join */
                m_probeStatus = PROBE_PREPARE_PAIR;
            } break;
            case PROBE_PREPARE_PAIR: {
                /* Find one partition pair can join. */
                bool found = preparePartition();

                /* All finished. */
                if (!found) {
                    return NULL;
                }
                m_probeStatus = PROBE_FETCH;
            } break;
            case PROBE_FETCH:
            case PROBE_DATA:
            case PROBE_FINAL: {
                /* Do join between one partition pair. */
                res_batch = (this->*m_probeTypeFun)(m_outerPartitions[m_probeIdx]);
                if (!BatchIsNull(res_batch)) {
                    return res_batch;
                } else {
                    Assert(m_innerPartitions[m_probeIdx] &&
                           m_innerPartitions[m_probeIdx]->m_status == partitionStatusMemory);
                    Assert(m_outerPartitions[m_probeIdx] &&
                           m_outerPartitions[m_probeIdx]->m_status == partitionStatusFile);
                    /* Done join on this partition, should release context and close files. */
                    finishJoinPartition(m_probeIdx);

                    /* go to state PROBE_PREPARE_PAIR */
                    m_probeStatus = PROBE_PREPARE_PAIR;
                }
            } break;
            default:
                ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_OBJECT),
                        errmodule(MOD_VEC_EXECUTOR),
                        (errmsg("Unrecognized vector sonic hashjoin run status."))));
                break;
        }
    }
}

/*
 * @Description: Fine one file partition pair can join.
 * @return - false when cannot get any pair because all partitions are finished searching.
 */
bool SonicHashJoin::preparePartition()
{
    Assert(m_probeIdx < m_partNum);
    int64 build_side_row_num = 0;           /* number of rows of the found inner partition */
    int64 probe_side_row_num = 0;           /* number of rows of the found outer partition */
    uint32 idx = m_partLoadedOffset + 1; /* offset of the partition to load */

    /*
     * Search for a valid partition that can be fit in the memory.
     * If an inner partition is too large for the current memory,
     * repartition the inner partition and the coresponding outer partition,
     * then search for the next, until we found a vaild one.
     */
    WaitState old_status = pgstat_report_waitstatus(STATE_EXEC_HASHJOIN_BUILD_HASH);
    for (; idx < m_partNum; ++idx) {
        m_probeIdx = m_partSizeOrderedIdx[idx];
        m_partLoadedOffset += 1;

        SonicHashPartition* buildP = m_innerPartitions[m_probeIdx];
        SonicHashPartition* probeP = m_outerPartitions[m_probeIdx];

        Assert(buildP != NULL);
        Assert(probeP != NULL);

        /* check if both partition stored in temp file */
        if (buildP->m_status != partitionStatusFile || probeP->m_status != partitionStatusFile) {
            finishJoinPartition(m_probeIdx);
            continue;
        }

        build_side_row_num = buildP->m_rows;
        probe_side_row_num = probeP->m_rows;
        bool join_this_partition = (build_side_row_num > 0) && (probe_side_row_num > 0);

        /*
         * If either inner or outer partition has no data,
         * so there is no need to probe the two partitions.
         */
        if (!join_this_partition) {
            /* free resources. */
            finishJoinPartition(m_probeIdx);
            continue;
        }

        /*
         * Check if we have enough memory for the partition:
         *
         * If not, Hash Join can not be done on this partition pair directly;
         * in this case, we repartition the partition into smaller partitions which will be
         * added to the current partitions set.
         *
         * Note: As for partitions which are proved to be invalid, we allow this repartition process.
         * These new partitions after repartition process will be marked as m_isValid=true, unless the
         * max rownum among these equals to rownum of the current partition.
         * In this case, if m_isValid is marked as false, this partition will not be repartitioned any more
         * and we print warning messages next time. Because it mostly caused by the duplicated hash table.
         */
        size_t size = size_t(((SonicHashFilePartition*)buildP)->m_varSize +
                             m_arrayExpandSize * ceil((double)build_side_row_num / (double)INIT_DATUM_ARRAY_SIZE));

        /* Add estimated hash table size */
        size += get_hash_head_size(build_side_row_num);

        if (m_isValid[m_probeIdx] && size > (m_memControl.totalMem)) {
            int64 old_part_row_num = buildP->m_rows;
            uint32 old_part_nums = m_partNum;

            int64 max_sub_part_row_num;
            uint32 i;

            /* Repartition process. */
            if (m_complicatekey) {
                rePartition<true>(m_probeIdx);
            } else {
                rePartition<false>(m_probeIdx);
            }

            /* If this partition is already partitioned 'm_maxPLevel' times, we would pay extra attention to it.*/
            if (m_pLevel[m_probeIdx] >= m_maxPLevel) {
                elog(LOG,
                    "[VecHashJoin] Warning: file [%u] has partitioned %d times, which exceeds 'm_maxPLevel' times. "
                    "Please pay attention to it.",
                    m_probeIdx,
                    m_pLevel[m_probeIdx]);
            }

            /* Find the max rowNum from new partitions repartitioned. */
            max_sub_part_row_num = m_innerPartitions[old_part_nums]->m_rows;
            for (i = old_part_nums + 1; i < m_partNum; i++)
                max_sub_part_row_num = Max(max_sub_part_row_num, m_innerPartitions[i]->m_rows);

            /* For a invalid repartition process, mark the new partitions as m_isValid = false.*/
            if (max_sub_part_row_num == old_part_row_num) {
                for (i = old_part_nums; i < m_partNum; i++)
                    m_isValid[i] = false;

                elog(LOG,
                    "[Vec SonicHashJoin] Warning: in partition[%u], after %d times of repartition, data to be built "
                    "hash table may be "
                    "duplicated or too large, try ANALYZE first to get better plan.",
                    m_probeIdx,
                    m_pLevel[m_probeIdx]);
            }

            finishJoinPartition(m_probeIdx);
            continue;
        }

        uint64 szHash;
        SonicHashMemPartition* mem_partition = NULL;
        /* Load build side m_probeIdx-th partition. */
        loadInnerPartition(m_probeIdx);
        mem_partition = (SonicHashMemPartition*)m_innerPartitions[m_probeIdx];

        Assert(probeP->m_status == partitionStatusFile);
        /* Prepare probe side file buffer. */
        ((SonicHashFilePartition*)probeP)->prepareFileHandlerBuffer();
        /* Rewind probe side file partition. */
        ((SonicHashFilePartition*)probeP)->rewindFiles();

        /* Build hash table for this partition. */
        m_hashSize = (int64)calcHashSize(build_side_row_num);
        szHash = Max(m_hashSize, build_side_row_num + 1);
        if (((uint64)szHash & 0xffff) == (uint64)szHash) {
            m_bucketTypeSize = 2;
            initHashTable(2, m_probeIdx);
            if (m_complicatekey) {
                if (mem_partition->m_segHashTable) {
                    m_probeTypeFun = &SonicHashJoin::probeMemoryTable<uint16, true, true>;
                    buildHashTable<uint16, true, true>(m_probeIdx);
                } else {
                    m_probeTypeFun = &SonicHashJoin::probeMemoryTable<uint16, true, false>;
                    buildHashTable<uint16, true, false>(m_probeIdx);
                }
            } else {
                if (mem_partition->m_segHashTable) {
                    m_probeTypeFun = &SonicHashJoin::probeMemoryTable<uint16, false, true>;
                    buildHashTable<uint16, false, true>(m_probeIdx);
                } else {
                    m_probeTypeFun = &SonicHashJoin::probeMemoryTable<uint16, false, false>;
                    buildHashTable<uint16, false, false>(m_probeIdx);
                }
            }
        } else {
            m_bucketTypeSize = 4;
            initHashTable(4, m_probeIdx);
            if (m_complicatekey) {
                if (mem_partition->m_segHashTable) {
                    m_probeTypeFun = &SonicHashJoin::probeMemoryTable<uint32, true, true>;
                    buildHashTable<uint32, true, true>(m_probeIdx);
                } else {
                    m_probeTypeFun = &SonicHashJoin::probeMemoryTable<uint32, true, false>;
                    buildHashTable<uint32, true, false>(m_probeIdx);
                }
            } else {
                if (mem_partition->m_segHashTable) {
                    m_probeTypeFun = &SonicHashJoin::probeMemoryTable<uint32, false, true>;
                    buildHashTable<uint32, false, true>(m_probeIdx);
                } else {
                    m_probeTypeFun = &SonicHashJoin::probeMemoryTable<uint32, false, false>;
                    buildHashTable<uint32, false, false>(m_probeIdx);
                }
            }
        }
        PROFILE_PART(m_probeIdx, m_bucketTypeSize);
        (void)pgstat_report_waitstatus(old_status);
        return true;
    }
    (void)pgstat_report_waitstatus(old_status);
    return false;
}

/*
 * @Description: Inner join function.
 * @in batch - batch from probe side.
 */
template <typename BucketType, bool complicateJoinKey, bool isSegHashTable, bool isPartStatus>
inline VectorBatch* SonicHashJoin::innerJoin(VectorBatch* batch)
{

    int i = 0;
    int j = 0;
    BucketType loc_id;
    ScalarValue* vals = NULL;
    uint8* flags = NULL;
    ScalarValue* src_vals = NULL;
    uint8* src_flags = NULL;
    uint16 left_rows = 0;
    uint16 rows = 0;
    uint32* loc1 = NULL;
    uint32* loc2 = NULL;
    uint16* loc3 = NULL;
    uint16* loc4 = NULL;
    bool* boolloc = NULL;

    SonicHashMemPartition* mem_partition = (SonicHashMemPartition*)m_innerPartitions[m_probeIdx];

    Assert(mem_partition && mem_partition->m_status == partitionStatusMemory);

    BucketType* hashNext = (BucketType*)mem_partition->m_next;

    while (m_selectRows) {
        /* match inner and outer keys. */
        if (complicateJoinKey) {
            matchComplicateKey<false>(batch, mem_partition);
        }
        else {
            for (i = 0; i < m_buildOp.keyNum; i++) {
                ScalarVector* val = &batch->m_arr[m_probeOp.keyIndx[i]];
                (this->*m_matchKey[i])(val, mem_partition->m_data[m_buildOp.keyIndx[i]], m_selectRows, i);
            }
        }

        /*
         * record matched inner and outer tuples.
         * Note: m_matchLocIndx maybe not zero.
         * Because some tuples may not be outputed in last cycle.
         */
        loc1 = &m_innerMatchLoc[m_matchLocIndx];
        loc3 = &m_outerMatchLoc[m_matchLocIndx];
        boolloc = m_match;
        loc2 = m_loc;
        loc4 = m_selectIndx;
        for (i = 0; i < m_selectRows; i++) {
            if (*boolloc++) {
                *loc1++ = *loc2;
                *loc3++ = *loc4;
                m_matchLocIndx++;
            }
            loc2++;
            loc4++;
        }

        /* Try to find the next inner tuple in the conflict array. */
        rows = 0;
        loc3 = m_selectIndx;
        loc4 = m_selectIndx;
        loc1 = m_loc;
        loc2 = m_loc;
        boolloc = m_match;
        for (i = 0; i < m_selectRows; i++) {
            if (isSegHashTable) {
                loc_id = (BucketType)mem_partition->m_segNext->getNthDatum(*loc2++);
            } else {
                loc_id = hashNext[*loc2++];
            }

            if (loc_id) {
                *loc3++ = *loc4;
                *loc1++ = loc_id;
                rows++;
                *boolloc++ = true;
            }
            loc4++;
        }
        m_selectRows = rows;

        /* Assemble result batch when we get enough matched tuples. */
        if (m_matchLocIndx >= BatchMaxSize) {
            /* Construct build side VectorBatch with data in SonicDatumArray. */
            mem_partition->m_data[0]->getArrayAtomIdx(BatchMaxSize, m_innerMatchLoc, m_arrayIdx);
            for (i = 0; i < m_buildOp.cols; i++) {
                vals = m_buildOp.batch->m_arr[i].m_vals;
                flags = m_buildOp.batch->m_arr[i].m_flag;
                mem_partition->m_data[i]->getDatumFlagArray(BatchMaxSize, m_arrayIdx, vals, flags);
            }

            /* Construct probe side VectorBatch with data in batch passed in. */
            for (i = 0; i < m_probeOp.cols; i++) {
                vals = m_probeOp.batch->m_arr[i].m_vals;
                src_vals = batch->m_arr[i].m_vals;
                flags = m_probeOp.batch->m_arr[i].m_flag;
                src_flags = batch->m_arr[i].m_flag;
                loc3 = m_outerMatchLoc;
                for (j = 0; j < BatchMaxSize; j++) {
                    *vals++ = src_vals[*loc3];
                    *flags++ = src_flags[*loc3];
                    loc3++;
                }
            }

            left_rows = m_matchLocIndx - BatchMaxSize;

            /*
             * The matched tuples maybe larger than BatchMaxSize.
             * Move the rest un-output tuples to output in next cycle.
             */
            if (left_rows) {
                errno_t rc;

                rc = memmove_s(m_innerMatchLoc,
                    BatchMaxSize * sizeof(uint32),
                    &m_innerMatchLoc[BatchMaxSize],
                    left_rows * sizeof(uint32));
                securec_check(rc, "\0", "\0");

                rc = memmove_s(m_outerMatchLoc,
                    BatchMaxSize * sizeof(uint16),
                    &m_outerMatchLoc[BatchMaxSize],
                    left_rows * sizeof(uint16));
                securec_check(rc, "\0", "\0");
            }
            m_matchLocIndx = left_rows;
            m_buildOp.batch->FixRowCount(BatchMaxSize);
            m_probeOp.batch->FixRowCount(BatchMaxSize);
            return buildRes(m_buildOp.batch, m_probeOp.batch);
        }
    }

    /*
     * End join with current batch.
     * Try to fetch more data from probe side.
     */
    if (isPartStatus) {
        m_probePartStatus = PROBE_FETCH;
    } else {
        m_probeStatus = PROBE_FETCH;
    }

    /* Assemble result batch with the left matched data. */
    if (m_matchLocIndx > 0) {
        mem_partition->m_data[0]->getArrayAtomIdx(m_matchLocIndx, m_innerMatchLoc, m_arrayIdx);
        for (i = 0; i < m_buildOp.cols; i++) {
            vals = m_buildOp.batch->m_arr[i].m_vals;
            flags = m_buildOp.batch->m_arr[i].m_flag;
            mem_partition->m_data[i]->getDatumFlagArray(m_matchLocIndx, m_arrayIdx, vals, flags);
        }

        for (i = 0; i < m_probeOp.cols; i++) {
            vals = m_probeOp.batch->m_arr[i].m_vals;
            src_vals = batch->m_arr[i].m_vals;
            flags = m_probeOp.batch->m_arr[i].m_flag;
            src_flags = batch->m_arr[i].m_flag;
            loc3 = m_outerMatchLoc;
            for (j = 0; j < m_matchLocIndx; j++) {
                *vals++ = src_vals[*loc3];
                *flags++ = src_flags[*loc3];
                loc3++;
            }
        }

        m_buildOp.batch->FixRowCount(m_matchLocIndx);
        m_probeOp.batch->FixRowCount(m_matchLocIndx);
        m_matchLocIndx = 0;
        return buildRes(m_buildOp.batch, m_probeOp.batch);
    } else
        return NULL;
}

/*
 * @Description: probe function for when multiple build partitions in memory.
 * @in probeP - Should be NULL for this function.
 */
template <typename BucketType, bool complicateJoinKey, bool isSegHashTable>
inline VectorBatch* SonicHashJoin::probePartition(SonicHashSource* probeP)
{
    PlanState* outer_node = outerPlanState(m_runtime);
    VectorBatch* res_batch = NULL;
    uint16 nrows;
    BucketType loc_id;

    uint16* loc1 = NULL;
    uint32* loc2 = NULL;
    uint32* loc3 = NULL;
    uint32* loc4 = NULL;
    uint32 part_idx;
    SonicHashMemPartition* mem_partition = (SonicHashMemPartition*)m_innerPartitions[m_probeIdx];
    PartitionStatus part_status;
    uint32 mask = 0;
    BucketType* hash_bucket = NULL;
    SonicDatumArray* seg_bucket = NULL;

    if (m_partLoadedOffset >= 0) {
        mask = mem_partition->m_mask;
        seg_bucket = mem_partition->m_segBucket;
        hash_bucket = (BucketType*)mem_partition->m_bucket;
    }

    for (;;) {
        /*
         * Use another status m_probePartStatus here.
         * Don't interfere with upper status in ProbeGrace().
         */
        switch (m_probePartStatus) {
            case PROBE_FETCH: {
                m_outRawBatch = VectorEngine(outer_node);
                /* If no more data from probe side, goto state PROBE_FINAL. */
                if (unlikely(BatchIsNull(m_outRawBatch))) {
                    m_probePartStatus = PROBE_FINAL;
                    break;
                }

                nrows = m_outRawBatch->m_rows;
                /* Calcute hash value of probe side data. */
                if (complicateJoinKey) {
                    /* Store hash value of complicate join key into m_hashVal[]. */
                    CalcComplicateHashVal(m_outRawBatch, m_runtime->hj_OuterHashKeys, false);
                } else {
                    hashBatchArray(
                        m_outRawBatch, (void*)m_probeOp.hashFunc, m_probeOp.hashFmgr, m_probeOp.keyIndx, m_hashVal);
                }

                m_selectRows = 0;
                loc3 = m_hashVal;
                loc1 = m_selectIndx;
                loc2 = m_loc;
                loc4 = m_partLoc;
                /*
                 * Iterate probe data to find whether
                 * the hash value between build and probe is same.
                 */
                for (int i = 0; i < nrows; i++, loc3++) {
                    part_idx = *loc3 % m_partNum;

                    /* check the status of the inner partition */
                    if (!m_memPartFlag[part_idx]) {
                        part_status = m_innerPartitions[part_idx]->m_status;
                        if (part_status == partitionStatusFinish) {
                            /*
                             * The inner partition has no data and labled as finish
                             * so there is no need to probe the data.
                             */
                            continue;
                        }

                        /* Should be file partition and save the data into temp file. */
                        Assert(part_status == partitionStatusFile);
                        m_diskPartIdx[m_diskPartNum].partIdx = part_idx;
                        m_diskPartIdx[m_diskPartNum++].rowIdx = i;

                        /* The data in file partition are processed later. */
                        continue;
                    }

                    Assert(mem_partition && mem_partition->m_status == partitionStatusMemory);

                    if (isSegHashTable) {
                        loc_id = (BucketType)seg_bucket->getNthDatum(GETLOCID(*loc3, mask));
                    } else {
                        loc_id = hash_bucket[GETLOCID(*loc3, mask)];
                    }

                    /*
                     * If the hash value is same between build and probe,
                     * record both positions, and partition index of build side.
                     */
                    if (loc_id) {
                        *loc1++ = i;
                        *loc2++ = loc_id;
                        m_match[m_selectRows++] = true;
                    }
                }
                if ((m_partLoadedOffset >= 0) && (mem_partition->m_rows > 0)) {
                    m_probePartStatus = PROBE_DATA;
                } else {
                    m_probePartStatus = PROBE_FETCH;
                }
                /* Save data to file partition. */
                WaitState oldStatus = pgstat_report_waitstatus(STATE_EXEC_HASHJOIN_WRITE_FILE);
                (this->*m_saveProbePartition)();
                (void)pgstat_report_waitstatus(oldStatus);
            } break;

            case PROBE_DATA: {
                /* Goto join process. */
                res_batch = innerJoin<BucketType, complicateJoinKey, isSegHashTable, true>(m_outRawBatch);
                if (!BatchIsNull(res_batch))
                    return res_batch;
            } break;
            case PROBE_FINAL:
                /* Free the probed partitions. */
                for (uint32 i = 0; i < m_partNum; i++) {
                    if (m_memPartFlag[i]) {
                        /*
                         * Free the inner and outer partition.
                         * If they are freed, make sure in the following procedure,
                         * those partition won't be called again.
                         */
                        finishJoinPartition(i);
                    }
                }

                /* done spilling probe side, record the probe info and the partition info */
                reportSorthashinfo(reportTypeProbe, m_partNum);
                HASH_BASED_DEBUG(recordPartitionInfo(false, -1, 0, m_partNum));
                /* release all the temp file buffer for all the unfinished outer partitions */
                releaseAllFileHandlerBuffer(false);
                return NULL;
            default:
                break;
        }
    }
    return NULL;
}

/*
 * @Description: repartition process on a specific partition,
 * 	when this function is called, both inner and outer should have data.
 * @in rePartIdx - partition index need to be repartition.
 */
template <bool complicateJoinKey>
void SonicHashJoin::rePartition(uint32 rePartIdx)
{
    VectorBatch* batch = NULL;
    uint32 part_num = 0;      /* the number of partitions that we created due to repartition */
    uint32 total_part_num = 0; /* the total number of partitions after repartition */
    int nrows = 0;           /* batch rows */
    int row_idx = 0;
    uint32 idx = 0;
    uint32 rbit = 0; /* hash rot bits */
    ScalarValue* arr_val = NULL;
    uint8* null_flag = NULL;
    uint32* part_idx = NULL;
    bool is_inner = false;
    SonicHashFilePartition* cur_partition = NULL;
    SonicHashPartition** cur_partition_source = NULL;

    AutoContextSwitch memSwitch(m_memControl.hashContext);

    /* compute repartition numbers */
    part_num = getPower2NextNum(m_innerPartitions[rePartIdx]->m_size / m_memControl.totalMem);
    part_num = Max(2, part_num);
    part_num = Min(part_num, SONIC_PART_MAX_NUM);

    total_part_num = m_partNum + part_num;

    Assert(m_pLevel != NULL);
    m_pLevel = (uint8*)repalloc(m_pLevel, total_part_num * sizeof(uint8));

    m_isValid = (bool*)repalloc(m_isValid, total_part_num * sizeof(bool));

    m_partSizeOrderedIdx = (uint32*)repalloc(m_partSizeOrderedIdx, total_part_num * sizeof(uint32));

    m_innerPartitions = (SonicHashPartition**)repalloc(m_innerPartitions, total_part_num * sizeof(SonicHashPartition*));
    m_outerPartitions = (SonicHashPartition**)repalloc(m_outerPartitions, total_part_num * sizeof(SonicHashPartition*));

    for (idx = m_partNum; idx < total_part_num; ++idx) {
        m_innerPartitions[idx] = New(CurrentMemoryContext) SonicHashFilePartition(
            (char*)"innerPartitionContext", complicateJoinKey, m_buildOp.tupleDesc, m_memControl.totalMem);

        m_outerPartitions[idx] = New(CurrentMemoryContext) SonicHashFilePartition(
            (char*)"outerPartitionContext", complicateJoinKey, m_probeOp.tupleDesc, m_memControl.totalMem);
        initPartition<true>(m_innerPartitions[idx]);
        initPartition<false>(m_outerPartitions[idx]);

        /* increase the hash level for the new partitions */
        m_pLevel[idx] = m_pLevel[rePartIdx] + 1;

        /* initial the m_isValid for enlarged partitions */
        m_isValid[idx] = true;

        /* append the new partitions into size ordered partition indexes */
        m_partSizeOrderedIdx[idx] = idx;
    }

    /* rot bits: avoid using same bits of hashvalue everytime */
    rbit = m_pLevel[rePartIdx] * 10;

    /*
     * Repartition process:
     * First, repartition the inner partition,
     * then, deal the the outer partition.
     */
    cur_partition_source = m_innerPartitions;
    cur_partition = (SonicHashFilePartition*)m_innerPartitions[rePartIdx];
    cur_partition->prepareFileHandlerBuffer();
    cur_partition->rewindFiles();
    is_inner = true;
    WaitState oldStatus = pgstat_report_waitstatus(STATE_EXEC_HASHJOIN_WRITE_FILE);
    for (;;) {
        batch = cur_partition->getBatch();
        if (unlikely(BatchIsNull(batch))) {
            if (is_inner) {
                /*
                 * Has finished dealing with the inner partition,
                 * so move to handle the outer partition.
                 */
                cur_partition_source = m_outerPartitions;
                cur_partition = (SonicHashFilePartition*)m_outerPartitions[rePartIdx];
                cur_partition->prepareFileHandlerBuffer();
                cur_partition->rewindFiles();
                is_inner = false;
                continue;
            }
            /* Has finished both inner and outer partitions. */
            break;
        }

        nrows = batch->m_rows;

        /* prepare hashVal */
        if (complicateJoinKey) {
            cur_partition->getHash(m_hashVal, nrows);
        } else {
            /* we do not have hashvalue yet, so compute it first */
            if (is_inner) {
                hashBatchArray(batch, (void*)m_buildOp.hashFunc, m_buildOp.hashFmgr, m_buildOp.keyIndx, m_hashVal);
            } else {
                hashBatchArray(batch, (void*)m_probeOp.hashFunc, m_probeOp.hashFmgr, m_probeOp.keyIndx, m_hashVal);
            }
        }

        /* prepare part_idx for each row */
        calcRePartIdx(m_hashVal, part_num, rbit, nrows);

        /* save the batch to disk */
        part_idx = m_partIdx;
        if (u_sess->attr.attr_sql.enable_sonic_optspill) {
            for (row_idx = 0; row_idx < nrows; row_idx++, part_idx++) {
                SonicHashFilePartition* filePatition = (SonicHashFilePartition*)cur_partition_source[*part_idx];
                for (uint16 i = 0; i < cur_partition->m_cols; ++i) {
                    arr_val = batch->m_arr[i].m_vals + row_idx;
                    null_flag = batch->m_arr[i].m_flag + row_idx;

                    filePatition->putVal<true>(arr_val, null_flag, i);
                }
            }
        } else {
            for (uint16 i = 0; i < cur_partition->m_cols; ++i) {
                row_idx = 0;
                part_idx = m_partIdx;
                arr_val = batch->m_arr[i].m_vals;
                null_flag = batch->m_arr[i].m_flag;
                for (row_idx = 0; row_idx < nrows; row_idx++, part_idx++) {
                    ((SonicHashFilePartition*)cur_partition_source[*part_idx])->putVal<false>(arr_val, null_flag, i);
                    arr_val++;
                    null_flag++;
                }
            }
        }
        /* Fix the number of rows in each partition */
        part_idx = m_partIdx;
        for (row_idx = 0; row_idx < nrows; row_idx++, part_idx++) {
            cur_partition_source[*part_idx]->m_rows += 1;
        }

        if (complicateJoinKey) {
            part_idx = m_partIdx;
            for (row_idx = 0; row_idx < nrows; row_idx++, part_idx++) {
                cur_partition_source[*part_idx]->putHash(&m_hashVal[row_idx]);
            }
        }
    }
    (void)pgstat_report_waitstatus(oldStatus);

    /* after repartition, release the buffer in file handler */
    for (idx = m_partNum; idx < total_part_num; ++idx) {
        ((SonicHashFilePartition*)m_innerPartitions[idx])->releaseFileHandlerBuffer();
        ((SonicHashFilePartition*)m_outerPartitions[idx])->releaseFileHandlerBuffer();
    }

    HASH_BASED_DEBUG(recordPartitionInfo(true, m_probeIdx, m_partNum, total_part_num));
    HASH_BASED_DEBUG(recordPartitionInfo(false, m_probeIdx, m_partNum, total_part_num));

    if (m_pLevel[rePartIdx] >= WARNING_SPILL_TIME) {
        t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->warning =
            (unsigned int)(t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->warning) | (1 << WLM_WARN_SPILL_TIMES_LARGE);
    }

    /* add instrument */
    reportSorthashinfo(reportTypeRepartition, total_part_num, rePartIdx);

    /* after repartition, fix the m_partNum for now we have total_part_num of partitions */
    m_partNum = total_part_num;
}

/*
 * Description: Delete memory context.
 */
void SonicHashJoin::freeMemoryContext()
{
    if (m_memControl.hashContext != NULL) {
        /* Delete child context for hashContext */
        MemoryContextDelete(m_memControl.hashContext);
        m_memControl.hashContext = NULL;
        m_innerPartitions = NULL;
        m_outerPartitions = NULL;
    }
    if (m_memControl.tmpContext != NULL) {
        /* Delete child context for tmpContext */
        MemoryContextDelete(m_memControl.tmpContext);
        m_memControl.tmpContext = NULL;
    }
}

/*
 * @Description: Release build and probe partition.
 * @in partIdx - Partition index.
 */
void SonicHashJoin::finishJoinPartition(uint32 partIdx)
{
    if (m_innerPartitions && m_innerPartitions[partIdx] != NULL) {
        m_innerPartitions[partIdx]->freeResources();
        m_innerPartitions[partIdx] = NULL;
    }

    if (m_outerPartitions && m_outerPartitions[partIdx] != NULL) {
        m_outerPartitions[partIdx]->freeResources();
        m_outerPartitions[partIdx] = NULL;
    }
}

/*
 * @Description: Close certain build and probe file.
 * @in partIdx: The build and side partition index to be closed.
 */
void SonicHashJoin::closePartFiles(uint32 partIdx)
{
    if (m_innerPartitions && m_innerPartitions[partIdx] && m_innerPartitions[partIdx]->m_status == partitionStatusFile)
        ((SonicHashFilePartition*)m_innerPartitions[partIdx])->closeFiles();

    if (m_outerPartitions && m_outerPartitions[partIdx] && m_outerPartitions[partIdx]->m_status == partitionStatusFile)
        ((SonicHashFilePartition*)m_outerPartitions[partIdx])->closeFiles();
}

/*
 * @Description: Close all build and probe files
 */
void SonicHashJoin::closeAllFiles()
{
    for (uint32 i = 0; i < m_partNum; i++) {
        closePartFiles(i);
    }
}

/*
 * @Description: Build output with inBatch and outBatch.
 * @in inBatch - Build side batch.
 * @in outBatch - Probe side batch.
 */
VectorBatch* SonicHashJoin::buildRes(VectorBatch* inBatch, VectorBatch* outBatch)
{
    ExprContext* econtext = NULL;
    VectorBatch* res_batch = NULL;
    ScalarVector* pvector = NULL;
    bool has_qual = false;

    DBG_ASSERT(inBatch->m_rows == outBatch->m_rows);

    ResetExprContext(m_runtime->js.ps.ps_ExprContext);

    /* Testing whether ps_ProjInfo is NULL */
    Assert(m_runtime->js.ps.ps_ProjInfo);
    econtext = m_runtime->js.ps.ps_ProjInfo->pi_exprContext;
    initEcontextBatch(NULL, outBatch, inBatch, NULL);

    if (m_runtime->js.joinqual != NULL) {
        has_qual = true;
        econtext->ecxt_scanbatch = inBatch;

        if (m_runtime->jitted_joinqual) {
            pvector = m_runtime->jitted_joinqual(econtext);
        } else {
            pvector = ExecVecQual(m_runtime->js.joinqual, econtext, false);
        }

        if (pvector == NULL) {
            inBatch->Reset();
            outBatch->Reset();
            return NULL;
        }
    }

    if (m_runtime->js.ps.qual != NULL) {
        has_qual = true;
        econtext->ecxt_scanbatch = inBatch;
        pvector = ExecVecQual(m_runtime->js.ps.qual, econtext, false);

        if (pvector == NULL) {
            inBatch->Reset();
            outBatch->Reset();
            return NULL;
        }
    }

    if (has_qual) {
        outBatch->PackT<true, false>(econtext->ecxt_scanbatch->m_sel);
        inBatch->PackT<true, false>(econtext->ecxt_scanbatch->m_sel);
    }

    initEcontextBatch(NULL, outBatch, inBatch, NULL);
    res_batch = ExecVecProject(m_runtime->js.ps.ps_ProjInfo);
    if (res_batch->m_rows != inBatch->m_rows) {
        res_batch->FixRowCount(inBatch->m_rows);
    }

    inBatch->Reset();
    outBatch->Reset();

    return res_batch;
}

/*
 * @Description: profile all hash tables in memory partition.
 * @in writeLog - If true, report log.
 * @in partIdx - index of the partition needed to profile.
 * @in bucketTypeSize - hash table size of one bucket.
 */
void SonicHashJoin::profile(bool writeLog, uint32 partIdx, uint8 bucketTypeSize)
{
    Assert(0 <= partIdx && partIdx < m_partNum);
    Assert(m_innerPartitions[partIdx]);

    SonicHashMemPartition* mem_partition = NULL;
    char stats[MAX_LOG_LEN];

    if (m_innerPartitions[partIdx]->m_status == partitionStatusFinish) {
        return;
    }

    Assert(m_innerPartitions[partIdx]->m_status == partitionStatusMemory);
    mem_partition = (SonicHashMemPartition*)m_innerPartitions[partIdx];
    if (bucketTypeSize == 2)
        if (mem_partition->m_segHashTable)
            profileFunc<uint16, true>(stats, partIdx);
        else
            profileFunc<uint16, false>(stats, partIdx);
    else if (mem_partition->m_segHashTable)
        profileFunc<uint32, true>(stats, partIdx);
    else
        profileFunc<uint32, false>(stats, partIdx);
    if (writeLog) {
        ereport(LOG,
            (errmodule(MOD_VEC_EXECUTOR),
                errmsg("[SonicHashJoin(%d)(total parts:%u)(part idx:%u)] %s",
                    m_runtime->js.ps.plan->plan_node_id,
                    m_partNum,
                    partIdx,
                    stats)));
    }
}

/*
 * @Description: profile hash table in specified partition.
 * @out stats: string records hash table profiling results.
 * @in partIdx: partition index.
 */
template <typename BucketType, bool isSegHashTable>
void SonicHashJoin::profileFunc(char* stats, uint32 partIdx)
{
    BucketType loc = 0;
    uint32 fill_cells = 0;
    uint32 total_num = 0;
    uint32 good_num = 0;
    uint32 small_num = 0;
    uint32 conflict_num = 0;
    uint32 chain_len = 0;
    uint32 max_chain_len = 0;

    Assert(m_innerPartitions[partIdx]);
    Assert(m_innerPartitions[partIdx]->m_status == partitionStatusMemory);

    SonicHashMemPartition* memPartition = (SonicHashMemPartition*)m_innerPartitions[partIdx];

    for (uint32 i = 0; i < memPartition->m_hashSize; i++) {
        if (isSegHashTable) {
            loc = (BucketType)memPartition->m_segBucket->getNthDatum(i);
        } else {
            loc = ((BucketType*)memPartition->m_bucket)[i];
        }

        if (loc != 0) {
            /* record each hash chain's length and accumulate hash element */
            chain_len = 0;
            fill_cells++;
            while (loc != 0) {
                chain_len++;
                if (isSegHashTable)
                    loc = (BucketType)memPartition->m_segNext->getNthDatum(loc);
                else
                    loc = ((BucketType*)memPartition->m_next)[loc];
            }

            /* record the number of hash chains with length equal to 2 */
            if (chain_len <= 2 && chain_len > 0) {
                good_num++;
            } else if (chain_len <= 5) {
                /* mark if the length of hash chain is greater than 3, we meet hash conflict */
                small_num++;
            } else {
                conflict_num++;
            }

            /* record the length of the max hash chain  */
            if (chain_len > max_chain_len) {
                max_chain_len = chain_len;
            }

            if (chain_len != 0) {
                total_num++;
            }
        }
    }

    /* organize the information */
    errno_t rc = sprintf_s(stats,
        MAX_LOG_LEN,
        "Hash Table Profiling: table size: %u,"
        " hash elements: %ld, table fill ratio %.2f, max hash chain len: %u,"
        " %u chains have length smaller than 2, %u chains have length smaller than 5,"
        " %u chains have conficts with length > 5.",
        memPartition->m_hashSize,
        memPartition->m_rows,
        (double)fill_cells / memPartition->m_hashSize,
        max_chain_len,
        good_num,
        small_num,
        conflict_num);
    securec_check_ss(rc, "\0", "\0");

    if (max_chain_len >= WARNING_HASH_CONFLICT_LEN || (total_num != 0 && conflict_num >= total_num / 2)) {
        pgstat_add_warning_hash_conflict();
        if (HAS_INSTR(&m_runtime->js, true)) {
            m_runtime->js.ps.instrument->warning |= (1 << WLM_WARN_HASH_CONFLICT);
        }
    }
}

/*
 * @Description: check if the sum of current allocated memory and hash table
 * memory will be allocated can be overflowed.
 * If system is too busy to allocate new memory, spill the data in memory.
 * If the sum memory is larger than the total memory, check if we can
 * spread total memory to the sum size.
 * If spread doesn't succeed, spill the data to the disk.
 */
void SonicHashJoin::judgeMemoryOverflow(uint64 hash_head_size)
{
    uint64 avail_mem = 0;
    uint64 allocate_mem = 0;
    uint64 estimate_mem;
    uint64 hash_mem;

    /*
     * This is only for Build phase before spill.
     * If other phases will use this function, change this assert.
     */
    Assert(m_innerPartitions[0]);

    calcHashContextSize(m_memControl.hashContext, &allocate_mem, &avail_mem);

    /* Do not add hash_mem here before judging whether it can be allocated. */
    m_memControl.allocatedMem = allocate_mem;
    m_memControl.availMem = avail_mem;

    /* Estimated hash table size. */
    hash_mem = hash_head_size;

    estimate_mem = allocate_mem + hash_mem;

    m_memControl.sysBusy = gs_sysmemory_busy(estimate_mem * SET_DOP(m_runtime->js.ps.plan->dop), true);

    if (m_memControl.sysBusy) {
        /* If system is busy, spill data to disk. */
        m_memControl.spillToDisk = true;

        /* memory control */
        AllocSetContext* set = (AllocSetContext*)(m_innerPartitions[0]->m_context);
        set->maxSpaceSize = m_memControl.allocatedMem;

        /* The total memory can be used is reduced to allocatedMem. */
        m_memControl.totalMem = m_memControl.allocatedMem;

        return;
    }

    if (estimate_mem > m_memControl.totalMem) {
        if (m_memControl.maxMem > m_memControl.totalMem) {
            int64 spreadMem =
                Min(Min((uint64)dywlm_client_get_memory() * 1024L, estimate_mem), m_memControl.maxMem - estimate_mem);
            if (spreadMem > estimate_mem * MEM_AUTO_SPREAD_MIN_RATIO) {
                /* Update totalMem if auto spread can be done. */
                m_memControl.totalMem = estimate_mem;
                m_memControl.totalMem += spreadMem;

                /* Update used memory with memory will be allocated. */
                m_memControl.allocatedMem += hash_mem;

                /* memory control */
                AllocSetContext* set = (AllocSetContext*)(m_innerPartitions[0]->m_context);
                set->maxSpaceSize += spreadMem;

                m_memControl.spreadNum++;
                MEMCTL_LOG(DEBUG2,
                    "SonicHashJoin(%d) auto mem spread %ldKB succeed, and work mem is %luKB.",
                    m_runtime->js.ps.plan->plan_node_id,
                    spreadMem / 1024L,
                    m_memControl.totalMem / 1024L);
                return;
            } else {
                /* failed to spread memory */
                MEMCTL_LOG(LOG,
                    "SonicHashJoin(%d) auto mem spread %ldKB failed, and work mem is %luKB.",
                    m_runtime->js.ps.plan->plan_node_id,
                    spreadMem / 1024L,
                    m_memControl.totalMem / 1024L);
            }

            /*
             * If has enable memory spread, but still has to spill,
             * log a warning.
             */
            if (m_memControl.spreadNum > 0) {
                pgstat_add_warning_spill_on_memory_spread();
            }
        }
        m_memControl.spillToDisk = true;
    } else {
        /*
         * If estimate_mem is smaller than the limit,
         * create hash table and do in-memory join later on.
         */
        m_memControl.allocatedMem += hash_mem;
    }
}

/*
 * @Description: check if there is enough memory to fit in the coming batch.
 * @return: true if has enought memory, false if need to spill.
 */
bool SonicHashJoin::hasEnoughMem()
{
    if (m_memControl.spillToDisk) {
        if (m_memControl.sysBusy) {
            m_memControl.totalMem = m_memControl.allocatedMem;
            /*
             * the "usedmem" logged here is the memory size that has been allocated,
             * that is, the memory size of the coming batch is excluded.
             * This is different from VecHashJoin,
             * which logs the allocated memory plus estimated memory size of the coming btach.
             */
            MEMCTL_LOG(LOG,
                "SonicHashJoin(%d) early spilled, workmem: %luKB, usedmem: %luKB,"
                "current HashContext, totalSpace: %luKB, usedSpace: %luKB",
                m_runtime->js.ps.plan->plan_node_id,
                m_memControl.totalMem / 1024L,
                m_memControl.allocatedMem / 1024L,
                m_memControl.allocatedMem / 1024L,
                (m_memControl.allocatedMem - m_memControl.availMem) / 1024L);
            pgstat_add_warning_early_spill();
        } else {
            ereport(LOG,
                (errmodule(MOD_VEC_EXECUTOR),
                    errmsg(
                        "Profiling Warning : SonicHashJoin(%d) Disk Spilled.", m_runtime->js.ps.plan->plan_node_id)));
            MEMCTL_LOG(LOG,
                "SonicHashJoin(%d) Disk Spilled: totalSpace: %luKB, freeSpace: %luKB",
                m_runtime->js.ps.plan->plan_node_id,
                m_memControl.allocatedMem / 1024L,
                m_memControl.availMem / 1024L);
        }
        return false;
    }
    return true;
}

/*
 * @Description: Calculate memory context size
 * @in ctx: memory context to calculate
 * @in allocateSize: pointer to put total allocated size including child context
 * @in freeSize: pointer to put total free size including child context
 */
void SonicHashJoin::calcHashContextSize(MemoryContext ctx, uint64* allocateSize, uint64* freeSize)
{
    AllocSetContext* aset = (AllocSetContext*)ctx;
    MemoryContext child;

    if (NULL == ctx) {
        return;
    }

    /* calculate MemoryContext Stats */
    *allocateSize += (aset->totalSpace);
    *freeSize += (aset->freeSpace);

    /* recursive MemoryContext's child */
    for (child = ctx->firstchild; child != NULL; child = child->nextchild) {
        calcHashContextSize(child, allocateSize, freeSize);
    }
}

/*
 * @Description: Calculate partition number with planner parameters.
 */
uint32 SonicHashJoin::calcPartitionNum()
{
    VecHashJoin* node = NULL;
    Plan* inner_plan = NULL;
    int64 nrows;
    int width;
    uint32 part_num;

    node = (VecHashJoin*)m_runtime->js.ps.plan;
    inner_plan = innerPlan(node);

    nrows = (int64)inner_plan->plan_rows;
    width = inner_plan->plan_width;
    elog(DEBUG2, "SonicHashJoin: total size: %ldByte, operator size: %ldByte.", (nrows * width), m_memControl.totalMem);

    part_num = getPower2NextNum((nrows * width) / m_memControl.totalMem);
    part_num = Max(32, part_num);
    part_num = Min(part_num, SONIC_PART_MAX_NUM);

    return part_num;
}

/*
 * @Description: Sort partitions by size.
 * @in memorySize - memory limit.
 * @return - the start index, thus the smallest partition index.
 */
void SonicHashJoin::sortPartitionSize(uint64 memorySize)
{
    for (uint32 cur_idx = 0; cur_idx < m_partNum; ++cur_idx) {
        m_partSizeOrderedIdx[cur_idx] = cur_idx;
    }
    quickSort(m_partSizeOrderedIdx, 0, m_partNum);

    SonicHashFilePartition* file_partition = NULL;
    int rows = 0;
    int tmp_rows = 0;
    int left_rows = 0;
    size_t size = 0;
    size_t tmp_size = 0;
    int32 part_idx = -1;
    m_partLoadedOffset = -1;
    uint64 hash_head_size = 0;

    size = m_arrayExpandSize;
    /* set rows to 1 because the head in the 1st atom is occupied by default */
    rows = 1;
    for (uint32 idx = 0; idx < m_partNum; ++idx) {
        part_idx = m_partSizeOrderedIdx[idx];
        Assert(part_idx >= 0);

        file_partition = (SonicHashFilePartition*)m_innerPartitions[part_idx];
        tmp_rows = file_partition->m_rows;
        tmp_size = file_partition->m_varSize;
        left_rows = INIT_DATUM_ARRAY_SIZE - (rows % INIT_DATUM_ARRAY_SIZE);
        if (tmp_rows > left_rows) {
            tmp_size += m_arrayExpandSize * ((tmp_rows - left_rows - 1) / INIT_DATUM_ARRAY_SIZE + 1);
        }

        /*
         * Add estimated hash table size.
         */
        hash_head_size = get_hash_head_size(tmp_rows);
        if ((size + tmp_size + hash_head_size) < memorySize) {
            m_memPartFlag[part_idx] = true;
            m_partLoadedOffset = idx;
            size += tmp_size + hash_head_size;
            rows += tmp_rows;
        } else {
            break;
        }
    }
}

/*
 * @Description: quicksort algorithm.
 * @in idx - Partition indexes array. The space should be allocated by caller.
 * @in start - The first position of idx to be sorted.
 * @in end - The last position of idx to be sorted + 1 .
 * Sort interval: [start, end)
 */
inline void SonicHashJoin::quickSort(uint32* idx, const int start, const int end)
{
    uint32 tmp = 0;
    int i, j, m;
    size_t mid_size;

    if (end - start > 1) {
        i = start;
        j = end - 1;
        m = idx[(start + end) / 2];
        Assert(m_innerPartitions[m]);
        mid_size = m_innerPartitions[m]->m_size;

        while (i <= j) {
            Assert(m_innerPartitions[idx[i]] && m_innerPartitions[idx[j]]);
            while (m_innerPartitions[idx[i]]->m_size < mid_size)
                i++;
            while (mid_size < m_innerPartitions[idx[j]]->m_size)
                j--;
            if (i <= j) {
                tmp = idx[j];
                idx[j] = idx[i];
                idx[i] = tmp;
                j--;
                i++;
            }
        }
        quickSort(idx, start, j + 1);
        quickSort(idx, i, end);
    }
}

/*
 * @Description: Initial match functions by desc.
 * @in desc - tuple description.
 * @in keyNum - number of hash keys.
 */
void SonicHashJoin::initMatchFunc(TupleDesc desc, uint16 keyNum)
{
    m_matchKey = (matchFun*)palloc0(sizeof(matchFun) * keyNum);

    for (int i = 0; i < keyNum; i++) {
        /* If the i-th hash key is simple type, specialize its match function. */
        if (m_integertype[i])
            DispatchKeyInnerFunction(i);
        else {
            if (m_runtime->js.nulleqqual != NIL) {
                /* Note: check int64 again! */
                m_matchKey[i] = &SonicHash::matchCheckColT<int64, int64, false, true>;
            } else {
                m_matchKey[i] = &SonicHash::matchCheckColT<int64, int64, false, false>;
            }
        }
    }
}

/*
 * @Description: Specify match function by build side hash key type.
 * @in KeyIdx - hash key index in m_buildOp.keyIndx.
 */
void SonicHashJoin::DispatchKeyInnerFunction(int KeyIdx)
{
    int attrid = m_buildOp.keyIndx[KeyIdx];
    switch ((m_buildOp.tupleDesc)->attrs[attrid]->atttypid) {
        case INT1OID:
            DispatchKeyOuterFunction<uint8>(KeyIdx);
            break;
        case INT2OID:
            DispatchKeyOuterFunction<int16>(KeyIdx);
            break;
        case INT4OID:
            DispatchKeyOuterFunction<int32>(KeyIdx);
            break;
        case INT8OID:
            DispatchKeyOuterFunction<int64>(KeyIdx);
            break;
        default:
            Assert(false);
            ereport(ERROR,
                (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmodule(MOD_VEC_EXECUTOR),
                    errmsg("Unrecognize data type %u when choosing match functions from inner hash keys.",
                        (m_buildOp.tupleDesc)->attrs[attrid]->atttypid)));
            break;
    }
}

/*
 * @Description: Specify match function by probe side hash key type.
 * @in KeyIdx - hash key index in m_probeOp.keyIndx.
 */
template <typename innerType>
void SonicHashJoin::DispatchKeyOuterFunction(int KeyIdx)
{
    int attr_id = m_probeOp.keyIndx[KeyIdx];
    switch ((m_probeOp.tupleDesc)->attrs[attr_id]->atttypid) {
        case INT1OID:
            if (m_runtime->js.nulleqqual != NIL)
                m_matchKey[KeyIdx] = &SonicHash::matchCheckColT<innerType, uint8, true, true>;
            else
                m_matchKey[KeyIdx] = &SonicHash::matchCheckColT<innerType, uint8, true, false>;
            break;
        case INT2OID:
            if (m_runtime->js.nulleqqual != NIL)
                m_matchKey[KeyIdx] = &SonicHash::matchCheckColT<innerType, int16, true, true>;
            else
                m_matchKey[KeyIdx] = &SonicHash::matchCheckColT<innerType, int16, true, false>;
            break;
        case INT4OID:
            if (m_runtime->js.nulleqqual != NIL)
                m_matchKey[KeyIdx] = &SonicHash::matchCheckColT<innerType, int32, true, true>;
            else
                m_matchKey[KeyIdx] = &SonicHash::matchCheckColT<innerType, int32, true, false>;
            break;
        case INT8OID:
            if (m_runtime->js.nulleqqual != NIL)
                m_matchKey[KeyIdx] = &SonicHash::matchCheckColT<innerType, int64, true, true>;
            else
                m_matchKey[KeyIdx] = &SonicHash::matchCheckColT<innerType, int64, true, false>;
            break;
        default:
            Assert(false);
            ereport(ERROR,
                (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmodule(MOD_VEC_EXECUTOR),
                    (errmsg("Unrecognize data type %u when choosing match functions from outer hash keys.",
                        (m_probeOp.tupleDesc)->attrs[attr_id]->atttypid))));
            break;
    }
}

/*
 * @Description: Calculate hash value for complicate join key.
 * @in batch - batch from build or probe side.
 * @in hashKeys - hash keys need to be calculated.
 * @in inner - true when batch is from build side.
 */
void SonicHashJoin::CalcComplicateHashVal(VectorBatch* batch, List* hashKeys, bool inner)
{
    ExprContext* econtext = m_runtime->js.ps.ps_ExprContext;
    ;
    ListCell* hk = NULL;
    ScalarVector* results = NULL;
    int nrows = batch->m_rows;
    bool first_enter = true;
    bool* pSelection = NULL;
    FmgrInfo* hash_functions = NULL;
    Datum key;
    ScalarValue hash_v;

    if (nrows == 0)
        return;

    m_cjVector->m_rows = 0;
    if (inner) {
        initEcontextBatch(NULL, NULL, batch, NULL);
        hash_functions = m_buildOp.hashFmgr;
    } else {
        initEcontextBatch(NULL, batch, NULL, NULL);
        hash_functions = m_probeOp.hashFmgr;
    }
    pSelection = batch->m_sel;
    ResetExprContext(econtext);
    AutoContextSwitch memSwitch(econtext->ecxt_per_tuple_memory);

    econtext->align_rows = nrows;
    int j = 0;
    foreach (hk, hashKeys) {
        ExprState* clause = (ExprState*)lfirst(hk);

        results = VectorExprEngine(clause, econtext, pSelection, m_cjVector, NULL);

        if (first_enter) {
            for (int i = 0; i < nrows; i++) {
                key = results->m_vals[i];

                if (NOT_NULL(results->m_flag[i]))
                    m_hashVal[i] = FunctionCall1(&hash_functions[j], key);
                else
                    m_hashVal[i] = 0;
            }
            first_enter = false;
        } else {
            for (int i = 0; i < nrows; i++) {
                if (NOT_NULL(results->m_flag[i])) {
                    key = results->m_vals[i];
                    /* rotate hashkey left 1 bit at each rehash step */
                    hash_v = m_hashVal[i];
                    hash_v = (hash_v << 1) | ((hash_v & 0x80000000) ? 1 : 0);
                    hash_v ^= FunctionCall1(&hash_functions[j], key);
                    m_hashVal[i] = hash_v;
                }
            }
        }
        j++;
    }

    /*
     * Rehash the hash value for avoiding the key and
     * distribute key using the same hash function.
     */
    for (int i = 0; i < nrows; i++)
        m_hashVal[i] = hash_uint32(DatumGetUInt32(m_hashVal[i]));
}

/*
 * @Description: match function for complicate join key.
 * @in batch - probe side data.
 * @in inPartition - If matchPartComplicateKey is false,
 * 	inPartition is build side data. Otherwise
 * 	inPartition are not used. build side data is
 * 	from m_innerPartitions[].
 */
template <bool matchPartComplicateKey>
void SonicHashJoin::matchComplicateKey(VectorBatch* batch, SonicHashMemPartition* inPartition)
{
    int i, j;
    int nrows = batch->m_rows;
    ScalarVector* scalar_vector = NULL;
    ExprContext* econtext = m_runtime->js.ps.ps_ExprContext;
    SonicDatumArray* array = NULL;
    int resultRow = 0;
    uint32* loc1 = NULL;
    uint32* loc2 = NULL;
    SonicHashMemPartition* mem_partition = NULL;

    ResetExprContext(econtext);

    m_complicate_innerBatch->Reset();
    m_complicate_outerBatch->Reset();

    for (i = 0; i < m_complicate_innerBatch->m_cols; i++) {
        if (!matchPartComplicateKey) {
            Assert(inPartition);

            /* Get data from the specify partition. */
            array = inPartition->m_data[i];
            array->getArrayAtomIdx(m_selectRows, m_loc, m_arrayIdx);
            array->getDatumFlagArray(m_selectRows, m_arrayIdx, m_matchKeys, m_nullFlag);
            scalar_vector = &m_complicate_innerBatch->m_arr[i];
            for (j = 0; j < m_selectRows; j++) {
                scalar_vector->m_vals[j] = m_matchKeys[j];
                scalar_vector->m_flag[j] = m_nullFlag[j];
            }
        } else {
            scalar_vector = &m_complicate_innerBatch->m_arr[i];
            loc1 = m_partLoc;
            loc2 = m_loc;
            for (j = 0; j < m_selectRows; j++, loc1++, loc2++) {
                mem_partition = (SonicHashMemPartition*)m_innerPartitions[*loc1];

                /* After ProbeXXX functions, m_innerPartitions[*loc1] should stay in memory */
                Assert(mem_partition);

                array = mem_partition->m_data[i];
                array->getNthDatumFlag(*loc2, (ScalarValue*)&m_matchKeys[j], &m_nullFlag[j]);
                scalar_vector->m_vals[j] = m_matchKeys[j];
                scalar_vector->m_flag[j] = m_nullFlag[j];
            }
        }
    }

    for (i = 0; i < nrows; i++)
        m_match[i] = false;
    for (i = 0; i < m_selectRows; i++) {
        for (j = 0; j < m_complicate_outerBatch->m_cols; j++) {
            scalar_vector = &m_complicate_outerBatch->m_arr[j];
            scalar_vector->m_vals[resultRow] = batch->m_arr[j].m_vals[m_selectIndx[i]];
            scalar_vector->m_flag[resultRow] = batch->m_arr[j].m_flag[m_selectIndx[i]];
        }
        resultRow++;
        m_match[i] = true;
    }

    m_complicate_innerBatch->FixRowCount(m_selectRows);
    m_complicate_outerBatch->FixRowCount(m_selectRows);

    /*
     * If m_complicate_innerBatch and m_complicate_outerBatch are both NULL,
     * no need to calculate the expression clause, return directly here.
     */
    if (unlikely(m_selectRows == 0))
        return;

    initEcontextBatch(m_complicate_outerBatch, m_complicate_outerBatch, m_complicate_innerBatch, NULL);

    ExecVecQual(m_runtime->hashclauses, econtext, false);

    /* for noneq_join */
    bool need_furtherCheck = false;
    if (m_runtime->js.nulleqqual != NULL) {
        /* no need to calc */
        for (i = 0; i < m_selectRows; i++) {
            if (m_complicate_outerBatch->m_sel[i])
                m_nulleqmatch[i] = false;
            else {
                m_nulleqmatch[i] = true;
                need_furtherCheck = true;
            }
        }

        if (need_furtherCheck) {
            bool* tmpSel = &m_complicate_outerBatch->m_sel[0];
            m_complicate_outerBatch->m_sel = &m_nulleqmatch[0];
            (void)ExecVecQual(m_runtime->js.nulleqqual, econtext, false);

            /* restore the selection */
            m_complicate_outerBatch->m_sel = tmpSel;
            for (i = 0; i < m_selectRows; i++) {
                m_complicate_outerBatch->m_sel[i] = m_complicate_outerBatch->m_sel[i] || m_nulleqmatch[i];
            }
        }
    }

    j = 0;
    for (i = 0; i < m_selectRows; i++) {
        if (m_match[i]) {
            if (!m_complicate_outerBatch->m_sel[j])
                m_match[i] = false;
            j++;
        }
    }
}

/*
 * @Description: bloom filter function.
 */
void SonicHashJoin::pushDownFilterIfNeed()
{
    filter::BloomFilter** bf_array = m_runtime->bf_runtime.bf_array;
    List* bf_var_list = m_runtime->bf_runtime.bf_var_list;
    int arr_num;
    int arr_size;
    uint8 null_flag;
    ScalarValue val;
    SonicHashMemPartition* mem_partition = NULL;

    if (u_sess->attr.attr_sql.enable_bloom_filter && MEMORY_HASH == m_strategy && !m_complicatekey &&
        m_rows <= DEFAULT_ORC_BLOOM_FILTER_ENTRIES * 5) {
        Assert(m_probeIdx == 0);
        Assert(m_innerPartitions[m_probeIdx] && m_innerPartitions[m_probeIdx]->m_status == partitionStatusMemory);
        mem_partition = (SonicHashMemPartition*)m_innerPartitions[m_probeIdx];

        for (int i = 0; i < list_length(bf_var_list); i++) {
            Var* var = (Var*)list_nth(bf_var_list, i);
            int idx = -1;
            uint32 rowidx = 0;
            for (int j = 0; j < m_buildOp.keyNum; ++j) {
                if (var->varoattno - 1 == m_buildOp.oKeyIndx[j] && var->varattno - 1 == m_buildOp.keyIndx[j]) {
                    idx = m_buildOp.keyIndx[j];
                    break;
                }
            }
            if (idx < 0) {
                continue;
            }

            Oid dataType = var->vartype;
            if (!SATISFY_BLOOM_FILTER(dataType)) {
                continue;
            }

            filter::BloomFilter* filter = filter::createBloomFilter(dataType,
                var->vartypmod,
                var->varcollid,
                HASHJOIN_BLOOM_FILTER,
                DEFAULT_ORC_BLOOM_FILTER_ENTRIES * 5,
                true);

            /*
             * For compute pool, we have to forbidden codegen of bf, since we
             * can not push down codegen expr now.
             */
            if (m_runtime->js.ps.state && m_runtime->js.ps.state->es_plannedstmt &&
                !m_runtime->js.ps.state->es_plannedstmt->has_obsrel) {
                switch (dataType) {
                    case INT2OID:
                    case INT4OID:
                    case INT8OID:
                        filter->jitted_bf_addLong = m_runtime->jitted_hashjoin_bfaddLong;
                        filter->jitted_bf_incLong = m_runtime->jitted_hashjoin_bfincLong;
                        break;
                    default:
                        /* do nothing */
                        break;
                }
            }
            arr_num = mem_partition->m_data[idx]->m_arrIdx + 1;
            for (int j = 0; j < arr_num; j++) {
                arr_size =
                    (j < arr_num - 1) ? mem_partition->m_data[idx]->m_atomSize : mem_partition->m_data[idx]->m_atomIdx;
                for (int k = 0; k < arr_size; k++) {
                    mem_partition->m_data[idx]->getNthDatumFlag(rowidx++, &val, &null_flag);
                    if (NOT_NULL(null_flag)) {
                        filter->addDatum((Datum)val);
                    }
                }
            }
            int pos = list_nth_int(m_runtime->bf_runtime.bf_filter_index, i);
            bf_array[pos] = filter;
        }
    }
}

/*
 * @Description: release all the file buffer of the inner or outer partitions
 * @in isInner - true to release inner partitions, false to release outer partitions.
 */
void SonicHashJoin::releaseAllFileHandlerBuffer(bool isInner)
{
    SonicHashPartition* partition = NULL;
    SonicHashPartition** partitions = isInner ? m_innerPartitions : m_outerPartitions;
    for (uint32 i = 0; i < m_partNum; ++i) {
        partition = partitions[i];
        if (partition != NULL && partition->m_status == partitionStatusFile) {
            ((SonicHashFilePartition*)partition)->releaseFileHandlerBuffer();
        }
    }
}

/*
 * @Description: Calculate partition index and record them in m_partIdx.
 * @in hashVal - hash value to be used. The space should be allocated by caller.
 * @in partNum - partition number.
 * @in nrows - hash value numbers.
 */
inline void SonicHashJoin::calcPartIdx(uint32* hashVal, uint32 partNum, int nrows)
{
    for (int i = 0; i < nrows; i++) {
        m_partIdx[i] = *hashVal % partNum;
        hashVal++;
    }
}

/*
 * @Description: Calculate partition index and record them in m_partIdx when do repartition.
 * @in hashVal - hash value to be used. The space should be allocated by caller.
 * @in partNum - partition number.
 * @in rbit - used to rotate hash value.
 * @in nrows - hash value numbers.
 */
inline void SonicHashJoin::calcRePartIdx(uint32* hashVal, uint32 partNum, uint32 rbit, int nrows)
{
    for (int i = 0; i < nrows; ++i) {
        m_partIdx[i] = m_partNum + (leftrot(hashVal[i], rbit) & (partNum - 1));
    }
}

/*
 * @Description: Initial probe partitions.
 */
void SonicHashJoin::initProbePartitions()
{
    AutoContextSwitch memSwitch(m_memControl.hashContext);
    SonicHashPartition** probeP = (SonicHashPartition**)palloc0(sizeof(SonicHashPartition*) * m_partNum);
    for (uint32 partIdx = 0; partIdx < m_partNum; partIdx++) {
        /* only need to init those outer file partitions that the coresponding inner partitions are in the disk */
        if (m_innerPartitions[partIdx] && m_innerPartitions[partIdx]->m_status == partitionStatusFile) {
            probeP[partIdx] = New(CurrentMemoryContext) SonicHashFilePartition(
                (char*)"outerPartitionContext", m_complicatekey, m_probeOp.tupleDesc, m_memControl.totalMem);
            initPartition<false>(probeP[partIdx]);
        }
    }
    m_outerPartitions = probeP;
}

/*
 * @Description: Calculate datum array expand size.
 * 	those size are not include the size of SONIC_VAR_TYPE data
 * 	or the size of SONIC_NUMERIC_COMPRESS_TYPE data.
 */
void SonicHashJoin::calcDatumArrayExpandSize()
{
    /* element size per row */
    size_t array_element_size = 0;
    SonicHashMemPartition* memPartition = (SonicHashMemPartition*)m_innerPartitions[0];
    for (uint16 colIdx = 0; colIdx < m_buildOp.cols; ++colIdx) {
        if (memPartition->m_data[colIdx]->m_desc.dataType == SONIC_CHAR_DIC_TYPE) {
            /* for SonicCharDatumArray, better to include the typeSize */
            array_element_size += memPartition->m_data[colIdx]->m_desc.typeSize;
        } else if (memPartition->m_data[colIdx]->m_desc.dataType == SONIC_NUMERIC_COMPRESS_TYPE) {
            /*
             * for SonicNumericDatumArray,
             * only inlcude the space for SonicNumericDatumArray->m_curOffset here,
             * the space to store the numeric will be recorded
             * into SonicHashPartition->m_varSize during spilling stage
             */
            array_element_size += sizeof(uint32);
        } else {
            array_element_size += memPartition->m_data[colIdx]->m_atomTypeSize;
        }
    }

    /*
     * m_arrayExpandSize includes all columns and their flags,
     * for the complicate key case, we should also record the space used for hash value,
     * which is an IntDatumArray without flag.
     */
    if (m_complicatekey) {
        array_element_size += sizeof(uint32);
    }
    m_arrayExpandSize = array_element_size * INIT_DATUM_ARRAY_SIZE + INIT_DATUM_ARRAY_SIZE * m_buildOp.cols;
}

/*
 * @Description: Calculate hash size by row numbers.
 * @in nrows - row number.
 */
inline uint64 SonicHashJoin::calcHashSize(int64 nrows)
{
#ifdef USE_PRIME
    return hashfindprime(nrows);
#else
    return Max(MIN_HASH_TABLE_SIZE, getPower2LessNum(Min(nrows, (int)(MAX_BUCKET_NUM))));
#endif
}

/*
 * @Description: Reset resources.
 */
void SonicHashJoin::ResetNecessary()
{
    if (!m_runtime->js.ps.plan->ispwj && m_strategy == MEMORY_HASH && m_runtime->js.ps.righttree->chgParam == NULL &&
        !((VecHashJoin*)m_runtime->js.ps.plan)->rebuildHashTable) {
        /* Okay to reuse the hash table; needn't rescan inner, either. */
        m_runtime->joinState = HASH_PROBE;
        m_probeStatus = PROBE_FETCH;
        return;
    }

    if (m_strategy == GRACE_HASH)
        closeAllFiles();

    /* Reset hashContext */
    MemoryContextResetAndDeleteChildren(m_memControl.hashContext);

    {
        /* Recreate m_innerPartitions */
        AutoContextSwitch memSwitch(m_memControl.hashContext);
        m_innerPartitions = (SonicHashPartition**)palloc0(sizeof(SonicHashPartition*));
        m_innerPartitions[0] = New(CurrentMemoryContext) SonicHashMemPartition(
            (char*)"innerPartitionContext", m_complicatekey, m_buildOp.tupleDesc, m_memControl.totalMem);
        initPartition<true>(m_innerPartitions[0]);
    }
    m_outerPartitions = NULL;

    m_runtime->joinState = HASH_BUILD;
    m_strategy = MEMORY_HASH;
    resetMemoryControl();
    m_rows = 0;
    m_probeIdx = 0;
    m_partNum = 1;
    errno_t rc = memset_s(m_memPartFlag, sizeof(m_memPartFlag), 0, SONIC_PART_MAX_NUM * sizeof(m_memPartFlag[0]));
    securec_check(rc, "\0", "\0");
    m_diskPartNum = 0;

    /*
     * If chgParam of subnode is not null then plan will be re-scanned
     * by first VectorEngine.
     */
    if (m_runtime->js.ps.righttree->chgParam == NULL) {
        VecExecReScan(m_runtime->js.ps.righttree);
    }
}

/*
 * @Description: reset memory control information.
 */
void SonicHashJoin::resetMemoryControl()
{
    /* do not reset the total memory, since it might be successfully spread in the last partition scan */
    m_memControl.sysBusy = false;
    m_memControl.spillToDisk = false;
    m_memControl.spillNum = 0;
    m_memControl.spreadNum = 0;
    m_memControl.availMem = 0;
    m_memControl.allocatedMem = 0;

    if (HAS_INSTR(&m_runtime->js, true)) {
        errno_t rc = memset_s(&(INSTR->sorthashinfo), sizeof(INSTR->sorthashinfo), 0, sizeof(struct SortHashInfo));
        securec_check(rc, "\0", "\0");
    }
}

/*
 * @Description: record partition information into log file
 * @in buildside - build side or not (probe side)
 * @in partIdx - index of the partition if any to be repartitioned
 * @in istart - start index of the partitions
 * @in iend - end index of the partitions
 * @return - void
 */
void SonicHashJoin::recordPartitionInfo(bool buildside, int32 partIdx, uint32 istart, uint32 iend)
{

    const char* side = buildside ? "Build" : "Probe";
    SonicHashPartition** source = (buildside ? m_innerPartitions : m_outerPartitions);
    SonicHashFilePartition* partition = NULL;

    Assert(istart >= 0 && istart <= iend);
    if (partIdx >= 0) {
        Assert(source[partIdx]);
        Assert(source[partIdx]->m_status == partitionStatusFile);
        partition = (SonicHashFilePartition*)(source[partIdx]);

        elog(DEBUG2,
            "[SonicVecHashJoin_RePartition] [%s Side]: "
            "Partition: %d, Spill Time: %d, File Num: %d, Total File Size: %lu, Total Memory: %lu, "
            "RePartition Num %u.",
            side,
            partIdx,
            m_pLevel[partIdx],
            partition->m_fileNum,
            partition->m_size,
            m_memControl.totalMem,
            iend - istart);
    }

    for (uint32 i = istart; i < iend; i++) {
        if (!buildside && (partIdx == -1) && source[i] == NULL) {
            /*
             * This case happens when recording probe partitions for the first time,
             * in this case, some outer partitions are not initialized,
             * cause coresponding inner partitions are already loaded in the memory.
             */
            continue;
        }

        Assert(source[i]);
        Assert(source[i]->m_status == partitionStatusFile);

        partition = (SonicHashFilePartition*)(source[i]);
        elog(DEBUG2,
            "[SonicVecHashJoin] [%s Side]: Partition: %u, Spill Time: %d, File Num: %d, Total File Size: %lu, Total "
            "Memory: %lu",
            side,
            i,
            m_pLevel[i],
            partition->m_fileNum,
            partition->m_size,
            m_memControl.totalMem);
    }
}

/*
 * @Description: analyze partition info
 * @in buildside - true for build side, false for probe side (including reparition)
 * @in startPartIdx - the first partition index to analyze
 * @in endPartIdx - the last partition index to analyze
 * @in totalFileNum - pointer to store all the file number
 * @in totalFileSize - pointer to store all the file size
 * @in partSizeMin - pointer to store the min total file size of a partition among all partitions
 * @in partSizeMax - pointer to store the max total file size of a partition among all partitions
 * @in spillPartNum - the actual number of partitions that are spilt to disk
 */
void SonicHashJoin::analyzePartition(bool buildside, uint32 startPartIdx, uint32 endPartIdx, int64* totalFileNum,
    long* totalFileSize, long* partSizeMin, long* partSizeMax, int* spillPartNum)
{
    SonicHashFilePartition* file_partition = NULL;
    uint16 file_num = 0;
    size_t part_size = 0;
    uint32 part_num = 0;
    SonicHashPartition** source_part = buildside ? m_innerPartitions : m_outerPartitions;

    for (uint32 part_idx = startPartIdx; part_idx < endPartIdx; ++part_idx) {
        file_partition = (SonicHashFilePartition*)source_part[part_idx];
        if (file_partition == NULL) {
            continue;
        }
        part_num++;

        if (u_sess->attr.attr_sql.enable_sonic_optspill) {
            uint16 actual_filenum = 0;
            for (int i = 0; i < file_partition->m_fileNum; i++) {
                if (file_partition->m_files[i] != NULL)
                    actual_filenum += 1;
            }
            file_num = actual_filenum;
        } else {
            file_num = file_partition->m_fileNum;
        }

        part_size = file_partition->m_size;

        (*totalFileNum) += (int64)file_num;
        (*totalFileSize) += part_size;
        (*partSizeMin) = Min((size_t)(*partSizeMin), part_size);
        (*partSizeMax) = Max((size_t)(*partSizeMax), part_size);
    }

    (*spillPartNum) += part_num;
}

/*
 * @Description: report instrument info
 * @in reportType - reportTypeBuild for build side,
 * 	reportTypeProbe for probe side,
 * 	reportTypeRepartition for repartition.
 * @in totalPartNum - the number of total partitions at the report time,
 * 	for reportTypeBuild and reportTypeProbe, it should be the m_partNum,
 * 	for repoartTypeRepartition, it should be m_partNum plus the number of repartitions.
 * @in rePartIdx - partition index that is repartitioned, only set when reportTypeRepartition.
 */
void SonicHashJoin::reportSorthashinfo(ReportType reportType, uint32 totalPartNum, int32 rePartIdx)
{
    if (!(HAS_INSTR(&m_runtime->js, true)))
        return;

    switch (reportType) {
        case reportTypeBuild:
            /* report the inner partitions info from index 0 to m_partNum */
            if (m_strategy == MEMORY_HASH) {
                INSTR->sorthashinfo.hash_writefile = false;
            } else {
                Assert(m_strategy == GRACE_HASH);
                Assert(m_partNum > 0);
                INSTR->sorthashinfo.spill_innerSizePartMin = INT64_MAX;
                analyzePartition(true,
                    0,
                    totalPartNum,
                    &(INSTR->sorthashinfo.hash_innerFileNum),
                    &(INSTR->sorthashinfo.spill_innerSize),
                    &(INSTR->sorthashinfo.spill_innerSizePartMin),
                    &(INSTR->sorthashinfo.spill_innerSizePartMax),
                    &(INSTR->sorthashinfo.spill_innerPartNum));
                INSTR->sorthashinfo.hash_writefile = true;
                INSTR->sorthashinfo.hash_partNum = totalPartNum;
                INSTR->sorthashinfo.hash_FileNum = INSTR->sorthashinfo.hash_innerFileNum;
                INSTR->sorthashinfo.spill_size = INSTR->sorthashinfo.spill_innerSize;
            }
            break;

        case reportTypeProbe:
            /* report the info of those outer partitions that are not NULL */
            INSTR->sorthashinfo.spill_outerSizePartMin = INT64_MAX;
            analyzePartition(false,
                0,
                totalPartNum,
                &(INSTR->sorthashinfo.hash_outerFileNum),
                &(INSTR->sorthashinfo.spill_outerSize),
                &(INSTR->sorthashinfo.spill_outerSizePartMin),
                &(INSTR->sorthashinfo.spill_outerSizePartMax),
                &(INSTR->sorthashinfo.spill_outerPartNum));
            if (INSTR->sorthashinfo.spill_outerPartNum == 0)
                INSTR->sorthashinfo.spill_outerSizePartMin = 0;
            INSTR->sorthashinfo.hash_FileNum += INSTR->sorthashinfo.hash_outerFileNum;
            INSTR->sorthashinfo.spill_size += INSTR->sorthashinfo.spill_outerSize;
            break;

        case reportTypeRepartition:
            /* report the repartitioned partition info and the new partitions' info */
            Assert(rePartIdx >= 0);
            Assert(rePartIdx < (int32)m_partNum);

            if (m_pLevel[rePartIdx] >= WARNING_SPILL_TIME) {
                INSTR->warning |= (1 << WLM_WARN_SPILL_TIMES_LARGE);
            }

            INSTR->sorthashinfo.hash_spillNum = Max(m_pLevel[rePartIdx], INSTR->sorthashinfo.hash_spillNum);
            analyzePartition(true,
                m_partNum,
                totalPartNum,
                &(INSTR->sorthashinfo.hash_innerFileNum),
                &(INSTR->sorthashinfo.spill_innerSize),
                &(INSTR->sorthashinfo.spill_innerSizePartMin),
                &(INSTR->sorthashinfo.spill_innerSizePartMax),
                &(INSTR->sorthashinfo.spill_innerPartNum));

            analyzePartition(false,
                m_partNum,
                totalPartNum,
                &(INSTR->sorthashinfo.hash_outerFileNum),
                &(INSTR->sorthashinfo.spill_outerSize),
                &(INSTR->sorthashinfo.spill_outerSizePartMin),
                &(INSTR->sorthashinfo.spill_outerSizePartMax),
                &(INSTR->sorthashinfo.spill_outerPartNum));

            INSTR->sorthashinfo.hash_partNum = totalPartNum;
            INSTR->sorthashinfo.hash_FileNum =
                INSTR->sorthashinfo.hash_innerFileNum + INSTR->sorthashinfo.hash_outerFileNum;
            INSTR->sorthashinfo.spill_size = INSTR->sorthashinfo.spill_innerSize + INSTR->sorthashinfo.spill_outerSize;
            break;

        default:
            ereport(ERROR,
                (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmodule(MOD_VEC_EXECUTOR),
                    (errmsg("SonicHashJoin reportSorthashinfo: Unsupport report type %d", reportType))));
            break;
    }
}

/*
 * @Description: Save probe data to file partitions.
 */
template <bool complicateJoinKey, bool optspill>
void SonicHashJoin::saveProbePartition()
{
    SonicHashFilePartition* file_partition = NULL;
    ScalarValue* arr_val = NULL;
    uint8* null_flag = NULL;
    uint32 part_idx;
    int row_idx;

    for (uint32 i = 0; i < m_diskPartNum; i++) {
        part_idx = m_diskPartIdx[i].partIdx;
        row_idx = m_diskPartIdx[i].rowIdx;
        file_partition = (SonicHashFilePartition*)m_outerPartitions[part_idx];
        Assert(file_partition && file_partition->m_status == partitionStatusFile);

        for (int j = 0; j < m_probeOp.cols; j++) {
            arr_val = &m_outRawBatch->m_arr[j].m_vals[row_idx];
            null_flag = &m_outRawBatch->m_arr[j].m_flag[row_idx];
            file_partition->putVal<optspill>(arr_val, null_flag, j);
        }
        /* Put hash value if complicateJoinKey is true. */
        if (complicateJoinKey)
            file_partition->putHash(&m_hashVal[row_idx]);

        file_partition->m_rows += 1;
    }

    m_diskPartNum = 0;
}
