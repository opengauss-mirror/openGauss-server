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
 * vechashtable.cpp
 *
 *
 * IDENTIFICATION
 *        Code/src/gausskernel/runtime/vecexecutor/vecnode/vechashtable.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "access/tableam.h"
#include "vecexecutor/vechashtable.h"
#include "vecexecutor/vecexecutor.h"
#include "executor/executor.h"
#include "nodes/memnodes.h"
#include "executor/node/nodeAgg.h"
#include "pgxc/pgxc.h"
#include "storage/lz4_file.h"
#include "utils/numeric.h"
#include "utils/numeric_gs.h"

/*
 * @Description:  create temp file
 * @return -  file pointer
 */
void* TempFileCreate()
{
    if (u_sess->attr.attr_sql.enable_compress_spill) {
        return (void*)LZ4FileCreate(false);
    } else {
        return (void*)BufFileCreateTemp(false);
    }
}

/*
 * @Description:  write data to temp file
 * @in file -  file pointer
 * @in data - data to be written
 * @in size - data size
 * @return - written size
 */
template <bool compress_spill>
size_t TempFileWrite(void* file, void* data, size_t size)
{
    if (compress_spill) {
        return (size_t)LZ4FileWrite((LZ4File*)file, (char*)data, size);
    } else {
        return BufFileWrite((BufFile*)file, data, size);
    }
}

/*
 * @Description:  read data from temp file
 * @in file -  file descriptor
 * @out data -
 * @in size - the size of data to be readed
 * @return -  read data size
 */
template <bool compress_spill>
size_t TempFileRead(void* file, void* data, size_t size)
{
    if (compress_spill) {
        return (size_t)LZ4FileRead((LZ4File*)file, (char*)data, size);
    } else {
        return BufFileRead((BufFile*)file, data, size);
    }
}

/*
 * @Description: add variable data to buf
 * @in buf: buffer
 * @in value: variable data
 * @return: void
 */
ScalarValue addToVarBuffer(VarBuf* buf, ScalarValue value)
{
    return PointerGetDatum(buf->Append(DatumGetPointer(value), VARSIZE_ANY(value)));
}

/*
 * @Description: add variable data to a memory context
 * @in context: memory context where data is written
 * @in val: variable data to be written
 * @return: void
 */
ScalarValue addVariable(MemoryContext context, ScalarValue val)
{
    AutoContextSwitch mem_guard(context);

    int key_size = VARSIZE_ANY(val);
    char* addr = (char*)palloc(key_size);
    errno_t rc = memcpy_s(addr, key_size, DatumGetPointer(val), key_size);
    securec_check(rc, "\0", "\0");
    return PointerGetDatum(addr);
}

/*
 * @Description: replace variable data oldVal with val
 * @in context: memory context where is the buffer
 * @in oldVal: space to hold new value val
 * @in val: variable data to be copied
 * @return: address of the buffer where is the val
 */
ScalarValue replaceVariable(MemoryContext context, ScalarValue old_val, ScalarValue val)
{
    int key_size = VARSIZE_ANY(val);
    int old_key_size = VARSIZE_ANY(old_val);
    errno_t rc;
    /* if we can reuse the space, just fine */
    if (key_size <= old_key_size) {
        rc = memcpy_s(DatumGetPointer(old_val), key_size, DatumGetPointer(val), key_size);
        securec_check(rc, "\0", "\0");
        return old_val;
    } else {
        AutoContextSwitch mem_guard(context);

        /* free the buffer one by one */
        pfree(DatumGetPointer(old_val));

        char* addr = (char*)palloc(key_size);
        rc = memcpy_s(addr, key_size, DatumGetPointer(val), key_size);
        securec_check(rc, "\0", "\0");
        return PointerGetDatum(addr);
    }
}

/*
 * @Description: transform the datum to a scalar with header and data part
 * @in datumVal: the datum to be transformed
 * @in datumType: datum's type
 * @in context: given memory context to hold the scalar with header and data part
 * @return: the scalar value with header and data part or pointer
 */
ScalarValue DatumToScalarInContext(MemoryContext context, Datum datum_val, Oid datum_type)
{
    DBG_ASSERT(datum_type != InvalidOid);

    /* switch to the given memory context first, because new buffers are needed here */
    AutoContextSwitch mem_guard(context);

    if (COL_IS_ENCODE(datum_type)) {
        switch (datum_type) {
            /* for datum type without header but fixed length */
            case MACADDROID:
                return ScalarVector::DatumFixLenToScalar(datum_val, 6);
            case TIMETZOID:
            case TINTERVALOID:
                return ScalarVector::DatumFixLenToScalar(datum_val, 12);
            case INTERVALOID:
                return ScalarVector::DatumFixLenToScalar(datum_val, 16);
            case NAMEOID:
                return ScalarVector::DatumFixLenToScalar(datum_val, 64);

            /* for datum type without header but variable length */
            case UNKNOWNOID:
            case CSTRINGOID: {
                Size datum_len = strlen((char*)datum_val);
                return ScalarVector::DatumCstringToScalar(datum_val, datum_len);
            }

            /* try to turn numeric data first */
            case NUMERICOID: {
                /*
                 * Turn numeric data to big integer.
                 * numeric -> int64|int128|original numeric
                 * if val == datum_val, then no actions in convert function,
                 * else we already make the transformation, just return the val
                 */
                ScalarValue val = try_convert_numeric_normal_to_fast(datum_val);
                if (val == datum_val)
                    return addVariable(context, val);
                else
                    return val;
            }

            /* for other variable datum with header, we just add the variable part */
            default:
                return addVariable(context, datum_val);
        }
    } else {
        /* if not the encoded mode, just untouch */
        return datum_val;
    }
}

/*
 * @Description	: Analyze current hash table to show the statistics information of hash chains,
 *				  including hash table size, invalid number of hash chains, distribution of the
 *				  length of hash chains.
 * @in stats		: The string used to record all the hash table information.
 */
void vechashtable::Profile(char* stats, bool* can_wlm_warning_statistics)
{
    int fill_rows = 0;
    int single_num = 0;
    int double_num = 0;
    int conflict_num = 0;
    int total_num = 0;
    int hash_size = m_size;
    int chain_len = 0;
    int max_chain_len = 0;

    hashCell* cell = NULL;
    for (int i = 0; i < m_size; i++) {
        cell = m_data[i];

        /* record each hash chain's length and accumulate hash element */
        chain_len = 0;
        while (cell != NULL) {
            fill_rows++;
            chain_len++;
            cell = cell->flag.m_next;
        }

        /* record the number of hash chains with length equal to 1 */
        if (chain_len == 1) {
            single_num++;
        }

        /* record the number of hash chains with length equal to 2 */
        if (chain_len == 2) {
            double_num++;
        }

        /* mark if the length of hash chain is greater than 3, we meet hash confilct */
        if (chain_len >= 3) {
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

void hashBasedOperator::ReplaceEqfunc()
{

    for (int i = 0; i < m_key; i++) {
        switch (m_keyDesc[i].typeId) {
            case TIMETZOID:
                m_eqfunctions[i].fn_addr = timetz_eq_withhead;
                break;
            case TINTERVALOID:
                m_eqfunctions[i].fn_addr = tintervaleq_withhead;
                break;
            case INTERVALOID:
                m_eqfunctions[i].fn_addr = interval_eq_withhead;
                break;
            case NAMEOID:
                m_eqfunctions[i].fn_addr = nameeq_withhead;
                break;
            default:
                break;
        }
    }
}

void hashBasedOperator::freeMemoryContext()
{
    if (m_hashContext != NULL) {
        /* Delete child context for m_hashContext */
        MemoryContextDeleteChildren(m_hashContext);
        MemoryContextDelete(m_hashContext);
        m_hashContext = NULL;
    }

    if (m_tmpContext != NULL) {
        MemoryContextDelete(m_tmpContext);
        m_tmpContext = NULL;
    }
}

/*
 * @Description: caculate hashcontext used
 * @in ctx - context name
 * @in memorySize - pointer to save result size
 * @return - void
 */
static void CalculateHashContextSize(MemoryContext ctx, int64* mem_size, int64* free_size)
{
    AllocSetContext* aset = (AllocSetContext*)ctx;
    MemoryContext child;

    if (ctx == NULL) {
        return;
    }

    /* calculate MemoryContext Stats */
    *mem_size += (aset->totalSpace);
    *free_size += (aset->freeSpace);

    /* recursive MemoryContext's child */
    for (child = ctx->firstchild; child != NULL; child = child->nextchild) {
        CalculateHashContextSize(child, mem_size, free_size);
    }
}

void hashBasedOperator::JudgeMemoryOverflow(char* op_name, int plan_id, int dop, Instrumentation* instrument)
{

    m_rows++;
    int64 used_size = 0;
    int64 free_size = 0;
    CalculateHashContextSize(m_hashContext, &used_size, &free_size);
    bool sys_busy = gs_sysmemory_busy(used_size * dop, false);
    AllocSetContext* set = (AllocSetContext*)m_hashContext;

    if (used_size > m_totalMem || sys_busy) {
        if (m_spillToDisk == false) {
            if (sys_busy) {
                m_sysBusy = true;
                m_totalMem = used_size;
                set->maxSpaceSize = used_size;
                MEMCTL_LOG(LOG,
                    "%s(%d) early spilled, workmem: %ldKB, usedmem: %ldKB, "
                    "hash context freeSpace: %ldKB.",
                    op_name,
                    plan_id,
                    m_totalMem / 1024L,
                    used_size / 1024L,
                    free_size / 1024L);
                pgstat_add_warning_early_spill();
            } else if (m_maxMem > m_totalMem) {
                /* try to spread mem, and record width if failed */
                m_totalMem = used_size;
                int64 spread_mem = Min(Min(dywlm_client_get_memory() * 1024L, m_totalMem), m_maxMem - m_totalMem);
                if (spread_mem > m_totalMem * MEM_AUTO_SPREAD_MIN_RATIO) {
                    m_totalMem += spread_mem;
                    set->maxSpaceSize += spread_mem;
                    m_spreadNum++;
                    MEMCTL_LOG(DEBUG2,
                        "%s(%d) auto mem spread %ldKB succeed, and work mem is %ldKB.",
                        op_name,
                        plan_id,
                        spread_mem / 1024L,
                        m_totalMem / 1024L);
                    return;
                }
                MEMCTL_LOG(LOG,
                    "%s(%d) auto mem spread %ldKB failed, and work mem is %ldKB.",
                    op_name,
                    plan_id,
                    spread_mem / 1024L,
                    m_totalMem / 1024L);
                if (m_spreadNum > 0) {
                    pgstat_add_warning_spill_on_memory_spread();
                }
            }

            if (m_tupleCount != 0) {
                m_colWidth /= m_tupleCount;
            }
            m_tupleCount = -1;
        }

        /*
         * used_size is the available size can be used in calculating hashsize for each temp file.
         */
        m_availmems = used_size;

        /* next slot will be inserted into temp file */
        if (m_spillToDisk == true) {
            m_strategy = HASH_RESPILL;
        }
        else {
            ereport(
                LOG, (errmodule(MOD_VEC_EXECUTOR), errmsg("Profiling Warning : %s(%d) Disk Spilled.", op_name, plan_id)));
            /* first time spill to disk */
            m_fill_table_rows = m_rows;
            m_strategy = HASH_IN_DISK;

            /* cache the memory size into instrument for explain performance */
            if (instrument != NULL) {
                instrument->memoryinfo.peakOpMemory = used_size;
            }
        }
    }
}

/*
 * @Description: judge memory allowed for expanding
 * @return - bool
 */
bool hashBasedOperator::JudgeMemoryAllowExpand()
{
    int64 used_size = 0;
    int64 free_size = 0;
    CalculateHashContextSize(m_hashContext, &used_size, &free_size);

    if (m_totalMem >= used_size * HASH_EXPAND_SIZE) {
        ereport(DEBUG2,
            (errmodule(MOD_VEC_EXECUTOR),
                errmsg("Allow Hash Expand: "
                       "avialmem: %ldKB, current HashContext, totalSpace: %ldKB, freeSpace: %ldKB",
                    m_totalMem / 1024L,
                    used_size / 1024L,
                    free_size / 1024L)));
        return true;
    } else {
        return false;
    }
}
void hashBasedOperator::closeFile()
{
    if (m_filesource) {
        m_filesource->closeAll();
    }
}

int hashBasedOperator::calcFileNum(long numGroups)
{
    int est_size = getPower2LessNum(2 * numGroups / m_rows);
    int file_num = Max(HASH_MIN_FILENUMBER, est_size);
    file_num = Min(file_num, HASH_MAX_FILENUMBER);
    return file_num;
}

/*
 * @Description: create temp hash file source
 * @in batch: template batch used to create file source
 * @in fileNum: number of temp files
 * @in planstate: plan state used to create file source
 * @return: hash file source
 */
hashFileSource* hashBasedOperator::CreateTempFile(VectorBatch* batch, int file_num, PlanState* plan_stat)
{
    MemoryContext stack_ctx = AllocSetContextCreate(CurrentMemoryContext,
        "SingleSideHashTempFileStackContext",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE,
        STACK_CONTEXT);

    hashFileSource* hash_file_src = New(CurrentMemoryContext) hashFileSource(batch,
        stack_ctx,
        m_cellSize,
        NULL,
        false,
        m_cols,
        file_num,
        outerPlanState(plan_stat)->ps_ResultTupleSlot->tts_tupleDescriptor);

    if (u_sess->instr_cxt.global_instr != NULL) {
        hash_file_src->m_spill_size = &plan_stat->instrument->sorthashinfo.spill_size;
    }

    return hash_file_src;
}

Datum vec_hash_any(PG_FUNCTION_ARGS)
{
    char* key = (char*)PG_GETARG_VARCHAR_PP(0);
    int len = PG_GETARG_INT32(1);
    return hash_any((unsigned char*)VARDATA_ANY(key), len);
}

hashOpSource::hashOpSource(PlanState* op) : m_op(op)
{}

VectorBatch* hashOpSource::getBatch()
{
    return VectorEngine(m_op);
}

TupleTableSlot* hashOpSource::getTup()
{
    return ExecProcNode(m_op);
}

hashMemSource::hashMemSource(List* data) : m_list(data), m_cell(NULL)
{
    m_cell = list_head(m_list);
}

hashCell* hashMemSource::getCell()
{
    hashCell* cell = NULL;

    if (m_cell != NULL) {
        cell = (hashCell*)lfirst(m_cell);
        m_cell = lnext(m_cell);
    } else {
        return NULL;
    }

    return cell;
}

hashFileSource::hashFileSource(VectorBatch* batch, MemoryContext context, int cell_size, hashCell* cell_array,
    bool complicate_join, int m_write_cols, int fileNum, TupleDesc tuple_desc)
    : m_cellSize(cell_size),
      m_rownum(NULL),
      m_cols(batch->m_cols),
      m_write_cols(m_write_cols),
      m_context(context),
      m_currentFileIdx(-1)
{
    int i;
    if (complicate_join) {
        /* enlarge the batch for storing hashvalue,  we dont care about the typeid of the last column,
         * because we just use the buffer to save hashvalue.
         * First m_cols columns' attributes
         */
        ScalarDesc* type_arr = (ScalarDesc*)palloc(sizeof(ScalarDesc) * (m_cols + 1));
        for (i = 0; i < m_cols; i++) {
            type_arr[i].typeId = tuple_desc->attrs[i]->atttypid;
            type_arr[i].typeMod = tuple_desc->attrs[i]->atttypmod;
            type_arr[i].encoded = COL_IS_ENCODE(type_arr[i].typeId);
        }
        /* attributes of last column, its typeId we dont care */
        type_arr[m_cols].typeId = UNKNOWNOID;
        type_arr[m_cols].typeMod = -1;
        type_arr[m_cols].encoded = false;
        /* template batch are created with columns 'm_cols + 1' */
        m_batch = New(CurrentMemoryContext) VectorBatch(CurrentMemoryContext, type_arr, m_cols + 1);
    } else {
        m_batch = New(CurrentMemoryContext) VectorBatch(CurrentMemoryContext, batch);
    }

    m_funType = (int*)palloc(m_cols * sizeof(int));
    m_stripFunArray = (stripValFun*)palloc(sizeof(stripValFun) * m_cols);
    for (i = 0; i < m_cols; i++) {
        Oid typid = tuple_desc->attrs[i]->atttypid;
        if (COL_IS_ENCODE(typid)) {
            m_funType[i] = VAR_FUN;
        } else {
            m_funType[i] = SCALAR_FUN;
        }

        switch (typid) {
            case MACADDROID:
            case TIMETZOID:
            case TINTERVALOID:
            case INTERVALOID:
            case NAMEOID:
                m_stripFunArray[i] = ExtractFixedType;
                break;
            case TIDOID:
                m_stripFunArray[i] = ExtractAddrType;
                break;
            case UNKNOWNOID:
            case CSTRINGOID:
                m_stripFunArray[i] = ExtractCstringType;
                break;
            default:
                m_stripFunArray[i] = ExtractVarType;
                break;
        }
    }

    if (u_sess->attr.attr_sql.enable_compress_spill) {
        /* basic functions with compress on */
        m_read[0] = &hashFileSource::readScalar<true>;
        m_read[1] = &hashFileSource::readVar<true>;

        m_write[0] = &hashFileSource::writeScalar<true>;
        m_write[1] = &hashFileSource::writeVar<true>;

        m_rewind = &hashFileSource::rewindCompress;
        m_close = &hashFileSource::closeCompress;

        m_writeBatch = &hashFileSource::writeBatchCompress;
    } else {
        /* basic functions with compress off */
        m_read[0] = &hashFileSource::readScalar<false>;
        m_read[1] = &hashFileSource::readVar<false>;

        m_write[0] = &hashFileSource::writeScalar<false>;
        m_write[1] = &hashFileSource::writeVar<false>;

        m_rewind = &hashFileSource::rewindNoCompress;
        m_close = &hashFileSource::closeNoCompress;

        m_writeBatch = &hashFileSource::writeBatchNoCompress;
    }

    /* basic functions distinguished by complicateJoinKey and compress */
    if (complicate_join) {
        /* for complicate join key, we read/write hash value as well */
        if (u_sess->attr.attr_sql.enable_compress_spill) {
            m_getCell = &hashFileSource::getCellCompress<true>;
            m_writeCell = &hashFileSource::writeCellCompress<true>;
            m_getBatch = &hashFileSource::getBatchCompress<true>;
            m_writeBatchWithHashval = &hashFileSource::writeBatchWithHashvalCompress<true>;
        } else {
            m_getCell = &hashFileSource::getCellNoCompress<true>;
            m_writeCell = &hashFileSource::writeCellNoCompress<true>;
            m_getBatch = &hashFileSource::getBatchNoCompress<true>;
            m_writeBatchWithHashval = &hashFileSource::writeBatchWithHashvalCompress<false>;
        }
    } else {
        /* write/read function do not carry hash value */
        if (u_sess->attr.attr_sql.enable_compress_spill) {
            m_getCell = &hashFileSource::getCellCompress<false>;
            m_writeCell = &hashFileSource::writeCellCompress<false>;
            m_getBatch = &hashFileSource::getBatchCompress<false>;
        } else {
            m_getCell = &hashFileSource::getCellNoCompress<false>;
            m_writeCell = &hashFileSource::writeCellNoCompress<false>;
            m_getBatch = &hashFileSource::getBatchNoCompress<false>;
        }
    }
    m_getTuple = NULL;
    m_writeTuple = NULL;

    m_fileNum = fileNum;
    m_rownum = (int64*)palloc0(sizeof(int64) * m_fileNum);
    m_file = (void**)palloc0(sizeof(void*) * m_fileNum);
    m_fileSize = (int64*)palloc0(sizeof(int64) * m_fileNum);
    for (i = 0; i < m_fileNum; i++) {
        m_file[i] = TempFileCreate();
    }

    m_cellArray = cell_array;

    /* only need in compress mode */
    if (u_sess->attr.attr_sql.enable_compress_spill) {
        m_values = (Datum*)palloc(sizeof(Datum) * tuple_desc->natts);
        m_isnull = (bool*)palloc(sizeof(bool) * tuple_desc->natts);
        m_tupleSize = 100;
        m_tuple = (MinimalTuple)palloc(m_tupleSize);
        m_tuple->t_len = m_tupleSize;
        m_hashTupleSlot = MakeTupleTableSlot(true, tuple_desc->tdTableAmType);
        ExecSetSlotDescriptor(m_hashTupleSlot, tuple_desc);
    }

    m_varSpaceLen = 0;
    m_total_filesize = 0;
    m_spill_size = &m_total_filesize;
}

hashFileSource::hashFileSource(TupleTableSlot* hash_slot, int file_num)
{
    m_hashTupleSlot = MakeTupleTableSlot();

    m_context = NULL;
    if (m_hashTupleSlot->tts_tupleDescriptor == NULL) {
        ExecSetSlotDescriptor(m_hashTupleSlot, hash_slot->tts_tupleDescriptor);
        m_hashTupleSlot->tts_tupslotTableAm = hash_slot->tts_tupleDescriptor->tdTableAmType;
    }

    m_cols = 0;
    m_write_cols = 0;
    m_tupleSize = 100;
    m_cellSize = 0;
    m_currentFileIdx = -1;
    m_varSpaceLen = 0;
    m_fileNum = file_num;
    m_tuple = (MinimalTuple)palloc(m_tupleSize);
    m_tuple->t_len = m_tupleSize;
    m_rownum = (int64*)palloc0(sizeof(int64) * m_fileNum);
    m_file = (void**)palloc0(sizeof(void*) * file_num);
    m_fileSize = (int64*)palloc0(sizeof(int64) * m_fileNum);
    for (int i = 0; i < file_num; i++) {
        m_file[i] = (void*)BufFileCreateTemp(false);
    }

    m_writeTuple = &hashFileSource::writeTupCompress<false>;
    m_getTuple = &hashFileSource::getTupCompress<false>;
    m_writeCell = NULL;
    m_getCell = NULL;
    m_writeBatch = NULL;
    m_writeBatchWithHashval = NULL;
    m_getBatch = NULL;
    m_rewind = &hashFileSource::rewindNoCompress;
    m_close = &hashFileSource::closeNoCompress;
    m_cellArray = NULL;
    m_stripFunArray = NULL;
    m_batch = NULL;
    m_funType = NULL;

    m_total_filesize = 0;
    m_spill_size = &m_total_filesize;
    m_values = NULL;
    m_isnull = NULL;
}

/*
 * @Description: rewind in compress mode
 * @in idx - file idx
 * @return - void
 */
void hashFileSource::rewindCompress(int idx)
{
    LZ4FileRewind((LZ4File*)m_file[idx]);
}

/*
 * @Description: rewind in no compress mode
 * @in idx - file idx
 * @return - void
 */
void hashFileSource::rewindNoCompress(int idx)
{
    if (BufFileSeek((BufFile*)m_file[idx], 0, 0L, SEEK_SET)) {
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not rewind hash-join temporary file: %m")));
    }
}

/*
 * @Description: file rewind
 * @in idx - file idx
 * @return - void
 */
void hashFileSource::rewind(int idx)
{
    InvokeFp(m_rewind)(idx);
}

/*
 * @Description: close file in compress mode
 * @in idx - file idx
 * @return - void
 */
void hashFileSource::closeCompress(int idx)
{
    if (m_file == NULL) {
        return;
    }

    if (m_file[idx]) {
        LZ4FileClose((LZ4File*)m_file[idx]);
    }

    m_file[idx] = NULL;
}

/*
 * @Description: close file in no compress mode
 * @in idx - file idx
 * @return - void
 */
void hashFileSource::closeNoCompress(int idx)
{
    if (m_file == NULL) {
        return;
    }

    if (m_file[idx]) {
        BufFileClose((BufFile*)(m_file[idx]));
    }

    m_file[idx] = NULL;
}

/*
 * @Description: close all files
 * @return: void
 */
void hashFileSource::closeAll()
{
    for (int i = 0; i < m_fileNum; i++) {
        close(i);
    }
}

/*
 * @Description: close file
 * @in idx - file idx
 * @return - void
 */
void hashFileSource::close(int idx)
{
    if (idx >= 0) {
        InvokeFp(m_close)(idx);
    }
}

void hashFileSource::setCurrentIdx(int idx)
{
    m_currentFileIdx = idx;
}

/*
 * @Description: get current file index
 * @return: curent file index
 */
int hashFileSource::getCurrentIdx()
{
    return m_currentFileIdx;
}

/*
 * @Description: collect numbers of rows exceeds rows_in_mem for all files
 * 		with index >= m_currentFileIdx
 * @in rows_in_mem: number of rows in memory
 * @return: total numbers
 */
int64 hashFileSource::getCurrentIdxRownum(int64 rows_in_mem)
{
    int64 nrows = 0;
    int64 addrows = 0;
    for (int i = m_currentFileIdx; i < m_fileNum; i++) {
        if (m_rownum[i] > rows_in_mem) {
            addrows = m_rownum[i] - rows_in_mem;
        } else {
            addrows = 0;
        }

        nrows += addrows;
    }
    return nrows;
}

/*
 * @Description: move onto the next temp file
 * @return: true(success) or false(no more files)
 */
bool hashFileSource::next()
{
    m_currentFileIdx++;

    while (m_currentFileIdx < m_fileNum && m_rownum[m_currentFileIdx] == 0) {
        close(m_currentFileIdx);
        m_currentFileIdx++;
    }

    if (m_currentFileIdx < m_fileNum) {
        return true;
    } else {
        return false;
    }
}

/*
 * @Description: enlarge file sources
 * @in fileNum: enlarged numbers
 * @return: void
 */
void hashFileSource::enlargeFileSource(int file_num)
{
    int i, j;
    errno_t rc = EOK;

    if (file_num <= 0) {
        return;
    }

    m_file = (void**)repalloc(m_file, (file_num + m_fileNum) * sizeof(void*));
    m_rownum = (int64*)repalloc(m_rownum, (file_num + m_fileNum) * sizeof(int64));
    m_fileSize = (int64*)repalloc(m_fileSize, (file_num + m_fileNum) * sizeof(int64));

    for (j = m_fileNum, i = 0; i < file_num; i++, j++) {
        m_file[j] = TempFileCreate();
    }
    rc = memset_s(m_rownum + m_fileNum, sizeof(int64) * file_num, '\0', sizeof(int64) * file_num);
    securec_check(rc, "\0", "\0");
    rc = memset_s(m_fileSize + m_fileNum, sizeof(int64) * file_num, '\0', sizeof(int64) * file_num);
    securec_check(rc, "\0", "\0");

    /* refresh the file number */
    m_fileNum += file_num;
}

/*
 * @Description: write hash value to temp file
 * @in val - hash value
 * @in flag - hash flag
 * @in idx - file idx
 * @return - size_t, write size
 */
template <bool compress_spill>
size_t hashFileSource::writeScalar(ScalarValue val, uint8 flag, int idx)
{
    size_t nread;
    hashVal value;

    value.flag = flag;
    value.val = val;

    nread = TempFileWrite<compress_spill>(m_file[idx], (void*)(&value), sizeof(hashVal));
    if (nread != sizeof(hashVal)) {
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not write into hash-join temporary file: %m")));
    }

    return nread;
}

/*
 * @Description: write cell value to temp file
 * @in val - cell value
 * @in flag - cell flag
 * @in idx - file idx
 * @return - size_t,  write size
 */
template <bool compress_spill>
size_t hashFileSource::writeVar(ScalarValue val, uint8 flag, int idx)
{
    size_t written;
    size_t ntotal = 0;
    Datum value = 0;
    uint32 val_size = 0;

    if (IS_NULL(flag) == false) {
        value = ScalarVector::Decode(val);
        val_size = VARSIZE_ANY(value);
    } else {
        val_size = sizeof(uint32);
        value = PointerGetDatum(&val_size);  // write a value whatever for null.
    }

    written = TempFileWrite<compress_spill>(m_file[idx], (void*)(&val_size), sizeof(uint32));
    if (written != sizeof(uint32)) {
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not read from hash-join temporary file: %m")));
    }
    ntotal += written;

    written = TempFileWrite<compress_spill>(m_file[idx], (void*)(value), val_size);
    if (written != val_size) {
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not read from hash-join temporary file: %m")));
    }
    ntotal += written;

    written = TempFileWrite<compress_spill>(m_file[idx], (void*)(&flag), sizeof(uint8));
    if (written != sizeof(uint8)) {
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not read from hash-join temporary file: %m")));
    }
    ntotal += written;

    return ntotal;
}

/*
 * @Description :  write the minituple to file with compress
 * @in tuple - the tuple to be written
 * @in idx - file index
 * @return - void
 */
template <bool compress_spill>
size_t hashFileSource::writeTupCompress(MinimalTuple tuple, int idx)
{
    size_t written;

    m_rownum[idx]++;
    written = TempFileWrite<compress_spill>(m_file[idx], (void*)tuple, tuple->t_len);
    if (written != tuple->t_len) {
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not write to hashAgg temporary file: %m")));
    }
    return written;
}

/*
 * @Description :  write the minituple to file
 * @in tuple - the tuple to be written
 * @in idx - file index
 * @return - void
 */
void hashFileSource::writeTup(MinimalTuple tuple, int file_idx)
{
    size_t written = InvokeFp(m_writeTuple)(tuple, file_idx);
    m_fileSize[file_idx] += written;
    *m_spill_size += written;
    pgstat_increase_session_spill_size(written);
}

/*
 * @Description :  read the hash value from the file
 * @in val - the  hash value read from file
 * @in flag - hash value flag
 * @return - size_t, read size
 */
template <bool compress_spill>
size_t hashFileSource::readScalar(ScalarValue* val, uint8* flag)
{
    size_t nread;
    hashVal value;
    nread = TempFileRead<compress_spill>(m_file[m_currentFileIdx], (void*)(&value), sizeof(hashVal));

    if (nread == 0) {
        return 0;
    }

    if (nread != sizeof(hashVal)) {
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not read from hash-join temporary file: %m")));
    }

    *val = value.val;
    *flag = value.flag;

    return nread;
}

/*
 * @Description :  read the value from the file
 * @in val - the value read from file
 * @in flag - value flag
 * @return - size_t, read size
 */
template <bool compress_spill>
size_t hashFileSource::readVar(ScalarValue* val, uint8* flag)
{
    size_t nread;
    uint32 var_size;
    char* var_addr = NULL;

    nread = TempFileRead<compress_spill>(m_file[m_currentFileIdx], (void*)(&var_size), sizeof(uint32));
    if (nread == 0) {
        return 0;
    }

    /* we allocate buffer (in m_context) to save the variable data */
    AutoContextSwitch mem_guard(m_context);
    var_addr = (char*)palloc(var_size * sizeof(char));

    nread = TempFileRead<compress_spill>(m_file[m_currentFileIdx], (void*)(var_addr), var_size);
    if (nread != var_size) {
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not read from hash-join temporary file: %m")));
    }

    nread = TempFileRead<compress_spill>(m_file[m_currentFileIdx], (void*)(flag), sizeof(uint8));
    if (nread != sizeof(uint8)) {
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not read from hash-join temporary file: %m")));
    }

    if (IS_NULL(*flag) == false) {
        *val = PointerGetDatum(var_addr);
    } else {
        *val = 0;  // set a value whatever
    }

    return nread;
}

/*
 * @Description :  get tuple
 * @return: the tuple we obtained
 */
TupleTableSlot* hashFileSource::getTup()
{
    return InvokeFp(m_getTuple)();
}

/*
 * @Description :  get tuple with compress from temp file
 * @return: the tuple we obtained
 */
template <bool compress_spill>
TupleTableSlot* hashFileSource::getTupCompress()
{
    uint32 tuplen;
    size_t nread;
    errno_t rc = EOK;

    nread = TempFileRead<compress_spill>(m_file[m_currentFileIdx], (void*)&tuplen, sizeof(uint32));

    if (nread == 0) {
        return NULL;
    }

    if (nread != sizeof(uint32)) {
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not read from hashAgg temporary file: %m")));
    }

    if (tuplen > m_tupleSize) {
        m_tupleSize = tuplen + 10;
        m_tuple = (MinimalTuple)repalloc(m_tuple, m_tupleSize);
    }

    rc = memset_s(m_tuple, m_tupleSize, 0, m_tupleSize);
    securec_check(rc, "\0", "\0");
    m_tuple->t_len = tuplen;
    nread = TempFileRead<compress_spill>(
        m_file[m_currentFileIdx], (void*)((char*)m_tuple + sizeof(uint32)), tuplen - sizeof(uint32));

    if (nread != tuplen - sizeof(uint32)) {
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not read from hashAgg temporary file: %m")));
    }

    return ExecStoreMinimalTuple(m_tuple, m_hashTupleSlot, false);
}

void hashFileSource::freeFileSource()
{
    (void)ExecClearTuple(m_hashTupleSlot);

    if (m_hashTupleSlot->tts_tupleDescriptor) {
        ReleaseTupleDesc(m_hashTupleSlot->tts_tupleDescriptor);
        m_hashTupleSlot->tts_tupleDescriptor = NULL;
    }

    if (m_tuple) {
        pfree_ext(m_tuple);
        m_tuple = NULL;
    }

    if (m_rownum) {
        pfree_ext(m_rownum);
        m_rownum = NULL;
    }

    if (m_file) {
        pfree_ext(m_file);
        m_file = NULL;
    }

    if (m_fileSize) {
        pfree_ext(m_fileSize);
        m_fileSize = NULL;
    }
}

/*
 * @Description :  write cell value no compress to temp file
 * @in cell - the cell value to be written
 * @in fileIdx - file idx
 * @return - void
 */
template <bool write_hashval>
size_t hashFileSource::writeCellNoCompress(hashCell* cell, int fileIdx)
{
    size_t written = 0;
    hashVal* val = &cell->m_val[0];
    m_rownum[fileIdx]++;

    for (int i = 0; i < m_cols; i++){
        written += RuntimeBinding(m_write, m_funType[i])(val[i].val, val[i].flag, fileIdx);
    }

    if (write_hashval) {
        written += writeScalar<false>(val[m_write_cols].val, val[m_write_cols].flag, fileIdx);
    }

    return written;
}

/*
 * @Description :  write cell value with compress to temp file
 * @in cell - the cell value to be written
 * @in fileIdx - file idx
 * @return - void
 */
template <bool write_hashval>
size_t hashFileSource::writeCellCompress(hashCell* cell, int file_idx)
{
    size_t ntotal = 0;
    hashVal* val = &cell->m_val[0];

    for (int i = 0; i < m_cols; i++) {
        if (NOT_NULL(val[i].flag)) {
            m_values[i] = (*m_stripFunArray[i])(&val[i].val);
        }

        m_isnull[i] = IS_NULL(val[i].flag);

        /* record row size */
        ntotal += sizeof(hashVal);

        if (m_funType[i] == VAR_FUN && !m_isnull[i]) {
            ntotal += VARSIZE_ANY(val[i].val);
        }
    }

    if (write_hashval) {
        size_t written;
        /* the type of  hashval is uint64 */
        written = TempFileWrite<true>(m_file[file_idx], (void*)&val[m_write_cols].val, sizeof(uint64));
        if (written != sizeof(uint64)) {
            ereport(ERROR, (errcode_for_file_access(), errmsg("could not write to hashJoin temporary file: %m")));
        }
        ntotal += written;
    }

    /* restore the len */
    m_tuple->t_len = m_tupleSize;

    m_tuple = tableam_tops_form_minimal_tuple(m_hashTupleSlot->tts_tupleDescriptor, m_values, m_isnull, m_tuple, HEAP_TUPLE);
    m_tupleSize = (m_tuple->t_len > m_tupleSize) ? m_tuple->t_len : m_tupleSize;
    writeTupCompress<true>(m_tuple, file_idx);

    /* the actual size without transform or compress */
    return ntotal;
}

void hashFileSource::writeBatch(VectorBatch* batch, int idx, HashKey key)
{
    int file_idx = key & (unsigned int)(m_fileNum - 1);
    size_t written = InvokeFp(m_writeBatch)(batch, idx, file_idx);
    m_fileSize[file_idx] += written;

    *m_spill_size += written;
    pgstat_increase_session_spill_size(written);
}

/*
 * @Description :  write batch value  to temp file
 * @in batch - the batch value to be written
 * @in idx - batch rows idx
 * @in fileIdx - file idx
 * @return - void
 */
void hashFileSource::writeBatchToFile(VectorBatch* batch, int idx, int file_idx)
{
    size_t written = InvokeFp(m_writeBatch)(batch, idx, file_idx);
    m_fileSize[file_idx] += written;

    *m_spill_size += written;
    pgstat_increase_session_spill_size(written);
}

/*
 * @Description :  write batch value no compress to temp file
 * @in batch - the batch value to be written
 * @in idx - batch rows idx
 * @in fileIdx - file idx
 * @return - void
 */
size_t hashFileSource::writeBatchNoCompress(VectorBatch* batch, int idx, int file_idx)
{
    size_t written = 0;
    ScalarVector* p_vector = NULL;
    m_rownum[file_idx]++;

    for (int i = 0; i < m_cols; i++) {
        p_vector = &batch->m_arr[i];
        written += RuntimeBinding(m_write, m_funType[i])(p_vector->m_vals[idx], p_vector->m_flag[idx], file_idx);
    }

    return written;
}

/*
 * @Description :  write the batch to file with compress
 * @in batch - the batch to be written
 * @in idx - current row index in batch
 * @in fileIdx - file index
 * @return - void
 */
size_t hashFileSource::writeBatchCompress(VectorBatch* batch, int idx, int file_idx)
{
    ScalarVector* p_vector = NULL;
    size_t written = 0;

    for (int i = 0; i < m_cols; i++) {
        p_vector = &batch->m_arr[i];
        if (NOT_NULL(p_vector->m_flag[idx])) {
            m_values[i] = (*m_stripFunArray[i])(&p_vector->m_vals[idx]);
        }
        m_isnull[i] = IS_NULL(p_vector->m_flag[idx]);

        /* record row size before transform and compress */
        written += sizeof(hashVal);
        if (m_funType[i] == VAR_FUN && !m_isnull[i]) {
            written += VARSIZE_ANY(p_vector->m_vals[idx]);
        }
    }

    /* restore the len */
    m_tuple->t_len = m_tupleSize;
	m_tuple = tableam_tops_form_minimal_tuple(m_hashTupleSlot->tts_tupleDescriptor, m_values, m_isnull, m_tuple, HEAP_TUPLE);
    m_tupleSize = (m_tuple->t_len > m_tupleSize) ? m_tuple->t_len : m_tupleSize;

    writeTupCompress<true>(m_tuple, file_idx);

    /* the actual size without transform or compress */
    return written;
}

/*
 * @Description :  write the batch with hash value to file in compress
 * @in batch - the batch to be written
 * @in idx - current row index in batch
 * @in key - hash value to be written
 * @in fileIdx - file index
 * @return - void
 */
template <bool compress_spill>
size_t hashFileSource::writeBatchWithHashvalCompress(VectorBatch* batch, int idx, HashKey key, int file_idx)
{
    size_t written = 0;
    ScalarValue hash_val = key;

    if (compress_spill == false) {
        written = writeBatchNoCompress(batch, idx, file_idx);
        written += writeScalar<false>(UInt32GetDatum(hash_val), 0, file_idx);
    } else {
        /* the type of  hash_val is uint64 */
        written = TempFileWrite<true>(m_file[file_idx], (void*)&hash_val, sizeof(uint64));

        if (written != sizeof(uint64)) {
            ereport(ERROR, (errcode_for_file_access(), errmsg("could not write to hashJoin temporary file: %m")));
        }

        written += writeBatchCompress(batch, idx, file_idx);
    }

    return written;
}

/*
 * @Description :  write the batch with hash value to file
 * @in batch - the batch to be written
 * @in idx - current row index in batch
 * @in key - hash key to be written
 * @return - void
 */
void hashFileSource::writeBatchWithHashval(VectorBatch* batch, int idx, HashKey key)
{
    int file_idx = key & (unsigned int)(m_fileNum - 1);
    size_t written = InvokeFp(m_writeBatchWithHashval)(batch, idx, key, file_idx);
    m_fileSize[file_idx] += written;

    *m_spill_size += written;
    pgstat_increase_session_spill_size(written);
}

/*
 * @Description :  write the batch with hash value to a specific file
 * @in batch - the batch to be written
 * @in idx - current row index in batch
 * @in key - hash key to be written
 * @in fileIdx - file index
 * @return - void
 */
void hashFileSource::writeBatchWithHashval(VectorBatch* batch, int idx, HashKey key, int file_idx)
{
    size_t written = InvokeFp(m_writeBatchWithHashval)(batch, idx, key, file_idx);
    m_fileSize[file_idx] += written;

    *m_spill_size += written;
    pgstat_increase_session_spill_size(written);
}

/*
 * @Description :  get batch value
 */
VectorBatch* hashFileSource::getBatch()
{
    CHECK_FOR_INTERRUPTS();

    return InvokeFp(m_getBatch)();
}

/*
 * @Description :  deform slot to fill vector batch
 * @in slot - the slot to be deform
 * @in idx - batch rows
 * @return -void
 */
void hashFileSource::assembleBatch(TupleTableSlot* slot, int idx)
{
    ScalarVector* pVector = NULL;
    for (int i = 0; i < m_cols; i++) {
        pVector = &m_batch->m_arr[i];
        if (slot->tts_isnull[i] == false) {
            SET_NOTNULL(pVector->m_flag[idx]);
            if (pVector->m_desc.encoded) {
                /*
                 * For variable datum read from file(such as slot), it doesn't contain headers, so we
                 * allocate a new buffer, which consists of a header and cooresponding datum(in slot)
                 * or pointer under a given MemoryContext (such as m_context), to save the variable
                 * datum.
                 * Note: we need buffer here(temp or not), so need a memory context
                 */
                pVector->m_vals[idx] = DatumToScalarInContext(m_context, slot->tts_values[i], pVector->m_desc.typeId);
            } else {
                /* for vector engine,  pVector->m_desc.typeId is INT8OID, not TIDOID */
                if (slot->tts_tupleDescriptor->attrs[i]->atttypid == TIDOID) {
                    pVector->m_vals[idx] = 0;
                    ItemPointer destTid = (ItemPointer)&pVector->m_vals[idx];
                    ItemPointer srcTid = (ItemPointer)DatumGetPointer(slot->tts_values[i]);
                    *destTid = *srcTid;
                } else {
                    pVector->m_vals[idx] = slot->tts_values[i];
                }
            }
        } else {
            SET_NULL(pVector->m_flag[idx]);
        }
    }
}

/*
 * @Description : get data from temp file
 * @return		: the batch read from file
 */
template <bool get_hashval>
VectorBatch* hashFileSource::getBatchCompress()
{
    int idx = 0;
    bool fetch_data = true;
    TupleTableSlot* slot = NULL;
    uint64 hkey;
    size_t nread;

    while (fetch_data) {
        if (get_hashval) {
            /* try to read hashvalue.
             * Note: 1) we get hash value first because, for one row, hash value are saved in
             * the front of other data 2) hash value without flag
             */
            nread = TempFileRead<true>(m_file[m_currentFileIdx], (void*)&hkey, sizeof(uint64));
            if (nread == 0) {
                /* no more data, stop read */
                break;
            }
            if (nread != sizeof(uint64)) {
                ereport(ERROR,
                    (errcode_for_file_access(), errmsg("could not read hashkey from hash-join temporary file: %m")));
            }

            /* we save hash value in the last column i.e. m_cols */
            m_batch->m_arr[m_cols].m_vals[idx] = hkey;
            SET_NOTNULL(m_batch->m_arr[m_cols].m_flag[idx]);
        }

        slot = getTupCompress<true>();

        if (slot == NULL) {
            /* the end */
            break;
        }

        /* Get the Table Accessor Method*/
        Assert(slot != NULL && slot->tts_tupleDescriptor != NULL);

        tableam_tslot_getallattrs(slot);
        Assert(slot->tts_nvalid == m_cols);

        /* Pack one slot into vectorbatch */
        assembleBatch(slot, idx);

        idx++;
        if (BatchMaxSize == idx) {
            break;
        }
    }

    if (idx == 0) {
        /* not data read from file */
        return NULL;
    }

    m_batch->FixRowCount(idx);
    return m_batch;
}

/*
 * @Description : get data from temp file
 * @return		: the batch read from file
 */
template <bool get_hashval>
VectorBatch* hashFileSource::getBatchNoCompress()
{
    size_t nread;
    int idx = 0;
    bool fetch_data = true;
    int i;
    ScalarVector* p_vector = NULL;
    hashVal hval;

    while (fetch_data) {
        for (i = 0; i < m_cols; i++) {
            p_vector = &m_batch->m_arr[i];
            nread = RuntimeBinding(m_read, m_funType[i])(&p_vector->m_vals[idx], &p_vector->m_flag[idx]);
            if (nread == 0) { /* get end of file */
                Assert(i == 0);
                fetch_data = false;
                break;
            }
        }

        if (fetch_data == false) {
            break;
        }

        if (get_hashval) {
            /* try to read hashvalue.
             * Note: for no compress mode, hash value, for one row, along with a flag are saved in the
             * behind of the other data
             */
            nread = readScalar<false>(&hval.val, &hval.flag);

            if (nread == 0) {
                /* no more data */
                break;
            } else {
                /* hashvalue are saved in the last column of the batch
                 * note: the m_batch should have at least m_cols+1 columns
                 */
                m_batch->m_arr[m_cols].m_vals[idx] = hval.val;
                m_batch->m_arr[m_cols].m_flag[idx] = hval.flag;
            }
        }

        idx++;
        if (idx == BatchMaxSize) {
            break;
        }
    }

    if (idx == 0) {
        return NULL;
    }

    for (i = 0; i < m_cols; i++) {
        m_batch->m_arr[i].m_rows = idx;
    }

    m_batch->m_rows = idx;
    return m_batch;
}

/*
 * @Description :  get the cell from file where the data be compressed
 * @return		: hashcell
 */
template <bool get_hashval>
hashCell* hashFileSource::getCellCompress()
{
    size_t nread;
    int idx = 0;
    bool fetch_data = true;
    int i;
    TupleTableSlot* slot = NULL;
    hashCell* cell = m_cellArray;
    uint64 header;
    uint64 hash_val;

    while (fetch_data) {
        if (get_hashval == true) {
            /* the type of  hashval is uint64 */
            nread = TempFileRead<true>(m_file[m_currentFileIdx], (void*)&header, sizeof(uint64));
            if (nread == 0) { /* end of file */
                fetch_data = false;
                break;
            }

            if (nread != sizeof(header)) {
                ereport(ERROR, (errcode_for_file_access(), errmsg("could not read from hash-join temporary file: %m")));
            }

            hash_val = header;
        }
        slot = getTupCompress<true>();

        if (slot == NULL) {
            fetch_data = false;
            break;
        }

        /*Get the Table Accessor Method*/
        Assert(slot != NULL && slot->tts_tupleDescriptor != NULL);

        tableam_tslot_getallattrs(slot);
        Assert(slot->tts_nvalid == m_cols);

        for (i = 0; i < m_cols; i++) {
            ScalarVector* pVector = &m_batch->m_arr[i];
            if (slot->tts_isnull[i] == false) {
                if (pVector->m_desc.encoded) {
                    /*
                     * For variable datum read from file(such as slot), it doesn't contain headers, so we
                     * allocate a new buffer, which consists of a header and cooresponding datum(in slot)
                     * or pointer under a given MemoryContext (such as m_context), to save the variable
                     * datum.
                     * Note: we need buffer here(temp or not), so need a memory context
                     */
                    cell->m_val[i].val = DatumToScalarInContext(m_context, slot->tts_values[i], pVector->m_desc.typeId);
                } else {
                    /* for vector engine,  pVector->m_desc.typeId is INT8OID, not TIDOID */
                    if (TIDOID == slot->tts_tupleDescriptor->attrs[i]->atttypid) {
                        ItemPointer destTid = (ItemPointer)&cell->m_val[i].val;
                        ItemPointer srcTid = (ItemPointer)DatumGetPointer(slot->tts_values[i]);
                        *destTid = *srcTid;
                    } else {
                        cell->m_val[i].val = slot->tts_values[i];
                    }
                }
                SET_NOTNULL(cell->m_val[i].flag);
            } else {
                SET_NULL(cell->m_val[i].flag);
            }
        }

        if (get_hashval == true) {
            cell->m_val[m_write_cols].val = hash_val;
            SET_NOTNULL(cell->m_val[m_write_cols].flag);
        }

        idx++;
        if (idx == BatchMaxSize) {
            break;
        }

        cell = (hashCell*)((char*)cell + m_cellSize);
    }

    if (idx == 0) {
        return NULL;
    }

    m_cellArray->flag.m_rows = idx;
    return m_cellArray;
}

/*
 * @Description :  get the cell from file where the data not compresse
 * @return		: hashcell
 */
template <bool get_hashval>
hashCell* hashFileSource::getCellNoCompress()
{
    size_t nread;
    int idx = 0;
    bool fetch_data = true;
    int i;

    hashCell* cell = m_cellArray;

    while (fetch_data) {
        for (i = 0; i < m_cols; i++) {
            nread = RuntimeBinding(m_read, m_funType[i])(&cell->m_val[i].val, &cell->m_val[i].flag);
            if (nread == 0) { /* get end of file */
                Assert(i == 0);
                fetch_data = false;
                break;
            }
        }

        if (get_hashval == true) {
            nread = readScalar<false>(&cell->m_val[m_write_cols].val, &cell->m_val[m_write_cols].flag);

            if (nread == 0) {
                fetch_data = false;
            }
        }

        if (fetch_data == false) {
            break;
        }

        idx++;
        if (idx == BatchMaxSize) {
            break;
        }

        cell = (hashCell*)((char*)cell + m_cellSize);
    }

    if (idx == 0) {
        return NULL;
    }

    m_cellArray->flag.m_rows = idx;

    return m_cellArray;
}

hashCell* hashFileSource::getCell()
{
    return InvokeFp(m_getCell)();
}

void hashFileSource::writeCell(hashCell* cell, HashKey key)
{
    int file_idx = key & (unsigned int)(m_fileNum - 1);
    m_fileSize[file_idx] += InvokeFp(m_writeCell)(cell, file_idx);
}

hashSortSource::hashSortSource(Batchsortstate* batch_sort_stat, VectorBatch* sort_batch)
    : m_SortBatch(sort_batch), m_batchSortStateIn(batch_sort_stat)
{}

VectorBatch* hashSortSource::getBatch()
{
    batchsort_getbatch(m_batchSortStateIn, true, m_SortBatch);
    return m_SortBatch;
}

void hashFileSource::resetVariableMemberIfNecessary(int file_num)
{
    errno_t rc = 0;
    if (m_context != NULL) {
        MemoryContextReset(m_context);
    }

    if (file_num <= m_fileNum) {
        rc = memset_s(m_rownum, sizeof(int64) * m_fileNum, '\0', sizeof(int64) * m_fileNum);
        securec_check(rc, "\0", "\0");
        rc = memset_s(m_file, sizeof(void*) * m_fileNum, '\0', sizeof(void*) * m_fileNum);
        securec_check(rc, "\0", "\0");
        rc = memset_s(m_fileSize, sizeof(int64) * m_fileNum, '\0', sizeof(int64) * m_fileNum);
        securec_check(rc, "\0", "\0");
        m_fileNum = file_num;
    } else {
        m_fileNum = file_num;
        pfree_ext(m_rownum);
        pfree_ext(m_file);
        pfree_ext(m_fileSize);
        m_rownum = (int64*)palloc0(sizeof(int64) * m_fileNum);
        m_file = (void**)palloc0(sizeof(void*) * m_fileNum);
        m_fileSize = (int64*)palloc0(sizeof(int64) * m_fileNum);
    }

    for (int i = 0; i < m_fileNum; i++) {
        m_file[i] = TempFileCreate();
    }
}

/*
 * @Description: free the buffer in a given file handler if necessary
 * @in fileIdx: file index of the current file set
 * @return: void
 *
 * Note:
 * 1) In a LZ4File handler, the buffer in the handler consists of
 * 		srcBuf: LZ4FileSrcBufSize = BLCKSZ * 8 = 64KB
 * and
 * 		compressBuf: [0, n] KB,  n ~ 64 KB
 * To avoid memory being allocated but unused at present, we can
 * release the buffer.
 *
 * 2) After run ReleaseFileHandlerBuffer(), the caller should call function
 * PrepareFileHandlerBuffer() to re-allocate buffer before read/write data
 * from/into temp files.
 */
void hashFileSource::ReleaseFileHandlerBuffer(int file_idx)
{
    /* In no_compress mode, nothing to do */
    if (u_sess->attr.attr_sql.enable_compress_spill == false || m_file == NULL) {
        return;
    }

    /* We only release the buffer in compress mode */
    LZ4File* lz4_file = NULL;

    Assert(file_idx < m_fileNum);
    lz4_file = (LZ4File*)m_file[file_idx];

    if (lz4_file != NULL) {
        /* First, flush the data(in buffer), if any, into disk */
        LZ4FileClearBuffer(lz4_file);

        /* Then, free the buffer */
        if (lz4_file->srcBuf) {
            pfree_ext(lz4_file->srcBuf);
            lz4_file->srcBuf = NULL;
        }
        if (lz4_file->compressBuf) {
            pfree_ext(lz4_file->compressBuf);
            lz4_file->compressBuf = NULL;
            lz4_file->compressBufSize = 0;
        }
    }
}

/*
 * @Description: release the buffer in all file handlers if necessary
 * @return: void
 */
void hashFileSource::ReleaseAllFileHandlerBuffer()
{
    /* release buffer one by one */
    for (int i = 0; i < m_fileNum; i++) {
        ReleaseFileHandlerBuffer(i);
    }
}

/*
 * @Description: prepare the buffer for read/write data from/into a
 * 	given temp file
 * @in fileIdx: the temp file index of the file set
 * @return: void
 */
void hashFileSource::PrepareFileHandlerBuffer(int file_idx)
{
    /* In no_compress mode, nothing to do */
    if (u_sess->attr.attr_sql.enable_compress_spill == false || m_file == NULL) {
        return;
    }

    /* We only prepare the buffer for compress mode */
    LZ4File* lz4_file = NULL;

    Assert(file_idx < m_fileNum);
    lz4_file = (LZ4File*)m_file[file_idx];

    if (lz4_file != NULL) {
        /* allocate the srcBuf and compressBuf */
        if (lz4_file->srcBuf == NULL) {
            lz4_file->srcBuf = (char*)palloc(LZ4FileSrcBufSize);
            lz4_file->srcDataSize = 0;
        }
        if (lz4_file->compressBuf == NULL) {
            lz4_file->compressBuf = (char*)palloc(LZ4FileSrcBufSize);
            lz4_file->compressBufSize = LZ4FileSrcBufSize;
        }
    }
}

/*
 * @Description: Reset fileSource when Rescan.
 * @return: void
 */
void hashFileSource::resetFileSource()
{
    if (m_context != NULL) {
        MemoryContextReset(m_context);
    }

    if (m_rownum) {
        pfree_ext(m_rownum);
        m_rownum = NULL;
    }

    if (m_file) {
        pfree_ext(m_file);
        m_file = NULL;
    }

    if (m_fileSize) {
        pfree_ext(m_fileSize);
        m_fileSize = NULL;
    }

    m_fileNum = 0;
    m_currentFileIdx = -1;
}

/*
 * @Description: initialize fileSource.
 * @in fileNum: Current file number.
 * @return: void
 */
void hashFileSource::initFileSource(int file_num)
{
    m_fileNum = file_num;
    m_rownum = (int64*)palloc0(sizeof(int64) * m_fileNum);
    m_file = (void**)palloc0(sizeof(void*) * m_fileNum);
    m_fileSize = (int64*)palloc0(sizeof(int64) * m_fileNum);
    for (int i = 0; i < m_fileNum; i++) {
        m_file[i] = TempFileCreate();
    }
}
