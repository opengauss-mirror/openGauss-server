/*
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
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
 * batchstore.cpp
 *
 * IDENTIFICATION
 *    src/common/backend/utils/sort/batchstore.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#include <limits.h>

#include "executor/executor.h"
#include "miscadmin.h"
#include "pg_trace.h"
#ifdef PGXC
#include "pgxc/execRemote.h"
#include "catalog/pgxc_node.h"
#endif
#include "access/nbtree.h"
#include "utils/memutils.h"
#include "utils/pg_rusage.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/syscache.h"
#include "utils/lsyscache.h"
#include "utils/logtape.h"
#include "commands/tablespace.h"
#include "catalog/pg_type.h"
#include "pgxc/execRemote.h"
#include "utils/datum.h"
#include "vecexecutor/vecvar.h"
#include "vecexecutor/vectorbatch.h"
#include "utils/builtins.h"
#include "executor/tuptable.h"
#include "storage/buf/buffile.h"
#include "utils/resowner.h"
#include "utils/batchstore.h"

BatchStore* batchstore_begin_heap(
    TupleDesc tupDesc, bool randomAccess, bool interXact, int64 maxKBytes, int maxMem, int planId, int dop)
{
    BatchStore* state = NULL;
    int eflags;
    MemoryContext oldcontext, storecontext;

    /*
     * This interpretation of the meaning of randomAccess is compatible with
     * the pre-8.3 behavior of tuplestores.
     */
    eflags = randomAccess ? (EXEC_FLAG_BACKWARD | EXEC_FLAG_REWIND) : (EXEC_FLAG_REWIND);

    storecontext = AllocSetContextCreate(CurrentMemoryContext,
        "BatchStore",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE,
        STANDARD_CONTEXT,
        maxKBytes * 1024L);

    oldcontext = MemoryContextSwitchTo(storecontext);

    state = (BatchStore*)palloc0(sizeof(BatchStore));
    state->m_status = BSS_INMEM;
    state->m_eflags = eflags;
    state->m_interXact = interXact;
    state->m_allowedMem = maxKBytes * 1024L - 1;
    state->m_availMem = state->m_allowedMem;
    state->m_myfile = NULL;
    state->m_storecontext = storecontext;
    state->m_resowner = t_thrd.utils_cxt.CurrentResourceOwner;
    state->m_lastFetchCursor = 0;
    state->m_curFetchCursor = 0;
    state->m_colInfo = NULL;
    state->tupDesc = tupDesc;
    state->m_colNum = tupDesc->natts;
    state->m_lastbatchnum = 0;
    state->m_lastfile_num = 0;
    state->m_lastwritebatchnum = 0;
    state->m_lastwritefile_offset = 0;
    state->m_lastfile_offset = 0;
    state->m_windowagg_use = false;

    state->m_storeColumns.Init(1024 * 10);
    state->m_activeptr = 0;
    state->m_readptrcount = 1;
    state->m_readptrsize = 8; /* arbitrary */
    state->m_readptrs = (BSReadPointer*)palloc(state->m_readptrsize * sizeof(BSReadPointer));

    state->m_readptrs[0].eflags = eflags;
    state->m_readptrs[0].eof_reached = false;
    state->m_readptrs[0].file = 0;
    state->m_readptrs[0].offset = 0L;
    state->readMultiColumn = ReadMultiColumn;
    state->getlen = GetLen;
    state->m_addWidth = true;
    state->m_maxMem = maxMem * 1024L;
    state->m_spreadNum = 0;
    state->m_planId = planId;
    state->m_dop = dop;

    (void)MemoryContextSwitchTo(oldcontext);

    return state;
}

unsigned int GetLen(BatchStore* state, bool eofOK)
{
    unsigned int len = 0;
    size_t nbytes;

    nbytes = BufFileRead(state->m_myfile, (void*)&len, sizeof(len));
    if (nbytes == sizeof(len))
        return len;
    if (nbytes != 0)
        ereport(ERROR, (errmodule(MOD_EXECUTOR), errcode(ERRCODE_FILE_READ_FAILED), errmsg("unexpected end of tape")));

    if (!eofOK)
        ereport(ERROR, (errmodule(MOD_EXECUTOR), errcode(ERRCODE_FILE_READ_FAILED), errmsg("unexpected end of data")));

    return 0;
}

void BatchStore::WriteMultiColumn(MultiColumns* multiColumn)
{
    int colNum = m_colNum;

    int dataLen = multiColumn->GetLen(colNum);
    int len = dataLen + sizeof(len);

    char* data = GetData(colNum, multiColumn);

    if (BufFileWrite(m_myfile, (void*)&len, sizeof(len)) != sizeof(len))
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not write to batchstore temporary file: %m")));

    if (BufFileWrite(m_myfile, (void*)data, dataLen) != (size_t)dataLen)
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not write to batchstore temporary file: %m")));

    if (m_backward) /* need trailing length word? */
        if (BufFileWrite(m_myfile, (void*)&len, sizeof(len)) != sizeof(len))
            ereport(ERROR, (errcode_for_file_access(), errmsg("could not write to batchstore temporary file: %m")));

    FreeMem(GetMemoryChunkSpace(data));
    pfree_ext(data);

    FreeMultiColumn(multiColumn);
}

void ReadMultiColumn(BatchStore* state, MultiColumns& multiColumn, unsigned int len)
{
    unsigned int dataLen = len - sizeof(int);
    Assert(dataLen > 0);

    char* dataPtr = (char*)palloc(dataLen);

    state->UseMem(GetMemoryChunkSpace(dataPtr));

    if (BufFileRead(state->m_myfile, (void*)dataPtr, dataLen) != (size_t)dataLen)
        ereport(ERROR, (errmodule(MOD_EXECUTOR), errcode(ERRCODE_FILE_READ_FAILED), errmsg("unexpected end of data")));

    state->SetData(dataPtr, dataLen, &multiColumn);

    state->FreeMem(dataLen);
    pfree_ext(dataPtr);

    if (state->m_backward) /* need trailing length word? */
        if (BufFileRead(state->m_myfile, (void*)&len, sizeof(len)) != sizeof(len))
            ereport(
                ERROR, (errmodule(MOD_EXECUTOR), errcode(ERRCODE_FILE_READ_FAILED), errmsg("unexpected end of data")));
}

void batchstore_set_eflags(BatchStore* state, int eflags)
{
    int i;

    if (state->m_status != BSS_INMEM || state->m_storeColumns.m_memRowNum != 0)
        ereport(ERROR,
            (errmodule(MOD_EXECUTOR),
                errcode(ERRCODE_UNEXPECTED_NODE_STATE),
                errmsg("too late to call batchstore_set_eflags")));

    state->m_readptrs[0].eflags = eflags;
    if (eflags < 0)
            ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                            errmsg("number need be a positive number in bitwise operation")));
    for (i = 1; i < state->m_readptrcount; i++) {
        if (state->m_readptrs[i].eflags < 0)
                ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                                errmsg("number need be a positive number in bitwise operation")));
        eflags = (unsigned int)eflags | (unsigned int)state->m_readptrs[i].eflags;
    }
    state->m_eflags = eflags;
}

int batchstore_alloc_read_pointer(BatchStore* state, int eflags)
{
    /* Check for possible increase of requirements */
    if (state->m_status != BSS_INMEM || state->m_storeColumns.m_memRowNum != 0) {
        if ((int)((unsigned int)state->m_eflags | (unsigned int)eflags) != state->m_eflags)
            ereport(ERROR,
                (errmodule(MOD_EXECUTOR),
                    errcode(ERRCODE_UNEXPECTED_NODE_STATE),
                    errmsg("too late to require new batchstore eflags")));
    }

    /* Make room for another read pointer if needed */
    if (state->m_readptrcount >= state->m_readptrsize) {
        int newcnt = state->m_readptrsize * 2;

        state->m_readptrs = (BSReadPointer*)repalloc(state->m_readptrs, newcnt * sizeof(BSReadPointer));
        state->m_readptrsize = newcnt;
    }

    /* And set it up */
    state->m_readptrs[state->m_readptrcount] = state->m_readptrs[0];
    state->m_readptrs[state->m_readptrcount].eflags = eflags;

    if (eflags < 0)
            ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                            errmsg("number need be a positive number in bitwise operation")));
    if (state->m_eflags < 0)
            ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                            errmsg("number need be a positive number in bitwise operation")));
    state->m_eflags = (unsigned int)state->m_eflags | (unsigned int)eflags;

    return state->m_readptrcount++;
}

/*
 * @Description: get bacth value for memory
 * @in forward: the scan direction
 * @in batch: the batch to store value
 * @in batch_rows: rows to be fetched
 * @return - void
 */
void BatchStore::GetBatchInMemory(bool forward, VectorBatch* batch, int batch_rows)
{
    int i = 0;
    bool endFlag = false;
    BSReadPointer* readptr = &m_readptrs[m_activeptr];
    m_lastFetchCursor = m_curFetchCursor;
    for (i = 0; i < batch_rows; ++i) {
        if (forward) {
            if (m_curFetchCursor < m_storeColumns.m_memRowNum) {
                MultiColumns multiColumn = m_storeColumns.m_memValues[m_curFetchCursor];

                AppendBatch(batch, multiColumn, i, false);

                m_curFetchCursor++;

                continue;
            }
            m_eofReached = true;
            readptr->eof_reached = true;

            /*
             * Complain if caller tries to retrieve more tuples than
             * originally asked for in a bounded sort.	This is because
             * returning EOF here might be the wrong thing.
             */
            endFlag = true;
        }

        if (endFlag)
            break;
    }
}

void BatchStore::GetBatchWriteFile(bool forward, VectorBatch* batch)
{
    if (m_readptrs->eof_reached && forward)
        return;
    /*
     * Switch from writing to reading.
     */
    BufFileTell(m_myfile, &writepos_file, &writepos_offset);
    if (!m_readptrs->eof_reached)
        if (BufFileSeek(m_myfile, m_readptrs->file, m_readptrs->offset, SEEK_SET) != 0)
            ereport(
                ERROR, (errmodule(MOD_EXECUTOR), errcode(ERRCODE_FILE_READ_FAILED), errmsg("batchstore seek failed")));

    m_status = BSS_READFILE;
}

/*
 * @Description: get bacth value for temp file
 * @in forward: the scan direction
 * @in batch: the batch to store value
 * @in batch_rows: rows to be fetched
 * @return - void
 */
void BatchStore::GetBatchReadFile(bool forward, VectorBatch* batch, int batch_rows)
{
    unsigned int tuplen;
    int i = 0;
    BSReadPointer* readptr = &m_readptrs[m_activeptr];

    BufFileTell(m_myfile, &m_lastfile_num, &m_lastfile_offset);

    for (i = 0; i < batch_rows; i++) {

        if (forward) {
            if ((tuplen = getlen(this, true)) != 0) {
                MultiColumns multiColumn;
                readMultiColumn(this, multiColumn, tuplen);
                AppendBatch(batch, multiColumn, i);
                continue;
            } else {
                readptr->eof_reached = true;
                m_eofReached = true;
                break;
            }
        }
    }
}

/*
 * @Description: get bacth value for temp file or memory
 * @in forward: the scan direction
 * @in batch: the batch to store value
 * @in batch_rows: rows to be fetched
 * @return - void
 */
void BatchStore::GetBatch(bool forward, VectorBatch* batch, int batch_rows)
{
    switch (m_status) {
        case BSS_INMEM:
            GetBatchInMemory(forward, batch, batch_rows);
            break;
        case BSS_WRITEFILE:
            GetBatchWriteFile(forward, batch);
            /* fall through */
        case BSS_READFILE:
            GetBatchReadFile(forward, batch, batch_rows);
            break;
        default:
            ereport(ERROR,
                (errmodule(MOD_EXECUTOR), errcode(ERRCODE_UNEXPECTED_NODE_STATE), errmsg("invalid batchstore state")));
    }

    if (batch->m_rows > 0) {
        for (int i = 0; i < batch->m_cols; ++i) {
            batch->m_arr[i].m_desc = m_colInfo[i].m_desc;
        }
    }
}

void BatchStore::PutBatch(VectorBatch* batch)
{
    for (int i = 0; i < m_readptrcount; i++) {
        if (i != m_activeptr)
            m_readptrs[i].eof_reached = false;
    }

    switch (m_status) {
        case BSS_INMEM:
            m_lastbatchnum = m_storeColumns.m_memRowNum;
            break;
        case BSS_WRITEFILE:
        case BSS_READFILE:
            BufFileTell(m_myfile, &m_lastwritebatchnum, &m_lastwritefile_offset);
            break;
        default:
            break;
    }

    WaitState oldStatus = pgstat_report_waitstatus(STATE_EXEC_MATERIAL);
    for (int row = 0; row < batch->m_rows; ++row) {
        MultiColumns multiColumn = CopyMultiColumn<false>(batch, row);
        switch (m_status) {
            case BSS_INMEM:
                PutBatchToMemory(&multiColumn);
                break;
            case BSS_WRITEFILE:
                (void)pgstat_report_waitstatus(STATE_EXEC_MATERIAL_WRITE_FILE);
                PutBatchWriteFile(&multiColumn);
                break;
            case BSS_READFILE:
                PutBatchReadFile(&multiColumn);
                break;
            default:
                ereport(ERROR,
                    (errmodule(MOD_EXECUTOR),
                        errcode(ERRCODE_UNEXPECTED_NODE_STATE),
                        errmsg("invalid batchstore state")));
        }
    }
    (void)pgstat_report_waitstatus(oldStatus);
}

void BatchStore::PutBatchToMemory(MultiColumns* multiColumn)
{
    ResourceOwner oldowner;

    if (!HasFreeSlot() || m_availMem <= 0) {
        /* if mem is used up, then adjust capacity */
        if (m_availMem <= 0)
            m_storeColumns.m_capacity = m_storeColumns.m_memRowNum + 1;
        GrowMemValueSlots("VecMaterialize", m_planId, m_storecontext);
    }

    PutValue(*multiColumn);

    /*
     * Done if we still fit in available memory and have array slots.
     */
    if (m_storeColumns.m_memRowNum < m_storeColumns.m_capacity && !LackMem())
        return;

    if (m_storeColumns.m_memRowNum > 0) {
        m_colWidth /= m_storeColumns.m_memRowNum;
        m_addWidth = false;
    }
    if (LackMem()) {
        elog(LOG, "Vecmaterialize mem lack, workmem: %ldKB, availmem: %ldKB", m_allowedMem / 1024L, m_availMem / 1024L);
    }

    /*
     * Nope; time to switch to tape-based operation.  Make sure that
     * the temp file(s) are created in suitable temp tablespaces.
     */
    PrepareTempTablespaces();

    /* associate the file with the store's resource owner */
    oldowner = t_thrd.utils_cxt.CurrentResourceOwner;
    t_thrd.utils_cxt.CurrentResourceOwner = m_resowner;

    m_myfile = BufFileCreateTemp(m_interXact);

    t_thrd.utils_cxt.CurrentResourceOwner = oldowner;

    /*
     * Freeze the decision about whether trailing length words will be
     * used.  We can't change this choice once data is on tape, even
     * though callers might drop the requirement.
     */
    m_backward = (m_eflags & EXEC_FLAG_BACKWARD) != 0;
    m_status = BSS_WRITEFILE;
    DumpMultiColumn();
}

void BatchStore::PutBatchWriteFile(MultiColumns* multiColumn)
{
    WriteMultiColumn(multiColumn);
}
void BatchStore::PutBatchReadFile(MultiColumns* multiColumn)
{
    /*
     * Switch from reading to writing.
     */
    if (!m_readptrs[m_activeptr].eof_reached)
        BufFileTell(m_myfile, &m_readptrs[m_activeptr].file, &m_readptrs[m_activeptr].offset);
    if (BufFileSeek(m_myfile, writepos_file, writepos_offset, SEEK_SET) != 0)
        ereport(ERROR,
            (errmodule(MOD_EXECUTOR), errcode(ERRCODE_FILE_READ_FAILED), errmsg("batchstore seek to EOF failed")));

    m_status = BSS_WRITEFILE;

    WriteMultiColumn(multiColumn);
}
void BatchStore::DumpMultiColumn()
{
    int i;
    int j;
    BSReadPointer* readptr = m_readptrs;

    for (j = 0; j < m_readptrcount; readptr++, j++) {
        if (!readptr->eof_reached) {
            BufFileTell(m_myfile, &readptr->file, &readptr->offset);
        }
    }

    for (i = 0;; i++) {
        if (i == m_lastbatchnum)
            BufFileTell(m_myfile, &m_lastwritebatchnum, &m_lastwritefile_offset);
        if (i >= m_storeColumns.m_memRowNum)
            break;
        WriteMultiColumn(&m_storeColumns.m_memValues[i]);
    }

    m_storeColumns.m_memRowNum = 0;
}

void batchstore_putbatch(BatchStore* state, VectorBatch* batch)
{
    MemoryContext oldcxt = MemoryContextSwitchTo(state->m_storecontext);
    state->PutBatch(batch);
    MemoryContextSwitchTo(oldcxt);
}

bool batchstore_getbatch(BatchStore* state, bool forward, VectorBatch* batch)
{
    batch->Reset();
    state->GetBatch(forward, batch);

    if (BatchIsNull(batch))
        return false;
    else
        return true;
}

void batchstore_end(BatchStore* state)
{
    if (state->m_myfile)
        BufFileClose(state->m_myfile);

    // Free the per-batchstore memory context, thereby releasing all working memory,
    // including the BatchStore struct itself.
    //
    MemoryContextDelete(state->m_storecontext);
}

/*
 * tuplestore_rescan		- rewind the active read pointer to start
 */
void batchstore_rescan(BatchStore* state)
{
    BSReadPointer* readptr = &state->m_readptrs[state->m_activeptr];

    Assert(readptr->eflags & EXEC_FLAG_REWIND);

    state->m_lastFetchCursor = 0;
    state->m_curFetchCursor = 0;
    state->m_eofReached = false;

    switch (state->m_status) {
        case BSS_INMEM:
            readptr->eof_reached = false;
            state->m_markposOffset = 0;
            break;
        case BSS_WRITEFILE:
            readptr->eof_reached = false;
            readptr->file = 0;
            readptr->offset = 0L;
            break;
        case BSS_READFILE:
            readptr->eof_reached = false;
            if (BufFileSeek(state->m_myfile, 0, 0L, SEEK_SET) != 0)
                ereport(ERROR,
                    (errmodule(MOD_EXECUTOR),
                        errcode(ERRCODE_FILE_READ_FAILED),
                        errmsg("batchstore seek to start failed")));
            break;
        default:
            ereport(ERROR,
                (errmodule(MOD_EXECUTOR), errcode(ERRCODE_UNEXPECTED_NODE_STATE), errmsg("invalid batchstore state")));
    }
}

/*
 * batchstore_trim	- remove all no-longer-needed tuples
 *
 * Calling this function authorizes the tuplestore to delete all tuples
 * before the oldest read pointer, if no read pointer is marked as requiring
 * REWIND capability.
 *
 * Note: this is obviously safe if no pointer has BACKWARD capability either.
 * If a pointer is marked as BACKWARD but not REWIND capable, it means that
 * the pointer can be moved backward but not before the oldest other read
 * pointer.
 */
void batchstore_trim(BatchStore* state, bool from_memory)
{
    int nremove;
    int i;
    errno_t rc;

    /*
     * Truncation is disallowed if any read pointer requires rewind
     * capability.
     */
    if (state->m_eflags & EXEC_FLAG_REWIND)
        return;

    /*
     * We don't bother trimming temp files since it usually would mean more
     * work than just letting them sit in kernel buffers until they age out.
     */
    if (state->m_status != BSS_INMEM)
        return;

    if (state->m_windowagg_use) {
        /*
         *  If batchstore used in windowagg,  delete tuples that have been read and used
         */
        if (state->m_curFetchCursor == state->m_storeColumns.m_memRowNum)
            nremove = 0;
        else
            nremove = state->m_curFetchCursor;
    } else {
        if (from_memory)
            nremove = state->m_lastFetchCursor;
        else
            nremove = state->m_lastbatchnum;
    }

    if (nremove <= 0) {
        return;
    }        

    /* Release no-longer-needed tuples */
    for (i = 0; i < nremove; i++) {
        state->FreeMultiColumn(&state->m_storeColumns.m_memValues[i]);
    }

    /*
     * Slide the array down and readjust pointers.
     *
     * In mergejoin's current usage, it's demonstrable that there will always
     * be exactly one non-removed tuple; so optimize that case.
     */
    if (nremove + 1 == state->m_storeColumns.m_memRowNum)
        state->m_storeColumns.m_memValues[0] = state->m_storeColumns.m_memValues[nremove];
    else {
        rc = memmove_s(state->m_storeColumns.m_memValues,
            (state->m_storeColumns.m_memRowNum - nremove) * sizeof(MultiColumns),
            state->m_storeColumns.m_memValues + nremove,
            (state->m_storeColumns.m_memRowNum - nremove) * sizeof(MultiColumns));
        securec_check(rc, "\0", "\0");
    }

    state->m_storeColumns.m_memRowNum -= nremove;
    state->m_curFetchCursor -= nremove;

    /* make sure  state->m_curFetchCursor is always >= 0 */
    if (state->m_curFetchCursor < 0)
        state->m_curFetchCursor = 0;
    state->m_lastFetchCursor = 0;
    state->m_lastbatchnum -= nremove;
}

/*
 * tuplestore_ateof
 *
 * Returns the active read pointer's eof_reached state.
 */
bool batchstore_ateof(BatchStore* state)
{
    return state->m_readptrs[state->m_activeptr].eof_reached;
}
void batchstore_restrpos(BatchStore* state)
{
    BSReadPointer* sptr = &state->m_readptrs[0];
    BSReadPointer* dptr = &state->m_readptrs[1];
    int file_number;
    off_t offset;

    *sptr = *dptr;

    switch (state->m_status) {
        case BSS_INMEM:
            state->m_curFetchCursor = 0;
            break;
        case BSS_WRITEFILE:
            if (sptr->eof_reached) {
                BufFileTell(state->m_myfile, &file_number, &offset);

                if (sptr->file <= file_number && sptr->offset <= offset)
                    sptr->eof_reached = false;
            }
            break;
        case BSS_READFILE:
            if (BufFileSeek(state->m_myfile, sptr->file, sptr->offset, SEEK_SET) != 0)
                ereport(ERROR,
                    (errmodule(MOD_EXECUTOR), errcode(ERRCODE_FILE_READ_FAILED), errmsg("batchstore seek failed")));
            break;
        default:
            ereport(ERROR,
                (errmodule(MOD_EXECUTOR), errcode(ERRCODE_UNEXPECTED_NODE_STATE), errmsg("invalid batchstore state")));
    }
}

void batchstore_markpos(BatchStore* state, bool from_mem)
{
    BSReadPointer* sptr = &state->m_readptrs[0];
    BSReadPointer* dptr = &state->m_readptrs[1];

    *dptr = *sptr;

    switch (state->m_status) {
        case BSS_INMEM:
            batchstore_trim(state, from_mem);
            break;
        case BSS_WRITEFILE:
            Assert(!from_mem);
            dptr->file = state->m_lastwritebatchnum;
            dptr->offset = state->m_lastwritefile_offset;
            *sptr = *dptr;
            break;
        case BSS_READFILE:
            Assert(!dptr->eof_reached || from_mem);
            dptr->file = state->m_lastfile_num;
            dptr->offset = state->m_lastfile_offset;
            *sptr = *dptr;
            break;
        default:
            ereport(ERROR,
                (errmodule(MOD_EXECUTOR), errcode(ERRCODE_UNEXPECTED_NODE_STATE), errmsg("invalid batchstore state")));
    }
}
