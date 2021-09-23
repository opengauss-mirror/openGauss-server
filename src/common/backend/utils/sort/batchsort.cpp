/* -------------------------------------------------------------------------
 *
 * batchsort.cpp
 *	  Generalized vector batch based sorting routines.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2011, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *    src/common/backend/utils/sort/batchsort.cpp
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
#include "instruments/instr_unique_sql.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/syscache.h"
#include "utils/lsyscache.h"
#include "commands/tablespace.h"
#include "catalog/pg_type.h"

#include "pgxc/execRemote.h"
#include "utils/datum.h"
#include "vecexecutor/vecvar.h"
#include "vecexecutor/vectorbatch.h"
#include "utils/builtins.h"
#include "utils/batchsort.h"
#include "utils/numeric.h"
#include "utils/numeric_gs.h"
#include "access/tuptoaster.h"

typedef int (*LLVM_CMC_func)(const MultiColumns* a, const MultiColumns* b, Batchsortstate* state);

const int MINORDER = 6;
const int TAPE_BUFFER_OVERHEAD = (BLCKSZ * 3);
const int MERGE_BUFFER_SIZE = (BLCKSZ * 32);
extern void CopyDataRowToBatch(RemoteQueryState* node, VectorBatch* batch);

template <bool abbreSortOptimize>
int CompareMultiColumn(const MultiColumns* a, const MultiColumns* b, Batchsortstate* state);

void WriteMultiColumn(Batchsortstate* state, int tapeNum, MultiColumns* multiColumn);

void ReadMultiColumn(Batchsortstate* state, MultiColumns& multiColumn, int tapeNum, unsigned int len);

unsigned int GetLen(Batchsortstate* state, int tapenum, bool eofOK);

unsigned int GetLenFromDataNode(Batchsortstate* state, int tapenum, bool eofOK);

unsigned int StreamGetLenFromDataNode(Batchsortstate* state, int tapenum, bool eofOK);

void ReadFromDataNode(Batchsortstate* state, MultiColumns& multiColumn, int tapeNum, unsigned int len);

Datum CmpNumericFunc(PG_FUNCTION_ARGS);

int CompareIntMutiColumn(const MultiColumns* a, const MultiColumns* b, Batchsortstate* state);

Batchsortstate* batchsort_begin_heap(TupleDesc tupDesc, int nkeys, AttrNumber* attNums, Oid* sortOperators,
    Oid* sortCollations, const bool* nullsFirstFlags, int64 workMem, bool randomAccess, int64 maxMem, int planId,
    int dop)
{
    Batchsortstate* state = NULL;
    MemoryContext oldcontext, sortcontext;

    // Default unlimited sort memory.
    if (workMem == 0)
        workMem = 0x0ffffffffffffff / 1024;

    /*
     * Make the Bathsortstate within the per-sort context.  This way, we
     * don't need a separate pfree_ext() operation for it at shutdown.
     */
    sortcontext = AllocSetContextCreate(CurrentMemoryContext,
        "BatchSort",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE,
        STANDARD_CONTEXT,
        workMem * 1024L);

    oldcontext = MemoryContextSwitchTo(sortcontext);

    state = (Batchsortstate*)palloc0(sizeof(Batchsortstate));
    state->sortcontext = sortcontext;
    state->tupDesc = tupDesc;
    state->m_colNum = tupDesc->natts;

    state->InitCommon(workMem, randomAccess);

    AssertArg(nkeys > 0);

#ifdef TRACE_SORT
    if (u_sess->attr.attr_common.trace_sort) {
        elog(LOG,
            "begin tuple sort: nkeys = %d, workMem = %ld, randomAccess = %c, maxMem = %ld",
            nkeys,
            workMem,
            randomAccess ? 't' : 'f',
            maxMem);
    }
#endif

    state->m_nKeys = nkeys;
    state->m_scanKeys = (ScanKey)palloc0(nkeys * sizeof(ScanKeyData));
    state->m_isSortKey = (bool*)palloc0(tupDesc->natts * sizeof(bool));

    for (int i = 0; i < nkeys; ++i) {
        Oid sortFunction;
        bool reverse = false;
        uint32 flags;

        AssertArg(attNums[i] != 0);
        AssertArg(sortOperators[i] != 0);

        if (!get_compare_function_for_ordering_op(sortOperators[i], &sortFunction, &reverse))
            ereport(ERROR,
                (errmodule(MOD_EXECUTOR),
                    errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                    errmsg("operator %u is not a valid ordering operator", sortOperators[i])));

        /* We use btree's conventions for encoding directionality */
        flags = 0;
        if (reverse)
            flags |= SK_BT_DESC;
        if (nullsFirstFlags[i])
            flags |= SK_BT_NULLS_FIRST;

        /*
         * We needn't fill in sk_strategy or sk_subtype since these scankeys
         * will never be passed to an index.
         */
        ScanKeyEntryInitialize(&state->m_scanKeys[i],
            flags,
            attNums[i],
            InvalidStrategy,
            InvalidOid,
            sortCollations[i],
            sortFunction,
            (Datum)0);

        state->m_isSortKey[attNums[i] - 1] = true;
    }

    state->abbrevNext = 10;
    /* Prepare SortSupport data for the first column */
    state->sortKeys = (SortSupport)palloc0(sizeof(SortSupportData));

    // initialize SortSupport:sortkey
    AssertArg(attNums[0] != 0);
    AssertArg(sortOperators[0] != 0);

    SortSupport sortKey = state->sortKeys;
    sortKey->ssup_cxt = CurrentMemoryContext;
    sortKey->ssup_collation = sortCollations[0];
    sortKey->ssup_nulls_first = nullsFirstFlags[0];
    sortKey->ssup_attno = attNums[0];
    /* Convey if abbreviation optimization is applicable in principle */
    sortKey->abbreviate = true;

    PrepareSortSupportFromOrderingOp(sortOperators[0], sortKey);

    state->m_connDNBatch = NULL;

    /*
     * If the first sort column doesn't satisfy optimized condition,
     * disable abbreviation here.
     */
    if (state->sortKeys[0].abbrev_converter == NULL) {
        state->compareMultiColumn = CompareMultiColumn<false>;
        state->sort_putbatch = batchsort_putbatch<false>;
    } else {
        state->compareMultiColumn = CompareMultiColumn<true>;
        state->sort_putbatch = batchsort_putbatch<true>;
    }
    state->writeMultiColumn = WriteMultiColumn;
    state->readMultiColumn = ReadMultiColumn;
    state->getlen = GetLen;
    state->m_addWidth = true;
    state->m_maxMem = maxMem * 1024L;
    state->m_spreadNum = 0;
    state->m_planId = planId;
    state->m_dop = dop;
    state->peakMemorySize = 0;

    (void)MemoryContextSwitchTo(oldcontext);

    return state;
}

#ifdef PGXC

/*
 * Tuples are coming from source where they are already sorted.
 * It is pretty much like sorting heap tuples but no need to load sorter.
 * Sorter initial status is final merge, and correct readtup and getlen
 * callbacks should be passed in.
 * Usage pattern of the merge sorter
 * batchsort_begin_merge
 * while (tuple = batchsort_getbatch())
 * {
 *     // process
 * }
 * batchsort_end_merge
 */
Batchsortstate* batchsort_begin_merge(TupleDesc tupDesc, int nkeys, AttrNumber* attNums, Oid* sortOperators,
    Oid* sortCollations, const bool* nullsFirstFlags, void* combiner, int64 workMem)
{
    Batchsortstate* state = NULL;
    MemoryContext oldcontext, sortcontext;

    // Default unlimited sort memory.
    if (workMem == 0)
        workMem = 0x0ffffffffffffff / 1024;

    // Make the Bathsortstate within the per-sort context.  This way, we
    // don't need a separate pfree_ext() operation for it at shutdown.
    //
    sortcontext = AllocSetContextCreate(CurrentMemoryContext,
        "BatchSort",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE,
        STANDARD_CONTEXT,
        workMem * 1024L);

    oldcontext = MemoryContextSwitchTo(sortcontext);
    state = (Batchsortstate*)palloc0(sizeof(Batchsortstate));
    state->sortcontext = sortcontext;

    state->InitCommon(workMem, false);

#ifdef TRACE_SORT
    if (u_sess->attr.attr_common.trace_sort)
        pg_rusage_init(&state->m_ruStart);
#endif

    AssertArg(nkeys > 0);
    AssertArg(combiner);

#ifdef TRACE_SORT
    if (u_sess->attr.attr_common.trace_sort) {
        elog(LOG, "begin merge sort: nkeys = %d, workMem = %ld", nkeys, workMem);
    }
#endif

    state->m_nKeys = nkeys;
    TRACE_POSTGRESQL_SORT_START(MERGE_SORT, false, nkeys, workMem, false);

    state->m_nKeys = nkeys;
    state->m_colNum = tupDesc->natts;

    state->tupDesc = tupDesc;
    state->m_scanKeys = (ScanKey)palloc0(nkeys * sizeof(ScanKeyData));
    state->m_isSortKey = (bool*)palloc0(tupDesc->natts * sizeof(bool));

    for (int i = 0; i < nkeys; ++i) {
        Oid sortFunction;
        bool reverse = false;
        uint32 flags;

        AssertArg(attNums[i] != 0);
        AssertArg(sortOperators[i] != 0);

        if (!get_compare_function_for_ordering_op(sortOperators[i], &sortFunction, &reverse))
            ereport(ERROR,
                (errmodule(MOD_EXECUTOR),
                    errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                    errmsg("operator %u is not a valid ordering operator", sortOperators[i])));

        /* We use btree's conventions for encoding directionality */
        flags = 0;
        if (reverse)
            flags |= SK_BT_DESC;
        if (nullsFirstFlags[i])
            flags |= SK_BT_NULLS_FIRST;

        /*
         * We needn't fill in sk_strategy or sk_subtype since these scankeys
         * will never be passed to an index.
         */
        ScanKeyEntryInitialize(&state->m_scanKeys[i],
            flags,
            attNums[i],
            InvalidStrategy,
            InvalidOid,
            sortCollations[i],
            sortFunction,
            (Datum)0);

        state->m_isSortKey[attNums[i] - 1] = true;
    }

    int conn_count = 0;

    if (IsA(combiner, VecRemoteQueryState)) {
        state->combiner = (RemoteQueryState*)combiner;
        state->writeMultiColumn = NULL;
        state->readMultiColumn = ReadFromDataNode;
        state->getlen = GetLenFromDataNode;
        conn_count = ((RemoteQueryState*)combiner)->conn_count;
    } else {
        Assert(IsA(combiner, VecStreamState));
        state->streamstate = (VecStreamState*)combiner;
        state->writeMultiColumn = NULL;
        state->readMultiColumn = ReadFromDataNode;
        state->getlen = StreamGetLenFromDataNode;
        conn_count = ((VecStreamState*)combiner)->conn_count;
    }

    /*
     * If there are multiple runs to be merged, when we go to read back
     * tuples from disk, abbreviated keys will not have been stored, and
     * we don't care to regenerate them.  Disable abbreviation from this
     * point on. Reset compare and putbatch function.
     */
    if (state->jitted_CompareMultiColumn)
        state->compareMultiColumn = ((LLVM_CMC_func)(state->jitted_CompareMultiColumn));
    else
        state->compareMultiColumn = CompareMultiColumn<false>;
    state->sort_putbatch = batchsort_putbatch<false>;
    /*
     * logical tape in this case is a sorted stream
     */
    state->m_datanodeFetchCursor = (int*)palloc0(conn_count * sizeof(int));

    state->m_maxTapes = conn_count;
    state->m_tapeRange = conn_count;

    state->m_mergeActive = (bool*)palloc0(conn_count * sizeof(bool));
    state->m_mergeNext = (int*)palloc0(conn_count * sizeof(int));
    state->m_mergeLast = (int*)palloc0(conn_count * sizeof(int));
    state->m_mergeAvailslots = (int*)palloc0(conn_count * sizeof(int));
    state->m_mergeAvailmem = (long*)palloc0(conn_count * sizeof(long));

    state->m_tpRuns = (int*)palloc0(conn_count * sizeof(int));
    state->m_tpDummy = (int*)palloc0(conn_count * sizeof(int));
    state->m_tpNum = (int*)palloc0(conn_count * sizeof(int));

    state->m_connDNBatch = (VectorBatch**)palloc(conn_count * sizeof(VectorBatch*));
    /* mark each stream (tape) has one run */
    for (int i = 0; i < conn_count; i++) {
        state->m_tpRuns[i] = 1;
        state->m_tpNum[i] = i;

        state->m_connDNBatch[i] = New(CurrentMemoryContext) VectorBatch(CurrentMemoryContext, tupDesc);
        state->m_connDNBatch[i]->Reset();
    }

    state->BeginMerge();
    state->m_status = BS_FINALMERGE;
    state->BindingGetBatchFun();
    state->peakMemorySize = 0;

    MemoryContextSwitchTo(oldcontext);

    return state;
}
#endif

/*
 * batchsort_set_bound
 *
 *	Advise batchsort that at most the first N result tuples are required.
 *
 * Must be called before inserting any tuples.	(Actually, we could allow it
 * as long as the sort hasn't spilled to disk, but there seems no need for
 * delayed calls at the moment.)
 *
 * This is a hint only. The batchsort may still return more tuples than
 * requested.
 */
void batchsort_set_bound(Batchsortstate* state, int64 bound)
{
#ifdef DEBUG_BOUNDED_SORT
    /* Honor GUC setting that disables the feature (for easy testing) */
    if (!u_sess->attr.attr_sql.optimize_bounded_sort)
        return;
#endif

    if (bound > (int64)(INT_MAX / 2))
        return;

    state->m_bounded = true;
    state->m_bound = (int)bound;
    /* restore the compare and putbatch function */
    if (state->jitted_CompareMultiColumn)
        state->compareMultiColumn = ((LLVM_CMC_func)(state->jitted_CompareMultiColumn));
    else
        state->compareMultiColumn = CompareMultiColumn<false>;
    state->sort_putbatch = batchsort_putbatch<false>;
    /*
     * Bounded sorts are not an effective target for abbreviated key
     * optimization.  Disable by setting state to be consistent with no
     * abbreviation support.
     */
    state->sortKeys->abbrev_converter = NULL;
    if (state->sortKeys->abbrev_full_comparator)
        state->sortKeys->comparator = state->sortKeys->abbrev_full_comparator;

    /* Not strictly necessary, but be tidy */
    state->sortKeys->abbrev_abort = NULL;
    state->sortKeys->abbrev_full_comparator = NULL;
}

/*
 * batchsort_end
 *
 *	Release resources and clean up.
 *
 * NOTE: after calling this, any pointers returned by batchsort_getXXX are
 * pointing to garbage.  Be careful not to attempt to use or free such
 * pointers afterwards!
 */
void batchsort_end(Batchsortstate* state)
{
    /* context swap probably not needed, but let's be safe */
    MemoryContext oldcontext = MemoryContextSwitchTo(state->sortcontext);

#ifdef TRACE_SORT
    long spaceUsed;

    if (state->m_tapeset)
        spaceUsed = LogicalTapeSetBlocks(state->m_tapeset);
    else
        spaceUsed = (state->m_allowedMem - state->m_availMem + 1023) / 1024;
#endif

    /*
     * Delete temporary "tape" files, if any.
     *
     * Note: want to include this in reported total cost of sort, hence need
     * for two #ifdef TRACE_SORT sections.
     */
    if (state->m_tapeset)
        LogicalTapeSetClose(state->m_tapeset);

#ifdef TRACE_SORT
    if (u_sess->attr.attr_common.trace_sort) {
        if (state->m_tapeset) {
            elog(LOG, "external sort ended, %ld disk blocks used: %s", spaceUsed, pg_rusage_show(&state->m_ruStart));
        } else {
            elog(LOG, "internal sort ended, %ld KB used: %s", spaceUsed, pg_rusage_show(&state->m_ruStart));
        }
    }

    TRACE_POSTGRESQL_SORT_DONE(state->m_tapeset != NULL, spaceUsed);
#else

    /*
     * If you disabled TRACE_SORT, you can still probe sort__done, but you
     * ain't getting space-used stats.
     */
    TRACE_POSTGRESQL_SORT_DONE(state->m_tapeset != NULL, 0L);
#endif

    MemoryContextSwitchTo(oldcontext);

    /*
     * Free the per-sort memory context, thereby releasing all working memory,
     * including the Batchsortstate struct itself.
     */
    MemoryContextDelete(state->sortcontext);
}

/*
 * All batches have been provided; finish the sort.
 */
void batchsort_performsort(Batchsortstate* state)
{
    MemoryContext oldcontext = MemoryContextSwitchTo(state->sortcontext);

#ifdef TRACE_SORT
    if (u_sess->attr.attr_common.trace_sort) {
        elog(LOG, "performsort starting: %s", pg_rusage_show(&state->m_ruStart));
    }
#endif

    switch (state->m_status) {
        case BS_INITIAL:

            if (state->m_storeColumns.m_memRowNum > 0) {
                state->m_colWidth /= state->m_storeColumns.m_memRowNum;
                state->m_addWidth = false;
            }
            state->SortInMem();

            state->m_curFetchCursor = 0;
            state->m_eofReached = false;
            state->m_markposOffset = 0;
            state->m_markposEof = false;
            state->m_status = BS_SORTEDINMEM;
            break;

        case BS_BOUNDED:

            /*
             * We were able to accumulate all the tuples required for output
             * in memory, using a heap to eliminate excess tuples.	Now we
             * have to transform the heap to a properly-sorted array.
             */
            state->SortBoundedHeap();
            state->m_curFetchCursor = 0;
            state->m_eofReached = false;
            state->m_markposOffset = 0;
            state->m_markposEof = false;
            state->m_status = BS_SORTEDINMEM;
            break;

        case BS_BUILDRUNS:

            /*
             * Finish tape-based sort.	First, flush all tuples remaining in
             * memory out to tape; then merge until we have a single remaining
             * run (or, if !randomAccess, one run per tape). Note that
             * mergeruns sets the correct state->status.
             */
            state->DumpMultiColumn(true);
            state->MergeRuns();

            state->m_eofReached = false;
            state->m_markposBlock = 0L;
            state->m_markposOffset = 0;
            state->m_markposEof = false;

            /* increase current session spill count */
            pgstat_increase_session_spill();

            break;

        default:
            ereport(ERROR,
                (errmodule(MOD_EXECUTOR), errcode(ERRCODE_UNEXPECTED_NODE_STATE), errmsg("invalid BatchSort state")));
            break;
    }

#ifdef TRACE_SORT
    if (u_sess->attr.attr_common.trace_sort) {
        elog(LOG, "performsort done: %s", pg_rusage_show(&state->m_ruStart));
    }
#endif

    MemoryContextSwitchTo(oldcontext);
    state->BindingGetBatchFun();
}

/*
 * Fetch the next batch in either forward or back direction.
 * If successful, put batch.
 */
void batchsort_getbatch(Batchsortstate* state, bool forward, VectorBatch* batch)
{
    batch->Reset();

    MemoryContext oldcontext = MemoryContextSwitchTo(state->sortcontext);

    state->GetBatch(forward, batch);

    MemoryContextSwitchTo(oldcontext);
}

/*
 * batchsort_rescan		- rewind and replay the scan
 */
void batchsort_rescan(Batchsortstate* state)
{
    MemoryContext oldcontext = MemoryContextSwitchTo(state->sortcontext);

    Assert(state->m_randomAccess);

    switch (state->m_status) {
        case BS_SORTEDINMEM:
            state->m_curFetchCursor = 0;
            state->m_eofReached = false;
            state->m_markposOffset = 0;
            state->m_markposEof = false;
            break;
        case BS_SORTEDONTAPE:
            LogicalTapeRewindForRead(state->m_tapeset, state->m_resultTape, BLCKSZ);
            state->m_eofReached = false;
            state->m_markposBlock = 0L;
            state->m_markposOffset = 0;
            state->m_markposEof = false;
            break;
        default:
            ereport(ERROR,
                (errmodule(MOD_EXECUTOR), errcode(ERRCODE_UNEXPECTED_NODE_STATE), errmsg("invalid batchsort state")));
            break;
    }

    MemoryContextSwitchTo(oldcontext);
}

/*
 * batchsort_markpos	- saves last scan position in the merged sort file
 * caller expect us to return the same batch (thus it can retrieve next row).
 */
void batchsort_markpos(Batchsortstate* state)
{
    MemoryContext oldcontext = MemoryContextSwitchTo(state->sortcontext);

    switch (state->m_status) {
        case BS_SORTEDINMEM:
            state->m_markposOffset = state->m_lastFetchCursor;
            state->m_markposEof = state->m_eofReached;
            break;
        case BS_SORTEDONTAPE:
            state->m_markposBlock = state->m_lastBlock;
            state->m_markposOffset = state->m_lastOffset;
            state->m_markposEof = state->m_eofReached;
            break;
        default:
            ereport(ERROR,
                (errmodule(MOD_EXECUTOR), errcode(ERRCODE_UNEXPECTED_NODE_STATE), errmsg("invalid batchsort state")));
            break;
    }

    MemoryContextSwitchTo(oldcontext);
}

/*
 * batchsort_restorepos - restores current position in merged sort file to
 *						  last saved position
 */
void batchsort_restorepos(Batchsortstate* state)
{
    MemoryContext oldcontext = MemoryContextSwitchTo(state->sortcontext);

    long m_curBlock = 0;
    int m_curOffset = 0;

    switch (state->m_status) {
        case BS_SORTEDINMEM:
            state->m_curFetchCursor = state->m_markposOffset;
            state->m_eofReached = state->m_markposEof;
            break;
        case BS_SORTEDONTAPE:

            if (state->m_markposEof) {
                LogicalTapeTell(state->m_tapeset, state->m_resultTape, &m_curBlock, &m_curOffset);

                if (state->m_markposBlock < m_curBlock || state->m_markposOffset < m_curOffset)
                    state->m_eofReached = false;
                else
                    state->m_eofReached = true;
            } else
                state->m_eofReached = false;

            LogicalTapeSeek(state->m_tapeset, state->m_resultTape, state->m_markposBlock, state->m_markposOffset);
            break;
        default:
            ereport(ERROR,
                (errmodule(MOD_EXECUTOR), errcode(ERRCODE_UNEXPECTED_NODE_STATE), errmsg("invalid batchsort state")));
            break;
    }

    MemoryContextSwitchTo(oldcontext);
}

/*
 * batchsort_get_stats - extract summary statistics
 *
 * This can be called after batchsort_performsort() finishes to obtain
 * printable summary information about how the sort was performed.
 * spaceUsed is measured in kilobytes.
 */
void batchsort_get_stats(Batchsortstate* state, int* sortMethodId, int* spaceTypeId, long* spaceUsed)
{
    if (state->m_tapeset) {
        *spaceTypeId = SORT_IN_DISK;
        *spaceUsed = LogicalTapeSetBlocks(state->m_tapeset) * (BLCKSZ / 1024);
    } else {
        *spaceTypeId = SORT_IN_MEMORY;
        *spaceUsed = (state->m_allowedMem - state->m_availMem + 1023) / 1024;
    }

    switch (state->m_status) {
        case BS_SORTEDINMEM:
            if (state->m_boundUsed)
                *sortMethodId = (int)HEAPSORT;
            else
                *sortMethodId = (int)QUICKSORT;
            break;
        case BS_SORTEDONTAPE:
            *sortMethodId = (int)EXTERNALSORT;
            break;
        case BS_FINALMERGE:
            *sortMethodId = (int)EXTERNALMERGE;
            break;
        default:
            *sortMethodId = (int)STILLINPROGRESS;
            break;
    }
}

void Batchsortstate::InitCommon(int64 workMem, bool randomAccess)
{
    m_status = BS_INITIAL;
    m_allowedMem = workMem * 1024L - 1;
    m_bounded = false;
    m_boundUsed = false;
    m_availMem = m_allowedMem;
    m_lastFetchCursor = 0;
    m_curFetchCursor = 0;
    m_lastBlock = 0;
    m_lastOffset = 0;
    m_colInfo = NULL;
    m_tapeset = NULL;
    m_resultTape = -1;
    m_randomAccess = randomAccess;

    if (t_thrd.utils_cxt.SortColumnOptimize)
        m_unsortColumns.Init(1024 * 10);

    m_storeColumns.Init(1024 * 10);
    UseMem(GetMemoryChunkSpace(&m_storeColumns));

    if (LackMem())
        ereport(ERROR,
            (errmodule(MOD_EXECUTOR),
                errcode(ERRCODE_INSUFFICIENT_RESOURCES),
                errmsg("insufficient memory allowed for sort, allowed memory is %ld byte, "
                       "available memory is %ld byte",
                    m_allowedMem,
                    m_availMem)));
}

bool Batchsortstate::ConsiderAbortCommon()
{
    Assert(sortKeys[0].abbrev_converter != NULL);
    Assert(sortKeys[0].abbrev_abort != NULL);
    Assert(sortKeys[0].abbrev_full_comparator != NULL);

    /*
     * Check effectiveness of abbreviation optimization.  Consider aborting
     * when still within memory limit.
     */
    if (m_status == BS_INITIAL && m_storeColumns.m_memRowNum >= abbrevNext) {
        abbrevNext *= 2;

        /*
         * Check opclass-supplied abbreviation abort routine.  It may
         * indicate that abbreviation should not proceed.
         */
        if (!sortKeys->abbrev_abort(m_storeColumns.m_memRowNum, sortKeys))
            return false;

        /*
         * Finally, restore authoritative comparator, and indicate that
         * abbreviation is not in play by setting abbrev_converter to NULL
         */
        sortKeys[0].comparator = sortKeys[0].abbrev_full_comparator;
        sortKeys[0].abbrev_converter = NULL;
        /* Not strictly necessary, but be tidy */
        sortKeys[0].abbrev_abort = NULL;
        sortKeys[0].abbrev_full_comparator = NULL;
        /* restore the compare and putbatch function */
        if (jitted_CompareMultiColumn)
            compareMultiColumn = ((LLVM_CMC_func)(jitted_CompareMultiColumn));
        else
            compareMultiColumn = CompareMultiColumn<false>;
        sort_putbatch = batchsort_putbatch<false>;

        /* Give up - expect original pass-by-value representation */
        return true;
    }

    return false;
}

void Batchsortstate::SortInMem()
{
    if (m_storeColumns.m_memRowNum > 1) {
        qsort_arg(m_storeColumns.m_memValues,
            m_storeColumns.m_memRowNum,
            sizeof(MultiColumns),
            (qsort_arg_comparator)compareMultiColumn,
            (void*)this);
    }
}

void Batchsortstate::GetBatchInMemory(bool forward, VectorBatch* batch)
{
    int i = 0;

    bool endFlag = false;
    m_lastFetchCursor = m_curFetchCursor;
    for (i = 0; i < BatchMaxSize; ++i) {
        Assert(forward || m_randomAccess);

        if (forward) {
            if (m_curFetchCursor < m_storeColumns.m_memRowNum) {
                MultiColumns multiColumn = m_storeColumns.m_memValues[m_curFetchCursor];

                AppendBatch(batch, multiColumn, i, false);

                m_curFetchCursor++;

                continue;
            }
            m_eofReached = true;

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

void Batchsortstate::GetBatchDisk(bool forward, VectorBatch* batch)
{
    unsigned int tuplen;
    int i = 0;

    bool endFlag = false;
    m_lastFetchCursor = m_curFetchCursor;

    LogicalTapeTell(m_tapeset, m_resultTape, &m_lastBlock, &m_lastOffset);

    for (i = 0; i < BatchMaxSize; i++) {
        Assert(forward || m_randomAccess);

        if (forward) {
            if (m_eofReached) {
                endFlag = true;
                break;
            }
            if ((tuplen = getlen(this, m_resultTape, true)) != 0) {
                MultiColumns multiColumn;
                readMultiColumn(this, multiColumn, m_resultTape, tuplen);
                AppendBatch(batch, multiColumn, i);
                continue;
            } else {
                m_eofReached = true;
                endFlag = true;
                break;
            }
        }
        tuplen = getlen(this, m_resultTape, false);
        /*
         * Now we have the length of the prior tuple, back up and read it.
         * Note: READTUP expects we are positioned after the initial
         * length word of the tuple, so back up to that point.
         */
        if (!LogicalTapeBackspace(m_tapeset, m_resultTape, tuplen))
            ereport(ERROR,
                (errmodule(MOD_EXECUTOR),
                    errcode(ERRCODE_FILE_READ_FAILED),
                    errmsg("bogus tuple length in backward scan")));

        MultiColumns multiColumn;
        readMultiColumn(this, multiColumn, m_resultTape, tuplen);
        AppendBatch(batch, multiColumn, i);

        if (endFlag)
            break;
    }
}

void Batchsortstate::GetBatchFinalMerge(bool forward, VectorBatch* batch)
{
    int i = 0;

    m_lastFetchCursor = m_curFetchCursor;
    for (i = 0; i < BatchMaxSize; ++i) {
        Assert(forward);

        /*
         * This code should match the inner loop of mergeonerun().
         */
        if (m_storeColumns.m_memRowNum > 0) {
            int srcTape = m_storeColumns.m_memValues[0].idx;
            Size tuplen;
            int idx;
            MultiColumns multiColumn;
            MultiColumns* pNewMultiColumn = NULL;

            multiColumn = m_storeColumns.m_memValues[0];

            /* returned tuple is no longer counted in our memory space */
            if (multiColumn.m_values) {
                tuplen = GetMemoryChunkSpace(multiColumn.m_values);
                m_availMem += tuplen;
                m_mergeAvailmem[srcTape] += tuplen;
            }
            BatchSortHeapSiftup<false>();

            if ((idx = m_mergeNext[srcTape]) == 0) {
                /*
                 * out of preloaded data on this tape, try to read more
                 *
                 * Unlike mergeonerun(), we only preload from the single
                 * tape that's run dry.  See mergepreread() comments.
                 */
                MergePreReadDone(srcTape);

                /*
                 * if still no data, we've reached end of run on this tape
                 */
                if ((idx = m_mergeNext[srcTape]) == 0) {
                    AppendBatch(batch, multiColumn, i);
                    continue;
                }
            }
            /* pull next preread tuple from list, insert in heap */
            pNewMultiColumn = &m_storeColumns.m_memValues[idx];
            m_mergeNext[srcTape] = pNewMultiColumn->idx;

            if (m_mergeNext[srcTape] == 0)
                m_mergeLast[srcTape] = 0;

            BatchSortHeapInsert<false>(pNewMultiColumn, srcTape);

            /* put the now-unused memtuples entry on the freelist */
            pNewMultiColumn->idx = m_mergeFreeList;

            m_mergeFreeList = idx;
            m_mergeAvailslots[srcTape]++;

            AppendBatch(batch, multiColumn, i);
            continue;
        }

        break;
    }
}

void Batchsortstate::BindingGetBatchFun()
{
    switch (m_status) {
        case BS_SORTEDINMEM:
            m_getBatchFun = &Batchsortstate::GetBatchInMemory;
            break;

        case BS_SORTEDONTAPE:
            m_getBatchFun = &Batchsortstate::GetBatchDisk;
            break;

        case BS_FINALMERGE:
            m_getBatchFun = &Batchsortstate::GetBatchFinalMerge;
            break;

        default:
            ereport(ERROR,
                (errmodule(MOD_EXECUTOR), errcode(ERRCODE_UNEXPECTED_NODE_STATE), errmsg("invalid tuplesort state")));
    }
}

void Batchsortstate::GetBatch(bool forward, VectorBatch* batch)
{
    (this->*m_getBatchFun)(forward, batch);
    if (batch->m_rows > 0) {
        for (int i = 0; i < batch->m_cols; ++i) {
            batch->m_arr[i].m_desc = m_colInfo[i].m_desc;
        }
    }
}

int Batchsortstate::GetSortMergeOrder()
{
    int mOrder;

    /*
     * We need one tape for each merge input, plus another one for the output,
     * and each of these tapes needs buffer space.	In addition we want
     * MERGE_BUFFER_SIZE workspace per input tape (but the output tape doesn't
     * count).
     *
     * Note: you might be thinking we need to account for the memtuples[]
     * array in this calculation, but we effectively treat that as part of the
     * MERGE_BUFFER_SIZE workspace.
     */
    mOrder = (m_allowedMem - TAPE_BUFFER_OVERHEAD) / (MERGE_BUFFER_SIZE + TAPE_BUFFER_OVERHEAD);

    /* Even in minimum memory, use at least a MINORDER merge */
    mOrder = Max(mOrder, MINORDER);

    return mOrder;
}

void Batchsortstate::MergePreRead()
{
    int srcTape;

    for (srcTape = 0; srcTape < m_maxTapes; srcTape++)
        MergePreReadDone(srcTape);
}

void Batchsortstate::MergePreReadDone(int srcTape)
{
    unsigned int tuplen;
    MultiColumns stup;
    int idx;
    long priorAvail, spaceUsed;

    if (!m_mergeActive[srcTape])
        return; /* tape's run is already exhausted */

    priorAvail = m_availMem;

    m_availMem = m_mergeAvailmem[srcTape];
    while (m_mergeAvailslots[srcTape] > 0 || m_mergeNext[srcTape] == 0) {
        /* read next tuple, if any */
#ifdef PGXC
        if ((tuplen = getlen(this, srcTape, true)) == 0) {
#else
        if ((tuplen = getlen(this, srcTape, true)) == 0) {
#endif
            m_mergeActive[srcTape] = false;
            break;
        }

        readMultiColumn(this, stup, srcTape, tuplen);

        /* find a free slot in memtuples[] for it */
        idx = m_mergeFreeList;

        if (idx)
            m_mergeFreeList = m_storeColumns.m_memValues[idx].idx;
        else {
            idx = m_mergeFirstFree++;
            Assert(idx < m_storeColumns.m_capacity);
        }

        m_mergeAvailslots[srcTape]--;

        /* store tuple, append to list for its tape */
        stup.idx = 0;

        m_storeColumns.m_memValues[idx] = stup;

        if (m_mergeLast[srcTape])
            m_storeColumns.m_memValues[m_mergeLast[srcTape]].idx = idx;
        else
            m_mergeNext[srcTape] = idx;
        m_mergeLast[srcTape] = idx;

        /* allow this loop to be cancellable */
        CHECK_FOR_INTERRUPTS();
    }

    /* update per-tape and global availmem counts */
    spaceUsed = m_mergeAvailmem[srcTape] - m_availMem;
    m_mergeAvailmem[srcTape] = m_availMem;
    m_availMem = priorAvail - spaceUsed;
}

/*
 * beginmerge - initialize for a merge pass
 *
 * We decrease the counts of real and dummy runs for each tape, and mark
 * which tapes contain active input runs in mergeactive[].	Then, load
 * as many tuples as we can from each active input tape, and finally
 * fill the merge heap with the first tuple from each active tape.
 */
void Batchsortstate::BeginMerge()
{
    int activeTapes;
    int tapenum;
    int srcTape;
    int slotsPerTape;
    long spacePerTape;
    errno_t rc;

    /* Heap should be empty here */
    Assert(m_storeColumns.m_memRowNum == 0);

    /* Adjust run counts and mark the active tapes */
    rc = memset_s(m_mergeActive, m_maxTapes * sizeof(bool), 0, m_maxTapes * sizeof(bool));
    securec_check(rc, "\0", "\0");
    activeTapes = 0;
    for (tapenum = 0; tapenum < m_tapeRange; tapenum++) {
        if (m_tpDummy[tapenum] > 0)
            m_tpDummy[tapenum]--;
        else {
            Assert(m_tpRuns[tapenum] > 0);
            m_tpRuns[tapenum]--;
            srcTape = m_tpNum[tapenum];
            m_mergeActive[srcTape] = true;
            activeTapes++;
        }
    }
    m_activeTapes = activeTapes;

    /* Clear merge-pass state variables */
    rc = memset_s(m_mergeNext, m_maxTapes * sizeof(int), 0, m_maxTapes * sizeof(int));
    securec_check(rc, "\0", "\0");
    rc = memset_s(m_mergeLast, m_maxTapes * sizeof(int), 0, m_maxTapes * sizeof(int));
    securec_check(rc, "\0", "\0");
    m_mergeFreeList = 0;            /* nothing in the freelist */
    m_mergeFirstFree = activeTapes; /* 1st slot avail for preread */

    /*
     * Initialize space allocation to let each active input tape have an equal
     * share of preread space.
     */
    if (activeTapes <= 0)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("ActiveTapes should be larger than zero.")));

    slotsPerTape = (m_storeColumns.m_capacity - m_mergeFirstFree) / activeTapes;
    Assert(slotsPerTape > 0);
    spacePerTape = m_availMem / activeTapes;
    for (srcTape = 0; srcTape < m_maxTapes; srcTape++) {
        if (m_mergeActive[srcTape]) {
            m_mergeAvailslots[srcTape] = slotsPerTape;
            m_mergeAvailmem[srcTape] = spacePerTape;
        }
    }

#ifdef TRACE_SORT
    if (u_sess->attr.attr_common.trace_sort)
        ereport(LOG,
            (errmodule(MOD_VEC_EXECUTOR),
                errmsg("Profiling LOG: "
                       "VecSort(%d) Begin Merge : activeTapes: %d, slotsPerTape: %d, spacePerTape: %ld",
                    m_planId,
                    activeTapes,
                    slotsPerTape,
                    spacePerTape)));
#endif

    MergePreRead();

    /* Load the merge heap with the first row from each input tape */
    for (srcTape = 0; srcTape < m_maxTapes; srcTape++) {
        int tupIndex = m_mergeNext[srcTape];
        MultiColumns* tup = NULL;

        if (tupIndex) {
            tup = &m_storeColumns.m_memValues[tupIndex];
            m_mergeNext[srcTape] = tup->idx;

            if (m_mergeNext[srcTape] == 0)
                m_mergeLast[srcTape] = 0;

            BatchSortHeapInsert<false>(tup, srcTape);

            /* put the now-unused memtuples entry on the freelist */
            tup->idx = m_mergeFreeList;
            m_mergeFreeList = tupIndex;
            m_mergeAvailslots[srcTape]++;
        }
    }
}

/*
 * Merge one run from each input tape, except ones with dummy runs.
 *
 * This is the inner loop of Algorithm D step D5.  We know that the
 * output tape is TAPE[T].
 */
void Batchsortstate::MergeOneRun()
{
    int destTape = m_tpNum[m_tapeRange];
    int srcTape;
    int idx;

    MultiColumns* multiColumn = NULL;

    long priorAvail, spaceFreed;

    /*
     * Start the merge by loading one tuple from each active source tape into
     * the heap.  We can also decrease the input run/dummy run counts.
     */
    BeginMerge();

    /*
     * Execute merge by repeatedly extracting lowest tuple in heap, writing it
     * out, and replacing it with next tuple from same tape (if there is
     * another one).
     */
    while (m_storeColumns.m_memRowNum > 0) {
        /* write the tuple to destTape */
        priorAvail = m_availMem;
        srcTape = m_storeColumns.m_memValues[0].idx;

        writeMultiColumn(this, destTape, &m_storeColumns.m_memValues[0]);

        /* writetup adjusted total free space, now fix per-tape space */
        spaceFreed = m_availMem - priorAvail;

        m_mergeAvailmem[srcTape] += spaceFreed;

        /* compact the heap */
        BatchSortHeapSiftup<false>();

        if ((idx = m_mergeNext[srcTape]) == 0) {
            /* out of preloaded data on this tape, try to read more */
            MergePreRead();

            /* if still no data, we've reached end of run on this tape */
            if ((idx = m_mergeNext[srcTape]) == 0)
                continue;
        }
        /* pull next preread tuple from list, insert in heap */
        multiColumn = &m_storeColumns.m_memValues[idx];

        m_mergeNext[srcTape] = multiColumn->idx;

        if (m_mergeNext[srcTape] == 0)
            m_mergeLast[srcTape] = 0;

        BatchSortHeapInsert<false>(multiColumn, srcTape);

        /* put the now-unused memtuples entry on the freelist */
        multiColumn->idx = m_mergeFreeList;

        m_mergeFreeList = idx;
        m_mergeAvailslots[srcTape]++;
    }

    /*
     * When the heap empties, we're done.  Write an end-of-run marker on the
     * output tape, and increment its count of real runs.
     */
    MarkRunEnd(destTape);
    m_tpRuns[m_tapeRange]++;

#ifdef TRACE_SORT
    if (u_sess->attr.attr_common.trace_sort) {
        elog(LOG, "finished %d-way merge step: %s", m_activeTapes, pg_rusage_show(&m_ruStart));
    }
#endif
}

/*
 * MergeRuns -- merge all the completed initial runs.
 *
 * This implements steps D5, D6 of Algorithm D.  All input data has
 * already been written to initial runs on tape.
 */
void Batchsortstate::MergeRuns()
{
    int tapenum;
    int svTape;
    int svRuns;
    int svDummy;

    Assert(m_status == BS_BUILDRUNS);
    Assert(m_storeColumns.m_memRowNum == 0);

    /*
     * If we produced only one initial run (quite likely if the total data
     * volume is between 1X and 2X workMem), we can just use that tape as the
     * finished output, rather than doing a useless merge.	(This obvious
     * optimization is not in Knuth's algorithm.)
     */
    if (m_curRun == 1) {
        m_resultTape = m_tpNum[m_destTape];
        /* must freeze and rewind the finished output tape */
        LogicalTapeFreeze(m_tapeset, m_resultTape);
        m_status = BS_SORTEDONTAPE;
        return;
    }

    if (sortKeys != NULL && sortKeys->abbrev_converter != NULL) {
        /*
         * If there are multiple runs to be merged, when we go to read back
         * tuples from disk, abbreviated keys will not have been stored, and we
         * don't care to regenerate them.  Disable abbreviation from this point
         * on.
         */
        sortKeys->abbrev_converter = NULL;
        sortKeys->comparator = sortKeys->abbrev_full_comparator;

        /* Not strictly necessary, but be tidy */
        sortKeys->abbrev_abort = NULL;
        sortKeys->abbrev_full_comparator = NULL;
    }

    /* End of step D2: rewind all output tapes to prepare for merging */
    for (tapenum = 0; tapenum < m_tapeRange; ++tapenum)
        LogicalTapeRewindForRead(m_tapeset, tapenum, BLCKSZ);

    for (;;) {
        CHECK_FOR_INTERRUPTS();
        /*
         * At this point we know that tape[T] is empty.  If there's just one
         * (real or dummy) run left on each input tape, then only one merge
         * pass remains.  If we don't have to produce a materialized sorted
         * tape, we can stop at this point and do the final merge on-the-fly.
         */
        if (!m_randomAccess) {
            bool allOneRun = true;

            Assert(m_tpRuns[m_tapeRange] == 0);
            for (tapenum = 0; tapenum < m_tapeRange; tapenum++) {
                if (m_tpRuns[tapenum] + m_tpDummy[tapenum] != 1) {
                    allOneRun = false;
                    break;
                }
            }
            if (allOneRun) {
                /* Tell logtape.c we won't be writing anymore */
                LogicalTapeSetForgetFreeSpace(m_tapeset);
                /* Initialize for the final merge pass */
                BeginMerge();
                m_status = BS_FINALMERGE;
                return;
            }
        }

        /* Step D5: merge runs onto tape[T] until tape[P] is empty */
        while (m_tpRuns[m_tapeRange - 1] || m_tpDummy[m_tapeRange - 1]) {
            bool allDummy = true;

            for (tapenum = 0; tapenum < m_tapeRange; tapenum++) {
                if (m_tpDummy[tapenum] == 0) {
                    allDummy = false;
                    break;
                }
            }

            if (allDummy) {
                m_tpDummy[m_tapeRange]++;
                for (tapenum = 0; tapenum < m_tapeRange; tapenum++)
                    m_tpDummy[tapenum]--;
            } else
                MergeOneRun();
        }

        /* Step D6: decrease level */
        if (--m_level == 0)
            break;
        /* rewind output tape T to use as new input */
        LogicalTapeRewindForRead(m_tapeset, m_tpNum[m_tapeRange], BLCKSZ);
        /* rewind used-up input tape P, and prepare it for write pass */
        LogicalTapeRewindForWrite(m_tapeset, m_tpNum[m_tapeRange - 1]);
        m_tpRuns[m_tapeRange - 1] = 0;

        /*
         * reassign tape units per step D6; note we no longer care about A[]
         */
        svTape = m_tpNum[m_tapeRange];
        svDummy = m_tpDummy[m_tapeRange];
        svRuns = m_tpRuns[m_tapeRange];

        for (tapenum = m_tapeRange; tapenum > 0; tapenum--) {
            m_tpNum[tapenum] = m_tpNum[tapenum - 1];
            m_tpDummy[tapenum] = m_tpDummy[tapenum - 1];
            m_tpRuns[tapenum] = m_tpRuns[tapenum - 1];
        }

        m_tpNum[0] = svTape;
        m_tpDummy[0] = svDummy;
        m_tpRuns[0] = svRuns;
    }

    /*
     * Done.  Knuth says that the result is on TAPE[1], but since we exited
     * the loop without performing the last iteration of step D6, we have not
     * rearranged the tape unit assignment, and therefore the result is on
     * TAPE[T].  We need to do it this way so that we can freeze the final
     * output tape while rewinding it.	The last iteration of step D6 would be
     * a waste of cycles anyway...
     */
    m_resultTape = m_tpNum[m_tapeRange];
    LogicalTapeFreeze(m_tapeset, m_resultTape);
    m_status = BS_SORTEDONTAPE;
}

void Batchsortstate::InitTapes()
{
    int maxTapes, j;

    long tapeSpace;

    /* Compute number of tapes to use: merge order plus 1 */
    maxTapes = GetSortMergeOrder() + 1;

    /*
     * We must have at least 2*maxTapes slots in the memtuples[] array, else
     * we'd not have room for merge heap plus preread.  It seems unlikely that
     * this case would ever occur, but be safe.
     */
    maxTapes = Min(maxTapes, m_storeColumns.m_memRowNum / 2);

    m_maxTapes = maxTapes;
    m_tapeRange = maxTapes - 1;

#ifdef TRACE_SORT
    if (u_sess->attr.attr_common.trace_sort) {
        elog(LOG, "switching to external sort with %d tapes: %s", maxTapes, pg_rusage_show(&m_ruStart));
    }
#endif

    /*
     * Decrease availMem to reflect the space needed for tape buffers; but
     * don't decrease it to the point that we have no room for tuples. (That
     * case is only likely to occur if sorting pass-by-value Datums; in all
     * other scenarios the memtuples[] array is unlikely to occupy more than
     * half of allowedMem.	In the pass-by-value case it's not important to
     * account for tuple space, so we don't care if LACKMEM becomes
     * inaccurate.)
     */
    tapeSpace = maxTapes * TAPE_BUFFER_OVERHEAD;

    if (tapeSpace + (long)GetMemoryChunkSpace(m_storeColumns.m_memValues) < m_allowedMem)
        UseMem(tapeSpace);

    /*
     * Make sure that the temp file(s) underlying the tape set are created in
     * suitable temp tablespaces.
     */
    PrepareTempTablespaces();

    /*
     * Create the tape set and allocate the per-tape data arrays.
     */
    m_tapeset = LogicalTapeSetCreate(maxTapes, NULL, NULL, 0);
    m_lastFileBlocks = 0L;

    m_mergeActive = (bool*)palloc0(maxTapes * sizeof(bool));
    m_mergeNext = (int*)palloc0(maxTapes * sizeof(int));
    m_mergeLast = (int*)palloc0(maxTapes * sizeof(int));
    m_mergeAvailslots = (int*)palloc0(maxTapes * sizeof(int));
    m_mergeAvailmem = (long*)palloc0(maxTapes * sizeof(long));
    m_tpFib = (int*)palloc0(maxTapes * sizeof(int));
    m_tpRuns = (int*)palloc0(maxTapes * sizeof(int));
    m_tpDummy = (int*)palloc0(maxTapes * sizeof(int));
    m_tpNum = (int*)palloc0(maxTapes * sizeof(int));

    int nRows = m_storeColumns.m_memRowNum;

    m_storeColumns.m_memRowNum = 0; /* make the heap empty */
    for (j = 0; j < nRows; j++) {
        /* Must copy source mutiColumn to avoid possible overwrite */
        MultiColumns multiColumnVal = m_storeColumns.m_memValues[j];

        BatchSortHeapInsert<false>(&multiColumnVal, 0);
    }
    Assert(m_storeColumns.m_memRowNum == nRows);

    m_curRun = 0;

    /*
     * Initialize variables of Algorithm D (step D1).
     */
    for (j = 0; j < maxTapes; j++) {
        m_tpFib[j] = 1;
        m_tpRuns[j] = 0;
        m_tpDummy[j] = 1;
        m_tpNum[j] = j;
    }
    m_tpFib[m_tapeRange] = 0;
    m_tpDummy[m_tapeRange] = 0;

    m_level = 1;
    m_destTape = 0;

    m_status = BS_BUILDRUNS;
}

template <bool checkIdx>
void Batchsortstate::BatchSortHeapInsert(MultiColumns* multiColumn, int multiColumnIdx)
{
    int j;

    /*
     * Save the tupleindex --- see notes above about writing on *tuple. It's a
     * historical artifact that tupleindex is passed as a separate argument
     * and not in *tuple, but it's notationally convenient so let's leave it
     * that way.
     */
    multiColumn->idx = multiColumnIdx;

    MultiColumns* rows = m_storeColumns.m_memValues;

    Assert(m_storeColumns.m_memRowNum < m_storeColumns.m_capacity);

    /*
     * Sift-up the new entry, per Knuth 5.2.3 exercise 16. Note that Knuth is
     * using 1-based array indexes, not 0-based.
     */
    j = m_storeColumns.m_memRowNum++;
    while (j > 0) {
        int i = (unsigned int)(j - 1) >> 1;

        if (THeapCompare<checkIdx>(multiColumn, rows + i) >= 0)
            break;
        rows[j] = rows[i];
        j = i;
    }
    rows[j] = *multiColumn;
}

template <bool checkIdx>
void Batchsortstate::BatchSortHeapSiftup()
{
    MultiColumns* rows = m_storeColumns.m_memValues;
    MultiColumns* multiColumn = NULL;
    int i;
    int n;

    if (--m_storeColumns.m_memRowNum <= 0)
        return;

    n = m_storeColumns.m_memRowNum;

    multiColumn = rows + n; /* tuple that must be reinserted */

    i = 0; /* i is where the "hole" is */
    for (;;) {
        int j = 2 * i + 1;

        if (j >= n)
            break;
        if (j + 1 < n && THeapCompare<checkIdx>(rows + j, rows + j + 1) > 0)
            j++;

        if (THeapCompare<checkIdx>(multiColumn, rows + j) <= 0)
            break;
        rows[i] = rows[j];
        i = j;
    }
    rows[i] = *multiColumn;
}

void Batchsortstate::DumpMultiColumn(bool all)
{
    WaitState oldStatus = pgstat_report_waitstatus(STATE_EXEC_SORT_WRITE_FILE);
    while (all || (m_availMem < 0 && m_storeColumns.m_memRowNum > 1) ||
           m_storeColumns.m_memRowNum >= m_storeColumns.m_capacity) {
        /*
         * Dump the heap's frontmost entry, and sift up to remove it from the
         * heap.
         */
        Assert(m_storeColumns.m_memRowNum > 0);

        writeMultiColumn(this, m_tpNum[m_destTape], &m_storeColumns.m_memValues[0]);
        BatchSortHeapSiftup<true>();

        /*
         * If the heap is empty *or* top run number has changed, we've
         * finished the current run.l
         */
        if (m_storeColumns.m_memRowNum == 0 || m_curRun != m_storeColumns.m_memValues[0].idx) {
            MarkRunEnd(m_tpNum[m_destTape]);
            m_curRun++;
            m_tpRuns[m_destTape]++;
            m_tpDummy[m_destTape]--;

#ifdef TRACE_SORT
            if (u_sess->attr.attr_common.trace_sort) {
                elog(LOG,
                    "finished writing%s run %d to tape %d: %s",
                    (m_storeColumns.m_memRowNum == 0) ? " final" : "",
                    m_curRun,
                    m_destTape,
                    pg_rusage_show(&m_ruStart));

                if (m_curRun % 10 == 0) {
                    ereport(LOG,
                        (errmodule(MOD_VEC_EXECUTOR),
                            errmsg("Profiling LOG: "
                                   "VecSort(%d) Disk Spilled : workmem: %ldKB, availmem: %ldKB, "
                                   "memRowNum: %d, memCapacity: %d",
                                m_planId,
                                m_allowedMem / 1024L,
                                m_availMem / 1024L,
                                m_storeColumns.m_memRowNum,
                                m_storeColumns.m_capacity)));
                }
            }
#endif

            if (m_storeColumns.m_memRowNum == 0)
                break;
            Assert(m_curRun == m_storeColumns.m_memValues[0].idx);
            SelectNewTape();
        }
    }
    (void)pgstat_report_waitstatus(oldStatus);
}

void Batchsortstate::MakeBoundedHeap()
{
    int tupcount = m_storeColumns.m_memRowNum;
    int i;

    Assert(m_status == BS_INITIAL);
    Assert(m_bounded);
    Assert(tupcount >= m_bound);

    /* Reverse sort direction so largest entry will be at root */
    ReverseDirectionHeap();

    /*
     * Using the TOP N version of CompareMultiColumn:
     * For sort node, field compareMultiColumn is already set to jitted_CompareMultiColumn
     * if codegen is generated here. When meet bounded heap case, we need to switch
     * to jitted_CompareMultiColumn_TOPN for the ReverseDirectionHeap().
     */
    if (jitted_CompareMultiColumn_TOPN && ((char*)compareMultiColumn) == jitted_CompareMultiColumn) {
        compareMultiColumn = ((LLVM_CMC_func)(jitted_CompareMultiColumn_TOPN));
    } else {
        /* For the case where the whole sql is pushed down on datanodes, all the
         * plan_node_ids are zero, we could not check if there is a limit node on
         * the upper level of sort node precislly. This case can also happen in
         * 'execute direct on' sql.
         */
        if (sortKeys[0].abbrev_converter == NULL)
            compareMultiColumn = CompareMultiColumn<false>;
        else
            compareMultiColumn = CompareMultiColumn<true>;
    }

    m_storeColumns.m_memRowNum = 0; /* make the heap empty */
    for (i = 0; i < tupcount; i++) {
        if (m_storeColumns.m_memRowNum >= m_bound &&
            compareMultiColumn(m_storeColumns.m_memValues + i, m_storeColumns.m_memValues, this) <= 0) {
            /* New tuple would just get thrown out, so skip it */
            FreeMultiColumn(m_storeColumns.m_memValues + i);
        } else {
            /* Insert next multiColumn into heap */
            /* Must copy source tuple to avoid possible overwrite */
            MultiColumns multiColumn = m_storeColumns.m_memValues[i];
            BatchSortHeapInsert<false>(&multiColumn, 0);

            /* If heap too full, discard largest entry */
            if (m_storeColumns.m_memRowNum > m_bound) {
                FreeMultiColumn(m_storeColumns.m_memValues);
                BatchSortHeapSiftup<false>();
            }
        }
    }

    Assert(m_storeColumns.m_memRowNum == m_bound);
    m_status = BS_BOUNDED;
}

void Batchsortstate::SortBoundedHeap()
{
    int rowCount = m_storeColumns.m_memRowNum;

    Assert(m_status == BS_BOUNDED);
    Assert(m_bounded);
    Assert(rowCount == m_bound);

    /*
     * We can unheapify in place because each sift-up will remove the largest
     * entry, which we can promptly store in the newly freed slot at the end.
     * Once we're down to a single-entry heap, we're done.
     */
    while (m_storeColumns.m_memRowNum > 1) {
        MultiColumns multiColumn = m_storeColumns.m_memValues[0];

        /* this sifts-up the next-largest entry and decreases memtupcount */
        BatchSortHeapSiftup<false>();
        m_storeColumns.m_memValues[m_storeColumns.m_memRowNum] = multiColumn;
    }
    m_storeColumns.m_memRowNum = rowCount;

    /*
     * Reverse sort direction back to the original state.  This is not
     * actually necessary but seems like a good idea for tidiness.
     */
    ReverseDirectionHeap();

    /* Switch back to jitted_CompareMultiColumn for TOP N */
    if (jitted_CompareMultiColumn && ((char*)compareMultiColumn) == jitted_CompareMultiColumn_TOPN) {
        compareMultiColumn = ((LLVM_CMC_func)(jitted_CompareMultiColumn));
    }

    m_status = BS_SORTEDINMEM;
    m_boundUsed = true;
}

inline void Batchsortstate::ReverseDirectionHeap()
{
    ScanKey scanKey = m_scanKeys;
    SortSupport sortKey = sortKeys;
    int nkey;

    for (nkey = 0; nkey < m_nKeys; nkey++, scanKey++) {
        scanKey->sk_flags ^= (SK_BT_DESC | SK_BT_NULLS_FIRST);
    }

    sortKey->ssup_reverse = !sortKey->ssup_reverse;
    sortKey->ssup_nulls_first = !sortKey->ssup_nulls_first;
}

void Batchsortstate::MarkRunEnd(int tapenum)
{
    unsigned int len = 0;

    LogicalTapeWrite(m_tapeset, tapenum, (void*)&len, sizeof(len));
}

void Batchsortstate::SelectNewTape()
{
    int j;
    int a;

    /* Step D3: advance j (destTape) */
    if (m_tpDummy[m_destTape] < m_tpDummy[m_destTape + 1]) {
        m_destTape++;
        return;
    }
    if (m_tpDummy[m_destTape] != 0) {
        m_destTape = 0;
        return;
    }

    /* Step D4: increase level */
    m_level++;
    a = m_tpFib[0];
    for (j = 0; j < m_tapeRange; j++) {
        m_tpDummy[j] = a + m_tpFib[j + 1] - m_tpFib[j];
        m_tpFib[j] = a + m_tpFib[j + 1];
    }
    m_destTape = 0;
}

template <bool checkIdx>
int Batchsortstate::THeapCompare(MultiColumns* a, MultiColumns* b)
{
    return (checkIdx && (a->idx != b->idx) ? (a->idx - b->idx) : compareMultiColumn(a, b, this));
}

int Batchsortstate::HeapCompare(MultiColumns* a, MultiColumns* b, bool checkIdx)
{
    return (checkIdx && (a->idx != b->idx) ? (a->idx - b->idx) : compareMultiColumn(a, b, this));
}

/*
 * Inline-able copy of FunctionCall2Coll() to save some cycles in sorting.
 */
static inline Datum myFunctionCall2Coll(FmgrInfo* flinfo, Oid collation, Datum arg1, Datum arg2)
{
    FunctionCallInfoData fcinfo;
    Datum result;

    InitFunctionCallInfoData(fcinfo, flinfo, 2, collation, NULL, NULL);

    fcinfo.arg[0] = arg1;
    fcinfo.arg[1] = arg2;
    fcinfo.argnull[0] = false;
    fcinfo.argnull[1] = false;

    result = FunctionCallInvoke(&fcinfo);

    /* Check for null result, since caller is clearly not expecting one */
    if (fcinfo.isnull)
        ereport(ERROR,
            (errmodule(MOD_EXECUTOR),
                errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                errmsg("function %u returned NULL", fcinfo.flinfo->fn_oid)));

    return result;
}

/*
 * Apply a sort function (by now converted to fmgr lookup form)
 * and return a 3-way comparison result.  This takes care of handling
 * reverse-sort and NULLs-ordering properly.  We assume that DESC and
 * NULLS_FIRST options are encoded in sk_flags the same way btree does it.
 */
static inline int32 inlineApplySortFunction(
    FmgrInfo* sortFunction, uint32 sk_flags, Oid collation, Datum datum1, bool isNull1, Datum datum2, bool isNull2)
{
    int32 compare = 0;

    if (!isNull1 && !isNull2) {
        if (sortFunction->fn_addr == NULL) {
            if (datum1 < datum2)
                compare = -1;
            else if (datum1 == datum2)
                compare = 0;
            else if (datum1 > datum2)
                compare = 1;
        } else
            compare = DatumGetInt32(myFunctionCall2Coll(sortFunction, collation, datum1, datum2));

        if (sk_flags & SK_BT_DESC)
            compare = -compare;
    } else if (isNull1) {
        if (isNull2)
            compare = 0; /* NULL "=" NULL */
        else if (sk_flags & SK_BT_NULLS_FIRST)
            compare = -1; /* NULL "<" NOT_NULL */
        else
            compare = 1; /* NULL ">" NOT_NULL */
    } else if (isNull2) {
        if (sk_flags & SK_BT_NULLS_FIRST)
            compare = 1; /* NOT_NULL ">" NULL */
        else
            compare = -1; /* NOT_NULL "<" NULL */
    }
    return compare;
}

/*
 * When abbreSortOptimize is true , we use bttextcmp_abbrev or
 * numeric_cmp_abbrev to speed up compare operation, else execute the
 * old logic.
 */
template <bool abbreSortOptimize>
int CompareMultiColumn(const MultiColumns* a, const MultiColumns* b, Batchsortstate* state)
{
    ScanKey scanKey = state->m_scanKeys;
    int nkey;
    int32 compare = 0;

    Datum datum1, datum2;
    Datum datum1Tmp, datum2Tmp;
    bool isnull1 = false;
    bool isnull2 = false;

    CHECK_FOR_INTERRUPTS();

    int colIdx;

    // abbreSortOptimize is ture, run this code
    if (abbreSortOptimize) {
        /* Compare the leading sort key */
        if (state->sortKeys->abbrev_converter) {
            colIdx = scanKey->sk_attno - 1;
            compare = ApplySortComparator(a->m_values[state->m_colNum],
                IS_NULL(a->m_nulls[colIdx]),
                b->m_values[state->m_colNum],
                IS_NULL(b->m_nulls[colIdx]),
                state->sortKeys);
            if (compare != 0)
                return compare;
        }
    }

    for (nkey = 0; nkey < state->m_nKeys; ++nkey, ++scanKey) {
        colIdx = scanKey->sk_attno - 1;

        Assert(colIdx >= 0);

        Form_pg_attribute attr = state->tupDesc->attrs[colIdx];
        Oid typeOid = attr->atttypid;

        datum1Tmp = datum1 = a->m_values[colIdx];
        datum2Tmp = datum2 = b->m_values[colIdx];
        isnull1 = IS_NULL(a->m_nulls[colIdx]);
        isnull2 = IS_NULL(b->m_nulls[colIdx]);

        switch (typeOid) {
            // extract header
            case TIMETZOID:
            case INTERVALOID:
            case TINTERVALOID:
            case NAMEOID:
            case MACADDROID:
                datum1 = isnull1 == false ? PointerGetDatum((char*)datum1Tmp + VARHDRSZ_SHORT) : 0;
                datum2 = isnull2 == false ? PointerGetDatum((char*)datum2Tmp + VARHDRSZ_SHORT) : 0;
                break;
            case TIDOID:
                datum1 = isnull1 == false ? PointerGetDatum(&datum1Tmp) : 0;
                datum2 = isnull2 == false ? PointerGetDatum(&datum2Tmp) : 0;
                break;
            default:
                break;
        }

        compare = inlineApplySortFunction(
            &scanKey->sk_func, scanKey->sk_flags, scanKey->sk_collation, datum1, isnull1, datum2, isnull2);
        if (compare != 0)
            return compare;
    }

    return compare;
}

void WriteMultiColumn(Batchsortstate* state, int tapeNum, MultiColumns* multiColumn)
{
    int colNum = state->m_colNum;

    int dataLen = multiColumn->GetLen(colNum);
    int len = dataLen + sizeof(len);

    char* data = state->GetData(colNum, multiColumn);

    LogicalTapeWrite(state->m_tapeset, tapeNum, (void*)&len, sizeof(len));

    LogicalTapeWrite(state->m_tapeset, tapeNum, (void*)data, dataLen);

    if (state->m_randomAccess)
        LogicalTapeWrite(state->m_tapeset, tapeNum, (void*)&len, sizeof(len));

    state->FreeMem(GetMemoryChunkSpace(data));
    pfree_ext(data);

    state->FreeMultiColumn(multiColumn);
}

void ReadMultiColumn(Batchsortstate* state, MultiColumns& multiColumn, int tapeNum, unsigned int len)
{
    Size chunkspace = 0;
    unsigned int dataLen = len - sizeof(int);
    Assert(len > sizeof(int));

    char* dataPtr = (char*)palloc(dataLen);

    chunkspace = GetMemoryChunkSpace(dataPtr);

    state->UseMem(chunkspace);

    if (LogicalTapeRead(state->m_tapeset, tapeNum, dataPtr, dataLen) != (size_t)dataLen)
        ereport(ERROR, (errmodule(MOD_EXECUTOR), errcode(ERRCODE_FILE_READ_FAILED), errmsg("unexpected end of data")));

    state->SetData(dataPtr, dataLen, &multiColumn);

    state->FreeMem(chunkspace);
    pfree_ext(dataPtr);

    /* need trailing length word? */
    if (state->m_randomAccess) {
        LogicalTapeRead(state->m_tapeset, tapeNum, &dataLen, sizeof(dataLen));
    }
}

unsigned int GetLen(Batchsortstate* state, int tapenum, bool eofOK)
{
    unsigned int len = 0;

    if (LogicalTapeRead(state->m_tapeset, tapenum, &len, sizeof(len)) != sizeof(len))
        ereport(ERROR, (errmodule(MOD_EXECUTOR), errcode(ERRCODE_FILE_READ_FAILED), errmsg("unexpected end of tape")));

    if (len == 0 && !eofOK)
        ereport(ERROR, (errmodule(MOD_EXECUTOR), errcode(ERRCODE_FILE_READ_FAILED), errmsg("unexpected end of data")));

    return len;
}

void ReadFromDataNode(Batchsortstate* state, MultiColumns& multiColumn, int tapeNum, unsigned int len)
{
    VectorBatch* vecBatch = state->m_connDNBatch[tapeNum];
    multiColumn.m_values = (ScalarValue*)palloc(len);
    multiColumn.m_nulls = (uint8*)palloc(vecBatch->m_cols * sizeof(uint8));

    state->UseMem(GetMemoryChunkSpace(multiColumn.m_values));
    state->UseMem(GetMemoryChunkSpace(multiColumn.m_nulls));

    int cursor = state->m_datanodeFetchCursor[tapeNum];

    uint8 flag;

    Assert(cursor < vecBatch->m_rows);

    for (int i = 0; i < vecBatch->m_cols; ++i) {
        flag = vecBatch->m_arr[i].m_flag[cursor];
        multiColumn.m_nulls[i] = flag;
        if (!IS_NULL(flag)) {
            if (state->NeedDecode(i)) {
                Form_pg_attribute attr = state->tupDesc->attrs[i];
                ScalarValue val = vecBatch->m_arr[i].m_vals[cursor];
                Datum v = ScalarVector::Decode(val);
                int typlen = 0;
                if (vecBatch->m_arr[i].m_desc.typeId == NAMEOID) {
                    typlen = datumGetSize(v, false, -2);
                } else {
                    typlen = VARSIZE_ANY(v);
                }
                multiColumn.m_values[i] = datumCopy(v, attr->attbyval, typlen);
                char* p = DatumGetPointer(multiColumn.m_values[i]);
                state->UseMem(GetMemoryChunkSpace(p));
            } else {
                multiColumn.m_values[i] = PointerGetDatum(vecBatch->m_arr[i].m_vals[cursor]);
            }
        }
    }

    if (++state->m_datanodeFetchCursor[tapeNum] == vecBatch->m_rows) {
        vecBatch->Reset();
        state->m_datanodeFetchCursor[tapeNum] = 0;
    }
}

static unsigned int GetBatchFromDatanode(Batchsortstate* state, int tapenum, VectorBatch* batch, bool eofOK)
{
    RemoteQueryState* combiner = state->combiner;
    PGXCNodeHandle* conn = NULL;

    // When u_sess->attr.attr_memory.work_mem is not big enough to pre read all the data from Datanodes
    // and some the other steps reuse the same Datanode connections, the left data
    // rows should be stored (buffered) in BufferConnection. We should refresh the
    // connections here because 'removing' and 'adjusting' current connection in
    // BufferConnection disorder them. Thus we can get right node oid in each connections.
    //
    if (combiner->switch_connection[tapenum] && !combiner->refresh_handles) {
        RemoteQuery* step = (RemoteQuery*)combiner->ss.ps.plan;
        PGXCNodeAllHandles* pgxc_handles = NULL;

        // Get needed Datanode connections.
        //
        pgxc_handles = get_handles(step->exec_nodes->nodeList, NULL, false);
        combiner->connections = pgxc_handles->datanode_handles;
        combiner->refresh_handles = true;
    }

    conn = combiner->connections[tapenum];

    /*
     * If connection is active (potentially has data to read) we can get node
     * number from the connection. If connection is not active (we have read all
     * available data rows) and if we have buffered data from that connection
     * the node number is stored in combiner->tapenodes[tapenum].
     * If connection is inactive and no buffered data we have EOF condition
     */
    int nid;

    /* May it ever happen ?! */
    if (conn == NULL && !combiner->tapenodes)
        ereport(ERROR,
            (errmodule(MOD_EXECUTOR),
                errcode(ERRCODE_FETCH_DATA_FAILED),
                errmsg("Failed to fetch from data node cursor")));

    nid = conn ? PGXCNodeGetNodeId(conn->nodeoid, PGXC_NODE_DATANODE) : combiner->tapenodes[tapenum];
    if (nid < 0)
        ereport(ERROR,
            (errmodule(MOD_EXECUTOR), errcode(ERRCODE_UNEXPECTED_NODE_STATE), errmsg("Node id %d is incorrect", nid)));

    /*
     * If there are buffered rows iterate over them and get first from
     * the requested tape
     */
    RemoteDataRowData datarow;

    if (RowStoreFetch(combiner->row_store, PGXCNodeGetNodeOid(nid, PGXC_NODE_DATANODE), &datarow)) {
        combiner->currentRow = datarow;
        // If we have message in the buffer, consume it.
        //
        if (combiner->currentRow.msg)
            CopyDataRowToBatch(combiner, batch);

        return datarow.msglen;
    }

    // Even though combiner->rowBuffer is NIL when exhausting the buffer,
    // flag 'switchConnection' can be used to return safely here.
    //
    if (combiner->switch_connection[tapenum]) {
        return 0;
    }

    /* Nothing is found in the buffer, check for EOF */
    if (conn == NULL) {
        if (eofOK)
            return 0;
        else
            ereport(
                ERROR, (errmodule(MOD_EXECUTOR), errcode(ERRCODE_DATA_EXCEPTION), errmsg("unexpected end of data")));
    }

    /* Going to get data from connection, buffer if needed */
    if (conn->state == DN_CONNECTION_STATE_QUERY && conn->combiner != combiner)
        BufferConnection(conn);

    /* Request more rows if needed */
    if (conn->state == DN_CONNECTION_STATE_IDLE) {
        Assert(combiner->cursor);
        if (pgxc_node_send_execute(conn, combiner->cursor, 1) != 0)
            ereport(ERROR,
                (errmodule(MOD_EXECUTOR),
                    errcode(ERRCODE_FETCH_DATA_FAILED),
                    errmsg("Failed to fetch from data node cursor")));

        if (pgxc_node_send_sync(conn) != 0)
            ereport(ERROR,
                (errmodule(MOD_EXECUTOR),
                    errcode(ERRCODE_FETCH_DATA_FAILED),
                    errmsg("Failed to fetch from data node cursor")));

        conn->state = DN_CONNECTION_STATE_QUERY;
        conn->combiner = combiner;
    }
    /* Read data from the connection until get a row or EOF */
    for (;;) {
        switch (handle_response(conn, combiner)) {
            case RESPONSE_SUSPENDED:
                /* Send Execute to request next row */
                Assert(combiner->cursor);
                if (pgxc_node_send_execute(conn, combiner->cursor, 1) != 0)
                    ereport(ERROR,
                        (errmodule(MOD_EXECUTOR),
                            errcode(ERRCODE_FETCH_DATA_FAILED),
                            errmsg("Failed to fetch from data node cursor")));
                if (pgxc_node_send_sync(conn) != 0)
                    ereport(ERROR,
                        (errmodule(MOD_EXECUTOR),
                            errcode(ERRCODE_FETCH_DATA_FAILED),
                            errmsg("Failed to fetch from data node cursor")));
                conn->state = DN_CONNECTION_STATE_QUERY;
                conn->combiner = combiner;
                /* fallthru */
            case RESPONSE_EOF:
                /* receive more data */
                struct timeval timeout;
                timeout.tv_sec = ERROR_CHECK_TIMEOUT;
                timeout.tv_usec = 0;

                /*
                 * If need check the other errors after getting a normal communcation error,
                 * set timeout first when coming to receive data again. If then get any poll
                 * error, report the former cached error in combiner(RemoteQueryState).
                 */
                if (pgxc_node_receive(1, &conn, combiner->need_error_check ? &timeout : NULL)) {
                    if (!combiner->need_error_check) {
                        int error_code;
                        char* error_msg = getSocketError(&error_code);

                        ereport(ERROR,
                            (errcode(error_code),
                                errmsg("Failed to read response from Datanodes Detail: %s\n", error_msg)));
                    } else {
                        ereport(LOG,
                            (errmsg("%s:%d recv fail", __FUNCTION__, __LINE__)));

                        combiner->need_error_check = false;
                        pgxc_node_report_error(combiner);
                    }
                }
                break;
            case RESPONSE_COMPLETE:
                /* EOF encountered, close the tape and report EOF */
                if (eofOK)
                    return 0;
                else
                    ereport(ERROR,
                        (errmodule(MOD_EXECUTOR),
                            errcode(ERRCODE_CONNECTION_EXCEPTION),
                            errmsg("unexpected end of data")));
                break;
            case RESPONSE_DATAROW:
                /* If we have message in the buffer, consume it */
                if (combiner->currentRow.msg) {
                    CopyDataRowToBatch(combiner, batch);
                }
                return 1;
            default:
                ereport(ERROR,
                    (errmodule(MOD_EXECUTOR),
                        errcode(ERRCODE_UNEXPECTED_NODE_STATE),
                        errmsg("Unexpected response from the data nodes")));
        }
        /* report error if any */
        pgxc_node_report_error(combiner);
    }
}

unsigned int GetLenFromDataNode(Batchsortstate* state, int tapenum, bool eofOK)
{
    unsigned int len = state->m_colNum * sizeof(ScalarValue);
    VectorBatch* vecBatch = state->m_connDNBatch[tapenum];

    if (BatchIsNull(vecBatch)) {
        GetBatchFromDatanode(state, tapenum, vecBatch, eofOK);

        Assert(state->m_datanodeFetchCursor[tapenum] == 0);
        /* fetch to end */
        if (!BatchIsNull(vecBatch)) {
            /*
             * it is only initialized once.
             */
            if (!state->m_colInfo)
                state->InitColInfo(vecBatch);
        } else
            len = 0;
    }

    return len;
}

/*
 * stream get vector batch for merge sort from producer
 */
static unsigned int StreamGetBatchFromDatanode(Batchsortstate* state, int tapenum, VectorBatch* vecBatch, bool eofOK)
{
    VecStreamState* node = state->streamstate;
    PGXCNodeHandle* connection = node->connections[tapenum];

    while (true) {
        int res = HandleStreamResponse(connection, node);
        switch (res) {
            /* Try next run. */
            case RESPONSE_EOF: {
                Assert(node->need_fresh_data == true);
                if (node->need_fresh_data) {
                    if (datanode_receive_from_logic_conn(1, &node->connections[tapenum], &node->netctl, -1)) {
                        int error_code = getStreamSocketError(gs_comm_strerror());
                        ereport(ERROR,
                            (errcode(error_code),
                                errmsg("Failed to read response from Datanodes. Detail: %s\n", gs_comm_strerror())));
                    }
                    continue;
                }
            } break;
            /* Finish one connection. */
            case RESPONSE_COMPLETE: {
                node->conn_count = node->conn_count - 1;

                /* All finished. */
                if (node->conn_count == 0) {
                    node->need_fresh_data = false;
                }

                return 0;
            } break;
            case RESPONSE_DATAROW: {
                if (node->buf.len != 0) {
                    node->batchForm(node, vecBatch);

                    if (vecBatch->m_rows == BatchMaxSize || node->redistribute == false) {
                        return 1;
                    } else {
                        return 0;
                    }
                } else {
                    return 0;
                }
            } break;
            default:
                ereport(ERROR, (errcode(ERRCODE_FETCH_DATA_FAILED), errmsg("Unexpected response from Datanode")));
                break;
        }

        break;
    }

    Assert(0);
    return (unsigned int)-1;
}

/*
 * the entry of stream get vector batch for merge sort
 */
unsigned int StreamGetLenFromDataNode(Batchsortstate* state, int tapenum, bool eofOK)
{
    unsigned int len = state->m_colNum * sizeof(ScalarValue);
    VectorBatch* vecBatch = state->m_connDNBatch[tapenum];

    if (BatchIsNull(vecBatch)) {
        (void)StreamGetBatchFromDatanode(state, tapenum, vecBatch, eofOK);

        Assert(state->m_datanodeFetchCursor[tapenum] == 0);
        /* fetch to end */
        if (!BatchIsNull(vecBatch)) {
            /*
             * it is only initialized once.
             */
            if (!state->m_colInfo)
                state->InitColInfo(vecBatch);
        } else
            len = 0;
    }

    return len;
}
