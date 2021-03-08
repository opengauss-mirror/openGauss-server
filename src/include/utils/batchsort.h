/* ---------------------------------------------------------------------------------------
 * 
 * batchsort.h
 *        Generalized tuple sorting routines.
 * 
 * This module handles sorting of heap tuples, index tuples, or single
 * Datums (and could easily support other kinds of sortable objects,
 * if necessary).  It works efficiently for both small and large amounts
 * of data.  Small amounts are sorted in-memory using qsort().	Large
 * amounts are sorted using temporary files and a standard external sort
 * algorithm.
 *
 * Portions Copyright (c) 1996-2011, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * 
 * IDENTIFICATION
 *        src/include/utils/batchsort.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef BATCHSORT_H
#define BATCHSORT_H

#include "access/itup.h"
#include "executor/tuptable.h"
#include "fmgr.h"
#ifdef PGXC
#include "pgxc/execRemote.h"
#include "vecexecutor/vecnodes.h"
#endif
#include "utils/elog.h"
#include "utils/relcache.h"
#include "vecexecutor/vecstore.h"
#include "utils/logtape.h"
#include "utils/pg_rusage.h"

extern THR_LOCAL int vsort_mem;
extern const int MINORDER;

/*
 * Possible states of a BatchSort object.
 */
typedef enum {
    BS_INITIAL = 0,
    BS_BOUNDED,
    BS_BUILDRUNS,
    BS_SORTEDINMEM,
    BS_SORTEDONTAPE,
    BS_FINALMERGE
} BatchSortStatus;

/*
 * Private state of a batchsort operation.
 */
class Batchsortstate : public VecStore {
public:
    // Memory context for main allocation
    MemoryContext sortcontext;

    /*
     * enumerated value as shown above
     */
    BatchSortStatus m_status;

#ifdef PGXC
    RemoteQueryState* combiner;
    VectorBatch** m_connDNBatch;

    int* m_datanodeFetchCursor;

    // merge sort
    VecStreamState* streamstate;
#endif
    /*
     * number of columns in sort key
     */
    int m_nKeys;

    /*
     * Scan keys
     */
    ScanKey m_scanKeys;

    /*
     * The sortKeys variable is used by every case other than the hash index
     * case; it is set by tuplesort_begin_xxx.
     */
    SortSupport sortKeys; /* array of length nKeys */

    /*
     * Additional state for managing "abbreviated key" sortsupport routines
     * (which currently may be used by all cases except the hash index case).
     * Tracks the intervals at which the optimization's effectiveness is
     * tested.
     */
    int64 abbrevNext; /* Tuple # at which to next check
                       * applicability */

    /*
     * did caller request random access?
     */
    bool m_randomAccess;

    /*
     * did caller specify a maximum number of tuples to return
     */
    bool m_bounded;

    /*
     * Top N sort
     */
    bool m_boundUsed;

    /*
     * if bounded, the maximum number of tuples
     */
    int m_bound;

    MultiColumnsData m_unsortColumns;

    bool* m_isSortKey;

    long m_lastBlock;

    int m_lastOffset;

    /*
     * variables which relate to external sort
     */
    int m_maxTapes;

    int m_tapeRange;

    LogicalTapeSet* m_tapeset;

    /*
     * While building initial runs, this is the current output run number
     * (starting at 0).  Afterwards, it is the number of initial runs we made.
     */
    int m_curRun;

    /*
     * Unless otherwise noted, all pointer variables below are pointers to
     * arrays of length maxTapes, holding per-tape data.
     */

    /*
     * These variables are only used during merge passes.  mergeactive[i] is
     * true if we are reading an input run from (actual) tape number i and
     * have not yet exhausted that run.  mergenext[i] is the memtuples index
     * of the next pre-read tuple (next to be loaded into the heap) for tape
     * i, or 0 if we are out of pre-read tuples.  mergelast[i] similarly
     * points to the last pre-read tuple from each tape.  mergeavailslots[i]
     * is the number of unused memtuples[] slots reserved for tape i, and
     * mergeavailmem[i] is the amount of unused space allocated for tape i.
     * mergefreelist and mergefirstfree keep track of unused locations in the
     * memtuples[] array.  The memtuples[].tupindex fields link together
     * pre-read tuples for each tape as well as recycled locations in
     * mergefreelist. It is OK to use 0 as a null link in these lists, because
     * memtuples[0] is part of the merge heap and is never a pre-read tuple.
     */
    bool* m_mergeActive;    /* active input run source? */
    int* m_mergeNext;       /* first preread tuple for each source */
    int* m_mergeLast;       /* last preread tuple for each source */
    int* m_mergeAvailslots; /* slots left for prereading each tape */
    long* m_mergeAvailmem;  /* availMem for prereading each tape */
    int m_mergeFreeList;    /* head of freelist of recycled slots */
    int m_mergeFirstFree;   /* first slot never used in this merge */

    /*
     * Variables for Algorithm D.  Note that destTape is a "logical" tape
     * number, ie, an index into the tp_xxx[] arrays.  Be careful to keep
     * "logical" and "actual" tape numbers straight!
     */
    int m_level;       /* Knuth's l */
    int m_destTape;    /* current output tape (Knuth's j, less 1) */
    int* m_tpFib;      /* Target Fibonacci run counts (A[]) */
    int* m_tpRuns;     /* # of real runs on each tape */
    int* m_tpDummy;    /* # of dummy runs for each tape (D[]) */
    int* m_tpNum;      /* Actual tape numbers (TAPE[]) */
    int m_activeTapes; /* # of active input tapes in merge pass */

    /*
     * These variables are used after completion of sorting to keep track of
     * the next tuple to return.  (In the tape case, the tape's current read
     * position is also critical state.)
     */
    int m_resultTape;  /* actual tape number of finished output */
    bool m_eofReached; /* reached EOF (needed for cursors) */

    /* markpos_xxx holds marked position for mark and restore */
    long m_markposBlock;   /* tape block# (only used if SORTEDONTAPE) */
    int m_markposOffset;   /* saved "current", or offset in tape block */
    bool m_markposEof;     /* saved "eof_reached" */
    long m_lastFileBlocks; /* last file blocks used in underlying file */

    int64 peakMemorySize; /* memory size before writeMultiColumn*/

    // Resource snapshot for time of sort start.
    //
#ifdef TRACE_SORT
    PGRUsage m_ruStart;
#endif

    char* jitted_CompareMultiColumn;      /* jitted function for CompareMultiColumn  */
    char* jitted_CompareMultiColumn_TOPN; /* jitted function for CompareMultiColumn used by Top N sort */

    /*
     * Initialize variables.
     */

    void InitCommon(int64 workMem, bool randomAccess);

    /*
     * Based on hyper log log estimation, consider wheather exit abbreviate
     * mode or not.
     */
    bool ConsiderAbortCommon();

    void SortInMem();

    int GetSortMergeOrder();

    void InitTapes();

    void DumpMultiColumn(bool all);

    int HeapCompare(MultiColumns* a, MultiColumns* b, bool checkIdx);

    template <bool checkIdx>
    int THeapCompare(MultiColumns* a, MultiColumns* b);

    template <bool checkIdx>
    void BatchSortHeapInsert(MultiColumns* multiColumn, int multiColumnIdx);

    template <bool checkIdx>
    void BatchSortHeapSiftup();

    void MarkRunEnd(int tapenum);

    void SelectNewTape();

    void MergeRuns();

    void BeginMerge();

    void MergeOneRun();

    void MergePreReadDone(int srcTape);

    void MergePreRead();
    void GetBatch(bool forward, VectorBatch* batch);

    void (Batchsortstate::*m_getBatchFun)(bool forward, VectorBatch* batch);

    void GetBatchInMemory(bool forward, VectorBatch* batch);

    void GetBatchDisk(bool forward, VectorBatch* batch);

    void GetBatchFinalMerge(bool forward, VectorBatch* batch);

    void BindingGetBatchFun();

    inline void ReverseDirectionHeap();

    void MakeBoundedHeap();

    void SortBoundedHeap();

    void DumpUnsortColumns(bool all);

    /*
     * These function pointers decouple the routines that must know what kind
     * of tuple we are sorting from the routines that don't need to know it.
     * They are set up by the tuplesort_begin_xxx routines.
     *
     * Function to compare two tuples; result is per qsort() convention, ie:
     * <0, 0, >0 according as a<b, a=b, a>b.  The API must match
     * qsort_arg_comparator.
     */
    int (*compareMultiColumn)(const MultiColumns* a, const MultiColumns* b, Batchsortstate* state);

    /*
     * Function to copy a supplied input tuple into palloc'd space and set up
     * its SortTuple representation (ie, set tuple/datum1/isnull1).  Also,
     * state->availMem must be decreased by the amount of space used for the
     * tuple copy (note the SortTuple struct itself is not counted).
     */
    void (*copyMultiColumn)(Batchsortstate* state, MultiColumns* stup, void* tup);

    /*
     * Function to write a stored tuple onto tape.	The representation of the
     * tuple on tape need not be the same as it is in memory; requirements on
     * the tape representation are given below.  After writing the tuple,
     * pfree() the out-of-line data (not the SortTuple struct!), and increase
     * state->availMem by the amount of memory space thereby released.
     */
    void (*writeMultiColumn)(Batchsortstate* state, int tapenum, MultiColumns* stup);

    /*
     * Function to read a stored tuple from tape back into memory. 'len' is
     * the already-read length of the stored tuple.  Create a palloc'd copy,
     * initialize tuple/datum1/isnull1 in the target SortTuple struct, and
     * decrease state->availMem by the amount of memory space consumed.
     */
    void (*readMultiColumn)(Batchsortstate* state, MultiColumns& stup, int tapenum, unsigned int len);

#ifdef PGXC
    /*
     * Function to read length of next stored tuple.
     * Used as 'len' parameter for readtup function.
     */
    unsigned int (*getlen)(Batchsortstate* state, int tapenum, bool eofOK);
#endif

    /*
     * Function to reverse the sort direction from its current state. (We
     * could dispense with this if we wanted to enforce that all variants
     * represent the sort key information alike.)
     */
    void (*reversedirection)(Batchsortstate* state);

    /*
     * Function to accept one batch while collecting input data for sort.
     * Note that the input data is always copied; the caller need not save it.
     *
     * We choose batchsort_putbatch<false> for normal case, choose
     * batchsort_putbatch<false> for fast abbreviate comparison function for sort.
     * So we need to implement different function of batchsort_putbatch.
     */
    void (*sort_putbatch)(Batchsortstate* state, VectorBatch* batch, int start, int end);
};

extern Batchsortstate* batchsort_begin_heap(TupleDesc tupDesc, int nkeys, AttrNumber* attNums, Oid* sortOperators,
    Oid* sortCollations, const bool* nullsFirstFlags, int64 workMem, bool randomAccess, int64 maxMem = 0,
    int planId = 0, int dop = 1);
#ifdef PGXC
extern Batchsortstate* batchsort_begin_merge(TupleDesc tupDesc, int nkeys, AttrNumber* attNums, Oid* sortOperators,
    Oid* sortCollations, const bool* nullsFirstFlags, void* combiner, int64 workMem);
#endif

extern void batchsort_set_bound(Batchsortstate* state, int64 bound);

/*
 * abbreSortOptimize used to mark whether allocate one more Datum for
 * fast compare of two data(text or numeric type)
 */
template <bool abbrevSortOptimize>
void putbatch(Batchsortstate* state, VectorBatch* batch, int start, int end)
{
    int64 memorySize = 0;
    for (int row = start; row < end; ++row) {
        MultiColumns multiColumn = state->CopyMultiColumn<abbrevSortOptimize>(batch, row);

        if (abbrevSortOptimize) {
            if (state->sortKeys->abbrev_converter && !IS_NULL(multiColumn.m_nulls[state->sortKeys->ssup_attno - 1]) &&
                !state->ConsiderAbortCommon())
                /* Store abbreviated key representation */
                multiColumn.m_values[batch->m_cols] = state->sortKeys->abbrev_converter(
                    multiColumn.m_values[state->sortKeys->ssup_attno - 1], state->sortKeys);
            else
                multiColumn.m_values[batch->m_cols] = multiColumn.m_values[state->sortKeys->ssup_attno - 1];
        }

        switch (state->m_status) {
            case BS_INITIAL:

                if ((!state->HasFreeSlot() || state->m_availMem <= 0) &&
                    state->m_storeColumns.m_memRowNum >= MINORDER * 2) {
                    /* if mem is used up, then adjust capacity */
                    if (state->m_availMem <= 0)
                        state->m_storeColumns.m_capacity = state->m_storeColumns.m_memRowNum + 1;
                    state->GrowMemValueSlots("VecSort", state->m_planId, state->sortcontext);
                }
                state->PutValue(multiColumn);

                /*
                 * Check if it's time to switch over to a bounded heapsort. We do
                 * so if the input tuple count exceeds twice the desired tuple
                 * count (this is a heuristic for where heapsort becomes cheaper
                 * than a quicksort), or if we've just filled workMem and have
                 * enough tuples to meet the bound.
                 *
                 * Note that once we enter TSS_BOUNDED state we will always try to
                 * complete the sort that way.	In the worst case, if later input
                 * tuples are larger than earlier ones, this might cause us to
                 * exceed workMem significantly.
                 */
                if (state->m_bounded && (state->m_storeColumns.m_memRowNum > state->m_bound * 2 ||
                                            (state->m_storeColumns.m_memRowNum > state->m_bound && state->LackMem()))) {
#ifdef TRACE_SORT
                    if (u_sess->attr.attr_common.trace_sort) {
                        elog(LOG,
                            "switching to bounded heapsort at %d tuples: %s",
                            state->m_storeColumns.m_memRowNum,
                            pg_rusage_show(&state->m_ruStart));
                    }
#endif
                    if (state->m_storeColumns.m_memRowNum > 0) {
                        state->m_colWidth /= state->m_storeColumns.m_memRowNum;
                        state->m_addWidth = false;
                    }
                    state->MakeBoundedHeap();
                    continue;
                }

                /*
                 * Once we do not have enough memory, we will turn to external sort. But before we do
                 * InitTapes, we should have at least MINORDER * 2 MultiColumns in MultiColumnsData.
                 * Since the minimal number of tapes we need in external sort is MINORDER.
                 */
                while (state->LackMem() && (state->m_storeColumns.m_memRowNum < MINORDER * 2)) {
                    state->m_availMem += state->m_allowedMem;
                }

                /*
                 * Done if we still fit in available memory and have at least MINORDER * 2 array slots.
                 */
                if ((state->m_storeColumns.m_memRowNum < state->m_storeColumns.m_capacity && !state->LackMem()) ||
                    state->m_storeColumns.m_memRowNum < MINORDER * 2)
                    continue;

                if (state->m_storeColumns.m_memRowNum > 0) {
                    state->m_colWidth /= state->m_storeColumns.m_memRowNum;
                    state->m_addWidth = false;
                }

                if (state->LackMem()) {
                    ereport(LOG,
                        (errmodule(MOD_VEC_EXECUTOR),
                            errmsg("Profiling Warning: "
                                   "VecSort(%d) Disk Spilled : workmem: %ldKB, availmem: %ldKB, "
                                   "memRowNum: %d, memCapacity: %d",
                                state->m_planId,
                                state->m_allowedMem / 1024L,
                                state->m_availMem / 1024L,
                                state->m_storeColumns.m_memRowNum,
                                state->m_storeColumns.m_capacity)));
                }

                state->InitTapes();

                /*
                 * Cache memory size info Batchsortstate before write to tapes.
                 * If the memory size has been cached during previous dump,
                 * do not update the size with current context size.
                 * note: state->peakMemorySize is only used to log memory size before dump for now.
                 *          Keep caution once it is adopted for other cases in the future..
                 */
                if (state->peakMemorySize <= 0) {
                    memorySize = 0;
                    CalculateContextSize(state->sortcontext, &memorySize);
                    state->peakMemorySize = memorySize;
                }

#ifdef TRACE_SORT
                if (u_sess->attr.attr_common.trace_sort)
                    ereport(LOG,
                        (errmodule(MOD_VEC_EXECUTOR),
                            errmsg("Profiling LOG: "
                                   "VecSort(%d) Disk Spilled : workmem: %ldKB, availmem: %ldKB, "
                                   "memRowNum: %d, memCapacity: %d",
                                state->m_planId,
                                state->m_allowedMem / 1024L,
                                state->m_availMem / 1024L,
                                state->m_storeColumns.m_memRowNum,
                                state->m_storeColumns.m_capacity)));
#endif

                state->m_storeColumns.m_capacity = state->m_storeColumns.m_memRowNum;

                /*
                 * If we are over the memory limit, dump tuples till we're under.
                 */
                state->DumpMultiColumn(false);

#ifdef TRACE_SORT
                if (u_sess->attr.attr_common.trace_sort)
                    ereport(LOG,
                        (errmodule(MOD_VEC_EXECUTOR),
                            errmsg("Profiling LOG: "
                                   "VecSort(%d) Disk Spilled : workmem: %ldKB, availmem: %ldKB, "
                                   "memRowNum: %d, memCapacity: %d",
                                state->m_planId,
                                state->m_allowedMem / 1024L,
                                state->m_availMem / 1024L,
                                state->m_storeColumns.m_memRowNum,
                                state->m_storeColumns.m_capacity)));
#endif

                break;

            case BS_BOUNDED:

                if (state->compareMultiColumn(&multiColumn, state->m_storeColumns.m_memValues, state) <= 0) {
                    state->FreeMultiColumn(&multiColumn);
                } else {
                    state->FreeMultiColumn(state->m_storeColumns.m_memValues);
                    state->BatchSortHeapSiftup<false>();
                    state->BatchSortHeapInsert<false>(&multiColumn, 0);
                }
                break;

            case BS_BUILDRUNS:

                if (state->compareMultiColumn(&multiColumn, &state->m_storeColumns.m_memValues[0], state) >= 0) {
                    state->BatchSortHeapInsert<true>(&multiColumn, state->m_curRun);
                } else {
                    state->BatchSortHeapInsert<true>(&multiColumn, state->m_curRun + 1);
                }

                /*
                 * If we are over the memory limit, dump tuples till we're under.
                 */
                state->DumpMultiColumn(false);

                break;

            default:
                elog(ERROR, "invalid BatchSort state");
                break;
        }
    }
}

/*
 * Accept one tuple while collecting input data for sort.
 * Note that the input data is always copied; the caller need not save it.
 *
 * When the first column of order_by_columns is text or numeric type, we
 * use bttextcmp_abbrev or numeric_cmp_abbrev to speed up compare operation,
 * meanwhile we need to allocate one more Datum to store prefix info, so
 * set abbreSortOptimize to be true. For other case set abbreSortOptimize
 * to be false.
 */
template <bool abbrevSortOptimize>
void batchsort_putbatch(Batchsortstate* state, VectorBatch* batch, int start, int end)
{
    MemoryContext oldcontext = MemoryContextSwitchTo(state->sortcontext);

    /*
     * it is only initialized once.
     */
    if (!state->m_colInfo) {
        state->InitColInfo(batch);
    }

    putbatch<abbrevSortOptimize>(state, batch, start, end);

    MemoryContextSwitchTo(oldcontext);
}

extern void batchsort_performsort(Batchsortstate* state);

extern void batchsort_getbatch(Batchsortstate* state, bool forward, VectorBatch* batch);

extern void batchsort_end(Batchsortstate* state);

extern void batchsort_get_stats(Batchsortstate* state, int* sortMethodId, int* spaceTypeId, long* spaceUsed);

/*
 * These routines may only be called if randomAccess was specified 'true'.
 * Likewise, backwards scan in gettuple/getdatum is only allowed if
 * randomAccess was specified.
 */

extern void batchsort_rescan(Batchsortstate* state);

extern void batchsort_markpos(Batchsortstate* state);
extern void batchsort_restorepos(Batchsortstate* state);

extern void batchsort_get_stats(Batchsortstate* state, int* sortMethodId, int* spaceTypeId, long* spaceUsed);

#endif /* BATCHSORT_H */
