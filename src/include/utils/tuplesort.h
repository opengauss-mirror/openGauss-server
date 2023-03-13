/* -------------------------------------------------------------------------
 *
 * tuplesort.h
 *	  Generalized tuple sorting routines.
 *
 * This module handles sorting of heap tuples, index tuples, or single
 * Datums (and could easily support other kinds of sortable objects,
 * if necessary).  It works efficiently for both small and large amounts
 * of data.  Small amounts are sorted in-memory using qsort().	Large
 * amounts are sorted using temporary files and a standard external sort
 * algorithm.
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/tuplesort.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef TUPLESORT_H
#define TUPLESORT_H

#include "access/itup.h"
#include "executor/tuptable.h"
#include "fmgr.h"
#include "utils/relcache.h"
#include "utils/tuplestore.h"
#include "utils/logtape.h"


/* Tuplesortstate is an opaque type whose details are not known outside
 * tuplesort.c.
 */

/*
 * Private mutable state of tuplesort-parallel-operation.  This is allocated
 * in shared memory.
 */

struct Sharedsort {
    /* mutex protects all fields prior to tapes */
    slock_t mutex;

    /*
     * currentWorker generates ordinal identifier numbers for parallel sort
     * workers.  These start from 0, and are always gapless.
     *
     * Workers increment workersFinished to indicate having finished.  If this
     * is equal to state.nParticipants within the leader, leader is ready to
     * merge worker runs.
     */

    int currentWorker;
    int workersFinished;

    /* Temporary file space */
    SharedFileSet fileset;

    /* Size of tapes flexible array */
    int nTapes;

    /* actual number of participants */
    int actualParticipants;

    /*
     * Tapes array used by workers to report back information needed by the
     * leader to concatenate all worker tapes into one for merging
     */
    TapeShare tapes[FLEXIBLE_ARRAY_MEMBER];
};

typedef struct Tuplesortstate Tuplesortstate;
typedef struct Sharedsort Sharedsort;

/*
 * Tuplesort parallel coordination state, allocated by each participant in
 * local memory.  Participant caller initializes everything.  See usage notes
 * below.
 */
typedef struct SortCoordinateData {
    /* Worker process?  If not, must be leader. */
    bool        isWorker;

    /*
     * Leader-process-passed number of participants known launched (workers
     * set this to -1).  Includes state within leader needed for it to
     * participate as a worker, if any.
     */
    int         nParticipants;

    /* Private opaque state (points to shared memory) */
    Sharedsort *sharedsort;
} SortCoordinateData;

typedef struct SortCoordinateData *SortCoordinate;

/*
 * We provide multiple interfaces to what is essentially the same code,
 * since different callers have different data to be sorted and want to
 * specify the sort key information differently.  There are two APIs for
 * sorting HeapTuples and two more for sorting IndexTuples.  Yet another
 * API supports sorting bare Datums.
 *
 * The "heap" API actually stores/sorts MinimalTuples, which means it doesn't
 * preserve the system columns (tuple identity and transaction visibility
 * info).  The sort keys are specified by column numbers within the tuples
 * and sort operator OIDs.	We save some cycles by passing and returning the
 * tuples in TupleTableSlots, rather than forming actual HeapTuples (which'd
 * have to be converted to MinimalTuples).	This API works well for sorts
 * executed as parts of plan trees.
 *
 * The "cluster" API stores/sorts full HeapTuples including all visibility
 * info. The sort keys are specified by reference to a btree index that is
 * defined on the relation to be sorted.  Note that putheaptuple/getheaptuple
 * go with this API, not the "begin_heap" one!
 *
 * The "index_btree" API stores/sorts IndexTuples (preserving all their
 * header fields).	The sort keys are specified by a btree index definition.
 *
 * The "index_hash" API is similar to index_btree, but the tuples are
 * actually sorted by their hash codes not the raw data.
 */

extern Tuplesortstate* tuplesort_begin_heap(TupleDesc tupDesc, int nkeys, AttrNumber* attNums, Oid* sortOperators,
    Oid* sortCollations, const bool* nullsFirstFlags, int64 workMem, bool randomAccess, int64 maxMem = 0,
    int planId = 0, int dop = 1);
extern Tuplesortstate* tuplesort_begin_cluster(
    TupleDesc tupDesc, Relation indexRel, int workMem, bool randomAccess, int maxMem, bool relIsUstore);
extern Tuplesortstate* tuplesort_begin_index_btree(
    Relation indexRel, bool enforceUnique, int workMem, SortCoordinate coordinate, bool randomAccess, int maxMem);
extern Tuplesortstate* tuplesort_begin_index_hash(
    Relation heapRel, Relation indexRel, uint32 high_mask, uint32 low_mask, uint32 max_buckets, 
    int workMem, bool randomAccess, int maxMem);
extern Tuplesortstate* tuplesort_begin_datum(
    Oid datumType, Oid sortOperator, Oid sortCollation, bool nullsFirstFlag, int workMem, bool randomAccess);
#ifdef PGXC
extern void tuplesort_puttupleslotontape(Tuplesortstate* state, TupleTableSlot* slot);
extern void tuplesort_remoteread_end(Tuplesortstate* state);
#endif

extern void tuplesort_set_bound(Tuplesortstate* state, int64 bound);
extern void tuplesort_set_siblings(Tuplesortstate* state, const int numKeys, const List *internalEntryList);

extern void tuplesort_puttupleslot(Tuplesortstate* state, TupleTableSlot* slot);
extern void TuplesortPutheaptuple(Tuplesortstate* state, HeapTuple tup);
extern void tuplesort_putindextuplevalues(
    Tuplesortstate* state, Relation rel, ItemPointer self, Datum* values, const bool* isnull);
extern void tuplesort_putdatum(Tuplesortstate* state, Datum val, bool isNull);

extern void tuplesort_performsort(Tuplesortstate* state);

extern bool tuplesort_gettupleslot(Tuplesortstate* state, bool forward, TupleTableSlot* slot, Datum* abbrev);
extern bool tuplesort_gettupleslot_into_tuplestore(
    Tuplesortstate* state, bool forward, TupleTableSlot* slot, Datum* abbrev, Tuplestorestate* tstate);
extern void* tuplesort_getheaptuple(Tuplesortstate* state, bool forward);
extern IndexTuple tuplesort_getindextuple(Tuplesortstate* state, bool forward);
extern bool tuplesort_getdatum(Tuplesortstate* state, bool forward, Datum* val, bool* isNull);

extern void tuplesort_end(Tuplesortstate* state);

extern void tuplesort_get_stats(Tuplesortstate* state, int* sortMethodId, int* spaceTypeId, long* spaceUsed);

extern int tuplesort_merge_order(double allowedMem);
extern Size tuplesort_estimate_shared(int nworkers);
extern void tuplesort_initialize_shared(Sharedsort *shared, int nWorkers);
extern void tuplesort_attach_shared(Sharedsort *shared);


/*
 * These routines may only be called if randomAccess was specified 'true'.
 * Likewise, backwards scan in gettuple/getdatum is only allowed if
 * randomAccess was specified.
 */

extern void tuplesort_rescan(Tuplesortstate* state);
extern void tuplesort_markpos(Tuplesortstate* state);
extern void tuplesort_restorepos(Tuplesortstate* state);

extern void sort_count(Tuplesortstate* state);

extern int64 tuplesort_get_avgwidth(Tuplesortstate* state);
extern bool tuplesort_get_busy_status(Tuplesortstate* state);
extern int tuplesort_get_spread_num(Tuplesortstate* state);
extern bool tuplesort_skiptuples(Tuplesortstate* state, int64 ntuples, bool forward);
extern void UpdateUniqueSQLSortStats(Tuplesortstate* state, TimestampTz* start_time);

/*
 * Return the int64 value of tuplesortstate->peakMemorySize
 */
extern int64 tuplesort_get_peak_memory(Tuplesortstate* state);

extern void tuplesort_workerfinish(Sharedsort *shared);

#endif /* TUPLESORT_H */
