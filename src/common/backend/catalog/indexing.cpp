/* -------------------------------------------------------------------------
 *
 * indexing.cpp
 *	  This file contains routines to support indexes defined on system
 *	  catalogs.
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/common/backend/catalog/indexing.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "catalog/index.h"
#include "catalog/indexing.h"
#include "executor/executor.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"

/*
 * CatalogOpenIndexes - open the indexes on a system catalog.
 *
 * When inserting or updating tuples in a system catalog, call this
 * to prepare to update the indexes for the catalog.
 *
 * In the current implementation, we share code for opening/closing the
 * indexes with execUtils.c.  But we do not use ExecInsertIndexTuples,
 * because we don't want to create an EState.  This implies that we
 * do not support partial or expressional indexes on system catalogs,
 * nor can we support generalized exclusion constraints.
 * This could be fixed with localized changes here if we wanted to pay
 * the extra overhead of building an EState.
 */
CatalogIndexState CatalogOpenIndexes(Relation heapRel)
{
    ResultRelInfo* resultRelInfo = NULL;

    resultRelInfo = makeNode(ResultRelInfo);
    resultRelInfo->ri_RangeTableIndex = 1; /* dummy */
    resultRelInfo->ri_RelationDesc = heapRel;
    resultRelInfo->ri_TrigDesc = NULL; /* we don't fire triggers */

    ExecOpenIndices(resultRelInfo, false);

    return resultRelInfo;
}

/*
 * CatalogCloseIndexes - clean up resources allocated by CatalogOpenIndexes
 */
void CatalogCloseIndexes(CatalogIndexState indstate)
{
    ExecCloseIndices(indstate);
    pfree(indstate);
    indstate = NULL;
}

/*
 * CatalogIndexInsert - insert index entries for one catalog tuple
 *
 * This should be called for each inserted or updated catalog tuple.
 *
 * This is effectively a cut-down version of ExecInsertIndexTuples.
 */
void CatalogIndexInsert(CatalogIndexState indstate, HeapTuple heapTuple)
{
    int numIndexes;
    RelationPtr relationDescs;
    Relation heapRelation;
    TupleTableSlot* slot = NULL;
    IndexInfo** indexInfoArray;
    Datum values[INDEX_MAX_KEYS];
    bool isnull[INDEX_MAX_KEYS];

    /* HOT update does not require index inserts */
#ifndef USE_ASSERT_CHECKING
    if (HeapTupleIsHeapOnly(heapTuple))
        return;
#endif
    /*
     * Get information from the state structure.  Fall out if nothing to do.
     */
    numIndexes = indstate->ri_NumIndices;
    if (numIndexes == 0)
        return;
    relationDescs = indstate->ri_IndexRelationDescs;
    indexInfoArray = indstate->ri_IndexRelationInfo;
    heapRelation = indstate->ri_RelationDesc;

    /* Need a slot to hold the tuple being examined */
    slot = MakeSingleTupleTableSlot(RelationGetDescr(heapRelation));
    (void)ExecStoreTuple(heapTuple, slot, InvalidBuffer, false);

    /*
     * for each index, form and insert the index tuple
     */
    for (int i = 0; i < numIndexes; i++) {
        IndexInfo* indexInfo = NULL;
        Relation index;

        indexInfo = indexInfoArray[i];
        index = relationDescs[i];

        /* If the index is marked as read-only, ignore it */
        if (!indexInfo->ii_ReadyForInserts)
            continue;

        /*
         * Expressional and partial indexes on system catalogs are not
         * supported, nor exclusion constraints, nor deferred uniqueness
         */
        Assert(indexInfo->ii_Expressions == NIL);
        Assert(indexInfo->ii_Predicate == NIL);
        Assert(indexInfo->ii_ExclusionOps == NULL);
        Assert(index->rd_index->indimmediate);
        Assert(indexInfo->ii_NumIndexKeyAttrs != 0);

#ifdef USE_ASSERT_CHECKING
        if (HeapTupleIsHeapOnly(heapTuple)) {
            Assert(!ReindexIsProcessingIndex(RelationGetRelid(index)));
            continue;
        }
#endif /* USE_ASSERT_CHECKING */

        /*
         * FormIndexDatum fills in its values and isnull parameters with the
         * appropriate values for the column(s) of the index.
         */
        FormIndexDatum(indexInfo,
            slot,
            NULL, /* no expression eval to do */
            values,
            isnull);

        /*
         * The index AM does the rest.
         */
        (void)index_insert(relationDescs[i], /* index relation */
            values,                    /* array of index Datums */
            isnull,                    /* is-null flags */
            &(heapTuple->t_self),      /* tid of heap tuple */
            heapRelation,
            relationDescs[i]->rd_index->indisunique ? UNIQUE_CHECK_YES : UNIQUE_CHECK_NO);
    }

    ExecDropSingleTupleTableSlot(slot);
}

/*
 * CatalogUpdateIndexes - do all the indexing work for a new catalog tuple
 *
 * This is a convenience routine for the common case where we only need
 * to insert or update a single tuple in a system catalog.	Avoid using it for
 * multiple tuples, since opening the indexes and building the index info
 * structures is moderately expensive.
 */
void CatalogUpdateIndexes(Relation heapRel, HeapTuple heapTuple)
{
    CatalogIndexState indstate;

    indstate = CatalogOpenIndexes(heapRel);
    CatalogIndexInsert(indstate, heapTuple);
    CatalogCloseIndexes(indstate);
}

/*
 * CatalogTupleUpdate - do heap and indexing work for updating a catalog tuple
 *
 * Update the tuple identified by "otid", replacing it with the data in "tup".
 *
 * This is a convenience routine for the common case of updating a single
 * tuple in a system catalog; it updates one heap tuple, keeping indexes
 * current.  Avoid using it for multiple tuples, since opening the indexes
 * and building the index info structures is moderately expensive.
 * (Use CatalogTupleUpdateWithInfo in such cases.)
 */
void
CatalogTupleUpdate(Relation heapRel, ItemPointer otid, HeapTuple tup)
{
    CatalogIndexState indstate;

    indstate = CatalogOpenIndexes(heapRel);

    simple_heap_update(heapRel, otid, tup);

    CatalogIndexInsert(indstate, tup);
    CatalogCloseIndexes(indstate);
}

/*
 * CatalogTupleInsert - do heap and indexing work for a new catalog tuple
 *
 * Insert the tuple data in "tup" into the specified catalog relation.
 * The Oid of the inserted tuple is returned.
 *
 * This is a convenience routine for the common case of inserting a single
 * tuple in a system catalog; it inserts a new heap tuple, keeping indexes
 * current.  Avoid using it for multiple tuples, since opening the indexes
 * and building the index info structures is moderately expensive.
 * (Use CatalogTupleInsertWithInfo in such cases.)
 */
Oid
CatalogTupleInsert(Relation heapRel, HeapTuple tup)
{
    CatalogIndexState indstate;
    Oid oid;

    indstate = CatalogOpenIndexes(heapRel);

    oid = simple_heap_insert(heapRel, tup);

    CatalogIndexInsert(indstate, tup);
    CatalogCloseIndexes(indstate);

    return oid;
}

/*
 * CatalogTupleDelete - do heap and indexing work for deleting a catalog tuple
 *
 * Delete the tuple identified by "tid" in the specified catalog.
 *
 * With Postgres heaps, there is no index work to do at deletion time;
 * cleanup will be done later by VACUUM.  However, callers of this function
 * shouldn't have to know that; we'd like a uniform abstraction for all
 * catalog tuple changes.  Hence, provide this currently-trivial wrapper.
 *
 * The abstraction is a bit leaky in that we don't provide an optimized
 * CatalogTupleDeleteWithInfo version, because there is currently nothing to
 * optimize.  If we ever need that, rather than touching a lot of call sites,
 * it might be better to do something about caching CatalogIndexState.
 */
void
CatalogTupleDelete(Relation heapRel, ItemPointer tid)
{
    simple_heap_delete(heapRel, tid);
}


