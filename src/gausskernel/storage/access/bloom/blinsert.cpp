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
 * -------------------------------------------------------------------------
 *
 * blinsert.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/bloom/blinsert.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/generic_xlog.h"
#include "access/genam.h"
#include "access/tableam.h"
#include "catalog/index.h"
#include "miscadmin.h"
#include "storage/buf/bufmgr.h"
#include "storage/indexfsm.h"
#include "utils/memutils.h"
#include "utils/rel.h"

#include "access/bloom.h"

/*
 * State of bloom index build. We accumulate one page data here before
 * flushing it to buffer manager.
 */
typedef struct {
    BloomState      blstate;            /* bloom index state */
    int64           indtuples;            /* total number of tuples indexed */
    MemoryContext   tmpCtx;             /* temporary memory context reset after each tuple */
    char            data[BLCKSZ];       /* cached page */
    int64           count;              /* number of tuples in cached page */
} BloomBuildState;

/*
 * Flush page cached in BloomBuildState.
 */
static void flushCachedPage(Relation index, BloomBuildState *buildstate)
{
    Page        page;
    Buffer      buffer = BloomNewBuffer(index);
    GenericXLogState *state;
    errno_t     rc;

    state = GenericXLogStart(index);
    page = GenericXLogRegisterBuffer(state, buffer, GENERIC_XLOG_FULL_IMAGE);
    rc = memcpy_s(page, BLCKSZ, buildstate->data, BLCKSZ);
    securec_check(rc, "\0", "\0");
    GenericXLogFinish(state);
    UnlockReleaseBuffer(buffer);
    CHECK_FOR_INTERRUPTS();
}

/*
 * (Re)initialize cached page in BloomBuildState.
 */
static void InitCachedPage(BloomBuildState *buildstate)
{
    errno_t rc = memset_s(buildstate->data, BLCKSZ, 0, BLCKSZ);
    securec_check(rc, "\0", "\0");
    BloomInitPage(buildstate->data, 0);
    buildstate->count = 0;
}

/*
 * Per-tuple callback from IndexBuildHeapScan.
 */
static void bloomBuildCallback(Relation index, HeapTuple htup, Datum *values,
                               const bool *isnull, bool tupleIsAlive, void *state)
{
    BloomBuildState *buildstate = (BloomBuildState *) state;
    MemoryContext oldCtx;
    BloomTuple *itup;

    oldCtx = MemoryContextSwitchTo(buildstate->tmpCtx);

    itup = BloomFormTuple(&buildstate->blstate, &htup->t_self, values, (bool *)isnull);
    /* Try to add next item to cached page */
    if (BloomPageAddItem(&buildstate->blstate, buildstate->data, itup)) {
        /* Next item was added successfully */
        buildstate->count++;
    } else {
        /* Cached page is full, flush it out and make a new one */
        flushCachedPage(index, buildstate);

        InitCachedPage(buildstate);

        if (!BloomPageAddItem(&buildstate->blstate, buildstate->data, itup)) {
            pfree_ext(itup);
            /* We shouldn't be here since we're inserting to the empty page */
            elog(ERROR, "could not add new bloom tuple to empty page");
        }

        /* Next item was added successfully */
        buildstate->count++;
    }

    /* Update total tuple count */
    buildstate->indtuples += 1;

    MemoryContextSwitchTo(oldCtx);
    MemoryContextReset(buildstate->tmpCtx);
}

/*
 * Build a new bloom index.
 */
IndexBuildResult *blbuild_internal(Relation heap, Relation index, IndexInfo *indexInfo)
{
    IndexBuildResult    *result;
    double              reltuples;
    BloomBuildState     buildstate;

    if (RelationGetNumberOfBlocks(index) != 0)
        elog(ERROR, "index \"%s\" already contains data",
             RelationGetRelationName(index));

    /* Initialize the meta page */
    BloomInitMetapage(index, MAIN_FORKNUM);

    /* Initialize the bloom build state */
    errno_t rc = memset_s(&buildstate, sizeof(buildstate), 0, sizeof(buildstate));
    securec_check(rc, "\0", "\0");

    initBloomState(&buildstate.blstate, index);
    buildstate.tmpCtx = AllocSetContextCreate(CurrentMemoryContext,
                                              "Bloom build temporary context",
                                              ALLOCSET_DEFAULT_MINSIZE,
                                              ALLOCSET_DEFAULT_INITSIZE,
                                              ALLOCSET_DEFAULT_MAXSIZE);
    InitCachedPage(&buildstate);

    /* Do the heap scan */
    reltuples = tableam_index_build_scan(heap, index, indexInfo, true,
                                         bloomBuildCallback, (void *)&buildstate,
                                         NULL);

    /*
     * There are could be some items in cached page.  Flush this page
     * if needed.
     */
    if (buildstate.count > 0) {
        flushCachedPage(index, &buildstate);
    }

    MemoryContextDelete(buildstate.tmpCtx);
    result = (IndexBuildResult *)palloc(sizeof(IndexBuildResult));
    result->heap_tuples = reltuples;
    result->index_tuples = buildstate.indtuples;

    return result;
}

/*
 * Build an empty bloom index in the initialization fork.
 */
void blbuildempty_internal(Relation index)
{
    if (RelationGetNumberOfBlocks(index) != 0)
        elog(ERROR, "index \"%s\" already contains data",
             RelationGetRelationName(index));

    /* Initialize the meta page */
    BloomInitMetapage(index, INIT_FORKNUM);
}

/*
 * Insert new tuple to the bloom index.
 */
bool blinsert_internal(Relation index, Datum *values, const bool *isnull,
                       ItemPointer ht_ctid, Relation heapRel, IndexUniqueCheck checkUnique)
{
    BloomState          blstate;
    BloomTuple          *itup;
    MemoryContext       oldCtx;
    MemoryContext       insertCtx;
    BloomMetaPageData   *metaData;
    Buffer              buffer;
    Buffer              metaBuffer;
    Page                page;
    Page                metaPage;
    OffsetNumber        nStart;
    GenericXLogState    *state;

    BlockNumber blkno = InvalidBlockNumber;

    insertCtx = AllocSetContextCreate(CurrentMemoryContext,
                                      "Bloom insert temporary context",
                                      ALLOCSET_DEFAULT_MINSIZE,
                                      ALLOCSET_DEFAULT_INITSIZE,
                                      ALLOCSET_DEFAULT_MAXSIZE);

    oldCtx = MemoryContextSwitchTo(insertCtx);

    initBloomState(&blstate, index);
    itup = BloomFormTuple(&blstate, ht_ctid, values, (bool *)isnull);
    /*
     * At first, try to insert new tuple to the first page in notFullPage
     * array.  If success we don't need to modify the meta page.
     */
    metaBuffer = ReadBuffer(index, BLOOM_METAPAGE_BLKNO);
    LockBuffer(metaBuffer, BUFFER_LOCK_SHARE);
    metaData = BloomPageGetMeta(BufferGetPage(metaBuffer));
    if (metaData->nEnd > metaData->nStart) {
        Page        page;

        blkno = metaData->notFullPage[metaData->nStart];

        Assert(blkno != InvalidBlockNumber);
        /* Don't hold metabuffer lock while doing insert */
        LockBuffer(metaBuffer, BUFFER_LOCK_UNLOCK);

        buffer = ReadBuffer(index, blkno);
        LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
        state = GenericXLogStart(index);
        page = GenericXLogRegisterBuffer(state, buffer, false);
        /*
         * We might have found a page that was recently deleted by VACUUM.  If
         * so, we can reuse it, but we must reinitialize it.
         */
        if (PageIsNew(page) || BloomPageIsDeleted(page))
            BloomInitPage(page, 0);

        if (BloomPageAddItem(&blstate, page, itup)) {
            /* Success!  Apply the change, clean up, and exit */
            pfree_ext(itup);
            GenericXLogFinish(state);
            UnlockReleaseBuffer(buffer);
            ReleaseBuffer(metaBuffer);
            MemoryContextSwitchTo(oldCtx);
            MemoryContextDelete(insertCtx);
            return false;
        }
        /* Didn't fit, must try other pages */
        GenericXLogAbort(state);
        UnlockReleaseBuffer(buffer);
    } else {
        /* First page in notFullPage isn't suitable */
        LockBuffer(metaBuffer, BUFFER_LOCK_UNLOCK);
    }

    /*
     * Try other pages in notFullPage array.  We will have to change nStart in
     * metapage.  Thus, grab exclusive lock on metapage.
     */
    LockBuffer(metaBuffer, BUFFER_LOCK_EXCLUSIVE);

    metaPage = BufferGetPage(metaBuffer);
    metaData = BloomPageGetMeta(metaPage);

    /*
     * Iterate over notFullPage array.  Skip page we already tried first.
     */
    nStart = metaData->nStart;
    if (metaData->nEnd > nStart &&
        blkno == metaData->notFullPage[nStart]) {
        nStart++;
    }

    /*
     * This loop iterates for each page we try from the notFullPage array, and
     * will also initialize a GenericXLogState for the fallback case of having
     * to allocate a new page.
     */
    for (;;) {
        state = GenericXLogStart(index);

        /* get modifiable copy of metapage */
        metaPage = GenericXLogRegisterBuffer(state, metaBuffer, 0);
        metaData = BloomPageGetMeta(metaPage);
        if (nStart >= metaData->nEnd) {
            break;                /* no more entries in notFullPage array */
        }

        blkno = metaData->notFullPage[nStart];
        Assert(blkno != InvalidBlockNumber);
        buffer = ReadBuffer(index, blkno);
        LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
        page = GenericXLogRegisterBuffer(state, buffer, 0);
        /* Basically same logic as above */
        if (PageIsNew(page) || BloomPageIsDeleted(page))
            BloomInitPage(page, 0);

        if (BloomPageAddItem(&blstate, page, itup)) {
            /* Success!  Apply the changes, clean up, and exit */
            pfree_ext(itup);
            metaData->nStart = nStart;
            GenericXLogFinish(state);
            UnlockReleaseBuffer(buffer);
            UnlockReleaseBuffer(metaBuffer);
            MemoryContextSwitchTo(oldCtx);
            MemoryContextDelete(insertCtx);
            return false;
        }

        /* Didn't fit, must try other pages */
        GenericXLogAbort(state);
        UnlockReleaseBuffer(buffer);
        nStart++;
    }

    /*
     * Didn't find place to insert in notFullPage array.  Allocate new page.
     * (XXX is it good to do this while holding ex-lock on the metapage??)
     */
    buffer = BloomNewBuffer(index);
    page = GenericXLogRegisterBuffer(state, buffer, GENERIC_XLOG_FULL_IMAGE);
    BloomInitPage(page, 0);

    if (!BloomPageAddItem(&blstate, page, itup)) {
        /* We shouldn't be here since we're inserting to the empty page */
        elog(ERROR, "could not add new bloom tuple to empty page");
    }

    /* Reset notFullPage array to contain just this new page */
    metaData->nStart = 0;
    metaData->nEnd = 1;
    metaData->notFullPage[0] = BufferGetBlockNumber(buffer);

    pfree_ext(itup);
    /* Apply the changes, clean up, and exit */
    GenericXLogFinish(state);

    UnlockReleaseBuffer(buffer);
    UnlockReleaseBuffer(metaBuffer);

    MemoryContextSwitchTo(oldCtx);
    MemoryContextDelete(insertCtx);

    return false;
}
