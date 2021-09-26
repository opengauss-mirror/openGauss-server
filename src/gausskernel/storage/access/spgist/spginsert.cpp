/* -------------------------------------------------------------------------
 *
 * spginsert.cpp
 *	  Externally visible index creation/insertion routines
 *
 * All the actual insertion logic is in spgdoinsert.c.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *			src/gausskernel/storage/access/spgist/spginsert.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/genam.h"
#include "access/spgist_private.h"
#include "access/tableam.h"
#include "utils/rel_gs.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "catalog/index.h"
#include "miscadmin.h"
#include "storage/buf/bufmgr.h"
#include "storage/smgr/smgr.h"
#include "utils/aiomem.h"
#include "utils/memutils.h"

typedef struct {
    SpGistState spgstate; /* SPGiST's working state */
    MemoryContext tmpCtx; /* per-tuple temporary context */
} SpGistBuildState;

/* Callback to process one heap tuple during IndexBuildHeapScan */
static void spgistBuildCallback(Relation index, HeapTuple htup, Datum *values, const bool *isnull, bool tupleIsAlive,
                                void *state)
{
    SpGistBuildState *buildstate = (SpGistBuildState *)state;
    MemoryContext oldCtx;

    /* Work in temp context, and reset it after each tuple */
    oldCtx = MemoryContextSwitchTo(buildstate->tmpCtx);

    spgdoinsert(index, &buildstate->spgstate, &htup->t_self, *values, *isnull);

    (void)MemoryContextSwitchTo(oldCtx);
    MemoryContextReset(buildstate->tmpCtx);
}

/*
 * Build an SP-GiST index.
 */
Datum spgbuild(PG_FUNCTION_ARGS)
{
    Relation heap = (Relation)PG_GETARG_POINTER(0);
    Relation index = (Relation)PG_GETARG_POINTER(1);
    IndexInfo *indexInfo = (IndexInfo *)PG_GETARG_POINTER(2);
    IndexBuildResult *result = NULL;
    double reltuples;
    SpGistBuildState buildstate;
    Buffer metabuffer, rootbuffer, nullbuffer;

    if (RelationGetNumberOfBlocks(index) != 0)
        ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
                        errmsg("index \"%s\" already contains data", RelationGetRelationName(index))));

    /*
     * Initialize the meta page and root pages
     */
    metabuffer = SpGistNewBuffer(index);
    rootbuffer = SpGistNewBuffer(index);
    nullbuffer = SpGistNewBuffer(index);

    Assert(BufferGetBlockNumber(metabuffer) == SPGIST_METAPAGE_BLKNO);
    Assert(BufferGetBlockNumber(rootbuffer) == SPGIST_ROOT_BLKNO);
    Assert(BufferGetBlockNumber(nullbuffer) == SPGIST_NULL_BLKNO);

    START_CRIT_SECTION();

    SpGistInitMetapage(BufferGetPage(metabuffer));
    MarkBufferDirty(metabuffer);
    SpGistInitBuffer(rootbuffer, SPGIST_LEAF);
    MarkBufferDirty(rootbuffer);
    SpGistInitBuffer(nullbuffer, SPGIST_LEAF | SPGIST_NULLS);
    MarkBufferDirty(nullbuffer);

    if (RelationNeedsWAL(index)) {
        XLogRecPtr recptr;

        XLogBeginInsert();

        /*
         * Replay will re-initialize the pages, so don't take full pages
         * images.  No other data to log.
         */
        XLogRegisterBuffer(0, metabuffer, REGBUF_WILL_INIT);
        XLogRegisterBuffer(1, rootbuffer, REGBUF_WILL_INIT | REGBUF_STANDARD);
        XLogRegisterBuffer(2, nullbuffer, REGBUF_WILL_INIT | REGBUF_STANDARD);

        recptr = XLogInsert(RM_SPGIST_ID, XLOG_SPGIST_CREATE_INDEX);

        PageSetLSN(BufferGetPage(metabuffer), recptr);
        PageSetLSN(BufferGetPage(rootbuffer), recptr);
        PageSetLSN(BufferGetPage(nullbuffer), recptr);
    }

    END_CRIT_SECTION();

    UnlockReleaseBuffer(metabuffer);
    UnlockReleaseBuffer(rootbuffer);
    UnlockReleaseBuffer(nullbuffer);

    /*
     * Now insert all the heap data into the index
     */
    initSpGistState(&buildstate.spgstate, index);
    buildstate.spgstate.isBuild = true;

    buildstate.tmpCtx = AllocSetContextCreate(CurrentMemoryContext, "SP-GiST build temporary context",
                                              ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE,
                                              ALLOCSET_DEFAULT_MAXSIZE);

    reltuples = tableam_index_build_scan(heap, index, indexInfo, true, spgistBuildCallback, (void*)&buildstate, NULL);

    MemoryContextDelete(buildstate.tmpCtx);

    SpGistUpdateMetaPage(index);

    result = (IndexBuildResult *)palloc0(sizeof(IndexBuildResult));
    result->heap_tuples = result->index_tuples = reltuples;

    PG_RETURN_POINTER(result);
}

/*
 * Build an empty SPGiST index in the initialization fork
 */
Datum spgbuildempty(PG_FUNCTION_ARGS)
{
    Relation index = (Relation)PG_GETARG_POINTER(0);
    Page page;

    /* Construct metapage. */
    ADIO_RUN()
    {
        page = (Page)adio_align_alloc(BLCKSZ);
    }
    ADIO_ELSE()
    {
        page = (Page)palloc(BLCKSZ);
    }
    ADIO_END();

    if (page == NULL) {
        ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NULL_VALUE), errmsg("variable page should not be NULL")));
    }

    SpGistInitMetapage(page);

    /*
     * Write the page and log it unconditionally.  This is important
     * particularly for indexes created on tablespaces and databases
     * whose creation happened after the last redo pointer as recovery
     * removes any of their existing content when the corresponding
     * create records are replayed.
     */
    PageSetChecksumInplace(page, SPGIST_METAPAGE_BLKNO);
    smgrwrite(index->rd_smgr, INIT_FORKNUM, SPGIST_METAPAGE_BLKNO, (char *)page, true);
    log_newpage(&index->rd_smgr->smgr_rnode.node, INIT_FORKNUM, SPGIST_METAPAGE_BLKNO, page, false);

    /* Likewise for the root page. */
    SpGistInitPage(page, SPGIST_LEAF);

    PageSetChecksumInplace(page, SPGIST_ROOT_BLKNO);
    smgrwrite(index->rd_smgr, INIT_FORKNUM, SPGIST_ROOT_BLKNO, (char *)page, true);
    log_newpage(&index->rd_smgr->smgr_rnode.node, INIT_FORKNUM, SPGIST_ROOT_BLKNO, page, true);

    /* Likewise for the null-tuples root page. */
    SpGistInitPage(page, SPGIST_LEAF | SPGIST_NULLS);

    PageSetChecksumInplace(page, SPGIST_NULL_BLKNO);
    smgrwrite(index->rd_smgr, INIT_FORKNUM, SPGIST_NULL_BLKNO, (char *)page, true);
    log_newpage(&index->rd_smgr->smgr_rnode.node, INIT_FORKNUM, SPGIST_NULL_BLKNO, page, true);

    /*
     * An immediate sync is required even if we xlog'd the pages, because the
     * writes did not go through shared buffers and therefore a concurrent
     * checkpoint may have moved the redo pointer past our xlog record.
     */
    smgrimmedsync(index->rd_smgr, INIT_FORKNUM);

    ADIO_RUN()
    {
        adio_align_free(page);
    }
    ADIO_ELSE()
    {
        pfree(page);
    }
    ADIO_END();

    PG_RETURN_VOID();
}

/*
 * Insert one new tuple into an SPGiST index.
 */
Datum spginsert(PG_FUNCTION_ARGS)
{
    Relation index = (Relation)PG_GETARG_POINTER(0);
    Datum *values = (Datum *)PG_GETARG_POINTER(1);
    bool *isnull = (bool *)PG_GETARG_POINTER(2);
    ItemPointer ht_ctid = (ItemPointer)PG_GETARG_POINTER(3);

#ifdef NOT_USED
    Relation heapRel = (Relation)PG_GETARG_POINTER(4);
    IndexUniqueCheck checkUnique = (IndexUniqueCheck)PG_GETARG_INT32(5);
#endif
    SpGistState spgstate;
    MemoryContext oldCtx;
    MemoryContext insertCtx;

    insertCtx = AllocSetContextCreate(CurrentMemoryContext, "SP-GiST insert temporary context",
                                      ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);
    oldCtx = MemoryContextSwitchTo(insertCtx);

    initSpGistState(&spgstate, index);

    spgdoinsert(index, &spgstate, ht_ctid, *values, *isnull);

    SpGistUpdateMetaPage(index);

    (void)MemoryContextSwitchTo(oldCtx);
    MemoryContextDelete(insertCtx);

    /* return false since we've not done any unique check */
    PG_RETURN_BOOL(false);
}

Datum spgmerge(PG_FUNCTION_ARGS)
{
    IndexBuildResult *result = NULL;

    ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED), errmsg("spgmerge: unimplemented")));
    PG_RETURN_POINTER(result);
}