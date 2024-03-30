/*-------------------------------------------------------------------------
 *
 * pg_buffercache_pages.c
 *	  display some contents of the buffer cache
 *
 *	  contrib/pg_buffercache/pg_buffercache_pages.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "catalog/pg_type.h"
#include "funcapi.h"
#include "storage/buf/buf_internals.h"
#include "storage/buf/bufmgr.h"

#define NUM_BUFFERCACHE_PAGES_ELEM 9

PG_MODULE_MAGIC;

Datum pg_buffercache_pages(PG_FUNCTION_ARGS);

/*
 * Record structure holding the to be exposed cache data.
 */
typedef struct {
    uint32 bufferid;
    Oid relfilenode;
    Oid reltablespace;
    Oid reldatabase;
    ForkNumber forknum;
    BlockNumber blocknum;
    bool isvalid;
    bool isdirty;
    uint16 usagecount;
} BufferCachePagesRec;

/*
 * Function context for data persisting over repeated calls.
 */
typedef struct {
    TupleDesc tupdesc;
    BufferCachePagesRec* record;
} BufferCachePagesContext;

/*
 * Function returning data from the shared buffer cache - buffer number,
 * relation node/tablespace/database/blocknum and dirty indicator.
 */
PG_FUNCTION_INFO_V1(pg_buffercache_pages);

Datum pg_buffercache_pages(PG_FUNCTION_ARGS)
{
    FuncCallContext* funcctx = NULL;
    Datum result;
    MemoryContext oldcontext;
    BufferCachePagesContext* fctx = NULL; /* User function context. */
    TupleDesc tupledesc;
    HeapTuple tuple;

    if (SRF_IS_FIRSTCALL()) {
        int i;
        BufferDescPadded *bufHdrPadded = NULL;
        uint64 buf_state;

        funcctx = SRF_FIRSTCALL_INIT();

        /* Switch context when allocating stuff to be used in later calls */
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        /* Create a user function context for cross-call persistence */
        fctx = (BufferCachePagesContext*)palloc(sizeof(BufferCachePagesContext));

        /* Construct a tuple descriptor for the result rows. */
        tupledesc = CreateTemplateTupleDesc(NUM_BUFFERCACHE_PAGES_ELEM, false);
        TupleDescInitEntry(tupledesc, (AttrNumber)1, "bufferid", INT4OID, -1, 0);
        TupleDescInitEntry(tupledesc, (AttrNumber)2, "relfilenode", OIDOID, -1, 0);
        TupleDescInitEntry(tupledesc, (AttrNumber)3, "bucketid", INT2OID, -1, 0);
        TupleDescInitEntry(tupledesc, (AttrNumber)4, "reltablespace", OIDOID, -1, 0);
        TupleDescInitEntry(tupledesc, (AttrNumber)5, "reldatabase", OIDOID, -1, 0);
        TupleDescInitEntry(tupledesc, (AttrNumber)6, "relforknumber", INT2OID, -1, 0);
        TupleDescInitEntry(tupledesc, (AttrNumber)7, "relblocknumber", INT8OID, -1, 0);
        TupleDescInitEntry(tupledesc, (AttrNumber)8, "isdirty", BOOLOID, -1, 0);
        TupleDescInitEntry(tupledesc, (AttrNumber)9, "usage_count", INT2OID, -1, 0);

        fctx->tupdesc = BlessTupleDesc(tupledesc);

        /* Allocate g_instance.attr.attr_storage.NBuffers worth of BufferCachePagesRec records. */
        fctx->record =
            (BufferCachePagesRec*)palloc_huge(CurrentMemoryContext,
            sizeof(BufferCachePagesRec) * g_instance.attr.attr_storage.NBuffers);

        /* Set max calls and remember the user function context. */
        funcctx->max_calls = g_instance.attr.attr_storage.NBuffers;
        funcctx->user_fctx = fctx;

        /* Return to original context when allocating transient memory */
        (void)MemoryContextSwitchTo(oldcontext);

        /*
         * To get a consistent picture of the buffer state, we must lock all
         * partitions of the buffer map.  Needless to say, this is horrible
         * for concurrency.  Must grab locks in increasing order to avoid
         * possible deadlocks.
         */
        for (i = 0; i < NUM_BUFFER_PARTITIONS; i++)
            (void)LWLockAcquire(GetMainLWLockByIndex(FirstBufMappingLock + i), LW_SHARED);

        /*
         * Scan though all the buffers, saving the relevant fields in the
         * fctx->record structure.
         */
        for (i = 0, bufHdrPadded = t_thrd.storage_cxt.BufferDescriptors; i < g_instance.attr.attr_storage.NBuffers;
             i++, bufHdrPadded++) {
            BufferDesc *bufHdr = &bufHdrPadded->bufferdesc;
            /* Lock each buffer header before inspecting. */
            buf_state = LockBufHdr(bufHdr);

            fctx->record[i].bufferid = BufferDescriptorGetBuffer(bufHdr);
            fctx->record[i].relfilenode = bufHdr->tag.rnode.relNode;
            fctx->record[i].bucketnode = bufHdr->tag.rnode.bucketNode;			
            fctx->record[i].reltablespace = bufHdr->tag.rnode.spcNode;
            fctx->record[i].reldatabase = bufHdr->tag.rnode.dbNode;
            fctx->record[i].forknum = bufHdr->tag.forkNum;
            fctx->record[i].blocknum = bufHdr->tag.blockNum;
            fctx->record[i].usagecount = BUF_STATE_GET_USAGECOUNT(buf_state);

            if (buf_state & BM_DIRTY)
                fctx->record[i].isdirty = true;
            else
                fctx->record[i].isdirty = false;

            /* Note if the buffer is valid, and has storage created */
            if ((buf_state & BM_VALID) && (buf_state & BM_TAG_VALID))
                fctx->record[i].isvalid = true;
            else
                fctx->record[i].isvalid = false;

            UnlockBufHdr(bufHdr, buf_state);
        }

        /*
         * And release locks.  We do this in reverse order for two reasons:
         * (1) Anyone else who needs more than one of the locks will be trying
         * to lock them in increasing order; we don't want to release the
         * other process until it can get all the locks it needs. (2) This
         * avoids O(N^2) behavior inside LWLockRelease.
         */
        for (i = NUM_BUFFER_PARTITIONS; --i >= 0;)
            LWLockRelease(GetMainLWLockByIndex(FirstBufMappingLock + i));
    }

    funcctx = SRF_PERCALL_SETUP();

    /* Get the saved state */
    fctx = (BufferCachePagesContext*)funcctx->user_fctx;

    if (funcctx->call_cntr < funcctx->max_calls) {
        uint32 i = funcctx->call_cntr;
        Datum values[NUM_BUFFERCACHE_PAGES_ELEM];
        bool nulls[NUM_BUFFERCACHE_PAGES_ELEM];

        values[0] = Int32GetDatum(fctx->record[i].bufferid);
        nulls[0] = false;

        /*
         * Set all fields except the bufferid to null if the buffer is unused
         * or not valid.
         */
        if (fctx->record[i].blocknum == InvalidBlockNumber || fctx->record[i].isvalid == false) {
            nulls[1] = true;
            nulls[2] = true;
            nulls[3] = true;
            nulls[4] = true;
            nulls[5] = true;
            nulls[6] = true;
            nulls[7] = true;
            nulls[8] = true;
        } else {
            values[1] = ObjectIdGetDatum(fctx->record[i].relfilenode);
            nulls[1] = false;
            values[2] = Int16GetDatum(fctx->record[i].relfilenode);
            nulls[2] = false;
            values[3] = ObjectIdGetDatum(fctx->record[i].reltablespace);
            nulls[3] = false;
            values[4] = ObjectIdGetDatum(fctx->record[i].reldatabase);
            nulls[4] = false;
            values[5] = ObjectIdGetDatum(fctx->record[i].forknum);
            nulls[5] = false;
            values[6] = Int64GetDatum((int64)fctx->record[i].blocknum);
            nulls[6] = false;
            values[7] = BoolGetDatum(fctx->record[i].isdirty);
            nulls[7] = false;
            values[8] = Int16GetDatum(fctx->record[i].usagecount);
            nulls[8] = false;
        }

        /* Build and return the tuple. */
        tuple = heap_form_tuple(fctx->tupdesc, values, nulls);
        result = HeapTupleGetDatum(tuple);

        SRF_RETURN_NEXT(funcctx, result);
    } else
        SRF_RETURN_DONE(funcctx);
}
