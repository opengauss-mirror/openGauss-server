/* -------------------------------------------------------------------------
 *
 * tqueue.c
 * 	  Use shm_mq to send & receive tuples between parallel backends
 *
 * A DestReceiver of type DestTupleQueue, which is a TQueueDestReceiver
 * under the hood, writes tuples from the executor to a shm_mq.
 *
 * A TupleQueueReader reads tuples from a shm_mq and returns the tuples.
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 * 	  src/backend/executor/tqueue.c
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/htup.h"
#include "catalog/pg_type.h"
#include "executor/tqueue.h"
#include "funcapi.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "utils/array.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rangetypes.h"
#include "utils/syscache.h"
#include "utils/typcache.h"

typedef enum {
    TQUEUE_REMAP_NONE,  /* no special processing required */
    TQUEUE_REMAP_ARRAY, /* array */
    TQUEUE_REMAP_RANGE, /* range */
    TQUEUE_REMAP_RECORD /* composite type, named or anonymous */
} RemapClass;

typedef struct {
    int natts;
    RemapClass mapping[FLEXIBLE_ARRAY_MEMBER];
} RemapInfo;

typedef struct {
    DestReceiver pub;
    shm_mq_handle *handle;
    MemoryContext tmpcontext;
    HTAB *recordhtab;
    char mode;
    TupleDesc tupledesc;
    RemapInfo *remapinfo;
} TQueueDestReceiver;

typedef struct RecordTypemodMap {
    int remotetypmod;
    int localtypmod;
} RecordTypemodMap;

struct TupleQueueReader {
    shm_mq_handle *queue;
    char mode;
    TupleDesc tupledesc;
    RemapInfo *remapinfo;
    HTAB *typmodmap;
};

#define TUPLE_QUEUE_MODE_CONTROL 'c'
#define TUPLE_QUEUE_MODE_DATA 'd'
#define CHECK_MESSAGE_LEN(remainLen, pos, readSize) \
    do {\
        if (unlikely((remainLen) < (readSize))) {\
            ereport(ERROR, (errmsg("Invalid message length")));\
        }\
        (remainLen) -= (readSize);\
        (pos) += (readSize);\
    } while (0)

static void tqueueWalk(TQueueDestReceiver *tqueue, RemapClass walktype, Datum value);
static void tqueueWalkRecord(TQueueDestReceiver *tqueue, Datum value);
static void tqueueWalkArray(TQueueDestReceiver *tqueue, Datum value);
static void tqueueWalkRange(TQueueDestReceiver *tqueue, Datum value);
static void tqueueSendTypmodInfo(TQueueDestReceiver *tqueue, int typmod, TupleDesc tupledesc);
static void TupleQueueHandleControlMessage(TupleQueueReader *reader, Size nbytes, char *data);
static HeapTuple TupleQueueHandleDataMessage(TupleQueueReader *reader, Size nbytes, HeapTupleHeader data);
static HeapTuple TupleQueueRemapTuple(TupleQueueReader *reader, TupleDesc tupledesc, RemapInfo *remapinfo,
    HeapTuple tuple);
static Datum TupleQueueRemap(TupleQueueReader *reader, RemapClass remapclass, Datum value);
static Datum TupleQueueRemapArray(TupleQueueReader *reader, Datum value);
static Datum TupleQueueRemapRange(TupleQueueReader *reader, Datum value);
static Datum TupleQueueRemapRecord(TupleQueueReader *reader, Datum value);
static RemapClass GetRemapClass(Oid type_id);
static RemapInfo *BuildRemapInfo(TupleDesc tupledesc);


/*
 * Receive a tuple from a query, and send it to the designated shm_mq.
 *
 * Returns true if successful, false if shm_mq has been detached.
 */
static void tqueueReceiveSlot(TupleTableSlot *slot, DestReceiver *self)
{
    TQueueDestReceiver *tqueue = (TQueueDestReceiver *)self;
    TupleDesc tupledesc = slot->tts_tupleDescriptor;

    /*
     * Test to see whether the tupledesc has changed; if so, set up for the
     * new tupledesc.  This is a strange test both because the executor really
     * shouldn't change the tupledesc, and also because it would be unsafe if
     * the old tupledesc could be freed and a new one allocated at the same
     * address.  But since some very old code in printtup.c uses a similar
     * test, we adopt it here as well.
     */
    if (tqueue->tupledesc != tupledesc) {
        if (tqueue->remapinfo != NULL)
            pfree(tqueue->remapinfo);
        tqueue->remapinfo = BuildRemapInfo(tupledesc);
        tqueue->tupledesc = tupledesc;
    }

    HeapTuple tuple = ExecMaterializeSlot(slot);

    /*
     * When, because of the types being transmitted, no record typemod mapping
     * can be needed, we can skip a good deal of work.
     */
    if (tqueue->remapinfo != NULL) {
        RemapInfo *remapinfo = tqueue->remapinfo;
        MemoryContext oldcontext = NULL;

        /* Deform the tuple so we can examine it, if not done already. */
        slot_getallattrs(slot);

        /* Iterate over each attribute and search it for transient typemods. */
        Assert(slot->tts_tupleDescriptor->natts == remapinfo->natts);
        for (AttrNumber i = 0; i < remapinfo->natts; ++i) {
            /* Ignore nulls and types that don't need special handling. */
            if (slot->tts_isnull[i] || remapinfo->mapping[i] == TQUEUE_REMAP_NONE)
                continue;

            /* Switch to temporary memory context to avoid leaking. */
            if (oldcontext == NULL) {
                if (tqueue->tmpcontext == NULL)
                    tqueue->tmpcontext = AllocSetContextCreate(TopMemoryContext, "tqueue temporary context",
                        ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);
                oldcontext = MemoryContextSwitchTo(tqueue->tmpcontext);
            }

            /* Invoke the appropriate walker function. */
            tqueueWalk(tqueue, remapinfo->mapping[i], slot->tts_values[i]);
        }

        /* If we used the temp context, reset it and restore prior context. */
        if (oldcontext != NULL) {
            (void)MemoryContextSwitchTo(oldcontext);
            MemoryContextReset(tqueue->tmpcontext);
        }

        /* If we entered control mode, switch back to data mode. */
        if (tqueue->mode != TUPLE_QUEUE_MODE_DATA) {
            tqueue->mode = TUPLE_QUEUE_MODE_DATA;
            (void)shm_mq_send(tqueue->handle, sizeof(char), &tqueue->mode, false);
        }
    }

    /* Send the tuple itself. */
    (void)shm_mq_send(tqueue->handle, tuple->t_len, tuple->t_data, false);
}

/*
 * Invoke the appropriate walker function based on the given RemapClass.
 */
static void tqueueWalk(TQueueDestReceiver *tqueue, RemapClass walktype, Datum value)
{
    check_stack_depth();

    switch (walktype) {
        case TQUEUE_REMAP_NONE:
            break;
        case TQUEUE_REMAP_ARRAY:
            tqueueWalkArray(tqueue, value);
            break;
        case TQUEUE_REMAP_RANGE:
            tqueueWalkRange(tqueue, value);
            break;
        case TQUEUE_REMAP_RECORD:
            tqueueWalkRecord(tqueue, value);
            break;
    }
}

/*
 * Walk a record and send control messages for transient record types
 * contained therein.
 */
static void tqueueWalkRecord(TQueueDestReceiver *tqueue, Datum value)
{
    /* Extract typmod from tuple. */
    HeapTupleHeader tup = DatumGetHeapTupleHeader(value);
    Oid type_id = HeapTupleHeaderGetTypeId(tup);
    int32 typmod = HeapTupleHeaderGetTypMod(tup);

    /* Look up tuple descriptor in typecache. */
    TupleDesc tupledesc = lookup_rowtype_tupdesc(type_id, typmod);

    /*
     * If this is a transient record time, send its TupleDesc as a control
     * message.  (tqueueSendTypemodInfo is smart enough to do this only once
     * per typmod.)
     */
    if (type_id == RECORDOID)
        tqueueSendTypmodInfo(tqueue, typmod, tupledesc);

    /*
     * Build the remap information for this tupledesc.  We might want to think
     * about keeping a cache of this information keyed by typeid and typemod,
     * but let's keep it simple for now.
     */
    RemapInfo *remapinfo = BuildRemapInfo(tupledesc);

    /*
     * If remapping is required, deform the tuple and process each field. When
     * BuildRemapInfo is null, the data types are such that there can be no
     * transient record types here, so we can skip all this work.
     */
    if (remapinfo != NULL) {
        HeapTupleData tdata;

        /* Deform the tuple so we can check each column within. */
        Datum *values = (Datum *)palloc(tupledesc->natts * sizeof(Datum));
        bool *isnull = (bool *)palloc(tupledesc->natts * sizeof(bool));
        tdata.t_len = HeapTupleHeaderGetDatumLength(tup);
        ItemPointerSetInvalid(&(tdata.t_self));
        tdata.t_tableOid = InvalidOid;
        tdata.t_data = tup;
        heap_deform_tuple(&tdata, tupledesc, values, isnull);

        /* Recursively check each non-NULL attribute. */
        for (AttrNumber i = 0; i < tupledesc->natts; ++i) {
            if (!isnull[i]) {
                tqueueWalk(tqueue, remapinfo->mapping[i], values[i]);
            }
        }
    }

    /* Release reference count acquired by lookup_rowtype_tupdesc. */
    DecrTupleDescRefCount(tupledesc);
}

/*
 * Walk a record and send control messages for transient record types
 * contained therein.
 */
static void tqueueWalkArray(TQueueDestReceiver *tqueue, Datum value)
{
    ArrayType *arr = DatumGetArrayTypeP(value);
    Oid type_id = ARR_ELEMTYPE(arr);
    int16 typlen;
    bool typbyval = false;
    char typalign;
    Datum *elem_values = NULL;
    bool *elem_nulls = NULL;
    int num_elems;
    RemapClass remapclass = GetRemapClass(type_id);

    /*
     * If the elements of the array don't need to be walked, we shouldn't have
     * been called in the first place: GetRemapClass should have returned NULL
     * when asked about this array type.
     */
    Assert(remapclass != TQUEUE_REMAP_NONE);

    /* Deconstruct the array. */
    get_typlenbyvalalign(type_id, &typlen, &typbyval, &typalign);
    deconstruct_array(arr, type_id, typlen, typbyval, typalign, &elem_values, &elem_nulls, &num_elems);

    /* Walk each element. */
    for (int i = 0; i < num_elems; ++i) {
        if (!elem_nulls[i]) {
            tqueueWalk(tqueue, remapclass, elem_values[i]);
        }
    }
}

/*
 * Walk a range type and send control messages for transient record types
 * contained therein.
 */
static void tqueueWalkRange(TQueueDestReceiver *tqueue, Datum value)
{
    RangeType *range = DatumGetRangeType(value);
    Oid type_id = RangeTypeGetOid(range);
    RangeBound lower;
    RangeBound upper;
    bool empty = false;

    /*
     * Extract the lower and upper bounds.  It might be worth implementing
     * some caching scheme here so that we don't look up the same typeids in
     * the type cache repeatedly, but for now let's keep it simple.
     */
    TypeCacheEntry *typcache = lookup_type_cache(type_id, TYPECACHE_RANGE_INFO);
    if (typcache->rngelemtype == NULL)
        ereport(ERROR, (errmsg("type %u is not a range type", type_id)));
    range_deserialize(typcache, range, &lower, &upper, &empty);

    /* Nothing to do for an empty range. */
    if (empty) {
        return;
    }

    /*
     * If the range bounds don't need to be walked, we shouldn't have been
     * called in the first place: GetRemapClass should have returned NULL when
     * asked about this range type.
     */
    RemapClass remapclass = GetRemapClass(type_id);
    Assert(remapclass != TQUEUE_REMAP_NONE);

    /* Walk each bound, if present. */
    if (!upper.infinite)
        tqueueWalk(tqueue, remapclass, upper.val);
    if (!lower.infinite)
        tqueueWalk(tqueue, remapclass, lower.val);
}

/*
 * Send tuple descriptor information for a transient typemod, unless we've
 * already done so previously.
 */
static void tqueueSendTypmodInfo(TQueueDestReceiver *tqueue, int typmod, TupleDesc tupledesc)
{
    StringInfoData buf;
    bool found = false;
    AttrNumber i;

    /* Initialize hash table if not done yet. */
    if (tqueue->recordhtab == NULL) {
        HASHCTL ctl;

        ctl.keysize = sizeof(int);
        ctl.entrysize = sizeof(int);
        ctl.hcxt = TopMemoryContext;
        tqueue->recordhtab = hash_create("tqueue record hashtable", 100, &ctl, HASH_ELEM | HASH_CONTEXT);
    }

    /* Have we already seen this record type?  If not, must report it. */
    (void)hash_search(tqueue->recordhtab, &typmod, HASH_ENTER, &found);
    if (found) {
        return;
    }

    /* If message queue is in data mode, switch to control mode. */
    if (tqueue->mode != TUPLE_QUEUE_MODE_CONTROL) {
        tqueue->mode = TUPLE_QUEUE_MODE_CONTROL;
        (void)shm_mq_send(tqueue->handle, sizeof(char), &tqueue->mode, false);
    }

    /* Assemble a control message. */
    initStringInfo(&buf);
    appendBinaryStringInfo(&buf, (char *)&typmod, sizeof(int));
    appendBinaryStringInfo(&buf, (char *)&tupledesc->natts, sizeof(int));
    appendBinaryStringInfo(&buf, (char *)&tupledesc->tdhasoid, sizeof(bool));
    for (i = 0; i < tupledesc->natts; ++i)
        appendBinaryStringInfo(&buf, (char *)tupledesc->attrs[i], sizeof(FormData_pg_attribute));

    /* Send control message. */
    (void)shm_mq_send(tqueue->handle, buf.len, buf.data, false);
}


/*
 * Prepare to receive tuples from executor.
 */
static void tqueueStartupReceiver(DestReceiver *self, int operation, TupleDesc typeinfo)
{
    /* do nothing */
}

/*
 * Clean up at end of an executor run
 */
static void tqueueShutdownReceiver(DestReceiver *self)
{
    TQueueDestReceiver *tqueue = (TQueueDestReceiver *)self;

    if (tqueue->handle != NULL) {
        shm_mq_detach(tqueue->handle);
        tqueue->handle = NULL;
    }
}

/*
 * Destroy receiver when done with it
 */
static void tqueueDestroyReceiver(DestReceiver *self)
{
    TQueueDestReceiver *tqueue = (TQueueDestReceiver *)self;

    if (tqueue->tmpcontext != NULL)
        MemoryContextDelete(tqueue->tmpcontext);
    if (tqueue->recordhtab != NULL)
        hash_destroy(tqueue->recordhtab);
    if (tqueue->remapinfo != NULL)
        pfree(tqueue->remapinfo);
    pfree(self);
}

/*
 * Create a DestReceiver that writes tuples to a tuple queue.
 */
DestReceiver *CreateTupleQueueDestReceiver(shm_mq_handle *handle)
{
    TQueueDestReceiver *self = (TQueueDestReceiver *)palloc0(sizeof(TQueueDestReceiver));

    self->pub.receiveSlot = tqueueReceiveSlot;
    self->pub.rStartup = tqueueStartupReceiver;
    self->pub.rShutdown = tqueueShutdownReceiver;
    self->pub.rDestroy = tqueueDestroyReceiver;
    self->pub.mydest = DestTupleQueue;
    self->handle = handle;
    self->tmpcontext = NULL;
    self->recordhtab = NULL;
    self->mode = TUPLE_QUEUE_MODE_DATA;
    self->remapinfo = NULL;

    return (DestReceiver *)self;
}

/*
 * Create a tuple queue reader.
 */
TupleQueueReader *CreateTupleQueueReader(shm_mq_handle *handle, TupleDesc tupledesc)
{
    TupleQueueReader *reader = (TupleQueueReader *)palloc0(sizeof(TupleQueueReader));

    reader->queue = handle;
    reader->mode = TUPLE_QUEUE_MODE_DATA;
    reader->tupledesc = tupledesc;
    reader->remapinfo = BuildRemapInfo(tupledesc);

    return reader;
}

/*
 * Destroy a tuple queue reader.
 *
 * Note: cleaning up the underlying shm_mq is the caller's responsibility.
 * We won't access it here, as it may be detached already.
 */
void DestroyTupleQueueReader(TupleQueueReader *reader)
{
    if (reader->remapinfo != NULL) {
        pfree(reader->remapinfo);
    }
    pfree(reader);
}

/*
 * Fetch a tuple from a tuple queue reader.
 *
 * The return value is NULL if there are no remaining tuples or if
 * nowait = true and no tuple is ready to return.  *done, if not NULL,
 * is set to true when there are no remaining tuples and otherwise to false.
 *
 * The returned tuple, if any, is allocated in CurrentMemoryContext.
 * Note that this routine must not leak memory!  (We used to allow that,
 * but not any more.)
 *
 * Even when shm_mq_receive() returns SHM_MQ_WOULD_BLOCK, this can still
 * accumulate bytes from a partially-read message, so it's useful to call
 * this with nowait = true even if nothing is returned.
 */
HeapTuple TupleQueueReaderNext(TupleQueueReader *reader, bool nowait, bool *done)
{
    if (done != NULL)
        *done = false;

    for (;;) {
        Size nbytes;
        void *data = NULL;
        /* Attempt to read a message. */
        shm_mq_result result = shm_mq_receive(reader->queue, &nbytes, &data, nowait);
        /* If queue is detached, set *done and return NULL. */
        if (result == SHM_MQ_DETACHED) {
            if (done != NULL)
                *done = true;
            return NULL;
        }

        /* In non-blocking mode, bail out if no message ready yet. */
        if (result == SHM_MQ_WOULD_BLOCK)
            return NULL;
        Assert(result == SHM_MQ_SUCCESS);

        /*
         * OK, we got a message.  Process it.
         *
         * One-byte messages are mode switch messages, so that we can switch
         * between "control" and "data" mode.  When in "data" mode, each
         * message (unless exactly one byte) is a tuple.  When in "control"
         * mode, each message provides a transient-typmod-to-tupledesc mapping
         * so we can interpret future tuples.
         */
        if (nbytes == 1) {
            /* Mode switch message. */
            reader->mode = ((char *)data)[0];
        } else if (reader->mode == TUPLE_QUEUE_MODE_DATA) {
            /* Tuple data. */
            return TupleQueueHandleDataMessage(reader, nbytes, (HeapTupleHeader)data);
        } else if (reader->mode == TUPLE_QUEUE_MODE_CONTROL) {
            /* Control message, describing a transient record type. */
            TupleQueueHandleControlMessage(reader, nbytes, (char *)data);
        } else {
            ereport(ERROR, (errmsg("invalid mode: %d", (int)reader->mode)));
        }
    }
}

/*
 * Handle a data message - that is, a tuple - from the remote side.
 */
static HeapTuple TupleQueueHandleDataMessage(TupleQueueReader *reader, Size nbytes, HeapTupleHeader data)
{
    HeapTupleData htup;

    ItemPointerSetInvalid(&htup.t_self);
    htup.t_tableOid = InvalidOid;
    htup.t_len = (uint32)nbytes;
    htup.t_data = data;

    return TupleQueueRemapTuple(reader, reader->tupledesc, reader->remapinfo, &htup);
}

/*
 * Remap tuple typmods per control information received from remote side.
 */
static HeapTuple TupleQueueRemapTuple(TupleQueueReader *reader, TupleDesc tupledesc, RemapInfo *remapinfo,
    HeapTuple tuple)
{
    /*
     * If no remapping is necessary, just copy the tuple into a single
     * palloc'd chunk, as caller will expect.
     */
    if (remapinfo == NULL)
        return heap_copytuple(tuple);

    /* Deform tuple so we can remap record typmods for individual attrs. */
    Datum *values = (Datum *)palloc(tupledesc->natts * sizeof(Datum));
    bool *isnull = (bool *)palloc(tupledesc->natts * sizeof(bool));
    heap_deform_tuple(tuple, tupledesc, values, isnull);
    Assert(tupledesc->natts == remapinfo->natts);

    /* Recursively check each non-NULL attribute. */
    for (int i = 0; i < tupledesc->natts; ++i) {
        if (isnull[i] || remapinfo->mapping[i] == TQUEUE_REMAP_NONE)
            continue;
        values[i] = TupleQueueRemap(reader, remapinfo->mapping[i], values[i]);
    }

    /* Reform the modified tuple. */
    return heap_form_tuple(tupledesc, values, isnull);
}

/*
 * Remap a value based on the specified remap class.
 */
static Datum TupleQueueRemap(TupleQueueReader *reader, RemapClass remapclass, Datum value)
{
    check_stack_depth();

    switch (remapclass) {
        case TQUEUE_REMAP_NONE:
            /* caller probably shouldn't have called us at all, but... */
            return value;

        case TQUEUE_REMAP_ARRAY:
            return TupleQueueRemapArray(reader, value);

        case TQUEUE_REMAP_RANGE:
            return TupleQueueRemapRange(reader, value);

        case TQUEUE_REMAP_RECORD:
            return TupleQueueRemapRecord(reader, value);
    }

    ereport(ERROR, (errmsg("unknown remap class: %d", (int)remapclass)));
    return (Datum)0;
}

/*
 * Remap an array.
 */
static Datum TupleQueueRemapArray(TupleQueueReader *reader, Datum value)
{
    ArrayType *arr = DatumGetArrayTypeP(value);
    Oid type_id = ARR_ELEMTYPE(arr);
    int16 typlen;
    bool typbyval;
    char typalign;
    Datum *elem_values = NULL;
    bool *elem_nulls = NULL;
    int num_elems;
    RemapClass remapclass = GetRemapClass(type_id);

    /*
     * If the elements of the array don't need to be walked, we shouldn't have
     * been called in the first place: GetRemapClass should have returned NULL
     * when asked about this array type.
     */
    Assert(remapclass != TQUEUE_REMAP_NONE);

    /* Deconstruct the array. */
    get_typlenbyvalalign(type_id, &typlen, &typbyval, &typalign);
    deconstruct_array(arr, type_id, typlen, typbyval, typalign, &elem_values, &elem_nulls, &num_elems);

    /* Remap each element. */
    for (int i = 0; i < num_elems; ++i) {
        if (!elem_nulls[i]) {
            elem_values[i] = TupleQueueRemap(reader, remapclass, elem_values[i]);
        }
    }

    /* Reconstruct and return the array.  */
    arr = construct_md_array(elem_values, elem_nulls, ARR_NDIM(arr), ARR_DIMS(arr), ARR_LBOUND(arr), type_id, typlen,
        typbyval, typalign);
    return PointerGetDatum(arr);
}

/*
 * Remap a range type.
 */
static Datum TupleQueueRemapRange(TupleQueueReader *reader, Datum value)
{
    RangeType *range = DatumGetRangeType(value);
    Oid type_id = RangeTypeGetOid(range);
    RangeBound lower;
    RangeBound upper;
    bool empty = false;

    /*
     * Extract the lower and upper bounds.  As in tqueueWalkRange, some
     * caching might be a good idea here.
     */
    TypeCacheEntry *typcache = lookup_type_cache(type_id, TYPECACHE_RANGE_INFO);
    if (typcache->rngelemtype == NULL)
        ereport(ERROR, (errmsg("type %u is not a range type", type_id)));
    range_deserialize(typcache, range, &lower, &upper, &empty);

    /* Nothing to do for an empty range. */
    if (empty)
        return value;

    /*
     * If the range bounds don't need to be walked, we shouldn't have been
     * called in the first place: GetRemapClass should have returned NULL when
     * asked about this range type.
     */
    RemapClass remapclass = GetRemapClass(type_id);
    Assert(remapclass != TQUEUE_REMAP_NONE);

    /* Remap each bound, if present. */
    if (!upper.infinite)
        upper.val = TupleQueueRemap(reader, remapclass, upper.val);
    if (!lower.infinite)
        lower.val = TupleQueueRemap(reader, remapclass, lower.val);

    /* And reserialize. */
    range = range_serialize(typcache, &lower, &upper, empty);
    return RangeTypeGetDatum(range);
}

/*
 * Remap a record.
 */
static Datum TupleQueueRemapRecord(TupleQueueReader *reader, Datum value)
{
    HeapTupleData htup;

    /* Fetch type OID and typemod. */
    HeapTupleHeader tup = DatumGetHeapTupleHeader(value);
    Oid type_id = HeapTupleHeaderGetTypeId(tup);
    int typmod = HeapTupleHeaderGetTypMod(tup);

    /* If transient record, replace remote typmod with local typmod. */
    if (type_id == RECORDOID) {
        Assert(reader->typmodmap != NULL);
        RecordTypemodMap *mapent = (RecordTypemodMap *)hash_search(reader->typmodmap, &typmod, HASH_FIND, NULL);
        if (mapent == NULL)
            ereport(ERROR, (errmsg("found unrecognized remote typmod %d", typmod)));
        typmod = mapent->localtypmod;
    }

    /*
     * Fetch tupledesc and compute remap info.  We should probably cache this
     * so that we don't have to keep recomputing it.
     */
    TupleDesc tupledesc = lookup_rowtype_tupdesc(type_id, typmod);
    RemapInfo *remapinfo = BuildRemapInfo(tupledesc);
    DecrTupleDescRefCount(tupledesc);

    /* Remap tuple. */
    ItemPointerSetInvalid(&htup.t_self);
    htup.t_tableOid = InvalidOid;
    htup.t_len = HeapTupleHeaderGetDatumLength(tup);
    htup.t_data = tup;
    HeapTuple atup = TupleQueueRemapTuple(reader, tupledesc, remapinfo, &htup);
    HeapTupleHeaderSetTypeId(atup->t_data, type_id);
    HeapTupleHeaderSetTypMod(atup->t_data, typmod);
    HeapTupleHeaderSetDatumLength(atup->t_data, htup.t_len);

    /* And return the results. */
    return HeapTupleGetDatum(atup);
}

/*
 * Handle a control message from the tuple queue reader.
 *
 * Control messages are sent when the remote side is sending tuples that
 * contain transient record types.  We need to arrange to bless those
 * record types locally and translate between remote and local typmods.
 */
static void TupleQueueHandleControlMessage(TupleQueueReader *reader, Size nbytes, char *data)
{
    Size rc = 0;
    bool found = false;

    /* Extract remote typmod. */
    int remotetypmod = (int)data[rc];
    CHECK_MESSAGE_LEN(nbytes, rc, sizeof(int));

    /* Extract attribute count. */
    int natts = (int)data[rc];
    CHECK_MESSAGE_LEN(nbytes, rc, sizeof(int));

    /* Extract hasoid flag. */
    bool hasoid = (bool)data[rc];
    CHECK_MESSAGE_LEN(nbytes, rc, sizeof(bool));

    /* Extract attribute details. */
    MemoryContext oldcontext = MemoryContextSwitchTo(t_thrd.mem_cxt.cur_transaction_mem_cxt);
    Form_pg_attribute *attrs = (Form_pg_attribute *)palloc(natts * sizeof(Form_pg_attribute));
    for (int i = 0; i < natts; ++i) {
        attrs[i] = (Form_pg_attribute)palloc(sizeof(FormData_pg_attribute));
        int errorno = memcpy_s(attrs[i], sizeof(FormData_pg_attribute), &data[rc], sizeof(FormData_pg_attribute));
        securec_check_c(errorno, "", "");
        CHECK_MESSAGE_LEN(nbytes, rc, sizeof(FormData_pg_attribute));
    }
    (void)MemoryContextSwitchTo(oldcontext);

    /* We should have read the whole message. */
    Assert(rc == nbytes);

    /* Construct TupleDesc. */
    TupleDesc tupledesc = CreateTupleDesc(natts, hasoid, attrs);
    tupledesc = BlessTupleDesc(tupledesc);

    /* Create map if it doesn't exist already. */
    if (reader->typmodmap == NULL) {
        HASHCTL ctl;

        ctl.keysize = sizeof(int);
        ctl.entrysize = sizeof(RecordTypemodMap);
        ctl.hcxt = t_thrd.mem_cxt.cur_transaction_mem_cxt;
        reader->typmodmap = hash_create("typmodmap hashtable", 100, &ctl, HASH_ELEM | HASH_CONTEXT);
    }

    /* Create map entry. */
    RecordTypemodMap *mapent = (RecordTypemodMap *)hash_search(reader->typmodmap, &remotetypmod, HASH_ENTER, &found);
    if (found) {
        ereport(ERROR, (errmsg("duplicate message for typmod %d", remotetypmod)));
    }
    mapent->localtypmod = tupledesc->tdtypmod;
    ereport(DEBUG3, (errmsg("mapping remote typmod %d to local typmod %d", remotetypmod, tupledesc->tdtypmod)));
}

/*
 * Build a mapping indicating what remapping class applies to each attribute
 * described by a tupledesc.
 */
static RemapInfo *BuildRemapInfo(TupleDesc tupledesc)
{
    Size size;
    AttrNumber i;
    bool noop = true;

    size = offsetof(RemapInfo, mapping) + sizeof(RemapClass) * tupledesc->natts;
    RemapInfo *remapinfo = (RemapInfo *)MemoryContextAllocZero(TopMemoryContext, size);
    remapinfo->natts = tupledesc->natts;
    for (i = 0; i < tupledesc->natts; ++i) {
        Form_pg_attribute attr = tupledesc->attrs[i];

        if (attr->attisdropped) {
            remapinfo->mapping[i] = TQUEUE_REMAP_NONE;
            continue;
        }

        remapinfo->mapping[i] = GetRemapClass(attr->atttypid);
        if (remapinfo->mapping[i] != TQUEUE_REMAP_NONE)
            noop = false;
    }

    if (noop) {
        pfree(remapinfo);
        remapinfo = NULL;
    }

    return remapinfo;
}

/*
 * Determine the remap class assocociated with a particular data type.
 *
 * Transient record types need to have the typmod applied on the sending side
 * replaced with a value on the receiving side that has the same meaning.
 *
 * Arrays, range types, and all record types (including named composite types)
 * need to searched for transient record values buried within them.
 * Surprisingly, a walker is required even when the indicated type is a
 * composite type, because the actual value may be a compatible transient
 * record type.
 */
static RemapClass GetRemapClass(Oid type_id)
{
    RemapClass forceResult = TQUEUE_REMAP_NONE;
    RemapClass innerResult = TQUEUE_REMAP_NONE;

    for (;;) {
        /* Simple cases. */
        if (type_id == RECORDOID) {
            innerResult = TQUEUE_REMAP_RECORD;
            break;
        }
        if (type_id == RECORDARRAYOID) {
            innerResult = TQUEUE_REMAP_ARRAY;
            break;
        }

        /* Otherwise, we need a syscache lookup to figure it out. */
        HeapTuple tup = SearchSysCache1((int)TYPEOID, ObjectIdGetDatum(type_id));
        if (!HeapTupleIsValid(tup))
            ereport(ERROR, (errmsg("cache lookup failed for type %u", type_id)));
        Form_pg_type typ = (Form_pg_type)GETSTRUCT(tup);
        /* Look through domains to underlying base type. */
        if (typ->typtype == TYPTYPE_DOMAIN) {
            type_id = typ->typbasetype;
            ReleaseSysCache(tup);
            continue;
        }

        /*
         * Look through arrays to underlying base type, but the final return
         * value must be either TQUEUE_REMAP_ARRAY or TQUEUE_REMAP_NONE.  (If
         * this is an array of integers, for example, we don't need to walk
         * it.)
         */
        if (OidIsValid(typ->typelem) && typ->typlen == -1) {
            type_id = typ->typelem;
            ReleaseSysCache(tup);
            if (forceResult == TQUEUE_REMAP_NONE) {
                forceResult = TQUEUE_REMAP_ARRAY;
            }
            continue;
        }

        /*
         * Similarly, look through ranges to the underlying base type, but the
         * final return value must be either TQUEUE_REMAP_RANGE or
         * TQUEUE_REMAP_NONE.
         */
        if (typ->typtype == TYPTYPE_RANGE) {
            ReleaseSysCache(tup);
            if (forceResult == TQUEUE_REMAP_NONE) {
                forceResult = TQUEUE_REMAP_RANGE;
            }
            type_id = get_range_subtype(type_id);
            continue;
        }

        /* Walk composite types.  Nothing else needs special handling. */
        if (typ->typtype == TYPTYPE_COMPOSITE) {
            innerResult = TQUEUE_REMAP_RECORD;
        }
        ReleaseSysCache(tup);
        break;
    }

    if (innerResult != TQUEUE_REMAP_NONE && forceResult != TQUEUE_REMAP_NONE) {
        return forceResult;
    }
    return innerResult;
}

