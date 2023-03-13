/* -------------------------------------------------------------------------
 *
 * tstoreReceiver.cpp
 * 	  An implementation of DestReceiver that stores the result tuples in
 * 	  a Tuplestore.
 *
 * Optionally, we can force detoasting (but not decompression) of out-of-line
 * toasted values.	This is to support cursors WITH HOLD, which must retain
 * data even if the underlying table is dropped.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 * 	  src/gausskernel/runtime/executor/tstoreReceiver.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/tuptoaster.h"
#include "access/tableam.h"
#include "executor/tstoreReceiver.h"

typedef struct {
    DestReceiver pub;
    /* parameters: */
    Tuplestorestate *tstore; /* where to put the data */
    MemoryContext cxt;       /* context containing tstore */
    bool detoast;            /* were we told to detoast? */
    /* workspace: */
    Datum *outvalues; /* values array for result tuple */
    Datum *tofree;    /* temp values to be pfree'd */
} TStoreState;

static void tstoreReceiveSlot_notoast(TupleTableSlot *slot, DestReceiver *self);
static void tstoreReceiveSlot_detoast(TupleTableSlot *slot, DestReceiver *self);

/*
 * Prepare to receive tuples from executor.
 */
static void tstoreStartupReceiver(DestReceiver *self, int operation, TupleDesc typeinfo)
{
    TStoreState *my_stat = (TStoreState *)self;
    bool need_toast = false;
    FormData_pg_attribute *attrs = typeinfo->attrs;
    int natts = typeinfo->natts;
    int i;

    /* Check if any columns require detoast work */
    if (my_stat->detoast) {
        for (i = 0; i < natts; i++) {
            if (attrs[i].attisdropped) {
                continue;
            }
            if (attrs[i].attlen == -1) {
                need_toast = true;
                break;
            }
        }
    }

    /* Set up appropriate callback */
    if (need_toast) {
        my_stat->pub.receiveSlot = tstoreReceiveSlot_detoast;
        /* Create workspace */
        my_stat->outvalues = (Datum *)MemoryContextAlloc(my_stat->cxt, natts * sizeof(Datum));
        my_stat->tofree = (Datum *)MemoryContextAlloc(my_stat->cxt, natts * sizeof(Datum));
    } else {
        my_stat->pub.receiveSlot = tstoreReceiveSlot_notoast;
        my_stat->outvalues = NULL;
        my_stat->tofree = NULL;
    }
}

/*
 * Receive a tuple from the executor and store it in the tuplestore.
 * This is for the easy case where we don't have to detoast.
 */
static void tstoreReceiveSlot_notoast(TupleTableSlot *slot, DestReceiver *self)
{
    TStoreState *my_stat = (TStoreState *)self;
    tuplestore_puttupleslot(my_stat->tstore, slot);
}

/*
 * Receive a tuple from the executor and store it in the tuplestore.
 * This is for the case where we have to detoast any toasted values.
 */
static void tstoreReceiveSlot_detoast(TupleTableSlot *slot, DestReceiver *self)
{
    TStoreState *my_stat = (TStoreState *)self;
    TupleDesc typeinfo = slot->tts_tupleDescriptor;
    FormData_pg_attribute *attrs = typeinfo->attrs;
    int natts = typeinfo->natts;
    int nfree;
    int i;
    MemoryContext oldcxt;

    /* Make sure the tuple is fully deconstructed */

    /* Get the Table Accessor Method*/
    Assert(slot != NULL && slot->tts_tupleDescriptor != NULL);

    tableam_tslot_getallattrs(slot);

    /*
     * Fetch back any out-of-line datums.  We build the new datums array in
     * my_stat->outvalues[] (but we can re-use the slot's isnull array). Also,
     * remember the fetched values to free afterwards.
     */
    nfree = 0;
    for (i = 0; i < natts; i++) {
        Datum val = slot->tts_values[i];

        if (!attrs[i].attisdropped && attrs[i].attlen == -1 && !slot->tts_isnull[i]) {
            if (VARATT_IS_EXTERNAL(DatumGetPointer(val))) {
                val = PointerGetDatum(heap_tuple_fetch_attr((struct varlena *)DatumGetPointer(val)));
                my_stat->tofree[nfree++] = val;
            }
        }

        my_stat->outvalues[i] = val;
    }

    /*
     * Push the modified tuple into the tuplestore.
     */
    oldcxt = MemoryContextSwitchTo(my_stat->cxt);
    tuplestore_putvalues(my_stat->tstore, typeinfo, my_stat->outvalues, slot->tts_isnull);
    (void)MemoryContextSwitchTo(oldcxt);

    /* And release any temporary detoasted values */
    for (i = 0; i < nfree; i++) {
        pfree(DatumGetPointer(my_stat->tofree[i]));
    }
}

/*
 * Clean up at end of an executor run
 */
static void tstoreShutdownReceiver(DestReceiver *self)
{
    TStoreState *my_stat = (TStoreState *)self;

    /* Release workspace if any */
    if (my_stat->outvalues != NULL) {
        pfree_ext(my_stat->outvalues);
    }

    my_stat->outvalues = NULL;
    if (my_stat->tofree != NULL) {
        pfree_ext(my_stat->tofree);
    }
    my_stat->tofree = NULL;
}

/*
 * Destroy receiver when done with it
 */
static void tstoreDestroyReceiver(DestReceiver *self)
{
    pfree_ext(self);
}

/*
 * Initially create a DestReceiver object.
 */
DestReceiver *CreateTuplestoreDestReceiver(void)
{
    TStoreState *self = (TStoreState *)palloc0(sizeof(TStoreState));

    self->pub.receiveSlot = tstoreReceiveSlot_notoast; /* might change */
    self->pub.rStartup = tstoreStartupReceiver;
    self->pub.rShutdown = tstoreShutdownReceiver;
    self->pub.rDestroy = tstoreDestroyReceiver;
    self->pub.mydest = DestTuplestore;
    self->pub.tmpContext = NULL;

    /* private fields will be set by SetTuplestoreDestReceiverParams */
    return (DestReceiver *)self;
}

/*
 * Set parameters for a TuplestoreDestReceiver
 */
void SetTuplestoreDestReceiverParams(DestReceiver *self, Tuplestorestate *tStore, MemoryContext tContext, bool detoast)
{
    TStoreState *my_stat = (TStoreState *)self;

    Assert(my_stat->pub.mydest == DestTuplestore);
    my_stat->tstore = tStore;
    my_stat->cxt = tContext;
    my_stat->detoast = detoast;
}
