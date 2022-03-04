/* -------------------------------------------------------------------------
 *
 * execTuples.cpp
 *	  Routines dealing with TupleTableSlots.  These are used for resource
 *	  management associated with tuples (eg, releasing buffer pins for
 *	  tuples in disk buffers, or freeing the memory occupied by transient
 *	  tuples).	Slots also provide access abstraction that lets us implement
 *	  "virtual" tuples to reduce data-copying overhead.
 *
 *	  Routines dealing with the type information for tuples. Currently,
 *	  the type information for a tuple is an array of FormData_pg_attribute.
 *	  This information is needed by routines manipulating tuples
 *	  (getattribute, formtuple, etc.).
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/runtime/executor/execTuples.cpp
 *
 * -------------------------------------------------------------------------
 * INTERFACE ROUTINES
 *
 *	 SLOT CREATION/DESTRUCTION
 *		MakeTupleTableSlot		- create an empty slot
 *		ExecAllocTableSlot		- create a slot within a tuple table
 *		ExecResetTupleTable		- clear and optionally delete a tuple table
 *		MakeSingleTupleTableSlot - make a standalone slot, set its descriptor
 *		ExecDropSingleTupleTableSlot - destroy a standalone slot
 *
 *	 SLOT ACCESSORS
 *		ExecSetSlotDescriptor	- set a slot's tuple descriptor
 *		ExecStoreTuple			- store a physical tuple in the slot
 *		ExecStoreMinimalTuple	- store a minimal physical tuple in the slot
 *		ExecClearTuple			- clear contents of a slot
 *		ExecStoreVirtualTuple	- mark slot as containing a virtual tuple
 *		ExecCopySlotTuple		- build a physical tuple from a slot
 *		ExecCopySlotMinimalTuple - build a minimal physical tuple from a slot
 *		ExecMaterializeSlot		- convert virtual to physical storage
 *		ExecCopySlot			- copy one slot's contents to another
 *
 *	 CONVENIENCE INITIALIZATION ROUTINES
 *		ExecInitResultTupleSlot    \	convenience routines to initialize
 *		ExecInitScanTupleSlot		\	the various tuple slots for nodes
 *		ExecInitExtraTupleSlot		/	which store copies of tuples.
 *		ExecInitNullTupleSlot	   /
 *
 *	 Routines that probably belong somewhere else:
 *		ExecTypeFromTL			- form a TupleDesc from a target list
 *
 *	 EXAMPLE OF HOW TABLE ROUTINES WORK
 *		Suppose we have a query such as SELECT emp.name FROM emp and we have
 *		a single SeqScan node in the query plan.
 *
 *		At ExecutorStart()
 *		----------------
 *		- ExecInitSeqScan() calls ExecInitScanTupleSlot() and
 *		  ExecInitResultTupleSlot() to construct TupleTableSlots
 *		  for the tuples returned by the access methods and the
 *		  tuples resulting from performing target list projections.
 *
 *		During ExecutorRun()
 *		----------------
 *		- SeqNext() calls ExecStoreTuple() to place the tuple returned
 *		  by the access methods into the scan tuple slot.
 *
 *		- ExecSeqScan() calls ExecStoreTuple() to take the result
 *		  tuple from ExecProject() and place it into the result tuple slot.
 *
 *		- ExecutePlan() calls ExecSelect(), which passes the result slot
 *		  to printtup(), which uses getallattrs() to extract the
 *		  individual Datums for printing.
 *
 *		At ExecutorEnd()
 *		----------------
 *		- EndPlan() calls ExecResetTupleTable() to clean up any remaining
 *		  tuples left over from executing the query.
 *
 *		The important thing to watch in the executor code is how pointers
 *		to the slots containing tuples are passed instead of the tuples
 *		themselves.  This facilitates the communication of related information
 *		(such as whether or not a tuple should be pfreed, what buffer contains
 *		this tuple, the tuple's tuple descriptor, etc).  It also allows us
 *		to avoid physically constructing projection tuples in many cases.
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "funcapi.h"
#include "access/tableam.h"
#include "catalog/pg_type.h"
#include "nodes/nodeFuncs.h"
#include "storage/buf/bufmgr.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/typcache.h"
#include "utils/memutils.h"

#include "access/ustore/knl_utuple.h"

static TupleDesc ExecTypeFromTLInternal(List* target_list, bool has_oid, bool skip_junk, bool mark_dropped = false, TableAmType tam = TAM_HEAP);

/* ----------------------------------------------------------------
 *				  tuple table create/delete functions
 * ----------------------------------------------------------------
 */
/* --------------------------------
 *		MakeTupleTableSlot
 *
 *		Basic routine to make an empty TupleTableSlot.
 * --------------------------------
 */
TupleTableSlot* MakeTupleTableSlot(bool has_tuple_mcxt, TableAmType tupslotTableAm)
{
    TupleTableSlot* slot = makeNode(TupleTableSlot);
    Assert(tupslotTableAm == TAM_HEAP || tupslotTableAm == TAM_USTORE);

    slot->tts_isempty = true;
    slot->tts_shouldFree = false;
    slot->tts_shouldFreeMin = false;
    slot->tts_tuple = NULL;
    slot->tts_tupleDescriptor = NULL;
#ifdef PGXC
    slot->tts_shouldFreeRow = false;
    slot->tts_dataRow = NULL;
    slot->tts_dataLen = -1;
    slot->tts_attinmeta = NULL;
#endif
    slot->tts_mcxt = CurrentMemoryContext;
    slot->tts_buffer = InvalidBuffer;
    slot->tts_nvalid = 0;
    slot->tts_values = NULL;
    slot->tts_isnull = NULL;
    slot->tts_mintuple = NULL;
#ifdef ENABLE_MULTIPLE_NODES
    slot->tts_per_tuple_mcxt = has_tuple_mcxt ? AllocSetContextCreate(slot->tts_mcxt,
        "SlotPerTupleMcxt",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE) : NULL;
#endif
    slot->tts_tupslotTableAm = tupslotTableAm;

    return slot;
}

/* --------------------------------
 *		ExecAllocTableSlot
 *
 *		Create a tuple table slot within a tuple table (which is just a List).
 * --------------------------------
 */
TupleTableSlot* ExecAllocTableSlot(List** tuple_table, TableAmType tupslotTableAm)
{
    TupleTableSlot* slot;

    slot = MakeTupleTableSlot();

    *tuple_table = lappend(*tuple_table, slot);

    slot->tts_tupslotTableAm = tupslotTableAm;

    return slot;
}

/* --------------------------------
 *		ExecResetTupleTable
 *
 *		This releases any resources (buffer pins, tupdesc refcounts)
 *		held by the tuple table, and optionally releases the memory
 *		occupied by the tuple table data structure.
 *		It is expected that this routine be called by EndPlan().
 * --------------------------------
 */
void ExecResetTupleTable(List* tuple_table, /* tuple table */
    bool should_free)                       /* true if we should free memory */
{
    ListCell* lc = NULL;

    foreach (lc, tuple_table) {
        TupleTableSlot* slot = (TupleTableSlot*)lfirst(lc);

        /* Always release resources and reset the slot to empty */
        (void)ExecClearTuple(slot);
        if (slot->tts_tupleDescriptor) {
            ReleaseTupleDesc(slot->tts_tupleDescriptor);
            slot->tts_tupleDescriptor = NULL;
        }

        /* If shouldFree, release memory occupied by the slot itself */
        if (should_free) {
            if (slot->tts_values)
                pfree_ext(slot->tts_values);
            if (slot->tts_isnull)
                pfree_ext(slot->tts_isnull);
            pfree_ext(slot->tts_lobPointers);
            if (slot->tts_per_tuple_mcxt)
                MemoryContextDelete(slot->tts_per_tuple_mcxt);
            pfree_ext(slot);
        }
    }

    /* If shouldFree, release the list structure */
    if (should_free) {
        list_free_ext(tuple_table);
    }
}

TupleTableSlot* ExecMakeTupleSlot(Tuple tuple, TableScanDesc tableScan, TupleTableSlot* slot, TableAmType tableAm)
{
    if (unlikely(RELATION_CREATE_BUCKET(tableScan->rs_rd))) {
        tableScan = ((HBktTblScanDesc)tableScan)->currBktScan;
    }

    if (tuple != NULL) {
        Assert(tableScan != NULL);
        slot->tts_tupslotTableAm = tableAm;
        return ExecStoreTuple(tuple, /* tuple to store */
            slot, /* slot to store in */
            tableScan->rs_cbuf, /* buffer associated with this tuple */
            false); /* don't pfree this pointer */
    }

    return ExecClearTuple(slot);
}

/* --------------------------------
 *		MakeSingleTupleTableSlot
 *
 *		This is a convenience routine for operations that need a
 *		standalone TupleTableSlot not gotten from the main executor
 *		tuple table.  It makes a single slot and initializes it
 *		to use the given tuple descriptor.
 * --------------------------------
 */
TupleTableSlot* MakeSingleTupleTableSlot(TupleDesc tup_desc, bool allocSlotCxt, TableAmType tupslotTableAm)
{
    TupleTableSlot* slot = MakeTupleTableSlot(allocSlotCxt, tupslotTableAm);
    ExecSetSlotDescriptor(slot, tup_desc);
    return slot;
}

/* --------------------------------
 *		ExecDropSingleTupleTableSlot
 *
 *		Release a TupleTableSlot made with MakeSingleTupleTableSlot.
 *		DON'T use this on a slot that's part of a tuple table list!
 * --------------------------------
 */
void ExecDropSingleTupleTableSlot(TupleTableSlot* slot)
{
    /* This should match ExecResetTupleTable's processing of one slot */
    (void)ExecClearTuple(slot);
    if (slot->tts_tupleDescriptor != NULL) {
        ReleaseTupleDesc(slot->tts_tupleDescriptor);
    }

    if (slot->tts_values != NULL) {
        pfree_ext(slot->tts_values);
    }

    if (slot->tts_isnull != NULL) {
        pfree_ext(slot->tts_isnull);
    }

    pfree_ext(slot->tts_lobPointers);

    if (slot->tts_per_tuple_mcxt != NULL) {
        MemoryContextDelete(slot->tts_per_tuple_mcxt);
    }
    pfree_ext(slot);
}

/* ----------------------------------------------------------------
 *				  tuple table slot accessor functions
 * ----------------------------------------------------------------
 */
/* --------------------------------
 *		ExecSetSlotDescriptor
 *
 *		This function is used to set the tuple descriptor associated
 *		with the slot's tuple.  The passed descriptor must have lifespan
 *		at least equal to the slot's.  If it is a reference-counted descriptor
 *		then the reference count is incremented for as long as the slot holds
 *		a reference.
 * --------------------------------
 */
void ExecSetSlotDescriptor(TupleTableSlot* slot, /* slot to change */
    TupleDesc tup_desc)                           /* new tuple descriptor */
{
    /* For safety, make sure slot is empty before changing it */
    (void)ExecClearTuple(slot);

    /*
     * Release any old descriptor.	Also release old Datum/isnull arrays if
     * present (we don't bother to check if they could be re-used).
     */
    if (slot->tts_tupleDescriptor != NULL) {
        ReleaseTupleDesc(slot->tts_tupleDescriptor);
    }
#ifdef PGXC
    /* XXX there in no routine to release AttInMetadata instance */
    if (slot->tts_attinmeta != NULL) {
        slot->tts_attinmeta = NULL;
    }
#endif

    if (slot->tts_values != NULL) {
        pfree_ext(slot->tts_values);
    }
    if (slot->tts_isnull != NULL) {
        pfree_ext(slot->tts_isnull);
    }
    pfree_ext(slot->tts_lobPointers);
    /*
     * Install the new descriptor; if it's refcounted, bump its refcount.
     */
    slot->tts_tupleDescriptor = tup_desc;
    PinTupleDesc(tup_desc);

    /*
     * Allocate Datum/isnull arrays of the appropriate size.  These must have
     * the same lifetime as the slot, so allocate in the slot's own context.
     */
    slot->tts_values = (Datum*)MemoryContextAlloc(slot->tts_mcxt, tup_desc->natts * sizeof(Datum));
    slot->tts_isnull = (bool*)MemoryContextAlloc(slot->tts_mcxt, tup_desc->natts * sizeof(bool));
    slot->tts_lobPointers = (Datum*)MemoryContextAlloc(slot->tts_mcxt, tup_desc->natts * sizeof(Datum));
}

/* --------------------------------
 *		ExecStoreTuple
 *
 *		This function is used to store a physical tuple into a specified
 *		slot in the tuple table.
 *
 *		tuple:	tuple to store
 *		slot:	slot to store it in
 *		buffer: disk buffer if tuple is in a disk page, else InvalidBuffer
 *		shouldFree: true if ExecClearTuple should pfree_ext() the tuple
 *					when done with it
 *
 * If 'buffer' is not InvalidBuffer, the tuple table code acquires a pin
 * on the buffer which is held until the slot is cleared, so that the tuple
 * won't go away on us.
 *
 * shouldFree is normally set 'true' for tuples constructed on-the-fly.
 * It must always be 'false' for tuples that are stored in disk pages,
 * since we don't want to try to pfree those.
 *
 * Another case where it is 'false' is when the referenced tuple is held
 * in a tuple table slot belonging to a lower-level executor Proc node.
 * In this case the lower-level slot retains ownership and responsibility
 * for eventually releasing the tuple.	When this method is used, we must
 * be certain that the upper-level Proc node will lose interest in the tuple
 * sooner than the lower-level one does!  If you're not certain, copy the
 * lower-level tuple with heap_copytuple and let the upper-level table
 * slot assume ownership of the copy!
 *
 * Return value is just the passed-in slot pointer.
 *
 * NOTE: before PostgreSQL 8.1, this function would accept a NULL tuple
 * pointer and effectively behave like ExecClearTuple (though you could
 * still specify a buffer to pin, which would be an odd combination).
 * This saved a couple lines of code in a few places, but seemed more likely
 * to mask logic errors than to be really useful, so it's now disallowed.
 * --------------------------------
 */
TupleTableSlot* ExecStoreTuple(Tuple tuple, TupleTableSlot* slot, Buffer buffer, bool should_free)
{
    /*
     * sanity checks
     */
    Assert(tuple != NULL);
    Assert(slot != NULL);
    Assert(slot->tts_tupleDescriptor != NULL);

    HeapTuple htup = (HeapTuple)tuple;
    if (slot->tts_tupslotTableAm == TAM_USTORE && htup->tupTableType == HEAP_TUPLE) {
        tuple = (Tuple)HeapToUHeap(slot->tts_tupleDescriptor, (HeapTuple)tuple);
    } else if (slot->tts_tupslotTableAm == TAM_HEAP && htup->tupTableType == UHEAP_TUPLE) {
        tuple = (Tuple)UHeapToHeap(slot->tts_tupleDescriptor, (UHeapTuple)tuple);
    }

    tableam_tslot_store_tuple(tuple, slot, buffer, should_free, false);

    return slot;
}

/* --------------------------------
 *		ExecStoreMinimalTuple
 *
 *		Like ExecStoreTuple, but insert a "minimal" tuple into the slot.
 *
 * No 'buffer' parameter since minimal tuples are never stored in relations.
 * --------------------------------
 */
TupleTableSlot* ExecStoreMinimalTuple(MinimalTuple mtup, TupleTableSlot* slot, bool should_free)
{
    /*
     * sanity checks
     */
    Assert(mtup != NULL);
    Assert(slot != NULL);
    Assert(slot->tts_tupleDescriptor != NULL);

    /*
     * store the minimal tuple in the slot.
     */
    tableam_tslot_store_minimal_tuple(mtup, slot, should_free);

    return slot;
}

/* --------------------------------
 *		ExecClearTuple
 *
 *		This function is used to clear out a slot in the tuple table.
 *
 *		NB: only the tuple is cleared, not the tuple descriptor (if any).
 * --------------------------------
 */
TupleTableSlot* ExecClearTuple(TupleTableSlot* slot) /* return: slot passed slot in which to store tuple */
{
    /*
     * sanity checks
     */
    Assert(slot != NULL);

    /*
     * clear the physical tuple or minimal tuple if present via TableAm.
     */
    if (slot->tts_shouldFree || slot->tts_shouldFreeMin) {
    	Assert(slot->tts_tupleDescriptor != NULL);
    	tableam_tslot_clear(slot);
    }

    /* 
     * tts_tuple may still be valid if tts_shouldFree is false, Original caller doesn't want this slot to free the tuple.
     */
    slot->tts_tuple = NULL;
    slot->tts_mintuple = NULL;
    slot->tts_shouldFree = false;
    slot->tts_shouldFreeMin = false;

#ifdef ENABLE_MULTIPLE_NODES
    if (slot->tts_shouldFreeRow) {
        pfree_ext(slot->tts_dataRow);
    }
    slot->tts_shouldFreeRow = false;
    slot->tts_dataRow = NULL;
    slot->tts_dataLen = -1;
    slot->tts_xcnodeoid = 0;
#endif

    /*
     * Drop the pin on the referenced buffer, if there is one.
     */
    if (BufferIsValid(slot->tts_buffer)) {
        ReleaseBuffer(slot->tts_buffer);
    }
    slot->tts_buffer = InvalidBuffer;

    /*
     * Mark it empty.
     */
    slot->tts_isempty = true;
    slot->tts_nvalid = 0;

    // Row uncompression use slot->tts_per_tuple_mcxt in some case, So we need
    // reset memory context. This memory context is introduced by PGXC and it only used
    // in function 'slot_deform_datarow'.  PGXC also do reset in function 'FetchTuple'.
    // So it is safe
    //
    ResetSlotPerTupleContext(slot);
    return slot;
}

/* --------------------------------
 *		ExecStoreVirtualTuple
 *			Mark a slot as containing a virtual tuple.
 *
 * The protocol for loading a slot with virtual tuple data is:
 *		* Call ExecClearTuple to mark the slot empty.
 *		* Store data into the Datum/isnull arrays.
 *		* Call ExecStoreVirtualTuple to mark the slot valid.
 * This is a bit unclean but it avoids one round of data copying.
 * --------------------------------
 */
TupleTableSlot* ExecStoreVirtualTuple(TupleTableSlot* slot)
{
    /*
     * sanity checks
     */
    Assert(slot != NULL);
    Assert(slot->tts_tupleDescriptor != NULL);
    Assert(slot->tts_isempty);

    slot->tts_isempty = false;
    slot->tts_nvalid = slot->tts_tupleDescriptor->natts;

    if (slot->tts_tupslotTableAm != slot->tts_tupleDescriptor->tdTableAmType) {
        // XXX: Should tts_tupleDescriptor be cloned before changing its contents
        // as some time it can be direct reference to the rd_att in RelationData.
        slot->tts_tupleDescriptor->tdTableAmType = slot->tts_tupslotTableAm;
    }

    return slot;
}

/* --------------------------------
 *		ExecStoreAllNullTuple
 *			Set up the slot to contain a null in every column.
 *
 * At first glance this might sound just like ExecClearTuple, but it's
 * entirely different: the slot ends up full, not empty.
 * --------------------------------
 */
TupleTableSlot* ExecStoreAllNullTuple(TupleTableSlot* slot)
{
    /*
     * sanity checks
     */
    Assert(slot != NULL);
    Assert(slot->tts_tupleDescriptor != NULL);

    /* Clear any old contents */
    (void)ExecClearTuple(slot);

    /*
     * Fill all the columns of the virtual tuple with nulls
     */
    errno_t rc = EOK;

    rc = memset_s(slot->tts_values,
        slot->tts_tupleDescriptor->natts * sizeof(Datum),
        0,
        slot->tts_tupleDescriptor->natts * sizeof(Datum));
    securec_check(rc, "\0", "\0");
    rc = memset_s(slot->tts_isnull,
        slot->tts_tupleDescriptor->natts * sizeof(bool),
        true,
        slot->tts_tupleDescriptor->natts * sizeof(bool));
    securec_check(rc, "\0", "\0");

    return ExecStoreVirtualTuple(slot);
}

/* --------------------------------
 *		ExecCopySlotTuple
 *			Obtain a copy of a slot's regular physical tuple.  The copy is
 *			palloc'd in the current memory context.
 *			The slot itself is undisturbed.
 *
 *		This works even if the slot contains a virtual or minimal tuple;
 *		however the "system columns" of the result will not be meaningful.
 * --------------------------------
 */
HeapTuple ExecCopySlotTuple(TupleTableSlot* slot)
{
    /*
     * sanity checks
     */
    Assert(slot != NULL);
    Assert(!slot->tts_isempty);

    return tableam_tslot_copy_heap_tuple(slot);
}

/* --------------------------------
 *		ExecCopySlotMinimalTuple
 *			Obtain a copy of a slot's minimal physical tuple.  The copy is
 *			palloc'd in the current memory context.
 *			The slot itself is undisturbed.
 * --------------------------------
 */
MinimalTuple ExecCopySlotMinimalTuple(TupleTableSlot* slot, bool need_transform_anyarray)
{
    /*
     * sanity checks
     */
    Assert(slot != NULL);
    Assert(!slot->tts_isempty);

    return tableam_tslot_copy_minimal_tuple(slot);
}

/* --------------------------------
 *		ExecFetchSlotTuple
 *			Fetch the slot's regular physical tuple.
 *
 *		If the slot contains a virtual tuple, we convert it to physical
 *		form.  The slot retains ownership of the physical tuple.
 *		If it contains a minimal tuple we convert to regular form and store
 *		that in addition to the minimal tuple (not instead of, because
 *		callers may hold pointers to Datums within the minimal tuple).
 *
 * The main difference between this and ExecMaterializeSlot() is that this
 * does not guarantee that the contained tuple is local storage.
 * Hence, the result must be treated as read-only.
 * --------------------------------
 */
HeapTuple ExecFetchSlotTuple(TupleTableSlot* slot)
{
    /*
     * sanity checks
     */
    Assert(slot != NULL);
    Assert(!slot->tts_isempty);

    return tableam_tslot_get_heap_tuple(slot);
}

/* --------------------------------
 *		ExecFetchSlotMinimalTuple
 *			Fetch the slot's minimal physical tuple.
 *
 *		If the slot contains a virtual tuple, we convert it to minimal
 *		physical form.	The slot retains ownership of the minimal tuple.
 *		If it contains a regular tuple we convert to minimal form and store
 *		that in addition to the regular tuple (not instead of, because
 *		callers may hold pointers to Datums within the regular tuple).
 *
 * As above, the result must be treated as read-only.
 * --------------------------------
 */
MinimalTuple ExecFetchSlotMinimalTuple(TupleTableSlot* slot)
{
    /*
     * sanity checks
     */
    Assert(slot != NULL);

    return tableam_tslot_get_minimal_tuple(slot);
}

/* --------------------------------
 *		ExecFetchSlotTupleDatum
 *			Fetch the slot's tuple as a composite-type Datum.
 *
 *		We convert the slot's contents to local physical-tuple form,
 *		and fill in the Datum header fields.  Note that the result
 *		always points to storage owned by the slot.
 * --------------------------------
 */
Datum ExecFetchSlotTupleDatum(TupleTableSlot* slot)
{
    HeapTuple tup;
    HeapTupleHeader td;
    TupleDesc tup_desc;

    /* Make sure we can scribble on the slot contents ... */
    tup = ExecMaterializeSlot(slot);
    /* ... and set up the composite-Datum header fields, in case not done */
    td = tup->t_data;
    tup_desc = slot->tts_tupleDescriptor;
    HeapTupleHeaderSetDatumLength(td, tup->t_len);
    HeapTupleHeaderSetTypeId(td, tup_desc->tdtypeid);
    HeapTupleHeaderSetTypMod(td, tup_desc->tdtypmod);
    return PointerGetDatum(td);
}

/* --------------------------------
 *		ExecMaterializeSlot
 *			Force a slot into the "materialized" state.
 *
 *		This causes the slot's tuple to be a local copy not dependent on
 *		any external storage.  A pointer to the contained tuple is returned.
 *
 *		A typical use for this operation is to prepare a computed tuple
 *		for being stored on disk.  The original data may or may not be
 *		virtual, but in any case we need a private copy for heap_insert
 *		to scribble on.
 * --------------------------------
 */
HeapTuple ExecMaterializeSlot(TupleTableSlot* slot)
{
    /*
     * sanity checks
     */
    Assert(slot != NULL);
    Assert(!slot->tts_isempty);

    return tableam_tslot_materialize(slot);
}

/* --------------------------------
 *		ExecCopySlot
 *			Copy the source slot's contents into the destination slot.
 *
 *		The destination acquires a private copy that will not go away
 *		if the source is cleared.
 *
 *		The caller must ensure the slots have compatible tupdescs.
 * --------------------------------
 */
TupleTableSlot* ExecCopySlot(TupleTableSlot* dst_slot, TupleTableSlot* src_slot)
{
    HeapTuple new_tuple;
    MemoryContext old_context;

    /*
     * There might be ways to optimize this when the source is virtual, but
     * for now just always build a physical copy.  Make sure it is in the
     * right context.
     */
    old_context = MemoryContextSwitchTo(dst_slot->tts_mcxt);
    new_tuple = ExecCopySlotTuple(src_slot);
    MemoryContextSwitchTo(old_context);

    return ExecStoreTuple(new_tuple, dst_slot, InvalidBuffer, true);
}

/* ----------------------------------------------------------------
 *				convenience initialization routines
 * ----------------------------------------------------------------
 */
/* --------------------------------
 *		ExecInit{Result,Scan,Extra}TupleSlot
 *
 *		These are convenience routines to initialize the specified slot
 *		in nodes inheriting the appropriate state.	ExecInitExtraTupleSlot
 *		is used for initializing special-purpose slots.
 * --------------------------------
 */
/* ----------------
 *		ExecInitResultTupleSlot
 * ----------------
 */
void ExecInitResultTupleSlot(EState* estate, PlanState* plan_state, TableAmType tam)
{
    plan_state->ps_ResultTupleSlot = ExecAllocTableSlot(&estate->es_tupleTable, tam);
}

/* ----------------
 *		ExecInitScanTupleSlot
 * ----------------
 */
void ExecInitScanTupleSlot(EState* estate, ScanState* scan_state, TableAmType tam)
{
    scan_state->ss_ScanTupleSlot = ExecAllocTableSlot(&estate->es_tupleTable, tam);
}

/* ----------------
 *		ExecInitExtraTupleSlot
 * ----------------
 */
TupleTableSlot* ExecInitExtraTupleSlot(EState* estate, TableAmType tam)
{
    return ExecAllocTableSlot(&estate->es_tupleTable, tam);
}

/* ----------------
 *		ExecInitNullTupleSlot
 *
 * Build a slot containing an all-nulls tuple of the given type.
 * This is used as a substitute for an input tuple when performing an
 * outer join.
 * ----------------
 */
TupleTableSlot* ExecInitNullTupleSlot(EState* estate, TupleDesc tup_type)
{
    TupleTableSlot* slot = ExecInitExtraTupleSlot(estate);

    ExecSetSlotDescriptor(slot, tup_type);

    return ExecStoreAllNullTuple(slot);
}

/* ----------------------------------------------------------------
 *		ExecTypeFromTL
 *
 *		Generate a tuple descriptor for the result tuple of a targetlist.
 *		(A parse/plan tlist must be passed, not an ExprState tlist.)
 *		Note that resjunk columns, if any, are included in the result.
 *
 *		Currently there are about 4 different places where we create
 *		TupleDescriptors.  They should all be merged, or perhaps
 *		be rewritten to call BuildDesc().
 * ----------------------------------------------------------------
 */
TupleDesc ExecTypeFromTL(List* target_list, bool has_oid, bool mark_dropped, TableAmType tam)
{
    return ExecTypeFromTLInternal(target_list, has_oid, false, mark_dropped, tam);
}

/* ----------------------------------------------------------------
 *		ExecCleanTypeFromTL
 *
 *		Same as above, but resjunk columns are omitted from the result.
 * ----------------------------------------------------------------
 */
TupleDesc ExecCleanTypeFromTL(List* target_list, bool has_oid, TableAmType tam)
{
    return ExecTypeFromTLInternal(target_list, has_oid, true, false, tam);
}

static TupleDesc ExecTypeFromTLInternal(List* target_list, bool has_oid, bool skip_junk, bool mark_dropped,  TableAmType tam)
{
    TupleDesc type_info;
    ListCell* l = NULL;
    int len;
    int cur_resno = 1;

    if (skip_junk)
        len = ExecCleanTargetListLength(target_list);
    else
        len = ExecTargetListLength(target_list);
    type_info = CreateTemplateTupleDesc(len, has_oid, tam);

    foreach (l, target_list) {
        TargetEntry* tle = (TargetEntry*)lfirst(l);

        if (skip_junk && tle->resjunk)
            continue;

        TupleDescInitEntry(
            type_info, cur_resno, tle->resname, exprType((Node*)tle->expr), exprTypmod((Node*)tle->expr), 0);
        TupleDescInitEntryCollation(type_info, cur_resno, exprCollation((Node*)tle->expr));

        /* mark dropped column, maybe we can find another way some day */
        if (mark_dropped && strstr(tle->resname, "........pg.dropped.")) {
            type_info->attrs[cur_resno - 1]->attisdropped = true;
        }

        cur_resno++;
    }

    return type_info;
}

/*
 * ExecTypeFromExprList - build a tuple descriptor from a list of Exprs
 *
 * Caller must also supply a list of field names (String nodes).
 */
TupleDesc ExecTypeFromExprList(List* expr_list, List* names_list,  TableAmType tam)
{
    TupleDesc type_info;
    ListCell* le = NULL;
    ListCell* ln = NULL;
    int cur_resno = 1;

    Assert(list_length(expr_list) == list_length(names_list));

    type_info = CreateTemplateTupleDesc(list_length(expr_list), false, tam);

    forboth(le, expr_list, ln, names_list)
    {
        Node* e = (Node*)lfirst(le);
        char* n = strVal(lfirst(ln));

        TupleDescInitEntry(type_info, cur_resno, n, exprType(e), exprTypmod(e), 0);
        TupleDescInitEntryCollation(type_info, cur_resno, exprCollation(e));
        cur_resno++;
    }

    return type_info;
}

/*
 * BlessTupleDesc - make a completed tuple descriptor useful for SRFs
 *
 * Rowtype Datums returned by a function must contain valid type information.
 * This happens "for free" if the tupdesc came from a relcache entry, but
 * not if we have manufactured a tupdesc for a transient RECORD datatype.
 * In that case we have to notify typcache.c of the existence of the type.
 */
TupleDesc BlessTupleDesc(TupleDesc tup_desc)
{
    if (tup_desc->tdtypeid == RECORDOID && tup_desc->tdtypmod < 0) {
        assign_record_type_typmod(tup_desc);
    }
    return tup_desc; /* just for notational convenience */
}

/*
 * TupleDescGetSlot - Initialize a slot based on the supplied tupledesc
 *
 * Note: this is obsolete; it is sufficient to call BlessTupleDesc on
 * the tupdesc.  We keep it around just for backwards compatibility with
 * existing user-written SRFs.
 */
TupleTableSlot* TupleDescGetSlot(TupleDesc tup_desc)
{
    TupleTableSlot* slot = NULL;

    /* The useful work is here */
    BlessTupleDesc(tup_desc);

    /* Make a standalone slot */
    slot = MakeSingleTupleTableSlot(tup_desc);

    /* Return the slot */
    return slot;
}

/*
 * TupleDescGetAttInMetadata - Build an AttInMetadata structure based on the
 * supplied TupleDesc. AttInMetadata can be used in conjunction with C strings
 * to produce a properly formed tuple.
 */
AttInMetadata* TupleDescGetAttInMetadata(TupleDesc tup_desc)
{
    int natts = tup_desc->natts;
    int i;
    Oid att_type_id;
    Oid att_in_func_id;
    FmgrInfo* att_in_func_info = NULL;
    Oid* att_io_params = NULL;
    int32* att_typ_mods = NULL;
    AttInMetadata* att_in_meta = NULL;

    att_in_meta = (AttInMetadata*)palloc(sizeof(AttInMetadata));

    /* "Bless" the tupledesc so that we can make rowtype datums with it */
    att_in_meta->tupdesc = BlessTupleDesc(tup_desc);

    /*
     * Gather info needed later to call the "in" function for each attribute
     */
    att_in_func_info = (FmgrInfo*)palloc0(natts * sizeof(FmgrInfo));
    att_io_params = (Oid*)palloc0(natts * sizeof(Oid));
    att_typ_mods = (int32*)palloc0(natts * sizeof(int32));

    for (i = 0; i < natts; i++) {
        /* Ignore dropped attributes */
        if (!tup_desc->attrs[i]->attisdropped) {
            att_type_id = tup_desc->attrs[i]->atttypid;
            getTypeInputInfo(att_type_id, &att_in_func_id, &att_io_params[i]);
            fmgr_info(att_in_func_id, &att_in_func_info[i]);
            att_typ_mods[i] = tup_desc->attrs[i]->atttypmod;
        }
    }
    att_in_meta->attinfuncs = att_in_func_info;
    att_in_meta->attioparams = att_io_params;
    att_in_meta->atttypmods = att_typ_mods;

    return att_in_meta;
}

/*
 * BuildTupleFromCStrings - build a HeapTuple given user data in C string form.
 * values is an array of C strings, one for each attribute of the return tuple.
 * A NULL string pointer indicates we want to create a NULL field.
 */
HeapTuple BuildTupleFromCStrings(AttInMetadata* att_in_meta, char** values)
{
    TupleDesc tup_desc = att_in_meta->tupdesc;
    int natts = tup_desc->natts;
    Datum* d_values = NULL;
    bool* nulls = NULL;
    int i;
    HeapTuple tuple;

    d_values = (Datum*)palloc(natts * sizeof(Datum));
    nulls = (bool*)palloc(natts * sizeof(bool));

    /* Call the "in" function for each non-dropped attribute */
    for (i = 0; i < natts; i++) {
        if (!tup_desc->attrs[i]->attisdropped) {
            /* Non-dropped attributes */
            d_values[i] = InputFunctionCall(
                &att_in_meta->attinfuncs[i], values[i], att_in_meta->attioparams[i], att_in_meta->atttypmods[i]);

            nulls[i] = (values[i] == NULL);
        } else {
            /* Handle dropped attributes by setting to NULL */
            d_values[i] = (Datum)0;
            nulls[i] = true;
        }
    }

    /*
     * Form a tuple
     */
    tuple = (HeapTuple)tableam_tops_form_tuple(tup_desc, d_values, nulls, HEAP_TUPLE);

    /*
     * Release locally palloc'd space.  XXX would probably be good to pfree
     * values of pass-by-reference datums, as well.
     */
    pfree_ext(d_values);
    pfree_ext(nulls);

    return tuple;
}

/*
 * Functions for sending tuples to the frontend (or other specified destination)
 * as though it is a SELECT result. These are used by utility commands that
 * need to project directly to the destination and don't need or want full
 * table function capability. Currently used by EXPLAIN and SHOW ALL.
 */
TupOutputState* begin_tup_output_tupdesc(DestReceiver* dest, TupleDesc tup_desc)
{
    TupOutputState* tstate = NULL;

    tstate = (TupOutputState*)palloc(sizeof(TupOutputState));

    tstate->slot = MakeSingleTupleTableSlot(tup_desc);
    tstate->dest = dest;

    (*tstate->dest->rStartup)(tstate->dest, (int)CMD_SELECT, tup_desc);

    return tstate;
}

/*
 * write a single tuple
 */
void do_tup_output(TupOutputState* tstate, Datum* values, size_t values_len, const bool* is_null, size_t is_null_len)
{
    Assert(values != NULL);
    Assert(values_len != 0);
    Assert(is_null != NULL);
    Assert(is_null_len != 0);
    TupleTableSlot* slot = tstate->slot;
    int natts = slot->tts_tupleDescriptor->natts;
    errno_t rc = EOK;

    /* make sure the slot is clear */
    (void)ExecClearTuple(slot);

    /* insert data */
    rc = memcpy_s(slot->tts_values, natts * sizeof(Datum), values, natts * sizeof(Datum));
    securec_check(rc, "\0", "\0");
    rc = memcpy_s(slot->tts_isnull, natts * sizeof(bool), is_null, natts * sizeof(bool));
    securec_check(rc, "\0", "\0");

    /* mark slot as containing a virtual tuple */
    ExecStoreVirtualTuple(slot);

    /* send the tuple to the receiver */
    (*tstate->dest->receiveSlot)(slot, tstate->dest);

    /* clean up */
    (void)ExecClearTuple(slot);
}

/*
 * write a chunk of text, breaking at newline characters
 *
 * Should only be used with a single-TEXT-attribute tupdesc.
 */
int do_text_output_multiline(TupOutputState* tstate, char* text)
{
    Datum values[1];
    bool is_null[1] = {false};
    int tuple_count = 0;

    while (*text) {
        char* eol = NULL;
        int len;

        eol = strchr(text, '\n');
        if (eol != NULL) {
            len = eol - text;
            eol++;
        } else {
            len = strlen(text);
            eol = text;
            eol += len;
        }

        values[0] = PointerGetDatum(cstring_to_text_with_len(text, len));
        do_tup_output(tstate, values, 1, is_null, 1);
        tuple_count++;
        pfree(DatumGetPointer(values[0]));
        text = eol;
    }
    return tuple_count;
}

void end_tup_output(TupOutputState* tstate)
{
    (*tstate->dest->rShutdown)(tstate->dest);
    /* note that destroying the dest is not ours to do */
    ExecDropSingleTupleTableSlot(tstate->slot);
    pfree_ext(tstate);
}

#ifdef PGXC
/* --------------------------------
 *		ExecStoreDataRowTuple
 *
 *		Store a buffer in DataRow message format into the slot.
 *
 * --------------------------------
 */
TupleTableSlot* ExecStoreDataRowTuple(char* msg, size_t len, Oid msgnode_oid, TupleTableSlot* slot, bool should_free)
{
    /*
     * sanity checks
     */
    Assert(msg != NULL);
    Assert(len > 0);
    Assert(slot != NULL);
    Assert(slot->tts_tupleDescriptor != NULL);

    /*
     * Free any old physical tuple belonging to the slot.
     */
    if (slot->tts_shouldFree && (HeapTuple)slot->tts_tuple != NULL) {
        heap_freetuple((HeapTuple)slot->tts_tuple);
        slot->tts_tuple = NULL;
        slot->tts_shouldFree = false;
    }
    if (slot->tts_shouldFreeMin) {
        heap_free_minimal_tuple(slot->tts_mintuple);
    }
    /*
     * if msg == slot->tts_dataRow then we would
     * free the dataRow in the slot loosing the contents in msg. It is safe
     * to reset shouldFreeRow, since it will be overwritten just below.
     */
    if (msg == slot->tts_dataRow) {
        slot->tts_shouldFreeRow = false;
    }
    if (slot->tts_shouldFreeRow) {
        pfree_ext(slot->tts_dataRow);
    }
    ResetSlotPerTupleContext(slot);

    /*
     * Drop the pin on the referenced buffer, if there is one.
     */
    if (BufferIsValid(slot->tts_buffer)) {
        ReleaseBuffer(slot->tts_buffer);
    }
    slot->tts_buffer = InvalidBuffer;

    /*
     * Store the new tuple into the specified slot.
     */
    slot->tts_isempty = false;
    slot->tts_shouldFree = false;
    slot->tts_shouldFreeMin = false;
    slot->tts_shouldFreeRow = should_free;
    slot->tts_tuple = NULL;
    slot->tts_mintuple = NULL;
    slot->tts_dataRow = msg;
    slot->tts_dataLen = len;
    slot->tts_xcnodeoid = msgnode_oid;
    /* Mark extracted state invalid */
    slot->tts_nvalid = 0;

    return slot;
}
#endif
