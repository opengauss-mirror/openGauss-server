/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <utils/lsyscache.h>
#include <utils/rel.h>
#include <catalog/pg_class.h>
#include <commands/trigger.h>
#include <nodes/nodes.h>
#include <cstdlib>

#include "compat.h"
#include "chunk_dispatch_state.h"
#include "chunk_dispatch_plan.h"
#include "chunk_dispatch.h"
#include "chunk_insert_state.h"
#include "chunk.h"
#include "cache.h"
#include "hypertable_cache.h"
#include "dimension.h"
#include "hypertable.h"

static void
chunk_dispatch_begin(ExtensiblePlanState *node, EState *estate, int eflags)
{
	ChunkDispatchState *state = (ChunkDispatchState *) node;
	Hypertable *ht;
	Cache *hypertable_cache;
	PlanState *ps;

	ht = ts_hypertable_cache_get_cache_and_entry(state->hypertable_relid,
												 CACHE_FLAG_NONE,
												 &hypertable_cache);
	ps = ExecInitNode(state->subplan, estate, eflags);
	state->hypertable_cache = hypertable_cache;
	state->dispatch = ts_chunk_dispatch_create(ht, estate);
	state->dispatch->dispatch_state = state;
	node->extensible_ps = list_make1(ps);
}

/*
 * Change to another chunk for inserts.
 *
 * Prepare the ModifyTableState executor node for inserting into another
 * chunk. Called every time we switch to another chunk for inserts.
 */
#if PG12_GE
static void
on_chunk_insert_state_changed(ChunkInsertState *cis, void *data)
{
	ChunkDispatchState *state = data;
	ModifyTableState *mtstate = state->mtstate;

	/* PG12 expects the current target slot to match the result relation. Thus
	 * we need to make sure it is up-to-date with the current chunk here. */
	mtstate->mt_scans[mtstate->mt_whichplan] = cis->slot;
}
#else
static void
on_chunk_insert_state_changed(ChunkInsertState *cis, void *data)
{
	ChunkDispatchState *state =(ChunkDispatchState*) data;
	ModifyTableState *mtstate = state->mtstate;
	ModifyTable *mtplan = castNode(ModifyTable, mtstate->ps.plan);

	/*
	 * Update the arbiter indexes for ON CONFLICT statements so that they
	 * match the chunk. In PG12, every result relation has its own arbiter
	 * index list, so no update is needed here.
	 */

}
#endif /* PG12_GE */

static TupleTableSlot *
chunk_dispatch_exec(ExtensiblePlanState *node)
{
	ChunkDispatchState *state = (ChunkDispatchState *) node;
	PlanState *substate =(PlanState*) linitial(node->extensible_ps);
	TupleTableSlot *slot;
	Point *point;
	ChunkInsertState *cis;
	ChunkDispatch *dispatch = state->dispatch;
	Hypertable *ht = dispatch->hypertable;
	EState *estate = node->ss.ps.state;
	MemoryContext old;

	#ifdef OG30
	List *tlist = state->cscan_state.ss.ps.targetlist;
	substate->targetlist = tlist;
	substate->plan->targetlist = tlist;
	#endif
	/* Get the next tuple from the subplan state node */
	slot = ExecProcNode(substate);

	if (TupIsNull(slot))
		return NULL;

	/* Switch to the executor's per-tuple memory context */
	old = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));

	/* Calculate the tuple's point in the N-dimensional hyperspace */
	point = ts_hyperspace_calculate_point(ht->space, slot);

	/* Save the main table's (hypertable's) ResultRelInfo */
	if (NULL == dispatch->hypertable_result_rel_info)
		dispatch->hypertable_result_rel_info = estate->es_result_relation_info;

	/* Find or create the insert state matching the point */
	cis = ts_chunk_dispatch_get_chunk_insert_state(dispatch,
												   point,
												   on_chunk_insert_state_changed,
												   state);

	/*
	 * Set the result relation in the executor state to the target chunk.
	 * This makes sure that the tuple gets inserted into the correct
	 * chunk. Note that since the ModifyTable executor saves and restores
	 * the es_result_relation_info this has to be updated every time, not
	 * just when the chunk changes.
	 */
	estate->es_result_relation_info = cis->result_relation_info;

	MemoryContextSwitchTo(old);

	/* Convert the tuple to the chunk's rowtype, if necessary */
	if (cis->hyper_to_chunk_map != NULL)
		slot = execute_attr_map_slot(cis->hyper_to_chunk_map->attrMap, slot, cis->slot);

	return slot;
}

static void
chunk_dispatch_end(ExtensiblePlanState *node)
{
	ChunkDispatchState *state = (ChunkDispatchState *) node;
	PlanState *substate =(PlanState*) linitial(node->extensible_ps);

	ExecEndNode(substate);
	ts_chunk_dispatch_destroy(state->dispatch);
	ts_cache_release(state->hypertable_cache);
}

static void
chunk_dispatch_rescan(ExtensiblePlanState *node)
{
	PlanState *substate = (PlanState*)linitial(node->extensible_ps);

	ExecReScan(substate);
}

static ExtensibleExecMethods chunk_dispatch_state_methods = {
	.ExtensibleName = CHUNK_DISPATCH_STATE_NAME,
	.BeginExtensiblePlan = chunk_dispatch_begin,
	.ExecExtensiblePlan = chunk_dispatch_exec,
	.EndExtensiblePlan = chunk_dispatch_end,
	.ReScanExtensiblePlan = chunk_dispatch_rescan,
};

ChunkDispatchState *
ts_chunk_dispatch_state_create(Oid hypertable_relid, Plan *subplan)
{
	ChunkDispatchState *state;

	state = (ChunkDispatchState *) newNode(sizeof(ChunkDispatchState), T_ExtensiblePlanState);
	state->hypertable_relid = hypertable_relid;
	state->subplan = subplan;
	state->cscan_state.methods = &chunk_dispatch_state_methods;
	return state;
}

#if PG12_GE
/* In PG12, tuple table slots moved to the result relation struct, which exists
 * in one instance per relation (including chunks). Therefore, no changes to
 * these slots are required when changing chunk.
 */
#define setup_tuple_slots_for_on_conflict_handling(state)
#elif PG11
/*
 * In PG11, tuple table slots for ON CONFLICT handling are tied to the format of
 * the "root" table, unless partition routing is enabled (which isn't the case
 * for hypertables).
 *
 * We need to replace the PG11 slots with new ones (that aren't tied to the
 * tuple descriptor of the root table) since we need to be able to dynamically
 * set the tuple descriptor to match the current chunk being inserted into.
 *
 * The slots in question are stored in the executor state's tuple table, which
 * is destroyed, along with all slots, at the end of execution
 * (ExecResetTupleTable). The slots in question include:
 *
 * - mt_existing: the slot that holds the old/existing tuple in the table that
 *   would be updated when there is a conflict.
 * - mt_conflproj: the slot that holds the projected "update" tuple.
 */
static void
setup_tuple_slots_for_on_conflict_handling(ChunkDispatchState *state)
{
	ModifyTableState *mtstate = state->mtstate;
	ModifyTable *mtplan = castNode(ModifyTable, mtstate->ps.plan);

	if (mtplan->upsertAction == ONCONFLICT_UPDATE)
	{
		TupleDesc tupdesc;

		Assert(mtstate->mt_existing != NULL);
		Assert(mtstate->mt_conflproj != NULL);

		tupdesc = mtstate->mt_existing->tts_tupleDescriptor;
		mtstate->mt_existing = ExecInitExtraTupleSlot(mtstate->ps.state, NULL);
		ExecSetSlotDescriptor(mtstate->mt_existing, tupdesc);

		/*
		 * in this case we must overwrite mt_conflproj because there are
		 * several pointers to it throughout expressions and other
		 * evaluations, and the original tuple will otherwise be stored to the
		 * old slot, whose pointer is saved there.
		 */
		tupdesc = mtstate->mt_conflproj->tts_tupleDescriptor;
		mtstate->mt_conflproj = ExecInitExtraTupleSlot(mtstate->ps.state, NULL);
		ExecSetSlotDescriptor(mtstate->mt_conflproj, tupdesc);
		mtstate->resultRelInfo->ri_onConflict->oc_ProjInfo->pi_state.resultslot =
			mtstate->mt_conflproj;
	}
}
#elif PG11_LT
static void
setup_tuple_slots_for_on_conflict_handling(ChunkDispatchState *state)
{
	ModifyTableState *mtstate = state->mtstate;
	ModifyTable *mtplan = castNode(ModifyTable, mtstate->ps.plan);
}
#endif

/*
 * This function is called during the init phase of the INSERT (ModifyTable)
 * plan, and gives the ChunkDispatchState node the access it needs to the
 * internals of the ModifyTableState node.
 *
 * Note that the function is called by the parent of the ModifyTableState node,
 * which guarantees that the ModifyTableState is fully initialized even though
 * ChunkDispatchState is a child of ModifyTableState.
 */
void
ts_chunk_dispatch_state_set_parent(ChunkDispatchState *state, ModifyTableState *mtstate)
{
	ModifyTable *mt_plan = castNode(ModifyTable, mtstate->ps.plan);

	/* Inserts on hypertables should always have one subplan */
	Assert(mtstate->mt_nplans == 1);
	state->mtstate = mtstate;
	setup_tuple_slots_for_on_conflict_handling(state);
}
