/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <parser/parsetree.h>
#include <nodes/execnodes.h>
#include <nodes/makefuncs.h>
#include <nodes/nodeFuncs.h>
#include <nodes/plannodes.h>
#include <executor/node/nodeModifyTable.h>
#include <utils/rel.h>
#include <utils/lsyscache.h>
#include <foreign/foreign.h>
#include <catalog/pg_type.h>

#include "hypertable_insert.h"
#include "chunk_dispatch_state.h"
#include "chunk_dispatch_plan.h"
#include "hypertable_cache.h"

static PlanState *
get_chunk_dispatch_state(ModifyTableState *mtstate, PlanState *substate)
{
	switch (nodeTag(substate))
	{
		case T_ExtensiblePlanState:
		{
			ExtensiblePlanState *csstate = castNode(ExtensiblePlanState, substate);

			if (strcmp(csstate->methods->ExtensibleName, CHUNK_DISPATCH_STATE_NAME) == 0)
				return substate;

			break;
		}
		case T_ResultState:
			return get_chunk_dispatch_state(mtstate, castNode(ResultState, substate)->ps.lefttree);
		default:
			break;
	}

	return NULL;
}

/*
 * HypertableInsert (with corresponding executor node) is a plan node that
 * implements INSERTs for hypertables. It is mostly a wrapper around the
 * ModifyTable plan node that simply calls the wrapped ModifyTable plan without
 * doing much else, apart from some initial state setup.
 *
 * The wrapping is needed to setup state in the execution phase, and give access
 * to the ModifyTableState node to sub-plan states in the PlanState tree. For
 * instance, the ChunkDispatchState node needs to set the arbiter index list in
 * the ModifyTableState node whenever it inserts into a new chunk.
 */
static void
hypertable_insert_begin(ExtensiblePlanState *node, EState *estate, int eflags)
{
	HypertableInsertState *state = (HypertableInsertState *) node;
	ModifyTableState *mtstate;
	PlanState *ps;
	bool PG_USED_FOR_ASSERTS_ONLY found_cdstate = false;
	int i;

	ps = ExecInitNode(&state->mt->plan, estate, eflags);
	node->extensible_ps = list_make1(ps);
	mtstate = castNode(ModifyTableState, ps);

	/*
	 * Find all ChunkDispatchState subnodes and set their parent
	 * ModifyTableState node
	 */
	for (i = 0; i < mtstate->mt_nplans; i++)
	{
		ChunkDispatchState *cdstate =
			(ChunkDispatchState *) get_chunk_dispatch_state(mtstate, mtstate->mt_plans[i]);

		if (cdstate)
		{
			found_cdstate = true;
			ts_chunk_dispatch_state_set_parent(cdstate, mtstate);
		}
	}

	/* Ensure that we found at least one ChunkDispatchState node */
	Assert(found_cdstate);
}

static TupleTableSlot *
hypertable_insert_exec(ExtensiblePlanState *node)
{
	return ExecProcNode((PlanState *)linitial(node->extensible_ps));
}

static void
hypertable_insert_end(ExtensiblePlanState *node)
{
	ExecEndNode((PlanState *)linitial(node->extensible_ps));
}

static void
hypertable_insert_rescan(ExtensiblePlanState *node)
{
	ExecReScan((PlanState *)linitial(node->extensible_ps));
}

static ExtensibleExecMethods hypertable_insert_state_methods = {
	.ExtensibleName = "HypertableInsertState",
	.BeginExtensiblePlan = hypertable_insert_begin,
	.ExecExtensiblePlan = hypertable_insert_exec,
	.EndExtensiblePlan = hypertable_insert_end,
	.ReScanExtensiblePlan = hypertable_insert_rescan,
};

static Node *
hypertable_insert_state_create(ExtensiblePlan *cscan)
{
	HypertableInsertState *state;
	ModifyTable *mt = castNode(ModifyTable, linitial(cscan->extensible_plans));

	state = (HypertableInsertState *) newNode(sizeof(HypertableInsertState), T_ExtensiblePlanState);
	state->cscan_state.methods = &hypertable_insert_state_methods;
	state->mt = mt;

	return (Node *) state;
}

static ExtensiblePlanMethods hypertable_insert_plan_methods = {
	.ExtensibleName = "HypertableInsert",
	.CreateExtensiblePlanState = hypertable_insert_state_create,
};

/*
 * Make a targetlist to meet CustomScan expectations.
 *
 * When a CustomScan isn't scanning a real relation (scanrelid=0), it will build
 * a virtual TupleDesc for the scan "input" based on custom_scan_tlist. The
 * "output" targetlist is then expected to reference the attributes of the
 * input's TupleDesc. Without projection, the targetlist will be only Vars with
 * varno set to INDEX_VAR (to indicate reference to the TupleDesc instead of a
 * real relation) and matching the order of the attributes in the TupleDesc.
 *
 * Any other order, or non-Vars, will lead to the CustomScan performing
 * projection.
 *
 * Since the CustomScan for hypertable insert just wraps ModifyTable, no
 * projection is needed, so we'll build a targetlist to avoid this.
 */
static List *
make_var_targetlist(const List *tlist)
{
	List *new_tlist = NIL;
	ListCell *lc;
	int resno = 1;

	foreach (lc, tlist)
	{
		TargetEntry *tle = lfirst_node(TargetEntry, lc);
		Var *var = makeVarFromTargetEntry(INDEX_VAR, tle);

		var->varattno = resno;
		new_tlist = lappend(new_tlist, makeTargetEntry(&var->xpr, resno, tle->resname, false));
		resno++;
	}

	return new_tlist;
}

/*
 * Construct the HypertableInsert's target list based on the ModifyTable's
 * target list, which now exists after having been created by
 * set_plan_references().
 */
void
ts_hypertable_insert_fixup_tlist(Plan *plan)
{
	if (IsA(plan, ExtensiblePlan))
	{
		ExtensiblePlan *cscan = (ExtensiblePlan *) plan;

		if (cscan->methods == &hypertable_insert_plan_methods)
		{
			ModifyTable *mt = linitial_node(ModifyTable, cscan->extensible_plans);

			if (mt->plan.targetlist == NIL)
			{
				cscan->extensible_plan_tlist = NIL;
				cscan->scan.plan.targetlist = NIL;
			}
			else
			{
				/* The input is the output of the child ModifyTable node */
				cscan->extensible_plan_tlist = mt->plan.targetlist;

				/* The output is a direct mapping of the input */
				cscan->scan.plan.targetlist = make_var_targetlist(mt->plan.targetlist);
			}
		}
	}
}

static Plan *
hypertable_insert_plan_create(PlannerInfo *root, RelOptInfo *rel, ExtensiblePath *best_path,
							  List *tlist, List *clauses, List *custom_plans)
{
	ExtensiblePlan *cscan = makeNode(ExtensiblePlan);

	#ifdef OG30
	ExtensiblePlan *mt =(ExtensiblePlan *) linitial(custom_plans);
	Assert(IsA(mt, ExtensiblePlan));
	cscan->methods = &hypertable_insert_plan_methods;
	cscan->extensible_plans = list_make1(mt);
	cscan->scan.scanrelid = 0;

	/* Copy costs, etc., from the original plan */
	cscan->scan.plan.startup_cost = mt->scan.plan.startup_cost;
	cscan->scan.plan.total_cost = mt->scan.plan.total_cost;
	cscan->scan.plan.plan_rows = mt->scan.plan.plan_rows;
	cscan->scan.plan.plan_width = mt->scan.plan.plan_width;

	#else
	ModifyTable *mt =(ModifyTable *) linitial(custom_plans);
	Assert(IsA(mt, ModifyTable));
	cscan->methods = &hypertable_insert_plan_methods;
	cscan->extensible_plans = list_make1(mt);
	cscan->scan.scanrelid = 0;

	/* Copy costs, etc., from the original plan */
	cscan->scan.plan.startup_cost = mt->plan.startup_cost;
	cscan->scan.plan.total_cost = mt->plan.total_cost;
	cscan->scan.plan.plan_rows = mt->plan.plan_rows;
	cscan->scan.plan.plan_width = mt->plan.plan_width;
	#endif

	
	/* The tlist is always NIL since the ModifyTable subplan doesn't have its
	 * targetlist set until set_plan_references (setrefs.c) is run */
	#ifndef OG30
	Assert(tlist == NIL);
	#endif

	/* Target list handling here needs special attention. Intuitively, we'd like
	 * to adopt the target list of the ModifyTable subplan we wrap without
	 * further projection. For a CustomScan this means setting the "input"
	 * custom_scan_tlist to the ModifyTable's target list and having an "output"
	 * targetlist that references the TupleDesc that is created from the
	 * custom_scan_tlist at execution time. Now, while this seems
	 * straight-forward, there are several things with how ModifyTable nodes are
	 * handled in the planner that complicates this:
	 *
	 * - First, ModifyTable doesn't have a targetlist set at this point, and
	 *   it is only set later in set_plan_references (setrefs.c) if there's a
	 *	 RETURNING clause.
	 *
	 * - Second, top-level plan nodes, except for ModifyTable nodes, need to
	 *	 have a targetlist matching root->processed_tlist. This is asserted in
	 *	 apply_tlist_labeling, which is called in create_plan (createplan.c)
	 *	 immediately after this function returns. ModifyTable is exempted
	 *	 because it doesn't always have a targetlist that matches
	 *	 processed_tlist. So, even if we had access to ModifyTable's
	 *	 targetlist here we wouldn't be able to use it since we're a
	 *	 CustomScan and thus not exempted.
	 *
	 * - Third, a CustomScan's targetlist should reference the attributes of the
	 *   TupleDesc that gets created from the custom_scan_tlist at the start of
	 *   execution. This means we need to make the targetlist into all Vars with
	 *   attribute numbers that correspond to the TupleDesc instead of result
	 *   relation in the ModifyTable.
	 *
	 * To get around these issues, we set the targetlist here to
	 * root->processed_tlist, and at the end of planning when the ModifyTable's
	 * targetlist is set, we go back and fix up the CustomScan's targetlist.
	 */
	cscan->scan.plan.targetlist =(List*) copyObject(root->processed_tlist);
	cscan->extensible_plan_tlist = cscan->scan.plan.targetlist;

	/*
	 * we save the original list of arbiter indexes here
	 * because we modify that list during execution and
	 * we still need the original list in case that plan
	 * gets reused
	 */

	return &cscan->scan.plan;
}

static ExtensiblePathMethods hypertable_insert_path_methods = {
	.ExtensibleName = "HypertableInsertPath",
	.PlanExtensiblePath = hypertable_insert_plan_create,
};

Path *
ts_hypertable_insert_path_create(PlannerInfo *root, ModifyTablePath *mtpath)
{
	Path *path = &mtpath->path;
	Cache *hcache = ts_hypertable_cache_pin();
	ListCell *lc_path, *lc_rel;
	List *subpaths = NIL;
	Hypertable *ht = NULL;
	HypertableInsertPath *hipath;

	Assert(list_length(mtpath->subpaths) == list_length(mtpath->resultRelations));

	forboth (lc_path, mtpath->subpaths, lc_rel, mtpath->resultRelations)
	{
		Path *subpath =(Path *) lfirst(lc_path);
		Index rti = lfirst_int(lc_rel);
		RangeTblEntry *rte = planner_rt_fetch(rti, root);

		ht = ts_hypertable_cache_get_entry(hcache, rte->relid, CACHE_FLAG_MISSING_OK);
		
		if (ht != NULL)
		{

			/*
			 * We replace the plan with our custom chunk dispatch plan.
			 */
			subpath = ts_chunk_dispatch_path_create(mtpath, subpath, rti, rte->relid);
		}

		subpaths = lappend(subpaths, subpath);
	}

	Assert(NULL != ht);

	ts_cache_release(hcache);

	hipath =(HypertableInsertPath *) palloc0(sizeof(HypertableInsertPath));

	/* Copy costs, etc. */
	memcpy(&hipath->cpath.path, path, sizeof(Path));
	hipath->cpath.path.type = T_ExtensiblePath;
	hipath->cpath.path.pathtype = T_ExtensiblePlan;
	hipath->cpath.extensible_paths = list_make1(mtpath);
	hipath->cpath.methods = &hypertable_insert_path_methods;
	path = &hipath->cpath.path;
	mtpath->subpaths = subpaths;

	return path;
}
