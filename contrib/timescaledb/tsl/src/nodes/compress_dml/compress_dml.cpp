/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>
#include <optimizer/pathnode.h>
#include <optimizer/paths.h>

#include "compat.h"
#include "chunk.h"
#include "hypertable.h"
#include "hypertable_compression.h"
#include "compress_dml.h"
#include "utils.h"

/*Path, Plan and State node for processing dml on compressed chunks
 * For now, this just blocks updates/deletes on compressed chunks
 * since trigger based approach does not work
 */

static Path *compress_chunk_dml_path_create(Path *subpath, Oid chunk_relid);
static Plan *compress_chunk_dml_plan_create(PlannerInfo *root, RelOptInfo *relopt,
											ExtensiblePath *best_path, List *tlist, List *clauses,
											List *custom_plans);
static Node *compress_chunk_dml_state_create(ExtensiblePlan *scan);

static void compress_chunk_dml_begin(ExtensiblePlanState *node, EState *estate, int eflags);
static TupleTableSlot *compress_chunk_dml_exec(ExtensiblePlanState *node);
static void compress_chunk_dml_end(ExtensiblePlanState *node);
static void compress_chunk_dml_rescan(ExtensiblePlanState *node);

static ExtensiblePathMethods compress_chunk_dml_path_methods = {
	.ExtensibleName = "CompressChunkDml",
	.PlanExtensiblePath = compress_chunk_dml_plan_create,
};

static ExtensiblePlanMethods compress_chunk_dml_plan_methods = {
	.ExtensibleName = "CompressChunkDml",
	.CreateExtensiblePlanState = compress_chunk_dml_state_create,
};

static ExtensibleExecMethods compress_chunk_dml_state_methods = {
	.ExtensibleName = COMPRESS_CHUNK_DML_STATE_NAME,
	.BeginExtensiblePlan = compress_chunk_dml_begin,
	.ExecExtensiblePlan = compress_chunk_dml_exec,
	.EndExtensiblePlan = compress_chunk_dml_end,
	.ReScanExtensiblePlan = compress_chunk_dml_rescan,
};

static void
compress_chunk_dml_begin(ExtensiblePlanState *node, EState *estate, int eflags)
{
	ExtensiblePlan *cscan = castNode(ExtensiblePlan, node->ss.ps.plan);
	Plan *subplan =(Plan *) linitial(cscan->extensible_plans);
	node->extensible_ps = list_make1(ExecInitNode(subplan, estate, eflags));
}

/*
 * nothing to reset for rescan in dml blocker
 */
static void
compress_chunk_dml_rescan(ExtensiblePlanState *node)
{
}

/* we cannot update/delete rows if we have a compressed chunk. so
 * throw an error. Note this subplan will return 0 tuples as the chunk is empty
 * and all rows are saved in the compressed chunk.
 */
static TupleTableSlot *
compress_chunk_dml_exec(ExtensiblePlanState *node)
{
	CompressChunkDmlState *state = (CompressChunkDmlState *) node;
	Oid chunk_relid = state->chunk_relid;
	elog(ERROR,
		 "cannot update/delete rows from chunk \"%s\" as it is compressed",
		 get_rel_name(chunk_relid));
	return NULL;
}

static void
compress_chunk_dml_end(ExtensiblePlanState *node)
{
	PlanState *substate =(PlanState *) linitial(node->extensible_ps);
	ExecEndNode(substate);
}

static Path *
compress_chunk_dml_path_create(Path *subpath, Oid chunk_relid)
{
	CompressChunkDmlPath *path = (CompressChunkDmlPath *) palloc0(sizeof(CompressChunkDmlPath));

	memcpy(&path->cpath.path, subpath, sizeof(Path));
	path->cpath.path.type = T_ExtensiblePath;
	path->cpath.path.pathtype = T_ExtensiblePlan;
	path->cpath.path.parent = subpath->parent;
	path->cpath.methods = &compress_chunk_dml_path_methods;
	path->cpath.extensible_paths = list_make1(subpath);
	path->chunk_relid = chunk_relid;

	return &path->cpath.path;
}

static Plan *
compress_chunk_dml_plan_create(PlannerInfo *root, RelOptInfo *relopt, ExtensiblePath *best_path,
							   List *tlist, List *clauses, List *custom_plans)
{
	CompressChunkDmlPath *cdpath = (CompressChunkDmlPath *) best_path;
	ExtensiblePlan *cscan = makeNode(ExtensiblePlan);

	Assert(list_length(custom_plans) == 1);

	cscan->methods = &compress_chunk_dml_plan_methods;
	cscan->extensible_plans = custom_plans;
	cscan->scan.scanrelid = relopt->relid;
	cscan->scan.plan.targetlist = tlist;
	cscan->extensible_plan_tlist = NIL;
	cscan->extensible_private = list_make1_oid(cdpath->chunk_relid);
	return &cscan->scan.plan;
}

static Node *
compress_chunk_dml_state_create(ExtensiblePlan *scan)
{
	CompressChunkDmlState *state;

	state = (CompressChunkDmlState *) newNode(sizeof(CompressChunkDmlState), T_ExtensiblePlanState);
	state->chunk_relid = linitial_oid(scan->extensible_private);
	state->cscan_state.methods = &compress_chunk_dml_state_methods;
	return (Node *) state;
}

Path *
compress_chunk_dml_generate_paths(Path *subpath, Chunk *chunk)
{
	Assert(chunk->fd.compressed_chunk_id > 0);
	return compress_chunk_dml_path_create(subpath, chunk->table_id);
}
