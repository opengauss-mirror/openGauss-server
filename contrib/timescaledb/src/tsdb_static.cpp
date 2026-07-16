#include "compat.h"

#include "commands/cluster.h"
#include "utils.h"


#include "access/skey.h"
#include "commands/extension.h"
#include "access/heapam.h"

#include "knl/knl_session.h"
#include "knl/knl_guc/knl_session_attr_sql.h"
#include "knl/knl_guc/knl_instance_attr_common.h"
#include "knl/knl_thread.h"

#include "utils/syscache.h"
#include "commands/copy.h"

#include "catalog/pg_type_fn.h"
#include "catalog/pg_ts_config.h"

#include "utils/dynamic_loader.h"
#include "utils/globalplancore.h"

#include "compat/tableam.h"
#include "utils/guc_tables.h"
#include "utils/array.h"
#include "commands/explain.h"
#include "compression/compression.h"
#include "compression/array.h"
#include "compression/dictionary.h"
#include "compression/gorilla.h"
#include "compression/deltadelta.h"

#include "catalog/pg_cast.h"
#include "catalog/pg_database.h"
#include "catalog/pg_conversion.h"
#include "catalog/pg_attrdef.h"
#include "catalog/pg_language.h"
#include "catalog/pg_amop.h"
#include "catalog/pg_amproc.h"
#include "catalog/pg_rewrite.h"
#include "catalog/pg_aggregate.h"

#include "catalog/pg_ts_parser.h"
#include "catalog/pg_ts_dict.h"
#include "catalog/pg_ts_template.h"
#include "catalog/pg_opfamily.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_largeobject_metadata.h"
#include "catalog/pg_foreign_server.h"
#include "storage/procarray.h"
#include "parser/parse_agg.h"
#include "event_trigger.h"
#include "tsdb_static2.cpp"


static Param* generate_new_param(PlannerInfo* root, Oid paramtype, int32 paramtypmod, Oid paramcollation)
{
    Param* retval = NULL;

    retval = makeNode(Param);
    retval->paramkind = PARAM_EXEC;
    retval->paramid = root->glob->nParamExec++;
    retval->paramtype = paramtype;
    retval->paramtypmod = paramtypmod;
    retval->paramcollid = paramcollation;
    retval->location = -1;

    return retval;
}


static void
logical_begin_heap_rewrite(RewriteState state)
{
	HASHCTL		hash_ctl;
	TransactionId logical_xmin;

	/*
	 * We only need to persist these mappings if the rewritten table can be
	 * accessed during logical decoding, if not, we can skip doing any
	 * additional work.
	 */
	state->rs_logical_rewrite =
		RelationIsAccessibleInLogicalDecoding(state->rs_old_rel);

	if (!state->rs_logical_rewrite)
		return;

	ProcArrayGetReplicationSlotXmin(NULL, &logical_xmin);

	/*
	 * If there are no logical slots in progress we don't need to do anything,
	 * there cannot be any remappings for relevant rows yet. The relation's
	 * lock protects us against races.
	 */
	if (logical_xmin == InvalidTransactionId)
	{
		state->rs_logical_rewrite = false;
		return;
	}

	state->rs_logical_xmin = logical_xmin;
	state->rs_begin_lsn = GetXLogInsertRecPtr();
	state->rs_num_rewrite_mappings = 0;

	memset(&hash_ctl, 0, sizeof(hash_ctl));
	hash_ctl.keysize = sizeof(TransactionId);
	hash_ctl.entrysize = sizeof(RewriteMappingFile);
	hash_ctl.hcxt = state->rs_cxt;

	state->rs_logical_mappings =
		hash_create("Logical rewrite mapping",
					128,		/* arbitrary initial size */
					&hash_ctl,
					HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

static int
toast_open_indexes(Relation toastrel,
				   LOCKMODE lock,
				   Relation **toastidxs,
				   int *num_indexes)
{
	int			i = 0;
	int			res = 0;
	bool		found = false;
	List	   *indexlist;
	ListCell   *lc;

	/* Get index list of the toast relation */
	indexlist = RelationGetIndexList(toastrel);
	Assert(indexlist != NIL);

	*num_indexes = list_length(indexlist);

	/* Open all the index relations */
	*toastidxs = (Relation *) palloc(*num_indexes * sizeof(Relation));
	foreach(lc, indexlist)
		(*toastidxs)[i++] = index_open(lfirst_oid(lc), lock);

	/* Fetch the first valid index in list */
	for (i = 0; i < *num_indexes; i++)
	{
		Relation	toastidx = (*toastidxs)[i];

		if (toastidx->rd_index->indisvalid)
		{
			res = i;
			found = true;
			break;
		}
	}

	/*
	 * Free index list, not necessary anymore as relations are opened and a
	 * valid index has been found.
	 */
	list_free(indexlist);

	/*
	 * The toast relation should have one valid index, so something is going
	 * wrong if there is nothing.
	 */
	if (!found)
		elog(ERROR, "no valid index found for toast relation with Oid %u",
			 RelationGetRelid(toastrel));

	return res;
}
static void
toast_close_indexes(Relation *toastidxs, int num_indexes, LOCKMODE lock)
{
	int			i;

	/* Close relations and clean up things */
	for (i = 0; i < num_indexes; i++)
		index_close(toastidxs[i], lock);
	pfree(toastidxs);
}

static bool
get_agg_clause_costs_walker(Node *node, get_agg_clause_costs_context *context)
{
	if (node == NULL)
		return false;
	if (IsA(node, Aggref))
	{
		Aggref	   *aggref = (Aggref *) node;
		AggClauseCosts *costs = context->costs;
		HeapTuple	aggTuple;
		Form_pg_aggregate aggform;
		Oid			aggtransfn;
		Oid			aggfinalfn;
		Oid			aggcombinefn;
		Oid			aggserialfn;
		Oid			aggdeserialfn;
		Oid			aggtranstype;
		int32		aggtransspace = 0;
		QualCost	argcosts;

		Assert(aggref->agglevelsup == 0);

		/*
		 * Fetch info about aggregate from pg_aggregate.  Note it's correct to
		 * ignore the moving-aggregate variant, since what we're concerned
		 * with here is aggregates not window functions.
		 */
		aggTuple = SearchSysCache1(AGGFNOID,
								   ObjectIdGetDatum(aggref->aggfnoid));
		if (!HeapTupleIsValid(aggTuple))
			elog(ERROR, "cache lookup failed for aggregate %u",
				 aggref->aggfnoid);
		aggform = (Form_pg_aggregate) GETSTRUCT(aggTuple);
		ReleaseSysCache(aggTuple);

		/*
		 * Resolve the possibly-polymorphic aggregate transition type, unless
		 * already done in a previous pass over the expression.
		 */
		if (OidIsValid(aggref->aggtranstype))
			aggtranstype = aggref->aggtranstype;
		else
		{
			Oid			inputTypes[FUNC_MAX_ARGS];
			int			numArguments;

			/* extract argument types (ignoring any ORDER BY expressions) */
			numArguments = get_aggregate_argtypes(aggref, inputTypes, FUNC_MAX_ARGS);

			/* resolve actual type of transition state, if polymorphic */
			aggtranstype = resolve_aggregate_transtype(aggref->aggfnoid,
													   aggtranstype,
													   inputTypes,
													   numArguments);
			aggref->aggtranstype = aggtranstype;
		}

		/*
		 * Count it, and check for cases requiring ordered input.  Note that
		 * ordered-set aggs always have nonempty aggorder.  Any ordered-input
		 * case also defeats partial aggregation.
		 */
		costs->numAggs++;
		if (aggref->aggorder != NIL || aggref->aggdistinct != NIL)
		{
			costs->numOrderedAggs++;
		}

		/*
		 * Add the appropriate component function execution costs to
		 * appropriate totals.
		 */
		if (DO_AGGSPLIT_COMBINE(context->aggsplit))
		{
			/* charge for combining previously aggregated states */
			costs->transCost.per_tuple += get_func_cost(aggcombinefn) *u_sess->attr.attr_sql.cpu_operator_cost;
		}
		else
			costs->transCost.per_tuple += get_func_cost(aggtransfn) *u_sess->attr.attr_sql.cpu_operator_cost;
		if (DO_AGGSPLIT_DESERIALIZE(context->aggsplit) &&
			OidIsValid(aggdeserialfn))
			costs->transCost.per_tuple += get_func_cost(aggdeserialfn) *u_sess->attr.attr_sql.cpu_operator_cost;
		if (DO_AGGSPLIT_SERIALIZE(context->aggsplit) &&
			OidIsValid(aggserialfn))
			costs->finalCost += get_func_cost(aggserialfn) *u_sess->attr.attr_sql.cpu_operator_cost;
		if (!DO_AGGSPLIT_SKIPFINAL(context->aggsplit) &&
			OidIsValid(aggfinalfn))
			costs->finalCost += get_func_cost(aggfinalfn) *u_sess->attr.attr_sql.cpu_operator_cost;

		/*
		 * These costs are incurred only by the initial aggregate node, so we
		 * mustn't include them again at upper levels.
		 */
		if (!DO_AGGSPLIT_COMBINE(context->aggsplit))
		{
			/* add the input expressions' cost to per-input-row costs */
			cost_qual_eval_node(&argcosts, (Node *) aggref->args, context->root);
			costs->transCost.startup += argcosts.startup;
			costs->transCost.per_tuple += argcosts.per_tuple;

			/*
			 * Add any filter's cost to per-input-row costs.
			 *
			 * XXX Ideally we should reduce input expression costs according
			 * to filter selectivity, but it's not clear it's worth the
			 * trouble.
			 */
			if (false)
			{
				costs->transCost.startup += argcosts.startup;
				costs->transCost.per_tuple += argcosts.per_tuple;
			}
		}

		/*
		 * If there are direct arguments, treat their evaluation cost like the
		 * cost of the finalfn.
		 */
		if (aggref->aggdirectargs)
		{
			cost_qual_eval_node(&argcosts, (Node *) aggref->aggdirectargs,
								context->root);
			costs->transCost.startup += argcosts.startup;
			costs->finalCost += argcosts.per_tuple;
		}

		/*
		 * If the transition type is pass-by-value then it doesn't add
		 * anything to the required size of the hashtable.  If it is
		 * pass-by-reference then we have to add the estimated size of the
		 * value itself, plus palloc overhead.
		 */
		if (!get_typbyval(aggtranstype))
		{
			int32		avgwidth;

			/* Use average width if aggregate definition gave one */
			if (aggtransspace > 0)
				avgwidth = aggtransspace;
			else if (aggtransfn == F_ARRAY_APPEND)
			{
				/*
				 * If the transition function is array_append(), it'll use an
				 * expanded array as transvalue, which will occupy at least
				 * ALLOCSET_SMALL_INITSIZE and possibly more.  Use that as the
				 * estimate for lack of a better idea.
				 */
				avgwidth = ALLOCSET_SMALL_INITSIZE;
			}
			else
			{
				/*
				 * If transition state is of same type as first aggregated
				 * input, assume it's the same typmod (same width) as well.
				 * This works for cases like MAX/MIN and is probably somewhat
				 * reasonable otherwise.
				 */
				int32		aggtranstypmod = -1;

				if (aggref->args)
				{
					TargetEntry *tle = (TargetEntry *) linitial(aggref->args);

					if (aggtranstype == exprType((Node *) tle->expr))
						aggtranstypmod = exprTypmod((Node *) tle->expr);
				}

				avgwidth = get_typavgwidth(aggtranstype, aggtranstypmod);
			}

			avgwidth = MAXALIGN(avgwidth);
			costs->transitionSpace += avgwidth + 2 * sizeof(void *);
		}
		else if (aggtranstype == INTERNALOID)
		{
			/*
			 * INTERNAL transition type is a special case: although INTERNAL
			 * is pass-by-value, it's almost certainly being used as a pointer
			 * to some large data structure.  The aggregate definition can
			 * provide an estimate of the size.  If it doesn't, then we assume
			 * ALLOCSET_DEFAULT_INITSIZE, which is a good guess if the data is
			 * being kept in a private memory context, as is done by
			 * array_agg() for instance.
			 */
			if (aggtransspace > 0)
				costs->transitionSpace += aggtransspace;
			else
				costs->transitionSpace += ALLOCSET_DEFAULT_INITSIZE;
		}

		/*
		 * We assume that the parser checked that there are no aggregates (of
		 * this level anyway) in the aggregated arguments, direct arguments,
		 * or filter clause.  Hence, we need not recurse into any of them.
		 */
		return false;
	}
	Assert(!IsA(node, SubLink));
	return expression_tree_walker(node,(bool (*)()) get_agg_clause_costs_walker,
								  (void *) context);
}

static bool
SanityCheckBackgroundWorker(BackgroundWorker *worker, int elevel)
{
	/* sanity check for flags */
	if (worker->bgw_flags & BGWORKER_BACKEND_DATABASE_CONNECTION)
	{
		if (!(worker->bgw_flags & BGWORKER_SHMEM_ACCESS))
		{
			ereport(elevel,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("background worker \"%s\": must attach to shared memory in order to request a database connection",
							worker->bgw_name)));
			return false;
		}

		if (worker->bgw_start_time == BgWorkerStart_PostmasterStart)
		{
			ereport(elevel,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("background worker \"%s\": cannot request database access if starting at postmaster start",
							worker->bgw_name)));
			return false;
		}

		/* XXX other checks? */
	}

	if ((worker->bgw_restart_time < 0 &&
		 worker->bgw_restart_time != BGW_NEVER_RESTART) ||
		(worker->bgw_restart_time > USECS_PER_DAY / 1000))
	{
		ereport(elevel,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("background worker \"%s\": invalid restart interval",
						worker->bgw_name)));
		return false;
	}

	return true;
}

static bool
ResourceArrayRemove(ResourceArray *resarr, Datum value)
{
	uint32		i,
				idx,
				lastidx = resarr->lastidx;

	Assert(value != resarr->invalidval);

	/* Search through all items, but try lastidx first. */
	if (RESARRAY_IS_ARRAY(resarr))
	{
		if (lastidx < resarr->nitems &&
			resarr->itemsarr[lastidx] == value)
		{
			resarr->itemsarr[lastidx] = resarr->itemsarr[resarr->nitems - 1];
			resarr->nitems--;
			/* Update lastidx to make reverse-order removals fast. */
			resarr->lastidx = resarr->nitems - 1;
			return true;
		}
		for (i = 0; i < resarr->nitems; i++)
		{
			if (resarr->itemsarr[i] == value)
			{
				resarr->itemsarr[i] = resarr->itemsarr[resarr->nitems - 1];
				resarr->nitems--;
				/* Update lastidx to make reverse-order removals fast. */
				resarr->lastidx = resarr->nitems - 1;
				return true;
			}
		}
	}
	else
	{
		uint32		mask = resarr->capacity - 1;

		if (lastidx < resarr->capacity &&
			resarr->itemsarr[lastidx] == value)
		{
			resarr->itemsarr[lastidx] = resarr->invalidval;
			resarr->nitems--;
			return true;
		}
		Datum tsdb_tem = hash_any((const unsigned char *)&value, sizeof(value));
		idx = DatumGetUInt32(tsdb_tem) & mask;
		for (i = 0; i < resarr->capacity; i++)
		{
			if (resarr->itemsarr[idx] == value)
			{
				resarr->itemsarr[idx] = resarr->invalidval;
				resarr->nitems--;
				return true;
			}
			idx = (idx + 1) & mask;
		}
	}

	return false;
}


static void
ResourceArrayAdd(ResourceArray *resarr, Datum value)
{
	uint32		idx;

	Assert(value != resarr->invalidval);
	Assert(resarr->nitems < resarr->maxitems);

	if (RESARRAY_IS_ARRAY(resarr))
	{
		/* Append to linear array. */
		idx = resarr->nitems;
	}
	else
	{
		/* Insert into first free slot at or after hash location. */
		uint32		mask = resarr->capacity - 1;
		Datum tsdb_tem = hash_any((const unsigned char *)&value, sizeof(value));
		idx = DatumGetUInt32(tsdb_tem) & mask;
		for (;;)
		{
			if (resarr->itemsarr[idx] == resarr->invalidval)
				break;
			idx = (idx + 1) & mask;
		}
	}
	resarr->lastidx = idx;
	resarr->itemsarr[idx] = value;
	resarr->nitems++;
}


static void
ResourceArrayEnlarge(ResourceArray *resarr)
{
	uint32		i,
				oldcap,
				newcap;
	Datum	   *olditemsarr;
	Datum	   *newitemsarr;

	if (resarr->nitems < resarr->maxitems)
		return;					/* no work needed */

	olditemsarr = resarr->itemsarr;
	oldcap = resarr->capacity;

	/* Double the capacity of the array (capacity must stay a power of 2!) */
	newcap = (oldcap > 0) ? oldcap * 2 : RESARRAY_INIT_SIZE;
	newitemsarr = (Datum *) MemoryContextAlloc(TopMemoryContext,
											   newcap * sizeof(Datum));
	for (i = 0; i < newcap; i++)
		newitemsarr[i] = resarr->invalidval;

	/* We assume we can't fail below this point, so OK to scribble on resarr */
	resarr->itemsarr = newitemsarr;
	resarr->capacity = newcap;
	resarr->maxitems = RESARRAY_MAX_ITEMS(newcap);
	resarr->nitems = 0;

	if (olditemsarr != NULL)
	{
		/*
		 * Transfer any pre-existing entries into the new array; they don't
		 * necessarily go where they were before, so this simple logic is the
		 * best way.  Note that if we were managing the set as a simple array,
		 * the entries after nitems are garbage, but that shouldn't matter
		 * because we won't get here unless nitems was equal to oldcap.
		 */
		for (i = 0; i < oldcap; i++)
		{
			if (olditemsarr[i] != resarr->invalidval)
				ResourceArrayAdd(resarr, olditemsarr[i]);
		}

		/* And release old array. */
		pfree(olditemsarr);
	}

	Assert(resarr->nitems < resarr->maxitems);
}

static int
DecodeTextArrayToCString(Datum array, char ***cstringp)
{
	ArrayType  *arr = DatumGetArrayTypeP(array);
	Datum	   *elems;
	char	  **cstring;
	int			i;
	int			nelems;

	if (ARR_NDIM(arr) != 1 || ARR_HASNULL(arr) || ARR_ELEMTYPE(arr) != TEXTOID)
		elog(ERROR, "expected 1-D text array");
	deconstruct_array(arr, TEXTOID, -1, false, 'i', &elems, NULL, &nelems);

	cstring =(char **) palloc(nelems * sizeof(char *));
	for (i = 0; i < nelems; ++i)
		cstring[i] = TextDatumGetCString(elems[i]);

	pfree(elems);
	*cstringp = cstring;
	return nelems;
}


static bool index_recheck_constraint(
    Relation index, Oid* constr_procs, Datum* existing_values, const bool* existing_isnull, Datum* new_values)
{
    int indnkeyatts = IndexRelationGetNumberOfKeyAttributes(index);
    int i;

    for (i = 0; i < indnkeyatts; i++) {
        /* Assume the exclusion operators are strict */
        if (existing_isnull[i]) {
            return false;
        }

        if (!DatumGetBool(
                OidFunctionCall2Coll(constr_procs[i], index->rd_indcollation[i], existing_values[i], new_values[i]))) {
            return false;
        }
    }

    return true;
} 

static bool
check_exclusion_or_unique_constraint(Relation heap, Relation index,
									 IndexInfo *indexInfo,
									 ItemPointer tupleid,
									 Datum *values, bool *isnull,
									 EState *estate, bool newIndex,
									 CEOUC_WAIT_MODE waitMode,
									 bool violationOK,
									 ItemPointer conflictTid)
{
	Oid		   *constr_procs;
	uint16	   *constr_strats;
	Oid		   *index_collations = index->rd_indcollation;
	int			index_natts = index->rd_index->indnatts;
	IndexScanDesc index_scan;
	HeapTuple	tup;
	ScanKeyData scankeys[INDEX_MAX_KEYS];
	SnapshotData DirtySnapshot;
	int			i;
	bool		conflict;
	bool		found_self;
	ExprContext *econtext;
	TupleTableSlot *existing_slot;
	TupleTableSlot *save_scantuple;

	if (indexInfo->ii_ExclusionOps)
	{
		constr_procs = indexInfo->ii_ExclusionProcs;
		constr_strats = indexInfo->ii_ExclusionStrats;
	}
	else
	{
		constr_procs = indexInfo->ii_UniqueProcs;
		constr_strats = indexInfo->ii_UniqueStrats;
	}

	/*
	 * If any of the input values are NULL, the constraint check is assumed to
	 * pass (i.e., we assume the operators are strict).
	 */
	for (i = 0; i < index_natts; i++)
	{
		if (isnull[i])
			return true;
	}

	/*
	 * Search the tuples that are in the index for any violations, including
	 * tuples that aren't visible yet.
	 */
	InitDirtySnapshot(DirtySnapshot);

	for (i = 0; i < index_natts; i++)
	{
		ScanKeyEntryInitialize(&scankeys[i],
							   0,
							   i + 1,
							   constr_strats[i],
							   InvalidOid,
							   index_collations[i],
							   constr_procs[i],
							   values[i]);
	}

	/*
	 * Need a TupleTableSlot to put existing tuples in.
	 *
	 * To use FormIndexDatum, we have to make the econtext's scantuple point
	 * to this slot.  Be sure to save and restore caller's value for
	 * scantuple.
	 */
	existing_slot = MakeSingleTupleTableSlot(RelationGetDescr(heap));

	econtext = GetPerTupleExprContext(estate);
	save_scantuple = econtext->ecxt_scantuple;
	econtext->ecxt_scantuple = existing_slot;

	/*
	 * May have to restart scan from this point if a potential conflict is
	 * found.
	 */
retry:
	conflict = false;
	found_self = false;
	index_scan = index_beginscan(heap, index, &DirtySnapshot, index_natts, 0);
	index_rescan(index_scan, scankeys, index_natts, NULL, 0);

	while ((tup =(HeapTuple) index_getnext(index_scan,
								ForwardScanDirection)) != NULL
								)
	{
		TransactionId xwait;
		ItemPointerData ctid_wait;
		XLTW_Oper	reason_wait;
		Datum		existing_values[INDEX_MAX_KEYS];
		bool		existing_isnull[INDEX_MAX_KEYS];
		char	   *error_new;
		char	   *error_existing;

		/*
		 * Ignore the entry for the tuple we're trying to check.
		 */
		if (ItemPointerIsValid(tupleid) &&
			ItemPointerEquals(tupleid, &tup->t_self))
		{
			if (found_self)		/* should not happen */
				elog(ERROR, "found self tuple multiple times in index \"%s\"",
					 RelationGetRelationName(index));
			found_self = true;
			continue;
		}

		/*
		 * Extract the index column values and isnull flags from the existing
		 * tuple.
		 */
		ExecStoreTuple(tup, existing_slot, InvalidBuffer, false);
		FormIndexDatum(indexInfo, existing_slot, estate,
					   existing_values, existing_isnull);

		/* If lossy indexscan, must recheck the condition */
		if (index_scan->xs_recheck)
		{
			if (!index_recheck_constraint(index,
										  constr_procs,
										  existing_values,
										  existing_isnull,
										  values))
				continue;		/* tuple doesn't actually match, so no
								 * conflict */
		}

		/*
		 * At this point we have either a conflict or a potential conflict.
		 *
		 * If an in-progress transaction is affecting the visibility of this
		 * tuple, we need to wait for it to complete and then recheck (unless
		 * the caller requested not to).  For simplicity we do rechecking by
		 * just restarting the whole scan --- this case probably doesn't
		 * happen often enough to be worth trying harder, and anyway we don't
		 * want to hold any index internal locks while waiting.
		 */
		xwait = TransactionIdIsValid(DirtySnapshot.xmin) ?
			DirtySnapshot.xmin : DirtySnapshot.xmax;

		if (TransactionIdIsValid(xwait) &&
			(waitMode == CEOUC_WAIT ||
			 (waitMode == CEOUC_LIVELOCK_PREVENTING_WAIT &&
			  TransactionIdPrecedes(GetCurrentTransactionId(), xwait))))
		{
			ctid_wait = tup->t_data->t_ctid;
			reason_wait = indexInfo->ii_ExclusionOps ?
				XLTW_RecheckExclusionConstr : XLTW_InsertIndex;
			index_endscan(index_scan);
			if (0)
				SpeculativeInsertionWait(DirtySnapshot.xmin,
										 0);
			else
				XactLockTableWait(xwait, heap,reason_wait);
			goto retry;
		}

		/*
		 * We have a definite conflict (or a potential one, but the caller
		 * didn't want to wait).  Return it to caller, or report it.
		 */
		if (violationOK)
		{
			conflict = true;
			if (conflictTid)
				*conflictTid = tup->t_self;
			break;
		}

		error_new = BuildIndexValueDescription(index, values, isnull);
		error_existing = BuildIndexValueDescription(index, existing_values,
													existing_isnull);
		if (newIndex)
			ereport(ERROR,
					(errcode(ERRCODE_EXCLUSION_VIOLATION),
					 errmsg("could not create exclusion constraint \"%s\"",
							RelationGetRelationName(index)),
					 error_new && error_existing ?
					 errdetail("Key %s conflicts with key %s.",
							   error_new, error_existing) :
					 errdetail("Key conflicts exist.")
					 ));
		else
			ereport(ERROR,
					(errcode(ERRCODE_EXCLUSION_VIOLATION),
					 errmsg("conflicting key value violates exclusion constraint \"%s\"",
							RelationGetRelationName(index)),
					 error_new && error_existing ?
					 errdetail("Key %s conflicts with existing key %s.",
							   error_new, error_existing) :
					 errdetail("Key conflicts with existing key.")
					 ));
	}

	index_endscan(index_scan);

	/*
	 * Ordinarily, at this point the search should have found the originally
	 * inserted tuple (if any), unless we exited the loop early because of
	 * conflict.  However, it is possible to define exclusion constraints for
	 * which that wouldn't be true --- for instance, if the operator is <>. So
	 * we no longer complain if found_self is still false.
	 */

	econtext->ecxt_scantuple = save_scantuple;

	ExecDropSingleTupleTableSlot(existing_slot);

	return !conflict;
}

static void
error_duplicate_filter_variable(const char *defname)
{
	ereport(ERROR,
			(errcode(ERRCODE_SYNTAX_ERROR),
			 errmsg("filter variable \"%s\" specified more than once",
					defname)));
} 

static Node *
make_agg_arg(Oid argtype, Oid argcollation)
{
	Param	   *argp = makeNode(Param);

	argp->paramkind = PARAM_EXEC;
	argp->paramid = -1;
	argp->paramtype = argtype;
	argp->paramtypmod = -1;
	argp->paramcollid = argcollation;
	argp->location = -1;
	return (Node *) argp;
} 


static void deform_next_attribute(bool& slow, long& off, Form_pg_attribute thisatt, char* tp)
{
    if (!slow && thisatt->attcacheoff >= 0) {
        off = thisatt->attcacheoff;
    } else if (thisatt->attlen == -1) {
        /*
         * We can only cache the offset for a varlena attribute if the
         * offset is already suitably aligned, so that there would be no
         * pad bytes in any case: then the offset will be valid for either
         * an aligned or unaligned value.
         */
        if (!slow && (uintptr_t)(off) == att_align_nominal(off, thisatt->attalign)) {
            thisatt->attcacheoff = off;
        } else {
            off = att_align_pointer(off, thisatt->attalign, -1, tp + off);
            slow = true;
        }
    } else {
        /* not varlena, so safe to use att_align_nominal */
        off = att_align_nominal(off, thisatt->attalign);

        if (!slow) {
            thisatt->attcacheoff = off;
        }
    }
}

static void slot_deform_tuple(TupleTableSlot *slot, uint32 natts)
{
    HeapTuple tuple = (HeapTuple)slot->tts_tuple;
    Assert(tuple->tupTableType == HEAP_TUPLE);
    TupleDesc tupleDesc = slot->tts_tupleDescriptor;
    Datum *values = slot->tts_values;
    bool *isnull = slot->tts_isnull;
    HeapTupleHeader tup = tuple->t_data;
    bool hasnulls = HeapTupleHasNulls(tuple);
    FormData_pg_attribute *att = tupleDesc->attrs;
    uint32 attnum;
    char *tp = NULL;         /* ptr to tuple data */
    long off;                /* offset in tuple data */
    bits8 *bp = tup->t_bits; /* ptr to null bitmap in tuple */
    bool slow = false;       /* can we use/set attcacheoff? */
    bool heapToUHeap = tupleDesc->td_tam_ops == TableAmUstore;
	
    /*
     * Check whether the first call for this tuple, and initialize or restore
     * loop state.
     */
    attnum = slot->tts_nvalid;
    if (attnum == 0) {
        /* Start from the first attribute */
        off = 0;
        slow = false;
    } else {
        /* Restore state from previous execution */
        off = slot->tts_off;
        slow = TTS_SLOW(slot);
    }

    /*
     * Ustore has different alignment rules so we force slow = true here.
     * See the comments in heap_deform_tuple() for more information.
     */
    slow = heapToUHeap ? true : slow;

    tp = (char *)tup + tup->t_hoff;

    for (; attnum < natts; attnum++) {
        Form_pg_attribute thisatt = &att[attnum];

        if (hasnulls && att_isnull(attnum, bp)) {
            values[attnum] = (Datum)0;
            isnull[attnum] = true;
            slow = true; /* can't use attcacheoff anymore */
            continue;
        }

        isnull[attnum] = false;

        deform_next_attribute(slow, off, thisatt, tp);

        values[attnum] = fetchatt(thisatt, tp + off);

        off = att_addlength_pointer(off, thisatt->attlen, tp + off);

        if (thisatt->attlen <= 0) {
            slow = true; /* can't use attcacheoff anymore */
        }
    }

    /*
     * Save state for next execution
     */
    slot->tts_nvalid = attnum;
    slot->tts_off = off;
    if (slow)
        slot->tts_flags |= TTS_FLAG_SLOW;
    else
        slot->tts_flags &= ~TTS_FLAG_SLOW;
}