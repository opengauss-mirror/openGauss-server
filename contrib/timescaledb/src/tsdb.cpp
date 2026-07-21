#include "compat.h"

#include "chunk_append/exec.h"
#include "constraint_aware_append.h"
#include "hypertable_cache.h"
#include "cache_invalidate.h"
#include "chunk_append/planner.h"
#include "process_utility.h"

#include "commands/cluster.h"
#include "utils.h"
#include "plan_agg_bookend.cpp"
#include "access/skey.h"
#include "commands/extension.h"

#include "knl/knl_session.h"
#include "knl/knl_guc/knl_session_attr_sql.h"
#include "knl/knl_guc/knl_instance_attr_common.h"
#include "knl/knl_thread.h"

#include "utils/syscache.h"


#include "catalog/pg_type_fn.h"
#include "catalog/pg_ts_config.h"
#include "catalog/namespace.h"

#include "utils/dynamic_loader.h"
#include "utils/globalplancore.h"

#include "compat/tableam.h"
#include "utils/guc_tables.h"
#include "utils/array.h"
#include "storage/procarray.h"

#include "commands/sequence.h"
#include "parser/parse_utilcmd.h"
#include "parser/parse_collate.h"
#include "parser/parse_relation.h"
#include "storage/predicate.h"
#include "commands/tablecmds.h"

#include "libpq/pqsignal.h"
#include "catalog/pg_authid.h"
#include "commands/dbcommands.h"
#include "nodes/pg_list.h"

#include "access/reloptions.h"
#include "access/visibilitymap.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/prep.h"
#include "optimizer/predtest.h"
#include "utils/acl.h"
#include "catalog/toasting.h"


#include "plan_agg_bookend.h"

#include "tsdb_dsm.h"
#include "tsdb_static.cpp"

#define BUFFER_MAPPING_LWLOCK_OFFSET	NUM_INDIVIDUAL_LWLOCKS 
#define DynamicSharedMemoryControlLock (&t_thrd.shemem_ptr_cxt.mainLWLockArray[34].lock) 
#define LOCK_MANAGER_LWLOCK_OFFSET		\
	(BUFFER_MAPPING_LWLOCK_OFFSET + NUM_BUFFER_PARTITIONS)
#define NUM_FIXED_LWLOCKS \
	(PREDICATELOCK_MANAGER_LWLOCK_OFFSET + NUM_PREDICATELOCK_PARTITIONS)

void NewRelationCreateToastTable(Oid relOid, Datum reloptions)
{
	AlterTableCreateToastTable(relOid,reloptions,AccessExclusiveLock);
}

void RegisterCustomScanMethods(const ExtensiblePlanMethods *methods)
{
}


Param *
SS_make_initplan_output_param(PlannerInfo *root,
							  Oid resulttype, int32 resulttypmod,
							  Oid resultcollation)
{
	return generate_new_param(root, resulttype, resulttypmod, resultcollation);
}

TableScanDescData *heap_beginscan_catalog(Relation relation, int nkeys,
					   ScanKey key)
{	
	Snapshot	snapshot = RegisterSnapshot(GetCatalogSnapshot());
	
	return tableam_scan_begin(relation, snapshot, nkeys, key);;
}

List* set_deparse_context_planstate(List* dpcontext, Node* planstate, List* ancestors)
{
	return nullptr;
}

void before_shmem_exit(pg_on_exit_callback function, Datum arg)
{
}

void InitPostgres(const char *in_dbname, Oid dboid, const char *username,
 			 Oid useroid, char *out_dbname)
{
}

int
namecpy(Name n1, Name n2)
{
	if (!n1 || !n2)
		return -1;
	StrNCpy(NameStr(*n1), NameStr(*n2), NAMEDATALEN);
	return 0;
}

void
ExecVacuum(VacuumStmt *vacstmt, bool isTopLevel)
{
	VacuumParams params;

	/* sanity checks on options */
	Assert(vacstmt->options & (VACOPT_VACUUM | VACOPT_ANALYZE));
	Assert((vacstmt->options & VACOPT_VACUUM) ||
		   !(vacstmt->options & (VACOPT_FULL | VACOPT_FREEZE)));
	Assert((vacstmt->options & VACOPT_ANALYZE) || vacstmt->va_cols == NIL);
	Assert(!(vacstmt->options & 64));

	/*
	 * All freeze ages are zero if the FREEZE option is given; otherwise pass
	 * them as -1 which means to use the default values.
	 */
	if (vacstmt->options & VACOPT_FREEZE)
	{
		params.freeze_min_age = 0;
		params.freeze_table_age = 0;
		params.multixact_freeze_min_age = 0;
		params.multixact_freeze_table_age = 0;
	}
	else
	{
		params.freeze_min_age = -1;
		params.freeze_table_age = -1;
		params.multixact_freeze_min_age = -1;
		params.multixact_freeze_table_age = -1;
	}

	/* user-invoked vacuum is never "for wraparound" */
	params.is_wraparound = false;

	/* user-invoked vacuum never uses this parameter */
	params.log_min_duration = -1;

	/* Now go through the common routine */
	vacuum(vacstmt,
		   vacstmt->rely_oid,
		   InvalidOid,
		   NULL,
		   isTopLevel);
}

/*
 * Block/unblock signals in a background worker
 */
void
BackgroundWorkerBlockSignals(void)
{
	PG_SETMASK(&t_thrd.libpq_cxt.BlockSig);
}

void
BackgroundWorkerUnblockSignals(void)
{
	PG_SETMASK(&t_thrd.libpq_cxt.UnBlockSig);
}

/*
 * Connect background worker to a database.
 */
void
BackgroundWorkerInitializeConnection(char *dbname, char *username)
{
	BackgroundWorker *worker = MyBgworkerEntry;

	/* XXX is this the right errcode? */
	if (!(worker->bgw_flags & BGWORKER_BACKEND_DATABASE_CONNECTION))
		ereport(FATAL,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("database connection requirement not indicated during registration")));

	InitPostgres(dbname, InvalidOid, username, InvalidOid, NULL);

	/* it had better not gotten out of "init" mode yet */
	if (!IsInitProcessingMode())
		ereport(ERROR, (errmsg("invalid processing mode in background worker")));
	SetProcessingMode(NormalProcessing);
}

BgwHandleStatus
WaitForBackgroundWorkerStartup(BackgroundWorkerHandle *handle, pid_t *pidp)
{
	  BgwHandleStatus status;
	  int     rc;

	  for (;;)
 
	{
		    pid_t    pid;

		    CHECK_FOR_INTERRUPTS();

		    status = GetBackgroundWorkerPid(handle, &pid);
		    if (status == BGW_STARTED)
      *pidp = pid;
		    if (status != BGW_NOT_YET_STARTED)
      break;

		    rc = WaitLatch(&t_thrd.proc->procLatch,
           WL_LATCH_SET | WL_POSTMASTER_DEATH, 0);

		    if (rc & WL_POSTMASTER_DEATH)
   
		{
			      break;
			   
		}

		    ResetLatch(&t_thrd.proc->procLatch);
		 
	}

	  return status;
}

bool
RegisterDynamicBackgroundWorker(BackgroundWorker *worker,
                BackgroundWorkerHandle **handle)
{
	  int     slotno;
	  bool    success = false;
	  uint64   generation = 0;

	  /*
	  * We can't register dynamic background workers from the postmaster. If
	  * this is a standalone backend, we're the only process and can't start
	  * any more. In a multi-process environment, it might be theoretically
	  * possible, but we don't currently support it due to locking
	  * considerations; see comments on the BackgroundWorkerSlot data
	  * structure.
	  */
  if (!IsUnderPostmaster)
    return false;

	  if (!SanityCheckBackgroundWorker(worker, ERROR))
    return false;

	  LWLockAcquire(BackgroundWorkerLock, LW_EXCLUSIVE);

	  /*
	  * Look for an unused slot. If we find one, grab it.
	  */
  for (slotno = 0; slotno < BackgroundWorkerData->total_slots; ++slotno)
 
	{
		    BackgroundWorkerSlot *slot = &BackgroundWorkerData->slot[slotno];

		    if (!slot->in_use)
   
		{
			      memcpy(&slot->worker, worker, sizeof(BackgroundWorker));
			      slot->pid = InvalidPid;
			  /* indicates not started yet */
      slot->generation++;
			      slot->terminate = false;
			      generation = slot->generation;

			      /*
			      * Make sure postmaster doesn't see the slot as in use before it
			      * sees the new contents.
			      */
      pg_write_barrier();

			      slot->in_use = true;
			      success = true;
			      break;
			   
		}
		 
	}

	  LWLockRelease(BackgroundWorkerLock);

	  /* If we found a slot, tell the postmaster to notice the change. */
  if (success)
    SendPostmasterSignal((PMSignalReason)6);

	  /*
	  * If we found a slot and the user has provided a handle, initialize it.
	  */
  if (success && handle)
 
	{
		    *handle =(BackgroundWorkerHandle*) palloc(sizeof(BackgroundWorkerHandle));
		    (*handle)->slot = slotno;
		    (*handle)->generation = generation;
		 
	}

	  return success;
}





void
BackgroundWorkerInitializeConnectionByOid(Oid dboid, Oid useroid)
{
	  BackgroundWorker *worker = MyBgworkerEntry;

	  /* XXX is this the right errcode? */
  if (!(worker->bgw_flags & BGWORKER_BACKEND_DATABASE_CONNECTION))
    ereport(FATAL,
        (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
        errmsg("database connection requirement not indicated during registration")));

	  InitPostgres(NULL, dboid, NULL, useroid, NULL);

	  /* it had better not gotten out of "init" mode yet */
  if (!IsInitProcessingMode())
    ereport(ERROR,
        (errmsg("invalid processing mode in background worker")));
	  SetProcessingMode(NormalProcessing);
}

AggPath *
create_agg_path(PlannerInfo *root, RelOptInfo *rel, Path *subpath, PathTarget *target,
				AggStrategy aggstrategy, AggSplit aggsplit, List *groupClause, List *qual,
				const AggClauseCosts *aggcosts, double numGroups)
{
	AggPath *pathnode = makeNode(AggPath);

	pathnode->path.pathtype = T_Agg;
	pathnode->path.parent = rel;
	/* For now, assume we are above any joins, so no parameterization */
	pathnode->path.param_info = NULL;
	
	if (aggstrategy == AGG_SORTED)
		pathnode->path.pathkeys = subpath->pathkeys; /* preserves order */
	else
		pathnode->path.pathkeys = NIL; /* output is unordered */
	pathnode->subpath = subpath;

	pathnode->aggstrategy = aggstrategy;
	pathnode->aggsplit = aggsplit;
	pathnode->numGroups = numGroups;
	pathnode->groupClause = groupClause;
	pathnode->qual = qual;

	cost_agg(&pathnode->path,
			 root,
			 aggstrategy,
			 aggcosts,
			 list_length(groupClause),
			 numGroups,
			 subpath->startup_cost,
			 subpath->total_cost,
			 subpath->rows);

	/* add tlist eval cost for each output row */
	pathnode->path.startup_cost += target->cost.startup;
	pathnode->path.total_cost +=
		target->cost.startup + target->cost.per_tuple * pathnode->path.rows;

	return pathnode;
}

void
add_partial_path(RelOptInfo *parent_rel, Path *new_path)
{
	  bool    accept_new = true;
	  /* unless we find a superior old path */
  ListCell  *insert_after = NULL;
	 /* where to insert new item */
  ListCell  *p1;
	  ListCell  *p1_prev;
	  ListCell  *p1_next;

	  /* Check for query cancel. */
  CHECK_FOR_INTERRUPTS();

	  /*
	  * As in add_path, throw out any paths which are dominated by the new
	  * path, but throw out the new path if some existing path dominates it.
	  */
  p1_prev = NULL;

	  if (accept_new)
 
	{
		// do nothing
	}
	  else  
	{
		    /* we should not see IndexPaths here, so always safe to delete */
    Assert(!IsA(new_path, IndexPath));
		    /* Reject and recycle the new path */
    pfree(new_path);
		 
	}
}

GatherPath *
create_gather_path(PlannerInfo *root, RelOptInfo *rel, Path *subpath,
         PathTarget *target, Relids required_outer, double *rows)
{
	  GatherPath *pathnode = makeNode(GatherPath);


	  pathnode->path.pathtype = T_Gather;
	  pathnode->path.parent = rel;
	  pathnode->path.param_info = get_baserel_parampathinfo(root, rel,
                             required_outer);
	  pathnode->path.pathkeys = NIL;
	  /* Gather has unordered result */

      pathnode->subpath = subpath;
	  pathnode->single_copy = false;

	  if (pathnode->num_workers == 0)
 
	{
		    pathnode->path.pathkeys = subpath->pathkeys;
		    pathnode->num_workers = 1;
		    pathnode->single_copy = true;
		 
	}

	  cost_gather(pathnode, root, rel, pathnode->path.param_info, rows);

	  return pathnode;
}

void
get_agg_clause_costs(PlannerInfo *root, Node *clause, AggSplit aggsplit,
          AggClauseCosts *costs)
{
	  get_agg_clause_costs_context context;

	  context.root = root;
	  context.aggsplit = aggsplit;
	  context.costs = costs;
	  (void) get_agg_clause_costs_walker(clause, &context);
}

void
add_path(RelOptInfo *parent_rel, Path *new_path)
{
	  bool    accept_new = true;
	  /* unless we find a superior old path */
  ListCell  *insert_after = NULL;
	 /* where to insert new item */
  List    *new_path_pathkeys;
	  ListCell  *p1;
	  ListCell  *p1_prev;
	  ListCell  *p1_next;

	  /*
	  * This is a convenient place to check for query cancel --- no part of the
	  * planner goes very long without calling add_path().
	  */
  CHECK_FOR_INTERRUPTS();

	  /* Pretend parameterized paths have no pathkeys, per comment above */
  new_path_pathkeys = new_path->param_info ? NIL : new_path->pathkeys;

	  /*
	  * Loop to check proposed new path against old paths. Note it is possible
	  * for more than one old path to be tossed out because new_path dominates
	  * it.
	  *
	  * We can't use foreach here because the loop body may delete the current
	  * list cell.
	  */
  p1_prev = NULL;
	  for (p1 = list_head(parent_rel->pathlist); p1 != NULL; p1 = p1_next)
 
	{
		    Path    *old_path = (Path *) lfirst(p1);
		    bool    remove_old = false; /* unless new proves superior */
		    PathCostComparison costcmp;
		    PathKeysComparison keyscmp;
		    BMS_Comparison outercmp;

		    p1_next = lnext(p1);

		    /*
		    * Do a fuzzy cost comparison with standard fuzziness limit.
		    */
    costcmp = compare_path_costs_fuzzily(new_path, old_path,
                      STD_FUZZ_FACTOR);

		    /*
		    * If the two paths compare differently for startup and total cost,
		    * then we want to keep both, and we can skip comparing pathkeys and
		    * required_outer rels. If they compare the same, proceed with the
		    * other comparisons. Row count is checked last. (We make the tests
		    * in this order because the cost comparison is most likely to turn
		    * out "different", and the pathkeys comparison next most likely. As
		    * explained above, row count very seldom makes a difference, so even
		    * though it's cheap to compare there's not much point in checking it
		    * earlier.)
		    */
    if (costcmp != COSTS_DIFFERENT)
   
		{
			      /* Similarly check to see if either dominates on pathkeys */
      List    *old_path_pathkeys;

			      old_path_pathkeys = old_path->param_info ? NIL : old_path->pathkeys;
			      keyscmp = compare_pathkeys(new_path_pathkeys,
                   old_path_pathkeys);
			      if (keyscmp != PATHKEYS_DIFFERENT)
     
			{
				        switch (costcmp)
       
				{
					          case COSTS_EQUAL:
            outercmp = bms_subset_compare(PATH_REQ_OUTER(new_path),
                         PATH_REQ_OUTER(old_path));
					            if (keyscmp == PATHKEYS_BETTER1)
           
					{
						              if ((outercmp == BMS_EQUAL ||
                outercmp == BMS_SUBSET1) &&
                new_path->rows <= old_path->rows )
				
                remove_old = true;
						  /* new dominates old */
           
					}
					            else if (keyscmp == PATHKEYS_BETTER2)
           
					{
						              if ((outercmp == BMS_EQUAL ||
                outercmp == BMS_SUBSET2) &&
                new_path->rows >= old_path->rows )
                
                accept_new = false;
						  /* old dominates new */
           
					}
					            else  /* keyscmp == PATHKEYS_EQUAL */
           
					{
						              if (outercmp == BMS_EQUAL)
             
						{
							                /*
							                * Same pathkeys and outer rels, and
							fuzzily                 * the same cost, so keep just
							one; to decide                 * which, first check
							parallel-safety, then                 * rows, then do a
							fuzzy cost comparison with                 * very small
							fuzz limit. (We used to do an                 * exact
							cost comparison, but that results in                 *
							annoying platform-specific plan variations               
							 * due to roundoff in the cost estimates.) If            
							    * things are still tied, arbitrarily keep           
							     * only the old path. Notice that we will          
							      * keep only the old path even if the           
							     * less-fuzzy comparison decides the startup         
							       * and total costs compare differently.         
							       */
				remove_old = true;
						}
						              else if (outercmp == BMS_SUBSET1 &&
                  new_path->rows <= old_path->rows )
                remove_old = true;
						  /* new dominates old */
              else if (outercmp == BMS_SUBSET2 &&
                  new_path->rows >= old_path->rows)
                accept_new = false;
						  /* old dominates new */
              /* else different parameterizations, keep both */
           
					}
					            break;
					          case COSTS_BETTER1:
            if (keyscmp != PATHKEYS_BETTER2)
           
					{
						              outercmp =
							bms_subset_compare(PATH_REQ_OUTER(new_path),
                         PATH_REQ_OUTER(old_path));
						              if ((outercmp == BMS_EQUAL ||
                outercmp == BMS_SUBSET1) &&
                new_path->rows <= old_path->rows)
                remove_old = true;
						  /* new dominates old */
           
					}
					            break;
					          case COSTS_BETTER2:
            if (keyscmp != PATHKEYS_BETTER1)
           
					{
						              outercmp =
							bms_subset_compare(PATH_REQ_OUTER(new_path),
                         PATH_REQ_OUTER(old_path));
						              if ((outercmp == BMS_EQUAL ||
                outercmp == BMS_SUBSET2) &&
                new_path->rows >= old_path->rows )
                accept_new = false;
						  /* old dominates new */
           
					}
					            break;
					          case COSTS_DIFFERENT:

            /*
             * can't get here, but keep this case to keep compiler
             * quiet
             */
            break;
					       
				}
				     
			}
			   
		}

		    /*
		    * Remove current element from pathlist if dominated by new.
		    */
    if (remove_old)
   
		{
			      parent_rel->pathlist = list_delete_cell(parent_rel->pathlist,
                          p1, p1_prev);

			      /*
			      * Delete the data pointed-to by the deleted cell, if possible
			      */
      if (!IsA(old_path, IndexPath))
        pfree(old_path);
			      /* p1_prev does not advance */
   
		}
		    else    
		{
			      /* new belongs after this old path if it has cost >= old's */
      if (new_path->total_cost >= old_path->total_cost)
        insert_after = p1;
			      /* p1_prev advances */
      p1_prev = p1;
			   
		}

		    /*
		    * If we found an old path that dominates new_path, we can quit
		    * scanning the pathlist; we will not add new_path, and we assume
		    * new_path cannot dominate any other elements of the pathlist.
		    */
    if (!accept_new)
      break;
		 
	}

	  if (accept_new)
 
	{
		    /* Accept the new path: insert it at proper place in pathlist */
    if (insert_after)
      lappend_cell(parent_rel->pathlist, insert_after, new_path);
		    else       parent_rel->pathlist = lcons(new_path, parent_rel->pathlist);
		 
	}
	  else  
	{
		    /* Reject and recycle the new path */
    if (!IsA(new_path, IndexPath))
      pfree(new_path);
		 
	}
}




RelOptInfo *
fetch_upper_rel(PlannerInfo *root, UpperRelationKind kind, Relids relids)
{
	  RelOptInfo *upperrel;
	  ListCell  *lc;

	  /*
	  * For the moment, our indexing data structure is just a List for each
	  * relation kind. If we ever get so many of one kind that this stops
	  * working well, we can improve it. No code outside this function should
	  * assume anything about how to find a particular upperrel.
	  */

  /* If we already made this upperrel for the query, return it */
  foreach (lc, root->upper_rels[kind])
 
	{
		    upperrel = (RelOptInfo *) lfirst(lc);

		    if (bms_equal(upperrel->relids, relids))
      return upperrel;
		 
	}

	  upperrel = makeNode(RelOptInfo);
	  upperrel->reloptkind = (RelOptKind)3;
	  upperrel->relids = bms_copy(relids);

	  /* cheap startup cost is interesting iff not all tuples to be retrieved */
	   /* might get changed later */
  	  upperrel->reltarget = create_empty_pathtarget();
	  upperrel->pathlist = NIL;
	  upperrel->cheapest_startup_path = NULL;
	  upperrel->cheapest_total_path = NULL;
	  upperrel->cheapest_unique_path = NULL;
	  upperrel->cheapest_parameterized_paths = NIL;

	  root->upper_rels[kind] = lappend(root->upper_rels[kind], upperrel);

	  return upperrel;
}


MinMaxAggPath *
create_minmaxagg_path(PlannerInfo *root,
           RelOptInfo *rel,
           PathTarget *target,
           List *mmaggregates,
           List *quals)
{
	  MinMaxAggPath *pathnode = makeNode(MinMaxAggPath);
	  Cost    initplan_cost;
	  ListCell  *lc;

	  /* The topmost generated Plan node will be a Result */
  	  pathnode->path.pathtype = T_BaseResult;
	  pathnode->path.parent = rel;
	  /* For now, assume we are above any joins, so no parameterization */
      pathnode->path.param_info = NULL;
	  /* Result is one unordered row */
      pathnode->path.rows = 1;
	  pathnode->path.pathkeys = NIL;

	  pathnode->mmaggregates = mmaggregates;
	  pathnode->quals = quals;

	  /* Calculate cost of all the initplans ... */
  initplan_cost = 0;
	  foreach (lc, mmaggregates)
 
	{
		    MinMaxAggInfo *mminfo = (MinMaxAggInfo *) lfirst(lc);

		    initplan_cost += mminfo->pathcost;
		 
	}
	

	  /* add tlist eval cost for each output row, plus cpu_tuple_cost */
  pathnode->path.startup_cost = initplan_cost + target->cost.startup;
	  pathnode->path.total_cost = initplan_cost + target->cost.startup +
    target->cost.per_tuple +u_sess->attr.attr_sql.cpu_tuple_cost;

	  return pathnode;
}

void
SS_identify_outer_params(PlannerInfo *root)
{
	  Bitmapset *outer_params;
	  PlannerInfo *proot;
	  ListCell  *l;

	  /*
	  * If no parameters have been assigned anywhere in the tree, we certainly
	  * don't need to do anything here.
	  */
  if (root->glob->nParamExec == 0)
    return;

	  /*
	  * Scan all query levels above this one to see which parameters are due to
	  * be available from them, either because lower query levels have
	  * requested them (via plan_params) or because they will be available from
	  * initPlans of those levels.
	  */
  outer_params = NULL;
	  for (proot = root->parent_root; proot != NULL; proot = proot->parent_root)
 
	{
		    /* Include ordinary Var/PHV/Aggref params */
    foreach (l, proot->plan_params)
   
		{
			      PlannerParamItem *pitem = (PlannerParamItem *) lfirst(l);

			      outer_params = bms_add_member(outer_params, pitem->paramId);
			   
		}
		    /* Include any outputs of outer-level initPlans */
    foreach (l, proot->init_plans)
   
		{
			      SubPlan  *initsubplan = (SubPlan *) lfirst(l);
			      ListCell  *l2;

			      foreach (l2, initsubplan->setParam)
     
			{
				        outer_params = bms_add_member(outer_params, lfirst_int(l2));
				     
			}
			   
		}
		    /* Include worktable ID, if a recursive query is being planned */
    if (proot->wt_param_id >= 0)
      outer_params = bms_add_member(outer_params, proot->wt_param_id);
		 
	}
}

void
SS_charge_for_initplans(PlannerInfo *root, RelOptInfo *final_rel)
{
	  Cost    initplan_cost;
	  ListCell  *lc;

	  /* Nothing to do if no initPlans */
  if (root->init_plans == NIL)
    return;

	  /*
	  * Compute the cost increment just once, since it will be the same for all
	  * Paths. We assume each initPlan gets run once during top plan startup.
	  * This is a conservative overestimate, since in fact an initPlan might be
	  * executed later than plan startup, or even not at all.
	  */
  initplan_cost = 0;
	  foreach (lc, root->init_plans)
 
	{
		    SubPlan  *initsubplan = (SubPlan *) lfirst(lc);

		    initplan_cost += initsubplan->startup_cost + initsubplan->per_call_cost;
		 
	}

	  /*
	  * Now adjust the costs and parallel_safe flags.
	  */
  foreach (lc, final_rel->pathlist)
 
	{
		    Path    *path = (Path *) lfirst(lc);

		    path->startup_cost += initplan_cost;
		    path->total_cost += initplan_cost;
		 
	}

	  /* We needn't do set_cheapest() here, caller will do it */
}

Oid
get_role_oid_or_public(const char *rolname)
{
	  if (strcmp(rolname, "public") == 0)
    return ACL_ID_PUBLIC;

	  return get_role_oid(rolname, false);
}


Oid
get_rolespec_oid(const Node *node, bool missing_ok)
{
	  RoleSpec  *role;
	  Oid     oid;

	  

	  role = (RoleSpec *) node;
	  switch (role->roletype)
 
	{
		    case ROLESPEC_CSTRING:
      Assert(role->rolename);
		      oid = get_role_oid(role->rolename, missing_ok);
		      break;

		    case ROLESPEC_CURRENT_USER:
      oid = GetUserId();
		      break;

		    case ROLESPEC_SESSION_USER:
      oid = GetSessionUserId();
		      break;

		    case ROLESPEC_PUBLIC:
      ereport(ERROR,
          (errcode(ERRCODE_UNDEFINED_OBJECT),
          errmsg("role \"%s\" does not exist", "public")));
		      oid = InvalidOid;
		 /* make compiler happy */
      break;

		    default:
      elog(ERROR, "unexpected role type %d", role->roletype);
		 
	}

	  return oid;
}

void
CreateCacheMemoryContext(void)
{
	  /*
  * Purely for paranoia, check that context doesn't exist; caller probably
  * did so already.
  */
 if (u_sess->cache_mem_cxt == nullptr ) {
            u_sess->cache_mem_cxt =
                AllocSetContextCreate(u_sess->top_mem_cxt, "SessionCacheMemoryContext", ALLOCSET_DEFAULT_SIZES);
        }

}

AttrNumber *
convert_tuples_by_name_map(TupleDesc indesc,
             TupleDesc outdesc,
             const char *msg)
{
	  AttrNumber *attrMap;
	  int     n;
	  int     i;

	  n = outdesc->natts;
	  attrMap = (AttrNumber *) palloc0(n * sizeof(AttrNumber));
	  for (i = 0; i < n; i++)
 
	{
		    Form_pg_attribute att = &outdesc->attrs[i];
		    char    *attname;
		    Oid     atttypid;
		    int32    atttypmod;
		    int     j;

		    if (att->attisdropped)
      continue;
		     /* attrMap[i] is already 0 */
    attname = NameStr(att->attname);
		    atttypid = att->atttypid;
		    atttypmod = att->atttypmod;
		    for (j = 0; j < indesc->natts; j++)
   
		{
			      att = &indesc->attrs[j];
			      if (att->attisdropped)
        continue;
			      if (strcmp(attname, NameStr(att->attname)) == 0)
     
			{
				        /* Found it, check type */
        if (atttypid != att->atttypid || atttypmod != att->atttypmod)
          ereport(
					ERROR,
              (
						errcode(ERRCODE_DATATYPE_MISMATCH),
              errmsg_internal("%s", _(msg)),
              errdetail(
							"Attribute \"%s\" of type %s does not match corresponding attribute of type %s.",
                   attname,
                   format_type_be(outdesc->tdtypeid),
                   format_type_be(indesc->tdtypeid))));
				        attrMap[i] = (AttrNumber) (j + 1);
				        break;
				     
			}
			   
		}
		    if (attrMap[i] == 0)
      ereport(ERROR,
          (errcode(ERRCODE_DATATYPE_MISMATCH),
          errmsg_internal("%s", _(msg)),
          errdetail("Attribute \"%s\" of type %s does not exist in type %s.",
               attname,
               format_type_be(outdesc->tdtypeid),
               format_type_be(indesc->tdtypeid))));
		 
	}

	  return attrMap;
}

int
check_enable_rls(Oid relid, Oid checkAsUser, bool noError)
{
	  Oid     user_id = checkAsUser ? checkAsUser : GetUserId();
	  HeapTuple  tuple;
	  Form_pg_class classform;
	  bool    relrowsecurity;
	  bool    relforcerowsecurity;
	  bool    amowner;

	  /* Nothing to do for built-in relations */
  if (relid < (Oid) FirstNormalObjectId)
    return RLS_NONE;

	  /* Fetch relation's relrowsecurity and relforcerowsecurity flags */
  tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
	  if (!HeapTupleIsValid(tuple))
    return RLS_NONE;
	  classform = (Form_pg_class) GETSTRUCT(tuple);

	  relrowsecurity = true;
	  relforcerowsecurity = true;

	  ReleaseSysCache(tuple);

	  /* Nothing to do if the relation does not have RLS */
  if (!relrowsecurity)
    return RLS_NONE;

	  /*
	  * BYPASSRLS users always bypass RLS. Note that superusers are always
	  * considered to have BYPASSRLS.
	  *
	  * Return RLS_NONE_ENV to indicate that this decision depends on the
	  * environment (in this case, the user_id).
	  */
  if (has_bypassrls_privilege(user_id))
    return RLS_NONE_ENV;

	  /*
	  * Table owners generally bypass RLS, except if the table has been set (by
	  * an owner) to FORCE ROW SECURITY, and this is not a referential
	  * integrity check.
	  *
	  * Return RLS_NONE_ENV to indicate that this decision depends on the
	  * environment (in this case, the user_id).
	  */
  amowner = pg_class_ownercheck(relid, user_id);
	  if (amowner)
 
	{
		    /*
		    * If FORCE ROW LEVEL SECURITY has been set on the relation then we
		    * should return RLS_ENABLED to indicate that RLS should be applied.
		    * If not, or if we are in an InNoForceRLSOperation context, we return
		    * RLS_NONE_ENV.
		    *
		    * InNoForceRLSOperation indicates that we should not apply RLS even
		    * if the table has FORCE RLS set - IF the current user is the owner.
		    * This is specifically to ensure that referential integrity checks
		    * are able to still run correctly.
		    *
		    * This is intentionally only done after we have checked that the user
		    * is the table owner, which should always be the case for referential
		    * integrity checks.
		    */
    if (!relforcerowsecurity || InNoForceRLSOperation())
      return RLS_NONE_ENV;
		 
	}

	  /*
  * We should apply RLS. However, the user may turn off the row_security
  * GUC to get a forced error instead.
  */
  if (!row_security && !noError)
    ereport(ERROR,
        (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
        errmsg("query would be affected by row-level security policy for table \"%s\"",
            get_rel_name(relid)),
        amowner ? errhint("To disable the policy for the table's owner, use ALTER TABLE NO FORCE ROW LEVEL SECURITY.") : 0));

	  /* RLS should be fully enabled for this relation. */
  return RLS_NONE;
}

void
PreventCommandIfParallelMode(const char *cmdname)
{
	  if (IsInParallelMode())
    ereport(
		ERROR,
        (
			errcode(ERRCODE_INVALID_TRANSACTION_STATE),
    /* translator: %s is name of a SQL command, eg CREATE */
        errmsg(
				"cannot execute %s during a parallel operation",
            cmdname)));
}

List *
ExecInsertIndexTuples(TupleTableSlot *slot,
           ItemPointer tupleid,
           EState *estate,
           bool noDupErr,
           bool *specConflict,
           List *arbiterIndexes)
{
	  List    *result = NIL;
	  ResultRelInfo *resultRelInfo;
	  int     i;
	  int     numIndices;
	  RelationPtr relationDescs;
	  Relation  heapRelation;
	  IndexInfo **indexInfoArray;
	  ExprContext *econtext;
	  Datum    values[INDEX_MAX_KEYS];
	  bool    isnull[INDEX_MAX_KEYS];

	  /*
	  * Get information from the result relation info structure.
	  */
  resultRelInfo = estate->es_result_relation_info;
	  numIndices = resultRelInfo->ri_NumIndices;
	  relationDescs = resultRelInfo->ri_IndexRelationDescs;
	  indexInfoArray = resultRelInfo->ri_IndexRelationInfo;
	  heapRelation = resultRelInfo->ri_RelationDesc;

	  /*
	  * We will use the EState's per-tuple context for evaluating predicates
	  * and index expressions (creating it if it's not already there).
	  */
  econtext = GetPerTupleExprContext(estate);

	  /* Arrange for econtext's scan tuple to be the tuple under test */
  econtext->ecxt_scantuple = slot;

	  /*
	  * for each index, form and insert the index tuple
	  */
  for (i = 0; i < numIndices; i++)
 
	{
		    Relation  indexRelation = relationDescs[i];
		    IndexInfo *indexInfo;
		    bool    applyNoDupErr;
		    IndexUniqueCheck checkUnique;
		    bool    satisfiesConstraint;

		    if (indexRelation == NULL)
      continue;

		    indexInfo = indexInfoArray[i];

		    /* If the index is marked as read-only, ignore it */
    if (!indexInfo->ii_ReadyForInserts)
      continue;

		    /* Check for partial index */
    if (indexInfo->ii_Predicate != NIL)
   
		{
			      List    *predicate;

			      /*
			      * If predicate state not set up yet, create it (in the estate's
			      * per-query context)
			      */
      predicate = indexInfo->ii_PredicateState;
			      if (predicate == NIL)
     
			{
				        predicate = (List *)
          ExecPrepareExpr((Expr *) indexInfo->ii_Predicate,
                  estate);
				        indexInfo->ii_PredicateState = predicate;
				     
			}

			      /* Skip this index-update if the predicate isn't satisfied */
      if (!ExecQual(predicate, econtext, false))
        continue;
			   
		}

		    /*
		    * FormIndexDatum fills in its values and isnull parameters with the
		    * appropriate values for the column(s) of the index.
		    */
    FormIndexDatum(indexInfo,
           slot,
           estate,
           values,
           isnull);

		    /* Check whether to apply noDupErr to this index */
    applyNoDupErr = noDupErr &&
      (arbiterIndexes == NIL ||
      list_member_oid(arbiterIndexes,
              indexRelation->rd_index->indexrelid));

		    /*
		    * The index AM does the actual insertion, plus uniqueness checking.
		    *
		    * For an immediate-mode unique index, we just tell the index AM to
		    * throw error if not unique.
		    *
		    * For a deferrable unique index, we tell the index AM to just detect
		    * possible non-uniqueness, and we add the index OID to the result
		    * list if further checking is needed.
		    *
		    * For a speculative insertion (used by INSERT ... ON CONFLICT), do
		    * the same as for a deferrable unique index.
		    */
    if (!indexRelation->rd_index->indisunique)
      checkUnique = UNIQUE_CHECK_NO;
		    else if (applyNoDupErr)
      checkUnique = UNIQUE_CHECK_PARTIAL;
		    else if (indexRelation->rd_index->indimmediate)
      checkUnique = UNIQUE_CHECK_YES;
		    else       checkUnique = UNIQUE_CHECK_PARTIAL;

		    satisfiesConstraint =
      index_insert(indexRelation, /* index relation */
            values,  /* array of index Datums */
            isnull,  /* null flags */
            tupleid,    /* tid of heap tuple */
            heapRelation, /* heap relation */
            checkUnique);
		/* type of uniqueness check to do */

    /*
     * If the index has an associated exclusion constraint, check that.
     * This is simpler than the process for uniqueness checks since we
     * always insert first and then check. If the constraint is deferred,
     * we check now anyway, but don't throw error on violation or wait for
     * a conclusive outcome from a concurrent insertion; instead we'll
     * queue a recheck event. Similarly, noDupErr callers (speculative
     * inserters) will recheck later, and wait for a conclusive outcome
     * then.
     *
     * An index for an exclusion constraint can't also be UNIQUE (not an
     * essential property, we just don't allow it in the grammar), so no
     * need to preserve the prior state of satisfiesConstraint.
     */
    if (indexInfo->ii_ExclusionOps != NULL)
   
		{
			      bool    violationOK;
			      CEOUC_WAIT_MODE waitMode;

			      if (applyNoDupErr)
     
			{
				        violationOK = true;
				        waitMode = CEOUC_LIVELOCK_PREVENTING_WAIT;
				     
			}
			      else if (!indexRelation->rd_index->indimmediate)
     
			{
				        violationOK = true;
				        waitMode = CEOUC_NOWAIT;
				     
			}
			      else      
			{
				        violationOK = false;
				        waitMode = CEOUC_WAIT;
				     
			}

			      satisfiesConstraint =
        check_exclusion_or_unique_constraint(heapRelation,
                          indexRelation, indexInfo,
                          tupleid, values, isnull,
                          estate, false,
                        waitMode, violationOK, NULL);
			   
		}

		    if ((checkUnique == UNIQUE_CHECK_PARTIAL ||
      indexInfo->ii_ExclusionOps != NULL) &&
      !satisfiesConstraint)
   
		{
			      /*
			      * The tuple potentially violates the uniqueness or exclusion
			      * constraint, so make a note of the index so that we can re-check
			      * it later. Speculative inserters are told if there was a
			      * speculative conflict, since that always requires a restart.
			      */
      result = lappend_oid(result, RelationGetRelid(indexRelation));
			      if (indexRelation->rd_index->indimmediate && specConflict)
        *specConflict = true;
			   
		}
		 
	}

	  return result;
}

void
TerminateBackgroundWorker(BackgroundWorkerHandle *handle)
{
	  BackgroundWorkerSlot *slot;
	  bool    signal_postmaster = false;

	  Assert(handle->slot < g_max_worker_processes);
	  slot = &BackgroundWorkerData->slot[handle->slot];

	  /* Set terminate flag in shared memory, unless slot has been reused. */
  LWLockAcquire(BackgroundWorkerLock, LW_EXCLUSIVE);
	  if (handle->generation == slot->generation)
 
	{
		    slot->terminate = true;
		    signal_postmaster = true;
		 
	}
	  LWLockRelease(BackgroundWorkerLock);

	  /* Make sure the postmaster notices the change to shared memory. */
  if (signal_postmaster)
    SendPostmasterSignal((PMSignalReason)6);
}

BgwHandleStatus
GetBackgroundWorkerPid(BackgroundWorkerHandle *handle, pid_t *pidp)
{
	  BackgroundWorkerSlot *slot;
	  pid_t    pid;

	  Assert(handle->slot < g_max_worker_processes);
	  slot = &BackgroundWorkerData->slot[handle->slot];

	  /*
	  * We could probably arrange to synchronize access to data using memory
	  * barriers only, but for now, let's just keep it simple and grab the
	  * lock. It seems unlikely that there will be enough traffic here to
	  * result in meaningful contention.
	  */
  LWLockAcquire(BackgroundWorkerLock, LW_SHARED);

	  /*
	  * The generation number can't be concurrently changed while we hold the
	  * lock. The pid, which is updated by the postmaster, can change at any
	  * time, but we assume such changes are atomic. So the value we read
	  * won't be garbage, but it might be out of date by the time the caller
	  * examines it (but that's unavoidable anyway).
	  */
  if (handle->generation != slot->generation)
    pid = 0;
	  else     pid = slot->pid;

	  /* All done. */
  LWLockRelease(BackgroundWorkerLock);

	  if (pid == 0)
    return BGW_STOPPED;
	  else if (pid == InvalidPid)
    return BGW_NOT_YET_STARTED;
	  *pidp = pid;
	  return BGW_STARTED;
}

BgwHandleStatus
WaitForBackgroundWorkerShutdown(BackgroundWorkerHandle *handle)
{
	  BgwHandleStatus status;
	  int     rc;

	  for (;;)
 
	{
		    pid_t    pid;

		    CHECK_FOR_INTERRUPTS();

		    status = GetBackgroundWorkerPid(handle, &pid);
		    if (status == BGW_STOPPED)
      break;

		    rc = WaitLatch(&t_thrd.proc->procLatch,
           WL_LATCH_SET | WL_POSTMASTER_DEATH, 0);

		    if (rc & WL_POSTMASTER_DEATH)
   
		{
			      break;
			   
		}

		    ResetLatch(&t_thrd.proc->procLatch);
		 
	}

	  return status;
}

void
CacheRegisterRelcacheCallback(RelcacheCallbackFunction func, Datum arg)
{
	knl_u_inval_context *inval_cxt = &t_thrd.lsc_cxt.lsc->inval_cxt;
	if (inval_cxt->relcache_callback_count >= MAX_RELCACHE_CALLBACKS)
		elog(FATAL, "out of relcache_callback_list slots");

	inval_cxt->relcache_callback_list[inval_cxt->relcache_callback_count].function = func;
	inval_cxt->relcache_callback_list[inval_cxt->relcache_callback_count].arg = arg;

	++inval_cxt->relcache_callback_count;
}





void
get_typlenbyval(Oid typid, int16 *typlen, bool *typbyval)
{
	HeapTuple tp;
	Form_pg_type typtup;

	tp = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
	if (!HeapTupleIsValid(tp))
		elog(ERROR, "cache lookup failed for type %u", typid);
	typtup = (Form_pg_type) GETSTRUCT(tp);
	*typlen = typtup->typlen;
	*typbyval = typtup->typbyval;
	ReleaseSysCache(tp);
}

void
mark_partial_aggref(Aggref *agg, AggSplit aggsplit)
{
	/* aggtranstype should be computed by this point */
	Assert(OidIsValid(agg->aggtranstype));
	/* ... but aggsplit should still be as the parser left it */
	Assert(agg->aggsplit == AGGSTAGE_NORMAL);

	/* Mark the Aggref with the intended partial-aggregation mode */
	agg->aggsplit = aggsplit;

	/*
	 * Adjust result type if needed.  Normally, a partial aggregate returns
	 * the aggregate's transition type; but if that's INTERNAL and we're
	 * serializing, it returns BYTEA instead.
	 */
	if (DO_AGGSPLIT_SKIPFINAL(aggsplit))
	{
		if (agg->aggtranstype == INTERNALOID && DO_AGGSPLIT_SERIALIZE(aggsplit))
			agg->aggtype = BYTEAOID;
		else
			agg->aggtype = agg->aggtranstype;
	}
}

void
slot_getallattrs(TupleTableSlot *slot)
{
	slot_getsomeattrs(slot, slot->tts_tupleDescriptor->natts);
}


List *
RelationGetFKeyList(Relation relation)
{
	List *result;
	Relation conrel;
	SysScanDesc conscan;
	ScanKeyData skey;
	HeapTuple htup;
	List *oldlist;
	MemoryContext oldcxt;

	/* Fast path: if it doesn't have any triggers, it can't have FKs */
	if (!relation->rd_rel->relhastriggers)
		return NIL;

	/*
	 * We build the list we intend to return (in the caller's context) while
	 * doing the scan.  After successfully completing the scan, we copy that
	 * list into the relcache entry.  This avoids cache-context memory leakage
	 * if we get some sort of error partway through.
	 */
	result = NIL;

	/* Prepare to scan pg_constraint for entries having conrelid = this rel. */
	ScanKeyInit(&skey,
				Anum_pg_constraint_conrelid,
				BTEqualStrategyNumber,
				F_OIDEQ,
				ObjectIdGetDatum(RelationGetRelid(relation)));

	conrel = heap_open(ConstraintRelationId, AccessShareLock);
	conscan = systable_beginscan(conrel, ConstraintRelidIndexId, true, NULL, 1, &skey);

	while (HeapTupleIsValid(htup = systable_getnext(conscan)))
	{
		Form_pg_constraint constraint = (Form_pg_constraint) GETSTRUCT(htup);
		ForeignKeyCacheInfo *info;
		Datum adatum;
		bool isnull;
		ArrayType *arr;
		int nelem;

		/* consider only foreign keys */
		if (constraint->contype != CONSTRAINT_FOREIGN)
			continue;

		info = makeNode(ForeignKeyCacheInfo);
		info->conrelid = constraint->conrelid;
		info->confrelid = constraint->confrelid;

		/* Extract data from conkey field */
		adatum = fastgetattr(htup, Anum_pg_constraint_conkey, conrel->rd_att, &isnull);
		if (isnull)
			elog(ERROR, "null conkey for rel %s", RelationGetRelationName(relation));

		arr = DatumGetArrayTypeP(adatum); /* ensure not toasted */
		nelem = ARR_DIMS(arr)[0];
		if (ARR_NDIM(arr) != 1 || nelem < 1 || nelem > INDEX_MAX_KEYS || ARR_HASNULL(arr) ||
			ARR_ELEMTYPE(arr) != INT2OID)
			elog(ERROR, "conkey is not a 1-D smallint array");

		info->nkeys = nelem;
		memcpy(info->conkey, ARR_DATA_PTR(arr), nelem * sizeof(AttrNumber));

		/* Likewise for confkey */
		adatum = fastgetattr(htup, Anum_pg_constraint_confkey, conrel->rd_att, &isnull);
		if (isnull)
			elog(ERROR, "null confkey for rel %s", RelationGetRelationName(relation));

		arr = DatumGetArrayTypeP(adatum); /* ensure not toasted */
		nelem = ARR_DIMS(arr)[0];
		if (ARR_NDIM(arr) != 1 || nelem != info->nkeys || ARR_HASNULL(arr) ||
			ARR_ELEMTYPE(arr) != INT2OID)
			elog(ERROR, "confkey is not a 1-D smallint array");

		memcpy(info->confkey, ARR_DATA_PTR(arr), nelem * sizeof(AttrNumber));

		/* Likewise for conpfeqop */
		adatum = fastgetattr(htup, Anum_pg_constraint_conpfeqop, conrel->rd_att, &isnull);
		if (isnull)
			elog(ERROR, "null conpfeqop for rel %s", RelationGetRelationName(relation));

		arr = DatumGetArrayTypeP(adatum); /* ensure not toasted */
		nelem = ARR_DIMS(arr)[0];
		if (ARR_NDIM(arr) != 1 || nelem != info->nkeys || ARR_HASNULL(arr) ||
			ARR_ELEMTYPE(arr) != OIDOID)
			elog(ERROR, "conpfeqop is not a 1-D OID array");

		memcpy(info->conpfeqop, ARR_DATA_PTR(arr), nelem * sizeof(Oid));

		/* Add FK's node to the result list */
		result = lappend(result, info);
	}

	systable_endscan(conscan);
	heap_close(conrel, AccessShareLock);

	/* Now save a copy of the completed list in the relcache entry. */
	oldcxt = MemoryContextSwitchTo(u_sess->cache_mem_cxt);
	oldlist = NIL;
	MemoryContextSwitchTo(oldcxt);

	/* Don't leak the old list, if there is one */
	list_free_deep(oldlist);

	return result;
}

/*
 * roleSpecsToIds
 *
 * Given a list of RoleSpecs, generate a list of role OIDs in the same order.
 *
 * ROLESPEC_PUBLIC is not allowed.
 */
List *
roleSpecsToIds(List *memberNames)
{
	List *result = NIL;
	ListCell *l;

	foreach (l, memberNames)
	{
		Node *rolespec = (Node *) lfirst(l);
		Oid roleid;

		roleid = get_rolespec_oid(rolespec, false);
		result = lappend_oid(result, roleid);
	}
	return result;
}


NamedLWLockTrancheRequest *NamedLWLockTrancheRequestArray;
LWLockPadded *
GetNamedLWLockTranche(const char *tranche_name)
{
	int lock_pos;
	int i;

	/*
	 * Obtain the position of base address of LWLock belonging to requested
	 * tranche_name in MainLWLockArray.  LWLocks for named tranches are placed
	 * in MainLWLockArray after fixed locks.
	 */
	lock_pos = NUM_FIXED_LWLOCKS;
	for (i = 0; i < NamedLWLockTrancheRequests; i++)
	{
		if (strcmp(NamedLWLockTrancheRequestArray[i].tranche_name, tranche_name) == 0)
			return &t_thrd.shemem_ptr_cxt.mainLWLockArray[lock_pos];

		lock_pos += NamedLWLockTrancheRequestArray[i].num_lwlocks;
	}

	if (i >= NamedLWLockTrancheRequests)
		elog(ERROR, "requested tranche is not registered");

	/* just to keep compiler quiet */
	return NULL;
}

/*
 * RequestNamedLWLockTranche
 *		Request that extra LWLocks be allocated during postmaster
 *		startup.
 *
 * This is only useful for extensions if called from the _PG_init hook
 * of a library that is loaded into the postmaster via
 * shared_preload_libraries.  Once shared memory has been allocated, calls
 * will be ignored.  (We could raise an error, but it seems better to make
 * it a no-op, so that libraries containing such calls can be reloaded if
 * needed.)
 */
void
RequestNamedLWLockTranche(const char *tranche_name, int num_lwlocks)
{
	NamedLWLockTrancheRequest *request;

	if (IsUnderPostmaster || !lock_named_request_allowed)
		return; /* too late */

	if (NamedLWLockTrancheRequestArray == NULL)
	{
		NamedLWLockTrancheRequestsAllocated = 16;
		NamedLWLockTrancheRequestArray =
			(NamedLWLockTrancheRequest *) MemoryContextAlloc(TopMemoryContext,
															 NamedLWLockTrancheRequestsAllocated *
																 sizeof(NamedLWLockTrancheRequest));
	}

	if (NamedLWLockTrancheRequests >= NamedLWLockTrancheRequestsAllocated)
	{
		int i = NamedLWLockTrancheRequestsAllocated;

		while (i <= NamedLWLockTrancheRequests)
			i *= 2;

		NamedLWLockTrancheRequestArray =
			(NamedLWLockTrancheRequest *) repalloc(NamedLWLockTrancheRequestArray,
												   i * sizeof(NamedLWLockTrancheRequest));
		NamedLWLockTrancheRequestsAllocated = i;
	}

	request = &NamedLWLockTrancheRequestArray[NamedLWLockTrancheRequests];
	Assert(strlen(tranche_name) + 1 < NAMEDATALEN);
	StrNCpy(request->tranche_name, tranche_name, NAMEDATALEN);
	request->num_lwlocks = num_lwlocks;
	NamedLWLockTrancheRequests++;
}


Oid
toast_get_valid_index(Oid toastoid, LOCKMODE lock)
{
	int num_indexes;
	int validIndex;
	Oid validIndexOid;
	Relation *toastidxs;
	Relation toastrel;

	/* Open the toast relation */
	toastrel = heap_open(toastoid, lock);

	/* Look for the valid index of the toast relation */
	validIndex = toast_open_indexes(toastrel, lock, &toastidxs, &num_indexes);
	validIndexOid = RelationGetRelid(toastidxs[validIndex]);

	/* Close the toast relation and all its indexes */
	toast_close_indexes(toastidxs, num_indexes, lock);
	heap_close(toastrel, lock);

	return validIndexOid;
}


ColumnDef *
makeColumnDef(const char *colname, Oid typeOid, int32 typmod, Oid collOid)
{
	ColumnDef *n = makeNode(ColumnDef);

	n->colname = pstrdup(colname);
	n->typname = makeTypeNameFromOid(typeOid, typmod);
	n->inhcount = 0;
	n->is_local = true;
	n->is_not_null = false;
	n->is_from_type = false;
	n->storage = 0;
	n->raw_default = NULL;
	n->cooked_default = NULL;
	n->collClause = NULL;
	n->collOid = collOid;
	n->constraints = NIL;
	n->fdwoptions = NIL;
	return n;
}


ArrayBuildStateArr *
initArrayResultArr(Oid array_type, Oid element_type, MemoryContext rcontext, bool subcontext)
{
	ArrayBuildStateArr *astate;
	MemoryContext arr_context = rcontext; /* by default use the parent
										   * ctx */

	/* Lookup element type, unless element_type already provided */
	if (!OidIsValid(element_type))
	{
		element_type = get_element_type(array_type);

		if (!OidIsValid(element_type))
			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
					 errmsg("data type %s is not an array type", format_type_be(array_type))));
	}

	/* Make a temporary context to hold all the junk */
	if (subcontext)
		arr_context =
			AllocSetContextCreate(rcontext, "accumArrayResultArr", ALLOCSET_DEFAULT_SIZES);

	/* Note we initialize all fields to zero */
	astate = (ArrayBuildStateArr *) MemoryContextAllocZero(arr_context, sizeof(ArrayBuildStateArr));
	astate->mcontext = arr_context;
	astate->private_cxt = subcontext;

	/* Save relevant datatype information */
	astate->array_type = array_type;
	astate->element_type = element_type;

	return astate;
}

ArrayBuildState *
initArrayResult(Oid element_type, MemoryContext rcontext, bool subcontext)
{
	ArrayBuildState *astate;
	MemoryContext arr_context = rcontext;

	/* Make a temporary context to hold all the junk */
	if (subcontext)
		arr_context = AllocSetContextCreate(rcontext, "accumArrayResult", ALLOCSET_DEFAULT_SIZES);

	astate = (ArrayBuildState *) MemoryContextAlloc(arr_context, sizeof(ArrayBuildState));
	astate->mcontext = arr_context;
	astate->alen = (subcontext ? 64 : 8); /* arbitrary starting array
										   * size */
	astate->dvalues = (Datum *) MemoryContextAlloc(arr_context, astate->alen * sizeof(Datum));
	astate->dnulls = (bool *) MemoryContextAlloc(arr_context, astate->alen * sizeof(bool));
	astate->nelems = 0;
	astate->element_type = element_type;
	get_typlenbyvalalign(element_type, &astate->typlen, &astate->typbyval, &astate->typalign);

	return astate;
}

ArrayBuildStateArr *
accumArrayResultArr(ArrayBuildStateArr *astate, Datum dvalue, bool disnull, Oid array_type,
					MemoryContext rcontext)
{
	ArrayType *arg;
	MemoryContext oldcontext;
	int *dims, *lbs, ndims, nitems, ndatabytes;
	char *data;
	int i;

	/*
	 * We disallow accumulating null subarrays.  Another plausible definition
	 * is to ignore them, but callers that want that can just skip calling
	 * this function.
	 */
	if (disnull)
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED), errmsg("cannot accumulate null arrays")));

	/* Detoast input array in caller's context */
	arg = DatumGetArrayTypeP(dvalue);

	if (astate == NULL)
		astate = initArrayResultArr(array_type, InvalidOid, rcontext, true);
	else
		Assert(astate->array_type == array_type);

	oldcontext = MemoryContextSwitchTo(astate->mcontext);

	/* Collect this input's dimensions */
	ndims = ARR_NDIM(arg);
	dims = ARR_DIMS(arg);
	lbs = ARR_LBOUND(arg);
	data = ARR_DATA_PTR(arg);
	nitems = ArrayGetNItems(ndims, dims);
	ndatabytes = ARR_SIZE(arg) - ARR_DATA_OFFSET(arg);

	if (astate->ndims == 0)
	{
		/* First input; check/save the dimensionality info */

		/* Should we allow empty inputs and just produce an empty output? */
		if (ndims == 0)
			ereport(ERROR,
					(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
					 errmsg("cannot accumulate empty arrays")));
		if (ndims + 1 > MAXDIM)
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("number of array dimensions (%d) exceeds the maximum allowed (%d)",
							ndims + 1,
							MAXDIM)));

		/*
		 * The output array will have n+1 dimensions, with the ones after the
		 * first matching the input's dimensions.
		 */
		astate->ndims = ndims + 1;
		astate->dims[0] = 0;
		memcpy(&astate->dims[1], dims, ndims * sizeof(int));
		astate->lbs[0] = 1;
		memcpy(&astate->lbs[1], lbs, ndims * sizeof(int));

		/* Allocate at least enough data space for this item */
		astate->abytes = 1024;
		while (astate->abytes <= ndatabytes)
			astate->abytes *= 2;
		astate->data = (char *) palloc(astate->abytes);
	}
	else
	{
		/* Second or later input: must match first input's dimensionality */
		if (astate->ndims != ndims + 1)
			ereport(ERROR,
					(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
					 errmsg("cannot accumulate arrays of different dimensionality")));
		for (i = 0; i < ndims; i++)
		{
			if (astate->dims[i + 1] != dims[i] || astate->lbs[i + 1] != lbs[i])
				ereport(ERROR,
						(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
						 errmsg("cannot accumulate arrays of different dimensionality")));
		}

		/* Enlarge data space if needed */
		if (astate->nbytes + ndatabytes > astate->abytes)
		{
			astate->abytes = Max(astate->abytes * 2, astate->nbytes + ndatabytes);
			astate->data = (char *) repalloc(astate->data, astate->abytes);
		}
	}

	/*
	 * Copy the data portion of the sub-array.  Note we assume that the
	 * advertised data length of the sub-array is properly aligned.  We do not
	 * have to worry about detoasting elements since whatever's in the
	 * sub-array should be OK already.
	 */
	memcpy(astate->data + astate->nbytes, data, ndatabytes);
	astate->nbytes += ndatabytes;

	/* Deal with null bitmap if needed */
	if (astate->nullbitmap || ARR_HASNULL(arg))
	{
		int newnitems = astate->nitems + nitems;

		if (astate->nullbitmap == NULL)
		{
			/*
			 * First input with nulls; we must retrospectively handle any
			 * previous inputs by marking all their items non-null.
			 */
			astate->aitems = 256;
			while (astate->aitems <= newnitems)
				astate->aitems *= 2;
			astate->nullbitmap = (bits8 *) palloc((astate->aitems + 7) / 8);
			array_bitmap_copy(astate->nullbitmap, 0, NULL, 0, astate->nitems);
		}
		else if (newnitems > astate->aitems)
		{
			astate->aitems = Max(astate->aitems * 2, newnitems);
			astate->nullbitmap = (bits8 *) repalloc(astate->nullbitmap, (astate->aitems + 7) / 8);
		}
		array_bitmap_copy(astate->nullbitmap, astate->nitems, ARR_NULLBITMAP(arg), 0, nitems);
	}

	astate->nitems += nitems;
	astate->dims[0] += 1;

	MemoryContextSwitchTo(oldcontext);

	/* Release detoasted copy if any */
	if ((Pointer) arg != DatumGetPointer(dvalue))
		pfree(arg);

	return astate;
}


Datum
makeArrayResultArr(ArrayBuildStateArr *astate, MemoryContext rcontext, bool release)
{
	ArrayType *result;
	MemoryContext oldcontext;

	/* Build the final array result in rcontext */
	oldcontext = MemoryContextSwitchTo(rcontext);

	if (astate->ndims == 0)
	{
		/* No inputs, return empty array */
		result = construct_empty_array(astate->element_type);
	}
	else
	{
		int dataoffset, nbytes;

		/* Compute required space */
		nbytes = astate->nbytes;
		if (astate->nullbitmap != NULL)
		{
			dataoffset = ARR_OVERHEAD_WITHNULLS(astate->ndims, astate->nitems);
			nbytes += dataoffset;
		}
		else
		{
			dataoffset = 0;
			nbytes += ARR_OVERHEAD_NONULLS(astate->ndims);
		}

		result = (ArrayType *) palloc0(nbytes);
		SET_VARSIZE(result, nbytes);
		result->ndim = astate->ndims;
		result->dataoffset = dataoffset;
		result->elemtype = astate->element_type;

		memcpy(ARR_DIMS(result), astate->dims, astate->ndims * sizeof(int));
		memcpy(ARR_LBOUND(result), astate->lbs, astate->ndims * sizeof(int));
		memcpy(ARR_DATA_PTR(result), astate->data, astate->nbytes);

		if (astate->nullbitmap != NULL)
			array_bitmap_copy(ARR_NULLBITMAP(result), 0, astate->nullbitmap, 0, astate->nitems);
	}

	MemoryContextSwitchTo(oldcontext);

	/* Clean up all the junk */
	if (release)
	{
		Assert(astate->private_cxt);
		MemoryContextDelete(astate->mcontext);
	}

	return PointerGetDatum(result);
}


void
check_index_predicates(PlannerInfo *root, RelOptInfo *rel)
{
	List *clauselist;
	bool have_partial;
	bool is_target_rel;
	Relids otherrels;
	ListCell *lc;

	/*
	 * Initialize the indrestrictinfo lists to be identical to
	 * baserestrictinfo, and check whether there are any partial indexes.  If
	 * not, this is all we need to do.
	 */
	have_partial = false;
	foreach (lc, rel->indexlist)
	{
		IndexOptInfo *index = (IndexOptInfo *) lfirst(lc);

		index->indrestrictinfo = rel->baserestrictinfo;
		if (index->indpred)
			have_partial = true;
	}
	if (!have_partial)
		return;

	/*
	 * Construct a list of clauses that we can assume true for the purpose of
	 * proving the index(es) usable.  Restriction clauses for the rel are
	 * always usable, and so are any join clauses that are "movable to" this
	 * rel.  Also, we can consider any EC-derivable join clauses (which must
	 * be "movable to" this rel, by definition).
	 */
	clauselist = list_copy(rel->baserestrictinfo);

	/* Scan the rel's join clauses */
	foreach (lc, rel->joininfo)
	{
		RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);

		/* Check if clause can be moved to this rel */
		if (!join_clause_is_movable_to(rinfo, NULL))
			continue;

		clauselist = lappend(clauselist, rinfo);
	}

	/*
	 * Add on any equivalence-derivable join clauses.  Computing the correct
	 * relid sets for generate_join_implied_equalities is slightly tricky
	 * because the rel could be a child rel rather than a true baserel, and in
	 * that case we must remove its parents' relid(s) from all_baserels.
	 */
	if (rel->reloptkind == RELOPT_OTHER_MEMBER_REL)
		otherrels = bms_difference(root->all_baserels, find_childrel_parents(root, rel));
	else
		otherrels = bms_difference(root->all_baserels, rel->relids);

	if (!bms_is_empty(otherrels))
		clauselist = list_concat(clauselist,
								 generate_join_implied_equalities(root,
																  bms_union(rel->relids, otherrels),
																  otherrels,
																  rel));

	/*
	 * Normally we remove quals that are implied by a partial index's
	 * predicate from indrestrictinfo, indicating that they need not be
	 * checked explicitly by an indexscan plan using this index.  However, if
	 * the rel is a target relation of UPDATE/DELETE/SELECT FOR UPDATE, we
	 * cannot remove such quals from the plan, because they need to be in the
	 * plan so that they will be properly rechecked by EvalPlanQual testing.
	 * Some day we might want to remove such quals from the main plan anyway
	 * and pass them through to EvalPlanQual via a side channel; but for now,
	 * we just don't remove implied quals at all for target relations.
	 */
	is_target_rel = (rel->relid == root->parse->resultRelation ||
					 get_plan_rowmark(root->rowMarks, rel->relid) != NULL);

	/*
	 * Now try to prove each index predicate true, and compute the
	 * indrestrictinfo lists for partial indexes.  Note that we compute the
	 * indrestrictinfo list even for non-predOK indexes; this might seem
	 * wasteful, but we may be able to use such indexes in OR clauses, cf
	 * generate_bitmap_or_paths().
	 */
	foreach (lc, rel->indexlist)
	{
		IndexOptInfo *index = (IndexOptInfo *) lfirst(lc);
		ListCell *lcr;

		if (index->indpred == NIL)
			continue; /* ignore non-partial indexes here */

		if (!index->predOK) /* don't repeat work if already proven OK */
			index->predOK = predicate_implied_by(index->indpred, clauselist);

		/* If rel is an update target, leave indrestrictinfo as set above */
		if (is_target_rel)
			continue;

		/* Else compute indrestrictinfo as the non-implied quals */
		index->indrestrictinfo = NIL;
		foreach (lcr, rel->baserestrictinfo)
		{
			RestrictInfo *rinfo = (RestrictInfo *) lfirst(lcr);

			/* predicate_implied_by() assumes first arg is immutable */
			if (contain_mutable_functions((Node *) rinfo->clause) ||
				!predicate_implied_by(list_make1(rinfo->clause), index->indpred))
				index->indrestrictinfo = lappend(index->indrestrictinfo, rinfo);
		}
	}
}



SortPath *
create_sort_path(PlannerInfo *root, RelOptInfo *rel, Path *subpath, List *pathkeys,
				 double limit_tuples)
{
	SortPath *pathnode = makeNode(SortPath);

	pathnode->path.pathtype = T_Sort;
	pathnode->path.parent = rel;
	/* Sort doesn't project, so use source path's pathtarget */
	/* For now, assume we are above any joins, so no parameterization */
	pathnode->path.param_info = NULL;
	pathnode->path.pathkeys = pathkeys;

	pathnode->subpath = subpath;

	cost_sort(&pathnode->path,
			  pathkeys,
			  subpath->total_cost,
			  subpath->rows,
			  0,
			  0.0, /* XXX comparison_cost shouldn't be 0? */
			  u_sess->opt_cxt.op_work_mem,
			  limit_tuples,
			  root->glob->vectorized);

	return pathnode;
}

void
ts_preprocess_first_last_aggregates(PlannerInfo *root, List *tlist)
{
	Query *parse = root->parse;
	FromExpr *jtnode;
	RangeTblRef *rtr;
	RangeTblEntry *rte;
	List *first_last_aggs;
	RelOptInfo *grouped_rel;
	ListCell *lc;
	List *mm_agg_list;
	MinMaxAggPath *minmaxagg_path;

	/* minmax_aggs list should be empty at this point */
	Assert(root->minmax_aggs == NIL);

	/* Nothing to do if query has no aggregates */
	if (!parse->hasAggs)
		return;

	Assert(!parse->setOperations);  /* shouldn't get here if a setop */
	Assert(parse->rowMarks == NIL); /* nor if FOR UPDATE */

	/*
	 * Reject unoptimizable cases.
	 *
	 * We don't handle the case when agg function is in ORDER BY. The reason
	 * being is that we replace Aggref node before sort keys are being
	 * generated.
	 *
	 * We don't handle GROUP BY or windowing, because our current
	 * implementations of grouping require looking at all the rows anyway, and
	 * so there's not much point in optimizing FIRST/LAST.
	 */
	if (parse->groupClause || list_length(parse->groupingSets) > 1 || parse->hasWindowFuncs ||
		contains_first_last_node(parse->sortClause, tlist))
		return;

	/*
	 * Reject if query contains any CTEs; there's no way to build an indexscan
	 * on one so we couldn't succeed here.  (If the CTEs are unreferenced,
	 * that's not true, but it doesn't seem worth expending cycles to check.)
	 */
	if (parse->cteList)
		return;

	/*
	 * We also restrict the query to reference exactly one table, since join
	 * conditions can't be handled reasonably.  (We could perhaps handle a
	 * query containing cartesian-product joins, but it hardly seems worth the
	 * trouble.)  However, the single table could be buried in several levels
	 * of FromExpr due to subqueries.  Note the "single" table could be an
	 * inheritance parent, too, including the case of a UNION ALL subquery
	 * that's been flattened to an appendrel.
	 */
	jtnode = parse->jointree;
	while (IsA(jtnode, FromExpr))
	{
		if (list_length(jtnode->fromlist) != 1)
			return;
		jtnode =(FromExpr *) linitial(jtnode->fromlist);
	}
	if (!IsA(jtnode, RangeTblRef))
		return;
	rtr = (RangeTblRef *) jtnode;
	rte = planner_rt_fetch(rtr->rtindex, root);
	if (rte->rtekind == RTE_RELATION)
		/* ordinary relation, ok */;
	else if (rte->rtekind == RTE_SUBQUERY && rte->inh)
		/* flattened UNION ALL subquery, ok */;
	else
		return;

	/*
	 * Scan the tlist and HAVING qual to find all the aggregates and verify
	 * all are FIRST/LAST aggregates.  Stop as soon as we find one that isn't.
	 */
	first_last_aggs = NIL;
	if (find_first_last_aggs_walker((Node *) tlist, &first_last_aggs))
		return;
	if (find_first_last_aggs_walker(parse->havingQual, &first_last_aggs))
		return;

	/*
	 * OK, there is at least the possibility of performing the optimization.
	 * Build an access path for each aggregate.  If any of the aggregates
	 * prove to be non-indexable, give up; there is no point in optimizing
	 * just some of them.
	 */
	foreach (lc, first_last_aggs)
	{
		FirstLastAggInfo *fl_info = (FirstLastAggInfo *) lfirst(lc);
		MinMaxAggInfo *mminfo = fl_info->m_agg_info;
		Oid eqop;
		bool reverse;

		/*
		 * We'll need the equality operator that goes with the aggregate's
		 * ordering operator.
		 */
		eqop = get_equality_op_for_ordering_op(mminfo->aggsortop, &reverse);
		if (!OidIsValid(eqop)) /* shouldn't happen */
			elog(ERROR,
				 "could not find equality operator for ordering operator %u",
				 mminfo->aggsortop);

		/*
		 * We can use either an ordering that gives NULLS FIRST or one that
		 * gives NULLS LAST; furthermore there's unlikely to be much
		 * performance difference between them, so it doesn't seem worth
		 * costing out both ways if we get a hit on the first one.  NULLS
		 * FIRST is more likely to be available if the operator is a
		 * reverse-sort operator, so try that first if reverse.
		 */
		if (build_first_last_path(root, fl_info, eqop, mminfo->aggsortop, reverse))
			continue;
		if (build_first_last_path(root, fl_info, eqop, mminfo->aggsortop, !reverse))
			continue;

		/* No indexable path for this aggregate, so fail */
		return;
	}

	/*
	 * OK, we can do the query this way. We are using MinMaxAggPath to store
	 * First/Last Agg path since the logic is almost the same. MinMaxAggPath
	 * is used later on by planner so by reusing it we don't need to re-invent
	 * planner.
	 *
	 * Prepare to create a MinMaxAggPath node.
	 *
	 * First, create an output Param node for each agg.  (If we end up not
	 * using the MinMaxAggPath, we'll waste a PARAM_EXEC slot for each agg,
	 * which is not worth worrying about.  We can't wait till create_plan time
	 * to decide whether to make the Param, unfortunately.)
	 */
	mm_agg_list = NIL;
	foreach (lc, first_last_aggs)
	{
		FirstLastAggInfo *fl_info = (FirstLastAggInfo *) lfirst(lc);
		MinMaxAggInfo *mminfo = fl_info->m_agg_info;

		mminfo->param = SS_make_initplan_output_param(root,
													  exprType((Node *) mminfo->target),
													  -1,
													  exprCollation((Node *) mminfo->target));
		mm_agg_list = lcons(mminfo, mm_agg_list);
	}

	/*
	 * Create a MinMaxAggPath node with the appropriate estimated costs and
	 * other needed data, and add it to the UPPERREL_GROUP_AGG upperrel, where
	 * it will compete against the standard aggregate implementation.  (It
	 * will likely always win, but we need not assume that here.)
	 *
	 * Note: grouping_planner won't have created this upperrel yet, but it's
	 * fine for us to create it first.  We will not have inserted the correct
	 * consider_parallel value in it, but MinMaxAggPath paths are currently
	 * never parallel-safe anyway, so that doesn't matter.  Likewise, it
	 * doesn't matter that we haven't filled FDW-related fields in the rel.
	 */
	grouped_rel = fetch_upper_rel(root, UPPERREL_GROUP_AGG, NULL);
	minmaxagg_path = create_minmaxagg_path(root,
										   grouped_rel,
										   create_pathtarget(root, tlist),
										   mm_agg_list,
										   (List *) parse->havingQual);
	/* Let's replace Aggref node since we will use subquery we've generated  */
	replace_aggref_in_tlist(minmaxagg_path);
	add_path(grouped_rel, (Path *) minmaxagg_path);
}


ModifyTablePath *
create_modifytable_path(PlannerInfo *root, RelOptInfo *rel,
						CmdType operation, bool canSetTag,
						Index nominalRelation,
						List *resultRelations, List *subpaths,
						List *subroots,
						List *withCheckOptionLists, List *returningLists,
						List *rowMarks, int epqParam)
{
	ModifyTablePath *pathnode = makeNode(ModifyTablePath);
	double		total_size;
	ListCell   *lc;

	Assert(list_length(resultRelations) == list_length(subpaths));
	Assert(list_length(resultRelations) == list_length(subroots));
	Assert(withCheckOptionLists == NIL ||
		   list_length(resultRelations) == list_length(withCheckOptionLists));
	Assert(returningLists == NIL ||
		   list_length(resultRelations) == list_length(returningLists));

	pathnode->path.pathtype = T_ModifyTable;
	pathnode->path.parent = rel;
	/* pathtarget is not interesting, just make it minimally valid */
	/* For now, assume we are above any joins, so no parameterization */
	pathnode->path.param_info = NULL;
	pathnode->path.pathkeys = NIL;

	/*
	 * Compute cost & rowcount as sum of subpath costs & rowcounts.
	 *
	 * Currently, we don't charge anything extra for the actual table
	 * modification work, nor for the WITH CHECK OPTIONS or RETURNING
	 * expressions if any.  It would only be window dressing, since
	 * ModifyTable is always a top-level node and there is no way for the
	 * costs to change any higher-level planning choices.  But we might want
	 * to make it look better sometime.
	 */
	pathnode->path.startup_cost = 0;
	pathnode->path.total_cost = 0;
	pathnode->path.rows = 0;
	total_size = 0;
	foreach(lc, subpaths)
	{
		Path	   *subpath = (Path *) lfirst(lc);

		if (lc == list_head(subpaths))	/* first node? */
			pathnode->path.startup_cost = subpath->startup_cost;
		pathnode->path.total_cost += subpath->total_cost;
		pathnode->path.rows += subpath->rows;
	}

	/*
	 * Set width to the average width of the subpath outputs.  XXX this is
	 * totally wrong: we should report zero if no RETURNING, else an average
	 * of the RETURNING tlist widths.  But it's what happened historically,
	 * and improving it is a task for another day.
	 */
	if (pathnode->path.rows > 0)
		total_size /= pathnode->path.rows;

	pathnode->operation = operation;
	pathnode->canSetTag = canSetTag;
	pathnode->nominalRelation = nominalRelation;
	pathnode->resultRelations = resultRelations;
	pathnode->subpaths = subpaths;
	pathnode->subroots = subroots;
	pathnode->withCheckOptionLists = withCheckOptionLists;
	pathnode->returningLists = returningLists;
	pathnode->rowMarks = rowMarks;
	pathnode->epqParam = epqParam;
	return pathnode;
}

static uint32 tsdb_index;

void set_extension_index(uint32 index)
{
    tsdb_index = index;  
}

tsdb_session_context* get_session_context()
{
    if (NULL != u_sess && NULL == u_sess->attr.attr_common.extension_session_vars_array) {
        tsdb_session_context* psc =
            (tsdb_session_context*)MemoryContextAllocZero(u_sess->self_mem_cxt, sizeof(tsdb_session_context));
        psc->tsdb_pinned_caches = NIL;
		psc->tsdb_pinned_caches_mctx = NULL;
		psc->tsdb_hypertable_cache_current = NULL;     
		psc->tsdb_planner_hcaches = NIL;
		psc->tsdb_TS_CTE_EXPAND= "ts_expand";
	
		psc->tsdb_chunk_append_plan_methods = {
			.ExtensibleName = "ChunkAppend",
			.CreateExtensiblePlanState = ts_chunk_append_state_create,
		};

		psc->tsdb_telemetry_level_options[0] = { "off", TELEMETRY_OFF, false };
		psc->tsdb_telemetry_level_options[1] = { "basic", TELEMETRY_BASIC, false };
		psc->tsdb_telemetry_level_options[2] = { NULL, 0, false };
		psc->tsdb_on_level = TELEMETRY_BASIC;

		psc->tsdb_expect_chunk_modification = false;
		psc->tsdb_ts_guc_license_key = "CommunityLicense";

		psc->tsdb_loaded = false;
		psc->tsdb_loader_present = true;
		psc->tsdb_NamedLWLockTrancheRequestsAllocated = 0;
		psc->tsdb_lock_named_request_allowed = true;
        psc->tsdb_global_extstate = EXTENSION_STATE_UNKNOWN;
		return psc;
    }
    if (u_sess->attr.attr_common.extension_session_vars_array[tsdb_index] == NULL) {
        init_session_vars();
    }
    return (tsdb_session_context*)u_sess->attr.attr_common.extension_session_vars_array[tsdb_index];
}

void init_session_vars(void) 
{
	if (u_sess->attr.attr_common.extension_session_vars_array[tsdb_index]!=NULL)
		return;
	
	RepallocSessionVarsArrayIfNecessary();
	tsdb_session_context* psc = (tsdb_session_context*)MemoryContextAllocZero(u_sess->self_mem_cxt, sizeof(tsdb_session_context));
	u_sess->attr.attr_common.extension_session_vars_array[tsdb_index] = psc;  
	psc->tsdb_pinned_caches = NIL;
	psc->tsdb_pinned_caches_mctx = NULL;
	psc->tsdb_hypertable_cache_current = NULL;     
	psc->tsdb_planner_hcaches = NIL;
	psc->tsdb_TS_CTE_EXPAND= "ts_expand";
	
	psc->tsdb_chunk_append_plan_methods = {
		.ExtensibleName = "ChunkAppend",
		.CreateExtensiblePlanState = ts_chunk_append_state_create,
	};

	psc->tsdb_telemetry_level_options[0] = { "off", TELEMETRY_OFF, false };
	psc->tsdb_telemetry_level_options[1] = { "basic", TELEMETRY_BASIC, false };
	psc->tsdb_telemetry_level_options[2] = { NULL, 0, false };
	psc->tsdb_on_level = TELEMETRY_BASIC;

	psc->tsdb_expect_chunk_modification = false;
	psc->tsdb_ts_guc_license_key = "CommunityLicense";

	psc->tsdb_loaded = false;
	psc->tsdb_loader_present = true;

	strcpy(psc->tsdb_base64, "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/");

	psc->tsdb_fixed_memory_cache_size=-1;
	psc->tsdb_extension_proxy_oid = InvalidOid;
	psc->tsdb_func_hash = NULL;
	
	psc->tsdb_downgrade_to_apache_enabled = false;
    psc->tsdb_tsl_handle = NULL;
	psc->tsdb_tsl_validate_license_fn = NULL;
	psc->tsdb_tsl_startup_fn = NULL;
	psc->tsdb_can_load = false;
	psc->tsdb_load_source = PGC_S_DEFAULT;

	psc->tsdb_dsm_init_done = false;

	psc->tsdb_dsm_control_mapped_size = 0;
	psc->tsdb_dsm_control_impl_private = NULL;

	psc->tsdb_NamedLWLockTrancheRequestsAllocated = 0;
	psc->tsdb_lock_named_request_allowed = true;

	psc->tsdb_ts_guc_disable_optimizations = false;
	psc->tsdb_ts_guc_optimize_non_hypertables = false;
	psc->tsdb_ts_guc_restoring = false;
	psc->tsdb_ts_guc_constraint_aware_append = true;
	psc->tsdb_ts_guc_enable_ordered_append = true;
	psc->tsdb_ts_guc_enable_chunk_append = true;
	psc->tsdb_ts_guc_enable_parallel_chunk_append = true;
	psc->tsdb_ts_guc_enable_runtime_exclusion = true;
	psc->tsdb_ts_guc_enable_constraint_exclusion = true;
	psc->tsdb_ts_guc_enable_cagg_reorder_groupby = true;
	psc->tsdb_ts_guc_enable_transparent_decompression = true;
	psc->tsdb_ts_guc_max_open_chunks_per_insert = 10;
	psc->tsdb_ts_guc_max_cached_chunks_per_hypertable = 10;
	psc->tsdb_ts_guc_telemetry_level = TELEMETRY_DEFAULT;

	psc->tsdb_ts_last_tune_time = NULL;
	psc->tsdb_ts_last_tune_version = NULL;
	psc->tsdb_ts_telemetry_cloud = NULL;
    psc->tsdb_global_extstate = EXTENSION_STATE_UNKNOWN;
	#ifdef TS_DEBUG
	ts_shutdown_bgw = false;
	ts_current_timestamp_mock = "";
	#endif
	
	_constraint_aware_append_init();

	if (!ts_extension_is_loaded())
		return;

	if (!tsdb_first_start)
	{
		_cache_init();
		_hypertable_cache_init();
		_cache_invalidate_init();

		_planner_init();
		_chunk_append_init();
		_process_utility_init();
		_event_trigger_init();
	}
}