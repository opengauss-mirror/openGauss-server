#ifndef TSDB_H
#define TSDB_H
#include "catalog/namespace.h"
#include "tsdb_head.h"
#include "utils/acl.h"
#include "catalog/dependency.h"
#include "access/rewriteheap.h"
#include "parser/parse_node.h"
#include "optimizer/planner.h"
#include "catalog/pg_inherits_fn.h"
#include "utils/hsearch.h"
#include "storage/ipc.h"
#include "storage/sinvaladt.h"
#include "parser/parse_coerce.h"
#include "access/hash.h"
#include "parser/parse_relation.h"
#include "commands/tablespace.h"
#include "catalog/index.h"
#include "bootstrap/bootstrap.h"
#include "parser/parser.h"
#include "nodes/nodeFuncs.h"
#include "catalog/index.h"
#include "access/heapam.h"
#include "access/genam.h"
#include "commands/vacuum.h"
#include "access/tableam.h"
#include "optimizer/subselect.h"
#include "utils/evtcache.h"
#include "cache.h"
#include "nodes/nodes.h"
#include <libpq/pqsignal.h>


extern void NewRelationCreateToastTable(Oid relOid, Datum reloptions); 
extern void RegisterCustomScanMethods(const ExtensiblePlanMethods *methods);
extern Param *SS_make_initplan_output_param(PlannerInfo *root,
							  Oid resulttype, int32 resulttypmod,
							  Oid resultcollation);
extern TableScanDescData *heap_beginscan_catalog(Relation relation, int nkeys,
					   ScanKey key);
extern int namecpy(Name n1, Name n2);    

extern List *RelationGetFKeyList(Relation relation);    

extern AttrNumber *convert_tuples_by_name_map(TupleDesc indesc,
						   TupleDesc outdesc,
						   const char *msg); 
extern int	check_enable_rls(Oid relid, Oid checkAsUser, bool noError); 

extern void PreventCommandIfParallelMode(const char *cmdname); 

extern List *ExecInsertIndexTuples(TupleTableSlot *slot, ItemPointer tupleid,
					  EState *estate, bool noDupErr, bool *specConflict,
					  List *arbiterIndexes);


extern void CreateCacheMemoryContext(void); 


void
add_partial_path(RelOptInfo *parent_rel, Path *new_path);

AggPath*
create_agg_path(PlannerInfo *root,
				RelOptInfo *rel,
				Path *subpath,
				PathTarget *target,
				AggStrategy aggstrategy,
				AggSplit aggsplit,
				List *groupClause,
				List *qual,
				const AggClauseCosts *aggcosts,
				double numGroups);
GatherPath* create_gather_path(PlannerInfo *root, RelOptInfo *rel, Path *subpath,
				   PathTarget *target, Relids required_outer, double *rows) ;
extern void get_agg_clause_costs(PlannerInfo *root, Node *clause,
					 AggSplit aggsplit, AggClauseCosts *costs); 
void add_path(RelOptInfo *parent_rel, Path *new_path);
extern ModifyTablePath *create_modifytable_path(PlannerInfo *root,
						RelOptInfo *rel,
						CmdType operation, bool canSetTag,
						Index nominalRelation,
						List *resultRelations, List *subpaths,
						List *subroots,
						List *withCheckOptionLists, List *returningLists,
						List *rowMarks, int epqParam);

RelOptInfo *fetch_upper_rel(PlannerInfo *root, UpperRelationKind kind, Relids relids);
MinMaxAggPath *create_minmaxagg_path(PlannerInfo *root,
					  RelOptInfo *rel,
					  PathTarget *target,
					  List *mmaggregates,
					  List *quals);
void SS_identify_outer_params(PlannerInfo *root);
void SS_charge_for_initplans(PlannerInfo *root, RelOptInfo *final_rel); 
Oid get_role_oid_or_public(const char *rolname); 
Oid get_rolespec_oid(const Node *node, bool missing_ok);

extern void ExecVacuum(VacuumStmt *vacstmt, bool isTopLevel); 
extern void BackgroundWorkerBlockSignals(void); 
extern void BackgroundWorkerUnblockSignals(void);
extern void BackgroundWorkerInitializeConnectionByOid(Oid dboid, Oid useroid); 
extern void BackgroundWorkerInitializeConnection(char *dbname, char *username);  
extern bool RegisterDynamicBackgroundWorker(BackgroundWorker *worker,
								BackgroundWorkerHandle **handle); 

extern void TerminateBackgroundWorker(BackgroundWorkerHandle *handle); 
extern BgwHandleStatus WaitForBackgroundWorkerStartup(BackgroundWorkerHandle *
							   handle, pid_t *pid); 
extern BgwHandleStatus GetBackgroundWorkerPid(BackgroundWorkerHandle *handle,
					   pid_t *pidp); 
extern BgwHandleStatus WaitForBackgroundWorkerShutdown(BackgroundWorkerHandle *);
extern void slot_getallattrs(TupleTableSlot *slot);

extern void mark_partial_aggref(Aggref *agg, AggSplit aggsplit); 
extern List *roleSpecsToIds(List *memberNames); 

extern LWLockPadded *GetNamedLWLockTranche(const char *tranche_name);
extern void RequestNamedLWLockTranche(const char *tranche_name, int num_lwlocks);   

extern Oid	toast_get_valid_index(Oid toastoid, LOCKMODE lock);
extern ColumnDef *makeColumnDef(const char *colname,
			  Oid typeOid, int32 typmod, Oid collOid); 
extern ArrayBuildStateArr *initArrayResultArr(Oid array_type, Oid element_type,
				   MemoryContext rcontext, bool subcontext);
extern ArrayBuildState *initArrayResult(Oid element_type,
				MemoryContext rcontext, bool subcontext); 
extern ArrayBuildStateArr *accumArrayResultArr(ArrayBuildStateArr *astate,
					Datum dvalue, bool disnull,
					Oid array_type,
					MemoryContext rcontext);  
extern Datum makeArrayResultArr(ArrayBuildStateArr *astate,
				   MemoryContext rcontext, bool release);
extern void check_index_predicates(PlannerInfo *root, RelOptInfo *rel); 
extern SortPath *create_sort_path(PlannerInfo *root,
				 RelOptInfo *rel,
				 Path *subpath,
				 List *pathkeys,
				 double limit_tuples);

extern bool IsInParallelMode(void);

extern void InitPostgres(const char *in_dbname, Oid dboid, const char *username,
 			 Oid useroid, char *out_dbname);

extern void cost_gather(GatherPath *path, PlannerInfo *root,
 			RelOptInfo *baserel, ParamPathInfo *param_info, double *rows); 

extern bool has_parallel_hazard(Node *node, bool allow_restricted); 

extern bool has_bypassrls_privilege(Oid roleid); 
extern bool InNoForceRLSOperation(void); 

extern void SpeculativeInsertionWait(TransactionId xid, uint32 token); 

extern void slot_getsomeattrs(TupleTableSlot *slot, int attnum); 

extern void ResourceOwnerEnlargeDSMs(ResourceOwner owner);
extern void ResourceOwnerRememberDSM(ResourceOwner owner,
						 dsm_segment *); 
extern void ResourceOwnerForgetDSM(ResourceOwner owner,
					   dsm_segment *); 



extern bool check_functions_in_node(Node *node, check_function_callback checker,
						void *context);
extern Relids find_childrel_parents(PlannerInfo *root, RelOptInfo *rel); 

extern "C" void set_extension_index(uint32 index);
extern "C" void init_session_vars(void);

extern tsdb_session_context* get_session_context();

#endif