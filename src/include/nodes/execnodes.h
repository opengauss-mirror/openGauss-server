/* -------------------------------------------------------------------------
 *
 * execnodes.h
 *	  definitions for executor state nodes
 *
 *
 * Portions Copyright (c) 2021, openGauss Contributors
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/nodes/execnodes.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef EXECNODES_H
#define EXECNODES_H

#include "access/genam.h"
#include "access/relscan.h"
#include "bulkload/dist_fdw.h"
#include "executor/instrument.h"
#include "nodes/params.h"
#include "nodes/plannodes.h"
#include "storage/pagecompress.h"
#include "utils/array.h"
#include "utils/bloom_filter.h"
#include "utils/reltrigger.h"
#include "utils/sortsupport.h"
#include "utils/tuplesort.h"
#include "utils/tuplestore.h"
#include "vecexecutor/vectorbatch.h"

#include "db4ai/matrix.h"

#ifdef ENABLE_MOT
// forward declaration for MOT JitContext
namespace JitExec
{
    struct JitContext;
}
#endif

/* struct for utility statement mem usage */
typedef struct UtilityDesc {
    double cost;      /* cost of utility statement */
    int query_mem[2]; /* max and min mem of utility statement */
    int min_mem;      /* operator min mem from cost estimation */
    int assigned_mem; /* statement mem assigned by workload manager */
} UtilityDesc;

/* ----------------
 *	  IndexInfo information
 *
 *		this struct holds the information needed to construct new index
 *		entries for a particular index.  Used for both index_build and
 *		retail creation of index entries.
 *
 *		NumIndexAttrs		total number of columns in this index
 *		NumIndexKeyAttrs	number of key columns in index
 *		KeyAttrNumbers		underlying-rel attribute numbers used as keys
 *							(zeroes indicate expressions). It also contains
 * 							info about included columns.
 *		Expressions			expr trees for expression entries, or NIL if none
 *		ExpressionsState	exec state for expressions, or NIL if none
 *		Predicate			partial-index predicate, or NIL if none
 *		PredicateState		exec state for predicate, or NIL if none
 *		ExclusionOps		Per-column exclusion operators, or NULL if none
 *		ExclusionProcs		Underlying function OIDs for ExclusionOps
 *		ExclusionStrats		Opclass strategy numbers for ExclusionOps
 *		UniqueOps           Theses are like Exclusion*, but for unique indexes
 *		UniqueProcs
 *		UniqueStrats
 *		Unique				is it a unique index?
 *		ReadyForInserts		is it valid for inserts?
 *		Concurrent			are we doing a concurrent index build?
 *		BrokenHotChain		did we detect any broken HOT chains?
 *
 * ii_Concurrent and ii_BrokenHotChain are used only during index build;
 * they're conventionally set to false otherwise.
 * ----------------
 */
typedef struct IndexInfo {
    NodeTag type;
    int ii_NumIndexAttrs;       /* total number of columns in index */
    int ii_NumIndexKeyAttrs;    /* number of key columns in index */
    AttrNumber ii_KeyAttrNumbers[INDEX_MAX_KEYS];
    List* ii_Expressions;       /* list of Expr */
    List* ii_ExpressionsState;  /* list of ExprState */
    List* ii_Predicate;         /* list of Expr */
    List* ii_PredicateState;    /* list of ExprState */
    Oid* ii_ExclusionOps;       /* array with one entry per column */
    Oid* ii_ExclusionProcs;     /* array with one entry per column */
    uint16* ii_ExclusionStrats; /* array with one entry per column */
    Oid *ii_UniqueOps;          /* array with one entry per column */
    Oid *ii_UniqueProcs;        /* array with one entry per column */
    uint16 *ii_UniqueStrats;    /* array with one entry per column */
    bool ii_Unique;
    bool ii_ReadyForInserts;
    bool ii_Concurrent;
    bool ii_BrokenHotChain;
    int  ii_ParallelWorkers;
    short ii_PgClassAttrId;
    UtilityDesc ii_desc; /* meminfo for index create */
} IndexInfo;

/* ----------------
 *	  ExprContext_CB
 *
 *		List of callbacks to be called at ExprContext shutdown.
 * ----------------
 */
typedef void (*ExprContextCallbackFunction)(Datum arg);

typedef struct ExprContext_CB {
    struct ExprContext_CB* next;
    ExprContextCallbackFunction function;
    ResourceOwner resowner;
    Datum arg;
} ExprContext_CB;

/* ----------------
 *	  ExprContext
 *
 *		This class holds the "current context" information
 *		needed to evaluate expressions for doing tuple qualifications
 *		and tuple projections.	For example, if an expression refers
 *		to an attribute in the current inner tuple then we need to know
 *		what the current inner tuple is and so we look at the expression
 *		context.
 *
 *	There are two memory contexts associated with an ExprContext:
 *	* ecxt_per_query_memory is a query-lifespan context, typically the same
 *	  context the ExprContext node itself is allocated in.	This context
 *	  can be used for purposes such as storing function call cache info.
 *	* ecxt_per_tuple_memory is a short-term context for expression results.
 *	  As the name suggests, it will typically be reset once per tuple,
 *	  before we begin to evaluate expressions for that tuple.  Each
 *	  ExprContext normally has its very own per-tuple memory context.
 *
 *	CurrentMemoryContext should be set to ecxt_per_tuple_memory before
 *	calling ExecEvalExpr() --- see ExecEvalExprSwitchContext().
 * ----------------
 */
struct PLpgSQL_execstate;

typedef struct ExprContext {
    NodeTag type;

    /* Tuples that Var nodes in expression may refer to */
    TupleTableSlot* ecxt_scantuple;
    TupleTableSlot* ecxt_innertuple;
    TupleTableSlot* ecxt_outertuple;

    /* Memory contexts for expression evaluation --- see notes above */
    MemoryContext ecxt_per_query_memory;
    MemoryContext ecxt_per_tuple_memory;

    /* Values to substitute for Param nodes in expression */
    ParamExecData* ecxt_param_exec_vals; /* for PARAM_EXEC params */
    ParamListInfo ecxt_param_list_info;  /* for other param types */

    /*
     * Values to substitute for Aggref nodes in the expressions of an Agg
     * node, or for WindowFunc nodes within a WindowAgg node.
     */
    Datum* ecxt_aggvalues; /* precomputed values for aggs/windowfuncs */
    bool* ecxt_aggnulls;   /* null flags for aggs/windowfuncs */

    /* Value to substitute for CaseTestExpr nodes in expression */
    Datum caseValue_datum;
    bool caseValue_isNull;

    ScalarVector* caseValue_vector;

    /* Value to substitute for CoerceToDomainValue nodes in expression */
    Datum domainValue_datum;
    bool domainValue_isNull;

    /* Link to containing EState (NULL if a standalone ExprContext) */
    struct EState* ecxt_estate;

    /* Functions to call back when ExprContext is shut down */
    ExprContext_CB* ecxt_callbacks;

    // vector specific fields
    // consider share space with row fields
    //
    VectorBatch* ecxt_scanbatch;
    VectorBatch* ecxt_innerbatch;
    VectorBatch* ecxt_outerbatch;

    // Batch to substitute for Aggref nodes in the expression of an VecAgg node
    //
    VectorBatch* ecxt_aggbatch;

    /*
     * mark the real rows for expression cluster, all the results' m_rows generated by
     * vec-expression are aligned by econtext->align_rows
     */
    int align_rows;

    bool m_fUseSelection;  // Shall we use selection vector?
    ScalarVector* qual_results;
    ScalarVector* boolVector;
    bool is_cursor;
    Cursor_Data cursor_data;
    int dno;
    PLpgSQL_execstate* plpgsql_estate;
    /*
     * For vector set-result function.
     */
    bool have_vec_set_fun;
    bool* vec_fun_sel;  // selection for vector set-result function.
    int current_row;
} ExprContext;

/*
 * Set-result status returned by ExecEvalExpr()
 */
typedef enum {
    ExprSingleResult,   /* expression does not return a set */
    ExprMultipleResult, /* this result is an element of a set */
    ExprEndResult       /* there are no more elements in the set */
} ExprDoneCond;

/*
 * Return modes for functions returning sets.  Note values must be chosen
 * as separate bits so that a bitmask can be formed to indicate supported
 * modes.  SFRM_Materialize_Random and SFRM_Materialize_Preferred are
 * auxiliary flags about SFRM_Materialize mode, rather than separate modes.
 */
typedef enum {
    SFRM_ValuePerCall = 0x01,         /* one value returned per call */
    SFRM_Materialize = 0x02,          /* result set instantiated in Tuplestore */
    SFRM_Materialize_Random = 0x04,   /* Tuplestore needs randomAccess */
    SFRM_Materialize_Preferred = 0x08 /* caller prefers Tuplestore */
} SetFunctionReturnMode;

/*
 * When calling a function that might return a set (multiple rows),
 * a node of this type is passed as fcinfo->resultinfo to allow
 * return status to be passed back.  A function returning set should
 * raise an error if no such resultinfo is provided.
 */
typedef struct ReturnSetInfo {
    NodeTag type;
    /* values set by caller: */
    ExprContext* econtext;  /* context function is being called in */
    TupleDesc expectedDesc; /* tuple descriptor expected by caller */
    int allowedModes;       /* bitmask: return modes caller can handle */
    /* result status from function (but pre-initialized by caller): */
    SetFunctionReturnMode returnMode; /* actual return mode */
    ExprDoneCond isDone;              /* status for ValuePerCall mode */
    /* fields filled by function in Materialize return mode: */
    Tuplestorestate* setResult; /* holds the complete returned tuple set */
    TupleDesc setDesc;          /* actual descriptor for returned tuples */
} ReturnSetInfo;

/* ----------------
 *		ProjectionInfo node information
 *
 *		This is all the information needed to perform projections ---
 *		that is, form new tuples by evaluation of targetlist expressions.
 *		Nodes which need to do projections create one of these.
 *
 *		ExecProject() evaluates the tlist, forms a tuple, and stores it
 *		in the given slot.	Note that the result will be a "virtual" tuple
 *		unless ExecMaterializeSlot() is then called to force it to be
 *		converted to a physical tuple.	The slot must have a tupledesc
 *		that matches the output of the tlist!
 *
 *		The planner very often produces tlists that consist entirely of
 *		simple Var references (lower levels of a plan tree almost always
 *		look like that).  And top-level tlists are often mostly Vars too.
 *		We therefore optimize execution of simple-Var tlist entries.
 *		The pi_targetlist list actually contains only the tlist entries that
 *		aren't simple Vars, while those that are Vars are processed using the
 *		varSlotOffsets/varNumbers/varOutputCols arrays.
 *
 *		The lastXXXVar fields are used to optimize fetching of fields from
 *		input tuples: they let us do a slot_getsomeattrs() call to ensure
 *		that all needed attributes are extracted in one pass.
 *
 *		targetlist		target list for projection (non-Var expressions only)
 *		exprContext		expression context in which to evaluate targetlist
 *		slot			slot to place projection result in
 *		itemIsDone		workspace array for ExecProject
 *		directMap		true if varOutputCols[] is an identity map
 *		numSimpleVars	number of simple Vars found in original tlist
 *		varSlotOffsets	array indicating which slot each simple Var is from
 *		varNumbers		array containing input attr numbers of simple Vars
 *		varOutputCols	array containing output attr numbers of simple Vars
 *		lastInnerVar	highest attnum from inner tuple slot (0 if none)
 *		lastOuterVar	highest attnum from outer tuple slot (0 if none)
 *		lastScanVar		highest attnum from scan tuple slot (0 if none)
 *		pi_maxOrmin	column table optimize, indicate if get this column's max or min.
 * ----------------
 */
typedef bool (*vectarget_func)(ExprContext* econtext, VectorBatch* pBatch);
typedef struct ProjectionInfo {
    NodeTag type;
    List* pi_targetlist;
    ExprContext* pi_exprContext;
    TupleTableSlot* pi_slot;
    ExprDoneCond* pi_itemIsDone;
    bool pi_directMap;
    bool pi_topPlan;             /* Whether the outermost layer query */
    int pi_numSimpleVars;
    int* pi_varSlotOffsets;
    int* pi_varNumbers;
    int* pi_varOutputCols;
    int pi_lastInnerVar;
    int pi_lastOuterVar;
    int pi_lastScanVar;
    List* pi_acessedVarNumbers;
    List* pi_sysAttrList;
    List* pi_lateAceessVarNumbers;
    List* pi_maxOrmin;
    List* pi_PackTCopyVars;            /* VarList to record those columns what we need to move */
    List* pi_PackLateAccessVarNumbers; /*VarList to record those columns what we need to move in late read cstore
                                          scan.*/
    bool pi_const;
    VectorBatch* pi_batch;
    vectarget_func jitted_vectarget; /* LLVM function pointer to point to the codegened targetlist expr function */
    VectorBatch* pi_setFuncBatch;
} ProjectionInfo;

/*
 * Function pointer which will be used by LLVM assemble. The created IR functions
 * will be added to the actual machine code.
 */
typedef ScalarVector* (*vecqual_func)(ExprContext* econtext);

/* ----------------
 *	  JunkFilter
 *
 *	  This class is used to store information regarding junk attributes.
 *	  A junk attribute is an attribute in a tuple that is needed only for
 *	  storing intermediate information in the executor, and does not belong
 *	  in emitted tuples.  For example, when we do an UPDATE query,
 *	  the planner adds a "junk" entry to the targetlist so that the tuples
 *	  returned to ExecutePlan() contain an extra attribute: the ctid of
 *	  the tuple to be updated.	This is needed to do the update, but we
 *	  don't want the ctid to be part of the stored new tuple!  So, we
 *	  apply a "junk filter" to remove the junk attributes and form the
 *	  real output tuple.  The junkfilter code also provides routines to
 *	  extract the values of the junk attribute(s) from the input tuple.
 *
 *	  targetList:		the original target list (including junk attributes).
 *	  cleanTupType:		the tuple descriptor for the "clean" tuple (with
 *						junk attributes removed).
 *	  cleanMap:			A map with the correspondence between the non-junk
 *						attribute numbers of the "original" tuple and the
 *						attribute numbers of the "clean" tuple.
 *	  resultSlot:		tuple slot used to hold cleaned tuple.
 *	  junkAttNo:		not used by junkfilter code.  Can be used by caller
 *						to remember the attno of a specific junk attribute
 *						(nodeModifyTable.c keeps the "ctid" or "wholerow"
 *						attno here).
 * ----------------
 */
typedef struct JunkFilter {
    NodeTag type;
    List* jf_targetList;
    TupleDesc jf_cleanTupType;
    AttrNumber* jf_cleanMap;
    TupleTableSlot* jf_resultSlot;
    AttrNumber jf_junkAttNo;
#ifdef PGXC
    /*
     * Similar to jf_junkAttNo that is used for ctid, we also need xc_node_id
     * and wholerow junk attribute numbers to be saved here. In XC, we need
     * multiple junk attributes at the same time, so just jf_junkAttNo is not
     * enough. In PG, jf_junkAttNo is used either for ctid or for wholerow,
     * it does not need both of them at the same time; ctid is used for physical
     * relations while wholerow is used for views.
     */
    AttrNumber jf_xc_node_id;
    AttrNumber jf_xc_wholerow;
    AttrNumber jf_xc_part_id;
    AttrNumber jf_xc_bucket_id;	
    List* jf_primary_keys;
#endif
} JunkFilter;

typedef struct MergeState {
    /* List of MERGE MATCHED action states */
    List* matchedActionStates;
    /* List of MERGE NOT MATCHED action states */
    List* notMatchedActionStates;
} MergeState;

/* ----------------------------------------------------------------
 *				 Expression State Trees
 *
 * Each executable expression tree has a parallel ExprState tree.
 *
 * Unlike PlanState, there is not an exact one-for-one correspondence between
 * ExprState node types and Expr node types.  Many Expr node types have no
 * need for node-type-specific run-time state, and so they can use plain
 * ExprState or GenericExprState as their associated ExprState node type.
 * ----------------------------------------------------------------
 */

/* ----------------
 *		ExprState node
 *
 * ExprState is the common superclass for all ExprState-type nodes.
 *
 * It can also be instantiated directly for leaf Expr nodes that need no
 * local run-time state (such as Var, Const, or Param).
 *
 * To save on dispatch overhead, each ExprState node contains a function
 * pointer to the routine to execute to evaluate the node.
 * ----------------
 */
typedef struct ExprState ExprState;

typedef Datum (*ExprStateEvalFunc)(ExprState* expression, ExprContext* econtext, bool* isNull, ExprDoneCond* isDone);
typedef ScalarVector* (*VectorExprFun)(
    ExprState* expression, ExprContext* econtext, bool* selVector, ScalarVector* inputVector, ExprDoneCond* isDone);

typedef void* (*exprFakeCodeGenSig)(void*);
struct ExprState {
    NodeTag type;
    Expr* expr;                 /* associated Expr node */
    ExprStateEvalFunc evalfunc; /* routine to run to execute node */

    // vectorized evaluator
    //
    VectorExprFun vecExprFun;

    exprFakeCodeGenSig exprCodeGen; /* routine to run llvm assembler function */

    ScalarVector tmpVector;

    Oid resultType;
};

/* ----------------
 *	  ResultRelInfo information
 *
 *		Whenever we update an existing relation, we have to
 *		update indices on the relation, and perhaps also fire triggers.
 *		The ResultRelInfo class is used to hold all the information needed
 *		about a result relation, including indices.. -cim 10/15/89
 *
 *		RangeTableIndex			result relation's range table index
 *		RelationDesc			relation descriptor for result relation
 *		NumIndices				# of indices existing on result relation
 *		ri_ContainGPI			indices whether contain global parition index
 *		IndexRelationDescs		array of relation descriptors for indices
 *		IndexRelationInfo		array of key/attr info for indices
 *		TrigDesc				triggers to be fired, if any
 *		TrigFunctions			cached lookup info for trigger functions
 *		TrigWhenExprs			array of trigger WHEN expr states
 *		TrigInstrument			optional runtime measurements for triggers
 *		FdwRoutine				FDW callback functions, if foreign table
 *		FdwState				available to save private state of FDW
 *		ConstraintExprs			array of constraint-checking expr states
 *		junkFilter				for removing junk attributes from tuples
 *		projectReturning		for computing a RETURNING list
 *		updateProj				for computing a UPSERT update list
 * ----------------
 */
typedef struct ResultRelInfo {
    NodeTag type;
    Index ri_RangeTableIndex;
    Relation ri_RelationDesc;
    int ri_NumIndices;
    bool ri_ContainGPI;
    RelationPtr ri_IndexRelationDescs;
    IndexInfo** ri_IndexRelationInfo;
    TriggerDesc* ri_TrigDesc;
    FmgrInfo* ri_TrigFunctions;
    List** ri_TrigWhenExprs;
    Instrumentation* ri_TrigInstrument;
    struct FdwRoutine* ri_FdwRoutine;
    void* ri_FdwState;
    List** ri_ConstraintExprs;
    JunkFilter* ri_junkFilter;
    AttrNumber ri_partOidAttNum;
    AttrNumber ri_bucketIdAttNum;	
    ProjectionInfo* ri_projectReturning;

    /* for running MERGE on this result relation */
    MergeState* ri_mergeState;

    /*
     * While executing MERGE, the target relation is processed twice; once
     * as a target relation and once to run a join between the target and the
     * source. We generate two different RTEs for these two purposes, one with
     * rte->inh set to false and other with rte->inh set to true.
     *
     * Since the plan re-evaluated by EvalPlanQual uses the join RTE, we must
     * install the updated tuple in the scan corresponding to that RTE. The
     * following member tracks the index of the second RTE for EvalPlanQual
     * purposes. ri_mergeTargetRTI is non-zero only when MERGE is in-progress.
     * We use ri_mergeTargetRTI to run EvalPlanQual for MERGE and
     * ri_RangeTableIndex elsewhere.
     */
    Index ri_mergeTargetRTI;
    ProjectionInfo* ri_updateProj;
    
    /* array of stored generated columns expr states */
    ExprState **ri_GeneratedExprs;

    /* number of stored generated columns we need to compute */
    int ri_NumGeneratedNeeded;
} ResultRelInfo;

/* bloom filter controller */
typedef struct BloomFilterControl {
    filter::BloomFilter** bfarray; /* bloom filter array. */
    int array_size;                /* bloom filter array size. */
} BloomFilterControl;

#define InvalidBktId  (-1)    /* invalid hash-bucket id */

/* ----------------
 *	  EState information
 *
 * Master working state for an Executor invocation
 * ----------------
 */
typedef struct EState {
    NodeTag type;

    /* Basic state for all query types: */
    ScanDirection es_direction;      /* current scan direction */
    Snapshot es_snapshot;            /* time qual to use */
    Snapshot es_crosscheck_snapshot; /* crosscheck time qual for RI */
    List* es_range_table;            /* List of RangeTblEntry */
    PlannedStmt* es_plannedstmt;     /* link to top of plan tree */

    JunkFilter* es_junkFilter; /* top-level junk filter, if any */

    /* If query can insert/delete tuples, the command ID to mark them with */
    CommandId es_output_cid;

    /* Info about target table(s) for insert/update/delete queries: */
    ResultRelInfo* es_result_relations;     /* array of ResultRelInfos */
    int es_num_result_relations;            /* length of array */
    ResultRelInfo* es_result_relation_info; /* currently active array elt */

    Relation esCurrentPartition;
    HTAB* esfRelations; /* do the update,delete , cache the Relation which get from partitionGetRelation */
#ifdef PGXC
    struct PlanState* es_result_remoterel;        /* currently active remote rel */
    struct PlanState* es_result_insert_remoterel; /* currently active remote rel */
    struct PlanState* es_result_update_remoterel; /* currently active remote rel */
    struct PlanState* es_result_delete_remoterel; /* currently active remote rel */
#endif

    /* Stuff used for firing triggers: */
    List* es_trig_target_relations;      /* trigger-only ResultRelInfos */
    TupleTableSlot* es_trig_tuple_slot;  /* for trigger output tuples */
    TupleTableSlot* es_trig_oldtup_slot; /* for TriggerEnabled */
    TupleTableSlot* es_trig_newtup_slot; /* for TriggerEnabled */

    /* Parameter info: */
    ParamListInfo es_param_list_info;  /* values of external params */
    ParamExecData* es_param_exec_vals; /* values of internal params */

    /* Other working state: */
    MemoryContext es_query_cxt;       /* per-query context in which EState lives */
    MemoryContext es_const_query_cxt; /* const per-query context used to create node context */

    List* es_tupleTable; /* List of TupleTableSlots */

    List* es_rowMarks; /* List of ExecRowMarks */

    List *es_modifiedRowHash; /* List of prevHash of modified rows */

    uint64 es_processed; /* # of tuples processed */

    uint64 deleteLimitCount; /* delete Limit */

    uint64 es_last_processed; /* last value of es_processed for ModifyTable plan*/

    Oid es_lastoid; /* last oid processed (by INSERT) */

    int es_top_eflags; /* eflags passed to ExecutorStart */
    int es_instrument; /* OR of InstrumentOption flags */
    bool es_finished;  /* true when ExecutorFinish is done */

    List* es_exprcontexts; /* List of ExprContexts within EState */

    List* es_subplanstates; /* List of PlanState for SubPlans */

    List* es_auxmodifytables; /* List of secondary ModifyTableStates */

    List* es_remotequerystates; /* List of RemoteQueryStates */

    /*
     * this ExprContext is for per-output-tuple operations, such as constraint
     * checks and index-value computations.  It will be reset for each output
     * tuple.  Note that it will be created only if needed.
     */
    ExprContext* es_per_tuple_exprcontext;

    /*
     * These fields are for re-evaluating plan quals when an updated tuple is
     * substituted in READ COMMITTED mode.	es_epqTuple[] contains tuples that
     * scan plan nodes should return instead of whatever they'd normally
     * return, or NULL if nothing to return; es_epqTupleSet[] is true if a
     * particular array entry is valid; and es_epqScanDone[] is state to
     * remember if the tuple has been returned already.  Arrays are of size
     * list_length(es_range_table) and are indexed by scan node scanrelid - 1.
     */
    Tuple* es_epqTuple; /* array of EPQ substitute tuples */
    TupleTableSlot** es_epqTupleSlot;
    bool* es_epqTupleSet;   /* true if EPQ tuple is provided */
    bool* es_epqScanDone;   /* true if EPQ tuple has been fetched */

    List* es_subplan_ids;
    bool es_skip_early_free;            /* true if we don't apply early free mechanisim, especially for subplan */
    /* true if we don't apply early-free-consumer mechanisim, especially for subplan */
    bool es_skip_early_deinit_consumer; 
    bool es_under_subplan;              /* true if operator is under a subplan */
    List* es_material_of_subplan;       /* List of Materialize operator of subplan */
    bool es_recursive_next_iteration;   /* true if under recursive-stream and need to rescan. */

    /* data redistribution for DFS table.
     * dataDestRelIndex is index into the range table. This variable
     * will take effect on data redistribution state.
     */
    Index dataDestRelIndex;

    BloomFilterControl es_bloom_filter; /* bloom filter controller */

    bool es_can_realtime_statistics; /* true if can realime statistics */
    bool es_can_history_statistics;  /* true if can history statistics */

    bool isRowTriggerShippable; /* true if all row triggers are shippable. */
#ifdef ENABLE_MOT
    JitExec::JitContext* mot_jit_context;   /* MOT JIT context required for executing LLVM jitted code */
#endif

    PruningResult* pruningResult;
} EState;

/*
 * ExecRowMark -
 *	   runtime representation of FOR UPDATE/SHARE clauses
 *
 * When doing UPDATE, DELETE, or SELECT FOR UPDATE/SHARE, we should have an
 * ExecRowMark for each non-target relation in the query (except inheritance
 * parent RTEs, which can be ignored at runtime).  See PlanRowMark for details
 * about most of the fields.  In addition to fields directly derived from
 * PlanRowMark, we store curCtid, which is used by the WHERE CURRENT OF code.
 *
 * EState->es_rowMarks is a list of these structs.
 */
typedef struct ExecRowMark {
    Relation relation;       /* opened and suitably locked relation */
    Index rti;               /* its range table index */
    Index prti;              /* parent range table index, if child */
    Index rowmarkId;         /* unique identifier for resjunk columns */
    RowMarkType markType;    /* see enum in nodes/plannodes.h */
    bool noWait;             /* NOWAIT option */
    int waitSec;      /* WAIT time Sec */
    ItemPointerData curCtid; /* ctid of currently locked tuple, if any */
    int numAttrs;            /* number of attributes in subplan */
} ExecRowMark;

#define PREV_HASH_LEN 36
/*
 * ExecModifiedRowHash -
 *	   store previous hash of modified row.
 *
 */
typedef struct ExecModifiedRowHash {
    char hash[PREV_HASH_LEN]; /* value of previous hash */
} ExecModifiedRowHash;

/*
 * ExecAuxRowMark -
 *	   additional runtime representation of FOR UPDATE/SHARE clauses
 *
 * Each LockRows and ModifyTable node keeps a list of the rowmarks it needs to
 * deal with.  In addition to a pointer to the related entry in es_rowMarks,
 * this struct carries the column number(s) of the resjunk columns associated
 * with the rowmark (see comments for PlanRowMark for more detail).  In the
 * case of ModifyTable, there has to be a separate ExecAuxRowMark list for
 * each child plan, because the resjunk columns could be at different physical
 * column positions in different subplans.
 */
typedef struct ExecAuxRowMark {
    ExecRowMark* rowmark;  /* related entry in es_rowMarks */
    AttrNumber ctidAttNo;  /* resno of ctid junk attribute, if any */
    AttrNumber toidAttNo;  /* resno of tableoid junk attribute, if any */
    AttrNumber tbidAttNo;  /* resno of bucketid junk attribute, if any */
    AttrNumber wholeAttNo; /* resno of whole-row junk attribute, if any */
} ExecAuxRowMark;

/* ----------------------------------------------------------------
 *				 Tuple Hash Tables
 *
 * All-in-memory tuple hash tables are used for a number of purposes.
 *
 * Note: tab_hash_funcs are for the key datatype(s) stored in the table,
 * and tab_eq_funcs are non-cross-type equality operators for those types.
 * Normally these are the only functions used, but FindTupleHashEntry()
 * supports searching a hashtable using cross-data-type hashing.  For that,
 * the caller must supply hash functions for the LHS datatype as well as
 * the cross-type equality operators to use.  in_hash_funcs and cur_eq_funcs
 * are set to point to the caller's function arrays while doing such a search.
 * During LookupTupleHashEntry(), they point to tab_hash_funcs and
 * tab_eq_funcs respectively.
 * ----------------------------------------------------------------
 */
typedef struct TupleHashEntryData* TupleHashEntry;
typedef struct TupleHashTableData* TupleHashTable;

typedef struct TupleHashEntryData {
    /* firstTuple must be the first field in this struct! */
    MinimalTuple firstTuple; /* copy of first tuple in this group */
                             /* there may be additional data beyond the end of this struct */
} TupleHashEntryData;        /* VARIABLE LENGTH STRUCT */

typedef struct TupleHashTableData {
    HTAB* hashtab;             /* underlying dynahash table */
    int numCols;               /* number of columns in lookup key */
    AttrNumber* keyColIdx;     /* attr numbers of key columns */
    FmgrInfo* tab_hash_funcs;  /* hash functions for table datatype(s) */
    FmgrInfo* tab_eq_funcs;    /* equality functions for table datatype(s) */
    MemoryContext tablecxt;    /* memory context containing table */
    MemoryContext tempcxt;     /* context for function evaluations */
    Size entrysize;            /* actual size to make each hash entry */
    TupleTableSlot* tableslot; /* slot for referencing table entries */
    /* The following fields are set transiently for each table search: */
    TupleTableSlot* inputslot; /* current input tuple's slot */
    FmgrInfo* in_hash_funcs;   /* hash functions for input datatype(s) */
    FmgrInfo* cur_eq_funcs;    /* equality functions for input vs. table */
    int64 width;               /* records total width in memory */
    bool add_width;            /* if width should be added */
    bool causedBySysRes;       /* the batch increase caused by system resources limit? */
} TupleHashTableData;

typedef HASH_SEQ_STATUS TupleHashIterator;

/*
 * Use InitTupleHashIterator/TermTupleHashIterator for a read/write scan.
 * Use ResetTupleHashIterator if the table can be frozen (in this case no
 * explicit scan termination is needed).
 */
#define InitTupleHashIterator(htable, iter) hash_seq_init(iter, (htable)->hashtab)
#define TermTupleHashIterator(iter) hash_seq_term(iter)
#define ResetTupleHashIterator(htable, iter)    \
    do {                                        \
        hash_freeze((htable)->hashtab);         \
        hash_seq_init(iter, (htable)->hashtab); \
    } while (0)
#define ScanTupleHashTable(iter) ((TupleHashEntry)hash_seq_search(iter))

/* ----------------
 *		GenericExprState node
 *
 * This is used for Expr node types that need no local run-time state,
 * but have one child Expr node.
 * ----------------
 */
typedef struct GenericExprState {
    ExprState xprstate;
    ExprState* arg; /* state of my child node */
} GenericExprState;

/* ----------------
 *		WholeRowVarExprState node
 * ----------------
 */
typedef struct WholeRowVarExprState {
    ExprState xprstate;
    struct PlanState* parent;   /* parent PlanState, or NULL if none */
    JunkFilter* wrv_junkFilter; /* JunkFilter to remove resjunk cols */
} WholeRowVarExprState;

/* ----------------
 *		AggrefExprState node
 * ----------------
 */
typedef struct AggrefExprState {
    ExprState xprstate;
    List* aggdirectargs;  /* states of direct-argument expressions */
    List* args;           /* states of argument expressions */
    ExprState* aggfilter; /* state of FILTER expression, if any */
    int aggno;            /* ID number for agg within its plan node */

    // Vectorized aggregation fields
    //
    int m_htbOffset;  // offset in the hash table
} AggrefExprState;

/* ----------------
 *		WindowFuncExprState node
 * ----------------
 */
typedef struct WindowFuncExprState {
    ExprState xprstate;
    List* args;  /* states of argument expressions */
    int wfuncno; /* ID number for wfunc within its plan node */

    // Vectorized aggregation fields
    //
    ScalarVector* m_resultVector;
} WindowFuncExprState;

/* ----------------
 *		ArrayRefExprState node
 *
 * Note: array types can be fixed-length (typlen > 0), but only when the
 * element type is itself fixed-length.  Otherwise they are varlena structures
 * and have typlen = -1.  In any case, an array type is never pass-by-value.
 * ----------------
 */
typedef struct ArrayRefExprState {
    ExprState xprstate;
    List* refupperindexpr; /* states for child nodes */
    List* reflowerindexpr;
    ExprState* refexpr;
    ExprState* refassgnexpr;
    int16 refattrlength; /* typlen of array type */
    int16 refelemlength; /* typlen of the array element type */
    bool refelembyval;   /* is the element type pass-by-value? */
    char refelemalign;   /* typalign of the element type */
} ArrayRefExprState;

/* ----------------
 *		FuncExprState node
 *
 * Although named for FuncExpr, this is also used for OpExpr, DistinctExpr,
 * and NullIf nodes; be careful to check what xprstate.expr is actually
 * pointing at!
 * ----------------
 */
typedef struct FuncExprState {
    ExprState xprstate;
    List* args; /* states of argument expressions */
    char prokind;
    /*
     * Function manager's lookup info for the target function.  If func.fn_oid
     * is InvalidOid, we haven't initialized it yet (nor any of the following
     * fields).
     */
    FmgrInfo func;

    /*
     * For a set-returning function (SRF) that returns a tuplestore, we keep
     * the tuplestore here and dole out the result rows one at a time. The
     * slot holds the row currently being returned.
     */
    Tuplestorestate* funcResultStore;
    TupleTableSlot* funcResultSlot;

    /*
     * In some cases we need to compute a tuple descriptor for the function's
     * output.	If so, it's stored here.
     */
    TupleDesc funcResultDesc;
    bool funcReturnsTuple; /* valid when funcResultDesc isn't
                            * NULL */

    /*
     * setArgsValid is true when we are evaluating a set-returning function
     * that uses value-per-call mode and we are in the middle of a call
     * series; we want to pass the same argument values to the function again
     * (and again, until it returns ExprEndResult).  This indicates that
     * fcinfo_data already contains valid argument data.
     */
    bool setArgsValid;

    /*
     * Flag to remember whether we found a set-valued argument to the
     * function. This causes the function result to be a set as well. Valid
     * only when setArgsValid is true or funcResultStore isn't NULL.
     */
    bool setHasSetArg; /* some argument returns a set */

    /*
     * Flag to remember whether we have registered a shutdown callback for
     * this FuncExprState.	We do so only if funcResultStore or setArgsValid
     * has been set at least once (since all the callback is for is to release
     * the tuplestore or clear setArgsValid).
     */
    bool shutdown_reg; /* a shutdown callback is registered */

    /*
     * Call parameter structure for the function.  This has been initialized
     * (by InitFunctionCallInfoData) if func.fn_oid is valid.  It also saves
     * argument values between calls, when setArgsValid is true.
     */
    FunctionCallInfoData fcinfo_data;

    ScalarVector* tmpVec;
} FuncExprState;

/* ----------------
 *		ScalarArrayOpExprState node
 *
 * This is a FuncExprState plus some additional data.
 * ----------------
 */
typedef struct ScalarArrayOpExprState {
    FuncExprState fxprstate;
    /* Cached info about array element type */
    Oid element_type;
    int16 typlen;
    bool typbyval;
    char typalign;
    bool* pSel; /* selection used to fast path of ALL/ANY */
    ScalarVector *tmpVecLeft;
    ScalarVector *tmpVecRight;
    ScalarVector* tmpVec;
} ScalarArrayOpExprState;

/* ----------------
 *		BoolExprState node
 * ----------------
 */
typedef struct BoolExprState {
    ExprState xprstate;
    List* args; /* states of argument expression(s) */

    bool tmpSelection[BatchMaxSize];  // temp selection
} BoolExprState;

/* ----------------
 *		SubPlanState node
 * ----------------
 */
typedef struct SubPlanState {
    ExprState xprstate;
    struct PlanState* planstate; /* subselect plan's state tree */
    ExprState* testexpr;         /* state of combining expression */
    List* args;                  /* states of argument expression(s) */
    ExprState* row_testexpr;     /* for vector hash subplan */
    HeapTuple curTuple;          /* copy of most recent tuple from subplan */
    Datum curArray;              /* most recent array from ARRAY() subplan */
    /* these are used when hashing the subselect's output: */
    ProjectionInfo* projLeft;   /* for projecting lefthand exprs */
    ProjectionInfo* projRight;  /* for projecting subselect output */
    TupleHashTable hashtable;   /* hash table for no-nulls subselect rows */
    TupleHashTable hashnulls;   /* hash table for rows with null(s) */
    bool havehashrows;          /* TRUE if hashtable is not empty */
    bool havenullrows;          /* TRUE if hashnulls is not empty */
    MemoryContext hashtablecxt; /* memory context containing hash tables */
    MemoryContext hashtempcxt;  /* temp memory context for hash tables */
    ExprContext* innerecontext; /* econtext for computing inner tuples */
    AttrNumber* keyColIdx;      /* control data for hash tables */
    FmgrInfo* tab_hash_funcs;   /* hash functions for table datatype(s) */
    FmgrInfo* tab_eq_funcs;     /* equality functions for table datatype(s) */
    FmgrInfo* lhs_hash_funcs;   /* hash functions for lefthand datatype(s) */
    FmgrInfo* cur_eq_funcs;     /* equality functions for LHS vs. table */

    /* for vector engine */
    int idx;                             /* a index to indicate which parameter to be pushed */
    ScalarVector** pParamVectorArray;    /* a array to store the param data */
    ScalarVector** pParamVectorTmp;      /* a temporary place to store the para data pointer */
    bool* pSel;                          /* selection used to fast path */
    VectorBatch* outExprBatch;           /* a batch for only one row to store the para data for vector expr */
    VectorBatch* innerExprBatch;         /* a batch for only one row to store the para data for vector expr */
    VectorBatch* scanExprBatch;          /* a batch for only one row to store the para data for vector expr */
    VectorBatch* aggExprBatch;           /* a batch for only one row to store the para data for vector expr */
    ScalarVector* tempvector;            /* a temp vector for vector expression */
    MemoryContext ecxt_per_batch_memory; /* memory contexts for one batch */
} SubPlanState;

/* ----------------
 *		AlternativeSubPlanState node
 * ----------------
 */
typedef struct AlternativeSubPlanState {
    ExprState xprstate;
    List* subplans; /* states of alternative subplans */
    int active;     /* list index of the one we're using */
} AlternativeSubPlanState;

/* ----------------
 *		FieldSelectState node
 * ----------------
 */
typedef struct FieldSelectState {
    ExprState xprstate;
    ExprState* arg;    /* input expression */
    TupleDesc argdesc; /* tupdesc for most recent input */
} FieldSelectState;

/* ----------------
 *		FieldStoreState node
 * ----------------
 */
typedef struct FieldStoreState {
    ExprState xprstate;
    ExprState* arg;    /* input tuple value */
    List* newvals;     /* new value(s) for field(s) */
    TupleDesc argdesc; /* tupdesc for most recent input */
} FieldStoreState;

/* ----------------
 *		CoerceViaIOState node
 * ----------------
 */
typedef struct CoerceViaIOState {
    ExprState xprstate;
    ExprState* arg;   /* input expression */
    FmgrInfo outfunc; /* lookup info for source output function */
    FmgrInfo infunc;  /* lookup info for result input function */
    Oid intypioparam; /* argument needed for input function */
} CoerceViaIOState;

/* ----------------
 *		ArrayCoerceExprState node
 * ----------------
 */
typedef struct ArrayCoerceExprState {
    ExprState xprstate;
    ExprState* arg;     /* input array value */
    Oid resultelemtype; /* element type of result array */
    FmgrInfo elemfunc;  /* lookup info for element coercion function */
    /* use struct pointer to avoid including array.h here */
    struct ArrayMapState* amstate; /* workspace for array_map */
} ArrayCoerceExprState;

/* ----------------
 *		ConvertRowtypeExprState node
 * ----------------
 */
typedef struct ConvertRowtypeExprState {
    ExprState xprstate;
    ExprState* arg;    /* input tuple value */
    TupleDesc indesc;  /* tupdesc for source rowtype */
    TupleDesc outdesc; /* tupdesc for result rowtype */
    /* use "struct" so we needn't include tupconvert.h here */
    struct TupleConversionMap* map;
    bool initialized;
} ConvertRowtypeExprState;

/* ----------------
 *		CaseExprState node
 * ----------------
 */
typedef struct CaseExprState {
    ExprState xprstate;
    ExprState* arg;       /* implicit equality comparison argument */
    List* args;           /* the arguments (list of WHEN clauses) */
    ExprState* defresult; /* the default result (ELSE clause) */
    bool* matchedResult;
    bool* localSel;
    ScalarVector* save_vector;
} CaseExprState;

/* ----------------
 *		CaseWhenState node
 * ----------------
 */
typedef struct CaseWhenState {
    ExprState xprstate;
    ExprState* expr;   /* condition expression */
    ExprState* result; /* substitution result */
} CaseWhenState;

/* ----------------
 *		ArrayExprState node
 *
 * Note: ARRAY[] expressions always produce varlena arrays, never fixed-length
 * arrays.
 * ----------------
 */
typedef struct ArrayExprState {
    ExprState xprstate;
    List* elements;   /* states for child nodes */
    int16 elemlength; /* typlen of the array element type */
    bool elembyval;   /* is the element type pass-by-value? */
    char elemalign;   /* typalign of the element type */
} ArrayExprState;

/* ----------------
 *		RowExprState node
 * ----------------
 */
typedef struct RowExprState {
    ExprState xprstate;
    List* args;            /* the arguments */
    TupleDesc tupdesc;     /* descriptor for result tuples */
    VectorBatch* rowBatch; /* Save need columns */
} RowExprState;

/* ----------------
 *		RowCompareExprState node
 * ----------------
 */
typedef struct RowCompareExprState {
    ExprState xprstate;
    List* largs;     /* the left-hand input arguments */
    List* rargs;     /* the right-hand input arguments */
    FmgrInfo* funcs; /* array of comparison function info */
    Oid* collations; /* array of collations to use */

    FunctionCallInfoData* cinfo;

    ScalarVector* left_argvec;  /* the left-hand input vector arguments */
    ScalarVector* right_argvec; /* the right-hand input vector arguments */
    ScalarVector* cmpresult;    /* vector for compare result */
    bool* pSel;                 /* vector for marking arguments to calculation */
} RowCompareExprState;

/* ----------------
 *		CoalesceExprState node
 * ----------------
 */
typedef struct CoalesceExprState {
    ExprState xprstate;
    List* args; /* the arguments */
    bool* pSel; /* selection used to fast path */
} CoalesceExprState;

/* ----------------
 *		MinMaxExprState node
 * ----------------
 */
typedef struct MinMaxExprState {
    ExprState xprstate;
    List* args;     /* the arguments */
    FmgrInfo cfunc; /* lookup info for comparison func */
    FunctionCallInfoData cinfo;

    ScalarVector* argvec; /* eval arg results */
    ScalarVector* cmpresult;
    bool* pSel;
} MinMaxExprState;

/* ----------------
 *		XmlExprState node
 * ----------------
 */
typedef struct XmlExprState {
    ExprState xprstate;
    List* named_args; /* ExprStates for named arguments */
    List* args;       /* ExprStates for other arguments */
} XmlExprState;

/* ----------------
 *		NullTestState node
 * ----------------
 */
typedef struct NullTestState {
    ExprState xprstate;
    ExprState* arg; /* input expression */
    /* used only if input is of composite type: */
    TupleDesc argdesc; /* tupdesc for most recent input */
} NullTestState;

/* ----------------
 *		HashFilterState node
 * ----------------
 */
typedef struct HashFilterState {
    ExprState xprstate;
    List* arg;       /* input expression */
    uint2* nodelist; /* Node indices where data is located */
    uint2* bucketMap;
    int    bucketCnt;
} HashFilterState;

/* ----------------
 *		CoerceToDomainState node
 * ----------------
 */
typedef struct CoerceToDomainState {
    ExprState xprstate;
    ExprState* arg; /* input expression */
    /* Cached list of constraints that need to be checked */
    List* constraints; /* list of DomainConstraintState nodes */
} CoerceToDomainState;

/*
 * DomainConstraintState - one item to check during CoerceToDomain
 *
 * Note: this is just a Node, and not an ExprState, because it has no
 * corresponding Expr to link to.  Nonetheless it is part of an ExprState
 * tree, so we give it a name following the xxxState convention.
 */
typedef enum DomainConstraintType { DOM_CONSTRAINT_NOTNULL, DOM_CONSTRAINT_CHECK } DomainConstraintType;

typedef struct DomainConstraintState {
    NodeTag type;
    DomainConstraintType constrainttype; /* constraint type */
    char* name;                          /* name of constraint (for error msgs) */
    ExprState* check_expr;               /* for CHECK, a boolean expression */
} DomainConstraintState;

typedef struct HbktScanSlot {
    int currSlot;
} HbktScanSlot;

/* ----------------------------------------------------------------
 *				 Executor State Trees
 *
 * An executing query has a PlanState tree paralleling the Plan tree
 * that describes the plan.
 * ----------------------------------------------------------------
 */
typedef enum {
	PST_None = 0,
	PST_Norm = 1,
	PST_Scan = 2
} PlanStubType;

/* ----------------
 *		PlanState node
 *
 * We never actually instantiate any PlanState nodes; this is just the common
 * abstract superclass for all PlanState-type nodes.
 * ----------------
 */
typedef struct PlanState {
    NodeTag type;

    Plan* plan; /* associated Plan node */

    EState* state; /* at execution time, states of individual
                    * nodes point to one EState for the whole
                    * top-level plan */

    Instrumentation* instrument; /* Optional runtime stats for this node */

    /*
     * Common structural data for all Plan types.  These links to subsidiary
     * state trees parallel links in the associated plan tree (except for the
     * subPlan list, which does not exist in the plan tree).
     */
    List* targetlist;           /* target list to be computed at this node */
    List* qual;                 /* implicitly-ANDed qual conditions */
    struct PlanState* lefttree; /* input plan tree(s) */
    struct PlanState* righttree;
    List* initPlan; /* Init SubPlanState nodes (un-correlated expr subselects) */
    List* subPlan;  /* SubPlanState nodes in my expressions */

    /*
     * State for management of parameter-change-driven rescanning
     */
    Bitmapset* chgParam; /* set of IDs of changed Params */
    HbktScanSlot hbktScanSlot;

    /*
     * Other run-time state needed by most if not all node types.
     */
    TupleTableSlot* ps_ResultTupleSlot; /* slot for my result tuples */
    ExprContext* ps_ExprContext;        /* node's expression-evaluation context */
    ProjectionInfo* ps_ProjInfo;        /* info for doing tuple projection */
    bool ps_TupFromTlist;               /* state flag for processing set-valued functions in targetlist */

    bool vectorized;  // is vectorized?

    MemoryContext nodeContext; /* Memory Context for this Node */

    bool earlyFreed;                 /* node memory already freed? */
    uint8  stubType;                 /* node stub execution type, see @PlanStubType */
    vectarget_func jitted_vectarget; /* LLVM IR function pointer to point to the codegened targetlist expr. */

    /*
     * Describe issues found in curernt plan node, mainly used for issue de-duplication
     * of data skew and inaccurate e-rows
     */
    List* plan_issues;
    bool recursive_reset; /* node already reset? */
    bool qual_is_inited;

    bool do_not_reset_rownum;
    int64 ps_rownum;    /* store current rownum */
} PlanState;

static inline bool planstate_need_stub(PlanState* ps)
{
	return ps->stubType != PST_None;
}

/* ----------------
 *	these are defined to avoid confusion problems with "left"
 *	and "right" and "inner" and "outer".  The convention is that
 *	the "left" plan is the "outer" plan and the "right" plan is
 *	the inner plan, but these make the code more readable.
 * ----------------
 */
#define innerPlanState(node) (((PlanState*)(node))->righttree)
#define outerPlanState(node) (((PlanState*)(node))->lefttree)

/* Macros for inline access to certain instrumentation counters */
#define InstrCountFiltered1(node, delta)                             \
    do {                                                             \
        if (((PlanState*)(node))->instrument)                        \
            ((PlanState*)(node))->instrument->nfiltered1 += (delta); \
    } while (0)
#define InstrCountFiltered2(node, delta)                             \
    do {                                                             \
        if (((PlanState*)(node))->instrument)                        \
            ((PlanState*)(node))->instrument->nfiltered2 += (delta); \
    } while (0)

/*
 * EPQState is state for executing an EvalPlanQual recheck on a candidate
 * tuple in ModifyTable or LockRows.  The estate and planstate fields are
 * NULL if inactive.
 */
typedef struct EPQState {
    EState* estate;           /* subsidiary EState */
    PlanState* planstate;     /* plan state tree ready to be executed */
    TupleTableSlot* origslot; /* original output tuple to be rechecked */
    Plan* plan;               /* plan tree to be executed */
    List* arowMarks;          /* ExecAuxRowMarks (non-locking only) */
    int epqParam;             /* ID of Param to force scan node re-eval */
    /*
     * We need its memory context to palloc es_epqTupleSlot if needed
     */
    EState                  *parentestate;
} EPQState;

/* ----------------
 *	 ResultState information
 * ----------------
 */
typedef struct ResultState {
    PlanState ps; /* its first field is NodeTag */
    ExprState* resconstantqual;
    bool rs_done;      /* are we done? */
    bool rs_checkqual; /* do we need to check the qual? */
} ResultState;

/* ----------------
 *	 MergeActionState information
 * ----------------
 */
typedef struct MergeActionState {
    NodeTag type;
    bool matched;           /* true=MATCHED, false=NOT MATCHED */
    ExprState* whenqual;    /* WHEN AND conditions */
    CmdType commandType;    /* INSERT/UPDATE/DELETE/DO NOTHING */
    ProjectionInfo* proj;   /* tuple projection info */
    TupleDesc tupDesc;      /* tuple descriptor for projection */
    JunkFilter* junkfilter; /* junkfilter for UPDATE */
    VectorBatch* scanBatch; /* scan batch for UPDATE */
} MergeActionState;

/* ----------------
 *	 UpsertState information
 * ----------------
 */
typedef struct UpsertState
{
    NodeTag         type;
    UpsertAction    us_action;              /* Flags showing DUPLICATE UPDATE NOTHING or SOMETHING */
    TupleTableSlot  *us_existing;           /* slot to store existing target tuple in */
    List            *us_excludedtlist;      /* the excluded pseudo relation's tlist */
    TupleTableSlot  *us_updateproj;         /* slot to update */
    List            *us_updateWhere;        /* state for the upsert where clause */
} UpsertState;

/* ----------------
 *	 ModifyTableState information
 * ----------------
 */
typedef struct ModifyTableState {
    PlanState ps;         /* its first field is NodeTag */
    CmdType operation;    /* INSERT, UPDATE, or DELETE */
    bool canSetTag;       /* do we set the command tag/es_processed? */
    bool mt_done;         /* are we done? */
    PlanState** mt_plans; /* subplans (one per target rel) */
#ifdef PGXC
    PlanState** mt_remoterels;        /* per-target remote query node */
    PlanState** mt_insert_remoterels; /* per-target remote query node */
    PlanState** mt_update_remoterels; /* per-target remote query node */
    PlanState** mt_delete_remoterels; /* per-target remote query node */
#endif
    int mt_nplans;                /* number of plans in the array */
    int mt_whichplan;             /* which one is being executed (0..n-1) */
    ResultRelInfo* resultRelInfo; /* per-subplan target relations */
    List** mt_arowmarks;          /* per-subplan ExecAuxRowMark lists */
    EPQState mt_epqstate;         /* for evaluating EvalPlanQual rechecks */
    bool fireBSTriggers;          /* do we need to fire stmt triggers? */
    Relation delete_delta_rel;    /* for online expansion's delete data catchup */

    // For error table
    //
    Relation errorRel;
    ErrorCacheEntry* cacheEnt;

    TupleTableSlot* mt_scan_slot;
    TupleTableSlot* mt_update_constr_slot; /* slot to store target tuple in for checking constraints */
    TupleTableSlot* mt_insert_constr_slot; /* slot to store target tuple in for checking constraints */
    TupleTableSlot* mt_mergeproj;          /* MERGE action projection target */
    uint32 mt_merge_subcommands;           /* Flags showing which subcommands are present INS/UPD/DEL/DO NOTHING */
    UpsertState* mt_upsert;                /*  DUPLICATE KEY UPDATE evaluation state */
    instr_time first_tuple_modified; /* record the end time for the first tuple inserted, deleted, or updated */
    ExprContext* limitExprContext; /* for limit expresssion */
} ModifyTableState;

typedef struct CopyFromManagerData* CopyFromManager;

typedef struct DistInsertSelectState {
    ModifyTableState mt;
    int rows;
    MemoryContext insert_mcxt;
    CopyFromManager mgr;
    BulkInsertState bistate;
    PageCompress* pcState;
} DistInsertSelectState;

/* ----------------
 *	 AppendState information
 *
 *		nplans			how many plans are in the array
 *		whichplan		which plan is being executed (0 .. n-1)
 * ----------------
 */
typedef struct AppendState {
    PlanState ps;            /* its first field is NodeTag */
    PlanState** appendplans; /* array of PlanStates for my inputs */
    int as_nplans;
    int as_whichplan;
} AppendState;

/* ----------------
 *	 MergeAppendState information
 *
 *		nplans			how many plans are in the array
 *		nkeys			number of sort key columns
 *		sortkeys		sort keys in SortSupport representation
 *		slots			current output tuple of each subplan
 *		heap			heap of active tuples (represented as array indexes)
 *		heap_size		number of active heap entries
 *		initialized		true if we have fetched first tuple from each subplan
 *		last_slot		last subplan fetched from (which must be re-called)
 * ----------------
 */
typedef struct MergeAppendState {
    PlanState ps;           /* its first field is NodeTag */
    PlanState** mergeplans; /* array of PlanStates for my inputs */
    int ms_nplans;
    int ms_nkeys;
    SortSupport ms_sortkeys;   /* array of length ms_nkeys */
    TupleTableSlot** ms_slots; /* array of length ms_nplans */
    int* ms_heap;              /* array of length ms_nplans */
    int ms_heap_size;          /* current active length of ms_heap[] */
    bool ms_initialized;       /* are subplans started? */
    int ms_last_slot;          /* last subplan slot we returned from */
} MergeAppendState;

/* ----------------
 *	 RecursiveUnionState information
 *
 *		RecursiveUnionState is used for performing a recursive union.
 *
 *		recursing			T when we're done scanning the non-recursive term
 *		intermediate_empty	T if intermediate_table is currently empty
 *		working_table		working table (to be scanned by recursive term)
 *		intermediate_table	current recursive output (next generation of WT)
 * ----------------
 */
struct StartWithOpState;
struct RecursiveUnionController;
typedef struct RecursiveUnionState {
    PlanState ps; /* its first field is NodeTag */
    bool recursing;
    bool intermediate_empty;
    Tuplestorestate* working_table;
    Tuplestorestate* intermediate_table;
    int iteration;
    /* Remaining fields are unused in UNION ALL case */
    FmgrInfo* eqfunctions;      /* per-grouping-field equality fns */
    FmgrInfo* hashfunctions;    /* per-grouping-field hash fns */
    MemoryContext tempContext;  /* short-term context for comparisons */
    TupleHashTable hashtable;   /* hash table for tuples already seen */
    MemoryContext tableContext; /* memory context containing hash table */
    MemoryContext convertContext; /* memory context for start with convert tuple */

    /*
     * MPP with-recursive support
     */
    /* Distributed with-recursive execution controller for current RecursiveUnion operator */
    RecursiveUnionController* rucontroller;

    /* record the number of tuples that produced by current iteration step */
    uint64 step_tuple_produced;

    /*
     * The share memory context pointer that is used in distributed recursive CTE
     * processing where WorkTable is access via different stream threads, we need
     * put it on higher level of memory context to persists the whole query runing
     * stage.
     */
    MemoryContext shareContext;

    /* support start with*/
    StartWithOpState *swstate;

    /*
     * tuple's index of relative order position in current iteration level
     * only useful once start with...connect by..siblings all exist.
     */
    uint64 sw_tuple_idx;
} RecursiveUnionState;

/* ----------------
 *	 StartWithOpState information
 * ----------------
 */
#define PSEUDO_COLUMN_NUM 4
typedef struct StartWithOpState
{
    PlanState ps;

    /* other attributes */
    int    swop_status;
    /*
     * An array of TargetEntry reference to store the entry for start-with pseudo
     * return columns's TLE
     * - entry[0]  LEVEL
     * - entry[1]  CONNECT_BY_ISLEAF
     * - entry[2]  CONNECT_BY_ISCYCLE
     * - entry[3]  ROWNUM
     */
    TargetEntry *sw_pseudoCols[PSEUDO_COLUMN_NUM];

    IterationStats iterStats;

    /* variables to help calculate pseodu return columns */
    TupleTableSlot      *sw_workingSlot;  /* A dedicate slot to hold tuple-2-tuple
                                             conversion in side of StartWithOp node,
                                             basically do not share use result tuple
                                             to to avoid to tuple store,fetch,
                                             conversion mess up */
    Tuplestorestate     *sw_workingTable; /* Hold incoming tuple slto from RU and processed
                                             one-by-on and store into resultTable */

    Tuplestorestate     *sw_backupTable;  /* Hold a copy of sw_workingTable*/

    Tuplestorestate     *sw_resultTable;  /* final calculated result stored int */

    AttrNumber           sw_keyAttnum;
    const char          *sw_curKeyArrayStr;

    /* tuple slot value array for conversion (to avoid per-tupe process memory alloc/free) */
    Datum               *sw_values;
    bool                *sw_isnull;
    int                 sw_connect_by_level;
    uint64              sw_rownum;
    int                 sw_level;
    int                 sw_numtuples;       /* number of tuples in current level */

    /*
     * nocycle stop flag, normally is used to handle nocycle stop on order siblings
     * case, as order-siblings add a sort operator on top of RU
     */
    bool                sw_nocycleStopOrderSiblings;

    MemoryContext       sw_context;
    List*               sw_cycle_rowmarks;
} StartWithOpState;

/* ----------------
 *	 BitmapAndState information
 * ----------------
 */
typedef struct BitmapAndState {
    PlanState ps;            /* its first field is NodeTag */
    PlanState** bitmapplans; /* array of PlanStates for my inputs */
    int nplans;              /* number of input plans */
} BitmapAndState;

/* ----------------
 *	 BitmapOrState information
 * ----------------
 */
typedef struct BitmapOrState {
    PlanState ps;            /* its first field is NodeTag */
    PlanState** bitmapplans; /* array of PlanStates for my inputs */
    int nplans;              /* number of input plans */
} BitmapOrState;

/* ----------------------------------------------------------------
 *             Run time predicate information
 * ----------------------------------------------------------------
 */
typedef struct RunTimeParamPredicateInfo {
    ExprState* paraExecExpr; /* internal executor parameter predicate information. */
    Expr* opExpr;            /* the operator expression */
    AttrNumber varNoPos;     /* an order number in the hdfsScanPredicateArr. */
    int32 typeMod;           /* var typmode. */
    Oid datumType;           /* the parameter data type. */
    Oid varTypeOid;          /* var type oid. */
    int32 paramPosition;     /* the parameter predicate position in *hdfsScanPredicateArr. */
} RunTimeParamPredicateInfo;

/* ----------------
 *   SampleScanInfo information
 * ----------------
 */
typedef struct SampleScanParams {
    List* args;                 /* expr states for TABLESAMPLE params */
    ExprState* repeatable;      /* expr state for REPEATABLE expr */
    TableSampleType sampleType; /* sample scan type.*/
    /* use struct pointer to avoid including tsmapi.h here */
    void* tsm_state; /* tablesample method can keep state here */
} SampleScanParams;

/* ----------------------------------------------------------------
*				 Batch Scan Information
* ----------------------------------------------------------------
*/
struct ScanBatchResult {
    int rows;           /* rows number for current page. */
    TupleTableSlot** scanTupleSlotInBatch; /* array size of BatchMaxSize, stores tuples scanned in a page */
};

struct ScanBatchState {
    VectorBatch*    pCurrentBatch;  /* for output in batch */
    VectorBatch*    pScanBatch;     /* batch formed from tuples */
    int             scanTupleSlotMaxNum; /* max row number of tuples can be scanned once */
    int             colNum;
    int *colId;    /* for qual and project, only save the used cols. */
    int maxcolId;
    bool *nullflag;  /*indicate the batch has null value for performance */
    bool *lateRead;  /* for project */
    bool scanfinished; /* last time return with rows, but pages of this partition is read out */
    ScanBatchResult scanBatch;
};

/* ----------------------------------------------------------------
 *				 Scan State Information
 * ----------------------------------------------------------------
 */

/* ----------------
 *	 ScanState information
 *
 *		ScanState extends PlanState for node types that represent
 *		scans of an underlying relation.  It can also be used for nodes
 *		that scan the output of an underlying plan node --- in that case,
 *		only ScanTupleSlot is actually useful, and it refers to the tuple
 *		retrieved from the subplan.
 *
 *		currentRelation    relation being scanned (NULL if none)
 *		currentScanDesc    current scan descriptor for scan (NULL if none)
 *		ScanTupleSlot	   pointer to slot in tuple table holding scan tuple
 * ----------------
 */
struct ScanState;
struct SeqScanAccessor;

/*
 * prototypes from functions in execScan.c
 */
typedef TupleTableSlot *(*ExecScanAccessMtd) (ScanState *node);
typedef bool(*ExecScanRecheckMtd) (ScanState *node, TupleTableSlot *slot);
typedef void (*SeqScanGetNextMtd)(TableScanDesc scan, TupleTableSlot* slot, ScanDirection direction);

typedef struct ScanState {
    PlanState ps; /* its first field is NodeTag */
    Relation ss_currentRelation;
    TableScanDesc ss_currentScanDesc;
    TupleTableSlot* ss_ScanTupleSlot;
    bool ss_ReScan;
    Relation ss_currentPartition;
    bool isPartTbl;
    int currentSlot; /* current iteration position */
    ScanDirection partScanDirection;
    List* partitions; /* list of Partition */
    List* subpartitions; /* list of SubPartition */
    LOCKMODE lockMode;
    List* runTimeParamPredicates;
    bool runTimePredicatesReady;
    bool is_scan_end; /* @hdfs Mark whether iterator is over or not, if the scan uses informational constraint. */
    SeqScanAccessor* ss_scanaccessor; /* prefetch related */
    int part_id;
    List* subPartLengthList;
    int startPartitionId;            /* start partition id for parallel threads. */
    int endPartitionId;              /* end partition id for parallel threads. */
    RangeScanInRedis rangeScanInRedis;         /* if it is a range scan in redistribution time */
    bool isSampleScan;               /* identify is it table sample scan or not. */
    SampleScanParams sampleScanInfo; /* TABLESAMPLE params include type/seed/repeatable. */
    SeqScanGetNextMtd  fillNextSlotFunc;
    ExecScanAccessMtd ScanNextMtd;
    bool scanBatchMode;
    ScanBatchState* scanBatchState;
} ScanState;

/*
 * SeqScan uses a bare ScanState as its state node, since it needs
 * no additional fields.
 */
typedef ScanState SeqScanState;

/*
 * These structs store information about index quals that don't have simple
 * constant right-hand sides.  See comments for ExecIndexBuildScanKeys()
 * for discussion.
 */
typedef struct {
    ScanKey scan_key;    /* scankey to put value into */
    ExprState* key_expr; /* expr to evaluate to get value */
    bool key_toastable;  /* is expr's result a toastable datatype? */
} IndexRuntimeKeyInfo;

typedef struct {
    ScanKey scan_key;      /* scankey to put value into */
    ExprState* array_expr; /* expr to evaluate to get array value */
    int next_elem;         /* next array element to use */
    int num_elems;         /* number of elems in current array value */
    Datum* elem_values;    /* array of num_elems Datums */
    bool* elem_nulls;      /* array of num_elems is-null flags */
} IndexArrayKeyInfo;

/* ----------------
 *	 IndexScanState information
 *
 *		indexqualorig	   execution state for indexqualorig expressions
 *		ScanKeys		   Skey structures for index quals
 *		NumScanKeys		   number of ScanKeys
 *		OrderByKeys		   Skey structures for index ordering operators
 *		NumOrderByKeys	   number of OrderByKeys
 *		RuntimeKeys		   info about Skeys that must be evaluated at runtime
 *		NumRuntimeKeys	   number of RuntimeKeys
 *		RuntimeKeysReady   true if runtime Skeys have been computed
 *		RuntimeContext	   expr context for evaling runtime Skeys
 *		RelationDesc	   index relation descriptor
 *		ScanDesc		   index scan descriptor
 * ----------------
 */
typedef struct IndexScanState {
    ScanState ss; /* its first field is NodeTag */
    List* indexqualorig;
    ScanKey iss_ScanKeys;
    int iss_NumScanKeys;
    ScanKey iss_OrderByKeys;
    int iss_NumOrderByKeys;
    IndexRuntimeKeyInfo* iss_RuntimeKeys;
    int iss_NumRuntimeKeys;
    bool iss_RuntimeKeysReady;
    ExprContext* iss_RuntimeContext;
    Relation iss_RelationDesc;
    IndexScanDesc iss_ScanDesc;
    List* iss_IndexPartitionList;
    LOCKMODE lockMode;
    Relation iss_CurrentIndexPartition;
} IndexScanState;

/* ----------------
 *	 IndexOnlyScanState information
 *
 *		indexqual		   execution state for indexqual expressions
 *		ScanKeys		   Skey structures for index quals
 *		NumScanKeys		   number of ScanKeys
 *		OrderByKeys		   Skey structures for index ordering operators
 *		NumOrderByKeys	   number of OrderByKeys
 *		RuntimeKeys		   info about Skeys that must be evaluated at runtime
 *		NumRuntimeKeys	   number of RuntimeKeys
 *		RuntimeKeysReady   true if runtime Skeys have been computed
 *		RuntimeContext	   expr context for evaling runtime Skeys
 *		RelationDesc	   index relation descriptor
 *		ScanDesc		   index scan descriptor
 *		VMBuffer		   buffer in use for visibility map testing, if any
 *		HeapFetches		   number of tuples we were forced to fetch from heap
 * ----------------
 */
typedef struct IndexOnlyScanState {
    ScanState ss; /* its first field is NodeTag */
    List* indexqual;
    ScanKey ioss_ScanKeys;
    int ioss_NumScanKeys;
    ScanKey ioss_OrderByKeys;
    int ioss_NumOrderByKeys;
    IndexRuntimeKeyInfo* ioss_RuntimeKeys;
    int ioss_NumRuntimeKeys;
    bool ioss_RuntimeKeysReady;
    ExprContext* ioss_RuntimeContext;
    Relation ioss_RelationDesc;
    IndexScanDesc ioss_ScanDesc;
    Buffer ioss_VMBuffer;
    long ioss_HeapFetches;
    List* ioss_IndexPartitionList;
    LOCKMODE lockMode;
    Relation ioss_CurrentIndexPartition;
} IndexOnlyScanState;

/* ----------------
 *	 BitmapIndexScanState information
 *
 *		result			   bitmap to return output into, or NULL
 *		ScanKeys		   Skey structures for index quals
 *		NumScanKeys		   number of ScanKeys
 *		RuntimeKeys		   info about Skeys that must be evaluated at runtime
 *		NumRuntimeKeys	   number of RuntimeKeys
 *		ArrayKeys		   info about Skeys that come from ScalarArrayOpExprs
 *		NumArrayKeys	   number of ArrayKeys
 *		RuntimeKeysReady   true if runtime Skeys have been computed
 *		RuntimeContext	   expr context for evaling runtime Skeys
 *		RelationDesc	   index relation descriptor
 *		ScanDesc		   index scan descriptor
 * ----------------
 */
typedef struct BitmapIndexScanState {
    ScanState ss; /* its first field is NodeTag */
    TIDBitmap* biss_result;
    ScanKey biss_ScanKeys;
    int biss_NumScanKeys;
    IndexRuntimeKeyInfo* biss_RuntimeKeys;
    int biss_NumRuntimeKeys;
    IndexArrayKeyInfo* biss_ArrayKeys;
    int biss_NumArrayKeys;
    bool biss_RuntimeKeysReady;
    ExprContext* biss_RuntimeContext;
    Relation biss_RelationDesc;
    IndexScanDesc biss_ScanDesc;
    List* biss_IndexPartitionList;
    LOCKMODE lockMode;
    Relation biss_CurrentIndexPartition;
} BitmapIndexScanState;

/* ----------------
 *	 BitmapHeapScanState information
 *
 *		bitmapqualorig	   execution state for bitmapqualorig expressions
 *		tbm				   bitmap obtained from child index scan(s)
 *		tbmiterator		   iterator for scanning current pages
 *		tbmres			   current-page data
 *		prefetch_iterator  iterator for prefetching ahead of current page
 *		prefetch_pages	   # pages prefetch iterator is ahead of current
 *		prefetch_target    target prefetch distance
 * ----------------
 */
typedef struct BitmapHeapScanState {
    ScanState ss; /* its first field is NodeTag */
    List* bitmapqualorig;
    TIDBitmap* tbm;
    TBMIterator* tbmiterator;
    TBMIterateResult* tbmres;
    long exact_pages;
    long lossy_pages;
    TBMIterator* prefetch_iterator;
    int prefetch_pages;
    int prefetch_target;
    GPIScanDesc gpi_scan;  /* global partition index scan use information */
    CBIScanDesc cbi_scan;  /* for crossbucket index scan */
} BitmapHeapScanState;

/* ----------------
 *	 TidScanState information
 *
 *		isCurrentOf    scan has a CurrentOfExpr qual
 *		NumTids		   number of tids in this scan
 *		TidPtr		   index of currently fetched tid
 *		TidList		   evaluated item pointers (array of size NumTids)
 * ----------------
 */
typedef struct TidScanState {
    ScanState ss;       /* its first field is NodeTag */
    List* tss_tidquals; /* list of ExprState nodes */
    bool tss_isCurrentOf;
    Relation tss_CurrentOf_CurrentPartition;
    int tss_NumTids;
    int tss_TidPtr;
    int tss_MarkTidPtr;
    ItemPointerData* tss_TidList;
    union {
        HeapTupleData tss_htup;
        UHeapTupleData tss_uhtup;
    };
    /* put decompressed tuple data into tss_ctbuf_hdr be careful  , when malloc memory  should give extra mem for
     *xs_ctbuf_hdr. t_bits which is varlength arr  */
    HeapTupleHeaderData tss_ctbuf_hdr;
} TidScanState;

#define SizeofTidScanState (offsetof(TidScanState, tss_ctbuf_hdr) + SizeofHeapTupleHeader)

/* ----------------
 *	 SubqueryScanState information
 *
 *		SubqueryScanState is used for scanning a sub-query in the range table.
 *		ScanTupleSlot references the current output tuple of the sub-query.
 * ----------------
 */
typedef struct SubqueryScanState {
    ScanState ss; /* its first field is NodeTag */
    PlanState* subplan;
} SubqueryScanState;

/* ----------------
 *	 FunctionScanState information
 *
 *		Function nodes are used to scan the results of a
 *		function appearing in FROM (typically a function returning set).
 *
 *		eflags				node's capability flags
 *		tupdesc				expected return tuple description
 *		tuplestorestate		private state of tuplestore.c
 *		funcexpr			state for function expression being evaluated
 * ----------------
 */
typedef struct FunctionScanState {
    ScanState ss; /* Its first field is NodeTag */
    int eflags;
    TupleDesc tupdesc;
    Tuplestorestate* tuplestorestate;
    ExprState* funcexpr;
    bool atomic;  /* Atomic execution context, does not allow transactions */
} FunctionScanState;

/* ----------------
 *	 ValuesScanState information
 *
 *		ValuesScan nodes are used to scan the results of a VALUES list
 *
 *		rowcontext			per-expression-list context
 *		exprlists			array of expression lists being evaluated
 *		array_len			size of array
 *		curr_idx			current array index (0-based)
 *		marked_idx			marked position (for mark/restore)
 *
 *	Note: ss.ps.ps_ExprContext is used to evaluate any qual or projection
 *	expressions attached to the node.  We create a second ExprContext,
 *	rowcontext, in which to build the executor expression state for each
 *	Values sublist.  Resetting this context lets us get rid of expression
 *	state for each row, avoiding major memory leakage over a long values list.
 * ----------------
 */
typedef struct ValuesScanState {
    ScanState ss; /* its first field is NodeTag */
    ExprContext* rowcontext;
    List** exprlists;
    int array_len;
    int curr_idx;
    int marked_idx;
} ValuesScanState;

/* ----------------
 *	 CteScanState information
 *
 *		CteScan nodes are used to scan a CommonTableExpr query.
 *
 * Multiple CteScan nodes can read out from the same CTE query.  We use
 * a tuplestore to hold rows that have been read from the CTE query but
 * not yet consumed by all readers.
 * ----------------
 */
typedef struct CteScanState {
    ScanState ss;            /* its first field is NodeTag */
    int eflags;              /* capability flags to pass to tuplestore */
    int readptr;             /* index of my tuplestore read pointer */
    PlanState* cteplanstate; /* PlanState for the CTE query itself */
    /* Link to the "leader" CteScanState (possibly this same node) */
    struct CteScanState* leader;
    /* The remaining fields are only valid in the "leader" CteScanState */
    Tuplestorestate* cte_table; /* rows already read from the CTE query */
    bool eof_cte;               /* reached end of CTE query? */
} CteScanState;

/* ----------------
 *	 WorkTableScanState information
 *
 *		WorkTableScan nodes are used to scan the work table created by
 *		a RecursiveUnion node.	We locate the RecursiveUnion node
 *		during executor startup.
 * ----------------
 */
typedef struct WorkTableScanState {
    ScanState ss; /* its first field is NodeTag */
    RecursiveUnionState* rustate;
} WorkTableScanState;

/* ----------------
 *	 ForeignScanState information
 *
 *		ForeignScan nodes are used to scan foreign-data tables.
 * ----------------
 */
typedef struct ForeignScanState {
    ScanState ss; /* its first field is NodeTag */
    /* use struct pointer to avoid including fdwapi.h here */
    struct FdwRoutine* fdwroutine;
    void* fdw_state; /* foreign-data wrapper can keep state here */

    MemoryContext scanMcxt;

    ForeignOptions* options;
} ForeignScanState;

/* ----------------
 *	 ExtensiblePlanState information
 *
 *		ExtensiblePlan nodes are used to execute extensible code within executor.
 *
 * Core code must avoid assuming that the ExtensiblePlanState is only as large as
 * the structure declared here; providers are allowed to make it the first
 * element in a larger structure, and typically would need to do so.  The
 * struct is actually allocated by the CreateExtensiblePlanState method associated
 * with the plan node.  Any additional fields can be initialized there, or in
 * the BeginExtensiblePlan method.
 * ----------------
 */
struct ExplainState; /* avoid including explain.h here */
struct ExtensiblePlanState;

typedef struct ExtensibleExecMethods {
    const char* ExtensibleName;

    /* Executor methods: mark/restore are optional, the rest are required */
    void (*BeginExtensiblePlan)(struct ExtensiblePlanState* node, EState* estate, int eflags);
    TupleTableSlot* (*ExecExtensiblePlan)(struct ExtensiblePlanState* node);
    void (*EndExtensiblePlan)(struct ExtensiblePlanState* node);
    void (*ReScanExtensiblePlan)(struct ExtensiblePlanState* node);
    void (*ExplainExtensiblePlan)(struct ExtensiblePlanState* node, List* ancestors, struct ExplainState* es);
} ExtensibleExecMethods;

typedef struct ExtensiblePlanState {
    ScanState ss;
    uint32 flags;        /* mask of EXTENSIBLEPATH_* flags, see relation.h */
    List* extensible_ps; /* list of child PlanState nodes, if any */
    const ExtensibleExecMethods* methods;
} ExtensiblePlanState;

/* ----------------------------------------------------------------
 *				 Join State Information
 * ----------------------------------------------------------------
 */

/* ----------------
 *	 JoinState information
 *
 *		Superclass for state nodes of join plans.
 * ----------------
 */
typedef struct JoinState {
    PlanState ps;
    JoinType jointype;
    List* joinqual; /* JOIN quals (in addition to ps.qual) */
    List* nulleqqual;
} JoinState;

/* ----------------
 *	 NestLoopState information
 *
 *		NeedNewOuter	   true if need new outer tuple on next call
 *		MatchedOuter	   true if found a join match for current outer tuple
 *		NullInnerTupleSlot prepared null tuple for left outer joins
 * ----------------
 */
typedef struct NestLoopState {
    JoinState js; /* its first field is NodeTag */
    bool nl_NeedNewOuter;
    bool nl_MatchedOuter;
    bool nl_MaterialAll;
    TupleTableSlot* nl_NullInnerTupleSlot;
} NestLoopState;

/* ----------------
 *	 MergeJoinState information
 *
 *		NumClauses		   number of mergejoinable join clauses
 *		Clauses			   info for each mergejoinable clause
 *		JoinState		   current state of ExecMergeJoin state machine
 *		ExtraMarks		   true to issue extra Mark operations on inner scan
 *		ConstFalseJoin	   true if we have a constant-false joinqual
 *		FillOuter		   true if should emit unjoined outer tuples anyway
 *		FillInner		   true if should emit unjoined inner tuples anyway
 *		MatchedOuter	   true if found a join match for current outer tuple
 *		MatchedInner	   true if found a join match for current inner tuple
 *		OuterTupleSlot	   slot in tuple table for cur outer tuple
 *		InnerTupleSlot	   slot in tuple table for cur inner tuple
 *		MarkedTupleSlot    slot in tuple table for marked tuple
 *		NullOuterTupleSlot prepared null tuple for right outer joins
 *		NullInnerTupleSlot prepared null tuple for left outer joins
 *		OuterEContext	   workspace for computing outer tuple's join values
 *		InnerEContext	   workspace for computing inner tuple's join values
 * ----------------
 */
/* private in nodeMergejoin.c: */
typedef struct MergeJoinClauseData* MergeJoinClause;

typedef struct MergeJoinState {
    JoinState js; /* its first field is NodeTag */
    int mj_NumClauses;
    MergeJoinClause mj_Clauses; /* array of length mj_NumClauses */
    int mj_JoinState;
    bool mj_ExtraMarks;
    bool mj_ConstFalseJoin;
    bool mj_FillOuter;
    bool mj_FillInner;
    bool mj_MatchedOuter;
    bool mj_MatchedInner;
    TupleTableSlot* mj_OuterTupleSlot;
    TupleTableSlot* mj_InnerTupleSlot;
    TupleTableSlot* mj_MarkedTupleSlot;
    TupleTableSlot* mj_NullOuterTupleSlot;
    TupleTableSlot* mj_NullInnerTupleSlot;
    ExprContext* mj_OuterEContext;
    ExprContext* mj_InnerEContext;
} MergeJoinState;

struct MergeJoinShared {
    JoinState js; /* its first field is NodeTag */
    int mj_NumClauses;
    int mj_JoinState;
    bool mj_ExtraMarks;
    bool mj_ConstFalseJoin;
    bool mj_FillOuter;
    bool mj_FillInner;
    bool mj_MatchedOuter;
    bool mj_MatchedInner;
};

/* private in vecmergejoin.cpp: */
typedef struct VecMergeJoinClauseData* VecMergeJoinClause;

// Mark the offset of a row in a batch
//      It is the same with integer and we rely on compiler to generate efficient
//      code for structure copy.
struct MJBatchOffset {
    // offset in the batch, starting from zero
    uint16 m_offset;

    // Flag: is it actually representing an empty row?
    bool m_fEmpty;
    // Flag: is the offset pointing to the marked batch?
    bool m_fMarked;
    // batch sequence: ok to wrap around
    int m_batchSeq;
};

struct BatchAccessor {
    // child query to retrieve data
    PlanState* m_plan;
    // Current offset. It also points to the batch the offset is against.
    // The maxOffset is used to detect the end of the batch.
    VectorBatch* m_curBatch;
    int m_batchSeq;
    int m_curOffset;
    int m_maxOffset;
};

struct VecMergeJoinState : public MergeJoinShared {
    // Vectorization run support
    VecMergeJoinClause mj_Clauses;
    MJBatchOffset mj_OuterOffset;
    MJBatchOffset mj_InnerOffset;
    ExprContext* mj_OuterEContext;
    ExprContext* mj_InnerEContext;
    // Marked batch and offset. We need both to represent a marked row.
    MJBatchOffset mj_MarkedOffset;
    VectorBatch* mj_MarkedBatch;

    // Input batches (inner: 0, outer: 1). We record current progress on the
    // input batches here.
    BatchAccessor m_inputs[2];

    // Previous batch join status. This is needed as some join types need
    // to look backward to decide if we shall output current tuples.
    MJBatchOffset m_prevInnerOffset;
    bool m_prevInnerQualified;
    MJBatchOffset m_prevOuterOffset;
    bool m_prevOuterQualified;

    // Result batch and intermediate results. pInner and pOuter hold the join
    // candiates passed the join key checks.
    bool m_fDone;
    VectorBatch* m_pInnerMatch;
    MJBatchOffset* m_pInnerOffset;
    VectorBatch* m_pOuterMatch;
    MJBatchOffset* m_pOuterOffset;
    VectorBatch* m_pCurrentBatch;
    VectorBatch* m_pReturnBatch;
    vecqual_func jitted_joinqual; /* LLVM IR function pointer to point to codegened mj_joinqualexpr */
};

/* ----------------
 *	 HashJoinState information
 *
 *		hashclauses				original form of the hashjoin condition
 *		hj_OuterHashKeys		the outer hash keys in the hashjoin condition
 *		hj_InnerHashKeys		the inner hash keys in the hashjoin condition
 *		hj_HashOperators		the join operators in the hashjoin condition
 *		hj_HashTable			hash table for the hashjoin
 *								(NULL if table not built yet)
 *		hj_CurHashValue			hash value for current outer tuple
 *		hj_CurBucketNo			regular bucket# for current outer tuple
 *		hj_CurSkewBucketNo		skew bucket# for current outer tuple
 *		hj_CurTuple				last inner tuple matched to current outer
 *								tuple, or NULL if starting search
 *								(hj_CurXXX variables are undefined if
 *								OuterTupleSlot is empty!)
 *		hj_OuterTupleSlot		tuple slot for outer tuples
 *		hj_HashTupleSlot		tuple slot for inner (hashed) tuples
 *		hj_NullOuterTupleSlot	prepared null tuple for right/full outer joins
 *		hj_NullInnerTupleSlot	prepared null tuple for left/full outer joins
 *		hj_FirstOuterTupleSlot	first tuple retrieved from outer plan
 *		hj_JoinState			current state of ExecHashJoin state machine
 *		hj_MatchedOuter			true if found a join match for current outer
 *		hj_OuterNotEmpty		true if outer relation known not empty
 * ----------------
 */
/* these structs are defined in executor/hashjoin.h: */
typedef struct HashJoinTupleData* HashJoinTuple;
typedef struct HashJoinTableData* HashJoinTable;

typedef struct HashJoinState {
    JoinState js;           /* its first field is NodeTag */
    List* hashclauses;      /* list of ExprState nodes */
    List* hj_OuterHashKeys; /* list of ExprState nodes */
    List* hj_InnerHashKeys; /* list of ExprState nodes */
    List* hj_HashOperators; /* list of operator OIDs */
    HashJoinTable hj_HashTable;
    uint32 hj_CurHashValue;
    int hj_CurBucketNo;
    int hj_CurSkewBucketNo;
    HashJoinTuple hj_CurTuple;
    /* pointer which follows hj_CurTuple help us to delete matched tuples in HashTable.designed for Right Semi/Anti Join */
    HashJoinTuple hj_PreTuple;
    TupleTableSlot* hj_OuterTupleSlot;
    TupleTableSlot* hj_HashTupleSlot;
    TupleTableSlot* hj_NullOuterTupleSlot;
    TupleTableSlot* hj_NullInnerTupleSlot;
    TupleTableSlot* hj_FirstOuterTupleSlot;
    int hj_JoinState;
    bool hj_MatchedOuter;
    bool hj_OuterNotEmpty;
    bool hj_streamBothSides;
    bool hj_rebuildHashtable;
} HashJoinState;

/* ----------------------------------------------------------------
 *				 Materialization State Information
 * ----------------------------------------------------------------
 */

/* ----------------
 *	 MaterialState information
 *
 *		materialize nodes are used to materialize the results
 *		of a subplan into a temporary file.
 *
 *		ss.ss_ScanTupleSlot refers to output of underlying plan.
 * ----------------
 */
typedef struct MaterialState {
    ScanState ss;        /* its first field is NodeTag */
    int eflags;          /* capability flags to pass to tuplestore */
    bool eof_underlying; /* reached end of underlying plan? */
    bool materalAll;
    Tuplestorestate* tuplestorestate;
} MaterialState;

/* ----------------
 *	 SortState information
 * ----------------
 */
typedef struct SortState {
    ScanState ss;         /* its first field is NodeTag */
    bool randomAccess;    /* need random access to sort output? */
    bool bounded;         /* is the result set bounded? */
    int64 bound;          /* if bounded, how many tuples are needed */
    bool sort_Done;       /* sort completed yet? */
    bool bounded_Done;    /* value of bounded we did the sort with */
    int64 bound_Done;     /* value of bound we did the sort with */
    void* tuplesortstate; /* private state of tuplesort.c */
    int32 local_work_mem; /* work_mem local for this sort */
    int sortMethodId;     /* sort method for explain */
    int spaceTypeId;      /* space type for explain */
    long spaceUsed;       /* space used for explain */
    int64* space_size;    /* spill size for temp table */
} SortState;

/* ---------------------
 *	GroupState information
 * -------------------------
 */
typedef struct GroupState {
    ScanState ss;          /* its first field is NodeTag */
    FmgrInfo* eqfunctions; /* per-field lookup data for equality fns */
    bool grp_done;         /* indicates completion of Group scan */
} GroupState;

/* ---------------------
 *	AggState information
 *
 *	ss.ss_ScanTupleSlot refers to output of underlying plan.
 *
 *	Note: ss.ps.ps_ExprContext contains ecxt_aggvalues and
 *	ecxt_aggnulls arrays, which hold the computed agg values for the current
 *	input group during evaluation of an Agg node's output tuple(s).  We
 *	create a second ExprContext, tmpcontext, in which to evaluate input
 *	expressions and run the aggregate transition functions.
 * -------------------------
 */
/* these structs are private in nodeAgg.c: */
typedef struct AggStatePerAggData* AggStatePerAgg;
typedef struct AggStatePerGroupData* AggStatePerGroup;
typedef struct AggStatePerPhaseData* AggStatePerPhase;

typedef struct AggState {
    ScanState ss;               /* its first field is NodeTag */
    List* aggs;                 /* all Aggref nodes in targetlist & quals */
    int numaggs;                /* length of list (could be zero!) */
    AggStatePerPhase phase;     /* pointer to current phase data */
    int numphases;              /* number of phases */
    int current_phase;          /* current phase number */
    FmgrInfo* hashfunctions;    /* per-grouping-field hash fns */
    AggStatePerAgg peragg;      /* per-Aggref information */
    MemoryContext* aggcontexts; /* memory context for long-lived data */
    ExprContext* tmpcontext;    /* econtext for input expressions */
    AggStatePerAgg curperagg;   /* identifies currently active aggregate */
    bool input_done;            /* indicates end of input */
    bool agg_done;              /* indicates completion of Agg scan */
    int projected_set;          /* The last projected grouping set */
    int current_set;            /* The current grouping set being evaluated */
    Bitmapset* grouped_cols;    /* grouped cols in current projection */
    List* all_grouped_cols;     /* list of all grouped cols in DESC order */
    /* These fields are for grouping set phase data */
    int maxsets;               /* The max number of sets in any phase */
    AggStatePerPhase phases;   /* array of all phases */
    Tuplesortstate* sort_in;   /* sorted input to phases > 0 */
    Tuplesortstate* sort_out;  /* input is copied here for next phase */
    TupleTableSlot* sort_slot; /* slot for sort results */
    /* these fields are used in AGG_PLAIN and AGG_SORTED modes: */
    AggStatePerGroup pergroup; /* per-Aggref-per-group working state */
    HeapTuple grp_firstTuple;  /* copy of first tuple of current group */
    /* these fields are used in AGG_HASHED mode: */
    TupleHashTable hashtable;   /* hash table with one entry per group */
    TupleTableSlot* hashslot;   /* slot for loading hash table */
    List* hash_needed;          /* list of columns needed in hash table */
    bool table_filled;          /* hash table filled yet? */
    TupleHashIterator hashiter; /* for iterating through hash table */
#ifdef PGXC
    bool is_final; /* apply the final step for aggregates */
#endif             /* PGXC */
    void* aggTempFileControl;
    FmgrInfo* eqfunctions; /* per-grouping-field equality fns */
} AggState;

/* ----------------
 *	WindowAggState information
 * ----------------
 */
/* these structs are private in nodeWindowAgg.c: */
typedef struct WindowStatePerFuncData* WindowStatePerFunc;
typedef struct WindowStatePerAggData* WindowStatePerAgg;

typedef struct WindowAggState {
    ScanState ss; /* its first field is NodeTag */

    /* these fields are filled in by ExecInitExpr: */
    List* funcs;  /* all WindowFunc nodes in targetlist */
    int numfuncs; /* total number of window functions */
    int numaggs;  /* number that are plain aggregates */

    WindowStatePerFunc perfunc; /* per-window-function information */
    WindowStatePerAgg peragg;   /* per-plain-aggregate information */
    FmgrInfo* partEqfunctions;  /* equality funcs for partition columns */
    FmgrInfo* ordEqfunctions;   /* equality funcs for ordering columns */
    Tuplestorestate* buffer;    /* stores rows of current partition */
    int current_ptr;            /* read pointer # for current */
    int64 spooled_rows;         /* total # of rows in buffer */
    int64 currentpos;           /* position of current row in partition */
    int64 frameheadpos;         /* current frame head position */
    int64 frametailpos;         /* current frame tail position */
    /* use struct pointer to avoid including windowapi.h here */
    struct WindowObjectData* agg_winobj; /* winobj for aggregate fetches */
    int64 aggregatedbase;                /* start row for current aggregates */
    int64 aggregatedupto;                /* rows before this one are aggregated */

    int frameOptions;       /* frame_clause options, see WindowDef */
    ExprState* startOffset; /* expression for starting bound offset */
    ExprState* endOffset;   /* expression for ending bound offset */
    Datum startOffsetValue; /* result of startOffset evaluation */
    Datum endOffsetValue;   /* result of endOffset evaluation */

    MemoryContext partcontext; /* context for partition-lifespan data */
    MemoryContext aggcontext;  /* context for each aggregate data */
    ExprContext* tmpcontext;   /* short-term evaluation context */

    bool all_first;         /* true if the scan is starting */
    bool all_done;          /* true if the scan is finished */
    bool partition_spooled; /* true if all tuples in current partition have been spooled into tuplestore */
    bool more_partitions;   /* true if there's more partitions after this one */
    bool framehead_valid;   /* true if frameheadpos is known up to date for current row */
    bool frametail_valid;   /* true if frametailpos is known up to date for current row */

    TupleTableSlot* first_part_slot; /* first tuple of current or next partition */

    /* temporary slots for tuples fetched back from tuplestore */
    TupleTableSlot* agg_row_slot;
    TupleTableSlot* temp_slot_1;
    TupleTableSlot* temp_slot_2;
} WindowAggState;

/* ----------------
 *	OrderedSetAggState information
 * ----------------
 */
typedef struct OrderedSetAggState {
    AggState* aggstate;
    Aggref* aggref;            /* Keep the agg info for start up used */
    Tuplesortstate* sortstate; /* Used for accumulate sort datum */
    int64 number_of_rows;      /* Number of normal datum inside sortstate */
    TupleDesc tupdesc;         /* Tuple descriptor for datum inside sortstate */

    Oid datumtype; /* Datatype of datums being sorted */
    int16 typLen;
    bool typByVal;
    char typAlign;
    Oid eq_operator; /* Equality operator associated with sort operator */
    char sign;       /* Sign for orderedSetAggState */
} OrderedSetAggState;

/* ----------------
 *	 UniqueState information
 *
 *		Unique nodes are used "on top of" sort nodes to discard
 *		duplicate tuples returned from the sort phase.	Basically
 *		all it does is compare the current tuple from the subplan
 *		with the previously fetched tuple (stored in its result slot).
 *		If the two are identical in all interesting fields, then
 *		we just fetch another tuple from the sort and try again.
 * ----------------
 */
typedef struct UniqueState {
    PlanState ps;              /* its first field is NodeTag */
    FmgrInfo* eqfunctions;     /* per-field lookup data for equality fns */
    MemoryContext tempContext; /* short-term context for comparisons */
} UniqueState;

/* ----------------
 *	 HashState information
 * ----------------
 */
typedef struct HashState {
    PlanState ps;            /* its first field is NodeTag */
    HashJoinTable hashtable; /* hash table for the hashjoin */
    List* hashkeys;          /* list of ExprState nodes */
    int32 local_work_mem;    /* work_mem local for this hash join */
    int64 spill_size;

    /* hashkeys is same as parent's hj_InnerHashKeys */
} HashState;

/* ----------------
 *	 SetOpState information
 *
 *		Even in "sorted" mode, SetOp nodes are more complex than a simple
 *		Unique, since we have to count how many duplicates to return.  But
 *		we also support hashing, so this is really more like a cut-down
 *		form of Agg.
 * ----------------
 */
/* this struct is private in nodeSetOp.c: */
typedef struct SetOpStatePerGroupData* SetOpStatePerGroup;

typedef struct SetOpState {
    PlanState ps;              /* its first field is NodeTag */
    FmgrInfo* eqfunctions;     /* per-grouping-field equality fns */
    FmgrInfo* hashfunctions;   /* per-grouping-field hash fns */
    bool setop_done;           /* indicates completion of output scan */
    long numOutput;            /* number of dups left to output */
    MemoryContext tempContext; /* short-term context for comparisons */
    /* these fields are used in SETOP_SORTED mode: */
    SetOpStatePerGroup pergroup; /* per-group working state */
    HeapTuple grp_firstTuple;    /* copy of first tuple of current group */
    /* these fields are used in SETOP_HASHED mode: */
    TupleHashTable hashtable;   /* hash table with one entry per group */
    MemoryContext tableContext; /* memory context containing hash table */
    bool table_filled;          /* hash table filled yet? */
    TupleHashIterator hashiter; /* for iterating through hash table */
    void* TempFileControl;
    int64 spill_size;
} SetOpState;

/* ----------------
 *	 LockRowsState information
 *
 *		LockRows nodes are used to enforce FOR [KEY] UPDATE/FOR SHARE locking.
 * ----------------
 */
typedef struct LockRowsState {
    PlanState ps;         /* its first field is NodeTag */
    List* lr_arowMarks;   /* List of ExecAuxRowMarks */
    EPQState lr_epqstate; /* for evaluating EvalPlanQual rechecks */
} LockRowsState;

/* ----------------
 *	 LimitState information
 *
 *		Limit nodes are used to enforce LIMIT/OFFSET clauses.
 *		They just select the desired subrange of their subplan's output.
 *
 * offset is the number of initial tuples to skip (0 does nothing).
 * count is the number of tuples to return after skipping the offset tuples.
 * If no limit count was specified, count is undefined and noCount is true.
 * When lstate == LIMIT_INITIAL, offset/count/noCount haven't been set yet.
 * ----------------
 */
typedef enum {
    LIMIT_INITIAL,    /* initial state for LIMIT node */
    LIMIT_RESCAN,     /* rescan after recomputing parameters */
    LIMIT_EMPTY,      /* there are no returnable rows */
    LIMIT_INWINDOW,   /* have returned a row in the window */
    LIMIT_SUBPLANEOF, /* at EOF of subplan (within window) */
    LIMIT_WINDOWEND,  /* stepped off end of window */
    LIMIT_WINDOWSTART /* stepped off beginning of window */
} LimitStateCond;

typedef struct LimitState {
    PlanState ps;            /* its first field is NodeTag */
    ExprState* limitOffset;  /* OFFSET parameter, or NULL if none */
    ExprState* limitCount;   /* COUNT parameter, or NULL if none */
    int64 offset;            /* current OFFSET value */
    int64 count;             /* current COUNT, if any */
    bool noCount;            /* if true, ignore count */
    LimitStateCond lstate;   /* state machine status, as above */
    int64 position;          /* 1-based index of last tuple returned */
    TupleTableSlot* subSlot; /* tuple last obtained from subplan */
} LimitState;

/*
 * Target	: data partition
 * Brief	: structure definition about partition iteration
 */
typedef struct PartIteratorState {
    PlanState ps;   /* its first field is NodeTag */
    int currentItr; /* the sequence number for processing partition */
    int subPartCurrentItr; /* the sequence number for processing partition */
} PartIteratorState;

struct VecLimitState : public LimitState {
    VectorBatch* subBatch;
};

/*
 * RownumState node: used for computing the pseudo-column ROWNUM
 */
typedef struct RownumState {
    ExprState  xprstate;
    PlanState* ps;   /* the value of ROWNUM depends on its parent PlanState */
} RownumState;

/* ----------------
 *		GroupingFuncExprState node
 *
 * The list of column numbers refers to the input tuples of the Agg node to
 * which the GroupingFunc belongs, and may contain 0 for references to columns
 * that are only present in grouping sets processed by different Agg nodes (and
 * which are therefore always considered "grouping" here).
 * ----------------
 */
typedef struct GroupingFuncExprState {
    ExprState xprstate;
    AggState* aggstate;
    List* clauses; /* integer list of column numbers */
} GroupingFuncExprState;

typedef struct GroupingIdExprState {
    ExprState xprstate;
    AggState* aggstate;
} GroupingIdExprState;

/*
 * used by CstoreInsert and DfsInsert in nodeModifyTable.h and vecmodifytable.cpp
 */
#define FLUSH_DATA(obj, type)                             \
    do {                                                  \
        ((type*)obj)->SetEndFlag();                       \
        ((type*)obj)->BatchInsert((VectorBatch*)NULL, 0); \
        ((type*)obj)->EndBatchInsert();                   \
        ((type*)obj)->Destroy();                          \
        delete (type*)obj;                                \
    } while (0)

/*
 * used by TsStoreInsert in nodeModifyTable.h and vecmodifytable.cpp
 */
#define FLUSH_DATA_TSDB(obj, type)                              \
    do{                                                     \
        ((type*)obj)->BatchInsert((VectorBatch*)NULL, 0);   \
        ((type*)obj)->end_batch_insert();           \
        ((type*)obj)->Destroy();                          \
    }while(0)
/*
 * record the first tuple time used by INSERT, UPDATE and DELETE in ExecModifyTable and ExecVecModifyTable
 */
#define record_first_time()                                     \
    do {                                                        \
        if (unlikely(is_first_modified)) {                      \
            INSTR_TIME_SET_CURRENT(node->first_tuple_modified); \
            is_first_modified = false;                          \
        }                                                       \
    } while (0)

extern TupleTableSlot* ExecMakeTupleSlot(Tuple tuple, TableScanDesc tableScan, TupleTableSlot* slot, TableAmType tableAm);

/*
 * When the global partition index is used for bitmap scanning,
 * checks whether the partition table needs to be
 * switched each time an tbmres is obtained.
 */
inline bool BitmapNodeNeedSwitchPartRel(BitmapHeapScanState* node)
{
    return tbm_is_global(node->tbm) && GPIScanCheckPartOid(node->gpi_scan, node->tbmres->partitionOid);
}

// DB4AI state node

struct AlgorithmAPI;
struct TrainModelState;

typedef struct ModelTuple {
    Datum               *values;    // attributes value
    bool                *isnull;    // whether an attribute is null
    Oid                 *typid;     // type of an attribute
    bool                *typbyval;  // attribute is passed by value or by reference
    int16               *typlen;    // the length of an attribute
    int                 ncolumns;   // number of attributes
} ModelTuple;

// returns the next data row, or nullptr when iteration has finished
typedef bool (*callback_ml_fetch)(void *callback_data, ModelTuple *tuple);

// restarts the iteration of the dataset without fetching any tuple at all
typedef void (*callback_ml_rescan)(void *callback_data);

typedef struct TrainModelState {
    ScanState           ss;     /* its first field is NodeTag */
    const TrainModel    *config;
    AlgorithmAPI        *algorithm;
    int                 finished;   // number of configurations still running
    // row data
    ModelTuple          tuple; // data as well as metadata
    bool                row_allocated;
    // utility functions
    callback_ml_fetch   fetch;
    callback_ml_rescan  rescan;
    void                *callback_data; // for direct ML, used by the fetch callback
} TrainModelState;
#endif /* EXECNODES_H */

