/* -------------------------------------------------------------------------
 *
 * plancache.h
 *	  Plan cache definitions.
 *
 * See plancache.c for comments.
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * Portions Copyright (c) 2021, openGauss Contributors
 * src/include/utils/plancache.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef PLANCACHE_H
#define PLANCACHE_H

#include "access/tupdesc.h"
#include "nodes/params.h"
#include "utils/atomic.h"
#ifdef PGXC
#include "pgxc/locator.h"
#include "nodes/plannodes.h"
#endif

#ifdef ENABLE_MOT
// forward declaration for MOT JitContext
namespace JitExec
{
    struct JitContext;
}
#endif

#define CACHEDPLANSOURCE_MAGIC 195726186
#define CACHEDPLAN_MAGIC 953717834

#ifdef ENABLE_MOT
/* different storage engine types that might be used by a query */
typedef enum {
    SE_TYPE_UNSPECIFIED = 0,    /* unspecified storage engine */
    SE_TYPE_MOT,                /* MOT storage engine */
    SE_TYPE_PAGE_BASED,         /* Page Based storage engine */
    SE_TYPE_MIXED               /* Mixed (MOT & Page Based) storage engines */
} StorageEngineType;
#endif

/* possible values for plan_cache_mode */
typedef enum {
	PLAN_CACHE_MODE_AUTO,
	PLAN_CACHE_MODE_FORCE_GENERIC_PLAN,
	PLAN_CACHE_MODE_FORCE_CUSTOM_PLAN
} PlanCacheMode;

typedef enum {
    GPC_PRIVATE,
    GPC_SHARED,
    GPC_CPLAN,  // generate cplan
    GPC_UNSHARED,   // not satisfied share condition, like stream plan
} GPCSourceKind;

typedef enum {
    GPC_SHARE_IN_LOCAL_SAVE_PLAN_LIST,
    GPC_SHARE_IN_LOCAL_UNGPC_PLAN_LIST,  // contains GPC_PRIVATE, GPC_UNSHARED and GPC_CPLAN plancache
    GPC_SHARE_IN_PREPARE_STATEMENT,
    GPC_SHARE_IN_SHARE_TABLE,
    GPC_SHARE_IN_SHARE_TABLE_INVALID_LIST,
} GPCSourceSharedLoc;

typedef enum {
	GPC_VALID,
    GPC_INVALID,
} GPCSourceSharedStatus;

typedef enum {
    SHARED_PLANCACHE = 0,
    UPSERT_UPDATE_QUERY,
#ifdef ENABLE_MOT
    PBE_OPT_AND_MOT_ENGINE,
#endif
    PARAM_EXPR,
    ONE_SHOT_PLAN,
    NO_BOUND_PARAM,
    TRANSACTION_STAT,
    CALLER_FORCE_GPLAN,
    CALLER_FORCE_CPLAN,
    CSTORE_TABLE,
    CHOOSE_BY_HINT,
    PBE_OPTIMIZATION,
    SETTING_FORCE_GPLAN,
    SETTING_FORCE_CPLAN,
    TRY_CPLAN,
    COST_PREFER_GPLAN,
    DEFAULT_CHOOSE,
    MAX_PLANCHOOSEREASON
} PlanChooseReason;

class GPCPlanStatus
{
public:
    GPCPlanStatus() {
        m_kind = GPC_PRIVATE;
        m_location = GPC_SHARE_IN_PREPARE_STATEMENT;
        m_status = GPC_VALID;
        m_refcount = 0;
    }

    inline void ShareInit()
    {
        m_kind = GPC_SHARED;
        m_location = GPC_SHARE_IN_PREPARE_STATEMENT;
        m_status = GPC_VALID;
        m_refcount = 0;
    }
    // NOTE: can not reset to GPC_PRIVATE
    inline void SetKind(GPCSourceKind kind)
    {
        Assert(kind != GPC_PRIVATE);
        pg_memory_barrier();
        m_kind = kind;
    }
    inline void SetLoc(GPCSourceSharedLoc location)
    {
        pg_memory_barrier();
        m_location = location;
    }

    inline void SetStatus(GPCSourceSharedStatus status)
    {
        pg_memory_barrier();
        m_status = status;
    }

    inline bool IsPrivatePlan()
    {
        pg_memory_barrier();
        return m_kind == GPC_PRIVATE;
    }

    inline bool IsSharePlan()
    {
        pg_memory_barrier();
        return m_kind == GPC_SHARED;
    }

    inline bool IsUnSharedPlan()
    {
        pg_memory_barrier();
        return m_kind == GPC_UNSHARED;
    }

    inline bool IsUnShareCplan()
    {
        pg_memory_barrier();
        return m_kind == GPC_CPLAN;
    }

    inline bool InSavePlanList(GPCSourceKind kind)
    {
        pg_memory_barrier();
        return m_kind == kind && m_location == GPC_SHARE_IN_LOCAL_SAVE_PLAN_LIST;
    }

    inline bool InUngpcPlanList()
    {
        pg_memory_barrier();
        return (m_kind != GPC_SHARED) && m_location == GPC_SHARE_IN_LOCAL_UNGPC_PLAN_LIST;
    }

    inline bool InPrepareStmt()
    {
        pg_memory_barrier();
        return m_location == GPC_SHARE_IN_PREPARE_STATEMENT;
    }

    inline bool InShareTable()
    {
        pg_memory_barrier();
        return m_kind == GPC_SHARED && (m_location == GPC_SHARE_IN_SHARE_TABLE || m_location == GPC_SHARE_IN_SHARE_TABLE_INVALID_LIST);
    }

    inline bool InShareTableInvalidList()
    {
        pg_memory_barrier();
        return (m_kind == GPC_SHARED && m_location == GPC_SHARE_IN_SHARE_TABLE_INVALID_LIST);
    }

    inline bool IsValid()
    {
        pg_memory_barrier();
        return m_status == GPC_VALID;
    }

    inline bool NeedDropSharedGPC()
    {
        Assert (m_kind == GPC_SHARED);
        pg_memory_barrier();
        return m_location == GPC_SHARE_IN_SHARE_TABLE && m_status == GPC_INVALID;
    }

    inline void AddRefcount()
    {
        pg_atomic_fetch_add_u32((volatile uint32*)&m_refcount, 1);
    }

    inline void SubRefCount()
    {
        pg_atomic_fetch_sub_u32((volatile uint32*)&m_refcount, 1);
    }

    /* this check function can only be used to change gpc table when has write lock */
    inline bool RefCountZero()
    {
        /* if is same as 0 then set 0 and return true, if is different then do nothing and return false */
        uint32 expect = 0;
        return pg_atomic_compare_exchange_u32((volatile uint32*)&m_refcount, &expect, 0);
    }

    inline int GetRefCount()
    {
        return m_refcount;
    }

private:
    volatile GPCSourceKind 	      m_kind;
    volatile GPCSourceSharedLoc    m_location;
    volatile GPCSourceSharedStatus m_status;
    volatile int    m_refcount;
};

typedef struct GPCSource
{
    GPCPlanStatus status;
    struct GPCKey*  key;   //remember when we generate the plan.
} GPCSource;

typedef struct SPISign
{
    uint32 spi_key;    /* hash value of PLpgSQL_function's fn_hashkey */
    Oid func_oid;
    uint32 spi_id;     /* spi idx to PLpgSQL_expr, same as plansource's spi_id */
    int plansource_id; /* plansource idx in spiplan's plancache_list */
} SPISign;

/*
 * CachedPlanSource (which might better have been called CachedQuery)
 * represents a SQL query that we expect to use multiple times.  It stores
 * the query source text, the raw parse tree, and the analyzed-and-rewritten
 * query tree, as well as adjunct data.  Cache invalidation can happen as a
 * result of DDL affecting objects used by the query.  In that case we discard
 * the analyzed-and-rewritten query tree, and rebuild it when next needed.
 *
 * An actual execution plan, represented by CachedPlan, is derived from the
 * CachedPlanSource when we need to execute the query.	The plan could be
 * either generic (usable with any set of plan parameters) or custom (for a
 * specific set of parameters).  plancache.c contains the logic that decides
 * which way to do it for any particular execution.  If we are using a generic
 * cached plan then it is meant to be re-used across multiple executions, so
 * callers must always treat CachedPlans as read-only.
 *
 * Once successfully built and "saved", CachedPlanSources typically live
 * for the life of the backend, although they can be dropped explicitly.
 * CachedPlans are reference-counted and go away automatically when the last
 * reference is dropped.  A CachedPlan can outlive the CachedPlanSource it
 * was created from.
 *
 * An "unsaved" CachedPlanSource can be used for generating plans, but it
 * lives in transient storage and will not be updated in response to sinval
 * events.
 *
 * CachedPlans made from saved CachedPlanSources are likewise in permanent
 * storage, so to avoid memory leaks, the reference-counted references to them
 * must be held in permanent data structures or ResourceOwners.  CachedPlans
 * made from unsaved CachedPlanSources are in children of the caller's
 * memory context, so references to them should not be longer-lived than
 * that context.  (Reference counting is somewhat pro forma in that case,
 * though it may be useful if the CachedPlan can be discarded early.)
 *
 * A CachedPlanSource has two associated memory contexts: one that holds the
 * struct itself, the query source text and the raw parse tree, and another
 * context that holds the rewritten query tree and associated data.  This
 * allows the query tree to be discarded easily when it is invalidated.
 *
 * Some callers wish to use the CachedPlan API even with one-shot queries
 * that have no reason to be saved at all.  We therefore support a "oneshot"
 * variant that does no data copying or invalidation checking.  In this case
 * there are no separate memory contexts: the CachedPlanSource struct and
 * all subsidiary data live in the caller's CurrentMemoryContext, and there
 * is no way to free memory short of clearing that entire context.  A oneshot
 * plan is always treated as unsaved.
 *
 * Note: the string referenced by commandTag is not subsidiary storage;
 * it is assumed to be a compile-time-constant string.	As with portals,
 * commandTag shall be NULL if and only if the original query string (before
 * rewriting) was an empty string.
 */
typedef struct CachedPlanSource {
    int magic;                   /* should equal CACHEDPLANSOURCE_MAGIC */
    Node* raw_parse_tree;        /* output of raw_parser() */
    const char* query_string;    /* source text of query */
    const char* commandTag;      /* command tag (a constant!), or NULL */
    Oid* param_types;            /* array of parameter type OIDs, or NULL */
	char* param_modes;
    int num_params;              /* length of param_types array */
    ParserSetupHook parserSetup; /* alternative parameter spec method */
    void* parserSetupArg;
    int cursor_options;    /* cursor options used for planning */
    Oid rewriteRoleId;     /* Role ID we did rewriting for */
    bool dependsOnRole;    /* is rewritten query specific to role? */
    bool fixed_result;     /* disallow change in result tupdesc? */
    TupleDesc resultDesc;  /* result type; NULL = doesn't return tuples */
    MemoryContext context; /* memory context holding all above */
    /* These fields describe the current analyzed-and-rewritten query tree: */
    List* query_list; /* list of Query nodes, or NIL if not valid */
    /*
     * Notice: be careful to use relationOids as it may contain non-table OID
     * in some scenarios, e.g. assignment of relationOids in fix_expr_common.
     */
    List* relationOids;                     /* contain OIDs of relations the queries depend on */
    List* invalItems;                       /* other dependencies, as PlanInvalItems */
    struct OverrideSearchPath* search_path; /* search_path used for
                                             * parsing and planning */
    MemoryContext query_context;            /* context holding the above, or NULL */
    /* If we have a generic plan, this is a reference-counted link to it: */
    struct CachedPlan* gplan; /* generic plan, or NULL if not valid */
    /* Some state flags: */
    bool is_complete; /* has CompleteCachedPlan been done? */
    bool is_saved;    /* has CachedPlanSource been "saved"? */
    bool is_valid;    /* is the query_list currently valid? */
    bool is_oneshot;  /* is it a "oneshot" plan? */
#ifdef ENABLE_MOT
    StorageEngineType storageEngineType;    /* which storage engine is used*/
    JitExec::JitContext* mot_jit_context;   /* MOT JIT context required for executing LLVM jitted code */
#endif
    int generation;   /* increments each time we create a plan */
    /* If CachedPlanSource has been saved, it is a member of a global list */
    struct CachedPlanSource* next_saved; /* list link, if so */
    /* State kept to help decide whether to use custom or generic plans: */
    double generic_cost;      /* cost of generic plan, or -1 if not known */
    double total_custom_cost; /* total cost of custom plans so far */
    int num_custom_plans;     /* number of plans included in total */

    GPCSource   gpc;            /* share plan cache */
    SPISign spi_signature;
    bool is_support_gplan;    /* true if generate gplan */
#ifdef PGXC
    char* stmt_name;          /* If set, this is a copy of prepared stmt name */
    bool stream_enabled;      /* true if the plan is made with stream operator enabled */
    struct CachedPlan* cplan; /* custom plan, or NULL if not valid */
    ExecNodes* single_exec_node;
    bool is_read_only;
    void* lightProxyObj; /* light cn object */

    void* opFusionObj; /* operator fusion object */
    bool is_checked_opfusion;

    bool gplan_is_fqs;      /* if gplan is a fqs plan, use generic plan if
                    so when enable_pbe_optimization is on */
    bool force_custom_plan; /* force to use custom plan */
    bool single_shard_stmt;  /* single shard stmt? */
#endif
} CachedPlanSource;

/*
 * CachedPlan represents an execution plan derived from a CachedPlanSource.
 * The reference count includes both the link from the parent CachedPlanSource
 * (if any), and any active plan executions, so the plan can be discarded
 * exactly when refcount goes to zero.	Both the struct itself and the
 * subsidiary data live in the context denoted by the context field.
 * This makes it easy to free a no-longer-needed cached plan.  (However,
 * if is_oneshot is true, the context does not belong solely to the CachedPlan
 * so no freeing is possible.)
 */
typedef struct CachedPlan {
    int magic;                /* should equal CACHEDPLAN_MAGIC */
    List* stmt_list;          /* list of statement nodes (PlannedStmts and
                               * bare utility statements) */
    bool is_saved;            /* is CachedPlan in a long-lived context? */
    bool is_valid;            /* is the stmt_list currently valid? */
    bool is_oneshot;          /* is it a "oneshot" plan? */

#ifdef ENABLE_MOT
    StorageEngineType storageEngineType;    /* which storage engine is used*/
    JitExec::JitContext* mot_jit_context;   /* MOT JIT context required for executing LLVM jitted code */
#endif

    bool dependsOnRole;       /* is plan specific to that role? */
    Oid planRoleId;           /* Role ID the plan was created for */
    TransactionId saved_xmin; /* if valid, replan when TransactionXmin
                               * changes from this value */
    int generation;           /* parent's generation number for this plan */
    int refcount;             /* count of live references to this struct */
    bool single_shard_stmt;  /* single shard stmt? */
    MemoryContext context;    /* context containing this CachedPlan */

    volatile int global_refcount;
    volatile bool is_share;       /* is it gpc share plan? */
    int dn_stmt_num;          /* datanode statment num */
    inline bool isShared()
    {
        pg_memory_barrier();
        return is_share;
    }
} CachedPlan;

extern void InitPlanCache(void);
extern void ResetPlanCache(void);

extern CachedPlanSource* CreateCachedPlan(Node* raw_parse_tree, const char* query_string,
#ifdef PGXC
    const char* stmt_name,
#endif
    const char* commandTag,
    bool enable_spi_gpc = false);
extern CachedPlanSource* CreateOneShotCachedPlan(
    Node* raw_parse_tree, const char* query_string, const char* commandTag);
extern void CompleteCachedPlan(CachedPlanSource* plansource, List* querytree_list, MemoryContext querytree_context,
    Oid* param_types, const char* paramModes, int num_params, ParserSetupHook parserSetup, void* parserSetupArg, int cursor_options,
    bool fixed_result, const char* stmt_name, ExecNodes* single_exec_node = NULL, bool is_read_only = false);

extern void SaveCachedPlan(CachedPlanSource* plansource);
extern void DropCachedPlan(CachedPlanSource* plansource);

extern void CachedPlanSetParentContext(CachedPlanSource* plansource, MemoryContext newcontext);

extern CachedPlanSource *CopyCachedPlan(CachedPlanSource *plansource, bool is_share);

extern bool CachedPlanIsValid(CachedPlanSource* plansource);

extern List* CachedPlanGetTargetList(CachedPlanSource* plansource);

extern CachedPlan* GetCachedPlan(CachedPlanSource* plansource, ParamListInfo boundParams, bool useResOwner);
extern void ReleaseCachedPlan(CachedPlan* plan, bool useResOwner);
extern void DropCachedPlanInternal(CachedPlanSource* plansource);
extern List* RevalidateCachedQuery(CachedPlanSource* plansource, bool has_lp = false);

extern void CheckRelDependency(CachedPlanSource *plansource, Oid relid);
extern void CheckInvalItemDependency(CachedPlanSource *plansource, int cacheid, uint32 hashvalue);
extern void ResetPlanCache(CachedPlanSource *plansource);

extern void PlanCacheRelCallback(Datum arg, Oid relid);
extern void PlanCacheFuncCallback(Datum arg, int cacheid, uint32 hashvalue);
extern void PlanCacheSysCallback(Datum arg, int cacheid, uint32 hashvalue);
extern bool IsStreamSupport();
extern void AcquirePlannerLocks(List* stmt_list, bool acquire);
extern void AcquireExecutorLocks(List* stmt_list, bool acquire);
extern bool CachedPlanAllowsSimpleValidityCheck(CachedPlanSource *plansource, CachedPlan *plan, ResourceOwner owner);
extern bool CachedPlanIsSimplyValid(CachedPlanSource *plansource, CachedPlan *plan, ResourceOwner owner);
#endif /* PLANCACHE_H */
