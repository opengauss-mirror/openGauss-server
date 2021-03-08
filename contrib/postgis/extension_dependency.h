/*
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * ---------------------------------------------------------------------------------------
 *
 * extension_dependency.h
 *        postgis extension head file
 *
 *
 *
 * IDENTIFICATION
 *        contrib/postgis/extension_dependency.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef EXTENSION_DEPENDENCY
#define EXTENSION_DEPENDENCY

#include <stdint.h>
#include <signal.h>
#include <ctype.h>
#include "postgres.h"
#include "access/htup.h"
#include "catalog/pg_type.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "mb/pg_wchar.h"
#include "nodes/memnodes.h"
#include "nodes/nodes.h"
#include "nodes/pg_list.h"
#include "nodes/parsenodes.h"
#include "utils/array.h"
#include "utils/errcodes.h"
#include "utils/geo_decls.h"
#include "utils/guc.h"
#include "utils/guc_tables.h"
#include "utils/hsearch.h"
#include "utils/palloc.h"
#include "utils/syscache.h"

typedef int Buffer;
typedef uint16 StrategyNumber;
typedef int16 AttrNumber;

typedef uint16 LocationIndex;
typedef uint32 BlockNumber;
#define RTLeftStrategyNumber 1
#define RTOverLeftStrategyNumber 2
#define RTOverlapStrategyNumber 3
#define RTOverRightStrategyNumber 4
#define RTRightStrategyNumber 5
#define RTSameStrategyNumber 6
#define RTContainsStrategyNumber 7    /* for @> */
#define RTContainedByStrategyNumber 8 /* for <@ */
#define RTOverBelowStrategyNumber 9
#define RTBelowStrategyNumber 10
#define RTAboveStrategyNumber 11
#define RTOverAboveStrategyNumber 12
#define RTOldContainsStrategyNumber 13    /* for old spelling of @> */
#define RTOldContainedByStrategyNumber 14 /* for old spelling of <@ */
#define RTKNNSearchStrategyNumber 15

#define WINDOW_SEEK_CURRENT 0
#define WINDOW_SEEK_HEAD 1
#define WINDOW_SEEK_TAIL 2

#define STATISTIC_NUM_SLOTS 5
#define STATISTIC_KIND_TBLSIZE 6
#define STARELKIND_CLASS 'c'
#define STARELKIND_PARTITION 'p'


/* spi.h */
#ifndef SPI_H
#define SPI_OK_CONNECT 1
#define SPI_OK_FINISH 2
#define SPI_OK_FETCH 3
#define SPI_OK_UTILITY 4
#define SPI_OK_SELECT 5
#define SPI_ERROR_NOOUTFUNC (-10)
#define SPI_ERROR_NOATTRIBUTE (-9)

extern THR_LOCAL PGDLLIMPORT uint32 SPI_processed;
extern THR_LOCAL int SPI_result;

typedef uint32 AclMode;
typedef uint16 OffsetNumber;
typedef struct GISTPageOpaqueData GISTPageOpaqueData;
typedef GISTPageOpaqueData* GISTPageOpaque;
typedef struct _SPI_plan* SPIPlanPtr;

typedef struct SPITupleTable {
    MemoryContext tuptabcxt; /* memory context of result table */
    uint32 alloced;          /* of alloced vals */
    uint32 free;             /* of free vals */
    TupleDesc tupdesc;       /* tuple descriptor */
    HeapTuple* vals;         /* tuples */
} SPITupleTable;

#endif

/* pg_proc.h */
typedef struct {
    NameData    proname;         /* procedure name */
    Oid         pronamespace;    /* OID of namespace containing this proc */
    Oid         proowner;        /* procedure owner */
    Oid         prolang;         /* OID of pg_language entry */
    float4      procost;         /* estimated execution cost */
    float4      prorows;         /* estimated # of rows out (if proretset) */
    Oid         provariadic;     /* element type of variadic array, or 0 */
    regproc     protransform;    /* transforms calls to it during planning */
    bool        proisagg;        /* is it an aggregate? */
    bool        proiswindow;     /* is it a window function? */
    bool        prosecdef;       /* security definer */
    bool        proleakproof;    /* is it a leak-proof function? */
    bool        proisstrict;     /* strict with respect to NULLs? */
    bool        proretset;       /* returns a set? */
    char        provolatile;     /* see PROVOLATILE_ categories below */
    int2        pronargs;        /* number of arguments */
    int2        pronargdefaults; /* number of arguments with defaults */
    Oid         prorettype;      /* OID of result type */

    oidvector    proargtypes;    /* parameter types (excludes OUT params) */

#ifdef CATALOG_VARLEN
    Oid         proallargtypes[1];        /* all param types (NULL if IN only) */
    char        proargmodes[1];           /* parameter modes (NULL if IN only) */
    text        proargnames[1];           /* parameter names (NULL if no names) */
    pg_node_tree proargdefaults;          /* list of expression trees for argument */
    text        prosrc;                   /* procedure source text */
    text        probin;                   /* secondary procedure info (can be NULL) */
    text        proconfig[1];             /* procedure-local GUC settings */
    aclitem     proacl[1];                /* access permissions */
    int2vector  prodefaultargpos;
    bool        fencedmode;
    bool        proshippable;    /* if provolatile is not 'i', proshippable will determine if the func can be shipped */
    bool        propackage;
    char        prokind;         /* see PROKIND_ categories below */
#endif
} FormData_pg_proc;
typedef FormData_pg_proc *Form_pg_proc;

/* funcapi.h */
#ifndef FUNCAPI_H
typedef enum TypeFuncClass {
    TYPEFUNC_SCALAR,    /* scalar result type */
    TYPEFUNC_COMPOSITE, /* determinable rowtype result */
    TYPEFUNC_RECORD,    /* indeterminate rowtype result */
    TYPEFUNC_OTHER      /* bogus type, eg pseudotype */
} TypeFuncClass;
#endif

/* nodes/execnodes.h */
typedef struct ExprContext ExprContext;
/* utils/tuplestore.h */
typedef struct Tuplestorestate Tuplestorestate;

/* nodes/execnodes.h */
#ifndef EXECNODES_H
typedef enum {
    ExprSingleResult,   /* expression does not return a set */
    ExprMultipleResult, /* this result is an element of a set */
    ExprEndResult       /* there are no more elements in the set */
} ExprDoneCond;

/* nodes/execnodes.h */
typedef enum {
    SFRM_ValuePerCall = 0x01,         /* one value returned per call */
    SFRM_Materialize = 0x02,          /* result set instantiated in Tuplestore */
    SFRM_Materialize_Random = 0x04,   /* Tuplestore needs randomAccess */
    SFRM_Materialize_Preferred = 0x08 /* caller prefers Tuplestore */
} SetFunctionReturnMode;

/* nodes/execnodes.h */
typedef struct ReturnSetInfo {
    NodeTag type;
    /* values set by caller */
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
#endif

typedef PageHeaderData* PageHeader;

/* funcapi.h */
#ifndef FUNCAPI_H
typedef struct AttInMetadata {
    /* full TupleDesc */
    TupleDesc tupdesc;
    /* array of attribute type input function finfo */
    FmgrInfo* attinfuncs;
    /* array of attribute type i/o parameter OIDs */
    Oid* attioparams;
    /* array of attribute typmod */
    int32* atttypmods;
} AttInMetadata;

/* funcapi.h */
typedef struct FuncCallContext {
    /*
     * Number of times we've been called before
     * call_cntr is initialized to 0 for you by SRF_FIRSTCALL_INIT(), and
     * incremented for you every time SRF_RETURN_NEXT() is called.
     */
    uint32 call_cntr;

    /*
     * OPTIONAL maximum number of calls
     *
     * max_calls is here for convenience only and setting it is optional. If
     * not set, you must provide alternative means to know when the function
     * is done.
     */
    uint32 max_calls;

    /*
     * OPTIONAL pointer to result slot
     *
     * This is obsolete and only present for backwards compatibility, viz,
     * user-defined SRFs that use the deprecated TupleDescGetSlot().
     */
    TupleTableSlot* slot;

    /*
     * OPTIONAL pointer to miscellaneous user-provided context information
     *
     * user_fctx is for use as a pointer to your own struct to retain
     * arbitrary context information between calls of your function.
     */
    void* user_fctx;

    /*
     * OPTIONAL pointer to struct containing attribute type input metadata
     *
     * attinmeta is for use when returning tuples (i.e. composite data types)
     * and is not used when returning base data types. It is only needed if
     * you intend to use BuildTupleFromCStrings() to create the return tuple.
     */
    AttInMetadata* attinmeta;

    /*
     * memory context used for structures that must live for multiple calls
     *
     * multi_call_memory_ctx is set by SRF_FIRSTCALL_INIT() for you, and used
     * by SRF_RETURN_DONE() for cleanup. It is the most appropriate memory
     * context for any memory that is to be reused across multiple calls of
     * the SRF.
     */
    MemoryContext multi_call_memory_ctx;

    /*
     * OPTIONAL pointer to struct containing tuple description
     *
     * tuple_desc is for use when returning tuples (i.e. composite data types)
     * and is only needed if you are going to build the tuples with
     * heap_form_tuple() rather than with BuildTupleFromCStrings(). Note that
     * the TupleDesc pointer stored here should usually have been run through
     * BlessTupleDesc() first.
     */
    TupleDesc tuple_desc;
} FuncCallContext;
#endif

/* windowapi.h */
typedef struct WindowAggState WindowAggState;
typedef struct WindowObjectData {
    NodeTag type;
    WindowAggState* winstate; /* parent WindowAggState */
    List* argstates;          /* ExprState trees for fn's arguments */
    void* localmem;           /* WinGetPartitionLocalMemory's chunk */
    int markptr;              /* tuplestore mark pointer for this fn */
    int readptr;              /* tuplestore read pointer for this fn */
    int64 markpos;            /* row that markptr is positioned on */
    int64 seekpos;            /* row that readptr is positioned on */
} WindowObjectData;

typedef Pointer Page;
typedef struct RelationData* Relation;
/* gist.h */
typedef struct GISTENTRY {
    Datum key;
    Relation rel;
    Page page;
    OffsetNumber offset;
    bool leafkey;
} GISTENTRY;

typedef XLogRecPtr GistNSN;

typedef struct GISTPageOpaqueData {
    GistNSN nsn;           /* this value must change on page split */
    BlockNumber rightlink; /* next page if any */
    uint16 flags;          /* see bit definitions above */
    uint16 gist_page_id;   /* for identification of GiST indexes */
} GISTPageOpaqueData;

typedef struct {
    int32 n; /* number of elements */
    GISTENTRY vector[FLEXIBLE_ARRAY_MEMBER];
} GistEntryVector;

typedef struct GIST_SPLITVEC {
    OffsetNumber* spl_left; /* array of entries that go left */
    int spl_nleft;          /* size of this array */
    Datum spl_ldatum;       /* Union of keys in spl_left */
    bool spl_ldatum_exists; /* true, if spl_ldatum already exists. */

    OffsetNumber* spl_right; /* array of entries that go right */
    int spl_nright;          /* size of the array */
    Datum spl_rdatum;        /* Union of keys in spl_right */
    bool spl_rdatum_exists;  /* true, if spl_rdatum already exists. */
} GIST_SPLITVEC;

#ifndef RELATION_H
/* nodes/relation.h */
typedef struct ItstDisKey {
    List* superset_keys; /* list of superset keys list, several members possible */
    List* matching_keys; /* list of exact matching keys,  */
} ItstDisKey;

typedef struct PlannerGlobal PlannerGlobal;

typedef struct PlannerInfo {
    NodeTag type;

    Query* parse; /* the Query being planned */

    PlannerGlobal* glob; /* global info for current planner run */

    Index query_level; /* 1 at the outermost Query */

    struct PlannerInfo* parent_root; /* NULL at outermost Query */

    /*
     * simple_rel_array holds pointers to "base rels" and "other rels" (see
     * comments for RelOptInfo for more info).    It is indexed by rangetable
     * index (so entry 0 is always wasted).  Entries can be NULL when an RTE
     * does not correspond to a base relation, such as a join RTE or an
     * unreferenced view RTE; or if the RelOptInfo hasn't been made yet.
     */
    struct RelOptInfo** simple_rel_array; /* All 1-rel RelOptInfos */
    int simple_rel_array_size;            /* allocated size of array */

    /*
     * List of changed var that mutated during cost-based rewrite optimization, the
     * element in the list is "struct RewriteVarMapping", for example:
     * inlist2join
     * pushjoin2union (will implemented)
     *
     */
    List* var_mappings;
    Relids var_mapping_rels; /* all the relations that related to inlist2join */

    /*
     * simple_rte_array is the same length as simple_rel_array and holds
     * pointers to the associated rangetable entries.  This lets us avoid
     * rt_fetch(), which can be a bit slow once large inheritance sets have
     * been expanded.
     */
    RangeTblEntry** simple_rte_array; /* rangetable as an array */

    /*
     * all_baserels is a Relids set of all base relids (but not "other"
     * relids) in the query; that is, the Relids identifier of the final join
     * we need to form.
     */
    Relids all_baserels;

    /*
     * join_rel_list is a list of all join-relation RelOptInfos we have
     * considered in this planning run.  For small problems we just scan the
     * list to do lookups, but when there are many join relations we build a
     * hash table for faster lookups.  The hash table is present and valid
     * when join_rel_hash is not NULL.    Note that we still maintain the list
     * even when using the hash table for lookups; this simplifies life for
     * GEQO
     */
    List* join_rel_list;        /* list of join-relation RelOptInfos */
    struct HTAB* join_rel_hash; /* optional hashtable for join relations */

    /*
     * When doing a dynamic-programming-style join search, join_rel_level[k]
     * is a list of all join-relation RelOptInfos of level k, and
     * join_cur_level is the current level.  New join-relation RelOptInfos are
     * automatically added to the join_rel_level[join_cur_level] list.
     * join_rel_level is NULL if not in use.
     */
    List** join_rel_level; /* lists of join-relation RelOptInfos */

    int join_cur_level; /* index of list being extended */

    List* init_plans; /* init SubPlans for query */

    List* cte_plan_ids; /* per-CTE-item list of subplan IDs */

    List* eq_classes; /* list of active EquivalenceClasses */

    List* canon_pathkeys; /* list of "canonical" PathKeys */

    /* list of RestrictInfos for mergejoinable outer join clauses
     * w/nonnullable var on left
     */
    List* left_join_clauses;

    /* list of RestrictInfos for mergejoinable outer join clauses
     * w/nonnullable var on right
     */
    List* right_join_clauses;

    /* list of RestrictInfos for mergejoinable full join clauses */
    List* full_join_clauses;

    List* join_info_list; /* list of SpecialJoinInfos */

    List* append_rel_list; /* list of AppendRelInfos */

    List* rowMarks; /* list of PlanRowMarks */

    List* placeholder_list; /* list of PlaceHolderInfos */

    /* desired pathkeys for query_planner(), and actual pathkeys afterwards */
    List* query_pathkeys;

    List* group_pathkeys;    /* groupClause pathkeys, if any */
    List* window_pathkeys;   /* pathkeys of bottom window, if any */
    List* distinct_pathkeys; /* distinctClause pathkeys, if any */
    List* sort_pathkeys;     /* sortClause pathkeys, if any */

    List* minmax_aggs; /* List of MinMaxAggInfos */

    List* initial_rels; /* RelOptInfos we are now trying to join */

    MemoryContext planner_cxt; /* context holding PlannerInfo */

    double total_table_pages; /* of pages in all tables of query */

    double tuple_fraction; /* tuple_fraction passed to query_planner */
    double limit_tuples;   /* limit_tuples passed to query_planner */

    /* true if parse->resultRelation is an inheritance child rel */
    bool hasInheritedTarget;
    bool hasJoinRTEs;            /* true if any RTEs are RTE_JOIN kind */
    bool hasHavingQual;          /* true if havingQual was non-null */
    bool hasPseudoConstantQuals; /* true if any RestrictInfo has * pseudoconstant = true */
    bool hasRecursion;           /* true if planning a recursive WITH item */

    /* Note: qualSecurityLevel is zero if there are no securityQuals */
    Index qualSecurityLevel; /* minimum security_level for quals */

#ifdef PGXC
    /* This field is used only when RemoteScan nodes are involved */
    int rs_alias_index; /* used to build the alias reference */

    /*
     * In Postgres-XC Coordinators are supposed to skip the handling of
     * row marks of type ROW_MARK_EXCLUSIVE & ROW_MARK_SHARE.
     * In order to do that we simply remove such type
     * of row marks from the list rowMarks. Instead they are saved
     * in xc_rowMarks list that is then handeled to add
     * FOR UPDATE/SHARE in the remote query
     */
    List* xc_rowMarks; /* list of PlanRowMarks of type ROW_MARK_EXCLUSIVE & ROW_MARK_SHARE */
#endif

    /* These fields are used only when hasRecursion is true: */
    int wt_param_id;                 /* PARAM_EXEC ID for the work table */
    struct Plan* non_recursive_plan; /* plan for non-recursive term */

    /* These fields are workspace for createplan.c */
    Relids curOuterRels;  /* outer rels above current node */
    List* curOuterParams; /* not-yet-assigned NestLoopParams */

    Index curIteratorParamIndex;
    bool isPartIteratorPlanning;
    int curItrs;
    List* subqueryRestrictInfo; /* Subquery RestrictInfo, which only be used in wondows agg */

    /* optional private data for join_search_hook, e.g., GEQO */
    void* join_search_private;

    /* Added post-release, will be in a saner place in 9.3: */
    List* plan_params; /* list of PlannerParamItems, see below */

    /* For count_distinct, save null info for group by clause */
    List* join_null_info;
    /* for GroupingFunc fixup in setrefs */
    AttrNumber* grouping_map;

    /* If current query level is correlated with upper level */
    bool is_correlated;

    /* data redistribution for DFS table.
     * dataDestRelIndex is index into the range table. This variable
     * will take effect on data redistribution state.
     * The effective value must be greater than 0.
     */
    Index dataDestRelIndex;

    /* interesting keys of current query level */
    ItstDisKey dis_keys;

    /*
     * indicate if the subquery planning root (PlannerInfo) is under or rooted from
     * recursive-cte planning.
     */
    bool is_under_recursive_cte;

    /*
     * indicate if the subquery planning root (PlannerInfo) is under recursive-cte's
     * recursive branch
     */
    bool is_under_recursive_tree;
    bool has_recursive_correlated_rte; /* true if any RTE correlated with recursive cte */
} PlannerInfo;
#endif

/* commands/vacuum.h */
typedef struct VacAttrStats VacAttrStats;
typedef struct VacAttrStats* VacAttrStatsP;

typedef Datum (*AnalyzeAttrFetchFunc)(VacAttrStatsP stats, int rownum, bool* isNull, Relation rel);

typedef void (*AnalyzeAttrComputeStatsFunc)(
    VacAttrStatsP stats, AnalyzeAttrFetchFunc fetchfunc, int samplerows, double totalrows, Relation rel);

/*
 * ATTRIBUTE_FIXED_PART_SIZE is the size of the fixed-layout,
 * guaranteed-not-null part of a pg_attribute row.    This is in fact as much
 * of the row as gets copied into tuple descriptors, so don't expect you
 * can access fields beyond attcollation except in a real tuple!
 */
#define ATTRIBUTE_FIXED_PART_SIZE (offsetof(FormData_pg_attribute, attkvtype) + sizeof(Oid))

typedef void (*AnalyzeAttrComputeStatsFunc)(
    VacAttrStatsP stats, AnalyzeAttrFetchFunc fetchfunc, int samplerows, double totalrows, Relation rel);

/* commands/vacuum.h */
typedef struct VacAttrStats {
    /*
     * These fields are set up by the main ANALYZE code before invoking the
     * type-specific typanalyze function.
     *
     * Note: do not assume that the data being analyzed has the same datatype
     * shown in attr, ie do not trust attr->atttypid, attlen, etc.    This is
     * because some index opclasses store a different type than the underlying
     * column/expression.  Instead use attrtypid, attrtypmod, and attrtype for
     * information about the datatype being fed to the typanalyze function.
     */
    unsigned int num_attrs;
    Form_pg_attribute* attrs;  /* copy of pg_attribute row for columns */
    Oid* attrtypid;            /* type of data being analyzed */
    int32* attrtypmod;         /* typmod of data being analyzed */
    Form_pg_type* attrtype;    /* copy of pg_type row for attrtypid */
    MemoryContext anl_context; /* where to save long-lived data */

    /*
     * These fields must be filled in by the typanalyze routine, unless it
     * returns FALSE.
     */
    AnalyzeAttrComputeStatsFunc compute_stats; /* function pointer */
    int minrows;                               /* Minimum # of rows wanted for stats */
    void* extra_data;                          /* for extra type-specific data */

    /*
     * These fields are to be filled in by the compute_stats routine. (They
     * are initialized to zero when the struct is created.)
     */
    bool stats_valid;
    float4 stanullfrac;   /* fraction of entries that are NULL */
    int4 stawidth;        /* average width of column values */
    float4 stadistinct;   /* # distinct values */
    float4 stadndistinct; /* # distinct value of dn1 */
    int2 stakind[STATISTIC_NUM_SLOTS];
    Oid staop[STATISTIC_NUM_SLOTS];
    int numnumbers[STATISTIC_NUM_SLOTS];
    float4* stanumbers[STATISTIC_NUM_SLOTS];
    int numvalues[STATISTIC_NUM_SLOTS];
    Datum* stavalues[STATISTIC_NUM_SLOTS];

    /*
     * These fields describe the stavalues[n] element types. They will be
     * initialized to match attrtypid, but a custom typanalyze function might
     * want to store an array of something other than the analyzed column's
     * elements. It should then overwrite these fields.
     */
    Oid statypid[STATISTIC_NUM_SLOTS];
    int2 statyplen[STATISTIC_NUM_SLOTS];
    bool statypbyval[STATISTIC_NUM_SLOTS];
    char statypalign[STATISTIC_NUM_SLOTS];

    /*
     * These fields are private to the main ANALYZE code and should not be
     * looked at by type-specific functions.
     */
    int tupattnum;   /* attribute number within tuples */
    HeapTuple* rows; /* access info for std fetch function */
    TupleDesc tupDesc;
    Datum* exprvals; /* access info for index fetch function */
    bool* exprnulls;
    int rowstride;
} VacAttrStats;

/* utils/selfuncs.h */
typedef struct VariableStatData {
    Node* var;            /* the Var or expression tree */
    RelOptInfo* rel;      /* Relation, or NULL if not identifiable */
    HeapTuple statsTuple; /* pg_statistic tuple, or NULL if none */
    /* NB: if statsTuple!=NULL, it must be freed when caller is done */
    void (*freefunc)(HeapTuple tuple); /* how to free statsTuple */
    Oid vartype;                       /* exposed type of expression */
    Oid atttype;                       /* type to pass to get_attstatsslot */
    int32 atttypmod;                   /* typmod to pass to get_attstatsslot */
    bool isunique;                     /* matches unique index or DISTINCT clause */
    bool enablePossion;                /* indentify we can use possion or not */
} VariableStatData;

typedef GISTPageOpaqueData* GISTPageOpaque;
/* this struct is private in nodeWindowAgg.c */
typedef struct WindowObjectData* WindowObject;

extern THR_LOCAL PGDLLIMPORT SPITupleTable* SPI_tuptable;

/* miscadmin.h */
extern THR_LOCAL PGDLLIMPORT char my_exec_path[];
extern THR_LOCAL PGDLLIMPORT volatile bool InterruptPending;

#define PageIsValid(page) PointerIsValid(page)
#define PointerIsValid(pointer) ((const void*)(pointer) != NULL)
typedef struct PortalData* Portal;
extern int SPI_exec(const char* src, long tcount);
extern int SPI_connect(CommandDest dest = DestSPI, void (*spiCallbackfn)(void*) = NULL, void* clientData = NULL);
extern int SPI_finish(void);
extern int SPI_execute(const char* src, bool read_only, long tcount);
extern char* SPI_getvalue(HeapTuple tuple, TupleDesc tupdesc, int fnumber);
extern int SPI_freeplan(SPIPlanPtr plan);
extern void SPI_freetuptable(SPITupleTable *tuptable);
extern void SPI_cursor_close(Portal portal);
extern int SPI_execute_plan(SPIPlanPtr plan, Datum *Values, const char *Nulls, bool read_only, long tcount);
extern SPIPlanPtr SPI_prepare(const char *src, int nargs, Oid *argtypes);
extern Datum SPI_getbinval(HeapTuple tuple, TupleDesc tupdesc, int fnumber, bool *isnull);
extern Portal SPI_cursor_open_with_args(const char* name, const char* src, int nargs, Oid* argtypes, Datum* Values,
										const char* Nulls, bool read_only, int cursorOptions);
extern void SPI_cursor_fetch(Portal portal, bool forward, long count);
extern void* SPI_repalloc(void* pointer, Size size);

extern char* text_to_cstring(const text* t);
extern text* cstring_to_text(const char* s);

extern HTAB* hash_create(const char* tabname, long nelem, HASHCTL* info, int flags);
extern void hash_destroy(HTAB* hashp);
extern void hash_remove(HTAB* hashp);
extern void hash_stats(const char* where, HTAB* hashp);
extern void* hash_search(HTAB* hashp, const void* keyPtr, HASHACTION action, bool* foundPtr);
extern uint32 get_hash_value(HTAB* hashp, const void* keyPtr);

extern MemoryContext MemoryContextCreate(
    NodeTag tag, Size size, MemoryContextMethods* methods, MemoryContext parent, const char* name);

extern void MemoryContextDelete(MemoryContext context);

extern void get_share_path(const char* my_exec_path, char* ret_path);

/* funcapi.h */
#define HeapTupleGetDatum(_tuple) PointerGetDatum((_tuple)->t_data)
#define TupleGetDatum(_slot, _tuple) PointerGetDatum((_tuple)->t_data)
extern TypeFuncClass get_func_result_type(Oid functionId, Oid* resultTypeId, TupleDesc* resultTupleDesc);

/* executor/executor.h */
extern Datum GetAttributeByName(HeapTupleHeader tuple, const char* attname, bool* isNull);

/* server/miscadmin.h */
#define CHECK_FOR_INTERRUPTS()   \
    do {                         \
        if (InterruptPending)    \
            ProcessInterrupts(); \
    } while (0)

extern void ProcessInterrupts(void);
extern int GetNumConfigOptions(void);

typedef void (*pqsigfunc)(int);
extern pqsigfunc pqsignal(int signo, pqsigfunc func);

typedef const Pg_finfo_record* (*PGFInfoFunction)(void);

extern Oid get_fn_expr_argtype(FmgrInfo* flinfo, int argnum);
extern int16 get_typlen(Oid typid);
extern bool get_typbyval(Oid typid);

extern Datum hash_any(register const unsigned char* k, register int keylen);

#define ARR_DIMS(a) ((int*)(((char*)(a)) + sizeof(ArrayType)))

extern int AggCheckCallContext(FunctionCallInfo fcinfo, MemoryContext* aggcontext);
extern Datum datumCopy(Datum value, bool typByVal, int typLen);

extern TypeFuncClass get_call_result_type(FunctionCallInfo fcinfo, Oid* resultTypeId, TupleDesc* resultTupleDesc);
extern TupleDesc BlessTupleDesc(TupleDesc tupdesc);
extern HeapTuple heap_form_tuple(TupleDesc tupleDescriptor, Datum* values, bool* isnull);

extern TypeFuncClass get_call_result_type(FunctionCallInfo fcinfo, Oid* resultTypeId, TupleDesc* resultTupleDesc);

extern int GetDatabaseEncoding(void);

extern Datum numeric_int4(PG_FUNCTION_ARGS);
extern Datum textout(PG_FUNCTION_ARGS);
extern int16 get_typlen(Oid typid);
extern bool get_typbyval(Oid typid);
extern void get_typlenbyval(Oid typid, int16* typlen, bool* typbyval);
extern void get_typlenbyvalalign(Oid typid, int16* typlen, bool* typbyval, char* typalign);

extern TupleDesc RelationNameGetTupleDesc(const char* relname);
extern AttInMetadata* TupleDescGetAttInMetadata(TupleDesc tupdesc);
extern HeapTuple BuildTupleFromCStrings(AttInMetadata* attinmeta, char** values);
extern void heap_freetuple(HeapTuple htup);
extern ArrayType* construct_array(Datum* elems, int nelems, Oid elmtype, int elmlen, bool elmbyval, char elmalign);
#define ARR_ELEMTYPE(a) ((a)->elemtype)

extern int32 pg_atoi(char* s, int size, int c);
extern void* SPI_palloc(Size size);
typedef bool (*GucStringCheckHook)(char** newval, void** extra, GucSource source);
typedef const char* (*GucShowHook)(void);

extern int SPI_fnumber(TupleDesc tupdesc, const char* fname);

extern ArrayBuildState* accumArrayResult(
    ArrayBuildState* astate, Datum dvalue, bool disnull, Oid element_type, MemoryContext rcontext);

extern Datum makeMdArrayResult(
    ArrayBuildState* astate, int ndims, int* dims, int* lbs, MemoryContext rcontext, bool release);

extern Datum regclassin(PG_FUNCTION_ARGS);

extern void examine_variable(PlannerInfo* root, Node* node, int varRelid, VariableStatData* vardata);

#define PointerIsValid(pointer) ((const void*)(pointer) != NULL)
#define HeapTupleIsValid(tuple) PointerIsValid(tuple)

#define ReleaseVariableStats(vardata)                    \
    do {                                                 \
        if (HeapTupleIsValid((vardata).statsTuple))      \
            (*(vardata).freefunc)((vardata).statsTuple); \
    } while (0)

extern FuncCallContext* init_MultiFuncCall(PG_FUNCTION_ARGS);
extern FuncCallContext* per_MultiFuncCall(PG_FUNCTION_ARGS);
extern void end_MultiFuncCall(PG_FUNCTION_ARGS, FuncCallContext* funcctx);

#define PG_WINDOW_OBJECT() ((WindowObject)fcinfo->context)

#define WindowObjectIsValid(winobj) ((winobj) != NULL && IsA(winobj, WindowObjectData))

extern void* WinGetPartitionLocalMemory(WindowObject winobj, Size sz);

extern int64 WinGetCurrentPosition(WindowObject winobj);
extern int64 WinGetPartitionRowCount(WindowObject winobj);

extern void WinSetMarkPosition(WindowObject winobj, int64 markpos);

extern bool WinRowsArePeers(WindowObject winobj, int64 pos1, int64 pos2);

extern Datum WinGetFuncArgInPartition(
    WindowObject winobj, int argno, int relpos, int seektype, bool set_mark, bool* isnull, bool* isout);

extern Datum WinGetFuncArgInFrame(
    WindowObject winobj, int argno, int relpos, int seektype, bool set_mark, bool* isnull, bool* isout);

extern Datum WinGetFuncArgCurrent(WindowObject winobj, int argno, bool* isnull);

/* funcapi.h */
#define SRF_IS_FIRSTCALL() (fcinfo->flinfo->fn_extra == NULL)
#define SRF_FIRSTCALL_INIT() init_MultiFuncCall(fcinfo)
#define SRF_PERCALL_SETUP() per_MultiFuncCall(fcinfo)
#define SRF_RETURN_NEXT(_funcctx, _result)        \
    do {                                          \
        ReturnSetInfo* rsi;                       \
        (_funcctx)->call_cntr++;                  \
        rsi = (ReturnSetInfo*)fcinfo->resultinfo; \
        rsi->isDone = ExprMultipleResult;         \
        PG_RETURN_DATUM(_result);                 \
    } while (0)
#define SRF_RETURN_DONE(_funcctx)                 \
    do {                                          \
        ReturnSetInfo* rsi;                       \
        end_MultiFuncCall(fcinfo, _funcctx);      \
        rsi = (ReturnSetInfo*)fcinfo->resultinfo; \
        rsi->isDone = ExprEndResult;              \
        PG_RETURN_NULL();                         \
    } while (0)

#define gistentryinit(e, k, r, pg, o, l) \
    do {                                 \
        (e).key = (k);                   \
        (e).rel = (r);                   \
        (e).page = (pg);                 \
        (e).offset = (o);                \
        (e).leafkey = (l);               \
    } while (0)

#define PageGetSpecialPointer(page) \
    (AssertMacro(PageIsValid(page)), (char*)((char*)(page) + ((PageHeader)(page))->pd_special))

#define GistPageGetOpaque(page) ((GISTPageOpaque)PageGetSpecialPointer(page))

#define F_LEAF (1 << 0) /* leaf page */
#define GistPageIsLeaf(page) (GistPageGetOpaque(page)->flags & F_LEAF)
#define GIST_LEAF(entry) (GistPageIsLeaf((entry)->page))

#define InvalidOffsetNumber ((OffsetNumber)0)
#define FirstOffsetNumber ((OffsetNumber)1)

#define OffsetNumberNext(offsetNumber) ((OffsetNumber)(1 + (offsetNumber)))

extern bool get_attstatsslot(HeapTuple statstuple, Oid atttype, int32 atttypmod, int reqkind, Oid reqop, Oid* actualop,
    Datum** values, int* nvalues, float4** numbers, int* nnumbers);

extern void free_attstatsslot(Oid atttype, Datum* values, int nvalues, float4* numbers, int nnumbers);

extern AttrNumber get_attnum(Oid relid, const char* attname);

#define rt_fetch(rangetable_index, rangetable) ((RangeTblEntry*)list_nth(rangetable, (rangetable_index)-1))

#define getrelid(rangeindex, rangetable) (rt_fetch(rangeindex, rangetable)->relid)

extern void vacuum_delay_point(void);

extern char* get_rel_name(Oid relid);

extern THR_LOCAL PGDLLIMPORT int default_statistics_target;

#define CStringGetTextDatum(s) PointerGetDatum(cstring_to_text(s))

#endif /* EXTENSION_DEPENDENCY */
