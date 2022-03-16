/* -------------------------------------------------------------------------
 *
 * ruleutils.c
 *	  Functions to convert stored expressions/querytrees back to
 *	  source text
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/ruleutils.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include <fcntl.h>

#ifndef WIN32
#include <sys/syscall.h>
#endif

#ifdef PGXC
#include "access/reloptions.h"
#endif /* PGXC */
#include "access/sysattr.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/pg_attrdef.h"
#include "catalog/pg_authid.h"
#ifdef PGXC
#include "catalog/pg_aggregate.h"
#endif /* PGXC */
#include "catalog/pg_collation.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_depend.h"
#include "catalog/pg_language.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_partition.h"
#include "catalog/pg_partition_fn.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_synonym.h"
#include "catalog/pg_trigger.h"
#include "catalog/pg_type.h"
#include "catalog/heap.h"
#include "catalog/gs_encrypted_proc.h"
#include "catalog/gs_encrypted_columns.h"
#include "catalog/gs_package.h"
#include "commands/comment.h"
#include "commands/defrem.h"
#include "commands/tablespace.h"
#include "commands/tablecmds.h"
#include "executor/spi.h"
#include "executor/spi_priv.h"
#include "funcapi.h"
#include "miscadmin.h"
#ifdef PGXC
#include "nodes/execnodes.h"
#include "catalog/pgxc_class.h"
#endif
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/tlist.h"
#include "parser/keywords.h"
#include "parser/parse_agg.h"
#include "parser/parse_func.h"
#include "parser/parse_hint.h"
#include "parser/parse_oper.h"
#include "parser/parse_type.h"
#include "parser/parser.h"
#include "parser/parsetree.h"
#include "parser/parse_expr.h"
#ifdef PGXC
#include "pgxc/pgxc.h"
#include "optimizer/pgxcplan.h"
#include "optimizer/pgxcship.h"
#endif
#include "rewrite/rewriteHandler.h"
#include "rewrite/rewriteManip.h"
#include "rewrite/rewriteSupport.h"
#include "utils/acl.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/plpgsql.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/syscache.h"
#include "utils/snapmgr.h"
#include "utils/typcache.h"
#include "utils/xml.h"
#include "vecexecutor/vecnodes.h"
#include "db4ai/gd.h"
#include "commands/sqladvisor.h"

/* ----------
 * Pretty formatting constants
 * ----------
 */

/* Indent counts */
#define PRETTYINDENT_STD 8
#define PRETTYINDENT_JOIN 13
#define PRETTYINDENT_JOIN_ON (PRETTYINDENT_JOIN - PRETTYINDENT_STD)
#define PRETTYINDENT_VAR 4

/* Pretty flags */
#define PRETTYFLAG_PAREN 1
#define PRETTYFLAG_INDENT 2

/* Default line length for pretty-print wrapping */
#define WRAP_COLUMN_DEFAULT 79

/* macro to test if pretty action needed */
#define PRETTY_PAREN(context) ((context)->prettyFlags & PRETTYFLAG_PAREN)
#define PRETTY_INDENT(context) ((context)->prettyFlags & PRETTYFLAG_INDENT)
#define MAXFLOATWIDTH 64
#define MAXDOUBLEWIDTH 128

#define atooid(x) ((Oid)strtoul((x), NULL, 10))

/* ----------
 * Local data types
 * ----------
 */

/* Context info needed for invoking a recursive querytree display routine */
typedef struct {
    StringInfo buf;           /* output buffer to append to */
    List* namespaces;         /* List of deparse_namespace nodes */
    List* windowClause;       /* Current query level's WINDOW clause */
    List* windowTList;        /* targetlist for resolving WINDOW clause */
    unsigned int prettyFlags; /* enabling of pretty-print functions */
    int wrapColumn;           /* max line length, or -1 for no limit */
    int indentLevel;          /* current indent level for prettyprint */
    bool varprefix;           /* TRUE to print prefixes on Vars */
#ifdef PGXC
    bool finalise_aggs;   /* should Datanode finalise the aggregates? */
    bool sortgroup_colno; /* instead of expression use resno for sortgrouprefs. */
    void* parser_arg;     /* dynamic variables */
#endif                    /* PGXC */
    bool qrw_phase;       /* for qrw phase, we support more deparse rule */
    bool viewdef;         /* just for dump viewdef */
    bool is_fqs;          /* just for fqs query */
    bool is_upsert_clause;  /* just for upsert clause */
} deparse_context;

/*
 * Each level of query context around a subtree needs a level of Var namespace.
 * A Var having varlevelsup=N refers to the N'th item (counting from 0) in
 * the current context's namespaces list.
 *
 * The rangetable is the list of actual RTEs from the query tree, and the
 * cte list is the list of actual CTEs.
 *
 * When deparsing plan trees, there is always just a single item in the
 * deparse_namespace list (since a plan tree never contains Vars with
 * varlevelsup > 0).  We store the PlanState node that is the immediate
 * parent of the expression to be deparsed, as well as a list of that
 * PlanState's ancestors.  In addition, we store its outer and inner subplan
 * state nodes, as well as their plan nodes' targetlists, and the indextlist
 * if the current PlanState is an IndexOnlyScanState.  (These fields could
 * be derived on-the-fly from the current PlanState, but it seems notationally
 * clearer to set them up as separate fields.)
 */
typedef struct {
    List* rtable; /* List of RangeTblEntry nodes */
    List* ctes;   /* List of CommonTableExpr nodes */
    /* Remaining fields are used only when deparsing a Plan tree: */
    PlanState* planstate;       /* immediate parent of current expression */
    List* ancestors;            /* ancestors of planstate */
    PlanState* outer_planstate; /* outer subplan state, or NULL if none */
    PlanState* inner_planstate; /* inner subplan state, or NULL if none */
    List* outer_tlist;          /* referent for OUTER_VAR Vars */
    List* inner_tlist;          /* referent for INNER_VAR Vars */
    List* index_tlist;          /* referent for INDEX_VAR Vars */
#ifdef PGXC
    bool remotequery; /* deparse context for remote query */
#endif
} deparse_namespace;

/* ----------
 * Global data
 * ----------
 */
static const char* query_getrulebyoid = "SELECT * FROM pg_catalog.pg_rewrite WHERE oid = $1";
static const char* query_getviewrule = "SELECT * FROM pg_catalog.pg_rewrite WHERE ev_class = $1 AND rulename = $2";

typedef struct tableInfo {
    int1 relcmpr;
    char relkind;
    char relpersistence;
    char parttype;
    bool relrowmovement;
    bool hasindex;
    bool hasPartialClusterKey;
    Oid tablespace;
    Oid spcid;
    char* reloptions;
    char* relname;
} tableInfo;

typedef struct SubpartitionInfo {
    bool issubpartition;
    char subparttype; /* subpartition type, 'r'/'l'/'h' */
    Oid subparentid;
    Oid subpartkeytype; /* the typeid of subpartkey */
    AttrNumber attnum; /* the attribute number of subpartkey in the relation */
    bool istypestring;
} SubpartitionInfo;

/* ----------
 * Local functions
 *
 * Most of these functions used to use fixed-size buffers to build their
 * results.  Now, they take an (already initialized) StringInfo object
 * as a parameter, and append their text output to its contents.
 * ----------
 */
static char* deparse_expression_pretty(
    Node* expr, List* dpcontext, bool forceprefix, bool showimplicit, int prettyFlags, int startIndent,
    bool no_alias = false);
extern char* pg_get_viewdef_worker(Oid viewoid, int prettyFlags, int wrapColumn);
extern char* pg_get_functiondef_worker(Oid funcid, int* headerlines);
static char* pg_get_triggerdef_worker(Oid trigid, bool pretty);
static void decompile_column_index_array(Datum column_index_array, Oid relId, StringInfo buf);
static char* pg_get_ruledef_worker(Oid ruleoid, int prettyFlags);
static char *pg_get_indexdef_worker(Oid indexrelid, int colno, const Oid *excludeOps, bool attrsOnly, bool showTblSpc,
    int prettyFlags, bool dumpSchemaOnly = false, bool showPartitionLocal = true, bool showSubpartitionLocal = true);
static void pg_get_indexdef_partitions(Oid indexrelid, Form_pg_index idxrec, bool showTblSpc, StringInfoData *buf,
    bool dumpSchemaOnly, bool showPartitionLocal, bool showSubpartitionLocal);
static char* pg_get_constraintdef_worker(Oid constraintId, bool fullCommand, int prettyFlags);
static text* pg_get_expr_worker(text* expr, Oid relid, const char* relname, int prettyFlags);
static int print_function_arguments(StringInfo buf, HeapTuple proctup, bool print_table_args, bool print_defaults);
static void print_function_ora_arguments(StringInfo buf, HeapTuple proctup);
static void print_function_rettype(StringInfo buf, HeapTuple proctup);
static void set_deparse_planstate(deparse_namespace* dpns, PlanState* ps);
#ifdef PGXC
static void set_deparse_plan(deparse_namespace* dpns, Plan* plan);
#endif
static void push_child_plan(deparse_namespace* dpns, PlanState* ps, deparse_namespace* save_dpns);
static void pop_child_plan(deparse_namespace* dpns, deparse_namespace* save_dpns);
static void push_ancestor_plan(deparse_namespace* dpns, ListCell* ancestor_cell, deparse_namespace* save_dpns);
static void pop_ancestor_plan(deparse_namespace* dpns, deparse_namespace* save_dpns);
static void make_ruledef(StringInfo buf, HeapTuple ruletup, TupleDesc rulettc, int prettyFlags);
static void make_viewdef(StringInfo buf, HeapTuple ruletup, TupleDesc rulettc, int prettyFlags, int wrapColumn);
static void get_query_def(Query* query, StringInfo buf, List* parentnamespace, TupleDesc resultDesc, int prettyFlags,
    int wrapColumn, int startIndent
#ifdef PGXC
    ,
    bool finalise_aggregates, bool sortgroup_colno, void* parserArg = NULL
#endif /* PGXC */
    ,
    bool qrw_phase = false, bool viewdef = false, bool is_fqs = false);
static void get_values_def(List* values_lists, deparse_context* context);
static void get_with_clause(Query* query, deparse_context* context);
static void get_select_query_def(Query* query, deparse_context* context, TupleDesc resultDesc);
static void get_insert_query_def(Query* query, deparse_context* context);
static void get_update_query_def(Query* query, deparse_context* context);
static void get_update_query_targetlist_def(
    Query* query, List* targetList, RangeTblEntry* rte, deparse_context* context);
static void get_delete_query_def(Query* query, deparse_context* context);
static void get_utility_query_def(Query* query, deparse_context* context);
static void get_basic_select_query(Query* query, deparse_context* context, TupleDesc resultDesc);
static void get_target_list(Query* query, List* targetList, deparse_context* context, TupleDesc resultDesc);
static void get_setop_query(Node* setOp, Query* query, deparse_context* context, TupleDesc resultDesc);
static Node* get_rule_sortgroupclause(Index ref, List* tlist, bool force_colno, deparse_context* context);
static void get_rule_groupingset(GroupingSet* gset, List* targetlist, deparse_context* context);
static void get_rule_orderby(List* orderList, List* targetList, bool force_colno, deparse_context* context);
static void get_rule_windowclause(Query* query, deparse_context* context);
static void get_rule_windowspec(WindowClause* wc, List* targetList, deparse_context* context);
static void get_rule_windowspec_listagg(WindowClause* wc, List* targetList, deparse_context* context);
static char* get_variable(
    Var* var, int levelsup, bool istoplevel, deparse_context* context, bool for_star = false, bool no_alias = false);
static RangeTblEntry* find_rte_by_refname(const char* refname, deparse_context* context);
static Node* find_param_referent(
    Param* param, deparse_context* context, deparse_namespace** dpns_p, ListCell** ancestor_cell_p);
static void get_parameter(Param* param, deparse_context* context);
static const char* get_simple_binary_op_name(OpExpr* expr);
static bool isSimpleNode(Node* node, Node* parentNode, int prettyFlags);
static void appendContextKeyword(
    deparse_context* context, const char* str, int indentBefore, int indentAfter, int indentPlus);
static void get_rule_expr(Node* node, deparse_context* context, bool showimplicit, bool no_alias = false);
static void get_rule_expr_funccall(Node* node, deparse_context* context, bool showimplicit);
static bool looks_like_function(Node* node);
static void get_oper_expr(OpExpr* expr, deparse_context* context, bool no_alias = false);
static void get_func_expr(FuncExpr* expr, deparse_context* context, bool showimplicit);
static void get_agg_expr(Aggref* aggref, deparse_context* context);
static void get_windowfunc_expr(WindowFunc* wfunc, deparse_context* context);
static void get_coercion_expr(
    Node* arg, deparse_context* context, Oid resulttype, int32 resulttypmod, Node* parentNode);
static void get_const_expr(Const* constval, deparse_context* context, int showtype);
static void get_const_collation(Const* constval, deparse_context* context);
static void simple_quote_literal(StringInfo buf, const char* val);
static void get_sublink_expr(SubLink* sublink, deparse_context* context);
static void get_from_clause(Query* query, const char* prefix, deparse_context* context, List* fromlist = NIL);
static void get_from_clause_item(Node* jtnode, Query* query, deparse_context* context);
static void get_from_clause_partition(RangeTblEntry* rte, StringInfo buf, deparse_context* context);
static void get_from_clause_subpartition(RangeTblEntry* rte, StringInfo buf, deparse_context* context);
static void get_from_clause_bucket(RangeTblEntry* rte, StringInfo buf, deparse_context* context);
static void get_from_clause_alias(Alias* alias, RangeTblEntry* rte, deparse_context* context);
static void get_from_clause_coldeflist(
    List* names, List* types, List* typmods, List* collations, deparse_context* context);
static void get_tablesample_def(TableSampleClause* tablesample, deparse_context* context);
static void GetTimecapsuleDef(const TimeCapsuleClause* timeCapsule, deparse_context* context);
static void get_opclass_name(Oid opclass, Oid actual_datatype, StringInfo buf);
static Node* processIndirection(Node* node, deparse_context* context, bool printit);
static void printSubscripts(ArrayRef* aref, deparse_context* context);
static char* get_relation_name(Oid relid);
static char* generate_relation_name(Oid relid, List* namespaces);
static char* generate_function_name(
    Oid funcid, int nargs, List* argnames, Oid* argtypes, bool was_variadic, bool* use_variadic_p);
static char* generate_operator_name(Oid operid, Oid arg1, Oid arg2);
static text* string_to_text(char* str);
static char* flatten_reloptions(Oid relid);
static Oid SearchSysTable(const char* query);
static void replace_cl_types_in_argtypes(Oid func_id, int numargs, Oid* argtypes, bool *is_client_logic);

static void AppendSubPartitionByInfo(StringInfo buf, Oid tableoid, SubpartitionInfo *subpartinfo);
static void AppendSubPartitionDetail(StringInfo buf, tableInfo tableinfo, SubpartitionInfo *subpartinfo);
static void AppendRangeIntervalPartitionInfo(StringInfo buf, Oid tableoid, tableInfo tableinfo, int partkeynum,
    Oid *iPartboundary, SubpartitionInfo *subpartinfo);
static void AppendListPartitionInfo(StringInfo buf, Oid tableoid, tableInfo tableinfo, int partkeynum,
    Oid *iPartboundary, SubpartitionInfo *subpartinfo);
static void AppendHashPartitionInfo(StringInfo buf, Oid tableoid, tableInfo tableinfo, int partkeynum,
    Oid *iPartboundary, SubpartitionInfo *subpartinfo);
static void AppendTablespaceInfo(const char *spcname, StringInfo buf, tableInfo tableinfo);

/* from pgxcship */
Var* get_var_from_node(Node* node, bool (*func)(Oid) = func_oid_check_reject);

#define only_marker(rte) ((rte)->inh ? "" : "ONLY ")

/* ----------
 * get_ruledef			- Do it all and return a text
 *				  that could be used as a statement
 *				  to recreate the rule
 * ----------
 */
Datum pg_get_ruledef(PG_FUNCTION_ARGS)
{
    Oid ruleoid = PG_GETARG_OID(0);

    PG_RETURN_TEXT_P(string_to_text(pg_get_ruledef_worker(ruleoid, 0)));
}

Datum pg_get_ruledef_ext(PG_FUNCTION_ARGS)
{
    Oid ruleoid = PG_GETARG_OID(0);
    bool pretty = PG_GETARG_BOOL(1);
    int prettyFlags;

    prettyFlags = pretty ? (PRETTYFLAG_PAREN | PRETTYFLAG_INDENT) : 0;
    PG_RETURN_TEXT_P(string_to_text(pg_get_ruledef_worker(ruleoid, prettyFlags)));
}

static char* pg_get_ruledef_worker(Oid ruleoid, int prettyFlags)
{
    Datum args[1];
    char nulls[1];
    int spirc;
    HeapTuple ruletup;
    TupleDesc rulettc;
    StringInfoData buf;

    /*
     * Do this first so that string is alloc'd in outer context not SPI's.
     */
    initStringInfo(&buf);

    /*
     * Connect to SPI manager
     */
    SPI_STACK_LOG("connect", NULL, NULL);
    if (SPI_connect() != SPI_OK_CONNECT)
        ereport(ERROR, (errcode(ERRCODE_SPI_CONNECTION_FAILURE), errmsg("SPI_connect failed")));

    /*
     * On the first call prepare the plan to lookup pg_rewrite. We read
     * pg_rewrite over the SPI manager instead of using the syscache to be
     * checked for read access on pg_rewrite.
     */
    if (u_sess->cache_cxt.plan_getrulebyoid == NULL) {
        Oid argtypes[1];
        SPIPlanPtr plan;

        argtypes[0] = OIDOID;
        plan = SPI_prepare(query_getrulebyoid, 1, argtypes);
        if (plan == NULL)
            ereport(ERROR,
                (errcode(ERRCODE_SPI_PREPARE_FAILURE), errmsg("SPI_prepare failed for \"%s\"", query_getrulebyoid)));
        Assert (u_sess->SPI_cxt._current->spi_hash_key == INVALID_SPI_KEY);
        SPI_keepplan(plan);
        u_sess->cache_cxt.plan_getrulebyoid = plan;
    }

    /*
     * Get the pg_rewrite tuple for this rule
     */
    args[0] = ObjectIdGetDatum(ruleoid);
    nulls[0] = ' ';
    spirc = SPI_execute_plan(u_sess->cache_cxt.plan_getrulebyoid, args, nulls, true, 1);
    if (spirc != SPI_OK_SELECT)
        ereport(ERROR,
            (errcode(ERRCODE_SPI_EXECUTE_FAILURE), errmsg("failed to get pg_rewrite tuple for rule %u", ruleoid)));
    if (SPI_processed != 1)
        appendStringInfo(&buf, "-");
    else {
        /*
         * Get the rule's definition and put it into executor's memory
         */
        ruletup = SPI_tuptable->vals[0];
        rulettc = SPI_tuptable->tupdesc;
        make_ruledef(&buf, ruletup, rulettc, prettyFlags);
    }

    /*
     * Disconnect from SPI manager
     */
    SPI_STACK_LOG("finish", NULL, NULL);
    if (SPI_finish() != SPI_OK_FINISH)
        ereport(ERROR, (errcode(ERRCODE_SPI_FINISH_FAILURE), errmsg("SPI_finish failed")));
    return buf.data;
}

static bool has_schema_privileges_of_viewoid(Oid viewoid)
{
    Oid oid = GetNamespaceIdbyRelId(viewoid);
    if (!(u_sess->analyze_cxt.is_under_analyze || (IS_PGXC_DATANODE && IsConnFromCoord()))) {
        AclResult aclresult = pg_namespace_aclcheck(oid, GetUserId(), ACL_USAGE);
        return aclresult == ACLCHECK_OK;
    }
    return true;
}

/* ----------
 * get_viewdef			- Mainly the same thing, but we
 *				  only return the SELECT part of a view
 * ----------
 */
Datum pg_get_viewdef(PG_FUNCTION_ARGS)
{
    /* By OID */
    Oid viewoid = PG_GETARG_OID(0);
    if (!has_schema_privileges_of_viewoid(viewoid)) {
        PG_RETURN_NULL();
    }
    PG_RETURN_TEXT_P(string_to_text(pg_get_viewdef_worker(viewoid, 0, -1)));
}

Datum pg_get_viewdef_ext(PG_FUNCTION_ARGS)
{
    /* By OID */
    Oid viewoid = PG_GETARG_OID(0);
    if (!has_schema_privileges_of_viewoid(viewoid)) {
        PG_RETURN_NULL();
    }
    bool pretty = PG_GETARG_BOOL(1);
    int prettyFlags;

    prettyFlags = pretty ? (PRETTYFLAG_PAREN | PRETTYFLAG_INDENT) : 0;
    PG_RETURN_TEXT_P(string_to_text(pg_get_viewdef_worker(viewoid, prettyFlags, WRAP_COLUMN_DEFAULT)));
}

Datum pg_get_viewdef_wrap(PG_FUNCTION_ARGS)
{
    /* By OID */
    Oid viewoid = PG_GETARG_OID(0);
    if (!has_schema_privileges_of_viewoid(viewoid)) {
        PG_RETURN_NULL();
    }
    int wrap = PG_GETARG_INT32(1);
    int prettyFlags;
    char* result = NULL;

    /* calling this implies we want pretty printing */
    prettyFlags = PRETTYFLAG_PAREN | PRETTYFLAG_INDENT;
    result = pg_get_viewdef_worker(viewoid, prettyFlags, wrap);
    if (result == NULL) {
        PG_RETURN_NULL();
    }
    PG_RETURN_TEXT_P(string_to_text(result));
}

Datum pg_get_viewdef_name(PG_FUNCTION_ARGS)
{
    /* By qualified name */
    text* viewname = PG_GETARG_TEXT_P(0);
    RangeVar* viewrel = NULL;
    Oid viewoid;
    List* names = NIL;

    /* Look up view name.  Can't lock it - we might not have privileges. */
    names = textToQualifiedNameList(viewname);
    viewrel = makeRangeVarFromNameList(names);
    viewoid = RangeVarGetRelid(viewrel, NoLock, false);
    if (!has_schema_privileges_of_viewoid(viewoid)) {
        PG_RETURN_NULL();
    }

    list_free_ext(names);
    PG_RETURN_TEXT_P(string_to_text(pg_get_viewdef_worker(viewoid, 0, -1)));
}

Datum pg_get_viewdef_name_ext(PG_FUNCTION_ARGS)
{
    /* By qualified name */
    text* viewname = PG_GETARG_TEXT_P(0);
    bool pretty = PG_GETARG_BOOL(1);
    int prettyFlags;
    RangeVar* viewrel = NULL;
    Oid viewoid;
    List* names = NIL;

    prettyFlags = pretty ? (PRETTYFLAG_PAREN | PRETTYFLAG_INDENT) : 0;

    /* Look up view name.  Can't lock it - we might not have privileges. */
    names = textToQualifiedNameList(viewname);
    viewrel = makeRangeVarFromNameList(names);
    viewoid = RangeVarGetRelid(viewrel, NoLock, false);
    if (!has_schema_privileges_of_viewoid(viewoid)) {
        PG_RETURN_NULL();
    }

    list_free_ext(names);
    PG_RETURN_TEXT_P(string_to_text(pg_get_viewdef_worker(viewoid, prettyFlags, WRAP_COLUMN_DEFAULT)));
}

/*
 * Common code for by-OID and by-name variants of pg_get_viewdef
 */
char* pg_get_viewdef_worker(Oid viewoid, int prettyFlags, int wrapColumn)
{
    Datum args[2];
    char nulls[2];
    int spirc;
    HeapTuple ruletup;
    TupleDesc rulettc;
    StringInfoData buf;

    /*
     * Do this first so that string is alloc'd in outer context not SPI's.
     */
    initStringInfo(&buf);

    /*
     * Connect to SPI manager
     */
    SPI_STACK_LOG("connect", NULL, NULL);
    if (SPI_connect() != SPI_OK_CONNECT)
        ereport(ERROR, (errcode(ERRCODE_SPI_CONNECTION_FAILURE), errmsg("SPI_connect failed")));

    /*
     * On the first call prepare the plan to lookup pg_rewrite. We read
     * pg_rewrite over the SPI manager instead of using the syscache to be
     * checked for read access on pg_rewrite.
     */
    if (u_sess->cache_cxt.plan_getviewrule == NULL) {
        Oid argtypes[2];
        SPIPlanPtr plan;

        argtypes[0] = OIDOID;
        argtypes[1] = NAMEOID;
        plan = SPI_prepare(query_getviewrule, 2, argtypes);
        if (plan == NULL)
            ereport(ERROR,
                (errcode(ERRCODE_SPI_PREPARE_FAILURE), errmsg("SPI_prepare failed for \"%s\"", query_getviewrule)));
        Assert (u_sess->SPI_cxt._current->spi_hash_key == INVALID_SPI_KEY);
        SPI_keepplan(plan);
        u_sess->cache_cxt.plan_getviewrule = plan;
    }

    /*
     * Get the pg_rewrite tuple for the view's SELECT rule
     */
    args[0] = ObjectIdGetDatum(viewoid);
    args[1] = DirectFunctionCall1(namein, CStringGetDatum(ViewSelectRuleName));
    nulls[0] = ' ';
    nulls[1] = ' ';
    spirc = SPI_execute_plan(u_sess->cache_cxt.plan_getviewrule, args, nulls, true, 2);
    if (spirc != SPI_OK_SELECT)
        ereport(ERROR,
            (errcode(ERRCODE_SPI_EXECUTE_FAILURE), errmsg("failed to get pg_rewrite tuple for view %u", viewoid)));
    if (SPI_processed != 1)
        appendStringInfo(&buf, "Not a view");
    else {
        /*
         * Get the rule's definition and put it into executor's memory
         */
        ruletup = SPI_tuptable->vals[0];
        rulettc = SPI_tuptable->tupdesc;
        make_viewdef(&buf, ruletup, rulettc, prettyFlags, wrapColumn);
    }

    /*
     * Disconnect from SPI manager
     */
    SPI_STACK_LOG("finish", NULL, NULL);
    if (SPI_finish() != SPI_OK_FINISH)
        ereport(ERROR, (errcode(ERRCODE_SPI_FINISH_FAILURE), errmsg("SPI_finish failed")));

    return buf.data;
}

/*
 * @Description: if the type is a string type
 * @in typid - type oid
 * @return - return true if the type is a string type.
 */
static bool isTypeString(Oid typid)
{
    switch (typid) {
        case INT2OID:
        case INT4OID:
        case INT8OID:
        case FLOAT4OID:
        case FLOAT8OID:
        case NUMERICOID:
            /* Here we ignore infinity and NaN */
            return false;
        default:
            /* All other types are regarded as string. */
            return true;
    }
}

static int GetDistributeKeyCount(Oid tableoid)
{
    bool isnull;
    HeapTuple tuple;
    Datum datum;
    int2vector* pcvec = NULL;
    int count;

    tuple = SearchSysCache1(PGXCCLASSRELID, ObjectIdGetDatum(tableoid));
    if (!HeapTupleIsValid(tuple)) {
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("table oid doesn't exist in system catalog table.")));
    }

    datum = SysCacheGetAttr(PGXCCLASSRELID, tuple, Anum_pgxc_class_pcattnum, &isnull);
    if (isnull) {
        ReleaseSysCache(tuple);
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("table's distribution key is null.")));
    }
    
    pcvec = (int2vector*)DatumGetPointer(datum);
    count = pcvec->dim1;

    ReleaseSysCache(tuple);

    return count;
}

static char* GetSliceName(HeapTuple tuple, TupleDesc desc)
{
    int fieldNumber;
    char* fieldValue = NULL;

    fieldNumber = SPI_fnumber(desc, "slice");
    fieldValue = SPI_getvalue(tuple, desc, fieldNumber);

    return fieldValue;
}

static void AppendSliceName(StringInfo buf, HeapTuple tuple, TupleDesc desc)
{
    appendStringInfo(buf, "SLICE ");
    appendStringInfo(buf, "%s", GetSliceName(tuple, desc));
    return;
}

static char* GetSliceNodeName(HeapTuple tuple, TupleDesc desc)
{
    int fieldNumber;
    char* fieldValue = NULL;

    fieldNumber = SPI_fnumber(desc, "nodename");
    fieldValue = SPI_getvalue(tuple, desc, fieldNumber);

    return fieldValue;
}

static void AppendSliceNodeName(StringInfo buf, const char* nodeName)
{
    appendStringInfo(buf, ")");

    /* Optional: DATANODE dn_name */
    if (nodeName != NULL) {
        appendStringInfo(buf, " DATANODE ");
        appendStringInfo(buf, "%s", nodeName);
    }

    return;
}

static void AppendSliceBoundary(StringInfo buf, HeapTuple tuple, TupleDesc desc,
    Oid tableoid, const int* boundaryIdx, const int* keyAttrs, int keyNum, bool parentheses)
{
    Oid fieldType;
    char* fieldValue = NULL;

    if (parentheses) {
        appendStringInfo(buf, "(");
    }

    for (int i = 0; i < keyNum; i++) {
        if (i > 0) {
            appendStringInfo(buf, ", ");
        }

        fieldValue = SPI_getvalue(tuple, desc, boundaryIdx[i]);
        if (fieldValue == NULL) {
            appendStringInfo(buf, "MAXVALUE");
        } else {
            fieldType = get_atttype(tableoid, (AttrNumber)keyAttrs[i]);
            if (isTypeString(fieldType)) {
                appendStringInfo(buf, "'%s'", fieldValue);
            } else {
                appendStringInfo(buf, "%s", fieldValue);
            }
        }
    }

    if (parentheses) {
        appendStringInfo(buf, ")");
    }

    return;
}

static int* GetBoundaryFieldNumber(TupleDesc desc, int keyNum)
{
    int rc;
    char fieldName[NAMEDATALEN];
    int* boundaryIdx = (int*)palloc0(keyNum * sizeof(int));

    for (int i = 1; i <= keyNum; i++) {
        rc = snprintf_s(fieldName,
            NAMEDATALEN,
            NAMEDATALEN - 1,
            "distboundary_%d",
            i);
        securec_check_ss_c(rc, "\0", "\0");
        boundaryIdx[i - 1] = SPI_fnumber(desc, fieldName);
    }

    return boundaryIdx;
}

static int* GetDistributeKeyAttrNumber(Oid tableoid)
{
    int i;
    bool isnull;
    HeapTuple tuple;
    Datum datum;
    int2vector* pcvec = NULL;
    int* keyAttrNumber = NULL;

    tuple = SearchSysCache1(PGXCCLASSRELID, ObjectIdGetDatum(tableoid));
    if (!HeapTupleIsValid(tuple)) {
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("table oid doesn't exist in system catalog table.")));
    }

    datum = SysCacheGetAttr(PGXCCLASSRELID, tuple, Anum_pgxc_class_pcattnum, &isnull);
    pcvec = (int2vector*)DatumGetPointer(datum);

    keyAttrNumber = (int*)palloc0(pcvec->dim1 * sizeof(int));
    for (i = 0; i < pcvec->dim1; i++) {
        keyAttrNumber[i] = pcvec->values[i];
    }

    ReleaseSysCache(tuple);

    return keyAttrNumber;
}

static void GetRangeSliceDefs(StringInfo query, Oid tableoid, int keyNum)
{
    int i;

    resetStringInfo(query);
    appendStringInfo(query, "select ");
    for (i = 1; i <= keyNum; i++) {
        appendStringInfo(query, "p.boundaries[%d] AS distboundary_%d, ", i, i);
    }
    appendStringInfo(query,
        "p.relname as slice, case when p.specified = 't' then q.node_name else null end as nodename "
        "FROM pgxc_slice p JOIN pgxc_node q on p.nodeoid = q.oid "
        "WHERE p.relid = '%u' AND p.type = 's' ORDER BY p.sliceorder",
        tableoid);

    if (SPI_execute(query->data, true, INT_MAX) != SPI_OK_SELECT) {
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("fail to execute query")));
    }

    return;
}

static void GetRangeDistributionDef(StringInfo query, StringInfo buf, Oid tableoid)
{
    int i;
    int tuplenum;
    int keyNum;
    HeapTuple tuple;
    TupleDesc desc;
    int* boundaryIdx = NULL;
    int* keyAttrs = NULL;
    char* nodeName = NULL;

    keyNum = GetDistributeKeyCount(tableoid);
    keyAttrs = GetDistributeKeyAttrNumber(tableoid);

    /* Get Range Slice Definition */
    GetRangeSliceDefs(query, tableoid, keyNum);
    boundaryIdx = GetBoundaryFieldNumber(SPI_tuptable->tupdesc, keyNum);

    /* SLICE slice_name VALUES LESS THAN (literal[,..]) [DATANODE dn_name] */
    tuplenum = SPI_processed;
    appendStringInfo(buf, "\n(\n    ");
    for (i = 0; i < tuplenum; i++) {
        tuple = SPI_tuptable->vals[i];
        desc = SPI_tuptable->tupdesc;

        nodeName = GetSliceNodeName(tuple, desc);

        AppendSliceName(buf, tuple, desc);
        appendStringInfo(buf, " VALUES LESS THAN (");
        AppendSliceBoundary(buf, tuple, desc, tableoid, boundaryIdx, keyAttrs, keyNum, false);
        AppendSliceNodeName(buf, nodeName);

        if (i < tuplenum - 1) {
            appendStringInfo(buf, ",\n    ");
        }
    }
    appendStringInfo(buf, "\n)");

    pfree_ext(boundaryIdx);
    pfree_ext(keyAttrs);

    return;
}

static void GetListSliceDefs(StringInfo query, Oid tableoid, int keyNum)
{
    int i;
    resetStringInfo(query);
    appendStringInfo(query, "select ");
    
    for (i = 1; i <= keyNum; i++) {
        appendStringInfo(query, "p.boundaries[%d] AS distboundary_%d, ", i, i);
    }
    appendStringInfo(query,
        "p.relname as slice, p.sindex as sindex, "
        "case when p.specified = 't' then q.node_name else null end as nodename "
        "FROM pgxc_slice p JOIN pgxc_node q ON p.nodeoid = q.oid "
        "WHERE p.relid = '%u' AND p.type = 's' ORDER BY p.sliceorder, p.sindex",
        tableoid);

    if (SPI_execute(query->data, true, INT_MAX) != SPI_OK_SELECT) {
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("fail to execute query")));
    }

    return;
}

static int GetSlicesindex(HeapTuple tuple, TupleDesc desc)
{
    int result = 0;
    int fieldNumber;
    char* fieldValue = NULL;

    fieldNumber = SPI_fnumber(desc, "sindex");
    fieldValue = SPI_getvalue(tuple, desc, fieldNumber);

    if (fieldValue != NULL) {
        result = atoi(fieldValue);
    }

    return result;
}

static void GetListDistributionDef(StringInfo query, StringInfo buf, Oid tableoid)
{
    int i;
    int keyNum;
    int tuplenum;
    int sindex;
    bool parentheses;
    char* nodeName = NULL;
    int* boundaryIdx = NULL;
    int* keyAttrs = NULL;
    HeapTuple tuple;
    TupleDesc desc;

    keyNum = GetDistributeKeyCount(tableoid);
    keyAttrs = GetDistributeKeyAttrNumber(tableoid);
    parentheses = (keyNum > 1);

    /* Get List Slice Definition */
    GetListSliceDefs(query, tableoid, keyNum);
    boundaryIdx = GetBoundaryFieldNumber(SPI_tuptable->tupdesc, keyNum);

     /* SLICE slice_name VALUES (...) [DATANODE dn_name] */
    tuplenum = SPI_processed;
    appendStringInfo(buf, "\n(\n");
    for (i = 0; i < tuplenum; i++) {
        tuple = SPI_tuptable->vals[i];
        desc = SPI_tuptable->tupdesc;
        sindex = GetSlicesindex(tuple, desc);

        if (sindex == 0) {
            if (i != 0) {
                AppendSliceNodeName(buf, nodeName);
                appendStringInfo(buf, ",\n");
            }

            /* SLICE slice_name */
            appendStringInfo(buf, "    ");
            AppendSliceName(buf, tuple, desc);
            appendStringInfo(buf, " VALUES (");

            pfree_ext(nodeName);
            nodeName = pstrdup_ext(GetSliceNodeName(tuple, desc));
        } else {
            appendStringInfo(buf, ", ");
        }

        /* handle DEFAULT situation: only dump one DEFAULT */
        if (SPI_getvalue(tuple, desc, boundaryIdx[0]) == NULL) {
            appendStringInfo(buf, "DEFAULT");
        } else {
            AppendSliceBoundary(buf, tuple, desc, tableoid, boundaryIdx, keyAttrs, keyNum, parentheses);
        }
    }

    AppendSliceNodeName(buf, nodeName);
    appendStringInfo(buf, "\n)");

    pfree_ext(boundaryIdx);
    pfree_ext(keyAttrs);
    pfree_ext(nodeName);

    return;
}

/*
 * @Description: get partition table defination
 * @in query - append query for SPI_execute.
 * @in buf - append defination string for partition table.
 * @in tableoid - parent table oid.
 * @in tableinfo - parent table info.
 * @return - void
 */
static void get_table_partitiondef(StringInfo query, StringInfo buf, Oid tableoid, tableInfo tableinfo)
{
    bool isnull = false;
    Relation relation = NULL;
    ScanKeyData key[2];
    SysScanDesc scan = NULL;
    HeapTuple tuple = NULL;
    char relkind = RELKIND_RELATION; /* set default */
    char partstrategy = PART_STRATEGY_VALUE; /* set default */
    char parttype = PARTTYPE_NON_PARTITIONED_RELATION; /* set default */
    int partkeynum = 0;
    Oid* iPartboundary = NULL;
    Form_pg_partition partition = NULL;

    HeapTuple ctuple = SearchSysCache1(RELOID, ObjectIdGetDatum(tableoid));
    if (!HeapTupleIsValid(ctuple)) {
        ereport(ERROR,
            (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                errmsg("cache lookup failed for relid %u", tableoid)));
    }
    Form_pg_class reltuple = (Form_pg_class)GETSTRUCT(ctuple);
    parttype = reltuple->parttype;
    ReleaseSysCache(ctuple);

    if (parttype == PARTTYPE_NON_PARTITIONED_RELATION) {
        return;
    }

    relation = heap_open(PartitionRelationId, AccessShareLock);

    ScanKeyInit(&key[0], Anum_pg_partition_parttype, BTEqualStrategyNumber, F_CHAREQ, CharGetDatum(relkind));
    ScanKeyInit(&key[1], Anum_pg_partition_parentid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(tableoid));

    scan = systable_beginscan(relation, PartitionParentOidIndexId, true, NULL, 2, key);
    if (HeapTupleIsValid(tuple = systable_getnext(scan))) {
        int2vector* partVec = NULL;
        Datum datum = SysCacheGetAttr(PARTRELID, tuple, Anum_pg_partition_partkey, &isnull);
        partition = (Form_pg_partition)GETSTRUCT(tuple);

        appendStringInfo(buf, "\n");

        if (tableinfo.relkind == RELKIND_FOREIGN_TABLE || tableinfo.relkind == RELKIND_STREAM) {
            appendStringInfo(buf, "PARTITION BY (");
        } else {
            partstrategy = partition->partstrategy;
            switch (partition->partstrategy) {
                case PART_STRATEGY_RANGE:
                case PART_STRATEGY_INTERVAL:
                    /* restructure range or interval partitioned table definition */
                    appendStringInfo(buf, "PARTITION BY RANGE (");
                    break;
                case PART_STRATEGY_LIST:
                    /* restructure list partitioned table definition */
                    appendStringInfo(buf, "PARTITION BY LIST (");
                    break;
                case PART_STRATEGY_HASH:
                    /* restructure hash partitioned table definition */
                    appendStringInfo(buf, "PARTITION BY HASH (");
                    break;
                case PART_STRATEGY_VALUE:
                    /* restructure value partitioned table definition */
                    appendStringInfo(buf, "PARTITION BY VALUES (");
                    break;
                default: /* PART_STRATEGY_INVALID */
                    ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg(
                        "unrecognized partition type %c for table %s", partition->partstrategy, tableinfo.relname)));
            }
        }

        if (isnull == false) {
            partVec = (int2vector*)DatumGetPointer(datum);
            partkeynum = partVec->dim1;
            iPartboundary = (Oid*)palloc0(partkeynum * sizeof(Oid));
            bool firstFlag = true;
            /* Build the partition list from the partVec stored in tuple. */
            for (int i = 0; i < partVec->dim1; i++) {
                char* attname = get_attname(tableoid, partVec->values[i]);
                iPartboundary[i] = get_atttype(tableoid, partVec->values[i]);
                if (!firstFlag) {
                    appendStringInfo(buf, ", ");
                }
                firstFlag = false;
                appendStringInfo(buf, "%s", quote_identifier(attname));
                pfree_ext(attname);
            }
        }
        appendStringInfo(buf, ")");
    }
    systable_endscan(scan);
    heap_close(relation, AccessShareLock);

    if (partstrategy == PART_STRATEGY_INTERVAL) {
        resetStringInfo(query);
        appendStringInfo(query,
            "SELECT p.interval[1] AS interval FROM pg_partition p "
            "WHERE p.parentid = %u AND p.parttype = '%c' AND p.partstrategy = '%c'",
            tableoid, PART_OBJ_TYPE_PARTED_TABLE, PART_STRATEGY_INTERVAL);
        (void)SPI_execute(query->data, true, INT_MAX);
        Assert(SPI_processed == 1);

        HeapTuple spiTuple = SPI_tuptable->vals[0];
        TupleDesc spiTupdesc = SPI_tuptable->tupdesc;
        char *ivalue = SPI_getvalue(spiTuple, spiTupdesc, SPI_fnumber(spiTupdesc, "interval"));
        appendStringInfo(buf, "\nINTERVAL ('%s')", ivalue);
    }

    SubpartitionInfo *subpartinfo = (SubpartitionInfo *)palloc0(sizeof(SubpartitionInfo));
    if (parttype == PARTTYPE_SUBPARTITIONED_RELATION) {
        AppendSubPartitionByInfo(buf, tableoid, subpartinfo);
    }

    if (partstrategy == PART_STRATEGY_RANGE || partstrategy == PART_STRATEGY_INTERVAL) {
        AppendRangeIntervalPartitionInfo(buf, tableoid, tableinfo, partkeynum, iPartboundary, subpartinfo);
    } else if (partstrategy == PART_STRATEGY_LIST) {
        AppendListPartitionInfo(buf, tableoid, tableinfo, partkeynum, iPartboundary, subpartinfo);
    } else if (partstrategy == PART_STRATEGY_HASH) {
        AppendHashPartitionInfo(buf, tableoid, tableinfo, partkeynum, iPartboundary, subpartinfo);
    } else { /* If partstrategy is 'value' or other type, no slice info */
        pfree_ext(iPartboundary);
        pfree_ext(subpartinfo);
        return;
    }

    if (tableinfo.relrowmovement) {
        appendStringInfo(buf, "\n%s", "ENABLE ROW MOVEMENT");
    }
    pfree_ext(iPartboundary);
    pfree_ext(subpartinfo);
}

static void AppendSubPartitionByInfo(StringInfo buf, Oid tableoid, SubpartitionInfo *subpartinfo)
{
    Relation partrel = NULL;
    ScanKeyData key[2];
    SysScanDesc scan = NULL;
    HeapTuple parttuple = NULL;
    ScanKeyData subkey[2];
    SysScanDesc subscan = NULL;
    HeapTuple subparttuple = NULL;
    bool isnull = false;

    partrel = heap_open(PartitionRelationId, AccessShareLock);
    ScanKeyInit(&key[0], Anum_pg_partition_parttype, BTEqualStrategyNumber, F_CHAREQ,
        CharGetDatum(PARTTYPE_PARTITIONED_RELATION));
    ScanKeyInit(&key[1], Anum_pg_partition_parentid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(tableoid));
    scan = systable_beginscan(partrel, PartitionParentOidIndexId, true, NULL, 2, key);
    parttuple = systable_getnext(scan);

    if (!HeapTupleIsValid(parttuple)) {
        systable_endscan(scan);
        heap_close(partrel, AccessShareLock);
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
            errmsg("could not find partition tuple for subpartition relation %u", tableoid)));
    }

    Datum datum = SysCacheGetAttr(PARTRELID, parttuple, Anum_pg_partition_partkey, &isnull);
    Assert(!isnull);
    int2vector *partVec = (int2vector *)DatumGetPointer(datum);
    int partkeynum = partVec->dim1;
    if (partkeynum != 1) {
        systable_endscan(scan);
        heap_close(partrel, AccessShareLock);
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("only support one partkey in subpartition table")));
    }
    char *attname = get_attname(tableoid, partVec->values[0]);
    Oid subparentid = HeapTupleGetOid(parttuple);

    ScanKeyInit(&subkey[0], Anum_pg_partition_parttype, BTEqualStrategyNumber, F_CHAREQ,
        CharGetDatum(PARTTYPE_SUBPARTITIONED_RELATION));
    ScanKeyInit(&subkey[1], Anum_pg_partition_parentid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(subparentid));
    subscan = systable_beginscan(partrel, PartitionParentOidIndexId, true, NULL, 2, subkey);
    subparttuple = systable_getnext(subscan);

    if (!HeapTupleIsValid(subparttuple)) {
        systable_endscan(scan);
        systable_endscan(subscan);
        heap_close(partrel, AccessShareLock);
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
            errmsg("could not find subpartition tuple for subpartition relation %u", tableoid)));
    }

    Form_pg_partition part = (Form_pg_partition)GETSTRUCT(subparttuple);
    switch (part->partstrategy) {
        case PART_STRATEGY_RANGE:
            appendStringInfo(buf, " SUBPARTITION BY RANGE (");
            break;
        case PART_STRATEGY_LIST:
            /* restructure list partitioned table definition */
            appendStringInfo(buf, " SUBPARTITION BY LIST (");
            break;
        case PART_STRATEGY_HASH:
            /* restructure hash partitioned table definition */
            appendStringInfo(buf, " SUBPARTITION BY HASH (");
            break;
        default:
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("unrecognized subpartition type %c", part->partstrategy)));
    }
    appendStringInfo(buf, "%s", quote_identifier(attname));
    appendStringInfo(buf, ")");
    pfree_ext(attname);

    subpartinfo->issubpartition = true;
    subpartinfo->attnum = partVec->values[0];
    subpartinfo->subparttype = part->partstrategy;
    subpartinfo->subpartkeytype = get_atttype(tableoid, subpartinfo->attnum);
    subpartinfo->istypestring = isTypeString(subpartinfo->subpartkeytype);

    systable_endscan(scan);
    systable_endscan(subscan);
    heap_close(partrel, AccessShareLock);
}

static void AppendSubPartitionDetail(StringInfo buf, tableInfo tableinfo, SubpartitionInfo *subpartinfo)
{
    appendStringInfo(buf, "\n    (");

    StringInfo query = makeStringInfo();
    appendStringInfo(query,
        "SELECT /*+ hashjoin(p t) */ p.relname AS partName, "
        "array_to_string(p.boundaries, ',') as partbound, "
        "array_to_string(p.boundaries, ''',''') as partboundstr, "
        "t.spcname AS reltblspc "
        "FROM pg_partition p LEFT JOIN pg_tablespace t "
        "ON p.reltablespace = t.oid "
        "WHERE p.parentid = %u AND p.parttype = '%c' AND p.partstrategy = '%c' "
        "ORDER BY p.boundaries[1]::%s ASC",
        subpartinfo->subparentid, PART_OBJ_TYPE_TABLE_SUB_PARTITION, subpartinfo->subparttype,
        get_typename(subpartinfo->subpartkeytype));

    (void)SPI_execute(query->data, true, INT_MAX);
    int proc = SPI_processed;
    SPITupleTable *spitup = SPI_tuptable;
    for (int i = 0; i < proc; i++) {
        if (i > 0) {
            appendStringInfo(buf, ",");
        }
        HeapTuple spi_tuple = spitup->vals[i];
        TupleDesc spi_tupdesc = spitup->tupdesc;
        char *pname = SPI_getvalue(spi_tuple, spi_tupdesc, SPI_fnumber(spi_tupdesc, "partname"));
        appendStringInfo(buf, "\n        SUBPARTITION %s", quote_identifier(pname));

        if (subpartinfo->subparttype == PART_STRATEGY_RANGE) {
            appendStringInfo(buf, " VALUES LESS THAN (");
            char *pvalue = SPI_getvalue(spi_tuple, spi_tupdesc, SPI_fnumber(spi_tupdesc, "partbound"));
            if (pvalue == NULL || strlen(pvalue) == 0) {
                appendStringInfo(buf, "MAXVALUE");
            } else if (subpartinfo->istypestring) {
                char *svalue = SPI_getvalue(spi_tuple, spi_tupdesc, SPI_fnumber(spi_tupdesc, "partboundstr"));
                appendStringInfo(buf, "'%s'", svalue);
                pfree_ext(svalue);
            } else {
                appendStringInfo(buf, "%s", pvalue);
            }
            appendStringInfo(buf, ")");
            pfree_ext(pvalue);
        } else if (subpartinfo->subparttype == PART_STRATEGY_LIST) {
            appendStringInfo(buf, " VALUES (");
            char *pvalue = SPI_getvalue(spi_tuple, spi_tupdesc, SPI_fnumber(spi_tupdesc, "partbound"));
            if (pvalue == NULL || strlen(pvalue) == 0) {
                appendStringInfo(buf, "DEFAULT");
            } else if (subpartinfo->istypestring) {
                char *svalue = SPI_getvalue(spi_tuple, spi_tupdesc, SPI_fnumber(spi_tupdesc, "partboundstr"));
                appendStringInfo(buf, "'%s'", svalue);
                pfree_ext(svalue);
            } else {
                appendStringInfo(buf, "%s", pvalue);
            }
            appendStringInfo(buf, ")");
            pfree_ext(pvalue);
        }

        /*
         * Append partition tablespace.
         * Skip it, if partition tablespace is the same as partitioned table.
         */
        int fno = SPI_fnumber(spi_tupdesc, "reltblspc");
        const char *spcname = SPI_getvalue(spi_tuple, spi_tupdesc, fno);
        AppendTablespaceInfo(spcname, buf, tableinfo);
    }
    DestroyStringInfo(query);

    appendStringInfo(buf, "\n    )");
}

static void AppendRangeIntervalPartitionInfo(StringInfo buf, Oid tableoid, tableInfo tableinfo, int partkeynum,
    Oid *iPartboundary, SubpartitionInfo *subpartinfo)
{
    appendStringInfo(buf, "\n( ");

    /* get table partitions info */
    StringInfo query = makeStringInfo();
    appendStringInfo(query, "SELECT /*+ hashjoin(p t) */p.relname AS partname, ");
    for (int i = 1; i <= partkeynum; i++) {
        appendStringInfo(query, "p.boundaries[%d] AS partboundary_%d, ", i, i);
    }
    appendStringInfo(query,
        "p.oid AS partoid, "
        "t.spcname AS reltblspc "
        "FROM pg_partition p LEFT JOIN pg_tablespace t "
        "ON p.reltablespace = t.oid "
        "WHERE p.parentid = %u AND p.parttype = '%c' "
        "AND p.partstrategy = '%c' ORDER BY ",
        tableoid, PART_OBJ_TYPE_TABLE_PARTITION, PART_STRATEGY_RANGE);
    for (int i = 1; i <= partkeynum; i++) {
        if (i == partkeynum) {
            appendStringInfo(query, "p.boundaries[%d]::%s ASC", i, get_typename(iPartboundary[i - 1]));
        } else {
            appendStringInfo(query, "p.boundaries[%d]::%s, ", i, get_typename(iPartboundary[i - 1]));
        }
    }

    (void)SPI_execute(query->data, true, INT_MAX);
    int proc = SPI_processed;
    SPITupleTable *spitup = SPI_tuptable;
    for (int i = 0; i < proc; i++) {
        if (i > 0) {
            appendStringInfo(buf, ",");
        }
        HeapTuple spi_tuple = spitup->vals[i];
        TupleDesc spi_tupdesc = spitup->tupdesc;
        char *pname = SPI_getvalue(spi_tuple, spi_tupdesc, SPI_fnumber(spi_tupdesc, "partname"));
        appendStringInfo(buf, "\n    PARTITION %s VALUES LESS THAN (", quote_identifier(pname));

        for (int j = 0; j < partkeynum; j++) {
            if (j > 0) {
                appendStringInfo(buf, ", ");
            }

            char *pvalue = NULL;
            char checkRowName[32] = {0};
            int rowNameLen = sizeof(checkRowName);
            int nRet = 0;
            nRet = snprintf_s(checkRowName, rowNameLen, rowNameLen - 1, "partboundary_%d", j + 1);
            securec_check_ss(nRet, "\0", "\0");

            pvalue = SPI_getvalue(spi_tuple, spi_tupdesc, SPI_fnumber(spi_tupdesc, checkRowName));
            if (pvalue == NULL) {
                appendStringInfo(buf, "MAXVALUE");
                continue;
            } else {
                if (isTypeString(iPartboundary[j])) {
                    appendStringInfo(buf, "'%s'", pvalue);
                } else {
                    appendStringInfo(buf, "%s", pvalue);
                }
            }
        }
        appendStringInfo(buf, ")");

        /*
         * Append partition tablespace.
         * Skip it, if partition tablespace is the same as partitioned table.
         */
        int fno = SPI_fnumber(spi_tupdesc, "reltblspc");
        const char *spcname = SPI_getvalue(spi_tuple, spi_tupdesc, fno);
        AppendTablespaceInfo(spcname, buf, tableinfo);

        if (subpartinfo->issubpartition) {
            subpartinfo->subparentid =
                atooid(SPI_getvalue(spi_tuple, spi_tupdesc, SPI_fnumber(spi_tupdesc, "partoid")));
            AppendSubPartitionDetail(buf, tableinfo, subpartinfo);
        }
    }
    DestroyStringInfo(query);

    appendStringInfo(buf, "\n)");
}
static void AppendListPartitionInfo(StringInfo buf, Oid tableoid, tableInfo tableinfo, int partkeynum,
    Oid *iPartboundary, SubpartitionInfo *subpartinfo)
{
    appendStringInfo(buf, "\n( ");

    /* we only support single partition key for list partition table */
    Assert(partkeynum == 1);

    /* get table partitions info */
    StringInfo query = makeStringInfo();
    appendStringInfo(query,
        "SELECT /*+ hashjoin(p t) */p.relname AS partname, "
        "array_to_string(p.boundaries, ',') as partbound, "
        "array_to_string(p.boundaries, ''',''') as partboundstr, "
        "p.oid AS partoid, "
        "t.spcname AS reltblspc "
        "FROM pg_partition p LEFT JOIN pg_tablespace t "
        "ON p.reltablespace = t.oid "
        "WHERE p.parentid = %u AND p.parttype = '%c' "
        "AND p.partstrategy = '%c' ORDER BY p.boundaries[1]::%s ASC",
        tableoid, PART_OBJ_TYPE_TABLE_PARTITION, PART_STRATEGY_LIST, get_typename(*iPartboundary));

    (void)SPI_execute(query->data, true, INT_MAX);
    int proc = SPI_processed;
    SPITupleTable *spitup = SPI_tuptable;
    for (int i = 0; i < proc; i++) {
        if (i > 0) {
            appendStringInfo(buf, ",");
        }
        HeapTuple spi_tuple = spitup->vals[i];
        TupleDesc spi_tupdesc = spitup->tupdesc;
        char *pname = SPI_getvalue(spi_tuple, spi_tupdesc, SPI_fnumber(spi_tupdesc, "partname"));
        appendStringInfo(buf, "\n    PARTITION %s VALUES (", quote_identifier(pname));

        char *pvalue = SPI_getvalue(spi_tuple, spi_tupdesc, SPI_fnumber(spi_tupdesc, "partbound"));
        if (pvalue == NULL || strlen(pvalue) == 0) {
            appendStringInfo(buf, "DEFAULT");
        } else if (isTypeString(*iPartboundary)) {
            char *svalue = SPI_getvalue(spi_tuple, spi_tupdesc, SPI_fnumber(spi_tupdesc, "partboundstr"));
            appendStringInfo(buf, "'%s'", svalue);
            pfree_ext(svalue);
        } else {
            appendStringInfo(buf, "%s", pvalue);
        }
        appendStringInfo(buf, ")");
        pfree_ext(pvalue);

        /*
         * Append partition tablespace.
         * Skip it, if partition tablespace is the same as partitioned table.
         */
        int fno = SPI_fnumber(spi_tupdesc, "reltblspc");
        const char *spcname = SPI_getvalue(spi_tuple, spi_tupdesc, fno);
        AppendTablespaceInfo(spcname, buf, tableinfo);

        if (subpartinfo->issubpartition) {
            subpartinfo->subparentid =
                atooid(SPI_getvalue(spi_tuple, spi_tupdesc, SPI_fnumber(spi_tupdesc, "partoid")));
            AppendSubPartitionDetail(buf, tableinfo, subpartinfo);
        }
    }
    DestroyStringInfo(query);

    appendStringInfo(buf, "\n)");
}

static void AppendHashPartitionInfo(StringInfo buf, Oid tableoid, tableInfo tableinfo, int partkeynum,
    Oid *iPartboundary, SubpartitionInfo *subpartinfo)
{
    appendStringInfo(buf, "\n( ");

    /* we only support single partition key for list partition table */
    Assert(partkeynum == 1);

    /* get table partitions info */
    StringInfo query = makeStringInfo();
    appendStringInfo(query,
        "SELECT /*+ hashjoin(p t) */p.relname AS partname, "
        "p.boundaries[1] AS partboundary, "
        "p.oid AS partoid, "
        "t.spcname AS reltblspc "
        "FROM pg_partition p LEFT JOIN pg_tablespace t "
        "ON p.reltablespace = t.oid "
        "WHERE p.parentid = %u AND p.parttype = '%c' "
        "AND p.partstrategy = '%c' ORDER BY ",
        tableoid, PART_OBJ_TYPE_TABLE_PARTITION, PART_STRATEGY_HASH);
    appendStringInfo(query, "p.boundaries[1]::%s ASC", get_typename(*iPartboundary));

    (void)SPI_execute(query->data, true, INT_MAX);
    int proc = SPI_processed;
    SPITupleTable *spitup = SPI_tuptable;
    for (int i = 0; i < proc; i++) {
        if (i > 0) {
            appendStringInfo(buf, ",");
        }
        HeapTuple spi_tuple = spitup->vals[i];
        TupleDesc spi_tupdesc = spitup->tupdesc;
        char *pname = SPI_getvalue(spi_tuple, spi_tupdesc, SPI_fnumber(spi_tupdesc, "partname"));
        appendStringInfo(buf, "\n    PARTITION %s", quote_identifier(pname));

        /*
         * Append partition tablespace.
         * Skip it, if partition tablespace is the same as partitioned table.
         */
        int fno = SPI_fnumber(spi_tupdesc, "reltblspc");
        const char *spcname = SPI_getvalue(spi_tuple, spi_tupdesc, fno);
        AppendTablespaceInfo(spcname, buf, tableinfo);

        if (subpartinfo->issubpartition) {
            subpartinfo->subparentid =
                atooid(SPI_getvalue(spi_tuple, spi_tupdesc, SPI_fnumber(spi_tupdesc, "partoid")));
            AppendSubPartitionDetail(buf, tableinfo, subpartinfo);
        }
    }
    DestroyStringInfo(query);

    appendStringInfo(buf, "\n)");
}

static void AppendTablespaceInfo(const char *spcname, StringInfo buf, tableInfo tableinfo)
{
    if (spcname != NULL) {
        appendStringInfo(buf, " TABLESPACE %s", quote_identifier(spcname));
    } else {
        appendStringInfo(buf, " TABLESPACE pg_default");
    }
}

/*
 * @Description: get collation's namespace oid by collation oid.
 * @in colloid - collation oid.
 * @return - namespace oid.
 */
static Oid get_collation_namespace(Oid colloid)
{
    HeapTuple tp = NULL;
    Oid result = InvalidOid;

    tp = SearchSysCache1(COLLOID, ObjectIdGetDatum(colloid));
    if (HeapTupleIsValid(tp)) {
        Form_pg_collation colltup = (Form_pg_collation)GETSTRUCT(tp);
        Oid space_oid = colltup->collnamespace;
        result = space_oid;
    }

    ReleaseSysCache(tp);
    return result;
}

static void get_compression_mode(Form_pg_attribute att_tup, StringInfo buf)
{
    switch (att_tup->attcmprmode) {
        case ATT_CMPR_DELTA:
            appendStringInfoString(buf, " DELTA");
            break;
        case ATT_CMPR_DICTIONARY:
            appendStringInfoString(buf, " DICTIONARY");
            break;
        case ATT_CMPR_PREFIX:
            appendStringInfoString(buf, " PREFIX");
            break;
        case ATT_CMPR_NUMSTR:
            appendStringInfoString(buf, " NUMSTR");
            break;
        default:
            // do nothing
            break;
    }
}

/*
 * @Description: get table's attribute infomartion.
 * @in tableoid - table oid.
 * @in buf - the string to append attribute info.
 * @in formatter -the formatter for foreign table.
 * @in ft_frmt_clmn -formatter columns.
 * @in cnt_ft_frmt_clmns - number of formatter columns.
 * @return - number of attribute.
 */
static int get_table_attribute(
    Oid tableoid, StringInfo buf, char* formatter, char** ft_frmt_clmn, int cnt_ft_frmt_clmns)
{
    int natts = get_relnatts(tableoid);
    int i;
    int actual_atts = 0;
    for (i = 0; i < natts; i++) {
        HeapTuple tp = SearchSysCache2(ATTNUM, ObjectIdGetDatum(tableoid), Int16GetDatum(i + 1));
        if (HeapTupleIsValid(tp)) {
            Form_pg_attribute att_tup = (Form_pg_attribute)GETSTRUCT(tp);
            if (att_tup->attkvtype == ATT_KV_HIDE) {
                ReleaseSysCache(tp);
                continue;
            }

            char* result = NULL;
            Datum txt;

            if (att_tup->attisdropped) {
                ReleaseSysCache(tp);
                continue;
            }

            txt = DirectFunctionCall2(
                format_type, ObjectIdGetDatum(att_tup->atttypid), ObjectIdGetDatum(att_tup->atttypmod));
            result = TextDatumGetCString(txt);
            result = format_type_with_typemod(att_tup->atttypid, att_tup->atttypmod);

            /* Format properly if not first attr */
            actual_atts == 0 ? appendStringInfo(buf, " (") : appendStringInfo(buf, ",");
            appendStringInfo(buf, "\n    ");
            actual_atts++;

            /* Attribute name */
            appendStringInfo(buf, "%s %s", quote_identifier(NameStr(att_tup->attname)), result);
            if (att_tup->attkvtype == ATT_KV_TAG)
                appendStringInfo(buf, " TSTag");
            else if (att_tup->attkvtype == ATT_KV_FIELD)
                appendStringInfo(buf, " TSField");
            else if (att_tup->attkvtype == ATT_KV_TIMETAG)
                appendStringInfo(buf, " TSTime");
            /* Compression mode */
            get_compression_mode(att_tup, buf);

            /* Add collation if not default for the type */
            if (OidIsValid(att_tup->attcollation)) {
                if (att_tup->attcollation != get_typcollation(att_tup->atttypid)) {
                    /* always schema-qualify, don't try to be smart */
                    char* collname = get_collation_name(att_tup->attcollation);
                    Oid namespace_oid = get_collation_namespace(att_tup->attcollation);
                    char* namespace_name = get_namespace_name(namespace_oid);
                    appendStringInfo(
                        buf, " COLLATE %s.%s", quote_identifier(namespace_name), quote_identifier(collname));
                    pfree_ext(collname);
                    pfree_ext(namespace_name);
                }
            }

            if (formatter != NULL) {
                int iter;
                char* relname = NameStr(att_tup->attname);
                for (iter = 0; iter < cnt_ft_frmt_clmns; iter++) {
                    if ((0 == strncmp(relname, ft_frmt_clmn[iter], strlen(relname))) &&
                        ('(' == ft_frmt_clmn[iter][strlen(relname)])) {
                        appendStringInfo(buf, " position%s", &ft_frmt_clmn[iter][strlen(relname)]);
                    }
                }
            }

            if (att_tup->atthasdef) {
                Form_pg_attrdef attrdef;
                Relation attrdefDesc;
                ScanKeyData skey[1];
                SysScanDesc adscan;
                HeapTuple tup = NULL;
                bool isnull = false;

                attrdefDesc = heap_open(AttrDefaultRelationId, AccessShareLock);

                ScanKeyInit(
                    &skey[0], Anum_pg_attrdef_adrelid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(tableoid));

                adscan = systable_beginscan(attrdefDesc, AttrDefaultIndexId, true, NULL, 1, skey);

                while (HeapTupleIsValid(tup = systable_getnext(adscan))) {
                    attrdef = (Form_pg_attrdef)GETSTRUCT(tup);
                    Datum val = fastgetattr(tup, Anum_pg_attrdef_adbin, attrdefDesc->rd_att, &isnull);

                    Datum txt = DirectFunctionCall2(pg_get_expr, val, ObjectIdGetDatum(tableoid));

                    if (attrdef->adnum == att_tup->attnum)
                        appendStringInfo(buf, " DEFAULT %s", TextDatumGetCString(txt));
                }

                systable_endscan(adscan);
                heap_close(attrdefDesc, AccessShareLock);
            }

            if (att_tup->attnotnull)
                appendStringInfo(buf, " NOT NULL");

            ReleaseSysCache(tp);
        }
    }

    return actual_atts;
}

bool IsHideTagDistribute(Oid relOid)
{
    Relation pcrel = NULL;
    ScanKeyData skey;
    SysScanDesc pcscan = NULL;
    HeapTuple htup = NULL;
    Form_pgxc_class pgxc_class;
    bool hide = false;

    ScanKeyInit(&skey, Anum_pgxc_class_pcrelid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(relOid));

    pcrel = heap_open(PgxcClassRelationId, AccessShareLock);
    pcscan = systable_beginscan(pcrel, PgxcClassPgxcRelIdIndexId, true, NULL, 1, &skey);
    htup = systable_getnext(pcscan);

    if (!HeapTupleIsValid(htup)) {
        systable_endscan(pcscan);
        heap_close(pcrel, AccessShareLock);
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_OBJECT),
                errmsg("could not open relation with OID %u", relOid),
                errdetail("Cannot open pgxcclass."),
                errcause("Cannot open pgxcclass."),
                erraction("Retry."),
                errmodule(MOD_OPT)));
    }

    pgxc_class = (Form_pgxc_class)GETSTRUCT(htup);

    if (pgxc_class->pcattnum.dim1 == 1 && get_kvtype(relOid, pgxc_class->pcattnum.values[0]) == ATT_KV_HIDE) {
        hide = true;
    }
    
    systable_endscan(pcscan);
    heap_close(pcrel, AccessShareLock);

    return hide;
}

/*
 * @Description: get table's distribute key.
 * @in relOid - table oid.
 * @return - distribute key.
 */
char* printDistributeKey(Oid relOid)
{
    Relation pcrel = NULL;
    ScanKeyData skey;
    SysScanDesc pcscan = NULL;
    HeapTuple htup = NULL;
    Form_pgxc_class pgxc_class;
    StringInfoData attname = {NULL, 0, 0, 0};
    initStringInfo(&attname);

    ScanKeyInit(&skey, Anum_pgxc_class_pcrelid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(relOid));

    pcrel = heap_open(PgxcClassRelationId, AccessShareLock);
    pcscan = systable_beginscan(pcrel, PgxcClassPgxcRelIdIndexId, true, NULL, 1, &skey);
    htup = systable_getnext(pcscan);

    if (!HeapTupleIsValid(htup)) {
        systable_endscan(pcscan);
        heap_close(pcrel, AccessShareLock);
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("could not open relation with OID %u", relOid)));
    }

    pgxc_class = (Form_pgxc_class)GETSTRUCT(htup);

    if (pgxc_class->pcattnum.values[0] == InvalidAttrNumber) {
        systable_endscan(pcscan);
        heap_close(pcrel, AccessShareLock);
        return NULL;
    }

    for (int i = 0; i < pgxc_class->pcattnum.dim1; i++) {
        if (i == 0) {
            appendStringInfoString(&attname, quote_identifier(get_attname(relOid, pgxc_class->pcattnum.values[i])));
        } else {
            appendStringInfoString(&attname, ", ");
            appendStringInfoString(&attname, quote_identifier(get_attname(relOid, pgxc_class->pcattnum.values[i])));
        }
    }

    systable_endscan(pcscan);
    heap_close(pcrel, AccessShareLock);
    return attname.data;
}

/*
 * @Description: get changing table's  info (alter table and comments).
 * @in tableoid - table oid.
 * @in buf - string to append changing info.
 * @in relname - table name.
 * @in relkind - relkind of the table.
 * @return - void
 */
static void get_changing_table_info(Oid tableoid, StringInfo buf, const char* relname, char relkind)
{
    char* comment = NULL;
    Relation pg_constraint = NULL;
    HeapTuple tuple = NULL;
    SysScanDesc scan;
    ScanKeyData skey[1];

    comment = GetComment(tableoid, RelationRelationId, 0);
    if (comment != NULL) {
        appendStringInfo(buf, "\nCOMMENT ON TABLE %s IS '%s';", relname, comment);
        pfree_ext(comment);
    }

    int natts = get_relnatts(tableoid);
    for (int i = 0; i < natts; i++) {
        HeapTuple tp;

        tp = SearchSysCache2(ATTNUM, ObjectIdGetDatum(tableoid), Int16GetDatum(i + 1));
        if (HeapTupleIsValid(tp)) {
            Form_pg_attribute att_tup = (Form_pg_attribute)GETSTRUCT(tp);
            const char* storage = NULL;

            if (att_tup->attisdropped) {
                ReleaseSysCache(tp);
                continue;
            }

            if (att_tup->attstattarget >= 0) {
                if (relkind == RELKIND_FOREIGN_TABLE) {
                    appendStringInfo(buf, "\nALTER FOREIGN TABLE %s ", relname);
                } else if (relkind == RELKIND_STREAM) {
                    appendStringInfo(buf, "\nALTER STREAM %s ", relname);
                } else {
                    appendStringInfo(buf, "\nALTER TABLE %s ", relname);
                }
                appendStringInfo(buf, "ALTER COLUMN %s ", quote_identifier(NameStr(att_tup->attname)));
                appendStringInfo(buf, "SET STATISTICS %d;", att_tup->attstattarget);
            }

            if (att_tup->attstattarget < -1) {
                int percent_value;
                percent_value = (att_tup->attstattarget + 1) * (-1);
                if (relkind == RELKIND_FOREIGN_TABLE) {
                    appendStringInfo(buf, "\nALTER FOREIGN TABLE %s ", relname);
                } else if (relkind == RELKIND_STREAM) {
                    appendStringInfo(buf, "\nALTER STREAM %s ", relname);
                } else {
                    appendStringInfo(buf, "\nALTER TABLE %s ", relname);
                }
                appendStringInfo(buf, "ALTER COLUMN %s ", quote_identifier(NameStr(att_tup->attname)));
                appendStringInfo(buf, "SET STATISTICS PERCENT %d;", percent_value);
            }

            if (get_typstorage(att_tup->atttypid) != att_tup->attstorage) {
                switch (att_tup->attstorage) {
                    case 'p':
                        storage = "PLAIN";
                        break;
                    case 'e':
                        storage = "EXTERNAL";
                        break;
                    case 'm':
                        storage = "MAIN";
                        break;
                    case 'x':
                        storage = "EXTENDED";
                        break;
                    default:
                        storage = NULL;
                        break;
                }

                if (storage != NULL) {
                    appendStringInfo(buf, "\nALTER TABLE %s ", relname);
                    appendStringInfo(buf, "ALTER COLUMN %s ", quote_identifier(NameStr(att_tup->attname)));
                    appendStringInfo(buf, "SET STORAGE %s;", storage);
                }
            }

            /*
             * Dump per-column attributes.
             */

            Datum datum;
            bool isNull = false;

            datum = SysCacheGetAttr(ATTNUM, tp, Anum_pg_attribute_attoptions, &isNull);
            if (!isNull) {
                Datum sep = CStringGetTextDatum(", ");
                Datum txt = OidFunctionCall2(F_ARRAY_TO_TEXT, datum, sep);
                char* attoptions = TextDatumGetCString(txt);

                if (attoptions != NULL && attoptions[0] != '\0') {
                    if (relkind == RELKIND_FOREIGN_TABLE) {
                        appendStringInfo(buf, "\nALTER FOREIGN TABLE %s ", relname);
                    } else if (relkind == RELKIND_STREAM) {
                        appendStringInfo(buf, "\nALTER STREAM %s ", relname);
                    } else {
                        appendStringInfo(buf, "\nALTER TABLE %s ", relname);
                    }
                    appendStringInfo(buf, "ALTER COLUMN %s ", quote_identifier(NameStr(att_tup->attname)));
                    appendStringInfo(buf, "SET (%s);", attoptions);

                    pfree_ext(attoptions);
                }
            }

            /*
             * Dump per-column fdw options.
             */
            datum = SysCacheGetAttr(ATTNUM, tp, Anum_pg_attribute_attfdwoptions, &isNull);
            if (!isNull && relkind == RELKIND_FOREIGN_TABLE) {
                Datum sep = CStringGetTextDatum(", ");
                Datum txt = OidFunctionCall2(F_ARRAY_TO_TEXT, datum, sep);
                char* attfdwoptions = TextDatumGetCString(txt);

                if (attfdwoptions != NULL && attfdwoptions[0] != '\0') {
                    appendStringInfo(buf, "\nALTER FOREIGN TABLE %s ", relname);
                    appendStringInfo(buf, "ALTER COLUMN %s ", quote_identifier(NameStr(att_tup->attname)));
                    appendStringInfo(buf, "OPTIONS (\n    %s\n);", attfdwoptions);
                }
            }

            if (!isNull && relkind == RELKIND_STREAM) {
                Datum sep = CStringGetTextDatum(", ");
                Datum txt = OidFunctionCall2(F_ARRAY_TO_TEXT, datum, sep);
                char* attfdwoptions = TextDatumGetCString(txt);

                if (attfdwoptions != NULL && attfdwoptions[0] != '\0') {
                    appendStringInfo(buf, "\nALTER STREAM %s ", relname);
                    appendStringInfo(buf, "ALTER COLUMN %s ", quote_identifier(NameStr(att_tup->attname)));
                    appendStringInfo(buf, "OPTIONS (\n    %s\n);", attfdwoptions);
                }
            }

            comment = GetComment(tableoid, RelationRelationId, i + 1);
            if (comment != NULL) {
                appendStringInfo(buf,
                    "\nCOMMENT ON COLUMN %s.%s IS '%s';",
                    relname,
                    quote_identifier(NameStr(att_tup->attname)),
                    comment);
                pfree_ext(comment);
            }
        }

        ReleaseSysCache(tp);
    }

    pg_constraint = heap_open(ConstraintRelationId, AccessShareLock);

    ScanKeyInit(&skey[0], Anum_pg_constraint_conrelid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(tableoid));

    scan = systable_beginscan(pg_constraint, ConstraintRelidIndexId, true, SnapshotNow, 1, skey);

    while (HeapTupleIsValid(tuple = systable_getnext(scan))) {
        Form_pg_constraint con = (Form_pg_constraint)GETSTRUCT(tuple);

        if (con->contype == 'c') {
            Oid conOid = HeapTupleGetOid(tuple);
            char* comment = GetComment(conOid, ConstraintRelationId, 0);
            if (comment != NULL) {
                appendStringInfo(buf,
                    "\nCOMMENT ON CONSTRAINT %s ON %s IS '%s';",
                    quote_identifier(NameStr(con->conname)),
                    relname,
                    comment);
                pfree_ext(comment);
            }
        }
    }

    systable_endscan(scan);
    heap_close(pg_constraint, AccessShareLock);
}

/*
 * @Description: get table's constraint info.
 * @in conForm - constraint info.
 * @in tuple - tuple for constraint.
 * @in buf - string to append comment info.
 * @in constraintId - constraint oid.
 * @in relname - table name.
 * @return - void
 */
static void get_table_constraint_info(
    Form_pg_constraint conForm, HeapTuple tuple, StringInfo buf, Oid constraintId, const char* relname)
{
    bool isnull = false;
    appendStringInfo(buf, "\nALTER TABLE %s ", relname);
    appendStringInfo(buf, "ADD CONSTRAINT %s ", quote_identifier(NameStr(conForm->conname)));

    /* Start off the constraint definition */
    if (conForm->contype == CONSTRAINT_PRIMARY)
        appendStringInfo(buf, "PRIMARY KEY (");
    else
        appendStringInfo(buf, "UNIQUE (");

    /* Fetch and build target column list */
    Datum val = SysCacheGetAttr(CONSTROID, tuple, Anum_pg_constraint_conkey, &isnull);
    if (isnull) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("null conkey for constraint %u", constraintId)));
    }

    decompile_column_index_array(val, conForm->conrelid, buf);

    appendStringInfo(buf, ")");

    Oid indexId = get_constraint_index(constraintId);

    /* XXX why do we only print these bits if fullCommand? */
    if (OidIsValid(indexId)) {
        char* options = flatten_reloptions(indexId);
        Oid tblspc;

        if (options != NULL) {
            appendStringInfo(buf, " WITH (%s)", options);
            pfree_ext(options);
        }

        tblspc = get_rel_tablespace(indexId);
        if (OidIsValid(tblspc)) {
            char* spcname = get_tablespace_name(tblspc);
            if (spcname != NULL) {
                appendStringInfo(buf, " USING INDEX TABLESPACE %s", quote_identifier(spcname));
            } else {
                ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("tablespace with OID %u does not exist", tblspc)));
            }
        }
    }

    if (conForm->condeferrable)
        appendStringInfo(buf, " DEFERRABLE");
    if (conForm->condeferred)
        appendStringInfo(buf, " INITIALLY DEFERRED");
    if (!conForm->convalidated)
        appendStringInfoString(buf, " NOT VALID");
}

/*
 * @Description: get foreign table's constraint info.
 * @in buf - string to append comment info.
 * @in tableoid - table oid.
 * @in relname - table name.
 * @return - void
 */
static void get_foreign_constraint_info(StringInfo buf, Oid tableoid, const char* relname)
{
    Relation pg_constraint = NULL;
    HeapTuple tuple = NULL;
    SysScanDesc scan = NULL;
    ScanKeyData skey[1];
    bool isnull = false;

    pg_constraint = heap_open(ConstraintRelationId, AccessShareLock);

    ScanKeyInit(&skey[0], Anum_pg_constraint_conrelid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(tableoid));

    scan = systable_beginscan(pg_constraint, ConstraintRelidIndexId, true, NULL, 1, skey);

    while (HeapTupleIsValid(tuple = systable_getnext(scan))) {
        Form_pg_constraint con = (Form_pg_constraint)GETSTRUCT(tuple);
        Oid conOid = HeapTupleGetOid(tuple);

        appendStringInfo(buf, "\nALTER FOREIGN TABLE %s ", relname);
        appendStringInfo(buf, "ADD CONSTRAINT %s", quote_identifier(NameStr(con->conname)));
        appendStringInfo(buf, " %s (", (con->contype == 'p') ? "PRIMARY KEY" : "UNIQUE");

        /* Fetch and build target column list */
        Datum val = SysCacheGetAttr(CONSTROID, tuple, Anum_pg_constraint_conkey, &isnull);
        if (isnull) {
            ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("null conkey for constraint %u", conOid)));
        }

        decompile_column_index_array(val, con->conrelid, buf);

        appendStringInfo(buf, ")");

        if (con->consoft) {
            appendStringInfo(buf, " NOT ENFORCED");
            if (con->conopt) {
                appendStringInfo(buf, " ENABLE QUERY OPTIMIZATION");
            } else {
                appendStringInfo(buf, " DISABLE QUERY OPTIMIZATION");
            }
        }

        appendStringInfo(buf, ";");
    }

    systable_endscan(scan);
    heap_close(pg_constraint, AccessShareLock);
}

/*
 * @Description: get table's index info.
 * @in tableoid - table oid.
 * @in buf - string to append comment info.
 * @in relname - table name.
 * @return - void
 */
static void get_index_list_info(Oid tableoid, StringInfo buf, const char* relname)
{
    Relation indrel = NULL;
    SysScanDesc indscan = NULL;
    ScanKeyData skey;
    HeapTuple htup = NULL;
    Oid constriantid = InvalidOid;

    /* Prepare to scan pg_index for entries having indrelid = this rel. */
    ScanKeyInit(&skey, Anum_pg_index_indrelid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(tableoid));

    indrel = heap_open(IndexRelationId, AccessShareLock);
    indscan = systable_beginscan(indrel, IndexIndrelidIndexId, true, NULL, 1, &skey);

    while (HeapTupleIsValid(htup = systable_getnext(indscan))) {
        Form_pg_index index = (Form_pg_index)GETSTRUCT(htup);
        /*
         * Ignore any indexes that are currently being dropped.  This will
         * prevent them from being searched, inserted into, or considered in
         * HOT-safety decisions.  It's unsafe to touch such an index at all
         * since its catalog entries could disappear at any instant.
         */
        if (!IndexIsLive(index))
            continue;

        constriantid = get_index_constraint(index->indexrelid);
        if (OidIsValid(constriantid)) {
            HeapTuple tup;
            Form_pg_constraint conForm;

            tup = SearchSysCache1(CONSTROID, ObjectIdGetDatum(constriantid));
            if (!HeapTupleIsValid(tup)) /* should not happen */
            {
                ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("cache lookup failed for constraint %u", constriantid)));
            }
            conForm = (Form_pg_constraint)GETSTRUCT(tup);

            if (conForm->contype == CONSTRAINT_UNIQUE || conForm->contype == CONSTRAINT_PRIMARY) {
                get_table_constraint_info(conForm, tup, buf, constriantid, relname);
                appendStringInfo(buf, ";");
            } else {
                char* index_def = pg_get_indexdef_worker(index->indexrelid, 0, NULL, false, true, 0);
                appendStringInfo(buf, "\n%s;", index_def);
            }
            /* Cleanup */
            ReleaseSysCache(tup);
        } else {
            char* index_def = pg_get_indexdef_worker(index->indexrelid, 0, NULL, false, true, 0);
            appendStringInfo(buf, "\n%s;", index_def);

            /* If the index is clustered, we need to record that. */
            if (index->indisclustered) {
                appendStringInfo(buf, "\nALTER TABLE %s CLUSTER", relname);
                appendStringInfo(buf, " ON %s;", quote_identifier(get_rel_name(index->indexrelid)));
            }
        }
        char* comment = GetComment(index->indexrelid, RelationRelationId, 0);
        if (comment != NULL) {
            appendStringInfo(
                buf, "\nCOMMENT ON INDEX %s IS '%s';", quote_identifier(get_rel_name(index->indexrelid)), comment);
            pfree_ext(comment);
        }
    }

    systable_endscan(indscan);
    heap_close(indrel, AccessShareLock);
}

/*
 * @Description: append table's info.
 * @in tableinfo - parent table info.
 * @in srvname - server name.
 * @in buf - string to append comment info.
 * @in tableoid - table oid.
 * @in isHDFSTbl - is a hdfs table
 * @in query - string for spi execute
 * @in ftoptions - options for foreign table
 * @in ft_write_only - write only
 * @return - is hdfs foreign table
 */
static bool append_table_info(tableInfo tableinfo, const char* srvname, StringInfo buf, Oid tableoid, bool isHDFSTbl,
    StringInfo query, char* ftoptions, bool ft_write_only)
{
    bool IsHDFSFTbl = false;
    int spirc;
    int proc;

    if (tableinfo.relkind == RELKIND_FOREIGN_TABLE || tableinfo.relkind == RELKIND_STREAM)
        appendStringInfo(buf, "\nSERVER %s", quote_identifier(srvname));

    if (tableinfo.reloptions && strlen(tableinfo.reloptions) > 0) {
        appendStringInfo(buf, "\nWITH (%s)", tableinfo.reloptions);
    }

    if (REL_CMPRS_FIELDS_EXTRACT == tableinfo.relcmpr) {
        appendStringInfo(buf, "\nCOMPRESS");
    }

    if (tableinfo.relkind != RELKIND_FOREIGN_TABLE || tableinfo.relkind != RELKIND_STREAM) {
        //* add tablespace info
        if (OidIsValid(tableinfo.tablespace)) {
            char* spcname = get_tablespace_name(tableinfo.tablespace);
            if (spcname != NULL) {
                appendStringInfo(buf, "\nTABLESPACE %s", quote_identifier(spcname));
            } else {
                ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_OBJECT),
                        errmsg("tablespace with OID %u does not exist", tableinfo.tablespace)));
            }
        }

        char locator_type = GetLocatorType(tableoid);
        if (IsLocatorColumnDistributed(locator_type) || IsLocatorReplicated(locator_type)) {
            appendStringInfo(buf, "\nDISTRIBUTE BY");

            if (IsHideTagDistribute(tableoid)) {
                appendStringInfo(buf, " hidetag");
            } else if (locator_type == LOCATOR_TYPE_HASH) {
                char* distribute_key = printDistributeKey(tableoid);
                appendStringInfo(buf, " HASH(%s)", distribute_key);
            } else if (locator_type == LOCATOR_TYPE_REPLICATED) {
                appendStringInfo(buf, " REPLICATION");
            } else if (locator_type == LOCATOR_TYPE_RROBIN) {
                appendStringInfo(buf, " ROUNDROBIN");
            } else if (locator_type == LOCATOR_TYPE_MODULO) {
                char* distribute_key = printDistributeKey(tableoid);
                appendStringInfo(buf, " MODULO (%s)", distribute_key);
            } else if (locator_type == LOCATOR_TYPE_RANGE) {
                char* distribute_key = printDistributeKey(tableoid);
                appendStringInfo(buf, " RANGE(%s)", distribute_key);
                GetRangeDistributionDef(query, buf, tableoid);
            } else {
                /* LOCATOR_TYPE_LIST */
                char* distribute_key = printDistributeKey(tableoid);
                appendStringInfo(buf, " LIST(%s)", distribute_key);
                GetListDistributionDef(query, buf, tableoid);
            }
        }

        if (!isHDFSTbl && IS_PGXC_COORDINATOR) {
            /*Adapt multi-nodegroup, local table creation statement TO NODE modified to TO GROUP*/
            Oid group_oid = get_pgxc_class_groupoid(tableoid);
            char* group_name = get_pgxc_groupname(group_oid, NULL);
            if (group_name != NULL) {
                appendStringInfo(buf, "\nTO GROUP %s", quote_identifier(group_name));
            } else {
                ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("computing nodegroup is not a valid group.")));
            }
            pfree_ext(group_name);
        }

        /*
         * Show information about partition table.
         * 1. Get the partition key postition and partition strategy from pg_partition.
         */
        get_table_partitiondef(query, buf, tableoid, tableinfo);
    } else {

        /*@hdfs
         * Judge whether the relation is a HDFS foreign table.
         */
        char* fdwnamestr = NULL;

        ForeignServer* Server = GetForeignServerByName(srvname, false);
        ForeignDataWrapper* wrapper = GetForeignDataWrapper(Server->fdwid);
        fdwnamestr = wrapper->fdwname;

        if (strncasecmp(fdwnamestr, HDFS_FDW, NAMEDATALEN) == 0 || strncasecmp(fdwnamestr, DFS_FDW, NAMEDATALEN) == 0 ||
            strncasecmp(fdwnamestr, "log_fdw", NAMEDATALEN) == 0) {
            IsHDFSFTbl = true;
        }
    }

    /* Dump generic options if any */
    if (ftoptions != NULL && ftoptions[0]) {
        /*Special handling required*/
        if ((NULL != strstr(ftoptions, "error_table")) || (NULL != strstr(ftoptions, "formatter")) ||
            (NULL != strstr(ftoptions, "log_remote")) || (NULL != strstr(ftoptions, "reject_limit"))) {
            int i_error_table;
            int i_log_remote;
            int i_reject_limit;
            int i_ftoptions;

            char* error_table = NULL;
            char* log_remote = NULL;
            char* reject_limit = NULL;
            char* ftoptionsUpdated = NULL;

            resetStringInfo(query);
            /* retrieve name of foreign server and generic options */
            appendStringInfo(query,
                "WITH ft_options AS "
                "(SELECT (pg_catalog.pg_options_to_table(ft.ftoptions)).option_name, "
                "(pg_catalog.pg_options_to_table(ft.ftoptions)).option_value "
                "FROM pg_catalog.pg_foreign_table ft "
                "WHERE ft.ftrelid = %u) "
                "SELECT "
                "( SELECT option_value FROM ft_options WHERE option_name = 'error_table' ) AS error_table,"
                "( SELECT option_value FROM ft_options WHERE option_name = 'log_remote' ) AS log_remote,"
                "( SELECT option_value FROM ft_options WHERE option_name = 'reject_limit' ) AS reject_limit,"
                "pg_catalog.array_to_string(ARRAY( "
                "SELECT pg_catalog.quote_ident(option_name) || ' ' || pg_catalog.quote_literal(option_value) "
                "FROM ft_options WHERE option_name <> 'error_table' AND option_name <> 'log_remote' "
                "AND option_name <> 'reject_limit' AND option_name <> 'formatter' ORDER BY option_name "
                "), E',\n	 ') AS ftoptions;",
                tableoid);

            spirc = SPI_execute(query->data, true, INT_MAX);
            proc = SPI_processed;

            i_ftoptions = SPI_fnumber(SPI_tuptable->tupdesc, "ftoptions");
            i_error_table = SPI_fnumber(SPI_tuptable->tupdesc, "error_table");
            i_log_remote = SPI_fnumber(SPI_tuptable->tupdesc, "log_remote");
            i_reject_limit = SPI_fnumber(SPI_tuptable->tupdesc, "reject_limit");

            ftoptionsUpdated = pstrdup(SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, i_ftoptions));

            char* tmp_value = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, i_error_table);
            if (tmp_value != NULL) {
                error_table = pstrdup(tmp_value);
            }

            tmp_value = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, i_log_remote);

            if (tmp_value != NULL) {
                log_remote = pstrdup(tmp_value);
            }

            tmp_value = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, i_reject_limit);

            if (tmp_value != NULL) {
                reject_limit = pstrdup(tmp_value);
            }

            appendStringInfo(buf, "\nOPTIONS (\n    %s\n)", ftoptionsUpdated);

            /* [ WRITE ONLY | READ ONLY ] */
            if (ft_write_only) {
                appendStringInfo(buf, " WRITE ONLY");
            }

            /* [ WITH error_table_name | LOG INTO error_table_name | LOG REMOTE ]  */
            if (NULL != error_table) {
                appendStringInfo(buf, " WITH %s", quote_identifier(error_table));
                pfree_ext(error_table);
            }

            if (NULL != log_remote) {
                appendStringInfo(buf, " REMOTE LOG '%s'", log_remote);
                pfree_ext(log_remote);
            }

            /* [ PER NODE REJECT LIMIT "'" m "'" ]	*/
            if (NULL != reject_limit) {
                appendStringInfo(buf, " PER NODE REJECT LIMIT '%s'", reject_limit);
                pfree_ext(reject_limit);
            }

            if (ftoptionsUpdated != NULL)
                pfree_ext(ftoptionsUpdated);
        } else {
            appendStringInfo(buf, "\nOPTIONS (\n    %s\n)", ftoptions);
            if (ft_write_only) {
                appendStringInfo(buf, " WRITE ONLY");
            }
        }
    }

    return IsHDFSFTbl;
}

/*
 * @Description: append table's alter table info.
 * @in tableinfo - parent table info.
 * @in has_not_valid_check - check is not valid
 * @in buf - string to append comment info.
 * @in tableoid - table oid.
 * @in relname -table name
 * @return - void
 */
static void get_table_alter_info(
    tableInfo tableinfo, bool has_not_valid_check, StringInfo buf, Oid tableoid, const char* relname)
{
    Relation pg_constraint = NULL;
    HeapTuple tuple = NULL;
    SysScanDesc scan = NULL;
    ScanKeyData skey[1];

    if (tableinfo.hasindex) {
        get_index_list_info(tableoid, buf, relname);
    }

    if (tableinfo.relkind == RELKIND_FOREIGN_TABLE || tableinfo.relkind == RELKIND_STREAM) {
        get_foreign_constraint_info(buf, tableoid, relname);
    }

    if (tableinfo.hasPartialClusterKey || has_not_valid_check) {
        /*
         * Fetch the constraint tuple from pg_constraint.  There may be more than
         * one match, because constraints are not required to have unique names;
         * if so, error out.
         */
        pg_constraint = heap_open(ConstraintRelationId, AccessShareLock);

        ScanKeyInit(&skey[0], Anum_pg_constraint_conrelid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(tableoid));

        scan = systable_beginscan(pg_constraint, ConstraintRelidIndexId, true, NULL, 1, skey);

        while (HeapTupleIsValid(tuple = systable_getnext(scan))) {
            Form_pg_constraint con = (Form_pg_constraint)GETSTRUCT(tuple);

            if (tableinfo.hasPartialClusterKey && con->contype == 's') {
                Oid conOid = HeapTupleGetOid(tuple);
                char* cluster_constraintdef =
                    pg_get_constraintdef_worker(conOid, false, PRETTYFLAG_PAREN | PRETTYFLAG_INDENT);
                if (cluster_constraintdef != NULL) {
                    if (tableinfo.relkind == RELKIND_FOREIGN_TABLE) {
                        appendStringInfo(buf, "\nALTER FOREIGN TABLE %s ", relname);
                    } else if (tableinfo.relkind == RELKIND_STREAM) {
                        appendStringInfo(buf, "\nALTER STREAM %s ", relname);
                    } else {
                        appendStringInfo(buf, "\nALTER TABLE %s ", relname);
                    }
                    appendStringInfo(buf, "ADD %s;", cluster_constraintdef);
                }
            } else if (has_not_valid_check && con->contype == 'c' && !con->convalidated) {
                Oid conOid = HeapTupleGetOid(tuple);
                char* cluster_constraintdef =
                    pg_get_constraintdef_worker(conOid, false, PRETTYFLAG_PAREN | PRETTYFLAG_INDENT);
                if (cluster_constraintdef != NULL) {
                    if (tableinfo.relkind == RELKIND_FOREIGN_TABLE) {
                        appendStringInfo(buf, "\nALTER FOREIGN TABLE %s ", relname);
                    } else if (tableinfo.relkind == RELKIND_STREAM) {
                        appendStringInfo(buf, "\nALTER STREAM %s ", relname);
                    } else {
                        appendStringInfo(buf, "\nALTER TABLE %s ", relname);
                    }
                    appendStringInfo(buf, "ADD CONSTRAINT %s ", quote_identifier(NameStr(con->conname)));
                    appendStringInfo(buf, "%s;", cluster_constraintdef);
                }
            }
        }

        systable_endscan(scan);
        heap_close(pg_constraint, AccessShareLock);
    }
}

static Oid SearchSysTable(const char* query)
{
    /*
     * Connect to SPI manager
     */
    SPI_STACK_LOG("connect", NULL, NULL);
    if (SPI_connect() != SPI_OK_CONNECT) {
        ereport(ERROR, (errcode(ERRCODE_SPI_CONNECTION_FAILURE), errmsg("SPI_connect failed")));
    }

    Oid oid = InvalidOid;
    int spirc = SPI_execute(query, true, INT_MAX);
    int proc = SPI_processed;
    if ((spirc == SPI_OK_SELECT) && (proc > 0)) {
        int fnumber = SPI_fnumber(SPI_tuptable->tupdesc, "oid");
        bool isnull = false;
        oid = (Oid)SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, fnumber, &isnull);
        if(isnull) {
            oid = InvalidOid;
        }
    }

    /*
     * Disconnect from SPI manager
     */
    SPI_STACK_LOG("finish", NULL, NULL);
    if (SPI_finish() != SPI_OK_FINISH) {
        ereport(ERROR, (errcode(ERRCODE_SPI_FINISH_FAILURE), errmsg("SPI_finish failed")));
    }

    return oid;
}


static inline bool IsTableVisible(Oid tableoid)
{
    StringInfoData query;
    initStringInfo(&query);
    appendStringInfo(&query, "select oid from %s where oid = %u", "pg_class", tableoid);
    Oid oid = SearchSysTable(query.data);
    pfree_ext(query.data);
    return OidIsValid(oid);
}

/*
 * @Description: get table's defination by table oid.
 * @in tableoid - table oid.
 * @return - table's defination.
 */
static char* pg_get_tabledef_worker(Oid tableoid)
{
    StringInfoData buf;
    StringInfoData query;
    const char* reltypename = NULL;
    int actual_atts = 0;
    bool isnull = false;
    char* srvname = NULL;
    char* ftoptions = NULL;
    bool ft_write_only = false;
    char* formatter = NULL;
    int cnt_ft_frmt_clmns = 0;  /* no of formatter columns */
    char** ft_frmt_clmn = NULL; /* formatter columns       */
    bool IsHDFSFTbl = false;    /*HDFS foreign table*/
    bool isHDFSTbl = false;     /*HDFS table*/
    tableInfo tableinfo;
    Form_pg_class classForm = NULL;
    int spirc;
    int proc;
    const char* relname = NULL;
    Relation pg_constraint = NULL;
    HeapTuple tuple = NULL;
    SysScanDesc scan = NULL;
    ScanKeyData skey[1];
    bool has_not_valid_check = false;

    /* init tableinfo.reloptions and tableinfo.reloftype */
    tableinfo.reloptions = NULL;

    if (IsTempTable(tableoid)) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("Can not get temporary tables defination.")));
    }

    tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(tableoid));
    if (!HeapTupleIsValid(tuple)) {
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for table %u.", tableoid)));
    }

    /* check table acl if enable private object like AlterDatabasePrivateObject */
    if (!IsTableVisible(tableoid)) {
        ReleaseSysCache(tuple);
        return NULL;
    }

    /*
     * Do this first so that string is alloc'd in outer context not SPI's.
     */
    initStringInfo(&buf);
    initStringInfo(&query);

    classForm = (Form_pg_class)GETSTRUCT(tuple);

    tableinfo.relkind = classForm->relkind;

    if (tableinfo.relkind != RELKIND_FOREIGN_TABLE && tableinfo.relkind != RELKIND_STREAM 
        && tableinfo.relkind != RELKIND_RELATION) {
        ReleaseSysCache(tuple);
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("Not a ordinary table or foreign table.")));
    }

    Datum reloptions = SysCacheGetAttr(RELOID, tuple, Anum_pg_class_reloptions, &isnull);
    if (isnull == false) {
        Datum sep = CStringGetTextDatum(", ");
        Datum txt = OidFunctionCall2(F_ARRAY_TO_TEXT, reloptions, sep);
        tableinfo.reloptions = TextDatumGetCString(txt);

        /*
         * skipping error tables
         */
        if (NULL != strstr(tableinfo.reloptions, "internal_mask")) {
            ReleaseSysCache(tuple);
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("Not a ordinary table or foreign table.")));
        }
    }

    tableinfo.relpersistence = classForm->relpersistence;
    tableinfo.tablespace = classForm->reltablespace;
    tableinfo.hasPartialClusterKey = classForm->relhasclusterkey;
    tableinfo.hasindex = classForm->relhasindex;
    tableinfo.relcmpr = classForm->relcmprs;
    tableinfo.relrowmovement = classForm->relrowmovement;
    tableinfo.parttype = classForm->parttype;
    tableinfo.spcid = classForm->relnamespace;
    tableinfo.relname = pstrdup(NameStr(classForm->relname));

    ReleaseSysCache(tuple);

    relname = quote_identifier(tableinfo.relname);
    /*
     * Connect to SPI manager
     */
    SPI_STACK_LOG("connect", NULL, NULL);
    if (SPI_connect() != SPI_OK_CONNECT)
        ereport(ERROR, (errcode(ERRCODE_SPI_CONNECTION_FAILURE), errmsg("SPI_connect failed")));

    if (tableinfo.relkind == RELKIND_FOREIGN_TABLE || tableinfo.relkind == RELKIND_STREAM) {

        int i_srvname, i_ftoptions, i_ftwriteonly;
        char* ft_write_only_str = NULL;

        resetStringInfo(&query);
        /* retrieve name of foreign server and generic options */
        appendStringInfo(&query,
            "SELECT fs.srvname, "
            "pg_catalog.array_to_string(ARRAY("
            "SELECT pg_catalog.quote_ident(option_name) || "
            "' ' || pg_catalog.quote_literal(option_value) "
            "FROM pg_catalog.pg_options_to_table(ftoptions) "
            "ORDER BY option_name"
            "), E',\n    ') AS ftoptions, ft.ftwriteonly ftwriteonly "
            "FROM pg_catalog.pg_foreign_table ft "
            "JOIN pg_catalog.pg_foreign_server fs "
            "ON (fs.oid = ft.ftserver) "
            "WHERE ft.ftrelid = %u",
            tableoid);

        spirc = SPI_execute(query.data, true, INT_MAX);
        proc = SPI_processed;

        i_srvname = SPI_fnumber(SPI_tuptable->tupdesc, "srvname");
        i_ftoptions = SPI_fnumber(SPI_tuptable->tupdesc, "ftoptions");
        i_ftwriteonly = SPI_fnumber(SPI_tuptable->tupdesc, "ftwriteonly");
        srvname = pstrdup(SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, i_srvname));
        ftoptions = pstrdup(SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, i_ftoptions));
        ft_write_only_str = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, i_ftwriteonly);

        if (('t' == ft_write_only_str[0]) || ('1' == ft_write_only_str[0]) ||
            (('o' == ft_write_only_str[0]) && ('n' == ft_write_only_str[1]))) {
            ft_write_only = true;
        }

        if ((ftoptions != NULL && ftoptions[0]) && (NULL != strstr(ftoptions, "formatter"))) {
            resetStringInfo(&query);
            appendStringInfo(&query,
                "WITH ft_options AS "
                "(SELECT (pg_catalog.pg_options_to_table(ft.ftoptions)).option_name, "
                "(pg_catalog.pg_options_to_table(ft.ftoptions)).option_value "
                "FROM pg_catalog.pg_foreign_table ft "
                "WHERE ft.ftrelid = %u) "
                "SELECT option_value FROM ft_options WHERE option_name = 'formatter'",
                tableoid);

            spirc = SPI_execute(query.data, true, INT_MAX);
            proc = SPI_processed;

            int i_formatter = SPI_fnumber(SPI_tuptable->tupdesc, "option_value");
            formatter = pstrdup(SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, i_formatter));

            char* temp_iter = formatter;
            cnt_ft_frmt_clmns = 1;
            while (*temp_iter != '\0') {
                if ('.' == *temp_iter)
                    cnt_ft_frmt_clmns++;
                temp_iter++;
            }

            ft_frmt_clmn = (char**)pg_malloc(cnt_ft_frmt_clmns * sizeof(char*));
            int iterf = 0;
            temp_iter = formatter;
            while (*temp_iter != '\0') {
                ft_frmt_clmn[iterf] = temp_iter;
                while (*temp_iter && '.' != *temp_iter)
                    temp_iter++;
                if ('.' == *temp_iter) {
                    iterf++;
                    *temp_iter = '\0';
                    temp_iter++;
                }
            }
        }

        reltypename = "FOREIGN TABLE";
    } else
        reltypename = "TABLE";

    if (NULL != tableinfo.reloptions && NULL != strstr(tableinfo.reloptions, "orientation=orc")) {
        isHDFSTbl = true;
    }

    appendStringInfo(&buf, "SET search_path = %s;", quote_identifier(get_namespace_name(tableinfo.spcid)));

    appendStringInfo(&buf, "\nCREATE %s%s %s",
        (tableinfo.relpersistence == RELPERSISTENCE_UNLOGGED) ?
        "UNLOGGED " :
        ((tableinfo.relpersistence == RELPERSISTENCE_GLOBAL_TEMP) ? "GLOBAL TEMPORARY " : ""),
        reltypename, relname);


    // get attribute info
    actual_atts = get_table_attribute(tableoid, &buf, formatter, ft_frmt_clmn, cnt_ft_frmt_clmns);

    /*
     * Fetch the constraint tuple from pg_constraint.  There may be more than
     * one match, because constraints are not required to have unique names;
     * if so, error out.
     */
    pg_constraint = heap_open(ConstraintRelationId, AccessShareLock);

    ScanKeyInit(&skey[0], Anum_pg_constraint_conrelid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(tableoid));

    scan = systable_beginscan(pg_constraint, ConstraintRelidIndexId, true, NULL, 1, skey);

    while (HeapTupleIsValid(tuple = systable_getnext(scan))) {
        Form_pg_constraint con = (Form_pg_constraint)GETSTRUCT(tuple);

        if (con->contype == 'c' || con->contype == 'f') {
            if (!con->convalidated) {
                has_not_valid_check = true;
                continue;
            }

            if (actual_atts == 0)
                appendStringInfo(&buf, " (\n    ");
            else
                appendStringInfo(&buf, ",\n    ");

            appendStringInfo(&buf, "CONSTRAINT %s ", quote_identifier(NameStr(con->conname)));
            Oid conOid = HeapTupleGetOid(tuple);
            appendStringInfo(&buf, "%s", pg_get_constraintdef_worker(conOid, false, 0));

            actual_atts++;
        }
    }

    systable_endscan(scan);
    heap_close(pg_constraint, AccessShareLock);

    if (actual_atts) {
        appendStringInfo(&buf, "\n)");
    }

    /* append  table info to buf */
    IsHDFSFTbl = append_table_info(tableinfo, srvname, &buf, tableoid, isHDFSTbl, &query, ftoptions, ft_write_only);

    if (RELKIND_FOREIGN_TABLE == tableinfo.relkind || RELKIND_STREAM == tableinfo.relkind) {
        /*
         * NOTICE: The distributeby clause is dumped when only a cordinator node
         * is oprerated by gs_dump. For datanode, it is not necessary to dump
         * distributeby clause.
         */
        if (IsHDFSFTbl) {
            char locator_type = GetLocatorType(tableoid);

            switch (locator_type) {
                case 'N':
                    appendStringInfo(&buf, "\nDISTRIBUTE BY ROUNDROBIN");
                    break;
                case 'R':
                    appendStringInfo(&buf, "\nDISTRIBUTE BY REPLICATION");
                    break;
                default:
                    break;
            }

            if (tableinfo.parttype == PARTTYPE_PARTITIONED_RELATION) {
                get_table_partitiondef(&query, &buf, tableoid, tableinfo);
                appendStringInfo(&buf, "AUTOMAPPED");
            }
        }
    }

    appendStringInfo(&buf, ";");

    get_changing_table_info(tableoid, &buf, relname, tableinfo.relkind);

    /* get alter table info */
    get_table_alter_info(tableinfo, has_not_valid_check, &buf, tableoid, relname);
    /*
     * Disconnect from SPI manager
     */
    SPI_STACK_LOG("finish", NULL, NULL);
    if (SPI_finish() != SPI_OK_FINISH)
        ereport(ERROR, (errcode(ERRCODE_SPI_FINISH_FAILURE), errmsg("SPI_finish failed")));

    pfree_ext(query.data);
    pfree_ext(tableinfo.relname);
    return buf.data;
}

/*
 * @Description: get table's defination by table oid.
 */
Datum pg_get_tabledef_ext(PG_FUNCTION_ARGS)
{
    /* By OID */
    Oid tableoid = PG_GETARG_OID(0);

    char* tabledef = pg_get_tabledef_worker(tableoid);
    if (tabledef != NULL)
        PG_RETURN_TEXT_P(string_to_text(tabledef));
    else
        PG_RETURN_NULL();
}

/* ----------
 * get_triggerdef			- Get the definition of a trigger
 * ----------
 */
Datum pg_get_triggerdef(PG_FUNCTION_ARGS)
{
    Oid trigid = PG_GETARG_OID(0);

    PG_RETURN_TEXT_P(string_to_text(pg_get_triggerdef_worker(trigid, false)));
}

Datum pg_get_triggerdef_ext(PG_FUNCTION_ARGS)
{
    Oid trigid = PG_GETARG_OID(0);
    bool pretty = PG_GETARG_BOOL(1);

    PG_RETURN_TEXT_P(string_to_text(pg_get_triggerdef_worker(trigid, pretty)));
}

static char* pg_get_triggerdef_worker(Oid trigid, bool pretty)
{
    HeapTuple ht_trig;
    Form_pg_trigger trigrec;
    StringInfoData buf;
    Relation tgrel;
    ScanKeyData skey[1];
    SysScanDesc tgscan;
    int findx = 0;
    char* tgname = NULL;
    Datum value;
    bool isnull = false;

    /*
     * Fetch the pg_trigger tuple by the Oid of the trigger
     */
    tgrel = heap_open(TriggerRelationId, AccessShareLock);

    ScanKeyInit(&skey[0], ObjectIdAttributeNumber, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(trigid));

    tgscan = systable_beginscan(tgrel, TriggerOidIndexId, true, NULL, 1, skey);

    ht_trig = systable_getnext(tgscan);

    if (!HeapTupleIsValid(ht_trig))
        ereport(ERROR, (errcode(ERRCODE_NO_DATA_FOUND), errmsg("could not find tuple for trigger %u", trigid)));
    trigrec = (Form_pg_trigger)GETSTRUCT(ht_trig);

    /*
     * Start the trigger definition. Note that the trigger's name should never
     * be schema-qualified, but the trigger rel's name may be.
     */
    initStringInfo(&buf);

    tgname = NameStr(trigrec->tgname);
    appendStringInfo(
        &buf, "CREATE %sTRIGGER %s ", OidIsValid(trigrec->tgconstraint) ? "CONSTRAINT " : "", quote_identifier(tgname));

    if (TRIGGER_FOR_BEFORE(trigrec->tgtype))
        appendStringInfo(&buf, "BEFORE");
    else if (TRIGGER_FOR_AFTER(trigrec->tgtype))
        appendStringInfo(&buf, "AFTER");
    else if (TRIGGER_FOR_INSTEAD(trigrec->tgtype))
        appendStringInfo(&buf, "INSTEAD OF");
    else
        ereport(
            ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("unexpected tgtype value: %d", trigrec->tgtype)));

    if (TRIGGER_FOR_INSERT(trigrec->tgtype)) {
        appendStringInfo(&buf, " INSERT");
        findx++;
    }
    if (TRIGGER_FOR_DELETE(trigrec->tgtype)) {
        if (findx > 0)
            appendStringInfo(&buf, " OR DELETE");
        else
            appendStringInfo(&buf, " DELETE");
        findx++;
    }
    if (TRIGGER_FOR_UPDATE(trigrec->tgtype)) {
        if (findx > 0)
            appendStringInfo(&buf, " OR UPDATE");
        else
            appendStringInfo(&buf, " UPDATE");
        findx++;
        /* tgattr is first var-width field, so OK to access directly */
        if (trigrec->tgattr.dim1 > 0) {
            int i;

            appendStringInfoString(&buf, " OF ");
            for (i = 0; i < trigrec->tgattr.dim1; i++) {
                char* attname = NULL;

                if (i > 0)
                    appendStringInfoString(&buf, ", ");
                attname = get_relid_attribute_name(trigrec->tgrelid, trigrec->tgattr.values[i]);
                appendStringInfoString(&buf, quote_identifier(attname));
            }
        }
    }
    if (TRIGGER_FOR_TRUNCATE(trigrec->tgtype)) {
        if (findx > 0)
            appendStringInfo(&buf, " OR TRUNCATE");
        else
            appendStringInfo(&buf, " TRUNCATE");
        findx++;
    }
    appendStringInfo(&buf, " ON %s ", generate_relation_name(trigrec->tgrelid, NIL));

    if (OidIsValid(trigrec->tgconstraint)) {
        if (OidIsValid(trigrec->tgconstrrelid))
            appendStringInfo(&buf, "FROM %s ", generate_relation_name(trigrec->tgconstrrelid, NIL));
        if (!trigrec->tgdeferrable)
            appendStringInfo(&buf, "NOT ");
        appendStringInfo(&buf, "DEFERRABLE INITIALLY ");
        if (trigrec->tginitdeferred)
            appendStringInfo(&buf, "DEFERRED ");
        else
            appendStringInfo(&buf, "IMMEDIATE ");
    }

    if (TRIGGER_FOR_ROW(trigrec->tgtype))
        appendStringInfo(&buf, "FOR EACH ROW ");
    else
        appendStringInfo(&buf, "FOR EACH STATEMENT ");

    /* If the trigger has a WHEN qualification, add that */
    value = fastgetattr(ht_trig, Anum_pg_trigger_tgqual, tgrel->rd_att, &isnull);
    if (!isnull) {
        Node* qual = NULL;
        char relkind;
        deparse_context context;
        deparse_namespace dpns;
        RangeTblEntry* oldrte = NULL;
        RangeTblEntry* newrte = NULL;

        appendStringInfoString(&buf, "WHEN (");

        qual = (Node*)stringToNode(TextDatumGetCString(value));

        relkind = get_rel_relkind(trigrec->tgrelid);

        /* Build minimal OLD and NEW RTEs for the rel */
        oldrte = makeNode(RangeTblEntry);
        oldrte->rtekind = RTE_RELATION;
        oldrte->relid = trigrec->tgrelid;
        oldrte->relkind = relkind;
        oldrte->eref = makeAlias("old", NIL);
        oldrte->lateral = false;
        oldrte->inh = false;
        oldrte->inFromCl = true;

        newrte = makeNode(RangeTblEntry);
        newrte->rtekind = RTE_RELATION;
        newrte->relid = trigrec->tgrelid;
        newrte->relkind = relkind;
        newrte->eref = makeAlias("new", NIL);
        newrte->lateral = false;
        newrte->inh = false;
        newrte->inFromCl = true;

        /* Build two-element rtable */
        errno_t rc = memset_s(&dpns, sizeof(dpns), 0, sizeof(dpns));
        securec_check(rc, "\0", "\0");
        dpns.rtable = list_make2(oldrte, newrte);
        dpns.ctes = NIL;

        /* Set up context with one-deep namespace stack */
        context.buf = &buf;
        context.namespaces = list_make1(&dpns);
        context.windowClause = NIL;
        context.windowTList = NIL;
        context.varprefix = true;
        context.prettyFlags = pretty ? PRETTYFLAG_PAREN : 0;
#ifdef PGXC
        context.finalise_aggs = false;
        context.sortgroup_colno = false;
        context.parser_arg = NULL;
        context.viewdef = false;
        context.is_fqs = false;
#endif /* PGXC */
        context.wrapColumn = WRAP_COLUMN_DEFAULT;
        context.indentLevel = PRETTYINDENT_STD;
        context.qrw_phase = false;
        context.is_upsert_clause = false;

        get_rule_expr(qual, &context, false);

        appendStringInfo(&buf, ") ");
    }

    appendStringInfo(&buf, "EXECUTE PROCEDURE %s(", generate_function_name(trigrec->tgfoid, 0, NIL, NULL, false, NULL));

    if (trigrec->tgnargs > 0) {
        char* p = NULL;
        int i;

        value = fastgetattr(ht_trig, Anum_pg_trigger_tgargs, tgrel->rd_att, &isnull);
        if (isnull)
            ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NULL_VALUE), errmsg("tgargs is null for trigger %u", trigid)));

        p = (char*)VARDATA(DatumGetByteaP(value));
        for (i = 0; i < trigrec->tgnargs; i++) {
            if (i > 0)
                appendStringInfo(&buf, ", ");
            simple_quote_literal(&buf, p);
            /* advance p to next string embedded in tgargs */
            while (*p)
                p++;
            p++;
        }
    }

    /* We deliberately do not put semi-colon at end */
    appendStringInfo(&buf, ")");

    /* Clean up */
    systable_endscan(tgscan);

    heap_close(tgrel, AccessShareLock);

    return buf.data;
}

/* ----------
 * get_indexdef			- Get the definition of an index
 *
 * In the extended version, there is a colno argument as well as pretty bool.
 *	if colno == 0, we want a complete index definition.
 *	if colno > 0, we only want the Nth index key's variable or expression.
 *
 * Note that the SQL-function versions of this omit any info about the
 * index tablespace; this is intentional because pg_dump wants it that way.
 * However pg_get_indexdef_string() includes the index tablespace.
 * ----------
 */
Datum pg_get_indexdef(PG_FUNCTION_ARGS)
{
    Oid indexrelid = PG_GETARG_OID(0);

    PG_RETURN_TEXT_P(string_to_text(pg_get_indexdef_worker(indexrelid, 0, NULL, false, true, 0)));
}

/**
 * @Description: Get he definition of an index for dump scene.
 * If the table is an interval partitioned table and the index is a local index and dumpSchemaOnly is true, 
 * only output indexes of range partitions, else the output is the same as pg_get_indexdef.
 * @in The index table Oid and dump scheme only flag.
 * @return Returns a palloc'd C string; no pretty-printing.
 */
Datum pg_get_indexdef_for_dump(PG_FUNCTION_ARGS)
{
    Oid indexrelid = PG_GETARG_OID(0);
    bool dumpSchemaOnly = PG_GETARG_BOOL(1);

    PG_RETURN_TEXT_P(string_to_text(pg_get_indexdef_worker(indexrelid, 0, NULL, false, true, 0, dumpSchemaOnly,
                                                           true, false)));
}

Datum pg_get_indexdef_ext(PG_FUNCTION_ARGS)
{
    Oid indexrelid = PG_GETARG_OID(0);
    int32 colno = PG_GETARG_INT32(1);
    bool pretty = PG_GETARG_BOOL(2);
    int prettyFlags;

    prettyFlags = pretty ? (PRETTYFLAG_PAREN | PRETTYFLAG_INDENT) : 0;
    PG_RETURN_TEXT_P(string_to_text(pg_get_indexdef_worker(indexrelid, colno, NULL, colno != 0, true, prettyFlags,
                                                           false, false, false)));
}

/**
 * @Description: Internal version for use by ALTER TABLE.
 * Includes a tablespace clause in the result.
 * @in The index table Oid.
 * @return Returns a palloc'd C string; no pretty-printing.
 */
char* pg_get_indexdef_string(Oid indexrelid)
{
    return pg_get_indexdef_worker(indexrelid, 0, NULL, false, true, 0);
}

/* Internal version that just reports the column definitions */
char* pg_get_indexdef_columns(Oid indexrelid, bool pretty)
{
    int prettyFlags;

    prettyFlags = pretty ? (PRETTYFLAG_PAREN | PRETTYFLAG_INDENT) : 0;
    return pg_get_indexdef_worker(indexrelid, 0, NULL, true, false, prettyFlags);
}

static void AppendOnePartitionIndex(Oid indexRelId, Oid partOid, bool showTblSpc, bool *isFirst,
                                    StringInfoData *buf, bool isSub = false)
{
    Oid partIdxOid = getPartitionIndexOid(indexRelId, partOid);

    HeapTuple partIdxHeapTuple = SearchSysCache1(PARTRELID, partIdxOid);

    if (!HeapTupleIsValid(partIdxHeapTuple)) {
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for index %u", partIdxOid)));
    }

    Form_pg_partition partIdxTuple = (Form_pg_partition)GETSTRUCT(partIdxHeapTuple);

    if (*isFirst) {
        *isFirst = false;
    } else {
        appendStringInfo(buf, ", ");
    }

    if (isSub) {
        appendStringInfo(buf, "\n    ");
    }

    appendStringInfo(buf, "PARTITION %s", quote_identifier(partIdxTuple->relname.data));
    if (showTblSpc && OidIsValid(partIdxTuple->reltablespace)) {
        char *tblspacName = get_tablespace_name(partIdxTuple->reltablespace);
        if (tblspacName != NULL)
            appendStringInfo(buf, " TABLESPACE %s", quote_identifier(tblspacName));
        else
            ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
                            errmsg("tablespace with OID %u does not exist", partIdxTuple->reltablespace)));
    }

    ReleaseSysCache(partIdxHeapTuple);
}

static void GetIndexdefForIntervalPartTabDumpSchemaOnly(Oid indexrelid, RangePartitionMap *partMap, bool showTblSpc,
                                                            StringInfoData *buf)
{
    bool isFirst = true;

    appendStringInfo(buf, " LOCAL");
    appendStringInfoChar(buf, '(');

    for (int i = 0; i < partMap->rangeElementsNum; ++i) {
        RangeElement &elem = partMap->rangeElements[i];
        /* for dummp schema only scene, don]t dump indexes of interval paritions */
        if (elem.isInterval) {
            continue;
        }
        AppendOnePartitionIndex(indexrelid, elem.partitionOid, showTblSpc, &isFirst, buf);
    }
    appendStringInfo(buf, ") ");
}

static void pg_get_indexdef_partitions(Oid indexrelid, Form_pg_index idxrec, bool showTblSpc, StringInfoData *buf,
                                       bool dumpSchemaOnly, bool showPartitionLocal, bool showSubpartitionLocal)
{
    Oid relid = idxrec->indrelid;
    /*
     * First of all, gs_dump will add share lock, no need to add lock here nor release it at the end of the function.
     * Second, gs_dump supports option '--non-lock-table', in which case we should not add lock.
     */
    Relation rel = heap_open(relid, NoLock);
    if (dumpSchemaOnly && rel->partMap->type == PART_TYPE_INTERVAL) {
        GetIndexdefForIntervalPartTabDumpSchemaOnly(indexrelid, (RangePartitionMap *)rel->partMap, showTblSpc, buf);
        heap_close(rel, NoLock);
        return;
    }

    appendStringInfo(buf, " LOCAL");
    /*
     * The LOCAL index information of the partition and subpartition table is more.
     * And the meta-statements (e.g. \d \d+ \dS) are used more.
     * Therefore, when the meta-statement is called, the LOCAL index information is not displayed.
     */
    bool isSub = RelationIsSubPartitioned(rel);
    if ((!isSub && !showPartitionLocal) || (isSub && !showSubpartitionLocal)) {
        heap_close(rel, NoLock);
        return;
    }

    List *partList = NIL;
    ListCell *lc = NULL;
    bool isFirst = true;
    if (isSub) {
        /* reserve this code, oneday we will support it */
        partList = RelationGetSubPartitionOidList(rel);
    } else {
        partList = relationGetPartitionOidList(rel);
    }

    if (isSub) {
        appendStringInfo(buf, "\n");
    }
    appendStringInfoChar(buf, '(');

    foreach (lc, partList) {
        Oid partOid = DatumGetObjectId(lfirst(lc));
        AppendOnePartitionIndex(indexrelid, partOid, showTblSpc, &isFirst, buf, isSub);
    }
    if (isSub) {
        appendStringInfo(buf, "\n");
    }
    appendStringInfo(buf, ") ");

    heap_close(rel, NoLock);
    releasePartitionOidList(&partList);
}

/*
 * Internal workhorse to decompile an index definition.
 *
 * This is now used for exclusion constraints as well: if excludeOps is not
 * NULL then it points to an array of exclusion operator OIDs.
 */
static char *pg_get_indexdef_worker(Oid indexrelid, int colno, const Oid *excludeOps, bool attrsOnly, bool showTblSpc,
    int prettyFlags, bool dumpSchemaOnly, bool showPartitionLocal, bool showSubpartitionLocal)
{
    /* might want a separate isConstraint parameter later */
    bool isConstraint = (excludeOps != NULL);
    HeapTuple ht_idx;
    HeapTuple ht_idxrel;
    HeapTuple ht_am;
    Form_pg_index idxrec;
    Form_pg_class idxrelrec;
    Form_pg_am amrec;
    List* indexprs = NIL;
    ListCell* indexpr_item = NULL;
    List* context = NIL;
    Oid indrelid;
    int keyno;
    Datum indcollDatum;
    Datum indclassDatum;
    Datum indoptionDatum;
    bool isnull = false;
    oidvector* indcollation = NULL;
    oidvector* indclass = NULL;
    int2vector* indoption = NULL;
    StringInfoData buf;
    char* str = NULL;
    char* sep = NULL;
    int indnkeyatts;

    /*
     * Fetch the pg_index tuple by the Oid of the index
     */
    ht_idx = SearchSysCache1(INDEXRELID, ObjectIdGetDatum(indexrelid));
    if (!HeapTupleIsValid(ht_idx))
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for index %u", indexrelid)));
    idxrec = (Form_pg_index)GETSTRUCT(ht_idx);

    indrelid = idxrec->indrelid;
    Assert(indexrelid == idxrec->indexrelid);
    indnkeyatts = GetIndexKeyAttsByTuple(NULL, ht_idx);

    /* Must get indcollation, indclass, and indoption the hard way */
    indcollDatum = SysCacheGetAttr(INDEXRELID, ht_idx, Anum_pg_index_indcollation, &isnull);
    Assert(!isnull);
    indcollation = (oidvector*)DatumGetPointer(indcollDatum);

    indclassDatum = SysCacheGetAttr(INDEXRELID, ht_idx, Anum_pg_index_indclass, &isnull);
    Assert(!isnull);
    indclass = (oidvector*)DatumGetPointer(indclassDatum);

    indoptionDatum = SysCacheGetAttr(INDEXRELID, ht_idx, Anum_pg_index_indoption, &isnull);
    Assert(!isnull);
    indoption = (int2vector*)DatumGetPointer(indoptionDatum);

    /*
     * Fetch the pg_class tuple of the index relation
     */
    ht_idxrel = SearchSysCache1(RELOID, ObjectIdGetDatum(indexrelid));
    if (!HeapTupleIsValid(ht_idxrel))
        ereport(
            ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for relation %u", indexrelid)));
    idxrelrec = (Form_pg_class)GETSTRUCT(ht_idxrel);

    /*
     * Fetch the pg_am tuple of the index' access method
     */
    ht_am = SearchSysCache1(AMOID, ObjectIdGetDatum(idxrelrec->relam));
    if (!HeapTupleIsValid(ht_am))
        ereport(ERROR,
            (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                errmsg("cache lookup failed for access method %u", idxrelrec->relam)));

    amrec = (Form_pg_am)GETSTRUCT(ht_am);

    /*
     * Get the index expressions, if any.  (NOTE: we do not use the relcache
     * versions of the expressions and predicate, because we want to display
     * non-const-folded expressions.)
     */
    if (!heap_attisnull(ht_idx, Anum_pg_index_indexprs, NULL)) {
        Datum exprsDatum;
        bool isnull = false;
        char* exprsString = NULL;

        exprsDatum = SysCacheGetAttr(INDEXRELID, ht_idx, Anum_pg_index_indexprs, &isnull);
        Assert(!isnull);
        exprsString = TextDatumGetCString(exprsDatum);
        indexprs = (List*)stringToNode(exprsString);
        pfree_ext(exprsString);
    } else
        indexprs = NIL;

    indexpr_item = list_head(indexprs);

    context = deparse_context_for(get_relation_name(indrelid), indrelid);

    /*
     * Start the index definition.	Note that the index's name should never be
     * schema-qualified, but the indexed rel's name may be.
     */
    initStringInfo(&buf);

    if (!attrsOnly) {
        if (!isConstraint)
            appendStringInfo(&buf,
                "CREATE %sINDEX %s ON %s USING %s (",
                idxrec->indisunique ? "UNIQUE " : "",
                quote_identifier(NameStr(idxrelrec->relname)),
                generate_relation_name(indrelid, NIL),
                quote_identifier(NameStr(amrec->amname)));
        else /* currently, must be EXCLUDE constraint */
            appendStringInfo(&buf, "EXCLUDE USING %s (", quote_identifier(NameStr(amrec->amname)));
    }

    /*
     * Report the indexed attributes
     */
    sep = "";
    for (keyno = 0; keyno < idxrec->indnatts; keyno++) {
        AttrNumber attnum = idxrec->indkey.values[keyno];
        int16 opt = indoption->values[keyno];
        Oid keycoltype;
        Oid keycolcollation;

        /*
         * Ignore non-key attributes if told to.
         */
        if (keyno >= indnkeyatts) {
            break;
        }

        if (!colno)
            appendStringInfoString(&buf, sep);
        sep = ", ";

        if (attnum != 0) {
            /* Simple index column */
            char* attname = NULL;
            int32 keycoltypmod;

            attname = get_relid_attribute_name(indrelid, attnum);
            if (!colno || colno == keyno + 1)
                appendStringInfoString(&buf, quote_identifier(attname));
            get_atttypetypmodcoll(indrelid, attnum, &keycoltype, &keycoltypmod, &keycolcollation);
        } else {
            /* expressional index */
            Node* indexkey = NULL;

            if (indexpr_item == NULL)
                ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NULL_VALUE), errmsg("too few entries in indexprs list")));

            indexkey = (Node*)lfirst(indexpr_item);
            indexpr_item = lnext(indexpr_item);
            /* Deparse */
            str = deparse_expression_pretty(indexkey, context, false, false, prettyFlags, 0);
            if (!colno || colno == keyno + 1) {
                /* Need parens if it's not a bare function call */
                if (indexkey && IsA(indexkey, FuncExpr) && ((FuncExpr*)indexkey)->funcformat == COERCE_EXPLICIT_CALL)
                    appendStringInfoString(&buf, str);
                else
                    appendStringInfo(&buf, "(%s)", str);
            }
            keycoltype = exprType(indexkey);
            keycolcollation = exprCollation(indexkey);
        }

        if (!attrsOnly && (!colno || colno == keyno + 1)) {
            Oid indcoll;

            if (keyno >= indnkeyatts) {
                continue;
            }

            /* Add collation, if not default for column */
            indcoll = indcollation->values[keyno];
            if (OidIsValid(indcoll) && indcoll != keycolcollation)
                appendStringInfo(&buf, " COLLATE %s", generate_collation_name((indcoll)));

            /* Add the operator class name, if not default */
            get_opclass_name(indclass->values[keyno], keycoltype, &buf);

            /* Add options if relevant */
            if (amrec->amcanorder) {
                /* if it supports sort ordering, report DESC and NULLS opts */
                if (opt & INDOPTION_DESC) {
                    appendStringInfo(&buf, " DESC");
                    /* NULLS FIRST is the default in this case */
                    if (!(opt & INDOPTION_NULLS_FIRST))
                        appendStringInfo(&buf, " NULLS LAST");
                } else {
                    if (opt & INDOPTION_NULLS_FIRST)
                        appendStringInfo(&buf, " NULLS FIRST");
                }
            }

            /* Add the exclusion operator if relevant */
            if (excludeOps != NULL)
                appendStringInfo(&buf, " WITH %s", generate_operator_name(excludeOps[keyno], keycoltype, keycoltype));
        }
    }

    if (!attrsOnly) {
        appendStringInfoChar(&buf, ')');

        if (idxrelrec->parttype == PARTTYPE_PARTITIONED_RELATION &&
            idxrelrec->relkind != RELKIND_GLOBAL_INDEX) {
            pg_get_indexdef_partitions(indexrelid, idxrec, showTblSpc, &buf, dumpSchemaOnly,
                showPartitionLocal, showSubpartitionLocal);
        }

        /*
         * If it has options, append "WITH (options)"
         */
        str = flatten_reloptions(indexrelid);
        if (str != NULL) {
            appendStringInfo(&buf, " WITH (%s)", str);
            pfree_ext(str);
        }

        /*
         * Print tablespace, but only if requested.
         */
        if (showTblSpc) {
            Oid tblspc;

            tblspc = get_rel_tablespace(indexrelid);
            if (!OidIsValid(tblspc)) {
                tblspc = u_sess->proc_cxt.MyDatabaseTableSpace;
            }
            if (isConstraint) {
                appendStringInfoString(&buf, " USING INDEX");
            }
            char* tblspacName = get_tablespace_name(tblspc);
            if (tblspacName != NULL)
                appendStringInfo(&buf, " TABLESPACE %s", quote_identifier(tblspacName));
            else
                ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("tablespace with OID %u does not exist", tblspc)));
        }

        /*
         * If it's a partial index, decompile and append the predicate
         */
        if (!heap_attisnull(ht_idx, Anum_pg_index_indpred, NULL)) {
            Node* node = NULL;
            Datum predDatum;
            bool isnull = false;
            char* predString = NULL;

            /* Convert text string to node tree */
            predDatum = SysCacheGetAttr(INDEXRELID, ht_idx, Anum_pg_index_indpred, &isnull);
            Assert(!isnull);
            predString = TextDatumGetCString(predDatum);
            node = (Node*)stringToNode(predString);
            pfree_ext(predString);

            /* Deparse */
            str = deparse_expression_pretty(node, context, false, false, prettyFlags, 0);
            if (isConstraint)
                appendStringInfo(&buf, " WHERE (%s)", str);
            else
                appendStringInfo(&buf, " WHERE %s", str);
        }
    }

    /* Clean up */
    ReleaseSysCache(ht_idx);
    ReleaseSysCache(ht_idxrel);
    ReleaseSysCache(ht_am);

    return buf.data;
}

/*
 * pg_get_constraintdef
 *
 * Returns the definition for the constraint, ie, everything that needs to
 * appear after "ALTER TABLE ... ADD CONSTRAINT <constraintname>".
 */
Datum pg_get_constraintdef(PG_FUNCTION_ARGS)
{
    Oid constraintId = PG_GETARG_OID(0);

    PG_RETURN_TEXT_P(string_to_text(pg_get_constraintdef_worker(constraintId, false, 0)));
}

Datum pg_get_constraintdef_ext(PG_FUNCTION_ARGS)
{
    Oid constraintId = PG_GETARG_OID(0);
    bool pretty = PG_GETARG_BOOL(1);
    int prettyFlags;

    prettyFlags = pretty ? (PRETTYFLAG_PAREN | PRETTYFLAG_INDENT) : 0;
    PG_RETURN_TEXT_P(string_to_text(pg_get_constraintdef_worker(constraintId, false, prettyFlags)));
}

/* Internal version that returns a palloc'd C string */
char* pg_get_constraintdef_string(Oid constraintId)
{
    return pg_get_constraintdef_worker(constraintId, true, 0);
}

static char* pg_get_constraintdef_worker(Oid constraintId, bool fullCommand, int prettyFlags)
{
    HeapTuple tup;
    Form_pg_constraint conForm;
    StringInfoData buf;

    tup = SearchSysCache1(CONSTROID, ObjectIdGetDatum(constraintId));
    if (!HeapTupleIsValid(tup)) /* should not happen */
        ereport(ERROR,
            (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for constraint %u", constraintId)));
    conForm = (Form_pg_constraint)GETSTRUCT(tup);

    initStringInfo(&buf);

    if (fullCommand && OidIsValid(conForm->conrelid)) {
        appendStringInfo(&buf, "ALTER TABLE ONLY %s ADD CONSTRAINT %s ",
            generate_relation_name(conForm->conrelid, NIL), quote_identifier(NameStr(conForm->conname)));
    }

    switch (conForm->contype) {
        case CONSTRAINT_FOREIGN: {
            Datum val;
            bool isnull = false;
            const char* string = NULL;

            /* Start off the constraint definition */
            appendStringInfo(&buf, "FOREIGN KEY (");

            /* Fetch and build referencing-column list */
            val = SysCacheGetAttr(CONSTROID, tup, Anum_pg_constraint_conkey, &isnull);
            if (isnull)
                ereport(ERROR,
                    (errcode(ERRCODE_UNEXPECTED_NULL_VALUE), errmsg("null conkey for constraint %u", constraintId)));

            decompile_column_index_array(val, conForm->conrelid, &buf);

            /* add foreign relation name */
            appendStringInfo(&buf, ") REFERENCES %s(", generate_relation_name(conForm->confrelid, NIL));

            /* Fetch and build referenced-column list */
            val = SysCacheGetAttr(CONSTROID, tup, Anum_pg_constraint_confkey, &isnull);
            if (isnull)
                ereport(ERROR,
                    (errcode(ERRCODE_UNEXPECTED_NULL_VALUE), errmsg("null confkey for constraint %u", constraintId)));

            decompile_column_index_array(val, conForm->confrelid, &buf);

            appendStringInfo(&buf, ")");

            /* Add match type */
            switch (conForm->confmatchtype) {
                case FKCONSTR_MATCH_FULL:
                    string = " MATCH FULL";
                    break;
                case FKCONSTR_MATCH_PARTIAL:
                    string = " MATCH PARTIAL";
                    break;
                case FKCONSTR_MATCH_UNSPECIFIED:
                    string = "";
                    break;
                default:
                    ereport(ERROR, (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                            errmsg("unrecognized confmatchtype: %d", conForm->confmatchtype)));
                    string = ""; /* keep compiler quiet */
                    break;
            }
            appendStringInfoString(&buf, string);

            /* Add ON UPDATE and ON DELETE clauses, if needed */
            switch (conForm->confupdtype) {
                case FKCONSTR_ACTION_NOACTION:
                    string = NULL; /* suppress default */
                    break;
                case FKCONSTR_ACTION_RESTRICT:
                    string = "RESTRICT";
                    break;
                case FKCONSTR_ACTION_CASCADE:
                    string = "CASCADE";
                    break;
                case FKCONSTR_ACTION_SETNULL:
                    string = "SET NULL";
                    break;
                case FKCONSTR_ACTION_SETDEFAULT:
                    string = "SET DEFAULT";
                    break;
                default:
                    ereport(ERROR, (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                            errmsg("unrecognized confupdtype: %d", conForm->confupdtype)));
                    string = NULL; /* keep compiler quiet */
                    break;
            }
            if (string != NULL)
                appendStringInfo(&buf, " ON UPDATE %s", string);

            switch (conForm->confdeltype) {
                case FKCONSTR_ACTION_NOACTION:
                    string = NULL; /* suppress default */
                    break;
                case FKCONSTR_ACTION_RESTRICT:
                    string = "RESTRICT";
                    break;
                case FKCONSTR_ACTION_CASCADE:
                    string = "CASCADE";
                    break;
                case FKCONSTR_ACTION_SETNULL:
                    string = "SET NULL";
                    break;
                case FKCONSTR_ACTION_SETDEFAULT:
                    string = "SET DEFAULT";
                    break;
                default:
                    ereport(ERROR, (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                            errmsg("unrecognized confdeltype: %d", conForm->confdeltype)));
                    string = NULL; /* keep compiler quiet */
                    break;
            }
            if (string != NULL)
                appendStringInfo(&buf, " ON DELETE %s", string);

            break;
        }
        case CONSTRAINT_PRIMARY:
        case CONSTRAINT_UNIQUE: {
            Datum val;
            bool isnull = false;
            Oid indexId;

            if (conForm->consoft) {
                if (conForm->conopt) {
                    appendStringInfo(&buf, "enable optimization Informational Constraint ");
                } else {
                    appendStringInfo(&buf, "disable optimization Informational Constraint ");
                }
            }

            /* Start off the constraint definition */
            if (conForm->contype == CONSTRAINT_PRIMARY)
                appendStringInfo(&buf, "PRIMARY KEY (");
            else
                appendStringInfo(&buf, "UNIQUE (");

            /* Fetch and build target column list */
            val = SysCacheGetAttr(CONSTROID, tup, Anum_pg_constraint_conkey, &isnull);
            if (isnull)
                ereport(ERROR,
                    (errcode(ERRCODE_UNEXPECTED_NULL_VALUE), errmsg("null conkey for constraint %u", constraintId)));

            decompile_column_index_array(val, conForm->conrelid, &buf);

            appendStringInfo(&buf, ")");

            /* Fetch and build including column list */
            isnull = true;
            val = SysCacheGetAttr(CONSTROID, tup, Anum_pg_constraint_conincluding, &isnull);
            if (!isnull) {
                appendStringInfoString(&buf, " INCLUDE (");
                decompile_column_index_array(val, conForm->conrelid, &buf);
                appendStringInfoChar(&buf, ')');
            }

            indexId = get_constraint_index(constraintId);
            /* XXX why do we only print these bits if fullCommand? */
            if (fullCommand && OidIsValid(indexId)) {
                char* options = flatten_reloptions(indexId);
                Oid tblspc;

                if (options != NULL) {
                    appendStringInfo(&buf, " WITH (%s)", options);
                    pfree_ext(options);
                }

                tblspc = get_rel_tablespace(indexId);
                if (OidIsValid(tblspc)) {
                    char* tblspcName = get_tablespace_name(tblspc);
                    if (tblspcName != NULL)
                        appendStringInfo(&buf, " USING INDEX TABLESPACE %s", quote_identifier(tblspcName));
                    else
                        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
                                errmsg("tablespace with OID %u does not exist", tblspc)));
                }
            }

            break;
        }
        case CONSTRAINT_CHECK: {
            Datum val;
            bool isnull = false;
            char* conbin = NULL;
            char* consrc = NULL;
            Node* expr = NULL;
            List* context = NIL;

            /* Fetch constraint expression in parsetree form */
            val = SysCacheGetAttr(CONSTROID, tup, Anum_pg_constraint_conbin, &isnull);
            if (isnull)
                ereport(ERROR,
                    (errcode(ERRCODE_UNEXPECTED_NULL_VALUE), errmsg("null conbin for constraint %u", constraintId)));

            conbin = TextDatumGetCString(val);
            expr = (Node*)stringToNode(conbin);

            /* Set up deparsing context for Var nodes in constraint */
            if (conForm->conrelid != InvalidOid) {
                /* relation constraint */
                context = deparse_context_for(get_relation_name(conForm->conrelid), conForm->conrelid);
            } else {
                /* domain constraint --- can't have Vars */
                context = NIL;
            }

            consrc = deparse_expression_pretty(expr, context, false, false, prettyFlags, 0);

            /*
             * Now emit the constraint definition, adding NO INHERIT if
             * necessary.
             *
             * There are cases where the constraint expression will be
             * fully parenthesized and we don't need the outer parens ...
             * but there are other cases where we do need 'em.  Be
             * conservative for now.
             *
             * Note that simply checking for leading '(' and trailing ')'
             * would NOT be good enough, consider "(x > 0) AND (y > 0)".
             */
            appendStringInfo(&buf, "CHECK (%s)%s", consrc, conForm->connoinherit ? " NO INHERIT" : "");
            break;
        }
        case CONSTRAINT_TRIGGER:

            /*
             * There isn't an ALTER TABLE syntax for creating a user-defined
             * constraint trigger, but it seems better to print something than
             * throw an error; if we throw error then this function couldn't
             * safely be applied to all rows of pg_constraint.
             */
            appendStringInfo(&buf, "TRIGGER");
            break;
        case CONSTRAINT_EXCLUSION: {
            Oid indexOid = conForm->conindid;
            Datum val;
            bool isnull = false;
            Datum* elems = NULL;
            int nElems;
            int i;
            Oid* operators = NULL;

            /* Extract operator OIDs from the pg_constraint tuple */
            val = SysCacheGetAttr(CONSTROID, tup, Anum_pg_constraint_conexclop, &isnull);
            if (isnull)
                ereport(ERROR,
                    (errcode(ERRCODE_UNEXPECTED_NULL_VALUE), errmsg("null conexclop for constraint %u", constraintId)));

            deconstruct_array(DatumGetArrayTypeP(val), OIDOID, sizeof(Oid), true, 'i', &elems, NULL, &nElems);

            operators = (Oid*)palloc(nElems * sizeof(Oid));
            for (i = 0; i < nElems; i++)
                operators[i] = DatumGetObjectId(elems[i]);

            /* pg_get_indexdef_worker does the rest */
            /* suppress tablespace because pg_dump wants it that way */
            appendStringInfoString(&buf, pg_get_indexdef_worker(indexOid, 0, operators, false, false, prettyFlags));
            break;
        }
        case CONSTRAINT_CLUSTER: {
            Datum val;
            bool isnull = false;

            /* Start off the constraint definition */
            appendStringInfo(&buf, "PARTIAL CLUSTER KEY (");

            /* Fetch and build referencing-column list */
            val = SysCacheGetAttr(CONSTROID, tup, Anum_pg_constraint_conkey, &isnull);
            if (isnull)
                ereport(ERROR,
                    (errcode(ERRCODE_UNEXPECTED_NULL_VALUE), errmsg("null conkey for constraint %u", constraintId)));

            decompile_column_index_array(val, conForm->conrelid, &buf);
            appendStringInfo(&buf, ")");
            break;
        }
        default:
            ereport(ERROR,
                (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE), errmsg("invalid constraint type \"%c\"", conForm->contype)));
            break;
    }

    if (conForm->condeferrable)
        appendStringInfo(&buf, " DEFERRABLE");
    if (conForm->condeferred)
        appendStringInfo(&buf, " INITIALLY DEFERRED");
    if (!conForm->convalidated)
        appendStringInfoString(&buf, " NOT VALID");

    /* Cleanup */
    ReleaseSysCache(tup);

    return buf.data;
}

/*
 * Convert an int16[] Datum into a comma-separated list of column names
 * for the indicated relation; append the list to buf.
 */
static void decompile_column_index_array(Datum column_index_array, Oid relId, StringInfo buf)
{
    Datum* keys = NULL;
    int nKeys;
    int j;

    /* Extract data from array of int16 */
    deconstruct_array(DatumGetArrayTypeP(column_index_array), INT2OID, 2, true, 's', &keys, NULL, &nKeys);

    for (j = 0; j < nKeys; j++) {
        char* colName = NULL;

        colName = get_relid_attribute_name(relId, DatumGetInt16(keys[j]));

        if (j == 0)
            appendStringInfoString(buf, quote_identifier(colName));
        else
            appendStringInfo(buf, ", %s", quote_identifier(colName));
    }
}

/* ----------
 * get_expr			- Decompile an expression tree
 *
 * Input: an expression tree in nodeToString form, and a relation OID
 *
 * Output: reverse-listed expression
 *
 * Currently, the expression can only refer to a single relation, namely
 * the one specified by the second parameter.  This is sufficient for
 * partial indexes, column default expressions, etc.  We also support
 * Var-free expressions, for which the OID can be InvalidOid.
 * ----------
 */
Datum pg_get_expr(PG_FUNCTION_ARGS)
{
    text* expr = PG_GETARG_TEXT_P(0);
    Oid relid = PG_GETARG_OID(1);
    char* relname = NULL;

    if (OidIsValid(relid)) {
        /* Get the name for the relation */
        relname = get_rel_name(relid);

        /*
         * If the OID isn't actually valid, don't throw an error, just return
         * NULL.  This is a bit questionable, but it's what we've done
         * historically, and it can help avoid unwanted failures when
         * examining catalog entries for just-deleted relations.
         */
        if (relname == NULL)
            PG_RETURN_NULL();
    } else
        relname = NULL;

    PG_RETURN_TEXT_P(pg_get_expr_worker(expr, relid, relname, 0));
}

Datum pg_get_expr_ext(PG_FUNCTION_ARGS)
{
    text* expr = PG_GETARG_TEXT_P(0);
    Oid relid = PG_GETARG_OID(1);
    bool pretty = PG_GETARG_BOOL(2);
    int prettyFlags;
    char* relname = NULL;

    prettyFlags = pretty ? (PRETTYFLAG_PAREN | PRETTYFLAG_INDENT) : 0;

    if (OidIsValid(relid)) {
        /* Get the name for the relation */
        relname = get_rel_name(relid);
        /* See notes above */
        if (relname == NULL)
            PG_RETURN_NULL();
    } else
        relname = NULL;

    PG_RETURN_TEXT_P(pg_get_expr_worker(expr, relid, relname, prettyFlags));
}

static text* pg_get_expr_worker(text* expr, Oid relid, const char* relname, int prettyFlags)
{
    Node* node = NULL;
    List* context = NIL;
    char* exprstr = NULL;
    char* str = NULL;

    /* Convert input TEXT object to C string */
    exprstr = text_to_cstring(expr);

    /* Convert expression to node tree */
    node = (Node*)stringToNode_skip_extern_fields(exprstr);

    pfree_ext(exprstr);

    /* Prepare deparse context if needed */
    if (OidIsValid(relid))
        context = deparse_context_for(relname, relid);
    else
        context = NIL;

    /* Deparse */
    str = deparse_expression_pretty(node, context, false, false, prettyFlags, 0);

    return string_to_text(str);
}

static inline bool IsUserVisible(Oid roleid)
{
    StringInfoData query;
    initStringInfo(&query);
    appendStringInfo(&query, "select oid from %s where oid = %u", "pg_roles", roleid);
    Oid oid = SearchSysTable(query.data);
    pfree_ext(query.data);
    return OidIsValid(oid);
}


/* ----------
 * get_userbyid			- Get a user name by roleid and
 *				  fallback to 'unknown (OID=n)'
 * ----------
 */
Datum pg_get_userbyid(PG_FUNCTION_ARGS)
{
    Oid roleid = PG_GETARG_OID(0);
    Name result;
    HeapTuple roletup;
    Form_pg_authid role_rec;
    errno_t rc;

    /* check table acl if enable private object like AlterDatabasePrivateObject */
    if (!IsUserVisible(roleid)) {
        PG_RETURN_NULL();
    }

    /*
     * Allocate space for the result
     */
    result = (Name)palloc(NAMEDATALEN);
    rc = memset_s(NameStr(*result), NAMEDATALEN, 0, NAMEDATALEN);
    securec_check(rc, "\0", "\0");

    /*
     * Get the pg_authid entry and print the result
     */
    roletup = SearchSysCache1(AUTHOID, ObjectIdGetDatum(roleid));
    if (HeapTupleIsValid(roletup)) {
        role_rec = (Form_pg_authid)GETSTRUCT(roletup);
        rc = strncpy_s(NameStr(*result), NAMEDATALEN, NameStr(role_rec->rolname), NAMEDATALEN - 1);
        securec_check(rc, "\0", "\0");
        ReleaseSysCache(roletup);
    } else {
        rc = sprintf_s(NameStr(*result), NAMEDATALEN, "unknown (OID=%u)", roleid);
        securec_check_ss(rc, "\0", "\0");
    }

    PG_RETURN_NAME(result);
}

/* ----------
 * pg_check_authid		- Check a user by roleid whether exist
 * ----------
 */
Datum pg_check_authid(PG_FUNCTION_ARGS)
{
    Oid authid = PG_GETARG_OID(0);
    HeapTuple roletup;

    /*
     * Get the pg_authid entry and print the result
     */
    roletup = SearchSysCache1(AUTHOID, ObjectIdGetDatum(authid));
    if (HeapTupleIsValid(roletup)) {
        ReleaseSysCache(roletup);
        PG_RETURN_BOOL(true);
    } else {
        PG_RETURN_BOOL(false);
    }
}

/*
 * pg_get_serial_sequence
 *		Get the name of the sequence used by a serial column,
 *		formatted suitably for passing to setval, nextval or currval.
 *		First parameter is not treated as double-quoted, second parameter
 *		is --- see documentation for reason.
 */
Datum pg_get_serial_sequence(PG_FUNCTION_ARGS)
{
    text* tablename = PG_GETARG_TEXT_P(0);
    text* columnname = PG_GETARG_TEXT_PP(1);
    RangeVar* tablerv = NULL;
    Oid tableOid;
    char* column = NULL;
    AttrNumber attnum;
    Oid sequenceId = InvalidOid;
    Relation depRel;
    ScanKeyData key[3];
    SysScanDesc scan;
    HeapTuple tup;
    List* names = NIL;

    /* Look up table name.	Can't lock it - we might not have privileges. */
    names = textToQualifiedNameList(tablename);
    tablerv = makeRangeVarFromNameList(names);
    tableOid = RangeVarGetRelid(tablerv, NoLock, false);

    /* Get the number of the column */
    column = text_to_cstring(columnname);
    attnum = get_attnum(tableOid, column);
    if (attnum == InvalidAttrNumber)
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_COLUMN),
                errmsg("column \"%s\" of relation \"%s\" does not exist", column, tablerv->relname)));

    /* Search the dependency table for the dependent sequence */
    depRel = heap_open(DependRelationId, AccessShareLock);

    ScanKeyInit(
        &key[0], Anum_pg_depend_refclassid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(RelationRelationId));
    ScanKeyInit(&key[1], Anum_pg_depend_refobjid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(tableOid));
    ScanKeyInit(&key[2], Anum_pg_depend_refobjsubid, BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(attnum));

    scan = systable_beginscan(depRel, DependReferenceIndexId, true, NULL, 3, key);

    while (HeapTupleIsValid(tup = systable_getnext(scan))) {
        Form_pg_depend deprec = (Form_pg_depend)GETSTRUCT(tup);

        /*
         * We assume any auto dependency of a sequence on a column must be
         * what we are looking for.  (We need the relkind test because indexes
         * can also have auto dependencies on columns.)
         */
        if (deprec->classid == RelationRelationId && deprec->objsubid == 0 && deprec->deptype == DEPENDENCY_AUTO &&
            RELKIND_IS_SEQUENCE(get_rel_relkind(deprec->objid))) {
            sequenceId = deprec->objid;
            break;
        }
    }

    systable_endscan(scan);
    heap_close(depRel, AccessShareLock);

    if (OidIsValid(sequenceId)) {
        HeapTuple classtup;
        Form_pg_class classtuple;
        char* nspname = NULL;
        char* result = NULL;

        /* Get the sequence's pg_class entry */
        classtup = SearchSysCache1(RELOID, ObjectIdGetDatum(sequenceId));
        if (!HeapTupleIsValid(classtup))
            ereport(ERROR,
                (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for relation %u", sequenceId)));

        classtuple = (Form_pg_class)GETSTRUCT(classtup);

        /* Get the namespace */
        nspname = get_namespace_name(classtuple->relnamespace);
        if (nspname == NULL)
            ereport(ERROR,
                (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                    errmsg("cache lookup failed for namespace %u", classtuple->relnamespace)));

        /* And construct the result string */
        result = quote_qualified_identifier(nspname, NameStr(classtuple->relname));

        ReleaseSysCache(classtup);
        list_free_ext(names);

        PG_RETURN_TEXT_P(string_to_text(result));
    }

    list_free_ext(names);
    PG_RETURN_NULL();
}

static inline bool IsFunctionVisible(Oid funcoid)
{
    StringInfoData query;
    initStringInfo(&query);
    appendStringInfo(&query, "select pg_namespace.oid from pg_namespace where pg_namespace.oid in "
        "(select pronamespace from pg_proc where pg_proc.oid = %u);", funcoid);
    Oid oid = SearchSysTable(query.data);
    pfree_ext(query.data);
    return OidIsValid(oid);
}

/*
 * pg_get_functiondef
 *		Returns the complete "CREATE OR REPLACE FUNCTION ..." statement for
 *		the specified function.
 *
 * Note: if you change the output format of this function, be careful not
 * to break psql's rules (in \ef and \sf) for identifying the start of the
 * function body.  To wit: the function body starts on a line that begins
 * with "AS ", and no preceding line will look like that.
 */
Datum pg_get_functiondef(PG_FUNCTION_ARGS)
{
    Oid funcid = PG_GETARG_OID(0);
    if (!IsFunctionVisible(funcid)) {
        PG_RETURN_NULL();
    }
    char* funcdef = NULL;
    TupleDesc tupdesc;
    HeapTuple tuple;
    Datum values[2];
    bool nulls[2];
    int headerlines = 0;
    errno_t rc = EOK;

    tupdesc = CreateTemplateTupleDesc(2, false, TAM_HEAP);
    TupleDescInitEntry(tupdesc, (AttrNumber)1, "headerlines", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)2, "definition", TEXTOID, -1, 0);
    (void)BlessTupleDesc(tupdesc);
    rc = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
    securec_check_c(rc, "\0", "\0");

    funcdef = pg_get_functiondef_worker(funcid, &headerlines);

    values[0] = Int32GetDatum((int32)(headerlines));
    values[1] = PointerGetDatum(string_to_text(funcdef));

    tuple = heap_form_tuple(tupdesc, values, nulls);
    PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}

/*
 * pg_get_function_arguments
 *		Get a nicely-formatted list of arguments for a function.
 *		This is everything that would go between the parentheses in
 *		CREATE FUNCTION.
 */
Datum pg_get_function_arguments(PG_FUNCTION_ARGS)
{
    Oid funcid = PG_GETARG_OID(0);
    if (!IsFunctionVisible(funcid)) {
        PG_RETURN_NULL();
    }
    StringInfoData buf;
    HeapTuple proctup;

    initStringInfo(&buf);

    proctup = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
    if (!HeapTupleIsValid(proctup))
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for function %u", funcid)));

    (void)print_function_arguments(&buf, proctup, false, true);

    ReleaseSysCache(proctup);

    PG_RETURN_TEXT_P(string_to_text(buf.data));
}

/*
 * pg_get_function_identity_arguments
 *		Get a formatted list of arguments for a function.
 *		This is everything that would go between the parentheses in
 *		ALTER FUNCTION, etc.  In particular, don't print defaults.
 */
Datum pg_get_function_identity_arguments(PG_FUNCTION_ARGS)
{
    Oid funcid = PG_GETARG_OID(0);
    if (!IsFunctionVisible(funcid)) {
        PG_RETURN_NULL();
    }
    StringInfoData buf;
    HeapTuple proctup;

    initStringInfo(&buf);

    proctup = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
    if (!HeapTupleIsValid(proctup))
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for function %u", funcid)));

    (void)print_function_arguments(&buf, proctup, false, false);

    ReleaseSysCache(proctup);

    PG_RETURN_TEXT_P(string_to_text(buf.data));
}

/*
 * pg_get_function_result
 *		Get a nicely-formatted version of the result type of a function.
 *		This is what would appear after RETURNS in CREATE FUNCTION.
 */
Datum pg_get_function_result(PG_FUNCTION_ARGS)
{
    Oid funcid = PG_GETARG_OID(0);
    if (!IsFunctionVisible(funcid)) {
        PG_RETURN_NULL();
    }
    StringInfoData buf;
    HeapTuple proctup;

    initStringInfo(&buf);

    proctup = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
    if (!HeapTupleIsValid(proctup))
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for function %u", funcid)));

    print_function_rettype(&buf, proctup);

    ReleaseSysCache(proctup);

    PG_RETURN_TEXT_P(string_to_text(buf.data));
}

char* pg_get_functiondef_worker(Oid funcid, int* headerlines)
{
    StringInfoData buf;
    StringInfoData dq;
    HeapTuple proctup;
    HeapTuple langtup;
    Form_pg_proc proc;
    Form_pg_language lang;
    Datum tmp;
    bool isnull = false;
    const char* prosrc = NULL;
    const char* name = NULL;
    const char* nsp = NULL;
    float4 procost;
    int oldlen;
    char* p = NULL;
    bool isOraFunc = false;
    NameData* pkgname = NULL;
    initStringInfo(&buf);

    /* Look up the function */
    proctup = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
    if (!HeapTupleIsValid(proctup))
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for function %u", funcid)));

    proc = (Form_pg_proc)GETSTRUCT(proctup);
    name = NameStr(proc->proname);

    if (proc->proisagg)
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("\"%s\" is an aggregate function", name)));

    /* Need its pg_language tuple for the language name */
    langtup = SearchSysCache1(LANGOID, ObjectIdGetDatum(proc->prolang));
    if (!HeapTupleIsValid(langtup))
        ereport(ERROR,
            (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for language %u", proc->prolang)));
    lang = (Form_pg_language)GETSTRUCT(langtup);

    /*
     * We always qualify the function name, to ensure the right function gets
     * replaced.
     */
    nsp = get_namespace_name(proc->pronamespace);
  

    bool proIsProcedure = false;
    if (t_thrd.proc->workingVersionNum >= STP_SUPPORT_COMMIT_ROLLBACK) {
        Datum datum = SysCacheGetAttr(PROCOID, proctup, Anum_pg_proc_prokind, &isnull);
        proIsProcedure = isnull ? false : PROC_IS_PRO(CharGetDatum(datum));
    }
 
    Datum packageOidDatum = SysCacheGetAttr(PROCOID, proctup, Anum_pg_proc_packageid, &isnull);
    if (!isnull)
    {
        Oid packageoid = DatumGetObjectId(packageOidDatum);
        if (OidIsValid(packageoid)) {
            pkgname = GetPackageName(packageoid);
        }
    } 

    if (proIsProcedure) {
        if (pkgname != NULL) {
            appendStringInfo(&buf, "CREATE OR REPLACE PROCEDURE %s(",
                                quote_qualified_identifier(nsp, pkgname->data, name));
        } else {
            appendStringInfo(&buf, "CREATE OR REPLACE PROCEDURE %s(", 
                                quote_qualified_identifier(nsp, name));
        }
    } else {
        if (pkgname != NULL) {
            appendStringInfo(&buf, "CREATE OR REPLACE FUNCTION %s(", 
                                quote_qualified_identifier(nsp, pkgname->data, name));
        } else {
            appendStringInfo(&buf, "CREATE OR REPLACE FUNCTION %s(", 
                                quote_qualified_identifier(nsp, name));
        }
    }

    if (t_thrd.proc->workingVersionNum >= COMMENT_PROC_VERSION_NUM) {
        tmp = SysCacheGetAttr(PROCOID, proctup, Anum_pg_proc_proargsrc, &isnull);
        if (isnull) {
            (void)print_function_arguments(&buf, proctup, false, true);
        } else {
            print_function_ora_arguments(&buf, proctup);
            isOraFunc = true;
        }
    } else {
        (void)print_function_arguments(&buf, proctup, false, true);
    }

    if (!proIsProcedure) {
        if (!isOraFunc) {
            appendStringInfoString(&buf, ")\n RETURNS ");
        } else {
            appendStringInfoString(&buf, ")\n RETURN ");
        }
    } else {
        appendStringInfoString(&buf, ")\n");
    }
    if (!proIsProcedure) {
        print_function_rettype(&buf, proctup);
        if (!isOraFunc) {
            appendStringInfo(&buf, "\n LANGUAGE %s\n", quote_identifier(NameStr(lang->lanname)));
        }
    }
    /* Emit some miscellaneous options on one line */
    oldlen = buf.len;
    if (proc->proiswindow)
        appendStringInfoString(&buf, " WINDOW");
    switch (proc->provolatile) {
        case PROVOLATILE_IMMUTABLE:
            appendStringInfoString(&buf, " IMMUTABLE");
            break;
        case PROVOLATILE_STABLE:
            appendStringInfoString(&buf, " STABLE");
            break;
        case PROVOLATILE_VOLATILE:
            break;
        default:
            break;
    }  
    if (proc->proisstrict)
        appendStringInfoString(&buf, " STRICT"); 
    if (PLSQL_SECURITY_DEFINER) {
        if (proc->prosecdef) {
            appendStringInfoString(&buf, " AUTHID DEFINER");
        } else {
            appendStringInfoString(&buf, " AUTHID CURRENT_USER");
        }
    } else {
        if (proc->prosecdef)
            appendStringInfoString(&buf, " SECURITY DEFINER");
    }
    if (proc->proleakproof)
        appendStringInfoString(&buf, " LEAKPROOF");

    Datum fenced = SysCacheGetAttr(PROCOID, proctup, Anum_pg_proc_fenced, &isnull);
    if (!isnull && DatumGetBool(fenced))
        appendStringInfoString(&buf, " FENCED");
    else if (!proIsProcedure)
        appendStringInfoString(&buf, " NOT FENCED");

    Datum shippable = SysCacheGetAttr(PROCOID, proctup, Anum_pg_proc_shippable, &isnull);
    if (!isnull && DatumGetBool(shippable))
        appendStringInfoString(&buf, " SHIPPABLE");
    else if (!proIsProcedure)
        appendStringInfoString(&buf, " NOT SHIPPABLE");

    Datum propackage = SysCacheGetAttr(PROCOID, proctup, Anum_pg_proc_package, &isnull);
    if (!isnull && DatumGetBool(propackage))
        appendStringInfoString(&buf, " PACKAGE");
    
    /* This code for the default cost and rows should match functioncmds.c */
    if (proc->prolang == INTERNALlanguageId || proc->prolang == ClanguageId)
        procost = 1;
    else
        procost = 100;
    if (proc->procost != procost)
        appendStringInfo(&buf, " COST %g", proc->procost);

    if (proc->prorows > 0 && proc->prorows != 1000)
        appendStringInfo(&buf, " ROWS %g", proc->prorows);

    if (oldlen != buf.len)
        appendStringInfoChar(&buf, '\n');

    /* Emit any proconfig options, one per line */
    tmp = SysCacheGetAttr(PROCOID, proctup, Anum_pg_proc_proconfig, &isnull);
    if (!isnull) {
        ArrayType* a = DatumGetArrayTypeP(tmp);
        int i;

        Assert(ARR_ELEMTYPE(a) == TEXTOID);
        Assert(ARR_NDIM(a) == 1);
        Assert(ARR_LBOUND(a)[0] == 1);

        for (i = 1; i <= ARR_DIMS(a)[0]; i++) {
            Datum d;

            d = array_ref(a,
                1,
                &i,
                -1 /* varlenarray */,
                -1 /* TEXT's typlen */,
                false /* TEXT's typbyval */,
                'i' /* TEXT's typalign */,
                &isnull);
            if (!isnull) {
                char* configitem = TextDatumGetCString(d);
                char* pos = NULL;

                pos = strchr(configitem, '=');
                if (pos == NULL)
                    continue;
                *pos++ = '\0';

                appendStringInfo(&buf, " SET %s TO ", quote_identifier(configitem));

                /*
                 * Some GUC variable names are 'LIST' type and hence must not
                 * be quoted.
                 */
                if (pg_strcasecmp(configitem, "DateStyle") == 0 || pg_strcasecmp(configitem, "search_path") == 0)
                    appendStringInfoString(&buf, pos);
                else
                    simple_quote_literal(&buf, pos);
                appendStringInfoChar(&buf, '\n');
            }
        }
    }

    /* And finally the function definition ... */
    appendStringInfoString(&buf, "AS ");

    tmp = SysCacheGetAttr(PROCOID, proctup, Anum_pg_proc_probin, &isnull);
    if (!isnull) {
        simple_quote_literal(&buf, TextDatumGetCString(tmp));
        appendStringInfoString(&buf, ", "); /* assume prosrc isn't null */
    }

    tmp = SysCacheGetAttr(PROCOID, proctup, Anum_pg_proc_prosrc, &isnull);
    if (isnull)
        ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NULL_VALUE), errmsg("null prosrc")));
    prosrc = TextDatumGetCString(tmp);

    /*
     * We always use dollar quoting.  Figure out a suitable delimiter.
     *
     * Since the user is likely to be editing the function body string, we
     * shouldn't use a short delimiter that he might easily create a conflict
     * with.  Hence prefer "$function$", but extend if needed.
     */
    initStringInfo(&dq);
    if ((!proIsProcedure) && (!isOraFunc)) {
        appendStringInfoString(&dq, "$function");
  
        while (strstr(prosrc, dq.data) != NULL)
            appendStringInfoChar(&dq, 'x');
        appendStringInfoChar(&dq, '$');
    } 
    appendStringInfoString(&buf, dq.data);
    // Count the header line numbers.
    //
    for (p = buf.data; *p; p++)
        if (*p == '\n')
            (*headerlines)++;

    appendStringInfoString(&buf, prosrc);
    appendStringInfoString(&buf, dq.data);
    if (proIsProcedure || isOraFunc) {
        appendStringInfoString(&buf, ";\n/");
    } else {
        appendStringInfoString(&buf, ";");
    }
    appendStringInfoString(&buf, "\n");

    ReleaseSysCache(langtup);
    ReleaseSysCache(proctup);

    pfree_ext(dq.data);

    return buf.data;
}

/*
 * Guts of pg_get_function_result: append the function's return type
 * to the specified buffer.
 */
static void print_function_rettype(StringInfo buf, HeapTuple proctup)
{
    Form_pg_proc proc = (Form_pg_proc)GETSTRUCT(proctup);
    int ntabargs = 0;
    StringInfoData rbuf;
    bool is_client_logic = false;

    initStringInfo(&rbuf);

    if (proc->proretset) {
        /* It might be a table function; try to print the arguments */
        appendStringInfoString(&rbuf, "TABLE(");
        ntabargs = print_function_arguments(&rbuf, proctup, true, false);
        if (ntabargs > 0)
            appendStringInfoString(&rbuf, ")");
        else
            resetStringInfo(&rbuf);
    }
    Oid ret_type_id = proc->prorettype;
    if (ntabargs == 0) {
        /* Not a table function, so do the normal thing */
        if (proc->proretset)
            appendStringInfoString(&rbuf, "SETOF ");
        if (IsClientLogicType(proc->prorettype)) {
            /* replace with original type */
            HeapTuple gs_oldtup = SearchSysCache1(GSCLPROCID, ObjectIdGetDatum(HeapTupleGetOid(proctup)));
            if (HeapTupleIsValid(gs_oldtup)) {
                bool isnull = false;
                Datum rettype_orig =
                    SysCacheGetAttr(GSCLPROCID, gs_oldtup, Anum_gs_encrypted_proc_prorettype_orig, &isnull);
                if (!isnull) {
                    ret_type_id = DatumGetObjectId(rettype_orig);
                    is_client_logic = true;
                }
                ReleaseSysCache(gs_oldtup);
            }
        }
        appendStringInfoString(&rbuf, format_type_be(ret_type_id));
        if (is_client_logic) {
            appendStringInfoString(&rbuf, " encrypted");
        }
    }

    appendStringInfoString(buf, rbuf.data);
}

/*
 * get param string from pg_proc_proargsrc.
 */
static void print_function_ora_arguments(StringInfo buf, HeapTuple proctup)
{
    Datum tmp;
    bool isnull = false;
    const char* proargsrc = NULL;

    tmp = SysCacheGetAttr(PROCOID, proctup, Anum_pg_proc_proargsrc, &isnull);
    Assert(!isnull);
    proargsrc = TextDatumGetCString(tmp);

    appendStringInfoString(buf, proargsrc);
}

/*
 * Common code for pg_get_function_arguments and pg_get_function_result:
 * append the desired subset of arguments to buf.  We print only TABLE
 * arguments when print_table_args is true, and all the others when it's false.
 * We print argument defaults only if print_defaults is true.
 * Function return value is the number of arguments printed.
 */
static int print_function_arguments(StringInfo buf, HeapTuple proctup, bool print_table_args, bool print_defaults)
{
    Form_pg_proc proc = (Form_pg_proc)GETSTRUCT(proctup);
    int numargs;
    Oid* argtypes = NULL;
    char** argnames = NULL;
    char* argmodes = NULL;
    int insertorderbyat = -1;
    int argsprinted;
    int inputargno;
    ListCell* nextargdefault = NULL;
    int i;
    Datum defposdatum;
    int2vector* defpos = NULL;
    int counter = 0;
    bool *is_client_logic = NULL;

    numargs = get_func_arg_info(proctup, &argtypes, &argnames, &argmodes);
    if (numargs) {
        is_client_logic = (bool*)palloc0(numargs * sizeof(bool));
    }
    replace_cl_types_in_argtypes(HeapTupleGetOid(proctup), numargs, argtypes, is_client_logic);

    if (print_defaults && proc->pronargdefaults > 0) {
        Datum proargdefaults;
        bool isnull = false;
        char* str = NULL;
        List* argdefaults = NIL;

        proargdefaults = SysCacheGetAttr(PROCOID, proctup, Anum_pg_proc_proargdefaults, &isnull);
        Assert(!isnull);
        str = TextDatumGetCString(proargdefaults);
        argdefaults = (List*)stringToNode(str);
        Assert(IsA(argdefaults, List));
        pfree_ext(str);
        nextargdefault = list_head(argdefaults);
        /* nlackdefaults counts only *input* arguments lacking defaults */

        if (proc->pronargs <= FUNC_MAX_ARGS_INROW) {
            defposdatum = SysCacheGetAttr(PROCOID, proctup, Anum_pg_proc_prodefaultargpos, &isnull);
            Assert(!isnull);
            defpos = (int2vector*)DatumGetPointer(defposdatum);
        } else {
            defposdatum = SysCacheGetAttr(PROCOID, proctup, Anum_pg_proc_prodefaultargposext, &isnull);
            Assert(!isnull);
            defpos = (int2vector*)PG_DETOAST_DATUM(defposdatum);
        }
    }

    /* Check for special treatment of ordered-set aggregates */
    if (proc->proisagg) {
        Oid proctupoid;
        HeapTuple aggtup;
        Form_pg_aggregate aggform;

        proctupoid = HeapTupleGetOid(proctup);
        aggtup = SearchSysCache1(AGGFNOID, ObjectIdGetDatum(proctupoid));
        if (!HeapTupleIsValid(aggtup))
            ereport(ERROR, (errmodule(MOD_OPT), (errmsg("cache lookup failed for aggregate %u", proctupoid))));
        aggform = (Form_pg_aggregate)GETSTRUCT(aggtup);
        if (AGGKIND_IS_ORDERED_SET(aggform->aggkind))
            insertorderbyat = aggform->aggnumdirectargs;
        ReleaseSysCache(aggtup);
    }

    argsprinted = 0;
    inputargno = 0;
    for (i = 0; i < numargs; i++) {
        Oid argtype = argtypes[i];
        char* argname = argnames ? argnames[i] : NULL;
        char argmode = argmodes ? argmodes[i] : PROARGMODE_IN;
        const char* modename = NULL;
        bool isinput = false;

        switch (argmode) {
            case PROARGMODE_IN:
                modename = "";
                isinput = true;
                break;
            case PROARGMODE_INOUT:
                modename = "INOUT ";
                isinput = true;
                break;
            case PROARGMODE_OUT:
                modename = "OUT ";
                isinput = false;
                break;
            case PROARGMODE_VARIADIC:
                modename = "VARIADIC ";
                isinput = true;
                break;
            case PROARGMODE_TABLE:
                modename = "";
                isinput = false;
                break;
            default:
                ereport(
                    ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid parameter mode '%c'", argmode)));
                modename = NULL; /* keep compiler quiet */
                isinput = false;
                break;
        }
        if (isinput)
            inputargno++; /* this is a 1-based counter */

        if (print_table_args != (argmode == PROARGMODE_TABLE))
            continue;

        if (argsprinted == insertorderbyat) {
            if (argsprinted)
                appendStringInfoChar(buf, ' ');
            appendStringInfoString(buf, "ORDER BY ");
        } else if (argsprinted)
            appendStringInfoString(buf, ", ");
        appendStringInfoString(buf, modename);
        if ((argname != NULL) && argname[0])
            appendStringInfo(buf, "%s ", quote_identifier(argname));
        appendStringInfoString(buf, format_type_be(argtype));
        if (is_client_logic[i]) {
            appendStringInfoString(buf, " encrypted");
        }

        /*
         * fetch default value for some argument
         */
        if (print_defaults && isinput && (defpos != NULL) &&
            counter < defpos->dim1  // avoid the subscript out of bounds
            && (argsprinted == defpos->values[counter])) {
            Node* expr = NULL;

            Assert(nextargdefault != NULL);
            expr = (Node*)lfirst(nextargdefault);
            nextargdefault = lnext(nextargdefault);

            appendStringInfo(buf, " DEFAULT %s", deparse_expression(expr, NIL, false, false));
            counter++;
        }

        argsprinted++;

        /* nasty hack: print the last arg twice for variadic ordered-set agg */
        if (argsprinted == insertorderbyat && i == numargs - 1) {
            i--;
            /* aggs shouldn't have defaults anyway, but just to be sure ... */
            print_defaults = false;
        }
    }
    pfree_ext(is_client_logic);
    return argsprinted;
}

/*
 * deparse_expression			- General utility for deparsing expressions
 *
 * calls deparse_expression_pretty with all prettyPrinting disabled
 */
char* deparse_expression(Node* expr, List* dpcontext, bool forceprefix, bool showimplicit, bool no_alias)
{
    return deparse_expression_pretty(expr, dpcontext, forceprefix, showimplicit, 0, 0, no_alias);
}

/* ----------
 * deparse_expression_pretty	- General utility for deparsing expressions
 *
 * expr is the node tree to be deparsed.  It must be a transformed expression
 * tree (ie, not the raw output of gram.y).
 *
 * dpcontext is a list of deparse_namespace nodes representing the context
 * for interpreting Vars in the node tree.
 *
 * forceprefix is TRUE to force all Vars to be prefixed with their table names.
 *
 * showimplicit is TRUE to force all implicit casts to be shown explicitly.
 *
 * tries to pretty up the output according to prettyFlags and startIndent.
 *
 * The result is a palloc'd string.
 * ----------
 */
static char* deparse_expression_pretty(
    Node* expr, List* dpcontext, bool forceprefix, bool showimplicit, int prettyFlags, int startIndent, bool no_alias)
{
    StringInfoData buf;
    deparse_context context;

    initStringInfo(&buf);
    context.buf = &buf;
    context.namespaces = dpcontext;
    context.windowClause = NIL;
    context.windowTList = NIL;
    context.varprefix = forceprefix;
    context.prettyFlags = prettyFlags;
#ifdef PGXC
    context.finalise_aggs = false;
    context.sortgroup_colno = false;
    context.parser_arg = NULL;
    context.viewdef = false;
    context.is_fqs = false;
#endif /* PGXC */
    context.wrapColumn = WRAP_COLUMN_DEFAULT;
    context.indentLevel = startIndent;
    context.qrw_phase = false;
    context.is_upsert_clause = false;
    get_rule_expr(expr, &context, showimplicit, no_alias);

    return buf.data;
}

/* ----------
 * deparse_context_for			- Build deparse context for a single relation
 *
 * Given the reference name (alias) and OID of a relation, build deparsing
 * context for an expression referencing only that relation (as varno 1,
 * varlevelsup 0).	This is sufficient for many uses of deparse_expression.
 * ----------
 */
List* deparse_context_for(const char* aliasname, Oid relid)
{
    deparse_namespace* dpns = NULL;
    RangeTblEntry* rte = NULL;

    dpns = (deparse_namespace*)palloc0(sizeof(deparse_namespace));

    /* Build a minimal RTE for the rel */
    rte = makeNode(RangeTblEntry);
    rte->rtekind = RTE_RELATION;
    rte->relid = relid;
    rte->relkind = RELKIND_RELATION; /* no need for exactness here */
    rte->eref = makeAlias(aliasname, NIL);
    rte->lateral = false;
    rte->inh = false;
    rte->inFromCl = true;

    /* Build one-element rtable */
    dpns->rtable = list_make1(rte);
    dpns->ctes = NIL;
#ifdef PGXC
    dpns->remotequery = false;
#endif

    /* Return a one-deep namespace stack */
    return list_make1(dpns);
}

/*
 * deparse_context_for_planstate	- Build deparse context for a plan
 *
 * When deparsing an expression in a Plan tree, we might have to resolve
 * OUTER_VAR, INNER_VAR, or INDEX_VAR references.  To do this, the caller must
 * provide the parent PlanState node.  Then OUTER_VAR and INNER_VAR references
 * can be resolved by drilling down into the left and right child plans.
 * Similarly, INDEX_VAR references can be resolved by reference to the
 * indextlist given in the parent IndexOnlyScan node.  (Note that we don't
 * currently support deparsing of indexquals in regular IndexScan or
 * BitmapIndexScan nodes; for those, we can only deparse the indexqualorig
 * fields, which won't contain INDEX_VAR Vars.)
 *
 * Note: planstate really ought to be declared as "PlanState *", but we use
 * "Node *" to avoid having to include execnodes.h in builtins.h.
 *
 * The ancestors list is a list of the PlanState's parent PlanStates, the
 * most-closely-nested first.  This is needed to resolve PARAM_EXEC Params.
 * Note we assume that all the PlanStates share the same rtable.
 *
 * The plan's rangetable list must also be passed.  We actually prefer to use
 * the rangetable to resolve simple Vars, but the plan inputs are necessary
 * for Vars with special varnos.
 */
List* deparse_context_for_planstate(Node* planstate, List* ancestors, List* rtable)
{
    deparse_namespace* dpns = NULL;

    dpns = (deparse_namespace*)palloc0(sizeof(deparse_namespace));

    /* Initialize fields that stay the same across the whole plan tree */
    dpns->rtable = rtable;
    dpns->ctes = NIL;
#ifdef PGXC
    dpns->remotequery = false;
#endif

    /* Set our attention on the specific plan node passed in */
    set_deparse_planstate(dpns, (PlanState*)planstate);
    dpns->ancestors = ancestors;

    /* Return a one-deep namespace stack */
    return list_make1(dpns);
}

/*
 * set_deparse_planstate: set up deparse_namespace to parse subexpressions
 * of a given PlanState node
 *
 * This sets the planstate, outer_planstate, inner_planstate, outer_tlist,
 * inner_tlist, and index_tlist fields.  Caller is responsible for adjusting
 * the ancestors list if necessary.  Note that the rtable and ctes fields do
 * not need to change when shifting attention to different plan nodes in a
 * single plan tree.
 */
static void set_deparse_planstate(deparse_namespace* dpns, PlanState* ps)
{
    dpns->planstate = ps;

    /*
     * We special-case Append and MergeAppend to pretend that the first child
     * plan is the OUTER referent; we have to interpret OUTER Vars in their
     * tlists according to one of the children, and the first one is the most
     * natural choice.	Likewise special-case ModifyTable to pretend that the
     * first child plan is the OUTER referent; this is to support RETURNING
     * lists containing references to non-target relations.
     */
    if (IsA(ps, AppendState))
        dpns->outer_planstate = ((AppendState*)ps)->appendplans[0];
    else if (IsA(ps, VecAppendState))
        dpns->outer_planstate = ((VecAppendState*)ps)->appendplans[0];
    else if (IsA(ps, MergeAppendState))
        dpns->outer_planstate = ((MergeAppendState*)ps)->mergeplans[0];
    else if (IsA(ps, ModifyTableState))
        dpns->outer_planstate = ((ModifyTableState*)ps)->mt_plans[0];
    else if (IsA(ps, VecModifyTableState))
        dpns->outer_planstate = ((VecModifyTableState*)ps)->mt_plans[0];
    else
        dpns->outer_planstate = outerPlanState(ps);

    if (dpns->outer_planstate != NULL)
        dpns->outer_tlist = dpns->outer_planstate->plan->targetlist;
    else
        dpns->outer_tlist = NIL;

    /*
     * For a SubqueryScan, pretend the subplan is INNER referent.  (We don't
     * use OUTER because that could someday conflict with the normal meaning.)
     * Likewise, for a CteScan, pretend the subquery's plan is INNER referent.
     * For DUPLICATE KEY UPDATE we just need the inner tlist to point to the
     * excluded expression's tlist. (Similar to the SubqueryScan we don't want
     * to reuse OUTER, it's used for RETURNING in some modify table cases,
     * although not INSERT ... ON DUPLICATE KEY UPDATE).
     */
    if (IsA(ps, SubqueryScanState))
        dpns->inner_planstate = ((SubqueryScanState*)ps)->subplan;
    else if (IsA(ps, VecSubqueryScanState))
        dpns->inner_planstate = ((VecSubqueryScanState*)ps)->subplan;
    else if (IsA(ps, CteScanState))
        dpns->inner_planstate = ((CteScanState*)ps)->cteplanstate;
    else if (IsA(ps, ModifyTableState) || IsA(ps, VecModifyTableState) || IsA(ps, DistInsertSelectState)) {
        ModifyTableState* mps = (ModifyTableState*)ps;
        dpns->outer_planstate = mps->mt_plans[0];

        dpns->inner_planstate = ps;
        /*
         * For merge into, we should deparse the inner plan,  since the targetlist and qual will
         * reference sourceTargetList, which comes from outer plan of the join (source table)
         */
        if (mps->operation == CMD_MERGE) {
            PlanState* jplanstate = dpns->outer_planstate;
            if (IsA(jplanstate, StreamState) || IsA(jplanstate, VecStreamState))
                jplanstate = jplanstate->lefttree;
            if (jplanstate->plan != NULL && IsJoinPlan((Node*)jplanstate->plan) &&
                ((Join*)jplanstate->plan)->jointype == JOIN_RIGHT) {
                dpns->inner_planstate = innerPlanState(jplanstate);
            } else {
                dpns->inner_planstate = outerPlanState(jplanstate);
            }
        }
    } else
        dpns->inner_planstate = innerPlanState(ps);

    if (IsA(ps, ModifyTableState) && ((ModifyTableState*)ps)->mt_upsert->us_excludedtlist != NIL) {
        /* For upsert deparse state. The second condition is somewhat ad-hoc but there's no flag to
         * mark upsert clause under PlanState.
         */
        dpns->inner_tlist = ((ModifyTableState*)ps)->mt_upsert->us_excludedtlist;
    } else if (dpns->inner_planstate != NULL) {
        if ((IsA(ps, ModifyTableState) || IsA(ps, VecModifyTableState) || IsA(ps, DistInsertSelectState)) &&
            ((ModifyTableState *)ps)->operation == CMD_MERGE) {
            /* For merge into statements, source relation is always the inner one. */
            dpns->inner_tlist = ((ModifyTable*)(ps->plan))->mergeSourceTargetList;
        } else {
            dpns->inner_tlist = dpns->inner_planstate->plan->targetlist;
        }
    } else {
        dpns->inner_tlist = NIL;
    }

    /* index_tlist is set only if it's an IndexOnlyScan */
    if (IsA(ps->plan, IndexOnlyScan))
        dpns->index_tlist = ((IndexOnlyScan*)ps->plan)->indextlist;
    else if (IsA(ps->plan, ExtensiblePlan))
        dpns->index_tlist = ((ExtensiblePlan*)ps->plan)->extensible_plan_tlist;
    else
        dpns->index_tlist = NIL;
}

#ifdef PGXC
/*
 * This is a special case deparse context to be used at the planning time to
 * generate query strings and expressions for remote shipping.
 *
 * XXX We should be careful while using this since the support is quite
 * limited. The only supported use case at this point is for remote join
 * reduction and some simple plan trees rooted by Agg node having a single
 * RemoteQuery node as leftree.
 */
List* deparse_context_for_plan(Node* plan, List* ancestors, List* rtable)
{
    deparse_namespace* dpns = NULL;

    dpns = (deparse_namespace*)palloc0(sizeof(deparse_namespace));

    /* Initialize fields that stay the same across the whole plan tree */
    dpns->rtable = rtable;
    dpns->ctes = NIL;
    dpns->remotequery = false;

    /* Set our attention on the specific plan node passed in */
    set_deparse_plan(dpns, (Plan*)plan);
    dpns->ancestors = ancestors;

    /* Return a one-deep namespace stack */
    return list_make1(dpns);
}

/*
 * Set deparse context for Plan. Only those plan nodes which are immediate (or
 * through simple nodes) parents of RemoteQuery nodes are supported right now.
 *
 * This is a kind of work-around since the new deparse interface (since 9.1)
 * expects a PlanState node. But planstates are instantiated only at execution
 * time when InitPlan is called. But we are required to deparse the query
 * during planning time, so we hand-cook these dummy PlanState nodes instead of
 * init-ing the plan. Another approach could have been to delay the query
 * generation to the execution time, but we are not yet sure if this can be
 * safely done, especially for remote join reduction.
 */
static void set_deparse_plan(deparse_namespace* dpns, Plan* plan)
{

    if (IsA(plan, NestLoop)) {
        NestLoop* nestloop = (NestLoop*)plan;

        dpns->planstate = (PlanState*)makeNode(NestLoopState);
        dpns->planstate->plan = plan;

        dpns->outer_planstate = (PlanState*)makeNode(PlanState);
        dpns->outer_planstate->plan = nestloop->join.plan.lefttree;

        dpns->inner_planstate = (PlanState*)makeNode(PlanState);
        dpns->inner_planstate->plan = nestloop->join.plan.righttree;
    } else if (IsA(plan, RemoteQuery)) {
        dpns->planstate = (PlanState*)makeNode(PlanState);
        dpns->planstate->plan = plan;
    } else if (IsA(plan, Agg) || IsA(plan, Group)) {
        /*
         * We expect plan tree as Group/Agg->Sort->Result->Material->RemoteQuery,
         * Result, Material nodes are optional. Sort is compulsory for Group but not
         * for Agg.
         * anything else is not handled right now.
         */
        Plan* temp_plan = plan->lefttree;
        Plan* remote_scan = NULL;

        if (temp_plan && IsA(temp_plan, Sort))
            temp_plan = temp_plan->lefttree;
        if (temp_plan && IsA(temp_plan, BaseResult))
            temp_plan = temp_plan->lefttree;
        if (temp_plan && IsA(temp_plan, Material))
            temp_plan = temp_plan->lefttree;
        if (temp_plan && IsA(temp_plan, RemoteQuery))
            remote_scan = temp_plan;

        if (remote_scan == NULL)
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("Deparse of this query at planning is not supported yet")));

        dpns->planstate = (PlanState*)makeNode(PlanState);
        dpns->planstate->plan = plan;
    } else
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("Deparse of this query at planning not supported yet")));
}

#endif
/*
 * push_child_plan: temporarily transfer deparsing attention to a child plan
 *
 * When expanding an OUTER_VAR or INNER_VAR reference, we must adjust the
 * deparse context in case the referenced expression itself uses
 * OUTER_VAR/INNER_VAR.  We modify the top stack entry in-place to avoid
 * affecting levelsup issues (although in a Plan tree there really shouldn't
 * be any).
 *
 * Caller must provide a local deparse_namespace variable to save the
 * previous state for pop_child_plan.
 */
static void push_child_plan(deparse_namespace* dpns, PlanState* ps, deparse_namespace* save_dpns)
{
    /* Save state for restoration later */
    *save_dpns = *dpns;

    /*
     * Currently we don't bother to adjust the ancestors list, because an
     * OUTER_VAR or INNER_VAR reference really shouldn't contain any Params
     * that would be set by the parent node itself.  If we did want to adjust
     * the list, lcons'ing dpns->planstate onto dpns->ancestors would be the
     * appropriate thing --- and pop_child_plan would need to undo the change
     * to the list.
     */

    /* Set attention on selected child */
    set_deparse_planstate(dpns, ps);
}

/*
 * pop_child_plan: undo the effects of push_child_plan
 */
static void pop_child_plan(deparse_namespace* dpns, deparse_namespace* save_dpns)
{
    /* Restore fields changed by push_child_plan */
    *dpns = *save_dpns;
}

/*
 * push_ancestor_plan: temporarily transfer deparsing attention to an
 * ancestor plan
 *
 * When expanding a Param reference, we must adjust the deparse context
 * to match the plan node that contains the expression being printed;
 * otherwise we'd fail if that expression itself contains a Param or
 * OUTER_VAR/INNER_VAR/INDEX_VAR variable.
 *
 * The target ancestor is conveniently identified by the ListCell holding it
 * in dpns->ancestors.
 *
 * Caller must provide a local deparse_namespace variable to save the
 * previous state for pop_ancestor_plan.
 */
static void push_ancestor_plan(deparse_namespace* dpns, ListCell* ancestor_cell, deparse_namespace* save_dpns)
{
    PlanState* ps = (PlanState*)lfirst(ancestor_cell);
    List* ancestors = NIL;

    /* Save state for restoration later */
    *save_dpns = *dpns;

    /* Build a new ancestor list with just this node's ancestors */
    ancestors = NIL;
    while ((ancestor_cell = lnext(ancestor_cell)) != NULL)
        ancestors = lappend(ancestors, lfirst(ancestor_cell));
    dpns->ancestors = ancestors;

    /* Set attention on selected ancestor */
    set_deparse_planstate(dpns, ps);
}

/*
 * pop_ancestor_plan: undo the effects of push_ancestor_plan
 */
static void pop_ancestor_plan(deparse_namespace* dpns, deparse_namespace* save_dpns)
{
    /* Free the ancestor list made in push_ancestor_plan */
    list_free_ext(dpns->ancestors);

    /* Restore fields changed by push_ancestor_plan */
    *dpns = *save_dpns;
}

/* ----------
 * make_ruledef			- reconstruct the CREATE RULE command
 *				  for a given pg_rewrite tuple
 * ----------
 */
static void make_ruledef(StringInfo buf, HeapTuple ruletup, TupleDesc rulettc, int prettyFlags)
{
    char* rulename = NULL;
    char ev_type;
    Oid ev_class;
    int2 ev_attr;
    bool is_instead = false;
    char* ev_qual = NULL;
    char* ev_action = NULL;
    List* actions = NIL;
    Relation ev_relation = NULL;
    TupleDesc viewResultDesc = NULL;
    int fno;
    Datum dat;
    bool isnull = false;

    /*
     * Get the attribute values from the rules tuple
     */
    fno = SPI_fnumber(rulettc, "rulename");
    dat = SPI_getbinval(ruletup, rulettc, fno, &isnull);
    Assert(!isnull);
    rulename = NameStr(*(DatumGetName(dat)));

    fno = SPI_fnumber(rulettc, "ev_type");
    dat = SPI_getbinval(ruletup, rulettc, fno, &isnull);
    Assert(!isnull);
    ev_type = DatumGetChar(dat);

    fno = SPI_fnumber(rulettc, "ev_class");
    dat = SPI_getbinval(ruletup, rulettc, fno, &isnull);
    Assert(!isnull);
    ev_class = DatumGetObjectId(dat);

    fno = SPI_fnumber(rulettc, "ev_attr");
    dat = SPI_getbinval(ruletup, rulettc, fno, &isnull);
    Assert(!isnull);
    ev_attr = DatumGetInt16(dat);

    fno = SPI_fnumber(rulettc, "is_instead");
    dat = SPI_getbinval(ruletup, rulettc, fno, &isnull);
    Assert(!isnull);
    is_instead = DatumGetBool(dat);

    /* these could be nulls */
    fno = SPI_fnumber(rulettc, "ev_qual");
    ev_qual = SPI_getvalue(ruletup, rulettc, fno);

    fno = SPI_fnumber(rulettc, "ev_action");
    ev_action = SPI_getvalue(ruletup, rulettc, fno);
    if (ev_action != NULL)
        actions = (List*)stringToNode(ev_action);

    ev_relation = heap_open(ev_class, AccessShareLock);

    /*
     * Build the rules definition text
     */
    appendStringInfo(buf, "CREATE RULE %s AS", quote_identifier(rulename));

    if ((unsigned int)prettyFlags & PRETTYFLAG_INDENT)
        appendStringInfoString(buf, "\n    ON ");
    else
        appendStringInfoString(buf, " ON ");

    /* The event the rule is fired for */
    switch (ev_type) {
        case '1':
            appendStringInfo(buf, "SELECT");
            viewResultDesc = RelationGetDescr(ev_relation);
            break;

        case '2':
            appendStringInfo(buf, "UPDATE");
            break;

        case '3':
            appendStringInfo(buf, "INSERT");
            break;

        case '4':
            appendStringInfo(buf, "DELETE");
            break;
            
        case '6':
            {
                Query* query = (Query*)linitial(actions);
                if (IsA(query->utilityStmt, CopyStmt)) {
                    appendStringInfo(buf, "COPY");
                } else if (IsA(query->utilityStmt, AlterTableStmt)) {
                    appendStringInfo(buf, "ALTER");
                }
                break;
            }

        default:
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("rule \"%s\" has unsupported event type %d", rulename, ev_type)));
            break;
    }

    /* The relation the rule is fired on */
    appendStringInfo(buf, " TO %s", generate_relation_name(ev_class, NIL));
    if (ev_attr > 0)
        appendStringInfo(buf, ".%s", quote_identifier(get_relid_attribute_name(ev_class, ev_attr)));

    /* If the rule has an event qualification, add it */
    if (ev_qual == NULL)
        ev_qual = "";
    if (strlen(ev_qual) > 0 && strcmp(ev_qual, "<>") != 0) {
        Node* qual = NULL;
        Query* query = NULL;
        deparse_context context;
        deparse_namespace dpns;

        if (prettyFlags & PRETTYFLAG_INDENT)
            appendStringInfoString(buf, "\n  ");
        appendStringInfo(buf, " WHERE ");

        qual = (Node*)stringToNode(ev_qual);

        /*
         * We need to make a context for recognizing any Vars in the qual
         * (which can only be references to OLD and NEW).  Use the rtable of
         * the first query in the action list for this purpose.
         */
        query = (Query*)linitial(actions);

        /*
         * If the action is INSERT...SELECT, OLD/NEW have been pushed down
         * into the SELECT, and that's what we need to look at. (Ugly kluge
         * ... try to fix this when we redesign querytrees.)
         */
        query = getInsertSelectQuery(query, NULL);

        /* Must acquire locks right away; see notes in get_query_def() */
        AcquireRewriteLocks(query, false);

        context.buf = buf;
        context.namespaces = list_make1(&dpns);
        context.windowClause = NIL;
        context.windowTList = NIL;
        context.varprefix = (list_length(query->rtable) != 1);
        context.prettyFlags = prettyFlags;
        context.wrapColumn = WRAP_COLUMN_DEFAULT;
        context.indentLevel = PRETTYINDENT_STD;
        context.qrw_phase = false;
#ifdef PGXC
        context.finalise_aggs = false;
        context.sortgroup_colno = false;
        context.parser_arg = NULL;
        context.viewdef = false;
        context.is_fqs = false;
#endif /* PGXC */
        context.is_upsert_clause = false;

        errno_t rc = memset_s(&dpns, sizeof(dpns), 0, sizeof(dpns));
        securec_check(rc, "\0", "\0");
        dpns.rtable = query->rtable;
        dpns.ctes = query->cteList;
#ifdef PGXC
        dpns.remotequery = false;
#endif
        get_rule_expr(qual, &context, false);
    }

    appendStringInfo(buf, " DO ");

    /* The INSTEAD keyword (if so) */
    if (is_instead)
        appendStringInfo(buf, "INSTEAD ");

    /* Finally the rules actions */
    if (list_length(actions) > 1) {
        ListCell* action = NULL;
        Query* query = NULL;

        appendStringInfo(buf, "(");
        foreach (action, actions) {
            query = (Query*)lfirst(action);
            get_query_def(query,
                buf,
                NIL,
                viewResultDesc,
                prettyFlags,
                WRAP_COLUMN_DEFAULT,
                0
#ifdef PGXC
                ,
                false,
                false,
                NULL
#endif /* PGXC */
                ,
                false,
                false);
            if (prettyFlags)
                appendStringInfo(buf, ";\n");
            else
                appendStringInfo(buf, "; ");
        }
        appendStringInfo(buf, ");");
    } else if (list_length(actions) == 0) {
        appendStringInfo(buf, "NOTHING;");
    } else {
        Query* query = NULL;

        query = (Query*)linitial(actions);
        get_query_def(query,
            buf,
            NIL,
            viewResultDesc,
            prettyFlags,
            WRAP_COLUMN_DEFAULT,
            0
#ifdef PGXC
            ,
            false,
            false,
            NULL
#endif /* PGXC */
            ,
            false,
            false);
        appendStringInfo(buf, ";");
    }
    heap_close(ev_relation, AccessShareLock);
}

/* ----------
 * make_viewdef			- reconstruct the SELECT part of a
 *				  view rewrite rule
 * ----------
 */
static void make_viewdef(StringInfo buf, HeapTuple ruletup, TupleDesc rulettc, int prettyFlags, int wrapColumn)
{
    Query* query = NULL;
    char ev_type;
    Oid ev_class;
    int2 ev_attr;
    bool is_instead = false;
    char* ev_qual = NULL;
    char* ev_action = NULL;
    List* actions = NIL;
    Relation ev_relation;
    int fno;
    bool isnull = false;

    /*
     * Get the attribute values from the rules tuple
     */
    fno = SPI_fnumber(rulettc, "ev_type");
    ev_type = (char)SPI_getbinval(ruletup, rulettc, fno, &isnull);

    fno = SPI_fnumber(rulettc, "ev_class");
    ev_class = (Oid)SPI_getbinval(ruletup, rulettc, fno, &isnull);

    fno = SPI_fnumber(rulettc, "ev_attr");
    ev_attr = (int2)SPI_getbinval(ruletup, rulettc, fno, &isnull);

    fno = SPI_fnumber(rulettc, "is_instead");
    is_instead = (bool)SPI_getbinval(ruletup, rulettc, fno, &isnull);

    fno = SPI_fnumber(rulettc, "ev_qual");
    ev_qual = SPI_getvalue(ruletup, rulettc, fno);

    fno = SPI_fnumber(rulettc, "ev_action");
    ev_action = SPI_getvalue(ruletup, rulettc, fno);
    if (ev_action != NULL)
        actions = (List*)stringToNode(ev_action);

    if (list_length(actions) != 1) {
        appendStringInfo(buf, "Not a view");
        return;
    }

    query = (Query*)linitial(actions);

    if (ev_type != '1' || ev_attr >= 0 || !is_instead || strcmp(ev_qual, "<>") != 0 ||
        query->commandType != CMD_SELECT) {
        appendStringInfo(buf, "Not a view");
        return;
    }

    ev_relation = heap_open(ev_class, AccessShareLock);

    get_query_def(query,
        buf,
        NIL,
        RelationGetDescr(ev_relation),
        prettyFlags,
        wrapColumn,
        0
#ifdef PGXC
        ,
        false,
        false,
        NULL
#endif /* PGXC */
        ,
        false,
        true);
    appendStringInfo(buf, ";");

    heap_close(ev_relation, AccessShareLock);
}

#ifdef PGXC
/* ----------
 * deparse_query			- Parse back one query parsetree
 *
 * Purpose of this function is to build up statement for a RemoteQuery
 * The query generated has all object names schema-qualified. This is
 * done by temporarily setting search_path to NIL.
 * It calls get_query_def without pretty print flags.
 *
 * Caution: get_query_def calls AcquireRewriteLocks, which might modify the RTEs
 * in place. So it is generally appropriate for the caller of this routine to
 * have first done a copyObject() to make a writable copy of the querytree in
 * the current memory context.
 * ----------
 */
void deparse_query(Query* query, StringInfo buf, List* parentnamespace, bool finalise_aggs, bool sortgroup_colno,
    void* parserArg, bool qrw_phase, bool is_fqs)
{
    OverrideSearchPath* tmp_search_path = NULL;
    List* schema_list = NIL;
    ListCell* schema = NULL;

    /*
     * Before deparsing the query, set the search_patch to NIL so that all the
     * object names in the deparsed query are schema qualified. This is required
     * so as to not have any dependency on current search_path. For e.g the same
     * remote query can be used even if search_path changes between two executes
     * of a prepared statement.
     * Note: We do not apply the above solution for temp tables for reasons shown
     * in the below comment.
     */
    tmp_search_path = GetOverrideSearchPath(CurrentMemoryContext);

    tmp_search_path->addTemp = true;
    schema_list = tmp_search_path->schemas;
    foreach (schema, schema_list) {
        if (isTempNamespace(lfirst_oid(schema))) {
            /* Is pg_temp the very first item ? If no, that means temp objects
             * should be qualified, otherwise the object name would possibly
             * be resolved from some other schema. We force schema
             * qualification by making sure the overridden search_path does not
             * have pg_temp implicitly added.
             * Why do we not *always* let the pg_temp qualification be there ?
             * Because the pg_temp schema name always gets deparsed into the
             * actual temp schema names like pg_temp_[1-9]*, and not the dummy
             * name pg_temp. And we do not want to use these names because
             * they are specific to the local node. pg_temp_2.obj1 at node 1
             * may be present in pg_temp_3 at node2.
             */
            if (list_head(schema_list) != schema)
                tmp_search_path->addTemp = false;

            break;
        }
    }

    tmp_search_path->schemas = NIL;
    PushOverrideSearchPath(tmp_search_path);

    get_query_def(query,
        buf,
        parentnamespace,
        NULL,
        PRETTYFLAG_PAREN,
        WRAP_COLUMN_DEFAULT,
        0
#ifdef PGXC
        ,
        finalise_aggs,
        sortgroup_colno,
        parserArg
#endif /* PGXC */
        ,
        qrw_phase,
        false,
        is_fqs);

    PopOverrideSearchPath();
}
#endif
/* ----------
 * get_query_def			- Parse back one query parsetree
 *
 * If resultDesc is not NULL, then it is the output tuple descriptor for
 * the view represented by a SELECT query.
 * ----------
 */
static void get_query_def(Query* query, StringInfo buf, List* parentnamespace, TupleDesc resultDesc, int prettyFlags,
    int wrapColumn, int startIndent
#ifdef PGXC
    ,
    bool finalise_aggs, bool sortgroup_colno, void* parserArg
#endif /* PGXC */
    ,
    bool qrw_phase, bool viewdef, bool is_fqs)
{
    deparse_context context;
    deparse_namespace dpns;

    /* Guard against excessively long or deeply-nested queries */
    CHECK_FOR_INTERRUPTS();
    check_stack_depth();

    /*
     * Before we begin to examine the query, acquire locks on referenced
     * relations, and fix up deleted columns in JOIN RTEs.	This ensures
     * consistent results.	Note we assume it's OK to scribble on the passed
     * querytree!For qrw phase, formal rewrite already add lock on relations,
     * and we don't want to change query tree again, so skip this.
     */
    if (!qrw_phase)
        AcquireRewriteLocks(query, false);

    context.buf = buf;
    context.namespaces = lcons(&dpns, list_copy(parentnamespace));
    context.windowClause = NIL;
    context.windowTList = NIL;
    context.varprefix = (parentnamespace != NIL || list_length(query->rtable) != 1);
    context.prettyFlags = prettyFlags;
    context.wrapColumn = wrapColumn;
    context.indentLevel = startIndent;
#ifdef PGXC
    context.finalise_aggs = finalise_aggs;
    context.sortgroup_colno = sortgroup_colno;
    context.parser_arg = parserArg;
#endif /* PGXC */
    context.qrw_phase = qrw_phase;
    context.viewdef = viewdef;
    context.is_fqs = is_fqs;
    context.is_upsert_clause = false;

    errno_t rc = memset_s(&dpns, sizeof(dpns), 0, sizeof(dpns));
    securec_check(rc, "", "");
    dpns.rtable = query->rtable;
    dpns.ctes = query->cteList;
#ifdef PGXC
    dpns.remotequery = false;
#endif

    switch (query->commandType) {
        case CMD_SELECT:
            get_select_query_def(query, &context, resultDesc);
            break;

        case CMD_UPDATE:
            get_update_query_def(query, &context);
            break;

        case CMD_INSERT:
            get_insert_query_def(query, &context);
            break;

        case CMD_DELETE:
            get_delete_query_def(query, &context);
            break;

        case CMD_MERGE:
            appendStringInfo(buf, "MERGE");
            break;

        case CMD_NOTHING:
            appendStringInfo(buf, "NOTHING");
            break;

        case CMD_UTILITY:
            get_utility_query_def(query, &context);
            break;

        default:
            ereport(ERROR,
                (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmsg("unrecognized query command type: %d", query->commandType)));
            break;
    }
}

/* ----------
 * get_values_def			- Parse back a VALUES list
 * ----------
 */
static void get_values_def(List* values_lists, deparse_context* context)
{
    StringInfo buf = context->buf;
    bool first_list = true;
    ListCell* vtl = NULL;

    appendStringInfoString(buf, "VALUES ");

    foreach (vtl, values_lists) {
        List* sublist = (List*)lfirst(vtl);
        bool first_col = true;
        ListCell* lc = NULL;

        if (first_list)
            first_list = false;
        else
            appendStringInfoString(buf, ", ");

        appendStringInfoChar(buf, '(');
        foreach (lc, sublist) {
            Node* col = (Node*)lfirst(lc);

            if (first_col)
                first_col = false;
            else
                appendStringInfoChar(buf, ',');

            /*
             * Strip any top-level nodes representing indirection assignments,
             * then print the result.
             */
            get_rule_expr(processIndirection(col, context, false), context, false);
        }
        appendStringInfoChar(buf, ')');
    }
}

/* ----------
 * get_with_clause			- Parse back a WITH clause
 * ----------
 */
static void get_with_clause(Query* query, deparse_context* context)
{
    StringInfo buf = context->buf;
    const char* sep = NULL;
    ListCell* l = NULL;

    if (query->cteList == NIL)
        return;

    if (PRETTY_INDENT(context)) {
        context->indentLevel += PRETTYINDENT_STD;
        appendStringInfoChar(buf, ' ');
    }

    if (query->hasRecursive)
        sep = "WITH RECURSIVE ";
    else
        sep = "WITH ";
    foreach (l, query->cteList) {
        CommonTableExpr* cte = (CommonTableExpr*)lfirst(l);

        appendStringInfoString(buf, sep);
        appendStringInfoString(buf, quote_identifier(cte->ctename));
        if (cte->aliascolnames) {
            bool first = true;
            ListCell* col = NULL;

            appendStringInfoChar(buf, '(');
            foreach (col, cte->aliascolnames) {
                if (first)
                    first = false;
                else
                    appendStringInfoString(buf, ", ");
                appendStringInfoString(buf, quote_identifier(strVal(lfirst(col))));
            }
            appendStringInfoChar(buf, ')');
        }
        appendStringInfoString(buf, " AS ");
        switch (cte->ctematerialized)
        {
            case CTEMaterializeDefault:
                break;
            case CTEMaterializeAlways:
                appendStringInfoString(buf, "MATERIALIZED ");
                break;
            case CTEMaterializeNever:
                appendStringInfoString(buf, "NOT MATERIALIZED ");
                break;
            default:
                ereport(ERROR,
                    (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                        errmsg("unrecognized materialization option: %d", (int)cte->ctematerialized)));
        }
        appendStringInfoChar(buf, '(');
        if (PRETTY_INDENT(context))
            appendContextKeyword(context, "", 0, 0, 0);
        get_query_def((Query*)cte->ctequery,
            buf,
            context->namespaces,
            NULL,
            context->prettyFlags,
            context->wrapColumn,
            context->indentLevel
#ifdef PGXC
            ,
            context->finalise_aggs,
            context->sortgroup_colno,
            context->parser_arg
#endif /* PGXC */
            ,
            context->qrw_phase,
            context->viewdef,
            context->is_fqs);
        if (PRETTY_INDENT(context))
            appendContextKeyword(context, "", 0, 0, 0);
        appendStringInfoChar(buf, ')');
        sep = ", ";
    }

    if (PRETTY_INDENT(context)) {
        context->indentLevel -= PRETTYINDENT_STD;
        appendContextKeyword(context, "", 0, 0, 0);
    } else
        appendStringInfoChar(buf, ' ');
}

/* ----------
 * get_select_query_def			- Parse back a SELECT parsetree
 * ----------
 */
static void get_select_query_def(Query* query, deparse_context* context, TupleDesc resultDesc)
{
    StringInfo buf = context->buf;
    List* save_windowclause = NIL;
    List* save_windowtlist = NIL;
    bool force_colno = false;
    ListCell* l = NULL;

    /* Insert the WITH clause if given */
    get_with_clause(query, context);

    /* Set up context for possible window functions */
    save_windowclause = context->windowClause;
    context->windowClause = query->windowClause;
    save_windowtlist = context->windowTList;
    context->windowTList = query->targetList;

    /*
     * If the Query node has a setOperations tree, then it's the top level of
     * a UNION/INTERSECT/EXCEPT query; only the WITH, ORDER BY and LIMIT
     * fields are interesting in the top query itself.
     */
    if (query->setOperations) {
        get_setop_query(query->setOperations, query, context, resultDesc);
        /* ORDER BY clauses must be simple in this case */
        force_colno = true;
    } else {
        get_basic_select_query(query, context, resultDesc);
        force_colno = false;
    }

    /* Add the ORDER BY clause if given */
    if (query->sortClause != NIL) {
        appendContextKeyword(context, " ORDER BY ", -PRETTYINDENT_STD, PRETTYINDENT_STD, 1);
        get_rule_orderby(query->sortClause, query->targetList, force_colno, context);
    }

    /* Add the LIMIT clause if given */
    if (query->limitOffset != NULL &&
        !(IsA(query->limitOffset, Const) && ((Const*)(query->limitOffset))->ismaxvalue)) {

        appendContextKeyword(context, " OFFSET ", -PRETTYINDENT_STD, PRETTYINDENT_STD, 0);
        get_rule_expr(query->limitOffset, context, false);
    }
    if (query->limitCount != NULL) {
        appendContextKeyword(context, " LIMIT ", -PRETTYINDENT_STD, PRETTYINDENT_STD, 0);
        if (IsA(query->limitCount, Const) && ((Const*)query->limitCount)->constisnull)
            appendStringInfo(buf, "ALL");
        else
            get_rule_expr(query->limitCount, context, false);
    }

    /* Add FOR [KEY] UPDATE/SHARE clauses if present */
    if (query->hasForUpdate) {
        foreach (l, query->rowMarks) {
            RowMarkClause* rc = (RowMarkClause*)lfirst(l);
            RangeTblEntry* rte = rt_fetch(rc->rti, query->rtable);

            /* don't print implicit clauses */
            if (rc->pushedDown)
                continue;

#ifndef ENABLE_MULTIPLE_NODES
            switch (rc->strength) {
                case LCS_FORKEYSHARE:
                    appendContextKeyword(context, " FOR KEY SHARE", -PRETTYINDENT_STD, PRETTYINDENT_STD, 0);
                    break;
                case LCS_FORSHARE:
                    appendContextKeyword(context, " FOR SHARE", -PRETTYINDENT_STD, PRETTYINDENT_STD, 0);
                    break;
                case LCS_FORNOKEYUPDATE:
                    appendContextKeyword(context, " FOR NO KEY UPDATE", -PRETTYINDENT_STD, PRETTYINDENT_STD, 0);
                    break;
                case LCS_FORUPDATE:
                    appendContextKeyword(context, " FOR UPDATE", -PRETTYINDENT_STD, PRETTYINDENT_STD, 0);
                    break;
                default:
                    ereport(ERROR, (errmsg("unknown lock type: %d", rc->strength)));
                    break;
            }
#else
            appendContextKeyword(context, rc->forUpdate ? " FOR UPDATE" : " FOR SHARE",
                -PRETTYINDENT_STD, PRETTYINDENT_STD, 0);
#endif
            appendStringInfo(buf, " OF %s", quote_identifier(rte->eref->aliasname));
            if (rc->noWait)
                appendStringInfo(buf, " NOWAIT");
            if (rc->waitSec > 0)
                appendStringInfo(buf, " WAIT %d", rc->waitSec);
        }
    }

    context->windowClause = save_windowclause;
    context->windowTList = save_windowtlist;
}

/*
 * Detect whether query looks like SELECT ... FROM VALUES();

 * if so, return the VALUES RTE.  Otherwise return NULL.
 */
static RangeTblEntry* get_simple_values_rte(Query* query)
{
    RangeTblEntry* result = NULL;
    ListCell* lc = NULL;

    /*
     * We want to return TRUE even if the Query also contains OLD or NEW rule
     * RTEs.  So the idea is to scan the rtable and see if there is only one
     * inFromCl RTE that is a VALUES RTE.
     */
    foreach (lc, query->rtable) {
        RangeTblEntry* rte = (RangeTblEntry*)lfirst(lc);

        if (rte->rtekind == RTE_VALUES && rte->inFromCl) {
            if (result != NULL)
                return NULL; /* multiple VALUES (probably not possible) */
            result = rte;
        } else if (rte->rtekind == RTE_RELATION && !rte->inFromCl)
            continue; /* ignore rule entries */
        else
            return NULL; /* something else -> not simple VALUES */
    }

    /*
     * We don't need to check the targetlist in any great detail, because
     * parser/analyze.c will never generate a "bare" VALUES RTE --- they only
     * appear inside auto-generated sub-queries with very restricted
     * structure.  However, DefineView might have modified the tlist by
     * injecting new column aliases; so compare tlist resnames against the
     * RTE's names to detect that.
     */
    if (result != NULL) {
        ListCell* lcn = NULL;
        if (list_length(query->targetList) != list_length(result->eref->colnames))
            return NULL; /* this probably cannot happen */
        forboth(lc, query->targetList, lcn, result->eref->colnames)
        {
            TargetEntry* tle = (TargetEntry*)lfirst(lc);
            char* cname = strVal(lfirst(lcn));

            if (tle->resjunk)
                return NULL; /* this probably cannot happen */
            if (tle->resname == NULL || strcmp(tle->resname, cname) != 0)
                return NULL; /* column name has been changed */
        }
    }

    return result;
}

static inline void get_hint_string_internal(const List* list, StringInfo buf)
{
    ListCell* lc = NULL;
    Hint* hint = NULL;
    foreach (lc, list) {
        hint = (Hint*)lfirst(lc);
        appendStringInfo(buf, "%s", descHint(hint));
    }
}

/*
 * @Description: Convert hint to string
 * @in hstate: hint state.
 * @out buf: String buf.
 */
void get_hint_string(HintState* hstate, StringInfo buf)
{
    if (hstate == NULL) {
        return;
    }

    ListCell* lc = NULL;
    Hint* hint = NULL;

    Assert(buf != NULL);
    appendStringInfo(buf, "/*+");
    get_hint_string_internal(hstate->join_hint, buf);
    get_hint_string_internal(hstate->leading_hint, buf);
    get_hint_string_internal(hstate->row_hint, buf);
    get_hint_string_internal(hstate->stream_hint, buf);
    get_hint_string_internal(hstate->block_name_hint, buf);
    get_hint_string_internal(hstate->scan_hint, buf);
    get_hint_string_internal(hstate->predpush_hint, buf);
    get_hint_string_internal(hstate->predpush_same_level_hint, buf);
    get_hint_string_internal(hstate->rewrite_hint, buf);
    get_hint_string_internal(hstate->gather_hint, buf);
    get_hint_string_internal(hstate->cache_plan_hint, buf);
    get_hint_string_internal(hstate->set_hint, buf);
    get_hint_string_internal(hstate->no_expand_hint, buf);
    get_hint_string_internal(hstate->no_gpc_hint, buf);
    foreach (lc, hstate->skew_hint) {
        hint = (Hint*)lfirst(lc);
        if (IsA(hint, SkewHintTransf)) {
            SkewHintTransf* hint_transf = (SkewHintTransf*)hint;
            hint = (Hint*)hint_transf->before;
        }

        appendStringInfo(buf, "%s", descHint(hint));
    }
    if (hstate->multi_node_hint) {
        appendStringInfo(buf, " multinode ");
    }
    appendStringInfo(buf, "*/");
}

static void get_basic_select_query(Query* query, deparse_context* context, TupleDesc resultDesc)
{
    StringInfo buf = context->buf;
    RangeTblEntry* values_rte = NULL;
    char* sep = NULL;
    ListCell* l = NULL;

    if (PRETTY_INDENT(context)) {
        context->indentLevel += PRETTYINDENT_STD;
        appendStringInfoChar(buf, ' ');
    }

    /*
     * If the query looks like SELECT * FROM (VALUES ...), then print just the
     * VALUES part.  This reverses what transformValuesClause() did at parse
     * time.
     */

    values_rte = get_simple_values_rte(query);
    if (values_rte != NULL) {
        get_values_def(values_rte->values_lists, context);
        return;
    }

    /*
     * Build up the query string - first we say SELECT
     */
    appendStringInfo(buf, "SELECT");
    get_hint_string(query->hintState, buf);

    /* Add the DISTINCT clause if given */
    if (query->distinctClause != NIL) {
        if (query->hasDistinctOn) {
            appendStringInfo(buf, " DISTINCT ON (");
            sep = "";
            foreach (l, query->distinctClause) {
                SortGroupClause* srt = (SortGroupClause*)lfirst(l);

                appendStringInfoString(buf, sep);
                get_rule_sortgroupclause(srt->tleSortGroupRef, query->targetList, false, context);
                sep = ", ";
            }
            appendStringInfo(buf, ")");
        } else
            appendStringInfo(buf, " DISTINCT");
    }

    /* Then we tell what to select (the targetlist) */
    get_target_list(query, query->targetList, context, resultDesc);

    /* Add the FROM clause if needed */
    get_from_clause(query, " FROM ", context);

    /* Add the WHERE clause if given */
    if (query->jointree->quals != NULL) {
        appendContextKeyword(context, " WHERE ", -PRETTYINDENT_STD, PRETTYINDENT_STD, 1);
        get_rule_expr(query->jointree->quals, context, false);
    }

    /* Add the GROUP BY clause if given */
    if (query->groupClause != NULL) {
        appendContextKeyword(context, " GROUP BY ", -PRETTYINDENT_STD, PRETTYINDENT_STD, 1);
        if (query->groupingSets == NIL) {
            sep = "";
            foreach (l, query->groupClause) {
                SortGroupClause* grp = (SortGroupClause*)lfirst(l);

                appendStringInfoString(buf, sep);
                get_rule_sortgroupclause(grp->tleSortGroupRef, query->targetList, false, context);
                sep = ", ";
            }
        } else {
            sep = "";
            foreach (l, query->groupingSets) {
                GroupingSet* grp = (GroupingSet*)lfirst(l);

                appendStringInfoString(buf, sep);
                get_rule_groupingset(grp, query->targetList, context);
                sep = ", ";
            }
        }
        /* Since we don't support vec aggregation, we don't output it in vector way	*/
        query->vec_output = false;
    }

    /* Add the HAVING clause if given */
    if (query->havingQual != NULL) {
        appendContextKeyword(context, " HAVING ", -PRETTYINDENT_STD, PRETTYINDENT_STD, 0);

        if (IsA(query->havingQual, List)) {
            StringInfo buf = context->buf;
            char* sep = NULL;
            ListCell* l = NULL;

            sep = "";
            foreach (l, (List*)query->havingQual) {
                appendStringInfoString(buf, sep);
                /* We need add brackets for each having qual, else can lead to semantic error.*/
                appendStringInfoString(buf, "(");
                get_rule_expr((Node*)lfirst(l), context, false);
                appendStringInfoString(buf, ")");
                sep = " AND ";
            }
        } else
            get_rule_expr(query->havingQual, context, false);
    }

    /* Add the WINDOW clause if needed */
    if (query->windowClause != NIL) {
        get_rule_windowclause(query, context);
        query->vec_output = false;
    }
}

/* ----------
 * get_target_list			- Parse back a SELECT target list
 *
 * This is also used for RETURNING lists in INSERT/UPDATE/DELETE.
 * ----------
 */
static void get_target_list(Query* query, List* targetList, deparse_context* context, TupleDesc resultDesc)
{
    StringInfo buf = context->buf;
    StringInfoData targetbuf;
    bool last_was_multiline = false;
    char* sep = NULL;
    int colno;
    ListCell* l = NULL;
    ListCell* star_start_cell = NULL;
    ListCell* star_end_cell = NULL;
    ListCell* star_only_cell = NULL;
    int star_start = -1;
    int star_end = -1;
    int star_only = -1;
#ifdef PGXC
    bool no_targetlist = true;
#endif

    Assert(list_length(query->starStart) == list_length(query->starEnd));
    Assert(list_length(query->starStart) == list_length(query->starOnly));

    star_start_cell = list_head(query->starStart);
    star_end_cell = list_head(query->starEnd);
    star_only_cell = list_head(query->starOnly);

    /* we use targetbuf to hold each TLE's text temporarily */
    initStringInfo(&targetbuf);

    sep = " ";
    colno = 0;
    foreach (l, targetList) {
        TargetEntry* tle = (TargetEntry*)lfirst(l);
        char* colname = NULL;
        char* attname = NULL;
        bool for_star = false;

        if (tle->resjunk)
            continue; /* ignore junk entries */

        /* Ignore junk columns from the targetlist in start with */
        if (query->hasRecursive && IsPseudoReturnColumn(tle->resname)) {
            continue;
        }

#ifdef PGXC
        /* Found at least one element in the target list */
        if (no_targetlist)
            no_targetlist = false;
#endif
        /*
         * dump view def, we dump * instead expanded target entry because expanded target entrys
         * mabye have same alias name which is figured by function FigureColnameInternal(), and
         * views will be fail to be recreate due to duplicate alias names, just as following case
         *
         *create view test_view(col1,col2) as
         *SELECT * FROM
         *(
         *		SELECT
         *			CAST(a/10000 AS DECIMAL(18,2)),
         *			CAST(CAST(b AS DECIMAL(18,4))/a*100 AS DECIMAL(18,2))
         *		FROM test_tb
         *);
         *
         * dump as:
         *
         *CREATE VIEW test_view AS
         *       SELECT
         *       __unnamed_subquery__."numeric" AS col1,
         *       __unnamed_subquery__."numeric" AS col2 FROM
         *   (
         *       SELECT
         *           ((test_tb.a / 10000))::numeric(18,2) AS "numeric",
         *            ((((test_tb.b)::numeric(18,4) / (test_tb.a)::numeric) * (100)::numeric))::numeric(18,2) AS
         *"numeric" FROM test_tb ) __unnamed_subquery__;
         */
        if (context->viewdef || (context->is_fqs && query->use_star_targets)) {
            if (star_start > 0) {
                if (star_end > tle->resno) {
                    colno++;
                    continue;
                }

                star_start = -1;
                star_end = -1;
                star_only = -1;
            }

            if (0 > star_start) {
                if (star_start_cell && lfirst_int(star_start_cell) == tle->resno) {
                    for_star = true;

                    star_start = lfirst_int(star_start_cell);
                    star_end = lfirst_int(star_end_cell);
                    star_only = lfirst_int(star_only_cell);

                    star_start_cell = lnext(star_start_cell);
                    star_end_cell = lnext(star_end_cell);
                    star_only_cell = lnext(star_only_cell);
                }
            }
        }

        appendStringInfoString(buf, sep);
        sep = ", ";
        colno++;

        if (for_star && star_only > 0) {
            appendStringInfo(buf, " *");
            continue;
        }

        /*
         * Put the new field text into targetbuf so we can decide after we've
         * got it whether or not it needs to go on a new line.
         */
        resetStringInfo(&targetbuf);
        context->buf = &targetbuf;

        /*
         * We special-case Var nodes rather than using get_rule_expr. This is
         * needed because get_rule_expr will display a whole-row Var as
         * "foo.*", which is the preferred notation in most contexts, but at
         * the top level of a SELECT list it's not right (the parser will
         * expand that notation into multiple columns, yielding behavior
         * different from a whole-row Var).  We need to call get_variable
         * directly so that we can tell it to do the right thing.
         */
        if (tle->expr && IsA(tle->expr, Var)) {
            attname = get_variable((Var*)tle->expr, 0, true, context, for_star);
        } else {
            /*
             * Attname not always be "?column?" for no var type.
             * For example:
             *  select * from (select '', val from test);
             * NULL alias is "unknow" rather then "?column?".
             *
             * In this case, we need add as alias.
             */
            get_rule_expr((Node*)tle->expr, context, true);
        }

        /*
         * Figure out what the result column should be called.	In the context
         * of a view, use the view's tuple descriptor (so as to pick up the
         * effects of any column RENAME that's been done on the view).
         * Otherwise, just use what we can find in the TLE.
         */
        if (resultDesc && colno <= resultDesc->natts)
            colname = NameStr(resultDesc->attrs[colno - 1]->attname);
        else
            colname = tle->resname;

        /* Show AS unless the column's name is correct as-is */
        if (colname && !for_star) /* resname could be NULL */
        {
            if (attname == NULL || strcmp(attname, colname) != 0)
                appendStringInfo(&targetbuf, " AS %s", quote_identifier(colname));
        }
        if (attname != NULL) {
            pfree_ext(attname);
            attname = NULL;
        }

        /* Restore context's output buffer */
        context->buf = buf;

        /* Consider line-wrapping if enabled */
        if (PRETTY_INDENT(context) && context->wrapColumn >= 0) {
            int leading_nl_pos = -1;
            char* trailing_nl = NULL;
            int pos;

            /* Does the new field start with whitespace plus a new line? */
            for (pos = 0; pos < targetbuf.len; pos++) {
                if (targetbuf.data[pos] == '\n') {
                    leading_nl_pos = pos;
                    break;
                }
                if (targetbuf.data[pos] != ' ')
                    break;
            }

            /* Locate the start of the current line in the output buffer */
            trailing_nl = strrchr(buf->data, '\n');
            if (trailing_nl == NULL)
                trailing_nl = buf->data;
            else
                trailing_nl++;

            /*
             * If the field we're adding is the first in the list, or it
             * already has a leading newline, don't add anything. Otherwise,
             * add a newline, plus some indentation, if either the new field
             * would cause an overflow or the last field used more than one
             * line.
             */
            if (colno > 1 && leading_nl_pos == -1 &&
                ((strlen(trailing_nl) + strlen(targetbuf.data) > (unsigned int)(context->wrapColumn)) ||
                    last_was_multiline))
                appendContextKeyword(context, "", -PRETTYINDENT_STD, PRETTYINDENT_STD, PRETTYINDENT_VAR);

            /* Remember this field's multiline status for next iteration */
            last_was_multiline = (strchr(targetbuf.data + leading_nl_pos + 1, '\n') != NULL);
        }

        /* Add the new field */
        appendStringInfoString(buf, targetbuf.data);
    }

#ifdef PGXC
    /*
     * Because the empty target list can generate invalid SQL
     * clause. Here, just fill a '*' to process a table without
     * any columns, this statement will be sent to Datanodes
     * and treated correctly on remote nodes.
     */
    if (no_targetlist)
        appendStringInfo(buf, " *");
#endif
    /* clean up */
    pfree_ext(targetbuf.data);
}

static void get_setop_query(Node* setOp, Query* query, deparse_context* context, TupleDesc resultDesc)
{
    StringInfo buf = context->buf;
    bool need_paren = false;

    /* Guard against excessively long or deeply-nested queries */
    CHECK_FOR_INTERRUPTS();
    check_stack_depth();

    if (IsA(setOp, RangeTblRef)) {
        RangeTblRef* rtr = (RangeTblRef*)setOp;
        RangeTblEntry* rte = rt_fetch(rtr->rtindex, query->rtable);
        Query* subquery = rte->subquery;

        Assert(subquery != NULL);

        /* Need parens if WITH, ORDER BY, FOR UPDATE, or LIMIT; see gram.y */
        need_paren = (subquery->cteList || subquery->sortClause || subquery->rowMarks || subquery->limitOffset ||
                      subquery->limitCount || subquery->setOperations);
        if (need_paren)
            appendStringInfoChar(buf, '(');

        if (subquery->setOperations == NULL) {
            get_query_def(subquery,
                buf,
                context->namespaces,
                resultDesc,
                context->prettyFlags,
                context->wrapColumn,
                context->indentLevel
#ifdef PGXC
                ,
                context->finalise_aggs,
                context->sortgroup_colno,
                context->parser_arg
#endif /* PGXC */
                ,
                context->qrw_phase,
                context->viewdef,
                context->is_fqs);
        } else {
            if (context->qrw_phase)
                get_setop_query(subquery->setOperations, subquery, context, resultDesc);
            else
                Assert(false);
        }

        if (need_paren)
            appendStringInfoChar(buf, ')');
    } else if (IsA(setOp, SetOperationStmt)) {
        SetOperationStmt* op = (SetOperationStmt*)setOp;

        if (PRETTY_INDENT(context)) {
            context->indentLevel += PRETTYINDENT_STD;
            appendStringInfoSpaces(buf, PRETTYINDENT_STD);
        }

        /*
         * We force parens whenever nesting two SetOperationStmts. There are
         * some cases in which parens are needed around a leaf query too, but
         * those are more easily handled at the next level down (see code
         * above).
         */
        need_paren = !IsA(op->larg, RangeTblRef);

        if (need_paren)
            appendStringInfoChar(buf, '(');
        get_setop_query(op->larg, query, context, resultDesc);
        if (need_paren)
            appendStringInfoChar(buf, ')');

        if (!PRETTY_INDENT(context))
            appendStringInfoChar(buf, ' ');
        switch (op->op) {
            case SETOP_UNION:
                appendContextKeyword(context, "UNION ", -PRETTYINDENT_STD, PRETTYINDENT_STD, 0);
                break;
            case SETOP_INTERSECT:
                appendContextKeyword(context, "INTERSECT ", -PRETTYINDENT_STD, PRETTYINDENT_STD, 0);
                break;
            case SETOP_EXCEPT:
                appendContextKeyword(context, "EXCEPT ", -PRETTYINDENT_STD, PRETTYINDENT_STD, 0);
                break;
            default:
                ereport(
                    ERROR, (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE), errmsg("unrecognized set op: %d", (int)op->op)));
        }
        if (op->all)
            appendStringInfo(buf, "ALL ");

        if (PRETTY_INDENT(context))
            appendContextKeyword(context, "", 0, 0, 0);

        need_paren = !IsA(op->rarg, RangeTblRef);

        if (need_paren)
            appendStringInfoChar(buf, '(');
        get_setop_query(op->rarg, query, context, resultDesc);
        if (need_paren)
            appendStringInfoChar(buf, ')');

        if (PRETTY_INDENT(context))
            context->indentLevel -= PRETTYINDENT_STD;
    } else {
        ereport(ERROR,
            (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE), errmsg("unrecognized node type: %d", (int)nodeTag(setOp))));
    }
}

/*
 * Display a sort/group clause.
 *
 * Also returns the expression tree, so caller need not find it again.
 */
static Node* get_rule_sortgroupclause(Index ref, List* tlist, bool force_colno, deparse_context* context)
{
    StringInfo buf = context->buf;
    TargetEntry* tle = NULL;
    Node* expr = NULL;

    tle = get_sortgroupref_tle(ref, tlist);
    expr = (Node*)tle->expr;

    /*
     * Use column-number form if requested by caller.  Otherwise, if
     * expression is a constant, force it to be dumped with an explicit cast
     * as decoration --- this is because a simple integer constant is
     * ambiguous (and will be misinterpreted by findTargetlistEntry()) if we
     * dump it without any decoration.	Otherwise, just dump the expression
     * normally.
     */
    if (force_colno || context->sortgroup_colno) {
        Assert(!tle->resjunk);
        appendStringInfo(buf, "%d", tle->resno);
    } else if (expr && IsA(expr, Const))
        get_const_expr((Const*)expr, context, 1);
    else
        get_rule_expr(expr, context, true);

    return expr;
}

/*
 * @Description: Display a GroupingSet.
 * @in gset: Grouping Set.
 * @in targetlist: targetlist.
 * @in context: Context info needed for invoking a recursive querytree display routine.
 */
static void get_rule_groupingset(GroupingSet* gset, List* targetlist, deparse_context* context)
{
    ListCell* l = NULL;
    StringInfo buf = context->buf;
    bool omit_child_parens = true;
    char* sep = "";

    switch (gset->kind) {
        case GROUPING_SET_EMPTY:
            appendStringInfoString(buf, "()");
            return;

        case GROUPING_SET_SIMPLE: {
            if (list_length(gset->content) != 1)
                appendStringInfoString(buf, "(");

            foreach (l, gset->content) {
                Index ref = lfirst_int(l);

                appendStringInfoString(buf, sep);
                get_rule_sortgroupclause(ref, targetlist, false, context);
                sep = ", ";
            }

            if (list_length(gset->content) != 1)
                appendStringInfoString(buf, ")");
        }
            return;

        case GROUPING_SET_ROLLUP:
            appendStringInfoString(buf, "ROLLUP(");
            break;
        case GROUPING_SET_CUBE:
            appendStringInfoString(buf, "CUBE(");
            break;
        case GROUPING_SET_SETS:
            appendStringInfoString(buf, "GROUPING SETS (");
            omit_child_parens = false;
            break;
        default:
            break;
    }

    foreach (l, gset->content) {
        appendStringInfoString(buf, sep);
        get_rule_groupingset((GroupingSet*)lfirst(l), targetlist, context);
        sep = ", ";
    }

    appendStringInfoString(buf, ")");
}

/*
 * Display an ORDER BY list.
 */
static void get_rule_orderby(List* orderList, List* targetList, bool force_colno, deparse_context* context)
{
    StringInfo buf = context->buf;
    const char* sep = NULL;
    ListCell* l = NULL;

    sep = "";
    foreach (l, orderList) {
        SortGroupClause* srt = (SortGroupClause*)lfirst(l);
        Node* sortexpr = NULL;
        Oid sortcoltype;
        TypeCacheEntry* typentry = NULL;

        appendStringInfoString(buf, sep);
        sortexpr = get_rule_sortgroupclause(srt->tleSortGroupRef, targetList, force_colno, context);
        sortcoltype = exprType(sortexpr);
        /* See whether operator is default < or > for datatype */
        typentry = lookup_type_cache(sortcoltype, TYPECACHE_LT_OPR | TYPECACHE_GT_OPR);
        if (srt->sortop == typentry->lt_opr) {
            /* ASC is default, so emit nothing for it */
            if (srt->nulls_first)
                appendStringInfo(buf, " NULLS FIRST");
        } else if (srt->sortop == typentry->gt_opr) {
            appendStringInfo(buf, " DESC");
            /* DESC defaults to NULLS FIRST */
            if (!srt->nulls_first)
                appendStringInfo(buf, " NULLS LAST");
        } else {
            appendStringInfo(buf, " USING %s", generate_operator_name(srt->sortop, sortcoltype, sortcoltype));
            /* be specific to eliminate ambiguity */
            if (srt->nulls_first)
                appendStringInfo(buf, " NULLS FIRST");
            else
                appendStringInfo(buf, " NULLS LAST");
        }
        sep = ", ";
    }
}

/*
 * Display a WINDOW clause.
 *
 * Note that the windowClause list might contain only anonymous window
 * specifications, in which case we should print nothing here.
 */
static void get_rule_windowclause(Query* query, deparse_context* context)
{
    StringInfo buf = context->buf;
    const char* sep = NULL;
    ListCell* l = NULL;

    sep = NULL;
    foreach (l, query->windowClause) {
        WindowClause* wc = (WindowClause*)lfirst(l);

        if (wc->name == NULL)
            continue; /* ignore anonymous windows */

        if (sep == NULL)
            appendContextKeyword(context, " WINDOW ", -PRETTYINDENT_STD, PRETTYINDENT_STD, 1);
        else
            appendStringInfoString(buf, sep);

        appendStringInfo(buf, "%s AS ", quote_identifier(wc->name));

        get_rule_windowspec(wc, query->targetList, context);

        sep = ", ";
    }
}

/*
 * Display a window definition
 */
static void get_rule_windowspec(WindowClause* wc, List* targetList, deparse_context* context)
{
    StringInfo buf = context->buf;
    bool needspace = false;
    const char* sep = NULL;
    ListCell* l = NULL;

    appendStringInfoChar(buf, '(');
    if (wc->refname) {
        appendStringInfoString(buf, quote_identifier(wc->refname));
        needspace = true;
    }
    /* partition clauses are always inherited, so only print if no refname */
    if (wc->partitionClause && !wc->refname) {
        if (needspace)
            appendStringInfoChar(buf, ' ');
        appendStringInfoString(buf, "PARTITION BY ");
        sep = "";
        foreach (l, wc->partitionClause) {
            SortGroupClause* grp = (SortGroupClause*)lfirst(l);

            appendStringInfoString(buf, sep);
            get_rule_sortgroupclause(grp->tleSortGroupRef, targetList, false, context);
            sep = ", ";
        }
        needspace = true;
    }
    /* print ordering clause only if not inherited */
    if (wc->orderClause && !wc->copiedOrder) {
        if (needspace)
            appendStringInfoChar(buf, ' ');
        appendStringInfoString(buf, "ORDER BY ");
        get_rule_orderby(wc->orderClause, targetList, false, context);
        needspace = true;
    }
    /* framing clause is never inherited, so print unless it's default */
    if (wc->frameOptions & FRAMEOPTION_NONDEFAULT) {
        if (needspace)
            appendStringInfoChar(buf, ' ');
        if (wc->frameOptions & FRAMEOPTION_RANGE)
            appendStringInfoString(buf, "RANGE ");
        else if (wc->frameOptions & FRAMEOPTION_ROWS)
            appendStringInfoString(buf, "ROWS ");
        else
            Assert(false);
        if (wc->frameOptions & FRAMEOPTION_BETWEEN)
            appendStringInfoString(buf, "BETWEEN ");
        if (wc->frameOptions & FRAMEOPTION_START_UNBOUNDED_PRECEDING)
            appendStringInfoString(buf, "UNBOUNDED PRECEDING ");
        else if (wc->frameOptions & FRAMEOPTION_START_CURRENT_ROW)
            appendStringInfoString(buf, "CURRENT ROW ");
        else if (wc->frameOptions & FRAMEOPTION_START_VALUE) {
            get_rule_expr(wc->startOffset, context, false);
            if (wc->frameOptions & FRAMEOPTION_START_VALUE_PRECEDING)
                appendStringInfoString(buf, " PRECEDING ");
            else if (wc->frameOptions & FRAMEOPTION_START_VALUE_FOLLOWING)
                appendStringInfoString(buf, " FOLLOWING ");
            else
                Assert(false);
        } else
            Assert(false);
        if (wc->frameOptions & FRAMEOPTION_BETWEEN) {
            appendStringInfoString(buf, "AND ");
            if (wc->frameOptions & FRAMEOPTION_END_UNBOUNDED_FOLLOWING)
                appendStringInfoString(buf, "UNBOUNDED FOLLOWING ");
            else if (wc->frameOptions & FRAMEOPTION_END_CURRENT_ROW)
                appendStringInfoString(buf, "CURRENT ROW ");
            else if (wc->frameOptions & FRAMEOPTION_END_VALUE) {
                get_rule_expr(wc->endOffset, context, false);
                if (wc->frameOptions & FRAMEOPTION_END_VALUE_PRECEDING)
                    appendStringInfoString(buf, " PRECEDING ");
                else if (wc->frameOptions & FRAMEOPTION_END_VALUE_FOLLOWING)
                    appendStringInfoString(buf, " FOLLOWING ");
                else
                    Assert(false);
            } else
                Assert(false);
        }
        /* we will now have a trailing space; remove it */
        buf->len--;
    }
    appendStringInfoChar(buf, ')');
}

/*
 * Display a window definition for listagg
 */
static void get_rule_windowspec_listagg(WindowClause* wc, List* targetList, deparse_context* context)
{
    StringInfo buf = context->buf;
    bool needspace = false;
    const char* sep = NULL;
    ListCell* l = NULL;

    appendStringInfoChar(buf, '(');
    if (wc->refname) {
        appendStringInfoString(buf, quote_identifier(wc->refname));
        needspace = true;
    }

    /* print ordering clause only if not inherited */
    if (wc->orderClause && !wc->copiedOrder) {
        if (needspace)
            appendStringInfoChar(buf, ' ');
        appendStringInfoString(buf, "ORDER BY ");
        get_rule_orderby(wc->orderClause, targetList, false, context);
        needspace = true;
    }

    appendStringInfoString(buf, ") OVER (");

    /* partition clauses are always inherited, so only print if no refname */
    if (wc->partitionClause && !wc->refname) {
        if (needspace)
            appendStringInfoChar(buf, ' ');
        appendStringInfoString(buf, "PARTITION BY ");
        sep = "";
        foreach (l, wc->partitionClause) {
            SortGroupClause* grp = (SortGroupClause*)lfirst(l);

            appendStringInfoString(buf, sep);
            get_rule_sortgroupclause(grp->tleSortGroupRef, targetList, false, context);
            sep = ", ";
        }
        needspace = true;
    }
    appendStringInfoChar(buf, ')');
}

/*
 * @Description: get_set_target_list    Parse back a SET clause
 * This is used for UPDATE and INSERT ON DUPLICATE KEY UPDATE.
 */
static void get_set_target_list(List* targetList, RangeTblEntry* rte, deparse_context* context)
{
    ListCell* lc = NULL;
    char* sep = NULL;
    StringInfo buf = context->buf;

    sep = "";
    foreach (lc, targetList) {
        TargetEntry* tle = (TargetEntry*)lfirst(lc);
        Node* expr = NULL;

        if (tle->resjunk)
            continue; /* ignore junk entries */

        appendStringInfoString(buf, sep);
        sep = ", ";
        /*
         * Put out name of target column; look in the catalogs, not at
         * tle->resname, since resname will fail to track RENAME.
         */
        appendStringInfoString(buf, quote_identifier(get_relid_attribute_name(rte->relid, tle->resno)));

        /*
         * Print any indirection needed (subfields or subscripts), and strip
         * off the top-level nodes representing the indirection assignments.
         */
        expr = processIndirection((Node*)tle->expr, context, true);

        appendStringInfo(buf, " = ");

        get_rule_expr(expr, context, false);
    }
}

/* ----------
 * get_insert_query_def			- Parse back an INSERT parsetree
 * ----------
 */
static void get_insert_query_def(Query* query, deparse_context* context)
{
    StringInfo buf = context->buf;
    RangeTblEntry* select_rte = NULL;
    RangeTblEntry* values_rte = NULL;
    RangeTblEntry* rte = NULL;
    char* sep = NULL;
    ListCell* values_cell = NULL;
    ListCell* l = NULL;
    List* strippedexprs = NIL;
    bool is_fqs_inselect = false;

    /* Insert the WITH clause if given */
    get_with_clause(query, context);

#ifdef PGXC
    /*
     * In the case of "INSERT ... DEFAULT VALUES" analyzed in pgxc planner,
     * return the sql statement directly if the table has no default values.
     */
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord() && !query->targetList) {
        appendStringInfo(buf, "%s", query->sql_statement);
        return;
    }

    /*
     * select_rte are not required by INSERT queries in XC
     * it should stay null for INSERT queries to work corretly
     * consider an example
     * insert into tt select * from tt
     * This query uses select_rte, but again that is not required in XC
     * Again here the query gets broken down into two queries
     * SELECT column1, column2 FROM ONLY tt WHERE true
     * and
     * INSERT INTO tt (column1, column2) VALUES ($1, $2)
     * Note again that the insert query does not need select_rte
     * Hence we keep both select_rte and values_rte NULL.
     */
    if (context->is_fqs || !(IS_PGXC_COORDINATOR && !IsConnFromCoord())) {
#endif
        /*
         * If it's an INSERT ... SELECT or multi-row VALUES, there will be a
         * single RTE for the SELECT or VALUES.  Plain VALUES has neither.
         */
        foreach (l, query->rtable) {
            rte = (RangeTblEntry*)lfirst(l);
            if (rte->rtekind == RTE_SUBQUERY) {
                is_fqs_inselect = true;
                if (select_rte != NULL) {
                    ereport(ERROR, (errcode(ERRCODE_RESTRICT_VIOLATION), errmsg("too many subquery RTEs in INSERT")));
                }
                select_rte = rte;
            }
        }

#ifdef PGXC
    }
#endif

    /*
     * values_rte will be required by INSERT queries in XC
     * only when the relation is located on a single node
     * requested by FQS
     * Consider an example
     * CREATE NODE GROUP ng WITH (datanode2);
     * CREATE TABLE tt (column1 int4, column2 text) TO GROUP ng;
     * INSERT INTO tt (column1) VALUES(1), (2)
     *
     * for other cases values_rte should stay null
     * create table tt as values(1,'One'),(2,'Two');
     * This query uses values_rte, but we do not need them in XC
     * because it gets broken down into two queries
     * CREATE TABLE tt(column1 int4, column2 text)
     * and
     * INSERT INTO tt (column1, column2) VALUES ($1, $2)
     * Note that the insert query does not need values_rte
     */
#ifdef PGXC
    if ((context->is_fqs && !is_fqs_inselect) || !(IS_PGXC_COORDINATOR && !IsConnFromCoord())) {
#endif
        foreach (l, query->rtable) {
            rte = (RangeTblEntry*)lfirst(l);
            if (rte->rtekind == RTE_VALUES) {
                if (values_rte != NULL) {
                    ereport(ERROR, (errcode(ERRCODE_RESTRICT_VIOLATION), errmsg("too many values RTEs in INSERT")));
                }
                values_rte = rte;
            }
        }
#ifdef PGXC
    }
#endif

    if ((select_rte != NULL) && (values_rte != NULL)) {
        ereport(ERROR, (errcode(ERRCODE_RESTRICT_VIOLATION), errmsg("both subquery and values RTEs in INSERT")));
    }
    /*
     * Start the query with INSERT INTO relname
     */
    rte = rt_fetch(query->resultRelation, query->rtable);
    Assert(rte->rtekind == RTE_RELATION);

    if (PRETTY_INDENT(context)) {
        context->indentLevel += PRETTYINDENT_STD;
        appendStringInfoChar(buf, ' ');
    }
    appendStringInfo(buf, "INSERT ");
    get_hint_string(query->hintState, buf);
    appendStringInfo(buf, "INTO %s ", generate_relation_name(rte->relid, NIL));

    /* During gray scale upgrade, do not deparse alias since old node cannot parse it. */
    if (t_thrd.proc->workingVersionNum >= UPSERT_WHERE_VERSION_NUM) {
        if (rte->alias != NULL) {
            /* Deparse alias if given */
            appendStringInfo(buf, "AS %s ", quote_identifier(rte->alias->aliasname));
        } else if (rte->eref != NULL && query->upsertClause != NULL) {
            /* Deparse synonym as alias for upsert statement's target table */
            appendStringInfo(buf, "AS %s ", quote_identifier(rte->eref->aliasname));
        }
    }

    /*
     * Add the insert-column-names list.  To handle indirection properly, we
     * need to look for indirection nodes in the top targetlist (if it's
     * INSERT ... SELECT or INSERT ... single VALUES), or in the first
     * expression list of the VALUES RTE (if it's INSERT ... multi VALUES). We
     * assume that all the expression lists will have similar indirection in
     * the latter case.
     */
    if (values_rte != NULL) {
        values_cell = list_head((List*)linitial(values_rte->values_lists));
    } else {
        values_cell = NULL;
    }
    strippedexprs = NIL;
    sep = "";
    if (query->targetList) {
        appendStringInfoChar(buf, '(');
    }

    bool is_fqs_insert = false;
    ListCell* ll = NULL;
    Index std_varno = 0;
    int pos = 0;
    if ((context->is_fqs && is_fqs_inselect) || (context->is_fqs && values_rte != NULL)) {
        is_fqs_insert = true;
    }

    foreach (l, query->targetList) {
        TargetEntry* tle = (TargetEntry*)lfirst(l);
        AttrNumber tle_resno = tle->resno;
        bool col_with_default = true;

        if (tle->resjunk)
            continue; /* ignore junk entries */

        /*
         * Put out name of target column; look in the catalogs, not at
         * tle->resname, since resname will fail to track RENAME.
         */
        if (is_fqs_insert) {
            pos++;
            foreach (ll, query->targetList) {
                TargetEntry* tmptle = (TargetEntry*)lfirst(ll);
                Var* var = get_var_from_node((Node*)(tmptle->expr), func_oid_check_pass);
                if (var == NULL)
                    continue;
                if (std_varno == 0)
                    std_varno = var->varno;
                if (var->varattno == pos && var->varno == std_varno) {
                    tle_resno = tmptle->resno;
                    col_with_default = false;
                    break;
                }
            }
            /*
             * Example:"create table t1(a int, b int default , c int)distribute by hash(a);
             * insert into t1(a,c) values(1,1),(1,1);"
             * Columns with default should not be reversely parsed.
             * It'll go wrong in DN. For example, It will be resolved as
             * insert into t1(a,b,c) values(1,1),(1,1);
             */
            if (col_with_default)
                continue;
        }

        appendStringInfoString(buf, sep);
        sep = ", ";

        const char* attr_str = quote_identifier(get_relid_attribute_name(rte->relid, tle_resno));
        appendStringInfoString(buf, attr_str);

        /*
         * Print any indirection needed (subfields or subscripts), and strip
         * off the top-level nodes representing the indirection assignments.
         */
        if (values_cell != NULL) {
            /* we discard the stripped expression in this case */
            processIndirection((Node*)lfirst(values_cell), context, true);
            values_cell = lnext(values_cell);
        } else {
            if (IsA((Node*)tle->expr, FieldStore)) {
                ListCell* l1 = NULL;
                ListCell* l2 = NULL;
                FieldStore* fstore = (FieldStore*)tle->expr;

                Assert(list_length(fstore->newvals) == list_length(fstore->fieldnums));
                forboth(l1, fstore->newvals, l2, fstore->fieldnums) {
                    FieldStore* single_fstore = NULL;

                    /* build a FieldStore node with single vals here. */
                    single_fstore = makeNode(FieldStore);
                    single_fstore->arg = (Expr*)fstore->arg;
                    single_fstore->newvals = list_make1(lfirst(l1));
                    single_fstore->fieldnums = list_make1_int(lfirst_int(l2));
                    single_fstore->resulttype = fstore->resulttype;

                    strippedexprs = lappend(strippedexprs, processIndirection((Node*)single_fstore, context, true));

                    /* if this is the last one, we don't append sep and column msg. */
                    if (lnext(l1) != NULL) {
                        appendStringInfoString(buf, sep);
                        appendStringInfoString(buf, attr_str);
                    }
                    pfree(single_fstore->newvals);
                    single_fstore->newvals = NULL;
                    pfree(single_fstore->fieldnums);
                    single_fstore->fieldnums = NULL;
                    pfree(single_fstore);
                    single_fstore = NULL;
                }
            } else if (IsA((Node*)tle->expr, ArrayRef)) {
                Node* aref = (Node*)tle->expr;

                for (;;) {
                    strippedexprs = lappend(strippedexprs, processIndirection(aref, context, true));

                    /*
                     * expand multiple entries for the same target attribute if need.
                     * if this is the last one, we don't append sep and column msg.
                     */
                    if (IsA(((ArrayRef*)aref)->refexpr, ArrayRef)) {
                        appendStringInfoString(buf, sep);
                        appendStringInfoString(buf, attr_str);
                        aref = (Node*)((ArrayRef*)aref)->refexpr;
                    } else {
                        break;
                    }
                }
            } else {
                /* normal case */
                strippedexprs = lappend(strippedexprs, (Node*)tle->expr);
            }
        }
        pfree_ext(attr_str);
    }
    if (query->targetList) {
        appendStringInfo(buf, ") ");
    }

    if (select_rte != NULL) {
        /* Add the SELECT */
        get_query_def(select_rte->subquery, buf, NIL, NULL, context->prettyFlags,
            context->wrapColumn, context->indentLevel,
#ifdef PGXC
            context->finalise_aggs, context->sortgroup_colno, context->parser_arg,
#endif /* PGXC */
            context->qrw_phase, context->viewdef, context->is_fqs);
    } else if (values_rte != NULL) {
        /* Add the multi-VALUES expression lists */
        get_values_def(values_rte->values_lists, context);
    } else if (strippedexprs != NULL) {
        /* Add the single-VALUES expression list */
        appendContextKeyword(context, "VALUES (", -PRETTYINDENT_STD, PRETTYINDENT_STD, 2);
        get_rule_expr((Node*)strippedexprs, context, false);
        appendStringInfoChar(buf, ')');
    } else {
        /* No expressions, so it must be DEFAULT VALUES */
        appendStringInfo(buf, "DEFAULT VALUES");
    }

    /* for MERGE INTO  statement for UPSERT, add ON DUPLICATE KEY UPDATE expression */
    if (query->mergeActionList) {
        appendStringInfo(buf, " ON DUPLICATE KEY UPDATE ");

        ListCell* l = NULL;
        bool update = false;
        foreach (l, query->mergeActionList) {
            MergeAction* mc = (MergeAction*)lfirst(l);

            /* only deparse the update clause */
            if (mc->commandType == CMD_UPDATE) {
                update = true;
                get_set_target_list(mc->targetList, rte, context);
            } else {
                continue;
            }
        }
        if (!update) {
            appendStringInfoString(buf, "NOTHING");
        }
    }

    /* for INSERT statement for UPSERT, add ON DUPLICATE KEY UPDATE expression */
    if (query->upsertClause != NULL && query->upsertClause->upsertAction != UPSERT_NONE) {
        appendStringInfoString(buf, " ON DUPLICATE KEY UPDATE ");
        UpsertExpr* upsertClause = query->upsertClause;
        if (upsertClause->upsertAction == UPSERT_NOTHING) {
            appendStringInfoString(buf, "NOTHING");
        } else {
            Assert(!context->is_upsert_clause); /* upsert clause cannot be nested */
            context->is_upsert_clause = true;
            get_update_query_targetlist_def(query, upsertClause->updateTlist, rte, context);
            context->is_upsert_clause = false;
            /* Add WHERE clause for UPDATE clause in UPSERT statement if given */
            if (upsertClause->upsertWhere != NULL) {
                appendContextKeyword(context, " WHERE ", PRETTYINDENT_STD, PRETTYINDENT_STD, 1);
                if (IsA(upsertClause->upsertWhere, List)) {
                    /* Need to revert flattened ands */
                    Expr* expr = make_ands_explicit((List*)upsertClause->upsertWhere);
                    get_rule_expr((Node*)expr, context, false);
                } else {
                    get_rule_expr(upsertClause->upsertWhere, context, false);
                }
            }
        }
    }

    /* Add RETURNING if present */
    if (query->returningList) {
        appendContextKeyword(context, " RETURNING", -PRETTYINDENT_STD, PRETTYINDENT_STD, 1);
        get_target_list(query, query->returningList, context, NULL);
    }
    list_free_ext(strippedexprs);
}

/* ----------
 * get_update_query_def			- Parse back an UPDATE parsetree
 * ----------
 */
static void get_update_query_def(Query* query, deparse_context* context)
{
    StringInfo buf = context->buf;
    RangeTblEntry* rte = NULL;

    /* Insert the WITH clause if given */
    get_with_clause(query, context);

    /*
     * Start the query with UPDATE relname SET
     */
    rte = rt_fetch(query->resultRelation, query->rtable);
    Assert(rte->rtekind == RTE_RELATION);
    if (PRETTY_INDENT(context)) {
        appendStringInfoChar(buf, ' ');
        context->indentLevel += PRETTYINDENT_STD;
    }
    appendStringInfo(buf, "UPDATE ");
    get_hint_string(query->hintState, buf);
    appendStringInfo(buf, "%s%s", only_marker(rte), generate_relation_name(rte->relid, NIL));
    if (rte->alias != NULL) {
        appendStringInfo(buf, " %s", quote_identifier(rte->alias->aliasname));
    }
    appendStringInfoString(buf, " SET ");

    /* Deparse targetlist */
    get_update_query_targetlist_def(query, query->targetList, rte, context);

    /* Add the FROM clause if needed */
    get_from_clause(query, " FROM ", context);

    /* Add a WHERE clause if given */
    if (query->jointree->quals != NULL) {
        appendContextKeyword(context, " WHERE ", PRETTYINDENT_STD, PRETTYINDENT_STD, 1);
        get_rule_expr(query->jointree->quals, context, false);
    }

    /* Add RETURNING if present */
    if (query->returningList) {
        appendContextKeyword(context, " RETURNING", PRETTYINDENT_STD, PRETTYINDENT_STD, 1);
        get_target_list(query, query->returningList, context, NULL);
    }
}

/* ----------
 * get_update_query_targetlist_def         - Parse back an UPDATE targetlist
 * ----------
 */
static void get_update_query_targetlist_def(Query* query, List* targetList,
    RangeTblEntry* rte, deparse_context* context)
{
    StringInfo buf = context->buf;
    ListCell* l = NULL;
    const char* sep;
    /* Add the comma separated list of 'attname = value' */
    sep = "";
    foreach(l, targetList) {
        TargetEntry* tle = (TargetEntry*)lfirst(l);
        Node* expr = NULL;

        if (tle->resjunk)
            continue; /* ignore junk entries */

        appendStringInfoString(buf, sep);
        sep = ", ";

        /*
         * Put out name of target column; look in the catalogs, not at
         * tle->resname, since resname will fail to track RENAME.
         */
        appendStringInfoString(buf, quote_identifier(get_relid_attribute_name(rte->relid, tle->resno)));

        /*
         * Print any indirection needed (subfields or subscripts), and strip
         * off the top-level nodes representing the indirection assignments.
         */
        if (IsA((Node*)tle->expr, FieldStore)) {
            ListCell* l1 = NULL;
            ListCell* l2 = NULL;
            FieldStore* fstore = (FieldStore*)tle->expr;
            FieldStore* single_fstore = makeNode(FieldStore);
            single_fstore->arg = (Expr*)fstore->arg;
            single_fstore->resulttype = fstore->resulttype;
            Assert(list_length(fstore->newvals) == list_length(fstore->fieldnums));
            /* expand multiple entries for the same target attribute if need. */
            forboth(l1, fstore->newvals, l2, fstore->fieldnums)
            {
                single_fstore->newvals = list_make1(lfirst(l1));
                single_fstore->fieldnums = list_make1_int(lfirst_int(l2));

                expr = processIndirection((Node*)single_fstore, context, true);
                appendStringInfo(buf, " = ");
                get_rule_expr(expr, context, false);

                /* if this is the last one, we don't append sep and column msg. */
                if (lnext(l1) != NULL) {
                    appendStringInfoString(buf, sep);
                    appendStringInfoString(buf, quote_identifier(get_relid_attribute_name(rte->relid, tle->resno)));
                }
                pfree(single_fstore->newvals);
                single_fstore->newvals = NULL;
                pfree(single_fstore->fieldnums);
                single_fstore->fieldnums = NULL;
            }
            pfree(single_fstore);
            single_fstore = NULL;
        } else if (IsA((Node*)tle->expr, ArrayRef)) {
            Node* aref = (Node*)tle->expr;

            for (;;) {
                expr = processIndirection(aref, context, true);
                appendStringInfo(buf, " = ");
                get_rule_expr(expr, context, false);

                /*
                 * expand multiple entries for the same target attribute if need
                 * if this is the last one, we don't append sep and column msg.
                 */
                if (IsA(((ArrayRef*)aref)->refexpr, ArrayRef)) {
                    appendStringInfoString(buf, sep);
                    appendStringInfoString(buf, quote_identifier(get_relid_attribute_name(rte->relid, tle->resno)));
                    aref = (Node*)((ArrayRef*)aref)->refexpr;
                } else {
                    break;
                }
            }
        } else {
            /* normal case */
            appendStringInfo(buf, " = ");
            get_rule_expr((Node*)tle->expr, context, false);
        }
    }
}

/* ----------
 * get_delete_query_def			- Parse back a DELETE parsetree
 * ----------
 */
static void get_delete_query_def(Query* query, deparse_context* context)
{
    StringInfo buf = context->buf;
    RangeTblEntry* rte = NULL;

    /* Insert the WITH clause if given */
    get_with_clause(query, context);

    /*
     * Start the query with DELETE FROM relname
     */
    rte = rt_fetch(query->resultRelation, query->rtable);
    Assert(rte->rtekind == RTE_RELATION);
    if (PRETTY_INDENT(context)) {
        appendStringInfoChar(buf, ' ');
        context->indentLevel += PRETTYINDENT_STD;
    }
    appendStringInfo(buf, "DELETE ");
    get_hint_string(query->hintState, buf);
    appendStringInfo(buf, "FROM %s%s", only_marker(rte), generate_relation_name(rte->relid, NIL));
    if (rte->alias != NULL)
        appendStringInfo(buf, " %s", quote_identifier(rte->alias->aliasname));

    /* Add the USING clause if given */
    get_from_clause(query, " USING ", context);

    /* Add a WHERE clause if given */
    if (query->jointree->quals != NULL) {
        appendContextKeyword(context, " WHERE ", -PRETTYINDENT_STD, PRETTYINDENT_STD, 1);
        get_rule_expr(query->jointree->quals, context, false);
    }

    /* Add a Limit clause if given */
    if (query->limitCount != NULL) {
        appendContextKeyword(context, " LIMIT ", -PRETTYINDENT_STD, PRETTYINDENT_STD, 0);
        get_rule_expr(query->limitCount, context, false);
    }

    /* Add RETURNING if present */
    if (query->returningList) {
        appendContextKeyword(context, " RETURNING", -PRETTYINDENT_STD, PRETTYINDENT_STD, 1);
        get_target_list(query, query->returningList, context, NULL);
    }
}

/* 
 * only support add colum and drop column, support text and numeric, number datatype,
 * if timeseries table support more datatype, should test gs_expand if it works well
 */
void get_utility_stmt_def(AlterTableStmt* stmt, StringInfo buf)
{
    Assert(stmt != NULL);
    RangeVar* relation = stmt->relation;
    
    appendStringInfo(buf, "ALTER TABLE ");

    if (relation->schemaname && relation->schemaname[0]) {
        appendStringInfo(buf, "%s.", quote_identifier(relation->schemaname));
    }
    appendStringInfo(buf, "%s", quote_identifier(relation->relname));
    if (stmt->cmds) {
        /*
         * now we only support have one action, does not support 
         * alter table cpu add column a numeric tsfield,add column c numeric tsfield;
         */            
        AlterTableCmd* cmd = (AlterTableCmd*)linitial(stmt->cmds);
        switch (cmd->subtype) {
            case AT_AddColumn: {
                appendStringInfo(buf, " ADD COLUMN ");
                ColumnDef* colDef = (ColumnDef*)cmd->def;
                
                appendStringInfo(buf, " %s", colDef->colname);
                
                char* columntype = NULL;
                if (list_length(colDef->typname->names) == 2) {
                    /* numeric case, so the first is pg_catalog, the second is column type name */
                    char* colcatalog = strVal(linitial(colDef->typname->names));
                    if (strcmp(colcatalog, "pg_catalog") != 0) {
                        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), 
                        errmsg("unsupported column type when converting to string")));
                    }
                    columntype = strVal(lsecond(colDef->typname->names));
                } else if (list_length(colDef->typname->names) == 1) {
                    /* text case */
                    columntype = strVal(linitial(colDef->typname->names));
                } else {
                    ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), 
                    errmsg("unsupported column type when converting to string")));
                }
                
                appendStringInfo(buf, " %s", columntype);
                if (colDef->kvtype == ATT_KV_FIELD) {
                    appendStringInfo(buf, " TSFIELD");
                } else if (colDef->kvtype == ATT_KV_TAG) {
                    appendStringInfo(buf, " TSTAG");
                } else if (colDef->kvtype == ATT_KV_TIMETAG) {
                    appendStringInfo(buf, " TSTIME");
                }          
            } break;

            case AT_DropColumn: {
                appendStringInfo(buf, " DROP COLUMN %s", cmd->name);
                if (cmd->behavior) {
                    appendStringInfo(buf, (cmd->behavior == DROP_CASCADE) ? " CASCADE" : " RESTRICT");
                }
            } break;

            default:
                ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), 
                        errmsg("unsupported cmd alter table type %d", cmd->subtype)));
                break;
        }
    } else {
        appendStringInfo(buf, " REDISANYVALUE");
    }

}

static void AppendDistributeByColDef(StringInfo buf, CreateStmt* stmt) 
{
    int i = 0;
    ListCell *cell = NULL;
    appendStringInfo(buf, "(");
    foreach (cell, stmt->distributeby->colname) {
        if (i == 0) {
            appendStringInfo(buf, "%s", strVal(lfirst(cell)));
        } else {
            appendStringInfo(buf, ", %s", strVal(lfirst(cell)));
        }
        i++;
    }
    appendStringInfo(buf, ")");
}

static bool IsDefaultSlice(StringInfo buf, RangePartitionDefState* sliceDef)
{
    ParseState* pstate = make_parsestate(NULL);
    Node* valueNode = (Node *)list_nth(sliceDef->boundary, 0);
    Const* valueConst = (Const *)transformExpr(pstate, valueNode);

    if (valueConst->ismaxvalue) {
        appendStringInfo(buf, "DEFAULT");
        free_parsestate(pstate);
        return true;
    } else {
        free_parsestate(pstate);
        return false;
    }
}

static char* EscapeQuotes(const char* src)
{
    int len = strlen(src), i, j;
    char* result = (char*)palloc(len * 2 + 1);

    for (i = 0, j = 0; i < len; i++) {
        /* backslash is already escaped */
        if (SQL_STR_DOUBLE(src[i], false)) {
            result[j++] = src[i];
        }
        result[j++] = src[i];
    }
    result[j] = '\0';
    return result;
}

static void AppendSliceItemDDL(StringInfo buf, RangePartitionDefState* sliceDef, CreateStmt* stmt, bool parentheses) 
{
    Oid typoutput;
    bool typIsVarlena = false;
    ParseState* pstate = make_parsestate(NULL);
    Const* valueConst = NULL;
    Node* valueNode = NULL;
    const char* valueString = NULL;
    char* escapedString = NULL;
    ListCell* cell = NULL;
    ListCell* cell2 = NULL;
    int i = 0;

    if (parentheses) {
        appendStringInfo(buf, "(");
    }

    forboth(cell, sliceDef->boundary, cell2, stmt->tableElts) {
        if (i > 0) {
            appendStringInfo(buf, ", ");
        }
        valueNode = (Node *)lfirst(cell);
        valueConst = (Const *)transformExpr(pstate, valueNode);

        if (valueConst->ismaxvalue) {
            /* already took care of DEFAULT slice for LIST tables in IsDefaultSlice */
            appendStringInfo(buf, "MAXVALUE");
        } else {
            getTypeOutputInfo(valueConst->consttype, &typoutput, &typIsVarlena);
            valueString = OidOutputFunctionCall(typoutput, valueConst->constvalue);
            escapedString = EscapeQuotes(valueString);
            appendStringInfo(buf, "'%s'", escapedString);
        }
        i++;
    }

    if (parentheses) {
        appendStringInfo(buf, ")");
    }

    pfree_ext(escapedString);
    return;    
}

static void AppendSliceDN(StringInfo buf, Node* n)
{
    char* dnName = NULL;
    switch (n->type) {
        case T_RangePartitionDefState: {
            RangePartitionDefState *sliceDef = (RangePartitionDefState *)n;
            dnName = sliceDef->tablespacename;
            break; 
        }
        case T_RangePartitionStartEndDefState: {
            RangePartitionStartEndDefState *sliceDef = (RangePartitionStartEndDefState *)n;
            dnName = sliceDef->tableSpaceName;
            break; 
        }
        case T_ListSliceDefState: {
            ListSliceDefState *sliceDef = (ListSliceDefState *)n;
            dnName = sliceDef->datanode_name;
            break;
        }
        default:
            break;
    }
    if (dnName == NULL) {
        return;
    }
    appendStringInfo(buf, " DATANODE %s", dnName);
}

static void AppendStartEndElement(StringInfo buf, List* valueList, CreateStmt* stmt)
{
    Oid typoutput;
    bool typIsVarlena = false;
    Node* valueNode = NULL;
    Const* valueConst = NULL;
    const char* valueString = NULL;
    ParseState* pstate = NULL;
    char* escapedString = NULL;

    if (list_length(valueList) != 1) {
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                    errmsg("distributed table has too many distribution keys."),
                    errhint("start/end syntax requires a distributed table with only one distribution key.")));
    }

    pstate = make_parsestate(NULL);
    valueNode = (Node *)list_nth(valueList, 0);
    valueConst = (Const *)transformExpr(pstate, valueNode);
    if (valueConst->ismaxvalue) {
        appendStringInfo(buf, "MAXVALUE");
    } else {
        getTypeOutputInfo(valueConst->consttype, &typoutput, &typIsVarlena);
        valueString = OidOutputFunctionCall(typoutput, valueConst->constvalue);
        escapedString = EscapeQuotes(valueString);
        appendStringInfo(buf, "'%s'", escapedString);
    }
    pfree_ext(escapedString);
    free_parsestate(pstate);
}

static void AppendStartEndDDL(StringInfo buf, RangePartitionStartEndDefState* sliceDef, CreateStmt* stmt)
{
    List* startValue = sliceDef->startValue;
    List* endValue = sliceDef->endValue;
    List* everyValue = sliceDef->everyValue;

    appendStringInfo(buf, "SLICE %s ", sliceDef->partitionName);
    if (startValue != NIL) {
        appendStringInfo(buf, "START(");
        AppendStartEndElement(buf, startValue, stmt);
        appendStringInfo(buf, ") ");
    }
    if (endValue != NIL) {
        appendStringInfo(buf, "END(");
        AppendStartEndElement(buf, endValue, stmt);
        appendStringInfo(buf, ") ");
    }
    if (everyValue != NIL) {
        appendStringInfo(buf, "EVERY(");
        AppendStartEndElement(buf, everyValue, stmt);
        appendStringInfo(buf, ") ");
    }
}

static void AppendValuesLessThanDDL(StringInfo buf, Node* node, CreateStmt* stmt)
{
    RangePartitionDefState *sliceDef = (RangePartitionDefState *)node;
    appendStringInfo(buf, "SLICE %s VALUES LESS THAN (", sliceDef->partitionName);
    AppendSliceItemDDL(buf, sliceDef, stmt, false);
    appendStringInfo(buf, ")");
    AppendSliceDN(buf, node);
    pfree(sliceDef);
}

static void AppendListDDL(StringInfo buf, Node* node, CreateStmt* stmt, int keyNum)
{
    int listIdx = 0;
    ListSliceDefState *listSlice = (ListSliceDefState *)node;
    RangePartitionDefState *sliceDef = makeNode(RangePartitionDefState);
    ListCell *cell = NULL;
    bool parentheses = keyNum > 1;
    
    /* wrap every list boundary to RangePartitionDefState, then append to the buffer */
    appendStringInfo(buf, "SLICE %s VALUES (", listSlice->name);
    foreach (cell, listSlice->boundaries) {
        sliceDef->partitionName = listSlice->name;
        sliceDef->boundary = (List *)lfirst(cell);
        if (listIdx > 0) {
            appendStringInfo(buf, ", ");
        }
        if (IsDefaultSlice(buf, sliceDef)) {
            break;
        } else {
            AppendSliceItemDDL(buf, sliceDef, stmt, parentheses);
        }
        listIdx++;
    }
    appendStringInfo(buf, ")");
    AppendSliceDN(buf, node);
    pfree(sliceDef);
}

static void AppendSliceDDL(StringInfo buf, CreateStmt* stmt) 
{
    if (stmt->distributeby->distState == NULL) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), 
            errmsg("Distribution Info is not defined.")));
    }

    int idx = 0;
    DistributeBy* distributeby = stmt->distributeby;
    ListCell *cell = NULL;
    DistState *distState = distributeby->distState;
    int keyNum = list_length(distributeby->colname);

    /* Slice References case, needs to append slice definitions of the base table! */
    if (distributeby->distState->refTableName != NULL) {
        appendStringInfo(buf, " SLICE REFERENCES %s ", distributeby->distState->refTableName);
        return;
    }

    appendStringInfo(buf, "(");
    foreach (cell, distState->sliceList) {
        Node *node = (Node *)lfirst(cell);
        if (idx > 0) {
            appendStringInfo(buf, ", ");
        }
        switch (node->type) {
            case T_RangePartitionDefState: {
                AppendValuesLessThanDDL(buf, node, stmt);
                break;
            }
            case T_ListSliceDefState: {
                AppendListDDL(buf, node, stmt, keyNum);
                break;
            }
            case T_RangePartitionStartEndDefState: {
                /* no transformation to less-than struct; deparse back to DDL as-is */
                RangePartitionStartEndDefState* sliceDef = (RangePartitionStartEndDefState *)node;
                AppendStartEndDDL(buf, sliceDef, stmt);
                break;
            }
            default: {
                ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), 
                    errmsg("Unrecognized distribution specification.")));
                break;
            }
        }
        idx++;
    }
    appendStringInfo(buf, ")"); /* end parentheses for whole range/list distribution */
}

/* ----------
 * get_utility_query_def			- Parse back a UTILITY parsetree
 * ----------
 */
static void get_utility_query_def(Query* query, deparse_context* context)
{
    StringInfo buf = context->buf;

    if (query->utilityStmt && IsA(query->utilityStmt, NotifyStmt)) {
        NotifyStmt* stmt = (NotifyStmt*)query->utilityStmt;

        appendContextKeyword(context, "", 0, PRETTYINDENT_STD, 1);
        appendStringInfo(buf, "NOTIFY %s", quote_identifier(stmt->conditionname));
        if (stmt->payload) {
            appendStringInfoString(buf, ", ");
            simple_quote_literal(buf, stmt->payload);
        }
    }
#ifdef PGXC
    else if (query->utilityStmt && IsA(query->utilityStmt, CreateStmt)) {
        CreateStmt* stmt = (CreateStmt*)query->utilityStmt;
        ListCell* column = NULL;
        const char* delimiter = "";
        RangeVar* relation = stmt->relation;
        bool istemp = (relation->relpersistence == RELPERSISTENCE_TEMP);
        bool isunlogged = (relation->relpersistence == RELPERSISTENCE_UNLOGGED);
        bool is_matview = stmt->relkind == OBJECT_MATVIEW ? true : false;

        if (istemp && relation->schemaname)
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_TABLE_DEFINITION), errmsg("temporary tables cannot specify a schema name")));

        appendStringInfo(buf, "CREATE %s %s TABLE ", istemp ? "TEMP" : "", isunlogged ? "UNLOGGED" : "");

        if (relation->schemaname && relation->schemaname[0])
            appendStringInfo(buf, "%s.", quote_identifier(relation->schemaname));
        appendStringInfo(buf, "%s", quote_identifier(relation->relname));

        appendStringInfo(buf, "(");
        foreach (column, stmt->tableElts) {
            Node* node = (Node*)lfirst(column);

            appendStringInfo(buf, "%s", delimiter);
            delimiter = ", ";

            if (IsA(node, ColumnDef)) {
                ColumnDef* coldef = (ColumnDef*)node;
                TypeName* tpname = coldef->typname;
                ClientLogicColumnRef *coldef_enc = coldef->clientLogicColumnRef;

                /* error out if we have no recourse at all */
                if (!OidIsValid(tpname->typeOid))
                    ereport(
                        ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("improper type oid: \"%u\"", tpname->typeOid)));

                /* if the column is encrypted, we should convert its data type */
                if (coldef_enc != NULL && coldef_enc->dest_typname != NULL) {
                    tpname = coldef_enc->dest_typname;
                }

                /* get typename from the oid */
                appendStringInfo(buf,
                    "%s %s",
                    quote_identifier(coldef->colname),
                    format_type_with_typemod(tpname->typeOid, tpname->typemod));

                // add the compress mode for this column
                switch (coldef->cmprs_mode) {
                    case ATT_CMPR_NOCOMPRESS:
                        appendStringInfoString(buf, " NOCOMPRESS ");
                        break;
                    case ATT_CMPR_DELTA:
                        appendStringInfoString(buf, " DELTA ");
                        break;
                    case ATT_CMPR_DICTIONARY:
                        appendStringInfoString(buf, " DICTIONARY ");
                        break;
                    case ATT_CMPR_PREFIX:
                        appendStringInfoString(buf, " PREFIX ");
                        break;
                    case ATT_CMPR_NUMSTR:
                        appendStringInfoString(buf, " NUMSTR ");
                        break;
                    default:
                        // do nothing
                        break;
                }
            } else
                ereport(
                    ERROR, (errcode(ERRCODE_INVALID_COLUMN_DEFINITION), errmsg("Invalid table column definition.")));
        }
        appendStringInfo(buf, ")");

        /* Append storage parameters, like for instance WITH (OIDS) */
        if (list_length(stmt->options) > 0) {
            Datum reloptions;
            static const char* validnsps[] = HEAP_RELOPT_NAMESPACES;

            reloptions = transformRelOptions((Datum)0, stmt->options, NULL, validnsps, false, false);

            if (reloptions) {
                Datum sep, txt;
                /* Below is inspired from flatten_reloptions() */
                sep = CStringGetTextDatum(", ");
                txt = OidFunctionCall2(F_ARRAY_TO_TEXT, reloptions, sep);
                appendStringInfo(buf, " WITH (%s)", TextDatumGetCString(txt));
            }
        }

        /* add the on commit clauses for temporary tables */
        switch (stmt->oncommit) {
            case ONCOMMIT_NOOP:
                /* do nothing */
                break;

            case ONCOMMIT_PRESERVE_ROWS:
                appendStringInfo(buf, " ON COMMIT PRESERVE ROWS");
                break;

            case ONCOMMIT_DELETE_ROWS:
                appendStringInfo(buf, " ON COMMIT DELETE ROWS");
                break;

            case ONCOMMIT_DROP:
                appendStringInfo(buf, " ON COMMIT DROP");
                break;
            default:
                break;
        }

        // add the compress clause for temporary tables
        //
        switch (stmt->row_compress) {
            case REL_CMPRS_NOT_SUPPORT:
            case REL_CMPRS_PAGE_PLAIN:
                appendStringInfoString(buf, " NOCOMPRESS ");
                break;
            case REL_CMPRS_FIELDS_EXTRACT:
                appendStringInfoString(buf, " COMPRESS ");
                break;
            case REL_CMPRS_MAX_TYPE:
                Assert(false);
                break;
            default:
                break;
        }

        if (stmt->tablespacename)
            appendStringInfo(buf, " TABLESPACE %s ", quote_identifier(stmt->tablespacename));

        if (stmt->distributeby) {
            ListCell* cell = NULL;
            int i = 0;
            /* add the on commit clauses for temporary tables */
            switch (stmt->distributeby->disttype) {
                case DISTTYPE_REPLICATION:
                    appendStringInfo(buf, " DISTRIBUTE BY REPLICATION");
                    break;

                case DISTTYPE_HASH:
                    i = 0;
                    appendStringInfo(buf, " DISTRIBUTE BY HASH(");
                    foreach (cell, stmt->distributeby->colname) {
                        if (0 == i) {
                            appendStringInfo(buf, "%s", quote_identifier(strVal(lfirst(cell))));
                        } else {
                            appendStringInfo(buf, " ,%s", quote_identifier(strVal(lfirst(cell))));
                        }
                        i++;
                    }
                    appendStringInfo(buf, ")");
                    break;

                case DISTTYPE_ROUNDROBIN:
                    appendStringInfo(buf, " DISTRIBUTE BY ROUNDROBIN");
                    break;

                case DISTTYPE_MODULO:
                    i = 0;
                    appendStringInfo(buf, " DISTRIBUTE BY MODULO(");
                    foreach (cell, stmt->distributeby->colname) {
                        if (0 == i) {
                            appendStringInfo(buf, "%s", strVal(lfirst(cell)));
                        } else {
                            appendStringInfo(buf, " ,%s", strVal(lfirst(cell)));
                        }
                        i++;
                    }
                    appendStringInfo(buf, ")");
                    break;

                case DISTTYPE_RANGE: {
                    appendStringInfo(buf, " DISTRIBUTE BY RANGE");
                    AppendDistributeByColDef(buf, stmt);
                    AppendSliceDDL(buf, stmt);
                    break;
                }

                case DISTTYPE_LIST: {
                    appendStringInfo(buf, " DISTRIBUTE BY LIST");
                    AppendDistributeByColDef(buf, stmt);
                    AppendSliceDDL(buf, stmt);
                    break;
                }

                default:
                    ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("Invalid distribution type")));
            }
        }

        if (stmt->subcluster) {
            ListCell* cell = NULL;

            switch (stmt->subcluster->clustertype) {
                case SUBCLUSTER_NODE:
                    appendStringInfo(buf, " TO NODE (");

                    /* Add node members */
                    Assert(stmt->subcluster->members);
                    foreach (cell, stmt->subcluster->members) {
                        appendStringInfo(buf, " %s", strVal(lfirst(cell)));
                        if (cell->next)
                            appendStringInfo(buf, ",");
                    }
                    appendStringInfo(buf, ")");
                    break;

                case SUBCLUSTER_GROUP:
                    appendStringInfo(buf, " TO GROUP");

                    /* Add group members */
                    Assert(stmt->subcluster->members);
                    foreach (cell, stmt->subcluster->members) {
                        appendStringInfo(buf, " %s", quote_identifier(strVal(lfirst(cell))));
                        if (cell->next)
                            appendStringInfo(buf, ",");
                    }
                    break;

                case SUBCLUSTER_NONE:
                default:
                    /* Nothing to do */
                    break;
            }
        }
        appendStringInfo(buf, " %s ",  is_matview ? "FOR MATERIALIZED VIEW" : "");
    }else if (query->utilityStmt && IsA(query->utilityStmt, ViewStmt)) {
        ViewStmt* stmt = (ViewStmt*)query->utilityStmt;
        Query* query = (Query *)stmt->query;
        const char* delimiter = "";
        RangeVar* relation = stmt->view;

        if (stmt->relkind == OBJECT_MATVIEW) {
            if (stmt->ivm) {
                appendStringInfo(buf, "CREATE INCREMENTAL MATERIALIZED VIEW ");
            } else {
                appendStringInfo(buf, "CREATE MATERIALIZED VIEW ");
            }
        } else {
            appendStringInfo(buf, "CREATE VIEW ");
        }

        if (relation->schemaname && relation->schemaname[0])
            appendStringInfo(buf, "%s.", quote_identifier(relation->schemaname));
        appendStringInfo(buf, "%s", quote_identifier(relation->relname));

        appendStringInfo(buf, " ( ");

        ListCell* lc  = list_head(stmt->aliases);
        ListCell* col = NULL;
        List* tlist = query->targetList;
        foreach (col, tlist) {
            TargetEntry* tle = (TargetEntry*)lfirst(col);

            /* Ignore junk columns from the targetlist */
            if (tle->resjunk)
                continue;

            appendStringInfo(buf, "%s", delimiter);
            delimiter = ", ";

            /* Take the column name specified if any */
            if (lc != NULL) {
                appendStringInfo(buf, " %s ",  quote_identifier(strVal(lfirst(lc))));
                lc = lnext(lc);
            } else {
                appendStringInfo(buf, " %s ",  quote_identifier(tle->resname));
            }
        }

        if (lc != NULL) {
            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                    errmsg("CREATE %s specifies too many column names",
                           stmt->relkind == OBJECT_MATVIEW ? "MATERIALIZED VIEW" : "VIEW AS")));
        }

        StringInfo select_sql = makeStringInfo();
        deparse_query((Query*)query, select_sql, NIL, false, false);
        appendStringInfo(buf, " ) AS %s", select_sql->data);
    }
#endif
    /*
     * construct the query tree, if have rewrite rule, get the QueryTree from system table pg_rewrite
     * and then convert it to original string
     */
    else if (query->utilityStmt && IsA(query->utilityStmt, CopyStmt)) {
        CopyStmt* stmt = (CopyStmt*)query->utilityStmt;
        RangeVar* relation = stmt->relation;
        appendStringInfo(buf, "COPY ");
        if (relation->schemaname && relation->schemaname[0]) {
            appendStringInfo(buf, "%s.", quote_identifier(relation->schemaname));
        }
        appendStringInfo(buf, "%s", quote_identifier(relation->relname));
        if (stmt->is_from) {
            appendStringInfo(buf, " FROM ");
        } else {
            appendStringInfo(buf, " TO ");
        }
        if (stmt->filename) {
            appendStringInfo(buf, "\'%s\'", stmt->filename);
        } else {
            /*
             * ignore the STDIN and STDOUT, redisanyvalue is same as stdin, 
             * if should get the true sql, need to support
             */
            appendStringInfo(buf, "REDISANYVALUE");
        }
    } else if (query->utilityStmt && IsA(query->utilityStmt, AlterTableStmt)) {
        AlterTableStmt* stmt = (AlterTableStmt*)query->utilityStmt;
        get_utility_stmt_def(stmt, buf);    
    } else {
        /* Currently only NOTIFY utility commands can appear in rules */
        ereport(ERROR, (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE), errmsg("unexpected utility statement type")));
    }
}

#ifdef PGXC
static void getVariableFromRTERemoteDummy(
    Var* var, deparse_namespace* dpns, deparse_context* context, int netlevelsup, bool no_alias)
{
    TargetEntry* tle = NULL;
    RemoteQuery* rqplan = NULL;
    Assert(netlevelsup == 0);
    StringInfo buf = context->buf;

    /*
     * Get the expression representing the given Var from base_tlist of the RemoteQuery.
     * If the planstate is ModifyTableState, use ModifyTableState's mt_remoterels due
     * to the varno has been changed by ModifyTable's remote_plan in set_plan_reference.
     */
    if (IsA(dpns->planstate, RemoteQueryState)) {
        rqplan = castNode(RemoteQuery, dpns->planstate->plan);
    } else if (IsA(dpns->planstate, ModifyTableState)) {
        ModifyTableState *planstate = castNode(ModifyTableState, dpns->planstate);
        Assert(planstate->mt_remoterels != NULL);
        rqplan = castNode(RemoteQuery, planstate->mt_remoterels[0]->plan);
    }

    if (rqplan != NULL) {
        tle = get_tle_by_resno(rqplan->base_tlist, var->varattno);
        if (tle == NULL)
            ereport(ERROR,
                   (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                    errmsg("bogus varattno for remotequery var: %d", var->varattno)));
        /*
         * Force parentheses because our caller probably assumed a Var is a
         * simple expression.
         */
        if (!IsA(tle->expr, Var))
            appendStringInfoChar(buf, '(');
        get_rule_expr((Node*)tle->expr, context, true, no_alias);
        if (!IsA(tle->expr, Var))
            appendStringInfoChar(buf, ')');
    } else {
        /* we do not know why this happen yet, but core is not acceptable */
        appendStringInfoChar(buf, '?');
    }
    
    return;
}
#endif

/*
 * Display a Var appropriately.
 *
 * In some cases (currently only when recursing into an unnamed join)
 * the Var's varlevelsup has to be interpreted with respect to a context
 * above the current one; levelsup indicates the offset.
 *
 * If istoplevel is TRUE, the Var is at the top level of a SELECT's
 * targetlist, which means we need special treatment of whole-row Vars.
 * Instead of the normal "tab.*", we'll print "tab.*::typename", which is a
 * dirty hack to prevent "tab.*" from being expanded into multiple columns.
 * (The parser will strip the useless coercion, so no inefficiency is added in
 * dump and reload.)  We used to print just "tab" in such cases, but that is
 * ambiguous and will yield the wrong result if "tab" is also a plain column
 * name in the query.
 *
 * Returns the attname of the Var, or NULL if the Var has no attname (because
 * it is a whole-row Var).
 */
static char* get_variable(
    Var* var, int levelsup, bool istoplevel, deparse_context* context, bool for_star, bool no_alias)
{
    StringInfo buf = context->buf;
    RangeTblEntry* rte = NULL;
    AttrNumber attnum = InvalidAttrNumber;
    int netlevelsup;
    deparse_namespace* dpns = NULL;
    char* schemaname = NULL;
    char* refname = NULL;
    char* attname = NULL;

    /* Find appropriate nesting depth */
    netlevelsup = var->varlevelsup + levelsup;
    if (netlevelsup >= list_length(context->namespaces)) {
        if (context->qrw_phase) {
            appendStringInfo(buf, "Param(%d,%d)", var->varlevelsup, levelsup);
            return NULL;
        } else
            ereport(ERROR,
                (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                    errmsg("bogus varlevelsup: %d offset %d", var->varlevelsup, levelsup)));
    }
    dpns = (deparse_namespace*)list_nth(context->namespaces, netlevelsup);

    /*
     * Try to find the relevant RTE in this rtable.  In a plan tree, it's
     * likely that varno is OUTER_VAR or INNER_VAR, in which case we must dig
     * down into the subplans, or INDEX_VAR, which is resolved similarly.
     */
#ifdef PGXC
    if (dpns->remotequery) {
        rte = rt_fetch(1, dpns->rtable);
        attnum = var->varattno;
    } else
#endif
        if (var->varno >= 1 && var->varno <= (uint)list_length(dpns->rtable)) {
        rte = rt_fetch(var->varno, dpns->rtable);
        attnum = var->varattno;
    } else if ((var->varno == OUTER_VAR) && dpns->outer_tlist) {
        TargetEntry* tle = NULL;
        deparse_namespace save_dpns;

        tle = get_tle_by_resno(dpns->outer_tlist, var->varattno);
        if (tle == NULL)
            ereport(ERROR,
                (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                    errmsg("bogus varattno for OUTER_VAR var: %d", var->varattno)));

        Assert(netlevelsup == 0);
        push_child_plan(dpns, dpns->outer_planstate, &save_dpns);

        /*
         * Force parentheses because our caller probably assumed a Var is a
         * simple expression.
         */
        if (!IsA(tle->expr, Var))
            appendStringInfoChar(buf, '(');
        get_rule_expr((Node*)tle->expr, context, true, no_alias);
        if (!IsA(tle->expr, Var))
            appendStringInfoChar(buf, ')');

        pop_child_plan(dpns, &save_dpns);
        return NULL;
    } else if (var->varno == INNER_VAR && dpns->inner_tlist) {
        TargetEntry* tle = NULL;
        deparse_namespace save_dpns;

        tle = get_tle_by_resno(dpns->inner_tlist, var->varattno);
        if (tle == NULL)
            ereport(ERROR,
                (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                    errmsg("bogus varattno for INNER_VAR var: %d", var->varattno)));

        Assert(netlevelsup == 0);
        push_child_plan(dpns, dpns->inner_planstate, &save_dpns);

        /*
         * Force parentheses because our caller probably assumed a Var is a
         * simple expression.
         */
        if (!IsA(tle->expr, Var))
            appendStringInfoChar(buf, '(');
        get_rule_expr((Node*)tle->expr, context, true, no_alias);
        if (!IsA(tle->expr, Var))
            appendStringInfoChar(buf, ')');

        pop_child_plan(dpns, &save_dpns);
        return NULL;
    } else if (var->varno == INDEX_VAR && dpns->index_tlist) {
        TargetEntry* tle = NULL;

        tle = get_tle_by_resno(dpns->index_tlist, var->varattno);
        if (tle == NULL)
            ereport(ERROR,
                (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                    errmsg("bogus varattno for INDEX_VAR var: %d", var->varattno)));

        Assert(netlevelsup == 0);

        /*
         * Force parentheses because our caller probably assumed a Var is a
         * simple expression.
         */
        if (!IsA(tle->expr, Var))
            appendStringInfoChar(buf, '(');
        get_rule_expr((Node*)tle->expr, context, true, no_alias);
        if (!IsA(tle->expr, Var))
            appendStringInfoChar(buf, ')');

        return NULL;
    } else {
        ereport(ERROR, (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE), errmsg("bogus varno: %d", var->varno)));
        return NULL; /* keep compiler quiet */
    }

    /*
     * The planner will sometimes emit Vars referencing resjunk elements of a
     * subquery's target list (this is currently only possible if it chooses
     * to generate a "physical tlist" for a SubqueryScan or CteScan node).
     * Although we prefer to print subquery-referencing Vars using the
     * subquery's alias, that's not possible for resjunk items since they have
     * no alias.  So in that case, drill down to the subplan and print the
     * contents of the referenced tlist item.  This works because in a plan
     * tree, such Vars can only occur in a SubqueryScan or CteScan node, and
     * we'll have set dpns->inner_planstate to reference the child plan node.
     */
    if ((rte->rtekind == RTE_SUBQUERY || rte->rtekind == RTE_CTE) && attnum > list_length(rte->eref->colnames) &&
        dpns->inner_planstate) {
        TargetEntry* tle = NULL;
        deparse_namespace save_dpns;

        tle = get_tle_by_resno(dpns->inner_tlist, var->varattno);
        if (tle == NULL)
            ereport(ERROR,
                (errcode(ERRCODE_UNEXPECTED_NULL_VALUE), errmsg("bogus varattno for subquery var: %d", var->varattno)));

        Assert(netlevelsup == 0);
        push_child_plan(dpns, dpns->inner_planstate, &save_dpns);

        /*
         * Force parentheses because our caller probably assumed a Var is a
         * simple expression.
         */
        if (!IsA(tle->expr, Var))
            appendStringInfoChar(buf, '(');
        get_rule_expr((Node*)tle->expr, context, true, no_alias);
        if (!IsA(tle->expr, Var))
            appendStringInfoChar(buf, ')');

        pop_child_plan(dpns, &save_dpns);
        return NULL;
    }

#ifdef PGXC
    if (rte->rtekind == RTE_REMOTE_DUMMY && attnum > list_length(rte->eref->colnames) && dpns->planstate) {
        getVariableFromRTERemoteDummy(var, dpns, context, netlevelsup, no_alias);
        return NULL;
    }
#endif /* PGXC */

    /* Identify names to use */
    schemaname = NULL; /* default assumptions */
    refname = rte->eref->aliasname;

    /* Exceptions occur only if the RTE is alias-less */
    if (rte->alias == NULL) {
        if (rte->rtekind == RTE_RELATION || no_alias) {
            /*
             * It's possible that use of the bare refname would find another
             * more-closely-nested RTE, or be ambiguous, in which case we need
             * to specify the schemaname to avoid these errors.
             */
            if (find_rte_by_refname(rte->eref->aliasname, context) != rte || no_alias)
                schemaname = get_namespace_name(get_rel_namespace(rte->relid));
#ifndef ENABLE_MULTIPLE_NODES
            if (no_alias)
                refname = rte->relname;
#endif /* ENABLE_MULTIPLE_NODES */
        } else if (rte->rtekind == RTE_JOIN) {
            /*
             * If it's an unnamed join, look at the expansion of the alias
             * variable.  If it's a simple reference to one of the input vars
             * then recursively print the name of that var, instead. (This
             * allows correct decompiling of cases where there are identically
             * named columns on both sides of the join.) When it's not a
             * simple reference, we have to just print the unqualified
             * variable name (this can only happen with columns that were
             * merged by USING or NATURAL clauses).
             *
             * This wouldn't work in decompiling plan trees, because we don't
             * store joinaliasvars lists after planning; but a plan tree
             * should never contain a join alias variable.
             */
            if (rte->joinaliasvars == NIL)
                ereport(ERROR,
                    (errcode(ERRCODE_UNEXPECTED_NULL_VALUE), errmsg("cannot decompile join alias var in plan tree")));
            if (attnum > 0) {
                Var* aliasvar = NULL;

                aliasvar = (Var*)list_nth(rte->joinaliasvars, attnum - 1);
                if (IsA(aliasvar, Var)) {
                    return get_variable(
                        aliasvar, var->varlevelsup + levelsup, istoplevel, context, for_star, no_alias);
                }
            }

            /*
             * Unnamed join has neither schemaname nor refname.  (Note: since
             * it's unnamed, there is no way the user could have referenced it
             * to create a whole-row Var for it.  So we don't have to cover
             * that case below.)
             */
            refname = NULL;
        }
    }

    if (attnum == InvalidAttrNumber)
        attname = NULL;
    else
        attname = get_rte_attribute_name(rte, attnum);

    if (refname && (context->varprefix || attname == NULL)) {
        if (schemaname != NULL)
            appendStringInfo(buf, "%s.", quote_identifier(schemaname));
        appendStringInfoString(buf, quote_identifier(refname));
        appendStringInfoChar(buf, '.');
    }

    if (for_star)
        appendStringInfoChar(buf, '*');
    else if (attname != NULL)
        appendStringInfoString(buf, quote_identifier(attname));
    else {
        appendStringInfoChar(buf, '*');
        if (istoplevel)
            appendStringInfo(buf, "::%s", format_type_with_typemod(var->vartype, var->vartypmod));
    }

    return attname;
}

/*
 * Get the name of a field of an expression of composite type.	The
 * expression is usually a Var, but we handle other cases too.
 *
 * levelsup is an extra offset to interpret the Var's varlevelsup correctly.
 *
 * This is fairly straightforward when the expression has a named composite
 * type; we need only look up the type in the catalogs.  However, the type
 * could also be RECORD.  Since no actual table or view column is allowed to
 * have type RECORD, a Var of type RECORD must refer to a JOIN or FUNCTION RTE
 * or to a subquery output.  We drill down to find the ultimate defining
 * expression and attempt to infer the field name from it.	We ereport if we
 * can't determine the name.
 *
 * Similarly, a PARAM of type RECORD has to refer to some expression of
 * a determinable composite type.
 */
static const char* get_name_for_var_field(Var* var, int fieldno, int levelsup, deparse_context* context)
{
    RangeTblEntry* rte = NULL;
    AttrNumber attnum = InvalidAttrNumber;
    int netlevelsup;
    deparse_namespace* dpns = NULL;
    TupleDesc tupleDesc;
    Node* expr = NULL;

    /*
     * If it's a RowExpr that was expanded from a whole-row Var, use the
     * column names attached to it.
     */
    if (IsA(var, RowExpr)) {
        RowExpr* r = (RowExpr*)var;

        if (fieldno > 0 && fieldno <= list_length(r->colnames))
            return strVal(list_nth(r->colnames, fieldno - 1));
    }

    /*
     * If it's a Param of type RECORD, try to find what the Param refers to.
     */
    if (IsA(var, Param)) {
        Param* param = (Param*)var;
        ListCell* ancestor_cell = NULL;

        expr = find_param_referent(param, context, &dpns, &ancestor_cell);
        if (expr != NULL) {
            /* Found a match, so recurse to decipher the field name */
            deparse_namespace save_dpns;
            const char* result = NULL;

            push_ancestor_plan(dpns, ancestor_cell, &save_dpns);
            result = get_name_for_var_field((Var*)expr, fieldno, 0, context);
            pop_ancestor_plan(dpns, &save_dpns);
            return result;
        }
    }

    /*
     * If it's a Var of type RECORD, we have to find what the Var refers to;
     * if not, we can use get_expr_result_type. If that fails, we try
     * lookup_rowtype_tupdesc, which will probably fail too, but will ereport
     * an acceptable message.
     */
    if (!IsA(var, Var) || var->vartype != RECORDOID) {
        if (get_expr_result_type((Node*)var, NULL, &tupleDesc) != TYPEFUNC_COMPOSITE)
            tupleDesc = lookup_rowtype_tupdesc_copy(exprType((Node*)var), exprTypmod((Node*)var));
        Assert(tupleDesc);
        /* Got the tupdesc, so we can extract the field name */
        Assert(fieldno >= 1 && fieldno <= tupleDesc->natts);
        return NameStr(tupleDesc->attrs[fieldno - 1]->attname);
    }

    /* Find appropriate nesting depth */
    netlevelsup = var->varlevelsup + levelsup;
    if (netlevelsup >= list_length(context->namespaces))
        ereport(ERROR,
            (errcode(ERRCODE_UNEXPECTED_NULL_VALUE), errmsg("bogus varattno for remotequery var: %d", netlevelsup)));
    dpns = (deparse_namespace*)list_nth(context->namespaces, netlevelsup);

    /*
     * Try to find the relevant RTE in this rtable.  In a plan tree, it's
     * likely that varno is OUTER_VAR or INNER_VAR, in which case we must dig
     * down into the subplans, or INDEX_VAR, which is resolved similarly.
     */
    if (var->varno >= 1 && var->varno <= (uint)list_length(dpns->rtable)) {
        rte = rt_fetch(var->varno, dpns->rtable);
        attnum = var->varattno;
    } else if (var->varno == OUTER_VAR && dpns->outer_tlist) {
        TargetEntry* tle = NULL;
        deparse_namespace save_dpns;
        const char* result = NULL;

        tle = get_tle_by_resno(dpns->outer_tlist, var->varattno);
        if (tle == NULL)
            ereport(ERROR,
                (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                    errmsg("bogus varattno for OUTER_VAR var: %d", var->varattno)));

        Assert(netlevelsup == 0);
        push_child_plan(dpns, dpns->outer_planstate, &save_dpns);

        result = get_name_for_var_field((Var*)tle->expr, fieldno, levelsup, context);

        pop_child_plan(dpns, &save_dpns);
        return result;
    } else if (var->varno == INNER_VAR && dpns->inner_tlist) {
        TargetEntry* tle = NULL;
        deparse_namespace save_dpns;
        const char* result = NULL;

        tle = get_tle_by_resno(dpns->inner_tlist, var->varattno);
        if (tle == NULL)
            ereport(ERROR,
                (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                    errmsg("bogus varattno for INNER_VAR var: %d", var->varattno)));

        Assert(netlevelsup == 0);
        push_child_plan(dpns, dpns->inner_planstate, &save_dpns);

        result = get_name_for_var_field((Var*)tle->expr, fieldno, levelsup, context);

        pop_child_plan(dpns, &save_dpns);
        return result;
    } else if (var->varno == INDEX_VAR && dpns->index_tlist) {
        TargetEntry* tle = NULL;
        const char* result = NULL;

        tle = get_tle_by_resno(dpns->index_tlist, var->varattno);
        if (tle == NULL)
            ereport(ERROR,
                (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                    errmsg("bogus varattno for INDEX_VAR var: %d", var->varattno)));

        Assert(netlevelsup == 0);

        result = get_name_for_var_field((Var*)tle->expr, fieldno, levelsup, context);

        return result;
    } else {
        ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NULL_VALUE), errmsg("bogus varno: %d", var->varno)));
        return NULL; /* keep compiler quiet */
    }

    if (attnum == InvalidAttrNumber) {
        /* Var is whole-row reference to RTE, so select the right field */
        return get_rte_attribute_name(rte, fieldno);
    }

    /*
     * This part has essentially the same logic as the parser's
     * expandRecordVariable() function, but we are dealing with a different
     * representation of the input context, and we only need one field name
     * not a TupleDesc.  Also, we need special cases for finding subquery and
     * CTE subplans when deparsing Plan trees.
     */
    expr = (Node*)var; /* default if we can't drill down */

    switch (rte->rtekind) {
        case RTE_RELATION:
        case RTE_VALUES:
        case RTE_RESULT:
            /*
             * This case should not occur: a column of a table or values list
             * shouldn't have type RECORD.  Fall through and fail (most
             * likely) at the bottom.
             */
            break;
        case RTE_SUBQUERY:
            /* Subselect-in-FROM: examine sub-select's output expr */
            {
                if (rte->subquery) {
                    TargetEntry* ste = get_tle_by_resno(rte->subquery->targetList, attnum);

                    if (ste == NULL || ste->resjunk)
                        ereport(ERROR,
                            (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                                errmsg("subquery %s does not have attribute %d", rte->eref->aliasname, attnum)));
                    expr = (Node*)ste->expr;
                    if (IsA(expr, Var)) {
                        /*
                         * Recurse into the sub-select to see what its Var
                         * refers to. We have to build an additional level of
                         * namespace to keep in step with varlevelsup in the
                         * subselect.
                         */
                        deparse_namespace mydpns;
                        const char* result = NULL;

                        errno_t rc = memset_s(&mydpns, sizeof(mydpns), 0, sizeof(mydpns));
                        securec_check(rc, "\0", "\0");
                        mydpns.rtable = rte->subquery->rtable;
                        mydpns.ctes = rte->subquery->cteList;
#ifdef PGXC
                        mydpns.remotequery = false;
#endif

                        context->namespaces = lcons(&mydpns, context->namespaces);

                        result = get_name_for_var_field((Var*)expr, fieldno, 0, context);

                        context->namespaces = list_delete_first(context->namespaces);

                        return result;
                    }
                    /* else fall through to inspect the expression */
                } else {
                    /*
                     * We're deparsing a Plan tree so we don't have complete
                     * RTE entries (in particular, rte->subquery is NULL). But
                     * the only place we'd see a Var directly referencing a
                     * SUBQUERY RTE is in a SubqueryScan plan node, and we can
                     * look into the child plan's tlist instead.
                     */
                    TargetEntry* tle = NULL;
                    deparse_namespace save_dpns;
                    const char* result = NULL;

                    if (dpns->inner_planstate == NULL)
                        ereport(ERROR,
                            (errmodule(MOD_OPT),
                                (errcode(ERRCODE_UNEXPECTED_NODE_STATE),
                                    errmsg("failed to find plan for subquery %s", rte->eref->aliasname))));
                    tle = get_tle_by_resno(dpns->inner_tlist, attnum);
                    if (tle == NULL)
                        ereport(ERROR,
                            (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                                errmsg("bogus varattno for subquery var: %d", attnum)));
                    Assert(netlevelsup == 0);
                    push_child_plan(dpns, dpns->inner_planstate, &save_dpns);

                    result = get_name_for_var_field((Var*)tle->expr, fieldno, levelsup, context);

                    pop_child_plan(dpns, &save_dpns);
                    return result;
                }
            }
            break;
        case RTE_JOIN:
            /* Join RTE --- recursively inspect the alias variable */
            if (rte->joinaliasvars == NIL)
                ereport(ERROR,
                    (errmodule(MOD_OPT),
                        (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                            errmsg("cannot decompile join alias var in plan tree"))));
            Assert(attnum > 0 && attnum <= list_length(rte->joinaliasvars));
            expr = (Node*)list_nth(rte->joinaliasvars, attnum - 1);
            if (IsA(expr, Var))
                return get_name_for_var_field((Var*)expr, fieldno, var->varlevelsup + levelsup, context);
            /* else fall through to inspect the expression */
            break;
        case RTE_FUNCTION:

            /*
             * We couldn't get here unless a function is declared with one of
             * its result columns as RECORD, which is not allowed.
             */
            break;
        case RTE_CTE:
            /* CTE reference: examine subquery's output expr */
            {
                CommonTableExpr* cte = NULL;
                Index ctelevelsup;
                ListCell* lc = NULL;

                /*
                 * Try to find the referenced CTE using the namespace stack.
                 */
                ctelevelsup = rte->ctelevelsup + netlevelsup;
                if (ctelevelsup >= (uint)list_length(context->namespaces))
                    lc = NULL;
                else {
                    deparse_namespace* ctedpns = NULL;

                    ctedpns = (deparse_namespace*)list_nth(context->namespaces, ctelevelsup);
                    foreach (lc, ctedpns->ctes) {
                        cte = (CommonTableExpr*)lfirst(lc);
                        if (strcmp(cte->ctename, rte->ctename) == 0)
                            break;
                    }
                }
                if (lc != NULL) {
                    Query* ctequery = (Query*)cte->ctequery;
                    TargetEntry* ste = get_tle_by_resno(GetCTETargetList(cte), attnum);

                    if (ste == NULL || ste->resjunk)
                        ereport(ERROR,
                            (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                                errmsg("subquery %s does not have attribute %d", rte->eref->aliasname, attnum)));
                    expr = (Node*)ste->expr;
                    if (IsA(expr, Var)) {
                        /*
                         * Recurse into the CTE to see what its Var refers to.
                         * We have to build an additional level of namespace
                         * to keep in step with varlevelsup in the CTE.
                         * Furthermore it could be an outer CTE, so we may
                         * have to delete some levels of namespace.
                         */
                        List* save_nslist = context->namespaces;
                        List* new_nslist = NIL;
                        deparse_namespace mydpns;
                        const char* result = NULL;

                        errno_t rc = memset_s(&mydpns, sizeof(mydpns), 0, sizeof(mydpns));
                        securec_check(rc, "\0", "\0");
                        mydpns.rtable = ctequery->rtable;
                        mydpns.ctes = ctequery->cteList;
#ifdef PGXC
                        mydpns.remotequery = false;
#endif

                        new_nslist = list_copy_tail(context->namespaces, ctelevelsup);
                        context->namespaces = lcons(&mydpns, new_nslist);

                        result = get_name_for_var_field((Var*)expr, fieldno, 0, context);

                        context->namespaces = save_nslist;

                        return result;
                    }
                    /* else fall through to inspect the expression */
                } else {
                    /*
                     * We're deparsing a Plan tree so we don't have a CTE
                     * list.  But the only place we'd see a Var directly
                     * referencing a CTE RTE is in a CteScan plan node, and we
                     * can look into the subplan's tlist instead.
                     */
                    TargetEntry* tle = NULL;
                    deparse_namespace save_dpns;
                    const char* result = NULL;

                    if (dpns->inner_planstate == NULL)
                        ereport(ERROR,
                            (errmodule(MOD_OPT),
                                (errcode(ERRCODE_UNEXPECTED_NODE_STATE),
                                    errmsg("failed to find plan for CTE %s", rte->eref->aliasname))));
                    tle = get_tle_by_resno(dpns->inner_tlist, attnum);
                    if (tle == NULL)
                        ereport(ERROR,
                            (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                                errmsg("bogus varattno for subquery var: %d", attnum)));
                    Assert(netlevelsup == 0);
                    push_child_plan(dpns, dpns->inner_planstate, &save_dpns);

                    result = get_name_for_var_field((Var*)tle->expr, fieldno, levelsup, context);

                    pop_child_plan(dpns, &save_dpns);
                    return result;
                }
            }
            break;
#ifdef PGXC
        case RTE_REMOTE_DUMMY:
            ereport(ERROR, (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE), errmsg("Invalid RTE found")));
            break;
#endif /* PGXC */
        default:
            break;
    }

    /*
     * We now have an expression we can't expand any more, so see if
     * get_expr_result_type() can do anything with it.	If not, pass to
     * lookup_rowtype_tupdesc() which will probably fail, but will give an
     * appropriate error message while failing.
     */
    if (get_expr_result_type(expr, NULL, &tupleDesc) != TYPEFUNC_COMPOSITE)
        tupleDesc = lookup_rowtype_tupdesc_copy(exprType(expr), exprTypmod(expr));
    Assert(tupleDesc);
    /* Got the tupdesc, so we can extract the field name */
    Assert(fieldno >= 1 && fieldno <= tupleDesc->natts);
    return NameStr(tupleDesc->attrs[fieldno - 1]->attname);
}

/*
 * find_rte_by_refname		- look up an RTE by refname in a deparse context
 *
 * Returns NULL if there is no matching RTE or the refname is ambiguous.
 *
 * NOTE: this code is not really correct since it does not take account of
 * the fact that not all the RTEs in a rangetable may be visible from the
 * point where a Var reference appears.  For the purposes we need, however,
 * the only consequence of a false match is that we might stick a schema
 * qualifier on a Var that doesn't really need it.  So it seems close
 * enough.
 */
static RangeTblEntry* find_rte_by_refname(const char* refname, deparse_context* context)
{
    RangeTblEntry* result = NULL;
    ListCell* nslist = NULL;

    foreach (nslist, context->namespaces) {
        deparse_namespace* dpns = (deparse_namespace*)lfirst(nslist);
        ListCell* rtlist = NULL;

        foreach (rtlist, dpns->rtable) {
            RangeTblEntry* rte = (RangeTblEntry*)lfirst(rtlist);
            /* duplicately named pulled-up rtable in upsert clause will not lead to ambiguity */
            if (!(context->is_upsert_clause && rte->pulled_from_subquery) &&
                strcmp(rte->eref->aliasname, refname) == 0) {
                if (result != NULL)
                    return NULL; /* it's ambiguous */
                result = rte;
            }
        }
        if (result != NULL)
            break;
    }
    return result;
}

/*
 * Try to find the referenced expression for a PARAM_EXEC Param that might
 * reference a parameter supplied by an upper NestLoop or SubPlan plan node.
 *
 * If successful, return the expression and set *dpns_p and *ancestor_cell_p
 * appropriately for calling push_ancestor_plan().	If no referent can be
 * found, return NULL.
 */
static Node* find_param_referent(
    Param* param, deparse_context* context, deparse_namespace** dpns_p, ListCell** ancestor_cell_p)
{
    /* Initialize output parameters to prevent compiler warnings */
    *dpns_p = NULL;
    *ancestor_cell_p = NULL;

    /*
     * If it's a PARAM_EXEC parameter, look for a matching NestLoopParam or
     * SubPlan argument.  This will necessarily be in some ancestor of the
     * current expression's PlanState.
     */
    if (param->paramkind == PARAM_EXEC) {
        deparse_namespace* dpns = NULL;
        PlanState* child_ps = NULL;
        bool in_same_plan_level = false;
        ListCell* lc = NULL;

        dpns = (deparse_namespace*)linitial(context->namespaces);
        child_ps = dpns->planstate;
        in_same_plan_level = true;

        foreach (lc, dpns->ancestors) {
            PlanState* ps = (PlanState*)lfirst(lc);
            ListCell* lc2 = NULL;

            /*
             * NestLoops transmit params to their inner child only; also, once
             * we've crawled up out of a subplan, this couldn't possibly be
             * the right match.
             */
            if (IsA(ps, NestLoopState) && child_ps == innerPlanState(ps) && in_same_plan_level) {
                NestLoop* nl = (NestLoop*)ps->plan;

                foreach (lc2, nl->nestParams) {
                    NestLoopParam* nlp = (NestLoopParam*)lfirst(lc2);

                    if (nlp->paramno == param->paramid) {
                        /* Found a match, so return it */
                        *dpns_p = dpns;
                        *ancestor_cell_p = lc;
                        return (Node*)nlp->paramval;
                    }
                }
            }

            /*
             * Check to see if we're crawling up from a subplan.
             */
            foreach (lc2, ps->subPlan) {
                SubPlanState* sstate = (SubPlanState*)lfirst(lc2);
                SubPlan* subplan = (SubPlan*)sstate->xprstate.expr;
                ListCell* lc3 = NULL;
                ListCell* lc4 = NULL;

                if (child_ps != sstate->planstate)
                    continue;

                /* Matched subplan, so check its arguments */
                forboth(lc3, subplan->parParam, lc4, subplan->args)
                {
                    int paramid = lfirst_int(lc3);
                    Node* arg = (Node*)lfirst(lc4);

                    if (paramid == param->paramid) {
                        /* Found a match, so return it */
                        *dpns_p = dpns;
                        *ancestor_cell_p = lc;
                        return arg;
                    }
                }

                /* Keep looking, but we are emerging from a subplan. */
                in_same_plan_level = false;
                break;
            }

            /*
             * Likewise check to see if we're emerging from an initplan.
             * Initplans never have any parParams, so no need to search that
             * list, but we need to know if we should reset
             * in_same_plan_level.
             */
            foreach (lc2, ps->initPlan) {
                SubPlanState* sstate = (SubPlanState*)lfirst(lc2);

                if (child_ps != sstate->planstate)
                    continue;

                /* No parameters to be had here. */
                Assert(((SubPlan*)sstate->xprstate.expr)->parParam == NIL);

                /* Keep looking, but we are emerging from an initplan. */
                in_same_plan_level = false;
                break;
            }

            /* No luck, crawl up to next ancestor */
            child_ps = ps;
        }
    }

    /* No referent found */
    return NULL;
}

/*
 * Display a Param appropriately.
 */
static void get_parameter(Param* param, deparse_context* context)
{
    Node* expr = NULL;
    deparse_namespace* dpns = NULL;
    ListCell* ancestor_cell = NULL;

    /*
     * If it's a PARAM_EXEC parameter, try to locate the expression from which
     * the parameter was computed.	Note that failing to find a referent isn't
     * an error, since the Param might well be a subplan output rather than an
     * input.
     */
    expr = find_param_referent(param, context, &dpns, &ancestor_cell);
    if (expr != NULL) {
        /* Found a match, so print it */
        deparse_namespace save_dpns;
        bool save_varprefix = false;
        bool need_paren = false;

        /* Switch attention to the ancestor plan node */
        push_ancestor_plan(dpns, ancestor_cell, &save_dpns);

        /*
         * Force prefixing of Vars, since they won't belong to the relation
         * being scanned in the original plan node.
         */
        save_varprefix = context->varprefix;
        context->varprefix = true;

        /*
         * A Param's expansion is typically a Var, Aggref, or upper-level
         * Param, which wouldn't need extra parentheses.  Otherwise, insert
         * parens to ensure the expression looks atomic.
         */
        need_paren = !(IsA(expr, Var) || IsA(expr, Aggref) || IsA(expr, Param));
        if (need_paren)
            appendStringInfoChar(context->buf, '(');

        get_rule_expr(expr, context, false);

        if (need_paren)
            appendStringInfoChar(context->buf, ')');

        context->varprefix = save_varprefix;

        pop_ancestor_plan(dpns, &save_dpns);

        return;
    }
    /*
     * If it's a PARAM_EXTERN parameter (only occurs while deparse of create table as stmt),
     * try to locate the expression from dynamic variables from plsql context
     */
    else if (param->paramkind == PARAM_EXTERN && context->parser_arg != NULL) {
        PLpgSQL_expr* arg = (PLpgSQL_expr*)context->parser_arg;
        struct PLpgSQL_nsitem* nsitem = NULL;
        for (nsitem = arg->ns; nsitem->itemtype == PLPGSQL_NSTYPE_VAR; nsitem = nsitem->prev) {
            if (nsitem->itemno == param->paramid - 1) {
                appendStringInfo(context->buf, "%s", nsitem->name);
                return;
            }
        }
    }

    /*
     * Not PARAM_EXEC, or couldn't find referent: just print $N.
     */
    appendStringInfo(context->buf, "$%d", param->paramid);
}

/*
 * get_simple_binary_op_name
 *
 * helper function for isSimpleNode
 * will return single char binary operator name, or NULL if it's not
 */
static const char* get_simple_binary_op_name(OpExpr* expr)
{
    List* args = expr->args;

    if (list_length(args) == 2) {
        /* binary operator */
        Node* arg1 = (Node*)linitial(args);
        Node* arg2 = (Node*)lsecond(args);
        const char* op = NULL;

        op = generate_operator_name(expr->opno, exprType(arg1), exprType(arg2));
        if (strlen(op) == 1)
            return op;
    }
    return NULL;
}

/*
 * isSimpleNode - check if given node is simple (doesn't need parenthesizing)
 *
 *	true   : simple in the context of parent node's type
 *	false  : not simple
 */
static bool isSimpleNode(Node* node, Node* parentNode, int prettyFlags)
{
    if (node == NULL)
        return false;

    switch (nodeTag(node)) {
        case T_Var:
        case T_Const:
        case T_Param:
        case T_CoerceToDomainValue:
        case T_SetToDefault:
        case T_CurrentOfExpr:
            /* single words: always simple */
            return true;

        case T_ArrayRef:
        case T_ArrayExpr:
        case T_RowExpr:
        case T_CoalesceExpr:
        case T_MinMaxExpr:
        case T_XmlExpr:
        case T_NullIfExpr:
        case T_Aggref:
        case T_WindowFunc:
        case T_FuncExpr:
            /* function-like: name(..) or name[..] */
            return true;

            /* CASE keywords act as parentheses */
        case T_CaseExpr:
            return true;

        case T_FieldSelect:

            /*
             * appears simple since . has top precedence, unless parent is
             * T_FieldSelect itself!
             */
            return (IsA(parentNode, FieldSelect) ? false : true);

        case T_FieldStore:

            /*
             * treat like FieldSelect (probably doesn't matter)
             */
            return (IsA(parentNode, FieldStore) ? false : true);

        case T_CoerceToDomain:
            /* maybe simple, check args */
            return isSimpleNode((Node*)((CoerceToDomain*)node)->arg, node, prettyFlags);
        case T_RelabelType:
            return isSimpleNode((Node*)((RelabelType*)node)->arg, node, prettyFlags);
        case T_CoerceViaIO:
            return isSimpleNode((Node*)((CoerceViaIO*)node)->arg, node, prettyFlags);
        case T_ArrayCoerceExpr:
            return isSimpleNode((Node*)((ArrayCoerceExpr*)node)->arg, node, prettyFlags);
        case T_ConvertRowtypeExpr:
            return isSimpleNode((Node*)((ConvertRowtypeExpr*)node)->arg, node, prettyFlags);

        case T_OpExpr: {
            /* depends on parent node type; needs further checking */
            if ((unsigned int)prettyFlags & PRETTYFLAG_PAREN && IsA(parentNode, OpExpr)) {
                const char* op = NULL;
                const char* parentOp = NULL;
                bool is_lopriop = false;
                bool is_hipriop = false;
                bool is_lopriparent = false;
                bool is_hipriparent = false;

                op = get_simple_binary_op_name((OpExpr*)node);
                if (op == NULL)
                    return false;

                /* We know only the basic operators + - and * / % */
                is_lopriop = (strchr("+-", *op) != NULL);
                is_hipriop = (strchr("*/%", *op) != NULL);
                if (!(is_lopriop || is_hipriop))
                    return false;

                parentOp = get_simple_binary_op_name((OpExpr*)parentNode);
                if (parentOp == NULL)
                    return false;

                is_lopriparent = (strchr("+-", *parentOp) != NULL);
                is_hipriparent = (strchr("*/%", *parentOp) != NULL);
                if (!(is_lopriparent || is_hipriparent))
                    return false;

                if (is_hipriop && is_lopriparent)
                    return true; /* op binds tighter than parent */

                if (is_lopriop && is_hipriparent)
                    return false;

                /*
                 * Operators are same priority --- can skip parens only if
                 * we have (a - b) - c, not a - (b - c).
                 */
                if (node == (Node*)linitial(((OpExpr*)parentNode)->args))
                    return true;

                return false;
            }
            /* else do the same stuff as for T_SubLink et al. */
            /* FALL THROUGH */
        }

        case T_SubLink:
        case T_NullTest:
        case T_BooleanTest:
        case T_HashFilter:
        case T_DistinctExpr:
            switch (nodeTag(parentNode)) {
                case T_FuncExpr: {
                    /* special handling for casts */
                    CoercionForm type = ((FuncExpr*)parentNode)->funcformat;

                    if (type == COERCE_EXPLICIT_CAST || type == COERCE_IMPLICIT_CAST)
                        return false;
                    return true; /* own parentheses */
                }
                case T_BoolExpr:     /* lower precedence */
                case T_ArrayRef:     /* other separators */
                case T_ArrayExpr:    /* other separators */
                case T_RowExpr:      /* other separators */
                case T_CoalesceExpr: /* own parentheses */
                case T_MinMaxExpr:   /* own parentheses */
                case T_XmlExpr:      /* own parentheses */
                case T_NullIfExpr:   /* other separators */
                case T_Aggref:       /* own parentheses */
                case T_WindowFunc:   /* own parentheses */
                case T_CaseExpr:     /* other separators */
                    return true;
                default:
                    return false;
            }

        case T_BoolExpr:
            switch (nodeTag(parentNode)) {
                case T_BoolExpr:
                    if (prettyFlags & PRETTYFLAG_PAREN) {
                        BoolExprType type;
                        BoolExprType parentType;

                        type = ((BoolExpr*)node)->boolop;
                        parentType = ((BoolExpr*)parentNode)->boolop;
                        switch (type) {
                            case NOT_EXPR:
                            case AND_EXPR:
                                if (parentType == AND_EXPR || parentType == OR_EXPR)
                                    return true;
                                break;
                            case OR_EXPR:
                                if (parentType == OR_EXPR)
                                    return true;
                                break;
                            default:
                                break;
                        }
                    }
                    return false;
                case T_FuncExpr: {
                    /* special handling for casts */
                    CoercionForm type = ((FuncExpr*)parentNode)->funcformat;

                    if (type == COERCE_EXPLICIT_CAST || type == COERCE_IMPLICIT_CAST)
                        return false;
                    return true; /* own parentheses */
                }
                case T_ArrayRef:     /* other separators */
                case T_ArrayExpr:    /* other separators */
                case T_RowExpr:      /* other separators */
                case T_CoalesceExpr: /* own parentheses */
                case T_MinMaxExpr:   /* own parentheses */
                case T_XmlExpr:      /* own parentheses */
                case T_NullIfExpr:   /* other separators */
                case T_Aggref:       /* own parentheses */
                case T_WindowFunc:   /* own parentheses */
                case T_CaseExpr:     /* other separators */
                    return true;
                default:
                    return false;
            }

        default:
            break;
    }
    /* those we don't know: in dubio complexo */
    return false;
}

/*
 * appendContextKeyword - append a keyword to buffer
 *
 * If prettyPrint is enabled, perform a line break, and adjust indentation.
 * Otherwise, just append the keyword.
 */
static void appendContextKeyword(
    deparse_context* context, const char* str, int indentBefore, int indentAfter, int indentPlus)
{
    if (PRETTY_INDENT(context)) {
        context->indentLevel += indentBefore;

        appendStringInfoChar(context->buf, '\n');
        appendStringInfoSpaces(context->buf, Max(context->indentLevel, 0) + indentPlus);
        appendStringInfoString(context->buf, str);

        context->indentLevel += indentAfter;
        if (context->indentLevel < 0)
            context->indentLevel = 0;
    } else
        appendStringInfoString(context->buf, str);
}

/*
 * get_rule_expr_paren	- deparse expr using get_rule_expr,
 * embracing the string with parentheses if necessary for prettyPrint.
 *
 * Never embrace if prettyFlags=0, because it's done in the calling node.
 *
 * Any node that does *not* embrace its argument node by sql syntax (with
 * parentheses, non-operator keywords like CASE/WHEN/ON, or comma etc) should
 * use get_rule_expr_paren instead of get_rule_expr so parentheses can be
 * added.
 */
static void get_rule_expr_paren(
    Node* node, deparse_context* context, bool showimplicit, Node* parentNode, bool no_alias = false)
{
    bool need_paren = false;

    need_paren = PRETTY_PAREN(context) && !isSimpleNode(node, parentNode, context->prettyFlags);

    if (need_paren)
        appendStringInfoChar(context->buf, '(');

    get_rule_expr(node, context, showimplicit, no_alias);

    if (need_paren)
        appendStringInfoChar(context->buf, ')');
}

/* ----------
 * get_rule_expr			- Parse back an expression
 *
 * Note: showimplicit determines whether we display any implicit cast that
 * is present at the top of the expression tree.  It is a passed argument,
 * not a field of the context struct, because we change the value as we
 * recurse down into the expression.  In general we suppress implicit casts
 * when the result type is known with certainty (eg, the arguments of an
 * OR must be boolean).  We display implicit casts for arguments of functions
 * and operators, since this is needed to be certain that the same function
 * or operator will be chosen when the expression is re-parsed.
 * ----------
 */
static void get_rule_expr(Node* node, deparse_context* context, bool showimplicit, bool no_alias)
{
    StringInfo buf = context->buf;
    char* tmp = NULL;

    if (node == NULL)
        return;

    /*
     * Each level of get_rule_expr must emit an indivisible term
     * (parenthesized if necessary) to ensure result is reparsed into the same
     * expression tree.  The only exception is that when the input is a List,
     * we emit the component items comma-separated with no surrounding
     * decoration; this is convenient for most callers.
     */
    switch (nodeTag(node)) {
        case T_Var:
            tmp = get_variable((Var*)node, 0, false, context, false, no_alias);
            if (tmp != NULL) {
                pfree_ext(tmp);
            }
            break;
            
        case T_Rownum:
            appendStringInfo(buf, "ROWNUM");
            break;

        case T_Const:
            get_const_expr((Const*)node, context, 0);
            break;

        case T_Param:
            get_parameter((Param*)node, context);
            break;

        case T_Aggref:
            get_agg_expr((Aggref*)node, context);
            break;

        case T_GroupingFunc: {
            GroupingFunc* gexpr = (GroupingFunc*)node;

            appendStringInfoString(buf, "GROUPING(");
            get_rule_expr((Node*)gexpr->args, context, true, no_alias);
            appendStringInfoChar(buf, ')');
        } break;

        case T_GroupingId: {
            appendStringInfoString(buf, "GROUPINGID()");
        } break;

        case T_WindowFunc:
            get_windowfunc_expr((WindowFunc*)node, context);
            break;

        case T_ArrayRef: {
            ArrayRef* aref = (ArrayRef*)node;
            bool need_parens = false;

            /*
             * If the argument is a CaseTestExpr, we must be inside a
             * FieldStore, ie, we are assigning to an element of an array
             * within a composite column.  Since we already punted on
             * displaying the FieldStore's target information, just punt
             * here too, and display only the assignment source
             * expression.
             */
            if (IsA(aref->refexpr, CaseTestExpr)) {
                Assert(aref->refassgnexpr);
                get_rule_expr((Node*)aref->refassgnexpr, context, showimplicit, no_alias);
                break;
            }

            /*
             * Parenthesize the argument unless it's a simple Var or a
             * FieldSelect.  (In particular, if it's another ArrayRef, we
             * *must* parenthesize to avoid confusion.)
             */
            need_parens = !IsA(aref->refexpr, Var) && !IsA(aref->refexpr, FieldSelect);
            if (need_parens)
                appendStringInfoChar(buf, '(');
            get_rule_expr((Node*)aref->refexpr, context, showimplicit, no_alias);
            if (need_parens)
                appendStringInfoChar(buf, ')');

            /*
             * If there's a refassgnexpr, we want to print the node in the
             * format "array[subscripts] := refassgnexpr".	This is not
             * legal SQL, so decompilation of INSERT or UPDATE statements
             * should always use processIndirection as part of the
             * statement-level syntax.	We should only see this when
             * EXPLAIN tries to print the targetlist of a plan resulting
             * from such a statement.
             */
            if (aref->refassgnexpr) {
                Node* refassgnexpr = NULL;

                /*
                 * Use processIndirection to print this node's subscripts
                 * as well as any additional field selections or
                 * subscripting in immediate descendants.  It returns the
                 * RHS expr that is actually being "assigned".
                 */
                refassgnexpr = processIndirection(node, context, true);
                appendStringInfoString(buf, " := ");
                get_rule_expr(refassgnexpr, context, showimplicit, no_alias);
            } else {
                /* Just an ordinary array fetch, so print subscripts */
                printSubscripts(aref, context);
            }
        } break;

        case T_FuncExpr:
            get_func_expr((FuncExpr*)node, context, showimplicit);
            break;

        case T_NamedArgExpr: {
            NamedArgExpr* na = (NamedArgExpr*)node;

            appendStringInfo(buf, "%s := ", quote_identifier(na->name));
            get_rule_expr((Node*)na->arg, context, showimplicit, no_alias);
        } break;

        case T_OpExpr:
            get_oper_expr((OpExpr*)node, context, no_alias);
            break;

        case T_DistinctExpr: {
            DistinctExpr* expr = (DistinctExpr*)node;
            List* args = expr->args;
            Node* arg1 = (Node*)linitial(args);
            Node* arg2 = (Node*)lsecond(args);

            if (!PRETTY_PAREN(context))
                appendStringInfoChar(buf, '(');
            get_rule_expr_paren(arg1, context, true, node, no_alias);
            appendStringInfo(buf, " IS DISTINCT FROM ");
            get_rule_expr_paren(arg2, context, true, node, no_alias);
            if (!PRETTY_PAREN(context))
                appendStringInfoChar(buf, ')');
        } break;

        case T_NullIfExpr: {
            NullIfExpr* nullifexpr = (NullIfExpr*)node;

            appendStringInfo(buf, "NULLIF(");
            get_rule_expr((Node*)nullifexpr->args, context, true, no_alias);
            appendStringInfoChar(buf, ')');
        } break;

        case T_ScalarArrayOpExpr: {
            ScalarArrayOpExpr* expr = (ScalarArrayOpExpr*)node;
            List* args = expr->args;
            Node* arg1 = (Node*)linitial(args);
            Node* arg2 = (Node*)lsecond(args);

            if (!PRETTY_PAREN(context))
                appendStringInfoChar(buf, '(');
            get_rule_expr_paren(arg1, context, true, node, no_alias);
            appendStringInfo(buf,
                " %s %s (",
                generate_operator_name(expr->opno, exprType(arg1), get_base_element_type(exprType(arg2))),
                expr->useOr ? "ANY" : "ALL");
            get_rule_expr_paren(arg2, context, true, node, no_alias);

            /*
             * There's inherent ambiguity in "x op ANY/ALL (y)" when y is
             * a bare sub-SELECT.  Since we're here, the sub-SELECT must
             * be meant as a scalar sub-SELECT yielding an array value to
             * be used in ScalarArrayOpExpr; but the grammar will
             * preferentially interpret such a construct as an ANY/ALL
             * SubLink.  To prevent misparsing the output that way, insert
             * a dummy coercion (which will be stripped by parse analysis,
             * so no inefficiency is added in dump and reload).  This is
             * indeed most likely what the user wrote to get the construct
             * accepted in the first place.
             */
            if (IsA(arg2, SubLink) && ((SubLink*)arg2)->subLinkType == EXPR_SUBLINK)
                appendStringInfo(buf, "::%s", format_type_with_typemod(exprType(arg2), exprTypmod(arg2)));
            appendStringInfoChar(buf, ')');
            if (!PRETTY_PAREN(context))
                appendStringInfoChar(buf, ')');
        } break;

        case T_BoolExpr: {
            BoolExpr* expr = (BoolExpr*)node;
            Node* first_arg = (Node*)linitial(expr->args);
            ListCell* arg = lnext(list_head(expr->args));

            switch (expr->boolop) {
                case AND_EXPR:
                    if (!PRETTY_PAREN(context))
                        appendStringInfoChar(buf, '(');
                    get_rule_expr_paren(first_arg, context, false, node, no_alias);
                    while (arg != NULL) {
                        appendStringInfo(buf, " AND ");
                        get_rule_expr_paren((Node*)lfirst(arg), context, false, node, no_alias);
                        arg = lnext(arg);
                    }
                    if (!PRETTY_PAREN(context))
                        appendStringInfoChar(buf, ')');
                    break;

                case OR_EXPR:
                    if (!PRETTY_PAREN(context))
                        appendStringInfoChar(buf, '(');
                    get_rule_expr_paren(first_arg, context, false, node, no_alias);
                    while (arg != NULL) {
                        appendStringInfo(buf, " OR ");
                        get_rule_expr_paren((Node*)lfirst(arg), context, false, node, no_alias);
                        arg = lnext(arg);
                    }
                    if (!PRETTY_PAREN(context))
                        appendStringInfoChar(buf, ')');
                    break;

                case NOT_EXPR:
                    if (!PRETTY_PAREN(context))
                        appendStringInfoChar(buf, '(');
                    appendStringInfo(buf, "NOT ");
                    get_rule_expr_paren(first_arg, context, false, node, no_alias);
                    if (!PRETTY_PAREN(context))
                        appendStringInfoChar(buf, ')');
                    break;

                default:
                    ereport(ERROR,
                        (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                            errmsg("unrecognized boolop: %d", (int)expr->boolop)));
            }
        } break;

        case T_SubLink:
            get_sublink_expr((SubLink*)node, context);
            break;

        case T_SubPlan: {
            SubPlan* subplan = (SubPlan*)node;

            /*
             * We cannot see an already-planned subplan in rule deparsing,
             * only while EXPLAINing a query plan.	We don't try to
             * reconstruct the original SQL, just reference the subplan
             * that appears elsewhere in EXPLAIN's result.
             */
            if (subplan->useHashTable)
                appendStringInfo(buf, "(hashed %s)", subplan->plan_name);
            else
                appendStringInfo(buf, "(%s)", subplan->plan_name);
        } break;

        case T_AlternativeSubPlan: {
            AlternativeSubPlan* asplan = (AlternativeSubPlan*)node;
            ListCell* lc = NULL;

            /* As above, this can only happen during EXPLAIN */
            appendStringInfo(buf, "(alternatives: ");
            foreach (lc, asplan->subplans) {
                SubPlan* splan = (SubPlan*)lfirst(lc);

                Assert(IsA(splan, SubPlan));
                if (splan->useHashTable)
                    appendStringInfo(buf, "hashed %s", splan->plan_name);
                else
                    appendStringInfo(buf, "%s", splan->plan_name);
                if (lnext(lc))
                    appendStringInfo(buf, " or ");
            }
            appendStringInfo(buf, ")");
        } break;

        case T_FieldSelect: {
            FieldSelect* fselect = (FieldSelect*)node;
            Node* arg = (Node*)fselect->arg;
            int fno = fselect->fieldnum;
            const char* fieldname = NULL;
            bool need_parens = false;

            /*
             * Parenthesize the argument unless it's an ArrayRef or
             * another FieldSelect.  Note in particular that it would be
             * WRONG to not parenthesize a Var argument; simplicity is not
             * the issue here, having the right number of names is.
             */
            need_parens = !IsA(arg, ArrayRef) && !IsA(arg, FieldSelect);
            if (need_parens)
                appendStringInfoChar(buf, '(');
            get_rule_expr(arg, context, true, no_alias);
            if (need_parens)
                appendStringInfoChar(buf, ')');

            /*
             * Get and print the field name.
             */
            fieldname = get_name_for_var_field((Var*)arg, fno, 0, context);
            appendStringInfo(buf, ".%s", quote_identifier(fieldname));
        } break;

        case T_FieldStore: {
            FieldStore* fstore = (FieldStore*)node;
            bool need_parens = false;

            /*
             * There is no good way to represent a FieldStore as real SQL,
             * so decompilation of INSERT or UPDATE statements should
             * always use processIndirection as part of the
             * statement-level syntax.	We should only get here when
             * EXPLAIN tries to print the targetlist of a plan resulting
             * from such a statement.  The plan case is even harder than
             * ordinary rules would be, because the planner tries to
             * collapse multiple assignments to the same field or subfield
             * into one FieldStore; so we can see a list of target fields
             * not just one, and the arguments could be FieldStores
             * themselves.	We don't bother to try to print the target
             * field names; we just print the source arguments, with a
             * ROW() around them if there's more than one.  This isn't
             * terribly complete, but it's probably good enough for
             * EXPLAIN's purposes; especially since anything more would be
             * either hopelessly confusing or an even poorer
             * representation of what the plan is actually doing.
             */
            need_parens = (list_length(fstore->newvals) != 1);
            if (need_parens)
                appendStringInfoString(buf, "ROW(");
            get_rule_expr((Node*)fstore->newvals, context, showimplicit, no_alias);
            if (need_parens)
                appendStringInfoChar(buf, ')');
        } break;

        case T_RelabelType: {
            RelabelType* relabel = (RelabelType*)node;
            Node* arg = (Node*)relabel->arg;

            if (relabel->relabelformat == COERCE_IMPLICIT_CAST && !showimplicit) {
                /* don't show the implicit cast */
                get_rule_expr_paren(arg, context, false, node, no_alias);
            } else {
                get_coercion_expr(arg, context, relabel->resulttype, relabel->resulttypmod, node);
            }
        } break;

        case T_CoerceViaIO: {
            CoerceViaIO* iocoerce = (CoerceViaIO*)node;
            Node* arg = (Node*)iocoerce->arg;

            if (iocoerce->coerceformat == COERCE_IMPLICIT_CAST && !showimplicit) {
                /* don't show the implicit cast */
                get_rule_expr_paren(arg, context, false, node, no_alias);
            } else {
                get_coercion_expr(arg, context, iocoerce->resulttype, -1, node);
            }
        } break;

        case T_ArrayCoerceExpr: {
            ArrayCoerceExpr* acoerce = (ArrayCoerceExpr*)node;
            Node* arg = (Node*)acoerce->arg;

            if (acoerce->coerceformat == COERCE_IMPLICIT_CAST && !showimplicit) {
                /* don't show the implicit cast */
                get_rule_expr_paren(arg, context, false, node, no_alias);
            } else {
                get_coercion_expr(arg, context, acoerce->resulttype, acoerce->resulttypmod, node);
            }
        } break;

        case T_ConvertRowtypeExpr: {
            ConvertRowtypeExpr* convert = (ConvertRowtypeExpr*)node;
            Node* arg = (Node*)convert->arg;

            if (convert->convertformat == COERCE_IMPLICIT_CAST && !showimplicit) {
                /* don't show the implicit cast */
                get_rule_expr_paren(arg, context, false, node, no_alias);
            } else {
                get_coercion_expr(arg, context, convert->resulttype, -1, node);
            }
        } break;

        case T_CollateExpr: {
            CollateExpr* collate = (CollateExpr*)node;
            Node* arg = (Node*)collate->arg;

            if (!PRETTY_PAREN(context))
                appendStringInfoChar(buf, '(');
            get_rule_expr_paren(arg, context, showimplicit, node, no_alias);
            appendStringInfo(buf, " COLLATE %s", generate_collation_name(collate->collOid));
            if (!PRETTY_PAREN(context))
                appendStringInfoChar(buf, ')');
        } break;

        case T_CaseExpr: {
            CaseExpr* caseexpr = (CaseExpr*)node;
            ListCell* temp = NULL;

            appendContextKeyword(context, "CASE", 0, PRETTYINDENT_VAR, 0);
            if (caseexpr->arg) {
                appendStringInfoChar(buf, ' ');
                get_rule_expr((Node*)caseexpr->arg, context, true);
            }
            foreach (temp, caseexpr->args) {
                CaseWhen* when = (CaseWhen*)lfirst(temp);
                Node* w = (Node*)when->expr;

                if (caseexpr->arg) {
                    /*
                     * The parser should have produced WHEN clauses of the
                     * form "CaseTestExpr = RHS", possibly with an
                     * implicit coercion inserted above the CaseTestExpr.
                     * For accurate decompilation of rules it's essential
                     * that we show just the RHS.  However in an
                     * expression that's been through the optimizer, the
                     * WHEN clause could be almost anything (since the
                     * equality operator could have been expanded into an
                     * inline function).  If we don't recognize the form
                     * of the WHEN clause, just punt and display it as-is.
                     */
                    if (IsA(w, OpExpr)) {
                        List* args = ((OpExpr*)w)->args;

                        if (list_length(args) == 2 &&
                            IsA(strip_implicit_coercions((Node*)linitial(args)), CaseTestExpr))
                            w = (Node*)lsecond(args);
                    }
                }

                if (!PRETTY_INDENT(context))
                    appendStringInfoChar(buf, ' ');
                appendContextKeyword(context, "WHEN ", 0, 0, 0);
                get_rule_expr(w, context, false, no_alias);
                appendStringInfo(buf, " THEN ");
                get_rule_expr((Node*)when->result, context, true, no_alias);
            }
            if (!PRETTY_INDENT(context))
                appendStringInfoChar(buf, ' ');
            appendContextKeyword(context, "ELSE ", 0, 0, 0);
            get_rule_expr((Node*)caseexpr->defresult, context, true, no_alias);
            if (!PRETTY_INDENT(context))
                appendStringInfoChar(buf, ' ');
            appendContextKeyword(context, "END", -PRETTYINDENT_VAR, 0, 0);
        } break;

        case T_CaseTestExpr: {
            /*
             * Normally we should never get here, since for expressions
             * that can contain this node type we attempt to avoid
             * recursing to it.  But in an optimized expression we might
             * be unable to avoid that (see comments for CaseExpr).  If we
             * do see one, print it as CASE_TEST_EXPR.
             */
            appendStringInfo(buf, "CASE_TEST_EXPR");
        } break;

        case T_ArrayExpr: {
            ArrayExpr* arrayexpr = (ArrayExpr*)node;

            appendStringInfo(buf, "ARRAY[");
            get_rule_expr((Node*)arrayexpr->elements, context, true, no_alias);
            appendStringInfoChar(buf, ']');

            /*
             * If the array isn't empty, we assume its elements are
             * coerced to the desired type.  If it's empty, though, we
             * need an explicit coercion to the array type.
             */
            if (arrayexpr->elements == NIL)
                appendStringInfo(buf, "::%s", format_type_with_typemod(arrayexpr->array_typeid, -1));
        } break;

        case T_RowExpr: {
            RowExpr* rowexpr = (RowExpr*)node;
            TupleDesc tupdesc = NULL;
            ListCell* arg = NULL;
            int i;
            char* sep = NULL;

            /*
             * If it's a named type and not RECORD, we may have to skip
             * dropped columns and/or claim there are NULLs for added
             * columns.
             */
            if (rowexpr->row_typeid != RECORDOID) {
                tupdesc = lookup_rowtype_tupdesc(rowexpr->row_typeid, -1);
                Assert(list_length(rowexpr->args) <= tupdesc->natts);
            }

            /*
             * SQL99 allows "ROW" to be omitted when there is more than
             * one column, but for simplicity we always print it.
             */
            appendStringInfo(buf, "ROW(");
            sep = "";
            i = 0;
            foreach (arg, rowexpr->args) {
                Node* e = (Node*)lfirst(arg);

                if (tupdesc == NULL || !tupdesc->attrs[i]->attisdropped) {
                    appendStringInfoString(buf, sep);
                    get_rule_expr(e, context, true, no_alias);
                    sep = ", ";
                }
                i++;
            }
            if (tupdesc != NULL) {
                while (i < tupdesc->natts) {
                    if (!tupdesc->attrs[i]->attisdropped) {
                        appendStringInfoString(buf, sep);
                        appendStringInfo(buf, "NULL");
                        sep = ", ";
                    }
                    i++;
                }

                ReleaseTupleDesc(tupdesc);
            }
            appendStringInfo(buf, ")");
            if (rowexpr->row_format == COERCE_EXPLICIT_CAST)
                appendStringInfo(buf, "::%s", format_type_with_typemod(rowexpr->row_typeid, -1));
        } break;

        case T_RowCompareExpr: {
            RowCompareExpr* rcexpr = (RowCompareExpr*)node;
            ListCell* arg = NULL;
            char* sep = NULL;

            /*
             * SQL99 allows "ROW" to be omitted when there is more than
             * one column, but for simplicity we always print it.
             */
            appendStringInfo(buf, "(ROW(");
            sep = "";
            foreach (arg, rcexpr->largs) {
                Node* e = (Node*)lfirst(arg);

                appendStringInfoString(buf, sep);
                get_rule_expr(e, context, true, no_alias);
                sep = ", ";
            }

            /*
             * We assume that the name of the first-column operator will
             * do for all the rest too.  This is definitely open to
             * failure, eg if some but not all operators were renamed
             * since the construct was parsed, but there seems no way to
             * be perfect.
             */
            appendStringInfo(buf,
                ") %s ROW(",
                generate_operator_name(linitial_oid(rcexpr->opnos),
                    exprType((Node*)linitial(rcexpr->largs)),
                    exprType((Node*)linitial(rcexpr->rargs))));
            sep = "";
            foreach (arg, rcexpr->rargs) {
                Node* e = (Node*)lfirst(arg);

                appendStringInfoString(buf, sep);
                get_rule_expr(e, context, true, no_alias);
                sep = ", ";
            }
            appendStringInfo(buf, "))");
        } break;

        case T_CoalesceExpr: {
            CoalesceExpr* coalesceexpr = (CoalesceExpr*)node;

            appendStringInfo(buf, "COALESCE(");
            get_rule_expr((Node*)coalesceexpr->args, context, true, no_alias);
            appendStringInfoChar(buf, ')');
        } break;

        case T_MinMaxExpr: {
            MinMaxExpr* minmaxexpr = (MinMaxExpr*)node;

            switch (minmaxexpr->op) {
                case IS_GREATEST:
                    appendStringInfo(buf, "GREATEST(");
                    break;
                case IS_LEAST:
                    appendStringInfo(buf, "LEAST(");
                    break;
                default:
                    break;
            }
            get_rule_expr((Node*)minmaxexpr->args, context, true, no_alias);
            appendStringInfoChar(buf, ')');
        } break;

        case T_XmlExpr: {
            XmlExpr* xexpr = (XmlExpr*)node;
            bool needcomma = false;
            ListCell* arg = NULL;
            ListCell* narg = NULL;
            Const* con = NULL;

            switch (xexpr->op) {
                case IS_XMLCONCAT:
                    appendStringInfoString(buf, "XMLCONCAT(");
                    break;
                case IS_XMLELEMENT:
                    appendStringInfoString(buf, "XMLELEMENT(");
                    break;
                case IS_XMLFOREST:
                    appendStringInfoString(buf, "XMLFOREST(");
                    break;
                case IS_XMLPARSE:
                    appendStringInfoString(buf, "XMLPARSE(");
                    break;
                case IS_XMLPI:
                    appendStringInfoString(buf, "XMLPI(");
                    break;
                case IS_XMLROOT:
                    appendStringInfoString(buf, "XMLROOT(");
                    break;
                case IS_XMLSERIALIZE:
                    appendStringInfoString(buf, "XMLSERIALIZE(");
                    break;
                case IS_DOCUMENT:
                    break;
                default:
                    break;
            }
            if (xexpr->op == IS_XMLPARSE || xexpr->op == IS_XMLSERIALIZE) {
                if (xexpr->xmloption == XMLOPTION_DOCUMENT)
                    appendStringInfoString(buf, "DOCUMENT ");
                else
                    appendStringInfoString(buf, "CONTENT ");
            }
            if (xexpr->name) {
                appendStringInfo(buf, "NAME %s", quote_identifier(map_xml_name_to_sql_identifier(xexpr->name)));
                needcomma = true;
            }
            if (xexpr->named_args) {
                if (xexpr->op != IS_XMLFOREST) {
                    if (needcomma)
                        appendStringInfoString(buf, ", ");
                    appendStringInfoString(buf, "XMLATTRIBUTES(");
                    needcomma = false;
                }
                forboth(arg, xexpr->named_args, narg, xexpr->arg_names)
                {
                    Node* e = (Node*)lfirst(arg);
                    char* argname = strVal(lfirst(narg));

                    if (needcomma)
                        appendStringInfoString(buf, ", ");
                    get_rule_expr((Node*)e, context, true, no_alias);
                    appendStringInfo(buf, " AS %s", quote_identifier(map_xml_name_to_sql_identifier(argname)));
                    needcomma = true;
                }
                if (xexpr->op != IS_XMLFOREST)
                    appendStringInfoChar(buf, ')');
            }
            if (xexpr->args) {
                if (needcomma)
                    appendStringInfoString(buf, ", ");
                switch (xexpr->op) {
                    case IS_XMLCONCAT:
                    case IS_XMLELEMENT:
                    case IS_XMLFOREST:
                    case IS_XMLPI:
                    case IS_XMLSERIALIZE:
                        /* no extra decoration needed */
                        get_rule_expr((Node*)xexpr->args, context, true, no_alias);
                        break;
                    case IS_XMLPARSE:
                        Assert(list_length(xexpr->args) == 2);

                        get_rule_expr((Node*)linitial(xexpr->args), context, true, no_alias);

                        con = (Const*)lsecond(xexpr->args);
                        Assert(IsA(con, Const));
                        Assert(!con->constisnull);
                        if (DatumGetBool(con->constvalue))
                            appendStringInfoString(buf, " PRESERVE WHITESPACE");
                        else
                            appendStringInfoString(buf, " STRIP WHITESPACE");
                        break;
                    case IS_XMLROOT:
                        Assert(list_length(xexpr->args) == 3);

                        get_rule_expr((Node*)linitial(xexpr->args), context, true, no_alias);

                        appendStringInfoString(buf, ", VERSION ");
                        con = (Const*)lsecond(xexpr->args);
                        if (IsA(con, Const) && con->constisnull)
                            appendStringInfoString(buf, "NO VALUE");
                        else
                            get_rule_expr((Node*)con, context, false, no_alias);

                        con = (Const*)lthird(xexpr->args);
                        Assert(IsA(con, Const));
                        if (con->constisnull)
                            /* suppress STANDALONE NO VALUE */;
                        else {
                            switch (DatumGetInt32(con->constvalue)) {
                                case XML_STANDALONE_YES:
                                    appendStringInfoString(buf, ", STANDALONE YES");
                                    break;
                                case XML_STANDALONE_NO:
                                    appendStringInfoString(buf, ", STANDALONE NO");
                                    break;
                                case XML_STANDALONE_NO_VALUE:
                                    appendStringInfoString(buf, ", STANDALONE NO VALUE");
                                    break;
                                default:
                                    break;
                            }
                        }
                        break;
                    case IS_DOCUMENT:
                        get_rule_expr_paren((Node*)xexpr->args, context, false, node, no_alias);
                        break;
                    default:
                        break;
                }
            }
            if (xexpr->op == IS_XMLSERIALIZE)
                appendStringInfo(buf, " AS %s", format_type_with_typemod(xexpr->type, xexpr->typmod));
            if (xexpr->op == IS_DOCUMENT)
                appendStringInfoString(buf, " IS DOCUMENT");
            else
                appendStringInfoChar(buf, ')');
        } break;

        case T_NullTest: {
            NullTest* ntest = (NullTest*)node;

            if (!PRETTY_PAREN(context))
                appendStringInfoChar(buf, '(');
            get_rule_expr_paren((Node*)ntest->arg, context, true, node, no_alias);
            /*
             * For scalar inputs, we prefer to print as IS [NOT] NULL,
             * which is shorter and traditional.  If it's a rowtype input
             * but we're applying a scalar test, must print IS [NOT]
             * DISTINCT FROM NULL to be semantically correct.
             */
            if (ntest->argisrow || !type_is_rowtype(exprType((Node *) ntest->arg))) {
                switch (ntest->nulltesttype)
                {
                    case IS_NULL:
                        appendStringInfoString(buf, " IS NULL");
                        break;
                    case IS_NOT_NULL:
                        appendStringInfoString(buf, " IS NOT NULL");
                        break;
                    default:
                        ereport(ERROR,
                            (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                                errmsg("unrecognized nulltesttype: %d", (int)ntest->nulltesttype)));
                }
            } else {
                switch (ntest->nulltesttype)
                {
                    case IS_NULL:
                        appendStringInfoString(buf, " IS NOT DISTINCT FROM NULL");
                        break;
                    case IS_NOT_NULL:
                        appendStringInfoString(buf, " IS DISTINCT FROM NULL");
                        break;
                    default:
                        ereport(ERROR,
                            (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                                errmsg("unrecognized nulltesttype: %d", (int)ntest->nulltesttype)));
                }
            }   
            if (!PRETTY_PAREN(context))
                appendStringInfoChar(buf, ')');
        } break;

        case T_HashFilter: {
            HashFilter* htest = (HashFilter*)node;
            ListCell* distVar = NULL;
            char* sep = "";

            if (!PRETTY_PAREN(context))
                appendStringInfoChar(buf, '(');

            appendStringInfo(buf, "Hash By ");
            foreach (distVar, htest->arg) {
                appendStringInfoString(buf, sep);
                Node* distriVar = (Node*)lfirst(distVar);
                get_rule_expr_paren(distriVar, context, true, node, no_alias);
                sep = ", ";
            }

            if (!PRETTY_PAREN(context))
                appendStringInfoChar(buf, ')');
        } break;

        case T_BooleanTest: {
            BooleanTest* btest = (BooleanTest*)node;

            if (!PRETTY_PAREN(context))
                appendStringInfoChar(buf, '(');
            get_rule_expr_paren((Node*)btest->arg, context, false, node, no_alias);
            switch (btest->booltesttype) {
                case IS_TRUE:
                    appendStringInfo(buf, " IS TRUE");
                    break;
                case IS_NOT_TRUE:
                    appendStringInfo(buf, " IS NOT TRUE");
                    break;
                case IS_FALSE:
                    appendStringInfo(buf, " IS FALSE");
                    break;
                case IS_NOT_FALSE:
                    appendStringInfo(buf, " IS NOT FALSE");
                    break;
                case IS_UNKNOWN:
                    appendStringInfo(buf, " IS UNKNOWN");
                    break;
                case IS_NOT_UNKNOWN:
                    appendStringInfo(buf, " IS NOT UNKNOWN");
                    break;
                default:
                    ereport(ERROR,
                        (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                            errmsg("unrecognized booltesttype: %d", (int)btest->booltesttype)));
            }
            if (!PRETTY_PAREN(context))
                appendStringInfoChar(buf, ')');
        } break;

        case T_CoerceToDomain: {
            CoerceToDomain* ctest = (CoerceToDomain*)node;
            Node* arg = (Node*)ctest->arg;

            if (ctest->coercionformat == COERCE_IMPLICIT_CAST && !showimplicit) {
                /* don't show the implicit cast */
                get_rule_expr(arg, context, false, no_alias);
            } else {
                get_coercion_expr(arg, context, ctest->resulttype, ctest->resulttypmod, node);
            }
        } break;

        case T_CoerceToDomainValue:
            appendStringInfo(buf, "VALUE");
            break;

        case T_SetToDefault:
            appendStringInfo(buf, "DEFAULT");
            break;

        case T_CurrentOfExpr: {
            CurrentOfExpr* cexpr = (CurrentOfExpr*)node;

            if (cexpr->cursor_name)
                appendStringInfo(buf, "CURRENT OF %s", quote_identifier(cexpr->cursor_name));
            else
                appendStringInfo(buf, "CURRENT OF $%d", cexpr->cursor_param);
        } break;

        case T_List: {
            char* sep = NULL;
            ListCell* l = NULL;

            sep = "";
            foreach (l, (List*)node) {
                appendStringInfoString(buf, sep);
                get_rule_expr((Node*)lfirst(l), context, showimplicit, no_alias);
                sep = ", ";
            }
        } break;

        default:
            if (context->qrw_phase)
                appendStringInfo(buf, "<unknown %d>", (int)nodeTag(node));
            else
                ereport(ERROR,
                    (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                        errmsg("unrecognized node type: %d", (int)nodeTag(node))));
            break;
    }
}

/*
 * @Description:
 * get_rule_expr_funccall	  - Parse back a function-call expression
 *
 * Same as get_rule_expr(), except that we guarantee that the output will
 * look like a function call, or like one of the things the grammar treats as
 * equivalent to a function call (see the func_expr_windowless production).
 * This is needed in places where the grammar uses func_expr_windowless and
 * you can't substitute a parenthesized a_expr.	If what we have isn't going
 * to look like a function call, wrap it in a dummy CAST() expression, which
 * will satisfy the grammar --- and, indeed, is likely what the user wrote to
 * produce such a thing.
 * @in node -	expr node
 * @in context - deparse context
 * @in showimplicit - whether display implicit casts for arguments of functions and operators
 * @out - void
 */
static void get_rule_expr_funccall(Node* node, deparse_context* context, bool showimplicit)
{
    if (looks_like_function(node)) {
        get_rule_expr(node, context, showimplicit);
    } else {
        StringInfo buf = context->buf;

        appendStringInfoString(buf, "CAST(");
        /* no point in showing any top-level implicit cast */
        get_rule_expr(node, context, false);
        appendStringInfo(buf, " AS %s)", format_type_with_typemod(exprType(node), exprTypmod(node)));
    }
}

/*
 * @Description:  Helper function to identify node types that satisfy func_expr_windowless.
 * If in doubt, "false" is always a safe answer.
 * @in node -	expr node
 * @out - bool
 */
static bool looks_like_function(Node* node)
{
    if (node == NULL)
        return false; /* probably shouldn't happen */
    switch (nodeTag(node)) {
        case T_FuncExpr:
            /* OK, unless it's going to deparse as a cast */
            return (((FuncExpr*)node)->funcformat == COERCE_EXPLICIT_CALL);
        case T_NullIfExpr:
        case T_CoalesceExpr:
        case T_MinMaxExpr:
        case T_XmlExpr:
            /* these are all accepted by func_expr_common_subexpr */
            return true;
        default:
            break;
    }
    return false;
}

/*
 * get_oper_expr			- Parse back an OpExpr node
 */
static void get_oper_expr(OpExpr* expr, deparse_context* context, bool no_alias)
{
    StringInfo buf = context->buf;
    Oid opno = expr->opno;
    List* args = expr->args;

    if (!PRETTY_PAREN(context))
        appendStringInfoChar(buf, '(');
    if (list_length(args) == 2) {
        /* binary operator */
        Node* arg1 = (Node*)linitial(args);
        Node* arg2 = (Node*)lsecond(args);

        get_rule_expr_paren(arg1, context, true, (Node*)expr, no_alias);
        appendStringInfo(buf, " %s ", generate_operator_name(opno, exprType(arg1), exprType(arg2)));
        get_rule_expr_paren(arg2, context, true, (Node*)expr, no_alias);
    } else {
        /* unary operator --- but which side? */
        Node* arg = (Node*)linitial(args);
        HeapTuple tp;
        Form_pg_operator optup;

        tp = SearchSysCache1(OPEROID, ObjectIdGetDatum(opno));
        if (!HeapTupleIsValid(tp))
            ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for operator %u", opno)));
        optup = (Form_pg_operator)GETSTRUCT(tp);
        switch (optup->oprkind) {
            case 'l':
                appendStringInfo(buf, "%s ", generate_operator_name(opno, InvalidOid, exprType(arg)));
                get_rule_expr_paren(arg, context, true, (Node*)expr, no_alias);
                break;
            case 'r':
                get_rule_expr_paren(arg, context, true, (Node*)expr, no_alias);
                appendStringInfo(buf, " %s", generate_operator_name(opno, exprType(arg), InvalidOid));
                break;
            default:
                ereport(ERROR,
                    (errmodule(MOD_OPT),
                        (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("bogus oprkind: %d", optup->oprkind))));
        }
        ReleaseSysCache(tp);
    }
    if (!PRETTY_PAREN(context))
        appendStringInfoChar(buf, ')');
}

/*
 * get_func_expr			- Parse back a FuncExpr node
 */
static void get_func_expr(FuncExpr* expr, deparse_context* context, bool showimplicit)
{
    StringInfo buf = context->buf;
    Oid funcoid = expr->funcid;
    Oid argtypes[FUNC_MAX_ARGS];
    int nargs;
    List* argnames = NIL;
    bool use_variadic = false;
    ListCell* l = NULL;

    /*
     * If the function call came from an implicit coercion, then just show the
     * first argument --- unless caller wants to see implicit coercions.
     */
    if (expr->funcformat == COERCE_IMPLICIT_CAST && !showimplicit) {
        get_rule_expr_paren((Node*)linitial(expr->args), context, false, (Node*)expr);
        return;
    }

    /*
     * If the function call came from a cast, then show the first argument
     * plus an explicit cast operation.
     */
    if (expr->funcformat == COERCE_EXPLICIT_CAST || expr->funcformat == COERCE_IMPLICIT_CAST) {
        Node* arg = (Node*)linitial(expr->args);
        Oid rettype = expr->funcresulttype;
        int32 coercedTypmod = -1;

        /* Get the typmod if this is a length-coercion function */
        if (false == context->is_fqs || expr->funcformat != COERCE_IMPLICIT_CAST)
            (void)exprIsLengthCoercion((Node*)expr, &coercedTypmod);

        get_coercion_expr(arg, context, rettype, coercedTypmod, (Node*)expr);

        list_free_ext(argnames);
        return;
    }

    /*
     * Normal function: display as proname(args).  First we need to extract
     * the argument datatypes.
     */
    if (list_length(expr->args) > FUNC_MAX_ARGS)
        ereport(ERROR, (errcode(ERRCODE_TOO_MANY_ARGUMENTS), errmsg("too many arguments")));
    nargs = 0;
    argnames = NIL;
    foreach (l, expr->args) {
        Node* arg = (Node*)lfirst(l);

        if (IsA(arg, NamedArgExpr))
            argnames = lappend(argnames, ((NamedArgExpr*)arg)->name);
        argtypes[nargs] = exprType(arg);
        nargs++;
    }

    appendStringInfo(
        buf, "%s(", generate_function_name(funcoid, nargs, argnames, argtypes, expr->funcvariadic, &use_variadic));
    nargs = 0;
    foreach (l, expr->args) {
        if (nargs++ > 0)
            appendStringInfoString(buf, ", ");
        if (use_variadic && lnext(l) == NULL)
            appendStringInfoString(buf, "VARIADIC ");
        get_rule_expr((Node*)lfirst(l), context, true);
    }
    appendStringInfoChar(buf, ')');
}

/*
 * get_agg_expr			- Parse back an Aggref node
 */
static void get_agg_expr(Aggref* aggref, deparse_context* context)
{
    StringInfo buf = context->buf;
    Oid argtypes[FUNC_MAX_ARGS];
    int nargs;
    bool use_variadic = false;
#ifdef PGXC
    bool added_finalfn = false;
#endif /* PGXC */

    /* Extract the regular arguments, ignoring resjunk stuff for the moment */
    /* Extract the argument types as seen by the parser */
    nargs = get_aggregate_argtypes(aggref, argtypes, FUNC_MAX_ARGS);

#ifdef PGXC
    /*
     * Datanode should send finalised aggregate results. Datanodes evaluate only
     * transition results. In order to get the finalised aggregate, we enclose
     * the aggregate call inside final function call, so as to get finalised
     * results at the Coordinator
     */
    if (context->finalise_aggs) {
        HeapTuple aggTuple;
        Form_pg_aggregate aggform;
        aggTuple = SearchSysCache(AGGFNOID, ObjectIdGetDatum(aggref->aggfnoid), 0, 0, 0);
        if (!HeapTupleIsValid(aggTuple))
            ereport(ERROR,
                (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                    errmsg("cache lookup failed for aggregate %u", aggref->aggfnoid)));
        aggform = (Form_pg_aggregate)GETSTRUCT(aggTuple);

        if (OidIsValid(aggform->aggfinalfn)) {
            appendStringInfo(buf, "%s(", generate_function_name(aggform->aggfinalfn, 0, NULL, NULL, NULL, NULL));
            added_finalfn = true;
        }
        ReleaseSysCache(aggTuple);
    }
#endif /* PGXC */
    char* funcname = generate_function_name(aggref->aggfnoid, nargs, NIL, argtypes, aggref->aggvariadic, &use_variadic);
    appendStringInfo(buf, "%s(%s", funcname, (aggref->aggdistinct != NIL) ? "DISTINCT " : "");

    if (AGGKIND_IS_ORDERED_SET(aggref->aggkind)) {
        /*
         * Ordered-set aggregates do not use "*" syntax and we needn't
         * worry about inserting VARIADIC. Also it's "Order by"
         * inputs are their aggregate arguments. So we can directly get
         * args as-is.
         */
        Assert(!aggref->aggvariadic);
        get_rule_expr((Node*)aggref->aggdirectargs, context, true);
        Assert(aggref->aggorder != NIL);
        appendStringInfoString(buf, ") WITHIN GROUP (ORDER BY ");
        get_rule_orderby(aggref->aggorder, aggref->args, false, context);
    } else {
        /* aggstar can be set only in zero-argument aggregates */
        if (aggref->aggstar)
            appendStringInfoChar(buf, '*');
        else {
            ListCell* l = NULL;
            int narg = 0;

            foreach (l, aggref->args) {
                TargetEntry* tle = (TargetEntry*)lfirst(l);

                Assert(!IsA((Node*)tle->expr, NamedArgExpr));
                if (tle->resjunk)
                    continue;
                if (narg++ > 0)
                    appendStringInfoString(buf, ", ");
                if (use_variadic && narg == nargs)
                    appendStringInfoString(buf, "VARIADIC ");
                get_rule_expr((Node*)tle->expr, context, true);
            }
        }

        if (aggref->aggorder != NIL) {
            Assert(funcname);   
            if (pg_strcasecmp(funcname, "listagg") == 0) {
                appendStringInfoString(buf, " ) WITHIN GROUP ( ORDER BY ");
                get_rule_orderby(aggref->aggorder, aggref->args, false, context);
            } else {
                appendStringInfoString(buf, " ORDER BY ");
                get_rule_orderby(aggref->aggorder, aggref->args, false, context);
            }
        }
    }

#ifdef PGXC
    if (added_finalfn)
        appendStringInfoChar(buf, ')');
#endif /* PGXC */
    appendStringInfoChar(buf, ')');
}

static bool construct_partitionClause(WindowAgg* node, WindowClause* wc)
{
    List* partitionClause = NIL;
    for (int i = 0; i < node->partNumCols; i++) {
        TargetEntry* tle = NULL;
        SortGroupClause* partcl = makeNode(SortGroupClause);
        tle = get_tle_by_resno(node->plan.lefttree->targetlist, node->partColIdx[i]);

        if (tle == NULL) {
            return false;
        }

        /*
         * ressortgroupref refers to windowagg's tlist
         * partColIdx      refers to subplan's tlist
         */
        ListCell *lc = NULL;
        foreach(lc, node->plan.targetlist) {
            TargetEntry *window_agg_te = (TargetEntry *)lfirst(lc);
            if (IsA(tle->expr, Var) && IsA(window_agg_te->expr, Var) &&
                _equalSimpleVar(tle->expr, window_agg_te->expr)) {
                if (window_agg_te->ressortgroupref > 0) {
                    partcl->tleSortGroupRef = window_agg_te->ressortgroupref;
                    /* found it */
                    break;
                }
            }
        }

        if (lc == NULL) {
            /* not found */
            list_free_ext(partitionClause);
            return false;
        }

        partcl->eqop = node->partOperators[i];
        partitionClause = lappend(partitionClause, partcl);
    }
    wc->partitionClause = partitionClause;
    return true;
}

/*
 * construct_windowClause	- Construct a WindowClause node for parser the param of OVER
 */
static void construct_windowClause(deparse_context* context)
{
    deparse_namespace* dpns = (deparse_namespace*)linitial(context->namespaces);
    PlanState* ps = dpns->planstate;
    WindowAgg* node = NULL;
    WindowClause* wc = makeNode(WindowClause);

    List* orderClause = NIL;

    bool isParaInValid = (NULL == ps) || (NULL == ps->plan) || ((!IsA(ps->plan, WindowAgg)) &&
                            (!IsA(ps->plan, VecWindowAgg)));
    if (isParaInValid) {
        return;
    }

    node = (WindowAgg*)ps->plan;

    /* Construct partitionClause if has partition column. */
    if (node->partNumCols > 0 && !construct_partitionClause(node, wc)) {
        return;
    }

    /* Construct orderClause if has order column. */
    if (node->ordNumCols > 0) {
        for (int i = 0; i < node->ordNumCols; i++) {
            TargetEntry* tle = NULL;
            SortGroupClause* sortcl = makeNode(SortGroupClause);
            tle = get_tle_by_resno(node->plan.lefttree->targetlist, node->ordColIdx[i]);

            if (tle == NULL) {
                return;
            }

            /*
             * ressortgroupref refers to windowagg's tlist
             * partColIdx      refers to subplan's tlist
             */
            ListCell *lc = NULL;
            foreach(lc, node->plan.targetlist) {
                TargetEntry *window_agg_te = (TargetEntry *)lfirst(lc);
                if (IsA(tle->expr, Var) && IsA(window_agg_te->expr, Var) &&
                    _equalSimpleVar(tle->expr, window_agg_te->expr)) {
                    if (window_agg_te->ressortgroupref > 0) {
                       sortcl->tleSortGroupRef = window_agg_te->ressortgroupref;
                       /* found it */
                       break;
                    }
                }
            }

            if (lc == NULL) {
                list_free_ext(orderClause);
                /* not found */
                return;
            }

            sortcl->sortop = node->ordOperators[i];
            orderClause = lappend(orderClause, sortcl);
        }
        wc->orderClause = orderClause;
    }

    wc->frameOptions = node->frameOptions;
    wc->startOffset = node->startOffset;
    wc->endOffset = node->endOffset;
    wc->winref = node->winref;
    wc->copiedOrder = false;

    context->windowClause = list_make1(wc);
    context->windowTList = node->plan.targetlist;
}

/*
 * get_windowfunc_expr	- Parse back a WindowFunc node
 */
static void get_windowfunc_expr(WindowFunc* wfunc, deparse_context* context)
{
    StringInfo buf = context->buf;
    Oid argtypes[FUNC_MAX_ARGS];
    int nargs;
    List* argnames = NIL;
    ListCell* l = NULL;
    char* funcname = NULL;

    if (list_length(wfunc->args) > FUNC_MAX_ARGS)
        ereport(ERROR, (errcode(ERRCODE_TOO_MANY_ARGUMENTS), errmsg("too many arguments")));
    nargs = 0;
    foreach (l, wfunc->args) {
        Node* arg = (Node*)lfirst(l);

        if (IsA(arg, NamedArgExpr))
            argnames = lappend(argnames, ((NamedArgExpr*)arg)->name);
        argtypes[nargs] = exprType(arg);
        nargs++;
    }

    funcname = generate_function_name(wfunc->winfnoid, nargs, argnames, argtypes, false, NULL);
    appendStringInfo(buf, "%s(", funcname);

    /* winstar can be set only in zero-argument aggregates */
    if (wfunc->winstar)
        appendStringInfoChar(buf, '*');
    else
        get_rule_expr((Node*)wfunc->args, context, true);

    Assert(funcname);
    if (pg_strcasecmp(funcname, "listagg") == 0)
        appendStringInfoString(buf, ") WITHIN GROUP ");
    else
        appendStringInfoString(buf, ") OVER ");

    construct_windowClause(context);

    foreach (l, context->windowClause) {
        WindowClause* wc = (WindowClause*)lfirst(l);

        if (wc->winref == wfunc->winref) {
            if (wc->name)
                appendStringInfoString(buf, quote_identifier(wc->name));
            else {
                if (pg_strcasecmp(funcname, "listagg") == 0)
                    get_rule_windowspec_listagg(wc, context->windowTList, context);
                else
                    get_rule_windowspec(wc, context->windowTList, context);
            }
            break;
        }
    }
    if (l == NULL) {
        if (context->windowClause != NULL)
            ereport(ERROR, (errmodule(MOD_OPT), (errcode(ERRCODE_WINDOWING_ERROR),
                        errmsg("could not find window clause for winref %u", wfunc->winref))));

        /*
         * In EXPLAIN, we don't have window context information available, so
         * we have to settle for this:
         */
        appendStringInfoString(buf, "(?)");
    }
}

/* ----------
 * get_coercion_expr
 *
 *	Make a string representation of a value coerced to a specific type
 * ----------
 */
static void get_coercion_expr(Node* arg, deparse_context* context, Oid resulttype, int32 resulttypmod, Node* parentNode)
{
    StringInfo buf = context->buf;

    /*
     * Since parse_coerce.c doesn't immediately collapse application of
     * length-coercion functions to constants, what we'll typically see in
     * such cases is a Const with typmod -1 and a length-coercion function
     * right above it.	Avoid generating redundant output. However, beware of
     * suppressing casts when the user actually wrote something like
     * 'foo'::text::char(3).
     */
    if (arg && IsA(arg, Const) && ((Const*)arg)->consttype == resulttype && ((Const*)arg)->consttypmod == -1) {
        /* Show the constant without normal ::typename decoration */
        get_const_expr((Const*)arg, context, -1);
    } else {
        if (!PRETTY_PAREN(context))
            appendStringInfoChar(buf, '(');
        get_rule_expr_paren(arg, context, false, parentNode);
        if (!PRETTY_PAREN(context))
            appendStringInfoChar(buf, ')');
    }
    /*
     * Never emit resulttype(arg) functional notation. A pg_proc entry could
     * take precedence, and a resulttype in pg_temp would require schema
     * qualification that format_type_with_typemod() would usually omit. We've
     * standardized on arg::resulttype, but CAST(arg AS resulttype) notation
     * would work fine.
     */
    appendStringInfo(buf, "::%s", format_type_with_typemod(resulttype, resulttypmod));
}

/* ----------
 * get_const_expr
 *
 *	Make a string representation of a Const
 *
 * showtype can be -1 to never show "::typename" decoration, or +1 to always
 * show it, or 0 to show it only if the constant wouldn't be assumed to be
 * the right type by default.
 *
 * If the Const's collation isn't default for its type, show that too.
 * This can only happen in trees that have been through constant-folding.
 * We assume we don't need to do this when showtype is -1.
 * ----------
 */
static void get_const_expr(Const* constval, deparse_context* context, int showtype)
{
    StringInfo buf = context->buf;
    Oid typoutput;
    bool typIsVarlena = false;
    char* extval = NULL;
    bool isfloat = false;
    bool needlabel = false;

    if (constval->constisnull || constval->ismaxvalue) {
        /*
         * Always label the type of a NULL/MAXVALUE constant to
         * prevent misdecisions about type when reparsing.
         */
        if (constval->ismaxvalue) {
            appendStringInfo(buf, "MAXVALUE");
        } else {
            appendStringInfo(buf, "NULL");
        }

        if (showtype >= 0) {
            appendStringInfo(buf, "::%s", format_type_with_typemod(constval->consttype, constval->consttypmod));
            get_const_collation(constval, context);
        }
        return;
    }

    getTypeOutputInfo(constval->consttype, &typoutput, &typIsVarlena);
    if (u_sess->exec_cxt.under_auto_explain)
        extval = pstrdup("***");
    else
        extval = OidOutputFunctionCall(typoutput, constval->constvalue);

    float8 num8 = 0;
    float4 num4 = 0;
    unsigned int ndig = 0;
    bool iseq = true;
    char* priStr = (char*)palloc(MAXDOUBLEWIDTH + 1);
    errno_t rc = EOK;

    switch (constval->consttype) {
        case FLOAT8OID:
            num8 = DatumGetFloat8(constval->constvalue);
            ndig = DBL_DIG + u_sess->attr.attr_common.extra_float_digits;
            rc = snprintf_s(priStr, MAXDOUBLEWIDTH + 1, MAXDOUBLEWIDTH, "%.*g", ndig + 2, num8);
            securec_check_ss(rc, "", "");

            if ((strspn(extval, "0123456789.") == ndig + 1 || strspn(extval, "0123456789") == ndig) &&
                strncmp(priStr, extval, strlen(priStr) + 1))
                iseq = false;

            break;
        case FLOAT4OID:
            num4 = DatumGetFloat4(constval->constvalue);
            ndig = FLT_DIG + u_sess->attr.attr_common.extra_float_digits;
            rc = snprintf_s(priStr, MAXDOUBLEWIDTH + 1, MAXDOUBLEWIDTH, "%.*g", ndig + 2, num4);
            securec_check_ss(rc, "", "");
            if ((strspn(extval, "0123456789.") == ndig + 1 || strspn(extval, "0123456789") == ndig) &&
                strncmp(priStr, extval, strlen(priStr) + 1))
                iseq = false;
            break;
        default:
            break;
    }

    switch (constval->consttype) {
        case FLOAT4OID:
        case FLOAT8OID: {
            if (strspn(extval, "0123456789+-eE.") == strlen(extval)) {
                if (!iseq) {
                    if (extval[0] == '+' || extval[0] == '-')
                        appendStringInfo(buf, "(%s)", priStr);
                    else
                        appendStringInfoString(buf, priStr);
                } else {
                    if (extval[0] == '+' || extval[0] == '-')
                        appendStringInfo(buf, "(%s)", extval);
                    else
                        appendStringInfoString(buf, extval);
                }
                if (strcspn(extval, "eE.") != strlen(extval))
                    isfloat = true; /* it looks like a float */
            } else {
                if (!iseq)
                    appendStringInfo(buf, "'%s'", priStr);
                else
                    appendStringInfo(buf, "'%s'", extval);
            }
            pfree_ext(priStr);
            break;
        }
        case INT2OID:
        case INT4OID:
        case INT8OID:
        case OIDOID:
        case NUMERICOID: {
            /*
             * These types are printed without quotes unless they contain
             * values that aren't accepted by the scanner unquoted (e.g.,
             * 'NaN').	Note that strtod() and friends might accept NaN,
             * so we can't use that to test.
             *
             * In reality we only need to defend against infinity and NaN,
             * so we need not get too crazy about pattern matching here.
             *
             * There is a special-case gotcha: if the constant is signed,
             * we need to parenthesize it, else the parser might see a
             * leading plus/minus as binding less tightly than adjacent
             * operators --- particularly, the cast that we might attach
             * below.
             */
            if (strspn(extval, "0123456789+-eE.") == strlen(extval)) {
                if (extval[0] == '+' || extval[0] == '-')
                    appendStringInfo(buf, "(%s)", extval);
                else
                    appendStringInfoString(buf, extval);
                if (strcspn(extval, "eE.") != strlen(extval))
                    isfloat = true; /* it looks like a float */
            } else
                appendStringInfo(buf, "'%s'", extval);
        } break;

        case BITOID:
        case VARBITOID:
            appendStringInfo(buf, "B'%s'", extval);
            break;

        case BOOLOID:
            if (strcmp(extval, "t") == 0)
                appendStringInfo(buf, "true");
            else
                appendStringInfo(buf, "false");
            break;

        default:
            simple_quote_literal(buf, extval);
            break;
    }

    pfree_ext(extval);

    if (showtype < 0)
        return;

    /*
     * For showtype == 0, append ::typename unless the constant will be
     * implicitly typed as the right type when it is read in.
     *
     * XXX this code has to be kept in sync with the behavior of the parser,
     * especially make_const.
     */
    switch (constval->consttype) {
        case BOOLOID:
        case INT4OID:
        case UNKNOWNOID:
            /* These types can be left unlabeled */
            needlabel = false;
            break;
        case NUMERICOID:

            /*
             * Float-looking constants will be typed as numeric, but if
             * there's a specific typmod we need to show it.
             */
            needlabel = !isfloat || (constval->consttypmod >= 0);
            break;
        default:
            needlabel = true;
            break;
    }
    if (needlabel || showtype > 0)
        appendStringInfo(buf, "::%s", format_type_with_typemod(constval->consttype, constval->consttypmod));

    get_const_collation(constval, context);
}

/*
 * helper for get_const_expr: append COLLATE if needed
 */
static void get_const_collation(Const* constval, deparse_context* context)
{
    StringInfo buf = context->buf;

    if (OidIsValid(constval->constcollid)) {
        Oid typcollation = get_typcollation(constval->consttype);
        if (constval->constcollid != typcollation) {
            appendStringInfo(buf, " COLLATE %s", generate_collation_name(constval->constcollid));
        }
    }
}

/*
 * simple_quote_literal - Format a string as a SQL literal, append to buf
 */
static void simple_quote_literal(StringInfo buf, const char* val)
{
    const char* valptr = NULL;

    /*
     * We form the string literal according to the prevailing setting of
     * standard_conforming_strings; we never use E''. User is responsible for
     * making sure result is used correctly.
     */
    appendStringInfoChar(buf, '\'');
    for (valptr = val; *valptr; valptr++) {
        char ch = *valptr;

        if (SQL_STR_DOUBLE(ch, !u_sess->attr.attr_sql.standard_conforming_strings))
            appendStringInfoChar(buf, ch);
        appendStringInfoChar(buf, ch);
    }
    appendStringInfoChar(buf, '\'');
}

static void set_string_info_left(SubLink* sublink, StringInfo buf)
{
    if (sublink->subLinkType == ARRAY_SUBLINK)
        appendStringInfo(buf, "ARRAY(");
    else
        appendStringInfoChar(buf, '(');
}

static void set_string_info_right(bool need_paren, StringInfo buf)
{
    if (need_paren)
        appendStringInfo(buf, "))");
    else
        appendStringInfoChar(buf, ')');
}

/*
 * @Description: Parse back a sublink
 * @in sublink: Subselect appearing in an expression.
 * @in context: Context info needed for invoking a recursive querytree display routine.
 */
static void get_sublink_expr(SubLink* sublink, deparse_context* context)
{
    StringInfo buf = context->buf;
    Query* query = (Query*)(sublink->subselect);
    char* opname = NULL;
    bool need_paren = false;

    set_string_info_left(sublink, buf);

    /*
     * Note that we print the name of only the first operator, when there are
     * multiple combining operators.  This is an approximation that could go
     * wrong in various scenarios (operators in different schemas, renamed
     * operators, etc) but there is not a whole lot we can do about it, since
     * the syntax allows only one operator to be shown.
     */
    if (sublink->testexpr) {
        if (IsA(sublink->testexpr, OpExpr)) {
            /* single combining operator */
            OpExpr* opexpr = (OpExpr*)sublink->testexpr;

            /*
             * To sublink's testexpr, we need not add implicit cast, because
             * we leave type of subquery's targetlist unchanged.
             * If we only change one side, the type cast rule can be changed during reparse,
             * like "create table as" case.
             */
            get_rule_expr((Node*)linitial(opexpr->args), context, false);
            opname = generate_operator_name(
                opexpr->opno, exprType((Node*)linitial(opexpr->args)), exprType((Node*)lsecond(opexpr->args)));
        } else if (IsA(sublink->testexpr, BoolExpr)) {
            /* multiple combining operators, = or <> cases */
            char* sep = NULL;
            ListCell* l = NULL;

            appendStringInfoChar(buf, '(');
            sep = "";
            foreach (l, ((BoolExpr*)sublink->testexpr)->args) {
                OpExpr* opexpr = (OpExpr*)lfirst(l);

                Assert(IsA(opexpr, OpExpr));
                appendStringInfoString(buf, sep);
                get_rule_expr((Node*)linitial(opexpr->args), context, false);
                if (opname == NULL)
                    opname = generate_operator_name(
                        opexpr->opno, exprType((Node*)linitial(opexpr->args)), exprType((Node*)lsecond(opexpr->args)));
                sep = ", ";
            }
            appendStringInfoChar(buf, ')');
        } else if (IsA(sublink->testexpr, RowCompareExpr)) {
            /* multiple combining operators, < <= > >= cases */
            RowCompareExpr* rcexpr = (RowCompareExpr*)sublink->testexpr;

            appendStringInfoChar(buf, '(');
            get_rule_expr((Node*)rcexpr->largs, context, false);
            opname = generate_operator_name(linitial_oid(rcexpr->opnos),
                exprType((Node*)linitial(rcexpr->largs)),
                exprType((Node*)linitial(rcexpr->rargs)));
            appendStringInfoChar(buf, ')');
        } else
            ereport(ERROR,
                (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmsg("unrecognized testexpr type: %d", (int)nodeTag(sublink->testexpr))));
    }

    need_paren = true;

    switch (sublink->subLinkType) {
        case EXISTS_SUBLINK:
            appendStringInfo(buf, "EXISTS ");
            break;

        case ANY_SUBLINK:
            Assert(opname != NULL);
            if (strcmp(opname, "=") == 0) /* Represent = ANY as IN */
                appendStringInfo(buf, " IN ");
            else
                appendStringInfo(buf, " %s ANY ", opname);
            break;

        case ALL_SUBLINK:
            appendStringInfo(buf, " %s ALL ", opname);
            break;

        case ROWCOMPARE_SUBLINK:
            appendStringInfo(buf, " %s ", opname);
            break;

        case EXPR_SUBLINK:
        case ARRAY_SUBLINK:
            need_paren = false;
            break;

        case CTE_SUBLINK: /* shouldn't occur in a SubLink */
        default:
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                        errmsg("unrecognized sublink type: %d", (int)sublink->subLinkType))));
            break;
    }

    if (need_paren)
        appendStringInfoChar(buf, '(');

    get_query_def(query,
        buf,
        context->namespaces,
        NULL,
        context->prettyFlags,
        context->wrapColumn,
        context->indentLevel
#ifdef PGXC
        ,
        context->finalise_aggs,
        context->sortgroup_colno,
        context->parser_arg
#endif /* PGXC */
        ,
        context->qrw_phase,
        context->viewdef,
        context->is_fqs);

    set_string_info_right(need_paren, buf);
}

/* ----------
 * get_from_clause			- Parse back a FROM clause
 *
 * "prefix" is the keyword that denotes the start of the list of FROM
 * elements. It is FROM when used to parse back SELECT and UPDATE, but
 * is USING when parsing back DELETE.
 * ----------
 */
static void get_from_clause(Query* query, const char* prefix, deparse_context* context, List* fromlist)
{
    StringInfo buf = context->buf;
    bool first = true;
    ListCell* l = NULL;
    List* srclist = (fromlist != NIL) ? fromlist : query->jointree->fromlist;

    /*
     * We use the query's jointree as a guide to what to print.  However, we
     * must ignore auto-added RTEs that are marked not inFromCl. (These can
     * only appear at the top level of the jointree, so it's sufficient to
     * check here.)  This check also ensures we ignore the rule pseudo-RTEs
     * for NEW and OLD.
     */
    foreach (l, srclist) {
        Node* jtnode = (Node*)lfirst(l);

        if (IsA(jtnode, RangeTblRef)) {
            int varno = ((RangeTblRef*)jtnode)->rtindex;
            RangeTblEntry* rte = rt_fetch(varno, query->rtable);

            if (!rte->inFromCl && (fromlist == NIL || varno != query->resultRelation))
                continue;
        }

        if (first) {
            appendContextKeyword(context, prefix, -PRETTYINDENT_STD, PRETTYINDENT_STD, 2);
            first = false;

            get_from_clause_item(jtnode, query, context);
        } else {
            StringInfoData itembuf;

            appendStringInfoString(buf, ", ");

            /*
             * Put the new FROM item's text into itembuf so we can decide
             * after we've got it whether or not it needs to go on a new line.
             */
            initStringInfo(&itembuf);
            context->buf = &itembuf;

            get_from_clause_item(jtnode, query, context);

            /* Restore context's output buffer */
            context->buf = buf;

            /* Consider line-wrapping if enabled */
            if (PRETTY_INDENT(context) && context->wrapColumn >= 0) {
                char* trailing_nl = NULL;

                /* Locate the start of the current line in the buffer */
                trailing_nl = strrchr(buf->data, '\n');
                if (trailing_nl == NULL)
                    trailing_nl = buf->data;
                else
                    trailing_nl++;

                /*
                 * Add a newline, plus some indentation, if the new item would
                 * cause an overflow.
                 */
                if (strlen(trailing_nl) + strlen(itembuf.data) > (unsigned int)(context->wrapColumn))
                    appendContextKeyword(context, "", -PRETTYINDENT_STD, PRETTYINDENT_STD, PRETTYINDENT_VAR);
            }

            /* Add the new item */
            appendStringInfoString(buf, itembuf.data);

            /* clean up */
            pfree_ext(itembuf.data);
        }
    }
}

static void get_from_clause_bucket(RangeTblEntry* rte, StringInfo buf, deparse_context* context)
{
    Assert(rte->isbucket);
    appendStringInfo(buf, " BUCKETS(");
    ListCell* lc = NULL;
    foreach (lc, rte->buckets) {
        appendStringInfo(buf, "%d", lfirst_int(lc));
        if (lc != list_tail(rte->buckets)) {
            appendStringInfo(buf, ",");
        }
    }
    appendStringInfo(buf, ") ");
}
static void get_from_clause_partition(RangeTblEntry* rte, StringInfo buf, deparse_context* context)
{
    Assert(rte->ispartrel);

    if (rte->pname) {
        /* get the newest partition name from oid given */
        pfree(rte->pname->aliasname);
        rte->pname->aliasname = getPartitionName(rte->partitionOid, false);
        appendStringInfo(buf, " PARTITION(%s)", quote_identifier(rte->pname->aliasname));
    } else {
        ListCell* cell = NULL;
        char* semicolon = "";

        Assert(rte->plist);
        appendStringInfo(buf, " PARTITION FOR(");
        foreach (cell, rte->plist) {
            Node* col = (Node*)lfirst(cell);

            appendStringInfoString(buf, semicolon);
            get_rule_expr(processIndirection(col, context, false), context, false);
            semicolon = " ,";
        }
        appendStringInfo(buf, ")");
    }
}

static void get_from_clause_subpartition(RangeTblEntry* rte, StringInfo buf, deparse_context* context)
{
    Assert(rte->ispartrel);

    if (rte->pname) {
        /* get the newest subpartition name from oid given */
        pfree(rte->pname->aliasname);
        rte->pname->aliasname = getPartitionName(rte->subpartitionOid, false);
        appendStringInfo(buf, " SUBPARTITION(%s)", quote_identifier(rte->pname->aliasname));
    } else {
        ListCell* cell = NULL;
        char* semicolon = "";

        Assert(rte->plist);
        appendStringInfo(buf, " SUBPARTITION FOR(");
        foreach (cell, rte->plist) {
            Node* col = (Node*)lfirst(cell);

            appendStringInfoString(buf, semicolon);
            get_rule_expr(processIndirection(col, context, false), context, false);
            semicolon = " ,";
        }
        appendStringInfo(buf, ")");
    }
}

static void get_from_clause_item(Node* jtnode, Query* query, deparse_context* context)
{
    StringInfo buf = context->buf;

    if (IsA(jtnode, RangeTblRef)) {
        int varno = ((RangeTblRef*)jtnode)->rtindex;
        RangeTblEntry* rte = rt_fetch(varno, query->rtable);
        bool gavealias = false;

        if (rte->lateral)
            appendStringInfoString(buf, "LATERAL ");

        switch (rte->rtekind) {
            case RTE_RELATION: {
                /* Normal relation RTE */
                if (OidIsValid(rte->refSynOid)) {
                    appendStringInfo(buf, "%s%s", only_marker(rte), GetQualifiedSynonymName(rte->refSynOid, true));
                } else {
                    appendStringInfo(
                        buf, "%s%s", only_marker(rte), generate_relation_name(rte->relid, context->namespaces));
                }
                if (rte->orientation == REL_COL_ORIENTED || rte->orientation == REL_TIMESERIES_ORIENTED)
                    query->vec_output = true;
            } break;
            case RTE_SUBQUERY:
                /* Subquery RTE */
                appendStringInfoChar(buf, '(');
                get_query_def(rte->subquery,
                    buf,
                    context->namespaces,
                    NULL,
                    context->prettyFlags,
                    context->wrapColumn,
                    context->indentLevel
#ifdef PGXC
                    ,
                    context->finalise_aggs,
                    context->sortgroup_colno,
                    context->parser_arg
#endif /* PGXC */
                    ,
                    context->qrw_phase,
                    context->viewdef,
                    context->is_fqs);
                appendStringInfoChar(buf, ')');
                break;
            case RTE_FUNCTION:
                /* Function RTE */
                get_rule_expr_funccall(rte->funcexpr, context, true);
                break;
            case RTE_VALUES:
                /* Values list RTE */
                appendStringInfoChar(buf, '(');
                get_values_def(rte->values_lists, context);
                appendStringInfoChar(buf, ')');
                break;
            case RTE_CTE:
                appendStringInfoString(buf, quote_identifier(rte->ctename));
                break;
            default:
                ereport(ERROR,
                    (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE), errmsg("unrecognized RTE kind: %d", (int)rte->rtekind)));
                break;
        }

        if (rte->isContainPartition) {
            get_from_clause_partition(rte, buf, context);
        }
        if (rte->isContainSubPartition) {
            get_from_clause_subpartition(rte, buf, context);
        }
        if (rte->isbucket) {
            get_from_clause_bucket(rte, buf, context);
        }

        if (rte->alias != NULL) {
            appendStringInfo(buf, " %s", quote_identifier(rte->alias->aliasname));
            gavealias = true;
        } else if (rte->rtekind == RTE_RELATION && strcmp(rte->eref->aliasname, get_relation_name(rte->relid)) != 0) {
            /*
             * Apparently the rel has been renamed since the rule was made.
             * Emit a fake alias clause so that variable references will still
             * work.  This is not a 100% solution but should work in most
             * reasonable situations.
             */
            appendStringInfo(buf, " %s", quote_identifier(rte->eref->aliasname));
            gavealias = true;
        }
#ifdef PGXC
        else if (rte->rtekind == RTE_SUBQUERY && rte->eref->aliasname) {
            /*
             *
             * This condition arises when the from clause is a view. The
             * corresponding subquery RTE has its eref set to view name.
             * The remote query generated has this subquery of which the
             * columns can be referred to as view_name.col1, so it should
             * be possible to refer to this subquery object.
             */
            appendStringInfo(buf, " %s", quote_identifier(rte->eref->aliasname));
            gavealias = true;
        }
#endif
        else if (rte->rtekind == RTE_FUNCTION) {
            /*
             * For a function RTE, always give an alias. This covers possible
             * renaming of the function and/or instability of the
             * FigureColname rules for things that aren't simple functions.
             */
            appendStringInfo(buf, " %s", quote_identifier(rte->eref->aliasname));
            gavealias = true;
        } else if (rte->rtekind == RTE_VALUES) {
            /* Alias is syntactically required for VALUES */
            appendStringInfo(buf, " %s", quote_identifier(rte->eref->aliasname));
            gavealias = true;
        }

        if (rte->rtekind == RTE_FUNCTION) {
            if (rte->funccoltypes != NIL) {
                /* Function returning RECORD, reconstruct the columndefs */
                if (!gavealias)
                    appendStringInfo(buf, " AS ");
                get_from_clause_coldeflist(
                    rte->eref->colnames, rte->funccoltypes, rte->funccoltypmods, rte->funccolcollations, context);
            } else {
                /*
                 * For a function RTE, always emit a complete column alias
                 * list; this is to protect against possible instability of
                 * the default column names (eg, from altering parameter
                 * names).
                 */
                get_from_clause_alias(rte->eref, rte, context);
            }
        } else {
            /*
             * For non-function RTEs, just report whatever the user originally
             * gave as column aliases.
             */
            get_from_clause_alias(rte->alias, rte, context);
        }

        /* Tablesample clause must go after any alias.*/
        if (rte->rtekind == RTE_RELATION && rte->tablesample) {
            get_tablesample_def(rte->tablesample, context);
        }
        if (rte->rtekind == RTE_RELATION && rte->timecapsule) {
            GetTimecapsuleDef(rte->timecapsule, context);
        }
    } else if (IsA(jtnode, JoinExpr)) {
        JoinExpr* j = (JoinExpr*)jtnode;
        bool need_paren_on_right = false;

        need_paren_on_right = PRETTY_PAREN(context) && !IsA(j->rarg, RangeTblRef) &&
                              !(IsA(j->rarg, JoinExpr) && ((JoinExpr*)j->rarg)->alias != NULL);

        if (!PRETTY_PAREN(context) || j->alias != NULL)
            appendStringInfoChar(buf, '(');

        get_from_clause_item(j->larg, query, context);

        if (j->isNatural) {
            if (!PRETTY_INDENT(context))
                appendStringInfoChar(buf, ' ');
            switch (j->jointype) {
                case JOIN_INNER:
                    appendContextKeyword(context, "NATURAL JOIN ", -PRETTYINDENT_JOIN, PRETTYINDENT_JOIN, 0);
                    break;
                case JOIN_LEFT:
                    appendContextKeyword(context, "NATURAL LEFT JOIN ", -PRETTYINDENT_JOIN, PRETTYINDENT_JOIN, 0);
                    break;
                case JOIN_FULL:
                    appendContextKeyword(context, "NATURAL FULL JOIN ", -PRETTYINDENT_JOIN, PRETTYINDENT_JOIN, 0);
                    break;
                case JOIN_RIGHT:
                    appendContextKeyword(context, "NATURAL RIGHT JOIN ", -PRETTYINDENT_JOIN, PRETTYINDENT_JOIN, 0);
                    break;
                default:
                    ereport(ERROR,
                        (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                            errmsg("unrecognized join type: %d", (int)j->jointype)));
            }
        } else {
            switch (j->jointype) {
                case JOIN_INNER:
                    if (j->quals)
                        appendContextKeyword(context, " JOIN ", -PRETTYINDENT_JOIN, PRETTYINDENT_JOIN, 2);
                    else
                        appendContextKeyword(context, " CROSS JOIN ", -PRETTYINDENT_JOIN, PRETTYINDENT_JOIN, 1);
                    break;
                case JOIN_LEFT:
                    appendContextKeyword(context, " LEFT JOIN ", -PRETTYINDENT_JOIN, PRETTYINDENT_JOIN, 2);
                    break;
                case JOIN_FULL:
                    appendContextKeyword(context, " FULL JOIN ", -PRETTYINDENT_JOIN, PRETTYINDENT_JOIN, 2);
                    break;
                case JOIN_RIGHT:
                    appendContextKeyword(context, " RIGHT JOIN ", -PRETTYINDENT_JOIN, PRETTYINDENT_JOIN, 2);
                    break;
                case JOIN_SEMI:
                    if (context->qrw_phase) {
                        appendContextKeyword(context, " SEMI JOIN ", -PRETTYINDENT_JOIN, PRETTYINDENT_JOIN, 2);
                        break;
                    }
                    /* fall through */
                case JOIN_ANTI:
                    if (context->qrw_phase) {
                        appendContextKeyword(context, " ANTI JOIN ", -PRETTYINDENT_JOIN, PRETTYINDENT_JOIN, 2);
                        break;
                    }
                    /* fall through */
                case JOIN_LEFT_ANTI_FULL:
                    if (context->qrw_phase) {
                        appendContextKeyword(
                            context, " LEFT ANTI FULL JOIN ", -PRETTYINDENT_JOIN, PRETTYINDENT_JOIN, 0);
                        break;
                    }
                    /* fall through */
                case JOIN_RIGHT_ANTI_FULL:
                    if (context->qrw_phase) {
                        appendContextKeyword(
                            context, " RIGHT ANTI FULL JOIN ", -PRETTYINDENT_JOIN, PRETTYINDENT_JOIN, 0);
                        break;
                    }
                    /* fall through */
                default:
                    ereport(ERROR,
                        (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                            errmsg("unrecognized join type: %d", (int)j->jointype)));
            }
        }

        if (need_paren_on_right)
            appendStringInfoChar(buf, '(');
        get_from_clause_item(j->rarg, query, context);
        if (need_paren_on_right)
            appendStringInfoChar(buf, ')');

        context->indentLevel -= PRETTYINDENT_JOIN_ON;

        if (!j->isNatural) {
            if (j->usingClause) {
                ListCell* col = NULL;

                appendStringInfo(buf, " USING (");
                foreach (col, j->usingClause) {
                    if (col != list_head(j->usingClause))
                        appendStringInfo(buf, ", ");
                    appendStringInfoString(buf, quote_identifier(strVal(lfirst(col))));
                }
                appendStringInfoChar(buf, ')');
            } else if (j->quals) {
                appendStringInfo(buf, " ON ");
                if (!PRETTY_PAREN(context))
                    appendStringInfoChar(buf, '(');
                get_rule_expr(j->quals, context, false);
                if (!PRETTY_PAREN(context))
                    appendStringInfoChar(buf, ')');
            }
        }
        if (!PRETTY_PAREN(context) || j->alias != NULL)
            appendStringInfoChar(buf, ')');

        /* Yes, it's correct to put alias after the right paren ... */
        if (j->alias != NULL) {
            appendStringInfo(buf, " %s", quote_identifier(j->alias->aliasname));
            get_from_clause_alias(j->alias, rt_fetch(j->rtindex, query->rtable), context);
        }
    } else if (context->qrw_phase && IsA(jtnode, FromExpr)) {
        FromExpr* expr = (FromExpr*)jtnode;

        appendStringInfoChar(buf, '(');
        appendStringInfo(buf, "SELECT *");

        /* Add the FROM clause if needed */
        get_from_clause(query, " FROM ", context, expr->fromlist);

        /* Add the WHERE clause if given */
        if (expr->quals != NULL) {
            appendContextKeyword(context, " WHERE ", -PRETTYINDENT_STD, PRETTYINDENT_STD, 1);
            get_rule_expr(expr->quals, context, false);
        }
        appendStringInfoChar(buf, ')');
    } else
        ereport(ERROR,
            (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE), errmsg("unrecognized node type: %d", (int)nodeTag(jtnode))));
}

/*
 * get_from_clause_alias - reproduce column alias list
 *
 * This is tricky because we must ignore dropped columns.
 */
static void get_from_clause_alias(Alias* alias, RangeTblEntry* rte, deparse_context* context)
{
    StringInfo buf = context->buf;
    ListCell* col = NULL;
    AttrNumber attnum;
    bool first = true;

    if (alias == NULL || alias->colnames == NIL)
        return; /* definitely nothing to do */

    attnum = 0;
    foreach (col, alias->colnames) {
        attnum++;
        if (get_rte_attribute_is_dropped(rte, attnum))
            continue;
        if (first) {
            appendStringInfoChar(buf, '(');
            first = false;
        } else
            appendStringInfo(buf, ", ");
        appendStringInfoString(buf, quote_identifier(strVal(lfirst(col))));
    }
    if (!first)
        appendStringInfoChar(buf, ')');
}

/*
 * Description: Print a TableSampleClause.
 *
 * Parameters: @in tablesample: TableSampleClause node.
 *             @out context: rewrite sql.
 *
 * Return: void
 */
static void get_tablesample_def(TableSampleClause* tablesample, deparse_context* context)
{
    StringInfo buf = context->buf;
    int nargs = 0;
    ListCell* l = NULL;
    const char* sampleMethod = (tablesample->sampleType == SYSTEM_SAMPLE)
                                   ? "SYSTEM"
                                   : ((tablesample->sampleType == BERNOULLI_SAMPLE) ? "BERNOULLI" : "HYBRID");

    /*
     * We should qualify the handler's function name if it wouldn't be
     * resolved by lookup in the current search path.
     */
    appendStringInfo(buf, " TABLESAMPLE %s (", sampleMethod);

    foreach (l, tablesample->args) {
        if (nargs++ > 0) {
            appendStringInfoString(buf, ", ");
        }
        get_rule_expr((Node*)lfirst(l), context, false);
    }
    appendStringInfoChar(buf, ')');

    if (tablesample->repeatable != NULL) {
        appendStringInfoString(buf, " REPEATABLE (");
        get_rule_expr((Node*)tablesample->repeatable, context, false);
        appendStringInfoChar(buf, ')');
    }
}

/*
 * Description: Print a TimeCapsuleClause.
 *
 * Parameters:
 *	@in timecapsule: TimeCapsuleClause node.
 *	@out context: rewrite sql.
 *
 * Return: void
 */
static void GetTimecapsuleDef(const TimeCapsuleClause* timeCapsule, deparse_context* context)
{
    StringInfo buf = context->buf;

    /*
     * We should qualify the handler's function name if it wouldn't be
     * resolved by lookup in the current search path.
     */
    appendStringInfo(buf, " TIMECAPSULE %s ", (timeCapsule->tvtype == TV_VERSION_CSN) ? "CSN" : "TIMESTAMP");

    get_rule_expr(timeCapsule->tvver, context, false);
}

/*
 * get_from_clause_coldeflist - reproduce FROM clause coldeflist
 *
 * The coldeflist is appended immediately (no space) to buf.  Caller is
 * responsible for ensuring that an alias or AS is present before it.
 */
static void get_from_clause_coldeflist(
    List* names, List* types, List* typmods, List* collations, deparse_context* context)
{
    StringInfo buf = context->buf;
    ListCell* l1 = NULL;
    ListCell* l2 = NULL;
    ListCell* l3 = NULL;
    ListCell* l4 = NULL;
    int i = 0;

    appendStringInfoChar(buf, '(');

    l2 = list_head(types);
    l3 = list_head(typmods);
    l4 = list_head(collations);
    foreach (l1, names) {
        char* attname = strVal(lfirst(l1));
        Oid atttypid;
        int32 atttypmod;
        Oid attcollation;

        atttypid = lfirst_oid(l2);
        l2 = lnext(l2);
        atttypmod = lfirst_int(l3);
        l3 = lnext(l3);
        attcollation = lfirst_oid(l4);
        l4 = lnext(l4);

        if (i > 0)
            appendStringInfo(buf, ", ");
        appendStringInfo(buf, "%s %s", quote_identifier(attname), format_type_with_typemod(atttypid, atttypmod));
        if (OidIsValid(attcollation) && attcollation != get_typcollation(atttypid))
            appendStringInfo(buf, " COLLATE %s", generate_collation_name(attcollation));
        i++;
    }

    appendStringInfoChar(buf, ')');
}

/*
 * get_opclass_name			- fetch name of an index operator class
 *
 * The opclass name is appended (after a space) to buf.
 *
 * Output is suppressed if the opclass is the default for the given
 * actual_datatype.  (If you don't want this behavior, just pass
 * InvalidOid for actual_datatype.)
 */
static void get_opclass_name(Oid opclass, Oid actual_datatype, StringInfo buf)
{
    HeapTuple ht_opc;
    Form_pg_opclass opcrec;
    char* opcname = NULL;
    char* nspname = NULL;

    ht_opc = SearchSysCache1(CLAOID, ObjectIdGetDatum(opclass));
    if (!HeapTupleIsValid(ht_opc))
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for opclass %u", opclass)));
    opcrec = (Form_pg_opclass)GETSTRUCT(ht_opc);

    if (!OidIsValid(actual_datatype) || GetDefaultOpClass(actual_datatype, opcrec->opcmethod) != opclass) {
        /* Okay, we need the opclass name.	Do we need to qualify it? */
        opcname = NameStr(opcrec->opcname);
        if (OpclassIsVisible(opclass))
            appendStringInfo(buf, " %s", quote_identifier(opcname));
        else {
            nspname = get_namespace_name(opcrec->opcnamespace);
            appendStringInfo(buf, " %s.%s", quote_identifier(nspname), quote_identifier(opcname));
        }
    }
    ReleaseSysCache(ht_opc);
}

/*
 * processIndirection - take care of array and subfield assignment
 *
 * We strip any top-level FieldStore or assignment ArrayRef nodes that
 * appear in the input, and return the subexpression that's to be assigned.
 * If printit is true, we also print out the appropriate decoration for the
 * base column name (that the caller just printed).
 */
static Node* processIndirection(Node* node, deparse_context* context, bool printit)
{
    StringInfo buf = context->buf;

    for (;;) {
        if (node == NULL)
            break;
        if (IsA(node, FieldStore)) {
            FieldStore* fstore = (FieldStore*)node;
            Oid typrelid;
            char* fieldname = NULL;

            /* lookup tuple type */
            typrelid = get_typ_typrelid(fstore->resulttype);
            if (!OidIsValid(typrelid))
                ereport(ERROR,
                    (errmodule(MOD_OPT),
                        (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                            errmsg("argument type %s of FieldStore is not a tuple type",
                                format_type_be(fstore->resulttype)))));

            /*
             * Print the field name.  There should only be one target field in
             * stored rules.  There could be more than that in executable
             * target lists, but this function cannot be used for that case.
             */
            Assert(list_length(fstore->fieldnums) == 1);
            fieldname = get_relid_attribute_name(typrelid, linitial_int(fstore->fieldnums));
            if (printit)
                appendStringInfo(buf, ".%s", quote_identifier(fieldname));

            /*
             * We ignore arg since it should be an uninteresting reference to
             * the target column or subcolumn.
             */
            node = (Node*)linitial(fstore->newvals);
        } else if (IsA(node, ArrayRef)) {
            ArrayRef* aref = (ArrayRef*)node;

            if (aref->refassgnexpr == NULL)
                break;
            if (printit)
                printSubscripts(aref, context);

            /*
             * We ignore refexpr since it should be an uninteresting reference
             * to the target column or subcolumn.
             */
            node = (Node*)aref->refassgnexpr;
        } else
            break;
    }

    return node;
}

static void printSubscripts(ArrayRef* aref, deparse_context* context)
{
    StringInfo buf = context->buf;
    ListCell* lowlist_item = NULL;
    ListCell* uplist_item = NULL;

    lowlist_item = list_head(aref->reflowerindexpr); /* could be NULL */
    foreach (uplist_item, aref->refupperindexpr) {
        appendStringInfoChar(buf, '[');
        if (lowlist_item != NULL) {
            get_rule_expr((Node*)lfirst(lowlist_item), context, false);
            appendStringInfoChar(buf, ':');
            lowlist_item = lnext(lowlist_item);
        }
        get_rule_expr((Node*)lfirst(uplist_item), context, false);
        appendStringInfoChar(buf, ']');
    }
}

/*
 * quote_identifier			- Quote an identifier only if needed
 *
 * When quotes are needed, we palloc the required space; slightly
 * space-wasteful but well worth it for notational simplicity.
 */
const char* quote_identifier(const char* ident)
{
    /*
     * Can avoid quoting if ident starts with a lowercase letter or underscore
     * and contains only lowercase letters, digits, and underscores, *and* is
     * not any SQL keyword.  Otherwise, supply quotes.
     */
    int nquotes = 0;
    bool safe = false;
    const char* ptr = NULL;
    char* result = NULL;
    char* optr = NULL;

    /*
     * would like to use <ctype.h> macros here, but they might yield unwanted
     * locale-specific results...
     */
    safe = ((ident[0] >= 'a' && ident[0] <= 'z') || ident[0] == '_');

    for (ptr = ident; *ptr; ptr++) {
        char ch = *ptr;

        if ((ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') || (ch == '_')) {
            /* okay */
        } else {
            safe = false;
            if (ch == '"')
                nquotes++;
        }
    }

    if (u_sess->attr.attr_sql.quote_all_identifiers)
        safe = false;

    if (safe) {
        /*
         * Check for keyword.  We quote keywords except for unreserved ones.
         * (In some cases we could avoid quoting a col_name or type_func_name
         * keyword, but it seems much harder than it's worth to tell that.)
         *
         * Note: ScanKeywordLookup() does case-insensitive comparison, but
         * that's fine, since we already know we have all-lower-case.
         */
        const ScanKeyword* keyword = ScanKeywordLookup(ident, ScanKeywords, NumScanKeywords);

        if (keyword != NULL && keyword->category != UNRESERVED_KEYWORD)
            safe = false;
    }

    if (safe)
        return ident; /* no change needed */

    result = (char*)palloc(strlen(ident) + nquotes + 2 + 1);

    optr = result;
    *optr++ = '"';
    for (ptr = ident; *ptr; ptr++) {
        char ch = *ptr;

        if (ch == '"')
            *optr++ = '"';
        *optr++ = ch;
    }
    *optr++ = '"';
    *optr = '\0';

    return result;
}

/*
 * quote_qualified_identifier	- Quote a possibly-qualified identifier
 *
 * Return a name of the form qualifier.ident, or just ident if qualifier
 * is NULL, quoting each component if necessary.  The result is palloc'd.
 */
char* quote_qualified_identifier(const char* qualifier, const char* ident1, const char* ident2)
{
    StringInfoData buf;

    initStringInfo(&buf);
    if (qualifier != NULL)
        appendStringInfo(&buf, "%s.", quote_identifier(qualifier));
    if (ident1 != NULL)
        appendStringInfoString(&buf, quote_identifier(ident1));
    if (ident2 != NULL && ident1 != NULL) {
        appendStringInfo(&buf, ".%s", quote_identifier(ident2));
    } else if (ident1 == NULL && ident2 != NULL) {
        appendStringInfoString(&buf, quote_identifier(ident2));
    }
    return buf.data;
}

/*
 * get_relation_name
 *		Get the unqualified name of a relation specified by OID
 *
 * This differs from the underlying get_rel_name() function in that it will
 * throw error instead of silently returning NULL if the OID is bad.
 */
static char* get_relation_name(Oid relid)
{
    char* relname = get_rel_name(relid);

    if (relname == NULL)
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for relation %u", relid)));
    return relname;
}

/*
 * generate_relation_name
 *		Compute the name to display for a relation specified by OID
 *
 * The result includes all necessary quoting and schema-prefixing.
 *
 * If namespaces isn't NIL, it must be a list of deparse_namespace nodes.
 * We will forcibly qualify the relation name if it equals any CTE name
 * visible in the namespace list.
 */
static char* generate_relation_name(Oid relid, List* namespaces)
{
    HeapTuple tp;
    Form_pg_class reltup;
    bool need_qual = false;
    ListCell* nslist = NULL;
    char* relname = NULL;
    char* nspname = NULL;
    char* result = NULL;

    tp = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
    if (!HeapTupleIsValid(tp))
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for relation %u", relid)));
    reltup = (Form_pg_class)GETSTRUCT(tp);
    relname = NameStr(reltup->relname);

    /* Check for conflicting CTE name */
    foreach (nslist, namespaces) {
        deparse_namespace* dpns = (deparse_namespace*)lfirst(nslist);
        ListCell* ctlist = NULL;

        foreach (ctlist, dpns->ctes) {
            CommonTableExpr* cte = (CommonTableExpr*)lfirst(ctlist);

            if (strcmp(cte->ctename, relname) == 0) {
                need_qual = true;
                break;
            }
        }
        if (need_qual)
            break;
    }

    /* Otherwise, qualify the name if not visible in search path */
    if (!need_qual)
        need_qual = !RelationIsVisible(relid);

    if (need_qual)
        nspname = get_namespace_name(reltup->relnamespace);
    else
        nspname = NULL;

    result = quote_qualified_identifier(nspname, relname);

    ReleaseSysCache(tp);

    return result;
}

/*
 * generate_function_name
 *		Compute the name to display for a function specified by OID,
 *		given that it is being called with the specified actual arg names and
 *		types.	(Those matter because of ambiguous-function resolution rules.)
 *
 * The result includes all necessary quoting and schema-prefixing.	We can
 * also pass back an indication of whether the function is variadic.
 */
static char* generate_function_name(
    Oid funcid, int nargs, List* argnames, Oid* argtypes, bool was_variadic, bool* use_variadic_p)
{
    HeapTuple proctup;
    Form_pg_proc procform;
    char* proname = NULL;
    char* nspname = NULL;
    char* result = NULL;
    bool use_variadic = false;
    FuncDetailCode p_result;
    Oid p_funcid;
    Oid p_rettype;
    bool p_retset = false;
    int p_nvargs;
    Oid* p_true_typeids = NULL;
    Oid p_vatype;
    NameData* pkgname = NULL;
    Datum pkgOiddatum;
    Oid pkgOid = InvalidOid;
    bool isnull = true;
    proctup = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
    if (!HeapTupleIsValid(proctup))
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for function %u", funcid)));
    procform = (Form_pg_proc)GETSTRUCT(proctup);
    proname = NameStr(procform->proname);
    pkgOiddatum = SysCacheGetAttr(PROCOID, proctup, Anum_pg_proc_packageid, &isnull);
    if (!isnull) {
        pkgOid = DatumGetObjectId(pkgOiddatum);
        pkgname = GetPackageName(pkgOid);
    }
    /*
     * If this function's element type of variadic array is not ANY
     * or it was used originally, we should print VARIADIC.  We must do
     * this first since it affects the lookup rules in func_get_detail().
     */
    if (use_variadic_p != NULL) {
        if (OidIsValid(procform->provariadic)) {
            if (procform->provariadic == ANYOID)
                use_variadic = was_variadic;
            else
                use_variadic = true;
        }
        *use_variadic_p = use_variadic;
    } else {
        Assert(!was_variadic);
    }

    /*
     * The idea here is to schema-qualify only if the parser would fail to
     * resolve the correct function given the unqualified func name with the
     * specified argtypes and VARIADIC flag.
     */
    p_result = func_get_detail(list_make1(makeString(proname)),
        NIL,
        argnames,
        nargs,
        argtypes,
        !use_variadic,
        true,
        &p_funcid,
        &p_rettype,
        &p_retset,
        &p_nvargs,
        &p_vatype,
        &p_true_typeids,
        NULL);
    if ((p_result == FUNCDETAIL_NORMAL || p_result == FUNCDETAIL_AGGREGATE || p_result == FUNCDETAIL_WINDOWFUNC) &&
        p_funcid == funcid)
        nspname = NULL;
    else
        nspname = get_namespace_name(procform->pronamespace);
    if (OidIsValid(pkgOid)) {
        result = quote_qualified_identifier(nspname, pkgname->data, proname);
    } else {
        result = quote_qualified_identifier(nspname, proname);
    }
    ReleaseSysCache(proctup);

    return result;
}

static Operator GetOperator(Form_pg_operator operform, Oid arg1, Oid arg2)
{
    Operator    p_result;
    char*       oprname = NameStr(operform->oprname);
    Value*      oprnameStr = makeString(oprname);
    List*       oprnameList = list_make1(oprnameStr);
    /*
     * The idea here is to schema-qualify only if the parser would fail to
     * resolve the correct operator given the unqualified op name with the
     * specified argtypes.
     */
    switch (operform->oprkind) {
        case 'b':
            p_result = oper(NULL, oprnameList, arg1, arg2, true, -1);
            break;
        case 'l':
            p_result = left_oper(NULL, oprnameList, arg2, true, -1);
            break;
        case 'r':
            p_result = right_oper(NULL, oprnameList, arg1, true, -1);
            break;
        default:
            ereport(ERROR,
                (errmodule(MOD_OPT),
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("unrecognized oprkind: %d", operform->oprkind))));
            p_result = NULL; /* keep compiler quiet */
            break;
    }

    list_free(oprnameList);
    pfree(oprnameStr);
    return p_result;
}

/*
 * generate_operator_name
 *		Compute the name to display for an operator specified by OID,
 *		given that it is being called with the specified actual arg types.
 *		(Arg types matter because of ambiguous-operator resolution rules.
 *		Pass InvalidOid for unused arg of a unary operator.)
 *
 * The result includes all necessary quoting and schema-prefixing,
 * plus the OPERATOR() decoration needed to use a qualified operator name
 * in an expression.
 */
static char* generate_operator_name(Oid operid, Oid arg1, Oid arg2)
{
    StringInfoData buf;
    HeapTuple opertup;
    Form_pg_operator operform;
    char* oprname = NULL;
    char* nspname = NULL;
    Operator p_result;

    initStringInfo(&buf);

    opertup = SearchSysCache1(OPEROID, ObjectIdGetDatum(operid));
    if (!HeapTupleIsValid(opertup))
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for operator %u", operid)));
    operform = (Form_pg_operator)GETSTRUCT(opertup);
    oprname = NameStr(operform->oprname);

    p_result = GetOperator(operform, arg1, arg2);

    if (p_result != NULL && oprid(p_result) == operid)
        nspname = NULL;
    else {
        nspname = get_namespace_name(operform->oprnamespace);
        appendStringInfo(&buf, "OPERATOR(%s.", quote_identifier(nspname));
    }

    appendStringInfoString(&buf, oprname);

    if (nspname != NULL)
        appendStringInfoChar(&buf, ')');

    if (p_result != NULL)
        ReleaseSysCache(p_result);

    ReleaseSysCache(opertup);

    return buf.data;
}

/*
 * generate_collation_name
 *		Compute the name to display for a collation specified by OID
 *
 * The result includes all necessary quoting and schema-prefixing.
 */
char* generate_collation_name(Oid collid)
{
    HeapTuple tp;
    Form_pg_collation colltup;
    char* collname = NULL;
    char* nspname = NULL;
    char* result = NULL;

    tp = SearchSysCache1(COLLOID, ObjectIdGetDatum(collid));
    if (!HeapTupleIsValid(tp))
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for collation %u", collid)));
    colltup = (Form_pg_collation)GETSTRUCT(tp);
    collname = NameStr(colltup->collname);

    if (!CollationIsVisible(collid))
        nspname = get_namespace_name(colltup->collnamespace);
    else
        nspname = NULL;

    result = quote_qualified_identifier(nspname, collname);

    ReleaseSysCache(tp);

    return result;
}

/*
 * Given a C string, produce a TEXT datum.
 *
 * We assume that the input was palloc'd and may be freed.
 */
static text* string_to_text(char* str)
{
    text* result = NULL;

    result = cstring_to_text(str);
    pfree_ext(str);
    return result;
}

/*
 * Generate a C string representing a relation's reloptions, or NULL if none.
 */
static char* flatten_reloptions(Oid relid)
{
    char* result = NULL;
    HeapTuple tuple;
    Datum reloptions;
    bool isnull = false;

    tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
    if (!HeapTupleIsValid(tuple))
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for relation %u", relid)));

    reloptions = SysCacheGetAttr(RELOID, tuple, Anum_pg_class_reloptions, &isnull);
    if (!isnull) {
        Datum sep, txt;

        /*
         * We want to use array_to_text(reloptions, ', ') --- but
         * DirectFunctionCall2(array_to_text) does not work, because
         * array_to_text() relies on flinfo to be valid.  So use
         * OidFunctionCall2.
         */
        sep = CStringGetTextDatum(", ");
        txt = OidFunctionCall2(F_ARRAY_TO_TEXT, reloptions, sep);
        result = TextDatumGetCString(txt);
    }

    ReleaseSysCache(tuple);

    return result;
}

/*Get table distribute columns*/
Datum getDistributeKey(PG_FUNCTION_ARGS)
{
    Oid relOid = PG_GETARG_OID(0);
    Relation pcrel;
    ScanKeyData skey;
    SysScanDesc pcscan;
    HeapTuple htup;
    Form_pgxc_class pgxc_class;
    StringInfoData attname = {NULL, 0, 0, 0};
    initStringInfo(&attname);

    ScanKeyInit(&skey, Anum_pgxc_class_pcrelid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(relOid));

    pcrel = heap_open(PgxcClassRelationId, AccessShareLock);
    pcscan = systable_beginscan(pcrel, PgxcClassPgxcRelIdIndexId, true, NULL, 1, &skey);
    htup = systable_getnext(pcscan);

    if (!HeapTupleIsValid(htup)) {
        systable_endscan(pcscan);
        heap_close(pcrel, AccessShareLock);
        if (IS_PGXC_COORDINATOR) {
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("could not open relation with OID %u", relOid))));
        } else if (IS_SINGLE_NODE) {
            /* Tables do not contain distribution information in Single instance mode */
            PG_RETURN_NULL();
        } else {
            ereport(WARNING,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                    errmsg("The info of distribution for the table is not available due to dedicated connection to "
                           "datanode")));
            PG_RETURN_NULL();
        }
    }

    pgxc_class = (Form_pgxc_class)GETSTRUCT(htup);
    if (pgxc_class->pcattnum.values[0] == InvalidAttrNumber) {
        systable_endscan(pcscan);
        heap_close(pcrel, AccessShareLock);
        PG_RETURN_NULL();
    }

    for (int i = 0; i < pgxc_class->pcattnum.dim1; i++) {
        if (i == 0) {
            appendStringInfoString(&attname, get_attname(relOid, pgxc_class->pcattnum.values[i]));
        } else {
            appendStringInfoString(&attname, ", ");
            appendStringInfoString(&attname, get_attname(relOid, pgxc_class->pcattnum.values[i]));
        }
    }

    systable_endscan(pcscan);
    heap_close(pcrel, AccessShareLock);
    PG_RETURN_TEXT_P(string_to_text(attname.data));
}

List* get_operator_name(Oid operid, Oid arg1, Oid arg2)
{
    HeapTuple opertup;
    Form_pg_operator operform;
    char* oprname = NULL;
    char* nspname = NULL;
    Operator p_result;
    List* opname = NIL;

    opertup = SearchSysCache1(OPEROID, ObjectIdGetDatum(operid));
    if (!HeapTupleIsValid(opertup))
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for operator %u", operid)));
    operform = (Form_pg_operator)GETSTRUCT(opertup);
    oprname = pstrdup(NameStr(operform->oprname));

    p_result = GetOperator(operform, arg1, arg2);

    if (p_result != NULL && oprid(p_result) == operid) {
        opname = NULL;
        opname = list_make1(makeString(oprname));
    } else {
        nspname = get_namespace_name(operform->oprnamespace);
        opname = list_make2(makeString(nspname), makeString(oprname));
    }

    if (p_result)
        ReleaseSysCache(p_result);

    ReleaseSysCache(opertup);

    return opname;
}

static void deparse_sequence_options(List* options, StringInfoData& str, bool owned_by_none, bool doingAlter)
{
    ListCell* option = NULL;
    foreach (option, options) {
        DefElem* defel = (DefElem*)lfirst(option);
        if (strcmp(defel->defname, "increment") == 0) {
            appendStringInfo(&str, " INCREMENT %ld", defGetInt64(defel));
        } else if (strcmp(defel->defname, "start") == 0) {
            appendStringInfo(&str, " START %ld", defGetInt64(defel));
        } else if (strcmp(defel->defname, "restart") == 0) {
            if (defel->arg == NULL)
                appendStringInfo(&str, " RESTART");
            else
                appendStringInfo(&str, " RESTART %ld", defGetInt64(defel));
        } else if (strcmp(defel->defname, "maxvalue") == 0) {
            if (defel->arg == NULL)
                appendStringInfo(&str, " NOMAXVALUE");
            else
                appendStringInfo(&str, " MAXVALUE %ld", defGetInt64(defel));
        } else if (strcmp(defel->defname, "minvalue") == 0) {
            if (defel->arg == NULL)
                appendStringInfo(&str, " NOMINVALUE");
            else
                appendStringInfo(&str, " MINVALUE %ld", defGetInt64(defel));
        } else if (strcmp(defel->defname, "cache") == 0) {
            appendStringInfo(&str, " CACHE %ld", defGetInt64(defel));
        } else if (strcmp(defel->defname, "cycle") == 0) {
            if (defGetInt64(defel) != 0)
                appendStringInfo(&str, " CYCLE");
            else
                appendStringInfo(&str, " NO CYCLE");
        } else if (strcmp(defel->defname, "owned_by") == 0) {
            if (owned_by_none) {
                if (doingAlter) {
                    appendStringInfo(&str, " OWNED BY NONE");
                }
                continue;
            }

            List* owned_by = defGetQualifiedName(defel);
            int nnames = list_length(owned_by);
            if (list_length(owned_by) == 1) {
                appendStringInfo(&str, " OWNED BY NONE");
            } else {
                List* relname = NIL;
                char* attrname = NULL;
                RangeVar* rel = NULL;

                /* Separate relname and attr name */
                relname = list_truncate(list_copy(owned_by), nnames - 1);
                attrname = strVal(lfirst(list_tail(owned_by)));
                rel = makeRangeVarFromNameList(relname);

                appendStringInfo(&str, " OWNED BY ");
                if (rel->schemaname && rel->schemaname[0])
                    appendStringInfo(&str, "%s.", quote_identifier(rel->schemaname));

                appendStringInfo(&str, "%s.%s", quote_identifier(rel->relname), quote_identifier(attrname));
            }
        } else {
            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("option \"%s\" not recognized", defel->defname)));
        }
    }
}


/*
 * deparse_create_sequence
 *          - General utility for deparsing create sequence.
 *
 */
char* deparse_create_sequence(Node* stmt, bool owned_by_none)
{
    StringInfoData str;
    CreateSeqStmt* create_seq = (CreateSeqStmt*)stmt;

    RangeVar* seqname = create_seq->sequence;
    bool istemp = (seqname->relpersistence == RELPERSISTENCE_TEMP);

    initStringInfo(&str);
    appendStringInfo(&str, "CREATE %s SEQUENCE ", istemp ? "TEMP" : "");

    if (seqname->schemaname && seqname->schemaname[0])
        appendStringInfo(&str, "%s.", quote_identifier(seqname->schemaname));
    appendStringInfo(&str, "%s", quote_identifier(seqname->relname));

    deparse_sequence_options(create_seq->options, str, owned_by_none, false);

    appendStringInfo(&str, ";");

    return str.data;
}

/*
 * deparse_alter_sequence
 *          - General utility for deparsing alter sequence.
 *
 */
char* deparse_alter_sequence(Node* stmt, bool owned_by_none)
{
    StringInfoData str;
    AlterSeqStmt* alter_seq = (AlterSeqStmt*)stmt;

    RangeVar* seqname = alter_seq->sequence;
    bool missing_ok = alter_seq->missing_ok;

    initStringInfo(&str);
    appendStringInfo(&str, "ALTER %s SEQUENCE ", missing_ok ? "IF EXISTS" : "");

    if (seqname->schemaname && seqname->schemaname[0])
        appendStringInfo(&str, "%s.", quote_identifier(seqname->schemaname));
    appendStringInfo(&str, "%s", quote_identifier(seqname->relname));

    deparse_sequence_options(alter_seq->options, str, owned_by_none, true);

    appendStringInfo(&str, ";");

    return str.data;
}

/*
 * Replace column managed type with original type for proper print
 * via proallargtypes
 */
static void replace_cl_types_in_argtypes(Oid func_id, int numargs, Oid* argtypes, bool* is_client_logic)
{
    HeapTuple gs_oldtup = NULL;
    Oid* vec_gs_all_types_orig = NULL;
    oidvector* proargcachedcol = NULL;
    int n_gs_args = 0;
    bool use_all_arg_types = false;
    for (int i = 0; i < numargs; i++) {
        if (IsClientLogicType(argtypes[i])) {
            if (!HeapTupleIsValid(gs_oldtup)) {
                /*
                 * check if use proallargtypes for substitution
                 */
                gs_oldtup = SearchSysCache1(GSCLPROCID, ObjectIdGetDatum(func_id));
                if (!HeapTupleIsValid(gs_oldtup)) {
                    break;
                }
                bool isnull = false;
                Datum gs_all_types_orig =
                    SysCacheGetAttr(GSCLPROCID, gs_oldtup, Anum_gs_encrypted_proc_proallargtypes_orig, &isnull);
                if (!isnull) {
                    ArrayType* arr_gs_all_types_orig = DatumGetArrayTypeP(gs_all_types_orig);
                    vec_gs_all_types_orig = (Oid*)ARR_DATA_PTR(arr_gs_all_types_orig);
                    n_gs_args = ARR_DIMS(arr_gs_all_types_orig)[0];
                    if (n_gs_args <= numargs) {
                        use_all_arg_types = true;
                    }
                }
                if (!use_all_arg_types) {
                    proargcachedcol = (oidvector*)DatumGetPointer(
                        SysCacheGetAttr(GSCLPROCID, gs_oldtup, Anum_gs_encrypted_proc_proargcachedcol, &isnull));
                    n_gs_args = proargcachedcol->dim1;
                }
            }
            if (i >= n_gs_args) {
                break;
            }
            if (use_all_arg_types) {
                /*
                 * For output parameter if origin type is -1
                 * use return value instead
                 */
                if (vec_gs_all_types_orig[i] != (Oid)-1) {
                    argtypes[i] = vec_gs_all_types_orig[i];
                } else {
                    bool isnull = false;
                    Datum rettype_orig =
                        SysCacheGetAttr(GSCLPROCID, gs_oldtup, Anum_gs_encrypted_proc_prorettype_orig, &isnull);
                    if (!isnull) {
                        argtypes[i] = DatumGetObjectId(rettype_orig);
                    }
                }
            } else {
                Oid cachedColId = proargcachedcol->values[i];
                HeapTuple tup = SearchSysCache1(CEOID, ObjectIdGetDatum(cachedColId));
                if (HeapTupleIsValid(tup)) {
                    Form_gs_encrypted_columns gs_cl_columns = (Form_gs_encrypted_columns)GETSTRUCT(tup);
                    argtypes[i] = gs_cl_columns->data_type_original_oid;
                    ReleaseSysCache(tup);
                }
            }
            is_client_logic[i] = true;
        }
    }
    if (HeapTupleIsValid(gs_oldtup)) {
        ReleaseSysCache(gs_oldtup);
    }
}
