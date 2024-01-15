/* -------------------------------------------------------------------------
 *
 * parse_node.h
 *		Internal definitions for parser
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 * src/include/parser/parse_node.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef PARSE_NODE_H
#define PARSE_NODE_H

#include "nodes/parsenodes.h"
#include "parser/analyze.h"
#include "utils/relcache.h"

typedef struct PlusJoinRTEInfo {
    bool needrecord; /* need record the RTE info for plus join? only in WhereClause is true for now */
    List* info;      /* List of PlusJoinRTEItem we record */
} PlusJoinRTEInfo;

/*
 * Expression kinds distinguished by transformExpr().  Many of these are not
 * semantically distinct so far as expression transformation goes; rather,
 * we distinguish them so that context-specific error messages can be printed.
 *
 * Note: EXPR_KIND_OTHER is not used in the core code, but is left for use
 * by extension code that might need to call transformExpr().  The core code
 * will not enforce any context-driven restrictions on EXPR_KIND_OTHER
 * expressions, so the caller would have to check for sub-selects, aggregates,
 * window functions, SRFs, etc if those need to be disallowed.
 */
typedef enum ParseExprKind
{
    EXPR_KIND_NONE = 0,            /* "not in an expression" */
    EXPR_KIND_OTHER,            /* reserved for extensions */
    EXPR_KIND_JOIN_ON,            /* JOIN ON */
    EXPR_KIND_JOIN_USING,        /* JOIN USING */
    EXPR_KIND_FROM_SUBSELECT,    /* sub-SELECT in FROM clause */
    EXPR_KIND_FROM_FUNCTION,    /* function in FROM clause */
    EXPR_KIND_WHERE,            /* WHERE */
    EXPR_KIND_HAVING,            /* HAVING */
    EXPR_KIND_FILTER,            /* FILTER */
    EXPR_KIND_WINDOW_PARTITION, /* window definition PARTITION BY */
    EXPR_KIND_WINDOW_ORDER,        /* window definition ORDER BY */
    EXPR_KIND_WINDOW_FRAME_RANGE,    /* window frame clause with RANGE */
    EXPR_KIND_WINDOW_FRAME_ROWS,    /* window frame clause with ROWS */
    EXPR_KIND_WINDOW_FRAME_GROUPS,    /* window frame clause with GROUPS */
    EXPR_KIND_SELECT_TARGET,    /* SELECT target list item */
    EXPR_KIND_INSERT_TARGET,    /* INSERT target list item */
    EXPR_KIND_UPDATE_SOURCE,    /* UPDATE assignment source item */
    EXPR_KIND_UPDATE_TARGET,    /* UPDATE assignment target item */
    EXPR_KIND_GROUP_BY,            /* GROUP BY */
    EXPR_KIND_ORDER_BY,            /* ORDER BY */
    EXPR_KIND_DISTINCT_ON,        /* DISTINCT ON */
    EXPR_KIND_LIMIT,            /* LIMIT */
    EXPR_KIND_OFFSET,            /* OFFSET */
    EXPR_KIND_RETURNING,        /* RETURNING */
    EXPR_KIND_VALUES,            /* VALUES */
    EXPR_KIND_VALUES_SINGLE,    /* single-row VALUES (in INSERT only) */
    EXPR_KIND_CHECK_CONSTRAINT, /* CHECK constraint for a table */
    EXPR_KIND_DOMAIN_CHECK,        /* CHECK constraint for a domain */
    EXPR_KIND_COLUMN_DEFAULT,    /* default value for a table column */
    EXPR_KIND_FUNCTION_DEFAULT, /* default parameter value for function */
    EXPR_KIND_INDEX_EXPRESSION, /* index expression */
    EXPR_KIND_INDEX_PREDICATE,    /* index predicate */
    EXPR_KIND_STATS_EXPRESSION, /* extended statistics expression */
    EXPR_KIND_ALTER_COL_TRANSFORM,    /* transform expr in ALTER COLUMN TYPE */
    EXPR_KIND_EXECUTE_PARAMETER,    /* parameter value in EXECUTE */
    EXPR_KIND_TRIGGER_WHEN,        /* WHEN condition in CREATE TRIGGER */
    EXPR_KIND_POLICY,            /* USING or WITH CHECK expr in policy */
    EXPR_KIND_PARTITION_BOUND,    /* partition bound expression */
    EXPR_KIND_PARTITION_EXPRESSION, /* PARTITION BY expression */
    EXPR_KIND_CALL_ARGUMENT,    /* procedure argument in CALL */
    EXPR_KIND_COPY_WHERE,        /* WHERE condition in COPY FROM */
    EXPR_KIND_GENERATED_COLUMN, /* generation expression for a column */
    EXPR_KIND_MERGE_WHEN,        /* WHEN condition in MERTE stmt */
    EXPR_KIND_CYCLE_MARK,        /* cycle mark value */
} ParseExprKind;

/*
 * Function signatures for parser hooks
 */
typedef struct ParseState ParseState;

typedef Node* (*PreParseColumnRefHook)(ParseState* pstate, ColumnRef* cref);
typedef Node* (*PostParseColumnRefHook)(ParseState* pstate, ColumnRef* cref, Node* var);
typedef Node* (*ParseParamRefHook)(ParseState* pstate, ParamRef* pref);
typedef Node* (*CoerceParamHook)(ParseState* pstate, Param* param, Oid targetTypeId, int32 targetTypeMod, int location);
typedef Node* (*CreateProcOperatorHook)(ParseState* pstate, Node* left, Node* right, Oid* ltypeid, Oid* rtypeid);
typedef void (*CreateProcInsertrHook)(ParseState* pstate, int param_no, Oid param_new_type, Oid relid,
                const char* col_name);
typedef void* (*GetFuncinfoFromStateHelper)(void* param);

/*
 * State information used during parse analysis
 *
 * parentParseState: NULL in a top-level ParseState.  When parsing a subquery,
 * links to current parse state of outer query.
 *
 * p_sourcetext: source string that generated the raw parsetree being
 * analyzed, or NULL if not available.	(The string is used only to
 * generate cursor positions in error messages: we need it to convert
 * byte-wise locations in parse structures to character-wise cursor
 * positions.)
 *
 * p_rtable: list of RTEs that will become the rangetable of the query.
 * Note that neither relname nor refname of these entries are necessarily
 * unique; searching the rtable by name is a bad idea.
 *
 * p_joinexprs: list of JoinExpr nodes associated with p_rtable entries.
 * This is one-for-one with p_rtable, but contains NULLs for non-join
 * RTEs, and may be shorter than p_rtable if the last RTE(s) aren't joins.
 *
 * p_joinlist: list of join items (RangeTblRef and JoinExpr nodes) that
 * will become the fromlist of the query's top-level FromExpr node.
 *
 * p_relnamespace: list of RTEs that represents the current namespace for
 * table lookup, ie, those RTEs that are accessible by qualified names.
 * This may be just a subset of the rtable + joinlist, and/or may contain
 * entries that are not yet added to the main joinlist.
 *
 * p_varnamespace: list of RTEs that represents the current namespace for
 * column lookup, ie, those RTEs that are accessible by unqualified names.
 * This is different from p_relnamespace because a JOIN without an alias does
 * not hide the contained tables (so they must still be in p_relnamespace)
 * but it does hide their columns (unqualified references to the columns must
 * refer to the JOIN, not the member tables).  Other special RTEs such as
 * NEW/OLD for rules may also appear in just one of these lists.
 *
 * p_ctenamespace: list of CommonTableExprs (WITH items) that are visible
 * at the moment.  This is different from p_relnamespace because you have
 * to make an RTE before you can access a CTE.
 *
 * p_future_ctes: list of CommonTableExprs (WITH items) that are not yet
 * visible due to scope rules.	This is used to help improve error messages.
 *
 * p_parent_cte: CommonTableExpr that immediately contains the current query,
 * if any.
 *
 * p_windowdefs: list of WindowDefs representing WINDOW and OVER clauses.
 * We collect these while transforming expressions and then transform them
 * afterwards (so that any resjunk tlist items needed for the sort/group
 * clauses end up at the end of the query tlist).  A WindowDef's location in
 * this list, counting from 1, is the winref number to use to reference it.
 */
struct ParseState {
    struct ParseState* parentParseState; /* stack link */
    const char* p_sourcetext;            /* source text, or NULL if not available */
    List* p_rtable;                      /* range table so far */
    List* p_joinexprs;                   /* JoinExprs for RTE_JOIN p_rtable entries */
    List* p_joinlist;                    /* join items so far (will become FromExpr
                                            node's fromlist) */
    List* p_relnamespace;                /* current namespace for relations */
    List* p_varnamespace;                /* current namespace for columns */
    bool  p_lateral_active;              /* p_lateral_only items visible? */
    bool  p_is_flt_frame;                /* Indicates whether it is a flattened expr frame */
    List* p_ctenamespace;                /* current namespace for common table exprs */
    List* p_future_ctes;                 /* common table exprs not yet in namespace */
    CommonTableExpr* p_parent_cte;       /* this query's containing CTE */
    List* p_windowdefs;                  /* raw representations of window clauses */
    ParseExprKind p_expr_kind;           /* what kind of expression we're parsing */
    List* p_rawdefaultlist;              /* raw default list */
    int p_next_resno;                    /* next targetlist resno to assign */
    List* p_locking_clause;              /* raw FOR UPDATE/FOR SHARE info */
    Node* p_value_substitute;            /* what to replace VALUE with, if any */

    /* Flags telling about things found in the query: */
    bool p_hasAggs;
    bool p_hasWindowFuncs;
    bool p_hasTargetSRFs;
    bool p_hasSubLinks;
    bool p_hasModifyingCTE;
    bool p_is_insert;
    bool p_locked_from_parent;
    bool p_resolve_unknowns; /* resolve unknown-type SELECT outputs as type text */
    bool p_hasSynonyms;
    List* p_target_relation;
    List* p_target_rangetblentry;
    bool p_is_decode;

    Node *p_last_srf; /* most recent set-returning func/op found */

    /*
     * used for start with...connect by rewrite
     */
    bool p_addStartInfo;
    List *p_start_info;
    int sw_subquery_idx; /* given unname-subquery unique name when sw rewrite */
    SelectStmt *p_sw_selectstmt;
    List *sw_fromClause;
    WithClause *origin_with;
    bool p_hasStartWith;
    bool p_has_ignore;  /* whether SQL has ignore hint */

    /*
     * Optional hook functions for parser callbacks.  These are null unless
     * set up by the caller of make_parsestate.
     */
    PreParseColumnRefHook p_pre_columnref_hook;
    PostParseColumnRefHook p_post_columnref_hook;
    PreParseColumnRefHook p_bind_variable_columnref_hook;
    PreParseColumnRefHook p_bind_describe_hook;
    ParseParamRefHook p_paramref_hook;
    CoerceParamHook p_coerce_param_hook;
    CreateProcOperatorHook p_create_proc_operator_hook;
    CreateProcInsertrHook p_create_proc_insert_hook;
    void* p_ref_hook_state; /* common passthrough link for above */
    void* p_cl_hook_state; /* cl related state - SQLFunctionParseInfoPtr  */
    List* p_target_list;
    void* p_bind_hook_state;
    void* p_describeco_hook_state;

    /*
     * star flag info
     *
     * create table t1(a int, b int);
     * create table t2(a int, b int);
     *
     * For query:  select * from t1
     * star_start = 1;
     * star_end = 2;
     * star_only = 1;
     *
     * For query:  select t1.*, t2.* from t1, t2
     * star_start = 1, 3;
     * star_end =  2, 4;
     * star_only =  -1, -1;
     */
    List* p_star_start;
    List* p_star_end;
    List* p_star_only;

    /*
     * The 	p_is_in_insert will indicate the sub link is under one top insert statement.
     * When p_is_in_insert is true, then we will check if sub link include foreign table,
     * If foreign is found, we will set top level insert ParseState's p_is_foreignTbl_exist to true.
     * Finially, we will set p_is_td_compatible_truncation to true if the td_compatible_truncation guc
     * parameter is on, no foreign table involved in this insert statement.
     */
    bool p_is_foreignTbl_exist;          /* make there is foreign table founded. */
    bool p_is_in_insert;                 /* mark the subquery is under one insert statement. */
    bool p_is_td_compatible_truncation;  /* mark the auto truncation for insert statement is enabled. */
    TdTruncCastStatus tdTruncCastStatus; /* Auto truncation Cast added, only used for stmt in stored procedure or
                                            prepare stmt. */
    bool isAliasReplace;                 /* Mark if permit replace. */

    /*
     * Fields for transform "(+)" to outerjoin
     */
    bool ignoreplus;   /*
                        * Whether ignore "(+)" during transform stmt? False is default,
                        * report error when found "(+)". Only true when transform WhereClause
                        * in SelectStmt.
                        */

    bool use_level; /* When selecting a column with the same name in an RTE list, whether to consider the
                     * priority of RTE.
                     * The priority refers to the index of RTE in the list. The smaller the index value, the
                     * higher the priority.
                     */

    PlusJoinRTEInfo* p_plusjoin_rte_info; /* The RTE info while processing "(+)" */
    List* p_updateRelations; /* For multiple-update, this is used to record the target table in the
                              * update statement, then assign to qry->resultRelations.
                              */
    List* p_updateRangeVars; /* For multiple-update, use relationClase to generate RangeVar list. */

    RightRefState* rightRefState;

    /*
     * whether to record the columns referenced by the ORDER BY statement
     * when transforming the SortClause.
     */
    bool shouldCheckOrderbyCol;
    /*
     * store the columns that ORDER BY statement referencing
     * if shouldCheckOrderbyCol is true else NIL.
     */
    List* orderbyCols; 
    List* p_indexhintLists; /*Force or use index in index hint list*/
    bool has_uservar;
};

/* An element of p_relnamespace or p_varnamespace */
typedef struct ParseNamespaceItem
{
   RangeTblEntry *p_rte;       /* The relation's rangetable entry */
   bool        p_lateral_only; /* Is only visible to LATERAL expressions? */
   bool        p_lateral_ok;   /* If so, does join type allow use? */
} ParseNamespaceItem;

/* Support for parser_errposition_callback function */
typedef struct ParseCallbackState {
    ParseState* pstate;
    int location;
#ifndef FRONTEND
    ErrorContextCallback errcontext;
#endif
} ParseCallbackState;

extern ParseState* make_parsestate(ParseState* parentParseState);
extern void free_parsestate(ParseState* pstate);
extern int parser_errposition(ParseState* pstate, int location);

extern void setup_parser_errposition_callback(ParseCallbackState* pcbstate, ParseState* pstate, int location);
extern void cancel_parser_errposition_callback(ParseCallbackState* pcbstate);

extern Var* make_var(ParseState* pstate, RangeTblEntry* rte, int attrno, int location);
extern Var* ts_make_var(ParseState* pstate, RangeTblEntry* rte, int attrno, int location);
extern Oid transformArrayType(Oid* arrayType, int32* arrayTypmod);
extern ArrayRef* transformArraySubscripts(ParseState* pstate, Node* arrayBase, Oid arrayType, Oid elementType,
    int32 arrayTypMod, List* indirection, Node* assignFrom);
extern Const* make_const(ParseState* pstate, Value* value, int location);

#endif /* PARSE_NODE_H */
