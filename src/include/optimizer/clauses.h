/* -------------------------------------------------------------------------
 *
 * clauses.h
 *	  prototypes for clauses.c.
 *
 *
 * Portions Copyright (c) 2021, openGauss Contributors
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/optimizer/clauses.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef CLAUSES_H
#define CLAUSES_H

#include "nodes/relation.h"
#include "parser/parse_node.h"
#include "nodes/nodeFuncs.h"

#ifndef ENABLE_MULTIPLE_NODES
#define is_andclause(clause) \
    ((clause) != NULL && IsA(clause, BoolExpr) && (((const BoolExpr *)(clause))->boolop) == AND_EXPR)
#define is_orclause(clause) \
    ((clause) != NULL && IsA(clause, BoolExpr) && (((const BoolExpr *)(clause))->boolop) == OR_EXPR)
#define is_notclause(clause) \
    ((clause) != NULL && IsA(clause, BoolExpr) && (((const BoolExpr *)(clause))->boolop) == NOT_EXPR)
#endif /* ENABLE_MULTIPLE_NODES */

#define is_opclause(clause) ((clause) != NULL && IsA(clause, OpExpr))
#define is_funcclause(clause) ((clause) != NULL && IsA(clause, FuncExpr))

#ifdef ENABLE_MULTIPLE_NODES
#define AFORMAT_NULL_TEST_MODE false
#else
#define AFORMAT_NULL_TEST_MODE (u_sess->attr.attr_sql.sql_compatibility == A_FORMAT && AFORMAT_NULL_TEST)
#endif

typedef struct {
    int numWindowFuncs; /* total number of WindowFuncs found */
    Index maxWinRef;    /* windowFuncs[] is indexed 0 .. maxWinRef */
    List** windowFuncs; /* lists of WindowFuncs for each winref */
    List* activeWindows;
} WindowLists;

typedef struct {
    ParamListInfo boundParams;
    PlannerInfo* root;
    List* active_fns;
    Node* case_val;
    bool estimate;
#ifdef USE_SPQ
    bool recurse_queries; /* recurse into query structures */
    bool recurse_sublink_testexpr; /* recurse into sublink test expressions */
    Size max_size; /* max constant binary size in bytes, 0: no restrictions */
#endif
} eval_const_expressions_context;

typedef enum { UNIQUE_CONSTRAINT, NOT_NULL_CONSTRAINT } constraintType;

extern Expr* make_opclause(
    Oid opno, Oid opresulttype, bool opretset, Expr* leftop, Expr* rightop, Oid opcollid, Oid inputcollid);
extern Node* get_leftop(const Expr* clause);
extern Node* get_rightop(const Expr* clause);

extern bool not_clause(Node* clause);
extern Expr* make_notclause(Expr* notclause);
extern Expr* get_notclausearg(Expr* notclause);

extern bool or_clause(Node* clause);
extern Expr* make_orclause(List* orclauses);

extern bool and_clause(Node* clause);
extern Expr* make_andclause(List* andclauses);
extern Node* make_and_qual(Node* qual1, Node* qual2);
extern Expr* make_ands_explicit(List* andclauses);
extern List* make_ands_implicit(Expr* clause);

extern bool contain_agg_clause(Node* clause);
extern bool contain_specified_agg_clause(Node* clause);
extern void count_agg_clauses(PlannerInfo* root, Node* clause, AggClauseCosts* costs);

extern bool contain_window_function(Node* clause);
extern void free_windowFunc_lists(WindowLists* lists);
extern WindowLists* make_windows_lists(Index maxWinRef);
extern void find_window_functions(Node* clause, WindowLists* lists);

extern double expression_returns_set_rows(Node* clause);
extern double tlist_returns_set_rows(List* tlist);

extern bool contain_subplans(Node* clause);

extern bool contain_mutable_functions(Node* clause);
extern bool contain_volatile_functions(Node* clause, bool deep = false);
extern bool contain_specified_function(Node* clause, Oid funcid);
extern bool contain_nonstrict_functions(Node* clause, bool check_agg = false);
extern bool contain_leaky_functions(Node* clause);
extern bool exec_simple_check_mutable_function(Node* clause);

extern Relids find_nonnullable_rels(Node* clause);
extern List* find_nonnullable_vars(Node* clause);
extern List* find_forced_null_vars(Node* clause);
extern Var* find_forced_null_var(Node* clause);

extern bool is_pseudo_constant_clause(Node* clause);
extern bool is_pseudo_constant_clause_relids(Node* clause, Relids relids);

extern int NumRelids(Node* clause);

extern void CommuteOpExpr(OpExpr* clause);
extern void CommuteRowCompareExpr(RowCompareExpr* clause);

extern Node* strip_implicit_coercions(Node* node);

extern Node* eval_const_expressions(PlannerInfo* root, Node* node);

extern Node* eval_const_expression_value(PlannerInfo* root, Node* node);

extern Node* eval_const_expressions_params(PlannerInfo* root, Node* node, ParamListInfo boundParams);

extern Node *eval_const_expression_value(PlannerInfo* root, Node* node, ParamListInfo boundParams);

extern Node* simplify_select_into_expression(Node* node, ParamListInfo boundParams, int *targetlist_len);

extern Node* estimate_expression_value(PlannerInfo* root, Node* node, EState* estate = NULL);

extern Query* inline_set_returning_function(PlannerInfo* root, RangeTblEntry* rte);
extern bool filter_cstore_clause(PlannerInfo* root, Expr* clause);
/* evaluate_expr used to be a  static function */
extern Expr* evaluate_expr(Expr* expr, Oid result_type, int32 result_typmod, Oid result_collation,
                           bool can_ignore = false);
extern bool contain_var_unsubstitutable_functions(Node* clause);
extern void distribute_qual_to_rels(PlannerInfo* root, Node* clause, bool is_deduced, bool below_outer_join,
    JoinType jointype, Index security_level, Relids qualscope, Relids ojscope, Relids outerjoin_nonnullable,
    Relids deduced_nullable_relids, List **postponed_qual_list);
extern void check_plan_correlation(PlannerInfo* root, Node* expr);
extern bool findConstraintByVar(Var* var, Oid relid, constraintType conType);
extern bool is_var_node(Node* node);
extern Node* substitute_var(Node* expr, List* planTargetList);
extern bool treat_as_join_clause(Node* clause, RestrictInfo* rinfo, int varRelid, SpecialJoinInfo* sjinfo);
extern List* extract_function_outarguments(Oid funcid, List* parameters, List* funcname);
extern bool need_adjust_agg_inner_func_type(Aggref* aggref);


extern bool contain_rownum_walker(Node *node, void *context); 

extern bool ContainRownumExpr(Node *node);

/* Check if it includes Rownum */
static inline void ExcludeRownumExpr(ParseState* pstate, Node* expr)
{
    if (ContainRownumExpr(expr))                                                          
            ereport(ERROR,                                                                      
                (errcode(ERRCODE_SYNTAX_ERROR),                                                 
                errmsg("specified ROWNUM is not allowed here."),                                
                parser_errposition(pstate, exprLocation(expr))));
}

extern List* get_quals_lists(Node *jtnode);

extern bool isTableofType(Oid typeOid, Oid* base_oid, Oid* indexbyType);
extern Expr* simplify_function(Oid funcid, Oid result_type, int32 result_typmod, Oid result_collid, Oid input_collid,
    List** args_p, bool process_args, bool allow_non_const, eval_const_expressions_context* context);
 
#ifdef USE_SPQ
extern Query *fold_constants(PlannerInfo *root, Query *q, ParamListInfo boundParams, Size max_size);
extern Query *flatten_join_alias_var_optimizer(Query *query, int queryLevel);
extern Expr *transform_array_Const_to_ArrayExpr(Const *c);
#endif
#endif /* CLAUSES_H */
