/* -------------------------------------------------------------------------
 *
 * prep.h
 *	  prototypes for files in optimizer/prep/
 *
 *
 * Portions Copyright (c) 2021, openGauss Contributors
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/optimizer/prep.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef PREP_H
#define PREP_H

#include "nodes/plannodes.h"
#include "nodes/parsenodes.h"
#include "nodes/relation.h"

#include "utils/guc.h"
#include "catalog/pg_operator.h"
#include "optimizer/clauses.h"

typedef enum UNIONALL_SHIPPING_TYPE
{
    SHIPPING_NONE,
    SHIPPING_PARTIAL,
    SHIPPING_ALL
}UNIONALL_SHIPPING_TYPE;

/*
 * find_dependent_phvs - are there any PlaceHolderVars whose relids are
 * exactly the given varno?
 */

typedef struct
{
    Relids          relids;
    int                     sublevels_up;
} find_dependent_phvs_context;

/*
 * prototypes for prepjointree.c
 */
extern void replace_empty_jointree(Query *parse);
extern Node *remove_useless_results_recurse(PlannerInfo *root, Node *jtnode);
extern int     get_result_relid(PlannerInfo *root, Node *jtnode);
extern void remove_result_refs(PlannerInfo *root, int varno, Node *newjtloc);
extern bool find_dependent_phvs(Node *node, int varno);
extern bool find_dependent_phvs_walker(Node *node, find_dependent_phvs_context *context);
extern void pull_up_sublinks(PlannerInfo* root);
extern void inline_set_returning_functions(PlannerInfo* root);
extern Node* pull_up_subqueries(PlannerInfo* root, Node* jtnode);
extern void flatten_simple_union_all(PlannerInfo* root);
extern void removeNotNullTest(PlannerInfo* root);
extern void reduce_outer_joins(PlannerInfo* root);
extern void remove_useless_result_rtes(PlannerInfo *root);
extern void reduce_inequality_fulljoins(PlannerInfo* root);
extern Relids get_relids_in_jointree(Node* jtnode, bool include_joins);
extern Relids get_relids_for_join(PlannerInfo* root, int joinrelid);
extern void pull_up_subquery_hint(PlannerInfo* root, Query* parse, HintState* hint_state);
extern Node *remove_useless_results_recurse(PlannerInfo *root, Node *jtnode);
#ifndef ENABLE_MULTIPLE_NODES
extern void preprocess_rownum(PlannerInfo *root, Query *parse);
#endif

/*
 * prototypes for prepnonjointree.cpp
 */
extern bool get_real_rte_varno_attno(
    Query* parse, Index* varno, AttrNumber* varattno, const Index targetVarno = InvalidOid);
extern bool is_join_inner_side(
    const Node* fromNode, const Index targetRTEIndex, const bool isParentInnerSide, bool* isFound);
extern Query* lazyagg_main(Query* parse);
extern void reduce_orderby(Query* query, bool reduce);

extern Node* get_real_rte_varno_attno_or_node(Query* parse, Index* varno, AttrNumber* varattno);

/*
 * prototypes for prepqual.c
 */
extern Node* negate_clause(Node* node);
extern Expr* canonicalize_qual(Expr* qual, bool is_check);

/*
 * prototypes for preptlist.c
 */
extern List* preprocess_targetlist(PlannerInfo* root, List* tlist);
extern List* preprocess_upsert_targetlist(List* tlist, int result_relation, List* range_table);
extern PlanRowMark* get_plan_rowmark(List* rowmarks, Index rtindex);

/*
 * prototypes for prepunion.c
 */
extern Plan* plan_set_operations(PlannerInfo* root, double tuple_fraction, List** sortClauses);

extern void expand_inherited_tables(PlannerInfo* root);
extern void make_inh_translation_list(
    Relation oldrelation, Relation newrelation, Index newvarno, List** translated_vars);
extern Bitmapset* translate_col_privs(const Bitmapset* parent_privs, List* translated_vars);

extern Node* adjust_appendrel_attrs(PlannerInfo* root, Node* node, AppendRelInfo* appinfo);

extern void mark_parent_child_pushdown_flag(Query *parent, Query *child);
extern bool check_base_rel_in_fromlist(Query *parse, Node *jtnode);
extern UNIONALL_SHIPPING_TYPE precheck_shipping_union_all(Query *subquery, Node *setOp);

/* judge if it is possible to optimize ROWNUM */
extern bool ContainRownumQual(const Query *parse);


#endif /* PREP_H */
