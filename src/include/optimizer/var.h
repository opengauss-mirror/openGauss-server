/* -------------------------------------------------------------------------
 *
 * var.h
 *	  prototypes for optimizer/util/var.c.
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/optimizer/var.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef VAR_H
#define VAR_H

#include "nodes/relation.h"

typedef enum {
    PVC_REJECT_AGGREGATES,  /* throw error if Aggref found */
    PVC_INCLUDE_AGGREGATES, /* include Aggrefs in output list */
    PVC_INCLUDE_AGGREGATES_OR_WINAGGS,
    PVC_RECURSE_AGGREGATES /* recurse into Aggref arguments */
} PVCAggregateBehavior;

typedef enum {
    PVC_REJECT_PLACEHOLDERS,  /* throw error if PlaceHolderVar found */
    PVC_INCLUDE_PLACEHOLDERS, /* include PlaceHolderVars in output list */
    PVC_RECURSE_PLACEHOLDERS  /* recurse into PlaceHolderVar arguments */
} PVCPlaceHolderBehavior;

/*
 * When we evaluate distinct number and biase of exprs, we meet some special
 * exprs that hard to give an accurate estimation
 */
typedef enum {
    PVC_REJECT_SPECIAL_EXPR,  /* skip if special exprs found */
    PVC_INCLUDE_SPECIAL_EXPR, /* include special nodes in output list */
    PVC_RECURSE_SPECIAL_EXPR  /* recurse into special expr nodes */
} PVCSPExprBehavior;

/*
 * Used by replace_node_clause as behavior flag.
 * To mark wheather (1) go into Aggref, (2) copy non-leaf nodes,
 * (3) only replace first target we meet.
 */
typedef enum {
    RNC_NONE = 0x00u,
    RNC_RECURSE_AGGREF = 0x01u,
    RNC_COPY_NON_LEAF_NODES = 0x02u,
    RNC_REPLACE_FIRST_ONLY = 0x04u
} ReplaceNodeClauseBehavior;

extern Relids pull_varnos(Node* node, int level = 0, bool isSkip = false);
extern Relids pull_varnos_of_level(Node *node, int levelsup);
extern void pull_varattnos(Node* node, Index varno, Bitmapset** varattnos);
extern List *pull_vars_of_level(Node *node, int levelsup);
extern bool contain_var_clause(Node* node);
extern bool contain_vars_of_level(Node* node, int levelsup);
extern int locate_var_of_level(Node* node, int levelsup);
extern int locate_var_of_relation(Node* node, int relid, int levelsup);
extern int find_minimum_var_level(Node* node);
extern List* pull_var_clause(Node* node, PVCAggregateBehavior aggbehavior, PVCPlaceHolderBehavior phbehavior,
    PVCSPExprBehavior spbehavior = PVC_RECURSE_SPECIAL_EXPR, bool includeUpperVars = false,
    bool includeUpperAggrefs = false);
extern Node* flatten_join_alias_vars(PlannerInfo* root, Node* node);
extern Node* replace_node_clause_for_equality(Node* clause, List* src_list, Node* dest);
extern Node* replace_node_clause(Node* node, Node* src_list, Node* dest_list, uint32 rncbehavior);
extern bool check_node_clause(Node* clause, Node* node);
extern bool contain_vars_of_level_or_above(Node* node, int levelsup);

extern bool check_param_clause(Node* clause);
extern Bitmapset* collect_param_clause(Node* clause);
extern bool check_param_expr(Node* node);
extern List* check_random_expr(Node* node);

extern List* check_subplan_expr(Node* node, bool recurseSubPlan = false);
extern bool check_varno(Node* qual, int varno, int varlevelsup);
extern List* check_vartype(Node* node);
extern Node *LocateOpExprLeafVar(Node *node);
#endif /* VAR_H */
