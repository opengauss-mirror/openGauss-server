/* -------------------------------------------------------------------------
 *
 * var.cpp
 *	  Var node manipulation routines
 *
 * Note: for most purposes, PlaceHolderVar is considered a Var too,
 * even if its contained expression is variable-free.  Also, CurrentOfExpr
 * is treated as a Var for purposes of determining whether an expression
 * contains variables.
 *
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/optimizer/util/var.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/sysattr.h"
#include "access/transam.h"
#include "catalog/pg_proc.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/prep.h"
#include "optimizer/var.h"
#include "parser/parsetree.h"
#include "parser/parse_oper.h"
#include "rewrite/rewriteManip.h"
#include "miscadmin.h"
#include "rusagestub.h"

typedef struct {
    Relids varnos;
    int sublevels_up;
    bool isSkipSublink;
} pull_varnos_context;

typedef struct {
    Bitmapset* varattnos;
    Index varno;
} pull_varattnos_context;

typedef struct {
    List *vars;
    int sublevels_up;
} pull_vars_context;

typedef struct {
    int var_location;
    int sublevels_up;
} locate_var_of_level_context;

typedef struct {
    int min_varlevel;
    int sublevels_up;
} find_minimum_var_level_context;

typedef struct {
    List* varlist;
    PVCAggregateBehavior aggbehavior;
    PVCPlaceHolderBehavior phbehavior;
    PVCSPExprBehavior spbehavior;
    bool includeUpperVars;
    bool includeUpperAggrefs;
} pull_var_clause_context;

typedef struct {
    Node* src;                 /* source node */
    Node* dest;                /* destination node */
    bool recurse_aggref;       /* mark wheather go into Aggref */
    bool isCopy;               /* mark if copy non-leaf nodes */
    bool replace_first_only;   /* mark if replace the first target only or all targets */
    uint32 replace_count;      /* counter of how many targets have been replaced (only available on single 
                                * node replace, not for list) 
                                */
} replace_node_clause_context;

typedef struct {
    PlannerInfo* root;
    int sublevels_up;
    bool possible_sublink; /* could aliases include a SubLink? */
    bool inserted_sublink; /* have we inserted a SubLink? */
} flatten_join_alias_vars_context;

typedef struct pull_node {
    List* nodeList;
    bool recurseSubPlan;
} pull_node;

typedef struct var_info {
    Index varno;       /* Var no. */
    Index varlevelsup; /* Var level up. */
    bool result;
} var_info;

static bool pull_varnos_walker(Node* node, pull_varnos_context* context);
static bool pull_varattnos_walker(Node* node, pull_varattnos_context* context);
static bool pull_vars_walker(Node *node, pull_vars_context *context);
static bool contain_var_clause_walker(Node* node, void* context);
static bool contain_vars_of_level_walker(Node* node, int* sublevels_up);
static bool locate_var_of_level_walker(Node* node, locate_var_of_level_context* context);
static bool find_minimum_var_level_walker(Node* node, find_minimum_var_level_context* context);
static bool pull_var_clause_walker(Node* node, pull_var_clause_context* context);
static Node* flatten_join_alias_vars_mutator(Node* node, flatten_join_alias_vars_context* context);
static Relids alias_relid_set(PlannerInfo* root, Relids relids);
static Node* replace_node_clause_mutator(Node* node, replace_node_clause_context* context);
static bool check_node_clause_walker(Node* clause, Node* node);
static bool check_param_clause_walker(Node* clause, void* context);
static bool check_param_expr_walker(Node* node, void* context);
static bool check_random_expr_walker(Node* node, pull_node* context);
static bool check_subplan_expr_walker(Node* node, pull_node* context);
static Node* get_special_node_from_expr(Node* node, List** varlist);
static bool contain_vars_of_level_or_above_walker(Node* node, int* sublevels_up);
static bool collect_param_clause_walker(Node* node, void* context);

/*
 * pull_varnos
 *		Create a set of all the distinct varnos present in a parsetree.
 *		Only varnos that reference level-zero rtable entries are considered.
 *
 * NOTE: this is used on not-yet-planned expressions.  It may therefore find
 * bare SubLinks, and if so it needs to recurse into them to look for uplevel
 * references to the desired rtable level!	But when we find a completed
 * SubPlan, we only need to look at the parameters passed to the subplan.
 */
Relids pull_varnos(Node* node, int level, bool isSkip)
{
    pull_varnos_context context;

    context.varnos = NULL;
    context.sublevels_up = level;
    context.isSkipSublink = isSkip;
    /*
     * Must be prepared to start with a Query or a bare expression tree; if
     * it's a Query, we don't want to increment sublevels_up.
     */
    (void)query_or_expression_tree_walker(node, (bool (*)())pull_varnos_walker, (void*)&context, 0);

    return context.varnos;
}

/*
 * pull_varnos_of_level
 *     Create a set of all the distinct varnos present in a parsetree.
 *     Only Vars of the specified level are considered.
 */
Relids
pull_varnos_of_level(Node *node, int levelsup)
{
   pull_varnos_context context;

   context.varnos = NULL;
   context.sublevels_up = levelsup;

   /*
    * Must be prepared to start with a Query or a bare expression tree; if
    * it's a Query, we don't want to increment sublevels_up.
    */
   query_or_expression_tree_walker(node,
                                   (bool (*)())pull_varnos_walker,
                                   (void *) &context,
                                   0);

    return context.varnos;
}

static bool pull_varnos_walker(Node* node, pull_varnos_context* context)
{
    if (node == NULL)
        return false;
    if (IsA(node, Var)) {
        Var* var = (Var*)node;

        if (var->varlevelsup == (unsigned int)context->sublevels_up)
            context->varnos = bms_add_member(context->varnos, var->varno);
        return false;
    }
    if (IsA(node, CurrentOfExpr)) {
        CurrentOfExpr* cexpr = (CurrentOfExpr*)node;

        if (context->sublevels_up == 0)
            context->varnos = bms_add_member(context->varnos, cexpr->cvarno);
        return false;
    }
    if (IsA(node, PlaceHolderVar)) {
        /*
         * Normally, we can just take the varnos in the contained expression.
         * But if it is variable-free, use the PHV's syntactic relids.
         */
        PlaceHolderVar* phv = (PlaceHolderVar*)node;
        pull_varnos_context subcontext;

        subcontext.varnos = NULL;
        subcontext.sublevels_up = context->sublevels_up;
        (void)pull_varnos_walker((Node*)phv->phexpr, &subcontext);

        if (bms_is_empty(subcontext.varnos) && phv->phlevelsup == (unsigned int)context->sublevels_up)
            context->varnos = bms_add_members(context->varnos, phv->phrels);
        else
            context->varnos = bms_join(context->varnos, subcontext.varnos);
        return false;
    }
    if (IsA(node, SubLink) && context->isSkipSublink) {
        return false;
    }
    if (IsA(node, Query)) {
        /* Recurse into RTE subquery or not-yet-planned sublink subquery */
        bool result = false;

        context->sublevels_up++;
        result = query_tree_walker((Query*)node, (bool (*)())pull_varnos_walker, (void*)context, 0);
        context->sublevels_up--;
        return result;
    }
    return expression_tree_walker(node, (bool (*)())pull_varnos_walker, (void*)context);
}

/*
 * pull_varattnos
 *		Find all the distinct attribute numbers present in an expression tree,
 *		and add them to the initial contents of *varattnos.
 *		Only Vars of the given varno and rtable level zero are considered.
 *
 * Attribute numbers are offset by FirstLowInvalidHeapAttributeNumber so that
 * we can include system attributes (e.g., OID) in the bitmap representation.
 *
 * Currently, this does not support unplanned subqueries; that is not needed
 * for current uses.  It will handle already-planned SubPlan nodes, though,
 * looking into only the "testexpr" and the "args" list.  (The subplan cannot
 * contain any other references to Vars of the current level.)
 */
void pull_varattnos(Node* node, Index varno, Bitmapset** varattnos)
{
    pull_varattnos_context context;

    context.varattnos = *varattnos;
    context.varno = varno;

    (void)pull_varattnos_walker(node, &context);

    *varattnos = context.varattnos;
}

static bool pull_varattnos_walker(Node* node, pull_varattnos_context* context)
{
    if (node == NULL)
        return false;
    if (IsA(node, Var)) {
        Var* var = (Var*)node;

        if (var->varno == context->varno && var->varlevelsup == 0)
            context->varattnos = bms_add_member(context->varattnos, var->varattno - FirstLowInvalidHeapAttributeNumber);
        return false;
    }

    /* Should not find an unplanned subquery */
    AssertEreport(!IsA(node, Query), MOD_OPT, "");

    return expression_tree_walker(node, (bool (*)())pull_varattnos_walker, (void*)context);
}

/*
 * pull_vars_of_level
 *     Create a list of all Vars referencing the specified query level
 *     in the given parsetree.
 *
 * This is used on unplanned parsetrees, so we don't expect to see any
 * PlaceHolderVars.
 *
 * Caution: the Vars are not copied, only linked into the list.
 */
List *
pull_vars_of_level(Node *node, int levelsup)
{
   pull_vars_context context;

   context.vars = NIL;
   context.sublevels_up = levelsup;

   /*
    * Must be prepared to start with a Query or a bare expression tree; if
    * it's a Query, we don't want to increment sublevels_up.
    */
   query_or_expression_tree_walker(node,
                                   (bool (*)())pull_vars_walker,
                                   (void *) &context,
                                   0);

   return context.vars;
}

static bool
pull_vars_walker(Node *node, pull_vars_context *context)
{
   if (node == NULL)
       return false;
   if (IsA(node, Var))
   {
       Var        *var = (Var *) node;

       if (var->varlevelsup == (Index)context->sublevels_up)
           context->vars = lappend(context->vars, var);
       return false;
   }
   if (IsA(node, PlaceHolderVar))
   {
       PlaceHolderVar *phv = (PlaceHolderVar *) node;
   
       if (phv->phlevelsup == (Index)context->sublevels_up)
           context->vars = lappend(context->vars, phv);
       /* we don't want to look into the contained expression */
       return false;
   }
   if (IsA(node, Query))
   {
       /* Recurse into RTE subquery or not-yet-planned sublink subquery */
       bool result = false;

       context->sublevels_up++;
       result = query_tree_walker((Query *) node, (bool (*)())pull_vars_walker,
                                  (void *) context, 0);
       context->sublevels_up--;
       return result;
   }
   return expression_tree_walker(node, (bool (*)())pull_vars_walker,
                                 (void *) context);
}

/*
 * contain_var_clause
 *	  Recursively scan a clause to discover whether it contains any Var nodes
 *	  (of the current query level).
 *
 *	  Returns true if any varnode found.
 *
 * Does not examine subqueries, therefore must only be used after reduction
 * of sublinks to subplans!
 */
bool contain_var_clause(Node* node)
{
    return contain_var_clause_walker(node, NULL);
}

static bool contain_var_clause_walker(Node* node, void* context)
{
    if (node == NULL)
        return false;
    if (IsA(node, Var)) {
        if (((Var*)node)->varlevelsup == 0)
            return true; /* abort the tree traversal and return true */
        return false;
    }
    if (IsA(node, CurrentOfExpr))
        return true;
    if (IsA(node, PlaceHolderVar)) {
        if (((PlaceHolderVar*)node)->phlevelsup == 0)
            return true; /* abort the tree traversal and return true */
                         /* else fall through to check the contained expr */
    }
    return expression_tree_walker(node, (bool (*)())contain_var_clause_walker, context);
}

/*
 * contain_vars_of_level
 *	  Recursively scan a clause to discover whether it contains any Var nodes
 *	  of the specified query level.
 *
 *	  Returns true if any such Var found.
 *
 * Will recurse into sublinks.	Also, may be invoked directly on a Query.
 */
bool contain_vars_of_level(Node* node, int levelsup)
{
    int sublevels_up = levelsup;

    return query_or_expression_tree_walker(node, (bool (*)())contain_vars_of_level_walker, (void*)&sublevels_up, 0);
}

static bool contain_vars_of_level_walker(Node* node, int* sublevels_up)
{
    if (node == NULL)
        return false;
    if (IsA(node, Var)) {
        if (((Var*)node)->varlevelsup == (Index)*sublevels_up)
            return true; /* abort tree traversal and return true */
        return false;
    }
    if (IsA(node, CurrentOfExpr)) {
        if (*sublevels_up == 0) {
            return true;
        }
        return false;
    }
    if (IsA(node, PlaceHolderVar)) {
        if (((PlaceHolderVar*)node)->phlevelsup == (unsigned int)*sublevels_up)
            return true; /* abort the tree traversal and return true */
                         /* else fall through to check the contained expr */
    }
    if (IsA(node, Query)) {
        /* Recurse into subselects */
        bool result = false;

        (*sublevels_up)++;
        result = query_tree_walker((Query*)node, (bool (*)())contain_vars_of_level_walker, (void*)sublevels_up, 0);
        (*sublevels_up)--;
        return result;
    }
    return expression_tree_walker(node, (bool (*)())contain_vars_of_level_walker, (void*)sublevels_up);
}

/*
 * contain_vars_of_level_or_above
 *	  Recursively scan a clause to discover whether it contains any Var nodes
 *	  of the specified query level or level above.
 *
 *	  Returns true if any such Var found.
 *
 */
bool contain_vars_of_level_or_above(Node* node, int levelsup)
{
    int sublevels_up = levelsup;

    return query_or_expression_tree_walker(
        node, (bool (*)())contain_vars_of_level_or_above_walker, (void*)&sublevels_up, 0);
}

static bool contain_vars_of_level_or_above_walker(Node* node, int* sublevels_up)
{
    if (node == NULL) {
        return false;
    }
    if (IsA(node, Var)) {
        if (((Var*)node)->varlevelsup >= (Index)*sublevels_up) {
            return true; /* abort tree traversal and return true */
        }
        return false;
    }
    if (IsA(node, CurrentOfExpr)) {
        if (*sublevels_up == 0) {
            return true;
        }
        return false;
    }
    if (IsA(node, PlaceHolderVar)) {
        if (((PlaceHolderVar*)node)->phlevelsup >= (unsigned int)*sublevels_up) {
            return true; /* abort the tree traversal and return true */
        }
        /* else fall through to check the contained expr */
    }
    if (IsA(node, Query)) {
        /* Recurse into subselects */
        bool result = false;

        result =
            query_tree_walker((Query*)node, (bool (*)())contain_vars_of_level_or_above_walker, (void*)sublevels_up, 0);
        return result;
    }
    return expression_tree_walker(node, (bool (*)())contain_vars_of_level_or_above_walker, (void*)sublevels_up);
}

/*
 * locate_var_of_level
 *	  Find the parse location of any Var of the specified query level.
 *
 * Returns -1 if no such Var is in the querytree, or if they all have
 * unknown parse location.	(The former case is probably caller error,
 * but we don't bother to distinguish it from the latter case.)
 *
 * Will recurse into sublinks.	Also, may be invoked directly on a Query.
 *
 * Note: it might seem appropriate to merge this functionality into
 * contain_vars_of_level, but that would complicate that function's API.
 * Currently, the only uses of this function are for error reporting,
 * and so shaving cycles probably isn't very important.
 */
int locate_var_of_level(Node* node, int levelsup)
{
    locate_var_of_level_context context;

    context.var_location = -1; /* in case we find nothing */
    context.sublevels_up = levelsup;

    (void)query_or_expression_tree_walker(node, (bool (*)())locate_var_of_level_walker, (void*)&context, 0);

    return context.var_location;
}

static bool locate_var_of_level_walker(Node* node, locate_var_of_level_context* context)
{
    if (node == NULL)
        return false;
    if (IsA(node, Var)) {
        Var* var = (Var*)node;

        if (var->varlevelsup == (unsigned int)context->sublevels_up && var->location >= 0) {
            context->var_location = var->location;
            return true; /* abort tree traversal and return true */
        }
        return false;
    }
    if (IsA(node, CurrentOfExpr)) {
        /* since CurrentOfExpr doesn't carry location, nothing we can do */
        return false;
    }
    /* No extra code needed for PlaceHolderVar; just look in contained expr */
    if (IsA(node, Query)) {
        /* Recurse into subselects */
        bool result = false;

        context->sublevels_up++;
        result = query_tree_walker((Query*)node, (bool (*)())locate_var_of_level_walker, (void*)context, 0);
        context->sublevels_up--;
        return result;
    }
    return expression_tree_walker(node, (bool (*)())locate_var_of_level_walker, (void*)context);
}

/*
 * find_minimum_var_level
 *	  Recursively scan a clause to find the lowest variable level it
 *	  contains --- for example, zero is returned if there are any local
 *	  variables, one if there are no local variables but there are
 *	  one-level-up outer references, etc.  Subqueries are scanned to see
 *	  if they possess relevant outer references.  (But any local variables
 *	  within subqueries are not relevant.)
 *
 *	  -1 is returned if the clause has no variables at all.
 *
 * Will recurse into sublinks.	Also, may be invoked directly on a Query.
 */
int find_minimum_var_level(Node* node)
{
    find_minimum_var_level_context context;

    context.min_varlevel = -1; /* signifies nothing found yet */
    context.sublevels_up = 0;

    (void)query_or_expression_tree_walker(node, (bool (*)())find_minimum_var_level_walker, (void*)&context, 0);

    return context.min_varlevel;
}

static bool find_minimum_var_level_walker(Node* node, find_minimum_var_level_context* context)
{
    if (node == NULL)
        return false;
    if (IsA(node, Var)) {
        int varlevelsup = ((Var*)node)->varlevelsup;

        /* convert levelsup to frame of reference of original query */
        varlevelsup -= context->sublevels_up;
        /* ignore local vars of subqueries */
        if (varlevelsup >= 0) {
            if (context->min_varlevel < 0 || context->min_varlevel > varlevelsup) {
                context->min_varlevel = varlevelsup;

                /*
                 * As soon as we find a local variable, we can abort the tree
                 * traversal, since min_varlevel is then certainly 0.
                 */
                if (varlevelsup == 0) {
                    return true;
                }
            }
        }
    }
    if (IsA(node, CurrentOfExpr)) {
        int varlevelsup = 0;

        /* convert levelsup to frame of reference of original query */
        varlevelsup -= context->sublevels_up;
        /* ignore local vars of subqueries */
        if (varlevelsup >= 0) {
            if (context->min_varlevel < 0 || context->min_varlevel > varlevelsup) {
                context->min_varlevel = varlevelsup;

                /*
                 * As soon as we find a local variable, we can abort the tree
                 * traversal, since min_varlevel is then certainly 0.
                 */
                if (varlevelsup == 0) {
                    return true;
                }
            }
        }
    }

    /*
     * An Aggref must be treated like a Var of its level.  Normally we'd get
     * the same result from looking at the Vars in the aggregate's argument,
     * but this fails in the case of a Var-less aggregate call (COUNT(*)).
     */
    if (IsA(node, Aggref)) {
        int agglevelsup = ((Aggref*)node)->agglevelsup;

        /* convert levelsup to frame of reference of original query */
        agglevelsup -= context->sublevels_up;
        /* ignore local aggs of subqueries */
        if (agglevelsup >= 0) {
            if (context->min_varlevel < 0 || context->min_varlevel > agglevelsup) {
                context->min_varlevel = agglevelsup;

                /*
                 * As soon as we find a local aggregate, we can abort the tree
                 * traversal, since min_varlevel is then certainly 0.
                 */
                if (agglevelsup == 0) {
                    return true;
                }
            }
        }
    }
    /* Likewise, make sure PlaceHolderVar is treated correctly */
    if (IsA(node, PlaceHolderVar)) {
        int phlevelsup = ((PlaceHolderVar*)node)->phlevelsup;

        /* convert levelsup to frame of reference of original query */
        phlevelsup -= context->sublevels_up;
        /* ignore local vars of subqueries */
        if (phlevelsup >= 0) {
            if (context->min_varlevel < 0 || context->min_varlevel > phlevelsup) {
                context->min_varlevel = phlevelsup;

                /*
                 * As soon as we find a local variable, we can abort the tree
                 * traversal, since min_varlevel is then certainly 0.
                 */
                if (phlevelsup == 0) {
                    return true;
                }
            }
        }
    }
    if (IsA(node, Query)) {
        /* Recurse into subselects */
        bool result = false;

        context->sublevels_up++;
        result = query_tree_walker((Query*)node, (bool (*)())find_minimum_var_level_walker, (void*)context, 0);
        context->sublevels_up--;
        return result;
    }
    return expression_tree_walker(node, (bool (*)())find_minimum_var_level_walker, (void*)context);
}

/*
 * pull_var_clause
 *	  Recursively pulls all Var nodes from an expression clause.
 *
 *	  Aggrefs are handled according to 'aggbehavior':
 *		PVC_REJECT_AGGREGATES		throw error if Aggref found
 *		PVC_INCLUDE_AGGREGATES		include Aggrefs in output list
 *		PVC_RECURSE_AGGREGATES		recurse into Aggref arguments
 *	  Vars within an Aggref's expression are included only in the last case.
 *
 *	  PlaceHolderVars are handled according to 'phbehavior':
 *		PVC_REJECT_PLACEHOLDERS		throw error if PlaceHolderVar found
 *		PVC_INCLUDE_PLACEHOLDERS	include PlaceHolderVars in output list
 *		PVC_RECURSE_PLACEHOLDERS	recurse into PlaceHolderVar arguments
 *	  Vars within a PHV's expression are included only in the last case.
 *
 *	  CurrentOfExpr nodes are ignored in all cases.
 *
 *	  Upper-level vars (with varlevelsup > 0) should not be seen here,
 *	  likewise for upper-level Aggrefs and PlaceHolderVars.
 *
 *	  Returns list of nodes found.	Note the nodes themselves are not
 *	  copied, only referenced.
 *
 * Does not examine subqueries, therefore must only be used after reduction
 * of sublinks to subplans!
 */
List* pull_var_clause(Node* node, PVCAggregateBehavior aggbehavior, PVCPlaceHolderBehavior phbehavior,
    PVCSPExprBehavior spbehavior, bool includeUpperVars, bool includeUpperAggrefs)
{
    pull_var_clause_context context;

    context.varlist = NIL;
    context.aggbehavior = aggbehavior;
    context.phbehavior = phbehavior;
    context.spbehavior = spbehavior;
    context.includeUpperVars = includeUpperVars;
    context.includeUpperAggrefs = includeUpperAggrefs;

    (void)pull_var_clause_walker(node, &context);
    return context.varlist;
}

static bool pull_var_clause_walker(Node* node, pull_var_clause_context* context)
{
    if (node == NULL)
        return false;

    /* We differentiate special exprs and add them to return list */
    if (context->spbehavior != PVC_RECURSE_SPECIAL_EXPR) {
        List* varlist = NIL;
        Node* sp_node = get_special_node_from_expr(node, &varlist);
        if (sp_node != NULL) {
            if (context->spbehavior == PVC_INCLUDE_SPECIAL_EXPR) {
                EstSPNode* estspnode = makeNode(EstSPNode);
                estspnode->expr = sp_node;
                estspnode->varlist = varlist;
                context->varlist = lappend(context->varlist, estspnode);
            } else
                list_free_ext(varlist);
            return false;
        }
    }

    if (IsA(node, Var)) {
        if (((Var*)node)->varlevelsup != 0) {
            if (!context->includeUpperVars) {
                ereport(ERROR,
                    (errmodule(MOD_OPT),
                        errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                        errmsg("Upper-level Var found where not expected")));
            }
        }
        context->varlist = lappend(context->varlist, node);
        return false;
    } else if (IsA(node, Aggref)) {
        if (((Aggref*)node)->agglevelsup != 0) {
            if (!context->includeUpperAggrefs) {
                ereport(ERROR,
                    (errmodule(MOD_OPT),
                        errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                        errmsg("Upper-level Aggref found where not expected")));
            }
        }
        switch (context->aggbehavior) {
            case PVC_REJECT_AGGREGATES:
                ereport(ERROR,
                    (errmodule(MOD_OPT),
                        errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                        errmsg("Aggref found where not expected")));
                break;
            case PVC_INCLUDE_AGGREGATES:
            case PVC_INCLUDE_AGGREGATES_OR_WINAGGS:
                context->varlist = lappend(context->varlist, node);
                /* we do NOT descend into the contained expression */
                return false;
            case PVC_RECURSE_AGGREGATES:
                /* ignore the aggregate, look at its argument instead */
                break;
        }
    } else if (IsA(node, GroupingFunc)) {
        if (((GroupingFunc*)node)->agglevelsup != 0)
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                    errmsg("Upper-level GROUPING found where not expected")));
        switch (context->aggbehavior) {
            case PVC_REJECT_AGGREGATES:
                ereport(ERROR,
                    (errmodule(MOD_OPT),
                        errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                        errmsg("GROUPING found where not expected")));
                break;
            case PVC_INCLUDE_AGGREGATES:
            case PVC_INCLUDE_AGGREGATES_OR_WINAGGS:
                context->varlist = lappend(context->varlist, node);
                /* we do NOT descend into the contained expression */
                return false;
            case PVC_RECURSE_AGGREGATES:

                /*
                 * we do NOT descend into the contained expression, even if
                 * the caller asked for it, because we never actually evaluate
                 * it - the result is driven entirely off the associated GROUP
                 * BY clause, so we never need to extract the actual Vars
                 * here.
                 */
                return false;
        }
    } else if (IsA(node, GroupingId)) {
        switch (context->aggbehavior) {
            case PVC_REJECT_AGGREGATES: {
                ereport(ERROR,
                    (errmodule(MOD_OPT),
                        errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                        errmsg("GROUPING found where not expected")));
            } break;
            case PVC_INCLUDE_AGGREGATES:
            case PVC_INCLUDE_AGGREGATES_OR_WINAGGS:
                context->varlist = lappend(context->varlist, node);
                return false;
            case PVC_RECURSE_AGGREGATES:
                return false;
        }
    } else if (IsA(node, WindowFunc)) {
        if (context->aggbehavior == PVC_INCLUDE_AGGREGATES_OR_WINAGGS) {
            context->varlist = lappend(context->varlist, node);
            return false;
        }
    } else if (IsA(node, PlaceHolderVar)) {
        if (((PlaceHolderVar*)node)->phlevelsup != 0)
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                    (errmsg("Upper-level PlaceHolderVar found where not expected"))));
        switch (context->phbehavior) {
            case PVC_REJECT_PLACEHOLDERS:
                ereport(ERROR,
                    (errmodule(MOD_OPT),
                        errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                        (errmsg("PlaceHolderVar found where not expected"))));
                break;
            case PVC_INCLUDE_PLACEHOLDERS:
                context->varlist = lappend(context->varlist, node);
                /* we do NOT descend into the contained expression */
                return false;
            case PVC_RECURSE_PLACEHOLDERS:
                /* ignore the placeholder, look at its argument instead */
                break;
        }
    }

    return expression_tree_walker(node, (bool (*)())pull_var_clause_walker, (void*)context);
}

/*
 * get_special_node_from_expr
 *
 * Now we can not accurate estimate distinct values and biase of some exprs,
 * which we called special expr. This function is used to identify such special
 * exprs, and we can give them a rough estimation later
 */
static Node* get_special_node_from_expr(Node* node, List** varlist)
{
    bool is_special = false;

    switch (nodeTag(node)) {
        case T_CaseExpr:
        case T_NullIfExpr:
        case T_Aggref:
        case T_GroupingFunc:
        case T_GroupingId:
            is_special = true;
            break;
        case T_FuncExpr:
            if (is_func_distinct_unshippable(((FuncExpr*)node)->funcid) ||
                ((FuncExpr*)node)->funcid >= FirstNormalObjectId || exprType(node) == BOOLOID)
                is_special = true;
            break;
        case T_CoalesceExpr: {
            List* args = ((CoalesceExpr*)node)->args;
            if (list_length(args) > 1 && IsA(llast(((CoalesceExpr*)node)->args), Const))
                is_special = true;
        } break;
        default:
            break;
    }

    if (is_special) {
        List* subvars =
            pull_var_clause(node, PVC_RECURSE_AGGREGATES, PVC_RECURSE_PLACEHOLDERS, PVC_RECURSE_SPECIAL_EXPR);
        if (subvars != NIL || IsA(node, Aggref) || IsA(node, GroupingFunc) || IsA(node, GroupingId)) {
            *varlist = list_concat_unique(NIL, subvars);
            list_free_ext(subvars);
            return node;
        }
    }

    return NULL;
}

/*
 * Generate new quality with two methods
 * 1. Replace the expresssion in  restrict-clause with the node 'dest'  if they have the
 *     same type and typmode
 * 2. If restrict-clause is an operator invocation, replace one side of the expression node
 *     with  node 'dest' if it is a member of src_list(that means it is a member expression
 *     of an EquivalenceClass)
 */
Node* replace_node_clause_for_equality(Node* clause, List* src_list, Node* dest)
{
    replace_node_clause_context context;
    ListCell* cell = NULL;
    Node* src = NULL;
    Expr* leftExpr = NULL;
    Expr* rightExpr = NULL;
    OpExpr* opExpr = NULL;
    List* opName = NULL;
    Oid opno = InvalidOid;
    bool defined = false;
    Expr* item1 = NULL;
    Expr* item2 = NULL;
    Node* newClause = NULL;

    foreach (cell, src_list) {
        Node* node = (Node*)lfirst(cell);

        if (exprType(dest) == exprType(node) && exprTypmod(dest) == exprTypmod(node)) {
            src = node;
            break;
        }
    }

    /* just replace 'src' in 'clause' with 'dest' */
    if (src != NULL) {
        context.src = src;
        context.dest = dest;
        context.recurse_aggref = true;
        context.isCopy = true;
        context.replace_first_only = false;
        context.replace_count = 0;

        return replace_node_clause_mutator(clause, &context);
    }

    /*
     * we just deal with clause like this
     * 1. it is an operator invocation
     * 2. one side is Const and the other side is a expression contains Var
     */
    if (!IsA(clause, OpExpr))
        return NULL;

    opExpr = (OpExpr*)clause;
    if (2 != list_length(opExpr->args))
        return NULL;

    if (opExpr->opretset)
        return NULL;

    leftExpr = (Expr*)get_leftop((Expr*)clause);
    rightExpr = (Expr*)get_rightop((Expr*)clause);
    if (list_member(src_list, leftExpr) && IsA(rightExpr, Const)) {
        item1 = (Expr*)dest;
        item2 = rightExpr;
    } else if (list_member(src_list, rightExpr) && IsA(leftExpr, Const)) {
        item1 = leftExpr;
        item2 = (Expr*)dest;
    } else
        return NULL;

    /* get operator names */
    opName = get_operator_name(opExpr->opno, exprType((Node*)leftExpr), exprType((Node*)rightExpr));

    /* looks up operator for new clause */
    opno = OperatorLookup(opName, exprType((Node*)item1), exprType((Node*)item2), &defined);
    list_free_deep(opName);

    if (!OidIsValid(opno))
        return NULL;

    newClause = (Node*)make_opclause(
        opno, BOOLOID, false, (Expr*)copyObject(item1), (Expr*)copyObject(item2), InvalidOid, opExpr->inputcollid);

    return newClause;
}

/*
 * @Description: replace src_list with dest_list.
 * @in clause - primary clause include src_list.
 * @in src_list - source node, it can be list mean need replace all node in list.
 * @in dest_list - target node, alse can be list.
 * @in rncbehavior - behavior flags, see more in ReplaceNodeClauseBehavior
 */
Node* replace_node_clause(Node* clause, Node* src_list, Node* dest_list, uint32 rncbehavior)
{
    if (src_list == NULL || dest_list == NULL) {
        return clause;
    }
    replace_node_clause_context context;

    context.src = (Node*)src_list;
    context.dest = (Node*)dest_list;
    context.recurse_aggref = (RNC_RECURSE_AGGREF & rncbehavior) > 0x00u;
    context.isCopy = (RNC_COPY_NON_LEAF_NODES & rncbehavior) > 0x00u;
    context.replace_first_only = (RNC_REPLACE_FIRST_ONLY & rncbehavior) > 0x00u;
    context.replace_count = 0;

    return replace_node_clause_mutator(clause, &context);
}

/*
 * @Description: mutator for replace_node_clause
 * @in node - primary clause include context.src.
 * @in context - mutator context, see more in replace_node_clause_context.
 */
static Node* replace_node_clause_mutator(Node* node, replace_node_clause_context* context)
{
    if (node == NULL) {
        return NULL;
    }

    if (IsA(context->src, List) && IsA(context->dest, List)) {
        ListCell *lc = NULL;
        ListCell *lc2 = NULL;
        Assert (list_length((List *) context->src) == list_length((List *) context->dest));
        forboth(lc, (List *) context->src, lc2, (List *) context->dest) {
            if (equal(node, lfirst(lc))) {
                if (context->isCopy) {
                    return (Node *) copyObject(lfirst(lc2));
                } else {
                    return (Node *) lfirst(lc2);
                }
            }
        }
    } else if (equal(node, context->src)) {
        /* If replace_first_only is set, and we have already got one, return directly */
        if (context->replace_first_only && (context->replace_count >= 1)) {
            return node;
        } else {
            ++context->replace_count;
            if (context->isCopy) {
                return (Node *) copyObject(context->dest);
            } else {
                return context->dest;
            }
        }
    }

    if (!context->recurse_aggref && IsA(node, Aggref)) {
        if (context->isCopy) {
            return (Node *) copyObject(node);
        } else {
            return node;
        }
    }

    return expression_tree_mutator(
        node, (Node* (*)(Node*, void*)) replace_node_clause_mutator, (void*)context, context->isCopy);
}

bool check_node_clause(Node* clause, Node* node)
{
    return check_node_clause_walker(clause, node);
}

static bool check_node_clause_walker(Node* node, Node* src)
{
    if (node == NULL)
        return false;

    if (equal(node, src))
        return true;

    return expression_tree_walker(node, (bool (*)())check_node_clause_walker, src);
}

bool check_param_clause(Node* clause)
{
    return check_param_clause_walker(clause, NULL);
}

static bool check_param_clause_walker(Node* node, void* context)
{
    if (node == NULL)
        return false;

    if (IsA(node, SubPlan) || (IsA(node, Param) && ((Param*)node)->paramkind != PARAM_EXTERN))
        return true;

    if (IsA(node, RestrictInfo)) {
        node = (Node*)((RestrictInfo*)node)->clause;
    }

    return expression_tree_walker(node, (bool (*)())check_param_clause_walker, (void*)context);
}

/* collect param ids */
typedef struct collect_param_context {
    Bitmapset *param_ids;
}collect_param_context;

Bitmapset* collect_param_clause(Node* clause)
{
    collect_param_context context;
    context.param_ids = NULL;
    collect_param_clause_walker(clause, &context);

    return context.param_ids;
}

static bool collect_param_clause_walker(Node* node, void* context)
{
    if (NULL == node)
        return false;

    collect_param_context *ctx = (collect_param_context *)context;

    if ((IsA(node, Param) && ((Param*)node)->paramkind != PARAM_EXTERN)) {
        Param *param = (Param *)node;
        ctx->param_ids = bms_add_member(ctx->param_ids, param->paramid);
        return true;
    }

    if (IsA(node, RestrictInfo)) {
        node = (Node*)((RestrictInfo*)node)->clause;
    }

    return expression_tree_walker(node, (bool (*)())collect_param_clause_walker, (void*)context);
}


/*
 *@Description: Check if the expr contain param
 *@in node: Search node
 *@return: true if the expr contain param
 */
bool check_param_expr(Node* node)
{
    return check_param_expr_walker(node, NULL);
}

static bool check_param_expr_walker(Node* node, void* context)
{
    if (node == NULL)
        return false;

    if (IsA(node, SubPlan) || IsA(node, Param))
        return true;

    return expression_tree_walker(node, (bool (*)())check_param_expr_walker, (void*)context);
}

/*
 * @Description: Check if RANDOM expr exist
 * @in node: Search node
 * @return: true if RANDOM expr exist in the node tree.
 */
List* check_random_expr(Node* node)
{
    pull_node context = {NULL, false};

    context.nodeList = NULL;
    context.recurseSubPlan = false;

    expression_tree_walker(node, (bool (*)())check_random_expr_walker, (void*)&context);
    return context.nodeList;
}

static bool check_random_expr_walker(Node* node, pull_node* context)
{
    if (node == NULL) {
        return false;
    }

    if (IsA(node, FuncExpr) &&
        (((FuncExpr*)node)->funcid == RANDOMFUNCOID || ((FuncExpr*)node)->funcid == GSENCRYPTAES128FUNCOID)) {
        context->nodeList = list_append_unique(context->nodeList, node);
        return false;
    }

    return expression_tree_walker(node, (bool (*)())check_random_expr_walker, (void*)context);
}

/*
 * @Description: Get param or subPlan from node.
 * @in node: Search node.
 * @in recurseSubPlan: If recurse subPlan.
 * @return: reture list include found param or param and subPlan else return NULL.
 */
List* check_subplan_expr(Node* node, bool recurseSubPlan)
{
    pull_node context = {NULL, false};

    context.nodeList = NULL;
    context.recurseSubPlan = recurseSubPlan;

    (void)query_or_expression_tree_walker(node, (bool (*)())check_subplan_expr_walker, (void*)&context, 0);

    return context.nodeList;
}

/*
 * @Description: Search param and subPlan into list in node.
 * @in node: Search node.
 * @out src: Keep param and subPlan list.
 */
static bool check_subplan_expr_walker(Node* node, pull_node* context)
{
    if (node == NULL)
        return false;

    if (IsA(node, Param) && ((Param*)node)->paramkind == PARAM_EXEC) {
        context->nodeList = list_append_unique(context->nodeList, node);
        return false;
    } else if (IsA(node, SubPlan) && !context->recurseSubPlan) {
        context->nodeList = list_append_unique(context->nodeList, node);
        return false;
    }

    return expression_tree_walker(node, (bool (*)())check_subplan_expr_walker, (void*)context);
}

/*
 * flatten_join_alias_vars
 *	  Replace Vars that reference JOIN outputs with references to the original
 *	  relation variables instead.  This allows quals involving such vars to be
 *	  pushed down.	Whole-row Vars that reference JOIN relations are expanded
 *	  into RowExpr constructs that name the individual output Vars.  This
 *	  is necessary since we will not scan the JOIN as a base relation, which
 *	  is the only way that the executor can directly handle whole-row Vars.
 *
 * This also adjusts relid sets found in some expression node types to
 * substitute the contained base rels for any join relid.
 *
 * If a JOIN contains sub-selects that have been flattened, its join alias
 * entries might now be arbitrary expressions, not just Vars.  This affects
 * this function in one important way: we might find ourselves inserting
 * SubLink expressions into subqueries, and we must make sure that their
 * Query.hasSubLinks fields get set to TRUE if so.	If there are any
 * SubLinks in the join alias lists, the outer Query should already have
 * hasSubLinks = TRUE, so this is only relevant to un-flattened subqueries.
 *
 * NOTE: this is used on not-yet-planned expressions.  We do not expect it
 * to be applied directly to a Query node.
 */
Node* flatten_join_alias_vars(PlannerInfo* root, Node* node)
{
    flatten_join_alias_vars_context context;

    context.root = root;
    context.sublevels_up = 0;
    /* flag whether join aliases could possibly contain SubLinks */
    context.possible_sublink = root->parse->hasSubLinks;
    /* if hasSubLinks is already true, no need to work hard */
    context.inserted_sublink = root->parse->hasSubLinks;

    return flatten_join_alias_vars_mutator(node, &context);
}

static Node* flatten_join_alias_vars_mutator(Node* node, flatten_join_alias_vars_context* context)
{
    if (node == NULL)
        return NULL;
    if (IsA(node, Var)) {
        Var* var = (Var*)node;
        RangeTblEntry* rte = NULL;
        Node* newvar = NULL;

        /* No change unless Var belongs to a JOIN of the target level */
        if (var->varlevelsup != (unsigned int)context->sublevels_up)
            return node; /* no need to copy, really */
        rte = rt_fetch(var->varno, context->root->parse->rtable);
        if (rte->rtekind != RTE_JOIN)
            return node;
        if (var->varattno == InvalidAttrNumber) {
            /* Must expand whole-row reference */
            RowExpr* rowexpr = NULL;
            List* fields = NIL;
            List* colnames = NIL;
            AttrNumber attnum;
            ListCell* lv = NULL;
            ListCell* ln = NULL;

            attnum = 0;
            AssertEreport(list_length(rte->joinaliasvars) == list_length(rte->eref->colnames), MOD_OPT, "");
            forboth(lv, rte->joinaliasvars, ln, rte->eref->colnames)
            {
                newvar = (Node*)lfirst(lv);
                attnum++;
                /* Ignore dropped columns */
                if (IsA(newvar, Const))
                    continue;
                newvar = (Node*)copyObject(newvar);

                /*
                 * If we are expanding an alias carried down from an upper
                 * query, must adjust its varlevelsup fields.
                 */
                if (context->sublevels_up != 0)
                    IncrementVarSublevelsUp(newvar, context->sublevels_up, 0);
                /* Preserve original Var's location, if possible */
                if (IsA(newvar, Var))
                    ((Var*)newvar)->location = var->location;
                /* Recurse in case join input is itself a join */
                /* (also takes care of setting inserted_sublink if needed) */
                newvar = flatten_join_alias_vars_mutator(newvar, context);
                fields = lappend(fields, newvar);
                /* We need the names of non-dropped columns, too */
                colnames = lappend(colnames, copyObject((Node*)lfirst(ln)));
            }
            rowexpr = makeNode(RowExpr);
            rowexpr->args = fields;
            rowexpr->row_typeid = var->vartype;
            rowexpr->row_format = COERCE_IMPLICIT_CAST;
            rowexpr->colnames = colnames;
            rowexpr->location = var->location;

            return (Node*)rowexpr;
        }

        /* Expand join alias reference */
        AssertEreport(var->varattno > 0, MOD_OPT, "");
        newvar = (Node*)list_nth(rte->joinaliasvars, var->varattno - 1);
        newvar = (Node*)copyObject(newvar);

        /*
         * If we are expanding an alias carried down from an upper query, must
         * adjust its varlevelsup fields.
         */
        if (context->sublevels_up != 0)
            IncrementVarSublevelsUp(newvar, context->sublevels_up, 0);

        /* Preserve original Var's location, if possible */
        if (IsA(newvar, Var))
            ((Var*)newvar)->location = var->location;

        /* Recurse in case join input is itself a join */
        newvar = flatten_join_alias_vars_mutator(newvar, context);

        /* Detect if we are adding a sublink to query */
        if (context->possible_sublink && !context->inserted_sublink)
            context->inserted_sublink = checkExprHasSubLink(newvar);

        return newvar;
    }
    if (IsA(node, PlaceHolderVar)) {
        /* Copy the PlaceHolderVar node with correct mutation of subnodes */
        PlaceHolderVar* phv = NULL;

        phv = (PlaceHolderVar*)expression_tree_mutator(
            node, (Node* (*)(Node*, void*)) flatten_join_alias_vars_mutator, (void*)context);
        /* now fix PlaceHolderVar's relid sets */
        if (phv->phlevelsup == (unsigned int)context->sublevels_up) {
            phv->phrels = alias_relid_set(context->root, phv->phrels);
        }
        return (Node*)phv;
    }

    if (IsA(node, Query)) {
        /* Recurse into RTE subquery or not-yet-planned sublink subquery */
        Query* newnode = NULL;
        bool save_inserted_sublink = false;

        context->sublevels_up++;
        save_inserted_sublink = context->inserted_sublink;
        context->inserted_sublink = ((Query*)node)->hasSubLinks;
        newnode = query_tree_mutator((Query*)node,
            (Node* (*)(Node*, void*)) flatten_join_alias_vars_mutator,
            (void*)context,
            QTW_IGNORE_JOINALIASES);
        newnode->hasSubLinks = (unsigned int)newnode->hasSubLinks | (unsigned int)context->inserted_sublink;
        context->inserted_sublink = save_inserted_sublink;
        context->sublevels_up--;
        return (Node*)newnode;
    }
    /* Already-planned tree not supported
     * But if this query is copied when doing full join rewritting, this is OK.
     */
    if (!context->root->parse->is_from_full_join_rewrite)
        AssertEreport(!IsA(node, SubPlan), MOD_OPT, "");
    /* Shouldn't need to handle these planner auxiliary nodes here */
    AssertEreport(!IsA(node, SpecialJoinInfo), MOD_OPT, "");
    Assert(!IsA(node, LateralJoinInfo));
    AssertEreport(!IsA(node, PlaceHolderInfo), MOD_OPT, "");
    AssertEreport(!IsA(node, MinMaxAggInfo), MOD_OPT, "");

    return expression_tree_mutator(node, (Node* (*)(Node*, void*)) flatten_join_alias_vars_mutator, (void*)context);
}

/*
 * alias_relid_set: in a set of RT indexes, replace joins by their
 * underlying base relids
 */
static Relids alias_relid_set(PlannerInfo* root, Relids relids)
{
    Relids result = NULL;
    Relids tmprelids;
    int rtindex;

    tmprelids = bms_copy(relids);
    while ((rtindex = bms_first_member(tmprelids)) >= 0) {
        RangeTblEntry* rte = rt_fetch(rtindex, root->parse->rtable);

        if (rte->rtekind == RTE_JOIN)
            result = bms_join(result, get_relids_for_join(root, rtindex));
        else
            result = bms_add_member(result, rtindex);
    }
    bms_free_ext(tmprelids);
    return result;
}

/*
 * @Descarption: Search node and compare varno.
 * @in node: Current node.
 * @in varInfo: Var_info struct.
 */
static bool check_varno_walker(Node* node, var_info* varInfo)
{
    if (node == NULL) {
        return false;
    } else if (IsA(node, Var)) {
        Var* var = (Var*)node;

        if (var->varno == varInfo->varno && var->varlevelsup == varInfo->varlevelsup) {
            return false;
        } else {
            varInfo->result = false;
            return true;
        }
    } else if (IsA(node, SubLink)) {
        varInfo->result = false;
        return true;
    }

    return expression_tree_walker(node, (bool (*)())check_varno_walker, varInfo);
}

/*
 * @Description: Check this node if include only include one varno.
 * @in qual: Checked node.
 * @in varno: Var no.
 */
bool check_varno(Node* qual, int varno, int varlevelsup)
{
    var_info varInfo = {0, 0, false};

    varInfo.varno = varno;
    varInfo.varlevelsup = varlevelsup;
    varInfo.result = true;

    (void)check_varno_walker(qual, &varInfo);

    return varInfo.result;
}

/*
 * @Descarption: Search node and check vartype > FirstNormalObjectId.
 * @in node: Current node.
 * @in varInfo: Var_info struct.
 */
static bool check_vartype_walker(Node* node, var_info* varInfo)
{
    if (node == NULL) {
        return false;
    } else if (IsA(node, Var)) {
        Var* var = (Var*)node;

        if (var->vartype > FirstNormalObjectId) {
            varInfo->result = true;
            return true;
        } else {
            return false;
        }
    }

    return expression_tree_walker(node, (bool (*)())check_vartype_walker, varInfo);
}

/*
 * @Description: Check this node if vartype > FirstNormalObjectId.
 * @in qual: Checked node.
 */
List* check_vartype(Node* node)
{
    var_info varInfo = {0, 0, false};
    List* rs = NIL;

    varInfo.result = false;

    (void)check_vartype_walker(node, &varInfo);

    if (varInfo.result)
        rs = lappend(rs, NULL);

    return rs;
}

Node* LocateOpExprLeafVar(Node* node)
{
    if (node == NULL) {
        return NULL;
    }

    if (IsA(node, Var)) {
        return node;
    }

    return expression_tree_mutator(node, (Node* (*)(Node*, void*)) LocateOpExprLeafVar, NULL);
}