/* -------------------------------------------------------------------------
 *
 * parse_agg.h
 *	  handle aggregates and window functions in parser
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/parser/parse_agg.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef PARSE_AGG_H
#define PARSE_AGG_H

#include "parser/parse_node.h"

extern void transformAggregateCall(ParseState* pstate, Aggref* agg, List* args, List* aggorder, bool agg_distinct);
extern void transformWindowFuncCall(ParseState* pstate, WindowFunc* wfunc, WindowDef* windef);

extern void parseCheckAggregates(ParseState* pstate, Query* qry);
extern void parseCheckWindowFuncs(ParseState* pstate, Query* qry);
extern Node* transformGroupingFunc(ParseState* pstate, GroupingFunc* g);

extern List* expand_grouping_sets(List* groupingSets, int limit);

extern List* extract_rollup_sets(List* groupingSets);

extern List* reorder_grouping_sets(List* groupingSets, List* sortclause);

extern List* preprocess_groupclause(PlannerInfo* root, List* force);

extern void build_aggregate_fnexprs(Oid* agg_input_types, int agg_num_inputs, Oid agg_state_type, Oid agg_result_type,
    Oid agg_input_collation, Oid transfn_oid, Oid finalfn_oid, Expr** transfnexpr, Expr** finalfnexpr);

extern bool check_windowagg_can_shuffle(List* partitionClause, List* targetList);

extern void build_trans_aggregate_fnexprs(int agg_num_inputs, int agg_num_direct_inputs, bool agg_ordered_set,
    bool agg_variadic, Oid agg_state_type, Oid* agg_input_types, Oid agg_result_type, Oid agg_input_collation,
    Oid transfn_oid, Oid finalfn_oid, Expr** transfnexpr, Expr** finalfnexpr);

extern int get_aggregate_argtypes(Aggref* aggref, Oid* inputTypes, int func_max_args);

extern Oid resolve_aggregate_transtype(Oid aggfuncid, Oid aggtranstype, Oid* inputTypes, int numArguments);

#endif /* PARSE_AGG_H */
