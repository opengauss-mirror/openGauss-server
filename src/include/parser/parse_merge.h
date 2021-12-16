/* ---------------------------------------------------------------------------------------
 * 
 * parse_merge.h
 *        handle merge-stmt in parser
 * 
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *        src/include/parser/parse_merge.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef PARSE_MERGE_H
#define PARSE_MERGE_H

#include "parser/parse_node.h"
extern Query* transformMergeStmt(ParseState* pstate, MergeStmt* stmt);
extern List* expandTargetTL(List* te_list, Query* parsetree);
extern List* expandActionTL(List* te_list, Query* parsetree);
extern List* expandQualTL(List* te_list, Query* parsetree);
extern bool check_unique_constraint(List*& index_list);
extern void setExtraUpdatedCols(RangeTblEntry* target_rte, TupleDesc tupdesc);
#endif
