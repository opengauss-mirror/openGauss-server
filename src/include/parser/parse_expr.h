/* -------------------------------------------------------------------------
 *
 * parse_expr.h
 *	  handle expressions in parser
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/parser/parse_expr.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef PARSE_EXPR_H
#define PARSE_EXPR_H

#include "parser/parse_node.h"
#include "utils/plpgsql.h"

extern Node* transformExpr(ParseState* pstate, Node* expr);
extern Expr* make_distinct_op(ParseState* pstate, List* opname, Node* ltree, Node* rtree, int location);
extern Oid getMultiFuncInfo(char* fun_expr, PLpgSQL_expr* expr);
extern void lockSeqForNextvalFunc(Node* node);
#endif /* PARSE_EXPR_H */
