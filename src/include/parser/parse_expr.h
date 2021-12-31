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

/* start with ... connect by related output routines */
extern bool IsPseudoReturnColumn(const char *colname);
extern char *makeStartWithDummayColname(char *alias, char *column);
extern Node* transformColumnRef(ParseState* pstate, ColumnRef* cref);
extern void AddStartWithTargetRelInfo(ParseState* pstate, Node* relNode,
                                         RangeTblEntry* rte, RangeTblRef *rtr);
extern void AdaptSWSelectStmt(ParseState *pstate, SelectStmt *stmt);
extern bool IsQuerySWCBRewrite(Query *query);
extern bool IsSWCBRewriteRTE(RangeTblEntry *rte);

#endif /* PARSE_EXPR_H */
