/* -------------------------------------------------------------------------
 *
 * rewriteHandler.h
 *		External interface to query rewriter.
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/rewrite/rewriteHandler.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef REWRITEHANDLER_H
#define REWRITEHANDLER_H

#include "utils/relcache.h"
#include "nodes/parsenodes.h"

extern List* QueryRewrite(Query* parsetree);
extern void AcquireRewriteLocks(Query* parsetree, bool forUpdatePushedDown);
extern Node* build_column_default(Relation rel, int attrno, bool isInsertCmd = false, bool needOnUpdate = false);
extern List* pull_qual_vars(Node* node, int varno = 0, int flags = 0, bool nonRepeat = false);
extern void rewriteTargetListMerge(Query* parsetree, Index result_relation, List* range_table);
extern List *query_rewrite_multiset_stmt(Query* parse_tree);
extern List *query_rewrite_set_stmt(Query* parse_tree);
extern Query* get_view_query(Relation view);
extern const char* view_query_is_auto_updatable(Query* viewquery, bool check_cols);
extern int relation_is_updatable(Oid reloid, bool include_triggers, Bitmapset* include_cols);
extern bool view_has_instead_trigger(Relation view, CmdType event);

#ifdef PGXC
extern List* QueryRewriteCTAS(Query* parsetree);
extern List* QueryRewriteRefresh(Query *parsetree);
#endif
extern Node* QueryRewriteNonConstant(Node *node);
extern List* QueryRewriteSelectIntoVarList(Node *node, int res_len);
extern Const* processResToConst(char* value, Oid atttypid, Oid collid);

#endif /* REWRITEHANDLER_H */
