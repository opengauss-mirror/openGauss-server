/* -------------------------------------------------------------------------
 *
 * analyze.h
 *		parse analysis for optimizable statements
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/parser/analyze.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef ANALYZE_H
#define ANALYZE_H

#ifndef FRONTEND_PARSER
#include "parser/parse_node.h"
#include "utils/plancache.h"

extern const char* const ANALYZE_TEMP_TABLE_PREFIX;

/* Hook for plugins to get control at end of parse analysis */
typedef void (*post_parse_analyze_hook_type)(ParseState* pstate, Query* query);

extern THR_LOCAL PGDLLIMPORT post_parse_analyze_hook_type post_parse_analyze_hook;

extern Query* parse_analyze(Node* parseTree, const char* sourceText, Oid* paramTypes, int numParams,
    bool isFirstNode = true, bool isCreateView = false);

extern Query* parse_analyze_varparams(Node* parseTree, const char* sourceText, Oid** paramTypes, int* numParams);


extern Query* parse_sub_analyze(Node* parseTree, ParseState* parentParseState, CommonTableExpr* parentCTE,
    bool locked_from_parent, bool resolve_unknowns);

extern List* transformInsertRow(ParseState* pstate, List* exprlist, List* stmtcols, List* icolumns, List* attrnos);
extern Query* transformTopLevelStmt(
    ParseState* pstate, Node* parseTree, bool isFirstNode = true, bool isCreateView = false);
extern Query* transformStmt(ParseState* pstate, Node* parseTree, bool isFirstNode = true, bool isCreateView = false);

extern bool analyze_requires_snapshot(Node* parseTree);

extern void CheckSelectLocking(Query* qry);
extern void applyLockingClause(Query* qry, Index rtindex, LockClauseStrength strength, bool noWait, bool pushedDown,
                               int waitSec);
#ifdef ENABLE_MOT
extern void CheckTablesStorageEngine(Query* qry, StorageEngineType* type);
extern bool CheckMotIndexedColumnUpdate(Query* qry);

typedef struct RTEDetectorContext {
    bool isMotTable;
    bool isPageTable;
    List* queryNodes;
    int sublevelsUp;
} RTEDetectorContext;

typedef struct UpdateDetectorContext {
    bool isIndexedColumnUpdate;
    List* queryNodes;
    int sublevelsUp;
} UpdateDetectorContext;
#endif

/* Record the rel name and corresponding columan name info */
typedef struct RelColumnInfo {
    char* relname;
    List* colnames;
} RelColumnInfo;

typedef struct PlusJoinRTEItem {
    RangeTblEntry* rte; /* The RTE that the column referennce */
    bool hasplus;       /* Does the Expr contains ''(+)" ? */
} PlusJoinRTEItem;

typedef struct OperatorPlusProcessContext {
    List* jointerms; /* List of Jointerm */
    bool contain_plus_outerjoin;
    ParseState* ps; /* ParseState of Current level */
    Node** whereClause;
    bool in_orclause;
    bool contain_joinExpr;
} OperatorPlusProcessContext;

typedef Query* (*transformSelectStmtHook)(ParseState* pstate, SelectStmt* stmt, bool isFirstNode, bool isCreateView);

typedef struct AnalyzerRoutine {
    transformSelectStmtHook transSelect;
} AnalyzerRoutine;

typedef Query* (*transformStmtFunc)(ParseState* pstate, Node* parseTree, bool isFirstNode, bool isCreateView);

extern void transformOperatorPlus(ParseState* pstate, Node** whereClause);
extern bool IsColumnRefPlusOuterJoin(const ColumnRef* cf);
extern PlusJoinRTEItem* makePlusJoinRTEItem(RangeTblEntry* rte, bool hasplus);
extern void setIgnorePlusFlag(ParseState* pstate, bool ignore);
extern void resetOperatorPlusFlag();

extern void fixResTargetNameWithTableNameRef(Relation rd, RangeVar* rel, ResTarget* res);
extern void fixResTargetListWithTableNameRef(Relation rd, RangeVar* rel, List* clause_list);
#endif /* !FRONTEND_PARSER */

extern bool getOperatorPlusFlag();


#endif /* ANALYZE_H */
