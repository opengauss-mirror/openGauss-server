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
    bool isFirstNode = true, bool isCreateView = false, ParseState* parent_pstate = NULL);

extern Query* parse_analyze_varparams(Node* parseTree, const char* sourceText, Oid** paramTypes, int* numParams);


extern Query* parse_sub_analyze(Node* parseTree, ParseState* parentParseState, CommonTableExpr* parentCTE,
    bool locked_from_parent, bool resolve_unknowns);
extern Node* parse_into_claues(Node* parse_tree, IntoClause* intoClause);
extern List* transformInsertRow(ParseState* pstate, List* exprlist, List* stmtcols, List* icolumns, List* attrnos);
extern Query* transformTopLevelStmt(
    ParseState* pstate, Node* parseTree, bool isFirstNode = true, bool isCreateView = false);
extern Query* transformStmt(ParseState* pstate, Node* parseTree, bool isFirstNode = true, bool isCreateView = false);

extern bool analyze_requires_snapshot(Node* parseTree);

extern void CheckSelectLocking(Query* qry);
extern void applyLockingClause(Query* qry, Index rtindex, LockClauseStrength strength, LockWaitPolicy waitPolicy, bool pushedDown,
                               int waitSec);
#ifdef ENABLE_MOT
extern void CheckTablesStorageEngine(Query* qry, StorageEngineType* type);

typedef struct RTEDetectorContext {
    bool isMotTable;
    bool isPageTable;
    List* queryNodes;
    int sublevelsUp;
} RTEDetectorContext;
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

typedef struct ColumnTypeForm {
    Oid originTypOid;
    Oid baseTypOid;
    int32 typmod;
    Oid collid;
    int typlen;
    bool typbyval;
    regproc typinput;
    Oid ioParam;
} ColumnTypeForm;

typedef struct ParseColumnCallbackState {
    ParseState* pstate;
    int location;
    ListCell* attrCell;
    ErrorContextCallback errcontext;
} ParseColumnCallbackState;

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
extern void UpdateParseCheck(ParseState *pstate, Node *qry);
extern void assign_query_ignore_flag(ParseState* pstate, Query* query);
#endif /* !FRONTEND_PARSER */

extern bool getOperatorPlusFlag();


#endif /* ANALYZE_H */
