/* -------------------------------------------------------------------------
 *
 * prepare.h
 *	  PREPARE, EXECUTE and DEALLOCATE commands, and prepared-stmt storage
 *
 *
 * Copyright (c) 2002-2012, PostgreSQL Global Development Group
 *
 * src/include/commands/prepare.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef PREPARE_H
#define PREPARE_H

#include "commands/explain.h"
#include "utils/globalplancache.h"
#include "utils/plancache.h"

#ifdef PGXC
typedef struct DatanodeStatement {
    /* dynahash.c requires key to be first field */
    char stmt_name[NAMEDATALEN];
    int current_nodes_number; /* number of nodes where statement is active */
    int max_nodes_number;     /* maximum number of nodes where statement is active */
    int* dns_node_indices;    /* node ids where statement is active */
} DatanodeStatement;
#endif

/* Utility statements PREPARE, EXECUTE, DEALLOCATE, EXPLAIN EXECUTE */
extern void PrepareQuery(PrepareStmt* stmt, const char* queryString);
extern void ExecuteQuery(ExecuteStmt* stmt, IntoClause* intoClause, const char* queryString, ParamListInfo params,
    DestReceiver* dest, char* completionTag);
extern void DeallocateQuery(DeallocateStmt* stmt);
extern void ExplainExecuteQuery(
    ExecuteStmt* execstmt, IntoClause* into, ExplainState* es, const char* queryString, ParamListInfo params);

/* Low-level access to stored prepared statements */
extern void StorePreparedStatementCNGPC(const char *stmt_name, CachedPlanSource *plansource,
                                        bool from_sql, bool is_share);
extern void StorePreparedStatement(const char* stmt_name, CachedPlanSource* plansource, bool from_sql);
extern PreparedStatement* FetchPreparedStatement(const char* stmt_name, bool throwError, bool need_valid);
extern void DropPreparedStatement(const char* stmt_name, bool showError);
extern TupleDesc FetchPreparedStatementResultDesc(PreparedStatement *stmt);
extern List* FetchPreparedStatementTargetList(PreparedStatement *stmt);

extern void DropAllPreparedStatements(void);
extern void HandlePreparedStatementsForReload(void);
extern void HandlePreparedStatementsForRetry(void);
extern bool HaveActiveCoordinatorPreparedStatement(const char* stmt_name);

#ifdef PGXC
extern DatanodeStatement* FetchDatanodeStatement(const char* stmt_name, bool throwError);
extern bool ActivateDatanodeStatementOnNode(const char* stmt_name, int nodeIdx);
extern void DeActiveAllDataNodeStatements(void);
extern bool HaveActiveDatanodeStatements(void);
extern void DropDatanodeStatement(const char* stmt_name);
extern int SetRemoteStatementName(Plan* plan, const char* stmt_name, int num_params, Oid* param_types, int n,
                                        bool isBuildingCustomPlan, bool is_plan_shared = false);
extern char* get_datanode_statement_name(const char* stmt_name, int n);
#endif
extern bool needRecompileQuery(ExecuteStmt* stmt);
extern void RePrepareQuery(ExecuteStmt* stmt);
extern bool checkRecompileCondition(CachedPlanSource* plansource);

extern void GetRemoteQuery(PlannedStmt* stmt, const char* queryString);
extern void GetRemoteQueryWalker(Plan* plan, void* context, const char* queryString);
extern void PlanTreeWalker(
    Plan* plan, void (*walker)(Plan*, void*, const char*), void* context, const char* queryString);

extern DatanodeStatement* light_set_datanode_queries(const char* stmt_name);
extern void GetPreparedStatements(HTAB* htbl, Tuplestorestate* tupStore, TupleDesc tupDesc, uint64 sessionId, char* userName);
extern void GetThreadPreparedStatements(Tuplestorestate* tupStore, TupleDesc tupDesc, uint64 sessionId);
extern bool quickPlanner(List* querytree_list, Node* parsetree, const char*queryString, CommandDest dest, char* completionTag);
#ifdef ENABLE_MOT
extern void TryMotJitCodegenQuery(const char* queryString, CachedPlanSource* psrc, Query* query);
#endif
#endif /* PREPARE_H */
