/* -------------------------------------------------------------------------
 *
 * execdesc.h
 *	  plan and query descriptor accessor macros used by the executor
 *	  and related modules.
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/executor/execdesc.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef EXECDESC_H
#define EXECDESC_H

#include "nodes/execnodes.h"
#include "tcop/dest.h"

#ifdef ENABLE_MOT
// forward declaration
namespace JitExec
{
    struct MotJitContext;
}
#endif

/* ----------------
 *		query descriptor:
 *
 *	a QueryDesc encapsulates everything that the executor
 *	needs to execute the query.
 *
 *	For the convenience of SQL-language functions, we also support QueryDescs
 *	containing utility statements; these must not be passed to the executor
 *	however.
 * ---------------------
 */
typedef struct QueryDesc {
    /* These fields are provided by CreateQueryDesc */
    CmdType operation;            /* CMD_SELECT, CMD_UPDATE, etc. */
    PlannedStmt* plannedstmt;     /* planner's output, or null if utility */
    Node* utilitystmt;            /* utility statement, or null */
    const char* sourceText;       /* source text of the query */
    Snapshot snapshot;            /* snapshot to use for query */
    Snapshot crosscheck_snapshot; /* crosscheck for RI update/delete */
    DestReceiver* dest;           /* the destination for tuple output */
    ParamListInfo params;         /* param values being passed in */
    int instrument_options;       /* OR of InstrumentOption flags */

    /* These fields are set by ExecutorStart */
    TupleDesc tupDesc;    /* descriptor for result tuples */
    EState* estate;       /* executor's query-wide state */
    PlanState* planstate; /* tree of per-plan-node state */

    /* This is always set NULL by the core system, but plugins can change it */
    struct Instrumentation* totaltime; /* total time spent in ExecutorRun */
    bool executed;                     /* if the query already executed */
    bool for_simplify_func;            /* if the query is for simplify function, skip reporting query plan */
#ifdef ENABLE_MOT
    JitExec::MotJitContext* mot_jit_context;   /* MOT JIT context required for executing LLVM jitted code */
#endif
} QueryDesc;

/* in pquery.c */
#ifdef ENABLE_MOT
extern QueryDesc* CreateQueryDesc(PlannedStmt* plannedstmt, const char* sourceText, Snapshot snapshot,
    Snapshot crosscheck_snapshot, DestReceiver* dest, ParamListInfo params, int instrument_options,
    JitExec::MotJitContext* motJitContext = nullptr);
#else
extern QueryDesc* CreateQueryDesc(PlannedStmt* plannedstmt, const char* sourceText, Snapshot snapshot,
    Snapshot crosscheck_snapshot, DestReceiver* dest, ParamListInfo params, int instrument_options);
#endif

extern QueryDesc* CreateUtilityQueryDesc(
    Node* utilitystmt, const char* sourceText, Snapshot snapshot, DestReceiver* dest, ParamListInfo params);

extern void FreeQueryDesc(QueryDesc* qdesc);

#endif /* EXECDESC_H  */
