/*--------------------------------------------------------------------------
 *
 * autonomous.h
 *        Run SQL commands using a background worker.
 *
 * Copyright (C) 2014, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *        src/include/tcop/autonomous.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef AUTONOMOUS_H
#define AUTONOMOUS_H

#include "access/tupdesc.h"
#include "nodes/pg_list.h"

struct AutonomousSession;
typedef struct AutonomousSession AutonomousSession;

struct AutonomousPreparedStatement;
typedef struct AutonomousPreparedStatement AutonomousPreparedStatement;

struct autonomous_session_fixed_data;
typedef struct autonomous_session_fixed_data autonomous_session_fixed_data;

typedef struct AutonomousResult {
	TupleDesc	tupdesc;
	List	   *tuples;
	char	   *command;
} AutonomousResult;

AutonomousSession *AutonomousSessionStart(void);
void AutonomousSessionEnd(AutonomousSession *session);
AutonomousResult *AutonomousSessionExecute(AutonomousSession *session, const char *sql);
AutonomousPreparedStatement *AutonomousSessionPrepare(AutonomousSession *session, const char *sql, int16 nargs,
                                                      Oid argtypes[], const char *argnames[]);
AutonomousResult *AutonomousSessionExecutePrepared(AutonomousPreparedStatement *stmt, int16 nargs, Datum *values, bool *nulls);
extern void autonomous_worker_main(Datum main_arg);

#endif /* AUTONOMOUS_H */
