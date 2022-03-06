/*
 * psql - the openGauss interactive terminal
 *
 * Copyright (c) 2000-2012, PostgreSQL Global Development Group
 *
 * src/bin/psql/command.h
 */
#ifndef COMMAND_H
#define COMMAND_H

#include "print.h"
#include "psqlscan.h"

typedef enum _backslashResult {
    PSQL_CMD_UNKNOWN = 0, /* not done parsing yet (internal only) */
    PSQL_CMD_SEND,        /* query complete; send off */
    PSQL_CMD_SKIP_LINE,   /* keep building query */
    PSQL_CMD_TERMINATE,   /* quit program */
    PSQL_CMD_NEWEDIT,     /* query buffer was changed (e.g., via \e) */
    PSQL_CMD_ERROR        /* the execution of the backslash command
                           * resulted in an error */
} backslashResult;

extern backslashResult HandleSlashCmds(PsqlScanState scan_state, PQExpBuffer query_buf);

extern int process_file(char* filename, bool single_txn, bool use_relative_path);

extern bool do_pset(const char* param, const char* value, printQueryOpt* popt, bool quiet);

extern void connection_warnings(bool in_startup);

extern void client_server_version_check(PGconn* conn);

extern void SyncVariables(void);

extern void UnsyncVariables(void);

/* max length of password */
#define MAX_PASSWORD_LENGTH 999

#endif /* COMMAND_H */

