/* -------------------------------------------------------------------------
 *
 * poolutils.h
 *
 *		Utilities for Postgres-XC Pooler
 *
 * Portions Copyright (c) 1996-2011, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * src/include/pgxc/poolutils.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef POOLUTILS_H
#define POOLUTILS_H

#include "nodes/parsenodes.h"

#define TIMEOUT_CLEAN_LOOP 15 /* Wait 15s for all the transactions to shutdown */

/* Error codes for connection cleaning */
#define CLEAN_CONNECTION_COMPLETED 0
#define CLEAN_CONNECTION_NOT_COMPLETED 1
#define CLEAN_CONNECTION_TX_REMAIN 2
#define CLEAN_CONNECTION_EOF -1

/* Results for pooler connection info check */
#define POOL_CHECK_SUCCESS 0
#define POOL_CHECK_FAILED 1

void CleanConnection(CleanConnStmt* stmt);
void DropDBCleanConnection(const char* dbname);
void DropRoleCleanConnection(const char* username);

/* Handle pooler connection reload */
void processPoolerReload(void);

#define ResetSessionExecutorInfo(is_force)                                      \
    do {                                                                        \
        MemoryContext old_context = MemoryContextSwitchTo(                      \
            SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_EXECUTOR));       \
        InitMultinodeExecutor(is_force);                                        \
        PoolManagerReconnect();                                                 \
        MemoryContextSwitchTo(old_context);                                     \
    } while (0)

#endif
