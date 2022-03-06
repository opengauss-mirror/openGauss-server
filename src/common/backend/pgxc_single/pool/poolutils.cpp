/* -------------------------------------------------------------------------
 *
 * poolutils.c
 *
 * Utilities for Postgres-XC pooler
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * IDENTIFICATION
 *    $$
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"
#include "miscadmin.h"
#include "libpq/pqsignal.h"
#include "libpq/libpq-int.h"

#include "access/gtm.h"
#include "access/xact.h"
#include "commands/dbcommands.h"
#include "commands/prepare.h"
#include "funcapi.h"
#include "gssignal/gs_signal.h"
#include "nodes/nodes.h"
#include "pgxc/locator.h"
#include "pgxc/nodemgr.h"
#include "pgxc/pgxc.h"
#include "pgxc/pgxcnode.h"
#include "pgxc/poolmgr.h"
#include "pgxc/poolutils.h"
#include "storage/procarray.h"
#include "threadpool/threadpool.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "utils/elog.h"

/*
 * pgxc_pool_check
 *
 * Check if Pooler information in catalog is consistent
 * with information cached.
 */
Datum pgxc_pool_check(PG_FUNCTION_ARGS)
{
    if (!superuser() && !(isOperatoradmin(GetUserId()) && u_sess->attr.attr_security.operation_mode))
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
            (errmsg("must be system admin or operator admin in operation mode to manage pooler"))));

    /* A Datanode has no pooler active, so do not bother about that */
    if (IS_PGXC_DATANODE)
        PG_RETURN_BOOL(true);

    /* Simply check with pooler */
    PG_RETURN_BOOL(PoolManagerCheckConnectionInfo());
}

Datum pgxc_disaster_read_set(PG_FUNCTION_ARGS)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    PG_RETURN_BOOL(false);
}

Datum pgxc_disaster_read_init(PG_FUNCTION_ARGS)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    PG_RETURN_BOOL(false);
}

Datum pgxc_disaster_read_clear(PG_FUNCTION_ARGS)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    PG_RETURN_BOOL(false);
}

Datum pgxc_disaster_read_status(PG_FUNCTION_ARGS)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    PG_RETURN_NULL();
}

/*
 * pgxc_pool_reload
 *
 * Reload data cached in pooler and reload node connection
 * information in all the server sessions. This aborts all
 * the existing transactions on this node and reinitializes pooler.
 * First a lock is taken on Pooler to keep consistency of node information
 * being updated. If connection information cached is already consistent
 * in pooler, reload is not executed.
 * Reload itself is made in 2 phases:
 * 1) Update database pools with new connection information based on catalog
 *    pgxc_node. Remote node pools are changed as follows:
 *	  - cluster nodes dropped in new cluster configuration are deleted and all
 *      their remote connections are dropped.
 *    - cluster nodes whose port or host value is modified are dropped the same
 *      way, as connection information has changed.
 *    - cluster nodes whose port or host has not changed are kept as is, but
 *      reorganized respecting the new cluster configuration.
 *    - new cluster nodes are added.
 * 2) Reload information in all the sessions of the local node.
 *    All the sessions in server are signaled to reconnect to pooler to get
 *    newest connection information and update connection information related
 *    to remote nodes. This results in losing prepared and temporary objects
 *    in all the sessions of server. All the existing transactions are aborted
 *    and a WARNING message is sent back to client.
 *    Session that invocated the reload does the same process, but no WARNING
 *    message is sent back to client.
 */
Datum pgxc_pool_reload(PG_FUNCTION_ARGS)
{
    MemoryContext old_context;

    if (!superuser() && !(isOperatoradmin(GetUserId()) && u_sess->attr.attr_security.operation_mode))
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
            (errmsg("must be system admin or operator admin in operation mode to manage pooler"))));

    if (IsTransactionBlock())
        ereport(ERROR,
            (errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
                errmsg("pgxc_pool_reload cannot run inside a transaction block")));

    /* A Datanode has no pooler active, so do not bother about that */
    if (IS_PGXC_DATANODE)
        PG_RETURN_BOOL(true);

    /* Reload connection information in pooler */
    PoolManagerReloadConnectionInfo();

    /* Be sure it is done consistently */
    if (!PoolManagerCheckConnectionInfo()) {
        PG_RETURN_BOOL(false);
    }

    /* Signal other sessions to reconnect to pooler */
    ReloadConnInfoOnBackends();

    /* Session is being reloaded, handle prepared and temporary objects */
    HandlePreparedStatementsForReload();

    /* Now session information is reset in correct memory context */
    old_context = MemoryContextSwitchTo(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_COMMUNICATION));

    /* Reinitialize session, while old pooler connection is active */
    InitMultinodeExecutor(true);

    /* And reconnect to pool manager */
    PoolManagerReconnect();

    MemoryContextSwitchTo(old_context);

    PG_RETURN_BOOL(true);
}

static bool CheckRoleValid(const char *userName)
{
    if (userName != NULL && !OidIsValid(get_role_oid(userName, false))) {
        ereport(WARNING, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("role \"%s\" does not exist", userName)));
        return false;
    }

    return true;
}

static void CheckDBActivate(const char *userName, const char *dbName, Oid dbOid)
{
    if (dbName != NULL) {
        int otherBackendsNum = 0, preparedXactsNum = 0;

        if (CountOtherDBBackends(dbOid, &otherBackendsNum, &preparedXactsNum)) {
            ereport(ERROR,
                (errcode(ERRCODE_OBJECT_IN_USE),
                    errmsg("database \"%s\" is being accessed by other users", dbName),
                    errdetail_busy_db(otherBackendsNum, preparedXactsNum)));
        }
    } else {
        Oid roleId = get_role_oid(userName, false);
        if (CountUserBackends(roleId))
            ereport(
                ERROR, (errcode(ERRCODE_OBJECT_IN_USE), errmsg("role \"%s\" is being used by other users", userName)));
    }
}

static void SendSignalToProcess(const char *dbName, Oid dbOid, const char *userName, Oid userOid)
{
    if ((dbName == NULL) && (userName == NULL)) {
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("must define Database name or user name")));
    }
    if ((dbOid == InvalidOid) && (userOid == InvalidOid)) {
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("database oid and user oid are all wrong")));
    }

    int loop = 0;
    int totalSessions = 0;
    int countAcviteSessions = 0;
    /* clear inner sessions(Autovacum and WDRXdb). */
    if (dbName != NULL) {
        int otherBackendsNum = 0;
        int preparedXactsNum = 0;

        if (CountOtherDBBackends(dbOid, &otherBackendsNum, &preparedXactsNum)) {
            ereport(LOG, (errmsg("Autovacum and WDRXdb sessions are cleaned successfully")));
        } else {
            ereport(LOG, (errmsg("Autovacum and WDRXdb sessions no need to clean")));
        }
    }

    CHECK_FOR_INTERRUPTS();

    if (ENABLE_THREAD_POOL) {
        /* clear active session in thread pool mode */
        totalSessions = g_threadPoolControler->GetSessionCtrl()->CleanDBSessions(dbOid, userOid);
        do {
            countAcviteSessions = g_threadPoolControler->GetSessionCtrl()->CountDBSessionsNotCleaned(dbOid, userOid);
            loop++;
            pg_usleep(1000000);
        } while ((countAcviteSessions > 0) && (loop < TIMEOUT_CLEAN_LOOP));
        if (loop == TIMEOUT_CLEAN_LOOP) {
            ereport(WARNING, (errcode(ERRCODE_SYNTAX_ERROR),
                errmsg("clean active sessions in thread pool mode not completed, remains %d.", countAcviteSessions)));
        }
        ereport(LOG, (errmsg("clean %d sessions in thread pool mode", totalSessions - countAcviteSessions)));
    }

    /* clear active session in non-thread pool mode or HA connection */
    loop = 0;
    while ((CountSingleNodeActiveBackends(dbOid, userOid) > 0) && (loop < TIMEOUT_CLEAN_LOOP)) {
        CancelSingleNodeBackends(dbOid, userOid, PROCSIG_RECOVERY_CONFLICT_DATABASE, true);
        loop++;
        pg_usleep(1000000);
    }
    if (loop == TIMEOUT_CLEAN_LOOP) {
        ereport(WARNING, (errcode(ERRCODE_SYNTAX_ERROR),
            errmsg("clean active sessions in non-thread pool mode not completed.")));
    }
    return;
}

/*
 * CleanConnection()
 *
 * Utility to clean up openGauss Pooler connections.
 * This utility is launched to all the Coordinators of the cluster
 *
 * Use of CLEAN CONNECTION is limited to a super user.
 * It is advised to clean connections before shutting down a Node or drop a Database.
 *
 * SQL query synopsis is as follows:
 * CLEAN CONNECTION TO
 *		(COORDINATOR num | DATANODE num | ALL {FORCE})
 *		[ FOR DATABASE dbname ]
 *		[ TO USER username ]
 *
 * Connection cleaning can be made on a chosen database called dbname
 * or/and a chosen user.
 * Cleaning is done for all the users of a given database
 * if no user name is specified.
 * Cleaning is done for all the databases for one user
 * if no database name is specified.
 *
 * It is also possible to clean connections of several Coordinators or Datanodes
 * Ex:	CLEAN CONNECTION TO DATANODE dn1,dn2,dn3 FOR DATABASE template1
 *		CLEAN CONNECTION TO COORDINATOR co2,co4,co3 FOR DATABASE template1
 *		CLEAN CONNECTION TO DATANODE dn2,dn5 TO USER postgres
 *		CLEAN CONNECTION TO COORDINATOR co6,co1 FOR DATABASE template1 TO USER postgres
 *
 * Or even to all Coordinators/Datanodes at the same time
 * Ex:	CLEAN CONNECTION TO DATANODE * FOR DATABASE template1
 *		CLEAN CONNECTION TO COORDINATOR * FOR DATABASE template1
 *		CLEAN CONNECTION TO COORDINATOR * TO USER postgres
 *		CLEAN CONNECTION TO COORDINATOR * FOR DATABASE template1 TO USER postgres
 *
 * When FORCE is used, all the transactions using pooler connections are aborted,
 * and pooler connections are cleaned up.
 * Ex:	CLEAN CONNECTION TO ALL FORCE FOR DATABASE template1;
 *		CLEAN CONNECTION TO ALL FORCE TO USER postgres;
 *		CLEAN CONNECTION TO ALL FORCE FOR DATABASE template1 TO USER postgres;
 *
 * FORCE can only be used with TO ALL, as it takes a lock on pooler to stop requests
 * asking for connections, aborts all the connections in the cluster, and cleans up
 * pool connections associated to the given user and/or database.
 */
void CleanConnection(CleanConnStmt* stmt)
{
    ListCell* nodelistItem = NULL;
    char* dbName = stmt->dbname;
    char* userName = stmt->username;
    bool isForce = stmt->is_force;
    bool isCheck = stmt->is_check;
    Oid dbOid = InvalidOid;
    Oid userOid = InvalidOid;

    /* Database name or user name is mandatory */
    if (dbName == NULL && userName == NULL) {
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("must define Database name or user name")));
    }

    if (dbName != NULL) {
        dbOid = get_database_oid(dbName, false);
        /* Permission checks:
         * 1. The database owner has only the permission to clean the specified database links.
         */
        AclResult aclresult = pg_database_aclcheck(dbOid, GetUserId(), ACL_ALTER | ACL_DROP);
        if (aclresult != ACLCHECK_OK && !pg_database_ownercheck(dbOid, GetUserId())) {
            aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_DATABASE, dbName);
        }
    } else {
        /* Permission checks:
         * 2. Only the superuser has the permission to specify a specific user to clean.
         */
        if (!superuser()) {
            ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("must be system admin to clean pool connections")));
        }
    }

    /* Check if role exists */
    if (!CheckRoleValid(userName)) {
        return;
    }
    if (userName != NULL) {
        userOid = get_role_oid(userName, false);
    }
    // CHECK is activated
    // Check if the database/user is being accessed by other users/sessions before drop it.
    if (isCheck) {
        CheckDBActivate(userName, dbName, dbOid);
    }

    /* check Node name is valid or not */
    foreach (nodelistItem, stmt->nodes) {
        char* node_name = strVal(lfirst(nodelistItem));
        Oid nodeoid = get_pgxc_nodeoid(node_name);
        if (!OidIsValid(nodeoid)) {
            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("Node %s: object not defined", node_name)));
        }
    }

    /*
     * FORCE is activated
     * Send a SIGTERM signal to all the processes in thread pool and HA session.
     */
    if (isForce) {
        SendSignalToProcess(dbName, dbOid, userName, userOid);
    }
}

/*
 * DropDBCleanConnection
 *
 * Clean Connection for given database before dropping it
 * FORCE is not used here
 */
void DropDBCleanConnection(const char* dbname)
{
#ifndef ENABLE_MULTIPLE_NODES
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return;
#else
    List* co_list = GetAllCoordNodes();
    List* dn_list = GetAllDataNodes();

    /* Check permissions for this database */
    AclResult aclresult = pg_database_aclcheck(get_database_oid(dbname, true), GetUserId(), ACL_ALTER | ACL_DROP);
    if (aclresult != ACLCHECK_OK && !pg_database_ownercheck(get_database_oid(dbname, true), GetUserId())) {
        aclcheck_error(ACLCHECK_NO_PRIV, ACL_KIND_DATABASE, dbname);
    }

    PoolManagerCleanConnection(dn_list, co_list, dbname, NULL);

    /* Clean up memory */
    if (co_list)
        list_free(co_list);
    if (dn_list)
        list_free(dn_list);
#endif
}

/*
 * DropRoleCleanConnection
 *
 * Clean Connection for given role before dropping it
 */
void DropRoleCleanConnection(const char* username)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return;
}

/*
 * processPoolerReload
 *
 * This is called when PROCSIG_PGXCPOOL_RELOAD is activated.
 * Abort the current transaction if any, then reconnect to pooler.
 * and reinitialize session connection information.
 */
void processPoolerReload(void)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return;
}

// Check if Pooler connection status is normal.
//
Datum pgxc_pool_connection_status(PG_FUNCTION_ARGS)
{
    List* co_list = GetAllCoordNodes();
    List* dn_list = GetAllDataNodes();
    bool status = true;

    if (!superuser() && !(isOperatoradmin(GetUserId()) && u_sess->attr.attr_security.operation_mode))
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
            (errmsg("must be system admin or operator admin in operation mode to manage pooler"))));

    /* cannot exec func in transaction or a write query */
    if (IsTransactionBlock() || TransactionIdIsValid(GetTopTransactionIdIfAny()))
        ereport(ERROR,
            (errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
                (errmsg(
                    "can not execute pgxc_pool_connection_status in a transaction block or a write transaction."))));
    // A Datanode has no pooler active, so do not bother about that.
    //
    if (IS_PGXC_DATANODE)
        PG_RETURN_BOOL(true);

    status = PoolManagerConnectionStatus(dn_list, co_list);

    // Clean up memory.
    //
    if (co_list != NIL)
        list_free(co_list);
    if (dn_list != NIL)
        list_free(dn_list);

    PG_RETURN_BOOL(status);
}

/*
 * Validate pooler connections by comparing the hostis_primary column in pgxc_node
 * with connections info hold in each pooler agent.
 * if co_node_name specified, validate the pooler connections to it.
 *
 * Note: if we do not use co_node_name, we should set co_node_name to ' ', not ''.
 */
Datum pg_pool_validate(PG_FUNCTION_ARGS)
{
    FuncCallContext* funcctx = NULL;
    InvalidBackendEntry* entry = NULL;
    errno_t rc;
    int ret = 0;
    char node_name[NAMEDATALEN];

    rc = memset_s(node_name, NAMEDATALEN, '\0', NAMEDATALEN);
    securec_check(rc, "\0", "\0");

    if (!superuser() && !(isOperatoradmin(GetUserId()) && u_sess->attr.attr_security.operation_mode))
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
            (errmsg("must be system admin or operator admin in operation mode to manage pooler"))));

    if (IsTransactionBlock())
        ereport(ERROR,
            (errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
                errmsg("pg_pool_validate cannot run inside a transaction block")));

    if (IS_PGXC_DATANODE && !IS_SINGLE_NODE)
        PG_RETURN_NULL();

    if (SRF_IS_FIRSTCALL()) {
        MemoryContext oldcontext;
        TupleDesc tupdesc;
        bool clear = PG_GETARG_BOOL(0);
        char* co_node_name = PG_GETARG_CSTRING(1);

        rc = strncpy_s(node_name, NAMEDATALEN, co_node_name, strlen(co_node_name));
        securec_check(rc, "\0", "\0");
        node_name[NAMEDATALEN - 1] = '\0';

        funcctx = SRF_FIRSTCALL_INIT();

        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        /* construct a tuple descriptor for the result row. */
        tupdesc = CreateTemplateTupleDesc(2, false, TAM_HEAP);
        TupleDescInitEntry(tupdesc, (AttrNumber)1, "pid", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)2, "node_name", TEXTOID, -1, 0);
        funcctx->tuple_desc = BlessTupleDesc(tupdesc);

        funcctx->user_fctx = (void*)PoolManagerValidateConnection(clear, node_name, funcctx->max_calls);

        MemoryContextSwitchTo(oldcontext);
    }

    funcctx = SRF_PERCALL_SETUP();
    entry = (InvalidBackendEntry*)funcctx->user_fctx;

    if (funcctx->call_cntr < funcctx->max_calls) {
        Datum values[2];
        bool nulls[2];
        char* namestring = NULL;
        HeapTuple tuple;
        char element[MAXPGPATH] = {0};
        int i = 0;

        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");
        rc = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
        securec_check(rc, "\0", "\0");

        entry += funcctx->call_cntr;

        namestring = (char*)palloc0(entry->total_len);
        ret = snprintf_s(namestring, entry->total_len, MAXPGPATH - 1, "%s", entry->node_name[0]);
        securec_check_ss(ret, "\0", "\0");
        for (i = 1; i < entry->num_nodes; i++) {
            ret = snprintf_s(element, sizeof(element), MAXPGPATH - 1, ",%s", entry->node_name[i]);
            securec_check_ss(ret, "\0", "\0");
            rc = strncat_s(namestring, entry->total_len, element, entry->total_len - strlen(namestring));
            securec_check(rc, "\0", "\0");
        }

        values[0] = Int64GetDatum(entry->tid);
        values[1] = CStringGetTextDatum(namestring);
        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        pfree(namestring);
        namestring = NULL;

        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    } else
        SRF_RETURN_DONE(funcctx);
}

/* Set pooler ping */
Datum pg_pool_ping(PG_FUNCTION_ARGS)
{
    bool mod = PG_GETARG_BOOL(0);

    if (!superuser() && !(isOperatoradmin(GetUserId()) && u_sess->attr.attr_security.operation_mode))
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
            (errmsg("must be system admin or operator admin in operation mode to manage pooler"))));

    if (IsTransactionBlock())
        ereport(ERROR,
            (errcode(ERRCODE_ACTIVE_SQL_TRANSACTION), errmsg("pg_pool_ping cannot run inside a transaction block")));

    /* A Datanode has no pooler active, so do not bother about that */
    if (IS_PGXC_DATANODE)
        PG_RETURN_BOOL(true);

    set_pooler_ping(mod);

    PG_RETURN_BOOL(true);
}

#ifdef ENABLE_MULTIPLE_NODES
/*
 * HandlePoolerReload
 *
 * This is called when PROCSIG_PGXCPOOL_RELOAD is activated.
 * Abort the current transaction if any, then reconnect to pooler.
 * and reinitialize session connection information.
 */
void HandlePoolerReload(void)
{
    MemoryContext old_context;

    /* A Datanode has no pooler active, so do not bother about that */
    if (IS_PGXC_DATANODE)
        return;

    /* Abort existing xact if any */
    AbortCurrentTransaction();

    /* Session is being reloaded, drop prepared and temporary objects */
    DropAllPreparedStatements();

    /* Now session information is reset in correct memory context */
    old_context = MemoryContextSwitchTo(THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_COMMUNICATION));

    /* Need to be able to look into catalogs */
    CurrentResourceOwner = ResourceOwnerCreate(NULL, "ForPoolerReload",
        THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_COMMUNICATION));

    /* Reinitialize session, while old pooler connection is active */
    InitMultinodeExecutor(true);

    /* And reconnect to pool manager */
    PoolManagerReconnect();

    /* Send a message back to client regarding session being reloaded */
    ereport(WARNING,
        (errcode(ERRCODE_OPERATOR_INTERVENTION),
            errmsg("session has been reloaded due to a cluster configuration modification"),
            errdetail("Temporary and prepared objects hold by session have been"
                      " dropped and current transaction has been aborted.")));

    /* Release everything */
    ResourceOwnerRelease(CurrentResourceOwner, RESOURCE_RELEASE_BEFORE_LOCKS, true, true);
    ResourceOwnerRelease(CurrentResourceOwner, RESOURCE_RELEASE_LOCKS, true, true);
    ResourceOwnerRelease(CurrentResourceOwner, RESOURCE_RELEASE_AFTER_LOCKS, true, true);
    CurrentResourceOwner = NULL;

    MemoryContextSwitchTo(old_context);
}

#endif
