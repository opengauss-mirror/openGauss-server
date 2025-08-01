/* -------------------------------------------------------------------------
 *
 * connection.c
 * 		  Connection management functions for postgres_fdw
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2012-2014, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 * 		  contrib/postgres_fdw/connection.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include "postgres_fdw.h"

#include "access/htup.h"
#include "catalog/pg_user_mapping.h"
#include "access/xact.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "utils/hsearch.h"
#include "utils/inval.h"
#include "utils/memutils.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"
#include "storage/ipc.h"
#include "executor/executor.h"

/*
 * Connection cache hash table entry
 *
 * The lookup key in this hash table is the foreign server OID plus the user
 * mapping OID.  (We use just one connection per user per foreign server,
 * so that we can ensure all scans use the same snapshot during a query.)
 *
 * The "conn" pointer can be NULL if we don't currently have a live connection.
 * When we do have a connection, xact_depth tracks the current depth of
 * transactions and subtransactions open on the remote side.  We need to issue
 * commands at the same nesting depth on the remote as we're executing at
 * ourselves, so that rolling back a subtransaction will kill the right
 * queries and not the wrong ones.
 */
typedef struct ConnCacheKey {
    Oid serverid; /* OID of foreign server */
    Oid userid;   /* OID of local user whose mapping we use */
} ConnCacheKey;

typedef struct ConnCacheEntry {
    ConnCacheKey key; /* hash key (must be first) */
    PGconn *conn;     /* connection to foreign server, or NULL */
    /* Remaining fields are invalid when conn is NULL: */
    int xact_depth;           /* 0 = no xact open, 1 = main xact open, 2 =
                               * one level of subxact open, etc */
    bool have_prep_stmt;      /* have we prepared any stmts in this xact? */
    bool have_error;          /* have any subxacts aborted in this xact? */
    bool changing_xact_state; /* xact state change in process */
    bool invalidated;         /* true if reconnect is pending */
    uint32 server_hashvalue;  /* hash value of foreign server OID */
    uint32 mapping_hashvalue; /* hash value of user mapping OID */
} ConnCacheEntry;

typedef struct PgFdwData_t {
    HTAB* connHash;
    unsigned int cursor_number; /* cursor numbers */
    unsigned int prep_stmt_number; /* prepared statement numbers */
    bool xact_got_connection; /* tracks whether any work is needed in callback functions */
} PgFdwData;

#define FDW_CONN_HASH (((PgFdwData*)u_sess->ext_fdw_ctx[POSTGRES_TYPE_FDW].connList)->connHash)
#define FDW_CURSOR_NUM (((PgFdwData*)u_sess->ext_fdw_ctx[POSTGRES_TYPE_FDW].connList)->cursor_number)
#define FDW_PREP_STMT_NUM (((PgFdwData*)u_sess->ext_fdw_ctx[POSTGRES_TYPE_FDW].connList)->prep_stmt_number)
#define FDW_XACT_GOT_CONN (((PgFdwData*)u_sess->ext_fdw_ctx[POSTGRES_TYPE_FDW].connList)->xact_got_connection)

/* prototypes of private functions */
static PGconn *connect_pg_server(ForeignServer *server, UserMapping *user);
static void disconnect_pg_server(ConnCacheEntry *entry);
static void check_conn_params(const char **keywords, const char **values);
static void configure_remote_session(PGconn *conn);
static void do_sql_command(PGconn *conn, const char *sql);
static void begin_remote_xact(ConnCacheEntry *entry);
static void pgfdw_xact_callback(XactEvent event, void *arg);
static void pgfdw_subxact_callback(SubXactEvent event, SubTransactionId mySubid, SubTransactionId parentSubid,
    void *arg);
static void pgfdw_inval_callback(Datum arg, int cacheid, uint32 hashvalue);
static void pgfdw_reject_incomplete_xact_state_change(ConnCacheEntry *entry);
static bool pgfdw_cancel_query(PGconn *conn);
static bool pgfdw_exec_cleanup_query(PGconn *conn, const char *query, bool ignore_errors);
static bool pgfdw_get_cleanup_result(PGconn *conn, TimestampTz endtime, PGresult **result);

static void pg_fdw_exit(int code, Datum arg)
{
    HASH_SEQ_STATUS scan;
    ConnCacheEntry *entry = NULL;

    if (FDW_CONN_HASH == NULL) {
        return;
    }

    hash_seq_init(&scan, FDW_CONN_HASH);
    while ((entry = (ConnCacheEntry *)hash_seq_search(&scan))) {
        if (entry->conn == NULL) {
            continue;
        }

        PQfinish(entry->conn);
        entry->conn = NULL;
    }
    /* clean-up memory */
    hash_destroy(FDW_CONN_HASH);
    FDW_CONN_HASH = NULL;
    FDW_CURSOR_NUM = 0;
    FDW_PREP_STMT_NUM = 0;
    FDW_XACT_GOT_CONN = false;
    u_sess->ext_fdw_ctx[POSTGRES_TYPE_FDW].fdwExitFunc = NULL;
    pfree_ext(u_sess->ext_fdw_ctx[POSTGRES_TYPE_FDW].connList);
}

/*
 * Get a PGconn which can be used to execute queries on the remote openGauss
 * server with the user's authorization.  A new connection is established
 * if we don't already have a suitable one, and a transaction is opened at
 * the right subtransaction nesting depth if we didn't do that already.
 *
 * will_prep_stmt must be true if caller intends to create any prepared
 * statements.  Since those don't go away automatically at transaction end
 * (not even on error), we need this flag to cue manual cleanup.
 */
PGconn *GetConnection(ForeignServer *server, UserMapping *user, bool will_prep_stmt)
{
    bool found;
    ConnCacheKey key;

    /* First time through, initialize connection cache hashtable */
    if (u_sess->ext_fdw_ctx[POSTGRES_TYPE_FDW].connList == NULL) {
        /* malloc private data using u_sess->cache_mem_cxt */
        MemoryContext oldcontext = MemoryContextSwitchTo(u_sess->cache_mem_cxt);
        u_sess->ext_fdw_ctx[POSTGRES_TYPE_FDW].connList = palloc0(sizeof(PgFdwData));
        (void)MemoryContextSwitchTo(oldcontext);

        HASHCTL ctl;
        errno_t rc = memset_s(&ctl, sizeof(ctl), 0, sizeof(ctl));
        securec_check(rc, "\0", "\0");
        ctl.keysize = sizeof(ConnCacheKey);
        ctl.entrysize = sizeof(ConnCacheEntry);
        ctl.hash = tag_hash;
        /* allocate ConnectionHash in the cache context */
        ctl.hcxt = u_sess->cache_mem_cxt;
        FDW_CONN_HASH = hash_create("postgres_fdw connections", 8, &ctl, HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

        /*
         * Register some callback functions that manage connection cleanup.
         * This should be done just once in each backend.
         */
        RegisterXactCallback(pgfdw_xact_callback, NULL);
        RegisterSubXactCallback(pgfdw_subxact_callback, NULL);

        CacheRegisterSessionSyscacheCallback(FOREIGNSERVEROID, pgfdw_inval_callback, (Datum)0);
        CacheRegisterSessionSyscacheCallback(USERMAPPINGOID, pgfdw_inval_callback, (Datum)0);

        if (IS_THREAD_POOL_SESSION) {
            u_sess->ext_fdw_ctx[POSTGRES_TYPE_FDW].fdwExitFunc = pg_fdw_exit;
        } else {
            on_proc_exit(&pg_fdw_exit, PointerGetDatum(NULL));
        }
    }

    /* Set flag that we did GetConnection during the current transaction */
    FDW_XACT_GOT_CONN = true;

    /* Create hash key for the entry.  Assume no pad bytes in key struct */
    key.serverid = server->serverid;
    key.userid = user->userid;

    /*
     * Find or create cached entry for requested connection.
     */
    ConnCacheEntry* entry = (ConnCacheEntry *)hash_search(FDW_CONN_HASH, &key, HASH_ENTER, &found);
    if (!found) {
        /*
         * We need only clear "conn" here; remaining fields will be filled
         * later when "conn" is set.
         */
        entry->conn = NULL;
    }

    /* Reject further use of connections which failed abort cleanup. */
    pgfdw_reject_incomplete_xact_state_change(entry);

    /*
     * If the connection needs to be remade due to invalidation, disconnect as
     * soon as we're out of all transactions.
     */
    if (entry->conn != NULL && entry->invalidated && entry->xact_depth == 0) {
        elog(DEBUG3, "closing connection %p for option changes to take effect", entry->conn);
        disconnect_pg_server(entry);
    }

    /*
     * We don't check the health of cached connection here, because it would
     * require some overhead.  Broken connection will be detected when the
     * connection is actually used.
     * If cache entry doesn't have a connection, we have to establish a new
     * connection.  (If connect_pg_server throws an error, the cache entry
     * will remain in a valid empty state, ie conn == NULL.)
     */
    if (entry->conn == NULL) {
        Oid umoid;

        /* Reset all transient state fields, to be sure all are clean */
        entry->xact_depth = 0;
        entry->have_prep_stmt = false;
        entry->have_error = false;
        entry->changing_xact_state = false;
        entry->invalidated = false;
        entry->server_hashvalue = GetSysCacheHashValue1(FOREIGNSERVEROID, ObjectIdGetDatum(server->serverid));
        /* Pre-9.6, UserMapping doesn't store its OID, so look it up again */
        umoid =
            GetSysCacheOid2(USERMAPPINGUSERSERVER, ObjectIdGetDatum(user->userid), ObjectIdGetDatum(user->serverid));
        if (!OidIsValid(umoid)) {
            /* Not found for the specific user -- try PUBLIC */
            umoid =
                GetSysCacheOid2(USERMAPPINGUSERSERVER, ObjectIdGetDatum(InvalidOid), ObjectIdGetDatum(user->serverid));
        }
        entry->mapping_hashvalue = GetSysCacheHashValue1(USERMAPPINGOID, ObjectIdGetDatum(umoid));

        /* Now try to make the connection */
        entry->conn = connect_pg_server(server, user);
        elog(DEBUG3, "new postgres_fdw connection %p for server \"%s\"", entry->conn, server->servername);
    }

    /*
     * Start a new transaction or subtransaction if needed.
     */
    begin_remote_xact(entry);

    /* Remember if caller will prepare statements */
    entry->have_prep_stmt = entry->have_prep_stmt || will_prep_stmt;

    return entry->conn;
}

PGconn* GetConnectionByFScanState(ForeignScanState* node, bool will_prep_stmt)
{
    ForeignScan* fsplan = (ForeignScan *)node->ss.ps.plan;
    EState* estate = node->ss.ps.state;
    RangeTblEntry* rte = NULL;
    Oid userid;
    ForeignTable* table = NULL;
    ForeignServer* server = NULL;
    UserMapping* user = NULL;
    int rtindex;

    /*
     * Identify which user to do the remote access as.  This should match what
     * ExecCheckRTEPerms() does.  In case of a join or aggregate, use the
     * lowest-numbered member RTE as a representative; we would get the same
     * result from any.
     */
    if (fsplan->scan.scanrelid > 0) {
        rtindex = fsplan->scan.scanrelid;
    } else {
        rtindex = bms_next_member(fsplan->fs_relids, -1);
    }
    rte = exec_rt_fetch(rtindex, estate);
    userid = rte->checkAsUser ? rte->checkAsUser : GetUserId();

    /* Get info about foreign table. */
    table = GetForeignTable(rte->relid);
    server = GetForeignServer(table->serverid);
    user = GetUserMapping(userid, table->serverid);

    return GetConnection(server, user, will_prep_stmt);
}

/*
 * Connect to remote server using specified server and user mapping properties.
 */
static PGconn *connect_pg_server(ForeignServer *server, UserMapping *user)
{
    PGconn *volatile conn = NULL;

    /*
     * Use PG_TRY block to ensure closing connection on error.
     */
    PG_TRY();
    {
        int n;

        /*
         * Construct connection params from generic options of ForeignServer
         * and UserMapping.  (Some of them might not be libpq options, in
         * which case we'll just waste a few array slots.)  Add 3 extra slots
         * for fallback_application_name, client_encoding, end marker.
         */
        n = list_length(server->options) + list_length(user->options) + 3;
        const char** keywords = (const char **)palloc(n * sizeof(char *));
        const char** values = (const char **)palloc(n * sizeof(char *));

        n = 0;
        n += ExtractConnectionOptions(server->options, keywords + n, values + n);
        n += ExtractConnectionOptions(user->options, keywords + n, values + n);

        /* Use "postgres_fdw" as fallback_application_name. */
        keywords[n] = "fallback_application_name";
        values[n] = "postgres_fdw";
        n++;

        /* Set client_encoding so that libpq can convert encoding properly. */
        keywords[n] = "client_encoding";
        values[n] = GetDatabaseEncodingName();
        n++;

        keywords[n] = values[n] = NULL;

        /* verify connection parameters and make connection */
        check_conn_params(keywords, values);

        conn = PQconnectdbParams(keywords, values, 0);
        if (conn == NULL || PQstatus(conn) != CONNECTION_OK) {
            int msglen;

            /* libpq typically appends a newline, strip that */
            char* connmessage = pstrdup(PQerrorMessage(conn));
            msglen = strlen(connmessage);
            if (msglen > 0 && connmessage[msglen - 1] == '\n') {
                connmessage[msglen - 1] = '\0';
            }
            ereport(ERROR, (errcode(ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION),
                errmsg("could not connect to server \"%s\"", server->servername),
                errdetail_internal("%s", connmessage)));
        }

        /*
         * Check that non-superuser has used password to establish connection;
         * otherwise, he's piggybacking on the openGauss server's user
         * identity. See also dblink_security_check() in contrib/dblink.
         */
        if (!superuser() && !PQconnectionUsedPassword(conn)) {
            ereport(ERROR, (errcode(ERRCODE_S_R_E_PROHIBITED_SQL_STATEMENT_ATTEMPTED), errmsg("password is required"),
                errdetail("Non-superuser cannot connect if the server does not request a password."),
                errhint("Target server's authentication method must be changed.")));
        }

        /* Prepare new session for use */
        configure_remote_session(conn);

        pfree(keywords);
        pfree(values);
    }
    PG_CATCH();
    {
        /* Release PGconn data structure if we managed to create one */
        if (conn) {
            PQfinish(conn);
        }
        PG_RE_THROW();
    }
    PG_END_TRY();

    return conn;
}

/*
 * Disconnect any open connection for a connection cache entry.
 */
static void disconnect_pg_server(ConnCacheEntry *entry)
{
    if (entry->conn != NULL) {
        PQfinish(entry->conn);
        entry->conn = NULL;
    }
}

/*
 * For non-superusers, insist that the connstr specify a password.  This
 * prevents a password from being picked up from .pgpass, a service file,
 * the environment, etc.  We don't want the postgres user's passwords
 * to be accessible to non-superusers.  (See also dblink_connstr_check in
 * contrib/dblink.)
 */
static void check_conn_params(const char **keywords, const char **values)
{
    int i;

    /* no check required if superuser */
    if (superuser()) {
        return;
    }

    /* ok if params contain a non-empty password */
    for (i = 0; keywords[i] != NULL; i++) {
        if (strcmp(keywords[i], "password") == 0 && values[i][0] != '\0') {
            return;
        }
    }

    ereport(ERROR, (errcode(ERRCODE_S_R_E_PROHIBITED_SQL_STATEMENT_ATTEMPTED), errmsg("password is required"),
        errdetail("Non-superusers must provide a password in the user mapping.")));
}

/*
 * Issue SET commands to make sure remote session is configured properly.
 *
 * We do this just once at connection, assuming nothing will change the
 * values later.  Since we'll never send volatile function calls to the
 * remote, there shouldn't be any way to break this assumption from our end.
 * It's possible to think of ways to break it at the remote end, eg making
 * a foreign table point to a view that includes a set_config call ---
 * but once you admit the possibility of a malicious view definition,
 * there are any number of ways to break things.
 */
static void configure_remote_session(PGconn *conn)
{
    int remoteversion = PQserverVersion(conn);

    /* Force the search path to contain only pg_catalog (see deparse.c) */
    do_sql_command(conn, "SET search_path = pg_catalog");

    /*
     * Set remote timezone; this is basically just cosmetic, since all
     * transmitted and returned timestamptzs should specify a zone explicitly
     * anyway.  However it makes the regression test outputs more predictable.
     *
     * We don't risk setting remote zone equal to ours, since the remote
     * server might use a different timezone database.  Instead, use UTC
     * (quoted, because very old servers are picky about case).
     */
    do_sql_command(conn, "SET timezone = 'UTC'");

    /*
     * Set values needed to ensure unambiguous data output from remote.  (This
     * logic should match what pg_dump does.  See also set_transmission_modes
     * in postgres_fdw.c.)
     */
    do_sql_command(conn, "SET datestyle = ISO");
    if (remoteversion >= 80400) {
        do_sql_command(conn, "SET intervalstyle = postgres");
    }
    if (remoteversion >= 90000) {
        do_sql_command(conn, "SET extra_float_digits = 3");
    } else {
        do_sql_command(conn, "SET extra_float_digits = 2");
    }
}

/*
 * Convenience subroutine to issue a non-data-returning SQL command to remote
 */
static void do_sql_command(PGconn *conn, const char *sql)
{
    if (!PQsendQuery(conn, sql)) {
        pgfdw_report_error(ERROR, NULL, conn, false, sql);
    }
    PGresult* res = pgfdw_get_result(conn, sql);
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        pgfdw_report_error(ERROR, res, conn, true, sql);
    }
    PQclear(res);
}

/*
 * Start remote transaction or subtransaction, if needed.
 *
 * Note that we always use at least REPEATABLE READ in the remote session.
 * This is so that, if a query initiates multiple scans of the same or
 * different foreign tables, we will get snapshot-consistent results from
 * those scans.  A disadvantage is that we can't provide sane emulation of
 * READ COMMITTED behavior --- it would be nice if we had some other way to
 * control which remote queries share a snapshot.
 */
static void begin_remote_xact(ConnCacheEntry *entry)
{
    int curlevel = GetCurrentTransactionNestLevel();

    /* Start main transaction if we haven't yet */
    if (entry->xact_depth <= 0) {
        const char* sql = NULL;

        elog(DEBUG3, "starting remote transaction on connection %p", entry->conn);

        if (IsolationIsSerializable()) {
            sql = "START TRANSACTION ISOLATION LEVEL SERIALIZABLE";
        } else {
            sql = "START TRANSACTION ISOLATION LEVEL REPEATABLE READ";
        }
        entry->changing_xact_state = true;
        do_sql_command(entry->conn, sql);
        entry->xact_depth = 1;
        entry->changing_xact_state = false;
    }

    /*
     * If we're in a subtransaction, stack up savepoints to match our level.
     * This ensures we can rollback just the desired effects when a
     * subtransaction aborts.
     */
    while (entry->xact_depth < curlevel) {
        char sql[64];

        int rc = snprintf_s(sql, sizeof(sql), sizeof(sql) - 1, "SAVEPOINT s%d", entry->xact_depth + 1);
        securec_check_ss(rc, "", "");
        entry->changing_xact_state = true;
        do_sql_command(entry->conn, sql);
        entry->xact_depth++;
        entry->changing_xact_state = false;
    }
}

/*
 * Release connection reference count created by calling GetConnection.
 */
void ReleaseConnection(PGconn *conn)
{
    /*
     * Currently, we don't actually track connection references because all
     * cleanup is managed on a transaction or subtransaction basis instead. So
     * there's nothing to do here.
     */
}

/*
 * Assign a "unique" number for a cursor.
 *
 * These really only need to be unique per connection within a transaction.
 * For the moment we ignore the per-connection point and assign them across
 * all connections in the transaction, but we ask for the connection to be
 * supplied in case we want to refine that.
 *
 * Note that even if wraparound happens in a very long transaction, actual
 * collisions are highly improbable; just be sure to use %u not %d to print.
 */
unsigned int GetCursorNumber(PGconn *conn)
{
    return ++(FDW_CURSOR_NUM);
}

/*
 * Assign a "unique" number for a prepared statement.
 *
 * This works much like GetCursorNumber, except that we never reset the counter
 * within a session.  That's because we can't be 100% sure we've gotten rid
 * of all prepared statements on all connections, and it's not really worth
 * increasing the risk of prepared-statement name collisions by resetting.
 */
unsigned int GetPrepStmtNumber(PGconn *conn)
{
    return ++(FDW_PREP_STMT_NUM);
}

/*
 * Submit a query and wait for the result.
 *
 * This function is interruptible by signals.
 *
 * Caller is responsible for the error handling on the result.
 */
PGresult *pgfdw_exec_query(PGconn *conn, const char *query)
{
    /*
     * Submit a query.  Since we don't use non-blocking mode, this also can
     * block.  But its risk is relatively small, so we ignore that for now.
     */
    if (!PQsendQuery(conn, query)) {
        pgfdw_report_error(ERROR, NULL, conn, false, query);
    }

    /* Wait for the result. */
    return pgfdw_get_result(conn, query);
}

/*
 * Wait for the result from a prior asynchronous execution function call.
 *
 * This function offers quick responsiveness by checking for any interruptions.
 *
 * This function emulates PQexec()'s behavior of returning the last result
 * when there are many.
 *
 * Caller is responsible for the error handling on the result.
 */
PGresult *pgfdw_get_result(PGconn *conn, const char *query)
{
    PGresult *volatile last_res = NULL;

    /* In what follows, do not leak any PGresults on an error. */
    PG_TRY();
    {
        for (;;) {
            while (PQisBusy(conn)) {
                int wc;

                /* Sleep until there's something to do */
                wc = WaitLatchOrSocket(&t_thrd.proc->procLatch, WL_LATCH_SET | WL_SOCKET_READABLE, PQsocket(conn), -1L);
                ResetLatch(&t_thrd.proc->procLatch);

                CHECK_FOR_INTERRUPTS();

                /* Data available in socket? */
                if (wc & WL_SOCKET_READABLE) {
                    if (!PQconsumeInput(conn)) {
                        pgfdw_report_error(ERROR, NULL, conn, false, query);
                    }
                }
            }

            PGresult* res = PQgetResult(conn);
            if (res == NULL) {
                break; /* query is complete */
            }

            PQclear(last_res);
            last_res = res;
        }
    }
    PG_CATCH();
    {
        PQclear(last_res);
        PG_RE_THROW();
    }
    PG_END_TRY();

    return last_res;
}

/*
 * Report an error we got from the remote server.
 *
 * elevel: error level to use (typically ERROR, but might be less)
 * res: PGresult containing the error
 * conn: connection we did the query on
 * clear: if true, PQclear the result (otherwise caller will handle it)
 * sql: NULL, or text of remote command we tried to execute
 *
 * Note: callers that choose not to throw ERROR for a remote error are
 * responsible for making sure that the associated ConnCacheEntry gets
 * marked with have_error = true.
 */
void pgfdw_report_error(int elevel, PGresult *res, PGconn *conn, bool clear, const char *sql)
{
    /* If requested, PGresult must be released before leaving this function. */
    PG_TRY();
    {
        char *diag_sqlstate = PQresultErrorField(res, PG_DIAG_SQLSTATE);
        char *message_primary = PQresultErrorField(res, PG_DIAG_MESSAGE_PRIMARY);
        char *message_detail = PQresultErrorField(res, PG_DIAG_MESSAGE_DETAIL);
        char *message_hint = PQresultErrorField(res, PG_DIAG_MESSAGE_HINT);
        char *message_context = PQresultErrorField(res, PG_DIAG_CONTEXT);
        int sqlstate;

        if (diag_sqlstate) {
            sqlstate =
                MAKE_SQLSTATE(diag_sqlstate[0], diag_sqlstate[1], diag_sqlstate[2], diag_sqlstate[3], diag_sqlstate[4]);
        } else {
            sqlstate = ERRCODE_CONNECTION_FAILURE;
        }

        /*
         * If we don't get a message from the PGresult, try the PGconn.  This
         * is needed because for connection-level failures, PQexec may just
         * return NULL, not a PGresult at all.
         */
        if (message_primary == NULL) {
            message_primary = PQerrorMessage(conn);
        }

        ereport(elevel, (errcode(sqlstate),
            message_primary ? errmsg_internal("%s", message_primary) :
            errmsg("could not obtain message string for remote error"),
            message_detail ? errdetail_internal("%s", message_detail) : 0,
            message_hint ? errhint("%s", message_hint) : 0, message_context ? errcontext("%s", message_context) : 0,
            sql ? errcontext("Remote SQL command: %s", sql) : 0));
    }
    PG_CATCH();
    {
        if (clear) {
            PQclear(res);
        }
        PG_RE_THROW();
    }
    PG_END_TRY();
    if (clear) {
        PQclear(res);
    }
}

/*
 * pgfdw_xact_callback --- cleanup at main-transaction end.
 */
static void pgfdw_xact_callback_commit(ConnCacheEntry *entry)
{
    /*
     * If abort cleanup previously failed for this connection,
     * we can't issue any more commands against it.
     */
    pgfdw_reject_incomplete_xact_state_change(entry);

    /* Commit all remote transactions during pre-commit */
    entry->changing_xact_state = true;
    do_sql_command(entry->conn, "COMMIT TRANSACTION");
    entry->changing_xact_state = false;

    /*
     * If there were any errors in subtransactions, and we
     * made prepared statements, do a DEALLOCATE ALL to make
     * sure we get rid of all prepared statements. This is
     * annoying and not terribly bulletproof, but it's
     * probably not worth trying harder.
     *
     * DEALLOCATE ALL only exists in 8.3 and later, so this
     * constrains how old a server postgres_fdw can
     * communicate with.  We intentionally ignore errors in
     * the DEALLOCATE, so that we can hobble along to some
     * extent with older servers (leaking prepared statements
     * as we go; but we don't really support update operations
     * pre-8.3 anyway).
     */
    if (entry->have_prep_stmt && entry->have_error) {
        PGresult* res = PQexec(entry->conn, "DEALLOCATE ALL");
        PQclear(res);
    }
    entry->have_prep_stmt = false;
    entry->have_error = false;

}


static void pgfdw_xact_callback_abort(ConnCacheEntry *entry)
{
    bool abort_cleanup_failure = false;

     /*
     * Don't try to clean up the connection if we're already
     * in error recursion trouble.
     */
    if (in_error_recursion_trouble()) {
        entry->changing_xact_state = true;
    }

    /*
     * If connection is already unsalvageable, don't touch it
     * further.
     */
    if (entry->changing_xact_state) {
        return;
    }

    /*
     * Mark this connection as in the process of changing
     * transaction state.
     */
    entry->changing_xact_state = true;

    /* Assume we might have lost track of prepared statements */
    entry->have_error = true;

    /*
     * If a command has been submitted to the remote server by
     * using an asynchronous execution function, the command
     * might not have yet completed.  Check to see if a command
     * is still being processed by the remote server, and if so,
     * request cancellation of the command.
     */
    if (PQtransactionStatus(entry->conn) == PQTRANS_ACTIVE && !pgfdw_cancel_query(entry->conn)) {
        /* Unable to cancel running query. */
        abort_cleanup_failure = true;
    } else if (!pgfdw_exec_cleanup_query(entry->conn, "ABORT TRANSACTION", false)) {
        /* Unable to abort remote transaction. */
        abort_cleanup_failure = true;
    } else if (entry->have_prep_stmt && entry->have_error &&
        !pgfdw_exec_cleanup_query(entry->conn, "DEALLOCATE ALL", true)) {
        /* Trouble clearing prepared statements. */
        abort_cleanup_failure = true;
    } else {
        entry->have_prep_stmt = false;
        entry->have_error = false;
    }

    /* Disarm changing_xact_state if it all worked. */
    entry->changing_xact_state = abort_cleanup_failure;
}
/*
 * pgfdw_xact_callback --- cleanup at main-transaction end.
 */
static void pgfdw_xact_callback(XactEvent event, void *arg)
{
    HASH_SEQ_STATUS scan;
    ConnCacheEntry *entry = NULL;

    /* Quick exit if no connections were touched in this transaction. */
    if (!FDW_XACT_GOT_CONN) {
        return;
    }

#ifdef ENABLE_MOT
    /*
     * These events are added only for MOT FDW. So, others can safely ignore these event.
     */
    if (event == XACT_EVENT_START || event == XACT_EVENT_RECORD_COMMIT || event == XACT_EVENT_PREROLLBACK_CLEANUP ||
        event == XACT_EVENT_POST_COMMIT_CLEANUP || event == XACT_EVENT_STMT_FINISH) {
        return;
    }
#endif

    /*
     * Scan all connection cache entries to find open remote transactions, and
     * close them.
     */
    hash_seq_init(&scan, FDW_CONN_HASH);
    while ((entry = (ConnCacheEntry *)hash_seq_search(&scan))) {
        /* Ignore cache entry if no open connection right now */
        if (entry->conn == NULL) {
            continue;
        }

        /* If it has an open remote transaction, try to close it */
        if (entry->xact_depth > 0) {
            
            elog(DEBUG3, "closing remote transaction on connection %p", entry->conn);

            switch (event) {
                case XACT_EVENT_PRE_COMMIT:
                    pgfdw_xact_callback_commit(entry);
                    break;
                case XACT_EVENT_PRE_PREPARE:
                	/*
					 * We disallow any remote transactions, since it's not
					 * very reasonable to hold them open until the prepared
					 * transaction is committed.  For the moment, throw error
					 * unconditionally; later we might allow read-only cases.
					 * Note that the error will cause us to come right back
					 * here with event == XACT_EVENT_ABORT, so we'll clean up
					 * the connection state at that point.
					 */
                    elog(ERROR, "cannot PREPARE a transaction that has operated on postgres_fdw foreign tables");
                    break;
                case XACT_EVENT_COMMIT:
                case XACT_EVENT_PREPARE:
                    /* Pre-commit should have closed the open transaction */
                    elog(ERROR, "missed cleaning up connection during pre-commit");
                    break;
                case XACT_EVENT_ABORT:
                    pgfdw_xact_callback_abort(entry);
                    break;
                default:
                    break;
            }
        }

        /* Reset state to show we're out of a transaction */
        entry->xact_depth = 0;

        /*
         * If the connection isn't in a good idle state, discard it to
         * recover. Next GetConnection will open a new connection.
         */
        if (PQstatus(entry->conn) != CONNECTION_OK || PQtransactionStatus(entry->conn) != PQTRANS_IDLE ||
            entry->changing_xact_state) {
            elog(DEBUG3, "discarding connection %p", entry->conn);
            disconnect_pg_server(entry);
        }
    }

    /*
     * Regardless of the event type, we can now mark ourselves as out of the
     * transaction.  (Note: if we are here during PRE_COMMIT or PRE_PREPARE,
     * this saves a useless scan of the hashtable during COMMIT or PREPARE.)
     */
    FDW_XACT_GOT_CONN = false;

    /* Also reset cursor numbering for next transaction */
    FDW_CURSOR_NUM = 0;
}

/*
 * pgfdw_subxact_callback --- cleanup at subtransaction end.
 */
static void pgfdw_subxact_callback(SubXactEvent event, SubTransactionId mySubid, SubTransactionId parentSubid,
    void *arg)
{
    HASH_SEQ_STATUS scan;
    ConnCacheEntry *entry = NULL;
    int curlevel;
    int rc;

    /* Nothing to do at subxact start, nor after commit. */
    if (!(event == SUBXACT_EVENT_ABORT_SUB || event == SUBXACT_EVENT_COMMIT_SUB)) {
        return;
    }

    /* Quick exit if no connections were touched in this transaction. */
    if (!FDW_XACT_GOT_CONN) {
        return;
    }

    /*
     * Scan all connection cache entries to find open remote subtransactions
     * of the current level, and close them.
     */
    curlevel = GetCurrentTransactionNestLevel();
    hash_seq_init(&scan, FDW_CONN_HASH);
    while ((entry = (ConnCacheEntry *)hash_seq_search(&scan))) {
        char sql[100];

        /*
         * We only care about connections with open remote subtransactions of
         * the current level.
         */
        if (entry->conn == NULL || entry->xact_depth < curlevel) {
            continue;
        }

        if (entry->xact_depth > curlevel) {
            elog(ERROR, "missed cleaning up remote subtransaction at level %d", entry->xact_depth);
        }

        if (event == SUBXACT_EVENT_COMMIT_SUB) {
            /*
             * If abort cleanup previously failed for this connection, we
             * can't issue any more commands against it.
             */
            pgfdw_reject_incomplete_xact_state_change(entry);

            /* Commit all remote subtransactions during pre-commit */
            rc = snprintf_s(sql, sizeof(sql), sizeof(sql) - 1, "RELEASE SAVEPOINT s%d", curlevel);
            securec_check_ss(rc, "", "");
            entry->changing_xact_state = true;
            do_sql_command(entry->conn, sql);
            entry->changing_xact_state = false;
        } else if (in_error_recursion_trouble()) {
            /*
             * Don't try to clean up the connection if we're already in error
             * recursion trouble.
             */
            entry->changing_xact_state = true;
        } else if (!entry->changing_xact_state) {
            bool abort_cleanup_failure = false;

            /* Remember that abort cleanup is in progress. */
            entry->changing_xact_state = true;

            /* Assume we might have lost track of prepared statements */
            entry->have_error = true;

            /*
             * If a command has been submitted to the remote server by using an
             * asynchronous execution function, the command might not have yet
             * completed.  Check to see if a command is still being processed by
             * the remote server, and if so, request cancellation of the
             * command.
             */
            if (PQtransactionStatus(entry->conn) == PQTRANS_ACTIVE && !pgfdw_cancel_query(entry->conn)) {
                abort_cleanup_failure = true;
            } else {
                /* Rollback all remote subtransactions during abort */
                rc = snprintf_s(sql, sizeof(sql), sizeof(sql) - 1,
                                "ROLLBACK TO SAVEPOINT s%d; RELEASE SAVEPOINT s%d", curlevel, curlevel);
                securec_check_ss(rc, "", "");
                if (!pgfdw_exec_cleanup_query(entry->conn, sql, false)) {
                    abort_cleanup_failure = true;
                }
            }

            /* Disarm changing_xact_state if it all worked. */
            entry->changing_xact_state = abort_cleanup_failure;
        }

        /* OK, we're outta that level of subtransaction */
        entry->xact_depth--;
    }
}

/*
 * Connection invalidation callback function
 *
 * After a change to a pg_foreign_server or pg_user_mapping catalog entry,
 * mark connections depending on that entry as needing to be remade.
 * We can't immediately destroy them, since they might be in the midst of
 * a transaction, but we'll remake them at the next opportunity.
 *
 * Although most cache invalidation callbacks blow away all the related stuff
 * regardless of the given hashvalue, connections are expensive enough that
 * it's worth trying to avoid that.
 *
 * NB: We could avoid unnecessary disconnection more strictly by examining
 * individual option values, but it seems too much effort for the gain.
 */
static void pgfdw_inval_callback(Datum arg, int cacheid, uint32 hashvalue)
{
    HASH_SEQ_STATUS scan;
    ConnCacheEntry *entry = NULL;

    Assert(cacheid == FOREIGNSERVEROID || cacheid == USERMAPPINGOID);

    /* ConnectionHash must exist already, if we're registered */
    hash_seq_init(&scan, FDW_CONN_HASH);
    while ((entry = (ConnCacheEntry *)hash_seq_search(&scan))) {
        /* Ignore invalid entries */
        if (entry->conn == NULL) {
            continue;
        }

        /* hashvalue == 0 means a cache reset, must clear all state */
        if (hashvalue == 0 || (cacheid == FOREIGNSERVEROID && entry->server_hashvalue == hashvalue) ||
            (cacheid == USERMAPPINGOID && entry->mapping_hashvalue == hashvalue)) {
            entry->invalidated = true;
        }
    }
}

/*
 * Raise an error if the given connection cache entry is marked as being
 * in the middle of an xact state change.  This should be called at which no
 * such change is expected to be in progress; if one is found to be in
 * progress, it means that we aborted in the middle of a previous state change
 * and now don't know what the remote transaction state actually is.
 * Such connections can't safely be further used.  Re-establishing the
 * connection would change the snapshot and roll back any writes already
 * performed, so that's not an option, either. Thus, we must abort.
 */
static void pgfdw_reject_incomplete_xact_state_change(ConnCacheEntry *entry)
{
    /* nothing to do for inactive entries and entries of sane state */
    if (entry->conn == NULL || !entry->changing_xact_state) {
        return;
    }

    /* make sure this entry is inactive */
    disconnect_pg_server(entry);

    /* find server name to be shown in the message below */
    ForeignServer* server = GetForeignServer(entry->key.serverid);

    ereport(ERROR,
        (errcode(ERRCODE_CONNECTION_EXCEPTION), errmsg("connection to server \"%s\" was lost", server->servername)));
}

/*
 * Cancel the currently-in-progress query (whose query text we do not have)
 * and ignore the result.  Returns true if we successfully cancel the query
 * and discard any pending result, and false if not.
 */
static bool pgfdw_cancel_query(PGconn *conn)
{
    PGcancel *cancel = NULL;
    char errbuf[256];
    PGresult *result = NULL;
    TimestampTz endtime;

    /*
     * If it takes too long to cancel the query and discard the result, assume
     * the connection is dead.
     */
    endtime = TimestampTzPlusMilliseconds(GetCurrentTimestamp(), 30000);

    /*
     * Issue cancel request.  Unfortunately, there's no good way to limit the
     * amount of time that we might block inside PQgetCancel().
     */
    if ((cancel = PQgetCancel(conn))) {
        if (!PQcancel(cancel, errbuf, sizeof(errbuf))) {
            ereport(WARNING,
                (errcode(ERRCODE_CONNECTION_FAILURE), errmsg("could not send cancel request: %s", errbuf)));
            PQfreeCancel(cancel);
            return false;
        }
        PQfreeCancel(cancel);
    }

    /* Get and discard the result of the query. */
    if (pgfdw_get_cleanup_result(conn, endtime, &result)) {
        return false;
    }
    PQclear(result);

    return true;
}

/*
 * Submit a query during (sub)abort cleanup and wait up to 30 seconds for the
 * result.  If the query is executed without error, the return value is true.
 * If the query is executed successfully but returns an error, the return
 * value is true if and only if ignore_errors is set.  If the query can't be
 * sent or times out, the return value is false.
 */
static bool pgfdw_exec_cleanup_query(PGconn *conn, const char *query, bool ignore_errors)
{
    PGresult *result = NULL;
    TimestampTz endtime;

    /*
     * If it takes too long to execute a cleanup query, assume the connection
     * is dead.  It's fairly likely that this is why we aborted in the first
     * place (e.g. statement timeout, user cancel), so the timeout shouldn't
     * be too long.
     */
    endtime = TimestampTzPlusMilliseconds(GetCurrentTimestamp(), 30000);

    /*
     * Submit a query.  Since we don't use non-blocking mode, this also can
     * block.  But its risk is relatively small, so we ignore that for now.
     */
    if (!PQsendQuery(conn, query)) {
        pgfdw_report_error(WARNING, NULL, conn, false, query);
        return false;
    }

    /* Get the result of the query. */
    if (pgfdw_get_cleanup_result(conn, endtime, &result)) {
        return false;
    }

    /* Issue a warning if not successful. */
    if (PQresultStatus(result) != PGRES_COMMAND_OK) {
        pgfdw_report_error(WARNING, result, conn, true, query);
        return ignore_errors;
    }
    PQclear(result);

    return true;
}

/*
 * Get, during abort cleanup, the result of a query that is in progress.  This
 * might be a query that is being interrupted by transaction abort, or it might
 * be a query that was initiated as part of transaction abort to get the remote
 * side back to the appropriate state.
 *
 * It's not a huge problem if we throw an ERROR here, but if we get into error
 * recursion trouble, we'll end up slamming the connection shut, which will
 * necessitate failing the entire toplevel transaction even if subtransactions
 * were used.  Try to use WARNING where we can.
 *
 * endtime is the time at which we should give up and assume the remote
 * side is dead.  Returns true if the timeout expired, otherwise false.
 * Sets *result except in case of a timeout.
 */
static bool pgfdw_get_cleanup_result(PGconn *conn, TimestampTz endtime, PGresult **result)
{
    volatile bool timed_out = false;
    PGresult *volatile last_res = NULL;

    /* In what follows, do not leak any PGresults on an error. */
    PG_TRY();
    {
        for (;;) {
            while (PQisBusy(conn)) {
                int wc;
                TimestampTz now = GetCurrentTimestamp();
                long secs;
                int microsecs;
                long cur_timeout;

                /* If timeout has expired, give up, else get sleep time. */
                if (now >= endtime) {
                    timed_out = true;
                    goto exit;
                }
                TimestampDifference(now, endtime, &secs, &microsecs);

                /* To protect against clock skew, limit sleep to one minute. */
                cur_timeout = Min(60000, secs * USECS_PER_SEC + microsecs);

                /* Sleep until there's something to do */
                wc = WaitLatchOrSocket(&t_thrd.proc->procLatch, WL_LATCH_SET | WL_SOCKET_READABLE | WL_TIMEOUT,
                    PQsocket(conn), cur_timeout);
                ResetLatch(&t_thrd.proc->procLatch);

                CHECK_FOR_INTERRUPTS();

                /* Data available in socket? */
                if (wc & WL_SOCKET_READABLE) {
                    if (!PQconsumeInput(conn)) {
                        /* connection trouble; treat the same as a timeout */
                        timed_out = true;
                        goto exit;
                    }
                }
            }

            PGresult* res = PQgetResult(conn);
            if (res == NULL) {
                break; /* query is complete */
            }

            PQclear(last_res);
            last_res = res;
        }
    exit:;
    }
    PG_CATCH();
    {
        PQclear(last_res);
        PG_RE_THROW();
    }
    PG_END_TRY();

    if (timed_out) {
        PQclear(last_res);
    } else {
        *result = last_res;
    }
    return timed_out;
}

