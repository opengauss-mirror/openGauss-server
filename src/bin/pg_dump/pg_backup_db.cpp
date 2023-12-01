/* -------------------------------------------------------------------------
 *
 * pg_backup_db.c
 *
 *	Implements the basic DB functions used by the archiver.
 *
 * IDENTIFICATION
 *	  src/bin/pg_dump/pg_backup_db.c
 *
 * -------------------------------------------------------------------------
 */

#include "pg_backup_db.h"
#include "dumpmem.h"
#include "dumputils.h"
#include "postgres.h"
#include "knl/knl_variable.h"
#include "bin/elog.h"

#include <unistd.h>
#include <ctype.h>
#ifdef HAVE_TERMIOS_H
#include <termios.h>
#endif

#ifdef GAUSS_SFT_TEST
#include "gauss_sft.h"
#endif

#ifndef WIN32
#include "libpq/libpq-int.h"
#endif

#define DB_MAX_ERR_STMT 128

/* translator: this is a module name */
static const char* modulename = gettext_noop("archiver (db)");

static void _check_database_version(ArchiveHandle* AH);
static PGconn* _connectDB(ArchiveHandle* AH, const char* newdbname, const char* newUser);
static void notice_processor(void* arg, const char* message);

static int _parse_version(const char* versionString)
{
    int v;

    v = parse_version(versionString);
    if (v < 0)
        exit_horribly(modulename, "could not parse version string \"%s\"\n", versionString);

    return v;
}

static void _check_database_version(ArchiveHandle* AH)
{
    int myversion;
    const char* remoteversion_str = NULL;
    int remoteversion;

    myversion = _parse_version(PG_VERSION);

    remoteversion_str = PQparameterStatus(AH->connection, "server_version");
    if (remoteversion_str == NULL)
        exit_horribly(modulename, "could not get server_version from libpq\n");

    remoteversion = _parse_version(remoteversion_str);
    /* before memory overwritting please free it */
    if (AH->publicArc.remoteVersionStr != NULL)
        free(AH->publicArc.remoteVersionStr);

    AH->publicArc.remoteVersionStr = gs_strdup(remoteversion_str);
    AH->publicArc.remoteVersion = remoteversion;
    if ((AH->archiveRemoteVersion) == NULL)
        AH->archiveRemoteVersion = AH->publicArc.remoteVersionStr;

    if (myversion != remoteversion &&
        (remoteversion < AH->publicArc.minRemoteVersion || remoteversion > AH->publicArc.maxRemoteVersion)) {
        write_msg(NULL, "server version: %s; %s version: %s\n", remoteversion_str, progname, PG_VERSION);
        exit_horribly(NULL, "aborting because of server version mismatch\n");
    }
}

/*
 * Reconnect to the server.  If dbname is not NULL, use that database,
 * else the one associated with the archive handle.  If username is
 * not NULL, use that user name, else the one from the handle.	If
 * both the database and the user match the existing connection already,
 * nothing will be done.
 *
 * Returns 1 in any case.
 */
int ReconnectToServer(ArchiveHandle* AH, const char* dbname, const char* username)
{
    PGconn* newConn = NULL;
    const char* newdbname = NULL;
    const char* newusername = NULL;

    if (dbname == NULL)
        newdbname = PQdb(AH->connection);
    else
        newdbname = dbname;

    if (username == NULL)
        newusername = PQuser(AH->connection);
    else
        newusername = username;

    /* Let's see if the request is already satisfied */
    if (strcmp(newdbname, PQdb(AH->connection)) == 0 && strcmp(newusername, PQuser(AH->connection)) == 0)
        return 1;

    newConn = _connectDB(AH, newdbname, newusername);

    PQfinish(AH->connection);
    AH->connection = newConn;

    return 1;
}

/*
 * Connect to the db again.
 *
 * Note: it's not really all that sensible to use a single-entry password
 * cache if the username keeps changing.  In current usage, however, the
 * username never does change, so one savedPassword is sufficient.	We do
 * update the cache on the off chance that the password has changed since the
 * start of the run.
 */
static PGconn* _connectDB(ArchiveHandle* AH, const char* reqdb, const char* requser)
{
    PGconn* newConn = NULL;
    const char* newdb = NULL;
    const char* newuser = NULL;
    char* password = AH->savedPassword;
    bool new_pass = false;
    errno_t rc = 0;

    if (reqdb == NULL)
        newdb = PQdb(AH->connection);
    else
        newdb = reqdb;

    if ((requser == NULL) || strlen(requser) == 0)
        newuser = PQuser(AH->connection);
    else
        newuser = requser;

    ahlog(AH, 1, "connecting to database \"%s\" as user \"%s\"\n", newdb, newuser);

    if (AH->promptPassword == TRI_YES && password == NULL) {
        password = simple_prompt("Password: ", 100, false);
        if (password == NULL)
            exit_horribly(modulename, "out of memory\n");
    }

    do {
#define PARAMS_ARRAY_SIZE 8
        const char** keywords = (const char**)pg_malloc(PARAMS_ARRAY_SIZE * sizeof(*keywords));
        const char** values = (const char**)pg_malloc(PARAMS_ARRAY_SIZE * sizeof(*values));

        keywords[0] = "host";
        values[0] = PQhost(AH->connection);
        keywords[1] = "port";
        values[1] = PQport(AH->connection);
        keywords[2] = "user";
        values[2] = newuser;
        keywords[3] = "password";
        values[3] = password;
        keywords[4] = "dbname";
        values[4] = newdb;
        keywords[5] = "fallback_application_name";
        values[5] = progname;
        keywords[6] = "enable_ce";
        values[6] = "1";
        keywords[7] = NULL;
        values[7] = NULL;

        new_pass = false;
        newConn = PQconnectdbParams(keywords, values, true);

        free(keywords);
        keywords = NULL;
        rc = memset_s(values, PARAMS_ARRAY_SIZE * sizeof(*values), 0, PARAMS_ARRAY_SIZE * sizeof(*values));
        securec_check_c(rc, "\0", "\0");
        free(values);
        values = NULL;

        if (newConn == NULL)
            exit_horribly(modulename, "failed to reconnect to database\n");

        if (PQstatus(newConn) == CONNECTION_BAD) {
            if (!PQconnectionNeedsPassword(newConn))
                exit_horribly(modulename, "could not reconnect to database: %s", PQerrorMessage(newConn));
            PQfinish(newConn);

            if (password != NULL)
                write_stderr("Password incorrect\n");

            write_stderr("Connecting to %s as %s\n", newdb, newuser);

            if (password != NULL) {
                rc = memset_s(password, strlen(password), 0, strlen(password));
                securec_check_c(rc, "\0", "\0");
                free(password);
                password = NULL;
            }

            if (AH->promptPassword != TRI_NO)
                password = simple_prompt("Password: ", 100, false);
            else
                exit_horribly(modulename, "connection needs password\n");

            if (password == NULL)
                exit_horribly(modulename, "out of memory\n");
            new_pass = true;
        }
    } while (new_pass);

    AH->savedPassword = password;

    /* check for version mismatch */
    _check_database_version(AH);

    PQsetNoticeProcessor(newConn, notice_processor, NULL);

    return newConn;
}

/*
 * Make a database connection with the given parameters.  The
 * connection handle is returned, the parameters are stored in AHX.
 * An interactive password prompt is automatically issued if required.
 *
 * Note: it's not really all that sensible to use a single-entry password
 * cache if the username keeps changing.  In current usage, however, the
 * username never does change, so one savedPassword is sufficient.
 */
char* ConnectDatabase(Archive* AHX, const char* dbname, const char* pghost, const char* pgport, const char* username,
    enum trivalue promptPassword, bool exitOnError)
{
    ArchiveHandle* AH = (ArchiveHandle*)AHX;
    char* password = AH->savedPassword;
    bool new_pass = false;
    errno_t rc = 0;

    if (AH->connection != NULL)
        exit_horribly(modulename, "already connected to a database\n");

    if (promptPassword == TRI_YES && password == NULL) {
        password = simple_prompt("Password: ", 100, false);
        if (password == NULL)
            exit_horribly(modulename, "out of memory\n");
    }
    AH->promptPassword = promptPassword;

    /*
     * Start the connection.  Loop until we have a password if requested by
     * backend.
     */
    do {
#define PARAMS_ARRAY_SIZE 8
        const char** keywords = (const char**)pg_malloc(PARAMS_ARRAY_SIZE * sizeof(*keywords));
        const char** values = (const char**)pg_malloc(PARAMS_ARRAY_SIZE * sizeof(*values));

        keywords[0] = "host";
        values[0] = pghost;
        keywords[1] = "port";
        values[1] = pgport;
        keywords[2] = "user";
        values[2] = username;
        keywords[3] = "password";
        values[3] = password;
        keywords[4] = "dbname";
        values[4] = dbname;
        keywords[5] = "fallback_application_name";
        values[5] = progname;
        keywords[6] = "enable_ce";
        values[6] = "1";
        keywords[7] = NULL;
        values[7] = NULL;

        new_pass = false;
        AH->connection = PQconnectdbParams(keywords, values, true);

#ifndef WIN32
        /* Clear password related memory to avoid leaks when core. */
        if ((AH->connection != NULL) && (AH->connection->pgpass != NULL)) {
            rc = memset_s(AH->connection->pgpass, strlen(AH->connection->pgpass), 0, strlen(AH->connection->pgpass));
            securec_check_c(rc, "\0", "\0");
        }
#endif

        free(keywords);
        keywords = NULL;
        rc = memset_s(values, PARAMS_ARRAY_SIZE * sizeof(*values), 0, PARAMS_ARRAY_SIZE * sizeof(*values));
        securec_check_c(rc, "\0", "\0");
        free(values);
        values = NULL;

        if ((AH->connection) == NULL)
            exit_horribly(modulename, "failed to connect to database\n");

        if (PQstatus(AH->connection) == CONNECTION_BAD &&
            (strstr(PQerrorMessage(AH->connection), "password") != NULL) && password == NULL &&
            promptPassword != TRI_NO) {
            PQfinish(AH->connection);
            password = simple_prompt("Password: ", 100, false);
            if (password == NULL)
                exit_horribly(modulename, "out of memory\n");
            new_pass = true;
        }
    } while (new_pass);

    AH->savedPassword = password;

    /* check to see that the backend connection was successfully made */
    if (PQstatus(AH->connection) == CONNECTION_BAD) {
        if (!exitOnError) {
            return PQerrorMessage(AH->connection);
        }
        exit_horribly(modulename,
            "connection to database \"%s\" failed: %s",
            PQdb(AH->connection) != NULL ? PQdb(AH->connection) : "",
            PQerrorMessage(AH->connection));
    }

    /* check for version mismatch */
    _check_database_version(AH);

    PQsetNoticeProcessor(AH->connection, notice_processor, NULL);
    return NULL;
}

/*
 * Close the connection to the database and also cancel off the query if we
 * have one running.
 */
void DisconnectDatabase(Archive* AHX)
{
    ArchiveHandle* AH = (ArchiveHandle*)AHX;
    PGcancel* cancel = NULL;
    char errbuf[1];

    if ((AH->connection) == NULL)
        return;

    if (PQtransactionStatus(AH->connection) == PQTRANS_ACTIVE) {
        if ((cancel = PQgetCancel(AH->connection)) != NULL) {
            (void)PQcancel(cancel, errbuf, sizeof(errbuf));
            PQfreeCancel(cancel);
        }
    }

    /* Clear password related memory to avoid leaks when exit. */
    char* pqpass = PQpass(AH->connection);
    if (pqpass != NULL) {
        errno_t rc = memset_s(pqpass, strlen(pqpass), 0, strlen(pqpass));
        securec_check_c(rc, "\0", "\0");
    }

    PQfinish(AH->connection); /* noop if AH->connection is NULL */
    AH->connection = NULL;
}

PGconn* GetConnection(Archive* AHX)
{
    ArchiveHandle* AH = (ArchiveHandle*)AHX;

    return AH->connection;
}

static void notice_processor(void* arg, const char* message)
{
    write_msg(NULL, "%s", message);
}

/* Like exit_horribly(), but with a complaint about a particular query. */
static void die_on_query_failure(ArchiveHandle* AH, const char* pchModulename, const char* query)
{
    write_msg(pchModulename, "query failed: %s", PQerrorMessage(AH->connection));
    exit_horribly(pchModulename, "query was: %s\n", query);
}

void ExecuteSqlStatement(Archive* AHX, const char* query)
{
    ArchiveHandle* AH = (ArchiveHandle*)AHX;
    PGresult* res = NULL;

    res = PQexec(AH->connection, query);
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
        die_on_query_failure(AH, modulename, query);
    PQclear(res);
}

PGresult* ExecuteSqlQuery(Archive* AHX, const char* query, ExecStatusType status)
{
    ArchiveHandle* AH = (ArchiveHandle*)AHX;
    PGresult* res = NULL;

    res = PQexec(AH->connection, query);
    if (PQresultStatus(res) != status)
        die_on_query_failure(AH, modulename, query);
    return res;
}

/*
 * Convenience function to send a query.
 * Monitors result to detect COPY statements
 */
static void ExecuteSqlCommand(ArchiveHandle* AH, const char* qry, const char* desc)
{
    PGconn* conn = AH->connection;
    PGresult* res = NULL;
    char errStmt[DB_MAX_ERR_STMT + 1] = {0};
    errno_t rc = 0;

#ifdef NOT_USED
    write_stderr("Executing: '%s'\n\n", qry);
#endif
    res = PQexec(conn, qry);

    switch (PQresultStatus(res)) {
        case PGRES_COMMAND_OK:
        case PGRES_TUPLES_OK:
        case PGRES_EMPTY_QUERY:
            /* A-OK */
            break;
        case PGRES_COPY_IN:
            /* Assume this is an expected result */
            AH->pgCopyIn = true;
            break;
        default:
            /* trouble */
            rc = strncpy_s(errStmt, DB_MAX_ERR_STMT + 1, qry, DB_MAX_ERR_STMT);
            securec_check_c(rc, "\0", "\0");
            if (errStmt[DB_MAX_ERR_STMT - 1] != '\0') {
                errStmt[DB_MAX_ERR_STMT - 4] = '.';
                errStmt[DB_MAX_ERR_STMT - 3] = '.';
                errStmt[DB_MAX_ERR_STMT - 2] = '.';
                errStmt[DB_MAX_ERR_STMT - 1] = '\0';
            }

            /* if --exit-on-error option is enables please clear the memory before exiting */
            if (AH->publicArc.exit_on_error)
                PQclear(res);

            warn_or_exit_horribly(AH, modulename, "%s: %s    Command was: %s\n", desc, PQerrorMessage(conn), errStmt);
            break;
    }

    PQclear(res);
}

/*
 * Process non-COPY table data (that is, INSERT commands).
 *
 * The commands have been run together as one long string for compressibility,
 * and we are receiving them in bufferloads with arbitrary boundaries, so we
 * have to locate command boundaries and save partial commands across calls.
 * All state must be kept in AH->sqlparse, not in local variables of this
 * routine.  We assume that AH->sqlparse was filled with zeroes when created.
 *
 * We have to lex the data to the extent of identifying literals and quoted
 * identifiers, so that we can recognize statement-terminating semicolons.
 * We assume that INSERT data will not contain SQL comments, E'' literals,
 * or dollar-quoted strings, so this is much simpler than a full SQL lexer.
 */
static void ExecuteInsertCommands(ArchiveHandle* AH, const char* buf, size_t bufLen)
{
    const char* qry = buf;
    const char* eos = buf + bufLen;

    /* initialize command buffer if first time through */
    if (AH->sqlparse.curCmd == NULL)
        AH->sqlparse.curCmd = createPQExpBuffer();

    for (; qry < eos; qry++) {
        char ch = *qry;

        /* For neatness, we skip any newlines between commands */
        if (!(ch == '\n' && AH->sqlparse.curCmd->len == 0))
            appendPQExpBufferChar(AH->sqlparse.curCmd, ch);

        switch (AH->sqlparse.state) {
            case SQL_SCAN: /* Default state == 0, set in _allocAH */
                if (ch == ';') {
                    /*
                     * We've found the end of a statement. Send it and reset
                     * the buffer.
                     */
                    ExecuteSqlCommand(AH, AH->sqlparse.curCmd->data, "could not execute query");
                    resetPQExpBuffer(AH->sqlparse.curCmd);
                } else if (ch == '\'') {
                    AH->sqlparse.state = SQL_IN_SINGLE_QUOTE;
                    AH->sqlparse.backSlash = false;
                } else if (ch == '"') {
                    AH->sqlparse.state = SQL_IN_DOUBLE_QUOTE;
                }
                break;

            case SQL_IN_SINGLE_QUOTE:
                /* We needn't handle '' specially */
                if (ch == '\'' && !AH->sqlparse.backSlash)
                    AH->sqlparse.state = SQL_SCAN;
                else if (ch == '\\' && !AH->publicArc.std_strings)
                    AH->sqlparse.backSlash = !AH->sqlparse.backSlash;
                else
                    AH->sqlparse.backSlash = false;
                break;

            case SQL_IN_DOUBLE_QUOTE:
                /* We needn't handle "" specially */
                if (ch == '"')
                    AH->sqlparse.state = SQL_SCAN;
                break;
            default:
                break;
        }
    }
}

/*
 * Print table name
 */
void PrintTblName(const char* buf, size_t bufLen)
{
    errno_t rc = 0;
    char* tableName = NULL;
    char* tmpStr = NULL;
    char* saveStr = NULL;
    bool isCompleted = false;
    const char* split = " ";

    tmpStr = (char*)pg_malloc(bufLen + 1);
    rc = memcpy_s(tmpStr, bufLen, buf, bufLen);
    securec_check_c(rc, "\0", "\0");
    tmpStr[bufLen] = '\0';

    tableName = strtok_r(tmpStr, split, &saveStr);
    while (tableName != NULL) {
        tableName = strtok_r(NULL, split, &saveStr);
        if ((isCompleted) && (tableName != NULL)) {
            write_msg(NULL, "table %s complete data imported !\n", tableName);
            break;
        }

        if (tableName == NULL) {
            break;
        }
        if (strcmp(tableName, "INTO") == 0) {
            isCompleted = true;
        }
    }

    free(tmpStr);
    tmpStr = NULL;
}

/*
 * Implement ahwrite() for direct-to-DB restore
 */
int ExecuteSqlCommandBuf(ArchiveHandle* AH, const char* buf, size_t bufLen)
{
    errno_t rc = 0;
    if (AH->outputKind == OUTPUT_COPYDATA) {
        /*
         * COPY data.
         *
         * We drop the data on the floor if libpq has failed to enter COPY
         * mode; this allows us to behave reasonably when trying to continue
         * after an error in a COPY command.
         */
        if (AH->pgCopyIn && PQputCopyData(AH->connection, buf, bufLen) <= 0)
            exit_horribly(modulename, "error returned by PQputCopyData: %s", PQerrorMessage(AH->connection));
    } else if (AH->outputKind == OUTPUT_OTHERDATA) {
        /*
         * Table data expressed as INSERT commands.
         */
        ExecuteInsertCommands(AH, buf, bufLen);

        PrintTblName(buf, bufLen);
    } else {
        /*
         * General SQL commands; we assume that commands will not be split
         * across calls.
         *
         * In most cases the data passed to us will be a null-terminated
         * string, but if it's not, we have to add a trailing null.
         */
        if (buf[bufLen] == '\0')
            ExecuteSqlCommand(AH, buf, "could not execute query");
        else {
            char* str = (char*)pg_malloc(bufLen + 1);

            rc = memcpy_s(str, bufLen, buf, bufLen);
            securec_check_c(rc, "\0", "\0");
            str[bufLen] = '\0';
            ExecuteSqlCommand(AH, str, "could not execute query");
            free(str);
            str = NULL;
        }
    }

    return 1;
}

/*
 * Terminate a COPY operation during direct-to-DB restore
 */
void EndDBCopyMode(ArchiveHandle* AH, TocEntry* te)
{
    if (AH->pgCopyIn) {
        PGresult* res = NULL;

        if (PQputCopyEnd(AH->connection, NULL) <= 0)
            exit_horribly(modulename, "error returned by PQputCopyEnd: %s", PQerrorMessage(AH->connection));

        /* Check command status and return to normal libpq state */
        res = PQgetResult(AH->connection);
        if (PQresultStatus(res) != PGRES_COMMAND_OK)
            warn_or_exit_horribly(
                AH, modulename, "COPY failed for table \"%s\": %s", te->tag, PQerrorMessage(AH->connection));
        PQclear(res);

        AH->pgCopyIn = false;
    }
}

void StartTransaction(ArchiveHandle* AH)
{
    ExecuteSqlCommand(AH, "START TRANSACTION", "could not start database transaction");
}

void CommitTransaction(ArchiveHandle* AH)
{
    ExecuteSqlCommand(AH, "COMMIT", "could not commit database transaction");
}

void DropBlobIfExists(ArchiveHandle* AH, Oid oid)
{
    /*
     * If we are not restoring to a direct database connection, we have to
     * guess about how to detect whether the blob exists.  Assume new-style.
     */
    if (AH->connection == NULL || PQserverVersion(AH->connection) >= 90000) {
        ahprintf(AH,
            "SELECT pg_catalog.lo_unlink(oid) "
            "FROM pg_catalog.pg_largeobject_metadata "
            "WHERE oid = '%u';\n",
            oid);
    } else {
        /* Restoring to pre-9.0 server, so do it the old way */
        ahprintf(AH,
            "SELECT CASE WHEN EXISTS("
            "SELECT 1 FROM pg_catalog.pg_largeobject WHERE loid = '%u'"
            ") THEN pg_catalog.lo_unlink('%u') END;\n",
            oid,
            oid);
    }
}

#ifdef DUMPSYSLOG
PGconn* get_dumplog_conn(
    const char* dbname, const char* pghost, const char* pgport, const char* username, const char* pwd)
{
    PGconn* conn = NULL;
    PGresult* res = NULL;
    int local_sversion = 0;
    int remote_sversion = 0;
    char* local_pversion = NULL;
    char* remote_pversion = NULL;
    const char* passwd = pwd;
    bool new_passwd = false;

    do {
#define DUMPLOG_PARAM_SIZE 8
        const char** keywords = (const char**)pg_malloc(DUMPLOG_PARAM_SIZE * sizeof(*keywords));
        const char** values = (const char**)pg_malloc(DUMPLOG_PARAM_SIZE * sizeof(*values));

        keywords[0] = "host";
        values[0] = pghost;
        keywords[1] = "port";
        values[1] = pgport;
        keywords[2] = "user";
        values[2] = username;
        keywords[3] = "password";
        values[3] = passwd;
        keywords[4] = "dbname";
        values[4] = dbname;
        keywords[5] = "dumpsyslog";
        values[5] = "true";
        keywords[6] = "fallback_application_name";
        values[6] = progname;
        keywords[7] = NULL;
        values[7] = NULL;

        new_passwd = false;
        conn = PQconnectdbParams(keywords, values, true);

        free(keywords);
        keywords = NULL;
        errno_t rc = memset_s(values, DUMPLOG_PARAM_SIZE * sizeof(*values), 0, DUMPLOG_PARAM_SIZE * sizeof(*values));
        securec_check_c(rc, "\0", "\0");
        free(values);
        values = NULL;

        if (CONNECTION_BAD == PQstatus(conn) && strstr(PQerrorMessage(conn), "password") && NULL == passwd) {
            PQfinish(conn);
            passwd = simple_prompt("Password: ", 100, false);
            if (passwd == NULL)
                exit_horribly(modulename, "out of memory\n");
            new_passwd = true;
        }
    } while (new_passwd);

    if (CONNECTION_BAD == PQstatus(conn))
        exit_horribly(modulename, "connection to database \"%s\" failed: %s", PQdb(conn), PQerrorMessage(conn));

    /* check for version mismatch */
    res = PQexec(conn, "IDENTIFY_VERSION");
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        PQclear(res);
        exit_horribly(modulename, "get dumpsyslog connect failed, could not get version result from server\n");
    }
    if (PQnfields(res) != 3 || PQntuples(res) != 1) {
        PQclear(res);
        exit_horribly(modulename, "get dumpsyslog connect failed, version result from server is not correct\n");
    }
    remote_sversion = atoi(PQgetvalue(res, 0, 0));
    local_sversion = PG_VERSION_NUM;
    remote_pversion = PQgetvalue(res, 0, 1);
    local_pversion = strdup(PG_PROTOCOL_VERSION);
    if (local_pversion == NULL) {
        PQclear(res);
        exit_horribly(modulename, "get dumpsyslog connect failed, could not get the local protocal version\n");
        return NULL;
    }

    if (local_sversion != remote_sversion ||
        strncmp(local_pversion, remote_pversion, strlen(PG_PROTOCOL_VERSION)) != 0) {
        PQclear(res);
        if (local_sversion != remote_sversion) {
            free(local_pversion);
            local_pversion = NULL;
            exit_horribly(
                modulename, "get dumpsyslog connect failed, local protocol version is not same to server version\n");
        } else {
            free(local_pversion);
            local_pversion = NULL;
            exit_horribly(
                modulename, "get dumpsyslog connect failed, local system version is not same to server version\n");
        }
    }
    free(local_pversion);
    local_pversion = NULL;
    return conn;
}
#endif
