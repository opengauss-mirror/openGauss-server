/* -------------------------------------------------------------------------
 *
 * streamutil.c - utility functions for pg_basebackup and pg_receivelog
 *
 * Author: Magnus Hagander <magnus@hagander.net>
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  src/bin/pg_basebackup/streamutil.c
 * -------------------------------------------------------------------------
 */

/*
 * We have to use postgres.h not postgres_fe.h here, because there's so much
 * backend-only stuff in the XLOG include files we need.  But we need a
 * frontend-ish environment otherwise.	Hence this ugly hack.
 */
#define FRONTEND 1
#include "postgres.h"
#include "knl/knl_variable.h"
#include "streamutil.h"
#include "libpq/libpq-int.h"

#include <stdio.h>
#include <string.h>

#include "bin/elog.h"
#include "logging.h"

PGconn* streamConn = NULL;
char* replication_slot = NULL;

char* register_username = NULL;
char* register_password = NULL;
char* dbport = NULL;
char* pg_host = NULL;
int dbgetpassword = 0; /* 0=auto, -1=never, 1=always */
static const int PASSWDLEN = 100;

/*
 * strdup() and malloc() replacements that prints an error and exits
 * if something goes wrong. Can never return NULL.
 */
char* xstrdup(const char* s)
{
    char* result = NULL;

    result = strdup(s);
    if (result == NULL) {
        pg_log(PG_PRINT, _("%s: out of memory\n"), progname);
        exit(1);
    }
    return result;
}

void* xmalloc0(int size)
{
    void* result = NULL;

    /* Avoid unportable behavior of malloc(0) */
    if (size == 0) {
        pg_log(PG_PRINT, _("%s: malloc 0\n"), progname);
        exit(1);
    }
    result = malloc(size);
    if (result == NULL) {
        pg_log(PG_PRINT, _("%s: out of memory\n"), progname);
        exit(1);
    }
    errno_t errorno = memset_s(result, (size_t)size, 0, (size_t)size);
    securec_check_c(errorno, "", "");
    return result;
}

static void CalculateArgCount(int *argcount)
{
    if (pg_host != NULL)
        (*argcount)++;
    if (register_username != NULL)
        (*argcount)++;
    if (dbport != NULL)
        (*argcount)++;

    return;
}


void ClearAndFreePasswd(void)
{
    if (register_password != nullptr) {
        errno_t errorno = memset_s(register_password, PASSWDLEN + 1, '\0', PASSWDLEN + 1);
        securec_check_c(errorno, "\0", "\0");
        free(register_password);
        register_password = nullptr;
    }
}

/* the dbport + 1 is used for WalSender */
char* inc_dbport(const char* db_port)
{
    int p = atoi(db_port);
    if (p <= 0) {
        pg_fatal(_("invalid port number \"%s\"\n"), db_port);
        return NULL;
    }

#define MAX_INT32_BUFF 20
    char* strport = (char*)xmalloc0(MAX_INT32_BUFF);
    errno_t rc = sprintf_s(strport, MAX_INT32_BUFF - 1, "%d", p + 1);
    securec_check_ss_c(rc, "", "");

    return strport;
}


/*
 * Connect to the server. Returns a valid PGconn pointer if connected,
 * or NULL on non-permanent error. On permanent error, the function will
 * call exit(1) directly. You should call ClearAndFreePasswd() outside
 * if password it is not needed anymore.
 */
PGconn* GetConnection(void)
{
    PGconn* tmpconn = NULL;
    int argcount = 6; /* dbname, fallback_app_name, connect_time, rw_timeout, pwd */
    const char** keywords;
    const char** values;
    const char* tmpparam = NULL;

    CalculateArgCount(&argcount);

    keywords = (const char**)xmalloc0((argcount + 1) * sizeof(*keywords));
    values = (const char**)xmalloc0((argcount + 1) * sizeof(*values));

    keywords[0] = "dbname";
    values[0] = "replication";
    keywords[1] = "replication";
    values[1] = "true";
    keywords[2] = "fallback_application_name";
    values[2] = progname;
    keywords[3] = "connect_timeout";  /* param connect_time   */
    values[3] = "120";                  /* default connect_time */
    keywords[4] = "rw_timeout";       /* param rw_timeout     */
    values[4] = "120";         /* rw_timeout value     */
    int i = 5;
    if (pg_host != NULL) {
        keywords[i] = "host";
        values[i] = pg_host;
        i++;
    }
    if (register_username != NULL) {
        keywords[i] = "user";
        values[i] = register_username;
        i++;
    }
    if (dbport != NULL) {
        keywords[i] = "port";
        values[i] = dbport;
        i++;
    }

    while (true) {
        if (register_password != NULL) {
            /*
             * We've saved a pwd when a previous connection succeeded,
             * meaning this is the call for a second session to the same
             * database, so just forcibly reuse that pwd.
             */
            keywords[argcount - 1] = "password";
            values[argcount - 1] = register_password;
            dbgetpassword = -1; /* Don't try again if this fails */
        } else if (dbgetpassword == 1) {
            register_password = simple_prompt(_("Password: "), PASSWDLEN, false);
            keywords[argcount - 1] = "password";
            values[argcount - 1] = register_password;
            dbgetpassword = -1;
        }

        tmpconn = PQconnectdbParams(keywords, values, true);
        /*
         * If there is too little memory even to allocate the PGconn object
         * and PQconnectdbParams returns NULL, we call exit(1) directly.
         */
        if (tmpconn == NULL) {
            fprintf(stderr, _("%s: could not connect to server\n"), progname);
            exit(1);
        }

        if (PQstatus(tmpconn) == CONNECTION_BAD && PQconnectionNeedsPassword(tmpconn) && dbgetpassword != -1) {
            dbgetpassword = 1; /* ask for password next time */
            ClearAndFreePasswd();
            PQfinish(tmpconn);
            tmpconn = NULL;
            continue;
        }

        if (PQstatus(tmpconn) != CONNECTION_OK) {
            fprintf(stderr, _("%s: could not connect to server: %s\n"), progname, PQerrorMessage(tmpconn));
            PQfinish(tmpconn);
            tmpconn = NULL;
            ClearAndFreePasswd();
            free(values);
            values = NULL;
            free(keywords);
            keywords = NULL;
            return NULL;
        }

        free(values);
        values = NULL;
        free(keywords);
        keywords = NULL;

        /*
         * Ensure we have the same value of integer timestamps as the server
         * we are connecting to.
         */
        tmpparam = PQparameterStatus(tmpconn, "integer_datetimes");
        if (tmpparam == NULL) {
            fprintf(stderr, _("%s: could not determine server setting for integer_datetimes\n"), progname);
            PQfinish(tmpconn);
            tmpconn = NULL;
            exit(1);
        }

#ifdef HAVE_INT64_TIMESTAMP
        if (strcmp(tmpparam, "on") != 0)
#else
        if (strcmp(tmpparam, "off") != 0)
#endif
        {
            fprintf(stderr, _("%s: integer_datetimes compile flag does not match server\n"), progname);
            PQfinish(tmpconn);
            tmpconn = NULL;
            exit(1);
        }
        return tmpconn;
    }
}

bool exe_sql(PGconn* Conn, const char* sqlCommond)
{
    PGresult* Ha_result = NULL;
    int numRows = 0;
    int maxRows = 0;
    int numColums = 0;
    int maxColums = 0;

    Ha_result = PQexec(Conn, sqlCommond);
    if ((PGRES_COMMAND_OK == PQresultStatus(Ha_result)) || (PGRES_TUPLES_OK == PQresultStatus(Ha_result))) {
        maxRows = PQntuples(Ha_result);
        maxColums = PQnfields(Ha_result);

        /* No results */
        if (maxRows == 0) {
            pg_log(PG_PRINT, "No information \n");
        } else { /* Print query results */
            for (numRows = 0; numRows < maxRows; numRows++) {
                for (numColums = 0; numColums < maxColums; numColums++) {
                    write_stderr(
                        "	%-30s : %s\n", PQfname(Ha_result, numColums), PQgetvalue(Ha_result, numRows, numColums));
                }
                pg_log(PG_PRINT, "\n");
            }
        }
    } else {
        pg_log(PG_WARNING, "exc_sql failed.\n");
        PQclear(Ha_result);
        return false;
    }

    PQclear(Ha_result);
    return true;
}

void update_obs_build_status(const char* build_status)
{
    int ret = 0;
    char sql_cmd[MAXPGPATH] = {0};

    ret = snprintf_s(sql_cmd, MAXPGPATH, MAX_PATH_LEN - 1,
        "select * from pg_catalog.gs_set_obs_file_context('%s/%s', '%s', '%s')",
        pgxc_node_name, taskid, build_status, slotname);
    securec_check_ss_c(ret, "\0", "\0");
    if (exe_sql(dbConn, sql_cmd) == false) {
        pg_log(PG_WARNING, _("failed to execute sql [%s]!\n"), sql_cmd);
    }
    return;
}
