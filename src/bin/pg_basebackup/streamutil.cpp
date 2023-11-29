/* -------------------------------------------------------------------------
 *
 * streamutil.c - utility functions for pg_basebackup and pg_receivelog
 *
 * Author: Magnus Hagander <magnus@hagander.net>
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
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
#include "utils/timestamp.h"
#include "streamutil.h"
#include "libpq/libpq-int.h"
#include <sys/time.h>
#include <stdio.h>
#include <string.h>

const char* progname;
char* dbhost = NULL;
char* dbuser = NULL;
char* dbport = NULL;
char* dbname = NULL;
int   rwtimeout = 0;
int dbgetpassword = 0; /* 0=auto, -1=never, 1=always */
char* dbpassword = NULL;
PGconn* conn = NULL;
int standby_message_timeout = 10 * 1000; /* 10 sec = default */
static const int PASSWDLEN = 100;

PGconn* streamConn = NULL;
char* replication_slot = NULL;

/*
 * tblspaceDirectory is used for saving the table space directory created by
 * full-build. tblspaceNum is the count of table space. The table space directory
 * should be empty before executing full-build, so when full-build failed, all
 * the directories should be removed. If not so ,the next full-build maybe fail
 * because some directory is not empty.
 */
char** tblspaceDirectory = NULL;
int tblspaceCount = 0;
int tblspaceIndex = 0;

void removeCreatedTblspace(void)
{
    if (tblspaceDirectory != NULL) {
        int k;
        for (k = 0; k < tblspaceCount; k++) {
            if (tblspaceDirectory[k] != NULL) {
                rmtree(tblspaceDirectory[k], true);
            }
        }
    }
}

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

void ClearAndFreePasswd(void)
{
    if (dbpassword != nullptr) {
        errno_t errorno = memset_s(dbpassword, sizeof(dbpassword), '\0', sizeof(dbpassword));
        securec_check_c(errorno, "\0", "\0");
        free(dbpassword);
        dbpassword = nullptr;
    }
}

static void CalculateArgCount(int *argcount)
{
    if (dbhost != NULL)
        (*argcount)++;
    if (dbuser != NULL)
        (*argcount)++;
    if (dbport != NULL)
        (*argcount)++;

    return;
}

static void CheckConnectionHost(PGconn* tmpconn)
{
    if (tmpconn == NULL || tmpconn->sock < 0) {
        fprintf(stderr, "failed to connect %s:%s.\n", 
            (dbhost == NULL) ? "Unknown" : dbhost,
            (dbport == NULL) ? "Unknown" : dbport);
        PQfinish(tmpconn);
        exit(1);
    }
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
    errno_t rc = EOK;
    char rwtimeoutStr[12] = {0};

    CalculateArgCount(&argcount);

    keywords = (const char**)xmalloc0((argcount + 1) * sizeof(*keywords));
    values = (const char**)xmalloc0((argcount + 1) * sizeof(*values));

    keywords[0] = "dbname";
    values[0] = dbname == NULL ? "replication" : dbname;
    keywords[1] = "replication";
    values[1] = dbname == NULL ? "true" : "database";
    keywords[2] = "fallback_application_name";
    values[2] = progname;
    keywords[3] = "connect_timeout";  /* param connect_time   */
    values[3] = "120";                  /* default connect_time */

    rc = snprintf_s(rwtimeoutStr, sizeof(rwtimeoutStr), sizeof(rwtimeoutStr) - 1, "%d",
                    rwtimeout ? rwtimeout : 120); /* default rw_timeout 120 */
    securec_check_ss_c(rc, "", "");

    keywords[4] = "rw_timeout";       /* param rw_timeout     */
    values[4] = rwtimeoutStr;         /* rw_timeout value     */
    int i = 5;
    if (dbhost != NULL) {
        keywords[i] = "host";
        values[i] = dbhost;
        i++;
    }
    if (dbuser != NULL) {
        keywords[i] = "user";
        values[i] = dbuser;
        i++;
    }
    if (dbport != NULL) {
        keywords[i] = "port";
        values[i] = dbport; // inc_dbport(dbport);
        i++;
    }

    while (true) {
        if (dbpassword != NULL) {
            /*
             * We've saved a pwd when a previous connection succeeded,
             * meaning this is the call for a second session to the same
             * database, so just forcibly reuse that pwd.
             */
            keywords[argcount - 1] = "password";
            values[argcount - 1] = dbpassword;
            dbgetpassword = -1; /* Don't try again if this fails */
        } else if (dbgetpassword == 1) {
            dbpassword = simple_prompt(_("Password: "), PASSWDLEN, false);
            keywords[argcount - 1] = "password";
            values[argcount - 1] = dbpassword;
            dbgetpassword = -1;
        }

        tmpconn = PQconnectdbParams(keywords, values, true);
        /*
         * If there is too little memory even to allocate the PGconn object
         * and PQconnectdbParams returns NULL, we call exit(1) directly.
         */
        CheckConnectionHost(tmpconn);
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

/*
 * Frontend version of GetCurrentTimestamp(), since we are not linked with
 * backend code.
 */
TimestampTz feGetCurrentTimestamp(void)
{
    TimestampTz result;
    struct timeval tp;

    gettimeofday(&tp, NULL);

    result = (TimestampTz)tp.tv_sec - ((POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY);

#ifdef HAVE_INT64_TIMESTAMP
    result = (result * USECS_PER_SEC) + tp.tv_usec;
#else
    result = result + (tp.tv_usec / 1000000.0);
#endif

    return result;
}

/*
 * Frontend version of TimestampDifference(), since we are not
 * linked with backend code.
 */
void feTimestampDifference(TimestampTz start_time, TimestampTz stop_time, long* secs, long* microsecs)
{
    TimestampTz diff = stop_time - start_time;

    if (diff <= 0) {
        *secs = 0;
        *microsecs = 0;
    } else {
#ifdef HAVE_INT64_TIMESTAMP
        *secs = (long)(diff / USECS_PER_SEC);
        *microsecs = (long)(diff % USECS_PER_SEC);
#else
        *secs = (long)diff;
        *microsecs = (long)((diff - *secs) * 1000000.0);
#endif
    }
}

/*
 * Frontend version of TimestampDifferenceExceeds(), since we are not
 * linked with backend code.
 */
bool feTimestampDifferenceExceeds(TimestampTz start_time, TimestampTz stop_time, int msec)
{
    TimestampTz diff = stop_time - start_time;

#ifdef HAVE_INT64_TIMESTAMP
    return (diff >= msec * INT64CONST(1000));
#else
    return (diff * 1000.0 >= msec);
#endif
}
