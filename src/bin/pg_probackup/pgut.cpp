/*-------------------------------------------------------------------------
 *
 * pgut.c
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2009-2013, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 * Portions Copyright (c) 2017-2019, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */

#include "pg_probackup.h"
#include "postgres_fe.h"

#include "getopt_long.h"
#include "libpq/libpq-fe.h"
#include "libpq/pqsignal.h"
#include "libpq/pqexpbuffer.h"
#include "common/fe_memutils.h"

#include <time.h>

#include "pgut.h"
#include "logger.h"
#include "file.h"

bool    prompt_password = true;

/* Database connections */
static PGcancel *volatile cancel_conn = NULL;

/* Interrupted by SIGINT (Ctrl+C) ? */
bool    interrupted = false;
bool    in_cleanup = false;
bool    in_password = false;

/* Connection routines */
static void init_cancel_handler(void);
static void on_before_exec(PGconn *conn, PGcancel *thread_cancel_conn);
static void on_after_exec(PGcancel *thread_cancel_conn);
static void on_interrupt(void);
void on_cleanup(void);
static pqsigfunc oldhandler = NULL;

static char ** pgut_pgfnames(const char *path, bool strict);
static void pgut_pgfnames_cleanup(char **filenames);

void discard_response(PGconn *conn);
static PGresult *pgut_async_query(PGconn *conn, const char *query,
                                  int nParams, const char **params,
                                  bool text_result);
extern char* inc_dbport(const char* dbport);
/* is not used now  #define DefaultHost		"localhost"   may be remove in the future*/
#define DefaultTty          ""
#define DefaultOption       ""
#define DefaultAuthtype         ""
/* is not used now #define DefaultTargetSessionAttrs	"any"  may be removed in the future*/
#ifdef USE_SSL
#define DefaultSSLMode "prefer"
#else
#define DefaultSSLMode      "disable"
#endif
/* is not used now #ifdef ENABLE_GSS
#define DefaultGSSMode "prefer"
#else
#define DefaultGSSMode "disable"
#endif  may be removed in the future*/

typedef struct _internalPQconninfoOption
{
    const char     *keyword;    /* The keyword of the option			*/
    const char     *envvar; /* Fallback environment variable name	*/
    const char    *compiled;   /* Fallback compiled in default value	*/
    char    *val;    /* Option's current value, or NULL		 */
    const  char    *label;  /* Label for field in connect dialog	*/
    const  char     *dispchar;   /* Indicates how to display this field in a */
    int      dispsize;    /* Field size in characters for dialog	*/
    off_t    connofs;  /* Offset into PGconn struct, -1 if not there */
} internalPQconninfoOption;

static const internalPQconninfoOption PQconninfoOptions[] = {
    /*
    *   * "authtype" is no longer used, so mark it "don't show".  We keep it in
    *    * the array so as not to reject conninfo strings from old apps that might
    *    * still try to set it.
    *   */
    {(const char *)"authtype", (const char *)"PGAUTHTYPE", (const char *)DefaultAuthtype, NULL,
        (const char *)"Database-Authtype", (const char *)"D", 20, -1},

    {(const char *)"service", (const char *)"PGSERVICE", NULL, NULL,
        (const char *)"Database-Service", (const char *)"", 20, -1},

    {(const char *)"user", (const char *)"PGUSER", NULL, NULL,
        (const char *)"Database-User", (const char *)"", 20,
    offsetof(struct pg_conn, pguser)},

    {(const char *)"password", (const char *)"PGPASSWORD", NULL, NULL,
        (const char *)"Database-Password", (const char *)"*", 20,
    offsetof(struct pg_conn, pgpass)},


    {(const char *)"connect_timeout", (const char *)"PGCONNECT_TIMEOUT", NULL, NULL,
        (const char *)"Connect-timeout", (const char *)"", 10,	/* strlen(INT32_MAX) == 10 */
    offsetof(struct pg_conn, connect_timeout)},

    {(const char *)"dbname", (const char *)"PGDATABASE", NULL, NULL,
        (const char *)"Database-Name", (const char *)"", 20,
    offsetof(struct pg_conn, dbName)},

    {(const char *)"host", (const char *)"PGHOST", NULL, NULL,
        (const char *)"Database-Host", (const char *)"", 40,
    offsetof(struct pg_conn, pghost)},

    {(const char *)"hostaddr", (const char *)"PGHOSTADDR", NULL, NULL,
        (const char *)"Database-Host-IP-Address", (const char *)"", 45,
    offsetof(struct pg_conn, pghostaddr)},

    {(const char *)"port", (const char *)"PGPORT", (const char *)DEF_PGPORT_STR, NULL,
        (const char *)"Database-Port", (const char *)"", 6,
    offsetof(struct pg_conn, pgport)},

    {(const char *)"client_encoding", (const char *)"PGCLIENTENCODING", NULL, NULL,
        (const char *)"Client-Encoding", (const char *)"", 10,
    offsetof(struct pg_conn, client_encoding_initial)},

    /*
    *   * "tty" is no longer used either, but keep it present for backwards
    *   * compatibility.
    *   */
    {(const char *)"tty", (const char *)"PGTTY", (const char *)DefaultTty, NULL,
        (const char *)"Backend-Debug-TTY", (const char *)"D", 40,
    offsetof(struct pg_conn, pgtty)},

    {(const char *)"options", (const char *)"PGOPTIONS", (const char *)DefaultOption, NULL,
        (const char *)"Backend-Options", (const char *)"", 40,
    offsetof(struct pg_conn, pgoptions)},

    {(const char *)"application_name", (const char *)"PGAPPNAME", NULL, NULL,
        (const char *)"Application-Name", (const char *)"", 64,
    offsetof(struct pg_conn, appname)},

    {(const char *)"fallback_application_name", NULL, NULL, NULL,
        (const char *)"Fallback-Application-Name", (const char *)"", 64,
    offsetof(struct pg_conn, fbappname)},

    {(const char *)"keepalives", NULL, NULL, NULL,
        (const char *)"TCP-Keepalives", (const char *)"", 1,	/* should be just '0' or '1' */
    offsetof(struct pg_conn, keepalives)},

    {(const char *)"keepalives_idle", NULL, NULL, NULL,
        (const char *)"TCP-Keepalives-Idle", (const char *)"", 10,	/* strlen(INT32_MAX) == 10 */
    offsetof(struct pg_conn, keepalives_idle)},

    {(const char *)"keepalives_interval", NULL, NULL, NULL,
        (const char *)"TCP-Keepalives-Interval", (const char *)"", 10,	/* strlen(INT32_MAX) == 10 */
    offsetof(struct pg_conn, keepalives_interval)},

    {(const char *)"keepalives_count", NULL, NULL, NULL,
        (const char *)"TCP-Keepalives-Count", (const char *)"", 10, /* strlen(INT32_MAX) == 10 */
    offsetof(struct pg_conn, keepalives_count)},

    /*
    *   * ssl options are allowed even without client SSL support because the
    *   * client can still handle SSL modes "disable" and "allow". Other
    *    * parameters have no effect on non-SSL connections, so there is no reason
    *   * to exclude them since none of them are mandatory.
    *    */
    {(const char *)"sslmode", (const char *)"PGSSLMODE", (const char *)DefaultSSLMode, NULL,
        (const char *)"SSL-Mode", (const char *)"", 12,		/* sizeof("verify-full") == 12 */
    offsetof(struct pg_conn, sslmode)},

    {(const char *)"sslcompression", (const char *)"PGSSLCOMPRESSION", (const char *)"0", NULL,
        (const char *)"SSL-Compression", (const char *)"", 1,
    offsetof(struct pg_conn, sslcompression)},

    {(const char *)"sslcert", (const char *)"PGSSLCERT", NULL, NULL,
        (const char *)"SSL-Client-Cert", (const char *)"", 64,
    offsetof(struct pg_conn, sslcert)},

    {(const char *)"sslkey", (const char *)"PGSSLKEY", NULL, NULL,
        (const char *)"SSL-Client-Key", (const char *)"", 64,
    offsetof(struct pg_conn, sslkey)},

    {(const char *)"sslrootcert", (const char *)"PGSSLROOTCERT", NULL, NULL,
        (const char *)"SSL-Root-Certificate", (const char *)"", 64,
    offsetof(struct pg_conn, sslrootcert)},

    {(const char *)"sslcrl", (const char *)"PGSSLCRL", NULL, NULL,
        (const char *)"SSL-Revocation-List", (const char *)"", 64,
    offsetof(struct pg_conn, sslcrl)},

    {(const char *)"requirepeer", (const char *)"PGREQUIREPEER", NULL, NULL,
        (const char *)"Require-Peer", (const char *)"", 10,
    offsetof(struct pg_conn, requirepeer)},

    /*
    *   * As with SSL, all GSS options are exposed even in builds that don't have
    *    * support.
    *   */

#ifdef KRB5
    /* Kerberos and GSSAPI authentication support specifying the service name */
    {(const char *)"krbsrvname", (const char *)"PGKRBSRVNAME", (const char *)PG_KRB_SRVNAM, NULL,
        (const char *)"Kerberos-service-name", (const char *)"", 20,
    offsetof(struct pg_conn, krbsrvname)},
#endif

    {(const char *)"replication", NULL, NULL, NULL,
        (const char *)"Replication", (const char *)"D", 5,
    offsetof(struct pg_conn, replication)},


    /* Terminating entry --- MUST BE LAST */
    {NULL, NULL, NULL, NULL,
    NULL, NULL, 0}
};


/*
 *  * Build a working copy of the constant PQconninfoOptions array.
 *   */
static PQconninfoOption *
conninfo_init(PQExpBuffer errorMessage)
{
    PQconninfoOption *options;
    PQconninfoOption *opt_dest;
    const internalPQconninfoOption *cur_opt;
    size_t size = sizeof(PQconninfoOption) * sizeof(PQconninfoOptions) / sizeof(PQconninfoOptions[0]);
    errno_t rc = 0;

    /*
    *   * Get enough memory for all options in PQconninfoOptions, even if some
    *   * end up being filtered out.
    *   */
    options = (PQconninfoOption *) malloc(size);
    if (options == NULL)
    {
        printfPQExpBuffer(errorMessage,
            libpq_gettext("out of memory\n"));
        return NULL;
    }
    opt_dest = options;

    for (cur_opt = PQconninfoOptions; cur_opt->keyword; cur_opt++)
    {
        /* Only copy the public part of the struct, not the full internal */
        rc = memcpy_s(opt_dest, size, cur_opt, sizeof(PQconninfoOption));
        securec_check_c(rc, "\0", "\0");
        opt_dest++;
        size = size - sizeof(PQconninfoOption);
    }
    rc = memset_s(opt_dest, size, 0, sizeof(PQconninfoOption));
    securec_check(rc, "\0", "\0");

    return options;
}

static bool
get_hexdigit(char digit, int *value)
{
    if ('0' <= digit && digit <= '9')
        *value = digit - '0';
    else if ('A' <= digit && digit <= 'F')
        *value = digit - 'A' + 10;
    else if ('a' <= digit && digit <= 'f')
        *value = digit - 'a' + 10;
    else
        return false;

    return true;
}


static char *
conninfo_uri_decode(const char *str, PQExpBuffer errorMessage)
{
    char    *buf;
    char    *p;
    const char *q = str;

    buf = (char *)malloc(strlen(str) + 1);
    if (buf == NULL)
    {
        printfPQExpBuffer(errorMessage, libpq_gettext("out of memory\n"));
        return NULL;
    }
    p = buf;

    for (;;)
    {
        if (*q != '%')
        {
            /* copy and check for NUL terminator */
            if (!(*(p++) = *(q++)))
                break;
        }
        else
        {
            int     hi;
            int     lo;
            int     c;

            ++q;    /* skip the percent sign itself */

            /*
            *   * Possible EOL will be caught by the first call to
            *   * get_hexdigit(), so we never dereference an invalid q pointer.
            *   */
            if (!(get_hexdigit(*q++, &hi) && get_hexdigit(*q++, &lo)))
            {
                printfPQExpBuffer(errorMessage,
                                                libpq_gettext("invalid percent-encoded token: \"%s\"\n"),
                                                str);
                free(buf);
                return NULL;
            }

            c = (hi << 4) | lo;
            if (c == 0)
            {
                printfPQExpBuffer(errorMessage,
                                                libpq_gettext("forbidden value %%00 in percent-encoded value: \"%s\"\n"),
                                                str);
                free(buf);
                return NULL;
            }
            *(p++) = c;
        }
    }

    return buf;
}


static PQconninfoOption *
conninfo_find(PQconninfoOption *connOptions, const char *keyword)
{
    PQconninfoOption *option;

    for (option = connOptions; option->keyword != NULL; option++)
    {
        if (strcmp(option->keyword, keyword) == 0)
            return option;
    }

    return NULL;
}


static PQconninfoOption *
conninfo_storeval(PQconninfoOption *connOptions,
                                    const char *keyword, const char *value,
                                    PQExpBuffer errorMessage, bool ignoreMissing,
                                    bool uri_decode)
{
    PQconninfoOption *option;
    char    *value_copy;

    /*
    *   * For backwards compatibility, requiressl=1 gets translated to
    *    * sslmode=require, and requiressl=0 gets translated to sslmode=prefer
    *   * (which is the default for sslmode).
    *   */
    if (strcmp(keyword, "requiressl") == 0)
    {
        keyword = "sslmode";
        if (value[0] == '1')
            value = "require";
        else
            value = "prefer";
    }

    option = conninfo_find(connOptions, keyword);
    if (option == NULL)
    {
        if (!ignoreMissing)
            printfPQExpBuffer(errorMessage,
                libpq_gettext("invalid connection option \"%s\"\n"),
                keyword);
        return NULL;
    }

    if (uri_decode)
    {
        value_copy = conninfo_uri_decode(value, errorMessage);
        if (value_copy == NULL)
            /* conninfo_uri_decode already set an error message */
            return NULL;
    }
    else
    {
        value_copy = strdup(value);
        if (value_copy == NULL)
        {
            printfPQExpBuffer(errorMessage, libpq_gettext("out of memory\n"));
            return NULL;
        }
    }

    if (option->val)
        free(option->val);
    option->val = value_copy;

    return option;
}


/*
 *  * Return the connection options used for the connection
 *   */
PQconninfoOption *
PQconninfo(PGconn *conn)
{
    PQExpBufferData errorBuf;
    PQconninfoOption *connOptions;

    if (conn == NULL)
        return NULL;

    /* We don't actually report any errors here, but callees want a buffer */
    initPQExpBuffer(&errorBuf);
    if (PQExpBufferDataBroken(errorBuf))
        return NULL;    /* out of memory already :-( */

    connOptions = conninfo_init(&errorBuf);

    if (connOptions != NULL)
    {
        const internalPQconninfoOption *option;

        for (option = PQconninfoOptions; option->keyword; option++)
        {
            char    **connmember;

            if (option->connofs < 0)
                continue;

            connmember = (char **) ((char *) conn + option->connofs);

            if (*connmember)
                conninfo_storeval(connOptions, option->keyword, *connmember,
                    &errorBuf, true, false);
        }
    }

    termPQExpBuffer(&errorBuf);

    return connOptions;
}

void
pgut_init(void)
{
    init_cancel_handler();
}

/*
 * Ask the user for a password; 'username' is the username the
 * password is for, if one has been explicitly specified.
 * Set malloc'd string to the global variable 'password'.
 */
static void
prompt_for_password(const char *username)
{
    in_password = true;
    int nRet = 0;

    if (password)
    {
        free(password);
        password = NULL;
    }

#if PG_VERSION_NUM >= 100000
    password = (char *) pgut_malloc(sizeof(char) * 100 + 1);
    if (username == NULL)
        simple_prompt("Password: ", password, 100, false);
    else
    {
        char    message[256];
        nRet = snprintf_s(message, lengthof(message), lengthof(message) - 1, "Password for user %s: ", username);
        securec_check_ss_c(nRet, "\0", "\0");
        simple_prompt(message, password, 100, false);
    }
#else
    if (username == NULL)
        password = simple_prompt("Password: ", 100, false);
    else
    {
        char    message[256];
        nRet = snprintf_s(message, lengthof(message), lengthof(message) - 1, "Password for user %s: ", username);
        securec_check_ss_c(nRet, "\0", "\0");
        password = simple_prompt(message, 100, false);
    }
#endif

    in_password = false;
}


PGconn* pgut_connect(const char *host, const char *port,
                     const char *dbname, const char *username)
{
    PGconn       *conn;
    int        argcount = 8;    /* dbname, fallback_app_name,connect_timeout,rw_timeout
                                         * host, user, port, password */
    int            i;
    const char **keywords;
    const char **values;
    errno_t rc = EOK;
    char rwtimeoutStr[12] = {0};

    if (interrupted && !in_cleanup)
        elog(ERROR, "interrupted");

    i = 0;

    keywords = (const char**)pg_malloc0((argcount + 1) * sizeof(*keywords));
    values = (const char**)pg_malloc0((argcount + 1) * sizeof(*values));

    keywords[i] = "dbname";                        /* param db name */
    values[i] = dbname;
    i++;
    keywords[i] = "fallback_application_name";     /* app name      */
    values[i] = PROGRAM_NAME;
    i++;
    keywords[i] = "connect_timeout";               /* param connect timeout */
    values[i] = "120";                               /* default value         */
    i++;

    rc = snprintf_s(rwtimeoutStr, sizeof(rwtimeoutStr), sizeof(rwtimeoutStr) - 1, "%d",
                    rw_timeout ? rw_timeout : 120);  /* default rw_timeout 120 */
    securec_check_ss_c(rc, "", "");
    keywords[i] = "rw_timeout";                    /* param rw_timeout      */
    values[i] = rwtimeoutStr;                      /* rw_timeout value      */
    i++;

    if (host)
    {
        keywords[i] = "host";                  /* param host            */
        values[i] = host;
        i++;
    }
    if (username)
    {
        keywords[i] = "user";                 /* param user             */
        values[i] = username;
        i++;
    }
    if (port)
    {
        keywords[i] = "port";                /* param port             */
        values[i] = port;
        i++;
    }

    /* Use (or reuse, on a subsequent connection) password if we have it */
    if (password)
    {
        keywords[i] = "password";           /* param password          */
        values[i] = password;
    }
    else
    {
        keywords[i] = NULL;
        values[i] = NULL;
    }

    /* Start the connection. Loop until we have a password if requested by backend. */
    for (;;)
    {
        errno_t err = EOK;
        size_t size = (argcount + 1) * sizeof(*values);
        conn = PQconnectdbParams(keywords, values, true);

        if (PQstatus(conn) == CONNECTION_OK)
        {
            pgut_atexit_push(pgut_disconnect_callback, conn);
            err = memset_s(values, size, 0, size);
            securec_check_c(err, "\0", "\0");
            pfree(values);
            pfree(keywords);
            return conn;
        }

        if (conn && PQconnectionNeedsPassword(conn) && prompt_password)
        {
            PQfinish(conn);
            prompt_for_password(username);

            if (interrupted)
                elog(ERROR, "interrupted");

            if (password == NULL || password[0] == '\0')
                elog(ERROR, "no password supplied");
            keywords[i] = "password";
            values[i] = password;
            continue;
        }
        elog(ERROR, "could not connect to database %s: %s",
             dbname, PQerrorMessage(conn));

        PQfinish(conn);
        err = memset_s(values, size, 0, size);
        securec_check_c(err, "\0", "\0");
        pfree(values);
        pfree(keywords);
        return NULL;
    }
}

PGconn* pgut_connect_replication(const char *host, const char *port,
                                                                const char *dbname, const char *username)
{
    PGconn  *tmpconn;
    int             argcount = 9;   /* dbname, replication, fallback_app_name,connect_timeout rw_timeout
                                     * host, user, port, password */
    int                 i;
    const char **keywords;
    const char **values;
    errno_t rc = EOK;
    char rwtimeoutStr[12] = {0};
    const char *malloc_port = NULL;

    if (interrupted && !in_cleanup)
        elog(ERROR, "interrupted");

    i = 0;

    keywords = (const char**)pg_malloc0((argcount + 1) * sizeof(*keywords));
    values = (const char**)pg_malloc0((argcount + 1) * sizeof(*values));


    keywords[i] = "dbname";
    values[i] = "replication";
    i++;
    keywords[i] = "replication";
    values[i] = "true";
    i++;
    keywords[i] = "fallback_application_name";
    values[i] = PROGRAM_NAME;
    i++;
    keywords[i] = "connect_timeout";                /* param connect timeout */
    values[i] = "120";                                /* default value         */
    i++;

    rc = snprintf_s(rwtimeoutStr, sizeof(rwtimeoutStr), sizeof(rwtimeoutStr) - 1, "%d",
                    rw_timeout ? rw_timeout : 120); /* default rw_timeout 120 */
    securec_check_ss_c(rc, "", "");
    keywords[i] = "rw_timeout";                     /* param rw_timeout      */
    values[i] = rwtimeoutStr;                       /* rw_timeout value  */
    i++;

    if (host)
    {
        keywords[i] = "host";
        values[i] = host;
        i++;
    }
    if (username)
    {
        keywords[i] = "user";
        values[i] = username;
        i++;
    }
    if (port)
    {
        keywords[i] = "port";
        values[i] = inc_dbport(port);
        malloc_port = values[i];
        i++;
    }

    /* Use (or reuse, on a subsequent connection) password if we have it */
    if (password)
    {
        keywords[i] = "password";
        values[i] = password;
    }
    else
    {
        keywords[i] = NULL;
        values[i] = NULL;
    }

    for (;;)
    {
        tmpconn = PQconnectdbParams(keywords, values, (int)true);


        if (PQstatus(tmpconn) == CONNECTION_OK)
        {
            free(values);
            free(keywords);
            if (malloc_port)
            {
                free((void *)malloc_port);
            }
            return tmpconn;
        }

        if (tmpconn && PQconnectionNeedsPassword(tmpconn) && prompt_password)
        {
            PQfinish(tmpconn);
            prompt_for_password(username);
            keywords[i] = "password";
            values[i] = password;
            continue;
        }

        elog(ERROR, "could not connect to database %s: %s",
        dbname, PQerrorMessage(tmpconn));
        PQfinish(tmpconn);
        free(values);
        free(keywords);
        if (malloc_port)
        {
            free((void *)malloc_port);
        }
        return NULL;
    }
}


void
pgut_disconnect(PGconn *conn)
{
    pgut_atexit_pop(pgut_disconnect_callback, conn);
    if (conn)
        PQfinish(conn);
}


PGresult *
pgut_execute_parallel(PGconn* conn,
                                            PGcancel* thread_cancel_conn, const char *query,
                                            int nParams, const char **params,
                                            bool text_result, bool ok_error, bool async)
{
    PGresult   *res;

    if (interrupted && !in_cleanup)
        elog(ERROR, "interrupted");

    /* write query to elog if verbose */
    if (logger_config.log_level_console <= VERBOSE ||
        logger_config.log_level_file <= VERBOSE)
    {
        int i;

        if (strchr(query, '\n'))
            elog(VERBOSE, "(query)\n%s", query);
        else
            elog(VERBOSE, "(query) %s", query);
        for (i = 0; i < nParams; i++)
            elog(VERBOSE, "\t(param:%d) = %s", i, params[i] ? params[i] : "(null)");
    }

    if (conn == NULL)
    {
        elog(ERROR, "not connected");
        return NULL;
    }

    
    if (async)
    {
        res = pgut_async_query(conn, query, nParams, params, text_result);
    }
    else
    {
        if (nParams == 0)
            res = PQexec(conn, query);
        else
            res = PQexecParams(conn, query, nParams, NULL, params, NULL, NULL,
                                                /*
                                                * Specify zero to obtain results in text format,
                                                * or one to obtain results in binary format.
                                                */
                                                (text_result) ? 0 : 1);
    }
    

    switch (PQresultStatus(res))
    {
        case PGRES_TUPLES_OK:
        case PGRES_COMMAND_OK:
        case PGRES_COPY_IN:
            break;
        default:
            if (ok_error && PQresultStatus(res) == PGRES_FATAL_ERROR)
                break;

            elog(ERROR, "query failed: %squery was: %s",
            PQerrorMessage(conn), query);
            break;
    }

    return res;
}

static PGresult *pgut_async_query(PGconn *conn, const char *query,
                                  int nParams, const char **params,
                                  bool text_result)
{
    /* clean any old data */
    discard_response(conn);

    if (nParams == 0)
        PQsendQuery(conn, query);
    else
        PQsendQueryParams(conn, query, nParams, NULL, params, NULL, NULL,
                                            /*
                                            * Specify zero to obtain results in text format,
                                            * or one to obtain results in binary format.
                                            */
                                            (text_result) ? 0 : 1);

    /* wait for processing, TODO: timeout */
    for (;;)
    {
        if (interrupted)
        {
            pgut_cancel(conn);
            pgut_disconnect(conn);
            elog(ERROR, "interrupted");
        }

        if (!PQconsumeInput(conn))
            elog(ERROR, "query failed: %s query was: %s",
                PQerrorMessage(conn), query);

        /* query is no done */
        if (!PQisBusy(conn))
            break;

        usleep(10000);
    }

    return PQgetResult(conn);
}

PGresult *
pgut_execute(PGconn* conn, const char *query, int nParams, const char **params)
{
    return pgut_execute_extended(conn, query, nParams, params, true, false);
}

PGresult *
pgut_execute_extended(PGconn* conn, const char *query, int nParams,
                                                const char **params, bool text_result, bool ok_error)
{
    PGresult   *res;
    ExecStatusType res_status;

    if (interrupted && !in_cleanup)
        elog(ERROR, "interrupted");

    /* write query to elog if verbose */
    if (logger_config.log_level_console <= VERBOSE ||
        logger_config.log_level_file <= VERBOSE)
    {
        int i;

        if (strchr(query, '\n'))
            elog(VERBOSE, "(query)\n%s", query);
        else
            elog(VERBOSE, "(query) %s", query);
        for (i = 0; i < nParams; i++)
            elog(VERBOSE, "\t(param:%d) = %s", i, params[i] ? params[i] : "(null)");
    }

    if (conn == NULL)
    {
        elog(ERROR, "not connected");
        return NULL;
    }

    on_before_exec(conn, NULL);
    if (nParams == 0)
        res = PQexec(conn, query);
    else
        res = PQexecParams(conn, query, nParams, NULL, params, NULL, NULL,
    /*
    * Specify zero to obtain results in text format,
    * or one to obtain results in binary format.
    */
    (text_result) ? 0 : 1);
        on_after_exec(NULL);

    res_status = PQresultStatus(res);
    switch (res_status)
    {
        case PGRES_TUPLES_OK:
        case PGRES_COMMAND_OK:
        case PGRES_COPY_IN:
            break;
        default:
            if (ok_error && res_status == PGRES_FATAL_ERROR)
                break;

            elog(ERROR, "query failed: %squery was: %s",
                PQerrorMessage(conn), query);
            break;
    }

    return res;
}

bool
pgut_send(PGconn* conn, const char *query, int nParams, const char **params, int elevel)
{
    int res;

    if (interrupted && !in_cleanup)
        elog(ERROR, "interrupted");

    /* write query to elog if verbose */
    if (logger_config.log_level_console <= VERBOSE ||
        logger_config.log_level_file <= VERBOSE)
    {
        int i;

        if (strchr(query, '\n'))
            elog(VERBOSE, "(query)\n%s", query);
        else
            elog(VERBOSE, "(query) %s", query);
        for (i = 0; i < nParams; i++)
            elog(VERBOSE, "\t(param:%d) = %s", i, params[i] ? params[i] : "(null)");
    }

    if (conn == NULL)
    {
        elog(elevel, "not connected");
        return false;
    }

    if (nParams == 0)
        res = PQsendQuery(conn, query);
    else
        res = PQsendQueryParams(conn, query, nParams, NULL, params, NULL, NULL, 0);

    if (res != 1)
    {
        elog(elevel, "query failed: %squery was: %s",
            PQerrorMessage(conn), query);
        return false;
    }

    return true;
}

void
pgut_cancel(PGconn* conn)
{
    PGcancel *cancel_conn = PQgetCancel(conn);
    char    errbuf[256];

    if (cancel_conn != NULL)
    {
        if (PQcancel_timeout(cancel_conn, errbuf, sizeof(errbuf),10))
            elog(WARNING, "Cancel request sent");
        else
            elog(WARNING, "Cancel request failed");
    }

    if (cancel_conn)
        PQfreeCancel(cancel_conn);
}

int
pgut_wait(int num, PGconn *connections[], struct timeval *timeout)
{
    /* all connections are busy. wait for finish */
    while (!interrupted)
    {
        int     i;
        fd_set      mask;
        int     maxsock;

        FD_ZERO(&mask);

        maxsock = -1;
        for (i = 0; i < num; i++)
        {
            int sock;

            if (connections[i] == NULL)
                continue;
            sock = PQsocket(connections[i]);
            if (sock >= 0)
            {
                FD_SET(sock, &mask);
                if (maxsock < sock)
                    maxsock = sock;
            }
        }

        if (maxsock == -1)
        {
            errno = ENOENT;
            return -1;
        }

        i = wait_for_sockets(maxsock + 1, &mask, timeout);
        if (i == 0)
            break;  /* timeout */

        for (i = 0; i < num; i++)
        {
            if (connections[i] && FD_ISSET(PQsocket(connections[i]), &mask))
            {
                PQconsumeInput(connections[i]);
                if (PQisBusy(connections[i]))
                    continue;
                return i;
            }
        }
    }

    errno = EINTR;
    return -1;
}

#ifdef WIN32
static CRITICAL_SECTION cancelConnLock;
#endif

/*
 * on_before_exec
 *
 * Set cancel_conn to point to the current database connection.
 */
static void
on_before_exec(PGconn *conn, PGcancel *thread_cancel_conn)
{
    PGcancel   *old;

    if (in_cleanup)
        return; /* forbid cancel during cleanup */

#ifdef WIN32
    EnterCriticalSection(&cancelConnLock);
#endif

    if (thread_cancel_conn)
    {
        
        old = thread_cancel_conn;

        /* be sure handle_interrupt doesn't use pointer while freeing */
        thread_cancel_conn = NULL;

        if (old != NULL)
            PQfreeCancel(old);

        thread_cancel_conn = PQgetCancel(conn);
    }
    else
    {
        /* Free the old one if we have one */
        old = cancel_conn;

        /* be sure handle_interrupt doesn't use pointer while freeing */
        cancel_conn = NULL;

        if (old != NULL)
            PQfreeCancel(old);

        cancel_conn = PQgetCancel(conn);
    }

#ifdef WIN32
    LeaveCriticalSection(&cancelConnLock);
#endif
}

/*
 * on_after_exec
 *
 * Free the current cancel connection, if any, and set to NULL.
 */
static void
on_after_exec(PGcancel *thread_cancel_conn)
{
    PGcancel   *old;

    if (in_cleanup)
        return; /* forbid cancel during cleanup */

#ifdef WIN32
    EnterCriticalSection(&cancelConnLock);
#endif

    if (thread_cancel_conn)
    {
        
        old = thread_cancel_conn;

        /* be sure handle_interrupt doesn't use pointer while freeing */
        thread_cancel_conn = NULL;

        if (old != NULL)
            PQfreeCancel(old);
    }
    else
    {
        old = cancel_conn;

        /* be sure handle_interrupt doesn't use pointer while freeing */
        cancel_conn = NULL;

        if (old != NULL)
            PQfreeCancel(old);
    }
#ifdef WIN32
    LeaveCriticalSection(&cancelConnLock);
#endif
}

/*
 * Handle interrupt signals by cancelling the current command.
 */
static void
on_interrupt(void)
{
    int     save_errno = errno;
    char        errbuf[256];

    /* Set interrupted flag */
    interrupted = true;

    /*
    * User prompts password, call on_cleanup() byhand. Unless we do that we will
    * get stuck forever until a user enters a password.
    */
    if (in_password)
    {
        on_cleanup();

        pqsignal(SIGINT, oldhandler);
        kill(0, SIGINT);
    }

    /* Send QueryCancel if we are processing a database query */
    if (!in_cleanup && cancel_conn != NULL &&
        PQcancel(cancel_conn, errbuf, sizeof(errbuf)))
    {
        elog(WARNING, "Cancel request sent");
    }

    errno = save_errno; /* just in case the write changed it */
}

typedef struct pgut_atexit_item pgut_atexit_item;
struct pgut_atexit_item
{
    pgut_atexit_callback    callback;
    void                            *userdata;
    pgut_atexit_item         *next;
};

static pgut_atexit_item *pgut_atexit_stack = NULL;

void
pgut_disconnect_callback(bool fatal, void *userdata)
{
    PGconn *conn = (PGconn *) userdata;
    if (conn)
        pgut_disconnect(conn);
}

void
pgut_atexit_push(pgut_atexit_callback callback, void *userdata)
{
    pgut_atexit_item *item = pgut_new(pgut_atexit_item);

    AssertArg(callback != NULL);

    item->callback = callback;
    item->userdata = userdata;
    item->next = pgut_atexit_stack;

    pgut_atexit_stack = item;
}

void
pgut_atexit_pop(pgut_atexit_callback callback, void *userdata)
{
    pgut_atexit_item  *item;
    pgut_atexit_item **prev;

    for (item = pgut_atexit_stack, prev = &pgut_atexit_stack;
            item;
            prev = &item->next, item = item->next)
    {
        if (item->callback == callback && item->userdata == userdata)
        {
            *prev = item->next;
            free(item);
            break;
        }
    }
}

static void
call_atexit_callbacks(bool fatal)
{
    pgut_atexit_item  *item;
    pgut_atexit_item  *next;
    for (item = pgut_atexit_stack; item; item = next){
        next = item->next;
        item->callback(fatal, item->userdata);
    }
}

void
on_cleanup(void)
{
    in_cleanup = true;
    interrupted = false;
    call_atexit_callbacks(false);
}

void *
pgut_malloc(size_t size)
{
    char *ret;
    if (size == 0)
    {
        elog(ERROR, "could not allocate 0 byte of memory");
        return NULL;
    }
    if ((ret = (char *)malloc(size)) == NULL)
        elog(ERROR, "could not allocate memory (%lu bytes): %s",
            (unsigned long) size, strerror(errno));
    return ret;
}

void *
pgut_realloc(void *p, size_t oldSize, size_t size)
{
    char *ret;
    int err = 0;
    if (size == 0 || size < oldSize)
    {
        elog(ERROR, "could not re-allocate memory(old size %lu, new size %lu)", oldSize, size);
        return NULL;
    }
	if ((ret = (char *)pgut_malloc(size)) == NULL)
		elog(ERROR, "could not re-allocate memory (%lu bytes): %s",
			(unsigned long) size, strerror(errno));
    if (oldSize != 0) {
        err = memcpy_s(ret, size, p, oldSize);
        securec_check_c(err, "\0", "\0");
        pfree(p);
    }
    return ret;
}

char *
pgut_strdup(const char *str)
{
    char *ret;

    if (str == NULL)
        return NULL;

    if ((ret = strdup(str)) == NULL)
        elog(ERROR, "could not duplicate string \"%s\": %s",
            str, strerror(errno));
    return ret;
}

FILE *
pgut_fopen(const char *path, const char *mode, bool missing_ok)
{
    FILE *fp;

    if ((fp = fio_open_stream(path, FIO_BACKUP_HOST)) == NULL)
    {
        if (missing_ok && errno == ENOENT)
            return NULL;

        elog(ERROR, "could not open file \"%s\": %s",
            path, strerror(errno));
    }

    return fp;
}

#ifdef WIN32
static int select_win32(int nfds, fd_set *readfds, fd_set *writefds, fd_set *exceptfds, const struct timeval * timeout);
#define select      select_win32
#endif

int
wait_for_socket(int sock, struct timeval *timeout)
{
    fd_set  fds;

    FD_ZERO(&fds);
    FD_SET(sock, &fds);
    return wait_for_sockets(sock + 1, &fds, timeout);
}

int
wait_for_sockets(int nfds, fd_set *fds, struct timeval *timeout)
{
    int     i;

    for (;;)
    {
        i = select(nfds, fds, NULL, NULL, timeout);
        if (i < 0)
        {
            if (interrupted)
                elog(ERROR, "interrupted");
            else if (errno != EINTR)
                elog(ERROR, "select failed: %s", strerror(errno));
        }
        else
            return i;
    }
}

#ifndef WIN32
static void
handle_interrupt(SIGNAL_ARGS)
{
    on_interrupt();
}

/* Handle various inrerruptions in the same way */
static void
init_cancel_handler(void)
{
    oldhandler = pqsignal(SIGINT, handle_interrupt);
    pqsignal(SIGQUIT, handle_interrupt);
    pqsignal(SIGTERM, handle_interrupt);
}
#else   /* WIN32 */

/*
 * Console control handler for Win32. Note that the control handler will
 * execute on a *different thread* than the main one, so we need to do
 * proper locking around those structures.
 */
static BOOL WINAPI
consoleHandler(DWORD dwCtrlType)
{
    if (dwCtrlType == CTRL_C_EVENT ||
        dwCtrlType == CTRL_BREAK_EVENT)
    {
        EnterCriticalSection(&cancelConnLock);
        on_interrupt();
        LeaveCriticalSection(&cancelConnLock);
        return TRUE;
    }
    else
        /* Return FALSE for any signals not being handled */
        return FALSE;
}

static void
init_cancel_handler(void)
{
    InitializeCriticalSection(&cancelConnLock);

    SetConsoleCtrlHandler(consoleHandler, TRUE);
}

int
sleep(unsigned int seconds)
{
    Sleep(seconds * 1000);
    return 0;
}

int
usleep(unsigned int usec)
{
    Sleep((usec + 999) / 1000);	/* rounded up */
    return 0;
}

#undef select
static int
select_win32(int nfds, fd_set *readfds, fd_set *writefds, fd_set *exceptfds, const struct timeval * timeout)
{
    struct timeval  remain;

    if (timeout != NULL)
        remain = *timeout;
    else
    {
        remain.tv_usec = 0;
        remain.tv_sec = LONG_MAX;   /* infinite */
    }

    /* sleep only one second because Ctrl+C doesn't interrupt select. */
    while (remain.tv_sec > 0 || remain.tv_usec > 0)
    {
        int ret;
        struct timeval  onesec;

        if (remain.tv_sec > 0)
        {
            onesec.tv_sec = 1;
            onesec.tv_usec = 0;
            remain.tv_sec -= 1;
        }
        else
        {
            onesec.tv_sec = 0;
            onesec.tv_usec = remain.tv_usec;
            remain.tv_usec = 0;
        }

        ret = select(nfds, readfds, writefds, exceptfds, &onesec);
        if (ret != 0)
        {
            /* succeeded or error */
            return ret;
        }
        else if (interrupted)
        {
            errno = EINTR;
            return 0;
        }
    }

    return 0;   /* timeout */
}

#endif   /* WIN32 */

void
discard_response(PGconn *conn)
{
    PGresult   *res;

    do
    {
        res = PQgetResult(conn);
        if (res)
            PQclear(res);
    } while (res);
}

/*
 * pgfnames
 *
 * return a list of the names of objects in the argument directory.  Caller
 * must call pgfnames_cleanup later to free the memory allocated by this
 * function.
 */
char **
pgut_pgfnames(const char *path, bool strict)
{
    DIR *dir;
    struct dirent *file;
    char    **filenames;
    int     numnames = 0;
    int     fnsize = 200;	/* enough for many small dbs */

    dir = opendir(path);
    if (dir == NULL)
    {
        elog(strict ? ERROR : WARNING, "could not open directory \"%s\": %m", path);
        return NULL;
    }

    filenames = (char **) palloc(fnsize * sizeof(char *));

    while (errno = 0, (file = readdir(dir)) != NULL)
    {
        if (strcmp(file->d_name, ".") != 0 && strcmp(file->d_name, "..") != 0)
        {
            if (numnames + 1 >= fnsize)
            {
                fnsize *= 2;
                filenames = (char **) gs_repalloc(filenames,
                                                             fnsize * sizeof(char *));
            }
            filenames[numnames++] = gs_pstrdup(file->d_name);
        }
    }

    if (errno)
    {
        elog(strict ? ERROR : WARNING, "could not read directory \"%s\": %m", path);
        for (int i = 0; i < numnames; i++) {
            pfree(filenames[i]);
        }
        pfree(filenames);
        (void)closedir(dir);
        return NULL;
    }

    filenames[numnames] = NULL;

    if (closedir(dir))
    {
        elog(strict ? ERROR : WARNING, "could not close directory \"%s\": %m", path);
        for (int i = 0; i < numnames; i++) {
            pfree(filenames[i]);
        }
        pfree(filenames);
        return NULL;
    }

    return filenames;
}

/*
 *  pgfnames_cleanup
 *
 *  deallocate memory used for filenames
 */
void
pgut_pgfnames_cleanup(char **filenames)
{
    char    **fn;

    for (fn = filenames; *fn; fn++)
        pfree(*fn);

    pfree(filenames);
}

/* Shamelessly stolen from commom/rmtree.c */
bool
pgut_rmtree(const char *path, bool rmtopdir, bool strict)
{
    bool    result = true;
    char    pathbuf[MAXPGPATH];
    char    **filenames;
    char    **filename;
    struct stat statbuf;
    int nRet = 0;

    /*
    * we copy all the names out of the directory before we start modifying
    * it.
    */
    filenames = pgut_pgfnames(path, strict);

    if (filenames == NULL)
        return false;

    /* now we have the names we can start removing things */
    for (filename = filenames; *filename; filename++)
    {
        nRet = snprintf_s(pathbuf, MAXPGPATH, MAXPGPATH - 1, "%s/%s", path, *filename);
        securec_check_ss_c(nRet, "\0", "\0");

        if (lstat(pathbuf, &statbuf) != 0)
        {
            elog(strict ? ERROR : WARNING, "could not stat file or directory \"%s\": %m", pathbuf);
            result = false;
            break;
        }

        if (S_ISDIR(statbuf.st_mode))
        {
            /* call ourselves recursively for a directory */
            if (!pgut_rmtree(pathbuf, true, strict))
            {
                result = false;
                break;
            }
        }
        else
        {
            if (unlink(pathbuf) != 0)
            {
                elog(strict ? ERROR : WARNING, "could not remove file or directory \"%s\": %m", pathbuf);
                result = false;
                break;
            }
        }
    }

    if (rmtopdir)
    {
        if (rmdir(path) != 0)
        {
            elog(strict ? ERROR : WARNING, "could not remove file or directory \"%s\": %m", path);
            result = false;
        }
    }

    pgut_pgfnames_cleanup(filenames);

    return result;
}
