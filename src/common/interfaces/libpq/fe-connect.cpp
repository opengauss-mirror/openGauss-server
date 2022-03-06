/* -------------------------------------------------------------------------
 *
 * fe-connect.cpp
 *	  functions related to setting up a connection to the backend
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/common/interfaces/libpq/fe-connect.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "postgres_fe.h"

#include <fcntl.h>

#include "libpq/libpq-fe.h"
#include "libpq/libpq-int.h"
#include "fe-auth.h"
#include "pg_config_paths.h"
#include "utils/syscall_lock.h"
#ifdef HAVE_CE
#include "client_logic_cache/icached_column_manager.h"
#endif /* HAVE_CE */

#ifndef WIN32
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#ifndef __USE_GNU
#define __USE_GNU
#endif

#include <dlfcn.h>
#include <pwd.h>
#endif

#ifdef WIN32
#include "win32.h"
#ifdef _WIN32_IE
#undef _WIN32_IE
#endif
#define _WIN32_IE 0x0500
#ifdef near
#undef near
#endif
#define near
#include <shlobj.h>
#ifdef WIN32_ONLY_COMPILER /* mstcpip.h is missing on mingw */
#include <mstcpip.h>
#endif
#else
#ifdef HAVE_NETINET_TCP_H
#include <netinet/tcp.h>
#endif
#include <arpa/inet.h>
#endif

#ifdef ENABLE_THREAD_SAFETY
#ifdef WIN32
#include "pthread-win32.h"
#endif
#endif

#ifdef USE_LDAP
#ifdef WIN32
#include <winldap.h>
#else
/* OpenLDAP deprecates RFC 1823, but we want standard conformance */
#define LDAP_DEPRECATED 1
#include <ldap.h>
typedef struct timeval LDAP_TIMEVAL;
#endif
static int ldapServiceLookup(const char* purl, PQconninfoOption* options, PQExpBuffer errorMessage);
#endif

#include "libpq/ip.h"
#include "mb/pg_wchar.h"

#ifndef FD_CLOEXEC
#define FD_CLOEXEC 1
#endif

#ifdef ENABLE_UT
#define static
#endif

#ifdef SUPPORT_PGPASSFILE
#ifndef WIN32
#define PGPASSFILE ".pgpass"
#else
#define PGPASSFILE "pgpass.conf"
#endif
#endif

extern const char* libpqVersionString;

/*
 * Pre-9.0 servers will return this SQLSTATE if asked to set
 * application_name in a startup packet.  We hard-wire the value rather
 * than looking into errcodes.h since it reflects historical behavior
 * rather than that of the current code.
 */
#define ERRCODE_APPNAME_UNKNOWN "42704"

/* This is part of the protocol so just define it */
#ifdef ERRCODE_INVALID_PASSWORD
#undef ERRCODE_INVALID_PASSWORD
#endif
#define ERRCODE_INVALID_PASSWORD "28P01"
/* This too */
#ifdef ERRCODE_CANNOT_CONNECT_NOW
#undef ERRCODE_CANNOT_CONNECT_NOW
#endif
#define ERRCODE_CANNOT_CONNECT_NOW "57P03"

/*
 * fall back options if they are not specified by arguments or defined
 * by environment variables
 */
#define DefaultHost "localhost"
#define DefaultTty ""
#define DefaultOption ""
#define DefaultAuthtype ""
#define DefaultPassword ""
#define DefaultTargetSessionAttrs    "any"
#ifdef USE_SSL
#define DefaultSSLMode "prefer"
#else
#define DefaultSSLMode "disable"
#endif
char* tcp_link_addr = NULL;
THR_LOCAL bool comm_client_bind = false;
THR_LOCAL volatile Oid* libpq_wait_nodeid = NULL;
THR_LOCAL volatile int* libpq_wait_nodecount = NULL;


long libpq_used_memory = 0;
long libcomm_used_memory = 0;
long comm_peak_used_memory = 0;

#if defined(WIN32) || defined(__sparc)
char* inet_net_ntop(int af, const void* src, int bits, char* dst, size_t size);
#endif
/* ----------
 * Definition of the conninfo parameters and their fallback resources.
 *
 * If Environment-Var and Compiled-in are specified as NULL, no
 * fallback is available. If after all no value can be determined
 * for an option, an error is returned.
 *
 * The value for the username is treated specially in conninfo_add_defaults.
 * If the value is not obtained any other way, the username is determined
 * by pg_fe_getauthname().
 *
 * The Label and Disp-Char entries are provided for applications that
 * want to use PQconndefaults() to create a generic database connection
 * dialog. Disp-Char is defined as follows:
 *		""		Normal input field
 *		"*"		Password field - hide value
 *		"D"		Debug option - don't show by default
 *
 * PQconninfoOptions[] is a constant static array that we use to initialize
 * a dynamically allocated working copy.  All the "val" fields in
 * PQconninfoOptions[] *must* be NULL.	In a working copy, non-null "val"
 * fields point to malloc'd strings that should be freed when the working
 * array is freed (see PQconninfoFree).
 * ----------
 */
static const PQconninfoOption PQconninfoOptions[] = {

    /*
     * "authtype" is no longer used, so mark it "don't show".  We keep it in
     * the array so as not to reject conninfo strings from old apps that might
     * still try to set it.
     */
    {"authtype", "PGAUTHTYPE", DefaultAuthtype, NULL, "Database-Authtype", "D", 20, 0},
    {"service", "PGSERVICE", NULL, NULL, "Database-Service", "", 20, 0},
    {"user", "PGUSER", NULL, NULL, "Database-User", "", 20, 0},
    {"password", "PGPASSWORD", NULL, NULL, "Database-Password", "*", 20, 0},
    {"connect_timeout", "PGCONNECT_TIMEOUT", NULL, NULL, "Connect-timeout", "", 10, 0},
    {"dbname", "PGDATABASE", NULL, NULL, "Database-Name", "", 20, 0},
    {"host", "PGHOST", NULL, NULL, "Database-Host", "", 40, 0},
    {"remote_nodename", "Nodename", NULL, NULL, "Remote-Nodename", "", 40, 0},
    {"hostaddr", "PGHOSTADDR", NULL, NULL, "Database-Host-IP-Address", "", 45, 0},
    {"port", "PGPORT", DEF_PGPORT_STR, NULL, "Database-Port", "", 6, 0},

    /*
     * Add the keywords of localhost and localport.
     * The keywords is needed, when libpq parse the conninfo.
     */
    {"localhost", "PGLOCALADDR", NULL, NULL, "Database-Local-IP-Address", "", 45, 0},
    {"localport", "PGLOCALPORT", DEF_PGPORT_STR, NULL, "Database-Local-Port", "", 6, 0},
    {"client_encoding", "PGCLIENTENCODING", NULL, NULL, "Client-Encoding", "", 10, 0},

    /*
     * "tty" is no longer used either, but keep it present for backwards
     * compatibility.
     */
    {"tty", "PGTTY", DefaultTty, NULL, "Backend-Debug-TTY", "D", 40, 0},
    {"options", "PGOPTIONS", DefaultOption, NULL, "Backend-Debug-Options", "D", 40, 0},
    {"application_name", "PGAPPNAME", NULL, NULL, "Application-Name", "", 64, 0},
    {"fallback_application_name", NULL, NULL, NULL, "Fallback-Application-Name", "", 64, 0},

    /* should be just '0' or '1' */
    {"fencedUdfRPCMode", NULL, NULL, NULL, "fencedUdfRPCMode", "", 1, 0},

    /* should be just '0' or '1' */
    {"keepalives", NULL, NULL, NULL, "TCP-Keepalives", "", 1, 0},
    {"keepalives_idle", NULL, NULL, NULL, "TCP-Keepalives-Idle", "", 10, 0},
    {"keepalives_interval", NULL, NULL, NULL, "TCP-Keepalives-Interval", "", 10, 0},
    {"keepalives_count", NULL, NULL, NULL, "TCP-Keepalives-Count", "", 10, 0},
    {"rw_timeout", NULL, NULL, NULL, "Read write timeout", "", 10, 0},

    /*
     * ssl options are allowed even without client SSL support because the
     * client can still handle SSL modes "disable" and "allow". Other
     * parameters have no effect on non-SSL connections, so there is no reason
     * to exclude them since none of them are mandatory.
     */
    {"sslmode", "PGSSLMODE", DefaultSSLMode, NULL, "SSL-Mode", "", 8, 0},
    {"sslcompression", "PGSSLCOMPRESSION", "1", NULL, "SSL-Compression", "", 1, 0},
    {"sslcert", "PGSSLCERT", NULL, NULL, "SSL-Client-Cert", "", 64, 0},
    {"sslkey", "PGSSLKEY", NULL, NULL, "SSL-Client-Key", "", 64, 0},
    {"sslrootcert", "PGSSLROOTCERT", NULL, NULL, "SSL-Root-Certificate", "", 64, 0},
    {"sslcrl", "PGSSLCRL", NULL, NULL, "SSL-Revocation-List", "", 64, 0},
    {"requirepeer", "PGREQUIREPEER", NULL, NULL, "Require-Peer", "", 10, 0},
#if defined(KRB5) || defined(ENABLE_GSS) || defined(ENABLE_SSPI)
    /* Kerberos and GSSAPI authentication support specifying the service name */
    {"krbsrvname", "PGKRBSRVNAME", PG_KRB_SRVNAM, NULL, "Kerberos-service-name", "", 20, 0},
#endif

#if defined(ENABLE_GSS) && defined(ENABLE_SSPI)

    /*
     * GSSAPI and SSPI both enabled, give a way to override which is used by
     * default
     */
    {"gsslib", "PGGSSLIB", NULL, NULL, "GSS-library", "", 7, 0},
#endif
    {"replication", NULL, NULL, NULL, "Replication", "D", 5, 0},
    {"backend_version", NULL, NULL, NULL, "Backend-version", "D", 10, 0},
    {"prototype", NULL, "1", NULL, "Prototype", "", 2, 0},
    {"enable_ce", NULL, NULL, NULL, "Enable Client Encryption", "D", 1, 0},

    /* Connection_info is a json string containing driver_name, driver_version, driver_path and os_user.
     * If connection_info is not NULL, use connection_info and ignore the value of connectionExtraInfo.
     * If connection_info is NULL:
     * generate connection_info strings related to libpq.
     * connection_info has only driver_name and driver_version while connectionExtraInfo is false.
     */
    {"connection_info", NULL, NULL, NULL, "Connection-Info", "", 8192, 0},
    {"connectionExtraInfo", NULL, NULL, NULL, "Connection-Extra-Info", "", 1, 0},
    {"target_session_attrs", "PGTARGETSESSIONATTRS",
        DefaultTargetSessionAttrs, NULL,
        "Target-Session-Attrs", "", 15, /* sizeof("prefer-standby") = 15 */
        offsetof(struct pg_conn, target_session_attrs)},

    /* Terminating entry --- MUST BE LAST */
    {NULL, NULL, NULL, NULL, NULL, NULL, 0, 0}};

static const PQEnvironmentOption EnvironmentOptions[] = {
    /* common user-interface settings */
    {"PGDATESTYLE", "datestyle"},
    {"PGTZ", "timezone"},
    /* internal performance-related settings */
    {"PGGEQO", "geqo"},
    {NULL, NULL}};

/* The connection URI must start with either of the following designators: */
static const char uri_designator[] = "postgresql://";
static const char short_uri_designator[] = "postgres://";

static bool connectOptions1(PGconn* conn, const char* conninfo);
static bool connectOptions2(PGconn* conn);
static int connectDBStart(PGconn* conn);
static int connectDBComplete(PGconn* conn);
static void connectSetConninfo(PGconn* conn);
static PGPing internal_ping(PGconn* conn);
static void fillPGconn(PGconn* conn, PQconninfoOption* connOptions);
static void release_conn_addrinfo(PGconn* conn);
static void sendTerminateConn(PGconn* conn);
static void pqDropConnection(PGconn* conn, bool flushInput);
static void pqDropServerData(PGconn *conn);
static int count_comma_separated_elems(const char *input);
static char *parse_comma_separated_list(char **startptr, bool *more);
static bool mutiHostlOptions(PGconn* conn);
static bool parse_int_param(const char *value, int *result, PGconn *conn, const char *context);
static bool resolve_host_addr(PGconn *conn);
static void reset_connection_state_machine(PGconn *conn);
static void reset_physical_connection(PGconn *conn);
static void try_next_host(PGconn *conn);
static bool saveErrorMessage(PGconn *conn, PQExpBuffer savedMessage);
static void restoreErrorMessage(PGconn *conn, PQExpBuffer savedMessage);
static PostgresPollingStatusType connection_check_target(PGconn* conn);
static PostgresPollingStatusType connection_consume(PGconn* conn);
static PostgresPollingStatusType connection_check_writable(PGconn* conn);
static PostgresPollingStatusType connection_check_standby(PGconn* conn);
static PQconninfoOption* conninfo_init(PQExpBuffer errorMessage);
static PQconninfoOption* parse_connection_string(const char* conninfo, PQExpBuffer errorMessage, bool use_defaults);
static int uri_prefix_length(const char* connstr);
static bool recognized_connection_string(const char* connstr);
static PQconninfoOption* conninfo_parse(const char* conninfo, PQExpBuffer errorMessage, bool use_defaults);
static PQconninfoOption* conninfo_array_parse(const char* const* keywords, const char* const* values,
    PQExpBuffer errorMessage, bool use_defaults, int expand_dbname);
static bool conninfo_add_defaults(PQconninfoOption* options, PQExpBuffer errorMessage);
static PQconninfoOption* conninfo_uri_parse(const char* uri, PQExpBuffer errorMessage, bool use_defaults);
static bool conninfo_uri_parse_options(PQconninfoOption* options, const char* uri, PQExpBuffer errorMessage);
static bool conninfo_uri_parse_params(char* params, PQconninfoOption* connOptions, PQExpBuffer errorMessage);
static char* conninfo_uri_decode(const char* str, PQExpBuffer errorMessage);
static bool get_hexdigit(char digit, unsigned int* value);
static const char* conninfo_getval(PQconninfoOption* connOptions, const char* keyword);
static PQconninfoOption* conninfo_storeval(PQconninfoOption* connOptions, const char* keyword, const char* value,
    PQExpBuffer errorMessage, bool ignoreMissing, bool uri_decode);
static PQconninfoOption* conninfo_find(PQconninfoOption* connOptions, const char* keyword);
static void defaultNoticeReceiver(void* arg, const PGresult* res);
static void defaultNoticeProcessor(void* arg, const char* message);
static int parseServiceInfo(PQconninfoOption* options, PQExpBuffer errorMessage);
static int parseServiceFile(const char* serviceFile, const char* service, PQconninfoOption* options,
    PQExpBuffer errorMessage, bool* group_found);
#ifdef SUPPORT_PGPASSFILE
static char* pwdfMatchesString(char* buf, char* token);
static char* PasswordFromFile(char* hostname, char* port, char* dbname, char* username);
static bool getPgPassFilename(char* pgpassfile);
static void dot_pg_pass_warning(PGconn* conn);
#endif
static void default_threadlock(int acquire);
static void set_libpq_stat_info(Oid nodeid, int count);

/* global variable because fe-auth.c needs to access it */
pgthreadlock_t pg_g_threadlock = default_threadlock;

/*
 *		Connecting to a Database
 *
 * There are now six different ways a user of this API can connect to the
 * database.  Two are not recommended for use in new code, because of their
 * lack of extensibility with respect to the passing of options to the
 * backend.  These are PQsetdb and PQsetdbLogin (the former now being a macro
 * to the latter).
 *
 * If it is desired to connect in a synchronous (blocking) manner, use the
 * function PQconnectdb or PQconnectdbParams. The former accepts a string of
 * option = value pairs (or a URI) which must be parsed; the latter takes two
 * NULL terminated arrays instead.
 *
 * To connect in an asynchronous (non-blocking) manner, use the functions
 * PQconnectStart or PQconnectStartParams (which differ in the same way as
 * PQconnectdb and PQconnectdbParams) and PQconnectPoll.
 *
 * Internally, the static functions connectDBStart, connectDBComplete
 * are part of the connection procedure.
 */

/*
 *		PQconnectdbParams
 *
 * establishes a connection to a openGauss backend through the postmaster
 * using connection information in two arrays.
 *
 * The keywords array is defined as
 *
 *	   const char *params[] = {"option1", "option2", NULL}
 *
 * The values array is defined as
 *
 *	   const char *values[] = {"value1", "value2", NULL}
 *
 * Returns a PGconn* which is needed for all subsequent libpq calls, or NULL
 * if a memory allocation failed.
 * If the status field of the connection returned is CONNECTION_BAD,
 * then some fields may be null'ed out instead of having valid values.
 *
 * You should call PQfinish (if conn is not NULL) regardless of whether this
 * call succeeded.
 */
PGconn* PQconnectdbParams(const char* const* keywords, const char* const* values, int expand_dbname)
{
    PGconn* conn = PQconnectStartParams(keywords, values, expand_dbname);

    if ((conn != NULL) && conn->status != CONNECTION_BAD) {

#ifdef HAVE_CE
        const char *role = (conn->pguser != NULL) ? conn->pguser : "";
        conn->client_logic->gucParams.role = role;
        if (connectDBComplete(conn) == 1 && conn->client_logic->enable_client_encryption) {
            conn->client_logic->m_cached_column_manager->load_cache(conn);
        }
#else
        (void)connectDBComplete(conn);
#endif

        if (conn->replication == NULL || (conn->replication[0] == '\0')) {
            (void)connectSetConninfo(conn);
        }
    }

    return conn;
}

/*
 *		PQpingParams
 *
 * check server status, accepting parameters identical to PQconnectdbParams
 */
PGPing PQpingParams(const char* const* keywords, const char* const* values, int expand_dbname)
{
    PGconn* conn = PQconnectStartParams(keywords, values, expand_dbname);
    PGPing ret;

    ret = internal_ping(conn);
    PQfinish(conn);

    return ret;
}

/*
 *		PQconnectdb
 *
 * establishes a connection to a openGauss backend through the postmaster
 * using connection information in a string.
 *
 * The conninfo string is either a whitespace-separated list of
 *
 *	   option = value
 *
 * definitions or a URI (refer to the documentation for details.) Value
 * might be a single value containing no whitespaces or a single quoted
 * string. If a single quote should appear anywhere in the value, it must be
 * escaped with a backslash like \'
 *
 * Returns a PGconn* which is needed for all subsequent libpq calls, or NULL
 * if a memory allocation failed.
 * If the status field of the connection returned is CONNECTION_BAD,
 * then some fields may be null'ed out instead of having valid values.
 *
 * You should call PQfinish (if conn is not NULL) regardless of whether this
 * call succeeded.
 */
PGconn* PQconnectdb(const char* conninfo)
{
    PGconn* conn = PQconnectStart(conninfo);

    if ((conn != NULL) && conn->status != CONNECTION_BAD) {
#ifdef HAVE_CE
        if (connectDBComplete(conn) == 1 && conn->client_logic->enable_client_encryption) {
            conn->client_logic->m_cached_column_manager->load_cache(conn);
        }
#else
        (void)connectDBComplete(conn);
#endif
    }

    return conn;
}

/*
 *      PQSetFinTime
 *
 * set finish time for each connection.
 */
void PQSetFinTime(time_t* fin_times, PGconn* conn)
{
    if (conn->connect_timeout != NULL) {
        int timeout = atoi(conn->connect_timeout);

        if (timeout > 0) {
            /*
             * Rounding could cause connection to fail; need at least 2 secs
             */
            if (timeout < 2)
                timeout = 2;

            /* calculate the finish time based on start + timeout */
            *fin_times = time(NULL) + timeout;
        }
    }
}

/*
 *      PQconnectdb Parallel
 *
 * establishes a connection to a openGauss backend through the postmaster
 * using connection information in a string.
 *
 * The conninfo string is either a whitespace-separated list of
 *
 *     option = value
 *
 * definitions or a URI (refer to the documentation for details.) Value
 * might be a single value containing no whitespaces or a single quoted
 * string. If a single quote should appear anywhere in the value, it must be
 * escaped with a backslash like \'
 *
 * Returns a boolean value which whether connect success or failed, or NULL
 * if a memory allocation failed.
 * If the status field of the connection returned is CONNECTION_BAD,
 * then some fields may be null'ed out instead of having valid values.
 *
 * You should call PQfinish (if conn is not NULL) regardless of whether this
 * call succeeded.
 */

int PQconnectdbParallel(char const* const* conninfo, int count, PGconn* conn[], Oid* nodeid)
{
    int conn_done = 0;
    int no_error = 1;

    const int timeout = 2;

    PostgresPollingStatusType* poll_stat = NULL;
    ConnStatusType* con_stat = NULL;
    int* retry_cnt = NULL;

    char buf[256];
    int ss_rc;
    int i = 0;

    if ((con_stat = (ConnStatusType*)calloc(sizeof(ConnStatusType), count)) == NULL) {
        ss_rc = sprintf_s(buf, sizeof(buf), "alloc memory failed!");
        securec_check_ss_c(ss_rc, "\0", "\0");
        (void)write(fileno(stderr), buf, strlen(buf));
        return 0;
    }

    if ((poll_stat = (PostgresPollingStatusType*)calloc(sizeof(PostgresPollingStatusType), count)) == NULL) {
        libpq_free(con_stat);
        ss_rc = sprintf_s(buf, sizeof(buf), "alloc memory failed!");
        securec_check_ss_c(ss_rc, "\0", "\0");
        (void)write(fileno(stderr), buf, strlen(buf));
        return 0;
    }

    if ((retry_cnt = (int*)calloc(sizeof(int), count)) == NULL) {
        libpq_free(poll_stat);
        libpq_free(con_stat);
        ss_rc = sprintf_s(buf, sizeof(buf), "alloc memory failed!");
        securec_check_ss_c(ss_rc, "\0", "\0");
        (void)write(fileno(stderr), buf, strlen(buf));
        return 0;
    }

    for (i = 0; i < count; ++i) {
        /* Connections have been tried */
        if (conn[i] != NULL) {
            conn_done++;
            continue;
        }
        conn[i] = PQconnectStart(conninfo[i]);

        if (conn[i] == NULL) {
            Oid tmp_nodeid = nodeid[i];
            set_libpq_stat_info(tmp_nodeid, count - conn_done);
            con_stat[i] = CONNECTION_BAD;
            poll_stat[i] = PGRES_POLLING_FAILED;
            retry_cnt[i] = -1;
            conn_done++;
            no_error = 0;
            continue;
        }

        con_stat[i] = CONNECTION_STARTED;

        retry_cnt[i] = 1;

        if (conn[i]->connect_timeout != NULL) {
            int connect_timeout = atoi(conn[i]->connect_timeout);

            if (connect_timeout > 0) {
                /*
                 * Rounding could cause connection to fail; need at least 2 secs
                 */
                if (connect_timeout < 2)
                    connect_timeout = 2;

                /* Set up max retry count, 2 secs for a retry. */
                retry_cnt[i] = connect_timeout / timeout;
            }
        }

        if (conn[i]->status != CONNECTION_BAD)
            poll_stat[i] = PGRES_POLLING_WRITING;
        else
            poll_stat[i] = PGRES_POLLING_FAILED;
    }

    i = 0;
    while (conn_done < count) {
        Oid tmp_nodeid = nodeid[i];
        if (CONNECTION_STARTED == con_stat[i]) {
            /*
             * Wait, if necessary.	Note that the initial state (just after
             * PQconnectStart) is to wait for the socket to select for writing.
             */
            switch (poll_stat[i]) {
                case PGRES_POLLING_OK:
                    /*
                     * Reset stored error messages since we now have a working
                     * connection
                     */
                    set_libpq_stat_info(tmp_nodeid, count - conn_done);
                    resetPQExpBuffer(&conn[i]->errorMessage);
                    con_stat[i] = CONNECTION_OK;
                    ++conn_done;
                    break;
                case PGRES_POLLING_READING:
                    /* wait for the finish time based on start + timeout */
                    if (pqWaitTimed(1, 0, conn[i], time(NULL) + timeout)) {
                        --retry_cnt[i];
                        /* no need to retry, connect failed. */
                        if (retry_cnt[i] <= 0) {
                            set_libpq_stat_info(tmp_nodeid, count - conn_done);
                            conn[i]->status = CONNECTION_BAD;
                            con_stat[i] = CONNECTION_BAD;
                            ++conn_done;
                            no_error = 0;
                        } else
                            resetPQExpBuffer(&conn[i]->errorMessage);
                    }

                    poll_stat[i] = PQconnectPoll(conn[i]);
                    break;
                case PGRES_POLLING_WRITING:
                    /* wait for the finish time based on start + timeout */
                    if (pqWaitTimed(0, 1, conn[i], time(NULL) + timeout)) {
                        --retry_cnt[i];
                        /* no need to retry, connect failed. */
                        if (retry_cnt[i] <= 0) {
                            set_libpq_stat_info(tmp_nodeid, count - conn_done);
                            conn[i]->status = CONNECTION_BAD;
                            con_stat[i] = CONNECTION_BAD;
                            ++conn_done;
                            no_error = 0;
                        } else
                            resetPQExpBuffer(&conn[i]->errorMessage);
                    }

                    poll_stat[i] = PQconnectPoll(conn[i]);
                    break;
                default:
                    /* Just in case we failed to set it in PQconnectPoll */
                    set_libpq_stat_info(tmp_nodeid, count - conn_done);
                    conn[i]->status = CONNECTION_BAD;
                    con_stat[i] = CONNECTION_BAD;
                    ++conn_done;
                    no_error = 0;
            }
        }

        i = (i + 1) % count;
    }

    libpq_free(poll_stat);
    libpq_free(con_stat);
    libpq_free(retry_cnt);
    return no_error;
}

/*
 *		PQping
 *
 * check server status, accepting parameters identical to PQconnectdb
 */
PGPing PQping(const char* conninfo)
{
    PGconn* conn = PQconnectStart(conninfo);
    PGPing ret;

    ret = internal_ping(conn);
    PQfinish(conn);

    return ret;
}

/*
 *		PQconnectStartParams
 *
 * Begins the establishment of a connection to a openGauss backend through the
 * postmaster using connection information in a struct.
 *
 * See comment for PQconnectdbParams for the definition of the string format.
 *
 * Returns a PGconn*.  If NULL is returned, a malloc error has occurred, and
 * you should not attempt to proceed with this connection.	If the status
 * field of the connection returned is CONNECTION_BAD, an error has
 * occurred. In this case you should call PQfinish on the result, (perhaps
 * inspecting the error message first).  Other fields of the structure may not
 * be valid if that occurs.  If the status field is not CONNECTION_BAD, then
 * this stage has succeeded - call PQconnectPoll, using select(2) to see when
 * this is necessary.
 *
 * See PQconnectPoll for more info.
 */
PGconn* PQconnectStartParams(const char* const* keywords, const char* const* values, int expand_dbname)
{
    PGconn* conn = NULL;
    PQconninfoOption* connOptions = NULL;

    /*
     * Allocate memory for the conn structure
     */
    conn = makeEmptyPGconn();
    if (conn == NULL)
        return NULL;

    /* Check for params keywords-values */
    if (keywords == NULL || values == NULL) {
        printfPQExpBuffer(&conn->errorMessage, libpq_gettext("invalid params keywords or values.\n"));
        conn->status = CONNECTION_BAD;
        return conn;
    }

    /*
     * Parse the conninfo arrays
     */
    connOptions = conninfo_array_parse(keywords, values, &conn->errorMessage, true, expand_dbname);
    if (connOptions == NULL) {
        conn->status = CONNECTION_BAD;
        /* errorMessage is already set */
        return conn;
    }

    /*
     * Move option values into conn structure
     */
    fillPGconn(conn, connOptions);

    /*
     * Free the option info - all is in conn now
     */
    PQconninfoFree(connOptions);

    /*
     * Compute derived options
     */
    if (!connectOptions2(conn))
        return conn;

    /*
     * Connect to the database
     */
    if (!connectDBStart(conn)) {
        /* Just in case we failed to set it in connectDBStart */
        conn->status = CONNECTION_BAD;
    }

    return conn;
}

/*
 * PQbuildPGconn
 *
 * Begins the establishment of a connection to a openGauss backend through the
 * postmaster using connection information in a string.
 *
 * malloc a PGconn*, init it and return in the input pointer connPtr.
 * Moreover, a startuppacket containing the infomation needed to send to backend is create by conninfo.
 * return a startuppakcet is all succeed. otherwise, NULL is returned if a malloc error has occurred, and
 * you should not attempt to proceed with this connection.
 */
char* PQbuildPGconn(const char* conninfo, PGconn** connPtr, int* packetlen)
{
    char* startpacket = NULL;

    /*
     * Allocate memory for the conn structure
     */
    *connPtr = makeEmptyPGconn();
    if (*connPtr == NULL)
        return NULL;
    PGconn* conn = *connPtr;

    /*
     * Parse the conninfo string
     */
    if (!connectOptions1(conn, conninfo))
        return NULL;

    /*
     * Compute derived options
     */
    if (!connectOptions2(conn))
        return NULL;

    /* Ensure our buffers are empty */
    conn->inStart = conn->inCursor = conn->inEnd = 0;
    conn->outCount = 0;

#ifdef USE_SSL
    /* setup values based on SSL mode */
    if (conn->sslmode[0] == 'd') /* "disable" */
        conn->allow_ssl_try = false;
    else if (conn->sslmode[0] == 'a') /* "allow" */
        conn->wait_ssl_try = true;
#endif

    /* Whenever change the authenication process, change the version here for compatible.*/
    conn->pversion = PG_PROTOCOL(3, 51);
    conn->send_appname = true;
    conn->status = CONNECTION_NEEDED;

    /* Build the startup packet*/
    if (PG_PROTOCOL_MAJOR(conn->pversion) >= 3)
        startpacket = pqBuildStartupPacket3(conn, packetlen, EnvironmentOptions);
    else
        startpacket = pqBuildStartupPacket2(conn, packetlen, EnvironmentOptions);

    return startpacket;
}

/*
 *		PQconnectStart
 *
 * Begins the establishment of a connection to a openGauss backend through the
 * postmaster using connection information in a string.
 *
 * See comment for PQconnectdb for the definition of the string format.
 *
 * Returns a PGconn*.  If NULL is returned, a malloc error has occurred, and
 * you should not attempt to proceed with this connection.	If the status
 * field of the connection returned is CONNECTION_BAD, an error has
 * occurred. In this case you should call PQfinish on the result, (perhaps
 * inspecting the error message first).  Other fields of the structure may not
 * be valid if that occurs.  If the status field is not CONNECTION_BAD, then
 * this stage has succeeded - call PQconnectPoll, using select(2) to see when
 * this is necessary.
 *
 * See PQconnectPoll for more info.
 */
PGconn* PQconnectStart(const char* conninfo)
{
    PGconn* conn = NULL;

    /*
     * Allocate memory for the conn structure
     */
    conn = makeEmptyPGconn();
    if (conn == NULL)
        return NULL;

    /*
     * Parse the conninfo string
     */
    if (!connectOptions1(conn, conninfo))
        return conn;

    /*
     * Compute derived options
     */
    if (!connectOptions2(conn))
        return conn;

    /*
     * Connect to the database
     */
    if (!connectDBStart(conn)) {
        /* Just in case we failed to set it in connectDBStart */
        conn->status = CONNECTION_BAD;
    }

    return conn;
}

static void fillPGconn(PGconn* conn, PQconninfoOption* connOptions)
{
    const char* tmp = NULL;

    /*
     * Move option values into conn structure
     *
     * Don't put anything cute here --- intelligence should be in
     * connectOptions2 ...
     *
     * XXX: probably worth checking strdup() return value here...
     */
    tmp = conninfo_getval(connOptions, "hostaddr");
    conn->pghostaddr = (tmp != NULL) ? strdup(tmp) : NULL;
    tmp = conninfo_getval(connOptions, "remote_nodename");
    conn->remote_nodename = (tmp != NULL) ? strdup(tmp) : NULL;
    tmp = conninfo_getval(connOptions, "host");
    conn->pghost = (tmp != NULL) ? strdup(tmp) : NULL;
    tmp = conninfo_getval(connOptions, "port");
    conn->pgport = (tmp != NULL) ? strdup(tmp) : NULL;
    tmp = conninfo_getval(connOptions, "localhost");
    conn->pglocalhost = (tmp != NULL) ? strdup(tmp) : NULL;
    tmp = conninfo_getval(connOptions, "localport");
    conn->pglocalport = (tmp != NULL) ? strdup(tmp) : NULL;
    tmp = conninfo_getval(connOptions, "tty");
    conn->pgtty = (tmp != NULL) ? strdup(tmp) : NULL;
    tmp = conninfo_getval(connOptions, "options");
    conn->pgoptions = (tmp != NULL) ? strdup(tmp) : NULL;
    tmp = conninfo_getval(connOptions, "application_name");
    conn->appname = (tmp != NULL) ? strdup(tmp) : NULL;
    tmp = conninfo_getval(connOptions, "fallback_application_name");
    conn->fbappname = (tmp != NULL) ? strdup(tmp) : NULL;
    tmp = conninfo_getval(connOptions, "dbname");
    conn->dbName = (tmp != NULL) ? strdup(tmp) : NULL;
    tmp = conninfo_getval(connOptions, "user");
    conn->pguser = (tmp != NULL) ? strdup(tmp) : NULL;
    tmp = conninfo_getval(connOptions, "password");
    conn->pgpass = (tmp != NULL) ? strdup(tmp) : NULL;
    tmp = conninfo_getval(connOptions, "connect_timeout");
    conn->connect_timeout = (tmp != NULL) ? strdup(tmp) : NULL;
    tmp = conninfo_getval(connOptions, "client_encoding");
    conn->client_encoding_initial = (tmp != NULL) ? strdup(tmp) : NULL;
    tmp = conninfo_getval(connOptions, "keepalives");
    conn->keepalives = (tmp != NULL) ? strdup(tmp) : NULL;
    tmp = conninfo_getval(connOptions, "keepalives_idle");
    conn->keepalives_idle = (tmp != NULL) ? strdup(tmp) : NULL;
    tmp = conninfo_getval(connOptions, "keepalives_interval");
    conn->keepalives_interval = (tmp != NULL) ? strdup(tmp) : NULL;
    tmp = conninfo_getval(connOptions, "keepalives_count");
    conn->keepalives_count = (tmp != NULL) ? strdup(tmp) : NULL;
    tmp = conninfo_getval(connOptions, "rw_timeout");
    conn->rw_timeout = (tmp != NULL) ? strdup(tmp) : NULL;
    tmp = conninfo_getval(connOptions, "sslmode");
    conn->sslmode = (tmp != NULL) ? strdup(tmp) : NULL;
    tmp = conninfo_getval(connOptions, "sslcompression");
    conn->sslcompression = (tmp != NULL) ? strdup(tmp) : NULL;
    tmp = conninfo_getval(connOptions, "sslkey");
    conn->sslkey = (tmp != NULL) ? strdup(tmp) : NULL;
    tmp = conninfo_getval(connOptions, "sslcert");
    conn->sslcert = (tmp != NULL) ? strdup(tmp) : NULL;
    tmp = conninfo_getval(connOptions, "sslrootcert");
    conn->sslrootcert = (tmp != NULL) ? strdup(tmp) : NULL;
    tmp = conninfo_getval(connOptions, "sslcrl");
    conn->sslcrl = (tmp != NULL) ? strdup(tmp) : NULL;
    tmp = conninfo_getval(connOptions, "target_session_attrs");
    conn->target_session_attrs = (tmp != NULL) ? strdup(tmp) : NULL;
    tmp = conninfo_getval(connOptions, "requirepeer");
    conn->requirepeer = (tmp != NULL) ? strdup(tmp) : NULL;
#if defined(KRB5) || defined(ENABLE_GSS) || defined(ENABLE_SSPI)
    tmp = conninfo_getval(connOptions, "krbsrvname");
    conn->krbsrvname = (tmp != NULL) ? strdup(tmp) : NULL;
#endif
#if defined(ENABLE_GSS) && defined(ENABLE_SSPI)
    tmp = conninfo_getval(connOptions, "gsslib");
    conn->gsslib = (tmp != NULL) ? strdup(tmp) : NULL;
#endif
    tmp = conninfo_getval(connOptions, "replication");
    conn->replication = (tmp != NULL) ? strdup(tmp) : NULL;
    tmp = conninfo_getval(connOptions, "backend_version");
    conn->backend_version = (tmp != NULL) ? strdup(tmp) : NULL;

    tmp = conninfo_getval(connOptions, "fencedUdfRPCMode");
    conn->fencedUdfRPCMode = (tmp != NULL) ? true : false;

    tmp = conninfo_getval(connOptions, "connection_info");
    conn->connection_info = tmp ? strdup(tmp) : NULL;

    tmp = conninfo_getval(connOptions, "connectionExtraInfo");
    conn->connection_extra_info = false;
    if (tmp != NULL) {
        if (strcmp("0", tmp) == 0)
            conn->connection_extra_info = false;
        else
            conn->connection_extra_info = true;
    }
#ifdef HAVE_CE
    tmp = conninfo_getval(connOptions, "enable_ce");
    conn->client_logic->enable_client_encryption = (tmp != NULL) ? true : false;
#endif
}

/*
 *		connectOptions1
 *
 * Internal subroutine to set up connection parameters given an already-
 * created PGconn and a conninfo string.  Derived settings should be
 * processed by calling connectOptions2 next.  (We split them because
 * PQsetdbLogin overrides defaults in between.)
 *
 * Returns true if OK, false if trouble (in which case errorMessage is set
 * and so is conn->status).
 */
static bool connectOptions1(PGconn* conn, const char* conninfo)
{
    PQconninfoOption* connOptions = NULL;

    if (conninfo == NULL) {
        conn->status = CONNECTION_BAD;
        /* errorMessage is already set */
        return false;
    }

    /*
     * Parse the conninfo string
     */
    connOptions = parse_connection_string(conninfo, &conn->errorMessage, true);
    if (connOptions == NULL) {
        conn->status = CONNECTION_BAD;
        /* errorMessage is already set */
        return false;
    }

    /*
     * Move option values into conn structure
     */
    fillPGconn(conn, connOptions);

    /*
     * Free the option info - all is in conn now
     */
    PQconninfoFree(connOptions);

    return true;
}

/*
 *		connectOptions2
 *
 * Compute derived connection options after absorbing all user-supplied info.
 *
 * Returns true if OK, false if trouble (in which case errorMessage is set
 * and so is conn->status).
 */
static bool connectOptions2(PGconn* conn)
{
    if (!mutiHostlOptions(conn)) {
        return false;
    }

    /*
     * If database name was not given, default it to equal user name
     */
    if ((conn->dbName == NULL || conn->dbName[0] == '\0') && conn->pguser != NULL) {
        if (conn->dbName != NULL)
            free(conn->dbName);
        conn->dbName = strdup(conn->pguser);
        if (!conn->dbName)
            goto oom_error;
    }
#ifdef SUPPORT_PGPASSFILE
    /*
     * Supply default password if none given. Note that the password might
     * be defferent for each host/port pair.
     */
    if (conn->pgpass == NULL || conn->pgpass[0] == '\0') {
        if (conn->pgpass != NULL) {
            erase_string(conn->pgpass);
            libpq_free(conn->pgpass);
        }

        for (int i = 0; i < conn->nconnhost; ++i) {
                /*
                 * Try to get a password for this host from file.  We use host
                 * for the hostname search key if given, else hostaddr (at
                 * least one of them is guaranteed nonempty by now).
                 */
                const char *pwhost = conn->connhost[i].host;

                if (pwhost == NULL || pwhost[0] == '\0')
                    pwhost = conn->connhost[i].hostaddr;
                conn->connhost[i].password = PasswordFromFile(conn->connhost[i].host,
                                                              conn->connhost[i].port,
                                                              conn->dbName, conn->pguser);
                if (conn->connhost[i].password == NULL)
                    conn->pgpass = strdup(DefaultPassword);
                else
                    conn->dot_pgpass_used = true;
        }
    }
#else
    if (conn->pgpass == NULL) {
        conn->pgpass = strdup(DefaultPassword);
    }
#endif

    /*
     * validate sslmode option
     */
    if (conn->sslmode != NULL) {
        if (strcmp(conn->sslmode, "disable") != 0 && strcmp(conn->sslmode, "allow") != 0 &&
            strcmp(conn->sslmode, "prefer") != 0 && strcmp(conn->sslmode, "require") != 0 &&
            strcmp(conn->sslmode, "verify-ca") != 0 && strcmp(conn->sslmode, "verify-full") != 0) {
            conn->status = CONNECTION_BAD;
            printfPQExpBuffer(&conn->errorMessage, libpq_gettext("invalid sslmode value: \"%s\"\n"), conn->sslmode);
            return false;
        }

#ifndef USE_SSL
        switch (conn->sslmode[0]) {
            case 'a': /* "allow" */
            case 'p': /* "prefer" */

                /*
                 * warn user that an SSL connection will never be negotiated
                 * since SSL was not compiled in?
                 */
                break;

            case 'r': /* "require" */
            case 'v': /* "verify-ca" or "verify-full" */
                conn->status = CONNECTION_BAD;
                printfPQExpBuffer(&conn->errorMessage,
                    libpq_gettext("sslmode value \"%s\" invalid when SSL support is not compiled in\n"),
                    conn->sslmode);
                return false;
        }
#endif
    } else
        conn->sslmode = strdup(DefaultSSLMode);

    /*
     * Resolve special "auto" client_encoding from the locale
     */
    if ((conn->client_encoding_initial != NULL) && strcmp(conn->client_encoding_initial, "auto") == 0) {
        free(conn->client_encoding_initial);
        conn->client_encoding_initial = strdup(pg_encoding_to_char(pg_get_encoding_from_locale(NULL, true)));
    }

    /*
     * Validate target_session_attrs option, and set target_server_type
     */
    if (conn->target_session_attrs) {
        if (strcmp(conn->target_session_attrs, "any") == 0)
            conn->target_server_type = SERVER_TYPE_ANY;
        else if (strcmp(conn->target_session_attrs, "read-write") == 0)
            conn->target_server_type = SERVER_TYPE_READ_WRITE;
        else if (strcmp(conn->target_session_attrs, "read-only") == 0)
            conn->target_server_type = SERVER_TYPE_READ_ONLY;
        else if (strcmp(conn->target_session_attrs, "primary") == 0)
            conn->target_server_type = SERVER_TYPE_PRIMARY;
        else if (strcmp(conn->target_session_attrs, "standby") == 0)
            conn->target_server_type = SERVER_TYPE_STANDBY;
        else if (strcmp(conn->target_session_attrs, "prefer-standby") == 0)
            conn->target_server_type = SERVER_TYPE_PREFER_STANDBY;
        else {
            conn->status = CONNECTION_BAD;
            appendPQExpBuffer(&conn->errorMessage,
                              libpq_gettext("invalid %s value: \"%s\"\n"),
                              "target_session_attrs",
                              conn->target_session_attrs);
            return false;
        }
    } else {
        conn->target_server_type = SERVER_TYPE_ANY;
    }

    /*
     * Only if we get this far is it appropriate to try to connect. (We need a
     * state flag, rather than just the boolean result of this function, in
     * case someone tries to PQreset() the PGconn.)
     */
    conn->options_valid = true;

    return true;

oom_error:
    conn->status = CONNECTION_BAD;
    printfPQExpBuffer(&conn->errorMessage, libpq_gettext("out of memory \n"));
    return false;
}

/*
 *		PQconndefaults
 *
 * Construct a default connection options array, which identifies all the
 * available options and shows any default values that are available from the
 * environment etc.  On error (eg out of memory), NULL is returned.
 *
 * Using this function, an application may determine all possible options
 * and their current default values.
 *
 * NOTE: as of PostgreSQL 7.0, the returned array is dynamically allocated
 * and should be freed when no longer needed via PQconninfoFree().	(In prior
 * versions, the returned array was static, but that's not thread-safe.)
 * Pre-7.0 applications that use this function will see a small memory leak
 * until they are updated to call PQconninfoFree.
 */
PQconninfoOption* PQconndefaults(void)
{
    PQExpBufferData errorBuf;
    PQconninfoOption* connOptions = NULL;

    /* We don't actually report any errors here, but callees want a buffer */
    initPQExpBuffer(&errorBuf);
    if (PQExpBufferDataBroken(errorBuf))
        return NULL; /* out of memory already :-( */

    connOptions = conninfo_init(&errorBuf);
    if (connOptions != NULL) {
        if (!conninfo_add_defaults(connOptions, &errorBuf)) {
            PQconninfoFree(connOptions);
            connOptions = NULL;
        }
    }

    termPQExpBuffer(&errorBuf);
    return connOptions;
}

/* ----------------
 *		PQsetdbLogin
 *
 * establishes a connection to a openGauss backend through the postmaster
 * at the specified host and port.
 *
 * returns a PGconn* which is needed for all subsequent libpq calls
 *
 * if the status field of the connection returned is CONNECTION_BAD,
 * then only the errorMessage is likely to be useful.
 * ----------------
 */
PGconn* PQsetdbLogin(const char* pghost, const char* pgport, const char* pgoptions, const char* pgtty,
    const char* dbName, const char* login, const char* pwd)
{
    PGconn* conn = NULL;

    /*
     * Allocate memory for the conn structure
     */
    conn = makeEmptyPGconn();
    if (conn == NULL)
        return NULL;

    /*
     * If the dbName parameter contains what looks like a connection string,
     * parse it into conn struct using connectOptions1.
     */
    if ((dbName != NULL) && recognized_connection_string(dbName)) {
        if (!connectOptions1(conn, dbName))
            return conn;
    } else {
        /*
         * Old-style path: first, parse an empty conninfo string in order to
         * set up the same defaults that PQconnectdb() would use.
         */
        if (!connectOptions1(conn, ""))
            return conn;

        /* Insert dbName parameter value into struct */
        if ((dbName != NULL) && (dbName[0] != '\0')) {
            if (conn->dbName != NULL)
                free(conn->dbName);
            conn->dbName = strdup(dbName);
        }
    }

    /*
     * Insert remaining parameters into struct, overriding defaults (as well
     * as any conflicting data from dbName taken as a conninfo).
     */
    if (pghost != NULL && pghost[0] != '\0') {
        if (conn->pghost != NULL)
            free(conn->pghost);
        conn->pghost = strdup(pghost);
    }

    if (pgport != NULL && pgport[0] != '\0') {
        if (conn->pgport != NULL)
            free(conn->pgport);
        conn->pgport = strdup(pgport);
    }

    if (pgoptions != NULL && pgoptions[0] != '\0') {
        if (conn->pgoptions != NULL)
            free(conn->pgoptions);
        conn->pgoptions = strdup(pgoptions);
    }

    if (pgtty != NULL && pgtty[0] != '\0') {
        if (conn->pgtty != NULL)
            free(conn->pgtty);
        conn->pgtty = strdup(pgtty);
    }

    if (login != NULL && login[0] != '\0') {
        if (conn->pguser != NULL)
            free(conn->pguser);
        conn->pguser = strdup(login);
    }

    if (pwd != NULL && pwd[0] != '\0') {
        if (conn->pgpass != NULL) {
            erase_string(conn->pgpass);
            free(conn->pgpass);
        }
        conn->pgpass = strdup(pwd);
    }

    /*
     * Compute derived options
     */
    if (!connectOptions2(conn))
        return conn;

    /*
     * Connect to the database
     */
    if (connectDBStart(conn)) {
#ifdef HAVE_CE
        if (connectDBComplete(conn) == 1 && conn->client_logic->enable_client_encryption) {
            conn->client_logic->m_cached_column_manager->load_cache(conn);
        }
#else
        (void)connectDBComplete(conn);
#endif
    }
    return conn;
}

/* ----------
 * connectNoDelay -
 * Sets the TCP_NODELAY socket option.
 * Returns 1 if successful, 0 if not.
 * ----------
 */
static int connectNoDelay(PGconn* conn)
{
#ifdef TCP_NODELAY
    int on = 1;

    if (setsockopt(conn->sock, IPPROTO_TCP, TCP_NODELAY, (char*)&on, sizeof(on)) < 0) {
        char sebuf[256];

        appendPQExpBuffer(&conn->errorMessage,
            libpq_gettext("could not set socket to TCP no delay mode: %s\n"),
            SOCK_STRERROR(SOCK_ERRNO, sebuf, sizeof(sebuf)));
        return 0;
    }
#endif

    return 1;
}

/* ----------
 * connectFailureMessage -
 * create a friendly error message on connection failure.
 * ----------
 */
static void connectFailureMessage(PGconn* conn, int errorno)
{
#ifdef HAVE_UNIX_SOCKETS
    if (IS_AF_UNIX(conn->raddr.addr.ss_family)) {
        char service[NI_MAXHOST];

        (void)pg_getnameinfo_all(
            &conn->raddr.addr, conn->raddr.salen, NULL, 0, service, sizeof(service), NI_NUMERICSERV);
        appendPQExpBuffer(&conn->errorMessage,
            libpq_gettext("could not connect to server: %s\n"
                          "\tIs the server running locally and accepting\n"
                          "\tconnections on Unix domain socket \"%s\"?\n"),
            strerror(errorno),
            service);
    } else
#endif /* HAVE_UNIX_SOCKETS */
    {
        char host_addr[NI_MAXHOST];
        int rcs = 0;
        const char* displayed_host = NULL;
        const char* displayed_port = NULL;

        struct sockaddr_storage* addr = &conn->raddr.addr;

        /*
         * Optionally display the network address with the hostname. This is
         * useful to distinguish between IPv4 and IPv6 connections.
         */
        if (conn->pghostaddr != NULL && conn->pghostaddr[0] != '\0') {
            check_strncpy_s(strncpy_s(host_addr, NI_MAXHOST, conn->connhost[conn->whichhost].hostaddr,
                                      strlen(conn->connhost[conn->whichhost].hostaddr)));
        } else if (addr->ss_family == AF_INET) {
#if defined(WIN32) || defined(_WIN64)
            rcs = strcpy_s(host_addr, NI_MAXHOST, "inet_net_ntop() unsupported on Windows");
#else
            if (inet_net_ntop(AF_INET, 
                &((struct sockaddr_in*)addr)->sin_addr.s_addr, 32, host_addr, sizeof(host_addr)) == NULL) {
                rcs = strcpy_s(host_addr, NI_MAXHOST, "???");
            }
#endif
        }
#ifdef HAVE_IPV6
        else if (addr->ss_family == AF_INET6) {
#if defined(WIN32) || defined(_WIN64)
            rcs = strcpy_s(host_addr, NI_MAXHOST, "inet_net_ntop() unsupported on Windows");
#else
            if (inet_net_ntop(AF_INET6, 
                &((struct sockaddr_in6*)addr)->sin6_addr.s6_addr, 128, host_addr, sizeof(host_addr)) == NULL) {
                rcs = strcpy_s(host_addr, NI_MAXHOST, "???");
            }
#endif
        }
#endif
        else {
            rcs = strcpy_s(host_addr, NI_MAXHOST, "???");
        }
        securec_check_c(rcs, "\0", "\0");

        if (conn->pghostaddr != NULL && conn->pghostaddr[0] != '\0')
            displayed_host = conn->connhost[conn->whichhost].hostaddr;
        else if (conn->pghost != NULL && conn->pghost[0] != '\0')
            displayed_host = conn->connhost[conn->whichhost].host;
        else
            displayed_host = DefaultHost;

        displayed_port = conn->connhost[conn->whichhost].port;
        if (displayed_port == NULL || displayed_port[0] == '\0') {
            displayed_port = DEF_PGPORT_STR;
        }

        /*
         * If the user did not supply an IP address using 'hostaddr', and
         * 'host' was missing or does not match our lookup, display the
         * looked-up IP address.
         */
        if ((conn->pghostaddr == NULL) && (conn->pghost == NULL || strcmp(displayed_host, host_addr) != 0))
            appendPQExpBuffer(&conn->errorMessage,
                libpq_gettext("could not connect to server: %s\n"
                              "\tIs the server running on host \"%s\" (%s) and accepting\n"
                              "\tTCP/IP connections on port %s?\n"),
                strerror(errorno),
                displayed_host,
                host_addr,
                displayed_port);
        else
            appendPQExpBuffer(&conn->errorMessage,
                libpq_gettext("could not connect to server: %s\n"
                              "\tIs the server running on host \"%s\" and accepting\n"
                              "\tTCP/IP connections on port %s?\n"),
                strerror(errorno),
                displayed_host,
                displayed_port);
    }
}

/*
 * Should we use keepalives?  Returns 1 if yes, 0 if no, and -1 if
 * conn->keepalives is set to a value which is not parseable as an
 * integer.
 */
static int useKeepalives(PGconn* conn)
{
    char* ep = NULL;
    int val;

    if (conn->keepalives == NULL)
        return 1;
    val = strtol(conn->keepalives, &ep, 10);
    if (*ep)
        return -1;
    return val != 0 ? 1 : 0;
}

#ifndef WIN32
/*
 * Set the keepalive idle timer.
 */
static int setKeepalivesIdle(PGconn* conn)
{
    int idle;

    if (conn->keepalives_idle == NULL)
        return 1;

    idle = atoi(conn->keepalives_idle);
    if (idle < 0)
        idle = 0;

#ifdef TCP_KEEPIDLE
    if (setsockopt(conn->sock, IPPROTO_TCP, TCP_KEEPIDLE, (char*)&idle, sizeof(idle)) < 0) {
        char sebuf[256];

        appendPQExpBuffer(&conn->errorMessage,
            libpq_gettext("setsockopt(TCP_KEEPIDLE) failed: %s\n"),
            SOCK_STRERROR(SOCK_ERRNO, sebuf, sizeof(sebuf)));
        return 0;
    }
#else
#ifdef TCP_KEEPALIVE
    /* Darwin uses TCP_KEEPALIVE rather than TCP_KEEPIDLE */
    if (setsockopt(conn->sock, IPPROTO_TCP, TCP_KEEPALIVE, (char*)&idle, sizeof(idle)) < 0) {
        char sebuf[256];

        appendPQExpBuffer(&conn->errorMessage,
            libpq_gettext("setsockopt(TCP_KEEPALIVE) failed: %s\n"),
            SOCK_STRERROR(SOCK_ERRNO, sebuf, sizeof(sebuf)));
        return 0;
    }
#endif
#endif

    return 1;
}

/*
 * Set the keepalive interval.
 */
static int setKeepalivesInterval(PGconn* conn)
{
    int interval;

    if (conn->keepalives_interval == NULL)
        return 1;

    interval = atoi(conn->keepalives_interval);
    if (interval < 0)
        interval = 0;

#ifdef TCP_KEEPINTVL
    if (setsockopt(conn->sock, IPPROTO_TCP, TCP_KEEPINTVL, (char*)&interval, sizeof(interval)) < 0) {
        char sebuf[256];

        appendPQExpBuffer(&conn->errorMessage,
            libpq_gettext("setsockopt(TCP_KEEPINTVL) failed: %s\n"),
            SOCK_STRERROR(SOCK_ERRNO, sebuf, sizeof(sebuf)));
        return 0;
    }
#endif

    return 1;
}

/*
 * Set the count of lost keepalive packets that will trigger a connection
 * break.
 */
static int setKeepalivesCount(PGconn* conn)
{
    int count;

    if (conn->keepalives_count == NULL)
        return 1;

    count = atoi(conn->keepalives_count);
    if (count < 0)
        count = 0;

#ifdef TCP_KEEPCNT
    if (setsockopt(conn->sock, IPPROTO_TCP, TCP_KEEPCNT, (char*)&count, sizeof(count)) < 0) {
        char sebuf[256];

        appendPQExpBuffer(&conn->errorMessage,
            libpq_gettext("setsockopt(TCP_KEEPCNT) failed: %s\n"),
            SOCK_STRERROR(SOCK_ERRNO, sebuf, sizeof(sebuf)));
        return 0;
    }
#endif

    return 1;
}
#else /* Win32 */
#ifdef SIO_KEEPALIVE_VALS
/*
 * Enable keepalives and set the keepalive values on Win32,
 * where they are always set in one batch.
 */
static int setKeepalivesWin32(PGconn* conn)
{
    struct tcp_keepalive ka;
    DWORD retsize;
    int idle = 0;
    int interval = 0;

    if (conn->keepalives_idle != NULL)
        idle = atoi(conn->keepalives_idle);
    if (idle <= 0)
        idle = 2 * 60 * 60; /* 2 hours = default */

    if (conn->keepalives_interval != NULL)
        interval = atoi(conn->keepalives_interval);
    if (interval <= 0)
        interval = 1; /* 1 second = default */

    ka.onoff = 1;
    ka.keepalivetime = idle * 1000;
    ka.keepaliveinterval = interval * 1000;

    if (WSAIoctl(conn->sock, SIO_KEEPALIVE_VALS, (LPVOID)&ka, sizeof(ka), NULL, 0, &retsize, NULL, NULL) != 0) {
        appendPQExpBuffer(
            &conn->errorMessage, libpq_gettext("WSAIoctl(SIO_KEEPALIVE_VALS) failed: %ui\n"), WSAGetLastError());
        return 0;
    }
    return 1;
}
#endif /* SIO_KEEPALIVE_VALS */
#endif /* WIN32 */

/* ----------
 * connectDBStart -
 *		Begin the process of making a connection to the backend.
 *
 * Returns 1 if successful, 0 if not.
 * ----------
 */
static int connectDBStart(PGconn* conn)
{
    if (conn == NULL)
        return 0;

    if (!conn->options_valid)
        goto connect_errReturn;

    /* Ensure our buffers are empty */
    conn->inStart = conn->inCursor = conn->inEnd = 0;
    conn->outCount = 0;

    if (conn->connhost == NULL) {
        if (mutiHostlOptions(conn)) {
            goto connect_errReturn;
        }
    }

    conn->whichhost = -1;
    /* Also reset the target_server_type state if needed */
    if (conn->target_server_type == SERVER_TYPE_PREFER_STANDBY_PASS2)
        conn->target_server_type = SERVER_TYPE_PREFER_STANDBY;

nexthost:
    try_next_host(conn);

    if (conn->whichhost >= conn->nconnhost) {
        /* Keep whichhost's value within the range of the array */
        conn->whichhost--;
        goto connect_errReturn;
    }

    /*
    * The code for processing CONNECTION_NEEDED state is in PQconnectPoll(),
    * so that it can easily be re-executed if needed again during the
    * asynchronous startup process.  However, we must run it once here,
    * because callers expect a success return from this routine to mean that
    * we are in PGRES_POLLING_WRITING connection state.
    */
    if (PQconnectPoll(conn) == PGRES_POLLING_WRITING) {
        return 1;
    } else {
        goto nexthost;
    }

connect_errReturn:
    if (conn->sock >= 0) {
        pqsecure_close(conn);
        closesocket(conn->sock);
        conn->sock = -1;
    }
    conn->status = CONNECTION_BAD;
    return 0;
}

/*
 *		connectDBComplete
 *
 * Block and complete a connection.
 *
 * Returns 1 on success, 0 on failure.
 */
static int connectDBComplete(PGconn* conn)
{
    PostgresPollingStatusType flag = PGRES_POLLING_WRITING;
    time_t finish_time = ((time_t)-1);
    int    last_whichhost = -2;  /* certainly different from whichhost */
    struct addrinfo *last_addr_cur = NULL;
    int    timeout = 0;
#ifdef ENABLE_LITE_MODE
    unsigned char runTime = 0;
#endif

    if (conn == NULL || conn->status == CONNECTION_BAD)
        return 0;

    /*
     * Set up a time limit, if connect_timeout isn't zero.
     */
    if (conn->connect_timeout != NULL) {
        timeout = atoi(conn->connect_timeout);

        if (timeout > 0) {
            /*
             * Rounding could cause connection to fail; need at least 2 secs
             */
            if (timeout < 2)
                timeout = 2;
            /* calculate the finish time based on start + timeout */
            finish_time = time(NULL) + timeout;
        }
    }

#ifdef ENABLE_LITE_MODE
    PQExpBuffer errMsgBuf = createPQExpBuffer();
    if (PQExpBufferBroken(errMsgBuf)) {
        printfPQExpBuffer(&conn->errorMessage, libpq_gettext("out of memory\n"));
        conn->status = CONNECTION_BAD;
        return 0;
    }
#endif

    for (;;) {
        int    err_ret = 0;

        /*
         * (Re)start the connect_timeout timer if it's active and we are
         * considering a different host than we were last time through.  If
         * we've already succeeded, though, needn't recalculate.
         */
        if (flag != PGRES_POLLING_OK &&
            timeout > 0 &&
            (conn->whichhost != last_whichhost ||
             conn->addr_cur != last_addr_cur)) {
            finish_time = time(NULL) + timeout;
            last_whichhost = conn->whichhost;
            last_addr_cur = conn->addr_cur;
        }

        /*
         * Wait, if necessary.	Note that the initial state (just after
         * PQconnectStart) is to wait for the socket to select for writing.
         */
        switch (flag) {
            case PGRES_POLLING_OK:

                /*
                 * Reset stored error messages since we now have a working
                 * connection
                 */
                resetPQExpBuffer(&conn->errorMessage);
#ifdef ENABLE_LITE_MODE
                destroyPQExpBuffer(errMsgBuf);
#endif
                return 1; /* success! */

            case PGRES_POLLING_READING:
                if (pqWaitTimed(1, 0, conn, finish_time)) {
                    err_ret = 1;
                }
                break;

            case PGRES_POLLING_WRITING:
                if (pqWaitTimed(0, 1, conn, finish_time)) {
                    err_ret = 1;
                }
                break;

            default:
                err_ret = 1;
                break;
        }

        if (err_ret == 1) {
#ifdef ENABLE_LITE_MODE
            if (runTime % 2 == 1) {
                appendPQExpBuffer(errMsgBuf, "(connect to V1 server) ");
            } else {
                appendPQExpBuffer(errMsgBuf, "(connect to V5 server) ");
            }
            appendPQExpBufferStr(errMsgBuf, PQerrorMessage(conn));
            resetPQExpBuffer(&conn->errorMessage);

            runTime++;
            /* ENABLE_LITE_MODE is compatible with V1 server, */
            /* and the database name of V1 server is uppercase letters. */
            /* So, we need to attempt to connect to the server twice. */
            if (runTime % 2 == 1) {
                for (; conn->whichhost < conn->nconnhost; conn->whichhost++) {
                    reset_physical_connection(conn);

                    reset_connection_state_machine(conn);

                    if (resolve_host_addr(conn)) {
                        break;
                    }
                }
                conn->pversion = PG_PROTOCOL(3, 0);
                conn->dbName = pg_strtoupper(conn->dbName);
            } else {
                /*
                 * Give up on current server/address, try the next one.
                 */
                try_next_host(conn);

                if (conn->whichhost >= conn->nconnhost) {
                    /* Keep whichhost's value within the range of the array */
                    conn->whichhost--;
                    /* Just in case we failed to set it in PQconnectPoll */
                    conn->status = CONNECTION_BAD;

                    resetPQExpBuffer(&conn->errorMessage);
                    appendPQExpBufferStr(&conn->errorMessage, errMsgBuf->data);
                    destroyPQExpBuffer(errMsgBuf);

                    return 0;
                }
                runTime = 0;
                conn->dbName = pg_strtolower(conn->dbName);
            }
#else
            /*
             * Give up on current server/address, try the next one.
             */
            try_next_host(conn);

            if (conn->whichhost >= conn->nconnhost) {
                /* Keep whichhost's value within the range of the array */
                conn->whichhost--;
                /* Just in case we failed to set it in PQconnectPoll */
                conn->status = CONNECTION_BAD;
                return 0;
            }
#endif
        }

        /*
         * Now try to advance the state machine.
         */
        flag = PQconnectPoll(conn);
    }
}

static void PQgetLibpath(char* libpath, int libpathLen)
{
    const char* fname = NULL;
    int fnameIndex = 0;
    int libpathIndex = 0;

    if (libpath == NULL || libpathLen <= 0) {
        return;
    }
#ifndef WIN32
    Dl_info dl_info;
    int ret = dladdr((void*)PQgetLibpath, &dl_info);
    if (ret != 0) {
        fname = dl_info.dli_fname;
    }
#else
    MEMORY_BASIC_INFORMATION mbi;
    char fpath[4096] = {'\0'};
    if ((VirtualQuery(PQgetLibpath, &mbi, sizeof(mbi)) != 0) &&
        GetModuleFileName((HMODULE)mbi.AllocationBase, fpath, sizeof(fpath))) {
        fname = fpath;
    }
#endif

    if (fname != NULL) {
        while ((fname[fnameIndex] != '\0') && (libpathIndex < libpathLen - 1)) {
            if (fname[fnameIndex] == '\'') {
                libpath[libpathIndex++] = '\'';
            } else if ((fname[fnameIndex] == '"') || (fname[fnameIndex] == '\\')) {
                libpath[libpathIndex++] = '\\';
            } else {
                libpath[libpathIndex++] = fname[fnameIndex++];
            }
        }
        libpath[libpathIndex] = '\0';
    } else
        return;
}

static void PQgetOSUser(char* username, int usernameLen)
{
    if (username == NULL || usernameLen <= 0) {
        return;
    }
#ifndef WIN32
    struct passwd* pw = getpwuid(geteuid());
    if (pw == NULL) {
        username[0] = '\0';
    } else {
        errno_t rc = strncpy_s(username, usernameLen, pw->pw_name, strlen(pw->pw_name));
        securec_check_c(rc, "\0", "\0");
    }
#else
    DWORD len = usernameLen;
    if (!GetUserName(username, &len)) {
        username[0] = '\0';
    }
#endif
}

static void connectSetConninfo(PGconn* conn)
{
    const char* query = NULL;
    if (conn == NULL || conn->status == CONNECTION_BAD) {
        return;
    }

    query = "select name, setting from pg_settings where name in ('connection_info')";
    PGresult* res = PQexec(conn, query);
    if (PQresultStatus(res) != PGRES_TUPLES_OK || PQntuples(res) != 1) {
        PQclear(res);
        return;
    }
    PQclear(res);
    res = NULL;

    if (conn->connection_info == NULL && libpqVersionString != NULL) {
        char tmp[8192] = {'\0'};
        if (conn->connection_extra_info) {
            char libpath[4096] = {'\0'};
            char username[128] = {'\0'};

            (void)PQgetLibpath(libpath, sizeof(libpath));
            (void)PQgetOSUser(username, sizeof(username));

            check_sprintf_s(sprintf_s(tmp,
                sizeof(tmp),
                "{\"driver_name\":\"libpq\",\"driver_version\":\"%s\",\"driver_path\":\"%s\",\"os_user\":\"%s\"}",
                libpqVersionString,
                libpath,
                username));
        } else {
            check_sprintf_s(sprintf_s(
                tmp, sizeof(tmp), "{\"driver_name\":\"libpq\",\"driver_version\":\"%s\"}", libpqVersionString));
        }

        conn->connection_info = strdup(tmp);
        if (conn->connection_info == NULL)
            return;
    }

    char setQuery[8192] = {'\0'};
    check_sprintf_s(sprintf_s(setQuery, sizeof(setQuery), "SET connection_info = '%s'", conn->connection_info));

    PGresult* setres = PQexec(conn, setQuery);
    PQclear(setres);
    setres = NULL;
}

/* ----------------
 *		PQconnectPoll
 *
 * Poll an asynchronous connection.
 *
 * Returns a PostgresPollingStatusType.
 * Before calling this function, use select(2) to determine when data
 * has arrived..
 *
 * You must call PQfinish whether or not this fails.
 *
 * This function and PQconnectStart are intended to allow connections to be
 * made without blocking the execution of your program on remote I/O. However,
 * there are a number of caveats:
 *
 *	 o	If you call PQtrace, ensure that the stream object into which you trace
 *		will not block.
 *	 o	If you do not supply an IP address for the remote host (i.e. you
 *		supply a host name instead) then PQconnectStart will block on
 *		gethostbyname.	You will be fine if using Unix sockets (i.e. by
 *		supplying neither a host name nor a host address).
 *	 o	If your backend wants to use Kerberos authentication then you must
 *		supply both a host name and a host address, otherwise this function
 *		may block on gethostname.
 *
 * ----------------
 */
PostgresPollingStatusType PQconnectPoll(PGconn* conn)
{
    PGresult* res = NULL;
    char sebuf[256];
    int optval;

    if (conn == NULL)
        return PGRES_POLLING_FAILED;

    /* Get the new data */
    switch (conn->status) {
            /*
             * We really shouldn't have been polled in these two cases, but we
             * can handle it.
             */
        case CONNECTION_BAD:
            return PGRES_POLLING_FAILED;
        case CONNECTION_OK:
            return PGRES_POLLING_OK;

            /* These are reading states */
        case CONNECTION_AWAITING_RESPONSE:
        case CONNECTION_AUTH_OK:
        case CONNECTION_CHECK_WRITABLE:
        case CONNECTION_CONSUME:
        case CONNECTION_CHECK_STANDBY: {
            /* Load waiting data */
            int n = pqReadData(conn);

            if (n < 0)
                goto error_return;
            if (n == 0)
                return PGRES_POLLING_READING;

            break;
        }

            /* These are writing states, so we just proceed. */
        case CONNECTION_STARTED:
        case CONNECTION_MADE:
            break;

            /* We allow pqSetenvPoll to decide whether to proceed. */
        case CONNECTION_SETENV:
            break;

            /* Special cases: proceed without waiting. */
        case CONNECTION_SSL_STARTUP:
        case CONNECTION_NEEDED:
        case CONNECTION_CHECK_TARGET:
            break;

        default:
            appendPQExpBuffer(&conn->errorMessage,
                libpq_gettext("invalid connection state, "
                              "probably indicative of memory corruption\n"));
            goto error_return;
    }

keep_going: /* We will come back to here until there is
             * nothing left to do. */
    switch (conn->status) {
        case CONNECTION_NEEDED: {
            /*
             * Try to initiate a connection to one of the addresses
             * returned by pg_getaddrinfo_all().  conn->addr_cur is the
             * next one to try. We fail when we run out of addresses.
             */
            while (conn->addr_cur != NULL) {
                struct addrinfo* addr_cur = conn->addr_cur;

                /* Remember current address for possible error msg
                 * ai_addrlen is size_t > SECUREC_MEM_MAX_LEN
                 */
                check_memcpy_s(
                    memcpy_s(&conn->raddr.addr, sizeof(conn->raddr.addr), addr_cur->ai_addr, addr_cur->ai_addrlen));

                conn->raddr.salen = addr_cur->ai_addrlen;

                /* Open a socket */
                conn->sock = socket(addr_cur->ai_family, SOCK_STREAM, 0);
                if (conn->sock < 0) {
                    /*
                     * ignore socket() failure if we have more addresses
                     * to try
                     */
                    if (addr_cur->ai_next != NULL) {
                        conn->addr_cur = addr_cur->ai_next;
                        continue;
                    }
                    appendPQExpBuffer(&conn->errorMessage,
                        libpq_gettext("could not create socket: %s\n"),
                        SOCK_STRERROR(SOCK_ERRNO, sebuf, sizeof(sebuf)));
                    break;
                }

                /*
                 * Select socket options: no delay of outgoing data for
                 * TCP sockets, nonblock mode, close-on-exec. Fail if any
                 * of this fails.
                 */
                if (!IS_AF_UNIX(addr_cur->ai_family) && !connectNoDelay(conn)) {
                    closesocket(conn->sock);
                    conn->sock = -1;
                    conn->addr_cur = addr_cur->ai_next;
                    continue;
                }
                if (
#ifndef WIN32
                    !IS_AF_UNIX(addr_cur->ai_family) &&
#endif
                    !pg_set_noblock(conn->sock)) {
                    appendPQExpBuffer(&conn->errorMessage,
                        libpq_gettext("could not set socket to non-blocking mode: %s\n"),
                        SOCK_STRERROR(SOCK_ERRNO, sebuf, sizeof(sebuf)));
                    closesocket(conn->sock);
                    conn->sock = -1;
                    conn->addr_cur = addr_cur->ai_next;
                    continue;
                }

#ifdef F_SETFD
                if (fcntl(conn->sock, F_SETFD, FD_CLOEXEC) == -1) {
                    appendPQExpBuffer(&conn->errorMessage,
                        libpq_gettext("could not set socket to close-on-exec mode: %s\n"),
                        SOCK_STRERROR(SOCK_ERRNO, sebuf, sizeof(sebuf)));
                    closesocket(conn->sock);
                    conn->sock = -1;
                    conn->addr_cur = addr_cur->ai_next;
                    continue;
                }
#endif /* F_SETFD */

                if (!IS_AF_UNIX(addr_cur->ai_family)) {
#ifndef WIN32
                    int on = 1;
#endif
                    int usekeepalives = useKeepalives(conn);
                    int err = 0;

                    if (usekeepalives < 0) {
                        appendPQExpBuffer(
                            &conn->errorMessage, libpq_gettext("keepalives parameter must be an integer\n"));
                        err = 1;
                    } else if (usekeepalives == 0) {
                        /* Do nothing */
                    }
#ifndef WIN32
                    else if (setsockopt(conn->sock, SOL_SOCKET, SO_KEEPALIVE, (char*)&on, sizeof(on)) < 0) {
                        appendPQExpBuffer(&conn->errorMessage,
                            libpq_gettext("setsockopt(SO_KEEPALIVE) failed: %s\n"),
                            SOCK_STRERROR(SOCK_ERRNO, sebuf, sizeof(sebuf)));
                        err = 1;
                    } else if (!setKeepalivesIdle(conn) || !setKeepalivesInterval(conn) || !setKeepalivesCount(conn))
                        err = 1;
#else /* WIN32 */
#ifdef SIO_KEEPALIVE_VALS
                    else if (!setKeepalivesWin32(conn))
                        err = 1;
#endif /* SIO_KEEPALIVE_VALS */
#endif /* WIN32 */

                    if (err) {
                        closesocket(conn->sock);
                        conn->sock = -1;
                        conn->addr_cur = addr_cur->ai_next;
                        continue;
                    }
                }

                /* ----------
                 * We have three methods of blocking SIGPIPE during
                 * send() calls to this socket:
                 *
                 *	- setsockopt(sock, SO_NOSIGPIPE)
                 *	- send(sock, ..., MSG_NOSIGNAL)
                 *	- setting the signal mask to SIG_IGN during send()
                 *
                 * The third method requires three syscalls per send,
                 * so we prefer either of the first two, but they are
                 * less portable.  The state is tracked in the following
                 * members of PGconn:
                 *
                 * conn->sigpipe_so		- we have set up SO_NOSIGPIPE
                 * conn->sigpipe_flag	- we're specifying MSG_NOSIGNAL
                 *
                 * If we can use SO_NOSIGPIPE, then set sigpipe_so here
                 * and we're done.  Otherwise, set sigpipe_flag so that
                 * we will try MSG_NOSIGNAL on sends.  If we get an error
                 * with MSG_NOSIGNAL, we'll clear that flag and revert to
                 * signal masking.
                 * ----------
                 */
                conn->sigpipe_so = false;
#ifdef MSG_NOSIGNAL
                conn->sigpipe_flag = true;
#else
                conn->sigpipe_flag = false;
#endif /* MSG_NOSIGNAL */

#ifdef SO_NOSIGPIPE
                optval = 1;
                if (setsockopt(conn->sock, SOL_SOCKET, SO_NOSIGPIPE, (char*)&optval, sizeof(optval)) == 0) {
                    conn->sigpipe_so = true;
                    conn->sigpipe_flag = false;
                }
#endif /* SO_NOSIGPIPE */

#ifndef WIN32

                if (IS_AF_UNIX(addr_cur->ai_family) && conn->connect_timeout != NULL) {
                    struct timeval timeout = {0, 0};
                    timeout.tv_sec = atoi(conn->connect_timeout);
                    (void)setsockopt(conn->sock, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
                }
#endif
                /* Random_Port_Reuse need set SO_REUSEADDR on */
                if (!IS_AF_UNIX(addr_cur->ai_family)) {
                    int on = 1;

                    if ((setsockopt(conn->sock, SOL_SOCKET, SO_REUSEADDR, (char*)&on, sizeof(on))) == -1) {
                        appendPQExpBuffer(
                            &conn->errorMessage, libpq_gettext("setsockopt(SO_REUSEADDR) failed: %d\n"), errno);
                        closesocket(conn->sock);
                        conn->sock = -1;
                        conn->addr_cur = addr_cur->ai_next;
                        continue;
                    }
                }

                char *bind_addr = tcp_link_addr;

#ifdef ENABLE_LITE_MODE
                /* For replication connection request under lite mode, client ip bind is forced */
                if ((bind_addr == NULL || strcmp(bind_addr, "0.0.0.0") == 0) &&
                    conn->replication != NULL && strcmp(conn->pglocalhost, "0.0.0.0") != 0) {
                    comm_client_bind = true;
                    bind_addr = conn->pglocalhost;
                }
#endif

                if (!IS_AF_UNIX(addr_cur->ai_family)
                    && comm_client_bind
                    && bind_addr != NULL
                    && strcmp(bind_addr, "0.0.0.0") != 0) {
                    struct addrinfo* addr = NULL;
                    struct addrinfo hint;
                    int ret;
                    int trybindnum = 10;

                    check_memset_s(memset_s(&hint, sizeof(hint), 0, sizeof(hint)));
                    hint.ai_family = AF_UNSPEC;
                    hint.ai_flags = AI_PASSIVE;
                    hint.ai_socktype = SOCK_STREAM;

                    ret = pg_getaddrinfo_all(bind_addr, NULL, &hint, &addr);
                    if (ret || addr == NULL) {
                        appendPQExpBuffer(&conn->errorMessage,
                            libpq_gettext("could not translate host name: %s, please check "
                                          "local_bind_address:\"%s\" and pglocalhost:\"%s\"!\n"),
                            gai_strerror(ret),
                            tcp_link_addr,
                            conn->pglocalhost);
                        if (addr != NULL)
                            pg_freeaddrinfo_all(hint.ai_family, addr);

                        closesocket(conn->sock);
                        conn->sock = -1;
                        conn->addr_cur = addr_cur->ai_next;
                        continue;
                    }

                    while (trybindnum--) {
                        ret = bind(conn->sock, addr->ai_addr, addr->ai_addrlen);
                        if (ret < 0 && errno != EINVAL) {
                            appendPQExpBuffer(&conn->errorMessage,
                                libpq_gettext("Connection bind %s is not successfull and errno[%d]:%m, "
                                              "please check if the port is sufficient\n"),
                                bind_addr,
                                errno);
#ifdef WIN32
                            Sleep(1000);
#else
                            (void)usleep(1000000L);
#endif
                            continue;
                        } else
                            break;
                    }

                    if (ret < 0 && errno != EINVAL) {
                        if (addr != NULL)
                            pg_freeaddrinfo_all(hint.ai_family, addr);

                        closesocket(conn->sock);
                        conn->sock = -1;
                        conn->addr_cur = addr_cur->ai_next;
                        continue;
                    }

                    if (addr != NULL)
                        pg_freeaddrinfo_all(hint.ai_family, addr);
                }

                /*
                 * Start/make connection.  This should not block, since we
                 * are in nonblock mode.  If it does, well, too bad.
                 */
                if (connect(conn->sock, addr_cur->ai_addr, addr_cur->ai_addrlen) < 0) {
#ifndef WIN32
                    if (IS_AF_UNIX(addr_cur->ai_family)) {
                        appendPQExpBuffer(&conn->errorMessage,
                            libpq_gettext("connect to server failed: %s\n"),
                            SOCK_STRERROR(SOCK_ERRNO, sebuf, sizeof(sebuf)));
                        closesocket(conn->sock);
                        conn->sock = -1;
                        conn->addr_cur = addr_cur->ai_next;
                        continue;
                    }
#endif
                    if (SOCK_ERRNO == EINPROGRESS || SOCK_ERRNO == EWOULDBLOCK || SOCK_ERRNO == EINTR ||
                        SOCK_ERRNO == 0) {
                        /*
                         * This is fine - we're in non-blocking mode, and
                         * the connection is in progress.  Tell caller to
                         * wait for write-ready on socket.
                         */
                        conn->status = CONNECTION_STARTED;
                        return PGRES_POLLING_WRITING;
                    }
                    /* otherwise, trouble */
                } else {
#ifndef WIN32
                    if (IS_AF_UNIX(addr_cur->ai_family)) {
                        if (conn->connect_timeout != NULL) {
                            struct timeval timeout = {0, 0};
                            (void)setsockopt(conn->sock, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
                        }
                        if (!pg_set_noblock(conn->sock)) {
                            appendPQExpBuffer(&conn->errorMessage,
                                libpq_gettext("could not set unix domain socket to non-blocking mode: %s\n"),
                                SOCK_STRERROR(SOCK_ERRNO, sebuf, sizeof(sebuf)));
                            fprintf(stdout, "could not set unix domain socket to non-blocking mode: %s, remote datanode %s.\n",
                                SOCK_STRERROR(SOCK_ERRNO, sebuf, sizeof(sebuf)), conn->remote_nodename);
                            closesocket(conn->sock);
                            conn->sock = -1;
                            conn->addr_cur = addr_cur->ai_next;
                            continue;
                        }
                    }

#endif
                    /*
                     * Hm, we're connected already --- seems the "nonblock
                     * connection" wasn't.  Advance the state machine and
                     * go do the next stuff.
                     */
                    conn->status = CONNECTION_STARTED;
                    goto keep_going;
                }

                /*
                 * This connection failed --- set up error report, then
                 * close socket (do it this way in case close() affects
                 * the value of errno...).	We will ignore the connect()
                 * failure and keep going if there are more addresses.
                 */
                connectFailureMessage(conn, SOCK_ERRNO);
                if (conn->sock >= 0) {
                    closesocket(conn->sock);
                    conn->sock = -1;
                }

                /*
                 * Try the next address, if any.
                 */
                conn->addr_cur = addr_cur->ai_next;
            } /* loop over addresses */

            /*
             * Ooops, no more addresses.  An appropriate error message is
             * already set up, so just set the right status.
             */
            goto error_return;
        }

        case CONNECTION_STARTED: {
            ACCEPT_TYPE_ARG3 optlen = sizeof(optval);

            /*
             * Write ready, since we've made it here, so the connection
             * has been made ... or has failed.
             */

            /*
             * Now check (using getsockopt) that there is not an error
             * state waiting for us on the socket.
             */

            if (getsockopt(conn->sock, SOL_SOCKET, SO_ERROR, (char*)&optval, &optlen) == -1) {
                appendPQExpBuffer(&conn->errorMessage,
                    libpq_gettext("could not get socket error status: %s\n"),
                    SOCK_STRERROR(SOCK_ERRNO, sebuf, sizeof(sebuf)));
                goto error_return;
            } else if (optval != 0) {
                /*
                 * When using a nonblocking connect, we will typically see
                 * connect failures at this point, so provide a friendly
                 * error message.
                 */
                connectFailureMessage(conn, SOCK_ERRNO);

                /*
                 * If more addresses remain, keep trying, just as in the
                 * case where connect() returned failure immediately.
                 */
                if (conn->addr_cur->ai_next != NULL) {
                    if (conn->sock >= 0) {
                        closesocket(conn->sock);
                        conn->sock = -1;
                    }
                    conn->addr_cur = conn->addr_cur->ai_next;
                    conn->status = CONNECTION_NEEDED;
                    goto keep_going;
                }
                goto error_return;
            }

            /* Fill in the client address */
            conn->laddr.salen = sizeof(conn->laddr.addr);
            if (getsockname(conn->sock, (struct sockaddr*)&conn->laddr.addr, &conn->laddr.salen) < 0) {
                appendPQExpBuffer(&conn->errorMessage,
                    libpq_gettext("could not get client address from socket: %s\n"),
                    SOCK_STRERROR(SOCK_ERRNO, sebuf, sizeof(sebuf)));
                goto error_return;
            }

            /*
             * Make sure we can write before advancing to next step.
             */
            conn->status = CONNECTION_MADE;
            return PGRES_POLLING_WRITING;
        }

        case CONNECTION_MADE: {
            char* startpacket = NULL;
            int packetlen;

#ifdef HAVE_UNIX_SOCKETS

            /*
             * Implement requirepeer check, if requested and it's a
             * Unix-domain socket.
             */
            if ((conn->requirepeer != NULL) && conn->requirepeer[0] && IS_AF_UNIX(conn->raddr.addr.ss_family)) {
                char pwdbuf[BUFSIZ];
                struct passwd pass_buf;
                struct passwd* pass = NULL;
                uid_t uid;
                gid_t gid;

                errno = 0;
                if (getpeereid(conn->sock, &uid, &gid) != 0) {
                    /*
                     * Provide special error message if getpeereid is a
                     * stub
                     */
                    if (errno == ENOSYS)
                        appendPQExpBuffer(&conn->errorMessage,
                            libpq_gettext("requirepeer parameter is not supported on this platform\n"));
                    else
                        appendPQExpBuffer(&conn->errorMessage,
                            libpq_gettext("could not get peer credentials: %s\n"),
                            pqStrerror(errno, sebuf, sizeof(sebuf)));
                    goto error_return;
                }

                (void)syscalllockAcquire(&getpwuid_lock);  // the getpwuid be used in pqGetpwuid is not thread safe
                (void)pqGetpwuid(uid, &pass_buf, pwdbuf, sizeof(pwdbuf), &pass);

                if (pass == NULL) {
                    (void)syscalllockRelease(&getpwuid_lock);
                    appendPQExpBuffer(
                        &conn->errorMessage, libpq_gettext("local user with ID %d does not exist\n"), (int)uid);
                    goto error_return;
                }

                if (strcmp(pass->pw_name, conn->requirepeer) != 0) {
                    appendPQExpBuffer(&conn->errorMessage,
                        libpq_gettext("requirepeer specifies \"%s\", but actual peer user name is \"%s\"\n"),
                        conn->requirepeer,
                        pass->pw_name);
                    (void)syscalllockRelease(&getpwuid_lock);
                    goto error_return;
                }
                (void)syscalllockRelease(&getpwuid_lock);
            }
#endif /* HAVE_UNIX_SOCKETS */

#ifdef USE_SSL

            /*
             * If SSL is enabled and we haven't already got it running,
             * request it instead of sending the startup message.
             */
            if (IS_AF_UNIX(conn->raddr.addr.ss_family)) {
                /* Don't bother requesting SSL over a Unix socket */
                conn->allow_ssl_try = false;
            }
            /* We only try SSL in outer connection, inner connection will not use SSL */
            if ((*conn->pgoptions == 0 || (strstr(conn->pgoptions, "remotetype") == NULL)) &&
                conn->allow_ssl_try && !conn->wait_ssl_try && conn->ssl == NULL)

            {
                ProtocolVersion pv;

                /*
                 * Send the SSL request packet.
                 *
                 * Theoretically, this could block, but it really
                 * shouldn't since we only got here if the socket is
                 * write-ready.
                 */
                pv = htonl(NEGOTIATE_SSL_CODE);
                if (pqPacketSend(conn, 0, &pv, sizeof(pv)) != STATUS_OK) {
                    // rest error message and insert more specific information
                    resetPQExpBuffer(&conn->errorMessage);
                    appendPQExpBuffer(&conn->errorMessage,
                        libpq_gettext("could not send SSL negotiation packet: %s\n"),
                        SOCK_STRERROR(SOCK_ERRNO, sebuf, sizeof(sebuf)));
                    goto error_return;
                }
                /* Ok, wait for response */
                conn->status = CONNECTION_SSL_STARTUP;
                return PGRES_POLLING_READING;
            }
#endif /* USE_SSL */

            /*
             * Build the startup packet.
             */
            if (PG_PROTOCOL_MAJOR(conn->pversion) >= 3)
                startpacket = pqBuildStartupPacket3(conn, &packetlen, EnvironmentOptions);
            else
                startpacket = pqBuildStartupPacket2(conn, &packetlen, EnvironmentOptions);
            if (startpacket == NULL) {
                /*
                 * will not appendbuffer here, since it's likely to also
                 * run out of memory
                 */
                if (conn->errorMessage.data == NULL)
                    printfPQExpBuffer(&conn->errorMessage, libpq_gettext("out of memory\n"));
                else if (conn->errorMessage.data[0] == 0)
                    appendPQExpBuffer(&conn->errorMessage, libpq_gettext("out of memory\n"));
                goto error_return;
            }

            /*
             * Send the startup packet.
             *
             * Theoretically, this could block, but it really shouldn't
             * since we only got here if the socket is write-ready.
             */
            if (pqPacketSend(conn, 0, startpacket, packetlen) != STATUS_OK) {
                // rest error message and insert more specific information
                resetPQExpBuffer(&conn->errorMessage);
                appendPQExpBuffer(&conn->errorMessage,
                    libpq_gettext("could not send startup packet: %s\n"
                                  "localhost: %s, localport: %s, remotehost: %s, remoteaddr: %s, remoteport:%s\n"),
                    SOCK_STRERROR(SOCK_ERRNO, sebuf, sizeof(sebuf)),
                    conn->pglocalhost, conn->pglocalport, conn->connhost[conn->whichhost].host,
                    conn->connhost[conn->whichhost].hostaddr, conn->connhost[conn->whichhost].port);
                libpq_free(startpacket);
                goto error_return;
            }

            libpq_free(startpacket);

            conn->status = CONNECTION_AWAITING_RESPONSE;
            return PGRES_POLLING_READING;
        }

            /*
             * Handle SSL negotiation: wait for postmaster messages and
             * respond as necessary.
             */
        case CONNECTION_SSL_STARTUP: {
#ifdef USE_SSL
            PostgresPollingStatusType pollres;

            /*
             * On first time through, get the postmaster's response to our
             * SSL negotiation packet.
             */
            if (conn->ssl == NULL) {
                /*
                 * We use pqReadData here since it has the logic to
                 * distinguish no-data-yet from connection closure. Since
                 * conn->ssl isn't set, a plain recv() will occur.
                 */
                char SSLok;
                int rdresult;

                rdresult = pqReadData(conn);
                if (rdresult < 0) {
                    /* errorMessage is already filled in */
                    goto error_return;
                }
                if (rdresult == 0) {
                    /* caller failed to wait for data */
                    return PGRES_POLLING_READING;
                }
                if (pqGetc(&SSLok, conn) < 0) {
                    /* should not happen really */
                    return PGRES_POLLING_READING;
                }
                if (SSLok == 'S') {
                    /* mark byte consumed */
                    conn->inStart = conn->inCursor;
                    /* Set up global SSL state if required */
                    if (pqsecure_initialize(conn) != 0)
                        goto error_return;
                } else if (SSLok == 'N') {
                    /* mark byte consumed */
                    conn->inStart = conn->inCursor;
                    /* OK to do without SSL? */
                    if (conn->sslmode[0] == 'r' || /* "require" */
                        conn->sslmode[0] == 'v') { /* "verify-ca" or
                                                    * "verify-full" */
                        /* Require SSL, but server does not want it */
                        appendPQExpBuffer(
                            &conn->errorMessage, libpq_gettext("server does not support SSL, but SSL was required\n"));
                        goto error_return;
                    }
                    /* Otherwise, proceed with normal startup */
                    conn->allow_ssl_try = false;
                    conn->status = CONNECTION_MADE;
                    return PGRES_POLLING_WRITING;
                } else if (SSLok == 'E') {
                    /*
                     * Server failure of some sort, such as failure to
                     * fork a backend process.	We need to process and
                     * report the error message, which might be formatted
                     * according to either protocol 2 or protocol 3.
                     * Rather than duplicate the code for that, we flip
                     * into AWAITING_RESPONSE state and let the code there
                     * deal with it.  Note we have *not* consumed the "E"
                     * byte here.
                     */
                    conn->status = CONNECTION_AWAITING_RESPONSE;
                    goto keep_going;
                } else {
                    appendPQExpBuffer(&conn->errorMessage,
                        libpq_gettext("received invalid response to SSL negotiation: %c\n"),
                        SSLok);
                    goto error_return;
                }
            }

            /*
             * Begin or continue the SSL negotiation process.
             */
            pollres = pqsecure_open_client(conn);
            if (pollres == PGRES_POLLING_OK) {
                /* SSL handshake done, ready to send startup packet */
                conn->status = CONNECTION_MADE;
                return PGRES_POLLING_WRITING;
            }
            if (pollres == PGRES_POLLING_FAILED) {
                /*
                 * Failed ... if sslmode is "prefer" then do a non-SSL
                 * retry
                 */
                if (conn->sslmode[0] == 'p' /* "prefer" */
                    && conn->allow_ssl_try  /* redundant? */
                    && !conn->wait_ssl_try) { /* redundant? */
                    /* only retry once */
                    conn->allow_ssl_try = false;
                    /* Must drop the old connection */
                    closesocket(conn->sock);
                    conn->sock = -1;
                    conn->status = CONNECTION_NEEDED;
                    /* Discard any unread/unsent data */
                    conn->inStart = conn->inCursor = conn->inEnd = 0;
                    conn->outCount = 0;
                    goto keep_going;
                }
            }
            return pollres;
#else  /* !USE_SSL */
            /* can't get here */
            goto error_return;
#endif /* USE_SSL */
        }

            /*
             * Handle authentication exchange: wait for postmaster messages
             * and respond as necessary.
             */
        case CONNECTION_AWAITING_RESPONSE: {
            char beresp;
            int msgLength;
            int avail;
            AuthRequest areq;

            /*
             * Scan the message from current point (note that if we find
             * the message is incomplete, we will return without advancing
             * inStart, and resume here next time).
             */
            conn->inCursor = conn->inStart;

            /* Read type byte */
            if (pqGetc(&beresp, conn)) {
                /* We'll come back when there is more data */
                return PGRES_POLLING_READING;
            }

            /*
             * Validate message type: we expect only an authentication
             * request or an error here.  Anything else probably means
             * it's not openGauss on the other end at all.
             */
            if (!(beresp == 'R' || beresp == 'E')) {
                appendPQExpBuffer(&conn->errorMessage,
                    libpq_gettext("expected authentication request from "
                                  "server, but received %c\n"),
                    beresp);
                goto error_return;
            }

            if (PG_PROTOCOL_MAJOR(conn->pversion) >= 3) {
                /* Read message length word */
                if (pqGetInt(&msgLength, 4, conn)) {
                    /* We'll come back when there is more data */
                    return PGRES_POLLING_READING;
                }
            } else {
                /* Set phony message length to disable checks below */
                msgLength = 8;
            }

            /*
             * Try to validate message length before using it.
             * Authentication requests can't be very large, although GSS
             * auth requests may not be that small.  Errors can be a
             * little larger, but not huge.  If we see a large apparent
             * length in an error, it means we're really talking to a
             * pre-3.0-protocol server; cope.
             */
            if (beresp == 'R' && (msgLength < 8 || msgLength > 2000)) {
                appendPQExpBuffer(&conn->errorMessage,
                    libpq_gettext("expected authentication request from "
                                  "server, but received %c\n"),
                    beresp);
                goto error_return;
            }

            if (beresp == 'E' && (msgLength < 8 || msgLength > 30000)) {
                /* Handle error from a pre-3.0 server */
                conn->inCursor = conn->inStart + 1; /* reread data */
                if (pqGets_append(&conn->errorMessage, conn)) {
                    /* We'll come back when there is more data */
                    return PGRES_POLLING_READING;
                }
                /* OK, we read the message; mark data consumed */
                conn->inStart = conn->inCursor;

                /*
                 * The postmaster typically won't end its message with a
                 * newline, so add one to conform to libpq conventions.
                 */
                appendPQExpBufferChar(&conn->errorMessage, '\n');

                /*
                 * If we tried to open the connection in 3.0 protocol,
                 * fall back to 2.0 protocol.
                 */
                if (PG_PROTOCOL_MAJOR(conn->pversion) >= 3) {
                    conn->pversion = PG_PROTOCOL(2, 0);
                    /* Must drop the old connection */
                    pqsecure_close(conn);
                    closesocket(conn->sock);
                    conn->sock = -1;
                    conn->status = CONNECTION_NEEDED;
                    /* Discard any unread/unsent data */
                    conn->inStart = conn->inCursor = conn->inEnd = 0;
                    conn->outCount = 0;
                    goto keep_going;
                }

                goto error_return;
            }

            /*
             * Can't process if message body isn't all here yet.
             *
             * (In protocol 2.0 case, we are assuming messages carry at
             * least 4 bytes of data.)
             */
            msgLength -= 4;
            avail = conn->inEnd - conn->inCursor;
            if (avail < msgLength) {
                /*
                 * Before returning, try to enlarge the input buffer if
                 * needed to hold the whole message; see notes in
                 * pqParseInput3.
                 */
                if (pqCheckInBufferSpace(conn->inCursor + (size_t)msgLength, conn))
                    goto error_return;
                /* We'll come back when there is more data */
                return PGRES_POLLING_READING;
            }

            /* Handle errors. */
            if (beresp == 'E') {
                if (PG_PROTOCOL_MAJOR(conn->pversion) >= 3) {
                    if (pqGetErrorNotice3(conn, true)) {
                        /* We'll come back when there is more data */
                        return PGRES_POLLING_READING;
                    }
                } else {
                    if (pqGets_append(&conn->errorMessage, conn)) {
                        /* We'll come back when there is more data */
                        return PGRES_POLLING_READING;
                    }
                }
                /* OK, we read the message; mark data consumed */
                conn->inStart = conn->inCursor;

#ifdef USE_SSL

                /*
                 * if sslmode is "allow" and we haven't tried an SSL
                 * connection already, then retry with an SSL connection
                 */
                if (conn->sslmode[0] == 'a' /* "allow" */
                    && conn->ssl == NULL && conn->allow_ssl_try && conn->wait_ssl_try) {
                    /* only retry once */
                    conn->wait_ssl_try = false;
                    /* Must drop the old connection */
                    closesocket(conn->sock);
                    conn->sock = -1;
                    conn->status = CONNECTION_NEEDED;
                    /* Discard any unread/unsent data */
                    conn->inStart = conn->inCursor = conn->inEnd = 0;
                    conn->outCount = 0;
                    goto keep_going;
                }

                /*
                 * if sslmode is "prefer" and we're in an SSL connection,
                 * then do a non-SSL retry
                 */
                if (conn->sslmode[0] == 'p'                        /* "prefer" */
                    && conn->allow_ssl_try && !conn->wait_ssl_try) { /* redundant? */
                    /* only retry once */
                    conn->allow_ssl_try = false;
                    /* Must drop the old connection */
                    pqsecure_close(conn);
                    closesocket(conn->sock);
                    conn->sock = -1;
                    conn->status = CONNECTION_NEEDED;
                    /* Discard any unread/unsent data */
                    conn->inStart = conn->inCursor = conn->inEnd = 0;
                    conn->outCount = 0;
                    goto keep_going;
                }
#endif

                goto error_return;
            }

            /* It is an authentication request. */
            conn->auth_req_received = true;

            /* Get the type of request. */
            if (pqGetInt((int*)&areq, 4, conn)) {
                /* We'll come back when there are more data */
                return PGRES_POLLING_READING;
            }

            /* Get the password salt if there is one. */
            if (areq == AUTH_REQ_MD5) {
                if (pqGetnchar(conn->md5Salt, sizeof(conn->md5Salt), conn)) {
                    /* We'll come back when there are more data */
                    return PGRES_POLLING_READING;
                }
            }
            if (areq == AUTH_REQ_MD5_SHA256) {
                if (pqGetnchar(conn->salt, SALT_LENGTH * 2, conn)) {
                    /* We'll come back when there are more data */
                    return PGRES_POLLING_READING;
                }

                if (pqGetnchar(conn->md5Salt, sizeof(conn->md5Salt), conn)) {
                    /* We'll come back when there are more data */
                    return PGRES_POLLING_READING;
                }
            }
            if ((areq == AUTH_REQ_SHA256)
#ifdef ENABLE_LITE_MODE
                || (areq == AUTH_REQ_SHA256_RFC)
#endif
                ) {
                if (pqGetInt((int*)(&(conn->password_stored_method)), 4, conn)) {
                    return PGRES_POLLING_READING;
                }

                if ((SHA256_PASSWORD == conn->password_stored_method)
#ifdef ENABLE_LITE_MODE
                    || (SHA256_PASSWORD_RFC == conn->password_stored_method)
#endif
                    ) {
                    if (pqGetnchar(conn->salt, SALT_LENGTH * 2, conn)) {
                        /* We'll come back when there are more data */
                        return PGRES_POLLING_READING;
                    }
                    conn->salt[SALT_LENGTH * 2] = '\0';

                    if (pqGetnchar(conn->token, TOKEN_LENGTH * 2, conn)) {
                        /* We'll come back when there are more data */
                        return PGRES_POLLING_READING;
                    }
                    conn->token[TOKEN_LENGTH * 2] = '\0';

                    /*
                     * Recivce the sever_signature before client_signature is not safe.
                     * need to : The rfc5802 authentication protocol need be enhanced.
                     */
                    if (PG_PROTOCOL_MINOR(conn->pversion) < PG_PROTOCOL_GAUSS_BASE) {
                        if (pqGetnchar(conn->sever_signature, HMAC_LENGTH * 2, conn)) {
                            /* We'll come back when there are more data */
                            return PGRES_POLLING_READING;
                        }
                        conn->sever_signature[HMAC_LENGTH * 2] = '\0';
                    }

                    /* Recv iteration of rfc5802 from server. */
                    if (PG_PROTOCOL_MINOR(conn->pversion) > PG_PROTOCOL_GAUSS_BASE) {
                        if (pqGetInt((int*)(&(conn->iteration_count)), 4, conn)) {
                            return PGRES_POLLING_READING;
                        }
                    }
                } else if (MD5_PASSWORD == conn->password_stored_method) {
                    if (pqGetnchar(conn->md5Salt, sizeof(conn->md5Salt), conn)) {
                        /* We'll come back when there are more data */
                        return PGRES_POLLING_READING;
                    }
                }
            }
            if (areq == AUTH_REQ_SM3) {
                if (pqGetInt((int*)(&(conn->password_stored_method)), 4, conn)) {
                    return PGRES_POLLING_READING;
                }

                if (SM3_PASSWORD == conn->password_stored_method) {
                    if (pqGetnchar(conn->salt, SALT_LENGTH * 2, conn)) {
                        /* We'll come back when there are more data */
                        return PGRES_POLLING_READING;
                    }
                    conn->salt[SALT_LENGTH * 2] = '\0';

                    if (pqGetnchar(conn->token, TOKEN_LENGTH * 2, conn)) {
                        /* We'll come back when there are more data */
                        return PGRES_POLLING_READING;
                    }
                    conn->token[TOKEN_LENGTH * 2] = '\0';

                    /*
                     * Recivce the sever_signature before client_signature is not safe.
                     * need to : The rfc5802 authentication protocol need be enhanced.
                     */
                    if (PG_PROTOCOL_MINOR(conn->pversion) < PG_PROTOCOL_GAUSS_BASE) {
                        if (pqGetnchar(conn->sever_signature, HMAC_LENGTH * 2, conn)) {
                            /* We'll come back when there are more data */
                            return PGRES_POLLING_READING;
                        }
                        conn->sever_signature[HMAC_LENGTH * 2] = '\0';
                    }

                    /* Recv iteration of rfc5802 from server. */
                    if (PG_PROTOCOL_MINOR(conn->pversion) > PG_PROTOCOL_GAUSS_BASE) {
                        if (pqGetInt((int*)(&(conn->iteration_count)), 4, conn)) {
                            return PGRES_POLLING_READING;
                        }
                    }
                }
            }

#if defined(ENABLE_GSS) || defined(ENABLE_SSPI)

            /*
             * Continue GSSAPI/SSPI authentication
             */
            if (areq == AUTH_REQ_GSS_CONT) {
                int llen = msgLength - 4;

                /*
                 * We can be called repeatedly for the same buffer. Avoid
                 * re-allocating the buffer in this case - just re-use the
                 * old buffer.
                 */
                if (llen != (int)conn->ginbuf.length) {
                    if (conn->ginbuf.value != NULL)
                        free(conn->ginbuf.value);

                    conn->ginbuf.length = llen;
                    conn->ginbuf.value = malloc(llen);
                    if (conn->ginbuf.value == NULL) {
                        printfPQExpBuffer(
                            &conn->errorMessage, libpq_gettext("out of memory allocating GSSAPI buffer (%d)"), llen);
                        goto error_return;
                    }
                }
#ifdef WIN32
                if (pqGetnchar((char*)conn->ginbuf.value, llen, conn))
#else
                if (pqGetnchar((char*)conn->ginbuf.value, llen, conn))
#endif
                {
                    /* We'll come back when there is more data. */
                    return PGRES_POLLING_READING;
                }
            }
#endif

            /*
             * OK, we successfully read the message; mark data consumed
             */
            conn->inStart = conn->inCursor;

            /* Respond to the request if necessary. */

            /*
             * Note that conn->pghost must be non-NULL if we are going to
             * avoid the Kerberos code doing a hostname look-up.
             */

            if (pg_fe_sendauth(areq, conn) != STATUS_OK) {
                conn->errorMessage.len = strlen(conn->errorMessage.data);
                goto error_return;
            }
            conn->errorMessage.len = strlen(conn->errorMessage.data);

            /*
             * Just make sure that any data sent by pg_fe_sendauth is
             * flushed out.  Although this theoretically could block, it
             * really shouldn't since we don't send large auth responses.
             */
            if (pqFlush(conn))
                goto error_return;

            if (areq == AUTH_REQ_OK) {
                /* We are done with authentication exchange */
                conn->status = CONNECTION_AUTH_OK;

                /*
                 * Set asyncStatus so that PQgetResult will think that
                 * what comes back next is the result of a query.  See
                 * below.
                 */
                conn->asyncStatus = PGASYNC_BUSY;
            }

            /* Look to see if we have more data yet. */
            goto keep_going;
        }

        case CONNECTION_AUTH_OK: {
            /*
             * Now we expect to hear from the backend. A ReadyForQuery
             * message indicates that startup is successful, but we might
             * also get an Error message indicating failure. (Notice
             * messages indicating nonfatal warnings are also allowed by
             * the protocol, as are ParameterStatus and BackendKeyData
             * messages.) Easiest way to handle this is to let
             * PQgetResult() read the messages. We just have to fake it
             * out about the state of the connection, by setting
             * asyncStatus = PGASYNC_BUSY (done above).
             */

            if (PQisBusy(conn))
                return PGRES_POLLING_READING;

            res = PQgetResult(conn);

            /*
             * NULL return indicating we have gone to IDLE state is
             * expected
             */
            if (res != NULL) {
                if (res->resultStatus != PGRES_FATAL_ERROR)
                    appendPQExpBuffer(
                        &conn->errorMessage, libpq_gettext("unexpected message from server during startup\n"));
                else if (conn->send_appname && (conn->appname != NULL || conn->fbappname != NULL)) {
                    /*
                     * If we tried to send application_name, check to see
                     * if the error is about that --- pre-9.0 servers will
                     * reject it at this stage of the process.	If so,
                     * close the connection and retry without sending
                     * application_name.  We could possibly get a false
                     * SQLSTATE match here and retry uselessly, but there
                     * seems no great harm in that; we'll just get the
                     * same error again if it's unrelated.
                     */
                    const char* sqlstate = PQresultErrorField(res, PG_DIAG_SQLSTATE);

                    if (sqlstate != NULL && strcmp(sqlstate, ERRCODE_APPNAME_UNKNOWN) == 0) {
                        PQclear(res);
                        conn->send_appname = false;
                        /* Must drop the old connection */
                        pqsecure_close(conn);
                        closesocket(conn->sock);
                        conn->sock = -1;
                        conn->status = CONNECTION_NEEDED;
                        /* Discard any unread/unsent data */
                        conn->inStart = conn->inCursor = conn->inEnd = 0;
                        conn->outCount = 0;
                        goto keep_going;
                    }
                }

                /*
                 * if the resultStatus is FATAL, then conn->errorMessage
                 * already has a copy of the error; needn't copy it back.
                 * But add a newline if it's not there already, since
                 * postmaster error messages may not have one.
                 */
                if (conn->errorMessage.len <= 0 || conn->errorMessage.data[conn->errorMessage.len - 1] != '\n')
                    appendPQExpBufferChar(&conn->errorMessage, '\n');
                PQclear(res);
                goto error_return;
            }

            /* We can release the address list now. */
            release_conn_addrinfo(conn);

            /* Fire up post-connection housekeeping if needed */
            if (PG_PROTOCOL_MAJOR(conn->pversion) < 3) {
                conn->status = CONNECTION_SETENV;
                conn->setenv_state = SETENV_STATE_CLIENT_ENCODING_SEND;
                conn->next_eo = EnvironmentOptions;
                return PGRES_POLLING_WRITING;
            }

            /* Otherwise, we are open for business! */
            conn->status = CONNECTION_CHECK_TARGET;
            goto keep_going;
        }

        case CONNECTION_SETENV:

            /*
             * Do post-connection housekeeping (only needed in protocol 2.0).
             *
             * We pretend that the connection is OK for the duration of these
             * queries.
             */
            conn->status = CONNECTION_OK;

            switch (pqSetenvPoll(conn)) {
                case PGRES_POLLING_OK: /* Success */
                    break;

                case PGRES_POLLING_READING: /* Still going */
                    conn->status = CONNECTION_SETENV;
                    return PGRES_POLLING_READING;

                case PGRES_POLLING_WRITING: /* Still going */
                    conn->status = CONNECTION_SETENV;
                    return PGRES_POLLING_WRITING;

                default:
                    goto error_return;
            }

            /* We are open for business! */
            conn->status = CONNECTION_CHECK_TARGET;
            goto keep_going;

        case CONNECTION_CHECK_TARGET:
            return connection_check_target(conn);
        case CONNECTION_CONSUME: {
            PostgresPollingStatusType ret = connection_consume(conn);
            if (ret == PGRES_POLLING_ACTIVE) {
                goto keep_going;
            } else {
                return ret;
            }
        }
        case CONNECTION_CHECK_WRITABLE: {
            PostgresPollingStatusType ret = connection_check_writable(conn);
            if (ret == PGRES_POLLING_ACTIVE) {
                goto keep_going;
            } else {
                return ret;
            }
        }
        case CONNECTION_CHECK_STANDBY: {
            PostgresPollingStatusType ret = connection_check_standby(conn);
            if (ret == PGRES_POLLING_ACTIVE) {
                goto keep_going;
            } else {
                return ret;
            }
        }

        default:
            appendPQExpBuffer(&conn->errorMessage,
                libpq_gettext("invalid connection state %d, "
                              "probably indicative of memory corruption\n"),
                conn->status);
            goto error_return;
    }

    /* Unreachable */

error_return:
#ifdef SUPPORT_PGPASSFILE
    dot_pg_pass_warning(conn);
#endif
    /*
     * We used to close the socket at this point, but that makes it awkward
     * for those above us if they wish to remove this socket from their own
     * records (an fd_set for example).  We'll just have this socket closed
     * when PQfinish is called (which is compulsory even after an error, since
     * the connection structure must be freed).
     */
    conn->status = CONNECTION_BAD;
    return PGRES_POLLING_FAILED;
}

/*
 * internal_ping
 *		Determine if a server is running and if we can connect to it.
 *
 * The argument is a connection that's been started, but not completed.
 */
static PGPing internal_ping(PGconn* conn)
{
    /* Say "no attempt" if we never got to PQconnectPoll */
    if (conn == NULL || !conn->options_valid)
        return PQPING_NO_ATTEMPT;

    /* Attempt to complete the connection */
    if (conn->status != CONNECTION_BAD)
        (void)connectDBComplete(conn);

    /* Definitely OK if we succeeded */
    if (conn->status != CONNECTION_BAD)
        return PQPING_OK;

    /*
     * Here begins the interesting part of "ping": determine the cause of the
     * failure in sufficient detail to decide what to return.  We do not want
     * to report that the server is not up just because we didn't have a valid
     * password, for example.  In fact, any sort of authentication request
     * implies the server is up.  (We need this check since the libpq side of
     * things might have pulled the plug on the connection before getting an
     * error as such from the postmaster.)
     */
    if (conn->auth_req_received)
        return PQPING_OK;

    /*
     * If we failed to get any ERROR response from the postmaster, report
     * PQPING_NO_RESPONSE.	This result could be somewhat misleading for a
     * pre-7.4 server, since it won't send back a SQLSTATE, but those are long
     * out of support.	Another corner case where the server could return a
     * failure without a SQLSTATE is fork failure, but NO_RESPONSE isn't
     * totally unreasonable for that anyway.  We expect that every other
     * failure case in a modern server will produce a report with a SQLSTATE.
     *
     * NOTE: whenever we get around to making libpq generate SQLSTATEs for
     * client-side errors, we should either not store those into
     * last_sqlstate, or add an extra flag so we can tell client-side errors
     * apart from server-side ones.
     */
    if (strlen(conn->last_sqlstate) != 5)
        return PQPING_NO_RESPONSE;

    /*
     * Report PQPING_REJECT if server says it's not accepting connections. (We
     * distinguish this case mainly for the convenience of pg_ctl.)
     */
    if (strcmp(conn->last_sqlstate, ERRCODE_CANNOT_CONNECT_NOW) == 0)
        return PQPING_REJECT;

    /*
     * Any other SQLSTATE can be taken to indicate that the server is up.
     * Presumably it didn't like our username, password, or database name; or
     * perhaps it had some transient failure, but that should not be taken as
     * meaning "it's down".
     */
    return PQPING_OK;
}

/*
 * makeEmptyPGconn
 *	 - create a PGconn data structure with (as yet) no interesting data
 */
PGconn* makeEmptyPGconn(void)
{
    PGconn* conn = NULL;

#ifdef WIN32

    /*
     * Make sure socket support is up and running.
     */
    WSADATA wsaData;

    if (WSAStartup(MAKEWORD(1, 1), &wsaData))
        return NULL;
    WSASetLastError(0);
#endif

    conn = (PGconn*)malloc(sizeof(PGconn));
    if (conn == NULL) {
#ifdef WIN32
        WSACleanup();
#endif
        return conn;
    }

    /* Zero all pointers and booleans */
    check_memset_s(memset_s(conn, sizeof(PGconn), 0, sizeof(PGconn)));

    /* install default notice hooks */
    conn->noticeHooks.noticeRec = defaultNoticeReceiver;
    conn->noticeHooks.noticeProc = defaultNoticeProcessor;

    conn->status = CONNECTION_BAD;
    conn->asyncStatus = PGASYNC_IDLE;
    conn->xactStatus = PQTRANS_IDLE;
    conn->options_valid = false;
    conn->nonblocking = false;
    conn->setenv_state = SETENV_STATE_IDLE;
    conn->client_encoding = PG_SQL_ASCII;
    conn->std_strings = false; /* unless server says differently */
    conn->whichhost = -1;
    conn->default_transaction_read_only = PG_BOOL_UNKNOWN;
    conn->in_hot_standby = PG_BOOL_UNKNOWN;
    conn->verbosity = PQERRORS_DEFAULT;
    conn->sock = -1;
    conn->auth_req_received = false;
    conn->password_needed = false;
    conn->dot_pgpass_used = false;
#ifdef USE_SSL
    conn->allow_ssl_try = true;
    conn->wait_ssl_try = false;
#endif
    conn->is_logic_conn = false;
#ifdef HAVE_CE
    conn->client_logic = new PGClientLogic(conn, NULL, NULL);
#endif // HAVE_CE

    /*
     * We try to send at least 8K at a time, which is the usual size of pipe
     * buffers on Unix systems.  That way, when we are sending a large amount
     * of data, we avoid incurring extra kernel context swaps for partial
     * bufferloads.  The output buffer is initially made 16K in size, and we
     * try to dump it after accumulating 8K.
     *
     * With the same goal of minimizing context swaps, the input buffer will
     * be enlarged anytime it has less than 8K free, so we initially allocate
     * twice that.
     */
    conn->inBufSize = 16 * 1024;
    conn->inBuffer = (char*)malloc(conn->inBufSize);
    conn->outBufSize = 16 * 1024;
    conn->outBuffer = (char*)malloc(conn->outBufSize);
    conn->rowBufLen = 32;
    conn->rowBuf = (PGdataValue*)malloc(conn->rowBufLen * sizeof(PGdataValue));
    initPQExpBuffer(&conn->errorMessage);
    initPQExpBuffer(&conn->workBuffer);

    if (conn->inBuffer == NULL || conn->outBuffer == NULL || conn->rowBuf == NULL ||
        PQExpBufferBroken(&conn->errorMessage) || PQExpBufferBroken(&conn->workBuffer)) {
        /* out of memory already :-( */
        freePGconn(conn);
        conn = NULL;
    }

    return conn;
}

/*
 * freePGconn
 *	 - free an idle (closed) PGconn data structure
 *
 * NOTE: this should not overlap any functionality with closePGconn().
 * Clearing/resetting of transient state belongs there; what we do here is
 * release data that is to be held for the life of the PGconn structure.
 * If a value ought to be cleared/freed during PQreset(), do it there not here.
 */
void freePGconn(PGconn* conn)
{
    int i;

    /* let any event procs clean up their state data */
    for (i = 0; i < conn->nEvents; i++) {
        PGEventConnDestroy evt;

        evt.conn = conn;
        (void)conn->events[i].proc(PGEVT_CONNDESTROY, &evt, conn->events[i].passThrough);
        libpq_free(conn->events[i].name);
    }

    /* clean up pg_conn_host structures */
    if (conn->connhost != NULL) {
        for (i = 0; i < conn->nconnhost; ++i) {
            if (conn->connhost[i].host != NULL)
                libpq_free(conn->connhost[i].host);
            if (conn->connhost[i].hostaddr != NULL)
                libpq_free(conn->connhost[i].hostaddr);
            if (conn->connhost[i].port != NULL)
                libpq_free(conn->connhost[i].port);
            if (conn->connhost[i].password != NULL) {
                erase_string(conn->connhost[i].password);
                libpq_free(conn->connhost[i].password);
            }
        }
        libpq_free(conn->connhost);
    }

    libpq_free(conn->client_encoding_initial);
    libpq_free(conn->events);
    libpq_free(conn->pghost);
    libpq_free(conn->remote_nodename);
    libpq_free(conn->pghostaddr);
    libpq_free(conn->pgport);
    libpq_free(conn->pgtty);
    libpq_free(conn->connect_timeout);
    libpq_free(conn->pgoptions);
    libpq_free(conn->appname);
    libpq_free(conn->fbappname);
    libpq_free(conn->dbName);
    libpq_free(conn->replication);
    libpq_free(conn->backend_version);
    libpq_free(conn->pguser);
    if (conn->pgpass != NULL) {
        erase_string(conn->pgpass);
        libpq_free(conn->pgpass);
    }
    libpq_free(conn->keepalives);
    libpq_free(conn->keepalives_idle);
    libpq_free(conn->keepalives_interval);
    libpq_free(conn->keepalives_count);
    libpq_free(conn->rw_timeout);
    libpq_free(conn->sslmode);
    libpq_free(conn->sslcert);
    libpq_free(conn->sslkey);
    libpq_free(conn->sslrootcert);
    libpq_free(conn->sslcrl);
    libpq_free(conn->sslcompression);
    libpq_free(conn->requirepeer);
#if defined(KRB5) || defined(ENABLE_GSS) || defined(ENABLE_SSPI)
    libpq_free(conn->krbsrvname);
#endif
#if defined(ENABLE_GSS) && defined(ENABLE_SSPI)
    libpq_free(conn->gsslib);
#endif
    /* Note that conn->Pfdebug is not ours to close or free */
    libpq_free(conn->last_query);
    libpq_free(conn->inBuffer);
    libpq_free(conn->outBuffer);
    libpq_free(conn->rowBuf);
    libpq_free(conn->pglocalhost);
    libpq_free(conn->pglocalport);
    libpq_free(conn->connection_info);
    if (conn->target_session_attrs)
        libpq_free(conn->target_session_attrs);
    termPQExpBuffer(&conn->errorMessage);
    termPQExpBuffer(&conn->workBuffer);
#ifdef HAVE_CE
    if (conn && conn->client_logic) {
        delete conn->client_logic;
        conn->client_logic = NULL;
    }
#endif

    libpq_free(conn);

#ifdef WIN32
    WSACleanup();
#endif
}

/*
 * closePGconn
 *	 - properly close a connection to the backend
 *
 * This should reset or release all transient state, but NOT the connection
 * parameters.	On exit, the PGconn should be in condition to start a fresh
 * connection with the same parameters (see PQreset()).
 */
void closePGconn(PGconn* conn)
{
    PGnotify* notify = NULL;
    pgParameterStatus* pstatus = NULL;

    /*
     * Note that the protocol doesn't allow us to send Terminate messages
     * during the startup phase.
     * Note: for logic conn we have done this step in LibcommCloseConn
     */
    if ((conn->sock >= 0 && conn->status == CONNECTION_OK) && (!conn->is_logic_conn)) {
        /*
         * Try to send "close connection" message to backend. Ignore any
         * error.
         */
        (void)pqPutMsgStart('X', false, conn);
        (void)pqPutMsgEnd(conn);
        (void)pqFlush(conn);
    }

    /*
     * Must reset the blocking status so a possible reconnect will work.
     *
     * Don't call PQsetnonblocking() because it will fail if it's unable to
     * flush the connection.
     */
    conn->nonblocking = FALSE;

    /*
     * Close the connection, reset all transient state, flush I/O buffers.
     */
    if ((conn->sock >= 0) && (!conn->is_logic_conn)) {
        pqsecure_close(conn);
        closesocket(conn->sock);
    }
    conn->sock = -1;
    conn->status = CONNECTION_BAD; /* Well, not really _bad_ - just
                                    * absent */
    conn->asyncStatus = PGASYNC_IDLE;
    pqClearAsyncResult(conn); /* deallocate result */
    resetPQExpBuffer(&conn->errorMessage);
    release_conn_addrinfo(conn);
    notify = conn->notifyHead;
    while (notify != NULL) {
        PGnotify* prev = notify;

        notify = notify->next;
        libpq_free(prev);
    }
    conn->notifyHead = conn->notifyTail = NULL;
    pstatus = conn->pstatus;
    while (pstatus != NULL) {
        pgParameterStatus* prev = pstatus;

        pstatus = pstatus->next;
        libpq_free(prev);
    }
    conn->pstatus = NULL;
    libpq_free(conn->lobjfuncs);
    conn->inStart = conn->inCursor = conn->inEnd = 0;
    conn->outCount = 0;
#ifdef ENABLE_GSS
    {
        OM_uint32 min_s;

        if (conn->gctx != NULL)
            gss_delete_sec_context(&min_s, &conn->gctx, GSS_C_NO_BUFFER);
        if (conn->gtarg_nam != NULL)
            gss_release_name(&min_s, &conn->gtarg_nam);
        if (conn->ginbuf.length)
            gss_release_buffer(&min_s, &conn->ginbuf);
        if (conn->goutbuf.length)
            gss_release_buffer(&min_s, &conn->goutbuf);
    }
#endif
#ifdef ENABLE_SSPI
    if (conn->ginbuf.length)
        libpq_free(conn->ginbuf.value);
    conn->ginbuf.length = 0;
    conn->ginbuf.value = NULL;
    libpq_free(conn->sspitarget);
    conn->sspitarget = NULL;
    if (conn->sspicred != NULL) {
        FreeCredentialsHandle(conn->sspicred);
        libpq_free(conn->sspicred);
    }
    if (conn->sspictx != NULL) {
        DeleteSecurityContext(conn->sspictx);
        libpq_free(conn->sspictx);
    }
#endif
}

/*
 * PQfinish: properly close a connection to the backend. Also frees
 * the PGconn data structure so it shouldn't be re-used after this.
 */
void PQfinish(PGconn* conn)
{
    if (conn != NULL) {
        closePGconn(conn);
        freePGconn(conn);
    }
}

/*
 * PQreset: resets the connection to the backend by closing the
 * existing connection and creating a new one.
 */
void PQreset(PGconn* conn)
{
    if (conn != NULL) {
        closePGconn(conn);

        if (connectDBStart(conn) && connectDBComplete(conn)) {
            /*
             * Notify event procs of successful reset.	We treat an event proc
             * failure as disabling the connection ... good idea?
             */
            int i;

            for (i = 0; i < conn->nEvents; i++) {
                PGEventConnReset evt;

                evt.conn = conn;
                if (!(conn->events[i].proc(PGEVT_CONNRESET, &evt, conn->events[i].passThrough))) {
                    conn->status = CONNECTION_BAD;
                    printfPQExpBuffer(&conn->errorMessage,
                        libpq_gettext("PGEventProc \"%s\" failed during PGEVT_CONNRESET event, remote datanode %s, err: %s\n"),
                        conn->events[i].name, conn->remote_nodename, strerror(errno));
                    break;
                }
            }

            /* reset connection_info */
            if ((conn != NULL) && conn->status != CONNECTION_BAD &&
                (conn->replication == NULL || conn->replication[0] == '\0')) {
                (void)connectSetConninfo(conn);
            }
        }
    }
}

/*
 * PQresetStart:
 * resets the connection to the backend
 * closes the existing connection and makes a new one
 * Returns 1 on success, 0 on failure.
 */
int PQresetStart(PGconn* conn)
{
    if (conn != NULL) {
        closePGconn(conn);

        return connectDBStart(conn);
    }

    return 0;
}

/*
 * PQresetPoll:
 * resets the connection to the backend
 * closes the existing connection and makes a new one
 */
PostgresPollingStatusType PQresetPoll(PGconn* conn)
{
    if (conn != NULL) {
        PostgresPollingStatusType status = PQconnectPoll(conn);

        if (status == PGRES_POLLING_OK) {
            /*
             * Notify event procs of successful reset.	We treat an event proc
             * failure as disabling the connection ... good idea?
             */
            int i;

            for (i = 0; i < conn->nEvents; i++) {
                PGEventConnReset evt;

                evt.conn = conn;
                if (!(conn->events[i].proc(PGEVT_CONNRESET, &evt, conn->events[i].passThrough))) {
                    conn->status = CONNECTION_BAD;
                    printfPQExpBuffer(&conn->errorMessage,
                        libpq_gettext("PGEventProc \"%s\" failed during PGEVT_CONNRESET event, errno: %s\n"),
                        conn->events[i].name, strerror(errno));
                    return PGRES_POLLING_FAILED;
                }
            }
        }

        return status;
    }

    return PGRES_POLLING_FAILED;
}

/*
 * PQcancelGet: get a PGcancel structure corresponding to a connection.
 *
 * A copy is needed to be able to cancel a running query from a different
 * thread. If the same structure is used all structure members would have
 * to be individually locked (if the entire structure was locked, it would
 * be impossible to cancel a synchronous query because the structure would
 * have to stay locked for the duration of the query).
 */
PGcancel* PQgetCancel(PGconn* conn)
{
    PGcancel* cancel = NULL;

    if (conn == NULL)
        return NULL;

    if (conn->sock < 0)
        return NULL;

    cancel = (PGcancel*)malloc(sizeof(PGcancel));
    if (cancel == NULL)
        return NULL;

    check_memcpy_s(memcpy_s(&cancel->raddr, sizeof(SockAddr), &conn->raddr, sizeof(SockAddr)));
    cancel->be_pid = conn->be_pid;
    cancel->be_key = conn->be_key;

    return cancel;
}

/* PQfreeCancel: free a cancel structure */
void PQfreeCancel(PGcancel* cancel)
{
    libpq_free(cancel);
}

/*
 * attempt to request stop of the
 * current query, stop the query asap.
 */
static int internal_stop(SockAddr* raddr, int be_pid, char* errbuf, int errbufsize, int timeo, uint64 query_id)
{
    int save_errno = SOCK_ERRNO;
    int tmpsock = -1;
    char sebuf[256];
    int maxlen;
    errno_t rc = 0;

    struct timeval timeout = {timeo, 0};
    socklen_t len = sizeof(timeout);

    struct {
        uint32 packetlen;
        StopRequestPacket cp;
    } crp;

    /*
     * We need to open a temporary connection to the postmaster. Do this with
     * only kernel calls.
     */
    if ((tmpsock = socket(raddr->addr.ss_family, SOCK_STREAM, 0)) < 0) {
        check_sprintf_s(
            snprintf_truncated_s(errbuf,errbufsize,
            "internal_stop() -- socket() failed: errno: %s",
            strerror(errno)));
        goto stop_errReturn;
    }

#ifdef F_SETFD
    if (fcntl(tmpsock, F_SETFD, FD_CLOEXEC) == -1) {
        check_sprintf_s(
            snprintf_truncated_s(errbuf,errbufsize,
            "internal_stop() -- setsockopt(FD_CLOEXEC) failed: errno: %s",
            strerror(errno)));
        goto stop_errReturn;
    }
#endif /* F_SETFD */

    if (timeo) {
        /* timeout for connect() & send() */
        if (setsockopt(tmpsock, SOL_SOCKET, SO_SNDTIMEO, (const char*)&timeout, len) < 0) {
            check_sprintf_s(
                snprintf_truncated_s(errbuf,errbufsize,
                "internal_stop() -- setsockopt(SO_SNDTIMEO) failed: errno: %s",
                strerror(errno)));
            goto stop_errReturn;
        }

        /* timeout for recv() */
        if (setsockopt(tmpsock, SOL_SOCKET, SO_RCVTIMEO, (const char*)&timeout, len) < 0) {
            check_sprintf_s(
                snprintf_truncated_s(errbuf,errbufsize,
                "internal_stop() -- setsockopt(SO_RCVTIMEO) failed: errno: %s",
                strerror(errno)));
            goto stop_errReturn;
        }
    }

retry3:
    if (connect(tmpsock, (struct sockaddr*)&raddr->addr, raddr->salen) < 0) {
        if (SOCK_ERRNO == EINTR)
            /* Interrupted system call - we'll just try again */
            goto retry3;

        if (SOCK_ERRNO == EINPROGRESS || SOCK_ERRNO == ETIMEDOUT) {
            /* connection timeout */
            check_sprintf_s(
            snprintf_truncated_s(errbuf,errbufsize,
            "internal_stop() -- connect() timeout, failed: errno: %s",
            strerror(errno)));
        } else {
            check_sprintf_s(
                snprintf_truncated_s(errbuf,errbufsize,
                "internal_stop() -- connect() failed: errno: %s",
                strerror(errno)));
        }
        goto stop_errReturn;
    }

    /*
     * We needn't set nonblocking I/O or NODELAY options here.
     */

    /* Create and send the cancel request packet. */

    crp.packetlen = htonl((uint32)sizeof(crp));
    crp.cp.stopRequestCode = (MsgType)htonl(STOP_REQUEST_CODE);
    crp.cp.backendPID = htonl(be_pid);
    crp.cp.query_id_first = htonl((uint32)(query_id >> 32));
    crp.cp.query_id_end = htonl((uint32)(query_id & 0xFFFFFFFF));

retry4:
    if (send(tmpsock, (char*)&crp, sizeof(crp), 0) != (int)sizeof(crp)) {
        if (SOCK_ERRNO == EINTR)
            /* Interrupted system call - we'll just try again */
            goto retry4;
#ifdef EAGAIN
        if (SOCK_ERRNO == EAGAIN || SOCK_ERRNO == EWOULDBLOCK) {
            /* send() timeout */
            check_sprintf_s(
            snprintf_truncated_s(errbuf,errbufsize,
            "internal_stop() -- send() timeout, failed: errno: %s",
            strerror(errno)));
        } else
#endif
        {
            check_sprintf_s(
                snprintf_truncated_s(errbuf,errbufsize,
                "internal_stop() -- send() failed: errno: %s",
                strerror(errno)));
        }

        goto stop_errReturn;
    }

    /*
     * Wait for the postmaster to close the connection, which indicates that
     * it's processed the request.  Without this delay, we might issue another
     * command only to find that our cancel zaps that command instead of the
     * one we thought we were canceling.  Note we don't actually expect this
     * read to obtain any data, we are just waiting for EOF to be signaled.
     */
retry5:
    if (recv(tmpsock, (char*)&crp, 1, 0) < 0) {
        if (SOCK_ERRNO == EINTR)
            /* Interrupted system call - we'll just try again */
            goto retry5;
        /* we ignore other error conditions */
    }

    /* All done */
    closesocket(tmpsock);
    SOCK_ERRNO_SET(save_errno);
    return TRUE;

stop_errReturn:

    /*
     * Make sure we don't overflow the error buffer. Leave space for the \n at
     * the end, and for the terminating zero.
     */
    maxlen = errbufsize - strlen(errbuf) - 2;
    if (maxlen >= 0) {
        rc = strncat_s(errbuf, errbufsize, SOCK_STRERROR(SOCK_ERRNO, sebuf, sizeof(sebuf)), maxlen);
        securec_check_c(rc, "\0", "\0");
        rc = strcat_s(errbuf, errbufsize, "\n");
        securec_check_c(rc, "\0", "\0");
    }
    if (tmpsock >= 0)
        closesocket(tmpsock);
    SOCK_ERRNO_SET(save_errno);
    return FALSE;
}

/*
 * PQStop: request query stop asap
 *
 * Returns TRUE if able to send the stop request, FALSE if not.
 *
 * On failure, an error message is stored in *errbuf, which must be of size
 * errbufsize (recommended size is 256 bytes).	*errbuf is not changed on
 * success return.
 */

int PQstop_timeout(PGcancel* cancel, char* errbuf, int errbufsize, int timeout, uint64 query_id)
{
    if (cancel == NULL) {
        check_strncpy_s(strncpy_s(errbuf, errbufsize, "PQStop() -- no stop object supplied", errbufsize - 1));
        return FALSE;
    }

    return internal_stop(&cancel->raddr, cancel->be_pid, errbuf, errbufsize, timeout, query_id);
}

int PQStop(PGcancel* cancel, char* errbuf, int errbufsize, uint64 query_id)
{
    return PQstop_timeout(cancel, errbuf, errbufsize, 0, query_id);
}

/*
 * PQcancel and PQrequestCancel: attempt to request cancellation of the
 * current operation.
 *
 * The return value is TRUE if the cancel request was successfully
 * dispatched, FALSE if not (in which case an error message is available).
 * Note: successful dispatch is no guarantee that there will be any effect at
 * the backend.  The application must read the operation result as usual.
 *
 * CAUTION: we want this routine to be safely callable from a signal handler
 * (for example, an application might want to call it in a SIGINT handler).
 * This means we cannot use any C library routine that might be non-reentrant.
 * malloc/free are often non-reentrant, and anything that might call them is
 * just as dangerous.  We avoid sprintf here for that reason.  Building up
 * error messages with strcpy/strcat is tedious but should be quite safe.
 * We also save/restore errno in case the signal handler support doesn't.
 *
 * internal_cancel() is an internal helper function to make code-sharing
 * between the two versions of the cancel function possible.
 */
static int internal_cancel(SockAddr* raddr, int be_pid, int be_key, char* errbuf, int errbufsize, int timeo = 0)
{
    int save_errno = SOCK_ERRNO;
    int tmpsock = -1;
    char sebuf[256];
    int maxlen;
    int on = 0;
    errno_t rc = 0;

    struct timeval timeout = {timeo, 0};
    socklen_t len = sizeof(timeout);

    struct {
        uint32 packetlen;
        CancelRequestPacket cp;
    } crp;

    /*
     * We need to open a temporary connection to the postmaster. Do this with
     * only kernel calls.
     */
    if ((tmpsock = socket(raddr->addr.ss_family, SOCK_STREAM, 0)) < 0) {
        check_sprintf_s(
            snprintf_truncated_s(errbuf,errbufsize,
            "internal_cancel() -- socket() failed: errno: %s",
            strerror(errno)));
        goto cancel_errReturn;
    }

#ifdef F_SETFD
    if (fcntl(tmpsock, F_SETFD, FD_CLOEXEC) == -1) {
        check_sprintf_s(
            snprintf_truncated_s(errbuf,errbufsize,
            "internal_cancel() -- setsockopt(FD_CLOEXEC) failed: errno: %s",
            strerror(errno)));
        goto cancel_errReturn;
    }
#endif /* F_SETFD */

    on = 1;
    if (setsockopt(tmpsock, SOL_SOCKET, SO_KEEPALIVE, (char*)&on, sizeof(on)) < 0) {
        check_sprintf_s(
            snprintf_truncated_s(errbuf,errbufsize,
            "internal_cancel() -- setsockopt(SO_KEEPALIVE) failed: errno: %s",
            strerror(errno)));
        goto cancel_errReturn;
    }

    if (timeo) {
        /* timeout for connect() & send() */
        if (setsockopt(tmpsock, SOL_SOCKET, SO_SNDTIMEO, (const char*)&timeout, len) < 0) {
            check_sprintf_s(
                snprintf_truncated_s(errbuf,errbufsize,
                "internal_cancel() -- setsockopt(SO_SNDTIMEO) failed: errno: %s",
                strerror(errno)));
            goto cancel_errReturn;
        }

        /* timeout for recv() */
        if (setsockopt(tmpsock, SOL_SOCKET, SO_RCVTIMEO, (const char*)&timeout, len) < 0) {
            check_sprintf_s(
                snprintf_truncated_s(errbuf,errbufsize,
                "internal_cancel() -- setsockopt(SO_RCVTIMEO) failed: errno: %s",
                strerror(errno)));
            goto cancel_errReturn;
        }
    }

retry3:
    if (connect(tmpsock, (struct sockaddr*)&raddr->addr, raddr->salen) < 0) {
        if (SOCK_ERRNO == EINTR)
            /* Interrupted system call - we'll just try again */
            goto retry3;

        if (SOCK_ERRNO == EINPROGRESS || SOCK_ERRNO == ETIMEDOUT) {
            /* connection timeout here */
            check_sprintf_s(
            snprintf_truncated_s(errbuf,errbufsize,
            "internal_cancel() -- connect() timeout, failed: errno: %s",
            strerror(errno)));
        } else {
            check_sprintf_s(
                snprintf_truncated_s(errbuf,errbufsize,
                "internal_cancel() -- connect() failed: errno: %s",
                strerror(errno)));
        }
        goto cancel_errReturn;
    }

    /*
     * We needn't set nonblocking I/O or NODELAY options here.
     */

    /* Create and send the cancel request packet. */

    crp.packetlen = htonl((uint32)sizeof(crp));
    crp.cp.cancelRequestCode = (MsgType)htonl(CANCEL_REQUEST_CODE);
    crp.cp.backendPID = htonl(be_pid);
    crp.cp.cancelAuthCode = htonl(be_key);

retry4:
    if (send(tmpsock, (char*)&crp, sizeof(crp), 0) != (int)sizeof(crp)) {
        if (SOCK_ERRNO == EINTR)
            /* Interrupted system call - we'll just try again */
            goto retry4;
#ifdef EAGAIN
        if (SOCK_ERRNO == EAGAIN || SOCK_ERRNO == EWOULDBLOCK) {
            /* send() timeout here */
            check_sprintf_s(
            snprintf_truncated_s(errbuf,errbufsize,
            "internal_cancel() -- send() timeout, failed: errno: %s",
            strerror(errno)));
        } else
#endif
        {
            check_sprintf_s(
                snprintf_truncated_s(errbuf,errbufsize,
                "internal_cancel() -- send() failed: errno: %s",
                strerror(errno)));
        }

        goto cancel_errReturn;
    }

    /*
     * Wait for the postmaster to close the connection, which indicates that
     * it's processed the request.  Without this delay, we might issue another
     * command only to find that our cancel zaps that command instead of the
     * one we thought we were canceling.  Note we don't actually expect this
     * read to obtain any data, we are just waiting for EOF to be signaled.
     */
retry5:
    if (recv(tmpsock, (char*)&crp, 1, 0) < 0) {
        if (SOCK_ERRNO == EINTR)
            /* Interrupted system call - we'll just try again */
            goto retry5;
        /* we ignore other error conditions */
    }

    /* All done */
    closesocket(tmpsock);
    SOCK_ERRNO_SET(save_errno);
    return TRUE;

cancel_errReturn:
    /*
     * Make sure we don't overflow the error buffer. Leave space for the \n at
     * the end, and for the terminating zero.
     */
    maxlen = errbufsize - strlen(errbuf) - 2;
    if (maxlen >= 0) {
        rc = strncat_s(errbuf, errbufsize, SOCK_STRERROR(SOCK_ERRNO, sebuf, sizeof(sebuf)), maxlen);
        securec_check_c(rc, "\0", "\0");
        rc = strcat_s(errbuf, errbufsize, "\n");
        securec_check_c(rc, "\0", "\0");
    }
    if (tmpsock >= 0)
        closesocket(tmpsock);
    SOCK_ERRNO_SET(save_errno);
    return FALSE;
}

/*
 * PQcancel: request query cancel
 *
 * Returns TRUE if able to send the cancel request, FALSE if not.
 *
 * On failure, an error message is stored in *errbuf, which must be of size
 * errbufsize (recommended size is 256 bytes).	*errbuf is not changed on
 * success return.
 */
int PQcancel(PGcancel* cancel, char* errbuf, int errbufsize)
{
    return PQcancel_timeout(cancel, errbuf, errbufsize, 0);
}

int PQcancel_timeout(PGcancel* cancel, char* errbuf, int errbufsize, int timeout)
{
    if (cancel == NULL) {
        check_strncpy_s(strncpy_s(errbuf, errbufsize, "PQcancel() -- no cancel object supplied", errbufsize - 1));
        return FALSE;
    }

    return internal_cancel(&cancel->raddr, cancel->be_pid, cancel->be_key, errbuf, errbufsize, timeout);
}

/*
 * PQrequestCancel: old, not thread-safe function for requesting query cancel
 *
 * Returns TRUE if able to send the cancel request, FALSE if not.
 *
 * On failure, the error message is saved in conn->errorMessage; this means
 * that this can't be used when there might be other active operations on
 * the connection object.
 *
 * NOTE: error messages will be cut off at the current size of the
 * error message buffer, since we dare not try to expand conn->errorMessage!
 */
int PQrequestCancel(PGconn* conn)
{
    int r;

    /* Check we have an open connection */
    if (conn == NULL)
        return FALSE;

    if (conn->sock < 0) {
        check_strncpy_s(strncpy_s(conn->errorMessage.data,
            conn->errorMessage.maxlen,
            "PQrequestCancel() -- connection is not open\n",
            conn->errorMessage.maxlen - 1));
        conn->errorMessage.len = strlen(conn->errorMessage.data);

        return FALSE;
    }

    r = internal_cancel(&conn->raddr, conn->be_pid, conn->be_key, conn->errorMessage.data, conn->errorMessage.maxlen);

    if (!r)
        conn->errorMessage.len = strlen(conn->errorMessage.data);

    return r;
}

/*
 * pqPacketSend() -- convenience routine to send a message to server.
 *
 * pack_type: the single-byte message type code.  (Pass zero for startup
 * packets, which have no message type code.)
 *
 * buf, buf_len: contents of message.  The given length includes only what
 * is in buf; the message type and message length fields are added here.
 *
 * RETURNS: STATUS_ERROR if the write fails, STATUS_OK otherwise.
 * SIDE_EFFECTS: may block.
 *
 * Note: all messages sent with this routine have a length word, whether
 * it's protocol 2.0 or 3.0.
 */
int pqPacketSend(PGconn* conn, char pack_type, const void* buf, size_t buf_len)
{
    /* Start the message. */
    if (pqPutMsgStart(pack_type, true, conn))
        return STATUS_ERROR;

    /* Send the message body. */
    if (pqPutnchar((const char*)buf, buf_len, conn))
        return STATUS_ERROR;

    /* Finish the message. */
    if (pqPutMsgEnd(conn))
        return STATUS_ERROR;

    /* Flush to ensure backend gets it. */
    if (pqFlush(conn))
        return STATUS_ERROR;

    return STATUS_OK;
}

#ifdef USE_LDAP

#define LDAP_URL "ldap://"
#define LDAP_DEF_PORT 389
#define PGLDAP_TIMEOUT 2

#define ld_is_sp_tab(x) ((x) == ' ' || (x) == '\t')
#define ld_is_nl_cr(x) ((x) == '\r' || (x) == '\n')

/*
 *		ldapServiceLookup
 *
 * Search the LDAP URL passed as first argument, treat the result as a
 * string of connection options that are parsed and added to the array of
 * options passed as second argument.
 *
 * LDAP URLs must conform to RFC 1959 without escape sequences.
 *	ldap://host:port/dn?attributes?scope?filter?extensions
 *
 * Returns
 *	0 if the lookup was successful,
 *	1 if the connection to the LDAP server could be established but
 *	  the search was unsuccessful,
 *	2 if a connection could not be established, and
 *	3 if a fatal error occurred.
 *
 * An error message is returned in the third argument for return codes 1 and 3.
 */
static int ldapServiceLookup(const char* purl, PQconninfoOption* options, PQExpBuffer errorMessage)
{
    int port = LDAP_DEF_PORT, scope, rc, msgid, size, state, oldstate, i;
    bool found_keyword = false;
    char *url = NULL, *hostname = NULL, *portstr = NULL, *endptr = NULL, *dn = NULL, *scopestr = NULL, *filter = NULL,
         *result = NULL, *p = NULL, *p1 = NULL, *optname = NULL, *optval = NULL;
    char* attrs[2] = {NULL, NULL};
    LDAP* ld = NULL;
    LDAPMessage *res = NULL, *entry = NULL;
    struct berval** values = NULL;
    LDAP_TIMEVAL time = {PGLDAP_TIMEOUT, 0};
    errno_t securec_rc = EOK;

    if ((url = strdup(purl)) == NULL) {
        printfPQExpBuffer(errorMessage, libpq_gettext("out of memory\n"));
        return 3;
    }

    /*
     * Parse URL components, check for correctness.  Basically, url has '\0'
     * placed at component boundaries and variables are pointed at each
     * component.
     */

    if (pg_strncasecmp(url, LDAP_URL, strlen(LDAP_URL)) != 0) {
        printfPQExpBuffer(errorMessage, libpq_gettext("invalid LDAP URL \"%s\": scheme must be ldap://\n"), purl);
        libpq_free(url);
        return 3;
    }

    /* hostname */
    hostname = url + strlen(LDAP_URL);
    if (*hostname == '/')       /* no hostname? */
        hostname = DefaultHost; /* the default */

    /* dn, "distinguished name" */
    p = strchr(url + strlen(LDAP_URL), '/');
    if (p == NULL || *(p + 1) == '\0' || *(p + 1) == '?') {
        printfPQExpBuffer(errorMessage, libpq_gettext("invalid LDAP URL \"%s\": missing distinguished name\n"), purl);
        libpq_free(url);
        return 3;
    }
    *p = '\0'; /* terminate hostname */
    dn = p + 1;

    /* attribute */
    if ((p = strchr(dn, '?')) == NULL || *(p + 1) == '\0' || *(p + 1) == '?') {
        printfPQExpBuffer(
            errorMessage, libpq_gettext("invalid LDAP URL \"%s\": must have exactly one attribute\n"), purl);
        libpq_free(url);
        return 3;
    }
    *p = '\0';
    attrs[0] = p + 1;

    /* scope */
    if ((p = strchr(attrs[0], '?')) == NULL || *(p + 1) == '\0' || *(p + 1) == '?') {
        printfPQExpBuffer(
            errorMessage, libpq_gettext("invalid LDAP URL \"%s\": must have search scope (base/one/sub)\n"), purl);
        libpq_free(url);
        return 3;
    }
    *p = '\0';
    scopestr = p + 1;

    /* filter */
    if ((p = strchr(scopestr, '?')) == NULL || *(p + 1) == '\0' || *(p + 1) == '?') {
        printfPQExpBuffer(errorMessage, libpq_gettext("invalid LDAP URL \"%s\": no filter\n"), purl);
        libpq_free(url);
        return 3;
    }
    *p = '\0';
    filter = p + 1;
    if ((p = strchr(filter, '?')) != NULL)
        *p = '\0';

    /* port number? */
    if ((p1 = strchr(hostname, ':')) != NULL) {
        long lport;

        *p1 = '\0';
        portstr = p1 + 1;
        errno = 0;
        lport = strtol(portstr, &endptr, 10);
        if (*portstr == '\0' || *endptr != '\0' || errno || lport < 0 || lport > 65535) {
            printfPQExpBuffer(errorMessage, libpq_gettext("invalid LDAP URL \"%s\": invalid port number\n"), purl);
            libpq_free(url);
            return 3;
        }
        port = (int)lport;
    }

    /* Allow only one attribute */
    if (strchr(attrs[0], ',') != NULL) {
        printfPQExpBuffer(
            errorMessage, libpq_gettext("invalid LDAP URL \"%s\": must have exactly one attribute\n"), purl);
        libpq_free(url);
        return 3;
    }

    /* set scope */
    if (pg_strcasecmp(scopestr, "base") == 0)
        scope = LDAP_SCOPE_BASE;
    else if (pg_strcasecmp(scopestr, "one") == 0)
        scope = LDAP_SCOPE_ONELEVEL;
    else if (pg_strcasecmp(scopestr, "sub") == 0)
        scope = LDAP_SCOPE_SUBTREE;
    else {
        printfPQExpBuffer(
            errorMessage, libpq_gettext("invalid LDAP URL \"%s\": must have search scope (base/one/sub)\n"), purl);
        libpq_free(url);
        return 3;
    }

    /* initialize LDAP structure */
    if ((ld = ldap_init(hostname, port)) == NULL) {
        printfPQExpBuffer(errorMessage, libpq_gettext("could not create LDAP structure\n"));
        libpq_free(url);
        return 3;
    }

    /*
     * Initialize connection to the server.  We do an explicit bind because we
     * want to return 2 if the bind fails.
     */
    if ((msgid = ldap_simple_bind(ld, NULL, NULL)) == -1) {
        /* error in ldap_simple_bind() */
        libpq_free(url);
        ldap_unbind(ld);
        return 2;
    }

    /* wait some time for the connection to succeed */
    res = NULL;
    if ((rc = ldap_result(ld, msgid, LDAP_MSG_ALL, &time, &res)) == -1 || res == NULL) {
        if (res != NULL) {
            /* timeout */
            ldap_msgfree(res);
        }
        /* error in ldap_result() */
        libpq_free(url);
        ldap_unbind(ld);
        return 2;
    }
    ldap_msgfree(res);

    /* search */
    res = NULL;
    if ((rc = ldap_search_st(ld, dn, scope, filter, attrs, 0, &time, &res)) != LDAP_SUCCESS) {
        if (res != NULL)
            ldap_msgfree(res);
        printfPQExpBuffer(errorMessage, libpq_gettext("lookup on LDAP server failed: %s\n"), ldap_err2string(rc));
        ldap_unbind(ld);
        libpq_free(url);
        return 1;
    }

    /* complain if there was not exactly one result */
    if ((rc = ldap_count_entries(ld, res)) != 1) {
        printfPQExpBuffer(errorMessage,
            rc ? libpq_gettext("more than one entry found on LDAP lookup\n")
               : libpq_gettext("no entry found on LDAP lookup\n"));
        ldap_msgfree(res);
        ldap_unbind(ld);
        libpq_free(url);
        return 1;
    }

    /* get entry */
    if ((entry = ldap_first_entry(ld, res)) == NULL) {
        /* should never happen */
        printfPQExpBuffer(errorMessage, libpq_gettext("no entry found on LDAP lookup\n"));
        ldap_msgfree(res);
        ldap_unbind(ld);
        libpq_free(url);
        return 1;
    }

    /* get values */
    if ((values = ldap_get_values_len(ld, entry, attrs[0])) == NULL) {
        printfPQExpBuffer(errorMessage, libpq_gettext("attribute has no values on LDAP lookup\n"));
        ldap_msgfree(res);
        ldap_unbind(ld);
        libpq_free(url);
        return 1;
    }

    ldap_msgfree(res);
    libpq_free(url);

    if (values[0] == NULL) {
        printfPQExpBuffer(errorMessage, libpq_gettext("attribute has no values on LDAP lookup\n"));
        ldap_value_free_len(values);
        ldap_unbind(ld);
        return 1;
    }

    /* concatenate values into a single string with newline terminators */
    size = 1; /* for the trailing null */
    for (i = 0; values[i] != NULL; i++)
        size += values[i]->bv_len + 1;
#ifdef WIN32
    if ((result = (char*)malloc(size)) == NULL)
#else
    if ((result = malloc(size)) == NULL)
#endif
    {
        printfPQExpBuffer(errorMessage, libpq_gettext("out of memory\n"));
        ldap_value_free_len(values);
        ldap_unbind(ld);
        return 3;
    }
    p = result;
    for (i = 0; values[i] != NULL; i++) {
        // bv_len is ULONG > SECUREC_MEM_MAX_LEN
        securec_rc = memcpy_s(p, values[i]->bv_len, values[i]->bv_val, values[i]->bv_len);
        securec_check_c(securec_rc, "", "");
        p += values[i]->bv_len;
        *(p++) = '\n';
    }
    *p = '\0';

    ldap_value_free_len(values);
    ldap_unbind(ld);

    /* parse result string */
    oldstate = state = 0;
    for (p = result; *p != '\0'; ++p) {
        switch (state) {
            case 0: /* between entries */
                if (!ld_is_sp_tab(*p) && !ld_is_nl_cr(*p)) {
                    optname = p;
                    state = 1;
                }
                break;
            case 1: /* in option name */
                if (ld_is_sp_tab(*p)) {
                    *p = '\0';
                    state = 2;
                } else if (ld_is_nl_cr(*p)) {
                    printfPQExpBuffer(
                        errorMessage, libpq_gettext("missing \"=\" after \"%s\" in connection info string\n"), optname);
                    libpq_free(result);
                    return 3;
                } else if (*p == '=') {
                    *p = '\0';
                    state = 3;
                }
                break;
            case 2: /* after option name */
                if (*p == '=') {
                    state = 3;
                } else if (!ld_is_sp_tab(*p)) {
                    printfPQExpBuffer(
                        errorMessage, libpq_gettext("missing \"=\" after \"%s\" in connection info string\n"), optname);
                    libpq_free(result);
                    return 3;
                }
                break;
            case 3: /* before option value */
                if (*p == '\'') {
                    optval = p + 1;
                    p1 = p + 1;
                    state = 5;
                } else if (ld_is_nl_cr(*p)) {
                    optval = optname + strlen(optname); /* empty */
                    state = 0;
                } else if (!ld_is_sp_tab(*p)) {
                    optval = p;
                    state = 4;
                }
                break;
            case 4: /* in unquoted option value */
                if (ld_is_sp_tab(*p) || ld_is_nl_cr(*p)) {
                    *p = '\0';
                    state = 0;
                }
                break;
            case 5: /* in quoted option value */
                if (*p == '\'') {
                    *p1 = '\0';
                    state = 0;
                } else if (*p == '\\')
                    state = 6;
                else
                    *(p1++) = *p;
                break;
            case 6: /* in quoted option value after escape */
                *(p1++) = *p;
                state = 5;
                break;
            default:
                break;
        }

        if (state == 0 && oldstate != 0) {
            found_keyword = false;
            for (i = 0; options[i].keyword; i++) {
                if (strcmp(options[i].keyword, optname) == 0) {
                    if (options[i].val == NULL) {
                        options[i].val = strdup(optval);
                        options[i].valsize = (options[i].val != NULL) ? strlen(options[i].val) : 0;
                    }

                    found_keyword = true;
                    break;
                }
            }
            if (!found_keyword) {
                printfPQExpBuffer(errorMessage, libpq_gettext("invalid connection option \"%s\"\n"), optname);
                libpq_free(result);
                return 1;
            }
            optname = NULL;
            optval = NULL;
        }
        oldstate = state;
    }

    libpq_free(result);

    if (state == 5 || state == 6) {
        printfPQExpBuffer(errorMessage, libpq_gettext("unterminated quoted string in connection info string\n"));
        return 3;
    }

    return 0;
}
#endif

#define MAXBUFSIZE 256

static int parseServiceInfo(PQconninfoOption* options, PQExpBuffer errorMessage)
{
    const char* service = conninfo_getval(options, "service");
    char serviceFile[MAXPGPATH] = {0};
    char* env = NULL;
    bool group_found = false;
    bool malloced = false;
    int status;
    struct stat stat_buf;
    int nRet = 0;

    /*
     * We have to special-case the environment variable PGSERVICE here, since
     * this is and should be called before inserting environment defaults for
     * other connection options.
     */
    if (service == NULL) {
        char* tmp = gs_getenv_r("PGSERVICE");
        if (check_client_env(tmp) == NULL)
            return 0;

        service = strdup(tmp);
        if (service != NULL)
            malloced = true;
        else
            return 0;
    }

    env = gs_getenv_r("PGSERVICEFILE");
    if (check_client_env(env) != NULL) {
        check_strncpy_s(strncpy_s(serviceFile, sizeof(serviceFile), env, strlen(env)));
    } else {
        char homedir[MAXPGPATH] = {0};

        if (!pqGetHomeDirectory(homedir, sizeof(homedir))) {
            printfPQExpBuffer(
                errorMessage,
                libpq_gettext("could not get home directory to locate service definition file, errno: %s"),
                strerror(errno));
            if (malloced) {
                free((char*)service);
                service = NULL;
            }
            return 1;
        }
        nRet = snprintf_s(serviceFile, MAXPGPATH, MAXPGPATH - 1, "%s/%s", homedir, ".pg_service.conf");
        securec_check_ss_c(nRet, "\0", "\0");
        errno = 0;
        if (stat(serviceFile, &stat_buf) != 0 && errno == ENOENT)
            goto next_file;
    }

    status = parseServiceFile(serviceFile, service, options, errorMessage, &group_found);
    if (group_found || status != 0) {
        if (malloced) {
            free((char*)service);
            service = NULL;
        }
        return status;
    }

next_file:

    /*
     * This could be used by any application so we can't use the binary
     * location to find our config files.
     */
    nRet = snprintf_s(serviceFile,
        MAXPGPATH,
        MAXPGPATH - 1,
        "%s/pg_service.conf",
        check_client_env(gs_getenv_r("PGSYSCONFDIR")) != NULL ? gs_getenv_r("PGSYSCONFDIR") : SYSCONFDIR);
    securec_check_ss_c(nRet, "\0", "\0");
    errno = 0;
    if (stat(serviceFile, &stat_buf) != 0 && errno == ENOENT)
        goto last_file;

    status = parseServiceFile(serviceFile, service, options, errorMessage, &group_found);
    if (status != 0) {
        if (malloced) {
            free((char*)service);
            service = NULL;
        }
        return status;
    }

last_file:
    if (!group_found) {
        printfPQExpBuffer(errorMessage,
            libpq_gettext("definition of service \"%s\" not found, errno: %s\n"), service, strerror(errno));
        if (malloced) {
            free((char*)service);
            service = NULL;
        }
        return 3;
    }
    if (malloced) {
        free((char*)service);
        service = NULL;
    }
    return 0;
}

static int parseServiceFile(const char* serviceFile, const char* service, PQconninfoOption* options,
    PQExpBuffer errorMessage, bool* group_found)
{
    int linenr = 0, i;
    FILE* f = NULL;
    char buf[MAXBUFSIZE] = {0}, *line = NULL;

    f = fopen(serviceFile, "r");
    if (f == NULL) {
        printfPQExpBuffer(errorMessage, libpq_gettext("service file \"%s\" not found\n"), serviceFile);
        return 1;
    }

    while ((line = fgets(buf, sizeof(buf), f)) != NULL) {
        linenr++;

        if (strlen(line) >= sizeof(buf) - 1) {
            fclose(f);
            printfPQExpBuffer(errorMessage,
                libpq_gettext("line %d too long in service file \"%s\", errno: %s\n"),
                linenr,
                serviceFile,
                strerror(errno));
            return 2;
        }

        /* ignore EOL at end of line */
        if (strlen(line) && line[strlen(line) - 1] == '\n')
            line[strlen(line) - 1] = 0;

        /* ignore leading blanks */
        while (*line && isspace((unsigned char)line[0]))
            line++;

        /* ignore comments and empty lines */
        if (strlen(line) == 0 || line[0] == '#')
            continue;

        /* Check for right groupname */
        if (line[0] == '[') {
            if (*group_found) {
                /* group info already read */
                fclose(f);
                return 0;
            }

            if (strncmp(line + 1, service, strlen(service)) == 0 && line[strlen(service) + 1] == ']')
                *group_found = true;
            else
                *group_found = false;
        } else {
            if (*group_found) {
                /*
                 * Finally, we are in the right group and can parse the line
                 */
                char* key = NULL;
                char* val = NULL;
                bool found_keyword = false;

#ifdef USE_LDAP
                if (strncmp(line, "ldap", 4) == 0) {
                    int rc = ldapServiceLookup(line, options, errorMessage);

                    /* if rc = 2, go on reading for fallback */
                    switch (rc) {
                        case 0:
                            fclose(f);
                            return 0;
                        case 1:
                        case 3:
                            fclose(f);
                            return 3;
                        case 2:
                            continue;
                    }
                }
#endif

                key = line;
                val = strchr(line, '=');
                if (val == NULL) {
                    printfPQExpBuffer(errorMessage,
                        libpq_gettext("syntax error in service file \"%s\", line %d\n"),
                        serviceFile,
                        linenr);
                    fclose(f);
                    return 3;
                }
                *val++ = '\0';

                /*
                 * Set the parameter --- but don't override any previous
                 * explicit setting.
                 */
                found_keyword = false;
                for (i = 0; options[i].keyword != NULL; i++) {
                    if (strcmp(options[i].keyword, key) == 0) {
                        if (options[i].val == NULL) {
                            options[i].val = strdup(val);
                            options[i].valsize = (options[i].val != NULL) ? strlen(options[i].val) : 0;
                        }

                        found_keyword = true;
                        break;
                    }
                }

                if (!found_keyword) {
                    printfPQExpBuffer(errorMessage,
                        libpq_gettext("syntax error in service file \"%s\", line %d\n"),
                        serviceFile,
                        linenr);
                    fclose(f);
                    return 3;
                }
            }
        }
    }

    fclose(f);

    return 0;
}

/*
 *		PQconninfoParse
 *
 * Parse a string like PQconnectdb() would do and return the
 * resulting connection options array.	NULL is returned on failure.
 * The result contains only options specified directly in the string,
 * not any possible default values.
 *
 * If errmsg isn't NULL, *errmsg is set to NULL on success, or a malloc'd
 * string on failure (use PQfreemem to free it).  In out-of-memory conditions
 * both *errmsg and the result could be NULL.
 *
 * NOTE: the returned array is dynamically allocated and should
 * be freed when no longer needed via PQconninfoFree().
 */
PQconninfoOption* PQconninfoParse(const char* conninfo, char** errmsg)
{
    PQExpBufferData errorBuf;
    PQconninfoOption* connOptions = NULL;

    if (errmsg != NULL)
        *errmsg = NULL; /* default */
    initPQExpBuffer(&errorBuf);
    if (PQExpBufferDataBroken(errorBuf))
        return NULL; /* out of memory already :-( */
    connOptions = parse_connection_string(conninfo, &errorBuf, false);
    if (connOptions == NULL && errmsg != NULL)
        *errmsg = errorBuf.data;
    else
        termPQExpBuffer(&errorBuf);
    return connOptions;
}

/*
 * Build a working copy of the constant PQconninfoOptions array.
 */
static PQconninfoOption* conninfo_init(PQExpBuffer errorMessage)
{
    PQconninfoOption* options = NULL;

    options = (PQconninfoOption*)malloc(sizeof(PQconninfoOptions));
    if (options == NULL) {
        printfPQExpBuffer(errorMessage, libpq_gettext("out of memory\n"));
        return NULL;
    }

    check_memcpy_s(memcpy_s(options, sizeof(PQconninfoOptions), PQconninfoOptions, sizeof(PQconninfoOptions)));

    return options;
}

/*
 * Connection string parser
 *
 * Returns a malloc'd PQconninfoOption array, if parsing is successful.
 * Otherwise, NULL is returned and an error message is left in errorMessage.
 *
 * If use_defaults is TRUE, default values are filled in (from a service file,
 * environment variables, etc).
 */
static PQconninfoOption* parse_connection_string(const char* connstr, PQExpBuffer errorMessage, bool use_defaults)
{
    /* Parse as URI if connection string matches URI prefix */
    if (uri_prefix_length(connstr) != 0)
        return conninfo_uri_parse(connstr, errorMessage, use_defaults);

    /* Parse as default otherwise */
    return conninfo_parse(connstr, errorMessage, use_defaults);
}

/*
 * Checks if connection string starts with either of the valid URI prefix
 * designators.
 *
 * Returns the URI prefix length, 0 if the string doesn't contain a URI prefix.
 */
static int uri_prefix_length(const char* connstr)
{
    if (strncmp(connstr, uri_designator, sizeof(uri_designator) - 1) == 0)
        return sizeof(uri_designator) - 1;

    if (strncmp(connstr, short_uri_designator, sizeof(short_uri_designator) - 1) == 0)
        return sizeof(short_uri_designator) - 1;

    return 0;
}

/*
 * Recognized connection string either starts with a valid URI prefix or
 * contains a "=" in it.
 *
 * Must be consistent with parse_connection_string: anything for which this
 * returns true should at least look like it's parseable by that routine.
 */
static bool recognized_connection_string(const char* connstr)
{
    return uri_prefix_length(connstr) != 0 || strchr(connstr, '=') != NULL;
}

/*
 * Subroutine for parse_connection_string
 *
 * Deal with a string containing key=value pairs.
 */
static PQconninfoOption* conninfo_parse(const char* conninfo, PQExpBuffer errorMessage, bool use_defaults)
{
    char* pname = NULL;
    char* pval = NULL;
    char* buf = NULL;
    char* cp = NULL;
    char* cp2 = NULL;
    PQconninfoOption* options = NULL;

    /* Make a working copy of PQconninfoOptions */
    options = conninfo_init(errorMessage);
    if (options == NULL)
        return NULL;

    /* Need a modifiable copy of the input string */
    if ((buf = strdup(conninfo)) == NULL) {
        printfPQExpBuffer(errorMessage, libpq_gettext("out of memory\n"));
        PQconninfoFree(options);
        return NULL;
    }
    cp = buf;

    while (*cp) {
        /* Skip blanks before the parameter name */
        if (isspace((unsigned char)*cp)) {
            cp++;
            continue;
        }

        /* Get the parameter name */
        pname = cp;
        while (*cp) {
            if (*cp == '=')
                break;
            if (isspace((unsigned char)*cp)) {
                *cp++ = '\0';
                while (*cp) {
                    if (!isspace((unsigned char)*cp))
                        break;
                    cp++;
                }
                break;
            }
            cp++;
        }

        /* Check that there is a following '=' */
        if (*cp != '=') {
            printfPQExpBuffer(
                errorMessage, libpq_gettext("missing \"=\" after \"%s\" in connection info string\n"), pname);
            PQconninfoFree(options);
            libpq_free(buf);
            return NULL;
        }
        *cp++ = '\0';

        /* Skip blanks after the '=' */
        while (*cp) {
            if (!isspace((unsigned char)*cp))
                break;
            cp++;
        }

        /* Get the parameter value */
        pval = cp;

        if (*cp != '\'') {
            cp2 = pval;
            while (*cp) {
                if (isspace((unsigned char)*cp)) {
                    *cp++ = '\0';
                    break;
                }
                if (*cp == '\\') {
                    cp++;
                    if (*cp != '\0')
                        *cp2++ = *cp++;
                } else
                    *cp2++ = *cp++;
            }
            *cp2 = '\0';
        } else {
            cp2 = pval;
            cp++;
            for (;;) {
                if (*cp == '\0') {
                    printfPQExpBuffer(
                        errorMessage, libpq_gettext("unterminated quoted string in connection info string\n"));
                    PQconninfoFree(options);
                    libpq_free(buf);
                    return NULL;
                }
                if (*cp == '\\') {
                    cp++;
                    if (*cp != '\0')
                        *cp2++ = *cp++;
                    continue;
                }
                if (*cp == '\'') {
                    *cp2 = '\0';
                    cp++;
                    break;
                }
                *cp2++ = *cp++;
            }
        }

        /*
         * Now that we have the name and the value, store the record.
         */
        if (conninfo_storeval(options, pname, pval, errorMessage, false, false) == NULL) {
            PQconninfoFree(options);
            libpq_free(buf);
            return NULL;
        }
    }

    /* Done with the modifiable input string */
    libpq_free(buf);

    /*
     * Add in defaults if the caller wants that.
     */
    if (use_defaults) {
        if (!conninfo_add_defaults(options, errorMessage)) {
            PQconninfoFree(options);
            return NULL;
        }
    }

    return options;
}

/*
 * Conninfo array parser routine
 *
 * If successful, a malloc'd PQconninfoOption array is returned.
 * If not successful, NULL is returned and an error message is
 * left in errorMessage.
 * Defaults are supplied (from a service file, environment variables, etc)
 * for unspecified options, but only if use_defaults is TRUE.
 *
 * If expand_dbname is non-zero, and the value passed for keyword "dbname" is a
 * connection string (as indicated by recognized_connection_string) then parse
 * and process it, overriding any previously processed conflicting
 * keywords. Subsequent keywords will take precedence, however.
 */
static PQconninfoOption* conninfo_array_parse(const char* const* keywords, const char* const* values,
    PQExpBuffer errorMessage, bool use_defaults, int expand_dbname)
{
    PQconninfoOption* options = NULL;
    PQconninfoOption* dbname_options = NULL;
    PQconninfoOption* option = NULL;
    int i = 0;

    /*
     * If expand_dbname is non-zero, check keyword "dbname" to see if val is
     * actually a recognized connection string.
     */
    while (expand_dbname && (keywords[i] != NULL)) {
        const char* pname = keywords[i];
        const char* pvalue = values[i];

        /* first find "dbname" if any */
        if (strcmp(pname, "dbname") == 0 && (pvalue != NULL)) {
            /*
             * If value is a connection string, parse it, but do not use
             * defaults here -- those get picked up later. We only want to
             * override for those parameters actually passed.
             */
            if (recognized_connection_string(pvalue)) {
                dbname_options = parse_connection_string(pvalue, errorMessage, false);
                if (dbname_options == NULL)
                    return NULL;
            }
            break;
        }
        ++i;
    }

    /* Make a working copy of PQconninfoOptions */
    options = conninfo_init(errorMessage);
    if (options == NULL) {
        PQconninfoFree(dbname_options);
        return NULL;
    }

    /* Parse the keywords/values arrays */
    i = 0;
    while (keywords[i] != NULL) {
        const char* pname = keywords[i];
        const char* pvalue = values[i];

        if (pvalue != NULL) {
            /* Search for the param record */
            for (option = options; option->keyword != NULL; option++) {
                if (strcmp(option->keyword, pname) == 0)
                    break;
            }

            /* Check for invalid connection option */
            if (option->keyword == NULL) {
                printfPQExpBuffer(errorMessage, libpq_gettext("invalid connection option \"%s\"\n"), pname);
                PQconninfoFree(options);
                PQconninfoFree(dbname_options);
                return NULL;
            }

            /*
             * If we are on the dbname parameter, and we have a parsed
             * connection string, copy those parameters across, overriding any
             * existing previous settings.
             */
            if (strcmp(pname, "dbname") == 0 && (dbname_options != NULL)) {
                PQconninfoOption* str_option = NULL;

                for (str_option = dbname_options; str_option->keyword != NULL; str_option++) {
                    if (str_option->val != NULL) {
                        int k;

                        for (k = 0; options[k].keyword != NULL; k++) {
                            if (strcmp(options[k].keyword, str_option->keyword) == 0) {
                                if (options[k].val != NULL)
                                    free(options[k].val);
                                options[k].val = strdup(str_option->val);
                                options[k].valsize = (options[k].val != NULL) ? strlen(options[k].val) : 0;
                                break;
                            }
                        }
                    }
                }
            } else {
                /*
                 * Store the value, overriding previous settings
                 */
                libpq_free(option->val);
                option->val = strdup(pvalue);
                if (option->val == NULL) {
                    printfPQExpBuffer(errorMessage, libpq_gettext("out of memory\n"));
                    PQconninfoFree(options);
                    PQconninfoFree(dbname_options);
                    return NULL;
                }

                option->valsize = strlen(option->val);
            }
        }
        ++i;
    }
    PQconninfoFree(dbname_options);

    /*
     * Add in defaults if the caller wants that.
     */
    if (use_defaults) {
        if (!conninfo_add_defaults(options, errorMessage)) {
            PQconninfoFree(options);
            return NULL;
        }
    }

    return options;
}

/*
 * Add the default values for any unspecified options to the connection
 * options array.
 *
 * Defaults are obtained from a service file, environment variables, etc.
 *
 * Returns TRUE if successful, otherwise FALSE; errorMessage is filled in
 * upon failure.  Note that failure to locate a default value is not an
 * error condition here --- we just leave the option's value as NULL.
 */
static bool conninfo_add_defaults(PQconninfoOption* options, PQExpBuffer errorMessage)
{
    PQconninfoOption* option = NULL;
    char* tmp = NULL;

    /*
     * If there's a service spec, use it to obtain any not-explicitly-given
     * parameters.
     */
    if (parseServiceInfo(options, errorMessage) != 0)
        return false;

    /*
     * Get the fallback resources for parameters not specified in the conninfo
     * string nor the service.
     */
    for (option = options; option->keyword != NULL; option++) {
        if (option->val != NULL)
            continue; /* Value was in conninfo or service */

        /*
         * Try to get the environment variable fallback
         */
        if (option->envvar != NULL && 0 != pg_strncasecmp(option->envvar, "PGPASSWORD", 10)) {
            tmp = gs_getenv_r(option->envvar);
            if (check_client_env(tmp) != NULL) {
                option->val = strdup(tmp);
                if (option->val == NULL) {
                    printfPQExpBuffer(errorMessage, libpq_gettext("out of memory\n"));
                    return false;
                }

                option->valsize = strlen(option->val);
                continue;
            }
        }

        /*
         * No environment variable specified or the variable isn't set - try
         * compiled-in default
         */
        if (option->compiled != NULL) {
            option->val = strdup(option->compiled);
            if (option->val == NULL) {
                printfPQExpBuffer(errorMessage, libpq_gettext("out of memory\n"));
                return false;
            }

            option->valsize = strlen(option->val);
            continue;
        }

        /*
         * Special handling for "user" option
         */
        if (strcmp(option->keyword, "user") == 0) {
            option->val = pg_fe_getauthname(errorMessage);
            option->valsize = (option->val == NULL) ? 0 : strlen(option->val);
            continue;
        }
    }

    return true;
}

/*
 * Subroutine for parse_connection_string
 *
 * Deal with a URI connection string.
 */
static PQconninfoOption* conninfo_uri_parse(const char* uri, PQExpBuffer errorMessage, bool use_defaults)
{
    PQconninfoOption* options = NULL;

    /* Make a working copy of PQconninfoOptions */
    options = conninfo_init(errorMessage);
    if (options == NULL)
        return NULL;

    if (!conninfo_uri_parse_options(options, uri, errorMessage)) {
        PQconninfoFree(options);
        return NULL;
    }

    /*
     * Add in defaults if the caller wants that.
     */
    if (use_defaults) {
        if (!conninfo_add_defaults(options, errorMessage)) {
            PQconninfoFree(options);
            return NULL;
        }
    }

    return options;
}

/*
 * conninfo_uri_parse_options
 *		Actual URI parser.
 *
 * If successful, returns true while the options array is filled with parsed
 * options from the URI.
 * If not successful, returns false and fills errorMessage accordingly.
 *
 * Parses the connection URI string in 'uri' according to the URI syntax (RFC
 * 3986):
 *
 * postgresql://[user[:password]@][netloc][:port][/dbname][?param1=value1&...]
 *
 * where "netloc" is a hostname, an IPv4 address, or an IPv6 address surrounded
 * by literal square brackets.
 *
 * Any of the URI parts might use percent-encoding (%xy).
 */
static bool conninfo_uri_parse_options(PQconninfoOption* options, const char* uri, PQExpBuffer errorMessage)
{
    int prefix_len;
    char* p = NULL;
    char* buf = strdup(uri); /* need a modifiable copy of the input
                              * URI */
    char* start = buf;
    char prevchar = '\0';
    char* user = NULL;
    char* host = NULL;
    bool retval = false;
    PQExpBufferData hostbuf;
    PQExpBufferData portbuf;

    initPQExpBuffer(&hostbuf);
    initPQExpBuffer(&portbuf);
    if (PQExpBufferDataBroken(hostbuf) || PQExpBufferDataBroken(portbuf)) {
        appendPQExpBufferStr(errorMessage,
                             libpq_gettext("out of memory\n"));
        goto cleanup;
    }

    if (buf == NULL) {
        printfPQExpBuffer(errorMessage, libpq_gettext("out of memory\n"));
        return false;
    }

    /* Skip the URI prefix */
    prefix_len = uri_prefix_length(uri);
    if (prefix_len == 0) {
        /* Should never happen */
        printfPQExpBuffer(
            errorMessage, libpq_gettext("invalid URI propagated to internal parser routine: \"%s\"\n"), uri);
        goto cleanup;
    }
    start += prefix_len;
    p = start;

    /* Look ahead for possible user credentials designator */
    while (*p && *p != '@' && *p != '/')
        ++p;
    if (*p == '@') {
        /*
         * Found username/password designator, so URI should be of the form
         * "scheme://user[:password]@[netloc]".
         */
        user = start;

        p = user;
        while (p != NULL && *p != ':' && *p != '@')
            ++p;

        /* Save last char and cut off at end of user name */
        prevchar = *p;
        *p = '\0';

        if (*user && conninfo_storeval(options, "user", user, errorMessage, false, true) == NULL)
            goto cleanup;

        if (prevchar == ':') {
            const char* password = p + 1;

            while (p != NULL && *p != '@')
                ++p;
            *p = '\0';

            if (*password && conninfo_storeval(options, "password", password, errorMessage, false, true) == NULL)
                goto cleanup;
        }

        /* Advance past end of parsed user name or password token */
        ++p;
    } else {
        /*
         * No username/password designator found.  Reset to start of URI.
         */
        p = start;
    }

    /*
     * There may be multiple netloc[:port] pairs, each separated from the next
     * by a comma.  When we initially enter this loop, "p" has been
     * incremented past optional URI credential information at this point and
     * now points at the "netloc" part of the URI.  On subsequent loop
     * iterations, "p" has been incremented past the comma separator and now
     * points at the start of the next "netloc".
     */
    for (;;) {
        /*
         * Look for IPv6 address.
         */
        if (*p == '[') {
            host = ++p;
            while (*p && *p != ']')
                ++p;
            if (!*p) {
                printfPQExpBuffer(errorMessage,
                    libpq_gettext(
                        "end of string reached when looking for matching \"]\" in IPv6 host address in URI: \"%s\"\n"),
                    uri);
                goto cleanup;
            }
            if (p == host) {
                printfPQExpBuffer(errorMessage, libpq_gettext("IPv6 host address may not be empty in URI: \"%s\"\n"), uri);
                goto cleanup;
            }

            /* Cut off the bracket and advance */
            *(p++) = '\0';

            /*
             * The address may be followed by a port specifier or a slash or a
             * query.
             */
            if (*p && *p != ':' && *p != '/' && *p != '?') {
                printfPQExpBuffer(errorMessage,
                    libpq_gettext("unexpected character \"%c\" at position %d in URI (expected \":\" or \"/\"): \"%s\"\n"),
                    *p,
                    (int)(p - buf + 1),
                    uri);
                goto cleanup;
            }
        } else {
            /* not an IPv6 address: DNS-named or IPv4 netloc */
            host = p;

            /*
             * Look for port specifier (colon) or end of host specifier (slash),
             * or query (question mark).
             */
            while (*p && *p != ':' && *p != '/' && *p != '?')
                ++p;
        }

        /* Save the hostname terminator before we null it */
        prevchar = *p;
        *p = '\0';

        appendPQExpBufferStr(&hostbuf, host);

        if (prevchar == ':') {
            const char *port = ++p; /* advance past host terminator */

            while (*p && *p != '/' && *p != '?' && *p != ',')
                ++p;

            prevchar = *p;
            *p = '\0';

            appendPQExpBufferStr(&portbuf, port);
        }

        if (prevchar != ',') {
            break;
        }

        ++p;                    /* advance past comma separator */
        appendPQExpBufferChar(&hostbuf, ',');
        appendPQExpBufferChar(&portbuf, ',');
    }

    /* Save final values for host and port. */
    if (PQExpBufferDataBroken(hostbuf) || PQExpBufferDataBroken(portbuf))
        goto cleanup;
    if (hostbuf.data[0] &&
        !conninfo_storeval(options, "host", hostbuf.data,
                           errorMessage, false, true))
        goto cleanup;
    if (portbuf.data[0] &&
        !conninfo_storeval(options, "port", portbuf.data,
                           errorMessage, false, true))
        goto cleanup;

    if (prevchar && prevchar != '?') {
        const char* dbname = ++p; /* advance past host terminator */

        /* Look for query parameters */
        while (*p && *p != '?')
            ++p;

        prevchar = *p;
        *p = '\0';

        /*
         * Avoid setting dbname to an empty string, as it forces the default
         * value (username) and ignores $PGDATABASE, as opposed to not setting
         * it at all.
         */
        if (*dbname && conninfo_storeval(options, "dbname", dbname, errorMessage, false, true) == NULL)
            goto cleanup;
    }

    if (prevchar) {
        ++p; /* advance past terminator */

        if (!conninfo_uri_parse_params(p, options, errorMessage))
            goto cleanup;
    }

    /* everything parsed okay */
    retval = true;

cleanup:
    check_memset_s(memset_s(buf, strlen(buf) + 1, 0, strlen(buf) + 1));
    p = NULL;
    libpq_free(buf);
    termPQExpBuffer(&hostbuf);
    termPQExpBuffer(&portbuf);
    return retval;
}

/*
 * Connection URI parameters parser routine
 *
 * If successful, returns true while connOptions is filled with parsed
 * parameters.	Otherwise, returns false and fills errorMessage appropriately.
 *
 * Destructively modifies 'params' buffer.
 */
static bool conninfo_uri_parse_params(char* params, PQconninfoOption* connOptions, PQExpBuffer errorMessage)
{
    while (*params) {
        char* keyword = params;
        char* value = NULL;
        char* p = params;
        bool malloced = false;

        /*
         * Scan the params string for '=' and '&', marking the end of keyword
         * and value respectively.
         */
        for (;;) {
            if (*p == '=') {
                /* Was there '=' already? */
                if (value != NULL) {
                    printfPQExpBuffer(errorMessage,
                        libpq_gettext("extra key/value separator \"=\" in URI query parameter: \"%s\"\n"),
                        params);
                    return false;
                }
                /* Cut off keyword, advance to value */
                *p = '\0';
                value = ++p;
            } else if (*p == '&' || *p == '\0') {
                char prevchar;

                /* Cut off value, remember old value */
                prevchar = *p;
                *p = '\0';

                /* Was there '=' at all? */
                if (value == NULL) {
                    printfPQExpBuffer(errorMessage,
                        libpq_gettext("missing key/value separator \"=\" in URI query parameter: \"%s\"\n"),
                        params);
                    return false;
                }

                /*
                 * If not at the end, advance; now pointing to start of the
                 * next parameter, if any.
                 */
                if (prevchar != '\0')
                    ++p;
                break;
            } else
                ++p; /* Advance over all other bytes. */
        }

        keyword = conninfo_uri_decode(keyword, errorMessage);
        if (keyword == NULL) {
            /* conninfo_uri_decode already set an error message */
            return false;
        }
        value = conninfo_uri_decode(value, errorMessage);
        if (value == NULL) {
            /* conninfo_uri_decode already set an error message */
            libpq_free(keyword);
            return false;
        }
        malloced = true;

        /*
         * Special keyword handling for improved JDBC compatibility.
         */
        if (strcmp(keyword, "ssl") == 0 && strcmp(value, "true") == 0) {
            libpq_free(keyword);
            libpq_free(value);
            malloced = false;

            keyword = "sslmode";
            value = "require";
        }

        /*
         * Store the value if the corresponding option exists; ignore
         * otherwise.  At this point both keyword and value are not
         * URI-encoded.
         */
        if (conninfo_storeval(connOptions, keyword, value, errorMessage, true, false) == NULL) {
            /*
             * Check if there was a hard error when decoding or storing the
             * option.
             */
            if (errorMessage->len != 0) {
                if (malloced) {
                    libpq_free(keyword);
                    libpq_free(value);
                }
                return false;
            }

            printfPQExpBuffer(errorMessage, libpq_gettext("invalid URI query parameter: \"%s\"\n"), keyword);
            if (malloced) {
                libpq_free(keyword);
                libpq_free(value);
            }
            return false;
        }
        if (malloced) {
            libpq_free(keyword);
            libpq_free(value);
        }

        /* Proceed to next key=value pair */
        params = p;
    }

    return true;
}

/*
 * Connection URI decoder routine
 *
 * If successful, returns the malloc'd decoded string.
 * If not successful, returns NULL and fills errorMessage accordingly.
 *
 * The string is decoded by replacing any percent-encoded tokens with
 * corresponding characters, while preserving any non-encoded characters.  A
 * percent-encoded token is a character triplet: a percent sign, followed by a
 * pair of hexadecimal digits (0-9A-F), where lower- and upper-case letters are
 * treated identically.
 */
static char* conninfo_uri_decode(const char* str, PQExpBuffer errorMessage)
{
    char* buf = (char*)malloc(strlen(str) + 1);
    char* p = buf;
    const char* q = str;

    if (buf == NULL) {
        printfPQExpBuffer(errorMessage, libpq_gettext("out of memory\n"));
        return NULL;
    }

    for (;;) {
        if (*q != '%') {
            /* copy and check for NUL terminator */
            if (!(*(p++) = *(q++)))
                break;
        } else {
            unsigned int hi;
            unsigned int lo;
            int c;

            ++q; /* skip the percent sign itself */

            /*
             * Possible EOL will be caught by the first call to
             * get_hexdigit(), so we never dereference an invalid q pointer.
             */
            if (!(get_hexdigit(*q++, &hi) && get_hexdigit(*q++, &lo))) {
                printfPQExpBuffer(errorMessage, libpq_gettext("invalid percent-encoded token: \"%s\"\n"), str);
                libpq_free(buf);
                return NULL;
            }

            c = (hi << 4) | lo;
            if (c == 0) {
                printfPQExpBuffer(
                    errorMessage, libpq_gettext("forbidden value %%00 in percent-encoded value: \"%s\"\n"), str);
                libpq_free(buf);
                return NULL;
            }
            *(p++) = c;
        }
    }

    return buf;
}

/*
 * Convert hexadecimal digit character to its integer value.
 *
 * If successful, returns true and value is filled with digit's base 16 value.
 * If not successful, returns false.
 *
 * Lower- and upper-case letters in the range A-F are treated identically.
 */
static bool get_hexdigit(char digit, unsigned int* value)
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

/*
 * Find an option value corresponding to the keyword in the connOptions array.
 *
 * If successful, returns a pointer to the corresponding option's value.
 * If not successful, returns NULL.
 */
static const char* conninfo_getval(PQconninfoOption* connOptions, const char* keyword)
{
    PQconninfoOption* option = NULL;

    option = conninfo_find(connOptions, keyword);

    return option != NULL ? option->val : NULL;
}

/*
 * Store a (new) value for an option corresponding to the keyword in
 * connOptions array.
 *
 * If uri_decode is true, the value is URI-decoded.  The keyword is always
 * assumed to be non URI-encoded.
 *
 * If successful, returns a pointer to the corresponding PQconninfoOption,
 * which value is replaced with a strdup'd copy of the passed value string.
 * The existing value for the option is free'd before replacing, if any.
 *
 * If not successful, returns NULL and fills errorMessage accordingly.
 * However, if the reason of failure is an invalid keyword being passed and
 * ignoreMissing is TRUE, errorMessage will be left untouched.
 */
static PQconninfoOption* conninfo_storeval(PQconninfoOption* connOptions, const char* keyword, const char* value,
    PQExpBuffer errorMessage, bool ignoreMissing, bool uri_decode)
{
    PQconninfoOption* option = NULL;
    char* value_copy = NULL;

    option = conninfo_find(connOptions, keyword);
    if (option == NULL) {
        if (!ignoreMissing)
            printfPQExpBuffer(errorMessage, libpq_gettext("invalid connection option \"%s\"\n"), keyword);
        return NULL;
    }

    if (uri_decode) {
        value_copy = conninfo_uri_decode(value, errorMessage);
        if (value_copy == NULL)
            /* conninfo_uri_decode already set an error message */
            return NULL;
    } else {
        value_copy = strdup(value);

        if (value_copy == NULL) {
            printfPQExpBuffer(errorMessage, libpq_gettext("out of memory\n"));
            return NULL;
        }
    }

    libpq_free(option->val);
    option->val = value_copy;
    option->valsize = strlen(option->val);
    return option;
}

/*
 * Find a PQconninfoOption option corresponding to the keyword in the
 * connOptions array.
 *
 * If successful, returns a pointer to the corresponding PQconninfoOption
 * structure.
 * If not successful, returns NULL.
 */
static PQconninfoOption* conninfo_find(PQconninfoOption* connOptions, const char* keyword)
{
    PQconninfoOption* option = NULL;

    for (option = connOptions; option->keyword != NULL; option++) {
        if (strcmp(option->keyword, keyword) == 0) {
            if ((option->val != NULL) && (option->valsize != strlen(option->val))) {
                Assert(false);
                return NULL;
            }

            return option;
        }
    }

    return NULL;
}

void PQconninfoFree(PQconninfoOption* connOptions)
{
    PQconninfoOption* option = NULL;

    if (connOptions == NULL)
        return;

    for (option = connOptions; option->keyword != NULL; option++) {
        if (strcmp(option->keyword, "password") == 0 && option->val != NULL) {
            /* clear password */
            int passWordLength = strlen(option->val);
            errno_t rc = memset_s(option->val, passWordLength, 0, passWordLength);
            securec_check_c(rc, "\0", "\0");
        }
        libpq_free(option->val);
        option->valsize = 0;
    }
    libpq_free(connOptions);
}

/* =========== accessor functions for PGconn ========= */
char* PQdb(const PGconn* conn)
{
    if (conn == NULL)
        return NULL;
    return conn->dbName;
}

char* PQuser(const PGconn* conn)
{
    if (conn == NULL)
        return NULL;
    return conn->pguser;
}

char* PQpass(const PGconn* conn)
{
    char       *password = NULL;

    if (conn == NULL)
        return NULL;
    if (conn->connhost != NULL)
        password = conn->connhost[conn->whichhost].password;
    if (password == NULL)
        password = conn->pgpass;
    return password;
}

char* PQhost(const PGconn* conn)
{
    if (conn == NULL)
        return NULL;
    if (conn->connhost != NULL) {
        /*
         * Return the verbatim host value provided by user, or hostaddr in its
         * lack.
         */
        if (conn->connhost[conn->whichhost].host != NULL &&
            conn->connhost[conn->whichhost].host[0] != '\0')
            return conn->connhost[conn->whichhost].host;
        else if (conn->connhost[conn->whichhost].hostaddr != NULL &&
                 conn->connhost[conn->whichhost].hostaddr[0] != '\0')
            return conn->connhost[conn->whichhost].hostaddr;
    }
    return NULL;
}

char* PQport(const PGconn* conn)
{
    if (conn == NULL)
        return NULL;
    if (conn->connhost != NULL)
        return conn->connhost[conn->whichhost].port;
    return conn->pgport;
}

char* PQtty(const PGconn* conn)
{
    if (conn == NULL)
        return NULL;
    return conn->pgtty;
}

char* PQoptions(const PGconn* conn)
{
    if (conn == NULL)
        return NULL;
    return conn->pgoptions;
}

ConnStatusType PQstatus(const PGconn* conn)
{
    if (conn == NULL)
        return CONNECTION_BAD;
    return conn->status;
}

PGTransactionStatusType PQtransactionStatus(const PGconn* conn)
{
    if (conn == NULL || conn->status != CONNECTION_OK)
        return PQTRANS_UNKNOWN;
    if (conn->asyncStatus != PGASYNC_IDLE)
        return PQTRANS_ACTIVE;
    return conn->xactStatus;
}

const char* PQparameterStatus(const PGconn* conn, const char* paramName)
{
    const pgParameterStatus* pstatus = NULL;

    if (conn == NULL || paramName == NULL)
        return NULL;
    for (pstatus = conn->pstatus; pstatus != NULL; pstatus = pstatus->next) {
        if (strcmp(pstatus->name, paramName) == 0)
            return pstatus->value;
    }
    return NULL;
}

int PQprotocolVersion(const PGconn* conn)
{
    if (conn == NULL)
        return 0;
    if (conn->status == CONNECTION_BAD)
        return 0;
    return PG_PROTOCOL_MAJOR(conn->pversion);
}

int PQserverVersion(const PGconn* conn)
{
    if (conn == NULL)
        return 0;
    if (conn->status == CONNECTION_BAD)
        return 0;
    return conn->sversion;
}

char* PQerrorMessage(const PGconn* conn)
{
    if (conn == NULL)
        return libpq_gettext("connection pointer is NULL\n");

    return conn->errorMessage.data;
}

int PQsocket(const PGconn* conn)
{
    if (conn == NULL)
        return -1;
    return conn->sock;
}

/*
 * Get the local sockaddr from conn
 */
struct sockaddr* PQLocalSockaddr(const PGconn* conn)
{
    if (conn == NULL)
        return NULL;

    return (struct sockaddr*)&(conn->laddr);
}

/*
 * Get the remote sockaddr from conn

 */
struct sockaddr* PQRemoteSockaddr(const PGconn* conn)
{
    if (conn == NULL)
        return NULL;

    return (struct sockaddr*)&(conn->raddr);
}

int PQbackendPID(const PGconn* conn)
{
    if (conn == NULL || conn->status != CONNECTION_OK)
        return 0;
    return conn->be_pid;
}

int PQconnectionNeedsPassword(const PGconn* conn)
{
    char*   password = NULL;

    if (conn == NULL)
        return false;

    password = PQpass(conn);
    if (conn->password_needed && (password == NULL || password[0] == '\0'))
        return true;
    else
        return false;
}

int PQconnectionUsedPassword(const PGconn* conn)
{
    if (conn == NULL)
        return false;
    if (conn->password_needed)
        return true;
    else
        return false;
}

int PQclientEncoding(const PGconn* conn)
{
    if (conn == NULL || conn->status != CONNECTION_OK)
        return -1;
    return conn->client_encoding;
}

int PQsetClientEncoding(PGconn* conn, const char* encoding)
{
    char qbuf[128];
    static const char query[] = "set client_encoding to '%s'";
    PGresult* res = NULL;
    int status;

    if (conn == NULL || conn->status != CONNECTION_OK)
        return -1;

    if (encoding == NULL)
        return -1;

    /* Resolve special "auto" value from the locale */
    if (strcmp(encoding, "auto") == 0)
        encoding = pg_encoding_to_char(pg_get_encoding_from_locale(NULL, true));

    /* check query buffer overflow */
    if (sizeof(qbuf) < (sizeof(query) + strlen(encoding)))
        return -1;

    /* ok, now send a query */
    check_sprintf_s(sprintf_s(qbuf, sizeof(qbuf), query, encoding));
    res = PQexec(conn, qbuf);

    if (res == NULL)
        return -1;
    if (res->resultStatus != PGRES_COMMAND_OK)
        status = -1;
    else {
        /*
         * In protocol 2 we have to assume the setting will stick, and adjust
         * our state immediately.  In protocol 3 and up we can rely on the
         * backend to report the parameter value, and we'll change state at
         * that time.
         */
        if (PG_PROTOCOL_MAJOR(conn->pversion) < 3)
            pqSaveParameterStatus(conn, "client_encoding", encoding);
        status = 0; /* everything is ok */
    }
    PQclear(res);
    return status;
}

/*
 * Add function for set build receive timeout at any time.
 */
int PQsetRwTimeout(PGconn* conn, int build_receive_timeout)
{
    char recv_timeout[64] = {'\0'};
    int nRet = 0;

    if (conn == NULL)
        return -1;

    nRet = snprintf_s(recv_timeout, sizeof(recv_timeout), sizeof(recv_timeout) - 1, "%d", build_receive_timeout);
    securec_check_ss_c(nRet, "\0", "\0");

    if (conn->rw_timeout != NULL)
        free(conn->rw_timeout);

    conn->rw_timeout = strdup(recv_timeout);
    return 0;
}

PGVerbosity PQsetErrorVerbosity(PGconn* conn, PGVerbosity verbosity)
{
    PGVerbosity old;

    if (conn == NULL)
        return PQERRORS_DEFAULT;
    old = conn->verbosity;
    conn->verbosity = verbosity;
    return old;
}

void PQtrace(PGconn* conn, FILE* debug_port)
{
    if (conn == NULL)
        return;
    PQuntrace(conn);
    conn->Pfdebug = debug_port;
}

void PQuntrace(PGconn* conn)
{
    if (conn == NULL)
        return;
    if (conn->Pfdebug != NULL) {
        (void)fflush(conn->Pfdebug);
        conn->Pfdebug = NULL;
    }
}

PQnoticeReceiver PQsetNoticeReceiver(PGconn* conn, PQnoticeReceiver proc, void* arg)
{
    PQnoticeReceiver old = NULL;

    if (conn == NULL)
        return NULL;

    old = conn->noticeHooks.noticeRec;
    if (proc != NULL) {
        conn->noticeHooks.noticeRec = proc;
        conn->noticeHooks.noticeRecArg = arg;
    }
    return old;
}

PQnoticeProcessor PQsetNoticeProcessor(PGconn* conn, PQnoticeProcessor proc, void* arg)
{
    PQnoticeProcessor old = NULL;

    if (conn == NULL)
        return NULL;

    old = conn->noticeHooks.noticeProc;
    if (proc != NULL) {
        conn->noticeHooks.noticeProc = proc;
        conn->noticeHooks.noticeProcArg = arg;
    }
    return old;
}

/*
 * The default notice message receiver just gets the standard notice text
 * and sends it to the notice processor.  This two-level setup exists
 * mostly for backwards compatibility; perhaps we should deprecate use of
 * PQsetNoticeProcessor?
 */
static void defaultNoticeReceiver(void* arg, const PGresult* res)
{
    (void)arg; /* not used */
    if (res->noticeHooks.noticeProc != NULL)
        (*res->noticeHooks.noticeProc)(res->noticeHooks.noticeProcArg, PQresultErrorMessage(res));
}

/*
 * The default notice message processor just prints the
 * message on stderr.  Applications can override this if they
 * want the messages to go elsewhere (a window, for example).
 * Note that simply discarding notices is probably a bad idea.
 */
static void defaultNoticeProcessor(void* arg, const char* message)
{
    (void)arg; /* not used */
    /* Note: we expect the supplied string to end with a newline already. */
    fprintf(stderr, "%s", message);
}
#ifdef SUPPORT_PGPASSFILE
/*
 * returns a pointer to the next token or NULL if the current
 * token doesn't match
 */
static char* pwdfMatchesString(char* buf, char* token)
{
    char* tbuf = NULL;
    char* ttok = NULL;
    bool bslash = false;

    if (buf == NULL || token == NULL)
        return NULL;
    tbuf = buf;
    ttok = token;
    if (tbuf[0] == '*' && tbuf[1] == ':')
        return tbuf + 2;
    while (*tbuf != 0) {
        if (*tbuf == '\\' && !bslash) {
            tbuf++;
            bslash = true;
        }
        if (*tbuf == ':' && *ttok == 0 && !bslash)
            return tbuf + 1;
        bslash = false;
        if (*ttok == 0)
            return NULL;
        if (*tbuf == *ttok) {
            tbuf++;
            ttok++;
        } else
            return NULL;
    }
    return NULL;
}

/* Get a password from the password file. Return value is malloc'd. */
static char* PasswordFromFile(char* hostname, char* port, char* dbname, char* username)
{
    FILE* fp = NULL;
    char pgpassfile[MAXPGPATH];
    struct stat stat_buf;

#define LINELEN NAMEDATALEN * 5
    char buf[LINELEN];

    if (dbname == NULL || strlen(dbname) == 0)
        return NULL;

    if (username == NULL || strlen(username) == 0)
        return NULL;

    /* 'localhost' matches pghost of '' or the default socket directory */
    if (hostname == NULL)
        hostname = DefaultHost;
    else if (is_absolute_path(hostname))

        /*
         * We should probably use canonicalize_path(), but then we have to
         * bring path.c into libpq, and it doesn't seem worth it.
         */
        if (strcmp(hostname, DEFAULT_PGSOCKET_DIR) == 0)
            hostname = DefaultHost;

    if (port == NULL)
        port = DEF_PGPORT_STR;

    if (!getPgPassFilename(pgpassfile))
        return NULL;

    /* If password file cannot be opened, ignore it. */
    if (stat(pgpassfile, &stat_buf) != 0)
        return NULL;

#ifndef WIN32
    if (!S_ISREG(stat_buf.st_mode)) {
        fprintf(stderr, libpq_gettext("WARNING: password file \"%s\" is not a plain file\n"), pgpassfile);
        return NULL;
    }

    /* If password file is insecure, alert the user and ignore it. */
    if (stat_buf.st_mode & (S_IRWXG | S_IRWXO)) {
        fprintf(stderr,
            libpq_gettext(
                "WARNING: password file \"%s\" has group or world access; permissions should be u=rw (0600) or less\n"),
            pgpassfile);
        return NULL;
    }
#else

        /*
         * On Win32, the directory is protected, so we don't have to check the
         * file.
         */
#endif

    fp = fopen(pgpassfile, "r");
    if (fp == NULL)
        return NULL;

    while (!feof(fp) && !ferror(fp)) {
        char *t = buf, *ret = NULL, *p1 = NULL, *p2 = NULL;
        int len;

        if (fgets(buf, sizeof(buf), fp) == NULL)
            break;

        len = strlen(buf);
        if (len == 0)
            continue;

        /* Remove trailing newline */
        if (buf[len - 1] == '\n')
            buf[len - 1] = 0;

        if ((t = pwdfMatchesString(t, hostname)) == NULL || (t = pwdfMatchesString(t, port)) == NULL ||
            (t = pwdfMatchesString(t, dbname)) == NULL || (t = pwdfMatchesString(t, username)) == NULL)
            continue;
        ret = strdup(t);
        fclose(fp);

        /* De-escape password. */
        for (p1 = p2 = ret; *p1 != ':' && *p1 != '\0'; ++p1, ++p2) {
            if (*p1 == '\\' && p1[1] != '\0')
                ++p1;
            *p2 = *p1;
        }
        *p2 = '\0';

        /* buf and t point to the same address. */
        erase_arr(buf);
        return ret;
    }

    fclose(fp);
    return NULL;

#undef LINELEN
}

static bool getPgPassFilename(char* pgpassfile)
{
    char* passfile_env = gs_getenv_r("PGPASSFILE");

    if (check_client_env(passfile_env) != NULL)
        /* use the literal path from the environment, if set */
        check_strncpy_s(strncpy_s(pgpassfile, MAXPGPATH, passfile_env, strlen(passfile_env)));
    else {
        char homedir[MAXPGPATH];

        if (!pqGetHomeDirectory(homedir, sizeof(homedir)))
            return false;
        check_sprintf_s(sprintf_s(pgpassfile, MAXPGPATH, "%s/%s", homedir, PGPASSFILE));
    }
    return true;
}

/*
 *	If the connection failed, we should mention if
 *	we got the password from .pgpass in case that
 *	password is wrong.
 */
static void dot_pg_pass_warning(PGconn* conn)
{
    /* If it was 'invalid authorization', add .pgpass mention */
    /* only works with >= 9.0 servers */
    if (conn->dot_pgpass_used && conn->password_needed && conn->result != NULL &&
        strcmp(PQresultErrorField(conn->result, PG_DIAG_SQLSTATE), ERRCODE_INVALID_PASSWORD) == 0) {
        char pgpassfile[MAXPGPATH];

        if (!getPgPassFilename(pgpassfile))
            return;
        appendPQExpBuffer(&conn->errorMessage, libpq_gettext("password retrieved from file \"%s\"\n"), pgpassfile);
    }
}
#endif

/*
 * Obtain user's home directory, return in given buffer
 *
 * On Unix, this actually returns the user's home directory.  On Windows
 * it returns the openGauss-specific application data folder.
 *
 * This is essentially the same as get_home_path(), but we don't use that
 * because we don't want to pull path.c into libpq (it pollutes application
 * namespace)
 */
bool pqGetHomeDirectory(char* buf, int bufsize)
{
#ifndef WIN32
    char pwdbuf[BUFSIZ];
    struct passwd pwdstr;
    struct passwd* pwd = NULL;

    // the getpwuid be used in pqGetpwuid is not thread safe
    //
    (void)syscalllockAcquire(&getpwuid_lock);

    if (pqGetpwuid(geteuid(), &pwdstr, pwdbuf, sizeof(pwdbuf), &pwd) != 0) {
        (void)syscalllockRelease(&getpwuid_lock);
        return false;
    }
    check_strncpy_s(strncpy_s(buf, bufsize, pwd->pw_dir, bufsize - 1));
    (void)syscalllockRelease(&getpwuid_lock);

    check_memset_s(memset_s(pwdbuf, BUFSIZ, 0, BUFSIZ));
    pwd = NULL;

    return true;
#else
    char tmppath[MAX_PATH];

    ZeroMemory(tmppath, sizeof(tmppath));
    if (SHGetFolderPath(NULL, CSIDL_APPDATA, NULL, 0, tmppath) != S_OK)
        return false;
    check_sprintf_s(sprintf_s(buf, bufsize, "%s/postgresql", tmppath));
    return true;
#endif
}

/*
 * To keep the API consistent, the locking stubs are always provided, even
 * if they are not required.
 */

static void default_threadlock(int acquire)
{
#ifdef ENABLE_THREAD_SAFETY
#ifndef WIN32
    static pthread_mutex_t singlethread_lock = PTHREAD_MUTEX_INITIALIZER;
#else
    static pthread_mutex_t singlethread_lock = NULL;
    static long mutex_initlock = 0;

    if (singlethread_lock == NULL) {
        while (InterlockedExchange(&mutex_initlock, 1) == 1)
            /* loop, another thread own the lock */;
        if (singlethread_lock == NULL) {
            if (pthread_mutex_init(&singlethread_lock, NULL))
                PGTHREAD_ERROR("failed to initialize mutex");
        }
        InterlockedExchange(&mutex_initlock, 0);
    }
#endif
    if (acquire) {
        if (pthread_mutex_lock(&singlethread_lock))
            PGTHREAD_ERROR("failed to lock mutex");
    } else {
        if (pthread_mutex_unlock(&singlethread_lock))
            PGTHREAD_ERROR("failed to unlock mutex");
    }
#endif
}

pgthreadlock_t PQregisterThreadLock(pgthreadlock_t newhandler)
{
    pgthreadlock_t prev = pg_g_threadlock;

    if (newhandler != NULL)
        pg_g_threadlock = newhandler;
    else
        pg_g_threadlock = default_threadlock;

    return prev;
}

/*
 * set current node info to libpq stat ptr
 * ptr is available only when track_activities guc is open
 * do not set if ptr is inavailable
 */
static void set_libpq_stat_info(Oid nodeid, int count)
{
    if (libpq_wait_nodeid != NULL) {
        *libpq_wait_nodeid = nodeid;
    }

    if (libpq_wait_nodecount != NULL) {
        *libpq_wait_nodecount = count;
    }
}

/*
 *        pqDropConnection
 *
 * Close any physical connection to the server, and reset associated
 * state inside the connection object.  We don't release state that
 * would be needed to reconnect, though.
 *
 *  We can always flush the output buffer, since there's no longer any hope
 * of sending that data.  However, unprocessed input data might still be
 * valuable, so the caller must tell us whether to flush that or not.
 */
static void pqDropConnection(PGconn* conn, bool flushInput)
{
    conn->nonblocking = false;

    /* Close the socket itself */
    if (conn->sock != PGINVALID_SOCKET && (!conn->is_logic_conn)) {
        pqsecure_close(conn);
        closesocket(conn->sock);
    }
    conn->sock = PGINVALID_SOCKET;

    /* Optionally discard any unread data */
    if (flushInput) {
        conn->inStart = conn->inCursor = conn->inEnd = 0;
    }

    /* Always discard any unsent data */
    conn->outCount = 0;

    /* Free authentication state */
#ifdef ENABLE_GSS
    {
        OM_uint32    min_s;

        if (conn->gctx != NULL)
            gss_delete_sec_context(&min_s, &conn->gctx, GSS_C_NO_BUFFER);
        if (conn->gtarg_nam != NULL)
            gss_release_name(&min_s, &conn->gtarg_nam);
        if (conn->ginbuf.length)
            gss_release_buffer(&min_s, &conn->ginbuf);
        if (conn->goutbuf.length)
            gss_release_buffer(&min_s, &conn->goutbuf);
    }
#endif
#ifdef ENABLE_SSPI
    if (conn->ginbuf.length)
        libpq_free(conn->ginbuf.value);
    conn->ginbuf.length = 0;
    conn->ginbuf.value = NULL;
    libpq_free(conn->sspitarget);
    conn->sspitarget = NULL;
    if (conn->sspicred != NULL) {
        FreeCredentialsHandle(conn->sspicred);
        libpq_free(conn->sspicred);
    }
    if (conn->sspictx != NULL) {
        DeleteSecurityContext(conn->sspictx);
        libpq_free(conn->sspictx);
    }
    conn->usesspi = 0;
#endif
}

/*
 *        pqDropServerData
 *
 * Clear all connection state data that was received from (or deduced about)
 * the server.  This is essential to do between connection attempts to
 * different servers, else we may incorrectly hold over some data from the
 * old server.
 *
 * It would be better to merge this into pqDropConnection, perhaps, but
 * right now we cannot because that function is called immediately on
 * detection of connection loss (cf. pqReadData, for instance).  This data
 * should be kept until we are actually starting a new connection.
 */
static void pqDropServerData(PGconn *conn)
{
    PGnotify   *notify;
    pgParameterStatus *pstatus;

    /* Forget pending notifies */
    notify = conn->notifyHead;
    while (notify != NULL) {
        PGnotify   *prev = notify;

        notify = notify->next;
        libpq_free(prev);
    }
    conn->notifyHead = conn->notifyTail = NULL;

    /* Reset ParameterStatus data, as well as variables deduced from it */
    pstatus = conn->pstatus;
    while (pstatus != NULL) {
        pgParameterStatus *prev = pstatus;

        pstatus = pstatus->next;
        libpq_free(prev);
    }
    conn->pstatus = NULL;
    conn->client_encoding = PG_SQL_ASCII;
    conn->std_strings = false;
    conn->setenv_state = SETENV_STATE_IDLE;
    conn->next_eo = NULL;
    conn->default_transaction_read_only = PG_BOOL_UNKNOWN;
    conn->in_hot_standby = PG_BOOL_UNKNOWN;
    conn->sversion = 0;

    /* Drop large-object lookup data */
    if (conn->lobjfuncs)
        libpq_free(conn->lobjfuncs);

    /* Reset assorted other per-connection state */
    conn->last_sqlstate[0] = '\0';
    conn->auth_req_received = false;
    conn->password_needed = false;
    conn->be_pid = 0;
    conn->be_key = 0;
}

/*
 * Count the number of elements in a simple comma-separated list.
 */
static int count_comma_separated_elems(const char *input)
{
    int            n;

    n = 1;
    for (; *input != '\0'; input++) {
        if (*input == ',') {
            n++;
        }

    }

    return n;
}

/*
 * Parse a simple comma-separated list.
 *
 * On each call, returns a malloc'd copy of the next element, and sets *more
 * to indicate whether there are any more elements in the list after this,
 * and updates *startptr to point to the next element, if any.
 *
 * On out of memory, returns NULL.
 */
static char *parse_comma_separated_list(char **startptr, bool *more)
{
    char       *p;
    char       *s = *startptr;
    char       *e;
    int        len;

    /*
     * Search for the end of the current element; a comma or end-of-string
     * acts as a terminator.
     */
    e = s;
    while (*e != '\0' && *e != ',') {
        ++e;
    }

    *more = (*e == ',');

    len = e - s;
    p = (char *) malloc(sizeof(char) * (len + 1));
    if (p) {
        check_memcpy_s(memcpy_s(p, len + 1, s, len));
        p[len] = '\0';
    }
    *startptr = e + 1;

    return p;
}

static bool mutiHostlOptions(PGconn* conn)
{
    int            i = 0;
    /*
     * Allocate memory for details about each host to which we might possibly
     * try to connect.  For that, count the number of elements in the hostaddr
     * or host options.  If neither is given, assume one host.
     */
    conn->whichhost = 0;
    if (conn->pghostaddr && conn->pghostaddr[0] != '\0') {
        conn->nconnhost = count_comma_separated_elems(conn->pghostaddr);
    } else if (conn->pghost && conn->pghost[0] != '\0') {
        conn->nconnhost = count_comma_separated_elems(conn->pghost);
    } else {
        conn->nconnhost = 1;
    }

#ifdef ENABLE_MULTIPLE_NODES
    if (conn->nconnhost > 1) {
        appendPQExpBuffer(&conn->errorMessage,
                          libpq_gettext("do not support multiple hosts\n"));
        return false;

    }
#endif

    conn->connhost = (pg_conn_host *)
        calloc(conn->nconnhost, sizeof(pg_conn_host));
    if (conn->connhost == NULL) {
        goto oom_error;
    }

    /*
     * We now have one pg_conn_host structure per possible host.  Fill in the
     * host and hostaddr fields for each, by splitting the parameter strings.
     */
    if (conn->pghostaddr != NULL && conn->pghostaddr[0] != '\0') {
        char       *s = conn->pghostaddr;
        bool        more = true;

        for (i = 0; i < conn->nconnhost && more; i++) {
            conn->connhost[i].hostaddr = parse_comma_separated_list(&s, &more);
            if (conn->connhost[i].hostaddr == NULL)
                goto oom_error;
        }

        /*
         * If hostaddr was given, the array was allocated according to the
         * number of elements in the hostaddr list, so it really should be the
         * right size.
         */
        Assert(!more);
        Assert(i == conn->nconnhost);
    }

    if (conn->pghost != NULL && conn->pghost[0] != '\0') {
        char       *s = conn->pghost;
        bool        more = true;

        for (i = 0; i < conn->nconnhost && more; i++) {
            conn->connhost[i].host = parse_comma_separated_list(&s, &more);
            if (conn->connhost[i].host == NULL)
                goto oom_error;
        }

        /* Check for wrong number of host items. */
        if (more || i != conn->nconnhost) {
            conn->status = CONNECTION_BAD;
            appendPQExpBuffer(&conn->errorMessage,
                              libpq_gettext("could not match %d host names to %d hostaddr values\n"),
                              count_comma_separated_elems(conn->pghost), conn->nconnhost);
            return false;
        }
    }

    /*
     * Now, for each host slot, identify the type of address spec, and fill in
     * the default address if nothing was given.
     */
    for (i = 0; i < conn->nconnhost; i++) {
        pg_conn_host *ch = &conn->connhost[i];

        if (ch->hostaddr != NULL && ch->hostaddr[0] != '\0')
            ch->type = CHT_HOST_ADDRESS;
        else if (ch->host != NULL && ch->host[0] != '\0') {
            ch->type = CHT_HOST_NAME;
#ifdef HAVE_UNIX_SOCKETS
            if (is_absolute_path(ch->host))
                ch->type = CHT_UNIX_SOCKET;
#endif
        } else {
            if (ch->host)
                libpq_free(ch->host);
#ifdef HAVE_UNIX_SOCKETS
            if (DEFAULT_PGSOCKET_DIR[0]) {
                ch->host = strdup(DEFAULT_PGSOCKET_DIR);
                ch->type = CHT_UNIX_SOCKET;
            }  else
#endif
            {
                ch->host = strdup(DefaultHost);
                ch->type = CHT_HOST_NAME;
            }
            if (ch->host == NULL)
                goto oom_error;
        }
    }
    /*
     * Next, work out the port number corresponding to each host name.
     *
     * Note: unlike the above for host names, this could leave the port fields
     * as null or empty strings.  We will substitute DEF_PGPORT whenever we
     * read such a port field.
     */
    if (conn->pgport != NULL && conn->pgport[0] != '\0') {
        char       *s = conn->pgport;
        bool        more = true;

        for (i = 0; i < conn->nconnhost && more; i++) {
            conn->connhost[i].port = parse_comma_separated_list(&s, &more);
            if (conn->connhost[i].port == NULL)
                goto oom_error;
        }

        /*
         * If exactly one port was given, use it for every host.  Otherwise,
         * there must be exactly as many ports as there were hosts.
         */
        if (i == 1 && !more) {
            for (i = 1; i < conn->nconnhost; i++) {
                conn->connhost[i].port = strdup(conn->connhost[0].port);
                if (conn->connhost[i].port == NULL)
                    goto oom_error;
            }
        } else if (more || i != conn->nconnhost) {
            conn->status = CONNECTION_BAD;
            appendPQExpBuffer(&conn->errorMessage,
                              libpq_gettext("could not match %d port numbers to %d hosts\n"),
                              count_comma_separated_elems(conn->pgport), conn->nconnhost);
            return false;
        }
    }

    /*
     * If user name was not given, fetch it.  (Most likely, the fetch will
     * fail, since the only way we get here is if pg_fe_getauthname() failed
     * during conninfo_add_defaults().  But now we want an error message.)
     */
    if (conn->pguser == NULL || conn->pguser[0] == '\0') {
        if (conn->pguser)
            libpq_free(conn->pguser);
        conn->pguser = pg_fe_getauthname(&conn->errorMessage);
        if (!conn->pguser) {
            conn->status = CONNECTION_BAD;
            return false;
        }
    }

    return true;

oom_error:
    conn->status = CONNECTION_BAD;
    printfPQExpBuffer(&conn->errorMessage, libpq_gettext("out of memory \n"));
    return false;

}

/*
 * sendTerminateConn
 *     - Send a terminate message to backend.
 */
static void sendTerminateConn(PGconn *conn)
{
    /*
     * Note that the protocol doesn't allow us to send Terminate messages
     * during the startup phase.
     */
    if (conn->sock != PGINVALID_SOCKET &&
        conn->status == CONNECTION_OK &&
        (!conn->is_logic_conn)) {
        /*
         * Try to send "close connection" message to backend. Ignore any
         * error.
         */
        pqPutMsgStart('X', false, conn);
        pqPutMsgEnd(conn);
        (void) pqFlush(conn);
    }
}

/*
 * Parse and try to interpret "value" as an integer value, and if successful,
 * store it in *result, complaining if there is any trailing garbage or an
 * overflow.  This allows any number of leading and trailing whitespaces.
 */
static bool parse_int_param(const char *value, int *result, PGconn *conn,
                            const char *context)
{
    char       *end;
    long        numval;

    Assert(value != NULL);

    *result = 0;

    /* strtol(3) skips leading whitespaces */
    errno = 0;
    numval = strtol(value, &end, 10);  /* size of(unsigned int port) = 10 */

    /*
     * If no progress was done during the parsing or an error happened, fail.
     * This tests properly for overflows of the result.
     */
    if (value == end || errno != 0 || numval != (int)numval) {
        goto error;
    }

    /*
     * Skip any trailing whitespace; if anything but whitespace remains before
     * the terminating character, fail
     */
    while (*end != '\0' && isspace((unsigned char) *end)) {
        end++;
    }

    if (*end != '\0') {
        goto error;
    }

    *result = numval;
    return true;

error:
    appendPQExpBuffer(&conn->errorMessage,
                      libpq_gettext("invalid integer value \"%s\" for connection option \"%s\"\n"),
                      value, context);
    return false;
}

/*
 * release_conn_addrinfo
 *     - Free any addrinfo list in the PGconn.
 */
static void release_conn_addrinfo(PGconn *conn)
{
    if (conn->addrlist) {
        pg_freeaddrinfo_all(conn->addrlist_family, conn->addrlist);
        conn->addrlist = NULL;
        conn->addr_cur = NULL;    /* for safety */
    }
}

/* Time to advance to next connhost[] entry? */
static bool resolve_host_addr(PGconn *conn)
{
    pg_conn_host *ch;
    struct addrinfo hint;
    int    thisport = 0;
    int    ret = 0;
    char   portstr[MAXPGPATH];
    int    nRet = 0;
    errno_t error_ret = 0;

    if (conn->whichhost >= conn->nconnhost) {
        return false;
    }

    /* Drop any address info for previous host */
    release_conn_addrinfo(conn);

    ch = &conn->connhost[conn->whichhost];

    /* Initialize hint structure */
    nRet = memset_s(&hint, sizeof(hint), 0, sizeof(hint));
    securec_check_c(nRet, "\0", "\0");
    hint.ai_socktype = SOCK_STREAM;
    conn->addrlist_family = hint.ai_family = AF_UNSPEC;

    /* Figure out the port number we're going to use. */
    if (ch->port == NULL || ch->port[0] == '\0') {
        thisport = DEF_PGPORT;
    } else {
        if (!parse_int_param(ch->port, &thisport, conn, "port"))
            return false;

        if (thisport < 1 || thisport > 65535) {
            appendPQExpBuffer(&conn->errorMessage,
                              libpq_gettext("invalid port number: \"%s\"\n"),
                              ch->port);
            return false;
        }
    }
    error_ret = snprintf_s(portstr, sizeof(portstr), sizeof(portstr) - 1, "%d", thisport);
    securec_check_ss_c(error_ret, "", "");

    /* Use pg_getaddrinfo_all() to resolve the address */
    switch (ch->type) {
        case CHT_HOST_NAME:
            ret = pg_getaddrinfo_all(ch->host, portstr, &hint,
                                     &conn->addrlist);
            if (ret || !conn->addrlist) {
                appendPQExpBuffer(&conn->errorMessage,
                                  libpq_gettext("could not translate host name \"%s\" to address: %s\n"),
                                  ch->host, gai_strerror(ret));
                return false;
            }
            break;

        case CHT_HOST_ADDRESS:
            hint.ai_flags = AI_NUMERICHOST;
            ret = pg_getaddrinfo_all(ch->hostaddr, portstr, &hint,
                                     &conn->addrlist);
            if (ret || !conn->addrlist) {
                appendPQExpBuffer(&conn->errorMessage,
                                  libpq_gettext("could not translate host name \"%s\" to address: %s\n"),
                                  ch->hostaddr, gai_strerror(ret));
                return false;
            }
            break;

        case CHT_UNIX_SOCKET:
#ifdef HAVE_UNIX_SOCKETS
                conn->addrlist_family = hint.ai_family = AF_UNIX;
                if (!conn->fencedUdfRPCMode)
                    UNIXSOCK_PATH(portstr, thisport, ch->host);
                else
                    UNIXSOCK_FENCED_MASTER_PATH(portstr, ch->host);

                if (strlen(portstr) >= UNIXSOCK_PATH_BUFLEN) {
                    appendPQExpBuffer(&conn->errorMessage,
                                      libpq_gettext("Unix-domain socket path \"%s\" is too long (maximum %d bytes)\n"),
                                      portstr,
                                      (int) (UNIXSOCK_PATH_BUFLEN - 1));
                    return false;
                }

                /*
                 * NULL hostname tells pg_getaddrinfo_all to parse the service
                 * name as a Unix-domain socket path.
                 */
                ret = pg_getaddrinfo_all(NULL, portstr, &hint,
                                         &conn->addrlist);
                if (ret || !conn->addrlist) {
                    appendPQExpBuffer(&conn->errorMessage,
                                      libpq_gettext("could not translate Unix-domain socket path \"%s\" to address: %s\n"),
                                      portstr, gai_strerror(ret));
                    return false;
                }
#else
                Assert(false);
#endif
                break;
        }

        /* OK, scan this addrlist for a working server address */
        conn->addr_cur = conn->addrlist;
        return true;
}


    /* Reset connection state machine? */
static void reset_connection_state_machine(PGconn *conn)
{
    /*
    * (Re) initialize our connection control variables for a set of
    * connection attempts to a single server address.  These variables
    * must persist across individual connection attempts, but we must
    * reset them when we start to consider a new server.
    */
    conn->pversion = PG_PROTOCOL(3, 51);
    conn->send_appname = true;

#ifdef USE_SSL
    /* setup values based on SSL mode */
   conn->allow_ssl_try = (conn->sslmode[0] != 'd');    /* "disable" */
   conn->wait_ssl_try = (conn->sslmode[0] == 'a'); /* "allow" */
#endif

        /* Reset conn->status to put the state machine in the right state */
        conn->status = CONNECTION_NEEDED;

}

    /* Force a new connection (perhaps to the same server as before)? */
static void reset_physical_connection(PGconn *conn)
{
        /* Drop any existing connection */
        pqDropConnection(conn, true);

        /* Reset all state obtained from old server */
        pqDropServerData(conn);

        /* Drop any PGresult we might have, too */
        conn->asyncStatus = PGASYNC_IDLE;
        conn->xactStatus = PQTRANS_IDLE;
        pqClearAsyncResult(conn);

}

/* Time to advance to next host */
static void try_next_host(PGconn *conn)
{
    if (conn->whichhost + 1 < conn->nconnhost) {
        conn->whichhost++;
    } else {
        /*
        * Oops, no more hosts.
        *
        * If we are trying to connect in "prefer-standby" mode, then drop
        * the standby requirement and start over.
        */
        if (conn->target_server_type == SERVER_TYPE_PREFER_STANDBY &&
            conn->nconnhost > 0) {
            conn->target_server_type = SERVER_TYPE_PREFER_STANDBY_PASS2;
            conn->whichhost = 0;
        } else {
            conn->whichhost++;
            return;
        }
    }

    for (; conn->whichhost < conn->nconnhost; conn->whichhost++) {
        reset_physical_connection(conn);

        reset_connection_state_machine(conn);

        if (resolve_host_addr(conn)) {
            break;
        }
    }
}

/*
 * This subroutine saves conn->errorMessage, which will be restored back by
 * restoreErrorMessage subroutine.
 */
static bool saveErrorMessage(PGconn *conn, PQExpBuffer savedMessage)
{
    initPQExpBuffer(savedMessage);
    if (PQExpBufferBroken(savedMessage)) {
        printfPQExpBuffer(&conn->errorMessage,
                          libpq_gettext("out of memory\n"));
        return false;
    }

    appendPQExpBufferStr(savedMessage,
                         conn->errorMessage.data);
    resetPQExpBuffer(&conn->errorMessage);
    return true;
}

/*
 * Restores saved error messages back to conn->errorMessage.
 */
static void restoreErrorMessage(PGconn *conn, PQExpBuffer savedMessage)
{
    appendPQExpBufferStr(savedMessage, conn->errorMessage.data);
    resetPQExpBuffer(&conn->errorMessage);
    appendPQExpBufferStr(&conn->errorMessage, savedMessage->data);
    termPQExpBuffer(savedMessage);
}

static PostgresPollingStatusType connection_check_target(PGconn* conn)
{
    PQExpBufferData savedMessage;

    if (conn->target_server_type == SERVER_TYPE_READ_WRITE ||
        conn->target_server_type == SERVER_TYPE_READ_ONLY) {
        bool        read_only_server;

        /*
         * We are yet to make a connection. Save all existing error
         * messages until we make a successful connection state.
         * This is important because PQsendQuery is going to reset
         * conn->errorMessage and we will loose error messages
         * related to previous hosts we have tried to connect and
         * failed.
         */
        if (!saveErrorMessage(conn, &savedMessage))
            goto omm_return;

        /*
         * If the server didn't report
         * "default_transaction_read_only" or "in_hot_standby" at
         * startup, we must determine its state by sending the
         * query "SHOW transaction_read_only".  This GUC exists in
         * all server versions that support 3.0 protocol.
         */
        if (conn->default_transaction_read_only == PG_BOOL_UNKNOWN ||
            conn->in_hot_standby == PG_BOOL_UNKNOWN) {
            conn->status = CONNECTION_OK;
            if (!PQsendQuery(conn,
                             "SHOW transaction_read_only"))
                goto error_return;
            /* We'll return to this state when we have the answer */
            conn->status = CONNECTION_CHECK_WRITABLE;
            restoreErrorMessage(conn, &savedMessage);
            return PGRES_POLLING_READING;
        }

        /* OK, we can make the test */
        read_only_server =
            (conn->default_transaction_read_only == PG_BOOL_YES ||
             conn->in_hot_standby == PG_BOOL_YES);

        if ((conn->target_server_type == SERVER_TYPE_READ_WRITE) ?
            read_only_server : !read_only_server) {
            /* Wrong server state, reject and try the next host */
            if (conn->target_server_type == SERVER_TYPE_READ_WRITE)
                appendPQExpBufferStr(&conn->errorMessage,
                                     libpq_gettext("session is read-only\n"));
            else
                appendPQExpBufferStr(&conn->errorMessage,
                                     libpq_gettext("session is not read-only\n"));

            /* Close connection politely. */
            conn->status = CONNECTION_OK;
            sendTerminateConn(conn);

            /*
             * Try next host if any, but we don't want to consider
             * additional addresses for this host.
             */
            goto error_return;
        }
    } else if (conn->target_server_type == SERVER_TYPE_PRIMARY ||
             conn->target_server_type == SERVER_TYPE_STANDBY ||
             conn->target_server_type == SERVER_TYPE_PREFER_STANDBY) {
        if (!saveErrorMessage(conn, &savedMessage))
            goto omm_return;

        /*
         * If the server didn't report "in_hot_standby" at
         * startup, we must determine its state by sending the
         * query "SELECT pg_catalog.pg_is_in_recovery()".  Servers
         * before 9.0 don't have that function, but by the same
         * token they don't have any standby mode, so we may just
         * assume the result.
         */
        if (conn->sversion < 90000)  /* server version 9.0 = 90000 */
            conn->in_hot_standby = PG_BOOL_NO;

        if (conn->in_hot_standby == PG_BOOL_UNKNOWN) {
            conn->status = CONNECTION_OK;
            if (!PQsendQuery(conn,
                             "SELECT pg_catalog.pg_is_in_recovery()"))
                goto error_return;
            /* We'll return to this state when we have the answer */
            conn->status = CONNECTION_CHECK_STANDBY;
            restoreErrorMessage(conn, &savedMessage);
            return PGRES_POLLING_READING;
        }

        /* OK, we can make the test */
        if ((conn->target_server_type == SERVER_TYPE_PRIMARY) ?
            (conn->in_hot_standby == PG_BOOL_YES) :
            (conn->in_hot_standby == PG_BOOL_NO)) {
            /* Wrong server state, reject and try the next host */
            if (conn->target_server_type == SERVER_TYPE_PRIMARY)
                appendPQExpBufferStr(&conn->errorMessage,
                                     libpq_gettext("server is in hot standby mode\n"));
            else
                appendPQExpBufferStr(&conn->errorMessage,
                                     libpq_gettext("server is not in hot standby mode\n"));

            /* Close connection politely. */
            conn->status = CONNECTION_OK;
            sendTerminateConn(conn);

            /*
             * Try next host if any, but we don't want to consider
             * additional addresses for this host.
             */
            goto error_return;
        }
    }

    /* We can release the address list now. */
    release_conn_addrinfo(conn);

    /* We are open for business! */
    conn->status = CONNECTION_OK;
    if (conn->target_server_type != SERVER_TYPE_ANY
        && conn->target_server_type != SERVER_TYPE_PREFER_STANDBY_PASS2) {
        restoreErrorMessage(conn, &savedMessage);
    }

    return PGRES_POLLING_OK;

/* Unreachable */

error_return:

    restoreErrorMessage(conn, &savedMessage);

omm_return:
    /*
     * We used to close the socket at this point, but that makes it awkward
     * for those above us if they wish to remove this socket from their own
     * records (an fd_set for example).  We'll just have this socket closed
     * when PQfinish is called (which is compulsory even after an error, since
     * the connection structure must be freed).
     */
    conn->status = CONNECTION_BAD;
    return PGRES_POLLING_FAILED;
}

static PostgresPollingStatusType connection_consume(PGconn* conn)
{
    PGresult   *res;
    PQExpBufferData savedMessage;

    if (!saveErrorMessage(conn, &savedMessage))
        goto omm_return;

    /*
     * This state just makes sure the connection is idle after
     * we've obtained the result of a SHOW or SELECT query.  Once
     * we're clear, return to CONNECTION_CHECK_TARGET state to
     * decide what to do next.  We must transiently set status =
     * CONNECTION_OK in order to use the result-consuming
     * subroutines.
     */
    conn->status = CONNECTION_OK;
    if (!PQconsumeInput(conn))
        goto error_return;

    if (PQisBusy(conn)) {
        conn->status = CONNECTION_CONSUME;
        restoreErrorMessage(conn, &savedMessage);
        return PGRES_POLLING_READING;
    }

    /* Call PQgetResult() again until we get a NULL result */
    res = PQgetResult(conn);
    if (res != NULL) {
        PQclear(res);
        conn->status = CONNECTION_CONSUME;
        restoreErrorMessage(conn, &savedMessage);
        return PGRES_POLLING_READING;
    }

    conn->status = CONNECTION_CHECK_TARGET;
    restoreErrorMessage(conn, &savedMessage);
    return PGRES_POLLING_ACTIVE;

/* Unreachable */

error_return:

    restoreErrorMessage(conn, &savedMessage);

omm_return:

    /*
     * We used to close the socket at this point, but that makes it awkward
     * for those above us if they wish to remove this socket from their own
     * records (an fd_set for example).  We'll just have this socket closed
     * when PQfinish is called (which is compulsory even after an error, since
     * the connection structure must be freed).
     */
    conn->status = CONNECTION_BAD;
    return PGRES_POLLING_FAILED;
}

static PostgresPollingStatusType connection_check_writable(PGconn* conn)
{
    PGresult   *res;
    PQExpBufferData savedMessage;

    /*
     * We are yet to make a connection. Save all existing error
     * messages until we make a successful connection state.
     * This is important because PQsendQuery is going to reset
     * conn->errorMessage and we will loose error messages
     * related to previous hosts we have tried to connect and
     * failed.
     */
    if (!saveErrorMessage(conn, &savedMessage))
        goto omm_return;

    /*
     * Waiting for result of "SHOW transaction_read_only".  We
     * must transiently set status = CONNECTION_OK in order to use
     * the result-consuming subroutines.
     */
    conn->status = CONNECTION_OK;
    if (!PQconsumeInput(conn))
        goto error_return;

    if (PQisBusy(conn)) {
        conn->status = CONNECTION_CHECK_WRITABLE;
        restoreErrorMessage(conn, &savedMessage);
        return PGRES_POLLING_READING;
    }

    res = PQgetResult(conn);
    if (res && PQresultStatus(res) == PGRES_TUPLES_OK &&
        PQntuples(res) == 1) {
        char       *val = PQgetvalue(res, 0, 0);

        /*
         * "transaction_read_only = on" proves that at least one
         * of default_transaction_read_only and in_hot_standby is
         * on, but we don't actually know which.  We don't care
         * though for the purpose of identifying a read-only
         * session, so satisfy the CONNECTION_CHECK_TARGET code by
         * claiming they are both on.  On the other hand, if it's
         * a read-write session, they are certainly both off.
         */
        if (strncmp(val, "on", 2) == 0) {  /* sizeof("on") = 2 */
            conn->default_transaction_read_only = PG_BOOL_YES;
            conn->in_hot_standby = PG_BOOL_YES;
        } else {
            conn->default_transaction_read_only = PG_BOOL_NO;
            conn->in_hot_standby = PG_BOOL_NO;
        }
        PQclear(res);

        /* Finish reading messages before continuing */
        conn->status = CONNECTION_CONSUME;
        restoreErrorMessage(conn, &savedMessage);
        return PGRES_POLLING_ACTIVE;
    }

    /* Something went wrong with "SHOW transaction_read_only". */
    if (res)
        PQclear(res);

    /* Append error report to conn->errorMessage. */
    appendPQExpBuffer(&conn->errorMessage,
                      libpq_gettext("\"%s\" failed\n"),
                      "SHOW transaction_read_only");

    /* Close connection politely. */
    conn->status = CONNECTION_OK;
    sendTerminateConn(conn);


/* Unreachable */

error_return:

    restoreErrorMessage(conn, &savedMessage);

omm_return:

    /*
     * We used to close the socket at this point, but that makes it awkward
     * for those above us if they wish to remove this socket from their own
     * records (an fd_set for example).  We'll just have this socket closed
     * when PQfinish is called (which is compulsory even after an error, since
     * the connection structure must be freed).
     */
    conn->status = CONNECTION_BAD;
    return PGRES_POLLING_FAILED;
}

static PostgresPollingStatusType connection_check_standby(PGconn* conn)
{
    PGresult   *res;
    PQExpBufferData savedMessage;

    if (!saveErrorMessage(conn, &savedMessage))
        goto omm_return;


    /*
     * Waiting for result of "SELECT pg_is_in_recovery()".  We
     * must transiently set status = CONNECTION_OK in order to use
     * the result-consuming subroutines.
     */
    conn->status = CONNECTION_OK;
    if (!PQconsumeInput(conn))
        goto error_return;

    if (PQisBusy(conn)) {
        conn->status = CONNECTION_CHECK_STANDBY;
        restoreErrorMessage(conn, &savedMessage);
        return PGRES_POLLING_READING;
    }

    res = PQgetResult(conn);
    if (res && PQresultStatus(res) == PGRES_TUPLES_OK &&
        PQntuples(res) == 1) {
        char       *val = PQgetvalue(res, 0, 0);

        if (strncmp(val, "t", 1) == 0)
            conn->in_hot_standby = PG_BOOL_YES;
        else
            conn->in_hot_standby = PG_BOOL_NO;
        PQclear(res);

        /* Finish reading messages before continuing */
        conn->status = CONNECTION_CONSUME;
        restoreErrorMessage(conn, &savedMessage);
        return PGRES_POLLING_ACTIVE;
    }

    /* Something went wrong with "SELECT pg_is_in_recovery()". */
    if (res)
        PQclear(res);

    /* Append error report to conn->errorMessage. */
    appendPQExpBuffer(&conn->errorMessage,
                      libpq_gettext("\"%s\" failed\n"),
                      "SELECT pg_is_in_recovery()");

    /* Close connection politely. */
    conn->status = CONNECTION_OK;
    sendTerminateConn(conn);


/* Unreachable */

error_return:

    restoreErrorMessage(conn, &savedMessage);

omm_return:
    /*
     * We used to close the socket at this point, but that makes it awkward
     * for those above us if they wish to remove this socket from their own
     * records (an fd_set for example).  We'll just have this socket closed
     * when PQfinish is called (which is compulsory even after an error, since
     * the connection structure must be freed).
     */
    conn->status = CONNECTION_BAD;
    return PGRES_POLLING_FAILED;
}

#ifdef ENABLE_UT
void uttest_parseServiceInfo(PQconninfoOption* options, PQExpBuffer errorMessage)
{
    parseServiceInfo(options, errorMessage);
}

void uttest_conninfo_uri_parse(const char* uri, PQExpBuffer errorMessage, bool use_defaults)
{
    (void)conninfo_uri_parse(uri, errorMessage, use_defaults);
    return;
}

void uttest_conninfo_uri_parse_options(PQconninfoOption* options, const char* uri, PQExpBuffer errorMessage)
{
    (void)conninfo_uri_parse_options(options, uri, errorMessage);
    return;
}

int uttest_parseServiceFile(const char* serviceFile, const char* service, PQconninfoOption* options,
    PQExpBuffer errorMessage, bool* group_found)
{
    return parseServiceFile(serviceFile, service, options, errorMessage, group_found);
}
#endif
