/*
 * psql - the openGauss interactive terminal
 *
 * Copyright (c) 2000-2012, PostgreSQL Global Development Group
 *
 * src/bin/psql/startup.c
 */
#include "settings.h"
#include "postgres_fe.h"

#include <sys/types.h>
#include <sys/time.h>

#ifndef WIN32
#include <unistd.h>
#else /* WIN32 */
#include <io.h>
#include <win32.h>
#endif /* WIN32 */

#include "getopt_long.h"

#include <locale.h>

#include "command.h"
#include "common.h"
#include "describe.h"
#include "help.h"
#include "input.h"
#include "mainloop.h"

/* Database Security: Data importing/dumping support AES128. */
#include <time.h>
#include "pgtime.h"
#include "mb/pg_wchar.h"

#ifndef WIN32
#include "libpq/libpq-int.h"
#endif
#include "nodes/pg_list.h"

/*
 * Global psql options
 */
PsqlSettings pset;
/* Used for change child process name in gsql parallel execute mode. */
char* argv_para;
int argv_num;
static bool is_pipeline = false;
static bool is_interactive = true;
#ifndef ENABLE_MULTIPLE_NODES
static const char *g_queryNodeState = "select local_role, db_state from pg_stat_get_stream_replications();";
static const char *g_expectedLocalRole = "Primary";
static const char *g_expectedDbState = "Normal";
#endif
/* The version of libpq */
extern const char* libpqVersionString;

#ifndef WIN32
#define SYSPSQLRC "gsqlrc"
#define PSQLRC ".gsqlrc"
#else
#define SYSPSQLRC "gsqlrc"
#define PSQLRC "gsqlrc.conf"
#endif

#ifdef ENABLE_UT
#define static
#endif

/*
 * Structures to pass information between the option parsing routine
 * and the main function
 */
enum _actions { ACT_NOTHING = 0, ACT_SINGLE_SLASH, ACT_LIST_DB, ACT_SINGLE_QUERY, ACT_FILE };

struct adhoc_opts {
    char* dbname;
    char* host;
    char* port;
    char* username;
    char* passwd;
    char* logfilename;
    enum _actions action;
    char* action_string;
    bool no_readline;
    bool no_psqlrc;
    bool single_txn;
#ifndef ENABLE_MULTIPLE_NODES
    bool multi_host;
    uint32 hostCount;
    List *hostList;
#endif
};

static void parse_psql_options(int argc, char* const argv[], struct adhoc_opts* options);
static void process_psqlrc(const char* argv0);
static void process_psqlrc_file(char* filename);
static void showVersion(void);
static void EstablishVariableSpace(void);
static void get_password_pipeline(struct adhoc_opts* options);

#if defined(USE_ASSERT_CHECKING) || defined(FASTCHECK)
bool check_parseonly_parameter(adhoc_opts options)
{
    if (pset.parseonly) {
        if (options.action != ACT_FILE) {
            fprintf(stderr, "%s: %s", pset.progname, "-f and -g argument must be set together\n");
            exit(EXIT_USER);
        } else {
            return true;
        }
    }
    return false;
}
#endif

/* Database Security: Data importing/dumping support AES128. */
static void set_aes_key(const char* dencrypt_key);

#ifdef HAVE_CE
#define PARAMS_ARRAY_SIZE 12
#else
#define PARAMS_ARRAY_SIZE 11
#endif

#ifdef ENABLE_LITE_MODE
void pg_free(void* ptr)
{
    if (ptr != NULL) {
        free(ptr);
        ptr = NULL;
    }
}

static void list_free_private(List* list, bool deep)
{
    ListCell* cell = NULL;

    cell = list_head(list);
    while (cell != NULL) {
        ListCell* tmp = cell;

        cell = lnext(cell);
        if (deep) {
            pg_free(lfirst(tmp));
        }
        pg_free(tmp);
    }

    if (list != NULL) {
        pg_free(list);
    }
}


void list_free(List* list)
{
    list_free_private(list, false);
}

static List* new_list(NodeTag type)
{
    List* new_list_val = NULL;
    ListCell* new_head = NULL;

    new_head = (ListCell*)pg_malloc(sizeof(*new_head));
    new_head->next = NULL;
    /* new_head->data is left undefined! */

    new_list_val = (List*)pg_malloc(sizeof(*new_list_val));
    new_list_val->type = type;
    new_list_val->length = 1;
    new_list_val->head = new_head;
    new_list_val->tail = new_head;

    return new_list_val;
}

static void new_tail_cell(List* list)
{
    ListCell* new_tail = NULL;

    new_tail = (ListCell*)pg_malloc(sizeof(*new_tail));
    new_tail->next = NULL;

    list->tail->next = new_tail;
    list->tail = new_tail;
    list->length++;
}

List* lappend(List* list, void* datum)
{
    if (list == NIL)
        list = new_list(T_List);
    else
        new_tail_cell(list);

    lfirst(list->tail) = datum;
    return list;
}

static void new_head_cell(List* list)
{
    ListCell* new_head = NULL;

    new_head = (ListCell*)pg_malloc(sizeof(*new_head));
    new_head->next = list->head;

    list->head = new_head;
    list->length++;
}

List* lcons(void* datum, List* list)
{
    if (list == NIL) {
        list = new_list(T_List);
    } else {
        new_head_cell(list);
    }

    lfirst(list->head) = datum;
    return list;
}

List* list_delete_cell(List* list, ListCell* cell, ListCell* prev)
{
    Assert(prev != NULL ? lnext(prev) == cell : list_head(list) == cell);

    /*
     * If we're about to delete the last node from the list, free the whole
     * list instead and return NIL, which is the only valid representation of
     * a zero-length list.
     */
    if (list->length == 1) {
        list_free(list);
        return NIL;
    }

    /*
     * Otherwise, adjust the necessary list links, deallocate the particular
     * node we have just removed, and return the list we were given.
     */
    list->length--;

    if (prev != NULL) {
        prev->next = cell->next;
    } else {
        list->head = cell->next;
    }

    if (list->tail == cell) {
        list->tail = prev;
    }

    cell->next = NULL;
    pg_free(cell);
    return list;
}

List* list_delete_first(List* list)
{
    if (list == NIL) {
        return NIL; /* would an error be better? */
    }

    return list_delete_cell(list, list_head(list), NULL);
}
#endif

#ifndef ENABLE_MULTIPLE_NODES
static bool IsPrimaryOfCentralizedCluster(PGconn *conn)
{
    PGresult* res = PQexec(conn, g_queryNodeState);
    if (res == NULL) {
        return false;
    }
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        PQclear(res);
        return false;
    }
    if (PQntuples(res) <= 0) {
        PQclear(res);
        return false;
    }
    if (PQgetisnull(res, 0, 0)) {
        PQclear(res);
        return false;
    }
    char *localRole = pg_strdup(PQgetvalue(res, 0, 0));
    if (localRole == NULL) {
        PQclear(res);
        return false;
    }
    if (strcmp(localRole, g_expectedLocalRole) != 0) {
        free(localRole);
        PQclear(res);
        return false;
    }
    free(localRole);
    if (PQgetisnull(res, 0, 1)) {
        PQclear(res);
        return false;
    }
    char *dbState = pg_strdup(PQgetvalue(res, 0, 1));
    if (dbState == NULL) {
        PQclear(res);
        return false;
    }
    if (strcmp(dbState, g_expectedDbState) != 0) {
        free(dbState);
        PQclear(res);
        return false;
    }
    free(dbState);
    PQclear(res);
    return true;
}
PGconn* PQconnectdbMultiHostParams(const char** keywords,
                                   char** values,
                                   struct adhoc_opts *option,
                                   char **password,
                                   const char *password_prompt)
{
    PGconn* conn = NULL;
    bool pass = false;
    ConnStatusType status = CONNECTION_BAD;
    char *hostname = NULL;
    List *hostList = NULL;
    bool isParseOnly = false;
    while (option->hostCount > 0) {
        hostList = option->hostList;
        hostname = (char *)linitial(hostList);
        values[0] = hostname;
        do {
            conn = PQconnectdbParams(keywords, values, (int)true);
            pass = false;
            status = CONNECTION_BAD;
            if (conn == NULL || conn->sock < 0) {
                fprintf(stderr,
                    "failed to connect %s:%s.\n",
                    ((conn->pghost == NULL) ? "Unknown" : conn->pghost),
                    ((conn->pgport == NULL) ? "Unknown" : conn->pgport));
                break;
            }
            status = PQstatus(conn);
            if (status == CONNECTION_BAD
                && (strstr(PQerrorMessage(conn), "password") != NULL)
                && (*password == NULL)
                && (pset.getPassword != TRI_NO)) {
                PQfinish(conn);
                *password = simple_prompt(password_prompt, MAX_PASSWORD_LENGTH, false);
                pass = true;
                values[3] = *password;
            }
        } while (pass);
        if ((status == CONNECTION_OK) && IsPrimaryOfCentralizedCluster(conn)) {
            fprintf(stderr, "Connect primary node %s\n", hostname);
            break;
        }
        if (hostname != NULL) {
            free(hostname);
        }
        if (option->hostCount > 1) {
            option->hostList = list_delete_first(hostList);
            option->hostCount--;
            PQfinish(conn);
        } else {
#if defined(USE_ASSERT_CHECKING) || defined(FASTCHECK)
            isParseOnly = check_parseonly_parameter(*option);
#endif
            option->hostCount--;
            if (status == CONNECTION_BAD && !isParseOnly) {
                fprintf(stderr, "%s: %s", pset.progname, PQerrorMessage(conn));
                PQfinish(conn);
                exit(EXIT_BADCONN);
            }
            PQfinish(conn);
            conn = NULL;
        }
    }
    return conn;
}
#endif

/*
 *
 * main
 *
 */
int main(int argc, char* argv[])
{
    struct adhoc_opts options;
    int successResult;
    char* password = NULL;
    char* password_prompt = NULL;
    bool new_pass = false;
    errno_t rc;
    bool isparseonly = false;
    bool has_action = false;

    /* Database Security: Data importing/dumping support AES128. */
    struct timeval aes_start_time;
    struct timeval aes_end_time;
    pg_time_t total_time = 0;

    set_pglocale_pgservice(argv[0], PG_TEXTDOMAIN("gsql"));

    if (strcmp(libpqVersionString, DEF_GS_VERSION) != 0) {
        fprintf(stderr,
            "[Warning]: The \"libpq.so\" loaded mismatch the version of gsql, "
            "please check it.\n"
            "expected: %s\nresult: %s\n",
            DEF_GS_VERSION,
            libpqVersionString);
#ifdef ENABLE_MULTIPLE_NODES
        exit(EXIT_FAILURE);
#endif
    }

    if (argc > 1) {
        if (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-?") == 0) {
            usage();
            exit(EXIT_SUCCESS);
        }
        if (strcmp(argv[1], "--version") == 0 || strcmp(argv[1], "-V") == 0) {
            showVersion();
            exit(EXIT_SUCCESS);
        }
    }

#ifdef WIN32
    setvbuf(stderr, NULL, _IONBF, 0);
#endif

    ignore_quit_signal();
    setup_cancel_handler();

    /* Server will strictly check the name message of client tool, it can't be changed. */
    pset.progname = "gsql";

    pset.db = NULL;
    setDecimalLocale();
    pset.encoding = PQenv2encoding();
    pset.queryFout = stdout;
    pset.queryFoutPipe = false;
    pset.cur_cmd_source = stdin;
    pset.cur_cmd_interactive = false;
#if defined(USE_ASSERT_CHECKING) || defined(FASTCHECK)
    pset.parseonly = false;
#endif
    /* We rely on unmentioned fields of pset.popt to start out 0/false/NULL */
    pset.popt.topt.format = PRINT_ALIGNED;
    pset.popt.topt.feedback = true;
    pset.popt.topt.border = 1;
    pset.popt.topt.pager = 1;
    pset.popt.topt.start_table = true;
    pset.popt.topt.stop_table = true;
    pset.popt.topt.default_footer = true;
    pset.popt.topt.fieldSep.separator = NULL;
    pset.popt.topt.tableAttr = NULL;
    /* Database Security: Data importing/dumping support AES128. */
    initDecryptInfo(&pset.decryptInfo);

    /* maintance mode is off on default */
    pset.maintance = false;
    /* client encryption is off on default */
    pset.enable_client_encryption = false;
    /* We must get COLUMNS here before readline() sets it */
    char* columnsEnvStr = GetEnvStr("COLUMNS");
    pset.popt.topt.env_columns = columnsEnvStr != NULL ? atoi(columnsEnvStr) : 0;
    if (columnsEnvStr != NULL) {
        free(columnsEnvStr);
        columnsEnvStr = NULL;
    }

    pset.notty = (!isatty(fileno(stdin)) || !isatty(fileno(stdout)));

    pset.getPassword = TRI_DEFAULT;

    // Init retry variables.
    //
    pset.retry_times = 0;
    rc = memset_s(pset.retry_sqlstate, sizeof(pset.retry_sqlstate), 0, sizeof(pset.retry_sqlstate));
    securec_check_c(rc, "\0", "\0");
    pset.retry_on = false;
    pset.retry_sleep = false;
    pset.max_retry_times = 0;

    EstablishVariableSpace();

    if (!SetVariable(pset.vars, "VERSION", PG_VERSION_STR)) {
        psql_error("set variable %s failed.\n", "VERSION");
    }

    /* Default values for variables */
    SetVariableBool(pset.vars, "AUTOCOMMIT");
    if (!SetVariable(pset.vars, "VERBOSITY", "default")) {
        psql_error("set variable %s failed.\n", "VERBOSITY");
    }
    if (!SetVariable(pset.vars, "PROMPT1", DEFAULT_PROMPT1)) {
        psql_error("set variable %s failed.\n", "PROMPT1");
    }
    if (!SetVariable(pset.vars, "PROMPT2", DEFAULT_PROMPT2)) {
        psql_error("set variable %s failed.\n", "PROMPT2");
    }
    if (!SetVariable(pset.vars, "PROMPT3", DEFAULT_PROMPT3)) {
        psql_error("set variable %s failed.\n", "PROMPT3");
    }

    /* init options.action_string */
    options.action_string = NULL;
    parse_psql_options(argc, argv, &options);

    if (is_pipeline) {
        get_password_pipeline(&options);
    }

    /* Save the argv and argc for change process name. */
    argv_para = argv[0];
    argv_num = argc;

    if ((pset.popt.topt.fieldSep.separator == NULL) && !pset.popt.topt.fieldSep.separator_zero) {
        pset.popt.topt.fieldSep.separator = pg_strdup(DEFAULT_FIELD_SEP);
        pset.popt.topt.fieldSep.separator_zero = false;
    }
    if ((pset.popt.topt.recordSep.separator == NULL) && !pset.popt.topt.recordSep.separator_zero) {
        pset.popt.topt.recordSep.separator = pg_strdup(DEFAULT_RECORD_SEP);
        pset.popt.topt.recordSep.separator_zero = false;
    }

    if (options.username == NULL)
        password_prompt = pg_strdup(_("Password: "));
    else {
        errno_t err = EOK;
        size_t len = strlen(_("Password for user %s: ")) - 2 + strlen(options.username) + 1;
        password_prompt = (char*)malloc(len);
        if (password_prompt == NULL)
            exit(EXIT_FAILURE);
        err = sprintf_s(password_prompt, len, _("Password for user %s: "), options.username);
        check_sprintf_s(err);
    }

    password = options.passwd;
    if (pset.getPassword == TRI_YES && password == NULL)
        password = simple_prompt(password_prompt, MAX_PASSWORD_LENGTH, false);

    /* loop until we have a password if requested by backend */
    do {
        const char** keywords = (const char**)pg_malloc(PARAMS_ARRAY_SIZE * sizeof(*keywords));
        char** values = (char**)pg_malloc(PARAMS_ARRAY_SIZE * sizeof(*values));
        bool* values_free = (bool*)pg_malloc(PARAMS_ARRAY_SIZE * sizeof(bool));
        char* tmpenv = GetEnvStr("PGCLIENTENCODING");

        rc = memset_s(values_free, PARAMS_ARRAY_SIZE * sizeof(bool), 0, PARAMS_ARRAY_SIZE * sizeof(bool));
        securec_check_c(rc, "\0", "\0");
        char* encodetext = NULL;

        keywords[0] = "host";
        values[0] = options.host;
        keywords[1] = "port";
        values[1] = options.port;
        keywords[2] = "user";
        values[2] = options.username;
        keywords[3] = "password";
        values[3] = password;
        keywords[4] = "dbname";
        values[4] = (char*)((options.action == ACT_LIST_DB && options.dbname == NULL) ? "postgres" : options.dbname);
        keywords[5] = "application_name";
        values[5] = (char*)(pset.progname);
        keywords[6] = "fallback_application_name";
        values[6] = (char*)(pset.progname);
        keywords[7] = "client_encoding";
        values[7] = (char*)((pset.notty || (tmpenv != NULL)) ? NULL : "auto");
        keywords[8] = "connect_timeout";
        values[8] = CONNECT_TIMEOUT;
#ifdef HAVE_CE
        keywords[9] = "enable_ce";
        values[9] = (pset.enable_client_encryption) ? (char*)"1" : NULL;
#endif
        if (pset.maintance) {
            keywords[PARAMS_ARRAY_SIZE - 2] = "options";
            values[PARAMS_ARRAY_SIZE - 2] = (char *)("-c xc_maintenance_mode=on");
        } else {
            keywords[PARAMS_ARRAY_SIZE - 2] = NULL;
            values[PARAMS_ARRAY_SIZE - 2] = NULL;
        }

        if (tmpenv != NULL)
            free(tmpenv);
        tmpenv = NULL;
        keywords[PARAMS_ARRAY_SIZE - 1] = NULL;
        values[PARAMS_ARRAY_SIZE - 1] = NULL;
        new_pass = false;
#ifdef ENABLE_MULTIPLE_NODES
        pset.db = PQconnectdbParams(keywords, values, (int)true);
#else
        if (options.multi_host && options.hostCount > 1) {
            pset.db = PQconnectdbMultiHostParams(keywords, values, &options, &password, password_prompt);
            if (pset.db == NULL) {
                fprintf(stderr, "failed to connect %s:%s.\n", options.host, options.port);
                exit(EXIT_BADCONN);
            }
        } else {
            pset.db = PQconnectdbParams(keywords, values, (int)true);
        }
#endif

        if (pset.db->sock < 0) {
            fprintf(stderr,
                "failed to connect %s:%s.\n",
                pset.db->pghost == NULL ? "Unknown" : pset.db->pghost,
                pset.db->pgport == NULL ? "Unknown" : pset.db->pgport);
            PQfinish(pset.db);
            exit(EXIT_BADCONN);
        }

        /* Encode password and stored in memory here for gsql parallel execute function. */
        if ((password != NULL) && strlen(password) != 0) {
            encodetext = SEC_encodeBase64((char*)password, strlen(password));
            if (encodetext == NULL) {
                fprintf(stderr, "%s: encode the parallel connect value failed.", pset.progname);
                PQfinish(pset.db);
                exit(EXIT_BADCONN);
            }
            values[3] = encodetext;
        }

        /* Stored connection and guc info for new connections in gsql parallel mode. */
        values_free[3] = true;
        if (options.dbname != NULL) {
            /* When we use a new dbname or exit the program, we need to free the value. */
            values_free[4] = true; /* The dbname index is 4 */
        }

        pset.connInfo.keywords = keywords;
        pset.connInfo.values = values;
        pset.connInfo.values_free = values_free;
        pset.num_guc_stmt = 0;
        pset.guc_stmt = NULL;

        /* Clear password related memory to avoid leaks when core. */
        if (password != NULL) {
            rc = memset_s(password, strlen(password), 0, strlen(password));
            securec_check_c(rc, "\0", "\0");
        }

        /* Whenever occur connect error of password, we will try ask for user input again. */
        if (PQstatus(pset.db) == CONNECTION_BAD && (strstr(PQerrorMessage(pset.db), "password") != NULL) &&
            password == NULL && pset.getPassword != TRI_NO) {
            PQfinish(pset.db);
            password = simple_prompt(password_prompt, MAX_PASSWORD_LENGTH, false);
            new_pass = true;
            free(keywords);
            keywords = NULL;
            free(values);
            values = NULL;
            free(values_free);
            values_free = NULL;
        }
    }
#ifdef ENABLE_MULTIPLE_NODES
    while (new_pass);
#else
    while (new_pass && options.multi_host == false);
#endif
    if (options.passwd == NULL) {
        if (password == pset.connInfo.values[3]) {
            pset.connInfo.values_free[3] = false;
            pset.connInfo.values[3] = NULL;
        }
        free(password);
        password = NULL;
    }
    free(password_prompt);

    /* Clear password related memory to avoid leaks when core. */
    if (options.passwd != NULL) {
        rc = memset_s(options.passwd, strlen(options.passwd), 0, strlen(options.passwd));
        securec_check_c(rc, "\0", "\0");
        free(options.passwd);
        options.passwd = NULL;
    }
#ifndef WIN32
    if (pset.db->pgpass != NULL) {
        rc = memset_s(pset.db->pgpass, strlen(pset.db->pgpass), 0, strlen(pset.db->pgpass));
        securec_check_c(rc, "\0", "\0");
    }
#endif
#if defined(USE_ASSERT_CHECKING) || defined(FASTCHECK)
    isparseonly = check_parseonly_parameter(options);
#endif
    if (PQstatus(pset.db) == CONNECTION_BAD && !isparseonly) {
        fprintf(stderr, "%s: %s", pset.progname, PQerrorMessage(pset.db));
        PQfinish(pset.db);
        exit(EXIT_BADCONN);
    }

    PQsetNoticeProcessor(pset.db, NoticeProcessor, NULL);

    SyncVariables();

    if (options.action == ACT_LIST_DB && !isparseonly) {
        int success;

        if (!options.no_psqlrc) {
            process_psqlrc(argv[0]);
        }
        success = listAllDbs((int)false);
        PQfinish(pset.db);
        exit(success ? EXIT_SUCCESS : EXIT_FAILURE);
    }

    if (options.logfilename != NULL) {
        canonicalize_path(options.logfilename);
        pset.logfile = fopen(options.logfilename, "a");
        if (pset.logfile == NULL)
            fprintf(stderr,
                _("%s: could not open log file \"%s\": %s\n"),
                pset.progname,
                options.logfilename,
                strerror(errno));
        else {
            int logfd = fileno(pset.logfile);
            /* change the privilege of log file for security. */
            if ((logfd >= 0) && (-1 == fchmod(logfd, S_IRUSR | S_IWUSR)))
                fprintf(stderr, _("could not set permissions of file  \"%s\"\n"), options.logfilename);
            logfd = -1;
        }
    }

    /* show warning message when the client and server have diffrent version numbers */
    if (!isparseonly) {
        /* show warning message when the client and server have diffrent version numbers */
        (void)client_server_version_check(pset.db);
    } else {
        /*
            we could not get the encoding from the server so let's get the encoding from the environment variable
        */
        if (pset.encoding == -1) {
            char *encodingStr = GetEnvStr("PGCLIENTENCODING");
            if (encodingStr) {
                check_env_value(encodingStr);
                pset.encoding = pg_char_to_encoding(encodingStr);
                free(encodingStr);
                encodingStr = NULL;
            }
        }
        /*
            again we could not get the encoding so let's try the default encoding of ASCII
            All we want is to print to the screen for the debugging so why not.
        */
        if (pset.encoding == -1) {
            pset.encoding = PG_SQL_ASCII;
        }
    }
     /* Now find something to do */
     /* process file given by -f */
    if (options.action == ACT_FILE) {
        if (!options.no_psqlrc) {
            process_psqlrc(argv[0]);
        }
        /* Database Security: Data importing/dumping support AES128. */
        gettimeofday(&aes_start_time, NULL);
        successResult = process_file(options.action_string, options.single_txn, false);
        gettimeofday(&aes_end_time, NULL);
        total_time = 1000 * (aes_end_time.tv_sec - aes_start_time.tv_sec) +
                     (aes_end_time.tv_usec - aes_start_time.tv_usec) / 1000;
        if (!isparseonly)
            fprintf(stdout, "total time: %lld  ms\n", (long long int)total_time);
    }

    /*
     * process slash command if one was given to -c
     */
    else if (options.action == ACT_SINGLE_SLASH) {
        PsqlScanState scan_state = NULL;

        if (pset.echo == PSQL_ECHO_ALL)
            puts(options.action_string);

        scan_state = psql_scan_create();
        psql_scan_setup(scan_state, options.action_string, (int)strlen(options.action_string));

        successResult = HandleSlashCmds(scan_state, NULL) != PSQL_CMD_ERROR ? EXIT_SUCCESS : EXIT_FAILURE;

        psql_scan_destroy(scan_state);
    }

    /*
     * If the query given to -c was a normal one, send it
     */
    else if (options.action == ACT_SINGLE_QUERY) {
        successResult = MainLoop(NULL, options.action_string);
        rc = memset_s(options.action_string, strlen(options.action_string), 0, strlen(options.action_string));
        securec_check_c(rc, "\0", "\0");
        free(options.action_string);
        options.action_string = NULL;
    }

    /*
     * or otherwise enter interactive main loop
     */
    else {
        if (!options.no_psqlrc) {
            process_psqlrc(argv[0]);
        }

        connection_warnings(true);
        if (!pset.quiet && !pset.notty)
            printf(_("Type \"help\" for help.\n\n"));

        canAddHist = true;
        initializeInput(options.no_readline ? 0 : 1);
        successResult = MainLoop(stdin);
    }

    /* clean up */
    if (pset.logfile != NULL) {
        fclose(pset.logfile);
        pset.logfile = NULL;
    }
    PQfinish(pset.db);
    setQFout(NULL);

    /* Free all the connection and guc info used in gsql parallel execute mode. */
    free(pset.connInfo.keywords);
    pset.connInfo.keywords = NULL;

    for (int i = 0; i < PARAMS_ARRAY_SIZE; i++) {
        if (pset.connInfo.values_free[i] && NULL != pset.connInfo.values[i]) {
            if (strlen(pset.connInfo.values[i]) != 0) {
                /* Erase the connection information in the memory. */
                rc = memset_s(pset.connInfo.values[i],
                              strlen(pset.connInfo.values[i]),
                              0,
                              strlen(pset.connInfo.values[i]));
                securec_check_c(rc, "\0", "\0");
                free(pset.connInfo.values[i]);
                pset.connInfo.values[i] = NULL;
            }
        }
    }
    free(pset.connInfo.values);
    pset.connInfo.values = NULL;
    free(pset.connInfo.values_free);
    pset.connInfo.values_free = NULL;

    for (int i = 0; i < pset.num_guc_stmt; i++) {
        free(pset.guc_stmt[i]);
        pset.guc_stmt[i] = NULL;
    }

    if (pset.guc_stmt != NULL)
        free(pset.guc_stmt);

    pset.guc_stmt = NULL;
    /* Free options.action_string, because it alloced memory when options.action is ACT_FILE*/
    has_action = (options.action == ACT_FILE) && (options.action_string != NULL);
    if (has_action) {
        free(options.action_string);
        options.action_string = NULL;
    }

    /* Clean up variables for query retry. */
    pset.max_retry_times = 0;
    ResetQueryRetryController();
    EmptyRetryErrcodesList(pset.errcodes_list);

    return successResult;
}

static void get_password_pipeline(struct adhoc_opts* options)
{
    int pass_max_len = 1024;
    char* pass_buf = NULL;
    errno_t rc = EOK;

    if (isatty(fileno(stdin))) {
        fprintf(stderr, "%s: %s", pset.progname, "Terminal is not allowed to use --pipeline\n");
        exit(EXIT_USER);
    }

    if (is_interactive) {
        fprintf(stderr, "%s: %s", pset.progname, "--pipeline must be used with -c or -f\n");
        exit(EXIT_USER);
    }

    pass_buf = (char*)pg_malloc(pass_max_len);
    rc = memset_s(pass_buf, pass_max_len, 0, pass_max_len);
    securec_check_c(rc, "\0", "\0");

    if (NULL != fgets(pass_buf, pass_max_len, stdin)) {
        pset.getPassword = TRI_YES;
        pass_buf[strlen(pass_buf) - 1] = '\0';
        options->passwd = pg_strdup(pass_buf);
    }

    rc = memset_s(pass_buf, pass_max_len, 0, pass_max_len);
    securec_check_c(rc, "\0", "\0");
    free(pass_buf);
    pass_buf = NULL;
}

#ifndef ENABLE_MULTIPLE_NODES
static char* TrimHost(char* src)
{
    char* s = 0;
    char* e = 0;
    char* c = 0;

    for (c = src; (c != NULL) && (*c != '\0'); ++c) {
        if (isspace(*c)) {
            if (e == NULL) {
                e = c;
            }
        } else {
            if (s == NULL) {
                s = c;
            }
            e = 0;
        }
    }
    if (s == NULL) {
        s = src;
    }
    if (e != NULL) {
        *e = 0;
    }

    return s;
}

static void ParseHostArg(const char *arg, struct adhoc_opts *options)
{
    const char *sep = ",";
    options->multi_host = false;
    options->hostList = NULL;
    options->hostCount = 0;
    if ((arg == NULL) || (strstr(arg, sep) == NULL)) {
        return;
    }
    char *inputStr = pg_strdup(arg);
    char *host = NULL;
    for (char *subStr = strtok(inputStr, sep); subStr != NULL; subStr = strtok(NULL, sep)) {
        host = pg_strdup(TrimHost(subStr));
        if (strlen(host) == 0) {
            free(host);
            continue;
        }
        options->hostList = (options->hostList == NULL) ? list_make1(host) : lappend(options->hostList, host);
        options->hostCount++;
    }
    if (options->hostCount > 1) {
        options->multi_host = true;
    }
    free(inputStr);
}
#endif

/*
 * Parse command line options
 */
static void parse_psql_options(int argc, char* const argv[], struct adhoc_opts* options)
{
    static struct option long_options[] = {
        {"echo-all", no_argument, NULL, 'a'},
        {"no-align", no_argument, NULL, 'A'},
        {"command", required_argument, NULL, 'c'},
        {"dbname", required_argument, NULL, 'd'},
        {"echo-queries", no_argument, NULL, 'e'},
        {"echo-hidden", no_argument, NULL, 'E'},
        {"file", required_argument, NULL, 'f'},
        {"field-separator", required_argument, NULL, 'F'},
        {"field-separator-zero", no_argument, NULL, 'z'},
        {"host", required_argument, NULL, 'h'},
        {"html", no_argument, NULL, 'H'},
        {"list", no_argument, NULL, 'l'},
        {"log-file", required_argument, NULL, 'L'},
        {"maintenance", no_argument, NULL, 'm'},
        {"no-libedit", no_argument, NULL, 'n'},
        {"single-transaction", no_argument, NULL, '1'},
        {"pipeline", no_argument, NULL, '2'},
        {"output", required_argument, NULL, 'o'},
        {"port", required_argument, NULL, 'p'},
        {"pset", required_argument, NULL, 'P'},
        {"quiet", no_argument, NULL, 'q'},
        {"enable-client-encryption", no_argument, NULL, 'C'},
        {"record-separator", required_argument, NULL, 'R'},
        {"record-separator-zero", no_argument, NULL, '0'},
        {"single-step", no_argument, NULL, 's'},
        {"single-line", no_argument, NULL, 'S'},
        {"tuples-only", no_argument, NULL, 't'},
        {"table-attr", required_argument, NULL, 'T'},
        {"username", required_argument, NULL, 'U'},
        {"set", required_argument, NULL, 'v'},
        {"variable", required_argument, NULL, 'v'},
        {"version", no_argument, NULL, 'V'},
        {"password", required_argument, NULL, 'W'},
        {"expanded", no_argument, NULL, 'x'},
        {"no-gsqlrc", no_argument, NULL, 'X'},
        {"help", no_argument, NULL, '?'},
        /* Database Security: Data importing/dumping support AES128. */
        {"with-key", required_argument, NULL, 'k'},
#if defined(USE_ASSERT_CHECKING) || defined(FASTCHECK)
        {"sql-parse", no_argument, NULL, 'g'},
#endif
        {NULL, 0, NULL, 0}
    };

    int optindex;
    extern char* optarg;
    extern int optind;
    int c;
    bool is_action_file = false;
    /* Database Security: Data importing/dumping support AES128. */
    char* dencrypt_key = NULL;
    char* dbname = NULL;
    errno_t rc = EOK;
#ifdef USE_READLINE
    useReadline = false;
#endif

    rc = memset_s(options, sizeof(*options), 0, sizeof(*options));
    check_memset_s(rc);

    while ((c = getopt_long(
                argc, argv, "aAc:d:eEf:F:gh:Hlk:L:mno:p:P:qCR:rsStT:U:v:W:VxXz?012", long_options, &optindex)) != -1) {
        switch (c) {
            case 'a':
                if (!SetVariable(pset.vars, "ECHO", "all")) {
                    psql_error("set variable %s failed.\n", "ECHO");
                }
                break;
            case 'A':
                pset.popt.topt.format = PRINT_UNALIGNED;
                break;
            case 'c':
                if (optarg == NULL) {
                    break;
                }
                is_interactive = false;
                options->action_string = optarg;
                if (optarg[0] == '\\') {
                    options->action = ACT_SINGLE_SLASH;
                    options->action_string++;
                } else {
                    options->action = ACT_SINGLE_QUERY;
                    options->action_string = pg_strdup(optarg); /* need to free in main() */
                    /* clear action string after -c command when it inludes sensitive info */
                    if (SensitiveStrCheck(optarg)) {
                        rc = memset_s(optarg, strlen(optarg), 0, strlen(optarg));
                        check_memset_s(rc);
                    }
                }
                break;
            case 'd':
                dbname = optarg;
                break;
            case 'e':
                if (!SetVariable(pset.vars, "ECHO", "queries")) {
                    psql_error("set variable %s failed.\n", "ECHO");
                }
                break;
            case 'E':
                SetVariableBool(pset.vars, "ECHO_HIDDEN");
                break;
            case 'f':
                if (optarg == NULL) {
                    break;
                }
                is_interactive = false;
                is_action_file = (options->action_string != NULL) && (options->action == ACT_FILE);
                if (is_action_file)
                    free(options->action_string);
                options->action_string = pg_strdup(optarg);
                options->action = ACT_FILE;
                break;
            case 'F':
                if (pset.popt.topt.fieldSep.separator != NULL)
                    free(pset.popt.topt.fieldSep.separator);
                pset.popt.topt.fieldSep.separator = pg_strdup(optarg);
                pset.popt.topt.fieldSep.separator_zero = false;
                break;
            case 'h':
                options->host = optarg;
#ifndef ENABLE_MULTIPLE_NODES
                ParseHostArg(options->host, options);
#endif
                break;
            case 'H':
                pset.popt.topt.format = PRINT_HTML;
                break;
            case 'l':
                options->action = ACT_LIST_DB;
                break;
            /* Database Security: Data importing/dumping support AES128. */
            case 'k': {
                pset.decryptInfo.encryptInclude = true;
                if (optarg == NULL) {
                    break;
                }
                dencrypt_key = pg_strdup(optarg);
                rc = memset_s(optarg, strlen(optarg), 0, strlen(optarg));
                check_memset_s(rc);
                set_aes_key(dencrypt_key);
                free(dencrypt_key);
                break;
            }
            case 'L':
                options->logfilename = optarg;
                break;
            case 'm':
                pset.maintance = true;
                break;
            case 'n':
                options->no_readline = true;
                break;
            case 'o':
                setQFout(optarg);
                break;
            case 'p':
                options->port = optarg;
                break;
            case 'P': {
                char* value = NULL;
                char* equal_loc = NULL;
                bool result = false;

                value = pg_strdup(optarg);
                equal_loc = strchr(value, '=');
                if (equal_loc == NULL)
                    result = do_pset(value, NULL, &pset.popt, true);
                else {
                    *equal_loc = '\0';
                    result = do_pset(value, equal_loc + 1, &pset.popt, true);
                }

                if (!result) {
                    fprintf(stderr, _("%s: could not set printing parameter \"%s\"\n"), pset.progname, value);
                    exit(EXIT_FAILURE);
                }

                free(value);
                break;
            }
            case 'q':
                SetVariableBool(pset.vars, "QUIET");
                break;
            case 'C':
                pset.enable_client_encryption = true;
                break;
            case 'r':
#ifdef USE_READLINE
                useReadline = true;
#endif
                break;
            case 'R':
                if (pset.popt.topt.recordSep.separator != NULL)
                    free(pset.popt.topt.recordSep.separator);
                pset.popt.topt.recordSep.separator = pg_strdup(optarg);
                pset.popt.topt.recordSep.separator_zero = false;
                break;
            case 's':
                SetVariableBool(pset.vars, "SINGLESTEP");
                break;
            case 'S':
                SetVariableBool(pset.vars, "SINGLELINE");
                break;
            case 't':
                pset.popt.topt.tuples_only = true;
                break;
            case 'T':
                if (pset.popt.topt.tableAttr != NULL)
                    free(pset.popt.topt.tableAttr);
                pset.popt.topt.tableAttr = pg_strdup(optarg);
                break;
            case 'U':
                if (strlen(optarg) >= MAXPGPATH) {
                    fprintf(stderr, _("%s: invalid username, max username len:%d\n"), pset.progname, MAXPGPATH);
                    exit(EXIT_FAILURE);
                }
                options->username = optarg;
                break;
            case 'v': {
                char* value = NULL;
                char* equal_loc = NULL;

                value = pg_strdup(optarg);
                equal_loc = strchr(value, '=');
                if (equal_loc == NULL) {
                    if (!DeleteVariable(pset.vars, value)) {
                        fprintf(stderr, _("%s: could not delete variable \"%s\"\n"), pset.progname, value);
                        exit(EXIT_FAILURE);
                    }
                } else {
                    *equal_loc = '\0';
                    if (!SetVariable(pset.vars, value, equal_loc + 1)) {
                        fprintf(stderr, _("%s: could not set variable \"%s\"\n"), pset.progname, value);
                        exit(EXIT_FAILURE);
                    }
                    setHistSize(value, equal_loc + 1, false);
                }

                free(value);
                break;
            }
            case 'V':
                showVersion();
                exit(EXIT_SUCCESS);
            case 'W':
                pset.getPassword = TRI_YES;
                if (optarg != NULL) {
                    options->passwd = pg_strdup(optarg);
                    rc = memset_s(optarg, strlen(optarg), 0, strlen(optarg));
                    check_memset_s(rc);
                }
                break;
            case 'x':
                pset.popt.topt.expanded = (unsigned short int)true;
                break;
            case 'X':
                options->no_psqlrc = true;
                break;
            case 'z':
                pset.popt.topt.fieldSep.separator_zero = true;
                break;
            case '0':
                pset.popt.topt.recordSep.separator_zero = true;
                break;
            case '1':
                options->single_txn = true;
                break;
            case '2':
                is_pipeline = true;
                break;
#if defined(USE_ASSERT_CHECKING) || defined(FASTCHECK)
            case 'g':
                pset.parseonly = true;
                break;
#endif
            case '?':
                /* Actual help option given */
                if (strcmp(argv[optind - 1], "-?") == 0 || strcmp(argv[optind - 1], "--help") == 0) {
                    usage();
                    exit(EXIT_SUCCESS);
                }
                /* unknown option reported by getopt */
                else {
                    fprintf(stderr, _("Try \"%s --help\" for more information.\n"), pset.progname);
                    exit(EXIT_FAILURE);
                }
                break;
            default:
                fprintf(stderr, _("Try \"%s --help\" for more information.\n"), pset.progname);
                exit(EXIT_FAILURE);
                break;
        }
    }

    /*
     * if we still have arguments, use it as the database name and username
     */
    while (argc - optind >= 1) {
        if (dbname == NULL) {
            dbname = argv[optind];
        } else if (options->username == NULL) {
            options->username = argv[optind];
        } else if (!pset.quiet) {
            fprintf(
                stderr, _("%s: warning: extra command-line argument \"%s\" ignored\n"), pset.progname, argv[optind]);
        }
        optind++;
    }

    /* mask Password information stored in dbname */
    if (dbname != NULL) {
        /* Save a copy for connection before masking the password. */
        options->dbname = pg_strdup(dbname);

        /* mask informations in URI string. */
        if (strncmp(dbname, "postgresql://", strlen("postgresql://")) == 0) {
            char *off_argv = dbname + strlen("postgresql://");
            rc = memset_s(off_argv, strlen(off_argv), '*', strlen(off_argv));
            check_memset_s(rc);
        } else if (strncmp(dbname, "postgres://", strlen("postgres://")) == 0) {
            char *off_argv = dbname + strlen("postgres://");
            rc = memset_s(off_argv, strlen(off_argv), '*', strlen(off_argv));
            check_memset_s(rc);
        }
        /* mask password in key/value string. */
        char *temp = NULL;
        if ((temp = strstr(dbname, "password")) != NULL) {
            char *off_argv = temp + strlen("password");
            rc = memset_s(off_argv, strlen(off_argv), '*', strlen(off_argv));
            check_memset_s(rc);
        }
    }
}

/*
 * Load .gsqlrc file, if found.
 */
static void process_psqlrc(const char* argv0)
{
    char home[MAXPGPATH];
    char rc_file[MAXPGPATH];
    char my_exec_path[MAXPGPATH] = {'\0'};
    char etc_path[MAXPGPATH];
    char* envrc = GetEnvStr("PSQLRC");
    bool withdecrypt = false;
    errno_t rc = EOK;

    /* we don't decrypt gsqlrc file here */
    if (pset.decryptInfo.encryptInclude) {
        pset.decryptInfo.encryptInclude = false;
        withdecrypt = true;
    }

    find_my_exec(argv0, my_exec_path);
    get_etc_path(my_exec_path, etc_path, sizeof(etc_path));

    rc = sprintf_s(rc_file, MAXPGPATH, "%s/%s", etc_path, SYSPSQLRC);
    check_sprintf_s(rc);
    process_psqlrc_file(rc_file);

    if (envrc != NULL && strlen(envrc) > 0) {
        /* might need to free() this */
        char* envrc_alloc = pg_strdup(envrc);
        expand_tilde(&envrc_alloc);
        process_psqlrc_file(envrc_alloc);
        free(envrc_alloc);
        envrc_alloc = NULL;
    } else if (get_home_path(home, sizeof(home))) {
        rc = sprintf_s(rc_file, MAXPGPATH, "%s/%s", home, PSQLRC);
        check_sprintf_s(rc);
        process_psqlrc_file(rc_file);
    }

    /* open the decrypt mark for other encrypt file */
    if (withdecrypt)
        pset.decryptInfo.encryptInclude = true;

    if (envrc != NULL)
        free(envrc);
    envrc = NULL;
}

static void process_psqlrc_file(char* filename)
{
    char *psqlrc_minor = NULL;
    char *psqlrc_major = NULL;
    errno_t err = EOK;
#if defined(WIN32) && (!defined(__MINGW32__))
#define R_OK 4
#endif

    psqlrc_minor = (char*)pg_malloc(strlen(filename) + 1 + strlen(PG_VERSION) + 1);
    err = sprintf_s(psqlrc_minor, strlen(filename) + 1 + strlen(PG_VERSION) + 1, "%s-%s", filename, PG_VERSION);
    check_sprintf_s(err);
    psqlrc_major = (char*)pg_malloc(strlen(filename) + 1 + strlen(PG_MAJORVERSION) + 1);
    err =
        sprintf_s(psqlrc_major, strlen(filename) + 1 + strlen(PG_MAJORVERSION) + 1, "%s-%s", filename, PG_MAJORVERSION);
    check_sprintf_s(err);
    /* check for minor version first, then major, then no version */
    if (access(psqlrc_minor, R_OK) == 0)
        (void)process_file(psqlrc_minor, false, false);
    else if (access(psqlrc_major, R_OK) == 0)
        (void)process_file(psqlrc_major, false, false);
    else if (access(filename, R_OK) == 0)
        (void)process_file(filename, false, false);

    free(psqlrc_minor);
    free(psqlrc_major);
}

/* showVersion
 *
 * This output format is intended to match GNU standards.
 */
static void showVersion(void)
{
#ifdef PGXC
    puts("gsql " DEF_GS_VERSION);
#else
    puts("gsql " DEF_GS_VERSION);
#endif
}

/*
 * Assign hooks for psql variables.
 *
 * This isn't an amazingly good place for them, but neither is anywhere else.
 */
static void autocommit_hook(const char* newval)
{
    pset.autocommit = ParseVariableBool(newval);
}

static void on_error_stop_hook(const char* newval)
{
    pset.on_error_stop = ParseVariableBool(newval);
}

static void quiet_hook(const char* newval)
{
    pset.quiet = ParseVariableBool(newval);
}

static void singleline_hook(const char* newval)
{
    pset.singleline = ParseVariableBool(newval);
}

static void singlestep_hook(const char* newval)
{
    pset.singlestep = ParseVariableBool(newval);
}

static void fetch_count_hook(const char* newval)
{
    pset.fetch_count = ParseVariableNum(newval, -1, -1, false);
}

static void echo_hook(const char* newval)
{
    if (newval == NULL)
        pset.echo = PSQL_ECHO_NONE;
    else if (strcmp(newval, "queries") == 0)
        pset.echo = PSQL_ECHO_QUERIES;
    else if (strcmp(newval, "all") == 0)
        pset.echo = PSQL_ECHO_ALL;
    else
        pset.echo = PSQL_ECHO_NONE;
}

static void echo_hidden_hook(const char* newval)
{
    if (newval == NULL)
        pset.echo_hidden = PSQL_ECHO_HIDDEN_OFF;
    else if (strcmp(newval, "noexec") == 0)
        pset.echo_hidden = PSQL_ECHO_HIDDEN_NOEXEC;
    else if (pg_strcasecmp(newval, "off") == 0)
        pset.echo_hidden = PSQL_ECHO_HIDDEN_OFF;
    else
        pset.echo_hidden = PSQL_ECHO_HIDDEN_ON;
}

static void on_error_rollback_hook(const char* newval)
{
    if (newval == NULL)
        pset.on_error_rollback = PSQL_ERROR_ROLLBACK_OFF;
    else if (pg_strcasecmp(newval, "interactive") == 0)
        pset.on_error_rollback = PSQL_ERROR_ROLLBACK_INTERACTIVE;
    else if (pg_strcasecmp(newval, "off") == 0)
        pset.on_error_rollback = PSQL_ERROR_ROLLBACK_OFF;
    else
        pset.on_error_rollback = PSQL_ERROR_ROLLBACK_ON;
}

static void histcontrol_hook(const char* newval)
{
    if (newval == NULL)
        pset.histcontrol = hctl_none;
    else if (strcmp(newval, "ignorespace") == 0)
        pset.histcontrol = hctl_ignorespace;
    else if (strcmp(newval, "ignoredups") == 0)
        pset.histcontrol = hctl_ignoredups;
    else if (strcmp(newval, "ignoreboth") == 0)
        pset.histcontrol = hctl_ignoreboth;
    else
        pset.histcontrol = hctl_none;
}

static void prompt1_hook(const char* newval)
{
    pset.prompt1 = newval != NULL ? newval : "";
}

static void prompt2_hook(const char* newval)
{
    pset.prompt2 = newval != NULL ? newval : "";
}

static void prompt3_hook(const char* newval)
{
    pset.prompt3 = newval != NULL ? newval : "";
}

static void verbosity_hook(const char* newval)
{
    if (newval == NULL)
        pset.verbosity = PQERRORS_DEFAULT;
    else if (strcmp(newval, "default") == 0)
        pset.verbosity = PQERRORS_DEFAULT;
    else if (strcmp(newval, "terse") == 0)
        pset.verbosity = PQERRORS_TERSE;
    else if (strcmp(newval, "verbose") == 0)
        pset.verbosity = PQERRORS_VERBOSE;
    else
        pset.verbosity = PQERRORS_DEFAULT;

    (void)PQsetErrorVerbosity(pset.db, pset.verbosity);
}

//  Iterate through the elements of ErrCodes and release allocated memory.
//
void EmptyRetryErrcodesList(ErrCodes& list)
{
    for (int i = 0; i < (int)list.size(); i++) {
        if (list[i] != NULL) {
            free(list[i]);
            list[i] = NULL;
        }
    }

    // Removes all elements from the vector.
    //
    list.clear();
}

// Parse the retry errcodes config file, cache the errcodes in a vector 'pset.retry_errcodes'.
//
static bool ReadRetryErrcodesConfigFile(void)
{
    char* gausshome_dir = NULL;
    char self_path[MAXPGPATH] = {0};
    char retry_errcodes_path[MAXPGPATH] = {0};
    int nRet = 0;
    FILE* fp = NULL;
    char* line = NULL;
    size_t len = 0;
    ErrCodes list;

    gausshome_dir = GetEnvStr("GAUSSHOME");
    if (gausshome_dir == NULL) {
        int r = (int)readlink("/proc/self/exe", self_path, sizeof(self_path) - 1);
        if (r < 0 || r >= (int)(MAXPGPATH - sizeof("/retry_errcodes.conf"))) {
            psql_error("Could not get proc self path.\n");
            return false;
        } else {
            char* ptr = strrchr(self_path, '/');

            if (NULL != ptr)
                *ptr = '\0';
            nRet = sprintf_s(retry_errcodes_path, MAXPGPATH, "%s/retry_errcodes.conf", self_path);
            check_sprintf_s(nRet);
        }
    } else {
        check_env_value(gausshome_dir);
        nRet = sprintf_s(retry_errcodes_path, MAXPGPATH, "%s/bin/retry_errcodes.conf", gausshome_dir);
        check_sprintf_s(nRet);
    }

    if (gausshome_dir != NULL)
        free(gausshome_dir);
    gausshome_dir = NULL;

    canonicalize_path(retry_errcodes_path);
    fp = fopen(retry_errcodes_path, "r");
    if (fp == NULL) {
        psql_error("Could not open retry errcodes config file.\n");
        return false;
    }

    while (getline(&line, &len, fp) != -1) {
        // Just check the length of errcode.
        // Length must be 'ERRCODE_LENGTH + 1' with '\n' in the end.
        //
        if (strlen(line) != ERRCODE_LENGTH + 1) {
            // If got wrong errcode, we should empty the errcodes list first.
            //
            EmptyRetryErrcodesList(list);
            psql_error("Wrong errcodes in config file.\n");
            fclose(fp);

            free(line);

            return false;
        }

        // Add a new element at the end of the vector.
        //
        list.push_back(pg_strdup(line));
    }

    if (list.size() == 0) {
        psql_error("No errcodes list in config file.\n");
        fclose(fp);

        if (line != NULL)
            free(line);

        return false;
    } else {
        // Empty the previous errcodes list and assign a new one to the vector.
        //
        EmptyRetryErrcodesList(pset.errcodes_list);
        pset.errcodes_list = list;
    }

    fclose(fp);

    if (line != NULL)
        free(line);

    return true;
}

static void retry_hook(const char* newval)
{
    int result = 0;

    if (newval == NULL)
        return;

    if (PQtransactionStatus(pset.db) != PQTRANS_IDLE) {
        psql_error("Retry within transaction is not supported.\n");
        return;
    }

    if (!newval[0]) {
        if (0 == pset.max_retry_times) {
            result = DEFAULT_RETRY_TIMES;
        } else {
            printf(_("Retry is off.\n"));
            pset.max_retry_times = 0;
            ResetQueryRetryController();
            EmptyRetryErrcodesList(pset.errcodes_list);
            return;
        }
    } else {
        char* endptr = NULL;

        errno = 0;
        result = (int)strtol(newval, &endptr, 10);

        // Check for various possible errors.
        if (errno == ERANGE || result != (int64)((int32)result)) {
            psql_error("Value exceeds integer range.\n");
            psql_error("Hint: The valid retry times is %d-%d.\n", DEFAULT_RETRY_TIMES, MAX_RETRY_TIMES);
            return;
        }

        if (*endptr != '\0' || endptr == newval || result < DEFAULT_RETRY_TIMES || result > MAX_RETRY_TIMES) {
            psql_error("Invalid retry times \"%s\".\n", newval);
            psql_error("Hint: The valid retry times is %d-%d.\n", DEFAULT_RETRY_TIMES, MAX_RETRY_TIMES);
            return;
        }
    }

    if (!ReadRetryErrcodesConfigFile()) {
        return;
    }
    pset.max_retry_times = result;
    if (!newval[0])
        printf(_("Retry is on with default retry times: %d.\n"), pset.max_retry_times);
    else
        printf(_("Retry is on with retry times: %d.\n"), pset.max_retry_times);
}

static void EstablishVariableSpace(void)
{
    pset.vars = CreateVariableSpace();

    SetVariableAssignHook(pset.vars, "AUTOCOMMIT", autocommit_hook);
    SetVariableAssignHook(pset.vars, "ON_ERROR_STOP", on_error_stop_hook);
    SetVariableAssignHook(pset.vars, "QUIET", quiet_hook);
    SetVariableAssignHook(pset.vars, "SINGLELINE", singleline_hook);
    SetVariableAssignHook(pset.vars, "SINGLESTEP", singlestep_hook);
    SetVariableAssignHook(pset.vars, "FETCH_COUNT", fetch_count_hook);
    SetVariableAssignHook(pset.vars, "ECHO", echo_hook);
    SetVariableAssignHook(pset.vars, "ECHO_HIDDEN", echo_hidden_hook);
    SetVariableAssignHook(pset.vars, "ON_ERROR_ROLLBACK", on_error_rollback_hook);
    SetVariableAssignHook(pset.vars, "HISTCONTROL", histcontrol_hook);
    SetVariableAssignHook(pset.vars, "PROMPT1", prompt1_hook);
    SetVariableAssignHook(pset.vars, "PROMPT2", prompt2_hook);
    SetVariableAssignHook(pset.vars, "PROMPT3", prompt3_hook);
    SetVariableAssignHook(pset.vars, "VERBOSITY", verbosity_hook);
    (void)SetVariableAssignHook(pset.vars, "RETRY", retry_hook);
}

/* Database Security: Data importing/dumping support AES128. */
/*
 * Funcation  : set_aes_key
 * Description: set aes g_key with dencryt_key if is not NULL
 *
 */
static void set_aes_key(const char* dencrypt_key)
{
    errno_t rc;

    if (dencrypt_key == NULL) {
        fprintf(stderr, _("%s: missing key\n"), pset.progname);
        exit(EXIT_FAILURE);
    }
    if (check_input_password(dencrypt_key)) {
        rc = memset_s(pset.decryptInfo.Key, KEY_MAX_LEN, 0, KEY_MAX_LEN);
        securec_check_c(rc, "\0", "\0");
        rc = strncpy_s((char*)pset.decryptInfo.Key, KEY_MAX_LEN, dencrypt_key, KEY_MAX_LEN - 1);
        securec_check_c(rc, "\0", "\0");
    } else {
        fprintf(stderr,
            _("%s:  The input key must be %d~%d bytes and "
            "contain at least three kinds of characters!\n"),
            pset.progname,
            MIN_KEY_LEN,
            MAX_KEY_LEN);
        exit(EXIT_FAILURE);
    }
}
