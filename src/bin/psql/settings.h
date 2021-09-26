/*
 * psql - the openGauss interactive terminal
 *
 * Copyright (c) 2000-2012, PostgreSQL Global Development Group
 *
 * src/bin/psql/settings.h
 */
#ifndef SETTINGS_H
#define SETTINGS_H

#include <vector>

#include "variables.h"
#include "print.h"

/* Database Security: Data importing/dumping support AES128. */
#include "utils/aes.h"

using std::vector;
typedef vector<char*> ErrCodes;

#define DEFAULT_FIELD_SEP "|"
#define DEFAULT_RECORD_SEP "\n"

#if defined(WIN32) || defined(__CYGWIN__)
#define DEFAULT_EDITOR "notepad.exe"
/* no DEFAULT_EDITOR_LINENUMBER_ARG for Notepad */
#else
#define DEFAULT_EDITOR "vi"
#define DEFAULT_EDITOR_LINENUMBER_ARG "+"
#endif

#define DEFAULT_PROMPT1 "%o%R%# "   /* gaussdb-ify replace '/' with 'o' */
#define DEFAULT_PROMPT2 "%o%R%# "
#define DEFAULT_PROMPT3 ">> "

typedef enum { PSQL_ECHO_NONE, PSQL_ECHO_QUERIES, PSQL_ECHO_ALL } PSQL_ECHO;

typedef enum { PSQL_ECHO_HIDDEN_OFF, PSQL_ECHO_HIDDEN_ON, PSQL_ECHO_HIDDEN_NOEXEC } PSQL_ECHO_HIDDEN;

typedef enum { PSQL_ERROR_ROLLBACK_OFF, PSQL_ERROR_ROLLBACK_INTERACTIVE, PSQL_ERROR_ROLLBACK_ON } PSQL_ERROR_ROLLBACK;

typedef enum {
    hctl_none = 0,
    hctl_ignorespace = 1,
    hctl_ignoredups = 2,
    hctl_ignoreboth = hctl_ignorespace | hctl_ignoredups
} HistControl;

enum trivalue { TRI_DEFAULT, TRI_NO, TRI_YES };

typedef struct _connectInfo {
    const char** keywords; /* options in connection info */
    char** values;         /* values of options in connection info. */
    bool* values_free;     /* Wether the values should be freed. */
} ConnectInfo;

typedef struct _psqlSettings {
    PGconn* db;         /* connection to backend */
    int encoding;       /* client_encoding */
    FILE* queryFout;    /* where to send the query results */
    bool queryFoutPipe; /* queryFout is from a popen() */

    printQueryOpt popt;

    char* gfname; /* one-shot file output argument for \g */

    bool notty;                /* stdin or stdout is not a tty (as determined
                                * on startup) */
    enum trivalue getPassword; /* prompt the user for a username and password */
    FILE* cur_cmd_source;      /* describe the status of the current main
                                * loop */
    bool cur_cmd_interactive;
    int sversion;         /* backend server version */
    const char* progname; /* in case you renamed psql */
    char* inputfile;      /* file being currently processed, if any */
    char* dirname;        /* current directory for \s display */

    uint64 lineno; /* also for error reporting */

    bool timing; /* enable timing of all queries */

    FILE* logfile; /* session log file handle */

    VariableSpace vars; /* "shell variable" repository */

    /*
     * The remaining fields are set by assign hooks associated with entries in
     * "vars".	They should not be set directly except by those hook
     * functions.
     */
    bool autocommit;
    bool on_error_stop;
    bool quiet;
    bool enable_client_encryption;
    bool singleline;
    bool singlestep;
    bool maintance;
#if defined(USE_ASSERT_CHECKING) || defined(FASTCHECK)
    bool parseonly; /* for gsql parser in debug version. */
#endif
    int fetch_count;
    PSQL_ECHO echo;
    PSQL_ECHO_HIDDEN echo_hidden;
    PSQL_ERROR_ROLLBACK on_error_rollback;
    HistControl histcontrol;
    const char* prompt1;
    const char* prompt2;
    const char* prompt3;
    PGVerbosity verbosity; /* current error verbosity level */
    /* Database Security: Data importing/dumping support AES128. */
    DecryptInfo decryptInfo;

    /* parallel execute multi statement. */
    bool parallel;
    int parallel_num;
    ConnectInfo connInfo;
    char** guc_stmt;
    int num_guc_stmt;

    // sql statement retry
    bool retry_on;
    bool retry_sleep;
    int retry_times;
    bool parallelCopyDone;
    bool parallelCopyOk;
    char retry_sqlstate[6];
    int max_retry_times;
    ErrCodes errcodes_list;
    struct parallelMutex_t* parallelMutex;
} PsqlSettings;

extern PsqlSettings pset;
extern char* argv_para;
extern int argv_num;

#ifndef EXIT_SUCCESS
#define EXIT_SUCCESS 0
#endif

#ifndef EXIT_FAILURE
#define EXIT_FAILURE 1
#endif

#define EXIT_BADCONN 2

#define EXIT_USER 3

/* Timeout for making connection, in seconds. */
#define CONNECT_TIMEOUT "300"
#endif

