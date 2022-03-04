/* -------------------------------------------------------------------------
 *
 * initdb --- initialize a PostgreSQL installation
 *
 * initdb creates (initializes) a PostgreSQL database cluster (site,
 * instance, installation, whatever).  A database cluster is a
 * collection of openGauss databases all managed by the same server.
 *
 * To create the database cluster, we create the directory that contains
 * all its data, create the files that hold the global tables, create
 * a few other control files for it, and create three databases: the
 * template databases "template0" and "template1", and a default user
 * database "postgres".
 *
 * The template databases are ordinary PostgreSQL databases.  template0
 * is never supposed to change after initdb, whereas template1 can be
 * changed to add site-local standard data.  Either one can be copied
 * to produce a new database.
 *
 * For largely-historical reasons, the template1 database is the one built
 * by the basic bootstrap process.	After it is complete, template0 and
 * the default database, openGauss, are made just by copying template1.
 *
 * To create template1, we run the openGauss (backend) program in bootstrap
 * mode and feed it data from the postgres.bki library file.  After this
 * initial bootstrap phase, some additional stuff is created by normal
 * SQL commands fed to a standalone backend.  Some of those commands are
 * just embedded into this program (yeah, it's ugly), but larger chunks
 * are taken from script files.
 *
 *
 * Note:
 *	 The program has some memory leakage - it isn't worth cleaning it up.
 *
 * This is a C implementation of the previous shell script for setting up a
 * openGauss cluster location, and should be highly compatible with it.
 * author of C translation: Andrew Dunstan	   mailto:andrew@dunslane.net
 *
 * This code is released under the terms of the PostgreSQL License.
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/bin/initdb/initdb.c
 *
 * -------------------------------------------------------------------------
 */

#include "postgres_fe.h"

#include <cipher.h>

#include "access/ustore/undo/knl_uundoapi.h"
#include "access/ustore/undo/knl_uundotxn.h"
#include "libpq/pqsignal.h"
#include "mb/pg_wchar.h"
#include "getaddrinfo.h"
#include "getopt_long.h"
#include "miscadmin.h"
#include "bin/elog.h"

#ifdef ENABLE_MULTIPLE_NODES
#include "distribute_core.h"
#endif

#ifdef PGXC
#define PGXC_NODENAME_LENGTH 64
#endif


#define PROG_NAME "gs_initdb"

/* Filename components for OpenTemporaryFile
 * Note that this macro must be the same to fd.h
 */
#define PG_TEMP_FILES_DIR "pgsql_tmp"
#define RESULT_LENGTH 20
#define BUF_LENGTH 64
#define PG_CAST_CHAR_LENGTH 1024
#define INFO_VERSION_LENGTH 100

/* Ideally this would be in a .h file, but it hardly seems worth the trouble */
extern const char* select_default_timezone(const char* share_path);

/* Database Security:  Support SHA256.*/
/*add sha256 encrypt method */
static const char* auth_methods_host[] = {"trust",
    "reject",
    "md5",
    "sha256",
    "sm3",
    "password",
    "ident",
#ifdef ENABLE_GSS
    "gss",
#endif
#ifdef ENABLE_SSPI
    "sspi",
#endif
#ifdef KRB5
    "krb5",
#endif
#ifdef USE_PAM
    "pam",
    "pam ",
#endif
#ifdef USE_LDAP
    "ldap",
#endif
#ifdef USE_SSL
    "cert",
#endif
    NULL};
/* Database Security:  Support SHA256.*/
/*add sha256 encrypt method */
static const char* auth_methods_local[] = {"trust",
    "reject",
    "md5",
    "sha256",
    "sm3",
    "password",
    "peer",
#ifdef USE_PAM
    "pam",
    "pam ",
#endif
#ifdef USE_LDAP
    "ldap",
#endif
    NULL};

/*
 * these values are passed in by makefile defines
 */
static char* share_path = NULL;

/* values to be obtained from arguments */
static char* host_ip = "";
static char* pg_data = "";
static char* encoding = "";
static char* locale = "";
static char* lc_collate = "";
static char* lc_ctype = "";
static char* lc_monetary = "";
static char* lc_numeric = "";
static char* lc_time = "";
static char* lc_messages = "";
static const char* default_text_search_config = "";
static char* username = "";
static bool pwprompt = false;
static char* pwfilename = NULL;
static const char* authmethodhost = "";
static const char* authmethodlocal = "";
static bool debug = false;
static bool noclean = false;
static bool show_setting = false;
static char* xlog_dir = "";
static bool security = false;
static char* dbcompatibility = "";
static char* new_xlog_file_path = "";

#ifdef PGXC
/* Name of the PGXC node initialized */
static char* nodename = NULL;
#endif

/* internal vars */
static const char* progname;
static char* encodingid = "0";
static char* bki_file;
static char* desc_file;
static char* shdesc_file;
static char* hba_file;
static char* ident_file;
static char* conf_file;
#ifdef ENABLE_MOT
static char* mot_conf_file;
#endif
static char* gazelle_conf_file;
static char* conversion_file;
static char* dictionary_file;
static char* info_schema_file;
static char* features_file;
static char* system_views_file;
static char* performance_views_file;
#ifdef ENABLE_PRIVATEGAUSS
static char* private_system_views_file;
#endif
#ifndef ENABLE_MULTIPLE_NODES
static char* snapshot_names[] = { "schema", "create", "prepare", "sample", "publish", "purge" };
#define SNAPSHOT_LEN (sizeof(snapshot_names) / sizeof(snapshot_names[0]))
static char* snapshot_files[SNAPSHOT_LEN];
static bool enableDCF = false;
#endif
static bool made_new_pgdata = false;
static bool found_existing_pgdata = false;
static bool made_new_xlogdir = false;
static bool found_existing_xlogdir = false;
static char infoversion[100];
static bool caught_signal = false;
static bool output_failed = false;
static int output_errno = 0;

static char* pwpasswd = NULL;
static char depasswd[RANDOM_LEN + 1];

/* defaults */
static int n_connections = 10;
static int n_buffers = 50;

#ifndef ENABLE_MULTIPLE_NODES
DB_CompatibilityAttr g_dbCompatArray[] = {
    {DB_CMPT_A, "A"},
    {DB_CMPT_B, "B"},
    {DB_CMPT_C, "C"},
    {DB_CMPT_PG, "PG"}
};
#endif

/*
 * Warning messages for authentication methods
 */
#define AUTHTRUST_WARNING                                                    \
    "# CAUTION: Configuring the system for local \"trust\" authentication\n" \
    "# allows any local user to connect as any PostgreSQL user, including\n" \
    "# the database sysadmin.  If you do not trust all your local users,\n"  \
    "# use another authentication method.\n"
static char* authwarning = NULL;
static const char* boot_options = NULL;
static const char* backend_options = NULL;

/*
 * Centralized knowledge of switches to pass to backend
 *
 * Note: in the shell-script version, we also passed PGDATA as a -D switch,
 * but here it is more convenient to pass it as an environment variable
 * (no quoting to worry about).
 */
static const char* raw_boot_options = "-F";
static const char* raw_backend_options = "--single "
#ifdef PGXC
                                     "--localxid "
#endif
                                     "-F -O -c search_path=pg_catalog -c exit_on_error=true";

#define FREE_AND_RESET(ptr)  \
    do {                     \
        if (NULL != (ptr) && reinterpret_cast<char*>(ptr) != static_cast<char*>("")) { \
            free(ptr);       \
            (ptr) = NULL;    \
        }                    \
    } while (0)

/* path to 'initdb' binary directory */
static char bin_path[MAXPGPATH];
static char backend_exec[MAXPGPATH];

static void* pg_malloc(size_t size);
static char* xstrdup(const char* s);
static char** replace_token(char** lines, const char* token, const char* replacement);

#ifndef HAVE_UNIX_SOCKETS
static char** filter_lines_with_token(char** lines, const char* token);
#endif
static char** readfile(const char* path);
static void writefile(char* path, char** lines);
static FILE* popen_check(const char* command, const char* mode);
static void exit_nicely(void);
static char* get_id(void);
static char* get_encoding_id(char* encoding_name);
static void set_input(char** dest, const char* filename);
static void check_input(char* path);
static void write_version_file(const char* extrapath);
static void create_pg_lockfiles(void);
static void set_null_conf(void);
static void test_config_settings(void);
static void setup_config(void);
static void bootstrap_template1(void);
static void setup_auth(void);
static void get_set_pwd(void);
static void setup_depend(void);
static void setup_sysviews(void);
static void setup_perfviews(void);
#ifdef ENABLE_PRIVATEGAUSS
static void setup_privsysviews(void);
#endif
static void setup_update(void);
#ifdef PGXC
static void setup_nodeself(void);
static bool is_valid_nodename(const char* nodename);
#endif
static void setup_description(void);
static void setup_collation(void);
static void setup_conversion(void);
static void setup_dictionary(void);
static void setup_privileges(void);
static void set_info_version(void);
static void setup_schema(void);
static void setup_bucketmap_len(void);
static void load_plpgsql(void);
static void load_supported_extension(void);
static void load_dist_fdw(void);
static void load_hdfs_fdw(void);
#ifdef ENABLE_MOT
static void load_mot_fdw(void); /* load MOT fdw */
#endif
static void load_hstore_extension(void);
#if ((defined(ENABLE_MULTIPLE_NODES)) || (defined(ENABLE_PRIVATEGAUSS)))
static void load_packages_extension(void);
#endif
#ifdef ENABLE_MULTIPLE_NODES
static void load_gsredistribute_extension(void);

#endif
static void vacuum_db(void);
static void make_template0(void);
static void make_postgres(void);
static void trapsig(int signum);
static void check_ok(void);
static char* escape_quotes(const char* src);
static int locale_date_order(const char* locale_time);
static void check_locale_name(int category, const char* locale_name, char** canonname);
static bool check_locale_encoding(const char* locale_encoding, int encoding);
static void setlocales(void);
static void usage(const char* prog_name);
static bool IsSpecialCharacter(char ch);
static bool CheckInitialPasswd(const char* username, const char* pass_wd);
static char* reverse_string(const char* str);
static void mkdirForPgLocationDir();
#ifdef WIN32
static int CreateRestrictedProcess(char* cmd, PROCESS_INFORMATION* processInfo);
#endif

static void InitUndoSubsystemMeta();
static void check_input_spec_char(char* input_env_value, bool skip_dollar = false);

/*
 * macros for running pipes to openGauss
 */
#define PG_CMD_DECL      \
    char cmd[MAXPGPATH]; \
    FILE* cmdfd = NULL

#define PG_CMD_OPEN                                                     \
    do {                                                                \
        cmdfd = popen_check(cmd, "w");                                  \
        if (cmdfd == NULL)                                              \
            exit_nicely(); /* message already printed by popen_check */ \
    } while (0)

#define PG_CMD_CLOSE                                                     \
    do {                                                                 \
        if (pclose_check(cmdfd))                                         \
            exit_nicely(); /* message already printed by pclose_check */ \
    } while (0)

#define PG_CMD_PUTS(line)                                \
    do {                                                 \
        if (fputs((line), cmdfd) < 0 || fflush(cmdfd) < 0) \
            output_failed = true, output_errno = errno;  \
    } while (0)

#define PG_CMD_PRINTF1(fmt, arg1)                               \
    do {                                                        \
        if (fprintf(cmdfd, (fmt), (arg1)) < 0 || fflush(cmdfd) < 0) \
            output_failed = true, output_errno = errno;         \
    } while (0)

#define PG_CMD_PRINTF2(fmt, arg1, arg2)                               \
    do {                                                              \
        if (fprintf(cmdfd, (fmt), (arg1), (arg2)) < 0 || fflush(cmdfd) < 0) \
            output_failed = true, output_errno = errno;               \
    } while (0)

#define PG_CMD_PRINTF3(fmt, arg1, arg2, arg3)                               \
    do {                                                                    \
        if (fprintf(cmdfd, (fmt), (arg1), (arg2), (arg3)) < 0 || fflush(cmdfd) < 0) \
            output_failed = true, output_errno = errno;                     \
    } while (0)

#ifndef WIN32
#define QUOTE_PATH ""
#define DIR_SEP "/"
#else
#define QUOTE_PATH "\""
#define DIR_SEP "\\"
#endif

#ifndef ERROR_LIMIT_LEN
#define ERROR_LIMIT_LEN 256
#endif

/* Macro about bucket length blow must keep same with kernel code */
#define DEFAULT_BUCKETSLEN 16384
#define MIN_BUCKETSLEN 32
#define MAX_BUCKETSLEN DEFAULT_BUCKETSLEN
int g_bucket_len = DEFAULT_BUCKETSLEN;

#define INSERT_BUCKET_SQL_LEN 512

static void check_input_spec_char(char* input_env_value, bool skip_dollar)
{
    if (input_env_value == NULL) {
        return;
    }

    const char* danger_character_list[] = {"|", ";", "&", "$", "<", ">", "`", "\\", "!", NULL};

    int i = 0;

    for (i = 0; danger_character_list[i] != NULL; i++) {
        if (strstr((const char*)input_env_value, danger_character_list[i]) != NULL &&
            !(skip_dollar && danger_character_list[i][0] == '$')) {
            write_stderr(_("Error: variable \"%s\" contains invaild symbol \"%s\".\n"),
                input_env_value, danger_character_list[i]);
            exit(1);
        }
    }
}

void check_env_value(const char* input_env_value)
{
    const char* danger_character_list[] = {"|",
        ";",
        "&",
        "$",
        "<",
        ">",
        "`",
        "\\",
        "'",
        "\"",
        "{",
        "}",
        "(",
        ")",
        "[",
        "]",
        "~",
        "*",
        "?",
        "!",
        "\n",
        NULL};
    int i = 0;

    for (i = 0; danger_character_list[i] != NULL; i++) {
        if (strstr(input_env_value, danger_character_list[i]) != NULL) {
            fprintf(
                stderr, "invalid token \"%s\" in input_env_value: (%s)\n", danger_character_list[i], input_env_value);
            exit(1);
        }
    }
}

/*
 * routines to check mem allocations and fail noisily.
 *
 * Note that we can't call exit_nicely() on a memory failure, as it calls
 * rmtree() which needs memory allocation. So we just exit with a bang.
 */
static void* pg_malloc(size_t size)
{
    void* result = NULL;

    /* Avoid unportable behavior of malloc(0) */
    if (size == 0)
        size = 1;
    result = malloc(size);
    if (result == NULL) {
        write_stderr(_("%s: out of memory\n"), progname);
        exit(1);
    }
    return result;
}

static char* xstrdup(const char* s)
{
    char* result = NULL;

    result = strdup(s);
    if (result == NULL) {
        write_stderr(_("%s: out of memory\n"), progname);
        exit(1);
    }
    return result;
}

/*
 * make a copy of the array of lines, with token replaced by replacement
 * the first time it occurs on each line.
 *
 * This does most of what sed was used for in the shell script, but
 * doesn't need any regexp stuff.
 */
static char** replace_token(char** lines, const char* token, const char* replacement)
{
    int numlines = 1;
    int i;
    char** result;
    errno_t rc = 0;
    int toklen, replen, diff;

    for (i = 0; lines[i] != NULL; i++) {
        numlines++;
    }
    result = (char**)pg_malloc(numlines * sizeof(char*));

    toklen = strlen(token);
    replen = strlen(replacement);
    diff = replen - toklen;

    for (i = 0; i < numlines; i++) {
        char* where = NULL;
        char* newline = NULL;
        int pre;

        /* just copy pointer if NULL or no change needed */
        if (lines[i] == NULL || (where = strstr(lines[i], token)) == NULL) {
            result[i] = lines[i];
            continue;
        }

        /* if we get here a change is needed - set up new line */

        newline = (char*)pg_malloc(strlen(lines[i]) + diff + 1);

        pre = where - lines[i];

        rc = strncpy_s(newline, (strlen(lines[i]) + diff + 1), lines[i], pre);
        securec_check_c(rc, newline, "\0");

        rc = strcpy_s(newline + pre, (strlen(lines[i]) + diff + 1 - pre), replacement);
        securec_check_c(rc, newline, "\0");

        rc = strcpy_s(newline + pre + replen, (strlen(lines[i]) + diff + 1 - pre - replen), lines[i] + pre + toklen);
        securec_check_c(rc, newline, "\0");

        result[i] = newline;
        FREE_AND_RESET(lines[i]);
    }
    free(lines);
    return result;
}

static char** append_token(char** lines, const char* token)
{
    int numlines = 0;
    int i;
    char** result;
    char* newline = NULL;
    errno_t rc = 0;
    int newlen;

    for (i = 0; lines[i] != NULL; i++) {
        numlines++;
    }

    result = (char**)pg_malloc((numlines + 2) * sizeof(char*));

    for (i = 0; i < numlines; i++) {
        result[i] = lines[i];
    }
    newlen = strlen(token) + 1;
    newline = (char*)pg_malloc(newlen);
    rc = strncpy_s(newline, newlen, token, strlen(token));
    securec_check_c(rc, newline, "\0");
    result[numlines] = newline;
    result[numlines + 1] = NULL;
    free(lines);
    return result;
}


/*
 * make a copy of lines without any that contain the token
 *
 * a sort of poor man's grep -v
 */
#ifndef HAVE_UNIX_SOCKETS
static char** filter_lines_with_token(char** lines, const char* token)
{
    int numlines = 1;
    int i, src, dst;
    char** result;

    for (i = 0; lines[i]; i++)
        numlines++;

    result = (char**)pg_malloc(numlines * sizeof(char*));

    for (src = 0, dst = 0; src < numlines; src++) {
        if (lines[src] == NULL || strstr(lines[src], token) == NULL)
            result[dst++] = lines[src];
    }

    return result;
}
#endif

/*
 * get the lines from a text file
 */
static char** readfile(const char* path)
{
    FILE* infile = NULL;
    int maxlength = 1, linelen = 0;
    int nlines = 0;
    int n;
    char** result;
    char* buffer = NULL;
    int c;

    if ((infile = fopen(path, "r")) == NULL) {
        char errBuffer[ERROR_LIMIT_LEN];
        write_stderr(_("%s: could not open file \"%s\" for reading: %s\n"),
            progname,
            path,
            pqStrerror(errno, errBuffer, ERROR_LIMIT_LEN));
        exit_nicely();
    }

    /* pass over the file twice - the first time to size the result */

    while ((c = fgetc(infile)) != EOF) {
        linelen++;
        if (c == '\n') {
            nlines++;
            if (linelen > maxlength)
                maxlength = linelen;
            linelen = 0;
        }
    }

    /* handle last line without a terminating newline (yuck) */
    if (linelen) {
        nlines++;
    }
        
    if (linelen > maxlength) {
        maxlength = linelen;
    }

    /* set up the result and the line buffer */
    result = (char**)pg_malloc((nlines + 1) * sizeof(char*));
    buffer = (char*)pg_malloc(maxlength + 1);

    /* now reprocess the file and store the lines */
    rewind(infile);
    n = 0;
    while (fgets(buffer, maxlength + 1, infile) != NULL && n < nlines)
        result[n++] = xstrdup(buffer);

    fclose(infile);
    FREE_AND_RESET(buffer);
    result[n] = NULL;

    return result;
}

/*
 * write an array of lines to a file
 *
 * This is only used to write text files.  Use fopen "w" not PG_BINARY_W
 * so that the resulting configuration files are nicely editable on Windows.
 */
static void writefile(char* path, char** lines)
{
    FILE* out_file = NULL;
    char** line;

    errno = 0;
    if ((out_file = fopen(path, "w")) == NULL) {
        char errBuffer[ERROR_LIMIT_LEN];
        write_stderr(_("%s: could not open file \"%s\" for writing: %s\n"),
            progname,
            path,
            pqStrerror(errno, errBuffer, ERROR_LIMIT_LEN));
        exit_nicely();
    }
    for (line = lines; *line != NULL; line++) {
        if (fputs(*line, out_file) < 0) {
            char errBuffer[ERROR_LIMIT_LEN];
            write_stderr(_("%s: could not write file \"%s\": %s\n"),
                progname,
                path,
                pqStrerror(errno, errBuffer, ERROR_LIMIT_LEN));
            fclose(out_file);
            exit_nicely();
        }
        FREE_AND_RESET(*line);
    }

    /* fsync the configuration file immediately, in case of an unfortunate system crash */
    if (fsync(fileno(out_file)) != 0) {
        char errBuffer[ERROR_LIMIT_LEN];
        write_stderr(
            _("%s: could not fsync file \"%s\": %s\n"), progname, path, pqStrerror(errno, errBuffer, ERROR_LIMIT_LEN));
        fclose(out_file);
        exit_nicely();
    }

    if (fclose(out_file) != 0) {
        char errBuffer[ERROR_LIMIT_LEN];
        write_stderr(
            _("%s: could not write file \"%s\": %s\n"), progname, path, pqStrerror(errno, errBuffer, ERROR_LIMIT_LEN));
        exit_nicely();
    }
}

/*
 * Open a subcommand with suitable error messaging
 */
static FILE* popen_check(const char* command, const char* mode)
{
    FILE* cmdfd = NULL;

    (void)fflush(stdout);
    (void)fflush(stderr);
    errno = 0;
    cmdfd = popen(command, mode);
    if (cmdfd == NULL) {
        char errBuffer[ERROR_LIMIT_LEN];
        write_stderr(_("%s: could not execute command \"%s\": %s\n"),
            progname,
            command,
            pqStrerror(errno, errBuffer, ERROR_LIMIT_LEN));
    }
    return cmdfd;
}

/*
 * clean up any files we created on failure
 * if we created the data directory remove it too
 */
static void exit_nicely(void)
{
    if (!noclean) {
        if (made_new_pgdata) {
            write_stderr(_("%s: removing data directory \"%s\"\n"), progname, pg_data);
            if (!rmtree(pg_data, true))
                write_stderr(_("%s: failed to remove data directory\n"), progname);
        } else if (found_existing_pgdata) {
            write_stderr(_("%s: removing contents of data directory \"%s\"\n"), progname, pg_data);
            if (!rmtree(pg_data, false))
                write_stderr(_("%s: failed to remove contents of data directory\n"), progname);
        }

        if (made_new_xlogdir) {
            write_stderr(_("%s: removing transaction log directory \"%s\"\n"), progname, xlog_dir);
            if (!rmtree(xlog_dir, true))
                write_stderr(_("%s: failed to remove transaction log directory\n"), progname);
        } else if (found_existing_xlogdir) {
            write_stderr(_("%s: removing contents of transaction log directory \"%s\"\n"), progname, xlog_dir);
            if (!rmtree(xlog_dir, false))
                write_stderr(_("%s: failed to remove contents of transaction log directory\n"), progname);
        }
        /* otherwise died during startup, do nothing! */
    } else {
        if (made_new_pgdata || found_existing_pgdata)
            write_stderr(_("%s: data directory \"%s\" not removed at user's request\n"), progname, pg_data);

        if (made_new_xlogdir || found_existing_xlogdir)
            write_stderr(_("%s: transaction log directory \"%s\" not removed at user's request\n"), progname, xlog_dir);
    }

    exit(1);
}

/*
 * find the current user
 *
 * on unix make sure it isn't really root
 */
static char* get_id(void)
{
#ifndef WIN32

    struct passwd* pw = NULL;

    if (geteuid() == 0) /* 0 is root's uid */
    {
        write_stderr(_("%s: cannot be run as root\n"
                       "Please log in (using, e.g., \"su\") as the "
                       "(unprivileged) user that will\n"
                       "own the server process.\n"),
            progname);
        exit(1);
    }

    pw = getpwuid(geteuid());
    if (pw == NULL) {
        char errBuffer[ERROR_LIMIT_LEN];
        write_stderr(_("%s: could not obtain information about current user: %s\n"),
            progname,
            pqStrerror(errno, errBuffer, ERROR_LIMIT_LEN));
        exit(1);
    }
#else /* the windows code */

    struct passwd_win32 {
        int pw_uid;
        char pw_name[128];
    } pass_win32;
    struct passwd_win32* pw = &pass_win32;
    DWORD pwname_size = sizeof(pass_win32.pw_name) - 1;

    pw->pw_uid = 1;
    if (!GetUserName(pw->pw_name, &pwname_size)) {
        char errBuffer[ERROR_LIMIT_LEN];
        write_stderr(
            _("%s: could not get current user name: %s\n"), progname, pqStrerror(errno, errBuffer, ERROR_LIMIT_LEN));
        exit(1);
    }
#endif

    return xstrdup(pw->pw_name);
}

static char* encodingid_to_string(int enc)
{
    char result[RESULT_LENGTH] = {0};
    int nRet = 0;

    nRet = sprintf_s(result, RESULT_LENGTH, "%d", enc);
    securec_check_ss_c(nRet, "\0", "\0");
    return xstrdup(result);
}

/*
 * get the encoding id for a given encoding name
 */
static char* get_encoding_id(char* encoding_name)
{
    int enc;

    if ((encoding_name != NULL) && *encoding_name) {
        if ((enc = pg_valid_server_encoding(encoding_name)) >= 0)
            return encodingid_to_string(enc);
    }
    write_stderr(_("%s: \"%s\" is not a valid server encoding name\n"),
        progname,
        encoding_name != NULL ? encoding_name : "(null)");
    exit(1);
}

/*
 * Support for determining the best default text search configuration.
 * We key this off the first part of LC_CTYPE (ie, the language name).
 */
struct tsearch_config_match {
    const char* tsconfname;
    const char* langname;
};

static const struct tsearch_config_match tsearch_config_languages[] = {
    {"danish", "da"},
    {"danish", "Danish"},
    {"dutch", "nl"},
    {"dutch", "Dutch"},
    {"english", "C"},
    {"english", "POSIX"},
    {"english", "en"},
    {"english", "English"},
    {"finnish", "fi"},
    {"finnish", "Finnish"},
    {"french", "fr"},
    {"french", "French"},
    {"german", "de"},
    {"german", "German"},
    {"hungarian", "hu"},
    {"hungarian", "Hungarian"},
    {"italian", "it"},
    {"italian", "Italian"},
    {"norwegian", "no"},
    {"norwegian", "Norwegian"},
    {"portuguese", "pt"},
    {"portuguese", "Portuguese"},
    {"romanian", "ro"},
    {"russian", "ru"},
    {"russian", "Russian"},
    {"spanish", "es"},
    {"spanish", "Spanish"},
    {"swedish", "sv"},
    {"swedish", "Swedish"},
    {"turkish", "tr"},
    {"turkish", "Turkish"},
    {NULL, NULL} /* end marker */
};

/*
 * Look for a text search configuration matching lc_ctype, and return its
 * name; return NULL if no match.
 */
static const char* find_matching_ts_config(const char* lc_type)
{
    int i;
    char *langname = NULL, *ptr = NULL;

    /*
     * Convert lc_ctype to a language name by stripping everything after an
     * underscore (usual case) or a hyphen (Windows "locale name"; see
     * comments at IsoLocaleName()).
     *
     * XXX Should ' ' be a stop character?  This would select "norwegian" for
     * the Windows locale "Norwegian (Nynorsk)_Norway.1252".  If we do so, we
     * should also accept the "nn" and "nb" Unix locales.
     *
     * Just for paranoia, we also stop at '.' or '@'.
     */
    if (lc_type == NULL)
        langname = xstrdup("");
    else {
        ptr = langname = xstrdup(lc_type);
        while (*ptr && *ptr != '_' && *ptr != '-' && *ptr != '.' && *ptr != '@')
            ptr++;
        *ptr = '\0';
    }

    for (i = 0; tsearch_config_languages[i].tsconfname != NULL; i++) {
        if (pg_strcasecmp(tsearch_config_languages[i].langname, langname) == 0) {
            FREE_AND_RESET(langname);
            return tsearch_config_languages[i].tsconfname;
        }
    }

    FREE_AND_RESET(langname);
    return NULL;
}

/*
 * set name of given input file variable under data directory
 */
static void set_input(char** dest, const char* filename)
{
    int nRet = 0;
    *dest = (char*)pg_malloc(strlen(share_path) + strlen(filename) + 2);
    nRet = sprintf_s(*dest, (strlen(share_path) + strlen(filename) + 2), "%s/%s", share_path, filename);
    securec_check_ss_c(nRet, *dest, "\0");
}

/*
 * check that given input file exists
 */
static void check_input(char* path)
{
    struct stat statbuf;

    if (stat(path, &statbuf) != 0) {
        if (errno == ENOENT) {
            write_stderr(_("%s: file \"%s\" does not exist\n"), progname, path);
            write_stderr(_("This might mean you have a corrupted installation or identified\n"
                           "the wrong directory with the invocation option -L.\n"));
        } else {
            char errBuffer[ERROR_LIMIT_LEN];
            write_stderr(_("%s: could not access file \"%s\": %s\n"),
                progname,
                path,
                pqStrerror(errno, errBuffer, ERROR_LIMIT_LEN));
            write_stderr(_("This might mean you have a corrupted installation or identified\n"
                           "the wrong directory with the invocation option -L.\n"));
        }
        exit(1);
    }
    if (!S_ISREG(statbuf.st_mode)) {
        write_stderr(_("%s: file \"%s\" is not a regular file\n"), progname, path);
        write_stderr(_("This might mean you have a corrupted installation or identified\n"
                       "the wrong directory with the invocation option -L.\n"));
        exit(1);
    }
}

/*
 * write out the PG_VERSION file in the data dir, or its subdirectory
 * if extrapath is not NULL
 */
static void write_version_file(const char* extrapath)
{
    FILE* version_file = NULL;
    char* path = NULL;
    int nRet = 0;

    if (extrapath == NULL) {
        path = (char*)pg_malloc(strlen(pg_data) + 12);
        nRet = sprintf_s(path, (strlen(pg_data) + 12), "%s/PG_VERSION", pg_data);
    } else {
        path = (char*)pg_malloc(strlen(pg_data) + strlen(extrapath) + 13);
        nRet = sprintf_s(path, (strlen(pg_data) + strlen(extrapath) + 13), "%s/%s/PG_VERSION", pg_data, extrapath);
    }
    securec_check_ss_c(nRet, path, "\0");

    errno = 0;
    canonicalize_path(path);
    version_file = fopen(path, PG_BINARY_W);
    if (NULL == version_file) {
        char errBuffer[ERROR_LIMIT_LEN];
        write_stderr(_("%s: could not open file \"%s\" for writing: %s\n"),
            progname,
            path,
            pqStrerror(errno, errBuffer, ERROR_LIMIT_LEN));
        FREE_AND_RESET(path);
        exit_nicely();
    }
    if (fprintf(version_file, "%s\n", PG_MAJORVERSION) < 0) {
        char errBuffer[ERROR_LIMIT_LEN];
        write_stderr(
            _("%s: could not write file \"%s\": %s\n"), progname, path, pqStrerror(errno, errBuffer, ERROR_LIMIT_LEN));
        fclose(version_file);
        FREE_AND_RESET(path);
        exit_nicely();
    }

    /* fsync the PG_VERSION file immediately, in case of an unfortunate system crash */
    if (fsync(fileno(version_file)) != 0) {
        char errBuffer[ERROR_LIMIT_LEN];
        write_stderr(
            _("%s: could not fsync file \"%s\": %s\n"), progname, path, pqStrerror(errno, errBuffer, ERROR_LIMIT_LEN));
        fclose(version_file);
        FREE_AND_RESET(path);
        exit_nicely();
    }

    if (fclose(version_file) != 0) {
        char errBuffer[ERROR_LIMIT_LEN];
        write_stderr(
            _("%s: could not write file \"%s\": %s\n"), progname, path, pqStrerror(errno, errBuffer, ERROR_LIMIT_LEN));
        FREE_AND_RESET(path);
        exit_nicely();
    }
    FREE_AND_RESET(path);
}

static void CreatePGDefaultTempDir()
{
    char* path = NULL;
    errno_t rc = 0;

    path = (char*)pg_malloc(strlen(pg_data) + strlen("base/") + strlen(PG_TEMP_FILES_DIR) + 13);
    rc = sprintf_s(path,
        (strlen(pg_data) + strlen("base/") + strlen(PG_TEMP_FILES_DIR) + 13),
        "%s/base/%s",
        pg_data,
        PG_TEMP_FILES_DIR);
    securec_check_ss_c(rc, path, "\0");

    if (mkdir(path, S_IRWXU) < 0) {
        char errBuffer[ERROR_LIMIT_LEN];
        write_stderr(
            _("%s: could not mkdir \"%s\": %s\n"), progname, path, pqStrerror(errno, errBuffer, ERROR_LIMIT_LEN));
        FREE_AND_RESET(path);
        exit_nicely();
    }
    FREE_AND_RESET(path);
}

/*
 * set up an empty config file so we can check config settings by launching
 * a test backend
 */
static void set_null_conf(void)
{
    FILE* confile = NULL;
    char* path = NULL;
    int nRet = 0;

    path = (char*)pg_malloc(strlen(pg_data) + 17);
    nRet = sprintf_s(path, (strlen(pg_data) + 17), "%s/postgresql.conf", pg_data);
    securec_check_ss_c(nRet, path, "\0");
    confile = fopen(path, PG_BINARY_W);
    if (confile == NULL) {
        char errBuffer[ERROR_LIMIT_LEN];
        write_stderr(_("%s: could not open file \"%s\" for writing: %s\n"),
            progname,
            path,
            pqStrerror(errno, errBuffer, ERROR_LIMIT_LEN));
        FREE_AND_RESET(path);
        exit_nicely();
    }
    if (fclose(confile)) {
        char errBuffer[ERROR_LIMIT_LEN];
        write_stderr(
            _("%s: could not write file \"%s\": %s\n"), progname, path, pqStrerror(errno, errBuffer, ERROR_LIMIT_LEN));
        FREE_AND_RESET(path);
        exit_nicely();
    }
    FREE_AND_RESET(path);
}

/*
 * Determine platform-specific config settings
 *
 * Use reasonable values if kernel will let us, else scale back.  Probe
 * for max_connections first since it is subject to more constraints than
 * shared_buffers.
 */
static void test_config_settings(void)
{
    /*
     * This macro defines the minimum shared_buffers we want for a given
     * max_connections value. The arrays show the settings to try.
     */
#define MIN_BUFS_FOR_CONNS(nconns) ((nconns)*10)

    static const int trial_conns[] = {100, 50, 40, 30, 20, 10};
    static const int trial_bufs[] = {
        4096, 3584, 3072, 2560, 2048, 1536, 1000, 900, 800, 700, 600, 500, 400, 300, 200, 100, 50};

    char cmd[MAXPGPATH];
    const int connslen = sizeof(trial_conns) / sizeof(int);
    const int bufslen = sizeof(trial_bufs) / sizeof(int);
    int nRet = 0;
    int i, status, test_conns, test_buffs, ok_buffers = 0;

    printf(_("selecting default max_connections ... "));
    (void)fflush(stdout);

    for (i = 0; i < connslen; i++) {
        test_conns = trial_conns[i];
        test_buffs = MIN_BUFS_FOR_CONNS(test_conns);

        nRet = snprintf_s(cmd,
            sizeof(cmd),
            sizeof(cmd) - 1,
            SYSTEMQUOTE "\"%s\" --boot -x0 %s "
                        "-c max_connections=%d "
                        "-c shared_buffers=%d "
                        "< \"%s\" > \"%s\" 2>&1" SYSTEMQUOTE,
            backend_exec,
            boot_options,
            test_conns,
            test_buffs,
            DEVNULL,
            DEVNULL);
        securec_check_ss_c(nRet, "\0", "\0");
        status = system(cmd);
        if (status == 0) {
            ok_buffers = test_buffs;
            break;
        }
    }
    if (i >= connslen) {
        i = connslen - 1;
    }

    n_connections = trial_conns[i];

    printf("%d\n", n_connections);

    printf(_("selecting default shared_buffers ... "));
    (void)fflush(stdout);

    for (i = 0; i < bufslen; i++) {
        /* Use same amount of memory, independent of BLCKSZ */
        test_buffs = (trial_bufs[i] * 8192) / BLCKSZ;
        if (test_buffs <= ok_buffers) {
            test_buffs = ok_buffers;
            break;
        }

        nRet = snprintf_s(cmd,
            sizeof(cmd),
            sizeof(cmd) - 1,
            SYSTEMQUOTE "\"%s\" --boot -x0 %s "
                        "-c max_connections=%d "
                        "-c shared_buffers=%d "
                        "< \"%s\" > \"%s\" 2>&1" SYSTEMQUOTE,
            backend_exec,
            boot_options,
            n_connections,
            test_buffs,
            DEVNULL,
            DEVNULL);
        securec_check_ss_c(nRet, "\0", "\0");
        status = system(cmd);
        if (status == 0)
            break;
    }
    n_buffers = test_buffs;

    if ((n_buffers * (BLCKSZ / 1024)) % 1024 == 0)
        printf("%dMB\n", (n_buffers * (BLCKSZ / 1024)) / 1024);
    else
        printf("%dkB\n", n_buffers * (BLCKSZ / 1024));
}

/*
 * set up all the config files
 */
static void setup_config(void)
{
    char** conflines;
    char repltok[TZ_STRLEN_MAX + 100];
    char path[MAXPGPATH];
    const char* default_timezone = NULL;
    int nRet = 0;
    errno_t rc = 0;
    char* buf_messages = NULL;
    char* buf_monetary = NULL;
    char* buf_numeric = NULL;
    char* buf_time = NULL;
    char* buf_default_text_search_config = NULL;
    char* buf_nodename = NULL;
    char* buf_default_timezone = NULL;

    fputs(_("creating configuration files ... "), stdout);
    (void)fflush(stdout);

    /* postgresql.conf */

    conflines = readfile(conf_file);

    nRet = sprintf_s(repltok, sizeof(repltok), "max_connections = %d", n_connections);
    securec_check_ss_c(nRet, "\0", "\0");
    conflines = replace_token(conflines, "#max_connections = 100", repltok);

    if ((n_buffers * (BLCKSZ / 1024)) % 1024 == 0)
        nRet = sprintf_s(repltok, sizeof(repltok), "shared_buffers = %dMB", (n_buffers * (BLCKSZ / 1024)) / 1024);
    else
        nRet = sprintf_s(repltok, sizeof(repltok), "shared_buffers = %dkB", n_buffers * (BLCKSZ / 1024));
    securec_check_ss_c(nRet, "\0", "\0");
    conflines = replace_token(conflines, "#shared_buffers = 32MB", repltok);

#if DEF_PGPORT != 5432
    nRet = sprintf_s(repltok, sizeof(repltok), "#port = %d", DEF_PGPORT);
    securec_check_ss_c(nRet, "\0", "\0");
    conflines = replace_token(conflines, "#port = 5432", repltok);
#endif

    buf_messages = escape_quotes(lc_messages);
    nRet = sprintf_s(repltok, sizeof(repltok), "lc_messages = '%s'", buf_messages);
    securec_check_ss_c(nRet, "\0", "\0");
    conflines = replace_token(conflines, "#lc_messages = 'C'", repltok);

    buf_monetary = escape_quotes(lc_monetary);
    nRet = sprintf_s(repltok, sizeof(repltok), "lc_monetary = '%s'", buf_monetary);
    securec_check_ss_c(nRet, "\0", "\0");
    conflines = replace_token(conflines, "#lc_monetary = 'C'", repltok);

    buf_numeric = escape_quotes(lc_numeric);
    nRet = sprintf_s(repltok, sizeof(repltok), "lc_numeric = '%s'", buf_numeric);
    securec_check_ss_c(nRet, "\0", "\0");
    conflines = replace_token(conflines, "#lc_numeric = 'C'", repltok);

    buf_time = escape_quotes(lc_time);
    nRet = sprintf_s(repltok, sizeof(repltok), "lc_time = '%s'", buf_time);
    securec_check_ss_c(nRet, "\0", "\0");
    conflines = replace_token(conflines, "#lc_time = 'C'", repltok);

    switch (locale_date_order(lc_time)) {
        case DATEORDER_YMD:
            rc = strcpy_s(repltok, (TZ_STRLEN_MAX + 100), "datestyle = 'iso, ymd'");
            break;
        case DATEORDER_DMY:
            rc = strcpy_s(repltok, (TZ_STRLEN_MAX + 100), "datestyle = 'iso, dmy'");
            break;
        case DATEORDER_MDY:
        default:
            rc = strcpy_s(repltok, (TZ_STRLEN_MAX + 100), "datestyle = 'iso, mdy'");
            break;
    }
    securec_check_c(rc, "\0", "\0");

    conflines = replace_token(conflines, "#datestyle = 'iso, mdy'", repltok);

    buf_default_text_search_config = escape_quotes(default_text_search_config);
    nRet = sprintf_s(
        repltok, sizeof(repltok), "default_text_search_config = 'pg_catalog.%s'", buf_default_text_search_config);
    securec_check_ss_c(nRet, "\0", "\0");
    conflines = replace_token(conflines, "#default_text_search_config = 'pg_catalog.simple'", repltok);
#ifdef PGXC
    /* Add openGauss node name to configuration file */
    buf_nodename = escape_quotes(nodename);

    nRet = sprintf_s(repltok, sizeof(repltok), "pgxc_node_name = '%s'", buf_nodename);
    securec_check_ss_c(nRet, "\0", "\0");
    conflines = replace_token(conflines, "#pgxc_node_name = ''", repltok);
#endif

    default_timezone = select_default_timezone(share_path);
    if (default_timezone != NULL) {
        buf_default_timezone = escape_quotes(default_timezone);
        nRet = sprintf_s(repltok, sizeof(repltok), "timezone = '%s'", buf_default_timezone);
        securec_check_ss_c(nRet, "\0", "\0");
        conflines = replace_token(conflines, "#timezone = 'GMT'", repltok);
        nRet = sprintf_s(repltok, sizeof(repltok), "log_timezone = '%s'", buf_default_timezone);
        securec_check_ss_c(nRet, "\0", "\0");
        conflines = replace_token(conflines, "#log_timezone = 'GMT'", repltok);
    }

#ifndef ENABLE_MULTIPLE_NODES
    if (enableDCF) {
        nRet = strcpy_s(repltok, sizeof(repltok), "enable_dcf = on");
        securec_check_ss_c(nRet, "\0", "\0");
        conflines = replace_token(conflines, "#enable_dcf = off", repltok);
    }
#endif

    if (strlen(new_xlog_file_path) != 0) {
        char* buf_xlog_file_path = NULL;
        buf_xlog_file_path = escape_quotes(new_xlog_file_path);
        nRet = sprintf_s(repltok, sizeof(repltok), "xlog_file_path = '%s'\n", buf_xlog_file_path);
        securec_check_ss_c(nRet, "\0", "\0");
        conflines = append_token(conflines, repltok);
        FREE_AND_RESET(buf_xlog_file_path);
    }

    nRet = sprintf_s(path, sizeof(path), "%s/postgresql.conf", pg_data);
    securec_check_ss_c(nRet, "\0", "\0");

    writefile(path, conflines);
    (void)chmod(path, S_IRUSR | S_IWUSR);

    FREE_AND_RESET(conflines);

#ifdef ENABLE_MOT
    /* mot.conf */

    conflines = readfile(mot_conf_file);
    nRet = sprintf_s(path, sizeof(path), "%s/mot.conf", pg_data);
    securec_check_ss_c(nRet, "\0", "\0");

    writefile(path, conflines);
    (void)chmod(path, S_IRUSR | S_IWUSR);

    FREE_AND_RESET(conflines);
#endif

    /* gs_gazelle.conf */

    conflines = readfile(gazelle_conf_file);
    nRet = sprintf_s(path, sizeof(path), "%s/gs_gazelle.conf", pg_data);
    securec_check_ss_c(nRet, "\0", "\0");

    writefile(path, conflines);
    (void)chmod(path, S_IRUSR | S_IWUSR);

    FREE_AND_RESET(conflines);

    /* pg_hba.conf */

    conflines = readfile(hba_file);

#ifndef HAVE_UNIX_SOCKETS
    conflines = filter_lines_with_token(conflines, "@remove-line-for-nolocal@");
#else
    conflines = replace_token(conflines, "@remove-line-for-nolocal@", "");
#endif

#ifdef HAVE_IPV6

    /*
     * Probe to see if there is really any platform support for IPv6, and
     * comment out the relevant pg_hba line if not.  This avoids runtime
     * warnings if getaddrinfo doesn't actually cope with IPv6.  Particularly
     * useful on Windows, where executables built on a machine with IPv6 may
     * have to run on a machine without.
     */
    {
        struct addrinfo* gai_result = NULL;
        struct addrinfo hints;
        int err = 0;

#ifdef WIN32
        /* need to call WSAStartup before calling getaddrinfo */
        WSADATA wsaData;

        err = WSAStartup(MAKEWORD(2, 2), &wsaData);
#endif

        /* for best results, this code should match parse_hba() */
        hints.ai_flags = AI_NUMERICHOST;
        hints.ai_family = PF_UNSPEC;
        hints.ai_socktype = 0;
        hints.ai_protocol = 0;
        hints.ai_addrlen = 0;
        hints.ai_canonname = NULL;
        hints.ai_addr = NULL;
        hints.ai_next = NULL;

        if (err != 0 || getaddrinfo("::1", NULL, &hints, &gai_result) != 0)
            conflines = replace_token(conflines,
                "host    all             all             ::1",
                "#host    all             all             ::1");

        freeaddrinfo(gai_result);
    }
#else  /* !HAVE_IPV6 */
    /* If we didn't compile IPV6 support at all, always comment it out */
    conflines = replace_token(
        conflines, "host    all             all             ::1", "#host    all             all             ::1");
#endif /* HAVE_IPV6 */

    /* Replace default authentication methods */
    conflines = replace_token(conflines, "@authmethodhost@", authmethodhost);
    conflines = replace_token(conflines, "@authmethodlocal@", authmethodlocal);

    conflines = replace_token(conflines,
        "@authcomment@",
        (strcmp(authmethodlocal, "trust") == 0 || strcmp(authmethodhost, "trust") == 0) ? AUTHTRUST_WARNING : "");

    /* Replace username for replication */
    conflines = replace_token(conflines, "@default_username@", username);

    nRet = sprintf_s(path, sizeof(path), "%s/pg_hba.conf", pg_data);
    securec_check_ss_c(nRet, "\0", "\0");

    writefile(path, conflines);
    (void)chmod(path, S_IRUSR | S_IWUSR);

    FREE_AND_RESET(conflines);

    /* pg_ident.conf */

    conflines = readfile(ident_file);

    nRet = snprintf_s(path, sizeof(path), sizeof(path) - 1, "%s/pg_ident.conf", pg_data);
    securec_check_ss_c(nRet, "\0", "\0");

    writefile(path, conflines);
    (void)chmod(path, S_IRUSR | S_IWUSR);

    FREE_AND_RESET(conflines);
    FREE_AND_RESET(buf_messages);
    FREE_AND_RESET(buf_monetary);
    FREE_AND_RESET(buf_numeric);
    FREE_AND_RESET(buf_time);
    FREE_AND_RESET(buf_default_text_search_config);
    FREE_AND_RESET(buf_nodename);
    FREE_AND_RESET(buf_default_timezone);

    check_ok();
}

/*
 * run the BKI script in bootstrap mode to create template1
 */
static void bootstrap_template1(void)
{
    PG_CMD_DECL;
    char** line;
    char* talkargs = "";
    char** bki_lines;
    char headerline[MAXPGPATH];
    char buf[BUF_LENGTH];
    int nRet = 0;
    char* buf_collate = NULL;
    char* buf_ctype = NULL;

    printf(_("creating template1 database in %s/base/1 ... "), pg_data);

    (void)fflush(stdout);

    if (debug)
        talkargs = "-d 5";

    bki_lines = readfile(bki_file);

    /* Check that bki file appears to be of the right version */

    nRet = snprintf_s(headerline, sizeof(headerline), sizeof(headerline) - 1, "# PostgreSQL %s\n", PG_MAJORVERSION);
    securec_check_ss_c(nRet, "\0", "\0");

    if (strcmp(headerline, *bki_lines) != 0) {
        write_stderr(_("%s: input file \"%s\" does not belong to PostgreSQL %s\n"
                       "Check your installation or specify the correct path "
                       "using the option -L.\n"),
            progname,
            bki_file,
            PG_VERSION);
        exit_nicely();
    }

    /* Substitute for various symbols used in the BKI file */

    nRet = sprintf_s(buf, BUF_LENGTH, "%d", NAMEDATALEN);
    securec_check_ss_c(nRet, "\0", "\0");
    bki_lines = replace_token(bki_lines, "NAMEDATALEN", buf);

    nRet = sprintf_s(buf, BUF_LENGTH, "%d", (int)sizeof(Pointer));
    securec_check_ss_c(nRet, "\0", "\0");
    bki_lines = replace_token(bki_lines, "SIZEOF_POINTER", buf);

    bki_lines = replace_token(bki_lines, "ALIGNOF_POINTER", (sizeof(Pointer) == 4) ? "i" : "d");

    bki_lines = replace_token(bki_lines, "FLOAT4PASSBYVAL", FLOAT4PASSBYVAL ? "true" : "false");

    bki_lines = replace_token(bki_lines, "FLOAT8PASSBYVAL", FLOAT8PASSBYVAL ? "true" : "false");

    bki_lines = replace_token(bki_lines, "POSTGRES", username);

    bki_lines = replace_token(bki_lines, "ENCODING", encodingid);

    buf_collate = escape_quotes(lc_collate);
    bki_lines = replace_token(bki_lines, "LC_COLLATE", buf_collate);
    FREE_AND_RESET(buf_collate);

    buf_ctype = escape_quotes(lc_ctype);
    bki_lines = replace_token(bki_lines, "LC_CTYPE", buf_ctype);
    FREE_AND_RESET(buf_ctype);

    /*
     * "--dbcompatibility" is a required argument to set installed database compatibility.
     * We can choose --dbcompatibility=A\B\C.
     * If we do not assign compatibility, B is default for distribution, A is default for single.
     * Invalid input is avoided.
     */
    if (pg_strcasecmp(dbcompatibility, g_dbCompatArray[DB_CMPT_A].name) == 0 ||
        pg_strcasecmp(dbcompatibility, g_dbCompatArray[DB_CMPT_B].name) == 0 ||
        pg_strcasecmp(dbcompatibility, g_dbCompatArray[DB_CMPT_C].name) == 0 ||
        pg_strcasecmp(dbcompatibility, g_dbCompatArray[DB_CMPT_PG].name) == 0) {
        bki_lines = replace_token(bki_lines, "DB_COMPATIBILITY", dbcompatibility);
    } else if (strlen(dbcompatibility) == 0) {
        /* If we do not specify database compatibility, set B defaultly for distribution, A defaultly for single. */
#ifdef ENABLE_MULTIPLE_NODES
        bki_lines = replace_token(bki_lines, "DB_COMPATIBILITY", g_dbCompatArray[DB_CMPT_B].name);
#else
        bki_lines = replace_token(bki_lines, "DB_COMPATIBILITY", g_dbCompatArray[DB_CMPT_A].name);
#endif
    } else {
        write_stderr(_("dbcompatibility \"%s\" is invalid\n"), dbcompatibility);
        exit_nicely();
    }

    /*
     * Pass correct LC_xxx environment to bootstrap.
     *
     * The shell script arranged to restore the LC settings afterwards, but
     * there doesn't seem to be any compelling reason to do that.
     */
    nRet = snprintf_s(cmd, sizeof(cmd), sizeof(cmd) - 1, "LC_COLLATE=%s", lc_collate);
    securec_check_ss_c(nRet, "\0", "\0");
    (void)putenv(xstrdup(cmd));

    nRet = snprintf_s(cmd, sizeof(cmd), sizeof(cmd) - 1, "LC_CTYPE=%s", lc_ctype);
    securec_check_ss_c(nRet, "\0", "\0");
    (void)putenv(xstrdup(cmd));

    (void)unsetenv("LC_ALL");

    /* Also ensure backend isn't confused by this environment var: */
    (void)unsetenv("PGCLIENTENCODING");

    nRet = snprintf_s(
        cmd, sizeof(cmd), sizeof(cmd) - 1, "\"%s\" --boot -x1 %s  %s 2>&1", backend_exec, boot_options, talkargs);
    securec_check_ss_c(nRet, "\0", "\0");

    PG_CMD_OPEN;

    for (line = bki_lines; *line != NULL; line++) {
        PG_CMD_PUTS(*line);
        FREE_AND_RESET(*line);
    }

    PG_CMD_CLOSE;

    FREE_AND_RESET(bki_lines);

    check_ok();
}

/*
 * set up the shadow password table
 */
static void setup_auth(void)
{
    PG_CMD_DECL;
    const char** line;
    int nRet = 0;
    static const char* pg_authid_setup[] = {/*
                                             * The authid table shouldn't be readable except through views, to
                                             * ensure passwords are not publicly visible.
                                             */
        "REVOKE ALL on pg_authid FROM public;\n",
        NULL};

    fputs(_("initializing pg_authid ... "), stdout);
    (void)fflush(stdout);

    nRet = snprintf_s(
        cmd, sizeof(cmd), sizeof(cmd) - 1, "\"%s\" %s template1 >%s 2>&1", backend_exec, backend_options, DEVNULL);
    securec_check_ss_c(nRet, "\0", "\0");

    PG_CMD_OPEN;

    for (line = pg_authid_setup; *line != NULL; line++)
        PG_CMD_PUTS(*line);

    PG_CMD_CLOSE;

    check_ok();
}

/*
 * get the superuser password if required, and call openGauss to set it
 */
static void get_set_pwd(void)
{
    PG_CMD_DECL;

    char* pwd1 = NULL;
    char* pwd2 = NULL;
    int i = 0;
    int nRet = 0;
    errno_t rc = 0;
    char* buf_pwd1 = NULL;
    if (pwfilename != NULL) {
        /*
         * Read password from file
         *
         * Ideally this should insist that the file not be world-readable.
         * However, this option is mainly intended for use on Windows where
         * file permissions may not exist at all, so we'll skip the paranoia
         * for now.
         */
        FILE* pwf = fopen(pwfilename, "r");
        char pwdbuf[MAXPGPATH];

        if (pwf == NULL) {
            char errBuffer[ERROR_LIMIT_LEN];
            write_stderr(_("%s: could not open file \"%s\" for reading: %s\n"),
                progname,
                pwfilename,
                pqStrerror(errno, errBuffer, ERROR_LIMIT_LEN));
            exit_nicely();
        }
        if (fgets(pwdbuf, sizeof(pwdbuf), pwf) == NULL) {
            if (ferror(pwf)) {
                char errBuffer[ERROR_LIMIT_LEN];
                write_stderr(_("%s: could not read password from file \"%s\": %s\n"),
                    progname,
                    pwfilename,
                    pqStrerror(errno, errBuffer, ERROR_LIMIT_LEN));
            } else
                write_stderr(_("%s: password file \"%s\" is empty\n"), progname, pwfilename);
            fclose(pwf);
            exit_nicely();
        }
        fclose(pwf);

        i = strlen(pwdbuf);
        while (i > 0 && (pwdbuf[i - 1] == '\r' || pwdbuf[i - 1] == '\n'))
            pwdbuf[--i] = '\0';

        pwd1 = xstrdup(pwdbuf);

    } else if (pwprompt) {
        /* else get password from readline */
        for (i = 0; i < 3; i++) {
            pwd1 = simple_prompt("Enter new system admin password: ", 1024, false);
            pwd2 = simple_prompt("Enter it again: ", 1024, false);

            /* if pwd1 not equal to pwd2, try new password again */
            if (pwd1 != NULL && pwd2 != NULL && strcmp(pwd1, pwd2) != 0) {
                write_stderr(_("Passwords didn't match.\n"));
                write_stderr(_("Please try new password again.\n"));

                rc = memset_s(pwd1, strlen(pwd1), 0, strlen(pwd1));
                securec_check_c(rc, "\0", "\0");
                FREE_AND_RESET(pwd1);

                rc = memset_s(pwd2, strlen(pwd2), 0, strlen(pwd2));
                securec_check_c(rc, "\0", "\0");
                FREE_AND_RESET(pwd2);

                continue;
            }

            /* Clear password related memory to avoid leaks when core. */
            if (pwd2 != NULL) {
                rc = memset_s(pwd2, strlen(pwd2), 0, strlen(pwd2));
                securec_check_c(rc, "\0", "\0");
                FREE_AND_RESET(pwd2);
            }

            /* if the pwd1 does not match, try it again*/
            if (CheckInitialPasswd(username, pwd1)) {
                break;
            } else if (pwd1 != NULL) {
                rc = memset_s(pwd1, strlen(pwd1), 0, strlen(pwd1));
                securec_check_c(rc, "\0", "\0");
                FREE_AND_RESET(pwd1);
            }
        }
        
        if (i == 3) {
            exit_nicely();
        }
    } else {
    /* if pwpasswd is not NULL, use it as password for the new superuser */
        if (pwpasswd != NULL) {
            pwd1 = pwpasswd;
            if (!CheckInitialPasswd(username, pwd1)) {
                exit_nicely();
            }
        }
    }

    printf(_("setting password ... "));
    (void)fflush(stdout);

    nRet = snprintf_s(
        cmd, sizeof(cmd), sizeof(cmd) - 1, "\"%s\" %s template1 >%s 2>&1", backend_exec, backend_options, DEVNULL);
    securec_check_ss_c(nRet, "\0", "\0");

    PG_CMD_OPEN;

    if (pwd1 != NULL) {
        buf_pwd1 = escape_quotes(pwd1);
        PG_CMD_PRINTF2("ALTER USER \"%s\" WITH PASSWORD E'%s';\n", username, buf_pwd1);
        rc = memset_s(buf_pwd1, strlen(buf_pwd1), 0, strlen(buf_pwd1));
        securec_check_c(rc, "\0", "\0");
        FREE_AND_RESET(buf_pwd1);
        /* Clear password related memory to avoid leaks when core. */
        rc = memset_s(pwd1, strlen(pwd1), 0, strlen(pwd1));
        securec_check_c(rc, "\0", "\0");
        FREE_AND_RESET(pwd1);
    }
    PG_CMD_CLOSE;

    check_ok();
}

/*
 * set up pg_depend
 */
static void setup_depend(void)
{
    PG_CMD_DECL;
    const char** line;
    int nRet = 0;
    static const char* pg_depend_setup[] = {/*
                                             * Make PIN entries in pg_depend for all objects made so far in the
                                             * tables that the dependency code handles.  This is overkill (the
                                             * system doesn't really depend on having every last weird datatype,
                                             * for instance) but generating only the minimum required set of
                                             * dependencies seems hard.
                                             *
                                             * Note that we deliberately do not pin the system views, which
                                             * haven't been created yet.  Also, no conversions, databases, or
                                             * tablespaces are pinned.
                                             *
                                             * First delete any already-made entries; PINs override all else, and
                                             * must be the only entries for their objects.
                                             */
        "DELETE FROM pg_depend;\n",
        "VACUUM pg_depend;\n",
        "DELETE FROM pg_shdepend;\n",
        "VACUUM pg_shdepend;\n",

        "INSERT INTO pg_depend SELECT 0,0,0, tableoid,oid,0, 'p' "
        " FROM pg_class;\n",
        "INSERT INTO pg_depend SELECT 0,0,0, tableoid,oid,0, 'p' "
        " FROM pg_type;\n",
        "INSERT INTO pg_depend SELECT 0,0,0, tableoid,oid,0, 'p' "
        " FROM pg_cast;\n",
        "INSERT INTO pg_depend SELECT 0,0,0, tableoid,oid,0, 'p' "
        " FROM pg_constraint;\n",
        "INSERT INTO pg_depend SELECT 0,0,0, tableoid,oid,0, 'p' "
        " FROM pg_attrdef;\n",
        "INSERT INTO pg_depend SELECT 0,0,0, tableoid,oid,0, 'p' "
        " FROM pg_language;\n",
        "INSERT INTO pg_depend SELECT 0,0,0, tableoid,oid,0, 'p' "
        " FROM pg_operator;\n",
        "INSERT INTO pg_depend SELECT 0,0,0, tableoid,oid,0, 'p' "
        " FROM pg_opclass;\n",
        "INSERT INTO pg_depend SELECT 0,0,0, tableoid,oid,0, 'p' "
        " FROM pg_opfamily;\n",
        "INSERT INTO pg_depend SELECT 0,0,0, tableoid,oid,0, 'p' "
        " FROM pg_amop;\n",
        "INSERT INTO pg_depend SELECT 0,0,0, tableoid,oid,0, 'p' "
        " FROM pg_amproc;\n",
        "INSERT INTO pg_depend SELECT 0,0,0, tableoid,oid,0, 'p' "
        " FROM pg_rewrite;\n",
        "INSERT INTO pg_depend SELECT 0,0,0, tableoid,oid,0, 'p' "
        " FROM pg_trigger;\n",

        /*
         * restriction here to avoid pinning the public namespace
         */
        "INSERT INTO pg_depend SELECT 0,0,0, tableoid,oid,0, 'p' "
        " FROM pg_namespace "
        "    WHERE nspname LIKE 'pg%' or nspname = 'cstore';\n",

        "INSERT INTO pg_depend SELECT 0,0,0, tableoid,oid,0, 'p' "
        " FROM pg_ts_parser;\n",
        "INSERT INTO pg_depend SELECT 0,0,0, tableoid,oid,0, 'p' "
        " FROM pg_ts_dict;\n",
        "INSERT INTO pg_depend SELECT 0,0,0, tableoid,oid,0, 'p' "
        " FROM pg_ts_template;\n",
        "INSERT INTO pg_depend SELECT 0,0,0, tableoid,oid,0, 'p' "
        " FROM pg_ts_config;\n",
        "INSERT INTO pg_depend SELECT 0,0,0, tableoid,oid,0, 'p' "
        " FROM pg_collation;\n",
        "INSERT INTO pg_shdepend SELECT 0,0,0,0, tableoid,oid, 'p' "
        " FROM pg_authid;\n",
        NULL};

    fputs(_("initializing dependencies ... "), stdout);
    (void)fflush(stdout);

    nRet = snprintf_s(
        cmd, sizeof(cmd), sizeof(cmd) - 1, "\"%s\" %s template1 >%s 2>&1", backend_exec, backend_options, DEVNULL);
    securec_check_ss_c(nRet, "\0", "\0");

    PG_CMD_OPEN;

    for (line = pg_depend_setup; *line != NULL; line++)
        PG_CMD_PUTS(*line);

    PG_CMD_CLOSE;

    check_ok();
}

/*
 * set up system views
 */
static void setup_sysviews(void)
{
    PG_CMD_DECL;
    char** line;
    char** sysviews_setup;
    int nRet = 0;

    fputs(_("creating system views ... "), stdout);
    (void)fflush(stdout);

    sysviews_setup = readfile(system_views_file);

    /*
     * We use -j here to avoid backslashing stuff in system_views.sql
     */
    nRet = snprintf_s(
        cmd, sizeof(cmd), sizeof(cmd) - 1, "\"%s\" %s -j template1 >%s 2>&1", backend_exec, backend_options, DEVNULL);
    securec_check_ss_c(nRet, "\0", "\0");

    PG_CMD_OPEN;

    for (line = sysviews_setup; *line != NULL; line++) {
        PG_CMD_PUTS(*line);
        FREE_AND_RESET(*line);
    }

    PG_CMD_CLOSE;

    FREE_AND_RESET(sysviews_setup);

    check_ok();
}

/*
 * set up performance views
 */
static void setup_perfviews(void)
{
    PG_CMD_DECL;
    char** line;
    char** perfviews_setup;
    int nRet = 0;

    fputs(_("creating performance views ... "), stdout);
    (void)fflush(stdout);

    perfviews_setup = readfile(performance_views_file);

    /*
     * We use -j here to avoid backslashing stuff in performance_views.sql
     */
    nRet = snprintf_s(
        cmd, sizeof(cmd), sizeof(cmd) - 1, "\"%s\" %s -j template1 >%s 2>&1", backend_exec, backend_options, DEVNULL);
    securec_check_ss_c(nRet, "\0", "\0");

    PG_CMD_OPEN;

    for (line = perfviews_setup; *line != NULL; line++) {
        PG_CMD_PUTS(*line);
        FREE_AND_RESET(*line);
    }

    PG_CMD_CLOSE;

    FREE_AND_RESET(perfviews_setup);

    check_ok();
}

#ifdef ENABLE_PRIVATEGAUSS
/*
 * set up private system views
 */
static void setup_privsysviews(void)
{
    PG_CMD_DECL;
    char** line;
    char** privsysviews_setup;
    int nRet = 0;

    fputs(_("creating private system views ... "), stdout);
    (void)fflush(stdout);

    privsysviews_setup = readfile(private_system_views_file);

    /*
     * We use -j here to avoid backslashing stuff in system_views.sql
     */
    nRet = snprintf_s(
        cmd, sizeof(cmd), sizeof(cmd) - 1, "\"%s\" %s -j template1 >%s 2>&1", backend_exec, backend_options, DEVNULL);
    securec_check_ss_c(nRet, "\0", "\0");

    PG_CMD_OPEN;

    for (line = privsysviews_setup; *line != NULL; line++) {
        PG_CMD_PUTS(*line);
        FREE_AND_RESET(*line);
    }

    PG_CMD_CLOSE;

    FREE_AND_RESET(privsysviews_setup);

    check_ok();
}
#endif

/*
 * set up snapshots
 */
#ifndef ENABLE_MULTIPLE_NODES
static void setup_snapshots(void)
{
    PG_CMD_DECL;
    char** line;
    char** current_setup;
    int nRet = 0;

    fputs(_("creating snapshots catalog ... "), stdout);
    (void)fflush(stdout);

    for (unsigned i = 0; i < SNAPSHOT_LEN; i++)
    {
        current_setup = readfile(snapshot_files[i]);

        nRet = snprintf_s(
            cmd, sizeof(cmd), sizeof(cmd) - 1, "\"%s\" %s -j template1 >%s 2>&1",
            backend_exec, backend_options, DEVNULL);
        securec_check_ss_c(nRet, "\0", "\0");

        PG_CMD_OPEN;

        for (line = current_setup; *line != NULL; line++) {
            PG_CMD_PUTS(*line);
            FREE_AND_RESET(*line);
        }

        PG_CMD_CLOSE;

        FREE_AND_RESET(current_setup);
    }

    check_ok();
}
#endif

/* * update system tables as we needed */
static void setup_update(void)
{
    PG_CMD_DECL;
    char pg_cast_char[PG_CAST_CHAR_LENGTH] = {0};
    int nRet = 0;

    fputs(_("update system tables ... "), stdout);
    (void)fflush(stdout);

    nRet = snprintf_s(
        cmd, sizeof(cmd), sizeof(cmd) - 1, "\"%s\" %s -j template1 >%s 2>&1", backend_exec, backend_options, DEVNULL);
    securec_check_ss_c(nRet, "\0", "\0");

    PG_CMD_OPEN;

    /*
     * Add new context into update_systbl.sql to update system tables.
     */
    nRet = sprintf_s(pg_cast_char,
        PG_CAST_CHAR_LENGTH,
        "\n COPY pg_cast FROM '%s%s' USING DELIMITERS '|';",
        share_path,
        "/pg_cast_oid.txt");
    securec_check_ss_c(nRet, "\0", "\0");
    PG_CMD_PUTS(pg_cast_char);

    PG_CMD_CLOSE;

    check_ok();
}

#ifdef PGXC
/*
 * set up Postgres-XC cluster node catalog data with node self
 * which is the node currently initialized.
 */
static void setup_nodeself(void)
{
#ifndef ENABLE_MULTIPLE_NODES
    return;
#endif
    PG_CMD_DECL;
    int nRet = 0;

    fputs(_("creating cluster information ... "), stdout);
    (void)fflush(stdout);

    nRet = snprintf_s(
        cmd, sizeof(cmd), sizeof(cmd) - 1, "\"%s\" %s template1 >%s 2>&1", backend_exec, backend_options, DEVNULL);
    securec_check_ss_c(nRet, "\0", "\0");

    PG_CMD_OPEN;

    if (strcmp(host_ip, "") != 0) {
        PG_CMD_PRINTF2("CREATE NODE %s WITH (type = 'coordinator', host = '%s');\n", nodename, host_ip);
    } else {
        PG_CMD_PRINTF1("CREATE NODE %s WITH (type = 'coordinator');\n", nodename);
    }

    PG_CMD_CLOSE;

    check_ok();
}

static bool is_valid_nodename(const char* nodename)
{
    /*
     * The node name must contain lowercase letters (a-z), underscores (_),
     * special characters #, digits (0-9), or dollar ($).
     * 
     * The node name must start with a lowercase letter (a-z), or an underscore (_).
     * 
     * The max length of nodename is 64.
     */
    int len = strlen(nodename);
    if (len <= 0 || len > PGXC_NODENAME_LENGTH) {
        return false;
    }

    for (int i = 0; i < len; i++) {
        char c = nodename[i];
        if (c == '_' || (c >= 'a' && c <= 'z')) {
            continue;
        }

        if (i == 0) {
            return false;
        }

        if ((c >= '0' && c <= '9') || c == '#' || c == '$') {
            continue;
        }

        return false;
    }
    return true;
}
#endif

/*
 * load description data
 */
static void setup_description(void)
{
    PG_CMD_DECL;
    int nRet = 0;
    char* buf_file = NULL;

    fputs(_("loading system objects' descriptions ... "), stdout);
    (void)fflush(stdout);

    nRet = snprintf_s(
        cmd, sizeof(cmd), sizeof(cmd) - 1, "\"%s\" %s template1 >%s 2>&1", backend_exec, backend_options, DEVNULL);
    securec_check_ss_c(nRet, "\0", "\0");

    PG_CMD_OPEN;

    PG_CMD_PUTS("CREATE TEMP TABLE tmp_pg_description ( "
                "	objoid oid, "
                "	classname name, "
                "	objsubid int4, "
                "	description text) WITHOUT OIDS;\n");

    buf_file = escape_quotes(desc_file);
    PG_CMD_PRINTF1("COPY tmp_pg_description FROM E'%s';\n", buf_file);
    FREE_AND_RESET(buf_file);

    PG_CMD_PUTS("INSERT INTO pg_description "
                " SELECT t.objoid, c.oid, t.objsubid, t.description "
                "  FROM tmp_pg_description t, pg_class c "
                "    WHERE c.relname = t.classname;\n");

    PG_CMD_PUTS("CREATE TEMP TABLE tmp_pg_shdescription ( "
                " objoid oid, "
                " classname name, "
                " description text) WITHOUT OIDS;\n");

    buf_file = escape_quotes(shdesc_file);
    PG_CMD_PRINTF1("COPY tmp_pg_shdescription FROM E'%s';\n", buf_file);
    FREE_AND_RESET(buf_file);

    PG_CMD_PUTS("INSERT INTO pg_shdescription "
                " SELECT t.objoid, c.oid, t.description "
                "  FROM tmp_pg_shdescription t, pg_class c "
                "   WHERE c.relname = t.classname;\n");

    /* Create default descriptions for operator implementation functions */
    PG_CMD_PUTS("WITH funcdescs AS ( "
                "SELECT p.oid as p_oid, oprname, "
                "coalesce(obj_description(o.oid, 'pg_operator'),'') as opdesc "
                "FROM pg_proc p JOIN pg_operator o ON oprcode = p.oid ) "
                "INSERT INTO pg_description "
                "  SELECT p_oid, 'pg_proc'::regclass, 0, "
                "    'implementation of ' || oprname || ' operator' "
                "  FROM funcdescs "
                "  WHERE opdesc NOT LIKE 'deprecated%' AND "
                "  NOT EXISTS (SELECT 1 FROM pg_description "
                "    WHERE objoid = p_oid AND classoid = 'pg_proc'::regclass);\n");

    PG_CMD_CLOSE;

    check_ok();
}

#ifdef HAVE_LOCALE_T
/*
 * "Normalize" a locale name, stripping off encoding tags such as
 * ".utf8" (e.g., "en_US.utf8" -> "en_US", but "br_FR.iso885915@euro"
 * -> "br_FR@euro").  Return true if a new, different name was
 * generated.
 */
static bool normalize_locale_name(char* newm, const char* old)
{
    char* n = newm;
    const char* o = old;
    bool changed = false;

    while (*o) {
        if (*o == '.') {
            /* skip over encoding tag such as ".utf8" or ".UTF-8" */
            o++;
            while ((*o >= 'A' && *o <= 'Z') || (*o >= 'a' && *o <= 'z') || (*o >= '0' && *o <= '9') || (*o == '-'))
                o++;
            changed = true;
        } else
            *n++ = *o++;
    }
    *n = '\0';

    return changed;
}
#endif /* HAVE_LOCALE_T */

/*
 * populate pg_collation
 */
static void setup_collation(void)
{
#if defined(HAVE_LOCALE_T) && !defined(WIN32)
    int i;
    FILE* locale_a_handle = NULL;
    char localebuf[NAMEDATALEN];
    int count = 0;
    int nRet = 0;

    PG_CMD_DECL;
#endif

    fputs(_("creating collations ... "), stdout);
    (void)fflush(stdout);

#if defined(HAVE_LOCALE_T) && !defined(WIN32)
    nRet = snprintf_s(
        cmd, sizeof(cmd), sizeof(cmd) - 1, "\"%s\" %s template1 >%s 2>&1", backend_exec, backend_options, DEVNULL);
    securec_check_ss_c(nRet, "\0", "\0");

    locale_a_handle = popen_check("locale -a", "r");
    if (locale_a_handle == NULL)
        return; /* complaint already printed */

    PG_CMD_OPEN;

    PG_CMD_PUTS("CREATE TEMP TABLE tmp_pg_collation ( "
                "	collname name, "
                "	locale name, "
                "	encoding int) WITHOUT OIDS;\n");

    while (fgets(localebuf, sizeof(localebuf), locale_a_handle) != NULL) {
        size_t len;
        int enc;
        char* quoted_locale = NULL;
        char alias[NAMEDATALEN];

        len = strlen(localebuf);

        if (len == 0 || localebuf[len - 1] != '\n') {
            if (debug)
                write_stderr(_("%s: locale name too long, skipped: \"%s\"\n"), progname, localebuf);
            continue;
        }
        localebuf[len - 1] = '\0';

        /*
         * Some systems have locale names that don't consist entirely of ASCII
         * letters (such as "bokm&aring;l" or "fran&ccedil;ais").  This is
         * pretty silly, since we need the locale itself to interpret the
         * non-ASCII characters. We can't do much with those, so we filter
         * them out.
         */
        bool skip = false;
        for (i = 0; i < (int)len; i++) {
            if (IS_HIGHBIT_SET(localebuf[i])) {
                skip = true;
                break;
            }
        }
        if (skip) {
            if (debug)
                write_stderr(_("%s: locale name has non-ASCII characters, skipped: \"%s\"\n"), progname, localebuf);
            continue;
        }

        enc = pg_get_encoding_from_locale(localebuf, debug);
        if (enc < 0) {
            /* error message printed by pg_get_encoding_from_locale() */
            continue;
        }
        if (!PG_VALID_BE_ENCODING(enc))
            continue; /* ignore locales for client-only encodings */
        if (enc == PG_SQL_ASCII)
            continue; /* C/POSIX are already in the catalog */

        count++;

        quoted_locale = escape_quotes(localebuf);

        PG_CMD_PRINTF3("INSERT INTO tmp_pg_collation VALUES (E'%s', E'%s', %d);\n", quoted_locale, quoted_locale, enc);

        /*
         * Generate aliases such as "en_US" in addition to "en_US.utf8" for
         * ease of use.  Note that collation names are unique per encoding
         * only, so this doesn't clash with "en_US" for LATIN1, say.
         */
        if (normalize_locale_name(alias, localebuf)) {
            char* buf_alias = NULL;

            buf_alias = escape_quotes(alias);
            PG_CMD_PRINTF3("INSERT INTO tmp_pg_collation VALUES (E'%s', E'%s', %d);\n", buf_alias, quoted_locale, enc);
            FREE_AND_RESET(buf_alias);
        }

        FREE_AND_RESET(quoted_locale);
    }

    /* Add an SQL-standard name */
    PG_CMD_PRINTF1("INSERT INTO tmp_pg_collation VALUES ('ucs_basic', 'C', %d);\n", PG_UTF8);

    /*
     * When copying collations to the final location, eliminate aliases that
     * conflict with an existing locale name for the same encoding.  For
     * example, "br_FR.iso88591" is normalized to "br_FR", both for encoding
     * LATIN1.	But the unnormalized locale "br_FR" already exists for LATIN1.
     * Prefer the alias that matches the OS locale name, else the first locale
     * name by sort order (arbitrary choice to be deterministic).
     *
     * Also, eliminate any aliases that conflict with pg_collation's
     * hard-wired entries for "C" etc.
     */
    PG_CMD_PUTS("INSERT INTO pg_collation (collname, collnamespace, collowner, collencoding, collcollate, collctype) "
                " SELECT DISTINCT ON (collname, encoding)"
                "   collname, "
                "   (SELECT oid FROM pg_namespace WHERE nspname = 'pg_catalog') AS collnamespace, "
                "   (SELECT relowner FROM pg_class WHERE relname = 'pg_collation') AS collowner, "
                "   encoding, locale, locale "
                "  FROM tmp_pg_collation"
                "  WHERE NOT EXISTS (SELECT 1 FROM pg_collation WHERE collname = tmp_pg_collation.collname)"
                "  ORDER BY collname, encoding, (collname = locale) DESC, locale;\n");

    (void)pclose(locale_a_handle);
    PG_CMD_CLOSE;

    check_ok();
    if (count == 0 && !debug) {
        printf(_("No usable system locales were found.\n"));
        printf(_("Use the option \"--debug\" to see details.\n"));
    }
#else  /* not HAVE_LOCALE_T && not WIN32 */
    printf(_("not supported on this platform\n"));
    fflush(stdout);
#endif /* not HAVE_LOCALE_T  && not WIN32 */
}

/*
 * load conversion functions
 */
static void setup_conversion(void)
{
    PG_CMD_DECL;
    char** line;
    char** conv_lines;
    int nRet = 0;

    fputs(_("creating conversions ... "), stdout);
    (void)fflush(stdout);

    nRet = snprintf_s(
        cmd, sizeof(cmd), sizeof(cmd) - 1, "\"%s\" %s template1 >%s 2>&1", backend_exec, backend_options, DEVNULL);
    securec_check_ss_c(nRet, "\0", "\0");

    PG_CMD_OPEN;

    conv_lines = readfile(conversion_file);
    for (line = conv_lines; *line != NULL; line++) {
        if (strstr(*line, "DROP CONVERSION") != *line)
            PG_CMD_PUTS(*line);
        FREE_AND_RESET(*line);
    }

    FREE_AND_RESET(conv_lines);

    PG_CMD_CLOSE;

    check_ok();
}

/*
 * load extra dictionaries (Snowball stemmers)
 */
static void setup_dictionary(void)
{
    PG_CMD_DECL;
    char** line;
    char** conv_lines;
    int nRet = 0;

    fputs(_("creating dictionaries ... "), stdout);
    (void)fflush(stdout);

    /*
     * We use -j here to avoid backslashing stuff
     */
    nRet = snprintf_s(
        cmd, sizeof(cmd), sizeof(cmd) - 1, "\"%s\" %s -j template1 >%s 2>&1", backend_exec, backend_options, DEVNULL);
    securec_check_ss_c(nRet, "\0", "\0");

    PG_CMD_OPEN;

    conv_lines = readfile(dictionary_file);
    for (line = conv_lines; *line != NULL; line++) {
        PG_CMD_PUTS(*line);
        FREE_AND_RESET(*line);
    }

    FREE_AND_RESET(conv_lines);

    PG_CMD_CLOSE;

    check_ok();
}

/*
 * Set up privileges
 *
 * We mark most system catalogs as world-readable.	We don't currently have
 * to touch functions, languages, or databases, because their default
 * permissions are OK.
 *
 * Some objects may require different permissions by default, so we
 * make sure we don't overwrite privilege sets that have already been
 * set (NOT NULL).
 */
static void setup_privileges(void)
{
    PG_CMD_DECL;
    char** line;
    char** priv_lines;
    static char** privileges_setup = (char**)pg_malloc(sizeof(char*) * 20);
    int nRet = 0;

    privileges_setup[0] = xstrdup("UPDATE pg_class "
                                 "  SET relacl = E'{\"=r/\\\\\"$POSTGRES_SUPERUSERNAME\\\\\"\"}' "
                                 "  WHERE relkind IN ('r', 'v', 'm', 'S') AND relacl IS NULL;\n");
    privileges_setup[1] = xstrdup("GRANT USAGE ON SCHEMA pg_catalog TO PUBLIC;\n");
    privileges_setup[2] = xstrdup("GRANT CREATE, USAGE ON SCHEMA public TO PUBLIC;\n");
    privileges_setup[3] = xstrdup("REVOKE ALL ON pg_largeobject FROM PUBLIC;\n");
    privileges_setup[4] = xstrdup("REVOKE ALL on pg_user_status FROM public;\n");
    privileges_setup[5] = xstrdup("REVOKE ALL on pg_auth_history FROM public;\n");
    privileges_setup[6] = xstrdup("REVOKE ALL on pg_extension_data_source FROM public;\n");
    privileges_setup[7] = NULL;
    privileges_setup[8] = xstrdup("REVOKE ALL on gs_auditing_policy FROM public;\n");
    privileges_setup[9] = xstrdup("REVOKE ALL on gs_auditing_policy_access FROM public;\n");
    privileges_setup[10] = xstrdup("REVOKE ALL on gs_auditing_policy_filters FROM public;\n");
    privileges_setup[11] = xstrdup("REVOKE ALL on gs_auditing_policy_privileges FROM public;\n");
    privileges_setup[12] = xstrdup("REVOKE ALL on gs_policy_label FROM public;\n");
    privileges_setup[13] = xstrdup("REVOKE ALL on gs_masking_policy FROM public;\n");
    privileges_setup[14] = xstrdup("REVOKE ALL on gs_masking_policy_actions FROM public;\n");
    privileges_setup[15] = xstrdup("REVOKE ALL on gs_masking_policy_filters FROM public;\n");
    privileges_setup[16] = xstrdup("GRANT USAGE ON SCHEMA sqladvisor TO PUBLIC;\n");
    privileges_setup[17] = xstrdup("GRANT USAGE ON SCHEMA dbe_pldebugger TO PUBLIC;\n");
    privileges_setup[18] = xstrdup("GRANT USAGE ON SCHEMA dbe_pldeveloper TO PUBLIC;\n");
    privileges_setup[19] = NULL;
    /* In security mode, we will revoke privilege of public on schema public. */
    if (security) {
        privileges_setup[7] = xstrdup("REVOKE ALL on schema public FROM public;\n");
    } else {
        privileges_setup[7] = xstrdup("REVOKE CREATE on schema public FROM public;\n");
    }

    fputs(_("setting privileges on built-in objects ... "), stdout);
    (void)fflush(stdout);

    nRet = snprintf_s(
        cmd, sizeof(cmd), sizeof(cmd) - 1, "\"%s\" %s template1 >%s 2>&1", backend_exec, backend_options, DEVNULL);
    securec_check_ss_c(nRet, "\0", "\0");

    PG_CMD_OPEN;

    priv_lines = replace_token(privileges_setup, "$POSTGRES_SUPERUSERNAME", username);
    for (line = priv_lines; *line != NULL; line++) {
        PG_CMD_PUTS(*line);
        FREE_AND_RESET(*line);
    }

    PG_CMD_CLOSE;

    FREE_AND_RESET(priv_lines);

    check_ok();
}

/*
 * extract the strange version of version required for information schema
 * (09.08.0007abc)
 */
static void set_info_version(void)
{
    char* letterversion = NULL;
    long major = 0, minor = 0, micro = 0;
    char* endptr = NULL;
    char* vstr = xstrdup(PG_VERSION);
    char* ptr = NULL;
    int nRet = 0;

    ptr = vstr + (strlen(vstr) - 1);
    while (ptr != vstr && (*ptr < '0' || *ptr > '9'))
        ptr--;
    letterversion = ptr + 1;
    major = strtol(vstr, &endptr, 10);
    if (*endptr)
        minor = strtol(endptr + 1, &endptr, 10);
    if (*endptr)
        micro = strtol(endptr + 1, &endptr, 10);
    nRet = snprintf_s(infoversion,
        INFO_VERSION_LENGTH,
        INFO_VERSION_LENGTH - 1,
        "%02ld.%02ld.%04ld%s",
        major,
        minor,
        micro,
        letterversion);
    securec_check_ss_c(nRet, "\0", "\0");

    FREE_AND_RESET(vstr);
}

/*
 * load info schema and populate from features file
 */
static void setup_schema(void)
{
    PG_CMD_DECL;
    char** line;
    char** lines;
    int nRet = 0;
    char* buf_features = NULL;

    fputs(_("creating information schema ... "), stdout);
    (void)fflush(stdout);

    lines = readfile(info_schema_file);

    /*
     * We use -j here to avoid backslashing stuff in information_schema.sql
     */
    nRet = snprintf_s(
        cmd, sizeof(cmd), sizeof(cmd) - 1, "\"%s\" %s -j template1 >%s 2>&1", backend_exec, backend_options, DEVNULL);
    securec_check_ss_c(nRet, "\0", "\0");

    PG_CMD_OPEN;

    for (line = lines; *line != NULL; line++) {
        PG_CMD_PUTS(*line);
        FREE_AND_RESET(*line);
    }

    FREE_AND_RESET(lines);

    PG_CMD_CLOSE;

    nRet = snprintf_s(
        cmd, sizeof(cmd), sizeof(cmd) - 1, "\"%s\" %s template1 >%s 2>&1", backend_exec, backend_options, DEVNULL);
    securec_check_ss_c(nRet, "\0", "\0");

    PG_CMD_OPEN;

    PG_CMD_PRINTF1("UPDATE information_schema.sql_implementation_info "
                   "  SET character_value = '%s' "
                   "  WHERE implementation_info_name = 'DBMS VERSION';\n",
        infoversion);

    buf_features = escape_quotes(features_file);
    PG_CMD_PRINTF1("COPY information_schema.sql_features "
                   "  (feature_id, feature_name, sub_feature_id, "
                   "  sub_feature_name, is_supported, comments) "
                   " FROM E'%s';\n",
        buf_features);
    FREE_AND_RESET(buf_features);

    PG_CMD_CLOSE;

    /*
     * "mnt" schema is set up to hold tables created for internal maintenance purpose.
     * Use the schema with the following rules:
     * 1. no initdb table in mnt. Table has to be created by user, which means
     *	its Oid > 16384 and it comes with valid "distribute by" option.
     * 2. User are allowed to create their own tables too in mnt.
     * 3. gs_dump, gs_restore, and roach are not to ignore/skip this schema
     *	in their functionalities.
     */

    /*

    Notice: The following code was commented out for the following reason:
    1. Adding a schema is never a easy job. OM and CM must make sync changes. And pushing them
        to make those changes requires tedious negotiation and certain amount of efforts and
        pacience.
    2. Various issues with upgrade and expansion might come with it, such as but not limited to:
        (1) There will always be a risk since users can use any name for their own schemas. If
             the name overlap, upgrade fails.
        (2) Expansion (dump/restore) needs to either dump all or none of the infomation in this
             schema, otherwise expansion yields the wrong results and the whole cluster became
             degraded.
        (3) Access control.
        (4) etc.

    so good luck.
    */

    check_ok();
}

/*
 * Initialize length of bucketmap
 */
static void setup_bucketmap_len(void)
{
    PG_CMD_DECL;
    int nRet = 0;
    char sql[INSERT_BUCKET_SQL_LEN] = { 0 };

    fputs(_("initialize global configure for bucketmap length ... "), stdout);
    (void)fflush(stdout);

    nRet = snprintf_s(
        cmd, sizeof(cmd), sizeof(cmd) - 1, "\"%s\" %s template1 >%s 2>&1", backend_exec, backend_options, DEVNULL);
    securec_check_ss_c(nRet, "\0", "\0");

    nRet = sprintf_s(sql, sizeof(sql),
               "INSERT INTO gs_global_config VALUES ('buckets_len', '%d');\n",
               g_bucket_len);
    securec_check_ss_c(nRet, "\0", "\0");

    PG_CMD_OPEN;

    PG_CMD_PUTS(sql);

    PG_CMD_CLOSE;

    check_ok();
}

/*
 * load PL/pgsql server-side language
 */
static void load_plpgsql(void)
{
    PG_CMD_DECL;
    int nRet = 0;

    fputs(_("loading PL/pgSQL server-side language ... "), stdout);
    (void)fflush(stdout);

    nRet = snprintf_s(
        cmd, sizeof(cmd), sizeof(cmd) - 1, "\"%s\" %s template1 >%s 2>&1", backend_exec, backend_options, DEVNULL);
    securec_check_ss_c(nRet, "\0", "\0");

    PG_CMD_OPEN;

    PG_CMD_PUTS("CREATE EXTENSION plpgsql;\n");

    PG_CMD_CLOSE;

    check_ok();
}

static void load_dist_fdw(void)
{
    PG_CMD_DECL;
    int nRet = 0;

    fputs(_("loading foreign-data wrapper for distfs access ... "), stdout);
    (void)fflush(stdout);

    nRet = snprintf_s(
        cmd, sizeof(cmd), sizeof(cmd) - 1, "\"%s\" %s template1 >%s 2>&1", backend_exec, backend_options, DEVNULL);
    securec_check_ss_c(nRet, "\0", "\0");

    PG_CMD_OPEN;

    PG_CMD_PUTS("CREATE EXTENSION dist_fdw;\n");

    PG_CMD_PUTS("CREATE SERVER gsmpp_server FOREIGN DATA WRAPPER dist_fdw;\n");

    PG_CMD_PUTS("CREATE EXTENSION file_fdw;\n");

    PG_CMD_PUTS("CREATE SERVER gsmpp_errorinfo_server FOREIGN DATA WRAPPER file_fdw;\n");

#ifdef ENABLE_MULTIPLE_NODES
    PG_CMD_PUTS("CREATE EXTENSION roach_api;\n");
#endif

    PG_CMD_CLOSE;

    check_ok();
}

/*@hdfs
 *brief: Loading foreign-data wrapper for hdfs
 */
static void load_hdfs_fdw(void)
{
    int nRet = 0;
    PG_CMD_DECL;

    fputs(_("loading foreign-data wrapper for hdfs access ... "), stdout);
    (void)fflush(stdout);

    nRet = snprintf_s(
        cmd, sizeof(cmd), sizeof(cmd) - 1, "\"%s\" %s template1 >%s 2>&1", backend_exec, backend_options, DEVNULL);
    securec_check_ss_c(nRet, "\0", "\0");

    PG_CMD_OPEN;

    PG_CMD_PUTS("CREATE EXTENSION hdfs_fdw;\n");

    PG_CMD_CLOSE;

    check_ok();
}

#ifdef ENABLE_MULTIPLE_NODES
static void load_gc_fdw(void)
{
    int nRet = 0;
    PG_CMD_DECL;

    fputs(_("loading foreign-data wrapper for gc access ... "), stdout);
    (void)fflush(stdout);

    nRet = snprintf_s(
        cmd, sizeof(cmd), sizeof(cmd) - 1, "\"%s\" %s template1 >%s 2>&1", backend_exec, backend_options, DEVNULL);
    securec_check_ss_c(nRet, "\0", "\0");

    PG_CMD_OPEN;

    PG_CMD_PUTS("CREATE EXTENSION gc_fdw;\n");

    PG_CMD_CLOSE;

    check_ok();
}
#endif

#if ((defined(ENABLE_MULTIPLE_NODES)) || (defined(ENABLE_PRIVATEGAUSS)))
static void load_packages_extension(void)
{
    PG_CMD_DECL;
    int nRet = 0;

    fputs(_("loading packages extension ... "), stdout);
    (void)fflush(stdout);

    nRet = snprintf_s(
        cmd, sizeof(cmd), sizeof(cmd) - 1, "\"%s\" %s template1 >%s 2>&1", backend_exec, backend_options, DEVNULL);
    securec_check_ss_c(nRet, "\0", "\0");

    PG_CMD_OPEN;

    PG_CMD_PUTS("CREATE EXTENSION packages;\nALTER EXTENSION packages UPDATE TO '1.1';\n");

    PG_CMD_CLOSE;

    check_ok();   
}
#endif

#ifdef ENABLE_MULTIPLE_NODES
/*
 * the gsredistribute extenstion used in OM command gs_expand, it will revoke kernel command gs_redis,
 * some function used in gs_redis, some functions for tsdb used in gs_expand, you can use 
 * drop extension gsredistribute cascade;create extension if not exists gsredistribute; 
 * in upgrade sql file to upgrade it, add by upgrade version 20205
 */
static void load_gsredistribute_extension(void)
{
    PG_CMD_DECL;
    int nRet = 0;

    fputs(_("loading gsredistribute extension ... "), stdout);
    (void)fflush(stdout);

    nRet = snprintf_s(
        cmd, sizeof(cmd), sizeof(cmd) - 1, "\"%s\" %s template1 >%s 2>&1", backend_exec, backend_options, DEVNULL);
    securec_check_ss_c(nRet, "\0", "\0");

    PG_CMD_OPEN;

    PG_CMD_PUTS("CREATE EXTENSION gsredistribute;\n");

    PG_CMD_CLOSE;

    check_ok();   
}

/*
 * load simsearch api for the  scene of gpu acceleration.
 */
static void load_searchserver_extension()
{
    PG_CMD_DECL;
    int nRet = 0;

    fputs(_("loading dimsearch extension ... "), stdout);
    (void)fflush(stdout);

    nRet = snprintf_s(
        cmd, sizeof(cmd), sizeof(cmd) - 1, "\"%s\" %s template1 >%s 2>&1", backend_exec, backend_options, DEVNULL);
    securec_check_ss_c(nRet, "\0", "\0");

    PG_CMD_OPEN;

    PG_CMD_PUTS("CREATE EXTENSION dimsearch;\n");

    PG_CMD_CLOSE;

    check_ok();
}

static void load_tsdb_extension(void)
{
    PG_CMD_DECL;
    int nRet = 0;

    fputs(_("loading tsdb extension ... "), stdout);
    (void)fflush(stdout);

    nRet = snprintf_s(
        cmd, sizeof(cmd), sizeof(cmd) - 1, "\"%s\" %s template1 >%s 2>&1", backend_exec, backend_options, DEVNULL);
    securec_check_ss_c(nRet, "\0", "\0");

    PG_CMD_OPEN;

    PG_CMD_PUTS("CREATE EXTENSION tsdb;\n");

    PG_CMD_CLOSE;

    check_ok();
}

static void load_streaming_extension(void)
{
    PG_CMD_DECL;
    int nRet = 0;

    fputs(_("loading streaming extension ... "), stdout);
    (void)fflush(stdout);

    nRet = snprintf_s(
        cmd, sizeof(cmd), sizeof(cmd) - 1, "\"%s\" %s template1 >%s 2>&1", backend_exec, backend_options, DEVNULL);
    securec_check_ss_c(nRet, "\0", "\0");

    PG_CMD_OPEN;

    PG_CMD_PUTS("CREATE EXTENSION streaming;\n");

    PG_CMD_CLOSE;

    check_ok();
}
#endif

#ifdef ENABLE_MOT
static void load_mot_fdw(void)
{
    int nRet = 0;
    PG_CMD_DECL;

    fputs(_("loading foreign-data wrapper for MOT access ... "), stdout);
    (void)fflush(stdout);

    nRet = snprintf_s(
        cmd, sizeof(cmd), sizeof(cmd) - 1, "\"%s\" %s template1 >%s 2>&1", backend_exec, backend_options, DEVNULL);
    securec_check_ss_c(nRet, "\0", "\0");

    PG_CMD_OPEN;

    PG_CMD_PUTS("CREATE EXTENSION mot_fdw;\n");

    PG_CMD_PUTS("CREATE SERVER mot_server FOREIGN DATA WRAPPER mot_fdw;\n");

    PG_CMD_CLOSE;

    check_ok();
}
#endif

static void load_hstore_extension(void)
{
    PG_CMD_DECL;
    int nRet = 0;

    fputs(_("loading hstore extension ... "), stdout);
    (void)fflush(stdout);

    nRet = snprintf_s(
        cmd, sizeof(cmd), sizeof(cmd) - 1, "\"%s\" %s template1 >%s 2>&1", backend_exec, backend_options, DEVNULL);
    securec_check_ss_c(nRet, "\0", "\0");

    PG_CMD_OPEN;

    PG_CMD_PUTS("CREATE EXTENSION hstore;\n");

    PG_CMD_CLOSE;

    check_ok();
}

static void load_log_extension(void)
{
    fputs(_("loading foreign-data wrapper for log access ... "), stdout);
    (void)fflush(stdout);

    PG_CMD_DECL;
    int nRet = snprintf_s(
        cmd, sizeof(cmd), sizeof(cmd) - 1, "\"%s\" %s template1 >%s 2>&1", backend_exec, backend_options, DEVNULL);
    securec_check_ss_c(nRet, "\0", "\0");

    PG_CMD_OPEN;
    PG_CMD_PUTS("CREATE EXTENSION log_fdw;\n");
    PG_CMD_CLOSE;
    check_ok();
}

static void load_security_plugin()
{
    fputs(_("loading security plugin ... "), stdout);
    (void)fflush(stdout);

    PG_CMD_DECL;
    int nRet = snprintf_s(
        cmd, sizeof(cmd), sizeof(cmd) - 1, "\"%s\" %s template1 >%s 2>&1", backend_exec, backend_options, DEVNULL);
    securec_check_ss_c(nRet, "\0", "\0");

    PG_CMD_OPEN;
    PG_CMD_PUTS("CREATE EXTENSION security_plugin;\n");
    PG_CMD_CLOSE;
    check_ok();
}

static void load_supported_extension(void)
{
    /* loading foreign-data wrapper for distfs access */
    load_dist_fdw();
    /* loading foreign-data wrapper for hdfs access */
    load_hdfs_fdw();

    /* loading foreign-data wrapper for openGauss access */
#ifdef ENABLE_MULTIPLE_NODES
    load_gc_fdw();
#endif

#if ((defined(ENABLE_MULTIPLE_NODES)) || (defined(ENABLE_PRIVATEGAUSS)))    
    /* loading packages extension */
    load_packages_extension();
#endif

#ifdef ENABLE_MULTIPLE_NODES

    load_gsredistribute_extension();

    load_searchserver_extension();

    load_tsdb_extension();

    load_streaming_extension();
#endif

    /* loading foreign-data wrapper for log access */
    load_log_extension();

    /* loading hstore extension */
    load_hstore_extension();

#ifdef ENABLE_MOT
    /* loading foreign-data wrapper for mot in-memory data access */
    load_mot_fdw();
#endif

    /* load security policy extension */
    load_security_plugin();
}

#ifdef PGXC
/*
 * Vacuum Freeze given database.
 */
static void vacuumfreeze(const char* dbname)
{
    PG_CMD_DECL;
    char msg[MAXPGPATH] = {0};
    int nRet = 0;
    nRet = sprintf_s(msg, sizeof(msg), "freezing database %s ... ", dbname);
    securec_check_ss_c(nRet, "\0", "\0");

    fputs(_(msg), stdout);
    (void)fflush(stdout);

    nRet = snprintf_s(
        cmd, sizeof(cmd), sizeof(cmd) - 1, "\"%s\" %s %s >%s 2>&1", backend_exec, backend_options, dbname, DEVNULL);
    securec_check_ss_c(nRet, "\0", "\0");

    PG_CMD_OPEN;

    PG_CMD_PUTS("VACUUM FREEZE;\n");

    PG_CMD_CLOSE;

    check_ok();
}
#endif /* PGXC */

/*
 * clean everything up in template1
 */
static void vacuum_db(void)
{
    PG_CMD_DECL;
    int nRet = 0;

    fputs(_("vacuuming database template1 ... "), stdout);
    (void)fflush(stdout);

    nRet = snprintf_s(
        cmd, sizeof(cmd), sizeof(cmd) - 1, "\"%s\" %s template1 >%s 2>&1", backend_exec, backend_options, DEVNULL);
    securec_check_ss_c(nRet, "\0", "\0");

    PG_CMD_OPEN;

    PG_CMD_PUTS("ANALYZE;\nVACUUM FULL;\nVACUUM FREEZE;\n");

    PG_CMD_CLOSE;

    check_ok();
}

/*
 * copy template1 to template0
 */
static void make_template0(void)
{
    PG_CMD_DECL;
    const char** line;
    int nRet = 0;
    static const char* template0_setup[] = {"CREATE DATABASE template0;\n",
        "UPDATE pg_database SET "
        "	datistemplate = 't', "
        "	datallowconn = 'f' "
        "    WHERE datname = 'template0';\n",

        /*
         * We use the OID of template0 to determine lastsysoid
         */
        "UPDATE pg_database SET datlastsysoid = "
        "    (SELECT oid FROM pg_database "
        "    WHERE datname = 'template0');\n",

        /*
         * Explicitly revoke public create-schema and create-temp-table
         * privileges in template1 and template0; else the latter would be on
         * by default
         */
        "REVOKE CREATE,TEMPORARY ON DATABASE template1 FROM public;\n",
        "REVOKE CREATE,TEMPORARY ON DATABASE template0 FROM public;\n",

        "COMMENT ON DATABASE template0 IS 'default template for new databases';\n",

        /*
         * Finally vacuum to clean up dead rows in pg_database
         */
        "VACUUM FULL pg_database;\n",
        NULL};

    fputs(_("copying template1 to template0 ... "), stdout);
    (void)fflush(stdout);

    nRet = snprintf_s(
        cmd, sizeof(cmd), sizeof(cmd) - 1, "\"%s\" %s template1 >%s 2>&1", backend_exec, backend_options, DEVNULL);
    securec_check_ss_c(nRet, "\0", "\0");

    PG_CMD_OPEN;

    for (line = template0_setup; *line != NULL; line++)
        PG_CMD_PUTS(*line);

    PG_CMD_CLOSE;

    check_ok();
}

/*
 * copy template1 to postgres
 */
static void make_postgres(void)
{
    PG_CMD_DECL;
    const char** line;
    int nRet = 0;
    static const char* postgres_setup[] = {"CREATE DATABASE postgres;\n",
        "COMMENT ON DATABASE postgres IS 'default administrative connection database';\n",
        NULL};

    fputs(_("copying template1 to postgres ... "), stdout);
    (void)fflush(stdout);

    nRet = snprintf_s(
        cmd, sizeof(cmd), sizeof(cmd) - 1, "\"%s\" %s template1 >%s 2>&1", backend_exec, backend_options, DEVNULL);
    securec_check_ss_c(nRet, "\0", "\0");

    PG_CMD_OPEN;

    for (line = postgres_setup; *line != NULL; line++)
        PG_CMD_PUTS(*line);

    PG_CMD_CLOSE;

    check_ok();
}

/*
 * signal handler in case we are interrupted.
 *
 * The Windows runtime docs at
 * http://msdn.microsoft.com/library/en-us/vclib/html/_crt_signal.asp
 * specifically forbid a number of things being done from a signal handler,
 * including IO, memory allocation and system calls, and only allow jmpbuf
 * if you are handling SIGFPE.
 *
 * I avoided doing the forbidden things by setting a flag instead of calling
 * exit_nicely() directly.
 *
 * Also note the behaviour of Windows with SIGINT, which says this:
 *	 Note	SIGINT is not supported for any Win32 application, including
 *	 Windows 98/Me and Windows NT/2000/XP. When a CTRL+C interrupt occurs,
 *	 Win32 operating systems generate a new thread to specifically handle
 *	 that interrupt. This can cause a single-thread application such as UNIX,
 *	 to become multithreaded, resulting in unexpected behavior.
 *
 * I have no idea how to handle this. (Strange they call UNIX an application!)
 * So this will need some testing on Windows.
 */
static void trapsig(int signum)
{
    /* handle systems that reset the handler, like Windows (grr) */
    (void)pqsignal(signum, trapsig);
    caught_signal = true;
}

/*
 * call exit_nicely() if we got a signal, or else output "ok".
 */
static void check_ok(void)
{
    if (caught_signal) {
        printf(_("caught signal\n"));
        (void)fflush(stdout);
        exit_nicely();
    } else if (output_failed) {
        char errBuffer[ERROR_LIMIT_LEN];
        printf(_("could not write to child process: %s\n"), pqStrerror(errno, errBuffer, ERROR_LIMIT_LEN));
        (void)fflush(stdout);
        exit_nicely();
    } else {
        /* all seems well */
        printf(_("ok\n"));
        (void)fflush(stdout);
    }
}

/*
 * Escape (by doubling) any single quotes or backslashes in given string
 *
 * Note: this is used to process both postgresql.conf entries and SQL
 * string literals.  Since postgresql.conf strings are defined to treat
 * backslashes as escapes, we have to double backslashes here.	Hence,
 * when using this for a SQL string literal, use E'' syntax.
 *
 * We do not need to worry about encoding considerations because all
 * valid backend encodings are ASCII-safe.
 */
static char* escape_quotes(const char* src)
{
    int len = strlen(src), i, j;
    char* result = (char*)pg_malloc(len * 2 + 1);

    for (i = 0, j = 0; i < len; i++) {
        if (SQL_STR_DOUBLE(src[i], true))
            result[j++] = src[i];
        result[j++] = src[i];
    }
    result[j] = '\0';
    return result;
}

/* Hack to suppress a warning about %x from some versions of gcc */
static inline size_t my_strftime(char* s, size_t max, const char* fmt, const struct tm* tm_val)
{
    return strftime(s, max, fmt, tm_val);
}

/*
 * Determine likely date order from locale
 */
static int locale_date_order(const char* locale_time)
{
    struct tm testtime;
    char buf[128];
    char* posD = NULL;
    char* posM = NULL;
    char* posY = NULL;
    char* save = NULL;
    size_t res = 0;
    int result;
    errno_t rc = 0;

    result = DATEORDER_MDY; /* default */

    save = setlocale(LC_TIME, NULL);
    if (save == NULL)
        return result;
    save = xstrdup(save);

    (void)setlocale(LC_TIME, locale_time);

    rc = memset_s(&testtime, sizeof(testtime), 0, sizeof(testtime));
    securec_check_c(rc, "\0", "\0");
    testtime.tm_mday = 22;
    testtime.tm_mon = 10;   /* November, should come out as "11" */
    testtime.tm_year = 133; /* 2033 */

    res = my_strftime(buf, sizeof(buf), "%x", &testtime);

    (void)setlocale(LC_TIME, save);
    FREE_AND_RESET(save);

    if (res == 0)
        return result;

    posM = strstr(buf, "11");
    posD = strstr(buf, "22");
    posY = strstr(buf, "33");

    if ((posM == NULL) || (posD == NULL) || (posY == NULL))
        return result;

    if (posY < posM && posM < posD)
        result = DATEORDER_YMD;
    else if (posD < posM)
        result = DATEORDER_DMY;
    else
        result = DATEORDER_MDY;

    return result;
}

/*
 * Verify that locale name is valid for the locale category.
 *
 * If successful, and canonname isn't NULL, a malloc'd copy of the locale's
 * canonical name is stored there.  This is especially useful for figuring out
 * what locale name "" means (ie, the environment value).  (Actually,
 * it seems that on most implementations that's the only thing it's good for;
 * we could wish that setlocale gave back a canonically spelled version of
 * the locale name, but typically it doesn't.)
 *
 * this should match the backend's check_locale() function
 */
static void check_locale_name(int category, const char* locale_name, char** canonname)
{
    char* save = NULL;
    char* res = NULL;

    if (canonname != NULL)
        *canonname = NULL; /* in case of failure */

    save = setlocale(category, NULL);
    if (save == NULL) {
        write_stderr(_("%s: setlocale() failed\n"), progname);
        exit(1);
    }

    /* save may be pointing at a modifiable scratch variable, so copy it. */
    save = xstrdup(save);

    /* set the locale with setlocale, to see if it accepts it. */
    res = setlocale(category, locale_name);

    /* save canonical name if requested. */
    if ((res != NULL) && (canonname != NULL))
        *canonname = xstrdup(res);

    /* restore old value. */
    if (setlocale(category, save) == NULL) {
        write_stderr(_("%s: failed to restore old locale \"%s\"\n"), progname, save);
        exit(1);
    }
    FREE_AND_RESET(save);

    /* should we exit here? */
    if (res == NULL) {
        if (*locale_name) {
            write_stderr(_("%s: invalid locale name \"%s\"\n"), progname, locale_name);
        } else {
            /*
             * If no relevant switch was given on command line, locale is an
             * empty string, which is not too helpful to report.  Presumably
             * setlocale() found something it did not like in the environment.
             * Ideally we'd report the bad environment variable, but since
             * setlocale's behavior is implementation-specific, it's hard to
             * be sure what it didn't like.  Print a safe generic message.
             */
            write_stderr(_("%s: invalid locale settings; check LANG and LC_* environment variables\n"), progname);
        }
        exit(1);
    }
}

/*
 * check if the chosen encoding matches the encoding required by the locale
 *
 * this should match the similar check in the backend createdb() function
 */
static bool check_locale_encoding(const char* locale_encoding, int user_enc)
{
    int locale_enc;

    locale_enc = pg_get_encoding_from_locale(locale_encoding, true);

    /* See notes in createdb() to understand these tests */
    if (!(locale_enc == user_enc || locale_enc == PG_SQL_ASCII || locale_enc == -1 ||
#ifdef WIN32
            user_enc == PG_UTF8 ||
#endif
            user_enc == PG_SQL_ASCII)) {
        write_stderr(_("%s: encoding mismatch\n"), progname);
        write_stderr(_("The encoding you selected (%s) and the encoding that the\n"
                       "selected locale uses (%s) do not match.  This would lead to\n"
                       "misbehavior in various character string processing functions.\n"
                       "Rerun %s and either do not specify an encoding explicitly,\n"
                       "or choose a matching combination.\n"),
            pg_encoding_to_char(user_enc),
            pg_encoding_to_char(locale_enc),
            progname);
        return false;
    }
    return true;
}

/*
 * set up the locale variables
 *
 * assumes we have called setlocale(LC_ALL, "") -- see set_pglocale_pgservice
 */
static void setlocales(void)
{
#define FREE_NOT_STATIC_STRING(s)                       \
    do {                                                     \
        if ((s) && (char*)(s) != (char*)"" && s != locale) { \
            free(s);                                         \
            (s) = NULL;                                      \
        }                                                    \
    } while (0)

    char* canonname = NULL;

    /* set empty lc_* values to locale config if set */

    if (strlen(locale) > 0) {
        if (strlen(lc_ctype) == 0)
            lc_ctype = locale;
        if (strlen(lc_collate) == 0)
            lc_collate = locale;
        if (strlen(lc_numeric) == 0)
            lc_numeric = locale;
        if (strlen(lc_time) == 0)
            lc_time = locale;
        if (strlen(lc_monetary) == 0)
            lc_monetary = locale;
        if (strlen(lc_messages) == 0)
            lc_messages = locale;
    }

    /*
     * canonicalize locale names, and override any missing/invalid values from
     * our current environment, should free memory before setting new address
     */

    check_locale_name(LC_CTYPE, lc_ctype, &canonname);
    FREE_NOT_STATIC_STRING(lc_ctype);
    lc_ctype = canonname;
    check_locale_name(LC_COLLATE, lc_collate, &canonname);
    FREE_NOT_STATIC_STRING(lc_collate);
    lc_collate = canonname;
    check_locale_name(LC_NUMERIC, lc_numeric, &canonname);
    FREE_NOT_STATIC_STRING(lc_numeric);
    lc_numeric = canonname;
    check_locale_name(LC_TIME, lc_time, &canonname);
    FREE_NOT_STATIC_STRING(lc_time);
    lc_time = canonname;
    check_locale_name(LC_MONETARY, lc_monetary, &canonname);
    FREE_NOT_STATIC_STRING(lc_monetary);
    lc_monetary = canonname;
#if defined(LC_MESSAGES) && !defined(WIN32)
    check_locale_name(LC_MESSAGES, lc_messages, &canonname);
    FREE_NOT_STATIC_STRING(lc_messages);
    lc_messages = canonname;
#else
    /* when LC_MESSAGES is not available, use the LC_CTYPE setting */
    check_locale_name(LC_CTYPE, lc_messages, &canonname);
    FREE_NOT_STATIC_STRING(lc_messages);
    lc_messages = canonname;
#endif
}

#ifdef WIN32
typedef BOOL(WINAPI* __CreateRestrictedToken)(
    HANDLE, DWORD, DWORD, PSID_AND_ATTRIBUTES, DWORD, PLUID_AND_ATTRIBUTES, DWORD, PSID_AND_ATTRIBUTES, PHANDLE);

/* Windows API define missing from some versions of MingW headers */
#ifndef DISABLE_MAX_PRIVILEGE
#define DISABLE_MAX_PRIVILEGE 0x1
#endif

/*
 * Create a restricted token and execute the specified process with it.
 *
 * Returns 0 on failure, non-zero on success, same as CreateProcess().
 *
 * On NT4, or any other system not containing the required functions, will
 * NOT execute anything.
 */
static int CreateRestrictedProcess(char* cmd, PROCESS_INFORMATION* processInfo)
{
    BOOL b;
    STARTUPINFO si;
    HANDLE origToken = NULL;
    HANDLE restrictedToken = NULL;
    SID_IDENTIFIER_AUTHORITY NtAuthority = {SECURITY_NT_AUTHORITY};
    SID_AND_ATTRIBUTES dropSids[2];
    __CreateRestrictedToken _CreateRestrictedToken = NULL;
    HANDLE Advapi32Handle = NULL;

    ZeroMemory(&si, sizeof(si));
    si.cb = sizeof(si);

    Advapi32Handle = LoadLibrary("ADVAPI32.DLL");
    if (Advapi32Handle != NULL) {
        _CreateRestrictedToken = (__CreateRestrictedToken)GetProcAddress(Advapi32Handle, "CreateRestrictedToken");
    }

    if (_CreateRestrictedToken == NULL) {
        write_stderr(_("%s: WARNING: cannot create restricted tokens on this platform\n"), progname);
        if (Advapi32Handle != NULL)
            FreeLibrary(Advapi32Handle);
        return 0;
    }

    /* Open the current token to use as a base for the restricted one */
    if (!OpenProcessToken(GetCurrentProcess(), TOKEN_ALL_ACCESS, &origToken)) {
        write_stderr(_("%s: could not open process token: error code %lu\n"), progname, GetLastError());
        return 0;
    }

    /* Allocate list of SIDs to remove */
    ZeroMemory(&dropSids, sizeof(dropSids));
    if (!AllocateAndInitializeSid(&NtAuthority,
            2,
            SECURITY_BUILTIN_DOMAIN_RID,
            DOMAIN_ALIAS_RID_ADMINS,
            0,
            0,
            0,
            0,
            0,
            0,
            &dropSids[0].Sid) ||
        !AllocateAndInitializeSid(&NtAuthority,
            2,
            SECURITY_BUILTIN_DOMAIN_RID,
            DOMAIN_ALIAS_RID_POWER_USERS,
            0,
            0,
            0,
            0,
            0,
            0,
            &dropSids[1].Sid)) {
        write_stderr(_("%s: could not allocate SIDs: error code %lu\n"), progname, GetLastError());
        return 0;
    }

    b = _CreateRestrictedToken(origToken,
        DISABLE_MAX_PRIVILEGE,
        sizeof(dropSids) / sizeof(dropSids[0]),
        dropSids,
        0,
        NULL,
        0,
        NULL,
        &restrictedToken);

    FreeSid(dropSids[1].Sid);
    FreeSid(dropSids[0].Sid);
    CloseHandle(origToken);
    FreeLibrary(Advapi32Handle);

    if (!b) {
        write_stderr(_("%s: could not create restricted token: error code %lu\n"), progname, GetLastError());
        return 0;
    }

#ifndef __CYGWIN__
    AddUserToTokenDacl(restrictedToken);
#endif

    if (!CreateProcessAsUser(
            restrictedToken, NULL, cmd, NULL, NULL, TRUE, CREATE_SUSPENDED, NULL, NULL, &si, processInfo))

    {
        write_stderr(
            _("%s: could not start process for command \"%s\": error code %lu\n"), progname, cmd, GetLastError());
        return 0;
    }

    return ResumeThread(processInfo->hThread);
}
#endif

/*
 * print help text
 */
static void usage(const char* prog_name)
{
    printf(_("%s initializes a openGauss database cluster.\n\n"), prog_name);
    printf(_("Usage:\n"));
    printf(_("  %s [OPTION]... [DATADIR]\n"), prog_name);
    printf(_("\nOptions:\n"));
    printf(_("  -A, --auth=METHOD         default authentication method for local connections\n"));
    printf(_("      --auth-host=METHOD    default authentication method for local TCP/IP connections\n"));
    printf(_("      --auth-local=METHOD   default authentication method for local-socket connections\n"));
#ifndef ENABLE_MULTIPLE_NODES
    printf(_("  -c, --enable-dcf          enable DCF mode\n"));
#endif
    printf(_(" [-D, --pgdata=]DATADIR     location for this database cluster\n"));
#ifdef ENABLE_MULTIPLE_NODES
    printf(_("      --nodename=NODENAME   name of openGauss node initialized\n"));
    printf(_("      --bucketlength=LENGTH length of bucketmap\n"));
#else
    printf(_("      --nodename=NODENAME   name of single node initialized\n"));
#endif
    printf(_("  -E, --encoding=ENCODING   set default encoding for new databases\n"));
    printf(_("      --locale=LOCALE       set default locale for new databases\n"));
    printf(_("      --dbcompatibility=DBCOMPATIBILITY   set default dbcompatibility for new database\n"));
    printf(_("      --lc-collate=, --lc-ctype=, --lc-messages=LOCALE\n"
             "      --lc-monetary=, --lc-numeric=, --lc-time=LOCALE\n"
             "                            set default locale in the respective category for\n"
             "                            new databases (default taken from environment)\n"));
    printf(_("      --no-locale           equivalent to --locale=C\n"));
    printf(_("      --pwfile=FILE         read password for the new system admin from file\n"));
    printf(_("  -T, --text-search-config=CFG\n"
             "                            default text search configuration\n"));
    printf(_("  -U, --username=NAME       database system admin name\n"));
    printf(_("  -W, --pwprompt            prompt for a password for the new system admin\n"));
    printf(_("  -w, --pwpasswd=PASSWD     get password from command line for the new system admin\n"));
    printf(_("  -C, --enpwdfiledir=DIR    get encrypted password of AES128 from cipher and rand file\n"));
    printf(_("  -X, --xlogdir=XLOGDIR     location for the transaction log directory\n"));
    printf(_("  -S, --security            remove normal user's privilege on public schema in security mode\n"));
    printf(_("  -g, --xlogpath=XLOGPATH   xlog file path of shared storage\n"));
    printf(_("\nLess commonly used options:\n"));
    printf(_("  -d, --debug               generate lots of debugging output\n"));
    printf(_("  -L DIRECTORY              where to find the input files\n"));
    printf(_("  -n, --noclean             do not clean up after errors\n"));
    printf(_("  -s, --show                show internal settings\n"));
    printf(_("\nOther options:\n"));
    printf(_("  -H, --host-ip             node_host of openGauss node initialized\n"));
    printf(_("  -V, --version             output version information, then exit\n"));
    printf(_("  -?, --help                show this help, then exit\n"));
    printf(_("\nIf the data directory is not specified, the environment variable PGDATA\n"
             "is used.\n"));
#if ((defined(ENABLE_MULTIPLE_NODES)) || (defined(ENABLE_PRIVATEGAUSS)))
    printf(_("\nReport bugs to GaussDB support.\n"));
#else
    printf(_("\nReport bugs to community@opengauss.org> or join opengauss community <https://opengauss.org>.\n"));
#endif
}

static void check_authmethod_unspecified(const char** authmethod)
{
    if (*authmethod == NULL || strlen(*authmethod) == 0) {
        authwarning = _("\nWARNING: enabling \"trust\" authentication for local connections\n"
                        "You can change this by editing pg_hba.conf or using the option -A, or\n"
                        "--auth-local and --auth-host, the next time you run gs_initdb.\n");
        *authmethod = "trust";
    }
}

static void check_authmethod_valid(const char* authmethod, const char** valid_methods, const char* conntype)
{
    const char** p;

    for (p = valid_methods; *p != NULL; p++) {
        if (strcmp(authmethod, *p) == 0)
            return;
        /* with space = param */
        if (strchr(authmethod, ' ') != NULL)
            if (strncmp(authmethod, *p, (authmethod - strchr(authmethod, ' '))) == 0)
                return;
    }

    write_stderr(
        _("%s: invalid authentication method \"%s\" for \"%s\" connections\n"), progname, authmethod, conntype);
    exit(1);
}

static bool is_file_exist(const char* path)
{
    struct stat statbuf;
    bool isExist = true;

    if (lstat(path, &statbuf) < 0) {
        if (errno != ENOENT) {
            write_stderr(_("could not stat file \"%s\": %s\n"), path, strerror(errno));
            exit(1);
        }

        isExist = false;
    }
    return isExist;
}

int main(int argc, char* argv[])
{
    /*
     * options with no short version return a low integer, the rest return
     * their short version value
     */
    static struct option long_options[] = {{"pgdata", required_argument, NULL, 'D'},
        {"encoding", required_argument, NULL, 'E'},
        {"locale", required_argument, NULL, 1},
        {"lc-collate", required_argument, NULL, 2},
        {"lc-ctype", required_argument, NULL, 3},
        {"lc-monetary", required_argument, NULL, 4},
        {"lc-numeric", required_argument, NULL, 5},
        {"lc-time", required_argument, NULL, 6},
        {"lc-messages", required_argument, NULL, 7},
        {"no-locale", no_argument, NULL, 8},
        {"text-search-config", required_argument, NULL, 'T'},
        {"auth", required_argument, NULL, 'A'},
        {"auth-local", required_argument, NULL, 10},
        {"auth-host", required_argument, NULL, 11},
        {"pwprompt", no_argument, NULL, 'W'},
        {"pwpasswd", required_argument, NULL, 'w'},
        {"enpwdfiledir", required_argument, NULL, 'C'},
        {"pwfile", required_argument, NULL, 9},
        {"username", required_argument, NULL, 'U'},
        {"help", no_argument, NULL, '?'},
        {"version", no_argument, NULL, 'V'},
        {"debug", no_argument, NULL, 'd'},
        {"show", no_argument, NULL, 's'},
        {"noclean", no_argument, NULL, 'n'},
        {"xlogdir", required_argument, NULL, 'X'},
        {"security", no_argument, NULL, 'S'},
        {"host-ip", required_argument, NULL, 'H'},
#ifndef ENABLE_MULTIPLE_NODES
        {"enable-dcf", no_argument, NULL, 'c'},
#endif
#ifdef PGXC
        {"nodename", required_argument, NULL, 12},
#endif
        {"dbcompatibility", required_argument, NULL, 13},
        {"bucketlength", required_argument, NULL, 14},
        {NULL, 0, NULL, 0}};

    int c, i, ret;
    int option_index;
    char* effective_user = NULL;
    char* pgdenv = NULL; /* PGDATA value gotten from and sent to
                          * environment */
    char bin_dir[MAXPGPATH];
    char* pg_data_native = NULL;
    int user_enc;
    errno_t rc = 0;
    int nRet = 0;
    /*
     * As their source variables are const string, which can't be free,
     * here we create some temporary buffer to store the duplicated strings.
     */
    char* authmethodhost_tmp = NULL;
    char* authmethodlocal_tmp = NULL;
    char* default_text_search_config_tmp = NULL;
    char encrypt_pwd_real_path[PATH_MAX] = {0};
    char cipher_key_file[MAXPGPATH] = {0};
    char rand_file[MAXPGPATH] = {0};

#ifdef WIN32
    char* restrict_env = NULL;
#endif
    static const char* subdirs[] = {"global",
        "pg_xlog",
        "pg_xlog/archive_status",
        "pg_clog",
        "pg_csnlog",
        "pg_notify",
        "pg_serial",
        "pg_snapshots",
        "pg_twophase",
        "pg_multixact",
        "pg_multixact/members",
        "pg_multixact/offsets",
        "base",
        "base/1",
        "pg_replslot",
        "pg_tblspc",
        "pg_stat_tmp",
        "pg_llog",
        "pg_llog/snapshots",
        "pg_llog/mappings",
        "pg_errorinfo",
        "undo",
        "pg_logical"};

    check_input_spec_char(argv[0]);
    progname = get_progname(argv[0]);
    set_pglocale_pgservice(argv[0], PG_TEXTDOMAIN("gs_initdb"));

    if (argc > 1) {
        if (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-?") == 0) {
            usage(progname);
            exit(0);
        }
        if (strcmp(argv[1], "--version") == 0 || strcmp(argv[1], "-V") == 0) {
#ifdef ENABLE_MULTIPLE_NODES
            puts("gs_initdb " DEF_GS_VERSION);
#else
            puts("gs_initdb (openGauss) " PG_VERSION);
#endif
            exit(0);
        }
    }

    /* process command-line options */

    while ((c = getopt_long(argc, argv, "cdD:E:L:nU:WA:SsT:X:C:w:H:g:", long_options, &option_index)) != -1) {
#define FREE_NOT_STATIC_ZERO_STRING(s)        \
    do {                                      \
        if ((s) && (char*)(s) != (char*)"") { \
            free(s);                          \
            (s) = NULL;                       \
        }                                     \
    } while (0)

        switch (c) {
            case 'A':
                FREE_NOT_STATIC_ZERO_STRING(authmethodlocal_tmp);
                FREE_NOT_STATIC_ZERO_STRING(authmethodhost_tmp);
                check_input_spec_char(optarg);
                authmethodlocal_tmp = authmethodhost_tmp = xstrdup(optarg);

                /*
                 * When ident is specified, use peer for local connections.
                 * Mirrored, when peer is specified, use ident for TCP/IP
                 * connections.
                 */
                if (strcmp(authmethodhost_tmp, "ident") == 0)
                    authmethodlocal_tmp = xstrdup("peer");
                else if (strcmp(authmethodlocal_tmp, "peer") == 0)
                    authmethodhost_tmp = xstrdup("ident");
                break;
            case 10:
                if (authmethodlocal_tmp != authmethodhost_tmp)
                    FREE_NOT_STATIC_ZERO_STRING(authmethodlocal_tmp);
                check_input_spec_char(optarg);
                authmethodlocal_tmp = xstrdup(optarg);
                break;
            case 11:
                if (authmethodlocal_tmp != authmethodhost_tmp)
                    FREE_NOT_STATIC_ZERO_STRING(authmethodhost_tmp);
                check_input_spec_char(optarg);
                authmethodhost_tmp = xstrdup(optarg);
                break;
#ifndef ENABLE_MULTIPLE_NODES
            case 'c':
                enableDCF = true;
                break;
#endif
            case 'D':
                FREE_NOT_STATIC_ZERO_STRING(pg_data);
                check_input_spec_char(optarg);
                pg_data = xstrdup(optarg);
                break;
            case 'E':
                FREE_NOT_STATIC_ZERO_STRING(encoding);
                check_input_spec_char(optarg);
                encoding = xstrdup(optarg);
                break;
            case 'W':
                pwprompt = true;
                break;
            case 'C':
                check_input_spec_char(optarg);
                if (realpath(optarg, encrypt_pwd_real_path) == NULL) {
                    write_stderr(_("%s: The parameter of -C is invalid.\n"), progname);
                    break;
                }

                ret = snprintf_s(cipher_key_file, MAXPGPATH, MAXPGPATH - 1,
                                 "%s/server.key.cipher", encrypt_pwd_real_path);
                securec_check_ss_c(ret, "\0", "\0");
                ret = snprintf_s(rand_file, MAXPGPATH, MAXPGPATH - 1,
                                 "%s/server.key.rand", encrypt_pwd_real_path);
                securec_check_ss_c(ret, "\0", "\0");
                if (!is_file_exist(cipher_key_file) || !is_file_exist(rand_file)) {
                    pwpasswd = NULL;
                    printf(_("Read cipher or random parameter file failed." 
                             "The password of the initial user is not set.\n"));
                    break;
                }

                decode_cipher_files(SERVER_MODE, NULL, encrypt_pwd_real_path, (unsigned char*)depasswd, true);
                FREE_NOT_STATIC_ZERO_STRING(pwpasswd);
                pwpasswd = xstrdup(depasswd);
                break;
            case 'w':
                if ((pwpasswd != NULL) && pwpasswd != (char*)"") {
                    rc = memset_s(pwpasswd, strlen(pwpasswd), 0, strlen(pwpasswd));
                    securec_check_c(rc, "\0", "\0");
                    FREE_AND_RESET(pwpasswd);
                }
                pwpasswd = xstrdup(optarg);
                rc = memset_s(optarg, strlen(optarg), 0, strlen(optarg));
                securec_check_c(rc, "\0", "\0");
                break;
            case 'U':
                FREE_NOT_STATIC_ZERO_STRING(username);
                check_input_spec_char(optarg, true);
                username = xstrdup(optarg);
                break;
            case 'd':
                debug = true;
                printf(_("Running in debug mode.\n"));
                break;
            case 'g':
                FREE_NOT_STATIC_ZERO_STRING(new_xlog_file_path);
                check_input_spec_char(optarg);
                new_xlog_file_path = xstrdup(optarg);
                break;
            case 'n':
                noclean = true;
                printf(_("Running in noclean mode.  Mistakes will not be cleaned up.\n"));
                break;
            case 'L':
                FREE_NOT_STATIC_ZERO_STRING(share_path);
                check_input_spec_char(optarg);
                share_path = xstrdup(optarg);
                break;
            case 1:
                FREE_NOT_STATIC_ZERO_STRING(locale);
                check_input_spec_char(optarg);
                locale = xstrdup(optarg);
                break;
            case 2:
                FREE_NOT_STATIC_ZERO_STRING(lc_collate);
                check_input_spec_char(optarg);
                lc_collate = xstrdup(optarg);
                break;
            case 3:
                FREE_NOT_STATIC_ZERO_STRING(lc_ctype);
                check_input_spec_char(optarg);
                lc_ctype = xstrdup(optarg);
                break;
            case 4:
                FREE_NOT_STATIC_ZERO_STRING(lc_monetary);
                check_input_spec_char(optarg);
                lc_monetary = xstrdup(optarg);
                break;
            case 5:
                FREE_NOT_STATIC_ZERO_STRING(lc_numeric);
                check_input_spec_char(optarg);
                lc_numeric = xstrdup(optarg);
                break;
            case 6:
                FREE_NOT_STATIC_ZERO_STRING(lc_time);
                check_input_spec_char(optarg);
                lc_time = xstrdup(optarg);
                break;
            case 7:
                FREE_NOT_STATIC_ZERO_STRING(lc_messages);
                check_input_spec_char(optarg);
                lc_messages = xstrdup(optarg);
                break;
            case 8:
                FREE_NOT_STATIC_ZERO_STRING(locale);
                locale = "C";
                break;
            case 9:
                FREE_NOT_STATIC_ZERO_STRING(pwfilename);
                check_input_spec_char(optarg);
                pwfilename = xstrdup(optarg);
                break;
            case 's':
                show_setting = true;
                break;
            case 'T':
                FREE_NOT_STATIC_ZERO_STRING(default_text_search_config_tmp);
                check_input_spec_char(optarg);
                default_text_search_config_tmp = xstrdup(optarg);
                break;
            case 'X':
                FREE_NOT_STATIC_ZERO_STRING(xlog_dir);
                check_input_spec_char(optarg);
                xlog_dir = xstrdup(optarg);
                break;
            case 'S':
                security = true;
                printf(_("Running in security mode.\n"));
                break;
            case 'H':
                FREE_NOT_STATIC_ZERO_STRING(host_ip);
                check_input_spec_char(optarg);
                host_ip = xstrdup(optarg);
                break;
#ifdef PGXC
            case 12:
                FREE_NOT_STATIC_ZERO_STRING(nodename);
                check_input_spec_char(optarg);
                nodename = xstrdup(optarg);
                break;
#endif
            case 13:
                FREE_NOT_STATIC_ZERO_STRING(dbcompatibility);
                check_input_spec_char(optarg);
                dbcompatibility = xstrdup(optarg);
                break;
           case 14:
                if (atoi(optarg) < MIN_BUCKETSLEN || atoi(optarg) > MAX_BUCKETSLEN) {
                    write_stderr(_("unexpected buckets length specified, valid range is %d - %d.\n"),
                        MIN_BUCKETSLEN, MAX_BUCKETSLEN);
                    exit(1);
                }
                g_bucket_len = atoi(optarg);
                break;

            default:
                /* getopt_long already emitted a complaint */
                write_stderr(_("Try \"%s --help\" for more information.\n"), progname);
                exit(1);
        }
#undef FREE_NOT_STATIC_ZERO_STRING
    }

    if (default_text_search_config_tmp != NULL)
        default_text_search_config = default_text_search_config_tmp;
    if (authmethodhost_tmp != NULL)
        authmethodhost = authmethodhost_tmp;
    if (authmethodlocal_tmp != NULL)
        authmethodlocal = authmethodlocal_tmp;

    /*
     * Non-option argument specifies data directory as long as it wasn't
     * already specified with -D / --pgdata
     */
    if (optind < argc && strlen(pg_data) == 0) {
        check_input_spec_char(argv[optind]);
        pg_data = xstrdup(argv[optind]);
        optind++;
    }

    if (optind < argc) {
        write_stderr(_("%s: too many command-line arguments (first is \"%s\")\n"), progname, argv[optind]);
        write_stderr(_("Try \"%s --help\" for more information.\n"), progname);
        exit(1);
    }

    if (pwprompt && (pwfilename != NULL)) {
        write_stderr(_("%s: password prompt and password file cannot be specified together\n"), progname);
        exit(1);
    }

#ifdef PGXC
    if (nodename == NULL) {
        write_stderr(_("%s: openGauss node name is mandatory\n"), progname);
        write_stderr(_("Try \"%s --help\" for more information.\n"), progname);
        exit(1);
    }

    if (!is_valid_nodename(nodename)) {
        write_stderr(_("%s: openGauss node name:%s is invalid.\nThe node name must consist of lowercase letters "
                        "(a-z), underscores (_), special characters #, digits (0-9), or dollar ($).\n"
                        "The node name must start with a lowercase letter (a-z),"
                        " or an underscore (_).\nThe max length of nodename is %d.\n"),
            progname, nodename, PGXC_NODENAME_LENGTH);
        exit(1);
    }

#endif

    check_authmethod_unspecified(&authmethodlocal);
    check_authmethod_unspecified(&authmethodhost);

    check_authmethod_valid(authmethodlocal, auth_methods_local, "local");
    check_authmethod_valid(authmethodhost, auth_methods_host, "host");

    if (strlen(pg_data) == 0) {
        pgdenv = getenv("PGDATA");
        if ((pgdenv != NULL) && strlen(pgdenv)) {
            /* PGDATA found */
            check_env_value(pgdenv);
            FREE_AND_RESET(pg_data);
            pg_data = xstrdup(pgdenv);
        } else {
            write_stderr(_("%s: no data directory specified\n"
                           "You must identify the directory where the data for this database system\n"
                           "will reside.  Do this with either the invocation option -D or the\n"
                           "environment variable PGDATA.\n"),
                progname);
            exit(1);
        }
    }

    pg_data_native = pg_data;
    canonicalize_path(pg_data);

#ifdef WIN32

    /*
     * Before we execute another program, make sure that we are running with a
     * restricted token. If not, re-execute ourselves with one.
     */

    if ((restrict_env = getenv("PG_RESTRICT_EXEC")) == NULL || strcmp(restrict_env, "1") != 0) {
        PROCESS_INFORMATION pi;
        char* cmdline = NULL;

        ZeroMemory(&pi, sizeof(pi));

        cmdline = xstrdup(GetCommandLine());

        putenv("PG_RESTRICT_EXEC=1");

        if (!CreateRestrictedProcess(cmdline, &pi)) {
            write_stderr(
                _("%s: could not re-execute with restricted token: error code %lu\n"), progname, GetLastError());
            FREE_AND_RESET(cmdline);
        } else {
            /*
             * Successfully re-execed. Now wait for child process to capture
             * exitcode.
             */
            DWORD x;

            CloseHandle(pi.hThread);
            WaitForSingleObject(pi.hProcess, INFINITE);

            if (!GetExitCodeProcess(pi.hProcess, &x)) {
                write_stderr(
                    _("%s: could not get exit code from subprocess: error code %lu\n"), progname, GetLastError());
                FREE_AND_RESET(cmdline);
                exit(1);
            }
            FREE_AND_RESET(cmdline);
            exit(x);
        }
    }
#endif

    /*
     * we have to set PGDATA for openGauss rather than pass it on the command
     * line to avoid dumb quoting problems on Windows, and we would especially
     * need quotes otherwise on Windows because paths there are most likely to
     * have embedded spaces.
     */
    pgdenv = (char*)pg_malloc(8 + strlen(pg_data));
    nRet = sprintf_s(pgdenv, (8 + strlen(pg_data)), "PGDATA=%s", pg_data);
    securec_check_ss_c(nRet, pgdenv, "\0");
    (void)putenv(pgdenv);

    if ((ret = find_other_exec(argv[0], "gaussdb", PG_BACKEND_VERSIONSTR, backend_exec)) < 0) {
        char full_path[MAXPGPATH] = {0};

        if (find_my_exec(argv[0], full_path) < 0) {
            rc = strncpy_s(full_path, sizeof(full_path), progname, sizeof(full_path) - 1);
            securec_check_c(rc, "\0", "\0");
        }

        if (ret == -1)
            write_stderr(_("The program \"gaussdb\" is needed by %s "
                           "but was not found in the\n"
                           "same directory as \"%s\".\n"
                           "Check your installation.\n"),
                progname,
                full_path);
        else
            write_stderr(_("The program \"gaussdb\" was found by \"%s\"\n"
                           "but was not the same version as %s.\n"
                           "Check your installation.\n"),
                full_path,
                progname);
        exit(1);
    }

    /* store binary directory */
    rc = strcpy_s(bin_path, MAXPGPATH, backend_exec);
    securec_check_c(rc, "\0", "\0");
    *last_dir_separator(bin_path) = '\0';
    canonicalize_path(bin_path);

    if (share_path == NULL) {
        share_path = (char*)pg_malloc(MAXPGPATH);
        get_share_path(backend_exec, share_path);
    } else if (!is_absolute_path(share_path)) {
        write_stderr(_("%s: input file location must be an absolute path\n"), progname);
        exit(1);
    }

    canonicalize_path(share_path);

    effective_user = get_id();
    if (strlen(username) == 0) {
        if (username != (char*)"") {
            FREE_AND_RESET(username);
        }
        username = effective_user;
    }
    set_input(&bki_file, "postgres.bki");
    set_input(&desc_file, "postgres.description");
    set_input(&shdesc_file, "postgres.shdescription");
    set_input(&hba_file, "pg_hba.conf.sample");
    set_input(&ident_file, "pg_ident.conf.sample");
    set_input(&conf_file, "postgresql.conf.sample");
#ifdef ENABLE_MOT
    set_input(&mot_conf_file, "mot.conf.sample");
#endif
    set_input(&gazelle_conf_file, "gs_gazelle.conf.sample");
    set_input(&conversion_file, "conversion_create.sql");
    set_input(&dictionary_file, "snowball_create.sql");
    set_input(&info_schema_file, "information_schema.sql");
    set_input(&features_file, "sql_features.txt");
    set_input(&system_views_file, "system_views.sql");
    set_input(&performance_views_file, "performance_views.sql");
#ifdef ENABLE_PRIVATEGAUSS
    set_input(&private_system_views_file, "private_system_views.sql");
#endif
#ifndef ENABLE_MULTIPLE_NODES
    for (unsigned i = 0; i < SNAPSHOT_LEN; i++) {
        char buf[128];
        nRet = sprintf_s(buf, sizeof(buf), "db4ai/snapshots/%s.sql", snapshot_names[i]);
        securec_check_ss_c(nRet, "\0", "\0");
        set_input(&snapshot_files[i], buf);
    }
#endif

    set_info_version();

    if (show_setting || debug) {
        write_stderr("VERSION=%s\n"
                     "PGDATA=%s\nshare_path=%s\nPGPATH=%s\n"
                     "POSTGRES_SUPERUSERNAME=%s\nPOSTGRES_BKI=%s\n"
                     "POSTGRES_DESCR=%s\nPOSTGRES_SHDESCR=%s\n"
                     "POSTGRESQL_CONF_SAMPLE=%s\n"
#ifdef ENABLE_MOT
                     "MOT_CONF_SAMPLE=%s\n"
#endif
                     "GAZELLE_CONF_SAMPLE=%s\n"
                     "PG_HBA_SAMPLE=%s\nPG_IDENT_SAMPLE=%s\n",
            PG_VERSION,
            pg_data,
            share_path,
            bin_path,
            username,
            bki_file,
            desc_file,
            shdesc_file,
            conf_file,
#ifdef ENABLE_MOT
            mot_conf_file,
#endif
            gazelle_conf_file,
            hba_file,
            ident_file);
        if (show_setting)
            exit(0);
    }

    check_input(bki_file);
    check_input(desc_file);
    check_input(shdesc_file);
    check_input(hba_file);
    check_input(ident_file);
    check_input(conf_file);
#ifdef ENABLE_MOT
    check_input(mot_conf_file);
#endif
    check_input(gazelle_conf_file);
    check_input(conversion_file);
    check_input(dictionary_file);
    check_input(info_schema_file);
    check_input(features_file);
    check_input(system_views_file);
    check_input(performance_views_file);
#ifdef ENABLE_PRIVATEGAUSS
    check_input(private_system_views_file);
#endif
#ifndef ENABLE_MULTIPLE_NODES
    for (unsigned i = 0; i < SNAPSHOT_LEN; i++) {
        check_input(snapshot_files[i]);
    }
#endif

    setlocales();

    printf(_("The files belonging to this database system will be owned "
             "by user \"%s\".\n"
             "This user must also own the server process.\n\n"),
        effective_user);

    if (strcmp(lc_ctype, lc_collate) == 0 && strcmp(lc_ctype, lc_time) == 0 && strcmp(lc_ctype, lc_numeric) == 0 &&
        strcmp(lc_ctype, lc_monetary) == 0 && strcmp(lc_ctype, lc_messages) == 0)
        printf(_("The database cluster will be initialized with locale \"%s\".\n"), lc_ctype);
    else {
        printf(_("The database cluster will be initialized with locales\n"
                 "  COLLATE:  %s\n"
                 "  CTYPE:    %s\n"
                 "  MESSAGES: %s\n"
                 "  MONETARY: %s\n"
                 "  NUMERIC:  %s\n"
                 "  TIME:     %s\n"),
            lc_collate,
            lc_ctype,
            lc_messages,
            lc_monetary,
            lc_numeric,
            lc_time);
    }

    if (strlen(encoding) == 0) {
        int ctype_enc;

        ctype_enc = pg_get_encoding_from_locale(lc_ctype, true);

        if (ctype_enc == -1) {
            /* Couldn't recognize the locale's codeset */
            write_stderr(_("%s: could not find suitable encoding for locale \"%s\"\n"), progname, lc_ctype);
            write_stderr(_("Rerun %s with the -E option.\n"), progname);
            write_stderr(_("Try \"%s --help\" for more information.\n"), progname);
            exit(1);
        } else if (!pg_valid_server_encoding_id(ctype_enc)) {
            /*
             * We recognized it, but it's not a legal server encoding. On
             * Windows, UTF-8 works with any locale, so we can fall back to
             * UTF-8.
             */
#ifdef WIN32
            printf(_("Encoding \"%s\" implied by locale is not allowed as a server-side encoding.\n"
                     "The default database encoding will be set to \"%s\" instead.\n"),
                pg_encoding_to_char(ctype_enc),
                pg_encoding_to_char(PG_UTF8));
            ctype_enc = PG_UTF8;
            encodingid = encodingid_to_string(ctype_enc);
#else
            write_stderr(_("%s: locale \"%s\" requires unsupported encoding \"%s\"\n"),
                progname,
                lc_ctype,
                pg_encoding_to_char(ctype_enc));
            write_stderr(_("Encoding \"%s\" is not allowed as a server-side encoding.\n"
                           "Rerun %s with a different locale selection.\n"),
                pg_encoding_to_char(ctype_enc),
                progname);
            exit(1);
#endif
        } else {
            encodingid = encodingid_to_string(ctype_enc);
            printf(_("The default database encoding has accordingly been set to \"%s\".\n"),
                pg_encoding_to_char(ctype_enc));
        }
    } else
        encodingid = get_encoding_id(encoding);

    user_enc = atoi(encodingid);
    if (!check_locale_encoding(lc_ctype, user_enc) || !check_locale_encoding(lc_collate, user_enc))
        exit(1); /* check_locale_encoding printed the error */

    if (strlen(default_text_search_config) == 0) {
        default_text_search_config = find_matching_ts_config(lc_ctype);
        if (default_text_search_config == NULL) {
            printf(_("%s: could not find suitable text search configuration for locale \"%s\"\n"), progname, lc_ctype);
            default_text_search_config = "simple";
        }
    } else {
        const char* checkmatch = find_matching_ts_config(lc_ctype);

        if (checkmatch == NULL) {
            printf(_("%s: warning: suitable text search configuration for locale \"%s\" is unknown\n"),
                progname,
                lc_ctype);
        } else if (strcmp(checkmatch, default_text_search_config) != 0) {
            printf(_("%s: warning: specified text search configuration \"%s\" might not match locale \"%s\"\n"),
                progname,
                default_text_search_config,
                lc_ctype);
        }
    }

    printf(_("The default text search configuration will be set to \"%s\".\n"), default_text_search_config);

    printf("\n");

    (void)umask(S_IRWXG | S_IRWXO);

    // log output redirect
    init_log(PROG_NAME);

    /*
     * now we are starting to do real work, trap signals so we can clean up
     */

    /* some of these are not valid on Windows */
#ifdef SIGHUP
    (void)pqsignal(SIGHUP, trapsig);
#endif
#ifdef SIGINT
    (void)pqsignal(SIGINT, trapsig);
#endif
#ifdef SIGQUIT
    (void)pqsignal(SIGQUIT, trapsig);
#endif
#ifdef SIGTERM
    (void)pqsignal(SIGTERM, trapsig);
#endif

    /* Ignore SIGPIPE when writing to backend, so we can clean up */
#ifdef SIGPIPE
    (void)pqsignal(SIGPIPE, SIG_IGN);
#endif

    switch (pg_check_dir(pg_data)) {
        case 0:
            /* PGDATA not there, must create it */
            printf(_("creating directory %s ... "), pg_data);
            (void)fflush(stdout);

            if (pg_mkdir_p(pg_data, S_IRWXU) != 0) {
                char errBuffer[ERROR_LIMIT_LEN];
                fprintf(stderr,
                    _("%s: could not create directory \"%s\": %s\n"),
                    progname,
                    pg_data,
                    pqStrerror(errno, errBuffer, ERROR_LIMIT_LEN));
                exit_nicely();
            } else {
                check_ok();
            }

            made_new_pgdata = true;
            break;

        case 1:
            /* Present but empty, fix permissions and use it */
            printf(_("fixing permissions on existing directory %s ... "), pg_data);
            (void)fflush(stdout);

            if (chmod(pg_data, S_IRWXU) != 0) {
                char errBuffer[ERROR_LIMIT_LEN];
                write_stderr(_("%s: could not change permissions of directory \"%s\": %s\n"),
                    progname,
                    pg_data,
                    pqStrerror(errno, errBuffer, ERROR_LIMIT_LEN));
                exit_nicely();
            } else
                check_ok();

            found_existing_pgdata = true;
            break;

        case 2:
            /* Present and not empty */
            {
                /* skip full_upgrade_bak */
                DIR* chkdir = NULL;
                struct dirent* de = NULL;
                bool only_full_upgrade_bak = true;
                if ((chkdir = opendir(pg_data)) != NULL) {
                    while ((de = gs_readdir(chkdir)) != NULL) {
                        if (strcmp(".", de->d_name) == 0 || strcmp("..", de->d_name) == 0) {
                            /* skip this and parent directory */
                            continue;
                        } else if (strcmp(de->d_name, "full_upgrade_bak") == 0 ||
                                   strcmp(de->d_name, "pg_location") == 0) {
                            /* skip full_upgrade_bak and pg_localtion */
                            continue;
                        } else {
                            /* not empty */
                            only_full_upgrade_bak = false;
                            break;
                        }
                    }

                    (void)closedir(chkdir);

                    if (only_full_upgrade_bak) {
                        check_ok();
                        break;
                    }
                }
            }

            write_stderr(_("%s: directory \"%s\" exists but is not empty\n"), progname, pg_data);
            write_stderr(_("If you want to create a new database system, either remove or empty\n"
                           "the directory \"%s\" or run %s\n"
                           "with an argument other than \"%s\".\n"),
                pg_data,
                progname,
                pg_data);
            exit(1); /* no further message needed */

        default: {
            char errBuffer[ERROR_LIMIT_LEN];
            /* Trouble accessing directory */
            write_stderr(_("%s: could not access directory \"%s\": %s\n"),
                progname,
                pg_data,
                pqStrerror(errno, errBuffer, ERROR_LIMIT_LEN));
            exit_nicely();
        }
    }

    /* Create transaction log symlink, if required */
    if (strcmp(xlog_dir, "") != 0) {
        char linkloc[MAXPGPATH] = {'\0'};

        /* clean up xlog directory name, check it's absolute */
        canonicalize_path(xlog_dir);
        if (!is_absolute_path(xlog_dir)) {
            write_stderr(_("%s: transaction log directory location must be an absolute path\n"), progname);
            exit_nicely();
        }

        /* check if the specified xlog directory exists/is empty */
        switch (pg_check_dir(xlog_dir)) {
            case 0:
                /* xlog directory not there, must create it */
                printf(_("creating directory %s ... "), xlog_dir);
                (void)fflush(stdout);

                if (pg_mkdir_p(xlog_dir, S_IRWXU) != 0) {
                    char errBuffer[ERROR_LIMIT_LEN];
                    write_stderr(_("%s: could not create directory \"%s\": %s\n"),
                        progname,
                        xlog_dir,
                        pqStrerror(errno, errBuffer, ERROR_LIMIT_LEN));
                    exit_nicely();
                } else
                    check_ok();

                made_new_xlogdir = true;
                break;

            case 1:
                /* Present but empty, fix permissions and use it */
                printf(_("fixing permissions on existing directory %s ... "), xlog_dir);
                (void)fflush(stdout);

                if (chmod(xlog_dir, S_IRWXU) != 0) {
                    char errBuffer[ERROR_LIMIT_LEN];
                    write_stderr(_("%s: could not change permissions of directory \"%s\": %s\n"),
                        progname,
                        xlog_dir,
                        pqStrerror(errno, errBuffer, ERROR_LIMIT_LEN));
                    exit_nicely();
                } else
                    check_ok();

                found_existing_xlogdir = true;
                break;

            case 2:
                /* Present and not empty */
                write_stderr(_("%s: directory \"%s\" exists but is not empty\n"), progname, xlog_dir);
                write_stderr(_("If you want to store the transaction log there, either\n"
                               "remove or empty the directory \"%s\".\n"),
                    xlog_dir);
                exit_nicely();
                break;

            default: {
                char errBuffer[ERROR_LIMIT_LEN];
                /* Trouble accessing directory */
                write_stderr(_("%s: could not access directory \"%s\": %s\n"),
                    progname,
                    xlog_dir,
                    pqStrerror(errno, errBuffer, ERROR_LIMIT_LEN));
                exit_nicely();
            } break;
        }

        /* form name of the place where the symlink must go */
        nRet = sprintf_s(linkloc, sizeof(linkloc), "%s/pg_xlog", pg_data);
        securec_check_ss_c(nRet, "\0", "\0");

#ifdef HAVE_SYMLINK
        if (symlink(xlog_dir, linkloc) != 0) {
            char errBuffer[ERROR_LIMIT_LEN];
            write_stderr(_("%s: could not create symbolic link \"%s\": %s\n"),
                progname,
                linkloc,
                pqStrerror(errno, errBuffer, ERROR_LIMIT_LEN));
            exit_nicely();
        }
#else
        write_stderr(_("%s: symlinks are not supported on this platform"), progname);
        exit_nicely();
#endif
    }

    /* Create required subdirectories */
    printf(_("creating subdirectories ... "));
    (void)fflush(stdout);

    for (i = 0; (unsigned int)(i) < lengthof(subdirs); i++) {
        char* path = NULL;
        errno_t sret = 0;
        size_t len = 0;

        /*
         * -X means using user define xlog directory, and will create symbolic pg_xlog
         * under pg_data directory, So no need to create these sub-directories again.
         */
        if (pg_strcasecmp(xlog_dir, "") != 0 && (pg_strcasecmp(subdirs[i], "pg_xlog") == 0)) {
            continue;
        }

        len = strlen(pg_data) + strlen(subdirs[i]) + 2;
        path = (char*)pg_malloc(len);

        sret = sprintf_s(path, len, "%s/%s", pg_data, subdirs[i]);
        securec_check_ss_c(sret, path, "\0");

        /*
         * The parent directory already exists, so we only need mkdir() not
         * pg_mkdir_p() here, which avoids some failure modes; cf bug #13853.
         */
        if (mkdir(path, S_IRWXU) < 0) {
            char errBuffer[ERROR_LIMIT_LEN];
            fprintf(stderr,
                _("%s: could not create directory \"%s\": %s\n"),
                progname,
                path,
                pqStrerror(errno, errBuffer, ERROR_LIMIT_LEN));

            FREE_AND_RESET(path);
            exit_nicely();
        }
        FREE_AND_RESET(path);
    }

    if (strlen(new_xlog_file_path) == 0) {
        boot_options = raw_boot_options;
        backend_options = raw_backend_options;
    } else {
        size_t total_len = strlen(raw_boot_options) + strlen(new_xlog_file_path) + 10;
        char *options = (char*)pg_malloc(total_len);
        errno_t sret = sprintf_s(options, total_len, "%s -g %s", raw_boot_options, new_xlog_file_path);
        securec_check_ss_c(sret, options, "\0");
        boot_options = options;

        total_len = strlen(raw_backend_options) + strlen(new_xlog_file_path) + 10;
        options = (char*)pg_malloc(total_len);
        sret = sprintf_s(options, total_len, "%s -g %s", raw_backend_options, new_xlog_file_path);
        securec_check_ss_c(sret, options, "\0");
        backend_options = options;

        struct stat st;
        if (stat(new_xlog_file_path, &st) < 0) {
            if (errno == ENOENT) {
                canonicalize_path(new_xlog_file_path);
                int fd = open(new_xlog_file_path, O_RDWR | O_CREAT | O_EXCL | PG_BINARY, 0600);

                close(fd);
                write_log("create file:%s", new_xlog_file_path);
            } else {
                char errBuffer[ERROR_LIMIT_LEN];
                write_stderr(_("%s: could not access file \"%s\": %s\n"), progname, new_xlog_file_path, 
                             pqStrerror(errno, errBuffer, ERROR_LIMIT_LEN));
                exit_nicely();
            }
        }
    }
    
    /* create or check pg_location path */
    mkdirForPgLocationDir();
    check_ok();

    /* Top level PG_VERSION is checked by bootstrapper, so make it first */
    write_version_file(NULL);

    create_pg_lockfiles();

    /* Select suitable configuration settings */
    set_null_conf();
    test_config_settings();

    /* Now create all the text config files */
    setup_config();

    /* Init undo subsystem meta. */
    InitUndoSubsystemMeta();

    /* Bootstrap template1 */
    bootstrap_template1();

    /*
     * Make the per-database PG_VERSION for template1 only after init'ing it
     */
    write_version_file("base/1");

    CreatePGDefaultTempDir();

    /* Create the stuff we don't need to use bootstrap mode for */

    setup_auth();
    get_set_pwd();

    setup_depend();
    load_plpgsql();
    setup_sysviews();
#ifdef ENABLE_PRIVATEGAUSS
    setup_privsysviews();
#endif
    setup_perfviews();

#ifdef PGXC
    /* Initialize catalog information about the node self */
    setup_nodeself();
#endif

    setup_description();

    setup_collation();

    setup_conversion();

    setup_dictionary();

    setup_privileges();

    setup_bucketmap_len();

    setup_schema();

    load_supported_extension();

    setup_update();

#ifndef ENABLE_MULTIPLE_NODES
    setup_snapshots();
#endif

    vacuum_db();

    make_template0();

    make_postgres();

#ifdef PGXC
    vacuumfreeze("template0");
    vacuumfreeze("template1");
    vacuumfreeze("postgres");
#endif

    if (authwarning != NULL)
        write_stderr("%s", authwarning);

    /* Get directory specification used to start this executable */
    rc = strncpy_s(bin_dir, sizeof(bin_dir), argv[0], sizeof(bin_dir) - 1);
    securec_check_c(rc, "\0", "\0");
    get_parent_directory(bin_dir);

#ifdef ENABLE_MULTIPLE_NODES
    printf(_("\nSuccess.\n You can now start the database server of the openGauss coordinator using:\n\n"
             "    %s%s%sgaussdb%s --coordinator -D %s%s%s\n"
             "or\n"
             "    %s%s%sgs_ctl%s start -D %s%s%s -Z coordinator -l logfile\n\n"
             " You can now start the database server of the openGauss datanode using:\n\n"
             "    %s%s%sgaussdb%s --datanode -D %s%s%s\n"
             "or \n"
             "    %s%s%sgs_ctl%s start -D %s%s%s -Z datanode -l logfile\n\n"),
        QUOTE_PATH,
        bin_dir,
        (strlen(bin_dir) > 0) ? DIR_SEP : "",
        QUOTE_PATH,
        QUOTE_PATH,
        pg_data_native,
        QUOTE_PATH,
        QUOTE_PATH,
        bin_dir,
        (strlen(bin_dir) > 0) ? DIR_SEP : "",
        QUOTE_PATH,
        QUOTE_PATH,
        pg_data_native,
        QUOTE_PATH,
        QUOTE_PATH,
        bin_dir,
        (strlen(bin_dir) > 0) ? DIR_SEP : "",
        QUOTE_PATH,
        QUOTE_PATH,
        pg_data_native,
        QUOTE_PATH,
        QUOTE_PATH,
        bin_dir,
        (strlen(bin_dir) > 0) ? DIR_SEP : "",
        QUOTE_PATH,
        QUOTE_PATH,
        pg_data_native,
        QUOTE_PATH);
#else
    printf(_("\nSuccess. You can now start the database server of single node using:\n\n"
             "    %s%s%sgaussdb%s -D %s%s%s --single_node\n"
             "or\n"
             "    %s%s%sgs_ctl%s start -D %s%s%s -Z single_node -l logfile\n\n"),
        QUOTE_PATH,
        bin_dir,
        (strlen(bin_dir) > 0) ? DIR_SEP : "",
        QUOTE_PATH,
        QUOTE_PATH,
        pg_data_native,
        QUOTE_PATH,
        QUOTE_PATH,
        bin_dir,
        (strlen(bin_dir) > 0) ? DIR_SEP : "",
        QUOTE_PATH,
        QUOTE_PATH,
        pg_data_native,
        QUOTE_PATH);
#endif

    /* free memory*/
    FREE_AND_RESET(effective_user);
    FREE_AND_RESET(lc_messages);
    FREE_AND_RESET(lc_ctype);
    FREE_AND_RESET(lc_collate);
    FREE_AND_RESET(lc_numeric);
    FREE_AND_RESET(lc_monetary);
    FREE_AND_RESET(lc_time);

    return 0;
}

static bool isDirectory(const char* basepath, const char* name)
{
    struct stat buf;
    char* path = NULL;
    int nRet = 0;
    size_t len = 0;

    len = strlen(basepath) + strlen(name) + 2;
    path = (char*)pg_malloc(len);

    nRet = sprintf_s(path, len, "%s/%s", basepath, name);
    securec_check_ss_c(nRet, "\0", "\0");

    if (stat(path, &buf) == -1 && errno == ENOENT) {
        FREE_AND_RESET(path);
        return false;
    }
    FREE_AND_RESET(path);

    if (S_ISREG(buf.st_mode)) {
        return false;
    } else {
        return true;
    }
}

static bool isMountDirCorrect(const char* basepath, const char* name)
{
    DIR* chk_mount_dir = NULL;
    struct dirent* de_mount = NULL;
    char* path = NULL;
    int nRet = 0;
    size_t len = 0;

    len = strlen(basepath) + strlen(name) + 2;
    path = (char*)pg_malloc(len);

    nRet = sprintf_s(path, len, "%s/%s", basepath, name);
    securec_check_ss_c(nRet, path, "\0");

    if ((chk_mount_dir = opendir(path)) != NULL) {
        while ((de_mount = gs_readdir(chk_mount_dir)) != NULL) {
            if (strcmp(".", de_mount->d_name) == 0 || strcmp("..", de_mount->d_name) == 0) {
                /* skip this and parent directory */
                continue;
            } else {
                if (strcmp(de_mount->d_name, "full_upgrade_bak") == 0 && isDirectory(path, de_mount->d_name)) {
                    continue;
                } else if (isDirectory(path, de_mount->d_name) && isMountDirCorrect(path, de_mount->d_name)) {
                    continue;
                } else {
                    FREE_AND_RESET(path);
                    (void)closedir(chk_mount_dir);
                    return false;
                }
            }
        }

        FREE_AND_RESET(path);
        (void)closedir(chk_mount_dir);
        return true;
    } else {
        FREE_AND_RESET(path);
        return false;
    }
}

static void mkdirForPgLocationDir()
{
    char* path = NULL;
    int nRet = 0;
    size_t len = 0;

    len = strlen(pg_data) + strlen("pg_location") + 2;
    path = (char*)pg_malloc(len);

    nRet = sprintf_s(path, len, "%s/pg_location", pg_data);
    securec_check_ss_c(nRet, "\0", "\0");

    switch (pg_check_dir(path)) {
        case 0:
            /* directory not there, must create it */
            if (pg_mkdir_p(path, S_IRWXU) != 0) {
                char errBuffer[ERROR_LIMIT_LEN];
                write_stderr(_("%s: could not create directory \"%s\": %s\n"),
                    progname,
                    path,
                    pqStrerror(errno, errBuffer, ERROR_LIMIT_LEN));
                FREE_AND_RESET(path);
                exit_nicely();
            }
            FREE_AND_RESET(path);
            break;

        case 1:
            /* Present but empty, fix permissions and use it */
            if (chmod(path, S_IRWXU) != 0) {
                char errBuffer[ERROR_LIMIT_LEN];
                write_stderr(_("%s: could not change permissions of directory \"%s\": %s\n"),
                    progname,
                    path,
                    pqStrerror(errno, errBuffer, ERROR_LIMIT_LEN));
                FREE_AND_RESET(path);
                exit_nicely();
            }
            FREE_AND_RESET(path);
            break;

        case 2:
            /* Present and not empty */
            {
                DIR* chk_pg_location_dir = NULL;
                struct dirent* de_pg_location = NULL;

                if ((chk_pg_location_dir = opendir(path)) != NULL) {
                    while ((de_pg_location = gs_readdir(chk_pg_location_dir)) != NULL) {
                        if (strcmp(".", de_pg_location->d_name) == 0 || strcmp("..", de_pg_location->d_name) == 0) {
                            /* skip this and parent directory */
                            continue;
                        } else if (isDirectory(path, de_pg_location->d_name) &&
                                   isMountDirCorrect(path, de_pg_location->d_name)) {
                            continue;
                        } else {
                            (void)closedir(chk_pg_location_dir);
                            goto failed_exit;
                        }
                    }

                    (void)closedir(chk_pg_location_dir);
                }

                FREE_AND_RESET(path);
                break;
            }

        failed_exit:
            write_stderr(_("%s: directory \"%s\" exists but is not empty\n"), progname, path);
            write_stderr(_("If you want to store the transaction log there, either\n"
                           "remove or empty the directory \"%s\".\n"),
                path);
            FREE_AND_RESET(path);
            exit_nicely();
            break;

        default: {
            char errBuffer[ERROR_LIMIT_LEN];
            /* Trouble accessing directory */
            write_stderr(_("%s: could not access directory \"%s\": %s\n"),
                progname,
                path,
                pqStrerror(errno, errBuffer, ERROR_LIMIT_LEN));
            FREE_AND_RESET(path);
            exit_nicely();
        } break;
    }
}

/*
 * Brief			: whether the ch is one of the specifically letters
 * Description		:
 * Notes			:
 */
static bool IsSpecialCharacter(char ch)
{
    const char* ptr = "~!@#$%^&*()-_=+\\|[{}];:,<.>/?";
    while (*ptr != '\0') {
        if (*ptr == ch) {
            return true;
        }
        ptr++;
    }
    return false;
}

/*
 * Brief			: Whether the initial password satisfy the complexity requirement.
 * Description		:
 * Notes			:
 */
static bool CheckInitialPasswd(const char* dbuser, const char* pass_wd)
{
    int slen = 0;
    int kinds[4] = {0};
    int kinds_num = 0;
    const char* ptr = NULL;

    char* reverse_str = NULL;

    int i = 0;
    if (NULL == dbuser) {
        write_stderr(_("The parameter username of CheckInitialPasswd is NULL\n"));
        return false;
    }

    if (NULL == pass_wd) {
        write_stderr(_("The parameter passwd of CheckInitialPasswd is NULL\n"));
        return false;
    }

    /* passwd must be ascii code*/
    ptr = pass_wd;
    while (*ptr != '\0') {
        if (*ptr < 0 || (*ptr - 127) > 0) {
            write_stderr(_("The passwd must be ascii code."));
            return false;
        }
        ptr++;
    }

    /* passwd must contain at least eight characters*/
    slen = strlen(pass_wd);
    if (slen < 8) {
        write_stderr(_("passwd must contain at least eight characters\n"));
        return false;
    }

    /* passwd should not equal to the rolname*/
    if (0 == pg_strcasecmp(pass_wd, dbuser)) {
        write_stderr(_("Password should not equal to the rolname.\n"));
        return false;
    }
    /* passwd should not equal to the reverse of rolname, include upper and lower*/
    reverse_str = reverse_string(dbuser);
    if (NULL == reverse_str) {
        (void)write_stderr(_("reverse_string failed, possibility out of memory\n"));
        return false;
    }
    if (0 == pg_strcasecmp(pass_wd, reverse_str)) {
        FREE_AND_RESET(reverse_str);
        (void)write_stderr(_("Password should not equal to the reverse of rolname.\n"));
        return false;
    }
    FREE_AND_RESET(reverse_str);
    /* passwd must contain at least three kinds of characters*/
    ptr = pass_wd;
    while (*ptr != '\0') {
        if (*ptr >= 'A' && *ptr <= 'Z') {
            kinds[0]++;
        } else if (*ptr >= 'a' && *ptr <= 'z') {
            kinds[1]++;
        } else if (*ptr >= '0' && *ptr <= '9') {
            kinds[2]++;
        } else if (IsSpecialCharacter(*ptr)) {
            kinds[3]++;
        }
        ptr++;
    }
    for (i = 0; i != 4; ++i) {
        if (kinds[i] > 0) {
            kinds_num++;
        }
    }
    if (kinds_num < 3) {
        write_stderr(_("Password must contain at least three kinds of characters.\n"));
        return false;
    }
    return true;
}

/*
 * Brief		    : reverse_string()
 * Description	    : reverse the string
 */
static char* reverse_string(const char* str)
{
    int i;
    size_t len;
    char* new_string = NULL;
    len = strlen(str);
    new_string = (char*)malloc(len + 1);
    if (NULL == new_string) {
        return NULL;
    }
    for (i = 0; i < (int)len; ++i) {
        new_string[len - i - 1] = str[i];
    }
    new_string[len] = '\0';
    return new_string;
}

static void create_pg_lockfiles(void)
{
#define FILELOCK_SIZE 1024
#define FILELOCK_NAME 32
#define FILE_LIST 2

    char content[FILELOCK_SIZE] = {0};
    FILE* lockfd = NULL;
    char* filelockpath = NULL;
    const char* filelocklist[FILE_LIST] = {"pg_ctl.lock", "postgresql.conf.lock"};
    int nRet = 0;

    filelockpath = (char*)pg_malloc(strlen(pg_data) + FILELOCK_NAME);

    for (int i = 0; i < FILE_LIST; i++) {
        nRet = sprintf_s(filelockpath, strlen(pg_data) + FILELOCK_NAME, "%s/%s", pg_data, filelocklist[i]);
        securec_check_ss_c(nRet, "\0", "\0");
        canonicalize_path(filelockpath);
        lockfd = fopen(filelockpath, PG_BINARY_W);
        if (NULL == lockfd) {
            char errBuffer[ERROR_LIMIT_LEN];
            (void)write_stderr(_("%s: could not create lock file \"%s\" : %s\n"),
                progname,
                filelockpath,
                pqStrerror(errno, errBuffer, ERROR_LIMIT_LEN));
            FREE_AND_RESET(filelockpath);
            exit_nicely();
        }

        (void)fwrite(content, FILELOCK_SIZE, 1, lockfd);
        if (fclose(lockfd)) {
            char errBuffer[ERROR_LIMIT_LEN];
            (void)write_stderr(_("%s: close lock file \"%s\" failed: %s\n"),
                progname,
                filelockpath,
                pqStrerror(errno, errBuffer, ERROR_LIMIT_LEN));
            FREE_AND_RESET(filelockpath);
            exit_nicely();
        }
    }
    FREE_AND_RESET(filelockpath);
}

static bool InitUndoZoneMeta(int fd)
{
    int rc = 0;
    uint64 writeSize = 0;
    uint32 ret = 0;
    uint32 totalZonePageCnt = 0;
    char metaPageBuffer[UNDO_META_PAGE_SIZE] = {'\0'};
    pg_crc32 zoneMetaPageCrc = 0;

    /* Init undospace meta, persist meta info into disk. */
    UNDOZONE_META_PAGE_COUNT(PERSIST_ZONE_COUNT, UNDOZONE_COUNT_PER_PAGE, totalZonePageCnt);
    for (uint32 loop = 0; loop < PERSIST_ZONE_COUNT; loop++) {
        uint32 zoneId = loop;
        uint32 offset = zoneId % UNDOZONE_COUNT_PER_PAGE;
        undo::UndoZoneMetaInfo *uzoneMetaPoint = NULL;

        if (zoneId % UNDOZONE_COUNT_PER_PAGE == 0) {
            rc = memset_s(metaPageBuffer, UNDO_META_PAGE_SIZE, 0, UNDO_META_PAGE_SIZE);
            securec_check(rc, "\0", "\0");

            /* On last page, count of undospace meta maybe less than UNDOSPACE_COUNT_PER_PAGE. */
            if ((uint32)(zoneId / UNDOZONE_COUNT_PER_PAGE) + 1 == totalZonePageCnt) {
                writeSize = (PERSIST_ZONE_COUNT - (totalZonePageCnt - 1) * UNDOZONE_COUNT_PER_PAGE) *
                    sizeof(undo::UndoZoneMetaInfo);
            } else {
                writeSize = sizeof(undo::UndoZoneMetaInfo) * UNDOZONE_COUNT_PER_PAGE;
            }
        }

        uzoneMetaPoint = (undo::UndoZoneMetaInfo *)(metaPageBuffer + offset * sizeof(undo::UndoZoneMetaInfo));
        uzoneMetaPoint->version = UNDO_ZONE_META_VERSION;
        uzoneMetaPoint->lsn = 0;
        uzoneMetaPoint->insert = UNDO_LOG_BLOCK_HEADER_SIZE;
        uzoneMetaPoint->discard = UNDO_LOG_BLOCK_HEADER_SIZE;
        uzoneMetaPoint->forceDiscard = UNDO_LOG_BLOCK_HEADER_SIZE;
        uzoneMetaPoint->recycleXid = 0;
        uzoneMetaPoint->allocate = UNDO_LOG_BLOCK_HEADER_SIZE;
        uzoneMetaPoint->recycle = UNDO_LOG_BLOCK_HEADER_SIZE;

        if ((zoneId + 1) % UNDOZONE_COUNT_PER_PAGE == 0 || (zoneId == PERSIST_ZONE_COUNT - 1 &&
            ((uint32)(zoneId / UNDOZONE_COUNT_PER_PAGE) + 1 == totalZonePageCnt))) {
            INIT_CRC32C(zoneMetaPageCrc);
            COMP_CRC32C(zoneMetaPageCrc, (void *)metaPageBuffer, writeSize);
            FIN_CRC32C(zoneMetaPageCrc);
            *(pg_crc32 *)(metaPageBuffer + writeSize) = zoneMetaPageCrc;

            ret = write(fd, (void *)metaPageBuffer, UNDO_META_PAGE_SIZE);
            if (ret != UNDO_META_PAGE_SIZE) {
                printf("[INIT UNDO] Write undozone meta info fail, expect size(%u), real size(%u)",
                    UNDO_META_PAGE_SIZE, ret);
                return false;
            }
        }
    }
    return true;
}

static bool InitUndoSpaceMeta(int fd)
{
    int rc = 0;
    uint64 writeSize = 0;
    uint32 ret = 0;
    uint32 totalUspPageCnt = 0;
    char metaPageBuffer[UNDO_META_PAGE_SIZE] = {'\0'};
    pg_crc32 spaceMetaPageCrc = 0;

    /* Init undospace meta, persist meta info into disk. */
    UNDOZONE_META_PAGE_COUNT(PERSIST_ZONE_COUNT, UNDOSPACE_COUNT_PER_PAGE, totalUspPageCnt);
    for (uint32 loop = 0; loop < PERSIST_ZONE_COUNT; loop++) {
        uint32 zoneId = loop;
        uint32 offset = zoneId % UNDOSPACE_COUNT_PER_PAGE;
        undo::UndoSpaceMetaInfo *uspMetaPoint = NULL;

        if (zoneId % UNDOSPACE_COUNT_PER_PAGE == 0) {
            rc = memset_s(metaPageBuffer, UNDO_META_PAGE_SIZE, 0, UNDO_META_PAGE_SIZE);
            securec_check(rc, "\0", "\0");

            /* On last page, count of undospace meta maybe less than UNDOSPACE_COUNT_PER_PAGE. */
            if ((uint32)(zoneId / UNDOSPACE_COUNT_PER_PAGE) + 1 == totalUspPageCnt) {
                writeSize = (PERSIST_ZONE_COUNT - (totalUspPageCnt - 1) * UNDOSPACE_COUNT_PER_PAGE) *
                    sizeof(undo::UndoSpaceMetaInfo);
            } else {
                writeSize = sizeof(undo::UndoSpaceMetaInfo) * UNDOSPACE_COUNT_PER_PAGE;
            }
        }

        uspMetaPoint = (undo::UndoSpaceMetaInfo *)(metaPageBuffer + offset * sizeof(undo::UndoSpaceMetaInfo));
        uspMetaPoint->version = UNDO_SPACE_META_VERSION;
        uspMetaPoint->lsn = 0;
        uspMetaPoint->head = 0;
        uspMetaPoint->tail = 0;

        if ((zoneId + 1) % UNDOSPACE_COUNT_PER_PAGE == 0 || (zoneId == PERSIST_ZONE_COUNT - 1 &&
            ((uint32)(zoneId / UNDOSPACE_COUNT_PER_PAGE) + 1 == totalUspPageCnt))) {
            INIT_CRC32C(spaceMetaPageCrc);
            COMP_CRC32C(spaceMetaPageCrc, (void *)metaPageBuffer, writeSize);
            FIN_CRC32C(spaceMetaPageCrc);
            *(pg_crc32 *)(metaPageBuffer + writeSize) = spaceMetaPageCrc;

            ret = write(fd, (void *)metaPageBuffer, UNDO_META_PAGE_SIZE);
            if (ret != UNDO_META_PAGE_SIZE) {
                printf("[INIT UNDO] Write undospace meta info fail, expect size(%u), real size(%u)",
                    UNDO_META_PAGE_SIZE, ret);
                return false;
            }
        }
    }
    return true;
}

static void InitUndoSubsystemMeta(void)
{
    int rc = 0;
    char undoFilePath[MAXPGPATH] = {'\0'};
    char tmpUndoFile[MAXPGPATH] = {'\0'};

    printf("Begin init undo subsystem meta.\n");
    rc = sprintf_s(undoFilePath, sizeof(undoFilePath), "%s/%s", pg_data, UNDO_META_FILE);
    securec_check_ss_c(rc, "\0", "\0");
    rc = sprintf_s(tmpUndoFile, sizeof(tmpUndoFile), "%s/%s_%s", pg_data, UNDO_META_FILE, "tmp");
    securec_check_ss_c(rc, "\0", "\0");

    if (access(undoFilePath, F_OK) != 0) {
        /* First, delete tmpUndoFile. */
        unlink(tmpUndoFile);
        int fd = open(tmpUndoFile, O_RDWR | O_CREAT | O_EXCL | PG_BINARY, S_IRUSR | S_IWUSR);
        if (fd < 0) {
            printf("[INIT UNDO] Open %s file failed, error (%s).", tmpUndoFile, strerror(errno));
            return;
        }

        /* init undo zone meta */
        if (!InitUndoZoneMeta(fd)) {
            goto ERROR_PROC;
        }
        /* init undo space meta */
        if (!InitUndoSpaceMeta(fd)) {
            goto ERROR_PROC;
        }
        /* init slot space meta */
        if (!InitUndoSpaceMeta((fd))) {
            goto ERROR_PROC;
        }

        /* Flush buffer to disk and close fd. */
        fsync(fd);
        close(fd);

        /* Rename tmpUndoFile to real undoFile. */
        if (rename(tmpUndoFile, undoFilePath) != 0) {
            printf("[INIT UNDO] Rename tmp undo meta file failed.\n");
            unlink(tmpUndoFile);
            goto ERROR_PROC;
        }

        printf("[INIT UNDO] Init undo subsystem meta successfully.\n");
        return;

    ERROR_PROC:
        close(fd);
        unlink(tmpUndoFile);
        printf("[INIT UNDO] Init undo subsystem meta failed, exit.\n");
        exit(1);
    }
}
