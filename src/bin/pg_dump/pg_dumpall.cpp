/* -------------------------------------------------------------------------
 *
 * pg_dumpall.c
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * src/bin/pg_dump/pg_dumpall.c
 *
 * -------------------------------------------------------------------------
 */

#include "postgres_fe.h"
#include <unistd.h>
#include <sys/time.h>

#ifdef ENABLE_NLS
#include <locale.h>
#endif

#include "getopt_long.h"

#include "dumputils.h"
#include "dumpmem.h"
#include "pg_backup.h"
#include "pg_dumpall.h"

#include "bin/elog.h"
#include "pgtime.h"

#include "openssl/rand.h"

#ifdef PGXC
#include "catalog/pg_resource_pool.h"
#include "catalog/pg_workload_group.h"
#include "catalog/pg_app_workloadgroup_mapping.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_tablespace.h"
#include "catalog/pg_user_status.h"
#include "catalog/pgxc_group.h"
#include "catalog/pgxc_node.h"
#include "catalog/pg_database.h"
#endif

#include "catalog/pg_extension_data_source.h"
#ifdef GAUSS_SFT_TEST
#include "gauss_sft.h"
#endif

#define PROG_NAME "gs_dumpall"

/* version string we expect back from pg_dump */
#define PGDUMP_VERSIONSTR "gs_dump " DEF_GS_VERSION "\n"
#define atoxid(x) ((TransactionId)strtoul((x), NULL, 10))

#ifdef ENABLE_UT
#define static
#endif

static void dropRoles(PGconn* conn);
static void dumpRoles(PGconn* conn);
static void dumpRoleMembership(PGconn* conn);
static void dumpGroups(PGconn* conn);
static void dropTablespaces(PGconn* conn);
static void dumpTablespaces(PGconn* conn);
static void dropDBs(PGconn* conn);
static void dumpCreateDB(PGconn* conn);
static void dumpDatabaseConfig(PGconn* conn, const char* dbname);
static void dumpUserConfig(PGconn* conn, const char* username);
static void dumpDbRoleConfig(PGconn* conn);
static void makeAlterConfigCommand(
    PGconn* conn, const char* arrayitem, const char* type, const char* name, const char* type2, const char* name2);
static void dumpDatabases(PGconn* conn);
static void dumpTimestamp(const char* msg);
static void doShellQuoting(PQExpBuffer buf, const char* str);
static void doShellQuotingForRandomstring(PQExpBuffer buf, const char* str);
static void doConnStrQuoting(PQExpBuffer buf, const char* str);

static char* formDumpCommand(const char* dbname, const char* dbfilename);
static char* getDatabaseFilename(const char* dbname);
static void executePopenCommandsParallel(const char* cmd, char* dbname);
static void readPopenOutputParallel();
static void SleepInMilliSec(uint32_t sleepMs);

static void buildShSecLabels(PGconn* conn, const char* catalog_name, uint32 objectId, PQExpBuffer buffer,
    const char* target, const char* objname);
static PGconn* connectDatabase(const char* dbname, const char* pghost, const char* pgport, const char* pguser,
    const char* pchPasswd, enum trivalue prompt_password, bool fail_on_error);
static PGresult* executeQuery(PGconn* conn, const char* query);
static void executeCommand(PGconn* conn, const char* query);
static void getopt_dumpall(int argc, char** argv, struct option options[], int* result);
static void check_encrypt_parameters_dumpall(const char* pEncrypt_mode, const char* pEncrypt_key);
static int dumpall_write(const void* ptr, size_t size, size_t nmemb, FILE* fp);
static int dumpall_printf(FILE* fp, const char* fmt, ...) __attribute__((format(PG_PRINTF_ATTRIBUTE, 2, 3)));
static void validate_dumpall_options(char** argv);
static void do_dumpall(PGconn* conn);
static PQExpBuffer handleTblSpcOpt(char* spcoptions);

#ifdef PGXC
static void dumpNodes(PGconn* conn);
static void dumpNodeGroups(PGconn* conn);
static void dumpResourcePools(PGconn* conn);
static void dumpWorkloadGroups(PGconn* conn);
static void dumpAppWorkloadGroupMapping(PGconn* conn);
#endif /* PGXC */

static void dumpDataSource(PGconn* conn);

static char pg_dump_bin[MAXPGPATH];
static PQExpBuffer pgdumpopts;
static bool skip_acls = false;
static bool verbose = false;

static int binary_upgrade = 0;
static int column_inserts = 0;
static int disable_dollar_quoting = 0;
static int disable_triggers = 0;
static int inserts = 0;
static int no_tablespaces = 0;
static int use_setsessauth = 0;
static int no_security_labels = 0;
static int no_unlogged_table_data = 0;
static int server_version;
static int non_Lock_Table = 0;
static int include_alter_table = 0;
static FILE* OPF;
static char* filename = NULL;
static char* filepath = NULL;

static bool data_only = false;
static bool schema_only = false;
static bool output_clean = false;
static bool globals_only = false;
static char* pghost = NULL;
static char* pgdb = NULL;
static char* pgport = NULL;
static bool roles_only = false;
static bool tablespaces_only = false;
static char* tablespaces_postfix_path = NULL;
static char* pguser = NULL;
static enum trivalue prompt_password = TRI_DEFAULT;
static char* use_role = NULL;
static char* rolepasswd = NULL;
static char* passwd = NULL;
static bool dont_overwritefile = false;
static bool dump_templatedb = false;
static char* parallel_jobs = NULL;
static bool is_pipeline = false;
static int no_subscriptions = 0;
static int no_publications = 0;

GS_UCHAR init_rand[RANDOM_LEN + 1] = {0};
#define RAND_COUNT 100

/* Database Security: Data importing/dumping support AES128. */
const char* encrypt_mode = NULL;
const char* encrypt_key = NULL;

#ifdef ENABLE_MULTIPLE_NODES
static int include_nodes = 0;
#endif

#ifdef PGXC
static int dump_nodes = 0;
static int include_buckets = 0;
static int dump_wrm = 0;
#endif /* PGXC */

#ifdef GSDUMP_LLT
bool lltRunning = true;
void stopLLT()
{
    lltRunning = false;
}
#endif /* PGXC */

static void generateRandArray();
static void free_dumpall();
static void get_password_pipeline();
static void get_role_password();
static void get_encrypt_key();

int main(int argc, char* argv[])
{
    PGconn* conn = NULL;
    int encoding = 0;
    const char* std_strings = NULL;
    int ret = 0;
    int optindex = 0;
    struct timeval aes_start_time;
    struct timeval aes_end_time;
    pg_time_t total_time = 0;
    errno_t rc = 0;

    static struct option long_options[] = {{"data-only", no_argument, NULL, 'a'},
        {"clean", no_argument, NULL, 'c'},
        {"file", required_argument, NULL, 'f'},
        {"globals-only", no_argument, NULL, 'g'},
        {"host", required_argument, NULL, 'h'},
        {"database", required_argument, NULL, 'l'},
        {"oids", no_argument, NULL, 'o'},
        {"no-owner", no_argument, NULL, 'O'},
        {"port", required_argument, NULL, 'p'},
        {"roles-only", no_argument, NULL, 'r'},
        {"schema-only", no_argument, NULL, 's'},
        {"sysadmin", required_argument, NULL, 'S'},
        {"tablespaces-only", no_argument, NULL, 't'},
        {"tablespaces-postfix", required_argument, NULL, 'T'},
        {"username", required_argument, NULL, 'U'},
        {"verbose", no_argument, NULL, 'v'},
        {"no-password", no_argument, NULL, 'w'},
        {"password", required_argument, NULL, 'W'},
        {"no-privileges", no_argument, NULL, 'x'},
        {"no-acl", no_argument, NULL, 'x'},

        /*
         * the following options don't have an equivalent short option letter
         */
        {"attribute-inserts", no_argument, &column_inserts, 1},
        {"binary-upgrade", no_argument, &binary_upgrade, 1},
        {"non-lock-table", no_argument, &non_Lock_Table, 1},
        {"column-inserts", no_argument, &column_inserts, 1},
        {"disable-dollar-quoting", no_argument, &disable_dollar_quoting, 1},
        {"disable-triggers", no_argument, &disable_triggers, 1},
        {"inserts", no_argument, &inserts, 1},
        {"lock-wait-timeout", required_argument, NULL, 2},
        {"no-tablespaces", no_argument, &no_tablespaces, 1},
        {"quote-all-identifiers", no_argument, &quote_all_identifiers, 1},
        {"role", required_argument, NULL, 3},
        {"rolepassword", required_argument, NULL, 5},
        /* Database Security: Data importing/dumping support AES128. */
        {"with-encryption", required_argument, NULL, 6},
        {"with-key", required_argument, NULL, 7},
        {"dont-overwrite-file", no_argument, NULL, 8},
        {"use-set-session-authorization", no_argument, &use_setsessauth, 1},
#if !defined(ENABLE_MULTIPLE_NODES)
        {"no-publications", no_argument, &no_publications, 1},
#endif
        {"no-security-labels", no_argument, &no_security_labels, 1},
#if !defined(ENABLE_MULTIPLE_NODES)
        {"no-subscriptions", no_argument, &no_subscriptions, 1},
#endif
        {"no-unlogged-table-data", no_argument, &no_unlogged_table_data, 1},
        {"include-alter-table", no_argument, &include_alter_table, 1},
#ifdef ENABLE_MULTIPLE_NODES
        {"dump-nodes", no_argument, &dump_nodes, 1},
        {"include-nodes", no_argument, &include_nodes, 1},
        {"include-buckets", no_argument, &include_buckets, 1},
        {"dump-wrm", no_argument, &dump_wrm, 1},
#endif
        {"binary-upgrade-usermap", required_argument, NULL, 9},
        {"include-extensions", no_argument, NULL, 10},
        {"include-templatedb", no_argument, NULL, 11},
        {"parallel-jobs", required_argument, NULL, 12},
        {"pipeline", no_argument, NULL, 13},
        {NULL, 0, NULL, 0}};

    set_pglocale_pgservice(argv[0], PG_TEXTDOMAIN("gs_dump"));
#ifdef GSDUMP_LLT
    while (lltRunning) {
        sleep(3);
    }
    return 0;
#endif

    progname = get_progname("gs_dumpall");
    gettimeofday(&aes_start_time, NULL);

    if (argc > 1) {
        if (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-?") == 0) {
            help();
            exit_nicely(0);
        }
        if (strcmp(argv[1], "--version") == 0 || strcmp(argv[1], "-V") == 0) {
            puts("gs_dumpall " DEF_GS_VERSION);
            exit_nicely(0);
        }
    }

    if ((ret = find_other_exec(argv[0], "gs_dump", PGDUMP_VERSIONSTR, pg_dump_bin)) < 0) {
        char full_path[MAXPGPATH] = {0};

        if (find_my_exec(argv[0], full_path) < 0) {
            errno_t err = EOK;
            err = strcpy_s(full_path, sizeof(full_path), progname);
            securec_check_c(err, "\0", "\0");
        }

        if (ret == -1)
            write_stderr(_("The program \"gs_dump\" is needed by %s "
                           "but was not found in the\n"
                           "same directory as \"%s\".\n"
                           "Check your installation.\n"),
                progname,
                full_path);
        else
            write_stderr(_("The program \"gs_dump\" was found by \"%s\"\n"
                           "but was not the same version as %s.\n"
                           "Check your installation.\n"),
                full_path,
                progname);
        exit_nicely(1);
    }

    /* parse the dumpall options */
    getopt_dumpall(argc, argv, long_options, &optindex);

    if (is_pipeline) {
        get_password_pipeline();
    }

    /* validate the optons values */
    validate_dumpall_options(argv);

    /*
     * If there was a database specified on the command line, use that,
     * otherwise try to connect to database "postgres", and failing that
     * "template1".  "postgres" is the preferred choice for 8.1 and later
     * servers, but it usually will not exist on older ones.
     */
    if (pgdb != NULL) {
        conn = connectDatabase(pgdb, pghost, pgport, pguser, passwd, prompt_password, false);

        if (conn == NULL) {
            write_stderr(_("%s: could not connect to database \"%s\"\n"), progname, pgdb);

            /* Clear password related memory to avoid leaks when exit. */
            if (passwd != NULL) {
                rc = memset_s(passwd, strlen(passwd), 0, strlen(passwd));
                securec_check_c(rc, "\0", "\0");
            }
            char* pqpass = PQpass(conn);
            if (pqpass != NULL) {
                rc = memset_s(pqpass, strlen(pqpass), 0, strlen(pqpass));
                securec_check_c(rc, "\0", "\0");
            }
            if (rolepasswd != NULL) {
                rc = memset_s(rolepasswd, strlen(rolepasswd), 0, strlen(rolepasswd));
                securec_check_c(rc, "\0", "\0");
            }

            exit_nicely(1);
        }
    } else {
        conn = connectDatabase("postgres", pghost, pgport, pguser, passwd, prompt_password, false);

        if (conn == NULL) {
            conn = connectDatabase("template1", pghost, pgport, pguser, passwd, prompt_password, true);
        }

        if (conn == NULL) {
            write_stderr(_("%s: could not connect to databases \"postgres\" or \"template1\"\n"
                           "Please specify an alternative database.\n"),
                progname);
            write_stderr(_("Try \"%s --help\" for more information.\n"), progname);

            /* Clear password related memory to avoid leaks when exit. */
            if (passwd != NULL) {
                rc = memset_s(passwd, strlen(passwd), 0, strlen(passwd));
                securec_check_c(rc, "\0", "\0");
            }
            char* pqpass = PQpass(conn);
            if (pqpass != NULL) {
                rc = memset_s(pqpass, strlen(pqpass), 0, strlen(pqpass));
                securec_check_c(rc, "\0", "\0");
            }
            if (rolepasswd != NULL) {
                rc = memset_s(rolepasswd, strlen(rolepasswd), 0, strlen(rolepasswd));
                securec_check_c(rc, "\0", "\0");
            }
            exit_nicely(1);
        }
    }

    /* Now the passwd is no longer needed, you can clean up it */
    if (passwd != NULL) {
        rc = memset_s(passwd, strlen(passwd), 0, strlen(passwd));
        securec_check_c(rc, "\0", "\0");
        free(passwd);
        passwd = NULL;
    }

    /*
     * Open the output file if required, otherwise use stdout
     */
    if (filename != NULL) {
        if ((dont_overwritefile == true) && (fileExists(filename) == true)) {
            write_msg(NULL,
                "Dumpall File specified already exists.\n"
                "Perform dumpall again by: \n1) specifying a different file "
                "or \n2) removing the specified file "
                "or \n3) Not specifying dont-overwrite-file option\n");
            /* Clear password related memory to avoid leaks when exit. */
            char* pqpass = PQpass(conn);
            if (pqpass != NULL) {
                rc = memset_s(pqpass, strlen(pqpass), 0, strlen(pqpass));
                securec_check_c(rc, "\0", "\0");
            }
            if (rolepasswd != NULL) {
                rc = memset_s(rolepasswd, strlen(rolepasswd), 0, strlen(rolepasswd));
                securec_check_c(rc, "\0", "\0");
            }

            exit_nicely(1);
        }

        // Prevent from concurrent dump operations on same file
        catalog_lock(filename);
        on_exit_nicely(catalog_unlock, NULL);

        OPF = fopen(filename, PG_BINARY_W);
        if (OPF == NULL) {
            write_stderr(_("%s: could not open the output file \"%s\": %s\n"), progname, filename, strerror(errno));
            /* Clear password related memory to avoid leaks when exit. */
            char* pqpass = PQpass(conn);
            if (pqpass != NULL) {
                rc = memset_s(pqpass, strlen(pqpass), 0, strlen(pqpass));
                securec_check_c(rc, "\0", "\0");
            }
            if (rolepasswd != NULL) {
                rc = memset_s(rolepasswd, strlen(rolepasswd), 0, strlen(rolepasswd));
                securec_check_c(rc, "\0", "\0");
            }

            exit_nicely(1);
        }
    } else
        OPF = stdout;

    if (instport == NULL) {
        instport = gs_strdup(PQport(conn));
    }

#ifndef ENABLE_MULTIPLE_NODES
    /*
     * During gs_dumpall, PQfnumber() is matched according to the lowercase column name.
     * However, when uppercase_attribute_name is on, the column names in the result set
     * will be converted to uppercase. So we need to turn off it temporarily. We don't
     * need to turn it on cause this connection is for gs_dumpall only, will not affect others.
     */
    if (!SetUppercaseAttributeNameToOff(conn)) {
        write_stderr(_("%s: set uppercase_attribute_name to off failed.\n"), progname);
        exit_nicely(1);
    }
#endif

    /*
     * Get the active encoding and the standard_conforming_strings setting, so
     * we know how to escape strings.
     */
    encoding = PQclientEncoding(conn);
    std_strings = PQparameterStatus(conn, "standard_conforming_strings");
    if (std_strings == NULL) {
        std_strings = "off";
    }

    if ((use_role != NULL) && (rolepasswd == NULL)) {
        get_role_password();
    }

    /* Set the role if requested */
    if (use_role != NULL && server_version >= 80100) {
        PGresult* res = NULL;
        PQExpBuffer query = createPQExpBuffer();
        char* retrolepasswd = PQescapeLiteral(conn, rolepasswd, strlen(rolepasswd));

        if (retrolepasswd == NULL) {
            /* Clear password related memory to avoid leaks when exit. */
            char* pqpass = PQpass(conn);
            if (pqpass != NULL) {
                rc = memset_s(pqpass, strlen(pqpass), 0, strlen(pqpass));
                securec_check_c(rc, "\0", "\0");
            }
            if (rolepasswd != NULL) {
                rc = memset_s(rolepasswd, strlen(rolepasswd), 0, strlen(rolepasswd));
                securec_check_c(rc, "\0", "\0");
            }
            write_msg(NULL, "Failed to escapes a string for an SQL command: %s\n", PQerrorMessage(conn));
            exit_nicely(1);
        }

        appendPQExpBuffer(query, "SET ROLE %s PASSWORD %s", fmtId(use_role), retrolepasswd);
        PQfreemem(retrolepasswd);

        res = PQexec(conn, query->data);
        if (PQresultStatus(res) != PGRES_COMMAND_OK) {
            fprintf(stderr, _("%s: query failed: %s"), progname, PQerrorMessage(conn));
            printf(_("%s: query failed: %s"), progname, PQerrorMessage(conn));

            /* Clear password related memory to avoid leaks when core. */
            char* pqpass = PQpass(conn);
            if (pqpass != NULL) {
                rc = memset_s(pqpass, strlen(pqpass), 0, strlen(pqpass));
                securec_check_c(rc, "\0", "\0");
            }
            if (rolepasswd != NULL) {
                rc = memset_s(rolepasswd, strlen(rolepasswd), 0, strlen(rolepasswd));
                securec_check_c(rc, "\0", "\0");
            }
            if (query->data != NULL) {
                rc = memset_s(query->data, strlen(query->data), 0, strlen(query->data));
                securec_check_c(rc, "\0", "\0");
            }

            PQclear(res);
            destroyPQExpBuffer(query);
            exit_nicely(1);
        }

        PQclear(res);
        if (query->data != NULL) {
            rc = memset_s(query->data, strlen(query->data), 0, strlen(query->data));
            securec_check_c(rc, "\0", "\0");
        }

        destroyPQExpBuffer(query);
    }

    if (rolepasswd != NULL) {
        rc = memset_s(rolepasswd, strlen(rolepasswd), 0, strlen(rolepasswd));
        securec_check_c(rc, "\0", "\0");
        free(rolepasswd);
        rolepasswd = NULL;
    }

    /* Force quoting of all identifiers if requested. */
    if (quote_all_identifiers && server_version >= 90100)
        executeCommand(conn, "SET quote_all_identifiers = true");

    if ('\0' != init_rand[0]) {
        dumpall_printf(OPF, "%s", init_rand);
    }

    dumpall_printf(OPF, "--\n-- openGauss database cluster dump\n--\n\n");
    if (verbose)
        dumpTimestamp((char*)"Started on");

    /*
     * We used to emit \connect postgres here, but that served no purpose
     * other than to break things for installations without a postgres
     * database.  Everything we're restoring here is a global, so whichever
     * database we're connected to at the moment is fine.
     */

    /* Replicate encoding and std_strings in output */
    dumpall_printf(OPF, "SET client_encoding = '%s';\n", pg_encoding_to_char(encoding));

    dumpall_printf(OPF, "SET standard_conforming_strings = %s;\n", std_strings);

    if (strcmp(std_strings, "off") == 0)
        dumpall_printf(OPF, "SET escape_string_warning = off;\n");
    dumpall_printf(OPF, "\n");
    do_dumpall(conn);

    /* Clear password related memory to avoid leaks when core. */
    char* pqpass = PQpass(conn);
    if (pqpass != NULL) {
        rc = memset_s(pqpass, strlen(pqpass), 0, strlen(pqpass));
        securec_check_c(rc, "\0", "\0");
    }
    PQfinish(conn);

    if (verbose)
        dumpTimestamp((char*)"Completed on");
    dumpall_printf(OPF, "--\n-- openGauss database cluster dump complete\n--\n\n");

    if (filename != NULL) {
        fclose(OPF);
        OPF = NULL;
    }

    write_msg(NULL, "dumpall operation successful\n");

    gettimeofday(&aes_end_time, NULL);
    total_time =
        1000 * (aes_end_time.tv_sec - aes_start_time.tv_sec) + (aes_end_time.tv_usec - aes_start_time.tv_usec) / 1000;
    write_msg(NULL, "total time: %lld  ms\n", (long long int)total_time);

    free_dumpall();

    exit_nicely(0);
}

static void get_password_pipeline()
{
    int pass_max_len = 1024;
    char* pass_buf = NULL;
    errno_t rc = EOK;

    if (isatty(fileno(stdin))) {
        exit_horribly(NULL, "Terminal is not allowed to use --pipeline\n");
    }

    pass_buf = (char*)pg_malloc(pass_max_len);
    rc = memset_s(pass_buf, pass_max_len, 0, pass_max_len);
    securec_check_c(rc, "\0", "\0");

    if (passwd != NULL) {
        errno_t rc = memset_s(passwd, strlen(passwd), 0, strlen(passwd));
        securec_check_c(rc, "\0", "\0");
        GS_FREE(passwd);
    }

    if (NULL != fgets(pass_buf, pass_max_len, stdin)) {
        prompt_password = TRI_YES;
        appendPQExpBuffer(pgdumpopts, " -W");
        pass_buf[strlen(pass_buf) - 1] = '\0';
        passwd = gs_strdup(pass_buf);
        doShellQuoting(pgdumpopts, passwd);
    }

    rc = memset_s(pass_buf, pass_max_len, 0, pass_max_len);
    securec_check_c(rc, "\0", "\0");
    free(pass_buf);
    pass_buf = NULL;

}

static void get_role_password() {
    GS_FREE(rolepasswd);
    rolepasswd = simple_prompt("Role Password: ", 100, false);
    if (rolepasswd == NULL) {
        exit_horribly(NULL, "out of memory\n");
    }
}

static void get_encrypt_key()
{
    GS_FREE(encrypt_key);
    encrypt_key = simple_prompt("Encrypt Key: ", MAX_PASSWDLEN, false);
    if (encrypt_key == NULL) {
        exit_horribly(NULL, "out of memory\n");
    }
    appendPQExpBuffer(pgdumpopts, " --with-key ");
    doShellQuoting(pgdumpopts, encrypt_key);
    generateRandArray();
    appendPQExpBuffer(pgdumpopts, " --with-salt ");
    /*
     * --with-salt comes from random generation, so we need to make sure it doesn't contain '\n' and '\r'
     */
    doShellQuotingForRandomstring(pgdumpopts, (char*)init_rand);
}

static void free_dumpall()
{
    GS_FREE(binary_upgrade_oldowner);
    GS_FREE(pghost);
    GS_FREE(pgdb);
    GS_FREE(pgport);
    GS_FREE(tablespaces_postfix_path);
    GS_FREE(pguser);
    GS_FREE(use_role);
    GS_FREE(encrypt_mode);
    GS_FREE(encrypt_key);
    GS_FREE(parallel_jobs);
    GS_FREE(filename);
}

/**
 * Database Security: Data importing/dumping support AES128.
 * Funcation  : check_encrypt_parameters
 * Description: check the input encrypt_mode must be AES128 ,currently only AES128 is supported,encrypt_key must be
 *                   16Bytes length.
 */
static void check_encrypt_parameters_dumpall(const char* pEncrypt_mode, const char* pEncrypt_key)
{
    if (pEncrypt_mode == NULL && pEncrypt_key == NULL) {
        return;
    }
    if (pEncrypt_mode == NULL) {
        exit_horribly(NULL, "No encryption method,only AES128 is available\n");
    }
    if (0 != strcmp(pEncrypt_mode, "AES128")) {
        exit_horribly(NULL, "%s is not supported,only AES128 is available\n", pEncrypt_mode);
    }
    if (pEncrypt_key == NULL) {
        exit_horribly(NULL, "No key for encryption,please input the key\n");
    }

    if (!check_input_password(pEncrypt_key)) {
        exit_horribly(NULL, "The input key must be %d~%d bytes and "
            "contain at least three kinds of characters!\n",
            MIN_KEY_LEN, MAX_KEY_LEN);
    }

    return;
}

static int dumpall_write(const void* ptr, size_t size, size_t nmemb, FILE* fp)
{
    size_t res;
    /* Database Security: Data importing/dumping support AES128. */
    bool encrypt_result = false;

    /* Database Security: Data importing/dumping support AES128. */
    if (encrypt_mode != NULL && encrypt_key != NULL) {
        res = size * nmemb;

        /* size of the data for the encryption is zero then return it from there */
        if (res == 0)
            return 0;

        encrypt_result = writeFileAfterEncryption(
            (FILE*)fp, (char*)ptr, (size * nmemb), MAX_DECRYPT_BUFF_LEN, (unsigned char*)encrypt_key, init_rand);
        if (!encrypt_result)
            exit_horribly(NULL, "Encryption failed: %s\n", strerror(errno));
    } else {
        res = fwrite(ptr, size, nmemb, (FILE*)fp);
        if (res != nmemb)
            exit_horribly(NULL, "could not write to output file: %s\n", strerror(errno));
    }

    return res;
}

/*
 *  Print formatted text to the output file (usually stdout).
 */
static int dumpall_printf(FILE* fp, const char* fmt, ...)
{
    char* p = NULL;
    va_list ap;
    int bSize = (int)(strlen(fmt) + 256); /* Usually enough */
    int cnt = -1;

    /*
     * This is paranoid: deal with the possibility that vsnprintf is willing
     * to ignore trailing null or returns > 0 even if string does not fit. It
     * may be the case that it returns cnt = bufsize.
     */
    while (cnt < 0 || cnt >= (bSize - 1)) {
        if (p != NULL) {
            free(p);
            p = NULL;
        }
        bSize *= 2;
        p = (char*)pg_malloc(bSize);
        va_start(ap, fmt);

        /* The loop process verifies the return value of the dangerous function, so no new verification function is
         * needed. At the same time, the new verification function will cause abnormal logic in the process.
         */
        cnt = vsnprintf_s(p, bSize, bSize - 1, fmt, ap);
        va_end(ap);
    }

    cnt = dumpall_write(p, 1, cnt, fp);
    free(p);
    p = NULL;
    return cnt;
}

static void do_dumpall(PGconn* conn)
{
    if (!data_only) {
        /*
         * If asked to --clean, do that first.	We can avoid detailed
         * dependency analysis because databases never depend on each other,
         * and tablespaces never depend on each other.	Roles could have
         * grants to each other, but DROP ROLE will clean those up silently.
         */
        if (output_clean) {
            if (!globals_only && !roles_only && !tablespaces_only)
                dropDBs(conn);

            if (!roles_only && !no_tablespaces) {
                if (server_version >= 80000)
                    dropTablespaces(conn);
            }

            if (!tablespaces_only)
                dropRoles(conn);
        }

        /*
         * Now create objects as requested.  Be careful that option logic here
         * is the same as for drops above.
         */
        if (!tablespaces_only) {
            /* Dump roles (users) */
            dumpRoles(conn);

            /* Dump role memberships --- need different method for pre-8.1 */
            if (server_version >= 80100)
                dumpRoleMembership(conn);
            else
                dumpGroups(conn);

            /*
             * dump Data Source
             * Note: the Data Source need info of system roles
             */
            dumpDataSource(conn);
        }

        if (!roles_only && !no_tablespaces) {
            /* Dump tablespaces */
            if (server_version >= 80000)
                dumpTablespaces(conn);
        }

        /* Dump CREATE DATABASE commands */
        if (!globals_only && !roles_only && !tablespaces_only)
            dumpCreateDB(conn);

        /* Dump role/database settings */
        if (!tablespaces_only && !roles_only) {
            if (server_version >= 90000)
                dumpDbRoleConfig(conn);
        }

#ifdef PGXC
        /* Dump nodes and node groups */
        if (dump_nodes) {
            dumpNodes(conn);
            dumpNodeGroups(conn);
        }

        /* Dump workload resource manager settings */
        if (dump_wrm) {
            dumpResourcePools(conn);
            dumpWorkloadGroups(conn);
            dumpAppWorkloadGroupMapping(conn);
        }
#endif
    }

    if (!globals_only && !roles_only && !tablespaces_only)
        dumpDatabases(conn);
}

/*
 * When do dumpall, dump will be called --with-salt.
 * Because all database encrypted files are written to the same file,
 * So it must ensure that the parameters passed in when they are called are consistent.
 * Especially hand --with-salt
 */
/* Get a random values as salt for encrypt */
static void generateRandArray()
{
    GS_UINT32 retval = 0;
    bool is_rand_ok = true;
    int k = 0;
    int i = 0;

    while (k++ < RAND_COUNT) {
        is_rand_ok = true;
        retval = RAND_priv_bytes(init_rand, RANDOM_LEN);
        if (retval != 1) {
            exit_horribly(NULL, "Generate random key failed\n");
        }
        for (i = 0; i < RANDOM_LEN; i++) {
            if (init_rand[i] == '\n' || init_rand[i] == '\r' || init_rand[i] == '\0') {
                is_rand_ok = false;
                break;
            }
        }
        if (is_rand_ok) {
            break;
        }
    }
    if (!is_rand_ok) {
        exit_horribly(NULL, "Generate random key failed.\n");
    }
}

/* parse the options for the dumpall */
static void getopt_dumpall(int argc, char** argv, struct option options[], int* result)
{
    int c;
    pgdumpopts = createPQExpBuffer();

    while ((c = getopt_long(argc, argv, "acf:gh:l:oOp:rsS:tT:U:W:vwx", options, result)) != -1) {
        switch (c) {
            case 'a':
                data_only = true;
                appendPQExpBuffer(pgdumpopts, " -a");
                break;

            case 'c':
                output_clean = true;
                break;

            case 'f':
                GS_FREE(filepath);
		GS_FREE(filename);
                check_env_value_c(optarg);
                filepath = gs_strdup(optarg);
                filename = make_absolute_path(filepath);
                free(filepath);
                filepath = NULL;
                break;

            case 'g':
                globals_only = true;
                break;

            case 'h':
                GS_FREE(pghost);
                check_env_value_c(optarg);
                pghost = gs_strdup(optarg);
                appendPQExpBuffer(pgdumpopts, " -h ");
                doShellQuoting(pgdumpopts, pghost);
                break;

            case 'l':
                GS_FREE(pgdb);
                check_env_value_c(optarg);
                pgdb = gs_strdup(optarg);
                break;

            case 'o':
                appendPQExpBuffer(pgdumpopts, " -o");
                break;

            case 'O':
                appendPQExpBuffer(pgdumpopts, " -O");
                break;

            case 'p':
                GS_FREE(pgport);
                check_env_value_c(optarg);
                pgport = gs_strdup(optarg);
                appendPQExpBuffer(pgdumpopts, " -p ");
                doShellQuoting(pgdumpopts, pgport);
                break;

            case 'r':
                roles_only = true;
                break;

            case 's':
                schema_only = true;
                appendPQExpBuffer(pgdumpopts, " -s");
                break;

            case 'S':
                check_env_value_c(optarg);
                appendPQExpBuffer(pgdumpopts, " -S ");
                doShellQuoting(pgdumpopts, optarg);
                break;

            case 't':
                tablespaces_only = true;
                break;

            case 'T':
                GS_FREE(tablespaces_postfix_path);
                tablespaces_postfix_path = gs_strdup(optarg);
                break;

            case 'U':
                GS_FREE(pguser);
                check_env_value_c(optarg);
                pguser = gs_strdup(optarg);
                appendPQExpBuffer(pgdumpopts, " -U ");
                doShellQuoting(pgdumpopts, pguser);
                break;

            case 'v':
                verbose = true;
                appendPQExpBuffer(pgdumpopts, " -v");
                break;

            case 'w':
                prompt_password = TRI_NO;
                appendPQExpBuffer(pgdumpopts, " -w");
                break;

            case 'W':
                prompt_password = TRI_YES;
                appendPQExpBuffer(pgdumpopts, " -W");
                if (passwd != NULL) {
                    errno_t rc = memset_s(passwd, strlen(passwd), 0, strlen(passwd));
                    securec_check_c(rc, "\0", "\0");
                }
                GS_FREE(passwd);
                passwd = gs_strdup(optarg);
                replace_password(argc, argv, "-W");
                replace_password(argc, argv, "--password");
                doShellQuoting(pgdumpopts, passwd);
                break;

            case 'x':
                skip_acls = true;
                appendPQExpBuffer(pgdumpopts, " -x");
                break;

            case 0:
                break;

            case 2:
                appendPQExpBuffer(pgdumpopts, " --lock-wait-timeout ");
                doShellQuoting(pgdumpopts, optarg);
                break;

            case 3:
                GS_FREE(use_role);
                check_env_value_c(optarg);
                use_role = gs_strdup(optarg);
                appendPQExpBuffer(pgdumpopts, " --role ");
                doShellQuoting(pgdumpopts, use_role);
                break;

            case 5: /* ROLE PASSWD */
                GS_FREE(rolepasswd);
                rolepasswd = gs_strdup(optarg);
                appendPQExpBuffer(pgdumpopts, " --rolepassword ");
                replace_password(argc, argv, "--rolepassword");
                doShellQuoting(pgdumpopts, rolepasswd);
                break;

            /* Database Security: Data importing/dumping support AES128. */
            case 6: /* AES mode , only AES128 is available */
                GS_FREE(encrypt_mode);
                encrypt_mode = gs_strdup(optarg);
                appendPQExpBuffer(pgdumpopts, " --with-encryption ");
                doShellQuoting(pgdumpopts, encrypt_mode);
                break;

            case 7: /* AES encryption key */
                GS_FREE(encrypt_key);
                encrypt_key = gs_strdup(optarg);
                appendPQExpBuffer(pgdumpopts, " --with-key ");
                replace_password(argc, argv, "--with-key");
                doShellQuoting(pgdumpopts, encrypt_key);
                generateRandArray();
                appendPQExpBuffer(pgdumpopts, " --with-salt ");
                /*
                 * --with-salt comes from random generation, so we need to make sure it doesn't contain '\n' and '\r'
                 */
                doShellQuotingForRandomstring(pgdumpopts, (char*)init_rand);

                break;

            case 8: /* overwrite file, if file is present */
                dont_overwritefile = true;
                break;

            case 9: /* BINARY UPGRADE USERMAP */
            {
                char* temp = NULL;
                GS_FREE(binary_upgrade_oldowner);
                binary_upgrade_oldowner = gs_strdup(optarg);
                if (NULL == (temp = strchr(binary_upgrade_oldowner, '='))) {
                    fprintf(stderr, _("%s: invalid binary upgrade usermap\n"), progname);
                    fprintf(stderr, _("Try \"%s --help\" for more information.\n"), progname);
                    exit_nicely(1);
                } else {
                    temp[0] = '\0';
                    binary_upgrade_newowner = temp + 1;
                }
                appendPQExpBuffer(pgdumpopts, " --binary-upgrade-usermap ");
                doShellQuoting(pgdumpopts, optarg);
                break;
            }

            case 10:
                appendPQExpBuffer(pgdumpopts, " --include-extensions ");
                break;

            case 11: /* dump template database also */
                dump_templatedb = true;
                break;

            case 12:
                if (parallel_jobs != NULL)
                    free(parallel_jobs);
                parallel_jobs = gs_strdup(optarg);
                break;

            case 13:
                is_pipeline = true;
                break;

            default:
                write_stderr(_("Try \"%s --help\" for more information.\n"), progname);
                exit_nicely(1);
        }
    }

    /* Complain if any arguments remain */
    if (optind < argc) {
        write_stderr(_("%s: too many command-line arguments (first is \"%s\")\n"), progname, argv[optind]);
        write_stderr(_("Try \"%s --help\" for more information.\n"), progname);
        exit_nicely(1);
    }
}

static void validate_dumpall_options(char** argv)
{
    // log output redirect
    init_log((char*)PROG_NAME);

    /* Make sure the user hasn't specified a mix of globals-only options */
    if (globals_only && roles_only) {
        write_stderr(_("%s: options -g/--globals-only and -r/--roles-only cannot be used together\n"), progname);
        write_stderr(_("Try \"%s --help\" for more information.\n"), progname);
        exit_nicely(1);
    }

    if (globals_only && tablespaces_only) {
        write_stderr(_("%s: options -g/--globals-only and -t/--tablespaces-only cannot be used together\n"), progname);
        write_stderr(_("Try \"%s --help\" for more information.\n"), progname);
        exit_nicely(1);
    }

    if (roles_only && tablespaces_only) {
        write_stderr(_("%s: options -r/--roles-only and -t/--tablespaces-only cannot be used together\n"), progname);
        write_stderr(_("Try \"%s --help\" for more information.\n"), progname);
        exit_nicely(1);
    }

    /* Make sure the user hasn't specified a mix of data-only,schema-only,golbals-only options */
    if (data_only && schema_only) {
        write_stderr(_("%s: options -s/--schema-only and -a/--data-only cannot be used together\n"), progname);

        write_stderr(_("Try \"%s --help\" for more information.\n"), progname);
        exit_nicely(1);
    }

    if (data_only && roles_only) {
        write_stderr(_("%s: options -r/--roles-only and -a/--data-only cannot be used together\n"), progname);
        write_stderr(_("Try \"%s --help\" for more information.\n"), progname);
        exit_nicely(1);
    }

    if (data_only && tablespaces_only) {
        write_stderr(_("%s: options -t/--tablespaces-only and -a/--data-only cannot be used together\n"), progname);
        write_stderr(_("Try \"%s --help\" for more information.\n"), progname);
        exit_nicely(1);
    }

    if (data_only && globals_only) {
        write_stderr(_("%s: options -g/--globals-only and -a/--data-only cannot be used together\n"), progname);
        write_stderr(_("Try \"%s --help\" for more information.\n"), progname);
        exit_nicely(1);
    }

    if ((tablespaces_postfix_path != NULL) && !binary_upgrade) {
        write_stderr(_("%s: options tablespaces-postfix should be used with --binary-upgrade option\n"), progname);
        write_stderr(_("Try \"%s --help\" for more information.\n"), progname);
        exit_nicely(1);
    }

    if ((binary_upgrade_oldowner != NULL || binary_upgrade_newowner != NULL) && !binary_upgrade) {
        write_stderr(_("%s: options --binary-upgrade-usermap should be used with --binary-upgrade option\n"), progname);
        write_stderr(_("Try \"%s --help\" for more information.\n"), progname);
        exit_nicely(1);
    }

    if (parallel_jobs != NULL) {
        int i = 0;
        int len = (int)strlen(parallel_jobs);
        for (i = 0; i < len; i++) {
            if (!isdigit(parallel_jobs[i])) {
                write_stderr(_("%s: options --parallel-jobs should be set between 1 and 1000\n"), progname);
                write_stderr(_("Try \"%s --help\" for more information.\n"), progname);
                exit_nicely(1);
            }
        }

        if ((atoi(parallel_jobs) < 1 || atoi(parallel_jobs) > 1000)) {
            write_stderr(_("%s: options --parallel-jobs should be set between 1 and 1000\n"), progname);
            write_stderr(_("Try \"%s --help\" for more information.\n"), progname);
            exit_nicely(1);
        }

        /* parallel dump databases need a output filename */
        if (filename == NULL) {
            write_stderr(_("%s: options --parallel-jobs should be used with -f/--file option\n"), progname);
            write_stderr(_("Try \"%s --help\" for more information.\n"), progname);
            exit_nicely(1);
        }
    }

    if ((encrypt_mode != NULL) && (encrypt_key == NULL)) {
        get_encrypt_key();
    }
    /* validate encryption mode and key */
    check_encrypt_parameters_dumpall(encrypt_mode, encrypt_key);

    /* Add long options to the pg_dump argument list */
    if (binary_upgrade)
        appendPQExpBuffer(pgdumpopts, " --binary-upgrade");
    if (column_inserts)
        appendPQExpBuffer(pgdumpopts, " --column-inserts");
    if (disable_dollar_quoting)
        appendPQExpBuffer(pgdumpopts, " --disable-dollar-quoting");
    if (disable_triggers)
        appendPQExpBuffer(pgdumpopts, " --disable-triggers");
    if (inserts)
        appendPQExpBuffer(pgdumpopts, " --inserts");
    if (no_tablespaces)
        appendPQExpBuffer(pgdumpopts, " --no-tablespaces");
    if (quote_all_identifiers)
        appendPQExpBuffer(pgdumpopts, " --quote-all-identifiers");
    if (use_setsessauth)
        appendPQExpBuffer(pgdumpopts, " --use-set-session-authorization");
    if (no_security_labels)
        appendPQExpBuffer(pgdumpopts, " --no-security-labels");
    if (no_unlogged_table_data)
        appendPQExpBuffer(pgdumpopts, " --no-unlogged-table-data");
    if (no_subscriptions)
        appendPQExpBuffer(pgdumpopts, " --no-subscriptions");
    if (no_publications)
        appendPQExpBuffer(pgdumpopts, " --no-publications");

#ifdef ENABLE_MULTIPLE_NODES
    if (include_nodes)
        appendPQExpBuffer(pgdumpopts, " --include-nodes");
#endif
}

void help(void)
{
    printf(_("%s extracts an openGauss database cluster into an SQL script file.\n\n"), progname);
    printf(_("Usage:\n"));
    printf(_("  %s [OPTION]...\n"), progname);

    printf(_("\nGeneral options:\n"));
    printf(_("  -f, --file=FILENAME                         output file name\n"));
    printf(_("  -v, --verbose                               verbose mode\n"));
    printf(_("  -V, --version                               output version information, then exit\n"));
    printf(_("  --lock-wait-timeout=TIMEOUT                 fail after waiting TIMEOUT for a table lock\n"));
    printf(_("  -?, --help                                  show this help, then exit\n"));
    printf(_("\nOptions controlling the output content:\n"));
    printf(_("  -a, --data-only                             dump only the data, not the schema\n"));
    printf(_("  -c, --clean                                 clean (drop) databases before recreating\n"));
    printf(_("  -g, --globals-only                          dump only global objects, no databases\n"));
    printf(_("  -o, --oids                                  include OIDs in dump\n"));
    printf(_("  -O, --no-owner                              skip restoration of object ownership\n"));
    printf(_("  -r, --roles-only                            dump only roles, no databases or tablespaces\n"));
    printf(_("  -s, --schema-only                           dump only the schema, no data\n"));
    printf(_("  -S, --sysadmin=NAME                         system admin user name to use in the dump\n"));
    printf(_("  -t, --tablespaces-only                      dump only tablespaces, no databases or roles\n"));
    printf(_("  -x, --no-privileges                         do not dump privileges (grant/revoke)\n"));
    printf(_("  --column-inserts/--attribute-inserts        dump data as INSERT commands with column names\n"));
    printf(_("  --disable-dollar-quoting                    disable dollar quoting, use SQL standard quoting\n"));
    printf(_("  --disable-triggers                          disable triggers during data-only restore\n"));
    printf(_("  --inserts                                   dump data as INSERT commands, rather than COPY\n"));
#if !defined(ENABLE_MULTIPLE_NODES)
    printf(_("  --no-publications                           do not dump publications\n"));
#endif
    printf(_("  --no-security-labels                        do not dump security label assignments\n"));
    printf(_("  --no-tablespaces                            do not dump tablespace assignments\n"));
#if !defined(ENABLE_MULTIPLE_NODES)
    printf(_("  --no-subscriptions                          do not dump subscriptions\n"));
#endif
    printf(_("  --no-unlogged-table-data                    do not dump unlogged table data\n"));
    printf(_("  --include-alter-table                       dump the table delete column\n"));
    printf(_("  --quote-all-identifiers                     quote all identifiers, even if not key words\n"));
    printf(_("  --dont-overwrite-file                       do not overwrite the existing file\n"));
    printf(_("  --use-set-session-authorization             use SET SESSION AUTHORIZATION commands instead of\n"
             "                                              ALTER OWNER commands to set ownership\n"));
    printf(_("  --with-encryption=AES128                    dump data is encrypted using AES128\n"));
    printf(_("  --with-key=KEY                              AES128 encryption key ,must be 16 bytes in length\n"));
    printf(_("  --include-extensions                        include extensions in dumpall \n"));
    printf(_("  --include-templatedb                        include dumping of template database also \n"));
    printf(_("  --pipeline                                  use pipeline to pass the password,\n"
             "                                              forbidden to use in terminal\n"));
#ifdef ENABLE_MULTIPLE_NODES
    printf(_("  --dump-nodes                                include nodes and node groups in the dump\n"));
    printf(_(
        "  --include-nodes                             include TO NODE clause in the dumped CREATE TABLE commands\n"));
    printf(_("  --include-buckets                           include BUCKETS clause in the dumped CREATE NODE GROUP "
             "commands\n"));
    printf(_("  --dump-wrm                                  include workload resource manager settings in the dump\n"));
#endif

    printf(_("  --binary-upgrade                            for use by upgrade utilities only\n"));
    printf(_(
        "  --binary-upgrade-usermap=\"USER1=USER2\"      to be used only by upgrade utility for mapping usernames\n"));
    printf(_("  --non-lock-table                            for use by OM tools utilities only\n"));
    printf(_("  --tablespaces-postfix                       to be used only by upgrade utility for adding the postfix "
             "name specified for all the tablespaces\n"));
    printf(_("  --parallel-jobs                             number of parallel jobs to dump databases\n"));

    printf(_("\nConnection options:\n"));
    printf(_("  -h, --host=HOSTNAME                         database server host or socket directory\n"));
    printf(_("  -l, --database=DBNAME                       alternative default database\n"));
    printf(_("  -p, --port=PORT                             database server port number\n"));
    printf(_("  -U, --username=NAME                         connect as specified database user\n"));
    printf(_("  -w, --no-password                           never prompt for password\n"));
    printf(_("  -W, --password=PASSWORD                     the password of specified database user\n"));
    printf(_("  --role=ROLENAME                             do SET ROLE before dump\n"));
    printf(_("  --rolepassword=ROLEPASSWORD                 the password for role\n"));
}

/*
 * Drop roles
 */
static void dropRoles(PGconn* conn)
{
    PGresult* res = NULL;
    int i_rolname = 0;
    int i = 0;

    if (server_version >= 80100)
        res = executeQuery(conn,
            "SELECT rolname "
            "FROM pg_authid "
            "ORDER BY 1");
    else
        res = executeQuery(conn,
            "SELECT usename as rolname "
            "FROM pg_shadow "
            "UNION "
            "SELECT groname as rolname "
            "FROM pg_group "
            "ORDER BY 1");

    i_rolname = PQfnumber(res, "rolname");

    if (PQntuples(res) > 0) {
        dumpall_printf(OPF, "--\n-- Drop roles\n--\n\n");
    }

    for (i = 0; i < PQntuples(res); i++) {
        const char* rolename = NULL;

        rolename = PQgetvalue(res, i, i_rolname);
        if (rolename == NULL) {
            continue;
        }

        dumpall_printf(OPF, "DROP ROLE IF EXISTS %s;\n", fmtId(rolename));
    }

    PQclear(res);

    dumpall_printf(OPF, "\n\n");
}

/*
 * Dump alter role for node group
 */
static void dumpAlterRolesForNodeGroup(PGconn* conn)
{
    PQExpBuffer query = NULL;
    PGresult* res = NULL;
    int i = 0;
    int ntups = 0;
    int i_rolname = 0;
    int i_rolkind = 0;
    int i_rolnodegroup = 0;
    bool is_rolnodegroup_exists = false;

    is_rolnodegroup_exists = is_column_exists(conn, AuthIdRelationId, "rolnodegroup");
    if (!is_rolnodegroup_exists)
        return;

    query = createPQExpBuffer();
    printfPQExpBuffer(query,
        "SELECT rolname, rolkind, (SELECT group_name FROM pg_catalog.pgxc_group WHERE oid = rolnodegroup) AS "
        "nodegroupname "
        "FROM pg_catalog.pg_authid "
        "ORDER BY oid");
    res = executeQuery(conn, query->data);
    i_rolname = PQfnumber(res, "rolname");
    i_rolkind = PQfnumber(res, "rolkind");
    i_rolnodegroup = PQfnumber(res, "nodegroupname");
    ntups = PQntuples(res);
    for (i = 0; i < ntups; i++) {
        const char* rolename = NULL;
        const char* nodegroupname = NULL;
        const char* rolkind = NULL;
        size_t len = 0;

        rolename = PQgetvalue(res, i, i_rolname);
        nodegroupname = PQgetvalue(res, i, i_rolnodegroup);
        if (rolename == NULL || nodegroupname == NULL) {
            continue;
        }

        if (!PQgetisnull(res, i, i_rolnodegroup) && strcmp(nodegroupname, "") != 0) {
            resetPQExpBuffer(query);
            appendPQExpBuffer(query, "ALTER ROLE %s WITH", fmtId(rolename));
            appendPQExpBuffer(query, " NODE GROUP %s", fmtId(nodegroupname));

            rolkind = PQgetvalue(res, i, i_rolkind);
            if (rolkind == NULL) {
                continue;
            }

            len = strlen(rolkind);
            if (!PQgetisnull(res, i, i_rolkind) && (int(len) - 1) == 0 && strcmp(rolkind, "v") == 0) {
                appendPQExpBuffer(query, " VCADMIN LOGIN;\n");
            } else {
                appendPQExpBuffer(query, ";\n");
            }

            dumpall_printf(OPF, "%s", query->data);
        }
    }

    if (ntups > 0)
        dumpall_printf(OPF, "\n\n");

    destroyPQExpBuffer(query);
    PQclear(res);
}

/*
 * Dump alter role for resource pool
 */
static void dumpAlterRolesForResourcePool(PGconn* conn)
{
    PQExpBuffer query = NULL;
    PGresult* res = NULL;
    int i = 0;
    int ntups = 0;
    int i_rolname = 0;
    int i_rolrespool = 0;
    int i_rolparentid = 0;
    const char* rolename = NULL;
    const char* rolrespool = NULL;
    const char* rolparentname = NULL;

    query = createPQExpBuffer();
    printfPQExpBuffer(query, "SELECT rolname, rolrespool, ");
    if (true == is_column_exists(conn, AuthIdRelationId, "rolparentid")) {
        appendPQExpBuffer(query, "pg_catalog.pg_stat_get_role_name(rolparentid) AS rolparentname ");
    } else {
        appendPQExpBuffer(query, "NULL as rolparentid ");
    }
    appendPQExpBuffer(query,
        "FROM pg_catalog.pg_authid "
        "ORDER BY oid");

    res = executeQuery(conn, query->data);
    i_rolname = PQfnumber(res, "rolname");
    i_rolrespool = PQfnumber(res, "rolrespool");
    i_rolparentid = PQfnumber(res, "rolparentname");
    ntups = PQntuples(res);
    /*
     * Because the resource pool is associated with the user group, it needs to be processed separately.
     * First, process user information without user group.
     * Second, the user group must use a user who must specify a resource pool.
     */
    for (i = 0; i < ntups; i++) {
        rolename = PQgetvalue(res, i, i_rolname);
        rolrespool = PQgetvalue(res, i, i_rolrespool);
        rolparentname = PQgetvalue(res, i, i_rolparentid);
        if (!PQgetisnull(res, i, i_rolparentid) && strcmp(rolparentname, "") != 0) {
            continue;
        }

        if (!PQgetisnull(res, i, i_rolrespool)) {
            resetPQExpBuffer(query);
            appendPQExpBuffer(query, "ALTER ROLE %s WITH", fmtId(rolename));
            appendPQExpBuffer(query, " RESOURCE POOL \"%s\";\n", rolrespool);
            dumpall_printf(OPF, "%s", query->data);
        }
    }

    for (i = 0; i < ntups; i++) {
        rolename = PQgetvalue(res, i, i_rolname);
        rolrespool = PQgetvalue(res, i, i_rolrespool);
        rolparentname = PQgetvalue(res, i, i_rolparentid);
        if (!PQgetisnull(res, i, i_rolparentid) && strcmp(rolparentname, "") != 0 &&
            !PQgetisnull(res, i, i_rolrespool)) {
            resetPQExpBuffer(query);
            appendPQExpBuffer(query, "ALTER ROLE %s WITH", fmtId(rolename));
            appendPQExpBuffer(query, " RESOURCE POOL \"%s\" ", rolrespool);
            appendPQExpBuffer(query, " USER GROUP '%s';\n", rolparentname);
            dumpall_printf(OPF, "%s", query->data);
        }
    }

    if (ntups > 0)
        dumpall_printf(OPF, "\n\n");

    destroyPQExpBuffer(query);
    PQclear(res);
}
/*
 * Dump roles
 */
static void dumpRoles(PGconn* conn)
{
    PQExpBuffer buf = createPQExpBuffer();
    PGresult* res = NULL;
    int i_oid = 0;
    int i_rolname = 0;
    int i_rolinherit = 0;
    int i_rolcreaterole = 0;
    int i_rolcreatedb = 0;
    int i_rolsysadmin = 0;
    int i_rolcanlogin = 0;
    int i_rolconnlimit = 0;
    int i_rolpassword = 0;
    int i_rolvalidbegin = 0;
    int i_rolvaliduntil = 0;
    int i_rolrespool = 0;
    int i_rolparentid = 0;
    int i_roltabspace = 0;
    int i_rolreplication = 0;
    int i_rolauditadmin = 0; /* add audit admin privilege */
    int i_rolcomment = 0;
    int i_rolkind = 0;
    int i_rolnodegroup = 0;
    int i_roluseft = 0; /* add foreign table operation privilege */
    int i_rolpwdexp = 0;
    int i = 0;
    bool has_parent = false;
    /* add monadmin, opradmin and poladmin privileges */
    int i_rolmonitoradmin = 0;
    int i_roloperatoradmin = 0;
    int i_rolpolicyadmin = 0;

    /* note: rolconfig is dumped later */
    if (server_version >= 90100) {
        printfPQExpBuffer(buf,
            "SELECT A.oid, rolname, rolinherit, "
            "rolcreaterole, rolcreatedb, rolsystemadmin, "
            "rolcanlogin, rolconnlimit, rolpassword, "
            "rolvaliduntil, rolreplication, rolauditadmin, "); /* add audit admin privilege */

        if (true == is_column_exists(conn, AuthIdRelationId, "rolvalidbegin")) {
            appendPQExpBuffer(buf, "rolvalidbegin, ");
        } else {
            appendPQExpBuffer(buf, "NULL as rolvalidbegin, ");
        }

        if (true == is_column_exists(conn, AuthIdRelationId, "rolrespool")) {
            appendPQExpBuffer(buf, "rolrespool, ");
        } else {
            appendPQExpBuffer(buf, "NULL as rolrespool, ");
        }

        if (true == is_column_exists(conn, AuthIdRelationId, "roluseft")) {
            appendPQExpBuffer(buf, "roluseft,");
        } else {
            appendPQExpBuffer(buf, "NULL as roluseft, ");
        }
        if (true == is_column_exists(conn, AuthIdRelationId, "rolparentid")) {
            appendPQExpBuffer(buf, "pg_catalog.pg_stat_get_role_name(rolparentid) AS rolparentname, ");
            has_parent = true;
        } else {
            appendPQExpBuffer(buf, "NULL as rolparentid, ");
        }

        if (true == is_column_exists(conn, AuthIdRelationId, "roltabspace")) {
            appendPQExpBuffer(buf, "roltabspace, ");
        } else {
            appendPQExpBuffer(buf, "NULL as roltabspace, ");
        }

        if (true == is_column_exists(conn, AuthIdRelationId, "rolkind")) {
            appendPQExpBuffer(buf, "rolkind, ");
        } else {
            appendPQExpBuffer(buf, "NULL as rolkind, ");
        }

        if (true == is_column_exists(conn, AuthIdRelationId, "rolnodegroup")) {
            appendPQExpBuffer(
                buf, "(SELECT group_name FROM pg_catalog.pgxc_group WHERE oid = rolnodegroup) AS nodegroupname, ");
        } else {
            appendPQExpBuffer(buf, "NULL as nodegroupname, ");
        }

        /* add monadmin, opradmin and poladmin privileges */
        if (is_column_exists(conn, AuthIdRelationId, "rolmonitoradmin") == true) {
            appendPQExpBuffer(buf, "rolmonitoradmin, ");
        } else {
            appendPQExpBuffer(buf, "NULL as rolmonitoradmin, ");
        }
        if (is_column_exists(conn, AuthIdRelationId, "roloperatoradmin") == true) {
            appendPQExpBuffer(buf, "roloperatoradmin, ");
        } else {
            appendPQExpBuffer(buf, "NULL as roloperatoradmin, ");
        }
        if (is_column_exists(conn, AuthIdRelationId, "rolpolicyadmin") == true) {
            appendPQExpBuffer(buf, "rolpolicyadmin, ");
        } else {
            appendPQExpBuffer(buf, "NULL as rolpolicyadmin, ");
        }
        if (is_column_exists(conn, UserStatusRelationId, "passwordexpired") == true) {
            appendPQExpBuffer(buf, "passwordexpired, ");
        } else {
            appendPQExpBuffer(buf, "NULL as passwordexpired, ");
        }

        appendPQExpBuffer(buf,
            "pg_catalog.shobj_description(oid, 'pg_authid') as rolcomment "
            "FROM pg_authid A left join pg_user_status on A.oid = pg_user_status.roloid "
            "ORDER BY ");

        if (has_parent)
            appendPQExpBuffer(buf, "rolparentid,");

        appendPQExpBuffer(buf, "2");
    } else if (server_version >= 80200)
        printfPQExpBuffer(buf,
            "SELECT oid, rolname, rolsuper, rolinherit, "
            "rolcreaterole, rolcreatedb, "
            "rolcanlogin, rolconnlimit, rolpassword, "
            "rolvaliduntil, false as rolreplication, false as rolauditadmin, " /* add audit admin privilege */
            "pg_catalog.shobj_description(oid, 'pg_authid') as rolcomment "
            "FROM pg_authid "
            "ORDER BY 2");
    else if (server_version >= 80100)
        printfPQExpBuffer(buf,
            "SELECT oid, rolname, rolsuper, rolinherit, "
            "rolcreaterole, rolcreatedb, "
            "rolcanlogin, rolconnlimit, rolpassword, "
            "rolvaliduntil, false as rolreplication, false as rolauditadmin, " /* add audit admin privilege */
            "null as rolcomment "
            "FROM pg_authid "
            "ORDER BY 2");
    else
        printfPQExpBuffer(buf,
            "SELECT 0, usename as rolname, "
            "usesuper as rolsuper, "
            "true as rolinherit, "
            "usesuper as rolcreaterole, "
            "usecreatedb as rolcreatedb, "
            "true as rolcanlogin, "
            "-1 as rolconnlimit, "
            "passwd as rolpassword, "
            "valuntil as rolvaliduntil, "
            "false as rolreplication, "
            "false as rolauditadmin, " /* add audit admin privilege */
            "null as rolcomment "
            "FROM pg_shadow "
            "UNION ALL "
            "SELECT 0, groname as rolname, "
            "false as rolsuper, "
            "true as rolinherit, "
            "false as rolcreaterole, "
            "false as rolcreatedb, "
            "false as rolcanlogin, "
            "-1 as rolconnlimit, "
            "null::text as rolpassword, "
            "null::abstime as rolvaliduntil, "
            "false as rolreplication, "
            "false as rolauditadmin, " /* add audit admin privilege */
            "null as rolcomment "
            "FROM pg_group "
            "WHERE NOT EXISTS (SELECT 1 FROM pg_shadow "
            " WHERE usename = groname) "
            "ORDER BY 2");

    res = executeQuery(conn, buf->data);

    i_oid = PQfnumber(res, "oid");
    i_rolname = PQfnumber(res, "rolname");
    i_rolinherit = PQfnumber(res, "rolinherit");
    i_rolcreaterole = PQfnumber(res, "rolcreaterole");
    i_rolcreatedb = PQfnumber(res, "rolcreatedb");
    i_rolsysadmin = PQfnumber(res, "rolsystemadmin");
    i_rolcanlogin = PQfnumber(res, "rolcanlogin");
    i_rolconnlimit = PQfnumber(res, "rolconnlimit");
    i_rolpassword = PQfnumber(res, "rolpassword");
    i_rolvalidbegin = PQfnumber(res, "rolvalidbegin");
    i_rolvaliduntil = PQfnumber(res, "rolvaliduntil");
    i_rolrespool = PQfnumber(res, "rolrespool");
    i_rolparentid = PQfnumber(res, "rolparentname");
    i_roltabspace = PQfnumber(res, "roltabspace");
    i_rolkind = PQfnumber(res, "rolkind");
    i_rolnodegroup = PQfnumber(res, "nodegroupname");
    i_rolreplication = PQfnumber(res, "rolreplication");
    i_rolauditadmin = PQfnumber(res, "rolauditadmin"); /* add audit admin privilege */
    i_rolcomment = PQfnumber(res, "rolcomment");
    i_roluseft = PQfnumber(res, "roluseft");
    /* add monadmin, opradmin and poladmin privileges */
    i_rolmonitoradmin = PQfnumber(res, "rolmonitoradmin");
    i_roloperatoradmin = PQfnumber(res, "roloperatoradmin");
    i_rolpolicyadmin = PQfnumber(res, "rolpolicyadmin");
    /* add passwordexpired */
    i_rolpwdexp = PQfnumber(res, "passwordexpired");

    if (PQntuples(res) > 0)
        dumpall_printf(OPF, "--\n-- Roles\n--\n\n");

    for (i = 0; i < PQntuples(res); i++) {
        const char* rolename = NULL;
        Oid auth_oid;
        char* tabspace = NULL;
        char* roltype = NULL;
        char* groupname = NULL;
        size_t len = 0;

        auth_oid = atooid(PQgetvalue(res, i, i_oid));
        rolename = PQgetvalue(res, i, i_rolname);
        if (strncmp("gs_role_", rolename, strlen("gs_role_")) == 0) {
            continue;
        }

        resetPQExpBuffer(buf);

        if (binary_upgrade) {
            appendPQExpBuffer(buf, "\n-- For binary upgrade, must preserve pg_authid.oid\n");
            appendPQExpBuffer(buf, "SELECT binary_upgrade.set_next_pg_authid_oid('%u'::pg_catalog.oid);\n\n", auth_oid);
        }

        /*
         * We dump CREATE ROLE followed by ALTER ROLE to ensure that the role
         * will acquire the right properties even if it already exists (ie, it
         * won't hurt for the CREATE to fail).  This is particularly important
         * for the role we are connected as, since even with --clean we will
         * have failed to drop it.
         */
        /* PASSWORD is must while creating role/user */
        appendPQExpBuffer(buf, "CREATE ROLE %s", fmtId(rolename));

        if (!PQgetisnull(res, i, i_rolpassword)) {
            appendPQExpBuffer(buf, " PASSWORD ");
            appendStringLiteralConn(buf, PQgetvalue(res, i, i_rolpassword), conn);
        } else {
            /* IAM, support DISABLE grammar */
            appendPQExpBuffer(buf, " PASSWORD DISABLE");
        }
        appendPQExpBuffer(buf, ";\n");

        appendPQExpBuffer(buf, "ALTER ROLE %s WITH", fmtId(rolename));

        /* NOSUPERUSER is not supported it is changed to NOSYSADMIN */
        if (strcmp(PQgetvalue(res, i, i_rolsysadmin), "t") == 0)
            appendPQExpBuffer(buf, " SYSADMIN");
        else
            appendPQExpBuffer(buf, " NOSYSADMIN");

        if (strcmp(PQgetvalue(res, i, i_rolinherit), "t") == 0)
            appendPQExpBuffer(buf, " INHERIT");
        else
            appendPQExpBuffer(buf, " NOINHERIT");

        if (strcmp(PQgetvalue(res, i, i_rolcreaterole), "t") == 0)
            appendPQExpBuffer(buf, " CREATEROLE");
        else
            appendPQExpBuffer(buf, " NOCREATEROLE");

        if (strcmp(PQgetvalue(res, i, i_rolcreatedb), "t") == 0)
            appendPQExpBuffer(buf, " CREATEDB");
        else
            appendPQExpBuffer(buf, " NOCREATEDB");

        if (!PQgetisnull(res, i, i_roluseft)) {
            if (strcmp(PQgetvalue(res, i, i_roluseft), "t") == 0)
                appendPQExpBuffer(buf, " USEFT");
            else
                appendPQExpBuffer(buf, " NOUSEFT");
        }

        if (strcmp(PQgetvalue(res, i, i_rolcanlogin), "t") == 0)
            appendPQExpBuffer(buf, " LOGIN");
        else
            appendPQExpBuffer(buf, " NOLOGIN");

        if (strcmp(PQgetvalue(res, i, i_rolreplication), "t") == 0)
            appendPQExpBuffer(buf, " REPLICATION");
        else
            appendPQExpBuffer(buf, " NOREPLICATION");
        /* Database Security: Support database audit */
        /*  add audit admin privilege */
        if (strcmp(PQgetvalue(res, i, i_rolauditadmin), "t") == 0)
            appendPQExpBuffer(buf, " AUDITADMIN");
        else
            appendPQExpBuffer(buf, " NOAUDITADMIN");

        /* add monadmin, opradmin and poladmin privilege */
        if (!PQgetisnull(res, i, i_rolmonitoradmin) && strcmp(PQgetvalue(res, i, i_rolmonitoradmin), "t") == 0) {
            appendPQExpBuffer(buf, " MONADMIN");
        }
        if (!PQgetisnull(res, i, i_roloperatoradmin) && strcmp(PQgetvalue(res, i, i_roloperatoradmin), "t") == 0) {
            appendPQExpBuffer(buf, " OPRADMIN");
        }
        if (!PQgetisnull(res, i, i_rolpolicyadmin) && strcmp(PQgetvalue(res, i, i_rolpolicyadmin), "t") == 0) {
            appendPQExpBuffer(buf, " POLADMIN");
        }
        if (!PQgetisnull(res, i, i_rolpwdexp) && strcmp(PQgetvalue(res, i, i_rolpwdexp), "1") == 0) {
            appendPQExpBuffer(buf, " PASSWORD EXPIRED");
        }

        if (strcmp(PQgetvalue(res, i, i_rolconnlimit), "-1") != 0)
            appendPQExpBuffer(buf, " CONNECTION LIMIT %s", PQgetvalue(res, i, i_rolconnlimit));
        if (!PQgetisnull(res, i, i_rolvalidbegin))
            appendPQExpBuffer(buf, " VALID BEGIN '%s'", PQgetvalue(res, i, i_rolvalidbegin));
        if (!PQgetisnull(res, i, i_rolvaliduntil))
            appendPQExpBuffer(buf, " VALID UNTIL '%s'", PQgetvalue(res, i, i_rolvaliduntil));

        /*
         * only -r/-g can print the whole alte SQL (not include node group and resource pool)
         */
        if ((roles_only || globals_only) && !dump_nodes && !dump_wrm) {
            /*
             * DN instance does not have nodegroup information. So this branch will be skipped.
             */
            if (!PQgetisnull(res, i, i_rolnodegroup) && 0 != strcmp(PQgetvalue(res, i, i_rolnodegroup), "")) {
                groupname = PQgetvalue(res, i, i_rolnodegroup);
                appendPQExpBuffer(buf, " NODE GROUP %s", fmtId(groupname));

                roltype = PQgetvalue(res, i, i_rolkind);
                len = strlen(roltype);
                if (!PQgetisnull(res, i, i_rolkind) && (int(len) - 1) == 0 && strcmp(roltype, "v") == 0) {
                    appendPQExpBuffer(buf, " VCADMIN");
                }
            }

            if (!PQgetisnull(res, i, i_rolrespool)) {
                appendPQExpBuffer(buf, " RESOURCE POOL \"%s\"", PQgetvalue(res, i, i_rolrespool));
            }

            if (!PQgetisnull(res, i, i_rolparentid) && strcmp(PQgetvalue(res, i, i_rolparentid), "") != 0) {
                appendPQExpBuffer(buf, " USER GROUP '%s'", PQgetvalue(res, i, i_rolparentid));
            }
        }

        /*
         * --dump-wrm, it means that it shoud dump alter role for resource pool.
         */
        if (dump_wrm && !dump_nodes && !PQgetisnull(res, i, i_rolnodegroup) &&
            0 != strcmp(PQgetvalue(res, i, i_rolnodegroup), "")) {
            groupname = PQgetvalue(res, i, i_rolnodegroup);
            appendPQExpBuffer(buf, " NODE GROUP %s", fmtId(groupname));

            roltype = PQgetvalue(res, i, i_rolkind);
            len = strlen(roltype);
            if (!PQgetisnull(res, i, i_rolkind) && (int(len) - 1) == 0 && strcmp(roltype, "v") == 0) {
                appendPQExpBuffer(buf, " VCADMIN");
            }
        }

        /*
         * --dump-nodes, it means that it shoud dump alter role for node group.
         */
        if (dump_nodes && !dump_wrm) {
            if (!PQgetisnull(res, i, i_rolrespool)) {
                appendPQExpBuffer(buf, " RESOURCE POOL \"%s\"", PQgetvalue(res, i, i_rolrespool));
            }

            if (!PQgetisnull(res, i, i_rolparentid) && strcmp(PQgetvalue(res, i, i_rolparentid), "") != 0) {
                appendPQExpBuffer(buf, " USER GROUP '%s'", PQgetvalue(res, i, i_rolparentid));
            }
        }

        tabspace = PQgetvalue(res, i, i_roltabspace);
        len = strlen(tabspace);
        /* check space limit */
        if (!PQgetisnull(res, i, i_roltabspace) && len > 0 && tabspace[len - 1] == 'K')
            appendPQExpBuffer(buf, " PERM SPACE '%s'", tabspace);

        /* INPENENDENT */
        roltype = PQgetvalue(res, i, i_rolkind);
        len = strlen(roltype);
        if (!PQgetisnull(res, i, i_rolkind) && (int(len) - 1) == 0 && strcmp(roltype, "i") == 0) {
            appendPQExpBuffer(buf, " INDEPENDENT");
        }
        /* PERSISTENCE */
        if (!PQgetisnull(res, i, i_rolkind) && (int(len) - 1) == 0 && strcmp(roltype, "p") == 0) {
            appendPQExpBuffer(buf, " PERSISTENCE");
        }

        appendPQExpBuffer(buf, ";\n");

        if (PQgetisnull(res, i, i_roluseft)) {
            appendPQExpBuffer(buf, "ALTER ROLE %s WITH USEFT", fmtId(rolename));
            appendPQExpBuffer(buf, ";\n");
        }

        if (!PQgetisnull(res, i, i_rolcomment)) {
            appendPQExpBuffer(buf, "COMMENT ON ROLE %s IS ", fmtId(rolename));
            appendStringLiteralConn(buf, PQgetvalue(res, i, i_rolcomment), conn);
            appendPQExpBuffer(buf, ";\n");
        }

        if (!no_security_labels && server_version >= 90200)
            buildShSecLabels(conn, "pg_authid", auth_oid, buf, "ROLE", rolename);

        dumpall_printf(OPF, "%s", buf->data);
    }

    /*
     * Dump configuration settings for roles after all roles have been dumped.
     * We do it this way because config settings for roles could mention the
     * names of other roles.
     */
    if (server_version >= 70300)
        for (i = 0; i < PQntuples(res); i++)
            dumpUserConfig(conn, PQgetvalue(res, i, i_rolname));

    PQclear(res);

    dumpall_printf(OPF, "\n\n");

    destroyPQExpBuffer(buf);
}

/*
 * Dump role memberships.  This code is used for 8.1 and later servers.
 *
 * Note: we expect dumpRoles already created all the roles, but there is
 * no membership yet.
 */
static void dumpRoleMembership(PGconn* conn)
{
    PGresult* res = NULL;
    int i;

    res = executeQuery(conn,
        "SELECT ur.rolname AS roleid, "
        "um.rolname AS member, "
        "a.admin_option, "
        "ug.rolname AS grantor "
        "FROM pg_auth_members a "
        "LEFT JOIN pg_authid ur on ur.oid = a.roleid "
        "LEFT JOIN pg_authid um on um.oid = a.member "
        "LEFT JOIN pg_authid ug on ug.oid = a.grantor "
        "ORDER BY 1,2,3");
    if (PQntuples(res) > 0)
        dumpall_printf(OPF, "--\n-- Role memberships\n--\n\n");

    for (i = 0; i < PQntuples(res); i++) {
        char* pchRoleid = PQgetvalue(res, i, 0);
        char* pchMember = PQgetvalue(res, i, 1);
        char* pchOption = PQgetvalue(res, i, 2);

        if ((binary_upgrade_oldowner != NULL) && (0 == strncmp(pchRoleid, binary_upgrade_oldowner, NAMEDATALEN))) {
            pchRoleid = binary_upgrade_newowner;
        }

        if ((binary_upgrade_oldowner != NULL) && (0 == strncmp(pchMember, binary_upgrade_oldowner, NAMEDATALEN))) {
            pchMember = binary_upgrade_newowner;
        }
        dumpall_printf(OPF, "GRANT %s", fmtId(pchRoleid));
        dumpall_printf(OPF, " TO %s", fmtId(pchMember));
        if (*pchOption == 't')
            dumpall_printf(OPF, " WITH ADMIN OPTION");

        /*
         * We don't track the grantor very carefully in the backend, so cope
         * with the possibility that it has been dropped.
         */
        if (!PQgetisnull(res, i, 3)) {
            char* grantor = PQgetvalue(res, i, 3);

            if ((binary_upgrade_oldowner != NULL) && (0 == strncmp(grantor, binary_upgrade_oldowner, NAMEDATALEN))) {
                grantor = binary_upgrade_newowner;
            }

            dumpall_printf(OPF, " GRANTED BY %s", fmtId(grantor));
        }
        dumpall_printf(OPF, ";\n");
    }

    PQclear(res);

    dumpall_printf(OPF, "\n\n");
}

/*
 * Dump group memberships from a pre-8.1 server.  It's annoying that we
 * can't share any useful amount of code with the post-8.1 case, but
 * the catalog representations are too different.
 *
 * Note: we expect dumpRoles already created all the roles, but there is
 * no membership yet.
 */
static void dumpGroups(PGconn* conn)
{
    PQExpBuffer buf = createPQExpBuffer();
    PGresult* res = NULL;
    int i;

    res = executeQuery(conn, "SELECT groname, grolist FROM pg_group ORDER BY 1");
    if (PQntuples(res) > 0) {
        dumpall_printf(OPF, "--\n-- Role memberships\n--\n\n");
    }

    for (i = 0; i < PQntuples(res); i++) {
        char* groname = PQgetvalue(res, i, 0);
        char* grolist = PQgetvalue(res, i, 1);
        PGresult* res2 = NULL;
        int j = 0;

        /*
         * Array representation is {1,2,3} ... convert to (1,2,3)
         */
        if (strlen(grolist) < 3)
            continue;

        grolist = gs_strdup(grolist);
        grolist[0] = '(';
        grolist[strlen(grolist) - 1] = ')';
        for (j = 0; j < (int)strlen(grolist); j++) {
            if (!(grolist[j] >= '0' && grolist[j] <= '9')
                    && grolist[j] != '('
                    && grolist[j] != ')'
                    && grolist[j] != ',') {
                write_stderr(_("grolist maybe exist sql injection: %s.\n"), grolist);
                PQfinish(conn);
                exit_nicely(1);
            }
        }
        printfPQExpBuffer(buf,
            "SELECT usename FROM pg_shadow "
            "WHERE usesysid IN %s ORDER BY 1",
            grolist);
        free(grolist);
        grolist = NULL;

        res2 = executeQuery(conn, buf->data);

        for (j = 0; j < PQntuples(res2); j++) {
            char* usename = PQgetvalue(res2, j, 0);

            /*
             * Don't try to grant a role to itself; can happen if old
             * installation has identically named user and group.
             */
            if (strcmp(groname, usename) == 0)
                continue;

            dumpall_printf(OPF, "GRANT %s", fmtId(groname));

            if ((binary_upgrade_oldowner != NULL) && (0 == strncmp(usename, binary_upgrade_oldowner, NAMEDATALEN))) {
                usename = binary_upgrade_newowner;
            }

            dumpall_printf(OPF, " TO %s;\n", fmtId(usename));
        }

        PQclear(res2);
    }

    PQclear(res);
    destroyPQExpBuffer(buf);

    dumpall_printf(OPF, "\n\n");
}

/*
 * Drop tablespaces.
 */
static void dropTablespaces(PGconn* conn)
{
    PGresult* res = NULL;
    int i;

    /*
     * Get all tablespaces except built-in ones (which we assume are named
     * pg_xxx)
     */
    res = executeQuery(conn,
        "SELECT spcname "
        "FROM pg_catalog.pg_tablespace "
        "WHERE spcname !~ '^pg_' "
        "ORDER BY 1");

    if (PQntuples(res) > 0) {
        dumpall_printf(OPF, "--\n-- Drop tablespaces\n--\n\n");
    }

    for (i = 0; i < PQntuples(res); i++) {
        char* spcname = PQgetvalue(res, i, 0);

        dumpall_printf(OPF, "DROP TABLESPACE IF EXISTS %s;\n", fmtId(spcname));
    }

    PQclear(res);

    dumpall_printf(OPF, "\n\n");
}

/*
 * Handle the tablespace option. If every option value is not double
 * quotes, option value will be handled as a lowercase string. For example, the following
 * string "filesystem=hDfs, address=10.185.178.241:25000,10.185.178.239:25000,
 * cfgpath=/opt/config, storepath=/hanfeng/Hdfs_ts505". we need add double quotes for
 * the option, otherwise storepath name is processed as lowercase, it is not storepath with expectation.
 * @_in_param spcoptions: The options to be handled.
 * @return the options using PQExpBuffer struct.
 */
static PQExpBuffer handleTblSpcOpt(char* spcoptions)
{
    PQExpBuffer tblSpcOpt = createPQExpBuffer();
    int i = 0;
    int optionLen = (int)strlen(spcoptions);
    for (; i < optionLen; i++) {
        if (spcoptions[i] == '=') {
            appendPQExpBuffer(tblSpcOpt, "='");
        } else if (spcoptions[i] == ',' && spcoptions[i + 1] == ' ') {
            appendPQExpBuffer(tblSpcOpt, "',");
        } else {
            appendPQExpBuffer(tblSpcOpt, "%c", spcoptions[i]);
        }
    }

    appendPQExpBuffer(tblSpcOpt, "'");
    return tblSpcOpt;
}

/*
 * Dump tablespaces.
 */
static void dumpTablespaces(PGconn* conn)
{
    PGresult* res = NULL;
    int i = 0;

    /*
     * Get all tablespaces except built-in ones (which we assume are named
     * pg_xxx)
     */
    if (server_version >= 90200) {
        /* pg_tablepsace system table add extra column */
        if (true == is_column_exists(conn, TableSpaceRelationId, "relative")) {
            res = executeQuery(conn,
                "SELECT oid, spcname, "
                "pg_catalog.pg_get_userbyid(spcowner) AS spcowner, "
                "pg_catalog.pg_tablespace_location(oid), spcacl, "
                "pg_catalog.array_to_string(spcoptions, ', '),"
                "pg_catalog.shobj_description(oid, 'pg_tablespace'), "
                "spcmaxsize, "
                "relative "
                "FROM pg_catalog.pg_tablespace "
                "WHERE spcname !~ '^pg_' "
                "ORDER BY 1");
        } else if (true == is_column_exists(conn, TableSpaceRelationId, "spcmaxsize")) {
            res = executeQuery(conn,
                "SELECT oid, spcname, "
                "pg_catalog.pg_get_userbyid(spcowner) AS spcowner, "
                "pg_catalog.pg_tablespace_location(oid), spcacl, "
                "pg_catalog.array_to_string(spcoptions, ', '),"
                "pg_catalog.shobj_description(oid, 'pg_tablespace'), "
                "spcmaxsize "
                "FROM pg_catalog.pg_tablespace "
                "WHERE spcname !~ '^pg_' "
                "ORDER BY 1");
        } else {
            res = executeQuery(conn,
                "SELECT oid, spcname, "
                "pg_catalog.pg_get_userbyid(spcowner) AS spcowner, "
                "pg_catalog.pg_tablespace_location(oid), spcacl, "
                "pg_catalog.array_to_string(spcoptions, ', '),"
                "pg_catalog.shobj_description(oid, 'pg_tablespace'), "
                "'' AS spcmaxsize "
                "FROM pg_catalog.pg_tablespace "
                "WHERE spcname !~ '^pg_' "
                "ORDER BY 1");
        }
    } else if (server_version >= 90000)
        res = executeQuery(conn,
            "SELECT oid, spcname, "
            "pg_catalog.pg_get_userbyid(spcowner) AS spcowner, "
            "spclocation, spcacl, "
            "pg_catalog.array_to_string(spcoptions, ', '),"
            "pg_catalog.shobj_description(oid, 'pg_tablespace') "
            "FROM pg_catalog.pg_tablespace "
            "WHERE spcname !~ '^pg_' "
            "ORDER BY 1");
    else if (server_version >= 80200)
        res = executeQuery(conn,
            "SELECT oid, spcname, "
            "pg_catalog.pg_get_userbyid(spcowner) AS spcowner, "
            "spclocation, spcacl, null, "
            "pg_catalog.shobj_description(oid, 'pg_tablespace') "
            "FROM pg_catalog.pg_tablespace "
            "WHERE spcname !~ '^pg_' "
            "ORDER BY 1");
    else
        res = executeQuery(conn,
            "SELECT oid, spcname, "
            "pg_catalog.pg_get_userbyid(spcowner) AS spcowner, "
            "spclocation, spcacl, "
            "null, null "
            "FROM pg_catalog.pg_tablespace "
            "WHERE spcname !~ '^pg_' "
            "ORDER BY 1");

    if (PQntuples(res) > 0) {
        dumpall_printf(OPF, "--\n-- Tablespaces\n--\n\n");
        dumpall_printf(OPF, "SET enable_absolute_tablespace = on;\n");
    }

    for (i = 0; i < PQntuples(res); i++) {
        PQExpBuffer buf = createPQExpBuffer();
        uint32 spcoid = atooid(PQgetvalue(res, i, 0));
        char* spcname = PQgetvalue(res, i, 1);
        char* spcowner = PQgetvalue(res, i, 2);
        char* spclocation = PQgetvalue(res, i, 3);
        char* spcacl = PQgetvalue(res, i, 4);
        char* spcoptions = PQgetvalue(res, i, 5);
        char* spccomment = PQgetvalue(res, i, 6);
        char* spcmaxsize = PQgetvalue(res, i, 7);

        /* if this column is not exist, relative will be NULL */
        char* relative = PQgetvalue(res, i, 8);
        char* fspcname = NULL;
        char temp_spcloc[MAXPGPATH + 1] = {0};
        int nRet = 0;

        PQExpBuffer tempBuf = createPQExpBuffer();
        bool IsHDFSTblSpc = false;

        appendPQExpBuffer(tempBuf, "%s", spcoptions);
        tempBuf->data = pg_strtolower(tempBuf->data);

        if (NULL != strstr(tempBuf->data, "filesystem=hdfs")) {
            IsHDFSTblSpc = true;
        }
        destroyPQExpBuffer(tempBuf);

        /* needed for buildACLCommands() */
        fspcname = gs_strdup(fmtId(spcname));

        appendPQExpBuffer(buf, "CREATE TABLESPACE %s", fspcname);

        if ((binary_upgrade_oldowner != NULL) && 0 == strncmp(spcowner, binary_upgrade_oldowner, NAMEDATALEN)) {
            appendPQExpBuffer(buf, " OWNER %s", fmtId(binary_upgrade_newowner));
        } else {
            appendPQExpBuffer(buf, " OWNER %s", fmtId(spcowner));
        }

        /* relative always 't' or 'f' if it is not null */
        if ((relative != NULL) && *relative == 't')
            appendPQExpBuffer(buf, " RELATIVE ");

        appendPQExpBuffer(buf, " LOCATION ");
        if ((tablespaces_postfix_path != NULL) &&
            ((strlen(tablespaces_postfix_path) + strlen(spclocation)) <= MAXPGPATH)) {
            nRet = snprintf_s(temp_spcloc,
                sizeof(temp_spcloc),
                sizeof(temp_spcloc) - 1,
                "%s%s",
                spclocation,
                tablespaces_postfix_path);
            securec_check_ss_c(nRet, "\0", "\0");
            spclocation = temp_spcloc;
        }
        appendStringLiteralConn(buf, spclocation, conn);

        if (NULL != spcmaxsize && spcmaxsize[0] != '\0')
            appendPQExpBuffer(buf, " MAXSIZE \'%s\'", spcmaxsize);

        /*
         * It is unsupported to alter HDFS tablespace, so do not dump alter tablespace command.
         * The option of HDFS tablesapce must exist in create tablespace command.
         */
        if (IsHDFSTblSpc) {
            PQExpBuffer tblSpcOpt = handleTblSpcOpt(spcoptions);
            appendPQExpBuffer(buf, " WITH(%s)", tblSpcOpt->data);
            destroyPQExpBuffer(tblSpcOpt);
        }

        appendPQExpBuffer(buf, ";\n");

        if (!IsHDFSTblSpc && spcoptions != NULL && spcoptions[0] != '\0')
            appendPQExpBuffer(buf, "ALTER TABLESPACE %s SET (%s);\n", fspcname, spcoptions);

        if (!skip_acls &&
            !buildACLCommands(fspcname, NULL, "TABLESPACE", spcacl, spcowner, "", server_version, buf, conn)) {
            write_stderr(_("%s: could not parse ACL list (%s) for tablespace \"%s\"\n"), progname, spcacl, fspcname);
            PQfinish(conn);
            exit_nicely(1);
        }

        if ((spccomment != NULL) && strlen(spccomment)) {
            appendPQExpBuffer(buf, "COMMENT ON TABLESPACE %s IS ", fspcname);
            appendStringLiteralConn(buf, spccomment, conn);
            appendPQExpBuffer(buf, ";\n");
        }

        if (!no_security_labels && server_version >= 90200)
            buildShSecLabels(conn, "pg_tablespace", spcoid, buf, "TABLESPACE", fspcname);

        dumpall_printf(OPF, "%s", buf->data);

        free(fspcname);
        fspcname = NULL;
        destroyPQExpBuffer(buf);
    }

    PQclear(res);
    dumpall_printf(OPF, "\n\n");
}

/*
 * Dump commands to drop each database.
 *
 * This should match the set of databases targeted by dumpCreateDB().
 */
static void dropDBs(PGconn* conn)
{
    PGresult* res = NULL;
    int i = 0;

    if (server_version >= 70100)
        res = executeQuery(conn,
            "SELECT datname "
            "FROM pg_database d "
            "WHERE datallowconn ORDER BY 1");
    else
        res = executeQuery(conn,
            "SELECT datname "
            "FROM pg_database d "
            "ORDER BY 1");

    if (PQntuples(res) > 0) {
        dumpall_printf(OPF, "--\n-- Drop databases\n--\n\n");
    }

    for (i = 0; i < PQntuples(res); i++) {
        char* dbname = PQgetvalue(res, i, 0);

        /*
         * Skip "template1" and "postgres"; the restore script is almost
         * certainly going to be run in one or the other, and we don't know
         * which.  This must agree with dumpCreateDB's choices!
         */
        if (strcmp(dbname, "template1") != 0 && strcmp(dbname, "postgres") != 0) {
            dumpall_printf(OPF, "DROP DATABASE IF EXISTS %s;\n", fmtId(dbname));
        }
    }

    PQclear(res);

    dumpall_printf(OPF, "\n\n");
}

/*
 * Dump commands to create each database.
 *
 * To minimize the number of reconnections (and possibly ensuing
 * password prompts) required by the output script, we emit all CREATE
 * DATABASE commands during the initial phase of the script, and then
 * run pg_dump for each database to dump the contents of that
 * database.  We skip databases marked not datallowconn, since we'd be
 * unable to connect to them anyway (and besides, we don't want to
 * dump template0).
 */
static void dumpCreateDB(PGconn* conn)
{
    PQExpBuffer buf = createPQExpBuffer();
    char* default_encoding = NULL;
    char* default_collate = NULL;
    char* default_ctype = NULL;
    PGresult* res = NULL;
    int i = 0;

    dumpall_printf(OPF, "--\n-- Database creation\n--\n\n");

    /*
     * First, get the installation's default encoding and locale information.
     * We will dump encoding and locale specifications in the CREATE DATABASE
     * commands for just those databases with values different from defaults.
     *
     * We consider template0's encoding and locale (or, pre-7.1, template1's)
     * to define the installation default.	Pre-8.4 installations do not have
     * per-database locale settings; for them, every database must necessarily
     * be using the installation default, so there's no need to do anything
     * (which is good, since in very old versions there is no good way to find
     * out what the installation locale is anyway...)
     */
    if (server_version >= 80400)
        res = executeQuery(conn,
            "SELECT pg_catalog.pg_encoding_to_char(encoding), "
            "datcollate, datctype "
            "FROM pg_database "
            "WHERE datname = 'template0'");
    else if (server_version >= 70100)
        res = executeQuery(conn,
            "SELECT pg_catalog.pg_encoding_to_char(encoding), "
            "null::text AS datcollate, null::text AS datctype "
            "FROM pg_database "
            "WHERE datname = 'template0'");
    else
        res = executeQuery(conn,
            "SELECT pg_catalog.pg_encoding_to_char(encoding), "
            "null::text AS datcollate, null::text AS datctype "
            "FROM pg_database "
            "WHERE datname = 'template1'");

    /* If for some reason the template DB isn't there, treat as unknown */
    if (PQntuples(res) > 0) {
        if (!PQgetisnull(res, 0, 0))
            default_encoding = gs_strdup(PQgetvalue(res, 0, 0));
        if (!PQgetisnull(res, 0, 1))
            default_collate = gs_strdup(PQgetvalue(res, 0, 1));
        if (!PQgetisnull(res, 0, 2))
            default_ctype = gs_strdup(PQgetvalue(res, 0, 2));
    }

    PQclear(res);

    bool isHasDatcompatibility = true;
    bool isHasDatfrozenxid64 = false;
    isHasDatfrozenxid64 = is_column_exists(conn, DatabaseRelationId, "datfrozenxid64");
    /* Now collect all the information about databases to dump */
    if (server_version >= 80400) {
        /* Add for upgrade */
        res = executeQuery(conn,
            "select 1 from pg_attribute where attrelid = (select oid from pg_class where relname = 'pg_database') and "
            "attname = 'datcompatibility';");
        isHasDatcompatibility = (PQntuples(res) == 1) ? true : false;
        PQclear(res);

        if (isHasDatcompatibility) {
            if (isHasDatfrozenxid64)
                res = executeQuery(conn,
                    "SELECT datname, "
                    "coalesce(rolname, (select rolname from pg_authid where oid=(select datdba from pg_database where "
                    "datname='template0'))), "
                    "pg_catalog.pg_encoding_to_char(d.encoding), "
                    "datcollate, datctype, datfrozenxid, datfrozenxid64, "
                    "datistemplate, datacl, datconnlimit, "
                    "(SELECT spcname FROM pg_tablespace t WHERE t.oid = d.dattablespace) AS dattablespace, "
                    "datcompatibility "
                    "FROM pg_database d LEFT JOIN pg_authid u ON (datdba = u.oid) "
                    "WHERE datallowconn ORDER BY 1");
            else
                res = executeQuery(conn,
                    "SELECT datname, "
                    "coalesce(rolname, (select rolname from pg_authid where oid=(select datdba from pg_database where "
                    "datname='template0'))), "
                    "pg_catalog.pg_encoding_to_char(d.encoding), "
                    "datcollate, datctype, datfrozenxid, 0 AS datfrozenxid64, "
                    "datistemplate, datacl, datconnlimit, "
                    "(SELECT spcname FROM pg_tablespace t WHERE t.oid = d.dattablespace) AS dattablespace, "
                    "datcompatibility "
                    "FROM pg_database d LEFT JOIN pg_authid u ON (datdba = u.oid) "
                    "WHERE datallowconn ORDER BY 1");
        } else {
            if (isHasDatfrozenxid64)
                res = executeQuery(conn,
                    "SELECT datname, "
                    "coalesce(rolname, (select rolname from pg_authid where oid=(select datdba from pg_database where "
                    "datname='template0'))), "
                    "pg_catalog.pg_encoding_to_char(d.encoding), "
                    "datcollate, datctype, datfrozenxid, datfrozenxid64, "
                    "datistemplate, datacl, datconnlimit, "
                    "(SELECT spcname FROM pg_tablespace t WHERE t.oid = d.dattablespace) AS dattablespace "
                    "FROM pg_database d LEFT JOIN pg_authid u ON (datdba = u.oid) "
                    "WHERE datallowconn ORDER BY 1");
            else
                res = executeQuery(conn,
                    "SELECT datname, "
                    "coalesce(rolname, (select rolname from pg_authid where oid=(select datdba from pg_database where "
                    "datname='template0'))), "
                    "pg_catalog.pg_encoding_to_char(d.encoding), "
                    "datcollate, datctype, datfrozenxid, 0 AS datfrozenxid64, "
                    "datistemplate, datacl, datconnlimit, "
                    "(SELECT spcname FROM pg_tablespace t WHERE t.oid = d.dattablespace) AS dattablespace "
                    "FROM pg_database d LEFT JOIN pg_authid u ON (datdba = u.oid) "
                    "WHERE datallowconn ORDER BY 1");
        }
    } else if (server_version >= 80100)
        res = executeQuery(conn,
            "SELECT datname, "
            "coalesce(rolname, (select rolname from pg_authid where oid=(select datdba from pg_database where "
            "datname='template0'))), "
            "pg_catalog.pg_encoding_to_char(d.encoding), "
            "null::text AS datcollate, null::text AS datctype, datfrozenxid, "
            "datistemplate, datacl, datconnlimit, "
            "(SELECT spcname FROM pg_tablespace t WHERE t.oid = d.dattablespace) AS dattablespace "
            "FROM pg_database d LEFT JOIN pg_authid u ON (datdba = u.oid) "
            "WHERE datallowconn ORDER BY 1");
    else if (server_version >= 80000)
        res = executeQuery(conn,
            "SELECT datname, "
            "coalesce(usename, (select usename from pg_shadow where usesysid=(select datdba from pg_database where "
            "datname='template0'))), "
            "pg_catalog.pg_encoding_to_char(d.encoding), "
            "null::text AS datcollate, null::text AS datctype, datfrozenxid, "
            "datistemplate, datacl, -1 as datconnlimit, "
            "(SELECT spcname FROM pg_tablespace t WHERE t.oid = d.dattablespace) AS dattablespace "
            "FROM pg_database d LEFT JOIN pg_shadow u ON (datdba = usesysid) "
            "WHERE datallowconn ORDER BY 1");
    else if (server_version >= 70300)
        res = executeQuery(conn,
            "SELECT datname, "
            "coalesce(usename, (select usename from pg_shadow where usesysid=(select datdba from pg_database where "
            "datname='template0'))), "
            "pg_catalog.pg_encoding_to_char(d.encoding), "
            "null::text AS datcollate, null::text AS datctype, datfrozenxid, "
            "datistemplate, datacl, -1 as datconnlimit, "
            "'pg_default' AS dattablespace "
            "FROM pg_database d LEFT JOIN pg_shadow u ON (datdba = usesysid) "
            "WHERE datallowconn ORDER BY 1");
    else if (server_version >= 70100)
        res = executeQuery(conn,
            "SELECT datname, "
            "coalesce("
            "(select usename from pg_shadow where usesysid=datdba), "
            "(select usename from pg_shadow where usesysid=(select datdba from pg_database where "
            "datname='template0'))), "
            "pg_catalog.pg_encoding_to_char(d.encoding), "
            "null::text AS datcollate, null::text AS datctype, 0 AS datfrozenxid, "
            "datistemplate, '' as datacl, -1 as datconnlimit, "
            "'pg_default' AS dattablespace "
            "FROM pg_database d "
            "WHERE datallowconn ORDER BY 1");
    else {
        /*
         * Note: 7.0 fails to cope with sub-select in COALESCE, so just deal
         * with getting a NULL by not printing any OWNER clause.
         */
        res = executeQuery(conn,
            "SELECT datname, "
            "(select usename from pg_shadow where usesysid=datdba), "
            "pg_catalog.pg_encoding_to_char(d.encoding), "
            "null::text AS datcollate, null::text AS datctype, 0 AS datfrozenxid, "
            "'f' as datistemplate, "
            "'' as datacl, -1 as datconnlimit, "
            "'pg_default' AS dattablespace "
            "FROM pg_database d "
            "ORDER BY 1");
    }

    for (i = 0; i < PQntuples(res); i++) {
        char* dbname = PQgetvalue(res, i, 0);
        char* dbowner = PQgetvalue(res, i, 1);
        char* dbencoding = PQgetvalue(res, i, 2);
        char* dbcollate = PQgetvalue(res, i, 3);
        char* dbctype = PQgetvalue(res, i, 4);
        uint32 dbfrozenxid = atooid(PQgetvalue(res, i, 5));
        uint64 dbfrozenxid64 = atoxid(PQgetvalue(res, i, 6));
        char* dbistemplate = PQgetvalue(res, i, 7);
        char* dbacl = PQgetvalue(res, i, 8);
        char* dbconnlimit = PQgetvalue(res, i, 9);
        char* dbtablespace = PQgetvalue(res, i, 10);
        char* dbcompatibility = NULL;
        if (isHasDatcompatibility) {
            dbcompatibility = PQgetvalue(res, i, 11);
        }

        char* fdbname = NULL;

        fdbname = gs_strdup(fmtId(dbname));

        resetPQExpBuffer(buf);

        /*
         * Skip the CREATE DATABASE commands for "template1" and "postgres",
         * since they are presumably already there in the destination cluster.
         * We do want to emit their ACLs and config options if any, however.
         */
        if (strcmp(dbname, "template1") != 0 && strcmp(dbname, "postgres") != 0) {
            appendPQExpBuffer(buf, "CREATE DATABASE %s", fdbname);

            appendPQExpBuffer(buf, " WITH TEMPLATE = template0");

            if (strlen(dbowner) != 0) {
                if ((binary_upgrade_oldowner != NULL) &&
                    (0 == strncmp(dbowner, binary_upgrade_oldowner, NAMEDATALEN))) {
                    appendPQExpBuffer(buf, " OWNER = %s", fmtId(binary_upgrade_newowner));
                } else {
                    appendPQExpBuffer(buf, " OWNER = %s", fmtId(dbowner));
                }
            }

            if ((default_encoding != NULL) && strcmp(dbencoding, default_encoding) != 0) {
                appendPQExpBuffer(buf, " ENCODING = ");
                appendStringLiteralConn(buf, dbencoding, conn);
            }

            if ((default_collate != NULL) && strcmp(dbcollate, default_collate) != 0) {
                appendPQExpBuffer(buf, " LC_COLLATE = ");
                appendStringLiteralConn(buf, dbcollate, conn);
            }

            if ((default_ctype != NULL) && strcmp(dbctype, default_ctype) != 0) {
                appendPQExpBuffer(buf, " LC_CTYPE = ");
                appendStringLiteralConn(buf, dbctype, conn);
            }

            /*
             * Output tablespace if it isn't the default.  For default, it
             * uses the default from the template database.  If tablespace is
             * specified and tablespace creation failed earlier, (e.g. no such
             * directory), the database creation will fail too.  One solution
             * would be to use 'SET default_tablespace' like we do in pg_dump
             * for setting non-default database locations.
             */
            if (strcmp(dbtablespace, "pg_default") != 0 && !no_tablespaces)
                appendPQExpBuffer(buf, " TABLESPACE = %s", fmtId(dbtablespace));

            if (strcmp(dbconnlimit, "-1") != 0)
                appendPQExpBuffer(buf, " CONNECTION LIMIT = %s", dbconnlimit);
            if (isHasDatcompatibility) {
                appendPQExpBuffer(buf, " dbcompatibility = \'%s\'", dbcompatibility);
            }

            appendPQExpBuffer(buf, ";\n");

            if (strcmp(dbistemplate, "t") == 0) {
                appendPQExpBuffer(buf, "UPDATE pg_catalog.pg_database SET datistemplate = 't' WHERE datname = ");
                appendStringLiteralConn(buf, dbname, conn);
                appendPQExpBuffer(buf, ";\n");
            }
        }

        if (binary_upgrade) {
            appendPQExpBuffer(
                buf, "-- For binary upgrade, set datfrozenxid %s.\n", isHasDatfrozenxid64 ? ", datfrozenxid64" : " ");
            if (isHasDatfrozenxid64)
                appendPQExpBuffer(buf,
                    "UPDATE pg_catalog.pg_database "
                    "SET datfrozenxid = '%u' "
                    "SET datfrozenxid64 = '%lu' "
                    "WHERE datname = ",
                    dbfrozenxid,
                    dbfrozenxid64);
            else
                appendPQExpBuffer(buf,
                    "UPDATE pg_catalog.pg_database "
                    "SET datfrozenxid = '%u' "
                    "WHERE datname = ",
                    dbfrozenxid);

            appendStringLiteralConn(buf, dbname, conn);
            appendPQExpBuffer(buf, ";\n");
        }

        if (!skip_acls && !buildACLCommands(fdbname, NULL, "DATABASE", dbacl, dbowner, "", server_version, buf, conn)) {
            write_stderr(_("%s: could not parse ACL list (%s) for database \"%s\"\n"), progname, dbacl, fdbname);
            PQfinish(conn);
            exit_nicely(1);
        }

        dumpall_printf(OPF, "%s", buf->data);

        if (server_version >= 70300)
            dumpDatabaseConfig(conn, dbname);

        free(fdbname);
        fdbname = NULL;
    }

    PQclear(res);
    destroyPQExpBuffer(buf);
    free(default_ctype);
    default_ctype = NULL;
    free(default_collate);
    default_collate = NULL;
    free(default_encoding);
    default_encoding = NULL;

    dumpall_printf(OPF, "\n\n");
}

/*
 * Dump database-specific configuration
 */
static void dumpDatabaseConfig(PGconn* conn, const char* dbname)
{
    PQExpBuffer buf = createPQExpBuffer();
    int count = 1;

    for (;;) {
        PGresult* res = NULL;

        if (server_version >= 90000)
            printfPQExpBuffer(buf,
                "SELECT setconfig[%d] FROM pg_db_role_setting WHERE "
                "setrole = 0 AND setdatabase = (SELECT oid FROM pg_database WHERE datname = ",
                count);
        else
            printfPQExpBuffer(buf, "SELECT datconfig[%d] FROM pg_database WHERE datname = ", count);
        appendStringLiteralConn(buf, dbname, conn);

        if (server_version >= 90000)
            appendPQExpBuffer(buf, ")");

        appendPQExpBuffer(buf, ";");

        res = executeQuery(conn, buf->data);
        if (PQntuples(res) == 1 && !PQgetisnull(res, 0, 0)) {
            makeAlterConfigCommand(conn, PQgetvalue(res, 0, 0), "DATABASE", dbname, NULL, NULL);
            PQclear(res);
            count++;
        } else {
            PQclear(res);
            break;
        }
    }

    destroyPQExpBuffer(buf);
}

/*
 * Dump user-specific configuration
 */
static void dumpUserConfig(PGconn* conn, const char* username)
{
    PQExpBuffer buf = createPQExpBuffer();
    int count = 1;

    for (;;) {
        PGresult* res = NULL;

        if (server_version >= 90000)
            printfPQExpBuffer(buf,
                "SELECT setconfig[%d] FROM pg_db_role_setting WHERE "
                "setdatabase = 0 AND setrole = "
                "(SELECT oid FROM pg_authid WHERE rolname = ",
                count);
        else if (server_version >= 80100)
            printfPQExpBuffer(buf, "SELECT rolconfig[%d] FROM pg_authid WHERE rolname = ", count);
        else
            printfPQExpBuffer(buf, "SELECT useconfig[%d] FROM pg_shadow WHERE usename = ", count);
        appendStringLiteralConn(buf, username, conn);
        if (server_version >= 90000)
            appendPQExpBuffer(buf, ")");

        res = executeQuery(conn, buf->data);
        if (PQntuples(res) == 1 && !PQgetisnull(res, 0, 0)) {
            makeAlterConfigCommand(conn, PQgetvalue(res, 0, 0), "ROLE", username, NULL, NULL);
            PQclear(res);
            count++;
        } else {
            PQclear(res);
            break;
        }
    }

    destroyPQExpBuffer(buf);
}

/*
 * Dump user-and-database-specific configuration
 */
static void dumpDbRoleConfig(PGconn* conn)
{
    PQExpBuffer buf = createPQExpBuffer();
    PGresult* res = NULL;
    int i = 0;

    printfPQExpBuffer(buf,
        "SELECT rolname, datname, pg_catalog.unnest(setconfig) "
        "FROM pg_db_role_setting, pg_authid, pg_database "
        "WHERE setrole = pg_authid.oid AND setdatabase = pg_database.oid");
    res = executeQuery(conn, buf->data);
    if (PQntuples(res) > 0) {
        dumpall_printf(OPF, "--\n-- Per-Database Role Settings \n--\n\n");

        for (i = 0; i < PQntuples(res); i++) {
            makeAlterConfigCommand(
                conn, PQgetvalue(res, i, 2), "ROLE", PQgetvalue(res, i, 0), "DATABASE", PQgetvalue(res, i, 1));
        }

        dumpall_printf(OPF, "\n\n");
    }

    PQclear(res);
    destroyPQExpBuffer(buf);
}

/*
 * Helper function for dumpXXXConfig().
 */
static void makeAlterConfigCommand(
    PGconn* conn, const char* arrayitem, const char* type, const char* name, const char* type2, const char* name2)
{
    char* pos = NULL;
    char* mine = NULL;
    PQExpBuffer buf;

    mine = gs_strdup(arrayitem);
    pos = strchr(mine, '=');
    if (pos == NULL) {
        free(mine);
        mine = NULL;
        return;
    }

    buf = createPQExpBuffer();

    *pos = 0;
    appendPQExpBuffer(buf, "ALTER %s %s ", type, fmtId(name));
    if (type2 != NULL && name2 != NULL)
        appendPQExpBuffer(buf, "IN %s %s ", type2, fmtId(name2));
    appendPQExpBuffer(buf, "SET %s TO ", fmtId(mine));

    /*
     * Some GUC variable names are 'LIST' type and hence must not be quoted.
     */
    if (pg_strcasecmp(mine, "DateStyle") == 0 || pg_strcasecmp(mine, "search_path") == 0)
        appendPQExpBuffer(buf, "%s", pos + 1);
    else
        appendStringLiteralConn(buf, pos + 1, conn);
    appendPQExpBuffer(buf, ";\n");

    dumpall_printf(OPF, "%s", buf->data);
    destroyPQExpBuffer(buf);
    free(mine);
    mine = NULL;
}

#define MAX_P_READ_BUF 1024
typedef struct tag_pcommand {
    FILE* pfp;
    char readbuf[MAX_P_READ_BUF];
    int cur_buf_loc;
    char* dbname;
    int retvalue;
} PARALLEL_COMMAND_S;

PARALLEL_COMMAND_S* g_parallel_command_cxt = NULL;
static int g_max_commands_parallel = 0;
static int g_cur_commands_parallel = 0;

/*
 * Dump contents of databases.
 */
static void dumpDatabases(PGconn* conn)
{
    PGresult* res = NULL;
    char* command = NULL;
    char* dbfilename = NULL;
    int i = 0, j = 0;
    int ndb = 0;
    int loop_times = 0;
    int loop_nums = 0;
    int rc = 0;

    if (server_version >= 70100)
        res = executeQuery(conn, "SELECT datname FROM pg_database WHERE datallowconn ORDER BY 1");
    else
        res = executeQuery(conn, "SELECT datname FROM pg_database ORDER BY 1");

    ndb = PQntuples(res);

    if (parallel_jobs != NULL)
        g_max_commands_parallel = atoi(parallel_jobs);
    else
        g_max_commands_parallel = 1;

    loop_times = (ndb / g_max_commands_parallel) + (ndb % g_max_commands_parallel ? 1 : 0);
    g_parallel_command_cxt = (PARALLEL_COMMAND_S*)pg_malloc(g_max_commands_parallel * sizeof(PARALLEL_COMMAND_S));

    for (i = 0; i < loop_times; i++) {
        loop_nums = (i == loop_times - 1) ? (ndb - i * g_max_commands_parallel) : g_max_commands_parallel;
        g_cur_commands_parallel = 0;

        rc = memset_s(g_parallel_command_cxt,
            g_max_commands_parallel * sizeof(PARALLEL_COMMAND_S),
            '\0',
            g_max_commands_parallel * sizeof(PARALLEL_COMMAND_S));
        securec_check_c(rc, "\0", "\0");

        for (j = 0; j < loop_nums; j++) {
            char* dbname = PQgetvalue(res, i * g_max_commands_parallel + j, 0);

            if ((strlen("template1") == strlen(dbname)) && (strncmp(dbname, "template1", strlen("template1")) == 0) &&
                (dump_templatedb == false)) {
                if (verbose)
                    write_stderr(_("%s: Skipping database \"%s\" as include-templatedb option not specified...\n"),
                        progname,
                        dbname);
                continue;
            }

            if (verbose)
                write_stderr(_("%s: dumping database \"%s\"...\n"), progname, dbname);

            dumpall_printf(OPF, "\\connect %s\n\n", fmtId(dbname));

            if (filename != NULL)
                fclose(OPF);

            if (parallel_jobs != NULL)
                dbfilename = getDatabaseFilename(dbname);
            else
                dbfilename = filename;

            command = formDumpCommand(dbname, dbfilename);

            if (verbose)
                write_stderr(_("%s: running \"%s\"\n"), progname, command);

            (void)executePopenCommandsParallel(command, dbname);

            if (filename != NULL) {
                OPF = fopen(filename, PG_BINARY_A);
                if (OPF == NULL) {
                    write_stderr(
                        _("%s: could not re-open the output file \"%s\": %s\n"), progname, filename, strerror(errno));
                    exit_nicely(1);
                }
            }

            GS_FREE(command);
            if (parallel_jobs != NULL) {
                GS_FREE(dbfilename);
            }
        }

        (void)readPopenOutputParallel();
    }

    GS_FREE(g_parallel_command_cxt);
    PQclear(res);
}

/*
 * form database file name.
 */
static char* getDatabaseFilename(const char* dbname)
{
    int rc;

    size_t fileLen = strlen(filename);
    size_t dbLen = strlen(dbname);
    size_t dbFileSize = fileLen + dbLen + 2;
    char* databaseFilename = (char*)pg_malloc(dbFileSize);
    rc = memset_s(databaseFilename, dbFileSize, '\0', dbFileSize);
    securec_check_c(rc, "\0", "\0");

    size_t tmpSize = fileLen + 1;
    char* tmp = (char*)pg_malloc(tmpSize);
    rc = memset_s(tmp, tmpSize, '\0', tmpSize);
    securec_check_c(rc, "\0", "\0");

    rc = strncpy_s(tmp, tmpSize, filename, fileLen);
    securec_check_c(rc, "\0", "\0");

    if (((int)strlen(tmp) > 4) && (strncmp(tmp + strlen(tmp) - 4, ".sql", 4) == 0)) {
        tmp[strlen(tmp) - 4] = '\0';
        rc = snprintf_s(databaseFilename,
            dbFileSize,
            dbFileSize - 1,
            "%s_%s.sql",
            tmp,
            dbname);
    } else {
        rc = snprintf_s(databaseFilename,
            dbFileSize,
            dbFileSize - 1,
            "%s_%s",
            tmp,
            dbname);
    }
    securec_check_ss_c(rc, databaseFilename, "\0");

    free(tmp);
    tmp = NULL;

    return databaseFilename;
}

/*
 * form pg_dump command for databases.
 */
static char* formDumpCommand(const char* dbname, const char* dbfilename)
{
    int rc;
    PQExpBuffer connstr = createPQExpBuffer();
    PQExpBuffer cmd = createPQExpBuffer();
    char* command = NULL;
    size_t cmdSize;
    size_t cmdLen;

    appendPQExpBuffer(cmd, SYSTEMQUOTE "\"%s\" %s", pg_dump_bin, pgdumpopts->data);

    /*  add --non-lock-table parameter */
    if (non_Lock_Table)
        appendPQExpBuffer(cmd, " --non-lock-table ");

    /*  add --include-alter-table parameter */
    if (include_alter_table)
        appendPQExpBuffer(cmd, " --include-alter-table ");
    /*
     * If we have a filename, use the undocumented plain-append pg_dump
     * format.
     */
    if (filename != NULL) {
        appendPQExpBuffer(cmd, " -f ");
        doShellQuoting(cmd, dbfilename);
        appendPQExpBuffer(cmd, " -Fa ");
    } else
        appendPQExpBuffer(cmd, " -Fp ");

    /* when do full upgrade, we do not need dump pmk schema which is at database postgres */
    if (binary_upgrade && (strlen("postgres") == strlen(dbname)) &&
        (strncmp(dbname, "postgres", strlen("postgres")) == 0))
        appendPQExpBuffer(cmd, " -N pmk ");

    /*
     * Construct a connection string from the database name, like
     * dbname='<database name>'. pg_dump would usually also accept the
     * database name as is, but if it contains any = characters, it would
     * incorrectly treat it as a connection string.
     */
    appendPQExpBuffer(connstr, "dbname='");
    doConnStrQuoting(connstr, dbname);
    appendPQExpBuffer(connstr, "'");

    doShellQuoting(cmd, connstr->data);

    appendPQExpBuffer(cmd, "%s", SYSTEMQUOTE);

    fflush(stdout);
    fflush(stderr);

    cmdLen = strlen(cmd->data);
    cmdSize = cmdLen + 1;
    command = (char*)pg_malloc(cmdSize);
    rc = memset_s(command, cmdSize, '\0', cmdSize);
    securec_check_c(rc, "\0", "\0");

    rc = strncpy_s(command, cmdSize, cmd->data, cmdLen);
    securec_check_c(rc, "\0", "\0");

    destroyPQExpBuffer(connstr);
    destroyPQExpBuffer(cmd);

    return command;
}

/*
 * execute gs_dump command parallel
 */
static void executePopenCommandsParallel(const char* cmd, char* dbname)
{
    int rc;
    PARALLEL_COMMAND_S* curr_cxt = NULL;

    curr_cxt = &g_parallel_command_cxt[g_cur_commands_parallel];
    g_cur_commands_parallel++;

    curr_cxt->cur_buf_loc = 0;
    rc = memset_s(curr_cxt->readbuf, sizeof(curr_cxt->readbuf), '\0', sizeof(curr_cxt->readbuf));
    securec_check_c(rc, "\0", "\0");

    curr_cxt->pfp = popen(cmd, "r");
    curr_cxt->dbname = dbname;

    if (curr_cxt->pfp != NULL) {
        uint32 flags;
        int fd = fileno(curr_cxt->pfp);
        flags = fcntl(fd, F_GETFL, 0);
        flags |= O_NONBLOCK;
        (void)fcntl(fd, F_SETFL, flags);
    }
}

/*
 * read popen output parallel
 */
static void readPopenOutputParallel()
{
    int rc = 0;
    int idx = 0;
    bool read_pending = false;
    char* result = NULL;
    PARALLEL_COMMAND_S* curr_cxt = NULL;
    bool error_in_execution = false;

    result = (char*)pg_malloc(MAX_P_READ_BUF + 1);
    rc = memset_s(result, MAX_P_READ_BUF + 1, '\0', MAX_P_READ_BUF + 1);
    securec_check_c(rc, "\0", "\0");

    read_pending = true;
    while (read_pending == true) {
        read_pending = false;
        for (idx = 0; idx < g_cur_commands_parallel; idx++) {
            curr_cxt = g_parallel_command_cxt + idx;

            /* pipe closed, stop to read pipe */
            if (curr_cxt->pfp == NULL) {
                continue;
            }
            errno = 0;
            /* successful get some results from pipe, read again */
            if (fgets(result, MAX_P_READ_BUF - 1, curr_cxt->pfp) != NULL) {
                int len = strlen(result);
                bool hasnewline = false;

                read_pending = true;
                if (len > 1 && result[len - 1] == '\n') {
                    hasnewline = true;
                } else if ((curr_cxt->cur_buf_loc + len + 1) < (int)sizeof(curr_cxt->readbuf)) {
                    rc = strncpy_s(curr_cxt->readbuf + curr_cxt->cur_buf_loc,
                        sizeof(curr_cxt->readbuf) - curr_cxt->cur_buf_loc,
                        result,
                        len + 1);
                    securec_check_c(rc, "\0", "\0");

                    curr_cxt->cur_buf_loc += len;
                    continue;
                }

                write_stderr(_("%s%s%s"), curr_cxt->readbuf, result, (hasnewline ? "" : "\n"));

                curr_cxt->readbuf[0] = '\0';
                curr_cxt->cur_buf_loc = 0;
            }
            /* no results currently, read again */
            else if (errno == EAGAIN) {
                read_pending = true;
                continue;
            }
            /* failed to get results from pipe, exit */
            else {
                curr_cxt->retvalue = pclose(curr_cxt->pfp);
                curr_cxt->pfp = NULL;

                if (curr_cxt->retvalue != 0) {
                    error_in_execution = true;
                    write_stderr(_("%s: gs_dump failed on database \"%s\", exiting\n"), progname, curr_cxt->dbname);
                    break;
                }
            }
        }

        if (error_in_execution == true) {
            break;
        }
        (void)SleepInMilliSec(100);
    }

    /* an error happend, close all commands and exit */
    if (error_in_execution) {
        for (idx = 0; idx < g_cur_commands_parallel; idx++) {
            curr_cxt = g_parallel_command_cxt + idx;

            if (curr_cxt->pfp == NULL) {
                continue;
            }

            curr_cxt->retvalue = pclose(curr_cxt->pfp);
        }

        free(result);
        result = NULL;
        exit_nicely(1);
    }

    g_cur_commands_parallel = 0;

    free(result);
    result = NULL;
}

static void SleepInMilliSec(uint32_t sleepMs)
{
    struct timespec ts;
    ts.tv_sec = (sleepMs - (sleepMs % 1000)) / 1000;
    ts.tv_nsec = (sleepMs % 1000) * 1000;

    (void)nanosleep(&ts, NULL);
}

/*
 * buildShSecLabels
 *
 * Build SECURITY LABEL command(s) for an shared object
 *
 * The caller has to provide object type and identifier to select security
 * labels from pg_seclabels system view.
 */
static void buildShSecLabels(PGconn* conn, const char* catalog_name, uint32 objectId, PQExpBuffer buffer,
    const char* target, const char* objname)
{
    PQExpBuffer sql = createPQExpBuffer();
    PGresult* res = NULL;

    buildShSecLabelQuery(conn, catalog_name, objectId, sql);
    res = executeQuery(conn, sql->data);
    emitShSecLabels(conn, res, buffer, target, objname);

    PQclear(res);
    destroyPQExpBuffer(sql);
}

/*
 * Make a database connection with the given parameters.  An
 * interactive password prompt is automatically issued if required.
 *
 * If fail_on_error is false, we return NULL without printing any message
 * on failure, but preserve any prompted password for the next try.
 */
static PGconn* connectDatabase(const char* dbname, const char* pchPghost, const char* pchPgport, const char* pchPguser,
    const char* pchPasswrd, enum trivalue enprompt_password, bool fail_on_error)
{
    PGconn* conn = NULL;
    bool new_pass = false;
    const char* remoteversion_str = NULL;
    int my_version = 0;

    if ((enprompt_password == TRI_YES) && (pchPasswrd == NULL))
        pchPasswrd = simple_prompt("Password: ", 100, false);

    /*
     * Start the connection.  Loop until we have a password if requested by
     * backend.
     */
    do {
#define PARAMS_ARRAY_SIZE 7
        const char** keywords = (const char**)pg_malloc(PARAMS_ARRAY_SIZE * sizeof(*keywords));
        const char** values = (const char**)pg_malloc(PARAMS_ARRAY_SIZE * sizeof(*values));

        keywords[0] = "host";
        values[0] = pchPghost;
        keywords[1] = "port";
        values[1] = pchPgport;
        keywords[2] = "user";
        values[2] = pchPguser;
        keywords[3] = "password";
        values[3] = pchPasswrd;
        keywords[4] = "dbname";
        values[4] = dbname;
        keywords[5] = "fallback_application_name";
        values[5] = progname;
        keywords[6] = NULL;
        values[6] = NULL;

        new_pass = false;
        conn = PQconnectdbParams(keywords, values, true);

        free(keywords);
        keywords = NULL;
        free(values);
        values = NULL;

        if (conn == NULL) {
            write_stderr(_("%s: could not connect to database \"%s\""), progname, dbname);
            exit_nicely(1);
        }

        if (PQstatus(conn) == CONNECTION_BAD && (strstr(PQerrorMessage(conn), "password") != NULL) &&
            pchPasswrd == NULL && enprompt_password != TRI_NO) {
            PQfinish(conn);
            pchPasswrd = simple_prompt("Password: ", 100, false);
            new_pass = true;
        }
    } while (new_pass);

    /* check to see that the backend connection was successfully made */
    if (PQstatus(conn) == CONNECTION_BAD) {
        if (fail_on_error) {
            write_stderr(_("%s: could not connect to database \"%s\": %s\n"), progname, dbname, PQerrorMessage(conn));
            exit_nicely(1);
        } else {
            PQfinish(conn);
            return NULL;
        }
    }

    remoteversion_str = PQparameterStatus(conn, "server_version");
    if (remoteversion_str == NULL) {
        write_stderr(_("%s: could not get server version\n"), progname);
        exit_nicely(1);
    }
    server_version = parse_version(remoteversion_str);
    if (server_version < 0) {
        write_stderr(_("%s: could not parse server version \"%s\"\n"), progname, remoteversion_str);
        exit_nicely(1);
    }

    my_version = parse_version(PG_VERSION);
    if (my_version < 0) {
        write_stderr(_("%s: could not parse version \"%s\"\n"), progname, PG_VERSION);
        exit_nicely(1);
    }

    /*
     * We allow the server to be back to 7.0, and up to any minor release of
     * our own major version.  (See also version check in pg_dump.c.)
     */
    if (my_version != server_version && (server_version < 70000 || (server_version / 100) > (my_version / 100))) {
        write_stderr(_("server version: %s; %s version: %s\n"), remoteversion_str, progname, PG_VERSION);
        write_stderr(_("aborting because of server version mismatch\n"));
        exit_nicely(1);
    }

    /*
     * On 7.3 and later, make sure we are not fooled by non-system schemas in
     * the search path.
     */
    if (server_version >= 70300)
        executeCommand(conn, "SET search_path = pg_catalog");

    return conn;
}

/*
 * Run a query, return the results, exit program on failure.
 */
static PGresult* executeQuery(PGconn* conn, const char* query)
{
    PGresult* res = NULL;

    if (verbose)
        write_stderr(_("%s: executing %s\n"), progname, query);

    res = PQexec(conn, query);
    if ((res == NULL) || PQresultStatus(res) != PGRES_TUPLES_OK) {
        write_stderr(_("%s: query failed: %s"), progname, PQerrorMessage(conn));
        write_stderr(_("%s: query was: %s\n"), progname, query);
        PQfinish(conn);
        exit_nicely(1);
    }

    return res;
}

/*
 * As above for a SQL command (which returns nothing).
 */
static void executeCommand(PGconn* conn, const char* query)
{
    PGresult* res = NULL;

    if (verbose)
        write_stderr(_("%s: executing %s\n"), progname, query);

    res = PQexec(conn, query);
    if (res == NULL || PQresultStatus(res) != PGRES_COMMAND_OK) {
        write_stderr(_("%s: query failed: %s"), progname, PQerrorMessage(conn));
        write_stderr(_("%s: query was: %s\n"), progname, query);
        PQfinish(conn);
        exit_nicely(1);
    }

    PQclear(res);
}

/*
 * dumpTimestamp
 */
static void dumpTimestamp(const char* msg)
{
    char buf[256];
    time_t now = time(NULL);
    struct tm stTm;

    /*
     * We don't print the timezone on Win32, because the names are long and
     * localized, which means they may contain characters in various random
     * encodings; this has been seen to cause encoding errors when reading the
     * dump script.
     */
    if ((localtime_r(&now, &stTm) != NULL) && (strftime(buf,
                                                        sizeof(buf),
#ifndef WIN32
                                                        "%Y-%m-%d %H:%M:%S %Z",
#else
                                                        "%Y-%m-%d %H:%M:%S",
#endif
                                                        localtime_r(&now, &stTm))) != 0)
        fprintf(OPF, "-- %s %s\n\n", msg, buf);
}

/*
 * Append the given string to the buffer, with suitable quoting for passing
 * the string as a value, in a keyword/pair value in a libpq connection
 * string
 */
static void doConnStrQuoting(PQExpBuffer buf, const char* str)
{
    while (*str) {
        /* ' and \ must be escaped by to \' and \\ */
        if (*str == '\'' || *str == '\\')
            appendPQExpBufferChar(buf, '\\');

        appendPQExpBufferChar(buf, *str);
        str++;
    }
}

static void doShellQuotingForRandomstring(PQExpBuffer buf, const char* str)
{
    const char* p = str;
    appendPQExpBufferChar(buf, '\'');
    for (p = str; *p; p++) {
        if (*p == '\n' || *p == '\r')
            appendPQExpBufferChar(buf, '#');

        if (*p == '\'')
            appendPQExpBuffer(buf, "'\"'\"'");
        else
            appendPQExpBufferChar(buf, *p);
    }
    appendPQExpBufferChar(buf, '\'');
}

/*
 * Append the given string to the shell command being built in the buffer,
 * with suitable shell-style quoting.
 *
 * Forbid LF or CR characters, which have scant practical use beyond designing
 * security breaches. The Windows command shell is unusable as a conduit for
 * arguments containing LF or CR characters. A future major release should
 * reject those characters in CREATE ROLE and CREATE DATABASE, because use
 * there eventually leads to errors here.
 */
static void doShellQuoting(PQExpBuffer buf, const char* str)
{
    const char* p = str;

#ifndef WIN32
    appendPQExpBufferChar(buf, '\'');
    for (p = str; *p; p++) {
        if (*p == '\n' || *p == '\r') {
            fprintf(stderr, _("shell command argument contains a newline or carriage.\n"));
            exit(1);
        }

        if (*p == '\'')
            appendPQExpBuffer(buf, "'\"'\"'");
        else
            appendPQExpBufferChar(buf, *p);
    }
    appendPQExpBufferChar(buf, '\'');
#else  /* WIN32 */

    appendPQExpBufferChar(buf, '"');
    for (p = str; *p; p++) {
        if (*p == '\n' || *p == '\r') {
            fprintf(stderr, _("shell command argument contains a newline or carriage.\n"));
            exit(1);
        }

        if (*p == '"')
            appendPQExpBuffer(buf, "\\\"");
        else
            appendPQExpBufferChar(buf, *p);
    }
    appendPQExpBufferChar(buf, '"');
#endif /* WIN32 */
}

#ifdef PGXC
static void dumpNodes(PGconn* conn)
{
    PQExpBuffer query;
    PGresult* res = NULL;
    int num = 0;
    int i = 0;
    int i_node_query = 0;
    /* judge the colunm hostis_primary whether is exists or not */
    bool is_hostis_primary_exists = false;
    bool is_nodeis_central_exists = false;
    bool is_nodeis_active_exists = false;

    is_hostis_primary_exists = is_column_exists(conn, PgxcNodeRelationId, "hostis_primary");
    is_nodeis_central_exists = is_column_exists(conn, PgxcNodeRelationId, "nodeis_central");
    is_nodeis_active_exists = is_column_exists(conn, PgxcNodeRelationId, "nodeis_active");

    query = createPQExpBuffer();
    appendPQExpBuffer(query,
        "SELECT 'CREATE NODE \"' || node_name || '\"  WITH (TYPE = ' || pg_catalog.chr(39) "
        "  || (case when node_type='C'  then 'coordinator' else 'datanode' end) || pg_catalog.chr(39)  "
        "  || ' , HOST = ' || pg_catalog.chr(39) || node_host || pg_catalog.chr(39) "
        "  || ' , HOST1 = ' || pg_catalog.chr(39) "
        "  || node_host1 || pg_catalog.chr(39) || ', %s PORT = ' "
        "  || (case when n.node_name = cn.setting then p.setting::int else node_port end) || ', PORT1 = ' "
        "  || (case when n.node_name = cn.setting then p.setting::int else node_port1 end) "
        "  || ' , SCTP_PORT = ' || sctp_port || ' , CONTROL_PORT = ' || control_port "
        "  || ' , SCTP_PORT1 = ' || sctp_port1 || ' , CONTROL_PORT1 = ' || control_port1 "
        "  || (case when nodeis_primary='t' then ', PRIMARY' else ' ' end) "
        "  || (case when nodeis_preferred then ', PREFERRED' else '' end) "
        "  || (case when node_type='D' then ', RW = TRUE' when node_type='S' then ', RW = FALSE' else '' end) "
        "  || ');' || %s || %s "
        " AS node_query "
        " FROM pg_catalog.pgxc_node n, (select setting from pg_settings where name = 'pgxc_node_name')  cn, "
        "  (select setting from pg_settings where name ='port') p",
        is_hostis_primary_exists ? "HOSTPRIMARY = ' || hostis_primary || '," : "",
        is_nodeis_central_exists ? "(CASE WHEN n.nodeis_central = true THEN 'UPDATE pgxc_node SET nodeis_central = "
                                   "true WHERE node_name=''' || n.node_name || ''';' ELSE ' ' END)"
                                 : "''",
        is_nodeis_active_exists ? "(CASE WHEN n.nodeis_active = false THEN 'UPDATE pgxc_node SET nodeis_active = false "
                                  "WHERE node_name=''' || n.node_name || ''';' ELSE ' ' END)"
                                : "''");

    if (binary_upgrade) {
        appendPQExpBuffer(
            query, " WHERE node_name not in (select setting from pg_settings where name ='pgxc_node_name')");
    }

    appendPQExpBuffer(query, " ORDER BY oid;");

    res = executeQuery(conn, query->data);
    num = PQntuples(res);
    i_node_query = PQfnumber(res, "node_query");

    if (num > 0)
        dumpall_printf(OPF, "--\n-- Nodes\n--\n\n");

    for (i = 0; i < num; i++) {
        dumpall_printf(OPF, "%s\n", PQgetvalue(res, i, i_node_query));
    }
    dumpall_printf(OPF, "\n");

    PQclear(res);
    destroyPQExpBuffer(query);
}

static void dumpNodeGroups(PGconn* conn)
{
    PQExpBuffer query;
    PGresult* res = NULL;
    int num = 0;
    int i = 0;
    int i_group_query = 0;
    int i_update_query = 0;
    /* judge the colunm whether is exists or not */
    bool is_installation_exists = false;
    bool is_group_kind_exists = false;

    is_installation_exists = is_column_exists(conn, PgxcGroupRelationId, "is_installation");
    is_group_kind_exists = is_column_exists(conn, PgxcGroupRelationId, "group_kind");
    query = createPQExpBuffer();
    if (is_installation_exists) {
        if (include_buckets) {
            if (is_group_kind_exists) {
                appendPQExpBuffer(query,
                    "SELECT 'CREATE NODE GROUP \"' || pgxc_group.group_name || '\" WITH(' || "
                    "pg_catalog.string_agg('\"' || "
                    "pgxc_node.node_name || '\"',',') || ') BUCKETS(' || pgxc_group.group_buckets || ') ' || (CASE "
                    "WHEN pgxc_group.group_kind = 'v' THEN 'VCGROUP' ELSE '' END) || ';' AS group_query,"
                    " (CASE WHEN pgxc_group.is_installation = 'TRUE' THEN 'UPDATE pg_catalog.pgxc_group SET "
                    "is_installation = TRUE, group_kind = ''i'' WHERE group_name = '\''||pgxc_group.group_name||'\'';'"
                    " WHEN pgxc_group.in_redistribution = 'y' THEN 'UPDATE pg_catalog.pgxc_group SET in_redistribution "
                    "= ''y'' WHERE group_name = '\''||pgxc_group.group_name||'\'';' ELSE '' END) AS update_query"
                    " FROM pg_catalog.pgxc_node, pg_catalog.pgxc_group"
                    " WHERE pgxc_group.group_name IN (SELECT group_name FROM pgxc_group WHERE group_kind != 'e' OR "
                    "group_kind IS NULL) AND pgxc_node.oid = ANY (pgxc_group.group_members)"
                    " GROUP BY pgxc_group.group_kind, pgxc_group.group_name, pgxc_group.group_buckets, "
                    "pgxc_group.is_installation, pgxc_group.in_redistribution"
                    " ORDER BY pgxc_group.is_installation desc ");
            } else {
                appendPQExpBuffer(query,
                    "SELECT 'CREATE NODE GROUP \"' || pgxc_group.group_name || '\" WITH(' || "
                    "pg_catalog.string_agg('\"' || "
                    "pgxc_node.node_name || '\"',',') || ') BUCKETS(' || pgxc_group.group_buckets || ');' AS "
                    "group_query,"
                    " (CASE WHEN pgxc_group.is_installation = 'TRUE' THEN 'UPDATE pg_catalog.pgxc_group SET "
                    "is_installation = TRUE, group_kind = ''i'' WHERE group_name = '\''||pgxc_group.group_name||'\'';'"
                    " WHEN pgxc_group.in_redistribution = 'y' THEN 'UPDATE pg_catalog.pgxc_group SET in_redistribution "
                    "= ''y'' WHERE group_name = '\''||pgxc_group.group_name||'\'';' ELSE '' END) AS update_query"
                    " FROM pg_catalog.pgxc_node, pg_catalog.pgxc_group"
                    " WHERE pgxc_group.group_name IN (SELECT group_name FROM pgxc_group WHERE group_kind != 'e' OR "
                    "group_kind IS NULL) AND pgxc_node.oid = ANY (pgxc_group.group_members)"
                    " GROUP BY pgxc_group.group_name, pgxc_group.group_buckets, pgxc_group.is_installation, "
                    "pgxc_group.in_redistribution"
                    " ORDER BY pgxc_group.is_installation desc ");
            }
        } else {
            if (is_group_kind_exists) {
                appendPQExpBuffer(query,
                    "SELECT 'CREATE NODE GROUP \"' || pgxc_group.group_name || '\" WITH(' || "
                    "pg_catalog.string_agg('\"' || "
                    "pgxc_node.node_name || '\"',',') || ') ' || (CASE WHEN pgxc_group.group_kind = 'v' THEN 'VCGROUP' "
                    "ELSE '' END) || ';' AS group_query,"
                    " (CASE WHEN pgxc_group.is_installation = 'TRUE' THEN 'UPDATE pg_catalog.pgxc_group SET "
                    "is_installation = TRUE, group_kind = ''i'' WHERE group_name = '\''||pgxc_group.group_name||'\'';'"
                    " WHEN pgxc_group.in_redistribution = 'y' THEN 'UPDATE pg_catalog.pgxc_group SET in_redistribution "
                    "= ''y'' WHERE group_name = '\''||pgxc_group.group_name||'\'';' ELSE '' END) AS update_query"
                    " FROM pg_catalog.pgxc_node, pg_catalog.pgxc_group"
                    " WHERE pgxc_group.group_name IN (SELECT group_name FROM pgxc_group WHERE group_kind != 'e' OR "
                    "group_kind IS NULL) AND pgxc_node.oid = ANY (pgxc_group.group_members)"
                    " GROUP BY pgxc_group.group_kind, pgxc_group.group_name, pgxc_group.is_installation, "
                    "pgxc_group.in_redistribution"
                    " ORDER BY pgxc_group.is_installation desc ");
            } else {
                appendPQExpBuffer(query,
                    "SELECT 'CREATE NODE GROUP \"' || pgxc_group.group_name || '\" WITH(' || "
                    "pg_catalog.string_agg('\"' || pgxc_node.node_name || '\"',',') || ');' AS group_query,"
                    " (CASE WHEN pgxc_group.is_installation = 'TRUE' THEN 'UPDATE pg_catalog.pgxc_group SET "
                    "is_installation = TRUE, group_kind = ''i'' WHERE group_name = '\''||pgxc_group.group_name||'\'';'"
                    " WHEN pgxc_group.in_redistribution = 'y' THEN 'UPDATE pg_catalog.pgxc_group SET in_redistribution "
                    "= ''y'' WHERE group_name = '\''||pgxc_group.group_name||'\'';' ELSE '' END) AS update_query"
                    " FROM pg_catalog.pgxc_node, pg_catalog.pgxc_group"
                    " WHERE pgxc_group.group_name IN (SELECT group_name FROM pgxc_group WHERE group_kind != 'e' OR "
                    "group_kind IS NULL) AND pgxc_node.oid = ANY (pgxc_group.group_members)"
                    " GROUP BY pgxc_group.group_name, pgxc_group.is_installation, pgxc_group.in_redistribution"
                    " ORDER BY pgxc_group.is_installation desc ");
            }
        }
    } else {
        /*
         * We known that if not is_installation_exists, group_kind is not exists.
         */
        const char* default_acl = "pg_catalog.concat('{', pg_catalog.concat(current_user,'=UCp/', "
                                  "current_user, ',=UCp/',current_user), '}')";
        if (include_buckets) {
            appendPQExpBuffer(query,
                "select 'CREATE NODE GROUP \"' || pgxc_group.group_name"
                " || '\" WITH(' || pg_catalog.string_agg('\"' || node_name || '\"',',') || ')"
                " BUCKETS(' || pgxc_group.group_buckets || ');' as group_query, "
                " 'UPDATE pg_catalog.pgxc_group SET is_installation = TRUE, group_kind = ''i'' WHERE group_name = "
                "'\''||pgxc_group.group_name||'\'';"
                "\nUPDATE pg_catalog.pgxc_group SET group_acl = '\''||%s||'\''::aclitem[] WHERE is_installation = "
                "TRUE;' AS update_query"
                " from pg_catalog.pgxc_node, pg_catalog.pgxc_group"
                " where pgxc_node.oid = any (pgxc_group.group_members)"
                " group by pgxc_group.group_name, pgxc_group.group_buckets, pgxc_group.in_redistribution"
                " order by pgxc_group.group_name",
                default_acl);
        } else {
            appendPQExpBuffer(query,
                "select 'CREATE NODE GROUP \"' || pgxc_group.group_name"
                " || '\" WITH(' || pg_catalog.string_agg('\"' || node_name || '\"',',') || ');' as group_query, "
                " 'UPDATE pg_catalog.pgxc_group SET is_installation = TRUE, group_kind = ''i'' WHERE group_name = "
                "'\''||pgxc_group.group_name||'\'';"
                "\nUPDATE pg_catalog.pgxc_group SET group_acl = '\''||%s||'\''::aclitem[] WHERE is_installation = "
                "TRUE;' AS update_query"
                " from pg_catalog.pgxc_node, pg_catalog.pgxc_group"
                " where pgxc_node.oid = any (pgxc_group.group_members)"
                " group by pgxc_group.group_name, pgxc_group.in_redistribution"
                " order by pgxc_group.group_name",
                default_acl);
        }
    }

    res = executeQuery(conn, query->data);
    num = PQntuples(res);
    i_group_query = PQfnumber(res, "group_query");
    i_update_query = PQfnumber(res, "update_query");

    if (num > 0)
        dumpall_printf(OPF, "--\n-- Node groups\n--\n\n");

    for (i = 0; i < num; i++) {
        char* upgrade_sql = NULL;
        upgrade_sql = PQgetvalue(res, i, i_update_query);

        dumpall_printf(OPF, "%s\n", PQgetvalue(res, i, i_group_query));
        if ((upgrade_sql != NULL) && (upgrade_sql[0] != '\0')) {
            dumpall_printf(OPF, "%s\n", upgrade_sql);
        }
    }
    dumpall_printf(OPF, "\n");
    destroyPQExpBuffer(query);
    PQclear(res);

    /* Adapt multi-nodegroup, change grammar grant */
    if (is_installation_exists) {
        PQExpBuffer query = createPQExpBuffer();
        PQExpBuffer buf = createPQExpBuffer();
        PQExpBuffer owner = createPQExpBuffer();
        PGresult* res = NULL;
        int i;
        char* install_acl = NULL;
        char* supername = NULL;

        // query super user
        appendPQExpBuffer(owner, "select usename from pg_user where usesysid = 10; ");
        res = executeQuery(conn, owner->data);
        supername = PQgetvalue(res, 0, 0);
        PQclear(res);

        // query groupname acl installation
        appendPQExpBuffer(query, "SELECT group_name, group_acl, is_installation FROM pg_catalog.pgxc_group; ");

        /* locate the attributes in the result set */
        res = executeQuery(conn, query->data);

        for (i = 0; i < PQntuples(res); i++) {
            char* group_name = PQgetvalue(res, i, 0);
            char* group_acl = PQgetvalue(res, i, 1);
            char* is_install = PQgetvalue(res, i, 2);

            if (is_install[0] == 't') {
                if (NULL == group_acl)
                    exit_horribly(NULL, "cannot duplicate null pointer\n");
                install_acl = strdup(group_acl);
                if (install_acl == NULL)
                    exit_horribly(NULL, "out of memory\n");
                continue;
            }
            if (!skip_acls &&
                !buildACLCommands(
                    group_name, NULL, "NODE GROUP", group_acl, supername, "", server_version, buf, conn)) {
                write_stderr(
                    _("%s: could not parse ACL list (%s) for node group \"%s\"\n"), progname, group_acl, group_name);

                free(install_acl);
                install_acl = NULL;
                destroyPQExpBuffer(query);
                destroyPQExpBuffer(buf);
                destroyPQExpBuffer(owner);

                PQfinish(conn);
                exit_nicely(1);
            }

            /* At last, we push these SQL into file */
            dumpall_printf(OPF, "%s", buf->data);
        }

        if (install_acl != NULL) {
            dumpall_printf(OPF, "\n--\n-- Update installation group acl\n--\n\n");
            dumpall_printf(OPF,
                "UPDATE pg_catalog.pgxc_group SET group_acl = '%s'::aclitem[] WHERE is_installation = true;",
                install_acl);
        }

        dumpall_printf(OPF, "\n\n");

        free(install_acl);
        install_acl = NULL;
        destroyPQExpBuffer(query);
        destroyPQExpBuffer(buf);
        destroyPQExpBuffer(owner);
        PQclear(res);
    }

    /*
     * when dump the role information(-r/-g/default, not -t),
     * it should print out the relation about node group and role
     */
    if (!tablespaces_only) {
        dumpall_printf(OPF, "--\n-- Output role and node group association information\n--\n\n");
        dumpAlterRolesForNodeGroup(conn);
    }
}

static void dumpResourcePools(PGconn* conn)
{
    PQExpBuffer query, buf;
    PGresult* res = NULL;
    int num = 0;
    int i = 0;
    int i_respool_name = 0;
    int i_act_statements = 0;
    int i_max_dop = 0;
    int i_memory_limit = 0;
    int i_mem_percent = 0;
    int i_control_group = 0;
    int i_parentid = 0;
    int i_io_limits = 0;
    int i_io_priority = 0;
    int i_nodegroupname = 0;
    /* judge the colunm whether is exists or not */
    bool is_active_statements_exists = false;
    bool is_node_group_exists = false;

    bool match = false;
    char poid_reserve[16] = {0};
    char cgroup_list[10][64];
    char cgroup_name[64] = {0};
    char cgroup_name_default[128] = {0};

    errno_t rc = memset_s(cgroup_list, sizeof(cgroup_list), 0, sizeof(cgroup_list));
    securec_check_c(rc, "\0", "\0");

    query = createPQExpBuffer();
    buf = createPQExpBuffer();
    is_active_statements_exists = is_column_exists(conn, ResourcePoolRelationId, "active_statements");
    is_node_group_exists = is_column_exists(conn, ResourcePoolRelationId, "nodegroup");

    appendPQExpBuffer(query,
        "select * from pg_catalog.pg_resource_pool where pg_resource_pool.oid != %d "
        "order by pg_resource_pool.control_group;",
        DEFAULT_POOL_OID);

    res = executeQuery(conn, query->data);

    i_respool_name = PQfnumber(res, "respool_name");
    i_control_group = PQfnumber(res, "control_group");
    i_mem_percent = PQfnumber(res, "mem_percent");

    if (is_active_statements_exists) {
        i_act_statements = PQfnumber(res, "active_statements");
        i_max_dop = PQfnumber(res, "max_dop");
        i_memory_limit = PQfnumber(res, "memory_limit");
        i_parentid = PQfnumber(res, "parentid");
        i_io_limits = PQfnumber(res, "io_limits");
        i_io_priority = PQfnumber(res, "io_priority");
        if (is_node_group_exists) {
            i_nodegroupname = PQfnumber(res, "nodegroup");
        }
    }

    num = PQntuples(res);

    fprintf(OPF, "--\n-- Resource pools\n--\n\n");

    for (i = 0; i < num; i++) {
        char* respool_name = PQgetvalue(res, i, i_respool_name);
        char* control_group = PQgetvalue(res, i, i_control_group);
        char* mem_percent = PQgetvalue(res, i, i_mem_percent);
        char* act_statements = NULL;
        char* max_dop = NULL;
        char* memory_limit = NULL;
        char* parentid = NULL;
        char* io_limits = NULL;
        char* io_priority = NULL;
        char* nodegroupname = NULL;

        // reset flag --match -- check whether same workload group has been found
        if (match) {
            match = false;
        }
        resetPQExpBuffer(buf);

        if (is_active_statements_exists) {
            act_statements = PQgetvalue(res, i, i_act_statements);
            max_dop = PQgetvalue(res, i, i_max_dop);
            memory_limit = PQgetvalue(res, i, i_memory_limit);
            parentid = PQgetvalue(res, i, i_parentid);
            io_limits = PQgetvalue(res, i, i_io_limits);
            io_priority = PQgetvalue(res, i, i_io_priority);

            if (is_node_group_exists) {
                nodegroupname = PQgetvalue(res, i, i_nodegroupname);
            }

            if ((parentid != NULL) && (parentid[0] != '\0') && (parentid[0] != '0') && (control_group != NULL) &&
                ('\0' != control_group[0])) {
                char workload_name[64] = {0};
                char* tmp1 = NULL;
                char* tmp2 = NULL;
                int j = 0;

                //  new parentid, init the cgroup_list
                if ((poid_reserve[0] && strcmp(parentid, poid_reserve) != 0) || (poid_reserve[0] == '\0')) {
                    rc = strncpy_s(poid_reserve, sizeof(poid_reserve), parentid, strlen(parentid));
                    securec_check_c(rc, "\0", "\0");

                    rc = memset_s(cgroup_list, sizeof(cgroup_list), 0, sizeof(cgroup_list));
                    securec_check_c(rc, "\0", "\0");
                }

                // cgroup_name is as a buffer for control_group
                rc = strncpy_s(cgroup_name, sizeof(cgroup_name) / sizeof(char), control_group, strlen(control_group));
                securec_check_c(rc, "\0", "\0");

                tmp1 = strchr(cgroup_name, ':');
                tmp2 = strrchr(cgroup_name, ':');
                if (tmp1 != NULL && tmp2 != NULL) {
                    if (tmp1 == tmp2) {
                        *tmp1++ = '\0';
                        rc = strncpy_s(workload_name, sizeof(workload_name) / sizeof(char), tmp1, strlen(tmp1));
                        securec_check_c(rc, "\0", "\0");
                    } else {
                        *tmp1++ = '\0';
                        *tmp2++ = '\0';
                        rc = strncpy_s(workload_name, sizeof(workload_name) / sizeof(char), tmp1, strlen(tmp1));
                        securec_check_c(rc, "\0", "\0");
                    }

                    for (j = 0; (j < 10) && cgroup_list[j][0] != '\0'; j++) {
                        if (strcmp(workload_name, GSCGROUP_RUSH_TIMESHARE) != 0 &&
                            strcmp(workload_name, GSCGROUP_HIGH_TIMESHARE) != 0 &&
                            strcmp(workload_name, GSCGROUP_MEDIUM_TIMESHARE) != 0 &&
                            strcmp(workload_name, GSCGROUP_LOW_TIMESHARE) != 0 &&
                            strcmp(cgroup_list[j], workload_name) == 0) {
                            match = true;

                            rc = snprintf_s(cgroup_name_default,
                                sizeof(cgroup_name_default) / sizeof(char),
                                sizeof(cgroup_name_default) / sizeof(char) - 1,
                                "%s:%s",
                                cgroup_name,
                                GSCGROUP_MEDIUM_TIMESHARE);
                            securec_check_ss_c(rc, "\0", "\0");
                            fprintf(stdout,
                                "NOTICE: multi_tenant cgroup \"%s\" is redundant, use \"%s\" instead. \n",
                                control_group,
                                cgroup_name_default);

                            break;
                        }
                    }

                    if (j >= 10) {
                        j = 0;
                    }
                    if (!match && strcmp(workload_name, GSCGROUP_RUSH_TIMESHARE) != 0 &&
                        strcmp(workload_name, GSCGROUP_HIGH_TIMESHARE) != 0 &&
                        strcmp(workload_name, GSCGROUP_MEDIUM_TIMESHARE) != 0 &&
                        strcmp(workload_name, GSCGROUP_LOW_TIMESHARE) != 0) {
                        rc = strncpy_s(cgroup_list[j], sizeof(cgroup_list[j]), workload_name, strlen(workload_name));
                        securec_check_c(rc, "\0", "\0");
                    }
                }
            }

            appendPQExpBuffer(
                buf, "CREATE RESOURCE POOL %s WITH(ACTIVE_STATEMENTS = %s", fmtId(respool_name), act_statements);
            if ((control_group != NULL) && ('\0' != control_group[0]) &&
                (0 != strncmp(control_group, "DefaultClass:Medium", strlen("DefaultClass:Medium")))) {
                appendPQExpBuffer(buf, ", CONTROL_GROUP = ");

                if (match) {
                    appendStringLiteralConn(buf, cgroup_name_default, conn);
                } else {
                    appendStringLiteralConn(buf, control_group, conn);
                }
            }

            if ((mem_percent != NULL) && strcmp(mem_percent, "0") != 0) {
                appendPQExpBuffer(buf, ", MEM_PERCENT = %s", mem_percent);
            }

            if ((max_dop != NULL) && strcmp(max_dop, "1") != 0) {
                appendPQExpBuffer(buf, ", MAX_DOP = %s", max_dop);
            }

            if ((memory_limit != NULL) && (memory_limit[0] != '\0') &&
                (0 != strncmp(memory_limit, "8GB", strlen("8GB")))) {
                appendPQExpBuffer(buf, ", MEMORY_LIMIT = ");
                appendStringLiteralConn(buf, memory_limit, conn);
            }

            if ((io_limits != NULL) && strcmp(io_limits, "0") != 0) {
                appendPQExpBuffer(buf, ", IO_LIMITS = %s", io_limits);
            }

            if ((io_priority != NULL) && (io_priority[0] != '\0') &&
                (0 != strncmp(io_priority, "None", strlen("None")))) {
                appendPQExpBuffer(buf, ", IO_PRIORITY = ");
                appendStringLiteralConn(buf, io_priority, conn);
            }

            if ((nodegroupname != NULL) && (nodegroupname[0] != '\0') &&
                (0 != strncmp(nodegroupname, "None", strlen("None")))) {
                appendPQExpBuffer(buf, ", NODEGROUP = ");
                appendStringLiteralConn(buf, nodegroupname, conn);
            }

            appendPQExpBuffer(buf, ");\n");
        } else {
            bool has_option = false;

            appendPQExpBuffer(buf, "CREATE RESOURCE POOL %s", fmtId(respool_name));
            if ((control_group != NULL) && ('\0' != control_group[0]) &&
                (0 != strncmp(control_group, "Medium", strlen("Medium")))) {
                appendPQExpBuffer(buf, " WITH(CONTROL_GROUP = ");
                appendStringLiteralConn(buf, control_group, conn);
                has_option = true;
            }
            if (strcmp(mem_percent, "0") != 0) {
                if (has_option)
                    appendPQExpBuffer(buf, ", MEM_PERCENT = %s", mem_percent);
                else {
                    appendPQExpBuffer(buf, " WITH(MEM_PERCENT = %s", mem_percent);
                    has_option = true;
                }
            }

            if (has_option)
                appendPQExpBuffer(buf, ")");

            appendPQExpBuffer(buf, ";\n");
        }

        fprintf(OPF, "%s", buf->data);
    }
    fprintf(OPF, "\n");
    fprintf(OPF, "SELECT pg_catalog.gs_wlm_rebuild_user_resource_pool(0);\n");

    /* when dump the role information(-r/-g/default, not -t),
     * it should print out the relation about node group and resource pool
     */
    if (!tablespaces_only) {
        fprintf(OPF, "--\n-- Output role and resource pool association information\n--\n\n");
        dumpAlterRolesForResourcePool(conn);
    }

    PQclear(res);
    destroyPQExpBuffer(query);
    destroyPQExpBuffer(buf);
}

static void dumpWorkloadGroups(PGconn* conn)
{
    PQExpBuffer query, buf;
    PGresult* res = NULL;
    int num;
    int i;
    int i_respool_name, i_workload_gpname, i_act_statements;

    query = createPQExpBuffer();
    buf = createPQExpBuffer();

    appendPQExpBuffer(query,
        "select pg_resource_pool.respool_name, pg_workload_group.workload_gpname, pg_workload_group.act_statements"
        " from pg_catalog.pg_resource_pool, pg_catalog.pg_workload_group"
        " where pg_workload_group.respool_oid = pg_resource_pool.oid and pg_workload_group.oid != %d"
        " order by pg_workload_group.workload_gpname;",
        DEFAULT_GROUP_OID);

    res = executeQuery(conn, query->data);

    num = PQntuples(res);

    i_respool_name = PQfnumber(res, "respool_name");
    i_workload_gpname = PQfnumber(res, "workload_gpname");
    i_act_statements = PQfnumber(res, "act_statements");

    if (num > 0)
        fprintf(OPF, "--\n-- Workload groups\n--\n\n");

    for (i = 0; i < num; i++) {
        char* respool_name = PQgetvalue(res, i, i_respool_name);
        char* workload_gpname = PQgetvalue(res, i, i_workload_gpname);
        char* act_statements = PQgetvalue(res, i, i_act_statements);

        resetPQExpBuffer(buf);

        appendPQExpBuffer(buf, "CREATE WORKLOAD GROUP %s", fmtId(workload_gpname));
        appendPQExpBuffer(buf, " USING RESOURCE POOL %s", fmtId(respool_name));
        if (strcmp(act_statements, "-1") != 0)
            appendPQExpBuffer(buf, " WITH(ACT_STATEMENTS = %s);\n", act_statements);
        else
            appendPQExpBuffer(buf, ";\n");

        fprintf(OPF, "%s", buf->data);
    }
    fprintf(OPF, "\n");

    PQclear(res);
    destroyPQExpBuffer(query);
    destroyPQExpBuffer(buf);
}

static void dumpAppWorkloadGroupMapping(PGconn* conn)
{
    PQExpBuffer query;
    PGresult* res = NULL;
    int num = 0;
    int i = 0;
    int i_app_query = 0;

    query = createPQExpBuffer();

    appendPQExpBuffer(query,
        "select 'CREATE APP WORKLOAD GROUP MAPPING ' || pg_app_workloadgroup_mapping.appname"
        " || ' WITH(WORKLOAD_GPNAME = ' || pg_app_workloadgroup_mapping.workload_gpname || ');'"
        " as app_query from  pg_catalog.pg_app_workloadgroup_mapping,"
        " pg_catalog.pg_workload_group where pg_app_workloadgroup_mapping.workload_gpname = "
        "pg_workload_group.workload_gpname"
        " and pg_app_workloadgroup_mapping.oid != %d order by pg_app_workloadgroup_mapping.appname;",
        DEFAULT_APP_OID);

    res = executeQuery(conn, query->data);
    num = PQntuples(res);
    i_app_query = PQfnumber(res, "app_query");

    if (num > 0)
        fprintf(OPF, "--\n-- Application/Workload groups mapping \n--\n\n");

    for (i = 0; i < num; i++) {
        fprintf(OPF, "%s\n", PQgetvalue(res, i, i_app_query));
    }
    fprintf(OPF, "\n");

    PQclear(res);
    destroyPQExpBuffer(query);
}
#endif

/*
 * dumpDataSource
 * 	dump out data source from pg_catalog.pg_extension_data_source
 * 	The basic idea is that we construct some SQL to create the data
 * 	source with exactlly right infos compared with that of system
 * 	table pg_extension_data_source.
 *
 * @conn: connection handler
 * RETURN: void
 */
static void dumpDataSource(PGconn* conn)
{
    PQExpBuffer query, buf;
    PGresult* res = NULL;
    PGresult* bufres = NULL;
    int num = 0;
    int i = 0;
    int i_srcname = 0, i_srcowner = 0, i_srctype = 0, i_srcversion = 0, i_srcacl = 0, i_srcoptions = 0;

    /* Firstly, check if exists pg_extension_data_source */
    if (server_version < 90200 || !is_column_exists(conn, DataSourceRelationId, "srcname"))
        return;

    /* Now, read  infos of Data Source from pg_extension_data_source */
    query = createPQExpBuffer();

    appendPQExpBuffer(query,
        " SELECT srcname, pg_catalog.pg_get_userbyid(srcowner) as ownername, "
        " srctype, srcversion, srcacl, "
        " pg_catalog.array_to_string(array(select pg_catalog.quote_ident(option_name) || ' ' "
        " || pg_catalog.quote_literal(option_value) from pg_catalog.pg_options_to_table(srcoptions) "
        " order by option_name), E',\n  ') AS opts "
        " FROM pg_extension_data_source order by 1;");

    /* locate the attributes in the result set */
    res = executeQuery(conn, query->data);
    i_srcname = PQfnumber(res, "srcname");
    i_srcowner = PQfnumber(res, "ownername");
    i_srctype = PQfnumber(res, "srctype");
    i_srcversion = PQfnumber(res, "srcversion");
    i_srcacl = PQfnumber(res, "srcacl");
    i_srcoptions = PQfnumber(res, "opts");

    /* we have totally num rows */
    num = PQntuples(res);
    if (num > 0)
        dumpall_printf(OPF, "--\n-- Data Sources\n--\n\n");
    else
    /* found nothing, just return */
    {
        PQclear(res);
        destroyPQExpBuffer(query);
        return;
    }

    /*
     * Then, construct the generic SQL one by one row.
     * Each of these SQL could generate a Data Source Object which holds
     * informations we just pull from the pg_extension_data_source.
     * In other words, we restore the data source in our database by running SQL.
     */
    buf = createPQExpBuffer();
    for (i = 0; i < num; i++) {
        char* srcname = PQgetvalue(res, i, i_srcname);
        char* ownername = PQgetvalue(res, i, i_srcowner);
        char* srctype = PQgetvalue(res, i, i_srctype);
        char* srcversion = PQgetvalue(res, i, i_srcversion);
        char* srcacl = PQgetvalue(res, i, i_srcacl);
        char* srcoptions = PQgetvalue(res, i, i_srcoptions);

        resetPQExpBuffer(query);

        /* build create SQL */
        appendPQExpBuffer(query, "CREATE DATA SOURCE %s ", srcname);
        if ((srctype != NULL) && strlen(srctype) > 0) {
            appendPQExpBuffer(query, " TYPE ");
            appendStringLiteralConn(query, srctype, conn);
        }
        if ((srcversion != NULL) && strlen(srcversion) > 0) {
            appendPQExpBuffer(query, " VERSION ");
            appendStringLiteralConn(query, srcversion, conn);
        }
        if ((srcoptions != NULL) && strlen(srcoptions) > 0)
            appendPQExpBuffer(query, " OPTIONS (%s) ", srcoptions);

        appendPQExpBuffer(query, ";\n");

        /* alter owner SQL */
        resetPQExpBuffer(buf);
        appendPQExpBuffer(
            buf, " select rolsuper, rolsystemadmin from pg_catalog.pg_authid u where u.rolname='%s';", ownername);
        bufres = executeQuery(conn, buf->data);
        if (PQntuples(bufres) > 0) {
            int i_su = PQfnumber(bufres, "rolsuper");
            int i_sysadmin = PQfnumber(bufres, "rolsystemadmin");
            char* su = PQgetvalue(bufres, 0, i_su);
            char* sysadmin = PQgetvalue(bufres, 0, i_sysadmin);
            if (su == NULL || sysadmin == NULL) {
                continue;
            }

            if (*su == 't' || *sysadmin == 't') {
                /*
                 * Owner of the data source has privileges of sysadmin or superuser, change the owner directly.
                 */
                appendPQExpBuffer(query, "ALTER DATA SOURCE %s OWNER TO %s;\n", srcname, ownername);
            } else {
                /*
                 * This is a special case: OWNER of the data source is not sysadmin or superuser.
                 * This is because we revoke the sysadmin or superuser privileges from the OWNER after
                 * it is been assigned the owner of the data source.
                 *
                 * Note: we assume that the user who run the restore program has privileges of superuser.
                 */
                appendPQExpBuffer(query, "ALTER USER %s WITH SYSADMIN;\n", ownername);
                appendPQExpBuffer(query, "ALTER DATA SOURCE %s OWNER TO %s;\n", srcname, ownername);
                appendPQExpBuffer(query, "ALTER USER %s WITH NOSYSADMIN;\n", ownername);
            }
        }
        PQclear(bufres);

        /* build ACL SQL (There may be more than one SQL inside) */
        if (!skip_acls &&
            !buildACLCommands(srcname, NULL, "DATA SOURCE", srcacl, ownername, "", server_version, query, conn)) {
            write_stderr(_("%s: could not parse ACL list (%s) for data source \"%s\"\n"), progname, srcacl, srcname);
            PQfinish(conn);
            exit_nicely(1);
        }

        /* At last, we push these SQL into file */
        dumpall_printf(OPF, "%s", query->data);
    }

    dumpall_printf(OPF, "\n");

    /* Done */
    PQclear(res);
    destroyPQExpBuffer(query);
    destroyPQExpBuffer(buf);
}
