/* -------------------------------------------------------------------------
 *
 * pg_restore.c
 *	pg_restore is an utility extracting postgres database definitions
 *	from a backup archive created by pg_dump using the archiver
 *	interface.
 *
 *	pg_restore will read the backup archive and
 *	dump out a script that reproduces
 *	the schema of the database in terms of
 *		  user-defined types
 *		  user-defined functions
 *		  tables
 *		  indexes
 *		  aggregates
 *		  operators
 *		  ACL - grant/revoke
 *
 * the output script is SQL that is understood by PostgreSQL
 *
 * Basic process in a restore operation is:
 *
 *	Open the Archive and read the TOC.
 *	Set flags in TOC entries, and *maybe* reorder them.
 *	Generate script to stdout
 *	Exit
 *
 * Copyright (c) 2000, Philip Warner
 *		Rights are granted to use this software in any way so long
 *		as this notice is not removed.
 *
 *	The author is not responsible for loss or damages that may
 *	result from its use.
 *
 *
 * IDENTIFICATION
 *		src/bin/pg_dump/pg_restore.c
 *
 * -------------------------------------------------------------------------
 */

#include "pg_backup_archiver.h"

#include "dumpmem.h"
#include "dumputils.h"

#include <ctype.h>
#include <sys/time.h>

#ifdef HAVE_TERMIOS_H
#include <termios.h>
#endif

#include <unistd.h>

#include "getopt_long.h"

#include "pgtime.h"

#include "bin/elog.h"
#define PROG_NAME "gs_restore"
extern char* optarg;
extern int optind;
#ifdef ENABLE_NLS
#include <locale.h>
#endif

#ifdef GAUSS_SFT_TEST
#include "gauss_sft.h"
#endif

#ifdef ENABLE_UT
#define static
#endif

void usage(const char* progname);
static bool checkDecryptArchive(char** pFileSpec, const ArchiveFormat fm, const char* key);
static void restore_getopts(int argc, char** argv, struct option* options, RestoreOptions* opts, char** inputFileSpec);
static void validate_restore_options(char** argv, RestoreOptions* opts);
static void free_restoreopts(RestoreOptions* opts);
static void free_SimpleStringList(SimpleStringList* list);
static void get_password_pipeline(RestoreOptions* opts);

static int disable_triggers = 0;
static int no_data_for_failed_tables = 0;
static int outputNoTablespaces = 0;
static int use_setsessauth = 0;
static int no_security_labels = 0;
static char* passwd = NULL;
static char* decrypt_key = NULL;
static bool is_encrypt = false;
static bool is_pipeline = false;
static int no_subscriptions = 0;
static int no_publications = 0;

typedef struct option optType;
#ifdef GSDUMP_LLT
bool lltRunning = true;
void stopLLT()
{
    lltRunning = false;
}
#endif

int main(int argc, char** argv)
{
    RestoreOptions* opts = NULL;
    int exit_code;
    Archive* AH = NULL;
    char* inputFileSpec = NULL;
    bool decryptfile = false;
    struct timeval restoreStartTime;
    struct timeval restoreEndTime;
    pg_time_t restoreTotalTime = 0;
    errno_t rc = 0;

    struct option cmdopts[] = {{"clean", 0, NULL, 'c'},
        {"create", 0, NULL, 'C'},
        {"data-only", 0, NULL, 'a'},
        {"dbname", 1, NULL, 'd'},
        {"exit-on-error", 0, NULL, 'e'},
        {"file", 1, NULL, 'f'},
        {"format", 1, NULL, 'F'},
        {"function", 1, NULL, 'P'},
        {"host", 1, NULL, 'h'},
        {"index", 1, NULL, 'I'},
        {"jobs", 1, NULL, 'j'},
        {"list", 0, NULL, 'l'},
        {"no-privileges", 0, NULL, 'x'},
        {"no-acl", 0, NULL, 'x'},
        {"no-owner", 0, NULL, 'O'},
        {"port", 1, NULL, 'p'},
        {"no-password", 0, NULL, 'w'},
        {"password", 1, NULL, 'W'},
        {"schema", 1, NULL, 'n'},
        {"schema-only", 0, NULL, 's'},
        {"sysadmin", 1, NULL, 'S'},
        {"table", 1, NULL, 't'},
#ifdef GAUSS_FEATURE_SUPPORT_FUTURE
        {"trigger", 1, NULL, 'T'},
#endif
        {"use-list", 1, NULL, 'L'},
        {"username", 1, NULL, 'U'},
        {"verbose", 0, NULL, 'v'},
        {"single-transaction", 0, NULL, '1'},

        /*
         * the following options don't have an equivalent short option letter
         */
        {"disable-triggers", no_argument, &disable_triggers, 1},
        {"no-data-for-failed-tables", no_argument, &no_data_for_failed_tables, 1},
        {"no-tablespaces", no_argument, &outputNoTablespaces, 1},
        {"role", required_argument, NULL, 2},
        {"section", required_argument, NULL, 3},
        {"use-set-session-authorization", no_argument, &use_setsessauth, 1},
#if !defined(ENABLE_MULTIPLE_NODES)
        {"no-publications", no_argument, &no_publications, 1},
#endif
        {"no-security-labels", no_argument, &no_security_labels, 1},
#if !defined(ENABLE_MULTIPLE_NODES)
        {"no-subscriptions", no_argument, &no_subscriptions, 1},
#endif
        {"rolepassword", required_argument, NULL, 5},
        {"with-key", required_argument, NULL, 6},
        {"pipeline", no_argument, NULL, 7},
#if defined(USE_ASSERT_CHECKING) || defined(FASTCHECK)
        {"disable-progress", no_argument, NULL, 8},
#endif
        {NULL, 0, NULL, 0}};

    set_pglocale_pgservice(argv[0], PG_TEXTDOMAIN("gs_dump"));

#ifdef GSDUMP_LLT
    while (lltRunning) {
        sleep(3);
    }
    return 0;
#endif
    init_parallel_dump_utils();

    opts = NewRestoreOptions();

    progname = get_progname("gs_restore");

    (void)gettimeofday(&restoreStartTime, NULL);

    if (argc > 1) {
        if (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-?") == 0) {
            usage(progname);
            exit_nicely(0);
        }
        if (strcmp(argv[1], "--version") == 0 || strcmp(argv[1], "-V") == 0) {
#ifdef ENABLE_MULTIPLE_NODES
            puts("gs_restore " DEF_GS_VERSION);
#else
            puts("gs_restore (openGauss) " PG_VERSION);
#endif
            exit_nicely(0);
        }
    }

    /* parse the restore options for gs_restore*/
    restore_getopts(argc, argv, cmdopts, opts, &inputFileSpec);

    if (is_pipeline) {
        get_password_pipeline(opts);
    }

    if (NULL == inputFileSpec) {
        write_stderr(_("Mandatory to specify dump filename/path for gs_restore\n"));
        if (opts->rolepassword != NULL) {
            rc = memset_s(opts->rolepassword, strlen(opts->rolepassword), 0, strlen(opts->rolepassword));
            securec_check_c(rc, "\0", "\0");
        }
        if (passwd != NULL) {
            rc = memset_s(passwd, strlen(passwd), 0, strlen(passwd));
            securec_check_c(rc, "\0", "\0");
        }
        exit_nicely(1);
    } else {
        inputFileSpec = make_absolute_path(inputFileSpec);
    }

    if ((NULL == opts->dbname) && (0 == opts->tocSummary)) {
        write_stderr(_("Mandatory to specify either dbname or list option for gs_restore\n"));
        if (opts->rolepassword != NULL) {
            rc = memset_s(opts->rolepassword, strlen(opts->rolepassword), 0, strlen(opts->rolepassword));
            securec_check_c(rc, "\0", "\0");
        }
        if (passwd != NULL) {
            rc = memset_s(passwd, strlen(passwd), 0, strlen(passwd));
            securec_check_c(rc, "\0", "\0");
        }
        free(inputFileSpec);
        inputFileSpec = NULL;
        exit_nicely(1);
    }

    if ((opts->dbname != NULL) && (1 == opts->tocSummary)) {
        write_stderr(_("Either one of dbname or list option need to be specified for gs_restore\n"));
        if (opts->rolepassword != NULL) {
            rc = memset_s(opts->rolepassword, strlen(opts->rolepassword), 0, strlen(opts->rolepassword));
            securec_check_c(rc, "\0", "\0");
        }
        if (passwd != NULL) {
            rc = memset_s(passwd, strlen(passwd), 0, strlen(passwd));
            securec_check_c(rc, "\0", "\0");
        }
        free(inputFileSpec);
        inputFileSpec = NULL;
        exit_nicely(1);
    }

    /* Make sure the input file exists */
    FILE* fp = fopen(inputFileSpec, PG_BINARY_R);
    if (NULL == fp) {
        (void)fprintf(stderr, "%s: %s\n", inputFileSpec, strerror(errno));
        exit_nicely(1);
    }
    (void)fclose(fp);
    fp = NULL;

    /* validate the restore options before start the actual operation */
    validate_restore_options(argv, opts);
    if (is_encrypt) {
        exit_horribly(NULL, "Encrypt mode is not supported yet.\n");
    }
    decryptfile = checkDecryptArchive(&inputFileSpec, (ArchiveFormat)opts->format, decrypt_key);

    /* Take lock on the file itself on non-directory format and create a
     * lock file on the directory and take lock on that file
     */
    if (false == IsDir(inputFileSpec)) {
        catalog_lock(inputFileSpec);
        on_exit_nicely(catalog_unlock, NULL);
    } else {
        char* lockFile = NULL;
        int nRet = 0;
        size_t lockFileLen = strlen(inputFileSpec) + strlen(DIR_SEPARATOR) + strlen(DIR_LOCK_FILE) + 1;
        lockFile = (char*)pg_malloc(lockFileLen);
        nRet = snprintf_s(lockFile, lockFileLen, lockFileLen - 1, "%s/%s", inputFileSpec, DIR_LOCK_FILE);
        securec_check_ss_c(nRet, lockFile, "\0");
        catalog_lock(lockFile);
        on_exit_nicely(remove_lock_file, lockFile);
        on_exit_nicely(catalog_unlock, NULL);
    }

    AH = OpenArchive(inputFileSpec, (ArchiveFormat)opts->format);

    /*
     * We don't have a connection yet but that doesn't matter. The connection
     * is initialized to NULL and if we terminate through exit_nicely() while
     * it's still NULL, the cleanup function will just be a no-op.
     */
    on_exit_close_archive(AH);

    /* Let the archiver know how noisy to be */
    AH->verbose = opts->verbose;

    /*
     * Whether to keep submitting sql commands as "gs_restore ... | gsql ... "
     */
    AH->exit_on_error = (opts->exit_on_error ? true : false);

    ((ArchiveHandle*)AH)->savedPassword = passwd;
    if (NULL != opts->tocFile)
        SortTocFromFile(AH, opts);

    /* See comments in pg_dump.c */
    if (opts->number_of_jobs <= 0
#ifdef WIN32
        || opts->number_of_jobs > MAXIMUM_WAIT_OBJECTS
#endif
    ) {
        if (decryptfile && !rmtree(inputFileSpec, true))
            write_stderr(_("%s: failed to remove dir %s.\n"), progname, inputFileSpec);
        write_stderr(_("%s: invalid number of parallel jobs\n"), progname);
        if (opts->rolepassword != NULL) {
            rc = memset_s(opts->rolepassword, strlen(opts->rolepassword), 0, strlen(opts->rolepassword));
            securec_check_c(rc, "\0", "\0");
        }
        if (passwd != NULL) {
            rc = memset_s(passwd, strlen(passwd), 0, strlen(passwd));
            securec_check_c(rc, "\0", "\0");
        }
        if (NULL != inputFileSpec) {
            free(inputFileSpec);
            inputFileSpec = NULL;
        }
        exit(1);
    }

    if (opts->tocSummary)
        PrintTOCSummary(AH, opts);
    else {
        SetArchiveRestoreOptions(AH, opts);
        RestoreArchive(AH);
    }

    /* Clear password related memory to avoid leaks when core. */
    if (opts->rolepassword != NULL) {
        rc = memset_s(opts->rolepassword, strlen(opts->rolepassword), 0, strlen(opts->rolepassword));
        securec_check_c(rc, "\0", "\0");
        free(opts->rolepassword);
        opts->rolepassword = NULL;
    }

    if (passwd != NULL) {
        rc = memset_s(passwd, strlen(passwd), 0, strlen(passwd));
        securec_check_c(rc, "\0", "\0");
    }
    GS_FREE(passwd);

    /* done, print a summary of ignored errors */
    if (AH->n_errors) {
        write_stderr(_("WARNING: errors ignored on restore: %d\n"), AH->n_errors);
    }

    /* AH may be freed in CloseArchive? */
    exit_code = AH->n_errors ? 1 : 0;

    CloseArchive(AH);

    GS_FREE(opts->superuser);
    if (decrypt_key != NULL) {
        rc = memset_s(decrypt_key, strlen(decrypt_key), '\0', strlen(decrypt_key));
        securec_check_c(rc, "\0", "\0");
    }
    GS_FREE(decrypt_key);

    /*free the memory allocated for gs_restore options */
    free_restoreopts(opts);

    if (decryptfile && !rmtree(inputFileSpec, true)) {
        write_stderr(_("%s: failed to remove dir %s.\n"), progname, inputFileSpec);
        if (NULL != inputFileSpec) {
            free(inputFileSpec);
            inputFileSpec = NULL;
        }
        exit(1);
    }

    if (NULL != inputFileSpec) {
        free(inputFileSpec);
        inputFileSpec = NULL;
    }
    write_msg(NULL, "restore operation successful\n");
    (void)gettimeofday(&restoreEndTime, NULL);
    restoreTotalTime = (restoreEndTime.tv_sec - restoreStartTime.tv_sec) * 1000 +
                       (restoreEndTime.tv_usec - restoreStartTime.tv_usec) / 1000;
    write_msg(NULL, "total time: %lld  ms\n", (long long int)restoreTotalTime);

    return exit_code;
}

static void get_password_pipeline(RestoreOptions* opts)
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
        rc = memset_s(passwd, strlen(passwd), 0, strlen(passwd));
        securec_check_c(rc, "\0", "\0");
        GS_FREE(passwd);
    }

    if (NULL != fgets(pass_buf, pass_max_len, stdin)) {
        opts->promptPassword = TRI_YES;
        pass_buf[strlen(pass_buf) - 1] = '\0';
        passwd = gs_strdup(pass_buf);
    }

    rc = memset_s(pass_buf, pass_max_len, 0, pass_max_len);
    securec_check_c(rc, "\0", "\0");
    free(pass_buf);
    pass_buf = NULL;
}

static void free_restoreopts(RestoreOptions* opts)
{
    if (opts != NULL) {
        free(opts->username);
        opts->username = NULL;
        free(opts->use_role);
        opts->use_role = NULL;
        free(opts->dbname);
        opts->dbname = NULL;
        free((void*)(opts->filename));
        opts->filename = NULL;
        free(opts->formatName);
        opts->formatName = NULL;
        free(opts->pghost);
        opts->pghost = NULL;
        free(opts->tocFile);
        opts->tocFile = NULL;
        free(opts->pgport);
        opts->pgport = NULL;

        if (opts->idWanted != NULL) {
            free(opts->idWanted);
            opts->idWanted = NULL;
        }

        /* free index names */
        free_SimpleStringList(&opts->indexNames);

        /* free function names */
        free_SimpleStringList(&opts->functionNames);

        /* free table names */
        free_SimpleStringList(&opts->tableNames);

        /* free triggers name */
        free_SimpleStringList(&opts->triggerNames);

        /* free schemas */
        free_SimpleStringList(&opts->schemaNames);

        free(opts);
        opts = NULL;
    }
}

static void free_SimpleStringList(SimpleStringList* list)
{
    SimpleStringListCell* cell = NULL;

    while (NULL != list && list->head != NULL) {
        cell = list->head;
        list->head = list->head->next;
        free(cell);
        cell = NULL;
    }
}

static void validate_restore_options(char** argv, RestoreOptions* opts)
{
    init_log((char*)PROG_NAME);

    /* Should get at most one of -d and -f, else user is confused */
    if (opts->dbname != NULL) {
        if (opts->filename != NULL) {
            write_stderr(_("%s: options -d/--dbname and -f/--file cannot be used together\n"), progname);
            write_stderr(_("Try \"%s --help\" for more information.\n"), progname);
            exit_nicely(1);
        }
        opts->useDB = 1;
    }

    if (opts->number_of_jobs <= 0) {
        write_stderr(_("%s: invalid number of parallel jobs\n"), progname);
        exit_nicely(1);
    }

    if (opts->dataOnly && opts->schemaOnly) {
        write_stderr(_("%s: options -s/--schema-only and -a/--data-only cannot be used together\n"), progname);
        exit_nicely(1);
    }

    if (opts->dataOnly && opts->dropSchema) {
        write_stderr(_("%s: options -c/--clean and -a/--data-only cannot be used together\n"), progname);
        exit_nicely(1);
    }

    /* Can't do single-txn mode with multiple connections */
    if (opts->single_txn && opts->number_of_jobs > 1) {
        write_stderr(_("%s: cannot specify both --single-transaction and multiple jobs\n"), progname);
        exit_nicely(1);
    }

    opts->disable_triggers = disable_triggers;
    opts->noDataForFailedTables = no_data_for_failed_tables;
    opts->noTablespace = outputNoTablespaces;
    opts->use_setsessauth = use_setsessauth;
    opts->no_security_labels = no_security_labels;
    opts->no_subscriptions = no_subscriptions;
    opts->no_publications = no_publications;

    if (NULL != opts->formatName) {
        switch (opts->formatName[0]) {
            case 'c':
            case 'C':
                opts->format = (int)archCustom;
                break;

            case 'd':
            case 'D':
                opts->format = (int)archDirectory;
                break;

            case 't':
            case 'T':
                opts->format = (int)archTar;
                break;

            default:
                write_msg(NULL,
                    "unrecognized archive format \"%s\"; please specify \"c\", \"d\", or \"t\"\n",
                    opts->formatName);
                exit_nicely(1);
        }
    }
}
static void restore_getopts(int argc, char** argv, struct option* options, RestoreOptions* opts, char** inputFileSpec)
{
    int c;

    while ((c = getopt_long(argc, argv, "acCd:ef:F:h:I:j:lL:n:Op:P:RsS:t:U:vwW:x1", options, NULL)) != -1) {
        switch (c) {
            case 'a': /* Dump data only */
                opts->dataOnly = 1;
                break;
            case 'c': /* clean (i.e., drop) schema prior to create */
                opts->dropSchema = 1;
                break;
            case 'C':
                opts->createDB = 1;
                break;
            case 'd':
                GS_FREE(opts->dbname);
                check_env_value_c(optarg);
                opts->dbname = gs_strdup(optarg);
                break;
            case 'e':
                opts->exit_on_error = (int)true;
                break;
            case 'f': /* output file name */
                GS_FREE(opts->filename);
                check_env_value_c(optarg);
                opts->filename = gs_strdup(optarg);
                break;
            case 'F':
                if (strlen(optarg) != 0)
                    GS_FREE(opts->formatName);
                check_env_value_c(optarg);
                opts->formatName = gs_strdup(optarg);
                break;

            case 'h':
                if (strlen(optarg) != 0)
                    GS_FREE(opts->pghost);
                check_env_value_c(optarg);
                opts->pghost = gs_strdup(optarg);
                break;

            case 'j': /* number of restore jobs */
                opts->number_of_jobs = atoi(optarg);
                break;

            case 'l': /* Dump the TOC summary */
                opts->tocSummary = 1;
                break;

            case 'L': /* input TOC summary file name */
                GS_FREE(opts->tocFile);
                check_env_value_c(optarg);
                opts->tocFile = gs_strdup(optarg);
                break;

            case 'n': /* Dump data for this schema only */
                opts->selNamespace = 1;
                simple_string_list_append(&opts->schemaNames, optarg);
                break;

            case 'O':
                opts->noOwner = 1;
                break;

            case 'p':
                if (strlen(optarg) != 0)
                    GS_FREE(opts->pgport);
                check_env_value_c(optarg);
                opts->pgport = gs_strdup(optarg);
                break;

            case 'P': /* Function */
                opts->selTypes = 1;
                opts->selFunction = 1;
                simple_string_list_append(&opts->functionNames, optarg);
                break;
            case 'I': /* Index */
                opts->selTypes = 1;
                opts->selIndex = 1;
                simple_string_list_append(&opts->indexNames, optarg);
                break;

#ifdef GAUSS_FEATURE_SUPPORT_FUTURE
            case 'T': /* Trigger */
                opts->selTypes = 1;
                opts->selTrigger = 1;
                simple_string_list_append(&opts->triggerNames, optarg);
                break;
#endif
            case 's': /* dump schema only */
                opts->schemaOnly = 1;
                break;
            case 'S': /* Superuser username */
                if (strlen(optarg) != 0) {
                    check_env_value_c(optarg);
                    opts->superuser = gs_strdup(optarg);
                }
                break;
            case 't': /* Dump data for this table only */
                opts->selTypes = 1;
                opts->selTable = 1;
                simple_string_list_append(&opts->tableNames, optarg);
                break;

            case 'U':
                GS_FREE(opts->username);
                check_env_value_c(optarg);
                opts->username = gs_strdup(optarg);
                break;

            case 'v': /* verbose */
                opts->verbose = 1;
                break;

            case 'w':
                opts->promptPassword = TRI_NO;
                break;

            case 'W':
                opts->promptPassword = TRI_YES;
                if (passwd != NULL) {
                    errno_t rc = memset_s(passwd, strlen(passwd), 0, strlen(passwd));
                    securec_check_c(rc, "\0", "\0");
                }
                GS_FREE(passwd);
                passwd = gs_strdup(optarg);
                replace_password(argc, argv, "-W");
                replace_password(argc, argv, "--password");
                break;

            case 'x': /* skip ACL dump */
                opts->aclsSkip = 1;
                break;

            case '1': /* Restore data in a single transaction */
                opts->single_txn = (int)true;
                opts->exit_on_error = (int)true;
                break;

            case 0:

                /*
                 * This covers the long options without a short equivalent.
                 */
                break;

            case 2: /* SET ROLE */
                GS_FREE(opts->use_role);
                check_env_value_c(optarg);
                opts->use_role = gs_strdup(optarg);
                break;

            case 3: /* section */
                set_dump_section(optarg, &(opts->dumpSections));
                break;

            case 5:
                GS_FREE(opts->rolepassword);
                opts->rolepassword = gs_strdup(optarg);
                replace_password(argc, argv, "--rolepassword");
                break;

            case 6: /* AES decryption key */
                GS_FREE(decrypt_key);
                decrypt_key = gs_strdup(optarg);
                replace_password(argc, argv, "--with-key");
                is_encrypt = true;
                break;

            case 7:
                is_pipeline = true;
                break;
#if defined(USE_ASSERT_CHECKING) || defined(FASTCHECK)
            case 8:
                opts->disable_progress = true;
                break;
#endif
            default:
                write_stderr(_("Try \"%s --help\" for more information.\n"), progname);
                exit_nicely(1);
        }
    }

    /* Get file name from command line */
    if (optind < argc)
        *inputFileSpec = argv[optind++];
    else
        *inputFileSpec = NULL;

    /* Complain if any arguments remain */
    if (optind < argc) {
        write_stderr(_("%s: too many command-line arguments (first is \"%s\")\n"), progname, argv[optind]);
        write_stderr(_("Try \"%s --help\" for more information.\n"), progname);
        exit_nicely(1);
    }
}

void usage(const char* pchProgname)
{
    printf(_("%s restores an openGauss database from an archive created by gs_dump.\n\n"), pchProgname);
    printf(_("Usage:\n"));
    printf(_("  %s [OPTION]... FILE\n"), pchProgname);

    printf(_("\nGeneral options:\n"));
    printf(_("  -d, --dbname=NAME                       connect to database name\n"));
    printf(_("  -f, --file=FILENAME                     output file name\n"));
    printf(_("  -F, --format=c|d|t                      backup file format (should be automatic)\n"));
    printf(_("  -l, --list                              print summarized TOC of the archive\n"));
    printf(_("  -v, --verbose                           verbose mode\n"));
    printf(_("  -V, --version                           output version information, then exit\n"));
    printf(_("  -?, --help                              show this help, then exit\n"));

    printf(_("\nOptions controlling the restore:\n"));
    printf(_("  -a, --data-only                       restore only the data, no schema\n"));
    printf(_("  -c, --clean                           clean (drop) database objects before recreating\n"));
    printf(_("  -C, --create                          create the target database\n"));
    printf(_("  -e, --exit-on-error                   exit on error, default is to continue\n"));
    printf(_("  -I, --index=NAME                      restore named index(s)\n"));
    printf(_("  -j, --jobs=NUM                        use this many parallel jobs to restore\n"));
    printf(_("  -L, --use-list=FILENAME               use table of contents from this file for\n"
             "                                        selecting/ordering output\n"));
    printf(_("  -n, --schema=NAME                     restore only objects in this schema(s)\n"));
    printf(_("  -O, --no-owner                        skip restoration of object ownership\n"));
    printf(_("  -P, --function=NAME(args)             restore named function(s)\n"));
    printf(_("  -s, --schema-only                     restore only the schema, no data\n"));
    printf(_("  -S, --sysadmin=NAME                   system admin user name to use for disabling triggers\n"));
    printf(_("  -t, --table=NAME                      restore named table(s)\n"));
    printf(_("  -T, --trigger=NAME                    restore named trigger(s)\n"));
    printf(_("  -x, --no-privileges/--no-acl          skip restoration of access privileges (grant/revoke)\n"));
    printf(_("  -1, --single-transaction              restore as a single transaction\n"));
    printf(_("  --disable-triggers                    disable triggers during data-only restore\n"));
    printf(_("  --no-data-for-failed-tables           do not restore data of tables that could not be\n"
             "                                        created\n"));
#if !defined(ENABLE_MULTIPLE_NODES)
    printf(_("  --no-publications                     do not restore publications\n"));
#endif
    printf(_("  --no-security-labels                  do not restore security labels\n"));
#if !defined(ENABLE_MULTIPLE_NODES)
    printf(_("  --no-subscriptions                    do not restore subscriptions\n"));
#endif
    printf(_("  --no-tablespaces                      do not restore tablespace assignments\n"));
    printf(_("  --section=SECTION                     restore named section (pre-data, data, or post-data)\n"));
    printf(_("  --use-set-session-authorization       use SET SESSION AUTHORIZATION commands instead of\n"
             "                                        ALTER OWNER commands to set ownership\n"));
    printf(_("  --pipeline                            use pipeline to pass the password,\n"
             "                                        forbidden to use in terminal\n"));

    printf(_("\nConnection options:\n"));
    printf(_("  -h, --host=HOSTNAME                   database server host or socket directory\n"));
    printf(_("  -p, --port=PORT                       database server port number\n"));
    printf(_("  -U, --username=NAME                   connect as specified database user\n"));
    printf(_("  -w, --no-password                     never prompt for password\n"));
    printf(_("  -W, --password=PASSWORD               the password of specified database user\n"));
    printf(_("  --role=ROLENAME                       do SET ROLE before restore\n"));
    printf(_("  --rolepassword=ROLEPASSWORD           the password for role\n"));
}

/*
 * Check whether the archive is encrytped ,then decrypt it if needed.
 */
static bool checkDecryptArchive(char** pFileSpec, const ArchiveFormat fm, const char* key)
{
    ArchiveHandle* AH = NULL;
    const char* mode = "AES128";

    AH = (ArchiveHandle*)pg_calloc(1, sizeof(ArchiveHandle));

    AH->fSpec = gs_strdup(*pFileSpec);
    AH->format = fm;

    check_encrypt_parameters((Archive*)AH, (key != NULL ? mode : NULL), key);

    decryptArchive((Archive*)AH, fm);

    if (strncmp(*pFileSpec, AH->fSpec, (strlen(*pFileSpec) + 1)) == 0) {
        free(AH->fSpec);
        AH->fSpec = NULL;
        free(AH);
        AH = NULL;
        return false;
    }

    if (NULL != *pFileSpec) {
        free(*pFileSpec);
        *pFileSpec = NULL;
    }

    *pFileSpec = gs_strdup(AH->fSpec);

    free(AH->fSpec);
    AH->fSpec = NULL;
    free(AH);
    AH = NULL;

    return true;
}

