/*
 *	opt.c
 *
 *	options functions
 *
 *	Copyright (c) 2010-2012, PostgreSQL Global Development Group
 *	contrib/pg_upgrade/option.c
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#include "miscadmin.h"

#include "pg_upgrade.h"

#include <getopt_long.h>
#include <time.h>
#include <sys/types.h>
#include <sys/stat.h>
#ifdef WIN32
#include <io.h>
#endif

#define MPPDB_TOOLS_VERSION DEF_GS_VERSION

static void usage(void);
static void check_required_directory(
    char** dirpath, char** configpath, char* envVarName, char* cmdLineOption, char* description);
static void doShellQuoting(PQExpBuffer buf, const char* str);

void pg_init_logfiles(char* log_path, char* instance_name);

UserOpts user_opts;

PQExpBuffer g_gs_upgrade_opts;

char g_log_path[MAXPGPATH] = {0};

extern char* dump_option;
char* argv0 = NULL;
extern char* cluster_map_file;
extern char* NodeName;
/*
 * parseCommandLine()
 *
 *	Parses the command line (argc, argv[]) and loads structures
 */
void parseCommandLine(int argc, char* argv[])
{
    static struct option long_options[] = {{"old-datadir", required_argument, NULL, 'd'},
        {"new-datadir", required_argument, NULL, 'D'},
        {"old-bindir", required_argument, NULL, 'b'},
        {"new-bindir", required_argument, NULL, 'B'},
        {"old-options", required_argument, NULL, 'o'},
        {"new-options", required_argument, NULL, 'O'},
        {"old-port", required_argument, NULL, 'p'},
        {"new-port", required_argument, NULL, 'P'},

        {"ha-nodename", required_argument, NULL, 2},
        {"ha-path", required_argument, NULL, 3},

        {"old-user", required_argument, NULL, 'u'},
        {"new-user", required_argument, NULL, 'U'},
        {"command", required_argument, NULL, 'c'},
        {"cluster_version", required_argument, NULL, 'C'},
        {"cluster_map", required_argument, NULL, 'm'},
        {"link", no_argument, NULL, 'k'},
        {"retain", no_argument, NULL, 'r'},
        {"dump-nodes", no_argument, NULL, 'n'},
        {"verbose", no_argument, NULL, 'v'},
        {"inplace_upgrade", no_argument, NULL, 'i'},
        {NULL, 0, NULL, 0}};
    int option;       /* Command line option */
    int optindex = 0; /* used by getopt_long */
    int os_user_effective_id;
    errno_t rc = 0;

    char path[MAXPGPATH];
    char instance_name[NAMEDATALEN] = {0};

    rc = strncpy_s(g_log_path, MAXPGPATH, ".", MAXPGPATH - 1);
    securec_check_c(rc, "\0", "\0");

    g_gs_upgrade_opts = createPQExpBuffer();

    user_opts.transfer_mode = TRANSFER_MODE_COPY;
    user_opts.pgHAipaddr = NULL;
    user_opts.pgHAdata = NULL;
    user_opts.cluster_version = 0;
    user_opts.inplace_upgrade = false;

    dump_option = "";
    argv0 = argv[0];
    os_info.progname = get_progname(argv[0]);

    /* Process libpq env. variables; load values here for usage() output */
    old_cluster.port = getenv("PGPORTOLD") ? atoi(getenv("PGPORTOLD")) : DEF_PGUPORT;
    new_cluster.port = getenv("PGPORTNEW") ? atoi(getenv("PGPORTNEW")) : DEF_PGUPORT;

    os_user_effective_id = get_user_info(&os_info.user);
    os_info.is_root_user = (os_user_effective_id == 0);
    old_cluster.user = NULL;
    new_cluster.user = NULL;
    cluster_map_file = NULL;
    cluster_type = "restoremode";
    /* we override just the database user name;  we got the OS id above */
    if (getenv("PGUSER")) {
        pg_free(os_info.user);
        /* must save value, getenv()'s pointer is not stable */
        os_info.user = pg_strdup(getenv("PGUSER"));
    }

    if (argc > 1) {
        if (strncmp(argv[1], "--help", sizeof("--help")) == 0 || strncmp(argv[1], "-?", sizeof("-?")) == 0) {
            usage();
            exit(0);
        }
        if (strncmp(argv[1], "--version", sizeof("--version")) == 0 || strncmp(argv[1], "-V", sizeof("-V")) == 0) {
            puts("gs_upgrade " MPPDB_TOOLS_VERSION);
            exit(0);
        }
    }

    while ((option = getopt_long(argc, argv, "d:D:b:B:c:C:ikl:m:nN:o:O:p:P:ru:U:vZ:", long_options, &optindex)) != -1) {
        switch (option) {
            case 'b':
                old_cluster.bindir = pg_strdup(optarg);
                appendPQExpBuffer(g_gs_upgrade_opts, " -b ");
                doShellQuoting(g_gs_upgrade_opts, optarg);
                break;

            case 'B':
                new_cluster.bindir = pg_strdup(optarg);
                appendPQExpBuffer(g_gs_upgrade_opts, " -B ");
                doShellQuoting(g_gs_upgrade_opts, optarg);
                break;
            case 'C':
                user_opts.cluster_version = atoi(optarg);
                if (user_opts.cluster_version < 0 || user_opts.cluster_version > 3) {
                    pg_log(PG_FATAL, "invalid cluster version command \"%s\"\n", optarg);
                    exit(1);
                }
                appendPQExpBuffer(g_gs_upgrade_opts, " -C ");
                doShellQuoting(g_gs_upgrade_opts, optarg);
                break;

            case 'c':
                if (strncmp(optarg, "check", sizeof("check")) == 0) {
                    user_opts.command = COMMAND_CHECK;
                } else if (strncmp(optarg, "init_upgrade", sizeof("init_upgrade")) == 0) {
                    user_opts.command = COMMAND_INIT_UPGRADE;
                } else if (strncmp(optarg, "upgrade", sizeof("upgrade")) == 0) {
                    user_opts.command = COMMAND_UPGRADE;
                } else if (strncmp(optarg, "upgrade_gtm", sizeof("upgrade_gtm")) == 0) {
                    user_opts.command = COMMAND_GTM_UPGRADE;
                } else if (strncmp(optarg, "stop_gtm", sizeof("stop_gtm")) == 0) {
                    user_opts.command = COMMAND_GTM_STOP;
                } else if (strncmp(optarg, "transfer_files", sizeof("transfer_files")) == 0) {
                    user_opts.command = COMMAND_TRANSFER_FILES;
                } else if (strncmp(optarg, "upgrade_log", sizeof("upgrade_log")) == 0) {
                    user_opts.command = COMMAND_UPGRADE_LOG;
                } else {
                    pg_log(PG_FATAL, "invalid command \"%s\"\n", optarg);
                    exit(1);
                }
                appendPQExpBuffer(g_gs_upgrade_opts, " -c ");
                doShellQuoting(g_gs_upgrade_opts, optarg);
                break;

            case 'd':
                old_cluster.pgdata = pg_strdup(optarg);
                old_cluster.pgconfig = pg_strdup(optarg);
                break;

            case 'D':
                new_cluster.pgdata = pg_strdup(optarg);
                new_cluster.pgconfig = pg_strdup(optarg);
                break;

            case 'k':
                user_opts.transfer_mode = TRANSFER_MODE_LINK;
                appendPQExpBuffer(g_gs_upgrade_opts, " -k");
                break;

            case 'o':
                old_cluster.pgopts = pg_strdup(optarg);
                appendPQExpBuffer(g_gs_upgrade_opts, " -o ");
                doShellQuoting(g_gs_upgrade_opts, optarg);
                break;

            case 'O':
                new_cluster.pgopts = pg_strdup(optarg);
                appendPQExpBuffer(g_gs_upgrade_opts, " -O ");
                doShellQuoting(g_gs_upgrade_opts, optarg);
                break;

                /*
                 * Someday, the port number option could be removed and passed
                 * using -o/-O, but that requires postmaster -C to be
                 * supported on all old/new versions.
                 */
            case 'p':
                if ((old_cluster.port = atoi(optarg)) <= 0) {
                    pg_log(PG_FATAL, "invalid old port number\n");
                    exit(1);
                }
                break;

            case 'P':
                if ((new_cluster.port = atoi(optarg)) <= 0) {
                    pg_log(PG_FATAL, "invalid new port number\n");
                    exit(1);
                }
                break;
            case 'n':
                dump_option = "--dump-nodes";
                appendPQExpBuffer(g_gs_upgrade_opts, " -n");
                break;
            case 'N':
                NodeName = pg_strdup(optarg);
                break;
            case 'r':
                log_opts.retain = true;
                appendPQExpBuffer(g_gs_upgrade_opts, " -r");
                break;

            case 'u':
                pg_free(old_cluster.user);
                old_cluster.user = pg_strdup(optarg);
                appendPQExpBuffer(g_gs_upgrade_opts, " -u ");
                doShellQuoting(g_gs_upgrade_opts, optarg);

                /*
                 * Push the user name into the environment so pre-9.1
                 * pg_ctl/libpq uses it.
                 */
                break;
            case 'U':
                pg_free(new_cluster.user);
                new_cluster.user = pg_strdup(optarg);
                appendPQExpBuffer(g_gs_upgrade_opts, " -U ");
                doShellQuoting(g_gs_upgrade_opts, optarg);

                /*
                 * Push the user name into the environment so pre-9.1
                 * pg_ctl/libpq uses it.
                 */
                break;

            case 'v':
                pg_log(PG_REPORT, "Running in verbose mode\n");
                log_opts.verbose = true;
                appendPQExpBuffer(g_gs_upgrade_opts, " -v");
                break;

            case 'Z':
                cluster_type = optarg;
                break;
            case 'l':
                rc = strncpy_s(g_log_path, MAXPGPATH, optarg, MAXPGPATH - 1);
                securec_check_c(rc, "\0", "\0");
                appendPQExpBuffer(g_gs_upgrade_opts, " -l ");
                doShellQuoting(g_gs_upgrade_opts, optarg);
                break;

            case 'm':
                pg_free(cluster_map_file);
                cluster_map_file = pg_strdup(optarg);
                break;

            case 'i':
                user_opts.inplace_upgrade = true;
                appendPQExpBuffer(g_gs_upgrade_opts, " -i");
                break;

            case 2:
                user_opts.pgHAipaddr = pg_strdup(optarg);
                break;

            case 3:
                user_opts.pgHAdata = pg_strdup(optarg);
                break;

            default:
                pg_log(PG_FATAL, "Try \"%s --help\" for more information.\n", os_info.progname);
                break;
        }
    }

    if (os_user_effective_id == 0) {
        if ((old_cluster.user == NULL) || (new_cluster.user == NULL)) {
            pg_log(PG_FATAL,
                "User infromation is mandatory when running as root user.\nTry \"%s --help\" for more information.\n",
                os_info.progname);
        }
    } else {
        if ((old_cluster.user != NULL) && (new_cluster.user != NULL)) {
            if (0 != strncmp(old_cluster.user, new_cluster.user, strlen(new_cluster.user) + 1)) {
                pg_log(PG_FATAL,
                    "New and Old users must be same when running as non root user.\nTry \"%s --help\" for more "
                    "information.\n",
                    os_info.progname);
            }
        }
    }

    if (old_cluster.user == NULL) {
        old_cluster.user = pg_strdup(new_cluster.user ? new_cluster.user : os_info.user);
    }

    if (new_cluster.user == NULL) {
        new_cluster.user = pg_strdup(old_cluster.user ? old_cluster.user : os_info.user);
    }

    if (NULL != old_cluster.pgdata) {
        (void)get_local_instancename_by_dbpath(old_cluster.pgdata, instance_name);
    }

    pg_init_logfiles(g_log_path, instance_name);

    /* Allow help and version to be run as root, so do the test here. */
    if ((log_opts.internal = fopen_priv(INTERNAL_LOG_FILE, "a")) == NULL)
        pg_log(PG_FATAL, "cannot write to log file %s\n", INTERNAL_LOG_FILE);

    /* Get values from env if not already set */
    check_required_directory(&old_cluster.bindir, NULL, "PGBINOLD", "-b", "old cluster binaries reside");
    check_required_directory(&new_cluster.bindir, NULL, "PGBINNEW", "-B", "new cluster binaries reside");

    /* update the LD_LIBRARY_PATH env path */
    rc = snprintf_s(path, MAXPGPATH, MAXPGPATH - 1, "%s/../lib", new_cluster.bindir);
    securec_check_ss_c(rc, "\0", "\0");
    pg_putenv("LD_LIBRARY_PATH", path);

    if (NULL == cluster_map_file && NULL == NodeName) {
        if (user_opts.command != COMMAND_TRANSFER_FILES) {
            check_required_directory(
                &old_cluster.pgdata, &old_cluster.pgconfig, "PGDATAOLD", "-d", "old cluster data resides");
        }

        check_required_directory(
            &new_cluster.pgdata, &new_cluster.pgconfig, "PGDATANEW", "-D", "new cluster data resides");
    }
}

static void usage(void)
{
    printf(_("gs_upgrade upgrades an MPPDB cluster from old version to new version.\n\
\nUsage:\n\
  gs_upgrade [OPTION]...\n\
\n\
Options:\n\
  -b, --old-bindir=OLDBINDIR    old cluster executable directory\n\
  -B, --new-bindir=NEWBINDIR    new cluster executable directory\n\
  -c, --command=COMMAND         commands \"check\", \"init_upgrade\" , \"upgrade_log\" or \"upgrade\"\n\
  -i, --inplace_upgrade         Upgrade database in inplace upgrade mode\n\
  -C, --cluster_version=VERSION Version number [0-POC_530, 1-PERF_POC, 2-V1R5, 3-V1R6]\n\
  -m, --cluster_map=MAPFILE     MPPDB cluster map info\n\
  -d, --old-datadir=OLDDATADIR  old cluster data directory\n\
  -D, --new-datadir=NEWDATADIR  new cluster data directory\n\
  -k, --link                    link instead of copying files to new cluster\n\
  -l LOGFOLDER                  log folder path\n\
  -o, --old-options=OPTIONS     old cluster options to pass to the server\n\
  -O, --new-options=OPTIONS     new cluster options to pass to the server\n\
  -p, --old-port=OLDPORT        old cluster port number\n\
  -P, --new-port=NEWPORT        new cluster port number\n\
  -n, --dump-nodes              include node information in dumpfile\n\
  -N [ all | nodename ]         node name for upgrading\n\
  -r, --retain                  retain SQL and log files after success\n\
  -u, --old-user=OLDUSERNAME    old cluster system admin\n\
  -U, --new-user=NEWUSERNAME    new cluster system admin\n\
  -v, --verbose                 enable verbose internal logging\n\
  -V, --version                 display version information, then exit\n\
  -?, -h, --help                show this help, then exit\n\
\n\
Before running gs_upgrade you must:\n\
  create a new database cluster (using the new version of gs_initdb)\n\
  shutdown the postmaster servicing the old cluster\n\
  shutdown the postmaster servicing the new cluster\n\
\n\
When you run gs_upgrade, you must provide the following information:\n\
  the data directory for the old cluster  (-d OLDDATADIR)\n\
  the data directory for the new cluster  (-D NEWDATADIR)\n\
  the \"bin\" directory for the old version (-b OLDBINDIR)\n\
  the \"bin\" directory for the new version (-B NEWBINDIR)\n\
\n\
For example:\n\
 gs_upgrade -N all -u oldUsername -U newUsername -b oldCluster/bin -B newCluster/bin -c init_upgrade\n\
 gs_upgrade -N all -u oldUsername -U newUsername -b oldCluster/bin -B newCluster/bin -c upgrade\n\
 gs_upgrade -N all -u oldUsername -U newUsername -b oldCluster/bin -B newCluster/bin -c upgrade_gtm\n\
 gs_upgrade -N all -u oldUsername -U newUsername -b oldCluster/bin -B newCluster/bin -c stop_gtm\n\
 gs_upgrade -d oldCluster/data -D newCluster/data -b oldCluster/bin -B newCluster/bin -c init_upgrade\n\
 gs_upgrade -d oldCluster/data -D newCluster/data -b oldCluster/bin -B newCluster/bin -c upgrade\n"));
}

/*
 * check_required_directory()
 *
 * Checks a directory option.
 *	dirpath		  - the directory name supplied on the command line
 *	configpath	  - optional configuration directory
 *	envVarName	  - the name of an environment variable to get if dirpath is NULL
 *	cmdLineOption - the command line option corresponds to this directory (-o, -O, -n, -N)
 *	description   - a description of this directory option
 *
 * We use the last two arguments to construct a meaningful error message if the
 * user hasn't provided the required directory name.
 */
static void check_required_directory(
    char** dirpath, char** configpath, char* envVarName, char* cmdLineOption, char* description)
{
    if (*dirpath == NULL || strlen(*dirpath) == 0) {
        const char* envVar = NULL;

        if ((envVar = getenv(envVarName)) && strlen(envVar)) {
            *dirpath = pg_strdup(envVar);
            if (configpath)
                *configpath = pg_strdup(envVar);
        } else
            pg_log(PG_FATAL,
                "You must identify the directory where the %s.\n"
                "Please use the %s command-line option or the %s environment variable.\n",
                description,
                cmdLineOption,
                envVarName);
    }

    /*
     * Trim off any trailing path separators because we construct paths
     * by appending to this path.
     */
#ifndef WIN32
    if ((*dirpath)[strlen(*dirpath) - 1] == '/')
#else
    if ((*dirpath)[strlen(*dirpath) - 1] == '/' || (*dirpath)[strlen(*dirpath) - 1] == '\\')
#endif
        (*dirpath)[strlen(*dirpath) - 1] = 0;
}

/*
 * adjust_data_dir
 *
 * If a configuration-only directory was specified, find the real data dir
 * by quering the running server.  This has limited checking because we
 * can't check for a running server because we can't find postmaster.pid.
 */
void adjust_data_dir(ClusterInfo* cluster)
{
    char filename[MAXPGPATH];
    char cmd[MAXPGPATH], cmd_output[MAX_STRING];
    FILE *fp, *output;
    int nRet = 0;

    /* If there is no postgresql.conf, it can't be a config-only dir */
    nRet = snprintf_s(filename, sizeof(filename), sizeof(filename) - 1, "%s/postgresql.conf", cluster->pgconfig);
    securec_check_ss_c(nRet, "\0", "\0");
    if ((fp = fopen(filename, "r")) == NULL)
        return;
    fclose(fp);

    /* If PG_VERSION exists, it can't be a config-only dir */
    nRet = snprintf_s(filename, sizeof(filename), sizeof(filename) - 1, "%s/PG_VERSION", cluster->pgconfig);
    securec_check_ss_c(nRet, "\0", "\0");
    if ((fp = fopen(filename, "r")) != NULL) {
        fclose(fp);
        return;
    }

    /* Must be a configuration directory, so find the real data directory. */

    prep_status("Finding the real data directory for the %s cluster", CLUSTER_NAME(cluster));

    /*
     * We don't have a data directory yet, so we can't check the PG version,
     * so this might fail --- only works for PG 9.2+.	If this fails,
     * pg_upgrade will fail anyway because the data files will not be found.
     */
    nRet = snprintf_s(cmd,
        sizeof(cmd),
        sizeof(cmd) - 1,
        "%s%s%s\"%s/postmaster\" -D \"%s\" -C data_directory",
        "sudo -u ",
        cluster->user,
        " sh -c ",
        cluster->bindir,
        cluster->pgconfig);
    securec_check_ss_c(nRet, "\0", "\0");

    if ((output = popen(cmd, "r")) == NULL || fgets(cmd_output, sizeof(cmd_output), output) == NULL)
        pg_log(PG_FATAL, "Could not get data directory using %s: %s\n", cmd, getErrorText(errno));

    pclose(output);

    /* Remove trailing newline */
    if (strchr(cmd_output, '\n') != NULL)
        *strchr(cmd_output, '\n') = '\0';

    cluster->pgdata = pg_strdup(cmd_output);

    check_ok();
}

/*
 * get_sock_dir
 *
 * Identify the socket directory to use for this cluster.  If we're doing
 * a live check (old cluster only), we need to find out where the postmaster
 * is listening.  Otherwise, we're going to put the socket into the current
 * directory.
 */
void get_sock_dir(ClusterInfo* cluster, bool live_check)
{
#ifdef HAVE_UNIX_SOCKETS
    /*
     *	sockdir and port were added to postmaster.pid in PG 9.1.
     *	Pre-9.1 cannot process pg_ctl -w for sockets in non-default
     *	locations.
     */
    if (GET_MAJOR_VERSION(cluster->major_version) >= 901) {
        if (!live_check) {
            /* Use the current directory for the socket */
            cluster->sockdir = (char*)pg_malloc(MAXPGPATH);
            if (!getcwd(cluster->sockdir, MAXPGPATH))
                pg_log(PG_FATAL, "cannot find current directory\n");
        } else {
            /*
             *	If we are doing a live check, we will use the old cluster's Unix
             *	domain socket directory so we can connect to the live server.
             */
            unsigned short orig_port = cluster->port;
            char filename[MAXPGPATH], line[MAXPGPATH];
            FILE* fp = NULL;
            int lineno;
            int nRet = 0;

            nRet = snprintf_s(filename, sizeof(filename), sizeof(filename) - 1, "%s/postmaster.pid", cluster->pgdata);
            securec_check_ss_c(nRet, "\0", "\0");
            if ((fp = fopen(filename, "r")) == NULL)
                pg_log(PG_FATAL, "Cannot open file %s: %m\n", filename);

            for (lineno = 1; lineno <= Max(LOCK_FILE_LINE_PORT, LOCK_FILE_LINE_SOCKET_DIR); lineno++) {
                if (fgets(line, sizeof(line), fp) == NULL)
                    pg_log(PG_FATAL, "Cannot read line %d from %s: %m\n", lineno, filename);

                /* potentially overwrite user-supplied value */
                if (lineno == LOCK_FILE_LINE_PORT)
                    (void)sscanf(line, "%hu", &old_cluster.port);
                if (lineno == LOCK_FILE_LINE_SOCKET_DIR) {
                    cluster->sockdir = (char*)pg_malloc(MAXPGPATH);
                    /* strip off newline */
                    (void)sscanf(line, "%s\n", cluster->sockdir);
                }
            }
            fclose(fp);

            /* warn of port number correction */
            if (orig_port != DEF_PGUPORT && old_cluster.port != orig_port)
                pg_log(PG_WARNING, "User-supplied old port number %hu corrected to %hu\n", orig_port, cluster->port);
        }
    } else
        /* Can't get sockdir and pg_ctl -w can't use a non-default, use default */
        cluster->sockdir = NULL;

#else /* !HAVE_UNIX_SOCKETS */
    cluster->sockdir = NULL;
#endif
}

static void doShellQuoting(PQExpBuffer buf, const char* str)
{
    const char* p = NULL;

#ifndef WIN32
    appendPQExpBufferChar(buf, '\'');
    for (p = str; *p; p++) {
        if (*p == '\'')
            appendPQExpBuffer(buf, "'\"'\"'");
        else
            appendPQExpBufferChar(buf, *p);
    }
    appendPQExpBufferChar(buf, '\'');
#else  /* WIN32 */

    appendPQExpBufferChar(buf, '"');
    for (p = str; *p; p++) {
        if (*p == '"')
            appendPQExpBuffer(buf, "\\\"");
        else
            appendPQExpBufferChar(buf, *p);
    }
    appendPQExpBufferChar(buf, '"');
#endif /* WIN32 */
}
